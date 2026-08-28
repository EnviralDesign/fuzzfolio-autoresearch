"""Build a read-only V37 candidate disposition ledger from frozen evidence.

This is deliberately an audit extractor, not a counterfactual reducer.  It
does not replay data, change archive policy, or construct a replacement
selection path.  The native finalizer remains the authority for all archive
semantics; this module only joins the retained receipts it emitted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "temporal_qd_v37_archive_disposition_ledger_v2"


class DispositionLedgerError(RuntimeError):
    """Raised when the retained V37 receipt chain cannot be reconciled."""


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DispositionLedgerError(f"could not read JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise DispositionLedgerError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise DispositionLedgerError(f"could not read JSONL: {path}") from exc
    rows: list[Mapping[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DispositionLedgerError(f"invalid JSONL: {path}:{line_number}") from exc
        if not isinstance(value, Mapping):
            raise DispositionLedgerError(f"object row required: {path}:{line_number}")
        rows.append(value)
    return rows


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DispositionLedgerError(f"{name} must be an object")
    return value


def _rows(value: object, *, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(row, Mapping) for row in value):
        raise DispositionLedgerError(f"{name} must be a list of objects")
    return list(value)


def _text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise DispositionLedgerError(f"{name} must be a nonempty string")
    return value


def _number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DispositionLedgerError(f"{name} must be numeric")
    return float(value)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, allow_nan=False, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(
            json.dumps(row, allow_nan=False, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def _generation_index(path: Path) -> int:
    name = path.name
    if not name.startswith("generation-"):
        raise DispositionLedgerError(f"unexpected generation directory: {path}")
    try:
        return int(name.removeprefix("generation-"))
    except ValueError as exc:
        raise DispositionLedgerError(f"invalid generation directory: {path}") from exc


def _candidate_id(row: Mapping[str, Any]) -> str:
    return _text(row.get("candidateId"), name="candidateId")


def _identity_from_history(candidate: Mapping[str, Any]) -> list[str]:
    output: list[str] = []
    history = candidate.get("structuralOperatorHistory")
    if not isinstance(history, list):
        return output
    for entry in history:
        if not isinstance(entry, Mapping):
            continue
        raw = entry.get("parentCandidateIdentitySha256")
        if isinstance(raw, str) and raw and raw not in output:
            output.append(raw)
        mate = entry.get("mateCandidateIdentitySha256")
        if isinstance(mate, str) and mate and mate not in output:
            output.append(mate)
    return output


def _last_operations(candidate: Mapping[str, Any]) -> list[str]:
    history = candidate.get("structuralOperatorHistory")
    if not isinstance(history, list):
        return []
    return [
        entry["operation"]
        for entry in history
        if isinstance(entry, Mapping) and isinstance(entry.get("operation"), str)
    ]


def _current_window_summary(aggregate: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for index, raw in enumerate(_rows(aggregate.get("windowRecords"), name="aggregate.windowRecords")):
        net = raw.get("conservativeNetR", raw.get("netR"))
        trades = raw.get("trades", raw.get("closedTrades"))
        output.append(
            {
                "ordinal": index,
                "windowId": raw.get("windowId"),
                "conservativeNetR": _number(net, name="window conservative net"),
                "closedTrades": _number(trades, name="window closed trades"),
            }
        )
    return output


def _unsupported_cumulative_detail(cumulative: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    """Record the native lane without recreating a missing gate-by-gate trace.

    The exact V37 finalizer intentionally returns its unselected candidates as
    the pre-classification base rows: their raw window evidence remains, but
    it does not publish whether support, direction, or economics first failed.
    """

    if cumulative.get("robustBreederLane") != "unsupported" or cumulative.get(
        "robustBreederEligible"
    ) is not False:
        raise DispositionLedgerError("unselected cumulative candidate has an invalid native lane")
    return "cumulative_native_unsupported_reason_unavailable", {
        "nativeLane": cumulative.get("robustBreederLane"),
        "nativeEligible": cumulative.get("robustBreederEligible"),
        "reasonDetail": "the V37 finalizer did not emit a gate-by-gate rejection reason",
    }


def _focus_labels(row: Mapping[str, Any]) -> list[str]:
    current = _mapping(row["currentPanel"], name="ledger current panel")
    windows = _rows(current["windows"], name="ledger windows")
    labels: list[str] = []
    if current["finiteSupportEligible"] and current["afterCostNetR"] > 0.0:
        labels.append("finite_support_after_cost_positive")
    if all(_number(window["conservativeNetR"], name="focus window net") > 0.0 for window in windows):
        labels.append("all_window_positive")
    candidate_id = row["candidateId"]
    if candidate_id in {
        "qd_599aa34a2aef63c49d3b0601e5cc",
        "qd_a8338e2e3bc4113cc307208723df",
        "qd_4987663ed6cf86fa49ae66c4517e",
    }:
        labels.append("named_current_panel_positive_immigrant")
    if candidate_id == "qd_f0075a48ced9d13932aadca62adb":
        labels.append("named_all_window_positive_support_boundary")
    if (
        row["origin"]["sourceMode"] == "qd_structural_offspring_bidirectional_pair"
        and current["finiteSupportEligible"]
        and current["afterCostNetR"] > 0.0
    ):
        labels.append("structural_offspring_finite_support_after_cost_positive")
    return labels


def _state_identity(
    *, generation: int, candidate_id: str, evaluation_sha256: str, coverage_sha256: str
) -> str:
    return _canonical_sha256(
        {
            "generationIndex": generation,
            "candidateId": candidate_id,
            "evaluationRecordCanonicalSha256": evaluation_sha256,
            "coverageCanonicalSha256": coverage_sha256,
        }
    )


def _bundle_coverage(
    *,
    bundles: Sequence[Mapping[str, Any]],
    current_panel_id: str,
    required_panel_ids: Sequence[str],
) -> tuple[Mapping[str, Any], str, str]:
    """Bind an evaluation state to only the panel bundles retained at that point."""

    normalized = [
        {
            "bundleSha256": _text(bundle.get("bundleSha256"), name="bundle sha256"),
            "panelId": _text(bundle.get("panelId"), name="bundle panelId"),
        }
        for bundle in bundles
    ]
    normalized.sort(key=lambda row: (row["panelId"], row["bundleSha256"]))
    panel_ids = [row["panelId"] for row in normalized]
    if len(panel_ids) != len(set(panel_ids)):
        raise DispositionLedgerError("candidate coverage contains duplicate panel IDs")
    required = sorted(set(required_panel_ids))
    complete = panel_ids == required
    coverage: Mapping[str, Any] = {
        "currentPanelId": current_panel_id,
        "requiredPanelIds": required,
        "retainedPanelBundles": normalized,
        "requiredCoverageComplete": complete,
    }
    return (
        coverage,
        _canonical_sha256(coverage),
        "exact_retained_required_evidence"
        if complete
        else "would_require_additional_backfill_evaluation",
    )


def _terminal_disposition(
    *,
    provisional_row: Mapping[str, Any] | None,
    cumulative_row: Mapping[str, Any] | None,
    archive_row: Mapping[str, Any] | None,
    quality_ids: set[str],
    frontier_ids: set[str],
    candidate_id: str,
) -> tuple[str, str, list[str], list[str], Mapping[str, Any] | None]:
    if provisional_row is None:
        return (
            "prefinalizer_newcomer_cap",
            "prefinalizer_newcomer_reduction",
            ["not_selected_by_128_newcomer_cap"],
            [],
            None,
        )
    if cumulative_row is None:
        raise DispositionLedgerError("provisional candidate lacks cumulative row")
    if archive_row is not None:
        return (
            "parent_archive_admitted",
            "parent_archive_admission",
            ["native_parent_archive_admitted"],
            [],
            {
                "robustSupport": cumulative_row.get("robustSupport"),
                "robustEconomics": cumulative_row.get("robustEconomics"),
                "nativeLane": cumulative_row.get("robustBreederLane"),
                "nativeEligible": cumulative_row.get("robustBreederEligible"),
            },
        )
    if candidate_id in quality_ids or candidate_id in frontier_ids:
        return (
            "parent_archive_cell_capacity_excluded",
            "parent_archive_cell_capacity",
            ["native_quality_or_frontier_not_projected"],
            [],
            {
                "robustSupport": cumulative_row.get("robustSupport"),
                "robustEconomics": cumulative_row.get("robustEconomics"),
                "nativeLane": cumulative_row.get("robustBreederLane"),
                "nativeEligible": cumulative_row.get("robustBreederEligible"),
            },
        )
    terminal, detail = _unsupported_cumulative_detail(cumulative_row)
    return (
        terminal,
        "cumulative_robust_breeder_gate",
        ["native_unsupported_lane"],
        ["gate_by_gate_reason_not_emitted"],
        detail,
    )


def _input_file(root: Path, generation: int, path: Path) -> Mapping[str, Any]:
    return {
        "generationIndex": generation,
        "relativePath": path.relative_to(root).as_posix(),
        "rawSha256": _sha256_file(path),
        "sizeBytes": path.stat().st_size,
    }


def build_v37_archive_disposition_ledger(*, v37_root: Path, output_dir: Path) -> Mapping[str, Any]:
    """Join the retained five-generation V37 evidence into deterministic reports."""

    root = v37_root.resolve()
    generations_root = root / "run" / "broad-4000x1024x5" / "generations"
    if not generations_root.is_dir():
        raise DispositionLedgerError(f"V37 generations root is absent: {generations_root}")
    generations = sorted(
        (path for path in generations_root.iterdir() if path.is_dir() and path.name.startswith("generation-")),
        key=_generation_index,
    )
    if not generations:
        raise DispositionLedgerError("V37 contains no generation directories")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise DispositionLedgerError(f"output directory must be empty: {output_dir}")

    input_files: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    identity_to_candidate_id: dict[str, str] = {}
    source_rows_by_generation: dict[int, Mapping[str, Any]] = {}
    parent_ids_by_generation: dict[int, set[str]] = {}
    cumulative_rows_by_generation: dict[int, Mapping[str, Any]] = {}
    archive_rows_by_generation: dict[int, Mapping[str, Any]] = {}

    for generation_dir in generations:
        generation = _generation_index(generation_dir)
        member_path = generation_dir / "campaign" / "proposal-current-panel" / "campaign-output" / "evaluated-members.jsonl"
        bundle_path = member_path.with_name("candidate-panel-bundles.jsonl")
        population_path = generation_dir / "proposal" / "evaluation-population.json"
        source_path = generation_dir / "native-finalization" / "source.json"
        cumulative_path = generation_dir / "native-finalization" / "evidence" / "cumulative-archive.json"
        archive_path = generation_dir / "native-finalization" / "archive.json"
        for path in (member_path, bundle_path, population_path, source_path, cumulative_path, archive_path):
            if not path.is_file():
                raise DispositionLedgerError(f"required V37 receipt is absent: {path}")
            input_files.append(_input_file(root, generation, path))

        members = _read_jsonl(member_path)
        current_bundles = _read_jsonl(bundle_path)
        population = _read_json(population_path)
        source = _read_json(source_path)
        cumulative = _read_json(cumulative_path)
        archive = _read_json(archive_path)
        source_rows_by_generation[generation] = source
        cumulative_rows_by_generation[generation] = cumulative
        archive_rows_by_generation[generation] = archive

        cohorts = _mapping(source.get("cohort"), name="source cohort")
        cohort_ids = {_candidate_id(row) for row in _rows(cohorts.get("candidates"), name="cohort candidates")}
        current_panel_id = _text(cohorts.get("panelId"), name="cohort panelId")
        previous = source.get("previousCumulativeArchive")
        previous_required_panel_ids = (
            [_text(panel_id, name="previous required panelId") for panel_id in _mapping(previous, name="previous cumulative archive").get("requiredPanelIds", [])]
            if previous is not None
            else []
        )
        required_panel_ids = sorted(set(previous_required_panel_ids + [current_panel_id]))
        current_bundles_by_id = {_candidate_id(row): row for row in current_bundles}
        if len(current_bundles_by_id) != len(current_bundles) or set(current_bundles_by_id) != {
            _candidate_id(row) for row in members
        }:
            raise DispositionLedgerError(f"G{generation} current-panel bundle/member sets differ")
        source_bundles_by_id: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for bundle in _rows(source.get("candidatePanelBundles"), name="source candidatePanelBundles"):
            source_bundles_by_id[_candidate_id(bundle)].append(bundle)
        provisional = _mapping(source.get("provisional"), name="source provisional")
        provisional_rows = _rows(provisional.get("candidates"), name="provisional candidates")
        provisional_by_id = {_candidate_id(row): row for row in provisional_rows}
        selected = _mapping(source.get("selectedRichMembers"), name="selected rich members")
        rich_by_id = {_candidate_id(row): row for row in _rows(selected.get("members"), name="selected rich member rows")}
        if set(provisional_by_id) != set(rich_by_id):
            raise DispositionLedgerError(f"G{generation} provisional and rich-member sets differ")

        cumulative_by_id = {
            _candidate_id(row): row for row in _rows(cumulative.get("members"), name="cumulative members")
        }
        if set(provisional_by_id) != set(cumulative_by_id):
            raise DispositionLedgerError(f"G{generation} cumulative and provisional member sets differ")
        quality_ids = set(cumulative.get("qualityCandidateIds", []))
        frontier_ids = set(cumulative.get("frontierCandidateIds", []))
        if quality_ids & frontier_ids:
            raise DispositionLedgerError(f"G{generation} native breeder lanes overlap")
        parent_by_id = {
            _candidate_id(member): member
            for cell in _rows(archive.get("cells"), name="archive cells")
            for member in _rows(cell.get("members"), name="archive cell members")
        }
        parent_ids_by_generation[generation] = set(parent_by_id)

        accepted_funnel = {
            _text(_mapping(row.get("candidate"), name="funnel candidate").get("candidateId"), name="funnel candidateId"): row
            for row in _rows(population.get("funnelEntries"), name="funnel entries")
            if row.get("disposition") == "accepted"
        }
        if len(members) != len(accepted_funnel):
            raise DispositionLedgerError(f"G{generation} evaluated/member funnel counts differ")

        retained_ids = {
            _text(candidate_id, name="retained parent candidateId")
            for candidate_id in cohorts.get("retainedParentEvaluationCandidateIds", [])
        }
        retained_members_by_id: dict[str, Mapping[str, Any]] = {}
        if retained_ids:
            retained_member_path = (
                generation_dir
                / "campaign"
                / "fast-prefinalizer"
                / "round-0000"
                / "task-0000"
                / "campaign-output"
                / "evaluated-members.jsonl"
            )
            retained_bundle_path = retained_member_path.with_name("candidate-panel-bundles.jsonl")
            for path in (retained_member_path, retained_bundle_path):
                if not path.is_file():
                    raise DispositionLedgerError(f"G{generation} retained-parent receipt is absent: {path}")
                input_files.append(_input_file(root, generation, path))
            retained_members = _read_jsonl(retained_member_path)
            retained_bundles = _read_jsonl(retained_bundle_path)
            retained_members_by_id = {_candidate_id(row): row for row in retained_members}
            retained_bundles_by_id = {_candidate_id(row): row for row in retained_bundles}
            if set(retained_members_by_id) != retained_ids or set(retained_bundles_by_id) != retained_ids:
                raise DispositionLedgerError(f"G{generation} retained-parent receipt IDs differ from source cohort")
            for candidate_id, bundle in retained_bundles_by_id.items():
                if candidate_id not in source_bundles_by_id:
                    source_bundles_by_id[candidate_id] = []
                if not any(
                    _text(existing.get("panelId"), name="source bundle panelId")
                    == _text(bundle.get("panelId"), name="retained bundle panelId")
                    for existing in source_bundles_by_id[candidate_id]
                ):
                    raise DispositionLedgerError(
                        f"G{generation} retained-parent bundle is absent from finalizer source: {candidate_id}"
                    )

        def append_state(
            *,
            member: Mapping[str, Any],
            evaluation_state_kind: str,
            funnel_entry: Mapping[str, Any] | None,
        ) -> None:
            candidate_id = _candidate_id(member)
            if candidate_id not in cohort_ids:
                raise DispositionLedgerError(f"G{generation} candidate lacks cohort receipt: {candidate_id}")
            if evaluation_state_kind == "proposal_current_panel" and funnel_entry is None:
                raise DispositionLedgerError(f"G{generation} proposal candidate lacks accepted funnel receipt: {candidate_id}")
            candidate = _mapping(member.get("candidate"), name="evaluated candidate")
            aggregate = _mapping(member.get("aggregate"), name="evaluated aggregate")
            identity = _text(candidate.get("candidateIdentitySha256"), name="candidate identity")
            if identity in identity_to_candidate_id and identity_to_candidate_id[identity] != candidate_id:
                raise DispositionLedgerError(f"candidate identity maps to multiple IDs: {identity}")
            identity_to_candidate_id[identity] = candidate_id

            provisional_row = provisional_by_id.get(candidate_id)
            cumulative_row = cumulative_by_id.get(candidate_id)
            archive_row = parent_by_id.get(candidate_id)
            current = {
                "afterCostNetR": _number(aggregate.get("totalConservativeNetR"), name="total conservative net"),
                "noCostNetR": _number(aggregate.get("totalNoCostNetR"), name="total no-cost net"),
                "costDragR": _number(aggregate.get("costDragR"), name="cost drag"),
                "totalTrades": _number(aggregate.get("totalTrades"), name="total trades"),
                "finiteSupportEligible": bool(_mapping(member.get("finiteDataValidity"), name="finite data validity").get("passesSupportGate")),
                "validForQuality": bool(_mapping(member.get("finiteDataValidity"), name="finite data validity").get("validForQuality")),
                "cellId": _mapping(member.get("descriptor"), name="descriptor").get("cellId"),
                "objectives": dict(_mapping(member.get("objectives"), name="objectives")),
                "windows": _current_window_summary(aggregate),
            }
            (
                terminal_reason,
                first_terminal_stage,
                all_reason_codes,
                secondary_failures,
                cumulative_detail,
            ) = _terminal_disposition(
                provisional_row=provisional_row,
                cumulative_row=cumulative_row,
                archive_row=archive_row,
                quality_ids=quality_ids,
                frontier_ids=frontier_ids,
                candidate_id=candidate_id,
            )
            state_bundles = source_bundles_by_id.get(candidate_id, []) if provisional_row else [
                current_bundles_by_id[candidate_id]
            ]
            coverage, coverage_sha256, counterfactual_eligibility = _bundle_coverage(
                bundles=state_bundles,
                current_panel_id=current_panel_id,
                required_panel_ids=required_panel_ids,
            )
            evaluation_sha256 = _canonical_sha256(member)

            row = {
                "schemaVersion": SCHEMA_VERSION,
                "generationIndex": generation,
                "candidateId": candidate_id,
                "evaluationStateKind": evaluation_state_kind,
                "evaluationRecordCanonicalSha256": evaluation_sha256,
                "evidenceCoverage": coverage,
                "coverageCanonicalSha256": coverage_sha256,
                "evaluationStateSha256": _state_identity(
                    generation=generation,
                    candidate_id=candidate_id,
                    evaluation_sha256=evaluation_sha256,
                    coverage_sha256=coverage_sha256,
                ),
                "counterfactualEligibility": counterfactual_eligibility,
                "candidateIdentitySha256": identity,
                "programSha256": candidate.get("programSha256"),
                "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
                "origin": {
                    "sourceMode": candidate.get("sourceMode"),
                    "structuralOperations": _last_operations(candidate),
                    "parentCandidateIdentitySha256s": _identity_from_history(candidate),
                },
                "funnel": (
                    {
                        "disposition": funnel_entry.get("disposition"),
                        "originKind": funnel_entry.get("originKind"),
                        "staticReachability": _mapping(_mapping(funnel_entry.get("funnelCandidate"), name="funnel candidate").get("staticReachability"), name="static reachability").get("outcome"),
                        "nativeValidation": _mapping(_mapping(funnel_entry.get("funnelCandidate"), name="funnel candidate").get("nativeValidation"), name="native validation").get("outcome"),
                    }
                    if funnel_entry is not None
                    else None
                ),
                "currentPanel": current,
                "prefinalizer": {
                    "provisionalSelected": provisional_row is not None,
                    "selectionRole": "retained_parent_mandatory"
                    if evaluation_state_kind == "retained_parent_current_panel"
                    else "newcomer_cell_balanced",
                    "currentPanelRank": provisional_row.get("currentPanelRank") if provisional_row else None,
                    "novelty": provisional_row.get("novelty") if provisional_row else None,
                    "selectionCellId": provisional_row.get("cellId") if provisional_row else None,
                },
                "cumulative": cumulative_detail,
                "parentArchive": {
                    "admitted": archive_row is not None,
                    "archiveLane": archive_row.get("archiveLane") if archive_row else None,
                    "retentionReason": archive_row.get("retentionReason") if archive_row else None,
                    "cellId": _mapping(archive_row.get("descriptor"), name="archive descriptor").get("cellId") if archive_row else None,
                },
                "resolvedExecutionDeduplication": "unavailable_in_retained_native_fast_ephemeral_outputs",
                "terminalReason": terminal_reason,
                "firstTerminalStage": first_terminal_stage,
                "allReasonCodesAtThatStage": all_reason_codes,
                "secondaryDiagnosticFailures": secondary_failures,
            }
            all_rows.append(row)

        for member in members:
            append_state(
                member=member,
                evaluation_state_kind="proposal_current_panel",
                funnel_entry=accepted_funnel.get(_candidate_id(member)),
            )
        for candidate_id in sorted(retained_ids):
            append_state(
                member=retained_members_by_id[candidate_id],
                evaluation_state_kind="retained_parent_current_panel",
                funnel_entry=None,
            )

    continuity_checks: list[dict[str, Any]] = []
    for generation in sorted(source_rows_by_generation):
        if generation == min(source_rows_by_generation):
            continue
        source = source_rows_by_generation[generation]
        historical_cumulative = cumulative_rows_by_generation[generation - 1]
        historical_parent = archive_rows_by_generation[generation - 1]
        source_cumulative = _mapping(
            source.get("previousCumulativeArchive"), name="previous cumulative archive"
        )
        source_parent = _mapping(
            source.get("previousParentArchiveSummary"), name="previous parent archive summary"
        )
        expected_parent_summary = {
            "schemaVersion": source_parent.get("schemaVersion"),
            "archiveSha256": historical_parent.get("archiveSha256"),
            "bidirectionalPairPolicy": historical_parent.get("bidirectionalPairPolicy"),
            "candidateCountSeen": historical_parent.get("candidateCountSeen"),
            "cellIds": sorted(
                _text(cell.get("cellId"), name="historical parent archive cellId")
                for cell in _rows(historical_parent.get("cells"), name="historical parent archive cells")
            ),
            "memberCount": historical_parent.get("memberCount"),
        }
        summary_without_hash = {
            key: value for key, value in source_parent.items() if key != "summarySha256"
        }
        checks = {
            "previousCumulativeArchiveExactCanonicalMatch": _canonical_sha256(source_cumulative)
            == _canonical_sha256(historical_cumulative),
            "previousCumulativeArchiveSha256Match": source_cumulative.get("archiveSha256")
            == historical_cumulative.get("archiveSha256"),
            "previousParentArchiveProjectionExactCanonicalMatch": _canonical_sha256(
                summary_without_hash
            )
            == _canonical_sha256(expected_parent_summary),
            "previousParentArchiveSummarySelfHashMatch": source_parent.get("summarySha256")
            == _canonical_sha256(summary_without_hash),
            "previousParentArchiveSha256Match": source_parent.get("archiveSha256")
            == historical_parent.get("archiveSha256"),
        }
        if not all(checks.values()):
            raise DispositionLedgerError(
                f"G{generation} frozen prior state does not match G{generation - 1} historical output"
            )
        continuity_checks.append(
            {
                "generationIndex": generation,
                "previousGenerationIndex": generation - 1,
                "historicalCumulativeArchiveSha256": historical_cumulative.get("archiveSha256"),
                "historicalParentArchiveSha256": historical_parent.get("archiveSha256"),
                "previousParentArchiveSummarySha256": source_parent.get("summarySha256"),
                "checks": checks,
                "passed": True,
            }
        )

    # Link accepted offspring back to their recorded parent identity only after
    # the entire current-panel corpus is indexed.
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for row in all_rows:
        if row["evaluationStateKind"] != "proposal_current_panel":
            continue
        for parent_identity in row["origin"]["parentCandidateIdentitySha256s"]:
            parent_id = identity_to_candidate_id.get(parent_identity)
            if parent_id is not None:
                children_by_parent[parent_id].append(row["candidateId"])
    for row in all_rows:
        candidate_id = row["candidateId"]
        later_reevaluations = [
            generation
            for generation, source in source_rows_by_generation.items()
            if candidate_id in set(_mapping(source["cohort"], name="cohort").get("retainedParentEvaluationCandidateIds", []))
        ]
        row["downstream"] = {
            "retainedParentReevaluationGenerations": sorted(later_reevaluations),
            "acceptedOffspringCandidateIds": sorted(children_by_parent.get(candidate_id, [])),
        }
        row["focusLabels"] = (
            _focus_labels(row) if row["evaluationStateKind"] == "proposal_current_panel" else []
        )

    all_rows.sort(
        key=lambda row: (
            row["generationIndex"],
            row["candidateId"],
            row["evaluationStateKind"],
        )
    )
    proposal_rows = [row for row in all_rows if row["evaluationStateKind"] == "proposal_current_panel"]
    retained_rows = [row for row in all_rows if row["evaluationStateKind"] == "retained_parent_current_panel"]
    focused = [row for row in proposal_rows if row["focusLabels"]]
    focus_label_counts = Counter(label for row in proposal_rows for label in row["focusLabels"])
    lineage_rollups: list[dict[str, Any]] = []
    for candidate_id in sorted({row["candidateId"] for row in all_rows}):
        states = [row for row in all_rows if row["candidateId"] == candidate_id]
        lineage_rollups.append(
            {
                "schemaVersion": SCHEMA_VERSION,
                "candidateId": candidate_id,
                "candidateIdentitySha256": states[0]["candidateIdentitySha256"],
                "evaluationStateCount": len(states),
                "evaluationStates": [
                    {
                        "coverageCanonicalSha256": row["coverageCanonicalSha256"],
                        "evaluationRecordCanonicalSha256": row["evaluationRecordCanonicalSha256"],
                        "evaluationStateKind": row["evaluationStateKind"],
                        "evaluationStateSha256": row["evaluationStateSha256"],
                        "generationIndex": row["generationIndex"],
                    }
                    for row in states
                ],
                "observedParentCandidateIdentitySha256s": sorted(
                    {
                        parent
                        for row in states
                        for parent in row["origin"]["parentCandidateIdentitySha256s"]
                    }
                ),
                "observedAcceptedOffspringCandidateIds": sorted(children_by_parent.get(candidate_id, [])),
                "observedParentArchiveAdmissionGenerations": sorted(
                    row["generationIndex"]
                    for row in states
                    if row["parentArchive"]["admitted"]
                ),
            }
        )
    terminals = Counter(row["terminalReason"] for row in all_rows)
    by_generation = {
        str(generation): {
            "evaluationStateCount": sum(1 for row in all_rows if row["generationIndex"] == generation),
            "proposalCurrentPanelCandidateCount": sum(
                1
                for row in proposal_rows
                if row["generationIndex"] == generation
            ),
            "retainedParentReevaluationStateCount": sum(
                1
                for row in retained_rows
                if row["generationIndex"] == generation
            ),
            "terminalReasons": dict(sorted(Counter(row["terminalReason"] for row in all_rows if row["generationIndex"] == generation).items())),
            "parentArchiveMemberCount": len(parent_ids_by_generation[generation]),
        }
        for generation in sorted(source_rows_by_generation)
    }
    summary = {
        "schemaVersion": SCHEMA_VERSION,
        "status": "read_only_reconciled",
        "v37Root": str(root),
        "evaluationStateCount": len(all_rows),
        "proposalCurrentPanelCandidateCount": len(proposal_rows),
        "retainedParentReevaluationStateCount": len(retained_rows),
        "candidateLineageRollupCount": len(lineage_rollups),
        "focusedProposalStateUnionCount": len(focused),
        "focusedProposalStateLabelCounts": dict(sorted(focus_label_counts.items())),
        "terminalReasonCounts": dict(sorted(terminals.items())),
        "crossGenerationControlContinuity": continuity_checks,
        "byGeneration": by_generation,
        "knownUnavailable": [
            "The retained fast-ephemeral native outputs do not contain per-candidate resolved-execution-deduplication provenance.",
            "The ledger does not infer a deduplication loss from matching program identities; it records that stage as unavailable.",
            "Only accepted current-panel offspring are linked to parents here. Rejected and no-op construction attempts remain proposal-funnel evidence, not evaluated candidates.",
        ],
        "inputFiles": sorted(input_files, key=lambda row: (row["generationIndex"], row["relativePath"])),
    }
    _write_jsonl(output_dir / "candidate-disposition-ledger.jsonl", all_rows)
    _write_jsonl(output_dir / "candidate-lineage-rollup.jsonl", lineage_rollups)
    _write_jsonl(output_dir / "focused-cohort-ledger.jsonl", focused)
    _write_json(output_dir / "cross-generation-control-continuity.json", continuity_checks)
    _write_json(output_dir / "summary.json", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v37-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    summary = build_v37_archive_disposition_ledger(
        v37_root=args.v37_root,
        output_dir=args.output_dir,
    )
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
