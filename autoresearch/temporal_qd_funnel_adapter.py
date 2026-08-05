"""QD-owned adapters for the immutable generation funnel.

The funnel reducer is intentionally generic.  This module is the one narrow
place that understands QD's proposal journal, frozen task matrix, immutable
result blobs, and completed archive.  It never selects archive members.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .result_codec import ResultCodecError, read_json_object
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_generation_funnel import (
    GenerationFunnelContractError,
    build_generation_funnel_artifact,
    proposal_attempt_from_journal_entry,
)

QD_FUNNEL_ADAPTER_SCHEMA = "temporal_qd_generation_funnel_adapter_v1"
QD_PROPOSAL_FUNNEL_STAGE_SCHEMA = "temporal_qd_proposal_funnel_stage_v1"


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def _rows(value: Any, *, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(row, Mapping) for row in value):
        raise TemporalDiscoveryContractError(f"{name} must be an object array")
    return list(value)


def _integer(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TemporalDiscoveryContractError(f"{name} must be a non-negative integer")
    return value


def _funnel_candidate(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    value = entry.get("funnelCandidate")
    if value is None:
        # The enabled journal version is intentionally required.  Guessing
        # earlier stage outcomes from an accepted population would turn later
        # success into fabricated historical evidence.
        return None
    row = _mapping(value, name="QD journal funnelCandidate")
    if row.get("schemaVersion") != QD_PROPOSAL_FUNNEL_STAGE_SCHEMA:
        raise TemporalDiscoveryContractError("QD journal funnel candidate schema is unsupported")
    candidate_id = row.get("candidateId")
    raw_sha = row.get("rawSourceProfileSha256")
    if not isinstance(candidate_id, str) or not candidate_id or not isinstance(raw_sha, str) or not raw_sha:
        raise TemporalDiscoveryContractError("QD journal funnel candidate identity is incomplete")
    return dict(row)


def _stage_records(entries: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    proposals: list[dict[str, Any]] = []
    static: list[dict[str, Any]] = []
    native: list[dict[str, Any]] = []
    admissions: list[dict[str, Any]] = []
    for entry in entries:
        attempt = proposal_attempt_from_journal_entry(entry)
        attempted_candidate = attempt.get("candidateId")
        candidate = _funnel_candidate(entry)
        if candidate is None:
            if attempted_candidate is not None:
                raise TemporalDiscoveryContractError(
                    "materialized QD proposal lacks its required funnel stage projection"
                )
            continue
        if (
            attempted_candidate != candidate["candidateId"]
            or attempt.get("rawSourceProfileSha256")
            != candidate["rawSourceProfileSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD funnel stage projection disagrees with proposal-journal authority"
            )
        base = {
            "candidateId": candidate["candidateId"],
            "rawSourceProfileSha256": candidate["rawSourceProfileSha256"],
        }
        proposal = dict(base)
        # Preserve the native identities in the proposal journal projection so
        # every later record joins the same immutable candidate binding.
        proposals.append(proposal)
        static_row = _mapping(candidate.get("staticReachability"), name="QD funnel static stage")
        static.append({**base, **dict(static_row)})
        if static_row.get("outcome") == "rejected":
            continue
        native_row = _mapping(candidate.get("nativeValidation"), name="QD funnel native stage")
        native.append({**base, **dict(native_row)})
        if native_row.get("outcome") == "rejected":
            continue
        admission_row = _mapping(candidate.get("admission"), name="QD funnel admission stage")
        admissions.append({**base, **dict(admission_row)})
    return proposals, static, native, admissions


def _planned_windows(authority: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    windows = _rows(authority.get("developmentWindows"), name="QD authority development windows")
    window_by_bounds = {
        (str(row.get("analysisWindowStart")), str(row.get("analysisWindowEnd"))): str(row.get("windowId"))
        for row in windows
    }
    if len(window_by_bounds) != len(windows) or any(not key[0] or not key[1] or not value for key, value in window_by_bounds.items()):
        raise TemporalDiscoveryContractError("QD authority windows are not uniquely bound")
    planned: dict[str, dict[str, Any]] = {}
    for task in _rows(manifest.get("tasks"), name="QD task manifest tasks"):
        task_id = task.get("task_id")
        payload = _mapping(task.get("payload"), name="QD task payload")
        candidate_id = payload.get("candidate_id")
        key = (str(payload.get("analysis_window_start")), str(payload.get("analysis_window_end")))
        window_id = window_by_bounds.get(key)
        if not isinstance(task_id, str) or not task_id or not isinstance(candidate_id, str) or not candidate_id or window_id is None:
            raise TemporalDiscoveryContractError("QD task matrix lacks an exact candidate/window binding")
        if task_id in planned:
            raise TemporalDiscoveryContractError("QD task matrix task identity is duplicated")
        planned[task_id] = {"candidateId": candidate_id, "windowId": window_id, "payload": payload}
    return planned


def _result_behavior(material: Mapping[str, Any], *, result_sha: str, window_id: str) -> dict[str, Any]:
    try:
        replay = _mapping(
            _mapping(material.get("cost_view_results"), name="QD result cost views").get("research_conservative"),
            name="QD conservative cost view",
        ).get("replay_result")
        replay = _mapping(replay, name="QD conservative replay")
    except TemporalDiscoveryContractError:
        raise
    traces = replay.get("executionTraces") or []
    trades = replay.get("trades") or []
    if not isinstance(traces, list) or not isinstance(trades, list):
        raise TemporalDiscoveryContractError("QD result behavior traces are invalid")
    scheduled = accepted = rejected = canceled = changed = 0
    for trace in traces:
        if not isinstance(trace, Mapping):
            raise TemporalDiscoveryContractError("QD execution trace is invalid")
        status = str(trace.get("status") or "")
        scheduled += status == "scheduled"
        rejected += status == "rejected"
        canceled += status == "canceled"
        accepted += status in {"filled", "applied", "closed"}
        changed += status in {"filled", "applied", "closed"}
    row = {
        "windowId": window_id,
        "resultSha256": result_sha,
        # A canceled intent was observed at the authoritative execution trace,
        # so it is an activation/attrition event, never dormant evidence.
        "activationCount": scheduled + accepted + rejected + canceled,
        "acceptedIntentOrEffectCount": accepted,
        "rejectedIntentOrEffectCount": rejected + canceled,
        "canceledIntentOrEffectCount": canceled,
        "positionChangeCount": changed,
        "tradeCloseCount": len(trades),
    }
    row["neverActivated"] = not any(int(row[key]) for key in row if key.endswith("Count"))
    return row


def _quality_disposition(
    behaviors: Sequence[Mapping[str, Any]],
    raw_results: Sequence[Mapping[str, Any]],
    *, minimum_total_trades: int,
    minimum_trades_per_window: int,
) -> tuple[str, list[str]]:
    trades = [int(row["tradeCloseCount"]) for row in behaviors]
    finite_economics = True
    conservative_net_returns: list[float] = []
    for result in raw_results:
        try:
            metrics = _mapping(
                _mapping(
                    _mapping(result.get("cost_view_results"), name="QD result cost views").get("research_conservative"),
                    name="QD conservative cost view",
                ).get("replay_result"),
                name="QD conservative replay",
            ).get("metrics")
            for field in ("terminalAdjustedTotalNetR", "terminalAdjustedMaxDrawdownR"):
                value = metrics.get(field)
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                    finite_economics = False
            conservative_net_r = metrics.get("terminalAdjustedTotalNetR")
            if (
                not isinstance(conservative_net_r, bool)
                and isinstance(conservative_net_r, (int, float))
                and math.isfinite(float(conservative_net_r))
            ):
                conservative_net_returns.append(float(conservative_net_r))
        except TemporalDiscoveryContractError:
            finite_economics = False
    reasons: list[str] = []
    if sum(trades) < minimum_total_trades:
        reasons.append("minimum_total_trades")
    if any(value < minimum_trades_per_window for value in trades):
        reasons.append("minimum_trades_per_window")
    if not finite_economics:
        reasons.append("finite_economics")
    # Match the QD archive's worst-window conservative objective.  The no-cost
    # view remains diagnostic evidence only; it must never qualify a candidate
    # whose conservative execution economics are negative in any window.
    if conservative_net_returns and min(conservative_net_returns) < 0.0:
        reasons.append("nonnegative_worst_window_conservative_net_r")
    return ("eligible" if not reasons else "not_eligible"), reasons


def build_qd_generation_funnel(
    *,
    proposal_entries: Sequence[Mapping[str, Any]],
    proposal_accounting: Mapping[str, Any],
    population: Mapping[str, Any],
    authority: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    archive: Mapping[str, Any],
    minimum_total_trades: int = 8,
    minimum_trades_per_window: int = 4,
) -> dict[str, Any]:
    """Reduce a frozen QD generation into the generic immutable funnel.

    A complete checkpoint produces ``evaluated`` plans.  An interrupted matrix
    is represented faithfully as ``partial`` or ``rejected`` and never receives
    activation or archive rows.  The supervisor only publishes this reducer
    after exact completion, but the stricter behavior makes restart/tamper
    tests meaningful and prevents stale rows from looking terminally selected.
    """
    entries = [_mapping(row, name="QD proposal journal entry") for row in proposal_entries]
    # G0 evaluates a deterministic sparse subset of an immutable construction
    # journal.  The funnel's attempt sequence is evaluation-local, while the
    # original construction locator remains bound on every row.
    g0 = population.get("g0Bootstrap")
    if g0 is not None:
        if not isinstance(g0, Mapping) or set(g0) != {"constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256"}:
            raise TemporalDiscoveryContractError("QD G0 funnel bootstrap binding is invalid")
        construction_ordinals: set[int] = set()
        remapped: list[dict[str, Any]] = []
        for local_ordinal, row in enumerate(entries):
            construction_ordinal = row.get("proposalOrdinal")
            if isinstance(construction_ordinal, bool) or not isinstance(construction_ordinal, int) or construction_ordinal in construction_ordinals:
                raise TemporalDiscoveryContractError("QD G0 funnel construction ordinal mapping is invalid")
            construction_ordinals.add(construction_ordinal)
            local = dict(row)
            local["proposalOrdinal"] = local_ordinal
            local["g0ConstructionReference"] = {
                "constructionProposalOrdinal": construction_ordinal,
                "proposalEntrySha256": row.get("entrySha256"),
                **dict(g0),
            }
            remapped.append(local)
        entries = remapped
    proposals, static_rows, native_rows, admission_rows = _stage_records(entries)
    if g0 is not None:
        entry_by_candidate = {
            str(_mapping(row.get("funnelCandidate"), name="G0 funnel candidate").get("candidateId")): row
            for row in entries
            if isinstance(row.get("funnelCandidate"), Mapping)
        }
        for row in admission_rows:
            source = entry_by_candidate.get(str(row.get("candidateId")))
            if source is None:
                raise TemporalDiscoveryContractError("G0 funnel admission lacks selected construction source")
            reference = _mapping(source.get("g0ConstructionReference"), name="G0 construction reference")
            proof = {
                "schemaVersion": "temporal_qd_g0_funnel_proof_v1",
                "candidateId": row["candidateId"],
                "rawSourceProfileSha256": row["rawSourceProfileSha256"],
                **dict(g0),
                "constructionProposalOrdinal": reference["constructionProposalOrdinal"],
                "proposalEntrySha256": reference["proposalEntrySha256"],
                "nativeStaticProofSha256": canonical_sha256({"static": source.get("funnelCandidate", {}).get("staticReachability"), "native": source.get("funnelCandidate", {}).get("nativeValidation")}),
            }
            row["g0BootstrapProof"] = proof
        g0_proof_authority = {
            "schemaVersion": "temporal_qd_g0_funnel_proof_authority_v1",
            "proofs": sorted((dict(row["g0BootstrapProof"]) for row in admission_rows), key=lambda row: str(row["candidateId"])),
        }
        g0_proof_authority["authoritySha256"] = canonical_sha256(g0_proof_authority)
    else:
        g0_proof_authority = None
    attempts = [proposal_attempt_from_journal_entry(entry) for entry in entries]
    if g0 is not None:
        # The generic funnel is intentionally evaluation-local for G0: its
        # attempt ledger describes exactly the sparse selected cohort, while
        # the immutable construction journal remains bound in each proof. Do
        # not present the 64-entry construction accounting beside 32 local
        # attempts, since the generic reducer correctly rejects that mismatch.
        construction_accounting = dict(proposal_accounting)
        proposal_accounting = {
            **construction_accounting,
            "dispositionCounts": {
                key: sum(1 for row in attempts if row["disposition"] == key)
                for key in sorted({str(row["disposition"]) for row in attempts})
            },
            "originProposalCounts": {
                key: sum(1 for row in attempts if row["originKind"] == key)
                for key in sorted({str(row["originKind"]) for row in attempts})
            },
            "g0ConstructionProposalAccounting": construction_accounting,
        }
    planned = _planned_windows(authority, task_manifest)
    completed = _mapping(checkpoint.get("completed"), name="QD evaluation checkpoint completed")
    candidate_results: dict[str, dict[str, tuple[dict[str, Any], Mapping[str, Any]]]] = defaultdict(dict)
    for task_id, record_value in completed.items():
        if task_id not in planned:
            raise TemporalDiscoveryContractError("QD checkpoint has an unplanned task result")
        record = _mapping(record_value, name="QD checkpoint result record")
        path = record.get("resultPath")
        if not isinstance(path, str) or not path:
            raise TemporalDiscoveryContractError("QD checkpoint result path is missing")
        try:
            material, _metadata = read_json_object(Path(path))
        except ResultCodecError as exc:
            raise TemporalDiscoveryContractError("QD immutable result blob is corrupt") from exc
        result_sha = canonical_sha256(material)
        if record.get("resultSha256") != result_sha:
            raise TemporalDiscoveryContractError("QD checkpoint result semantic identity mismatch")
        task = planned[str(task_id)]
        if record.get("candidateId") != task["candidateId"]:
            raise TemporalDiscoveryContractError("QD checkpoint result candidate identity mismatch")
        behavior = _result_behavior(material, result_sha=result_sha, window_id=task["windowId"])
        if task["windowId"] in candidate_results[task["candidateId"]]:
            raise TemporalDiscoveryContractError("QD checkpoint duplicated a candidate/window result")
        candidate_results[task["candidateId"]][task["windowId"]] = (behavior, material)
    expected: dict[str, list[str]] = defaultdict(list)
    for task in planned.values():
        expected[task["candidateId"]].append(task["windowId"])
    archive_members = {
        str(member.get("candidateId")): member
        for cell in _rows(archive.get("cells"), name="QD archive cells")
        for member in _rows(_mapping(cell, name="QD archive cell").get("members"), name="QD archive cell members")
    }
    rotating_proposal_only = isinstance(
        archive.get("rotatingEvidenceTransaction"), Mapping
    )
    duplicate_ids = {
        str(candidate_id)
        for row in _rows(_mapping(archive.get("resolvedExecutionDeduplication") or {"duplicates": []}, name="QD resolved deduplication").get("duplicates"), name="QD resolved duplicates")
        for candidate_id in (_mapping(row, name="QD resolved duplicate").get("discardedCandidateIds") or [])
    }
    evaluation_plans: list[dict[str, Any]] = []
    evaluation_results: list[dict[str, Any]] = []
    activation: list[dict[str, Any]] = []
    retention: list[dict[str, Any]] = []
    admission_ids = {row["candidateId"] for row in admission_rows if row.get("outcome") == "admitted"}
    proposal_by_id = {row["candidateId"]: row for row in proposals}
    population_candidates = _rows(population.get("candidates"), name="QD population candidates")
    if int(population.get("candidateCount") or -1) != len(population_candidates):
        raise TemporalDiscoveryContractError("QD population candidate accounting is invalid")
    population_by_id = {
        str(_mapping(row, name="QD population candidate").get("candidateId") or ""): row
        for row in population_candidates
    }
    if len(population_by_id) != len(population_candidates) or set(population_by_id) != admission_ids:
        raise TemporalDiscoveryContractError(
            "QD funnel admitted journal candidates disagree with frozen population"
        )
    for candidate_id in sorted(admission_ids):
        source_sha = population_by_id[candidate_id].get("sourceProfileSha256")
        if source_sha != proposal_by_id[candidate_id]["rawSourceProfileSha256"]:
            raise TemporalDiscoveryContractError(
                "QD funnel population source identity disagrees with proposal journal"
            )
    for candidate_id in sorted(admission_ids):
        if candidate_id not in expected:
            raise TemporalDiscoveryContractError("admitted QD candidate lacks a planned task matrix")
        base = dict(proposal_by_id[candidate_id])
        candidate_windows = sorted(expected[candidate_id])
        observed = candidate_results.get(candidate_id, {})
        if len(observed) == len(candidate_windows):
            outcome = "evaluated"
        elif observed:
            outcome = "partial"
        else:
            outcome = "rejected"
        canonical_identity = next((row.get("canonicalEvidenceIdentitySha256") for row in admission_rows if row["candidateId"] == candidate_id), None)
        g0_proof = next((row.get("g0BootstrapProof") for row in admission_rows if row["candidateId"] == candidate_id), None)
        evaluation_plans.append({**base, "canonicalEvidenceIdentitySha256": canonical_identity, **({"g0BootstrapProof": g0_proof} if g0_proof is not None else {}), "outcome": outcome, "expectedWindowIds": candidate_windows})
        for window_id, (behavior, _material) in sorted(observed.items()):
            evaluation_results.append({**base, "canonicalEvidenceIdentitySha256": canonical_identity, "windowId": window_id, "resultSha256": behavior["resultSha256"]})
        if outcome != "evaluated":
            continue
        behaviors = [observed[window][0] for window in candidate_windows]
        raw_results = [observed[window][1] for window in candidate_windows]
        quality, reasons = _quality_disposition(behaviors, raw_results, minimum_total_trades=minimum_total_trades, minimum_trades_per_window=minimum_trades_per_window)
        member = archive_members.get(candidate_id)
        if (
            rotating_proposal_only
            and member is not None
            and member.get("archiveLane") in {"quality", "rotating_frontier"}
        ):
            # The rotating transaction has already classified exact cumulative
            # equal-coverage evidence.  This artifact intentionally reports
            # only the proposal campaign, so reapplying legacy current-panel
            # support/worst-window gates here would contradict that authority.
            if "finite_economics" in reasons:
                raise TemporalDiscoveryContractError(
                    "rotating archive retained non-finite proposal evidence"
                )
            quality, reasons = "eligible", []
        evidence = {"schemaVersion": QD_FUNNEL_ADAPTER_SCHEMA, "candidateId": candidate_id, "windows": behaviors}
        activation_outcome = "recorded" if quality == "eligible" else "quality_rejected"
        activation.append({
            **base,
            "canonicalEvidenceIdentitySha256": canonical_identity,
            "outcome": activation_outcome,
            "qualityDisposition": quality,
            "reasons": reasons,
            "actualActivationCount": sum(int(row["activationCount"]) for row in behaviors),
            "acceptedIntentOrEffectCount": sum(int(row["acceptedIntentOrEffectCount"]) for row in behaviors),
            "rejectedIntentOrEffectCount": sum(int(row["rejectedIntentOrEffectCount"]) for row in behaviors),
            "positionChangeCount": sum(int(row["positionChangeCount"]) for row in behaviors),
            "tradeCloseCount": sum(int(row["tradeCloseCount"]) for row in behaviors),
            "neverActivated": all(bool(row["neverActivated"]) for row in behaviors),
            "activationEvidenceSha256": canonical_sha256(evidence),
        })
        # A non-quality candidate may appear in the bounded negative-novelty
        # exploration lane. Preserve that fact explicitly without allowing the
        # generic funnel to treat it as a quality/promotion retention.
        if activation_outcome == "recorded":
            retention.append({
                **base,
                "canonicalEvidenceIdentitySha256": canonical_identity,
                "outcome": "retained" if member is not None else "not_retained",
                "reasons": ([] if member is not None else (["resolved_execution_duplicate"] if candidate_id in duplicate_ids else ["not_selected_by_archive"])),
                **({"archiveMemberIdentitySha256": canonical_sha256(member)} if member is not None else {}),
            })
        else:
            if member is not None and member.get("archiveLane") == "negative_novelty":
                retention.append({
                    **base,
                    "canonicalEvidenceIdentitySha256": canonical_identity,
                    "outcome": "retained",
                    "reasons": ["non_promotable_scheduled_negative_novelty_exploration"],
                    "archiveLane": "negative_novelty",
                    "retentionClassification": "non_promotable_scheduled_exploration",
                    "archiveMemberIdentitySha256": canonical_sha256(member),
                })
    try:
        return build_generation_funnel_artifact(
            proposal_attempt_ledger=attempts,
            proposal_journal=proposals,
            static_reachability_records=static_rows,
            native_validation_records=native_rows,
            admission_records=admission_rows,
            evaluation_plans=evaluation_plans,
            evaluation_results=evaluation_results,
            activation_quality_records=activation,
            archive_retention_records=retention,
            proposal_accounting=proposal_accounting,
            g0_proof_authority=g0_proof_authority,
        )
    except GenerationFunnelContractError as exc:
        raise TemporalDiscoveryContractError(f"QD generation funnel contract failed: {exc}") from exc


__all__ = [
    "QD_FUNNEL_ADAPTER_SCHEMA",
    "QD_PROPOSAL_FUNNEL_STAGE_SCHEMA",
    "build_qd_generation_funnel",
]
