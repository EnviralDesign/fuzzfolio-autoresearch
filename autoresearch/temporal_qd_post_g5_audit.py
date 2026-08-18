"""Compact post-G5 audit and continuation gate for temporal QD runs."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError


AUDIT_SCHEMA = "temporal_qd_post_g5_audit_v1"
LADDER_SUMMARY_SCHEMA = "temporal_qd_v5_evidence_ladder_summary_v1"
DECISION_HOLD = "hold_for_evidence_ladder"
DECISION_STOP = "stop"
DECISION_PROCEED = "proceed_to_g9"


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def _read_self_hashed(path: Path, *, field: str, name: str) -> dict[str, Any]:
    value = _read(path, name=name)
    supplied = value.get(field)
    body = dict(value)
    body.pop(field, None)
    if not isinstance(supplied, str) or canonical_sha256(body) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity drifted")
    return value


def _run_root(path: Path | str) -> Path:
    root = Path(path).resolve()
    if (root / "state.json").is_file() and (root / "config.json").is_file():
        return root
    matches = list(root.glob("run/*/state.json"))
    if len(matches) != 1:
        raise TemporalDiscoveryContractError("run root does not contain one supervisor state")
    return matches[0].parent.resolve()


def _archive_path(root: Path, generation: int, record: Mapping[str, Any]) -> Path:
    supplied = record.get("archivePath")
    if isinstance(supplied, str) and Path(supplied).is_absolute():
        path = Path(supplied).resolve()
        if path.is_file():
            return path
    generation_root = root / "generations" / f"generation-{generation:04d}"
    candidates = sorted(generation_root.glob("**/archive.json"))
    if len(candidates) != 1:
        raise TemporalDiscoveryContractError(
            f"generation {generation} does not contain one final archive"
        )
    return candidates[0].resolve()


def _proposal_result(root: Path, generation: int) -> dict[str, Any]:
    generation_root = root / "generations" / f"generation-{generation:04d}"
    matches = sorted(generation_root.glob("proposal/**/v5-proposal-result.json"))
    if len(matches) != 1:
        raise TemporalDiscoveryContractError(
            f"generation {generation} does not contain one proposal result"
        )
    result = _read(matches[0], name=f"generation {generation} proposal result")
    supplied = result.get("resultSha256")
    body = dict(result)
    body.pop("resultSha256", None)
    if not isinstance(supplied, str) or canonical_sha256(body) != supplied:
        raise TemporalDiscoveryContractError(
            f"generation {generation} proposal result identity drifted"
        )
    return result


def _members(archive: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    cells = archive.get("cells")
    if not isinstance(cells, list):
        raise TemporalDiscoveryContractError("archive cells are invalid")
    output: list[Mapping[str, Any]] = []
    for cell in cells:
        if not isinstance(cell, Mapping) or not isinstance(cell.get("members"), list):
            raise TemporalDiscoveryContractError("archive cell is malformed")
        for member in cell["members"]:
            if not isinstance(member, Mapping):
                raise TemporalDiscoveryContractError("archive member is malformed")
            output.append(member)
    return output


def _finite_numeric_tree(value: object) -> bool:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if isinstance(value, Mapping):
        return bool(value) and all(_finite_numeric_tree(item) for item in value.values())
    if isinstance(value, list):
        return bool(value) and all(_finite_numeric_tree(item) for item in value)
    return False


def _source_mode(member: Mapping[str, Any]) -> str:
    candidate = member.get("candidate")
    value = candidate.get("sourceMode") if isinstance(candidate, Mapping) else None
    if not isinstance(value, str):
        aggregate = member.get("aggregate")
        value = aggregate.get("sourceMode") if isinstance(aggregate, Mapping) else None
    return value if isinstance(value, str) else "unknown"


def audit_temporal_qd_g5(
    run_root: Path | str,
    *,
    ladder_summary_path: Path | str | None = None,
) -> dict[str, Any]:
    """Audit a completed five-generation run and make the G6–G9 continuation decision."""

    root = _run_root(run_root)
    config = _read_self_hashed(
        root / "config.json", field="configSha256", name="QD config"
    )
    state = _read_self_hashed(root / "state.json", field="stateSha256", name="QD state")
    plan = config.get("generationPlan")
    records = state.get("completedGenerations")
    if not isinstance(plan, Mapping) or not isinstance(records, list):
        raise TemporalDiscoveryContractError("run lacks generation plan or records")
    expected_last = plan.get("lastGenerationIndex")
    if expected_last != 5:
        raise TemporalDiscoveryContractError("post-G5 audit accepts only an exact five-generation run")

    hard_failures: list[str] = []
    warnings: list[str] = []
    generations: list[dict[str, Any]] = []
    previous_ids: set[str] | None = None
    low_child_streak = 0
    high_turnover_without_gain = False
    target_candidates = plan.get("targetUniqueCandidatesPerGeneration")
    g0 = config.get("g0Bootstrap")
    g0_pool = g0.get("initialConstructionPoolSize") if isinstance(g0, Mapping) else None
    for generation in range(1, 6):
        matches = [
            record
            for record in records
            if isinstance(record, Mapping) and record.get("generationIndex") == generation
        ]
        if len(matches) != 1:
            hard_failures.append(f"generation {generation} record count is {len(matches)}")
            continue
        record = matches[0]
        record_body = dict(record)
        record_sha = record_body.pop("generationRecordSha256", None)
        if not isinstance(record_sha, str) or canonical_sha256(record_body) != record_sha:
            hard_failures.append(f"generation {generation} record identity drifted")
        proposal = _proposal_result(root, generation)
        if proposal.get("selectedEvaluationCandidateCount") != target_candidates:
            hard_failures.append(
                f"generation {generation} selected candidate count drifted"
            )
        if record.get("candidateCount") != target_candidates:
            hard_failures.append(f"generation {generation} record candidate count drifted")
        if generation == 1 and proposal.get("attemptCount") != g0_pool:
            hard_failures.append("generation 1 construction pool count drifted")
        archive = _read(
            _archive_path(root, generation, record),
            name=f"generation {generation} archive",
        )
        supplied_sha = archive.get("archiveSha256")
        body = dict(archive)
        body.pop("archiveSha256", None)
        if not isinstance(supplied_sha, str) or canonical_sha256(body) != supplied_sha:
            hard_failures.append(f"generation {generation} archive identity drifted")
        if archive.get("generationIndex") != generation:
            hard_failures.append(f"generation {generation} archive generation drifted")
        if record.get("archiveSha256") != supplied_sha:
            hard_failures.append(f"generation {generation} record/archive binding drifted")
        members = _members(archive)
        ids = [member.get("candidateId") for member in members]
        valid_ids = [value for value in ids if isinstance(value, str) and value]
        if len(valid_ids) != len(members) or len(set(valid_ids)) != len(valid_ids):
            hard_failures.append(f"generation {generation} has invalid or duplicate members")
        cell_capacity = archive.get("cellCapacity")
        for cell in archive.get("cells") or []:
            if (
                isinstance(cell_capacity, int)
                and isinstance(cell, Mapping)
                and isinstance(cell.get("members"), list)
                and len(cell["members"]) > cell_capacity
            ):
                hard_failures.append(f"generation {generation} exceeds cell capacity")
        if archive.get("memberCount") != len(members):
            hard_failures.append(f"generation {generation} member count drifted")
        if archive.get("occupiedCellCount") != len(archive.get("cells") or []):
            hard_failures.append(f"generation {generation} occupied-cell count drifted")
        if not all(
            _finite_numeric_tree(member.get("objectives"))
            and _finite_numeric_tree(member.get("robustObjectives"))
            for member in members
        ):
            hard_failures.append(f"generation {generation} has non-finite objectives")

        current_ids = set(valid_ids)
        retained = len(current_ids & previous_ids) if previous_ids is not None else 0
        added = len(current_ids - previous_ids) if previous_ids is not None else len(current_ids)
        turnover = (
            1.0 - retained / max(len(previous_ids), len(current_ids), 1)
            if previous_ids is not None
            else 1.0
        )
        child_count = sum(
            1
            for member in members
            if any(token in _source_mode(member) for token in ("offspring", "mutation", "crossover"))
        )
        child_share = child_count / max(len(members), 1)
        low_child_streak = low_child_streak + 1 if child_share < 0.10 and generation > 1 else 0
        if generation > 1 and turnover > 0.90 and int(archive.get("newCellCount") or 0) == 0:
            high_turnover_without_gain = True
        source_modes: dict[str, int] = {}
        for member in members:
            mode = _source_mode(member)
            source_modes[mode] = source_modes.get(mode, 0) + 1
        generations.append(
            {
                "generationIndex": generation,
                "evaluatedCandidateCount": record.get("candidateCount"),
                "proposalAttemptCount": proposal.get("attemptCount"),
                "acceptedCandidateCount": proposal.get("acceptedCandidateCount"),
                "selectedEvaluationCandidateCount": proposal.get(
                    "selectedEvaluationCandidateCount"
                ),
                "proposalTimings": proposal.get("timings"),
                "taskCount": record.get("totalGenerationTaskCount"),
                "memberCount": len(members),
                "occupiedCellCount": archive.get("occupiedCellCount"),
                "newCellCount": archive.get("newCellCount"),
                "qualityMemberCount": archive.get("qualityMemberCount"),
                "observationalMemberCount": archive.get("observationalMemberCount"),
                "negativeNoveltyMemberCount": archive.get("negativeNoveltyMemberCount"),
                "retainedMemberCount": retained,
                "addedMemberCount": added,
                "turnover": turnover,
                "genuineChildShare": child_share,
                "sourceModes": source_modes,
                "archiveSha256": supplied_sha,
            }
        )
        previous_ids = current_ids

    if len(records) != 5:
        hard_failures.append(f"completed generation count is {len(records)}, expected 5")
    if state.get("status") != "completed":
        hard_failures.append("supervisor state is not completed")
    if low_child_streak >= 2:
        warnings.append("genuine child contribution stayed below 10% for two generations")
    if high_turnover_without_gain:
        warnings.append("archive turnover exceeded 90% without new-cell gain")

    ladder: dict[str, Any] | None = None
    run_quality_audit: dict[str, Any] | None = None
    run_quality_path = root / "quality-audit" / "run-quality-audit.json"
    if run_quality_path.is_file():
        loaded = _read(run_quality_path, name="run quality audit")
        supplied = loaded.get("auditSha256")
        body = dict(loaded)
        body.pop("auditSha256", None)
        if isinstance(supplied, str) and canonical_sha256(body) == supplied:
            run_quality_audit = {
                "schemaVersion": loaded.get("schemaVersion"),
                "auditSha256": supplied,
                "generationCount": loaded.get("generationCount"),
                "generationSummaries": loaded.get("generationSummaries"),
                "crossGenerationTrends": loaded.get("crossGenerationTrends"),
            }
    decision = DECISION_STOP if hard_failures else DECISION_HOLD
    reasons = list(hard_failures)
    if not hard_failures and ladder_summary_path is not None:
        ladder = _read_self_hashed(
            Path(ladder_summary_path).resolve(),
            field="summarySha256",
            name="evidence-ladder summary",
        )
        expected_ladder_fields = {
            "schemaVersion",
            "configSha256",
            "sourceGenerationIndex",
            "sourceArchiveSha256",
            "ladderAuthoritySha256",
            "validationTailAuthoritySha256",
            "scrutinyTailAuthoritySha256",
            "commonPanelHypervolumeGainPercent",
            "confidenceIntervalsExcludeZero",
            "summarySha256",
        }
        final_archive_sha = generations[-1]["archiveSha256"] if generations else None
        if (
            set(ladder) != expected_ladder_fields
            or ladder.get("schemaVersion") != LADDER_SUMMARY_SCHEMA
            or ladder.get("configSha256") != config.get("configSha256")
            or ladder.get("sourceGenerationIndex") != 5
            or ladder.get("sourceArchiveSha256") != final_archive_sha
        ):
            raise TemporalDiscoveryContractError(
                "evidence-ladder summary is not bound to this completed G5 run"
            )
        for field in (
            "sourceArchiveSha256",
            "ladderAuthoritySha256",
            "validationTailAuthoritySha256",
            "scrutinyTailAuthoritySha256",
        ):
            value = ladder.get(field)
            if (
                not isinstance(value, str)
                or not value.startswith("sha256:")
                or len(value) != 71
            ):
                raise TemporalDiscoveryContractError(
                    f"evidence-ladder summary {field} is invalid"
                )
        gains = ladder.get("commonPanelHypervolumeGainPercent")
        if (
            not isinstance(gains, list)
            or len(gains) < 2
            or not all(
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
                for value in gains
            )
        ):
            raise TemporalDiscoveryContractError("ladder summary lacks common-panel gains")
        confidence = ladder.get("confidenceIntervalsExcludeZero")
        if confidence is not True:
            reasons.append("common-panel gain is not statistically separated from zero")
            decision = DECISION_STOP
        elif len(gains) >= 2 and all(float(value) < 1.0 for value in gains[-2:]):
            reasons.append("common-panel gain stayed below 1% for two generations")
            decision = DECISION_STOP
        elif low_child_streak >= 2 or high_turnover_without_gain:
            reasons.extend(warnings)
            decision = DECISION_STOP
        else:
            decision = DECISION_PROCEED
    elif not hard_failures:
        reasons.append("native validation/scrutiny ladder summary is required before G6–G9")

    result: dict[str, Any] = {
        "schemaVersion": AUDIT_SCHEMA,
        "runRoot": str(root),
        "generationCount": len(generations),
        "generations": generations,
        "hardFailures": hard_failures,
        "warnings": warnings,
        "decision": decision,
        "decisionReasons": reasons,
        "ladderSummary": ladder,
        "runQualityAudit": run_quality_audit,
    }
    result["auditSha256"] = canonical_sha256(result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_root", type=Path)
    parser.add_argument("--ladder-summary", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    result = audit_temporal_qd_g5(
        args.run_root, ladder_summary_path=args.ladder_summary
    )
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8", newline="\n")
    print(encoded, end="")
    return 0 if result["decision"] == DECISION_PROCEED else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["AUDIT_SCHEMA", "LADDER_SUMMARY_SCHEMA", "audit_temporal_qd_g5"]
