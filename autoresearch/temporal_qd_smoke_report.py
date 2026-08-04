"""Build a compact diversity, timing, and resource report for a QD smoke."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


SMOKE_REPORT_SCHEMA = "temporal_qd_construction_smoke_report_v1"


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object at {path}")
    return value


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _counter_increment(target: dict[str, int], value: Any) -> None:
    if isinstance(value, (Mapping, list, tuple)):
        key = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    else:
        key = str(value)
    target[key] = target.get(key, 0) + 1


def _empty_side_distribution() -> dict[str, Any]:
    return {
        "moduleCount": 0,
        "seedNameCounts": {},
        "evidenceGroupCounts": {},
        "eventBindingCounts": {},
        "holdKindCounts": {},
        "plannedGrammarDepthCounts": {},
        "appliedGrammarDepthCounts": {},
        "grammarOperationFamilyCounts": {},
        "plannedIndicatorDepthCounts": {},
        "appliedIndicatorDepthCounts": {},
        "indicatorOperatorCounts": {},
        "indicatorConstructionKindCounts": {},
        "indicatorCountCounts": {},
        "evidenceGroupMemberShapeCounts": {},
    }


def _empty_construction_distribution() -> dict[str, Any]:
    return {
        "proposalCount": 0,
        "sides": {
            "long": _empty_side_distribution(),
            "short": _empty_side_distribution(),
        },
    }


def _add_construction_audit(
    target: dict[str, Any], entry: Mapping[str, Any]
) -> bool:
    proposal = entry.get("proposal")
    audit = (
        proposal.get("factoryConstructionAudit")
        if isinstance(proposal, Mapping)
        else None
    )
    modules = audit.get("sides") if isinstance(audit, Mapping) else None
    if not isinstance(modules, Mapping):
        return False
    target["proposalCount"] += 1
    for direction in ("long", "short"):
        module = modules.get(direction)
        if not isinstance(module, Mapping):
            continue
        side = target["sides"][direction]
        side["moduleCount"] += 1
        selector = (
            module.get("selector")
            if isinstance(module.get("selector"), Mapping)
            else {}
        )
        _counter_increment(side["seedNameCounts"], selector.get("seedName"))
        _counter_increment(side["evidenceGroupCounts"], selector.get("groupId"))
        _counter_increment(side["eventBindingCounts"], selector.get("eventId"))
        grammar = (
            module.get("grammar")
            if isinstance(module.get("grammar"), Mapping)
            else {}
        )
        indicator = (
            module.get("indicator")
            if isinstance(module.get("indicator"), Mapping)
            else {}
        )
        shape = (
            module.get("profileShape")
            if isinstance(module.get("profileShape"), Mapping)
            else {}
        )
        _counter_increment(side["holdKindCounts"], shape.get("holdKind"))
        _counter_increment(
            side["plannedGrammarDepthCounts"], grammar.get("plannedDepth")
        )
        _counter_increment(
            side["appliedGrammarDepthCounts"], grammar.get("appliedDepth")
        )
        for step in grammar.get("steps") or []:
            if isinstance(step, Mapping):
                _counter_increment(
                    side["grammarOperationFamilyCounts"],
                    step.get("operationFamily"),
                )
        _counter_increment(
            side["plannedIndicatorDepthCounts"], indicator.get("plannedDepth")
        )
        _counter_increment(
            side["appliedIndicatorDepthCounts"], indicator.get("appliedDepth")
        )
        for step in indicator.get("steps") or []:
            if isinstance(step, Mapping):
                _counter_increment(
                    side["indicatorOperatorCounts"], step.get("operatorId")
                )
                _counter_increment(
                    side["indicatorConstructionKindCounts"],
                    step.get("constructionKind"),
                )
        _counter_increment(
            side["indicatorCountCounts"], shape.get("indicatorCount")
        )
        _counter_increment(
            side["evidenceGroupMemberShapeCounts"],
            shape.get("evidenceGroupMemberCounts") or [],
        )
    return True


def _sort_construction_distribution(value: dict[str, Any]) -> None:
    for side in value["sides"].values():
        for key, counts in list(side.items()):
            if isinstance(counts, dict):
                side[key] = dict(sorted(counts.items()))


def _entry_diversity(root: Path) -> dict[str, Any]:
    dispositions: dict[str, int] = {}
    candidate_identities: set[str] = set()
    pair_genome_identities: set[str] = set()
    accepted_count = 0
    proposal_count = 0
    attempted_distribution = _empty_construction_distribution()
    accepted_distribution = _empty_construction_distribution()
    journal_root = root / "proposal-journal"
    paths = sorted(journal_root.glob("*.json")) if journal_root.exists() else []
    for path in paths:
        entry = _read_json(path)
        proposal_count += 1
        disposition = str(entry.get("disposition") or "unknown")
        dispositions[disposition] = dispositions.get(disposition, 0) + 1
        has_construction_audit = _add_construction_audit(
            attempted_distribution, entry
        )
        if has_construction_audit and disposition == "accepted":
            _add_construction_audit(accepted_distribution, entry)
        if disposition != "accepted":
            continue
        accepted_count += 1
        candidate = entry.get("candidate")
        if not isinstance(candidate, Mapping):
            continue
        candidate_identity = str(candidate.get("candidateIdentitySha256") or "")
        if candidate_identity:
            candidate_identities.add(candidate_identity)
        genome = candidate.get("bidirectionalGenome")
        if not isinstance(genome, Mapping):
            continue
        identities = genome.get("identities")
        if isinstance(identities, Mapping):
            pair_identity = str(identities.get("rawPairSha256") or "")
            if pair_identity:
                pair_genome_identities.add(pair_identity)
    duplicate_count = sum(
        count
        for disposition, count in dispositions.items()
        if disposition.startswith("duplicate_")
    )
    result: dict[str, Any] = {
        "proposalCount": proposal_count,
        "acceptedCount": accepted_count,
        "acceptanceRatio": accepted_count / proposal_count if proposal_count else 0.0,
        "duplicateCount": duplicate_count,
        "duplicateRatio": duplicate_count / proposal_count if proposal_count else 0.0,
        "dispositionCounts": dict(sorted(dispositions.items())),
        "acceptedUniqueCandidateIdentityCount": len(candidate_identities),
        "acceptedUniquePairGenomeCount": len(pair_genome_identities),
    }
    if attempted_distribution["proposalCount"]:
        _sort_construction_distribution(attempted_distribution)
        _sort_construction_distribution(accepted_distribution)
        distribution = {
            "schemaVersion": "temporal_qd_rich_immigrant_distribution_v1",
            "attempted": attempted_distribution,
            "accepted": accepted_distribution,
        }
        distribution["distributionSha256"] = _canonical_sha256(distribution)
        result["immigrantConstructionDistribution"] = distribution
    return result


def _top_phases(
    phase_breakdown: Mapping[str, Any],
    metric: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = []
    for phase, stats in phase_breakdown.items():
        if not isinstance(stats, Mapping):
            continue
        if (
            stats.get("aggregationRole") == "overlapping_total"
            or phase == "proposal.total"
        ):
            continue
        values = stats.get(metric)
        if not isinstance(values, Mapping):
            continue
        total_ns = int(values.get("totalNs") or 0)
        rows.append(
            {
                "phase": str(phase),
                "callCount": int(values.get("count") or 0),
                "totalSeconds": total_ns / 1_000_000_000,
                "meanMilliseconds": int(values.get("meanNs") or 0) / 1_000_000,
                "p95Milliseconds": int(values.get("p95Ns") or 0) / 1_000_000,
                "maxMilliseconds": int(values.get("maxNs") or 0) / 1_000_000,
            }
        )
    rows.sort(key=lambda item: (-item["totalSeconds"], item["phase"]))
    return rows[:limit]


def build_qd_construction_smoke_report(
    generation_root: Path | str,
    *,
    top_phase_limit: int = 20,
) -> dict[str, Any]:
    root = Path(generation_root).resolve()
    if top_phase_limit < 1:
        raise ValueError("top phase limit must be positive")
    summary = _read_json(root / "performance" / "latest-summary.json")
    diversity = _entry_diversity(root)
    journal_path = root / "generation-journal.json"
    journal = _read_json(journal_path) if journal_path.exists() else None
    if journal is not None:
        diversity.update(
            {
                "proposalCount": int(journal["proposalCount"]),
                "acceptedCount": int(journal["acceptedCount"]),
                "acceptanceRatio": (
                    int(journal["acceptedCount"]) / int(journal["proposalCount"])
                    if int(journal["proposalCount"])
                    else 0.0
                ),
                "dispositionCounts": journal["dispositionCounts"],
                "duplicateCounters": journal["duplicateCounters"],
                "uniqueIdentityCounts": journal["uniqueIdentityCounts"],
                "immigrantConstructionDistribution": journal.get(
                    "immigrantConstructionDistribution"
                ),
            }
        )
    phase_breakdown = summary.get("phaseBreakdown") or {}
    wall_seconds = int(summary.get("wallDurationNs") or 0) / 1_000_000_000
    coordinator_cpu_seconds = (
        int(summary.get("coordinatorCpuNs") or 0) / 1_000_000_000
    )
    report: dict[str, Any] = {
        "schemaVersion": SMOKE_REPORT_SCHEMA,
        "generationRoot": str(root),
        "outcome": summary.get("outcome"),
        **(
            {"errorType": summary["errorType"]}
            if summary.get("errorType")
            else {}
        ),
        "result": summary.get("result") or {},
        "diversity": diversity,
        "timing": {
            "wallSeconds": wall_seconds,
            "coordinatorCpuSeconds": coordinator_cpu_seconds,
            "meanCoordinatorCoreEquivalent": (
                coordinator_cpu_seconds / wall_seconds if wall_seconds else 0.0
            ),
            "topExclusiveWallPhases": _top_phases(
                phase_breakdown,
                "exclusiveWall",
                limit=top_phase_limit,
            ),
            "topExclusiveCoordinatorCpuPhases": _top_phases(
                phase_breakdown,
                "coordinatorCpuExclusive",
                limit=top_phase_limit,
            ),
            "cpuAttribution": (
                "coordinator spans use exact process CPU; recursive child CPU and "
                "host pressure are sampled with the active phase context"
            ),
        },
        "resources": summary.get("resources") or {},
        "instrumentation": summary.get("instrumentation") or {},
        "semanticIdentityParticipation": "excluded_observational_artifact",
    }
    report["reportSha256"] = _canonical_sha256(report)
    return report


__all__ = [
    "SMOKE_REPORT_SCHEMA",
    "build_qd_construction_smoke_report",
]
