"""Build a compact diversity, timing, and resource report for a QD smoke."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


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


def _proposal_entries(root: Path) -> list[dict[str, Any]]:
    journal_root = root / "proposal-journal"
    if not journal_root.exists():
        return []
    return [_read_json(path) for path in sorted(journal_root.glob("*.json"))]


def _entry_diversity(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    dispositions: dict[str, int] = {}
    candidate_identities: set[str] = set()
    pair_genome_identities: set[str] = set()
    accepted_count = 0
    for entry in entries:
        disposition = str(entry.get("disposition") or "unknown")
        dispositions[disposition] = dispositions.get(disposition, 0) + 1
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
    proposal_count = len(entries)
    duplicate_count = sum(
        count
        for disposition, count in dispositions.items()
        if disposition.startswith("duplicate_")
    )
    return {
        "proposalCount": proposal_count,
        "acceptedCount": accepted_count,
        "acceptanceRatio": accepted_count / proposal_count if proposal_count else 0.0,
        "duplicateCount": duplicate_count,
        "duplicateRatio": duplicate_count / proposal_count if proposal_count else 0.0,
        "dispositionCounts": dict(sorted(dispositions.items())),
        "acceptedUniqueCandidateIdentityCount": len(candidate_identities),
        "acceptedUniquePairGenomeCount": len(pair_genome_identities),
    }


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
    entries = _proposal_entries(root)
    diversity = _entry_diversity(entries)
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
