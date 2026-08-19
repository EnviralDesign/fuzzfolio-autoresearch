"""Compact operator-family heritability report from generation quality audits."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_generation_quality_audit import _generation_root

REPORT_SCHEMA = "temporal_qd_operator_family_heritability_report_v1"
HANDOFF_SCHEMA = "temporal_qd_v37_operator_family_handoff_v1"


def _compact_relative(block: Mapping[str, Any] | None) -> dict[str, Any]:
    block = block or {}
    return {
        "comparisonCount": block.get("comparisonCount"),
        "meanParentRelativeConservativeNetR": block.get(
            "meanParentRelativeConservativeNetR"
        ),
        "medianParentRelativeConservativeNetR": block.get(
            "medianParentRelativeConservativeNetR"
        ),
        "offspringBeatParentCount": block.get("offspringBeatParentCount"),
        "offspringBeatParentRate": block.get("offspringBeatParentRate"),
    }


def _compact_family_row(row: Mapping[str, Any]) -> dict[str, Any]:
    mix = row.get("actionMix") if isinstance(row.get("actionMix"), Mapping) else {}
    return {
        "operatorFamily": row.get("operatorFamily"),
        "constructedCandidateCount": row.get("constructedCandidateCount"),
        "evaluatedCandidateCount": row.get("evaluatedCandidateCount"),
        "finiteSupportEligibleCount": row.get("finiteSupportEligibleCount"),
        "currentPanelQualityLikeCount": row.get("currentPanelQualityLikeCount"),
        "currentPanelNetPositiveCount": row.get("currentPanelNetPositiveCount"),
        "currentPanelNetPositiveRate": row.get("currentPanelNetPositiveRate"),
        "meanCurrentPanelConservativeNetR": row.get("meanCurrentPanelConservativeNetR"),
        "medianCurrentPanelConservativeNetR": row.get(
            "medianCurrentPanelConservativeNetR"
        ),
        "previousGenerationParentRelative": _compact_relative(
            row.get("previousGenerationParentRelative")
            if isinstance(row.get("previousGenerationParentRelative"), Mapping)
            else None
        ),
        "samePanelParentRelative": _compact_relative(
            row.get("samePanelParentRelative")
            if isinstance(row.get("samePanelParentRelative"), Mapping)
            else None
        ),
        "medianManagementActionShare": mix.get("medianManagementActionShare"),
        "meanManagementActionShare": mix.get("meanManagementActionShare"),
        "meanTotalTrades": mix.get("meanTotalTrades"),
        "meanCostDragR": mix.get("meanCostDragR"),
    }


def _compact_origin_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "originKind": row.get("originKind"),
        "constructedCandidateCount": row.get("constructedCandidateCount"),
        "evaluatedCandidateCount": row.get("evaluatedCandidateCount"),
        "finiteSupportEligibleCount": row.get("finiteSupportEligibleCount"),
        "currentPanelQualityLikeCount": row.get("currentPanelQualityLikeCount"),
        "meanCurrentPanelConservativeNetR": row.get("meanCurrentPanelConservativeNetR"),
        "archiveRetainedCount": row.get("archiveRetainedCount"),
    }


def _compact_parent_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "parentCandidateId": row.get("parentCandidateId"),
        "offspringConstructedCount": row.get("offspringConstructedCount"),
        "offspringEvaluatedCount": row.get("offspringEvaluatedCount"),
        "samePanelParentComparisonAvailable": row.get(
            "samePanelParentComparisonAvailable"
        ),
    }


def build_operator_family_report(
    run_root: Path | str, *, through_generation: int = 5
) -> dict[str, Any]:
    root = Path(run_root).resolve()
    generations: list[dict[str, Any]] = []
    for index in range(1, through_generation + 1):
        path = (
            _generation_root(root, index)
            / "quality-audit"
            / "generation-quality-audit.json"
        )
        if not path.is_file():
            continue
        audit = json.loads(path.read_text(encoding="utf-8"))
        families = [
            _compact_family_row(row)
            for row in (audit.get("operatorFamilyYield") or [])
            if isinstance(row, Mapping)
        ]
        generations.append(
            {
                "generationIndex": index,
                "auditSha256": audit.get("auditSha256"),
                "evaluatedCandidateCount": (audit.get("evaluation") or {}).get(
                    "evaluatedCandidateCount"
                ),
                "archiveMemberCount": (audit.get("archive") or {}).get("memberCount"),
                "finiteSupportEligibleCount": (
                    audit.get("cumulativeQualification") or {}
                ).get("finiteSupportEligibleCount"),
                "currentPanelQualityLikeCount": (
                    audit.get("cumulativeQualification") or {}
                ).get("currentPanelQualityLikeCount"),
                "originYield": [
                    _compact_origin_row(row)
                    for row in (audit.get("originYield") or [])
                    if isinstance(row, Mapping)
                ],
                "operatorFamilyYield": families,
                "parentYield": [
                    _compact_parent_row(row)
                    for row in (audit.get("parentYield") or [])
                    if isinstance(row, Mapping)
                ],
            }
        )
    body = {
        "schemaVersion": REPORT_SCHEMA,
        "runRoot": str(root),
        "generationCount": len(generations),
        "generations": generations,
    }
    body["reportSha256"] = canonical_sha256(
        {key: value for key, value in body.items() if key != "reportSha256"}
    )
    return body


def render_pro_agent_handoff(report: Mapping[str, Any]) -> str:
    lines = [
        "# V37 operator-family heritability handoff",
        "",
        "This is observational evidence from the completed V37 run after quality-audit",
        "repairs. It is **not** a balanced parent × operator matrix and it is **not**",
        "permission to launch another 1024×5 campaign.",
        "",
        f"Report schema: `{report.get('schemaVersion')}`",
        f"Report SHA: `{report.get('reportSha256')}`",
        f"Generations present: {report.get('generationCount')}",
        "",
        "## What this answers",
        "",
        "- Which terminal operator family produced V37 children (`hold`, `resource`,",
        "  `topology`, `temporal`, `initial_protection`, `crossover`).",
        "- Parent-relative conservative net R using the previous generation's panel",
        "  (same-panel parent evals are typically 0).",
        "- Management-action share from realized `actionCounts`",
        "  (tighten / break-even / trailing vs enter+exit).",
        "",
        "## What this does not answer",
        "",
        "- Exact clones as controls.",
        "- Balanced 32-children-per-family allocation.",
        "- One-change-at-a-time variants.",
        "- Independent-panel survival of a child.",
        "- Why one G2 parent had zero accepted kids (V37 has no attempt sidecar).",
        "",
        "## Generation tables",
        "",
    ]
    for generation in report.get("generations") or []:
        if not isinstance(generation, Mapping):
            continue
        index = generation.get("generationIndex")
        lines.append(
            f"### G{index}  archive={generation.get('archiveMemberCount')}  "
            f"entering-support={generation.get('finiteSupportEligibleCount')}  "
            f"entering-quality-like={generation.get('currentPanelQualityLikeCount')}"
        )
        lines.append("")
        lines.append(
            "| Family | N | Support | Quality-like | Net+ | Mean net R | "
            "Prev ΔR | Beat parent | Mgmt share | Mean trades |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for row in generation.get("operatorFamilyYield") or []:
            if not isinstance(row, Mapping):
                continue
            prev = row.get("previousGenerationParentRelative") or {}
            mean_delta = prev.get("meanParentRelativeConservativeNetR")
            beat = prev.get("offspringBeatParentRate")
            mean_net = row.get("meanCurrentPanelConservativeNetR")
            mgmt = row.get("medianManagementActionShare")
            trades = row.get("meanTotalTrades")

            def _fmt(value: Any, digits: int = 3) -> str:
                if value is None:
                    return "—"
                if isinstance(value, float):
                    return f"{value:.{digits}f}"
                return str(value)

            lines.append(
                "| {family} | {n} | {support} | {quality} | {netpos} | {mean_net} | "
                "{delta} | {beat} | {mgmt} | {trades} |".format(
                    family=row.get("operatorFamily"),
                    n=row.get("constructedCandidateCount"),
                    support=row.get("finiteSupportEligibleCount"),
                    quality=row.get("currentPanelQualityLikeCount"),
                    netpos=row.get("currentPanelNetPositiveCount"),
                    mean_net=_fmt(mean_net),
                    delta=_fmt(mean_delta),
                    beat=_fmt(beat, 4),
                    mgmt=_fmt(mgmt),
                    trades=_fmt(trades, 1),
                )
            )
        parents = generation.get("parentYield") or []
        if parents:
            lines.append("")
            lines.append("Parents with accepted children:")
            for parent in parents:
                if not isinstance(parent, Mapping):
                    continue
                lines.append(
                    f"- `{parent.get('parentCandidateId')}`: "
                    f"{parent.get('offspringConstructedCount')} constructed / "
                    f"{parent.get('offspringEvaluatedCount')} evaluated"
                )
        lines.append("")
    lines.extend(
        [
            "## Recommended next experiment (still not a full re-run)",
            "",
            "Freeze V37 G2 archive parents plus one inactive and one active-negative",
            "control. For each parent produce 32 children per operator family, plus",
            "exact clones. Evaluate on one frozen panel. Success is at least one family",
            "with nonnegative median parent-relative net R or a repeatable positive tail,",
            "without systematically worse worst-window risk.",
            "",
            "Do not change breeding quotas, archive gates, or management one-shot/rearm",
            "as an economic fix until that matrix exists.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--through-generation", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--handoff-markdown", type=Path)
    args = parser.parse_args(argv)
    report = build_operator_family_report(
        args.run_root, through_generation=args.through_generation
    )
    encoded = canonical_json(report) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8", newline="\n")
    if args.handoff_markdown is not None:
        args.handoff_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.handoff_markdown.write_text(
            render_pro_agent_handoff(report), encoding="utf-8", newline="\n"
        )
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "REPORT_SCHEMA",
    "build_operator_family_report",
    "render_pro_agent_handoff",
]
