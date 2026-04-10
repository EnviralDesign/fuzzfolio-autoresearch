"""Render a compact markdown report from pipeline summaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def export_summary_report(
    *,
    raw_summary_path: Path | None,
    scored_summary_path: Path | None,
    split_manifest_path: Path | None,
    out_path: Path,
) -> None:
    raw_summary = _load_json(raw_summary_path)
    scored_summary = _load_json(scored_summary_path)
    split_manifest = _load_json(split_manifest_path)

    lines = [
        "# Training Pipeline Summary",
        "",
    ]

    if raw_summary:
        lines.extend(
            [
                "## Extraction",
                "",
                f"- Processed runs: {raw_summary.get('processed_runs', 0)}",
                f"- Extracted steps: {raw_summary.get('extracted_steps', 0)}",
            ]
        )
        marker_counts = raw_summary.get("marker_counts")
        if isinstance(marker_counts, dict):
            lines.append("- Key markers:")
            for key in (
                "contains_run_cli",
                "contains_typed_tool",
                "hard_action_failure",
                "response_repair_triggered",
                "step_guard_triggered",
                "response_guard_blocked",
                "timeframe_mismatch_present",
            ):
                if key in marker_counts:
                    lines.append(f"  - {key}: {marker_counts[key]}")
        lines.append("")

    if scored_summary:
        counts = scored_summary.get("counts") if isinstance(scored_summary.get("counts"), dict) else {}
        lines.extend(
            [
                "## Scoring",
                "",
                f"- Records: {counts.get('records', 0)}",
                f"- Keep for base SFT: {counts.get('keep_for_base_sft', 0)}",
                f"- Keep for recovery: {counts.get('keep_for_recovery', 0)}",
                "",
                "### Base SFT Tool Distribution",
                "",
            ]
        )
        keep_tools = scored_summary.get("keep_tool_counts")
        if isinstance(keep_tools, dict):
            for key, value in sorted(
                keep_tools.items(), key=lambda item: (-int(item[1]), str(item[0]))
            ):
                lines.append(f"- {key}: {value}")
        lines.extend(["", "### Top Recovery Classes", ""])
        recovery = scored_summary.get("recovery_failure_class_counts")
        if isinstance(recovery, dict):
            for key, value in sorted(
                recovery.items(), key=lambda item: (-int(item[1]), str(item[0]))
            )[:10]:
                lines.append(f"- {key}: {value}")
        lines.append("")

    if split_manifest:
        lines.extend(["## Splits", ""])
        split_counts = split_manifest.get("split_record_counts")
        if isinstance(split_counts, dict):
            for key, value in split_counts.items():
                lines.append(f"- {key}: {value}")
        split_run_counts = split_manifest.get("split_run_counts")
        if isinstance(split_run_counts, dict):
            lines.append("")
            lines.append("### Run Counts")
            lines.append("")
            for key, value in split_run_counts.items():
                lines.append(f"- {key}: {value}")
        recovery_runs = split_manifest.get("recovery_holdout_runs")
        if isinstance(recovery_runs, list):
            lines.append("")
            lines.append(f"- Recovery holdout runs: {len(recovery_runs)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a compact training pipeline report.")
    parser.add_argument("--raw-summary", type=Path, help="Raw extraction summary JSON.")
    parser.add_argument("--scored-summary", type=Path, help="Scored summary JSON.")
    parser.add_argument("--split-manifest", type=Path, help="Split manifest JSON.")
    parser.add_argument("--out", type=Path, required=True, help="Output markdown path.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    export_summary_report(
        raw_summary_path=args.raw_summary,
        scored_summary_path=args.scored_summary,
        split_manifest_path=args.split_manifest,
        out_path=args.out,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
