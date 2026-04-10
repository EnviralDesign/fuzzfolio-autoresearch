"""Aggregate deterministic labels into auditable scoring reports."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


def _first_tool(record: dict[str, Any]) -> str:
    actions = record.get("target_actions")
    if not isinstance(actions, list) or not actions:
        return "none"
    first = actions[0]
    if not isinstance(first, dict):
        return "unknown"
    return str(first.get("tool") or "unknown")


def score_examples(
    input_path: Path,
    output_path: Path,
    summary_path: Path | None = None,
    markdown_path: Path | None = None,
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    keep_phase = Counter()
    keep_tool = Counter()
    rejection_reasons = Counter()
    recovery_classes = Counter()
    grade_counts = Counter()

    with input_path.open("r", encoding="utf-8") as source, output_path.open(
        "w", encoding="utf-8"
    ) as sink:
        for line in source:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                continue
            quality = (
                record.get("quality_labels")
                if isinstance(record.get("quality_labels"), dict)
                else {}
            )
            grade = str(quality.get("grade") or "unknown")
            grade_counts[grade] += 1
            counts["records"] += 1
            if quality.get("keep_for_base_sft"):
                counts["keep_for_base_sft"] += 1
                keep_phase[str(record.get("phase") or "unknown")] += 1
                keep_tool[_first_tool(record)] += 1
            if quality.get("keep_for_recovery"):
                counts["keep_for_recovery"] += 1
            for item in record.get("rejection_reasons") or []:
                rejection_reasons[str(item)] += 1
            recovery = (
                record.get("recovery_labels")
                if isinstance(record.get("recovery_labels"), dict)
                else {}
            )
            for item in recovery.get("failure_classes") or []:
                recovery_classes[str(item)] += 1
            sink.write(json.dumps(record, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "counts": dict(counts),
        "grade_counts": dict(grade_counts),
        "keep_phase_counts": dict(keep_phase),
        "keep_tool_counts": dict(keep_tool),
        "rejection_reason_counts": dict(rejection_reasons),
        "recovery_failure_class_counts": dict(recovery_classes),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Dataset Scoring Report",
            "",
            f"- Records: {counts['records']}",
            f"- Keep for base SFT: {counts['keep_for_base_sft']}",
            f"- Keep for recovery: {counts['keep_for_recovery']}",
            "",
            "## Grades",
            "",
        ]
        for grade, value in sorted(grade_counts.items()):
            lines.append(f"- {grade}: {value}")
        lines.extend(
            [
                "",
                "## Base SFT Tool Distribution",
                "",
            ]
        )
        for tool, value in keep_tool.most_common():
            lines.append(f"- {tool}: {value}")
        lines.extend(
            [
                "",
                "## Top Rejection Reasons",
                "",
            ]
        )
        for reason, value in rejection_reasons.most_common(10):
            lines.append(f"- {reason}: {value}")
        lines.extend(
            [
                "",
                "## Top Recovery Classes",
                "",
            ]
        )
        for reason, value in recovery_classes.most_common(10):
            lines.append(f"- {reason}: {value}")
        markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate scored training examples.")
    parser.add_argument("--input", type=Path, required=True, help="Input labeled JSONL.")
    parser.add_argument("--out", type=Path, required=True, help="Output scored JSONL.")
    parser.add_argument("--summary-out", type=Path, help="Optional JSON summary path.")
    parser.add_argument("--markdown-out", type=Path, help="Optional markdown report path.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    score_examples(args.input, args.out, args.summary_out, args.markdown_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
