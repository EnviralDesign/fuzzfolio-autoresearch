"""Build a focused opening-step scaffold slice from split examples."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def build_opening_scaffold_slice(
    input_paths: list[Path],
    output_path: Path,
    *,
    summary_path: Path | None = None,
    required_grades: set[str] | None = None,
    required_step: int = 1,
    required_tool: str = "prepare_profile",
    required_mode: str = "scaffold_from_seed",
    exact_action_count: int | None = None,
    strip_first_action_fields: set[str] | None = None,
    rewrite_reasoning_mode: str = "none",
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    counts = Counter()
    per_input_counts: dict[str, int] = {}

    for input_path in input_paths:
        selected_from_input = 0
        for row in _load_jsonl(input_path):
            counts["rows_seen"] += 1
            if required_grades is not None:
                grade = str((row.get("quality_labels") or {}).get("grade") or "")
                if grade not in required_grades:
                    counts["grade_mismatch"] += 1
                    continue
            if int(row.get("step") or 0) != required_step:
                counts["step_mismatch"] += 1
                continue
            prompt_state = row.get("prompt_state_compact_v2")
            if not isinstance(prompt_state, dict):
                counts["missing_compact_v2"] += 1
                continue
            target = row.get("target_response_normalized")
            actions = target.get("actions") if isinstance(target, dict) else None
            if exact_action_count is not None:
                if not isinstance(actions, list) or len(actions) != exact_action_count:
                    counts["action_count_mismatch"] += 1
                    continue
            first = (
                actions[0]
                if isinstance(actions, list) and actions and isinstance(actions[0], dict)
                else None
            )
            if not isinstance(first, dict):
                counts["missing_first_action"] += 1
                continue
            if str(first.get("tool") or "") != required_tool:
                counts["tool_mismatch"] += 1
                continue
            if str(first.get("mode") or "") != required_mode:
                counts["mode_mismatch"] += 1
                continue
            record = dict(row)
            if isinstance(target, dict) and isinstance(actions, list):
                normalized_target = dict(target)
                normalized_actions = list(actions)
                normalized_first = dict(first)
                if strip_first_action_fields is not None:
                    normalized_first = {
                        key: value
                        for key, value in normalized_first.items()
                        if key in strip_first_action_fields
                    }
                    counts["first_action_fields_stripped"] += 1
                normalized_actions[0] = normalized_first
                normalized_target["actions"] = normalized_actions
                if rewrite_reasoning_mode == "opening_minimal":
                    normalized_target["reasoning"] = (
                        "Fresh run opening step. Create one seed-guided candidate scaffold now so it can be validated next."
                    )
                    counts["reasoning_rewritten"] += 1
                record["target_response_normalized"] = normalized_target
                record["target_actions"] = normalized_actions
                record["targetreasoningshort"] = str(normalized_target.get("reasoning") or "")[:240]
            rows.append(record)
            counts["rows_selected"] += 1
            selected_from_input += 1
        per_input_counts[str(input_path.resolve())] = selected_from_input

    rows.sort(
        key=lambda row: (
            str(row.get("run_id") or ""),
            int(row.get("step") or 0),
            str(row.get("example_id") or ""),
        )
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as sink:
        for row in rows:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_paths": [str(path.resolve()) for path in input_paths],
        "output_path": str(output_path.resolve()),
        "required_grades": sorted(required_grades) if required_grades is not None else None,
        "required_step": required_step,
        "required_tool": required_tool,
        "required_mode": required_mode,
        "exact_action_count": exact_action_count,
        "strip_first_action_fields": sorted(strip_first_action_fields) if strip_first_action_fields is not None else None,
        "rewrite_reasoning_mode": rewrite_reasoning_mode,
        "rows_emitted": len(rows),
        "per_input_counts": per_input_counts,
        "counts": dict(counts),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a focused opening-step scaffold slice."
    )
    parser.add_argument(
        "--input",
        type=Path,
        action="append",
        required=True,
        help="Input split JSONL path. Repeatable.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output JSONL path.")
    parser.add_argument("--summary-out", type=Path, help="Optional summary JSON path.")
    parser.add_argument(
        "--required-grades",
        default="A",
        help="Comma-separated quality grades, or ANY.",
    )
    parser.add_argument("--required-step", type=int, default=1, help="Required step number.")
    parser.add_argument(
        "--required-tool",
        default="prepare_profile",
        help="Required first action tool.",
    )
    parser.add_argument(
        "--required-mode",
        default="scaffold_from_seed",
        help="Required first action mode.",
    )
    parser.add_argument(
        "--exact-action-count",
        type=int,
        help="Optional exact number of actions required in the target response.",
    )
    parser.add_argument(
        "--strip-first-action-fields",
        help="Optional comma-separated allowlist of first-action fields to keep.",
    )
    parser.add_argument(
        "--rewrite-reasoning-mode",
        choices=("none", "opening_minimal"),
        default="none",
        help="Optional deterministic reasoning rewrite mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    required_grades: set[str] | None
    raw_grades = str(args.required_grades or "").strip()
    if not raw_grades or raw_grades.upper() == "ANY":
        required_grades = None
    else:
        required_grades = {
            item.strip() for item in raw_grades.split(",") if item.strip()
        } or None
    strip_first_action_fields: set[str] | None
    raw_fields = str(args.strip_first_action_fields or "").strip()
    if not raw_fields:
        strip_first_action_fields = None
    else:
        strip_first_action_fields = {
            item.strip() for item in raw_fields.split(",") if item.strip()
        } or None
    build_opening_scaffold_slice(
        args.input,
        args.out,
        summary_path=args.summary_out,
        required_grades=required_grades,
        required_step=int(args.required_step),
        required_tool=str(args.required_tool),
        required_mode=str(args.required_mode),
        exact_action_count=args.exact_action_count,
        strip_first_action_fields=strip_first_action_fields,
        rewrite_reasoning_mode=str(args.rewrite_reasoning_mode),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
