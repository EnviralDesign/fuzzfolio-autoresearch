"""Build a deterministic targeted slice from split examples."""

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


def _parse_quota(items: list[str]) -> dict[str, int]:
    quotas: dict[str, int] = {}
    for item in items:
        tool, _, count_text = str(item).partition(":")
        if not tool or not count_text:
            raise ValueError(f"Invalid quota entry: {item!r}")
        quotas[tool.strip()] = int(count_text)
    return quotas


def build_targeted_slice(
    input_path: Path,
    output_path: Path,
    *,
    quotas: dict[str, int],
    summary_path: Path | None = None,
    exclude_example_ids: set[str] | None = None,
    exclude_run_ids: set[str] | None = None,
    required_grade: str | None = "A",
) -> dict[str, Any]:
    rows = _load_jsonl(input_path)
    exclude_example_ids = exclude_example_ids or set()
    exclude_run_ids = exclude_run_ids or set()

    buckets: dict[str, list[dict[str, Any]]] = {tool: [] for tool in quotas}
    skipped = Counter()
    for row in rows:
        example_id = str(row.get("example_id") or "")
        run_id = str(row.get("run_id") or "")
        if example_id in exclude_example_ids:
            skipped["exclude_example_id"] += 1
            continue
        if run_id in exclude_run_ids:
            skipped["exclude_run_id"] += 1
            continue
        if required_grade is not None and str((row.get("quality_labels") or {}).get("grade") or "") != required_grade:
            skipped["grade_mismatch"] += 1
            continue
        if not isinstance(row.get("prompt_state_compact_v2"), dict):
            skipped["missing_compact_v2"] += 1
            continue
        target = row.get("target_response_normalized")
        actions = target.get("actions") if isinstance(target, dict) else None
        first_action = actions[0] if isinstance(actions, list) and actions and isinstance(actions[0], dict) else None
        tool = str(first_action.get("tool") or "") if isinstance(first_action, dict) else ""
        if tool not in quotas:
            skipped["tool_not_selected"] += 1
            continue
        if str((row.get("policy_labels") or {}).get("deterministic_followup_target") or "") != tool:
            skipped["not_deterministic_match"] += 1
            continue
        buckets[tool].append(row)

    selected: list[dict[str, Any]] = []
    selection_counts = Counter()
    for tool, limit in quotas.items():
        candidates = buckets.get(tool, [])
        candidates.sort(
            key=lambda row: (
                str(row.get("run_id") or ""),
                int(row.get("step") or 0),
                str(row.get("example_id") or ""),
            )
        )
        chosen = candidates[:limit]
        selected.extend(chosen)
        selection_counts[tool] = len(chosen)

    selected.sort(
        key=lambda row: (
            str(row.get("run_id") or ""),
            int(row.get("step") or 0),
            str(row.get("example_id") or ""),
        )
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as sink:
        for row in selected:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "required_grade": required_grade,
        "quotas": quotas,
        "rows_emitted": len(selected),
        "selection_counts": dict(selection_counts),
        "skipped_counts": dict(skipped),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a targeted deterministic slice.")
    parser.add_argument("--input", type=Path, required=True, help="Input split JSONL path.")
    parser.add_argument("--out", type=Path, required=True, help="Output JSONL path.")
    parser.add_argument("--summary-out", type=Path, help="Optional summary JSON path.")
    parser.add_argument(
        "--quota",
        action="append",
        required=True,
        help="Per-tool quota in TOOL:COUNT form. Repeatable.",
    )
    parser.add_argument(
        "--exclude-example-id",
        action="append",
        default=[],
        help="Example id to exclude. Repeatable.",
    )
    parser.add_argument(
        "--exclude-run-id",
        action="append",
        default=[],
        help="Run id to exclude. Repeatable.",
    )
    parser.add_argument("--required-grade", default="A", help="Required quality grade, default A.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_targeted_slice(
        args.input,
        args.out,
        quotas=_parse_quota(args.quota),
        summary_path=args.summary_out,
        exclude_example_ids=set(args.exclude_example_id),
        exclude_run_ids=set(args.exclude_run_id),
        required_grade=args.required_grade,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
