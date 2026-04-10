"""Build a non-holdout opening-step field-grounding benchmark slice."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION
from .normalize_state import build_prompt_variants


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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as sink:
        for row in rows:
            sink.write(json.dumps(row, ensure_ascii=True) + "\n")


def _sort_key(row: dict[str, Any]) -> tuple[str, str, int, str]:
    return (
        str(row.get("split_hint") or ""),
        str(row.get("run_id") or ""),
        int(row.get("step") or 0),
        str(row.get("example_id") or ""),
    )


def _first_action(target: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(target, dict):
        return None
    actions = target.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    return first if isinstance(first, dict) else None


def _normalize_opening_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if int(row.get("step") or 0) != 1:
        return None
    target = (
        row.get("target_response_normalized")
        if isinstance(row.get("target_response_normalized"), dict)
        else row.get("target_response")
    )
    if not isinstance(target, dict):
        return None
    actions = target.get("actions")
    if not isinstance(actions, list) or len(actions) != 1:
        return None
    first = _first_action(target)
    if not isinstance(first, dict):
        return None
    if str(first.get("tool") or "") != "prepare_profile":
        return None
    if str(first.get("mode") or "") != "scaffold_from_seed":
        return None
    instruments = [str(item).strip() for item in list(first.get("instruments") or []) if str(item).strip()]
    candidate_name = str(first.get("candidate_name") or "").strip()
    if not instruments or not candidate_name:
        return None
    prompt_state = row.get("prompt_state") if isinstance(row.get("prompt_state"), dict) else None
    if not isinstance(prompt_state, dict):
        return None
    opening_grounding = {
        "allowed_seed_instruments": instruments,
        "preferred_initial_instruments": instruments,
        "preferred_initial_instrument_rule": "benchmark_reference",
        "candidate_name_hint": candidate_name or "cand1",
    }
    prompt_state = dict(prompt_state)
    prompt_state["opening_grounding"] = opening_grounding
    record = dict(row)
    record["prompt_state"] = prompt_state
    prompt_variants = build_prompt_variants(record)
    record["prompt_state_full"] = prompt_variants["full"]
    record["prompt_state_compact"] = prompt_variants["compact"]
    record["prompt_state_compact_v2"] = prompt_variants["compact_v2"]
    record["opening_grounding_expected"] = {
        "instruments": instruments,
        "candidate_name": candidate_name or "cand1",
        "forbidden_instruments": ["ALL", "__BASKET__"],
    }
    record["benchmark_focus"] = "opening_grounding"
    return record


def build_opening_grounding_benchmark(
    input_paths: list[Path],
    output_path: Path,
    *,
    summary_path: Path | None = None,
    limit: int = 12,
) -> dict[str, Any]:
    counts = Counter()
    selected: list[dict[str, Any]] = []
    seen_examples: set[str] = set()
    seen_runs: set[str] = set()
    for input_path in input_paths:
        for row in _load_jsonl(input_path):
            counts["rows_seen"] += 1
            example_id = str(row.get("example_id") or "")
            if example_id in seen_examples:
                counts["duplicate_example"] += 1
                continue
            normalized = _normalize_opening_row(row)
            if normalized is None:
                counts["not_opening_grounding_candidate"] += 1
                continue
            run_id = str(normalized.get("run_id") or "")
            if run_id in seen_runs and len(selected) >= limit:
                counts["run_limit_skipped"] += 1
                continue
            seen_examples.add(example_id)
            seen_runs.add(run_id)
            selected.append(normalized)
            counts["rows_selected"] += 1
            counts[f"split:{normalized.get('split_hint') or 'unknown'}"] += 1
            if len(selected) >= limit:
                break
        if len(selected) >= limit:
            break
    selected.sort(key=_sort_key)
    _write_jsonl(output_path, selected)
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_paths": [str(path.resolve()) for path in input_paths],
        "output_path": str(output_path.resolve()),
        "limit": limit,
        "rows_emitted": len(selected),
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
        description="Build a non-holdout opening-step grounding benchmark."
    )
    parser.add_argument(
        "--input",
        type=Path,
        action="append",
        required=True,
        help="Input split JSONL path. Repeatable.",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--limit", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    build_opening_grounding_benchmark(
        args.input,
        args.out,
        summary_path=args.summary_out,
        limit=int(args.limit),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
