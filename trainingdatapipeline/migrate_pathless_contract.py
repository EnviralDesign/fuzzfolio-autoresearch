"""Migrate normalized/final benchmark records to the pathless live contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .normalize_state import (
    _canonicalize_value,
    _pathless_model_value,
    build_prompt_variants,
)


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


def migrate_record(record: dict[str, Any]) -> dict[str, Any]:
    prompt_state = (
        record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    )
    prompt_state = _pathless_model_value(prompt_state)
    run_info = prompt_state.get("run") if isinstance(prompt_state.get("run"), dict) else {}
    run_dir = str(run_info.get("run_dir") or "")

    normalized = dict(record)
    normalized["prompt_state"] = prompt_state
    normalized["target_response"] = _pathless_model_value(record.get("target_response"))
    normalized["target_actions"] = _pathless_model_value(record.get("target_actions"))
    normalized["action_signatures"] = _pathless_model_value(record.get("action_signatures"))
    for key in (
        "prior_action_summary",
        "current_result_facts",
        "tool_results_summary",
        "manager_events",
        "trace_event_facts",
    ):
        if key in record:
            normalized[key] = _pathless_model_value(record.get(key))
    prompt_variants = build_prompt_variants({"prompt_state": prompt_state})
    normalized["prompt_state_full"] = prompt_variants["full"]
    normalized["prompt_state_compact"] = prompt_variants["compact"]
    normalized["prompt_state_compact_v2"] = prompt_variants["compact_v2"]
    normalized["target_response_normalized"] = _canonicalize_value(
        _pathless_model_value(record.get("target_response_normalized") or record.get("target_response")),
        run_dir,
    )
    normalized["target_actions_normalized"] = _canonicalize_value(
        _pathless_model_value(record.get("target_actions_normalized") or record.get("target_actions")),
        run_dir,
    )
    normalized["action_signatures_normalized"] = _canonicalize_value(
        _pathless_model_value(record.get("action_signatures_normalized") or record.get("action_signatures")),
        run_dir,
    )
    return normalized


def migrate_pathless_contract(input_path: Path, output_path: Path) -> int:
    rows = [migrate_record(row) for row in _load_jsonl(input_path)]
    _write_jsonl(output_path, rows)
    return len(rows)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate normalized/final benchmark records to the pathless contract."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    migrate_pathless_contract(args.input, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
