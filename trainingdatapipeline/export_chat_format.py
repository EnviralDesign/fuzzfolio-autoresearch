"""Export scored examples into chat-format SFT records."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from autoresearch.controller_protocol import SFT_SYSTEM_PROTOCOL
from trainingdatapipeline.normalize_state import build_prompt_variants


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


def _validate_target(target: Any) -> dict[str, Any]:
    if not isinstance(target, dict):
        raise ValueError("Target response must be a dict.")
    reasoning = target.get("reasoning")
    actions = target.get("actions")
    if not isinstance(reasoning, str):
        raise ValueError("Target reasoning must be a string.")
    if not isinstance(actions, list):
        raise ValueError("Target actions must be a list.")
    return {"reasoning": reasoning, "actions": actions}


def export_chat_format(
    input_path: Path,
    output_path: Path,
    *,
    prompt_variant: str = "compact",
    include_multi_target: bool = False,
) -> int:
    prompt_key = {
        "full": "prompt_state_full",
        "compact": "prompt_state_compact",
        "compact-v2": "prompt_state_compact_v2",
    }[prompt_variant]
    rows = _load_jsonl(input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as sink:
        for row in rows:
            prompt_state = row.get(prompt_key)
            if not isinstance(prompt_state, dict) and isinstance(row.get("prompt_state"), dict):
                prompt_state = build_prompt_variants(row).get(
                    "compact_v2" if prompt_variant == "compact-v2" else prompt_variant
                )
            if not isinstance(prompt_state, dict):
                continue
            target = _validate_target(row.get("target_response_normalized"))
            payload = {
                "example_id": row.get("example_id"),
                "run_id": row.get("run_id"),
                "split_hint": row.get("split_hint"),
                "messages": [
                    {"role": "system", "content": SFT_SYSTEM_PROTOCOL},
                    {"role": "user", "content": json.dumps(prompt_state, ensure_ascii=True)},
                    {
                        "role": "assistant",
                        "content": json.dumps(target, ensure_ascii=True),
                    },
                ],
                "metadata": {
                    "phase": row.get("phase"),
                    "quality_labels": row.get("quality_labels"),
                    "prompt_variant": prompt_variant,
                    "multi_target": include_multi_target,
                },
            }
            sink.write(json.dumps(payload, ensure_ascii=True) + "\n")
            count += 1
    return count


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export training examples in chat format.")
    parser.add_argument("--input", type=Path, required=True, help="Input final split JSONL path.")
    parser.add_argument("--out", type=Path, required=True, help="Output chat JSONL path.")
    parser.add_argument(
        "--prompt-variant",
        choices=("compact", "compact-v2", "full"),
        default="compact",
        help="Prompt-state variant to export.",
    )
    parser.add_argument(
        "--multi-target",
        action="store_true",
        help="Placeholder flag for future multi-target export mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    export_chat_format(
        args.input,
        args.out,
        prompt_variant=args.prompt_variant,
        include_multi_target=args.multi_target,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
