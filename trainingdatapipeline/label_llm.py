"""Constrained teacher relabeling for ambiguous high-value states."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autoresearch.config import load_config
from autoresearch.provider import ChatMessage, create_provider, provider_trace_scope

from . import PIPELINE_VERSION
from .offline_validator import validate_candidate_response

DEFAULT_TEACHERS = [
    "openai-54-mini",
    "gemini-3-flash-preview",
    "gemini-3.1-flash-lite-preview",
]

AMBIGUOUS_REASONS = {
    "run_cli_policy_step",
    "browse_only_step",
    "empty_reasoning",
    "controller_blocked",
}

CORRECTIVE_FOCUS_HINTS = {
    "formatting_cleanliness": "Return exactly one raw JSON object only. Do not add Markdown fences, duplicated objects, trailing commentary, or suffix junk.",
    "missing_evaluate_instruments": "If you choose evaluate_candidate, include the required instruments array from prompt_state handles or recent successful steps.",
    "inspect_artifact_binding": "If you choose inspect_artifact, bind it to the concrete attempt_id or artifact_dir already present in prompt_state.",
    "wrong_tool_choice": "Preserve the intended next-action family unless prompt_state clearly rules it out.",
    "other_contract_failure": "Stay close to the controller-visible state and fix only the concrete contract defect.",
}


def _load_rows(path: Path) -> list[dict[str, Any]]:
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


def _select_ambiguous_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        quality = row.get("quality_labels") if isinstance(row.get("quality_labels"), dict) else {}
        if quality.get("keep_for_base_sft") or quality.get("keep_for_recovery"):
            continue
        reasons = set(str(item) for item in (row.get("rejection_reasons") or []))
        if not reasons or not reasons.issubset(AMBIGUOUS_REASONS):
            continue
        if "hard_action_failure" in reasons or "recovery_or_failure_state" in reasons:
            continue
        candidates.append(row)
    candidates.sort(
        key=lambda row: (
            str(row.get("run_id") or ""),
            int(row.get("step") or 0),
            str(row.get("example_id") or ""),
        )
    )
    return candidates[:limit]


def _build_messages(record: dict[str, Any], candidate_count: int) -> list[ChatMessage]:
    prompt_state = (
        record.get("prompt_state_compact")
        if isinstance(record.get("prompt_state_compact"), dict)
        else record.get("prompt_state_full")
    )
    tool_guide = {
        "prepare_profile": {
            "required": ["tool", "mode"],
            "modes": {
                "scaffold_from_seed": ["indicator_ids"],
                "clone_local": ["source_candidate_name"],
                "from_template": [],
            },
        },
        "mutate_profile": {"required": ["tool", "mutations"], "one_of": [["candidate_name"], ["profile_ref"]]},
        "validate_profile": {"required": ["tool"], "one_of": [["candidate_name"], ["profile_ref"]]},
        "register_profile": {"required": ["tool"], "one_of": [["candidate_name"], ["profile_ref"]], "optional": ["operation", "profile_ref"]},
        "evaluate_candidate": {
            "required": ["tool", "instruments"],
            "one_of": [["profile_ref"], ["candidate_name"]],
            "optional": ["timeframe", "timeframe_policy", "lookback_months", "mode"],
        },
        "run_parameter_sweep": {"required": ["tool", "profile_ref", "axes"]},
        "inspect_artifact": {"one_of": [["artifact_dir"], ["attempt_id"]], "required": ["tool"]},
        "compare_artifacts": {"required": ["tool"], "one_of": [["attempt_ids"], ["artifact_dirs"]]},
        "read_file": {"required": ["tool", "path"]},
        "list_dir": {"required": ["tool", "path"]},
        "write_file": {"required": ["tool", "path", "content"]},
        "run_cli": {"required": ["tool", "args"]},
        "finish": {"required": ["tool", "summary"]},
    }
    corrective_focus = [str(item) for item in (record.get("corrective_focus") or []) if str(item)]
    corrective_metadata = record.get("corrective_metadata") if isinstance(record.get("corrective_metadata"), dict) else {}
    original_target = (
        record.get("original_target_response_normalized")
        if isinstance(record.get("original_target_response_normalized"), dict)
        else record.get("target_response_normalized")
    )
    corrective_mode = bool(corrective_focus or corrective_metadata)
    corrective_payload: dict[str, Any] | None = None
    if corrective_mode:
        corrective_payload = {
            "mode": "corrective_relabel",
            "failure_classes": corrective_focus,
            "instructions": [
                CORRECTIVE_FOCUS_HINTS[item]
                for item in corrective_focus
                if item in CORRECTIVE_FOCUS_HINTS
            ],
            "known_bad_output": {
                "parse_error": corrective_metadata.get("parse_error"),
                "validation_errors": corrective_metadata.get("validation_errors"),
                "predicted_first_tool": corrective_metadata.get("predicted_first_tool"),
                "target_first_tool": corrective_metadata.get("target_first_tool"),
                "generated_text_excerpt": str(corrective_metadata.get("generated_text") or "")[:500],
            },
            "preferred_reference_shape": original_target,
        }
    payload = {
        "task": "Relabel the next explorer response for this controller state.",
        "constraints": [
            "Return JSON only.",
            f"Return 2 to {candidate_count} candidate responses.",
            "Each candidate must be a valid top-level object with keys reasoning and actions.",
            "Each actions entry must be an object, never a string.",
            "Use only the allowed controller tools listed below.",
            "Use typed tools when possible and avoid run_cli unless strictly necessary.",
            "Keep reasoning short and operational. Do not include chain-of-thought.",
            "Do not use future knowledge.",
        ],
        "allowed_tools": tool_guide,
        "preferred_policy": [
            "Prefer prepare_profile, validate_profile, register_profile, evaluate_candidate, run_parameter_sweep, inspect_artifact, compare_artifacts.",
            "Do not invent tools like typed.create_candidate, get_market_summary, or explore_environment.",
            "Use candidate_name for run-owned drafts and profile_ref for registered profiles. Do not emit filesystem paths.",
            "If the state does not justify finish, do not use finish.",
        ],
        "response_template": {
            "candidates": [
                {
                    "reasoning": "short operational rationale",
                    "actions": [
                        {"tool": "prepare_profile", "mode": "scaffold_from_seed", "indicator_ids": ["INDICATOR_A", "INDICATOR_B"]}
                    ],
                }
            ]
        },
        "prompt_state": prompt_state,
    }
    if corrective_payload is not None:
        payload["corrective_mode"] = corrective_payload
    system = (
        "You are relabeling explorer turns for fuzzfolio autoresearch. "
        "Return only a JSON object with key candidates. "
        "candidates must be an array of 2 to 4 objects, each with keys reasoning and actions."
    )
    if corrective_mode:
        system += (
            " This is a corrective relabel task. Fix the concrete contract failure while staying close to the intended next action."
        )
    return [
        ChatMessage(role="system", content=system),
        ChatMessage(role="user", content=json.dumps(payload, ensure_ascii=True)),
    ]


def _extract_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = payload.get("candidates")
    if isinstance(candidates, list):
        return [item for item in candidates if isinstance(item, (dict, list))]
    if "reasoning" in payload and "actions" in payload:
        return [payload]
    return []


def relabel_rows(
    input_path: Path,
    output_path: Path,
    *,
    summary_path: Path | None = None,
    capture_dir: Path | None = None,
    profiles: list[str] | None = None,
    limit: int = 12,
    candidate_count: int = 3,
    use_input_order: bool = False,
) -> dict[str, Any]:
    config = load_config()
    teacher_names = profiles or list(DEFAULT_TEACHERS)
    loaded_rows = _load_rows(input_path)
    rows = loaded_rows[:limit] if use_input_order else _select_ambiguous_rows(loaded_rows, limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if capture_dir is not None:
        capture_dir.mkdir(parents=True, exist_ok=True)

    counts = Counter()
    profile_counts = Counter()
    total_duration_seconds = 0.0

    with output_path.open("w", encoding="utf-8") as sink:
        for row in rows:
            messages = _build_messages(row, candidate_count)
            for profile_name in teacher_names:
                if profile_name not in config.providers:
                    continue
                provider = create_provider(config.providers[profile_name])
                capture_path = (
                    capture_dir / f"{profile_name}.jsonl" if capture_dir is not None else None
                )
                started = time.perf_counter()
                raw_payload: dict[str, Any] | None = None
                error_text: str | None = None
                try:
                    with provider_trace_scope(
                        label="teacher_relabel",
                        run_id=str(row.get("run_id") or ""),
                        step=int(row.get("step") or 0),
                        phase="llm_relabel",
                        provider_type=config.providers[profile_name].provider_type,
                        model=config.providers[profile_name].model,
                        capture_path=str(capture_path) if capture_path is not None else None,
                    ):
                        raw_payload = provider.complete_json(messages)
                except Exception as exc:
                    error_text = str(exc)
                elapsed = time.perf_counter() - started
                total_duration_seconds += elapsed
                profile_counts[profile_name] += 1

                validated_candidates: list[dict[str, Any]] = []
                raw_candidates: list[Any] = []
                if raw_payload is not None:
                    raw_candidates = _extract_candidates(raw_payload)
                    for candidate in raw_candidates:
                        validation = validate_candidate_response(row, candidate)
                        validated_candidates.append(
                            {
                                "candidate": validation.normalized_response or candidate,
                                "ok": validation.ok,
                                "errors": validation.errors,
                                "warnings": validation.warnings,
                                "stateful_checks": validation.stateful_checks,
                            }
                        )
                chosen = next(
                    (
                        item["candidate"]
                        for item in validated_candidates
                        if item.get("ok")
                    ),
                    None,
                )
                emitted = {
                    "example_id": row.get("example_id"),
                    "run_id": row.get("run_id"),
                    "step": row.get("step"),
                    "phase": row.get("phase"),
                    "split_hint": row.get("split_hint"),
                    "sourcetype": "llm_relabeled",
                    "teacher_profile": profile_name,
                    "teacher_model": config.providers[profile_name].model,
                    "candidate_count_requested": candidate_count,
                    "prompt_state_compact": row.get("prompt_state_compact"),
                    "prompt_state_full": row.get("prompt_state_full"),
                    "original_target_response_normalized": row.get("target_response_normalized"),
                    "target_response_normalized": chosen,
                    "target_actions": chosen.get("actions") if isinstance(chosen, dict) else None,
                    "targetreasoningshort": str(chosen.get("reasoning") or "")[:240] if isinstance(chosen, dict) else None,
                    "validated_candidates": validated_candidates,
                    "selected_target_response": chosen,
                    "selected_target_actions": chosen.get("actions") if isinstance(chosen, dict) else None,
                    "selected_reasoning_short": str(chosen.get("reasoning") or "")[:240] if isinstance(chosen, dict) else None,
                    "validation_passed": chosen is not None,
                    "provider_error": error_text,
                    "duration_seconds": round(elapsed, 3),
                    "quality_labels": row.get("quality_labels"),
                    "rejection_reasons": row.get("rejection_reasons"),
                    "provenance": {
                        "source_example_id": row.get("example_id"),
                        "input_path": str(input_path.resolve()),
                    },
                }
                if chosen is not None:
                    counts["selected"] += 1
                if error_text:
                    counts["provider_errors"] += 1
                counts["rows_emitted"] += 1
                sink.write(json.dumps(emitted, ensure_ascii=True) + "\n")

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "teacher_profiles": teacher_names,
        "input_examples_considered": len(rows),
        "rows_emitted": counts["rows_emitted"],
        "selected_count": counts["selected"],
        "provider_error_count": counts["provider_errors"],
        "profile_call_counts": dict(profile_counts),
        "total_duration_seconds": round(total_duration_seconds, 3),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Constrained LLM relabeling for ambiguous states.")
    parser.add_argument("--input", type=Path, required=True, help="Input scored JSONL.")
    parser.add_argument("--out", type=Path, required=True, help="Output relabeled JSONL.")
    parser.add_argument("--summary-out", type=Path, help="Optional summary JSON path.")
    parser.add_argument("--capture-dir", type=Path, help="Optional raw teacher capture dir.")
    parser.add_argument(
        "--profile",
        action="append",
        dest="profiles",
        help="Teacher profile name. Repeatable. Defaults to cheap-teacher bake-off set.",
    )
    parser.add_argument("--limit", type=int, default=12, help="Maximum ambiguous examples to relabel.")
    parser.add_argument(
        "--candidate-count",
        type=int,
        default=3,
        help="Requested candidate count per example.",
    )
    parser.add_argument(
        "--use-input-order",
        action="store_true",
        help="Use the input file as a preselected relabel batch instead of applying the built-in ambiguous selector.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    relabel_rows(
        args.input,
        args.out,
        summary_path=args.summary_out,
        capture_dir=args.capture_dir,
        profiles=args.profiles,
        limit=args.limit,
        candidate_count=args.candidate_count,
        use_input_order=args.use_input_order,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
