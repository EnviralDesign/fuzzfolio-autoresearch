"""Offline evaluation helpers for dataset and model-output validity."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
import sys
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

from autoresearch.controller_protocol import (
    LOCAL_OPENING_STEP_PROTOCOL,
    SFT_SYSTEM_PROTOCOL,
)
from autoresearch.controller import (
    canonicalize_followup_step_response,
    canonicalize_local_opening_step_response,
)
from trainingdatapipeline.normalize_state import build_prompt_variants
from trainingdatapipeline.offline_validator import validate_candidate_response


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


def _preferred_dtype() -> torch.dtype:
    if torch.cuda.is_available() and torch.cuda.is_bf16_supported():
        return torch.bfloat16
    return torch.float16


def _load_adapter_model(
    *,
    model_id: str,
    adapter_dir: Path,
    quantization: str,
) -> tuple[Any, Any]:
    dtype = _preferred_dtype()
    tokenizer_source = str(adapter_dir) if adapter_dir.exists() else model_id
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_source)
    if tokenizer.pad_token is None and tokenizer.eos_token is not None:
        tokenizer.pad_token = tokenizer.eos_token
    model_kwargs: dict[str, Any] = {"dtype": dtype}
    if quantization == "4bit":
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=dtype,
        )
    base_model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
    model = PeftModel.from_pretrained(base_model, str(adapter_dir))
    model.eval()
    if getattr(model.config, "use_cache", None) is not None:
        model.config.use_cache = True
    return model, tokenizer


LEGACY_MODEL_PATH_FIELDS = {
    "profile_path",
    "destination_path",
    "source_profile_path",
}


def _resolved_prompt_state(
    record: dict[str, Any],
    prompt_variant: str,
    *,
    apply_opening_runtime_canonicalizer: bool = False,
) -> tuple[dict[str, Any], str]:
    prompt_variants = build_prompt_variants(record)
    prompt_key = {
        "full": "full",
        "compact": "compact",
        "compact-v2": "compact_v2",
    }[prompt_variant]
    prompt_state = prompt_variants.get(prompt_key)
    if not isinstance(prompt_state, dict):
        cached_key = {
            "full": "prompt_state_full",
            "compact": "prompt_state_compact",
            "compact-v2": "prompt_state_compact_v2",
        }[prompt_variant]
        prompt_state = record.get(cached_key)
    if not isinstance(prompt_state, dict):
        prompt_state = record.get("prompt_state")
    if not isinstance(prompt_state, dict):
        raise ValueError(f"Record {record.get('example_id')} is missing prompt state for variant={prompt_variant}.")
    system_protocol = SFT_SYSTEM_PROTOCOL
    if apply_opening_runtime_canonicalizer and _should_apply_opening_runtime_canonicalizer(record):
        system_protocol = LOCAL_OPENING_STEP_PROTOCOL
    return prompt_state, system_protocol


def _build_prompt_messages(
    record: dict[str, Any],
    prompt_variant: str,
    *,
    apply_opening_runtime_canonicalizer: bool = False,
) -> list[dict[str, str]]:
    prompt_state, system_protocol = _resolved_prompt_state(
        record,
        prompt_variant,
        apply_opening_runtime_canonicalizer=apply_opening_runtime_canonicalizer,
    )
    return [
        {"role": "system", "content": system_protocol},
        {"role": "user", "content": json.dumps(prompt_state, ensure_ascii=True)},
    ]


def _extract_json_candidate(text: str) -> tuple[dict[str, Any] | list[Any] | None, str | None]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
            cleaned = "\n".join(lines[1:-1]).strip()
    candidates = [cleaned]
    first = cleaned.find("{")
    last = cleaned.rfind("}")
    if first != -1 and last != -1 and last > first:
        snippet = cleaned[first : last + 1]
        if snippet not in candidates:
            candidates.append(snippet)
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, (dict, list)):
            return payload, None
    return None, "Could not parse generated text as JSON object."


def _first_tool(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    actions = payload.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    if not isinstance(first, dict):
        return None
    tool = first.get("tool")
    return str(tool) if tool is not None else None


def _first_action(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    actions = payload.get("actions")
    if not isinstance(actions, list) or not actions:
        return None
    first = actions[0]
    return first if isinstance(first, dict) else None


def _should_apply_opening_runtime_canonicalizer(record: dict[str, Any]) -> bool:
    try:
        step = int(record.get("step") or 0)
    except (TypeError, ValueError):
        step = 0
    if step != 1:
        return False
    return _first_tool(record.get("target_response_normalized")) == "prepare_profile"


def _opening_runtime_context(record: dict[str, Any]) -> dict[str, Any]:
    prompt_state = record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    opening_grounding = (
        prompt_state.get("opening_grounding")
        if isinstance(prompt_state.get("opening_grounding"), dict)
        else {}
    )
    starter_instruments = list(opening_grounding.get("preferred_initial_instruments") or [])
    candidate_name_hint = str(opening_grounding.get("candidate_name_hint") or "cand1")
    return {
        "starter_instruments": starter_instruments,
        "candidate_name_hint": candidate_name_hint,
    }


def _followup_runtime_context(
    record: dict[str, Any],
    prompt_variant: str,
) -> dict[str, Any]:
    prompt_state, _system_protocol = _resolved_prompt_state(record, prompt_variant)
    template = (
        prompt_state.get("next_action_template")
        if isinstance(prompt_state.get("next_action_template"), dict)
        else {}
    )
    return {
        "next_action_template": template if isinstance(template, dict) else {},
    }


def _payload_uses_legacy_path_fields(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in LEGACY_MODEL_PATH_FIELDS:
                return True
            if _payload_uses_legacy_path_fields(item):
                return True
    if isinstance(value, list):
        return any(_payload_uses_legacy_path_fields(item) for item in value)
    return False


def _canonicalize_record_relative_text(value: Any, run_dir: str) -> Any:
    if not isinstance(value, str):
        return value
    normalized = str(value)
    if run_dir:
        run_dir_clean = run_dir.rstrip("\\/")
        replacements = (
            (run_dir_clean + "\\profiles", "<PROFILES_DIR>"),
            (run_dir_clean + "\\evals", "<EVALS_DIR>"),
            (run_dir_clean + "\\notes", "<NOTES_DIR>"),
            (run_dir_clean, "<RUN_DIR>"),
        )
        for source, target in replacements:
            normalized = normalized.replace(source, target)
    return normalized


def _score_opening_grounding(
    record: dict[str, Any],
    normalized_payload: dict[str, Any] | None,
    *,
    validation_ok: bool,
) -> dict[str, Any] | None:
    if not _should_apply_opening_runtime_canonicalizer(record):
        return None
    first = _first_tool(normalized_payload) if isinstance(normalized_payload, dict) else None
    if first != "prepare_profile":
        return {
            "instrument_grounding_ok": False,
            "candidate_handle_ok": False,
            "uses_forbidden_instrument": False,
            "opening_grounding_success": False,
        }
    actions = normalized_payload.get("actions") if isinstance(normalized_payload, dict) else None
    predicted_action = actions[0] if isinstance(actions, list) and actions and isinstance(actions[0], dict) else {}
    expected = (
        record.get("opening_grounding_expected")
        if isinstance(record.get("opening_grounding_expected"), dict)
        else {}
    )
    expected_action = _first_action(record.get("target_response_normalized"))
    expected_instruments = list(expected.get("instruments") or (expected_action.get("instruments") if isinstance(expected_action, dict) else []) or [])
    expected_candidate_name = str(
        expected.get("candidate_name")
        or (expected_action.get("candidate_name") if isinstance(expected_action, dict) else "")
        or ""
    ).strip()
    predicted_instruments = list(predicted_action.get("instruments") or [])
    predicted_candidate_name = str(predicted_action.get("candidate_name") or "").strip()
    instrument_grounding_ok = predicted_instruments == expected_instruments and not any(
        str(item or "").upper() in {"ALL", "__BASKET__"} for item in predicted_instruments
    )
    candidate_handle_ok = (
        predicted_candidate_name
        and expected_candidate_name
        and predicted_candidate_name == expected_candidate_name
    )
    uses_forbidden_instrument = any(
        str(item or "").upper() in {"ALL", "__BASKET__"} for item in predicted_instruments
    )
    return {
        "instrument_grounding_ok": instrument_grounding_ok,
        "candidate_handle_ok": candidate_handle_ok,
        "uses_forbidden_instrument": uses_forbidden_instrument,
        "opening_grounding_success": bool(
            validation_ok
            and instrument_grounding_ok
            and candidate_handle_ok
            and not uses_forbidden_instrument
        ),
    }


def adapter_generate_validity(
    reference_path: Path,
    *,
    model_id: str,
    adapter_dir: Path,
    out_path: Path,
    summary_path: Path | None = None,
    limit: int | None = None,
    prompt_variant: str = "compact",
    quantization: str = "4bit",
    max_new_tokens: int = 256,
    apply_opening_runtime_canonicalizer: bool = False,
    apply_followup_runtime_canonicalizer: bool = False,
) -> dict[str, Any]:
    model, tokenizer = _load_adapter_model(
        model_id=model_id,
        adapter_dir=adapter_dir,
        quantization=quantization,
    )
    rows = _load_jsonl(reference_path)
    if limit is not None:
        rows = rows[:limit]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    generated_tokens: list[int] = []
    durations: list[float] = []
    with out_path.open("w", encoding="utf-8") as sink:
        for record in rows:
            example_id = str(record.get("example_id") or "")
            messages = _build_prompt_messages(
                record,
                prompt_variant,
                apply_opening_runtime_canonicalizer=apply_opening_runtime_canonicalizer,
            )
            if hasattr(tokenizer, "apply_chat_template"):
                prompt_text = tokenizer.apply_chat_template(
                    messages,
                    tokenize=False,
                    add_generation_prompt=True,
                )
            else:
                prompt_text = "\n".join(f"{message['role']}: {message['content']}" for message in messages)
            model_inputs = tokenizer(prompt_text, return_tensors="pt")
            model_inputs = {key: value.to(model.device) for key, value in model_inputs.items()}
            started = time.perf_counter()
            with torch.inference_mode():
                outputs = model.generate(
                    **model_inputs,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                    pad_token_id=tokenizer.pad_token_id,
                    eos_token_id=tokenizer.eos_token_id,
                )
            elapsed = time.perf_counter() - started
            prompt_length = int(model_inputs["input_ids"].shape[1])
            generated = outputs[0][prompt_length:]
            generated_text = tokenizer.decode(generated, skip_special_tokens=True)
            candidate_payload, parse_error = _extract_json_candidate(generated_text)
            runtime_canonicalized = False
            if (
                apply_opening_runtime_canonicalizer
                and isinstance(candidate_payload, dict)
                and _should_apply_opening_runtime_canonicalizer(record)
            ):
                runtime_context = _opening_runtime_context(record)
                canonicalized_payload = canonicalize_local_opening_step_response(
                    candidate_payload,
                    starter_instruments=list(
                        runtime_context.get("starter_instruments") or []
                    ),
                    candidate_name_hint=str(
                        runtime_context.get("candidate_name_hint") or "cand1"
                    ),
                )
                runtime_canonicalized = canonicalized_payload != candidate_payload
                candidate_payload = canonicalized_payload
            elif (
                apply_followup_runtime_canonicalizer
                and isinstance(candidate_payload, dict)
            ):
                followup_context = _followup_runtime_context(record, prompt_variant)
                canonicalized_payload = canonicalize_followup_step_response(
                    candidate_payload,
                    next_action_template=followup_context.get("next_action_template"),
                )
                runtime_canonicalized = canonicalized_payload != candidate_payload
                candidate_payload = canonicalized_payload
            validation = None
            if candidate_payload is not None:
                validation = validate_candidate_response(record, candidate_payload)
            target_tool = _first_tool(record.get("target_response_normalized"))
            predicted_tool = _first_tool(validation.normalized_response if validation else candidate_payload if isinstance(candidate_payload, dict) else None)
            deterministic_target = None
            policy_labels = record.get("policy_labels")
            if isinstance(policy_labels, dict):
                deterministic_target = policy_labels.get("deterministic_followup_target")
            grounding_metrics = _score_opening_grounding(
                record,
                validation.normalized_response
                if validation is not None
                else candidate_payload if isinstance(candidate_payload, dict) else None,
                validation_ok=bool(validation.ok) if validation is not None else False,
            )
            emitted = {
                "example_id": example_id,
                "run_id": record.get("run_id"),
                "step": record.get("step"),
                "phase": record.get("phase"),
                "deterministic_followup_target": deterministic_target,
                "target_first_tool": target_tool,
                "predicted_first_tool": predicted_tool,
                "generated_tokens": int(generated.shape[0]),
                "duration_seconds": round(elapsed, 3),
                "generated_text": generated_text,
                "parsed_prediction": candidate_payload,
                "runtime_canonicalized": runtime_canonicalized,
                "parse_ok": candidate_payload is not None,
                "parse_error": parse_error,
                "validation_ok": bool(validation.ok) if validation is not None else False,
                "validation_errors": list(validation.errors) if validation is not None else [],
                "validation_warnings": list(validation.warnings) if validation is not None else [],
                "uses_legacy_path_fields": _payload_uses_legacy_path_fields(candidate_payload),
            }
            if grounding_metrics is not None:
                emitted.update(grounding_metrics)
            sink.write(json.dumps(emitted, ensure_ascii=True) + "\n")
            counts["rows"] += 1
            generated_tokens.append(int(generated.shape[0]))
            durations.append(elapsed)
            if candidate_payload is not None:
                counts["json_parse_ok"] += 1
            else:
                counts["json_parse_failed"] += 1
            if runtime_canonicalized:
                counts["runtime_canonicalized"] += 1
            if validation is not None and validation.ok:
                counts["validator_ok"] += 1
            else:
                counts["validator_failed"] += 1
            if predicted_tool:
                counts[f"predicted_tool:{predicted_tool}"] += 1
            if target_tool and predicted_tool == target_tool:
                counts["first_tool_match"] += 1
            if deterministic_target:
                counts["deterministic_rows"] += 1
                if predicted_tool == str(deterministic_target):
                    counts["deterministic_tool_match"] += 1
            if validation is not None:
                for error in validation.errors:
                    counts[f"validation_error:{error}"] += 1
            if not emitted["uses_legacy_path_fields"]:
                counts["pathless_action_ok"] += 1
            else:
                counts["pathless_action_failed"] += 1
            if grounding_metrics is not None:
                counts["opening_grounding_rows"] += 1
                if grounding_metrics["instrument_grounding_ok"]:
                    counts["instrument_grounding_ok"] += 1
                if grounding_metrics["candidate_handle_ok"]:
                    counts["candidate_handle_ok"] += 1
                if grounding_metrics["uses_forbidden_instrument"]:
                    counts["uses_forbidden_instrument"] += 1
                if grounding_metrics["opening_grounding_success"]:
                    counts["opening_grounding_success"] += 1
    summary = {
        "reference_path": str(reference_path.resolve()),
        "adapter_dir": str(adapter_dir.resolve()),
        "output_path": str(out_path.resolve()),
        "rows": counts["rows"],
        "json_parse_ok": counts["json_parse_ok"],
        "validator_ok": counts["validator_ok"],
        "first_tool_match": counts["first_tool_match"],
        "deterministic_rows": counts["deterministic_rows"],
        "deterministic_tool_match": counts["deterministic_tool_match"],
        "pathless_action_ok": counts["pathless_action_ok"],
        "opening_grounding_rows": counts["opening_grounding_rows"],
        "instrument_grounding_ok": counts["instrument_grounding_ok"],
        "candidate_handle_ok": counts["candidate_handle_ok"],
        "uses_forbidden_instrument": counts["uses_forbidden_instrument"],
        "opening_grounding_success": counts["opening_grounding_success"],
        "avg_generated_tokens": round(sum(generated_tokens) / len(generated_tokens), 2) if generated_tokens else 0.0,
        "avg_duration_seconds": round(sum(durations) / len(durations), 3) if durations else 0.0,
        "counts": dict(counts),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return summary


def dataset_sanity(input_path: Path) -> dict[str, Any]:
    rows = _load_jsonl(input_path)
    counts = Counter()
    for row in rows:
        counts["rows"] += 1
        messages = row.get("messages")
        if isinstance(messages, list) and len(messages) == 3:
            counts["three_message_rows"] += 1
        assistant = messages[2]["content"] if isinstance(messages, list) and len(messages) >= 3 else None
        if isinstance(assistant, str):
            try:
                payload = json.loads(assistant)
                if isinstance(payload, dict) and isinstance(payload.get("actions"), list):
                    counts["assistant_json_valid"] += 1
            except json.JSONDecodeError:
                pass
    return dict(counts)


def prediction_validity(reference_path: Path, prediction_path: Path) -> dict[str, Any]:
    references = {str(row.get("example_id") or ""): row for row in _load_jsonl(reference_path)}
    predictions = _load_jsonl(prediction_path)
    counts = Counter()
    for row in predictions:
        example_id = str(row.get("example_id") or "")
        reference = references.get(example_id)
        if not reference:
            counts["missing_reference"] += 1
            continue
        candidate = row.get("prediction")
        try:
            validation = validate_candidate_response(reference, candidate)
        except Exception:
            counts["validator_exception"] += 1
            continue
        counts["rows"] += 1
        if validation.ok:
            counts["valid_predictions"] += 1
        else:
            counts["invalid_predictions"] += 1
    return dict(counts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline evaluation helpers.")
    sub = parser.add_subparsers(dest="command", required=True)

    sanity = sub.add_parser("dataset-sanity")
    sanity.add_argument("--input", type=Path, required=True)

    validity = sub.add_parser("prediction-validity")
    validity.add_argument("--reference", type=Path, required=True)
    validity.add_argument("--predictions", type=Path, required=True)

    generate = sub.add_parser("adapter-generate-validity")
    generate.add_argument("--reference", type=Path, required=True)
    generate.add_argument("--adapter-dir", type=Path, required=True)
    generate.add_argument("--out", type=Path, required=True)
    generate.add_argument("--summary-out", type=Path)
    generate.add_argument("--model-id", default="google/gemma-4-E4B-it")
    generate.add_argument("--limit", type=int)
    generate.add_argument("--prompt-variant", choices=("compact", "compact-v2", "full"), default="compact")
    generate.add_argument("--quantization", choices=("none", "4bit"), default="4bit")
    generate.add_argument("--max-new-tokens", type=int, default=256)
    generate.add_argument("--apply-opening-runtime-canonicalizer", action="store_true")
    generate.add_argument("--apply-followup-runtime-canonicalizer", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "dataset-sanity":
        print(json.dumps(dataset_sanity(args.input), ensure_ascii=True, indent=2))
        return 0
    if args.command == "prediction-validity":
        print(
            json.dumps(
                prediction_validity(args.reference, args.predictions),
                ensure_ascii=True,
                indent=2,
            )
        )
        return 0
    if args.command == "adapter-generate-validity":
        print(
            json.dumps(
                adapter_generate_validity(
                    args.reference,
                    model_id=args.model_id,
                    adapter_dir=args.adapter_dir,
                    out_path=args.out,
                    summary_path=args.summary_out,
                    limit=args.limit,
                    prompt_variant=args.prompt_variant,
                    quantization=args.quantization,
                    max_new_tokens=args.max_new_tokens,
                    apply_opening_runtime_canonicalizer=args.apply_opening_runtime_canonicalizer,
                    apply_followup_runtime_canonicalizer=args.apply_followup_runtime_canonicalizer,
                ),
                ensure_ascii=True,
                indent=2,
            )
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
