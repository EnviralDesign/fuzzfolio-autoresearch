"""Normalize replayed raw step records into trainable prompt variants."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION


def _canonicalize_text(text: str, run_dir: str) -> str:
    normalized = str(text)
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


def _canonicalize_value(value: Any, run_dir: str) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return _canonicalize_text(value, run_dir)
    if isinstance(value, list):
        return [_canonicalize_value(item, run_dir) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonicalize_value(item, run_dir) for key, item in value.items()
        }
    return _canonicalize_text(value, run_dir)


LEGACY_MODEL_PATH_FIELDS = frozenset(
    {"profile_path", "destination_path", "source_profile_path", "metadata_out_path"}
)


def _candidate_name_from_path_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = str(value or "").strip().replace("/", "\\")
    if not text:
        return None
    return Path(text).stem or None


def _serialized_signature(payload: dict[str, Any], limit: int = 220) -> str:
    return _shorten_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True),
        limit,
    )


def _rewrite_legacy_path_fields_in_text(text: str) -> str:
    normalized = str(text or "")
    replacements = (
        ("source_profile_path", "source_candidate_name"),
        ("destination_path", "destination_candidate_name"),
        ("profile_path", "candidate_name"),
    )
    for legacy_field, handle_field in replacements:
        pattern = re.compile(rf'"{legacy_field}"\s*:\s*"([^"]+)"')

        def _replace(match: re.Match[str]) -> str:
            handle = _candidate_name_from_path_text(match.group(1))
            if not handle:
                return ""
            if f'"{handle_field}"' in normalized:
                return ""
            return f'"{handle_field}": "{handle}"'

        normalized = pattern.sub(_replace, normalized)
    normalized = re.sub(r",\s*,", ", ", normalized)
    normalized = re.sub(r"\{\s*,", "{", normalized)
    normalized = re.sub(r"\[\s*,", "[", normalized)
    return normalized


def _pathless_jsonish_text(text: str) -> str:
    stripped = str(text or "").strip()
    if not stripped:
        return stripped
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, (dict, list)):
            return json.dumps(_pathless_model_value(payload), ensure_ascii=True)
    return _rewrite_legacy_path_fields_in_text(stripped)


def _pathless_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    normalized = {str(key): _pathless_model_value(item) for key, item in mapping.items()}
    tool = str(normalized.get("tool") or "").strip()
    mode = str(normalized.get("mode") or "").strip()
    candidate_name = normalized.get("candidate_name")
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        candidate_name = _candidate_name_from_path_text(
            normalized.get("profile_path")
        ) or _candidate_name_from_path_text(normalized.get("destination_path"))
    if isinstance(candidate_name, str) and candidate_name.strip():
        normalized["candidate_name"] = candidate_name

    source_candidate_name = normalized.get("source_candidate_name")
    if not isinstance(source_candidate_name, str) or not source_candidate_name.strip():
        source_candidate_name = _candidate_name_from_path_text(
            normalized.get("source_profile_path")
        )
    if isinstance(source_candidate_name, str) and source_candidate_name.strip():
        normalized["source_candidate_name"] = source_candidate_name

    destination_candidate_name = normalized.get("destination_candidate_name")
    if (
        not isinstance(destination_candidate_name, str)
        or not destination_candidate_name.strip()
    ):
        destination_candidate_name = _candidate_name_from_path_text(
            normalized.get("destination_path")
        )
    if (
        isinstance(destination_candidate_name, str)
        and destination_candidate_name.strip()
    ):
        normalized["destination_candidate_name"] = destination_candidate_name

    if tool == "prepare_profile" and mode == "clone_local":
        normalized.pop("candidate_name", None)
    elif tool == "prepare_profile":
        normalized.pop("destination_candidate_name", None)
    elif tool in {"validate_profile", "register_profile", "evaluate_candidate"}:
        normalized.pop("destination_candidate_name", None)

    for field in LEGACY_MODEL_PATH_FIELDS:
        normalized.pop(field, None)

    if tool and "signature" in normalized:
        signature_payload = {key: value for key, value in normalized.items() if key != "signature"}
        normalized["signature"] = _serialized_signature(signature_payload)

    action_signatures = normalized.get("action_signatures")
    if isinstance(action_signatures, list) and all(
        isinstance(item, dict) for item in action_signatures
    ):
        normalized["action_signatures"] = [
            _pathless_mapping(dict(item)) for item in action_signatures
        ]
        normalized["action_summary"] = [
            _shorten_text(
                json.dumps(
                    {
                        key: value
                        for key, value in item.items()
                        if key != "signature"
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                200,
            )
            for item in normalized["action_signatures"][:3]
        ]

    return normalized


def _pathless_action_dict(action: dict[str, Any]) -> dict[str, Any]:
    return _pathless_mapping(dict(action))


def _pathless_model_value(value: Any) -> Any:
    if isinstance(value, str):
        return _pathless_jsonish_text(value)
    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            return [_pathless_action_dict(dict(item)) for item in value]
        return [_pathless_model_value(item) for item in value]
    if isinstance(value, dict):
        if "tool" in value:
            return _pathless_action_dict(dict(value))
        normalized = _pathless_mapping(dict(value))
        actions = normalized.get("actions")
        if isinstance(actions, list) and all(isinstance(item, dict) for item in actions):
            normalized["actions"] = [_pathless_action_dict(dict(item)) for item in actions]
        return normalized
    return value


def _shorten_text(value: Any, limit: int = 120) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _parse_horizon_months_hint(text: Any) -> int | None:
    if not isinstance(text, str):
        return None
    match = re.search(r"about\s+(\d+)\s+months", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _compact_seed_context(seed_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(seed_context, dict):
        return None
    return {
        "goal_id": seed_context.get("exploration_goal_id"),
        "goal_summary": seed_context.get("exploration_goal_summary"),
        "seed_indicators": list(seed_context.get("seed_indicators") or [])[:10],
        "timeframes": list(seed_context.get("timeframes") or [])[:8],
        "worker_split": list(seed_context.get("worker_split") or [])[:3],
    }


def _compact_run_metadata(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    keys = (
        "explorer_profile",
        "explorer_provider",
        "explorer_model",
        "quality_score_preset",
    )
    compact = {key: metadata.get(key) for key in keys if metadata.get(key) is not None}
    return compact or None


def _compact_run_v2(run_info: dict[str, Any]) -> dict[str, Any]:
    inventory = (
        run_info.get("artifact_inventory")
        if isinstance(run_info.get("artifact_inventory"), dict)
        else {}
    )
    required_present = (
        inventory.get("required_present")
        if isinstance(inventory.get("required_present"), dict)
        else {}
    )
    optional_present = (
        inventory.get("optional_present")
        if isinstance(inventory.get("optional_present"), dict)
        else {}
    )
    optional_dirs_present = (
        inventory.get("optional_dirs_present")
        if isinstance(inventory.get("optional_dirs_present"), dict)
        else {}
    )
    return {
        "run_id": run_info.get("run_id"),
        "has_controller_log": bool(required_present.get("controller-log.jsonl")),
        "has_attempts": bool(optional_present.get("attempts.jsonl")),
        "has_runtime_state": bool(optional_present.get("runtime-state.json")),
        "has_runtime_trace": bool(optional_present.get("runtime-trace.jsonl")),
        "has_profiles": bool(optional_dirs_present.get("profiles")),
        "has_evals": bool(optional_dirs_present.get("evals")),
    }


def _compact_recent_steps(items: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items[-6:]:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "step": item.get("step"),
                "phase": item.get("phase"),
                "reasoning_short": item.get("reasoning_short"),
                "action_summary": list(item.get("action_summary") or [])[:3],
                "manager_event_count": item.get("manager_event_count"),
            }
        )
    return compact


def _compact_recent_attempts(items: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items[-8:]:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "sequence": item.get("sequence"),
                "candidate_name": item.get("candidate_name"),
                "profile_ref": item.get("profile_ref"),
                "composite_score": item.get("composite_score"),
                "effective_window_months": item.get("effective_window_months"),
                "timeframe": item.get("timeframe"),
            }
        )
    return compact


def _compact_recent_attempts_v2(items: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items[-2:]:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "sequence": item.get("sequence"),
                "attempt_id": item.get("attempt_id"),
                "artifact_dir": item.get("artifact_dir"),
                "profile_ref": item.get("profile_ref"),
                "score": item.get("composite_score"),
                "effective_window_months": item.get("effective_window_months"),
                "timeframe": item.get("timeframe"),
            }
        )
    return compact


def _compact_seed_context_v2(seed_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(seed_context, dict):
        return None
    seed = {
        "goal_id": seed_context.get("exploration_goal_id"),
        "goal_summary": _shorten_text(seed_context.get("exploration_goal_summary"), 96)
        if seed_context.get("exploration_goal_summary")
        else None,
        "seed_indicators": list(seed_context.get("seed_indicators") or [])[:2],
        "timeframes": list(seed_context.get("timeframes") or [])[:3],
    }
    return {key: value for key, value in seed.items() if value not in (None, [], "")} or None


def _compact_opening_grounding_v2(
    grounding: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(grounding, dict):
        return None
    compact = {
        "allowed_seed_instruments": list(grounding.get("allowed_seed_instruments") or [])[:4],
        "preferred_initial_instruments": list(
            grounding.get("preferred_initial_instruments") or []
        )[:4],
        "preferred_initial_instrument_rule": grounding.get(
            "preferred_initial_instrument_rule"
        ),
        "candidate_name_hint": grounding.get("candidate_name_hint"),
    }
    return {key: value for key, value in compact.items() if value not in (None, [], "")} or None


def _compact_recent_steps_v2(items: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items[-2:]:
        if not isinstance(item, dict):
            continue
        action_signatures = item.get("action_signatures") if isinstance(item.get("action_signatures"), list) else []
        result_summary = item.get("result_summary") if isinstance(item.get("result_summary"), list) else []
        primary_tool = None
        if action_signatures and isinstance(action_signatures[0], dict):
            primary_tool = action_signatures[0].get("tool")
        next_recommended_action = None
        ready_for_registration = None
        ready_to_evaluate = None
        guard_blocked = False
        profile_ref = None
        created_profile_ref = None
        attempt_id = None
        artifact_dir = None
        instruments = None
        candidate_name = None
        destination_candidate_name = None
        requested_horizon_months = None
        evaluation_mode = None
        view = None
        operation = None
        for signature in reversed(action_signatures):
            if not isinstance(signature, dict):
                continue
            if profile_ref is None and signature.get("profile_ref") is not None:
                profile_ref = signature.get("profile_ref")
            if instruments is None and isinstance(signature.get("instruments"), list):
                instruments = list(signature.get("instruments") or [])[:4]
            if candidate_name is None and signature.get("candidate_name") is not None:
                candidate_name = signature.get("candidate_name")
            if (
                destination_candidate_name is None
                and signature.get("destination_candidate_name") is not None
            ):
                destination_candidate_name = signature.get("destination_candidate_name")
            if requested_horizon_months is None and signature.get("requested_horizon_months") is not None:
                requested_horizon_months = signature.get("requested_horizon_months")
            if evaluation_mode is None and signature.get("evaluation_mode") is not None:
                evaluation_mode = signature.get("evaluation_mode")
            if view is None and signature.get("view") is not None:
                view = signature.get("view")
            if operation is None and signature.get("operation") is not None:
                operation = signature.get("operation")
        for result in result_summary:
            if not isinstance(result, dict):
                continue
            if next_recommended_action is None and result.get("next_recommended_action") is not None:
                next_recommended_action = result.get("next_recommended_action")
            if ready_for_registration is None and result.get("ready_for_registration") is not None:
                ready_for_registration = bool(result.get("ready_for_registration"))
            if ready_to_evaluate is None and result.get("ready_to_evaluate") is not None:
                ready_to_evaluate = bool(result.get("ready_to_evaluate"))
            if created_profile_ref is None and result.get("created_profile_ref") is not None:
                created_profile_ref = result.get("created_profile_ref")
            if profile_ref is None and result.get("profile_ref") is not None:
                profile_ref = result.get("profile_ref")
            if attempt_id is None and result.get("attempt_id") is not None:
                attempt_id = result.get("attempt_id")
            if artifact_dir is None and result.get("artifact_dir") is not None:
                artifact_dir = result.get("artifact_dir")
            auto_log = result.get("auto_log") if isinstance(result.get("auto_log"), dict) else {}
            if attempt_id is None and auto_log.get("attempt_id") is not None:
                attempt_id = auto_log.get("attempt_id")
            if artifact_dir is None and auto_log.get("artifact_dir") is not None:
                artifact_dir = auto_log.get("artifact_dir")
            tool_name = str(result.get("tool") or "")
            if tool_name in {"response_guard", "step_guard"}:
                guard_blocked = True
        step_payload = {
            "step": item.get("step"),
            "primary_tool": primary_tool,
            "next_recommended_action": next_recommended_action,
            "ready_for_registration": ready_for_registration,
            "ready_to_evaluate": ready_to_evaluate,
            "guard_blocked": guard_blocked,
        }
        if profile_ref is not None:
            step_payload["profile_ref"] = profile_ref
        if created_profile_ref is not None:
            step_payload["created_profile_ref"] = created_profile_ref
        if attempt_id is not None:
            step_payload["attempt_id"] = attempt_id
        if artifact_dir is not None:
            step_payload["artifact_dir"] = artifact_dir
        if instruments:
            step_payload["instruments"] = instruments
        if candidate_name is not None:
            step_payload["candidate_name"] = candidate_name
        if destination_candidate_name is not None:
            step_payload["destination_candidate_name"] = destination_candidate_name
        if requested_horizon_months is not None:
            step_payload["requested_horizon_months"] = requested_horizon_months
        if evaluation_mode is not None:
            step_payload["evaluation_mode"] = evaluation_mode
        if view is not None:
            step_payload["view"] = view
        if operation is not None:
            step_payload["operation"] = operation
        compact.append(step_payload)
    return compact


def _compact_handles_v2(
    raw_recent_steps: list[Any],
    raw_recent_attempts: list[Any],
) -> dict[str, Any] | None:
    handles: dict[str, Any] = {}
    attempt_ids: list[Any] = []
    artifact_dirs: list[Any] = []
    for attempt in raw_recent_attempts[-3:]:
        if not isinstance(attempt, dict):
            continue
        if attempt.get("attempt_id") is not None:
            attempt_ids.append(attempt.get("attempt_id"))
        if attempt.get("artifact_dir") is not None:
            artifact_dirs.append(attempt.get("artifact_dir"))
        if handles.get("profile_ref") is None and attempt.get("profile_ref") is not None:
            handles["profile_ref"] = attempt.get("profile_ref")
    for step in reversed(raw_recent_steps[-4:]):
        if not isinstance(step, dict):
            continue
        action_signatures = (
            step.get("action_signatures") if isinstance(step.get("action_signatures"), list) else []
        )
        result_summary = (
            step.get("result_summary") if isinstance(step.get("result_summary"), list) else []
        )
        for signature in reversed(action_signatures):
            if not isinstance(signature, dict):
                continue
            signature = _pathless_action_dict(dict(signature))
            for key in (
                "profile_ref",
                "candidate_name",
                "destination_candidate_name",
                "requested_horizon_months",
                "evaluation_mode",
                "view",
                "operation",
                "candidate_name_prefix",
                "timeframe_policy",
            ):
                if handles.get(key) is None and signature.get(key) is not None:
                    handles[key] = signature.get(key)
            if handles.get("instruments") is None and isinstance(signature.get("instruments"), list):
                handles["instruments"] = list(signature.get("instruments") or [])[:4]
        for result in reversed(result_summary):
            if not isinstance(result, dict):
                continue
            for key in (
                "profile_ref",
                "created_profile_ref",
                "attempt_id",
                "artifact_dir",
                "next_recommended_action",
            ):
                if handles.get(key) is None and result.get(key) is not None:
                    handles[key] = result.get(key)
            if handles.get("ready_for_registration") is None and result.get("ready_for_registration") is not None:
                handles["ready_for_registration"] = bool(result.get("ready_for_registration"))
            if handles.get("ready_to_evaluate") is None and result.get("ready_to_evaluate") is not None:
                handles["ready_to_evaluate"] = bool(result.get("ready_to_evaluate"))
            auto_log = result.get("auto_log") if isinstance(result.get("auto_log"), dict) else {}
            for key in ("attempt_id", "artifact_dir", "effective_window_months"):
                if handles.get(key) is None and auto_log.get(key) is not None:
                    handles[key] = auto_log.get(key)
    if attempt_ids:
        handles["recent_attempt_ids"] = attempt_ids[-2:]
    if artifact_dirs:
        handles["recent_artifact_dirs"] = artifact_dirs[-2:]
    return handles or None


def _synthesized_next_action_template_v2(
    raw_recent_steps: list[Any],
    raw_recent_attempts: list[Any],
    explicit_template: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if isinstance(explicit_template, dict) and explicit_template:
        return _pathless_action_dict(dict(explicit_template))
    recent_steps = _compact_recent_steps_v2(raw_recent_steps)
    latest = recent_steps[-1] if recent_steps else {}
    if not isinstance(latest, dict):
        latest = {}
    handles = _compact_handles_v2(raw_recent_steps, raw_recent_attempts) or {}
    next_action = str(
        latest.get("next_recommended_action")
        or handles.get("next_recommended_action")
        or ""
    ).strip()
    candidate_name = str(
        latest.get("candidate_name")
        or handles.get("candidate_name")
        or ""
    ).strip()
    profile_ref = str(
        latest.get("created_profile_ref")
        or latest.get("profile_ref")
        or handles.get("created_profile_ref")
        or handles.get("profile_ref")
        or ""
    ).strip()
    instruments = list(latest.get("instruments") or handles.get("instruments") or [])
    if next_action == "validate_profile" and candidate_name:
        return {"tool": "validate_profile", "candidate_name": candidate_name}
    if next_action == "register_profile" and candidate_name:
        return {"tool": "register_profile", "candidate_name": candidate_name}
    if next_action == "evaluate_candidate" and instruments:
        template: dict[str, Any] = {
            "tool": "evaluate_candidate",
            "instruments": instruments[:4],
            "timeframe_policy": "profile_default",
            "evaluation_mode": "screen",
        }
        if profile_ref:
            template["profile_ref"] = profile_ref
            return template
        if candidate_name:
            template["candidate_name"] = candidate_name
            return template
    return None


def _compact_next_action_template_v2(
    template: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(template, dict):
        return None
    compact = _pathless_action_dict(dict(template))
    allowed_fields = {
        "tool",
        "candidate_name",
        "profile_ref",
        "instruments",
        "attempt_id",
        "artifact_dir",
        "timeframe_policy",
        "evaluation_mode",
        "requested_horizon_months",
        "view",
        "operation",
    }
    compact = {
        key: value
        for key, value in compact.items()
        if key in allowed_fields and value not in (None, "", [], {})
    }
    return compact or None


def _score_target_code(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    lowered = text.lower()
    if "first credible scored candidate" in lowered:
        return "first_scored_candidate"
    if "provisional leader" in lowered:
        return "provisional_leader"
    if "validated leader" in lowered:
        return "validated_leader"
    if "quality_score >=" in lowered or "quality score >=" in lowered:
        return "quality_threshold"
    return _shorten_text(text, 48) if text.strip() else None


def _compact_controller_v2(
    controller: dict[str, Any] | None,
    timeframe_status: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(controller, dict):
        return None
    score_target = controller.get("score_target")
    compact = {
        "step": controller.get("step"),
        "phase": controller.get("phase"),
        "horizon_months_hint": _parse_horizon_months_hint(controller.get("horizon_target")),
        "score_target": _score_target_code(score_target),
    }
    if isinstance(timeframe_status, dict):
        compact["timeframe_status"] = {
            "has_mismatch": bool(timeframe_status.get("has_mismatch")),
            "repeat_blocked": bool(timeframe_status.get("repeat_blocked")),
        }
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})} or None


def build_prompt_variants(record: dict[str, Any]) -> dict[str, dict[str, Any]]:
    prompt_state = (
        record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    )
    prompt_state = _pathless_model_value(prompt_state)
    run_info = prompt_state.get("run") if isinstance(prompt_state.get("run"), dict) else {}
    run_dir = str(run_info.get("run_dir") or "")
    full_variant = _canonicalize_value(prompt_state, run_dir)
    compact_variant = {
        "run": {
            "run_id": run_info.get("run_id"),
            "artifact_flags": (
                run_info.get("artifact_inventory")
                if isinstance(run_info.get("artifact_inventory"), dict)
                else None
            ),
        },
        "controller": prompt_state.get("controller"),
        "seed": _compact_seed_context(
            prompt_state.get("seed_context")
            if isinstance(prompt_state.get("seed_context"), dict)
            else None
        ),
        "run_metadata": _compact_run_metadata(
            prompt_state.get("run_metadata")
            if isinstance(prompt_state.get("run_metadata"), dict)
            else None
        ),
        "recent_steps": _compact_recent_steps(
            prompt_state.get("recent_step_window")
            if isinstance(prompt_state.get("recent_step_window"), list)
            else []
        ),
        "recent_attempts": _compact_recent_attempts(
            prompt_state.get("recent_attempts")
            if isinstance(prompt_state.get("recent_attempts"), list)
            else []
        ),
    }
    next_action_template = _synthesized_next_action_template_v2(
        prompt_state.get("recent_step_window")
        if isinstance(prompt_state.get("recent_step_window"), list)
        else [],
        prompt_state.get("recent_attempts")
        if isinstance(prompt_state.get("recent_attempts"), list)
        else [],
        prompt_state.get("next_action_template")
        if isinstance(prompt_state.get("next_action_template"), dict)
        else None,
    )
    compact_variant_v2 = {
        "run": {"run_id": run_info.get("run_id")},
        "controller": _compact_controller_v2(
            prompt_state.get("controller")
            if isinstance(prompt_state.get("controller"), dict)
            else None,
            prompt_state.get("timeframe_status")
            if isinstance(prompt_state.get("timeframe_status"), dict)
            else None,
        ),
        "seed": _compact_seed_context_v2(
            prompt_state.get("seed_context")
            if isinstance(prompt_state.get("seed_context"), dict)
            else None
        ),
        "opening_grounding": _compact_opening_grounding_v2(
            prompt_state.get("opening_grounding")
            if isinstance(prompt_state.get("opening_grounding"), dict)
            else None
        ),
        "recent_steps": _compact_recent_steps_v2(
            prompt_state.get("recent_step_window")
            if isinstance(prompt_state.get("recent_step_window"), list)
            else []
        ),
        "recent_attempts": _compact_recent_attempts_v2(
            prompt_state.get("recent_attempts")
            if isinstance(prompt_state.get("recent_attempts"), list)
            else []
        ),
    }
    compact_variant["next_action_template"] = _compact_next_action_template_v2(
        next_action_template
    )
    compact_variant_v2["next_action_template"] = _compact_next_action_template_v2(
        next_action_template
    )
    compact_variant_v2["handles"] = _compact_handles_v2(
        prompt_state.get("recent_step_window")
        if isinstance(prompt_state.get("recent_step_window"), list)
        else [],
        prompt_state.get("recent_attempts")
        if isinstance(prompt_state.get("recent_attempts"), list)
        else [],
    )
    compact_variant = _canonicalize_value(compact_variant, run_dir)
    compact_variant_v2 = _canonicalize_value(compact_variant_v2, run_dir)
    return {
        "full": full_variant,
        "compact": compact_variant,
        "compact_v2": compact_variant_v2,
    }


def normalize_records(input_path: Path, output_path: Path, summary_path: Path | None = None) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    record_count = 0
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
            prompt_variants = build_prompt_variants(record)
            prompt_state = (
                record.get("prompt_state")
                if isinstance(record.get("prompt_state"), dict)
                else {}
            )
            run_info = prompt_state.get("run") if isinstance(prompt_state.get("run"), dict) else {}
            run_dir = str(run_info.get("run_dir") or "")
            normalized = dict(record)
            normalized["prompt_state_full"] = prompt_variants["full"]
            normalized["prompt_state_compact"] = prompt_variants["compact"]
            normalized["prompt_state_compact_v2"] = prompt_variants["compact_v2"]
            for key in (
                "prior_action_summary",
                "current_result_facts",
                "tool_results_summary",
                "manager_events",
                "trace_event_facts",
            ):
                if key in record:
                    normalized[key] = _pathless_model_value(record.get(key))
            normalized["target_response_normalized"] = _canonicalize_value(
                _pathless_model_value(record.get("target_response")),
                run_dir,
            )
            normalized["target_actions_normalized"] = _canonicalize_value(
                _pathless_model_value(record.get("target_actions")),
                run_dir,
            )
            normalized["action_signatures_normalized"] = _canonicalize_value(
                _pathless_model_value(record.get("action_signatures")),
                run_dir,
            )
            sink.write(json.dumps(normalized, ensure_ascii=True) + "\n")
            record_count += 1
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "record_count": record_count,
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
        description="Normalize replayed step records into prompt variants."
    )
    parser.add_argument("--input", type=Path, required=True, help="Input JSONL path.")
    parser.add_argument("--out", type=Path, required=True, help="Output JSONL path.")
    parser.add_argument(
        "--summary-out",
        type=Path,
        help="Optional JSON summary report path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    normalize_records(args.input, args.out, args.summary_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
