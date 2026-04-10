"""Replay pre-step controller-visible state from historical run artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .replay_types import DiscoveredRun


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _shorten(text: Any, limit: int = 220) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


LEGACY_MODEL_PATH_FIELDS = frozenset(
    {"profile_path", "destination_path", "source_profile_path", "metadata_out_path"}
)


def _candidate_name_from_path_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = str(value or "").strip().replace("/", "\\")
    if not text:
        return None
    return Path(text).stem or None


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
            handle = _candidate_name_from_path_value(match.group(1))
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


def _pathless_payload(value: Any) -> Any:
    if isinstance(value, str):
        stripped = str(value or "").strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, (dict, list)):
                return json.dumps(_pathless_payload(payload), ensure_ascii=True)
        return _rewrite_legacy_path_fields_in_text(stripped)
    if isinstance(value, list):
        return [_pathless_payload(item) for item in value]
    if not isinstance(value, dict):
        return value
    normalized = {str(key): _pathless_payload(item) for key, item in value.items()}
    candidate_name = normalized.get("candidate_name")
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        candidate_name = _candidate_name_from_path_value(
            normalized.get("profile_path")
        ) or _candidate_name_from_path_value(normalized.get("destination_path"))
    if isinstance(candidate_name, str) and candidate_name.strip():
        normalized["candidate_name"] = candidate_name
    source_candidate_name = normalized.get("source_candidate_name")
    if not isinstance(source_candidate_name, str) or not source_candidate_name.strip():
        source_candidate_name = _candidate_name_from_path_value(
            normalized.get("source_profile_path")
        )
    if isinstance(source_candidate_name, str) and source_candidate_name.strip():
        normalized["source_candidate_name"] = source_candidate_name
    destination_candidate_name = normalized.get("destination_candidate_name")
    if (
        not isinstance(destination_candidate_name, str)
        or not destination_candidate_name.strip()
    ):
        destination_candidate_name = _candidate_name_from_path_value(
            normalized.get("destination_path")
        )
    if (
        isinstance(destination_candidate_name, str)
        and destination_candidate_name.strip()
    ):
        normalized["destination_candidate_name"] = destination_candidate_name
    tool = str(normalized.get("tool") or "").strip()
    mode = str(normalized.get("mode") or "").strip()
    if tool == "prepare_profile" and mode == "clone_local":
        normalized.pop("candidate_name", None)
    elif tool == "prepare_profile":
        normalized.pop("destination_candidate_name", None)
    elif tool in {"validate_profile", "register_profile", "evaluate_candidate"}:
        normalized.pop("destination_candidate_name", None)
    for field in LEGACY_MODEL_PATH_FIELDS:
        normalized.pop(field, None)
    if tool and "signature" in normalized:
        signature_payload = {
            key: item for key, item in normalized.items() if key != "signature"
        }
        normalized["signature"] = _shorten(
            json.dumps(signature_payload, ensure_ascii=True, sort_keys=True),
            220,
        )
    return normalized


def _action_summary(action: Any) -> str:
    if not isinstance(action, dict):
        return _shorten(action, 160)
    tool = str(action.get("tool") or "").strip() or "unknown"
    if tool == "run_cli":
        args = action.get("args")
        if isinstance(args, list):
            return _shorten("run_cli " + " ".join(str(item) for item in args), 200)
    if tool in {"read_file", "write_file", "list_dir"}:
        target = str(action.get("path") or "").strip()
        return _shorten(f"{tool} {target}", 200)
    pathless_action = _pathless_payload(
        {key: value for key, value in action.items() if key != "content"}
    )
    rendered = json.dumps(
        pathless_action,
        ensure_ascii=True,
    )
    return _shorten(rendered, 200)


def _result_summary(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"kind": "unknown", "summary": _shorten(result, 200)}
    summary: dict[str, Any] = {
        "tool": str(result.get("tool") or "unknown"),
    }
    if "ok" in result:
        summary["ok"] = bool(result.get("ok"))
    if result.get("error"):
        summary["error"] = _shorten(result.get("error"), 240)
    if result.get("errors"):
        summary["errors"] = [
            _shorten(item, 120) for item in list(result.get("errors") or [])[:3]
        ]
    if result.get("next_recommended_action"):
        summary["next_recommended_action"] = str(
            result.get("next_recommended_action")
        )
    if result.get("created_profile_ref"):
        summary["created_profile_ref"] = str(result.get("created_profile_ref"))
    if result.get("profile_ref"):
        summary["profile_ref"] = str(result.get("profile_ref"))
    if result.get("attempt_id"):
        summary["attempt_id"] = str(result.get("attempt_id"))
    if result.get("artifact_dir"):
        summary["artifact_dir"] = str(result.get("artifact_dir"))
    if result.get("score") is not None:
        summary["score"] = result.get("score")
    if result.get("ready_for_registration") is not None:
        summary["ready_for_registration"] = bool(result.get("ready_for_registration"))
    if result.get("ready_to_evaluate") is not None:
        summary["ready_to_evaluate"] = bool(result.get("ready_to_evaluate"))
    if isinstance(result.get("timeframe_mismatch"), dict):
        summary["timeframe_mismatch"] = {
            key: result["timeframe_mismatch"].get(key)
            for key in ("requested", "effective", "mismatch", "message")
            if key in result["timeframe_mismatch"]
        }
    auto_log = result.get("auto_log")
    if isinstance(auto_log, dict):
        summary["auto_log"] = {
            key: auto_log.get(key)
            for key in (
                "status",
                "attempt_id",
                "composite_score",
                "primary_score",
                "score_basis",
                "artifact_dir",
            )
            if key in auto_log
        }
    return _pathless_payload(summary)


def _action_signature(action: Any) -> dict[str, Any]:
    if not isinstance(action, dict):
        return {"tool": "unknown", "signature": _shorten(action, 160)}
    tool = str(action.get("tool") or "").strip() or "unknown"
    signature: dict[str, Any] = {"tool": tool}
    if tool == "run_cli":
        args = action.get("args")
        if isinstance(args, list):
            signature["args_head"] = [str(item) for item in args[:6]]
            signature["signature"] = _shorten(
                "run_cli " + " ".join(str(item) for item in args[:10]),
                200,
            )
        else:
            command = str(action.get("command") or "").strip()
            signature["signature"] = _shorten(f"run_cli {command}", 200)
        return signature
    candidate_name = action.get("candidate_name")
    if not isinstance(candidate_name, str) or not candidate_name.strip():
        for path_key in ("profile_path", "destination_path"):
            path_value = action.get(path_key)
            if isinstance(path_value, str) and path_value.strip():
                candidate_name = Path(path_value).stem
                break
    destination_candidate_name = action.get("destination_candidate_name")
    if (
        not isinstance(destination_candidate_name, str)
        or not destination_candidate_name.strip()
    ):
        path_value = action.get("destination_path")
        if isinstance(path_value, str) and path_value.strip():
            destination_candidate_name = Path(path_value).stem
    for key in (
        "profile_ref",
        "attempt_id",
        "artifact_dir",
        "inspect_ref",
        "timeframe",
        "timeframe_policy",
        "mode",
        "operation",
        "view",
        "candidate_name",
        "destination_candidate_name",
        "candidate_name_prefix",
        "requested_horizon_months",
        "evaluation_mode",
    ):
        if key == "candidate_name":
            value = candidate_name
        elif key == "destination_candidate_name":
            value = destination_candidate_name
        else:
            value = action.get(key)
        if value is not None:
            signature[key] = value
    if isinstance(action.get("instruments"), list):
        signature["instruments"] = [
            str(item) for item in list(action.get("instruments") or [])[:6]
        ]
    if tool == "run_parameter_sweep" and isinstance(action.get("axes"), list):
        signature["axes"] = [str(item) for item in list(action.get("axes") or [])[:8]]
    if tool in {"prepare_profile", "mutate_profile"} and isinstance(
        action.get("indicator_ids"), list
    ):
        signature["indicator_ids"] = [
            str(item) for item in list(action.get("indicator_ids") or [])[:8]
        ]
    signature["signature"] = _shorten(
        json.dumps(signature, ensure_ascii=True, sort_keys=True),
        220,
    )
    return signature


def _result_facts(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"tool": "unknown", "raw_summary": _shorten(result, 240)}
    facts: dict[str, Any] = {
        "tool": str(result.get("tool") or "unknown"),
    }
    for key in (
        "ok",
        "status",
        "next_recommended_action",
        "created_profile_ref",
        "profile_ref",
        "attempt_id",
        "artifact_dir",
        "artifact_kind",
        "inspect_ref",
        "score",
        "score_basis",
        "ready_for_registration",
        "ready_to_evaluate",
        "requested_timeframe",
        "effective_timeframe",
        "requested_horizon_months",
        "effective_window_months",
        "timeframe_auto_adjusted",
        "candidate_summary",
        "material_changes",
        "controller_hint",
        "suggested_next_move",
        "compare_summary",
        "sweep_summary",
        "branch_lifecycle_after_eval",
        "validation_evidence",
        "retention_relevant_flags",
        "attempt_logged",
        "created",
        "updated",
        "view",
        "artifact_resolution",
    ):
        if key in result and result.get(key) is not None:
            facts[key] = result.get(key)
    if result.get("error"):
        facts["error"] = _shorten(result.get("error"), 320)
    if isinstance(result.get("errors"), list):
        facts["errors"] = [_shorten(item, 160) for item in result.get("errors", [])[:5]]
    if isinstance(result.get("warnings"), list):
        facts["warnings"] = [
            _shorten(item, 160) for item in result.get("warnings", [])[:5]
        ]
    if isinstance(result.get("timeframe_mismatch"), dict):
        facts["timeframe_mismatch"] = {
            key: result["timeframe_mismatch"].get(key)
            for key in ("requested", "effective", "mismatch", "message", "source")
            if key in result["timeframe_mismatch"]
        }
    if isinstance(result.get("auto_log"), dict):
        facts["auto_log"] = {
            key: result["auto_log"].get(key)
            for key in (
                "status",
                "attempt_id",
                "composite_score",
                "primary_score",
                "score_basis",
                "artifact_dir",
                "requested_timeframe",
                "effective_timeframe",
                "effective_window_months",
                "requested_horizon_months",
            )
            if key in result["auto_log"]
        }
    if isinstance(result.get("ranked_comparison"), list):
        facts["ranked_comparison_head"] = list(result.get("ranked_comparison") or [])[:3]
    if isinstance(result.get("ranked_results"), list):
        facts["ranked_results_head"] = list(result.get("ranked_results") or [])[:3]
    if isinstance(result.get("best_variant"), dict):
        facts["best_variant"] = result.get("best_variant")
    return _pathless_payload(facts)


def _seed_context(seed_prompt: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(seed_prompt, dict):
        return None
    exploration_goal = (
        seed_prompt.get("exploration_goal")
        if isinstance(seed_prompt.get("exploration_goal"), dict)
        else {}
    )
    worker_split = exploration_goal.get("worker_split")
    summarized_split: list[dict[str, str]] = []
    if isinstance(worker_split, list):
        for item in worker_split[:4]:
            if not isinstance(item, dict):
                continue
            branch = str(item.get("branch") or "").strip()
            goal = str(item.get("goal") or "").strip()
            if branch or goal:
                summarized_split.append({"branch": branch, "goal": goal})
    return {
        "seed": seed_prompt.get("seed"),
        "catalog_note": seed_prompt.get("catalog_note"),
        "exploration_goal_id": exploration_goal.get("id"),
        "exploration_goal_summary": exploration_goal.get("summary"),
        "seed_indicators": list(seed_prompt.get("indicators") or [])[:20]
        if isinstance(seed_prompt.get("indicators"), list)
        else [],
        "timeframes": list(seed_prompt.get("timeframes") or [])[:12]
        if isinstance(seed_prompt.get("timeframes"), list)
        else [],
        "single_tf_suggestions": list(seed_prompt.get("single_tf_suggestions") or [])[
            :8
        ]
        if isinstance(seed_prompt.get("single_tf_suggestions"), list)
        else [],
        "multi_tf_suggestions": list(seed_prompt.get("multi_tf_suggestions") or [])[:8]
        if isinstance(seed_prompt.get("multi_tf_suggestions"), list)
        else [],
        "worker_split": summarized_split,
    }


def _run_metadata_context(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    keys = (
        "run_id",
        "created_at",
        "quality_score_preset",
        "explorer_profile",
        "explorer_provider",
        "explorer_model",
        "supervisor_profile",
        "supervisor_provider",
        "supervisor_model",
    )
    return {key: metadata.get(key) for key in keys if key in metadata}


def _attempt_summary(attempt: dict[str, Any]) -> dict[str, Any]:
    best_summary = (
        attempt.get("best_summary") if isinstance(attempt.get("best_summary"), dict) else {}
    )
    market_window = (
        best_summary.get("market_data_window")
        if isinstance(best_summary.get("market_data_window"), dict)
        else {}
    )
    return {
        "sequence": attempt.get("sequence"),
        "attempt_id": attempt.get("attempt_id"),
        "candidate_name": attempt.get("candidate_name"),
        "profile_ref": attempt.get("profile_ref"),
        "composite_score": attempt.get("composite_score"),
        "primary_score": attempt.get("primary_score"),
        "score_basis": attempt.get("score_basis"),
        "artifact_dir": attempt.get("artifact_dir"),
        "effective_window_months": market_window.get("effective_window_months"),
        "timeframe": best_summary.get("timeframe"),
    }


def _trace_flags(trace_entries: list[dict[str, Any]]) -> dict[str, Any]:
    phases = {str(item.get("phase") or "") for item in trace_entries}
    statuses = {str(item.get("status") or "") for item in trace_entries}
    return {
        "response_repair_triggered": "response_repair" in phases,
        "payload_shape_repair_triggered": "payload_shape_repair" in phases,
        "response_guard_blocked": "response_guard" in phases,
        "step_guard_triggered": "step_guard" in phases,
        "manager_phase_seen": "manager" in phases,
        "trace_phases": sorted(phase for phase in phases if phase),
        "trace_statuses": sorted(status for status in statuses if status),
    }


def _trace_event_facts(trace_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    interesting_phases = {
        "response_repair",
        "payload_shape_repair",
        "response_guard",
        "step_guard",
        "finish",
        "action_execution",
    }
    for item in trace_entries:
        phase = str(item.get("phase") or "").strip()
        status = str(item.get("status") or "").strip()
        if phase not in interesting_phases:
            continue
        if phase == "action_execution" and status not in {"action_failed"}:
            continue
        fact = {
            "phase": phase,
            "status": status,
            "message": _shorten(item.get("message"), 240),
        }
        for key in ("error", "action", "tool"):
            value = item.get(key)
            if value:
                fact[key] = _shorten(value, 240)
        if item.get("error_count") is not None:
            fact["error_count"] = item.get("error_count")
        if item.get("ok") is not None:
            fact["ok"] = bool(item.get("ok"))
        facts.append(fact)
    return facts


def _timeframe_status_snapshot(
    prior_timeframe_mismatches: list[dict[str, Any]],
) -> dict[str, Any]:
    if not prior_timeframe_mismatches:
        return {
            "has_mismatch": False,
            "repeat_blocked": False,
            "total_mismatches": 0,
            "latest": None,
        }
    latest = prior_timeframe_mismatches[-1]
    requested = str(latest.get("requested") or "").strip().lower()
    repeat_count = 0
    if requested:
        repeat_count = sum(
            1
            for item in prior_timeframe_mismatches
            if str(item.get("requested") or "").strip().lower() == requested
        )
    return {
        "has_mismatch": True,
        "repeat_blocked": repeat_count >= 2,
        "total_mismatches": len(prior_timeframe_mismatches),
        "latest": {
            key: latest.get(key)
            for key in ("requested", "effective", "mismatch", "message", "source")
        },
    }


def _current_step_markers(
    payload: dict[str, Any], trace_entries: list[dict[str, Any]]
) -> dict[str, Any]:
    actions = payload.get("actions") if isinstance(payload.get("actions"), list) else []
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    tool_names = [
        str(action.get("tool") or "").strip()
        for action in actions
        if isinstance(action, dict)
    ]
    flags = _trace_flags(trace_entries)
    flags.update(
        {
            "contains_run_cli": "run_cli" in tool_names,
            "contains_typed_tool": any(
                name
                in {
                    "prepare_profile",
                    "mutate_profile",
                    "validate_profile",
                    "register_profile",
                    "evaluate_candidate",
                    "run_parameter_sweep",
                    "inspect_artifact",
                    "compare_artifacts",
                }
                for name in tool_names
            ),
            "contains_read_file": "read_file" in tool_names,
            "contains_list_dir": "list_dir" in tool_names,
            "contains_write_file": "write_file" in tool_names,
            "contains_finish": "finish" in tool_names,
            "manager_event_count": len(payload.get("manager_events") or [])
            if isinstance(payload.get("manager_events"), list)
            else 0,
            "hard_action_failure": any(
                isinstance(result, dict) and result.get("ok") is False for result in results
            ),
            "auto_log_present": any(
                isinstance(result, dict) and isinstance(result.get("auto_log"), dict)
                for result in results
            ),
            "timeframe_mismatch_present": any(
                isinstance(result, dict) and isinstance(result.get("timeframe_mismatch"), dict)
                for result in results
            ),
        }
    )
    return flags


def replay_run_steps(run: DiscoveredRun, *, recent_step_limit: int = 6, recent_attempt_limit: int = 8) -> list[dict[str, Any]]:
    run_dir = run.run_dir
    controller_steps = _load_jsonl(run_dir / "controller-log.jsonl")
    trace_rows = _load_jsonl(run_dir / "runtime-trace.jsonl")
    attempts = _load_jsonl(run_dir / "attempts.jsonl")
    seed_prompt = _load_json_if_exists(run_dir / "seed-prompt.json")
    run_metadata = _load_json_if_exists(run_dir / "run-metadata.json")

    trace_by_step: dict[int, list[dict[str, Any]]] = {}
    for row in trace_rows:
        step = row.get("step")
        if isinstance(step, int):
            trace_by_step.setdefault(step, []).append(row)

    attempts_with_time: list[tuple[datetime | None, dict[str, Any]]] = [
        (_parse_iso(item.get("created_at")), item) for item in attempts
    ]
    attempts_with_time.sort(
        key=lambda item: (
            item[0] is None,
            item[0] or datetime.min,
            int(item[1].get("sequence") or 0),
        )
    )

    replayed: list[dict[str, Any]] = []
    prior_steps: list[dict[str, Any]] = []
    prior_timeframe_mismatches: list[dict[str, Any]] = []
    seed_context = _seed_context(seed_prompt)
    metadata_context = _run_metadata_context(run_metadata)

    for payload in controller_steps:
        step = payload.get("step")
        if not isinstance(step, int):
            continue
        phase = str(payload.get("phase") or "").strip()
        timestamp_text = str(payload.get("timestamp") or "").strip() or None
        step_ts = _parse_iso(timestamp_text)
        prior_attempts = [
            attempt
            for created_at, attempt in attempts_with_time
            if created_at is not None and step_ts is not None and created_at < step_ts
        ]
        if step_ts is None:
            prior_attempts = [attempt for _created_at, attempt in attempts_with_time]
        prompt_state = {
            "run": {
                "run_id": run.run_id,
                "run_dir": str(run.run_dir),
                "parsed_started_at": run.parsed_started_at,
                "artifact_inventory": run.artifact_inventory.to_dict(),
            },
            "controller": {
                "step": step,
                "phase": phase,
                "horizon_target": payload.get("horizon_target"),
                "score_target": payload.get("score_target"),
            },
            "seed_context": seed_context,
            "run_metadata": metadata_context,
            "timeframe_status": _timeframe_status_snapshot(prior_timeframe_mismatches),
            "recent_step_window": prior_steps[-recent_step_limit:],
            "recent_attempts": [
                _attempt_summary(item) for item in prior_attempts[-recent_attempt_limit:]
            ],
        }
        actions = payload.get("actions") if isinstance(payload.get("actions"), list) else []
        results = payload.get("results") if isinstance(payload.get("results"), list) else []
        manager_events = (
            payload.get("manager_events") if isinstance(payload.get("manager_events"), list) else []
        )
        trace_entries = trace_by_step.get(step, [])
        action_signatures = [_action_signature(action) for action in actions]
        current_result_facts = [_result_facts(item) for item in results]
        record = {
            "example_id": f"{run.run_id}-step-{step:05d}",
            "run_id": run.run_id,
            "step": step,
            "timestamp": timestamp_text,
            "phase": phase,
            "source_type": "realrun",
            "prompt_state": prompt_state,
            "target_response": {
                "reasoning": str(payload.get("reasoning") or "").strip(),
                "actions": actions,
            },
            "target_actions": actions,
            "action_signatures": action_signatures,
            "target_reasoning_short": _shorten(payload.get("reasoning"), 240),
            "prior_action_summary": [
                item["action_summary"] for item in prior_steps[-1:]
            ][0]
            if prior_steps
            else [],
            "current_result_facts": current_result_facts,
            "tool_results_summary": [_result_summary(item) for item in results[:8]],
            "manager_events": manager_events,
            "trace_markers": _current_step_markers(payload, trace_entries),
            "trace_event_facts": _trace_event_facts(trace_entries),
            "provenance": {
                "controller_log_path": str(run_dir / "controller-log.jsonl"),
                "runtime_trace_path": str(run_dir / "runtime-trace.jsonl")
                if (run_dir / "runtime-trace.jsonl").exists()
                else None,
                "attempts_path": str(run_dir / "attempts.jsonl")
                if (run_dir / "attempts.jsonl").exists()
                else None,
                "seed_prompt_path": str(run_dir / "seed-prompt.json")
                if (run_dir / "seed-prompt.json").exists()
                else None,
                "run_metadata_path": str(run_dir / "run-metadata.json")
                if (run_dir / "run-metadata.json").exists()
                else None,
            },
        }
        replayed.append(record)
        prior_steps.append(
            {
                "step": step,
                "phase": phase,
                "reasoning_short": _shorten(payload.get("reasoning"), 180),
                "action_summary": [_action_summary(action) for action in actions[:3]],
                "action_signatures": action_signatures[:3],
                "result_summary": [_result_summary(result) for result in results[:4]],
                "manager_event_count": len(manager_events),
            }
        )
        for result in current_result_facts:
            mismatch = result.get("timeframe_mismatch")
            if isinstance(mismatch, dict) and mismatch:
                prior_timeframe_mismatches.append(mismatch)
    return replayed
