"""Offline validation wrapper for relabel candidates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from autoresearch.config import load_config
from autoresearch.controller import ResearchController, RunPolicy, ToolContext


@dataclass
class OfflineValidationResult:
    ok: bool
    normalized_response: dict[str, Any] | None
    errors: list[str]
    warnings: list[str]
    stateful_checks: dict[str, str]


def _build_tool_context(record: dict[str, Any]) -> ToolContext:
    prompt_state = record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    run_info = prompt_state.get("run") if isinstance(prompt_state.get("run"), dict) else {}
    run_dir = Path(str(run_info.get("run_dir") or ".")).resolve()
    return ToolContext(
        run_id=str(record.get("run_id") or run_info.get("run_id") or ""),
        run_dir=run_dir,
        attempts_path=run_dir / "attempts.jsonl",
        run_metadata_path=run_dir / "run-metadata.json",
        profiles_dir=run_dir / "profiles",
        evals_dir=run_dir / "evals",
        notes_dir=run_dir / "notes",
        progress_plot_path=run_dir / "progress.png",
        cli_help_catalog_path=run_dir / "cli-help-catalog.json",
        seed_prompt_path=(run_dir / "seed-prompt.json") if (run_dir / "seed-prompt.json").exists() else None,
        profile_template_path=Path(load_config().repo_root / "portable_profile_template.json"),
        indicator_catalog_summary=None,
        seed_indicator_parameter_hints=None,
        instrument_catalog_summary=None,
    )


def _build_controller(record: dict[str, Any]) -> ResearchController:
    config = load_config()
    controller = object.__new__(ResearchController)
    controller.config = config
    controller._timeframe_mismatches = []
    controller._family_branches = {}
    controller._branch_overlay = SimpleNamespace(
        validated_leader_family_id=None,
        budget_mode=None,
    )
    controller.profile_sources = {}
    controller.finish_denials = 0
    prompt_state = record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    timeframe_status = prompt_state.get("timeframe_status") if isinstance(prompt_state.get("timeframe_status"), dict) else {"has_mismatch": False}
    controller._get_timeframe_mismatch_status = lambda: timeframe_status
    recent_payloads = []
    for item in prompt_state.get("recent_step_window") or []:
        if not isinstance(item, dict):
            continue
        recent_payloads.append(
            {
                "actions": list(item.get("action_signatures") or []),
                "results": list(item.get("result_summary") or []),
            }
        )
    controller._load_recent_step_payloads = lambda _tool_context, limit: recent_payloads[-limit:]
    attempts = prompt_state.get("recent_attempts") if isinstance(prompt_state.get("recent_attempts"), list) else []
    controller._run_attempts = lambda _run_id: list(attempts)
    controller._score_target_snapshot = lambda _tool_context: {"summary": str(record.get("score_target") or "")}
    return controller


def _custom_repeat_check(record: dict[str, Any], actions: list[dict[str, Any]]) -> list[str]:
    prompt_state = record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    recent_steps = prompt_state.get("recent_step_window") if isinstance(prompt_state.get("recent_step_window"), list) else []
    if len(recent_steps) < 3:
        return []
    current_tools = [str(action.get("tool") or "") for action in actions if isinstance(action, dict)]
    if not current_tools:
        return []
    comparable = []
    for step in recent_steps[-3:]:
        if not isinstance(step, dict):
            return []
        sigs = step.get("action_signatures")
        if not isinstance(sigs, list):
            return []
        comparable.append([str(item.get("tool") or "") for item in sigs if isinstance(item, dict)])
    if all(item == current_tools for item in comparable):
        return ["Candidate repeats the same action tool sequence as the last 3 steps."]
    return []


def _custom_finish_check(record: dict[str, Any], actions: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    phase = str(record.get("phase") or "").strip()
    prompt_state = record.get("prompt_state") if isinstance(record.get("prompt_state"), dict) else {}
    recent_attempts = prompt_state.get("recent_attempts") if isinstance(prompt_state.get("recent_attempts"), list) else []
    for index, action in enumerate(actions, start=1):
        if not isinstance(action, dict) or str(action.get("tool") or "") != "finish":
            continue
        summary = str(action.get("summary") or "").strip()
        if not summary:
            errors.append(f"Action {index}: finish requires a non-empty summary.")
        if phase != "wrap_up":
            errors.append(f"Action {index}: finish is not allowed outside wrap_up phase.")
        if len(recent_attempts) < 4:
            errors.append(
                f"Action {index}: finish requires more evaluated attempts before stopping."
            )
    return errors


def validate_candidate_response(
    record: dict[str, Any],
    candidate_payload: dict[str, Any] | list[Any],
) -> OfflineValidationResult:
    controller = _build_controller(record)
    tool_context = _build_tool_context(record)
    policy = RunPolicy(mode_name="relabel", allow_finish=True)
    warnings: list[str] = []
    stateful_checks = {
        "action_shape": "checked",
        "finish_policy": "checked",
        "timeframe_repeat": "checked",
        "repeat_loop": "checked",
        "branch_lifecycle": "unverifiable_without_branch_snapshot",
    }
    try:
        normalized = controller._normalize_model_response(candidate_payload)
    except Exception as exc:
        return OfflineValidationResult(
            ok=False,
            normalized_response=None,
            errors=[str(exc)],
            warnings=warnings,
            stateful_checks=stateful_checks,
        )
    actions = normalized.get("actions")
    errors = list(controller._validate_actions(actions))
    errors.extend(_custom_finish_check(record, actions if isinstance(actions, list) else []))
    if isinstance(actions, list):
        errors.extend(controller._validate_timeframe_mismatch_block(actions))
        errors.extend(_custom_repeat_check(record, actions))
    if stateful_checks["branch_lifecycle"].startswith("unverifiable"):
        warnings.append("Branch lifecycle gating was not checked offline for this candidate.")
    return OfflineValidationResult(
        ok=not errors,
        normalized_response=normalized if isinstance(normalized, dict) else None,
        errors=errors,
        warnings=warnings,
        stateful_checks=stateful_checks,
    )
