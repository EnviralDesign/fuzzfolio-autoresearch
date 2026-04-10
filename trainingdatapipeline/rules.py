"""Shared deterministic rule helpers for explorer training examples."""

from __future__ import annotations

from typing import Any

TYPED_TOOLS = {
    "prepare_profile",
    "mutate_profile",
    "validate_profile",
    "register_profile",
    "evaluate_candidate",
    "run_parameter_sweep",
    "inspect_artifact",
    "compare_artifacts",
}

NON_TYPED_TOOLS = {
    "run_cli",
    "read_file",
    "write_file",
    "list_dir",
    "finish",
}

RECOVERY_FAILURE_CLASSES = (
    "invalidjsonshape",
    "missingrequiredfield",
    "wrongtoolfor_state",
    "invalidclifamilyorsubcommand",
    "profilereforpathresolution_error",
    "timeframerepeatblock",
    "exploitdeadviolation",
    "finish_denied",
    "repeated_stall",
    "overuseofread_file",
)

FORCED_NEXT_ACTIONS = {
    "validate_profile",
    "register_profile",
    "evaluate_candidate",
    "inspect_artifact",
    "compare_artifacts",
}


def action_tools(record: dict[str, Any]) -> list[str]:
    actions = record.get("target_actions")
    if not isinstance(actions, list):
        return []
    tools: list[str] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        tool = str(action.get("tool") or "").strip()
        if tool:
            tools.append(tool)
    return tools


def latest_prior_step(record: dict[str, Any]) -> dict[str, Any] | None:
    prompt_state = record.get("prompt_state")
    if not isinstance(prompt_state, dict):
        return None
    recent_steps = prompt_state.get("recent_step_window")
    if not isinstance(recent_steps, list) or not recent_steps:
        return None
    latest = recent_steps[-1]
    return latest if isinstance(latest, dict) else None


def latest_prior_result_summaries(record: dict[str, Any]) -> list[dict[str, Any]]:
    latest = latest_prior_step(record)
    if not isinstance(latest, dict):
        return []
    summaries = latest.get("result_summary")
    if not isinstance(summaries, list):
        return []
    return [item for item in summaries if isinstance(item, dict)]


def latest_prior_next_recommended_action(record: dict[str, Any]) -> str | None:
    for result in latest_prior_result_summaries(record):
        action = str(result.get("next_recommended_action") or "").strip()
        if action:
            return action
    return None


def latest_prior_had_error(record: dict[str, Any]) -> bool:
    for result in latest_prior_result_summaries(record):
        if result.get("error"):
            return True
        if isinstance(result.get("errors"), list) and result.get("errors"):
            return True
        if str(result.get("tool") or "") in {"step_guard", "response_guard", "yield_guard"}:
            return True
    return False


def latest_prior_timeframe_mismatch(record: dict[str, Any]) -> dict[str, Any] | None:
    for result in latest_prior_result_summaries(record):
        mismatch = result.get("timeframe_mismatch")
        if isinstance(mismatch, dict) and mismatch:
            return mismatch
    prompt_state = record.get("prompt_state")
    if not isinstance(prompt_state, dict):
        return None
    timeframe_status = prompt_state.get("timeframe_status")
    if not isinstance(timeframe_status, dict):
        return None
    latest = timeframe_status.get("latest")
    return latest if isinstance(latest, dict) else None


def prompt_timeframe_repeat_blocked(record: dict[str, Any]) -> bool:
    prompt_state = record.get("prompt_state")
    if not isinstance(prompt_state, dict):
        return False
    timeframe_status = prompt_state.get("timeframe_status")
    if not isinstance(timeframe_status, dict):
        return False
    return bool(timeframe_status.get("repeat_blocked"))


def current_result_facts(record: dict[str, Any]) -> list[dict[str, Any]]:
    facts = record.get("current_result_facts")
    if not isinstance(facts, list):
        return []
    return [item for item in facts if isinstance(item, dict)]


def current_result_tools(record: dict[str, Any]) -> set[str]:
    return {str(item.get("tool") or "") for item in current_result_facts(record)}


def has_marker(record: dict[str, Any], key: str) -> bool:
    markers = record.get("trace_markers")
    if not isinstance(markers, dict):
        return False
    return bool(markers.get(key))


def is_typed_tool_only(record: dict[str, Any]) -> bool:
    tools = action_tools(record)
    return bool(tools) and all(tool in TYPED_TOOLS for tool in tools)


def is_run_cli_only(record: dict[str, Any]) -> bool:
    tools = action_tools(record)
    return bool(tools) and all(tool == "run_cli" for tool in tools)


def is_read_or_list_only(record: dict[str, Any]) -> bool:
    tools = action_tools(record)
    return bool(tools) and all(tool in {"read_file", "list_dir"} for tool in tools)


def repeated_browse_loop(record: dict[str, Any]) -> bool:
    if not is_read_or_list_only(record):
        return False
    latest = latest_prior_step(record)
    if not isinstance(latest, dict):
        return False
    previous_actions = latest.get("action_signatures")
    if not isinstance(previous_actions, list) or not previous_actions:
        return False
    prior_tools = {str(item.get("tool") or "") for item in previous_actions if isinstance(item, dict)}
    return bool(prior_tools) and prior_tools.issubset({"read_file", "list_dir"})


def deterministic_followup_target(record: dict[str, Any]) -> str | None:
    next_action = latest_prior_next_recommended_action(record)
    if not next_action:
        return None
    if next_action in FORCED_NEXT_ACTIONS:
        return next_action
    return None


def current_first_tool(record: dict[str, Any]) -> str | None:
    tools = action_tools(record)
    return tools[0] if tools else None

