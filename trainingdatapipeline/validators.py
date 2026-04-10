"""Validation helpers for replayed and labeled training examples."""

from __future__ import annotations

from typing import Any


def has_valid_target_response(record: dict[str, Any]) -> bool:
    target = record.get("target_response")
    if not isinstance(target, dict):
        return False
    reasoning = target.get("reasoning")
    actions = target.get("actions")
    return isinstance(reasoning, str) and isinstance(actions, list)


def known_action_tools(record: dict[str, Any], allowed_tools: set[str]) -> bool:
    actions = record.get("target_actions")
    if not isinstance(actions, list):
        return False
    for action in actions:
        if not isinstance(action, dict):
            return False
        tool = str(action.get("tool") or "").strip()
        if not tool or tool not in allowed_tools:
            return False
    return True


def has_non_empty_reasoning(record: dict[str, Any]) -> bool:
    target = record.get("target_response")
    if not isinstance(target, dict):
        return False
    return bool(str(target.get("reasoning") or "").strip())


def has_placeholder_leakage(value: Any) -> bool:
    if isinstance(value, str):
        return "<RUN_DIR>" in value or "<PROFILES_DIR>" in value or "<EVALS_DIR>" in value
    if isinstance(value, list):
        return any(has_placeholder_leakage(item) for item in value)
    if isinstance(value, dict):
        return any(has_placeholder_leakage(item) for item in value.values())
    return False

