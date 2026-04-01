"""Manager hook dispatcher (thin seam for future policy)."""

from __future__ import annotations

from typing import Any

from .manager_models import ManagerHookEvent


def should_consider_hook(
    _hook: ManagerHookEvent,
    *,
    manager_enabled: bool,
    manager_profiles_non_empty: bool,
) -> bool:
    if not manager_enabled or not manager_profiles_non_empty:
        return False
    # All hook types are eligible when manager is configured.
    return True


def hook_context_extra(hook: ManagerHookEvent, payload: dict[str, Any]) -> dict[str, Any]:
    """Optional enrichment for tracing; no policy here."""
    return {"hook": hook.value, **payload}


def select_post_eval_hook(
    *,
    had_timeframe_mismatch: bool,
    explicit_retention_fail: bool,
    unresolved_validation: bool,
    frontier_improved: bool,
) -> ManagerHookEvent:
    """Choose the most specific post-eval hook for the current evidence."""

    if explicit_retention_fail:
        return ManagerHookEvent.on_explicit_retention_fail
    if had_timeframe_mismatch:
        return ManagerHookEvent.on_timeframe_mismatch
    if unresolved_validation:
        return ManagerHookEvent.on_unresolved_validation
    if frontier_improved:
        return ManagerHookEvent.on_candidate_frontier_change
    return ManagerHookEvent.after_validation_eval
