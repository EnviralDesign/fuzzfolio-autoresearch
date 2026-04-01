"""Runtime bookkeeping for manager invocations (observability + idempotency hints)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .manager_models import ManagerHookEvent


@dataclass
class ManagerRuntimeState:
    """Persisted in controller and surfaced under runtime-state.json["manager"]."""

    last_hook: str | None = None
    last_hook_step: int | None = None
    last_rationale: str | None = None
    last_actions_applied: list[dict[str, Any]] = field(default_factory=list)
    last_raw_ok: bool | None = None
    last_error: str | None = None
    invocation_incomplete: bool = False
    manager_notes: list[str] = field(default_factory=list)

    def record_invocation(
        self,
        *,
        hook: ManagerHookEvent,
        step: int,
        rationale: str | None,
        actions_applied: list[dict[str, Any]],
        raw_ok: bool,
        error: str | None,
        invocation_incomplete: bool,
    ) -> None:
        self.last_hook = hook.value
        self.last_hook_step = step
        self.last_rationale = (rationale or "")[:4000] or None
        self.last_actions_applied = actions_applied
        self.last_raw_ok = raw_ok
        self.last_error = error
        self.invocation_incomplete = invocation_incomplete

    def to_snapshot_dict(self) -> dict[str, Any]:
        return {
            "last_hook": self.last_hook,
            "last_hook_step": self.last_hook_step,
            "last_rationale": self.last_rationale,
            "last_actions_applied": list(self.last_actions_applied),
            "last_raw_ok": self.last_raw_ok,
            "last_error": self.last_error,
            "invocation_incomplete": self.invocation_incomplete,
            "manager_notes_tail": self.manager_notes[-12:],
        }
