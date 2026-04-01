"""Typed manager control plane: hooks, packets, decisions, actions.

The manager is not a second explorer. It returns only branch-control actions
that the controller applies deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ManagerHookEvent(str, Enum):
    """Events that may trigger a manager LLM call."""

    after_scored_eval = "after_scored_eval"
    after_validation_eval = "after_validation_eval"
    on_timeframe_mismatch = "on_timeframe_mismatch"
    on_unresolved_validation = "on_unresolved_validation"
    on_explicit_retention_fail = "on_explicit_retention_fail"
    on_candidate_frontier_change = "on_candidate_frontier_change"
    on_stale_validation_without_validated = "on_stale_validation_without_validated"
    before_wrap_up_decision = "before_wrap_up_decision"


class ManagerActionKind(str, Enum):
    set_provisional_leader = "set_provisional_leader"
    set_validated_leader = "set_validated_leader"
    clear_provisional_leader = "clear_provisional_leader"
    clear_validated_leader = "clear_validated_leader"
    demote_family = "demote_family"
    suppress_family = "suppress_family"
    clear_suppression = "clear_suppression"
    mark_retryable = "mark_retryable"
    mark_unresolved = "mark_unresolved"
    start_reseed_window = "start_reseed_window"
    stop_reseed_window = "stop_reseed_window"
    set_budget_mode = "set_budget_mode"
    attach_manager_note = "attach_manager_note"


@dataclass
class ManagerAction:
    kind: ManagerActionKind
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class ManagerDecision:
    """Parsed manager output."""

    rationale: str
    actions: list[ManagerAction]
    confidence: str = "medium"


@dataclass
class ManagerPacket:
    """Structured packet; converted to JSON for the manager prompt."""

    hook: ManagerHookEvent
    step: int
    step_limit: int
    phase: str
    budget_mode: str | None
    reseed_active: bool
    validation_stale_steps: int
    frontier_best_score: float | None
    frontier_prior_best: float | None
    provisional_leader_family_id: str | None
    validated_leader_family_id: str | None
    last_validation_digest: dict[str, Any] | None
    candidate_families: list[dict[str, Any]]
    recent_issues: list[str]
    extra: dict[str, Any] = field(default_factory=dict)

    def to_llm_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "hook": self.hook.value,
            "step": self.step,
            "step_limit": self.step_limit,
            "phase": self.phase,
            "budget_mode": self.budget_mode,
            "reseed_active": self.reseed_active,
            "validation_stale_without_validated_steps": self.validation_stale_steps,
            "frontier_best_score": self.frontier_best_score,
            "frontier_prior_best": self.frontier_prior_best,
            "provisional_leader_family_id": self.provisional_leader_family_id,
            "validated_leader_family_id": self.validated_leader_family_id,
            "last_scored_validation_digest": self.last_validation_digest,
            "candidate_families": self.candidate_families,
            "recent_issues": self.recent_issues,
        }
        if self.extra:
            d["extra"] = self.extra
        return d
