"""Controller-owned branch lifecycle state keyed by indicator family_id."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


LIFECYCLE_SCOUT = "scout"
LIFECYCLE_PROVISIONAL_CONTENDER = "provisional_contender"
LIFECYCLE_PROVISIONAL_LEADER = "provisional_leader"
LIFECYCLE_VALIDATED_LEADER = "validated_leader"
LIFECYCLE_RETENTION_WARNING = "retention_warning"
LIFECYCLE_COLLAPSED = "collapsed"
LIFECYCLE_BANKRUPT_COOLDOWN = "bankrupt_cooldown"
LIFECYCLE_RESEED_ELIGIBLE = "reseed_eligible"

PROMOTION_SCOUT = "scout"
PROMOTION_PROVISIONAL = "provisional"
PROMOTION_VALIDATED = "validated"

RETENTION_UNTESTED = "untested"
RETENTION_PENDING = "pending"
RETENTION_PASSED = "passed"
RETENTION_FAILED = "failed"

BUDGET_SCOUTING = "scouting"
BUDGET_EXPLOIT = "exploit"
BUDGET_VALIDATION = "validation"
BUDGET_COLLAPSE_RECOVERY = "collapse_recovery"
BUDGET_WRAP_UP = "wrap_up"


@dataclass
class FamilyBranchState:
    family_id: str
    first_seen_attempt_id: str | None = None
    best_attempt_id: str | None = None
    best_score: float | None = None
    best_horizon_months: int | None = None
    best_effective_window_months: float | None = None
    latest_attempt_id: str | None = None
    latest_score: float | None = None
    latest_horizon_months: int | None = None
    latest_effective_window_months: float | None = None
    lifecycle_state: str = LIFECYCLE_SCOUT
    promotion_level: str = PROMOTION_SCOUT
    retention_status: str = RETENTION_UNTESTED
    # Score-retention gate (canonical; mirrors former _family_retention_state)
    retention_check_done: bool = False
    retention_check_passed: bool | None = None
    retention_baseline_score: float | None = None
    retention_last_delta: float | None = None
    retention_last_ratio: float | None = None
    retention_support_quality: str = "normal"
    retention_last_horizon: int | None = None
    retention_last_eval_score: float | None = None
    unresolved_validation_active: bool = False
    needs_structural_contrast: bool = False
    retention_fail_count: int = 0
    coverage_inadequate_count: int = 0
    long_rung_low_score_streak: int = 0
    cooldown_until_step: int = 0
    collapse_reason: str | None = None
    structural_contrast_required: bool = False
    last_structural_contrast_step: int = 0
    bankrupt: bool = False
    hard_dead: bool = False
    reseed_triggered: bool = False
    exploit_dead: bool = False
    unresolved_coverage_count: int = 0
    last_validation_outcome: str | None = None
    last_coverage_status: str | None = None
    timeframe_mismatch_hits: int = 0
    last_profile_ref: str | None = None
    provisional_peak_score: float | None = None
    provisional_peak_horizon_months: int | None = None

    last_validation_evidence: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Alias for prompts / snapshots (single concept: structural contrast)
        d["structural_contrast_required_effective"] = (
            self.structural_contrast_required or self.needs_structural_contrast
        )
        d["lifecycle_collapsed"] = self.lifecycle_state == LIFECYCLE_COLLAPSED
        d["exploit_suppressed"] = bool(self.exploit_dead)
        d["policy_suppressed"] = (
            self.lifecycle_state == LIFECYCLE_COLLAPSED or bool(self.exploit_dead)
        )
        return d

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> FamilyBranchState:
        known = {f.name for f in FamilyBranchState.__dataclass_fields__.values()}
        kwargs = {k: v for k, v in payload.items() if k in known}
        family_id = str(kwargs.get("family_id") or "")
        if not family_id:
            raise ValueError("family_id required")
        return FamilyBranchState(**kwargs)


@dataclass
class BranchRunOverlay:
    """Ephemeral run-level branch policy overlay (not keyed by family)."""

    provisional_leader_family_id: str | None = None
    validated_leader_family_id: str | None = None
    shadow_leader_family_id: str | None = None
    shadow_leader_reason: str | None = None
    """Latest scored attempt's canonical validation evidence (for step packet / tooling)."""
    last_scored_validation_digest: dict[str, Any] | None = None
    budget_mode: str = BUDGET_SCOUTING
    reseed_active: bool = False
    reseed_started_step: int | None = None
    collapse_recovery_remaining: int = 0
    recent_retention_failures: list[int] = field(default_factory=list)
    explored_family_count: int = 0


def ensure_family_branch(
    branches: dict[str, FamilyBranchState], family_id: str
) -> FamilyBranchState:
    if family_id not in branches:
        branches[family_id] = FamilyBranchState(family_id=family_id)
    return branches[family_id]


def cooldown_active(state: FamilyBranchState, step: int) -> bool:
    return state.bankrupt and state.cooldown_until_step > step
