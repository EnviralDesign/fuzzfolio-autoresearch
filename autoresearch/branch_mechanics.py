"""Deterministic branch mechanics: post-eval facts, overlay sync, budget mode.

Policy-style branch adjudication (who leads, who is suppressed, reseed) is owned
by the manager via `manager_actions`. This module records evaluation facts,
builds validation evidence, and keeps overlay/budget consistent with current IDs.
"""

from __future__ import annotations

from typing import Any

from . import branch_lifecycle as bl
from . import validation_outcome as vo

HARD_COLLAPSE_REASONS = frozenset(
    {
        "retention_threshold_failed",
        "repeated_long_horizon_scores_far_below_provisional_peak",
        "repeated_timeframe_intent_mismatch",
    }
)


def _family_is_live_leader_candidate(ctrl: Any, family_id: str | None) -> bool:
    if not family_id:
        return False
    branch = ctrl._family_branches.get(str(family_id).strip())
    if not branch:
        return False
    return not branch.exploit_dead and branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED


def mark_family_collapsed(
    ctrl: Any,
    tool_context: Any,
    family_id: str,
    reason: str,
    step: int,
    step_limit: int,
) -> None:
    cfg = ctrl.config.research
    branch = bl.ensure_family_branch(ctrl._family_branches, family_id)
    if branch.exploit_dead:
        return
    branch.lifecycle_state = bl.LIFECYCLE_COLLAPSED
    branch.retention_status = bl.RETENTION_FAILED
    branch.bankrupt = True
    branch.hard_dead = reason in HARD_COLLAPSE_REASONS
    branch.exploit_dead = True
    branch.collapse_reason = reason
    branch.structural_contrast_required = True
    branch.needs_structural_contrast = True
    branch.cooldown_until_step = step + int(cfg.bankruptcy_cooldown_steps)
    ctrl._branch_overlay.recent_retention_failures.append(step)
    keep = max(20, cfg.reseed_max_recent_failures_window * 4)
    ctrl._branch_overlay.recent_retention_failures = ctrl._branch_overlay.recent_retention_failures[
        -keep:
    ]
    ctrl._trace_runtime(
        tool_context,
        step=step,
        phase="branch_lifecycle",
        status="collapsed",
        message=f"Family {family_id[:20]}... collapsed: {reason}",
        family_id_prefix=family_id[:16],
    )


def apply_overlay_provisional_leadership(ctrl: Any) -> None:
    overlay = ctrl._branch_overlay
    leader = overlay.provisional_leader_family_id
    validated = overlay.validated_leader_family_id
    if not _family_is_live_leader_candidate(ctrl, validated):
        overlay.validated_leader_family_id = None
        validated = None
    if leader == validated:
        overlay.provisional_leader_family_id = None
        overlay.provisional_leader_promotability = None
        leader = None
    elif not _family_is_live_leader_candidate(ctrl, leader):
        overlay.provisional_leader_family_id = None
        overlay.provisional_leader_promotability = None
        leader = None
    if overlay.shadow_leader_family_id and not _family_is_live_leader_candidate(
        ctrl, overlay.shadow_leader_family_id
    ):
        overlay.shadow_leader_family_id = None
        overlay.shadow_leader_reason = None
    for fid, st in ctrl._family_branches.items():
        if (
            st.lifecycle_state == bl.LIFECYCLE_VALIDATED_LEADER
            and fid != validated
            and fid != leader
            and st.lifecycle_state != bl.LIFECYCLE_COLLAPSED
        ):
            st.lifecycle_state = (
                bl.LIFECYCLE_PROVISIONAL_CONTENDER
                if st.promotion_level != bl.PROMOTION_SCOUT
                else bl.LIFECYCLE_SCOUT
            )
        if (
            st.lifecycle_state == bl.LIFECYCLE_PROVISIONAL_LEADER
            and fid != leader
            and fid != validated
            and st.promotion_level != bl.PROMOTION_VALIDATED
        ):
            st.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_CONTENDER
    if validated:
        br = ctrl._family_branches.get(validated)
        if br:
            br.promotion_level = bl.PROMOTION_VALIDATED
            br.lifecycle_state = bl.LIFECYCLE_VALIDATED_LEADER
            br.structural_contrast_required = False
            br.needs_structural_contrast = False
    if not leader:
        return
    br = ctrl._family_branches.get(leader)
    if not br or br.exploit_dead or br.lifecycle_state == bl.LIFECYCLE_COLLAPSED:
        return
    if br.promotion_level == bl.PROMOTION_SCOUT:
        br.promotion_level = bl.PROMOTION_PROVISIONAL
    if br.lifecycle_state not in (
        bl.LIFECYCLE_VALIDATED_LEADER,
        bl.LIFECYCLE_COLLAPSED,
    ):
        br.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_LEADER


def sync_branch_budget_mode(
    ctrl: Any,
    step: int,
    step_limit: int,
    policy: Any,
) -> None:
    overlay = ctrl._branch_overlay
    overlay.shadow_leader_family_id = None
    overlay.shadow_leader_reason = None
    phase_info = ctrl._run_phase_info(step, step_limit, policy)
    phase_name = str(phase_info.get("name") or "")
    overlay.explored_family_count = len(ctrl._family_branches)
    if phase_name == "wrap_up":
        overlay.budget_mode = bl.BUDGET_WRAP_UP
    elif overlay.reseed_active and overlay.collapse_recovery_remaining > 0:
        overlay.budget_mode = bl.BUDGET_COLLAPSE_RECOVERY
    elif overlay.validated_leader_family_id:
        overlay.budget_mode = (
            bl.BUDGET_VALIDATION
            if phase_name in {"mid", "late"}
            else bl.BUDGET_EXPLOIT
        )
    elif (
        sum(1 for b in ctrl._family_branches.values() if b.exploit_dead)
        >= ctrl.config.research.max_bankrupt_families_before_force_breadth
    ):
        overlay.budget_mode = bl.BUDGET_SCOUTING
    elif phase_name == "early" or not overlay.provisional_leader_family_id:
        overlay.budget_mode = bl.BUDGET_SCOUTING
    else:
        overlay.budget_mode = (
            bl.BUDGET_VALIDATION if phase_name in {"mid", "late"} else bl.BUDGET_EXPLOIT
        )
    apply_overlay_provisional_leadership(ctrl)
    prov_id = overlay.provisional_leader_family_id
    if prov_id and prov_id in ctrl._family_branches:
        overlay.provisional_leader_promotability = ctrl._family_branches[
            prov_id
        ].promotability_status
    else:
        overlay.provisional_leader_promotability = None


def refresh_family_after_scored_eval(
    ctrl: Any,
    tool_context: Any,
    step: int,
    _step_limit: int,
    *,
    family_id: str | None,
    profile_ref: str | None,
    attempt_id: str | None,
    score: float,
    requested_horizon_months: int | None,
    effective_window_months: float | None,
    retention_result: dict[str, Any] | None,
    behavior_digest: dict[str, Any] | None,
    had_timeframe_mismatch: bool,
    requested_timeframe: str | None = None,
    effective_timeframe: str | None = None,
    effective_window_source: str | None = None,
) -> None:
    """Update per-family facts and last_scored_validation_digest after a scored eval.

    Does not assign overlay leaders, auto-collapse families, or persist. Call
    `sync_branch_budget_mode` afterward.
    """
    if not family_id:
        return
    cfg = ctrl.config.research
    branch = bl.ensure_family_branch(ctrl._family_branches, family_id)
    digest = behavior_digest or {}
    support_shape = str(digest.get("support_shape") or "")

    if branch.first_seen_attempt_id is None and attempt_id:
        branch.first_seen_attempt_id = attempt_id
    branch.latest_attempt_id = attempt_id
    branch.latest_score = score
    branch.latest_horizon_months = requested_horizon_months
    branch.latest_effective_window_months = effective_window_months
    if profile_ref:
        branch.last_profile_ref = str(profile_ref).strip()

    if branch.best_score is None or score > branch.best_score:
        branch.best_score = score
        branch.best_attempt_id = attempt_id
        if requested_horizon_months is not None:
            branch.best_horizon_months = requested_horizon_months
        if effective_window_months is not None:
            branch.best_effective_window_months = effective_window_months
    elif branch.best_score is not None and abs(float(score) - float(branch.best_score)) < 1e-9:
        if branch.best_horizon_months is None and requested_horizon_months is not None:
            branch.best_horizon_months = requested_horizon_months
        if (
            branch.best_effective_window_months is None
            and effective_window_months is not None
        ):
            branch.best_effective_window_months = effective_window_months

    if support_shape == "too_sparse" and branch.lifecycle_state != bl.LIFECYCLE_COLLAPSED:
        branch.lifecycle_state = bl.LIFECYCLE_RETENTION_WARNING

    coverage_status, coverage_ok = vo.classify_coverage(
        requested_horizon_months=requested_horizon_months,
        effective_window_months=effective_window_months,
        effective_coverage_min_ratio=cfg.effective_coverage_min_ratio,
    )
    branch.last_coverage_status = coverage_status
    hardened_unresolved = False
    if coverage_status == vo.COVERAGE_UNRESOLVED:
        if (
            requested_horizon_months is not None
            and cfg.horizon_failure_counts_as_retention_fail
        ):
            branch.unresolved_coverage_count += 1
            if branch.unresolved_coverage_count >= cfg.unresolved_coverage_harden_after:
                branch.retention_fail_count += 1
                branch.unresolved_coverage_count = 0
                branch.coverage_inadequate_count += 1
                hardened_unresolved = True
        else:
            branch.unresolved_coverage_count = 0
    elif coverage_status == vo.COVERAGE_INADEQUATE:
        branch.unresolved_coverage_count = 0
        if cfg.horizon_failure_counts_as_retention_fail:
            branch.retention_fail_count += 1
            branch.coverage_inadequate_count += 1
    else:
        branch.unresolved_coverage_count = 0

    if had_timeframe_mismatch:
        branch.timeframe_mismatch_hits += 1

    min_horizon = cfg.validated_leader_min_horizon_months
    if (
        requested_horizon_months is not None
        and requested_horizon_months < min_horizon
    ):
        peak = branch.provisional_peak_score
        if peak is None or score > peak:
            branch.provisional_peak_score = score
            branch.provisional_peak_horizon_months = requested_horizon_months

    rr = retention_result or {}
    strict_pass = branch.retention_check_passed is True
    weak_retention_pass = (
        not branch.exploit_dead
        and not strict_pass
        and not rr.get("retention_failed")
        and coverage_status != vo.COVERAGE_INADEQUATE
        and not hardened_unresolved
        and not had_timeframe_mismatch
        and ctrl._timeframes_compatible_for_provisional(
            requested_timeframe, effective_timeframe
        )
    )
    if rr.get("retention_failed") and not branch.exploit_dead:
        branch.retention_status = bl.RETENTION_FAILED
    elif had_timeframe_mismatch and not branch.exploit_dead:
        branch.retention_status = bl.RETENTION_PENDING
    elif not branch.exploit_dead and (strict_pass or weak_retention_pass):
        if coverage_status == vo.COVERAGE_UNRESOLVED and not hardened_unresolved:
            branch.retention_status = bl.RETENTION_PENDING
        elif coverage_status == vo.COVERAGE_INADEQUATE or hardened_unresolved:
            branch.retention_status = bl.RETENTION_FAILED
        else:
            branch.retention_status = bl.RETENTION_PASSED

    if rr.get("needs_retention_check") and not branch.exploit_dead:
        branch.retention_status = bl.RETENTION_PENDING

    branch.unresolved_validation_active = (
        coverage_status == vo.COVERAGE_UNRESOLVED and not hardened_unresolved
    )
    branch.needs_structural_contrast = branch.structural_contrast_required

    evidence = vo.build_validation_outcome(
        family_id=family_id,
        attempt_id=attempt_id,
        requested_horizon_months=requested_horizon_months,
        effective_window_months=effective_window_months,
        requested_timeframe=requested_timeframe,
        effective_timeframe=effective_timeframe,
        coverage_status=coverage_status,
        coverage_ok=coverage_ok,
        retention_result=rr,
        branch_retention_status=branch.retention_status,
        branch_retention_passed=branch.retention_check_passed,
        is_retention_horizon_check=(
            requested_horizon_months is not None
            and requested_horizon_months >= min_horizon
        ),
        hardened_unresolved=hardened_unresolved,
        weak_provisional_evidence=weak_retention_pass,
        effective_window_source=effective_window_source,
        timeframe_mismatch=had_timeframe_mismatch,
    )
    evd = evidence.to_dict()
    branch.last_validation_evidence = evd
    branch.last_validation_outcome = evidence.outcome
    branch.promotability_status = evidence.promotability_status
    branch.validation_confidence = evidence.validation_confidence

    ctrl._branch_overlay.last_scored_validation_digest = {
        "family_id": family_id,
        "attempt_id": attempt_id,
        "validation_evidence": evd,
        "lifecycle_state": branch.lifecycle_state,
        "promotion_level": branch.promotion_level,
        "retention_status": branch.retention_status,
        "coverage_status": branch.last_coverage_status,
        "last_validation_outcome": branch.last_validation_outcome,
        "promotability_status": branch.promotability_status,
        "validation_confidence": branch.validation_confidence,
        "exploit_dead": branch.exploit_dead,
        "collapse_reason": branch.collapse_reason,
    }
