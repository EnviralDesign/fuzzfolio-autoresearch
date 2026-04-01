"""Canonical tri-state validation / coverage outcomes for branch lifecycle."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

# Evaluation resolution (per attempt / family update)
VALIDATION_PASSED = "passed"
VALIDATION_FAILED = "failed"
VALIDATION_UNRESOLVED = "unresolved"

# Effective coverage vs requested horizon
COVERAGE_OK = "ok"
COVERAGE_INADEQUATE = "inadequate"
COVERAGE_UNRESOLVED = "unresolved"

# Explicit evidence tiers (for observability; do not collapse into a single "unresolved")
EVIDENCE_FAILED_EXPLICIT = "failed_explicit_validation"
EVIDENCE_FAILED_HARDENED_UNRESOLVED = "failed_after_unresolved_budget"
EVIDENCE_UNRESOLVED_MISSING_DATA = "unresolved_missing_or_incomplete_data"
EVIDENCE_UNRESOLVED_RETRYABLE = "unresolved_retry_allowed"
EVIDENCE_UNRESOLVED_PENDING = "unresolved_pending_retention"
EVIDENCE_UNRESOLVED_TIMEFRAME_MISMATCH = "unresolved_timeframe_intent_mismatch"
EVIDENCE_PASSED = "passed_clear_validation"

# Controller-facing promotability (not synonymous with validation tri-state)
PROMOTABILITY_BLOCKED = "blocked"
PROMOTABILITY_VALIDATED_READY = "validated_ready"
PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE = "provisional_best_available"
PROMOTABILITY_RETRY_RECOMMENDED = "retry_recommended"
PROMOTABILITY_UNKNOWN = "unknown"


@dataclass
class ValidationOutcome:
    family_id: str | None
    attempt_id: str | None
    requested_horizon_months: int | None
    effective_window_months: float | None
    requested_timeframe: str | None = None
    effective_timeframe: str | None = None
    outcome: str = VALIDATION_UNRESOLVED
    reason: str | None = None
    delta_vs_baseline: float | None = None
    ratio_vs_baseline: float | None = None
    coverage_status: str = COVERAGE_OK
    is_retention_check: bool = False
    should_promote: bool = False
    should_collapse: bool = False
    should_retry_resolution: bool = False
    evidence_tier: str | None = None
    promotability_status: str = PROMOTABILITY_UNKNOWN
    validation_confidence: str = "low"
    effective_window_source: str | None = None
    timeframe_mismatch: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_coverage(
    *,
    requested_horizon_months: int | None,
    effective_window_months: float | None,
    effective_coverage_min_ratio: float,
) -> tuple[str, bool]:
    """Return (coverage_status, coverage_ok_for_promotion).

    *ok* means effective window meets requested horizon floor when both are known.
    Missing effective window with a requested horizon is *unresolved*, not inadequate.
    """
    if requested_horizon_months is None:
        return COVERAGE_OK, True
    if effective_window_months is None:
        return COVERAGE_UNRESOLVED, False
    try:
        req = float(requested_horizon_months)
        eff = float(effective_window_months)
        floor = req * float(effective_coverage_min_ratio)
        if eff + 1e-6 < floor:
            return COVERAGE_INADEQUATE, False
        return COVERAGE_OK, True
    except (TypeError, ValueError):
        return COVERAGE_UNRESOLVED, False


def build_validation_outcome(
    *,
    family_id: str,
    attempt_id: str | None,
    requested_horizon_months: int | None,
    effective_window_months: float | None,
    requested_timeframe: str | None,
    effective_timeframe: str | None,
    coverage_status: str,
    coverage_ok: bool,
    retention_result: dict[str, Any],
    branch_retention_status: str,
    branch_retention_passed: bool | None,
    is_retention_horizon_check: bool,
    hardened_unresolved: bool,
    weak_provisional_evidence: bool = False,
    effective_window_source: str | None = None,
    timeframe_mismatch: bool = False,
) -> ValidationOutcome:
    """Assemble one canonical evidence object after an eval + lifecycle refresh."""
    rr = retention_result or {}
    if timeframe_mismatch:
        d_raw, r_raw = rr.get("delta"), rr.get("ratio")
        return ValidationOutcome(
            family_id=family_id,
            attempt_id=attempt_id,
            requested_horizon_months=requested_horizon_months,
            effective_window_months=effective_window_months,
            requested_timeframe=requested_timeframe,
            effective_timeframe=effective_timeframe,
            outcome=VALIDATION_UNRESOLVED,
            reason="requested_effective_timeframe_mismatch",
            delta_vs_baseline=float(d_raw) if d_raw is not None else None,
            ratio_vs_baseline=float(r_raw) if r_raw is not None else None,
            coverage_status=coverage_status,
            is_retention_check=is_retention_horizon_check,
            should_promote=False,
            should_collapse=False,
            should_retry_resolution=True,
            evidence_tier=EVIDENCE_UNRESOLVED_TIMEFRAME_MISMATCH,
            promotability_status=PROMOTABILITY_BLOCKED,
            validation_confidence="low",
            effective_window_source=effective_window_source,
            timeframe_mismatch=True,
        )
    failed = bool(rr.get("retention_failed"))
    outcome = VALIDATION_UNRESOLVED
    reason: str | None = None
    if failed:
        outcome = VALIDATION_FAILED
        reason = "retention_threshold_or_horizon_gating"
    elif coverage_status == COVERAGE_UNRESOLVED and not hardened_unresolved:
        outcome = VALIDATION_UNRESOLVED
        reason = "missing_effective_window_or_coverage_unknown"
    elif coverage_status == COVERAGE_INADEQUATE or hardened_unresolved:
        outcome = VALIDATION_FAILED
        reason = "inadequate_effective_coverage_or_hardened_unresolved"
    elif branch_retention_passed is True and coverage_ok:
        outcome = VALIDATION_PASSED
        reason = "retention_and_coverage_ok"
    elif branch_retention_status == "failed":
        outcome = VALIDATION_FAILED
        reason = "branch_retention_status_failed"
    elif branch_retention_status == "pending":
        outcome = VALIDATION_UNRESOLVED
        reason = "branch_retention_pending"

    delta = rr.get("delta")
    ratio = rr.get("ratio")
    delta_f = float(delta) if delta is not None else None
    ratio_f = float(ratio) if ratio is not None else None

    should_collapse = failed
    should_retry_resolution = outcome == VALIDATION_UNRESOLVED and not hardened_unresolved
    should_promote = outcome == VALIDATION_PASSED and is_retention_horizon_check and coverage_ok

    promotability_status = PROMOTABILITY_UNKNOWN
    validation_confidence = "low"
    if failed or should_collapse:
        promotability_status = PROMOTABILITY_BLOCKED
        validation_confidence = "low"
    elif should_promote:
        promotability_status = PROMOTABILITY_VALIDATED_READY
        validation_confidence = "high"
    elif weak_provisional_evidence:
        promotability_status = PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE
        validation_confidence = "low" if outcome == VALIDATION_UNRESOLVED else "medium"
    elif outcome == VALIDATION_FAILED:
        promotability_status = PROMOTABILITY_BLOCKED
        validation_confidence = "low"
    elif outcome == VALIDATION_UNRESOLVED:
        promotability_status = PROMOTABILITY_RETRY_RECOMMENDED
        validation_confidence = "medium"
    elif outcome == VALIDATION_PASSED:
        promotability_status = PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE
        validation_confidence = "medium"

    evidence_tier: str | None = None
    if outcome == VALIDATION_PASSED:
        evidence_tier = EVIDENCE_PASSED
    elif outcome == VALIDATION_FAILED:
        if hardened_unresolved:
            evidence_tier = EVIDENCE_FAILED_HARDENED_UNRESOLVED
        else:
            evidence_tier = EVIDENCE_FAILED_EXPLICIT
    elif branch_retention_status == "pending":
        evidence_tier = EVIDENCE_UNRESOLVED_PENDING
    elif coverage_status == COVERAGE_UNRESOLVED:
        evidence_tier = EVIDENCE_UNRESOLVED_RETRYABLE
    else:
        evidence_tier = EVIDENCE_UNRESOLVED_MISSING_DATA

    return ValidationOutcome(
        family_id=family_id,
        attempt_id=attempt_id,
        requested_horizon_months=requested_horizon_months,
        effective_window_months=effective_window_months,
        requested_timeframe=requested_timeframe,
        effective_timeframe=effective_timeframe,
        outcome=outcome,
        reason=reason,
        delta_vs_baseline=delta_f,
        ratio_vs_baseline=ratio_f,
        coverage_status=coverage_status,
        is_retention_check=is_retention_horizon_check,
        should_promote=should_promote,
        should_collapse=should_collapse,
        should_retry_resolution=should_retry_resolution,
        evidence_tier=evidence_tier,
        promotability_status=promotability_status,
        validation_confidence=validation_confidence,
        effective_window_source=effective_window_source,
        timeframe_mismatch=False,
    )
