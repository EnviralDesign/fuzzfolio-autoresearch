"""Tests for ValidationOutcome assembly."""

from __future__ import annotations

from autoresearch import validation_outcome as vo


def test_build_passed_when_retention_ok() -> None:
    out = vo.build_validation_outcome(
        family_id="fam",
        attempt_id="run-x-attempt-00001",
        requested_horizon_months=24,
        effective_window_months=22.0,
        requested_timeframe="H1",
        effective_timeframe="H1",
        coverage_status=vo.COVERAGE_OK,
        coverage_ok=True,
        retention_result={"retention_failed": False},
        branch_retention_status="passed",
        branch_retention_passed=True,
        is_retention_horizon_check=True,
        hardened_unresolved=False,
    )
    assert out.outcome == vo.VALIDATION_PASSED
    assert out.should_retry_resolution is False
    assert out.evidence_tier == vo.EVIDENCE_PASSED


def test_build_unresolved_missing_coverage() -> None:
    out = vo.build_validation_outcome(
        family_id="fam",
        attempt_id=None,
        requested_horizon_months=12,
        effective_window_months=None,
        requested_timeframe=None,
        effective_timeframe=None,
        coverage_status=vo.COVERAGE_UNRESOLVED,
        coverage_ok=False,
        retention_result={},
        branch_retention_status="pending",
        branch_retention_passed=None,
        is_retention_horizon_check=False,
        hardened_unresolved=False,
    )
    assert out.outcome == vo.VALIDATION_UNRESOLVED
    assert out.should_retry_resolution is True
    assert out.evidence_tier == vo.EVIDENCE_UNRESOLVED_PENDING


def test_failed_hardened_tier() -> None:
    out = vo.build_validation_outcome(
        family_id="fam",
        attempt_id="a1",
        requested_horizon_months=24,
        effective_window_months=None,
        requested_timeframe=None,
        effective_timeframe=None,
        coverage_status=vo.COVERAGE_UNRESOLVED,
        coverage_ok=False,
        retention_result={"retention_failed": False},
        branch_retention_status="failed",
        branch_retention_passed=False,
        is_retention_horizon_check=True,
        hardened_unresolved=True,
    )
    assert out.outcome == vo.VALIDATION_FAILED
    assert out.evidence_tier == vo.EVIDENCE_FAILED_HARDENED_UNRESOLVED


def test_classify_coverage() -> None:
    s, ok = vo.classify_coverage(
        requested_horizon_months=12,
        effective_window_months=11.0,
        effective_coverage_min_ratio=0.88,
    )
    assert s == vo.COVERAGE_OK
    assert ok is True
    s2, ok2 = vo.classify_coverage(
        requested_horizon_months=12,
        effective_window_months=8.0,
        effective_coverage_min_ratio=0.88,
    )
    assert s2 == vo.COVERAGE_INADEQUATE
    assert ok2 is False
