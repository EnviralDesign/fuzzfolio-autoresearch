from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from autoresearch.evidence_plan import (
    build_replay_evidence_plan,
    enforce_replay_evidence_plan,
    validate_replay_evidence_plan,
)


PROFILE = {"name": "frozen", "notificationThreshold": 73, "indicators": []}


def _plan():
    return build_replay_evidence_plan(
        campaign_plan_id="campaign:test",
        evidence_role="full_backtest",
        selection_data_end="2026-07-08T23:59:59Z",
        analysis_window_start="2023-07-08T23:59:59Z",
        analysis_window_end="2026-07-08T23:59:59Z",
        requested_horizon_months=36,
        profile_snapshot=PROFILE,
    )


def test_evidence_plan_is_deterministic_and_self_authenticating() -> None:
    first = _plan()
    second = _plan()

    assert first.plan_id == second.plan_id
    assert first.plan_id.startswith("sha256:")
    assert first.analysis_window_end == "2026-07-08T23:59:59Z"


def test_evidence_plan_rejects_mutated_window() -> None:
    payload = _plan().model_dump(mode="json")
    payload["analysis_window_start"] = "2023-07-09T23:59:59Z"

    with pytest.raises(ValidationError, match="hash mismatch"):
        validate_replay_evidence_plan(payload)


def test_selection_evidence_cannot_cross_selection_cutoff() -> None:
    with pytest.raises(ValidationError, match="selection_data_end"):
        build_replay_evidence_plan(
            evidence_role="training",
            selection_data_end="2026-01-01T00:00:00Z",
            analysis_window_start="2025-01-01T00:00:00Z",
            analysis_window_end="2026-02-01T00:00:00Z",
            requested_horizon_months=12,
            profile_snapshot=PROFILE,
        )


def test_outer_test_may_extend_beyond_selection_cutoff() -> None:
    plan = build_replay_evidence_plan(
        evidence_role="outer_test",
        selection_data_end="2026-01-01T00:00:00Z",
        analysis_window_start="2026-01-16T00:00:00Z",
        analysis_window_end="2026-07-01T00:00:00Z",
        requested_horizon_months=6,
        profile_snapshot=PROFILE,
    )

    assert plan.evidence_role == "outer_test"


def test_enforcement_rejects_dynamic_or_mismatched_inputs() -> None:
    plan = _plan()
    with pytest.raises(ValueError, match="lookback_months"):
        enforce_replay_evidence_plan(
            plan,
            profile_snapshot=PROFILE,
            analysis_window_start=plan.analysis_window_start,
            analysis_window_end=plan.analysis_window_end,
            lookback_months=36,
        )
    with pytest.raises(ValueError, match="profile snapshot"):
        enforce_replay_evidence_plan(
            plan,
            profile_snapshot={**copy.deepcopy(PROFILE), "notificationThreshold": 80},
            analysis_window_start=plan.analysis_window_start,
            analysis_window_end=plan.analysis_window_end,
            lookback_months=None,
        )
    with pytest.raises(ValueError, match="analysis_window_end"):
        enforce_replay_evidence_plan(
            plan,
            profile_snapshot=PROFILE,
            analysis_window_start=plan.analysis_window_start,
            analysis_window_end="2026-07-09T00:00:00Z",
            lookback_months=None,
        )


def test_enforcement_accepts_exact_frozen_request() -> None:
    plan = _plan()
    resolved = enforce_replay_evidence_plan(
        plan,
        profile_snapshot=PROFILE,
        analysis_window_start=plan.analysis_window_start,
        analysis_window_end=plan.analysis_window_end,
        lookback_months=None,
    )

    assert resolved.plan_id == plan.plan_id
