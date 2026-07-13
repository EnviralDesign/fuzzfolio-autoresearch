from __future__ import annotations

import pytest

from autoresearch.evidence_plan import enforce_replay_evidence_plan
from autoresearch.nested_evidence import (
    build_nested_train_fold,
    freeze_nested_outer_test,
)


PROFILE = {"name": "frozen", "notificationThreshold": 73, "indicators": []}
CELL = {"stop_loss_percent": 0.1, "reward_multiple": 2.0}


def _train_fold():
    return build_nested_train_fold(
        campaign_plan_id="nested:test",
        fold_id="fold-01",
        profile_snapshot=PROFILE,
        train_start="2022-01-01T00:00:00Z",
        train_end="2024-12-31T23:59:59Z",
        train_horizon_months=36,
        embargo_days=15,
        lake_manifest_sha256="sha256:" + "a" * 64,
    )


def test_outer_plan_can_only_be_minted_after_cell_is_frozen() -> None:
    fold = freeze_nested_outer_test(
        _train_fold(),
        profile_snapshot=PROFILE,
        selected_cell=CELL,
        selection_basis="recommended_cell",
        test_start="2025-01-16T00:00:00Z",
        test_end="2025-06-30T23:59:59Z",
        test_horizon_months=6,
    )

    assert fold.outer_test_plan is not None
    assert fold.outer_test_plan.evidence_role == "outer_test"
    assert fold.outer_test_plan.selection_data_end == "2024-12-31T23:59:59Z"
    assert (
        fold.outer_test_plan.execution_cell_sha256
        == fold.cell_receipt.execution_cell_sha256
    )
    assert fold.outer_test_plan.lake_manifest_sha256 == fold.train_plan.lake_manifest_sha256
    assert fold.cell_receipt.lake_manifest_sha256 == fold.train_plan.lake_manifest_sha256


def test_outer_plan_rejects_mutated_execution_cell() -> None:
    fold = freeze_nested_outer_test(
        _train_fold(),
        profile_snapshot=PROFILE,
        selected_cell=CELL,
        selection_basis="best_cell",
        test_start="2025-01-16T00:00:00Z",
        test_end="2025-06-30T23:59:59Z",
        test_horizon_months=6,
    )

    with pytest.raises(ValueError, match="execution cell does not match"):
        enforce_replay_evidence_plan(
            fold.outer_test_plan,
            profile_snapshot=PROFILE,
            analysis_window_start="2025-01-16T00:00:00Z",
            analysis_window_end="2025-06-30T23:59:59Z",
            lookback_months=None,
            execution_cell={"stop_loss_percent": 0.12, "reward_multiple": 2.0},
        )


def test_outer_test_cannot_overlap_training() -> None:
    with pytest.raises(ValueError, match="embargo"):
        freeze_nested_outer_test(
            _train_fold(),
            profile_snapshot=PROFILE,
            selected_cell=CELL,
            selection_basis="robust_cell",
            test_start="2024-12-31T23:59:59Z",
            test_end="2025-06-30T23:59:59Z",
            test_horizon_months=6,
        )


def test_outer_profile_must_match_train_profile() -> None:
    with pytest.raises(ValueError, match="profile snapshot"):
        freeze_nested_outer_test(
            _train_fold(),
            profile_snapshot={**PROFILE, "notificationThreshold": 80},
            selected_cell=CELL,
            selection_basis="recommended_cell",
            test_start="2025-01-16T00:00:00Z",
            test_end="2025-06-30T23:59:59Z",
            test_horizon_months=6,
        )
