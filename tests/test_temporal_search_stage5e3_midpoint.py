from __future__ import annotations

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_search_stage5e3_midpoint import (
    _activity_category,
    _spearman,
    freeze_stage5e3_midpoint,
)


@pytest.mark.parametrize(
    ("e_trades", "f_trades", "expected"),
    [
        (1, 1, "active_both"),
        (1, 0, "active_e_only"),
        (0, 1, "active_f_only"),
        (0, 0, "inactive_both"),
    ],
)
def test_activity_category_is_window_explicit(
    e_trades: int, f_trades: int, expected: str
) -> None:
    assert _activity_category(e_trades, f_trades) == expected


def test_spearman_uses_average_ranks_for_ties() -> None:
    assert _spearman([0.0, 0.0, 1.0, 2.0], [0.0, 0.0, 2.0, 1.0]) == pytest.approx(
        0.7777777777777778
    )


def test_spearman_returns_none_for_undefined_inputs() -> None:
    assert _spearman([], []) is None
    assert _spearman([1.0, 1.0], [2.0, 3.0]) is None


def test_midpoint_requires_an_exact_analysis_commit(tmp_path) -> None:
    with pytest.raises(
        TemporalDiscoveryContractError, match="analysis commit must be an exact"
    ):
        freeze_stage5e3_midpoint(
            root=tmp_path / "prelaunch",
            output_root=tmp_path / "midpoint",
            autoresearch_analysis_commit="not-a-commit",
        )
