from __future__ import annotations

from autoresearch.temporal_search_stage5e4_diagnostics import (
    _causal_blocker,
    _pacing_metrics,
)


def test_causal_blocker_distinguishes_opportunity_from_lifecycle() -> None:
    assert (
        _causal_blocker(
            positions=2,
            source_occupancy=0,
            maximum_mfe_r=0.75,
            required_r=0.5,
            applied=0,
            rejected=0,
            selected=0,
            extra_guard_kinds=[],
        )
        == "intrabar_mfe_at_or_above_threshold_wrong_source_state"
    )
    assert (
        _causal_blocker(
            positions=2,
            source_occupancy=10,
            maximum_mfe_r=0.25,
            required_r=0.5,
            applied=0,
            rejected=0,
            selected=0,
            extra_guard_kinds=[],
        )
        == "intrabar_mfe_below_threshold"
    )


def test_pacing_metrics_keep_inter_entry_and_post_close_gaps_separate() -> None:
    result = _pacing_metrics(
        entry_clocks=[2, 10, 20],
        exit_clocks=[5, 18, 25],
        holding_bars=[4, 9, 6],
    )
    assert result["interEntryGapBars"]["median"] == 9.0
    assert result["postCloseReentryGapBars"]["median"] == 3.5
    assert result["rapidPostCloseReentryCounts"]["3"] == 1
    assert result["shortHoldingCounts"]["6"] == 2
