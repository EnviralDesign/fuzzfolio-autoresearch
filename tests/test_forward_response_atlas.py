from __future__ import annotations

from autoresearch.forward_response_atlas import (
    compute_forward_event_records,
    summarize_forward_events,
)


def test_compute_forward_event_records_measures_long_mfe_mae() -> None:
    records = compute_forward_event_records(
        close=[100, 100, 104, 106],
        high=[100, 101, 105, 107],
        low=[99, 99, 99, 105],
        long_score=[0, 1, 0, 0],
        short_score=[0, 0, 0, 0],
        horizons=[2],
    )

    assert len(records) == 1
    record = records[0]
    assert record["direction"] == "long"
    assert record["event_index"] == 1
    assert record["horizon_bars"] == 2
    assert record["forward_return_pct"] == 6.0
    assert record["mfe_pct"] == 7.0
    assert record["mae_pct"] == 1.0
    assert record["mfe_gt_mae"] is True


def test_compute_forward_event_records_measures_short_response() -> None:
    records = compute_forward_event_records(
        close=[100, 100, 96, 94],
        high=[101, 101, 97, 96],
        low=[99, 99, 95, 93],
        long_score=[0, 0, 0, 0],
        short_score=[0, 1, 0, 0],
        horizons=[2],
    )

    assert len(records) == 1
    record = records[0]
    assert record["direction"] == "short"
    assert record["forward_return_pct"] == 6.0
    assert record["mfe_pct"] == 7.0
    assert record["mae_pct"] == 0.0


def test_summarize_forward_events_assigns_directional_tailwind() -> None:
    events = [
        {
            "forward_return_pct": 0.2,
            "mfe_pct": 0.4,
            "mae_pct": 0.1,
            "mfe_minus_mae_pct": 0.3,
            "mfe_gt_mae": True,
            "volatility_normalized_return": 0.5,
        }
        for _ in range(40)
    ]

    summary = summarize_forward_events(events, min_events=10)

    assert summary["sample_count"] == 40
    assert summary["win_rate_pct"] == 100.0
    assert summary["mfe_gt_mae_rate_pct"] == 100.0
    assert summary["response_bucket"] == "directional_tailwind"
    assert summary["forward_response_score"] > 62.0
