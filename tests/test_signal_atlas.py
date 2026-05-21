from __future__ import annotations

from autoresearch.signal_atlas import compute_signal_metrics


def test_compute_signal_metrics_detects_sparse_balanced_events() -> None:
    metrics = compute_signal_metrics(
        [0, 1, 0, 0, 1, 1, 0, 0],
        [0, 0, 0, 1, 0, 0, 0, 1],
        timestamps=[f"t{i}" for i in range(8)],
    )

    assert metrics["bars"] == 8
    assert metrics["long_event_count"] == 2
    assert metrics["short_event_count"] == 2
    assert metrics["event_count"] == 3
    assert metrics["either_active_bars"] == 5
    assert metrics["long_share_of_active"] == 0.6
    assert metrics["density_bucket"] == "saturated"
    assert metrics["balance_bucket"] == "balanced"
    assert metrics["first_timestamp"] == "t7"
    assert metrics["last_timestamp"] == "t0"


def test_compute_signal_metrics_flags_flat_series() -> None:
    metrics = compute_signal_metrics([0, 0, 0], [0, 0, 0])

    assert metrics["density_bucket"] == "flat"
    assert metrics["balance_bucket"] == "flat"
    assert metrics["event_count"] == 0
    assert metrics["active_percent"] == 0.0


def test_compute_signal_metrics_flags_one_sided_series() -> None:
    metrics = compute_signal_metrics([0, 1, 0, 1, 0], [0, 0, 0, 0, 0])

    assert metrics["long_event_count"] == 2
    assert metrics["short_event_count"] == 0
    assert metrics["balance_bucket"] == "one_sided"
    assert metrics["long_share_of_active"] == 1.0
