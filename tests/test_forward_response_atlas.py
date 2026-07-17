from __future__ import annotations

import json
from types import SimpleNamespace

from autoresearch.forward_response_atlas import (
    _ForwardEventAccumulator,
    _accumulator_rows,
    _combined_direction_rows,
    _grouped_summaries,
    build_forward_response_atlas,
    compute_forward_event_records,
    iter_forward_event_records,
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


def test_forward_event_accumulator_matches_list_summary() -> None:
    events = [
        {
            "indicator_id": "RSI",
            "direction": "long",
            "horizon_bars": 3,
            "forward_return_pct": 0.2,
            "mfe_pct": 0.5,
            "mae_pct": 0.1,
            "mfe_minus_mae_pct": 0.4,
            "mfe_gt_mae": True,
            "volatility_normalized_return": 0.7,
        },
        {
            "indicator_id": "RSI",
            "direction": "long",
            "horizon_bars": 3,
            "forward_return_pct": -0.1,
            "mfe_pct": 0.1,
            "mae_pct": 0.3,
            "mfe_minus_mae_pct": -0.2,
            "mfe_gt_mae": False,
            "volatility_normalized_return": None,
        },
    ]
    accumulator = _ForwardEventAccumulator()
    for event in events:
        accumulator.add(event)

    assert accumulator.summary(min_events=1) == summarize_forward_events(events, min_events=1)
    grouped = {}
    for event in events:
        key = (event["indicator_id"], event["direction"], event["horizon_bars"])
        grouped.setdefault(key, _ForwardEventAccumulator()).add(event)
    assert _accumulator_rows(
        grouped, ("indicator_id", "direction", "horizon_bars"), min_events=1
    ) == _grouped_summaries(events, ("indicator_id", "direction", "horizon_bars"), min_events=1)


def test_combined_direction_rows_match_explicit_both_accumulator() -> None:
    events = [
        {
            "indicator_id": "RSI",
            "direction": "long",
            "horizon_bars": 3,
            "forward_return_pct": 0.2,
            "mfe_pct": 0.5,
            "mae_pct": 0.1,
            "mfe_minus_mae_pct": 0.4,
            "mfe_gt_mae": True,
            "volatility_normalized_return": 0.7,
        },
        {
            "indicator_id": "RSI",
            "direction": "short",
            "horizon_bars": 3,
            "forward_return_pct": -0.1,
            "mfe_pct": 0.1,
            "mae_pct": 0.3,
            "mfe_minus_mae_pct": -0.2,
            "mfe_gt_mae": False,
            "volatility_normalized_return": None,
        },
        {
            "indicator_id": "RSI",
            "direction": "short",
            "horizon_bars": 3,
            "forward_return_pct": 0.4,
            "mfe_pct": 0.6,
            "mae_pct": 0.2,
            "mfe_minus_mae_pct": 0.4,
            "mfe_gt_mae": True,
            "volatility_normalized_return": 0.9,
        },
    ]
    directional = {}
    explicit_both = _ForwardEventAccumulator()
    for event in events:
        key = (event["indicator_id"], event["direction"], event["horizon_bars"])
        directional.setdefault(key, _ForwardEventAccumulator()).add(event)
        explicit_both.add(event)

    rows = _combined_direction_rows(directional, min_events=1)

    assert rows == [
        {
            "indicator_id": "RSI",
            "horizon_bars": 3,
            "direction": "both",
            **explicit_both.summary(min_events=1),
        }
    ]


def test_iter_forward_event_records_matches_list_wrapper() -> None:
    kwargs = {
        "close": [100, 100, 104, 106, 108],
        "high": [100, 101, 105, 107, 110],
        "low": [99, 99, 99, 105, 106],
        "long_score": [0, 1, 0, 0, 1],
        "short_score": [0, 0, 1, 0, 0],
        "horizons": [1, 2],
    }

    assert list(iter_forward_event_records(**kwargs)) == compute_forward_event_records(**kwargs)


def test_build_forward_response_atlas_reduces_raw_cells_without_retaining_events(tmp_path) -> None:
    signal_dir = tmp_path / "signal-atlas"
    raw_dir = signal_dir / "raw"
    raw_dir.mkdir(parents=True)
    raw_path = raw_dir / "rsi-m5-eurusd.json"
    raw_path.write_text(
        json.dumps(
            {
                "data": {
                    "timestamp": ["t3", "t2", "t1", "t0"],
                    "close": [106, 104, 100, 100],
                    "high": [107, 105, 101, 100],
                    "low": [105, 99, 99, 99],
                    "long_score": [0, 0, 1, 0],
                    "short_score": [0, 0, 0, 0],
                }
            }
        ),
        encoding="utf-8",
    )
    signal_payload = {
        "summary": {
            "generated_at": "2026-01-01T00:00:00Z",
            "selection": {
                "indicator_ids": ["RSI"],
                "signal_role_filter": "trigger",
                "signal_roles": ["trigger"],
                "instruments": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
        "rows": [
            {
                "indicator_id": "RSI",
                "instrument": "EURUSD",
                "timeframe": "M5",
                "status": "ok",
                "raw_path": str(raw_path),
            }
        ],
    }
    (signal_dir / "signal-atlas.json").write_text(json.dumps(signal_payload), encoding="utf-8")

    result = build_forward_response_atlas(
        SimpleNamespace(repo_root=tmp_path, derived_root=tmp_path / "derived"),
        signal_atlas_dir=signal_dir,
        out_dir=tmp_path / "forward",
        horizons=[2],
        min_events=1,
    )

    assert result.summary["result_counts"]["event_horizon_records"] == 1
    assert result.summary["result_counts"]["cell_rollup_rows"] == 1
    assert result.summary["result_counts"]["indicator_rollup_rows"] == 2
    assert result.summary["priors"][0]["indicator_id"] == "RSI"
