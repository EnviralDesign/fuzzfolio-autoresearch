from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from autoresearch.anchor_pair_atlas import (
    build_pair_profile_document,
    default_probe_as_of_date,
    resolve_probe_as_of_date,
    _sensitivity_args_for_row,
    _timing_bucket,
    _timing_lookback_values,
    _timing_variant_sides,
    _replace_profile_id_arg,
    score_anchor_pair,
    select_anchor_pair_queue,
    signal_density_score,
)


def test_default_probe_as_of_date_uses_previous_utc_month_end() -> None:
    assert (
        default_probe_as_of_date(datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc))
        == "2026-06-30T23:59:59Z"
    )
    assert resolve_probe_as_of_date("2026-05-15T00:00:00Z") == "2026-05-15T00:00:00Z"


def test_sensitivity_args_include_lake_safe_as_of_date() -> None:
    args = _sensitivity_args_for_row(
        {
            "probe_timeframe": "M5",
            "instruments": "EURUSD,XAUUSD",
        },
        lookback_months=3,
        as_of_date="2026-06-30T23:59:59Z",
        quality_score_preset="profile-drop",
        execution_cost_mode="research-conservative",
        result_dir=Path("probe-results") / "probe-1",
    )

    assert args[args.index("--lookback-months") + 1] == "3"
    assert args[args.index("--as-of-date") + 1] == "2026-06-30T23:59:59Z"
    assert args.count("--instrument") == 2


def test_signal_density_score_prefers_usable_balanced_rollups() -> None:
    score, density_bucket, balance_bucket = signal_density_score(
        {
            "status": "ok",
            "density_bucket": "usable",
            "event_count": 600,
            "balance_bucket_counts": {"balanced": 8},
        }
    )

    assert density_bucket == "usable"
    assert balance_bucket == "balanced"
    assert score > 85.0


def test_score_anchor_pair_blocks_default_problem_triggers() -> None:
    scored = score_anchor_pair(
        anchor_type="compression",
        anchor_row={"base_indicator_id": "SQUEEZE"},
        trigger_row={
            "signal_role": "trigger",
            "strategy_role": "confirm",
            "base_indicator_id": "PATTERN",
        },
        static_pair_row={"compatibility_prior_score": 90},
        signal_rollup={
            "status": "ok",
            "density_bucket": "flat",
            "event_count": 0,
            "balance_bucket_counts": {"flat": 8},
        },
        forward_prior={
            "forward_response_prior_score": 0,
            "forward_response_prior_bucket": "default_problem",
            "best_cell_context": "",
        },
        probe_timeframe="M5",
        instruments=["EURUSD"],
    )

    assert scored["pair_prior_bucket"] == "blocked_default_problem"
    assert scored["pair_issue"] == "flat_or_no_forward_events"


def test_select_anchor_pair_queue_diversifies_anchor_types() -> None:
    rows = [
        {
            "anchor_type": "trend",
            "anchor_id": "A_TREND",
            "trigger_id": "TRIGGER_A",
            "probe_timeframe": "M5",
            "pair_prior_score": 75,
            "pair_prior_bucket": "probe_now",
        },
        {
            "anchor_type": "compression",
            "anchor_id": "A_COMP",
            "trigger_id": "TRIGGER_A",
            "probe_timeframe": "M5",
            "pair_prior_score": 74,
            "pair_prior_bucket": "probe_now",
        },
        {
            "anchor_type": "mean_reversion",
            "anchor_id": "A_MR",
            "trigger_id": "TRIGGER_A",
            "probe_timeframe": "M5",
            "pair_prior_score": 73,
            "pair_prior_bucket": "probe_now",
        },
    ]

    queue = select_anchor_pair_queue(rows, max_pairs=3)

    assert {row["anchor_type"] for row in queue} == {
        "trend",
        "compression",
        "mean_reversion",
    }
    assert [row["queue_rank"] for row in queue] == [1, 2, 3]
    assert all(row["probe_id"].startswith("l3-") for row in queue)


def test_build_pair_profile_document_uses_anchor_and_trigger_timeframes() -> None:
    catalog_by_id = {
        "ANCHOR": {
            "meta": {
                "id": "ANCHOR",
                "name": "Anchor",
                "namespace": "Trend",
                "talibFunction": "ANCHOR_FN",
                "supportsTradingMode": False,
                "usesRangeConfiguration": True,
                "description": "Anchor description",
                "inputs": ["close"],
                "valueRange": {"min": 0, "max": 1},
            },
            "config": {
                "label": "Anchor",
                "timeframe": "M15",
                "ranges": {"buy": [0, 1], "sell": [0, 1]},
            }
        },
        "TRIGGER": {
            "meta": {
                "id": "TRIGGER",
                "name": "Trigger",
                "namespace": "Momentum",
                "talibFunction": "TRIGGER_FN",
                "supportsTradingMode": False,
                "usesRangeConfiguration": True,
                "description": "Trigger description",
                "inputs": ["close"],
                "valueRange": {"min": 0, "max": 1},
            },
            "config": {
                "label": "Trigger",
                "timeframe": "M5",
                "ranges": {"buy": [0, 1], "sell": [0, 1]},
            }
        },
    }

    doc = build_pair_profile_document(
        catalog_by_id=catalog_by_id,
        anchor_id="ANCHOR",
        trigger_id="TRIGGER",
        anchor_type="trend",
        probe_timeframe="M5",
        anchor_timeframe="M15",
        instruments=["EURUSD", "GBPUSD"],
        probe_id="l3-test",
    )

    indicators = doc["profile"]["indicators"]
    assert doc["profile"]["notificationThreshold"] == 80
    assert indicators[0]["config"]["timeframe"] == "M15"
    assert indicators[1]["config"]["timeframe"] == "M5"
    assert indicators[0]["meta"]["name"] == "Anchor"
    assert indicators[0]["meta"]["namespace"] == "Trend"
    assert indicators[0]["meta"]["talibFunction"] == "ANCHOR_FN"
    assert indicators[0]["meta"]["instanceId"] == "l3-test-anchor"
    assert indicators[1]["meta"]["name"] == "Trigger"
    assert indicators[1]["meta"]["namespace"] == "Momentum"
    assert indicators[1]["meta"]["talibFunction"] == "TRIGGER_FN"
    assert indicators[1]["meta"]["instanceId"] == "l3-test-trigger"
    assert doc["profile"]["instruments"] == ["EURUSD", "GBPUSD"]


def test_build_pair_profile_document_can_override_trigger_lookback_bars() -> None:
    catalog_by_id = {
        "ANCHOR": {
            "config": {
                "label": "Anchor",
                "lookbackBars": 1,
                "ranges": {"buy": [0, 1], "sell": [0, 1]},
            }
        },
        "TRIGGER": {
            "config": {
                "label": "Trigger",
                "lookbackBars": 1,
                "ranges": {"buy": [0, 1], "sell": [0, 1]},
            }
        },
    }

    doc = build_pair_profile_document(
        catalog_by_id=catalog_by_id,
        anchor_id="ANCHOR",
        trigger_id="TRIGGER",
        anchor_type="trend",
        probe_timeframe="M5",
        anchor_timeframe="M15",
        instruments=["EURUSD"],
        probe_id="l3b-test",
        trigger_lookback_bars=3,
    )

    indicators = doc["profile"]["indicators"]
    assert indicators[0]["config"]["lookbackBars"] == 1
    assert indicators[1]["config"]["lookbackBars"] == 3


def test_timing_lookback_values_deduplicates_and_sorts() -> None:
    assert _timing_lookback_values([3, 1, 2, 2, 0]) == [1, 2, 3]


def test_timing_variant_sides_accepts_both_side_variant() -> None:
    assert _timing_variant_sides(["both", "trigger", "bogus", "both"]) == [
        "both",
        "trigger",
    ]


def test_timing_bucket_identifies_rescued_and_lost_positive() -> None:
    assert _timing_bucket(score=55, baseline_score=0, status="ok") == "rescued_positive"
    assert _timing_bucket(score=45, baseline_score=60, status="ok") == "lost_positive"
    assert _timing_bucket(score=66, baseline_score=60, status="ok") == "improved"
    assert _timing_bucket(score=54, baseline_score=60, status="ok") == "degraded"
    assert _timing_bucket(score=60, baseline_score=60, status="failed") == "unscored"


def test_replace_profile_id_arg_preserves_other_args() -> None:
    assert _replace_profile_id_arg(
        ["sensitivity-basket", "--profile-ref", "<PROFILE_ID>", "--timeframe", "M5"],
        "abc123",
    ) == ["sensitivity-basket", "--profile-ref", "abc123", "--timeframe", "M5"]
