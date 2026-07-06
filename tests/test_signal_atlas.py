from __future__ import annotations

from autoresearch.signal_atlas import (
    _normalize_signal_roles,
    _profile_document_for_indicator,
    _select_indicator_ids,
    compute_signal_metrics,
)


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


def test_select_indicator_ids_all_signal_roles() -> None:
    rows = {
        "TRIGGER_A": {"signal_role": "trigger", "static_prior_score": "70"},
        "SETUP_A": {"signal_role": "setup", "static_prior_score": "80"},
        "CONTEXT_A": {"signal_role": "context", "static_prior_score": "60"},
    }

    selected = _select_indicator_ids(
        rows,
        indicator_ids=None,
        signal_role="all",
        max_indicators=None,
    )

    assert selected == ["SETUP_A", "TRIGGER_A", "CONTEXT_A"]


def test_select_indicator_ids_accepts_comma_separated_signal_roles() -> None:
    rows = {
        "TRIGGER_A": {"signal_role": "trigger", "static_prior_score": "70"},
        "SETUP_A": {"signal_role": "setup", "static_prior_score": "80"},
        "CONTEXT_A": {"signal_role": "context", "static_prior_score": "90"},
        "FILTER_A": {"signal_role": "filter", "static_prior_score": "60"},
    }

    selected = _select_indicator_ids(
        rows,
        indicator_ids=None,
        signal_role="setup,filter",
        max_indicators=None,
    )

    assert selected == ["SETUP_A", "FILTER_A"]


def test_normalize_signal_roles_all_is_empty_filter() -> None:
    assert _normalize_signal_roles("all") == []
    assert _normalize_signal_roles(["trigger", "setup,context"]) == [
        "trigger",
        "setup",
        "context",
    ]


def test_profile_document_preserves_worker_required_indicator_metadata() -> None:
    catalog_item = {
        "meta": {
            "id": "RSI_CROSSBACK",
            "name": "RSI Crossback",
            "namespace": "TRIGGERS",
            "talibFunction": "RSI_CROSSBACK",
            "supportsTradingMode": True,
            "usesRangeConfiguration": True,
            "description": "Event trigger.",
            "inputs": ["close"],
            "valueRange": {"min": 0, "max": 1, "step": 1, "marks": [], "minRange": 0},
            "talibMeta": [],
            "requiredPaddingBars": 300,
            "signalRole": "trigger",
        },
        "config": {
            "label": "RSI Crossback",
            "ranges": {"buy": [0, 1], "sell": [0, 1]},
            "talibConfig": [],
            "weight": 1,
            "timeframe": "M5",
            "lookbackBars": 1,
            "isActive": False,
        },
    }

    document = _profile_document_for_indicator(
        catalog_item,
        indicator_id="RSI_CROSSBACK",
        timeframe="M15",
        instruments=["EURUSD"],
        label_prefix="Signal Atlas",
    )

    indicator = document["profile"]["indicators"][0]
    meta = indicator["meta"]
    assert meta["id"] == "RSI_CROSSBACK"
    assert meta["instanceId"] == "signal-atlas-rsi-crossback-m15"
    assert meta["name"] == "RSI Crossback"
    assert meta["namespace"] == "TRIGGERS"
    assert meta["talibFunction"] == "RSI_CROSSBACK"
    assert meta["supportsTradingMode"] is True
    assert meta["usesRangeConfiguration"] is True
    assert indicator["config"]["timeframe"] == "M15"
    assert indicator["config"]["isActive"] is True
