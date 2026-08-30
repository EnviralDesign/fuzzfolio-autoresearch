"""Unit coverage for the frozen V4 forward-response correction contract."""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import pandas as pd


def load_metrics_module():
    repo_root = Path(__file__).resolve().parents[1]
    path = (
        repo_root
        / "research"
        / "temporal-qd"
        / "component-surrogate-validation-v4"
        / "component_surrogate_v4_metrics.py"
    )
    spec = importlib.util.spec_from_file_location("component_surrogate_v4_metrics", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_corrected_forward_returns_and_positive_adverse_excursion_magnitude() -> None:
    metrics = load_metrics_module()
    close = np.array([10.0, 12.0, 8.0, 10.0])
    high = np.array([10.0, 13.0, 10.1, 10.2])
    low = np.array([10.0, 9.9, 7.0, 7.8])

    long = metrics.forward_response_by_horizon(
        close=close,
        high=high,
        low=low,
        starts=np.array([0]),
        direction="long",
        horizons=(1, 2),
    )
    short = metrics.forward_response_by_horizon(
        close=close,
        high=high,
        low=low,
        starts=np.array([1]),
        direction="short",
        horizons=(1,),
    )

    assert long[0]["meanDirectionalReturn"] == 0.2
    assert short[0]["meanDirectionalReturn"] == (12.0 - 8.0) / 12.0
    assert long[1]["meanMAE"] == 0.3
    assert long[1]["meanMFE"] == 0.3
    assert long[1]["meanMFEminusMAE"] == 0.0


def test_corrected_asymmetry_can_be_negative_and_is_not_v3_excursion_span() -> None:
    metrics = load_metrics_module()
    forward = metrics.forward_response_by_horizon(
        close=np.array([10.0, 9.0]),
        high=np.array([10.0, 10.1]),
        low=np.array([10.0, 8.0]),
        starts=np.array([0]),
        direction="long",
        horizons=(1,),
    )[0]

    assert math.isclose(forward["meanMFE"], 0.01)
    assert math.isclose(forward["meanMAE"], 0.2)
    assert math.isclose(forward["meanMFEminusMAE"], -0.19)
    assert math.isclose(forward["legacyV3ExcursionSpanDiagnostic"], 0.21)
    assert forward["legacyV3ExcursionSpanDiagnostic"] != forward["meanMFEminusMAE"]


def test_event_start_uses_the_recovered_warmup_predecessor() -> None:
    metrics = load_metrics_module()
    events = np.array([True, True, False, True])

    assert metrics.event_start_mask(events, prior_event=True).tolist() == [False, False, False, True]
    assert metrics.event_start_mask(events, prior_event=False).tolist() == [True, False, False, True]


def test_incomplete_end_of_window_horizon_is_excluded() -> None:
    metrics = load_metrics_module()
    forward = metrics.forward_response_by_horizon(
        close=np.array([10.0, 11.0, 12.0]),
        high=np.array([10.0, 11.0, 12.0]),
        low=np.array([10.0, 11.0, 12.0]),
        starts=np.array([0, 2]),
        direction="long",
        horizons=(1, 3),
    )

    assert forward[0]["sampleCount"] == 1
    assert forward[1]["sampleCount"] == 0
    assert forward[1]["unavailableReason"] == "no event start has a complete forward horizon"


def test_event_series_identity_is_stable_and_utc_explicit() -> None:
    metrics = load_metrics_module()
    index = pd.DatetimeIndex(["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z"])

    assert metrics.event_series_payload(index, np.array([False, True])) == {
        "schemaVersion": "temporal_qd_component_surrogate_event_series_v4",
        "timestamps": ["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z"],
        "active": [False, True],
    }
