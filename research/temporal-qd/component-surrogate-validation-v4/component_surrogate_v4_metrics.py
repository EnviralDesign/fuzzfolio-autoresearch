"""Outcome-free event and forward-response primitives for V4.

The formulas deliberately mirror ``forward_response_atlas_v1`` where its raw
score event input can be replaced by the sealed raw EventBinding state.  V4
keeps its unitless directional returns, while the volatility-normalized field
uses the Atlas's percent-volatility denominator exactly.
"""

from __future__ import annotations

import math
import statistics
from typing import Any

import numpy as np
import pandas as pd


FORWARD_HORIZONS = (1, 3, 6, 12, 24)
ATLAS_VOL_LOOKBACK = 48


def event_start_mask(events: np.ndarray, *, prior_event: bool) -> np.ndarray:
    """Return false-to-true starts, preserving the warm-up predecessor."""
    values = np.asarray(events, dtype=bool).reshape(-1)
    if not len(values):
        return np.asarray([], dtype=bool)
    prior = np.empty(len(values), dtype=bool)
    prior[0] = bool(prior_event)
    prior[1:] = values[:-1]
    return values & ~prior


def event_series_payload(index: pd.DatetimeIndex, events: np.ndarray) -> dict[str, Any]:
    """Stable identity material for one complete decision-clock event series."""
    values = np.asarray(events, dtype=bool).reshape(-1)
    if len(index) != len(values):
        raise RuntimeError("event-series timestamp/value length drift")
    if index.tz is None:
        raise RuntimeError("event-series timestamps must be UTC-aware")
    return {
        "schemaVersion": "temporal_qd_component_surrogate_event_series_v4",
        "timestamps": [stamp.strftime("%Y-%m-%dT%H:%M:%SZ") for stamp in index],
        "active": values.astype(bool).tolist(),
    }


def _pre_event_volatility_pct(close: np.ndarray, event_index: int, lookback: int) -> float | None:
    """Exact ``forward_response_atlas_v1`` pre-event volatility calculation."""
    start = max(1, event_index - max(2, int(lookback)) + 1)
    returns: list[float] = []
    for position in range(start, event_index + 1):
        previous = float(close[position - 1])
        current = float(close[position])
        if previous > 0.0 and current > 0.0 and math.isfinite(previous) and math.isfinite(current):
            returns.append(((current - previous) / previous) * 100.0)
    if len(returns) < 2:
        return None
    return round(float(statistics.pstdev(returns)), 6)


def forward_response_by_horizon(
    *,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    starts: np.ndarray,
    direction: str,
    horizons: tuple[int, ...] = FORWARD_HORIZONS,
    volatility_lookback: int = ATLAS_VOL_LOOKBACK,
) -> list[dict[str, Any]]:
    """Summarize corrected forward responses for every predeclared horizon."""
    if direction not in {"long", "short"}:
        raise ValueError(f"unsupported direction: {direction!r}")
    close_values = np.asarray(close, dtype=float).reshape(-1)
    high_values = np.asarray(high, dtype=float).reshape(-1)
    low_values = np.asarray(low, dtype=float).reshape(-1)
    start_positions = np.asarray(starts, dtype=int).reshape(-1)
    if not (len(close_values) == len(high_values) == len(low_values)):
        raise RuntimeError("forward-response price-array length drift")
    sign = 1.0 if direction == "long" else -1.0
    result: list[dict[str, Any]] = []
    for horizon in horizons:
        samples = [int(index) for index in start_positions if int(index) + horizon < len(close_values)]
        if not samples:
            result.append({
                "horizonBars": horizon,
                "sampleCount": 0,
                "meanDirectionalReturn": None,
                "medianDirectionalReturn": None,
                "directionalHitRate": None,
                "meanMFE": None,
                "meanMAE": None,
                "meanMFEminusMAE": None,
                "mfeGreaterThanMaeRate": None,
                "volatilitySampleCount": 0,
                "meanPreEventVolatilityPct": None,
                "meanVolatilityNormalizedDirectionalReturn": None,
                "legacyV3ExcursionSpanDiagnostic": None,
                "unavailableReason": "no event start has a complete forward horizon",
            })
            continue
        directional_returns: list[float] = []
        mfes: list[float] = []
        maes: list[float] = []
        volatility_values: list[float] = []
        volatility_normalized: list[float] = []
        for index in samples:
            entry = float(close_values[index])
            highs = high_values[index + 1 : index + horizon + 1]
            lows = low_values[index + 1 : index + horizon + 1]
            future_close = float(close_values[index + horizon])
            if (
                entry <= 0.0
                or not math.isfinite(entry)
                or not math.isfinite(future_close)
                or len(highs) != horizon
                or len(lows) != horizon
                or not np.isfinite(highs).all()
                or not np.isfinite(lows).all()
            ):
                continue
            directional_return = sign * (future_close - entry) / entry
            maximum_high = float(np.max(highs))
            minimum_low = float(np.min(lows))
            if direction == "long":
                mfe = max(0.0, (maximum_high - entry) / entry)
                mae = max(0.0, (entry - minimum_low) / entry)
            else:
                mfe = max(0.0, (entry - minimum_low) / entry)
                mae = max(0.0, (maximum_high - entry) / entry)
            if not all(math.isfinite(value) for value in (directional_return, mfe, mae)):
                continue
            directional_returns.append(directional_return)
            mfes.append(mfe)
            maes.append(mae)
            pre_event_volatility_pct = _pre_event_volatility_pct(close_values, index, volatility_lookback)
            if pre_event_volatility_pct is not None and pre_event_volatility_pct > 0.0:
                volatility_values.append(pre_event_volatility_pct)
                normalized = (directional_return * 100.0) / (
                    pre_event_volatility_pct * math.sqrt(float(horizon))
                )
                if math.isfinite(normalized):
                    volatility_normalized.append(normalized)
        if not directional_returns:
            result.append({
                "horizonBars": horizon,
                "sampleCount": 0,
                "meanDirectionalReturn": None,
                "medianDirectionalReturn": None,
                "directionalHitRate": None,
                "meanMFE": None,
                "meanMAE": None,
                "meanMFEminusMAE": None,
                "mfeGreaterThanMaeRate": None,
                "volatilitySampleCount": 0,
                "meanPreEventVolatilityPct": None,
                "meanVolatilityNormalizedDirectionalReturn": None,
                "legacyV3ExcursionSpanDiagnostic": None,
                "unavailableReason": "forward bars contain no finite price sample",
            })
            continue
        result.append({
            "horizonBars": horizon,
            "sampleCount": len(directional_returns),
            "meanDirectionalReturn": float(statistics.fmean(directional_returns)),
            "medianDirectionalReturn": float(statistics.median(directional_returns)),
            "directionalHitRate": float(sum(value > 0.0 for value in directional_returns) / len(directional_returns)),
            "meanMFE": float(statistics.fmean(mfes)),
            "meanMAE": float(statistics.fmean(maes)),
            "meanMFEminusMAE": float(statistics.fmean(mfe - mae for mfe, mae in zip(mfes, maes, strict=True))),
            "mfeGreaterThanMaeRate": float(sum(mfe > mae for mfe, mae in zip(mfes, maes, strict=True)) / len(mfes)),
            "volatilitySampleCount": len(volatility_normalized),
            "meanPreEventVolatilityPct": float(statistics.fmean(volatility_values)) if volatility_values else None,
            "meanVolatilityNormalizedDirectionalReturn": float(statistics.fmean(volatility_normalized)) if volatility_normalized else None,
            "legacyV3ExcursionSpanDiagnostic": float(statistics.fmean(mfe + mae for mfe, mae in zip(mfes, maes, strict=True))),
            "unavailableReason": None,
        })
    return result
