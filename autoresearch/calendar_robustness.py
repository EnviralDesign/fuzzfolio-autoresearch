"""Calendar robustness metrics for 36-month scrutiny equity curves.

Measures whether a strategy's profit is spread across the backtest window or
concentrated in a single era (typically the most recent one). Used by the
play-hand promotion gate to avoid promoting regime-concentrated strategies.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

MIN_SUFFICIENT_CURVE_POINTS = 60
DEFAULT_SEGMENT_COUNT = 4
DEFAULT_MIN_SEGMENTS_POSITIVE = 3
DEFAULT_MAX_RECENT_THIRD_SHARE = 0.8

GATE_REASON_SEGMENTS = "calendar_segments_insufficient"
GATE_REASON_RECENCY = "calendar_recency_concentration"
GATE_REASON_CURVE_INSUFFICIENT = "calendar_curve_insufficient"


@dataclass(frozen=True)
class CalendarRobustness:
    segment_r: list[float]
    segments_positive: int
    recent_third_share: float | None
    total_r: float
    window_start: str | None
    window_end: str | None
    point_count: int
    sufficient: bool


@dataclass(frozen=True)
class GateDecision:
    passed: bool
    reasons: list[str]
    metrics: dict[str, Any]


def _parse_curve_point(point: Any) -> tuple[date, float] | None:
    if not isinstance(point, dict):
        return None
    raw_date = str(point.get("date") or "").strip()[:10]
    if not raw_date:
        return None
    try:
        parsed_date = date.fromisoformat(raw_date)
    except ValueError:
        return None
    value = point.get("realized_r")
    if value is None or isinstance(value, bool):
        return None
    try:
        realized = float(value)
    except (TypeError, ValueError):
        return None
    return parsed_date, realized


def _slice_sums(
    parsed: list[tuple[date, float]],
    *,
    start_ordinal: int,
    span_days: int,
    slice_count: int,
) -> list[float]:
    """Sum realized_r into equal TIME slices between the first and last date."""
    sums = [0.0] * slice_count
    for day, value in parsed:
        index = ((day.toordinal() - start_ordinal) * slice_count) // span_days
        if index >= slice_count:
            index = slice_count - 1
        elif index < 0:
            index = 0
        sums[index] += value
    return sums


def compute_calendar_robustness(
    points: list[dict],
    *,
    segment_count: int = DEFAULT_SEGMENT_COUNT,
) -> CalendarRobustness:
    if segment_count < 1:
        raise ValueError("segment_count must be a positive integer")
    parsed = sorted(
        (entry for entry in (_parse_curve_point(point) for point in points) if entry is not None),
        key=lambda entry: entry[0],
    )
    point_count = len(parsed)
    total_r = sum(value for _, value in parsed)
    window_start = parsed[0][0].isoformat() if parsed else None
    window_end = parsed[-1][0].isoformat() if parsed else None
    span_days = (parsed[-1][0].toordinal() - parsed[0][0].toordinal()) if parsed else 0
    sufficient = point_count >= MIN_SUFFICIENT_CURVE_POINTS and span_days > 0

    if not parsed or span_days <= 0:
        return CalendarRobustness(
            segment_r=[],
            segments_positive=0,
            recent_third_share=None,
            total_r=total_r,
            window_start=window_start,
            window_end=window_end,
            point_count=point_count,
            sufficient=False,
        )

    start_ordinal = parsed[0][0].toordinal()
    segment_r = _slice_sums(
        parsed,
        start_ordinal=start_ordinal,
        span_days=span_days,
        slice_count=segment_count,
    )
    segments_positive = sum(1 for value in segment_r if value > 0)
    thirds = _slice_sums(
        parsed,
        start_ordinal=start_ordinal,
        span_days=span_days,
        slice_count=3,
    )
    recent_third_share = (thirds[-1] / total_r) if total_r > 0 else None
    return CalendarRobustness(
        segment_r=segment_r,
        segments_positive=segments_positive,
        recent_third_share=recent_third_share,
        total_r=total_r,
        window_start=window_start,
        window_end=window_end,
        point_count=point_count,
        sufficient=sufficient,
    )


def evaluate_calendar_gate(
    robustness: CalendarRobustness,
    *,
    min_segments_positive: int = DEFAULT_MIN_SEGMENTS_POSITIVE,
    max_recent_third_share: float = DEFAULT_MAX_RECENT_THIRD_SHARE,
) -> GateDecision:
    metrics: dict[str, Any] = {
        "segment_r": list(robustness.segment_r),
        "segments_positive": robustness.segments_positive,
        "recent_third_share": robustness.recent_third_share,
        "total_r": robustness.total_r,
        "window_start": robustness.window_start,
        "window_end": robustness.window_end,
        "point_count": robustness.point_count,
        "sufficient": robustness.sufficient,
        "min_segments_positive": min_segments_positive,
        "max_recent_third_share": max_recent_third_share,
    }
    if not robustness.sufficient:
        # Missing or short curves should not punish promotion; pass with a warning.
        return GateDecision(
            passed=True,
            reasons=[GATE_REASON_CURVE_INSUFFICIENT],
            metrics=metrics,
        )
    reasons: list[str] = []
    if robustness.segments_positive < min_segments_positive:
        reasons.append(GATE_REASON_SEGMENTS)
    if (
        robustness.recent_third_share is not None
        and robustness.recent_third_share > max_recent_third_share
    ):
        reasons.append(GATE_REASON_RECENCY)
    return GateDecision(passed=not reasons, reasons=reasons, metrics=metrics)
