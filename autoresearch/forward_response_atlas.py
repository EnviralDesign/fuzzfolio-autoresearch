from __future__ import annotations

import csv
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import AppConfig
from .signal_atlas import DEFAULT_SIGNAL_ATLAS_DIRNAME


SCHEMA_VERSION = "forward_response_atlas_v1"
DEFAULT_FORWARD_RESPONSE_DIRNAME = "forward-response-atlas"
DEFAULT_FORWARD_HORIZONS = (1, 3, 6, 12, 24)
DEFAULT_VOL_LOOKBACK = 48
DEFAULT_MIN_EVENTS = 30
SIGNAL_EPSILON = 1e-9


@dataclass(frozen=True)
class ForwardResponseAtlasBuildResult:
    atlas_path: Path
    cell_csv_path: Path
    rollup_csv_path: Path
    priors_csv_path: Path
    issues_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "forward_response_atlas_json": str(self.atlas_path),
            "forward_response_cell_csv": str(self.cell_csv_path),
            "forward_response_rollup_csv": str(self.rollup_csv_path),
            "forward_response_priors_csv": str(self.priors_csv_path),
            "forward_response_issues_csv": str(self.issues_path),
            "forward_response_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _numeric_series(values: Any) -> list[float]:
    series: list[float] = []
    for value in _as_list(values):
        if isinstance(value, bool):
            series.append(1.0 if value else 0.0)
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            series.append(0.0)
            continue
        series.append(number if math.isfinite(number) else 0.0)
    return series


def _mean(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return None
    return round(float(statistics.fmean(clean)), 6)


def _median(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return None
    return round(float(statistics.median(clean)), 6)


def _percentile(values: Iterable[float], percentile: float) -> float | None:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return round(clean[0], 6)
    rank = (len(clean) - 1) * max(0.0, min(1.0, percentile))
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return round(clean[lower], 6)
    weight = rank - lower
    return round(clean[lower] * (1.0 - weight) + clean[upper] * weight, 6)


def _stddev(values: list[float]) -> float | None:
    clean = [value for value in values if math.isfinite(value)]
    if len(clean) < 2:
        return None
    return float(statistics.pstdev(clean))


def _event_starts(active_flags: list[bool]) -> list[int]:
    starts: list[int] = []
    previous = False
    for index, active in enumerate(active_flags):
        if active and not previous:
            starts.append(index)
        previous = active
    return starts


def _pre_event_volatility_pct(close: list[float], event_index: int, lookback: int) -> float | None:
    start = max(1, event_index - max(2, int(lookback)) + 1)
    returns: list[float] = []
    for index in range(start, event_index + 1):
        previous = close[index - 1]
        current = close[index]
        if previous > 0 and current > 0:
            returns.append(((current - previous) / previous) * 100.0)
    stdev = _stddev(returns)
    return None if stdev is None else round(stdev, 6)


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100.0, 4)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _data_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _extract_chronological_series(payload: dict[str, Any]) -> dict[str, list[Any]]:
    data = _data_payload(payload)
    close = _numeric_series(data.get("close"))
    high = _numeric_series(data.get("high"))
    low = _numeric_series(data.get("low"))
    long_score = _numeric_series(data.get("long_score"))
    short_score = _numeric_series(data.get("short_score"))
    timestamps = _as_list(data.get("timestamp"))
    bars = min(
        len(close),
        len(high),
        len(low),
        len(long_score),
        len(short_score),
        len(timestamps) if timestamps else len(close),
    )
    return {
        "timestamp": list(reversed(timestamps[:bars])) if timestamps else [],
        "close": list(reversed(close[:bars])),
        "high": list(reversed(high[:bars])),
        "low": list(reversed(low[:bars])),
        "long_score": list(reversed(long_score[:bars])),
        "short_score": list(reversed(short_score[:bars])),
    }


def compute_forward_event_records(
    close: list[float],
    high: list[float],
    low: list[float],
    long_score: list[float],
    short_score: list[float],
    *,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    vol_lookback: int = DEFAULT_VOL_LOOKBACK,
    threshold: float = SIGNAL_EPSILON,
) -> list[dict[str, Any]]:
    horizon_values = sorted({max(1, int(value)) for value in horizons})
    bars = min(len(close), len(high), len(low), len(long_score), len(short_score))
    close = close[:bars]
    high = high[:bars]
    low = low[:bars]
    event_records: list[dict[str, Any]] = []

    directions = (
        ("long", 1.0, long_score),
        ("short", -1.0, short_score),
    )
    for direction_label, direction_multiplier, scores in directions:
        flags = [value > threshold for value in scores[:bars]]
        for event_index in _event_starts(flags):
            entry_close = close[event_index]
            if event_index >= bars - 1 or entry_close <= 0:
                continue
            pre_event_volatility_pct = _pre_event_volatility_pct(
                close,
                event_index,
                vol_lookback,
            )
            for horizon in horizon_values:
                future_index = event_index + horizon
                if future_index >= bars:
                    continue
                future_highs = high[event_index + 1 : future_index + 1]
                future_lows = low[event_index + 1 : future_index + 1]
                if not future_highs or not future_lows:
                    continue
                future_close = close[future_index]
                raw_forward_pct = ((future_close - entry_close) / entry_close) * 100.0
                directional_forward_pct = raw_forward_pct * direction_multiplier
                max_high = max(future_highs)
                min_low = min(future_lows)
                if direction_label == "long":
                    mfe_pct = max(0.0, ((max_high - entry_close) / entry_close) * 100.0)
                    mae_pct = max(0.0, ((entry_close - min_low) / entry_close) * 100.0)
                else:
                    mfe_pct = max(0.0, ((entry_close - min_low) / entry_close) * 100.0)
                    mae_pct = max(0.0, ((max_high - entry_close) / entry_close) * 100.0)
                vol_norm = None
                if pre_event_volatility_pct and pre_event_volatility_pct > 0:
                    vol_norm = directional_forward_pct / (
                        pre_event_volatility_pct * math.sqrt(float(horizon))
                    )
                event_records.append(
                    {
                        "direction": direction_label,
                        "event_index": event_index,
                        "horizon_bars": horizon,
                        "entry_close": round(entry_close, 8),
                        "future_close": round(future_close, 8),
                        "forward_return_pct": round(directional_forward_pct, 6),
                        "raw_forward_return_pct": round(raw_forward_pct, 6),
                        "mfe_pct": round(mfe_pct, 6),
                        "mae_pct": round(mae_pct, 6),
                        "mfe_minus_mae_pct": round(mfe_pct - mae_pct, 6),
                        "mfe_gt_mae": mfe_pct > mae_pct,
                        "pre_event_volatility_pct": pre_event_volatility_pct,
                        "volatility_normalized_return": round(vol_norm, 6)
                        if vol_norm is not None and math.isfinite(vol_norm)
                        else None,
                    }
                )
    return event_records


def _response_bucket(summary: dict[str, Any], *, min_events: int) -> str:
    samples = int(summary.get("sample_count") or 0)
    if samples <= 0:
        return "no_events"
    if samples < max(1, int(min_events)):
        return "low_sample"
    mean_forward = float(summary.get("mean_forward_return_pct") or 0.0)
    median_forward = float(summary.get("median_forward_return_pct") or 0.0)
    win_rate = float(summary.get("win_rate_pct") or 0.0)
    mfe_rate = float(summary.get("mfe_gt_mae_rate_pct") or 0.0)
    mean_edge = float(summary.get("mean_mfe_minus_mae_pct") or 0.0)

    if mean_forward > 0 and median_forward > 0 and win_rate >= 52.5:
        return "directional_tailwind"
    if mean_forward < 0 and median_forward < 0 and win_rate <= 47.5:
        return "directional_headwind"
    if mean_edge > 0 and mfe_rate >= 55.0:
        return "favorable_excursion_asymmetry"
    if mean_edge < 0 and mfe_rate <= 45.0:
        return "adverse_excursion_asymmetry"
    return "neutral_mixed"


def _forward_response_score(summary: dict[str, Any], *, min_events: int) -> float:
    samples = int(summary.get("sample_count") or 0)
    if samples <= 0:
        return 0.0
    sample_factor = min(1.0, samples / max(1.0, float(min_events) * 4.0))
    win_rate = float(summary.get("win_rate_pct") or 50.0)
    mfe_rate = float(summary.get("mfe_gt_mae_rate_pct") or 50.0)
    vol_norm = float(summary.get("mean_volatility_normalized_return") or 0.0)
    score = 50.0
    score += _clamp((win_rate - 50.0) * 1.3, -16.0, 16.0)
    score += _clamp((mfe_rate - 50.0) * 0.9, -14.0, 14.0)
    score += _clamp(vol_norm * 10.0, -14.0, 14.0)
    score = 50.0 + (score - 50.0) * sample_factor
    return round(_clamp(score, 0.0, 100.0), 2)


def summarize_forward_events(
    event_records: list[dict[str, Any]],
    *,
    min_events: int = DEFAULT_MIN_EVENTS,
) -> dict[str, Any]:
    sample_count = len(event_records)
    if sample_count <= 0:
        return {
            "status": "no_events",
            "sample_count": 0,
            "response_bucket": "no_events",
            "forward_response_score": 0.0,
        }

    returns = [float(record["forward_return_pct"]) for record in event_records]
    mfe_values = [float(record["mfe_pct"]) for record in event_records]
    mae_values = [float(record["mae_pct"]) for record in event_records]
    edge_values = [float(record["mfe_minus_mae_pct"]) for record in event_records]
    vol_norm_values = [
        float(record["volatility_normalized_return"])
        for record in event_records
        if record.get("volatility_normalized_return") is not None
    ]
    wins = sum(1 for value in returns if value > 0)
    losses = sum(1 for value in returns if value < 0)
    mfe_wins = sum(1 for record in event_records if bool(record.get("mfe_gt_mae")))
    summary = {
        "status": "ok",
        "sample_count": sample_count,
        "win_count": wins,
        "loss_count": losses,
        "win_rate_pct": _rate(wins, sample_count),
        "mean_forward_return_pct": _mean(returns),
        "median_forward_return_pct": _median(returns),
        "p25_forward_return_pct": _percentile(returns, 0.25),
        "p75_forward_return_pct": _percentile(returns, 0.75),
        "mean_mfe_pct": _mean(mfe_values),
        "median_mfe_pct": _median(mfe_values),
        "mean_mae_pct": _mean(mae_values),
        "median_mae_pct": _median(mae_values),
        "mean_mfe_minus_mae_pct": _mean(edge_values),
        "median_mfe_minus_mae_pct": _median(edge_values),
        "mfe_gt_mae_rate_pct": _rate(mfe_wins, sample_count),
        "mean_volatility_normalized_return": _mean(vol_norm_values),
        "median_volatility_normalized_return": _median(vol_norm_values),
    }
    summary["response_bucket"] = _response_bucket(summary, min_events=min_events)
    summary["forward_response_score"] = _forward_response_score(summary, min_events=min_events)
    return summary


def _grouped_summaries(
    event_records: list[dict[str, Any]],
    keys: tuple[str, ...],
    *,
    min_events: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in event_records:
        grouped[tuple(record.get(key) for key in keys)].append(record)
    rows: list[dict[str, Any]] = []
    for key_values, records in grouped.items():
        row = {key: key_values[index] for index, key in enumerate(keys)}
        row.update(summarize_forward_events(records, min_events=min_events))
        rows.append(row)
    rows.sort(key=lambda row: tuple(str(row.get(key) or "") for key in keys))
    return rows


def _indicator_prior_rows(
    indicator_rows: list[dict[str, Any]],
    cell_rows: list[dict[str, Any]],
    all_indicator_ids: list[str],
    *,
    min_events: int,
) -> list[dict[str, Any]]:
    rows_by_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in indicator_rows:
        if row.get("direction") != "both":
            continue
        rows_by_indicator[str(row.get("indicator_id") or "")].append(row)
    cells_by_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cell_rows:
        if int(row.get("sample_count") or 0) < max(1, min_events):
            continue
        cells_by_indicator[str(row.get("indicator_id") or "")].append(row)

    prior_rows: list[dict[str, Any]] = []
    for indicator_id in all_indicator_ids:
        candidates = rows_by_indicator.get(indicator_id, [])
        cell_candidates = sorted(
            cells_by_indicator.get(indicator_id, []),
            key=lambda row: (
                -float(row.get("forward_response_score") or 0.0),
                -int(row.get("sample_count") or 0),
            ),
        )
        best_cell = cell_candidates[0] if cell_candidates else {}
        strong_cells = [
            row
            for row in cell_candidates
            if float(row.get("forward_response_score") or 0.0) >= 70.0
        ]
        if not candidates:
            prior_rows.append(
                {
                    "indicator_id": indicator_id,
                    "status": "no_events",
                    "best_horizon_bars": None,
                    "forward_response_prior_score": 0.0,
                    "aggregate_forward_response_score": 0.0,
                    "forward_response_prior_bucket": "default_problem",
                    "response_bucket": "no_events",
                    "sample_count": 0,
                    "best_cell_score": float(best_cell.get("forward_response_score") or 0.0),
                    "strong_cell_count": len(strong_cells),
                }
            )
            continue
        best = max(
            candidates,
            key=lambda row: (
                float(row.get("forward_response_score") or 0.0),
                int(row.get("sample_count") or 0),
            ),
        )
        score = float(best.get("forward_response_score") or 0.0)
        bucket = str(best.get("response_bucket") or "")
        samples = int(best.get("sample_count") or 0)
        if samples < max(1, min_events):
            prior_bucket = "uncertain_low_sample"
        elif bucket == "directional_tailwind" and score >= 62.0:
            prior_bucket = "promising_forward_response"
        elif bucket == "favorable_excursion_asymmetry" and score >= 57.0:
            prior_bucket = "promising_exit_asymmetry"
        elif float(best_cell.get("forward_response_score") or 0.0) >= 70.0:
            prior_bucket = "context_dependent_forward_response"
        elif bucket in {"directional_headwind", "adverse_excursion_asymmetry"} and score <= 43.0:
            prior_bucket = "low_prior_forward_response"
        else:
            prior_bucket = "neutral_forward_response"
        best_cell_context = ""
        if best_cell:
            best_cell_context = " ".join(
                str(best_cell.get(key) or "")
                for key in ("instrument", "timeframe", "direction", "horizon_bars")
            ).strip()
        best_cell_score = float(best_cell.get("forward_response_score") or 0.0)
        prior_score = score
        if prior_bucket == "context_dependent_forward_response":
            prior_score = max(score, min(75.0, best_cell_score * 0.90))
        prior_rows.append(
            {
                "indicator_id": indicator_id,
                "status": "ok",
                "best_horizon_bars": best.get("horizon_bars"),
                "forward_response_prior_score": round(prior_score, 2),
                "aggregate_forward_response_score": round(score, 2),
                "forward_response_prior_bucket": prior_bucket,
                "response_bucket": bucket,
                "sample_count": samples,
                "win_rate_pct": best.get("win_rate_pct"),
                "mean_forward_return_pct": best.get("mean_forward_return_pct"),
                "median_forward_return_pct": best.get("median_forward_return_pct"),
                "mfe_gt_mae_rate_pct": best.get("mfe_gt_mae_rate_pct"),
                "mean_mfe_minus_mae_pct": best.get("mean_mfe_minus_mae_pct"),
                "mean_volatility_normalized_return": best.get("mean_volatility_normalized_return"),
                "best_cell_score": best_cell.get("forward_response_score"),
                "best_cell_context": best_cell_context,
                "best_cell_response_bucket": best_cell.get("response_bucket"),
                "best_cell_sample_count": best_cell.get("sample_count"),
                "best_cell_win_rate_pct": best_cell.get("win_rate_pct"),
                "best_cell_mean_forward_return_pct": best_cell.get("mean_forward_return_pct"),
                "strong_cell_count": len(strong_cells),
            }
        )
    prior_rows.sort(
        key=lambda row: (
            -float(row.get("forward_response_prior_score") or 0.0),
            str(row.get("indicator_id") or ""),
        )
    )
    return prior_rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _write_issues_csv(path: Path, prior_rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "indicator_id",
        "issue",
        "severity",
        "detail",
        "forward_response_prior_score",
        "sample_count",
    ]
    issue_rows: list[dict[str, Any]] = []
    for row in prior_rows:
        bucket = str(row.get("forward_response_prior_bucket") or "")
        if bucket == "default_problem":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "no_forward_events",
                    "severity": "high",
                    "detail": row.get("response_bucket"),
                    "forward_response_prior_score": row.get("forward_response_prior_score"),
                    "sample_count": row.get("sample_count"),
                }
            )
        elif bucket == "uncertain_low_sample":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "low_forward_sample",
                    "severity": "medium",
                    "detail": row.get("response_bucket"),
                    "forward_response_prior_score": row.get("forward_response_prior_score"),
                    "sample_count": row.get("sample_count"),
                }
            )
        elif bucket == "low_prior_forward_response":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "negative_forward_response",
                    "severity": "medium",
                    "detail": row.get("response_bucket"),
                    "forward_response_prior_score": row.get("forward_response_prior_score"),
                    "sample_count": row.get("sample_count"),
                }
            )
        elif bucket == "context_dependent_forward_response":
            issue_rows.append(
                {
                    "indicator_id": row.get("indicator_id"),
                    "issue": "context_dependent_forward_response",
                    "severity": "low",
                    "detail": row.get("best_cell_context"),
                    "forward_response_prior_score": row.get("forward_response_prior_score"),
                    "sample_count": row.get("sample_count"),
                }
            )
    _write_csv(path, issue_rows, fieldnames)


def build_forward_response_atlas(
    config: AppConfig,
    *,
    signal_atlas_dir: Path | None = None,
    out_dir: Path | None = None,
    horizons: list[int] | None = None,
    vol_lookback: int = DEFAULT_VOL_LOOKBACK,
    min_events: int = DEFAULT_MIN_EVENTS,
    threshold: float = SIGNAL_EPSILON,
) -> ForwardResponseAtlasBuildResult:
    source_dir = (
        signal_atlas_dir.expanduser().resolve()
        if signal_atlas_dir is not None
        else config.derived_root / DEFAULT_SIGNAL_ATLAS_DIRNAME
    )
    signal_atlas_path = source_dir / "signal-atlas.json"
    if not signal_atlas_path.exists():
        raise FileNotFoundError(
            f"Missing signal atlas at {signal_atlas_path}. Run `uv run build-signal-atlas` first."
        )
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_FORWARD_RESPONSE_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    horizon_values = sorted({max(1, int(value)) for value in (horizons or list(DEFAULT_FORWARD_HORIZONS))})
    vol_lookback = max(2, int(vol_lookback or DEFAULT_VOL_LOOKBACK))
    min_events = max(1, int(min_events or DEFAULT_MIN_EVENTS))
    signal_payload = _as_dict(_load_json(signal_atlas_path))
    signal_summary = _as_dict(signal_payload.get("summary"))
    signal_rows = [
        row
        for row in _as_list(signal_payload.get("rows"))
        if isinstance(row, dict) and row.get("status") == "ok"
    ]
    selected_indicator_ids = [
        _clean_upper(value)
        for value in _as_list(_as_dict(signal_summary.get("selection")).get("indicator_ids"))
    ]

    all_events: list[dict[str, Any]] = []
    cell_rollups: list[dict[str, Any]] = []
    no_event_cells: list[dict[str, Any]] = []
    for signal_row in signal_rows:
        raw_path = Path(str(signal_row.get("raw_path") or ""))
        if not raw_path.is_absolute():
            raw_path = (config.repo_root / raw_path).resolve()
        if not raw_path.exists():
            no_event_cells.append(
                {
                    "indicator_id": _clean_upper(signal_row.get("indicator_id")),
                    "instrument": _clean_upper(signal_row.get("instrument")),
                    "timeframe": _clean_upper(signal_row.get("timeframe")),
                    "status": "missing_raw",
                    "raw_path": str(raw_path),
                }
            )
            continue
        series = _extract_chronological_series(_as_dict(_load_json(raw_path)))
        events = compute_forward_event_records(
            series["close"],
            series["high"],
            series["low"],
            series["long_score"],
            series["short_score"],
            horizons=horizon_values,
            vol_lookback=vol_lookback,
            threshold=threshold,
        )
        indicator_id = _clean_upper(signal_row.get("indicator_id"))
        instrument = _clean_upper(signal_row.get("instrument"))
        timeframe = _clean_upper(signal_row.get("timeframe"))
        for event in events:
            event["indicator_id"] = indicator_id
            event["instrument"] = instrument
            event["timeframe"] = timeframe
        if events:
            all_events.extend(events)
            cell_rollups.extend(
                _grouped_summaries(
                    events,
                    ("indicator_id", "instrument", "timeframe", "direction", "horizon_bars"),
                    min_events=min_events,
                )
            )
        else:
            no_event_cells.append(
                {
                    "indicator_id": indicator_id,
                    "instrument": instrument,
                    "timeframe": timeframe,
                    "status": "no_events",
                    "raw_path": str(raw_path),
                }
            )

    indicator_direction_rows = _grouped_summaries(
        all_events,
        ("indicator_id", "direction", "horizon_bars"),
        min_events=min_events,
    )
    indicator_both_rows = _grouped_summaries(
        all_events,
        ("indicator_id", "horizon_bars"),
        min_events=min_events,
    )
    for row in indicator_both_rows:
        row["direction"] = "both"
    indicator_rollups = [*indicator_both_rows, *indicator_direction_rows]
    indicator_rollups.sort(
        key=lambda row: (
            str(row.get("indicator_id") or ""),
            int(row.get("horizon_bars") or 0),
            str(row.get("direction") or ""),
        )
    )
    prior_rows = _indicator_prior_rows(
        indicator_rollups,
        cell_rollups,
        selected_indicator_ids,
        min_events=min_events,
    )

    prior_bucket_counts: dict[str, int] = {}
    response_bucket_counts: dict[str, int] = {}
    for row in prior_rows:
        prior_bucket = str(row.get("forward_response_prior_bucket") or "unknown")
        response_bucket = str(row.get("response_bucket") or "unknown")
        prior_bucket_counts[prior_bucket] = prior_bucket_counts.get(prior_bucket, 0) + 1
        response_bucket_counts[response_bucket] = response_bucket_counts.get(response_bucket, 0) + 1

    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "signal_atlas_path": str(signal_atlas_path),
            "signal_atlas_generated_at": signal_payload.get("generated_at"),
        },
        "selection": {
            "indicator_count": len(selected_indicator_ids),
            "cell_count": len(signal_rows),
            "horizons": horizon_values,
            "vol_lookback": vol_lookback,
            "min_events": min_events,
            "threshold": threshold,
        },
        "result_counts": {
            "event_horizon_records": len(all_events),
            "cell_rollup_rows": len(cell_rollups),
            "indicator_rollup_rows": len(indicator_rollups),
            "no_event_cells": len(no_event_cells),
            "prior_bucket_counts": dict(sorted(prior_bucket_counts.items())),
            "response_bucket_counts": dict(sorted(response_bucket_counts.items())),
        },
        "priors": prior_rows,
    }

    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "cell_rollups": cell_rollups,
        "indicator_rollups": indicator_rollups,
        "priors": prior_rows,
        "no_event_cells": no_event_cells,
    }
    atlas_path = target_dir / "forward-response-atlas.json"
    cell_csv_path = target_dir / "forward-response-atlas.csv"
    rollup_csv_path = target_dir / "forward-response-rollups.csv"
    priors_csv_path = target_dir / "forward-response-priors.csv"
    issues_path = target_dir / "forward-response-issues.csv"
    summary_path = target_dir / "forward-response-summary.json"

    metric_fields = [
        "indicator_id",
        "instrument",
        "timeframe",
        "direction",
        "horizon_bars",
        "status",
        "sample_count",
        "win_rate_pct",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "p25_forward_return_pct",
        "p75_forward_return_pct",
        "mean_mfe_pct",
        "mean_mae_pct",
        "mean_mfe_minus_mae_pct",
        "mfe_gt_mae_rate_pct",
        "mean_volatility_normalized_return",
        "response_bucket",
        "forward_response_score",
    ]
    rollup_fields = [field for field in metric_fields if field not in {"instrument", "timeframe"}]
    prior_fields = [
        "indicator_id",
        "status",
        "best_horizon_bars",
        "forward_response_prior_score",
        "aggregate_forward_response_score",
        "forward_response_prior_bucket",
        "response_bucket",
        "sample_count",
        "win_rate_pct",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mfe_gt_mae_rate_pct",
        "mean_mfe_minus_mae_pct",
        "mean_volatility_normalized_return",
        "best_cell_score",
        "best_cell_context",
        "best_cell_response_bucket",
        "best_cell_sample_count",
        "best_cell_win_rate_pct",
        "best_cell_mean_forward_return_pct",
        "strong_cell_count",
    ]

    _write_json(atlas_path, atlas_payload)
    _write_json(summary_path, summary)
    _write_csv(cell_csv_path, cell_rollups, metric_fields)
    _write_csv(rollup_csv_path, indicator_rollups, rollup_fields)
    _write_csv(priors_csv_path, prior_rows, prior_fields)
    _write_issues_csv(issues_path, prior_rows)

    return ForwardResponseAtlasBuildResult(
        atlas_path=atlas_path,
        cell_csv_path=cell_csv_path,
        rollup_csv_path=rollup_csv_path,
        priors_csv_path=priors_csv_path,
        issues_path=issues_path,
        summary_path=summary_path,
        summary=summary,
    )
