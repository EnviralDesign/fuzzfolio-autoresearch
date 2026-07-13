from __future__ import annotations

import csv
import importlib.machinery
import importlib.util
import json
import math
import random
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .strategy_identity import derive_strategy_identity


FX_CODES = {
    "AUD",
    "CAD",
    "CHF",
    "CNH",
    "EUR",
    "GBP",
    "HKD",
    "JPY",
    "MXN",
    "NOK",
    "NZD",
    "SEK",
    "SGD",
    "TRY",
    "USD",
    "ZAR",
}
METAL_SYMBOLS = {"XAUUSD", "XAGUSD"}
INDEX_SYMBOLS = {
    "AUS200",
    "DE40",
    "FCHI40",
    "GDAXI",
    "HK50",
    "JP225",
    "NDX",
    "RUSS2000",
    "SP500",
    "SPA35",
    "STOXX50E",
    "UK100",
    "US30",
    "US500",
    "USTECH",
    "WS30",
}
COMMODITY_SYMBOLS = {"UKOUSD", "USOUSD", "XBRUSD", "XNGUSD", "XTIUSD"}
CRYPTO_SYMBOLS = {"BCHUSD", "BTCUSD", "ETHUSD", "LTCUSD", "SOLUSD", "XRPUSD"}
_RUST_OPTIMIZER_MODULES: dict[bool, Any] = {}


DEFAULT_OBJECTIVES: dict[str, dict[str, float]] = {
    "return": {
        "final_r": 1.0,
        "maxdd_r": -2.0,
        "negative_month": -70.0,
        "negative_week": -0.8,
        "worst_week_abs": -1.8,
        "top_day_share": -240.0,
        "loss_streak": -8.0,
        "hold_over_24h": -1.0,
        "avg_open_over_target": -4.0,
        "peak_open_over_target": -1.5,
        "trade_over_target_pm": -0.15,
        "constraint_violation": -240.0,
    },
    "balanced": {
        "final_r": 0.65,
        "maxdd_r": -6.0,
        "positive_month": 10.0,
        "negative_month": -210.0,
        "negative_week": -2.4,
        "worst_week_abs": -3.4,
        "worst_day_abs": -1.3,
        "top_day_share": -520.0,
        "loss_streak": -18.0,
        "hold_over_24h": -3.0,
        "avg_open_over_target": -18.0,
        "peak_open_over_target": -4.0,
        "trade_over_target_pm": -0.55,
        "constraint_violation": -420.0,
    },
    "stability": {
        "final_r": 0.4,
        "maxdd_r": -9.0,
        "positive_month": 20.0,
        "negative_month": -360.0,
        "negative_week": -4.5,
        "worst_month_abs": -8.0,
        "worst_week_abs": -7.0,
        "worst_day_abs": -2.5,
        "top_day_share": -950.0,
        "loss_streak": -35.0,
        "hold_over_24h": -5.0,
        "avg_open_over_target": -30.0,
        "peak_open_over_target": -6.0,
        "trade_over_target_pm": -0.8,
        "constraint_violation": -650.0,
    },
    "deployable": {
        "final_r": 0.52,
        "maxdd_r": -8.5,
        "positive_month": 22.0,
        "negative_month": -420.0,
        "negative_week": -5.0,
        "worst_month_abs": -10.0,
        "worst_week_abs": -8.5,
        "worst_day_abs": -3.0,
        "top_day_share": -900.0,
        "loss_streak": -42.0,
        "hold_over_24h": -7.0,
        "avg_open_over_target": -45.0,
        "peak_open_over_target": -10.0,
        "trade_over_target_pm": -1.0,
        "constraint_violation": -850.0,
    },
}


PARETO_DIMENSIONS: tuple[tuple[str, str], ...] = (
    ("final_r", "max"),
    ("maxdd_r", "min"),
    ("neg_months", "min"),
    ("neg_weeks", "min"),
    ("worst_month_r", "max"),
    ("worst_week_r", "max"),
    ("worst_day_r", "max"),
    ("top_day_gain_share", "min"),
    ("max_daily_loss_streak", "min"),
    ("mean_avg_hold_hours", "min"),
    ("avg_open_positions", "min"),
    ("peak_open_positions", "min"),
)


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def read_json_if_exists(path: str | Path | None) -> Any:
    if not path:
        return None
    try:
        resolved = Path(path)
        if not resolved.exists():
            return None
        return json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def instrument_asset_class(symbol: str) -> str:
    token = str(symbol or "").strip().upper()
    if token in METAL_SYMBOLS:
        return "metal"
    if token in INDEX_SYMBOLS:
        return "index"
    if token in COMMODITY_SYMBOLS:
        return "commodity"
    if token in CRYPTO_SYMBOLS:
        return "crypto"
    if len(token) == 6 and token[:3] in FX_CODES and token[3:] in FX_CODES:
        return "fx"
    return "other"


def row_instruments(row: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for key in ("instruments_36m", "instruments", "instrument", "primary_instrument"):
        raw = row.get(key)
        if isinstance(raw, list):
            values.extend(raw)
        elif isinstance(raw, str):
            values.extend(re.split(r"[,;| ]+", raw))
    output: list[str] = []
    seen: set[str] = set()
    for item in values:
        token = str(item or "").strip().upper()
        if token and token not in seen:
            output.append(token)
            seen.add(token)
    return output


def strategy_family_identity(row: dict[str, Any]) -> tuple[str, str]:
    identity = derive_strategy_identity(row)
    return (
        str(identity["structural_family_id"]),
        str(identity["structural_family_source"]),
    )


def strategy_family_token(row: dict[str, Any]) -> str:
    return strategy_family_identity(row)[0]


def max_drawdown(values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def loss_streak(values: list[float]) -> tuple[int, float]:
    current = 0
    longest = 0
    streaks: list[int] = []
    for value in values:
        if value < -1e-9:
            current += 1
            continue
        if current:
            streaks.append(current)
        longest = max(longest, current)
        current = 0
    if current:
        streaks.append(current)
    longest = max(longest, current)
    average = sum(streaks) / len(streaks) if streaks else 0.0
    return longest, average


def group_values(dates: list[str], values: list[float], mode: str) -> dict[str, float]:
    grouped: dict[str, float] = defaultdict(float)
    for date_text, value in zip(dates, values):
        if mode == "month":
            key = date_text[:7]
        elif mode == "week":
            parsed = datetime.fromisoformat(date_text)
            iso = parsed.isocalendar()
            key = f"{iso.year}-W{iso.week:02d}"
        else:
            key = date_text
        grouped[key] += value
    return dict(grouped)


def count_positive_negative_flat(values: list[float]) -> tuple[int, int, int]:
    positive = sum(1 for value in values if value > 1e-9)
    negative = sum(1 for value in values if value < -1e-9)
    flat = sum(1 for value in values if abs(value) <= 1e-9)
    return positive, negative, flat


def pearson_corr(first: list[float], second: list[float]) -> float:
    size = min(len(first), len(second))
    if size < 3:
        return 0.0
    left = first[:size]
    right = second[:size]
    left_mean = sum(left) / size
    right_mean = sum(right) / size
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 1e-12 or right_var <= 1e-12:
        return 0.0
    covariance = sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right)
    )
    return covariance / math.sqrt(left_var * right_var)


@dataclass(frozen=True)
class PortfolioOptimizerSpec:
    portfolio_name: str = "portfolio-optimizer"
    portfolio_size: int = 20
    candidate_scope: str = "promoted"
    min_score: float = 45.0
    require_positive_source_return: bool = True
    allowed_asset_classes: tuple[str, ...] = ("fx", "metal", "index")
    allowed_instruments: tuple[str, ...] = ()
    blocked_instruments: tuple[str, ...] = ()
    max_avg_hold_hours: float = 48.0
    max_p90_hold_hours: float = 144.0
    max_single_hold_hours: float = 336.0
    candidate_limit: int = 120
    swap_candidate_limit: int = 80
    objective_names: tuple[str, ...] = ("return", "balanced", "stability")
    max_swaps: int = 10
    random_starts: int = 3
    random_seed: int = 17
    max_per_family: int = 1
    max_instrument_share: float = 4.0
    min_fx_share: float = 7.0
    max_metal_share: float = 8.0
    max_index_share: float = 6.0
    max_avg_open_positions: float = 7.0
    max_peak_open_positions: float = 28.0
    target_trades_per_month: float = 160.0
    max_trades_per_month: float = 260.0
    correlation_penalty_weight: float = 0.0
    diversification_mode: str = "penalty"
    portfolio_sharpe_weight: float = 0.0
    risk_weight_multiplier: float = 1.0
    baseline_attempt_ids: tuple[str, ...] = ()
    required_attempt_ids: tuple[str, ...] = ()
    account: dict[str, Any] = field(default_factory=dict, compare=False)


def objective_weights_for_spec(
    spec: PortfolioOptimizerSpec,
) -> dict[str, dict[str, float]]:
    """Resolve objective weights, scaling risk terms without scaling return reward."""
    multiplier = max(0.0, float(spec.risk_weight_multiplier))
    return {
        name: {
            key: (
                float(value)
                if key == "final_r"
                else float(value) * multiplier
            )
            for key, value in weights.items()
        }
        for name, weights in DEFAULT_OBJECTIVES.items()
    }


@dataclass
class OptimizerCandidate:
    attempt_id: str
    row: dict[str, Any]
    instruments: list[str]
    asset_classes: set[str]
    primary_asset_class: str
    family: str
    family_source: str
    lineage_id: str | None
    behavior_fingerprint: str | None
    structural_family_signature: dict[str, Any] | None
    score: float
    created_at: str | None
    avg_hold_hours: float
    p90_hold_hours: float | None
    max_hold_hours: float | None
    path_quality: float | None
    stop_loss_percent: float | None
    trade_count: int
    trades_per_month: float
    dates: list[str]
    daily_r: list[float]
    open_counts: list[int]
    closed_counts: list[int]
    vector: list[float] = field(default_factory=list)
    open_vector: list[int] = field(default_factory=list)
    closed_vector: list[int] = field(default_factory=list)
    month_vector: list[float] = field(default_factory=list)
    week_vector: list[float] = field(default_factory=list)

    @property
    def final_r(self) -> float:
        return sum(self.daily_r)

    @property
    def maxdd_r(self) -> float:
        return max_drawdown(self.daily_r)


def _account_value(account: dict[str, Any], *keys: str, default: float | None = None) -> float | None:
    for key in keys:
        value = safe_float(account.get(key))
        if value is not None:
            return value
    return default


def _account_bool(account: dict[str, Any], key: str, *, default: bool = False) -> bool:
    value = account.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _candidate_account_risk_rejection(
    row: dict[str, Any],
    account: dict[str, Any],
) -> str | None:
    if not isinstance(account, dict) or not account:
        return None
    variance_tolerance = _account_value(
        account,
        "risk_variance_tolerance_pct",
        "max_risk_variance_pct",
        "riskVarianceTolerancePct",
    )
    max_actual_risk_pct = _account_value(
        account,
        "max_actual_risk_pct",
        "maxActualRiskPct",
    )
    filter_enabled = _account_bool(
        account,
        "risk_variance_filter_enabled",
        default=variance_tolerance is not None or max_actual_risk_pct is not None,
    )
    if not filter_enabled:
        return None
    starting_balance = _account_value(
        account,
        "balance_usd",
        "account_size_usd",
        "balance",
        "account_balance",
    )
    configured_risk_pct = _account_value(
        account,
        "risk_per_trade_pct",
        "risk_per_trade_percent",
        "risk_pct",
    )
    min_lot = _account_value(account, "min_lot", "minLot", default=0.0) or 0.0
    lot_step = _account_value(account, "lot_step", "lotStep", default=0.0001) or 0.0001
    notional_per_lot = (
        _account_value(
            account,
            "notional_usd_per_lot",
            "notionalUsdPerLot",
            default=100000.0,
        )
        or 100000.0
    )
    stop_loss_percent = _candidate_stop_loss_percent(row)
    if stop_loss_percent is None:
        stop_loss_percent = _hold_metrics(row).get("stop_loss")
    if (
        starting_balance is None
        or starting_balance <= 0
        or configured_risk_pct is None
        or configured_risk_pct <= 0
        or stop_loss_percent is None
        or stop_loss_percent <= 0
        or notional_per_lot <= 0
    ):
        return None

    target_risk = float(starting_balance) * (float(configured_risk_pct) / 100.0)
    risk_per_lot = float(notional_per_lot) * (float(stop_loss_percent) / 100.0)
    if target_risk <= 0 or risk_per_lot <= 0:
        return None
    raw_lots = target_risk / risk_per_lot
    rounded_lots = (
        math.floor((raw_lots / lot_step) + 1e-9) * lot_step
        if lot_step > 0
        else raw_lots
    )
    lots = min_lot if min_lot > 0 and rounded_lots < min_lot else max(0.0, rounded_lots)
    if lots <= 0:
        return None
    actual_risk = lots * risk_per_lot
    actual_risk_pct = (actual_risk / float(starting_balance)) * 100.0
    actual_multiple = actual_risk / target_risk
    upward_variance_pct = max(0.0, (actual_multiple - 1.0) * 100.0)
    absolute_variance_pct = abs(actual_multiple - 1.0) * 100.0
    direction = str(account.get("risk_variance_direction") or "upside").strip().lower()
    measured_variance = (
        absolute_variance_pct
        if direction in {"absolute", "both", "abs"}
        else upward_variance_pct
    )
    if variance_tolerance is not None and measured_variance > float(variance_tolerance):
        return "account_risk_variance_too_high"
    if max_actual_risk_pct is not None and actual_risk_pct > float(max_actual_risk_pct):
        return "account_actual_risk_too_high"
    return None


def _candidate_stop_loss_percent(row: dict[str, Any]) -> float | None:
    for key in (
        "selected_stop_loss_percent_36m",
        "best_stop_loss_percent_36m",
        "robust_stop_loss_percent_36m",
        "stop_loss_percent_36m",
    ):
        value = safe_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _hold_metrics(row: dict[str, Any]) -> dict[str, float | None]:
    payload = read_json_if_exists(row.get("full_backtest_result_path_36m")) or {}
    aggregate = ((payload.get("data") or {}).get("aggregate") or {})
    metrics = aggregate.get("best_cell_path_metrics") or {}
    best_cell = aggregate.get("best_cell") or {}
    recommended_cell = aggregate.get("recommended_cell") or {}
    stop_loss = _candidate_stop_loss_percent(row)
    if stop_loss is None and isinstance(best_cell, dict):
        stop_loss = safe_float(best_cell.get("stop_loss_percent"))
    if stop_loss is None and isinstance(recommended_cell, dict):
        stop_loss = safe_float(recommended_cell.get("stop_loss_percent"))
    return {
        "avg_hold": safe_float(metrics.get("avg_holding_hours")),
        "p90_hold": safe_float(metrics.get("p90_holding_hours")),
        "max_hold": safe_float(metrics.get("max_holding_hours")),
        "path_quality": safe_float(metrics.get("path_quality")),
        "stop_loss": stop_loss,
    }


def _calendar_daily_curve(row: dict[str, Any]) -> tuple[list[str], list[float], list[int], list[int]]:
    payload = read_json_if_exists(row.get("full_backtest_calendar_curve_path_36m")) or {}
    points = ((payload.get("curve") or {}).get("points") or [])
    dates: list[str] = []
    daily: list[float] = []
    open_counts: list[int] = []
    closed_counts: list[int] = []
    previous_equity = 0.0
    for point in points:
        date_text = str(point.get("date") or "").strip()
        if not date_text:
            continue
        equity = safe_float(point.get("equity_r"), previous_equity) or 0.0
        dates.append(date_text)
        daily.append(equity - previous_equity)
        previous_equity = equity
        open_counts.append(int(safe_float(point.get("open_trade_count"), 0.0) or 0))
        closed_counts.append(int(safe_float(point.get("closed_trade_count"), 0.0) or 0))
    return dates, daily, open_counts, closed_counts



def authoritative_candidate_score(row: dict[str, Any]) -> float:
    """Return the current 36m score used for optimizer eligibility and ranking.

    Discovery-era scores such as final_scrutiny_score/composite_score can be stale
    after a forced 36m rebuild. Once a row has valid 36m full-backtest status,
    the current 36m score is authoritative and stale discovery scores must not
    override it.
    """
    score = safe_float(row.get("score_36m"))
    if score is not None:
        return score
    lab_score = safe_float(row.get("score_lab_score_36m"))
    if lab_score is not None:
        return lab_score
    return 0.0

def candidate_rejection_reason(
    row: dict[str, Any],
    spec: PortfolioOptimizerSpec,
) -> str | None:
    if str(row.get("full_backtest_validation_status_36m") or "") != "valid":
        reason_codes = list(
            row.get("full_backtest_validation_reason_codes_36m") or []
        )
        if reason_codes:
            return f"full_backtest_{str(reason_codes[0])}"
        return "invalid_or_missing_full_backtest"
    if not Path(str(row.get("full_backtest_calendar_curve_path_36m") or "")).exists():
        return "missing_source_calendar_curve"
    instruments = row_instruments(row)
    asset_classes = {instrument_asset_class(item) for item in instruments} or {"other"}
    allowed = set(spec.allowed_asset_classes)
    if not asset_classes <= allowed:
        return "blocked_asset_class"
    normalized_instruments = {str(item).strip().upper() for item in instruments if str(item).strip()}
    blocked_instruments = {
        str(item).strip().upper()
        for item in spec.blocked_instruments
        if str(item).strip()
    }
    if blocked_instruments and normalized_instruments & blocked_instruments:
        return "blocked_instrument"
    allowed_instruments = {
        str(item).strip().upper()
        for item in spec.allowed_instruments
        if str(item).strip()
    }
    if allowed_instruments and not normalized_instruments <= allowed_instruments:
        return "unsupported_instrument"
    account_rejection = _candidate_account_risk_rejection(row, spec.account)
    if account_rejection:
        return account_rejection
    score = authoritative_candidate_score(row)
    if score < spec.min_score:
        return "score_below_min"
    holds = _hold_metrics(row)
    avg_hold = holds["avg_hold"]
    p90_hold = holds["p90_hold"]
    max_hold = holds["max_hold"]
    if avg_hold is None:
        return "missing_hold_metrics"
    if avg_hold > spec.max_avg_hold_hours:
        return "avg_hold_too_long"
    if p90_hold is not None and p90_hold > spec.max_p90_hold_hours:
        return "p90_hold_too_long"
    if max_hold is not None and max_hold > spec.max_single_hold_hours:
        return "single_hold_too_long"
    dates, daily, _, _ = _calendar_daily_curve(row)
    if not dates:
        return "empty_calendar_curve"
    if spec.require_positive_source_return and sum(daily) <= 0.0:
        return "nonpositive_source_calendar_return"
    return None


def build_optimizer_candidates(
    rows: list[dict[str, Any]],
    spec: PortfolioOptimizerSpec,
) -> tuple[list[OptimizerCandidate], dict[str, int]]:
    candidates: list[OptimizerCandidate] = []
    rejections: Counter[str] = Counter()
    for row in rows:
        attempt_id = str(row.get("attempt_id") or "").strip()
        if not attempt_id:
            rejections["missing_attempt_id"] += 1
            continue
        reason = candidate_rejection_reason(row, spec)
        if reason:
            rejections[reason] += 1
            continue
        instruments = row_instruments(row)
        asset_classes = {instrument_asset_class(item) for item in instruments} or {"other"}
        score = authoritative_candidate_score(row)
        holds = _hold_metrics(row)
        dates, daily, open_counts, closed_counts = _calendar_daily_curve(row)
        if len(asset_classes) == 1:
            primary_asset_class = sorted(asset_classes)[0]
        elif "metal" in asset_classes:
            primary_asset_class = "metal"
        elif "index" in asset_classes:
            primary_asset_class = "index"
        else:
            primary_asset_class = "fx"
        identity = derive_strategy_identity(row)
        candidates.append(
            # Family identity is deliberately resolved before candidate construction so
            # optimizer caps and research reporting consume the same durable token.
            OptimizerCandidate(
                attempt_id=attempt_id,
                row=row,
                instruments=instruments,
                asset_classes=asset_classes,
                primary_asset_class=primary_asset_class,
                family=str(identity["structural_family_id"]),
                family_source=str(identity["structural_family_source"]),
                lineage_id=(
                    str(identity["lineage_id"]) if identity.get("lineage_id") else None
                ),
                behavior_fingerprint=(
                    str(identity["behavior_fingerprint"])
                    if identity.get("behavior_fingerprint")
                    else None
                ),
                structural_family_signature=(
                    dict(identity["structural_family_signature"])
                    if isinstance(identity.get("structural_family_signature"), dict)
                    else None
                ),
                score=score,
                created_at=str(row.get("created_at") or "") or None,
                avg_hold_hours=float(holds["avg_hold"] or 0.0),
                p90_hold_hours=holds["p90_hold"],
                max_hold_hours=holds["max_hold"],
                path_quality=holds["path_quality"],
                stop_loss_percent=holds["stop_loss"],
                trade_count=int(
                    safe_float(row.get("trade_count_36m"), sum(closed_counts)) or 0
                ),
                trades_per_month=safe_float(row.get("trades_per_month_36m"), 0.0)
                or 0.0,
                dates=dates,
                daily_r=daily,
                open_counts=open_counts,
                closed_counts=closed_counts,
            )
        )
    candidates.sort(key=lambda item: (item.score, item.final_r), reverse=True)
    if spec.candidate_limit > 0:
        protected_ids = set(spec.baseline_attempt_ids) | set(spec.required_attempt_ids)
        protected_candidates = [
            candidate for candidate in candidates if candidate.attempt_id in protected_ids
        ]
        protected_seen = {candidate.attempt_id for candidate in protected_candidates}
        limited_candidates = [
            candidate
            for candidate in candidates
            if candidate.attempt_id not in protected_seen
        ][: spec.candidate_limit]
        candidates = [*protected_candidates, *limited_candidates]
    return candidates, dict(rejections)


class PortfolioSearch:
    def __init__(
        self,
        candidates: list[OptimizerCandidate],
        spec: PortfolioOptimizerSpec,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.spec = spec
        self.candidates = candidates
        self.progress_callback = progress_callback
        self.by_id = {candidate.attempt_id: candidate for candidate in candidates}
        self._metrics_cache: dict[tuple[tuple[str, ...], bool], dict[str, Any]] = {}
        self._score_cache: dict[tuple[str, tuple[str, ...]], float] = {}
        self._archive: dict[tuple[str, ...], dict[str, Any]] = {}
        self._pair_corr_cache: dict[tuple[str, str], float] = {}
        self._positive_corr_cache: dict[tuple[str, ...], float] = {}
        self._sharpe_cache: dict[tuple[str, ...], float] = {}
        self._sum_stats_cache: dict[tuple[str, ...], tuple[list[float], float, float]] = {}
        self._candidate_sum: dict[str, float] = {}
        self._candidate_sumsq: dict[str, float] = {}
        self.dates = sorted({date for candidate in candidates for date in candidate.dates})
        self.date_index = {date: index for index, date in enumerate(self.dates)}
        month_keys = list(dict.fromkeys(date[:7] for date in self.dates))
        week_keys = list(
            dict.fromkeys(
                f"{parsed.isocalendar().year}-W{parsed.isocalendar().week:02d}"
                for parsed in (datetime.fromisoformat(date) for date in self.dates)
            )
        )
        month_lookup = {key: index for index, key in enumerate(month_keys)}
        week_lookup = {key: index for index, key in enumerate(week_keys)}
        self.month_index = [month_lookup[date[:7]] for date in self.dates]
        self.week_index = []
        for date in self.dates:
            iso = datetime.fromisoformat(date).isocalendar()
            self.week_index.append(week_lookup[f"{iso.year}-W{iso.week:02d}"])
        self.month_count = len(month_keys)
        self.week_count = len(week_keys)
        for candidate in self.candidates:
            vector = [0.0] * len(self.dates)
            open_vector = [0] * len(self.dates)
            closed_vector = [0] * len(self.dates)
            for date, value, open_count, closed_count in zip(
                candidate.dates,
                candidate.daily_r,
                candidate.open_counts,
                candidate.closed_counts,
            ):
                index = self.date_index[date]
                vector[index] += value
                open_vector[index] += open_count
                closed_vector[index] += closed_count
            candidate.vector = vector
            candidate.open_vector = open_vector
            candidate.closed_vector = closed_vector
            candidate.month_vector = self._group_sums(vector, self.month_index, self.month_count)
            candidate.week_vector = self._group_sums(vector, self.week_index, self.week_count)
            self._candidate_sum[candidate.attempt_id] = sum(vector)
            self._candidate_sumsq[candidate.attempt_id] = sum(
                value * value for value in vector
            )

    def progress(self, event: str, **fields: Any) -> None:
        if self.progress_callback is None:
            return
        payload = {
            "event": event,
            "candidate_count": len(self.candidates),
            "archive_size": len(self._archive),
            "metrics_cache_size": len(self._metrics_cache),
            "score_cache_size": len(self._score_cache),
            **fields,
        }
        self.progress_callback(payload)

    def ids_for_known_attempts(self, attempt_ids: list[str] | tuple[str, ...]) -> list[str]:
        return [attempt_id for attempt_id in attempt_ids if attempt_id in self.by_id]

    def cache_key(self, selected_ids: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(attempt_id for attempt_id in selected_ids if attempt_id in self.by_id))

    def combine_vectors(self, selected_ids: list[str]) -> tuple[list[float], list[int], list[int]]:
        daily = [0.0] * len(self.dates)
        open_counts = [0] * len(self.dates)
        closed_counts = [0] * len(self.dates)
        for attempt_id in selected_ids:
            candidate = self.by_id[attempt_id]
            for index, value in enumerate(candidate.vector):
                daily[index] += value
                open_counts[index] += candidate.open_vector[index]
                closed_counts[index] += candidate.closed_vector[index]
        return daily, open_counts, closed_counts

    @staticmethod
    def _group_sums(values: list[float], indexes: list[int], size: int) -> list[float]:
        grouped = [0.0] * size
        for index, value in zip(indexes, values):
            grouped[index] += value
        return grouped

    def _constraint_violation_size_from_stats(
        self,
        selected_ids: list[str],
        *,
        avg_open_positions: float,
        peak_open_positions: float,
        trades_per_month: float,
    ) -> float:
        instrument_counts, asset_counts, family_counts = self.exposure_counts(selected_ids)
        violation_size = float(abs(len(selected_ids) - self.spec.portfolio_size))
        violation_size += float(max(0, len(selected_ids) - len(set(selected_ids))))
        violation_size += max(
            0.0,
            float(max(instrument_counts.values() or [0.0])) - self.spec.max_instrument_share,
        )
        violation_size += max(
            0.0,
            float(max(family_counts.values() or [0])) - self.spec.max_per_family,
        )
        violation_size += max(0.0, self.spec.min_fx_share - asset_counts.get("fx", 0.0))
        violation_size += max(0.0, asset_counts.get("metal", 0.0) - self.spec.max_metal_share)
        violation_size += max(0.0, asset_counts.get("index", 0.0) - self.spec.max_index_share)
        if self.spec.max_avg_open_positions > 0:
            violation_size += max(
                0.0, avg_open_positions - self.spec.max_avg_open_positions
            )
        if self.spec.max_peak_open_positions > 0:
            violation_size += max(
                0.0, peak_open_positions - self.spec.max_peak_open_positions
            )
        if self.spec.max_trades_per_month > 0:
            violation_size += max(
                0.0, trades_per_month - self.spec.max_trades_per_month
            )
        return violation_size

    def _avg_positive_pair_corr_uncached(self, selected_ids: list[str]) -> float:
        ids = list(
            dict.fromkeys(
                attempt_id for attempt_id in selected_ids if attempt_id in self.by_id
            )
        )
        total = 0.0
        pairs = 0
        for left_index, left_id in enumerate(ids):
            for right_id in ids[left_index + 1 :]:
                total += max(0.0, self.pair_corr(left_id, right_id))
                pairs += 1
        return total / pairs if pairs else 0.0

    def _objective_score_extension(
        self,
        selected_ids: list[str],
        added_id: str,
        objective_name: str,
        *,
        base_daily: list[float],
        base_open: list[int],
        base_closed: list[int],
        base_months: list[float],
        base_weeks: list[float],
    ) -> float:
        """Score one transient base-plus-candidate trial without retaining it."""
        candidate = self.by_id[added_id]
        trial_ids = [*selected_ids, added_id]
        final_r = 0.0
        sumsq = 0.0
        equity = 0.0
        peak = 0.0
        maxdd_r = 0.0
        positive_day_gain = 0.0
        best_day_r = float("-inf")
        worst_day_r = float("inf")
        current_loss_streak = 0
        max_loss_streak = 0
        open_total = 0
        peak_open_positions = 0
        total_closed_trades = 0
        for index, base_value in enumerate(base_daily):
            value = base_value + candidate.vector[index]
            final_r += value
            sumsq += value * value
            equity += value
            peak = max(peak, equity)
            maxdd_r = max(maxdd_r, peak - equity)
            positive_day_gain += max(0.0, value)
            best_day_r = max(best_day_r, value)
            worst_day_r = min(worst_day_r, value)
            if value < -1e-9:
                current_loss_streak += 1
                max_loss_streak = max(max_loss_streak, current_loss_streak)
            else:
                current_loss_streak = 0
            open_count = base_open[index] + candidate.open_vector[index]
            open_total += open_count
            peak_open_positions = max(peak_open_positions, open_count)
            total_closed_trades += base_closed[index] + candidate.closed_vector[index]
        if not base_daily:
            best_day_r = 0.0
            worst_day_r = 0.0

        months = [
            value + candidate.month_vector[index]
            for index, value in enumerate(base_months)
        ]
        weeks = [
            value + candidate.week_vector[index]
            for index, value in enumerate(base_weeks)
        ]
        pos_months, neg_months, _ = count_positive_negative_flat(months)
        _, neg_weeks, _ = count_positive_negative_flat(weeks)
        worst_month_r = min(months) if months else 0.0
        worst_week_r = min(weeks) if weeks else 0.0
        avg_open_positions = open_total / len(base_open) if base_open else 0.0
        trades_per_month = (
            total_closed_trades / self.month_count if self.month_count else 0.0
        )
        mean_avg_hold_hours = statistics.mean(
            self.by_id[attempt_id].avg_hold_hours for attempt_id in trial_ids
        ) if trial_ids else 0.0
        violation_size = self._constraint_violation_size_from_stats(
            trial_ids,
            avg_open_positions=avg_open_positions,
            peak_open_positions=float(peak_open_positions),
            trades_per_month=trades_per_month,
        )
        top_day_gain_share = (
            best_day_r / positive_day_gain if positive_day_gain > 0.0 else 1.0
        )

        weights = objective_weights_for_spec(self.spec)[objective_name]
        score = 0.0
        score += weights.get("final_r", 0.0) * final_r
        score += weights.get("maxdd_r", 0.0) * maxdd_r
        score += weights.get("positive_month", 0.0) * pos_months
        score += weights.get("negative_month", 0.0) * neg_months
        score += weights.get("negative_week", 0.0) * neg_weeks
        score += weights.get("worst_month_abs", 0.0) * abs(min(0.0, worst_month_r))
        score += weights.get("worst_week_abs", 0.0) * abs(min(0.0, worst_week_r))
        score += weights.get("worst_day_abs", 0.0) * abs(min(0.0, worst_day_r))
        score += weights.get("top_day_share", 0.0) * top_day_gain_share
        score += weights.get("loss_streak", 0.0) * max_loss_streak
        score += weights.get("hold_over_24h", 0.0) * max(
            0.0, mean_avg_hold_hours - 24.0
        )
        score += weights.get("avg_open_position", 0.0) * avg_open_positions
        score += weights.get("peak_open_position", 0.0) * peak_open_positions
        score += weights.get("avg_open_over_target", 0.0) * max(
            0.0, avg_open_positions - self.spec.max_avg_open_positions
        )
        score += weights.get("peak_open_over_target", 0.0) * max(
            0.0, peak_open_positions - self.spec.max_peak_open_positions
        )
        score += weights.get("trade_over_target_pm", 0.0) * max(
            0.0, trades_per_month - self.spec.target_trades_per_month
        )
        score += weights.get("constraint_violation", 0.0) * violation_size
        if self.spec.correlation_penalty_weight > 0.0:
            score -= self.spec.correlation_penalty_weight * self._avg_positive_pair_corr_uncached(
                trial_ids
            )
        if (
            str(self.spec.diversification_mode or "penalty").lower() == "marginal_sharpe"
            and self.spec.portfolio_sharpe_weight > 0.0
            and len(base_daily) >= 2
        ):
            mean = final_r / len(base_daily)
            variance = max(0.0, (sumsq / len(base_daily)) - (mean * mean))
            std = math.sqrt(variance)
            sharpe = mean / std if std > 1e-12 else 0.0
            score += self.spec.portfolio_sharpe_weight * sharpe
        return score

    def pair_corr(self, left_id: str, right_id: str) -> float:
        """Pairwise daily-return correlation, computed at most once per pair per run."""
        if left_id == right_id:
            return 1.0
        key = (left_id, right_id) if left_id < right_id else (right_id, left_id)
        cached = self._pair_corr_cache.get(key)
        if cached is None:
            cached = pearson_corr(self.by_id[key[0]].vector, self.by_id[key[1]].vector)
            self._pair_corr_cache[key] = cached
        return cached

    def avg_positive_pair_corr(self, selected_ids: list[str] | tuple[str, ...]) -> float:
        """Mean of max(0, corr) across all selected pairs from the precomputed pair cache."""
        ids = list(dict.fromkeys(
            attempt_id for attempt_id in selected_ids if attempt_id in self.by_id
        ))
        key = tuple(sorted(ids))
        cached = self._positive_corr_cache.get(key)
        if cached is not None:
            return cached
        total = 0.0
        pairs = 0
        for left_index, left_id in enumerate(ids):
            for right_id in ids[left_index + 1 :]:
                total += max(0.0, self.pair_corr(left_id, right_id))
                pairs += 1
        value = total / pairs if pairs else 0.0
        self._positive_corr_cache[key] = value
        return value

    def _selection_sum_stats(self, key: tuple[str, ...]) -> tuple[float, float]:
        """Return (total, sum of squares) of the summed daily-return vector for `key`.

        Base selections (the current greedy/swap working set) are cached with their
        full summed vector; candidate trials that extend a cached base by one member
        are evaluated incrementally in O(days) without materializing a new vector.
        """
        cached = self._sum_stats_cache.get(key)
        if cached is not None:
            return cached[1], cached[2]
        for index in range(len(key)):
            base_key = key[:index] + key[index + 1 :]
            base = self._sum_stats_cache.get(base_key)
            if base is None:
                continue
            base_vector, base_total, base_sumsq = base
            added = self.by_id[key[index]].vector
            cross = 0.0
            for base_value, added_value in zip(base_vector, added):
                cross += base_value * added_value
            total = base_total + self._candidate_sum[key[index]]
            sumsq = base_sumsq + 2.0 * cross + self._candidate_sumsq[key[index]]
            return total, sumsq
        vector = [0.0] * len(self.dates)
        for attempt_id in key:
            for index, value in enumerate(self.by_id[attempt_id].vector):
                vector[index] += value
        total = sum(vector)
        sumsq = sum(value * value for value in vector)
        self._sum_stats_cache[key] = (vector, total, sumsq)
        return total, sumsq

    def portfolio_sharpe(self, selected_ids: list[str] | tuple[str, ...]) -> float:
        """Daily portfolio Sharpe: mean(daily R) / population std(daily R)."""
        ids = list(dict.fromkeys(
            attempt_id for attempt_id in selected_ids if attempt_id in self.by_id
        ))
        key = tuple(sorted(ids))
        cached = self._sharpe_cache.get(key)
        if cached is not None:
            return cached
        day_count = len(self.dates)
        if not key or day_count < 2:
            value = 0.0
        else:
            total, sumsq = self._selection_sum_stats(key)
            mean = total / day_count
            variance = max(0.0, (sumsq / day_count) - (mean * mean))
            std = math.sqrt(variance)
            value = mean / std if std > 1e-12 else 0.0
        self._sharpe_cache[key] = value
        return value

    def _diversification_adjustment(self, selected_ids: list[str]) -> float:
        adjustment = 0.0
        if self.spec.correlation_penalty_weight > 0.0:
            adjustment -= self.spec.correlation_penalty_weight * self.avg_positive_pair_corr(
                selected_ids
            )
        if (
            str(self.spec.diversification_mode or "penalty").lower() == "marginal_sharpe"
            and self.spec.portfolio_sharpe_weight > 0.0
        ):
            adjustment += self.spec.portfolio_sharpe_weight * self.portfolio_sharpe(
                selected_ids
            )
        return adjustment

    def exposure_counts(self, selected_ids: list[str]) -> tuple[Counter[str], Counter[str], Counter[str]]:
        instrument_counts: Counter[str] = Counter()
        asset_counts: Counter[str] = Counter()
        family_counts: Counter[str] = Counter()
        for attempt_id in selected_ids:
            candidate = self.by_id[attempt_id]
            family_counts[candidate.family] += 1
            share = 1.0 / max(1, len(candidate.instruments))
            for instrument in candidate.instruments:
                instrument_counts[instrument] += share
                asset_counts[instrument_asset_class(instrument)] += share
        return instrument_counts, asset_counts, family_counts

    def account_simulation(
        self,
        selected_ids: list[str],
        *,
        risk_basis: str = "initial",
        risk_pct: float | None = None,
    ) -> dict[str, Any]:
        account = self.spec.account or {}
        starting_balance = _account_value(
            account,
            "balance_usd",
            "account_size_usd",
            "balance",
            "account_balance",
        )
        configured_risk_pct = (
            risk_pct
            if risk_pct is not None
            else _account_value(
                account,
                "risk_per_trade_pct",
                "risk_per_trade_percent",
                "risk_pct",
            )
        )
        if (
            starting_balance is None
            or starting_balance <= 0
            or configured_risk_pct is None
            or configured_risk_pct <= 0
        ):
            return {}
        leverage = _account_value(account, "leverage", default=1.0) or 1.0
        min_lot = _account_value(account, "min_lot", "minLot", default=0.0) or 0.0
        lot_step = _account_value(account, "lot_step", "lotStep", default=0.0001) or 0.0001
        notional_per_lot = (
            _account_value(
                account,
                "notional_usd_per_lot",
                "notionalUsdPerLot",
                default=100000.0,
            )
            or 100000.0
        )
        margin_call_level_pct = (
            _account_value(
                account,
                "margin_call_level_pct",
                "marginCallLevelPercent",
                default=70.0,
            )
            or 70.0
        )
        stop_out_level_pct = (
            _account_value(
                account,
                "stop_out_level_pct",
                "stopOutLevelPercent",
                default=50.0,
            )
            or 50.0
        )
        cost_r_per_trade = sum(
            _account_value(account, key, default=0.0) or 0.0
            for key in (
                "commission_r_per_trade",
                "spread_r_per_trade",
                "slippage_r_per_trade",
            )
        )
        normalized_basis = str(risk_basis or account.get("dashboard_risk_basis") or "initial").lower()
        if normalized_basis not in {"initial", "current"}:
            normalized_basis = "initial"
        selected = [attempt_id for attempt_id in selected_ids if attempt_id in self.by_id]
        balance = float(starting_balance)
        realized = 0.0
        peak_balance = balance
        min_balance = balance
        max_drawdown_usd = 0.0
        max_used_margin_usd = 0.0
        min_margin_level_pct = float("inf")
        max_margin_risk_pct = 0.0
        min_lot_forced_trades = 0
        total_closed_trades = 0
        risk_variance_weighted = 0.0
        risk_variance_weight = 0
        max_actual_risk_pct = 0.0
        max_actual_risk_multiple = 0.0
        margin_liquidated = False
        first_liquidation_date: str | None = None
        risk_fraction = float(configured_risk_pct) / 100.0

        for index, date_text in enumerate(self.dates):
            target_risk = (
                max(0.0, balance) * risk_fraction
                if normalized_basis == "current"
                else float(starting_balance) * risk_fraction
            )
            balance_delta = 0.0
            realized_delta = 0.0
            used_margin = 0.0
            open_trades = 0
            closed_trades = 0
            for attempt_id in selected:
                candidate = self.by_id[attempt_id]
                daily_r = candidate.vector[index] if index < len(candidate.vector) else 0.0
                open_count = candidate.open_vector[index] if index < len(candidate.open_vector) else 0
                closed_count = candidate.closed_vector[index] if index < len(candidate.closed_vector) else 0
                stop_loss_percent = candidate.stop_loss_percent
                if (
                    target_risk <= 0
                    or stop_loss_percent is None
                    or stop_loss_percent <= 0
                    or notional_per_lot <= 0
                ):
                    sized_risk = target_risk
                    lots = 0.0
                    forced_min_lot = False
                else:
                    risk_per_lot = notional_per_lot * (stop_loss_percent / 100.0)
                    raw_lots = target_risk / risk_per_lot if risk_per_lot > 0 else 0.0
                    rounded_lots = (
                        math.floor((raw_lots / lot_step) + 1e-9) * lot_step
                        if lot_step > 0
                        else raw_lots
                    )
                    forced_min_lot = min_lot > 0 and rounded_lots < min_lot
                    lots = min_lot if forced_min_lot else max(0.0, rounded_lots)
                    sized_risk = lots * risk_per_lot
                    if closed_count > 0 and target_risk > 0:
                        actual_risk_pct = (sized_risk / max(0.000001, balance)) * 100.0
                        actual_multiple = sized_risk / max(0.000001, target_risk)
                        variance = actual_multiple - 1.0
                        risk_variance_weighted += variance * closed_count
                        risk_variance_weight += closed_count
                        max_actual_risk_pct = max(max_actual_risk_pct, actual_risk_pct)
                        max_actual_risk_multiple = max(max_actual_risk_multiple, actual_multiple)
                    if forced_min_lot and closed_count > 0:
                        min_lot_forced_trades += closed_count
                used_margin += open_count * (lots * notional_per_lot / max(1.0, leverage))
                open_trades += open_count
                closed_trades += closed_count
                if not margin_liquidated:
                    net_daily_r = daily_r - (closed_count * cost_r_per_trade)
                    balance_delta += net_daily_r * sized_risk
                    realized_delta += net_daily_r * sized_risk
            if not margin_liquidated:
                balance = round(balance + balance_delta, 2)
                realized = round(realized + realized_delta, 2)
            total_closed_trades += closed_trades
            stop_out_equity = used_margin * (stop_out_level_pct / 100.0)
            margin_level_pct = (balance / used_margin * 100.0) if used_margin > 0 else None
            margin_risk_pct = (
                min(100.0, max(0.0, stop_out_level_pct / max(margin_level_pct, 0.000001) * 100.0))
                if margin_level_pct is not None and stop_out_level_pct > 0
                else 0.0
            )
            if not margin_liquidated and used_margin > 0 and balance <= stop_out_equity:
                margin_liquidated = True
                first_liquidation_date = date_text
            peak_balance = max(peak_balance, balance)
            min_balance = min(min_balance, balance)
            max_drawdown_usd = max(max_drawdown_usd, peak_balance - balance)
            max_used_margin_usd = max(max_used_margin_usd, used_margin)
            if margin_level_pct is not None and math.isfinite(margin_level_pct):
                min_margin_level_pct = min(min_margin_level_pct, margin_level_pct)
            max_margin_risk_pct = max(max_margin_risk_pct, margin_risk_pct)
        final_return_pct = ((balance - starting_balance) / starting_balance) * 100.0
        return {
            "risk_basis": normalized_basis,
            "risk_pct": float(configured_risk_pct),
            "starting_balance": round(float(starting_balance), 2),
            "final_balance": round(balance, 2),
            "final_realized_usd": round(realized, 2),
            "final_return_pct": round(final_return_pct, 6),
            "max_drawdown_usd": round(max_drawdown_usd, 2),
            "max_drawdown_pct": round((max_drawdown_usd / starting_balance) * 100.0, 6),
            "min_balance": round(min_balance, 2),
            "blown": bool(margin_liquidated or balance <= 0),
            "margin_liquidated": bool(margin_liquidated),
            "first_liquidation_date": first_liquidation_date,
            "max_used_margin_usd": round(max_used_margin_usd, 2),
            "min_margin_level_pct": (
                round(min_margin_level_pct, 6)
                if math.isfinite(min_margin_level_pct)
                else None
            ),
            "max_margin_risk_pct": round(max_margin_risk_pct, 6),
            "margin_call_level_pct": float(margin_call_level_pct),
            "stop_out_level_pct": float(stop_out_level_pct),
            "min_lot_forced_trades": int(min_lot_forced_trades),
            "min_lot_forced_trade_pct": (
                round((min_lot_forced_trades / total_closed_trades) * 100.0, 6)
                if total_closed_trades > 0
                else 0.0
            ),
            "total_closed_trades": int(total_closed_trades),
            "avg_actual_risk_variance_pct": (
                round((risk_variance_weighted / risk_variance_weight) * 100.0, 6)
                if risk_variance_weight > 0
                else 0.0
            ),
            "max_actual_risk_pct": round(max_actual_risk_pct, 6),
            "max_actual_risk_multiple": round(max_actual_risk_multiple, 6),
        }

    def constraint_violations(self, selected_ids: list[str]) -> dict[str, float]:
        instrument_counts, asset_counts, family_counts = self.exposure_counts(selected_ids)
        daily, open_counts, closed_counts = self.combine_vectors(selected_ids)
        month_count = len(group_values(self.dates, daily, "month"))
        trades_per_month = (
            sum(closed_counts) / month_count
            if month_count > 0
            else 0.0
        )
        avg_open_positions = (
            sum(open_counts) / len(open_counts)
            if open_counts
            else 0.0
        )
        peak_open_positions = max(open_counts) if open_counts else 0.0
        violations: dict[str, float] = {}
        if len(selected_ids) != self.spec.portfolio_size:
            violations["portfolio_size"] = abs(len(selected_ids) - self.spec.portfolio_size)
        duplicate_count = len(selected_ids) - len(set(selected_ids))
        if duplicate_count > 0:
            violations["duplicate_attempts"] = float(duplicate_count)
        max_instrument = max(instrument_counts.values() or [0.0])
        if max_instrument > self.spec.max_instrument_share:
            violations["instrument_share"] = max_instrument - self.spec.max_instrument_share
        max_family = max(family_counts.values() or [0])
        if max_family > self.spec.max_per_family:
            violations["family_cap"] = float(max_family - self.spec.max_per_family)
        fx_share = asset_counts.get("fx", 0.0)
        if fx_share < self.spec.min_fx_share:
            violations["min_fx_share"] = self.spec.min_fx_share - fx_share
        metal_share = asset_counts.get("metal", 0.0)
        if metal_share > self.spec.max_metal_share:
            violations["max_metal_share"] = metal_share - self.spec.max_metal_share
        index_share = asset_counts.get("index", 0.0)
        if index_share > self.spec.max_index_share:
            violations["max_index_share"] = index_share - self.spec.max_index_share
        if (
            self.spec.max_avg_open_positions > 0
            and avg_open_positions > self.spec.max_avg_open_positions
        ):
            violations["max_avg_open_positions"] = (
                avg_open_positions - self.spec.max_avg_open_positions
            )
        if (
            self.spec.max_peak_open_positions > 0
            and peak_open_positions > self.spec.max_peak_open_positions
        ):
            violations["max_peak_open_positions"] = (
                peak_open_positions - self.spec.max_peak_open_positions
            )
        if (
            self.spec.max_trades_per_month > 0
            and trades_per_month > self.spec.max_trades_per_month
        ):
            violations["max_trades_per_month"] = (
                trades_per_month - self.spec.max_trades_per_month
            )
        return violations

    def metrics(
        self,
        selected_ids: list[str],
        *,
        include_correlation: bool = False,
        include_account: bool = True,
    ) -> dict[str, Any]:
        selected_ids = [attempt_id for attempt_id in selected_ids if attempt_id in self.by_id]
        cache_key = (
            self.cache_key(selected_ids),
            bool(include_correlation),
            bool(include_account),
        )
        cached = self._metrics_cache.get(cache_key)
        if cached is not None:
            return cached
        daily, open_counts, closed_counts = self.combine_vectors(selected_ids)
        months = group_values(self.dates, daily, "month")
        weeks = group_values(self.dates, daily, "week")
        days = dict(zip(self.dates, daily))
        pos_months, neg_months, flat_months = count_positive_negative_flat(
            list(months.values())
        )
        pos_weeks, neg_weeks, flat_weeks = count_positive_negative_flat(
            list(weeks.values())
        )
        pos_days, neg_days, flat_days = count_positive_negative_flat(list(days.values()))
        best_day = max(days.items(), key=lambda item: item[1]) if days else ("", 0.0)
        worst_day = min(days.items(), key=lambda item: item[1]) if days else ("", 0.0)
        best_week = max(weeks.items(), key=lambda item: item[1]) if weeks else ("", 0.0)
        worst_week = min(weeks.items(), key=lambda item: item[1]) if weeks else ("", 0.0)
        best_month = max(months.items(), key=lambda item: item[1]) if months else ("", 0.0)
        worst_month = min(months.items(), key=lambda item: item[1]) if months else ("", 0.0)
        final_r = sum(daily)
        positive_day_gain = sum(max(0.0, value) for value in daily)
        max_loss_streak, avg_loss_streak = loss_streak(daily)
        instrument_counts, asset_counts, family_counts = self.exposure_counts(selected_ids)
        average_holds = [self.by_id[item].avg_hold_hours for item in selected_ids]
        p90_holds = [
            self.by_id[item].p90_hold_hours
            for item in selected_ids
            if self.by_id[item].p90_hold_hours is not None
        ]
        max_holds = [
            self.by_id[item].max_hold_hours
            for item in selected_ids
            if self.by_id[item].max_hold_hours is not None
        ]
        result: dict[str, Any] = {
            "count": len(selected_ids),
            "final_r": final_r,
            "maxdd_r": max_drawdown(daily),
            "return_to_dd": final_r / max_drawdown(daily) if max_drawdown(daily) else None,
            "month_count": len(months),
            "week_count": len(weeks),
            "pos_months": pos_months,
            "neg_months": neg_months,
            "flat_months": flat_months,
            "worst_month": worst_month[0],
            "worst_month_r": worst_month[1],
            "best_month": best_month[0],
            "best_month_r": best_month[1],
            "pos_weeks": pos_weeks,
            "neg_weeks": neg_weeks,
            "flat_weeks": flat_weeks,
            "worst_week": worst_week[0],
            "worst_week_r": worst_week[1],
            "best_week": best_week[0],
            "best_week_r": best_week[1],
            "pos_days": pos_days,
            "neg_days": neg_days,
            "flat_days": flat_days,
            "worst_day": worst_day[0],
            "worst_day_r": worst_day[1],
            "best_day": best_day[0],
            "best_day_r": best_day[1],
            "positive_day_gain_r": positive_day_gain,
            "top_day_gain_share": (
                best_day[1] / positive_day_gain
                if positive_day_gain > 0.0
                else 1.0
            ),
            "max_daily_loss_streak": max_loss_streak,
            "avg_daily_loss_streak": avg_loss_streak,
            "avg_open_positions": sum(open_counts) / len(open_counts)
            if open_counts
            else 0.0,
            "peak_open_positions": max(open_counts) if open_counts else 0,
            "total_closed_trades": sum(closed_counts),
            "trades_per_month": (
                sum(closed_counts) / len(months)
                if months
                else 0.0
            ),
            "instrument_counts": dict(sorted(instrument_counts.items())),
            "asset_class_counts": dict(sorted(asset_counts.items())),
            "family_counts": dict(family_counts),
            "mean_avg_hold_hours": statistics.mean(average_holds)
            if average_holds
            else 0.0,
            "max_avg_hold_hours": max(average_holds) if average_holds else 0.0,
            "max_p90_hold_hours": max(p90_holds) if p90_holds else 0.0,
            "max_single_trade_hold_hours": max(max_holds) if max_holds else 0.0,
            "constraint_violations": self.constraint_violations(selected_ids),
        }
        if include_account and self.spec.account:
            result["account_initial"] = self.account_simulation(
                selected_ids,
                risk_basis="initial",
            )
            result["account_current"] = self.account_simulation(
                selected_ids,
                risk_basis="current",
            )
        if include_correlation:
            correlations: list[float] = []
            unique_ids = list(dict.fromkeys(selected_ids))
            for left_index, left_id in enumerate(unique_ids):
                for right_id in unique_ids[left_index + 1 :]:
                    correlations.append(self.pair_corr(left_id, right_id))
            result["avg_pair_corr"] = statistics.mean(correlations) if correlations else 0.0
            result["max_pair_corr"] = max(correlations) if correlations else 0.0
            result["avg_positive_pair_corr"] = self.avg_positive_pair_corr(selected_ids)
            result["portfolio_sharpe"] = self.portfolio_sharpe(selected_ids)
        self._metrics_cache[cache_key] = result
        return result

    def objective_score(self, selected_ids: list[str], objective_name: str) -> float:
        score_key = (objective_name, self.cache_key(selected_ids))
        cached = self._score_cache.get(score_key)
        if cached is not None:
            return cached
        weights = objective_weights_for_spec(self.spec)[objective_name]
        metrics = self.metrics(selected_ids, include_account=False)
        violation_size = sum(float(value) for value in metrics["constraint_violations"].values())
        score = 0.0
        score += weights.get("final_r", 0.0) * float(metrics["final_r"])
        score += weights.get("maxdd_r", 0.0) * float(metrics["maxdd_r"])
        score += weights.get("positive_month", 0.0) * float(metrics["pos_months"])
        score += weights.get("negative_month", 0.0) * float(metrics["neg_months"])
        score += weights.get("negative_week", 0.0) * float(metrics["neg_weeks"])
        score += weights.get("worst_month_abs", 0.0) * abs(
            min(0.0, float(metrics["worst_month_r"]))
        )
        score += weights.get("worst_week_abs", 0.0) * abs(
            min(0.0, float(metrics["worst_week_r"]))
        )
        score += weights.get("worst_day_abs", 0.0) * abs(
            min(0.0, float(metrics["worst_day_r"]))
        )
        score += weights.get("top_day_share", 0.0) * float(metrics["top_day_gain_share"])
        score += weights.get("loss_streak", 0.0) * float(metrics["max_daily_loss_streak"])
        score += weights.get("hold_over_24h", 0.0) * max(
            0.0, float(metrics["mean_avg_hold_hours"]) - 24.0
        )
        score += weights.get("avg_open_position", 0.0) * float(
            metrics["avg_open_positions"]
        )
        score += weights.get("peak_open_position", 0.0) * float(
            metrics["peak_open_positions"]
        )
        score += weights.get("avg_open_over_target", 0.0) * max(
            0.0,
            float(metrics["avg_open_positions"]) - self.spec.max_avg_open_positions,
        )
        score += weights.get("peak_open_over_target", 0.0) * max(
            0.0,
            float(metrics["peak_open_positions"]) - self.spec.max_peak_open_positions,
        )
        score += weights.get("trade_over_target_pm", 0.0) * max(
            0.0,
            float(metrics["trades_per_month"]) - self.spec.target_trades_per_month,
        )
        score += weights.get("constraint_violation", 0.0) * violation_size
        score += self._diversification_adjustment(selected_ids)
        self._score_cache[score_key] = score
        return score

    def record_archive(
        self,
        selected_ids: list[str],
        *,
        objective_name: str,
        label: str,
        objective_score: float | None = None,
    ) -> None:
        ids = [attempt_id for attempt_id in selected_ids if attempt_id in self.by_id]
        key = self.cache_key(ids)
        if not key:
            return
        scores = {
            name: self.objective_score(ids, name)
            for name in DEFAULT_OBJECTIVES
        }
        row = {
            "archive_label": label,
            "objective_name": objective_name,
            "objective_score": (
                objective_score
                if objective_score is not None
                else scores.get(objective_name)
            ),
            "objective_scores": scores,
            "selected_attempt_ids": list(ids),
            "metrics": self.metrics(ids, include_correlation=False),
        }
        existing = self._archive.get(key)
        if existing is None:
            self._archive[key] = row
            return
        current_score = float(row.get("objective_score") or float("-inf"))
        existing_score = float(existing.get("objective_score") or float("-inf"))
        if current_score > existing_score:
            self._archive[key] = row

    def candidate_row(self, attempt_id: str, rank: int | None = None) -> dict[str, Any]:
        candidate = self.by_id[attempt_id]
        row = {
            "attempt_id": candidate.attempt_id,
            "candidate_name": candidate.row.get("candidate_name"),
            "run_id": candidate.row.get("run_id"),
            "created_at": candidate.created_at,
            "instruments": "|".join(candidate.instruments),
            "asset_class": candidate.primary_asset_class,
            "score": round(candidate.score, 6),
            "final_r": round(candidate.final_r, 6),
            "maxdd_r": round(candidate.maxdd_r, 6),
            "avg_holding_hours": round(candidate.avg_hold_hours, 6),
            "p90_holding_hours": (
                round(candidate.p90_hold_hours, 6)
                if candidate.p90_hold_hours is not None
                else None
            ),
            "max_holding_hours": (
                round(candidate.max_hold_hours, 6)
                if candidate.max_hold_hours is not None
                else None
            ),
            "trades_per_month": round(candidate.trades_per_month, 6),
            "trade_count": candidate.trade_count,
            "profile_ref": candidate.row.get("profile_ref"),
            "profile_path": candidate.row.get("profile_path"),
            "family": candidate.family,
        }
        if rank is not None:
            row = {"rank": rank, **row}
        return row

    def greedy_seed(self, objective_name: str) -> list[str]:
        self.progress("greedy_seed_start", objective=objective_name)
        selected = self.ids_for_known_attempts(self.spec.required_attempt_ids)
        pool = [candidate.attempt_id for candidate in self.candidates]
        while len(selected) < self.spec.portfolio_size:
            base_daily, base_open, base_closed = self.combine_vectors(selected)
            base_months = self._group_sums(
                base_daily, self.month_index, self.month_count
            )
            base_weeks = self._group_sums(base_daily, self.week_index, self.week_count)
            best_attempt_id: str | None = None
            best_score = float("-inf")
            for attempt_id in pool:
                if attempt_id in selected:
                    continue
                score = self._objective_score_extension(
                    selected,
                    attempt_id,
                    objective_name,
                    base_daily=base_daily,
                    base_open=base_open,
                    base_closed=base_closed,
                    base_months=base_months,
                    base_weeks=base_weeks,
                )
                if score > best_score:
                    best_score = score
                    best_attempt_id = attempt_id
            if best_attempt_id is None:
                break
            selected.append(best_attempt_id)
            self.progress(
                "greedy_seed_pick",
                objective=objective_name,
                selected_count=len(selected),
                selected_attempt_id=best_attempt_id,
                objective_score=best_score,
            )
        return selected

    def random_seed(self, rng: random.Random) -> list[str] | None:
        if len(self.candidates) < self.spec.portfolio_size:
            return None
        required = self.ids_for_known_attempts(self.spec.required_attempt_ids)
        if len(required) > self.spec.portfolio_size:
            raise ValueError("required_attempt_ids exceed portfolio_size")
        required_set = set(required)
        ids = [
            candidate.attempt_id
            for candidate in self.candidates
            if candidate.attempt_id not in required_set
        ]
        sample_size = self.spec.portfolio_size - len(required)
        best: list[str] | None = None
        best_violation = float("inf")
        for _ in range(500):
            sample = [*required, *rng.sample(ids, sample_size)]
            violations = self.constraint_violations(sample)
            violation_size = sum(float(value) for value in violations.values())
            if violation_size < best_violation:
                best = sample
                best_violation = violation_size
            if not violations:
                return sample
        return best

    def improve_by_swaps(
        self,
        seed_ids: list[str],
        objective_name: str,
        *,
        start_name: str = "",
    ) -> tuple[list[str], list[dict[str, Any]]]:
        required = self.ids_for_known_attempts(self.spec.required_attempt_ids)
        if len(required) > self.spec.portfolio_size:
            raise ValueError("required_attempt_ids exceed portfolio_size")
        selected = list(dict.fromkeys([*required, *seed_ids]))
        if len(selected) > self.spec.portfolio_size:
            selected = selected[: self.spec.portfolio_size]
        if len(selected) < self.spec.portfolio_size:
            for candidate in self.candidates:
                if candidate.attempt_id not in selected:
                    selected.append(candidate.attempt_id)
                if len(selected) >= self.spec.portfolio_size:
                    break
        best_score = self.objective_score(selected, objective_name)
        active_pool = (
            min(self.spec.swap_candidate_limit, len(self.candidates))
            if self.spec.swap_candidate_limit > 0
            else len(self.candidates)
        )
        self.progress(
            "swap_search_start",
            objective=objective_name,
            start=start_name,
            selected_count=len(selected),
            swap_candidate_count=active_pool,
            max_swaps=self.spec.max_swaps,
            objective_score=best_score,
        )
        self.record_archive(
            selected,
            objective_name=objective_name,
            label=f"{start_name or 'seed'}:seed",
            objective_score=best_score,
        )
        swaps: list[dict[str, Any]] = []
        if self.spec.swap_candidate_limit > 0:
            pool = [
                candidate.attempt_id
                for candidate in self.candidates[: self.spec.swap_candidate_limit]
            ]
            for attempt_id in selected:
                if attempt_id not in pool:
                    pool.append(attempt_id)
        else:
            pool = [candidate.attempt_id for candidate in self.candidates]
        for _ in range(max(0, self.spec.max_swaps)):
            best_move: tuple[float, str, str, list[str]] | None = None
            selected_set = set(selected)
            evaluated = 0
            iteration = len(swaps) + 1
            self.progress(
                "swap_iteration_start",
                objective=objective_name,
                start=start_name,
                iteration=iteration,
                selected_count=len(selected),
                pool_count=len(pool),
                objective_score=best_score,
            )
            for removed in selected:
                if removed in required:
                    continue
                base_ids = [attempt_id for attempt_id in selected if attempt_id != removed]
                base_daily, base_open, base_closed = self.combine_vectors(base_ids)
                base_months = self._group_sums(
                    base_daily, self.month_index, self.month_count
                )
                base_weeks = self._group_sums(
                    base_daily, self.week_index, self.week_count
                )
                for added in pool:
                    if added in selected_set:
                        continue
                    evaluated += 1
                    trial = [*base_ids, added]
                    score = self._objective_score_extension(
                        base_ids,
                        added,
                        objective_name,
                        base_daily=base_daily,
                        base_open=base_open,
                        base_closed=base_closed,
                        base_months=base_months,
                        base_weeks=base_weeks,
                    )
                    if score > best_score + 1e-9:
                        best_move = (score, removed, added, trial)
                        best_score = score
            if best_move is None:
                self.progress(
                    "swap_iteration_done",
                    objective=objective_name,
                    start=start_name,
                    iteration=iteration,
                    improved=False,
                    evaluated=evaluated,
                    objective_score=best_score,
                )
                break
            score, removed, added, trial = best_move
            score = self.objective_score(trial, objective_name)
            swaps.append(
                {
                    "removed": removed,
                    "added": added,
                    "objective_after": score,
                }
            )
            selected = trial
            self.record_archive(
                selected,
                objective_name=objective_name,
                label=f"{start_name or 'seed'}:swap_{len(swaps)}",
                objective_score=score,
            )
            self.progress(
                "swap_iteration_done",
                objective=objective_name,
                start=start_name,
                iteration=iteration,
                improved=True,
                evaluated=evaluated,
                removed=removed,
                added=added,
                objective_score=score,
            )
        return selected, swaps

    @staticmethod
    def _metric_value(metrics: dict[str, Any], key: str) -> float:
        value = safe_float(metrics.get(key))
        if value is None:
            return float("inf")
        return value

    @classmethod
    def dominates(cls, left: dict[str, Any], right: dict[str, Any]) -> bool:
        left_metrics = left.get("metrics") or {}
        right_metrics = right.get("metrics") or {}
        if left_metrics.get("constraint_violations") and not right_metrics.get(
            "constraint_violations"
        ):
            return False
        if right_metrics.get("constraint_violations") and not left_metrics.get(
            "constraint_violations"
        ):
            return True
        better = False
        epsilon = 1e-9
        for key, direction in PARETO_DIMENSIONS:
            left_value = cls._metric_value(left_metrics, key)
            right_value = cls._metric_value(right_metrics, key)
            if direction == "max":
                if left_value < right_value - epsilon:
                    return False
                if left_value > right_value + epsilon:
                    better = True
            else:
                if left_value > right_value + epsilon:
                    return False
                if left_value < right_value - epsilon:
                    better = True
        return better

    def pareto_front(self, *, limit: int = 50) -> list[dict[str, Any]]:
        archived = [
            item
            for item in self._archive.values()
            if int((item.get("metrics") or {}).get("count") or 0) == self.spec.portfolio_size
        ]
        front: list[dict[str, Any]] = []
        for item in archived:
            if any(self.dominates(other, item) for other in archived if other is not item):
                continue
            front.append(item)
        front.sort(
            key=lambda item: (
                -float((item.get("objective_scores") or {}).get("balanced") or float("-inf")),
                -float((item.get("metrics") or {}).get("final_r") or float("-inf")),
                float((item.get("metrics") or {}).get("maxdd_r") or float("inf")),
            )
        )
        return front[: max(1, int(limit))]

    def optimize(self) -> dict[str, Any]:
        self.progress(
            "optimize_start",
            objectives=list(self.spec.objective_names),
            portfolio_size=self.spec.portfolio_size,
            random_starts=self.spec.random_starts,
            max_swaps=self.spec.max_swaps,
        )
        rng = random.Random(self.spec.random_seed)
        variants: dict[str, dict[str, Any]] = {}
        baseline_seed = self.ids_for_known_attempts(self.spec.baseline_attempt_ids)
        for objective_name in self.spec.objective_names:
            if objective_name not in DEFAULT_OBJECTIVES:
                continue
            self.progress("objective_start", objective=objective_name)
            starts: list[tuple[str, list[str]]] = []
            if baseline_seed:
                starts.append(("baseline", baseline_seed))
            starts.append(("greedy", self.greedy_seed(objective_name)))
            for index in range(max(0, self.spec.random_starts)):
                seed = self.random_seed(rng)
                if seed:
                    starts.append((f"random_{index + 1}", seed))
            self.progress(
                "objective_starts_ready",
                objective=objective_name,
                start_count=len(starts),
                starts=[name for name, _ids in starts],
            )
            best_ids: list[str] = []
            best_score = float("-inf")
            best_swaps: list[dict[str, Any]] = []
            best_start = ""
            for start_name, start_ids in starts:
                self.progress(
                    "start_begin",
                    objective=objective_name,
                    start=start_name,
                    selected_count=len(start_ids),
                )
                selected, swaps = self.improve_by_swaps(
                    start_ids,
                    objective_name,
                    start_name=start_name,
                )
                score = self.objective_score(selected, objective_name)
                self.record_archive(
                    selected,
                    objective_name=objective_name,
                    label=f"{start_name}:final",
                    objective_score=score,
                )
                if score > best_score:
                    best_score = score
                    best_ids = selected
                    best_swaps = swaps
                    best_start = start_name
                self.progress(
                    "start_done",
                    objective=objective_name,
                    start=start_name,
                    swap_count=len(swaps),
                    objective_score=score,
                    best_objective_score=best_score,
                )
            best_avg_positive_corr = self.avg_positive_pair_corr(best_ids)
            best_portfolio_sharpe = self.portfolio_sharpe(best_ids)
            diversification_mode = str(self.spec.diversification_mode or "penalty").lower()
            variants[objective_name] = {
                "objective_name": objective_name,
                "objective_score": best_score,
                "start": best_start,
                "selected_attempt_ids": best_ids,
                "swaps": best_swaps,
                "diversification": {
                    "mode": diversification_mode,
                    "correlation_penalty_weight": self.spec.correlation_penalty_weight,
                    "portfolio_sharpe_weight": self.spec.portfolio_sharpe_weight,
                    "avg_positive_pair_corr": best_avg_positive_corr,
                    "correlation_penalty": (
                        self.spec.correlation_penalty_weight * best_avg_positive_corr
                    ),
                    "portfolio_sharpe": best_portfolio_sharpe,
                    "portfolio_sharpe_term": (
                        self.spec.portfolio_sharpe_weight * best_portfolio_sharpe
                        if diversification_mode == "marginal_sharpe"
                        else 0.0
                    ),
                },
                "metrics": self.metrics(best_ids, include_correlation=True),
                "selected": [
                    self.candidate_row(attempt_id, rank=index + 1)
                    for index, attempt_id in enumerate(best_ids)
                ],
            }
            self.progress(
                "objective_done",
                objective=objective_name,
                best_start=best_start,
                objective_score=best_score,
                selected_count=len(best_ids),
            )
        self.progress("optimize_done", variant_count=len(variants))
        return variants


def optimizer_candidate_payload(candidate: OptimizerCandidate) -> dict[str, Any]:
    return {
        "attempt_id": candidate.attempt_id,
        "candidate_name": candidate.row.get("candidate_name"),
        "run_id": candidate.row.get("run_id"),
        "created_at": candidate.created_at,
        "instruments": candidate.instruments,
        "family": candidate.family,
        "family_source": candidate.family_source,
        "lineage_id": candidate.lineage_id,
        "behavior_fingerprint": candidate.behavior_fingerprint,
        "structural_family_signature": candidate.structural_family_signature,
        "score": candidate.score,
        "avg_hold_hours": candidate.avg_hold_hours,
        "p90_hold_hours": candidate.p90_hold_hours,
        "max_hold_hours": candidate.max_hold_hours,
        "path_quality": candidate.path_quality,
        "stop_loss_percent": candidate.stop_loss_percent,
        "trade_count": candidate.trade_count,
        "trades_per_month": candidate.trades_per_month,
        "dates": candidate.dates,
        "daily_r": candidate.daily_r,
        "open_counts": candidate.open_counts,
        "closed_counts": candidate.closed_counts,
    }


def _optimizer_spec_payload(spec: PortfolioOptimizerSpec) -> dict[str, Any]:
    return {
        "portfolio_name": spec.portfolio_name,
        "portfolio_size": spec.portfolio_size,
        "candidate_limit": spec.candidate_limit,
        "require_positive_source_return": spec.require_positive_source_return,
        "swap_candidate_limit": spec.swap_candidate_limit,
        "objective_names": list(spec.objective_names),
        "max_swaps": spec.max_swaps,
        "random_starts": spec.random_starts,
        "random_seed": spec.random_seed,
        "max_per_family": spec.max_per_family,
        "max_instrument_share": spec.max_instrument_share,
        "min_fx_share": spec.min_fx_share,
        "max_metal_share": spec.max_metal_share,
        "max_index_share": spec.max_index_share,
        "max_avg_open_positions": spec.max_avg_open_positions,
        "max_peak_open_positions": spec.max_peak_open_positions,
        "target_trades_per_month": spec.target_trades_per_month,
        "max_trades_per_month": spec.max_trades_per_month,
        "correlation_penalty_weight": spec.correlation_penalty_weight,
        "diversification_mode": spec.diversification_mode,
        "portfolio_sharpe_weight": spec.portfolio_sharpe_weight,
        "risk_weight_multiplier": spec.risk_weight_multiplier,
        "baseline_attempt_ids": list(spec.baseline_attempt_ids),
        "required_attempt_ids": list(spec.required_attempt_ids),
        "account": spec.account,
    }


def _rust_optimizer_input_payload(
    candidates: list[OptimizerCandidate],
    spec: PortfolioOptimizerSpec,
) -> dict[str, Any]:
    return {
        "spec": _optimizer_spec_payload(spec),
        "candidates": [optimizer_candidate_payload(candidate) for candidate in candidates],
        "objectives": objective_weights_for_spec(spec),
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _rust_optimizer_manifest() -> Path:
    return _repo_root() / "rust" / "portfolio-optimizer" / "Cargo.toml"


def _rust_optimizer_cdylib_path(*, release: bool = True) -> Path:
    profile = "release" if release else "debug"
    target_root = _repo_root() / "rust" / "portfolio-optimizer" / "target" / profile
    if sys.platform.startswith("win"):
        return target_root / "portfolio_optimizer_rs.dll"
    if sys.platform == "darwin":
        return target_root / "libportfolio_optimizer_rs.dylib"
    return target_root / "libportfolio_optimizer_rs.so"


def _ensure_rust_optimizer_extension(*, release: bool = True) -> Path:
    manifest = _rust_optimizer_manifest()
    if not manifest.exists():
        raise RuntimeError(f"Rust optimizer manifest not found: {manifest}")
    command = [
        "cargo",
        "build",
        "--quiet",
        "--manifest-path",
        str(manifest),
        "--features",
        "python-extension",
        "--lib",
    ]
    if release:
        command.insert(2, "--release")
    completed = subprocess.run(
        command,
        cwd=_repo_root(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "failed to build Rust optimizer PyO3 extension: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    cdylib_path = _rust_optimizer_cdylib_path(release=release)
    if not cdylib_path.exists():
        raise RuntimeError(f"Rust optimizer extension build did not produce {cdylib_path}")
    return cdylib_path


def _load_rust_optimizer_module(*, release: bool = True) -> Any:
    cached = _RUST_OPTIMIZER_MODULES.get(bool(release))
    if cached is not None:
        return cached
    cdylib_path = _ensure_rust_optimizer_extension(release=release)
    loader = importlib.machinery.ExtensionFileLoader(
        "portfolio_optimizer_rs", str(cdylib_path)
    )
    spec = importlib.util.spec_from_file_location(
        "portfolio_optimizer_rs", cdylib_path, loader=loader
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to create import spec for {cdylib_path}")
    module = importlib.util.module_from_spec(spec)
    previous_module = sys.modules.pop("portfolio_optimizer_rs", None)
    try:
        sys.modules["portfolio_optimizer_rs"] = module
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop("portfolio_optimizer_rs", None)
        if previous_module is not None:
            sys.modules["portfolio_optimizer_rs"] = previous_module
        raise
    _RUST_OPTIMIZER_MODULES[bool(release)] = module
    return module


def _hydrate_rust_variants(
    variants: dict[str, dict[str, Any]],
    search: PortfolioSearch,
) -> dict[str, dict[str, Any]]:
    hydrated: dict[str, dict[str, Any]] = {}
    for name, variant in variants.items():
        selected_ids = [
            str(item)
            for item in variant.get("selected_attempt_ids", [])
            if str(item) in search.by_id
        ]
        hydrated[name] = {
            **variant,
            "selected_attempt_ids": selected_ids,
            "selected": [
                search.candidate_row(attempt_id, rank=index + 1)
                for index, attempt_id in enumerate(selected_ids)
            ],
        }
    return hydrated


def _run_pyo3_optimizer(
    candidates: list[OptimizerCandidate],
    spec: PortfolioOptimizerSpec,
    search: PortfolioSearch,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    module = _load_rust_optimizer_module(release=True)
    payload = _rust_optimizer_input_payload(candidates, spec)
    output_json = module.optimize_json(json.dumps(payload, ensure_ascii=True))
    output = json.loads(output_json)
    variants = output.get("variants") or {}
    pareto_front = output.get("pareto_front") or []
    if not isinstance(variants, dict) or not isinstance(pareto_front, list):
        raise RuntimeError("Rust optimizer returned an unexpected payload shape.")
    return _hydrate_rust_variants(variants, search), pareto_front


def analyze_behavioral_similarity(
    candidates: list[OptimizerCandidate],
    *,
    reference_attempt_ids: list[str],
    active_epsilon: float = 1e-9,
    worst_quantile: float = 0.1,
    min_observations: int = 3,
    behavioral_weights: dict[str, float] | None = None,
    cluster_threshold: float = 0.8,
) -> dict[str, Any]:
    if not candidates:
        raise ValueError("Behavioral similarity requires candidates.")
    known_ids = {candidate.attempt_id for candidate in candidates}
    references = list(dict.fromkeys(str(item) for item in reference_attempt_ids))
    if not references or any(item not in known_ids for item in references):
        raise ValueError("Behavioral similarity requires known reference attempt IDs.")
    weights = behavioral_weights or {
        "active_overlap": 0.2,
        "return_correlation": 0.2,
        "downside_correlation": 0.3,
        "worst_decile_correlation": 0.3,
    }
    module = _load_rust_optimizer_module(release=True)
    payload = {
        "schema_version": "portfolio-behavioral-similarity-v1",
        "candidates": [
            {
                "attempt_id": candidate.attempt_id,
                "dates": candidate.dates,
                "daily_r": candidate.daily_r,
            }
            for candidate in candidates
        ],
        "reference_attempt_ids": references,
        "active_epsilon": float(active_epsilon),
        "worst_quantile": float(worst_quantile),
        "min_observations": int(min_observations),
        "behavioral_weights": {
            key: float(weights.get(key, 0.0))
            for key in (
                "active_overlap",
                "return_correlation",
                "downside_correlation",
                "worst_decile_correlation",
            )
        },
        "cluster_threshold": float(cluster_threshold),
    }
    output_json = module.analyze_similarity_json(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    )
    output = json.loads(output_json)
    if output.get("schema_version") != payload["schema_version"]:
        raise RuntimeError("Rust behavioral similarity returned an unexpected schema.")
    if output.get("attempt_ids") != sorted(known_ids):
        raise RuntimeError("Rust behavioral similarity returned unexpected candidate IDs.")
    return output


def run_optimizer_backend(
    candidates: list[OptimizerCandidate],
    spec: PortfolioOptimizerSpec,
    *,
    backend: str = "auto",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[PortfolioSearch, dict[str, dict[str, Any]], list[dict[str, Any]], str]:
    requested_backend = str(backend or "auto").lower()
    if requested_backend not in {"python", "pyo3", "auto"}:
        raise ValueError(f"Unsupported optimizer backend: {backend}")
    required_ids = tuple(dict.fromkeys(spec.required_attempt_ids))
    known_ids = {candidate.attempt_id for candidate in candidates}
    missing_required = sorted(set(required_ids) - known_ids)
    if missing_required:
        raise ValueError(
            "required_attempt_ids are missing from the optimizer candidate pool: "
            + ", ".join(missing_required)
        )
    if len(required_ids) > spec.portfolio_size:
        raise ValueError("required_attempt_ids exceed portfolio_size")
    search = PortfolioSearch(candidates, spec, progress_callback=progress_callback)
    if requested_backend == "python":
        variants = search.optimize()
        return search, variants, search.pareto_front(limit=50), "python"
    try:
        if progress_callback is not None:
            progress_callback({"event": "rust_optimizer_start", "backend": "pyo3"})
        variants, pareto_front = _run_pyo3_optimizer(candidates, spec, search)
        if progress_callback is not None:
            progress_callback(
                {
                    "event": "rust_optimizer_done",
                    "backend": "pyo3",
                    "variant_count": len(variants),
                    "pareto_front_count": len(pareto_front),
                }
            )
        return search, variants, pareto_front, "pyo3"
    except Exception as exc:
        if progress_callback is not None:
            progress_callback(
                {
                    "event": "rust_optimizer_failed",
                    "backend": "pyo3",
                    "requested_backend": requested_backend,
                    "reason": str(exc),
                }
            )
        raise RuntimeError(
            "Rust/PyO3 portfolio optimizer failed; Python fallback is disabled. "
            "Fix the native backend before running portfolio optimization."
        ) from exc


def load_baseline_attempt_ids(report_path: Path) -> list[str]:
    payload = read_json_if_exists(report_path) or {}
    selected = payload.get("selected") or []
    attempt_ids: list[str] = []
    if isinstance(selected, list):
        for row in selected:
            if isinstance(row, dict):
                attempt_id = str(row.get("attempt_id") or "").strip()
                if attempt_id:
                    attempt_ids.append(attempt_id)
    raw_attempts = payload.get("selected_attempt_ids") or payload.get("attempt_ids") or []
    if isinstance(raw_attempts, list):
        for item in raw_attempts:
            attempt_id = str(item or "").strip()
            if attempt_id:
                attempt_ids.append(attempt_id)
    return list(dict.fromkeys(attempt_ids))


def load_baseline_account(report_path: Path) -> dict[str, Any]:
    payload = read_json_if_exists(report_path) or {}
    account = payload.get("account")
    if not isinstance(account, dict):
        return {}
    return {
        key: value
        for key, value in account.items()
        if value not in (None, [], {})
    }


def baseline_metrics_from_ids(
    search: PortfolioSearch,
    name: str,
    attempt_ids: list[str],
) -> dict[str, Any]:
    selected_ids = search.ids_for_known_attempts(attempt_ids)
    return {
        "name": name,
        "input_count": len(attempt_ids),
        "matched_count": len(selected_ids),
        "selected_attempt_ids": selected_ids,
        "metrics": search.metrics(selected_ids, include_correlation=True)
        if selected_ids
        else {},
    }


def write_optimizer_report(
    *,
    report_root: Path,
    spec: PortfolioOptimizerSpec,
    candidates: list[OptimizerCandidate],
    rejections: dict[str, int],
    variants: dict[str, dict[str, Any]],
    baselines: list[dict[str, Any]],
    pareto_front: list[dict[str, Any]],
    source_info: dict[str, Any],
) -> dict[str, Any]:
    report_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "portfolio_name": spec.portfolio_name,
        "spec": {
            "portfolio_size": spec.portfolio_size,
            "candidate_scope": spec.candidate_scope,
            "min_score": spec.min_score,
            "require_positive_source_return": spec.require_positive_source_return,
            "allowed_asset_classes": list(spec.allowed_asset_classes),
            "allowed_instruments": list(spec.allowed_instruments),
            "blocked_instruments": list(spec.blocked_instruments),
            "max_avg_hold_hours": spec.max_avg_hold_hours,
            "max_p90_hold_hours": spec.max_p90_hold_hours,
            "max_single_hold_hours": spec.max_single_hold_hours,
            "candidate_limit": spec.candidate_limit,
            "swap_candidate_limit": spec.swap_candidate_limit,
            "objective_names": list(spec.objective_names),
            "max_swaps": spec.max_swaps,
            "random_starts": spec.random_starts,
            "random_seed": spec.random_seed,
            "max_per_family": spec.max_per_family,
            "max_instrument_share": spec.max_instrument_share,
            "min_fx_share": spec.min_fx_share,
            "max_metal_share": spec.max_metal_share,
            "max_index_share": spec.max_index_share,
            "max_avg_open_positions": spec.max_avg_open_positions,
            "max_peak_open_positions": spec.max_peak_open_positions,
            "target_trades_per_month": spec.target_trades_per_month,
            "max_trades_per_month": spec.max_trades_per_month,
            "correlation_penalty_weight": spec.correlation_penalty_weight,
            "diversification_mode": spec.diversification_mode,
            "portfolio_sharpe_weight": spec.portfolio_sharpe_weight,
            "risk_weight_multiplier": spec.risk_weight_multiplier,
            "account": spec.account,
        },
        "source": source_info,
        "candidate_count": len(candidates),
        "rejections": rejections,
        "baselines": baselines,
        "variants": variants,
        "pareto_front": pareto_front,
    }
    json_path = report_root / "portfolio-optimization.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    candidate_csv = report_root / "optimizer-candidates.csv"
    candidate_search = PortfolioSearch(candidates, spec)
    with candidate_csv.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "rank",
            "attempt_id",
            "candidate_name",
            "run_id",
            "created_at",
            "instruments",
            "asset_class",
            "score",
            "final_r",
            "maxdd_r",
            "avg_holding_hours",
            "p90_holding_hours",
            "max_holding_hours",
            "trades_per_month",
            "trade_count",
            "family",
            "profile_ref",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index, candidate in enumerate(candidates, start=1):
            row = candidate_search.candidate_row(candidate.attempt_id, index)
            writer.writerow({key: row.get(key) for key in fieldnames})
    comparison_csv = report_root / "portfolio-variant-comparison.csv"
    with comparison_csv.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "kind",
            "name",
            "count",
            "final_r",
            "maxdd_r",
            "neg_months",
            "neg_weeks",
            "max_daily_loss_streak",
            "worst_month_r",
            "worst_week_r",
            "worst_day_r",
            "top_day_gain_share",
            "avg_open_positions",
            "peak_open_positions",
            "trades_per_month",
            "mean_avg_hold_hours",
            "max_avg_hold_hours",
            "max_p90_hold_hours",
            "max_single_trade_hold_hours",
            "avg_pair_corr",
            "max_pair_corr",
            "avg_positive_pair_corr",
            "portfolio_sharpe",
            "account_initial_final_balance",
            "account_initial_return_pct",
            "account_initial_maxdd_usd",
            "account_initial_blown",
            "account_initial_min_lot_forced_trade_pct",
            "account_initial_max_actual_risk_pct",
            "account_current_final_balance",
            "account_current_return_pct",
            "account_current_maxdd_usd",
            "account_current_blown",
            "account_current_min_lot_forced_trade_pct",
            "account_current_max_actual_risk_pct",
            "constraint_violations",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for baseline in baselines:
            metrics = baseline.get("metrics") or {}
            writer.writerow(_comparison_row("baseline", baseline.get("name"), metrics))
        for name, variant in variants.items():
            writer.writerow(_comparison_row("variant", name, variant.get("metrics") or {}))
    for name, variant in variants.items():
        selected_csv = report_root / f"selected-{name}.csv"
        with selected_csv.open("w", newline="", encoding="utf-8") as handle:
            selected = list(variant.get("selected") or [])
            fieldnames = list(selected[0].keys()) if selected else ["rank", "attempt_id"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(selected)
        config_payload = {
            "version": 2,
            "portfolio_name": f"{spec.portfolio_name}-{name}",
            "candidate_scope": "all",
            "catch_up_full_backtests": False,
            "generate_profile_drops": False,
            "export_bundle": False,
            "profile_drop_exit_policy_cell": "best",
            "selected_attempt_ids": variant.get("selected_attempt_ids") or [],
            "sleeves": [
                {
                    "name": f"optimizer-{name}",
                    "prefilter_limit": spec.portfolio_size,
                    "candidate_limit": -1,
                    "shortlist_size": spec.portfolio_size,
                    "min_score_36": 0.0,
                    "drawdown_penalty": 0.0,
                    "novelty_penalty": 0.0,
                    "max_per_run": -1,
                    "max_per_strategy_key": -1,
                    "max_sameness_to_board": -1.0,
                    "require_full_backtest_36": True,
                    "scalar_metric_terms": [],
                    "field_filters": [],
                }
            ],
        }
        if spec.account:
            config_payload["account"] = spec.account
        (report_root / f"build-portfolio-{name}.json").write_text(
            json.dumps(config_payload, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
    pareto_csv = report_root / "portfolio-pareto-front.csv"
    with pareto_csv.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "rank",
            "archive_label",
            "objective_name",
            "return_score",
            "balanced_score",
            "stability_score",
            "deployable_score",
            "count",
            "final_r",
            "maxdd_r",
            "return_to_dd",
            "neg_months",
            "neg_weeks",
            "worst_month_r",
            "worst_week_r",
            "worst_day_r",
            "top_day_gain_share",
            "max_daily_loss_streak",
            "avg_open_positions",
            "mean_avg_hold_hours",
            "peak_open_positions",
            "trades_per_month",
            "account_initial_final_balance",
            "account_initial_return_pct",
            "account_initial_blown",
            "account_current_final_balance",
            "account_current_return_pct",
            "account_current_blown",
            "selected_attempt_ids",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index, item in enumerate(pareto_front, start=1):
            metrics = item.get("metrics") or {}
            scores = item.get("objective_scores") or {}
            writer.writerow(
                {
                    "rank": index,
                    "archive_label": item.get("archive_label"),
                    "objective_name": item.get("objective_name"),
                    "return_score": scores.get("return"),
                    "balanced_score": scores.get("balanced"),
                    "stability_score": scores.get("stability"),
                    "deployable_score": scores.get("deployable"),
                    "count": metrics.get("count"),
                    "final_r": metrics.get("final_r"),
                    "maxdd_r": metrics.get("maxdd_r"),
                    "return_to_dd": metrics.get("return_to_dd"),
                    "neg_months": metrics.get("neg_months"),
                    "neg_weeks": metrics.get("neg_weeks"),
                    "worst_month_r": metrics.get("worst_month_r"),
                    "worst_week_r": metrics.get("worst_week_r"),
                    "worst_day_r": metrics.get("worst_day_r"),
                    "top_day_gain_share": metrics.get("top_day_gain_share"),
                    "max_daily_loss_streak": metrics.get("max_daily_loss_streak"),
                    "avg_open_positions": metrics.get("avg_open_positions"),
                    "mean_avg_hold_hours": metrics.get("mean_avg_hold_hours"),
                    "peak_open_positions": metrics.get("peak_open_positions"),
                    "trades_per_month": metrics.get("trades_per_month"),
                    "account_initial_final_balance": (metrics.get("account_initial") or {}).get(
                        "final_balance"
                    ),
                    "account_initial_return_pct": (metrics.get("account_initial") or {}).get(
                        "final_return_pct"
                    ),
                    "account_initial_blown": (metrics.get("account_initial") or {}).get(
                        "blown"
                    ),
                    "account_current_final_balance": (metrics.get("account_current") or {}).get(
                        "final_balance"
                    ),
                    "account_current_return_pct": (metrics.get("account_current") or {}).get(
                        "final_return_pct"
                    ),
                    "account_current_blown": (metrics.get("account_current") or {}).get(
                        "blown"
                    ),
                    "selected_attempt_ids": "|".join(item.get("selected_attempt_ids") or []),
                }
            )
    for index, item in enumerate(pareto_front[:10], start=1):
        config_payload = {
            "version": 2,
            "portfolio_name": f"{spec.portfolio_name}-pareto-{index:02d}",
            "candidate_scope": "all",
            "catch_up_full_backtests": False,
            "generate_profile_drops": False,
            "export_bundle": False,
            "profile_drop_exit_policy_cell": "best",
            "selected_attempt_ids": item.get("selected_attempt_ids") or [],
            "sleeves": [
                {
                    "name": f"optimizer-pareto-{index:02d}",
                    "prefilter_limit": spec.portfolio_size,
                    "candidate_limit": -1,
                    "shortlist_size": spec.portfolio_size,
                    "min_score_36": 0.0,
                    "drawdown_penalty": 0.0,
                    "novelty_penalty": 0.0,
                    "max_per_run": -1,
                    "max_per_strategy_key": -1,
                    "max_sameness_to_board": -1.0,
                    "require_full_backtest_36": True,
                    "scalar_metric_terms": [],
                    "field_filters": [],
                }
            ],
        }
        if spec.account:
            config_payload["account"] = spec.account
        (report_root / f"build-portfolio-pareto-{index:02d}.json").write_text(
            json.dumps(config_payload, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
    markdown_path = report_root / "portfolio-optimization.md"
    markdown_path.write_text(
        _optimization_markdown(
            payload,
            comparison_csv=comparison_csv,
            pareto_csv=pareto_csv,
        ),
        encoding="utf-8",
    )
    return {
        "report_root": str(report_root),
        "optimization_json": str(json_path),
        "optimization_markdown": str(markdown_path),
        "candidate_csv": str(candidate_csv),
        "comparison_csv": str(comparison_csv),
        "pareto_csv": str(pareto_csv),
        "candidate_count": len(candidates),
        "variant_count": len(variants),
        "baseline_count": len(baselines),
        "pareto_front_count": len(pareto_front),
    }


def _comparison_row(kind: str, name: Any, metrics: dict[str, Any]) -> dict[str, Any]:
    account_initial = metrics.get("account_initial") or {}
    account_current = metrics.get("account_current") or {}
    return {
        "kind": kind,
        "name": name,
        "count": metrics.get("count"),
        "final_r": metrics.get("final_r"),
        "maxdd_r": metrics.get("maxdd_r"),
        "neg_months": metrics.get("neg_months"),
        "neg_weeks": metrics.get("neg_weeks"),
        "max_daily_loss_streak": metrics.get("max_daily_loss_streak"),
        "worst_month_r": metrics.get("worst_month_r"),
        "worst_week_r": metrics.get("worst_week_r"),
        "worst_day_r": metrics.get("worst_day_r"),
        "top_day_gain_share": metrics.get("top_day_gain_share"),
        "avg_open_positions": metrics.get("avg_open_positions"),
        "peak_open_positions": metrics.get("peak_open_positions"),
        "trades_per_month": metrics.get("trades_per_month"),
        "mean_avg_hold_hours": metrics.get("mean_avg_hold_hours"),
        "max_avg_hold_hours": metrics.get("max_avg_hold_hours"),
        "max_p90_hold_hours": metrics.get("max_p90_hold_hours"),
        "max_single_trade_hold_hours": metrics.get("max_single_trade_hold_hours"),
        "avg_pair_corr": metrics.get("avg_pair_corr"),
        "max_pair_corr": metrics.get("max_pair_corr"),
        "avg_positive_pair_corr": metrics.get("avg_positive_pair_corr"),
        "portfolio_sharpe": metrics.get("portfolio_sharpe"),
        "account_initial_final_balance": account_initial.get("final_balance"),
        "account_initial_return_pct": account_initial.get("final_return_pct"),
        "account_initial_maxdd_usd": account_initial.get("max_drawdown_usd"),
        "account_initial_blown": account_initial.get("blown"),
        "account_initial_min_lot_forced_trade_pct": account_initial.get(
            "min_lot_forced_trade_pct"
        ),
        "account_initial_max_actual_risk_pct": account_initial.get("max_actual_risk_pct"),
        "account_current_final_balance": account_current.get("final_balance"),
        "account_current_return_pct": account_current.get("final_return_pct"),
        "account_current_maxdd_usd": account_current.get("max_drawdown_usd"),
        "account_current_blown": account_current.get("blown"),
        "account_current_min_lot_forced_trade_pct": account_current.get(
            "min_lot_forced_trade_pct"
        ),
        "account_current_max_actual_risk_pct": account_current.get("max_actual_risk_pct"),
        "constraint_violations": json.dumps(
            metrics.get("constraint_violations") or {}, ensure_ascii=True
        ),
    }


def _format_metric_line(name: str, metrics: dict[str, Any]) -> str:
    account = metrics.get("account_initial") or {}
    account_text = (
        f"${float(account.get('final_balance') or 0.0):,.0f} / "
        f"{float(account.get('final_return_pct') or 0.0):.1f}%"
        if account
        else "-"
    )
    return (
        f"| {name} | {metrics.get('count', 0)} | "
        f"{float(metrics.get('final_r') or 0.0):.2f} | "
        f"{float(metrics.get('maxdd_r') or 0.0):.2f} | "
        f"{int(metrics.get('neg_months') or 0)} | "
        f"{int(metrics.get('neg_weeks') or 0)} | "
        f"{int(metrics.get('max_daily_loss_streak') or 0)} | "
        f"{float(metrics.get('worst_week_r') or 0.0):.2f} | "
        f"{float(metrics.get('top_day_gain_share') or 0.0):.2%} | "
        f"{float(metrics.get('avg_open_positions') or 0.0):.1f} / "
        f"{float(metrics.get('peak_open_positions') or 0.0):.0f} | "
        f"{float(metrics.get('trades_per_month') or 0.0):.1f} | "
        f"{float(metrics.get('mean_avg_hold_hours') or 0.0):.1f}h | "
        f"{account_text} | "
        f"{json.dumps(metrics.get('constraint_violations') or {}, ensure_ascii=True)} |"
    )


def _optimization_markdown(
    payload: dict[str, Any],
    *,
    comparison_csv: Path,
    pareto_csv: Path,
) -> str:
    lines = [
        f"# {payload.get('portfolio_name')} Portfolio Optimization",
        "",
        f"Generated: {payload.get('generated_at')}",
        "",
        "## Candidate Pool",
        "",
        f"- Candidates retained: {payload.get('candidate_count')}",
        f"- Rejections: `{json.dumps(payload.get('rejections') or {}, ensure_ascii=True)}`",
        "",
        "## Comparison",
        "",
        "| Name | Count | Final R | Max DD R | Neg Months | Neg Weeks | Max Loss Streak | Worst Week R | Top Day Share | Open Avg/Peak | Trades/Mo | Mean Avg Hold | Account Initial | Violations |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for baseline in payload.get("baselines") or []:
        lines.append(
            _format_metric_line(
                f"baseline:{baseline.get('name')}",
                baseline.get("metrics") or {},
            )
        )
    for name, variant in (payload.get("variants") or {}).items():
        lines.append(_format_metric_line(f"variant:{name}", variant.get("metrics") or {}))
    pareto_front = list(payload.get("pareto_front") or [])
    lines.extend(
        [
            "",
            "## Pareto Front",
            "",
            f"- Nondominated archived portfolios: {len(pareto_front)}",
            "",
            "| Rank | Label | Final R | Max DD R | Neg Months | Neg Weeks | Worst Week R | Top Day Share | Open Avg/Peak | Trades/Mo | Mean Avg Hold | Account Initial |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for index, item in enumerate(pareto_front[:12], start=1):
        metrics = item.get("metrics") or {}
        account = metrics.get("account_initial") or {}
        account_text = (
            f"${float(account.get('final_balance') or 0.0):,.0f} / "
            f"{float(account.get('final_return_pct') or 0.0):.1f}%"
            if account
            else "-"
        )
        lines.append(
            f"| {index} | {item.get('archive_label')} | "
            f"{float(metrics.get('final_r') or 0.0):.2f} | "
            f"{float(metrics.get('maxdd_r') or 0.0):.2f} | "
            f"{int(metrics.get('neg_months') or 0)} | "
            f"{int(metrics.get('neg_weeks') or 0)} | "
            f"{float(metrics.get('worst_week_r') or 0.0):.2f} | "
            f"{float(metrics.get('top_day_gain_share') or 0.0):.2%} | "
            f"{float(metrics.get('avg_open_positions') or 0.0):.1f} / "
            f"{float(metrics.get('peak_open_positions') or 0.0):.0f} | "
            f"{float(metrics.get('trades_per_month') or 0.0):.1f} | "
            f"{float(metrics.get('mean_avg_hold_hours') or 0.0):.1f}h | "
            f"{account_text} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "This optimizer scores complete portfolios from source calendar ledgers. It is separate from the older sleeve selector: individual candidates can lose to lower-score strategies if they worsen the basket calendar, holds, concentration, or drawdown.",
            "",
            f"CSV comparison: `{comparison_csv}`",
            f"Pareto CSV: `{pareto_csv}`",
        ]
    )
    return "\n".join(lines) + "\n"
