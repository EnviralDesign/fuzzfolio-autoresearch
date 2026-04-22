from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
import re
from typing import Any, Callable

from .corpus_tools import (
    build_similarity_payload as build_candidate_similarity_payload,
    compute_scalar_metric_bonus,
    subset_similarity_payload,
    select_promotion_board,
)


DEFAULT_SLEEVE_SPEC: dict[str, Any] = {
    "prefilter_limit": 128,
    "candidate_limit": -1,
    "shortlist_size": 12,
    "min_score_36": 40.0,
    "min_retention_ratio": 0.0,
    "min_trades_per_month": 0.0,
    "max_drawdown_r": -1.0,
    "drawdown_penalty": 0.65,
    "trade_rate_bonus_weight": 0.0,
    "trade_rate_bonus_target": 8.0,
    "novelty_penalty": 18.0,
    "max_per_run": 1,
    "max_per_strategy_key": 1,
    "max_sameness_to_board": 0.78,
    "require_full_backtest_36": True,
    "scalar_metric_terms": [],
    "field_filters": [],
}

DEFAULT_BREADTH_SCALAR_METRIC_TERMS: list[dict[str, Any]] = [
    {
        "name": "breadth_score",
        "field_candidates": [
            "breadth_score_36m",
            "durability_score_36m",
            "gain_breadth_score_36m",
        ],
        "direction": "higher",
        "target": 1.0,
        "weight": 6.0,
    },
    {
        "name": "effective_profitable_episodes",
        "field": "effective_profitable_episodes_36m",
        "direction": "higher",
        "target": 8.0,
        "weight": 3.0,
    },
    {
        "name": "active_gain_weeks_ratio",
        "field": "active_gain_weeks_ratio_36m",
        "direction": "higher",
        "target": 0.35,
        "weight": 3.0,
    },
    {
        "name": "top_3_episode_share",
        "field": "top_3_episode_share_36m",
        "direction": "lower",
        "target": 1.0,
        "weight": 3.0,
    },
    {
        "name": "gain_concentration_risk",
        "field_candidates": [
            "gain_concentration_risk_36m",
            "episode_concentration_risk_36m",
        ],
        "direction": "lower",
        "target": 1.0,
        "weight": 3.0,
    },
    {
        "name": "overlap_factor",
        "field": "overlap_factor_36m",
        "direction": "lower",
        "target": 6.0,
        "weight": 2.0,
    },
]


DEFAULT_PORTFOLIO_SPEC: dict[str, Any] = {
    "version": 1,
    "portfolio_name": "default-portfolio",
    "catch_up_full_backtests": False,
    "catch_up_force_rebuild": False,
    "catch_up_require_scrutiny_36": False,
    "full_backtest_job_timeout_seconds": 2400,
    "generate_profile_drops": True,
    "export_bundle": True,
    "profile_drop_lookback_months": 36,
    "profile_drop_timeout_seconds": 1800,
    "profile_drop_workers": 4,
    "chart_trades_x_max": 300.0,
    "sleeves": [
        {
            **DEFAULT_SLEEVE_SPEC,
            "name": "quality",
            "shortlist_size": 24,
            "trade_rate_bonus_weight": 0.0,
            "trade_rate_bonus_target": 8.0,
        },
        {
            **DEFAULT_SLEEVE_SPEC,
            "name": "cadence",
            "trade_rate_bonus_weight": 8.0,
            "trade_rate_bonus_target": 4.0,
        },
        {
            **DEFAULT_SLEEVE_SPEC,
            "name": "breadth",
            "shortlist_size": 12,
            "scalar_metric_terms": deepcopy(DEFAULT_BREADTH_SCALAR_METRIC_TERMS),
        },
    ],
}

DEFAULT_ACCOUNT_ASSET_MARGIN_WEIGHTS: dict[str, float] = {
    "fx": 1.0,
    "metal": 1.4,
    "commodity": 1.8,
    "index": 2.0,
    "equity": 2.2,
    "crypto": 3.5,
    "other": 2.0,
}

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

METAL_SYMBOLS = {"XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"}
COMMODITY_SYMBOLS = {"USOIL", "UKOIL", "WTI", "BRENT", "NATGAS", "NGAS"}
INDEX_SYMBOLS = {
    "US30",
    "US500",
    "SPX500",
    "NAS100",
    "USTEC",
    "DE40",
    "GER40",
    "UK100",
    "JP225",
    "AUS200",
    "HK50",
    "EU50",
}
CRYPTO_BASES = {
    "ADA",
    "AVAX",
    "BCH",
    "BNB",
    "BTC",
    "DOGE",
    "DOT",
    "ETH",
    "LTC",
    "SOL",
    "XLM",
    "XMR",
    "XRP",
}
HOURS_PER_MONTH = 24.0 * 365.25 / 12.0


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def default_portfolio_spec() -> dict[str, Any]:
    return deepcopy(DEFAULT_PORTFOLIO_SPEC)


def _merge_sleeve_spec(raw_spec: dict[str, Any], index: int) -> dict[str, Any]:
    merged = deepcopy(DEFAULT_SLEEVE_SPEC)
    merged.update(dict(raw_spec))
    name = str(merged.get("name") or "").strip()
    merged["name"] = name or f"sleeve-{index + 1}"
    scalar_metric_terms = merged.get("scalar_metric_terms")
    merged["scalar_metric_terms"] = (
        deepcopy(scalar_metric_terms) if isinstance(scalar_metric_terms, list) else []
    )
    field_filters = merged.get("field_filters")
    merged["field_filters"] = (
        deepcopy(field_filters) if isinstance(field_filters, list) else []
    )
    return merged


def load_portfolio_spec(path: Path) -> tuple[dict[str, Any], bool]:
    defaulted = False
    spec = default_portfolio_spec()
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Portfolio spec must be a JSON object: {path}")
        spec.update({key: value for key, value in payload.items() if key != "sleeves"})
        raw_sleeves = payload.get("sleeves")
        if raw_sleeves is None:
            raw_sleeves = spec.get("sleeves")
    else:
        defaulted = True
        raw_sleeves = spec.get("sleeves")
    if not isinstance(raw_sleeves, list) or not raw_sleeves:
        raise ValueError(f"Portfolio spec must define a non-empty sleeves list: {path}")
    spec["sleeves"] = [
        _merge_sleeve_spec(item if isinstance(item, dict) else {}, index)
        for index, item in enumerate(raw_sleeves)
    ]
    return spec, defaulted


def _normalize_token_list(raw_values: Any) -> list[str]:
    if isinstance(raw_values, str):
        values = [token.strip() for token in raw_values.split(",")]
    elif isinstance(raw_values, list):
        values = [str(token).strip() for token in raw_values]
    else:
        values = []
    normalized: list[str] = []
    seen: set[str] = set()
    for token in values:
        if not token:
            continue
        if token not in seen:
            normalized.append(token)
            seen.add(token)
    return normalized


def _normalize_asset_class_token(raw_value: Any) -> str | None:
    token = str(raw_value or "").strip().lower()
    if not token:
        return None
    aliases = {
        "forex": "fx",
        "fx": "fx",
        "metals": "metal",
        "metal": "metal",
        "commodities": "commodity",
        "commodity": "commodity",
        "indices": "index",
        "index": "index",
        "stocks": "equity",
        "stock": "equity",
        "equities": "equity",
        "equity": "equity",
        "crypto": "crypto",
        "cryptocurrency": "crypto",
        "other": "other",
    }
    return aliases.get(token, token)


def _normalize_account_spec(raw_spec: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw_spec, dict):
        return {}
    spec = dict(raw_spec)
    account_size = _safe_float(
        spec.get("account_size_usd")
        or spec.get("balance_usd")
        or spec.get("balance")
        or spec.get("account_balance")
    )
    leverage = _safe_float(spec.get("leverage"))
    risk_per_trade_pct = _safe_float(
        spec.get("risk_per_trade_pct")
        or spec.get("risk_per_trade_percent")
        or spec.get("risk_pct")
    )
    allowed_asset_classes = [
        token
        for token in (
            _normalize_asset_class_token(item)
            for item in _normalize_token_list(spec.get("allowed_asset_classes") or [])
        )
        if token
    ]
    blocked_asset_classes = [
        token
        for token in (
            _normalize_asset_class_token(item)
            for item in _normalize_token_list(spec.get("blocked_asset_classes") or [])
        )
        if token
    ]
    asset_class_margin_weights = dict(DEFAULT_ACCOUNT_ASSET_MARGIN_WEIGHTS)
    raw_weights = spec.get("asset_class_margin_weights")
    if isinstance(raw_weights, dict):
        for raw_key, raw_value in raw_weights.items():
            token = _normalize_asset_class_token(raw_key)
            weight = _safe_float(raw_value)
            if token and weight is not None and weight > 0.0:
                asset_class_margin_weights[token] = weight
    return {
        **spec,
        "account_size_usd": account_size,
        "leverage": leverage,
        "risk_per_trade_pct": risk_per_trade_pct,
        "allowed_asset_classes": allowed_asset_classes,
        "blocked_asset_classes": blocked_asset_classes,
        "asset_class_margin_weights": asset_class_margin_weights,
    }


def _resolve_account_spec(
    raw_account: Any,
    account_presets: dict[str, Any] | None = None,
    *,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = deepcopy(fallback) if isinstance(fallback, dict) else {}
    preset_name = None
    payload: dict[str, Any] = {}
    if isinstance(raw_account, str):
        preset_name = raw_account.strip()
        candidate = (account_presets or {}).get(preset_name)
        if isinstance(candidate, dict):
            payload = dict(candidate)
    elif isinstance(raw_account, dict):
        payload = dict(raw_account)
        preset_token = str(payload.get("account_preset") or "").strip()
        if preset_token and isinstance((account_presets or {}).get(preset_token), dict):
            preset_name = preset_token
            resolved.update(dict((account_presets or {})[preset_token]))
    if payload:
        resolved.update(payload)
    normalized = _normalize_account_spec(resolved)
    if preset_name:
        normalized["account_preset_name"] = preset_name
    return normalized


def load_portfolio_build_specs(path: Path) -> tuple[list[dict[str, Any]], bool]:
    if not path.exists():
        spec, defaulted = load_portfolio_spec(path)
        spec.pop("portfolio_variants", None)
        spec.pop("account_presets", None)
        spec["account"] = _normalize_account_spec(spec.get("account"))
        return [spec], defaulted

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Portfolio spec must be a JSON object: {path}")

    account_presets = (
        dict(payload.get("account_presets"))
        if isinstance(payload.get("account_presets"), dict)
        else {}
    )
    raw_variants = payload.get("portfolio_variants")
    if not isinstance(raw_variants, list) or not raw_variants:
        spec, defaulted = load_portfolio_spec(path)
        spec.pop("portfolio_variants", None)
        spec.pop("account_presets", None)
        spec["account"] = _resolve_account_spec(
            payload.get("account") or payload.get("account_preset"),
            account_presets,
        )
        return [spec], defaulted

    shared_spec = default_portfolio_spec()
    shared_spec.update(
        {
            key: value
            for key, value in payload.items()
            if key not in {"sleeves", "portfolio_variants", "account", "account_preset", "account_presets"}
        }
    )
    shared_raw_sleeves = payload.get("sleeves")
    shared_account = _resolve_account_spec(
        payload.get("account") or payload.get("account_preset"),
        account_presets,
    )
    portfolio_specs: list[dict[str, Any]] = []
    for index, raw_variant in enumerate(raw_variants):
        variant = dict(raw_variant) if isinstance(raw_variant, dict) else {}
        spec = deepcopy(shared_spec)
        spec.update(
            {
                key: value
                for key, value in variant.items()
                if key not in {"sleeves", "account", "account_preset"}
            }
        )
        raw_sleeves = (
            variant.get("sleeves")
            if isinstance(variant.get("sleeves"), list)
            else shared_raw_sleeves
        )
        if not isinstance(raw_sleeves, list) or not raw_sleeves:
            raise ValueError(f"Portfolio variant {index + 1} is missing a sleeves list: {path}")
        spec["sleeves"] = [
            _merge_sleeve_spec(item if isinstance(item, dict) else {}, sleeve_index)
            for sleeve_index, item in enumerate(raw_sleeves)
        ]
        spec["account"] = _resolve_account_spec(
            variant.get("account") or variant.get("account_preset"),
            account_presets,
            fallback=shared_account,
        )
        portfolio_name = str(spec.get("portfolio_name") or "").strip()
        spec["portfolio_name"] = portfolio_name or f"portfolio-{index + 1}"
        portfolio_specs.append(spec)
    return portfolio_specs, False


def infer_instrument_asset_class(instrument: Any) -> str:
    token = str(instrument or "").strip().upper()
    if not token:
        return "other"
    if token in METAL_SYMBOLS or token.startswith(("XAU", "XAG", "XPT", "XPD")):
        return "metal"
    if token in COMMODITY_SYMBOLS:
        return "commodity"
    if token in INDEX_SYMBOLS:
        return "index"
    if (
        len(token) == 6
        and token[:3].isalpha()
        and token[3:].isalpha()
        and token[:3] in FX_CODES
        and token[3:] in FX_CODES
    ):
        return "fx"
    if token.endswith("USD") and token[:-3] in CRYPTO_BASES:
        return "crypto"
    if re.fullmatch(r"[A-Z]{1,5}", token):
        return "equity"
    return "other"


@lru_cache(maxsize=4096)
def _load_json_payload(path_raw: str) -> dict[str, Any]:
    path = Path(path_raw)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _nested_get(payload: dict[str, Any], path: list[str]) -> Any:
    current: Any = payload
    for token in path:
        if not isinstance(current, dict):
            return None
        current = current.get(token)
    return current


def _load_full_backtest_aggregate(row: dict[str, Any]) -> dict[str, Any]:
    result_path = str(
        row.get("full_backtest_result_path_36m") or row.get("scrutiny_result_path_36m") or ""
    ).strip()
    if not result_path:
        return {}
    payload = _load_json_payload(result_path)
    data = payload.get("data")
    aggregate = data.get("aggregate") if isinstance(data, dict) else None
    return aggregate if isinstance(aggregate, dict) else {}


def _safe_metric_field(
    row: dict[str, Any], filter_spec: dict[str, Any]
) -> tuple[str | None, float | None]:
    field = str(filter_spec.get("field") or "").strip()
    if field:
        return field, _safe_float(row.get(field))
    for candidate in filter_spec.get("field_candidates") or []:
        token = str(candidate or "").strip()
        if not token:
            continue
        value = _safe_float(row.get(token))
        if value is not None:
            return token, value
    return None, None


def _field_filter_token(filter_spec: dict[str, Any], index: int) -> str:
    raw_name = str(
        filter_spec.get("name")
        or filter_spec.get("field")
        or "_".join(str(item or "") for item in (filter_spec.get("field_candidates") or []))
        or f"field_filter_{index + 1}"
    ).strip()
    token = re.sub(r"[^A-Za-z0-9]+", "_", raw_name).strip("_").lower()
    return token or f"field_filter_{index + 1}"


def _asset_classes_for_row(row: dict[str, Any]) -> list[str]:
    instruments = list(row.get("instruments_36m") or row.get("base_instruments") or [])
    classes = {infer_instrument_asset_class(instrument) for instrument in instruments}
    return sorted(token for token in classes if token)


def _compute_account_metrics(
    row: dict[str, Any],
    account_spec: dict[str, Any] | None,
) -> dict[str, Any]:
    aggregate = _load_full_backtest_aggregate(row)
    behavior_summary = aggregate.get("behavior_summary") if isinstance(aggregate.get("behavior_summary"), dict) else {}
    best_cell = aggregate.get("best_cell") if isinstance(aggregate.get("best_cell"), dict) else {}
    best_cell_path_metrics = (
        aggregate.get("best_cell_path_metrics")
        if isinstance(aggregate.get("best_cell_path_metrics"), dict)
        else {}
    )
    matrix_summary = aggregate.get("matrix_summary") if isinstance(aggregate.get("matrix_summary"), dict) else {}
    robust_cell = matrix_summary.get("robust_cell") if isinstance(matrix_summary.get("robust_cell"), dict) else {}
    quality_inputs = _nested_get(aggregate, ["quality_score", "inputs"])
    quality_inputs = quality_inputs if isinstance(quality_inputs, dict) else {}

    asset_classes = _asset_classes_for_row(row)
    primary_asset_class = asset_classes[0] if len(asset_classes) == 1 else ("mixed" if asset_classes else None)
    trades_per_month = _safe_float(row.get("trades_per_month_36m"))
    avg_holding_hours = _safe_float(best_cell_path_metrics.get("avg_holding_hours"))
    p90_holding_hours = _safe_float(best_cell_path_metrics.get("p90_holding_hours"))
    max_holding_hours = _safe_float(best_cell_path_metrics.get("max_holding_hours"))
    signal_coverage_ratio = _safe_float(behavior_summary.get("signal_coverage_ratio"))
    signal_density = _safe_float(behavior_summary.get("signal_density"))
    bars_per_signal = _safe_float(behavior_summary.get("bars_per_signal"))
    time_under_water_ratio = _safe_float(best_cell_path_metrics.get("time_under_water_ratio"))
    final_equity_r = _safe_float(best_cell_path_metrics.get("final_equity_r"))
    profit_factor = _safe_float(best_cell.get("profit_factor"))
    expectancy_r = _safe_float(best_cell.get("avg_net_r_per_closed_trade"))
    robust_expectancy_r = _safe_float(robust_cell.get("avg_net_r_per_closed_trade"))
    edge_rate_r_per_month = _safe_float(quality_inputs.get("edge_rate_r_per_month"))
    positive_cell_ratio = _safe_float(matrix_summary.get("positive_cell_ratio"))
    positive_week_ratio = _safe_float(_nested_get(best_cell_path_metrics, ["temporal_breadth", "positive_week_ratio"]))
    stop_loss_percent = _safe_float(robust_cell.get("stop_loss_percent")) or _safe_float(
        best_cell.get("stop_loss_percent")
    )

    avg_open_positions = None
    if trades_per_month is not None and avg_holding_hours is not None:
        avg_open_positions = trades_per_month * avg_holding_hours / HOURS_PER_MONTH
    coverage_positions = None
    if signal_coverage_ratio is not None:
        coverage_positions = signal_coverage_ratio * max(1, len(list(row.get("instruments_36m") or [])))
    if coverage_positions is not None:
        avg_open_positions = max(avg_open_positions or 0.0, coverage_positions)

    p90_open_positions = None
    if trades_per_month is not None and p90_holding_hours is not None:
        p90_open_positions = trades_per_month * p90_holding_hours / HOURS_PER_MONTH
    elif avg_open_positions is not None:
        p90_open_positions = avg_open_positions
    if coverage_positions is not None:
        p90_open_positions = max(p90_open_positions or 0.0, coverage_positions)

    metrics = {
        "asset_classes_36m": asset_classes,
        "asset_class_count_36m": len(asset_classes),
        "primary_asset_class_36m": primary_asset_class,
        "best_stop_loss_percent_36m": _safe_float(best_cell.get("stop_loss_percent")),
        "robust_stop_loss_percent_36m": _safe_float(robust_cell.get("stop_loss_percent")),
        "stop_loss_percent_36m": stop_loss_percent,
        "avg_holding_hours_36m": avg_holding_hours,
        "p90_holding_hours_36m": p90_holding_hours,
        "max_holding_hours_36m": max_holding_hours,
        "time_under_water_ratio_36m": time_under_water_ratio,
        "signal_coverage_ratio_36m": signal_coverage_ratio,
        "signal_density_36m": signal_density,
        "bars_per_signal_36m": bars_per_signal,
        "signal_selectivity_36m": behavior_summary.get("signal_selectivity"),
        "estimated_avg_open_positions_36m": avg_open_positions,
        "estimated_peak_open_positions_36m": p90_open_positions,
        "final_equity_r_36m": final_equity_r,
        "profit_factor_36m": profit_factor,
        "expectancy_r_36m": expectancy_r,
        "robust_expectancy_r_36m": robust_expectancy_r,
        "edge_rate_r_per_month_36m": edge_rate_r_per_month,
        "positive_cell_ratio_36m": positive_cell_ratio,
        "positive_week_ratio_36m": positive_week_ratio,
    }

    normalized_account = _normalize_account_spec(account_spec) if isinstance(account_spec, dict) else {}
    if not normalized_account:
        return metrics

    blocked_asset_classes = set(normalized_account.get("blocked_asset_classes") or [])
    allowed_asset_classes = set(normalized_account.get("allowed_asset_classes") or [])
    asset_class_margin_weights = dict(
        normalized_account.get("asset_class_margin_weights") or DEFAULT_ACCOUNT_ASSET_MARGIN_WEIGHTS
    )
    asset_margin_weight = max(
        (
            _safe_float(asset_class_margin_weights.get(asset_class))
            or DEFAULT_ACCOUNT_ASSET_MARGIN_WEIGHTS.get(asset_class, 2.0)
        )
        for asset_class in (asset_classes or ["other"])
    )
    blocked_asset_class_count = len(blocked_asset_classes.intersection(asset_classes))
    disallowed_asset_class_count = blocked_asset_class_count
    if allowed_asset_classes:
        disallowed_asset_class_count = len(
            [
                asset_class
                for asset_class in asset_classes
                if asset_class not in allowed_asset_classes
            ]
        )
    allowed_flag = (
        1.0 if not allowed_asset_classes or set(asset_classes).issubset(allowed_asset_classes) else 0.0
    )
    leverage = _safe_float(normalized_account.get("leverage"))
    risk_per_trade_pct = _safe_float(normalized_account.get("risk_per_trade_pct"))
    account_size_usd = _safe_float(normalized_account.get("account_size_usd"))

    avg_margin_load_pct = None
    peak_margin_load_pct = None
    if (
        leverage is not None
        and leverage > 0.0
        and risk_per_trade_pct is not None
        and risk_per_trade_pct > 0.0
        and stop_loss_percent is not None
        and stop_loss_percent > 0.0
    ):
        risk_fraction = risk_per_trade_pct / 100.0
        stop_loss_fraction = stop_loss_percent / 100.0
        if avg_open_positions is not None:
            avg_margin_load_pct = (
                100.0 * avg_open_positions * risk_fraction * asset_margin_weight / (stop_loss_fraction * leverage)
            )
        if p90_open_positions is not None:
            peak_margin_load_pct = (
                100.0 * p90_open_positions * risk_fraction * asset_margin_weight / (stop_loss_fraction * leverage)
            )

    metrics.update(
        {
            "account_name": normalized_account.get("name"),
            "account_size_usd": account_size_usd,
            "account_leverage": leverage,
            "account_risk_per_trade_pct": risk_per_trade_pct,
            "account_asset_margin_weight_36m": asset_margin_weight,
            "account_blocked_asset_class_count_36m": blocked_asset_class_count,
            "account_disallowed_asset_class_count_36m": disallowed_asset_class_count,
            "account_asset_class_allowed_flag_36m": allowed_flag,
            "account_estimated_avg_margin_load_pct_36m": avg_margin_load_pct,
            "account_estimated_peak_margin_load_pct_36m": peak_margin_load_pct,
            "account_estimated_avg_margin_load_usd_36m": (
                (account_size_usd * avg_margin_load_pct / 100.0)
                if account_size_usd is not None and avg_margin_load_pct is not None
                else None
            ),
            "account_estimated_peak_margin_load_usd_36m": (
                (account_size_usd * peak_margin_load_pct / 100.0)
                if account_size_usd is not None and peak_margin_load_pct is not None
                else None
            ),
        }
    )
    return metrics


def enrich_rows_for_account(
    rows: list[dict[str, Any]],
    account_spec: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    normalized_account = _normalize_account_spec(account_spec) if isinstance(account_spec, dict) else {}
    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        enriched_row = dict(row)
        enriched_row.update(_compute_account_metrics(enriched_row, normalized_account))
        enriched_rows.append(enriched_row)
    return enriched_rows


def filter_selection_candidate_rows(
    rows: list[dict[str, Any]],
    *,
    candidate_limit: int,
    min_score_36: float,
    min_retention_ratio: float,
    min_trades_per_month: float,
    max_drawdown_r: float,
    require_full_backtest_36: bool,
    field_filters: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int], float | None]:
    filtered_rows = list(rows)
    if candidate_limit >= 0:
        filtered_rows = filtered_rows[:candidate_limit]
    filter_rejections = {
        "missing_score_36m": 0,
        "score_below_min_score_36": 0,
        "missing_trades_per_month_36m": 0,
        "trades_below_min_trades_per_month": 0,
        "missing_retention_ratio_36m_vs_12m": 0,
        "retention_below_min_retention_ratio": 0,
        "missing_drawdown_36m": 0,
        "drawdown_above_max_drawdown_r": 0,
        "missing_full_backtest_36m": 0,
        "invalid_full_backtest_36m": 0,
    }
    normalized_field_filters = list(field_filters or [])
    field_filter_tokens = [
        _field_filter_token(filter_spec, index)
        for index, filter_spec in enumerate(normalized_field_filters)
    ]
    for token in field_filter_tokens:
        filter_rejections[f"field_filter_missing_{token}"] = 0
        filter_rejections[f"field_filter_failed_{token}"] = 0
    candidate_rows: list[dict[str, Any]] = []
    max_drawdown_cap = None if float(max_drawdown_r) < 0.0 else float(max_drawdown_r)
    for row in filtered_rows:
        score_36 = _safe_float(row.get("score_36m"))
        if score_36 is None:
            filter_rejections["missing_score_36m"] += 1
            continue
        if score_36 < float(min_score_36):
            filter_rejections["score_below_min_score_36"] += 1
            continue
        trades_per_month_36 = _safe_float(row.get("trades_per_month_36m"))
        if float(min_trades_per_month) > 0.0:
            if trades_per_month_36 is None:
                filter_rejections["missing_trades_per_month_36m"] += 1
                continue
            if trades_per_month_36 < float(min_trades_per_month):
                filter_rejections["trades_below_min_trades_per_month"] += 1
                continue
        retention_ratio = _safe_float(row.get("score_retention_ratio_36m_vs_12m"))
        if float(min_retention_ratio) > 0.0:
            if retention_ratio is None:
                filter_rejections["missing_retention_ratio_36m_vs_12m"] += 1
                continue
            if retention_ratio < float(min_retention_ratio):
                filter_rejections["retention_below_min_retention_ratio"] += 1
                continue
        drawdown_36 = _safe_float(row.get("max_drawdown_r_36m"))
        if max_drawdown_cap is not None:
            if drawdown_36 is None:
                filter_rejections["missing_drawdown_36m"] += 1
                continue
            if drawdown_36 > max_drawdown_cap:
                filter_rejections["drawdown_above_max_drawdown_r"] += 1
                continue
        if require_full_backtest_36 and not bool(row.get("has_full_backtest_36m")):
            filter_rejections["missing_full_backtest_36m"] += 1
            continue
        if (
            require_full_backtest_36
            and str(row.get("full_backtest_validation_status_36m") or "") != "valid"
        ):
            filter_rejections["invalid_full_backtest_36m"] += 1
            continue
        row_rejected = False
        for index, filter_spec in enumerate(normalized_field_filters):
            token = field_filter_tokens[index]
            direction = str(filter_spec.get("direction") or "lower").strip().lower()
            allow_missing = bool(filter_spec.get("allow_missing"))
            target = _safe_float(filter_spec.get("target"))
            if target is None:
                continue
            _field_name, field_value = _safe_metric_field(row, filter_spec)
            if field_value is None:
                if allow_missing:
                    continue
                filter_rejections[f"field_filter_missing_{token}"] += 1
                row_rejected = True
                break
            passes = False
            if direction == "lower":
                passes = field_value <= float(target)
            elif direction == "higher":
                passes = field_value >= float(target)
            elif direction == "equal":
                passes = abs(field_value - float(target)) <= 1e-9
            else:
                raise ValueError(f"Unsupported field filter direction: {direction}")
            if not passes:
                filter_rejections[f"field_filter_failed_{token}"] += 1
                row_rejected = True
                break
        if row_rejected:
            continue
        candidate_rows.append(row)
    return candidate_rows, filter_rejections, max_drawdown_cap


def _trade_rate_bonus(
    sleeve_spec: dict[str, Any], trades_per_month: Any
) -> tuple[float, float]:
    value = _safe_float(trades_per_month)
    if value is None or value <= 0.0:
        return 0.0, 0.0
    weight = max(0.0, float(sleeve_spec.get("trade_rate_bonus_weight", 0.0)))
    if weight <= 0.0:
        return 0.0, 0.0
    target = max(0.1, float(sleeve_spec.get("trade_rate_bonus_target", 8.0)))
    try:
        from math import log1p

        fraction = min(1.0, log1p(value) / log1p(target))
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0, 0.0
    return weight * fraction, fraction


def resolve_prefilter_limit(sleeve_spec: dict[str, Any]) -> int:
    explicit_limit = _safe_int(sleeve_spec.get("prefilter_limit"))
    if explicit_limit is not None and explicit_limit >= 0:
        return explicit_limit
    legacy_limit = _safe_int(sleeve_spec.get("candidate_limit"))
    if legacy_limit is not None and legacy_limit >= 0:
        return legacy_limit
    shortlist_size = max(1, int(sleeve_spec.get("shortlist_size", 12)))
    return max(64, shortlist_size * 8)


def build_prefiltered_candidate_rows(
    qualified_rows: list[dict[str, Any]],
    sleeve_spec: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    prefilter_limit = resolve_prefilter_limit(sleeve_spec)
    scalar_metric_terms = list(sleeve_spec.get("scalar_metric_terms") or [])
    ranked_rows: list[dict[str, Any]] = []
    for row in qualified_rows:
        drawdown_r = _safe_float(row.get("max_drawdown_r_36m"))
        drawdown_component = (
            float(sleeve_spec.get("drawdown_penalty", 0.65)) * float(drawdown_r)
            if drawdown_r is not None and float(sleeve_spec.get("drawdown_penalty", 0.65)) > 0.0
            else 0.0
        )
        trade_bonus_component, trade_bonus_fraction = _trade_rate_bonus(
            sleeve_spec, row.get("trades_per_month_36m")
        )
        scalar_metric_bonus_component, scalar_metric_bonus_terms = (
            compute_scalar_metric_bonus(row, scalar_metric_terms)
        )
        provisional_utility = (
            float(row.get("score_36m") or float("-inf"))
            + trade_bonus_component
            + scalar_metric_bonus_component
            - drawdown_component
        )
        ranked_row = dict(row)
        ranked_row["prefilter_utility"] = provisional_utility
        ranked_row["prefilter_score_component"] = float(
            row.get("score_36m") or float("-inf")
        )
        ranked_row["prefilter_drawdown_penalty_component"] = drawdown_component
        ranked_row["prefilter_trade_rate_bonus_component"] = trade_bonus_component
        ranked_row["prefilter_trade_rate_bonus_fraction"] = trade_bonus_fraction
        ranked_row["prefilter_scalar_metric_bonus_component"] = scalar_metric_bonus_component
        ranked_row["prefilter_scalar_metric_bonus_terms"] = scalar_metric_bonus_terms
        ranked_rows.append(ranked_row)
    ranked_rows.sort(
        key=lambda row: (
            -float(row.get("prefilter_utility") or float("-inf")),
            -float(_safe_float(row.get("score_36m")) or float("-inf")),
            -float(_safe_float(row.get("trades_per_month_36m")) or 0.0),
            str(row.get("attempt_id") or ""),
        )
    )
    retained_rows = ranked_rows[:prefilter_limit]
    for index, row in enumerate(retained_rows, start=1):
        row["prefilter_rank"] = index
    return retained_rows, prefilter_limit


def build_sleeve_prefilter(
    rows: list[dict[str, Any]],
    sleeve_spec: dict[str, Any],
) -> dict[str, Any]:
    qualified_rows, filter_rejections, max_drawdown_cap = filter_selection_candidate_rows(
        rows,
        candidate_limit=-1,
        min_score_36=float(sleeve_spec.get("min_score_36", 40.0)),
        min_retention_ratio=float(sleeve_spec.get("min_retention_ratio", 0.0)),
        min_trades_per_month=float(sleeve_spec.get("min_trades_per_month", 0.0)),
        max_drawdown_r=float(sleeve_spec.get("max_drawdown_r", -1.0)),
        require_full_backtest_36=bool(sleeve_spec.get("require_full_backtest_36", True)),
        field_filters=list(sleeve_spec.get("field_filters") or []),
    )
    candidate_rows, prefilter_limit = build_prefiltered_candidate_rows(
        qualified_rows, sleeve_spec
    )
    return {
        "name": sleeve_spec.get("name"),
        "spec": dict(sleeve_spec),
        "qualified_rows": qualified_rows,
        "candidate_rows": candidate_rows,
        "prefilter_limit": prefilter_limit,
        "prefilter_excluded_count": max(0, len(qualified_rows) - len(candidate_rows)),
        "filter_rejections": filter_rejections,
        "max_drawdown_cap": max_drawdown_cap,
    }


def finalize_sleeve_selection(
    prefilter_result: dict[str, Any],
    *,
    similarity_progress_callback: Callable[[dict[str, Any]], None] | None = None,
    similarity_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_rows = list(prefilter_result.get("candidate_rows") or [])
    sleeve_spec = dict(prefilter_result.get("spec") or {})
    effective_similarity_payload = (
        subset_similarity_payload(similarity_payload, candidate_rows)
        if similarity_payload is not None
        else build_candidate_similarity_payload(
            candidate_rows, progress_callback=similarity_progress_callback
        )
    )
    board = select_promotion_board(
        candidate_rows,
        effective_similarity_payload,
        board_size=int(sleeve_spec.get("shortlist_size", 12)),
        novelty_penalty=float(sleeve_spec.get("novelty_penalty", 18.0)),
        drawdown_penalty=float(sleeve_spec.get("drawdown_penalty", 0.65)),
        trade_rate_bonus_weight=float(sleeve_spec.get("trade_rate_bonus_weight", 0.0)),
        trade_rate_bonus_target=float(sleeve_spec.get("trade_rate_bonus_target", 8.0)),
        scalar_metric_terms=list(sleeve_spec.get("scalar_metric_terms") or []),
        max_drawdown_r=prefilter_result.get("max_drawdown_cap"),
        max_sameness_to_board=(
            None
            if float(sleeve_spec.get("max_sameness_to_board", 0.78)) < 0.0
            else float(sleeve_spec.get("max_sameness_to_board", 0.78))
        ),
        max_per_run=(
            None
            if int(sleeve_spec.get("max_per_run", 1)) < 0
            else int(sleeve_spec["max_per_run"])
        ),
        max_per_strategy_key=(
            None
            if int(sleeve_spec.get("max_per_strategy_key", 1)) < 0
            else int(sleeve_spec["max_per_strategy_key"])
        ),
    )
    selected_rows = [dict(row) for row in (board.get("selected") or [])]
    for rank, row in enumerate(selected_rows, start=1):
        row["sleeve_name"] = sleeve_spec.get("name")
        row["sleeve_selection_rank"] = rank
    return {
        **dict(prefilter_result),
        "similarity_payload": effective_similarity_payload,
        "board": board,
        "selected_rows": selected_rows,
    }


def build_sleeve_selection(
    rows: list[dict[str, Any]],
    sleeve_spec: dict[str, Any],
    similarity_progress_callback: Callable[[dict[str, Any]], None] | None = None,
    similarity_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prefilter_result = build_sleeve_prefilter(rows, sleeve_spec)
    return finalize_sleeve_selection(
        prefilter_result,
        similarity_progress_callback=similarity_progress_callback,
        similarity_payload=similarity_payload,
    )


def _merge_row_union(
    sleeve_results: list[dict[str, Any]],
    *,
    row_field: str,
    label_field: str,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for sleeve in sleeve_results:
        sleeve_name = str(sleeve.get("name") or "")
        for row in sleeve.get(row_field) or []:
            attempt_id = str(row.get("attempt_id") or "").strip()
            if not attempt_id:
                continue
            if attempt_id not in merged:
                merged[attempt_id] = dict(row)
                merged[attempt_id][label_field] = [sleeve_name]
                merged[attempt_id][f"{label_field}_count"] = 1
                continue
            labels = list(merged[attempt_id].get(label_field) or [])
            if sleeve_name not in labels:
                labels.append(sleeve_name)
            merged[attempt_id][label_field] = labels
            merged[attempt_id][f"{label_field}_count"] = len(labels)
    merged_rows = list(merged.values())
    merged_rows.sort(
        key=lambda row: (
            -int(row.get(f"{label_field}_count") or 0),
            -float(_safe_float(row.get("score_36m")) or float("-inf")),
            -float(_safe_float(row.get("trades_per_month_36m")) or 0.0),
            str(row.get("attempt_id") or ""),
        )
    )
    for index, row in enumerate(merged_rows, start=1):
        row["selection_rank"] = index
        row["portfolio_rank"] = index
    return merged_rows


def merge_portfolio_sleeves(sleeve_results: list[dict[str, Any]]) -> dict[str, Any]:
    selected_rows = _merge_row_union(
        sleeve_results, row_field="selected_rows", label_field="selected_by_sleeves"
    )
    candidate_rows = _merge_row_union(
        sleeve_results, row_field="candidate_rows", label_field="qualified_by_sleeves"
    )
    overlap_count = sum(
        1 for row in selected_rows if int(row.get("selected_by_sleeves_count") or 0) > 1
    )
    return {
        "selected_rows": selected_rows,
        "candidate_rows": candidate_rows,
        "selected_overlap_count": overlap_count,
        "selected_union_count": len(selected_rows),
        "candidate_union_count": len(candidate_rows),
    }
