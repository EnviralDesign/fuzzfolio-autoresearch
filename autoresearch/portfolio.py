from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .corpus_tools import (
    build_similarity_payload as build_candidate_similarity_payload,
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
}


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
    ],
}


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
    merged = {**DEFAULT_SLEEVE_SPEC, **dict(raw_spec)}
    name = str(merged.get("name") or "").strip()
    merged["name"] = name or f"sleeve-{index + 1}"
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


def filter_selection_candidate_rows(
    rows: list[dict[str, Any]],
    *,
    candidate_limit: int,
    min_score_36: float,
    min_retention_ratio: float,
    min_trades_per_month: float,
    max_drawdown_r: float,
    require_full_backtest_36: bool,
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
        provisional_utility = (
            float(row.get("score_36m") or float("-inf"))
            + trade_bonus_component
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
