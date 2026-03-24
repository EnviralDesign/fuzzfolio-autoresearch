from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import ScoreAdjustmentConfig


@dataclass
class AttemptScore:
    primary_score: float
    composite_score: float
    adjustments: dict[str, float]
    best_summary: dict[str, Any]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_summary(compare_payload: dict[str, Any]) -> dict[str, Any]:
    best = compare_payload.get("best")
    if isinstance(best, dict):
        return best
    ranked = compare_payload.get("ranked")
    if isinstance(ranked, list) and ranked and isinstance(ranked[0], dict):
        return ranked[0]
    raise ValueError("compare-sensitivity payload did not include a best or ranked summary.")


def build_attempt_score(
    compare_payload: dict[str, Any],
    adjustments_config: ScoreAdjustmentConfig,
) -> AttemptScore:
    best = _best_summary(compare_payload)
    primary_score = _safe_float(best.get("rank_score"))
    if primary_score is None:
        raise ValueError("compare-sensitivity summary did not contain rank_score.")

    adjustments: dict[str, float] = {}
    composite_score = primary_score

    best_cell = best.get("best_cell") or {}
    resolved_trades = int(best_cell.get("resolved_trades") or 0)
    if (
        adjustments_config.low_trade_count_threshold > 0
        and resolved_trades < adjustments_config.low_trade_count_threshold
    ):
        gap = adjustments_config.low_trade_count_threshold - resolved_trades
        ratio = gap / adjustments_config.low_trade_count_threshold
        penalty = -adjustments_config.low_trade_count_penalty * ratio
        adjustments["low_trade_count_penalty"] = penalty
        composite_score += penalty

    signal_count = int(best.get("signal_count") or 0)
    if (
        adjustments_config.low_signal_count_threshold > 0
        and signal_count < adjustments_config.low_signal_count_threshold
    ):
        gap = adjustments_config.low_signal_count_threshold - signal_count
        ratio = gap / adjustments_config.low_signal_count_threshold
        penalty = -adjustments_config.low_signal_count_penalty * ratio
        adjustments["low_signal_count_penalty"] = penalty
        composite_score += penalty

    matrix_summary = best.get("matrix_summary") or {}
    positive_ratio = _safe_float(matrix_summary.get("positive_cell_ratio")) or 0.0
    if (
        adjustments_config.low_positive_cell_ratio_threshold > 0
        and positive_ratio < adjustments_config.low_positive_cell_ratio_threshold
    ):
        gap = adjustments_config.low_positive_cell_ratio_threshold - positive_ratio
        ratio = gap / max(adjustments_config.low_positive_cell_ratio_threshold, 1e-9)
        penalty = -adjustments_config.low_positive_cell_ratio_penalty * ratio
        adjustments["low_positive_cell_ratio_penalty"] = penalty
        composite_score += penalty

    return AttemptScore(
        primary_score=primary_score,
        composite_score=composite_score,
        adjustments=adjustments,
        best_summary=best,
    )


def load_sensitivity_snapshot(artifact_dir: Path) -> dict[str, Any] | None:
    path = artifact_dir / "sensitivity-response.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
