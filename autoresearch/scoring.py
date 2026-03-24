from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass
class AttemptScore:
    primary_score: float
    composite_score: float
    score_basis: str
    metrics: dict[str, float | None]
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


def _get_nested(payload: dict[str, Any] | None, path: list[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _find_numeric_by_key(payload: Any, key: str) -> float | None:
    if isinstance(payload, dict):
        if key in payload:
            value = _safe_float(payload.get(key))
            if value is not None:
                return value
        for value in payload.values():
            found = _find_numeric_by_key(value, key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_numeric_by_key(item, key)
            if found is not None:
                return found
    return None


def _extract_metric(
    key: str,
    *,
    best_summary: dict[str, Any],
    compare_payload: dict[str, Any],
    sensitivity_snapshot: dict[str, Any] | None,
    preferred_paths: list[list[str]],
) -> float | None:
    for source in [best_summary, compare_payload, sensitivity_snapshot]:
        if not isinstance(source, dict):
            continue
        for path in preferred_paths:
            value = _safe_float(_get_nested(source, path))
            if value is not None:
                return value
        found = _find_numeric_by_key(source, key)
        if found is not None:
            return found
    return None


def build_attempt_score(
    compare_payload: dict[str, Any],
    sensitivity_snapshot: dict[str, Any] | None = None,
) -> AttemptScore:
    best = _best_summary(compare_payload)
    rank_score = _safe_float(best.get("rank_score"))
    if rank_score is None:
        raise ValueError("compare-sensitivity summary did not contain rank_score.")

    psr = _extract_metric(
        "psr",
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        preferred_paths=[
            ["best_cell_path_metrics", "psr"],
            ["data", "aggregate", "best_cell_path_metrics", "psr"],
            ["data", "best_cell_path_metrics", "psr"],
        ],
    )
    dsr = _extract_metric(
        "dsr",
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        preferred_paths=[
            ["dsr"],
            ["data", "aggregate", "dsr"],
            ["data", "dsr"],
            ["aggregate", "dsr"],
        ],
    )
    k_ratio = _extract_metric(
        "k_ratio",
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        preferred_paths=[
            ["best_cell_path_metrics", "k_ratio"],
            ["data", "aggregate", "best_cell_path_metrics", "k_ratio"],
            ["data", "best_cell_path_metrics", "k_ratio"],
        ],
    )
    sharpe_r = _extract_metric(
        "sharpe_r",
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        preferred_paths=[
            ["best_cell_path_metrics", "sharpe_r"],
            ["data", "aggregate", "best_cell_path_metrics", "sharpe_r"],
            ["data", "best_cell_path_metrics", "sharpe_r"],
        ],
    )
    metrics = {
        "rank_score": rank_score,
        "dsr": dsr,
        "psr": psr,
        "k_ratio": k_ratio,
        "sharpe_r": sharpe_r,
    }
    if dsr is not None:
        composite_score = dsr
        score_basis = "dsr"
    elif psr is not None:
        composite_score = psr
        score_basis = "psr"
    else:
        composite_score = rank_score
        score_basis = "rank_score"

    return AttemptScore(
        primary_score=rank_score,
        composite_score=composite_score,
        score_basis=score_basis,
        metrics=metrics,
        best_summary=best,
    )


def load_sensitivity_snapshot(artifact_dir: Path) -> dict[str, Any] | None:
    path = artifact_dir / "sensitivity-response.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
