from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CANONICAL_SCORE_LAB_VERSION = "score_lab_v2_5_3"


@dataclass
class AttemptScore:
    primary_score: float | None
    composite_score: float | None
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


def _find_mapping_by_key(payload: Any, key: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
        for nested in payload.values():
            found = _find_mapping_by_key(nested, key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_mapping_by_key(item, key)
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


def _extract_score_lab_payload(
    *,
    best_summary: dict[str, Any],
    compare_payload: dict[str, Any],
    sensitivity_snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
    preferred_paths = [
        ["score_lab"],
        ["score_lab_payload"],
        ["scoreLab"],
        ["scoreLabPayload"],
        ["best", "score_lab"],
        ["best", "score_lab_payload"],
        ["best", "scoreLab"],
        ["best", "scoreLabPayload"],
        ["data", "aggregate", "score_lab"],
        ["data", "aggregate", "scoreLab"],
        ["data", "score_lab"],
        ["data", "scoreLab"],
        ["aggregate", "score_lab"],
        ["aggregate", "scoreLab"],
    ]
    for source in [best_summary, compare_payload, sensitivity_snapshot]:
        if not isinstance(source, dict):
            continue
        for path in preferred_paths:
            value = _get_nested(source, path)
            if isinstance(value, dict):
                return value
        found = _find_mapping_by_key(source, "score_lab")
        if found is not None:
            return found
    return None


def build_attempt_score(
    compare_payload: dict[str, Any],
    sensitivity_snapshot: dict[str, Any] | None = None,
) -> AttemptScore:
    best = _best_summary(compare_payload)
    score_lab_payload = _extract_score_lab_payload(
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
    )
    score_lab_version = (
        str(score_lab_payload.get("version") or "").strip()
        if isinstance(score_lab_payload, dict)
        else ""
    )
    score_lab_score = (
        _safe_float(score_lab_payload.get("score"))
        if isinstance(score_lab_payload, dict)
        else None
    )
    legacy_quality_score = _extract_metric(
        "quality_score",
        best_summary=best,
        compare_payload=compare_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        preferred_paths=[
            ["quality_score"],
            ["quality_score", "score"],
            ["data", "aggregate", "quality_score"],
            ["data", "aggregate", "quality_score", "score"],
            ["data", "quality_score"],
            ["data", "quality_score", "score"],
        ],
    )
    legacy_quality_score_version = (
        _get_nested(best, ["quality_score_version"])
        or _get_nested(compare_payload, ["quality_score_version"])
        or _get_nested(sensitivity_snapshot or {}, ["data", "aggregate", "quality_score", "version"])
        or _get_nested(sensitivity_snapshot or {}, ["data", "quality_score", "version"])
    )
    quality_score_belief_basis = (
        _get_nested(best, ["quality_score_belief_basis"])
        or _get_nested(compare_payload, ["quality_score_belief_basis"])
        or _get_nested(sensitivity_snapshot or {}, ["data", "aggregate", "quality_score", "belief_basis"])
        or _get_nested(sensitivity_snapshot or {}, ["data", "quality_score", "belief_basis"])
    )
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
        "score_lab": score_lab_score,
        "legacy_quality_score": legacy_quality_score,
        "quality_score": legacy_quality_score,
        "dsr": dsr,
        "psr": psr,
        "k_ratio": k_ratio,
        "sharpe_r": sharpe_r,
    }
    composite_score = (
        score_lab_score
        if score_lab_version == CANONICAL_SCORE_LAB_VERSION and score_lab_score is not None
        else None
    )
    if composite_score is not None:
        combiner = (
            str(score_lab_payload.get("combiner") or "").strip()
            if isinstance(score_lab_payload, dict)
            else ""
        )
        score_basis = f"{CANONICAL_SCORE_LAB_VERSION}:{combiner or 'canonical'}"
    elif score_lab_version:
        score_basis = f"stale_score_lab:{score_lab_version}"
    elif legacy_quality_score is not None:
        version_text = (
            str(legacy_quality_score_version).strip()
            if isinstance(legacy_quality_score_version, str) and legacy_quality_score_version.strip()
            else "quality"
        )
        belief_text = (
            str(quality_score_belief_basis).strip()
            if isinstance(quality_score_belief_basis, str) and quality_score_belief_basis.strip()
            else "unknown"
        )
        score_basis = f"missing_{CANONICAL_SCORE_LAB_VERSION}:{version_text}:{belief_text}"
    else:
        score_basis = "unscored"

    return AttemptScore(
        primary_score=composite_score,
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
