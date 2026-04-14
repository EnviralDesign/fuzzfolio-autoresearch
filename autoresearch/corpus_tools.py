from __future__ import annotations

import csv
import json
from math import log1p
from pathlib import Path
from typing import Any, Callable

from .scoring import build_attempt_score


SCRUTINY_CACHE_DIRNAME = "scrutiny-cache"
FULL_BACKTEST_CURVE_FILENAME = "full-backtest-36mo-curve.json"
FULL_BACKTEST_RESULT_FILENAME = "full-backtest-36mo-result.json"


def load_json_if_exists(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return None


def nested_get(payload: Any, path: list[str]) -> Any:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def normalize_tokens(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        token = str(raw or "").strip().upper()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def strategy_key(timeframe: Any, instruments: list[Any] | None) -> str | None:
    timeframe_token = str(timeframe or "").strip().upper()
    instrument_tokens = normalize_tokens(list(instruments or []))
    if not timeframe_token and not instrument_tokens:
        return None
    if timeframe_token and instrument_tokens:
        return f"{timeframe_token}|{','.join(instrument_tokens)}"
    if timeframe_token:
        return timeframe_token
    return ",".join(instrument_tokens)


def scrutiny_cache_dir_for_artifact_dir(
    artifact_dir: Path, lookback_months: int
) -> Path:
    return artifact_dir / SCRUTINY_CACHE_DIRNAME / f"{int(lookback_months)}mo"


def scrutiny_manifest_path_for_artifact_dir(
    artifact_dir: Path, lookback_months: int
) -> Path:
    return scrutiny_cache_dir_for_artifact_dir(artifact_dir, lookback_months) / "manifest.json"


def legacy_validation_cache_dir(
    validation_cache_root: Path, run_id: str, lookback_months: int
) -> Path:
    return validation_cache_root / run_id / f"{int(lookback_months)}mo"


def legacy_validation_manifest_path(
    validation_cache_root: Path, run_id: str, lookback_months: int
) -> Path:
    return legacy_validation_cache_dir(validation_cache_root, run_id, lookback_months) / "manifest.json"


def attempt_artifact_dir(attempt: dict[str, Any]) -> Path | None:
    artifact_dir_raw = str(attempt.get("artifact_dir") or "").strip()
    if not artifact_dir_raw:
        return None
    return Path(artifact_dir_raw).resolve()


def attempt_profile_path(attempt: dict[str, Any]) -> Path | None:
    profile_path_raw = str(attempt.get("profile_path") or "").strip()
    if not profile_path_raw:
        return None
    return Path(profile_path_raw).resolve()


def attempt_request_payload(attempt: dict[str, Any]) -> dict[str, Any]:
    artifact_dir = attempt_artifact_dir(attempt)
    if artifact_dir is None:
        return {}
    payload = load_json_if_exists(artifact_dir / "deep-replay-job.json")
    request_payload = payload.get("request") if isinstance(payload, dict) else None
    return request_payload if isinstance(request_payload, dict) else {}


def _profile_payload(attempt: dict[str, Any]) -> dict[str, Any]:
    profile_path = attempt_profile_path(attempt)
    if profile_path is None:
        return {}
    payload = load_json_if_exists(profile_path)
    return payload if isinstance(payload, dict) else {}


def attempt_instruments(attempt: dict[str, Any]) -> list[str]:
    request_payload = attempt_request_payload(attempt)
    instruments = request_payload.get("instruments")
    if isinstance(instruments, list):
        normalized = normalize_tokens(instruments)
        if normalized:
            return normalized

    profile_payload = _profile_payload(attempt)
    from_profile = nested_get(profile_payload, ["profile", "instruments"])
    if isinstance(from_profile, list):
        normalized = normalize_tokens(from_profile)
        if normalized:
            return normalized

    best_summary = attempt.get("best_summary")
    if isinstance(best_summary, dict):
        token = str(best_summary.get("instrument") or "").strip().upper()
        if token and token != "__BASKET__":
            return [token]
    return []


def attempt_timeframe(attempt: dict[str, Any]) -> str | None:
    request_payload = attempt_request_payload(attempt)
    token = str(request_payload.get("timeframe") or "").strip().upper()
    if token:
        return token

    best_summary = attempt.get("best_summary")
    if isinstance(best_summary, dict):
        token = str(best_summary.get("timeframe") or "").strip().upper()
        if token:
            return token

    profile_payload = _profile_payload(attempt)
    indicators = nested_get(profile_payload, ["profile", "indicators"])
    if isinstance(indicators, list):
        seen: list[str] = []
        for indicator in indicators:
            if not isinstance(indicator, dict):
                continue
            token = str(nested_get(indicator, ["config", "timeframe"]) or "").strip().upper()
            if token and token not in seen:
                seen.append(token)
        if seen:
            return seen[0]
    return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_scalar_metric_bonus(
    row: dict[str, Any], scalar_metric_terms: list[dict[str, Any]] | None
) -> tuple[float, list[dict[str, Any]]]:
    total = 0.0
    resolved_terms: list[dict[str, Any]] = []
    for index, term in enumerate(list(scalar_metric_terms or []), start=1):
        if not isinstance(term, dict):
            continue
        weight = _safe_float(term.get("weight"))
        if weight is None or weight <= 0.0:
            continue
        direction = str(term.get("direction") or "higher").strip().lower()
        name = str(term.get("name") or f"metric-{index}").strip() or f"metric-{index}"
        field_candidates: list[str] = []
        field = str(term.get("field") or "").strip()
        if field:
            field_candidates.append(field)
        raw_candidates = term.get("field_candidates")
        if isinstance(raw_candidates, list):
            for candidate in raw_candidates:
                token = str(candidate or "").strip()
                if token and token not in field_candidates:
                    field_candidates.append(token)
        if not field_candidates:
            continue
        used_field = None
        raw_value = None
        for candidate in field_candidates:
            value = _safe_float(row.get(candidate))
            if value is None:
                continue
            used_field = candidate
            raw_value = value
            break
        if used_field is None or raw_value is None:
            continue
        target = _safe_float(term.get("target"))
        baseline = target if target is not None and target > 0.0 else 1.0
        if direction == "lower":
            fraction = 1.0 - (raw_value / baseline)
        else:
            fraction = raw_value / baseline
        fraction = max(0.0, min(1.0, fraction))
        component = float(weight) * fraction
        total += component
        resolved_terms.append(
            {
                "name": name,
                "field": used_field,
                "raw_value": raw_value,
                "direction": "lower" if direction == "lower" else "higher",
                "target": baseline,
                "fraction": fraction,
                "component": component,
                "weight": float(weight),
            }
        )
    return total, resolved_terms


def _best_summary(attempt: dict[str, Any]) -> dict[str, Any]:
    best_summary = attempt.get("best_summary")
    return best_summary if isinstance(best_summary, dict) else {}


def _metric_value(payload: dict[str, Any], *path: str) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _safe_float(current)


def attempt_trade_count(attempt: dict[str, Any]) -> int | None:
    best_summary = _best_summary(attempt)
    candidates = [
        best_summary.get("best_cell_path_metrics"),
        best_summary.get("best_cell"),
    ]
    for payload in candidates:
        if not isinstance(payload, dict):
            continue
        raw = payload.get("trade_count")
        if raw is None:
            raw = payload.get("resolved_trades")
        try:
            trade_count = int(raw)
        except (TypeError, ValueError):
            continue
        if trade_count >= 0:
            return trade_count
    return None


def attempt_effective_window_months(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    quality_score_payload = best_summary.get("quality_score_payload")
    if isinstance(quality_score_payload, dict):
        inputs = quality_score_payload.get("inputs")
        if isinstance(inputs, dict):
            value = _safe_float(inputs.get("effective_window_months"))
            if value is not None and value > 0:
                return value
    market_window = best_summary.get("market_data_window")
    if isinstance(market_window, dict):
        value = _safe_float(market_window.get("effective_window_months"))
        if value is not None and value > 0:
            return value
    return None


def attempt_trades_per_month(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    quality_score_payload = best_summary.get("quality_score_payload")
    if isinstance(quality_score_payload, dict):
        inputs = quality_score_payload.get("inputs")
        if isinstance(inputs, dict):
            value = _safe_float(inputs.get("trades_per_month"))
            if value is not None and value >= 0:
                return value

    trade_count = attempt_trade_count(attempt)
    effective_window_months = attempt_effective_window_months(attempt)
    if (
        trade_count is None
        or effective_window_months is None
        or effective_window_months <= 0
    ):
        return None
    return float(trade_count) / float(effective_window_months)


def attempt_max_drawdown_r(attempt: dict[str, Any]) -> float | None:
    best_summary = _best_summary(attempt)
    candidates = [
        best_summary.get("best_cell_path_metrics"),
        best_summary.get("quality_score_payload"),
    ]
    for payload in candidates:
        if not isinstance(payload, dict):
            continue
        value = _metric_value(payload, "max_drawdown_r")
        if value is not None:
            return value
        value = _metric_value(payload, "inputs", "max_drawdown_r")
        if value is not None:
            return value
    return None


def load_scored_sensitivity_result(result_path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(result_path)
    if not isinstance(payload, dict):
        return None
    aggregate = nested_get(payload, ["data", "aggregate"])
    if not isinstance(aggregate, dict):
        aggregate = nested_get(payload, ["data"])
    compare_payload = {"best": aggregate or {}}
    score = build_attempt_score(compare_payload, payload)
    synthetic_attempt = {
        "best_summary": score.best_summary,
        "composite_score": score.composite_score,
    }
    return {
        "score": score.composite_score,
        "score_basis": score.score_basis,
        "metrics": score.metrics,
        "best_summary": score.best_summary,
        "trade_count": attempt_trade_count(synthetic_attempt),
        "trades_per_month": attempt_trades_per_month(synthetic_attempt),
        "effective_window_months": attempt_effective_window_months(synthetic_attempt),
        "max_drawdown_r": attempt_max_drawdown_r(synthetic_attempt),
    }


def _cell_value(payload: dict[str, Any] | None, key: str) -> float | None:
    if not isinstance(payload, dict):
        return None
    return _safe_float(payload.get(key))


def validate_full_backtest_artifacts(attempt: dict[str, Any]) -> dict[str, Any]:
    artifact_dir = attempt_artifact_dir(attempt)
    if artifact_dir is None:
        return {
            "status": "missing",
            "issues": ["missing_artifact_dir"],
            "result_exists": False,
            "curve_exists": False,
            "curve_point_count": 0,
            "analysis_status": None,
            "cell_match": None,
            "result_path": None,
            "curve_path": None,
        }

    result_path = artifact_dir / FULL_BACKTEST_RESULT_FILENAME
    curve_path = artifact_dir / FULL_BACKTEST_CURVE_FILENAME
    result_exists = result_path.exists()
    curve_exists = curve_path.exists()
    issues: list[str] = []

    if not result_exists and not curve_exists:
        return {
            "status": "missing",
            "issues": [],
            "result_exists": False,
            "curve_exists": False,
            "curve_point_count": 0,
            "analysis_status": None,
            "cell_match": None,
            "result_path": str(result_path),
            "curve_path": str(curve_path),
        }
    if not result_exists:
        issues.append("missing_result_file")
    if not curve_exists:
        issues.append("missing_curve_file")

    result_payload = load_json_if_exists(result_path) if result_exists else None
    curve_payload = load_json_if_exists(curve_path) if curve_exists else None

    if result_exists and not isinstance(result_payload, dict):
        issues.append("invalid_result_json")
    if curve_exists and not isinstance(curve_payload, dict):
        issues.append("invalid_curve_json")

    aggregate = nested_get(result_payload, ["data", "aggregate"])
    if not isinstance(aggregate, dict):
        aggregate = nested_get(result_payload, ["data"])
    if result_exists and not isinstance(aggregate, dict):
        issues.append("missing_result_aggregate")

    curve_points = nested_get(curve_payload, ["curve", "points"])
    if curve_exists and not isinstance(curve_points, list):
        issues.append("missing_curve_points")
        curve_points = []
    elif curve_exists and len(curve_points) == 0:
        issues.append("empty_curve_points")

    curve_cell = None
    if isinstance(curve_payload, dict):
        curve_cell = curve_payload.get("cell")
        if not isinstance(curve_cell, dict):
            curve_cell = nested_get(curve_payload, ["curve", "cell"])
    best_cell = aggregate.get("best_cell") if isinstance(aggregate, dict) else None
    if curve_exists and not isinstance(curve_cell, dict):
        issues.append("missing_curve_cell")

    cell_match: bool | None = None
    if isinstance(best_cell, dict) and isinstance(curve_cell, dict):
        compared_any = False
        mismatched = False
        for key in ("reward_multiple", "stop_loss_percent", "take_profit_percent"):
            left = _cell_value(best_cell, key)
            right = _cell_value(curve_cell, key)
            if left is None or right is None:
                continue
            compared_any = True
            if abs(left - right) > 1e-9:
                mismatched = True
        if compared_any:
            cell_match = not mismatched
            if mismatched:
                issues.append("best_cell_mismatch")

    analysis_status = (
        str(aggregate.get("analysis_status") or "").strip().lower()
        if isinstance(aggregate, dict)
        else None
    )
    if analysis_status and analysis_status != "success":
        issues.append(f"analysis_status:{analysis_status}")

    status = "valid" if not issues else "invalid"
    return {
        "status": status,
        "issues": issues,
        "result_exists": result_exists,
        "curve_exists": curve_exists,
        "curve_point_count": len(curve_points) if isinstance(curve_points, list) else 0,
        "analysis_status": analysis_status or None,
        "cell_match": cell_match,
        "result_path": str(result_path),
        "curve_path": str(curve_path),
    }


def _request_payload_for_source(
    attempt: dict[str, Any],
    *,
    source_job_path: Path | None,
) -> dict[str, Any]:
    if source_job_path is not None:
        payload = load_json_if_exists(source_job_path)
        request_payload = payload.get("request") if isinstance(payload, dict) else None
        if isinstance(request_payload, dict):
            return request_payload
    return attempt_request_payload(attempt)


def _legacy_manifest_matches_attempt(
    manifest_payload: dict[str, Any],
    attempt: dict[str, Any],
    lookback_months: int,
) -> bool:
    if not manifest_payload:
        return False
    if str(manifest_payload.get("attempt_id") or "") != str(attempt.get("attempt_id") or ""):
        return False
    if int(manifest_payload.get("lookback_months") or 0) != int(lookback_months):
        return False
    return True


def resolve_attempt_scrutiny_source(
    attempt: dict[str, Any],
    lookback_months: int,
    *,
    validation_cache_root: Path | None = None,
) -> dict[str, Any]:
    artifact_dir = attempt_artifact_dir(attempt)
    if artifact_dir is None:
        return {"available": False, "source": None}

    cache_dir = scrutiny_cache_dir_for_artifact_dir(artifact_dir, lookback_months)
    cache_result_path = cache_dir / "sensitivity-response.json"
    cache_curve_path = cache_dir / "best-cell-path-detail.json"
    cache_job_path = cache_dir / "deep-replay-job.json"
    if cache_result_path.exists() and cache_curve_path.exists():
        summary = load_scored_sensitivity_result(cache_result_path) or {}
        request_payload = _request_payload_for_source(
            attempt, source_job_path=cache_job_path if cache_job_path.exists() else None
        )
        return {
            "available": True,
            "source": "attempt_scrutiny_cache",
            "artifact_dir": str(cache_dir),
            "result_path": str(cache_result_path),
            "curve_path": str(cache_curve_path),
            "job_path": str(cache_job_path) if cache_job_path.exists() else None,
            "manifest_path": str(cache_dir / "manifest.json"),
            "timeframe": str(request_payload.get("timeframe") or attempt_timeframe(attempt) or "").strip().upper()
            or None,
            "instruments": normalize_tokens(list(request_payload.get("instruments") or attempt_instruments(attempt))),
            **summary,
        }

    if int(lookback_months) == 36:
        full_result_path = artifact_dir / FULL_BACKTEST_RESULT_FILENAME
        full_curve_path = artifact_dir / FULL_BACKTEST_CURVE_FILENAME
        if full_result_path.exists() and full_curve_path.exists():
            summary = load_scored_sensitivity_result(full_result_path) or {}
            request_payload = _request_payload_for_source(
                attempt,
                source_job_path=(artifact_dir / "deep-replay-job.json")
                if (artifact_dir / "deep-replay-job.json").exists()
                else None,
            )
            return {
                "available": True,
                "source": "full_backtest",
                "artifact_dir": str(artifact_dir),
                "result_path": str(full_result_path),
                "curve_path": str(full_curve_path),
                "job_path": str(artifact_dir / "deep-replay-job.json")
                if (artifact_dir / "deep-replay-job.json").exists()
                else None,
                "manifest_path": None,
                "timeframe": str(request_payload.get("timeframe") or attempt_timeframe(attempt) or "").strip().upper()
                or None,
                "instruments": normalize_tokens(list(request_payload.get("instruments") or attempt_instruments(attempt))),
                **summary,
            }

    if validation_cache_root is not None:
        run_id = str(attempt.get("run_id") or "").strip()
        if run_id:
            legacy_dir = legacy_validation_cache_dir(
                validation_cache_root, run_id, lookback_months
            )
            legacy_manifest_path = legacy_dir / "manifest.json"
            legacy_result_path = legacy_dir / "sensitivity-response.json"
            legacy_curve_path = legacy_dir / "best-cell-path-detail.json"
            if (
                legacy_manifest_path.exists()
                and legacy_result_path.exists()
                and legacy_curve_path.exists()
            ):
                manifest_payload = load_json_if_exists(legacy_manifest_path)
                if isinstance(manifest_payload, dict) and _legacy_manifest_matches_attempt(
                    manifest_payload, attempt, lookback_months
                ):
                    summary = load_scored_sensitivity_result(legacy_result_path) or {}
                    request_payload = _request_payload_for_source(
                        attempt,
                        source_job_path=(legacy_dir / "deep-replay-job.json")
                        if (legacy_dir / "deep-replay-job.json").exists()
                        else None,
                    )
                    return {
                        "available": True,
                        "source": "legacy_run_validation_cache",
                        "artifact_dir": str(legacy_dir),
                        "result_path": str(legacy_result_path),
                        "curve_path": str(legacy_curve_path),
                        "job_path": str(legacy_dir / "deep-replay-job.json")
                        if (legacy_dir / "deep-replay-job.json").exists()
                        else None,
                        "manifest_path": str(legacy_manifest_path),
                        "timeframe": str(request_payload.get("timeframe") or attempt_timeframe(attempt) or "").strip().upper()
                        or None,
                        "instruments": normalize_tokens(list(request_payload.get("instruments") or attempt_instruments(attempt))),
                        **summary,
                    }

    return {"available": False, "source": None}


def extract_attempt_catalog_row(
    attempt: dict[str, Any],
    run_metadata: dict[str, Any] | None,
    *,
    validation_cache_root: Path | None = None,
) -> dict[str, Any]:
    artifact_dir = attempt_artifact_dir(attempt)
    profile_path = attempt_profile_path(attempt)
    request_payload = attempt_request_payload(attempt)
    instruments = attempt_instruments(attempt)
    timeframe = attempt_timeframe(attempt)
    scrutiny_12 = resolve_attempt_scrutiny_source(
        attempt, 12, validation_cache_root=validation_cache_root
    )
    scrutiny_36 = resolve_attempt_scrutiny_source(
        attempt, 36, validation_cache_root=validation_cache_root
    )
    score_12 = _safe_float(scrutiny_12.get("score"))
    score_36 = _safe_float(scrutiny_36.get("score"))
    base_strategy_key = strategy_key(timeframe, instruments)
    strategy_key_12m = strategy_key(
        scrutiny_12.get("timeframe"),
        list(scrutiny_12.get("instruments") or []),
    )
    strategy_key_36m = strategy_key(
        scrutiny_36.get("timeframe"),
        list(scrutiny_36.get("instruments") or []),
    )
    full_backtest_validation = validate_full_backtest_artifacts(attempt)

    return {
        "run_id": str(attempt.get("run_id") or ""),
        "attempt_id": str(attempt.get("attempt_id") or ""),
        "sequence": attempt.get("sequence"),
        "created_at": attempt.get("created_at"),
        "candidate_name": attempt.get("candidate_name"),
        "artifact_dir": str(artifact_dir) if artifact_dir is not None else None,
        "profile_ref": attempt.get("profile_ref"),
        "profile_path": str(profile_path) if profile_path is not None else None,
        "composite_score": attempt.get("composite_score"),
        "score_basis": attempt.get("score_basis"),
        "explorer_model": (run_metadata or {}).get("explorer_model"),
        "explorer_profile": (run_metadata or {}).get("explorer_profile"),
        "requested_timeframe": attempt.get("requested_timeframe"),
        "effective_timeframe": attempt.get("effective_timeframe"),
        "base_timeframe": timeframe,
        "base_instruments": instruments,
        "base_strategy_key": base_strategy_key,
        "instrument_count": len(instruments),
        "requested_horizon_months": attempt.get("requested_horizon_months"),
        "effective_window_months": attempt.get("effective_window_months"),
        "resolved_trades": attempt.get("resolved_trades") or attempt_trade_count(attempt),
        "trades_per_month": attempt.get("trades_per_month") or attempt_trades_per_month(attempt),
        "positive_cell_ratio": attempt.get("positive_cell_ratio"),
        "max_drawdown_r": attempt_max_drawdown_r(attempt),
        "has_sensitivity_response": bool(
            artifact_dir is not None and (artifact_dir / "sensitivity-response.json").exists()
        ),
        "has_best_cell_curve": bool(
            artifact_dir is not None and (artifact_dir / "best-cell-path-detail.json").exists()
        ),
        "has_deep_replay_job": bool(
            artifact_dir is not None and (artifact_dir / "deep-replay-job.json").exists()
        ),
        "has_profile_file": bool(profile_path is not None and profile_path.exists()),
        "request_instruments": normalize_tokens(list(request_payload.get("instruments") or [])),
        "request_timeframe": str(request_payload.get("timeframe") or "").strip().upper()
        or None,
        "scrutiny_source_12m": scrutiny_12.get("source"),
        "scrutiny_result_path_12m": scrutiny_12.get("result_path"),
        "scrutiny_curve_path_12m": scrutiny_12.get("curve_path"),
        "score_12m": score_12,
        "score_basis_12m": scrutiny_12.get("score_basis"),
        "trade_count_12m": scrutiny_12.get("trade_count"),
        "trades_per_month_12m": scrutiny_12.get("trades_per_month"),
        "effective_window_months_12m": scrutiny_12.get("effective_window_months"),
        "max_drawdown_r_12m": scrutiny_12.get("max_drawdown_r"),
        "timeframe_12m": scrutiny_12.get("timeframe"),
        "instruments_12m": scrutiny_12.get("instruments") or [],
        "strategy_key_12m": strategy_key_12m,
        "has_scrutiny_12m": bool(scrutiny_12.get("available")),
        "scrutiny_source_36m": scrutiny_36.get("source"),
        "scrutiny_result_path_36m": scrutiny_36.get("result_path"),
        "scrutiny_curve_path_36m": scrutiny_36.get("curve_path"),
        "score_36m": score_36,
        "score_basis_36m": scrutiny_36.get("score_basis"),
        "trade_count_36m": scrutiny_36.get("trade_count"),
        "trades_per_month_36m": scrutiny_36.get("trades_per_month"),
        "effective_window_months_36m": scrutiny_36.get("effective_window_months"),
        "max_drawdown_r_36m": scrutiny_36.get("max_drawdown_r"),
        "timeframe_36m": scrutiny_36.get("timeframe"),
        "instruments_36m": scrutiny_36.get("instruments") or [],
        "strategy_key_36m": strategy_key_36m,
        "has_scrutiny_36m": bool(scrutiny_36.get("available")),
        "has_full_backtest_36m": bool(full_backtest_validation.get("result_exists"))
        and bool(full_backtest_validation.get("curve_exists")),
        "has_full_backtest_result_36m": bool(full_backtest_validation.get("result_exists")),
        "has_full_backtest_curve_36m": bool(full_backtest_validation.get("curve_exists")),
        "full_backtest_result_path_36m": full_backtest_validation.get("result_path"),
        "full_backtest_curve_path_36m": full_backtest_validation.get("curve_path"),
        "full_backtest_validation_status_36m": full_backtest_validation.get("status"),
        "full_backtest_validation_issue_count_36m": len(
            list(full_backtest_validation.get("issues") or [])
        ),
        "full_backtest_validation_issues_36m": list(
            full_backtest_validation.get("issues") or []
        ),
        "full_backtest_curve_point_count_36m": full_backtest_validation.get(
            "curve_point_count"
        ),
        "full_backtest_analysis_status_36m": full_backtest_validation.get(
            "analysis_status"
        ),
        "full_backtest_cell_match_36m": full_backtest_validation.get("cell_match"),
        "score_delta_36m_minus_12m": (
            score_36 - score_12 if score_36 is not None and score_12 is not None else None
        ),
        "score_retention_ratio_36m_vs_12m": (
            (score_36 / score_12)
            if score_36 is not None and score_12 not in {None, 0.0, -0.0}
            else None
        ),
    }


def catalog_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scored_rows = [row for row in rows if _safe_float(row.get("composite_score")) is not None]
    scrutiny_36 = [row for row in rows if row.get("has_scrutiny_36m")]
    full_backtest_36 = [row for row in rows if row.get("has_full_backtest_36m")]
    valid_full_backtest_36 = [
        row
        for row in rows
        if str(row.get("full_backtest_validation_status_36m") or "") == "valid"
    ]
    invalid_full_backtest_36 = [
        row
        for row in rows
        if str(row.get("full_backtest_validation_status_36m") or "") == "invalid"
    ]
    partial_full_backtest_36 = [
        row
        for row in rows
        if bool(row.get("has_full_backtest_result_36m"))
        != bool(row.get("has_full_backtest_curve_36m"))
    ]
    score_36_values = [
        float(row["score_36m"])
        for row in scrutiny_36
        if _safe_float(row.get("score_36m")) is not None
    ]
    score_36_values.sort()
    median_score_36 = (
        score_36_values[len(score_36_values) // 2] if score_36_values else None
    )
    run_ids = {str(row.get("run_id") or "").strip() for row in rows if str(row.get("run_id") or "").strip()}
    base_strategy_keys = {
        str(row.get("base_strategy_key") or "").strip()
        for row in rows
        if str(row.get("base_strategy_key") or "").strip()
    }
    strategy_keys_36 = {
        str(row.get("strategy_key_36m") or "").strip()
        for row in scrutiny_36
        if str(row.get("strategy_key_36m") or "").strip()
    }
    full_backtest_strategy_keys_36 = {
        str(row.get("strategy_key_36m") or "").strip()
        for row in full_backtest_36
        if str(row.get("strategy_key_36m") or "").strip()
    }
    attempt_count = len(rows)
    scrutiny_36_count = len(scrutiny_36)
    full_backtest_36_count = len(full_backtest_36)
    return {
        "run_count": len(run_ids),
        "attempt_count": attempt_count,
        "scored_attempt_count": len(scored_rows),
        "unique_base_strategy_count": len(base_strategy_keys),
        "unique_strategy_count_36m": len(strategy_keys_36),
        "unique_full_backtest_strategy_count_36m": len(full_backtest_strategy_keys_36),
        "attempts_with_scrutiny_12m": sum(
            1 for row in rows if row.get("has_scrutiny_12m")
        ),
        "attempts_with_scrutiny_36m": scrutiny_36_count,
        "attempts_with_full_backtest_36m": full_backtest_36_count,
        "attempts_with_valid_full_backtest_36m": len(valid_full_backtest_36),
        "attempts_with_invalid_full_backtest_36m": len(invalid_full_backtest_36),
        "attempts_with_partial_full_backtest_36m": len(partial_full_backtest_36),
        "attempts_with_base_sensitivity": sum(
            1 for row in rows if row.get("has_sensitivity_response")
        ),
        "scrutiny_36m_coverage_ratio": (
            (float(scrutiny_36_count) / float(attempt_count)) if attempt_count > 0 else None
        ),
        "full_backtest_36m_coverage_ratio": (
            (float(full_backtest_36_count) / float(attempt_count))
            if attempt_count > 0
            else None
        ),
        "valid_full_backtest_36m_coverage_ratio": (
            (float(len(valid_full_backtest_36)) / float(attempt_count))
            if attempt_count > 0
            else None
        ),
        "full_backtest_36m_vs_scrutiny_coverage_ratio": (
            (float(full_backtest_36_count) / float(scrutiny_36_count))
            if scrutiny_36_count > 0
            else None
        ),
        "valid_full_backtest_36m_vs_scrutiny_coverage_ratio": (
            (float(len(valid_full_backtest_36)) / float(scrutiny_36_count))
            if scrutiny_36_count > 0
            else None
        ),
        "median_score_36m": median_score_36,
        "score_36m_ge_40": sum(
            1
            for row in scrutiny_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 40.0
        ),
        "score_36m_ge_60": sum(
            1
            for row in scrutiny_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 60.0
        ),
        "score_36m_ge_70": sum(
            1
            for row in scrutiny_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 70.0
        ),
        "full_backtest_36m_ge_40": sum(
            1
            for row in full_backtest_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 40.0
        ),
        "full_backtest_36m_ge_60": sum(
            1
            for row in full_backtest_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 60.0
        ),
        "full_backtest_36m_ge_70": sum(
            1
            for row in full_backtest_36
            if _safe_float(row.get("score_36m")) is not None
            and float(row["score_36m"]) >= 70.0
        ),
    }


def full_backtest_provisional_reasons(
    summary: dict[str, Any],
    *,
    require_full_backtest_36: bool = False,
    selected_rows: list[dict[str, Any]] | None = None,
) -> list[str]:
    reasons: list[str] = []
    scrutiny_count = int(summary.get("attempts_with_scrutiny_36m") or 0)
    full_count = int(summary.get("attempts_with_full_backtest_36m") or 0)
    valid_count = int(summary.get("attempts_with_valid_full_backtest_36m") or 0)
    invalid_count = int(summary.get("attempts_with_invalid_full_backtest_36m") or 0)
    partial_count = int(summary.get("attempts_with_partial_full_backtest_36m") or 0)

    if scrutiny_count > valid_count:
        reasons.append(
            f"36mo full-backtest coverage is still incomplete: {valid_count}/{scrutiny_count} scrutiny-qualified attempts validate cleanly."
        )
    if invalid_count > 0:
        reasons.append(
            f"{invalid_count} attempts currently have invalid 36mo full-backtest artifacts that should be healed or rebuilt."
        )
    if partial_count > 0:
        reasons.append(
            f"{partial_count} attempts have only one side of the 36mo full-backtest artifact pair."
        )
    if require_full_backtest_36 and full_count > valid_count:
        reasons.append(
            f"{full_count - valid_count} attempts have nominal 36mo full-backtest files but they do not validate cleanly yet."
        )
    selected = list(selected_rows or [])
    if selected and any(not bool(row.get("has_full_backtest_36m")) for row in selected):
        reasons.append(
            "Selected board entries still include scrutiny-only candidates without attempt-local 36mo full backtests."
        )
    return reasons


def build_full_backtest_audit(
    rows: list[dict[str, Any]],
    *,
    invalid_example_limit: int = 25,
    pending_example_limit: int = 25,
) -> dict[str, Any]:
    summary = catalog_summary(rows)
    invalid_rows = [
        row
        for row in rows
        if str(row.get("full_backtest_validation_status_36m") or "") == "invalid"
    ]
    pending_rows = [
        row
        for row in rows
        if bool(row.get("has_scrutiny_36m")) and not bool(row.get("has_full_backtest_36m"))
    ]
    invalid_rows.sort(
        key=lambda row: (
            float(row.get("score_36m") or float("-inf")),
            float(row.get("composite_score") or float("-inf")),
        ),
        reverse=True,
    )
    pending_rows.sort(
        key=lambda row: (
            float(row.get("score_36m") or float("-inf")),
            float(row.get("composite_score") or float("-inf")),
        ),
        reverse=True,
    )
    reasons = full_backtest_provisional_reasons(summary, require_full_backtest_36=True)

    def compact_row(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": row.get("run_id"),
            "attempt_id": row.get("attempt_id"),
            "candidate_name": row.get("candidate_name"),
            "score_36m": row.get("score_36m"),
            "composite_score": row.get("composite_score"),
            "strategy_key_36m": row.get("strategy_key_36m"),
            "full_backtest_validation_status_36m": row.get(
                "full_backtest_validation_status_36m"
            ),
            "full_backtest_validation_issues_36m": row.get(
                "full_backtest_validation_issues_36m"
            ),
            "full_backtest_curve_point_count_36m": row.get(
                "full_backtest_curve_point_count_36m"
            ),
            "trades_per_month_36m": row.get("trades_per_month_36m"),
            "full_backtest_result_path_36m": row.get("full_backtest_result_path_36m"),
            "full_backtest_curve_path_36m": row.get("full_backtest_curve_path_36m"),
        }

    return {
        "summary": summary,
        "status": "provisional" if reasons else "ready_for_review",
        "provisional_reasons": reasons,
        "invalid_examples": [compact_row(row) for row in invalid_rows[:invalid_example_limit]],
        "pending_scrutiny_examples": [
            compact_row(row) for row in pending_rows[:pending_example_limit]
        ],
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized: dict[str, Any] = {}
            for key in fieldnames:
                value = row.get(key)
                if isinstance(value, (list, dict)):
                    serialized[key] = json.dumps(value, ensure_ascii=True)
                else:
                    serialized[key] = value
            writer.writerow(serialized)


def load_curve_series(curve_path: Path) -> dict[str, float]:
    payload = load_json_if_exists(curve_path)
    points = nested_get(payload, ["curve", "points"])
    if not isinstance(points, list):
        return {}
    series: dict[str, float] = {}
    for point in points:
        if not isinstance(point, dict):
            continue
        date_key = str(point.get("date") or "").strip()
        if not date_key:
            continue
        value = _safe_float(point.get("realized_r"))
        if value is None:
            continue
        series[date_key] = value
    return series


def pearson_correlation(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 3:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0.0 or right_var <= 0.0:
        return None
    covariance = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
    return covariance / (left_var**0.5 * right_var**0.5)


def build_similarity_payload(
    candidate_rows: list[dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    prepared: list[dict[str, Any]] = []
    candidate_total = len(candidate_rows)
    if progress_callback is not None:
        progress_callback({"stage": "prepare_start", "total": candidate_total})
    for index, row in enumerate(candidate_rows, start=1):
        curve_path_raw = str(row.get("scrutiny_curve_path_36m") or "").strip()
        if not curve_path_raw:
            if progress_callback is not None and (
                index == candidate_total or index == 1 or index % 50 == 0
            ):
                progress_callback(
                    {
                        "stage": "prepare_progress",
                        "completed": index,
                        "total": candidate_total,
                        "prepared_count": len(prepared),
                    }
                )
            continue
        curve_series = load_curve_series(Path(curve_path_raw))
        if not curve_series:
            if progress_callback is not None and (
                index == candidate_total or index == 1 or index % 50 == 0
            ):
                progress_callback(
                    {
                        "stage": "prepare_progress",
                        "completed": index,
                        "total": candidate_total,
                        "prepared_count": len(prepared),
                    }
                )
            continue
        prepared.append(
            {
                **row,
                "curve_series": curve_series,
                "curve_dates": set(curve_series.keys()),
                "active_dates": {
                    date
                    for date, value in curve_series.items()
                    if abs(float(value)) > 1e-9
                },
                "instruments_norm": normalize_tokens(
                    list(row.get("instruments_36m") or row.get("base_instruments") or [])
                ),
                "instruments_set": set(
                    normalize_tokens(
                        list(
                            row.get("instruments_36m") or row.get("base_instruments") or []
                        )
                    )
                ),
                "timeframe_norm": str(
                    row.get("timeframe_36m") or row.get("base_timeframe") or ""
                ).strip().upper()
                or None,
                "strategy_key_norm": str(
                    row.get("strategy_key_36m") or row.get("base_strategy_key") or ""
                ).strip()
                or None,
                "trades_per_month_norm": _safe_float(row.get("trades_per_month_36m")),
                "max_drawdown_norm": _safe_float(row.get("max_drawdown_r_36m")),
            }
        )
        if progress_callback is not None and (
            index == candidate_total or index == 1 or index % 50 == 0
        ):
            progress_callback(
                {
                    "stage": "prepare_progress",
                    "completed": index,
                    "total": candidate_total,
                    "prepared_count": len(prepared),
                }
            )

    if not prepared:
        if progress_callback is not None:
            progress_callback({"stage": "pairs_start", "total": 0, "prepared_count": 0})
        return {"leaders": [], "pairs": [], "matrix_labels": [], "matrix_values": []}

    prepared.sort(
        key=lambda item: float(item.get("score_36m") or float("-inf")), reverse=True
    )
    pair_records: list[dict[str, Any]] = []
    pair_total = (len(prepared) * (len(prepared) - 1)) // 2
    processed_pairs = 0
    if progress_callback is not None:
        progress_callback(
            {
                "stage": "pairs_start",
                "total": pair_total,
                "prepared_count": len(prepared),
            }
        )

    for left_index, left in enumerate(prepared):
        left_dates = left["curve_dates"]
        left_values_map = left["curve_series"]
        left_instruments = left["instruments_set"]
        active_left = left["active_dates"]
        for right_index in range(left_index + 1, len(prepared)):
            processed_pairs += 1
            right = prepared[right_index]
            right_dates = right["curve_dates"]
            common_dates = sorted(left_dates & right_dates)
            if len(common_dates) < 30:
                if progress_callback is not None and (
                    processed_pairs == pair_total
                    or processed_pairs == 1
                    or processed_pairs % 5000 == 0
                ):
                    progress_callback(
                        {
                            "stage": "pairs_progress",
                            "completed": processed_pairs,
                            "total": pair_total,
                            "prepared_count": len(prepared),
                        }
                    )
                continue
            left_values = [float(left_values_map[date]) for date in common_dates]
            right_values = [float(right["curve_series"][date]) for date in common_dates]
            corr = pearson_correlation(left_values, right_values)
            positive_corr = max(0.0, float(corr)) if corr is not None else 0.0
            right_instruments = right["instruments_set"]
            union_instruments = left_instruments | right_instruments
            instrument_overlap = (
                len(left_instruments & right_instruments) / len(union_instruments)
                if union_instruments
                else 0.0
            )
            active_right = right["active_dates"]
            active_union = active_left | active_right
            shared_active_ratio = (
                len(active_left & active_right) / len(active_union)
                if active_union
                else 0.0
            )
            same_timeframe = left["timeframe_norm"] == right["timeframe_norm"]
            cadence_similarity = 0.0
            left_trades_per_month = _safe_float(left.get("trades_per_month_norm"))
            right_trades_per_month = _safe_float(right.get("trades_per_month_norm"))
            if (
                left_trades_per_month is not None
                and right_trades_per_month is not None
                and left_trades_per_month >= 0.0
                and right_trades_per_month >= 0.0
            ):
                larger = max(left_trades_per_month, right_trades_per_month, 0.1)
                smaller = max(min(left_trades_per_month, right_trades_per_month), 0.1)
                cadence_similarity = max(0.0, min(1.0, smaller / larger))
            drawdown_similarity = 0.0
            left_drawdown = _safe_float(left.get("max_drawdown_norm"))
            right_drawdown = _safe_float(right.get("max_drawdown_norm"))
            if (
                left_drawdown is not None
                and right_drawdown is not None
                and left_drawdown >= 0.0
                and right_drawdown >= 0.0
            ):
                larger = max(left_drawdown, right_drawdown, 0.1)
                smaller = max(min(left_drawdown, right_drawdown), 0.1)
                drawdown_similarity = max(0.0, min(1.0, smaller / larger))
            same_strategy_key = (
                bool(left.get("strategy_key_norm"))
                and left.get("strategy_key_norm") == right.get("strategy_key_norm")
            )
            similarity_score = max(
                0.0,
                min(
                    1.0,
                    positive_corr * 0.50
                    + shared_active_ratio * 0.20
                    + instrument_overlap * 0.10
                    + cadence_similarity * 0.05
                    + drawdown_similarity * 0.05
                    + (0.05 if same_timeframe else 0.0)
                    + (0.05 if same_strategy_key else 0.0),
                ),
            )
            pair_records.append(
                {
                    "left_attempt_id": left["attempt_id"],
                    "left_run_id": left["run_id"],
                    "left_candidate_name": left.get("candidate_name"),
                    "right_attempt_id": right["attempt_id"],
                    "right_run_id": right["run_id"],
                    "right_candidate_name": right.get("candidate_name"),
                    "left_score_36m": left.get("score_36m"),
                    "right_score_36m": right.get("score_36m"),
                    "correlation": corr,
                    "positive_correlation": positive_corr,
                    "shared_active_ratio": shared_active_ratio,
                    "instrument_overlap_ratio": instrument_overlap,
                    "cadence_similarity": cadence_similarity,
                    "drawdown_similarity": drawdown_similarity,
                    "same_strategy_key": same_strategy_key,
                    "same_timeframe": same_timeframe,
                    "overlap_days": len(common_dates),
                    "similarity_score": similarity_score,
                }
            )
            if progress_callback is not None and (
                processed_pairs == pair_total
                or processed_pairs == 1
                or processed_pairs % 5000 == 0
            ):
                progress_callback(
                    {
                        "stage": "pairs_progress",
                        "completed": processed_pairs,
                        "total": pair_total,
                        "prepared_count": len(prepared),
                    }
                )

    adjacency: dict[str, list[dict[str, Any]]] = {
        str(item["attempt_id"]): [] for item in prepared
    }
    for pair in pair_records:
        adjacency[pair["left_attempt_id"]].append(pair)
        adjacency[pair["right_attempt_id"]].append(pair)

    leaders: list[dict[str, Any]] = []
    for row in prepared:
        related = adjacency.get(str(row["attempt_id"]), [])
        max_pair = max(
            related,
            key=lambda item: float(item.get("similarity_score", 0.0)),
            default=None,
        )
        avg_sameness = (
            sum(float(item.get("similarity_score", 0.0)) for item in related)
            / len(related)
            if related
            else 0.0
        )
        closest_match_attempt_id = None
        if max_pair:
            if max_pair["left_attempt_id"] == row["attempt_id"]:
                closest_match_attempt_id = max_pair["right_attempt_id"]
            else:
                closest_match_attempt_id = max_pair["left_attempt_id"]
        leaders.append(
            {
                "run_id": row["run_id"],
                "attempt_id": row["attempt_id"],
                "candidate_name": row.get("candidate_name"),
                "score_36m": row.get("score_36m"),
                "score_12m": row.get("score_12m"),
                "score_delta_36m_minus_12m": row.get("score_delta_36m_minus_12m"),
                "score_retention_ratio_36m_vs_12m": row.get(
                    "score_retention_ratio_36m_vs_12m"
                ),
                "trades_per_month_36m": row.get("trades_per_month_36m"),
                "trade_count_36m": row.get("trade_count_36m"),
                "timeframe_36m": row.get("timeframe_36m"),
                "instruments_36m": list(row.get("instruments_36m") or []),
                "avg_sameness": avg_sameness,
                "max_sameness": float(max_pair.get("similarity_score", 0.0))
                if max_pair
                else 0.0,
                "closest_match_attempt_id": closest_match_attempt_id,
            }
        )

    pair_records.sort(
        key=lambda item: float(item.get("similarity_score", 0.0)), reverse=True
    )
    matrix_labels = [
        f"{item['run_id']} | {item.get('candidate_name') or item['attempt_id']}"
        for item in prepared
    ]
    pair_lookup: dict[tuple[str, str], float] = {}
    for pair in pair_records:
        key = tuple(sorted([str(pair["left_attempt_id"]), str(pair["right_attempt_id"])]))
        pair_lookup[key] = float(pair.get("similarity_score", 0.0))

    matrix_values: list[list[float]] = []
    for left in prepared:
        row_values: list[float] = []
        for right in prepared:
            if left["attempt_id"] == right["attempt_id"]:
                row_values.append(1.0)
                continue
            key = tuple(sorted([str(left["attempt_id"]), str(right["attempt_id"])]))
            row_values.append(float(pair_lookup.get(key, 0.0)))
        matrix_values.append(row_values)

    return {
        "leaders": leaders,
        "pairs": pair_records,
        "matrix_labels": matrix_labels,
        "matrix_values": matrix_values,
    }


def subset_similarity_payload(
    similarity_payload: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    wanted_attempt_ids = {
        str(row.get("attempt_id") or "").strip() for row in candidate_rows if str(row.get("attempt_id") or "").strip()
    }
    if not wanted_attempt_ids:
        return {"leaders": [], "pairs": [], "matrix_labels": [], "matrix_values": []}

    filtered_leaders = [
        dict(row)
        for row in list(similarity_payload.get("leaders") or [])
        if str(row.get("attempt_id") or "").strip() in wanted_attempt_ids
    ]
    filtered_pairs = [
        dict(pair)
        for pair in list(similarity_payload.get("pairs") or [])
        if str(pair.get("left_attempt_id") or "").strip() in wanted_attempt_ids
        and str(pair.get("right_attempt_id") or "").strip() in wanted_attempt_ids
    ]
    label_by_attempt_id = {
        str(row.get("attempt_id") or "").strip(): (
            f"{row.get('run_id')} | {row.get('candidate_name') or row.get('attempt_id')}"
        )
        for row in candidate_rows
        if str(row.get("attempt_id") or "").strip()
    }
    ordered_attempt_ids = [
        str(row.get("attempt_id") or "").strip()
        for row in candidate_rows
        if str(row.get("attempt_id") or "").strip() in wanted_attempt_ids
    ]
    pair_lookup: dict[tuple[str, str], float] = {}
    for pair in filtered_pairs:
        left = str(pair.get("left_attempt_id") or "").strip()
        right = str(pair.get("right_attempt_id") or "").strip()
        if not left or not right:
            continue
        pair_lookup[tuple(sorted([left, right]))] = float(pair.get("similarity_score") or 0.0)
    matrix_values: list[list[float]] = []
    for left in ordered_attempt_ids:
        row_values: list[float] = []
        for right in ordered_attempt_ids:
            if left == right:
                row_values.append(1.0)
                continue
            row_values.append(float(pair_lookup.get(tuple(sorted([left, right])), 0.0)))
        matrix_values.append(row_values)
    return {
        "leaders": filtered_leaders,
        "pairs": filtered_pairs,
        "matrix_labels": [label_by_attempt_id.get(attempt_id, attempt_id) for attempt_id in ordered_attempt_ids],
        "matrix_values": matrix_values,
    }


def select_promotion_board(
    candidate_rows: list[dict[str, Any]],
    similarity_payload: dict[str, Any],
    *,
    board_size: int,
    novelty_penalty: float,
    drawdown_penalty: float = 0.0,
    trade_rate_bonus_weight: float = 0.0,
    trade_rate_bonus_target: float = 8.0,
    scalar_metric_terms: list[dict[str, Any]] | None = None,
    max_drawdown_r: float | None = None,
    max_sameness_to_board: float | None = None,
    max_per_run: int | None = None,
    max_per_strategy_key: int | None = None,
) -> dict[str, Any]:
    def trade_rate_bonus(trades_per_month: float | None) -> tuple[float, float]:
        value = _safe_float(trades_per_month)
        if value is None or value <= 0.0:
            return 0.0, 0.0
        weight = max(0.0, float(trade_rate_bonus_weight))
        if weight <= 0.0:
            return 0.0, 0.0
        target = max(0.1, float(trade_rate_bonus_target))
        fraction = min(1.0, log1p(value) / log1p(target))
        return weight * fraction, fraction

    leaders = {
        str(row.get("attempt_id")): dict(row)
        for row in similarity_payload.get("leaders") or []
    }
    pair_lookup: dict[tuple[str, str], float] = {}
    for pair in similarity_payload.get("pairs") or []:
        left = str(pair.get("left_attempt_id") or "")
        right = str(pair.get("right_attempt_id") or "")
        if not left or not right:
            continue
        pair_lookup[tuple(sorted([left, right]))] = float(
            pair.get("similarity_score") or 0.0
        )

    remaining = [dict(row) for row in candidate_rows]
    remaining.sort(
        key=lambda row: float(row.get("score_36m") or float("-inf")), reverse=True
    )
    selected: list[dict[str, Any]] = []
    selected_by_run: dict[str, int] = {}
    selected_by_strategy_key: dict[str, int] = {}

    while remaining and len(selected) < max(0, int(board_size)):
        best_index = 0
        best_choice: dict[str, Any] | None = None
        for index, row in enumerate(remaining):
            attempt_id = str(row.get("attempt_id") or "")
            run_id = str(row.get("run_id") or "").strip()
            strategy_token = str(
                row.get("strategy_key_36m") or row.get("base_strategy_key") or ""
            ).strip()
            if (
                max_per_run is not None
                and run_id
                and selected_by_run.get(run_id, 0) >= int(max_per_run)
            ):
                continue
            if (
                max_per_strategy_key is not None
                and strategy_token
                and selected_by_strategy_key.get(strategy_token, 0)
                >= int(max_per_strategy_key)
            ):
                continue
            drawdown_r = _safe_float(row.get("max_drawdown_r_36m"))
            if (
                max_drawdown_r is not None
                and drawdown_r is not None
                and drawdown_r > float(max_drawdown_r)
            ):
                continue
            pair_scores = []
            closest_selected_attempt_id = None
            closest_selected_sameness = 0.0
            for selected_row in selected:
                selected_attempt_id = str(selected_row.get("attempt_id") or "")
                pair_score = float(
                    pair_lookup.get(
                        tuple(sorted([attempt_id, selected_attempt_id])), 0.0
                    )
                )
                pair_scores.append(pair_score)
                if pair_score >= closest_selected_sameness:
                    closest_selected_sameness = pair_score
                    closest_selected_attempt_id = selected_attempt_id
            max_sameness_to_selected = max(pair_scores) if pair_scores else 0.0
            avg_sameness_to_selected = (
                sum(pair_scores) / len(pair_scores) if pair_scores else 0.0
            )
            if (
                max_sameness_to_board is not None
                and selected
                and max_sameness_to_selected > float(max_sameness_to_board)
            ):
                continue
            drawdown_component = (
                float(drawdown_penalty) * float(drawdown_r)
                if drawdown_r is not None and float(drawdown_penalty) > 0.0
                else 0.0
            )
            trade_bonus_component, trade_bonus_fraction = trade_rate_bonus(
                row.get("trades_per_month_36m")
            )
            scalar_metric_bonus_component, scalar_metric_bonus_terms = (
                compute_scalar_metric_bonus(row, scalar_metric_terms)
            )
            utility = (
                float(row.get("score_36m") or float("-inf"))
                + trade_bonus_component
                + scalar_metric_bonus_component
                - (float(novelty_penalty) * max_sameness_to_selected)
                - drawdown_component
            )
            candidate = dict(row)
            candidate["selection_utility"] = utility
            candidate["score_component"] = float(row.get("score_36m") or float("-inf"))
            candidate["drawdown_penalty_component"] = drawdown_component
            candidate["trade_rate_bonus_component"] = trade_bonus_component
            candidate["trade_rate_bonus_fraction"] = trade_bonus_fraction
            candidate["scalar_metric_bonus_component"] = scalar_metric_bonus_component
            candidate["scalar_metric_bonus_terms"] = scalar_metric_bonus_terms
            candidate["max_sameness_to_selected"] = max_sameness_to_selected
            candidate["avg_sameness_to_selected"] = avg_sameness_to_selected
            candidate["closest_selected_attempt_id"] = closest_selected_attempt_id
            candidate["selected_run_count_if_chosen"] = (
                selected_by_run.get(run_id, 0) + 1 if run_id else None
            )
            candidate["selected_strategy_count_if_chosen"] = (
                selected_by_strategy_key.get(strategy_token, 0) + 1
                if strategy_token
                else None
            )
            leader_row = leaders.get(attempt_id) or {}
            candidate["global_max_sameness"] = float(
                leader_row.get("max_sameness") or 0.0
            )
            if best_choice is None:
                best_index = index
                best_choice = candidate
                continue
            incumbent = best_choice
            better = (
                utility > float(incumbent.get("selection_utility") or float("-inf"))
                or (
                    utility
                    == float(incumbent.get("selection_utility") or float("-inf"))
                    and max_sameness_to_selected
                    < float(incumbent.get("max_sameness_to_selected") or 1.0)
                )
                or (
                    utility
                    == float(incumbent.get("selection_utility") or float("-inf"))
                    and max_sameness_to_selected
                    == float(incumbent.get("max_sameness_to_selected") or 1.0)
                    and float(row.get("score_36m") or float("-inf"))
                    > float(incumbent.get("score_36m") or float("-inf"))
                )
            )
            if better:
                best_index = index
                best_choice = candidate
        if best_choice is None:
            break
        best_choice["selection_rank"] = len(selected) + 1
        selected.append(best_choice)
        selected_run_id = str(best_choice.get("run_id") or "").strip()
        if selected_run_id:
            selected_by_run[selected_run_id] = selected_by_run.get(selected_run_id, 0) + 1
        selected_strategy_key = str(
            best_choice.get("strategy_key_36m") or best_choice.get("base_strategy_key") or ""
        ).strip()
        if selected_strategy_key:
            selected_by_strategy_key[selected_strategy_key] = (
                selected_by_strategy_key.get(selected_strategy_key, 0) + 1
            )
        remaining.pop(best_index)

    alternates: list[dict[str, Any]] = []
    for row in remaining[: max(10, board_size)]:
        attempt_id = str(row.get("attempt_id") or "")
        pair_scores = [
            float(
                pair_lookup.get(
                    tuple(
                        sorted([attempt_id, str(selected_row.get("attempt_id") or "")])
                    ),
                    0.0,
                )
            )
            for selected_row in selected
        ]
        candidate = dict(row)
        candidate["max_sameness_to_board"] = max(pair_scores) if pair_scores else 0.0
        trade_bonus_component, trade_bonus_fraction = trade_rate_bonus(
            row.get("trades_per_month_36m")
        )
        scalar_metric_bonus_component, scalar_metric_bonus_terms = compute_scalar_metric_bonus(
            row, scalar_metric_terms
        )
        candidate["trade_rate_bonus_component"] = trade_bonus_component
        candidate["trade_rate_bonus_fraction"] = trade_bonus_fraction
        candidate["scalar_metric_bonus_component"] = scalar_metric_bonus_component
        candidate["scalar_metric_bonus_terms"] = scalar_metric_bonus_terms
        candidate["selection_utility"] = (
            float(row.get("score_36m") or float("-inf"))
            + trade_bonus_component
            + scalar_metric_bonus_component
            - (float(novelty_penalty) * candidate["max_sameness_to_board"])
        )
        alternates.append(candidate)

    alternates.sort(
        key=lambda row: (
            float(row.get("selection_utility") or float("-inf")),
            float(row.get("score_36m") or float("-inf")),
        ),
        reverse=True,
    )

    return {
        "selected": selected,
        "alternates": alternates,
        "selected_by_run": selected_by_run,
        "selected_by_strategy_key": selected_by_strategy_key,
    }
