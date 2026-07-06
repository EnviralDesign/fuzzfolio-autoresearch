from __future__ import annotations

import math
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .anchor_pair_atlas import (
    DEFAULT_EXECUTION_COST_MODE,
    DEFAULT_LOOKBACK_MONTHS,
    DEFAULT_PROBE_TIMEOUT_SECONDS as ANCHOR_PAIR_DEFAULT_PROBE_TIMEOUT_SECONDS,
    DEFAULT_QUALITY_SCORE_PRESET,
    _anchor_timeframe,
    _as_dict,
    _as_list,
    _catalog_by_id,
    _clean_token,
    _clean_upper,
    _clamp,
    _float_value,
    _fuzzfolio_base_args,
    _int_value,
    _load_json,
    _normalize_tokens,
    _read_csv_rows,
    _replace_profile_id_arg,
    _result_row_from_score,
    _sensitivity_args_for_row,
    _write_csv,
    _write_json,
    _write_run_script,
    build_pair_profile_document,
    resolve_probe_as_of_date,
)
from .config import AppConfig
from .forward_response_atlas import DEFAULT_FORWARD_RESPONSE_DIRNAME
from .fuzzfolio import CliError, FuzzfolioCli
from .indicator_atlas import DEFAULT_ATLAS_DIRNAME, build_indicator_atlas, load_indicator_catalog
from .recipe_priors import DEFAULT_RECIPE_PRIORS_DIRNAME
from .scoring import build_attempt_score, load_sensitivity_snapshot
from .signal_atlas import DEFAULT_INSTRUMENTS, DEFAULT_SIGNAL_ATLAS_DIRNAME, DEFAULT_TIMEFRAMES


SCHEMA_VERSION = "discovery_pair_atlas_v1"
RESULTS_SCHEMA_VERSION = "discovery_pair_probe_results_v1"
DEFAULT_DISCOVERY_PAIR_DIRNAME = "discovery-pair-atlas"
DEFAULT_MAX_QUEUE_PAIRS = 1536
DEFAULT_RANDOM_SEED = 20260521
DEFAULT_JOB_TIMEOUT_SECONDS = 2400
DEFAULT_PROBE_TIMEOUT_SECONDS = max(
    ANCHOR_PAIR_DEFAULT_PROBE_TIMEOUT_SECONDS,
    (DEFAULT_JOB_TIMEOUT_SECONDS * 2) + 300,
)
DEFAULT_PROBE_WORKERS = 8

DISCOVERY_LANE_FRACTIONS: dict[str, float] = {
    "proven_neighbor": 0.25,
    "plausible_novel": 0.45,
    "under_tested_role_correct": 0.20,
    "wild_diversity": 0.10,
}

LANE_ORDER = (
    "proven_neighbor",
    "plausible_novel",
    "under_tested_role_correct",
    "wild_diversity",
    "known_result",
)

DENSITY_SCORE: dict[str, float] = {
    "usable": 88.0,
    "sparse": 64.0,
    "dense": 54.0,
    "very_sparse": 36.0,
    "saturated": 22.0,
    "flat": 0.0,
    "no_data": 42.0,
    "unknown": 50.0,
}


@dataclass(frozen=True)
class DiscoveryPairAtlasBuildResult:
    atlas_path: Path
    matrix_csv_path: Path
    queue_csv_path: Path
    manifest_path: Path
    run_script_path: Path
    profile_dir: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "discovery_pair_atlas_json": str(self.atlas_path),
            "discovery_pair_matrix_csv": str(self.matrix_csv_path),
            "discovery_pair_queue_csv": str(self.queue_csv_path),
            "discovery_pair_run_manifest_json": str(self.manifest_path),
            "discovery_pair_run_script": str(self.run_script_path),
            "discovery_pair_profile_dir": str(self.profile_dir),
            "discovery_pair_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


@dataclass(frozen=True)
class DiscoveryPairProbeRunResult:
    results_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "discovery_pair_probe_results_csv": str(self.results_csv_path),
            "discovery_pair_probe_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _safe_round(value: float, digits: int = 2) -> float:
    return round(_clamp(value), digits)


def _indicator_rows(indicator_atlas_dir: Path) -> list[dict[str, Any]]:
    csv_path = indicator_atlas_dir / "indicator-atlas.csv"
    if csv_path.exists():
        return _read_csv_rows(csv_path)
    atlas_path = indicator_atlas_dir / "indicator-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing indicator atlas at {indicator_atlas_dir}. "
            "Run `uv run build-indicator-atlas` first."
        )
    payload = _as_dict(_load_json(atlas_path))
    return [row for row in _as_list(payload.get("indicators")) if isinstance(row, dict)]


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        _clean_upper(row.get("id")): row
        for row in rows
        if _clean_upper(row.get("id"))
    }


def _optional_signal_rollups(signal_atlas_dir: Path) -> dict[str, dict[str, Any]]:
    summary_path = signal_atlas_dir / "signal-atlas-summary.json"
    atlas_path = signal_atlas_dir / "signal-atlas.json"
    payload: dict[str, Any] = {}
    if summary_path.exists():
        payload = _as_dict(_load_json(summary_path))
    elif atlas_path.exists():
        payload = _as_dict(_as_dict(_load_json(atlas_path)).get("summary"))
    return {
        _clean_upper(indicator_id): _as_dict(row)
        for indicator_id, row in _as_dict(payload.get("indicator_rollups")).items()
        if _clean_upper(indicator_id)
    }


def _optional_forward_priors(forward_response_dir: Path) -> dict[str, dict[str, Any]]:
    csv_path = forward_response_dir / "forward-response-priors.csv"
    if not csv_path.exists():
        return {}
    return {
        _clean_upper(row.get("indicator_id")): row
        for row in _read_csv_rows(csv_path)
        if _clean_upper(row.get("indicator_id"))
    }


def _slot_prior_rollups(recipe_priors_dir: Path) -> dict[str, dict[str, Any]]:
    rows = _read_csv_rows(recipe_priors_dir / "slot-indicator-priors.csv")
    rollups: dict[str, dict[str, Any]] = {}
    for row in rows:
        indicator_id = _clean_upper(row.get("indicator_id"))
        if not indicator_id:
            continue
        current = rollups.setdefault(
            indicator_id,
            {
                "best_slot_score": 0.0,
                "best_sampling_weight": 0.0,
                "best_sampling_lane": "",
                "slot_count": 0,
            },
        )
        current["slot_count"] = _int_value(current.get("slot_count")) + 1
        score = _float_value(row.get("recipe_slot_score"))
        if score >= _float_value(current.get("best_slot_score")):
            current["best_slot_score"] = score
            current["best_sampling_lane"] = _clean_token(row.get("sampling_lane"))
        current["best_sampling_weight"] = max(
            _float_value(current.get("best_sampling_weight")),
            _float_value(row.get("sampling_weight")),
        )
    return rollups


def _pair_key(first_id: Any, second_id: Any, timeframe: Any) -> tuple[str, str, str]:
    return (_clean_upper(first_id), _clean_upper(second_id), _clean_upper(timeframe))


def _known_pair_evidence(
    anchor_pair_dir: Path,
    recipe_priors_dir: Path,
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[str, dict[str, Any]]]:
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _read_csv_rows(anchor_pair_dir / "anchor-pair-probe-results.csv"):
        key = _pair_key(row.get("anchor_id"), row.get("trigger_id"), row.get("probe_timeframe"))
        if not all(key):
            continue
        by_key[key] = {
            "source": "anchor_pair_probe_results",
            "probe_id": row.get("probe_id"),
            "status": row.get("status"),
            "composite_score": _float_value(row.get("composite_score")),
            "primary_score": _float_value(row.get("primary_score")),
            "signal_count": _float_value(row.get("signal_count")),
            "best_expectancy_r": _float_value(row.get("best_expectancy_r")),
            "best_trades": _float_value(row.get("best_trades")),
            "best_profit_factor": _float_value(row.get("best_profit_factor")),
        }
    for row in _read_csv_rows(recipe_priors_dir / "pair-priors.csv"):
        key = _pair_key(row.get("anchor_id"), row.get("trigger_id"), row.get("probe_timeframe"))
        if not all(key) or key in by_key:
            continue
        by_key[key] = {
            "source": "recipe_pair_priors",
            "probe_id": row.get("probe_id"),
            "status": "scored",
            "composite_score": _float_value(row.get("composite_score")),
            "primary_score": _float_value(row.get("composite_score")),
            "signal_count": _float_value(row.get("signal_count")),
            "best_expectancy_r": _float_value(row.get("best_expectancy_r")),
            "best_trades": _float_value(row.get("best_trades")),
            "best_profit_factor": _float_value(row.get("best_profit_factor")),
        }

    by_indicator: dict[str, dict[str, Any]] = {}
    for (first_id, second_id, _timeframe), row in by_key.items():
        score = _float_value(row.get("composite_score"))
        positive = score >= 50.0
        for indicator_id in (first_id, second_id):
            rollup = by_indicator.setdefault(
                indicator_id,
                {
                    "tested_pair_count": 0,
                    "positive_pair_count": 0,
                    "best_pair_score": 0.0,
                    "best_pair_probe_id": "",
                },
            )
            rollup["tested_pair_count"] = _int_value(rollup.get("tested_pair_count")) + 1
            if positive:
                rollup["positive_pair_count"] = _int_value(rollup.get("positive_pair_count")) + 1
            if score >= _float_value(rollup.get("best_pair_score")):
                rollup["best_pair_score"] = score
                rollup["best_pair_probe_id"] = row.get("probe_id")
    return by_key, by_indicator


def _signal_score(rollup: dict[str, Any] | None) -> tuple[float, str, float, int]:
    if not rollup:
        return 50.0, "unknown", 0.0, 0
    status = _clean_token(rollup.get("status"))
    bucket = _clean_token(rollup.get("density_bucket")) or "unknown"
    if status and status != "ok":
        return 20.0, bucket, _float_value(rollup.get("active_percent")), _int_value(rollup.get("event_count"))
    score = DENSITY_SCORE.get(bucket, 50.0)
    event_count = _int_value(rollup.get("event_count"))
    if event_count >= 500:
        score += 5.0
    elif 0 < event_count < 40:
        score -= 10.0
    balance_counts = _as_dict(rollup.get("balance_bucket_counts"))
    if balance_counts.get("balanced"):
        score += 6.0
    elif balance_counts.get("one_sided") or balance_counts.get("flat"):
        score -= 12.0
    return _safe_round(score), bucket, _float_value(rollup.get("active_percent")), event_count


def _forward_score(prior: dict[str, Any] | None) -> tuple[float, str, str, int]:
    if not prior:
        return 50.0, "unknown", "", 0
    return (
        _safe_round(_float_value(prior.get("forward_response_prior_score"), 50.0)),
        _clean_token(prior.get("forward_response_prior_bucket")) or "unknown",
        _clean_token(prior.get("best_cell_context")),
        _int_value(prior.get("strong_cell_count")),
    )


def _role_order_score(first_row: dict[str, Any], second_row: dict[str, Any]) -> float:
    first_role = _clean_token(first_row.get("signal_role")).lower()
    second_role = _clean_token(second_row.get("signal_role")).lower()
    if second_role == "trigger" and first_role in {"context", "filter", "setup"}:
        return 90.0 if first_role == "context" else 84.0
    if second_role == "trigger" and first_role == "trigger":
        return 65.0
    if second_role == "setup" and first_role in {"context", "filter", "setup"}:
        return 72.0
    if first_role == "trigger" and second_role in {"context", "filter", "setup"}:
        return 46.0
    if first_role in {"context", "filter"} and second_role in {"context", "filter"}:
        return 54.0
    return 58.0


def _strategy_pair_score(first_row: dict[str, Any], second_row: dict[str, Any]) -> float:
    first_strategy = _clean_token(first_row.get("strategy_role")).lower()
    second_strategy = _clean_token(second_row.get("strategy_role")).lower()
    pair = {first_strategy, second_strategy}
    score = 50.0
    if first_strategy == second_strategy and first_strategy:
        score += 8.0
    if "mean-reversion" in pair and pair.intersection({"confirm", "trend"}):
        score += 12.0
    if "mean-reversion" in pair and "breakout" in pair:
        score += 6.0
    if "trend" in pair and pair.intersection({"breakout", "confirm"}):
        score += 16.0
    if "breakout" in pair and pair.intersection({"confirm", "volatility"}):
        score += 15.0
    if "volatility" in pair and pair.intersection({"breakout", "trend"}):
        score += 12.0
    if "volume" in pair and pair.intersection({"breakout", "trend", "confirm"}):
        score += 10.0
    if not first_strategy or not second_strategy:
        score -= 4.0
    return _safe_round(score)


def _score_bucket(score: float) -> str:
    if score >= 72.0:
        return "high"
    if score >= 58.0:
        return "medium"
    if score >= 44.0:
        return "low"
    return "weak"


def _known_status(evidence: dict[str, Any] | None) -> str:
    if not evidence:
        return "untested"
    score = _float_value(evidence.get("composite_score"))
    if score >= 50.0:
        return "exact_known_positive"
    if score > 0.0:
        return "exact_known_low"
    return "exact_known_zero"


def _discovery_lane(
    *,
    known_pair_status: str,
    local_score: float,
    role_score: float,
    first_rollup: dict[str, Any],
    second_rollup: dict[str, Any],
    reverse_known_score: float,
) -> tuple[str, str]:
    if known_pair_status != "untested":
        return "known_result", known_pair_status

    first_positive = _int_value(first_rollup.get("positive_pair_count"))
    second_positive = _int_value(second_rollup.get("positive_pair_count"))
    tested_count = _int_value(first_rollup.get("tested_pair_count")) + _int_value(
        second_rollup.get("tested_pair_count")
    )
    has_positive_neighbor = first_positive > 0 or second_positive > 0 or reverse_known_score >= 50.0
    if has_positive_neighbor and local_score >= 54.0:
        return "proven_neighbor", "contains_indicator_or_reverse_order_with_positive_pair_evidence"
    if role_score >= 68.0 and local_score >= 56.0:
        return "plausible_novel", "role_order_and_local_priors_are_plausible"
    if role_score >= 64.0 and tested_count <= 4 and local_score >= 44.0:
        return "under_tested_role_correct", "role_correct_but_pair_family_is_under_tested"
    return "wild_diversity", "kept_for_exploration_or_nonobvious_complementarity"


def score_discovery_pair(
    *,
    first_row: dict[str, Any],
    second_row: dict[str, Any],
    signal_rollups: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    slot_priors: dict[str, dict[str, Any]],
    known_pairs: dict[tuple[str, str, str], dict[str, Any]],
    indicator_pair_rollups: dict[str, dict[str, Any]],
    probe_timeframe: str,
    instruments: list[str],
) -> dict[str, Any]:
    first_id = _clean_upper(first_row.get("id"))
    second_id = _clean_upper(second_row.get("id"))
    first_signal_score, first_density_bucket, first_active_percent, first_event_count = _signal_score(
        signal_rollups.get(first_id)
    )
    second_signal_score, second_density_bucket, second_active_percent, second_event_count = _signal_score(
        signal_rollups.get(second_id)
    )
    first_forward_score, first_forward_bucket, first_context, first_strong_cells = _forward_score(
        forward_priors.get(first_id)
    )
    second_forward_score, second_forward_bucket, second_context, second_strong_cells = _forward_score(
        forward_priors.get(second_id)
    )
    first_slot = _as_dict(slot_priors.get(first_id))
    second_slot = _as_dict(slot_priors.get(second_id))
    first_pair_rollup = _as_dict(indicator_pair_rollups.get(first_id))
    second_pair_rollup = _as_dict(indicator_pair_rollups.get(second_id))
    role_score = _role_order_score(first_row, second_row)
    strategy_score = _strategy_pair_score(first_row, second_row)
    static_score = (
        _float_value(first_row.get("static_prior_score"), 50.0) * 0.45
        + _float_value(second_row.get("static_prior_score"), 50.0) * 0.55
    )
    density_score = first_signal_score * 0.35 + second_signal_score * 0.65
    forward_score = first_forward_score * 0.30 + second_forward_score * 0.70
    slot_score = (
        _float_value(first_slot.get("best_slot_score"), 50.0) * 0.45
        + _float_value(second_slot.get("best_slot_score"), 50.0) * 0.55
    )
    empirical_neighbor_score = max(
        _float_value(first_pair_rollup.get("best_pair_score"), 0.0),
        _float_value(second_pair_rollup.get("best_pair_score"), 0.0),
    )
    best_context = second_context or first_context
    context_bonus = 0.0
    context_parts = [part for part in best_context.split() if part]
    if len(context_parts) >= 2 and _clean_upper(context_parts[1]) == _clean_upper(probe_timeframe):
        context_bonus += 2.5
    if context_parts and _clean_upper(context_parts[0]) in set(instruments):
        context_bonus += 1.5

    shared_base = bool(
        first_row.get("base_indicator_id")
        and first_row.get("base_indicator_id") == second_row.get("base_indicator_id")
    )
    local_score = (
        role_score * 0.24
        + strategy_score * 0.14
        + density_score * 0.16
        + forward_score * 0.18
        + static_score * 0.12
        + slot_score * 0.10
        + empirical_neighbor_score * 0.06
        + context_bonus
    )
    if shared_base:
        local_score -= 8.0
    if second_density_bucket in {"flat", "saturated"}:
        local_score -= 8.0
    if second_forward_bucket in {"default_problem"}:
        local_score -= 10.0

    key = _pair_key(first_id, second_id, probe_timeframe)
    reverse_key = _pair_key(second_id, first_id, probe_timeframe)
    known = known_pairs.get(key)
    reverse_known = known_pairs.get(reverse_key)
    known_pair_status = _known_status(known)
    reverse_known_score = _float_value(_as_dict(reverse_known).get("composite_score"))
    local_score = _safe_round(local_score)
    lane, reason = _discovery_lane(
        known_pair_status=known_pair_status,
        local_score=local_score,
        role_score=role_score,
        first_rollup=first_pair_rollup,
        second_rollup=second_pair_rollup,
        reverse_known_score=reverse_known_score,
    )

    return {
        "role_order_score": role_score,
        "strategy_pair_score": strategy_score,
        "first_signal_density_score": first_signal_score,
        "first_signal_density_bucket": first_density_bucket,
        "first_active_percent": first_active_percent,
        "first_event_count": first_event_count,
        "second_signal_density_score": second_signal_score,
        "second_signal_density_bucket": second_density_bucket,
        "second_active_percent": second_active_percent,
        "second_event_count": second_event_count,
        "first_forward_response_prior_score": first_forward_score,
        "first_forward_response_prior_bucket": first_forward_bucket,
        "first_best_forward_context": first_context,
        "first_strong_cell_count": first_strong_cells,
        "second_forward_response_prior_score": second_forward_score,
        "second_forward_response_prior_bucket": second_forward_bucket,
        "second_best_forward_context": second_context,
        "second_strong_cell_count": second_strong_cells,
        "best_forward_context": best_context,
        "first_static_prior_score": _safe_round(_float_value(first_row.get("static_prior_score"), 50.0)),
        "second_static_prior_score": _safe_round(_float_value(second_row.get("static_prior_score"), 50.0)),
        "first_best_slot_score": _safe_round(_float_value(first_slot.get("best_slot_score"), 50.0)),
        "second_best_slot_score": _safe_round(_float_value(second_slot.get("best_slot_score"), 50.0)),
        "first_best_sampling_lane": first_slot.get("best_sampling_lane") or "",
        "second_best_sampling_lane": second_slot.get("best_sampling_lane") or "",
        "first_tested_pair_count": _int_value(first_pair_rollup.get("tested_pair_count")),
        "second_tested_pair_count": _int_value(second_pair_rollup.get("tested_pair_count")),
        "first_positive_pair_count": _int_value(first_pair_rollup.get("positive_pair_count")),
        "second_positive_pair_count": _int_value(second_pair_rollup.get("positive_pair_count")),
        "neighbor_positive_pair_count": _int_value(first_pair_rollup.get("positive_pair_count"))
        + _int_value(second_pair_rollup.get("positive_pair_count")),
        "known_pair_status": known_pair_status,
        "known_pair_score": _float_value(_as_dict(known).get("composite_score")) if known else "",
        "known_pair_probe_id": _as_dict(known).get("probe_id") if known else "",
        "reverse_known_pair_score": reverse_known_score if reverse_known else "",
        "reverse_known_pair_probe_id": _as_dict(reverse_known).get("probe_id") if reverse_known else "",
        "shared_base_indicator": shared_base,
        "local_discovery_score": local_score,
        "local_score_bucket": _score_bucket(local_score),
        "discovery_lane": lane,
        "discovery_reason": reason,
        "pair_prior_score": local_score,
        "pair_prior_bucket": lane,
    }


def build_discovery_pair_rows(
    *,
    rows_by_id: dict[str, dict[str, Any]],
    signal_rollups: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    slot_priors: dict[str, dict[str, Any]],
    known_pairs: dict[tuple[str, str, str], dict[str, Any]],
    indicator_pair_rollups: dict[str, dict[str, Any]],
    first_ids: list[str] | None,
    second_ids: list[str] | None,
    timeframes: list[str],
    instruments: list[str],
) -> list[dict[str, Any]]:
    explicit_first = set(_normalize_tokens(first_ids))
    explicit_second = set(_normalize_tokens(second_ids))
    eligible_ids = [
        indicator_id
        for indicator_id, row in rows_by_id.items()
        if _bool_value(row.get("generation_eligible"))
    ]
    first_pool = [indicator_id for indicator_id in eligible_ids if not explicit_first or indicator_id in explicit_first]
    second_pool = [
        indicator_id
        for indicator_id in eligible_ids
        if not explicit_second or indicator_id in explicit_second
    ]
    rows: list[dict[str, Any]] = []
    for first_id in first_pool:
        first_row = rows_by_id[first_id]
        for second_id in second_pool:
            if first_id == second_id:
                continue
            second_row = rows_by_id[second_id]
            for probe_timeframe in timeframes:
                scored = score_discovery_pair(
                    first_row=first_row,
                    second_row=second_row,
                    signal_rollups=signal_rollups,
                    forward_priors=forward_priors,
                    slot_priors=slot_priors,
                    known_pairs=known_pairs,
                    indicator_pair_rollups=indicator_pair_rollups,
                    probe_timeframe=probe_timeframe,
                    instruments=instruments,
                )
                rows.append(
                    {
                        "first_indicator_id": first_id,
                        "first_signal_role": first_row.get("signal_role"),
                        "first_strategy_role": first_row.get("strategy_role"),
                        "first_namespace": first_row.get("namespace"),
                        "first_base_indicator_id": first_row.get("base_indicator_id"),
                        "second_indicator_id": second_id,
                        "second_signal_role": second_row.get("signal_role"),
                        "second_strategy_role": second_row.get("strategy_role"),
                        "second_namespace": second_row.get("namespace"),
                        "second_base_indicator_id": second_row.get("base_indicator_id"),
                        "probe_timeframe": _clean_upper(probe_timeframe),
                        "instruments": ",".join(instruments),
                        "anchor_type": "discovery",
                        "anchor_id": first_id,
                        "trigger_id": second_id,
                        **scored,
                    }
                )
    rows.sort(
        key=lambda row: (
            LANE_ORDER.index(row.get("discovery_lane"))
            if row.get("discovery_lane") in LANE_ORDER
            else 99,
            -_float_value(row.get("local_discovery_score")),
            str(row.get("first_indicator_id") or ""),
            str(row.get("second_indicator_id") or ""),
            str(row.get("probe_timeframe") or ""),
        )
    )
    return rows


def _lane_quotas(max_pairs: int, lane_fractions: dict[str, float]) -> dict[str, int]:
    max_pairs = max(1, int(max_pairs))
    raw = {
        lane: max_pairs * max(0.0, float(fraction))
        for lane, fraction in lane_fractions.items()
    }
    quotas = {lane: int(math.floor(value)) for lane, value in raw.items()}
    remaining = max_pairs - sum(quotas.values())
    remainders = sorted(
        raw.items(),
        key=lambda item: (-(item[1] - math.floor(item[1])), item[0]),
    )
    index = 0
    while remaining > 0 and remainders:
        lane = remainders[index % len(remainders)][0]
        quotas[lane] = quotas.get(lane, 0) + 1
        remaining -= 1
        index += 1
    return quotas


def _selection_sort_key(row: dict[str, Any], *, rng: random.Random, lane: str) -> tuple[float, str, str, str]:
    score = _float_value(row.get("local_discovery_score"))
    if lane == "wild_diversity":
        score = 45.0 + rng.random() * 20.0
    elif lane == "under_tested_role_correct":
        tested = _int_value(row.get("first_tested_pair_count")) + _int_value(
            row.get("second_tested_pair_count")
        )
        score = score + max(0.0, 10.0 - tested)
    else:
        score = score + rng.random() * 0.001
    return (
        -score,
        str(row.get("first_indicator_id") or ""),
        str(row.get("second_indicator_id") or ""),
        str(row.get("probe_timeframe") or ""),
    )


def _slug_token(value: Any, max_length: int = 22) -> str:
    token = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    token = token or "unknown"
    return token[:max_length].strip("-") or token[:max_length]


def _assign_queue_metadata(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    for rank, row in enumerate(rows, start=1):
        queued = dict(row)
        first_id = _clean_upper(queued.get("first_indicator_id"))
        second_id = _clean_upper(queued.get("second_indicator_id"))
        timeframe = _clean_upper(queued.get("probe_timeframe"))
        queued["queue_rank"] = rank
        queued["probe_id"] = (
            f"dp-{rank:04d}-{_slug_token(first_id, 18)}-"
            f"{_slug_token(second_id, 18)}-{timeframe.lower()}"
        )
        queued["anchor_id"] = first_id
        queued["trigger_id"] = second_id
        queued["anchor_type"] = "discovery"
        queued["pair_prior_score"] = queued.get("local_discovery_score")
        queued["pair_prior_bucket"] = queued.get("discovery_lane")
        queue.append(queued)
    return queue


def select_discovery_pair_queue(
    rows: list[dict[str, Any]],
    *,
    max_pairs: int = DEFAULT_MAX_QUEUE_PAIRS,
    full_queue: bool = False,
    include_known_retests: bool = False,
    random_seed: int = DEFAULT_RANDOM_SEED,
    lane_fractions: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    rng = random.Random(random_seed)
    eligible = [
        dict(row)
        for row in rows
        if include_known_retests or row.get("discovery_lane") != "known_result"
    ]
    if full_queue:
        eligible.sort(
            key=lambda row: (
                LANE_ORDER.index(row.get("discovery_lane"))
                if row.get("discovery_lane") in LANE_ORDER
                else 99,
                -_float_value(row.get("local_discovery_score")),
                str(row.get("first_indicator_id") or ""),
                str(row.get("second_indicator_id") or ""),
                str(row.get("probe_timeframe") or ""),
            )
        )
        return _assign_queue_metadata(eligible)

    max_pairs = max(1, int(max_pairs or DEFAULT_MAX_QUEUE_PAIRS))
    fractions = dict(DISCOVERY_LANE_FRACTIONS)
    if lane_fractions:
        fractions.update(lane_fractions)
    quotas = _lane_quotas(max_pairs, fractions)
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()
    first_counts: dict[str, int] = {}
    second_counts: dict[str, int] = {}
    indicator_cap = max(12, int(math.ceil(max_pairs * 0.045)))

    def add_from_lane(lane: str, quota: int) -> None:
        if quota <= 0:
            return
        lane_rows = [row for row in eligible if row.get("discovery_lane") == lane]
        lane_rows.sort(key=lambda row: _selection_sort_key(row, rng=rng, lane=lane))
        for pass_index in (0, 1):
            for row in lane_rows:
                if len([item for item in selected if item.get("discovery_lane") == lane]) >= quota:
                    return
                key = _pair_key(
                    row.get("first_indicator_id"),
                    row.get("second_indicator_id"),
                    row.get("probe_timeframe"),
                )
                if key in selected_keys:
                    continue
                first_id, second_id, _ = key
                if pass_index == 0 and (
                    first_counts.get(first_id, 0) >= indicator_cap
                    or second_counts.get(second_id, 0) >= indicator_cap
                ):
                    continue
                selected.append(row)
                selected_keys.add(key)
                first_counts[first_id] = first_counts.get(first_id, 0) + 1
                second_counts[second_id] = second_counts.get(second_id, 0) + 1

    for lane in ("proven_neighbor", "plausible_novel", "under_tested_role_correct", "wild_diversity"):
        add_from_lane(lane, quotas.get(lane, 0))

    if len(selected) < max_pairs:
        remaining = [
            row
            for row in eligible
            if _pair_key(
                row.get("first_indicator_id"),
                row.get("second_indicator_id"),
                row.get("probe_timeframe"),
            )
            not in selected_keys
        ]
        remaining.sort(
            key=lambda row: (
                -_float_value(row.get("local_discovery_score")),
                str(row.get("discovery_lane") or ""),
                str(row.get("first_indicator_id") or ""),
                str(row.get("second_indicator_id") or ""),
                str(row.get("probe_timeframe") or ""),
            )
        )
        selected.extend(remaining[: max_pairs - len(selected)])

    selected.sort(
        key=lambda row: (
            LANE_ORDER.index(row.get("discovery_lane"))
            if row.get("discovery_lane") in LANE_ORDER
            else 99,
            -_float_value(row.get("local_discovery_score")),
            str(row.get("first_indicator_id") or ""),
            str(row.get("second_indicator_id") or ""),
            str(row.get("probe_timeframe") or ""),
        )
    )
    return _assign_queue_metadata(selected[:max_pairs])


def _matrix_fieldnames() -> list[str]:
    return [
        "first_indicator_id",
        "first_signal_role",
        "first_strategy_role",
        "first_namespace",
        "first_base_indicator_id",
        "second_indicator_id",
        "second_signal_role",
        "second_strategy_role",
        "second_namespace",
        "second_base_indicator_id",
        "probe_timeframe",
        "instruments",
        "role_order_score",
        "strategy_pair_score",
        "first_signal_density_score",
        "first_signal_density_bucket",
        "first_active_percent",
        "first_event_count",
        "second_signal_density_score",
        "second_signal_density_bucket",
        "second_active_percent",
        "second_event_count",
        "first_forward_response_prior_score",
        "first_forward_response_prior_bucket",
        "first_best_forward_context",
        "first_strong_cell_count",
        "second_forward_response_prior_score",
        "second_forward_response_prior_bucket",
        "second_best_forward_context",
        "second_strong_cell_count",
        "best_forward_context",
        "first_static_prior_score",
        "second_static_prior_score",
        "first_best_slot_score",
        "second_best_slot_score",
        "first_best_sampling_lane",
        "second_best_sampling_lane",
        "first_tested_pair_count",
        "second_tested_pair_count",
        "first_positive_pair_count",
        "second_positive_pair_count",
        "neighbor_positive_pair_count",
        "known_pair_status",
        "known_pair_score",
        "known_pair_probe_id",
        "reverse_known_pair_score",
        "reverse_known_pair_probe_id",
        "shared_base_indicator",
        "local_discovery_score",
        "local_score_bucket",
        "discovery_lane",
        "discovery_reason",
    ]


def _queue_fieldnames() -> list[str]:
    return [
        "queue_rank",
        "probe_id",
        *_matrix_fieldnames(),
        "anchor_type",
        "anchor_id",
        "trigger_id",
        "pair_prior_score",
        "pair_prior_bucket",
        "anchor_timeframe",
        "profile_path",
        "result_dir",
    ]


def _result_fieldnames() -> list[str]:
    return [
        "probe_id",
        "queue_rank",
        "discovery_lane",
        "first_indicator_id",
        "second_indicator_id",
        "probe_timeframe",
        "local_discovery_score",
        "local_score_bucket",
        "known_pair_status",
        "profile_id",
        "output_dir",
        "status",
        "primary_score",
        "composite_score",
        "score_basis",
        "signal_count",
        "best_expectancy_r",
        "best_trades",
        "best_win_rate",
        "best_profit_factor",
        "error",
    ]


def _with_job_timeout_args(args: list[str], job_timeout_seconds: int | None) -> list[str]:
    if job_timeout_seconds is None or int(job_timeout_seconds) <= 0:
        return list(args)
    cleaned: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg == "--job-timeout-seconds":
            skip_next = True
            continue
        cleaned.append(arg)
    return ["--job-timeout-seconds", str(int(job_timeout_seconds)), *cleaned]


def build_discovery_pair_atlas(
    config: AppConfig,
    *,
    indicator_atlas_dir: Path | None = None,
    signal_atlas_dir: Path | None = None,
    forward_response_dir: Path | None = None,
    recipe_priors_dir: Path | None = None,
    anchor_pair_dir: Path | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    refresh_static_atlas: bool = False,
    first_ids: list[str] | None = None,
    second_ids: list[str] | None = None,
    instruments: list[str] | None = None,
    timeframes: list[str] | None = None,
    max_pairs: int = DEFAULT_MAX_QUEUE_PAIRS,
    full_queue: bool = False,
    include_known_retests: bool = False,
    random_seed: int = DEFAULT_RANDOM_SEED,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    as_of_date: str | None = None,
    job_timeout_seconds: int | None = DEFAULT_JOB_TIMEOUT_SECONDS,
    emit_profile_docs: bool = True,
    quality_score_preset: str = DEFAULT_QUALITY_SCORE_PRESET,
    execution_cost_mode: str = DEFAULT_EXECUTION_COST_MODE,
) -> DiscoveryPairAtlasBuildResult:
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_PAIR_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = target_dir / "profiles"
    result_root = target_dir / "probe-results"
    if emit_profile_docs:
        profile_dir.mkdir(parents=True, exist_ok=True)
    result_root.mkdir(parents=True, exist_ok=True)

    indicator_dir = (
        indicator_atlas_dir.expanduser().resolve()
        if indicator_atlas_dir is not None
        else config.derived_root / DEFAULT_ATLAS_DIRNAME
    )
    static_atlas_path = indicator_dir / "indicator-atlas.json"
    if refresh_static_atlas or not static_atlas_path.exists():
        static_result = build_indicator_atlas(
            config,
            workspace_root=workspace_root,
            catalog_path=catalog_path,
            out_dir=indicator_dir,
        )
        static_atlas_path = static_result.atlas_path
    if not static_atlas_path.exists() and not (indicator_dir / "indicator-atlas.csv").exists():
        raise FileNotFoundError(
            f"Missing indicator atlas at {indicator_dir}. Run `uv run build-indicator-atlas` first."
        )

    signal_dir = (
        signal_atlas_dir.expanduser().resolve()
        if signal_atlas_dir is not None
        else config.derived_root / DEFAULT_SIGNAL_ATLAS_DIRNAME
    )
    forward_dir = (
        forward_response_dir.expanduser().resolve()
        if forward_response_dir is not None
        else config.derived_root / DEFAULT_FORWARD_RESPONSE_DIRNAME
    )
    priors_dir = (
        recipe_priors_dir.expanduser().resolve()
        if recipe_priors_dir is not None
        else config.derived_root / DEFAULT_RECIPE_PRIORS_DIRNAME
    )
    layer3_dir = (
        anchor_pair_dir.expanduser().resolve()
        if anchor_pair_dir is not None
        else config.derived_root / "anchor-pair-atlas"
    )

    static_rows = _indicator_rows(indicator_dir)
    rows_by_id = _rows_by_id(static_rows)
    signal_rollups = _optional_signal_rollups(signal_dir)
    forward_priors = _optional_forward_priors(forward_dir)
    slot_priors = _slot_prior_rollups(priors_dir)
    known_pairs, indicator_pair_rollups = _known_pair_evidence(layer3_dir, priors_dir)
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    catalog_by_id = _catalog_by_id(catalog_payload)

    instrument_panel = _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS)
    timeframe_panel = _normalize_tokens(timeframes) or list(DEFAULT_TIMEFRAMES)
    lookback_months = max(1, int(lookback_months or DEFAULT_LOOKBACK_MONTHS))
    resolved_as_of_date = resolve_probe_as_of_date(as_of_date)
    rows = build_discovery_pair_rows(
        rows_by_id=rows_by_id,
        signal_rollups=signal_rollups,
        forward_priors=forward_priors,
        slot_priors=slot_priors,
        known_pairs=known_pairs,
        indicator_pair_rollups=indicator_pair_rollups,
        first_ids=first_ids,
        second_ids=second_ids,
        timeframes=timeframe_panel,
        instruments=instrument_panel,
    )
    queue_rows = select_discovery_pair_queue(
        rows,
        max_pairs=max_pairs,
        full_queue=full_queue,
        include_known_retests=include_known_retests,
        random_seed=random_seed,
    )

    probes: list[dict[str, Any]] = []
    exe, base_args = _fuzzfolio_base_args(config)
    for row in queue_rows:
        first_id = _clean_upper(row.get("first_indicator_id"))
        second_id = _clean_upper(row.get("second_indicator_id"))
        if first_id not in catalog_by_id or second_id not in catalog_by_id:
            continue
        probe_timeframe = _clean_upper(row.get("probe_timeframe"))
        anchor_timeframe = _anchor_timeframe(
            catalog_by_id[first_id],
            probe_timeframe=probe_timeframe,
        )
        probe_id = _clean_token(row.get("probe_id"))
        profile_path = profile_dir / f"{probe_id}.json"
        result_dir = result_root / probe_id
        if emit_profile_docs:
            profile_doc = build_pair_profile_document(
                catalog_by_id=catalog_by_id,
                anchor_id=first_id,
                trigger_id=second_id,
                anchor_type="discovery",
                probe_timeframe=probe_timeframe,
                anchor_timeframe=anchor_timeframe,
                instruments=instrument_panel,
                probe_id=probe_id,
            )
            profile = _as_dict(profile_doc.get("profile"))
            profile["name"] = f"Discovery Pair {first_id}+{second_id} {probe_timeframe}"
            profile["description"] = (
                "Temporary AutoResearch discovery pair probe profile. "
                "This tests an ordered two-indicator combination selected from the "
                "broad discovery queue; backend P&L evidence is the source of truth."
            )
            _write_json(profile_path, profile_doc)
        row["anchor_timeframe"] = anchor_timeframe
        row["profile_path"] = str(profile_path)
        row["result_dir"] = str(result_dir)
        sensitivity_args = _sensitivity_args_for_row(
            row,
            lookback_months=lookback_months,
            as_of_date=resolved_as_of_date,
            quality_score_preset=quality_score_preset,
            execution_cost_mode=execution_cost_mode,
            result_dir=result_dir,
        )
        sensitivity_args = _with_job_timeout_args(sensitivity_args, job_timeout_seconds)
        probes.append(
            {
                "probe_id": probe_id,
                "queue_rank": row.get("queue_rank"),
                "discovery_lane": row.get("discovery_lane"),
                "first_indicator_id": first_id,
                "second_indicator_id": second_id,
                "probe_timeframe": probe_timeframe,
                "anchor_timeframe": anchor_timeframe,
                "profile_path": str(profile_path),
                "output_dir": str(result_dir),
                "create_profile_args": ["profiles", "create", "--file", str(profile_path), "--pretty"],
                "sensitivity_basket_args": sensitivity_args,
                "local_discovery_score": row.get("local_discovery_score"),
            }
        )

    lane_counts: dict[str, int] = {}
    queue_lane_counts: dict[str, int] = {}
    known_status_counts: dict[str, int] = {}
    for row in rows:
        lane = _clean_token(row.get("discovery_lane")) or "unknown"
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
        status = _clean_token(row.get("known_pair_status")) or "unknown"
        known_status_counts[status] = known_status_counts.get(status, 0) + 1
    for row in queue_rows:
        lane = _clean_token(row.get("discovery_lane")) or "unknown"
        queue_lane_counts[lane] = queue_lane_counts.get(lane, 0) + 1

    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "indicator_atlas_dir": str(indicator_dir),
            "signal_atlas_dir": str(signal_dir),
            "forward_response_dir": str(forward_dir),
            "recipe_priors_dir": str(priors_dir),
            "anchor_pair_dir": str(layer3_dir),
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
        },
        "selection": {
            "first_ids": _normalize_tokens(first_ids),
            "second_ids": _normalize_tokens(second_ids),
            "instruments": instrument_panel,
            "timeframes": timeframe_panel,
            "max_pairs": None if full_queue else max_pairs,
            "full_queue": full_queue,
            "include_known_retests": include_known_retests,
            "random_seed": random_seed,
            "lane_fractions": DISCOVERY_LANE_FRACTIONS,
            "lookback_months": lookback_months,
            "as_of_date": resolved_as_of_date,
            "job_timeout_seconds": job_timeout_seconds,
            "quality_score_preset": quality_score_preset,
            "execution_cost_mode": execution_cost_mode,
        },
        "result_counts": {
            "eligible_indicators": len(
                [row for row in static_rows if _bool_value(row.get("generation_eligible"))]
            ),
            "pair_matrix_rows": len(rows),
            "queue_rows": len(queue_rows),
            "profile_docs": len(probes) if emit_profile_docs else 0,
            "discovery_lane_counts": dict(sorted(lane_counts.items())),
            "queue_lane_counts": dict(sorted(queue_lane_counts.items())),
            "known_pair_status_counts": dict(sorted(known_status_counts.items())),
            "known_pair_evidence_rows": len(known_pairs),
        },
        "top_queue": [
            {
                "queue_rank": row.get("queue_rank"),
                "probe_id": row.get("probe_id"),
                "lane": row.get("discovery_lane"),
                "first_indicator_id": row.get("first_indicator_id"),
                "second_indicator_id": row.get("second_indicator_id"),
                "probe_timeframe": row.get("probe_timeframe"),
                "local_discovery_score": row.get("local_discovery_score"),
                "known_pair_status": row.get("known_pair_status"),
                "discovery_reason": row.get("discovery_reason"),
            }
            for row in queue_rows[:15]
        ],
    }

    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "pair_rows": rows,
        "queue_rows": queue_rows,
        "run_manifest": {
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    }

    atlas_path = target_dir / "discovery-pair-atlas.json"
    matrix_csv_path = target_dir / "discovery-pair-matrix.csv"
    queue_csv_path = target_dir / "discovery-pair-queue.csv"
    manifest_path = target_dir / "discovery-pair-run-manifest.json"
    run_script_path = target_dir / "run-discovery-pair-probes.ps1"
    summary_path = target_dir / "discovery-pair-summary.json"

    _write_json(atlas_path, atlas_payload)
    _write_csv(matrix_csv_path, rows, _matrix_fieldnames())
    _write_csv(queue_csv_path, queue_rows, _queue_fieldnames())
    _write_json(
        manifest_path,
        {
            "schema_version": "discovery_pair_run_manifest_v1",
            "generated_at": summary["generated_at"],
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    )
    _write_run_script(
        run_script_path,
        exe=exe,
        base_args=base_args,
        probes=probes,
        generated_by="uv run build-discovery-pair-atlas",
        description="Runs the queued broad discovery pair sensitivity-basket probes.",
    )
    _write_json(summary_path, summary)

    return DiscoveryPairAtlasBuildResult(
        atlas_path=atlas_path,
        matrix_csv_path=matrix_csv_path,
        queue_csv_path=queue_csv_path,
        manifest_path=manifest_path,
        run_script_path=run_script_path,
        profile_dir=profile_dir,
        summary_path=summary_path,
        summary=summary,
    )


def _probe_rows_by_id(queue_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        _clean_token(row.get("probe_id")): row
        for row in queue_rows
        if _clean_token(row.get("probe_id"))
    }


def _select_probe_rows(
    queue_rows: list[dict[str, Any]],
    *,
    probe_ids: list[str] | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    requested = [_clean_token(value) for value in probe_ids or [] if _clean_token(value)]
    if requested:
        by_id = _probe_rows_by_id(queue_rows)
        selected = [by_id[probe_id] for probe_id in requested if probe_id in by_id]
    else:
        selected = sorted(queue_rows, key=lambda row: _int_value(row.get("queue_rank"), 1_000_000))
    if limit is not None and limit >= 0:
        selected = selected[: int(limit)]
    return [dict(row) for row in selected]


def _result_row_from_discovery_score(
    row: dict[str, Any],
    *,
    profile_id: str | None,
    output_dir: Path,
    status: str,
    score_payload: Any | None = None,
    sensitivity_snapshot: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    anchor_result = _result_row_from_score(
        row,
        profile_id=profile_id,
        output_dir=output_dir,
        status=status,
        score_payload=score_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        error=error,
    )
    return {
        "probe_id": row.get("probe_id"),
        "queue_rank": row.get("queue_rank"),
        "discovery_lane": row.get("discovery_lane"),
        "first_indicator_id": row.get("first_indicator_id") or row.get("anchor_id"),
        "second_indicator_id": row.get("second_indicator_id") or row.get("trigger_id"),
        "probe_timeframe": row.get("probe_timeframe"),
        "local_discovery_score": row.get("local_discovery_score") or row.get("pair_prior_score"),
        "local_score_bucket": row.get("local_score_bucket"),
        "known_pair_status": row.get("known_pair_status"),
        "profile_id": profile_id,
        "output_dir": str(output_dir),
        "status": status,
        "primary_score": anchor_result.get("primary_score"),
        "composite_score": anchor_result.get("composite_score"),
        "score_basis": anchor_result.get("score_basis"),
        "signal_count": anchor_result.get("signal_count"),
        "best_expectancy_r": anchor_result.get("best_expectancy_r"),
        "best_trades": anchor_result.get("best_trades"),
        "best_win_rate": anchor_result.get("best_win_rate"),
        "best_profit_factor": anchor_result.get("best_profit_factor"),
        "error": error,
    }


def run_discovery_pair_probes(
    config: AppConfig,
    *,
    atlas_dir: Path | None = None,
    probe_ids: list[str] | None = None,
    limit: int | None = None,
    force: bool = False,
    keep_profiles: bool = False,
    timeout_seconds: int | None = DEFAULT_PROBE_TIMEOUT_SECONDS,
    job_timeout_seconds: int | None = DEFAULT_JOB_TIMEOUT_SECONDS,
    probe_workers: int = DEFAULT_PROBE_WORKERS,
    progress_callback: Any | None = None,
) -> DiscoveryPairProbeRunResult:
    source_dir = (
        atlas_dir.expanduser().resolve()
        if atlas_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_PAIR_DIRNAME
    )
    atlas_path = source_dir / "discovery-pair-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing discovery pair atlas at {atlas_path}. "
            "Run `uv run build-discovery-pair-atlas` first."
        )
    payload = _as_dict(_load_json(atlas_path))
    queue_rows = [row for row in _as_list(payload.get("queue_rows")) if isinstance(row, dict)]
    manifest = _as_dict(payload.get("run_manifest"))
    manifest_probes = {
        _clean_token(row.get("probe_id")): row
        for row in _as_list(manifest.get("probes"))
        if isinstance(row, dict) and _clean_token(row.get("probe_id"))
    }
    selected_rows = _select_probe_rows(queue_rows, probe_ids=probe_ids, limit=limit)

    results: list[dict[str, Any]] = []
    FuzzfolioCli(config.fuzzfolio).ensure_login()

    def run_one(row: dict[str, Any]) -> dict[str, Any]:
        probe_id = _clean_token(row.get("probe_id"))
        manifest_probe = _as_dict(manifest_probes.get(probe_id))
        profile_path = Path(_clean_token(manifest_probe.get("profile_path") or row.get("profile_path")))
        output_dir = Path(_clean_token(manifest_probe.get("output_dir") or row.get("result_dir")))
        if not profile_path.is_absolute():
            profile_path = (config.repo_root / profile_path).resolve()
        if not output_dir.is_absolute():
            output_dir = (config.repo_root / output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        sensitivity_path = output_dir / "sensitivity-response.json"
        cli = FuzzfolioCli(config.fuzzfolio)
        if sensitivity_path.exists() and not force:
            try:
                compare_payload = cli.score_artifact(output_dir)
                snapshot = load_sensitivity_snapshot(output_dir)
                score = build_attempt_score(compare_payload, snapshot)
                return _result_row_from_discovery_score(
                    row,
                    profile_id=None,
                    output_dir=output_dir,
                    status="skipped_existing",
                    score_payload=score,
                    sensitivity_snapshot=snapshot,
                )
            except Exception as exc:
                return _result_row_from_discovery_score(
                    row,
                    profile_id=None,
                    output_dir=output_dir,
                    status="skipped_existing_unscored",
                    error=str(exc)[:500],
                )

        profile_id: str | None = None
        try:
            profile_id = cli.create_cloud_profile(profile_path)
            sensitivity_args = [
                str(value)
                for value in _as_list(manifest_probe.get("sensitivity_basket_args"))
            ]
            if not sensitivity_args:
                sensitivity_args = _sensitivity_args_for_row(
                    row,
                    lookback_months=DEFAULT_LOOKBACK_MONTHS,
                    as_of_date=None,
                    quality_score_preset=DEFAULT_QUALITY_SCORE_PRESET,
                    execution_cost_mode=DEFAULT_EXECUTION_COST_MODE,
                    result_dir=output_dir,
                )
            sensitivity_args = _with_job_timeout_args(sensitivity_args, job_timeout_seconds)
            sensitivity_args = _replace_profile_id_arg(sensitivity_args, profile_id)
            cli.run(sensitivity_args, timeout_seconds=timeout_seconds)
            compare_payload = cli.score_artifact(output_dir)
            snapshot = load_sensitivity_snapshot(output_dir)
            score = build_attempt_score(compare_payload, snapshot)
            return _result_row_from_discovery_score(
                row,
                profile_id=profile_id,
                output_dir=output_dir,
                status="ok",
                score_payload=score,
                sensitivity_snapshot=snapshot,
            )
        except Exception as exc:
            return _result_row_from_discovery_score(
                row,
                profile_id=profile_id,
                output_dir=output_dir,
                status="failed",
                error=str(exc)[:500],
            )
        finally:
            if profile_id and not keep_profiles:
                try:
                    cli.run(
                        ["profiles", "delete", "--profile-ref", profile_id, "--pretty"],
                        timeout_seconds=timeout_seconds,
                    )
                except CliError:
                    pass

    worker_count = max(1, int(probe_workers or 1))
    completed = 0
    if worker_count == 1 or len(selected_rows) <= 1:
        for row in selected_rows:
            result_row = run_one(row)
            results.append(result_row)
            completed += 1
            if progress_callback:
                progress_callback(
                    {
                        "completed": completed,
                        "total": len(selected_rows),
                        "probe_id": result_row.get("probe_id"),
                        "status": result_row.get("status"),
                    }
                )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(run_one, row): row for row in selected_rows}
            for future in as_completed(futures):
                result_row = future.result()
                results.append(result_row)
                completed += 1
                if progress_callback:
                    progress_callback(
                        {
                            "completed": completed,
                            "total": len(selected_rows),
                            "probe_id": result_row.get("probe_id"),
                            "status": result_row.get("status"),
                        }
                    )

    results.sort(key=lambda row: _int_value(row.get("queue_rank"), 1_000_000))

    results_csv_path = source_dir / "discovery-pair-probe-results.csv"
    summary_path = source_dir / "discovery-pair-probe-summary.json"
    _write_csv(results_csv_path, results, _result_fieldnames())
    status_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    positive_by_lane: dict[str, int] = {}
    scored_by_lane: dict[str, int] = {}
    for row in results:
        status = _clean_token(row.get("status")) or "unknown"
        lane = _clean_token(row.get("discovery_lane")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
        if row.get("composite_score") is not None and row.get("composite_score") != "":
            scored_by_lane[lane] = scored_by_lane.get(lane, 0) + 1
            if _float_value(row.get("composite_score")) >= 50.0:
                positive_by_lane[lane] = positive_by_lane.get(lane, 0) + 1
    scored = [
        row
        for row in results
        if row.get("composite_score") is not None and row.get("composite_score") != ""
    ]
    scored.sort(key=lambda row: -_float_value(row.get("composite_score")))
    top_by_lane: dict[str, list[dict[str, Any]]] = {}
    for row in scored:
        lane = _clean_token(row.get("discovery_lane")) or "unknown"
        bucket = top_by_lane.setdefault(lane, [])
        if len(bucket) < 5:
            bucket.append(row)
    summary = {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "discovery_pair_atlas_path": str(atlas_path),
        },
        "selection": {
            "requested_probe_ids": probe_ids or [],
            "limit": limit,
            "force": force,
            "keep_profiles": keep_profiles,
            "timeout_seconds": timeout_seconds,
            "job_timeout_seconds": job_timeout_seconds,
            "probe_workers": worker_count,
        },
        "result_counts": {
            "selected": len(selected_rows),
            "completed": len(results),
            "status_counts": dict(sorted(status_counts.items())),
            "lane_counts": dict(sorted(lane_counts.items())),
            "scored": len(scored),
            "scored_by_lane": dict(sorted(scored_by_lane.items())),
            "positive_by_lane": dict(sorted(positive_by_lane.items())),
        },
        "top_scored": scored[:20],
        "top_by_lane": top_by_lane,
    }
    _write_json(summary_path, summary)
    return DiscoveryPairProbeRunResult(
        results_csv_path=results_csv_path,
        summary_path=summary_path,
        summary=summary,
    )
