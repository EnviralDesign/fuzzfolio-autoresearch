from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .indicator_atlas import DEFAULT_ATLAS_DIRNAME, RECIPE_DEFINITIONS
from .signal_atlas import DEFAULT_SIGNAL_ATLAS_DIRNAME
from .forward_response_atlas import DEFAULT_FORWARD_RESPONSE_DIRNAME
from .anchor_pair_atlas import (
    DEFAULT_ANCHOR_PAIR_DIRNAME,
    DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME,
)


SCHEMA_VERSION = "empirical_recipe_priors_v1"
DEFAULT_RECIPE_PRIORS_DIRNAME = "recipe-priors"
DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME = "discovery-recipe-validation-atlas"
DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME = "discovery-recipe-scrutiny-atlas"

RECIPE_BY_ANCHOR_TYPE: dict[str, str] = {
    "trend": "trend_pullback_continuation",
    "mean_reversion": "mean_reversion_reclaim",
    "compression": "breakout_compression_release",
    "profile_value": "profile_value_context",
}

DENSITY_SCORE: dict[str, float] = {
    "usable": 88.0,
    "sparse": 62.0,
    "dense": 50.0,
    "very_sparse": 35.0,
    "saturated": 20.0,
    "flat": 0.0,
    "no_data": 0.0,
}

DISCOVERY_RECIPE_INCLUDED_BUCKETS: dict[str, str] = {
    "retained_strong": "high_prior",
    "retained": "high_prior",
    "partial_retention": "uncertain_prior",
    "new_strong_cluster_expansion": "medium_prior",
    "new_positive_cluster_expansion": "uncertain_prior",
}

DISCOVERY_RECIPE_BUCKET_ADJUSTMENT: dict[str, float] = {
    "retained_strong": 8.0,
    "retained": 4.0,
    "partial_retention": -4.0,
    "new_strong_cluster_expansion": 3.0,
    "new_positive_cluster_expansion": -6.0,
}

DISCOVERY_RECIPE_NEGATIVE_BUCKETS = {
    "failed_retention",
    "new_failed_cluster_expansion",
    "new_low_cluster_expansion",
}

TEMPLATE_CONFIG_KEYS = (
    "timeframe",
    "lookbackBars",
    "ranges",
    "talibConfig",
    "weight",
    "isTrendFollowing",
    "normalizationMode",
    "useFormingBar",
    "scale",
)


@dataclass(frozen=True)
class RecipePriorsBuildResult:
    priors_path: Path
    slot_csv_path: Path
    pair_csv_path: Path
    pair_negative_csv_path: Path
    cluster_negative_csv_path: Path
    retention_failures_csv_path: Path
    seed_plan_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "recipe_priors_json": str(self.priors_path),
            "slot_indicator_priors_csv": str(self.slot_csv_path),
            "pair_priors_csv": str(self.pair_csv_path),
            "pair_negative_priors_csv": str(self.pair_negative_csv_path),
            "cluster_expansion_negative_priors_csv": str(self.cluster_negative_csv_path),
            "retention_failures_csv": str(self.retention_failures_csv_path),
            "play_hand_seed_plan_json": str(self.seed_plan_path),
            "recipe_priors_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _float_value(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _int_value(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _indicator_rows(indicator_atlas_dir: Path) -> list[dict[str, Any]]:
    csv_path = indicator_atlas_dir / "indicator-atlas.csv"
    if csv_path.exists():
        return _read_csv_rows(csv_path)
    atlas_path = indicator_atlas_dir / "indicator-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing indicator atlas at {indicator_atlas_dir}. Run `uv run build-indicator-atlas` first."
        )
    payload = _as_dict(_load_json(atlas_path))
    return [row for row in _as_list(payload.get("indicators")) if isinstance(row, dict)]


def _signal_rollups(signal_atlas_dir: Path) -> dict[str, dict[str, Any]]:
    summary_path = signal_atlas_dir / "signal-atlas-summary.json"
    if not summary_path.exists():
        return {}
    payload = _as_dict(_load_json(summary_path))
    return {
        _clean_upper(indicator_id): _as_dict(row)
        for indicator_id, row in _as_dict(payload.get("indicator_rollups")).items()
        if _clean_upper(indicator_id)
    }


def _forward_priors(forward_response_dir: Path) -> dict[str, dict[str, Any]]:
    return {
        _clean_upper(row.get("indicator_id")): row
        for row in _read_csv_rows(forward_response_dir / "forward-response-priors.csv")
        if _clean_upper(row.get("indicator_id"))
    }


def _static_slot_scores(indicator_atlas_dir: Path) -> dict[tuple[str, str, str], float]:
    path = indicator_atlas_dir / "recipe-priors.json"
    if not path.exists():
        return {}
    payload = _as_dict(_load_json(path))
    scores: dict[tuple[str, str, str], float] = {}
    recipes = _as_dict(payload.get("recipes"))
    for recipe_name, recipe_payload in recipes.items():
        slots = _as_dict(_as_dict(recipe_payload).get("slots"))
        for slot_name, candidates in slots.items():
            for item in _as_list(candidates):
                if not isinstance(item, dict):
                    continue
                indicator_id = _clean_upper(item.get("id"))
                if indicator_id:
                    scores[(str(recipe_name), str(slot_name), indicator_id)] = _float_value(
                        item.get("recipe_slot_prior")
                    )
    return scores


def _row_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        _clean_upper(row.get("id")): row
        for row in rows
        if _clean_upper(row.get("id"))
    }


def _slot_roles(recipe_name: str, slot_name: str) -> tuple[str, ...]:
    definition = _as_dict(RECIPE_DEFINITIONS.get(recipe_name))
    roles = _as_dict(definition.get("slots")).get(slot_name)
    if isinstance(roles, tuple):
        return roles
    if isinstance(roles, list):
        return tuple(str(role) for role in roles)
    return ()


def _fallback_static_slot_score(
    indicator_row: dict[str, Any],
    *,
    recipe_name: str,
    slot_name: str,
) -> float:
    signal_role = str(indicator_row.get("signal_role") or "").lower()
    strategy_role = str(indicator_row.get("strategy_role") or "").lower()
    definition = _as_dict(RECIPE_DEFINITIONS.get(recipe_name))
    slot_roles = _slot_roles(recipe_name, slot_name)
    score = _float_value(indicator_row.get("static_prior_score"), 45.0)
    score += 22.0 if signal_role in slot_roles else -12.0
    score += _float_value(_as_dict(definition.get("strategy_weights")).get(strategy_role))
    if not _bool_value(indicator_row.get("generation_eligible")):
        score -= 50.0
    return round(_clamp(score), 2)


def _signal_score(signal_rollup: dict[str, Any] | None) -> tuple[float, str]:
    if not signal_rollup:
        return 42.0, "unknown"
    bucket = _clean_token(signal_rollup.get("density_bucket")) or "unknown"
    score = DENSITY_SCORE.get(bucket, 50.0)
    balance_counts = _as_dict(signal_rollup.get("balance_bucket_counts"))
    if balance_counts.get("balanced"):
        score += 6.0
    if bucket in {"flat", "no_data"}:
        score = 0.0
    return round(_clamp(score), 2), bucket


def _forward_score(forward_prior: dict[str, Any] | None) -> tuple[float, str]:
    if not forward_prior:
        return 42.0, "unknown"
    return (
        round(_clamp(_float_value(forward_prior.get("forward_response_prior_score"), 50.0)), 2),
        _clean_token(forward_prior.get("forward_response_prior_bucket")) or "unknown",
    )


def _empty_stats() -> dict[str, Any]:
    return {
        "count": 0,
        "positive_count": 0,
        "zero_count": 0,
        "best_score": None,
        "avg_score": None,
        "best_probe_id": None,
        "best_timeframe": None,
    }


def _add_score_stat(stats: dict[str, Any], *, score: float, probe_id: str, timeframe: str) -> None:
    count = int(stats.get("count") or 0)
    total = _float_value(stats.get("_total_score"))
    stats["count"] = count + 1
    stats["_total_score"] = total + score
    stats["avg_score"] = round((total + score) / float(count + 1), 4)
    if score >= 50.0:
        stats["positive_count"] = int(stats.get("positive_count") or 0) + 1
    if score <= 0.0:
        stats["zero_count"] = int(stats.get("zero_count") or 0) + 1
    best_score = stats.get("best_score")
    if best_score is None or score > _float_value(best_score):
        stats["best_score"] = round(score, 4)
        stats["best_probe_id"] = probe_id
        stats["best_timeframe"] = timeframe


def build_pair_evidence(
    pair_rows: list[dict[str, Any]],
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    list[dict[str, Any]],
]:
    trigger_stats: dict[tuple[str, str], dict[str, Any]] = {}
    anchor_stats: dict[tuple[str, str], dict[str, Any]] = {}
    pair_priors: list[dict[str, Any]] = []
    for row in pair_rows:
        anchor_type = _clean_token(row.get("anchor_type"))
        recipe_name = RECIPE_BY_ANCHOR_TYPE.get(anchor_type)
        if not recipe_name:
            continue
        score = _float_value(row.get("composite_score"))
        anchor_id = _clean_upper(row.get("anchor_id"))
        trigger_id = _clean_upper(row.get("trigger_id"))
        probe_id = _clean_token(row.get("probe_id"))
        timeframe = _clean_upper(row.get("probe_timeframe"))
        if trigger_id:
            stats = trigger_stats.setdefault((recipe_name, trigger_id), _empty_stats())
            _add_score_stat(stats, score=score, probe_id=probe_id, timeframe=timeframe)
        if anchor_id:
            stats = anchor_stats.setdefault((recipe_name, anchor_id), _empty_stats())
            _add_score_stat(stats, score=score, probe_id=probe_id, timeframe=timeframe)
        pair_priors.append(
            {
                "source": "anchor_pair_atlas",
                "recipe": recipe_name,
                "anchor_type": anchor_type,
                "anchor_id": anchor_id,
                "trigger_id": trigger_id,
                "probe_timeframe": timeframe,
                "probe_id": probe_id,
                "pair_prior_score": row.get("pair_prior_score"),
                "composite_score": score,
                "signal_count": row.get("signal_count"),
                "best_expectancy_r": row.get("best_expectancy_r"),
                "best_trades": row.get("best_trades"),
                "best_profit_factor": row.get("best_profit_factor"),
            }
        )
    for stats in [*trigger_stats.values(), *anchor_stats.values()]:
        stats.pop("_total_score", None)
    return trigger_stats, anchor_stats, pair_priors


def build_timing_evidence(
    timing_rows: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    evidence: dict[tuple[str, str], dict[str, Any]] = {}
    for row in timing_rows:
        recipe_name = RECIPE_BY_ANCHOR_TYPE.get(_clean_token(row.get("anchor_type")))
        trigger_id = _clean_upper(row.get("trigger_id"))
        if not recipe_name or not trigger_id:
            continue
        item = evidence.setdefault(
            (recipe_name, trigger_id),
            {
                "count": 0,
                "positive_delta_count": 0,
                "material_improved_count": 0,
                "degraded_count": 0,
                "lost_positive_count": 0,
                "best_delta": None,
                "best_variant_lookback_bars": None,
                "best_timing_probe_id": None,
            },
        )
        delta = _float_value(row.get("score_delta"))
        item["count"] = int(item.get("count") or 0) + 1
        if delta > 0:
            item["positive_delta_count"] = int(item.get("positive_delta_count") or 0) + 1
        if delta >= 5.0:
            item["material_improved_count"] = int(item.get("material_improved_count") or 0) + 1
        bucket = _clean_token(row.get("timing_bucket"))
        if bucket == "degraded":
            item["degraded_count"] = int(item.get("degraded_count") or 0) + 1
        if bucket == "lost_positive":
            item["lost_positive_count"] = int(item.get("lost_positive_count") or 0) + 1
        best_delta = item.get("best_delta")
        if best_delta is None or delta > _float_value(best_delta):
            item["best_delta"] = round(delta, 4)
            item["best_variant_lookback_bars"] = row.get("variant_lookback_bars")
            item["best_timing_probe_id"] = row.get("timing_probe_id")
    return evidence


def timing_policy_for(evidence: dict[str, Any] | None) -> tuple[str, float, str | None]:
    if not evidence:
        return "catalog_default", 0.0, None
    material = _int_value(evidence.get("material_improved_count"))
    lost = _int_value(evidence.get("lost_positive_count"))
    degraded = _int_value(evidence.get("degraded_count"))
    best_delta = _float_value(evidence.get("best_delta"))
    if material > 0 and lost == 0:
        return "allow_variant", min(8.0, max(2.0, best_delta * 0.20)), _clean_token(
            evidence.get("best_variant_lookback_bars")
        )
    if lost > 0 or degraded >= 2:
        return "catalog_default_only", -8.0 if lost else -4.0, None
    if best_delta > 0:
        return "watch_variant", min(3.0, best_delta * 0.15), _clean_token(
            evidence.get("best_variant_lookback_bars")
        )
    return "catalog_default", 0.0, None


def sampling_lane(
    score: float,
    *,
    empirical_count: int,
    positive_pair_count: int = 0,
    default_problem: bool = False,
    has_behavior_evidence: bool = False,
) -> str:
    if default_problem:
        return "blocked_default_problem"
    if empirical_count > 0:
        if positive_pair_count > 0 and score >= 60.0:
            return "high_prior"
        if score >= 50.0:
            return "medium_prior"
        if score >= 35.0:
            return "uncertain_prior"
        return "wild_exploration"
    if has_behavior_evidence and score >= 78.0:
        return "medium_prior"
    if score >= 55.0:
        return "uncertain_prior"
    return "wild_exploration"


def sampling_weight(score: float, lane: str) -> float:
    if lane == "blocked_default_problem":
        return 0.0
    lane_multiplier = {
        "high_prior": 1.35,
        "medium_prior": 0.8,
        "uncertain_prior": 0.24,
        "wild_exploration": 0.08,
    }.get(lane, 0.20)
    return round(max(0.1, ((score / 100.0) ** 2) * 100.0 * lane_multiplier), 4)


def pair_sampling_lane(score: float) -> str:
    if score >= 65.0:
        return "high_prior"
    if score >= 60.0:
        return "medium_prior"
    if score >= 40.0:
        return "uncertain_prior"
    return "wild_exploration"


def _retention_bucket_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        bucket = _clean_token(row.get("retention_bucket")) or "unknown"
        counts[bucket] = counts.get(bucket, 0) + 1
    return dict(sorted(counts.items()))


def _discovery_recipe_score(row: dict[str, Any]) -> float:
    validation_score = _float_value(
        row.get("primary_score"),
        _float_value(row.get("composite_score")),
    )
    discovery_score = _float_value(row.get("discovery_evidence_score"))
    priority_score = _float_value(row.get("validation_priority_score"))
    bucket = _clean_token(row.get("retention_bucket"))
    score = (validation_score * 0.78) + (discovery_score * 0.12) + (priority_score * 0.05)
    score += DISCOVERY_RECIPE_BUCKET_ADJUSTMENT.get(bucket, 0.0)
    return round(_clamp(score), 4)


def _discovery_slot_row(
    *,
    recipe_id: str,
    slot_name: str,
    indicator_id: str,
    indicator_row: dict[str, Any] | None,
    result_row: dict[str, Any],
    recipe_slot_score: float,
) -> dict[str, Any]:
    bucket = _clean_token(result_row.get("retention_bucket"))
    lane = DISCOVERY_RECIPE_INCLUDED_BUCKETS.get(
        bucket,
        pair_sampling_lane(recipe_slot_score),
    )
    validation_score = _float_value(
        result_row.get("primary_score"),
        _float_value(result_row.get("composite_score")),
    )
    return {
        "source": "discovery_recipe_validation",
        "recipe": recipe_id,
        "slot": slot_name,
        "indicator_id": indicator_id,
        "signal_role": _as_dict(indicator_row).get("signal_role"),
        "strategy_role": _as_dict(indicator_row).get("strategy_role"),
        "namespace": _as_dict(indicator_row).get("namespace"),
        "slot_role_fit": "discovered_cluster",
        "static_slot_score": None,
        "signal_density_score": None,
        "signal_density_bucket": None,
        "forward_response_score": None,
        "forward_response_bucket": None,
        "empirical_pair_count": 1,
        "positive_pair_count": 1 if validation_score >= 50.0 else 0,
        "best_pair_score": round(validation_score, 4),
        "avg_pair_score": round(validation_score, 4),
        "best_pair_probe_id": result_row.get("probe_id"),
        "best_pair_timeframe": _clean_upper(result_row.get("probe_timeframe")),
        "timing_policy": "catalog_default",
        "timing_adjustment": 0.0,
        "recommended_trigger_lookback_bars": None,
        "recipe_slot_score": recipe_slot_score,
        "sampling_lane": lane,
        "sampling_weight": sampling_weight(recipe_slot_score, lane),
        "retention_bucket": bucket,
        "validation_probe_id": result_row.get("probe_id"),
        "validation_score": round(validation_score, 4),
        "retention_ratio": result_row.get("retention_ratio"),
    }


def _load_recommended_profile_template(
    profile_dir: Path | list[Path] | tuple[Path, ...] | None,
    row: dict[str, Any],
) -> dict[str, Any] | None:
    if profile_dir is None:
        return None
    probe_id = _clean_token(row.get("probe_id"))
    if not probe_id:
        return None
    profile_dirs = list(profile_dir) if isinstance(profile_dir, (list, tuple)) else [profile_dir]
    profile_path: Path | None = None
    for candidate_dir in profile_dirs:
        candidate_path = candidate_dir / f"{probe_id}.json"
        if candidate_path.exists():
            profile_path = candidate_path
            break
    if profile_path is None and _clean_token(row.get("source_validation_probe_id")):
        source_probe_id = _clean_token(row.get("source_validation_probe_id"))
        for candidate_dir in profile_dirs:
            candidate_path = candidate_dir / f"{source_probe_id}.json"
            if candidate_path.exists():
                profile_path = candidate_path
                break
    if profile_path is None:
        return None
    payload = _as_dict(_load_json(profile_path))
    profile = _as_dict(payload.get("profile")) or payload
    first_id = _clean_upper(row.get("first_indicator_id"))
    second_id = _clean_upper(row.get("second_indicator_id"))
    wanted = {value for value in (first_id, second_id) if value}
    indicator_defaults: list[dict[str, Any]] = []
    for item in _as_list(profile.get("indicators")):
        if not isinstance(item, dict):
            continue
        meta = _as_dict(item.get("meta"))
        indicator_id = _clean_upper(meta.get("id"))
        if not indicator_id or indicator_id not in wanted:
            continue
        config = _as_dict(item.get("config"))
        default_row = {
            "indicator_id": indicator_id,
            "instance_id": meta.get("instanceId"),
        }
        for key in TEMPLATE_CONFIG_KEYS:
            if key in config:
                default_row[key] = config.get(key)
        indicator_defaults.append(default_row)
    if not indicator_defaults:
        return None
    return {
        "probe_id": probe_id,
        "profile_path": str(profile_path),
        "timeframe": _clean_upper(row.get("probe_timeframe")),
        "lookback_months": _int_value(row.get("lookback_months")),
        "instruments": [
            _clean_upper(value)
            for value in _as_list(profile.get("instruments"))
            if _clean_upper(value)
        ],
        "indicator_defaults": indicator_defaults,
    }


def _profile_template_csv_bits(template: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(template, dict):
        return {
            "recommended_profile_template_path": None,
            "recommended_template_timeframe": None,
            "recommended_template_indicator_count": None,
        }
    indicators = [
        row
        for row in _as_list(template.get("indicator_defaults"))
        if isinstance(row, dict)
    ]
    return {
        "recommended_profile_template_path": template.get("profile_path"),
        "recommended_template_timeframe": template.get("timeframe"),
        "recommended_template_indicator_count": len(indicators),
    }


def build_discovered_recipe_prior_evidence(
    validation_rows: list[dict[str, Any]],
    *,
    rows_by_id: dict[str, dict[str, Any]],
    profile_dir: Path | list[Path] | tuple[Path, ...] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    latest_rows = _latest_validation_rows_by_pair(validation_rows)
    retained_rows = [
        row
        for row in latest_rows
        if _clean_token(row.get("retention_bucket")) in DISCOVERY_RECIPE_INCLUDED_BUCKETS
        and _float_value(row.get("primary_score"), _float_value(row.get("composite_score"))) > 0.0
    ]
    pair_priors: list[dict[str, Any]] = []
    slot_best: dict[tuple[str, str, str], dict[str, Any]] = {}
    recipe_meta: dict[str, dict[str, Any]] = {}

    def keep_slot(row: dict[str, Any]) -> None:
        key = (
            _clean_token(row.get("recipe")),
            _clean_token(row.get("slot")),
            _clean_upper(row.get("indicator_id")),
        )
        if not all(key):
            return
        current = slot_best.get(key)
        if current is None or _float_value(row.get("recipe_slot_score")) > _float_value(
            current.get("recipe_slot_score")
        ):
            slot_best[key] = row

    for row in retained_rows:
        recipe_id = _clean_token(row.get("recipe_id"))
        first_id = _clean_upper(row.get("first_indicator_id"))
        second_id = _clean_upper(row.get("second_indicator_id"))
        if not recipe_id or not first_id or not second_id:
            continue
        bucket = _clean_token(row.get("retention_bucket"))
        validation_score = _float_value(
            row.get("primary_score"),
            _float_value(row.get("composite_score")),
        )
        recipe_score = _discovery_recipe_score(row)
        lane = DISCOVERY_RECIPE_INCLUDED_BUCKETS.get(bucket, pair_sampling_lane(recipe_score))
        pair_weight = sampling_weight(recipe_score, lane)
        recommended_template = _load_recommended_profile_template(profile_dir, row)
        recipe_meta.setdefault(
            recipe_id,
            {
                "source": "discovery_recipe_validation",
                "recipe_confidence": row.get("recipe_confidence"),
                "first_cluster_id": row.get("first_cluster_id"),
                "second_cluster_id": row.get("second_cluster_id"),
            },
        )
        pair_priors.append(
            {
                "source": "discovery_recipe_validation",
                "recipe": recipe_id,
                "anchor_type": "discovered_recipe_validation",
                "anchor_id": first_id,
                "trigger_id": second_id,
                "probe_timeframe": _clean_upper(row.get("probe_timeframe")),
                "probe_id": row.get("probe_id"),
                "pair_prior_score": round(_float_value(row.get("validation_priority_score")), 4),
                "composite_score": round(validation_score, 4),
                "signal_count": row.get("signal_count"),
                "best_expectancy_r": row.get("best_expectancy_r"),
                "best_trades": row.get("best_trades"),
                "best_profit_factor": row.get("best_profit_factor"),
                "timing_policy": "catalog_default",
                "timing_adjustment": 0.0,
                "recommended_trigger_lookback_bars": None,
                "pair_sampling_score": recipe_score,
                "pair_sampling_lane": (
                    "positive_pair"
                    if lane in {"high_prior", "medium_prior"}
                    else "near_miss_pair"
                ),
                "pair_sampling_weight": pair_weight,
                "recipe_confidence": row.get("recipe_confidence"),
                "retention_bucket": bucket,
                "validation_score": round(validation_score, 4),
                "retention_ratio": row.get("retention_ratio"),
                "discovery_evidence_score": row.get("discovery_evidence_score"),
                "recommended_profile_template": recommended_template,
                **_profile_template_csv_bits(recommended_template),
            }
        )
        keep_slot(
            _discovery_slot_row(
                recipe_id=recipe_id,
                slot_name="context_or_setup_cluster",
                indicator_id=first_id,
                indicator_row=rows_by_id.get(first_id),
                result_row=row,
                recipe_slot_score=recipe_score,
            )
        )
        keep_slot(
            _discovery_slot_row(
                recipe_id=recipe_id,
                slot_name="trigger_or_response_cluster",
                indicator_id=second_id,
                indicator_row=rows_by_id.get(second_id),
                result_row=row,
                recipe_slot_score=recipe_score,
            )
        )

    slot_rows = sorted(
        slot_best.values(),
        key=lambda item: (
            _clean_token(item.get("recipe")),
            _clean_token(item.get("slot")),
            -_float_value(item.get("sampling_weight")),
            _clean_upper(item.get("indicator_id")),
        ),
    )
    recipes: dict[str, Any] = {}
    for recipe_id, meta in sorted(recipe_meta.items()):
        slots: dict[str, list[dict[str, Any]]] = {
            "context_or_setup_cluster": [],
            "trigger_or_response_cluster": [],
        }
        for row in slot_rows:
            if row.get("recipe") == recipe_id:
                slots.setdefault(str(row.get("slot")), []).append(row)
        for values in slots.values():
            values.sort(
                key=lambda item: (
                    _float_value(item.get("sampling_weight")),
                    _float_value(item.get("recipe_slot_score")),
                ),
                reverse=True,
            )
        recipes[recipe_id] = {
            **meta,
            "strategy_weights": {},
            "slots": slots,
        }

    summary = {
        "source_rows": len(validation_rows),
        "latest_pair_rows": len(latest_rows),
        "retained_rows": len(retained_rows),
        "retention_bucket_counts": _retention_bucket_counts(validation_rows),
        "included_retention_buckets": sorted(DISCOVERY_RECIPE_INCLUDED_BUCKETS),
        "recommended_template_rows": sum(
            1 for row in pair_priors if isinstance(row.get("recommended_profile_template"), dict)
        ),
        "discovered_recipe_count": len(recipes),
        "discovered_pair_prior_rows": len(pair_priors),
        "discovered_slot_rows": len(slot_rows),
    }
    return recipes, slot_rows, pair_priors, summary


def _unordered_pair_id(first_id: Any, second_id: Any) -> str:
    values = sorted(value for value in (_clean_upper(first_id), _clean_upper(second_id)) if value)
    return "+".join(values)


def _validation_pair_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _clean_token(row.get("recipe_id")),
        _clean_upper(row.get("first_indicator_id")),
        _clean_upper(row.get("second_indicator_id")),
        _clean_upper(row.get("probe_timeframe")),
    )


def _latest_validation_rows_by_pair(validation_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in validation_rows:
        status = _clean_token(row.get("status"))
        if status not in {"", "ok", "skipped_existing"}:
            continue
        key = _validation_pair_key(row)
        if not all(key):
            continue
        current = latest.get(key)
        if current is None:
            latest[key] = row
            continue
        row_rank = (
            _int_value(row.get("lookback_months")),
            _float_value(row.get("primary_score"), _float_value(row.get("composite_score"))),
        )
        current_rank = (
            _int_value(current.get("lookback_months")),
            _float_value(current.get("primary_score"), _float_value(current.get("composite_score"))),
        )
        if row_rank > current_rank:
            latest[key] = row
    return list(latest.values())


def build_discovery_negative_priors(
    validation_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    pair_rows: list[dict[str, Any]] = []
    retention_failures: list[dict[str, Any]] = []
    cluster_groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in validation_rows:
        status = _clean_token(row.get("status"))
        if status not in {"", "ok", "skipped_existing"}:
            continue
        bucket = _clean_token(row.get("retention_bucket"))
        validation_score = _float_value(
            row.get("primary_score"),
            _float_value(row.get("composite_score")),
        )
        discovery_score = _float_value(row.get("discovery_evidence_score"))
        first_id = _clean_upper(row.get("first_indicator_id"))
        second_id = _clean_upper(row.get("second_indicator_id"))
        timeframe = _clean_upper(row.get("probe_timeframe"))
        cluster_key = (
            _clean_token(row.get("recipe_id")),
            _clean_token(row.get("first_cluster_id")),
            _clean_token(row.get("second_cluster_id")),
            timeframe,
        )
        cluster = cluster_groups.setdefault(
            cluster_key,
            {
                "recipe": cluster_key[0],
                "first_cluster_id": cluster_key[1],
                "second_cluster_id": cluster_key[2],
                "probe_timeframe": cluster_key[3],
                "tested_count": 0,
                "retained_count": 0,
                "retained_strong_count": 0,
                "partial_count": 0,
                "failed_retention_count": 0,
                "new_failed_count": 0,
                "new_low_count": 0,
                "failed_count": 0,
                "positive_count": 0,
                "total_count": 0,
                "worst_validation_score": None,
                "best_validation_score": None,
                "median_validation_score": None,
                "example_probe_id": row.get("probe_id"),
                "_scores": [],
            },
        )
        cluster["tested_count"] = int(cluster.get("tested_count") or 0) + 1
        cluster["total_count"] = int(cluster.get("total_count") or 0) + 1
        if validation_score >= 50.0:
            cluster["positive_count"] = int(cluster.get("positive_count") or 0) + 1
        if bucket == "retained_strong":
            cluster["retained_strong_count"] = int(cluster.get("retained_strong_count") or 0) + 1
            cluster["retained_count"] = int(cluster.get("retained_count") or 0) + 1
        elif bucket == "retained":
            cluster["retained_count"] = int(cluster.get("retained_count") or 0) + 1
        elif bucket == "partial_retention":
            cluster["partial_count"] = int(cluster.get("partial_count") or 0) + 1
        elif bucket == "failed_retention":
            cluster["failed_retention_count"] = int(cluster.get("failed_retention_count") or 0) + 1
        elif bucket == "new_failed_cluster_expansion":
            cluster["new_failed_count"] = int(cluster.get("new_failed_count") or 0) + 1
        elif bucket == "new_low_cluster_expansion":
            cluster["new_low_count"] = int(cluster.get("new_low_count") or 0) + 1
        worst = cluster.get("worst_validation_score")
        if worst is None or validation_score < _float_value(worst):
            cluster["worst_validation_score"] = round(validation_score, 4)
        best = cluster.get("best_validation_score")
        if best is None or validation_score > _float_value(best):
            cluster["best_validation_score"] = round(validation_score, 4)
        scores = cluster.get("_scores")
        if isinstance(scores, list):
            scores.append(validation_score)

        is_failure = bucket in DISCOVERY_RECIPE_NEGATIVE_BUCKETS or (
            discovery_score >= 50.0 and validation_score <= 0.0
        )
        if not is_failure:
            continue
        cluster["failed_count"] = int(cluster.get("failed_count") or 0) + 1
        reason = (
            "cluster_expansion_failed"
            if bucket.startswith("new_")
            else "retention_failed"
        )
        if discovery_score >= 50.0 and validation_score <= 0.0:
            reason = "positive_discovery_collapsed"
        negative_weight = 1.0
        if discovery_score >= 65.0 and validation_score <= 0.0:
            negative_weight = 1.5
        elif bucket.startswith("new_"):
            negative_weight = 0.75
        negative_scope = (
            "hard_unordered"
            if negative_weight >= 1.5 and reason == "positive_discovery_collapsed"
            else "soft_exact_timeframe"
        )
        pair_row = {
            "source": "discovery_recipe_validation",
            "recipe": row.get("recipe_id"),
            "first_indicator_id": first_id,
            "second_indicator_id": second_id,
            "ordered_pair_id": f"{first_id}->{second_id}",
            "unordered_pair_id": _unordered_pair_id(first_id, second_id),
            "probe_timeframe": timeframe,
            "probe_id": row.get("probe_id"),
            "lookback_months": row.get("lookback_months"),
            "retention_bucket": bucket,
            "validation_score": round(validation_score, 4),
            "retention_ratio": row.get("retention_ratio"),
            "discovery_evidence_score": row.get("discovery_evidence_score"),
            "discovery_evidence_probe_id": row.get("discovery_evidence_probe_id"),
            "signal_count": row.get("signal_count"),
            "best_expectancy_r": row.get("best_expectancy_r"),
            "best_trades": row.get("best_trades"),
            "best_profit_factor": row.get("best_profit_factor"),
            "negative_reason": reason,
            "negative_scope": negative_scope,
            "negative_weight": negative_weight,
        }
        pair_rows.append(pair_row)
        if discovery_score >= 50.0:
            retention_failures.append(pair_row)
    cluster_rows: list[dict[str, Any]] = []
    for row in cluster_groups.values():
        total = max(1, int(row.get("tested_count") or row.get("total_count") or 0))
        failed = int(row.get("failed_count") or 0)
        if failed <= 0:
            continue
        scores = sorted(score for score in row.pop("_scores", []) if math.isfinite(score))
        if scores:
            midpoint = len(scores) // 2
            if len(scores) % 2:
                row["median_validation_score"] = round(scores[midpoint], 4)
            else:
                row["median_validation_score"] = round((scores[midpoint - 1] + scores[midpoint]) / 2.0, 4)
        retained = int(row.get("retained_count") or 0)
        row["failure_rate"] = round(failed / float(total), 4)
        row["retained_rate"] = round(retained / float(total), 4)
        if total >= 6 and row["failure_rate"] >= 0.85:
            row["soft_penalty_multiplier"] = 0.5
        elif total >= 4 and row["failure_rate"] >= 0.75 and retained == 0:
            row["soft_penalty_multiplier"] = 0.7
        else:
            row["soft_penalty_multiplier"] = 1.0
        row["negative_weight"] = round(min(2.0, 0.5 + row["failure_rate"]), 4)
        cluster_rows.append(row)
    pair_rows.sort(
        key=lambda row: (
            -_float_value(row.get("negative_weight")),
            -_float_value(row.get("discovery_evidence_score")),
            str(row.get("ordered_pair_id") or ""),
        )
    )
    cluster_rows.sort(
        key=lambda row: (
            -_float_value(row.get("negative_weight")),
            -_int_value(row.get("failed_count")),
            str(row.get("recipe") or ""),
        )
    )
    retention_failures.sort(
        key=lambda row: (
            -_float_value(row.get("discovery_evidence_score")),
            str(row.get("ordered_pair_id") or ""),
        )
    )
    summary = {
        "negative_pair_rows": len(pair_rows),
        "cluster_negative_rows": len(cluster_rows),
        "retention_failure_rows": len(retention_failures),
        "negative_retention_buckets": sorted(DISCOVERY_RECIPE_NEGATIVE_BUCKETS),
    }
    return pair_rows, cluster_rows, retention_failures, summary


def score_slot_candidate(
    indicator_row: dict[str, Any],
    *,
    recipe_name: str,
    slot_name: str,
    static_slot_scores: dict[tuple[str, str, str], float],
    signal_rollups: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    trigger_pair_stats: dict[tuple[str, str], dict[str, Any]],
    anchor_pair_stats: dict[tuple[str, str], dict[str, Any]],
    timing_evidence: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    indicator_id = _clean_upper(indicator_row.get("id"))
    signal_role = str(indicator_row.get("signal_role") or "").lower()
    slot_roles = _slot_roles(recipe_name, slot_name)
    static_score = static_slot_scores.get(
        (recipe_name, slot_name, indicator_id),
        _fallback_static_slot_score(
            indicator_row,
            recipe_name=recipe_name,
            slot_name=slot_name,
        ),
    )
    sig_score, density_bucket = _signal_score(signal_rollups.get(indicator_id))
    fwd_score, forward_bucket = _forward_score(forward_priors.get(indicator_id))
    if slot_name == "trigger":
        empirical = trigger_pair_stats.get((recipe_name, indicator_id), _empty_stats())
    else:
        empirical = anchor_pair_stats.get((recipe_name, indicator_id), _empty_stats())
    empirical_count = _int_value(empirical.get("count"))
    best_pair_score = empirical.get("best_score")
    pair_component = _float_value(best_pair_score, 50.0)
    if empirical_count:
        score = (static_score * 0.42) + (sig_score * 0.12) + (fwd_score * 0.14) + (pair_component * 0.32)
    else:
        score = (static_score * 0.66) + (sig_score * 0.16) + (fwd_score * 0.18)

    slot_role_fit = "matched" if signal_role in slot_roles else "mismatch"
    if slot_role_fit == "mismatch":
        score -= 5.0 if empirical_count else 18.0

    timing = timing_evidence.get((recipe_name, indicator_id)) if slot_name == "trigger" else None
    timing_policy, timing_adjustment, recommended_lookback = timing_policy_for(timing)
    score += timing_adjustment

    default_problem = density_bucket in {"flat", "no_data"} or forward_bucket == "default_problem"
    if default_problem and slot_name == "trigger":
        score -= 40.0
    score = round(_clamp(score), 4)
    lane = sampling_lane(
        score,
        empirical_count=empirical_count,
        positive_pair_count=_int_value(empirical.get("positive_count")),
        default_problem=default_problem and slot_name == "trigger",
        has_behavior_evidence=bool(signal_rollups.get(indicator_id) or forward_priors.get(indicator_id)),
    )
    return {
        "source": "curated_recipe_prior",
        "recipe": recipe_name,
        "slot": slot_name,
        "indicator_id": indicator_id,
        "signal_role": indicator_row.get("signal_role"),
        "strategy_role": indicator_row.get("strategy_role"),
        "namespace": indicator_row.get("namespace"),
        "slot_role_fit": slot_role_fit,
        "static_slot_score": round(static_score, 4),
        "signal_density_score": sig_score,
        "signal_density_bucket": density_bucket,
        "forward_response_score": fwd_score,
        "forward_response_bucket": forward_bucket,
        "empirical_pair_count": empirical_count,
        "positive_pair_count": empirical.get("positive_count"),
        "best_pair_score": best_pair_score,
        "avg_pair_score": empirical.get("avg_score"),
        "best_pair_probe_id": empirical.get("best_probe_id"),
        "best_pair_timeframe": empirical.get("best_timeframe"),
        "timing_policy": timing_policy,
        "timing_adjustment": round(timing_adjustment, 4),
        "recommended_trigger_lookback_bars": recommended_lookback,
        "recipe_slot_score": score,
        "sampling_lane": lane,
        "sampling_weight": sampling_weight(score, lane),
    }


def build_recipe_prior_artifacts(
    *,
    indicator_rows: list[dict[str, Any]],
    static_slot_scores: dict[tuple[str, str, str], float],
    signal_rollups: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    pair_results: list[dict[str, Any]],
    timing_results: list[dict[str, Any]],
    discovery_validation_results: list[dict[str, Any]] | None = None,
    discovery_validation_profile_dir: Path | list[Path] | tuple[Path, ...] | None = None,
    max_slot_candidates: int,
    max_pair_candidates: int,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    rows_by_id = _row_by_id(indicator_rows)
    generation_rows = [
        row
        for row in indicator_rows
        if _bool_value(row.get("generation_eligible"))
    ]
    trigger_pair_stats, anchor_pair_stats, pair_priors = build_pair_evidence(pair_results)
    timing = build_timing_evidence(timing_results)
    (
        discovered_recipes,
        discovered_slot_rows,
        discovered_pair_priors,
        discovered_summary,
    ) = build_discovered_recipe_prior_evidence(
        discovery_validation_results or [],
        rows_by_id=rows_by_id,
        profile_dir=discovery_validation_profile_dir,
    )
    (
        negative_pair_rows,
        negative_cluster_rows,
        retention_failure_rows,
        negative_summary,
    ) = build_discovery_negative_priors(discovery_validation_results or [])

    slot_rows: list[dict[str, Any]] = []
    recipes: dict[str, Any] = {}
    for recipe_name, definition in RECIPE_DEFINITIONS.items():
        recipe_slots: dict[str, Any] = {}
        for slot_name in _as_dict(definition.get("slots")):
            candidates = [
                score_slot_candidate(
                    row,
                    recipe_name=recipe_name,
                    slot_name=slot_name,
                    static_slot_scores=static_slot_scores,
                    signal_rollups=signal_rollups,
                    forward_priors=forward_priors,
                    trigger_pair_stats=trigger_pair_stats,
                    anchor_pair_stats=anchor_pair_stats,
                    timing_evidence=timing,
                )
                for row in generation_rows
            ]
            candidates.sort(
                key=lambda row: (
                    _float_value(row.get("sampling_weight")),
                    _float_value(row.get("recipe_slot_score")),
                    str(row.get("indicator_id") or ""),
                ),
                reverse=True,
            )
            selected = candidates[:max_slot_candidates]
            slot_rows.extend(selected)
            recipe_slots[slot_name] = selected
        recipes[recipe_name] = {
            "source": "curated_recipe_prior",
            "strategy_weights": definition.get("strategy_weights"),
            "slots": recipe_slots,
        }

    recipes.update(discovered_recipes)
    slot_rows.extend(discovered_slot_rows)
    pair_priors.extend(discovered_pair_priors)

    for pair in pair_priors:
        if _clean_token(pair.get("source")) == "discovery_recipe_validation":
            continue
        recipe_name = _clean_token(pair.get("recipe"))
        trigger_id = _clean_upper(pair.get("trigger_id"))
        trigger_timing = timing.get((recipe_name, trigger_id))
        policy, adjustment, recommended_lookback = timing_policy_for(trigger_timing)
        score = _float_value(pair.get("composite_score"))
        prior = _float_value(pair.get("pair_prior_score"))
        pair["timing_policy"] = policy
        pair["timing_adjustment"] = round(adjustment, 4)
        pair["recommended_trigger_lookback_bars"] = recommended_lookback
        pair["pair_sampling_score"] = round(_clamp((score * 0.72) + (prior * 0.18) + adjustment), 4)
        pair["pair_sampling_lane"] = (
            "positive_pair"
            if score >= 50.0
            else "near_miss_pair"
            if score >= 40.0 or adjustment > 4.0
            else "low_pair"
        )
        pair["pair_sampling_weight"] = sampling_weight(
            _float_value(pair.get("pair_sampling_score")),
            pair_sampling_lane(_float_value(pair.get("pair_sampling_score"))),
        )
    pair_priors.sort(
        key=lambda row: (
            _float_value(row.get("pair_sampling_weight")),
            _float_value(row.get("pair_sampling_score")),
        ),
        reverse=True,
    )
    pair_priors = pair_priors[:max_pair_candidates]
    any_36m_retained = any(
        _int_value(row.get("lookback_months")) >= 36
        and _clean_token(row.get("retention_bucket")) in {"retained", "retained_strong"}
        for row in discovery_validation_results or []
    )
    sampling_policy = (
        {
            "guided_prior_fraction": 0.80,
            "uncertain_prior_fraction": 0.15,
            "wild_exploration_fraction": 0.05,
            "maturity": "has_36m_retention",
            "interpretation": "Weights bias random selection; they do not hard-filter the catalog.",
        }
        if any_36m_retained
        else {
            "guided_prior_fraction": 0.60,
            "uncertain_prior_fraction": 0.25,
            "wild_exploration_fraction": 0.15,
            "maturity": "pre_36m_retention",
            "interpretation": "Weights bias random selection; they do not hard-filter the catalog.",
        }
    )

    seed_plan = {
        "schema_version": "play_hand_seed_plan_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sampling_policy": sampling_policy,
        "negative_pairs": negative_pair_rows[:300],
        "recipes": {
            recipe_name: {
                "source": recipe_payload.get("source", "curated_recipe_prior"),
                "recipe_confidence": recipe_payload.get("recipe_confidence"),
                "first_cluster_id": recipe_payload.get("first_cluster_id"),
                "second_cluster_id": recipe_payload.get("second_cluster_id"),
                "slot_menus": {
                    slot_name: [
                        {
                            "source": row.get("source", "curated_recipe_prior"),
                            "indicator_id": row["indicator_id"],
                            "sampling_weight": row["sampling_weight"],
                            "sampling_lane": row["sampling_lane"],
                            "recipe_slot_score": row["recipe_slot_score"],
                            "timing_policy": row["timing_policy"],
                            "recommended_trigger_lookback_bars": row[
                                "recommended_trigger_lookback_bars"
                            ],
                            "best_pair_score": row["best_pair_score"],
                            "retention_bucket": row.get("retention_bucket"),
                            "validation_probe_id": row.get("validation_probe_id"),
                            "validation_score": row.get("validation_score"),
                        }
                        for row in recipe_payload["slots"][slot_name]
                    ]
                    for slot_name in recipe_payload["slots"]
                },
                "pair_menu": [
                    row
                    for row in pair_priors
                    if row.get("recipe") == recipe_name
                ][:25],
                "recommended_templates": [
                    {"name": "core_3", "slots": ["context", "setup", "trigger"]},
                    {"name": "guarded_4", "slots": ["context", "setup", "trigger", "guard"]},
                    {"name": "double_trigger_4", "slots": ["context", "setup", "trigger", "trigger"]},
                ],
            }
            for recipe_name, recipe_payload in recipes.items()
        },
    }
    lane_counts: dict[str, int] = {}
    for row in slot_rows:
        lane = _clean_token(row.get("sampling_lane")) or "unknown"
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "result_counts": {
            "indicator_rows": len(indicator_rows),
            "generation_eligible": len(generation_rows),
            "slot_indicator_rows": len(slot_rows),
            "pair_prior_rows": len(pair_priors),
            "slot_sampling_lane_counts": dict(sorted(lane_counts.items())),
            "discovered_validation_rows": discovered_summary["source_rows"],
            "discovered_validation_retained_rows": discovered_summary["retained_rows"],
            "discovered_recipe_count": discovered_summary["discovered_recipe_count"],
            "discovered_recipe_pair_rows": discovered_summary["discovered_pair_prior_rows"],
            "discovered_recipe_template_rows": discovered_summary["recommended_template_rows"],
            "negative_pair_rows": negative_summary["negative_pair_rows"],
            "cluster_negative_rows": negative_summary["cluster_negative_rows"],
            "retention_failure_rows": negative_summary["retention_failure_rows"],
        },
        "discovered_recipe_validation": discovered_summary,
        "negative_priors": negative_summary,
        "top_slots": {
            recipe_name: {
                slot_name: recipe_payload["slots"][slot_name][:8]
                for slot_name in recipe_payload["slots"]
            }
            for recipe_name, recipe_payload in recipes.items()
        },
        "top_pairs": pair_priors[:20],
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "note": (
            "Empirical recipe priors blend static catalog roles, signal density, forward response, "
            "Layer 3 pair outcomes, and Layer 3b timing evidence. Weights bias sampling; they are not hard filters."
        ),
        "sampling_policy": seed_plan["sampling_policy"],
        "recipes": recipes,
        "pair_priors": pair_priors,
        "negative_pair_priors": negative_pair_rows,
        "cluster_expansion_negative_priors": negative_cluster_rows,
        "retention_failures": retention_failure_rows,
        "summary": summary,
    }
    return (
        payload,
        slot_rows,
        pair_priors,
        negative_pair_rows,
        negative_cluster_rows,
        retention_failure_rows,
        seed_plan,
        summary,
    )


def _slot_fieldnames() -> list[str]:
    return [
        "source",
        "recipe",
        "slot",
        "indicator_id",
        "signal_role",
        "strategy_role",
        "namespace",
        "slot_role_fit",
        "static_slot_score",
        "signal_density_score",
        "signal_density_bucket",
        "forward_response_score",
        "forward_response_bucket",
        "empirical_pair_count",
        "positive_pair_count",
        "best_pair_score",
        "avg_pair_score",
        "best_pair_probe_id",
        "best_pair_timeframe",
        "timing_policy",
        "timing_adjustment",
        "recommended_trigger_lookback_bars",
        "recipe_slot_score",
        "sampling_lane",
        "sampling_weight",
        "retention_bucket",
        "validation_probe_id",
        "validation_score",
        "retention_ratio",
    ]


def _pair_fieldnames() -> list[str]:
    return [
        "source",
        "recipe",
        "anchor_type",
        "anchor_id",
        "trigger_id",
        "probe_timeframe",
        "probe_id",
        "pair_prior_score",
        "composite_score",
        "signal_count",
        "best_expectancy_r",
        "best_trades",
        "best_profit_factor",
        "timing_policy",
        "timing_adjustment",
        "recommended_trigger_lookback_bars",
        "pair_sampling_score",
        "pair_sampling_lane",
        "pair_sampling_weight",
        "recipe_confidence",
        "retention_bucket",
        "validation_score",
        "retention_ratio",
        "discovery_evidence_score",
        "recommended_profile_template_path",
        "recommended_template_timeframe",
        "recommended_template_indicator_count",
    ]


def _pair_negative_fieldnames() -> list[str]:
    return [
        "source",
        "recipe",
        "first_indicator_id",
        "second_indicator_id",
        "ordered_pair_id",
        "unordered_pair_id",
        "probe_timeframe",
        "probe_id",
        "lookback_months",
        "retention_bucket",
        "validation_score",
        "retention_ratio",
        "discovery_evidence_score",
        "discovery_evidence_probe_id",
        "signal_count",
        "best_expectancy_r",
        "best_trades",
        "best_profit_factor",
        "negative_reason",
        "negative_scope",
        "negative_weight",
    ]


def _cluster_negative_fieldnames() -> list[str]:
    return [
        "recipe",
        "first_cluster_id",
        "second_cluster_id",
        "probe_timeframe",
        "tested_count",
        "retained_count",
        "retained_strong_count",
        "partial_count",
        "failed_retention_count",
        "new_failed_count",
        "new_low_count",
        "failed_count",
        "positive_count",
        "total_count",
        "failure_rate",
        "retained_rate",
        "worst_validation_score",
        "best_validation_score",
        "median_validation_score",
        "example_probe_id",
        "soft_penalty_multiplier",
        "negative_weight",
    ]


def build_recipe_priors(
    config: AppConfig,
    *,
    indicator_atlas_dir: Path | None = None,
    signal_atlas_dir: Path | None = None,
    forward_response_dir: Path | None = None,
    anchor_pair_dir: Path | None = None,
    anchor_pair_timing_dir: Path | None = None,
    discovery_recipe_validation_dir: Path | None = None,
    out_dir: Path | None = None,
    max_slot_candidates: int = 40,
    max_pair_candidates: int = 80,
) -> RecipePriorsBuildResult:
    indicator_dir = (
        indicator_atlas_dir.expanduser().resolve()
        if indicator_atlas_dir is not None
        else config.derived_root / DEFAULT_ATLAS_DIRNAME
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
    pair_dir = (
        anchor_pair_dir.expanduser().resolve()
        if anchor_pair_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_DIRNAME
    )
    timing_dir = (
        anchor_pair_timing_dir.expanduser().resolve()
        if anchor_pair_timing_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME
    )
    discovery_validation_dir = (
        discovery_recipe_validation_dir.expanduser().resolve()
        if discovery_recipe_validation_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME
    )
    discovery_scrutiny_dir = config.derived_root / DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_RECIPE_PRIORS_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    indicator_rows = _indicator_rows(indicator_dir)
    pair_results_path = pair_dir / "anchor-pair-probe-results.csv"
    if not pair_results_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair results at {pair_results_path}. Run `uv run run-anchor-pair-probes --all` first."
        )
    timing_results_path = timing_dir / "anchor-pair-timing-results.csv"
    if not timing_results_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair timing results at {timing_results_path}. Run `uv run run-anchor-pair-timing-probes` first."
        )
    discovery_validation_results_path = (
        discovery_validation_dir / "discovery-recipe-validation-results.csv"
    )
    discovery_scrutiny_results_path = (
        discovery_scrutiny_dir / "discovery-recipe-validation-results.csv"
    )
    discovery_validation_results = (
        _read_csv_rows(discovery_validation_results_path)
        if discovery_validation_results_path.exists()
        else []
    )
    if discovery_scrutiny_results_path.exists():
        discovery_validation_results.extend(_read_csv_rows(discovery_scrutiny_results_path))

    (
        payload,
        slot_rows,
        pair_rows,
        pair_negative_rows,
        cluster_negative_rows,
        retention_failure_rows,
        seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicator_rows,
        static_slot_scores=_static_slot_scores(indicator_dir),
        signal_rollups=_signal_rollups(signal_dir),
        forward_priors=_forward_priors(forward_dir),
        pair_results=_read_csv_rows(pair_results_path),
        timing_results=_read_csv_rows(timing_results_path),
        discovery_validation_results=discovery_validation_results,
        discovery_validation_profile_dir=[
            discovery_validation_dir / "profiles",
            discovery_scrutiny_dir / "profiles",
        ],
        max_slot_candidates=max(1, int(max_slot_candidates)),
        max_pair_candidates=max(1, int(max_pair_candidates)),
    )
    summary["source"] = {
        "indicator_atlas_dir": str(indicator_dir),
        "signal_atlas_dir": str(signal_dir),
        "forward_response_dir": str(forward_dir),
        "anchor_pair_results_path": str(pair_results_path),
        "anchor_pair_timing_results_path": str(timing_results_path),
        "discovery_recipe_validation_results_path": (
            str(discovery_validation_results_path)
            if discovery_validation_results_path.exists()
            else None
        ),
        "discovery_recipe_scrutiny_results_path": (
            str(discovery_scrutiny_results_path)
            if discovery_scrutiny_results_path.exists()
            else None
        ),
    }
    payload["summary"] = summary

    priors_path = target_dir / "recipe-priors.json"
    slot_csv_path = target_dir / "slot-indicator-priors.csv"
    pair_csv_path = target_dir / "pair-priors.csv"
    pair_negative_csv_path = target_dir / "pair-negative-priors.csv"
    cluster_negative_csv_path = target_dir / "cluster-expansion-negative-priors.csv"
    retention_failures_csv_path = target_dir / "retention-failures.csv"
    seed_plan_path = target_dir / "play-hand-seed-plan.json"
    summary_path = target_dir / "recipe-priors-summary.json"

    _write_json(priors_path, payload)
    _write_csv(slot_csv_path, slot_rows, _slot_fieldnames())
    _write_csv(pair_csv_path, pair_rows, _pair_fieldnames())
    _write_csv(pair_negative_csv_path, pair_negative_rows, _pair_negative_fieldnames())
    _write_csv(cluster_negative_csv_path, cluster_negative_rows, _cluster_negative_fieldnames())
    _write_csv(retention_failures_csv_path, retention_failure_rows, _pair_negative_fieldnames())
    _write_json(seed_plan_path, seed_plan)
    _write_json(summary_path, summary)
    return RecipePriorsBuildResult(
        priors_path=priors_path,
        slot_csv_path=slot_csv_path,
        pair_csv_path=pair_csv_path,
        pair_negative_csv_path=pair_negative_csv_path,
        cluster_negative_csv_path=cluster_negative_csv_path,
        retention_failures_csv_path=retention_failures_csv_path,
        seed_plan_path=seed_plan_path,
        summary_path=summary_path,
        summary=summary,
    )
