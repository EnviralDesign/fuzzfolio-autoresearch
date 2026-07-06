from __future__ import annotations

import copy
import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .indicator_atlas import (
    DEFAULT_ANCHORS,
    DEFAULT_ATLAS_DIRNAME,
    build_indicator_atlas,
    load_indicator_catalog,
)
from .fuzzfolio import CliError, FuzzfolioCli
from .scoring import build_attempt_score, load_sensitivity_snapshot
from .signal_atlas import DEFAULT_INSTRUMENTS, DEFAULT_SIGNAL_ATLAS_DIRNAME, DEFAULT_TIMEFRAMES
from .forward_response_atlas import DEFAULT_FORWARD_RESPONSE_DIRNAME


SCHEMA_VERSION = "anchor_pair_atlas_v1"
TIMING_SCHEMA_VERSION = "anchor_pair_timing_atlas_v1"
DEFAULT_ANCHOR_PAIR_DIRNAME = "anchor-pair-atlas"
DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME = "anchor-pair-timing-atlas"
DEFAULT_MAX_QUEUE_PAIRS = 48
DEFAULT_LOOKBACK_MONTHS = 3
DEFAULT_QUALITY_SCORE_PRESET = "profile-drop"
DEFAULT_EXECUTION_COST_MODE = "research-conservative"
DEFAULT_PROBE_RUN_LIMIT = 8
DEFAULT_PROBE_TIMEOUT_SECONDS = 2400
DEFAULT_TIMING_LOOKBACK_BARS = (1, 2, 3)

ANCHOR_TYPE_RECIPES: dict[str, str] = {
    "trend": "trend_pullback_continuation",
    "mean_reversion": "mean_reversion_reclaim",
    "compression": "breakout_compression_release",
    "profile_value": "profile_value_context",
}

DENSITY_BUCKET_SCORES: dict[str, float] = {
    "usable": 78.0,
    "sparse": 62.0,
    "dense": 52.0,
    "very_sparse": 32.0,
    "saturated": 18.0,
    "flat": 0.0,
    "no_data": 0.0,
}


def default_probe_as_of_date(now: datetime | None = None) -> str:
    """Return a lake-safe default as-of date at the end of the previous UTC month."""
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    first_of_month = value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    previous_month_end = first_of_month - timedelta(seconds=1)
    return previous_month_end.strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_probe_as_of_date(as_of_date: str | None = None) -> str:
    value = str(as_of_date or "").strip()
    return value or default_probe_as_of_date()


@dataclass(frozen=True)
class AnchorPairAtlasBuildResult:
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
            "anchor_pair_atlas_json": str(self.atlas_path),
            "anchor_pair_matrix_csv": str(self.matrix_csv_path),
            "anchor_pair_queue_csv": str(self.queue_csv_path),
            "anchor_pair_run_manifest_json": str(self.manifest_path),
            "anchor_pair_run_script": str(self.run_script_path),
            "anchor_pair_profile_dir": str(self.profile_dir),
            "anchor_pair_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


@dataclass(frozen=True)
class AnchorPairProbeRunResult:
    results_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "anchor_pair_probe_results_csv": str(self.results_csv_path),
            "anchor_pair_probe_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


@dataclass(frozen=True)
class AnchorPairTimingAtlasBuildResult:
    atlas_path: Path
    queue_csv_path: Path
    manifest_path: Path
    run_script_path: Path
    profile_dir: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "anchor_pair_timing_atlas_json": str(self.atlas_path),
            "anchor_pair_timing_queue_csv": str(self.queue_csv_path),
            "anchor_pair_timing_run_manifest_json": str(self.manifest_path),
            "anchor_pair_timing_run_script": str(self.run_script_path),
            "anchor_pair_timing_profile_dir": str(self.profile_dir),
            "anchor_pair_timing_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


@dataclass(frozen=True)
class AnchorPairTimingProbeRunResult:
    results_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "anchor_pair_timing_results_csv": str(self.results_csv_path),
            "anchor_pair_timing_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_tokens(values: list[str] | tuple[str, ...] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = _clean_upper(value)
        if token and token not in seen:
            cleaned.append(token)
            seen.add(token)
    return cleaned


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


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _dominant_bucket(counts: Any, default: str = "unknown") -> str:
    if not isinstance(counts, dict) or not counts:
        return default
    return max(
        counts.items(),
        key=lambda item: (_int_value(item[1]), str(item[0])),
    )[0]


def _catalog_by_id(catalog_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for item in _as_list(catalog_payload.get("indicators")):
        if not isinstance(item, dict):
            continue
        indicator_id = _clean_upper(_as_dict(item.get("meta")).get("id"))
        if indicator_id:
            by_id[indicator_id] = item
    return by_id


def _catalog_lookback_bars(
    catalog_by_id: dict[str, dict[str, Any]],
    indicator_id: str,
    *,
    default: int = 1,
) -> int:
    item = _as_dict(catalog_by_id.get(_clean_upper(indicator_id)))
    config = _as_dict(item.get("config"))
    return max(1, _int_value(config.get("lookbackBars"), default))


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        indicator_id = _clean_upper(row.get("id"))
        if indicator_id:
            by_id[indicator_id] = row
    return by_id


def _anchor_defaults(anchor_ids: list[str] | None = None) -> list[tuple[str, str]]:
    requested = set(_normalize_tokens(anchor_ids))
    anchors: list[tuple[str, str]] = []
    for anchor_type, ids in DEFAULT_ANCHORS.items():
        for anchor_id in ids:
            normalized = _clean_upper(anchor_id)
            if requested and normalized not in requested:
                continue
            anchors.append((anchor_type, normalized))
    return anchors


def _signal_rollups_by_id(signal_atlas_dir: Path) -> dict[str, dict[str, Any]]:
    summary_path = signal_atlas_dir / "signal-atlas-summary.json"
    atlas_path = signal_atlas_dir / "signal-atlas.json"
    payload: dict[str, Any]
    if summary_path.exists():
        payload = _as_dict(_load_json(summary_path))
    elif atlas_path.exists():
        payload = _as_dict(_as_dict(_load_json(atlas_path)).get("summary"))
    else:
        raise FileNotFoundError(
            f"Missing signal atlas at {signal_atlas_dir}. Run `uv run build-signal-atlas` first."
        )
    rollups = _as_dict(payload.get("indicator_rollups"))
    return {
        _clean_upper(indicator_id): _as_dict(row)
        for indicator_id, row in rollups.items()
        if _clean_upper(indicator_id)
    }


def _forward_priors_by_id(forward_response_dir: Path) -> dict[str, dict[str, Any]]:
    csv_path = forward_response_dir / "forward-response-priors.csv"
    summary_path = forward_response_dir / "forward-response-summary.json"
    if csv_path.exists():
        return {
            _clean_upper(row.get("indicator_id")): row
            for row in _read_csv_rows(csv_path)
            if _clean_upper(row.get("indicator_id"))
        }
    if summary_path.exists():
        payload = _as_dict(_load_json(summary_path))
        return {
            _clean_upper(row.get("indicator_id")): row
            for row in _as_list(payload.get("priors"))
            if isinstance(row, dict) and _clean_upper(row.get("indicator_id"))
        }
    raise FileNotFoundError(
        f"Missing forward-response atlas at {forward_response_dir}. "
        "Run `uv run build-forward-response-atlas` first."
    )


def _static_pair_lookup(indicator_atlas_dir: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _read_csv_rows(indicator_atlas_dir / "indicator-pair-matrix.csv"):
        key = (
            _clean_token(row.get("anchor_type")).lower(),
            _clean_upper(row.get("anchor_id")),
            _clean_upper(row.get("trigger_id")),
        )
        if all(key):
            lookup[key] = row
    return lookup


def signal_density_score(rollup: dict[str, Any] | None) -> tuple[float, str, str]:
    if not rollup or rollup.get("status") != "ok":
        return 0.0, "no_data", "no_data"
    density_bucket = _clean_token(rollup.get("density_bucket")) or "no_data"
    balance_bucket = _dominant_bucket(rollup.get("balance_bucket_counts"), "unknown")
    score = DENSITY_BUCKET_SCORES.get(density_bucket, 40.0)
    if balance_bucket == "balanced":
        score += 8.0
    elif balance_bucket in {"long_biased", "short_biased"}:
        score -= 4.0
    elif balance_bucket in {"one_sided", "flat"}:
        score -= 12.0
    event_count = _int_value(rollup.get("event_count"))
    if event_count >= 500:
        score += 4.0
    elif event_count < 50:
        score -= 8.0
    return round(_clamp(score), 2), density_bucket, balance_bucket


def _recipe_fit_score(anchor_type: str, trigger_row: dict[str, Any]) -> float:
    strategy_role = _clean_token(trigger_row.get("strategy_role")).lower()
    signal_role = _clean_token(trigger_row.get("signal_role")).lower()
    score = 50.0
    if signal_role == "trigger":
        score += 8.0
    if anchor_type == "trend":
        score += 18.0 if strategy_role in {"trend", "breakout", "confirm"} else -4.0
    elif anchor_type == "mean_reversion":
        score += 18.0 if strategy_role == "mean-reversion" else 4.0 if strategy_role == "confirm" else -4.0
    elif anchor_type == "compression":
        score += 20.0 if strategy_role in {"breakout", "confirm"} else 2.0
    elif anchor_type == "profile_value":
        score += 12.0 if strategy_role in {"mean-reversion", "confirm"} else 0.0
    return round(_clamp(score), 2)


def _parse_best_cell_context(context: str) -> dict[str, str]:
    parts = [part for part in _clean_token(context).split() if part]
    parsed = {
        "instrument": parts[0].upper() if len(parts) >= 1 else "",
        "timeframe": parts[1].upper() if len(parts) >= 2 else "",
        "direction": parts[2].lower() if len(parts) >= 3 else "",
        "horizon_bars": parts[3] if len(parts) >= 4 else "",
    }
    return parsed


def score_anchor_pair(
    *,
    anchor_type: str,
    anchor_row: dict[str, Any],
    trigger_row: dict[str, Any],
    static_pair_row: dict[str, Any] | None,
    signal_rollup: dict[str, Any] | None,
    forward_prior: dict[str, Any] | None,
    probe_timeframe: str,
    instruments: list[str],
) -> dict[str, Any]:
    density_score, density_bucket, balance_bucket = signal_density_score(signal_rollup)
    compatibility_score = _float_value(
        _as_dict(static_pair_row).get("compatibility_prior_score"),
        50.0,
    )
    forward_score = _float_value(
        _as_dict(forward_prior).get("forward_response_prior_score"),
        45.0,
    )
    forward_bucket = _clean_token(
        _as_dict(forward_prior).get("forward_response_prior_bucket")
    ) or "missing_forward_prior"
    recipe_score = _recipe_fit_score(anchor_type, trigger_row)
    best_context = _clean_token(_as_dict(forward_prior).get("best_cell_context"))
    best_context_parts = _parse_best_cell_context(best_context)
    context_bonus = 0.0
    if best_context_parts["timeframe"] == _clean_upper(probe_timeframe):
        context_bonus += 4.0
    if best_context_parts["instrument"] in set(instruments):
        context_bonus += 2.0

    pair_score = (
        compatibility_score * 0.32
        + density_score * 0.24
        + forward_score * 0.28
        + recipe_score * 0.16
        + context_bonus
    )
    shared_base = bool(
        anchor_row.get("base_indicator_id")
        and anchor_row.get("base_indicator_id") == trigger_row.get("base_indicator_id")
    )
    if shared_base:
        pair_score -= 6.0

    issue = ""
    if density_bucket == "flat" or forward_bucket == "default_problem":
        bucket = "blocked_default_problem"
        issue = "flat_or_no_forward_events"
    elif density_bucket in {"saturated", "very_sparse"}:
        bucket = "probe_selectively"
        issue = f"density_{density_bucket}"
    elif pair_score >= 70.0:
        bucket = "probe_now"
    elif pair_score >= 58.0:
        bucket = "probe_selectively"
    elif forward_bucket == "context_dependent_forward_response" and pair_score >= 52.0:
        bucket = "context_probe"
    else:
        bucket = "hold_low_prior"

    pair_score = round(_clamp(pair_score), 2)
    label = "high" if pair_score >= 70.0 else "medium" if pair_score >= 55.0 else "low"
    return {
        "compatibility_prior_score": round(compatibility_score, 2),
        "signal_density_score": density_score,
        "signal_density_bucket": density_bucket,
        "signal_balance_bucket": balance_bucket,
        "forward_response_prior_score": round(forward_score, 2),
        "forward_response_prior_bucket": forward_bucket,
        "best_forward_context": best_context,
        "strong_cell_count": _int_value(_as_dict(forward_prior).get("strong_cell_count")),
        "recipe_fit_score": recipe_score,
        "pair_prior_score": pair_score,
        "pair_prior_label": label,
        "pair_prior_bucket": bucket,
        "pair_issue": issue,
        "shared_base_indicator": shared_base,
    }


def _select_trigger_ids(
    rows_by_id: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    signal_rollups: dict[str, dict[str, Any]],
    *,
    trigger_ids: list[str] | None,
    max_triggers: int | None,
) -> list[str]:
    explicit = _normalize_tokens(trigger_ids)
    if explicit:
        selected = [indicator_id for indicator_id in explicit if indicator_id in rows_by_id]
    else:
        selected = [
            indicator_id
            for indicator_id, row in rows_by_id.items()
            if row.get("generation_eligible")
            and _clean_token(row.get("signal_role")).lower() == "trigger"
        ]
        selected.sort(
            key=lambda indicator_id: (
                -_float_value(
                    _as_dict(forward_priors.get(indicator_id)).get("forward_response_prior_score"),
                    45.0,
                ),
                -_float_value(
                    _as_dict(signal_rollups.get(indicator_id)).get("active_percent"),
                    0.0,
                ),
                -_float_value(rows_by_id[indicator_id].get("static_prior_score"), 0.0),
                indicator_id,
            )
        )
    if max_triggers is not None and max_triggers >= 0:
        selected = selected[: int(max_triggers)]
    return selected


def _anchor_timeframe(
    catalog_item: dict[str, Any],
    *,
    probe_timeframe: str,
) -> str:
    meta = _as_dict(catalog_item.get("meta"))
    config = _as_dict(catalog_item.get("config"))
    preferred_role = _clean_token(meta.get("preferredTimeframeRole")).lower()
    default_timeframe = _clean_upper(config.get("timeframe"))
    if preferred_role == "higher-context" and default_timeframe:
        return default_timeframe
    return _clean_upper(probe_timeframe)


def _profile_config(
    catalog_item: dict[str, Any],
    *,
    indicator_id: str,
    timeframe: str,
    weight: float,
    lookback_bars: int | None = None,
) -> dict[str, Any]:
    config = copy.deepcopy(_as_dict(catalog_item.get("config")))
    config["timeframe"] = timeframe
    config["isActive"] = True
    config["weight"] = weight
    if not config.get("label"):
        config["label"] = indicator_id
    if lookback_bars is not None:
        config["lookbackBars"] = max(1, int(lookback_bars))
    elif not config.get("lookbackBars"):
        config["lookbackBars"] = 1
    if not config.get("talibConfig"):
        config["talibConfig"] = []
    if not config.get("ranges"):
        config["ranges"] = {"buy": [0, 1], "sell": [0, 1]}
    if "useFormingBar" not in config:
        config["useFormingBar"] = False
    if "normalizationMode" not in config:
        config["normalizationMode"] = "none"
    if "scale" not in config:
        config["scale"] = 1.0
    return config


def _profile_meta(
    catalog_item: dict[str, Any],
    *,
    indicator_id: str,
    instance_id: str,
) -> dict[str, Any]:
    meta = copy.deepcopy(_as_dict(catalog_item.get("meta")))
    meta["id"] = indicator_id
    meta["instanceId"] = instance_id
    return meta


def build_pair_profile_document(
    *,
    catalog_by_id: dict[str, dict[str, Any]],
    anchor_id: str,
    trigger_id: str,
    anchor_type: str,
    probe_timeframe: str,
    anchor_timeframe: str,
    instruments: list[str],
    probe_id: str,
    anchor_lookback_bars: int | None = None,
    trigger_lookback_bars: int | None = None,
) -> dict[str, Any]:
    anchor_item = catalog_by_id[anchor_id]
    trigger_item = catalog_by_id[trigger_id]
    return {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {
            "name": f"Atlas L3 {anchor_type} {anchor_id}+{trigger_id} {probe_timeframe}",
            "description": (
                "Temporary AutoResearch Layer 3 anchor-pair probe profile. "
                "Designed to test trigger behavior conditional on a recipe anchor."
            ),
            "directionMode": "both",
            "indicators": [
                {
                    "meta": _profile_meta(
                        anchor_item,
                        indicator_id=anchor_id,
                        instance_id=f"{probe_id}-anchor",
                    ),
                    "config": _profile_config(
                        anchor_item,
                        indicator_id=anchor_id,
                        timeframe=anchor_timeframe,
                        weight=1.0,
                        lookback_bars=anchor_lookback_bars,
                    ),
                },
                {
                    "meta": _profile_meta(
                        trigger_item,
                        indicator_id=trigger_id,
                        instance_id=f"{probe_id}-trigger",
                    ),
                    "config": _profile_config(
                        trigger_item,
                        indicator_id=trigger_id,
                        timeframe=probe_timeframe,
                        weight=1.0,
                        lookback_bars=trigger_lookback_bars,
                    ),
                },
            ],
            "instruments": instruments,
            "isActive": False,
            "notificationThreshold": 80,
            "version": "v1",
        },
    }


def build_anchor_pair_rows(
    *,
    rows_by_id: dict[str, dict[str, Any]],
    static_pairs: dict[tuple[str, str, str], dict[str, Any]],
    signal_rollups: dict[str, dict[str, Any]],
    forward_priors: dict[str, dict[str, Any]],
    anchor_ids: list[str] | None,
    trigger_ids: list[str] | None,
    timeframes: list[str],
    instruments: list[str],
    max_triggers: int | None,
) -> list[dict[str, Any]]:
    selected_triggers = _select_trigger_ids(
        rows_by_id,
        forward_priors,
        signal_rollups,
        trigger_ids=trigger_ids,
        max_triggers=max_triggers,
    )
    selected_anchors = _anchor_defaults(anchor_ids)
    rows: list[dict[str, Any]] = []
    for anchor_type, anchor_id in selected_anchors:
        anchor_row = rows_by_id.get(anchor_id)
        if not anchor_row or not anchor_row.get("generation_eligible"):
            continue
        for trigger_id in selected_triggers:
            if trigger_id == anchor_id:
                continue
            trigger_row = rows_by_id.get(trigger_id)
            if not trigger_row or not trigger_row.get("generation_eligible"):
                continue
            static_pair_row = static_pairs.get((anchor_type, anchor_id, trigger_id))
            for probe_timeframe in timeframes:
                score_fields = score_anchor_pair(
                    anchor_type=anchor_type,
                    anchor_row=anchor_row,
                    trigger_row=trigger_row,
                    static_pair_row=static_pair_row,
                    signal_rollup=signal_rollups.get(trigger_id),
                    forward_prior=forward_priors.get(trigger_id),
                    probe_timeframe=probe_timeframe,
                    instruments=instruments,
                )
                rows.append(
                    {
                        "anchor_type": anchor_type,
                        "recipe": ANCHOR_TYPE_RECIPES.get(anchor_type, anchor_type),
                        "anchor_id": anchor_id,
                        "anchor_signal_role": anchor_row.get("signal_role"),
                        "anchor_strategy_role": anchor_row.get("strategy_role"),
                        "trigger_id": trigger_id,
                        "trigger_signal_role": trigger_row.get("signal_role"),
                        "trigger_strategy_role": trigger_row.get("strategy_role"),
                        "probe_timeframe": _clean_upper(probe_timeframe),
                        "instruments": ",".join(instruments),
                        "trigger_active_percent": _as_dict(signal_rollups.get(trigger_id)).get(
                            "active_percent"
                        ),
                        "trigger_event_count": _as_dict(signal_rollups.get(trigger_id)).get(
                            "event_count"
                        ),
                        **score_fields,
                    }
                )
    rows.sort(
        key=lambda row: (
            -_float_value(row.get("pair_prior_score")),
            str(row.get("anchor_type") or ""),
            str(row.get("anchor_id") or ""),
            str(row.get("trigger_id") or ""),
            str(row.get("probe_timeframe") or ""),
        )
    )
    return rows


def select_anchor_pair_queue(
    rows: list[dict[str, Any]],
    *,
    max_pairs: int = DEFAULT_MAX_QUEUE_PAIRS,
) -> list[dict[str, Any]]:
    max_pairs = max(1, int(max_pairs or DEFAULT_MAX_QUEUE_PAIRS))
    preferred = [
        row
        for row in rows
        if row.get("pair_prior_bucket")
        in {"probe_now", "probe_selectively", "context_probe"}
    ]
    preferred.sort(
        key=lambda row: (
            -_float_value(row.get("pair_prior_score")),
            str(row.get("anchor_type") or ""),
            str(row.get("trigger_id") or ""),
        )
    )
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str, str]] = set()
    anchor_types = sorted({str(row.get("anchor_type") or "") for row in preferred})
    while len(selected) < max_pairs and anchor_types:
        added_this_round = False
        for anchor_type in list(anchor_types):
            candidate = next(
                (
                    row
                    for row in preferred
                    if row.get("anchor_type") == anchor_type
                    and (
                        str(row.get("anchor_id") or ""),
                        str(row.get("trigger_id") or ""),
                        str(row.get("probe_timeframe") or ""),
                    )
                    not in selected_keys
                ),
                None,
            )
            if candidate is None:
                anchor_types.remove(anchor_type)
                continue
            key = (
                str(candidate.get("anchor_id") or ""),
                str(candidate.get("trigger_id") or ""),
                str(candidate.get("probe_timeframe") or ""),
            )
            selected_keys.add(key)
            selected.append(candidate)
            added_this_round = True
            if len(selected) >= max_pairs:
                break
        if not added_this_round:
            break

    wildcard_budget = max(1, int(round(max_pairs * 0.05))) if max_pairs >= 10 else 0
    wildcard_candidates = [
        row
        for row in rows
        if row.get("pair_prior_bucket") == "hold_low_prior"
        and row.get("signal_density_bucket") not in {"flat", "no_data"}
    ]
    wildcard_candidates.sort(
        key=lambda row: (
            -_float_value(row.get("forward_response_prior_score")),
            -_float_value(row.get("compatibility_prior_score")),
            str(row.get("trigger_id") or ""),
        )
    )
    for candidate in wildcard_candidates:
        if len(selected) >= max_pairs:
            break
        if wildcard_budget <= 0:
            break
        key = (
            str(candidate.get("anchor_id") or ""),
            str(candidate.get("trigger_id") or ""),
            str(candidate.get("probe_timeframe") or ""),
        )
        if key in selected_keys:
            continue
        selected_keys.add(key)
        row = dict(candidate)
        row["pair_prior_bucket"] = "wildcard_probe"
        selected.append(row)
        wildcard_budget -= 1

    selected.sort(
        key=lambda row: (
            -_float_value(row.get("pair_prior_score")),
            str(row.get("anchor_type") or ""),
            str(row.get("trigger_id") or ""),
        )
    )
    for index, row in enumerate(selected, start=1):
        row["queue_rank"] = index
        row["probe_id"] = (
            f"l3-{index:03d}-"
            f"{str(row.get('anchor_id') or '').lower().replace('_', '-')}-"
            f"{str(row.get('trigger_id') or '').lower().replace('_', '-')}-"
            f"{str(row.get('probe_timeframe') or '').lower()}"
        )
    return selected


def _ps_array(values: list[str], *, raw_tokens: set[str] | None = None) -> str:
    if not values:
        return "@()"
    raw = raw_tokens or set()
    parts: list[str] = []
    for value in values:
        if value in raw:
            parts.append(value)
        else:
            parts.append("'" + value.replace("'", "''") + "'")
    return "@(" + ", ".join(parts) + ")"


def _fuzzfolio_base_args(config: AppConfig) -> tuple[str, list[str]]:
    fuzzfolio = config.fuzzfolio
    args: list[str] = []
    if fuzzfolio.base_url:
        args.extend(["--base-url", fuzzfolio.base_url])
    if fuzzfolio.auth_profile:
        args.extend(["--auth-profile", fuzzfolio.auth_profile])
    if fuzzfolio.workspace_root:
        args.extend(["--workspace-root", str(fuzzfolio.workspace_root)])
    return fuzzfolio.cli_command, args


def _sensitivity_args_for_row(
    row: dict[str, Any],
    *,
    lookback_months: int,
    as_of_date: str | None,
    quality_score_preset: str,
    execution_cost_mode: str,
    result_dir: Path,
) -> list[str]:
    args = [
        "sensitivity-basket",
        "--profile-ref",
        "<PROFILE_ID>",
        "--timeframe",
        _clean_upper(row.get("probe_timeframe")),
        "--lookback-months",
        str(int(lookback_months)),
        "--as-of-date",
        resolve_probe_as_of_date(as_of_date),
    ]
    for instrument in [
        token for token in _clean_token(row.get("instruments")).split(",") if token
    ]:
        args.extend(["--instrument", instrument])
    args.extend(
        [
            "--quality-score-preset",
            quality_score_preset,
            "--execution-cost-mode",
            execution_cost_mode,
            "--allow-timeframe-mismatch",
            "--include-per-instrument",
            "--path-metrics-mode",
            "highlighted",
            "--cell-detail-artifacts",
            "recommended",
            "--output-dir",
            str(result_dir),
        ]
    )
    return args


def _replace_profile_id_arg(args: list[str], profile_id: str) -> list[str]:
    return [profile_id if arg == "<PROFILE_ID>" else arg for arg in args]


def _write_run_script(
    path: Path,
    *,
    exe: str,
    base_args: list[str],
    probes: list[dict[str, Any]],
    generated_by: str = "uv run build-anchor-pair-atlas",
    description: str = "Runs the queued Layer 3 anchor-pair sensitivity-basket probes.",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    escaped_exe = exe.replace("'", "''")
    lines = [
        f"# Auto-generated by `{generated_by}`.",
        f"# {description}",
        "$ErrorActionPreference = 'Stop'",
        f"$FuzzfolioExe = '{escaped_exe}'",
        "$FuzzfolioBaseArgs = " + _ps_array(base_args),
        "function Invoke-Fuzzfolio {",
        "  param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Args)",
        "  & $FuzzfolioExe @FuzzfolioBaseArgs @Args",
        "  if ($LASTEXITCODE -ne 0) { throw \"fuzzfolio-agent-cli failed with exit $LASTEXITCODE\" }",
        "}",
        "",
    ]
    for probe in probes:
        sensitivity_args = list(probe["sensitivity_basket_args"])
        output_dir = str(probe["output_dir"])
        escaped_output_dir = output_dir.replace("'", "''")
        args_without_profile_id: list[str] = []
        skip_next = False
        for index, arg in enumerate(sensitivity_args):
            if skip_next:
                skip_next = False
                continue
            if arg == "--profile-ref":
                args_without_profile_id.extend(["--profile-ref", "$profileId"])
                skip_next = True
                continue
            args_without_profile_id.append(arg)
        lines.extend(
            [
                f"Write-Host 'Running {probe['probe_id']}'",
                "$created = Invoke-Fuzzfolio "
                + _ps_array(["profiles", "create", "--file", probe["profile_path"], "--pretty"])
                + " | ConvertFrom-Json",
                "$profileId = $created.data.id",
                f"New-Item -ItemType Directory -Force -Path '{escaped_output_dir}' | Out-Null",
                "Invoke-Fuzzfolio "
                + _ps_array(args_without_profile_id, raw_tokens={"$profileId"}),
                "",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _matrix_fieldnames() -> list[str]:
    return [
        "anchor_type",
        "recipe",
        "anchor_id",
        "anchor_signal_role",
        "anchor_strategy_role",
        "trigger_id",
        "trigger_signal_role",
        "trigger_strategy_role",
        "probe_timeframe",
        "instruments",
        "compatibility_prior_score",
        "signal_density_score",
        "signal_density_bucket",
        "signal_balance_bucket",
        "trigger_active_percent",
        "trigger_event_count",
        "forward_response_prior_score",
        "forward_response_prior_bucket",
        "best_forward_context",
        "strong_cell_count",
        "recipe_fit_score",
        "pair_prior_score",
        "pair_prior_label",
        "pair_prior_bucket",
        "pair_issue",
        "shared_base_indicator",
    ]


def _queue_fieldnames() -> list[str]:
    return [
        "queue_rank",
        "probe_id",
        *_matrix_fieldnames(),
        "anchor_timeframe",
        "profile_path",
        "result_dir",
    ]


def build_anchor_pair_atlas(
    config: AppConfig,
    *,
    indicator_atlas_dir: Path | None = None,
    signal_atlas_dir: Path | None = None,
    forward_response_dir: Path | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    refresh_static_atlas: bool = False,
    anchor_ids: list[str] | None = None,
    trigger_ids: list[str] | None = None,
    instruments: list[str] | None = None,
    timeframes: list[str] | None = None,
    max_triggers: int | None = None,
    max_pairs: int = DEFAULT_MAX_QUEUE_PAIRS,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    as_of_date: str | None = None,
    emit_profile_docs: bool = True,
    quality_score_preset: str = DEFAULT_QUALITY_SCORE_PRESET,
    execution_cost_mode: str = DEFAULT_EXECUTION_COST_MODE,
) -> AnchorPairAtlasBuildResult:
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_DIRNAME
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
    if not static_atlas_path.exists():
        raise FileNotFoundError(
            f"Missing indicator atlas at {static_atlas_path}. Run `uv run build-indicator-atlas` first."
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

    static_payload = _as_dict(_load_json(static_atlas_path))
    static_rows = [
        row for row in _as_list(static_payload.get("indicators")) if isinstance(row, dict)
    ]
    indicator_rows_by_id = _rows_by_id(static_rows)
    static_pairs = _static_pair_lookup(indicator_dir)
    signal_rollups = _signal_rollups_by_id(signal_dir)
    forward_priors = _forward_priors_by_id(forward_dir)
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
    rows = build_anchor_pair_rows(
        rows_by_id=indicator_rows_by_id,
        static_pairs=static_pairs,
        signal_rollups=signal_rollups,
        forward_priors=forward_priors,
        anchor_ids=anchor_ids,
        trigger_ids=trigger_ids,
        timeframes=timeframe_panel,
        instruments=instrument_panel,
        max_triggers=max_triggers,
    )
    queue_rows = select_anchor_pair_queue(rows, max_pairs=max_pairs)

    probes: list[dict[str, Any]] = []
    exe, base_args = _fuzzfolio_base_args(config)
    for row in queue_rows:
        anchor_id = _clean_upper(row.get("anchor_id"))
        trigger_id = _clean_upper(row.get("trigger_id"))
        if anchor_id not in catalog_by_id or trigger_id not in catalog_by_id:
            continue
        anchor_timeframe = _anchor_timeframe(
            catalog_by_id[anchor_id],
            probe_timeframe=_clean_upper(row.get("probe_timeframe")),
        )
        probe_id = _clean_token(row.get("probe_id"))
        profile_path = profile_dir / f"{probe_id}.json"
        result_dir = result_root / probe_id
        if emit_profile_docs:
            profile_doc = build_pair_profile_document(
                catalog_by_id=catalog_by_id,
                anchor_id=anchor_id,
                trigger_id=trigger_id,
                anchor_type=_clean_token(row.get("anchor_type")),
                probe_timeframe=_clean_upper(row.get("probe_timeframe")),
                anchor_timeframe=anchor_timeframe,
                instruments=instrument_panel,
                probe_id=probe_id,
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
        probes.append(
            {
                "probe_id": probe_id,
                "queue_rank": row.get("queue_rank"),
                "anchor_type": row.get("anchor_type"),
                "anchor_id": anchor_id,
                "trigger_id": trigger_id,
                "probe_timeframe": row.get("probe_timeframe"),
                "anchor_timeframe": anchor_timeframe,
                "profile_path": str(profile_path),
                "output_dir": str(result_dir),
                "create_profile_args": ["profiles", "create", "--file", str(profile_path), "--pretty"],
                "sensitivity_basket_args": sensitivity_args,
                "pair_prior_score": row.get("pair_prior_score"),
                "pair_prior_bucket": row.get("pair_prior_bucket"),
            }
        )

    bucket_counts: dict[str, int] = {}
    anchor_type_counts: dict[str, int] = {}
    for row in rows:
        bucket = _clean_token(row.get("pair_prior_bucket")) or "unknown"
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        anchor_type = _clean_token(row.get("anchor_type")) or "unknown"
        anchor_type_counts[anchor_type] = anchor_type_counts.get(anchor_type, 0) + 1
    queue_bucket_counts: dict[str, int] = {}
    for row in queue_rows:
        bucket = _clean_token(row.get("pair_prior_bucket")) or "unknown"
        queue_bucket_counts[bucket] = queue_bucket_counts.get(bucket, 0) + 1

    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "indicator_atlas_path": str(static_atlas_path),
            "signal_atlas_dir": str(signal_dir),
            "forward_response_dir": str(forward_dir),
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
        },
        "selection": {
            "anchors": [
                {"anchor_type": anchor_type, "anchor_id": anchor_id}
                for anchor_type, anchor_id in _anchor_defaults(anchor_ids)
            ],
            "trigger_count": len(
                _select_trigger_ids(
                    indicator_rows_by_id,
                    forward_priors,
                    signal_rollups,
                    trigger_ids=trigger_ids,
                    max_triggers=max_triggers,
                )
            ),
            "instruments": instrument_panel,
            "timeframes": timeframe_panel,
            "max_pairs": max_pairs,
            "lookback_months": lookback_months,
            "as_of_date": resolved_as_of_date,
            "quality_score_preset": quality_score_preset,
            "execution_cost_mode": execution_cost_mode,
        },
        "result_counts": {
            "pair_matrix_rows": len(rows),
            "queue_rows": len(queue_rows),
            "profile_docs": len(probes) if emit_profile_docs else 0,
            "pair_prior_bucket_counts": dict(sorted(bucket_counts.items())),
            "queue_bucket_counts": dict(sorted(queue_bucket_counts.items())),
            "anchor_type_counts": dict(sorted(anchor_type_counts.items())),
        },
        "top_queue": [
            {
                "queue_rank": row.get("queue_rank"),
                "probe_id": row.get("probe_id"),
                "anchor_type": row.get("anchor_type"),
                "anchor_id": row.get("anchor_id"),
                "trigger_id": row.get("trigger_id"),
                "probe_timeframe": row.get("probe_timeframe"),
                "pair_prior_score": row.get("pair_prior_score"),
                "pair_prior_bucket": row.get("pair_prior_bucket"),
                "forward_response_prior_bucket": row.get("forward_response_prior_bucket"),
                "best_forward_context": row.get("best_forward_context"),
            }
            for row in queue_rows[:10]
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

    atlas_path = target_dir / "anchor-pair-atlas.json"
    matrix_csv_path = target_dir / "anchor-pair-matrix.csv"
    queue_csv_path = target_dir / "anchor-pair-queue.csv"
    manifest_path = target_dir / "anchor-pair-run-manifest.json"
    run_script_path = target_dir / "run-anchor-pair-probes.ps1"
    summary_path = target_dir / "anchor-pair-summary.json"

    _write_json(atlas_path, atlas_payload)
    _write_csv(matrix_csv_path, rows, _matrix_fieldnames())
    _write_csv(queue_csv_path, queue_rows, _queue_fieldnames())
    _write_json(
        manifest_path,
        {
            "schema_version": "anchor_pair_run_manifest_v1",
            "generated_at": summary["generated_at"],
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    )
    _write_run_script(run_script_path, exe=exe, base_args=base_args, probes=probes)
    _write_json(summary_path, summary)

    return AnchorPairAtlasBuildResult(
        atlas_path=atlas_path,
        matrix_csv_path=matrix_csv_path,
        queue_csv_path=queue_csv_path,
        manifest_path=manifest_path,
        run_script_path=run_script_path,
        profile_dir=profile_dir,
        summary_path=summary_path,
        summary=summary,
    )


def _row_instruments(row: dict[str, Any]) -> list[str]:
    return [
        token
        for token in _clean_token(row.get("instruments")).split(",")
        if token
    ] or list(DEFAULT_INSTRUMENTS)


def _timing_lookback_values(values: list[int] | tuple[int, ...] | None) -> list[int]:
    cleaned: list[int] = []
    seen: set[int] = set()
    for value in values or DEFAULT_TIMING_LOOKBACK_BARS:
        lookback = max(1, _int_value(value, 1))
        if lookback in seen:
            continue
        cleaned.append(lookback)
        seen.add(lookback)
    return sorted(cleaned)


def _timing_probe_id(
    *,
    rank: int,
    base_probe_id: str,
    variant_side: str,
    lookback_bars: int,
) -> str:
    side_token = _clean_token(variant_side)
    side = "an" if side_token == "anchor" else "bt" if side_token == "both" else "tr"
    return f"l3b-{rank:03d}-{base_probe_id}-{side}-lb{lookback_bars}"


def _timing_variant_sides(values: list[str] | tuple[str, ...] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or ("trigger", "anchor"):
        side = _clean_token(value).lower()
        if side not in {"trigger", "anchor", "both"} or side in seen:
            continue
        cleaned.append(side)
        seen.add(side)
    return cleaned or ["trigger"]


def _timing_queue_fieldnames() -> list[str]:
    return [
        "queue_rank",
        "probe_id",
        "timing_rank",
        "timing_probe_id",
        "base_queue_rank",
        "base_probe_id",
        "variant_side",
        "variant_indicator_id",
        "variant_lookback_bars",
        "baseline_lookback_bars",
        "anchor_baseline_lookback_bars",
        "trigger_baseline_lookback_bars",
        "anchor_variant_lookback_bars",
        "trigger_variant_lookback_bars",
        "anchor_type",
        "recipe",
        "anchor_id",
        "anchor_signal_role",
        "anchor_strategy_role",
        "trigger_id",
        "trigger_signal_role",
        "trigger_strategy_role",
        "probe_timeframe",
        "instruments",
        "anchor_timeframe",
        "pair_prior_score",
        "pair_prior_bucket",
        "baseline_status",
        "baseline_composite_score",
        "baseline_signal_count",
        "baseline_best_expectancy_r",
        "baseline_best_trades",
        "baseline_best_profit_factor",
        "profile_path",
        "result_dir",
    ]


def build_anchor_pair_timing_atlas(
    config: AppConfig,
    *,
    anchor_pair_atlas_dir: Path | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    base_probe_ids: list[str] | None = None,
    limit_base_pairs: int | None = None,
    lookback_bars: list[int] | tuple[int, ...] | None = None,
    variant_sides: list[str] | tuple[str, ...] | None = None,
    include_baseline_variants: bool = False,
    emit_profile_docs: bool = True,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    as_of_date: str | None = None,
    quality_score_preset: str = DEFAULT_QUALITY_SCORE_PRESET,
    execution_cost_mode: str = DEFAULT_EXECUTION_COST_MODE,
) -> AnchorPairTimingAtlasBuildResult:
    source_dir = (
        anchor_pair_atlas_dir.expanduser().resolve()
        if anchor_pair_atlas_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_DIRNAME
    )
    source_atlas_path = source_dir / "anchor-pair-atlas.json"
    source_results_path = source_dir / "anchor-pair-probe-results.csv"
    if not source_atlas_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair atlas at {source_atlas_path}. Run `uv run build-anchor-pair-atlas` first."
        )
    if not source_results_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair results at {source_results_path}. Run `uv run run-anchor-pair-probes --all` first."
        )

    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = target_dir / "profiles"
    result_root = target_dir / "probe-results"
    if emit_profile_docs:
        profile_dir.mkdir(parents=True, exist_ok=True)
    result_root.mkdir(parents=True, exist_ok=True)

    source_payload = _as_dict(_load_json(source_atlas_path))
    base_queue_rows = [
        row for row in _as_list(source_payload.get("queue_rows")) if isinstance(row, dict)
    ]
    selected_base_rows = _select_probe_rows(
        base_queue_rows,
        probe_ids=base_probe_ids,
        limit=limit_base_pairs,
    )
    baseline_results = {
        _clean_token(row.get("probe_id")): row
        for row in _read_csv_rows(source_results_path)
        if _clean_token(row.get("probe_id"))
    }
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    catalog_by_id = _catalog_by_id(catalog_payload)
    timing_lookbacks = _timing_lookback_values(lookback_bars)
    timing_variant_sides = _timing_variant_sides(variant_sides)
    lookback_months = max(1, int(lookback_months or DEFAULT_LOOKBACK_MONTHS))
    resolved_as_of_date = resolve_probe_as_of_date(as_of_date)

    exe, base_args = _fuzzfolio_base_args(config)
    timing_rows: list[dict[str, Any]] = []
    probes: list[dict[str, Any]] = []
    skipped_missing_baseline: list[str] = []
    skipped_missing_catalog: list[str] = []
    rank = 0
    for base_row in selected_base_rows:
        base_probe_id = _clean_token(base_row.get("probe_id"))
        baseline_row = baseline_results.get(base_probe_id)
        if not baseline_row:
            skipped_missing_baseline.append(base_probe_id)
            continue
        anchor_id = _clean_upper(base_row.get("anchor_id"))
        trigger_id = _clean_upper(base_row.get("trigger_id"))
        if anchor_id not in catalog_by_id or trigger_id not in catalog_by_id:
            skipped_missing_catalog.append(base_probe_id)
            continue
        anchor_timeframe = _clean_upper(base_row.get("anchor_timeframe")) or _anchor_timeframe(
            catalog_by_id[anchor_id],
            probe_timeframe=_clean_upper(base_row.get("probe_timeframe")),
        )
        for variant_side in timing_variant_sides:
            if variant_side == "anchor":
                variant_indicator_id = anchor_id
            elif variant_side == "both":
                variant_indicator_id = f"{anchor_id}+{trigger_id}"
            else:
                variant_indicator_id = trigger_id
            anchor_baseline_lookback = _catalog_lookback_bars(catalog_by_id, anchor_id)
            trigger_baseline_lookback = _catalog_lookback_bars(catalog_by_id, trigger_id)
            baseline_lookback = (
                anchor_baseline_lookback
                if variant_side == "anchor"
                else f"{anchor_baseline_lookback}+{trigger_baseline_lookback}"
                if variant_side == "both"
                else trigger_baseline_lookback
            )
            for variant_lookback in timing_lookbacks:
                is_baseline_variant = (
                    variant_lookback == anchor_baseline_lookback
                    if variant_side == "anchor"
                    else (
                        variant_lookback == anchor_baseline_lookback
                        and variant_lookback == trigger_baseline_lookback
                    )
                    if variant_side == "both"
                    else variant_lookback == trigger_baseline_lookback
                )
                if (
                    not include_baseline_variants
                    and is_baseline_variant
                ):
                    continue
                rank += 1
                timing_probe_id = _timing_probe_id(
                    rank=rank,
                    base_probe_id=base_probe_id,
                    variant_side=variant_side,
                    lookback_bars=variant_lookback,
                )
                profile_path = profile_dir / f"{timing_probe_id}.json"
                result_dir = result_root / timing_probe_id
                if emit_profile_docs:
                    profile_doc = build_pair_profile_document(
                        catalog_by_id=catalog_by_id,
                        anchor_id=anchor_id,
                        trigger_id=trigger_id,
                        anchor_type=_clean_token(base_row.get("anchor_type")),
                        probe_timeframe=_clean_upper(base_row.get("probe_timeframe")),
                        anchor_timeframe=anchor_timeframe,
                        instruments=_row_instruments(base_row),
                        probe_id=timing_probe_id,
                        anchor_lookback_bars=variant_lookback
                        if variant_side in {"anchor", "both"}
                        else None,
                        trigger_lookback_bars=variant_lookback
                        if variant_side in {"trigger", "both"}
                        else None,
                    )
                    profile_doc["profile"]["name"] = (
                        f"Atlas L3b timing {base_row.get('anchor_type')} "
                        f"{anchor_id}+{trigger_id} {_clean_upper(base_row.get('probe_timeframe'))} "
                        f"{variant_side} lb{variant_lookback}"
                    )
                    profile_doc["profile"]["description"] = (
                        "Temporary AutoResearch Layer 3b timing-tolerance probe profile. "
                        f"Tests whether {variant_side} score persistence improves pair confluence."
                    )
                    _write_json(profile_path, profile_doc)

                timing_row = dict(base_row)
                timing_row.update(
                    {
                        "queue_rank": rank,
                        "probe_id": timing_probe_id,
                        "timing_rank": rank,
                        "timing_probe_id": timing_probe_id,
                        "base_queue_rank": base_row.get("queue_rank"),
                        "base_probe_id": base_probe_id,
                        "variant_side": variant_side,
                        "variant_indicator_id": variant_indicator_id,
                        "variant_lookback_bars": variant_lookback,
                        "baseline_lookback_bars": baseline_lookback,
                        "anchor_baseline_lookback_bars": anchor_baseline_lookback,
                        "trigger_baseline_lookback_bars": trigger_baseline_lookback,
                        "anchor_variant_lookback_bars": variant_lookback
                        if variant_side in {"anchor", "both"}
                        else "",
                        "trigger_variant_lookback_bars": variant_lookback
                        if variant_side in {"trigger", "both"}
                        else "",
                        "anchor_timeframe": anchor_timeframe,
                        "baseline_status": baseline_row.get("status"),
                        "baseline_composite_score": baseline_row.get("composite_score"),
                        "baseline_signal_count": baseline_row.get("signal_count"),
                        "baseline_best_expectancy_r": baseline_row.get("best_expectancy_r"),
                        "baseline_best_trades": baseline_row.get("best_trades"),
                        "baseline_best_profit_factor": baseline_row.get("best_profit_factor"),
                        "profile_path": str(profile_path),
                        "result_dir": str(result_dir),
                    }
                )
                sensitivity_args = _sensitivity_args_for_row(
                    timing_row,
                    lookback_months=lookback_months,
                    as_of_date=resolved_as_of_date,
                    quality_score_preset=quality_score_preset,
                    execution_cost_mode=execution_cost_mode,
                    result_dir=result_dir,
                )
                timing_rows.append(timing_row)
                probes.append(
                    {
                        "probe_id": timing_probe_id,
                        "timing_probe_id": timing_probe_id,
                        "timing_rank": rank,
                        "base_probe_id": base_probe_id,
                        "variant_side": variant_side,
                        "variant_indicator_id": variant_indicator_id,
                        "variant_lookback_bars": variant_lookback,
                        "baseline_lookback_bars": baseline_lookback,
                        "anchor_baseline_lookback_bars": anchor_baseline_lookback,
                        "trigger_baseline_lookback_bars": trigger_baseline_lookback,
                        "anchor_variant_lookback_bars": variant_lookback
                        if variant_side in {"anchor", "both"}
                        else "",
                        "trigger_variant_lookback_bars": variant_lookback
                        if variant_side in {"trigger", "both"}
                        else "",
                        "anchor_type": base_row.get("anchor_type"),
                        "anchor_id": anchor_id,
                        "trigger_id": trigger_id,
                        "probe_timeframe": base_row.get("probe_timeframe"),
                        "anchor_timeframe": anchor_timeframe,
                        "profile_path": str(profile_path),
                        "output_dir": str(result_dir),
                        "create_profile_args": ["profiles", "create", "--file", str(profile_path), "--pretty"],
                        "sensitivity_basket_args": sensitivity_args,
                        "pair_prior_score": base_row.get("pair_prior_score"),
                        "pair_prior_bucket": base_row.get("pair_prior_bucket"),
                    }
                )

    lookback_variant_counts: dict[str, int] = {}
    variant_side_counts: dict[str, int] = {}
    for row in timing_rows:
        lookback = _clean_token(row.get("variant_lookback_bars"))
        lookback_variant_counts[lookback] = lookback_variant_counts.get(lookback, 0) + 1
        side = _clean_token(row.get("variant_side")) or "unknown"
        variant_side_counts[side] = variant_side_counts.get(side, 0) + 1

    summary = {
        "schema_version": TIMING_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "anchor_pair_atlas_path": str(source_atlas_path),
            "anchor_pair_results_path": str(source_results_path),
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
        },
        "selection": {
            "requested_base_probe_ids": base_probe_ids or [],
            "limit_base_pairs": limit_base_pairs,
            "lookback_bars": timing_lookbacks,
            "variant_sides": timing_variant_sides,
            "include_baseline_variants": include_baseline_variants,
            "lookback_months": lookback_months,
            "as_of_date": resolved_as_of_date,
            "quality_score_preset": quality_score_preset,
            "execution_cost_mode": execution_cost_mode,
        },
        "result_counts": {
            "base_pairs_considered": len(selected_base_rows),
            "timing_variants": len(timing_rows),
            "profile_docs": len(probes) if emit_profile_docs else 0,
            "lookback_variant_counts": dict(sorted(lookback_variant_counts.items())),
            "variant_side_counts": dict(sorted(variant_side_counts.items())),
            "skipped_missing_baseline": len(skipped_missing_baseline),
            "skipped_missing_catalog": len(skipped_missing_catalog),
        },
        "skipped": {
            "missing_baseline": skipped_missing_baseline,
            "missing_catalog": skipped_missing_catalog,
        },
        "top_queue": timing_rows[:10],
    }

    atlas_payload = {
        "schema_version": TIMING_SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "timing_queue_rows": timing_rows,
        "run_manifest": {
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    }

    atlas_path = target_dir / "anchor-pair-timing-atlas.json"
    queue_csv_path = target_dir / "anchor-pair-timing-queue.csv"
    manifest_path = target_dir / "anchor-pair-timing-run-manifest.json"
    run_script_path = target_dir / "run-anchor-pair-timing-probes.ps1"
    summary_path = target_dir / "anchor-pair-timing-build-summary.json"

    _write_json(atlas_path, atlas_payload)
    _write_csv(queue_csv_path, timing_rows, _timing_queue_fieldnames())
    _write_json(
        manifest_path,
        {
            "schema_version": "anchor_pair_timing_run_manifest_v1",
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
        generated_by="uv run build-anchor-pair-timing-atlas",
        description="Runs the queued Layer 3b anchor-pair timing-tolerance probes.",
    )
    _write_json(summary_path, summary)

    return AnchorPairTimingAtlasBuildResult(
        atlas_path=atlas_path,
        queue_csv_path=queue_csv_path,
        manifest_path=manifest_path,
        run_script_path=run_script_path,
        profile_dir=profile_dir,
        summary_path=summary_path,
        summary=summary,
    )


def _probe_rows_by_id(queue_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in queue_rows:
        probe_id = _clean_token(row.get("probe_id"))
        if probe_id:
            rows[probe_id] = row
    return rows


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


def _result_row_from_score(
    row: dict[str, Any],
    *,
    profile_id: str | None,
    output_dir: Path,
    status: str,
    score_payload: Any | None = None,
    sensitivity_snapshot: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    result = {
        "probe_id": row.get("probe_id"),
        "queue_rank": row.get("queue_rank"),
        "anchor_type": row.get("anchor_type"),
        "anchor_id": row.get("anchor_id"),
        "trigger_id": row.get("trigger_id"),
        "probe_timeframe": row.get("probe_timeframe"),
        "pair_prior_score": row.get("pair_prior_score"),
        "pair_prior_bucket": row.get("pair_prior_bucket"),
        "profile_id": profile_id,
        "output_dir": str(output_dir),
        "status": status,
        "primary_score": None,
        "composite_score": None,
        "score_basis": None,
        "signal_count": None,
        "best_expectancy_r": None,
        "best_trades": None,
        "best_win_rate": None,
        "best_profit_factor": None,
        "error": error,
    }
    if score_payload is not None:
        result.update(
            {
                "primary_score": getattr(score_payload, "primary_score", None),
                "composite_score": getattr(score_payload, "composite_score", None),
                "score_basis": getattr(score_payload, "score_basis", None),
            }
        )
        best_summary = getattr(score_payload, "best_summary", {}) or {}
        metrics = getattr(score_payload, "metrics", {}) or {}
        result["best_trades"] = (
            best_summary.get("trades")
            or best_summary.get("trade_count")
            or metrics.get("trades")
            or metrics.get("trade_count")
        )
        result["best_win_rate"] = best_summary.get("win_rate") or metrics.get("win_rate")
        result["best_profit_factor"] = (
            best_summary.get("profit_factor") or metrics.get("profit_factor")
        )
    snapshot = _as_dict(sensitivity_snapshot)
    aggregate = _as_dict(_as_dict(snapshot.get("data")).get("aggregate"))
    if not aggregate:
        aggregate = _as_dict(snapshot.get("aggregate"))
    best_cell = _as_dict(aggregate.get("best_cell"))
    recommended_cell = _as_dict(aggregate.get("recommended_cell"))
    behavior = _as_dict(aggregate.get("behavior_summary"))
    result["signal_count"] = (
        behavior.get("signal_count")
        or (
            _float_value(behavior.get("long_signal_count"))
            + _float_value(behavior.get("short_signal_count"))
        )
        or None
    )
    result["best_expectancy_r"] = (
        best_cell.get("avg_net_r_per_closed_trade")
        or recommended_cell.get("avg_net_r_per_closed_trade")
    )
    result["best_trades"] = (
        result.get("best_trades")
        or best_cell.get("resolved_trades")
        or recommended_cell.get("resolved_trades")
    )
    result["best_win_rate"] = (
        result.get("best_win_rate")
        or best_cell.get("win_rate")
        or recommended_cell.get("win_rate")
    )
    result["best_profit_factor"] = (
        result.get("best_profit_factor")
        or best_cell.get("profit_factor")
        or recommended_cell.get("profit_factor")
    )
    return result


def _probe_results_fieldnames() -> list[str]:
    return [
        "probe_id",
        "queue_rank",
        "anchor_type",
        "anchor_id",
        "trigger_id",
        "probe_timeframe",
        "pair_prior_score",
        "pair_prior_bucket",
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


def _numeric_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    number = _float_value(value, default=math.nan)
    return number if math.isfinite(number) else None


def _numeric_delta(current: Any, baseline: Any) -> float | None:
    current_number = _numeric_or_none(current)
    baseline_number = _numeric_or_none(baseline)
    if current_number is None or baseline_number is None:
        return None
    return current_number - baseline_number


def _timing_bucket(*, score: Any, baseline_score: Any, status: str) -> str:
    if status not in {"ok", "skipped_existing"}:
        return "unscored"
    score_number = _numeric_or_none(score)
    baseline_number = _numeric_or_none(baseline_score)
    if score_number is None or baseline_number is None:
        return "unscored"
    delta = score_number - baseline_number
    if baseline_number < 50.0 <= score_number:
        return "rescued_positive"
    if baseline_number >= 50.0 > score_number:
        return "lost_positive"
    if delta >= 5.0:
        return "improved"
    if delta <= -5.0:
        return "degraded"
    return "neutral"


def _timing_result_row_from_score(
    row: dict[str, Any],
    *,
    profile_id: str | None,
    output_dir: Path,
    status: str,
    score_payload: Any | None = None,
    sensitivity_snapshot: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    scored = _result_row_from_score(
        row,
        profile_id=profile_id,
        output_dir=output_dir,
        status=status,
        score_payload=score_payload,
        sensitivity_snapshot=sensitivity_snapshot,
        error=error,
    )
    timing_bucket = _timing_bucket(
        score=scored.get("composite_score"),
        baseline_score=row.get("baseline_composite_score"),
        status=status,
    )
    return {
        "timing_probe_id": row.get("timing_probe_id") or row.get("probe_id"),
        "timing_rank": row.get("timing_rank") or row.get("queue_rank"),
        "base_probe_id": row.get("base_probe_id"),
        "base_queue_rank": row.get("base_queue_rank"),
        "variant_side": row.get("variant_side"),
        "variant_indicator_id": row.get("variant_indicator_id"),
        "variant_lookback_bars": row.get("variant_lookback_bars"),
        "baseline_lookback_bars": row.get("baseline_lookback_bars"),
        "anchor_baseline_lookback_bars": row.get("anchor_baseline_lookback_bars"),
        "trigger_baseline_lookback_bars": row.get("trigger_baseline_lookback_bars"),
        "anchor_variant_lookback_bars": row.get("anchor_variant_lookback_bars"),
        "trigger_variant_lookback_bars": row.get("trigger_variant_lookback_bars"),
        "anchor_type": row.get("anchor_type"),
        "anchor_id": row.get("anchor_id"),
        "trigger_id": row.get("trigger_id"),
        "probe_timeframe": row.get("probe_timeframe"),
        "pair_prior_score": row.get("pair_prior_score"),
        "pair_prior_bucket": row.get("pair_prior_bucket"),
        "baseline_status": row.get("baseline_status"),
        "baseline_composite_score": row.get("baseline_composite_score"),
        "baseline_signal_count": row.get("baseline_signal_count"),
        "baseline_best_expectancy_r": row.get("baseline_best_expectancy_r"),
        "baseline_best_trades": row.get("baseline_best_trades"),
        "baseline_best_profit_factor": row.get("baseline_best_profit_factor"),
        "profile_id": profile_id,
        "output_dir": str(output_dir),
        "status": status,
        "primary_score": scored.get("primary_score"),
        "composite_score": scored.get("composite_score"),
        "score_delta": _numeric_delta(
            scored.get("composite_score"),
            row.get("baseline_composite_score"),
        ),
        "score_basis": scored.get("score_basis"),
        "signal_count": scored.get("signal_count"),
        "signal_count_delta": _numeric_delta(
            scored.get("signal_count"),
            row.get("baseline_signal_count"),
        ),
        "best_expectancy_r": scored.get("best_expectancy_r"),
        "expectancy_delta": _numeric_delta(
            scored.get("best_expectancy_r"),
            row.get("baseline_best_expectancy_r"),
        ),
        "best_trades": scored.get("best_trades"),
        "trade_delta": _numeric_delta(
            scored.get("best_trades"),
            row.get("baseline_best_trades"),
        ),
        "best_win_rate": scored.get("best_win_rate"),
        "best_profit_factor": scored.get("best_profit_factor"),
        "profit_factor_delta": _numeric_delta(
            scored.get("best_profit_factor"),
            row.get("baseline_best_profit_factor"),
        ),
        "timing_bucket": timing_bucket,
        "error": error,
    }


def _timing_results_fieldnames() -> list[str]:
    return [
        "timing_probe_id",
        "timing_rank",
        "base_probe_id",
        "base_queue_rank",
        "variant_side",
        "variant_indicator_id",
        "variant_lookback_bars",
        "baseline_lookback_bars",
        "anchor_baseline_lookback_bars",
        "trigger_baseline_lookback_bars",
        "anchor_variant_lookback_bars",
        "trigger_variant_lookback_bars",
        "anchor_type",
        "anchor_id",
        "trigger_id",
        "probe_timeframe",
        "pair_prior_score",
        "pair_prior_bucket",
        "baseline_status",
        "baseline_composite_score",
        "baseline_signal_count",
        "baseline_best_expectancy_r",
        "baseline_best_trades",
        "baseline_best_profit_factor",
        "profile_id",
        "output_dir",
        "status",
        "primary_score",
        "composite_score",
        "score_delta",
        "score_basis",
        "signal_count",
        "signal_count_delta",
        "best_expectancy_r",
        "expectancy_delta",
        "best_trades",
        "trade_delta",
        "best_win_rate",
        "best_profit_factor",
        "profit_factor_delta",
        "timing_bucket",
        "error",
    ]


def run_anchor_pair_probes(
    config: AppConfig,
    *,
    atlas_dir: Path | None = None,
    probe_ids: list[str] | None = None,
    limit: int | None = DEFAULT_PROBE_RUN_LIMIT,
    force: bool = False,
    keep_profiles: bool = False,
    timeout_seconds: int | None = DEFAULT_PROBE_TIMEOUT_SECONDS,
    progress_callback: Any | None = None,
) -> AnchorPairProbeRunResult:
    source_dir = (
        atlas_dir.expanduser().resolve()
        if atlas_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_DIRNAME
    )
    atlas_path = source_dir / "anchor-pair-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair atlas at {atlas_path}. Run `uv run build-anchor-pair-atlas` first."
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

    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()

    results: list[dict[str, Any]] = []
    completed = 0
    for row in selected_rows:
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
        if sensitivity_path.exists() and not force:
            try:
                compare_payload = cli.score_artifact(output_dir)
                snapshot = load_sensitivity_snapshot(output_dir)
                score = build_attempt_score(compare_payload, snapshot)
                results.append(
                    _result_row_from_score(
                        row,
                        profile_id=None,
                        output_dir=output_dir,
                        status="skipped_existing",
                        score_payload=score,
                        sensitivity_snapshot=snapshot,
                    )
                )
            except Exception as exc:
                results.append(
                    _result_row_from_score(
                        row,
                        profile_id=None,
                        output_dir=output_dir,
                        status="skipped_existing_unscored",
                        error=str(exc)[:500],
                    )
                )
            completed += 1
            if progress_callback:
                progress_callback({"completed": completed, "total": len(selected_rows), "probe_id": probe_id, "status": results[-1]["status"]})
            continue

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
            sensitivity_args = _replace_profile_id_arg(sensitivity_args, profile_id)
            cli.run(
                sensitivity_args,
                timeout_seconds=timeout_seconds,
            )
            compare_payload = cli.score_artifact(output_dir)
            snapshot = load_sensitivity_snapshot(output_dir)
            score = build_attempt_score(compare_payload, snapshot)
            results.append(
                _result_row_from_score(
                    row,
                    profile_id=profile_id,
                    output_dir=output_dir,
                    status="ok",
                    score_payload=score,
                    sensitivity_snapshot=snapshot,
                )
            )
        except Exception as exc:
            results.append(
                _result_row_from_score(
                    row,
                    profile_id=profile_id,
                    output_dir=output_dir,
                    status="failed",
                    error=str(exc)[:500],
                )
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
            completed += 1
            if progress_callback:
                progress_callback(
                    {
                        "completed": completed,
                        "total": len(selected_rows),
                        "probe_id": probe_id,
                        "status": results[-1]["status"],
                    }
                )

    results_csv_path = source_dir / "anchor-pair-probe-results.csv"
    summary_path = source_dir / "anchor-pair-probe-summary.json"
    _write_csv(results_csv_path, results, _probe_results_fieldnames())
    status_counts: dict[str, int] = {}
    for row in results:
        status = _clean_token(row.get("status")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
    scored = [
        row
        for row in results
        if row.get("composite_score") is not None
    ]
    scored.sort(key=lambda row: -_float_value(row.get("composite_score")))
    summary = {
        "schema_version": "anchor_pair_probe_results_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "anchor_pair_atlas_path": str(atlas_path),
        },
        "selection": {
            "requested_probe_ids": probe_ids or [],
            "limit": limit,
            "force": force,
            "keep_profiles": keep_profiles,
            "timeout_seconds": timeout_seconds,
        },
        "result_counts": {
            "selected": len(selected_rows),
            "completed": len(results),
            "status_counts": dict(sorted(status_counts.items())),
            "scored": len(scored),
        },
        "top_scored": scored[:10],
    }
    _write_json(summary_path, summary)
    return AnchorPairProbeRunResult(
        results_csv_path=results_csv_path,
        summary_path=summary_path,
        summary=summary,
    )


def run_anchor_pair_timing_probes(
    config: AppConfig,
    *,
    atlas_dir: Path | None = None,
    timing_probe_ids: list[str] | None = None,
    limit: int | None = None,
    force: bool = False,
    keep_profiles: bool = False,
    timeout_seconds: int | None = DEFAULT_PROBE_TIMEOUT_SECONDS,
    progress_callback: Any | None = None,
) -> AnchorPairTimingProbeRunResult:
    source_dir = (
        atlas_dir.expanduser().resolve()
        if atlas_dir is not None
        else config.derived_root / DEFAULT_ANCHOR_PAIR_TIMING_DIRNAME
    )
    atlas_path = source_dir / "anchor-pair-timing-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing anchor pair timing atlas at {atlas_path}. Run `uv run build-anchor-pair-timing-atlas` first."
        )
    payload = _as_dict(_load_json(atlas_path))
    queue_rows = [
        row for row in _as_list(payload.get("timing_queue_rows")) if isinstance(row, dict)
    ]
    manifest = _as_dict(payload.get("run_manifest"))
    manifest_probes = {
        _clean_token(row.get("timing_probe_id") or row.get("probe_id")): row
        for row in _as_list(manifest.get("probes"))
        if isinstance(row, dict) and _clean_token(row.get("timing_probe_id") or row.get("probe_id"))
    }
    selected_rows = _select_probe_rows(
        queue_rows,
        probe_ids=timing_probe_ids,
        limit=limit,
    )

    cli = FuzzfolioCli(config.fuzzfolio)
    cli.ensure_login()

    results: list[dict[str, Any]] = []
    completed = 0
    for row in selected_rows:
        timing_probe_id = _clean_token(row.get("timing_probe_id") or row.get("probe_id"))
        manifest_probe = _as_dict(manifest_probes.get(timing_probe_id))
        profile_path = Path(_clean_token(manifest_probe.get("profile_path") or row.get("profile_path")))
        output_dir = Path(_clean_token(manifest_probe.get("output_dir") or row.get("result_dir")))
        if not profile_path.is_absolute():
            profile_path = (config.repo_root / profile_path).resolve()
        if not output_dir.is_absolute():
            output_dir = (config.repo_root / output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        sensitivity_path = output_dir / "sensitivity-response.json"
        if sensitivity_path.exists() and not force:
            try:
                compare_payload = cli.score_artifact(output_dir)
                snapshot = load_sensitivity_snapshot(output_dir)
                score = build_attempt_score(compare_payload, snapshot)
                results.append(
                    _timing_result_row_from_score(
                        row,
                        profile_id=None,
                        output_dir=output_dir,
                        status="skipped_existing",
                        score_payload=score,
                        sensitivity_snapshot=snapshot,
                    )
                )
            except Exception as exc:
                results.append(
                    _timing_result_row_from_score(
                        row,
                        profile_id=None,
                        output_dir=output_dir,
                        status="skipped_existing_unscored",
                        error=str(exc)[:500],
                    )
                )
            completed += 1
            if progress_callback:
                progress_callback(
                    {
                        "completed": completed,
                        "total": len(selected_rows),
                        "probe_id": timing_probe_id,
                        "status": results[-1]["status"],
                    }
                )
            continue

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
            sensitivity_args = _replace_profile_id_arg(sensitivity_args, profile_id)
            cli.run(
                sensitivity_args,
                timeout_seconds=timeout_seconds,
            )
            compare_payload = cli.score_artifact(output_dir)
            snapshot = load_sensitivity_snapshot(output_dir)
            score = build_attempt_score(compare_payload, snapshot)
            results.append(
                _timing_result_row_from_score(
                    row,
                    profile_id=profile_id,
                    output_dir=output_dir,
                    status="ok",
                    score_payload=score,
                    sensitivity_snapshot=snapshot,
                )
            )
        except Exception as exc:
            results.append(
                _timing_result_row_from_score(
                    row,
                    profile_id=profile_id,
                    output_dir=output_dir,
                    status="failed",
                    error=str(exc)[:500],
                )
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
            completed += 1
            if progress_callback:
                progress_callback(
                    {
                        "completed": completed,
                        "total": len(selected_rows),
                        "probe_id": timing_probe_id,
                        "status": results[-1]["status"],
                    }
                )

    results_csv_path = source_dir / "anchor-pair-timing-results.csv"
    summary_path = source_dir / "anchor-pair-timing-summary.json"
    _write_csv(results_csv_path, results, _timing_results_fieldnames())
    status_counts: dict[str, int] = {}
    bucket_counts: dict[str, int] = {}
    lookback_counts: dict[str, int] = {}
    lookback_delta_totals: dict[str, float] = {}
    lookback_delta_counts: dict[str, int] = {}
    for row in results:
        status = _clean_token(row.get("status")) or "unknown"
        bucket = _clean_token(row.get("timing_bucket")) or "unknown"
        lookback = _clean_token(row.get("variant_lookback_bars")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        lookback_counts[lookback] = lookback_counts.get(lookback, 0) + 1
        delta = _numeric_or_none(row.get("score_delta"))
        if delta is not None:
            lookback_delta_totals[lookback] = lookback_delta_totals.get(lookback, 0.0) + delta
            lookback_delta_counts[lookback] = lookback_delta_counts.get(lookback, 0) + 1

    scored = [
        row
        for row in results
        if row.get("composite_score") is not None
    ]
    scored.sort(key=lambda row: -_float_value(row.get("composite_score")))
    positive_delta = [
        row
        for row in scored
        if _numeric_or_none(row.get("score_delta")) is not None
        and _float_value(row.get("score_delta")) > 0
    ]
    positive_delta.sort(key=lambda row: -_float_value(row.get("score_delta")))
    material_improved = [
        row
        for row in scored
        if row.get("timing_bucket") == "improved"
    ]
    by_base_best: dict[str, dict[str, Any]] = {}
    for row in scored:
        base_probe_id = _clean_token(row.get("base_probe_id"))
        if not base_probe_id:
            continue
        existing = by_base_best.get(base_probe_id)
        if existing is None or _float_value(row.get("composite_score")) > _float_value(
            existing.get("composite_score")
        ):
            by_base_best[base_probe_id] = row
    rescued = [
        row
        for row in scored
        if row.get("timing_bucket") == "rescued_positive"
    ]
    lookback_summary = {
        lookback: {
            "count": lookback_counts.get(lookback, 0),
            "avg_score_delta": (
                lookback_delta_totals[lookback] / lookback_delta_counts[lookback]
                if lookback_delta_counts.get(lookback)
                else None
            ),
        }
        for lookback in sorted(lookback_counts, key=lambda value: _int_value(value, 1_000))
    }
    summary = {
        "schema_version": "anchor_pair_timing_results_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "anchor_pair_timing_atlas_path": str(atlas_path),
        },
        "selection": {
            "requested_timing_probe_ids": timing_probe_ids or [],
            "limit": limit,
            "force": force,
            "keep_profiles": keep_profiles,
            "timeout_seconds": timeout_seconds,
        },
        "result_counts": {
            "selected": len(selected_rows),
            "completed": len(results),
            "status_counts": dict(sorted(status_counts.items())),
            "timing_bucket_counts": dict(sorted(bucket_counts.items())),
            "variant_lookback_counts": dict(sorted(lookback_counts.items())),
            "scored": len(scored),
            "positive_delta": len(positive_delta),
            "material_improved": len(material_improved),
            "rescued_positive": len(rescued),
            "base_pairs_with_timing_winner": len(
                [
                    row
                    for row in by_base_best.values()
                    if _float_value(row.get("score_delta")) > 0
                ]
            ),
        },
        "lookback_summary": lookback_summary,
        "top_scored": scored[:10],
        "top_improvements": positive_delta[:10],
        "rescued_positive": rescued[:10],
        "best_variant_by_base_probe": sorted(
            by_base_best.values(),
            key=lambda row: _int_value(row.get("base_queue_rank"), 1_000_000),
        )[:20],
    }
    _write_json(summary_path, summary)
    return AnchorPairTimingProbeRunResult(
        results_csv_path=results_csv_path,
        summary_path=summary_path,
        summary=summary,
    )
