from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .anchor_pair_atlas import (
    DEFAULT_EXECUTION_COST_MODE,
    DEFAULT_QUALITY_SCORE_PRESET,
    _anchor_timeframe,
    _as_dict,
    _as_list,
    _catalog_by_id,
    _clean_token,
    _clean_upper,
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
)
from .config import AppConfig
from .discovery_cluster_atlas import DEFAULT_DISCOVERY_CLUSTER_DIRNAME
from .discovery_pair_atlas import (
    DEFAULT_JOB_TIMEOUT_SECONDS as DISCOVERY_PAIR_DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_PROBE_WORKERS,
    _select_probe_rows,
    _with_job_timeout_args,
)
from .fuzzfolio import CliError, FuzzfolioCli
from .indicator_atlas import build_indicator_atlas, load_indicator_catalog
from .scoring import build_attempt_score, load_sensitivity_snapshot
from .signal_atlas import DEFAULT_INSTRUMENTS


SCHEMA_VERSION = "discovery_recipe_validation_atlas_v1"
RESULTS_SCHEMA_VERSION = "discovery_recipe_validation_results_v1"
DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME = "discovery-recipe-validation-atlas"
DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME = "discovery-recipe-scrutiny-atlas"
DEFAULT_INCLUDED_CONFIDENCE = (
    "high_candidate",
    "promising_candidate",
)
DEFAULT_LOOKBACK_MONTHS = 12
DEFAULT_SCRUTINY_LOOKBACK_MONTHS = 36
DEFAULT_SCRUTINY_BUCKETS = ("retained_strong", "retained")
DEFAULT_MAX_RECIPES = 8
DEFAULT_MAX_PAIRS_PER_RECIPE = 8
DEFAULT_FIRST_MEMBER_LIMIT = 6
DEFAULT_SECOND_MEMBER_LIMIT = 6
DEFAULT_JOB_TIMEOUT_SECONDS = max(7200, DISCOVERY_PAIR_DEFAULT_JOB_TIMEOUT_SECONDS)
DEFAULT_PROBE_TIMEOUT_SECONDS = (DEFAULT_JOB_TIMEOUT_SECONDS * 2) + 300


@dataclass(frozen=True)
class DiscoveryRecipeValidationAtlasBuildResult:
    atlas_path: Path
    queue_csv_path: Path
    manifest_path: Path
    run_script_path: Path
    profile_dir: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "discovery_recipe_validation_atlas_json": str(self.atlas_path),
            "discovery_recipe_validation_queue_csv": str(self.queue_csv_path),
            "discovery_recipe_validation_run_manifest_json": str(self.manifest_path),
            "discovery_recipe_validation_run_script": str(self.run_script_path),
            "discovery_recipe_validation_profile_dir": str(self.profile_dir),
            "discovery_recipe_validation_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


@dataclass(frozen=True)
class DiscoveryRecipeValidationProbeRunResult:
    results_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "discovery_recipe_validation_results_csv": str(self.results_csv_path),
            "discovery_recipe_validation_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _safe_slug(value: Any, *, max_length: int = 18) -> str:
    text = "".join(ch if ch.isalnum() else "-" for ch in _clean_token(value).lower())
    while "--" in text:
        text = text.replace("--", "-")
    text = text.strip("-") or "item"
    return text[:max_length].strip("-") or "item"


def _recipe_slug(recipe_id: Any) -> str:
    text = _clean_token(recipe_id).lower()
    if text.startswith("discovered_recipe_"):
        suffix = text.removeprefix("discovered_recipe_").strip("_-")
        if suffix:
            return f"r{suffix}"
    return _safe_slug(text, max_length=8)


def _split_csv_tokens(value: Any) -> list[str]:
    return [token.strip() for token in _clean_token(value).split(",") if token.strip()]


def _load_discovered_recipes(cluster_atlas_dir: Path) -> list[dict[str, Any]]:
    recipes_path = cluster_atlas_dir / "discovered-recipes.json"
    atlas_path = cluster_atlas_dir / "discovery-cluster-atlas.json"
    if recipes_path.exists():
        payload = _as_dict(_load_json(recipes_path))
        return [row for row in _as_list(payload.get("recipes")) if isinstance(row, dict)]
    if atlas_path.exists():
        payload = _as_dict(_load_json(atlas_path))
        return [row for row in _as_list(payload.get("discovered_recipes")) if isinstance(row, dict)]
    raise FileNotFoundError(
        f"Missing discovered recipes under {cluster_atlas_dir}. "
        "Run `uv run build-discovery-cluster-atlas` first."
    )


def _cluster_slot(recipe: dict[str, Any], key: str) -> dict[str, Any]:
    return _as_dict(_as_dict(recipe.get("slots")).get(key))


def _evidence_lookup(recipe: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for example in _as_list(recipe.get("evidence_examples")):
        if not isinstance(example, dict):
            continue
        key = (
            _clean_upper(example.get("first_indicator_id")),
            _clean_upper(example.get("second_indicator_id")),
            _clean_upper(example.get("probe_timeframe")),
        )
        if not all(key):
            continue
        existing = lookup.get(key)
        if existing is None or _float_value(example.get("composite_score")) > _float_value(
            existing.get("composite_score")
        ):
            lookup[key] = example
    return lookup


def _candidate_score(
    *,
    recipe: dict[str, Any],
    first_rank: int,
    second_rank: int,
    timeframe_rank: int,
    evidence: dict[str, Any] | None,
) -> float:
    recipe_score = _float_value(recipe.get("compatibility_score"))
    evidence_score = _float_value(_as_dict(evidence).get("composite_score"))
    rank_penalty = (first_rank * 1.75) + (second_rank * 1.75) + (timeframe_rank * 1.25)
    score = recipe_score * 0.55 + evidence_score * 0.35 - rank_penalty
    if evidence:
        score += 12.0
    confidence = _clean_token(recipe.get("confidence"))
    if confidence == "high_candidate":
        score += 6.0
    elif confidence == "promising_candidate":
        score += 3.0
    return round(score, 4)


def build_validation_queue_rows(
    recipes: list[dict[str, Any]],
    *,
    included_confidence: list[str] | tuple[str, ...] = DEFAULT_INCLUDED_CONFIDENCE,
    max_recipes: int = DEFAULT_MAX_RECIPES,
    max_pairs_per_recipe: int = DEFAULT_MAX_PAIRS_PER_RECIPE,
    first_member_limit: int = DEFAULT_FIRST_MEMBER_LIMIT,
    second_member_limit: int = DEFAULT_SECOND_MEMBER_LIMIT,
    timeframes: list[str] | None = None,
    instruments: list[str] | None = None,
) -> list[dict[str, Any]]:
    confidence_set = {_clean_token(value) for value in included_confidence if _clean_token(value)}
    recipe_candidates = [
        recipe
        for recipe in recipes
        if _clean_token(recipe.get("confidence")) in confidence_set
    ]
    recipe_candidates.sort(
        key=lambda recipe: (
            -_float_value(recipe.get("compatibility_score")),
            -_float_value(recipe.get("best_score")),
            str(recipe.get("recipe_id") or ""),
        )
    )
    selected_recipes = recipe_candidates[: max(0, int(max_recipes))]
    instrument_panel = _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS)
    explicit_timeframes = _normalize_tokens(timeframes)
    queue: list[dict[str, Any]] = []
    for recipe in selected_recipes:
        recipe_id = _clean_token(recipe.get("recipe_id"))
        first_slot = _cluster_slot(recipe, "context_or_setup_cluster")
        second_slot = _cluster_slot(recipe, "trigger_or_response_cluster")
        first_members = _normalize_tokens(
            [
                str(value)
                for value in _as_list(first_slot.get("recommended_indicators"))[
                    : max(1, int(first_member_limit))
                ]
            ]
        )
        second_members = _normalize_tokens(
            [
                str(value)
                for value in _as_list(second_slot.get("recommended_indicators"))[
                    : max(1, int(second_member_limit))
                ]
            ]
        )
        preferred_timeframes = explicit_timeframes or _normalize_tokens(
            _split_csv_tokens(recipe.get("top_timeframes"))
        ) or ["M5", "M15"]
        evidence_by_key = _evidence_lookup(recipe)
        candidate_rows: list[dict[str, Any]] = []
        for first_rank, first_id in enumerate(first_members):
            for second_rank, second_id in enumerate(second_members):
                if first_id == second_id:
                    continue
                for timeframe_rank, timeframe in enumerate(preferred_timeframes):
                    key = (first_id, second_id, timeframe)
                    evidence = evidence_by_key.get(key)
                    priority_score = _candidate_score(
                        recipe=recipe,
                        first_rank=first_rank,
                        second_rank=second_rank,
                        timeframe_rank=timeframe_rank,
                        evidence=evidence,
                    )
                    candidate_rows.append(
                        {
                            "recipe_id": recipe_id,
                            "recipe_confidence": recipe.get("confidence"),
                            "recipe_name": recipe.get("name"),
                            "recipe_compatibility_score": recipe.get("compatibility_score"),
                            "recipe_best_score": recipe.get("best_score"),
                            "recipe_positive_pair_count": recipe.get("positive_pair_count"),
                            "recipe_strong_pair_count": recipe.get("strong_pair_count"),
                            "first_cluster_id": first_slot.get("cluster_id"),
                            "first_cluster_label": first_slot.get("label"),
                            "second_cluster_id": second_slot.get("cluster_id"),
                            "second_cluster_label": second_slot.get("label"),
                            "first_indicator_id": first_id,
                            "second_indicator_id": second_id,
                            "anchor_id": first_id,
                            "trigger_id": second_id,
                            "anchor_type": "discovered_recipe",
                            "probe_timeframe": timeframe,
                            "instruments": ",".join(instrument_panel),
                            "validation_priority_score": priority_score,
                            "discovery_evidence_score": _float_value(
                                _as_dict(evidence).get("composite_score")
                            ),
                            "discovery_evidence_probe_id": _clean_token(
                                _as_dict(evidence).get("probe_id")
                            ),
                            "discovery_lane": _clean_token(
                                _as_dict(evidence).get("discovery_lane")
                            )
                            or "cluster_expansion",
                            "pair_prior_score": priority_score,
                            "pair_prior_bucket": "recipe_validation",
                            "local_discovery_score": priority_score,
                            "local_score_bucket": "validation",
                            "known_pair_status": "discovered_recipe_candidate",
                        }
                    )
        candidate_rows.sort(
            key=lambda row: (
                -_float_value(row.get("validation_priority_score")),
                str(row.get("first_indicator_id") or ""),
                str(row.get("second_indicator_id") or ""),
                str(row.get("probe_timeframe") or ""),
            )
        )
        queue.extend(candidate_rows[: max(1, int(max_pairs_per_recipe))])
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in queue:
        key = (
            _clean_token(row.get("recipe_id")),
            _clean_upper(row.get("first_indicator_id")),
            _clean_upper(row.get("second_indicator_id")),
            _clean_upper(row.get("probe_timeframe")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    deduped.sort(
        key=lambda row: (
            -_float_value(row.get("validation_priority_score")),
            str(row.get("recipe_id") or ""),
            str(row.get("first_indicator_id") or ""),
            str(row.get("second_indicator_id") or ""),
        )
    )
    for index, row in enumerate(deduped, start=1):
        row["queue_rank"] = index
        row["probe_id"] = (
            f"drv-{index:04d}-"
            f"{_recipe_slug(row.get('recipe_id'))}-"
            f"{_safe_slug(row.get('first_indicator_id'))}-"
            f"{_safe_slug(row.get('second_indicator_id'))}-"
            f"{_safe_slug(row.get('probe_timeframe'), max_length=4)}"
        )
    return deduped


def build_retained_scrutiny_queue_rows(
    validation_result_rows: list[dict[str, Any]],
    *,
    included_buckets: list[str] | tuple[str, ...] = DEFAULT_SCRUTINY_BUCKETS,
    max_rows: int | None = None,
    timeframes: list[str] | None = None,
    instruments: list[str] | None = None,
) -> list[dict[str, Any]]:
    bucket_set = {_clean_token(value) for value in included_buckets if _clean_token(value)}
    timeframe_set = set(_normalize_tokens(timeframes))
    instrument_panel = _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS)
    candidates: list[dict[str, Any]] = []
    for source_row in validation_result_rows:
        status = _clean_token(source_row.get("status"))
        bucket = _clean_token(source_row.get("retention_bucket"))
        validation_score = _float_value(
            source_row.get("primary_score"),
            _float_value(source_row.get("composite_score")),
        )
        timeframe = _clean_upper(source_row.get("probe_timeframe"))
        if status not in {"", "ok", "skipped_existing"}:
            continue
        if bucket not in bucket_set:
            continue
        if validation_score <= 0.0:
            continue
        if timeframe_set and timeframe not in timeframe_set:
            continue
        priority_score = round(
            (validation_score * 0.72)
            + (_float_value(source_row.get("discovery_evidence_score")) * 0.16)
            + (_float_value(source_row.get("validation_priority_score")) * 0.06),
            4,
        )
        first_id = _clean_upper(source_row.get("first_indicator_id"))
        second_id = _clean_upper(source_row.get("second_indicator_id"))
        recipe_id = _clean_token(source_row.get("recipe_id"))
        if not first_id or not second_id or not recipe_id:
            continue
        candidates.append(
            {
                "recipe_id": recipe_id,
                "recipe_confidence": source_row.get("recipe_confidence"),
                "recipe_name": source_row.get("recipe_id"),
                "recipe_compatibility_score": source_row.get("validation_priority_score"),
                "recipe_best_score": validation_score,
                "recipe_positive_pair_count": 1,
                "recipe_strong_pair_count": 1 if validation_score >= 70.0 else 0,
                "first_cluster_id": source_row.get("first_cluster_id"),
                "first_cluster_label": source_row.get("first_cluster_id"),
                "second_cluster_id": source_row.get("second_cluster_id"),
                "second_cluster_label": source_row.get("second_cluster_id"),
                "first_indicator_id": first_id,
                "second_indicator_id": second_id,
                "anchor_id": first_id,
                "trigger_id": second_id,
                "anchor_type": "discovered_recipe_scrutiny",
                "probe_timeframe": timeframe,
                "instruments": ",".join(instrument_panel),
                "validation_priority_score": priority_score,
                "discovery_evidence_score": source_row.get("discovery_evidence_score"),
                "discovery_evidence_probe_id": source_row.get("discovery_evidence_probe_id"),
                "discovery_lane": source_row.get("discovery_lane"),
                "pair_prior_score": priority_score,
                "pair_prior_bucket": "recipe_scrutiny",
                "local_discovery_score": priority_score,
                "local_score_bucket": "scrutiny",
                "known_pair_status": "retained_discovered_recipe_36m_candidate",
                "source_validation_probe_id": source_row.get("probe_id"),
                "source_retention_bucket": bucket,
                "source_validation_score": validation_score,
                "source_retention_ratio": source_row.get("retention_ratio"),
            }
        )
    candidates.sort(
        key=lambda row: (
            -_float_value(row.get("validation_priority_score")),
            str(row.get("recipe_id") or ""),
            str(row.get("first_indicator_id") or ""),
            str(row.get("second_indicator_id") or ""),
        )
    )
    if max_rows is not None:
        candidates = candidates[: max(0, int(max_rows))]
    for index, row in enumerate(candidates, start=1):
        row["queue_rank"] = index
        row["probe_id"] = (
            f"drs-{index:04d}-"
            f"{_recipe_slug(row.get('recipe_id'))}-"
            f"{_safe_slug(row.get('first_indicator_id'))}-"
            f"{_safe_slug(row.get('second_indicator_id'))}-"
            f"{_safe_slug(row.get('probe_timeframe'), max_length=4)}"
        )
    return candidates


def _queue_fieldnames() -> list[str]:
    return [
        "queue_rank",
        "probe_id",
        "recipe_id",
        "recipe_confidence",
        "recipe_name",
        "recipe_compatibility_score",
        "recipe_best_score",
        "recipe_positive_pair_count",
        "recipe_strong_pair_count",
        "first_cluster_id",
        "first_cluster_label",
        "second_cluster_id",
        "second_cluster_label",
        "first_indicator_id",
        "second_indicator_id",
        "probe_timeframe",
        "instruments",
        "validation_priority_score",
        "discovery_evidence_score",
        "discovery_evidence_probe_id",
        "discovery_lane",
        "anchor_type",
        "anchor_id",
        "trigger_id",
        "pair_prior_score",
        "pair_prior_bucket",
        "anchor_timeframe",
        "local_discovery_score",
        "local_score_bucket",
        "known_pair_status",
        "source_validation_probe_id",
        "source_retention_bucket",
        "source_validation_score",
        "source_retention_ratio",
        "profile_path",
        "result_dir",
    ]


def _result_fieldnames() -> list[str]:
    return [
        "probe_id",
        "queue_rank",
        "recipe_id",
        "recipe_confidence",
        "first_cluster_id",
        "second_cluster_id",
        "first_indicator_id",
        "second_indicator_id",
        "probe_timeframe",
        "lookback_months",
        "validation_priority_score",
        "discovery_evidence_score",
        "discovery_evidence_probe_id",
        "discovery_lane",
        "profile_id",
        "output_dir",
        "status",
        "primary_score",
        "composite_score",
        "retention_ratio",
        "retention_bucket",
        "score_basis",
        "signal_count",
        "best_expectancy_r",
        "best_trades",
        "best_win_rate",
        "best_profit_factor",
        "source_validation_probe_id",
        "source_retention_bucket",
        "source_validation_score",
        "source_retention_ratio",
        "error",
    ]


def build_discovery_recipe_validation_atlas(
    config: AppConfig,
    *,
    cluster_atlas_dir: Path | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    refresh_static_atlas: bool = False,
    included_confidence: list[str] | None = None,
    instruments: list[str] | None = None,
    timeframes: list[str] | None = None,
    max_recipes: int = DEFAULT_MAX_RECIPES,
    max_pairs_per_recipe: int = DEFAULT_MAX_PAIRS_PER_RECIPE,
    first_member_limit: int = DEFAULT_FIRST_MEMBER_LIMIT,
    second_member_limit: int = DEFAULT_SECOND_MEMBER_LIMIT,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    job_timeout_seconds: int | None = DEFAULT_JOB_TIMEOUT_SECONDS,
    emit_profile_docs: bool = True,
    quality_score_preset: str = DEFAULT_QUALITY_SCORE_PRESET,
    execution_cost_mode: str = DEFAULT_EXECUTION_COST_MODE,
) -> DiscoveryRecipeValidationAtlasBuildResult:
    source_dir = (
        cluster_atlas_dir.expanduser().resolve()
        if cluster_atlas_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_CLUSTER_DIRNAME
    )
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = target_dir / "profiles"
    result_root = target_dir / "probe-results"
    if emit_profile_docs:
        profile_dir.mkdir(parents=True, exist_ok=True)
    result_root.mkdir(parents=True, exist_ok=True)

    recipes = _load_discovered_recipes(source_dir)
    included = included_confidence or list(DEFAULT_INCLUDED_CONFIDENCE)
    queue_rows = build_validation_queue_rows(
        recipes,
        included_confidence=included,
        max_recipes=max_recipes,
        max_pairs_per_recipe=max_pairs_per_recipe,
        first_member_limit=first_member_limit,
        second_member_limit=second_member_limit,
        timeframes=timeframes,
        instruments=instruments,
    )
    lookback_months = max(1, int(lookback_months or DEFAULT_LOOKBACK_MONTHS))
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    if refresh_static_atlas:
        build_indicator_atlas(
            config,
            workspace_root=workspace_root,
            catalog_path=catalog_path,
            out_dir=config.derived_root / "indicator-atlas",
        )
    catalog_by_id = _catalog_by_id(catalog_payload)

    probes: list[dict[str, Any]] = []
    exe, base_args = _fuzzfolio_base_args(config)
    filtered_rows: list[dict[str, Any]] = []
    for row in queue_rows:
        first_id = _clean_upper(row.get("first_indicator_id"))
        second_id = _clean_upper(row.get("second_indicator_id"))
        if first_id not in catalog_by_id or second_id not in catalog_by_id:
            continue
        probe_id = _clean_token(row.get("probe_id"))
        probe_timeframe = _clean_upper(row.get("probe_timeframe"))
        anchor_timeframe = _anchor_timeframe(
            catalog_by_id[first_id],
            probe_timeframe=probe_timeframe,
        )
        profile_path = profile_dir / f"{probe_id}.json"
        result_dir = result_root / probe_id
        row["anchor_timeframe"] = anchor_timeframe
        row["profile_path"] = str(profile_path)
        row["result_dir"] = str(result_dir)
        if emit_profile_docs:
            profile_doc = build_pair_profile_document(
                catalog_by_id=catalog_by_id,
                anchor_id=first_id,
                trigger_id=second_id,
                anchor_type="discovered_recipe_validation",
                probe_timeframe=probe_timeframe,
                anchor_timeframe=anchor_timeframe,
                instruments=_split_csv_tokens(row.get("instruments")),
                probe_id=probe_id,
            )
            profile = _as_dict(profile_doc.get("profile"))
            profile["name"] = (
                f"Discovery Recipe Validation {row.get('recipe_id')} "
                f"{first_id}+{second_id} {probe_timeframe}"
            )
            profile["description"] = (
                "Temporary AutoResearch 12-month validation profile for an "
                "empirically discovered cluster recipe. This validates retention "
                "before discovered recipes influence Play Hand."
            )
            _write_json(profile_path, profile_doc)
        sensitivity_args = _sensitivity_args_for_row(
            row,
            lookback_months=lookback_months,
            quality_score_preset=quality_score_preset,
            execution_cost_mode=execution_cost_mode,
            result_dir=result_dir,
        )
        sensitivity_args = _with_job_timeout_args(sensitivity_args, job_timeout_seconds)
        filtered_rows.append(row)
        probes.append(
            {
                "probe_id": probe_id,
                "queue_rank": row.get("queue_rank"),
                "recipe_id": row.get("recipe_id"),
                "recipe_confidence": row.get("recipe_confidence"),
                "first_indicator_id": first_id,
                "second_indicator_id": second_id,
                "probe_timeframe": probe_timeframe,
                "lookback_months": lookback_months,
                "anchor_timeframe": anchor_timeframe,
                "profile_path": str(profile_path),
                "output_dir": str(result_dir),
                "create_profile_args": ["profiles", "create", "--file", str(profile_path), "--pretty"],
                "sensitivity_basket_args": sensitivity_args,
                "validation_priority_score": row.get("validation_priority_score"),
            }
        )

    confidence_counts: dict[str, int] = {}
    for row in filtered_rows:
        key = _clean_token(row.get("recipe_confidence")) or "unknown"
        confidence_counts[key] = confidence_counts.get(key, 0) + 1
    recipe_counts: dict[str, int] = {}
    for row in filtered_rows:
        key = _clean_token(row.get("recipe_id")) or "unknown"
        recipe_counts[key] = recipe_counts.get(key, 0) + 1
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "cluster_atlas_dir": str(source_dir),
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
        },
        "selection": {
            "included_confidence": included,
            "instruments": _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS),
            "timeframes": _normalize_tokens(timeframes),
            "max_recipes": max_recipes,
            "max_pairs_per_recipe": max_pairs_per_recipe,
            "first_member_limit": first_member_limit,
            "second_member_limit": second_member_limit,
            "lookback_months": lookback_months,
            "job_timeout_seconds": job_timeout_seconds,
            "quality_score_preset": quality_score_preset,
            "execution_cost_mode": execution_cost_mode,
        },
        "result_counts": {
            "available_recipes": len(recipes),
            "queue_rows": len(filtered_rows),
            "profile_docs": len(probes) if emit_profile_docs else 0,
            "queued_recipe_counts": dict(sorted(recipe_counts.items())),
            "queued_confidence_counts": dict(sorted(confidence_counts.items())),
        },
        "top_queue": filtered_rows[:15],
    }
    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "queue_rows": filtered_rows,
        "run_manifest": {
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    }

    atlas_path = target_dir / "discovery-recipe-validation-atlas.json"
    queue_csv_path = target_dir / "discovery-recipe-validation-queue.csv"
    manifest_path = target_dir / "discovery-recipe-validation-run-manifest.json"
    run_script_path = target_dir / "run-discovery-recipe-validation-probes.ps1"
    summary_path = target_dir / "discovery-recipe-validation-summary.json"
    _write_json(atlas_path, atlas_payload)
    _write_csv(queue_csv_path, filtered_rows, _queue_fieldnames())
    _write_json(
        manifest_path,
        {
            "schema_version": "discovery_recipe_validation_run_manifest_v1",
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
        generated_by="uv run build-discovery-recipe-validation-atlas",
        description="Runs queued 12-month discovered recipe validation sensitivity-basket probes.",
    )
    _write_json(summary_path, summary)
    return DiscoveryRecipeValidationAtlasBuildResult(
        atlas_path=atlas_path,
        queue_csv_path=queue_csv_path,
        manifest_path=manifest_path,
        run_script_path=run_script_path,
        profile_dir=profile_dir,
        summary_path=summary_path,
        summary=summary,
    )


def build_discovery_recipe_scrutiny_atlas(
    config: AppConfig,
    *,
    validation_atlas_dir: Path | None = None,
    out_dir: Path | None = None,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    refresh_static_atlas: bool = False,
    included_buckets: list[str] | None = None,
    instruments: list[str] | None = None,
    timeframes: list[str] | None = None,
    max_rows: int | None = None,
    lookback_months: int = DEFAULT_SCRUTINY_LOOKBACK_MONTHS,
    job_timeout_seconds: int | None = DEFAULT_JOB_TIMEOUT_SECONDS,
    emit_profile_docs: bool = True,
    quality_score_preset: str = DEFAULT_QUALITY_SCORE_PRESET,
    execution_cost_mode: str = DEFAULT_EXECUTION_COST_MODE,
) -> DiscoveryRecipeValidationAtlasBuildResult:
    source_dir = (
        validation_atlas_dir.expanduser().resolve()
        if validation_atlas_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME
    )
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_RECIPE_SCRUTINY_DIRNAME
    )
    results_path = source_dir / "discovery-recipe-validation-results.csv"
    if not results_path.exists():
        raise FileNotFoundError(
            f"Missing discovery recipe validation results at {results_path}. "
            "Run `uv run run-discovery-recipe-validation-probes` first."
        )
    target_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = target_dir / "profiles"
    result_root = target_dir / "probe-results"
    if emit_profile_docs:
        profile_dir.mkdir(parents=True, exist_ok=True)
    result_root.mkdir(parents=True, exist_ok=True)

    source_rows = [dict(row) for row in _read_csv_rows(results_path)]
    included = included_buckets or list(DEFAULT_SCRUTINY_BUCKETS)
    queue_rows = build_retained_scrutiny_queue_rows(
        source_rows,
        included_buckets=included,
        max_rows=max_rows,
        timeframes=timeframes,
        instruments=instruments,
    )
    lookback_months = max(1, int(lookback_months or DEFAULT_SCRUTINY_LOOKBACK_MONTHS))
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    if refresh_static_atlas:
        build_indicator_atlas(
            config,
            workspace_root=workspace_root,
            catalog_path=catalog_path,
            out_dir=config.derived_root / "indicator-atlas",
        )
    catalog_by_id = _catalog_by_id(catalog_payload)

    probes: list[dict[str, Any]] = []
    exe, base_args = _fuzzfolio_base_args(config)
    filtered_rows: list[dict[str, Any]] = []
    for row in queue_rows:
        first_id = _clean_upper(row.get("first_indicator_id"))
        second_id = _clean_upper(row.get("second_indicator_id"))
        if first_id not in catalog_by_id or second_id not in catalog_by_id:
            continue
        probe_id = _clean_token(row.get("probe_id"))
        probe_timeframe = _clean_upper(row.get("probe_timeframe"))
        anchor_timeframe = _anchor_timeframe(
            catalog_by_id[first_id],
            probe_timeframe=probe_timeframe,
        )
        profile_path = profile_dir / f"{probe_id}.json"
        result_dir = result_root / probe_id
        row["anchor_timeframe"] = anchor_timeframe
        row["profile_path"] = str(profile_path)
        row["result_dir"] = str(result_dir)
        if emit_profile_docs:
            profile_doc = build_pair_profile_document(
                catalog_by_id=catalog_by_id,
                anchor_id=first_id,
                trigger_id=second_id,
                anchor_type="discovered_recipe_scrutiny",
                probe_timeframe=probe_timeframe,
                anchor_timeframe=anchor_timeframe,
                instruments=_split_csv_tokens(row.get("instruments")),
                probe_id=probe_id,
            )
            profile = _as_dict(profile_doc.get("profile"))
            profile["name"] = (
                f"Discovery Recipe 36m Scrutiny {row.get('recipe_id')} "
                f"{first_id}+{second_id} {probe_timeframe}"
            )
            profile["description"] = (
                "Temporary AutoResearch 36-month scrutiny profile for a retained "
                "empirically discovered recipe pair. This is the high-prior gate "
                "after 12-month retention."
            )
            _write_json(profile_path, profile_doc)
        sensitivity_args = _sensitivity_args_for_row(
            row,
            lookback_months=lookback_months,
            quality_score_preset=quality_score_preset,
            execution_cost_mode=execution_cost_mode,
            result_dir=result_dir,
        )
        sensitivity_args = _with_job_timeout_args(sensitivity_args, job_timeout_seconds)
        filtered_rows.append(row)
        probes.append(
            {
                "probe_id": probe_id,
                "queue_rank": row.get("queue_rank"),
                "recipe_id": row.get("recipe_id"),
                "recipe_confidence": row.get("recipe_confidence"),
                "first_indicator_id": first_id,
                "second_indicator_id": second_id,
                "probe_timeframe": probe_timeframe,
                "lookback_months": lookback_months,
                "anchor_timeframe": anchor_timeframe,
                "profile_path": str(profile_path),
                "output_dir": str(result_dir),
                "create_profile_args": ["profiles", "create", "--file", str(profile_path), "--pretty"],
                "sensitivity_basket_args": sensitivity_args,
                "validation_priority_score": row.get("validation_priority_score"),
                "source_validation_probe_id": row.get("source_validation_probe_id"),
                "source_retention_bucket": row.get("source_retention_bucket"),
            }
        )

    bucket_counts: dict[str, int] = {}
    for row in filtered_rows:
        key = _clean_token(row.get("source_retention_bucket")) or "unknown"
        bucket_counts[key] = bucket_counts.get(key, 0) + 1
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "validation_atlas_dir": str(source_dir),
            "validation_results_path": str(results_path),
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
        },
        "selection": {
            "included_buckets": included,
            "instruments": _normalize_tokens(instruments) or list(DEFAULT_INSTRUMENTS),
            "timeframes": _normalize_tokens(timeframes),
            "max_rows": max_rows,
            "lookback_months": lookback_months,
            "job_timeout_seconds": job_timeout_seconds,
            "quality_score_preset": quality_score_preset,
            "execution_cost_mode": execution_cost_mode,
        },
        "result_counts": {
            "source_validation_rows": len(source_rows),
            "queue_rows": len(filtered_rows),
            "profile_docs": len(probes) if emit_profile_docs else 0,
            "source_retention_bucket_counts": dict(sorted(bucket_counts.items())),
        },
        "top_queue": filtered_rows[:15],
    }
    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "summary": summary,
        "queue_rows": filtered_rows,
        "run_manifest": {
            "fuzzfolio_exe": exe,
            "fuzzfolio_base_args": base_args,
            "probes": probes,
        },
    }

    atlas_path = target_dir / "discovery-recipe-validation-atlas.json"
    queue_csv_path = target_dir / "discovery-recipe-validation-queue.csv"
    manifest_path = target_dir / "discovery-recipe-validation-run-manifest.json"
    run_script_path = target_dir / "run-discovery-recipe-scrutiny-probes.ps1"
    summary_path = target_dir / "discovery-recipe-scrutiny-summary.json"
    _write_json(atlas_path, atlas_payload)
    _write_csv(queue_csv_path, filtered_rows, _queue_fieldnames())
    _write_json(
        manifest_path,
        {
            "schema_version": "discovery_recipe_scrutiny_run_manifest_v1",
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
        generated_by="uv run build-discovery-recipe-scrutiny-atlas",
        description="Runs queued 36-month retained discovered recipe scrutiny sensitivity-basket probes.",
    )
    _write_json(summary_path, summary)
    return DiscoveryRecipeValidationAtlasBuildResult(
        atlas_path=atlas_path,
        queue_csv_path=queue_csv_path,
        manifest_path=manifest_path,
        run_script_path=run_script_path,
        profile_dir=profile_dir,
        summary_path=summary_path,
        summary=summary,
    )


def _retention_bucket(
    ratio: float | None,
    validation_score: float,
    *,
    has_discovery_evidence: bool = True,
) -> str:
    if not has_discovery_evidence:
        if validation_score >= 70.0:
            return "new_strong_cluster_expansion"
        if validation_score >= 60.0:
            return "new_positive_cluster_expansion"
        if validation_score > 0.0:
            return "new_low_cluster_expansion"
        return "new_failed_cluster_expansion"
    if ratio is None:
        return "unscored"
    if validation_score >= 70.0 and ratio >= 0.90:
        return "retained_strong"
    if validation_score >= 60.0 and ratio >= 0.75:
        return "retained"
    if validation_score >= 50.0 and ratio >= 0.55:
        return "partial_retention"
    return "failed_retention"


def _result_row_from_validation_score(
    row: dict[str, Any],
    *,
    profile_id: str | None,
    output_dir: Path,
    status: str,
    lookback_months: int,
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
    discovery_score = _float_value(row.get("discovery_evidence_score"))
    validation_score = _float_value(anchor_result.get("composite_score"))
    ratio: float | None = None
    has_discovery_evidence = discovery_score > 0.0
    if has_discovery_evidence:
        ratio = round(validation_score / discovery_score, 4)
    return {
        "probe_id": row.get("probe_id"),
        "queue_rank": row.get("queue_rank"),
        "recipe_id": row.get("recipe_id"),
        "recipe_confidence": row.get("recipe_confidence"),
        "first_cluster_id": row.get("first_cluster_id"),
        "second_cluster_id": row.get("second_cluster_id"),
        "first_indicator_id": row.get("first_indicator_id") or row.get("anchor_id"),
        "second_indicator_id": row.get("second_indicator_id") or row.get("trigger_id"),
        "probe_timeframe": row.get("probe_timeframe"),
        "lookback_months": lookback_months,
        "validation_priority_score": row.get("validation_priority_score"),
        "discovery_evidence_score": row.get("discovery_evidence_score"),
        "discovery_evidence_probe_id": row.get("discovery_evidence_probe_id"),
        "discovery_lane": row.get("discovery_lane"),
        "profile_id": profile_id,
        "output_dir": str(output_dir),
        "status": status,
        "primary_score": anchor_result.get("primary_score"),
        "composite_score": anchor_result.get("composite_score"),
        "retention_ratio": ratio,
        "retention_bucket": _retention_bucket(
            ratio,
            validation_score,
            has_discovery_evidence=has_discovery_evidence,
        ),
        "score_basis": anchor_result.get("score_basis"),
        "signal_count": anchor_result.get("signal_count"),
        "best_expectancy_r": anchor_result.get("best_expectancy_r"),
        "best_trades": anchor_result.get("best_trades"),
        "best_win_rate": anchor_result.get("best_win_rate"),
        "best_profit_factor": anchor_result.get("best_profit_factor"),
        "source_validation_probe_id": row.get("source_validation_probe_id"),
        "source_retention_bucket": row.get("source_retention_bucket"),
        "source_validation_score": row.get("source_validation_score"),
        "source_retention_ratio": row.get("source_retention_ratio"),
        "error": error,
    }


def run_discovery_recipe_validation_probes(
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
) -> DiscoveryRecipeValidationProbeRunResult:
    source_dir = (
        atlas_dir.expanduser().resolve()
        if atlas_dir is not None
        else config.derived_root / DEFAULT_DISCOVERY_RECIPE_VALIDATION_DIRNAME
    )
    atlas_path = source_dir / "discovery-recipe-validation-atlas.json"
    if not atlas_path.exists():
        raise FileNotFoundError(
            f"Missing discovery recipe validation atlas at {atlas_path}. "
            "Run `uv run build-discovery-recipe-validation-atlas` first."
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
    lookback_months = _int_value(
        _as_dict(_as_dict(payload.get("summary")).get("selection")).get("lookback_months"),
        DEFAULT_LOOKBACK_MONTHS,
    )
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
                return _result_row_from_validation_score(
                    row,
                    profile_id=None,
                    output_dir=output_dir,
                    status="skipped_existing",
                    lookback_months=lookback_months,
                    score_payload=score,
                    sensitivity_snapshot=snapshot,
                )
            except Exception as exc:
                return _result_row_from_validation_score(
                    row,
                    profile_id=None,
                    output_dir=output_dir,
                    status="skipped_existing_unscored",
                    lookback_months=lookback_months,
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
                    lookback_months=lookback_months,
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
            return _result_row_from_validation_score(
                row,
                profile_id=profile_id,
                output_dir=output_dir,
                status="ok",
                lookback_months=lookback_months,
                score_payload=score,
                sensitivity_snapshot=snapshot,
            )
        except Exception as exc:
            return _result_row_from_validation_score(
                row,
                profile_id=profile_id,
                output_dir=output_dir,
                status="failed",
                lookback_months=lookback_months,
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

    results: list[dict[str, Any]] = []
    worker_count = max(1, int(probe_workers or 1))
    completed = 0
    if worker_count == 1 or len(selected_rows) <= 1:
        for row in selected_rows:
            result = run_one(row)
            results.append(result)
            completed += 1
            if progress_callback:
                progress_callback(
                    {
                        "completed": completed,
                        "total": len(selected_rows),
                        "probe_id": result.get("probe_id"),
                        "status": result.get("status"),
                    }
                )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(run_one, row): row for row in selected_rows}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                completed += 1
                if progress_callback:
                    progress_callback(
                        {
                            "completed": completed,
                            "total": len(selected_rows),
                            "probe_id": result.get("probe_id"),
                            "status": result.get("status"),
                        }
                    )
    results.sort(key=lambda row: _int_value(row.get("queue_rank"), 1_000_000))

    results_csv_path = source_dir / "discovery-recipe-validation-results.csv"
    summary_path = source_dir / "discovery-recipe-validation-results-summary.json"
    _write_csv(results_csv_path, results, _result_fieldnames())

    status_counts: dict[str, int] = {}
    retention_counts: dict[str, int] = {}
    recipe_counts: dict[str, int] = {}
    for row in results:
        status = _clean_token(row.get("status")) or "unknown"
        bucket = _clean_token(row.get("retention_bucket")) or "unknown"
        recipe_id = _clean_token(row.get("recipe_id")) or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        retention_counts[bucket] = retention_counts.get(bucket, 0) + 1
        recipe_counts[recipe_id] = recipe_counts.get(recipe_id, 0) + 1
    scored = [
        row
        for row in results
        if row.get("composite_score") is not None and row.get("composite_score") != ""
    ]
    scored.sort(key=lambda row: -_float_value(row.get("composite_score")))
    retained = [
        row
        for row in scored
        if _clean_token(row.get("retention_bucket")) in {"retained", "retained_strong"}
    ]
    by_recipe: dict[str, list[dict[str, Any]]] = {}
    for row in scored:
        recipe_id = _clean_token(row.get("recipe_id")) or "unknown"
        bucket = by_recipe.setdefault(recipe_id, [])
        if len(bucket) < 5:
            bucket.append(row)
    summary = {
        "schema_version": RESULTS_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "discovery_recipe_validation_atlas_path": str(atlas_path),
        },
        "selection": {
            "requested_probe_ids": probe_ids or [],
            "limit": limit,
            "force": force,
            "keep_profiles": keep_profiles,
            "timeout_seconds": timeout_seconds,
            "job_timeout_seconds": job_timeout_seconds,
            "probe_workers": worker_count,
            "lookback_months": lookback_months,
        },
        "result_counts": {
            "selected": len(selected_rows),
            "completed": len(results),
            "status_counts": dict(sorted(status_counts.items())),
            "retention_bucket_counts": dict(sorted(retention_counts.items())),
            "recipe_counts": dict(sorted(recipe_counts.items())),
            "scored": len(scored),
            "retained": len(retained),
        },
        "top_scored": scored[:20],
        "top_retained": retained[:20],
        "top_by_recipe": by_recipe,
    }
    _write_json(summary_path, summary)
    return DiscoveryRecipeValidationProbeRunResult(
        results_csv_path=results_csv_path,
        summary_path=summary_path,
        summary=summary,
    )
