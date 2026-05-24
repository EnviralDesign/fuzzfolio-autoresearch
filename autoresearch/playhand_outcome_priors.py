from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig


SCHEMA_VERSION = "playhand_outcome_priors_v1"
DEFAULT_PLAYHAND_OUTCOME_PRIORS_DIRNAME = "playhand-outcome-priors"
DEFAULT_REVIEW_DIRNAME = "cgpt review"

PAIR_FIELDNAMES = [
    "family_id",
    "dealt_pair_probe_id",
    "template_branch_source_probe_id",
    "dealt_recipe",
    "dealt_pair_source",
    "count",
    "promoted",
    "tombstoned",
    "promotion_rate",
    "exact_selected",
    "mutated_selected",
    "exact_rescues",
    "exact_outscored_mutated",
    "comparable_template_runs",
    "mutated_wins_over_exact",
    "exact_rescue_rate",
    "exact_selected_rate",
    "mutated_win_rate",
    "avg_mutation_delta",
    "avg_score",
    "avg_positive_score",
    "best_score",
    "best_seed",
    "family_classification",
    "family_policy",
    "exact_branch_required",
    "recommended_max_indicators",
    "role_balanced_fill_limit",
    "mutation_pressure",
    "sampling_weight_multiplier",
    "family_cap_share",
    "source_batches",
]

RECIPE_FIELDNAMES = [
    "recipe",
    "recipe_source",
    "count",
    "promoted",
    "tombstoned",
    "promotion_rate",
    "exact_selected",
    "mutated_selected",
    "exact_rescues",
    "exact_outscored_mutated",
    "comparable_template_runs",
    "mutated_wins_over_exact",
    "exact_rescue_rate",
    "exact_selected_rate",
    "mutated_win_rate",
    "avg_mutation_delta",
    "avg_score",
    "avg_positive_score",
    "best_score",
    "best_seed",
    "family_classification",
    "recipe_policy",
    "recipe_sampling_weight_multiplier",
    "recipe_cap_share",
    "source_batches",
]


@dataclass(frozen=True)
class PlayHandOutcomePriorsBuildResult:
    priors_path: Path
    pair_csv_path: Path
    recipe_csv_path: Path
    summary_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "playhand_outcome_priors_json": str(self.priors_path),
            "pair_family_outcome_priors_csv": str(self.pair_csv_path),
            "recipe_outcome_priors_csv": str(self.recipe_csv_path),
            "playhand_outcome_priors_summary_json": str(self.summary_path),
            "summary": self.summary,
        }


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or isinstance(value, bool):
        return default
    text = str(value).strip()
    if text == "":
        return default
    try:
        number = float(text)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _safe_int(value: Any, default: int = 0) -> int:
    number = _safe_float(value)
    if number is None:
        return default
    return int(number)


def _safe_rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def _round(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(value, digits)


def _read_csv(path: Path) -> list[dict[str, Any]]:
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


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def default_report_dirs(config: AppConfig) -> list[Path]:
    candidates: list[Path] = []
    review_root = config.repo_root / DEFAULT_REVIEW_DIRNAME
    if review_root.exists():
        candidates.extend(
            sorted(review_root.glob("playhand-prior-test-clean-*"), key=lambda path: path.name)
        )
    if config.derived_root.exists():
        candidates.extend(
            sorted(config.derived_root.glob("playhand-prior-test-clean-*"), key=lambda path: path.name)
        )
    deduped: list[Path] = []
    seen: set[Path] = set()
    seen_names: set[str] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen or path.name in seen_names:
            continue
        if (path / "recipe-performance-pairs.csv").exists() or (
            path / "recipe-performance-recipes.csv"
        ).exists():
            seen.add(resolved)
            seen_names.add(path.name)
            deduped.append(path)
    return deduped


def _family_id(row: dict[str, Any]) -> str:
    return _clean(row.get("template_branch_source_probe_id")) or _clean(
        row.get("dealt_pair_probe_id")
    )


def _policy_for_pair_classification(
    classification: str,
    *,
    promotion_rate: float | None = None,
) -> dict[str, Any]:
    if classification == "template_locked":
        return {
            "family_policy": "template_locked",
            "exact_branch_required": True,
            "recommended_max_indicators": 2,
            "role_balanced_fill_limit": 0,
            "mutation_pressure": "low",
            "sampling_weight_multiplier": 1.15,
            "family_cap_share": 0.15,
        }
    if classification == "template_guarded":
        return {
            "family_policy": "template_guarded",
            "exact_branch_required": True,
            "recommended_max_indicators": 4,
            "role_balanced_fill_limit": 1,
            "mutation_pressure": "normal",
            "sampling_weight_multiplier": 1.05,
            "family_cap_share": 0.15,
        }
    if classification == "mutation_friendly":
        return {
            "family_policy": "mutation_friendly",
            "exact_branch_required": True,
            "recommended_max_indicators": 4,
            "role_balanced_fill_limit": 2,
            "mutation_pressure": "normal",
            "sampling_weight_multiplier": 1.10,
            "family_cap_share": 0.15,
        }
    if classification == "unstable":
        if (promotion_rate or 0.0) >= 0.70:
            return {
                "family_policy": "unstable",
                "exact_branch_required": True,
                "recommended_max_indicators": 3,
                "role_balanced_fill_limit": 1,
                "mutation_pressure": "low",
                "sampling_weight_multiplier": 0.85,
                "family_cap_share": 0.12,
            }
        return {
            "family_policy": "unstable",
            "exact_branch_required": True,
            "recommended_max_indicators": 3,
            "role_balanced_fill_limit": 0,
            "mutation_pressure": "low",
            "sampling_weight_multiplier": 0.65,
            "family_cap_share": 0.08,
        }
    return {
        "family_policy": "under_sampled",
        "exact_branch_required": True,
        "recommended_max_indicators": 3,
        "role_balanced_fill_limit": 1,
        "mutation_pressure": "low",
        "sampling_weight_multiplier": 0.85,
        "family_cap_share": 0.08,
    }


def _classify(
    *,
    count: int,
    promotion_rate: float | None,
    comparable_template_runs: int,
    exact_rescue_rate: float | None,
    exact_selected_rate: float | None,
    mutated_win_rate: float | None,
    avg_mutation_delta: float | None,
) -> str:
    if count < 3:
        return "under_sampled"
    if (exact_rescue_rate or 0.0) >= 0.40:
        return "template_locked"
    if (mutated_win_rate or 0.0) >= 0.60 and (avg_mutation_delta or 0.0) > 3.0:
        return "mutation_friendly"
    if (exact_selected_rate or 0.0) >= 0.40:
        return "template_guarded"
    if (
        count >= 5
        and (promotion_rate or 0.0) >= 0.90
        and comparable_template_runs >= 5
        and avg_mutation_delta is not None
        and avg_mutation_delta <= -5.0
    ):
        return "template_guarded"
    return "unstable"


def _recipe_policy(row: dict[str, Any]) -> dict[str, Any]:
    recipe_source = _clean(row.get("recipe_source"))
    classification = _clean(row.get("family_classification")) or "unstable"
    promotion_rate = _safe_float(row.get("promotion_rate"), 0.0) or 0.0
    if recipe_source == "discovery_recipe_validation" and promotion_rate >= 0.9:
        multiplier = 1.10 if classification == "template_locked" else 1.05
        cap = 0.30
    elif classification == "unstable" and promotion_rate < 0.35:
        multiplier = 0.60
        cap = 0.12
    elif classification == "unstable" and promotion_rate < 0.60:
        multiplier = 0.85
        cap = 0.18
    elif classification == "under_sampled":
        multiplier = 0.90
        cap = 0.12
    else:
        multiplier = 1.0
        cap = 0.25 if recipe_source == "discovery_recipe_validation" else 0.20
    return {
        "recipe_policy": classification,
        "recipe_sampling_weight_multiplier": multiplier,
        "recipe_cap_share": cap,
    }


def _aggregate_group(rows: list[dict[str, Any]], *, key_field: str) -> dict[str, Any]:
    count = sum(_safe_int(row.get("count")) for row in rows)
    promoted = sum(_safe_int(row.get("promoted")) for row in rows)
    tombstoned = sum(_safe_int(row.get("tombstoned")) for row in rows)
    exact_selected = sum(_safe_int(row.get("exact_selected")) for row in rows)
    mutated_selected = sum(_safe_int(row.get("mutated_selected")) for row in rows)
    exact_rescues = sum(_safe_int(row.get("exact_rescues")) for row in rows)
    exact_outscored = sum(_safe_int(row.get("exact_outscored_mutated")) for row in rows)
    comparable = sum(_safe_int(row.get("comparable_template_runs")) for row in rows)
    mutated_wins = sum(_safe_int(row.get("mutated_wins_over_exact")) for row in rows)

    def weighted_avg(field: str, weight_field: str) -> float | None:
        total_weight = 0
        total_value = 0.0
        for row in rows:
            value = _safe_float(row.get(field))
            weight = _safe_int(row.get(weight_field))
            if value is None or weight <= 0:
                continue
            total_value += value * weight
            total_weight += weight
        return _round(total_value / total_weight) if total_weight else None

    best_row = max(
        rows,
        key=lambda row: _safe_float(row.get("best_score"), float("-inf")) or float("-inf"),
    )
    avg_mutation_delta = weighted_avg("avg_mutation_delta", "comparable_template_runs")
    exact_rescue_rate = _safe_rate(exact_rescues, count)
    exact_selected_rate = _safe_rate(exact_selected, count)
    mutated_win_rate = _safe_rate(mutated_wins, comparable)
    promotion_rate = _safe_rate(promoted, count)
    classification = _classify(
        count=count,
        promotion_rate=promotion_rate,
        comparable_template_runs=comparable,
        exact_rescue_rate=exact_rescue_rate,
        exact_selected_rate=exact_selected_rate,
        mutated_win_rate=mutated_win_rate,
        avg_mutation_delta=avg_mutation_delta,
    )
    source_batches = sorted({_clean(row.get("_source_batch")) for row in rows if _clean(row.get("_source_batch"))})
    return {
        key_field: _clean(best_row.get(key_field)),
        "count": count,
        "promoted": promoted,
        "tombstoned": tombstoned,
        "promotion_rate": promotion_rate,
        "exact_selected": exact_selected,
        "mutated_selected": mutated_selected,
        "exact_rescues": exact_rescues,
        "exact_outscored_mutated": exact_outscored,
        "comparable_template_runs": comparable,
        "mutated_wins_over_exact": mutated_wins,
        "exact_rescue_rate": exact_rescue_rate,
        "exact_selected_rate": exact_selected_rate,
        "mutated_win_rate": mutated_win_rate,
        "avg_mutation_delta": avg_mutation_delta,
        "avg_score": weighted_avg("avg_score", "count"),
        "avg_positive_score": weighted_avg("avg_positive_score", "promoted"),
        "best_score": _safe_float(best_row.get("best_score")),
        "best_seed": best_row.get("best_seed"),
        "family_classification": classification,
        "source_batches": ",".join(source_batches),
    }


def build_playhand_outcome_prior_artifacts(
    *,
    report_dirs: list[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    pair_groups: dict[str, list[dict[str, Any]]] = {}
    recipe_groups: dict[str, list[dict[str, Any]]] = {}
    source_dirs: list[str] = []
    for report_dir in report_dirs:
        if not report_dir.exists():
            continue
        label = report_dir.name
        pair_rows = _read_csv(report_dir / "recipe-performance-pairs.csv")
        recipe_rows = _read_csv(report_dir / "recipe-performance-recipes.csv")
        if pair_rows or recipe_rows:
            source_dirs.append(str(report_dir))
        for row in pair_rows:
            family_id = _family_id(row)
            source = _clean(row.get("dealt_pair_source"))
            recipe = _clean(row.get("dealt_recipe"))
            if not family_id or family_id == "unknown" or source in {"", "unknown"} or recipe == "unknown":
                continue
            row = dict(row)
            row["_source_batch"] = label
            row["family_id"] = family_id
            pair_groups.setdefault(family_id, []).append(row)
        for row in recipe_rows:
            recipe = _clean(row.get("dealt_recipe"))
            source = _clean(row.get("dealt_recipe_source"))
            if not recipe or recipe == "unknown" or source in {"", "unknown"}:
                continue
            row = dict(row)
            row["_source_batch"] = label
            row["recipe"] = recipe
            row["recipe_source"] = source
            recipe_groups.setdefault(recipe, []).append(row)

    pair_rows_out: list[dict[str, Any]] = []
    for family_id, rows in pair_groups.items():
        row = _aggregate_group(rows, key_field="family_id")
        exemplar = max(rows, key=lambda item: _safe_int(item.get("count")))
        row.update(
            {
                "family_id": family_id,
                "dealt_pair_probe_id": _clean(exemplar.get("dealt_pair_probe_id")),
                "template_branch_source_probe_id": _clean(
                    exemplar.get("template_branch_source_probe_id")
                ),
                "dealt_recipe": _clean(exemplar.get("dealt_recipe")),
                "dealt_pair_source": _clean(exemplar.get("dealt_pair_source")),
            }
        )
        row.update(
            _policy_for_pair_classification(
                str(row.get("family_classification") or ""),
                promotion_rate=_safe_float(row.get("promotion_rate"), 0.0),
            )
        )
        pair_rows_out.append(row)

    recipe_rows_out: list[dict[str, Any]] = []
    for recipe, rows in recipe_groups.items():
        row = _aggregate_group(rows, key_field="recipe")
        exemplar = max(rows, key=lambda item: _safe_int(item.get("count")))
        row.update(
            {
                "recipe": recipe,
                "recipe_source": _clean(exemplar.get("recipe_source")),
            }
        )
        row.update(_recipe_policy(row))
        recipe_rows_out.append(row)

    pair_rows_out.sort(
        key=lambda row: (
            _safe_int(row.get("count")),
            _safe_float(row.get("promotion_rate"), 0.0) or 0.0,
            _safe_float(row.get("best_score"), 0.0) or 0.0,
            str(row.get("family_id") or ""),
        ),
        reverse=True,
    )
    recipe_rows_out.sort(
        key=lambda row: (
            _safe_int(row.get("count")),
            _safe_float(row.get("promotion_rate"), 0.0) or 0.0,
            _safe_float(row.get("best_score"), 0.0) or 0.0,
            str(row.get("recipe") or ""),
        ),
        reverse=True,
    )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_report_dirs": source_dirs,
        "result_counts": {
            "pair_family_rows": len(pair_rows_out),
            "recipe_rows": len(recipe_rows_out),
            "template_locked_pair_families": sum(
                1 for row in pair_rows_out if row.get("family_policy") == "template_locked"
            ),
            "template_guarded_pair_families": sum(
                1 for row in pair_rows_out if row.get("family_policy") == "template_guarded"
            ),
            "mutation_friendly_pair_families": sum(
                1 for row in pair_rows_out if row.get("family_policy") == "mutation_friendly"
            ),
        },
        "classification_rules": [
            "under_sampled: count < 3",
            "template_locked: exact_rescue_rate >= 0.40",
            "mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3",
            (
                "template_guarded: exact_selected_rate >= 0.40, or count >= 5 "
                "with promotion_rate >= 0.90, comparable_template_runs >= 5, "
                "and avg_mutation_delta <= -5"
            ),
            "unstable: otherwise; productive unstable families use softened sampling policy",
        ],
        "policy_defaults": {
            "max_exact_pair_template_family_share": 0.15,
            "max_discovered_recipe_share": 0.30,
            "global_sampling_policy": "preserve existing 70/20/10 when present",
        },
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": summary["generated_at"],
        "note": (
            "Play Hand outcome priors are a second-layer policy overlay derived from clean batch "
            "outcomes. They do not overwrite empirical validation scores."
        ),
        "pair_families": {str(row["family_id"]): row for row in pair_rows_out},
        "recipes": {str(row["recipe"]): row for row in recipe_rows_out},
        "summary": summary,
    }
    return payload, pair_rows_out, recipe_rows_out, summary


def build_playhand_outcome_priors(
    config: AppConfig,
    *,
    report_dirs: list[Path] | None = None,
    out_dir: Path | None = None,
) -> PlayHandOutcomePriorsBuildResult:
    selected_report_dirs = [
        path.expanduser().resolve() for path in (report_dirs or default_report_dirs(config))
    ]
    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_PLAYHAND_OUTCOME_PRIORS_DIRNAME
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    payload, pair_rows, recipe_rows, summary = build_playhand_outcome_prior_artifacts(
        report_dirs=selected_report_dirs
    )
    priors_path = target_dir / "playhand-outcome-priors.json"
    pair_csv_path = target_dir / "pair-family-outcome-priors.csv"
    recipe_csv_path = target_dir / "recipe-outcome-priors.csv"
    summary_path = target_dir / "playhand-outcome-priors-summary.json"
    _write_json(priors_path, payload)
    _write_csv(pair_csv_path, pair_rows, PAIR_FIELDNAMES)
    _write_csv(recipe_csv_path, recipe_rows, RECIPE_FIELDNAMES)
    _write_json(summary_path, summary)
    return PlayHandOutcomePriorsBuildResult(
        priors_path=priors_path,
        pair_csv_path=pair_csv_path,
        recipe_csv_path=recipe_csv_path,
        summary_path=summary_path,
        summary=summary,
    )
