from __future__ import annotations

import ast
import csv
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig


SCHEMA_VERSION = "indicator_atlas_v1"
DEFAULT_ATLAS_DIRNAME = "indicator-atlas"
DEFAULT_ANCHORS: dict[str, tuple[str, ...]] = {
    "trend": ("MA_SLOPE_TREND", "ADX", "KALMAN_VELOCITY_CONFIRM"),
    "mean_reversion": ("RSI_MEAN_REVERSION", "BBANDS_POSITION_MEAN_REVERSION"),
    "compression": (
        "BOLLINGER_KELTNER_SQUEEZE_FILTER",
        "TOBY_CRABEL_NARROW_RANGE",
    ),
    "profile_value": ("ROLLING_VOLUME_PROFILE_CONTEXT",),
}
RECIPE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "trend_pullback_continuation": {
        "strategy_weights": {"trend": 20, "confirm": 8, "breakout": 4},
        "slots": {
            "context": ("context", "filter"),
            "setup": ("setup",),
            "trigger": ("trigger",),
            "guard": ("filter", "context"),
        },
    },
    "mean_reversion_reclaim": {
        "strategy_weights": {"mean-reversion": 22, "confirm": 6, "filter": 4},
        "slots": {
            "context": ("context", "filter"),
            "setup": ("setup",),
            "trigger": ("trigger",),
            "guard": ("filter", "context"),
        },
    },
    "breakout_compression_release": {
        "strategy_weights": {"breakout": 24, "trend": 8, "confirm": 5},
        "slots": {
            "context": ("context", "filter"),
            "setup": ("setup", "filter"),
            "trigger": ("trigger",),
            "guard": ("filter", "context"),
        },
    },
    "profile_value_context": {
        "strategy_weights": {"filter": 14, "mean-reversion": 10, "confirm": 6},
        "slots": {
            "context": ("context", "filter"),
            "setup": ("setup",),
            "trigger": ("trigger",),
            "guard": ("filter", "context"),
        },
    },
}


@dataclass(frozen=True)
class IndicatorAtlasBuildResult:
    atlas_path: Path
    csv_path: Path
    dependencies_path: Path
    pair_matrix_path: Path
    recipe_priors_path: Path
    summary: dict[str, Any]

    def as_summary(self) -> dict[str, Any]:
        return {
            "indicator_atlas_json": str(self.atlas_path),
            "indicator_atlas_csv": str(self.csv_path),
            "indicator_dependencies_json": str(self.dependencies_path),
            "indicator_pair_matrix_csv": str(self.pair_matrix_path),
            "recipe_priors_json": str(self.recipe_priors_path),
            "summary": self.summary,
        }


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


def _clean_upper(value: Any) -> str:
    return _clean_token(value).upper()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _coerce_count(value: float) -> int | None:
    if not math.isfinite(value) or value < 0:
        return None
    rounded = int(round(value))
    if abs(value - rounded) < 1e-9:
        return rounded
    return int(math.floor(value + 1e-9))


def _allowed_value_count(parameter: dict[str, Any]) -> tuple[int | None, str | None]:
    options = _as_list(parameter.get("options"))
    if options:
        return len(options), None
    ui_type = str(parameter.get("uiType") or "").strip().lower()
    if ui_type in {"checkbox", "boolean", "toggle"} or isinstance(parameter.get("default"), bool):
        return 2, None

    minimum = _coerce_float(parameter.get("min"))
    maximum = _coerce_float(parameter.get("max"))
    step = _coerce_float(parameter.get("step"))
    if minimum is None or maximum is None or step is None:
        return None, "missing_min_max_step"
    if step <= 0:
        return None, "non_positive_step"
    if maximum < minimum:
        return None, "max_less_than_min"

    count = _coerce_count(((maximum - minimum) / step) + 1)
    if count is None or count <= 0:
        return None, "invalid_numeric_range"
    return count, None


def _parameter_default(config_defaults: dict[str, Any], name: str, fallback: Any) -> Any:
    for item in _as_list(config_defaults.get("talibConfig")):
        if not isinstance(item, dict):
            continue
        if _clean_token(item.get("name")) == name:
            return item.get("value", fallback)
    return fallback


def _compact_ring_values(parameter: dict[str, Any], default_value: Any) -> list[Any]:
    values: list[Any] = []

    def add(value: Any) -> None:
        if value is None or value in values:
            return
        values.append(value)

    options = _as_list(parameter.get("options"))
    if options:
        add(default_value)
        for option in options[:4]:
            if isinstance(option, dict):
                add(option.get("value"))
        return values[:5]

    add(parameter.get("min"))
    for mark in _as_list(parameter.get("marks")):
        if isinstance(mark, dict):
            add(mark.get("value"))
    add(default_value)
    add(parameter.get("max"))
    return values[:5]


def _static_prior_score(row: dict[str, Any]) -> float:
    score = 45.0
    if row.get("known_implementation_class"):
        score += 18.0
    else:
        score -= 30.0

    signal_role = str(row.get("signal_role") or "").lower()
    if signal_role == "trigger":
        score += 10.0
    elif signal_role == "setup":
        score += 8.0
    elif signal_role == "context":
        score += 4.0
    elif signal_role == "filter":
        score += 2.0

    if row.get("signal_persistence") == "event-with-lookback":
        score += 4.0

    cardinality = row.get("theoretical_parameter_cardinality")
    if not row.get("parameter_cardinality_known"):
        score -= 8.0
    elif isinstance(cardinality, int):
        if cardinality <= 1_000:
            score += 10.0
        elif cardinality <= 100_000:
            score += 4.0
        elif cardinality <= 1_000_000:
            score -= 2.0
        else:
            score -= 10.0

    if row.get("namespace") in {"Patterns", "Volume"}:
        score -= 2.0

    return round(max(0.0, min(100.0, score)), 2)


def _static_prior_bucket(row: dict[str, Any]) -> str:
    if not row.get("generation_eligible"):
        return "broken_or_unmapped"
    signal_role = str(row.get("signal_role") or "").lower()
    strategy_role = str(row.get("strategy_role") or "").lower()
    namespace = str(row.get("namespace") or "")
    if signal_role in {"context", "filter"} or strategy_role == "filter":
        return "context_dependent"
    if namespace in {"Patterns", "Volume"}:
        return "context_dependent"
    if not row.get("parameter_cardinality_known"):
        return "uncertain_parameter_space"
    cardinality = row.get("theoretical_parameter_cardinality")
    if isinstance(cardinality, int) and cardinality > 1_000_000:
        return "low_prior_broad_space"
    if signal_role in {"trigger", "setup"}:
        return "high_static_prior"
    return "medium_static_prior"


def _audit_indicator(
    indicator: dict[str, Any],
    *,
    implementation_by_id: dict[str, str],
    source_file_by_id: dict[str, str],
) -> dict[str, Any]:
    meta = _as_dict(indicator.get("meta"))
    config_defaults = _as_dict(indicator.get("config"))
    indicator_id = _clean_upper(meta.get("id"))
    talib_meta = [item for item in _as_list(meta.get("talibMeta")) if isinstance(item, dict)]

    parameter_axes: list[dict[str, Any]] = []
    cardinality = 1
    cardinality_known = True
    unknown_parameter_count = 0
    sweepable_parameters: list[str] = []
    for parameter in talib_meta:
        name = _clean_token(parameter.get("name"))
        if not name:
            unknown_parameter_count += 1
            cardinality_known = False
            continue
        default_value = _parameter_default(config_defaults, name, parameter.get("default"))
        value_count, unknown_reason = _allowed_value_count(parameter)
        if value_count is None:
            cardinality_known = False
            unknown_parameter_count += 1
        else:
            cardinality *= max(1, int(value_count))
        if value_count is None or value_count > 1:
            sweepable_parameters.append(name)
        parameter_axes.append(
            {
                "name": name,
                "ui_type": parameter.get("uiType"),
                "default": default_value,
                "min": parameter.get("min"),
                "max": parameter.get("max"),
                "step": parameter.get("step"),
                "allowed_value_count": value_count,
                "cardinality_known": value_count is not None,
                "unknown_reason": unknown_reason,
                "ring_values": _compact_ring_values(parameter, default_value),
            }
        )

    implementation_class = implementation_by_id.get(indicator_id)
    row: dict[str, Any] = {
        "id": indicator_id,
        "name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "talib_function": meta.get("talibFunction"),
        "base_indicator_id": meta.get("baseIndicatorId"),
        "strategy_role": meta.get("strategyRole"),
        "signal_role": meta.get("signalRole"),
        "signal_persistence": meta.get("signalPersistence"),
        "preferred_timeframe_role": meta.get("preferredTimeframeRole"),
        "supports_trading_mode": meta.get("supportsTradingMode"),
        "uses_range_configuration": meta.get("usesRangeConfiguration"),
        "required_padding_bars": meta.get("requiredPaddingBars"),
        "known_implementation_class": implementation_class,
        "implementation_mapped": bool(implementation_class),
        "source_file": source_file_by_id.get(indicator_id),
        "generation_eligible": bool(indicator_id and config_defaults and implementation_class),
        "expected_scaffoldable": bool(indicator_id and config_defaults and implementation_class),
        "verified_scaffoldable": None,
        "scaffold_check_status": "not_checked",
        "talib_parameter_count": len(talib_meta),
        "sweepable_parameter_count": len(sweepable_parameters),
        "sweepable_parameters": sweepable_parameters,
        "parameter_cardinality_known": cardinality_known,
        "unknown_parameter_count": unknown_parameter_count,
        "theoretical_parameter_cardinality": cardinality if cardinality_known else None,
        "parameter_axes": parameter_axes,
        "default_timeframe": config_defaults.get("timeframe"),
        "default_lookback_bars": config_defaults.get("lookbackBars"),
        "default_weight": config_defaults.get("weight"),
        "default_is_trend_following": config_defaults.get("isTrendFollowing"),
        "default_ranges": config_defaults.get("ranges"),
    }
    row["static_prior_score"] = _static_prior_score(row)
    row["static_prior_bucket"] = _static_prior_bucket(row)
    return row


def _catalog_path_for_workspace(workspace_root: Path) -> Path:
    return workspace_root / "shared" / "constants" / "indicators.json"


def _indicator_constants_dir_for_workspace(workspace_root: Path) -> Path:
    return workspace_root / "shared" / "constants" / "indicators"


def _factory_path_for_workspace(workspace_root: Path) -> Path:
    return (
        workspace_root
        / "shared"
        / "python"
        / "fuzzfolio_core"
        / "fuzzfolio_core"
        / "scoring_engine"
        / "indicators"
        / "indicator_factory.py"
    )


def resolve_trading_dashboard_root(
    config: AppConfig,
    workspace_root: Path | None = None,
) -> Path:
    candidates: list[Path] = []
    if workspace_root is not None:
        candidates.append(workspace_root)
    if config.fuzzfolio.workspace_root is not None:
        candidates.append(config.fuzzfolio.workspace_root)
    env_value = os.environ.get("AUTORESEARCH_FUZZFOLIO_WORKSPACE_ROOT")
    if env_value:
        candidates.append(Path(env_value))
    candidates.extend(
        [
            Path(r"C:\repos\Trading-Dashboard"),
            config.repo_root.parent / "Trading-Dashboard",
        ]
    )

    seen: set[str] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        if _catalog_path_for_workspace(resolved).exists():
            return resolved

    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "Could not locate Trading-Dashboard indicator catalog. "
        "Pass --workspace-root or --catalog-path. Searched: "
        + searched
    )


def load_indicator_catalog(
    *,
    config: AppConfig,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
) -> tuple[dict[str, Any], Path | None, Path]:
    if catalog_path is not None:
        resolved_catalog = catalog_path.expanduser().resolve()
        payload = _read_json(resolved_catalog)
        return payload, None, resolved_catalog

    resolved_root = resolve_trading_dashboard_root(config, workspace_root)
    resolved_catalog = _catalog_path_for_workspace(resolved_root)
    payload = _read_json(resolved_catalog)
    return payload, resolved_root, resolved_catalog


def parse_indicator_factory_mapping(factory_path: Path | None) -> dict[str, str]:
    if factory_path is None or not factory_path.exists():
        return {}
    tree = ast.parse(factory_path.read_text(encoding="utf-8"))

    def parse_mapping_node(value: ast.AST) -> dict[str, str] | None:
        if not isinstance(value, ast.Dict):
            return None
        mapping: dict[str, str] = {}
        for key_node, value_node in zip(value.keys, value.values):
            if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
                continue
            if isinstance(value_node, ast.Name):
                mapping[key_node.value.upper()] = value_node.id
            elif isinstance(value_node, ast.Attribute):
                mapping[key_node.value.upper()] = value_node.attr
        return mapping

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            if not any(
                isinstance(target, ast.Name) and target.id == "INDICATOR_CLASSES"
                for target in node.targets
            ):
                continue
            mapping = parse_mapping_node(node.value)
            if mapping is not None:
                return mapping
        if isinstance(node, ast.AnnAssign):
            if not isinstance(node.target, ast.Name) or node.target.id != "INDICATOR_CLASSES":
                continue
            mapping = parse_mapping_node(node.value)
            if mapping is not None:
                return mapping
    return {}


def _source_file_index(workspace_root: Path | None) -> dict[str, str]:
    if workspace_root is None:
        return {}
    constants_dir = _indicator_constants_dir_for_workspace(workspace_root)
    if not constants_dir.exists():
        return {}
    source_by_id: dict[str, str] = {}
    for path in sorted(constants_dir.glob("*.json")):
        try:
            payload = _read_json(path)
        except Exception:
            continue
        indicator_id = _clean_upper(_as_dict(payload.get("meta")).get("id"))
        if indicator_id:
            source_by_id[indicator_id] = str(path)
    return source_by_id


def build_indicator_rows(
    catalog_payload: dict[str, Any],
    *,
    implementation_by_id: dict[str, str],
    source_file_by_id: dict[str, str],
) -> list[dict[str, Any]]:
    indicators = [
        item
        for item in _as_list(catalog_payload.get("indicators"))
        if isinstance(item, dict)
    ]
    rows = [
        _audit_indicator(
            indicator,
            implementation_by_id=implementation_by_id,
            source_file_by_id=source_file_by_id,
        )
        for indicator in indicators
    ]
    return sorted(rows, key=lambda row: str(row.get("id") or ""))


def build_indicator_dependencies(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, defaultdict[str, list[str]]] = {
        "by_base_indicator_id": defaultdict(list),
        "by_implementation_class": defaultdict(list),
        "by_talib_function": defaultdict(list),
        "by_namespace": defaultdict(list),
        "by_signal_role": defaultdict(list),
        "by_strategy_role": defaultdict(list),
    }
    for row in rows:
        indicator_id = str(row.get("id") or "")
        if not indicator_id:
            continue
        for group_name, field in (
            ("by_base_indicator_id", "base_indicator_id"),
            ("by_implementation_class", "known_implementation_class"),
            ("by_talib_function", "talib_function"),
            ("by_namespace", "namespace"),
            ("by_signal_role", "signal_role"),
            ("by_strategy_role", "strategy_role"),
        ):
            value = _clean_token(row.get(field)) or "__missing__"
            groups[group_name][value].append(indicator_id)

    return {
        "schema_version": "indicator_dependencies_v1",
        "groups": {
            name: {key: sorted(values) for key, values in sorted(group.items())}
            for name, group in groups.items()
        },
        "missing_implementation_ids": sorted(
            str(row.get("id"))
            for row in rows
            if row.get("id") and not row.get("known_implementation_class")
        ),
        "variant_edges": [
            {
                "base_indicator_id": row.get("base_indicator_id"),
                "indicator_id": row.get("id"),
            }
            for row in rows
            if row.get("base_indicator_id") and row.get("base_indicator_id") != row.get("id")
        ],
    }


def _count_by(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counter = Counter(str(row.get(field) or "__missing__") for row in rows)
    return dict(sorted(counter.items()))


def build_indicator_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "indicator_count": len(rows),
        "generation_eligible_count": sum(1 for row in rows if row.get("generation_eligible")),
        "implementation_mapped_count": sum(1 for row in rows if row.get("known_implementation_class")),
        "missing_implementation_count": sum(1 for row in rows if not row.get("known_implementation_class")),
        "missing_implementation_ids": sorted(
            str(row.get("id"))
            for row in rows
            if row.get("id") and not row.get("known_implementation_class")
        ),
        "signal_role_counts": _count_by(rows, "signal_role"),
        "strategy_role_counts": _count_by(rows, "strategy_role"),
        "namespace_counts": _count_by(rows, "namespace"),
        "static_prior_bucket_counts": _count_by(rows, "static_prior_bucket"),
        "total_theoretical_parameter_cardinality_known": all(
            bool(row.get("parameter_cardinality_known")) for row in rows
        ),
        "largest_parameter_spaces": [
            {
                "id": row.get("id"),
                "theoretical_parameter_cardinality": row.get(
                    "theoretical_parameter_cardinality"
                ),
                "talib_parameter_count": row.get("talib_parameter_count"),
            }
            for row in sorted(
                rows,
                key=lambda item: int(item.get("theoretical_parameter_cardinality") or -1),
                reverse=True,
            )[:10]
        ],
    }


def _slot_candidate_score(
    row: dict[str, Any],
    *,
    recipe_definition: dict[str, Any],
    slot_roles: tuple[str, ...],
) -> float:
    signal_role = str(row.get("signal_role") or "").lower()
    strategy_role = str(row.get("strategy_role") or "").lower()
    score = float(row.get("static_prior_score") or 0.0)
    if signal_role in slot_roles:
        score += 22.0
    else:
        score -= 12.0
    score += float(recipe_definition.get("strategy_weights", {}).get(strategy_role, 0))
    if not row.get("generation_eligible"):
        score -= 50.0
    return round(max(0.0, min(100.0, score)), 2)


def build_recipe_priors(rows: list[dict[str, Any]]) -> dict[str, Any]:
    recipes: dict[str, Any] = {}
    for recipe_name, definition in RECIPE_DEFINITIONS.items():
        slots: dict[str, Any] = {}
        for slot_name, slot_roles in definition["slots"].items():
            candidates = [
                {
                    "id": row.get("id"),
                    "signal_role": row.get("signal_role"),
                    "strategy_role": row.get("strategy_role"),
                    "namespace": row.get("namespace"),
                    "static_prior_score": row.get("static_prior_score"),
                    "recipe_slot_prior": _slot_candidate_score(
                        row,
                        recipe_definition=definition,
                        slot_roles=slot_roles,
                    ),
                    "static_prior_bucket": row.get("static_prior_bucket"),
                }
                for row in rows
                if row.get("generation_eligible")
            ]
            candidates.sort(
                key=lambda item: (
                    float(item.get("recipe_slot_prior") or 0.0),
                    str(item.get("id") or ""),
                ),
                reverse=True,
            )
            slots[slot_name] = candidates[:25]
        recipes[recipe_name] = {
            "strategy_weights": definition["strategy_weights"],
            "slots": slots,
        }
    return {
        "schema_version": "recipe_priors_v1",
        "note": (
            "These are static priors from catalog metadata, implementation mapping, "
            "and parameter-space size. They are not empirical P&L rankings."
        ),
        "sampling_policy": {
            "high_medium_prior_fraction": 0.80,
            "uncertain_prior_fraction": 0.15,
            "wild_exploration_fraction": 0.05,
        },
        "recipes": recipes,
    }


def _pair_score(anchor: dict[str, Any], trigger: dict[str, Any], anchor_type: str) -> tuple[float, str]:
    score = 35.0
    score += float(anchor.get("static_prior_score") or 0.0) * 0.20
    score += float(trigger.get("static_prior_score") or 0.0) * 0.30
    trigger_strategy = str(trigger.get("strategy_role") or "").lower()
    anchor_strategy = str(anchor.get("strategy_role") or "").lower()

    if anchor_type == "trend" and trigger_strategy in {"trend", "breakout", "confirm"}:
        score += 14.0
    elif anchor_type == "mean_reversion" and trigger_strategy == "mean-reversion":
        score += 14.0
    elif anchor_type == "compression" and trigger_strategy in {"breakout", "confirm"}:
        score += 14.0
    elif anchor_type == "profile_value" and trigger_strategy in {"mean-reversion", "confirm"}:
        score += 8.0

    if anchor_strategy and anchor_strategy == trigger_strategy:
        score += 4.0
    if anchor.get("base_indicator_id") and anchor.get("base_indicator_id") == trigger.get("base_indicator_id"):
        score -= 10.0

    score = round(max(0.0, min(100.0, score)), 2)
    label = "high" if score >= 72 else "medium" if score >= 55 else "low"
    return score, label


def build_anchor_pair_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(row.get("id") or ""): row for row in rows}
    triggers = [
        row
        for row in rows
        if row.get("generation_eligible") and str(row.get("signal_role") or "").lower() == "trigger"
    ]
    matrix: list[dict[str, Any]] = []
    for anchor_type, anchor_ids in DEFAULT_ANCHORS.items():
        for anchor_id in anchor_ids:
            anchor = by_id.get(anchor_id)
            if not anchor or not anchor.get("generation_eligible"):
                continue
            for trigger in triggers:
                trigger_id = str(trigger.get("id") or "")
                if trigger_id == anchor_id:
                    continue
                score, label = _pair_score(anchor, trigger, anchor_type)
                matrix.append(
                    {
                        "anchor_type": anchor_type,
                        "anchor_id": anchor_id,
                        "anchor_signal_role": anchor.get("signal_role"),
                        "anchor_strategy_role": anchor.get("strategy_role"),
                        "trigger_id": trigger_id,
                        "trigger_strategy_role": trigger.get("strategy_role"),
                        "compatibility_prior_score": score,
                        "compatibility_label": label,
                        "shared_base_indicator": bool(
                            anchor.get("base_indicator_id")
                            and anchor.get("base_indicator_id")
                            == trigger.get("base_indicator_id")
                        ),
                    }
                )
    return sorted(
        matrix,
        key=lambda item: (
            str(item.get("anchor_type") or ""),
            -float(item.get("compatibility_prior_score") or 0.0),
            str(item.get("trigger_id") or ""),
        ),
    )


def _write_indicator_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "name",
        "namespace",
        "talib_function",
        "base_indicator_id",
        "strategy_role",
        "signal_role",
        "signal_persistence",
        "preferred_timeframe_role",
        "known_implementation_class",
        "generation_eligible",
        "expected_scaffoldable",
        "verified_scaffoldable",
        "talib_parameter_count",
        "sweepable_parameter_count",
        "sweepable_parameters",
        "parameter_cardinality_known",
        "theoretical_parameter_cardinality",
        "static_prior_score",
        "static_prior_bucket",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = {field: row.get(field) for field in fieldnames}
            out["sweepable_parameters"] = ",".join(row.get("sweepable_parameters") or [])
            writer.writerow(out)


def _write_pair_matrix_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "anchor_type",
        "anchor_id",
        "anchor_signal_role",
        "anchor_strategy_role",
        "trigger_id",
        "trigger_strategy_role",
        "compatibility_prior_score",
        "compatibility_label",
        "shared_base_indicator",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_indicator_atlas(
    config: AppConfig,
    *,
    workspace_root: Path | None = None,
    catalog_path: Path | None = None,
    out_dir: Path | None = None,
) -> IndicatorAtlasBuildResult:
    catalog_payload, resolved_workspace_root, resolved_catalog_path = load_indicator_catalog(
        config=config,
        workspace_root=workspace_root,
        catalog_path=catalog_path,
    )
    factory_path = (
        _factory_path_for_workspace(resolved_workspace_root)
        if resolved_workspace_root is not None
        else None
    )
    implementation_by_id = parse_indicator_factory_mapping(factory_path)
    source_file_by_id = _source_file_index(resolved_workspace_root)
    rows = build_indicator_rows(
        catalog_payload,
        implementation_by_id=implementation_by_id,
        source_file_by_id=source_file_by_id,
    )
    summary = build_indicator_summary(rows)
    dependencies = build_indicator_dependencies(rows)
    recipe_priors = build_recipe_priors(rows)
    pair_matrix = build_anchor_pair_matrix(rows)

    target_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else config.derived_root / DEFAULT_ATLAS_DIRNAME
    )
    atlas_path = target_dir / "indicator-atlas.json"
    csv_path = target_dir / "indicator-atlas.csv"
    dependencies_path = target_dir / "indicator-dependencies.json"
    pair_matrix_path = target_dir / "indicator-pair-matrix.csv"
    recipe_priors_path = target_dir / "recipe-priors.json"

    atlas_payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "workspace_root": str(resolved_workspace_root) if resolved_workspace_root else None,
            "catalog_path": str(resolved_catalog_path),
            "factory_path": str(factory_path) if factory_path else None,
        },
        "summary": summary,
        "timeframes": catalog_payload.get("timeframes"),
        "anchors": DEFAULT_ANCHORS,
        "indicators": rows,
    }
    _write_json(atlas_path, atlas_payload)
    _write_indicator_csv(csv_path, rows)
    _write_json(dependencies_path, dependencies)
    _write_pair_matrix_csv(pair_matrix_path, pair_matrix)
    _write_json(recipe_priors_path, recipe_priors)

    return IndicatorAtlasBuildResult(
        atlas_path=atlas_path,
        csv_path=csv_path,
        dependencies_path=dependencies_path,
        pair_matrix_path=pair_matrix_path,
        recipe_priors_path=recipe_priors_path,
        summary=summary,
    )
