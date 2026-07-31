from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
from typing import Any, Protocol

from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    validate_authority,
)

from .temporal_discovery_base import *

def _entry_context_options(profile: dict[str, Any]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for path, node in _walk(profile.get("graph", {}), ("graph",)):
        if not isinstance(node, dict):
            continue
        kind = node.get("kind")
        if kind in {"evidence_at_least", "evidence_below"}:
            current = node.get("thresholdPercent")
            for value in _THRESHOLD_GRID:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="entry_context",
                            operator="evidence_threshold",
                            path=(*path, "thresholdPercent"),
                            replacement=value,
                        )
                    )
        elif kind == "condition_streak_at_least":
            current = node.get("thresholdPercent")
            for value in _THRESHOLD_GRID:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="entry_context",
                            operator="streak_threshold",
                            path=(*path, "thresholdPercent"),
                            replacement=value,
                        )
                    )
            comparison = node.get("comparison")
            replacement = "below" if comparison == "at_least" else "at_least"
            options.append(
                _mutation(
                    family="entry_context",
                    operator="streak_comparison",
                    path=(*path, "comparison"),
                    replacement=replacement,
                )
            )
        elif kind == "utc_time_window":
            current_pair = (node.get("startMinute"), node.get("endMinute"))
            for start, end in _TIME_WINDOWS:
                if current_pair != (start, end):
                    replacement = dict(node)
                    replacement["startMinute"] = start
                    replacement["endMinute"] = end
                    options.append(
                        _mutation(
                            family="entry_context",
                            operator="utc_session",
                            path=path,
                            replacement=replacement,
                        )
                    )
    return options


def _graph_structure_options(profile: dict[str, Any]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for path, node in _walk(profile.get("graph", {}), ("graph",)):
        if not isinstance(node, dict):
            continue
        kind = node.get("kind")
        if kind in {
            "event_age_at_most",
            "state_age_at_least",
            "state_age_at_most",
        }:
            current = node.get("events")
            for value in _EVENT_GRID:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="graph_structure",
                            operator=f"{kind}_events",
                            path=(*path, "events"),
                            replacement=value,
                        )
                    )
        elif kind == "condition_streak_at_least":
            current = node.get("events")
            for value in _EVENT_GRID[1:]:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="graph_structure",
                            operator="condition_streak_events",
                            path=(*path, "events"),
                            replacement=value,
                        )
                    )
        elif kind == "position_age_at_least":
            current = node.get("events")
            for value in _POS_AGE_GRID:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="graph_structure",
                            operator="position_age_events",
                            path=(*path, "events"),
                            replacement=value,
                        )
                    )
        elif kind in {"unrealized_r_at_least", "unrealized_r_at_most"}:
            current = node.get("value")
            for value in _R_GRID:
                if _different(current, value):
                    options.append(
                        _mutation(
                            family="graph_structure",
                            operator=f"{kind}_value",
                            path=(*path, "value"),
                            replacement=value,
                        )
                    )
        elif kind in {"all", "any"} and isinstance(node.get("guards"), list):
            guards = node["guards"]
            if len(guards) >= 2:
                replacement = dict(node)
                replacement["kind"] = "any" if kind == "all" else "all"
                options.append(
                    _mutation(
                        family="graph_structure",
                        operator="boolean_mode",
                        path=path,
                        replacement=replacement,
                    )
                )
                for index in range(len(guards)):
                    reduced = dict(node)
                    reduced["guards"] = [
                        copy.deepcopy(item)
                        for item_index, item in enumerate(guards)
                        if item_index != index
                    ]
                    options.append(
                        _mutation(
                            family="graph_structure",
                            operator="drop_boolean_child",
                            path=path,
                            replacement=reduced,
                        )
                    )
        elif kind in {
            "evidence_at_least",
            "evidence_below",
            "fresh_event",
            "event_age_at_most",
            "condition_streak_at_least",
            "utc_time_window",
            "state_age_at_least",
            "state_age_at_most",
            "unrealized_r_at_least",
            "unrealized_r_at_most",
        }:
            parent = _get(profile, path[:-1]) if path else None
            if not (isinstance(parent, dict) and parent.get("kind") == "not"):
                options.append(
                    _mutation(
                        family="graph_structure",
                        operator="negate_guard",
                        path=path,
                        replacement={"kind": "not", "guard": copy.deepcopy(node)},
                    )
                )
    return options


def _target_replacements(current: dict[str, Any]) -> list[dict[str, Any]]:
    replacements: list[dict[str, Any]] = [{"kind": "none"}]
    replacements.extend(
        {"kind": "reward_multiple", "multiple": value}
        for value in _TARGET_R_GRID
    )
    replacements.extend(
        {"kind": "fixed_percent", "percent": value}
        for value in _TARGET_PERCENT_GRID
    )
    kind = current.get("kind")
    if kind == "indicator_distance_multiple":
        binding = current.get("bindingId")
        replacements.extend(
            {
                "kind": "indicator_distance_multiple",
                "bindingId": binding,
                "multiple": value,
            }
            for value in _DISTANCE_MULTIPLE_GRID
        )
    elif kind == "indicator_price_level":
        replacements.append(copy.deepcopy(current))
    return replacements


def _trailing_replacements(
    current: dict[str, Any] | None,
) -> list[dict[str, Any] | None]:
    replacements: list[dict[str, Any] | None] = [None]
    anchors = (
        {"kind": "bar_close"},
        {"kind": "favorable_bar_extreme"},
    )
    activations = (
        {"kind": "immediate"},
        {"kind": "after_unrealized_r", "value": 0.5},
        {"kind": "after_unrealized_r", "value": 1.0},
        {"kind": "after_position_age", "bars": 3},
        {"kind": "after_position_age", "bars": 8},
        {"kind": "after_r_and_age", "value": 1.0, "bars": 3},
        {"kind": "explicit"},
    )
    for anchor in anchors:
        for multiple in _TRAIL_R_GRID:
            for activation in activations:
                replacements.append(
                    {
                        "anchor": copy.deepcopy(anchor),
                        "distance": {
                            "kind": "fixed_initial_r",
                            "multiple": multiple,
                        },
                        "activation": copy.deepcopy(activation),
                        "minimumStepInitialR": 0.0,
                    }
                )
        for percent in _TRAIL_PERCENT_GRID:
            replacements.append(
                {
                    "anchor": copy.deepcopy(anchor),
                    "distance": {
                        "kind": "fixed_percent_of_entry",
                        "percent": percent,
                    },
                    "activation": {"kind": "immediate"},
                    "minimumStepInitialR": 0.0,
                }
            )

    if isinstance(current, dict):
        anchor = current.get("anchor")
        distance = current.get("distance")
        activation = current.get("activation")
        if isinstance(anchor, dict) and anchor.get("kind") == "indicator_price_level":
            for step in _MIN_STEP_GRID:
                replacement = copy.deepcopy(current)
                replacement["minimumStepInitialR"] = step
                replacements.append(replacement)
        if (
            isinstance(distance, dict)
            and distance.get("kind") == "indicator_distance_multiple"
        ):
            binding = distance.get("bindingId")
            for multiple in _DISTANCE_MULTIPLE_GRID:
                replacement = copy.deepcopy(current)
                replacement["distance"] = {
                    "kind": "indicator_distance_multiple",
                    "bindingId": binding,
                    "multiple": multiple,
                }
                replacements.append(replacement)
        if isinstance(activation, dict):
            for candidate in activations:
                replacement = copy.deepcopy(current)
                replacement["activation"] = copy.deepcopy(candidate)
                replacements.append(replacement)
        for step in _MIN_STEP_GRID:
            replacement = copy.deepcopy(current)
            replacement["minimumStepInitialR"] = step
            replacements.append(replacement)
    return replacements


def _management_closure_options(
    profile: dict[str, Any],
) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    config = profile.get("executionConfig")
    library = (
        config.get("managementLibrary")
        if isinstance(config, dict)
        else None
    )
    if isinstance(library, dict):
        plans = library.get("plans")
        if isinstance(plans, list):
            for plan_index, plan in enumerate(plans):
                if not isinstance(plan, dict):
                    continue
                base = (
                    "executionConfig",
                    "managementLibrary",
                    "plans",
                    plan_index,
                )
                stop = plan.get("initialStop")
                if isinstance(stop, dict):
                    kind = stop.get("kind")
                    if kind == "fixed_percent":
                        for value in _STOP_PERCENT_GRID:
                            if _different(stop.get("percent"), value):
                                options.append(
                                    _mutation(
                                        family="management_closure",
                                        operator="initial_stop_percent",
                                        path=(*base, "initialStop", "percent"),
                                        replacement=value,
                                    )
                                )
                    elif kind == "indicator_distance_multiple":
                        for value in _DISTANCE_MULTIPLE_GRID:
                            if _different(stop.get("multiple"), value):
                                options.append(
                                    _mutation(
                                        family="management_closure",
                                        operator="initial_stop_distance_multiple",
                                        path=(*base, "initialStop", "multiple"),
                                        replacement=value,
                                    )
                                )
                target = plan.get("initialTarget")
                if isinstance(target, dict):
                    for replacement in _target_replacements(target):
                        if _different(target, replacement):
                            options.append(
                                _mutation(
                                    family="management_closure",
                                    operator="initial_target",
                                    path=(*base, "initialTarget"),
                                    replacement=replacement,
                                )
                            )
                trailing = plan.get("trailingStop")
                for replacement in _trailing_replacements(
                    trailing if isinstance(trailing, dict) else None
                ):
                    if replacement is None:
                        if "trailingStop" in plan:
                            copy_plan = copy.deepcopy(plan)
                            copy_plan.pop("trailingStop", None)
                            options.append(
                                _mutation(
                                    family="management_closure",
                                    operator="remove_trailing",
                                    path=base,
                                    replacement=copy_plan,
                                )
                            )
                    elif _different(trailing, replacement):
                        options.append(
                            _mutation(
                                family="management_closure",
                                operator="trailing_policy",
                                path=(*base, "trailingStop"),
                                replacement=replacement,
                            )
                        )

    for path, node in _walk(profile.get("graph", {}), ("graph",)):
        if not isinstance(node, dict):
            continue
        kind = node.get("kind")
        if kind == "tighten_stop_next_open":
            locator = node.get("stopLocator")
            if isinstance(locator, dict):
                if locator.get("kind") == "initial_r_multiple":
                    for value in (-0.75, -0.5, -0.25, 0.0, 0.5, 1.0, 2.0):
                        if _different(locator.get("multiple"), value):
                            options.append(
                                _mutation(
                                    family="management_closure",
                                    operator="tighten_stop_r",
                                    path=(*path, "stopLocator", "multiple"),
                                    replacement=value,
                                )
                            )
                elif locator.get("kind") == "indicator_distance_multiple":
                    for value in _DISTANCE_MULTIPLE_GRID:
                        if _different(locator.get("multiple"), value):
                            options.append(
                                _mutation(
                                    family="management_closure",
                                    operator="tighten_stop_distance",
                                    path=(*path, "stopLocator", "multiple"),
                                    replacement=value,
                                )
                            )
        elif kind == "set_target_next_open":
            locator = node.get("targetLocator")
            if isinstance(locator, dict):
                for replacement in _target_replacements(locator):
                    if replacement.get("kind") != "none" and _different(
                        locator,
                        replacement,
                    ):
                        options.append(
                            _mutation(
                                family="management_closure",
                                operator="set_target_locator",
                                path=(*path, "targetLocator"),
                                replacement=replacement,
                            )
                        )
    return options


def _available_mutations(
    profile: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    grouped = {
        "entry_context": _entry_context_options(profile),
        "graph_structure": _graph_structure_options(profile),
        "management_closure": _management_closure_options(profile),
    }
    for family in grouped:
        dedup: dict[str, dict[str, Any]] = {}
        for option in grouped[family]:
            public = {key: value for key, value in option.items() if key != "_path"}
            dedup[canonical_sha256(public)] = option
        grouped[family] = [
            dedup[key] for key in sorted(dedup)
        ]
    return grouped


def _apply_option(
    profile: dict[str, Any],
    option: Mapping[str, Any],
) -> dict[str, Any]:
    path = option.get("_path")
    if not isinstance(path, tuple):
        raise TemporalDiscoveryContractError(
            "internal mutation path is missing"
        )
    output = _clone(profile, name="profile before mutation")
    _set(output, path, option["replacement"])
    return output


def _public_mutation(option: Mapping[str, Any], old_value: Any) -> dict[str, Any]:
    return {
        "family": option["family"],
        "operator": option["operator"],
        "path": option["path"],
        "priorSha256": canonical_sha256(old_value),
        "replacement": _clone(option["replacement"], name="replacement"),
    }


def _mutate_profile(
    seed_profile: Mapping[str, Any],
    *,
    rng: random.Random,
    source_mode: str,
    mutation_count: int,
    family_rotation: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    profile = _ensure_explicit_management(dict(seed_profile))
    trace: list[dict[str, Any]] = []
    families: list[str] = []
    if source_mode == "de_novo":
        families.extend(_MUTATION_FAMILIES)
        while len(families) < mutation_count:
            families.append(rng.choice(_MUTATION_FAMILIES))
        rng.shuffle(families)
    else:
        for offset in range(mutation_count):
            families.append(
                _MUTATION_FAMILIES[
                    (family_rotation + offset) % len(_MUTATION_FAMILIES)
                ]
            )

    used: set[str] = set()
    for requested_family in families:
        grouped = _available_mutations(profile)
        family_order = (
            requested_family,
            *(
                family
                for family in _MUTATION_FAMILIES
                if family != requested_family
            ),
        )
        candidates: list[dict[str, Any]] = []
        for family in family_order:
            candidates = [
                item
                for item in grouped[family]
                if canonical_sha256(
                    {key: value for key, value in item.items() if key != "_path"}
                )
                not in used
            ]
            if candidates:
                break
        if not candidates:
            break
        selected = candidates[rng.randrange(len(candidates))]
        identity = canonical_sha256(
            {key: value for key, value in selected.items() if key != "_path"}
        )
        used.add(identity)
        try:
            old_value = copy.deepcopy(_get(profile, selected["_path"]))
        except (KeyError, IndexError):
            old_value = {"__absent__": True}
        profile = _apply_option(profile, selected)
        trace.append(_public_mutation(selected, old_value))

    structure_identity = canonical_sha256(
        {
            "seedlessProfile": {
                key: value
                for key, value in profile.items()
                if key not in {"name", "description"}
            },
            "mutations": trace,
        }
    )
    profile["name"] = (
        "Temporal discovery "
        + structure_identity.removeprefix("sha256:")[:16]
    )
    profile["description"] = (
        "Deterministically generated temporal search candidate; "
        f"sourceMode={source_mode}; mutationCount={len(trace)}."
    )
    profile["isActive"] = False
    return profile, trace




__all__ = ['_entry_context_options', '_graph_structure_options', '_target_replacements', '_trailing_replacements', '_management_closure_options', '_available_mutations', '_apply_option', '_public_mutation', '_mutate_profile']
