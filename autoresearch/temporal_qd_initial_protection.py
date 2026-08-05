"""Frozen initial-protection vocabulary for bidirectional QD search.

This module deliberately authors only source-profile JSON.  The Dashboard
native validator remains the authority for management-model parsing, binding
reachability, and executable protection geometry.  Keeping this surface here
makes the search vocabulary explicit without creating a second implementation
of the execution engine.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any

from .temporal_bidirectional_genome import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError


INITIAL_PROTECTION_POLICY_SCHEMA = "temporal_qd_initial_protection_policy_v2"


def default_initial_protection_policy() -> dict[str, Any]:
    """Return the closed, coarse search vocabulary for initial protection.

    The values intentionally form a multiplicative-ish grid.  Fine decimal
    sweeps would add mostly correlated candidates while making restart audits
    and G0 coverage harder to reason about.
    """

    return {
        "schemaVersion": INITIAL_PROTECTION_POLICY_SCHEMA,
        "stopPercentChoices": [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0],
        "rewardMultipleChoices": [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0],
        "targetPercentChoices": [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0],
        "distanceMultipleChoices": [0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0],
        # Conditional on selecting initial protection, choose adjacent edits
        # most often.  The pair proposal records the exact selected plan, so
        # replay never depends on random state.
        "mutationClassWeights": {
            "adjacent": 70,
            "jump": 25,
            "kind_switch": 5,
        },
        "immigrantModes": [
            "coupled_reward_multiple",
            "decoupled_fixed_percent",
            "no_fixed_target",
            "dynamic_catalog_authorized",
        ],
    }


def validate_initial_protection_policy(value: Mapping[str, Any]) -> dict[str, Any]:
    policy = copy.deepcopy(dict(value))
    expected = default_initial_protection_policy()
    if policy != expected:
        raise TemporalDiscoveryContractError(
            "initial protection operator policy is not the closed admitted policy"
        )
    return policy


def _library(profile: Mapping[str, Any]) -> Mapping[str, Any]:
    execution = profile.get("executionConfig")
    library = execution.get("managementLibrary") if isinstance(execution, Mapping) else None
    if not isinstance(library, Mapping):
        raise TemporalDiscoveryContractError("profile has no explicit management library")
    plans = library.get("plans")
    if not isinstance(plans, list):
        raise TemporalDiscoveryContractError("management library plans are invalid")
    return library


def _plan(profile: Mapping[str, Any], plan_id: str) -> Mapping[str, Any]:
    matches = [item for item in _library(profile)["plans"] if isinstance(item, Mapping) and item.get("id") == plan_id]
    if len(matches) != 1:
        raise TemporalDiscoveryContractError("initial protection plan selector did not resolve one management plan")
    return matches[0]


def _classify_scalar(current: float, choices: Sequence[float], candidate: float) -> str:
    values = list(choices)
    try:
        index = values.index(float(current))
    except ValueError:
        return "jump"
    return "adjacent" if abs(values.index(float(candidate)) - index) == 1 else "jump"


def _binding_locators(profile: Mapping[str, Any], *, site: str, multiples: Sequence[float]) -> list[dict[str, Any]]:
    """Return dynamic locators backed by already declared native bindings.

    New scalar bindings are created through the existing construction operator
    in the pair bridge.  This helper intentionally never invents a binding or
    indicator output.
    """

    bindings = _library(profile).get("scalarBindings") or []
    result: list[dict[str, Any]] = []
    for binding in bindings:
        if not isinstance(binding, Mapping) or binding.get("availability") != "completed_bar":
            continue
        binding_id = binding.get("id")
        value_kind = binding.get("valueKind")
        if not isinstance(binding_id, str):
            continue
        if value_kind == "price_level":
            result.append({"kind": "indicator_price_level", "bindingId": binding_id})
        elif value_kind == "price_distance":
            result.extend(
                {
                    "kind": "indicator_distance_multiple",
                    "bindingId": binding_id,
                    "multiple": value,
                }
                for value in multiples
            )
    return result


def _binding_references(profile: Mapping[str, Any]) -> set[str]:
    """Find every management scalar binding referenced after a mutation.

    Keep this deliberately narrower than a whole-profile recursive scan: only
    canonical management-locator sites count, including later stop/target
    actions.  This avoids retaining dead bindings while never deleting one
    that a surviving plan or action still needs.
    """

    found: set[str] = set()

    def add(locator: Any) -> None:
        if isinstance(locator, Mapping) and locator.get("kind") in {
            "indicator_price_level",
            "indicator_distance_multiple",
        } and isinstance(locator.get("bindingId"), str):
            found.add(locator["bindingId"])

    library = _library(profile)
    for plan in library["plans"]:
        if not isinstance(plan, Mapping):
            continue
        add(plan.get("initialStop"))
        add(plan.get("initialTarget"))
        trailing = plan.get("trailingStop")
        if isinstance(trailing, Mapping):
            add(trailing.get("anchor"))
            add(trailing.get("distance"))
    graph = profile.get("graph")
    for transition in (graph.get("transitions") if isinstance(graph, Mapping) else []) or []:
        if not isinstance(transition, Mapping):
            continue
        for action in transition.get("actions") or []:
            if not isinstance(action, Mapping):
                continue
            add(action.get("stopLocator"))
            add(action.get("targetLocator"))
    return found


def _remove_unreferenced_scalar_bindings(profile: dict[str, Any]) -> list[dict[str, Any]]:
    library = profile["executionConfig"]["managementLibrary"]
    bindings = library.get("scalarBindings")
    if not isinstance(bindings, list):
        return []
    references = _binding_references(profile)
    removed = [copy.deepcopy(item) for item in bindings if isinstance(item, Mapping) and item.get("id") not in references]
    retained = [item for item in bindings if isinstance(item, Mapping) and item.get("id") in references]
    if removed:
        if retained:
            library["scalarBindings"] = retained
        else:
            library.pop("scalarBindings", None)
    return sorted(removed, key=lambda item: str(item.get("id") or ""))


def _replacement_rows(
    profile: Mapping[str, Any], *, plan_id: str, site: str, policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    plan = _plan(profile, plan_id)
    current = plan.get("initialStop" if site == "stop" else "initialTarget")
    if not isinstance(current, Mapping):
        return []
    current_kind = current.get("kind")
    rows: list[dict[str, Any]] = []
    if site == "stop":
        for value in policy["stopPercentChoices"]:
            candidate = {"kind": "fixed_percent", "percent": value}
            if candidate != current:
                mutation_class = (
                    _classify_scalar(float(current.get("percent")), policy["stopPercentChoices"], value)
                    if current_kind == "fixed_percent"
                    else "kind_switch"
                )
                rows.append({"replacement": candidate, "mutationClass": mutation_class})
    else:
        for value in policy["rewardMultipleChoices"]:
            candidate = {"kind": "reward_multiple", "multiple": value}
            if candidate != current:
                mutation_class = (
                    _classify_scalar(float(current.get("multiple")), policy["rewardMultipleChoices"], value)
                    if current_kind == "reward_multiple"
                    else "kind_switch"
                )
                rows.append({"replacement": candidate, "mutationClass": mutation_class})
        for value in policy["targetPercentChoices"]:
            candidate = {"kind": "fixed_percent", "percent": value}
            if candidate != current:
                mutation_class = (
                    _classify_scalar(float(current.get("percent")), policy["targetPercentChoices"], value)
                    if current_kind == "fixed_percent"
                    else "kind_switch"
                )
                rows.append({"replacement": candidate, "mutationClass": mutation_class})
        candidate = {"kind": "none"}
        if candidate != current:
            rows.append({"replacement": candidate, "mutationClass": "kind_switch"})
    for candidate in _binding_locators(
        profile, site=site, multiples=policy["distanceMultipleChoices"]
    ):
        if candidate != current:
            mutation_class = "kind_switch"
            if (
                current_kind == "indicator_distance_multiple"
                and candidate.get("kind") == "indicator_distance_multiple"
                and candidate.get("bindingId") == current.get("bindingId")
            ):
                mutation_class = _classify_scalar(
                    float(current.get("multiple")),
                    policy["distanceMultipleChoices"],
                    float(candidate["multiple"]),
                )
            rows.append({"replacement": candidate, "mutationClass": mutation_class})
    return rows


def enumerate_initial_protection_plans(
    profile: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Enumerate canonical, side-local stop/target replacements.

    These plans only use already declared scalar bindings.  The caller may add
    further dynamic-binding construction plans through the canonical v3
    construction operator.
    """

    frozen = validate_initial_protection_policy(policy)
    plans: dict[str, dict[str, Any]] = {}
    for item in _library(profile)["plans"]:
        if not isinstance(item, Mapping) or not isinstance(item.get("id"), str):
            continue
        for site in ("stop", "target"):
            for row in _replacement_rows(profile, plan_id=item["id"], site=site, policy=frozen):
                plan = {
                    "kind": "initial_protection",
                    "planId": item["id"],
                    "site": site,
                    "replacement": row["replacement"],
                    "mutationClass": row["mutationClass"],
                }
                plan["planSha256"] = canonical_sha256(plan)
                plans[plan["planSha256"]] = plan
    return [plans[key] for key in sorted(plans)]


def apply_initial_protection_plan(
    profile: Mapping[str, Any], plan: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply only an enumerated replacement and return an auditable trace."""

    canonical = next(
        (
            item
            for item in enumerate_initial_protection_plans(profile, policy)
            if item == dict(plan)
        ),
        None,
    )
    if canonical is None:
        raise TemporalDiscoveryContractError("initial protection plan is not canonical and applicable")
    child = copy.deepcopy(dict(profile))
    plans = child["executionConfig"]["managementLibrary"]["plans"]
    selected = next(item for item in plans if item.get("id") == canonical["planId"])
    key = "initialStop" if canonical["site"] == "stop" else "initialTarget"
    before = copy.deepcopy(selected[key])
    selected[key] = copy.deepcopy(canonical["replacement"])
    removed_bindings = _remove_unreferenced_scalar_bindings(child)
    audit = {
        "schemaVersion": "temporal_qd_initial_protection_application_v1",
        "planSha256": canonical["planSha256"],
        "managementPlanId": canonical["planId"],
        "site": canonical["site"],
        "mutationClass": canonical["mutationClass"],
        "before": before,
        "after": copy.deepcopy(canonical["replacement"]),
        "removedUnreferencedScalarBindings": removed_bindings,
    }
    audit["applicationSha256"] = canonical_sha256(audit)
    return child, audit


def immigrant_initial_protection_selector(
    *, policy: Mapping[str, Any], choose: Any, seed: str
) -> dict[str, Any]:
    """Select a stratified static G0 protection mode with a caller-supplied chooser."""

    frozen = validate_initial_protection_policy(policy)
    mode = choose(seed, axis="initial_protection_mode", values=frozen["immigrantModes"])
    if mode == "dynamic_catalog_authorized":
        # Dynamic construction chooses the concrete catalog-authorized scalar
        # (and, for distance scalars, its grid value) only after the profile is
        # known.  Do not add unused fixed-percent/R axes to this selector.
        return {
            "mode": mode,
            "dynamicSite": choose(
                seed,
                axis="initial_protection_dynamic_site",
                values=["initial_stop", "initial_target"],
            ),
        }
    stop = choose(seed, axis="initial_protection_stop_percent", values=frozen["stopPercentChoices"])
    target_values = (
        frozen["rewardMultipleChoices"]
        if mode == "coupled_reward_multiple"
        else frozen["targetPercentChoices"]
    )
    target = choose(seed, axis="initial_protection_target", values=target_values)
    result = {"mode": mode, "stopPercent": stop}
    if mode == "coupled_reward_multiple":
        result["rewardMultiple"] = target
    elif mode == "decoupled_fixed_percent":
        result["targetPercent"] = target
    return result


def apply_immigrant_initial_protection(
    profile: Mapping[str, Any], *, plan_id: str, selector: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply a single coupled or decoupled G0 plan atomically."""

    validate_initial_protection_policy(policy)
    mode = selector.get("mode")
    stop = selector.get("stopPercent")
    if mode not in {"coupled_reward_multiple", "decoupled_fixed_percent", "no_fixed_target"} or not isinstance(stop, (int, float)):
        raise TemporalDiscoveryContractError("initial protection immigrant selector is invalid")
    child = copy.deepcopy(dict(profile))
    plans = child["executionConfig"]["managementLibrary"]["plans"]
    selected = next((item for item in plans if item.get("id") == plan_id), None)
    if not isinstance(selected, dict):
        raise TemporalDiscoveryContractError("initial protection immigrant plan selector did not resolve one management plan")
    before = {
        "initialStop": copy.deepcopy(selected.get("initialStop")),
        "initialTarget": copy.deepcopy(selected.get("initialTarget")),
    }
    selected["initialStop"] = {"kind": "fixed_percent", "percent": float(stop)}
    if mode == "coupled_reward_multiple":
        target = selector.get("rewardMultiple")
        if not isinstance(target, (int, float)):
            raise TemporalDiscoveryContractError("coupled immigrant target selector is invalid")
        selected["initialTarget"] = {"kind": "reward_multiple", "multiple": float(target)}
    elif mode == "decoupled_fixed_percent":
        target = selector.get("targetPercent")
        if not isinstance(target, (int, float)):
            raise TemporalDiscoveryContractError("decoupled immigrant target selector is invalid")
        selected["initialTarget"] = {"kind": "fixed_percent", "percent": float(target)}
    else:
        selected["initialTarget"] = {"kind": "none"}
    audit = {
        "schemaVersion": "temporal_qd_initial_protection_immigrant_application_v1",
        "managementPlanId": plan_id,
        "selector": copy.deepcopy(dict(selector)),
        "before": before,
        "after": {
            "initialStop": copy.deepcopy(selected["initialStop"]),
            "initialTarget": copy.deepcopy(selected["initialTarget"]),
        },
    }
    audit["applicationSha256"] = canonical_sha256(audit)
    return child, audit


__all__ = [
    "INITIAL_PROTECTION_POLICY_SCHEMA",
    "apply_immigrant_initial_protection",
    "apply_initial_protection_plan",
    "default_initial_protection_policy",
    "enumerate_initial_protection_plans",
    "immigrant_initial_protection_selector",
    "validate_initial_protection_policy",
]
