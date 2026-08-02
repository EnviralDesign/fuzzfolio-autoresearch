"""Stage 5E7-v3 deterministic construction operators.

This module is deliberately independent from the v2 mutation family and its
continuation RNG.  The operators are small whole-program transactions: a
construction plan either produces a complete management/graph change or is
not enumerated at all.  They use the generic structural-operator identities
and keep the catalog contract explicit so a caller cannot invent scalar
outputs or timeframes.
"""

from __future__ import annotations

import copy
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    canonical_sha256,
)
from .temporal_search_policy_v2 import inspect_management_reachability
from .temporal_structural_operators import (
    StructuralOperatorRegistry,
    finalize_application,
    finalize_audit,
    finalize_plan,
)


GENERATOR_V3_VERSION = "temporal_discovery_generator_v3_construction"
CONSTRUCTION_OPERATOR_VERSION = "1"
CONSTRUCTION_REACHABILITY_SCHEMA = "temporal_construction_reachability_v3"
CONSTRUCTION_OPERATOR_SPEC_SCHEMA = "temporal_construction_operator_spec_v1"
MAX_MANAGEMENT_PLANS = 16
MAX_SCALAR_BINDINGS = 32

SCALAR_DYNAMIC_MANAGEMENT = "scalar_dynamic_management_v3"
MANAGEMENT_PLAN = "management_plan_v3"
DIRECTION_FLIP = "direction_flip_v3"
GRAPH_BOUND_TIMEFRAME = "graph_bound_indicator_timeframe_v3"
INDICATOR_FAMILY_SUBSTITUTION = "indicator_family_substitution_v3"


def _catalog_items(raw: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    items = raw.get("indicators")
    if not isinstance(items, list):
        raise TemporalDiscoveryContractError("generator-v3 catalog requires indicators")
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping):
            continue
        meta = dict(item["meta"])
        indicator_id = str(meta.get("id") or "").strip()
        if not indicator_id or indicator_id in result:
            raise TemporalDiscoveryContractError("generator-v3 catalog indicator IDs must be unique")
        result[indicator_id] = meta
    if not result:
        raise TemporalDiscoveryContractError("generator-v3 catalog has no indicators")
    return result


class ConstructionCatalog:
    """A compact, identity-bound view of the Dashboard indicator catalog."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        value = _clone(payload, name="generator-v3 catalog")
        frames = value.get("timeframes")
        if not isinstance(frames, Mapping) or not frames:
            raise TemporalDiscoveryContractError("generator-v3 catalog requires timeframes")
        self.payload = value
        self.indicators = _catalog_items(value)
        self.timeframes = tuple(sorted(str(key).upper() for key in frames if str(key).strip()))
        if not self.timeframes:
            raise TemporalDiscoveryContractError("generator-v3 catalog has no timeframes")
        self.catalog_sha256 = canonical_sha256(value)

    def scalar_outputs(self, indicator_id: str) -> tuple[dict[str, str], ...]:
        meta = self.indicators.get(indicator_id)
        if meta is None:
            return ()
        outputs = meta.get("managementScalarOutputs")
        if not isinstance(outputs, list):
            return ()
        normalized: list[dict[str, str]] = []
        for item in outputs:
            if not isinstance(item, Mapping):
                continue
            key = str(item.get("outputKey") or "").strip()
            value_kind = str(item.get("valueKind") or "").strip()
            unit = str(item.get("unit") or "").strip()
            expected_unit = "price" if value_kind == "price_level" else "price_distance"
            if key and value_kind in {"price_level", "price_distance"} and unit == expected_unit:
                normalized.append(
                    {"outputKey": key, "valueKind": value_kind, "unit": unit}
                )
        return tuple(sorted(normalized, key=lambda item: (item["outputKey"], item["valueKind"])))


def _library(profile: Mapping[str, Any]) -> dict[str, Any] | None:
    config = profile.get("executionConfig")
    value = config.get("managementLibrary") if isinstance(config, Mapping) else None
    return dict(value) if isinstance(value, Mapping) else None


def _plans(library: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = library.get("plans")
    return [dict(item) for item in value] if isinstance(value, list) and all(isinstance(item, Mapping) for item in value) else []


def _plan_by_id(profile: Mapping[str, Any], plan_id: str) -> dict[str, Any] | None:
    library = _library(profile)
    if library is None:
        return None
    # Return the actual plan node when called against a mutable child; callers
    # use this helper to apply a transaction, not merely inspect a snapshot.
    values = library.get("plans")
    if not isinstance(values, list):
        return None
    return next(
        (item for item in values if isinstance(item, dict) and item.get("id") == plan_id),
        None,
    )


def _entry_actions(profile: Mapping[str, Any]) -> Iterable[tuple[int, int, Mapping[str, Any]]]:
    graph = profile.get("graph")
    transitions = graph.get("transitions") if isinstance(graph, Mapping) else None
    if not isinstance(transitions, list):
        return
    for transition_index, transition in enumerate(transitions):
        if not isinstance(transition, Mapping):
            continue
        actions = transition.get("actions")
        if not isinstance(actions, list):
            continue
        for action_index, action in enumerate(actions):
            if isinstance(action, Mapping) and action.get("kind") == "enter_next_open":
                yield transition_index, action_index, action


def _binding_locator_references(profile: Mapping[str, Any]) -> Counter[str]:
    refs: Counter[str] = Counter()

    def add(locator: Any) -> None:
        if isinstance(locator, Mapping) and locator.get("kind") in {
            "indicator_price_level",
            "indicator_distance_multiple",
        }:
            binding_id = str(locator.get("bindingId") or "")
            if binding_id:
                refs[binding_id] += 1

    library = _library(profile)
    if library is not None:
        for plan in _plans(library):
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
            if isinstance(action, Mapping) and action.get("kind") == "set_target_next_open":
                add(action.get("targetLocator"))
    return refs


def _v3_management_reachability(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Static checks extending v2 with scalar binding closure.

    ``defaultPlanId`` is an execution reference even when current entry actions
    explicitly select another plan.  v2's historical orphan report intentionally
    does not count it; v3 does so that plan creation can be constructive without
    faking duplicate entry routes.
    """

    library = _library(profile)
    issues: list[str] = []
    plan_ids: set[str] = set()
    referenced_plans: set[str] = set()
    binding_ids: set[str] = set()
    referenced_bindings = _binding_locator_references(profile)
    if library is None:
        issues.append("missing_management_library")
    else:
        plans = _plans(library)
        if not 1 <= len(plans) <= MAX_MANAGEMENT_PLANS:
            issues.append("management_plan_count_out_of_bounds")
        plan_ids = {str(plan.get("id") or "") for plan in plans}
        if "" in plan_ids or len(plan_ids) != len(plans):
            issues.append("management_plan_ids_invalid")
        default_id = str(library.get("defaultPlanId") or "")
        if default_id not in plan_ids:
            issues.append("default_management_plan_unknown")
        else:
            referenced_plans.add(default_id)
        entries = list(_entry_actions(profile))
        if not entries:
            issues.append("no_entry_route")
        for _transition_index, _action_index, action in entries:
            selected = str(action.get("managementPlanId") or default_id)
            if selected not in plan_ids:
                issues.append("entry_route_unknown_management_plan")
            else:
                referenced_plans.add(selected)
        if plan_ids - referenced_plans:
            issues.append("orphan_management_plan")

        bindings = library.get("scalarBindings") or []
        if not isinstance(bindings, list) or len(bindings) > MAX_SCALAR_BINDINGS:
            issues.append("management_scalar_binding_count_out_of_bounds")
            bindings = []
        sources: set[tuple[str, str]] = set()
        for binding in bindings:
            if not isinstance(binding, Mapping):
                issues.append("management_scalar_binding_invalid")
                continue
            binding_id = str(binding.get("id") or "")
            source = (str(binding.get("indicatorInstanceId") or ""), str(binding.get("outputKey") or ""))
            kind = str(binding.get("valueKind") or "")
            if not binding_id or binding_id in binding_ids or not all(source) or kind not in {"price_level", "price_distance"}:
                issues.append("management_scalar_binding_invalid")
                continue
            binding_ids.add(binding_id)
            if source in sources:
                issues.append("duplicate_management_scalar_source")
            sources.add(source)
            if binding.get("availability", "completed_bar") != "completed_bar":
                issues.append("management_scalar_binding_not_completed_bar")
        if set(referenced_bindings) - binding_ids:
            issues.append("management_scalar_binding_missing")
        if binding_ids - set(referenced_bindings):
            issues.append("orphan_management_scalar_binding")

    # Preserve v2 graph reachability diagnostics other than its older default
    # plan orphan convention.  This makes construction fail closed on dormant
    # management actions, unknown plans, or absent fill paths.
    v2 = inspect_management_reachability(profile)
    for code, count in (v2.get("issueCounts") or {}).items():
        if code != "orphan_management_plan":
            issues.extend([str(code)] * int(count))
    counts = dict(sorted(Counter(issues).items()))
    report = {
        "schemaVersion": CONSTRUCTION_REACHABILITY_SCHEMA,
        "generatorVersion": GENERATOR_V3_VERSION,
        "acceptable": not counts,
        "managementPlanIds": sorted(plan_ids),
        "referencedManagementPlanIds": sorted(referenced_plans),
        "orphanManagementPlanIds": sorted(plan_ids - referenced_plans),
        "managementScalarBindingIds": sorted(binding_ids),
        "referencedScalarBindingIds": sorted(referenced_bindings),
        "orphanScalarBindingIds": sorted(binding_ids - set(referenced_bindings)),
        "issueCounts": counts,
        "v2ReachabilitySha256": v2["reachabilitySha256"],
    }
    report["reachabilitySha256"] = canonical_sha256(report)
    return report


def inspect_construction_reachability(profile: Mapping[str, Any]) -> dict[str, Any]:
    return _v3_management_reachability(profile)


def _graph_bound_instances(profile: Mapping[str, Any]) -> set[str]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        return set()
    bound: set[str] = set()
    for group in graph.get("evidenceGroups") or []:
        if isinstance(group, Mapping):
            bound.update(str(value) for value in group.get("indicatorInstanceIds") or [] if str(value))
    for binding in graph.get("eventBindings") or []:
        if isinstance(binding, Mapping) and binding.get("indicatorInstanceId"):
            bound.add(str(binding["indicatorInstanceId"]))
    return bound


def _scalar_authorizations(profile: Mapping[str, Any], catalog: ConstructionCatalog) -> list[dict[str, str]]:
    library = _library(profile)
    existing_sources = {
        (str(item.get("indicatorInstanceId") or ""), str(item.get("outputKey") or ""))
        for item in ((library.get("scalarBindings") or []) if library else [])
        if isinstance(item, Mapping)
    }
    result: list[dict[str, str]] = []
    for indicator in profile.get("indicators") or []:
        if not isinstance(indicator, Mapping):
            continue
        meta = indicator.get("meta")
        config = indicator.get("config")
        if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
            continue
        indicator_id = str(meta.get("id") or "")
        instance_id = str(meta.get("instanceId") or "")
        if not indicator_id or not instance_id or config.get("isActive") is not True or config.get("useFormingBar") is not False:
            continue
        authored = meta.get("managementScalarOutputs")
        if not isinstance(authored, list):
            continue
        authored_tokens = {
            (str(item.get("outputKey") or ""), str(item.get("valueKind") or ""), str(item.get("unit") or ""))
            for item in authored if isinstance(item, Mapping)
        }
        for output in catalog.scalar_outputs(indicator_id):
            token = (output["outputKey"], output["valueKind"], output["unit"])
            if token not in authored_tokens or (instance_id, output["outputKey"]) in existing_sources:
                continue
            binding_id = f"scalar_{instance_id}_{output['outputKey']}"
            existing_ids = {
                str(item.get("id") or "")
                for item in ((library.get("scalarBindings") or []) if library else [])
                if isinstance(item, Mapping)
            }
            if binding_id in existing_ids:
                continue
            result.append({
                "indicatorId": indicator_id,
                "indicatorInstanceId": instance_id,
                "outputKey": output["outputKey"],
                "valueKind": output["valueKind"],
                "bindingId": binding_id,
            })
    return sorted(result, key=lambda item: (item["indicatorInstanceId"], item["outputKey"], item["valueKind"]))


def _locator_for(kind: str, binding_id: str, *, multiple: float = 1.0) -> dict[str, Any]:
    if kind == "price_level":
        return {"kind": "indicator_price_level", "bindingId": binding_id}
    return {"kind": "indicator_distance_multiple", "bindingId": binding_id, "multiple": multiple}


def _plan_locator_sites(plan: Mapping[str, Any], value_kind: str) -> list[dict[str, Any]]:
    sites = [
        {"path": ["initialStop"], "multiple": 1.0, "site": "initial_stop"},
        {"path": ["initialTarget"], "multiple": 2.0, "site": "initial_target"},
    ]
    trailing = plan.get("trailingStop")
    if value_kind == "price_level":
        if isinstance(trailing, Mapping):
            sites.append({"path": ["trailingStop", "anchor"], "multiple": 1.0, "site": "trailing_anchor"})
        else:
            sites.append({"path": ["trailingStop"], "multiple": 1.0, "site": "trailing_anchor_create"})
    else:
        if isinstance(trailing, Mapping):
            sites.append({"path": ["trailingStop", "distance"], "multiple": 1.0, "site": "trailing_distance"})
        else:
            sites.append({"path": ["trailingStop"], "multiple": 1.0, "site": "trailing_distance_create"})
    return sites


def _get_path(root: Mapping[str, Any], path: Sequence[str]) -> Any:
    value: Any = root
    for part in path:
        value = value[part]
    return value


def _set_path(root: dict[str, Any], path: Sequence[str], value: Any) -> None:
    target: Any = root
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = _clone(value, name="construction replacement")


def _remove_unreferenced_bindings(profile: dict[str, Any]) -> list[dict[str, Any]]:
    library = _library(profile)
    if library is None:
        return []
    refs = _binding_locator_references(profile)
    bindings = library.get("scalarBindings") or []
    removed = [dict(item) for item in bindings if isinstance(item, Mapping) and not refs.get(str(item.get("id") or ""))]
    if removed:
        profile["executionConfig"]["managementLibrary"]["scalarBindings"] = [
            dict(item) for item in bindings
            if isinstance(item, Mapping) and refs.get(str(item.get("id") or ""))
        ]
    return sorted(removed, key=lambda item: str(item.get("id") or ""))


class _ConstructionOperator:
    operator_id = ""
    operator_version = CONSTRUCTION_OPERATOR_VERSION

    def __init__(self, catalog: ConstructionCatalog) -> None:
        self.catalog = catalog
        spec = {
            "schemaVersion": CONSTRUCTION_OPERATOR_SPEC_SCHEMA,
            "operatorId": self.operator_id,
            "operatorVersion": self.operator_version,
            "generatorVersion": GENERATOR_V3_VERSION,
            "catalogSha256": catalog.catalog_sha256,
        }
        spec["operatorSpecSha256"] = canonical_sha256(spec)
        self.specification = spec

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        raise NotImplementedError

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        parent = _clone(profile, name="construction parent profile")
        parent_sha = canonical_sha256(parent)
        plans: dict[str, dict[str, Any]] = {}
        for construction in self._constructions(parent):
            construction = _clone(construction, name="construction plan")
            identity = {
                "schemaVersion": "temporal_construction_identity_v1",
                "generatorVersion": GENERATOR_V3_VERSION,
                "operatorId": self.operator_id,
                "operatorVersion": self.operator_version,
                "parentSourceProfileSha256": parent_sha,
                "catalogSha256": self.catalog.catalog_sha256,
                "construction": construction,
            }
            plan = finalize_plan({
                "operatorId": self.operator_id,
                "operatorVersion": self.operator_version,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "parentSourceProfileSha256": parent_sha,
                "catalogSha256": self.catalog.catalog_sha256,
                "construction": construction,
                "constructionIdentitySha256": canonical_sha256(identity),
            })
            plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def _preview(self, profile: Mapping[str, Any], plan: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        parent = _clone(profile, name="construction parent profile")
        selected = _clone(plan, name="construction plan")
        if selected not in self.enumerate_plans(parent):
            raise TemporalDiscoveryContractError(f"{self.operator_id} plan is not canonical and applicable")
        return self._transform(parent, selected["construction"])

    def preview(self, profile: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
        return self._preview(profile, plan)[0]

    def apply(self, profile: Mapping[str, Any], plan: Mapping[str, Any], *, parent_validated_program_sha256: str, child_validated_program_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
        child, trace = self._preview(profile, plan)
        reachability = _v3_management_reachability(child)
        checks = {
            "parent_identity_bound": canonical_sha256(profile) == plan["parentSourceProfileSha256"],
            "catalog_identity_bound": plan["catalogSha256"] == self.catalog.catalog_sha256,
            "construction_identity_bound": plan["constructionIdentitySha256"] == canonical_sha256({
                "schemaVersion": "temporal_construction_identity_v1",
                "generatorVersion": GENERATOR_V3_VERSION,
                "operatorId": self.operator_id,
                "operatorVersion": self.operator_version,
                "parentSourceProfileSha256": plan["parentSourceProfileSha256"],
                "catalogSha256": self.catalog.catalog_sha256,
                "construction": plan["construction"],
            }),
            "management_and_binding_reachability": reachability["acceptable"] is True,
        }
        audit = finalize_audit(
            checks,
            operatorId=self.operator_id,
            operatorVersion=self.operator_version,
            generatorVersion=GENERATOR_V3_VERSION,
            planSha256=plan["planSha256"],
            constructionIdentitySha256=plan["constructionIdentitySha256"],
            childSourceProfileSha256=canonical_sha256(child),
            reachability=reachability,
        )
        application = finalize_application({
            "operatorId": self.operator_id,
            "operatorVersion": self.operator_version,
            "generatorVersion": GENERATOR_V3_VERSION,
            "operatorSpecSha256": self.specification["operatorSpecSha256"],
            "planSha256": plan["planSha256"],
            "constructionIdentitySha256": plan["constructionIdentitySha256"],
            "parentSourceProfileSha256": canonical_sha256(profile),
            "childSourceProfileSha256": canonical_sha256(child),
            "parentValidatedProgramSha256": parent_validated_program_sha256,
            "childValidatedProgramSha256": child_validated_program_sha256,
            "mutationTrace": trace,
            "staticInvariantReport": audit,
            "evidenceScope": {
                "marketReplayRun": False,
                "firedEvidence": "unmeasured",
                "activationEvidence": "unmeasured",
                # Candidate-bound evidence plans are regenerated by the QD
                # campaign from the changed source profile; do not reuse a
                # lake scope whose graph-bound indicator timeframe changed.
                "evidencePlanRotationRequired": self.operator_id == GRAPH_BOUND_TIMEFRAME,
                "lakeScopeRegenerationRequired": self.operator_id == GRAPH_BOUND_TIMEFRAME,
            },
        })
        return child, application

    def audit(self, parent_profile: Mapping[str, Any], transformed_profile: Mapping[str, Any], application_record: Mapping[str, Any]) -> dict[str, Any]:
        application = _clone(application_record, name="construction application")
        application_identity = application.pop("applicationSha256", None)
        plan = next((item for item in self.enumerate_plans(parent_profile) if item["planSha256"] == application.get("planSha256")), None)
        exact_child = None
        exact_trace: list[dict[str, Any]] = []
        if plan is not None:
            exact_child, exact_trace = self._preview(parent_profile, plan)
        checks = {
            "application_identity_exact": isinstance(application_identity, str) and canonical_sha256(application) == application_identity,
            "operator_identity_exact": application.get("operatorId") == self.operator_id and application.get("operatorVersion") == self.operator_version,
            "plan_is_currently_applicable": plan is not None,
            "transformed_profile_exact": exact_child == transformed_profile,
            "mutation_trace_exact": application.get("mutationTrace") == exact_trace,
            "reachability_exact": _v3_management_reachability(transformed_profile).get("acceptable") is True,
        }
        return finalize_audit(checks, operatorId=self.operator_id, applicationSha256=application_identity)


class ScalarDynamicManagementConstructionOperator(_ConstructionOperator):
    operator_id = SCALAR_DYNAMIC_MANAGEMENT

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        library = _library(profile)
        if library is None or not _v3_management_reachability(profile)["acceptable"]:
            return []
        if len(library.get("scalarBindings") or []) >= MAX_SCALAR_BINDINGS:
            return []
        referenced = _v3_management_reachability(profile)["referencedManagementPlanIds"]
        for scalar in _scalar_authorizations(profile, self.catalog):
            for plan_id in referenced:
                plan = _plan_by_id(profile, plan_id)
                if plan is None:
                    continue
                for site in _plan_locator_sites(plan, scalar["valueKind"]):
                    yield {
                        "kind": "scalar_dynamic_management",
                        "scalar": scalar,
                        "planId": plan_id,
                        "site": site["site"],
                        "locatorPath": site["path"],
                        "multiple": site["multiple"],
                    }

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        child = copy.deepcopy(profile)
        scalar = construction["scalar"]
        plan_id = str(construction["planId"])
        plan = _plan_by_id(child, plan_id)
        if plan is None:
            raise TemporalDiscoveryContractError("construction target plan disappeared")
        path = [str(item) for item in construction["locatorPath"]]
        site = str(construction["site"])
        locator = _locator_for(str(scalar["valueKind"]), str(scalar["bindingId"]), multiple=float(construction["multiple"]))
        if site == "trailing_anchor_create":
            replacement = {"anchor": locator, "distance": {"kind": "fixed_initial_r", "multiple": 1.0}, "activation": {"kind": "immediate"}, "minimumStepInitialR": 0.0}
            before: Any = plan.get("trailingStop", {"__absent__": True})
        elif site == "trailing_distance_create":
            replacement = {"anchor": {"kind": "bar_close"}, "distance": locator, "activation": {"kind": "immediate"}, "minimumStepInitialR": 0.0}
            before = plan.get("trailingStop", {"__absent__": True})
        else:
            replacement = locator
            before = _get_path(plan, path)
        _set_path(plan, path, replacement)
        bindings = child["executionConfig"]["managementLibrary"].setdefault("scalarBindings", [])
        binding = {"id": scalar["bindingId"], "indicatorInstanceId": scalar["indicatorInstanceId"], "outputKey": scalar["outputKey"], "valueKind": scalar["valueKind"], "availability": "completed_bar"}
        bindings.append(binding)
        bindings.sort(key=lambda item: str(item["id"]))
        trace = [
            {"operation": "replace_locator", "planId": plan_id, "path": "/" + "/".join(path), "before": _clone(before, name="prior locator"), "after": _clone(replacement, name="dynamic locator")},
            {"operation": "add_scalar_binding", "binding": binding},
        ]
        for removed in _remove_unreferenced_bindings(child):
            trace.append({"operation": "delete_unreferenced_scalar_binding", "binding": removed})
        return child, trace


class ManagementPlanConstructionOperator(_ConstructionOperator):
    operator_id = MANAGEMENT_PLAN

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        library = _library(profile)
        if library is None or not _v3_management_reachability(profile)["acceptable"]:
            return []
        plans = _plans(library)
        entries = sorted(_entry_actions(profile), key=lambda item: (str(profile["graph"]["transitions"][item[0]].get("id") or ""), item[1]))
        if entries and len(plans) < MAX_MANAGEMENT_PLANS:
            suffix = canonical_sha256({"parent": canonical_sha256(profile), "operator": self.operator_id, "kind": "create"}).removeprefix("sha256:")[:12]
            plan_id = f"constructed_plan_{suffix}"
            if plan_id not in {str(item.get("id") or "") for item in plans}:
                transition_index, action_index, _action = entries[0]
                yield {"kind": "create_plan", "plan": {"id": plan_id, "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}}, "entryTransitionIndex": transition_index, "entryActionIndex": action_index}
        if len(plans) > 1:
            plan_ids = sorted(str(item.get("id") or "") for item in plans)
            for deleted in plan_ids:
                replacement = next(value for value in plan_ids if value != deleted)
                candidate = {
                    "kind": "delete_plan",
                    "deletedPlanId": deleted,
                    "replacementPlanId": replacement,
                    "rewriteDefault": str(library.get("defaultPlanId") or "") == deleted,
                }
                child, _trace = self._transform(profile, candidate)
                if _v3_management_reachability(child)["acceptable"]:
                    yield candidate

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        child = copy.deepcopy(profile)
        library = child["executionConfig"]["managementLibrary"]
        trace: list[dict[str, Any]] = []
        if construction["kind"] == "create_plan":
            plan = _clone(construction["plan"], name="constructed management plan")
            library["plans"].append(plan)
            library["plans"].sort(key=lambda item: str(item["id"]))
            action = child["graph"]["transitions"][int(construction["entryTransitionIndex"])]["actions"][int(construction["entryActionIndex"])]
            before = action.get("managementPlanId", {"__absent__": True})
            action["managementPlanId"] = plan["id"]
            trace.extend((
                {"operation": "create_management_plan", "plan": plan},
                {"operation": "rewrite_enter_management_plan", "transitionId": child["graph"]["transitions"][int(construction["entryTransitionIndex"])]["id"], "actionIndex": int(construction["entryActionIndex"]), "before": before, "after": plan["id"]},
                {"operation": "preserve_default_management_plan", "planId": library["defaultPlanId"]},
            ))
        elif construction["kind"] == "delete_plan":
            deleted = str(construction["deletedPlanId"])
            replacement = str(construction["replacementPlanId"])
            old_plan = next(item for item in library["plans"] if item.get("id") == deleted)
            library["plans"] = [item for item in library["plans"] if item.get("id") != deleted]
            if library.get("defaultPlanId") == deleted:
                library["defaultPlanId"] = replacement
                trace.append({"operation": "rewrite_default_management_plan", "before": deleted, "after": replacement})
            for transition in child["graph"]["transitions"]:
                for action_index, action in enumerate(transition.get("actions") or []):
                    if isinstance(action, dict) and action.get("kind") == "enter_next_open" and action.get("managementPlanId") == deleted:
                        action["managementPlanId"] = replacement
                        trace.append({"operation": "rewrite_enter_management_plan", "transitionId": transition.get("id"), "actionIndex": action_index, "before": deleted, "after": replacement})
            trace.insert(0, {"operation": "delete_management_plan", "plan": old_plan})
            for removed in _remove_unreferenced_bindings(child):
                trace.append({"operation": "delete_unreferenced_scalar_binding", "binding": removed})
        else:
            raise TemporalDiscoveryContractError("unknown management-plan construction")
        return child, trace


class DirectionFlipConstructionOperator(_ConstructionOperator):
    operator_id = DIRECTION_FLIP

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        current = str(profile.get("directionMode") or "")
        if current in {"long", "short"}:
            yield {"kind": "direction_flip", "before": current, "after": "short" if current == "long" else "long"}

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        child = copy.deepcopy(profile)
        if child.get("directionMode") != construction["before"]:
            raise TemporalDiscoveryContractError("direction construction parent drift")
        child["directionMode"] = construction["after"]
        return child, [{"operation": "flip_direction", "before": construction["before"], "after": construction["after"]}]


class GraphBoundTimeframeConstructionOperator(_ConstructionOperator):
    operator_id = GRAPH_BOUND_TIMEFRAME

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        bound = _graph_bound_instances(profile)
        for index, indicator in enumerate(profile.get("indicators") or []):
            if not isinstance(indicator, Mapping):
                continue
            meta = indicator.get("meta")
            config = indicator.get("config")
            if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
                continue
            instance_id = str(meta.get("instanceId") or "")
            indicator_id = str(meta.get("id") or "")
            current = str(config.get("timeframe") or "").upper()
            if instance_id not in bound or indicator_id not in self.catalog.indicators or current not in self.catalog.timeframes:
                continue
            for replacement in self.catalog.timeframes:
                if replacement != current:
                    yield {"kind": "graph_bound_timeframe_substitution", "indicatorIndex": index, "indicatorInstanceId": instance_id, "indicatorId": indicator_id, "before": current, "after": replacement, "evidenceLakeScope": {"regenerationRequired": True, "reason": "graph_bound_indicator_timeframe_changed"}}

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        child = copy.deepcopy(profile)
        indicator = child["indicators"][int(construction["indicatorIndex"])]
        if indicator["meta"].get("instanceId") != construction["indicatorInstanceId"] or indicator["config"].get("timeframe", "").upper() != construction["before"]:
            raise TemporalDiscoveryContractError("timeframe construction parent drift")
        indicator["config"]["timeframe"] = construction["after"]
        return child, [{"operation": "substitute_graph_bound_indicator_timeframe", "indicatorInstanceId": construction["indicatorInstanceId"], "before": construction["before"], "after": construction["after"], "evidenceLakeScope": construction["evidenceLakeScope"]}]


class DeferredIndicatorFamilySubstitutionOperator:
    """Visible but disabled until a strict semantic compatibility map is admitted."""

    operator_id = INDICATOR_FAMILY_SUBSTITUTION
    operator_version = CONSTRUCTION_OPERATOR_VERSION
    enabled = False
    deferred_reason = "strict_event_scalar_role_persistence_base_family_compatibility_map_not_admitted"

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        return []

    def apply(self, profile: Mapping[str, Any], plan: Mapping[str, Any], *, parent_validated_program_sha256: str, child_validated_program_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
        raise TemporalDiscoveryContractError("indicator-family substitution is deferred and disabled")

    def audit(self, parent_profile: Mapping[str, Any], transformed_profile: Mapping[str, Any], application_record: Mapping[str, Any]) -> dict[str, Any]:
        return finalize_audit({"operator_is_deferred": True}, operatorId=self.operator_id, deferredReason=self.deferred_reason)


class GeneratorV3ConstructionRegistry:
    """Versioned construction policy/registry; it never alters v2 continuation."""

    def __init__(self, catalog: Mapping[str, Any] | ConstructionCatalog) -> None:
        self.catalog = catalog if isinstance(catalog, ConstructionCatalog) else ConstructionCatalog(catalog)
        self._enabled = (
            ScalarDynamicManagementConstructionOperator(self.catalog),
            ManagementPlanConstructionOperator(self.catalog),
            DirectionFlipConstructionOperator(self.catalog),
            GraphBoundTimeframeConstructionOperator(self.catalog),
        )
        self._deferred = (DeferredIndicatorFamilySubstitutionOperator(),)
        self.structural_registry = StructuralOperatorRegistry(self._enabled)
        policy = {
            "schemaVersion": "temporal_generator_v3_construction_policy_v1",
            "generatorVersion": GENERATOR_V3_VERSION,
            "catalogSha256": self.catalog.catalog_sha256,
            "enabledOperatorIds": list(self.enabled_operator_ids),
            "deferredOperators": [{"operatorId": item.operator_id, "reason": item.deferred_reason} for item in self._deferred],
        }
        policy["policySha256"] = canonical_sha256(policy)
        self.policy = policy

    @property
    def enabled_operator_ids(self) -> tuple[str, ...]:
        return self.structural_registry.operator_ids

    @property
    def deferred_operators(self) -> tuple[DeferredIndicatorFamilySubstitutionOperator, ...]:
        return self._deferred

    def get(self, operator_id: str) -> Any:
        return self.structural_registry.get(operator_id)

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        return self.structural_registry.enumerate_plans(profile)


__all__ = [
    "CONSTRUCTION_OPERATOR_VERSION",
    "CONSTRUCTION_REACHABILITY_SCHEMA",
    "DIRECTION_FLIP",
    "GENERATOR_V3_VERSION",
    "GRAPH_BOUND_TIMEFRAME",
    "INDICATOR_FAMILY_SUBSTITUTION",
    "MANAGEMENT_PLAN",
    "MAX_MANAGEMENT_PLANS",
    "SCALAR_DYNAMIC_MANAGEMENT",
    "ConstructionCatalog",
    "DeferredIndicatorFamilySubstitutionOperator",
    "DirectionFlipConstructionOperator",
    "GeneratorV3ConstructionRegistry",
    "GraphBoundTimeframeConstructionOperator",
    "ManagementPlanConstructionOperator",
    "ScalarDynamicManagementConstructionOperator",
    "inspect_construction_reachability",
]
