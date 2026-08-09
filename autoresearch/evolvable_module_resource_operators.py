"""Deterministic resource mutations for :mod:`evolvable_module_genome`.

This is a deliberately *offline* operator surface.  It is not registered in
the QD scheduler yet.  Its job is to turn the v1 genome's typed resource pool
into a real, catalog-bound search substrate without smuggling new indicator
semantics into AutoResearch.  The Dashboard catalog remains the authority for
every indicator, raw event, timeframe, range and TA parameter we admit.

The public API is intentionally small:

``GenomeResourceOperatorLayer(catalog).enumerate_plans(genome)``
    returns canonical, content-addressed mutation plans;
``preview`` / ``apply``
    reconstruct the exact child genome from the immutable parent and plan;
``audit``
    independently proves a stored application still replays exactly.

Plans are fail-closed.  In particular, an operation may not leave an orphan
resource, borrow the opposite directional module's resources, evade a v1
budget, change a raw event's freshness, or use an indicator the catalog does
not explicitly authorize for its binding role.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from typing import Any, Iterable, Mapping, Sequence

from .evolvable_module_genome import (
    EvolvableGenomeError,
    EvolvableModuleGenomeV1,
    GenomeNodeV1,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
)
from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from .temporal_structural_operators import (
    finalize_application,
    finalize_audit,
    finalize_plan,
)
from .temporal_indicator_learning_v1 import (
    EVIDENCE_LOOKBACK_CHOICES,
    IndicatorLearningCatalog,
    _binding_contract,
    _catalog_meta_matches,
    _event_contract,
    _fuzzy_evidence_contract,
    _period_choices,
    _period_order_valid,
    _range_choices,
    _replace_talib,
)


RESOURCE_OPERATOR_VERSION = "evolvable_module_resource_operators_v1"
RESOURCE_OPERATOR_SCHEMA = "evolvable_module_resource_operator_plan_v1"
_WEIGHT_STEP = 0.25
_MIN_WEIGHT = 0.25


class GenomeResourceOperatorError(EvolvableGenomeError):
    """A catalog-bound resource mutation cannot be represented safely."""


def _clone(value: Any) -> Any:
    """Reject noncanonical plan inputs before they influence an identity."""

    try:
        import json

        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise GenomeResourceOperatorError("resource mutation requires finite canonical JSON") from exc


def _sid(prefix: str, seed: Mapping[str, Any], existing: Iterable[str]) -> str:
    """Stable short resource IDs; collision resolution is deterministic."""

    occupied = set(existing)
    base = f"{prefix}_{canonical_sha256(_clone(seed)).split(':', 1)[-1][:12]}"
    candidate = base
    index = 2
    while candidate in occupied:
        candidate = f"{base}_{index}"
        index += 1
    return candidate


def _resource_map(genome: EvolvableModuleGenomeV1, kind: ResourceKind) -> dict[str, dict[str, Any]]:
    return genome.resources.mapping(kind)


def _references(genome: EvolvableModuleGenomeV1, kind: ResourceKind, resource_id: str) -> list[GenomeNodeV1]:
    return [node for node in genome.nodes if any(use.kind is kind and use.resource_id == resource_id for use in node.resources)]


def _replace_node(genome: EvolvableModuleGenomeV1, changed: GenomeNodeV1) -> EvolvableModuleGenomeV1:
    return replace(genome, nodes=tuple(changed if node.node_id == changed.node_id else node for node in genome.nodes))


def _replace_resources(
    genome: EvolvableModuleGenomeV1,
    *,
    indicators: Sequence[Mapping[str, Any]] | None = None,
    groups: Sequence[Mapping[str, Any]] | None = None,
    events: Sequence[Mapping[str, Any]] | None = None,
) -> EvolvableModuleGenomeV1:
    pool = ResourcePoolV1(
        indicators=tuple(indicators if indicators is not None else genome.resources.indicators),
        evidence_groups=tuple(groups if groups is not None else genome.resources.evidence_groups),
        events=tuple(events if events is not None else genome.resources.events),
        management_refs=genome.resources.management_refs,
    )
    return replace(genome, resources=pool)


def _with_resource(node: GenomeNodeV1, kind: ResourceKind, resource_id: str) -> GenomeNodeV1:
    if any(use.kind is kind and use.resource_id == resource_id for use in node.resources):
        raise GenomeResourceOperatorError("resource already belongs to route")
    return replace(node, resources=(*node.resources, ResourceUse(kind, resource_id)))


def _without_resource(node: GenomeNodeV1, kind: ResourceKind, resource_id: str) -> GenomeNodeV1:
    values = tuple(use for use in node.resources if not (use.kind is kind and use.resource_id == resource_id))
    if len(values) == len(node.resources):
        raise GenomeResourceOperatorError("resource is not owned by route")
    return replace(node, resources=values)


def _all_with(guard: Mapping[str, Any], clause: Mapping[str, Any]) -> dict[str, Any]:
    current = _clone(dict(guard))
    if current.get("kind") == "all" and isinstance(current.get("guards"), list):
        return {"kind": "all", "guards": [*current["guards"], _clone(dict(clause))]}
    return {"kind": "all", "guards": [current, _clone(dict(clause))]}


def _remove_direct_all_clause(guard: Mapping[str, Any], *, kind: str, field: str, value: str) -> dict[str, Any] | None:
    """Remove only an explicit clause we previously know how to replay.

    This conservatism is intentional: arbitrary guard surgery belongs to the
    graph-grammar layer, not a resource operator that might silently alter a
    route's Boolean meaning.
    """

    raw = _clone(dict(guard))
    if raw.get("kind") != "all" or not isinstance(raw.get("guards"), list):
        return None
    kept = [item for item in raw["guards"] if not (isinstance(item, Mapping) and item.get("kind") == kind and str(item.get(field) or "") == value)]
    if len(kept) == len(raw["guards"]) or not kept:
        return None
    return kept[0] if len(kept) == 1 else {"kind": "all", "guards": kept}


def _evidence_clause(group_id: str, threshold: float) -> dict[str, Any]:
    return {"kind": "evidence_at_least", "groupId": group_id, "thresholdPercent": float(threshold)}


def _event_clause(event_id: str) -> dict[str, Any]:
    return {"kind": "fresh_event", "eventId": event_id}


def _groups_for_indicator(genome: EvolvableModuleGenomeV1, instance_id: str) -> list[str]:
    return [str(group["id"]) for group in genome.resources.mapping(ResourceKind.EVIDENCE_GROUP).values() if instance_id in {str(item) for item in group.get("indicatorInstanceIds") or []}]


def _event_ids_for_indicator(genome: EvolvableModuleGenomeV1, instance_id: str) -> list[str]:
    return [str(event["id"]) for event in genome.resources.mapping(ResourceKind.EVENT).values() if str(event.get("indicatorInstanceId") or "") == instance_id]


def _indicator_is_exclusive_to_group(genome: EvolvableModuleGenomeV1, instance_id: str, group_id: str) -> bool:
    return _groups_for_indicator(genome, instance_id) == [group_id] and not _event_ids_for_indicator(genome, instance_id)


def _normal_weights(indicators: list[dict[str, Any]], member_ids: Sequence[str]) -> None:
    """Set positive, exactly normalized evidence weights for one exclusive group."""

    count = len(member_ids)
    if count < 1:
        raise GenomeResourceOperatorError("evidence group must retain at least one member")
    # v1's group cap is three.  Quantized equal/default allocations are exact
    # enough for two/four only; use deterministic positive thirds for three.
    weight = 1.0 / count
    wanted = set(member_ids)
    seen = 0
    for item in indicators:
        meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
        if str(meta.get("instanceId") or "") in wanted:
            config = item.setdefault("config", {})
            if not isinstance(config, dict):
                raise GenomeResourceOperatorError("catalog indicator config is not mutable JSON")
            config["weight"] = weight
            seen += 1
    if seen != count:
        raise GenomeResourceOperatorError("weight mutation has a dangling member")


def _group_thresholds(genome: EvolvableModuleGenomeV1, group_id: str) -> list[tuple[GenomeNodeV1, float]]:
    """Return direct, mutation-owned threshold sites only."""

    sites: list[tuple[GenomeNodeV1, float]] = []
    for node in _references(genome, ResourceKind.EVIDENCE_GROUP, group_id):
        guard = node.guard
        candidates = guard.get("guards") if guard.get("kind") == "all" else [guard]
        for candidate in candidates or []:
            if isinstance(candidate, Mapping) and candidate.get("kind") in {"evidence_at_least", "evidence_below"} and str(candidate.get("groupId") or "") == group_id:
                try:
                    value = float(candidate["thresholdPercent"])
                except (KeyError, TypeError, ValueError):
                    continue
                if 0.0 < value < 100.0:
                    sites.append((node, value))
    return sites


def _replace_direct_threshold(guard: Mapping[str, Any], group_id: str, before: float, after: float) -> dict[str, Any]:
    raw = _clone(dict(guard))
    values = raw.get("guards") if raw.get("kind") == "all" else [raw]
    changed = False
    rewritten: list[Any] = []
    for value in values or []:
        item = _clone(value)
        if isinstance(item, dict) and item.get("kind") in {"evidence_at_least", "evidence_below"} and str(item.get("groupId") or "") == group_id and float(item.get("thresholdPercent")) == before:
            item["thresholdPercent"] = after
            changed = True
        rewritten.append(item)
    if not changed:
        raise GenomeResourceOperatorError("threshold parent drift")
    return (rewritten[0] if len(rewritten) == 1 else {"kind": "all", "guards": rewritten})


def _catalog_item(catalog: IndicatorLearningCatalog, indicator_id: str, instance_id: str, direction: str) -> dict[str, Any]:
    entry = catalog.entry(indicator_id)
    if entry is None:
        raise GenomeResourceOperatorError("catalog indicator disappeared")
    item = {"meta": _clone(entry["meta"]), "config": _clone(entry["config"]), "ownerSide": direction}
    item["meta"]["instanceId"] = instance_id
    if not isinstance(item["config"], dict):
        raise GenomeResourceOperatorError("catalog indicator config is not an object")
    item["config"]["isActive"] = True
    item["config"]["useFormingBar"] = False
    frame = str(item["config"].get("timeframe") or "").upper()
    item["config"]["timeframe"] = frame if frame in catalog.timeframe_policy else catalog.timeframe_policy[0]
    return item


def _source_catalog_matches(catalog: IndicatorLearningCatalog, item: Mapping[str, Any]) -> bool:
    meta = item.get("meta")
    if not isinstance(meta, Mapping):
        return False
    source = catalog.entry(str(meta.get("id") or ""))
    return source is not None and _catalog_meta_matches(meta, source["meta"])


def _replace_indicator_from_catalog(
    catalog: IndicatorLearningCatalog, item: Mapping[str, Any], replacement_id: str, direction: str
) -> dict[str, Any]:
    meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
    config = item.get("config") if isinstance(item.get("config"), Mapping) else {}
    instance_id = str(meta.get("instanceId") or "")
    replacement = _catalog_item(catalog, replacement_id, instance_id, direction)
    # Runtime binding knobs remain exact; catalog defaults own the actual
    # calculation parameters and all metadata.
    for key in ("isActive", "useFormingBar", "timeframe", "lookbackBars", "weight", "ranges"):
        if key in config:
            replacement["config"][key] = _clone(config[key])
    replacement["config"]["useFormingBar"] = False
    return replacement


class GenomeResourceOperatorLayer:
    """Catalog-authorized, content-addressed mutations over one v1 module."""

    def __init__(self, catalog: Mapping[str, Any] | IndicatorLearningCatalog) -> None:
        self.catalog = catalog if isinstance(catalog, IndicatorLearningCatalog) else IndicatorLearningCatalog(catalog)
        self.specification = {
            "schemaVersion": RESOURCE_OPERATOR_SCHEMA,
            "operatorVersion": RESOURCE_OPERATOR_VERSION,
            "catalogSha256": self.catalog.catalog_sha256,
            "timeframePolicy": list(self.catalog.timeframe_policy),
            "rawEvents": "fresh_only_v1",
            "weights": {"positive": True, "normalizedWithinExclusiveGroup": True, "minimum": _MIN_WEIGHT},
        }
        self.specification["operatorSpecSha256"] = canonical_sha256(self.specification)

    def enumerate_plans(self, genome: EvolvableModuleGenomeV1) -> list[dict[str, Any]]:
        """Return only plans whose reconstructed child validates fully."""

        genome.validate()
        parent_sha = genome.identity_sha256
        plans: dict[str, dict[str, Any]] = {}
        for construction in self._constructions(genome):
            try:
                construction = _clone(construction)
                child, _ = self._transform(genome, construction)
                child.validate()
            except (GenomeResourceOperatorError, EvolvableGenomeError, TemporalDiscoveryContractError, KeyError, TypeError, ValueError):
                continue
            identity = {
                "schemaVersion": RESOURCE_OPERATOR_SCHEMA,
                "operatorVersion": RESOURCE_OPERATOR_VERSION,
                "catalogSha256": self.catalog.catalog_sha256,
                "parentGenomeSha256": parent_sha,
                "construction": construction,
            }
            plan = finalize_plan({
                "operatorVersion": RESOURCE_OPERATOR_VERSION,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "parentGenomeSha256": parent_sha,
                "catalogSha256": self.catalog.catalog_sha256,
                "construction": construction,
                "constructionIdentitySha256": canonical_sha256(identity),
            })
            plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def preview(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
        return self._preview_with_trace(genome, plan)[0]

    def apply(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, dict[str, Any]]:
        child, trace = self._preview_with_trace(genome, plan)
        report = self._static_report(genome, child, plan)
        if not report["allChecksPassed"]:
            raise GenomeResourceOperatorError("resource mutation invariant audit failed")
        application = finalize_application({
            "operatorVersion": RESOURCE_OPERATOR_VERSION,
            "operatorSpecSha256": self.specification["operatorSpecSha256"],
            "planSha256": plan["planSha256"],
            "constructionIdentitySha256": plan["constructionIdentitySha256"],
            "parentGenomeSha256": genome.identity_sha256,
            "childGenomeSha256": child.identity_sha256,
            "parentSemanticTopologySha256": genome.semantic_topology_signature(),
            "childSemanticTopologySha256": child.semantic_topology_signature(),
            "semanticDelta": trace,
            "staticInvariantReport": report,
        })
        return child, application

    def audit(self, parent: EvolvableModuleGenomeV1, child: EvolvableModuleGenomeV1, application: Mapping[str, Any]) -> dict[str, Any]:
        stored = _clone(application)
        identity = stored.pop("applicationSha256", None)
        plan = next((item for item in self.enumerate_plans(parent) if item["planSha256"] == stored.get("planSha256")), None)
        replay_child: EvolvableModuleGenomeV1 | None = None
        trace: list[dict[str, Any]] | None = None
        if plan is not None:
            try:
                replay_child, trace = self._preview_with_trace(parent, plan)
            except GenomeResourceOperatorError:
                pass
        expected = self._static_report(parent, child, plan) if plan is not None else None
        return finalize_audit({
            "application_identity_exact": isinstance(identity, str) and canonical_sha256(stored) == identity,
            "plan_is_currently_applicable": plan is not None,
            "parent_identity_exact": stored.get("parentGenomeSha256") == parent.identity_sha256,
            "child_identity_exact": stored.get("childGenomeSha256") == child.identity_sha256,
            "replay_child_exact": replay_child is not None and replay_child.canonical() == child.canonical(),
            "semantic_delta_exact": trace is not None and stored.get("semanticDelta") == trace,
            "static_report_exact": stored.get("staticInvariantReport") == expected,
            "static_report_passing": isinstance(expected, Mapping) and expected.get("allChecksPassed") is True,
        }, operatorVersion=RESOURCE_OPERATOR_VERSION, applicationSha256=identity)

    def _preview_with_trace(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, list[dict[str, Any]]]:
        value = _clone(plan)
        if value.get("parentGenomeSha256") != genome.identity_sha256 or value.get("catalogSha256") != self.catalog.catalog_sha256:
            raise GenomeResourceOperatorError("resource mutation plan parent or catalog identity drift")
        current = {item["planSha256"]: item for item in self.enumerate_plans(genome)}
        canonical = current.get(value.get("planSha256"))
        if canonical != value:
            raise GenomeResourceOperatorError("resource mutation plan is not canonical and applicable")
        child, trace = self._transform(genome, value["construction"])
        child.validate()
        return child, trace

    def _static_report(self, parent: EvolvableModuleGenomeV1, child: EvolvableModuleGenomeV1, plan: Mapping[str, Any] | None) -> dict[str, Any]:
        try:
            parent.validate(); child.validate()
            checks = {
                "parent_valid": True,
                "child_valid": True,
                "parent_catalog_bound": self._catalog_bound(parent),
                "child_catalog_bound": self._catalog_bound(child),
                "raw_events_fresh": self._raw_events_fresh(child),
                "plan_binds_parent": plan is not None and plan.get("parentGenomeSha256") == parent.identity_sha256,
                "plan_binds_catalog": plan is not None and plan.get("catalogSha256") == self.catalog.catalog_sha256,
            }
        except (EvolvableGenomeError, GenomeResourceOperatorError):
            checks = {"parent_valid": False, "child_valid": False}
        return finalize_audit(checks, operatorVersion=RESOURCE_OPERATOR_VERSION, planSha256=(plan or {}).get("planSha256"), childGenomeSha256=child.identity_sha256)

    def _catalog_bound(self, genome: EvolvableModuleGenomeV1) -> bool:
        return all(_source_catalog_matches(self.catalog, item) for item in genome.resources.mapping(ResourceKind.INDICATOR).values())

    def _raw_events_fresh(self, genome: EvolvableModuleGenomeV1) -> bool:
        indicators = _resource_map(genome, ResourceKind.INDICATOR)
        for event in _resource_map(genome, ResourceKind.EVENT).values():
            instance = str(event.get("indicatorInstanceId") or "")
            item = indicators.get(instance)
            if item is None or not isinstance(item.get("config"), Mapping):
                return False
            if int(item["config"].get("lookbackBars", 1)) != 1:
                return False
        return True

    def _constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        yield from self._group_constructions(genome)
        yield from self._member_constructions(genome)
        yield from self._weight_and_threshold_constructions(genome)
        yield from self._indicator_constructions(genome)
        yield from self._event_constructions(genome)

    def _group_constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        groups = _resource_map(genome, ResourceKind.EVIDENCE_GROUP)
        group_ids = set(groups)
        # Create a new, separately-addressable evidence group on a route that
        # already owns a group.  It starts with one existing catalog-proven
        # fuzzy member, so no resource can become orphaned.
        if len(groups) < genome.budget.max_evidence_groups:
            for source_id, group in sorted(groups.items()):
                members = [str(item) for item in group.get("indicatorInstanceIds") or []]
                if not members:
                    continue
                for node in _references(genome, ResourceKind.EVIDENCE_GROUP, source_id):
                    new_id = _sid("eg", {"operation": "group_create", "parent": genome.identity_sha256, "source": source_id, "route": node.node_id}, group_ids)
                    yield {"kind": "evidence_group_create", "sourceGroupId": source_id, "groupId": new_id, "nodeId": node.node_id, "members": [members[0]], "thresholdPercent": 50.0}
        # Removal is intentionally limited to a directly removable route
        # clause; arbitrary Boolean simplification is a grammar operation.
        for group_id in sorted(groups):
            owners = _references(genome, ResourceKind.EVIDENCE_GROUP, group_id)
            if len(owners) != 1:
                continue
            node = owners[0]
            after = _remove_direct_all_clause(node.guard, kind="evidence_at_least", field="groupId", value=group_id)
            if after is not None:
                yield {"kind": "evidence_group_remove", "groupId": group_id, "nodeId": node.node_id, "beforeGuard": _clone(node.guard), "afterGuard": after}
        # Split an exclusive multi-member group into two independently guarded
        # pieces; the new group is immediately route-owned.
        if len(groups) < genome.budget.max_evidence_groups:
            for group_id, group in sorted(groups.items()):
                members = [str(item) for item in group.get("indicatorInstanceIds") or []]
                if len(members) < 2 or any(not _indicator_is_exclusive_to_group(genome, member, group_id) for member in members):
                    continue
                owners = _references(genome, ResourceKind.EVIDENCE_GROUP, group_id)
                if len(owners) != 1:
                    continue
                node = owners[0]
                new_id = _sid("eg", {"operation": "group_split", "parent": genome.identity_sha256, "source": group_id, "route": node.node_id}, group_ids)
                yield {"kind": "evidence_group_split", "sourceGroupId": group_id, "groupId": new_id, "nodeId": node.node_id, "leftMembers": [members[0]], "rightMembers": members[1:], "thresholdPercent": 50.0}
        # Merge only co-owned, exclusive groups whose union stays within cap.
        values = sorted(groups)
        for index, left_id in enumerate(values):
            for right_id in values[index + 1:]:
                left, right = groups[left_id], groups[right_id]
                members = list(dict.fromkeys([*(str(x) for x in left.get("indicatorInstanceIds") or []), *(str(x) for x in right.get("indicatorInstanceIds") or [])]))
                if len(members) > genome.budget.max_group_members or any(not (_groups_for_indicator(genome, member) in ([left_id], [right_id])) for member in members):
                    continue
                left_nodes, right_nodes = _references(genome, ResourceKind.EVIDENCE_GROUP, left_id), _references(genome, ResourceKind.EVIDENCE_GROUP, right_id)
                if len(left_nodes) != 1 or left_nodes != right_nodes:
                    continue
                node = left_nodes[0]
                after = _remove_direct_all_clause(node.guard, kind="evidence_at_least", field="groupId", value=right_id)
                if after is not None:
                    yield {"kind": "evidence_group_merge", "leftGroupId": left_id, "rightGroupId": right_id, "nodeId": node.node_id, "members": members, "beforeGuard": _clone(node.guard), "afterGuard": after}

    def _member_constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        groups, indicators = _resource_map(genome, ResourceKind.EVIDENCE_GROUP), _resource_map(genome, ResourceKind.INDICATOR)
        for group_id, group in sorted(groups.items()):
            members = [str(item) for item in group.get("indicatorInstanceIds") or []]
            contract = self._fuzzy_contract_for_members(indicators, members)
            if contract is None:
                continue
            if len(members) < genome.budget.max_group_members:
                for instance_id, item in sorted(indicators.items()):
                    # A member may be intentionally shared with another
                    # evidence group.  It is still route-owned, so insertion
                    # cannot manufacture an orphan.  Event sources remain
                    # excluded: raw events and fuzzy state scores are
                    # distinct typed capabilities.
                    if instance_id in members or _event_ids_for_indicator(genome, instance_id):
                        continue
                    meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
                    if _fuzzy_evidence_contract(meta) == contract:
                        yield {"kind": "evidence_member_insert", "groupId": group_id, "indicatorInstanceId": instance_id, "beforeMembers": members}
            if len(members) > 1:
                for instance_id in members:
                    if len(_groups_for_indicator(genome, instance_id)) > 1 or _event_ids_for_indicator(genome, instance_id):
                        yield {"kind": "evidence_member_remove", "groupId": group_id, "indicatorInstanceId": instance_id, "beforeMembers": members}

    def _weight_and_threshold_constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        groups, indicators = _resource_map(genome, ResourceKind.EVIDENCE_GROUP), _resource_map(genome, ResourceKind.INDICATOR)
        for group_id, group in sorted(groups.items()):
            members = [str(item) for item in group.get("indicatorInstanceIds") or []]
            if len(members) > 1 and all(_indicator_is_exclusive_to_group(genome, member, group_id) for member in members):
                weights = [float((indicators[member].get("config") or {}).get("weight", 1.0)) for member in members]
                total = sum(weights)
                if total > 0 and all(weight > 0 for weight in weights):
                    normalized = [weight / total for weight in weights]
                    for index, member in enumerate(members):
                        for delta in (-_WEIGHT_STEP, _WEIGHT_STEP):
                            candidate = normalized[:]
                            candidate[index] += delta
                            rest = len(candidate) - 1
                            if rest < 1 or candidate[index] < _MIN_WEIGHT:
                                continue
                            adjustment = -delta / rest
                            for other in range(len(candidate)):
                                if other != index:
                                    candidate[other] += adjustment
                            if any(value < _MIN_WEIGHT for value in candidate):
                                continue
                            yield {"kind": "evidence_weight_mutate", "groupId": group_id, "indicatorInstanceId": member, "beforeWeights": dict(zip(members, normalized)), "afterWeights": dict(zip(members, candidate))}
            for node, before in _group_thresholds(genome, group_id):
                for after in (before - 5.0, before + 5.0):
                    if 5.0 <= after <= 95.0:
                        yield {"kind": "evidence_threshold_mutate", "groupId": group_id, "nodeId": node.node_id, "before": before, "after": after}

    def _indicator_constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        groups, indicators = _resource_map(genome, ResourceKind.EVIDENCE_GROUP), _resource_map(genome, ResourceKind.INDICATOR)
        # Catalog-backed instance insert/remove.
        if len(indicators) < genome.budget.max_indicators:
            for group_id, group in sorted(groups.items()):
                members = [str(item) for item in group.get("indicatorInstanceIds") or []]
                contract = self._fuzzy_contract_for_members(indicators, members)
                if contract is None or len(members) >= genome.budget.max_group_members:
                    continue
                for indicator_id, entry in sorted(self.catalog.indicators.items()):
                    if _fuzzy_evidence_contract(entry["meta"]) != contract:
                        continue
                    for timeframe in self.catalog.timeframe_policy:
                        instance_id = _sid("ind", {"operation": "indicator_insert", "parent": genome.identity_sha256, "group": group_id, "indicator": indicator_id, "timeframe": timeframe}, indicators)
                        candidate = _catalog_item(self.catalog, indicator_id, instance_id, genome.direction)
                        candidate["config"]["timeframe"] = timeframe
                        # Reusing a primitive is legitimate search substrate when
                        # the instance is materially distinct (for example RSI
                        # M5 versus H1). Reject only an exact semantic duplicate.
                        def semantic(row: Mapping[str, Any]) -> str:
                            copy = _clone(row)
                            copy["meta"].pop("instanceId", None)
                            return canonical_sha256(copy)
                        candidate_key = semantic(candidate)
                        if any(
                            semantic(indicators[member]) == candidate_key
                            for member in members
                            if member in indicators
                        ):
                            continue
                        yield {"kind": "indicator_instance_insert", "groupId": group_id, "indicatorId": indicator_id, "indicatorInstanceId": instance_id, "timeframe": timeframe, "beforeMembers": members}
        for group_id, group in sorted(groups.items()):
            members = [str(item) for item in group.get("indicatorInstanceIds") or []]
            if len(members) <= 1:
                continue
            for instance_id in members:
                if _indicator_is_exclusive_to_group(genome, instance_id, group_id):
                    yield {"kind": "indicator_instance_remove", "groupId": group_id, "indicatorInstanceId": instance_id, "beforeMembers": members}
        # Substitute only when the existing binding's technical contract agrees.
        for instance_id, item in sorted(indicators.items()):
            if not _source_catalog_matches(self.catalog, item):
                continue
            meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
            fuzzy, event = bool(_groups_for_indicator(genome, instance_id)), bool(_event_ids_for_indicator(genome, instance_id))
            if fuzzy and event:
                continue
            source_contract = _binding_contract(meta, fuzzy=fuzzy, event=event, scalar=False)
            if source_contract is None:
                continue
            for replacement_id, entry in sorted(self.catalog.indicators.items()):
                if replacement_id == meta.get("id") or _binding_contract(entry["meta"], fuzzy=fuzzy, event=event, scalar=False) != source_contract:
                    continue
                yield {"kind": "indicator_substitute", "indicatorInstanceId": instance_id, "beforeIndicatorId": meta.get("id"), "afterIndicatorId": replacement_id, "bindingContract": source_contract}
        # Bounded catalog parameter / timeframe / lookback changes.  Events
        # retain a fresh (one-bar) identity by construction.
        for instance_id, item in sorted(indicators.items()):
            if not _source_catalog_matches(self.catalog, item):
                continue
            meta, config = item.get("meta"), item.get("config")
            if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
                continue
            bound = bool(_groups_for_indicator(genome, instance_id) or _event_ids_for_indicator(genome, instance_id))
            if not bound:
                continue
            current_frame = str(config.get("timeframe") or "").upper()
            if current_frame in self.catalog.timeframe_policy:
                for frame in self.catalog.timeframe_policy:
                    if frame != current_frame:
                        yield {"kind": "indicator_timeframe_mutate", "indicatorInstanceId": instance_id, "before": current_frame, "after": frame}
            if _groups_for_indicator(genome, instance_id) and not _event_ids_for_indicator(genome, instance_id):
                current_lookback = int(config.get("lookbackBars", 1))
                if current_lookback in EVIDENCE_LOOKBACK_CHOICES:
                    for lookback in EVIDENCE_LOOKBACK_CHOICES:
                        if lookback != current_lookback:
                            yield {"kind": "indicator_lookback_mutate", "indicatorInstanceId": instance_id, "before": current_lookback, "after": lookback}
            for change in _period_choices(meta, config):
                yield {"kind": "indicator_period_mutate", "indicatorInstanceId": instance_id, "change": change}
            for change in _range_choices(meta, config):
                yield {"kind": "indicator_range_mutate", "indicatorInstanceId": instance_id, "change": change}

    def _event_constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        indicators, events = _resource_map(genome, ResourceKind.INDICATOR), _resource_map(genome, ResourceKind.EVENT)
        # Insert an event plus its source indicator atomically so neither can
        # ever be orphaned.  A pre-position node receives the fresh-event
        # clause and binding reference in the same immutable transform.
        if len(events) < genome.budget.max_events and len(indicators) < genome.budget.max_indicators:
            for node in sorted((node for node in genome.nodes if node.zone.value in {"entry", "setup"}), key=lambda node: node.node_id):
                if any(use.kind is ResourceKind.EVENT for use in node.resources):
                    continue
                for indicator_id, entry in sorted(self.catalog.indicators.items()):
                    contract = _event_contract(entry["meta"])
                    if contract is None:
                        continue
                    instance_id = _sid("evtind", {"operation": "event_insert", "parent": genome.identity_sha256, "node": node.node_id, "indicator": indicator_id}, indicators)
                    event_id = _sid("evt", {"operation": "event_insert", "parent": genome.identity_sha256, "node": node.node_id, "instance": instance_id}, events)
                    yield {"kind": "directional_event_insert", "nodeId": node.node_id, "indicatorId": indicator_id, "indicatorInstanceId": instance_id, "eventId": event_id, "contract": contract}
        for event_id, event in sorted(events.items()):
            instance_id = str(event.get("indicatorInstanceId") or "")
            owners = _references(genome, ResourceKind.EVENT, event_id)
            if len(owners) != 1 or _groups_for_indicator(genome, instance_id):
                continue
            node = owners[0]
            after = _remove_direct_all_clause(node.guard, kind="fresh_event", field="eventId", value=event_id)
            if after is not None:
                yield {"kind": "directional_event_remove", "eventId": event_id, "indicatorInstanceId": instance_id, "nodeId": node.node_id, "beforeGuard": _clone(node.guard), "afterGuard": after}
        for event_id, event in sorted(events.items()):
            instance_id = str(event.get("indicatorInstanceId") or "")
            item = indicators.get(instance_id)
            if item is None or not _source_catalog_matches(self.catalog, item):
                continue
            meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
            contract = _binding_contract(meta, fuzzy=False, event=True, scalar=False)
            if contract is None:
                continue
            for replacement_id, entry in sorted(self.catalog.indicators.items()):
                if replacement_id != meta.get("id") and _binding_contract(entry["meta"], fuzzy=False, event=True, scalar=False) == contract:
                    yield {"kind": "directional_event_substitute", "eventId": event_id, "indicatorInstanceId": instance_id, "beforeIndicatorId": meta.get("id"), "afterIndicatorId": replacement_id, "bindingContract": contract}

    @staticmethod
    def _fuzzy_contract_for_members(indicators: Mapping[str, Mapping[str, Any]], members: Sequence[str]) -> dict[str, Any] | None:
        contracts = []
        for member in members:
            item = indicators.get(member)
            meta = item.get("meta") if isinstance(item, Mapping) and isinstance(item.get("meta"), Mapping) else {}
            contract = _fuzzy_evidence_contract(meta)
            if contract is None:
                return None
            contracts.append(contract)
        return contracts[0] if contracts and all(contract == contracts[0] for contract in contracts) else None

    def _transform(self, genome: EvolvableModuleGenomeV1, construction: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, list[dict[str, Any]]]:
        kind = str(construction.get("kind") or "")
        groups, indicators, events = _resource_map(genome, ResourceKind.EVIDENCE_GROUP), _resource_map(genome, ResourceKind.INDICATOR), _resource_map(genome, ResourceKind.EVENT)
        if kind == "evidence_group_create":
            source, group_id, node_id = (str(construction[key]) for key in ("sourceGroupId", "groupId", "nodeId"))
            node = next((item for item in genome.nodes if item.node_id == node_id), None)
            if source not in groups or group_id in groups or node is None or node not in _references(genome, ResourceKind.EVIDENCE_GROUP, source):
                raise GenomeResourceOperatorError("evidence group create parent drift")
            members = [str(item) for item in construction.get("members") or []]
            if len(members) != 1 or members[0] not in groups[source].get("indicatorInstanceIds", []):
                raise GenomeResourceOperatorError("evidence group create membership drift")
            new_group = {"id": group_id, "indicatorInstanceIds": members, "ownerSide": genome.direction}
            child = _replace_resources(genome, groups=[*groups.values(), new_group])
            changed = _with_resource(node, ResourceKind.EVIDENCE_GROUP, group_id)
            changed = replace(changed, guard=_all_with(changed.guard, _evidence_clause(group_id, float(construction["thresholdPercent"]))))
            child = _replace_node(child, changed)
            return child, [{"operation": kind, "groupId": group_id, "sourceGroupId": source, "nodeId": node_id, "members": members}]
        if kind == "evidence_group_remove":
            group_id, node_id = str(construction["groupId"]), str(construction["nodeId"])
            node = next((item for item in genome.nodes if item.node_id == node_id), None)
            if group_id not in groups or node is None or _references(genome, ResourceKind.EVIDENCE_GROUP, group_id) != [node] or node.guard != construction.get("beforeGuard"):
                raise GenomeResourceOperatorError("evidence group remove parent drift")
            changed = _without_resource(node, ResourceKind.EVIDENCE_GROUP, group_id)
            changed = replace(changed, guard=_clone(construction["afterGuard"]))
            child = _replace_node(_replace_resources(genome, groups=[group for key, group in groups.items() if key != group_id]), changed)
            return child, [{"operation": kind, "groupId": group_id, "nodeId": node_id}]
        if kind == "evidence_group_split":
            source, new_id, node_id = (str(construction[key]) for key in ("sourceGroupId", "groupId", "nodeId"))
            node = next((item for item in genome.nodes if item.node_id == node_id), None)
            left, right = [str(item) for item in construction.get("leftMembers") or []], [str(item) for item in construction.get("rightMembers") or []]
            if source not in groups or new_id in groups or node is None or _references(genome, ResourceKind.EVIDENCE_GROUP, source) != [node] or set(left + right) != set(groups[source].get("indicatorInstanceIds") or []) or not left or not right:
                raise GenomeResourceOperatorError("evidence group split parent drift")
            revised = _clone(groups[source]); revised["indicatorInstanceIds"] = left
            new_group = {"id": new_id, "indicatorInstanceIds": right, "ownerSide": genome.direction}
            child = _replace_resources(genome, groups=[*(group for key, group in groups.items() if key != source), revised, new_group])
            all_indicators = [copy.deepcopy(item) for item in indicators.values()]
            _normal_weights(all_indicators, left); _normal_weights(all_indicators, right)
            child = _replace_resources(child, indicators=all_indicators)
            changed = _with_resource(node, ResourceKind.EVIDENCE_GROUP, new_id)
            changed = replace(changed, guard=_all_with(changed.guard, _evidence_clause(new_id, float(construction["thresholdPercent"]))))
            child = _replace_node(child, changed)
            return child, [{"operation": kind, "sourceGroupId": source, "groupId": new_id, "leftMembers": left, "rightMembers": right, "nodeId": node_id}]
        if kind == "evidence_group_merge":
            left_id, right_id, node_id = (str(construction[key]) for key in ("leftGroupId", "rightGroupId", "nodeId"))
            node = next((item for item in genome.nodes if item.node_id == node_id), None)
            if left_id not in groups or right_id not in groups or node is None or _references(genome, ResourceKind.EVIDENCE_GROUP, left_id) != [node] or _references(genome, ResourceKind.EVIDENCE_GROUP, right_id) != [node] or node.guard != construction.get("beforeGuard"):
                raise GenomeResourceOperatorError("evidence group merge parent drift")
            members = [str(item) for item in construction.get("members") or []]
            expected = list(dict.fromkeys([*(str(item) for item in groups[left_id].get("indicatorInstanceIds") or []), *(str(item) for item in groups[right_id].get("indicatorInstanceIds") or [])]))
            if members != expected:
                raise GenomeResourceOperatorError("evidence group merge membership drift")
            revised = _clone(groups[left_id]); revised["indicatorInstanceIds"] = members
            child = _replace_resources(genome, groups=[*(group for key, group in groups.items() if key not in {left_id, right_id}), revised])
            all_indicators = [copy.deepcopy(item) for item in indicators.values()]; _normal_weights(all_indicators, members)
            child = _replace_resources(child, indicators=all_indicators)
            changed = _without_resource(node, ResourceKind.EVIDENCE_GROUP, right_id)
            changed = replace(changed, guard=_clone(construction["afterGuard"]))
            child = _replace_node(child, changed)
            return child, [{"operation": kind, "leftGroupId": left_id, "removedGroupId": right_id, "members": members, "nodeId": node_id}]
        if kind in {"evidence_member_insert", "evidence_member_remove"}:
            group_id, instance_id = str(construction["groupId"]), str(construction["indicatorInstanceId"])
            group = groups.get(group_id)
            before = [str(item) for item in construction.get("beforeMembers") or []]
            if group is None or [str(item) for item in group.get("indicatorInstanceIds") or []] != before or instance_id not in indicators:
                raise GenomeResourceOperatorError("evidence membership parent drift")
            members = [*before, instance_id] if kind.endswith("insert") else [item for item in before if item != instance_id]
            if not members or len(members) > genome.budget.max_group_members:
                raise GenomeResourceOperatorError("evidence membership violates group budget")
            revised = _clone(group); revised["indicatorInstanceIds"] = sorted(members)
            child = _replace_resources(genome, groups=[*(item for key, item in groups.items() if key != group_id), revised])
            if all(_indicator_is_exclusive_to_group(genome, member, group_id) or (kind.endswith("insert") and member == instance_id and not _groups_for_indicator(genome, member)) for member in members):
                all_indicators = [copy.deepcopy(item) for item in indicators.values()]; _normal_weights(all_indicators, members); child = _replace_resources(child, indicators=all_indicators)
            return child, [{"operation": kind, "groupId": group_id, "indicatorInstanceId": instance_id, "beforeMembers": before, "afterMembers": sorted(members)}]
        if kind == "evidence_weight_mutate":
            group_id = str(construction["groupId"]); group = groups.get(group_id)
            members = [str(item) for item in (group or {}).get("indicatorInstanceIds") or []]
            before, after = construction.get("beforeWeights"), construction.get("afterWeights")
            if group is None or not isinstance(before, Mapping) or not isinstance(after, Mapping) or set(before) != set(members) or set(after) != set(members):
                raise GenomeResourceOperatorError("evidence weight parent drift")
            current = {member: float((indicators[member].get("config") or {}).get("weight", 1.0)) for member in members}
            total = sum(current.values())
            normalized = {member: current[member] / total for member in members} if total > 0 else {}
            if normalized != {str(key): float(value) for key, value in before.items()} or any(float(value) < _MIN_WEIGHT for value in after.values()) or abs(sum(float(value) for value in after.values()) - 1.0) > 1e-12:
                raise GenomeResourceOperatorError("evidence weight values are not positive normalized mutation")
            all_indicators = [copy.deepcopy(item) for item in indicators.values()]
            for item in all_indicators:
                identifier = str((item.get("meta") or {}).get("instanceId") or "")
                if identifier in after:
                    item["config"]["weight"] = float(after[identifier])
            child = _replace_resources(genome, indicators=all_indicators)
            return child, [{"operation": kind, "groupId": group_id, "beforeWeights": normalized, "afterWeights": _clone(after)}]
        if kind == "evidence_threshold_mutate":
            node = next((item for item in genome.nodes if item.node_id == str(construction["nodeId"])), None)
            if node is None:
                raise GenomeResourceOperatorError("threshold route disappeared")
            changed = replace(node, guard=_replace_direct_threshold(node.guard, str(construction["groupId"]), float(construction["before"]), float(construction["after"])))
            child = _replace_node(genome, changed)
            return child, [{"operation": kind, "groupId": construction["groupId"], "nodeId": node.node_id, "before": construction["before"], "after": construction["after"]}]
        if kind == "indicator_instance_insert":
            group_id, indicator_id, instance_id = (str(construction[key]) for key in ("groupId", "indicatorId", "indicatorInstanceId"))
            group = groups.get(group_id); before = [str(item) for item in construction.get("beforeMembers") or []]
            if group is None or [str(item) for item in group.get("indicatorInstanceIds") or []] != before or instance_id in indicators:
                raise GenomeResourceOperatorError("indicator insert parent drift")
            item = _catalog_item(self.catalog, indicator_id, instance_id, genome.direction)
            timeframe = str(construction.get("timeframe") or "").upper()
            if timeframe not in self.catalog.timeframe_policy:
                raise GenomeResourceOperatorError("indicator insert timeframe is not catalog-authorized")
            item["config"]["timeframe"] = timeframe
            if _fuzzy_evidence_contract(item["meta"]) != self._fuzzy_contract_for_members(indicators, before):
                raise GenomeResourceOperatorError("catalog indicator does not match fuzzy binding")
            members = sorted([*before, instance_id]); revised = _clone(group); revised["indicatorInstanceIds"] = members
            child = _replace_resources(genome, indicators=[*indicators.values(), item], groups=[*(value for key, value in groups.items() if key != group_id), revised])
            all_indicators = [copy.deepcopy(value) for value in child.resources.mapping(ResourceKind.INDICATOR).values()]; _normal_weights(all_indicators, members)
            child = _replace_resources(child, indicators=all_indicators)
            return child, [{"operation": kind, "groupId": group_id, "indicatorInstanceId": instance_id, "indicatorId": indicator_id, "afterMembers": members}]
        if kind == "indicator_instance_remove":
            group_id, instance_id = str(construction["groupId"]), str(construction["indicatorInstanceId"])
            group = groups.get(group_id); before = [str(item) for item in construction.get("beforeMembers") or []]
            if group is None or [str(item) for item in group.get("indicatorInstanceIds") or []] != before or not _indicator_is_exclusive_to_group(genome, instance_id, group_id):
                raise GenomeResourceOperatorError("indicator remove parent drift")
            members = [item for item in before if item != instance_id]
            if not members:
                raise GenomeResourceOperatorError("cannot remove final fuzzy indicator")
            revised = _clone(group); revised["indicatorInstanceIds"] = members
            child = _replace_resources(genome, indicators=[item for key, item in indicators.items() if key != instance_id], groups=[*(value for key, value in groups.items() if key != group_id), revised])
            all_indicators = [copy.deepcopy(value) for value in child.resources.mapping(ResourceKind.INDICATOR).values()]; _normal_weights(all_indicators, members)
            child = _replace_resources(child, indicators=all_indicators)
            return child, [{"operation": kind, "groupId": group_id, "indicatorInstanceId": instance_id, "afterMembers": members}]
        if kind in {"indicator_substitute", "directional_event_substitute"}:
            instance_id, before_id, after_id = (str(construction[key]) for key in ("indicatorInstanceId", "beforeIndicatorId", "afterIndicatorId"))
            item = indicators.get(instance_id)
            if item is None or str((item.get("meta") or {}).get("id") or "") != before_id:
                raise GenomeResourceOperatorError("indicator substitution parent drift")
            fuzzy, event = bool(_groups_for_indicator(genome, instance_id)), bool(_event_ids_for_indicator(genome, instance_id))
            if _binding_contract((item.get("meta") or {}), fuzzy=fuzzy, event=event, scalar=False) != construction.get("bindingContract"):
                raise GenomeResourceOperatorError("indicator substitution binding drift")
            replacement = _replace_indicator_from_catalog(self.catalog, item, after_id, genome.direction)
            if _binding_contract(replacement["meta"], fuzzy=fuzzy, event=event, scalar=False) != construction.get("bindingContract"):
                raise GenomeResourceOperatorError("replacement catalog capability drift")
            child = _replace_resources(genome, indicators=[replacement if key == instance_id else value for key, value in indicators.items()])
            return child, [{"operation": kind, "indicatorInstanceId": instance_id, "beforeIndicatorId": before_id, "afterIndicatorId": after_id}]
        if kind in {"indicator_timeframe_mutate", "indicator_lookback_mutate", "indicator_period_mutate", "indicator_range_mutate"}:
            instance_id = str(construction["indicatorInstanceId"]); item = indicators.get(instance_id)
            if item is None or not _source_catalog_matches(self.catalog, item):
                raise GenomeResourceOperatorError("indicator parameter catalog or parent drift")
            revised = copy.deepcopy(item); config = revised.get("config")
            if not isinstance(config, dict):
                raise GenomeResourceOperatorError("indicator parameter config is invalid")
            if kind == "indicator_timeframe_mutate":
                if str(config.get("timeframe") or "").upper() != construction.get("before") or construction.get("after") not in self.catalog.timeframe_policy:
                    raise GenomeResourceOperatorError("indicator timeframe parent drift")
                config["timeframe"] = construction["after"]
            elif kind == "indicator_lookback_mutate":
                if _event_ids_for_indicator(genome, instance_id) or int(config.get("lookbackBars", 1)) != int(construction["before"]) or int(construction["after"]) not in EVIDENCE_LOOKBACK_CHOICES:
                    raise GenomeResourceOperatorError("indicator lookback violates fresh-event contract")
                config["lookbackBars"] = int(construction["after"])
            elif kind == "indicator_period_mutate":
                change = construction.get("change")
                if not isinstance(change, Mapping):
                    raise GenomeResourceOperatorError("indicator period mutation missing change")
                _replace_talib(config, str(change["parameter"]), change["after"])
                if not _period_order_valid(config):
                    raise GenomeResourceOperatorError("indicator period mutation breaks ordered parameters")
            else:
                change = construction.get("change")
                if not isinstance(change, Mapping) or not isinstance(config.get("ranges"), dict) or config["ranges"].get(change.get("side")) != change.get("before"):
                    raise GenomeResourceOperatorError("indicator range parent drift")
                config["ranges"][change["side"]] = _clone(change["after"])
            child = _replace_resources(genome, indicators=[revised if key == instance_id else value for key, value in indicators.items()])
            return child, [{"operation": kind, "indicatorInstanceId": instance_id, "construction": _clone(construction)}]
        if kind == "directional_event_insert":
            node = next((item for item in genome.nodes if item.node_id == str(construction["nodeId"])), None)
            instance_id, event_id, indicator_id = (str(construction[key]) for key in ("indicatorInstanceId", "eventId", "indicatorId"))
            contract = construction.get("contract")
            if node is None or instance_id in indicators or event_id in events or any(use.kind is ResourceKind.EVENT for use in node.resources):
                raise GenomeResourceOperatorError("event insert parent drift")
            item = _catalog_item(self.catalog, indicator_id, instance_id, genome.direction)
            if _event_contract(item["meta"]) != contract:
                raise GenomeResourceOperatorError("event catalog capability drift")
            item["config"]["lookbackBars"] = 1
            schema = contract["eventOutputSchema"]
            event = {"id": event_id, "indicatorInstanceId": instance_id, "longOutput": schema["longOutput"], "shortOutput": schema["shortOutput"], "ownerSide": genome.direction}
            child = _replace_resources(genome, indicators=[*indicators.values(), item], events=[*events.values(), event])
            changed = _with_resource(node, ResourceKind.EVENT, event_id)
            changed = replace(changed, guard=_all_with(changed.guard, _event_clause(event_id)))
            child = _replace_node(child, changed)
            return child, [{"operation": kind, "eventId": event_id, "indicatorInstanceId": instance_id, "indicatorId": indicator_id, "nodeId": node.node_id}]
        if kind == "directional_event_remove":
            event_id, instance_id, node_id = (str(construction[key]) for key in ("eventId", "indicatorInstanceId", "nodeId"))
            node = next((item for item in genome.nodes if item.node_id == node_id), None)
            if event_id not in events or node is None or _references(genome, ResourceKind.EVENT, event_id) != [node] or str(events[event_id].get("indicatorInstanceId") or "") != instance_id or node.guard != construction.get("beforeGuard") or _groups_for_indicator(genome, instance_id):
                raise GenomeResourceOperatorError("event remove parent drift")
            child = _replace_resources(genome, indicators=[item for key, item in indicators.items() if key != instance_id], events=[event for key, event in events.items() if key != event_id])
            changed = _without_resource(node, ResourceKind.EVENT, event_id); changed = replace(changed, guard=_clone(construction["afterGuard"]))
            child = _replace_node(child, changed)
            return child, [{"operation": kind, "eventId": event_id, "indicatorInstanceId": instance_id, "nodeId": node_id}]
        raise GenomeResourceOperatorError("unknown resource mutation construction")


__all__ = [
    "GenomeResourceOperatorError",
    "GenomeResourceOperatorLayer",
    "RESOURCE_OPERATOR_SCHEMA",
    "RESOURCE_OPERATOR_VERSION",
]
