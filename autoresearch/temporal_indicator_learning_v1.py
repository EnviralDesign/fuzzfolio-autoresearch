"""Bounded, catalog-provenance indicator learning for temporal candidates.

This is deliberately a controller-side construction surface.  It emits only
complete deterministic plans and does not alter the QD runner or worker
contract.  The catalog is authority for every value emitted here; missing
semantic metadata produces an explicit deferred disposition, never a guess.
"""

from __future__ import annotations

import copy
import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_structural_operators import StructuralOperatorRegistry, finalize_application, finalize_audit, finalize_plan


INDICATOR_LEARNING_VERSION = "temporal_indicator_learning_v1"
INDICATOR_LEARNING_OPERATOR_VERSION = "1"
TIMEFRAME_POLICY_DEFAULT = ("M5", "M15", "H1")
EVIDENCE_LOOKBACK_CHOICES = (1, 2, 3, 5)

GRAPH_BOUND_TIMEFRAME = "indicator_graph_bound_timeframe_v1"
EVIDENCE_LOOKBACK = "indicator_evidence_lookback_v1"
TA_PERIOD = "indicator_ta_period_v1"
SEMANTIC_RANGE = "indicator_semantic_range_v1"
FAMILY_SUBSTITUTION = "indicator_family_substitution_v1"
EVIDENCE_WEIGHT = "evidence_contribution_weight_v1"
EVIDENCE_MEMBERSHIP = "evidence_group_membership_v1"
INDICATOR_INSTANCE = "indicator_instance_structure_v1"

_WEIGHT_MULTIPLIERS = (0.5, 0.75, 1.25, 1.5)
_MAX_WEIGHT = 10.0
_MAX_EVIDENCE_GROUP_MEMBERS = 3
_MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION = 3


def _items(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in payload.get("indicators") or []:
        if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping):
            continue
        meta = dict(item["meta"])
        indicator_id = str(meta.get("id") or "").strip()
        if not indicator_id or indicator_id in result:
            raise TemporalDiscoveryContractError("indicator learning catalog IDs must be unique")
        result[indicator_id] = {"meta": meta, "config": _clone(item.get("config") or {}, name="catalog indicator config")}
    if not result:
        raise TemporalDiscoveryContractError("indicator learning catalog requires indicators")
    return result


def _signature(value: Any) -> str:
    return canonical_sha256(_clone(value, name="catalog compatibility metadata"))


def _catalog_equivalent_meta(value: Mapping[str, Any]) -> dict[str, Any]:
    """Instance identity is authored at hydration time, not catalog authority."""
    result = _clone(value, name="indicator metadata")
    result.pop("instanceId", None)
    # The pair-authority builder intentionally removes catalog documentation
    # before a module grammar snapshots profile material.  Documentation never
    # alters output capability and must not make a real catalog instance look
    # stale to the learning surface.
    result.pop("docs", None)
    return result


def _strip_model_only_nulls(value: Any, catalog_shape: Any) -> Any:
    """Remove only null fields added by the resolved Pydantic model.

    The current Dashboard hydration model materializes optional descriptor keys
    such as ``options: null`` that are absent from the JSON catalog.  They are
    not source drift.  Any non-null unknown field remains and fails equality.
    """
    if isinstance(value, Mapping) and isinstance(catalog_shape, Mapping):
        return {
            key: _strip_model_only_nulls(item, catalog_shape[key]) if key in catalog_shape else item
            for key, item in value.items()
            if key in catalog_shape or item is not None
        }
    if isinstance(value, list) and isinstance(catalog_shape, list) and len(value) == len(catalog_shape):
        return [_strip_model_only_nulls(item, catalog_shape[index]) for index, item in enumerate(value)]
    return value


def _normalize_model_numbers(value: Any) -> Any:
    """Pydantic resolves numeric catalog values to floats in profile dumps."""
    if isinstance(value, Mapping):
        return {key: _normalize_model_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_model_numbers(item) for item in value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return value


def _catalog_meta_matches(authored: Mapping[str, Any], catalog: Mapping[str, Any]) -> bool:
    catalog_meta = _catalog_equivalent_meta(catalog)
    return _signature(_normalize_model_numbers(_strip_model_only_nulls(_catalog_equivalent_meta(authored), catalog_meta))) == _signature(_normalize_model_numbers(catalog_meta))


class IndicatorLearningCatalog:
    """Small immutable view that exposes only catalog-justified mutations."""

    def __init__(self, payload: Mapping[str, Any], *, timeframe_policy: Sequence[str] = TIMEFRAME_POLICY_DEFAULT) -> None:
        self.payload = _clone(payload, name="indicator learning catalog")
        frames = self.payload.get("timeframes")
        if not isinstance(frames, Mapping):
            raise TemporalDiscoveryContractError("indicator learning catalog requires timeframes")
        available = {str(key).upper() for key in frames if str(key).strip()}
        requested = tuple(dict.fromkeys(str(value).upper() for value in timeframe_policy))
        if not requested or any(value not in available for value in requested):
            raise TemporalDiscoveryContractError("indicator learning timeframe policy is not catalog-backed")
        self.timeframe_policy = tuple(sorted(requested))
        self.indicators = _items(self.payload)
        self.catalog_sha256 = canonical_sha256({"payload": self.payload, "timeframePolicy": list(self.timeframe_policy)})

    def entry(self, indicator_id: str) -> dict[str, Any] | None:
        value = self.indicators.get(indicator_id)
        return _clone(value, name="catalog indicator entry") if value is not None else None

    def deferred_substitution_dispositions(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Explain unavailable typed replacement without weakening contracts."""
        result: list[dict[str, Any]] = []
        for index, indicator in enumerate(profile.get("indicators") or []):
            if not isinstance(indicator, Mapping) or not isinstance(indicator.get("meta"), Mapping):
                continue
            indicator_id = str(indicator["meta"].get("id") or "")
            source = self.entry(indicator_id)
            if source is None:
                result.append({"indicatorIndex": index, "indicatorId": indicator_id, "disposition": "deferred", "reason": "source_catalog_metadata_missing"})
                continue
            missing = _compatibility_missing(source["meta"])
            if missing:
                result.append({"indicatorIndex": index, "indicatorId": indicator_id, "disposition": "deferred", "reason": "source_compatibility_metadata_missing", "missing": missing})
                continue
            instance_id = str(indicator["meta"].get("instanceId") or "")
            fuzzy, event, scalar = _instance_binding_shape(profile, instance_id)
            if not (fuzzy or event or scalar):
                continue
            contract = _binding_contract(source["meta"], fuzzy=fuzzy, event=event, scalar=scalar)
            if contract is not None:
                peers = [
                    replacement_id
                    for replacement_id, replacement in self.indicators.items()
                    if replacement_id != indicator_id
                    and not _compatibility_missing(replacement["meta"])
                    and _binding_contract(replacement["meta"], fuzzy=fuzzy, event=event, scalar=scalar) == contract
                ]
                if peers:
                    continue
            if event:
                result.append({"indicatorIndex": index, "indicatorId": indicator_id, "disposition": "deferred", "reason": "event_output_schema_metadata_not_admitted"})
            elif scalar:
                result.append({"indicatorIndex": index, "indicatorId": indicator_id, "disposition": "deferred", "reason": "management_scalar_binding_replacement_not_admitted"})
            elif fuzzy:
                result.append({"indicatorIndex": index, "indicatorId": indicator_id, "disposition": "deferred", "reason": "fuzzy_evidence_capability_not_admitted"})
        return result


def _bound_instances(profile: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    evidence: set[str] = set()
    events: set[str] = set()
    for group in graph.get("evidenceGroups") or []:
        if isinstance(group, Mapping):
            evidence.update(str(item) for item in group.get("indicatorInstanceIds") or [] if str(item))
    for binding in graph.get("eventBindings") or []:
        if isinstance(binding, Mapping) and binding.get("indicatorInstanceId"):
            events.add(str(binding["indicatorInstanceId"]))
    return evidence, events


def _scalar_bound_instances(profile: Mapping[str, Any]) -> set[str]:
    library = ((profile.get("executionConfig") or {}).get("managementLibrary") or {}) if isinstance(profile.get("executionConfig"), Mapping) else {}
    return {str(binding.get("indicatorInstanceId")) for binding in library.get("scalarBindings") or [] if isinstance(binding, Mapping) and binding.get("indicatorInstanceId")}


def _talib_values(config: Mapping[str, Any]) -> dict[str, Any]:
    return {str(item.get("name")): item.get("value") for item in config.get("talibConfig") or [] if isinstance(item, Mapping) and item.get("name")}


def _replace_talib(config: dict[str, Any], name: str, value: Any) -> None:
    rows = config.get("talibConfig")
    if not isinstance(rows, list):
        raise TemporalDiscoveryContractError("indicator config requires talibConfig list")
    for row in rows:
        if isinstance(row, dict) and row.get("name") == name:
            row["value"] = value
            return
    raise TemporalDiscoveryContractError("catalog period descriptor is absent from indicator config")


def _period_choices(meta: Mapping[str, Any], config: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
    current = _talib_values(config)
    for descriptor in meta.get("talibMeta") or []:
        if not isinstance(descriptor, Mapping):
            continue
        name = str(descriptor.get("name") or "")
        if "period" not in name.lower() or name not in current:
            continue
        if descriptor.get("uiType") not in {"integer_slider", "float_slider"}:
            continue
        try:
            nominal = descriptor["default"]
            minimum = descriptor["min"]
            maximum = descriptor["max"]
            if not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (nominal, minimum, maximum)):
                continue
        except KeyError:
            continue
        marks = sorted({item.get("value") for item in descriptor.get("marks") or [] if isinstance(item, Mapping) and isinstance(item.get("value"), (int, float))})
        fast = max((value for value in marks if value < nominal), default=minimum)
        slow = min((value for value in marks if value > nominal), default=maximum)
        for label, value in (("fast", fast), ("nominal", nominal), ("slow", slow)):
            if value != current[name]:
                yield {"parameter": name, "choice": label, "before": current[name], "after": value, "descriptor": {"name": name, "default": nominal, "min": minimum, "max": maximum}}


def _period_order_valid(config: Mapping[str, Any]) -> bool:
    values = _talib_values(config)
    fast = [float(value) for name, value in values.items() if name.lower().startswith("fast") and isinstance(value, (int, float))]
    slow = [float(value) for name, value in values.items() if name.lower().startswith("slow") and isinstance(value, (int, float))]
    return not fast or not slow or max(fast) < min(slow)


def _range_choices(meta: Mapping[str, Any], config: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
    if meta.get("usesRangeConfiguration") is not True or not isinstance(meta.get("valueRange"), Mapping):
        return
    value_range = meta["valueRange"]
    try:
        minimum, maximum, step, width = (float(value_range[key]) for key in ("min", "max", "step", "minRange"))
    except (KeyError, TypeError, ValueError):
        return
    if step <= 0 or width <= 0:
        return
    ranges = config.get("ranges")
    if not isinstance(ranges, Mapping):
        return
    for side in ("buy", "sell"):
        prior = ranges.get(side)
        if not isinstance(prior, list) or len(prior) != 2 or not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in prior):
            continue
        lower, upper = float(prior[0]), float(prior[1])
        for label, candidate in (("shift_lower", [lower - step, upper - step]), ("shift_higher", [lower + step, upper + step]), ("widen", [lower - step, upper + step]), ("narrow", [lower + step, upper - step])):
            if candidate[0] < minimum or candidate[1] > maximum or candidate[1] - candidate[0] < width or candidate == [lower, upper]:
                continue
            yield {"side": side, "choice": label, "before": list(prior), "after": candidate, "catalogValueRange": {"min": minimum, "max": maximum, "step": step, "minRange": width}}


def _compatibility_missing(meta: Mapping[str, Any]) -> list[str]:
    # ``strategyRole`` and ``signalRole`` remain useful priors for analysis,
    # but are deliberately not a hard search eligibility boundary.  A role
    # label is not an output-type contract.
    required = ("signalPersistence", "valueRange", "requiredPaddingBars")
    return [key for key in required if key not in meta]


_SUBSTITUTION_KEYS = (
    "substitutionClass", "polarity", "scoreUnit", "rawUnit",
    "eventOutputSchema", "persistenceCompatibility",
)


def _substitution_contract(meta: Mapping[str, Any]) -> dict[str, Any] | None:
    value = meta.get("familySubstitution")
    if not isinstance(value, Mapping) or any(key not in value for key in _SUBSTITUTION_KEYS):
        return None
    normalized = _clone(dict(value), name="family substitution contract")
    if not all(isinstance(normalized[key], str) and normalized[key] for key in ("substitutionClass", "polarity", "scoreUnit", "rawUnit", "persistenceCompatibility")):
        return None
    if normalized["persistenceCompatibility"] != meta.get("signalPersistence"):
        return None
    return normalized


def _substitution_contract_missing(meta: Mapping[str, Any]) -> list[str]:
    value = meta.get("familySubstitution")
    if not isinstance(value, Mapping):
        return ["familySubstitution"]
    return [key for key in _SUBSTITUTION_KEYS if key not in value]


def _numeric_range(meta: Mapping[str, Any]) -> dict[str, float] | None:
    value = meta.get("valueRange")
    if not isinstance(value, Mapping):
        return None
    try:
        minimum, maximum, step, width = (float(value[key]) for key in ("min", "max", "step", "minRange"))
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(item) for item in (minimum, maximum, step, width)) or step <= 0.0 or width <= 0.0 or maximum - minimum < width:
        return None
    return {"min": minimum, "max": maximum, "step": step, "minRange": width}


def _scalar_output_contract(meta: Mapping[str, Any]) -> list[dict[str, str]] | None:
    rows = meta.get("managementScalarOutputs")
    if not isinstance(rows, list) or not rows:
        return None
    normalized: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None
        output_key, value_kind, unit = (str(row.get(key) or "").strip() for key in ("outputKey", "valueKind", "unit"))
        if not output_key or value_kind not in {"price_level", "price_distance"}:
            return None
        expected_unit = "price" if value_kind == "price_level" else "price_distance"
        if unit != expected_unit:
            return None
        normalized.append({"outputKey": output_key, "valueKind": value_kind, "unit": unit})
    if len({(row["outputKey"], row["valueKind"], row["unit"]) for row in normalized}) != len(normalized):
        return None
    return sorted(normalized, key=lambda row: (row["outputKey"], row["valueKind"], row["unit"]))


def _fuzzy_evidence_contract(meta: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the contract for a score that can join a fuzzy evidence group.

    This is intentionally capability-based: state persistence plus an
    authored numeric score range is the Dashboard's actual fuzzy-input
    surface.  Strategy and signal-role labels are retained in plans as priors,
    never compared to decide eligibility.
    """
    if meta.get("signalPersistence") != "state" or meta.get("usesRangeConfiguration") is not True:
        return None
    numeric_range = _numeric_range(meta)
    if numeric_range is None:
        return None
    explicit = meta.get("familySubstitution")
    scalar_outputs = _scalar_output_contract(meta)
    scalar_shape: dict[str, Any] = {"scalarOutputs": scalar_outputs} if scalar_outputs is not None else {"scalarOutputs": []}
    if explicit is None:
        return {"kind": "fuzzy_evidence", "schema": "derived_ranged_state_score_v1", **scalar_shape}
    contract = _substitution_contract(meta)
    if contract is None or not isinstance(contract.get("eventOutputSchema"), Mapping):
        return None
    # A catalog that supplies an explicit contract owns its compatibility
    # declaration.  Include every technical field but no taxonomy labels.
    return {"kind": "fuzzy_evidence", "schema": "explicit_family_substitution_v1", "contract": contract, **scalar_shape}


def _event_contract(meta: Mapping[str, Any]) -> dict[str, Any] | None:
    """Admit raw-event replacement only with concrete directional tokens."""
    contract = _substitution_contract(meta)
    schema = contract.get("eventOutputSchema") if contract is not None else None
    if meta.get("signalPersistence") not in {"event", "event-with-lookback"} or not isinstance(schema, Mapping):
        return None
    if schema.get("kind") != "directional_tokens":
        return None
    long_output, short_output = (str(schema.get(key) or "").strip() for key in ("longOutput", "shortOutput"))
    if not long_output or not short_output or long_output == short_output:
        return None
    return {"kind": "raw_event", "signalPersistence": meta["signalPersistence"], "eventOutputSchema": {"kind": "directional_tokens", "longOutput": long_output, "shortOutput": short_output}}


def _management_scalar_contract(meta: Mapping[str, Any]) -> dict[str, Any] | None:
    outputs = _scalar_output_contract(meta)
    if outputs is None or meta.get("signalPersistence") != "state":
        return None
    return {"kind": "scalar_management", "outputs": outputs}


def _binding_contract(meta: Mapping[str, Any], *, fuzzy: bool, event: bool, scalar: bool) -> dict[str, Any] | None:
    """Construct the exact technical capability contract required by bindings."""
    if event and (fuzzy or scalar):
        return None
    required: list[dict[str, Any]] = []
    if fuzzy:
        value = _fuzzy_evidence_contract(meta)
        if value is None:
            return None
        required.append(value)
    if event:
        value = _event_contract(meta)
        if value is None:
            return None
        required.append(value)
    if scalar:
        value = _management_scalar_contract(meta)
        if value is None:
            return None
        required.append(value)
    return {"schemaVersion": "temporal_indicator_binding_contract_v1", "capabilities": required} if required else None


def _range_signature(meta: Mapping[str, Any]) -> str:
    numeric_range = _numeric_range(meta)
    return _signature(numeric_range) if numeric_range is not None else ""


def _range_sides(profile: Mapping[str, Any], instance_id: str) -> set[str]:
    """Return score sides that can reach this evidence instance, fail closed."""
    evidence, _events = _bound_instances(profile)
    if instance_id not in evidence:
        return set()
    direction = str(profile.get("directionMode") or "")
    if direction == "long":
        return {"buy"}
    if direction == "short":
        return {"sell"}
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    arbitration = graph.get("entryArbitration") if isinstance(graph, Mapping) else None
    if direction != "both" or not isinstance(arbitration, Mapping) or not isinstance(arbitration.get("modules"), list):
        return set()
    group_owner = {
        str(group_id): str(module.get("direction"))
        for module in arbitration["modules"] if isinstance(module, Mapping)
        for group_id in module.get("evidenceGroupIds") or []
        if str(module.get("direction")) in {"long", "short"}
    }
    owners = {
        group_owner.get(str(group.get("id")))
        for group in graph.get("evidenceGroups") or [] if isinstance(group, Mapping)
        if instance_id in {str(value) for value in group.get("indicatorInstanceIds") or []}
    }
    return {"buy" if owner == "long" else "sell" for owner in owners if owner in {"long", "short"}}


def _reachable_evidence_groups(profile: Mapping[str, Any]) -> set[str]:
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    initial = str(graph.get("initialStateId") or "")
    transitions = [item for item in graph.get("transitions") or [] if isinstance(item, Mapping)]
    reachable = {initial} if initial else set()
    changed = True
    while changed:
        changed = False
        for transition in transitions:
            if str(transition.get("sourceStateId") or "") in reachable:
                target = str(transition.get("destinationStateId") or "")
                if target and target not in reachable:
                    reachable.add(target); changed = True
    groups: set[str] = set()
    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            if value.get("kind") in {"evidence_at_least", "evidence_below"} and value.get("groupId"):
                groups.add(str(value["groupId"]))
            for item in value.values(): visit(item)
        elif isinstance(value, list):
            for item in value: visit(item)
    for transition in transitions:
        if str(transition.get("sourceStateId") or "") in reachable:
            visit(transition.get("guard"))
    return groups


def _module_owner(profile: Mapping[str, Any], *, group_id: str | None = None, instance_id: str | None = None) -> str | None:
    if str(profile.get("directionMode") or "") != "both":
        return None
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    arbitration = graph.get("entryArbitration") if isinstance(graph, Mapping) else None
    modules = arbitration.get("modules") if isinstance(arbitration, Mapping) else None
    if not isinstance(modules, list): return None
    owners: set[str] = set()
    for module in modules:
        if not isinstance(module, Mapping): return None
        side = str(module.get("direction") or "")
        if side not in {"long", "short"} or not isinstance(module.get("indicatorIds"), list): return None
        ids = {str(value) for value in module["indicatorIds"]}
        groups = {str(value) for value in module.get("evidenceGroupIds") or []}
        if (instance_id is not None and instance_id in ids) or (group_id is not None and group_id in groups): owners.add(side)
    return next(iter(owners)) if len(owners) == 1 else None


def _aligned_score_signature(item: Mapping[str, Any]) -> str | None:
    meta, config = item.get("meta"), item.get("config")
    if not isinstance(meta, Mapping) or not isinstance(config, Mapping): return None
    if meta.get("signalPersistence") != "state" or config.get("isActive") is not True or config.get("useFormingBar") is not False: return None
    return _signature({"signalRole": meta.get("signalRole"), "signalPersistence": meta.get("signalPersistence"), "valueRange": meta.get("valueRange"), "usesRangeConfiguration": meta.get("usesRangeConfiguration"), "scaleStrategy": meta.get("scaleStrategy"), "timeframe": config.get("timeframe")})


def _evidence_weight_eligible(profile: Mapping[str, Any], instance_id: str) -> bool:
    evidence, events = _bound_instances(profile)
    if instance_id not in evidence or instance_id in events or instance_id in _scalar_bound_instances(profile): return False
    item = next((value for value in profile.get("indicators") or [] if isinstance(value, Mapping) and str((value.get("meta") or {}).get("instanceId") or "") == instance_id), None)
    if not isinstance(item, Mapping) or _aligned_score_signature(item) is None: return False
    try:
        weight = float((item.get("config") or {}).get("weight"))
    except (TypeError, ValueError): return False
    reachable_groups = _reachable_evidence_groups(profile)
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    contributes_to_non_singleton_reachable_group = any(
        isinstance(group, Mapping)
        and str(group.get("id") or "") in reachable_groups
        and len(group.get("indicatorInstanceIds") or []) > 1
        and instance_id in {
            str(value) for value in group.get("indicatorInstanceIds") or []
        }
        for group in graph.get("evidenceGroups") or []
    )
    return (
        math.isfinite(weight)
        and 0.0 < weight <= _MAX_WEIGHT
        # A singleton group has no relative evidence contribution, so weight
        # changes are observational no-ops and must not consume search budget.
        and contributes_to_non_singleton_reachable_group
    )


def _bound_count_by_direction(profile: Mapping[str, Any]) -> dict[str, int] | None:
    """Count fuzzy-evidence instances without weakening the v3 boundary.

    Event and management bindings have incompatible execution/output semantics,
    and are governed by their own closed contracts.  The three-instance budget
    limits the mutable fuzzy evidence surface, allowing a 1/2/3-member group
    to coexist with the module's raw event trigger.
    """
    evidence, events = _bound_instances(profile)
    bound = evidence
    direction = str(profile.get("directionMode") or "")
    if direction in {"long", "short"}:
        return {direction: len(bound)}
    if direction != "both":
        return None
    graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
    arbitration = graph.get("entryArbitration") if isinstance(graph, Mapping) else None
    modules = arbitration.get("modules") if isinstance(arbitration, Mapping) else None
    if not isinstance(modules, list):
        return None
    ownership: dict[str, str] = {}
    for module in modules:
        if not isinstance(module, Mapping):
            return None
        side = str(module.get("direction") or "")
        ids = module.get("indicatorIds")
        if side not in {"long", "short"} or not isinstance(ids, list):
            return None
        for instance in ids:
            token = str(instance or "")
            if not token or token in ownership:
                return None
            ownership[token] = side
    if not bound.issubset(ownership):
        return None
    return {side: sum(1 for instance in bound if ownership[instance] == side) for side in ("long", "short")}


def _bound_instance_cap_ok(profile: Mapping[str, Any]) -> bool:
    counts = _bound_count_by_direction(profile)
    # Partial composite fixtures do not expose ownership manifests; no
    # instance-structural operation is admitted there, but legacy per-field
    # mutations remain independently auditable.
    return counts is None or all(value <= _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION for value in counts.values())


def _instance_binding_shape(profile: Mapping[str, Any], instance_id: str) -> tuple[bool, bool, bool]:
    evidence, events = _bound_instances(profile)
    scalar = _scalar_bound_instances(profile)
    return instance_id in evidence, instance_id in events, instance_id in scalar


def _fuzzy_group_member_eligible(profile: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
    meta, config = item.get("meta"), item.get("config")
    if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
        return False
    instance = str(meta.get("instanceId") or "")
    fuzzy, event, scalar = _instance_binding_shape(profile, instance)
    # Evidence membership is never allowed to smuggle an event or an execution
    # scalar into a fuzzy group.  A pre-existing scalar+fuzzy dual-use instance
    # is left untouched, but no new dual-use topology is constructed here.
    return fuzzy and not event and not scalar and _binding_contract(meta, fuzzy=True, event=False, scalar=False) is not None and config.get("isActive") is True and config.get("useFormingBar") is False


def _profile_invariants(profile: Mapping[str, Any]) -> dict[str, bool]:
    evidence, events = _bound_instances(profile)
    indicators = [item for item in profile.get("indicators") or [] if isinstance(item, Mapping)]
    by_instance = {str((item.get("meta") or {}).get("instanceId") or ""): item for item in indicators}
    scalar = _scalar_bound_instances(profile)
    groups = [group for group in ((profile.get("graph") or {}).get("evidenceGroups") or []) if isinstance(group, Mapping)]
    def event_lookback_is_one(instance: str) -> bool:
        item = by_instance.get(instance)
        if not isinstance(item, Mapping):
            return False
        try:
            return int((item.get("config") or {}).get("lookbackBars", 1)) == 1
        except (TypeError, ValueError):
            return False
    return {
        "event_bound_lookback_is_one": all(event_lookback_is_one(instance) for instance in events),
        "bound_instances_exist": all(instance in by_instance for instance in evidence | events | scalar),
        "scalar_bindings_do_not_overlap_event_persistence": not bool(events & scalar),
        "event_bindings_do_not_overlap_fuzzy_evidence": not bool(events & evidence),
        "evidence_group_membership_is_closed": all(
            isinstance(group.get("indicatorInstanceIds"), list)
            and 1 <= len(group["indicatorInstanceIds"]) <= _MAX_EVIDENCE_GROUP_MEMBERS
            and len({str(value) for value in group["indicatorInstanceIds"]}) == len(group["indicatorInstanceIds"])
            and all(str(value) in by_instance for value in group["indicatorInstanceIds"])
            for group in groups
        ),
        "bound_indicator_instances_within_direction_cap": _bound_instance_cap_ok(profile),
        "indicator_instance_ids_are_unique": len(by_instance) == len(indicators) and "" not in by_instance,
    }


class _IndicatorOperator:
    operator_id = ""
    operator_version = INDICATOR_LEARNING_OPERATOR_VERSION

    def __init__(self, catalog: IndicatorLearningCatalog) -> None:
        self.catalog = catalog
        self.specification = {"schemaVersion": "temporal_indicator_learning_operator_spec_v1", "operatorId": self.operator_id, "operatorVersion": self.operator_version, "learningVersion": INDICATOR_LEARNING_VERSION, "catalogSha256": catalog.catalog_sha256}
        self.specification["operatorSpecSha256"] = canonical_sha256(self.specification)

    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    def _transform(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        raise NotImplementedError

    def _construction_relevant(self, profile: Mapping[str, Any], construction: Mapping[str, Any]) -> bool:
        return True

    def _static_report(self, parent: Mapping[str, Any], child: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
        checks = {
            **{f"parent_{key}": value for key, value in _profile_invariants(parent).items()},
            **{f"child_{key}": value for key, value in _profile_invariants(child).items()},
            "construction_remains_behaviorally_relevant": self._construction_relevant(parent, plan["construction"]),
            "scalar_binding_sources_preserved": _scalar_bound_instances(parent) == _scalar_bound_instances(child),
        }
        return finalize_audit(checks, operatorId=self.operator_id, operatorVersion=self.operator_version, planSha256=plan["planSha256"], childSourceProfileSha256=canonical_sha256(child))

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        parent = _clone(profile, name="indicator learning parent")
        if not all(_profile_invariants(parent).values()):
            return []
        parent_sha = canonical_sha256(parent)
        plans: dict[str, dict[str, Any]] = {}
        for construction in self._constructions(parent):
            construction = _clone(construction, name="indicator learning construction")
            if not self._construction_relevant(parent, construction):
                continue
            child, _trace = self._transform(parent, construction)
            if not all(_profile_invariants(child).values()):
                continue
            identity = {"schemaVersion": "temporal_indicator_learning_identity_v1", "operatorId": self.operator_id, "operatorVersion": self.operator_version, "learningVersion": INDICATOR_LEARNING_VERSION, "parentSourceProfileSha256": parent_sha, "catalogSha256": self.catalog.catalog_sha256, "construction": construction}
            plan = finalize_plan({"operatorId": self.operator_id, "operatorVersion": self.operator_version, "operatorSpecSha256": self.specification["operatorSpecSha256"], "parentSourceProfileSha256": parent_sha, "catalogSha256": self.catalog.catalog_sha256, "construction": construction, "constructionIdentitySha256": canonical_sha256(identity)})
            plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def _preview(self, profile: Mapping[str, Any], plan: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if plan not in self.enumerate_plans(profile):
            raise TemporalDiscoveryContractError(f"{self.operator_id} plan is not canonical and applicable")
        child, trace = self._transform(_clone(profile, name="indicator learning parent"), plan["construction"])
        if not all(_profile_invariants(child).values()):
            raise TemporalDiscoveryContractError(f"{self.operator_id} child violates indicator-learning invariants")
        return child, trace

    def preview(self, profile: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
        return self._preview(profile, plan)[0]

    def apply(self, profile: Mapping[str, Any], plan: Mapping[str, Any], *, parent_validated_program_sha256: str, child_validated_program_sha256: str) -> tuple[dict[str, Any], dict[str, Any]]:
        child, trace = self._preview(profile, plan)
        audit = self._static_report(profile, child, plan)
        if not audit["allChecksPassed"]:
            raise TemporalDiscoveryContractError(f"{self.operator_id} invariant audit failed")
        application = finalize_application({"operatorId": self.operator_id, "operatorVersion": self.operator_version, "operatorSpecSha256": self.specification["operatorSpecSha256"], "planSha256": plan["planSha256"], "constructionIdentitySha256": plan["constructionIdentitySha256"], "parentSourceProfileSha256": canonical_sha256(profile), "childSourceProfileSha256": canonical_sha256(child), "parentValidatedProgramSha256": parent_validated_program_sha256, "childValidatedProgramSha256": child_validated_program_sha256, "mutationTrace": trace, "staticInvariantReport": audit, "evidenceScope": {"marketReplayRun": False, "firedEvidence": "unmeasured", "activationEvidence": "unmeasured", "evidencePlanRotationRequired": True, "lakeScopeRegenerationRequired": True}})
        return child, application

    def audit(self, parent_profile: Mapping[str, Any], transformed_profile: Mapping[str, Any], application_record: Mapping[str, Any]) -> dict[str, Any]:
        application = _clone(application_record, name="indicator learning application")
        identity = application.pop("applicationSha256", None)
        plan = next((item for item in self.enumerate_plans(parent_profile) if item["planSha256"] == application.get("planSha256")), None)
        child, trace = self._preview(parent_profile, plan) if plan is not None else ({}, [])
        expected_static = self._static_report(parent_profile, transformed_profile, plan) if plan is not None else None
        embedded = application.get("staticInvariantReport")
        return finalize_audit({"application_identity_exact": isinstance(identity, str) and canonical_sha256(application) == identity, "plan_is_currently_applicable": plan is not None, "transformed_profile_exact": child == transformed_profile, "mutation_trace_exact": application.get("mutationTrace") == trace, "embedded_static_report_exact": embedded == expected_static, "embedded_static_report_passing": isinstance(embedded, Mapping) and embedded.get("allChecksPassed") is True, "recomputed_static_report_passing": isinstance(expected_static, Mapping) and expected_static.get("allChecksPassed") is True}, operatorId=self.operator_id, applicationSha256=identity)


class GraphBoundTimeframeOperator(_IndicatorOperator):
    operator_id = GRAPH_BOUND_TIMEFRAME
    def _constructions(self, profile: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        evidence, events = _bound_instances(profile)
        # A timeframe is graph/execution bound when it can affect either
        # evidence, event extraction, or a management scalar.  Restricting
        # this to evidence groups would leave event-only/scalar-only changes
        # outside the required evidence-plan rotation discipline.
        bound = evidence | events | _scalar_bound_instances(profile)
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping): continue
            meta, config = item.get("meta"), item.get("config")
            if not isinstance(meta, Mapping) or not isinstance(config, Mapping): continue
            instance, indicator_id, current = str(meta.get("instanceId") or ""), str(meta.get("id") or ""), str(config.get("timeframe") or "").upper()
            if instance not in bound or self.catalog.entry(indicator_id) is None or current not in self.catalog.timeframe_policy: continue
            for replacement in self.catalog.timeframe_policy:
                if replacement != current: yield {"kind": "graph_bound_timeframe", "indicatorIndex": index, "indicatorInstanceId": instance, "before": current, "after": replacement, "policy": list(self.catalog.timeframe_policy)}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]
        if item["meta"].get("instanceId") != construction["indicatorInstanceId"] or str(item["config"].get("timeframe") or "").upper() != construction["before"]: raise TemporalDiscoveryContractError("timeframe parent drift")
        item["config"]["timeframe"] = construction["after"]
        return child, [{"operation": "set_graph_bound_timeframe", "indicatorInstanceId": construction["indicatorInstanceId"], "before": construction["before"], "after": construction["after"]}]
    def _construction_relevant(self, profile, construction):
        evidence, events = _bound_instances(profile)
        return construction.get("indicatorInstanceId") in evidence | events | _scalar_bound_instances(profile)


class EvidenceLookbackOperator(_IndicatorOperator):
    operator_id = EVIDENCE_LOOKBACK
    def _constructions(self, profile):
        evidence, events = _bound_instances(profile)
        scalar_bound = _scalar_bound_instances(profile)
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping): continue
            meta, config = item.get("meta"), item.get("config")
            if not isinstance(meta, Mapping) or not isinstance(config, Mapping): continue
            instance = str(meta.get("instanceId") or "")
            if instance not in evidence or instance in events or instance in scalar_bound or int(config.get("lookbackBars", 1)) not in EVIDENCE_LOOKBACK_CHOICES: continue
            for replacement in EVIDENCE_LOOKBACK_CHOICES:
                if replacement != config.get("lookbackBars", 1): yield {"kind": "evidence_lookback", "indicatorIndex": index, "indicatorInstanceId": instance, "before": config.get("lookbackBars", 1), "after": replacement, "allowed": list(EVIDENCE_LOOKBACK_CHOICES)}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]
        if item["meta"].get("instanceId") != construction["indicatorInstanceId"] or item["config"].get("lookbackBars", 1) != construction["before"]: raise TemporalDiscoveryContractError("lookback parent drift")
        item["config"]["lookbackBars"] = construction["after"]
        return child, [{"operation": "set_evidence_lookback", "indicatorInstanceId": construction["indicatorInstanceId"], "before": construction["before"], "after": construction["after"]}]
    def _construction_relevant(self, profile, construction):
        evidence, events = _bound_instances(profile)
        instance = construction.get("indicatorInstanceId")
        return instance in evidence and instance not in events and instance not in _scalar_bound_instances(profile)


class TaPeriodOperator(_IndicatorOperator):
    operator_id = TA_PERIOD
    def _constructions(self, profile):
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping) or not isinstance(item.get("config"), Mapping): continue
            instance = str(item["meta"].get("instanceId") or "")
            evidence, events = _bound_instances(profile)
            if instance not in evidence | events | _scalar_bound_instances(profile): continue
            entry = self.catalog.entry(str(item["meta"].get("id") or ""))
            if entry is None or not _catalog_meta_matches(item["meta"], entry["meta"]): continue
            for change in _period_choices(entry["meta"], item["config"]):
                candidate = copy.deepcopy(item["config"]); _replace_talib(candidate, change["parameter"], change["after"])
                if _period_order_valid(candidate): yield {"kind": "ta_period", "indicatorIndex": index, "indicatorInstanceId": str(item["meta"].get("instanceId") or ""), "indicatorId": item["meta"]["id"], "change": change}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]; change = construction["change"]
        if item["meta"].get("instanceId") != construction["indicatorInstanceId"] or _talib_values(item["config"]).get(change["parameter"]) != change["before"]: raise TemporalDiscoveryContractError("period parent drift")
        _replace_talib(item["config"], change["parameter"], change["after"])
        if not _period_order_valid(item["config"]): raise TemporalDiscoveryContractError("period substitution violates fast/slow ordering")
        return child, [{"operation": "set_ta_period", "indicatorInstanceId": construction["indicatorInstanceId"], "parameter": change["parameter"], "choice": change["choice"], "before": change["before"], "after": change["after"]}]
    def _construction_relevant(self, profile, construction):
        evidence, events = _bound_instances(profile)
        return construction.get("indicatorInstanceId") in evidence | events | _scalar_bound_instances(profile)


class SemanticRangeOperator(_IndicatorOperator):
    operator_id = SEMANTIC_RANGE
    def _constructions(self, profile):
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping) or not isinstance(item.get("config"), Mapping): continue
            instance = str(item["meta"].get("instanceId") or "")
            sides = _range_sides(profile, instance)
            if not sides: continue
            entry = self.catalog.entry(str(item["meta"].get("id") or ""))
            if entry is None or not _catalog_meta_matches(item["meta"], entry["meta"]): continue
            for change in _range_choices(entry["meta"], item["config"]):
                if change["side"] in sides:
                    yield {"kind": "semantic_score_range", "indicatorIndex": index, "indicatorInstanceId": instance, "indicatorId": item["meta"]["id"], "change": change}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]; change = construction["change"]
        if item["meta"].get("instanceId") != construction["indicatorInstanceId"] or item["config"].get("ranges", {}).get(change["side"]) != change["before"]: raise TemporalDiscoveryContractError("semantic range parent drift")
        item["config"]["ranges"][change["side"]] = change["after"]
        return child, [{"operation": "set_semantic_score_range", "indicatorInstanceId": construction["indicatorInstanceId"], "side": change["side"], "choice": change["choice"], "before": change["before"], "after": change["after"], "catalogValueRange": change["catalogValueRange"]}]
    def _construction_relevant(self, profile, construction):
        return construction.get("change", {}).get("side") in _range_sides(profile, str(construction.get("indicatorInstanceId") or ""))


class EvidenceContributionWeightOperator(_IndicatorOperator):
    operator_id = EVIDENCE_WEIGHT
    def _constructions(self, profile):
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping): continue
            instance = str((item.get("meta") or {}).get("instanceId") or "")
            if not _evidence_weight_eligible(profile, instance): continue
            weight = float((item.get("config") or {}).get("weight"))
            for multiplier in _WEIGHT_MULTIPLIERS:
                replacement = round(weight * multiplier * 4.0) / 4.0
                if 0.0 < replacement <= _MAX_WEIGHT and replacement != weight:
                    yield {"kind": "evidence_contribution_weight", "indicatorIndex": index, "indicatorInstanceId": instance, "before": weight, "after": replacement, "multiplier": multiplier, "quantization": 0.25, "bounds": {"exclusiveMinimum": 0.0, "maximum": _MAX_WEIGHT}}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]
        if str(item["meta"].get("instanceId") or "") != construction["indicatorInstanceId"] or float(item["config"].get("weight")) != construction["before"]: raise TemporalDiscoveryContractError("weight parent drift")
        item["config"]["weight"] = construction["after"]
        return child, [{"operation": "set_evidence_contribution_weight", "indicatorInstanceId": construction["indicatorInstanceId"], "before": construction["before"], "after": construction["after"], "multiplier": construction["multiplier"], "quantization": construction["quantization"]}]
    def _construction_relevant(self, profile, construction):
        return _evidence_weight_eligible(profile, str(construction.get("indicatorInstanceId") or ""))


class EvidenceGroupMembershipOperator(_IndicatorOperator):
    operator_id = EVIDENCE_MEMBERSHIP
    def _group_owner_ok(self, profile, group_id: str, instance_id: str) -> bool:
        if str(profile.get("directionMode") or "") != "both": return True
        group_owner = _module_owner(profile, group_id=group_id)
        instance_owner = _module_owner(profile, instance_id=instance_id)
        return group_owner is not None and group_owner == instance_owner
    def _constructions(self, profile):
        graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
        events = _bound_instances(profile)[1]; scalar = _scalar_bound_instances(profile)
        indicators = {str((item.get("meta") or {}).get("instanceId") or ""): item for item in profile.get("indicators") or [] if isinstance(item, Mapping)}
        reachable = _reachable_evidence_groups(profile)
        for group_index, group in enumerate(graph.get("evidenceGroups") or []):
            if not isinstance(group, Mapping): continue
            group_id = str(group.get("id") or ""); members = [str(value) for value in group.get("indicatorInstanceIds") or []]
            if group_id not in reachable or not members or len(set(members)) != len(members): continue
            if any(
                member not in indicators
                or not _fuzzy_group_member_eligible(profile, indicators[member])
                for member in members
            ):
                continue
            member_contracts = {
                _fuzzy_evidence_contract(indicators[member]["meta"])
                and _signature(_fuzzy_evidence_contract(indicators[member]["meta"]))
                for member in members
            }
            if len(member_contracts) != 1:
                continue
            member_contract = next(iter(member_contracts))
            for member in members:
                if len(members) > 1 and member not in events and member not in scalar and self._group_owner_ok(profile, group_id, member):
                    yield {"kind": "remove_evidence_member", "groupIndex": group_index, "groupId": group_id, "indicatorInstanceId": member, "beforeMembers": members}
            if len(members) >= _MAX_EVIDENCE_GROUP_MEMBERS: continue
            for candidate_id, candidate in sorted(indicators.items()):
                if candidate_id in members or candidate_id in events or candidate_id in scalar: continue
                meta, config = candidate.get("meta"), candidate.get("config")
                if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
                    continue
                # This is fuzzy evidence construction, not a role-family
                # match.  Differently ranged state scores can coexist; each
                # retains its own catalog-backed semantic range.
                if _binding_contract(meta, fuzzy=True, event=False, scalar=False) is None or _signature(_fuzzy_evidence_contract(meta)) != member_contract or config.get("isActive") is not True or config.get("useFormingBar") is not False or not self._group_owner_ok(profile, group_id, candidate_id):
                    continue
                yield {"kind": "add_evidence_member", "groupIndex": group_index, "groupId": group_id, "indicatorInstanceId": candidate_id, "beforeMembers": members}
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); group = child["graph"]["evidenceGroups"][construction["groupIndex"]]
        if group.get("id") != construction["groupId"] or [str(value) for value in group.get("indicatorInstanceIds") or []] != construction["beforeMembers"]: raise TemporalDiscoveryContractError("membership parent drift")
        members = list(group["indicatorInstanceIds"]); instance = construction["indicatorInstanceId"]
        if construction["kind"] == "add_evidence_member":
            if instance in members or len(members) >= _MAX_EVIDENCE_GROUP_MEMBERS: raise TemporalDiscoveryContractError("membership add is invalid")
            members.append(instance); operation = "add_evidence_group_member"
        elif construction["kind"] == "remove_evidence_member":
            if instance not in members or len(members) <= 1: raise TemporalDiscoveryContractError("membership removal is invalid")
            members.remove(instance); operation = "remove_evidence_group_member"
        else: raise TemporalDiscoveryContractError("unknown membership construction")
        group["indicatorInstanceIds"] = sorted(members)
        return child, [{"operation": operation, "groupId": construction["groupId"], "indicatorInstanceId": instance, "beforeMembers": construction["beforeMembers"], "afterMembers": group["indicatorInstanceIds"]}]
    def _construction_relevant(self, profile, construction):
        return any(
            str(group.get("id") or "") == construction.get("groupId") and str(group.get("id") or "") in _reachable_evidence_groups(profile)
            for group in ((profile.get("graph") or {}).get("evidenceGroups") or []) if isinstance(group, Mapping)
        )


class FamilySubstitutionOperator(_IndicatorOperator):
    operator_id = FAMILY_SUBSTITUTION
    def _constructions(self, profile):
        evidence, events = _bound_instances(profile); scalar_bound = _scalar_bound_instances(profile)
        for index, item in enumerate(profile.get("indicators") or []):
            if not isinstance(item, Mapping) or not isinstance(item.get("meta"), Mapping) or not isinstance(item.get("config"), Mapping): continue
            source_id, instance = str(item["meta"].get("id") or ""), str(item["meta"].get("instanceId") or "")
            if instance not in evidence | events | scalar_bound: continue
            source = self.catalog.entry(source_id)
            if source is None or _compatibility_missing(source["meta"]) or not _catalog_meta_matches(item["meta"], source["meta"]): continue
            fuzzy, event, scalar = _instance_binding_shape(profile, instance)
            contract = _binding_contract(source["meta"], fuzzy=fuzzy, event=event, scalar=scalar)
            if contract is None: continue
            for replacement_id, replacement in sorted(self.catalog.indicators.items()):
                meta = replacement["meta"]
                if replacement_id == source_id or _compatibility_missing(meta): continue
                if _binding_contract(meta, fuzzy=fuzzy, event=event, scalar=scalar) != contract: continue
                yield {
                    "kind": "family_substitution", "indicatorIndex": index,
                    "indicatorInstanceId": instance, "beforeIndicatorId": source_id,
                    "afterIndicatorId": replacement_id, "capabilityContract": contract,
                    "eventBound": event, "evidenceBound": fuzzy, "scalarBound": scalar,
                    # Labels are visible to selection/analysis but never used
                    # above as an eligibility predicate.
                    "softRolePrior": {
                        "beforeStrategyRole": source["meta"].get("strategyRole"),
                        "afterStrategyRole": meta.get("strategyRole"),
                        "beforeSignalRole": source["meta"].get("signalRole"),
                        "afterSignalRole": meta.get("signalRole"),
                    },
                }
    def _transform(self, profile, construction):
        child = copy.deepcopy(profile); item = child["indicators"][construction["indicatorIndex"]]
        if item["meta"].get("instanceId") != construction["indicatorInstanceId"] or item["meta"].get("id") != construction["beforeIndicatorId"]: raise TemporalDiscoveryContractError("family parent drift")
        replacement = self.catalog.entry(construction["afterIndicatorId"])
        if replacement is None: raise TemporalDiscoveryContractError("replacement catalog metadata disappeared")
        old_config = item["config"]; replacement_config = replacement["config"]
        if not replacement_config: raise TemporalDiscoveryContractError("replacement config metadata missing")
        new_meta = copy.deepcopy(replacement["meta"]); new_meta["instanceId"] = construction["indicatorInstanceId"]
        new_config = copy.deepcopy(replacement_config)
        # Substitution is a one-axis causal panel: generic authored behavior
        # remains parent-owned.  The replacement contributes only its
        # family-specific TA configuration and its catalog metadata.
        for key in ("isActive", "useFormingBar", "timeframe", "lookbackBars", "weight"):
            if key in old_config: new_config[key] = old_config[key]
        # Thresholds only retain their prior meaning under the same numeric
        # semantic range.  Otherwise the replacement's catalog defaults are
        # intentionally used and the range operator can evolve them later.
        if _range_signature(replacement["meta"]) == _range_signature(item["meta"]):
            if "ranges" in old_config:
                new_config["ranges"] = copy.deepcopy(old_config["ranges"])
        item["meta"], item["config"] = new_meta, new_config
        return child, [{"operation": "substitute_indicator_family", "indicatorInstanceId": construction["indicatorInstanceId"], "beforeIndicatorId": construction["beforeIndicatorId"], "afterIndicatorId": construction["afterIndicatorId"], "capabilityContract": construction["capabilityContract"], "softRolePrior": construction["softRolePrior"]}]
    def _construction_relevant(self, profile, construction):
        evidence, events = _bound_instances(profile)
        return construction.get("indicatorInstanceId") in evidence | events | _scalar_bound_instances(profile)


def _new_instance_id(indicator_id: str, existing: set[str]) -> str:
    base = "fz_" + "".join(char.lower() if char.isalnum() else "_" for char in indicator_id).strip("_")
    base = base[:58] or "fz_indicator"
    ordinal = 1
    while True:
        candidate = f"{base}_{ordinal}"
        if candidate not in existing:
            return candidate
        ordinal += 1


class IndicatorInstanceOperator(_IndicatorOperator):
    """Atomically insert or remove a fuzzy-evidence indicator instance.

    Insertion also binds the new instance into a reachable fuzzy group, while
    removal deletes every fuzzy membership for the instance.  This prevents
    dead unbound inventory from accumulating and keeps every mutation useful.
    """

    operator_id = INDICATOR_INSTANCE

    def _constructions(self, profile):
        graph = profile.get("graph") if isinstance(profile.get("graph"), Mapping) else {}
        groups = graph.get("evidenceGroups") or []
        indicators = [item for item in profile.get("indicators") or [] if isinstance(item, Mapping)]
        by_instance = {
            str((item.get("meta") or {}).get("instanceId") or ""): item
            for item in indicators
        }
        existing = set(by_instance)
        evidence, events = _bound_instances(profile)
        scalar = _scalar_bound_instances(profile)
        reachable = _reachable_evidence_groups(profile)
        counts = _bound_count_by_direction(profile)
        if counts is None:
            return
        # A v2 module has one direction.  A v3 composite must have explicit
        # ownership and cannot borrow a candidate from the other module.
        direction = str(profile.get("directionMode") or "")
        for group_index, group in enumerate(groups):
            if not isinstance(group, Mapping):
                continue
            group_id = str(group.get("id") or "")
            members = [str(value) for value in group.get("indicatorInstanceIds") or []]
            if group_id not in reachable or not members or len(members) >= _MAX_EVIDENCE_GROUP_MEMBERS:
                continue
            if any(member not in by_instance or not _fuzzy_group_member_eligible(profile, by_instance[member]) for member in members):
                continue
            member_contracts = {
                _signature(_fuzzy_evidence_contract(by_instance[member]["meta"]))
                for member in members
            }
            if len(member_contracts) != 1:
                continue
            member_contract = next(iter(member_contracts))
            owner = _module_owner(profile, group_id=group_id)
            if direction == "both":
                if owner is None or counts.get(owner, _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION) >= _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION:
                    continue
            elif counts.get(direction, _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION) >= _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION:
                continue
            existing_identity = {
                str((by_instance[member].get("meta") or {}).get("id") or "")
                for member in members
            }
            for indicator_id, entry in sorted(self.catalog.indicators.items()):
                meta, config = entry["meta"], entry["config"]
                if indicator_id in existing_identity or _fuzzy_evidence_contract(meta) is None or _signature(_fuzzy_evidence_contract(meta)) != member_contract:
                    continue
                if not isinstance(config, Mapping) or config.get("isActive") is not True:
                    continue
                instance_id = _new_instance_id(indicator_id, existing)
                yield {
                    "kind": "insert_fuzzy_indicator_instance", "groupIndex": group_index,
                    "groupId": group_id, "indicatorId": indicator_id,
                    "indicatorInstanceId": instance_id, "beforeMembers": members,
                    "softRolePrior": {"strategyRole": meta.get("strategyRole"), "signalRole": meta.get("signalRole")},
                }

        # Delete only a pure fuzzy instance, and only when every affected
        # group remains non-empty.  Raw event and scalar-management identities
        # are deliberately immutable on this surface.
        for instance_id, item in sorted(by_instance.items()):
            if not instance_id or instance_id in events or instance_id in scalar or instance_id not in evidence:
                continue
            fuzzy, event, scalar_binding = _instance_binding_shape(profile, instance_id)
            meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
            if not fuzzy or event or scalar_binding or _binding_contract(meta, fuzzy=True, event=False, scalar=False) is None:
                continue
            affected = [
                (index, group)
                for index, group in enumerate(groups)
                if isinstance(group, Mapping) and instance_id in {str(value) for value in group.get("indicatorInstanceIds") or []}
            ]
            if not affected or any(len(group.get("indicatorInstanceIds") or []) <= 1 for _index, group in affected):
                continue
            if not any(str(group.get("id") or "") in reachable for _index, group in affected):
                continue
            if direction == "both" and _module_owner(profile, instance_id=instance_id) is None:
                continue
            yield {
                "kind": "remove_fuzzy_indicator_instance", "indicatorInstanceId": instance_id,
                "indicatorId": str(meta.get("id") or ""),
                "affectedGroups": [
                    {"groupIndex": index, "groupId": str(group.get("id") or ""), "beforeMembers": [str(value) for value in group.get("indicatorInstanceIds") or []]}
                    for index, group in affected
                ],
            }

    def _transform(self, profile, construction):
        child = copy.deepcopy(profile)
        graph = child.get("graph") if isinstance(child.get("graph"), Mapping) else None
        if not isinstance(graph, dict) or not isinstance(child.get("indicators"), list):
            raise TemporalDiscoveryContractError("indicator instance parent shape is invalid")
        kind = construction.get("kind")
        if kind == "insert_fuzzy_indicator_instance":
            group = graph.get("evidenceGroups", [])[construction["groupIndex"]]
            if not isinstance(group, dict) or group.get("id") != construction["groupId"] or [str(value) for value in group.get("indicatorInstanceIds") or []] != construction["beforeMembers"]:
                raise TemporalDiscoveryContractError("indicator insertion parent drift")
            if any(str((item.get("meta") or {}).get("instanceId") or "") == construction["indicatorInstanceId"] for item in child["indicators"] if isinstance(item, Mapping)):
                raise TemporalDiscoveryContractError("indicator insertion instance id already exists")
            entry = self.catalog.entry(str(construction["indicatorId"]))
            if entry is None or _fuzzy_evidence_contract(entry["meta"]) is None:
                raise TemporalDiscoveryContractError("indicator insertion catalog capability disappeared")
            item = {"meta": copy.deepcopy(entry["meta"]), "config": copy.deepcopy(entry["config"])}
            item["meta"]["instanceId"] = construction["indicatorInstanceId"]
            # Catalog defaults omit this runtime-only safety field.  Every
            # constructed fuzzy score is explicitly closed-bar based.
            item["config"]["useFormingBar"] = False
            child["indicators"].append(item)
            group["indicatorInstanceIds"] = sorted([*group["indicatorInstanceIds"], construction["indicatorInstanceId"]])
            return child, [{"operation": "insert_fuzzy_indicator_instance", "indicatorInstanceId": construction["indicatorInstanceId"], "indicatorId": construction["indicatorId"], "groupId": construction["groupId"], "beforeMembers": construction["beforeMembers"], "afterMembers": group["indicatorInstanceIds"], "softRolePrior": construction["softRolePrior"]}]
        if kind == "remove_fuzzy_indicator_instance":
            instance_id = construction["indicatorInstanceId"]
            target = [item for item in child["indicators"] if isinstance(item, Mapping) and str((item.get("meta") or {}).get("instanceId") or "") == instance_id]
            if len(target) != 1 or str((target[0].get("meta") or {}).get("id") or "") != construction["indicatorId"]:
                raise TemporalDiscoveryContractError("indicator removal parent drift")
            traces = []
            for affected in construction["affectedGroups"]:
                group = graph.get("evidenceGroups", [])[affected["groupIndex"]]
                if not isinstance(group, dict) or group.get("id") != affected["groupId"] or [str(value) for value in group.get("indicatorInstanceIds") or []] != affected["beforeMembers"]:
                    raise TemporalDiscoveryContractError("indicator removal group drift")
                group["indicatorInstanceIds"] = [value for value in group["indicatorInstanceIds"] if value != instance_id]
                traces.append({"groupId": affected["groupId"], "beforeMembers": affected["beforeMembers"], "afterMembers": group["indicatorInstanceIds"]})
            child["indicators"] = [item for item in child["indicators"] if item not in target]
            return child, [{"operation": "remove_fuzzy_indicator_instance", "indicatorInstanceId": instance_id, "indicatorId": construction["indicatorId"], "affectedGroups": traces}]
        raise TemporalDiscoveryContractError("unknown indicator instance construction")

    def _construction_relevant(self, profile, construction):
        # Re-enumeration is the canonical, drift-safe relevance proof.  This
        # lightweight check excludes stale group references before transform.
        if construction.get("kind") == "insert_fuzzy_indicator_instance":
            return str(construction.get("groupId") or "") in _reachable_evidence_groups(profile)
        if construction.get("kind") == "remove_fuzzy_indicator_instance":
            return any(str(item.get("groupId") or "") in _reachable_evidence_groups(profile) for item in construction.get("affectedGroups") or [] if isinstance(item, Mapping))
        return False


class IndicatorLearningRegistry:
    def __init__(self, catalog: Mapping[str, Any] | IndicatorLearningCatalog, *, timeframe_policy: Sequence[str] = TIMEFRAME_POLICY_DEFAULT) -> None:
        self.catalog = catalog if isinstance(catalog, IndicatorLearningCatalog) else IndicatorLearningCatalog(catalog, timeframe_policy=timeframe_policy)
        self._operators = (GraphBoundTimeframeOperator(self.catalog), EvidenceLookbackOperator(self.catalog), TaPeriodOperator(self.catalog), SemanticRangeOperator(self.catalog), EvidenceContributionWeightOperator(self.catalog), EvidenceGroupMembershipOperator(self.catalog), IndicatorInstanceOperator(self.catalog), FamilySubstitutionOperator(self.catalog))
        self.structural_registry = StructuralOperatorRegistry(self._operators)
        self.policy = {"schemaVersion": "temporal_indicator_learning_policy_v1", "learningVersion": INDICATOR_LEARNING_VERSION, "catalogSha256": self.catalog.catalog_sha256, "timeframePolicy": list(self.catalog.timeframe_policy), "evidenceLookbackChoices": list(EVIDENCE_LOOKBACK_CHOICES), "maxBoundFuzzyInstancesPerDirection": _MAX_BOUND_INDICATOR_INSTANCES_PER_DIRECTION, "maxEvidenceGroupMembers": _MAX_EVIDENCE_GROUP_MEMBERS, "operatorIds": list(self.operator_ids)}
        self.policy["policySha256"] = canonical_sha256(self.policy)
    @property
    def operator_ids(self) -> tuple[str, ...]: return self.structural_registry.operator_ids
    def get(self, operator_id: str) -> _IndicatorOperator: return self.structural_registry.get(operator_id)  # type: ignore[return-value]
    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]: return self.structural_registry.enumerate_plans(profile)
    def deferred_dispositions(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]: return self.catalog.deferred_substitution_dispositions(profile)


__all__ = ["EVIDENCE_LOOKBACK", "EVIDENCE_LOOKBACK_CHOICES", "EVIDENCE_MEMBERSHIP", "EVIDENCE_WEIGHT", "FAMILY_SUBSTITUTION", "GRAPH_BOUND_TIMEFRAME", "INDICATOR_INSTANCE", "INDICATOR_LEARNING_OPERATOR_VERSION", "INDICATOR_LEARNING_VERSION", "IndicatorLearningCatalog", "IndicatorLearningRegistry", "SEMANTIC_RANGE", "TA_PERIOD", "TIMEFRAME_POLICY_DEFAULT"]
