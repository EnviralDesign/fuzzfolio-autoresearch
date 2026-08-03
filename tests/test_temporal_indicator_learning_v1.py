from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_indicator_learning_v1 import (
    EVIDENCE_LOOKBACK,
    EVIDENCE_MEMBERSHIP,
    EVIDENCE_WEIGHT,
    FAMILY_SUBSTITUTION,
    GRAPH_BOUND_TIMEFRAME,
    SEMANTIC_RANGE,
    TA_PERIOD,
    IndicatorLearningRegistry,
)


SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64


def _meta(indicator_id: str, *, role: str = "mean-reversion", signal_role: str = "setup", persistence: str = "state", scalar: bool = False) -> dict:
    result = {
        "id": indicator_id,
        "strategyRole": role,
        "signalRole": signal_role,
        "signalPersistence": persistence,
        "valueRange": {"min": 0, "max": 100, "step": 1, "minRange": 5},
        "usesRangeConfiguration": True,
        "inputs": ["close"],
        "requiredPaddingBars": 100,
        "talibMeta": [{"name": "timeperiod", "uiType": "integer_slider", "default": 14, "min": 2, "max": 30, "marks": [{"value": 5}, {"value": 14}, {"value": 20}]}],
        "familySubstitution": {"substitutionClass": "bounded_oscillator", "polarity": "symmetric", "scoreUnit": "native_score", "rawUnit": "index", "eventOutputSchema": {"kind": "none"}, "normalizationScale": {"kind": "normalized"}, "persistenceCompatibility": persistence},
    }
    if scalar:
        result["managementScalarOutputs"] = [{"outputKey": "level", "valueKind": "price_level", "unit": "price"}]
    return result


def _config(*, timeframe: str = "M5", lookback: int = 1, period: int = 14) -> dict:
    return {"isActive": True, "useFormingBar": False, "timeframe": timeframe, "lookbackBars": lookback, "ranges": {"buy": [20, 40], "sell": [60, 80]}, "talibConfig": [{"name": "timeperiod", "value": period}]}


def _catalog() -> dict:
    rsi = _meta("RSI_A")
    cci = _meta("CCI_A")
    event = _meta("EVENT_A", role="confirm", signal_role="trigger", persistence="event-with-lookback")
    event["valueRange"] = {"min": 0, "max": 1, "step": 1, "minRange": 1}
    event["usesRangeConfiguration"] = False
    scalar = _meta("SCALAR_A", role="filter", signal_role="filter", scalar=True)
    return {"timeframes": {"M5": {}, "M15": {}, "H1": {}, "H4": {}}, "indicators": [{"meta": rsi, "config": _config()}, {"meta": cci, "config": _config(period=20)}, {"meta": event, "config": _config()}, {"meta": scalar, "config": _config()}]}


def _instance(catalog: dict, indicator_id: str, instance_id: str, **kwargs) -> dict:
    entry = next(item for item in catalog["indicators"] if item["meta"]["id"] == indicator_id)
    meta = copy.deepcopy(entry["meta"]); meta["instanceId"] = instance_id
    config = copy.deepcopy(entry["config"]); config.update(kwargs)
    return {"meta": meta, "config": config}


def _profile() -> dict:
    catalog = _catalog()
    state = _instance(catalog, "RSI_A", "state", lookbackBars=3)
    state["config"]["weight"] = 0.7
    return {
        "directionMode": "long",
        "indicators": [state, _instance(catalog, "EVENT_A", "event", lookbackBars=1), _instance(catalog, "SCALAR_A", "scalar")],
        "executionConfig": {"managementLibrary": {"scalarBindings": [{"id": "bound", "indicatorInstanceId": "scalar", "outputKey": "level", "valueKind": "price_level", "availability": "completed_bar"}]}},
        "graph": {"initialStateId": "flat", "evidenceGroups": [{"id": "context", "indicatorInstanceIds": ["state"]}], "eventBindings": [{"id": "trigger", "indicatorInstanceId": "event", "longOutput": "bullish", "shortOutput": "bearish"}], "transitions": [{"id": "uses_context", "sourceStateId": "flat", "destinationStateId": "flat", "guard": {"kind": "evidence_at_least", "groupId": "context", "thresholdPercent": 50}}]},
    }


def _first(registry: IndicatorLearningRegistry, profile: dict, operator_id: str) -> dict:
    values = registry.get(operator_id).enumerate_plans(profile)
    assert values
    assert values == registry.get(operator_id).enumerate_plans(copy.deepcopy(profile))
    return values[0]


def test_catalog_bound_timeframe_period_and_range_plans_are_deterministic_and_auditable() -> None:
    catalog, profile = _catalog(), _profile()
    registry = IndicatorLearningRegistry(catalog)
    assert set(registry.operator_ids) == {GRAPH_BOUND_TIMEFRAME, EVIDENCE_LOOKBACK, TA_PERIOD, SEMANTIC_RANGE, EVIDENCE_WEIGHT, EVIDENCE_MEMBERSHIP, FAMILY_SUBSTITUTION}
    for operator_id in (GRAPH_BOUND_TIMEFRAME, TA_PERIOD, SEMANTIC_RANGE):
        operator = registry.get(operator_id)
        plan = _first(registry, profile, operator_id)
        child, application = operator.apply(profile, plan, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
        assert application["planSha256"] == plan["planSha256"]
        assert application["evidenceScope"]["lakeScopeRegenerationRequired"] is True
        assert operator.audit(profile, child, application)["allChecksPassed"] is True
    period_plans = registry.get(TA_PERIOD).enumerate_plans(profile)
    assert {item["construction"]["change"]["choice"] for item in period_plans} == {"fast", "slow"}
    assert all(item["construction"]["change"]["descriptor"]["min"] == 2 for item in period_plans)
    range_plans = registry.get(SEMANTIC_RANGE).enumerate_plans(profile)
    assert all(item["construction"]["change"]["catalogValueRange"]["minRange"] == 5.0 for item in range_plans)


def test_only_evidence_bound_instances_can_mutate_lookback_and_event_is_hard_frozen() -> None:
    profile = _profile(); registry = IndicatorLearningRegistry(_catalog())
    plans = registry.get(EVIDENCE_LOOKBACK).enumerate_plans(profile)
    assert {item["construction"]["indicatorInstanceId"] for item in plans} == {"state"}
    child, _ = registry.get(EVIDENCE_LOOKBACK).apply(profile, plans[0], parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert child["indicators"][1]["config"]["lookbackBars"] == 1
    broken = copy.deepcopy(profile); broken["indicators"][1]["config"]["lookbackBars"] = 2
    # No operator is allowed to legitimize a profile whose event persistence is
    # already noncanonical; application fails closed instead of passing it on.
    with pytest.raises(TemporalDiscoveryContractError, match="not canonical"):
        registry.get(EVIDENCE_LOOKBACK).apply(broken, plans[0], parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)


def test_family_substitution_is_strict_and_never_rebinds_event_or_scalar_instances() -> None:
    catalog, profile = _catalog(), _profile(); registry = IndicatorLearningRegistry(catalog)
    plans = registry.get(FAMILY_SUBSTITUTION).enumerate_plans(profile)
    assert plans and {item["construction"]["indicatorInstanceId"] for item in plans} == {"state"}
    plan = plans[0]
    child, application = registry.get(FAMILY_SUBSTITUTION).apply(profile, plan, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert child["indicators"][0]["meta"]["id"] == "CCI_A"
    assert child["indicators"][0]["config"]["talibConfig"] == [{"name": "timeperiod", "value": 20}]
    for key in ("isActive", "useFormingBar", "timeframe", "lookbackBars", "weight", "ranges"):
        assert child["indicators"][0]["config"][key] == profile["indicators"][0]["config"][key]
    assert child["executionConfig"]["managementLibrary"]["scalarBindings"] == profile["executionConfig"]["managementLibrary"]["scalarBindings"]
    assert registry.get(FAMILY_SUBSTITUTION).audit(profile, child, application)["allChecksPassed"] is True

    incompatible = copy.deepcopy(catalog)
    next(item for item in incompatible["indicators"] if item["meta"]["id"] == "CCI_A")["meta"]["signalPersistence"] = "event-with-lookback"
    assert IndicatorLearningRegistry(incompatible).get(FAMILY_SUBSTITUTION).enumerate_plans(profile) == []


def test_graph_bound_timeframe_covers_evidence_event_and_scalar_only_instances() -> None:
    profile = _profile()
    # The scalar is no longer evidence-bound; all three binding kinds must
    # still rotate its timeframe/evidence scope when changed.
    profile["graph"]["evidenceGroups"][0]["indicatorInstanceIds"] = ["state"]
    plans = IndicatorLearningRegistry(_catalog()).get(GRAPH_BOUND_TIMEFRAME).enumerate_plans(profile)
    assert {item["construction"]["indicatorInstanceId"] for item in plans} == {"state", "event", "scalar"}


def test_substitution_and_period_mutations_fail_closed_on_missing_or_unrepresentable_catalog_metadata() -> None:
    catalog, profile = _catalog(), _profile()
    next(item for item in catalog["indicators"] if item["meta"]["id"] == "RSI_A")["meta"].pop("inputs")
    registry = IndicatorLearningRegistry(catalog)
    assert registry.get(FAMILY_SUBSTITUTION).enumerate_plans(profile) == []
    dispositions = registry.deferred_dispositions(profile)
    assert dispositions[0] == {"indicatorIndex": 0, "indicatorId": "RSI_A", "disposition": "deferred", "reason": "source_compatibility_metadata_missing", "missing": ["inputs"]}
    assert {row["reason"] for row in dispositions} >= {"event_output_schema_metadata_not_admitted", "management_scalar_binding_replacement_not_admitted"}

    catalog = _catalog(); profile = _profile()
    profile["indicators"][0]["config"]["talibConfig"] = []
    assert all(
        item["construction"]["indicatorInstanceId"] != "state"
        for item in IndicatorLearningRegistry(catalog).get(TA_PERIOD).enumerate_plans(profile)
    )


def test_policy_and_plan_identities_bind_exact_catalog_and_parent() -> None:
    catalog, profile = _catalog(), _profile(); registry = IndicatorLearningRegistry(catalog)
    first = registry.get(GRAPH_BOUND_TIMEFRAME).enumerate_plans(profile)
    assert registry.policy["catalogSha256"] == registry.catalog.catalog_sha256
    altered = copy.deepcopy(catalog); altered["timeframes"].pop("H1")
    with pytest.raises(TemporalDiscoveryContractError, match="timeframe policy"):
        IndicatorLearningRegistry(altered)
    mutated_parent = copy.deepcopy(profile); mutated_parent["name"] = "different"
    with pytest.raises(TemporalDiscoveryContractError, match="not canonical"):
        registry.get(GRAPH_BOUND_TIMEFRAME).apply(mutated_parent, first[0], parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert canonical_sha256(profile) == first[0]["parentSourceProfileSha256"]


def test_noncanonical_event_persistence_fails_closed_during_enumeration_and_audit() -> None:
    registry, profile = IndicatorLearningRegistry(_catalog()), _profile()
    noncanonical = copy.deepcopy(profile)
    noncanonical["indicators"][1]["config"]["lookbackBars"] = 3
    assert registry.enumerate_plans(noncanonical) == []
    plan = _first(registry, profile, EVIDENCE_LOOKBACK)
    child, application = registry.get(EVIDENCE_LOOKBACK).apply(profile, plan, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    tampered = copy.deepcopy(application)
    tampered["staticInvariantReport"]["allChecksPassed"] = False
    tampered.pop("applicationSha256")
    tampered["applicationSha256"] = canonical_sha256(tampered)
    assert registry.get(EVIDENCE_LOOKBACK).audit(profile, child, tampered)["allChecksPassed"] is False


def test_behavioral_relevance_excludes_unbound_scalar_range_and_opposite_side() -> None:
    registry, profile = IndicatorLearningRegistry(_catalog()), _profile()
    profile["indicators"].append(_instance(_catalog(), "CCI_A", "unbound"))
    profile["graph"]["evidenceGroups"][0]["indicatorInstanceIds"] = ["state"]
    assert all(
        item["construction"]["indicatorInstanceId"] != "unbound"
        for item in registry.get(TA_PERIOD).enumerate_plans(profile)
    )
    assert all(
        item["construction"]["indicatorInstanceId"] != "unbound"
        for item in registry.get(GRAPH_BOUND_TIMEFRAME).enumerate_plans(profile)
    )
    ranges = registry.get(SEMANTIC_RANGE).enumerate_plans(profile)
    assert ranges and {item["construction"]["change"]["side"] for item in ranges} == {"buy"}
    assert all(item["construction"]["indicatorInstanceId"] != "scalar" for item in ranges)
    short = copy.deepcopy(profile); short["directionMode"] = "short"
    assert {item["construction"]["change"]["side"] for item in registry.get(SEMANTIC_RANGE).enumerate_plans(short)} == {"sell"}


def test_composite_module_ownership_controls_semantic_range_side() -> None:
    registry, profile = IndicatorLearningRegistry(_catalog()), _profile()
    profile["directionMode"] = "both"
    profile["graph"]["evidenceGroups"][0]["id"] = "long_context"
    profile["graph"]["entryArbitration"] = {"modules": [
        {"direction": "long", "evidenceGroupIds": ["long_context"]},
        {"direction": "short", "evidenceGroupIds": []},
    ]}
    assert {item["construction"]["change"]["side"] for item in registry.get(SEMANTIC_RANGE).enumerate_plans(profile)} == {"buy"}


def test_declared_but_unsafe_family_pairs_never_substitute() -> None:
    catalog = _catalog()
    pairs = (("MINUS_DI", "PLUS_DI", "negative", "positive"), ("MACD_HISTOGRAM_PIPS", "MOM", "histogram", "momentum"))
    catalog["indicators"] = []
    for left, right, left_polarity, right_polarity in pairs:
        for indicator_id, polarity in ((left, left_polarity), (right, right_polarity)):
            meta = _meta(indicator_id)
            meta["familySubstitution"]["polarity"] = polarity
            catalog["indicators"].append({"meta": meta, "config": _config()})
    profile = {"directionMode": "long", "indicators": [_instance(catalog, "MINUS_DI", "minus"), _instance(catalog, "MACD_HISTOGRAM_PIPS", "hist")], "executionConfig": {"managementLibrary": {"scalarBindings": []}}, "graph": {"evidenceGroups": [{"id": "context", "indicatorInstanceIds": ["minus", "hist"]}], "eventBindings": []}}
    assert IndicatorLearningRegistry(catalog).get(FAMILY_SUBSTITUTION).enumerate_plans(profile) == []


def test_evidence_weight_is_quantized_reachable_and_excludes_event_scalar_and_unbound() -> None:
    catalog, profile = _catalog(), _profile()
    profile["indicators"].append(_instance(catalog, "CCI_A", "unbound"))
    profile["graph"]["evidenceGroups"].append(
        {"id": "unused_context", "indicatorInstanceIds": ["unbound"]}
    )
    registry = IndicatorLearningRegistry(catalog)
    plans = registry.get(EVIDENCE_WEIGHT).enumerate_plans(profile)
    assert plans and {plan["construction"]["indicatorInstanceId"] for plan in plans} == {"state"}
    assert {plan["construction"]["after"] for plan in plans} == {0.25, 0.5, 1.0}
    plan = plans[0]
    child, application = registry.get(EVIDENCE_WEIGHT).apply(profile, plan, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert child["indicators"][0]["config"]["weight"] == plan["construction"]["after"]
    assert registry.get(EVIDENCE_WEIGHT).audit(profile, child, application)["allChecksPassed"] is True

    below = copy.deepcopy(profile)
    below["graph"]["transitions"][0]["guard"]["kind"] = "evidence_below"
    assert {
        plan["construction"]["indicatorInstanceId"]
        for plan in registry.get(EVIDENCE_WEIGHT).enumerate_plans(below)
    } == {"state"}


def test_evidence_membership_add_remove_last_member_cap_and_cross_module_rejection() -> None:
    catalog, profile = _catalog(), _profile()
    profile["indicators"].append(_instance(catalog, "CCI_A", "cci"))
    registry = IndicatorLearningRegistry(catalog)
    plans = registry.get(EVIDENCE_MEMBERSHIP).enumerate_plans(profile)
    adds = [plan for plan in plans if plan["construction"]["kind"] == "add_evidence_member"]
    assert len(adds) == 1 and adds[0]["construction"]["indicatorInstanceId"] == "cci"
    assert not [plan for plan in plans if plan["construction"]["kind"] == "remove_evidence_member"]
    child, application = registry.get(EVIDENCE_MEMBERSHIP).apply(profile, adds[0], parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert child["graph"]["evidenceGroups"][0]["indicatorInstanceIds"] == ["cci", "state"]
    assert registry.get(EVIDENCE_MEMBERSHIP).audit(profile, child, application)["allChecksPassed"] is True
    removal = next(plan for plan in registry.get(EVIDENCE_MEMBERSHIP).enumerate_plans(child) if plan["construction"]["kind"] == "remove_evidence_member")
    removed = registry.get(EVIDENCE_MEMBERSHIP).preview(child, removal)
    assert len(removed["graph"]["evidenceGroups"][0]["indicatorInstanceIds"]) == 1

    capped = copy.deepcopy(profile)
    capped["indicators"] = [_instance(catalog, "RSI_A", f"member_{index}") for index in range(16)] + [_instance(catalog, "CCI_A", "candidate")]
    capped["graph"]["evidenceGroups"][0]["indicatorInstanceIds"] = [f"member_{index}" for index in range(16)]
    assert not [plan for plan in registry.get(EVIDENCE_MEMBERSHIP).enumerate_plans(capped) if plan["construction"]["kind"] == "add_evidence_member"]

    composite = copy.deepcopy(profile)
    composite["directionMode"] = "both"
    composite["graph"]["evidenceGroups"][0]["id"] = "long_context"
    composite["graph"]["entryArbitration"] = {"modules": [
        {"direction": "long", "evidenceGroupIds": ["long_context"], "indicatorIds": ["state"]},
        {"direction": "short", "evidenceGroupIds": [], "indicatorIds": ["cci"]},
    ]}
    assert registry.get(EVIDENCE_MEMBERSHIP).enumerate_plans(composite) == []


def test_membership_v3_fails_closed_when_manifest_indicator_ownership_is_missing() -> None:
    catalog, profile = _catalog(), _profile()
    profile["indicators"].append(_instance(catalog, "CCI_A", "cci"))
    profile["directionMode"] = "both"
    profile["graph"]["entryArbitration"] = {"modules": [
        {"direction": "long", "evidenceGroupIds": ["context"]},
        {"direction": "short", "evidenceGroupIds": []},
    ]}
    assert IndicatorLearningRegistry(catalog).get(EVIDENCE_MEMBERSHIP).enumerate_plans(profile) == []
