from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.evolvable_module_genome import (
    EffectKind,
    EvolvableModuleGenomeV1,
    GenomeNodeV1,
    GenomeEdgeV1,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
    Zone,
)
from autoresearch.evolvable_module_resource_operators import (
    GenomeResourceOperatorError,
    GenomeResourceOperatorLayer,
)


REAL_CATALOG = Path(r"C:\repos\Trading-Dashboard\shared\constants\indicators.json")


def _meta(indicator_id: str, *, persistence: str = "state", event: bool = False) -> dict:
    schema = (
        {"kind": "directional_tokens", "longOutput": "bullish", "shortOutput": "bearish"}
        if event
        else {"kind": "none"}
    )
    return {
        "id": indicator_id,
        "strategyRole": "test-role",
        "signalRole": "trigger" if event else "context",
        "signalPersistence": persistence,
        "valueRange": {"min": 0, "max": 100, "step": 1, "minRange": 5},
        "usesRangeConfiguration": not event,
        "inputs": ["close"],
        "requiredPaddingBars": 100,
        "talibMeta": [{"name": "timeperiod", "uiType": "integer_slider", "default": 14, "min": 2, "max": 30, "marks": [{"value": 5}, {"value": 14}, {"value": 20}]}],
        "familySubstitution": {
            "substitutionClass": "directional_event_v1" if event else "bounded_score_v1",
            "polarity": "bidirectional" if event else "symmetric",
            "scoreUnit": "binary_0_1" if event else "native_score",
            "rawUnit": "directional_boolean_outputs" if event else "index",
            "eventOutputSchema": schema,
            "persistenceCompatibility": persistence,
        },
    }


def _config(*, period: int = 14) -> dict:
    return {
        "isActive": True,
        "useFormingBar": False,
        "timeframe": "M5",
        "lookbackBars": 1,
        "weight": 0.5,
        "ranges": {"buy": [20, 40], "sell": [60, 80]},
        "talibConfig": [{"name": "timeperiod", "value": period}],
    }


def _catalog() -> dict:
    return {
        "timeframes": {"M5": {}, "M15": {}, "H1": {}},
        "indicators": [
            {"meta": _meta("STATE_A"), "config": _config()},
            {"meta": _meta("STATE_B"), "config": _config(period=20)},
            {"meta": _meta("STATE_C"), "config": _config(period=5)},
            {"meta": _meta("EVENT_A", persistence="event-with-lookback", event=True), "config": _config()},
            {"meta": _meta("EVENT_B", persistence="event-with-lookback", event=True), "config": _config()},
        ],
    }


def _item(catalog: dict, indicator_id: str, instance_id: str) -> dict:
    entry = next(row for row in catalog["indicators"] if row["meta"]["id"] == indicator_id)
    result = copy.deepcopy(entry)
    result["meta"]["instanceId"] = instance_id
    result["ownerSide"] = "long"
    return result


def _genome() -> EvolvableModuleGenomeV1:
    catalog = _catalog()
    resources = ResourcePoolV1(
        indicators=(_item(catalog, "STATE_A", "state_a"), _item(catalog, "STATE_B", "state_b"), _item(catalog, "EVENT_A", "event_a")),
        evidence_groups=({"id": "g_context", "indicatorInstanceIds": ["state_a", "state_b"], "ownerSide": "long"},),
        events=({"id": "e_trigger", "indicatorInstanceId": "event_a", "longOutput": "bullish", "shortOutput": "bearish", "ownerSide": "long"},),
        management_refs=({"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}},),
    )
    use = lambda kind, identifier: ResourceUse(kind=kind, resource_id=identifier)
    nodes = (
        GenomeNodeV1("start", Zone.ENTRY, "start", {"kind": "position_exists", "expected": False}),
        GenomeNodeV1("setup", Zone.SETUP, "setup", {"kind": "all", "guards": [{"kind": "evidence_at_least", "groupId": "g_context", "thresholdPercent": 55.0}, {"kind": "fresh_event", "eventId": "e_trigger"}]}, (use(ResourceKind.EVIDENCE_GROUP, "g_context"), use(ResourceKind.EVENT, "e_trigger"))),
        GenomeNodeV1("entry", Zone.ENTRY, "entry", resources=(use(ResourceKind.MANAGEMENT_REF, "base"),)),
        GenomeNodeV1("hub", Zone.POSITION, "position_hub"),
    )
    edges = (
        GenomeEdgeV1("start_setup", "start", "setup"),
        GenomeEdgeV1("setup_entry", "setup", "entry", effect=EffectKind.ENTER),
        GenomeEdgeV1("entry_hub", "entry", "hub"),
    )
    genome = EvolvableModuleGenomeV1("long", resources, nodes, edges)
    genome.validate()
    return genome


def _real_catalog_genome() -> tuple[dict, EvolvableModuleGenomeV1]:
    """A minimal genome hydrated from the current Dashboard catalog itself."""

    catalog = json.loads(REAL_CATALOG.read_text(encoding="utf-8"))
    state_a, state_b, event = "MACD_HISTOGRAM_PIPS_TREND", "MOM_TREND", "MA_CROSSOVER"
    resources = ResourcePoolV1(
        indicators=(_item(catalog, state_a, "real_state_a"), _item(catalog, state_b, "real_state_b"), _item(catalog, event, "real_event")),
        evidence_groups=({"id": "real_group", "indicatorInstanceIds": ["real_state_a", "real_state_b"], "ownerSide": "long"},),
        events=({"id": "real_event_binding", "indicatorInstanceId": "real_event", "longOutput": "bullish", "shortOutput": "bearish", "ownerSide": "long"},),
        management_refs=({"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}},),
    )
    # The exact event output tokens are catalog-proven, not assumed.
    event_meta = next(row["meta"] for row in catalog["indicators"] if row["meta"]["id"] == event)
    schema = event_meta["familySubstitution"]["eventOutputSchema"]
    resources = ResourcePoolV1(resources.indicators, resources.evidence_groups, ({"id": "real_event_binding", "indicatorInstanceId": "real_event", "longOutput": schema["longOutput"], "shortOutput": schema["shortOutput"], "ownerSide": "long"},), resources.management_refs)
    use = lambda kind, identifier: ResourceUse(kind=kind, resource_id=identifier)
    nodes = (
        GenomeNodeV1("start", Zone.ENTRY, "start", {"kind": "position_exists", "expected": False}),
        GenomeNodeV1("setup", Zone.SETUP, "setup", {"kind": "all", "guards": [{"kind": "evidence_at_least", "groupId": "real_group", "thresholdPercent": 55.0}, {"kind": "fresh_event", "eventId": "real_event_binding"}]}, (use(ResourceKind.EVIDENCE_GROUP, "real_group"), use(ResourceKind.EVENT, "real_event_binding"))),
        GenomeNodeV1("entry", Zone.ENTRY, "entry", resources=(use(ResourceKind.MANAGEMENT_REF, "base"),)),
        GenomeNodeV1("hub", Zone.POSITION, "position_hub"),
    )
    genome = EvolvableModuleGenomeV1("long", resources, nodes, (GenomeEdgeV1("start_setup", "start", "setup"), GenomeEdgeV1("setup_entry", "setup", "entry", effect=EffectKind.ENTER), GenomeEdgeV1("entry_hub", "entry", "hub")))
    genome.validate()
    return catalog, genome


def _plan(layer: GenomeResourceOperatorLayer, genome: EvolvableModuleGenomeV1, kind: str) -> dict:
    plans = layer.enumerate_plans(genome)
    matches = [plan for plan in plans if plan["construction"]["kind"] == kind]
    assert matches, f"missing {kind}; available: {[plan['construction']['kind'] for plan in plans]}"
    return matches[0]


def _apply(layer: GenomeResourceOperatorLayer, genome: EvolvableModuleGenomeV1, kind: str):
    parent = genome.canonical()
    plan = _plan(layer, genome, kind)
    child, application = layer.apply(genome, plan)
    assert genome.canonical() == parent
    assert layer.preview(genome, plan).canonical() == child.canonical()
    assert layer.audit(genome, child, application)["allChecksPassed"] is True
    return child, application


def test_all_core_resource_mutation_families_are_deterministic_replayable_and_catalog_bound() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    first = layer.enumerate_plans(genome)
    assert first == layer.enumerate_plans(_genome())
    kinds = {plan["construction"]["kind"] for plan in first}
    assert {
        "evidence_group_create", "evidence_group_split", "evidence_weight_mutate", "evidence_threshold_mutate",
        "indicator_instance_insert", "indicator_instance_remove", "indicator_substitute", "indicator_timeframe_mutate",
        "indicator_lookback_mutate", "indicator_period_mutate", "indicator_range_mutate", "directional_event_insert",
        "directional_event_remove", "directional_event_substitute",
    } <= kinds
    # Each route is content-addressed and replays to the same sealed child.
    for kind in ("evidence_group_create", "evidence_group_split", "evidence_weight_mutate", "evidence_threshold_mutate", "indicator_instance_insert", "indicator_instance_remove", "indicator_substitute", "indicator_timeframe_mutate", "indicator_lookback_mutate", "indicator_period_mutate", "indicator_range_mutate", "directional_event_insert", "directional_event_remove", "directional_event_substitute"):
        _apply(layer, genome, kind)


def test_split_then_merge_and_insert_then_remove_preserve_closure_and_route_ownership() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    split, _ = _apply(layer, genome, "evidence_group_split")
    assert len(split.resources.evidence_groups) == 2
    merged, _ = _apply(layer, split, "evidence_group_merge")
    assert len(merged.resources.evidence_groups) == 1
    assert set(next(iter(merged.resources.mapping(ResourceKind.EVIDENCE_GROUP).values()))["indicatorInstanceIds"]) == {"state_a", "state_b"}

    inserted, _ = _apply(layer, genome, "indicator_instance_insert")
    assert len(inserted.resources.indicators) == 4
    removed, _ = _apply(layer, inserted, "indicator_instance_remove")
    assert len(removed.resources.indicators) == 3


def test_membership_moves_are_only_admitted_when_resource_remains_owned_elsewhere() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    created, _ = _apply(layer, genome, "evidence_group_create")
    # Shared, capability-compatible state evidence can be inserted into the
    # sibling group without inventing a new indicator or losing its source
    # route ownership.
    expanded, _ = _apply(layer, created, "evidence_member_insert")
    assert any(len(group["indicatorInstanceIds"]) == 2 for group in expanded.resources.mapping(ResourceKind.EVIDENCE_GROUP).values())
    # state_a is now shared by two groups, so removal from the original group
    # leaves it owned by the created group instead of orphaning it.
    removed, _ = _apply(layer, created, "evidence_member_remove")
    groups = removed.resources.mapping(ResourceKind.EVIDENCE_GROUP)
    assert "state_a" not in groups["g_context"]["indicatorInstanceIds"]
    assert "state_a" in next(group["indicatorInstanceIds"] for group_id, group in groups.items() if group_id != "g_context")
    # Remove can also retract a generated group once all of its members remain
    # independently owned by the original group.
    restored, _ = _apply(layer, created, "evidence_group_remove")
    assert len(restored.resources.evidence_groups) == 1


def test_stale_or_tampered_or_cross_side_applications_fail_closed() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    plan = _plan(layer, genome, "indicator_substitute")
    first_child, first_application = layer.apply(genome, plan)
    replay_child, replay_application = layer.apply(genome, plan)
    assert replay_child.canonical() == first_child.canonical()
    assert replay_application == first_application
    altered = copy.deepcopy(plan)
    altered["construction"]["afterIndicatorId"] = "EVENT_A"
    with pytest.raises(GenomeResourceOperatorError, match="canonical"):
        layer.apply(genome, altered)
    child, _ = _apply(layer, genome, "indicator_timeframe_mutate")
    with pytest.raises(GenomeResourceOperatorError, match="identity drift"):
        layer.apply(child, plan)

    # The core validator remains the authority for ownership, even if someone
    # manufactures a superficially plausible resource row.
    bad = copy.deepcopy(genome.resources.indicators[0]); bad["ownerSide"] = "short"
    broken = EvolvableModuleGenomeV1(genome.direction, ResourcePoolV1((bad, *genome.resources.indicators[1:]), genome.resources.evidence_groups, genome.resources.events, genome.resources.management_refs), genome.nodes, genome.edges)
    with pytest.raises(Exception, match="cross-side"):
        layer.enumerate_plans(broken)


def test_raw_event_freshness_never_mutates_lookback_and_event_insert_is_atomic() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    event_instances = {row["indicatorInstanceId"] for row in genome.resources.mapping(ResourceKind.EVENT).values()}
    assert all(plan["construction"]["indicatorInstanceId"] not in event_instances for plan in layer.enumerate_plans(genome) if plan["construction"]["kind"] == "indicator_lookback_mutate")
    child, _ = _apply(layer, genome, "directional_event_insert")
    event = next(item for item in child.resources.mapping(ResourceKind.EVENT).values() if item["id"] != "e_trigger")
    indicator = child.resources.mapping(ResourceKind.INDICATOR)[event["indicatorInstanceId"]]
    assert indicator["config"]["lookbackBars"] == 1
    assert indicator["config"]["useFormingBar"] is False


def test_same_catalog_primitive_can_be_a_distinct_timeframe_instance_but_not_an_exact_duplicate() -> None:
    layer, genome = GenomeResourceOperatorLayer(_catalog()), _genome()
    inserts = [
        plan["construction"]
        for plan in layer.enumerate_plans(genome)
        if plan["construction"]["kind"] == "indicator_instance_insert"
        and plan["construction"]["indicatorId"] == "STATE_A"
    ]
    # STATE_A already exists at M5. The M5 duplicate is rejected, while M15
    # and H1 remain distinct catalog-bound instances in the same fuzzy group.
    assert {item["timeframe"] for item in inserts} == {"M15", "H1"}
    plan = next(item for item in layer.enumerate_plans(genome) if item["construction"] == inserts[0])
    child, application = layer.apply(genome, plan)
    assert layer.audit(genome, child, application)["allChecksPassed"] is True


@pytest.mark.skipif(not REAL_CATALOG.is_file(), reason="current Dashboard indicator catalog is unavailable")
def test_current_dashboard_catalog_admits_real_capability_matched_state_and_event_substitutions() -> None:
    catalog, genome = _real_catalog_genome()
    layer = GenomeResourceOperatorLayer(catalog)
    plans = layer.enumerate_plans(genome)
    substitutions = [plan for plan in plans if plan["construction"]["kind"] == "indicator_substitute"]
    events = [plan for plan in plans if plan["construction"]["kind"] == "directional_event_substitute"]
    assert any(plan["construction"]["indicatorInstanceId"] == "real_state_a" for plan in substitutions)
    assert any(plan["construction"]["indicatorInstanceId"] == "real_event" for plan in events)
    child, application = layer.apply(genome, substitutions[0])
    assert layer.audit(genome, child, application)["allChecksPassed"] is True
