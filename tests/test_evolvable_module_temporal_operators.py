from __future__ import annotations

import copy

import pytest

from autoresearch.evolvable_module_genome import (
    EffectKind,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
    Zone,
    decode_program,
)
from autoresearch.evolvable_module_temporal_operators import (
    GenomeTemporalOperatorError,
    GenomeTemporalOperatorLayer,
)


def _use(kind: ResourceKind, identifier: str) -> ResourceUse:
    return ResourceUse(kind=kind, resource_id=identifier)


def _genome() -> EvolvableModuleGenomeV1:
    resources = ResourcePoolV1(
        indicators=({"meta": {"id": "I_RSI", "instanceId": "rsi"}, "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"}},),
        evidence_groups=({"id": "g_rsi", "indicatorInstanceIds": ["rsi"]},),
        events=({"id": "e_rsi", "indicatorInstanceId": "rsi", "longOutput": "bullish", "shortOutput": "bearish"},),
        management_refs=({"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}},),
    )
    nodes = (
        GenomeNodeV1("start", Zone.ENTRY, "start", {"kind": "position_exists", "expected": False}),
        GenomeNodeV1("setup", Zone.SETUP, "setup", {"kind": "all", "guards": [
            {"kind": "evidence_at_least", "groupId": "g_rsi", "thresholdPercent": 55.0},
            {"kind": "fresh_event", "eventId": "e_rsi"},
            {"kind": "utc_time_window", "startMinute": 0, "endMinute": 360, "weekdays": [0, 1, 2, 3, 4]},
        ]}, (_use(ResourceKind.EVIDENCE_GROUP, "g_rsi"), _use(ResourceKind.EVENT, "e_rsi"))),
        GenomeNodeV1("entry", Zone.ENTRY, "entry", resources=(_use(ResourceKind.MANAGEMENT_REF, "base"),)),
        GenomeNodeV1("hub", Zone.POSITION, "position_hub"),
        GenomeNodeV1("manage", Zone.MANAGEMENT, "break_even", {"kind": "state_age_at_least", "events": 3}),
        GenomeNodeV1("exit", Zone.EXIT, "time_exit", {"kind": "position_age_at_least", "events": 8}),
    )
    edges = (
        GenomeEdgeV1("start_setup", "start", "setup"),
        GenomeEdgeV1("setup_entry", "setup", "entry", effect=EffectKind.ENTER),
        GenomeEdgeV1("entry_hub", "entry", "hub"),
        GenomeEdgeV1("hub_manage", "hub", "manage", effect=EffectKind.BREAK_EVEN),
        GenomeEdgeV1("hub_exit", "hub", "exit", priority=20, effect=EffectKind.EXIT),
    )
    genome = EvolvableModuleGenomeV1("long", resources, nodes, edges)
    genome.validate()
    return genome


def _one(layer: GenomeTemporalOperatorLayer, genome: EvolvableModuleGenomeV1, family: str) -> dict:
    return next(plan for plan in layer.enumerate_plans(genome) if plan["construction"]["family"] == family)


def test_temporal_guard_domains_are_deterministic_content_bound_and_replayable() -> None:
    genome, layer = _genome(), GenomeTemporalOperatorLayer()
    original = genome.canonical()
    plans = layer.enumerate_plans(genome)
    restored = decode_program(
        program_kind=genome.program_kind,
        codec=genome.codec,
        payload=genome.canonical(),
    )
    assert plans == layer.enumerate_plans(restored)
    assert {
        "predicate_edge", "consecutive_true", "fresh_event_age_window",
        "fresh_event_absence", "state_or_condition_age", "position_age",
        "utc_session_window", "action_cooldown",
    } <= {item["construction"]["family"] for item in plans}

    for family in {item["construction"]["family"] for item in plans}:
        plan = _one(layer, genome, family)
        child, application = layer.apply(genome, plan)
        assert genome.canonical() == original
        assert child.resources.canonical() == genome.resources.canonical()
        assert child.budget.canonical() == genome.budget.canonical()
        assert layer.preview(genome, plan).canonical() == child.canonical()
        restarted_child = layer.preview(restored, plan)
        assert restarted_child.canonical() == child.canonical()
        assert restarted_child.identity_sha256 == child.identity_sha256
        assert layer.audit(genome, child, application)["allChecksPassed"] is True
        construction = plan["construction"]
        assert construction["kind"] == "typed_guard_replace"
        assert construction["site"]["ownerKind"] in {"node", "edge"}


def test_cooldown_only_targets_a_repeatable_management_dispatch() -> None:
    layer, genome = GenomeTemporalOperatorLayer(), _genome()
    cooldown = _one(layer, genome, "action_cooldown")
    construction = cooldown["construction"]
    assert construction["site"] == {"ownerKind": "edge", "ownerId": "hub_manage", "guardPath": []}
    added = construction["afterGuard"]["guards"][-1]
    assert added == {
        "kind": "action_cooldown_elapsed",
        "transitionId": "e_hub_manage",
        "actionOrdinal": 0,
        "evaluations": added["evaluations"],
    }
    assert added["evaluations"] in {1, 3, 5}
    assert not [plan for plan in layer.enumerate_plans(genome) if plan["construction"]["family"] == "action_cooldown" and plan["construction"]["site"]["ownerId"] == "hub_exit"]


class _Native:
    def __init__(self, acceptable: bool) -> None:
        self.acceptable = acceptable
        self.calls: list[tuple[dict, str]] = []

    def validate_v2(self, *, profile, candidate_id):
        self.calls.append((copy.deepcopy(profile), candidate_id))
        return {"candidateId": candidate_id, "candidateAcceptable": self.acceptable}


def test_native_boundary_is_optional_but_fail_closed_when_bound() -> None:
    genome = _genome()
    accepted = _Native(True)
    layer = GenomeTemporalOperatorLayer(native_validator=accepted)
    plan = _one(layer, genome, "utc_session_window")
    layer.apply(genome, plan)
    assert accepted.calls

    rejected = GenomeTemporalOperatorLayer(native_validator=_Native(False))
    assert rejected.enumerate_plans(genome) == []
    with pytest.raises(GenomeTemporalOperatorError, match="not canonical and applicable"):
        rejected.preview(genome, plan)


def test_tampered_plan_and_application_fail_audit_without_changing_parent() -> None:
    layer, genome = GenomeTemporalOperatorLayer(), _genome()
    plan = _one(layer, genome, "fresh_event_age_window")
    child, application = layer.apply(genome, plan)
    tampered_plan = copy.deepcopy(plan)
    tampered_plan["construction"]["afterGuard"]["maximumEvents"] = 99
    with pytest.raises(GenomeTemporalOperatorError, match="not canonical"):
        layer.preview(genome, tampered_plan)
    tampered_application = copy.deepcopy(application)
    tampered_application["childGenomeSha256"] = "sha256:" + "0" * 64
    assert layer.audit(genome, child, tampered_application)["allChecksPassed"] is False
