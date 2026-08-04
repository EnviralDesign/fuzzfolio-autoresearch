from __future__ import annotations

import pytest

from autoresearch.temporal_qd_pair_factory import _registry_identity
from autoresearch.temporal_typed_motif_grammar import (
    ENTRY_ROUTE_DECISION_INDICATOR_CAP,
    ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
    Fragment,
    GrammarContext,
    GrammarError,
    ModuleProgram,
    TypedFragmentGrammar,
    entry_route_decision_indicator_report,
    validate_entry_route_decision_indicator_cap,
)


def _profile(
    *,
    groups: dict[str, list[str]],
    events: dict[str, str],
    entry_guard: dict,
    route_guard: dict | None = None,
    direction: str = "long",
) -> dict:
    indicator_ids = sorted({item for values in groups.values() for item in values} | set(events.values()))
    transitions = []
    entry_source = "ready"
    if route_guard is not None:
        entry_source = "armed"
        transitions.append(
            {
                "id": "arm",
                "sourceStateId": "ready",
                "destinationStateId": entry_source,
                "eventClass": "decision",
                "guard": route_guard,
                "actions": [],
            }
        )
    transitions.append(
        {
            "id": "entry",
            "sourceStateId": entry_source,
            "destinationStateId": "entry_pending",
            "eventClass": "decision",
            "guard": entry_guard,
            "actions": [{"kind": "enter_next_open", "managementPlanId": "base"}],
        }
    )
    return {
        "version": "v2",
        "directionMode": direction,
        "indicators": [{"meta": {"instanceId": item}} for item in indicator_ids],
        "graph": {
            "initialStateId": "ready",
            "evidenceGroups": [
                {"id": key, "indicatorInstanceIds": value}
                for key, value in groups.items()
            ],
            "eventBindings": [
                {"id": key, "indicatorInstanceId": value}
                for key, value in events.items()
            ],
            "transitions": transitions,
        },
    }


def _level(group_id: str) -> dict:
    return {"kind": "evidence_at_least", "groupId": group_id, "thresholdPercent": 50.0}


def _event(event_id: str) -> dict:
    return {"kind": "fresh_event", "eventId": event_id}


def test_all_unions_prior_route_and_entry_decision_indicators() -> None:
    profile = _profile(
        groups={"g1": ["i1"], "g2": ["i2"]},
        events={"e3": "i3"},
        route_guard=_level("g1"),
        entry_guard={"kind": "all", "guards": [_level("g2"), _event("e3")]},
    )
    report = validate_entry_route_decision_indicator_cap(profile)
    assert report["observedMaximumDistinctDecisionIndicatorInstances"] == 3


def test_any_preserves_the_largest_feasible_child_path_not_a_union_of_alternatives() -> None:
    profile = _profile(
        groups={"g1": ["i1"], "g2": ["i2"], "g3": ["i3"]},
        events={},
        entry_guard={
            "kind": "all",
            "guards": [
                {
                    "kind": "any",
                    "guards": [
                        {"kind": "all", "guards": [_level("g1"), _level("g2")]},
                        {"kind": "all", "guards": [_level("g1"), _level("g3")]},
                    ],
                },
                _level("g1"),
            ],
        },
    )
    report = entry_route_decision_indicator_report(profile)
    assert report["observedMaximumDistinctDecisionIndicatorInstances"] == 2


def test_predicate_edge_recurses_into_its_predicate() -> None:
    profile = _profile(
        groups={"g1": ["i1"], "g2": ["i2"]},
        events={"e3": "i3"},
        entry_guard={
            "kind": "all",
            "guards": [
                {
                    "kind": "predicate_edge",
                    "direction": "rising",
                    "predicate": {"kind": "all", "guards": [_level("g1"), _level("g2")]},
                },
                _event("e3"),
            ],
        },
    )
    assert validate_entry_route_decision_indicator_cap(profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 3
    profile["graph"]["transitions"][0]["guard"]["guards"][0].pop("predicate")
    with pytest.raises(GrammarError, match="predicate-edge decision guard is not closed"):
        validate_entry_route_decision_indicator_cap(profile)


def test_not_of_a_leaf_still_consumes_its_decision_indicator() -> None:
    profile = _profile(
        groups={"g1": ["i1"]},
        events={},
        entry_guard={"kind": "not", "guard": _level("g1")},
    )
    assert validate_entry_route_decision_indicator_cap(profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 1


def test_not_of_any_is_conjunctive_and_rejects_four_distinct_indicators() -> None:
    profile = _profile(
        groups={"g1": ["i1"], "g2": ["i2"], "g3": ["i3"]},
        events={"e4": "i4"},
        entry_guard={
            "kind": "all",
            "guards": [
                {"kind": "not", "guard": {"kind": "any", "guards": [_level("g1"), _level("g2")]}},
                _level("g3"),
                _event("e4"),
            ],
        },
    )
    with pytest.raises(GrammarError, match="distinct decision-indicator cap"):
        validate_entry_route_decision_indicator_cap(profile)


def test_not_of_all_preserves_alternatives() -> None:
    profile = _profile(
        groups={"g1": ["i1"], "g2": ["i2"]},
        events={},
        entry_guard={
            "kind": "all",
            "guards": [
                {"kind": "not", "guard": {"kind": "all", "guards": [_level("g1"), _level("g2")]}},
                _level("g1"),
            ],
        },
    )
    report = validate_entry_route_decision_indicator_cap(profile)
    assert report["entryTransitions"][0]["routeDistinctDecisionIndicatorCounts"] == [1, 2]


def test_malformed_not_wrapper_fails_closed() -> None:
    profile = _profile(
        groups={"g1": ["i1"]},
        events={},
        entry_guard={"kind": "not"},
    )
    with pytest.raises(GrammarError, match="not decision guard is not closed"):
        validate_entry_route_decision_indicator_cap(profile)


def test_three_fuzzy_members_plus_one_raw_event_is_rejected() -> None:
    profile = _profile(
        groups={"three_fuzzy": ["i1", "i2", "i3"]},
        events={"raw_event": "i4"},
        entry_guard={"kind": "all", "guards": [_level("three_fuzzy"), _event("raw_event")]},
    )
    with pytest.raises(GrammarError, match="distinct decision-indicator cap"):
        validate_entry_route_decision_indicator_cap(profile)


def test_two_fuzzy_members_plus_one_raw_event_is_accepted() -> None:
    profile = _profile(
        groups={"two_fuzzy": ["i1", "i2"]},
        events={"raw_event": "i3"},
        entry_guard={"kind": "all", "guards": [_level("two_fuzzy"), _event("raw_event")]},
    )
    assert validate_entry_route_decision_indicator_cap(profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 3


def test_position_economic_runtime_and_management_scalar_guards_do_not_consume_the_cap() -> None:
    profile = _profile(
        groups={"fuzzy": ["i1"]},
        events={"raw_event": "i2"},
        entry_guard={
            "kind": "all",
            "guards": [
                _level("fuzzy"),
                _event("raw_event"),
                {"kind": "position_exists", "expected": False},
                {"kind": "state_age_at_least", "events": 2},
                {"kind": "unrealized_r_at_least", "value": 1.0},
                {"kind": "economic_calendar_open"},
            ],
        },
    )
    profile["indicators"].append({"meta": {"instanceId": "management_atr"}})
    assert validate_entry_route_decision_indicator_cap(profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 2


def test_bidirectional_ownership_is_per_side_not_a_combined_six_indicator_cap() -> None:
    long_profile = _profile(
        groups={"long_fuzzy": ["l1", "l2"]},
        events={"long_event": "l3"},
        entry_guard={"kind": "all", "guards": [_level("long_fuzzy"), _event("long_event")]},
        direction="long",
    )
    short_profile = _profile(
        groups={"short_fuzzy": ["s1", "s2"]},
        events={"short_event": "s3"},
        entry_guard={"kind": "all", "guards": [_level("short_fuzzy"), _event("short_event")]},
        direction="short",
    )
    assert validate_entry_route_decision_indicator_cap(long_profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 3
    assert validate_entry_route_decision_indicator_cap(short_profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 3
    long_profile["graph"]["evidenceGroups"][0]["indicatorInstanceIds"].append("l4")
    long_profile["indicators"].append({"meta": {"instanceId": "l4"}})
    with pytest.raises(GrammarError, match="distinct decision-indicator cap"):
        validate_entry_route_decision_indicator_cap(long_profile)
    assert validate_entry_route_decision_indicator_cap(short_profile)["observedMaximumDistinctDecisionIndicatorInstances"] == 3


def _grammar_context() -> GrammarContext:
    return GrammarContext(
        instrument="EURUSD",
        indicators=tuple({"meta": {"instanceId": item}} for item in ("i1", "i2", "i3", "i4")),
        evidence_groups=(
            {"id": "g_one", "indicatorInstanceIds": ["i1"]},
            {"id": "g_three", "indicatorInstanceIds": ["i2", "i3", "i4"]},
        ),
        event_bindings=(
            {"id": "e_one", "indicatorInstanceId": "i1"},
        ),
        execution_config={"managementLibrary": {"plans": [{"id": "base"}]}},
    )


def test_seed_and_mutation_boundaries_fail_closed_before_immigrant_or_offspring_freeze() -> None:
    grammar = TypedFragmentGrammar(_grammar_context(), native_authority=object())
    program = grammar.seed(
        direction="long",
        name="breakout",
        group_id="g_one",
        event_id="e_one",
        plan_id="base",
    )
    entry_index = next(
        index
        for index, fragment in enumerate(program.fragments)
        if fragment.production_id == "enter_on_level"
    )
    rebind = next(
        plan
        for plan in grammar.enumerate_operations(program)
        if plan == {"operation": "rebind", "index": entry_index, "slot": "group", "value": "g_three"}
    )
    with pytest.raises(GrammarError, match="distinct decision-indicator cap"):
        grammar.apply(program, rebind)


def test_crossover_validates_the_entry_owning_side_before_emitting_offspring() -> None:
    grammar = TypedFragmentGrammar(_grammar_context(), native_authority=object())
    valid = grammar.seed(
        direction="long",
        name="breakout",
        group_id="g_one",
        event_id="e_one",
        plan_id="base",
    )
    entry = next(
        fragment for fragment in valid.fragments if fragment.production_id == "enter_on_level"
    )
    forged = ModuleProgram(
        "long",
        tuple(
            Fragment(
                fragment.uid,
                fragment.production_id,
                {**fragment.resources, "group": "g_three"}
                if fragment is entry
                else fragment.resources,
                fragment.choices,
            )
            for fragment in valid.fragments
        ),
    )
    with pytest.raises(GrammarError, match="distinct decision-indicator cap"):
        grammar.crossover(forged, valid, direction="long")


def test_cap_policy_is_frozen_into_the_grammar_registry_identity() -> None:
    registry = _registry_identity()
    assert registry["grammarVersion"] == "3"
    assert registry["entryRouteDecisionIndicatorPolicy"] == {
        "semanticVersion": ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
        "maxDistinctDecisionIndicatorInstances": ENTRY_ROUTE_DECISION_INDICATOR_CAP,
    }


def test_dense_16_state_63_edge_dag_uses_a_bounded_state_indicator_fixpoint() -> None:
    """The DAG has over 13k simple path prefixes but one indicator set/state."""

    states = [f"s{index}" for index in range(15)]
    transitions = []
    for destination_index in range(1, len(states)):
        for source_index in range(max(0, destination_index - 5), destination_index):
            transitions.append(
                {
                    "id": f"{source_index}_{destination_index}",
                    "sourceStateId": states[source_index],
                    "destinationStateId": states[destination_index],
                    "eventClass": "decision",
                    "guard": {"kind": "state_age_at_least", "events": 0},
                    "actions": [],
                }
            )
    transitions.extend(
        (
            {
                "id": "0_14_extra",
                "sourceStateId": "s0",
                "destinationStateId": "s14",
                "eventClass": "decision",
                "guard": {"kind": "state_age_at_least", "events": 0},
                "actions": [],
            },
            {
                "id": "1_14_extra",
                "sourceStateId": "s1",
                "destinationStateId": "s14",
                "eventClass": "decision",
                "guard": {"kind": "state_age_at_least", "events": 0},
                "actions": [],
            },
            {
                "id": "entry",
                "sourceStateId": "s14",
                "destinationStateId": "entry_pending",
                "eventClass": "decision",
                "guard": {"kind": "position_exists", "expected": False},
                "actions": [{"kind": "enter_next_open", "managementPlanId": "base"}],
            },
        )
    )
    assert len(transitions) == 63
    path_counts = [1]
    for destination_index in range(1, len(states)):
        total = sum(
            path_counts[source_index]
            for source_index in range(max(0, destination_index - 5), destination_index)
        )
        if destination_index == 14:
            total += path_counts[0] + path_counts[1]
        path_counts.append(total)
    assert path_counts[-1] == 6_932
    assert sum(path_counts[1:]) == 14_105
    report = validate_entry_route_decision_indicator_cap(
        {
            "version": "v2",
            "directionMode": "long",
            "indicators": [],
            "graph": {
                "initialStateId": "s0",
                "evidenceGroups": [],
                "eventBindings": [],
                "transitions": transitions,
            },
        }
    )
    assert report["entryTransitions"][0]["routeCount"] == 1
    assert report["reachableStateIndicatorSetCount"] == 15
