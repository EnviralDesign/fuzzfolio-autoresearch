from __future__ import annotations

import copy
import json
from pathlib import Path

import autoresearch.temporal_operator_expansion_admission as admission_module
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_operator_confirmed_entry import (
    ConfirmedEntryStructuralOperator,
)
from autoresearch.temporal_operator_expansion import (
    EdgeTriggerPredicateOperator,
    EventAgeWindowOperator,
    MaximumPositionAgeExitOperator,
    MinimumPositionAgeGateOperator,
    RepeatActionCooldownOperator,
    RequireConsecutiveTrueOperator,
    SequenceActionGateOperator,
    expanded_structural_operators,
)
from autoresearch.temporal_structural_operators import StructuralOperatorRegistry


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "seven operator fixture",
        "description": "repository-only synthetic fixture",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {"managementPlanId": "fixed_protection"},
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": [],
            "initialStateId": "flat",
            "states": [
                {"id": "flat"},
                {"id": "entry_requested"},
                {"id": "open"},
                {"id": "exit_requested"},
            ],
            "evidenceGroups": [
                {"id": "signal", "indicatorInstanceIds": ["signal"]},
            ],
            "eventBindings": [
                {
                    "id": "trigger",
                    "indicatorInstanceId": "signal",
                    "longOutput": "bullish",
                    "shortOutput": "bearish",
                }
            ],
            "transitions": [
                {
                    "id": "enter",
                    "sourceStateId": "flat",
                    "destinationStateId": "entry_requested",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": False},
                            {
                                "kind": "evidence_at_least",
                                "groupId": "signal",
                                "thresholdPercent": 60.0,
                            },
                            {"kind": "fresh_event", "eventId": "trigger"},
                        ],
                    },
                    "actions": [
                        {
                            "kind": "enter_next_open",
                            "managementPlanId": "fixed_protection",
                        }
                    ],
                    "reasonCode": "enter",
                },
                {
                    "id": "entry_filled",
                    "sourceStateId": "entry_requested",
                    "destinationStateId": "open",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "filled"},
                    "actions": [],
                    "reasonCode": "entry_filled",
                },
                {
                    "id": "manage",
                    "sourceStateId": "open",
                    "destinationStateId": "open",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {
                                "kind": "evidence_at_least",
                                "groupId": "signal",
                                "thresholdPercent": 80.0,
                            },
                        ],
                    },
                    "actions": [{"kind": "move_stop_to_break_even_next_open"}],
                    "reasonCode": "manage",
                },
                {
                    "id": "exit",
                    "sourceStateId": "open",
                    "destinationStateId": "exit_requested",
                    "eventClass": "decision",
                    "priority": 20,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {"kind": "unrealized_r_at_least", "value": 1.0},
                        ],
                    },
                    "actions": [{"kind": "exit_next_open"}],
                    "reasonCode": "exit",
                },
                {
                    "id": "closed",
                    "sourceStateId": "open",
                    "destinationStateId": "flat",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
            ],
        },
    }


def test_initial_registry_has_exactly_eight_family_first_operators() -> None:
    registry = StructuralOperatorRegistry(
        [ConfirmedEntryStructuralOperator(), *expanded_structural_operators()]
    )
    assert len(registry.operator_ids) == 8
    assert registry.operator_ids == tuple(sorted(registry.operator_ids))


def test_all_seven_families_enumerate_deterministically_and_preview_purely() -> None:
    parent = _profile()
    frozen = copy.deepcopy(parent)
    for operator in expanded_structural_operators():
        first = operator.enumerate_plans(parent)
        second = operator.enumerate_plans(copy.deepcopy(parent))
        assert first == second == sorted(first, key=lambda item: item["planSha256"])
        assert first, operator.operator_id
        child = operator.preview(parent, first[0])
        assert child != parent
        assert parent == frozen


def test_wrapper_runtime_occurrence_identity_is_stable_when_transition_order_changes() -> (  # noqa: E501
    None
):
    parent = _profile()
    reordered = copy.deepcopy(parent)
    reordered["graph"]["transitions"] = list(
        reversed(reordered["graph"]["transitions"])
    )
    for operator in (EdgeTriggerPredicateOperator(), RequireConsecutiveTrueOperator()):
        left = {
            (
                plan["targetTransitionId"],
                plan["sourceGuardSha256"],
                plan["parameters"]["occurrenceSha256"],
            )
            for plan in operator.enumerate_plans(parent)
        }
        right = {
            (
                plan["targetTransitionId"],
                plan["sourceGuardSha256"],
                plan["parameters"]["occurrenceSha256"],
            )
            for plan in operator.enumerate_plans(reordered)
        }
        assert left == right


def test_event_window_reuses_binding_and_skips_equivalent_window() -> None:
    operator = EventAgeWindowOperator()
    plans = [
        item
        for item in operator.enumerate_plans(_profile())
        if item["targetTransitionId"] == "enter"
    ]
    assert plans
    assert {item["replacementGuard"]["eventId"] for item in plans} == {"trigger"}


def test_sequence_moves_action_to_later_confirmation_and_preserves_other_routes() -> (
    None
):
    operator = SequenceActionGateOperator()
    plan = next(
        item
        for item in operator.enumerate_plans(_profile())
        if item["targetTransitionId"] == "exit"
    )
    child = operator.preview(_profile(), plan)
    added_state = child["graph"]["states"][-1]["id"]
    setup = next(
        item
        for item in child["graph"]["transitions"]
        if item["sourceStateId"] == "open" and item["destinationStateId"] == added_state
    )
    confirmation = next(
        item
        for item in child["graph"]["transitions"]
        if item["sourceStateId"] == added_state
        and item.get("actions") == [{"kind": "exit_next_open"}]
    )
    assert setup["actions"] == []
    assert setup["guard"]["kind"] == "predicate_edge"
    assert confirmation["guard"]["guards"][0] == {
        "kind": "state_age_at_least",
        "events": 1,
    }
    assert any(
        item["sourceStateId"] == added_state
        and item["eventClass"] == "execution"
        and item["destinationStateId"] == "flat"
        for item in child["graph"]["transitions"]
    )


def test_cooldown_and_position_age_operators_target_only_the_authored_action_gate() -> (
    None
):
    profile = _profile()
    cooldown = RepeatActionCooldownOperator()
    cooldown_plan = cooldown.enumerate_plans(profile)[0]
    cooldown_child = cooldown.preview(profile, cooldown_plan)
    manage = next(
        item
        for item in cooldown_child["graph"]["transitions"]
        if item["id"] == "manage"
    )
    assert manage["guard"]["guards"][-1]["kind"] == "action_cooldown_elapsed"

    minimum = MinimumPositionAgeGateOperator()
    minimum_plan = next(
        item
        for item in minimum.enumerate_plans(profile)
        if item["targetTransitionId"] == "exit"
    )
    minimum_child = minimum.preview(profile, minimum_plan)
    exit_guard = next(
        item for item in minimum_child["graph"]["transitions"] if item["id"] == "exit"
    )["guard"]
    assert exit_guard["guards"][-1]["kind"] == "position_age_at_least"

    maximum = MaximumPositionAgeExitOperator()
    maximum_child = maximum.preview(profile, maximum.enumerate_plans(profile)[0])
    exit_guard = next(
        item for item in maximum_child["graph"]["transitions"] if item["id"] == "exit"
    )["guard"]
    assert exit_guard["kind"] == "any"
    assert exit_guard["guards"][-1]["kind"] == "position_age_at_least"


def test_seven_family_admission_is_content_exact_and_repeatable(
    tmp_path: Path, monkeypatch
) -> None:
    profile = _profile()
    source_sha = canonical_sha256(profile)
    population = {
        "schemaVersion": "test_identity_set_v1",
        "count": 1,
        "values": [
            {
                "candidateId": "test_parent",
                "sourceProfile": profile,
                "sourceProfileSha256": source_sha,
                "programSha256": canonical_sha256({"program": profile}),
            }
        ],
    }
    population["setSha256"] = canonical_sha256(population)
    population_path = tmp_path / "population.json"
    population_path.write_text(
        json.dumps(population, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    class FakeValidator:
        def __init__(self, command, *, timeout_seconds):
            assert command == ["fake-validator"]
            assert timeout_seconds == 60.0

        def validate(
            self,
            *,
            candidate_id,
            source_profile,
            expected_raw_source_profile_sha256,
        ):
            assert (
                canonical_sha256(source_profile) == expected_raw_source_profile_sha256
            )
            program_sha = canonical_sha256({"program": source_profile})
            snapshot_sha = canonical_sha256({"snapshot": source_profile})
            report_sha = canonical_sha256(
                {
                    "candidateId": candidate_id,
                    "programSha256": program_sha,
                    "profileSnapshotSha256": snapshot_sha,
                }
            )
            return {
                "candidateAcceptable": True,
                "status": "valid_evaluable",
                "programSha256": program_sha,
                "profileSnapshotSha256": snapshot_sha,
                "validationReportSha256": report_sha,
                "issues": [],
            }

    monkeypatch.setattr(admission_module, "SubprocessCandidateValidator", FakeValidator)
    first = admission_module.build_operator_expansion_admission(
        population_path=population_path,
        validator_command=["fake-validator"],
        output_root=tmp_path / "first",
        admitted_per_operator=3,
    )
    second = admission_module.build_operator_expansion_admission(
        population_path=population_path,
        validator_command=["fake-validator"],
        output_root=tmp_path / "second",
        admitted_per_operator=3,
    )
    assert first["reportSha256"] == second["reportSha256"]
    assert first["admittedPlanCount"] == first["nativeValidCount"]
    report = json.loads(
        (tmp_path / "first" / "operator-admission.json").read_text(encoding="utf-8")
    )
    assert all(item["admittedPlanCount"] >= 1 for item in report["families"])
    assert {item["operatorId"] for item in report["families"]} == {
        operator.operator_id for operator in expanded_structural_operators()
    }
