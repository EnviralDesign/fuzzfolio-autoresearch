from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_operator_confirmed_entry import (
    OPERATOR_ID,
    ConfirmedEntryStructuralOperator,
    apply_confirmed_entry_plan,
    audit_confirmed_entry_application,
    enumerate_confirmed_entry_plans,
    inspect_confirmed_entry_applicability,
    preview_confirmed_entry_plan,
)
from autoresearch.temporal_structural_operators import (
    StructuralOperatorRegistry,
    build_candidate_lineage,
)

SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "strict confirmed-entry operator fixture",
        "description": "repository-only synthetic fixture",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {
            "managementPlanId": "fixed_protection",
            "sentinel": {"mustRemain": "byte-identical"},
        },
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
            ],
            "evidenceGroups": [
                {"id": "stretch", "indicatorInstanceIds": ["rsi"]},
            ],
            "eventBindings": [
                {
                    "id": "crossback",
                    "indicatorInstanceId": "rsi",
                    "longOutput": "bullish",
                    "shortOutput": "bearish",
                }
            ],
            "transitions": [
                {
                    "id": "direct_entry",
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
                                "groupId": "stretch",
                                "thresholdPercent": 70.0,
                            },
                            {
                                "kind": "any",
                                "guards": [
                                    {"kind": "fresh_event", "eventId": "crossback"},
                                    {
                                        "kind": "event_age_at_most",
                                        "eventId": "crossback",
                                        "events": 1,
                                    },
                                ],
                            },
                        ],
                    },
                    "actions": [
                        {
                            "kind": "enter_next_open",
                            "managementPlanId": "fixed_protection",
                        }
                    ],
                    "reasonCode": "direct_entry",
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
                    "id": "closed_to_flat",
                    "sourceStateId": "open",
                    "destinationStateId": "flat",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "position_closed",
                },
            ],
        },
    }


def test_registry_enumerates_one_canonical_plan_without_randomness() -> None:
    profile = _profile()
    operator = ConfirmedEntryStructuralOperator()
    registry = StructuralOperatorRegistry([operator])

    first = registry.enumerate_plans(profile)
    second = registry.enumerate_plans(copy.deepcopy(profile))

    assert first == second == sorted(first, key=lambda item: item["planSha256"])
    assert len(first) == 1
    assert first[0]["operatorId"] == OPERATOR_ID
    assert first[0]["confirmationBindingIdentity"] == "event:crossback"
    assert first[0]["parentSourceProfileSha256"] == canonical_sha256(profile)


def test_apply_splits_entry_and_preserves_management_and_non_target_graph() -> None:
    profile = _profile()
    plan = enumerate_confirmed_entry_plans(profile)[0]
    assert preview_confirmed_entry_plan(profile, plan)["graph"]["states"][-1][
        "id"
    ].startswith("armed_confirmation_")
    child, application = apply_confirmed_entry_plan(
        profile,
        plan,
        parent_validated_program_sha256=SHA_A,
        child_validated_program_sha256=SHA_B,
    )

    assert child["executionConfig"] == profile["executionConfig"]
    assert child["graph"]["transitions"][1:3] == profile["graph"]["transitions"][1:3]
    assert len(child["graph"]["states"]) == len(profile["graph"]["states"]) + 1
    added = application["delta"]["addedTransitions"]
    assert [item["priority"] for item in added[1:]] == [10, 20, 30]
    assert added[0]["actions"] == []
    assert added[1]["actions"] == profile["graph"]["transitions"][0]["actions"]
    assert added[2]["actions"] == added[3]["actions"] == []
    assert added[1]["guard"]["guards"][0] == {
        "kind": "state_age_at_least",
        "events": 1,
    }
    assert added[1]["guard"]["guards"][1] == {
        "kind": "position_exists",
        "expected": False,
    }
    assert added[3]["guard"] == {"kind": "state_age_at_least", "events": 3}
    assert application["parentValidatedProgramSha256"] == SHA_A
    assert application["childValidatedProgramSha256"] == SHA_B
    assert application["staticInvariantReport"]["allChecksPassed"] is True

    audit = audit_confirmed_entry_application(profile, child, application)
    assert audit["allChecksPassed"] is True


def test_audit_detects_transformed_profile_and_application_tampering() -> None:
    profile = _profile()
    plan = enumerate_confirmed_entry_plans(profile)[0]
    child, application = apply_confirmed_entry_plan(
        profile,
        plan,
        parent_validated_program_sha256=SHA_A,
        child_validated_program_sha256=SHA_B,
    )
    tampered_child = copy.deepcopy(child)
    tampered_child["executionConfig"]["sentinel"]["mustRemain"] = "changed"
    child_audit = audit_confirmed_entry_application(
        profile, tampered_child, application
    )
    assert child_audit["allChecksPassed"] is False
    assert child_audit["checks"]["transformed_profile_exact"] is False

    tampered_application = copy.deepcopy(application)
    tampered_application["delta"]["addedTransitions"][0]["reasonCode"] = "changed"
    application_audit = audit_confirmed_entry_application(
        profile, child, tampered_application
    )
    assert application_audit["allChecksPassed"] is False
    assert application_audit["checks"]["application_identity_exact"] is False
    assert application_audit["checks"]["application_delta_exact"] is False


@pytest.mark.parametrize(
    "mutation",
    [
        "second_entry",
        "entry_side_effect",
        "single_binding",
        "no_invertible_setup_anchor",
    ],
)
def test_strict_applicability_fails_closed(mutation: str) -> None:
    profile = _profile()
    entry = profile["graph"]["transitions"][0]
    if mutation == "second_entry":
        duplicate = copy.deepcopy(entry)
        duplicate["id"] = "other_entry"
        profile["graph"]["transitions"].append(duplicate)
    elif mutation == "entry_side_effect":
        entry["actions"].append({"kind": "exit_next_open"})
    elif mutation == "single_binding":
        entry["guard"]["guards"].pop(2)
    else:
        entry["guard"]["guards"][1] = {
            "kind": "utc_time_window",
            "startMinute": 420,
            "endMinute": 960,
        }

    assert enumerate_confirmed_entry_plans(profile) == []


def test_application_rejects_plan_from_another_parent() -> None:
    profile = _profile()
    plan = enumerate_confirmed_entry_plans(profile)[0]
    changed = copy.deepcopy(profile)
    changed["description"] = "different parent identity"
    with pytest.raises(TemporalDiscoveryContractError, match="canonical applicable"):
        apply_confirmed_entry_plan(
            changed,
            plan,
            parent_validated_program_sha256=SHA_A,
            child_validated_program_sha256=SHA_B,
        )


def test_event_history_tautology_is_inapplicable_because_expiry_is_impossible() -> None:
    profile = _profile()
    event_clause = profile["graph"]["transitions"][0]["guard"]["guards"][2]
    event_clause["guards"][0] = {
        "kind": "not",
        "guard": {"kind": "fresh_event", "eventId": "crossback"},
    }
    assert enumerate_confirmed_entry_plans(profile) == []
    report = inspect_confirmed_entry_applicability(profile)
    assert report["issueCodes"] == ["confirmation_tautological_over_event_age"]


def test_conjunctive_not_fresh_and_recent_event_still_permits_expiry() -> None:
    profile = _profile()
    event_clause = profile["graph"]["transitions"][0]["guard"]["guards"][2]
    event_clause["kind"] = "all"
    event_clause["guards"][0] = {
        "kind": "not",
        "guard": {"kind": "fresh_event", "eventId": "crossback"},
    }
    assert len(enumerate_confirmed_entry_plans(profile)) == 1


def test_lineage_binds_parent_arrays_operator_plan_and_application() -> None:
    profile = _profile()
    plan = enumerate_confirmed_entry_plans(profile)[0]
    child, application = apply_confirmed_entry_plan(
        profile,
        plan,
        parent_validated_program_sha256=SHA_A,
        child_validated_program_sha256=SHA_B,
    )
    lineage = build_candidate_lineage(
        candidate_id="generation_1_birth_7",
        candidate_source_profile_sha256=canonical_sha256(child),
        candidate_validated_program_sha256=SHA_B,
        generation_index=1,
        birth_ordinal=7,
        parent_candidate_ids=["parent_0"],
        parent_program_sha256s=[SHA_A],
        operator_id=OPERATOR_ID,
        operator_version="1",
        plan_sha256=plan["planSha256"],
        application_sha256=application["applicationSha256"],
    )
    assert lineage["originKind"] == "structural_operator"
    assert lineage["parentCandidateIds"] == ["parent_0"]
    assert lineage["parentValidatedProgramSha256s"] == [SHA_A]
    assert lineage["lineageSha256"] == canonical_sha256(
        {key: value for key, value in lineage.items() if key != "lineageSha256"}
    )
