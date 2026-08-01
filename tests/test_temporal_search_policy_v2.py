from __future__ import annotations

from autoresearch.temporal_search_policy_v2 import (
    GENERATOR_V2_PARAMETERS,
    _repair_profile,
    inspect_management_reachability,
)


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "v2 policy test",
        "description": "v2 policy test",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "plan",
                "plans": [
                    {
                        "id": "plan",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                        "initialTarget": {
                            "kind": "reward_multiple",
                            "multiple": 1.0,
                        },
                        "trailingStop": {
                            "activation": {"kind": "explicit"},
                            "anchor": {"kind": "bar_close"},
                            "distance": {
                                "kind": "fixed_initial_r",
                                "multiple": 1.0,
                            },
                            "minimumStepInitialR": 0.0,
                        },
                    }
                ],
            }
        },
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": ["data.completed_ohlc"],
            "initialStateId": "flat",
            "states": [
                {"id": "flat"},
                {"id": "entry_requested"},
                {"id": "open"},
                {"id": "protected"},
                {"id": "closed"},
            ],
            "evidenceGroups": [],
            "eventBindings": [],
            "transitions": [
                {
                    "id": "flat_to_entry",
                    "sourceStateId": "flat",
                    "destinationStateId": "entry_requested",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {"kind": "position_exists", "expected": False},
                    "actions": [
                        {"kind": "enter_next_open", "managementPlanId": "plan"}
                    ],
                    "reasonCode": "entry",
                },
                {
                    "id": "entry_to_open",
                    "sourceStateId": "entry_requested",
                    "destinationStateId": "open",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "filled"},
                    "actions": [],
                    "reasonCode": "filled",
                },
                {
                    "id": "protect",
                    "sourceStateId": "open",
                    "destinationStateId": "protected",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {"kind": "unrealized_r_at_least", "value": 3.0},
                        ],
                    },
                    "actions": [{"kind": "move_stop_to_break_even_next_open"}],
                    "reasonCode": "protect",
                },
                {
                    "id": "open_closed",
                    "sourceStateId": "open",
                    "destinationStateId": "closed",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
                {
                    "id": "protected_closed",
                    "sourceStateId": "protected",
                    "destinationStateId": "closed",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
            ],
        },
    }


def test_repair_orders_break_even_before_explicit_trailing() -> None:
    repaired, repairs = _repair_profile(
        _profile(), parameters=GENERATOR_V2_PARAMETERS
    )
    protect = next(
        item for item in repaired["graph"]["transitions"] if item["id"] == "protect"
    )
    threshold = next(
        item
        for item in protect["guard"]["guards"]
        if item["kind"] == "unrealized_r_at_least"
    )
    activation = next(
        item
        for item in repaired["graph"]["transitions"]
        if any(
            action["kind"] == "activate_trailing_stop_next_open"
            for action in item.get("actions", [])
        )
    )
    assert threshold["value"] == 0.5
    assert any(
        item["kind"] == "unrealized_r_at_least" and item["value"] == 1.0
        for item in activation["guard"]["guards"]
    )
    assert any(item["kind"] == "explicit_trailing_activation_route" for item in repairs)
    assert inspect_management_reachability(repaired)["acceptable"] is True


def test_reachability_rejects_orphan_management_plan() -> None:
    profile = _profile()
    profile["executionConfig"]["managementLibrary"]["plans"].append(
        {
            "id": "orphan",
            "initialStop": {"kind": "fixed_percent", "percent": 1.0},
            "initialTarget": {"kind": "none"},
        }
    )
    report = inspect_management_reachability(profile)
    assert report["acceptable"] is False
    assert report["issueCounts"] == {"explicit_trailing_missing_activation_action": 1, "orphan_management_plan": 1}
