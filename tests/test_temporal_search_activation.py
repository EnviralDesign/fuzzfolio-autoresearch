from __future__ import annotations

from autoresearch.temporal_search_activation import (
    _authored_instances,
    _break_even_window,
    _trailing_window,
)


def _candidate() -> dict:
    return {
        "candidateId": "td_0000000000000000000000000001",
        "sourceMode": "broad_seed_mutation",
        "seedId": "seed",
        "mutationTrace": [{"family": "management_closure"}],
        "sourceProfile": {
            "graph": {
                "initialStateId": "flat",
                "transitions": [
                    {
                        "id": "enter",
                        "sourceStateId": "flat",
                        "destinationStateId": "open",
                        "eventClass": "decision",
                        "priority": 0,
                        "guard": {"kind": "state_age_at_least", "events": 0},
                        "actions": [
                            {
                                "kind": "enter_next_open",
                                "managementPlanId": "plan",
                            }
                        ],
                    },
                    {
                        "id": "protect",
                        "sourceStateId": "open",
                        "destinationStateId": "protected",
                        "eventClass": "decision",
                        "priority": 0,
                        "guard": {"kind": "unrealized_r_at_least", "value": 1.0},
                        "actions": [{"kind": "move_stop_to_break_even_next_open"}],
                    },
                ],
            },
            "executionConfig": {
                "managementLibrary": {
                    "defaultPlanId": "plan",
                    "plans": [
                        {
                            "id": "plan",
                            "trailingStop": {
                                "activation": {"kind": "immediate"},
                                "anchor": {"kind": "favorable_bar_extreme"},
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
        },
    }


def _payload(replay: dict) -> dict:
    return {
        "analysis_window_start": "2020-01-01T00:00:00Z",
        "analysis_window_end": "2020-02-01T00:00:00Z",
        "_causalityStage": "screening",
        "cost_view_results": {
            "research_conservative": {"replay_result": replay}
        },
    }


def test_authored_instances_preserve_complete_management_dimensions() -> None:
    break_even, trailing = _authored_instances(_candidate())
    assert break_even["activationThreshold"] == 1.0
    assert break_even["anchorSpec"] == '{"kind":"entry_price"}'
    assert break_even["distanceSpec"] == '{"kind":"break_even"}'
    assert trailing["activationMode"] == "immediate"
    assert trailing["entryRoutes"] == ["enter"]
    assert trailing["distanceSpec"] == (
        '{"kind":"fixed_initial_r","multiple":1.0}'
    )


def test_break_even_counts_only_rejected_reason_codes() -> None:
    break_even = _authored_instances(_candidate())[0]
    replay = {
        "metrics": {"positionsOpened": 1, "stateOccupancy": {"open": 2}},
        "graphTraces": [
            {"transitionId": "protect", "intentIds": ["intent"]}
        ],
        "executionTraces": [
            {
                "actionKind": "move_stop_to_break_even_next_open",
                "intentId": "intent",
                "status": "scheduled",
                "reasonCode": "intent_scheduled",
            },
            {
                "actionKind": "move_stop_to_break_even_next_open",
                "intentId": "intent",
                "status": "rejected",
                "reasonCode": "stop_not_tightened",
            },
        ],
        "trades": [{"maxFavorableExcursionR": 1.5, "holdingBars": 3}],
    }
    result = _break_even_window(break_even, _payload(replay))
    assert result["deepestReachedState"] == "intent_scheduled_but_rejected"
    assert result["rejectionReasons"] == {"stop_not_tightened": 1}
    assert result["positionEligibleCount"] == 1


def test_break_even_trade_is_attributed_only_to_its_exact_transition() -> None:
    candidate = _candidate()
    candidate["sourceProfile"]["graph"]["transitions"].append(
        {
            "id": "protect_alternate",
            "sourceStateId": "alternate_open",
            "destinationStateId": "protected",
            "eventClass": "decision",
            "priority": 0,
            "guard": {"kind": "unrealized_r_at_least", "value": 1.0},
            "actions": [{"kind": "move_stop_to_break_even_next_open"}],
        }
    )
    primary, alternate, _trailing = _authored_instances(candidate)
    replay = {
        "metrics": {
            "positionsOpened": 1,
            "stateOccupancy": {"open": 2, "alternate_open": 2},
        },
        "graphTraces": [
            {"transitionId": "protect", "intentIds": ["intent"]}
        ],
        "executionTraces": [
            {
                "actionKind": "move_stop_to_break_even_next_open",
                "intentId": "intent",
                "positionId": "position",
                "status": "scheduled",
                "reasonCode": "intent_scheduled",
            },
            {
                "actionKind": "move_stop_to_break_even_next_open",
                "intentId": "intent",
                "positionId": "position",
                "status": "applied",
                "reasonCode": "break_even_applied",
            },
        ],
        "trades": [
            {
                "positionId": "position",
                "breakEvenApplied": True,
                "closeReason": "break_even_stop",
                "maxFavorableExcursionR": 1.5,
                "holdingBars": 3,
            }
        ],
    }

    primary_result = _break_even_window(primary, _payload(replay))
    alternate_result = _break_even_window(alternate, _payload(replay))
    replay["trades"][0]["closeReason"] = "break_even_gap"
    primary_gap_result = _break_even_window(primary, _payload(replay))

    assert primary_result["breakEvenTradeCount"] == 1
    assert primary_result["deepestReachedState"] == (
        "activated_and_changed_trade_closure"
    )
    assert primary_gap_result["deepestReachedState"] == (
        "activated_and_changed_trade_closure"
    )
    assert alternate_result["breakEvenTradeCount"] == 0
    assert alternate_result["deepestReachedState"] == (
        "guard_evaluated_but_never_true"
    )


def test_immediate_trailing_is_activated_atomically_at_entry() -> None:
    trailing = _authored_instances(_candidate())[1]
    position_id = "position"
    replay = {
        "metrics": {},
        "graphTraces": [],
        "executionTraces": [],
        "trades": [
            {
                "positionId": position_id,
                "managementPlanId": "plan",
                "maxFavorableExcursionR": 0.0,
                "holdingBars": 1,
                "closeReason": "stop_loss",
            }
        ],
        "finalExecutionState": {"position": None},
    }
    result = _trailing_window(trailing, _payload(replay))
    assert result["deepestReachedState"] == "activated_successfully"
    assert result["automaticOrExplicitActivationCount"] == 1
    assert result["positionEligibleCount"] == 1
    assert result["rejectionReasons"] == {}
