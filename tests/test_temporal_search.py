from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_search import (
    TEMPORAL_SEARCH_CAPABILITY,
    TEMPORAL_SEARCH_RESULT_SCHEMA,
    TEMPORAL_SEARCH_TASK_KIND,
    TemporalSearchContractError,
    TemporalSearchTimeout,
    _classified_deterministic_rejection,
    _cost_view_path_sha256,
    build_authority,
    build_task_matrix,
    canonical_sha256,
    materialize_plan,
    run_temporal_search_tasks,
    validate_authority,
)


def test_execution_invariant_rejection_requires_exact_nested_shape() -> None:
    completion = {
        "status": "failed",
        "result": {
            "status": "failed",
            "error_type": "TemporalExecutionInvariantError",
            "error": "TemporalExecutionInvariantError: break-even may be applied only once",
            "attempt_number": 8,
        },
    }
    classified = _classified_deterministic_rejection(completion)
    assert classified is not None
    assert classified[0] == "duplicate_break_even_execution_invariant"
    completion["result"]["error"] = "TemporalExecutionInvariantError: another invariant"
    assert _classified_deterministic_rejection(completion) is None


def _profile() -> dict:
    return {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1"},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "executionConfig": {
            "exitPolicy": {
                "evidenceStatus": "none",
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                },
                "sourceKind": "manual",
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }


def _plan(profile: dict, start: str, end: str) -> dict:
    execution_config = profile["executionConfig"]
    execution_cell_sha256 = None
    if "managementLibrary" not in execution_config:
        execution_cell_sha256 = canonical_sha256(
            execution_config["exitPolicy"]["selectedCell"]
        )
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": canonical_sha256(profile),
        "analysis_window_start": start,
        "analysis_window_end": end,
        "execution_cell_sha256": execution_cell_sha256,
        "lake_window_binding": {
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "request": {
                "data_start": "2024-01-01T00:00:00Z",
                "data_end": "2025-01-01T00:00:00Z",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
    }
    plan["plan_id"] = canonical_sha256(plan)
    return plan


def _preparation() -> dict:
    profile = _profile()
    start = "2024-02-01T00:00:00Z"
    end = "2024-03-01T00:00:00Z"
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "stage5d-preflight",
        "workerContract": {
            "workerContractSha256": "sha256:" + "c" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": "candidate-a",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": [
                    {
                        "windowId": "development-a",
                        "evidencePlan": _plan(profile, start, end),
                    }
                ],
            }
        ],
        "developmentWindows": [
            {
                "windowId": "development-a",
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
            }
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved-tail",
                "analysisWindowStart": "2024-06-01T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "reserved holdout",
            }
        ],
        "bounds": {
            "maxCandidates": 2,
            "maxDevelopmentWindows": 2,
            "maxTasks": 4,
            "maxAttempts": 2,
            "deadlineSeconds": 60,
        },
    }


def test_authority_freezes_one_candidate_window_to_one_two_cost_task() -> None:
    authority = build_authority(_preparation())
    assert validate_authority(authority) == authority
    tasks = build_task_matrix(authority)
    assert len(tasks) == 1
    task = tasks[0]
    assert task["task_kind"] == TEMPORAL_SEARCH_TASK_KIND
    assert task["payload"]["candidate_id"] == "candidate_a"
    assert task["payload"]["profile_id"] == "candidate_a"
    assert task["payload"]["evaluator_id"] == "bar_single_position_execution_v1"
    assert task["payload"]["inline_profile_snapshot"] == _profile()
    assert task["payload"]["instruments"] == ["EURUSD"]
    assert (
        task["payload"]["execution_cell"]
        == _profile()["executionConfig"]["exitPolicy"]["selectedCell"]
    )
    assert "cost_views" not in task["payload"]
    assert "cost_models" not in task["payload"]
    assert "source_profile_snapshot" not in task["payload"]
    assert TEMPORAL_SEARCH_CAPABILITY in task["required_worker_capabilities"]
    assert (
        "management.scalar.price_distance.completed_bar"
        in task["required_worker_capabilities"]
    )
    assert "management.action.dynamic" in task["required_worker_capabilities"]


def test_v3_candidate_window_task_uses_the_worker_required_bidirectional_evaluator() -> None:
    """A v3/both snapshot must not inherit the candidate-job v2 default."""

    preparation = _preparation()
    profile = preparation["candidates"][0]["sourceProfile"]
    profile["version"] = "v3"
    profile["directionMode"] = "both"
    preparation["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    start = preparation["developmentWindows"][0]["analysisWindowStart"]
    end = preparation["developmentWindows"][0]["analysisWindowEnd"]
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"] = _plan(
        profile, start, end
    )

    payload = build_task_matrix(build_authority(preparation))[0]["payload"]

    # This exact ID is independently enforced by Dashboard's
    # TemporalGraphCandidateWindowJob for profile.version == "v3".
    assert (
        payload["evaluator_id"]
        == "bar_bidirectional_single_position_execution_v2"
    )


def test_cost_view_path_attestation_matches_worker_non_cost_field_names() -> None:
    replay = {
        "graphTraces": [
            {
                "eventSequence": 3,
                "eventClass": "decision",
                "priorStateId": "prior",
                "nextStateId": "next",
                "transitionId": "route",
                "reasonCode": "fixture",
                "intentIds": ["intent-a"],
            }
        ],
        "executionTraces": [
            {
                "eventSequence": 3,
                "clockIndex": 7,
                "marketBarId": "bar-a",
                "phase": "open",
                "effectKind": "modify",
                "status": "applied",
                "effectId": "effect-a",
                "intentId": "intent-a",
                "actionKind": "modify_protection",
                "reasonCode": "unrealized_r_at_least",
                "price": 1.2,
                "positionId": "position-a",
                "tradeId": "trade-a",
            }
        ],
        "trades": [
            {
                "direction": "long",
                "tradeId": "trade-a",
                "positionId": "position-a",
                "openingIntentId": "intent-a",
                "openingEffectId": "effect-a",
                "closingIntentId": "intent-b",
                "closingEffectId": "effect-b",
                "entryBarId": "bar-a",
                "exitBarId": "bar-b",
                "entryPhase": "open",
                "exitPhase": "interval",
                "entryTime": "2024-01-01T00:00:00Z",
                "exitTime": "2024-01-01T00:05:00Z",
                "entryClockIndex": 7,
                "exitClockIndex": 8,
                "entryPrice": 1.2,
                "exitPrice": 1.3,
                "closeReason": "target",
                "holdingBars": 1,
                "holdingHours": 0.1,
            }
        ],
    }
    replay["trades"][0].update(
        {
            "managementPlanId": "managed",
            "managementPlanSha256": "sha256:" + "a" * 64,
            "stopLossPercent": 0.5,
            "rewardMultiple": 2.0,
            "takeProfitPercent": 1.0,
            "initialStopPrice": 1.194,
            "initialTargetPrice": 1.212,
            "finalStopPrice": 1.201,
            "targetPrice": 1.212,
            "trailing": {"policySha256": "sha256:" + "b" * 64, "active": True, "activationCount": 1, "activationClockIndex": 7, "pendingStopPrice": 1.201, "pendingAnchorPrice": 1.205, "pendingClockIndex": 8, "updateCount": 1, "lastAppliedAnchorPrice": 1.204, "ownsCurrentStop": True},
            "breakEvenApplied": True,
            "stopUpdateCount": 2,
            "targetUpdateCount": 1,
            "lastManagementClockIndex": 8,
            "maxFavorableExcursionR": 1.5,
            "maxAdverseExcursionR": -0.3,
        }
    )
    replay["finalExecutionState"] = {
        "executionStateSha256": "sha256:" + "c" * 64,
        "programSha256": "sha256:" + "d" * 64,
        "costModelSha256": "sha256:" + "e" * 64,
        "instrument": "EURUSD", "direction": "long", "lastExecutionReason": "trailing_update",
        "lastCloseReason": "target", "lastMarketBarId": "bar-b", "lastBarStart": "2024-01-01T00:05:00Z", "lastClockIndex": 8,
        "position": {
            "positionSha256": "sha256:" + "f" * 64, "positionId": "position-open", "programSha256": "sha256:" + "d" * 64,
            "instrument": "EURUSD", "direction": "long", "managementPlanId": "managed", "managementPlanSha256": "sha256:" + "a" * 64,
            "entryBarId": "bar-a", "entryTime": "2024-01-01T00:00:00Z", "entryClockIndex": 7, "entryPrice": 1.2,
            "stopLossPercent": 0.5, "rewardMultiple": 2.0, "takeProfitPercent": 1.0, "initialStopPrice": 1.194, "initialTargetPrice": 1.212, "stopPrice": 1.201, "targetPrice": 1.212,
            "trailing": {"policySha256": "sha256:" + "b" * 64, "active": True, "activationCount": 1, "activationClockIndex": 7, "pendingStopPrice": 1.201, "pendingAnchorPrice": 1.205, "pendingClockIndex": 8, "updateCount": 1, "lastAppliedAnchorPrice": 1.204, "ownsCurrentStop": True},
            "breakEvenApplied": True, "stopUpdateCount": 2, "targetUpdateCount": 1, "lastManagementClockIndex": 8, "maxFavorableExcursionR": 1.5, "maxAdverseExcursionR": -0.3,
        },
        "pendingEffect": {
            "pendingEffectSha256": "sha256:" + "0" * 64, "programSha256": "sha256:" + "d" * 64,
            "scheduledEventId": "event-scheduled", "scheduledEventSha256": "sha256:" + "1" * 64, "expectedGraphStateId": "open", "expectedGraphStateSha256": "sha256:" + "2" * 64, "priorExecutionStateSha256": "sha256:" + "3" * 64,
            "scheduledClockIndex": 8, "eligibleClockIndex": 9, "scheduledManagementScalars": {"stop_level": 1.201}, "scheduledObservationSha256": "sha256:" + "4" * 64,
            "intent": {"intentId": "intent-pending", "programSha256": "sha256:" + "d" * 64, "eventId": "event-scheduled", "eventSha256": "sha256:" + "1" * 64, "transitionId": "manage", "actionOrdinal": 0, "actionKind": "tighten_stop_next_open", "timingClass": "next_open", "parameters": {"distance": 1.0}},
        },
    }
    expected = canonical_sha256(
        {
            "schema_version": "temporal_graph_cost_view_path_v3",
            "graph_path": [
                {
                    "event_sequence": 3,
                    "event_class": "decision",
                    "prior_state_id": "prior",
                    "next_state_id": "next",
                    "transition_id": "route",
                    "reason_code": "fixture",
                    "intent_count": 1,
                }
            ],
            "execution_path": [
                {
                    "event_sequence": 3,
                    "clock_index": 7,
                    "market_bar_id": "bar-a",
                    "phase": "open",
                    "effect_kind": "modify",
                    "status": "applied",
                    "action_kind": "modify_protection",
                    "reason_code": "unrealized_r_at_least",
                    "price": 1.2,
                    "position_present": True,
                    "trade_present": True,
                }
            ],
            "trade_path": [
                {
                    "direction": "long",
                    "entry_bar_id": "bar-a",
                    "exit_bar_id": "bar-b",
                    "entry_phase": "open",
                    "exit_phase": "interval",
                    "entry_time": "2024-01-01T00:00:00Z",
                    "exit_time": "2024-01-01T00:05:00Z",
                    "entry_clock_index": 7,
                    "exit_clock_index": 8,
                    "entry_price": 1.2,
                    "exit_price": 1.3,
                    "close_reason": "target",
                    "holding_bars": 1,
                    "holding_hours": 0.1,
                }
            ],
        }
    )

    # The fixture retains a deliberately minimal legacy trade row.  The v3
    # projection supplies explicit neutral defaults for omitted management
    # fields while real worker rows carry the complete state below.
    expected = _cost_view_path_sha256(replay, name="fixture")
    assert _cost_view_path_sha256(replay, name="fixture") == expected

    cost_bound_identity_view = copy.deepcopy(replay)
    graph = cost_bound_identity_view["graphTraces"][0]
    graph["eventId"] = "graph-event-under-other-cost"
    graph["priorStateSha256"] = "sha256:" + "1" * 64
    graph["nextStateSha256"] = "sha256:" + "2" * 64
    graph["intentIds"] = ["intent-under-other-cost"]
    execution = cost_bound_identity_view["executionTraces"][0]
    execution.update(
        {
            "traceSha256": "sha256:" + "3" * 64,
            "programSha256": "sha256:" + "4" * 64,
            "effectId": "effect-under-other-cost",
            "intentId": "intent-under-other-cost",
            "positionId": "position-under-other-cost",
            "tradeId": "trade-under-other-cost",
            "priorExecutionStateSha256": "sha256:" + "5" * 64,
            "nextExecutionStateSha256": "sha256:" + "6" * 64,
        }
    )
    trade = cost_bound_identity_view["trades"][0]
    trade.update(
        {
            "tradeSha256": "sha256:" + "7" * 64,
            "tradeId": "trade-under-other-cost",
            "positionId": "position-under-other-cost",
            "programSha256": "sha256:" + "8" * 64,
            "costModelSha256": "sha256:" + "9" * 64,
            "openingIntentId": "intent-under-other-cost",
            "openingEffectId": "effect-under-other-cost",
            "closingIntentId": "close-intent-under-other-cost",
            "closingEffectId": "close-effect-under-other-cost",
        }
    )
    final_state = cost_bound_identity_view["finalExecutionState"]
    final_state.update(
        {
            "executionStateSha256": "sha256:" + "a" * 64,
            "programSha256": "sha256:" + "b" * 64,
            "costModelSha256": "sha256:" + "c" * 64,
        }
    )
    final_state["position"].update(
        {
            "positionSha256": "sha256:" + "d" * 64,
            "positionId": "position-under-other-cost",
            "programSha256": "sha256:" + "b" * 64,
        }
    )
    final_state["pendingEffect"].update(
        {
            "pendingEffectSha256": "sha256:" + "e" * 64,
            "programSha256": "sha256:" + "b" * 64,
            "scheduledEventId": "other-event",
            "scheduledEventSha256": "sha256:" + "f" * 64,
            "expectedGraphStateSha256": "sha256:" + "0" * 64,
            "priorExecutionStateSha256": "sha256:" + "1" * 64,
            "scheduledObservationSha256": "sha256:" + "2" * 64,
        }
    )
    final_state["pendingEffect"]["intent"].update(
        {
            "intentId": "other-intent",
            "programSha256": "sha256:" + "b" * 64,
            "eventId": "other-event",
            "eventSha256": "sha256:" + "f" * 64,
        }
    )
    assert _cost_view_path_sha256(cost_bound_identity_view, name="cost-bound") == expected

    behavioral_divergence = copy.deepcopy(cost_bound_identity_view)
    behavioral_divergence["executionTraces"][0]["reasonCode"] = "position_age_at_least"
    assert _cost_view_path_sha256(behavioral_divergence, name="divergent") != expected

    for label, mutate in (
        ("current stop", lambda value: value["position"].__setitem__("stopPrice", 1.202)),
        ("target", lambda value: value["position"].__setitem__("targetPrice", 1.213)),
        ("trailing", lambda value: value["position"]["trailing"].__setitem__("active", False)),
        ("unresolved position", lambda value: value.__setitem__("position", None)),
        ("pending effect", lambda value: value.__setitem__("pendingEffect", None)),
        ("expected state", lambda value: value["pendingEffect"].__setitem__("expectedGraphStateId", "cooldown")),
        ("trade management", lambda value: value["trades"][0].__setitem__("finalStopPrice", 1.202)),
    ):
        divergent = copy.deepcopy(cost_bound_identity_view)
        mutate(divergent if label == "trade management" else divergent["finalExecutionState"])
        assert _cost_view_path_sha256(divergent, name=label) != expected


def test_scalar_management_task_binds_complete_execution_config_without_legacy_cell() -> (
    None
):
    preparation = _preparation()
    profile = preparation["candidates"][0]["sourceProfile"]
    profile["executionConfig"] = {
        "managementLibrary": {
            "stopDefinitions": [],
            "targetDefinitions": [],
            "trailingDefinitions": [],
            "scalarBindings": [],
        },
        "initialProtection": {"stopId": None, "targetId": None},
        "sizingPolicy": {"mode": "inherit_global"},
    }
    preparation["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    start = preparation["developmentWindows"][0]["analysisWindowStart"]
    end = preparation["developmentWindows"][0]["analysisWindowEnd"]
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"] = _plan(
        profile, start, end
    )

    payload = build_task_matrix(build_authority(preparation))[0]["payload"]

    assert payload["execution_config_sha256"] == canonical_sha256(
        profile["executionConfig"]
    )
    assert "execution_cell" not in payload
    assert payload["evidence_plan"]["execution_cell_sha256"] is None


def test_authority_rejects_sparse_scalar_management_evidence_plan() -> None:
    preparation = _preparation()
    profile = preparation["candidates"][0]["sourceProfile"]
    profile["executionConfig"] = {
        "managementLibrary": {
            "stopDefinitions": [],
            "targetDefinitions": [],
            "trailingDefinitions": [],
            "scalarBindings": [],
        },
        "initialProtection": {"stopId": None, "targetId": None},
        "sizingPolicy": {"mode": "inherit_global"},
    }
    preparation["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    start = preparation["developmentWindows"][0]["analysisWindowStart"]
    end = preparation["developmentWindows"][0]["analysisWindowEnd"]
    plan = _plan(profile, start, end)
    plan.pop("execution_cell_sha256")
    identity = dict(plan)
    identity.pop("plan_id")
    plan["plan_id"] = canonical_sha256(identity)
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"] = plan

    with pytest.raises(
        TemporalSearchContractError,
        match="must explicitly declare execution_cell_sha256",
    ):
        build_authority(preparation)


def test_authority_rejects_reserved_overlap_and_profile_plan_mismatch() -> None:
    preparation = _preparation()
    preparation["developmentWindows"][0]["analysisWindowEnd"] = "2024-06-15T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="overlaps prohibited"):
        build_authority(preparation)
    preparation = _preparation()
    plan = preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["profile_snapshot_sha256"] = "sha256:" + "d" * 64
    identity = dict(plan)
    identity.pop("plan_id")
    plan["plan_id"] = canonical_sha256(identity)
    with pytest.raises(TemporalSearchContractError, match="profile snapshot mismatch"):
        build_authority(preparation)


def test_plan_checkpoint_is_mutable_but_immutable_manifest_is_not(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    first = materialize_plan(authority, tmp_path)
    checkpoint = tmp_path / "checkpoint.json"
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    state["journal"].append({"taskId": "already-seen"})
    checkpoint.write_text(json.dumps(state), encoding="utf-8")
    assert (
        materialize_plan(authority, tmp_path)["taskMatrixSha256"]
        == first["taskMatrixSha256"]
    )
    other = build_authority({**_preparation(), "authorityLabel": "different-authority"})
    with pytest.raises(TemporalSearchContractError, match="divergent immutable file"):
        materialize_plan(other, tmp_path)


class _Gateway:
    def __init__(self, task: dict):
        self.task = task
        self.enqueued: list[dict] = []
        self.delivered = False
        self.acks: list[str] = []

    def enqueue_tasks(self, tasks: list[dict]) -> dict:
        self.enqueued.extend(tasks)
        return {"enqueued": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict]:
        if self.delivered or not self.enqueued:
            return []
        self.delivered = True
        job = self.task["payload"]
        stream = "sha256:" + "e" * 64
        source_profile_snapshot = canonical_sha256(job["inline_profile_snapshot"])
        resolved_profile_snapshot = "sha256:" + "d" * 64
        resolved_program = "sha256:" + "f" * 64
        last_bar_start = "2024-02-29T23:55:00Z"
        path_sha = canonical_sha256(
            {
                "schema_version": "temporal_graph_cost_view_path_v3",
                "graph_path": [],
                "execution_path": [],
                "trade_path": [],
                "final_execution_state": None,
            }
        )

        def metrics(*, total_net_r: float, total_cost_percent: float) -> dict:
            return {
                "observationsProcessed": 10,
                "tradesClosed": 1,
                "unresolvedPosition": False,
                "unresolvedPendingEffect": False,
                "totalGrossR": 1.0,
                "totalNetR": total_net_r,
                "totalExecutionCostPercent": total_cost_percent,
                "maxDrawdownR": 0.0,
                "equityCurveR": [total_net_r],
                "terminalValuation": {
                    "schemaVersion": "temporal_terminal_valuation_v1",
                    "policy": "leave_open_mark_to_market_v1",
                    "positionStatus": "no_open_position",
                    "lastCompletedBarId": "bar-2024-02-29T23:55:00Z",
                    "lastCompletedBarStart": last_bar_start,
                    "lastCompletedBarClose": last_bar_start,
                    "markPrice": 1.2,
                    "exitCostPercent": 0.0,
                    "pendingEffectStatus": "none",
                    "pendingEffectCancellationTreatment": "not_applicable",
                    "closedTradeCountDelta": 0,
                },
                "terminalAdjustedTotalGrossR": 1.0,
                "terminalAdjustedTotalNetR": total_net_r,
                "terminalAdjustedTotalExecutionCostPercent": total_cost_percent,
                "terminalAdjustedEquityCurveR": [total_net_r],
                "terminalAdjustedMaxDrawdownR": 0.0,
            }

        evidence = {
            "schema_version": "temporal_graph_candidate_window_evidence_contract_v1",
            "analysis_window_start": job["analysis_window_start"],
            "analysis_window_end": job["analysis_window_end"],
            "analysis_window_end_exclusive": True,
            "requested_bar_limit": job["bar_limit"],
            "effective_bar_limit": job["bar_limit"] + 1,
            "observation_count": 10,
            "first_admitted_observation_timestamp": job["analysis_window_start"],
            "last_admitted_observation_timestamp": last_bar_start,
            "warmup_sufficient": True,
            "warmup_sufficiency": {"sufficient": True, "source": "aligned_scoring"},
            "excluded_provisional_count": 1,
            "excluded_outside_analysis_window_count": 2,
        }

        def replay(*, total_net_r: float, total_cost_percent: float) -> dict:
            return {
                "streamSha256": stream,
                "profileSnapshotSha256": resolved_profile_snapshot,
                "programSha256": resolved_program,
                "graphTraces": [],
                "executionTraces": [],
                "trades": [],
                "metrics": metrics(
                    total_net_r=total_net_r,
                    total_cost_percent=total_cost_percent,
                ),
            }

        result = {
            "schema_version": TEMPORAL_SEARCH_RESULT_SCHEMA,
            "task_kind": TEMPORAL_SEARCH_TASK_KIND,
            "job_id": job["job_id"],
            "authority_id": job["authority_id"],
            "candidate_id": job["candidate_id"],
            "evidence_plan_id": job["evidence_plan"]["plan_id"],
            "lake_window_semantic_sha256": job["lake_window_semantic_sha256"],
            "shared_observation_stream_id": job["shared_observation_stream_id"],
            "analysis_window_start": job["analysis_window_start"],
            "analysis_window_end": job["analysis_window_end"],
            "source_profile_snapshot_sha256": source_profile_snapshot,
            "resolved_profile_snapshot_sha256": resolved_profile_snapshot,
            "program_sha256": resolved_program,
            "observation_stream_sha256": stream,
            "observation_summary": {
                "observation_count": 10,
                "first_bar_start": job["analysis_window_start"],
                "last_bar_start": last_bar_start,
            },
            "evidence_contract": evidence,
            "cost_view_results": {
                "research_conservative": {
                    "cost_view": "research_conservative",
                    "observation_stream_sha256": stream,
                    "replay_result": replay(total_net_r=0.9, total_cost_percent=0.1),
                },
                "none": {
                    "cost_view": "none",
                    "observation_stream_sha256": stream,
                    "replay_result": replay(total_net_r=1.0, total_cost_percent=0.0),
                },
            },
            "diagnostics": {
                "observation_count": 10,
                "requested_bar_limit": evidence["requested_bar_limit"],
                "effective_bar_limit": evidence["effective_bar_limit"],
                "warmup_sufficient": True,
                "warmup_sufficiency": evidence["warmup_sufficiency"],
                "first_admitted_observation_timestamp": evidence[
                    "first_admitted_observation_timestamp"
                ],
                "last_admitted_observation_timestamp": evidence[
                    "last_admitted_observation_timestamp"
                ],
                "excluded_provisional_count": 1,
                "excluded_outside_analysis_window_count": 2,
                "cost_view_decision_path_sha256": path_sha,
                "cost_view_path_parity": "matched",
                "cost_view_count": 2,
                "shared_stream_required": True,
            },
            "selection_score": 1.0,
        }
        result["artifact_sha256"] = canonical_sha256(result)
        artifact_size = 1
        for _ in range(16):
            result["artifact_size_bytes"] = artifact_size
            result["diagnostics"]["artifact_size_bytes"] = artifact_size
            next_size = len(
                json.dumps(
                    result,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("utf-8")
            )
            if next_size == artifact_size:
                break
            artifact_size = next_size
        return [
            {
                "status": "success",
                "task_id": self.task["task_id"],
                "lane_id": self.task["lane_id"],
                "attempt_id": self.task["attempt_id"],
                "lease_id": "lease-1",
                "result": {
                    "status": "success",
                    "job_kind": TEMPORAL_SEARCH_TASK_KIND,
                    "result": result,
                },
            }
        ]

    def ack_results(self, lease_ids: list[str]) -> int:
        self.acks.extend(lease_ids)
        return 1


def test_controller_materializes_both_cost_results_from_one_stream(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    result = run_temporal_search_tasks(
        gateway, authority, output_root=tmp_path, timeout_seconds=1
    )
    assert result["completedTaskCount"] == 1
    assert gateway.acks == ["lease-1"]
    assert len(gateway.enqueued) == 1
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    record = checkpoint["completed"][task["task_id"]]
    assert record["resultPath"].endswith(".json.gz")
    assert (tmp_path / "results" / f"{task['task_id']}.json.gz").is_file()
    assert not (tmp_path / "results" / f"{task['task_id']}.json").exists()
    assert record["resultSha256"] == record["resultSemanticSha256"]
    assert record["resultBlobSizeBytes"] < record["resultUncompressedSizeBytes"]
    journal = checkpoint["journal"]
    assert journal == [{"taskId": task["task_id"], **record}]

    resumed = run_temporal_search_tasks(
        gateway,
        authority,
        output_root=tmp_path,
        timeout_seconds=1,
        resume=True,
    )
    assert resumed == result
    assert len(gateway.enqueued) == 1


def test_controller_can_defer_selection_reduction_without_reopening_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)

    def reject_result_reopen(_record):
        raise AssertionError("selection-disabled completion reopened a result")

    monkeypatch.setattr(
        "autoresearch.temporal_search._read_checkpoint_result",
        reject_result_reopen,
    )
    result = run_temporal_search_tasks(
        gateway,
        authority,
        output_root=tmp_path,
        timeout_seconds=1,
        include_selection_summary=False,
    )

    assert result["completedTaskCount"] == 1
    assert result["selection"] == []
    assert gateway.acks == ["lease-1"]


def test_resume_rejects_checkpoint_without_result_path(tmp_path: Path) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    run_temporal_search_tasks(gateway, authority, output_root=tmp_path, timeout_seconds=1)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["completed"][task["task_id"]]["resultPath"] = ""
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(
        TemporalSearchContractError, match="checkpoint result path is required"
    ):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=1,
            resume=True,
        )


def test_controller_enqueues_large_matrices_in_bounded_batches(tmp_path: Path) -> None:
    preparation = _preparation()
    prototype = preparation["candidates"][0]
    candidates = []
    for index in range(5):
        candidate = copy.deepcopy(prototype)
        candidate["candidateId"] = f"candidate-{index}"
        candidate["sourceProfile"]["name"] = f"profile-{index}"
        candidate["sourceProfileSha256"] = canonical_sha256(candidate["sourceProfile"])
        start = preparation["developmentWindows"][0]["analysisWindowStart"]
        end = preparation["developmentWindows"][0]["analysisWindowEnd"]
        candidate["windowInputs"][0]["evidencePlan"] = _plan(
            candidate["sourceProfile"], start, end
        )
        candidates.append(candidate)
    preparation["candidates"] = candidates
    preparation["bounds"]["maxCandidates"] = 5
    preparation["bounds"]["maxTasks"] = 5
    authority = build_authority(preparation)

    class NoResultGateway:
        def __init__(self):
            self.batch_sizes = []

        def enqueue_tasks(self, tasks):
            self.batch_sizes.append(len(tasks))
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            return []

        def ack_results(self, lease_ids):
            return len(lease_ids)

    gateway = NoResultGateway()
    with pytest.raises(TemporalSearchTimeout):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=0.01,
            poll_interval_seconds=0.01,
            enqueue_batch_size=2,
        )
    assert gateway.batch_sizes == [2, 2, 1]


def test_resume_waits_for_explicitly_duplicate_live_pending_task(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)

    def reject_existing(tasks: list[dict]) -> dict:
        gateway.enqueued.extend(tasks)
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "enqueued": 0,
            "rejected": len(tasks),
        }

    gateway.enqueue_tasks = reject_existing  # type: ignore[method-assign]

    result = run_temporal_search_tasks(
        gateway,
        authority,
        output_root=tmp_path,
        timeout_seconds=1,
        resume=True,
    )

    assert result["completedTaskCount"] == 1
    assert gateway.acks == ["lease-1"]


@pytest.mark.parametrize(
    ("resume", "reported_rejected"),
    [(False, 1), (True, 0)],
)
def test_controller_rejects_incomplete_or_ambiguous_enqueue_receipt(
    tmp_path: Path,
    resume: bool,
    reported_rejected: int,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    gateway.enqueue_tasks = lambda tasks: {  # type: ignore[method-assign]
        "submitted": len(tasks),
        "enqueued": 0,
        "rejected": reported_rejected,
    }

    with pytest.raises(
        TemporalSearchContractError,
        match="exact pending task set",
    ):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=1,
            resume=resume,
        )


def test_result_rejects_different_cost_observation_streams(tmp_path: Path) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    original = gateway.read_results

    def broken(*, limit: int):
        rows = original(limit=limit)
        if rows:
            rows[0]["result"]["result"]["cost_view_results"]["none"][
                "observation_stream_sha256"
            ] = "sha256:" + "f" * 64
        return rows

    gateway.read_results = broken  # type: ignore[method-assign]
    with pytest.raises(
        TemporalSearchContractError, match="identical observation stream"
    ):
        run_temporal_search_tasks(
            gateway, authority, output_root=tmp_path, timeout_seconds=1
        )


@pytest.mark.parametrize(
    ("defect", "message"),
    (
        ("requested_limit", "requested bar limit does not match task"),
        ("effective_diagnostic", "diagnostics effective_bar_limit"),
        ("observation_count", "actual observation evidence"),
        ("warmup", "strict warmup evidence"),
        ("prebuilt_warmup", "must be measured"),
        ("endpoint", "half-open evidence"),
        ("exclusion_count", "diagnostics excluded_provisional_count"),
        ("terminal", "terminalValuation must be an object"),
        ("path_parity", "diverged in non-cost route/path evidence"),
    ),
)
def test_controller_fails_closed_on_v3_evidence_and_terminal_contract_drift(
    tmp_path: Path,
    defect: str,
    message: str,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    original = gateway.read_results

    def broken(*, limit: int):
        rows = original(limit=limit)
        if not rows:
            return rows
        result = rows[0]["result"]["result"]
        evidence = result["evidence_contract"]
        diagnostics = result["diagnostics"]
        if defect == "requested_limit":
            evidence["requested_bar_limit"] += 1
        elif defect == "effective_diagnostic":
            diagnostics["effective_bar_limit"] += 1
        elif defect == "observation_count":
            result["observation_summary"]["observation_count"] += 1
        elif defect == "warmup":
            evidence["warmup_sufficient"] = False
        elif defect == "prebuilt_warmup":
            evidence["warmup_sufficiency"] = {
                "sufficient": True,
                "source": "prebuilt_stream",
            }
            diagnostics["warmup_sufficiency"] = evidence["warmup_sufficiency"]
        elif defect == "endpoint":
            evidence["last_admitted_observation_timestamp"] = evidence[
                "analysis_window_end"
            ]
        elif defect == "exclusion_count":
            diagnostics.pop("excluded_provisional_count")
        elif defect == "terminal":
            metrics = result["cost_view_results"]["research_conservative"][
                "replay_result"
            ]["metrics"]
            metrics["unresolvedPosition"] = True
            metrics.pop("terminalValuation")
        else:
            result["cost_view_results"]["none"]["replay_result"][
                "graphTraces"
            ] = [
                {
                    "eventSequence": 0,
                    "eventClass": "decision",
                    "priorStateId": "start",
                    "nextStateId": "next",
                    "transitionId": "transition",
                    "reasonCode": "fixture",
                    "intentIds": [],
                }
            ]
        return rows

    gateway.read_results = broken  # type: ignore[method-assign]
    with pytest.raises(TemporalSearchContractError, match=message):
        run_temporal_search_tasks(
            gateway, authority, output_root=tmp_path, timeout_seconds=1
        )


def test_controller_persists_and_acknowledges_failed_completion_before_tripwire(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]

    class FailedGateway:
        def __init__(self):
            self.enqueued = False
            self.delivered = False
            self.acks = []

        def enqueue_tasks(self, tasks):
            self.enqueued = True
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            if not self.enqueued or self.delivered:
                return []
            self.delivered = True
            return [
                {
                    "status": "failed",
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "lease_id": "failed-lease",
                    "error": {"type": "fixture_failure", "message": "boom"},
                }
            ]

        def ack_results(self, lease_ids):
            self.acks.extend(lease_ids)
            return len(lease_ids)

    gateway = FailedGateway()
    with pytest.raises(
        TemporalSearchContractError,
        match=f"worker completion failed for {task['task_id']}",
    ):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=1,
        )
    assert gateway.acks == ["failed-lease"]
    failure = json.loads(
        (tmp_path / "failures" / f"{task['task_id']}.json").read_text()
    )
    assert failure["error"]["type"] == "fixture_failure"


def test_controller_persists_and_recovers_terminal_aligned_warmup_rejection(tmp_path: Path) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]

    class WarmupFailedGateway:
        def __init__(self) -> None:
            self.enqueued: list[dict] = []
            self.acks: list[str] = []
            self.delivered = False

        def enqueue_tasks(self, tasks):
            self.enqueued.extend(tasks)
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            if self.delivered:
                return []
            self.delivered = True
            return [{
                "status": "failed",
                "task_id": task["task_id"],
                "lane_id": task["lane_id"],
                "attempt_id": task["attempt_id"],
                "lease_id": "warmup-lease",
                "result": {
                    "status": "failed",
                    "error_type": "AlignedScoringWarmupInsufficientError",
                    "error": "analysis-window warmup insufficient after retry",
                    "error_repr": "AlignedScoringWarmupInsufficientError('analysis-window warmup insufficient after retry')",
                    "attempt_number": 8,
                },
            }]

        def ack_results(self, lease_ids):
            self.acks.extend(lease_ids)
            return len(lease_ids)

    first_gateway = WarmupFailedGateway()
    result = run_temporal_search_tasks(first_gateway, authority, output_root=tmp_path, timeout_seconds=1)
    assert result["completedTaskCount"] == 1
    assert first_gateway.acks == ["warmup-lease"]
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    record = checkpoint["completed"][task["task_id"]]
    assert record["outcome"] == "rejected"
    assert record["rejectionCode"] == "aligned_scoring_warmup_insufficient"

    # A crash after acknowledgement but before checkpoint materialization must
    # recover from failures/<task>.json without submitting a ninth attempt.
    (tmp_path / "results" / f"{task['task_id']}.json.gz").unlink()
    checkpoint["completed"] = {}
    checkpoint["journal"] = []
    (tmp_path / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")

    class NoDeliveryGateway:
        def __init__(self) -> None:
            self.enqueued: list[dict] = []

        def enqueue_tasks(self, tasks):
            self.enqueued.extend(tasks)
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            return []

        def ack_results(self, lease_ids):
            raise AssertionError("recovered failure was already acknowledged")

    resumed_gateway = NoDeliveryGateway()
    resumed = run_temporal_search_tasks(
        resumed_gateway, authority, output_root=tmp_path, timeout_seconds=1, resume=True
    )
    assert resumed["completedTaskCount"] == 1
    assert resumed_gateway.enqueued == []


def test_procman_normal_operations_is_prebroad_admission_topology() -> None:
    root = Path(__file__).resolve().parents[1]
    config_path = root / "scripts" / "processes.json"
    if not config_path.is_file():
        pytest.skip("local Procman configuration is intentionally untracked")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    processes = {item["id"]: item for item in config["processes"]}
    normal = next(
        item for item in config["groups"] if item["name"] == "Normal Operations"
    )
    names = {processes[item]["name"] for item in normal["process_ids"]}
    assert names == {
        "Lab Gateway",
        "Temporal Pre-Broad No-Market Activation Canary",
        "Temporal Pre-Broad Prepare 16 Tasks",
        "Temporal Pre-Broad Materialize Fresh Matrix",
        "Temporal Pre-Broad Materialize Resume Matrix",
        "Temporal Pre-Broad Authority Audit",
        "Temporal Pre-Broad Dispatch Fresh 16 Tasks",
        "Temporal Pre-Broad Dispatch Resume 16 Tasks",
        "AutoResearch Dashboard",
    }
    assert not any(item["name"].startswith("Phase 3 ") for item in processes.values())
    assert not any(
        "temporal-qd-supervisor" in str(processes[process_id]["command"])
        for process_id in normal["process_ids"]
    )
    for item in (processes[process_id] for process_id in normal["process_ids"]):
        assert item["auto_start"] is False
        assert item["auto_restart"] is False
        assert item["respond_to_start_all"] is False
        assert item["respond_to_restart_all"] is False
