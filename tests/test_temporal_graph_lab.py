from __future__ import annotations

import copy
from pathlib import Path

import pytest

from autoresearch.play_hand_lab_gateway import LabTask, PlayHandLabGateway
from autoresearch.temporal_graph_lab import (
    TEMPORAL_GRAPH_REPLAY_CAPABILITY,
    TemporalGraphLabContractError,
    TemporalGraphLabMaterializationError,
    build_temporal_graph_lab_task,
    canonical_sha256,
    materialize_temporal_graph_lab_result,
    validate_temporal_graph_lab_result,
)
from autoresearch.temporal_graph_lab_coordinator import run_temporal_graph_lab_tasks


HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
HASH_C = "sha256:" + "c" * 64
HASH_D = "sha256:" + "d" * 64
HASH_E = "sha256:" + "e" * 64
HASH_F = "sha256:" + "f" * 64


def _cell() -> dict:
    return {
        "stopLossPercent": 0.5,
        "rewardMultiple": 2.0,
        "takeProfitPercent": 1.0,
    }


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "Stage 4 test profile",
        "description": "Contract fixture only.",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {"exitPolicy": {"selectedCell": _cell()}},
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
            "states": [{"id": "flat"}],
            "evidenceGroups": [],
            "eventBindings": [],
            "transitions": [],
        },
    }


def _evidence_plan(profile: dict) -> dict:
    identity = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "campaign_plan_id": "stage4-test",
        "evidence_role": "inner_validation",
        "selection_data_end": "2025-02-01T00:00:00Z",
        "analysis_window_start": "2025-01-01T00:00:00Z",
        "analysis_window_end": "2025-02-01T00:00:00Z",
        "requested_horizon_months": 1,
        "profile_snapshot_sha256": canonical_sha256(profile),
        "execution_cell_sha256": canonical_sha256(_cell()),
        "lake_window_binding": {
            "request": {
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
                "data_start": "2024-12-01T00:00:00Z",
                "data_end": "2025-02-01T00:00:00Z",
                "coverage_policy": "require_complete",
            },
            "window_semantic_sha256": HASH_A,
            "attestation_sha256": HASH_B,
            "creation_global_coverage_sha256": HASH_C,
        },
        "data_availability_cutoff": "2025-02-01T00:00:00Z",
        "coverage_policy": "require_complete",
    }
    return {
        "plan_id": canonical_sha256(identity),
        **identity,
        "lake_manifest_sha256": None,
    }


def _task(*, expected_result_sha256: str | None = HASH_D) -> dict:
    profile = _profile()
    return build_temporal_graph_lab_task(
        source_profile=profile,
        temporal_source_profile_sha256=HASH_E,
        evidence_plan=_evidence_plan(profile),
        execution_cell=_cell(),
        worker_contract_hash=HASH_C,
        instrument="eurusd",
        timeframe="m5",
        analysis_window_start="2025-01-01T00:00:00+00:00",
        analysis_window_end="2025-02-01T00:00:00Z",
        profile_id="stage4-profile",
        task_id="stage4-task",
        lane_id="stage4-lane",
        attempt_id="stage4-attempt",
        campaign_id="stage4-campaign",
        bar_limit=5000,
        expected_result_sha256=expected_result_sha256,
    )


def _material(task: dict) -> dict:
    job = task["payload"]
    result_sha = job.get("expected_result_sha256") or HASH_D
    return {
        "schema_version": "temporal_graph_lab_result_v1",
        "task_kind": "temporal_graph_replay",
        "job_id": job["job_id"],
        "profile_id": job["profile_id"],
        "source_profile_snapshot_sha256": job[
            "temporal_source_profile_sha256"
        ],
        "resolved_profile_snapshot_sha256": HASH_F,
        "program_sha256": HASH_A,
        "stream_sha256": HASH_B,
        "replay_result_sha256": result_sha,
        "final_checkpoint_sha256": HASH_C,
        "evaluator_id": "bar_single_position_execution_v1",
        "fill_policy": "temporal_bar_fill_v1",
        "end_policy": "leave_open",
        "cost_model_sha256": HASH_E,
        "evidence_plan_id": job["evidence_plan"]["plan_id"],
        "observed_window_semantic_sha256": job["evidence_plan"]
        ["lake_window_binding"]["window_semantic_sha256"],
        "observation_summary": {
            "instrument": "EURUSD",
            "timeframe": "M5",
            "observation_count": 120,
            "first_bar_start": "2025-01-01T00:00:00Z",
            "last_bar_start": "2025-01-01T09:55:00Z",
            "stream_diagnostics": {
                "alignedScoreUnit": "fraction_0_1",
                "evidenceScoreUnit": "percent_0_100",
            },
        },
        "replay_result": {
            "schemaVersion": "temporal_graph_replay_result_v1",
            "resultSha256": result_sha,
            "profileSnapshotSha256": HASH_F,
            "programSha256": HASH_A,
            "streamSha256": HASH_B,
            "finalCheckpointSha256": HASH_C,
            "evaluatorId": "bar_single_position_execution_v1",
            "fillPolicy": "temporal_bar_fill_v1",
            "endPolicy": "leave_open",
            "costModelSha256": HASH_E,
        },
        "execution_evidence": {
            "plan_id": job["evidence_plan"]["plan_id"],
            "observed_window_semantic_sha256": HASH_A,
        },
        "worker_attribution": {
            "worker_id": "worker-stage4",
            "worker_contract_hash": job["required_worker_contract_hash"],
        },
        "timing": {"total_seconds": 1.25},
        "expected_result_sha256": job.get("expected_result_sha256"),
        "parity_status": (
            "matched"
            if job.get("expected_result_sha256") is not None
            else "not_requested"
        ),
    }


def _completion(task: dict) -> dict:
    return {
        "task_id": task["task_id"],
        "lease_id": "lease-stage4",
        "worker_id": "worker-stage4",
        "lane_id": task["lane_id"],
        "attempt_id": task["attempt_id"],
        "status": "success",
        "accepted_at": 1.0,
        "accepted_at_wall": "2025-02-01T00:00:01Z",
        "result": {
            "job_id": task["payload"]["job_id"],
            "job_kind": "temporal_graph_replay",
            "status": "success",
            "started_at": "2025-02-01T00:00:00Z",
            "completed_at": "2025-02-01T00:00:01Z",
            "result": _material(task),
        },
    }


def test_task_builder_binds_worker_capability_and_evidence() -> None:
    task = _task()
    job = task["payload"]

    assert task["task_kind"] == "temporal_graph_replay"
    assert task["required_worker_capabilities"] == [
        TEMPORAL_GRAPH_REPLAY_CAPABILITY
    ]
    assert job["schema_version"] == "temporal_graph_replay_job_v1"
    assert job["instruments"] == ["EURUSD"]
    assert job["timeframe"] == "M5"
    assert job["lookback_months"] is None
    assert job["temporal_source_profile_sha256"] == HASH_E
    assert job["required_worker_contract_hash"] == HASH_C
    assert job["evidence_plan"]["schema_version"].endswith(".v2")
    assert job["evidence_plan"]["lake_window_binding"] is not None


def test_gateway_excludes_incompatible_workers_and_preserves_snapshot() -> None:
    task_payload = _task()
    gateway = PlayHandLabGateway()
    assert gateway.enqueue(LabTask.from_payload(task_payload)) is True

    wrong_contract = gateway.claim(
        "worker-wrong-contract",
        contract_hash=HASH_B,
        capabilities=[TEMPORAL_GRAPH_REPLAY_CAPABILITY],
    )
    assert wrong_contract["status"] == "no_work"
    assert wrong_contract["reason"] == "no_compatible_work"

    missing_capability = gateway.claim(
        "worker-missing-capability",
        contract_hash=HASH_C,
        capabilities=["deep_replay"],
    )
    assert missing_capability["status"] == "no_work"

    claimed = gateway.claim(
        "worker-compatible",
        contract_hash=HASH_C,
        capabilities=[TEMPORAL_GRAPH_REPLAY_CAPABILITY],
    )
    assert claimed["status"] == "leased"
    assert claimed["job_kind"] == "temporal_graph_replay"
    assert claimed["payload"] == task_payload["payload"]
    assert claimed["resolved_profile_snapshot"] == task_payload["payload"][
        "inline_profile_snapshot"
    ]


def test_result_validation_and_materialization_are_immutable(tmp_path: Path) -> None:
    task = _task()
    completion = _completion(task)
    validated = validate_temporal_graph_lab_result(task, completion)

    first = materialize_temporal_graph_lab_result(tmp_path, task, validated)
    second = materialize_temporal_graph_lab_result(tmp_path, task, validated)

    assert first == second
    bundle = Path(first["bundle_path"])
    assert (bundle / "request.json").is_file()
    assert (bundle / "result.json").is_file()
    assert (bundle / "manifest.json").is_file()
    assert first["manifest"]["replay_result_sha256"] == HASH_D
    assert first["manifest"]["parity_status"] == "matched"

    conflicting = copy.deepcopy(completion)
    conflicting["result"]["result"]["timing"]["total_seconds"] = 99.0
    conflict_validated = validate_temporal_graph_lab_result(task, conflicting)
    with pytest.raises(
        TemporalGraphLabMaterializationError,
        match="immutable temporal artifact conflict",
    ):
        materialize_temporal_graph_lab_result(
            tmp_path,
            task,
            conflict_validated,
        )


def test_result_validator_rejects_worker_and_parity_drift() -> None:
    task = _task()
    completion = _completion(task)
    completion["result"]["result"]["worker_attribution"][
        "worker_contract_hash"
    ] = HASH_B
    with pytest.raises(TemporalGraphLabContractError, match="worker contract"):
        validate_temporal_graph_lab_result(task, completion)

    completion = _completion(task)
    completion["result"]["result"]["replay_result_sha256"] = HASH_F
    with pytest.raises(TemporalGraphLabContractError):
        validate_temporal_graph_lab_result(task, completion)


class FakeClient:
    def __init__(self, completion: dict, *, preexisting: bool = False) -> None:
        self.completion = completion
        self.preexisting = preexisting
        self.enqueued = False
        self.enqueue_calls = 0
        self.acked: list[str] = []

    def enqueue_tasks(self, tasks):
        self.enqueue_calls += 1
        self.enqueued = True
        return {"enqueued": len(tasks)}

    def read_results(self, *, limit):
        if self.acked:
            return []
        if self.preexisting or self.enqueued:
            return [self.completion]
        return []

    def ack_results(self, lease_ids):
        self.acked.extend(lease_ids)
        return len(lease_ids)


def test_coordinator_acks_only_after_successful_materialization(tmp_path: Path) -> None:
    task = _task()
    client = FakeClient(_completion(task))

    artifacts = run_temporal_graph_lab_tasks(
        client,
        [task],
        output_root=tmp_path,
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert len(artifacts) == 1
    assert client.enqueue_calls == 1
    assert client.acked == ["lease-stage4"]

    invalid = _completion(task)
    invalid["result"]["result"]["worker_attribution"][
        "worker_contract_hash"
    ] = HASH_B
    invalid_client = FakeClient(invalid)
    with pytest.raises(TemporalGraphLabContractError):
        run_temporal_graph_lab_tasks(
            invalid_client,
            [task],
            output_root=tmp_path / "invalid",
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
        )
    assert invalid_client.acked == []


def test_coordinator_resumes_preexisting_unacked_result_without_reenqueue(
    tmp_path: Path,
) -> None:
    task = _task()
    client = FakeClient(_completion(task), preexisting=True)

    artifacts = run_temporal_graph_lab_tasks(
        client,
        [task],
        output_root=tmp_path,
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert len(artifacts) == 1
    assert client.enqueue_calls == 0
    assert client.acked == ["lease-stage4"]


def test_coordinator_rejects_outer_status_and_worker_attribution_drift(
    tmp_path: Path,
) -> None:
    task = _task()
    failed = _completion(task)
    failed["status"] = "failed"
    with pytest.raises(TemporalGraphLabContractError, match="not successful"):
        run_temporal_graph_lab_tasks(
            FakeClient(failed, preexisting=True),
            [task],
            output_root=tmp_path / "failed",
        )

    wrong_worker = _completion(task)
    wrong_worker["worker_id"] = "different-worker"
    with pytest.raises(TemporalGraphLabContractError, match="worker attribution"):
        run_temporal_graph_lab_tasks(
            FakeClient(wrong_worker, preexisting=True),
            [task],
            output_root=tmp_path / "worker",
        )
