import asyncio
import json
import threading
import time
from pathlib import Path

import pytest
import requests
from websockets.asyncio.client import connect as websocket_connect
from autoresearch.__main__ import build_parser
from autoresearch.play_hand_lab_gateway import (
    LabGatewayConfig,
    LabTask,
    PlayHandLabGateway,
    SaturationSimulationConfig,
    WebSocketSaturationSimulationConfig,
    build_lab_gateway_http_server,
    cmd_play_hand_lab_gateway,
    _start_uvicorn_gateway_thread,
    run_http_saturation_simulation_sync,
    run_saturation_simulation,
    HttpSaturationSimulationConfig,
    run_websocket_saturation_simulation_sync,
)
from autoresearch.play_hand_lab import _worker_result_identity


def test_lab_gateway_defaults_are_cloud_tolerant() -> None:
    config = LabGatewayConfig()

    assert config.lease_ttl_seconds == 600.0
    assert config.worker_stale_after_seconds == 600.0
    assert config.worker_prune_after_seconds == 1800.0
    assert config.max_result_backlog_bytes == 2 * 1024 * 1024 * 1024
    assert config.result_backpressure_bytes == 1024 * 1024 * 1024
    assert config.lake_mutation_retry_after_seconds == 90.0
    assert config.lake_timeout_retry_after_seconds == 45.0


def test_lab_gateway_claim_complete_and_duplicate_completion() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            payload={"work_seconds": 10},
        )
    )

    gateway.register_worker("worker-1", pool="local")
    claim = gateway.claim("worker-1", pool="local")

    assert claim["status"] == "leased"
    assert claim["task_id"] == "task-1"
    assert claim["task"]["payload"]["work_seconds"] == 10
    assert claim["queue_name"] == "QUEUE:deep_replay_jobs"
    assert claim["stream_message_id"] == "lab:task-1:1"
    assert claim["job_kind"] == "fake_compute"
    assert claim["payload"] == {"work_seconds": 10}

    completion = gateway.complete(
        "worker-1",
        claim["lease_id"],
        result={"score": 12.5},
    )
    assert completion["status"] == "accepted"
    assert completion["completion"]["task_id"] == "task-1"
    assert completion["completion"]["status"] == "success"

    duplicate = gateway.complete(
        "worker-1",
        claim["lease_id"],
        result={"score": 99.0},
    )
    assert duplicate["status"] == "duplicate"
    assert duplicate["completion"]["task_id"] == "task-1"
    assert duplicate["completion"]["status"] == "success"

    snapshot = gateway.snapshot()
    assert snapshot["completed_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["retained_task_count"] == 0
    assert snapshot["metrics"]["terminal_tasks_pruned"] == 1
    assert snapshot["worker_busy_rate"] == 0.0


def test_result_identity_survives_ack_failure_and_redelivery() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(LabTask(task_id="task-redeliver", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway.complete("worker-1", claim["lease_id"], result={"score": 12.5})

    first = gateway.read_results(limit=1)[0]
    assert gateway.ack_results(["not-the-delivered-lease"]) == 0
    second = gateway.read_results(limit=1)[0]
    assert first["accepted_at_wall"] != ""
    assert _worker_result_identity(first) == _worker_result_identity(second)

    changed = {**second, "result": {"score": 99.0}}
    assert _worker_result_identity(changed) != _worker_result_identity(second)


def test_lab_gateway_failed_completion_counts_as_failed_task() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")

    completion = gateway.complete(
        "worker-1",
        claim["lease_id"],
        status="failed",
        result={"status": "failed", "error": "worker reported failure"},
    )

    assert completion["status"] == "accepted"
    snapshot = gateway.snapshot()
    assert snapshot["completed_tasks"] == 0
    assert snapshot["failed_tasks"] == 1
    assert snapshot["metrics"]["failed_completions"] == 1
    results = gateway.read_results(limit=1)
    assert results[0]["status"] == "failed"
    assert results[0]["result"]["error"] == "worker reported failure"


def test_lab_gateway_failed_result_payload_sets_top_level_failed_status() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")

    gateway.complete(
        "worker-1",
        claim["lease_id"],
        result={"status": "failed", "error": "worker reported failure"},
    )

    snapshot = gateway.snapshot()
    results = gateway.read_results(limit=1)
    assert snapshot["completed_tasks"] == 0
    assert snapshot["failed_tasks"] == 1
    assert results[0]["status"] == "failed"


def test_lab_gateway_sweep_claim_exposes_resolved_profile_snapshot() -> None:
    profile_snapshot = {"name": "base", "indicators": [{"id": "RSI"}]}
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            task_kind="sweep_shard",
            payload={
                "shard_id": "shard-1",
                "base_profile_snapshot": profile_snapshot,
                "required_capabilities": ["deep_replay", "sweep_shard"],
            },
            required_worker_capabilities={"deep_replay", "sweep_shard"},
        )
    )
    gateway.register_worker("worker-1", capabilities=["deep_replay", "sweep_shard"])

    claim = gateway.claim("worker-1", capabilities=["deep_replay", "sweep_shard"])

    assert claim["status"] == "leased"
    assert claim["task"]["payload"]["base_profile_snapshot"] == profile_snapshot
    assert claim["task"]["resolved_profile_snapshot"] == profile_snapshot
    assert claim["queue_name"] == "QUEUE:sweep_shard_jobs"
    assert claim["job_kind"] == "sweep_shard"
    assert claim["payload"]["base_profile_snapshot"] == profile_snapshot
    assert claim["resolved_profile_snapshot"] == profile_snapshot


def test_lab_gateway_full_backtest_claim_matches_replay_worker_contract() -> None:
    profile_snapshot = {"name": "bounded", "notificationThreshold": 80}
    required_hash = "sha256:" + "a" * 64
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="full-backtest-1",
            lane_id="corpus-full-backtest",
            attempt_id="attempt-1",
            task_kind="full_backtest_cache",
            payload={
                "job_id": "full-backtest-1",
                "inline_profile_snapshot": profile_snapshot,
                "required_worker_contract_hash": required_hash,
                "required_capabilities": ["deep_replay", "full_backtest_cache"],
            },
            required_worker_capabilities={"full_backtest_cache"},
        )
    )
    gateway.register_worker(
        "worker-1",
        contract_hash=required_hash,
        capabilities=["deep_replay", "full_backtest_cache"],
    )

    claim = gateway.claim(
        "worker-1",
        contract_hash=required_hash,
        capabilities=["deep_replay", "full_backtest_cache"],
    )

    assert claim["status"] == "leased"
    assert claim["queue_name"] == "QUEUE:deep_replay_jobs"
    assert claim["job_kind"] == "full_backtest_cache"
    assert claim["payload"]["job_id"] == "full-backtest-1"
    assert claim["resolved_profile_snapshot"] == profile_snapshot
    assert claim["required_worker_contract_hash"] == required_hash


def test_lab_gateway_requeues_retryable_failure_until_cap() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            max_attempts=2,
        )
    )

    gateway.register_worker("worker-1")
    first_claim = gateway.claim("worker-1")
    failed = gateway.fail("worker-1", first_claim["lease_id"], error="temporary", retryable=True)
    assert failed["status"] == "requeued"

    second_claim = gateway.claim("worker-1")
    terminal_result = {"outcome": "no_valid_cell", "execution_evidence": {"plan_id": "p1"}}
    failed_final = gateway.fail(
        "worker-1",
        second_claim["lease_id"],
        error="temporary",
        retryable=True,
        terminal_result=terminal_result,
    )
    assert failed_final["status"] == "failed"

    snapshot = gateway.snapshot()
    assert snapshot["failed_tasks"] == 1
    assert snapshot["queued_tasks"] == 0
    results = gateway.read_results()
    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert results[0]["result"]["error"] == "temporary"
    assert results[0]["result"]["terminal_result"] == terminal_result


def test_lab_gateway_delays_lake_mutation_retryable_failure() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lake_mutation_retry_after_seconds=30.0))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            max_attempts=3,
        )
    )

    gateway.register_worker("worker-1")
    first_claim = gateway.claim("worker-1")
    failed = gateway.fail(
        "worker-1",
        first_claim["lease_id"],
        error="Remote market data lake is mutating; retry after the mutation completes",
        retryable=True,
    )

    assert failed["status"] == "requeued"
    assert failed["retry_after_seconds"] == 30.0
    assert gateway.snapshot()["metrics"]["retry_delayed_requeues"] == 1

    delayed_claim = gateway.claim("worker-1")
    assert delayed_claim["status"] == "no_work"
    assert delayed_claim["reason"] == "retry_delay"
    assert delayed_claim["retry_after_seconds"] >= 29.0

    gateway._tasks["task-1"].available_at = time.monotonic() - 0.001
    second_claim = gateway.claim("worker-1")
    assert second_claim["status"] == "leased"
    assert second_claim["task_id"] == "task-1"
    assert second_claim["attempt_number"] == 1
    assert failed["attempt_budget_preserved"] is True


def test_lab_gateway_lake_mutation_retries_do_not_exhaust_attempt_cap() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lake_mutation_retry_after_seconds=30.0))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            max_attempts=1,
        )
    )
    gateway.register_worker("worker-1")

    first_claim = gateway.claim("worker-1")
    first_failed = gateway.fail(
        "worker-1",
        first_claim["lease_id"],
        error="Remote market data lake is mutating; retry after the mutation completes",
        retryable=True,
    )
    assert first_failed["status"] == "requeued"
    assert first_failed["attempt_budget_preserved"] is True

    gateway._tasks["task-1"].available_at = time.monotonic() - 0.001
    second_claim = gateway.claim("worker-1")
    second_failed = gateway.fail(
        "worker-1",
        second_claim["lease_id"],
        error="Remote market data lake is mutating; retry after the mutation completes",
        retryable=True,
    )

    snapshot = gateway.snapshot()
    assert second_failed["status"] == "requeued"
    assert snapshot["failed_tasks"] == 0
    assert snapshot["queued_tasks"] == 1
    assert snapshot["metrics"]["failures_final"] == 0
    assert snapshot["metrics"]["retry_preserved_attempt_requeues"] == 2


def test_lab_gateway_http_retryable_false_string_is_terminal() -> None:
    gateway = PlayHandLabGateway()
    server = build_lab_gateway_http_server(
        host="127.0.0.1",
        port=0,
        token="secret",
        gateway=gateway,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    headers = {"Authorization": "Bearer secret"}
    try:
        assert requests.post(
            f"{base_url}/tasks",
            json={
                "tasks": [
                    {
                        "task_id": "task-1",
                        "lane_id": "lane-1",
                        "attempt_id": "attempt-1",
                        "max_attempts": 2,
                    }
                ]
            },
            headers=headers,
            timeout=5,
        ).json()["enqueued"] == 1
        assert requests.post(
            f"{base_url}/register",
            json={"worker_id": "worker-1"},
            headers=headers,
            timeout=5,
        ).json()["status"] == "registered"
        claim = requests.post(
            f"{base_url}/claim",
            json={"worker_id": "worker-1"},
            headers=headers,
            timeout=5,
        ).json()

        failed = requests.post(
            f"{base_url}/leases/{claim['lease_id']}/fail",
            json={"worker_id": "worker-1", "retryable": "false", "error": "terminal"},
            headers=headers,
            timeout=5,
        ).json()

        assert failed["status"] == "failed"
        assert gateway.snapshot()["queued_tasks"] == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_lab_gateway_results_are_peeked_until_acked() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway.complete("worker-1", claim["lease_id"], result={"score": 12.5})

    first = gateway.read_results(limit=1)
    second = gateway.read_results(limit=1)

    assert first == second
    assert gateway.snapshot()["result_backlog"] == 1
    assert gateway.ack_results([claim["lease_id"]]) == 1
    assert gateway.read_results(limit=1) == []
    assert gateway.snapshot()["result_backlog_bytes"] == 0


def test_lab_gateway_duplicate_task_enqueue_is_idempotent_across_terminal_prune() -> None:
    gateway = PlayHandLabGateway()
    task = LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1")

    assert gateway.enqueue(task) is True
    assert gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1")) is False
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway.complete("worker-1", claim["lease_id"], result={"ok": True})

    assert gateway.snapshot()["retained_task_count"] == 0
    assert gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1")) is False
    assert gateway.snapshot()["metrics"]["duplicate_task_enqueues"] == 2


def test_lab_gateway_http_tasks_reports_actual_accepted_count() -> None:
    gateway = PlayHandLabGateway()
    server = build_lab_gateway_http_server(
        host="127.0.0.1",
        port=0,
        token="secret",
        gateway=gateway,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    headers = {"Authorization": "Bearer secret"}
    try:
        response = requests.post(
            f"{base_url}/tasks",
            json={
                "tasks": [
                    {"task_id": "task-1", "lane_id": "lane-1", "attempt_id": "attempt-1"},
                    {"task_id": "task-1", "lane_id": "lane-1", "attempt_id": "attempt-1"},
                ]
            },
            headers=headers,
            timeout=5,
        )
        payload = response.json()

        assert response.status_code == 200
        assert payload["submitted"] == 2
        assert payload["accepted"] == 1
        assert payload["enqueued"] == 1
        assert payload["rejected"] == 1
        assert gateway.snapshot()["metrics"]["duplicate_task_enqueues"] == 1
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_lab_gateway_result_backpressure_pauses_new_claims() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            max_result_backlog_bytes=10_000,
            result_backpressure_bytes=100,
        )
    )
    gateway.enqueue_many(
        [
            LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"),
            LabTask(task_id="task-2", lane_id="lane-1", attempt_id="attempt-2"),
        ]
    )
    gateway.register_worker("worker-1")
    first = gateway.claim("worker-1")
    gateway.complete("worker-1", first["lease_id"], result={"payload": "x" * 500})

    second = gateway.claim("worker-1")

    assert second["status"] == "no_work"
    assert second["reason"] == "result_backlog_pressure"
    snapshot = gateway.snapshot()
    assert snapshot["result_backpressure_active"] is True
    assert snapshot["queued_tasks"] == 1


def test_lab_gateway_result_backlog_is_byte_bounded() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            max_result_backlog=100,
            max_result_backlog_bytes=900,
            result_backpressure_bytes=0,
        )
    )
    for index in range(4):
        gateway.enqueue(LabTask(task_id=f"task-{index}", lane_id="lane-1", attempt_id=f"attempt-{index}"))
    gateway.register_worker("worker-1")

    for _ in range(4):
        claim = gateway.claim("worker-1")
        gateway.complete("worker-1", claim["lease_id"], result={"payload": "x" * 500})

    snapshot = gateway.snapshot()
    assert snapshot["completed_tasks"] == 4
    assert snapshot["result_backlog"] < 4
    assert snapshot["metrics"]["results_dropped"] > 0


def test_lab_gateway_enforces_worker_slots() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue_many(
        [
            LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"),
            LabTask(task_id="task-2", lane_id="lane-1", attempt_id="attempt-2"),
        ]
    )
    gateway.register_worker("worker-1", slots=1)

    first = gateway.claim("worker-1")
    second = gateway.claim("worker-1")

    assert first["status"] == "leased"
    assert second["status"] == "no_work"
    assert second["reason"] == "worker_slots_full"
    assert gateway.snapshot()["busy_slots"] == 1

    gateway.complete("worker-1", first["lease_id"], result={"ok": True})
    third = gateway.claim("worker-1")
    assert third["status"] == "leased"
    assert third["task_id"] == "task-2"


def test_lab_gateway_filters_claims_by_worker_contract() -> None:
    required_hash = "sha256:" + "a" * 64
    wrong_hash = "sha256:" + "b" * 64
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            task_kind="deep_replay",
            payload={
                "required_worker_contract_hash": required_hash,
                "required_capabilities": ["deep_replay"],
            },
        )
    )

    gateway.register_worker(
        "worker-wrong",
        contract_hash=wrong_hash,
        capabilities=["deep_replay"],
    )
    wrong_claim = gateway.claim(
        "worker-wrong",
        contract_hash=wrong_hash,
        capabilities=["deep_replay"],
    )
    assert wrong_claim["status"] == "no_work"
    assert wrong_claim["reason"] == "no_compatible_work"

    gateway.register_worker(
        "worker-right",
        contract_hash=required_hash,
        capabilities=["deep_replay"],
    )
    right_claim = gateway.claim(
        "worker-right",
        contract_hash=required_hash,
        capabilities=["deep_replay"],
    )
    assert right_claim["status"] == "leased"
    assert right_claim["task_id"] == "task-1"
    assert gateway.snapshot()["metrics"]["incompatible_claims"] == 1


def test_lab_gateway_filters_claims_by_lab_protocol_capability() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            task_kind="fake_compute",
            payload={
                "required_capabilities": [
                    "fake_compute",
                    "playhand_lab_protocol:playhand-lab-worker-v1",
                ],
            },
        )
    )

    gateway.register_worker("worker-old", capabilities=["fake_compute"])
    old_claim = gateway.claim("worker-old", capabilities=["fake_compute"])
    assert old_claim["status"] == "no_work"
    assert old_claim["reason"] == "no_compatible_work"

    gateway.register_worker(
        "worker-current",
        capabilities=["fake_compute", "playhand_lab_protocol:playhand-lab-worker-v1"],
    )
    current_claim = gateway.claim(
        "worker-current",
        capabilities=["fake_compute", "playhand_lab_protocol:playhand-lab-worker-v1"],
    )
    assert current_claim["status"] == "leased"
    assert current_claim["task_id"] == "task-1"


def test_lab_gateway_uses_scheduling_capabilities_without_leaking_to_payload() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask.from_payload(
            {
                "task_id": "task-1",
                "lane_id": "lane-1",
                "attempt_id": "attempt-1",
                "task_kind": "deep_replay",
                "required_worker_capabilities": [
                    "deep_replay",
                    "playhand_lab_protocol:playhand-lab-worker-v1",
                ],
                "payload": {
                    "job_id": "task-1",
                    "required_capabilities": ["deep_replay"],
                },
            }
        )
    )

    gateway.register_worker("worker-old", capabilities=["deep_replay"])
    old_claim = gateway.claim("worker-old", capabilities=["deep_replay"])
    assert old_claim["status"] == "no_work"
    assert old_claim["reason"] == "no_compatible_work"

    gateway.register_worker(
        "worker-current",
        capabilities=["deep_replay", "playhand_lab_protocol:playhand-lab-worker-v1"],
    )
    current_claim = gateway.claim(
        "worker-current",
        capabilities=["deep_replay", "playhand_lab_protocol:playhand-lab-worker-v1"],
    )

    assert current_claim["status"] == "leased"
    assert current_claim["task"]["payload"]["required_capabilities"] == ["deep_replay"]


def test_lab_gateway_reaps_expired_leases_and_rejects_stale_completion() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lease_ttl_seconds=0.001))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=0.001,
            max_attempts=2,
        )
    )

    gateway.register_worker("worker-1")
    first_claim = gateway.claim("worker-1")
    assert first_claim["status"] == "leased"

    gateway._leases[first_claim["lease_id"]].expires_at = 0.0
    assert gateway.reap_expired_leases() == 1

    stale = gateway.complete("worker-1", first_claim["lease_id"], result={"late": True})
    assert stale["status"] == "lease_lost"

    second_claim = gateway.claim("worker-2")
    assert second_claim["status"] == "leased"
    assert second_claim["task_id"] == "task-1"
    assert second_claim["attempt_number"] == 2


def test_lab_gateway_rejects_expired_completion_without_manual_reap() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lease_ttl_seconds=0.001))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=0.001,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway._leases[claim["lease_id"]].expires_at = 0.0

    completion = gateway.complete("worker-1", claim["lease_id"], result={"late": True})

    snapshot = gateway.snapshot()
    assert completion["status"] == "lease_lost"
    assert snapshot["completed_tasks"] == 0
    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["metrics"]["expired_leases_requeued"] == 1
    assert snapshot["metrics"]["lost_completions"] == 1


def test_lab_gateway_lease_progress_updates_worker_status_detail() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
        )
    )
    gateway.register_worker("worker-1")
    gateway.heartbeat_worker("worker-1", status_detail="processing lab lease")
    claim = gateway.claim("worker-1")

    assert gateway.heartbeat_lease(
        "worker-1",
        claim["lease_id"],
        progress={
            "status_detail": "Warming lake cache.",
            "current_step": "warming_lake_cache",
        },
    ) is True

    snapshot = gateway.snapshot(include_workers=True)
    assert snapshot["workers"][0]["status_detail"] == "Warming lake cache."
    assert snapshot["workers"][0]["progress"]["current_step"] == "warming_lake_cache"
    assert snapshot["busy_slots_by_phase"] == {"warming_lake_cache": 1}


def test_lab_gateway_snapshot_reaps_crashed_worker_lease() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lease_ttl_seconds=0.001))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=0.001,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway._leases[claim["lease_id"]].expires_at = 0.0

    snapshot = gateway.snapshot()

    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["busy_worker_count"] == 0
    assert snapshot["metrics"]["expired_leases_requeued"] == 1
    second_claim = gateway.claim("worker-2")
    assert second_claim["status"] == "leased"
    assert second_claim["attempt_number"] == 2


def test_lab_gateway_snapshot_requeues_stale_worker_lease_before_deadline() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            lease_ttl_seconds=600,
            worker_stale_after_seconds=10,
            worker_prune_after_seconds=600,
        )
    )
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=600,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    assert claim["status"] == "leased"

    gateway._workers["worker-1"].heartbeat_at = time.monotonic() - 20.0
    snapshot = gateway.snapshot(include_workers=True)

    assert snapshot["worker_count"] == 0
    assert snapshot["online_worker_count"] == 0
    assert snapshot["registered_worker_count"] == 1
    assert snapshot["stale_worker_count"] == 1
    assert snapshot["busy_worker_count"] == 0
    assert snapshot["worker_slots"] == 0
    assert snapshot["busy_slots"] == 0
    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["metrics"]["stale_worker_leases_requeued"] == 1
    assert snapshot["metrics"]["expired_leases_requeued"] == 0
    assert snapshot["workers"] == []
    assert snapshot["stale_workers"][0]["worker_id"] == "worker-1"

    second_claim = gateway.claim("worker-2")
    assert second_claim["status"] == "leased"
    assert second_claim["attempt_number"] == 2


def test_lab_gateway_snapshot_prunes_old_idle_stale_workers() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(worker_stale_after_seconds=10, worker_prune_after_seconds=20)
    )
    gateway.register_worker("worker-1")
    gateway._workers["worker-1"].heartbeat_at = 0.0

    snapshot = gateway.snapshot(include_workers=True)

    assert snapshot["worker_count"] == 0
    assert snapshot["registered_worker_count"] == 0
    assert snapshot["retained_worker_count"] == 0
    assert snapshot["stale_worker_count"] == 0
    assert snapshot["workers"] == []
    assert snapshot["stale_workers"] == []
    assert snapshot["metrics"]["workers_pruned"] == 1


def test_lab_gateway_unregister_worker_requeues_active_lease() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=600,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    assert claim["status"] == "leased"

    assert gateway.unregister_worker("worker-1") is True
    snapshot = gateway.snapshot(include_workers=True)

    assert snapshot["worker_count"] == 0
    assert snapshot["registered_worker_count"] == 0
    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["metrics"]["stale_worker_leases_requeued"] == 1
    assert snapshot["metrics"]["workers_unregistered"] == 1


def test_lab_gateway_same_worker_instance_preserves_lease_on_reregister() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=600,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1", instance_id="instance-a")
    claim = gateway.claim("worker-1")

    worker = gateway.register_worker("worker-1", instance_id="instance-a")
    snapshot = gateway.snapshot()

    assert worker["worker_instance_id"] == "instance-a"
    assert snapshot["queued_tasks"] == 0
    assert snapshot["active_leases"] == 1
    assert snapshot["metrics"]["worker_instance_replacements"] == 0
    assert gateway.heartbeat_lease("worker-1", claim["lease_id"]) is True


def test_lab_gateway_new_worker_instance_immediately_requeues_old_lease() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=2400,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1", instance_id="instance-a")
    first_claim = gateway.claim("worker-1")

    worker = gateway.register_worker("worker-1", instance_id="instance-b")
    snapshot = gateway.snapshot()

    assert worker["worker_instance_id"] == "instance-b"
    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["metrics"]["worker_instance_replacements"] == 1
    assert snapshot["metrics"]["worker_instance_leases_requeued"] == 1
    assert gateway.heartbeat_lease("worker-1", first_claim["lease_id"]) is False

    second_claim = gateway.claim("worker-1")
    assert second_claim["status"] == "leased"
    assert second_claim["task_id"] == "task-1"
    assert second_claim["attempt_number"] == 2


def test_lab_gateway_rejects_heartbeat_from_wrong_worker_instance() -> None:
    gateway = PlayHandLabGateway()
    gateway.register_worker("worker-1", instance_id="instance-a")

    assert gateway.heartbeat_worker("worker-1", instance_id="instance-b") is False
    assert gateway.heartbeat_worker("worker-1", instance_id="instance-a") is True


def test_lab_gateway_expired_lease_heartbeat_does_not_renew() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lease_ttl_seconds=0.001))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=0.001,
            max_attempts=2,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway._leases[claim["lease_id"]].expires_at = 0.0

    assert gateway.heartbeat_lease("worker-1", claim["lease_id"]) is False
    snapshot = gateway.snapshot()
    assert snapshot["queued_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["metrics"]["expired_leases_requeued"] == 1


def test_lab_gateway_expired_lease_final_failure_is_pruned_and_reported() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(lease_ttl_seconds=0.001))
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=0.001,
            max_attempts=1,
        )
    )
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    gateway._leases[claim["lease_id"]].expires_at = 0.0

    assert gateway.heartbeat_lease("worker-1", claim["lease_id"]) is False

    snapshot = gateway.snapshot()
    results = gateway.read_results(limit=1)
    assert snapshot["queued_tasks"] == 0
    assert snapshot["active_leases"] == 0
    assert snapshot["retained_task_count"] == 0
    assert snapshot["failed_tasks"] == 1
    assert snapshot["metrics"]["failures_final"] == 1
    assert snapshot["metrics"]["terminal_tasks_pruned"] == 1
    assert results[0]["status"] == "failed"
    assert results[0]["result"]["error"] == "lease_expired_retry_limit"


def test_lab_gateway_result_backlog_is_bounded() -> None:
    gateway = PlayHandLabGateway(LabGatewayConfig(max_result_backlog=2))
    for index in range(4):
        gateway.enqueue(
            LabTask(
                task_id=f"task-{index}",
                lane_id="lane-1",
                attempt_id=f"attempt-{index}",
            )
        )
    gateway.register_worker("worker-1")

    for _ in range(4):
        claim = gateway.claim("worker-1")
        gateway.complete("worker-1", claim["lease_id"], result={"ok": True})

    snapshot = gateway.snapshot()
    assert snapshot["completed_tasks"] == 4
    assert snapshot["result_backlog"] == 2
    assert snapshot["metrics"]["results_dropped"] == 2
    assert len(gateway.drain_results()) == 2


def test_lab_gateway_http_worker_lifecycle() -> None:
    gateway = PlayHandLabGateway()
    server = build_lab_gateway_http_server(
        host="127.0.0.1",
        port=0,
        token="secret",
        gateway=gateway,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    headers = {"Authorization": "Bearer secret"}
    try:
        unauthorized = requests.get(f"{base_url}/snapshot", timeout=5)
        assert unauthorized.status_code == 401

        registered = requests.post(
            f"{base_url}/register",
            json={"worker_id": "worker-1", "pool": "local", "slots": 1},
            headers=headers,
            timeout=5,
        )
        assert registered.status_code == 200
        assert registered.json()["status"] == "registered"

        enqueued = requests.post(
            f"{base_url}/tasks",
            json={
                "tasks": [
                    {
                        "task_id": "task-1",
                        "lane_id": "lane-1",
                        "attempt_id": "attempt-1",
                        "payload": {"work_seconds": 1.5},
                    }
                ]
            },
            headers=headers,
            timeout=5,
        )
        assert enqueued.status_code == 200
        assert enqueued.json()["enqueued"] == 1

        claim = requests.post(
            f"{base_url}/claim",
            json={"worker_id": "worker-1", "pool": "local"},
            headers=headers,
            timeout=5,
        )
        assert claim.status_code == 200
        claim_payload = claim.json()
        assert claim_payload["status"] == "leased"
        assert claim_payload["task"]["payload"]["work_seconds"] == 1.5
        assert claim_payload["queue_name"] == "QUEUE:deep_replay_jobs"
        assert claim_payload["payload"]["work_seconds"] == 1.5

        completed = requests.post(
            f"{base_url}/leases/{claim_payload['lease_id']}/complete",
            json={
                "worker_id": "worker-1",
                "status": "success",
                "result": {"score": 42.0},
            },
            headers=headers,
            timeout=5,
        )
        assert completed.status_code == 200
        assert completed.json()["status"] == "accepted"

        snapshot = requests.get(f"{base_url}/snapshot", headers=headers, timeout=5)
        assert snapshot.status_code == 200
        assert snapshot.json()["completed_tasks"] == 1

        results = requests.get(f"{base_url}/results?limit=1", headers=headers, timeout=5)
        assert results.status_code == 200
        assert results.json()["results"][0]["result"]["score"] == 42.0

        repeated = requests.get(f"{base_url}/results?limit=1", headers=headers, timeout=5)
        assert repeated.status_code == 200
        assert repeated.json()["results"][0]["result"]["score"] == 42.0

        acked = requests.post(
            f"{base_url}/results/ack",
            json={"lease_ids": [claim_payload["lease_id"]]},
            headers=headers,
            timeout=5,
        )
        assert acked.status_code == 200
        assert acked.json()["acked"] == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_lab_gateway_asgi_rejects_oversized_body() -> None:
    from autoresearch.play_hand_lab_gateway import create_lab_gateway_app

    async def run() -> list[dict]:
        app = create_lab_gateway_app(PlayHandLabGateway(), max_body_bytes=16)
        sent: list[dict] = []
        messages = [
            {
                "type": "http.request",
                "body": b'{"blob":"' + (b"x" * 2048) + b'"}',
                "more_body": False,
            }
        ]

        async def receive() -> dict:
            return messages.pop(0)

        async def send(message: dict) -> None:
            sent.append(message)

        await app(
            {
                "type": "http",
                "method": "POST",
                "path": "/tasks",
                "query_string": b"",
                "headers": [],
            },
            receive,
            send,
        )
        return sent

    sent = asyncio.run(run())
    assert sent[0]["status"] == 413


def test_lab_gateway_cli_uses_token_file_for_non_loopback_bind(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FUZZFOLIO_LAB_GATEWAY_TOKEN", raising=False)
    token_file = tmp_path / "gateway-token.txt"
    monkeypatch.setenv("FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE", str(token_file))
    served = {}

    def fake_serve_lab_gateway(**kwargs) -> None:
        served.update(kwargs)

    monkeypatch.setattr("autoresearch.play_hand_lab_gateway.serve_lab_gateway", fake_serve_lab_gateway)

    assert cmd_play_hand_lab_gateway(
        host="0.0.0.0",
        port=8799,
        lake_mutation_retry_after_seconds=120.0,
        lake_timeout_retry_after_seconds=60.0,
    ) == 0

    token = token_file.read_text(encoding="ascii").strip()
    assert token
    assert served["host"] == "0.0.0.0"
    assert served["port"] == 8799
    assert served["token"] == token
    assert served["lake_mutation_retry_after_seconds"] == 120.0
    assert served["lake_timeout_retry_after_seconds"] == 60.0


def test_saturation_simulation_keeps_100_virtual_workers_busy() -> None:
    result = asyncio.run(
        run_saturation_simulation(
            SaturationSimulationConfig(
                worker_count=100,
                target_completions=300,
                fixed_work_seconds=10.0,
                time_scale=0.001,
                max_wall_seconds=8.0,
                sample_interval_seconds=0.005,
            )
        )
    )

    assert result["ok"]
    assert result["snapshot"]["completed_tasks"] >= 300
    assert result["saturated_busy_rate_avg"] >= 0.90


def test_http_saturation_simulation_measures_request_boundary() -> None:
    result = run_http_saturation_simulation_sync(
        HttpSaturationSimulationConfig(
            worker_count=8,
            target_completions=24,
            work_seconds=0.001,
            max_wall_seconds=10.0,
            sample_interval_seconds=0.005,
        )
    )

    assert result["ok"]
    assert result["snapshot"]["completed_tasks"] >= 24
    assert result["claim_latency_p95_ms"] > 0
    assert result["completion_latency_p95_ms"] > 0


def test_websocket_saturation_simulation_measures_persistent_worker_path() -> None:
    result = run_websocket_saturation_simulation_sync(
        WebSocketSaturationSimulationConfig(
            worker_count=8,
            target_completions=24,
            work_seconds=0.001,
            max_wall_seconds=10.0,
            sample_interval_seconds=0.005,
        )
    )

    assert result["ok"]
    assert result["snapshot"]["completed_tasks"] >= 24
    assert result["claim_latency_p95_ms"] > 0
    assert result["completion_latency_p95_ms"] > 0


def test_lab_gateway_websocket_rejects_query_token_auth() -> None:
    gateway = PlayHandLabGateway()
    server, thread, base_url = _start_uvicorn_gateway_thread(gateway, token="secret")
    ws_url = base_url.replace("http://", "ws://", 1) + "/ws"

    async def run_probe() -> None:
        with pytest.raises(Exception):
            async with websocket_connect(f"{ws_url}?token=secret", open_timeout=5, ping_interval=None):
                pass

        async with websocket_connect(
            ws_url,
            additional_headers={"Authorization": "Bearer secret"},
            open_timeout=5,
            ping_interval=None,
        ) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "type": "register",
                        "worker_id": "worker-1",
                        "pool": "ws-test",
                        "slots": 1,
                    }
                )
            )
            response = json.loads(await websocket.recv())
            assert response["status"] == "registered"

    try:
        asyncio.run(run_probe())
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_lab_gateway_websocket_disconnect_keeps_active_lease_for_reconnect() -> None:
    gateway = PlayHandLabGateway()
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            deadline_seconds=600,
            max_attempts=2,
        )
    )
    server, thread, base_url = _start_uvicorn_gateway_thread(gateway, token="secret")
    ws_url = base_url.replace("http://", "ws://", 1) + "/ws"

    async def run_probe() -> str:
        async with websocket_connect(
            ws_url,
            additional_headers={"Authorization": "Bearer secret"},
            open_timeout=5,
            ping_interval=None,
        ) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "type": "register",
                        "worker_id": "worker-1",
                        "pool": "ws-test",
                        "slots": 1,
                    }
                )
            )
            register_response = json.loads(await websocket.recv())
            assert register_response["status"] == "registered"

            await websocket.send(
                json.dumps({"type": "claim", "worker_id": "worker-1", "pool": "ws-test"})
            )
            claim_response = json.loads(await websocket.recv())
            assert claim_response["status"] == "leased"
            assert claim_response["queue_name"] == "QUEUE:deep_replay_jobs"
            assert claim_response["job_kind"] == "fake_compute"
            assert claim_response["payload"] == {}
            return str(claim_response["lease_id"])

    try:
        lease_id = asyncio.run(run_probe())
        snapshot = gateway.snapshot(include_workers=True)

        assert snapshot["worker_count"] == 1
        assert snapshot["queued_tasks"] == 0
        assert snapshot["active_leases"] == 1
        assert snapshot["metrics"]["workers_unregistered"] == 0
        assert gateway.heartbeat_lease("worker-1", lease_id) is True
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_build_parser_accepts_play_hand_lab_sim_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["play-hand-lab-sim"])

    assert args.command == "play-hand-lab-sim"
    assert args.workers == 100
    assert args.fixed_work_seconds == 10.0
    assert args.runtime_distribution == "fixed"

    args = parser.parse_args(
        [
            "play-hand-lab-sim",
            "--workers",
            "500",
            "--target-completions",
            "1000",
            "--runtime-distribution",
            "lognormal",
            "--json",
        ]
    )

    assert args.workers == 500
    assert args.target_completions == 1000
    assert args.runtime_distribution == "lognormal"
    assert args.json is True

    args = parser.parse_args(
        [
            "play-hand-lab-gateway",
            "--host",
            "0.0.0.0",
            "--port",
            "8799",
            "--token",
            "secret",
            "--ws-ping-interval-seconds",
            "45",
            "--ws-ping-timeout-seconds",
            "240",
        ]
    )
    assert args.command == "play-hand-lab-gateway"
    assert args.host == "0.0.0.0"
    assert args.port == 8799
    assert args.token == "secret"
    assert args.ws_ping_interval_seconds == 45
    assert args.ws_ping_timeout_seconds == 240

    args = parser.parse_args(
        [
            "play-hand-lab-http-sim",
            "--workers",
            "250",
            "--target-completions",
            "500",
            "--work-seconds",
            "0.01",
            "--json",
        ]
    )
    assert args.command == "play-hand-lab-http-sim"
    assert args.workers == 250
    assert args.target_completions == 500
    assert args.work_seconds == 0.01
    assert args.json is True


def test_build_parser_accepts_play_hand_massive_v2_aliases() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "play-hand-massive-v2",
            "--target-runs",
            "12",
            "--active-runs",
            "3",
            "--max-results-per-cycle",
            "500",
            "--max-drain-seconds",
            "0.75",
            "--mode",
            "finite",
            "--enqueue-failure-limit",
            "7",
            "--enqueue-retry-base-seconds",
            "0.25",
            "--terminal-lane-retention",
            "123",
            "--seed-plan-path",
            "isolated-recipe-priors",
            "--instrument-pool-preset",
            "fx",
            "--instrument-pool-set",
            "metals,crypto",
            "--json",
        ]
    )
    assert args.command == "play-hand-massive-v2"
    assert args.target_runs == 12
    assert args.active_runs == 3
    assert args.max_results_per_cycle == 500
    assert args.max_drain_seconds == 0.75
    assert args.enqueue_failure_limit == 7
    assert args.enqueue_retry_base_seconds == 0.25
    assert args.terminal_lane_retention == 123
    assert args.seed_plan_path == Path("isolated-recipe-priors")
    assert args.mode == "finite"
    assert args.instrument_pool_preset == ["fx", "metals,crypto"]
    assert args.screen_anchor_mode == "random"
    assert args.screen_anchor_envelope_months == 36
    assert args.validation_months == 12
    assert args.validation_min_score == 45.0
    assert args.scrutiny_months == 36
    assert args.final_min_score == 40.0
    assert args.json is True

    args = parser.parse_args(
        [
            "play-hand-massive-v2",
            "--lanes",
            "12",
            "--screen-anchor-mode",
            "now",
            "--validation-months",
            "9",
            "--validation-min-score",
            "47.5",
            "--final-min-score",
            "42.5",
        ]
    )
    assert args.target_runs == 12
    assert args.screen_anchor_mode == "now"
    assert args.validation_months == 9
    assert args.validation_min_score == 47.5
    assert args.final_min_score == 42.5

    args = parser.parse_args(
        [
            "play-hand-massive-v2-gateway",
            "--port",
            "8799",
            "--max-result-backlog-mb",
            "512",
            "--result-backpressure-mb",
            "256",
            "--max-recent-completions",
            "1000",
            "--max-recent-terminal-task-ids",
            "2000",
            "--lake-mutation-retry-after-seconds",
            "120",
            "--lake-timeout-retry-after-seconds",
            "60",
        ]
    )
    assert args.command == "play-hand-massive-v2-gateway"
    assert args.port == 8799
    assert args.max_result_backlog_mb == 512
    assert args.result_backpressure_mb == 256
    assert args.max_recent_completions == 1000
    assert args.max_recent_terminal_task_ids == 2000
    assert args.lake_mutation_retry_after_seconds == 120
    assert args.lake_timeout_retry_after_seconds == 60

    args = parser.parse_args(["play-hand-massive-v2-ws-sim", "--workers", "1000"])
    assert args.command == "play-hand-massive-v2-ws-sim"
    assert args.workers == 1000

    args = parser.parse_args(
        [
            "play-hand-lab-ws-sim",
            "--workers",
            "250",
            "--target-completions",
            "500",
            "--work-seconds",
            "0.01",
            "--json",
        ]
    )
    assert args.command == "play-hand-lab-ws-sim"
    assert args.workers == 250
    assert args.target_completions == 500
    assert args.work_seconds == 0.01
    assert args.json is True
