import asyncio
import json
import threading

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

    completion = gateway.complete(
        "worker-1",
        claim["lease_id"],
        result={"score": 12.5},
    )
    assert completion["status"] == "accepted"
    assert completion["completion"]["result"]["score"] == 12.5

    duplicate = gateway.complete(
        "worker-1",
        claim["lease_id"],
        result={"score": 99.0},
    )
    assert duplicate["status"] == "duplicate"
    assert duplicate["completion"]["result"]["score"] == 12.5

    snapshot = gateway.snapshot()
    assert snapshot["completed_tasks"] == 1
    assert snapshot["active_leases"] == 0
    assert snapshot["worker_busy_rate"] == 0.0


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
    failed_final = gateway.fail("worker-1", second_claim["lease_id"], error="temporary", retryable=True)
    assert failed_final["status"] == "failed"

    snapshot = gateway.snapshot()
    assert snapshot["failed_tasks"] == 1
    assert snapshot["queued_tasks"] == 0
    results = gateway.read_results()
    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert results[0]["result"]["error"] == "temporary"


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

    assert cmd_play_hand_lab_gateway(host="0.0.0.0", port=8799) == 0

    token = token_file.read_text(encoding="ascii").strip()
    assert token
    assert served["host"] == "0.0.0.0"
    assert served["port"] == 8799
    assert served["token"] == token


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
        ]
    )
    assert args.command == "play-hand-lab-gateway"
    assert args.host == "0.0.0.0"
    assert args.port == 8799
    assert args.token == "secret"

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
            "--mode",
            "finite",
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
    assert args.mode == "finite"
    assert args.instrument_pool_preset == ["fx", "metals,crypto"]
    assert args.json is True

    args = parser.parse_args(["play-hand-massive-v2", "--lanes", "12"])
    assert args.target_runs == 12

    args = parser.parse_args(["play-hand-massive-v2-gateway", "--port", "8799"])
    assert args.command == "play-hand-massive-v2-gateway"
    assert args.port == 8799

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
