from __future__ import annotations

from autoresearch.play_hand_lab_gateway import (
    LabGatewayConfig,
    LabTask,
    PlayHandLabGateway,
    _json_dumps_bytes,
)
from autoresearch.play_hand_lab_gateway_runtime import PackedLabResult


def _task(task_id: str) -> LabTask:
    return LabTask(
        task_id=task_id,
        lane_id="lane-001",
        attempt_id=task_id,
        task_kind="fake_compute",
        payload={"work_seconds": 0},
    )


def test_packed_results_release_backpressure_after_prefix_ack() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            result_backpressure_bytes=1_000,
            max_result_backlog_bytes=10_000_000,
        )
    )
    gateway.enqueue_many([_task("task-1"), _task("task-2")])
    gateway.register_worker("worker-1")

    first = gateway.claim("worker-1")
    assert first["status"] == "leased"
    accepted = gateway.complete(
        "worker-1",
        str(first["lease_id"]),
        result={"status": "success", "large": "x" * 50_000},
    )
    assert accepted["status"] == "accepted"
    assert isinstance(gateway._results[0], PackedLabResult)
    packed = gateway._results[0]
    assert len(packed.payload_zlib) < len(_json_dumps_bytes(packed.to_payload()))

    blocked = gateway.claim("worker-1")
    assert blocked["status"] == "no_work"
    assert blocked["reason"] == "result_backlog_pressure"

    result_batch = gateway.read_results(limit=1)
    assert result_batch[0]["result"]["large"] == "x" * 50_000
    assert gateway.ack_results([str(result_batch[0]["lease_id"])]) == 1

    released = gateway.claim("worker-1")
    assert released["status"] == "leased"
    snapshot = gateway.snapshot()
    assert snapshot["result_backlog"] == 0
    assert snapshot["result_backlog_bytes"] == 0
    assert snapshot["metrics"]["results_acked"] == 1


def test_non_prefix_ack_keeps_compatibility_fallback() -> None:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            result_backpressure_bytes=10_000_000,
            max_result_backlog_bytes=10_000_000,
        )
    )
    gateway.enqueue_many([_task("task-1"), _task("task-2")])
    gateway.register_worker("worker-1")

    lease_ids: list[str] = []
    for _ in range(2):
        claim = gateway.claim("worker-1")
        assert claim["status"] == "leased"
        lease_id = str(claim["lease_id"])
        lease_ids.append(lease_id)
        assert gateway.complete(
            "worker-1",
            lease_id,
            result={"status": "success", "payload": "y" * 1_000},
        )["status"] == "accepted"

    assert gateway.ack_results([lease_ids[1]]) == 1
    remaining = gateway.read_results(limit=10)
    assert [item["lease_id"] for item in remaining] == [lease_ids[0]]
