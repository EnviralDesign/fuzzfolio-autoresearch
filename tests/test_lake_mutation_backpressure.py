from __future__ import annotations

import threading

import pytest
import requests

from autoresearch.ephemeral_worker_sessions import AuthPrincipal
from autoresearch.play_hand_lab_gateway import (
    LabGatewayAsgiApp,
    LabGatewayConfig,
    LabTask,
    PlayHandLabGateway,
    _is_lake_mutation_conflict,
    build_lab_gateway_http_server,
)


CANONICAL_MUTATION_MESSAGE = (
    "Remote market data lake is mutating; retry after the advertised delay"
)
MUTATION_ERROR_TYPE = "RemoteLakeMutationInProgress"


def _gateway_with_lease(
    *,
    worker_id: str = "worker-1",
    mutation_delay: float = 30.0,
) -> tuple[PlayHandLabGateway, dict]:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            lake_mutation_retry_after_seconds=mutation_delay,
            no_work_retry_after_seconds=0.01,
        )
    )
    gateway.enqueue(
        LabTask(
            task_id="task-1",
            lane_id="lane-1",
            attempt_id="attempt-1",
            max_attempts=1,
        )
    )
    gateway.register_worker(worker_id)
    claim = gateway.claim(worker_id)
    assert claim["status"] == "leased"
    return gateway, claim


def _assert_mutation_backpressure(
    gateway: PlayHandLabGateway,
    failed: dict,
    *,
    expected_delay: float,
) -> None:
    assert failed["status"] == "requeued"
    assert failed["retry_after_seconds"] == expected_delay
    assert failed["attempt_budget_preserved"] is True

    snapshot = gateway.snapshot()
    assert snapshot["failed_tasks"] == 0
    assert snapshot["queued_tasks"] == 1
    assert snapshot["lake_retry_reason"] == "mutation"
    assert snapshot["lake_circuit_state"] == "cooldown"
    assert snapshot["metrics"]["failures_requeued"] == 1
    assert snapshot["metrics"]["retry_delayed_requeues"] == 1
    assert snapshot["metrics"]["retry_preserved_attempt_requeues"] == 1
    assert snapshot["metrics"]["lake_circuit_breaker_activations"] == 1

    gateway.register_worker("worker-2")
    blocked = gateway.claim("worker-2")
    assert blocked["status"] == "no_work"
    assert blocked["reason"] == "lake_retry_delay"
    assert blocked["retry_reason"] == "mutation"
    assert blocked["retry_after_seconds"] > 0


def test_structured_mutation_uses_configured_floor_and_opens_circuit() -> None:
    gateway, claim = _gateway_with_lease(mutation_delay=30.0)

    failed = gateway.fail(
        "worker-1",
        claim["lease_id"],
        error="translated remote conflict",
        error_type="RuntimeError",
        retryable=True,
        retry_after_seconds=2.0,
        lake_error_code="lake_mutation_in_progress",
        retry_reason="mutation",
    )

    _assert_mutation_backpressure(gateway, failed, expected_delay=30.0)


def test_exact_type_and_message_fallback_opens_circuit() -> None:
    gateway, claim = _gateway_with_lease(mutation_delay=30.0)

    failed = gateway.fail(
        "worker-1",
        claim["lease_id"],
        error=CANONICAL_MUTATION_MESSAGE,
        error_type=MUTATION_ERROR_TYPE,
        retryable=True,
        retry_after_seconds=2.0,
    )

    _assert_mutation_backpressure(gateway, failed, expected_delay=30.0)


def test_isolated_rendered_error_fallback_works_without_error_type() -> None:
    gateway, claim = _gateway_with_lease(mutation_delay=30.0)

    failed = gateway.fail(
        "worker-1",
        claim["lease_id"],
        error=f"{MUTATION_ERROR_TYPE}: {CANONICAL_MUTATION_MESSAGE}",
        error_type=None,
        retryable=True,
    )

    _assert_mutation_backpressure(gateway, failed, expected_delay=30.0)


@pytest.mark.parametrize(
    ("error", "error_type"),
    [
        (CANONICAL_MUTATION_MESSAGE, "RuntimeError"),
        ("Remote market data lake is mutating", MUTATION_ERROR_TYPE),
        ("mutation", None),
        ("409 Conflict from market data lake window-attestations", None),
        (f"{MUTATION_ERROR_TYPE}: {CANONICAL_MUTATION_MESSAGE} extra", None),
        ("unrelated failure", MUTATION_ERROR_TYPE),
    ],
)
def test_mutation_fallback_does_not_classify_arbitrary_failures(
    error: str,
    error_type: str | None,
) -> None:
    assert not _is_lake_mutation_conflict(
        error=error,
        error_type=error_type,
        lake_error_code=None,
        retry_reason=None,
    )


def test_lab_ws_fail_handler_applies_metadata_missing_fallback() -> None:
    gateway, claim = _gateway_with_lease(mutation_delay=30.0)
    app = LabGatewayAsgiApp(gateway, token="secret")

    response = app._handle_worker_message(
        {
            "type": "fail",
            "worker_id": "worker-1",
            "lease_id": claim["lease_id"],
            "retryable": True,
            "retry_after_seconds": 2.0,
            "error": CANONICAL_MUTATION_MESSAGE,
            "error_type": MUTATION_ERROR_TYPE,
        },
        principal=AuthPrincipal(kind="durable_worker"),
    )

    assert response["type"] == "fail"
    _assert_mutation_backpressure(gateway, response, expected_delay=30.0)


def test_legacy_http_fail_handler_forwards_structured_mutation_metadata() -> None:
    gateway, claim = _gateway_with_lease(mutation_delay=30.0)
    server = build_lab_gateway_http_server(
        host="127.0.0.1",
        port=0,
        token="secret",
        gateway=gateway,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = int(server.server_address[1])
        response = requests.post(
            f"http://127.0.0.1:{port}/leases/{claim['lease_id']}/fail",
            headers={"Authorization": "Bearer secret"},
            json={
                "worker_id": "worker-1",
                "retryable": True,
                "retry_after_seconds": 2.0,
                "error": "translated remote conflict",
                "error_type": "RuntimeError",
                "lake_error_code": "lake_mutation_in_progress",
                "retry_reason": "mutation",
            },
            timeout=5,
        )
        response.raise_for_status()
        failed = response.json()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    _assert_mutation_backpressure(gateway, failed, expected_delay=30.0)
