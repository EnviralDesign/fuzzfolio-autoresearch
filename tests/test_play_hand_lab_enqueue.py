from __future__ import annotations

from typing import Any

import pytest
import requests

from autoresearch import play_hand_lab_enqueue as enqueue_module


def _http_error(status_code: int, message: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = "http://127.0.0.1:8799/tasks"
    return requests.HTTPError(message, response=response)


class _SizeLimitedGateway:
    def __init__(self, max_tasks: int) -> None:
        self.max_tasks = max_tasks
        self.attempts: list[list[str]] = []
        self.accepted_task_ids: list[str] = []

    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, int | str]:
        task_ids = [str(task["task_id"]) for task in tasks]
        self.attempts.append(task_ids)
        if len(tasks) > self.max_tasks:
            raise _http_error(413, "413 Client Error: Request Entity Too Large")
        self.accepted_task_ids.extend(task_ids)
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "accepted": len(tasks),
            "enqueued": len(tasks),
            "rejected": 0,
        }


class _TransientGateway:
    def __init__(self) -> None:
        self.calls = 0

    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, int | str]:
        self.calls += 1
        if self.calls == 1:
            raise requests.ConnectionError("gateway temporarily unavailable")
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "accepted": len(tasks),
            "enqueued": len(tasks),
            "rejected": 0,
        }


def _tasks(count: int) -> list[dict[str, Any]]:
    return [{"task_id": f"task-{index:04d}"} for index in range(count)]


def test_resume_scale_enqueue_is_bounded_to_32_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(enqueue_module.LAB_ENQUEUE_BATCH_TASKS_ENV, raising=False)
    monkeypatch.setattr(enqueue_module, "_append_event", lambda *args, **kwargs: None)
    gateway = _SizeLimitedGateway(max_tasks=32)

    result = enqueue_module.enqueue_gateway_tasks_with_retries(
        gateway,
        object(),
        _tasks(912),
        reason="resume_unresolved",
        failure_limit=5,
        retry_base_seconds=0,
    )

    assert result == {
        "status": "accepted",
        "submitted": 912,
        "accepted": 912,
        "enqueued": 912,
        "rejected": 0,
        "batch_count": 29,
    }
    assert max(map(len, gateway.attempts)) == 32
    assert gateway.accepted_task_ids == [task["task_id"] for task in _tasks(912)]


def test_413_batches_are_bisected_without_retrying_the_same_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(enqueue_module.LAB_ENQUEUE_BATCH_TASKS_ENV, "8")
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        enqueue_module,
        "_append_event",
        lambda _ctx, _phase, status, **fields: events.append((status, fields)),
    )
    gateway = _SizeLimitedGateway(max_tasks=3)

    result = enqueue_module.enqueue_gateway_tasks_with_retries(
        gateway,
        object(),
        _tasks(10),
        reason="resume_unresolved",
        failure_limit=5,
        retry_base_seconds=0,
    )

    assert result["submitted"] == 10
    assert result["accepted"] == 10
    assert gateway.accepted_task_ids == [task["task_id"] for task in _tasks(10)]
    assert [len(attempt) for attempt in gateway.attempts] == [8, 4, 2, 2, 4, 2, 2, 2]
    assert [status for status, _fields in events] == [
        "task_enqueue_split",
        "task_enqueue_split",
        "task_enqueue_split",
    ]


def test_transient_enqueue_failure_retries_the_current_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(enqueue_module.LAB_ENQUEUE_BATCH_TASKS_ENV, "32")
    events: list[tuple[str, dict[str, Any]]] = []
    sleeps: list[float] = []
    monkeypatch.setattr(
        enqueue_module,
        "_append_event",
        lambda _ctx, _phase, status, **fields: events.append((status, fields)),
    )
    monkeypatch.setattr(enqueue_module.time, "sleep", sleeps.append)
    gateway = _TransientGateway()

    result = enqueue_module.enqueue_gateway_tasks_with_retries(
        gateway,
        object(),
        _tasks(3),
        reason="lane_top_up",
        failure_limit=3,
        retry_base_seconds=0.25,
    )

    assert gateway.calls == 2
    assert result["accepted"] == 3
    assert sleeps == [0.25]
    assert [status for status, _fields in events] == ["task_enqueue_failed"]
    assert events[0][1]["attempt"] == 1


def test_single_oversized_task_fails_immediately_with_task_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(enqueue_module.LAB_ENQUEUE_BATCH_TASKS_ENV, "32")
    events: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        enqueue_module,
        "_append_event",
        lambda _ctx, _phase, status, **fields: events.append((status, fields)),
    )
    gateway = _SizeLimitedGateway(max_tasks=0)

    with pytest.raises(requests.HTTPError):
        enqueue_module.enqueue_gateway_tasks_with_retries(
            gateway,
            object(),
            [{"task_id": "too-large-task"}],
            reason="resume_unresolved",
            failure_limit=5,
            retry_base_seconds=0,
        )

    assert len(gateway.attempts) == 1
    assert events == [
        (
            "task_enqueue_too_large",
            {
                "reason": "resume_unresolved",
                "task_id": "too-large-task",
                "task_count": 1,
                "error": "413 Client Error: Request Entity Too Large",
            },
        )
    ]


def test_installer_replaces_coordinator_enqueue_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    from autoresearch import play_hand_lab

    sentinel = object()
    monkeypatch.setattr(play_hand_lab, "_enqueue_gateway_tasks_with_retries", sentinel)

    enqueue_module.install_bounded_gateway_enqueue()

    assert (
        play_hand_lab._enqueue_gateway_tasks_with_retries
        is enqueue_module.enqueue_gateway_tasks_with_retries
    )
