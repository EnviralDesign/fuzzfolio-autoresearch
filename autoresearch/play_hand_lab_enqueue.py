from __future__ import annotations

import os
import time
from collections import deque
from typing import Any

import requests

from .play_hand import _append_event


DEFAULT_LAB_ENQUEUE_BATCH_TASKS = 32
LAB_ENQUEUE_BATCH_TASKS_ENV = "PLAY_HAND_LAB_ENQUEUE_BATCH_TASKS"


def _enqueue_batch_task_limit() -> int:
    raw = os.getenv(LAB_ENQUEUE_BATCH_TASKS_ENV)
    if raw:
        try:
            return max(int(raw), 1)
        except ValueError:
            pass
    return DEFAULT_LAB_ENQUEUE_BATCH_TASKS


def _request_entity_too_large(exc: requests.RequestException) -> bool:
    response = getattr(exc, "response", None)
    if response is not None and int(response.status_code or 0) == 413:
        return True
    return "413" in str(exc) and "request entity too large" in str(exc).lower()


def _enqueue_result_counts(result: Any, *, task_count: int) -> dict[str, int]:
    payload = result if isinstance(result, dict) else {}
    submitted = int(payload.get("submitted", task_count) or 0)
    accepted = int(payload.get("accepted", payload.get("enqueued", submitted)) or 0)
    enqueued = int(payload.get("enqueued", accepted) or 0)
    rejected = int(payload.get("rejected", max(submitted - accepted, 0)) or 0)
    return {
        "submitted": submitted,
        "accepted": accepted,
        "enqueued": enqueued,
        "rejected": rejected,
    }


def enqueue_gateway_tasks_with_retries(
    gateway: Any,
    campaign_ctx: Any,
    tasks: list[dict[str, Any]],
    *,
    reason: str,
    failure_limit: int,
    retry_base_seconds: float,
) -> dict[str, Any]:
    """Enqueue tasks in bounded, 413-adaptive batches.

    Resume can contain hundreds of unresolved tasks whose restored profile snapshots
    make one combined request larger than the gateway body limit. Start with a
    conservative task-count bound, then bisect only the batch rejected with 413.
    Gateway task ids are idempotent, so a later retry after partial success is safe.
    """
    if not tasks:
        return {}

    max_failures = max(int(failure_limit), 1)
    retry_base = max(float(retry_base_seconds), 0.0)
    batch_limit = _enqueue_batch_task_limit()
    pending: deque[list[dict[str, Any]]] = deque(
        tasks[offset : offset + batch_limit]
        for offset in range(0, len(tasks), batch_limit)
    )
    aggregate = {
        "status": "accepted",
        "submitted": 0,
        "accepted": 0,
        "enqueued": 0,
        "rejected": 0,
        "batch_count": 0,
    }

    while pending:
        batch = pending.popleft()
        for attempt in range(1, max_failures + 1):
            try:
                result = gateway.enqueue_tasks(batch)
            except requests.RequestException as exc:
                if _request_entity_too_large(exc):
                    if len(batch) == 1:
                        task_id = str(batch[0].get("task_id") or "unknown")
                        _append_event(
                            campaign_ctx,
                            "gateway",
                            "task_enqueue_too_large",
                            reason=reason,
                            task_id=task_id,
                            task_count=1,
                            error=str(exc)[:1000],
                        )
                        raise

                    split_at = len(batch) // 2
                    left = batch[:split_at]
                    right = batch[split_at:]
                    pending.appendleft(right)
                    pending.appendleft(left)
                    _append_event(
                        campaign_ctx,
                        "gateway",
                        "task_enqueue_split",
                        reason=reason,
                        task_count=len(batch),
                        left_task_count=len(left),
                        right_task_count=len(right),
                        http_status=413,
                    )
                    break

                _append_event(
                    campaign_ctx,
                    "gateway",
                    "task_enqueue_failed",
                    reason=reason,
                    error=str(exc)[:1000],
                    attempt=attempt,
                    failure_limit=max_failures,
                    task_count=len(batch),
                )
                if attempt >= max_failures:
                    raise
                if retry_base > 0:
                    time.sleep(min(retry_base * attempt, 30.0))
            else:
                counts = _enqueue_result_counts(result, task_count=len(batch))
                for key, value in counts.items():
                    aggregate[key] += value
                aggregate["batch_count"] += 1
                break

    if reason == "resume_unresolved":
        from .play_hand_lab_memory import release_resume_enqueue_memory

        release_resume_enqueue_memory(tasks)
    return aggregate


def install_bounded_gateway_enqueue() -> None:
    """Install bounded enqueueing and memory bounds into the coordinator."""
    from . import play_hand_lab
    from .play_hand_lab_memory import install_play_hand_lab_memory_bounds

    play_hand_lab._enqueue_gateway_tasks_with_retries = enqueue_gateway_tasks_with_retries
    install_play_hand_lab_memory_bounds()


__all__ = [
    "DEFAULT_LAB_ENQUEUE_BATCH_TASKS",
    "LAB_ENQUEUE_BATCH_TASKS_ENV",
    "enqueue_gateway_tasks_with_retries",
    "install_bounded_gateway_enqueue",
]
