from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any, Mapping

from .temporal_graph_lab import (
    TEMPORAL_GRAPH_REPLAY_TASK_KIND,
    LabGatewayClientProtocol,
    TemporalGraphLabContractError,
    TemporalGraphLabTimeout,
    materialize_temporal_graph_lab_result,
    validate_temporal_graph_lab_result,
)


def _clone_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalGraphLabContractError(f"{field_name} must be an object")
    try:
        return json.loads(
            json.dumps(
                dict(value),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise TemporalGraphLabContractError(
            f"{field_name} is not finite JSON"
        ) from exc


def _task_map(tasks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for raw in tasks:
        task = _clone_mapping(raw, field_name="task")
        task_id = str(task.get("task_id") or "").strip()
        if not task_id:
            raise TemporalGraphLabContractError("task_id is required")
        if task_id in output:
            raise TemporalGraphLabContractError(f"duplicate task ID: {task_id}")
        if task.get("task_kind") != TEMPORAL_GRAPH_REPLAY_TASK_KIND:
            raise TemporalGraphLabContractError("unexpected task kind")
        output[task_id] = task
    return output


def _validate_completion_delivery(
    task: Mapping[str, Any],
    completion: Mapping[str, Any],
):
    if str(completion.get("status") or "").strip().lower() != "success":
        raise TemporalGraphLabContractError(
            f"Lab completion is not successful: {completion.get('status')!r}"
        )
    validated = validate_temporal_graph_lab_result(task, completion)
    envelope_job_id = str(validated.worker_envelope.get("job_id") or "").strip()
    expected_job_id = str((task.get("payload") or {}).get("job_id") or "").strip()
    if envelope_job_id != expected_job_id:
        raise TemporalGraphLabContractError("worker envelope job identity mismatch")
    completion_worker = str(completion.get("worker_id") or "").strip()
    material_worker = str(
        (validated.material_result.get("worker_attribution") or {}).get("worker_id")
        or ""
    ).strip()
    if completion_worker and material_worker and completion_worker != material_worker:
        raise TemporalGraphLabContractError("worker attribution identity mismatch")
    return validated


def _consume_completion(
    client: LabGatewayClientProtocol,
    *,
    task: dict[str, Any],
    completion: Mapping[str, Any],
    output_root: Path | str,
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    validated = _validate_completion_delivery(task, completion)
    artifact = materialize_temporal_graph_lab_result(
        output_root,
        task,
        validated,
    )
    if existing is not None and existing["manifest"] != artifact["manifest"]:
        raise TemporalGraphLabContractError(
            f"conflicting duplicate temporal result: {validated.task_id}"
        )
    acked = client.ack_results([validated.lease_id])
    if acked != 1:
        raise TemporalGraphLabContractError(
            f"gateway did not acknowledge result lease {validated.lease_id}"
        )
    return artifact


def run_temporal_graph_lab_tasks(
    client: LabGatewayClientProtocol,
    tasks: list[dict[str, Any]],
    *,
    output_root: Path | str,
    timeout_seconds: float = 900.0,
    poll_interval_seconds: float = 0.25,
) -> list[dict[str, Any]]:
    """Run or resume one isolated set of temporal Lab tasks.

    Results already present for the exact requested task identities are validated,
    materialized, and acknowledged before enqueue. This makes a coordinator crash
    after file materialization but before result acknowledgement safely resumable.
    Unrelated result backlog still fails closed.
    """

    if not tasks:
        return []
    tasks_by_id = _task_map(tasks)
    completed: dict[str, dict[str, Any]] = {}

    preexisting = client.read_results(limit=max(len(tasks_by_id) * 2, 32))
    for completion in preexisting:
        task_id = str(completion.get("task_id") or "").strip()
        task = tasks_by_id.get(task_id)
        if task is None:
            raise TemporalGraphLabContractError(
                f"unrelated Lab result encountered before enqueue: {task_id or '<missing>'}"
            )
        completed[task_id] = _consume_completion(
            client,
            task=task,
            completion=completion,
            output_root=output_root,
            existing=completed.get(task_id),
        )

    pending = set(tasks_by_id) - set(completed)
    if pending:
        enqueue_receipt = client.enqueue_tasks(
            [tasks_by_id[task_id] for task_id in tasks_by_id if task_id in pending]
        )
        enqueued = int(enqueue_receipt.get("enqueued") or 0)
        if enqueued != len(pending):
            raise TemporalGraphLabContractError(
                f"gateway enqueued {enqueued} of {len(pending)} temporal tasks"
            )

    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while pending:
        if time.monotonic() >= deadline:
            raise TemporalGraphLabTimeout(
                "timed out waiting for temporal graph Lab results"
            )
        results = client.read_results(limit=max(len(pending) * 2, 8))
        if not results:
            time.sleep(max(float(poll_interval_seconds), 0.01))
            continue
        for completion in results:
            task_id = str(completion.get("task_id") or "").strip()
            task = tasks_by_id.get(task_id)
            if task is None:
                raise TemporalGraphLabContractError(
                    f"unrelated Lab result encountered: {task_id or '<missing>'}"
                )
            completed[task_id] = _consume_completion(
                client,
                task=task,
                completion=completion,
                output_root=output_root,
                existing=completed.get(task_id),
            )
            pending.discard(task_id)

    return [completed[task_id] for task_id in tasks_by_id]


__all__ = ["run_temporal_graph_lab_tasks"]
