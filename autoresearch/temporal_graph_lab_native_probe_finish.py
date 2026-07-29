from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Mapping

from .play_hand_lab import LabGatewayClient
from .temporal_graph_lab import (
    materialize_temporal_graph_lab_result,
    validate_temporal_graph_lab_result,
)
from .temporal_graph_lab_native_probe_common import (
    REPORT_SCHEMA,
    STATE_SCHEMA,
    _assert_no_unrelated_results,
    _completion_identity,
    _file_sha256,
    _load_json,
    _now_iso,
    _post_json,
    _wait_for_completion,
    _write_json,
)


def _cross_check_local_evidence(
    local: Mapping[str, Any],
    material: Mapping[str, Any],
) -> dict[str, Any]:
    checks = {
        "sourceProfileSnapshotSha256": "source_profile_snapshot_sha256",
        "resolvedProfileSnapshotSha256": "resolved_profile_snapshot_sha256",
        "programSha256": "program_sha256",
        "streamSha256": "stream_sha256",
        "resultSha256": "replay_result_sha256",
        "finalCheckpointSha256": "final_checkpoint_sha256",
    }
    verified: dict[str, Any] = {}
    for local_key, worker_key in checks.items():
        local_value = local.get(local_key)
        worker_value = material.get(worker_key)
        if local_value != worker_value:
            raise RuntimeError(
                f"local/worker parity mismatch for {local_key}: "
                f"{local_value!r} != {worker_value!r}"
            )
        verified[local_key] = local_value
    summary = material.get("observation_summary")
    if not isinstance(summary, Mapping):
        raise RuntimeError("worker material result has no observation summary")
    if int(local.get("observationCount") or 0) != int(
        summary.get("observation_count") or 0
    ):
        raise RuntimeError("local/worker observation count mismatch")
    verified["observationCount"] = int(local.get("observationCount") or 0)

    local_execution = local.get("executionEvidence")
    worker_execution = material.get("execution_evidence")
    if not isinstance(local_execution, Mapping) or not isinstance(worker_execution, Mapping):
        raise RuntimeError("local/worker execution evidence is missing")
    stable_execution_keys = (
        "expected_window_semantic_sha256",
        "observed_window_semantic_sha256",
        "semantic_contract_id",
        "lake_window_request",
        "expected_attestation_sha256",
        "observed_attestation_sha256",
    )
    execution_verified: dict[str, Any] = {}
    for key in stable_execution_keys:
        local_value = local_execution.get(key)
        worker_value = worker_execution.get(key)
        if local_value != worker_value:
            raise RuntimeError(
                f"local/worker execution evidence mismatch for {key}: "
                f"{local_value!r} != {worker_value!r}"
            )
        execution_verified[key] = local_value
    verified["executionEvidence"] = execution_verified
    return verified


def finish(args: argparse.Namespace) -> dict[str, Any]:
    state = _load_json(args.state)
    if state.get("schemaVersion") != STATE_SCHEMA:
        raise ValueError("unknown temporal graph Lab native probe state schema")
    task = state.get("task")
    local_evidence = state.get("localEvidence")
    if not isinstance(task, dict) or not isinstance(local_evidence, dict):
        raise ValueError("native probe state is missing task or local evidence")
    task_id = str(task.get("task_id") or "")
    if not task_id:
        raise ValueError("native probe state task has no task_id")

    client = LabGatewayClient(
        base_url=args.gateway_url,
        token=args.gateway_token,
        timeout_seconds=args.request_timeout_seconds,
    )
    try:
        health = client.health()
        if health.get("ok") is not True:
            raise RuntimeError("Lab Gateway health check did not return ok=true")
        first = _wait_for_completion(
            client,
            task_id=task_id,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        first_identity = _completion_identity(first)

        second_results = client.read_results(limit=32)
        second_matching = _assert_no_unrelated_results(
            second_results,
            task_id=task_id,
        )
        if len(second_matching) != 1:
            raise RuntimeError("unacknowledged result was not redelivered exactly once")
        second = second_matching[0]
        if _completion_identity(second) != first_identity:
            raise RuntimeError("redelivered result changed before acknowledgement")

        first_validated = validate_temporal_graph_lab_result(task, first)
        second_validated = validate_temporal_graph_lab_result(task, second)
        first_artifact = materialize_temporal_graph_lab_result(
            args.output_root,
            task,
            first_validated,
        )
        second_artifact = materialize_temporal_graph_lab_result(
            args.output_root,
            task,
            second_validated,
        )
        if first_artifact != second_artifact:
            raise RuntimeError(
                "idempotent result materialization changed the artifact receipt"
            )

        worker_id = str(first.get("worker_id") or "")
        lease_id = str(first.get("lease_id") or "")
        worker_result = first.get("result")
        if not worker_id or not lease_id or not isinstance(worker_result, dict):
            raise RuntimeError(
                "completion is missing worker, lease, or result identity"
            )
        duplicate = _post_json(
            client,
            f"/leases/{lease_id}/complete",
            {
                "worker_id": worker_id,
                "status": "success",
                "result": worker_result,
            },
            token=args.gateway_token,
        )
        if duplicate.get("status") != "duplicate":
            raise RuntimeError(
                "gateway did not classify repeated completion as duplicate"
            )

        after_duplicate = client.read_results(limit=32)
        duplicate_matching = _assert_no_unrelated_results(
            after_duplicate,
            task_id=task_id,
        )
        if len(duplicate_matching) != 1:
            raise RuntimeError("duplicate completion appended another result")
        if _completion_identity(duplicate_matching[0]) != first_identity:
            raise RuntimeError(
                "duplicate completion changed the redelivered result"
            )

        acked = client.ack_results([lease_id])
        if acked != 1:
            raise RuntimeError(
                f"gateway acknowledged {acked} results; expected 1"
            )
        remaining = client.read_results(limit=32)
        if remaining:
            raise RuntimeError(
                "acknowledged result remained in the gateway backlog"
            )

        parity = _cross_check_local_evidence(
            local_evidence,
            first_validated.material_result,
        )
        bundle = Path(str(first_artifact["bundle_path"]))
        artifact_files: dict[str, Any] = {}
        for name in ("request.json", "result.json", "manifest.json"):
            path = bundle / name
            artifact_files[name] = {
                "path": str(path),
                "length": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
        snapshot = client.snapshot()
        report = {
            "schemaVersion": REPORT_SCHEMA,
            "completedAt": _now_iso(),
            "taskId": task_id,
            "repositoryInputs": {
                "preparationPath": state.get("preparationPath"),
                "preparationSha256": state.get("preparationSha256"),
                "workerContract": state.get("workerContract"),
            },
            "incompatibleWorkerExclusion": state.get(
                "incompatibleWorkerExclusion"
            ),
            "resultRedelivery": {
                "status": "verified",
                "completionIdentity": first_identity,
                "leaseId": lease_id,
            },
            "duplicateCompletion": {
                "status": "verified",
                "receipt": duplicate,
            },
            "acknowledgement": {"status": "verified", "acked": acked},
            "localWorkerParity": {"status": "verified", "checks": parity},
            "artifactMaterialization": {
                "status": "verified",
                "bundlePath": str(bundle),
                "manifest": first_artifact["manifest"],
                "files": artifact_files,
            },
            "gatewayHealth": health,
            "gatewaySnapshotAfterAck": snapshot,
            "reviewOutcome": "verified",
        }
        _write_json(args.report_out, report)
        return {
            "schemaVersion": "temporal_graph_lab_native_probe_finish_result_v1",
            "taskId": task_id,
            "reportPath": str(args.report_out.expanduser().resolve()),
            "bundlePath": str(bundle),
            "reviewOutcome": "verified",
        }
    finally:
        client.close()
