from __future__ import annotations

import argparse
from typing import Any

from .play_hand_lab import LabGatewayClient
from .temporal_graph_lab_native_probe_common import (
    STATE_SCHEMA,
    _build_task_from_preparation,
    _file_sha256,
    _load_json,
    _now_iso,
    _post_json,
    _write_json,
)


def preflight(args: argparse.Namespace) -> dict[str, Any]:
    preparation = _load_json(args.preparation)
    task = _build_task_from_preparation(preparation)
    task_id = str(task["task_id"])
    client = LabGatewayClient(
        base_url=args.gateway_url,
        token=args.gateway_token,
        timeout_seconds=args.request_timeout_seconds,
    )
    try:
        health = client.health()
        if health.get("ok") is not True:
            raise RuntimeError("Lab Gateway health check did not return ok=true")
        backlog = client.read_results(limit=32)
        if backlog:
            raise RuntimeError(
                "Lab result backlog must be empty before the isolated native probe"
            )
        enqueue = client.enqueue_tasks([task])
        if int(enqueue.get("enqueued") or 0) != 1:
            raise RuntimeError(
                f"gateway enqueued {enqueue.get('enqueued')!r} temporal tasks; expected 1"
            )

        required_hash = str(task["payload"]["required_worker_contract_hash"])
        capability = "temporal_graph_replay_v1"
        fake_cases = (
            {
                "case": "wrong_contract",
                "worker_id": f"native-probe-wrong-contract-{task_id}",
                "contract_hash": "sha256:" + "0" * 64,
                "capabilities": [capability],
            },
            {
                "case": "missing_capability",
                "worker_id": f"native-probe-missing-capability-{task_id}",
                "contract_hash": required_hash,
                "capabilities": [],
            },
        )
        exclusion_receipts: list[dict[str, Any]] = []
        for case in fake_cases :
            worker_payload = {
                "worker_id": case["worker_id"],
                "pool": "lab",
                "slots": 1,
                "contract_hash": case["contract_hash"],
                "capabilities": case["capabilities"],
            }
            registered = _post_json(
                client,
                "/register",
                worker_payload,
                token=args.gateway_token,
            )
            claim = _post_json(
                client,
                "/claim",
                worker_payload,
                token=args.gateway_token,
            )
            if registered.get("status") != "registered":
                raise RuntimeError(f"fake worker registration failed: {case['case']}")
            if claim.get("status") not in {"no_work", "no_compatible_work"}:
                raise RuntimeError(
                    f"incompatible worker leased protected work: {case['case']}"
                )
            exclusion_receipts.append(
                {
                    "case": case["case"],
                    "workerId": case["worker_id"],
                    "register": registered,
                    "claim": claim,
                }
            )

        state = {
            "schemaVersion": STATE_SCHEMA,
            "createdAt": _now_iso(),
            "preparationPath": str(args.preparation.expanduser().resolve()),
            "preparationSha256": _file_sha256(args.preparation),
            "task": task,
            "localEvidence": preparation.get("localEvidence"),
            "workerContract": preparation.get("workerContract"),
            "gatewayHealth": health,
            "enqueueReceipt": enqueue,
            "incompatibleWorkerExclusion": exclusion_receipts,
        }
        _write_json(args.state_out, state)
        return {
            "schemaVersion": "temporal_graph_lab_native_probe_preflight_result_v1",
            "taskId": task_id,
            "statePath": str(args.state_out.expanduser().resolve()),
            "incompatibleWorkerExclusion": "verified",
            "nextAction": "start the actual FuzzFolio lab_http worker, then run finish",
        }
    finally:
        client.close()
