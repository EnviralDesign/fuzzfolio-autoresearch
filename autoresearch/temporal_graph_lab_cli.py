from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from .play_hand_lab import LabGatewayClient
from .temporal_graph_lab import (
    build_temporal_graph_lab_task,
    run_temporal_graph_lab_tasks,
)


PREPARATION_SCHEMA = "temporal_graph_lab_preparation_v1"


def _load_preparation(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("preparation file must contain one JSON object")
    if payload.get("schemaVersion") != PREPARATION_SCHEMA:
        raise ValueError("unknown temporal graph Lab preparation schema")
    builder_inputs = payload.get("builderInputs")
    if not isinstance(builder_inputs, dict):
        raise ValueError("preparation file has no builderInputs object")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Submit one prepared temporal graph replay to the existing Lab "
            "Gateway, validate the worker result, and materialize its immutable bundle."
        )
    )
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--gateway-token")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.25)
    parser.add_argument("--task-json-out", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    client: LabGatewayClient | None = None
    try:
        preparation = _load_preparation(args.preparation)
        task = build_temporal_graph_lab_task(**preparation["builderInputs"])
        if args.task_json_out is not None:
            args.task_json_out.parent.mkdir(parents=True, exist_ok=True)
            args.task_json_out.write_text(
                json.dumps(task, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )
        client = LabGatewayClient(
            base_url=args.gateway_url,
            token=args.gateway_token,
            timeout_seconds=min(max(args.timeout_seconds, 5.0), 120.0),
        )
        health = client.health()
        if health.get("ok") is not True:
            raise RuntimeError("Lab Gateway health check did not return ok=true")
        artifacts = run_temporal_graph_lab_tasks(
            client,
            [task],
            output_root=args.output_root,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        output = {
            "schemaVersion": "temporal_graph_lab_submission_result_v1",
            "gateway": health,
            "taskId": task["task_id"],
            "localEvidence": preparation.get("localEvidence"),
            "artifacts": artifacts,
        }
        print(json.dumps(output, indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_lab_submission_error_v1",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
