"""CLI surface for the finite Stage 5D temporal candidate/window controller."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Mapping

from .play_hand_lab import LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    materialize_plan,
    run_temporal_search_tasks,
    validate_authority,
)


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchContractError(f"JSON root must be an object: {path}")
    return value


def _emit(value: Mapping[str, Any]) -> None:
    print(json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True))


def authority_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze or audit a finite temporal candidate/window search authority.")
    parser.add_argument("--preparation", type=Path)
    parser.add_argument("--authority-path", type=Path, required=True)
    parser.add_argument("--audit", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.audit:
            if args.preparation is not None:
                parser.error("--audit accepts only --authority-path")
            authority = validate_authority(_read(args.authority_path))
            _emit({"schemaVersion": "temporal_graph_candidate_window_authority_audit_v1", "ok": True, "authorityId": authority["authorityId"], "taskCount": len(authority["candidates"]) * len(authority["developmentWindows"])})
            return 0
        if args.preparation is None:
            parser.error("--preparation is required unless --audit is used")
        preparation = _read(args.preparation)
        if preparation.get("schemaVersion") != TEMPORAL_SEARCH_PREPARATION_SCHEMA:
            raise TemporalSearchContractError("unknown temporal search preparation schema")
        authority = build_authority(preparation)
        args.authority_path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(authority, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        if args.authority_path.exists() and args.authority_path.read_text(encoding="utf-8") != encoded:
            raise TemporalSearchContractError("refusing to overwrite a divergent authority")
        args.authority_path.write_text(encoded, encoding="utf-8")
        _emit({"schemaVersion": "temporal_graph_candidate_window_authority_freeze_result_v1", "authorityId": authority["authorityId"], "authorityPath": str(args.authority_path.resolve()), "taskCount": len(authority["candidates"]) * len(authority["developmentWindows"])})
        return 0
    except Exception as exc:
        _emit({"schemaVersion": "temporal_graph_candidate_window_authority_error_v1", "errorType": type(exc).__name__, "message": str(exc)})
        return 1


def search_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run or resume one finite authority-bound temporal candidate/window task matrix.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fresh", action="store_true")
    mode.add_argument("--resume", action="store_true")
    parser.add_argument("--authority-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--gateway-token")
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.25)
    parser.add_argument("--enqueue-batch-size", type=int, default=128)
    parser.add_argument("--plan-only", action="store_true", help="Validate and materialize the immutable task matrix without gateway I/O.")
    args = parser.parse_args(argv)
    client: LabGatewayClient | None = None
    try:
        authority = validate_authority(_read(args.authority_path))
        if args.plan_only:
            manifest = materialize_plan(authority, args.output_root)
            _emit({"schemaVersion": "temporal_graph_candidate_window_plan_result_v1", "authorityId": authority["authorityId"], "taskCount": manifest["taskCount"], "outputRoot": str(args.output_root.resolve())})
            return 0
        client = LabGatewayClient(
            base_url=args.gateway_url,
            token=args.gateway_token or load_lab_gateway_token(create=False),
            timeout_seconds=min(max(args.timeout_seconds, 5.0), 120.0),
        )
        health = client.health()
        if health.get("ok") is not True:
            raise RuntimeError("Lab Gateway health check did not return ok=true")
        result = run_temporal_search_tasks(client, authority, output_root=args.output_root, timeout_seconds=args.timeout_seconds, poll_interval_seconds=args.poll_interval_seconds, resume=args.resume, enqueue_batch_size=args.enqueue_batch_size)
        _emit({"gateway": health, **result})
        return 0
    except Exception as exc:
        _emit({"schemaVersion": "temporal_graph_candidate_window_search_error_v1", "errorType": type(exc).__name__, "message": str(exc)})
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(search_main())
