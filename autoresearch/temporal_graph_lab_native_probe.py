from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .temporal_graph_lab_native_probe_common import (
    PREPARATION_SCHEMA,
    REPORT_SCHEMA,
    STATE_SCHEMA,
    _assert_no_unrelated_results,
    _build_task_from_preparation,
    _completion_identity,
)
from .temporal_graph_lab_native_probe_finish import (
    _cross_check_local_evidence,
    finish as _finish,
)
from .temporal_graph_lab_native_probe_preflight import preflight as _preflight


__all__ = [
    "PREPARATION_SCHEMA",
    "REPORT_SCHEMA",
    "STATE_SCHEMA",
    "_assert_no_unrelated_results",
    "_build_task_from_preparation",
    "_completion_identity",
    "_cross_check_local_evidence",
    "_finish",
    "_preflight",
    "main",
]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Two-phase live Lab Gateway acceptance probe for Stage 4 temporal "
            "graph distributed replay."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser(
        "preflight",
        help=(
            "enqueue the task and prove incompatible-worker exclusion before "
            "the real worker starts"
        ),
    )
    preflight.add_argument("--preparation", type=Path, required=True)
    preflight.add_argument("--gateway-url", required=True)
    preflight.add_argument("--gateway-token")
    preflight.add_argument("--state-out", type=Path, required=True)
    preflight.add_argument(
        "--request-timeout-seconds", type=float, default=30.0
    )

    finish = subparsers.add_parser(
        "finish",
        help=(
            "wait for the real worker, prove redelivery/duplicate/ack semantics, "
            "and freeze artifacts"
        ),
    )
    finish.add_argument("--state", type=Path, required=True)
    finish.add_argument("--gateway-url", required=True)
    finish.add_argument("--gateway-token")
    finish.add_argument("--output-root", type=Path, required=True)
    finish.add_argument("--report-out", type=Path, required=True)
    finish.add_argument("--timeout-seconds", type=float, default=900.0)
    finish.add_argument(
        "--poll-interval-seconds", type=float, default=0.25
    )
    finish.add_argument(
        "--request-timeout-seconds", type=float, default=30.0
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = _preflight(args) if args.command == "preflight" else _finish(args)
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_lab_native_probe_error_v1",
                    "phase": args.command,
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
