"""CLI for deterministic progressive temporal discovery."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .temporal_discovery import (
    SubprocessCandidateValidator,
    TemporalDiscoveryContractError,
    audit_discovery,
    finalize_discovery,
    generate_discovery,
    select_confirmation_stage,
)


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read {name}: {path}"
        ) from exc
    if not isinstance(payload, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return payload


def _validator_command(path: Path) -> list[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read validator command: {path}"
        ) from exc
    if (
        not isinstance(payload, list)
        or not payload
        or any(not isinstance(item, str) or not item.strip() for item in payload)
    ):
        raise TemporalDiscoveryContractError(
            "validator command file must contain a non-empty JSON string array"
        )
    return payload


def _emit(payload: dict[str, Any]) -> None:
    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate, screen, finalize, or audit one authority-bound "
            "temporal discovery pilot."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser(
        "generate",
        help="Generate and validate the frozen initial population.",
    )
    generate.add_argument("--preparation", type=Path, required=True)
    generate.add_argument("--output-root", type=Path, required=True)
    generate.add_argument(
        "--validator-command-file",
        type=Path,
        required=True,
        help=(
            "JSON array containing the exact FuzzFolio validator command "
            "without per-candidate arguments."
        ),
    )
    generate.add_argument(
        "--validator-timeout-seconds",
        type=float,
        default=30.0,
    )

    select = subparsers.add_parser(
        "select",
        help="Freeze the confirmation population from initial results.",
    )
    select.add_argument("--discovery-root", type=Path, required=True)
    select.add_argument("--initial-result-root", type=Path, required=True)

    finalize = subparsers.add_parser(
        "finalize",
        help="Build final four-window economic and novelty archives.",
    )
    finalize.add_argument("--discovery-root", type=Path, required=True)
    finalize.add_argument("--initial-result-root", type=Path, required=True)
    finalize.add_argument(
        "--confirmation-result-root",
        type=Path,
        required=True,
    )

    audit = subparsers.add_parser(
        "audit",
        help="Rehash the discovery authority and all local immutable artifacts.",
    )
    audit.add_argument("--discovery-root", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            validator = SubprocessCandidateValidator(
                _validator_command(args.validator_command_file),
                timeout_seconds=args.validator_timeout_seconds,
            )
            result = generate_discovery(
                _read(args.preparation, name="discovery preparation"),
                validator=validator,
                output_root=args.output_root,
            )
        elif args.command == "select":
            result = select_confirmation_stage(
                args.discovery_root,
                initial_result_root=args.initial_result_root,
            )
        elif args.command == "finalize":
            result = finalize_discovery(
                args.discovery_root,
                initial_result_root=args.initial_result_root,
                confirmation_result_root=args.confirmation_result_root,
            )
        else:
            result = audit_discovery(args.discovery_root)
        _emit(result)
        return 0
    except Exception as exc:
        _emit(
            {
                "schemaVersion": "temporal_graph_discovery_error_v1",
                "command": args.command,
                "errorType": type(exc).__name__,
                "message": str(exc),
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
