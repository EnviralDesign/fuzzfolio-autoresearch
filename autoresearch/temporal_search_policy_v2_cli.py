from __future__ import annotations

import argparse
import json
from pathlib import Path

from .temporal_discovery_validation import SubprocessCandidateValidator
from .temporal_search_policy_v2 import (
    GENERATOR_V2_PARAMETER_PROFILES,
    audit_management_witnesses,
    audit_policy_v2_population,
    generate_policy_v2_population,
)


def _read(path: Path, *, name: str):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not read {name}: {path}") from exc
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build or audit repository-only Stage 5E-2 policy artifacts."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser(
        "generate",
        help="Generate the 256-program activation-aware no-market population.",
    )
    generate.add_argument("--source-preparation", type=Path, required=True)
    generate.add_argument("--causality-root", type=Path, required=True)
    generate.add_argument("--output-root", type=Path, required=True)
    generate.add_argument("--validator-command-file", type=Path, required=True)
    generate.add_argument("--validator-timeout-seconds", type=float, default=30.0)
    generate.add_argument(
        "--parameter-profile",
        choices=sorted(GENERATOR_V2_PARAMETER_PROFILES),
        default="stage5e2_synthetic_admission",
    )

    audit = commands.add_parser("audit-generator", help="Audit generator v2 artifacts.")
    audit.add_argument("--output-root", type=Path, required=True)
    witnesses = commands.add_parser(
        "audit-witnesses", help="Audit native management witness artifacts."
    )
    witnesses.add_argument("--output-root", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "generate":
        command = _read(args.validator_command_file, name="validator command")
        if not isinstance(command, list) or not command or not all(
            isinstance(item, str) and item.strip() for item in command
        ):
            raise RuntimeError("validator command must be a non-empty string array")
        result = generate_policy_v2_population(
            _read(args.source_preparation, name="source preparation"),
            validator=SubprocessCandidateValidator(
                command,
                timeout_seconds=args.validator_timeout_seconds,
            ),
            causality_root=args.causality_root,
            output_root=args.output_root,
            parameters=GENERATOR_V2_PARAMETER_PROFILES[args.parameter_profile],
        )
    elif args.command == "audit-generator":
        result = audit_policy_v2_population(args.output_root)
    else:
        result = audit_management_witnesses(args.output_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
