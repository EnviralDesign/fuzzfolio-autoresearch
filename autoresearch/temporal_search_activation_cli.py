from __future__ import annotations

import argparse
import json

from .temporal_search_activation import (
    audit_activation_causality,
    build_activation_causality,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build or audit the read-only Stage 5E-2 management-activation "
            "causality artifact."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser(
        "build",
        help="Diagnose the exact immutable Stage 5E-0/5E-1 result corpus.",
    )
    build.add_argument("--discovery-root", required=True)
    build.add_argument("--initial-result-root", required=True)
    build.add_argument("--confirmation-result-root", required=True)
    build.add_argument("--control-result-root", required=True)
    build.add_argument("--output-root", required=True)

    audit = commands.add_parser(
        "audit",
        help="Rehash the frozen causality report, dossiers, and manifest.",
    )
    audit.add_argument("--output-root", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "build":
        result = build_activation_causality(
            discovery_root=args.discovery_root,
            initial_result_root=args.initial_result_root,
            confirmation_result_root=args.confirmation_result_root,
            control_result_root=args.control_result_root,
            output_root=args.output_root,
        )
    else:
        result = audit_activation_causality(args.output_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
