"""CLI for the Stage 5E-3 mandatory screening midpoint."""

from __future__ import annotations

import argparse
import json

from .temporal_search_stage5e3_midpoint import (
    audit_stage5e3_midpoint,
    freeze_stage5e3_midpoint,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Freeze or audit the Stage 5E-3 E/F mandatory midpoint."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--root", required=True)
    freeze.add_argument("--output-root", required=True)
    freeze.add_argument("--autoresearch-analysis-commit", required=True)
    freeze.add_argument("--gateway-url", default="http://127.0.0.1:8799")
    audit = commands.add_parser("audit")
    audit.add_argument("--output-root", required=True)
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze_stage5e3_midpoint(
            root=args.root,
            output_root=args.output_root,
            autoresearch_analysis_commit=args.autoresearch_analysis_commit,
            gateway_url=args.gateway_url,
        )
    else:
        result = audit_stage5e3_midpoint(args.output_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
