"""CLI for the Stage 5E-4 read-only E/F diagnostics."""

from __future__ import annotations

import argparse
import json

from .temporal_search_stage5e4_diagnostics import (
    audit_stage5e4_diagnostics,
    freeze_stage5e4_diagnostics,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Freeze or audit the Stage 5E-4 read-only E/F diagnosis."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--prelaunch-root", required=True)
    freeze.add_argument("--midpoint-root", required=True)
    freeze.add_argument("--output-root", required=True)
    freeze.add_argument("--autoresearch-analysis-commit", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--output-root", required=True)
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze_stage5e4_diagnostics(
            prelaunch_root=args.prelaunch_root,
            midpoint_root=args.midpoint_root,
            output_root=args.output_root,
            autoresearch_analysis_commit=args.autoresearch_analysis_commit,
        )
    else:
        result = audit_stage5e4_diagnostics(args.output_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
