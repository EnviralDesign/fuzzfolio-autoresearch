from __future__ import annotations

import argparse
import json
from pathlib import Path

from .temporal_search_stage5e3 import (
    audit_prelaunch_checkpoint,
    freeze_prelaunch_checkpoint,
    freeze_window_selection,
    prepare_screening_prelaunch,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze or audit the Stage 5E-3 modest-campaign prelaunch."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    windows = commands.add_parser("select-windows")
    windows.add_argument("--output-root", type=Path, required=True)

    prepare = commands.add_parser("prepare-screening")
    prepare.add_argument("--root", type=Path, required=True)
    prepare.add_argument("--source-preparation", type=Path, required=True)
    prepare.add_argument("--autoresearch-implementation-commit", required=True)
    prepare.add_argument("--fuzzfolio-commit", required=True)
    prepare.add_argument("--worker-contract-sha256", required=True)

    freeze = commands.add_parser("freeze")
    freeze.add_argument("--root", type=Path, required=True)
    freeze.add_argument("--autoresearch-evidence-commit", required=True)
    freeze.add_argument("--workflow-run-id", required=True)
    freeze.add_argument("--workflow-url", required=True)
    freeze.add_argument("--workflow-conclusion", required=True)

    audit = commands.add_parser("audit")
    audit.add_argument("--root", type=Path, required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "select-windows":
        result = freeze_window_selection(args.output_root)
    elif args.command == "prepare-screening":
        result = prepare_screening_prelaunch(
            root=args.root,
            source_preparation_path=args.source_preparation,
            autoresearch_implementation_commit=args.autoresearch_implementation_commit,
            fuzzfolio_commit=args.fuzzfolio_commit,
            worker_contract_sha256=args.worker_contract_sha256,
        )
    elif args.command == "freeze":
        result = freeze_prelaunch_checkpoint(
            root=args.root,
            autoresearch_evidence_commit=args.autoresearch_evidence_commit,
            workflow_run_id=args.workflow_run_id,
            workflow_url=args.workflow_url,
            workflow_conclusion=args.workflow_conclusion,
        )
    else:
        result = audit_prelaunch_checkpoint(args.root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
