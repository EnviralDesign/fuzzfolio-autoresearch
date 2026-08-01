from __future__ import annotations

import argparse
import json

from .temporal_search_stage5e2_checkpoint import (
    audit_stage5e2_checkpoint,
    freeze_stage5e2_checkpoint,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--root", required=True)
    freeze.add_argument("--autoresearch-commit", required=True)
    freeze.add_argument("--fuzzfolio-commit", required=True)
    freeze.add_argument("--worker-contract-sha256", required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--root", required=True)
    args = parser.parse_args()
    if args.command == "freeze":
        result = freeze_stage5e2_checkpoint(
            root=args.root,
            autoresearch_commit=args.autoresearch_commit,
            fuzzfolio_commit=args.fuzzfolio_commit,
            worker_contract_sha256=args.worker_contract_sha256,
        )
    else:
        result = audit_stage5e2_checkpoint(args.root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
