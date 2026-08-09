"""Materialize the opt-in immutable proposal-lineage sidecar for one generation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_proposal_lineage_artifact import (
    DEFAULT_OUTPUT_RELATIVE_PATH,
    DEFAULT_SOURCE_RELATIVE_PATH,
    _read_json_object,
    build_proposal_lineage_artifact,
    materialize_completed_generation_lineage,
    verify_source_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation-dir", required=True, type=Path)
    parser.add_argument("--source-relative-path", type=Path, default=DEFAULT_SOURCE_RELATIVE_PATH)
    parser.add_argument("--output-relative-path", type=Path, default=DEFAULT_OUTPUT_RELATIVE_PATH)
    parser.add_argument("--dry-run", action="store_true", help="validate and build only; write no files")
    parser.add_argument("--require-source", action="store_true", help="fail instead of treating a legacy run as read-only")
    args = parser.parse_args(argv)
    if args.dry_run:
        source_path = args.generation_dir / args.source_relative_path
        if not source_path.is_file():
            if args.require_source:
                raise TemporalDiscoveryContractError("proposal lineage source is absent")
            print("legacy/read-only generation: no proposal-lineage source; no files written")
            return 0
        source = _read_json_object(source_path, name="proposal lineage source")
        verify_source_artifacts(source, generation_root=args.generation_dir)
        artifact = build_proposal_lineage_artifact(source)
    else:
        artifact = materialize_completed_generation_lineage(
            args.generation_dir,
            source_relative_path=args.source_relative_path,
            output_relative_path=args.output_relative_path,
        )
        if artifact is None:
            if args.require_source:
                raise TemporalDiscoveryContractError("proposal lineage source is absent")
            print("legacy/read-only generation: no proposal-lineage source; no files written")
            return 0
    print(
        "materialized proposal-lineage sidecar "
        f"source={artifact['manifest']['sourceSha256']} "
        f"report={artifact['manifest']['reportSha256']} "
        f"entries={artifact['manifest']['entryCount']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TemporalDiscoveryContractError as exc:
        print(f"proposal-lineage materialization rejected: {exc}", file=sys.stderr)
        raise SystemExit(2)
