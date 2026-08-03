"""Verify cold and warm QD immigrant continuation equivalence in one process."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from autoresearch.temporal_generator_v2_continuation import ExactGeneratorV2Continuation


def _expected_hashes(root: Path) -> dict[int, str]:
    values: dict[int, str] = {}
    for path in sorted((root / "proposal-journal").glob("*.json")):
        entry = json.loads(path.read_text(encoding="utf-8"))
        immigrant = ((entry.get("proposal") or {}).get("immigrantProposal"))
        if isinstance(immigrant, dict):
            values[int(immigrant["continuationOrdinal"])] = str(
                immigrant["immigrantProposalSha256"]
            )
    return values


def _build(args: argparse.Namespace, ordinal: int) -> ExactGeneratorV2Continuation:
    return ExactGeneratorV2Continuation(
        source_preparation_path=args.source_preparation,
        base_generator_root=args.base_generator_root,
        confirmed_entry_admission_root=args.confirmed_entry_admission_root,
        start_continuation_ordinal=ordinal,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-preparation", required=True, type=Path)
    parser.add_argument("--base-generator-root", required=True, type=Path)
    parser.add_argument("--confirmed-entry-admission-root", required=True, type=Path)
    parser.add_argument("--expected-proposal-root", required=True, type=Path)
    parser.add_argument("--start-continuation-ordinal", required=True, type=int)
    parser.add_argument("--warm-sample-proposals", type=int, default=512)
    args = parser.parse_args()
    if args.warm_sample_proposals < 2:
        raise ValueError("warm sample must contain at least two proposals")
    expected = _expected_hashes(args.expected_proposal_root)

    cold_started = time.perf_counter()
    cold = _build(args, args.start_continuation_ordinal)
    cold_seconds = time.perf_counter() - cold_started
    first = cold.next_proposal()

    sample_started = time.perf_counter()
    for _ in range(args.warm_sample_proposals - 1):
        cold.next_proposal()
    warm_sample_seconds = time.perf_counter() - sample_started

    warm_started = time.perf_counter()
    warm = _build(args, cold.next_continuation_ordinal)
    warm_seconds = time.perf_counter() - warm_started
    second = warm.next_proposal()

    for proposal in (first, second):
        ordinal = int(proposal["continuationOrdinal"])
        if ordinal in expected and expected[ordinal] != proposal["immigrantProposalSha256"]:
            raise RuntimeError(f"cold/warm proposal diverged from admitted artifact at {ordinal}")
    print(
        json.dumps(
            {
                "coldConstructionSeconds": round(cold_seconds, 6),
                "warmConstructionSeconds": round(warm_seconds, 6),
                "speedup": round(cold_seconds / warm_seconds, 3),
                "warmSampleProposalCount": args.warm_sample_proposals,
                "warmSampleSeconds": round(warm_sample_seconds, 6),
                "warmSampleProposalsPerSecond": round(
                    (args.warm_sample_proposals - 1) / warm_sample_seconds, 3
                ),
                "sourceIdentitySha256": cold.source_identity["sourceIdentitySha256"],
                "verifiedContinuationOrdinals": [
                    first["continuationOrdinal"],
                    second["continuationOrdinal"],
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
