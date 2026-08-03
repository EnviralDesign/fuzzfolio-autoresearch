"""Measure QD ledger duplicate scans without changing any persisted artifacts."""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import time

import autoresearch.temporal_qd_evolution as qd
from autoresearch.temporal_discovery_base import canonical_sha256


def _candidate(index: int) -> dict[str, str]:
    return {
        "candidateIdentitySha256": canonical_sha256({"candidate": index}),
        "programSha256": canonical_sha256({"program": index}),
        "sourceProfileSha256": canonical_sha256({"source": index}),
        "profileSnapshotSha256": canonical_sha256({"snapshot": index}),
        "canonicalEvidenceIdentitySha256": canonical_sha256({"evidence": index}),
    }


def _seconds(function) -> float:
    started = time.perf_counter()
    function()
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--duplicate-iterations", type=int, default=1000)
    parser.add_argument("--accepted-iterations", type=int, default=200)
    args = parser.parse_args()
    ledger = json.loads(args.ledger.read_text(encoding="utf-8"))
    # The newest record is the duplicate-heavy steady-state case: the legacy
    # scan must traverse the full ledger before finding each matching identity.
    duplicate = dict(ledger["records"][-1])

    scanned = deepcopy(ledger)
    scanned_seconds = _seconds(
        lambda: [qd._ledger_duplicate_check(scanned, duplicate) for _ in range(args.duplicate_iterations)]
    )
    indexed = deepcopy(ledger)
    index = qd._ledger_identity_index(indexed)
    indexed_seconds = _seconds(
        lambda: [
            qd._ledger_duplicate_check(indexed, duplicate, identity_index=index)
            for _ in range(args.duplicate_iterations)
        ]
    )

    base = len(ledger["records"])
    accepted_scanned = deepcopy(ledger)
    accepted_scanned_seconds = _seconds(
        lambda: [
            (
                qd._ledger_accept(accepted_scanned, candidate)
                if qd._ledger_duplicate_check(accepted_scanned, candidate)[0] is None
                else None
            )
            for candidate in (_candidate(base + offset) for offset in range(args.accepted_iterations))
        ]
    )
    accepted_indexed = deepcopy(ledger)
    accepted_index = qd._ledger_identity_index(accepted_indexed)
    accepted_indexed_seconds = _seconds(
        lambda: [
            (
                qd._ledger_accept(accepted_indexed, candidate, identity_index=accepted_index)
                if qd._ledger_duplicate_check(
                    accepted_indexed, candidate, identity_index=accepted_index
                )[0] is None
                else None
            )
            for candidate in (_candidate(base + offset) for offset in range(args.accepted_iterations))
        ]
    )
    if accepted_scanned != accepted_indexed:
        raise RuntimeError("indexed ledger diverged from record-scan ledger")
    result = {
        "ledgerRecordCount": base,
        "duplicateHeavy": {
            "iterations": args.duplicate_iterations,
            "recordScanSeconds": round(scanned_seconds, 6),
            "indexedSeconds": round(indexed_seconds, 6),
            "speedup": round(scanned_seconds / indexed_seconds, 3),
        },
        "acceptedHeavy": {
            "iterations": args.accepted_iterations,
            "recordScanSeconds": round(accepted_scanned_seconds, 6),
            "indexedSeconds": round(accepted_indexed_seconds, 6),
            "speedup": round(accepted_scanned_seconds / accepted_indexed_seconds, 3),
            "ledgerContentEqual": True,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
