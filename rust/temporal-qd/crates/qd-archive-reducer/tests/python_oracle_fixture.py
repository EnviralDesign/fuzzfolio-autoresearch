"""Build authenticated tail inputs and Python archive-selection expectations.

This fixture deliberately uses the production `select_qd_archive` oracle. The
native boundary starts after `build_qd_archive` has materialized equivalent
evaluated members, so the fixture freezes that exact reducer seam rather than
replaying tasks/results in a Rust test.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

from autoresearch.temporal_qd_evolution import select_qd_archive


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def identity(value: dict, field: str) -> str:
    clone = dict(value)
    clone.pop(field, None)
    return "sha256:" + hashlib.sha256(canonical(clone)[:-1]).hexdigest()


def member(index: int, *, duplicate: bool = False, support: bool = True, negative: bool = False) -> dict:
    program = 1 if duplicate else index + 10
    worst = -1.0 if negative else index / 100.0 + 0.1
    return {
        "candidateId": f"candidate{index:03}",
        "generationIndex": 7,
        "candidate": {"programSha256": f"sha256:{program:064x}"},
        "aggregate": {
            "resolvedProgramSha256": f"sha256:{program:064x}",
            "authoredProgramSha256": f"sha256:{program:064x}",
        },
        "descriptor": {"cellId": "one|root|one|none|small|moderate|medium"},
        "objectives": {
            "worstWindowConservativeNetR": worst,
            "maximumDrawdownR": float(128 - index),
            "structuralComplexity": float(index),
        },
        "finiteDataValidity": {
            "isFiniteData": True,
            "passesSupportGate": support,
            "validForQuality": support,
        },
        "cappedTradeSupport": 20.0,
    }


def policy() -> dict:
    return {
        "schemaVersion": "temporal_qd_policy_v4",
        "archive": {"defaultCellCapacity": 4, "lanes": {}, "negativeNoveltyMaxMembersPerCell": 1},
        "parentSelection": {},
        "resolvedExecutionDeduplication": {
            "identity": "aggregate.resolvedProgramSha256",
            "representativeOrdering": [],
            "required": True,
            "stage": "before_archive_reduction",
        },
        "tradeSupport": {"capTrades": 20},
    }


def write_case(root: Path, name: str, rows: list[dict]) -> None:
    case = root / name
    case.mkdir()
    members_path = case / "evaluated-members.jsonl"
    raw = b"".join(canonical(row) for row in rows)
    members_path.write_bytes(raw)
    population = "sha256:" + "a" * 64
    result_set = "sha256:" + "b" * 64
    tail = {
        "schemaVersion": "temporal_qd_native_tail_reduction_result_v1",
        "populationSha256": population,
        "resultSetSha256": result_set,
        "evaluatedMembers": {
            "schemaVersion": "temporal_qd_evaluated_members_v1",
            "evaluationRejectedCandidates": [
                {"candidateId": "structural", "disposition": "rejected", "reasonCode": "duplicate_break_even_execution_invariant", "structuralProvenance": {"modules": []}},
                {"candidateId": "warmup", "disposition": "rejected", "reasonCode": "insufficient_aligned_history", "windowRejections": []},
            ],
            "membersFile": {"rawSha256": digest(raw), "recordCount": len(rows)},
        },
    }
    tail["resultSha256"] = identity(tail, "resultSha256")
    tail_path = case / "tail-reduction-result.json"
    tail_path.write_bytes(canonical(tail))
    frozen = policy()
    policy_sha = "sha256:" + hashlib.sha256(canonical(frozen)[:-1]).hexdigest()
    manifest = {
        "schemaVersion": "temporal_qd_native_archive_reduction_manifest_v1",
        "contractVersion": "temporal_qd_native_foundation_v1",
        "operation": "reduce_evidence_ladder_archive",
        "tailReductionResultPath": str(tail_path),
        "tailReductionResultSha256": digest(canonical(tail)),
        "evaluatedMembersPath": str(members_path),
        "evaluatedMembersSha256": digest(raw),
        "populationSha256": population,
        "resultSetSha256": result_set,
        "generationIndex": 7,
        "cellCapacity": 4,
        "archivePolicy": {"policyName": "python-oracle-policy", "policySha256": policy_sha, "frozenPolicy": frozen},
        "directionAware": False,
        "resultPath": "archive-reduction-result.json",
        "archivePath": "archive.json",
    }
    manifest["manifestSha256"] = identity(manifest, "manifestSha256")
    (case / "manifest.json").write_bytes(canonical(manifest))

    # This is the exact Python selection output at the native reducer seam.
    # Duplicate execution removal is intentionally performed before this call,
    # exactly as build_qd_archive does.
    winners: dict[str, dict] = {}
    for row in rows:
        resolved = row["aggregate"]["resolvedProgramSha256"]
        current = winners.get(resolved)
        if current is None or (row["objectives"]["worstWindowConservativeNetR"], row["candidateId"]) > (current["objectives"]["worstWindowConservativeNetR"], current["candidateId"]):
            winners[resolved] = row
    expected = select_qd_archive(copy.deepcopy(list(winners.values())), cell_capacity=4)
    # build_qd_archive applies these zero-count generation-accounting fields
    # after calling select_qd_archive.
    for cell in expected:
        cell["selectionVisitCount"] = 0
        cell["offspringAttemptCount"] = 0
    (case / "python-select-expected.json").write_bytes(canonical(expected))


def main() -> None:
    root = Path(sys.argv[1])
    small = [member(0, duplicate=True), member(1, duplicate=True), member(2), member(3, negative=True), member(4, support=False)]
    write_case(root, "small", small)
    large = [member(index) for index in range(128)]
    write_case(root, "large", large)


if __name__ == "__main__":
    main()
