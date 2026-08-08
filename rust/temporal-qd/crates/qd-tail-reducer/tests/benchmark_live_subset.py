"""Prepare and benchmark a read-only live-input subset against Python.

Usage:
  python benchmark_live_subset.py prepare EVAL INDEX COUNT OUT
  python benchmark_live_subset.py oracle OUT
  python benchmark_live_subset.py compare OUT

Preparation reads the live artifacts but writes only below OUT. Timed oracle
work reads the prepared compact subset, not the live campaign.
"""

from __future__ import annotations

import json
import sys
import time
import tracemalloc
from pathlib import Path

from autoresearch.result_codec import canonical_json_bytes, semantic_sha256
from autoresearch.temporal_discovery_results import (
    _aggregate_candidate,
    _require_candidate_execution_binding,
    _result_set_sha256,
)
from autoresearch.temporal_qd_evolution import (
    _duplicate_break_even_modules,
    _finite_data_validity,
    _objective_row,
    qd_behavior_descriptor,
)
from autoresearch.temporal_qd_rotating_evidence import reduce_provisional_diverse_survivors
from autoresearch.temporal_qd_tail_result_index import (
    load_indexed_stage_results,
    validate_tail_result_index,
)


def peak_working_set_bytes() -> int | None:
    if sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    get_current_process = ctypes.windll.kernel32.GetCurrentProcess
    get_current_process.restype = wintypes.HANDLE
    get_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
    get_memory_info.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    get_memory_info.restype = wintypes.BOOL
    ok = get_memory_info(
        get_current_process(),
        ctypes.byref(counters),
        counters.cb,
    )
    return int(counters.PeakWorkingSetSize) if ok else None


def write(path: Path, value: object, *, newline: bool = False) -> None:
    payload = canonical_json_bytes(value) + (b"\n" if newline else b"")
    path.write_bytes(payload)


def prepare(evaluation_path: Path, index_path: Path, count: int, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    index = json.loads(index_path.read_text(encoding="utf-8"))
    candidates = sorted(evaluation["candidates"], key=lambda row: row["candidateId"])[:count]
    candidate_ids = {row["candidateId"] for row in candidates}
    entries = [row for row in index["entries"] if row["task"]["candidateId"] in candidate_ids]
    evaluation["candidates"] = candidates
    evaluation["candidateCount"] = len(candidates)
    evaluation.pop("evaluationPopulationSha256")
    evaluation["evaluationPopulationSha256"] = semantic_sha256(evaluation)
    index["entries"] = entries
    index["taskCount"] = len(entries)
    index["sourceResultBlobBytes"] = sum(row["rawResultRef"]["blobSizeBytes"] for row in entries)
    index.pop("tailResultIndexSha256")
    index["tailResultIndexSha256"] = semantic_sha256(index)
    write(out / "evaluation-population.json", evaluation)
    write(out / "tail-result-index-v3.json", index)
    manifest = {
        "schemaVersion": "temporal_qd_native_tail_reduction_manifest_v1",
        "contractVersion": "temporal_qd_native_foundation_v1",
        "operation": "reduce_evaluated_members_and_provisional",
        "evaluationPopulationPath": str((out / "evaluation-population.json").resolve()),
        "evaluationPopulationSha256": evaluation["evaluationPopulationSha256"],
        "tailResultIndexPath": str((out / "tail-result-index-v3.json").resolve()),
        "tailResultIndexSha256": index["tailResultIndexSha256"],
        "generationIndex": evaluation["generationIndex"],
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "provisionalLimit": 128,
        "resultPath": "tail-reduction-result.json",
    }
    manifest["manifestSha256"] = semantic_sha256(manifest)
    write(out / "manifest.json", manifest, newline=True)


def oracle(out: Path) -> None:
    tracemalloc.start()
    started = time.perf_counter()
    evaluation = json.loads((out / "evaluation-population.json").read_text(encoding="utf-8"))
    index = validate_tail_result_index(json.loads((out / "tail-result-index-v3.json").read_text(encoding="utf-8")))
    grouped = load_indexed_stage_results(index)
    candidates = {row["candidateId"]: row for row in evaluation["candidates"]}
    assert set(grouped) == set(candidates)
    members = []
    rejected = []
    for candidate_id in sorted(candidates):
        candidate = candidates[candidate_id]
        windows = grouped[candidate_id]
        violations = _duplicate_break_even_modules(candidate)
        if violations:
            rejected.append({
                "candidateId": candidate_id,
                "disposition": "rejected",
                "reasonCode": "duplicate_break_even_execution_invariant",
                "structuralProvenance": {"modules": violations},
            })
            continue
        window_rejections = [row for row in windows if row.get("evaluationRejected") is True]
        if window_rejections:
            rejected.append({
                "candidateId": candidate_id,
                "disposition": "rejected",
                "reasonCode": str((window_rejections[0]["rejection"] or {}).get("reason_code")),
                "windowRejections": [{"windowId": row["windowId"], "rejection": row["rejection"]} for row in window_rejections],
            })
            continue
        _require_candidate_execution_binding(candidate, windows)
        aggregate = _aggregate_candidate(candidate, windows)
        members.append({
            "candidateId": candidate_id,
            "generationIndex": evaluation["generationIndex"],
            "candidate": candidate,
            "aggregate": aggregate,
            "descriptor": qd_behavior_descriptor(candidate, aggregate),
            "objectives": _objective_row(candidate, aggregate),
            "finiteDataValidity": _finite_data_validity(aggregate, minimum_total_trades=8, minimum_trades_per_window=4, cap_trades=20),
            "cappedTradeSupport": float(min(max(0, int(aggregate.get("totalTrades") or 0)), 20)),
        })
    counts = {}
    for member in members:
        cell = member["descriptor"]["cellId"]
        counts[cell] = counts.get(cell, 0) + 1
    provisional_input = [{
        "candidateId": member["candidateId"],
        "candidateIdentitySha256": member["candidate"].get("candidateIdentitySha256"),
        "programSha256": member["candidate"].get("programSha256"),
        "profileSnapshotSha256": member["candidate"].get("profileSnapshotSha256"),
        "cellId": member["descriptor"]["cellId"],
        "costView": "research_conservative",
        "currentPanelRank": float(member["aggregate"].get("totalConservativeNetR") or 0.0),
        "novelty": 1.0 / float(counts[member["descriptor"]["cellId"]]),
    } for member in members]
    value = {
        "members": members,
        "evaluationRejectedCandidates": rejected,
        "resultSetSha256": _result_set_sha256(grouped),
        "provisional": reduce_provisional_diverse_survivors(provisional_input, limit=128),
    }
    write(out / "python-oracle.json", value)
    elapsed = time.perf_counter() - started
    _current, peak = tracemalloc.get_traced_memory()
    print(json.dumps({
        "elapsedSeconds": elapsed,
        "tracemallocPeakBytes": peak,
        "peakWorkingSetBytes": peak_working_set_bytes(),
        "memberCount": len(members),
    }, sort_keys=True))


def compare(out: Path) -> None:
    expected = json.loads((out / "python-oracle.json").read_text(encoding="utf-8"))
    result = json.loads((out / "tail-reduction-result.json").read_text(encoding="utf-8"))
    members = [json.loads(line) for line in (out / "evaluated-members.jsonl").read_text(encoding="utf-8").splitlines()]
    assert members == expected["members"]
    assert result["evaluatedMembers"]["evaluationRejectedCandidates"] == expected["evaluationRejectedCandidates"]
    assert result["resultSetSha256"] == expected["resultSetSha256"]
    assert result["provisional"]["candidates"] == expected["provisional"]
    print(json.dumps({"parity": True, "memberCount": len(members)}, sort_keys=True))


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "prepare":
        prepare(Path(sys.argv[2]), Path(sys.argv[3]), int(sys.argv[4]), Path(sys.argv[5]))
    elif command == "oracle":
        oracle(Path(sys.argv[2]))
    elif command == "compare":
        compare(Path(sys.argv[2]))
    else:
        raise SystemExit(f"unknown command: {command}")
