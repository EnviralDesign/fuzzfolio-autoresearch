"""Prepare read-only live subsets for campaign-seal parity/benchmarks.

This is test tooling only. It reads the completed campaign and writes a new
isolated fixture directory; it never mutates campaign state.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from autoresearch.result_codec import canonical_json_bytes, semantic_sha256


def write(path: Path, value: object, *, newline: bool = False) -> None:
    path.write_bytes(canonical_json_bytes(value) + (b"\n" if newline else b""))


def prepare(generation: Path, count: int, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    screening = generation / "campaign" / "screening-run"
    index = json.loads((screening / "tail-result-index-v3.json").read_text("utf-8"))
    evaluation = json.loads((generation / "proposal" / "evaluation-population.json").read_text("utf-8"))
    manifest = json.loads((screening / "task-manifest.json").read_text("utf-8"))
    ordered_candidates = sorted(evaluation["candidates"], key=lambda row: row["candidateId"])
    candidates = ordered_candidates[:count]
    rejected_ids = {
        row["task"]["candidateId"] for row in index["entries"] if "rejection" in row
    }
    if rejected_ids and not any(row["candidateId"] in rejected_ids for row in candidates):
        rejected_candidate = next(
            row for row in ordered_candidates if row["candidateId"] in rejected_ids
        )
        candidates[-1] = rejected_candidate
        candidates.sort(key=lambda row: row["candidateId"])
    candidate_ids = {row["candidateId"] for row in candidates}
    entries = [row for row in index["entries"] if row["task"]["candidateId"] in candidate_ids]
    by_task = {row["task_id"]: row for row in manifest["tasks"]}

    evaluation["candidates"] = candidates
    evaluation["candidateCount"] = len(candidates)
    evaluation.pop("evaluationPopulationSha256")
    evaluation["evaluationPopulationSha256"] = semantic_sha256(evaluation)
    write(out / "evaluation-population.json", evaluation)

    expected = dict(index)
    expected["entries"] = entries
    expected["taskCount"] = len(entries)
    expected["sourceResultBlobBytes"] = sum(row["rawResultRef"]["blobSizeBytes"] for row in entries)
    expected.pop("tailResultIndexSha256")
    expected["tailResultIndexSha256"] = semantic_sha256(expected)
    write(out / "expected-tail-result-index-v3.json", expected)

    source_tasks = []
    for entry in entries:
        task_id = entry["task"]["taskId"]
        task = by_task[task_id]
        payload = task["payload"]
        raw_ref = entry["rawResultRef"]
        source_tasks.append({
            "task": entry["task"],
            "taskPayloadBinding": {
                "taskPayloadSha256": semantic_sha256(payload),
                "barLimit": payload["bar_limit"],
            },
            "rawResultPath": str((screening / raw_ref["relativePath"]).resolve()),
            "rawResultRef": raw_ref,
            "resultBinding": {
                "taskKind": task["task_kind"],
                "jobId": payload["job_id"],
                "authorityId": payload["authority_id"],
                "candidateId": payload["candidate_id"],
                "evidencePlanId": payload["evidence_plan"]["plan_id"],
                "lakeWindowSemanticSha256": payload["lake_window_semantic_sha256"],
                "sharedObservationStreamId": payload["shared_observation_stream_id"],
            },
        })
    source = {
        "schemaVersion": "temporal_qd_campaign_seal_source_v1",
        "authorityId": index["authorityId"],
        "authoritySha256": index["authoritySha256"],
        "taskMatrixSha256": index["taskMatrixSha256"],
        "taskManifestSha256": index["taskManifestSha256"],
        "taskManifestPath": str((screening / "task-manifest.json").resolve()),
        "checkpointSha256": index["checkpointSha256"],
        "taskCount": len(source_tasks),
        "funnelProjectionIncluded": index["funnelProjectionIncluded"],
        "tasks": source_tasks,
    }
    source["sourceSha256"] = semantic_sha256(source)
    write(out / "campaign-seal-source.json", source, newline=True)
    native = {
        "schemaVersion": "temporal_qd_campaign_seal_manifest_v1",
        "contractVersion": "temporal_qd_native_foundation_v1",
        "operation": "seal_completed_task_matrix_and_reduce_tail",
        "sourcePath": str((out / "campaign-seal-source.json").resolve()),
        "sourceSha256": source["sourceSha256"],
        "evaluationPopulationPath": str((out / "evaluation-population.json").resolve()),
        "evaluationPopulationSha256": evaluation["evaluationPopulationSha256"],
        "generationIndex": evaluation["generationIndex"],
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "provisionalLimit": 128,
        "resultPath": "generation-tail-transaction-result.json",
    }
    if len(sys.argv) > 4:
        native["runtimeAuthoritySha256"] = sys.argv[4]
    native["manifestSha256"] = semantic_sha256(native)
    write(out / "manifest.json", native, newline=True)


if __name__ == "__main__":
    prepare(Path(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3]))
