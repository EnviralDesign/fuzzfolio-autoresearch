"""Small semantic oracle for the native v5 funnel reduction seam.

The fixture contains the full construction attempt stream (including a
candidate-free pre-plan failure and an admitted duplicate), plus only the v4
compact funnel projection.  Python is used solely to produce expected bytes;
the Rust reducer never imports this helper or reopens raw results.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))

from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_generation_funnel import (
    DEFAULT_COMPLETENESS_POLICY,
    build_generation_funnel_artifact,
)


def sha(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def canonical(value: object, newline: bool = True) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + ("\n" if newline else "")).encode()


def self_hash(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def write(path: Path, value: object, newline: bool = True) -> None:
    path.write_bytes(canonical(value, newline))


def descriptor(path: Path, rows: list[dict]) -> dict:
    data = b"".join(canonical(row) for row in rows)
    path.write_bytes(data)
    return {
        "path": str(path.resolve()),
        "rawSha256": "sha256:" + hashlib.sha256(data).hexdigest(),
        "sizeBytes": len(data),
        "recordCount": len(rows),
    }


def main() -> None:
    root = Path(sys.argv[1]).resolve(); root.mkdir(parents=True, exist_ok=True)
    raw_a, raw_b = sha("raw-a"), sha("raw-b")
    stage_a = {
        "schemaVersion": "temporal_qd_proposal_funnel_stage_v1", "candidateId": "candidate-a", "rawSourceProfileSha256": raw_a,
        "staticReachability": {"outcome": "reachable", "reasons": []},
        "nativeValidation": {"outcome": "valid", "reasons": [], "resolvedProfileSha256": sha("profile-a"), "programSha256": sha("program-a"), "validationReportSha256": sha("report-a")},
        "admission": {"outcome": "admitted", "reasons": [], "canonicalEvidenceIdentitySha256": sha("executable-a")},
    }
    stage_b = {
        "schemaVersion": "temporal_qd_proposal_funnel_stage_v1", "candidateId": "candidate-b", "rawSourceProfileSha256": raw_b,
        "staticReachability": {"outcome": "reachable", "reasons": []},
        "nativeValidation": {"outcome": "valid", "reasons": [], "resolvedProfileSha256": sha("profile-b"), "programSha256": sha("program-b"), "validationReportSha256": sha("report-b")},
        "admission": {"outcome": "rejected_duplicate", "reasons": ["duplicate_executable"], "canonicalEvidenceIdentitySha256": sha("executable-b")},
    }
    rows = [
        {"schemaVersion": "temporal_qd_v5_proposal_funnel_entry_v1", "entrySha256": sha("attempt-0"), "proposalOrdinal": 0, "originKind": "random_immigrant", "disposition": "rejected"},
        {"schemaVersion": "temporal_qd_v5_proposal_funnel_entry_v1", "entrySha256": sha("attempt-1"), "proposalOrdinal": 1, "originKind": "random_immigrant", "disposition": "accepted", "candidate": {"candidateId": "candidate-a", "sourceProfileSha256": raw_a}, "proposal": {"candidateId": "candidate-a", "rawSourceProfileSha256": raw_a}, "funnelCandidate": stage_a, "acceptedCompactRecordSha256": sha("compact-a")},
        {"schemaVersion": "temporal_qd_v5_proposal_funnel_entry_v1", "entrySha256": sha("attempt-2"), "proposalOrdinal": 2, "originKind": "structural_offspring", "disposition": "rejected", "candidate": {"candidateId": "candidate-b", "sourceProfileSha256": raw_b}, "proposal": {"candidateId": "candidate-b", "rawSourceProfileSha256": raw_b}, "funnelCandidate": stage_b},
    ]
    attempts = descriptor(root / "attempts.jsonl", rows)
    authority = self_hash({"schemaVersion": "temporal_qd_v5_directional_tail_authority_v1", "generationIndex": 2, "runtimeAuthoritySha256": sha("runtime"), "tailResultIndexSchema": "temporal_qd_tail_result_index_v4", "tailResultEntrySchema": "temporal_qd_tail_result_index_entry_v4", "rawRotatingProvenanceSchema": "temporal_qd_v5_raw_rotating_provenance_v1"}, "tailAuthoritySha256")

    def index_entry(task_id: str, start: str, end: str, result: str, net: float) -> dict:
        raw = {"schemaVersion": "temporal_qd_tail_raw_result_ref_v1", "relativePath": f"results/{task_id}.json.gz", "codec": "gzip-json-v1", "resultSha256": sha(result), "semanticSizeBytes": 1, "uncompressedSha256": sha("uncompressed-" + task_id), "uncompressedSizeBytes": 1, "blobSha256": sha("blob-" + task_id), "blobSizeBytes": 1}
        task = {"taskId": task_id, "candidateId": "candidate-a", "analysisWindowStart": start, "analysisWindowEnd": end, "evidencePlanSemanticSha256": sha("plan-" + task_id), "taskPayloadSha256": sha("payload-" + task_id)}
        entry = {"schemaVersion": "temporal_qd_tail_result_index_entry_v4", "task": task, "rawResultRef": raw, "rawTaskProvenance": {"taskId": task_id, "resultSha256": raw["resultSha256"]}, "stageProjection": {"schemaVersion": "temporal_qd_tail_stage_projection_v1", "codec": "gzip-canonical-json-v1", "semanticSha256": sha("stage-" + task_id), "semanticSizeBytes": 1, "blobBase64": "eA=="}, "rotatingEvidenceMetrics": {"conservativeNetR": net, "noCostNetR": net, "maxDrawdownR": 1.0, "closedTrades": 4, "observations": 10, "v3Admissible": True, "resolvedProgramSha256": sha("program-a"), "resolvedProfileSnapshotSha256": sha("profile-a"), "sourceProfileSnapshotSha256": raw_a}, "funnelProjection": {"resultBehavior": {"windowId": f"{start}/{end}", "resultSha256": raw["resultSha256"], "activationCount": 1, "acceptedIntentOrEffectCount": 1, "rejectedIntentOrEffectCount": 0, "canceledIntentOrEffectCount": 0, "positionChangeCount": 1, "tradeCloseCount": 4, "neverActivated": False}, "terminalAdjustedConservativeNetR": net, "terminalAdjustedMaxDrawdownR": 1.0}, "rawRotatingProvenance": {"schemaVersion": "temporal_qd_v5_raw_rotating_provenance_v1", "taskId": task_id, "resultSha256": raw["resultSha256"], "observationStreamSha256": sha("observations-" + task_id), "conservativeReplayStreamSha256": sha("replay-" + task_id), "realizedBehaviorSha256": sha("behavior-" + task_id)}}
        return self_hash(entry, "entrySha256")

    entries = [index_entry("task-a", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", "result-a", 2.0), index_entry("task-b", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z", "result-b", 1.0)]
    index = self_hash({"schemaVersion": "temporal_qd_tail_result_index_v4", "authorityId": sha("authority"), "authoritySha256": sha("authority-body"), "taskMatrixSha256": sha("matrix"), "taskManifestSha256": sha("manifest"), "checkpointSha256": sha("checkpoint"), "taskCount": 2, "funnelProjectionIncluded": True, "sourceResultBlobBytes": 2, "entries": entries}, "tailResultIndexSha256")
    index_path = root / "tail-index.json"; write(index_path, index, newline=False)
    index_bytes = index_path.read_bytes()
    index_descriptor = {"path": str(index_path.resolve()), "rawSha256": "sha256:" + hashlib.sha256(index_bytes).hexdigest(), "sizeBytes": len(index_bytes)}
    campaign_seal = self_hash({"schemaVersion": "temporal_qd_campaign_seal_v1", "contractVersion": "temporal_qd_native_foundation_v1", "manifestSha256": sha("seal-manifest"), "sourceSha256": sha("seal-source"), "authorityId": sha("authority"), "authoritySha256": sha("authority-body"), "taskMatrixSha256": sha("matrix"), "taskManifestSha256": sha("manifest"), "checkpointSha256": sha("checkpoint"), "taskCount": 2, "rawResultReadCount": 2, "sourceResultBlobBytes": 2, "sourceResultUncompressedBytes": 2, "sourceResultSemanticBytes": 2, "tailResultIndex": {"path": "tail-result-index-v4.json", "sha256": index["tailResultIndexSha256"]}, "rawResultInventory": {"path": "raw-result-inventory.json", "sha256": sha("inventory"), "bytes": 2}}, "campaignSealSha256")
    accounting = {"proposalAttemptCount": 3, "dispositionCounts": {"accepted": 1, "rejected": 2}, "originProposalCounts": {"random_immigrant": 2, "structural_offspring": 1}}
    panel = {"panelId": "panel-2", "windows": [{"windowId": "w-1", "analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2024-02-01T00:00:00Z"}, {"windowId": "w-2", "analysisWindowStart": "2024-02-01T00:00:00Z", "analysisWindowEnd": "2024-03-01T00:00:00Z"}]}
    attempt_receipt = {
        "schemaVersion": "temporal_qd_v5_evolved_attempt_stream_receipt_v1",
        "inputSha256": sha("evolved-input"), "proposalResultSha256": sha("evolved-result"),
        "proposalReceiptSha256": sha("evolved-receipt"), "outputInventorySha256": sha("evolved-inventory"),
        "fragmentBundleSha256": sha("evolved-fragments"), "evaluationPopulationSha256": sha("evolved-population"),
        "attemptStream": {**attempts, "rowSchema": "temporal_qd_v5_proposal_funnel_entry_v1"},
        "proposalAccounting": accounting,
    }
    self_hash(attempt_receipt, "receiptSha256")
    attempt_receipt_path = root / "evolved-attempt-receipt.json"; write(attempt_receipt_path, attempt_receipt)
    attempt_receipt_raw = attempt_receipt_path.read_bytes()
    proposal_attempt_authority = {"kind": "evolved", "receiptPath": str(attempt_receipt_path.resolve()), "receiptFileSha256": "sha256:" + hashlib.sha256(attempt_receipt_raw).hexdigest(), "receiptSizeBytes": len(attempt_receipt_raw), "receiptSha256": attempt_receipt["receiptSha256"]}
    input_value = {"schemaVersion": "temporal_qd_v5_native_funnel_reduction_input_v2", "contractVersion": "temporal_qd_native_foundation_v1", "generationIndex": 2, "proposalAttemptAuthority": proposal_attempt_authority, "evaluationPanel": panel, "tailAuthority": authority, "campaignSeal": campaign_seal, "tailResultIndex": index_descriptor, "minimumTotalTrades": 8, "minimumTradesPerWindow": 4}
    self_hash(input_value, "inputSha256")
    write(root / "input.json", input_value)

    funnel_bytes = b",".join(canonical(row, newline=False) for row in rows)
    def fragment(kind: str, count: int, payload: bytes) -> dict:
        return {"kind": kind, "fragmentSha256": "sha256:" + hashlib.sha256(payload).hexdigest(), "encodedBytes": len(payload), "rowCount": count}
    fragments = {
        "schemaVersion": "temporal_qd_v5_evolved_publication_fragments_v2",
        "acceptedCandidateCount": 1, "proposalAttemptCount": 3,
        "populationCandidates": fragment("populationCandidates", 1, b"p"),
        "evaluationCandidates": fragment("evaluationCandidates", 1, b"e"),
        "evaluationFunnelEntries": fragment("evaluationFunnelEntries", 3, funnel_bytes),
        "generationJournalBindings": fragment("generationJournalBindings", 1, b"j"),
    }
    self_hash(fragments, "fragmentBundleSha256")
    public_evaluation = {"schemaVersion": "temporal_qd_evaluation_population_v1", "generationIndex": 2, "candidateCount": 1, "proposalAttempts": 3, "funnelEntries": rows}
    public_evaluation_path = root / "evaluation-population.json"; write(public_evaluation_path, public_evaluation)
    public_bytes = public_evaluation_path.read_bytes()
    adapter = {"schemaVersion": "temporal_qd_v5_core_funnel_receipt_adapter_input_v1", "contractVersion": "temporal_qd_native_foundation_v1", "coreFragments": fragments, "evaluationPopulation": {"path": str(public_evaluation_path.resolve()), "rawSha256": "sha256:" + hashlib.sha256(public_bytes).hexdigest(), "sizeBytes": len(public_bytes)}}
    self_hash(adapter, "inputSha256")
    write(root / "core-adapter-input.json", adapter)

    base_a = {"candidateId": "candidate-a", "rawSourceProfileSha256": raw_a}
    base_b = {"candidateId": "candidate-b", "rawSourceProfileSha256": raw_b}
    results = [dict(base_a, canonicalEvidenceIdentitySha256=sha("executable-a"), windowId="w-1", resultSha256=sha("result-a")), dict(base_a, canonicalEvidenceIdentitySha256=sha("executable-a"), windowId="w-2", resultSha256=sha("result-b"))]
    artifact = build_generation_funnel_artifact(
        proposal_attempt_ledger=[{"attemptIdentitySha256": r["entrySha256"], "proposalOrdinal": r["proposalOrdinal"], "originKind": r["originKind"], "disposition": r["disposition"], **({"candidateId": r["candidate"]["candidateId"], "rawSourceProfileSha256": r["candidate"]["sourceProfileSha256"]} if "candidate" in r else {})} for r in rows],
        proposal_journal=[base_a, base_b], static_reachability_records=[dict(base_a, outcome="reachable"), dict(base_b, outcome="reachable")], native_validation_records=[dict(base_a, outcome="valid", resolvedProfileSha256=sha("profile-a"), programSha256=sha("program-a"), validationReportSha256=sha("report-a")), dict(base_b, outcome="valid", resolvedProfileSha256=sha("profile-b"), programSha256=sha("program-b"), validationReportSha256=sha("report-b"))], admission_records=[dict(base_a, outcome="admitted", canonicalEvidenceIdentitySha256=sha("executable-a")), dict(base_b, outcome="rejected_duplicate", canonicalEvidenceIdentitySha256=sha("executable-b"), reasons=["duplicate_executable"])], evaluation_plans=[dict(base_a, canonicalEvidenceIdentitySha256=sha("executable-a"), outcome="evaluated", expectedWindowIds=["w-1", "w-2"])], evaluation_results=results, activation_quality_records=[dict(base_a, canonicalEvidenceIdentitySha256=sha("executable-a"), outcome="recorded", qualityDisposition="eligible")], archive_retention_records=[dict(base_a, canonicalEvidenceIdentitySha256=sha("executable-a"), outcome="not_retained")], proposal_accounting=accounting, completeness_policy=DEFAULT_COMPLETENESS_POLICY,
    )
    expected = {"schemaVersion": "temporal_qd_native_funnel_reduction_source_v1", "preArchiveProjection": True, "completenessPolicy": artifact["completenessPolicy"], "proposalAccounting": artifact["proposalAccounting"], "proposalAttempts": artifact["attemptLedger"]["attempts"], "candidateStageRows": artifact["candidates"]}
    expected["proposalAccounting"] = accounting
    self_hash(expected, "funnelSourceSha256")
    write(root / "expected.json", expected)


if __name__ == "__main__":
    main()
