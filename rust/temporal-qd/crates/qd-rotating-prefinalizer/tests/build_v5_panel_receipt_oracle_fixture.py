"""Small byte oracle for the native v4-to-panel receipt producer.

The expected document is assembled with the established Python evidence
builders, while the input index and members are fully sealed local artifacts.
"""
from __future__ import annotations

import hashlib
import base64
import gzip
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))

from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_rotating_evidence import (
    build_candidate_panel_bundle,
    build_candidate_window_evidence,
    build_rotating_evidence_contract,
)


def dump(value: object, *, lf: bool = True) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + ("\n" if lf else "")).encode()


def write(path: Path, value: object, *, lf: bool = True) -> None:
    path.write_bytes(dump(value, lf=lf))


def digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def self_hash(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def file_descriptor(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "rawSha256": "sha256:" + hashlib.sha256(data).hexdigest(), "sizeBytes": len(data)}


def main() -> None:
    root = Path(sys.argv[1]).resolve(); root.mkdir(parents=True, exist_ok=True)
    contract = build_rotating_evidence_contract({
        "schemaVersion": "temporal_qd_rotating_evidence_input_v1",
        "developmentYears": [{"analysisWindowStart": f"202{year}-01-01T00:00:00Z", "analysisWindowEnd": f"202{year + 1}-01-01T00:00:00Z"} for year in range(1, 5)],
        "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"},
        "provisionalSurvivorCount": 2, "breederWidth": 2,
    })
    panel = contract["panels"][0]
    authority = self_hash({
        "schemaVersion": "temporal_qd_v5_directional_tail_authority_v1", "generationIndex": 1,
        "runtimeAuthoritySha256": digest("r"), "tailResultIndexSchema": "temporal_qd_tail_result_index_v4",
        "tailResultEntrySchema": "temporal_qd_tail_result_index_entry_v4",
        "rawRotatingProvenanceSchema": "temporal_qd_v5_raw_rotating_provenance_v1",
    }, "tailAuthoritySha256")
    candidate_count = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    candidate_markers = (("candidate-a", "a"), ("candidate-b", "b")) if candidate_count == 2 else tuple((f"candidate-{ordinal:03d}", f"member-{ordinal:03d}") for ordinal in range(candidate_count))
    candidates = []
    entries = []
    for candidate_id, marker in candidate_markers:
        candidate = {"candidateId": candidate_id, "candidateIdentitySha256": digest(marker), "programSha256": digest("x" + marker), "profileSnapshotSha256": digest("p" + marker)}
        candidates.append(candidate)
        for window in panel["windows"]:
            task = {"taskId": f"{candidate_id}-{window['windowId']}", "candidateId": candidate_id, "analysisWindowStart": window["analysisWindowStart"], "analysisWindowEnd": window["analysisWindowEnd"], "evidencePlanSemanticSha256": digest("e" + marker), "taskPayloadSha256": digest("t" + marker)}
            raw = {"schemaVersion": "temporal_qd_tail_raw_result_ref_v1", "relativePath": "opaque", "codec": "gzip-json-v1", "resultSha256": digest("z" + marker), "semanticSizeBytes": 1, "uncompressedSha256": digest("u" + marker), "uncompressedSizeBytes": 1, "blobSha256": digest("g" + marker), "blobSizeBytes": 1}
            metrics = {"conservativeNetR": 1.25, "noCostNetR": 1.5, "maxDrawdownR": -0.5, "closedTrades": 3, "observations": 5, "v3Admissible": True, "resolvedProgramSha256": candidate["programSha256"], "resolvedProfileSnapshotSha256": digest("q" + marker), "sourceProfileSnapshotSha256": candidate["profileSnapshotSha256"]}
            zero_side = {"closedTrades": 0, "wins": 0, "losses": 0, "flatTrades": 0, "grossR": 0.0, "netR": 0.0, "costR": 0.0, "holdingBars": 0, "holdingHours": 0.0, "activeWindowCount": 0, "closeReasonCounts": {}, "actionCounts": {}, "transitionCounts": {}, "terminalStatusCounts": {}, "terminalDirectionCount": 0, "conflictAbstentions": 0, "tradeSequence": [], "active": False, "activeWindowFraction": 0.0, "exposureProxy": 0.0, "averageHoldingBars": 0.0, "closeReasonDistribution": {}, "actionDistribution": {}, "transitionDistribution": {}}
            long_side = {**zero_side, "closedTrades": 3, "wins": 3, "grossR": 1.5, "netR": 1.25, "costR": 0.25, "holdingBars": 6, "holdingHours": 6.0, "activeWindowCount": 1, "active": True, "activeWindowFraction": 1.0, "exposureProxy": 1.2, "averageHoldingBars": 2.0}
            realized = {"schemaVersion": "temporal_realized_behavior_v1", "windowId": window["windowId"], "reportedClosedTrades": 3, "materializedClosedTrades": 3, "unattributedClosedTrades": 0, "observations": 5, "terminal": {}, "conflictAbstentions": 0, "unattributedConflictAbstentions": 0, "sides": {"long": long_side, "short": zero_side}}
            stage = {**metrics, "trades": metrics["closedTrades"], "realizedBehavior": realized}
            stage.pop("closedTrades")
            stage_raw = dump(stage, lf=False)
            stage_blob = gzip.compress(stage_raw, mtime=0)
            stage_projection = {"schemaVersion": "temporal_qd_tail_stage_projection_v1", "codec": "gzip-canonical-json-v1", "semanticSha256": "sha256:" + hashlib.sha256(stage_raw).hexdigest(), "semanticSizeBytes": len(stage_raw), "blobBase64": base64.b64encode(stage_blob).decode()}
            raw_provenance = {"schemaVersion": "temporal_qd_v5_raw_rotating_provenance_v1", "taskId": task["taskId"], "resultSha256": raw["resultSha256"], "observationStreamSha256": digest("o" + marker), "conservativeReplayStreamSha256": digest("c" + marker), "realizedBehaviorSha256": canonical_sha256(realized)}
            entry = {"schemaVersion": "temporal_qd_tail_result_index_entry_v4", "task": task, "rawResultRef": raw, "rawTaskProvenance": {"taskId": task["taskId"], "resultSha256": raw["resultSha256"]}, "stageProjection": stage_projection, "rotatingEvidenceMetrics": metrics, "funnelProjection": {"opaque": True}, "rawRotatingProvenance": raw_provenance}
            self_hash(entry, "entrySha256"); entries.append(entry)
    index = {"schemaVersion": "temporal_qd_tail_result_index_v4", "authorityId": digest("i"), "authoritySha256": digest("j"), "taskMatrixSha256": digest("m"), "taskManifestSha256": digest("n"), "checkpointSha256": digest("k"), "taskCount": len(entries), "funnelProjectionIncluded": True, "sourceResultBlobBytes": 0, "entries": entries}
    self_hash(index, "tailResultIndexSha256")
    index_path = root / "tail-result-index-v4.json"; write(index_path, index, lf=False)
    seal = self_hash({"schemaVersion": "temporal_qd_campaign_seal_v1", "contractVersion": "temporal_qd_native_foundation_v1", "manifestSha256": digest("campaign-manifest"), "runtimeAuthoritySha256": authority["runtimeAuthoritySha256"], "sourceSha256": digest("source"), "authorityId": index["authorityId"], "authoritySha256": index["authoritySha256"], "taskMatrixSha256": index["taskMatrixSha256"], "taskManifestSha256": index["taskManifestSha256"], "checkpointSha256": index["checkpointSha256"], "taskCount": index["taskCount"], "rawResultReadCount": index["taskCount"], "sourceResultBlobBytes": 0, "sourceResultUncompressedBytes": 0, "sourceResultSemanticBytes": 0, "tailResultIndex": {"path": "tail-result-index-v4.json", "sha256": index["tailResultIndexSha256"]}, "rawResultInventory": {"path": "raw-result-inventory.jsonl", "sha256": digest("inventory"), "bytes": 0}}, "campaignSealSha256")
    member_rows = [{"candidate": candidate, "descriptor": {"cellId": "fixture"}, "aggregate": {"totalConservativeNetR": 1.0}} for candidate in candidates]
    members_path = root / "evaluated-members.jsonl"; members_path.write_bytes(b"".join(dump(row) for row in member_rows))
    members_data = members_path.read_bytes()
    evaluated = {"schemaVersion": "temporal_qd_evaluated_members_v1", "memberCount": len(member_rows), "evaluationRejectionCount": 0, "evaluationRejectedCandidates": [], "membersFile": {"path": "evaluated-members.jsonl", "rawSha256": "sha256:" + hashlib.sha256(members_data).hexdigest(), "sizeBytes": len(members_data), "recordCount": len(member_rows)}}
    tail_result = {"schemaVersion": "temporal_qd_native_tail_reduction_result_v1", "generationIndex": 1, "manifestSha256": digest("tail-manifest"), "evaluationPopulationSha256": digest("evaluation"), "populationSha256": digest("population"), "tailResultIndexSha256": index["tailResultIndexSha256"], "taskMatrixSha256": index["taskMatrixSha256"], "resultSetSha256": digest("result-set"), "runtimeAuthoritySha256": authority["runtimeAuthoritySha256"], "evaluatedMembers": evaluated, "provisional": {"schemaVersion": "temporal_qd_native_provisional_survivors_v1", "candidates": []}}
    self_hash(tail_result, "resultSha256")
    tail_result_path = root / "tail-reduction-result.json"; write(tail_result_path, tail_result)
    tail_result_data = tail_result_path.read_bytes()
    tail_receipt = {"schemaVersion": "temporal_qd_tail_authority_receipt_v1", "generationIndex": 1, "tailReductionManifestSha256": tail_result["manifestSha256"], "evaluationPopulationSha256": tail_result["evaluationPopulationSha256"], "populationSha256": tail_result["populationSha256"], "tailResultIndexSha256": index["tailResultIndexSha256"], "taskMatrixSha256": index["taskMatrixSha256"], "resultSetSha256": tail_result["resultSetSha256"], "runtimeAuthoritySha256": authority["runtimeAuthoritySha256"], "tailReductionResult": {"path": "tail-reduction-result.json", "rawSha256": "sha256:" + hashlib.sha256(tail_result_data).hexdigest(), "sizeBytes": len(tail_result_data), "resultSha256": tail_result["resultSha256"]}, "evaluatedMembers": evaluated["membersFile"]}
    self_hash(tail_receipt, "tailAuthoritySha256")
    tail_receipt_path = root / "tail-authority.json"; write(tail_receipt_path, tail_receipt)
    records = {}
    for candidate in candidates:
        rows = []
        for entry in entries:
            if entry["task"]["candidateId"] == candidate["candidateId"]:
                window = next(row for row in panel["windows"] if row["analysisWindowStart"] == entry["task"]["analysisWindowStart"] and row["analysisWindowEnd"] == entry["task"]["analysisWindowEnd"])
                stage_blob = gzip.decompress(base64.b64decode(entry["stageProjection"]["blobBase64"]))
                stage = json.loads(stage_blob)
                enriched_metrics = {**entry["rotatingEvidenceMetrics"], "realizedBehavior": stage["realizedBehavior"]}
                rows.append(build_candidate_window_evidence(candidate=candidate, panel=panel, window=window, metrics=enriched_metrics, evidence_plan_semantic_sha256=entry["task"]["evidencePlanSemanticSha256"], provenance={"authorityId": index["authorityId"], "taskMatrixSha256": index["taskMatrixSha256"], **entry["rawTaskProvenance"], "rawRotatingProvenanceSha256": canonical_sha256(entry["rawRotatingProvenance"])}))
        records[candidate["candidateId"]] = rows
    bundles = [build_candidate_panel_bundle(contract=contract, candidate=candidate, panel_id=panel["panelId"], records=records[candidate["candidateId"]]) for candidate in candidates]
    source = self_hash({"schemaVersion": "temporal_qd_v5_rotating_compact_evidence_source_v1", "tailAuthority": authority, "tailResultIndex": {"schemaVersion": "temporal_qd_v5_tail_result_index_v4_descriptor_v1", "relativePath": "tail-result-index-v4.json", "tailResultIndexSha256": index["tailResultIndexSha256"]}}, "compactEvidenceSourceSha256")
    expected = self_hash({"schemaVersion": "temporal_qd_v5_rotating_panel_bundle_receipt_v1", "role": "proposal_current_panel", "campaignSeal": seal, "compactEvidenceSource": source, "candidatePanelBundles": bundles}, "receiptSha256")
    index_descriptor = file_descriptor(index_path) | {"relativePath": "tail-result-index-v4.json", "tailResultIndexSha256": index["tailResultIndexSha256"]}
    input_value = {"schemaVersion": "temporal_qd_v5_rotating_panel_bundle_input_v2", "contractVersion": "temporal_qd_native_foundation_v1", "generationIndex": 1, "campaignRole": "proposal_current_panel", "campaignSeal": seal, "tailAuthority": {"receiptPath": str(tail_receipt_path), "receiptSha256": tail_receipt["tailAuthoritySha256"]}, "tailResultIndex": index_descriptor, "directionalTailAuthority": authority, "rotatingEvidence": contract, "panel": panel}
    self_hash(input_value, "inputSha256")
    write(root / "input.json", input_value); write(root / "expected.json", expected)


if __name__ == "__main__": main()
