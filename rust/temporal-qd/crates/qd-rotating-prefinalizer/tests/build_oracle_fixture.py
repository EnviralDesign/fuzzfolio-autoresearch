"""Build a compact Python-oracle fixture for the Rust rotating pre-finalizer.

It deliberately exercises generation 2: two required panels, diverse
round-robin selection, compact campaign-seal rows, and a complete hydrated
bundle snapshot.  No worker/raw-result process is involved.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_rotating_evidence import (
    build_current_panel_evaluation_cohort,
    build_rotating_evidence_contract,
    reduce_provisional_diverse_survivors,
)


def write(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8", newline="\n")


def digest(label: str) -> str:
    return "sha256:" + (label * 64)[:64]


def self_hash(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def descriptor(path: Path, rows: list[dict]) -> dict:
    data = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows)
    path.write_text(data, encoding="utf-8", newline="\n")
    return {"path": str(path.resolve()), "rawSha256": "sha256:" + __import__("hashlib").sha256(data.encode()).hexdigest(), "sizeBytes": len(data.encode()), "recordCount": len(rows)}


def main() -> None:
    out = Path(sys.argv[1]).resolve(); out.mkdir(parents=True, exist_ok=True)
    contract = build_rotating_evidence_contract({
        "schemaVersion": "temporal_qd_rotating_evidence_input_v1",
        "developmentYears": [{"analysisWindowStart": f"202{year}-01-01T00:00:00Z", "analysisWindowEnd": f"202{year+1}-01-01T00:00:00Z"} for year in range(1, 5)],
        "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"},
        "provisionalSurvivorCount": 2, "breederWidth": 2,
    })
    candidates = []
    for ident, cell, rank in (("a", "cell-1", 9.0), ("b", "cell-2", 3.0), ("c", "cell-1", 8.0)):
        candidate = {"candidateId": f"candidate-{ident}", "candidateIdentitySha256": digest(ident), "programSha256": digest(chr(ord(ident)+3)), "profileSnapshotSha256": digest(chr(ord(ident)+6))}
        candidates.append({"candidateId": candidate["candidateId"], "candidate": candidate, "descriptor": {"cellId": cell}, "aggregate": {"totalConservativeNetR": rank}})
    proposal_members = descriptor(out / "proposal-members.jsonl", candidates)
    parents = descriptor(out / "parents.jsonl", [])
    seal = self_hash({"schemaVersion": "temporal_qd_campaign_seal_v1", "campaign": "fixture"}, "campaignSealSha256")
    tail_authority = self_hash({"schemaVersion": "temporal_qd_v5_directional_tail_authority_v1", "generationIndex": 2, "runtimeAuthoritySha256": digest("t"), "tailResultIndexSchema": "temporal_qd_tail_result_index_v4", "tailResultEntrySchema": "temporal_qd_tail_result_index_entry_v4", "rawRotatingProvenanceSchema": "temporal_qd_v5_raw_rotating_provenance_v1"}, "tailAuthoritySha256")
    compact_source = self_hash({"schemaVersion": "temporal_qd_v5_rotating_compact_evidence_source_v1", "tailAuthority": tail_authority, "tailResultIndex": {"schemaVersion": "temporal_qd_v5_tail_result_index_v4_descriptor_v1", "relativePath": "tail-result-index-v4.json", "tailResultIndexSha256": digest("i")}}, "compactEvidenceSourceSha256")
    bundles = []
    for candidate in candidates[:2]:
        source = candidate["candidate"]
        for panel in ("panel-1", "panel-2"):
            bundles.append(self_hash({"schemaVersion": "temporal_qd_candidate_panel_evidence_bundle_v1", "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "candidateId": source["candidateId"], "candidateIdentitySha256": source["candidateIdentitySha256"], "programSha256": source["programSha256"], "rawSourceProfileSha256": source["profileSnapshotSha256"], "normalizedProfileSnapshotSha256": source["profileSnapshotSha256"], "panelId": panel, "windowEvidence": [], "windowEvidenceDigests": [], "rawTaskProvenance": []}, "bundleSha256"))
    receipt = self_hash({"schemaVersion": "temporal_qd_v5_rotating_panel_bundle_receipt_v1", "role": "proposal_current_panel", "campaignSeal": seal, "compactEvidenceSource": compact_source, "candidatePanelBundles": bundles}, "receiptSha256")
    proposal_binding = self_hash({"schemaVersion": "temporal_qd_rotating_candidate_source_binding_v1", "sourceRole": "proposal_evaluation_population", "sourceSemanticSha256": digest("e")}, "bindingSha256")
    parent_binding = self_hash({"schemaVersion": "temporal_qd_rotating_candidate_source_binding_v1", "sourceRole": "retained_parent_archive", "sourceSemanticSha256": digest("f")}, "bindingSha256")
    context = {"previousCumulativeArchive": None, "previousParentArchiveSummary": {"summary": "fixture"}, "archivePolicy": {"policy": "fixture"}, "cellCapacity": 3, "campaigns": [{"role": "proposal_current_panel"}], "artifactLedgerBase": {"artifact": "fixture"}, "publicationPaths": {"archive": "fixture"}, "funnelReductionSource": {"funnel": "fixture"}, "generationRecordBase": {"record": "fixture"}, "stateTransitionBase": {"nextGenerationIndex": 3}, "runtimeAuthoritySha256": digest("d")}
    value = {"schemaVersion": "temporal_qd_rotating_prefinalizer_input_v1", "contractVersion": "temporal_qd_native_foundation_v1", "generationIndex": 2, "rotatingEvidence": contract, "proposalCampaignSeal": seal, "proposalMembers": proposal_members, "proposalMembersCampaignSealSha256": seal["campaignSealSha256"], "proposalPopulationBinding": proposal_binding, "retainedParents": parents, "retainedParentArchiveBinding": parent_binding, "currentPanelReceipts": [], "panelBundleReceipts": [receipt], "previousCandidatePanelBundles": [], "finalizerContext": context}
    value["inputSha256"] = canonical_sha256(value)
    write(out / "input.json", value)
    cohort = build_current_panel_evaluation_cohort(new_candidates=[row["candidate"] for row in candidates], retained_parents=[], contract=contract, generation_index=2)
    counts = {"cell-1": 2, "cell-2": 1}
    provisional_rows = reduce_provisional_diverse_survivors([{"candidateId": row["candidateId"], "candidateIdentitySha256": row["candidate"]["candidateIdentitySha256"], "programSha256": row["candidate"]["programSha256"], "profileSnapshotSha256": row["candidate"]["profileSnapshotSha256"], "cellId": row["descriptor"]["cellId"], "costView": "research_conservative", "currentPanelRank": row["aggregate"]["totalConservativeNetR"], "novelty": 1.0 / counts[row["descriptor"]["cellId"]]} for row in candidates], limit=2)
    provisional = self_hash({"schemaVersion": "temporal_qd_provisional_survivors_v1", "generationIndex": 2, "panelId": "panel-2", "cohortSha256": cohort["cohortSha256"], "candidateCount": 2, "candidates": provisional_rows}, "provisionalSha256")
    expected_source = self_hash({"schemaVersion": "temporal_qd_generation_finalization_source_v1", "contractVersion": "temporal_qd_native_foundation_v1", "generationIndex": 2, "rotatingEvidence": contract, "cohort": cohort, "provisional": provisional, "baselineCandidatePanelBundles": sorted(bundles, key=lambda row: (row["candidateId"], row["panelId"])), "completeBundleSnapshot": True, "auxiliaryPlan": None, "auxiliaryCampaignReceipts": [], "previousCumulativeArchive": None, "previousParentArchiveSummary": context["previousParentArchiveSummary"], "archivePolicy": context["archivePolicy"], "richMembers": candidates[:2], "currentMemberCount": 3, "cellCapacity": 3, "campaigns": context["campaigns"], "artifactLedgerBase": context["artifactLedgerBase"], "publicationPaths": context["publicationPaths"], "funnelReductionSource": context["funnelReductionSource"], "generationRecordBase": context["generationRecordBase"], "stateTransitionBase": context["stateTransitionBase"]}, "sourceSha256")
    write(out / "expected.json", {"cohort": cohort, "provisional": provisional, "source": expected_source})


if __name__ == "__main__": main()
