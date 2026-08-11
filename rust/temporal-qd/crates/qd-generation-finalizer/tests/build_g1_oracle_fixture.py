"""Build a read-only G1 finalizer fixture from an existing completed run.

This is intentionally a developer/oracle utility, not a production gateway.
It never edits the source run. The generated source copies only already
committed compact evidence and the rich members which earned breeding rights.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from autoresearch.temporal_discovery_base import canonical_sha256


CONTRACT_VERSION = "temporal_qd_native_foundation_v1"


def write_canonical(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def self_hash(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    run_root = args.run_root.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    generation = run_root / "generations" / "generation-0001"

    load = lambda path: json.loads(Path(path).read_text(encoding="utf-8"))
    config = load(run_root / "config.json")
    cohort = load(generation / "evidence" / "cohort.json")
    provisional = load(generation / "evidence" / "provisional.json")
    cumulative = load(generation / "evidence" / "cumulative-archive.json")
    archive = load(generation / "archive.json")
    ledger = load(generation / "evidence" / "generation-ledger.json")
    funnel = load(generation / "generation-funnel.json")
    previous = load(config["initialArchive"]["path"])

    rich_members = [member for cell in archive["cells"] for member in cell["members"]]
    previous_summary = self_hash(
        {
            "schemaVersion": "temporal_qd_previous_parent_archive_summary_v1",
            "archiveSha256": previous["archiveSha256"],
            "candidateCountSeen": previous["candidateCountSeen"],
            "memberCount": previous["memberCount"],
            "cellIds": sorted(str(row["cellId"]) for row in previous["cells"]),
            **(
                {"bidirectionalPairPolicy": previous["bidirectionalPairPolicy"]}
                if "bidirectionalPairPolicy" in previous
                else {}
            ),
        },
        "summarySha256",
    )
    archive_policy = self_hash(
        {
            "schemaVersion": "temporal_qd_archive_policy_binding_v1",
            "qdVersion": config["qdVersion"],
            "policyName": config["policyName"],
            "policySha256": config["policySha256"],
            "frozenPolicy": config["frozenPolicy"],
        },
        "policyBindingSha256",
    )
    funnel_source = self_hash(
        {
            "schemaVersion": "temporal_qd_native_funnel_reduction_source_v1",
            "completenessPolicy": funnel["completenessPolicy"],
            "proposalAccounting": funnel["proposalAccounting"],
            "proposalAttempts": funnel["attemptLedger"]["attempts"],
            "candidateStageRows": funnel["candidates"],
        },
        "funnelSourceSha256",
    )
    provisional_ids = {
        row["candidateId"] for row in provisional["candidates"]
    }
    required_panels = []
    for index in range(1, 2):
        panel = config["rotatingEvidence"]["panels"][
            (index - 1)
            % config["rotatingEvidence"]["absoluteGenerationMapping"]["cycleLength"]
        ]["panelId"]
        if panel not in required_panels:
            required_panels.append(panel)
    selected = self_hash(
        {
            "schemaVersion": "temporal_qd_selected_rich_members_v1",
            "generationIndex": 1,
            "cohortSha256": cohort["cohortSha256"],
            "provisionalSha256": provisional["provisionalSha256"],
            "members": rich_members,
            "memberCount": len(rich_members),
        },
        "selectedRichMembersSha256",
    )
    coverage = self_hash(
        {
            "schemaVersion": "temporal_qd_v5_panel_coverage_v1",
            "generationIndex": 1,
            "rotatingEvidenceSha256": config["rotatingEvidence"]["rotatingEvidenceSha256"],
            "cohortSha256": cohort["cohortSha256"],
            "provisionalSha256": provisional["provisionalSha256"],
            "requiredPanelIds": required_panels,
            "candidatePanelBundleSha256": canonical_sha256(
                cumulative["candidatePanelBundles"]
            ),
            "coverage": {
                candidate_id: {
                    "panelIds": required_panels,
                }
                for candidate_id in sorted(provisional_ids)
            },
        },
        "panelCoverageSha256",
    )
    admitted_campaigns = self_hash(
        {
            "schemaVersion": "temporal_qd_v5_admitted_campaign_ledger_v1",
            "generationIndex": 1,
            "rotatingEvidenceSha256": config["rotatingEvidence"]["rotatingEvidenceSha256"],
            "cohortSha256": cohort["cohortSha256"],
            "provisionalSha256": provisional["provisionalSha256"],
            "campaigns": ledger["campaigns"],
        },
        "admittedCampaignLedgerSha256",
    )
    state_basis = self_hash(
        {
            "schemaVersion": "temporal_qd_v5_generation_state_basis_v1",
            "configSha256": canonical_sha256(config),
            "generationIndex": 1,
            "completedGenerationsSha256": canonical_sha256([]),
            "uniqueCandidatesEvaluated": 0,
            "workerTasksCompleted": 0,
            "nextImmigrantContinuationOrdinal": 0,
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
        },
        "stateBasisSha256",
    )
    semantic_authority = canonical_sha256(
        {"fixture": "g1-python-oracle-v2", "generationIndex": 1}
    )
    runtime_authority = canonical_sha256(
        {"fixture": "g1-runtime-authority-v2"}
    )
    source = {
        "schemaVersion": "temporal_qd_generation_finalization_source_v2",
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": 1,
        "semanticAuthoritySha256": semantic_authority,
        "runtimeAuthoritySha256": runtime_authority,
        "stateBasis": state_basis,
        "completedGenerationRecords": [],
        "proposalStateAuthority": {
            "generationKind": "g0",
            "proposalManifestSha256": canonical_sha256({"fixture": "proposal-manifest"}),
            "proposalReceiptSha256": canonical_sha256({"fixture": "proposal-receipt"}),
            "generationJournalSha256": canonical_sha256({"fixture": "generation-journal"}),
            "inputIdentityLedgerSha256": None,
            "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
            "outputIdentityLedgerSha256": canonical_sha256({"fixture": "identity-ledger"}),
            "outputIdentityLedgerFileSha256": canonical_sha256({"fixture": "identity-ledger-file"}),
        },
        "rotatingEvidence": config["rotatingEvidence"],
        "cohort": cohort,
        "provisional": provisional,
        "panelCoverage": coverage,
        "selectedRichMembers": selected,
        "baselineCandidatePanelBundles": cumulative["candidatePanelBundles"],
        "previousCumulativeArchive": None,
        "previousParentArchiveSummary": previous_summary,
        "archivePolicy": archive_policy,
        "admittedCampaignLedger": admitted_campaigns,
        "funnelReductionSource": funnel_source,
    }
    self_hash(source, "sourceSha256")
    source_path = output / "source.json"
    write_canonical(source_path, source)
    manifest = self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v2",
            "contractVersion": CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": runtime_authority,
            "semanticAuthoritySha256": semantic_authority,
            "sourcePath": str(source_path),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    write_canonical(output / "manifest.json", manifest)


if __name__ == "__main__":
    main()
