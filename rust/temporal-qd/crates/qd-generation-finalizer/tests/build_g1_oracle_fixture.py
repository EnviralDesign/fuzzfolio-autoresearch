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
    state = load(run_root / "state.json")
    cohort = load(generation / "evidence" / "cohort.json")
    provisional = load(generation / "evidence" / "provisional.json")
    cumulative = load(generation / "evidence" / "cumulative-archive.json")
    archive = load(generation / "archive.json")
    checkpoint = load(generation / "evidence" / "checkpoint.json")
    ledger = load(generation / "evidence" / "generation-ledger.json")
    funnel = load(generation / "generation-funnel.json")
    previous = load(config["initialArchive"]["path"])
    generation_record = state["completedGenerations"][0]

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
    record_base = {
        key: value
        for key, value in generation_record.items()
        if key
        not in {
            "archiveSha256",
            "resultSetSha256",
            "rotatingEvidenceLedgerSha256",
            "rotatingEvidenceCheckpointSha256",
            "cumulativeArchiveSha256",
        }
    }
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
    source = {
        "schemaVersion": "temporal_qd_generation_finalization_source_v1",
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": 1,
        "rotatingEvidence": config["rotatingEvidence"],
        "cohort": cohort,
        "provisional": provisional,
        "baselineCandidatePanelBundles": cumulative["candidatePanelBundles"],
        "completeBundleSnapshot": True,
        "auxiliaryPlan": None,
        "auxiliaryCampaignReceipts": [],
        "previousCumulativeArchive": None,
        "previousParentArchiveSummary": previous_summary,
        "archivePolicy": archive_policy,
        "richMembers": rich_members,
        "currentMemberCount": archive["candidateCountReducedThisGeneration"],
        "cellCapacity": archive["cellCapacity"],
        "campaigns": ledger["campaigns"],
        "stageArtifacts": checkpoint["stageArtifacts"],
        "artifactLedger": generation_record["artifacts"],
        "funnelReductionSource": funnel_source,
        "generationRecordBase": record_base,
        "stateTransitionBase": {
            "nextGenerationIndex": 2,
            "nextStage": "generation_proposal",
            "candidateCountIncrement": generation_record["candidateCount"],
            "workerTaskCountIncrement": generation_record["totalGenerationTaskCount"],
            "nextImmigrantContinuationOrdinal": generation_record[
                "nextImmigrantContinuationOrdinal"
            ],
        },
    }
    self_hash(source, "sourceSha256")
    source_path = output / "source.json"
    write_canonical(source_path, source)
    manifest = self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "sourcePath": str(source_path),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    write_canonical(output / "manifest.json", manifest)


if __name__ == "__main__":
    main()
