"""Executable V2.4 launch gate that recomputes production evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v2 import reduce_files

SCHEMA = "temporal_qd_topology_v2_4_launch_gate_v1"
BASE_COMMIT = "dc68ab80eda27a80ef1133790143a499739157a2"
WORKER_IMAGE = "sha256:1817ddc68b55433bb81c59572e51d5dddc40e2a95ac9004fafee979adbb913fe"
WORKER_CONTRACT = "sha256:ae5d0e53aa19e1e241468c009e248457560ca63e2e3d785854750b028736c9df"
WORKER_COMMIT = "0fbe84a9f7b73b97789c8370b268f4d01eeb37ce"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def _verify(value: dict[str, Any], field: str, label: str) -> bool:
    unsigned = dict(value)
    stored = unsigned.pop(field, None)
    if stored != canonical_sha256(unsigned):
        raise ValueError(f"{label} self-hash mismatch")
    return True


def _raw_descriptor(path: Path, logical_id: str) -> dict[str, Any]:
    return {
        "logicalId": logical_id,
        "rawSha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        "sizeBytes": path.stat().st_size,
    }


def _git_ancestry(repo_root: Path) -> bool:
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    ).returncode == 0


def _parity(corpus: dict[str, Any], parity_bin: Path) -> bool:
    requests = "".join(
        canonical_json(
            {
                "schemaVersion": "temporal_qd_topology_replication_request_v2",
                "inputs": case["inputs"],
                "identitiesValid": case["identitiesValid"],
            }
        )
        + "\n"
        for case in corpus["cases"]
    )
    completed = subprocess.run(
        [str(parity_bin)],
        input=requests,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if completed.returncode:
        return False
    observed = [json.loads(line) for line in completed.stdout.splitlines() if line]
    return observed == [case["expected"] for case in corpus["cases"]]


def build_gate(
    *,
    repo_root: Path,
    authority_root: Path,
    proof_root: Path,
    opener: Path,
    parity_bin: Path,
    cross_root_report_path: Path,
) -> dict[str, Any]:
    launch = _load(authority_root / "topology-production-launch-control-v1.json")
    mapping = _load(authority_root / "topology-production-task-mapping-v1.json")
    templates = _load(authority_root / "topology-production-output-templates-v1.json")
    rule = _load(authority_root / "topology-replication-survival-rule-v1.json")
    analyzer = _load(authority_root / "topology-post-run-analyzer-contract-v1.json")
    scientific_path = authority_root.parent / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json"
    scientific = _load(scientific_path)
    prior = _load(authority_root.parent / "rust-canonical-authority-v2" / "topology-launch-go-nogo-v1.json")
    parity_path = authority_root.parent / "rust-canonical-authority-v2-4" / "topology-replication-parity-corpus-v2.json"
    parity = _load(parity_path)
    reducer_contract_path = authority_root.parent / "rust-canonical-authority-v2-4" / "topology-production-reducer-contract-v2.json"
    reducer_contract = _load(reducer_contract_path)
    cross_root = _load(cross_root_report_path)

    authority_hashes_valid = all(
        (
            _verify(launch, "launchControlSha256", "launch control"),
            _verify(mapping, "mappingSha256", "task mapping"),
            _verify(templates, "templatesSha256", "output templates"),
            _verify(rule, "replicationRuleSha256", "replication rule"),
            _verify(analyzer, "analyzerContractSha256", "analyzer contract"),
            _verify(scientific, "scientificContractSha256", "scientific contract"),
            _verify(prior, "goNogoSha256", "prior go/no-go"),
            _verify(parity, "corpusSha256", "parity corpus"),
            _verify(reducer_contract, "reducerContractSha256", "production reducer contract"),
            _verify(cross_root, "crossRootReportSha256", "cross-root report"),
        )
    )

    checkpoints = [
        proof_root / f"panel-{panel}" / "campaign-output-local" / "campaign-output-checkpoint.json"
        for panel in (1, 2, 3)
    ]
    analysis = reduce_files(
        checkpoints=checkpoints,
        opener=opener,
        launch_control_path=authority_root / "topology-production-launch-control-v1.json",
        task_mapping_path=authority_root / "topology-production-task-mapping-v1.json",
        replication_rule_path=authority_root / "topology-replication-survival-rule-v1.json",
        scientific_contract_path=scientific_path,
        analyzer_contract_path=authority_root / "topology-post-run-analyzer-contract-v1.json",
    )
    committed_analysis_path = proof_root / "topology-production-analysis-v2.json"
    committed_analysis = _load(committed_analysis_path)

    all_task_ids: set[str] = set()
    conformance_complete = True
    lifecycle_complete = True
    proof_descriptors: list[dict[str, Any]] = []
    for panel in (1, 2, 3):
        panel_root = proof_root / f"panel-{panel}"
        conformance_path = panel_root / "no-market-conformance-all-48.json"
        conformance = _load(conformance_path)
        fixtures = conformance.get("executedFixtures") or []
        task_ids = [row.get("task", {}).get("task_id") for row in fixtures]
        workers = [row.get("task", {}).get("payload", {}) for row in fixtures]
        conformance_complete &= (
            conformance.get("marketDataRead") is False
            and conformance.get("dispatchPerformed") is False
            and conformance.get("validatedTaskCount") == 48
            and conformance.get("fullWorkerExecutionFixtureCount") == 48
            and len(fixtures) == 48
            and len(set(task_ids)) == 48
            and conformance.get("workerContractHash") == WORKER_CONTRACT
            and conformance.get("workerImageDigest") == WORKER_IMAGE
            and all(row.get("required_worker_contract_hash") == WORKER_CONTRACT for row in workers)
            and all(row.get("required_worker_image_digest") == WORKER_IMAGE for row in workers)
            and all(row.get("required_worker_source_git_commit") == WORKER_COMMIT for row in workers)
        )
        all_task_ids.update(task_ids)

        gateway_log_path = panel_root / "fake-gateway-log.json"
        gateway_receipt_path = panel_root / "gateway-local-output" / ".native-gateway-dispatch" / "execution-receipt.json"
        proof_path = panel_root / ("campaign-output-proof.json" if panel == 1 else "campaign-output-proof-v2-4.json")
        gateway_log, gateway_receipt, proof = _load(gateway_log_path), _load(gateway_receipt_path), _load(proof_path)
        lifecycle_complete &= (
            gateway_log.get("loopbackOnly") is True
            and gateway_log.get("marketDataRead") is False
            and gateway_log.get("endpointLog") == ["GET /results?limit=48", "POST /tasks:48", "GET /results?limit=48", "POST /results/ack:48"]
            and all(row.get("journalDurableBeforeAck") is True and row.get("resultPackDurableBeforeAck") is True for row in gateway_log.get("ackDurability") or [])
            and gateway_receipt.get("taskCount") == 48
            and gateway_receipt.get("completedTaskCount") == 48
            and gateway_receipt.get("resultCount") == 48
            and proof.get("freshRestart") is False
            and proof.get("reopenRestart") is True
            and proof.get("recoveredRestart") is True
            and proof.get("tamperRejected") is True
            and proof.get("taskCount") == 48
            and proof.get("evaluatedMemberCount") == 12
            and proof.get("panelBundleCount") == 12
        )
        proof_descriptors.extend(
            (
                _raw_descriptor(conformance_path, f"panel-{panel}/no-market-conformance-all-48.json"),
                _raw_descriptor(gateway_log_path, f"panel-{panel}/fake-gateway-log.json"),
                _raw_descriptor(gateway_receipt_path, f"panel-{panel}/execution-receipt.json"),
                _raw_descriptor(proof_path, f"panel-{panel}/campaign-output-proof.json"),
            )
        )

    mapped_task_ids = {row["newTaskId"] for row in mapping["mappings"]}
    gates = {
        "exactV2_3Ancestry": _git_ancestry(repo_root),
        "frozenAuthoritiesSelfHashAndBindingsValid": authority_hashes_valid,
        "frozenScientificAndReplicationIdentitiesPreserved": scientific["scientificContractSha256"] == analyzer["scientificContractSha256"] and rule["replicationRuleSha256"] == analyzer["replicationRuleSha256"],
        "productionReducerContractBoundToFrozenScience": reducer_contract.get("scientificContractSha256") == scientific["scientificContractSha256"] and reducer_contract.get("replicationRuleSha256") == rule["replicationRuleSha256"] and reducer_contract.get("callerComputedMetricDictionariesAccepted") is False and reducer_contract.get("callerIdentityValidityBooleanAccepted") is False,
        "exactThreeCheckpointsAnd144TaskPackage": len(checkpoints) == 3 and launch.get("totalInspectedTaskCount") == 144 and launch.get("panelTaskCounts") == [48, 48, 48],
        "exactOneToOneSemanticMappingRecomputed": len(mapped_task_ids) == 144 and mapped_task_ids == all_task_ids,
        "candidateProgramProfileAndWorkerIdentitiesAuthenticated": analysis.get("status") == "complete" and conformance_complete,
        "completeThreePanelNoMarketGatewayCampaignOutputProof": lifecycle_complete,
        "productionReducerDirectIntegrationProof": analysis == committed_analysis and len(analysis.get("blocks") or {}) == 3,
        "exactPythonRustReplicationProjectionParity": _parity(parity, parity_bin),
        "expandedCrossRootDeterminism": cross_root.get("allPortableArtifactsByteIdentical") is True and cross_root.get("operationalDifferencePermittedOnlyForRootBoundManifestAndCheckpoint") is True and len(cross_root.get("portableArtifacts") or []) >= 22,
        "adversarialFailClosedEvidence": all(proof.get("tamperRejected") is True for proof in [_load(proof_root / "panel-1" / "campaign-output-proof.json"), _load(proof_root / "panel-2" / "campaign-output-proof-v2-4.json"), _load(proof_root / "panel-3" / "campaign-output-proof-v2-4.json")]) and reduce_files(
            checkpoints=checkpoints[:2],
            opener=opener,
            launch_control_path=authority_root / "topology-production-launch-control-v1.json",
            task_mapping_path=authority_root / "topology-production-task-mapping-v1.json",
            replication_rule_path=authority_root / "topology-replication-survival-rule-v1.json",
            scientific_contract_path=scientific_path,
            analyzer_contract_path=authority_root / "topology-post-run-analyzer-contract-v1.json",
        ).get("status") == "incomplete_invalid",
        "dispatchAuthorityDisabled": launch.get("dispatchEnabled") is False and templates.get("dispatchEnabled") is False,
        "untouchedConfirmationPendingAndCannotRescue": rule.get("untouchedConfirmation", {}).get("statusBeforeExecution") == "pending" and rule.get("untouchedConfirmation", {}).get("mayRescueInspectedFailure") is False and analysis.get("untouchedConfirmationStatus") == "pending",
    }
    gate: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "baseCommit": BASE_COMMIT,
        "scientificContractSha256": scientific["scientificContractSha256"],
        "replicationRuleSha256": rule["replicationRuleSha256"],
        "launchControlSha256": launch["launchControlSha256"],
        "mappingSha256": mapping["mappingSha256"],
        "analysisSha256": analysis.get("analysisSha256"),
        "crossRootReportSha256": cross_root["crossRootReportSha256"],
        "parityCorpusSha256": parity["corpusSha256"],
        "reducerContractSha256": reducer_contract["reducerContractSha256"],
        "dispatchEnabled": False,
        "untouchedConfirmationStatus": "pending",
        "evidence": proof_descriptors
        + [
            _raw_descriptor(committed_analysis_path, "topology-production-analysis-v2.json"),
            _raw_descriptor(cross_root_report_path, "topology-v2-4-cross-root-report.json"),
            _raw_descriptor(parity_path, "topology-replication-parity-corpus-v2.json"),
            _raw_descriptor(reducer_contract_path, "topology-production-reducer-contract-v2.json"),
        ],
        "gates": gates,
        "readyForAuthorizedTopologyCaseStudyLaunch": all(gates.values()),
    }
    gate["launchGateSha256"] = canonical_sha256(gate)
    return gate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--proof-root", type=Path, required=True)
    parser.add_argument("--production-opener", type=Path, required=True)
    parser.add_argument("--parity-bin", type=Path, required=True)
    parser.add_argument("--cross-root-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    gate = build_gate(
        repo_root=args.repo_root,
        authority_root=args.authority_root,
        proof_root=args.proof_root,
        opener=args.production_opener,
        parity_bin=args.parity_bin,
        cross_root_report_path=args.cross_root_report,
    )
    args.output.write_text(canonical_json(gate) + "\n", encoding="utf-8", newline="\n")
    if not gate["readyForAuthorizedTopologyCaseStudyLaunch"]:
        raise SystemExit("V2.4 launch gate is not ready")


if __name__ == "__main__":
    main()
