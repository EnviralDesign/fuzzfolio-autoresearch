"""Content-addressed launch gate for the Rust-canonical topology V2.3 package."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256

SCHEMA = "temporal_qd_topology_v2_3_launch_gate_v1"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def _raw_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _descriptor(path: Path, logical_id: str) -> dict[str, Any]:
    return {"logicalId": logical_id, "rawSha256": _raw_sha(path), "sizeBytes": path.stat().st_size}


def build_gate(
    *,
    authority_root: Path,
    production_evidence_path: Path,
    conformance_paths: list[Path],
    full_48_conformance_path: Path,
    gateway_log_path: Path,
    gateway_receipt_path: Path,
    campaign_output_proof_path: Path,
) -> dict[str, Any]:
    control_path = authority_root / "topology-production-launch-control-v1.json"
    mapping_path = authority_root / "topology-production-task-mapping-v1.json"
    output_templates_path = authority_root / "topology-production-output-templates-v1.json"
    rule_path = authority_root / "topology-replication-survival-rule-v1.json"
    analyzer_path = authority_root / "topology-post-run-analyzer-contract-v1.json"
    prior_path = authority_root.parent / "rust-canonical-authority-v2" / "topology-launch-go-nogo-v1.json"
    scientific_path = authority_root.parent / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json"

    control, mapping, outputs = _load(control_path), _load(mapping_path), _load(output_templates_path)
    rule, analyzer, prior, scientific = _load(rule_path), _load(analyzer_path), _load(prior_path), _load(scientific_path)
    evidence = _load(production_evidence_path)
    conformances = [_load(path) for path in conformance_paths]
    full_48 = _load(full_48_conformance_path)
    gateway_log, gateway_receipt = _load(gateway_log_path), _load(gateway_receipt_path)
    campaign_output = _load(campaign_output_proof_path)

    gates = {
        "previousV2_2GateReady": prior.get("readyForTopologyCaseStudyLaunch") is True,
        "scientificContractPreservedAndBound": analyzer.get("scientificContractSha256") == scientific.get("scientificContractSha256"),
        "replicationRuleFrozenAndBound": analyzer.get("replicationRuleSha256") == rule.get("replicationRuleSha256") and rule.get("crossPanelOperator") == "all",
        "postRunAnalyzerFrozen": isinstance(analyzer.get("analyzerContractSha256"), str),
        "productionCampaignInputCheckpointCountIsThree": len(control.get("panels") or []) == 3,
        "productionCountsAreTwelveByFourByThree": control.get("candidateCountPerPanel") == 12 and control.get("panelTaskCounts") == [48, 48, 48] and control.get("totalInspectedTaskCount") == 144,
        "allProductionCheckpointsOpen": evidence.get("allCheckpointsOpened") is True,
        "crossRootDeterminismPassed": evidence.get("crossRootAllEqual") is True,
        "executedSeamUsesExactProductionCheckpointIdentity": evidence.get("allCheckpointIdentitiesMatchExecutedSeamProof") is True,
        "oneToOneGenericTaskMappingPassed": mapping.get("mappedTaskCount") == 144 and len(mapping.get("mappings") or []) == 144,
        "campaignOutputTemplatesFrozenForAllPanels": len(outputs.get("templates") or []) == 3 and outputs.get("dispatchEnabled") is False,
        "allTwelveCandidatesConformOnEveryPanelWithoutMarketData": len(conformances) == 3 and all(row.get("validatedCandidateCount") == 12 and row.get("fullWorkerExecutionFixtureCount") == 12 and row.get("marketDataRead") is False for row in conformances),
        "fullFortyEightResultProductionPanelFixturePassed": full_48.get("fullWorkerExecutionFixtureCount") == 48 and full_48.get("validatedTaskCount") == 48 and full_48.get("marketDataRead") is False,
        "loopbackGatewayDurableBeforeAckPassed": gateway_log.get("loopbackOnly") is True and gateway_log.get("marketDataRead") is False and all(row.get("journalDurableBeforeAck") is True and row.get("resultPackDurableBeforeAck") is True for row in gateway_log.get("ackDurability") or []),
        "gatewayReceiptCommittedForExactFortyEightTasks": gateway_receipt.get("taskCount") == 48 and gateway_receipt.get("completedTaskCount") == 48 and gateway_receipt.get("resultCount") == 48,
        "campaignOutputFreshReopenAndTamperProofPassed": campaign_output.get("freshRestart") is False and campaign_output.get("reopenRestart") is True and campaign_output.get("recoveredRestart") is True and campaign_output.get("tamperRejected") is True and campaign_output.get("taskCount") == 48 and campaign_output.get("evaluatedMemberCount") == 12 and campaign_output.get("panelBundleCount") == 12,
        "noDispatchAuthorityRemainsDisabled": control.get("dispatchEnabled") is False,
        "untouchedConfirmationRemainsPending": rule.get("untouchedConfirmation", {}).get("statusBeforeExecution") == "pending",
    }
    gate: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "scientificContractSha256": scientific["scientificContractSha256"],
        "replicationRuleSha256": rule["replicationRuleSha256"],
        "analyzerContractSha256": analyzer["analyzerContractSha256"],
        "launchControlSha256": control["launchControlSha256"],
        "mappingSha256": mapping["mappingSha256"],
        "outputTemplatesSha256": outputs["templatesSha256"],
        "priorV2_2GoNogoSha256": prior["goNogoSha256"],
        "proofScope": "three_exact_checkpoints_plus_all_panel_candidate_conformance_plus_full_panel_1_48_task_loopback_seal",
        "dispatchEnabled": False,
        "untouchedConfirmationStatus": "pending",
        "evidence": {
            "productionEvidence": _descriptor(production_evidence_path, "production-evidence-report.json"),
            "panelConformance": [_descriptor(path, f"panel-{index}-no-market-conformance.json") for index, path in enumerate(conformance_paths, 1)],
            "full48Conformance": _descriptor(full_48_conformance_path, "panel-1-no-market-conformance-all-48.json"),
            "loopbackGateway": _descriptor(gateway_log_path, "panel-1-fake-gateway-log.json"),
            "gatewayReceipt": _descriptor(gateway_receipt_path, "panel-1-execution-receipt.json"),
            "campaignOutput": _descriptor(campaign_output_proof_path, "panel-1-campaign-output-proof.json"),
        },
        "gates": gates,
        "readyForAuthorizedTopologyCaseStudyLaunch": all(gates.values()),
    }
    gate["launchGateSha256"] = canonical_sha256(gate)
    return gate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--production-evidence", type=Path, required=True)
    parser.add_argument("--panel-conformance", type=Path, nargs=3, required=True)
    parser.add_argument("--full-48-conformance", type=Path, required=True)
    parser.add_argument("--gateway-log", type=Path, required=True)
    parser.add_argument("--gateway-receipt", type=Path, required=True)
    parser.add_argument("--campaign-output-proof", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    gate = build_gate(
        authority_root=args.authority_root,
        production_evidence_path=args.production_evidence,
        conformance_paths=args.panel_conformance,
        full_48_conformance_path=args.full_48_conformance,
        gateway_log_path=args.gateway_log,
        gateway_receipt_path=args.gateway_receipt,
        campaign_output_proof_path=args.campaign_output_proof,
    )
    args.output.write_text(canonical_json(gate) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
