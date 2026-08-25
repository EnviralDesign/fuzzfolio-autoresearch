"""Executable V2.5 gate for the corrected authenticated topology reducer."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v3 import SCHEMA as ANALYSIS_SCHEMA
from .temporal_qd_topology_production_reducer_v3 import reduce_files_v3

SCHEMA = "temporal_qd_topology_v2_5_launch_gate_v1"
BASE_COMMIT = "f7809189cc404d6b5898eb24ff067f4859ff1131"
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


def _raw(path: Path, logical_id: str) -> dict[str, Any]:
    return {
        "logicalId": logical_id,
        "rawSha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        "sizeBytes": path.stat().st_size,
    }


def _ancestry(repo_root: Path) -> bool:
    return subprocess.run(
        ["git", "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    ).returncode == 0


def _policy_regressions(repo_root: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_temporal_qd_topology_panel_usefulness_v2.py",
            "-q",
        ],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return {
        "command": "python -m pytest tests/test_temporal_qd_topology_panel_usefulness_v2.py -q",
        "exitCode": completed.returncode,
        "expectedCaseCount": 33,
        "passed": completed.returncode == 0 and "33 passed" in completed.stdout,
    }


def _mechanism_schema_correct(analysis: dict[str, Any]) -> bool:
    for block in analysis.get("blocks", {}).values():
        for panel in block.get("panelReports", {}).values():
            mechanism = panel.get("mechanism", {})
            for arm in ("P", "T", "E", "TE"):
                row = mechanism.get(arm, {})
                if (
                    "closedTradeCountChangeVersusP" not in row
                    or "entrySequenceComparison" not in row
                    or "changedSideTransitionDistribution" not in row
                    or row.get("eventSpecificActivation", {}).get("status") != "unavailable"
                    or "changedTradeOpportunityCountVersusP" in row
                    or "entryTimingShiftVersusP" in row
                    or "routeEventActivation" in row
                ):
                    return False
    return True


def build_gate(
    *,
    repo_root: Path,
    authority_root: Path,
    v2_5_authority_root: Path,
    proof_root: Path,
    opener: Path,
    cross_root_report_path: Path,
    prior_v2_4_gate_path: Path,
) -> dict[str, Any]:
    launch_path = authority_root / "topology-production-launch-control-v1.json"
    mapping_path = authority_root / "topology-production-task-mapping-v1.json"
    rule_path = authority_root / "topology-replication-survival-rule-v1.json"
    analyzer_path = authority_root / "topology-post-run-analyzer-contract-v1.json"
    scientific_path = authority_root.parent / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json"
    policy_path = v2_5_authority_root / "topology-panel-usefulness-policy-v2.json"
    contract_path = v2_5_authority_root / "topology-production-reducer-contract-v3.json"
    parity_path = v2_5_authority_root / "topology-policy-parity-corpus-v2.json"
    analysis_path = proof_root / "topology-production-analysis-v3.json"
    launch, mapping, rule, analyzer, scientific = map(
        _load, (launch_path, mapping_path, rule_path, analyzer_path, scientific_path)
    )
    policy, contract, parity, committed_analysis, cross_root, prior_gate = map(
        _load, (policy_path, contract_path, parity_path, analysis_path, cross_root_report_path, prior_v2_4_gate_path)
    )
    authority_hashes_valid = all(
        (
            _verify(launch, "launchControlSha256", "launch control"),
            _verify(mapping, "mappingSha256", "task mapping"),
            _verify(rule, "replicationRuleSha256", "replication rule"),
            _verify(analyzer, "analyzerContractSha256", "analyzer contract"),
            _verify(scientific, "scientificContractSha256", "scientific contract"),
            _verify(policy, "panelUsefulnessPolicySha256", "V2.5 panel policy"),
            _verify(contract, "reducerContractSha256", "V2.5 reducer contract"),
            _verify(parity, "parityCorpusSha256", "V2.5 parity corpus"),
            _verify(committed_analysis, "analysisSha256", "V2.5 production analysis"),
            _verify(cross_root, "crossRootReportSha256", "V2.5 cross-root report"),
            _verify(prior_gate, "launchGateSha256", "V2.4 launch gate"),
        )
    )
    checkpoints = [
        proof_root / f"panel-{panel}" / "campaign-output-local" / "campaign-output-checkpoint.json"
        for panel in (1, 2, 3)
    ]
    analysis = reduce_files_v3(
        checkpoints=checkpoints,
        opener=opener,
        launch_control_path=launch_path,
        task_mapping_path=mapping_path,
        replication_rule_path=rule_path,
        scientific_contract_path=scientific_path,
        analyzer_contract_path=analyzer_path,
        panel_policy_path=policy_path,
    )
    incomplete = reduce_files_v3(
        checkpoints=checkpoints[:2],
        opener=opener,
        launch_control_path=launch_path,
        task_mapping_path=mapping_path,
        replication_rule_path=rule_path,
        scientific_contract_path=scientific_path,
        analyzer_contract_path=analyzer_path,
        panel_policy_path=policy_path,
    )
    regressions = _policy_regressions(repo_root)
    all_u_v2 = [
        panel["usefulProgressiveInnovationV2"]
        for block in analysis.get("blocks", {}).values()
        for panel in block.get("panelReports", {}).values()
    ]
    exact_policy = (
        contract.get("panelUsefulnessPolicySha256") == policy.get("panelUsefulnessPolicySha256")
        and contract.get("archivePolicySha256") == policy.get("archivePolicySha256")
        and contract.get("directionSelectionPolicySha256") == policy.get("directionSelectionPolicySha256")
        and contract.get("supportThresholds") == {"minimumTotalTrades": 8, "minimumTradesPerWindow": 4}
        and contract.get("eligibilityGateScope") == "TE_only"
        and contract.get("controlEligibilityDisposition") == "diagnostic_nonveto"
        and contract.get("parityCorpusSha256") == parity.get("parityCorpusSha256")
    )
    gates = {
        "exactV2_4Ancestry": _ancestry(repo_root),
        "allAuthoritiesAndResultsSelfHashed": authority_hashes_valid,
        "originalV1ScienceAndReplicationRulePreserved": policy.get("originalScientificContractSha256") == scientific.get("scientificContractSha256") and policy.get("originalReplicationRuleSha256") == rule.get("replicationRuleSha256"),
        "exactFrozenProductionPolicyBound": exact_policy,
        "teOnlyEligibilityAndControlNonvetoRegressionProof": regressions["passed"],
        "exactPythonRustPolicyPanelAndReplicationParity": regressions["passed"],
        "exactThreeAuthenticatedPanelsAnd144Tasks": analysis.get("status") == "complete" and len(analysis.get("authenticatedPanels", {})) == 3 and launch.get("totalInspectedTaskCount") == 144,
        "correctedReducerDirectDeterministicIntegration": analysis == committed_analysis and analysis.get("schemaVersion") == ANALYSIS_SCHEMA and len(analysis.get("blocks", {})) == 3,
        "panelLocalPredicateIsU_v2": all(
            block.get("replication", {}).get("panelLocalPredicate") == "U_v2"
            for block in analysis.get("blocks", {}).values()
        ) and len(all_u_v2) == 9,
        "correctedMechanismSchema": _mechanism_schema_correct(analysis),
        "missingPanelFailsIncompleteInvalid": incomplete.get("status") == "incomplete_invalid",
        "crossRootPortableAuthorityAndAnalysisDeterministic": cross_root.get("allPortableArtifactsByteIdentical") is True and cross_root.get("noAbsoluteHostRootInScientificAuthority") is True,
        "noMarketV2_4LifecycleProofInherited": prior_gate.get("readyForAuthorizedTopologyCaseStudyLaunch") is True,
        "dispatchAuthorityDisabled": launch.get("dispatchEnabled") is False and policy.get("dispatchEnabled") is False and analysis.get("dispatchEnabled") is False,
        "untouchedConfirmationPendingCannotRescue": analysis.get("untouchedConfirmationStatus") == "pending" and policy.get("untouchedConfirmation") == "pending_same_U_v2_exact_block" and all(block.get("confirmationStatus") == "pending" for block in analysis.get("blocks", {}).values()),
    }
    gate: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "baseCommit": BASE_COMMIT,
        "workerSourceCommit": WORKER_COMMIT,
        "workerImageDigest": WORKER_IMAGE,
        "workerContractSha256": WORKER_CONTRACT,
        "panelUsefulnessPolicySha256": policy["panelUsefulnessPolicySha256"],
        "reducerContractSha256": contract["reducerContractSha256"],
        "parityCorpusSha256": parity["parityCorpusSha256"],
        "analysisSha256": analysis.get("analysisSha256"),
        "dispatchEnabled": False,
        "untouchedConfirmationStatus": "pending",
        "policyRegressionExecution": regressions,
        "evidence": [
            _raw(policy_path, "topology-panel-usefulness-policy-v2.json"),
            _raw(contract_path, "topology-production-reducer-contract-v3.json"),
            _raw(parity_path, "topology-policy-parity-corpus-v2.json"),
            _raw(analysis_path, "topology-production-analysis-v3.json"),
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
    parser.add_argument("--v2-5-authority-root", type=Path, required=True)
    parser.add_argument("--proof-root", type=Path, required=True)
    parser.add_argument("--production-opener", type=Path, required=True)
    parser.add_argument("--cross-root-report", type=Path, required=True)
    parser.add_argument("--prior-v2-4-gate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    gate = build_gate(
        repo_root=args.repo_root.resolve(),
        authority_root=args.authority_root,
        v2_5_authority_root=args.v2_5_authority_root,
        proof_root=args.proof_root,
        opener=args.production_opener,
        cross_root_report_path=args.cross_root_report,
        prior_v2_4_gate_path=args.prior_v2_4_gate,
    )
    args.output.write_text(canonical_json(gate) + "\n", encoding="utf-8", newline="\n")
    if not gate["readyForAuthorizedTopologyCaseStudyLaunch"]:
        raise SystemExit("V2.5 launch gate is not ready")


if __name__ == "__main__":
    main()
