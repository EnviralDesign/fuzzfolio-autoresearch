"""Executable V2.6.2 gate for the production-package worker rebind."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v3 import SCHEMA as ANALYSIS_SCHEMA
from .temporal_qd_topology_production_reducer_v3 import reduce_files_v3
from .temporal_qd_v2_5_launch_gate import _mechanism_schema_correct

SCHEMA = "temporal_qd_topology_v2_6_2_launch_gate_v1"
CHECKPOINT_SCHEMA = "temporal_qd_v5_campaign_input_checkpoint_v1"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain an object")
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


def _head(repo_root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()


def _production_inputs(
    production_root: Path, worker: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    checkpoints: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    expected_worker = (
        worker["contract_hash"],
        worker["image_digest"],
        worker["git_sha"],
    )
    for panel in (1, 2, 3):
        panel_root = production_root / f"panel-{panel}"
        checkpoint = _load(panel_root / "campaign-input-checkpoint.json")
        rows = [
            json.loads(line)
            for line in (panel_root / "screening-run" / "tasks.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        if (
            checkpoint.get("schemaVersion") != CHECKPOINT_SCHEMA
            or checkpoint.get("panelId") != f"panel-{panel}"
            or checkpoint.get("candidateCount") != 12
            or checkpoint.get("windowCount") != 4
            or checkpoint.get("taskCount") != 48
            or len(rows) != 48
        ):
            raise ValueError(f"panel-{panel} is not an exact 12x4 V5 campaign input")
        for row in rows:
            payload = row.get("payload", {})
            actual_worker = (
                payload.get("required_worker_contract_hash"),
                payload.get("required_worker_image_digest"),
                payload.get("required_worker_source_git_commit"),
            )
            if (
                payload.get("schema_version")
                != "temporal_graph_candidate_window_job_v2"
                or actual_worker != expected_worker
            ):
                raise ValueError("production task worker/science binding drifted")
        checkpoints.append(checkpoint)
        tasks.extend(rows)
    return checkpoints, tasks


def build_gate(
    *,
    repo_root: Path,
    production_root: Path,
    authority_root: Path,
    scientific_authority_root: Path,
    v2_5_authority_root: Path,
    proof_root: Path,
    opener: Path,
    worker_contract_path: Path,
    expected_source_commit: str,
    cross_root_report_path: Path,
    generic_mismatch_report_path: Path,
    exact_image_report_path: Path,
    lifecycle_report_path: Path,
    selection_path: Path,
    prior_v2_5_gate_path: Path,
    analysis_path: Path,
) -> dict[str, Any]:
    launch_path = authority_root / "topology-production-launch-control-v1.json"
    mapping_path = authority_root / "topology-production-task-mapping-v1.json"
    rule_path = scientific_authority_root / "topology-replication-survival-rule-v1.json"
    analyzer_path = (
        scientific_authority_root / "topology-post-run-analyzer-contract-v1.json"
    )
    scientific_path = (
        scientific_authority_root.parent
        / "rust-canonical-authority-v2"
        / "topology-scientific-contract-v1.json"
    )
    policy_path = v2_5_authority_root / "topology-panel-usefulness-policy-v2.json"
    contract_path = v2_5_authority_root / "topology-production-reducer-contract-v3.json"
    parity_path = v2_5_authority_root / "topology-policy-parity-corpus-v2.json"

    launch, mapping, rule, analyzer, scientific = map(
        _load, (launch_path, mapping_path, rule_path, analyzer_path, scientific_path)
    )
    policy, contract, parity = map(_load, (policy_path, contract_path, parity_path))
    worker, cross_root, mismatch, exact_image, lifecycle, selection, prior_gate = map(
        _load,
        (
            worker_contract_path,
            cross_root_report_path,
            generic_mismatch_report_path,
            exact_image_report_path,
            lifecycle_report_path,
            selection_path,
            prior_v2_5_gate_path,
        ),
    )
    committed_analysis = _load(analysis_path)

    authorities_self_hashed = all(
        (
            _verify(launch, "launchControlSha256", "launch control"),
            _verify(mapping, "mappingSha256", "task mapping"),
            _verify(rule, "replicationRuleSha256", "replication rule"),
            _verify(analyzer, "analyzerContractSha256", "analyzer contract"),
            _verify(scientific, "scientificContractSha256", "scientific contract"),
            _verify(policy, "panelUsefulnessPolicySha256", "panel policy"),
            _verify(contract, "reducerContractSha256", "reducer contract"),
            _verify(parity, "parityCorpusSha256", "parity corpus"),
            _verify(cross_root, "crossRootReportSha256", "cross-root report"),
            _verify(mismatch, "mismatchReportSha256", "generic mismatch report"),
            _verify(lifecycle, "lifecycleReportSha256", "lifecycle report"),
            _verify(selection, "selectionAuthoritySha256", "selection authority"),
            _verify(prior_gate, "launchGateSha256", "prior V2.5 launch gate"),
            _verify(committed_analysis, "analysisSha256", "production analysis"),
        )
    )
    checkpoints, tasks = _production_inputs(production_root, worker)
    task_ids = [row["task_id"] for row in tasks]
    candidates = {row["payload"]["candidate_id"] for row in tasks}
    checkpoint_paths = [
        proof_root
        / f"panel-{panel}"
        / "campaign-output-local"
        / "campaign-output-checkpoint.json"
        for panel in (1, 2, 3)
    ]
    analysis = reduce_files_v3(
        checkpoints=checkpoint_paths,
        opener=opener,
        launch_control_path=launch_path,
        task_mapping_path=mapping_path,
        replication_rule_path=rule_path,
        scientific_contract_path=scientific_path,
        analyzer_contract_path=analyzer_path,
        panel_policy_path=policy_path,
    )
    incomplete = reduce_files_v3(
        checkpoints=checkpoint_paths[:2],
        opener=opener,
        launch_control_path=launch_path,
        task_mapping_path=mapping_path,
        replication_rule_path=rule_path,
        scientific_contract_path=scientific_path,
        analyzer_contract_path=analyzer_path,
        panel_policy_path=policy_path,
    )
    all_u_v2 = [
        panel["usefulProgressiveInnovationV2"]
        for block in analysis.get("blocks", {}).values()
        for panel in block.get("panelReports", {}).values()
    ]
    exact_blocks = {
        block_id: block.get("arms")
        for block_id, block in analysis.get("blocks", {}).items()
        if block.get("replication", {}).get("inspectedPromising") is True
    }
    selected_blocks = {
        row["blockId"]: row["arms"] for row in selection.get("blocks", [])
    }
    gates = {
        "exactSourceCommit": _head(repo_root) == expected_source_commit,
        "allAuthoritiesAndResultsSelfHashed": authorities_self_hashed,
        "genericCheckpointRejectedByProductionOpener": mismatch.get(
            "genericCheckpointRejectedByV5Opener"
        )
        is True,
        "genericGoNogoCannotSatisfyProductionGate": mismatch.get(
            "genericReadyForTopologyCaseStudyLaunch"
        )
        is True
        and mismatch.get("productionGateSatisfiedByGenericArtifacts") is False,
        "exactThreeV5CampaignInputs": len(checkpoints) == 3
        and all(row.get("schemaVersion") == CHECKPOINT_SCHEMA for row in checkpoints),
        "exact144UniqueTasksAnd12Candidates": len(tasks) == 144
        and len(set(task_ids)) == 144
        and len(candidates) == 12,
        "oneToOneScientificMapping": mapping.get("mappedTaskCount") == 144
        and len(mapping.get("mappings", [])) == 144
        and len({row["oldTaskId"] for row in mapping.get("mappings", [])}) == 144
        and len({row["newTaskId"] for row in mapping.get("mappings", [])}) == 144,
        "exactImageProductionTaskValidation": exact_image.get("validatedTaskCount")
        == 144
        and exact_image.get("fullWorkerExecutionCandidateCount") == 12
        and exact_image.get("runtimeWorkerContractUsed") is True
        and exact_image.get("catalogVerificationExecuted") is True
        and exact_image.get("sourceProfileRewriteCount") == 0
        and exact_image.get("workerContractHash") == worker.get("contract_hash")
        and exact_image.get("workerImageDigest") == worker.get("image_digest"),
        "exactImageNoNetworkMarketGatewayOrDispatch": exact_image.get("networkEnabled")
        is False
        and exact_image.get("marketDataRead") is False
        and exact_image.get("gatewayContact") is False
        and exact_image.get("taskDispatchCount") == 0,
        "productionLifecycleDurableReopenAndTamperClosed": lifecycle.get("panelCount")
        == 3
        and lifecycle.get("taskCount") == 144
        and lifecycle.get("allDurableBeforeAck") is True
        and lifecycle.get("allReopened") is True
        and lifecycle.get("allTamperRejected") is True
        and lifecycle.get("allRecovered") is True,
        "correctedReducerDirectDeterministicIntegration": analysis == committed_analysis
        and analysis.get("schemaVersion") == ANALYSIS_SCHEMA
        and analysis.get("status") == "complete"
        and len(analysis.get("blocks", {})) == 3,
        "panelLocalPredicateIsU_v2": len(all_u_v2) == 9
        and all(
            block.get("replication", {}).get("panelLocalPredicate") == "U_v2"
            for block in analysis.get("blocks", {}).values()
        ),
        "correctedMechanismSchema": _mechanism_schema_correct(analysis),
        "missingPanelFailsIncompleteInvalid": incomplete.get("status")
        == "incomplete_invalid",
        "crossRootCompletePackageDeterministic": cross_root.get(
            "allPortableArtifactsByteIdentical"
        )
        is True
        and cross_root.get("allProductionCheckpointsOpen") is True
        and cross_root.get("noAbsoluteHostRootInPackage") is True,
        "priorV2_5LaunchAuthorityInherited": prior_gate.get(
            "readyForAuthorizedTopologyCaseStudyLaunch"
        )
        is True,
        "untouchedConfirmationSelectsAllAndOnlyInspectedPromising": selection.get(
            "status"
        )
        == "pending"
        and selection.get("confirmationExecutionAuthorized") is False
        and selection.get("dispatchEnabled") is False
        and selected_blocks == exact_blocks,
        "dispatchAuthorityDisabled": launch.get("dispatchEnabled") is False
        and policy.get("dispatchEnabled") is False
        and analysis.get("dispatchEnabled") is False,
    }
    gate: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "sourceCommit": expected_source_commit,
        "workerSourceCommit": worker["git_sha"],
        "workerImageDigest": worker["image_digest"],
        "workerContractSha256": worker["contract_hash"],
        "panelUsefulnessPolicySha256": policy["panelUsefulnessPolicySha256"],
        "reducerContractSha256": contract["reducerContractSha256"],
        "analysisSha256": analysis.get("analysisSha256"),
        "dispatchEnabled": False,
        "confirmationStatus": "pending",
        "evidence": [
            _raw(generic_mismatch_report_path, "generic-v5-checkpoint-mismatch.json"),
            _raw(exact_image_report_path, "exact-image-production-conformance.json"),
            _raw(lifecycle_report_path, "production-lifecycle-report.json"),
            _raw(cross_root_report_path, "cross-root-determinism.json"),
            _raw(selection_path, "untouched-confirmation-selection-v2-6-2.json"),
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
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--scientific-authority-root", type=Path, required=True)
    parser.add_argument("--v2-5-authority-root", type=Path, required=True)
    parser.add_argument("--proof-root", type=Path, required=True)
    parser.add_argument("--production-opener", type=Path, required=True)
    parser.add_argument("--worker-contract", type=Path, required=True)
    parser.add_argument("--expected-source-commit", required=True)
    parser.add_argument("--cross-root-report", type=Path, required=True)
    parser.add_argument("--generic-mismatch-report", type=Path, required=True)
    parser.add_argument("--exact-image-report", type=Path, required=True)
    parser.add_argument("--lifecycle-report", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--prior-v2-5-gate", type=Path, required=True)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    gate = build_gate(
        repo_root=args.repo_root.resolve(),
        production_root=args.production_root.resolve(),
        authority_root=args.authority_root.resolve(),
        scientific_authority_root=args.scientific_authority_root.resolve(),
        v2_5_authority_root=args.v2_5_authority_root.resolve(),
        proof_root=args.proof_root.resolve(),
        opener=args.production_opener.resolve(),
        worker_contract_path=args.worker_contract.resolve(),
        expected_source_commit=args.expected_source_commit,
        cross_root_report_path=args.cross_root_report.resolve(),
        generic_mismatch_report_path=args.generic_mismatch_report.resolve(),
        exact_image_report_path=args.exact_image_report.resolve(),
        lifecycle_report_path=args.lifecycle_report.resolve(),
        selection_path=args.selection.resolve(),
        prior_v2_5_gate_path=args.prior_v2_5_gate.resolve(),
        analysis_path=args.analysis.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(canonical_json(gate) + "\n", encoding="utf-8", newline="\n")
    if not gate["readyForAuthorizedTopologyCaseStudyLaunch"]:
        raise SystemExit("V2.6.2 launch gate is not ready")


if __name__ == "__main__":
    main()
