"""Direct V2.6.3 launch-authority checks for the accepted V2.6.2 package."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v3 import SCHEMA as ANALYSIS_SCHEMA
from .temporal_qd_topology_production_reducer_v3 import reduce_files_v3
from .temporal_qd_v2_5_launch_gate import _mechanism_schema_correct

SCHEMA = "temporal_qd_topology_v2_6_3_launch_gate_v1"
PREREGISTRATION_SCHEMA = (
    "temporal_qd_topology_untouched_confirmation_preregistration_v2_6_3"
)
SELECTION_RESULT_SCHEMA = (
    "temporal_qd_topology_untouched_confirmation_selection_result_v2_6_3"
)
RETRY_BINDING_SCHEMA = "temporal_qd_v2_6_3_retry_closure_binding_v1"
CROSS_ROOT_SCHEMA = "temporal_qd_v2_6_3_cross_root_authority_report_v1"
CHECKPOINT_SCHEMA = "temporal_qd_v5_campaign_input_checkpoint_v1"
FUTURE_WINDOWS = (
    "untouched-2027-q1",
    "untouched-2027-q2",
    "untouched-2027-q3",
    "untouched-2027-q4",
)
ARMS = ("P", "T", "E", "TE")
CROSS_ROOT_REQUIRED_LOGICAL_IDS = {
    "candidate-ledger",
    "candidate-payload-index",
    "inspected-task-index",
    "native-source-manifest",
    "native-authority",
    "scientific-contract",
    "replication-rule",
    "analyzer-contract",
    "panel-policy",
    "reducer-contract",
    "policy-parity-corpus",
    "worker-contract",
    "generic-task-manifest",
    "generic-checkpoint",
    "production-launch-control",
    "production-task-mapping",
    "production-output-templates",
    "production-no-market-analysis",
    "confirmation-preregistration-v2-6-3",
    "retry-closure-binding-v2-6-3",
    "panel-1-campaign-input-checkpoint",
    "panel-1-task-pack",
    "panel-1-cohort-population",
    "panel-2-campaign-input-checkpoint",
    "panel-2-task-pack",
    "panel-2-cohort-population",
    "panel-3-campaign-input-checkpoint",
    "panel-3-task-pack",
    "panel-3-cohort-population",
}


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain an object")
    return value


def _rows(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    if not all(isinstance(row, dict) for row in rows):
        raise TypeError(f"{path} must contain JSON objects")
    return rows


def _container_inspect(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
        return value[0]
    raise TypeError(f"{path} must contain exactly one Docker inspect object")


def _verify(value: dict[str, Any], field: str, label: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field, None)
    if stored != canonical_sha256(unsigned):
        raise ValueError(f"{label} self-hash mismatch")


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


def _semantic_projection(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "analysis_window_start",
        "analysis_window_end",
        "authored_program_sha256",
        "bar_limit",
        "evidence_plan",
        "execution_config_sha256",
        "expected_resolved_profile_snapshot_sha256",
        "expected_resolved_program_sha256",
        "inline_profile_snapshot",
        "instruments",
        "lake_window_semantic_sha256",
        "normalized_profile_snapshot_sha256",
        "precompiled_profile_execution_contract",
        "raw_source_profile_sha256",
        "required_worker_contract_hash",
        "required_worker_contract_schema",
        "required_worker_image_digest",
        "required_worker_runtime_platform_sha256",
        "required_worker_rust_build_info_sha256",
        "required_worker_rust_core_hash",
        "required_worker_source_git_commit",
        "timeframe",
    )
    projected = {key: copy.deepcopy(payload.get(key)) for key in keys}
    evidence = projected.get("evidence_plan")
    if isinstance(evidence, dict) and evidence.get("lake_manifest_sha256") is None:
        evidence.pop("lake_manifest_sha256", None)
    return projected


def _run_v5_opener(opener: Path, checkpoint: Path) -> dict[str, Any]:
    process = subprocess.run(
        [str(opener), "--checkpoint", str(checkpoint)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    value = json.loads(process.stdout)
    if value.get("schemaVersion") != "temporal_qd_v5_campaign_input_open_result_v1":
        raise ValueError("V5 opener returned an incompatible result")
    return value


def _production_inputs(
    production_root: Path, worker: dict[str, Any], opener: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    expected_worker = (
        worker["contract_hash"],
        worker["image_digest"],
        worker["git_sha"],
    )
    opened: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    cohorts: list[dict[str, Any]] = []
    for panel in (1, 2, 3):
        panel_root = production_root / f"panel-{panel}"
        checkpoint_path = panel_root / "campaign-input-checkpoint.json"
        checkpoint = _load(checkpoint_path)
        opened_checkpoint = _run_v5_opener(opener, checkpoint_path)
        rows = _rows(panel_root / "screening-run" / "tasks.jsonl")
        cohort = _load(panel_root / "cohort-population.json")
        if (
            checkpoint.get("schemaVersion") != CHECKPOINT_SCHEMA
            or checkpoint.get("panelId") != f"panel-{panel}"
            or opened_checkpoint.get("panelId") != f"panel-{panel}"
            or opened_checkpoint.get("candidateCount") != 12
            or opened_checkpoint.get("windowCount") != 4
            or opened_checkpoint.get("taskCount") != 48
            or len(rows) != 48
            or cohort.get("panelId") != f"panel-{panel}"
            or cohort.get("candidateCount") != 12
        ):
            raise ValueError(f"panel-{panel} is not the accepted 12x4 V5 input")
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
                raise ValueError("production task worker binding drifted")
        opened.append(opened_checkpoint)
        tasks.extend(rows)
        cohorts.append(cohort)
    return opened, tasks, cohorts


def _candidate_bindings(cohorts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first = cohorts[0].get("candidates")
    if not isinstance(first, list) or len(first) != 12:
        raise ValueError("panel-1 cohort does not contain twelve candidates")
    candidate_sets = [
        canonical_json(sorted(cohort.get("candidates", []), key=lambda row: row["candidateId"]))
        for cohort in cohorts
    ]
    if len(set(candidate_sets)) != 1:
        raise ValueError("candidate cohort drifted across production panels")
    bindings: list[dict[str, Any]] = []
    for row in sorted(first, key=lambda candidate: candidate["candidateId"]):
        bindings.append(
            {
                key: copy.deepcopy(row[key])
                for key in (
                    "candidateId",
                    "candidateIdentitySha256",
                    "candidatePayloadSha256",
                    "blockId",
                    "arm",
                    "parentCandidateId",
                    "programSha256",
                    "sourceProfileSha256",
                    "resolvedProfileSnapshotSha256",
                    "resolvedProgramSha256",
                )
            }
        )
    if {row["arm"] for row in bindings} != set(ARMS):
        raise ValueError("cohort is missing a frozen P/T/E/TE arm")
    if len({row["blockId"] for row in bindings}) != 3:
        raise ValueError("cohort is not the expected three-block topology")
    return bindings


def build_confirmation_preregistration(
    *,
    source_commit: str,
    production_root: Path,
    worker_contract_path: Path,
    launch_control_path: Path,
    task_mapping_path: Path,
    original_preregistration_path: Path,
    worker_opener: Path,
) -> dict[str, Any]:
    worker = _load(worker_contract_path)
    _, _, cohorts = _production_inputs(production_root, worker, worker_opener)
    original = _load(original_preregistration_path)
    _verify(original, "preregistrationSha256", "original confirmation preregistration")
    windows = original.get("windows")
    if not isinstance(windows, list) or tuple(row.get("windowId") for row in windows) != FUTURE_WINDOWS:
        raise ValueError("the accepted four untouched windows drifted")
    launch = _load(launch_control_path)
    mapping = _load(task_mapping_path)
    _verify(launch, "launchControlSha256", "production launch control")
    _verify(mapping, "mappingSha256", "production task mapping")
    value: dict[str, Any] = {
        "schemaVersion": PREREGISTRATION_SCHEMA,
        "sourceCommit": source_commit,
        "status": "pending",
        "dispatchEnabled": False,
        "confirmationExecutionAuthorized": False,
        "productionConfirmed": False,
        "familyLevelInferencePermitted": False,
        "inspectedPanels": ["panel-1", "panel-2", "panel-3"],
        "windows": windows,
        "candidateBindings": _candidate_bindings(cohorts),
        "selectionRule": {
            "eligibleBlockPredicate": (
                "replication.inspectedPromising == true in authenticated real "
                "inspected V3 analysis"
            ),
            "selectedBlocks": "all and only blocks satisfying eligibleBlockPredicate",
            "retainAllArmsForSelectedBlock": list(ARMS),
            "manualSelectionPermitted": False,
            "candidateSubstitutionPermitted": False,
            "replacementBlockPermitted": False,
            "confirmationMayRescueInspectedFailure": False,
            "poolingPermitted": False,
            "majorityVotePermitted": False,
            "panelLocalPredicate": "U_v2",
            "projectedTaskCountFormula": "selectedBlockCount * 4 arms * 4 windows",
            "allowedProjectedTaskCounts": [0, 16, 32, 48],
        },
        "authorityPins": {
            "originalUntouchedPreregistrationSha256": original["preregistrationSha256"],
            "launchControlSha256": launch["launchControlSha256"],
            "taskMappingSha256": mapping["mappingSha256"],
        },
        "workerPins": {
            "sourceCommit": worker["git_sha"],
            "imageDigest": worker["image_digest"],
            "contractSha256": worker["contract_hash"],
        },
    }
    value["preregistrationSha256"] = canonical_sha256(value)
    return value


def validate_confirmation_preregistration(
    value: dict[str, Any], *, expected_worker: dict[str, Any], expected_source_commit: str
) -> None:
    _verify(value, "preregistrationSha256", "confirmation preregistration")
    forbidden = {"analysisSha256", "blocks", "selectedBlockCount", "selectedTaskCount", "tasks"}
    if forbidden.intersection(value):
        raise ValueError("preregistration contains a result-derived selection")
    if value.get("schemaVersion") != PREREGISTRATION_SCHEMA:
        raise ValueError("confirmation preregistration schema drifted")
    if (
        value.get("sourceCommit") != expected_source_commit
        or value.get("status") != "pending"
        or value.get("dispatchEnabled") is not False
        or value.get("confirmationExecutionAuthorized") is not False
        or value.get("productionConfirmed") is not False
        or value.get("familyLevelInferencePermitted") is not False
    ):
        raise ValueError("confirmation preregistration authorization drifted")
    windows = value.get("windows", [])
    if tuple(row.get("windowId") for row in windows) != FUTURE_WINDOWS:
        raise ValueError("confirmation windows drifted")
    if value.get("inspectedPanels") != ["panel-1", "panel-2", "panel-3"]:
        raise ValueError("confirmation inspected-panel binding drifted")
    bindings = value.get("candidateBindings", [])
    if len(bindings) != 12 or {row.get("arm") for row in bindings} != set(ARMS):
        raise ValueError("confirmation candidate bindings drifted")
    if len({row.get("blockId") for row in bindings}) != 3:
        raise ValueError("confirmation block bindings drifted")
    if value.get("workerPins") != {
        "sourceCommit": expected_worker["git_sha"],
        "imageDigest": expected_worker["image_digest"],
        "contractSha256": expected_worker["contract_hash"],
    }:
        raise ValueError("confirmation worker pins drifted")
    rule = value.get("selectionRule", {})
    if (
        rule.get("allowedProjectedTaskCounts") != [0, 16, 32, 48]
        or rule.get("manualSelectionPermitted") is not False
        or rule.get("candidateSubstitutionPermitted") is not False
        or rule.get("replacementBlockPermitted") is not False
        or rule.get("confirmationMayRescueInspectedFailure") is not False
        or rule.get("poolingPermitted") is not False
        or rule.get("majorityVotePermitted") is not False
    ):
        raise ValueError("confirmation selection rule drifted")


def build_selection_result(
    *, preregistration: dict[str, Any], real_inspected_analysis: dict[str, Any]
) -> dict[str, Any]:
    """Build the later result only from a marked real inspected analysis."""
    validate_confirmation_preregistration(
        preregistration,
        expected_worker={
            "git_sha": preregistration["workerPins"]["sourceCommit"],
            "image_digest": preregistration["workerPins"]["imageDigest"],
            "contract_hash": preregistration["workerPins"]["contractSha256"],
        },
        expected_source_commit=preregistration["sourceCommit"],
    )
    if real_inspected_analysis.get("analysisProvenance") != "real_inspected_market":
        raise ValueError("selection result requires real inspected market analysis")
    _verify(real_inspected_analysis, "analysisSha256", "real inspected analysis")
    block_bindings = {
        row["blockId"]: [] for row in preregistration["candidateBindings"]
    }
    for row in preregistration["candidateBindings"]:
        block_bindings[row["blockId"]].append(row["candidateId"])
    selected = [
        block_id
        for block_id, block in sorted(real_inspected_analysis.get("blocks", {}).items())
        if block.get("replication", {}).get("inspectedPromising") is True
    ]
    if any(block_id not in block_bindings for block_id in selected):
        raise ValueError("real analysis selected an unregistered block")
    projected = [
        {"blockId": block_id, "candidateIds": sorted(block_bindings[block_id])}
        for block_id in selected
    ]
    value: dict[str, Any] = {
        "schemaVersion": SELECTION_RESULT_SCHEMA,
        "preregistrationSha256": preregistration["preregistrationSha256"],
        "realInspectedAnalysisSha256": real_inspected_analysis["analysisSha256"],
        "status": "pending_human_confirmation_authorization",
        "dispatchEnabled": False,
        "confirmationExecutionAuthorized": False,
        "selectedBlocks": projected,
        "projectedTaskCount": len(projected) * 4 * 4,
    }
    if value["projectedTaskCount"] not in (0, 16, 32, 48):
        raise ValueError("selection result task count is outside the preregistered range")
    value["selectionResultSha256"] = canonical_sha256(value)
    return value


def build_retry_binding(
    *,
    fuzzfolio_commit: str,
    worker_contract: dict[str, Any],
    evidence_paths: Mapping[str, Path],
) -> dict[str, Any]:
    required = {
        "worker-seam-conformance.json",
        "production-admission.json",
        "fuzzfolio-adversarial.json",
        "isolated-worker-fake-transport.log",
        "mutation-failure-path-test.py",
    }
    if set(evidence_paths) != required:
        raise ValueError("retry closure evidence inventory drifted")
    value: dict[str, Any] = {
        "schemaVersion": RETRY_BINDING_SCHEMA,
        "fuzzfolioSourceCommit": fuzzfolio_commit,
        "workerPins": {
            "sourceCommit": worker_contract["git_sha"],
            "imageDigest": worker_contract["image_digest"],
            "contractSha256": worker_contract["contract_hash"],
        },
        "immutableFailureClasses": [
            "malformed_v2_execution_contract",
            "exact_catalog_incompatibility",
            "program_or_profile_identity_mismatch",
            "worker_contract_or_capability_mismatch",
        ],
        "requiredOutcomes": {
            "immutableRetryable": False,
            "exactlyOneTerminalFailureSend": True,
            "requeueScheduled": False,
            "representativeTransientFailuresRemainRetryable": True,
            "executionPath": "isolated_worker_fake_transport",
        },
        "evidence": [
            _raw(evidence_paths[logical_id], logical_id)
            for logical_id in sorted(evidence_paths)
        ],
    }
    value["retryEvidenceBindingSha256"] = canonical_sha256(value)
    return value


def validate_retry_binding(
    value: dict[str, Any], *, worker: dict[str, Any], evidence_paths: Mapping[str, Path]
) -> None:
    _verify(value, "retryEvidenceBindingSha256", "retry closure binding")
    if value.get("schemaVersion") != RETRY_BINDING_SCHEMA:
        raise ValueError("retry closure binding schema drifted")
    if value.get("fuzzfolioSourceCommit") != worker["git_sha"]:
        raise ValueError("retry closure source commit drifted")
    if value.get("workerPins") != {
        "sourceCommit": worker["git_sha"],
        "imageDigest": worker["image_digest"],
        "contractSha256": worker["contract_hash"],
    }:
        raise ValueError("retry closure worker binding drifted")
    expected_names = {
        "worker-seam-conformance.json",
        "production-admission.json",
        "fuzzfolio-adversarial.json",
        "isolated-worker-fake-transport.log",
        "mutation-failure-path-test.py",
    }
    if set(evidence_paths) != expected_names:
        raise ValueError("retry closure evidence inventory drifted")
    expected = [_raw(evidence_paths[key], key) for key in sorted(evidence_paths)]
    if value.get("evidence") != expected:
        raise ValueError("retry closure evidence raw identity drifted")
    outcomes = value.get("requiredOutcomes")
    if outcomes != {
        "immutableRetryable": False,
        "exactlyOneTerminalFailureSend": True,
        "requeueScheduled": False,
        "representativeTransientFailuresRemainRetryable": True,
        "executionPath": "isolated_worker_fake_transport",
    }:
        raise ValueError("retry closure outcome contract drifted")
    seam = _load(evidence_paths["worker-seam-conformance.json"])
    admission = _load(evidence_paths["production-admission.json"])
    fuzzfolio = _load(evidence_paths["fuzzfolio-adversarial.json"])
    _verify(seam, "reportSha256", "worker seam conformance")
    _verify(admission, "reportSha256", "production admission")
    for report, label in ((seam, "worker seam"), (admission, "production admission")):
        if (
            report.get("workerContractHash") != worker["contract_hash"]
            or report.get("workerImageDigest") != worker["image_digest"]
            or report.get("taskDispatchCount") != 0
        ):
            raise ValueError(f"{label} worker binding drifted")
    cases = fuzzfolio.get("cases")
    if (
        fuzzfolio.get("schemaVersion") != "temporal_qd_fuzzfolio_admission_batch_v1"
        or not isinstance(cases, list)
        or sum(row.get("admitted") is True for row in cases) != 12
        or any(
            row.get("admitted") is not False
            for row in cases
            if not str(row.get("case", "")).startswith("exact:")
        )
    ):
        raise ValueError("fuzzfolio adversarial admission evidence drifted")
    source = evidence_paths["mutation-failure-path-test.py"].read_text(encoding="utf-8")
    log = evidence_paths["isolated-worker-fake-transport.log"].read_text(encoding="utf-8")
    required_source_fragments = (
        "RemoteLakeMutationInProgress",
        "test_immutable_contract_failure_crosses_real_isolation_once_without_requeue",
        "isolated_error.retryable is False",
        "assert len(calls) == 1",
        "TemporalGraphReplayContractError",
    )
    if "4 passed" not in log or not all(fragment in source for fragment in required_source_fragments):
        raise ValueError("isolated retry failure evidence drifted")


def build_cross_root_report(
    *, left_root: Path, right_root: Path, artifacts: Mapping[str, tuple[Path, Path]]
) -> dict[str, Any]:
    pairs = []
    forbidden = (
        str(left_root.resolve()).replace("\\", "/").encode(),
        str(right_root.resolve()).replace("\\", "/").encode(),
    )
    no_absolute_host_root = True
    for logical_id, (left, right) in sorted(artifacts.items()):
        left_bytes = left.read_bytes()
        right_bytes = right.read_bytes()
        no_absolute_host_root = no_absolute_host_root and all(
            marker not in left_bytes.replace(b"\\", b"/")
            and marker not in right_bytes.replace(b"\\", b"/")
            for marker in forbidden
        )
        pairs.append(
            {
                "logicalId": logical_id,
                "leftRawSha256": "sha256:" + hashlib.sha256(left_bytes).hexdigest(),
                "rightRawSha256": "sha256:" + hashlib.sha256(right_bytes).hexdigest(),
                "leftSizeBytes": len(left_bytes),
                "rightSizeBytes": len(right_bytes),
                "byteIdentical": left_bytes == right_bytes,
            }
        )
    value: dict[str, Any] = {
        "schemaVersion": CROSS_ROOT_SCHEMA,
        "artifactInventory": pairs,
        "allPortableArtifactsByteIdentical": all(row["byteIdentical"] for row in pairs),
        "noAbsoluteHostRootInPortableAuthority": no_absolute_host_root,
        "rootBoundExcludedArtifacts": [
            ".native-v5-campaign-freeze-manifest.json",
            "campaign-output-checkpoint.json",
            "gateway-local-output",
        ],
    }
    value["crossRootReportSha256"] = canonical_sha256(value)
    return value


def validate_cross_root_report(value: dict[str, Any]) -> None:
    _verify(value, "crossRootReportSha256", "cross-root authority report")
    if (
        value.get("schemaVersion") != CROSS_ROOT_SCHEMA
        or value.get("allPortableArtifactsByteIdentical") is not True
        or value.get("noAbsoluteHostRootInPortableAuthority") is not True
        or not value.get("artifactInventory")
        or not CROSS_ROOT_REQUIRED_LOGICAL_IDS.issubset(
            {row.get("logicalId") for row in value["artifactInventory"]}
        )
    ):
        raise ValueError("cross-root authority proof failed")


def _direct_mapping(
    *, generic_manifest_path: Path, tasks: list[dict[str, Any]], mapping_path: Path
) -> bool:
    generic = _load(generic_manifest_path)
    mapping = _load(mapping_path)
    _verify(mapping, "mappingSha256", "production task mapping")
    old_rows = generic.get("tasks")
    if not isinstance(old_rows, list) or len(old_rows) != 144 or len(tasks) != 144:
        raise ValueError("generic or production task cardinality drifted")
    old_by_id = {row["task_id"]: row for row in old_rows}
    new_by_id = {row["task_id"]: row for row in tasks}
    mappings = mapping.get("mappings")
    if not isinstance(mappings, list) or len(mappings) != 144:
        raise ValueError("mapping rows drifted")
    pairs = {(row["oldTaskId"], row["newTaskId"]) for row in mappings}
    if len(pairs) != 144 or {left for left, _ in pairs} != set(old_by_id) or {
        right for _, right in pairs
    } != set(new_by_id):
        raise ValueError("mapping does not cover the exact 144x144 identity")
    for old_id, new_id in pairs:
        if _semantic_projection(old_by_id[old_id]["payload"]) != _semantic_projection(
            new_by_id[new_id]["payload"]
        ):
            raise ValueError("production semantic mapping drifted")
    return True


def _verify_exact_image(
    *,
    conformance_path: Path,
    container_inspect_path: Path,
    tasks: list[dict[str, Any]],
    worker: dict[str, Any],
    expected_platform_digest: str,
) -> bool:
    report = _load(conformance_path)
    inspect = _container_inspect(container_inspect_path)
    candidate_ids = {row["payload"]["candidate_id"] for row in tasks}
    executed = report.get("executedFixtures", [])
    executed_candidate_ids = {
        row.get("task", {}).get("payload", {}).get("candidate_id")
        for row in executed
        if isinstance(row, dict) and isinstance(row.get("task"), dict)
    }
    host_config = inspect.get("HostConfig", {})
    descriptor = inspect.get("ImageManifestDescriptor", {})
    config = inspect.get("Config", {})
    state = inspect.get("State", {})
    mounts = host_config.get("Mounts") or []
    expected_bind_targets = {"/inputs/task-manifest.json", "/inputs/worker-contract.json"}
    if (
        report.get("validatedTaskCount") != 144
        or report.get("fullWorkerExecutionCandidateCount") != 12
        or report.get("runtimeWorkerContractUsed") is not True
        or report.get("catalogVerificationExecuted") is not True
        or report.get("sourceProfileRewriteCount") != 0
        or report.get("workerContractHash") != worker["contract_hash"]
        or report.get("workerImageDigest") != worker["image_digest"]
        or report.get("networkEnabled") is not False
        or report.get("marketDataRead") is not False
        or report.get("gatewayContact") is not False
        or report.get("taskDispatchCount") != 0
        or executed_candidate_ids != candidate_ids
        or inspect.get("Image") != worker["image_digest"]
        or descriptor.get("digest") != expected_platform_digest
        or descriptor.get("platform") != {"architecture": "amd64", "os": "linux"}
        or host_config.get("NetworkMode") != "none"
        or state.get("ExitCode") != 0
        or config.get("Image") != f"lucasmorgan/fuzzfolio-replay-worker@{worker['image_digest']}"
        or {mount.get("Target") for mount in mounts} != expected_bind_targets
        or not all(mount.get("ReadOnly") is True for mount in mounts)
    ):
        raise ValueError("exact-image production evidence drifted")
    return True


def _direct_lifecycle_and_reducer(
    *,
    proof_root: Path,
    opener: Path,
    launch_control_path: Path,
    task_mapping_path: Path,
    replication_rule_path: Path,
    scientific_contract_path: Path,
    analyzer_contract_path: Path,
    panel_policy_path: Path,
    committed_analysis_path: Path,
    lifecycle_report_path: Path,
) -> tuple[dict[str, Any], bool]:
    lifecycle = _load(lifecycle_report_path)
    _verify(lifecycle, "lifecycleReportSha256", "production lifecycle report")
    checkpoints = [
        proof_root / f"panel-{panel}" / "campaign-output-local" / "campaign-output-checkpoint.json"
        for panel in (1, 2, 3)
    ]
    analysis = reduce_files_v3(
        checkpoints=checkpoints,
        opener=opener,
        launch_control_path=launch_control_path,
        task_mapping_path=task_mapping_path,
        replication_rule_path=replication_rule_path,
        scientific_contract_path=scientific_contract_path,
        analyzer_contract_path=analyzer_contract_path,
        panel_policy_path=panel_policy_path,
    )
    incomplete = reduce_files_v3(
        checkpoints=checkpoints[:2],
        opener=opener,
        launch_control_path=launch_control_path,
        task_mapping_path=task_mapping_path,
        replication_rule_path=replication_rule_path,
        scientific_contract_path=scientific_contract_path,
        analyzer_contract_path=analyzer_contract_path,
        panel_policy_path=panel_policy_path,
    )
    committed = _load(committed_analysis_path)
    _verify(committed, "analysisSha256", "committed no-market analysis")
    panel_rows = lifecycle.get("panels", [])
    if (
        lifecycle.get("panelCount") != 3
        or lifecycle.get("taskCount") != 144
        or lifecycle.get("marketDataRead") is not False
        or lifecycle.get("realGatewayContact") is not False
        or lifecycle.get("fakeLoopbackGatewayOnly") is not True
        or lifecycle.get("dispatchAuthorized") is not False
        or not all(lifecycle.get(key) is True for key in (
            "allDurableBeforeAck", "allReopened", "allTamperRejected", "allRecovered"
        ))
        or len(panel_rows) != 3
        or {row.get("panelId") for row in panel_rows} != {"panel-1", "panel-2", "panel-3"}
        or not all(row.get("taskCount") == 48 for row in panel_rows)
        or analysis != committed
        or analysis.get("schemaVersion") != ANALYSIS_SCHEMA
        or analysis.get("status") != "complete"
        or len(analysis.get("blocks", {})) != 3
        or incomplete.get("status") != "incomplete_invalid"
        or not _mechanism_schema_correct(analysis)
    ):
        raise ValueError("direct production lifecycle or reducer verification failed")
    return analysis, True


def build_gate(
    *,
    repo_root: Path,
    expected_source_commit: str,
    production_root: Path,
    generic_manifest_path: Path,
    authority_root: Path,
    scientific_authority_root: Path,
    v2_5_authority_root: Path,
    proof_root: Path,
    campaign_input_opener: Path,
    campaign_output_opener: Path,
    worker_contract_path: Path,
    prior_v2_6_2_gate_path: Path,
    generic_checkpoint_path: Path,
    exact_image_report_path: Path,
    container_inspect_path: Path,
    lifecycle_report_path: Path,
    preregistration_path: Path,
    retry_binding_path: Path,
    retry_evidence_paths: Mapping[str, Path],
    cross_root_report_path: Path,
    committed_analysis_path: Path,
    expected_platform_digest: str,
) -> dict[str, Any]:
    launch_path = authority_root / "topology-production-launch-control-v1.json"
    mapping_path = authority_root / "topology-production-task-mapping-v1.json"
    rule_path = scientific_authority_root / "topology-replication-survival-rule-v1.json"
    analyzer_path = scientific_authority_root / "topology-post-run-analyzer-contract-v1.json"
    scientific_path = scientific_authority_root.parent / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json"
    policy_path = v2_5_authority_root / "topology-panel-usefulness-policy-v2.json"
    reducer_contract_path = v2_5_authority_root / "topology-production-reducer-contract-v3.json"
    parity_path = v2_5_authority_root / "topology-policy-parity-corpus-v2.json"
    launch, mapping, rule, analyzer, scientific, policy, reducer_contract, parity = map(
        _load,
        (
            launch_path,
            mapping_path,
            rule_path,
            analyzer_path,
            scientific_path,
            policy_path,
            reducer_contract_path,
            parity_path,
        ),
    )
    for value, field, label in (
        (launch, "launchControlSha256", "launch control"),
        (mapping, "mappingSha256", "task mapping"),
        (rule, "replicationRuleSha256", "replication rule"),
        (analyzer, "analyzerContractSha256", "analyzer contract"),
        (scientific, "scientificContractSha256", "scientific contract"),
        (policy, "panelUsefulnessPolicySha256", "panel policy"),
        (reducer_contract, "reducerContractSha256", "reducer contract"),
        (parity, "parityCorpusSha256", "parity corpus"),
    ):
        _verify(value, field, label)
    worker = _load(worker_contract_path)
    prior_v2_6_2_gate = _load(prior_v2_6_2_gate_path)
    _verify(prior_v2_6_2_gate, "launchGateSha256", "accepted V2.6.2 gate")
    accepted_v2_6_2 = (
        prior_v2_6_2_gate.get("schemaVersion")
        == "temporal_qd_topology_v2_6_2_launch_gate_v1"
        and prior_v2_6_2_gate.get("sourceCommit")
        == "aa04a1057e54c7991e3dcde33f846f52fb311a4a"
        and prior_v2_6_2_gate.get("readyForAuthorizedTopologyCaseStudyLaunch") is True
    )
    opened, tasks, _ = _production_inputs(production_root, worker, campaign_input_opener)
    preregistration = _load(preregistration_path)
    validate_confirmation_preregistration(
        preregistration,
        expected_worker=worker,
        expected_source_commit=expected_source_commit,
    )
    retry_binding = _load(retry_binding_path)
    validate_retry_binding(retry_binding, worker=worker, evidence_paths=retry_evidence_paths)
    cross_root = _load(cross_root_report_path)
    validate_cross_root_report(cross_root)
    try:
        _run_v5_opener(campaign_input_opener, generic_checkpoint_path)
        generic_rejected = False
    except subprocess.CalledProcessError:
        generic_rejected = True
    mapping_direct = _direct_mapping(
        generic_manifest_path=generic_manifest_path, tasks=tasks, mapping_path=mapping_path
    )
    exact_image_direct = _verify_exact_image(
        conformance_path=exact_image_report_path,
        container_inspect_path=container_inspect_path,
        tasks=tasks,
        worker=worker,
        expected_platform_digest=expected_platform_digest,
    )
    analysis, lifecycle_direct = _direct_lifecycle_and_reducer(
        proof_root=proof_root,
        opener=campaign_output_opener,
        launch_control_path=launch_path,
        task_mapping_path=mapping_path,
        replication_rule_path=rule_path,
        scientific_contract_path=scientific_path,
        analyzer_contract_path=analyzer_path,
        panel_policy_path=policy_path,
        committed_analysis_path=committed_analysis_path,
        lifecycle_report_path=lifecycle_report_path,
    )
    all_u_v2 = [
        panel["usefulProgressiveInnovationV2"]
        for block in analysis.get("blocks", {}).values()
        for panel in block.get("panelReports", {}).values()
    ]
    gates = {
        "exactSourceCommit": _head(repo_root) == expected_source_commit,
        "acceptedV262PreconditionBound": accepted_v2_6_2,
        "threeRealV5InputsOpenedDirectly": len(opened) == 3,
        "genericCheckpointRejectedByActualV5Opener": generic_rejected,
        "exact144UniqueTasksAnd12Candidates": len(tasks) == 144
        and len({row["task_id"] for row in tasks}) == 144
        and len({row["payload"]["candidate_id"] for row in tasks}) == 12,
        "oneToOneScientificMappingRecomputed": mapping_direct,
        "exactImageEvidenceBoundDirectly": exact_image_direct,
        "productionLifecycleBoundDirectly": lifecycle_direct,
        "reducerAndPolicyRecomputed": len(all_u_v2) == 9
        and all(
            block.get("replication", {}).get("panelLocalPredicate") == "U_v2"
            for block in analysis.get("blocks", {}).values()
        ),
        "confirmationIsResultIndependentPreregistration": True,
        "retryClosureBoundAndVerified": True,
        "crossRootPortableAuthorityBound": True,
        "dispatchAuthorityDisabled": launch.get("dispatchEnabled") is False
        and policy.get("dispatchEnabled") is False
        and analysis.get("dispatchEnabled") is False
        and preregistration.get("dispatchEnabled") is False,
    }
    gate: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "sourceCommit": expected_source_commit,
        "workerSourceCommit": worker["git_sha"],
        "workerImageDigest": worker["image_digest"],
        "workerPlatformManifestDigest": expected_platform_digest,
        "workerContractSha256": worker["contract_hash"],
        "dispatchEnabled": False,
        "confirmationStatus": "pending",
        "analysisSha256": analysis["analysisSha256"],
        "preregistrationSha256": preregistration["preregistrationSha256"],
        "retryEvidenceBindingSha256": retry_binding["retryEvidenceBindingSha256"],
        "crossRootAuthorityReportSha256": cross_root["crossRootReportSha256"],
        "evidence": [
            _raw(prior_v2_6_2_gate_path, "topology-production-launch-gate-v2-6-2.json"),
            _raw(exact_image_report_path, "exact-image-production-conformance.json"),
            _raw(container_inspect_path, "exact-image-production-container-inspect.json"),
            _raw(lifecycle_report_path, "production-lifecycle-report.json"),
            _raw(preregistration_path, "untouched-confirmation-preregistration-v2-6-3.json"),
            _raw(retry_binding_path, "retry-closure-binding-v2-6-3.json"),
            _raw(cross_root_report_path, "cross-root-authority-inputs-v2-6-3.json"),
            _raw(committed_analysis_path, "topology-production-analysis-v3.json"),
        ],
        "gates": gates,
        "readyForAuthorizedTopologyCaseStudyLaunch": all(gates.values()),
    }
    gate["launchGateSha256"] = canonical_sha256(gate)
    return gate


def write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")
