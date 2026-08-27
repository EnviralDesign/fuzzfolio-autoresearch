"""Production-shaped, no-dispatch V2.3 topology campaign package."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .evolvable_module_qd_authority import evolvable_behavior_attribution_requirement
from .temporal_qd_campaign_native import freeze_qd_v5_campaign_native
from .temporal_qd_evolution import directional_qd_archive_policy_authority
from .temporal_qd_rotating_evidence import build_rotating_evidence_contract
from .temporal_search import build_authority

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "research/temporal-qd/rust-canonical-authority-v2"
ROTATING = ROOT / "runs/temporal-qd-v5-native-4000x1024x5-20260813-v1/authority/rotating-evidence"
WORKER_SHA = "sha256:ae5d0e53aa19e1e241468c009e248457560ca63e2e3d785854750b028736c9df"
SOURCE_COMMIT = "2b3ea1bea86676e621383651d1104608082786bc"


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def _write_pretty(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _raw_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate_rows(
    generic: dict[str, Any], *, authority_package_root: Path = V2
) -> list[dict[str, Any]]:
    envelopes = _load(authority_package_root / "native-candidate-envelopes-v1.json")
    metadata = {row["candidateId"]: row for row in envelopes["candidates"]}
    first: dict[str, dict[str, Any]] = {}
    for task in generic["tasks"]:
        payload = task["payload"]
        first.setdefault(payload["candidate_id"], payload)
    rows = []
    for candidate_id in sorted(first):
        payload = first[candidate_id]
        contract = payload["precompiled_profile_execution_contract"]
        profile = payload["inline_profile_snapshot"]
        profile_sha = canonical_sha256(profile)
        if profile_sha != payload["raw_source_profile_sha256"]:
            raise RuntimeError(f"raw source profile drift for {candidate_id}")
        rows.append(
            {
                "candidateId": candidate_id,
                "candidateIdentitySha256": metadata[candidate_id]["candidateIdentitySha256"],
                "sourceMode": "rust_canonical_topology_preregistered_arm",
                "seedId": metadata[candidate_id]["parentCandidateId"],
                "parentCandidateId": metadata[candidate_id]["parentCandidateId"],
                "blockId": metadata[candidate_id]["blockId"],
                "arm": metadata[candidate_id]["arm"],
                "programSha256": payload["authored_program_sha256"],
                "sourceProfile": profile,
                "sourceProfileSha256": profile_sha,
                "candidatePayloadSha256": contract["candidatePayloadSha256"],
                "nativeAuthoritySha256": contract["authoritySha256"],
                "precompiledProfileExecutionContract": contract,
                "profileSnapshotSha256": contract["profileSnapshotSha256"],
                "resolvedProfileSnapshotSha256": contract["expectedResolvedProfileSnapshotSha256"],
                "resolvedProgramSha256": contract["expectedResolvedProgramSha256"],
            }
        )
    if len(rows) != 12:
        raise RuntimeError("V2.3 requires exactly twelve candidates")
    return rows


def _population(candidates: list[dict[str, Any]], generation: int) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schemaVersion": "temporal_qd_evaluation_population_v1",
        "generationIndex": generation,
        "populationSha256": canonical_sha256(
            {"schemaVersion": "temporal_qd_v2_3_topology_population_v1", "candidates": candidates}
        ),
        "candidates": candidates,
    }
    value["evaluationPopulationSha256"] = canonical_sha256(value)
    return value


def _template_authority_id(template: dict[str, Any]) -> str:
    projected = copy.deepcopy(template)
    worker = projected["workerContract"]
    projected["workerContract"] = {
        "workerContractSha256": worker["workerContractSha256"],
        "workerContractSchema": worker["workerContractSchema"],
    }
    authority = build_authority(projected)
    authority["workerContract"] = copy.deepcopy(worker)
    normalized = {
        "schemaVersion": template["schemaVersion"],
        "authorityLabel": template["authorityLabel"],
        "workerContract": worker,
        "bounds": template["bounds"],
        "prohibitedEvidence": template["prohibitedEvidence"],
        "developmentWindows": template["developmentWindows"],
        "candidates": authority["candidates"],
    }
    authority["preparationSha256"] = canonical_sha256(normalized)
    authority.pop("authorityId", None)
    return canonical_sha256(authority)


def _templates(
    input_root: Path,
    worker: dict[str, Any],
    *,
    rotating_evidence_root: Path = ROTATING,
) -> tuple[dict[str, Any], dict[str, Path]]:
    raw_config = _load(rotating_evidence_root / "rotating-evidence-config.json")
    paths: dict[str, Path] = {}
    descriptors: dict[str, Any] = {}
    for index in range(1, 5):
        panel_id = f"panel-{index}"
        template = _load(rotating_evidence_root / f"{panel_id}-template-preparation.json")
        template["authorityLabel"] = f"rust-canonical-topology-v2-3-{panel_id}"
        template["workerContract"] = copy.deepcopy(worker)
        template["bounds"].update({"maxCandidates": 12, "maxDevelopmentWindows": 4, "maxTasks": 48})
        path = input_root / f"{panel_id}-template-preparation.json"
        _write_pretty(path, template)
        paths[panel_id] = path
        descriptors[panel_id] = {
            "path": path.name,
            "preparationSha256": canonical_sha256(template),
            "authorityId": _template_authority_id(template),
        }
    raw_config["provisionalSurvivorCount"] = 12
    raw_config["breederWidth"] = min(int(raw_config["breederWidth"]), 12)
    raw_config["panelTemplates"] = descriptors
    return build_rotating_evidence_contract(raw_config), paths


def _semantic_projection(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "analysis_window_start", "analysis_window_end", "authored_program_sha256",
        "bar_limit", "evidence_plan", "execution_config_sha256",
        "expected_resolved_profile_snapshot_sha256", "expected_resolved_program_sha256",
        "inline_profile_snapshot", "instruments", "lake_window_semantic_sha256",
        "normalized_profile_snapshot_sha256", "precompiled_profile_execution_contract",
        "raw_source_profile_sha256", "required_worker_contract_hash",
        "required_worker_contract_schema", "required_worker_image_digest",
        "required_worker_runtime_platform_sha256", "required_worker_rust_build_info_sha256",
        "required_worker_rust_core_hash", "required_worker_source_git_commit", "timeframe",
    )
    projected = {key: copy.deepcopy(payload.get(key)) for key in keys}
    evidence = projected.get("evidence_plan")
    if isinstance(evidence, dict) and evidence.get("lake_manifest_sha256") is None:
        evidence.pop("lake_manifest_sha256", None)
    return projected


def _campaign_output_templates(
    *, output_root: Path, rotating: dict[str, Any], panels: list[dict[str, Any]]
) -> dict[str, Any]:
    panel_authority = {row["panelId"]: row for row in rotating["panels"]}
    templates = []
    for panel in panels:
        panel_id = panel["panelId"]
        cohort = _load(output_root / panel_id / "cohort-population.json")
        templates.append(
            {
                "panelId": panel_id,
                "generationIndex": int(panel_id.removeprefix("panel-")),
                "campaignRole": "proposal_current_panel",
                "campaignInputCheckpointPath": panel["checkpointPath"],
                "campaignInputCheckpointSha256": panel["checkpointSha256"],
                "gatewayExecutionReceiptPathTemplate": (
                    f"gateway/{panel_id}/.native-gateway-dispatch/execution-receipt.json"
                ),
                "outputRootTemplate": f"campaign-output/{panel_id}",
                "rotatingEvidenceSha256": rotating["rotatingEvidenceSha256"],
                "panel": panel_authority[panel_id],
                "cohortSource": {
                    "kind": "proposal_evaluation_population",
                    "sourceSemanticSha256": cohort["populationSha256"],
                    "candidateCount": cohort["candidateCount"],
                    "selectionSha256": None,
                },
                "minimumTotalTrades": 8,
                "minimumTradesPerWindow": 4,
                "capTrades": 20,
                "provisionalLimit": 12,
                "runtimeAuthoritySha256Disposition": "bind_at_launch_after_binary_build",
                "gatewayExecutionReceiptSha256Disposition": "bind_after_local_or_authorized_gateway_completion",
            }
        )
    value: dict[str, Any] = {
        "schemaVersion": "temporal_qd_topology_v2_3_campaign_output_templates_v1",
        "dispatchEnabled": False,
        "templates": templates,
    }
    value["templatesSha256"] = canonical_sha256(value)
    return value


def generate(
    *,
    generic_manifest: Path,
    output_root: Path,
    native_binary: Path,
    compact_root: Path | None = None,
    authority_package_root: Path = V2,
    rotating_evidence_root: Path = ROTATING,
    execution_engine_commit: str = SOURCE_COMMIT,
) -> dict[str, Any]:
    if output_root.exists():
        raise RuntimeError("V2.3 output root already exists")
    generic = _load(generic_manifest)
    candidates = _candidate_rows(generic, authority_package_root=authority_package_root)
    authority = _load(authority_package_root / "inspected-campaign-authority-v1.json")
    worker = authority["workerContract"]
    worker_sha = worker["workerContractSha256"]
    worker_bindings = {
        (
            row["payload"].get("required_worker_contract_hash"),
            row["payload"].get("required_worker_image_digest"),
            row["payload"].get("required_worker_source_git_commit"),
        )
        for row in generic["tasks"]
    }
    expected_worker_binding = {
        (worker_sha, worker["imageDigest"], worker["sourceGitCommit"])
    }
    if worker_bindings != expected_worker_binding:
        raise RuntimeError("generic task worker binding does not match authority package")
    input_root = output_root / "freeze-inputs"
    input_root.mkdir(parents=True)
    rotating, templates = _templates(
        input_root, worker, rotating_evidence_root=rotating_evidence_root
    )
    catalog_path = input_root / "construction-catalog.json"
    _write_pretty(catalog_path, _load(rotating_evidence_root / "construction-catalog.json"))
    catalog_sha = canonical_sha256(_load(catalog_path))
    panels = []
    new_tasks: list[dict[str, Any]] = []
    for generation in (1, 2, 3):
        panel_id = f"panel-{generation}"
        population_path = input_root / f"{panel_id}-evaluation-population.json"
        _write(population_path, _population(candidates, generation))
        panel_root = output_root / panel_id
        freeze_qd_v5_campaign_native(
            evaluation_population_path=population_path,
            evaluation_population_raw_sha256=_raw_sha(population_path),
            template_preparation_path=templates[panel_id],
            template_preparation_sha256=canonical_sha256(_load(templates[panel_id])),
            construction_catalog_path=catalog_path,
            construction_catalog_sha256=catalog_sha,
            output_root=panel_root,
            execution_engine_commit=execution_engine_commit,
            worker_contract_sha256=worker_sha,
            rotating_evidence=rotating,
            archive_policy_authority=directional_qd_archive_policy_authority(),
            behavior_attribution_requirement=evolvable_behavior_attribution_requirement(),
            campaign_role="proposal_current_panel",
            panel_id=panel_id,
            native_binary=native_binary,
        )
        checkpoint = _load(panel_root / "campaign-input-checkpoint.json")
        rows = [json.loads(line) for line in (panel_root / "screening-run/tasks.jsonl").read_text().splitlines()]
        if checkpoint["taskCount"] != 48 or len(rows) != 48:
            raise RuntimeError(f"{panel_id} task count drift")
        if {row["payload"]["schema_version"] for row in rows} != {"temporal_graph_candidate_window_job_v2"}:
            raise RuntimeError(f"{panel_id} did not preserve V2 tasks")
        new_tasks.extend(rows)
        panels.append(
            {
                "panelId": panel_id,
                "checkpointPath": f"{panel_id}/campaign-input-checkpoint.json",
                "checkpointSha256": checkpoint["checkpointSha256"],
                "taskMatrixSha256": checkpoint["taskMatrixSha256"],
                "candidateCount": checkpoint["candidateCount"],
                "windowCount": checkpoint["windowCount"],
                "taskCount": checkpoint["taskCount"],
            }
        )
    old_by_key = {(row["payload"]["candidate_id"], row["payload"]["window_id"]): row for row in generic["tasks"]}
    mappings = []
    for row in new_tasks:
        payload = row["payload"]
        # Production V5 binds panel separately; the old generic wrapper prefixed it into window_id.
        candidates_old = [item for (cid, wid), item in old_by_key.items() if cid == payload["candidate_id"] and wid.endswith("-" + payload["window_id"])]
        if len(candidates_old) != 1:
            raise RuntimeError("generic-to-production task mapping is not one-to-one")
        old = candidates_old[0]
        if _semantic_projection(old["payload"]) != _semantic_projection(payload):
            raise RuntimeError("candidate/evidence semantic drift in V2.3 mapping")
        mappings.append({
            "oldTaskId": old["task_id"], "newTaskId": row["task_id"],
            "candidateId": payload["candidate_id"], "oldWindowId": old["payload"]["window_id"],
            "newWindowId": payload["window_id"],
        })
    if len(mappings) != 144 or len({row["oldTaskId"] for row in mappings}) != 144:
        raise RuntimeError("V2.3 task mapping cardinality drift")
    mapping_report: dict[str, Any] = {
        "schemaVersion": "temporal_qd_topology_v2_3_task_mapping_v1",
        "oldTaskMatrixSha256": generic["taskMatrixSha256"],
        "mappedTaskCount": 144,
        "intentionalWrapperChanges": [
            "panel_id_is_a_campaign_checkpoint_binding_instead_of_a_window_id_prefix",
            "campaign_authority_task_and_attempt_ids_are_rederived_by_the_v5_freezer",
            "production_behavior_attribution_request_and_capability_are_added_without_economic_semantic_change",
            "cohort_reporting_metadata_binds_each_candidate_to_its_preregistered_block_arm_and_parent",
        ],
        "mappings": sorted(mappings, key=lambda row: row["oldTaskId"]),
    }
    mapping_report["mappingSha256"] = canonical_sha256(mapping_report)
    _write(output_root / "task-mapping-report.json", mapping_report)
    control: dict[str, Any] = {
        "schemaVersion": "temporal_qd_topology_v2_3_launch_control_v1",
        "dispatchEnabled": False,
        "panelCount": 3,
        "candidateCountPerPanel": 12,
        "panelTaskCounts": [48, 48, 48],
        "totalInspectedTaskCount": 144,
        "replicationRulePath": "research/temporal-qd/rust-canonical-authority-v2-3/topology-replication-survival-rule-v1.json",
        "mappingSha256": mapping_report["mappingSha256"],
        "panels": panels,
    }
    control["launchControlSha256"] = canonical_sha256(control)
    _write(output_root / "topology-launch-control.json", control)
    output_templates = _campaign_output_templates(
        output_root=output_root, rotating=rotating, panels=panels
    )
    _write(output_root / "campaign-output-templates.json", output_templates)
    if compact_root is not None:
        _write(compact_root / "topology-production-task-mapping-v1.json", mapping_report)
        _write(compact_root / "topology-production-launch-control-v1.json", control)
        _write(
            compact_root / "topology-production-output-templates-v1.json",
            output_templates,
        )
    return control


def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generic-task-manifest", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--native-binary", required=True, type=Path)
    parser.add_argument("--compact-root", type=Path)
    parser.add_argument("--authority-package-root", type=Path, default=V2)
    parser.add_argument("--rotating-evidence-root", type=Path, default=ROTATING)
    parser.add_argument("--execution-engine-commit", default=SOURCE_COMMIT)
    args = parser.parse_args()
    print(canonical_json(generate(
        generic_manifest=args.generic_task_manifest,
        output_root=args.output_root,
        native_binary=args.native_binary,
        compact_root=args.compact_root,
        authority_package_root=args.authority_package_root,
        rotating_evidence_root=args.rotating_evidence_root,
        execution_engine_commit=args.execution_engine_commit,
    )))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
