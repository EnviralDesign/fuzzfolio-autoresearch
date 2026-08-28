"""Run-local authority binding for V2.6.4 topology analysis and execution.

The launch gate copies its selected authority into a fresh run before any
dispatch.  Later reducers recover their paths solely from the self-hashed
run receipt; they never choose a repository authority root.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v3 import reduce_files_v3


SCHEMA = "temporal_qd_topology_v2_6_4_run_authority_receipt_v1"
SNAPSHOT_DIRECTORY = "topology-run-authority"
RECEIPT_NAME = "topology-run-authority-receipt.json"
PANELS = ("panel-1", "panel-2", "panel-3")
AUTHORITY_FILES = {
    "launchControl": ("topology-production-launch-control-v1.json", "launchControlSha256"),
    "taskMapping": ("topology-production-task-mapping-v1.json", "mappingSha256"),
    "replicationRule": ("topology-replication-survival-rule-v1.json", "replicationRuleSha256"),
    "scientificContract": ("topology-scientific-contract-v1.json", "scientificContractSha256"),
    "analyzerContract": ("topology-post-run-analyzer-contract-v1.json", "analyzerContractSha256"),
    "panelPolicy": ("topology-panel-usefulness-policy-v2.json", "panelUsefulnessPolicySha256"),
}


class RunAuthorityError(ValueError):
    """The run-local authority cannot authenticate its selected inputs."""


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RunAuthorityError(f"expected JSON object: {path}")
    return value


def _rows(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    if not all(isinstance(row, dict) for row in rows):
        raise RunAuthorityError(f"expected JSON objects: {path}")
    return rows


def _raw(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _verify_self_hash(value: Mapping[str, Any], field: str, label: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field, None)
    if stored != canonical_sha256(unsigned):
        raise RunAuthorityError(f"{label} self-hash mismatch")


def _relative_to(root: Path, path: Path, label: str) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise RunAuthorityError(f"{label} must be inside the fresh run root") from exc


def _authority_documents(authority_sources: Mapping[str, Path]) -> dict[str, tuple[Path, dict[str, Any]]]:
    if set(authority_sources) != set(AUTHORITY_FILES):
        raise RunAuthorityError("authority source set is incomplete or has unknown entries")
    documents: dict[str, tuple[Path, dict[str, Any]]] = {}
    for logical_id, (_, hash_field) in AUTHORITY_FILES.items():
        path = Path(authority_sources[logical_id])
        if not path.is_file():
            raise RunAuthorityError(f"authority source is missing: {logical_id}")
        value = _load(path)
        _verify_self_hash(value, hash_field, logical_id)
        documents[logical_id] = (path, value)
    control = documents["launchControl"][1]
    mapping = documents["taskMapping"][1]
    if (
        control.get("dispatchEnabled") is not False
        or control.get("panelCount") != 3
        or control.get("totalInspectedTaskCount") != 144
        or control.get("mappingSha256") != mapping.get("mappingSha256")
    ):
        raise RunAuthorityError("launch control is not the exact safe 3x48 authority")
    return documents


def _input_bindings(run_root: Path, input_roots: Mapping[str, Path]) -> tuple[list[dict[str, Any]], set[str]]:
    if set(input_roots) != set(PANELS):
        raise RunAuthorityError("fresh run must bind exactly panel-1, panel-2, and panel-3 inputs")
    bindings: list[dict[str, Any]] = []
    all_task_ids: set[str] = set()
    for panel_id in PANELS:
        panel_root = Path(input_roots[panel_id])
        relative_root = _relative_to(run_root, panel_root, f"{panel_id} input root")
        checkpoint_path = panel_root / "campaign-input-checkpoint.json"
        task_path = panel_root / "screening-run" / "tasks.jsonl"
        checkpoint = _load(checkpoint_path)
        tasks = _rows(task_path)
        task_ids = {str(row.get("task_id")) for row in tasks}
        if (
            checkpoint.get("panelId") != panel_id
            or checkpoint.get("taskCount") != 48
            or not isinstance(checkpoint.get("checkpointSha256"), str)
            or not isinstance(checkpoint.get("taskMatrixSha256"), str)
            or checkpoint.get("tasks", {}).get("rawSha256") != _raw(task_path)
            or checkpoint.get("tasks", {}).get("taskMatrixSha256") != checkpoint.get("taskMatrixSha256")
            or len(tasks) != 48
            or len(task_ids) != 48
            or all_task_ids.intersection(task_ids)
        ):
            raise RunAuthorityError(f"{panel_id} copied input binding drifted")
        all_task_ids.update(task_ids)
        bindings.append(
            {
                "panelId": panel_id,
                "relativeRoot": relative_root,
                "checkpointRelativePath": f"{relative_root}/campaign-input-checkpoint.json",
                "tasksRelativePath": f"{relative_root}/screening-run/tasks.jsonl",
                "checkpointSha256": checkpoint["checkpointSha256"],
                "taskMatrixSha256": checkpoint["taskMatrixSha256"],
                "checkpointRawSha256": _raw(checkpoint_path),
                "taskPackRawSha256": _raw(task_path),
                "taskCount": 48,
            }
        )
    if len(all_task_ids) != 144:
        raise RunAuthorityError("fresh run inputs are not an exact 144-task set")
    return bindings, all_task_ids


def _verify_mapping_matches_inputs(mapping: Mapping[str, Any], task_ids: set[str]) -> None:
    rows = mapping.get("mappings")
    if not isinstance(rows, list):
        raise RunAuthorityError("task mapping has no mappings list")
    mapping_ids = [str(row.get("newTaskId")) for row in rows if isinstance(row, Mapping)]
    if (
        mapping.get("mappedTaskCount") != 144
        or len(mapping_ids) != 144
        or len(set(mapping_ids)) != 144
        or set(mapping_ids) != task_ids
    ):
        raise RunAuthorityError("copied inputs do not match the selected authority task mapping")


def _verify_control_matches_input_bindings(
    control: Mapping[str, Any], input_bindings: list[dict[str, Any]]
) -> None:
    """Require the launch control to name the exact copied panel inputs."""

    panels = control.get("panels")
    if not isinstance(panels, list) or len(panels) != len(PANELS):
        raise RunAuthorityError("launch control must describe exactly three panels")
    if control.get("panelTaskCounts") != [48, 48, 48]:
        raise RunAuthorityError("launch control panel task counts drifted")
    expected_by_id = {binding["panelId"]: binding for binding in input_bindings}
    seen: set[str] = set()
    for descriptor in panels:
        if not isinstance(descriptor, Mapping):
            raise RunAuthorityError("launch control panel descriptor is invalid")
        panel_id = descriptor.get("panelId")
        if panel_id not in expected_by_id or panel_id in seen:
            raise RunAuthorityError("launch control panel IDs must be panel-1, panel-2, and panel-3 exactly once")
        seen.add(panel_id)
        binding = expected_by_id[panel_id]
        if (
            descriptor.get("checkpointSha256") != binding["checkpointSha256"]
            or descriptor.get("taskMatrixSha256") != binding["taskMatrixSha256"]
        ):
            raise RunAuthorityError(f"{panel_id} launch-control/input identity drifted")
        if (
            descriptor.get("candidateCount") != 12
            or descriptor.get("windowCount") != 4
            or descriptor.get("taskCount") != 48
        ):
            raise RunAuthorityError(f"{panel_id} launch-control panel cardinality drifted")
    if seen != set(PANELS):
        raise RunAuthorityError("launch control panel IDs must be panel-1, panel-2, and panel-3 exactly once")


def snapshot_run_authority(
    *,
    run_root: Path,
    input_roots: Mapping[str, Path],
    authority_sources: Mapping[str, Path],
) -> dict[str, Any]:
    """Write the one authority snapshot permitted for a fresh run.

    Validation precedes all writes.  The selected mapping must exactly equal
    the existing copied input task IDs, so a stale but otherwise valid control
    fails before dispatch.
    """

    run_root = Path(run_root)
    snapshot_root = run_root / SNAPSHOT_DIRECTORY
    receipt_path = run_root / RECEIPT_NAME
    if snapshot_root.exists() or receipt_path.exists():
        raise RunAuthorityError("fresh run already contains a topology authority snapshot")
    documents = _authority_documents(authority_sources)
    input_bindings, task_ids = _input_bindings(run_root, input_roots)
    _verify_control_matches_input_bindings(documents["launchControl"][1], input_bindings)
    _verify_mapping_matches_inputs(documents["taskMapping"][1], task_ids)
    snapshot_files: dict[str, dict[str, Any]] = {}
    snapshot_root.mkdir(parents=True)
    for logical_id, (source_path, value) in documents.items():
        filename, hash_field = AUTHORITY_FILES[logical_id]
        target = snapshot_root / filename
        shutil.copyfile(source_path, target)
        if _raw(source_path) != _raw(target):
            raise RunAuthorityError(f"authority snapshot byte copy failed: {logical_id}")
        snapshot_files[logical_id] = {
            "relativePath": f"{SNAPSHOT_DIRECTORY}/{filename}",
            "sourceRawSha256": _raw(source_path),
            "snapshotRawSha256": _raw(target),
            "semanticSha256": value[hash_field],
        }
    receipt: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "status": "ready_for_dispatch_or_analysis",
        "dispatchEnabled": False,
        "snapshotDirectory": SNAPSHOT_DIRECTORY,
        "launchControlSha256": documents["launchControl"][1]["launchControlSha256"],
        "mappingSha256": documents["taskMapping"][1]["mappingSha256"],
        "authorityFiles": snapshot_files,
        "inputBindings": input_bindings,
        "mappedTaskCount": len(task_ids),
    }
    receipt["runAuthorityReceiptSha256"] = canonical_sha256(receipt)
    receipt_path.write_text(canonical_json(receipt) + "\n", encoding="utf-8", newline="\n")
    return receipt


def reducer_authority_from_run_receipt(run_root: Path) -> dict[str, Path]:
    """Return only the local snapshot paths after revalidating the receipt."""

    run_root = Path(run_root)
    receipt = _load(run_root / RECEIPT_NAME)
    _verify_self_hash(receipt, "runAuthorityReceiptSha256", "run authority receipt")
    if (
        receipt.get("schemaVersion") != SCHEMA
        or receipt.get("status") != "ready_for_dispatch_or_analysis"
        or receipt.get("dispatchEnabled") is not False
        or receipt.get("snapshotDirectory") != SNAPSHOT_DIRECTORY
    ):
        raise RunAuthorityError("run authority receipt stopping-boundary drifted")
    paths: dict[str, Path] = {}
    authority_files = receipt.get("authorityFiles")
    if not isinstance(authority_files, Mapping) or set(authority_files) != set(AUTHORITY_FILES):
        raise RunAuthorityError("run authority receipt snapshot inventory drifted")
    for logical_id, (filename, hash_field) in AUTHORITY_FILES.items():
        record = authority_files[logical_id]
        expected_relative = f"{SNAPSHOT_DIRECTORY}/{filename}"
        if not isinstance(record, Mapping) or record.get("relativePath") != expected_relative:
            raise RunAuthorityError(f"run authority receipt path drifted: {logical_id}")
        path = run_root / expected_relative
        if not path.is_file() or record.get("snapshotRawSha256") != _raw(path):
            raise RunAuthorityError(f"run authority snapshot is missing or changed: {logical_id}")
        value = _load(path)
        _verify_self_hash(value, hash_field, logical_id)
        if record.get("semanticSha256") != value.get(hash_field):
            raise RunAuthorityError(f"run authority semantic binding drifted: {logical_id}")
        paths[logical_id] = path
    control = _load(paths["launchControl"])
    mapping = _load(paths["taskMapping"])
    if (
        control.get("launchControlSha256") != receipt.get("launchControlSha256")
        or mapping.get("mappingSha256") != receipt.get("mappingSha256")
        or control.get("mappingSha256") != mapping.get("mappingSha256")
    ):
        raise RunAuthorityError("gate and reducer authority differ")
    input_roots: dict[str, Path] = {}
    bindings = receipt.get("inputBindings")
    if not isinstance(bindings, list) or len(bindings) != 3:
        raise RunAuthorityError("run authority receipt input inventory drifted")
    for binding in bindings:
        panel_id = binding.get("panelId")
        if panel_id not in PANELS or panel_id in input_roots:
            raise RunAuthorityError("run authority receipt has invalid input panel bindings")
        relative_root = binding.get("relativeRoot")
        if not isinstance(relative_root, str):
            raise RunAuthorityError("run authority receipt input root is missing")
        root = run_root / relative_root
        _relative_to(run_root, root, f"{panel_id} receipt input root")
        input_roots[panel_id] = root
    rebuilt_bindings, task_ids = _input_bindings(run_root, input_roots)
    if rebuilt_bindings != bindings:
        raise RunAuthorityError("copied inputs differ from the bound pre-dispatch receipt")
    _verify_mapping_matches_inputs(mapping, task_ids)
    return paths


def reduce_run_outputs_v3(*, run_root: Path, opener: Path) -> dict[str, Any]:
    """Reduce a run using its receipt-bound local authority and no other root."""

    run_root = Path(run_root)
    paths = reducer_authority_from_run_receipt(run_root)
    checkpoints = [
        run_root / "panels" / panel_id / "campaign-output" / "campaign-output-checkpoint.json"
        for panel_id in PANELS
    ]
    return reduce_files_v3(
        checkpoints=checkpoints,
        opener=Path(opener),
        launch_control_path=paths["launchControl"],
        task_mapping_path=paths["taskMapping"],
        replication_rule_path=paths["replicationRule"],
        scientific_contract_path=paths["scientificContract"],
        analyzer_contract_path=paths["analyzerContract"],
        panel_policy_path=paths["panelPolicy"],
    )


__all__ = [
    "AUTHORITY_FILES",
    "PANELS",
    "RECEIPT_NAME",
    "RunAuthorityError",
    "SNAPSHOT_DIRECTORY",
    "reduce_run_outputs_v3",
    "reducer_authority_from_run_receipt",
    "snapshot_run_authority",
]
