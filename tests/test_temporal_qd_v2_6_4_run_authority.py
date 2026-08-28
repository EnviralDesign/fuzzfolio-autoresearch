from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch import temporal_qd_v2_6_4_run_authority as authority


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, str):
        path.write_text(value, encoding="utf-8", newline="\n")
    else:
        path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def _raw(path: Path) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _self_hashed(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def _input_roots(run_root: Path) -> tuple[dict[str, Path], list[str]]:
    roots: dict[str, Path] = {}
    task_ids: list[str] = []
    for panel_index, panel_id in enumerate(authority.PANELS, start=1):
        root = run_root / "inputs" / panel_id
        roots[panel_id] = root
        rows = []
        for task_index in range(48):
            task_id = f"task-{panel_index}-{task_index:02d}"
            task_ids.append(task_id)
            rows.append({"task_id": task_id, "payload": {"panel": panel_id}})
        task_path = root / "screening-run" / "tasks.jsonl"
        _write(task_path, "".join(canonical_json(row) + "\n" for row in rows))
        _write(
            root / "campaign-input-checkpoint.json",
            {
                "panelId": panel_id,
                "taskCount": 48,
                "checkpointSha256": f"sha256:checkpoint-{panel_index}",
                "taskMatrixSha256": f"sha256:matrix-{panel_index}",
                "tasks": {
                    "rawSha256": _raw(task_path),
                    "taskMatrixSha256": f"sha256:matrix-{panel_index}",
                },
            },
        )
    return roots, task_ids


def _authority_sources(
    tmp_path: Path,
    task_ids: list[str],
    *,
    stale_mapping: bool = False,
    panels: list[dict] | None = None,
    panel_task_counts: list[int] | None = None,
) -> dict[str, Path]:
    source_root = tmp_path / "external-authority"
    mapping_ids = [f"stale-{index:03d}" for index in range(144)] if stale_mapping else task_ids
    mapping = _self_hashed(
        {
            "mappedTaskCount": 144,
            "mappings": [{"newTaskId": task_id} for task_id in mapping_ids],
        },
        "mappingSha256",
    )
    control = _self_hashed(
        {
            "dispatchEnabled": False,
            "panelCount": 3,
            "totalInspectedTaskCount": 144,
            "panelTaskCounts": [48, 48, 48] if panel_task_counts is None else panel_task_counts,
            "panels": (
                [
                {
                    "panelId": f"panel-{panel_index}",
                    "checkpointSha256": f"sha256:checkpoint-{panel_index}",
                    "taskMatrixSha256": f"sha256:matrix-{panel_index}",
                    "candidateCount": 12,
                    "windowCount": 4,
                    "taskCount": 48,
                }
                for panel_index in range(1, 4)
                ]
                if panels is None
                else panels
            ),
            "mappingSha256": mapping["mappingSha256"],
        },
        "launchControlSha256",
    )
    documents = {
        "launchControl": (control, "launchControlSha256"),
        "taskMapping": (mapping, "mappingSha256"),
        "replicationRule": (_self_hashed({"rule": "frozen"}, "replicationRuleSha256"), "replicationRuleSha256"),
        "scientificContract": (_self_hashed({"science": "frozen"}, "scientificContractSha256"), "scientificContractSha256"),
        "analyzerContract": (_self_hashed({"analyzer": "frozen"}, "analyzerContractSha256"), "analyzerContractSha256"),
        "panelPolicy": (_self_hashed({"policy": "frozen"}, "panelUsefulnessPolicySha256"), "panelUsefulnessPolicySha256"),
    }
    sources: dict[str, Path] = {}
    for logical_id, (value, _) in documents.items():
        filename, _ = authority.AUTHORITY_FILES[logical_id]
        path = source_root / filename
        _write(path, value)
        sources[logical_id] = path
    return sources


def _ready_run(tmp_path: Path) -> tuple[Path, dict[str, Path], dict[str, Path]]:
    run_root = tmp_path / "run"
    run_root.mkdir()
    input_roots, task_ids = _input_roots(run_root)
    authority_sources = _authority_sources(tmp_path, task_ids)
    authority.snapshot_run_authority(
        run_root=run_root,
        input_roots=input_roots,
        authority_sources=authority_sources,
    )
    return run_root, input_roots, authority_sources


def _update_receipt(run_root: Path, update) -> None:
    path = run_root / authority.RECEIPT_NAME
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt.pop("runAuthorityReceiptSha256")
    update(receipt)
    receipt["runAuthorityReceiptSha256"] = canonical_sha256(receipt)
    _write(path, receipt)


def test_correct_authority_snapshots_and_reducer_can_only_reopen_local_paths(tmp_path: Path) -> None:
    run_root, _, sources = _ready_run(tmp_path)
    paths = authority.reducer_authority_from_run_receipt(run_root)
    assert set(paths) == set(authority.AUTHORITY_FILES)
    assert all(path.is_relative_to(run_root) for path in paths.values())
    assert paths["launchControl"].read_bytes() == sources["launchControl"].read_bytes()
    assert paths["taskMapping"].read_bytes() == sources["taskMapping"].read_bytes()


def test_stale_valid_mapping_fails_before_snapshot_or_dispatch(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir()
    input_roots, task_ids = _input_roots(run_root)
    stale_sources = _authority_sources(tmp_path, task_ids, stale_mapping=True)
    with pytest.raises(authority.RunAuthorityError, match="copied inputs do not match"):
        authority.snapshot_run_authority(
            run_root=run_root,
            input_roots=input_roots,
            authority_sources=stale_sources,
        )
    assert not (run_root / authority.SNAPSHOT_DIRECTORY).exists()
    assert not (run_root / authority.RECEIPT_NAME).exists()


def test_gate_and_reducer_authority_difference_fails_closed(tmp_path: Path) -> None:
    run_root, _, _ = _ready_run(tmp_path)
    mapping_path = run_root / authority.SNAPSHOT_DIRECTORY / authority.AUTHORITY_FILES["taskMapping"][0]
    changed = _self_hashed(
        {"mappedTaskCount": 144, "mappings": [{"newTaskId": f"other-{index:03d}"} for index in range(144)]},
        "mappingSha256",
    )
    _write(mapping_path, changed)

    def change(receipt: dict) -> None:
        receipt["mappingSha256"] = changed["mappingSha256"]
        record = receipt["authorityFiles"]["taskMapping"]
        record["snapshotRawSha256"] = _raw(mapping_path)
        record["semanticSha256"] = changed["mappingSha256"]

    _update_receipt(run_root, change)
    with pytest.raises(authority.RunAuthorityError, match="gate and reducer authority differ"):
        authority.reducer_authority_from_run_receipt(run_root)


def test_missing_snapshot_file_fails_closed(tmp_path: Path) -> None:
    run_root, _, _ = _ready_run(tmp_path)
    (run_root / authority.SNAPSHOT_DIRECTORY / authority.AUTHORITY_FILES["launchControl"][0]).unlink()
    with pytest.raises(authority.RunAuthorityError, match="snapshot is missing or changed"):
        authority.reducer_authority_from_run_receipt(run_root)


def test_copied_input_drift_fails_closed_after_snapshot(tmp_path: Path) -> None:
    run_root, inputs, _ = _ready_run(tmp_path)
    task_path = inputs["panel-1"] / "screening-run" / "tasks.jsonl"
    task_path.write_text(task_path.read_text(encoding="utf-8") + "\n", encoding="utf-8", newline="\n")
    with pytest.raises(authority.RunAuthorityError, match="copied input binding drifted"):
        authority.reducer_authority_from_run_receipt(run_root)


@pytest.mark.parametrize(
    ("field", "bad_value", "match"),
    [
        ("checkpointSha256", "sha256:wrong-checkpoint", "launch-control/input identity drifted"),
        ("taskMatrixSha256", "sha256:wrong-task-matrix", "launch-control/input identity drifted"),
    ],
)
def test_stale_control_panel_identity_fails_before_snapshot(
    tmp_path: Path, field: str, bad_value: str, match: str
) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir()
    input_roots, task_ids = _input_roots(run_root)
    panels = [
        {
            "panelId": f"panel-{panel_index}",
            "checkpointSha256": f"sha256:checkpoint-{panel_index}",
            "taskMatrixSha256": f"sha256:matrix-{panel_index}",
            "candidateCount": 12,
            "windowCount": 4,
            "taskCount": 48,
        }
        for panel_index in range(1, 4)
    ]
    panels[1][field] = bad_value
    sources = _authority_sources(tmp_path, task_ids, panels=panels)
    with pytest.raises(authority.RunAuthorityError, match=match):
        authority.snapshot_run_authority(
            run_root=run_root,
            input_roots=input_roots,
            authority_sources=sources,
        )
    assert not (run_root / authority.SNAPSHOT_DIRECTORY).exists()
    assert not (run_root / authority.RECEIPT_NAME).exists()


@pytest.mark.parametrize(
    ("panels", "panel_task_counts", "match"),
    [
        (
            [
                {
                    "panelId": "panel-1",
                    "checkpointSha256": "sha256:checkpoint-1",
                    "taskMatrixSha256": "sha256:matrix-1",
                    "candidateCount": 12,
                    "windowCount": 4,
                    "taskCount": 48,
                },
                {
                    "panelId": "panel-1",
                    "checkpointSha256": "sha256:checkpoint-2",
                    "taskMatrixSha256": "sha256:matrix-2",
                    "candidateCount": 12,
                    "windowCount": 4,
                    "taskCount": 48,
                },
                {
                    "panelId": "panel-3",
                    "checkpointSha256": "sha256:checkpoint-3",
                    "taskMatrixSha256": "sha256:matrix-3",
                    "candidateCount": 12,
                    "windowCount": 4,
                    "taskCount": 48,
                },
            ],
            [48, 48, 48],
            "launch control panel IDs",
        ),
        (None, [48, 47, 49], "launch control panel task counts drifted"),
    ],
)
def test_control_panel_shape_drift_fails_before_snapshot(
    tmp_path: Path, panels: list[dict] | None, panel_task_counts: list[int], match: str
) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir()
    input_roots, task_ids = _input_roots(run_root)
    sources = _authority_sources(
        tmp_path,
        task_ids,
        panels=panels,
        panel_task_counts=panel_task_counts,
    )
    with pytest.raises(authority.RunAuthorityError, match=match):
        authority.snapshot_run_authority(
            run_root=run_root,
            input_roots=input_roots,
            authority_sources=sources,
        )
    assert not (run_root / authority.SNAPSHOT_DIRECTORY).exists()
    assert not (run_root / authority.RECEIPT_NAME).exists()


@pytest.mark.parametrize(("field", "bad_value"), [("candidateCount", 11), ("windowCount", 5), ("taskCount", 47)])
def test_control_panel_cardinality_drift_fails_before_snapshot(
    tmp_path: Path, field: str, bad_value: int
) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir()
    input_roots, task_ids = _input_roots(run_root)
    panels = [
        {
            "panelId": f"panel-{panel_index}",
            "checkpointSha256": f"sha256:checkpoint-{panel_index}",
            "taskMatrixSha256": f"sha256:matrix-{panel_index}",
            "candidateCount": 12,
            "windowCount": 4,
            "taskCount": 48,
        }
        for panel_index in range(1, 4)
    ]
    panels[2][field] = bad_value
    sources = _authority_sources(tmp_path, task_ids, panels=panels)
    with pytest.raises(authority.RunAuthorityError, match="launch-control panel cardinality drifted"):
        authority.snapshot_run_authority(
            run_root=run_root,
            input_roots=input_roots,
            authority_sources=sources,
        )
    assert not (run_root / authority.SNAPSHOT_DIRECTORY).exists()
    assert not (run_root / authority.RECEIPT_NAME).exists()


def test_copied_checkpoint_drift_fails_closed_after_snapshot(tmp_path: Path) -> None:
    run_root, inputs, _ = _ready_run(tmp_path)
    checkpoint_path = inputs["panel-1"] / "campaign-input-checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["checkpointSha256"] = "sha256:changed-after-snapshot"
    _write(checkpoint_path, checkpoint)
    with pytest.raises(authority.RunAuthorityError, match="copied inputs differ"):
        authority.reducer_authority_from_run_receipt(run_root)


def test_analysis_wrapper_has_no_authority_root_and_uses_receipt_snapshot(tmp_path: Path, monkeypatch) -> None:
    run_root, _, _ = _ready_run(tmp_path)
    captured: dict = {}

    def fake_reduce(**kwargs):
        captured.update(kwargs)
        return {"status": "complete"}

    monkeypatch.setattr(authority, "reduce_files_v3", fake_reduce)
    result = authority.reduce_run_outputs_v3(run_root=run_root, opener=tmp_path / "opener.exe")
    assert result == {"status": "complete"}
    assert "authority_root" not in inspect.signature(authority.reduce_run_outputs_v3).parameters
    assert captured["launch_control_path"].is_relative_to(run_root)
    assert captured["task_mapping_path"].is_relative_to(run_root)
    assert captured["launch_control_path"] != Path("C:/some/stale/authority.json")


def test_prior_sealed_outputs_can_reduce_with_bound_authority_without_recopying(tmp_path: Path, monkeypatch) -> None:
    run_root, _, _ = _ready_run(tmp_path)
    captured: dict = {}
    monkeypatch.setattr(authority, "reduce_files_v3", lambda **kwargs: captured.update(kwargs) or {"status": "complete"})
    authority.reduce_run_outputs_v3(run_root=run_root, opener=tmp_path / "opener.exe")
    assert [path.as_posix() for path in captured["checkpoints"]] == [
        (run_root / "panels" / panel / "campaign-output" / "campaign-output-checkpoint.json").as_posix()
        for panel in authority.PANELS
    ]
    assert all(
        captured[key].is_relative_to(run_root)
        for key in (
            "launch_control_path",
            "task_mapping_path",
            "replication_rule_path",
            "scientific_contract_path",
            "analyzer_contract_path",
            "panel_policy_path",
        )
    )
