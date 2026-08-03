from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_prebroad_control import (
    WINDOWS,
    build_prebroad_authority,
    materialize_prebroad_matrix,
)
from autoresearch.temporal_prebroad_dispatch import (
    build_prebroad_dispatch_tasks,
    load_prebroad_dispatch_inputs,
    run_prebroad_dispatch,
)
from autoresearch.temporal_search import (
    TEMPORAL_BIDIRECTIONAL_REPLAY_CAPABILITY,
    TEMPORAL_SEARCH_CAPABILITY,
    TEMPORAL_SEARCH_TASK_KIND,
    TemporalSearchContractError,
    canonical_sha256,
)


SHA = "sha256:" + "a" * 64


def _catalog_resolution(profile_sha: str) -> dict:
    material = {
        "schemaVersion": "temporal_prebroad_catalog_resolution_v1",
        "rawSourceProfileSha256": profile_sha,
        "resolvedProfileSnapshotSha256": SHA,
        "resolvedProgramSha256": SHA,
        "indicatorCatalogSha256": SHA,
    }
    return {**material, "catalogResolutionSha256": canonical_sha256(material)}


def _plan(profile_sha: str, window_id: str, start: str, end: str) -> dict:
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": profile_sha,
        "analysis_window_start": start,
        "analysis_window_end": end,
        "campaign_plan_id": f"test:{window_id}",
        "coverage_policy": "require_complete",
        "data_availability_cutoff": "2026-01-01T00:00:00Z",
        "evidence_role": "development_parity",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "requested_horizon_months": 1,
        "selection_data_end": end,
        "lake_window_binding": {
            "schema_version": "fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "request": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
                "data_start": start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
            "window_semantic_sha256": SHA,
            "attestation_sha256": SHA,
            "creation_global_coverage_sha256": SHA,
            "creation_source_coverage_sha256": SHA,
            "legacy_selection_manifest_sha256": None,
        },
    }
    identity = dict(plan)
    identity.pop("lake_manifest_sha256")
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _inputs(tmp_path: Path) -> tuple[dict, dict]:
    pairs = []
    for index in range(8):
        candidate_id = f"candidate_{index}"
        profile = {
            "version": "v3",
            "directionMode": "both",
            "instruments": ["EURUSD"],
            "executionConfig": {
                "managementLibrary": {},
                "initialProtection": {"stopId": None, "targetId": None},
            },
        }
        profile_sha = canonical_sha256(profile)
        validation = {
            "candidateId": candidate_id,
            "candidateAcceptable": True,
            "status": "valid_evaluable",
            "programSha256": SHA,
            "validationReportSha256": SHA,
            "rawSourceProfileSha256": profile_sha,
            "profileSnapshotSha256": SHA,
            "evaluatorId": "bar_bidirectional_single_position_execution_v2",
        }
        pairs.append(
            {
                "candidateId": candidate_id,
                "profile": profile,
                "profileSha256": profile_sha,
                "catalogResolution": _catalog_resolution(profile_sha),
                "validation": validation,
                "timeframe": "M5",
                "barLimit": 500,
                "windowInputs": [
                    {"windowId": window_id, "evidencePlan": _plan(profile_sha, window_id, start, end)}
                    for window_id, start, end in WINDOWS
                ],
            }
        )
    accepted = {
        "schemaVersion": "temporal_prebroad_accepted_pairs_v2",
        "workerContract": {"workerContractSha256": SHA, "workerContractSchema": "replay-worker-contract-v1"},
        "pairs": pairs,
    }
    reports = {pair["candidateId"]: pair["validation"] for pair in pairs}
    authority = build_prebroad_authority(accepted, native_reports=reports)
    materialize_prebroad_matrix(
        authority, tmp_path, required_authority_id=authority["authorityId"], resume=False, native_reports=reports
    )
    (tmp_path / "authority-id.txt").write_text(authority["authorityId"] + "\n", encoding="utf-8")
    return authority, reports


def _paths(root: Path) -> dict[str, Path]:
    return {
        "authority_path": root / "authority.json",
        "authority_id_path": root / "authority-id.txt",
        "manifest_path": root / "task-manifest.json",
    }


def test_dispatch_builds_only_the_exact_v3_both_labtask_matrix(tmp_path: Path) -> None:
    authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")
    loaded, manifest = load_prebroad_dispatch_inputs(**paths, native_reports=reports)
    tasks = build_prebroad_dispatch_tasks(loaded, manifest)

    assert len(tasks) == 16
    assert {task["task_kind"] for task in tasks} == {TEMPORAL_SEARCH_TASK_KIND}
    assert all(task["max_attempts"] == 1 for task in tasks)
    assert all(TEMPORAL_SEARCH_CAPABILITY in task["required_worker_capabilities"] for task in tasks)
    assert all(
        TEMPORAL_BIDIRECTIONAL_REPLAY_CAPABILITY
        in task["required_worker_capabilities"]
        for task in tasks
    )
    for task in tasks:
        job = task["payload"]
        assert job["authority_id"] == authority["authorityId"]
        assert job["inline_profile_snapshot"]["version"] == "v3"
        assert job["inline_profile_snapshot"]["directionMode"] == "both"
        assert job["evaluator_id"] == "bar_bidirectional_single_position_execution_v2"
        assert job["required_worker_contract_schema"] == "replay-worker-contract-v1"
        assert job["required_worker_contract_hash"] == SHA
        assert job["evidence_plan"]["plan_id"] in {
            item["evidencePlan"]["plan_id"] for item in manifest["tasks"]
        }


def test_dispatch_refuses_a_modified_no_dispatch_manifest_before_gateway_contact(tmp_path: Path) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")
    manifest = json.loads(paths["manifest_path"].read_text(encoding="utf-8"))
    manifest["tasks"][0]["barLimit"] = 501
    paths["manifest_path"].write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(TemporalSearchContractError, match="exact closed deterministic matrix"):
        load_prebroad_dispatch_inputs(**paths, native_reports=reports)


def test_dispatch_rejects_a_worker_contract_schema_the_worker_cannot_admit(
    tmp_path: Path,
) -> None:
    authority, reports = _inputs(tmp_path / "source")
    accepted = {
        "schemaVersion": "temporal_prebroad_accepted_pairs_v2",
        "workerContract": {
            "workerContractSha256": SHA,
            "workerContractSchema": "invented-worker-contract-v9",
        },
        "pairs": authority["pairs"],
    }
    bad = build_prebroad_authority(accepted, native_reports=reports)
    root = tmp_path / "bad-contract"
    materialize_prebroad_matrix(
        bad,
        root,
        required_authority_id=bad["authorityId"],
        resume=False,
        native_reports=reports,
    )
    (root / "authority-id.txt").write_text(bad["authorityId"] + "\n", encoding="utf-8")
    with pytest.raises(TemporalSearchContractError, match="replay-worker-contract-v1"):
        load_prebroad_dispatch_inputs(**_paths(root), native_reports=reports)


class _DuplicateGateway:
    def __init__(self) -> None:
        self.enqueued: list[dict] = []
        self.acks: list[str] = []
        self.delivered = False

    def enqueue_tasks(self, tasks: list[dict]) -> dict:
        self.enqueued.extend(copy.deepcopy(tasks))
        return {"status": "accepted", "submitted": len(tasks), "accepted": 0, "enqueued": 0, "rejected": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict]:
        if self.delivered or not self.enqueued:
            return []
        self.delivered = True
        return [
            {
                "status": "success",
                "task_id": task["task_id"],
                "lane_id": task["lane_id"],
                "attempt_id": task["attempt_id"],
                "lease_id": f"lease-{index}",
                "result": {},
            }
            for index, task in enumerate(self.enqueued)
        ]

    def ack_results(self, lease_ids: list[str]) -> int:
        self.acks.extend(lease_ids)
        return len(lease_ids)


def _bound_material(task: dict) -> dict:
    job = task["payload"]
    return {
        "material": task["task_id"],
        "profile_id": job["profile_id"],
        "execution_config_sha256": job.get("execution_config_sha256"),
        "observation_summary": {
            "instrument": job["instruments"][0],
            "timeframe": job["timeframe"],
        },
        "worker_attribution": {
            "worker_contract_hash": job["required_worker_contract_hash"]
        },
    }


def test_resume_accepts_only_exact_duplicate_receipt_and_persists_before_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")

    # The existing temporal-search validator is independently exhaustively
    # tested.  Here isolate dispatcher ordering and durable receipt behavior.
    monkeypatch.setattr(
        "autoresearch.temporal_prebroad_dispatch._result_material",
        lambda task, completion: _bound_material(task),
    )
    gateway = _DuplicateGateway()
    result = run_prebroad_dispatch(
        **paths,
        output_root=tmp_path / "dispatch",
        client=gateway,
        resume=True,
        timeout_seconds=1,
        native_reports=reports,
    )

    assert result["completedTaskCount"] == 16
    assert len(gateway.acks) == 16
    checkpoint = json.loads((tmp_path / "dispatch" / "checkpoint.json").read_text(encoding="utf-8"))
    assert len(checkpoint["completed"]) == 16
    assert len(checkpoint["journal"]) == 16
    assert checkpoint["journalSha256"].startswith("sha256:")
    assert all((tmp_path / "dispatch" / "results" / f"{task_id}.json.gz").is_file() for task_id in checkpoint["completed"])


def test_dispatch_rejects_unrelated_completion_without_ack(tmp_path: Path) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")

    class Gateway:
        acks: list[str] = []

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            return {"status": "accepted", "submitted": len(tasks), "accepted": len(tasks), "enqueued": len(tasks), "rejected": 0}

        def read_results(self, *, limit: int) -> list[dict]:
            return [{"status": "success", "task_id": "unrelated", "lease_id": "lease-x"}]

        def ack_results(self, lease_ids: list[str]) -> int:
            self.acks.extend(lease_ids)
            return len(lease_ids)

    gateway = Gateway()
    with pytest.raises(TemporalSearchContractError, match="unrelated"):
        run_prebroad_dispatch(
            **paths,
            output_root=tmp_path / "dispatch",
            client=gateway,
            native_reports=reports,
        )
    assert gateway.acks == []


def test_dispatch_persists_and_acks_failed_completion_before_tripwire(tmp_path: Path) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")
    _loaded, manifest = load_prebroad_dispatch_inputs(**paths, native_reports=reports)
    task_id = manifest["tasks"][0]["taskId"]

    class Gateway:
        acks: list[str] = []

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            return {"status": "accepted", "submitted": len(tasks), "accepted": len(tasks), "enqueued": len(tasks), "rejected": 0}

        def read_results(self, *, limit: int) -> list[dict]:
            return [{"status": "failed", "task_id": task_id, "lease_id": "lease-x"}]

        def ack_results(self, lease_ids: list[str]) -> int:
            self.acks.extend(lease_ids)
            return len(lease_ids)

    gateway = Gateway()
    with pytest.raises(TemporalSearchContractError, match="failed"):
        run_prebroad_dispatch(
            **paths,
            output_root=tmp_path / "dispatch",
            client=gateway,
            native_reports=reports,
        )
    assert gateway.acks == ["lease-x"]
    failure = tmp_path / "dispatch" / "failures" / f"{task_id}.json"
    assert json.loads(failure.read_text(encoding="utf-8"))["status"] == "failed"


def test_dispatch_rejects_worker_attribution_drift_without_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")
    gateway = _DuplicateGateway()

    def wrong_worker(task: dict, completion: dict) -> dict:
        material = _bound_material(task)
        material["worker_attribution"]["worker_contract_hash"] = "sha256:" + "b" * 64
        return material

    monkeypatch.setattr(
        "autoresearch.temporal_prebroad_dispatch._result_material", wrong_worker
    )
    with pytest.raises(TemporalSearchContractError, match="worker contract"):
        run_prebroad_dispatch(
            **paths,
            output_root=tmp_path / "dispatch",
            client=gateway,
            resume=True,
            timeout_seconds=1,
            native_reports=reports,
        )
    assert gateway.acks == []
    assert not (tmp_path / "dispatch" / "results").exists()


def test_resume_revalidates_persisted_material_against_its_exact_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _authority, reports = _inputs(tmp_path / "control")
    paths = _paths(tmp_path / "control")
    gateway = _DuplicateGateway()
    monkeypatch.setattr(
        "autoresearch.temporal_prebroad_dispatch._result_material",
        lambda task, completion: _bound_material(task),
    )
    run_prebroad_dispatch(
        **paths,
        output_root=tmp_path / "dispatch",
        client=gateway,
        resume=True,
        timeout_seconds=1,
        native_reports=reports,
    )

    from autoresearch import temporal_prebroad_dispatch as dispatch

    original_read = dispatch._read_checkpoint_result

    def drifted_read(record: dict) -> dict:
        material = original_read(record)
        material["profile_id"] = "candidate_from_another_task"
        return material

    monkeypatch.setattr(dispatch, "_read_checkpoint_result", drifted_read)
    monkeypatch.setattr(
        dispatch,
        "_result_material",
        lambda task, completion: completion["result"]["result"],
    )
    with pytest.raises(TemporalSearchContractError, match="profile does not match"):
        run_prebroad_dispatch(
            **paths,
            output_root=tmp_path / "dispatch",
            client=gateway,
            resume=True,
            timeout_seconds=1,
            native_reports=reports,
        )
