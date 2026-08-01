from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_search import (
    TEMPORAL_SEARCH_CAPABILITY,
    TEMPORAL_SEARCH_RESULT_SCHEMA,
    TEMPORAL_SEARCH_TASK_KIND,
    TemporalSearchContractError,
    TemporalSearchTimeout,
    build_authority,
    build_task_matrix,
    canonical_sha256,
    materialize_plan,
    run_temporal_search_tasks,
    validate_authority,
)


def _profile() -> dict:
    return {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1"},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "executionConfig": {
            "exitPolicy": {
                "evidenceStatus": "none",
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                },
                "sourceKind": "manual",
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }


def _plan(profile: dict, start: str, end: str) -> dict:
    execution_config = profile["executionConfig"]
    execution_cell_sha256 = None
    if "managementLibrary" not in execution_config:
        execution_cell_sha256 = canonical_sha256(
            execution_config["exitPolicy"]["selectedCell"]
        )
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": canonical_sha256(profile),
        "analysis_window_start": start,
        "analysis_window_end": end,
        "execution_cell_sha256": execution_cell_sha256,
        "lake_window_binding": {
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "request": {
                "data_start": "2024-01-01T00:00:00Z",
                "data_end": "2025-01-01T00:00:00Z",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
    }
    plan["plan_id"] = canonical_sha256(plan)
    return plan


def _preparation() -> dict:
    profile = _profile()
    start = "2024-02-01T00:00:00Z"
    end = "2024-03-01T00:00:00Z"
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "stage5d-preflight",
        "workerContract": {
            "workerContractSha256": "sha256:" + "c" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": "candidate-a",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": [
                    {
                        "windowId": "development-a",
                        "evidencePlan": _plan(profile, start, end),
                    }
                ],
            }
        ],
        "developmentWindows": [
            {
                "windowId": "development-a",
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
            }
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved-tail",
                "analysisWindowStart": "2024-06-01T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "reserved holdout",
            }
        ],
        "bounds": {
            "maxCandidates": 2,
            "maxDevelopmentWindows": 2,
            "maxTasks": 4,
            "maxAttempts": 2,
            "deadlineSeconds": 60,
        },
    }


def test_authority_freezes_one_candidate_window_to_one_two_cost_task() -> None:
    authority = build_authority(_preparation())
    assert validate_authority(authority) == authority
    tasks = build_task_matrix(authority)
    assert len(tasks) == 1
    task = tasks[0]
    assert task["task_kind"] == TEMPORAL_SEARCH_TASK_KIND
    assert task["payload"]["candidate_id"] == "candidate_a"
    assert task["payload"]["profile_id"] == "candidate_a"
    assert task["payload"]["inline_profile_snapshot"] == _profile()
    assert task["payload"]["instruments"] == ["EURUSD"]
    assert (
        task["payload"]["execution_cell"]
        == _profile()["executionConfig"]["exitPolicy"]["selectedCell"]
    )
    assert "cost_views" not in task["payload"]
    assert "cost_models" not in task["payload"]
    assert "source_profile_snapshot" not in task["payload"]
    assert TEMPORAL_SEARCH_CAPABILITY in task["required_worker_capabilities"]
    assert (
        "management.scalar.price_distance.completed_bar"
        in task["required_worker_capabilities"]
    )
    assert "management.action.dynamic" in task["required_worker_capabilities"]


def test_scalar_management_task_binds_complete_execution_config_without_legacy_cell() -> (
    None
):
    preparation = _preparation()
    profile = preparation["candidates"][0]["sourceProfile"]
    profile["executionConfig"] = {
        "managementLibrary": {
            "stopDefinitions": [],
            "targetDefinitions": [],
            "trailingDefinitions": [],
            "scalarBindings": [],
        },
        "initialProtection": {"stopId": None, "targetId": None},
        "sizingPolicy": {"mode": "inherit_global"},
    }
    preparation["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    start = preparation["developmentWindows"][0]["analysisWindowStart"]
    end = preparation["developmentWindows"][0]["analysisWindowEnd"]
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"] = _plan(
        profile, start, end
    )

    payload = build_task_matrix(build_authority(preparation))[0]["payload"]

    assert payload["execution_config_sha256"] == canonical_sha256(
        profile["executionConfig"]
    )
    assert "execution_cell" not in payload
    assert payload["evidence_plan"]["execution_cell_sha256"] is None


def test_authority_rejects_sparse_scalar_management_evidence_plan() -> None:
    preparation = _preparation()
    profile = preparation["candidates"][0]["sourceProfile"]
    profile["executionConfig"] = {
        "managementLibrary": {
            "stopDefinitions": [],
            "targetDefinitions": [],
            "trailingDefinitions": [],
            "scalarBindings": [],
        },
        "initialProtection": {"stopId": None, "targetId": None},
        "sizingPolicy": {"mode": "inherit_global"},
    }
    preparation["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    start = preparation["developmentWindows"][0]["analysisWindowStart"]
    end = preparation["developmentWindows"][0]["analysisWindowEnd"]
    plan = _plan(profile, start, end)
    plan.pop("execution_cell_sha256")
    identity = dict(plan)
    identity.pop("plan_id")
    plan["plan_id"] = canonical_sha256(identity)
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"] = plan

    with pytest.raises(
        TemporalSearchContractError,
        match="must explicitly declare execution_cell_sha256",
    ):
        build_authority(preparation)


def test_authority_rejects_reserved_overlap_and_profile_plan_mismatch() -> None:
    preparation = _preparation()
    preparation["developmentWindows"][0]["analysisWindowEnd"] = "2024-06-15T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="overlaps prohibited"):
        build_authority(preparation)
    preparation = _preparation()
    plan = preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["profile_snapshot_sha256"] = "sha256:" + "d" * 64
    identity = dict(plan)
    identity.pop("plan_id")
    plan["plan_id"] = canonical_sha256(identity)
    with pytest.raises(TemporalSearchContractError, match="profile snapshot mismatch"):
        build_authority(preparation)


def test_plan_checkpoint_is_mutable_but_immutable_manifest_is_not(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    first = materialize_plan(authority, tmp_path)
    checkpoint = tmp_path / "checkpoint.json"
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    state["journal"].append({"taskId": "already-seen"})
    checkpoint.write_text(json.dumps(state), encoding="utf-8")
    assert (
        materialize_plan(authority, tmp_path)["taskMatrixSha256"]
        == first["taskMatrixSha256"]
    )
    other = build_authority({**_preparation(), "authorityLabel": "different-authority"})
    with pytest.raises(TemporalSearchContractError, match="divergent immutable file"):
        materialize_plan(other, tmp_path)


class _Gateway:
    def __init__(self, task: dict):
        self.task = task
        self.enqueued: list[dict] = []
        self.delivered = False
        self.acks: list[str] = []

    def enqueue_tasks(self, tasks: list[dict]) -> dict:
        self.enqueued.extend(tasks)
        return {"enqueued": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict]:
        if self.delivered or not self.enqueued:
            return []
        self.delivered = True
        job = self.task["payload"]
        stream = "sha256:" + "e" * 64
        result = {
            "schema_version": TEMPORAL_SEARCH_RESULT_SCHEMA,
            "task_kind": TEMPORAL_SEARCH_TASK_KIND,
            "job_id": job["job_id"],
            "authority_id": job["authority_id"],
            "candidate_id": job["candidate_id"],
            "evidence_plan_id": job["evidence_plan"]["plan_id"],
            "lake_window_semantic_sha256": job["lake_window_semantic_sha256"],
            "shared_observation_stream_id": job["shared_observation_stream_id"],
            "cost_view_results": {
                "research_conservative": {
                    "cost_view": "research_conservative",
                    "observation_stream_sha256": stream,
                },
                "none": {"cost_view": "none", "observation_stream_sha256": stream},
            },
            "diagnostics": {},
            "selection_score": 1.0,
        }
        result["artifact_sha256"] = canonical_sha256(result)
        artifact_size = 1
        for _ in range(16):
            result["artifact_size_bytes"] = artifact_size
            result["diagnostics"]["artifact_size_bytes"] = artifact_size
            next_size = len(
                json.dumps(
                    result,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("utf-8")
            )
            if next_size == artifact_size:
                break
            artifact_size = next_size
        return [
            {
                "status": "success",
                "task_id": self.task["task_id"],
                "lane_id": self.task["lane_id"],
                "attempt_id": self.task["attempt_id"],
                "lease_id": "lease-1",
                "result": {
                    "status": "success",
                    "job_kind": TEMPORAL_SEARCH_TASK_KIND,
                    "result": result,
                },
            }
        ]

    def ack_results(self, lease_ids: list[str]) -> int:
        self.acks.extend(lease_ids)
        return 1


def test_controller_materializes_both_cost_results_from_one_stream(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    result = run_temporal_search_tasks(
        gateway, authority, output_root=tmp_path, timeout_seconds=1
    )
    assert result["completedTaskCount"] == 1
    assert gateway.acks == ["lease-1"]
    assert len(gateway.enqueued) == 1


def test_controller_enqueues_large_matrices_in_bounded_batches(tmp_path: Path) -> None:
    preparation = _preparation()
    prototype = preparation["candidates"][0]
    candidates = []
    for index in range(5):
        candidate = copy.deepcopy(prototype)
        candidate["candidateId"] = f"candidate-{index}"
        candidate["sourceProfile"]["name"] = f"profile-{index}"
        candidate["sourceProfileSha256"] = canonical_sha256(candidate["sourceProfile"])
        start = preparation["developmentWindows"][0]["analysisWindowStart"]
        end = preparation["developmentWindows"][0]["analysisWindowEnd"]
        candidate["windowInputs"][0]["evidencePlan"] = _plan(
            candidate["sourceProfile"], start, end
        )
        candidates.append(candidate)
    preparation["candidates"] = candidates
    preparation["bounds"]["maxCandidates"] = 5
    preparation["bounds"]["maxTasks"] = 5
    authority = build_authority(preparation)

    class NoResultGateway:
        def __init__(self):
            self.batch_sizes = []

        def enqueue_tasks(self, tasks):
            self.batch_sizes.append(len(tasks))
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            return []

        def ack_results(self, lease_ids):
            return len(lease_ids)

    gateway = NoResultGateway()
    with pytest.raises(TemporalSearchTimeout):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=0.01,
            poll_interval_seconds=0.01,
            enqueue_batch_size=2,
        )
    assert gateway.batch_sizes == [2, 2, 1]


def test_resume_waits_for_explicitly_duplicate_live_pending_task(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)

    def reject_existing(tasks: list[dict]) -> dict:
        gateway.enqueued.extend(tasks)
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "enqueued": 0,
            "rejected": len(tasks),
        }

    gateway.enqueue_tasks = reject_existing  # type: ignore[method-assign]

    result = run_temporal_search_tasks(
        gateway,
        authority,
        output_root=tmp_path,
        timeout_seconds=1,
        resume=True,
    )

    assert result["completedTaskCount"] == 1
    assert gateway.acks == ["lease-1"]


@pytest.mark.parametrize(
    ("resume", "reported_rejected"),
    [(False, 1), (True, 0)],
)
def test_controller_rejects_incomplete_or_ambiguous_enqueue_receipt(
    tmp_path: Path,
    resume: bool,
    reported_rejected: int,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    gateway.enqueue_tasks = lambda tasks: {  # type: ignore[method-assign]
        "submitted": len(tasks),
        "enqueued": 0,
        "rejected": reported_rejected,
    }

    with pytest.raises(
        TemporalSearchContractError,
        match="exact pending task set",
    ):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=1,
            resume=resume,
        )


def test_result_rejects_different_cost_observation_streams(tmp_path: Path) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]
    gateway = _Gateway(task)
    original = gateway.read_results

    def broken(*, limit: int):
        rows = original(limit=limit)
        if rows:
            rows[0]["result"]["result"]["cost_view_results"]["none"][
                "observation_stream_sha256"
            ] = "sha256:" + "f" * 64
        return rows

    gateway.read_results = broken  # type: ignore[method-assign]
    with pytest.raises(
        TemporalSearchContractError, match="identical observation stream"
    ):
        run_temporal_search_tasks(
            gateway, authority, output_root=tmp_path, timeout_seconds=1
        )


def test_controller_persists_and_acknowledges_failed_completion_before_tripwire(
    tmp_path: Path,
) -> None:
    authority = build_authority(_preparation())
    task = build_task_matrix(authority)[0]

    class FailedGateway:
        def __init__(self):
            self.enqueued = False
            self.delivered = False
            self.acks = []

        def enqueue_tasks(self, tasks):
            self.enqueued = True
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit):
            if not self.enqueued or self.delivered:
                return []
            self.delivered = True
            return [
                {
                    "status": "failed",
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "lease_id": "failed-lease",
                    "error": {"type": "fixture_failure", "message": "boom"},
                }
            ]

        def ack_results(self, lease_ids):
            self.acks.extend(lease_ids)
            return len(lease_ids)

    gateway = FailedGateway()
    with pytest.raises(
        TemporalSearchContractError,
        match=f"worker completion failed for {task['task_id']}",
    ):
        run_temporal_search_tasks(
            gateway,
            authority,
            output_root=tmp_path,
            timeout_seconds=1,
        )
    assert gateway.acks == ["failed-lease"]
    failure = json.loads(
        (tmp_path / "failures" / f"{task['task_id']}.json").read_text()
    )
    assert failure["error"]["type"] == "fixture_failure"


def test_procman_normal_operations_is_temporal_search_topology() -> None:
    root = Path(__file__).resolve().parents[1]
    config_path = root / "scripts" / "processes.json"
    if not config_path.is_file():
        pytest.skip("local Procman configuration is intentionally untracked")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    processes = {item["id"]: item for item in config["processes"]}
    normal = next(
        item for item in config["groups"] if item["name"] == "Normal Operations"
    )
    names = {processes[item]["name"] for item in normal["process_ids"]}
    assert names == {
        "Lab Gateway",
        "Temporal QD Broad Search (10k)",
        "AutoResearch Dashboard",
    }
    assert not any(item["name"].startswith("Phase 3 ") for item in processes.values())
    supervisor = next(
        processes[item]
        for item in normal["process_ids"]
        if processes[item]["name"] == "Temporal QD Broad Search (10k)"
    )
    assert "temporal-qd-supervisor" in supervisor["command"]
    assert "--generation-count 4" in supervisor["command"]
    assert "--broad-admission" in supervisor["command"]
