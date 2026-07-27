from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_memory_deep
from autoresearch import play_hand_lab_policy_resume
from autoresearch.durable_execution import DurableExecutionError, DurableExecutionJournal


def _journal(path: Path) -> DurableExecutionJournal:
    return DurableExecutionJournal(
        path,
        execution_id="phase3-policy-resume-test",
        lineage={"campaign_id": "phase3-policy-resume-test"},
    )


def _load_compacted_terminal_task(
    tmp_path: Path,
    *,
    assignment: dict[str, Any],
) -> tuple[DurableExecutionJournal, dict[str, Any], str]:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    task_id = "phase3-policy-resume-test-lane-00000-task-00001-baseline_3mo"
    task = {
        "task_id": task_id,
        "lane_id": "lane_000",
        "attempt_id": task_id,
        "task_kind": "deep_replay",
        "payload": {
            "job_id": task_id,
            "inline_profile_snapshot": {"large": "x" * 10_000},
            "policy_assignment": assignment,
        },
    }
    writer = _journal(path)
    writer.apply_batch(
        registrations=[(task_id, task)],
        completions=[
            (
                task_id,
                {
                    "recorded_result": {
                        "task_id": task_id,
                        "status": "success",
                        "phase": "baseline_3mo",
                    }
                },
            )
        ],
    )

    reader = _journal(path)
    view = reader.load()
    durable_tasks = view["tasks"]
    assert "payload" not in durable_tasks[task_id]
    assert play_hand_lab_memory_deep._ACTIVE_PLAY_HAND_JOURNAL is reader
    return reader, durable_tasks, task_id


def _lane(
    tmp_path: Path,
    *,
    task_id: str,
    assignment: dict[str, Any],
) -> play_hand_lab.LabLaneState:
    return play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-policy-resume-test-lane-00000",
        run_dir=tmp_path / "lane-00000",
        task_ids=[task_id],
        completed_task_ids={task_id},
        task_specs={},
        policy_assignment=assignment,
    )


def test_policy_recompute_reads_compact_terminal_task_evidence_without_rehydrating_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
        "candidate_attributes": {"recipe": "trend"},
    }
    _reader, durable_tasks, task_id = _load_compacted_terminal_task(
        tmp_path,
        assignment=assignment,
    )
    lane = _lane(tmp_path, task_id=task_id, assignment=assignment)
    observed: dict[str, Any] = {}

    def fake_recompute(
        policy_state: dict[str, Any],
        *,
        lanes: list[Any],
        unresolved_tasks: list[dict[str, Any]],
        durable_tasks_by_id: Mapping[str, Any],
        pruned_lane_count: int,
    ) -> dict[str, Any]:
        task = durable_tasks_by_id.get(task_id)
        assert isinstance(task, dict)
        payload = task.get("payload")
        assert payload == {
            "task_id": task_id,
            "lane_id": "lane_000",
            "policy_assignment": assignment,
        }
        assert play_hand_lab._durable_task_policy_assignment(payload) == assignment
        observed["called"] = True
        return {"status": "verified"}

    monkeypatch.setattr(
        play_hand_lab_policy_resume,
        "_ORIGINAL_RECOMPUTE_POLICY_STATE",
        fake_recompute,
    )

    result = play_hand_lab._recompute_campaign_policy_state_from_durable_lanes(
        {"schema_version": "test"},
        lanes=[lane],
        unresolved_tasks=[],
        durable_tasks_by_id=durable_tasks,
        pruned_lane_count=0,
    )

    assert result == {"status": "verified"}
    assert observed == {"called": True}
    # The compact overlay must never inflate the warm journal/task-map cache again.
    assert "payload" not in durable_tasks[task_id]


def test_policy_recompute_restores_proof_for_a_compacted_terminal_shard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
    }
    _reader, durable_tasks, task_id = _load_compacted_terminal_task(
        tmp_path,
        assignment=assignment,
    )
    lane = _lane(tmp_path, task_id=task_id, assignment=assignment)
    # The deep-memory reducer keeps the shard's merge fields but drops this
    # potentially large assignment. The resume wrapper must retrieve only the
    # sealed assignment proof from the register record.
    lane.task_specs[task_id] = {
        "phase": "coarse_expand",
        "task_kind": "sweep_shard",
        "sweep_id": "coarse-expand-001",
    }
    observed: dict[str, Any] = {}

    def fake_recompute(
        policy_state: dict[str, Any],
        *,
        lanes: list[Any],
        unresolved_tasks: list[dict[str, Any]],
        durable_tasks_by_id: Mapping[str, Any],
        pruned_lane_count: int,
    ) -> dict[str, Any]:
        payload = durable_tasks_by_id[task_id]["payload"]
        assert payload["policy_assignment"] == assignment
        observed["called"] = True
        return {"status": "verified"}

    monkeypatch.setattr(
        play_hand_lab_policy_resume,
        "_ORIGINAL_RECOMPUTE_POLICY_STATE",
        fake_recompute,
    )
    result = play_hand_lab._recompute_campaign_policy_state_from_durable_lanes(
        {"schema_version": "test"},
        lanes=[lane],
        unresolved_tasks=[],
        durable_tasks_by_id=durable_tasks,
        pruned_lane_count=0,
    )

    assert result == {"status": "verified"}
    assert observed == {"called": True}
    assert "payload" not in durable_tasks[task_id]


def test_policy_resume_rejects_terminal_task_assignment_conflict(tmp_path: Path) -> None:
    journal_assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
    }
    reader, durable_tasks, task_id = _load_compacted_terminal_task(
        tmp_path,
        assignment=journal_assignment,
    )
    lane = _lane(
        tmp_path,
        task_id=task_id,
        assignment={
            "policy_lane": "wild",
            "policy_manifest_sha256": "sha256:" + "a" * 64,
        },
    )

    with pytest.raises(
        DurableExecutionError,
        match="durable task policy assignment mismatch",
    ):
        play_hand_lab_policy_resume._compact_policy_payloads_from_journal(
            play_hand_lab,
            lanes=[lane],
            durable_tasks_by_id=durable_tasks,
            journal=reader,
        )


def test_policy_resume_recovery_is_installed_on_package_import() -> None:
    assert (
        play_hand_lab._recompute_campaign_policy_state_from_durable_lanes.__module__
        == "autoresearch.play_hand_lab_policy_resume"
    )
