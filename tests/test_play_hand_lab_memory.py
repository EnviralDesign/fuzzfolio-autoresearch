from __future__ import annotations

from pathlib import Path
from typing import Any

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue
from autoresearch.durable_execution import DurableExecutionJournal


def _journal(tmp_path: Path) -> DurableExecutionJournal:
    return DurableExecutionJournal(
        tmp_path / "play-hand-lab-execution-journal.json",
        execution_id="phase3-memory-test",
        lineage={"campaign_id": "phase3-memory-test"},
    )


class _AcceptingGateway:
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "accepted": len(tasks),
            "enqueued": len(tasks),
            "rejected": 0,
        }


def test_unresolved_compacts_only_the_in_memory_terminal_cache(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    terminal_payload = {
        "task_id": "done-task",
        "lane_id": "lane-001",
        "payload": {"large": "x" * 10_000},
    }
    pending_payload = {
        "task_id": "pending-task",
        "lane_id": "lane-002",
        "payload": {"large": "y" * 10_000},
    }
    journal.apply_batch(
        registrations=[
            ("done-task", terminal_payload),
            ("pending-task", pending_payload),
        ],
        completions=[
            (
                "done-task",
                {"recorded_result": {"status": "success", "detail": "z" * 10_000}},
            )
        ],
    )
    disk_before = journal.path.read_bytes()

    unresolved = journal.unresolved()

    assert journal.path.read_bytes() == disk_before
    assert [item["task_id"] for item in unresolved] == ["pending-task"]
    assert unresolved[0]["payload"] == pending_payload
    cached_terminal = journal._tasks["done-task"]
    assert "payload" not in cached_terminal
    assert cached_terminal["terminal_receipt"] == {
        "receipt_sha256": journal.terminal("done-task")["terminal_receipt"]["receipt_sha256"]
    }


def test_compacted_terminal_records_remain_readable_and_revocable(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    task_payload = {
        "task_id": "task-001",
        "lane_id": "lane-001",
        "payload": {"profile": {"large": "x" * 2_000}},
    }
    receipt = {"recorded_result": {"status": "success", "large": "r" * 2_000}}
    journal.apply_batch(
        registrations=[("task-001", task_payload)],
        completions=[("task-001", receipt)],
    )
    journal.unresolved()

    restored_terminal = journal.terminal("task-001")
    assert restored_terminal is not None
    assert restored_terminal["payload"] == task_payload
    assert restored_terminal["terminal_receipt"]["payload"] == receipt

    revoked = journal.revoke_terminal("task-001")
    assert revoked["status"] == "pending"
    assert revoked["payload"] == task_payload
    assert [item["task_id"] for item in journal.unresolved()] == ["task-001"]


def test_new_completions_are_compacted_after_resume_mode_is_enabled(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    payload = {
        "task_id": "pending-task",
        "lane_id": "lane-001",
        "payload": {"large": "x" * 5_000},
    }
    receipt = {"recorded_result": {"status": "success", "large": "r" * 5_000}}
    journal.apply_batch(registrations=[("pending-task", payload)])
    journal.unresolved()

    journal.apply_batch(completions=[("pending-task", receipt)])

    assert "payload" not in journal._tasks["pending-task"]
    restored = journal.terminal("pending-task")
    assert restored is not None
    assert restored["payload"] == payload
    assert restored["terminal_receipt"]["payload"] == receipt


def test_attached_resume_tasks_share_heavy_immutable_payloads(tmp_path: Path) -> None:
    task = {
        "task_id": "resume-task-001",
        "lane_id": "lane-001",
        "attempt_id": "attempt-001",
        "task_kind": "deep_replay",
        "payload": {
            "inline_profile_snapshot": {
                "profile": {"indicators": [{"large": "x" * 10_000}]}
            },
            "params_by_index": {"0": {"large": "y" * 10_000}},
        },
    }

    first = play_hand_lab._attach_task_profile_snapshots(task, tmp_path)
    second = play_hand_lab._attach_task_profile_snapshots(task, tmp_path)

    assert first == second
    assert first is not second
    assert first["payload"] is not second["payload"]
    assert (
        first["payload"]["inline_profile_snapshot"]
        is second["payload"]["inline_profile_snapshot"]
    )
    assert first["payload"]["params_by_index"] is second["payload"]["params_by_index"]
    first["payload"]["local_only"] = True
    assert "local_only" not in second["payload"]


def test_successful_resume_enqueue_releases_only_the_transient_input_list() -> None:
    tasks = [{"task_id": "resume-001"}, {"task_id": "resume-002"}]

    result = play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries(
        _AcceptingGateway(),
        object(),
        tasks,
        reason="resume_unresolved",
        failure_limit=1,
        retry_base_seconds=0,
    )

    assert result["accepted"] == 2
    assert tasks == []


def test_normal_enqueue_keeps_the_callers_task_list_intact() -> None:
    tasks = [{"task_id": "lane-001"}, {"task_id": "lane-002"}]

    result = play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries(
        _AcceptingGateway(),
        object(),
        tasks,
        reason="lane_top_up",
        failure_limit=1,
        retry_base_seconds=0,
    )

    assert result["accepted"] == 2
    assert [task["task_id"] for task in tasks] == ["lane-001", "lane-002"]
