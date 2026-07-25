from __future__ import annotations

from pathlib import Path
from typing import Any

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue
from autoresearch import play_hand_lab_memory
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
    assert play_hand_lab_memory.release_checkpointed_task_payloads(["resume-task-001"]) == 2


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
    assert play_hand_lab_memory.release_checkpointed_task_payloads(
        ["lane-001", "lane-002"]
    ) == 2


def test_checkpointed_payload_release_is_scoped_and_idempotent() -> None:
    completed = {
        "task_id": "steady-done",
        "lane_id": "lane-010",
        "attempt_id": "attempt-done",
        "task_kind": "sweep_shard",
        "phase": "focused",
        "payload": {
            "inline_profile_snapshot": {"large": "x" * 20_000},
            "params_by_index": {"0": {"large": "y" * 20_000}},
        },
    }
    live = {
        "task_id": "steady-live",
        "lane_id": "lane-010",
        "payload": {"large": "z" * 20_000},
    }
    live_before = {"task_id": live["task_id"], "lane_id": live["lane_id"], "payload": dict(live["payload"])}
    play_hand_lab_memory.track_live_task_payloads([completed, live])

    assert play_hand_lab_memory.release_checkpointed_task_payloads(["steady-done"]) == 1
    assert completed == {
        "task_id": "steady-done",
        "lane_id": "lane-010",
        "attempt_id": "attempt-done",
        "task_kind": "sweep_shard",
        "phase": "focused",
    }
    assert live == live_before
    assert play_hand_lab_memory.release_checkpointed_task_payloads(["steady-done"]) == 0
    assert play_hand_lab_memory.release_checkpointed_task_payloads(["steady-live"]) == 1


def test_resume_transient_copy_is_untracked_without_compacting_global_copy() -> None:
    global_task = {
        "task_id": "resume-shared",
        "lane_id": "lane-020",
        "payload": {"large": "g" * 10_000},
    }
    transient_task = {
        "task_id": "resume-shared",
        "lane_id": "lane-020",
        "payload": {"large": "t" * 10_000},
    }
    transient_list = [transient_task]
    play_hand_lab_memory.track_live_task_payloads([global_task, transient_task])

    play_hand_lab_memory.release_resume_enqueue_memory(transient_list)

    assert transient_list == []
    assert "payload" in transient_task
    assert play_hand_lab_memory.release_checkpointed_task_payloads(["resume-shared"]) == 1
    assert "payload" not in global_task
    assert "payload" in transient_task


def test_journal_completion_compacts_the_tracked_worker_envelope(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    task = {
        "task_id": "steady-journal-task",
        "lane_id": "lane-030",
        "attempt_id": "attempt-030",
        "task_kind": "sweep_shard",
        "phase": "coarse",
        "payload": {
            "profile": {"large": "p" * 25_000},
            "params_by_index": {"0": {"large": "q" * 25_000}},
        },
    }
    receipt = {"recorded_result": {"status": "success"}}
    play_hand_lab_memory.track_live_task_payloads([task])

    journal.apply_batch(registrations=[(task["task_id"], task)])
    disk_after_registration = journal.path.read_text(encoding="utf-8")
    assert "params_by_index" in disk_after_registration

    journal.apply_batch(completions=[(task["task_id"], receipt)])

    assert task == {
        "task_id": "steady-journal-task",
        "lane_id": "lane-030",
        "attempt_id": "attempt-030",
        "task_kind": "sweep_shard",
        "phase": "coarse",
    }
    disk_after_completion = journal.path.read_text(encoding="utf-8")
    assert "params_by_index" in disk_after_completion
    restored = journal.terminal("steady-journal-task")
    assert restored is not None
    assert restored["payload"]["payload"]["params_by_index"]["0"]["large"]
