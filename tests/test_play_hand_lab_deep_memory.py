from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue
from autoresearch import play_hand_lab_memory_deep
from autoresearch.durable_execution import DurableExecutionError, DurableExecutionJournal
from autoresearch.evidence_plan import canonical_sha256


class _AcceptingGateway:
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "status": "accepted",
            "submitted": len(tasks),
            "accepted": len(tasks),
            "enqueued": len(tasks),
            "rejected": 0,
        }


def _journal(path: Path) -> DurableExecutionJournal:
    return DurableExecutionJournal(
        path,
        execution_id="phase3-deep-memory-test",
        lineage={"campaign_id": "phase3-deep-memory-test"},
    )


def test_heavy_enqueued_task_envelopes_are_compacted_immediately() -> None:
    task = {
        "task_id": "heavy-live-task",
        "lane_id": "lane-001",
        "attempt_id": "attempt-001",
        "task_kind": "sweep_shard",
        "payload": {
            "base_profile_snapshot": {"large": "x" * 50_000},
            "params_by_index": {"0": {"large": "y" * 50_000}},
        },
    }

    result = play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries(
        _AcceptingGateway(),
        object(),
        [task],
        reason="lane_top_up",
        failure_limit=1,
        retry_base_seconds=0,
    )

    assert result["accepted"] == 1
    assert result["released_task_payload_copies"] >= 1
    assert task == {
        "task_id": "heavy-live-task",
        "lane_id": "lane-001",
        "attempt_id": "attempt-001",
        "task_kind": "sweep_shard",
    }


def test_streaming_reload_starts_with_terminal_records_compacted(tmp_path: Path) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    writer = _journal(path)
    terminal_payload = {
        "task_id": "terminal-task",
        "lane_id": "lane-001",
        "payload": {"large": "x" * 100_000},
    }
    pending_payload = {
        "task_id": "pending-task",
        "lane_id": "lane-002",
        "payload": {"large": "y" * 100_000},
    }
    receipt = {
        "recorded_result": {
            "status": "success",
            "sweep_payload": {"ranked": [{"large": "z" * 100_000}]},
        }
    }
    writer.apply_batch(
        registrations=[
            ("terminal-task", terminal_payload),
            ("pending-task", pending_payload),
        ],
        completions=[("terminal-task", receipt)],
    )
    disk_before = path.read_bytes()

    reader = _journal(path)
    view = reader.load()

    assert path.read_bytes() == disk_before
    assert "payload" not in view["tasks"]["terminal-task"]
    assert view["tasks"]["terminal-task"]["terminal_receipt"].keys() == {
        "receipt_sha256"
    }
    assert view["tasks"]["pending-task"]["payload"] == pending_payload

    restored = reader.terminal("terminal-task")
    assert restored is not None
    assert restored["payload"] == terminal_payload
    assert restored["terminal_receipt"]["payload"] == receipt


def test_streaming_reload_restores_a_revoked_task_as_pending(tmp_path: Path) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    writer = _journal(path)
    payload = {
        "task_id": "revoked-task",
        "lane_id": "lane-003",
        "payload": {"large": "r" * 50_000},
    }
    receipt = {"recorded_result": {"status": "success"}}
    writer.apply_batch(
        registrations=[("revoked-task", payload)],
        completions=[("revoked-task", receipt)],
        revocations=["revoked-task"],
    )

    reader = _journal(path)
    view = reader.load()

    assert view["tasks"]["revoked-task"]["status"] == "pending"
    assert view["tasks"]["revoked-task"]["payload"] == payload
    unresolved = reader.unresolved()
    assert [item["task_id"] for item in unresolved] == ["revoked-task"]
    assert unresolved[0]["payload"] == payload


def test_summary_samples_do_not_retain_full_sweep_payloads() -> None:
    ranked = [
        {
            "permutation_index": index,
            "child_job_id": f"child-{index}",
            "status": "success",
            "score": 80.0 - index,
            "fitness_value": 80.0 - index,
            "parameters": {"axis": index},
            "large": "x" * 20_000,
        }
        for index in range(8)
    ]
    sweep_payload = {
        "sweep_id": "sweep-001",
        "shard_id": "shard-001",
        "outcome": "scored",
        "permutation_indices": list(range(8)),
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": ranked[0],
        "failed_permutations": [],
    }
    recorded = {
        "task_id": "sample-task",
        "attempt_id": "sample-attempt",
        "artifact_dir": "C:/tmp/sample",
        "score": 80.0,
        "status": "success",
        "phase": "focused",
        "task_kind": "sweep_shard",
        "policy_assignment": {"policy_lane": "guided"},
        "sweep_payload": sweep_payload,
    }
    samples: list[dict[str, Any]] = []
    source_size = len(json.dumps(recorded))

    play_hand_lab._add_recorded_result_sample(samples, recorded)

    assert len(samples) == 1
    assert "sweep_payload" not in samples[0]
    assert samples[0]["sweep_summary"] == {
        "sweep_id": "sweep-001",
        "shard_id": "shard-001",
        "outcome": "scored",
        "permutation_count": 8,
        "scored_count": 8,
        "failed_count": 0,
        "best": {
            "permutation_index": 0,
            "child_job_id": "child-0",
            "status": "success",
            "score": 80.0,
            "fitness_value": 80.0,
            "parameters": {"axis": 0},
        },
    }
    assert recorded["sweep_payload"] is sweep_payload
    assert len(json.dumps(samples[0])) < source_size // 10


def test_compacted_terminal_sweep_spec_omits_policy_assignment() -> None:
    policy_assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
    }
    spec = {
        "phase": "coarse_expand",
        "task_kind": "sweep_shard",
        "sweep_id": "coarse-expand-001",
        "params_by_index": {"0": {"period": 14}},
        "policy_assignment": policy_assignment,
        "large_transient_payload": "x" * 10_000,
    }

    play_hand_lab_memory_deep._compact_sweep_task_spec(spec)

    assert "policy_assignment" not in spec
    assert "params_by_index" not in spec
    assert "large_transient_payload" not in spec


def _sealed_terminal_sweep_journal(tmp_path: Path) -> tuple[DurableExecutionJournal, dict[str, Any]]:
    task_id = "phase3-lane-00001-task-00001-coarse_probe-shard-0000"
    params_by_index = {
        "0": {"indicator[0].talib.timeperiod": 8},
        "1": {"indicator[0].talib.timeperiod": 14},
    }
    envelope = {
        "task_id": task_id,
        "lane_id": "lane_0001",
        "task_kind": "sweep_shard",
        "payload": {
            "sweep_id": "lane-00001-coarse-probe",
            "shard_id": "lane-00001-coarse-probe-shard-0000",
            "params_by_index": params_by_index,
        },
    }
    journal = _journal(tmp_path / "play-hand-lab-execution-journal.json")
    journal.apply_batch(
        registrations=[(task_id, envelope)],
        completions=[(task_id, {"recorded_result": {"status": "success"}})],
    )
    reader = _journal(journal.path)
    reader.load()
    return reader, envelope


def test_compacted_sweep_merge_restores_sealed_register_params(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal, envelope = _sealed_terminal_sweep_journal(tmp_path)
    task_id = str(envelope["task_id"])
    params_by_index = envelope["payload"]["params_by_index"]
    spec = {
        "task_kind": "sweep_shard",
        "sweep_id": envelope["payload"]["sweep_id"],
        "shard_id": envelope["payload"]["shard_id"],
        "params_by_index_sha256": canonical_sha256(params_by_index),
    }

    class _Lane:
        lane_id = "lane_0001"
        phase_task_ids = {"coarse_probe": [task_id]}
        task_specs = {task_id: spec}

    monkeypatch.setattr(play_hand_lab_memory_deep, "_ACTIVE_PLAY_HAND_JOURNAL", journal)
    monkeypatch.setattr(
        play_hand_lab,
        "_rebuild_sweep_shard_params_by_index",
        lambda *_args: pytest.fail("sealed task should not be recomputed from mutable lane state"),
    )

    restored = play_hand_lab_memory_deep._restore_sweep_params(
        play_hand_lab,
        _Lane(),
        "coarse_probe",
    )

    assert restored == [task_id]
    assert spec["params_by_index"] == params_by_index


def test_compacted_sweep_merge_rejects_sealed_param_hash_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal, envelope = _sealed_terminal_sweep_journal(tmp_path)
    task_id = str(envelope["task_id"])
    spec = {
        "task_kind": "sweep_shard",
        "sweep_id": envelope["payload"]["sweep_id"],
        "shard_id": envelope["payload"]["shard_id"],
        "params_by_index_sha256": "sha256:" + "0" * 64,
    }

    class _Lane:
        lane_id = "lane_0001"
        phase_task_ids = {"coarse_probe": [task_id]}
        task_specs = {task_id: spec}

    monkeypatch.setattr(play_hand_lab_memory_deep, "_ACTIVE_PLAY_HAND_JOURNAL", journal)

    with pytest.raises(DurableExecutionError, match="sealed sweep params conflict"):
        play_hand_lab_memory_deep._restore_sweep_params(
            play_hand_lab,
            _Lane(),
            "coarse_probe",
        )


def test_compacted_sweep_merge_requires_a_sealed_param_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal, envelope = _sealed_terminal_sweep_journal(tmp_path)
    task_id = str(envelope["task_id"])
    spec = {
        "task_kind": "sweep_shard",
        "sweep_id": envelope["payload"]["sweep_id"],
        "shard_id": envelope["payload"]["shard_id"],
    }

    class _Lane:
        lane_id = "lane_0001"
        phase_task_ids = {"coarse_probe": [task_id]}
        task_specs = {task_id: spec}

    monkeypatch.setattr(play_hand_lab_memory_deep, "_ACTIVE_PLAY_HAND_JOURNAL", journal)

    with pytest.raises(DurableExecutionError, match="no params identity"):
        play_hand_lab_memory_deep._restore_sweep_params(
            play_hand_lab,
            _Lane(),
            "coarse_probe",
        )


def test_compacted_sweep_merge_falls_back_only_when_task_is_not_in_active_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal, _envelope = _sealed_terminal_sweep_journal(tmp_path)
    task_id = "different-campaign-lane-00001-task-00001-coarse_probe-shard-0000"
    rebuilt = {"0": {"indicator[0].talib.timeperiod": 8}}
    spec = {
        "task_kind": "sweep_shard",
        "params_by_index_sha256": canonical_sha256(rebuilt),
    }

    class _Lane:
        lane_id = "lane_0001"
        phase_task_ids = {"coarse_probe": [task_id]}
        task_specs = {task_id: spec}

    monkeypatch.setattr(play_hand_lab_memory_deep, "_ACTIVE_PLAY_HAND_JOURNAL", journal)
    monkeypatch.setattr(
        play_hand_lab,
        "_rebuild_sweep_shard_params_by_index",
        lambda *_args: rebuilt,
    )

    restored = play_hand_lab_memory_deep._restore_sweep_params(
        play_hand_lab,
        _Lane(),
        "coarse_probe",
    )

    assert restored == [task_id]
    assert spec["params_by_index"] == rebuilt
