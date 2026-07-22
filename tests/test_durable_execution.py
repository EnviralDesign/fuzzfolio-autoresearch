from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.durable_execution as durable_execution

from autoresearch.durable_execution import (
    DurableExecutionError,
    DurableExecutionJournal,
    artifact_receipt,
    validate_artifact_receipt,
)


def test_atomic_write_retries_transient_windows_replace_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "journal.json"
    real_replace = durable_execution.os.replace
    attempts = 0

    def transient_replace(source: Path, destination: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "Access is denied", str(destination))
        real_replace(source, destination)

    monkeypatch.setattr(durable_execution.os, "replace", transient_replace)
    monkeypatch.setattr(durable_execution.time, "sleep", lambda _seconds: None)

    durable_execution.atomic_write_json(target, {"status": "complete"})

    assert attempts == 3
    assert json.loads(target.read_text(encoding="utf-8")) == {"status": "complete"}
    assert list(tmp_path.glob("*.tmp")) == []


def test_journal_replays_unresolved_work_and_accepts_identical_terminal_duplicate(
    tmp_path: Path,
) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="sha256:execution",
        lineage={"generation": "g1"},
    )
    journal.register("task-1", {"value": 1})
    journal.register("task-2", {"value": 2})
    journal.complete("task-1", {"status": "calculated", "artifact": "a"})

    resumed = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="sha256:execution",
        lineage={"generation": "g1"},
    )
    assert [task["task_id"] for task in resumed.unresolved()] == ["task-2"]
    resumed.complete("task-1", {"status": "calculated", "artifact": "a"})
    with pytest.raises(DurableExecutionError, match="conflicting duplicate"):
        resumed.complete("task-1", {"status": "calculated", "artifact": "different"})


def test_journal_fails_closed_on_payload_or_lineage_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "journal.json"
    journal = DurableExecutionJournal(path, execution_id="plan-1", lineage={"cutoff": "A"})
    journal.register("task-1", {"value": 1})

    with pytest.raises(DurableExecutionError, match="payload conflicts"):
        journal.register("task-1", {"value": 2})
    with pytest.raises(DurableExecutionError, match="lineage mismatch"):
        DurableExecutionJournal(
            path, execution_id="plan-1", lineage={"cutoff": "B"}
        ).load()


def test_task_payload_sha256_matches_json_normalized_registered_payload(
    tmp_path: Path,
) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    payload = {
        "phase": "coarse_probe",
        "params_by_index": {0: {"alpha": 1}, 1: {"alpha": 2}},
    }

    registered = journal.register("task-1", payload)

    assert registered["payload_sha256"] == journal.task_payload_sha256(payload)
    assert registered["payload"]["params_by_index"] == {
        "0": {"alpha": 1},
        "1": {"alpha": 2},
    }


def test_journal_owns_nested_task_and_terminal_payloads(tmp_path: Path) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A", "nested": {"version": 1}},
    )
    task = {"nested": {"value": "registered"}}
    terminal = {"status": "calculated", "evidence": {"value": "terminal"}}

    journal.register("task-1", task)
    task["nested"]["value"] = "caller-mutated"
    journal.complete("task-1", terminal)
    terminal["evidence"]["value"] = "caller-mutated"

    resumed = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A", "nested": {"version": 1}},
    )
    assert resumed.unresolved() == []
    task_receipt = resumed.terminal("task-1")
    assert task_receipt is not None
    assert task_receipt["terminal_receipt"]["payload"]["evidence"]["value"] == "terminal"


def test_journal_applies_registrations_and_completions_with_one_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.load(create=True)
    writes = 0
    real_atomic_write_json = durable_execution.atomic_write_json

    def counted_atomic_write_json(path: Path, payload: dict) -> None:
        nonlocal writes
        writes += 1
        real_atomic_write_json(path, payload)

    monkeypatch.setattr(
        durable_execution,
        "atomic_write_json",
        counted_atomic_write_json,
    )

    updated = journal.apply_batch(
        registrations=[
            ("task-1", {"value": 1}),
            ("task-2", {"value": 2}),
        ],
        completions=[("task-1", {"status": "calculated"})],
    )

    assert writes == 1
    assert updated["tasks"]["task-1"]["status"] == "terminal"
    assert updated["tasks"]["task-2"]["status"] == "pending"

    journal.apply_batch(
        registrations=[("task-1", {"value": 1})],
        completions=[("task-1", {"status": "calculated"})],
    )
    assert writes == 1


def test_journal_batch_conflict_does_not_persist_partial_changes(tmp_path: Path) -> None:
    path = tmp_path / "journal.json"
    journal = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.register("task-1", {"value": 1})
    before = path.read_bytes()

    with pytest.raises(DurableExecutionError, match="unknown task"):
        journal.apply_batch(
            registrations=[("task-2", {"value": 2})],
            completions=[("missing-task", {"status": "calculated"})],
        )

    assert path.read_bytes() == before


def test_artifact_receipt_rejects_partial_or_mutated_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "stage" / "result.json"
    artifact.parent.mkdir()
    artifact.write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    receipt = artifact_receipt([artifact], root=tmp_path)

    assert validate_artifact_receipt(receipt)["files"]
    artifact.write_text(json.dumps({"status": "partial"}), encoding="utf-8")
    with pytest.raises(DurableExecutionError, match="verification failed"):
        validate_artifact_receipt(receipt)
