from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.durable_execution import (
    DurableExecutionError,
    DurableExecutionJournal,
    artifact_receipt,
    validate_artifact_receipt,
)


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


def test_artifact_receipt_rejects_partial_or_mutated_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "stage" / "result.json"
    artifact.parent.mkdir()
    artifact.write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    receipt = artifact_receipt([artifact], root=tmp_path)

    assert validate_artifact_receipt(receipt)["files"]
    artifact.write_text(json.dumps({"status": "partial"}), encoding="utf-8")
    with pytest.raises(DurableExecutionError, match="verification failed"):
        validate_artifact_receipt(receipt)
