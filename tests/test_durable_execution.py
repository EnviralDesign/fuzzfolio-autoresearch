from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.durable_execution as durable_execution

from autoresearch.durable_execution import (
    DurableExecutionError,
    DurableExecutionJournal,
    V1_JOURNAL_SCHEMA,
    artifact_receipt,
    validate_artifact_receipt,
)
from autoresearch.evidence_plan import canonical_sha256


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


def test_journal_applies_registrations_and_completions_with_one_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.load(create=True)
    appends = 0
    real_append = DurableExecutionJournal._append_records

    def counted_append(self: DurableExecutionJournal, records: list[dict]) -> None:
        nonlocal appends
        appends += 1
        real_append(self, records)

    monkeypatch.setattr(DurableExecutionJournal, "_append_records", counted_append)

    updated = journal.apply_batch(
        registrations=[
            ("task-1", {"value": 1}),
            ("task-2", {"value": 2}),
        ],
        completions=[("task-1", {"status": "calculated"})],
    )

    assert appends == 1
    assert updated["tasks"]["task-1"]["status"] == "terminal"
    assert updated["tasks"]["task-2"]["status"] == "pending"

    journal.apply_batch(
        registrations=[("task-1", {"value": 1})],
        completions=[("task-1", {"status": "calculated"})],
    )
    assert appends == 1


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
    assert "task-2" not in journal.load()["tasks"]


def test_artifact_receipt_rejects_partial_or_mutated_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "stage" / "result.json"
    artifact.parent.mkdir()
    artifact.write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    receipt = artifact_receipt([artifact], root=tmp_path)

    assert validate_artifact_receipt(receipt)["files"]
    artifact.write_text(json.dumps({"status": "partial"}), encoding="utf-8")
    with pytest.raises(DurableExecutionError, match="verification failed"):
        validate_artifact_receipt(receipt)


def test_v1_whole_file_journal_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    payload = {
        "schema_version": V1_JOURNAL_SCHEMA,
        "execution_id": "plan-1",
        "lineage": {"cutoff": "A"},
        "tasks": {
            "task-1": {
                "task_id": "task-1",
                "payload_sha256": "sha256:dead",
                "payload": {"value": 1},
                "status": "pending",
                "terminal_receipt": None,
            }
        },
    }
    payload["journal_identity"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "journal_identity"}
    )
    path.write_text(json.dumps(payload), encoding="utf-8")

    journal = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    with pytest.raises(DurableExecutionError, match="retired v1 whole-file format"):
        journal.load()


def test_apply_batch_appends_without_rewriting_prefix(tmp_path: Path) -> None:
    path = tmp_path / "journal.json"
    journal = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.load(create=True)
    header_line = path.read_text(encoding="utf-8").splitlines()[0]
    size_after_create = path.stat().st_size

    journal.apply_batch(
        registrations=[
            ("task-1", {"value": 1}),
            ("task-2", {"value": 2}),
        ],
        completions=[("task-1", {"status": "calculated"})],
    )
    size_after_first = path.stat().st_size
    assert size_after_first > size_after_create
    assert path.read_text(encoding="utf-8").splitlines()[0] == header_line

    journal.apply_batch(registrations=[("task-3", {"value": 3})])
    size_after_second = path.stat().st_size
    assert size_after_second > size_after_first
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == header_line
    assert json.loads(lines[0])["record_type"] == "header"
    assert any(json.loads(line).get("task_id") == "task-3" for line in lines[1:])


def test_warm_apply_batch_does_not_reread_journal_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.json"
    journal = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.load(create=True)
    journal.apply_batch(registrations=[("task-1", {"value": 1})])

    reads = 0
    real_read_text = Path.read_text

    def counted_read_text(self: Path, *args: object, **kwargs: object) -> str:
        nonlocal reads
        if self.resolve(strict=False) == path.resolve(strict=False):
            reads += 1
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted_read_text)
    journal.apply_batch(registrations=[("task-2", {"value": 2})])
    assert reads == 0
    assert set(journal.load()["tasks"]) == {"task-1", "task-2"}


def test_revoke_terminal_clears_receipt_and_replays(tmp_path: Path) -> None:
    path = tmp_path / "journal.json"
    journal = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.register("task-1", {"value": 1})
    journal.complete("task-1", {"status": "calculated"})
    assert journal.terminal("task-1") is not None

    revoked = journal.revoke_terminal("task-1")
    assert revoked["status"] == "pending"
    assert revoked["terminal_receipt"] is None
    assert journal.terminal("task-1") is None

    # Duplicate revoke on pending is a no-op (no new append needed for identity).
    before = path.read_bytes()
    again = journal.revoke_terminal("task-1")
    assert again["status"] == "pending"
    assert path.read_bytes() == before

    resumed = DurableExecutionJournal(
        path,
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    assert resumed.terminal("task-1") is None
    assert [task["task_id"] for task in resumed.unresolved()] == ["task-1"]

    with pytest.raises(DurableExecutionError, match="unknown task"):
        journal.revoke_terminal("missing-task")


def test_apply_batch_revocations_append_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.apply_batch(
        registrations=[("task-1", {"value": 1}), ("task-2", {"value": 2})],
        completions=[
            ("task-1", {"status": "calculated"}),
            ("task-2", {"status": "calculated"}),
        ],
    )
    appends = 0
    real_append = DurableExecutionJournal._append_records

    def counted_append(self: DurableExecutionJournal, records: list[dict]) -> None:
        nonlocal appends
        appends += 1
        real_append(self, records)

    monkeypatch.setattr(DurableExecutionJournal, "_append_records", counted_append)
    journal.apply_batch(revocations=["task-1", "task-2"])
    assert appends == 1
    assert journal.terminal("task-1") is None
    assert journal.terminal("task-2") is None


def test_view_tasks_map_is_detached_from_warm_cache(tmp_path: Path) -> None:
    journal = DurableExecutionJournal(
        tmp_path / "journal.json",
        execution_id="plan-1",
        lineage={"cutoff": "A"},
    )
    journal.register("task-1", {"value": 1})
    snapshot = journal.load()
    snapshot["tasks"].clear()
    assert "task-1" in journal.load()["tasks"]
    updated = journal.apply_batch(completions=[("task-1", {"status": "calculated"})])
    updated["tasks"].clear()
    assert journal.terminal("task-1") is not None
