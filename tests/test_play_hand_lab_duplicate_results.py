from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_duplicate_results as duplicate_results
from autoresearch.durable_execution import DurableExecutionError


def _sealed_result(tmp_path: Path) -> tuple[Path, dict, dict]:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    (artifact_dir / "result.json").write_text('{"ok":true}', encoding="utf-8")
    task_id = "phase3-test-task-0001"
    recorded = {
        "task_id": task_id,
        "artifact_dir": str(artifact_dir),
        "status": "success",
        "score": 71.25,
    }
    receipt = play_hand_lab._write_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256="sha256:" + "a" * 64,
        recorded_result=recorded,
    )
    row = {
        "lab_campaign_task_id": task_id,
        "lab_worker_result_sha256": "sha256:" + "a" * 64,
        "artifact_dir": str(artifact_dir),
    }
    return artifact_dir, receipt, row


def test_valid_sealed_receipt_is_authoritative_for_ledger_row(tmp_path: Path) -> None:
    _artifact_dir, receipt, row = _sealed_result(tmp_path)

    recovered = duplicate_results._authoritative_receipt_for_ledger_row(
        play_hand_lab,
        task_id=str(row["lab_campaign_task_id"]),
        recovered_row=row,
    )

    assert recovered == receipt


def test_ledger_and_receipt_identity_mismatch_remains_fail_closed(tmp_path: Path) -> None:
    _artifact_dir, _receipt, row = _sealed_result(tmp_path)
    row["lab_worker_result_sha256"] = "sha256:" + "b" * 64

    with pytest.raises(
        DurableExecutionError,
        match="ledger and sealed receipt identities conflict",
    ):
        duplicate_results._authoritative_receipt_for_ledger_row(
            play_hand_lab,
            task_id=str(row["lab_campaign_task_id"]),
            recovered_row=row,
        )


def test_conflicting_late_worker_identity_uses_valid_sealed_receipt(
    tmp_path: Path,
) -> None:
    _artifact_dir, receipt, row = _sealed_result(tmp_path)
    task_id = str(row["lab_campaign_task_id"])

    validated = play_hand_lab._validate_task_result_receipt_payload(
        receipt,
        task_id=task_id,
        worker_result_sha256="sha256:" + "c" * 64,
    )

    assert validated["worker_result_sha256"] == "sha256:" + "a" * 64
    assert validated["recorded_result"] == receipt["recorded_result"]


def test_crash_window_recovers_recorded_result_from_sealed_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _artifact_dir, receipt, row = _sealed_result(tmp_path)
    lane_ctx = SimpleNamespace(attempts_path=tmp_path / "attempts.jsonl")
    monkeypatch.setattr(play_hand_lab, "load_attempts", lambda _path: [row])

    recovered = duplicate_results._recover_recorded_result_from_sealed_receipt(
        play_hand_lab,
        lane_ctx=lane_ctx,
        task_id=str(row["lab_campaign_task_id"]),
    )

    assert recovered == receipt["recorded_result"]


def test_terminal_receipt_keeps_first_worker_identity_after_rerun(
    tmp_path: Path,
) -> None:
    _artifact_dir, receipt, row = _sealed_result(tmp_path)
    task_id = str(row["lab_campaign_task_id"])
    rerun_result = {
        "task_id": task_id,
        "lease_id": "new-lease",
        "worker_id": "different-worker",
        "result": {
            "result": {"aggregate": {"score": 71.25}},
            "completed_at": "2026-07-24T00:00:00Z",
        },
    }
    assert play_hand_lab._worker_result_identity(rerun_result) != receipt[
        "worker_result_sha256"
    ]

    terminal = play_hand_lab._terminal_receipt_for_result(
        dict(receipt["recorded_result"]),
        rerun_result,
    )

    assert terminal == receipt
    assert terminal["worker_result_sha256"] == "sha256:" + "a" * 64
