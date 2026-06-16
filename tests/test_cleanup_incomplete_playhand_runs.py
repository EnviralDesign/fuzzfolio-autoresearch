from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import autoresearch.__main__ as ar_main
from autoresearch.ledger import write_attempts, write_run_metadata


def _build_run(
    runs_root: Path,
    run_id: str,
    metadata: dict[str, object],
    attempts: list[dict[str, object]] | None = None,
    last_modified_seconds_ago: int | None = None,
) -> Path:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_run_metadata(run_dir, {"run_id": run_id, **metadata})
    if attempts is not None:
        write_attempts(run_dir / "attempts.jsonl", attempts)
    if last_modified_seconds_ago is not None:
        when = time.time() - float(last_modified_seconds_ago)
        os.utime(run_dir, (when, when))
    return run_dir


def test_cleanup_incomplete_playhand_runs_dry_run_matches_only_in_progress_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    _build_run(
        runs_root,
        "run-incomplete",
        {"runner": "play_hand_v1"},
        [
            {
                "attempt_id": "run-incomplete-attempt-00001",
                "run_id": "run-incomplete",
                "runner": "play_hand_v1",
            }
        ],
    )
    _build_run(
        runs_root,
        "run-missing-artifacts",
        {
            "runner": "play_hand_v1",
            "final_attempt_id": "run-missing-artifacts-attempt-00001",
            "final_scrutiny_score": 45.0,
            "final_scrutiny_passed": True,
        },
        [
            {
                "attempt_id": "run-missing-artifacts-attempt-00001",
                "run_id": "run-missing-artifacts",
                "runner": "play_hand_v1",
            }
        ],
    )
    _build_run(
        runs_root,
        "run-finalized",
        {
            "runner": "play_hand_v1",
            "canonical_attempt_id": "run-finalized-attempt-00001",
            "final_attempt_id": "run-finalized-attempt-00001",
            "final_scrutiny_score": 61.0,
            "final_scrutiny_passed": True,
            "calendar_gate": {"passed": True},
            "calendar_gate_mode": "report",
        },
        [
            {
                "attempt_id": "run-finalized-attempt-00001",
                "run_id": "run-finalized",
                "runner": "play_hand_v1",
            }
        ],
    )
    _build_run(runs_root, "run-non-playhand", {"runner": "some-other-runner"})

    monkeypatch.setattr(
        ar_main,
        "load_config",
        lambda: SimpleNamespace(runs_root=runs_root),
    )

    exit_code = ar_main.cmd_cleanup_incomplete_playhand_runs(
        run_ids=None,
        older_than_minutes=None,
        dry_run=True,
        preview=20,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["matched_runs"] == 1
    assert payload["preview"][0]["run_id"] == "run-incomplete"

    assert (runs_root / "run-incomplete").exists()
    assert (runs_root / "run-missing-artifacts").exists()


def test_cleanup_incomplete_playhand_runs_executes_with_age_filter_and_run_id_targeting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    _build_run(
        runs_root,
        "run-old",
        {"runner": "play_hand_v1"},
        [
            {
                "attempt_id": "run-old-attempt-00001",
                "run_id": "run-old",
                "runner": "play_hand_v1",
            }
        ],
        last_modified_seconds_ago=180,
    )
    _build_run(
        runs_root,
        "run-fresh",
        {"runner": "play_hand_v1"},
        [
            {
                "attempt_id": "run-fresh-attempt-00001",
                "run_id": "run-fresh",
                "runner": "play_hand_v1",
            }
        ],
        last_modified_seconds_ago=1,
    )

    monkeypatch.setattr(
        ar_main,
        "load_config",
        lambda: SimpleNamespace(runs_root=runs_root),
    )

    exit_code = ar_main.cmd_cleanup_incomplete_playhand_runs(
        run_ids=["run-old", "run-fresh"],
        older_than_minutes=2.0,
        dry_run=True,
        preview=20,
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["matched_runs"] == 1
    assert payload["preview"][0]["run_id"] == "run-old"

    exit_code = ar_main.cmd_cleanup_incomplete_playhand_runs(
        run_ids=["run-old", "run-fresh"],
        older_than_minutes=2.0,
        dry_run=False,
        preview=20,
        as_json=True,
    )
    delete_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert delete_payload["deleted_runs"] == 1
    assert delete_payload["deleted_preview"][0]["run_id"] == "run-old"
    assert not (runs_root / "run-old").exists()
    assert (runs_root / "run-fresh").exists()


def test_cleanup_incomplete_playhand_runs_rejects_unknown_run_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    _build_run(runs_root, "run-incomplete", {"runner": "play_hand_v1"})

    monkeypatch.setattr(
        ar_main,
        "load_config",
        lambda: SimpleNamespace(runs_root=runs_root),
    )

    with pytest.raises(SystemExit):
        ar_main.cmd_cleanup_incomplete_playhand_runs(
            run_ids=["definitely-missing"],
            older_than_minutes=None,
            dry_run=True,
            preview=20,
            as_json=True,
        )
