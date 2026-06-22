from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import autoresearch.__main__ as ar_main
import autoresearch.play_hand_lab as lab
from autoresearch.ledger import write_run_metadata


def _config(runs_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        runs_root=runs_root,
        research=SimpleNamespace(plot_lower_is_better=False),
    )


def _lane_context(runs_root: Path, run_id: str) -> lab.PlayHandContext:
    run_dir = runs_root / run_id
    profiles_dir = run_dir / "profiles"
    evals_dir = run_dir / "evals"
    run_dir.mkdir(parents=True, exist_ok=True)
    profiles_dir.mkdir(parents=True, exist_ok=True)
    evals_dir.mkdir(parents=True, exist_ok=True)
    return lab.PlayHandContext(
        config=_config(runs_root),
        cli=SimpleNamespace(),
        run_id=run_id,
        run_dir=run_dir,
        profiles_dir=profiles_dir,
        evals_dir=evals_dir,
        attempts_path=run_dir / "attempts.jsonl",
        events_path=run_dir / "play-hand-lab-lane-events.jsonl",
        summary_path=run_dir / "summary.json",
    )


def test_play_hand_lab_default_retention_writes_canonical_artifacts_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs_root = tmp_path / "runs"
    run_id = "20260620T000000000000Z-playhand-lab-lane-001-v1"
    lane_ctx = _lane_context(runs_root, run_id)
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id=run_id,
        run_dir=lane_ctx.run_dir,
        profile_ref="lab-inline:test",
        instruments=["EURUSD"],
        task_specs={
            "task-1": {
                "task_kind": "deep_replay",
                "phase": "baseline_3mo",
                "lookback_months": 3,
            }
        },
    )
    runtime = lab.PlayHandLabRuntimeConfig(task_mode="deep_replay")

    monkeypatch.setattr(
        lab,
        "_score_lab_artifact",
        lambda **_kwargs: (
            lab.AttemptScore(
                primary_score=12.0,
                composite_score=12.0,
                score_basis="test",
                metrics={"score_lab": 12.0},
                best_summary={"score_lab": {"score": 12.0}},
            ),
            None,
        ),
    )
    monkeypatch.setattr(lab, "render_progress_artifacts", lambda *args, **kwargs: None)

    result = lab._record_lab_result(
        config=_config(runs_root),
        cli=SimpleNamespace(),
        lane_ctx=lane_ctx,
        lane=lane,
        runtime=runtime,
        lab_result={
            "task_id": "task-1",
            "attempt_id": "task-1",
            "status": "success",
            "worker_id": "worker-1",
            "lease_id": "lease-1",
            "result": {
                "job_id": "task-1",
                "status": "success",
                "request": {"instruments": ["EURUSD"], "lookback_months": 3},
                "result": {"aggregate": {"score_lab": {"score": 12.0}}},
            },
        },
        reward_matrix=None,
    )

    artifact_dir = Path(result["artifact_dir"])
    assert (artifact_dir / "sensitivity-response.json").exists()
    assert (artifact_dir / "deep-replay-job.json").exists()
    assert not (artifact_dir / "lab-result.json").exists()
    assert not (artifact_dir / "lab-worker-result.json").exists()


def test_cleanup_playhand_lab_raw_artifacts_dry_run_and_execute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "20260620T000000000000Z-playhand-lab-lane-001-v1"
    good_artifact = run_dir / "evals" / "eval_lab_coarse_probe_task-1"
    unsafe_artifact = run_dir / "evals" / "eval_lab_orphan_task-2"
    non_lab_artifact = runs_root / "manual-run" / "evals" / "eval_lab_coarse_probe_task-1"
    for path in (good_artifact, unsafe_artifact, non_lab_artifact):
        path.mkdir(parents=True, exist_ok=True)
    write_run_metadata(run_dir, {"runner": "play_hand_lab_v1"})
    write_run_metadata(runs_root / "manual-run", {"runner": "manual"})
    for name in ("lab-result.json", "lab-worker-result.json", "sweep-shard-result.json"):
        (good_artifact / name).write_text(json.dumps({"name": name}), encoding="utf-8")
        (non_lab_artifact / name).write_text(json.dumps({"name": name}), encoding="utf-8")
    (good_artifact / "sweep-results.json").write_text("{}", encoding="utf-8")
    (unsafe_artifact / "lab-result.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ar_main, "load_config", lambda: SimpleNamespace(runs_root=runs_root))

    exit_code = ar_main.cmd_cleanup_playhand_lab_raw_artifacts(
        run_ids=None,
        older_than_minutes=None,
        execute=False,
        preview=10,
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["matched_files"] == 3
    assert payload["blocked_files"] == 1
    assert (good_artifact / "lab-result.json").exists()

    exit_code = ar_main.cmd_cleanup_playhand_lab_raw_artifacts(
        run_ids=[run_dir.name],
        older_than_minutes=None,
        execute=True,
        preview=10,
        as_json=True,
    )
    delete_payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert delete_payload["deleted_files"] == 3
    assert not (good_artifact / "lab-result.json").exists()
    assert not (good_artifact / "lab-worker-result.json").exists()
    assert not (good_artifact / "sweep-shard-result.json").exists()
    assert (good_artifact / "sweep-results.json").exists()
    assert (unsafe_artifact / "lab-result.json").exists()
    assert (non_lab_artifact / "lab-result.json").exists()


def test_compact_runs_json_dry_run_and_execute_preserves_json_and_mtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "run-a"
    run_dir.mkdir(parents=True)
    path = run_dir / "payload.json"
    path.write_text(json.dumps({"alpha": [1, 2, 3], "beta": {"ok": True}}, indent=2), encoding="utf-8")
    old_time = time.time() - 3600
    os.utime(path, (old_time, old_time))

    monkeypatch.setattr(ar_main, "load_config", lambda: SimpleNamespace(runs_root=runs_root))

    exit_code = ar_main.cmd_compact_runs_json(
        targets=["run-a"],
        older_than_minutes=None,
        execute=False,
        preview=10,
        as_json=True,
    )
    dry_payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert dry_payload["changed_files"] == 1
    assert dry_payload["dry_run"] is True
    assert "\n  " in path.read_text(encoding="utf-8")

    before_mtime = path.stat().st_mtime
    exit_code = ar_main.cmd_compact_runs_json(
        targets=["run-a"],
        older_than_minutes=None,
        execute=True,
        preview=10,
        as_json=True,
    )
    execute_payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert execute_payload["changed_files"] == 1
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "alpha": [1, 2, 3],
        "beta": {"ok": True},
    }
    assert "\n  " not in path.read_text(encoding="utf-8")
    assert path.stat().st_mtime == pytest.approx(before_mtime, abs=0.01)
