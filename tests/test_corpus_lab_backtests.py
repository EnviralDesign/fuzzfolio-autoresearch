from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autoresearch import __main__ as ar_main
from autoresearch.corpus_lab_backtests import (
    LabBacktestConfig,
    build_full_backtest_lab_task,
    materialize_full_backtest_lab_result,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_full_backtest_lab_task_unwraps_portable_profile(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {
            "format": "fuzzfolio.scoring-profile",
            "profile": {
                "name": "Portable",
                "instruments": ["EURUSD"],
                "directionMode": "both",
                "notificationThreshold": 77,
                "indicators": [],
            },
        },
    )
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {
            "request": {
                "profile_id": "profile-a",
                "instruments": ["GBPUSD"],
                "timeframe": "M15",
                "market_data_source": "lake_bars",
                "bar_limit": 1234,
                "matrix": {"reward_step_r": 0.25, "reward_columns": 4},
                "options": {"path_metrics_mode": "highlighted"},
            }
        },
    )
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
        "profile_ref": "profile-a",
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 8,
            "effective_max_reward_r": 4.0,
        },
    }
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop"),
        execution_cost_mode="research_conservative",
    )

    task = build_full_backtest_lab_task(
        config=config,
        run_dir=run_dir,
        attempt=attempt,
        run_metadata={},
        lab_config=LabBacktestConfig(worker_contract_hash="sha256:test"),
    )

    assert task["task_kind"] == "full_backtest_cache"
    assert task["required_worker_capabilities"] == ["full_backtest_cache"]
    payload = task["payload"]
    assert payload["inline_profile_snapshot"]["name"] == "Portable"
    assert payload["instruments"] == ["GBPUSD"]
    assert payload["timeframe"] == "M15"
    assert payload["lookback_months"] == 36
    assert payload["matrix"]["reward_step_r"] == 0.5
    assert payload["matrix"]["reward_columns"] == 8
    assert payload["options"]["quality_score_preset"] == "profile_drop"
    assert payload["required_capabilities"] == ["deep_replay", "full_backtest_cache"]


def test_materialize_full_backtest_lab_result_accepts_nested_worker_result(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "eval"
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {"request": {"profile_id": "original-profile", "timeframe": "M15"}},
    )
    attempt = {"attempt_id": "attempt-a", "artifact_dir": str(artifact_dir)}
    worker_result = {
        "request": {"profile_id": "profile-a", "instruments": ["EURUSD"], "timeframe": "M5"},
        "sensitivity_response": {
            "status": "success",
            "data": {
                "aggregate": {
                    "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
                    "score_lab": {"version": ar_main.CANONICAL_SCORE_LAB_VERSION, "score": 80},
                }
            },
        },
        "best_cell_detail": {
            "cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
            "curve": {
                "period_granularity": "day",
                "downsampled": False,
                "point_count": 1,
                "returned_point_count": 1,
                "points": [{"date": "2026-01-01", "equity_r": 1.0}],
            },
        },
        "recommended_cell_detail": {
            "cell": {"stop_loss_percent": 0.12, "reward_multiple": 1.5},
            "curve": {"points": [{"date": "2026-01-01", "equity_r": 0.8}]},
        },
    }
    lab_result = {
        "status": "success",
        "task_id": "task-1",
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "result": {"status": "success", "result": worker_result},
    }

    paths = materialize_full_backtest_lab_result(attempt=attempt, lab_result=lab_result)

    assert Path(paths["result_path"]).exists()
    assert Path(paths["curve_path"]).exists()
    assert Path(paths["calendar_curve_path"]).exists()
    assert Path(paths["recommended_curve_path"]).exists()
    result_payload = json.loads(Path(paths["result_path"]).read_text(encoding="utf-8"))
    assert result_payload["data"]["aggregate"]["score_lab"]["score"] == 80
    job_payload = json.loads((artifact_dir / "deep-replay-job.json").read_text(encoding="utf-8"))
    assert job_payload["request"]["profile_id"] == "original-profile"
    full_job_payload = json.loads(
        (artifact_dir / "full-backtest-36mo-deep-replay-job.json").read_text(encoding="utf-8")
    )
    assert full_job_payload["request"]["profile_id"] == "profile-a"


def test_cmd_calculate_full_backtests_lab_gateway_backend(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "best_summary": {"best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}},
    }
    row = {"attempt_id": "attempt-a", "run_id": "run-a", "candidate_name": "final"}
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=3),
        full_backtest_failures_json_path=tmp_path / "full-backtest-failures.json",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    monkeypatch.setattr(ar_main, "_matching_run_dirs", lambda *_args, **_kwargs: [run_dir])
    monkeypatch.setattr(ar_main, "_catalog_rows_for_run_dirs", lambda *_args, **_kwargs: [row])
    monkeypatch.setattr(
        ar_main,
        "_matched_attempt_items",
        lambda *_args, **_kwargs: [(run_dir, [attempt], attempt)],
    )
    monkeypatch.setattr(ar_main, "_refresh_global_derived_corpus_state", lambda _config: {"status": "refreshed"})
    monkeypatch.setattr(ar_main, "resolve_lab_backtest_config", lambda **_kwargs: LabBacktestConfig())
    calls: list[dict] = []

    def fake_run_lab_full_backtests(**kwargs):
        calls.append(kwargs)
        return (
            [
                {
                    "run_id": "run-a",
                    "attempt_id": "attempt-a",
                    "status": "calculated",
                    "backend": "lab_gateway",
                }
            ],
            1,
            0,
        )

    monkeypatch.setattr(ar_main, "run_lab_full_backtests", fake_run_lab_full_backtests)

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=["attempt-a"],
        limit=None,
        max_workers=2,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=True,
        job_timeout_seconds=120,
        full_backtest_backend="lab-gateway",
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["full_backtest_backend"] == "lab_gateway"
    assert payload["calculated"] == 1
    assert calls[0]["max_workers"] == 1
    assert calls[0]["items"][0][1]["attempt_id"] == "attempt-a"
