from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import __main__ as ar_main
from autoresearch.corpus_lab_backtests import (
    LabBacktestConfig,
    build_full_backtest_lab_task,
    materialize_full_backtest_lab_result,
    run_lab_full_backtests,
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
        evidence_window_end="2026-07-08T23:59:59Z",
    )

    assert task["task_kind"] == "full_backtest_cache"
    assert task["required_worker_capabilities"] == ["full_backtest_cache"]
    payload = task["payload"]
    assert payload["inline_profile_snapshot"]["name"] == "Portable"
    assert payload["instruments"] == ["EURUSD"]
    assert payload["alert_threshold"] == 77
    assert payload["timeframe"] == "M15"
    assert "lookback_months" not in payload
    assert payload["analysis_window_start"] == "2023-07-08T23:59:59Z"
    assert payload["analysis_window_end"] == "2026-07-08T23:59:59Z"
    assert payload["evidence_plan"]["evidence_role"] == "full_backtest"
    assert payload["evidence_plan"]["requested_horizon_months"] == 36
    assert payload["evidence_plan"]["plan_id"].startswith("sha256:")
    assert payload["matrix"]["reward_step_r"] == 0.5
    assert payload["matrix"]["reward_columns"] == 8
    assert payload["options"]["quality_score_preset"] == "profile_drop"
    assert payload["required_capabilities"] == ["deep_replay", "full_backtest_cache"]


@pytest.mark.parametrize(
    ("profile_threshold", "historical_threshold", "expected_threshold"),
    [(70, 80, 70), (80, 80, 80)],
)
def test_build_full_backtest_lab_task_uses_canonical_profile_threshold(
    tmp_path: Path,
    profile_threshold: float,
    historical_threshold: float,
    expected_threshold: float,
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {
            "profile": {
                "version": "v1",
                "notificationThreshold": profile_threshold,
                "directionMode": "both",
                "instruments": ["XAGUSD"],
                "indicators": [],
            }
        },
    )
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {
            "request": {
                "alert_threshold": historical_threshold,
                "instruments": ["XAGUSD"],
                "timeframe": "M5",
            }
        },
    )
    attempt = {
        "run_id": "run-a",
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }

    task = build_full_backtest_lab_task(
        config=SimpleNamespace(
            research=SimpleNamespace(quality_score_preset="profile_drop")
        ),
        run_dir=run_dir,
        attempt=attempt,
        run_metadata={},
        lab_config=LabBacktestConfig(worker_contract_hash="sha256:test"),
    )

    assert task["payload"]["alert_threshold"] == expected_threshold
    assert (
        task["payload"]["inline_profile_snapshot"]["notificationThreshold"]
        == expected_threshold
    )
    assert task["metadata"]["historical_request_alert_threshold"] == historical_threshold


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
    manifest_path = artifact_dir / "full-backtest-36mo-manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema"] == "autoresearch-full-backtest-provenance-v1"
    result_payload = json.loads(Path(paths["result_path"]).read_text(encoding="utf-8"))
    assert (
        result_payload["data"]["aggregate"]["autoresearch_provenance"]["schema"]
        == "autoresearch-full-backtest-provenance-v1"
    )
    result_payload = json.loads(Path(paths["result_path"]).read_text(encoding="utf-8"))
    assert result_payload["data"]["aggregate"]["score_lab"]["score"] == 80
    job_payload = json.loads((artifact_dir / "deep-replay-job.json").read_text(encoding="utf-8"))
    assert job_payload["request"]["profile_id"] == "original-profile"
    full_job_payload = json.loads(
        (artifact_dir / "full-backtest-36mo-deep-replay-job.json").read_text(encoding="utf-8")
    )
    assert full_job_payload["request"]["profile_id"] == "profile-a"


def test_materialize_full_backtest_lab_result_requires_recommended_detail(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "eval"
    attempt = {"attempt_id": "attempt-a", "artifact_dir": str(artifact_dir)}
    lab_result = {
        "status": "success",
        "task_id": "task-1",
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "result": {
            "sensitivity_response": {
                "status": "success",
                "data": {
                    "aggregate": {
                        "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
                        "score_lab": {
                            "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                            "score": 80,
                        },
                    }
                },
            },
            "best_cell_detail": {
                "cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
                "curve": {"points": [{"date": "2026-01-01", "equity_r": 1.0}]},
            },
        },
    }

    with pytest.raises(RuntimeError, match="recommended_cell_detail"):
        materialize_full_backtest_lab_result(attempt=attempt, lab_result=lab_result)


def test_materialize_60m_evidence_does_not_overwrite_legacy_36m_files(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {
            "profile": {
                "name": "Frozen",
                "instruments": ["EURUSD"],
                "notificationThreshold": 73,
                "indicators": [],
            }
        },
    )
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {"request": {"timeframe": "M5", "instruments": ["EURUSD"]}},
    )
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }
    task = build_full_backtest_lab_task(
        config=SimpleNamespace(
            research=SimpleNamespace(quality_score_preset="profile_drop")
        ),
        run_dir=run_dir,
        attempt=attempt,
        run_metadata={},
        lab_config=LabBacktestConfig(worker_contract_hash="sha256:test"),
        requested_horizon_months=60,
        evidence_window_start="2021-07-08T23:59:59Z",
        evidence_window_end="2026-07-08T23:59:59Z",
        campaign_plan_id="campaign:60m-test",
        lake_manifest_sha256="sha256:" + "a" * 64,
    )
    evidence_plan = task["payload"]["evidence_plan"]
    execution_evidence = {
        **evidence_plan,
        "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
    }
    worker_result = {
        "request": task["payload"],
        "execution_evidence": execution_evidence,
        "sensitivity_response": {
            "status": "success",
            "data": {
                "aggregate": {
                    "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
                    "score_lab": {
                        "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                        "score": 80,
                    },
                }
            },
        },
        "best_cell_detail": {
            "cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
            "curve": {"points": [{"date": "2026-01-01", "equity_r": 1.0}]},
        },
        "calendar_curve": {
            "cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
            "curve": {"points": [{"date": "2026-01-01", "equity_r": 1.0}]},
        },
        "recommended_cell_detail": {
            "cell": {"stop_loss_percent": 0.12, "reward_multiple": 1.5},
            "curve": {"points": [{"date": "2026-01-01", "equity_r": 0.8}]},
        },
    }
    lab_result = {
        "status": "success",
        "worker_id": "worker-1",
        "result": {"status": "success", "result": worker_result},
    }

    paths = materialize_full_backtest_lab_result(
        attempt=attempt,
        lab_result=lab_result,
        task=task,
    )

    assert paths["evidence_plan_id"] == evidence_plan["plan_id"]
    assert evidence_plan["lake_manifest_sha256"] == "sha256:" + "a" * 64
    assert Path(paths["evidence_manifest_path"]).exists()
    assert not (artifact_dir / "full-backtest-36mo-result.json").exists()
    assert not (artifact_dir / "full-backtest-36mo-curve.json").exists()


def test_run_lab_full_backtests_skips_missing_profile(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": None,
    }
    row = {"attempt_id": "attempt-a", "run_id": "run-a", "candidate_name": "final"}
    emitted: list[str] = []

    class FakeGateway:
        enqueued: list[list[dict]] = []

        def __init__(self, **_kwargs) -> None:
            pass

        def read_results(self, *, limit: int) -> list[dict]:
            _ = limit
            return []

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.enqueued.append(tasks)
            return {"accepted": len(tasks)}

        def close(self) -> None:
            pass

    monkeypatch.setattr("autoresearch.corpus_lab_backtests.LabGatewayClient", FakeGateway)

    results, calculated, failed = run_lab_full_backtests(
        config=SimpleNamespace(research=SimpleNamespace(quality_score_preset="profile_drop")),
        items=[(run_dir, attempt, row, {})],
        lab_config=LabBacktestConfig(poll_interval_seconds=0.1),
        max_workers=1,
        emit=emitted.append,
    )

    assert calculated == 0
    assert failed == 1
    assert results[0]["status"] == "failed"
    assert "missing a local profile file" in results[0]["error"]
    assert emitted and "lab skipped" in emitted[0]
    assert FakeGateway.enqueued == []


def test_run_lab_full_backtests_fails_fast_on_unrelated_gateway_result(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {"profile": {"name": "profile", "instruments": ["EURUSD"], "indicators": []}},
    )
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {"request": {"timeframe": "M15", "instruments": ["EURUSD"]}},
    )
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }
    row = {"attempt_id": "attempt-a", "run_id": "run-a", "candidate_name": "final"}

    class FakeGateway:
        acked: list[list[str]] = []
        read_count = 0

        def __init__(self, **_kwargs) -> None:
            pass

        def read_results(self, *, limit: int) -> list[dict]:
            _ = limit
            self.__class__.read_count += 1
            if self.__class__.read_count == 1:
                return []
            return [
                {
                    "task_id": "foreign-task",
                    "lease_id": "foreign-lease",
                    "status": "success",
                    "result": {},
                }
            ]

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            return {"accepted": len(tasks)}

        def ack_results(self, lease_ids: list[str]) -> None:
            self.acked.append(list(lease_ids))

        def close(self) -> None:
            pass

    monkeypatch.setattr("autoresearch.corpus_lab_backtests.LabGatewayClient", FakeGateway)

    with pytest.raises(RuntimeError, match="unrelated results"):
        run_lab_full_backtests(
            config=SimpleNamespace(
                research=SimpleNamespace(quality_score_preset="profile_drop")
            ),
            items=[(run_dir, attempt, row, {})],
            lab_config=LabBacktestConfig(poll_interval_seconds=0.1),
            max_workers=1,
        )

    assert FakeGateway.acked == []


def test_cmd_calculate_full_backtests_lab_gateway_backend(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {
            "profile": {
                "notificationThreshold": 80,
                "instruments": ["EURUSD"],
                "indicators": [],
            }
        },
    )
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
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
    monkeypatch.setattr(
        ar_main,
        "_refresh_global_derived_corpus_state",
        lambda _config, **_kwargs: {"status": "refreshed"},
    )
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


@pytest.mark.parametrize(
    ("force_rebuild", "expected_count"),
    [(False, 1), (True, 2)],
)
def test_calculate_full_backtests_dry_run_is_selective_and_submits_no_work(
    tmp_path: Path,
    monkeypatch,
    capsys,
    force_rebuild: bool,
    expected_count: int,
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    profile_path = run_dir / "profiles" / "profile.json"
    _write_json(
        profile_path,
        {
            "profile": {
                "notificationThreshold": 80,
                "instruments": ["EURUSD"],
                "indicators": [],
            }
        },
    )
    attempts = []
    rows = []
    for attempt_id in ("stale-attempt", "valid-attempt"):
        artifact_dir = run_dir / "evals" / attempt_id
        artifact_dir.mkdir(parents=True)
        attempts.append(
            {
                "attempt_id": attempt_id,
                "artifact_dir": str(artifact_dir),
                "profile_path": str(profile_path),
                "best_summary": {
                    "best_cell": {
                        "stop_loss_percent": 0.1,
                        "reward_multiple": 1.0,
                    }
                },
            }
        )
        rows.append(
            {
                "attempt_id": attempt_id,
                "run_id": "run-a",
                "candidate_name": attempt_id,
            }
        )
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=2),
        full_backtest_failures_json_path=tmp_path / "failures.json",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    monkeypatch.setattr(ar_main, "_matching_run_dirs", lambda *_args, **_kwargs: [run_dir])
    monkeypatch.setattr(ar_main, "_catalog_rows_for_run_dirs", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(
        ar_main,
        "_matched_attempt_items",
        lambda *_args, **_kwargs: [
            (run_dir, attempts, attempt) for attempt in attempts
        ],
    )

    def fake_validation(attempt, **_kwargs):
        if attempt["attempt_id"] == "stale-attempt":
            return {
                "status": "invalid",
                "reason_codes": ["stale_effective_end"],
                "rebuild_reason_codes": ["stale_effective_end"],
                "rebuild_required": True,
            }
        return {
            "status": "valid",
            "reason_codes": [],
            "rebuild_reason_codes": [],
            "rebuild_required": False,
        }

    monkeypatch.setattr(ar_main, "validate_full_backtest_artifacts", fake_validation)
    monkeypatch.setattr(
        ar_main,
        "run_lab_full_backtests",
        lambda **_kwargs: pytest.fail("dry run submitted gateway work"),
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=None,
        limit=None,
        max_workers=2,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=force_rebuild,
        job_timeout_seconds=120,
        dry_run=True,
        full_backtest_backend="lab-gateway",
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["status"] == "dry_run"
    assert payload["selected_for_rebuild"] == expected_count
    assert payload["calculated"] == 0


def test_calculate_full_backtests_does_not_submit_unrebuildable_profiles(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "attempt-a"
    artifact_dir.mkdir(parents=True)
    attempt = {
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "best_summary": {
            "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}
        },
    }
    sol_artifact_dir = run_dir / "evals" / "attempt-sol"
    sol_profile_path = run_dir / "profiles" / "sol.json"
    sol_artifact_dir.mkdir(parents=True)
    _write_json(
        sol_profile_path,
        {
            "profile": {
                "notificationThreshold": 80,
                "instruments": ["SOLUSD"],
                "indicators": [],
            }
        },
    )
    sol_attempt = {
        "attempt_id": "attempt-sol",
        "artifact_dir": str(sol_artifact_dir),
        "profile_path": str(sol_profile_path),
        "best_summary": {
            "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}
        },
    }
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=2),
        full_backtest_failures_json_path=tmp_path / "failures.json",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    monkeypatch.setattr(ar_main, "_matching_run_dirs", lambda *_args, **_kwargs: [run_dir])
    monkeypatch.setattr(
        ar_main,
        "_catalog_rows_for_run_dirs",
        lambda *_args, **_kwargs: [
            {"attempt_id": "attempt-a", "run_id": "run-a", "candidate_name": "candidate"},
            {"attempt_id": "attempt-sol", "run_id": "run-a", "candidate_name": "sol"},
        ],
    )
    monkeypatch.setattr(
        ar_main,
        "_matched_attempt_items",
        lambda *_args, **_kwargs: [
            (run_dir, [attempt, sol_attempt], attempt),
            (run_dir, [attempt, sol_attempt], sol_attempt),
        ],
    )
    monkeypatch.setattr(
        ar_main,
        "validate_full_backtest_artifacts",
        lambda *_args, **_kwargs: {
            "status": "missing",
            "reason_codes": ["missing_artifact"],
            "rebuild_reason_codes": ["missing_artifact"],
            "rebuild_required": True,
        },
    )
    monkeypatch.setattr(
        ar_main,
        "run_lab_full_backtests",
        lambda **_kwargs: pytest.fail("unrebuildable attempt submitted gateway work"),
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=None,
        limit=None,
        max_workers=2,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=False,
        job_timeout_seconds=120,
        dry_run=True,
        full_backtest_backend="lab-gateway",
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["selected_for_rebuild"] == 0
    assert payload["filter_rejections"]["missing_canonical_profile"] == 1
    assert payload["filter_rejections"]["excluded_research_instrument"] == 1
