from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from autoresearch import __main__ as ar_main
from autoresearch import dashboard as dashboard_mod
from autoresearch.config import FuzzfolioConfig
from autoresearch.fuzzfolio import CliError


class _StubConfig:
    def __init__(self, workspace_root: Path):
        self.validation_cache_root = workspace_root / "validation-cache"
        self.validation_cache_root.mkdir(parents=True, exist_ok=True)
        self.research = SimpleNamespace(quality_score_preset="profile-drop")
        self.fuzzfolio = FuzzfolioConfig(
            workspace_root=workspace_root,
            cli_command="fuzzfolio-agent-cli",
            base_url="http://localhost:7946/api/dev",
            auth_profile="robot",
        )


def test_run_full_backtest_retries_with_local_profile_on_profile_not_found(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-1",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "stale-cloud-ref",
        "profile_path": str(profile_path),
    }

    seen_profile_refs: list[str] = []

    def fake_run_full_backtest(cfg, candidate):
        seen_profile_refs.append(str(candidate.get("profile_ref") or ""))
        if len(seen_profile_refs) == 1:
            raise RuntimeError("Profile not found")
        return {"curve_path": "curve.json", "result_path": "result.json"}

    monkeypatch.setattr(ar_main, "_run_full_backtest_for_attempt", fake_run_full_backtest)

    result = ar_main._run_full_backtest_with_retry(config, attempt)

    assert result["curve_path"] == "curve.json"
    assert seen_profile_refs == ["stale-cloud-ref", ""]


def test_run_full_backtest_retries_with_local_profile_on_missing_curve_message(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-missing-curve",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "stale-cloud-ref",
        "profile_path": str(profile_path),
    }

    seen_profile_refs: list[str] = []

    def fake_run_full_backtest(cfg, candidate):
        seen_profile_refs.append(str(candidate.get("profile_ref") or ""))
        if len(seen_profile_refs) == 1:
            raise RuntimeError(
                "sensitivity-basket did not produce best-cell-path-detail.json. Files in output dir: []"
            )
        return {"curve_path": "curve.json", "result_path": "result.json"}

    monkeypatch.setattr(ar_main, "_run_full_backtest_for_attempt", fake_run_full_backtest)

    result = ar_main._run_full_backtest_with_retry(config, attempt)

    assert result["curve_path"] == "curve.json"
    assert result["retry_mode"] == "local_profile_reupload"
    assert seen_profile_refs == ["stale-cloud-ref", ""]


def test_run_full_backtest_salvages_outputs_after_timeout(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-2",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "cloud-ref",
        "profile_path": str(profile_path),
    }

    def fake_run(self, args, **kwargs):
        output_dir = Path(args[args.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "best-cell-path-detail.json").write_text(
            json.dumps({"curve": {"points": []}}), encoding="utf-8"
        )
        (output_dir / "recommended-cell-path-detail.json").write_text(
            json.dumps(
                {
                    "cell": {
                        "stop_loss_percent": 0.05,
                        "reward_multiple": 2.0,
                    },
                    "curve": {"points": []},
                }
            ),
            encoding="utf-8",
        )
        (output_dir / "sensitivity-response.json").write_text(
            json.dumps({"data": {"aggregate": {"quality_score": {"score": 55.0}}}}),
            encoding="utf-8",
        )
        raise CliError("Command timed out after 1800s: fuzzfolio-agent-cli ...")

    monkeypatch.setattr(
        dashboard_mod.FuzzfolioCli,
        "ensure_login",
        lambda self: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "run", fake_run)

    result = dashboard_mod._run_full_backtest_for_attempt(config, attempt)

    curve_path = artifact_dir / dashboard_mod.FULL_BACKTEST_CURVE_FILENAME
    result_path = artifact_dir / dashboard_mod.FULL_BACKTEST_RESULT_FILENAME
    assert result["curve_path"] == str(curve_path)
    assert result["result_path"] == str(result_path)
    assert curve_path.exists()
    assert result_path.exists()


def test_run_full_backtest_forwards_normalized_quality_score_preset(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    (artifact_dir / "deep-replay-job.json").write_text(
        json.dumps(
            {
                "request": {
                    "timeframe": "M15",
                    "instruments": ["EURUSD", "GBPUSD"],
                }
            }
        ),
        encoding="utf-8",
    )
    config = _StubConfig(tmp_path)
    config.research.quality_score_preset = "profile_drop"
    attempt = {
        "attempt_id": "run-attempt-3",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "cloud-ref",
        "profile_path": str(profile_path),
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 8,
            "effective_max_reward_r": 4.0,
        },
    }

    seen_args: list[str] = []

    def fake_run(self, args, **kwargs):
        nonlocal seen_args
        seen_args = list(args)
        output_dir = Path(args[args.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "best-cell-path-detail.json").write_text(
            json.dumps({"curve": {"points": []}}), encoding="utf-8"
        )
        (output_dir / "recommended-cell-path-detail.json").write_text(
            json.dumps(
                {
                    "cell": {
                        "stop_loss_percent": 0.05,
                        "reward_multiple": 2.0,
                    },
                    "curve": {"points": []},
                }
            ),
            encoding="utf-8",
        )
        (output_dir / "sensitivity-response.json").write_text(
            json.dumps({"data": {"aggregate": {"quality_score": {"score": 55.0}}}}),
            encoding="utf-8",
        )

        class _Result:
            returncode = 0
            stdout = ""
            stderr = ""

        return _Result()

    monkeypatch.setattr(
        dashboard_mod.FuzzfolioCli,
        "ensure_login",
        lambda self: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "run", fake_run)

    result = dashboard_mod._run_full_backtest_for_attempt(config, attempt)

    assert result["curve_path"] == str(
        artifact_dir / dashboard_mod.FULL_BACKTEST_CURVE_FILENAME
    )
    assert "--quality-score-preset" in seen_args
    preset_index = seen_args.index("--quality-score-preset")
    assert seen_args[preset_index + 1] == "profile-drop"
    detail_mode_index = seen_args.index("--cell-detail-artifacts")
    assert seen_args[detail_mode_index + 1] == "both"
    reward_step_index = seen_args.index("--reward-step-r")
    assert seen_args[reward_step_index + 1] == "0.5"
    reward_columns_index = seen_args.index("--reward-columns")
    assert seen_args[reward_columns_index + 1] == "8"
    assert seen_args.count("--instrument") == 2
    recommended_path = artifact_dir / dashboard_mod.FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME
    assert result["recommended_curve_path"] == str(recommended_path)
    assert recommended_path.exists()


def test_run_full_backtest_ensures_login_before_running_sensitivity(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    (artifact_dir / "deep-replay-job.json").write_text(
        json.dumps(
            {
                "request": {
                    "timeframe": "M5",
                    "instruments": ["EURUSD"],
                }
            }
        ),
        encoding="utf-8",
    )
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-auth-refresh",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "cloud-ref",
        "profile_path": str(profile_path),
    }

    call_order: list[str] = []

    def fake_ensure_login(self):
        call_order.append("ensure_login")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_run(self, args, **kwargs):
        call_order.append("run")
        output_dir = Path(args[args.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "best-cell-path-detail.json").write_text(
            json.dumps({"curve": {"points": []}}), encoding="utf-8"
        )
        (output_dir / "sensitivity-response.json").write_text(
            json.dumps({"data": {"aggregate": {"quality_score": {"score": 55.0}}}}),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "ensure_login", fake_ensure_login)
    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "run", fake_run)

    result = dashboard_mod._run_full_backtest_for_attempt(config, attempt)

    assert result["curve_path"] == str(
        artifact_dir / dashboard_mod.FULL_BACKTEST_CURVE_FILENAME
    )
    assert call_order[:2] == ["ensure_login", "run"]


def test_run_full_backtest_retries_without_cell_detail_artifacts_for_old_cli(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    (artifact_dir / "deep-replay-job.json").write_text(
        json.dumps(
            {
                "request": {
                    "timeframe": "M5",
                    "instruments": ["EURUSD"],
                }
            }
        ),
        encoding="utf-8",
    )
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-old-cli",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "cloud-ref",
        "profile_path": str(profile_path),
    }

    seen_args: list[list[str]] = []

    def fake_run(self, args, **kwargs):
        seen_args.append(list(args))
        output_dir = Path(args[args.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        if "--cell-detail-artifacts" in args:
            return SimpleNamespace(
                returncode=2,
                stdout="",
                stderr="error: unexpected argument '--cell-detail-artifacts'",
            )
        (output_dir / "best-cell-path-detail.json").write_text(
            json.dumps({"curve": {"points": []}}), encoding="utf-8"
        )
        (output_dir / "sensitivity-response.json").write_text(
            json.dumps({"data": {"aggregate": {"quality_score": {"score": 55.0}}}}),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        dashboard_mod.FuzzfolioCli,
        "ensure_login",
        lambda self: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "run", fake_run)

    result = dashboard_mod._run_full_backtest_for_attempt(config, attempt)

    assert result["curve_path"] == str(
        artifact_dir / dashboard_mod.FULL_BACKTEST_CURVE_FILENAME
    )
    assert len(seen_args) == 2
    assert "--cell-detail-artifacts" in seen_args[0]
    assert "--cell-detail-artifacts" not in seen_args[1]


def test_copy_full_backtest_outputs_surfaces_profile_not_found_from_result_json(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    sensitivity_output_dir = tmp_path / "out"
    sensitivity_output_dir.mkdir()
    (sensitivity_output_dir / "sensitivity-response.json").write_text(
        json.dumps({"error": {"message": "Profile not found"}}),
        encoding="utf-8",
    )

    try:
        dashboard_mod._copy_full_backtest_outputs(artifact_dir, sensitivity_output_dir)
    except RuntimeError as exc:
        assert "Profile not found" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")


def test_attempt_has_backtestable_cell_requires_best_or_robust_cell() -> None:
    assert (
        ar_main._attempt_has_backtestable_cell(
            {"best_summary": {"best_cell": {"reward_multiple": 2.0}}}
        )
        is True
    )
    assert (
        ar_main._attempt_has_backtestable_cell(
            {
                "best_summary": {
                    "best_cell": None,
                    "matrix_summary": {
                        "robust_cell": {"reward_multiple": 2.0},
                    },
                }
            }
        )
        is True
    )
    assert (
        ar_main._attempt_has_backtestable_cell(
            {
                "best_summary": {
                    "best_cell": None,
                    "matrix_summary": {"robust_cell": None},
                }
            }
        )
        is False
    )


def test_calculate_full_backtests_rebuilds_stale_score_lab_result(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    curve_path = artifact_dir / "full-backtest-36mo-curve.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "score_lab": {"version": "score_lab_v2_5_1", "score": 72.0},
                        "matrix_summary": {
                            "reward_column_summaries": [
                                {"reward_multiple": 0.5},
                                {"reward_multiple": 1.0},
                            ]
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    attempt = {
        "attempt_id": "run-a-attempt-00001",
        "run_id": "run-a",
        "candidate_name": "final",
        "artifact_dir": str(artifact_dir),
        "best_summary": {"best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}},
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 2,
            "effective_max_reward_r": 1.0,
        },
    }
    row = {
        "attempt_id": attempt["attempt_id"],
        "run_id": "run-a",
        "candidate_name": "final",
        "full_backtest_validation_status_36m": "invalid",
    }
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=1),
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
    calls: list[dict[str, object]] = []

    def fake_run_full_backtest_with_retry(*_args, **kwargs):
        calls.append(kwargs)
        return {"curve_path": str(curve_path), "result_path": str(result_path)}

    monkeypatch.setattr(
        ar_main,
        "_run_full_backtest_with_retry",
        fake_run_full_backtest_with_retry,
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=[str(attempt["attempt_id"])],
        limit=None,
        max_workers=1,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=False,
        job_timeout_seconds=None,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["force_rebuild"] is False
    assert payload["eligible_attempts"] == 1
    assert payload["calculated"] == 1


def test_calculate_full_backtests_rebuilds_stale_effective_window(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    stale_end = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    requested_end = datetime.now(timezone.utc).isoformat()
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    curve_path = artifact_dir / "full-backtest-36mo-curve.json"
    calendar_curve_path = artifact_dir / ar_main.FULL_BACKTEST_CALENDAR_CURVE_FILENAME
    recommended_curve_path = artifact_dir / ar_main.FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "analysis_status": "success",
                        "score_lab": {
                            "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                            "score": 72.0,
                        },
                        "matrix_summary": {
                            "reward_column_summaries": [
                                {"reward_multiple": 0.5},
                                {"reward_multiple": 1.0},
                            ]
                        },
                        "market_data_window": {
                            "effective_window_end": stale_end,
                            "requested_window_end": requested_end,
                            "window_truncated_end": True,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    calendar_curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    recommended_curve_path.write_text(
        json.dumps(
            {
                "cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0},
                "curve": {"points": []},
            }
        ),
        encoding="utf-8",
    )
    attempt = {
        "attempt_id": "run-a-attempt-00001",
        "run_id": "run-a",
        "candidate_name": "final",
        "artifact_dir": str(artifact_dir),
        "best_summary": {"best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}},
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 2,
            "effective_max_reward_r": 1.0,
        },
    }
    row = {
        "attempt_id": attempt["attempt_id"],
        "run_id": "run-a",
        "candidate_name": "final",
        "full_backtest_validation_status_36m": "valid",
    }
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=1),
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
    monkeypatch.setattr(
        ar_main,
        "result_matches_execution_cost_model",
        lambda *_args, **_kwargs: True,
    )
    calls: list[dict[str, object]] = []

    def fake_run_full_backtest_with_retry(*_args, **kwargs):
        calls.append(kwargs)
        return {"curve_path": str(curve_path), "result_path": str(result_path)}

    monkeypatch.setattr(
        ar_main,
        "_run_full_backtest_with_retry",
        fake_run_full_backtest_with_retry,
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=[str(attempt["attempt_id"])],
        limit=None,
        max_workers=1,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=False,
        job_timeout_seconds=None,
        full_backtest_max_age_days=7,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert len(calls) == 1
    assert payload["eligible_attempts"] == 1
    assert payload["calculation_reasons"] == {"stale_effective_window_end": 1}


def test_calculate_full_backtests_keeps_recent_truncated_window(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    effective_end = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    requested_end = datetime.now(timezone.utc).isoformat()
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    curve_path = artifact_dir / "full-backtest-36mo-curve.json"
    calendar_curve_path = artifact_dir / ar_main.FULL_BACKTEST_CALENDAR_CURVE_FILENAME
    recommended_curve_path = artifact_dir / ar_main.FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "analysis_status": "success",
                        "score_lab": {
                            "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                            "score": 72.0,
                        },
                        "matrix_summary": {
                            "reward_column_summaries": [{"reward_multiple": 1.0}]
                        },
                        "market_data_window": {
                            "effective_window_end": effective_end,
                            "requested_window_end": requested_end,
                            "window_truncated_end": True,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    calendar_curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    recommended_curve_path.write_text(
        json.dumps(
            {
                "cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0},
                "curve": {"points": []},
            }
        ),
        encoding="utf-8",
    )
    attempt = {
        "attempt_id": "run-a-attempt-00001",
        "run_id": "run-a",
        "candidate_name": "final",
        "artifact_dir": str(artifact_dir),
        "best_summary": {"best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}},
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 2,
            "effective_max_reward_r": 1.0,
        },
    }
    row = {
        "attempt_id": attempt["attempt_id"],
        "run_id": "run-a",
        "candidate_name": "final",
        "full_backtest_validation_status_36m": "valid",
    }
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=1),
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
    monkeypatch.setattr(
        ar_main,
        "result_matches_execution_cost_model",
        lambda *_args, **_kwargs: True,
    )
    calls: list[dict[str, object]] = []

    def fake_run_full_backtest_with_retry(*_args, **kwargs):
        calls.append(kwargs)
        return {"curve_path": str(curve_path), "result_path": str(result_path)}

    monkeypatch.setattr(
        ar_main,
        "_run_full_backtest_with_retry",
        fake_run_full_backtest_with_retry,
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=[str(attempt["attempt_id"])],
        limit=None,
        max_workers=1,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=False,
        job_timeout_seconds=None,
        full_backtest_max_age_days=7,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert calls == []
    assert payload["eligible_attempts"] == 0
    assert payload["filter_rejections"]["already_has_full_backtest"] == 1


def test_calculate_full_backtests_rebuilds_missing_recommended_detail(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    effective_end = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    requested_end = datetime.now(timezone.utc).isoformat()
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    curve_path = artifact_dir / "full-backtest-36mo-curve.json"
    calendar_curve_path = artifact_dir / ar_main.FULL_BACKTEST_CALENDAR_CURVE_FILENAME
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "analysis_status": "success",
                        "best_cell": {
                            "stop_loss_percent": 0.1,
                            "reward_multiple": 1.0,
                        },
                        "score_lab": {
                            "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                            "score": 72.0,
                        },
                        "matrix_summary": {
                            "reward_column_summaries": [{"reward_multiple": 1.0}]
                        },
                        "market_data_window": {
                            "effective_window_end": effective_end,
                            "requested_window_end": requested_end,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    calendar_curve_path.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    attempt = {
        "attempt_id": "run-a-attempt-00001",
        "run_id": "run-a",
        "candidate_name": "final",
        "artifact_dir": str(artifact_dir),
        "best_summary": {"best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 1.0}},
        "reward_matrix": {
            "reward_step_r": 1.0,
            "reward_columns": 1,
            "effective_max_reward_r": 1.0,
        },
    }
    row = {
        "attempt_id": attempt["attempt_id"],
        "run_id": "run-a",
        "candidate_name": "final",
        "full_backtest_validation_status_36m": "valid",
    }
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        research=SimpleNamespace(validation_max_concurrency=1),
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
    monkeypatch.setattr(
        ar_main,
        "result_matches_execution_cost_model",
        lambda *_args, **_kwargs: True,
    )
    calls: list[dict[str, object]] = []

    def fake_missing_recommended_rebuild(*_args, **kwargs):
        calls.append(kwargs)
        return {"curve_path": str(curve_path), "result_path": str(result_path)}

    monkeypatch.setattr(
        ar_main,
        "_run_full_backtest_with_retry",
        fake_missing_recommended_rebuild,
    )

    exit_code = ar_main.cmd_calculate_full_backtests(
        run_ids=["run-a"],
        attempt_ids=[str(attempt["attempt_id"])],
        limit=None,
        max_workers=1,
        use_dev_sim_worker_count=False,
        require_scrutiny_36=False,
        force_rebuild=False,
        job_timeout_seconds=None,
        full_backtest_max_age_days=7,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert len(calls) == 1
    assert payload["eligible_attempts"] == 1
    assert payload["calculation_reasons"] == {"missing_recommended_cell_detail": 1}


def test_result_matches_attempt_reward_matrix_rejects_expanded_reward_grid(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "full-backtest-36mo-result.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "matrix_summary": {
                            "reward_column_summaries": [
                                {"reward_multiple": 0.5},
                                {"reward_multiple": 4.0},
                                {"reward_multiple": 12.5},
                            ]
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    attempt = {
        "reward_matrix": {
            "reward_step_r": 0.5,
            "reward_columns": 8,
            "effective_max_reward_r": 4.0,
        }
    }

    assert ar_main._result_matches_attempt_reward_matrix(result_path, attempt) is False


def test_attempt_with_run_reward_matrix_uses_run_metadata() -> None:
    attempt = {"attempt_id": "run-attempt-1"}
    merged = ar_main._attempt_with_run_reward_matrix(
        attempt,
        run_metadata={
            "reward_matrix": {
                "requested_max_reward_r": 4.0,
                "reward_step_r": 0.5,
                "reward_columns": 8,
                "effective_max_reward_r": 4.0,
            }
        },
    )

    assert merged["max_reward_r"] == 4.0
    assert merged["reward_columns"] == 8
    assert attempt.get("reward_columns") is None
