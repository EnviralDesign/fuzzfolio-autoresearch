from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch.corpus_lab_backtests import (
    LabBacktestConfig,
    build_full_backtest_lab_task,
    materialize_full_backtest_lab_result,
    materialize_no_valid_cell_lab_result,
    materialize_outer_test_lab_result,
)
from autoresearch.nested_gateway import _window_end, run_nested_gateway_fold
from autoresearch import nested_gateway as ng


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _worker_result(task: dict, *, tracked: bool) -> dict:
    plan = task["payload"]["evidence_plan"]
    execution_evidence = dict(plan)
    if plan.get("lake_manifest_sha256"):
        execution_evidence["observed_lake_manifest_sha256"] = plan[
            "lake_manifest_sha256"
        ]
    tracked_cell = task["payload"].get("tracked_cell")
    best = {"stop_loss_percent": 0.1, "reward_multiple": 2.0}
    recommended = {"stop_loss_percent": 0.12, "reward_multiple": 1.5}
    detail = lambda cell: {
        "cell": cell,
        "curve": {
            "period_granularity": "day",
            "downsampled": False,
            "point_count": 1,
            "returned_point_count": 1,
            "points": [{"date": "2025-01-01", "equity_r": 1.0}],
        },
    }
    aggregate = {
        "best_cell": best,
        "matrix": {"rows": [{"expectancy_r": 999.0}]},
        "matrix_summary": {"robust_cell": recommended},
        "score_lab": {"version": "score_lab_v2_5_3", "score": 99.0},
        "market_data_window": {
            "requested_start": plan["analysis_window_start"],
            "requested_end": plan["analysis_window_end"],
        },
        "tracked_cell_result": (
            {
                **tracked_cell,
                "take_profit_percent": tracked_cell["stop_loss_percent"]
                * tracked_cell["reward_multiple"],
                "total_signals": 10,
                "resolved_trades": 9,
                "wins": 5,
                "losses": 4,
                "unresolved": 1,
                "expectancy_r": 0.1,
            }
            if tracked and tracked_cell
            else None
        ),
    }
    inner = {
        "request": task["payload"],
        "sensitivity_response": {
            "status": "success",
            "requested_timeframe": task["payload"]["timeframe"],
            "effective_timeframe": task["payload"]["timeframe"],
            "data": {"aggregate": aggregate},
        },
        "best_cell_detail": detail(best),
        "recommended_cell_detail": detail(recommended),
        "tracked_cell_detail": detail(tracked_cell) if tracked_cell else None,
        "calendar_curve": detail(best),
    }
    return {
        "status": "success",
        "task_id": task["task_id"],
        "lease_id": f"lease-{task['task_id']}",
        "worker_id": "worker-1",
        "result": {
            "status": "success",
            "job_kind": "full_backtest_cache",
            "completed_at": "2026-07-10T00:00:00Z",
            "execution_evidence": execution_evidence,
            "result": inner,
        },
    }


def _no_valid_terminal_result(task: dict) -> dict:
    plan = dict(task["payload"]["evidence_plan"])
    return {
        "schema": "fuzzfolio-replay-terminal-result-v1",
        "status": "nonviable",
        "outcome": "no_valid_cell",
        "diagnostics": {
            "signal_count": 0,
            "resolved_trade_count_max": 0,
            "market_data_window": {"filtered_bar_count": 123},
        },
        "execution_evidence": {
            **plan,
            "observed_lake_manifest_sha256": plan.get("lake_manifest_sha256"),
        },
    }


def _terminal_worker_result(task: dict) -> dict:
    return {
        "status": "failed",
        "task_id": task["task_id"],
        "lease_id": f"lease-{task['task_id']}",
        "worker_id": "worker-1",
        "result": {
            "error": "FullBacktestNoValidCellError: no valid cell",
            "terminal_result": _no_valid_terminal_result(task),
        },
    }


def _attempt_fixture(
    tmp_path: Path, *, run_id: str, attempt_id: str
) -> tuple[Path, dict]:
    run_dir = tmp_path / "runs" / run_id
    artifact_dir = run_dir / "evals" / "final"
    profile_path = run_dir / "profiles" / "profile.json"
    artifact_dir.mkdir(parents=True)
    _write_json(
        profile_path,
        {
            "profile": {
                "name": attempt_id,
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
    return run_dir, {
        "run_id": run_id,
        "attempt_id": attempt_id,
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }


def test_nested_gateway_date_only_end_is_half_open_midnight() -> None:
    assert _window_end("2026-07-13") == "2026-07-14T00:00:00Z"
    assert _window_end("2026-07-14T00:00:00Z") == "2026-07-14T00:00:00Z"


def test_nested_gateway_fold_is_redacted_and_resumable(
    tmp_path: Path, monkeypatch
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
        "run_id": "run-a",
        "attempt_id": "attempt-a",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }
    items = [(run_dir, attempt, {"attempt_id": "attempt-a"}, {})]
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop")
    )
    lab_config = LabBacktestConfig(worker_contract_hash="sha256:test")

    def fake_train(*, tasks, **_kwargs):
        results = []
        for source_attempt, task in tasks:
            paths = materialize_full_backtest_lab_result(
                attempt=source_attempt,
                task=task,
                lab_result=_worker_result(task, tracked=False),
            )
            results.append({"status": "calculated", **paths})
        return results

    def fake_outer(*, tasks, **_kwargs):
        results = []
        for source_attempt, task, fold_payload in tasks:
            paths = materialize_outer_test_lab_result(
                attempt=source_attempt,
                task=task,
                cell_receipt=fold_payload["cell_receipt"],
                lab_result=_worker_result(task, tracked=True),
            )
            results.append({"status": "calculated", **paths})
        return results

    monkeypatch.setattr("autoresearch.nested_gateway._run_train_tasks", fake_train)
    monkeypatch.setattr("autoresearch.nested_gateway._run_outer_tasks", fake_outer)
    fold = {
        "fold_id": "fold-01",
        "train_start": "2022-01-01",
        "train_end": "2024-12-31",
        "test_start": "2025-01-16",
        "test_end": "2025-06-30",
        "embargo_days": 15,
    }

    first = run_nested_gateway_fold(
        config=config,
        items=items,
        fold=fold,
        campaign_plan_id="nested:test",
        campaign_root=tmp_path / "campaign",
        lab_config=lab_config,
        max_workers=1,
        train_horizon_months=36,
        test_horizon_months=6,
        lake_manifest_sha256="sha256:" + "a" * 64,
    )

    assert first["status"] == "complete"
    assert first["train_calculated_count"] == 1
    assert first["outer_calculated_count"] == 1
    outer_result_path = Path(first["outer_results"][0]["result_path"])
    outer_payload = json.loads(outer_result_path.read_text(encoding="utf-8"))
    aggregate = outer_payload["data"]["aggregate"]
    assert "tracked_cell_result" in aggregate
    assert "matrix" not in aggregate
    assert "best_cell" not in aggregate
    assert "score_lab" not in aggregate

    monkeypatch.setattr(
        "autoresearch.nested_gateway._run_train_tasks",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("train reran")),
    )
    monkeypatch.setattr(
        "autoresearch.nested_gateway._run_outer_tasks",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("outer reran")),
    )
    second = run_nested_gateway_fold(
        config=config,
        items=items,
        fold=fold,
        campaign_plan_id="nested:test",
        campaign_root=tmp_path / "campaign",
        lab_config=lab_config,
        max_workers=1,
        train_horizon_months=36,
        test_horizon_months=6,
        lake_manifest_sha256="sha256:" + "a" * 64,
    )

    assert second["status"] == "complete"
    assert second["train_reused_count"] == 1
    assert second["outer_reused_count"] == 1


def test_nested_gateway_freezes_cells_without_submitting_or_reading_outer_evidence(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir, attempt = _attempt_fixture(
        tmp_path, run_id="run-freeze", attempt_id="attempt-freeze"
    )
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop")
    )
    lab_config = LabBacktestConfig(worker_contract_hash="sha256:test")

    def fake_train(*, tasks, **_kwargs):
        results = []
        for source_attempt, task in tasks:
            paths = materialize_full_backtest_lab_result(
                attempt=source_attempt,
                task=task,
                lab_result=_worker_result(task, tracked=False),
            )
            results.append({"status": "calculated", **paths})
        return results

    monkeypatch.setattr(ng, "_run_train_tasks", fake_train)
    monkeypatch.setattr(
        ng,
        "_run_outer_tasks",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("outer work was submitted before portfolio freeze")
        ),
    )
    result = run_nested_gateway_fold(
        config=config,
        items=[(run_dir, attempt, {"attempt_id": attempt["attempt_id"]}, {})],
        fold={
            "fold_id": "fold-01",
            "train_start": "2022-01-01",
            "train_end": "2024-12-31",
            "test_start": "2025-01-16",
            "test_end": "2025-06-30",
            "embargo_days": 15,
        },
        campaign_plan_id="nested:freeze-first",
        campaign_root=tmp_path / "campaign",
        lab_config=lab_config,
        max_workers=1,
        train_horizon_months=36,
        test_horizon_months=6,
        lake_manifest_sha256="sha256:" + "a" * 64,
        submit_outer=False,
    )

    assert result["status"] == "cells_frozen"
    assert result["outer_results"] == []
    assert result["records"][0]["outer_validation_status"] == "pending_selection"
    assert result["records"][0]["cell_receipt"]["execution_cell"]


def test_nested_gateway_preserves_no_signal_stage_semantics(
    tmp_path: Path, monkeypatch
) -> None:
    train_nonviable_run, train_nonviable_attempt = _attempt_fixture(
        tmp_path,
        run_id="run-train-nonviable",
        attempt_id="attempt-train-nonviable",
    )
    outer_nonviable_run, outer_nonviable_attempt = _attempt_fixture(
        tmp_path,
        run_id="run-outer-nonviable",
        attempt_id="attempt-outer-nonviable",
    )
    items = [
        (
            train_nonviable_run,
            train_nonviable_attempt,
            {"attempt_id": "attempt-train-nonviable"},
            {},
        ),
        (
            outer_nonviable_run,
            outer_nonviable_attempt,
            {"attempt_id": "attempt-outer-nonviable"},
            {},
        ),
    ]
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop")
    )
    lab_config = LabBacktestConfig(worker_contract_hash="sha256:test")

    def fake_train(*, tasks, **_kwargs):
        results = []
        for source_attempt, task in tasks:
            if source_attempt["attempt_id"] == "attempt-train-nonviable":
                paths = materialize_no_valid_cell_lab_result(
                    attempt=source_attempt,
                    task=task,
                    lab_result=_terminal_worker_result(task),
                )
                results.append({"status": "nonviable", **paths})
                continue
            paths = materialize_full_backtest_lab_result(
                attempt=source_attempt,
                task=task,
                lab_result=_worker_result(task, tracked=False),
            )
            results.append({"status": "calculated", **paths})
        return results

    def fake_outer(*, tasks, **_kwargs):
        assert len(tasks) == 1
        results = []
        for source_attempt, task, _fold_payload in tasks:
            paths = materialize_no_valid_cell_lab_result(
                attempt=source_attempt,
                task=task,
                lab_result=_terminal_worker_result(task),
            )
            results.append({"status": "nonviable", **paths})
        return results

    monkeypatch.setattr("autoresearch.nested_gateway._run_train_tasks", fake_train)
    monkeypatch.setattr("autoresearch.nested_gateway._run_outer_tasks", fake_outer)
    fold = {
        "fold_id": "fold-01",
        "train_start": "2022-01-01",
        "train_end": "2024-12-31",
        "test_start": "2025-01-16",
        "test_end": "2025-06-30",
        "embargo_days": 15,
    }

    result = run_nested_gateway_fold(
        config=config,
        items=items,
        fold=fold,
        campaign_plan_id="nested:no-signal-test",
        campaign_root=tmp_path / "campaign",
        lab_config=lab_config,
        max_workers=2,
        train_horizon_months=36,
        test_horizon_months=6,
        lake_manifest_sha256="sha256:" + "a" * 64,
    )

    assert result["status"] == "complete"
    assert result["train_calculated_count"] == 1
    assert result["train_nonviable_count"] == 1
    assert result["outer_calculated_count"] == 0
    assert result["outer_nonviable_count"] == 1
    assert result["outer_failed_count"] == 0
    assert result["outer_skipped_train_nonviable_count"] == 1
    records = {row["attempt_id"]: row for row in result["records"]}
    assert records["attempt-train-nonviable"]["stage_status"] == "train_nonviable"
    assert records["attempt-train-nonviable"]["outer_validation_status"] == "not_applicable"
    assert records["attempt-outer-nonviable"]["outer_validation_status"] == "nonviable"
    assert (
        records["attempt-outer-nonviable"]["outer_terminal_outcome"]["outcome"]
        == "no_valid_cell"
    )

    monkeypatch.setattr(
        "autoresearch.nested_gateway._run_train_tasks",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("train reran")),
    )
    monkeypatch.setattr(
        "autoresearch.nested_gateway._run_outer_tasks",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("outer reran")),
    )
    resumed = run_nested_gateway_fold(
        config=config,
        items=items,
        fold=fold,
        campaign_plan_id="nested:no-signal-test",
        campaign_root=tmp_path / "campaign",
        lab_config=lab_config,
        max_workers=2,
        train_horizon_months=36,
        test_horizon_months=6,
        lake_manifest_sha256="sha256:" + "a" * 64,
    )
    assert resumed["status"] == "complete"
    assert resumed["train_reused_count"] == 2
    assert resumed["train_calculated_count"] == 0
    assert resumed["train_nonviable_count"] == 1
    assert resumed["outer_reused_count"] == 1
    assert resumed["outer_nonviable_count"] == 1


def test_nested_gateway_infrastructure_failures_abort(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir, attempt = _attempt_fixture(
        tmp_path,
        run_id="run-infra-fail",
        attempt_id="attempt-infra-fail",
    )
    items = [(run_dir, attempt, {"attempt_id": "attempt-infra-fail"}, {})]
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop")
    )
    lab_config = LabBacktestConfig(worker_contract_hash="sha256:test")

    def fake_train(*, tasks, **_kwargs):
        results = []
        for source_attempt, task in tasks:
            paths = materialize_full_backtest_lab_result(
                attempt=source_attempt,
                task=task,
                lab_result=_worker_result(task, tracked=False),
            )
            results.append({"status": "calculated", **paths})
        return results

    def fake_outer(*, tasks, **_kwargs):
        return [
            {
                "attempt_id": tasks[0][0]["attempt_id"],
                "task_id": tasks[0][1]["task_id"],
                "status": "failed",
                "error": "worker crashed",
            }
        ]

    monkeypatch.setattr("autoresearch.nested_gateway._run_train_tasks", fake_train)
    monkeypatch.setattr("autoresearch.nested_gateway._run_outer_tasks", fake_outer)

    with pytest.raises(RuntimeError, match="Nested outer stage failed"):
        run_nested_gateway_fold(
            config=config,
            items=items,
            fold={
                "fold_id": "fold-01",
                "train_start": "2022-01-01",
                "train_end": "2024-12-31",
                "test_start": "2025-01-16",
                "test_end": "2025-06-30",
                "embargo_days": 15,
            },
            campaign_plan_id="nested:infra-failure-test",
            campaign_root=tmp_path / "campaign",
            lab_config=lab_config,
            max_workers=1,
            train_horizon_months=36,
            test_horizon_months=6,
            lake_manifest_sha256="sha256:" + "a" * 64,
        )


def test_nested_gateway_task_runners_materialize_no_valid_cell(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir, attempt = _attempt_fixture(
        tmp_path,
        run_id="run-terminal-runner",
        attempt_id="attempt-terminal-runner",
    )
    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile_drop")
    )
    lab_config = LabBacktestConfig(
        worker_contract_hash="sha256:test",
        poll_interval_seconds=0.01,
    )

    class FakeGateway:
        task: dict | None = None
        returned = False
        acked: list[str] = []

        def __init__(self, **_kwargs) -> None:
            pass

        def read_results(self, *, limit: int) -> list[dict]:
            _ = limit
            if self.task is None or self.returned:
                return []
            self.__class__.returned = True
            return [_terminal_worker_result(self.task)]

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.__class__.task = tasks[0]
            return {"accepted": len(tasks)}

        def ack_results(self, lease_ids: list[str]) -> None:
            self.__class__.acked.extend(lease_ids)

        def close(self) -> None:
            pass

    monkeypatch.setattr("autoresearch.nested_gateway.LabGatewayClient", FakeGateway)
    train_task = build_full_backtest_lab_task(
        config=config,
        run_dir=run_dir,
        attempt=attempt,
        run_metadata={},
        lab_config=lab_config,
        evidence_window_start="2022-01-01T00:00:00Z",
        evidence_window_end="2025-01-01T00:00:00Z",
        requested_horizon_months=36,
        campaign_plan_id="nested:runner-terminal",
        lake_manifest_sha256="sha256:" + "a" * 64,
        task_id="train-terminal",
    )

    train_results = ng._run_train_tasks(
        tasks=[(attempt, train_task)],
        lab_config=lab_config,
        max_workers=1,
        emit=None,
    )

    assert train_results[0]["status"] == "nonviable"
    assert train_results[0]["terminal_outcome"]["outcome"] == "no_valid_cell"
    assert FakeGateway.acked == ["lease-train-terminal"]

    FakeGateway.task = None
    FakeGateway.returned = False
    FakeGateway.acked = []
    tracked_cell = {"stop_loss_percent": 0.1, "reward_multiple": 2.0}
    outer_task = build_full_backtest_lab_task(
        config=config,
        run_dir=run_dir,
        attempt=attempt,
        run_metadata={},
        lab_config=lab_config,
        evidence_window_start="2025-01-16T00:00:00Z",
        evidence_window_end="2025-07-01T00:00:00Z",
        requested_horizon_months=6,
        evidence_role="outer_test",
        campaign_plan_id="nested:runner-terminal",
        lake_manifest_sha256="sha256:" + "a" * 64,
        tracked_cell=tracked_cell,
        task_id="outer-terminal",
    )

    outer_results = ng._run_outer_tasks(
        tasks=[
            (
                attempt,
                outer_task,
                {"cell_receipt": {"execution_cell": tracked_cell}},
            )
        ],
        lab_config=lab_config,
        max_workers=1,
        emit=None,
    )

    assert outer_results[0]["status"] == "nonviable"
    assert outer_results[0]["terminal_outcome"]["outcome"] == "no_valid_cell"
    assert FakeGateway.acked == ["lease-outer-terminal"]
