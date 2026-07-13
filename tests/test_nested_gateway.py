from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autoresearch.corpus_lab_backtests import (
    LabBacktestConfig,
    build_full_backtest_lab_task,
    materialize_full_backtest_lab_result,
    materialize_outer_test_lab_result,
)
from autoresearch.nested_gateway import run_nested_gateway_fold


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
