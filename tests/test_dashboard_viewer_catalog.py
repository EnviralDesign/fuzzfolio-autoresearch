from __future__ import annotations

import json
import sqlite3
import pytest
from types import SimpleNamespace

from autoresearch.catalog_index import refresh_incremental_attempt_catalog
from autoresearch.dashboard_viewer import (
    _PROFILE_DROP_EXIT_POLICY_CACHE,
    _RESULT_CELL_CACHE,
    ViewerState,
    _normalize_path_fields,
)
from autoresearch.ledger import ATTEMPTS_FILE_NAME, RUN_METADATA_FILE_NAME


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_attempt(run_dir, *, attempt_id: str, score: float) -> None:
    artifact_dir = run_dir / "evals" / attempt_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    profile_path = artifact_dir / "profile.json"
    _write_json(profile_path, {"profile": {"instruments": ["EURUSD"]}})
    attempt = {
        "run_id": run_dir.name,
        "attempt_id": attempt_id,
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
        "composite_score": score,
        "best_summary": {"best_cell": {"trade_count": 4, "resolved_trades": 4}},
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / ATTEMPTS_FILE_NAME).write_text(
        json.dumps(attempt, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _write_json(run_dir / RUN_METADATA_FILE_NAME, {"run_status": "complete"})


def test_viewer_state_loads_catalog_from_sqlite_without_json(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-1"
    _write_attempt(run_dir, attempt_id="sqlite-attempt", score=42.0)
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=runs_root,
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_summary_path=derived_root / "attempt-catalog-summary.json",
        full_backtest_audit_json_path=derived_root / "full-backtest-audit.json",
        promotion_board_json_path=derived_root / "promotion-board.json",
        corpus_tradeoff_plot_path=derived_root / "corpus-score-vs-trades.png",
    )
    refresh_incremental_attempt_catalog(config, run_dirs=[run_dir], load_rows=False)

    rows = ViewerState(config).catalog_rows()

    assert [row["attempt_id"] for row in rows] == ["sqlite-attempt"]
    assert not config.attempt_catalog_json_path.exists()


def test_viewer_state_requires_sqlite_catalog(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=runs_root,
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_summary_path=derived_root / "attempt-catalog-summary.json",
        full_backtest_audit_json_path=derived_root / "full-backtest-audit.json",
        promotion_board_json_path=derived_root / "promotion-board.json",
        corpus_tradeoff_plot_path=derived_root / "corpus-score-vs-trades.png",
    )

    with pytest.raises(FileNotFoundError, match="Attempt catalog SQLite database"):
        ViewerState(config).catalog_rows()


def test_viewer_state_surfaces_corrupt_sqlite_catalog(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=runs_root,
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_summary_path=derived_root / "attempt-catalog-summary.json",
        full_backtest_audit_json_path=derived_root / "full-backtest-audit.json",
        promotion_board_json_path=derived_root / "promotion-board.json",
        corpus_tradeoff_plot_path=derived_root / "corpus-score-vs-trades.png",
    )
    config.attempt_catalog_sqlite_path.write_text("not sqlite", encoding="utf-8")
    _write_json(config.attempt_catalog_json_path, [{"attempt_id": "json-attempt"}])

    with pytest.raises(sqlite3.DatabaseError):
        ViewerState(config).catalog_rows()


def test_dashboard_catalog_prefers_rendered_profile_drop_exit_policy(tmp_path) -> None:
    _RESULT_CELL_CACHE.clear()
    _PROFILE_DROP_EXIT_POLICY_CACHE.clear()
    artifact_dir = tmp_path / "runs" / "run-1" / "evals" / "final"
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    profile_document_path = (
        artifact_dir
        / ".profile-drop-36mo"
        / "bundle"
        / "latest"
        / "profile-document.json"
    )
    _write_json(
        result_path,
        {
            "data": {
                "aggregate": {
                    "recommended_cell": {
                        "reward_multiple": 12,
                        "stop_loss_percent": 0.04,
                        "take_profit_percent": 0.48,
                    }
                }
            }
        },
    )
    _write_json(
        profile_document_path,
        {
            "profile": {
                "executionConfig": {
                    "exitPolicy": {
                        "recommendation": {
                            "basis": "best_cell",
                            "cell": {
                                "rewardMultiple": 6.5,
                                "stopLossPercent": 0.02,
                                "takeProfitPercent": 0.13,
                            },
                        }
                    }
                }
            }
        },
    )

    config = SimpleNamespace(repo_root=tmp_path, runs_root=tmp_path / "runs")
    normalized = _normalize_path_fields(
        config,
        {
            "run_id": "run-1",
            "artifact_dir": str(artifact_dir),
            "full_backtest_result_path_36m": str(result_path),
        },
    )

    assert normalized["reward_multiple_36m"] == 6.5
    assert normalized["selected_stop_loss_percent_36m"] == 0.02
    assert normalized["selected_take_profit_percent_36m"] == 0.13
    assert normalized["reward_multiple_basis_36m"] == "best_cell"


def test_dashboard_catalog_uses_raw_recommended_cell_without_profile_drop(tmp_path) -> None:
    _RESULT_CELL_CACHE.clear()
    _PROFILE_DROP_EXIT_POLICY_CACHE.clear()
    artifact_dir = tmp_path / "runs" / "run-1" / "evals" / "final"
    result_path = artifact_dir / "full-backtest-36mo-result.json"
    _write_json(
        result_path,
        {
            "data": {
                "aggregate": {
                    "recommended_cell": {
                        "reward_multiple": 12,
                        "stop_loss_percent": 0.04,
                        "take_profit_percent": 0.48,
                    }
                }
            }
        },
    )

    config = SimpleNamespace(repo_root=tmp_path, runs_root=tmp_path / "runs")
    normalized = _normalize_path_fields(
        config,
        {
            "run_id": "run-1",
            "artifact_dir": str(artifact_dir),
            "full_backtest_result_path_36m": str(result_path),
        },
    )

    assert normalized["reward_multiple_36m"] == 12
    assert normalized["selected_stop_loss_percent_36m"] == 0.04
    assert normalized["selected_take_profit_percent_36m"] == 0.48
    assert normalized["reward_multiple_basis_36m"] == "recommended_cell"
