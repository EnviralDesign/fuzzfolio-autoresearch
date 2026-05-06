from __future__ import annotations

import json
from types import SimpleNamespace

from autoresearch.dashboard_viewer import (
    _PROFILE_DROP_EXIT_POLICY_CACHE,
    _RESULT_CELL_CACHE,
    _normalize_path_fields,
)


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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
