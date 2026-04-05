import json
from autoresearch.plotting import render_attempt_tradeoff_scatter_artifacts

from autoresearch import corpus_tools as ct


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _sample_sensitivity_payload(score: float) -> dict:
    return {
        "data": {
            "aggregate": {
                "quality_score": score,
                "quality_score_version": "v1",
                "quality_score_belief_basis": "psr",
                "timeframe": "M15",
                "best_cell": {
                    "resolved_trades": 90,
                    "profit_factor": 2.1,
                    "avg_net_r_per_closed_trade": 0.5,
                    "reward_multiple": 1.5,
                    "stop_loss_percent": 0.3,
                    "take_profit_percent": 0.45,
                },
                "best_cell_path_metrics": {
                    "psr": 0.99,
                    "k_ratio": 12.0,
                    "sharpe_r": 0.8,
                    "trade_count": 90,
                    "max_drawdown_r": 4.0,
                },
                "quality_score_payload": {
                    "score": score,
                    "version": "v1",
                    "belief_basis": "psr",
                    "inputs": {
                        "effective_window_months": 35.5,
                        "trades_per_month": 2.5,
                        "max_drawdown_r": 4.0,
                    },
                },
            }
        }
    }


def _sample_curve_payload(scale: float = 1.0, *, cell: dict | None = None) -> dict:
    return {
        "cell": cell
        or {
            "reward_multiple": 1.5,
            "stop_loss_percent": 0.3,
            "take_profit_percent": 0.45,
        },
        "curve": {
            "points": [
                {"date": f"2024-01-{day:02d}", "realized_r": float(day) * scale}
                for day in range(1, 40)
            ]
        }
    }


def test_resolve_attempt_scrutiny_source_falls_back_to_full_backtest(tmp_path):
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_RESULT_FILENAME,
        _sample_sensitivity_payload(61.25),
    )
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_CURVE_FILENAME,
        _sample_curve_payload(1.0),
    )
    _write_json(
        artifact_dir / "deep-replay-job.json",
        {"request": {"timeframe": "M15", "instruments": ["EURUSD"]}},
    )

    attempt = {
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "artifact_dir": str(artifact_dir),
    }
    resolved = ct.resolve_attempt_scrutiny_source(attempt, 36)

    assert resolved["available"] is True
    assert resolved["source"] == "full_backtest"
    assert resolved["score"] == 61.25
    assert resolved["timeframe"] == "M15"
    assert resolved["instruments"] == ["EURUSD"]


def test_validate_full_backtest_artifacts_accepts_valid_pair(tmp_path):
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_RESULT_FILENAME,
        _sample_sensitivity_payload(61.25),
    )
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_CURVE_FILENAME,
        _sample_curve_payload(1.0),
    )

    attempt = {"artifact_dir": str(artifact_dir)}
    validation = ct.validate_full_backtest_artifacts(attempt)

    assert validation["status"] == "valid"
    assert validation["issues"] == []
    assert validation["curve_point_count"] == 39
    assert validation["cell_match"] is True


def test_validate_full_backtest_artifacts_flags_mismatched_cell(tmp_path):
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_RESULT_FILENAME,
        _sample_sensitivity_payload(61.25),
    )
    _write_json(
        artifact_dir / ct.FULL_BACKTEST_CURVE_FILENAME,
        _sample_curve_payload(
            1.0,
            cell={
                "reward_multiple": 2.0,
                "stop_loss_percent": 0.3,
                "take_profit_percent": 0.6,
            },
        ),
    )

    attempt = {"artifact_dir": str(artifact_dir)}
    validation = ct.validate_full_backtest_artifacts(attempt)

    assert validation["status"] == "invalid"
    assert "best_cell_mismatch" in validation["issues"]
    assert validation["cell_match"] is False


def test_build_similarity_payload_uses_instrument_overlap(tmp_path):
    curve_a = tmp_path / "a.json"
    curve_b = tmp_path / "b.json"
    curve_c = tmp_path / "c.json"
    _write_json(curve_a, _sample_curve_payload(1.0))
    _write_json(curve_b, _sample_curve_payload(1.0))
    _write_json(curve_c, _sample_curve_payload(1.0))

    rows = [
        {
            "run_id": "run-a",
            "attempt_id": "attempt-a",
            "candidate_name": "A",
            "score_36m": 70.0,
            "scrutiny_curve_path_36m": str(curve_a),
            "instruments_36m": ["EURUSD"],
            "timeframe_36m": "M15",
        },
        {
            "run_id": "run-b",
            "attempt_id": "attempt-b",
            "candidate_name": "B",
            "score_36m": 69.0,
            "scrutiny_curve_path_36m": str(curve_b),
            "instruments_36m": ["EURUSD"],
            "timeframe_36m": "M15",
        },
        {
            "run_id": "run-c",
            "attempt_id": "attempt-c",
            "candidate_name": "C",
            "score_36m": 68.0,
            "scrutiny_curve_path_36m": str(curve_c),
            "instruments_36m": ["GBPUSD"],
            "timeframe_36m": "M15",
        },
    ]

    payload = ct.build_similarity_payload(rows)
    pairs = {
        tuple(sorted([pair["left_attempt_id"], pair["right_attempt_id"]])): pair
        for pair in payload["pairs"]
    }

    same_instrument = pairs[("attempt-a", "attempt-b")]["similarity_score"]
    different_instrument = pairs[("attempt-a", "attempt-c")]["similarity_score"]
    assert same_instrument > different_instrument


def test_select_promotion_board_prefers_diverse_second_pick():
    rows = [
        {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
        {"attempt_id": "B", "run_id": "run-b", "score_36m": 79.0},
        {"attempt_id": "C", "run_id": "run-c", "score_36m": 76.0},
    ]
    similarity_payload = {
        "leaders": [],
        "pairs": [
            {"left_attempt_id": "A", "right_attempt_id": "B", "similarity_score": 0.9},
            {"left_attempt_id": "A", "right_attempt_id": "C", "similarity_score": 0.1},
            {"left_attempt_id": "B", "right_attempt_id": "C", "similarity_score": 0.1},
        ],
    }

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=2,
        novelty_penalty=10.0,
        max_sameness_to_board=None,
        max_per_run=None,
        max_per_strategy_key=None,
    )

    selected_ids = [row["attempt_id"] for row in board["selected"]]
    assert selected_ids == ["A", "C"]


def test_select_promotion_board_respects_sameness_ceiling():
    rows = [
        {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
        {"attempt_id": "B", "run_id": "run-b", "score_36m": 79.0},
    ]
    similarity_payload = {
        "leaders": [],
        "pairs": [
            {"left_attempt_id": "A", "right_attempt_id": "B", "similarity_score": 0.95},
        ],
    }

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=2,
        novelty_penalty=10.0,
        max_sameness_to_board=0.85,
        max_per_run=None,
        max_per_strategy_key=None,
    )

    selected_ids = [row["attempt_id"] for row in board["selected"]]
    assert selected_ids == ["A"]


def test_select_promotion_board_respects_run_cap():
    rows = [
        {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0, "strategy_key_36m": "M15|EURUSD"},
        {"attempt_id": "B", "run_id": "run-a", "score_36m": 79.0, "strategy_key_36m": "M15|GBPUSD"},
        {"attempt_id": "C", "run_id": "run-b", "score_36m": 70.0, "strategy_key_36m": "M15|USDJPY"},
    ]
    similarity_payload = {
        "leaders": [],
        "pairs": [],
    }

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=3,
        novelty_penalty=10.0,
        max_sameness_to_board=None,
        max_per_run=1,
        max_per_strategy_key=None,
    )

    selected_ids = [row["attempt_id"] for row in board["selected"]]
    assert selected_ids == ["A", "C"]


def test_select_promotion_board_respects_strategy_cap():
    rows = [
        {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0, "strategy_key_36m": "M15|EURUSD"},
        {"attempt_id": "B", "run_id": "run-b", "score_36m": 79.0, "strategy_key_36m": "M15|EURUSD"},
        {"attempt_id": "C", "run_id": "run-c", "score_36m": 70.0, "strategy_key_36m": "M5|GBPUSD"},
    ]
    similarity_payload = {
        "leaders": [],
        "pairs": [],
    }

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=3,
        novelty_penalty=10.0,
        max_sameness_to_board=None,
        max_per_run=None,
        max_per_strategy_key=1,
    )

    selected_ids = [row["attempt_id"] for row in board["selected"]]
    assert selected_ids == ["A", "C"]


def test_select_promotion_board_can_penalize_high_drawdown():
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "max_drawdown_r_36m": 18.0,
            "strategy_key_36m": "M15|EURUSD",
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 78.0,
            "max_drawdown_r_36m": 4.0,
            "strategy_key_36m": "M15|GBPUSD",
        },
    ]
    similarity_payload = {"leaders": [], "pairs": []}

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=1,
        novelty_penalty=0.0,
        drawdown_penalty=0.5,
        max_drawdown_r=None,
        max_sameness_to_board=None,
        max_per_run=None,
        max_per_strategy_key=None,
    )

    assert [row["attempt_id"] for row in board["selected"]] == ["B"]


def test_select_promotion_board_can_reward_higher_trade_cadence():
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "trades_per_month_36m": 0.3,
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 78.0,
            "trades_per_month_36m": 6.0,
        },
    ]
    similarity_payload = {"leaders": [], "pairs": []}

    board = ct.select_promotion_board(
        rows,
        similarity_payload,
        board_size=1,
        novelty_penalty=0.0,
        trade_rate_bonus_weight=3.0,
        trade_rate_bonus_target=6.0,
        max_sameness_to_board=None,
        max_per_run=None,
        max_per_strategy_key=None,
    )

    selected = board["selected"]
    assert [row["attempt_id"] for row in selected] == ["B"]
    assert float(selected[0]["trade_rate_bonus_component"]) > 0.0


def test_catalog_summary_reports_full_backtest_validation_counts():
    rows = [
        {
            "run_id": "run-a",
            "composite_score": 1.0,
            "base_strategy_key": "M15|EURUSD",
            "strategy_key_36m": "M15|EURUSD",
            "has_scrutiny_12m": True,
            "has_scrutiny_36m": True,
            "has_full_backtest_36m": True,
            "has_sensitivity_response": True,
            "score_36m": 72.0,
            "full_backtest_validation_status_36m": "valid",
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": True,
        },
        {
            "run_id": "run-b",
            "composite_score": 1.0,
            "base_strategy_key": "M15|GBPUSD",
            "strategy_key_36m": "M15|GBPUSD",
            "has_scrutiny_12m": False,
            "has_scrutiny_36m": True,
            "has_full_backtest_36m": False,
            "has_sensitivity_response": True,
            "score_36m": 64.0,
            "full_backtest_validation_status_36m": "missing",
            "has_full_backtest_result_36m": False,
            "has_full_backtest_curve_36m": False,
        },
        {
            "run_id": "run-c",
            "composite_score": 1.0,
            "base_strategy_key": "M5|USDJPY",
            "strategy_key_36m": "M5|USDJPY",
            "has_scrutiny_12m": False,
            "has_scrutiny_36m": False,
            "has_full_backtest_36m": False,
            "has_sensitivity_response": False,
            "score_36m": None,
            "full_backtest_validation_status_36m": "invalid",
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": False,
        },
    ]

    summary = ct.catalog_summary(rows)

    assert summary["attempts_with_scrutiny_36m"] == 2
    assert summary["attempts_with_full_backtest_36m"] == 1
    assert summary["attempts_with_valid_full_backtest_36m"] == 1
    assert summary["attempts_with_invalid_full_backtest_36m"] == 1
    assert summary["attempts_with_partial_full_backtest_36m"] == 1
    assert summary["full_backtest_36m_vs_scrutiny_coverage_ratio"] == 0.5
    assert summary["valid_full_backtest_36m_vs_scrutiny_coverage_ratio"] == 0.5


def test_build_full_backtest_audit_marks_partial_corpus_as_provisional():
    rows = [
        {
            "run_id": "run-a",
            "attempt_id": "A",
            "candidate_name": "A",
            "score_36m": 72.0,
            "composite_score": 50.0,
            "strategy_key_36m": "M15|EURUSD",
            "trades_per_month_36m": 2.0,
            "has_scrutiny_36m": True,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
            "full_backtest_validation_issues_36m": [],
            "full_backtest_curve_point_count_36m": 39,
            "full_backtest_result_path_36m": "result-a.json",
            "full_backtest_curve_path_36m": "curve-a.json",
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": True,
            "base_strategy_key": "M15|EURUSD",
            "has_scrutiny_12m": False,
            "has_sensitivity_response": True,
        },
        {
            "run_id": "run-b",
            "attempt_id": "B",
            "candidate_name": "B",
            "score_36m": 68.0,
            "composite_score": 48.0,
            "strategy_key_36m": "M15|GBPUSD",
            "trades_per_month_36m": 1.8,
            "has_scrutiny_36m": True,
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
            "full_backtest_validation_issues_36m": [],
            "full_backtest_curve_point_count_36m": 0,
            "full_backtest_result_path_36m": "result-b.json",
            "full_backtest_curve_path_36m": "curve-b.json",
            "has_full_backtest_result_36m": False,
            "has_full_backtest_curve_36m": False,
            "base_strategy_key": "M15|GBPUSD",
            "has_scrutiny_12m": False,
            "has_sensitivity_response": True,
        },
        {
            "run_id": "run-c",
            "attempt_id": "C",
            "candidate_name": "C",
            "score_36m": 55.0,
            "composite_score": 40.0,
            "strategy_key_36m": "M5|USDJPY",
            "trades_per_month_36m": 1.2,
            "has_scrutiny_36m": False,
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "invalid",
            "full_backtest_validation_issues_36m": ["missing_curve_file"],
            "full_backtest_curve_point_count_36m": 0,
            "full_backtest_result_path_36m": "result-c.json",
            "full_backtest_curve_path_36m": "curve-c.json",
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": False,
            "base_strategy_key": "M5|USDJPY",
            "has_scrutiny_12m": False,
            "has_sensitivity_response": True,
        },
    ]

    audit = ct.build_full_backtest_audit(rows, invalid_example_limit=5, pending_example_limit=5)

    assert audit["status"] == "provisional"
    assert audit["summary"]["attempts_with_valid_full_backtest_36m"] == 1
    assert audit["summary"]["attempts_with_invalid_full_backtest_36m"] == 1
    assert audit["pending_scrutiny_examples"][0]["attempt_id"] == "B"
    assert any("coverage is still incomplete" in reason for reason in audit["provisional_reasons"])


def test_render_attempt_tradeoff_scatter_artifacts_filters_full_backtest_only(tmp_path):
    rows = [
        {
            "attempt_id": "A",
            "candidate_name": "alpha",
            "score_36m": 70.0,
            "trades_per_month_36m": 2.5,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "B",
            "candidate_name": "beta",
            "score_36m": 68.0,
            "trades_per_month_36m": 3.0,
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
        },
    ]

    plotted = render_attempt_tradeoff_scatter_artifacts(
        rows,
        tmp_path / "chart.png",
        tmp_path / "chart.json",
        require_full_backtest_36=True,
    )

    assert [row["attempt_id"] for row in plotted] == ["A"]
    assert (tmp_path / "chart.png").exists()
    assert (tmp_path / "chart.json").exists()


def test_render_attempt_tradeoff_scatter_artifacts_accepts_x_axis_cap(tmp_path):
    rows = [
        {
            "attempt_id": "A",
            "candidate_name": "alpha",
            "score_36m": 70.0,
            "trades_per_month_36m": 450.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        }
    ]

    plotted = render_attempt_tradeoff_scatter_artifacts(
        rows,
        tmp_path / "chart-cap.png",
        tmp_path / "chart-cap.json",
        require_full_backtest_36=False,
        x_axis_max=300.0,
    )

    assert [row["attempt_id"] for row in plotted] == ["A"]
    assert (tmp_path / "chart-cap.png").exists()
