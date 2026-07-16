from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from autoresearch.portfolio_optimizer import (
    PortfolioOptimizerSpec,
    build_optimizer_candidates,
    objective_weights_for_spec,
)
from autoresearch.portfolio_research import (
    PortfolioResearchError,
    add_months,
    apply_behavioral_cluster_deduplication,
    analyze_campaign,
    build_consensus_evidence,
    build_experiment_spec,
    expand_experiments,
    family_selection_frequency,
    intersect_candidate_calendar,
    package_research_finalist,
    portfolio_similarity,
    rebuild_research_report,
    run_research_campaign,
    run_atomic_experiment,
    run_nested_cell_temporal_validation,
    selection_frequency,
    slice_candidate,
    temporal_adjacent_churn,
    temporal_folds,
    validate_suite,
)


def _row(
    tmp_path: Path,
    attempt_id: str,
    instrument: str,
    points: list[tuple[str, float]],
) -> dict:
    result_path = tmp_path / f"{attempt_id}-result.json"
    curve_path = tmp_path / f"{attempt_id}-calendar.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "score_lab": {"score": 70.0},
                        "resolved_trade_count_max": len(points),
                        "best_cell_path_metrics": {
                            "avg_holding_hours": 8.0,
                            "p90_holding_hours": 16.0,
                            "max_holding_hours": 24.0,
                            "path_quality": 0.9,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    curve_path.write_text(
        json.dumps(
            {
                "curve": {
                    "points": [
                        {
                            "date": date_text,
                            "equity_r": equity_r,
                            "open_trade_count": 1,
                            "closed_trade_count": 1,
                        }
                        for date_text, equity_r in points
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    return {
        "attempt_id": attempt_id,
        "run_id": f"run-{attempt_id}",
        "candidate_name": f"{attempt_id}-strategy",
        "profile_ref": f"profile-{attempt_id}",
        "score_36m": 70.0,
        "full_backtest_validation_status_36m": "valid",
        "full_backtest_result_path_36m": str(result_path),
        "full_backtest_calendar_curve_path_36m": str(curve_path),
        "instruments_36m": [instrument],
        "trade_count_36m": len(points),
        "trades_per_month_36m": 12.0,
        "selected_stop_loss_percent_36m": 1.0,
    }


def _suite(*, temporal_enabled: bool = False) -> dict:
    return {
        "portfolio": {
            "sizes": [2],
            "objectives": ["stability"],
            "base_optimizer_args": {
                "candidate_limit": 3,
                "min_score": 50.0,
                "random_starts": 0,
                "max_swaps": 0,
                "max_per_family": 1,
                "min_fx_share": 0,
                "max_metal_share": 2,
                "max_index_share": 2,
                "max_instrument_share": 1,
            },
        },
        "robustness": {
            "seeds": [17],
            "candidate_limits": [3],
            "risk_weight_multipliers": [1.0],
            "diversification_profiles": [{"name": "test"}],
        },
        "temporal_validation": {"enabled": temporal_enabled},
        "selection_policy": {
            "require_no_account_failure": False,
            "max_negative_test_folds": 0,
            "minimum_worst_test_return_r": -100.0,
        },
    }


def test_suite_validation_and_matrix_expansion() -> None:
    suite = _suite()
    suite["portfolio"] = {**suite["portfolio"], "sizes": [1, 2], "objectives": ["return", "stability"]}
    suite["robustness"] = {
        **suite["robustness"],
        "seeds": [7, 11],
        "candidate_limits": [3, 5],
        "risk_weight_multipliers": [0.8, 1.2],
        "diversification_profiles": [{"name": "base"}, {"name": "diverse"}],
    }

    validate_suite(suite, suite_name="unit")
    experiments = expand_experiments(suite)

    assert len(experiments) == 64
    assert len({row["experiment_id"] for row in experiments}) == 64
    assert {
        (row["portfolio_size"], row["objective"], row["risk_weight_multiplier"])
        for row in experiments
    } == {
        (size, objective, multiplier)
        for size in (1, 2)
        for objective in ("return", "stability")
        for multiplier in (0.8, 1.2)
    }

    with pytest.raises(PortfolioResearchError, match="positive portfolio.sizes"):
        validate_suite({**suite, "portfolio": {"sizes": [0], "objectives": ["return"]}}, suite_name="broken")
    invalid_consensus = _suite()
    invalid_consensus["selection_policy"][
        "minimum_consensus_fold_frequency"
    ] = 1.1
    with pytest.raises(PortfolioResearchError, match="must be between 0 and 1"):
        validate_suite(invalid_consensus, suite_name="broken-consensus")
    unsupported_nested = _suite(temporal_enabled=True)
    unsupported_nested["temporal_validation"].update(
        {
            "train_months": 12,
            "test_months": 3,
            "step_months": 3,
            "nested_cell_selection": True,
        }
    )
    with pytest.raises(PortfolioResearchError, match="not implemented"):
        validate_suite(unsupported_nested, suite_name="unsupported-nested")
    retired_health_flag = _suite()
    retired_health_flag["selection_policy"]["require_zero_invalid_artifacts"] = True
    with pytest.raises(PortfolioResearchError, match="retired"):
        validate_suite(retired_health_flag, suite_name="retired-health-flag")


def test_temporal_folds_apply_month_geometry_and_embargo() -> None:
    folds = temporal_folds(
        start="2025-01-01",
        end="2025-08-31",
        train_months=2,
        test_months=1,
        step_months=2,
        embargo_days=3,
    )

    assert folds == [
        {
            "fold_id": "fold-01",
            "train_start": "2025-01-01",
            "train_end": "2025-02-28",
            "test_start": "2025-03-04",
            "test_end": "2025-04-03",
            "embargo_days": 3,
        },
        {
            "fold_id": "fold-02",
            "train_start": "2025-03-01",
            "train_end": "2025-04-30",
            "test_start": "2025-05-04",
            "test_end": "2025-06-03",
            "embargo_days": 3,
        },
        {
            "fold_id": "fold-03",
            "train_start": "2025-05-01",
            "train_end": "2025-06-30",
            "test_start": "2025-07-04",
            "test_end": "2025-08-03",
            "embargo_days": 3,
        },
    ]


def test_temporal_folds_keep_equal_step_outer_tests_contiguous() -> None:
    folds = temporal_folds(
        start="2021-06-29",
        end="2026-07-13",
        train_months=36,
        test_months=6,
        step_months=6,
        embargo_days=15,
    )

    assert folds == [
        {
            "fold_id": "fold-01",
            "train_start": "2021-06-29",
            "train_end": "2024-06-28",
            "test_start": "2024-07-14",
            "test_end": "2025-01-13",
            "embargo_days": 15,
        },
        {
            "fold_id": "fold-02",
            "train_start": "2021-12-30",
            "train_end": "2024-12-29",
            "test_start": "2025-01-14",
            "test_end": "2025-07-13",
            "embargo_days": 15,
        },
        {
            "fold_id": "fold-03",
            "train_start": "2022-06-29",
            "train_end": "2025-06-28",
            "test_start": "2025-07-14",
            "test_end": "2026-01-13",
            "embargo_days": 15,
        },
        {
            "fold_id": "fold-04",
            "train_start": "2022-12-30",
            "train_end": "2025-12-29",
            "test_start": "2026-01-14",
            "test_end": "2026-07-13",
            "embargo_days": 15,
        },
    ]
    assert folds[-1]["test_end"] == "2026-07-13"
    for left, right in zip(folds, folds[1:]):
        assert date.fromisoformat(right["test_start"]) == date.fromisoformat(
            left["test_end"]
        ) + timedelta(days=1)
    for fold in folds:
        train_start = date.fromisoformat(fold["train_start"])
        train_end = date.fromisoformat(fold["train_end"])
        test_start = date.fromisoformat(fold["test_start"])
        assert add_months(train_start, 36) - timedelta(days=1) == train_end
        assert (test_start - train_end).days - 1 == 15


def test_temporal_folds_handle_leap_and_year_end_contiguity() -> None:
    leap_folds = temporal_folds(
        start="2019-02-14",
        end="2021-02-28",
        train_months=12,
        test_months=6,
        step_months=6,
        embargo_days=15,
    )
    assert [(row["test_start"], row["test_end"]) for row in leap_folds] == [
        ("2020-02-29", "2020-08-28"),
        ("2020-08-29", "2021-02-27"),
    ]

    year_end_folds = temporal_folds(
        start="2021-12-16",
        end="2023-04-30",
        train_months=12,
        test_months=2,
        step_months=2,
        embargo_days=15,
    )
    assert [(row["test_start"], row["test_end"]) for row in year_end_folds] == [
        ("2022-12-31", "2023-02-27"),
        ("2023-02-28", "2023-04-27"),
    ]
    for folds in (leap_folds, year_end_folds):
        for left, right in zip(folds, folds[1:]):
            assert date.fromisoformat(right["test_start"]) == date.fromisoformat(
                left["test_end"]
            ) + timedelta(days=1)
        for fold in folds:
            assert (
                date.fromisoformat(fold["test_start"])
                - date.fromisoformat(fold["train_end"])
            ).days - 1 == 15


def test_nested_cell_validation_selects_on_train_and_scores_outer_only(
    tmp_path: Path,
) -> None:
    train_points = [
        ((date(2023, 1, 1) + timedelta(days=index)).isoformat(), index / 100.0)
        for index in range(730)
    ]
    outer_points = [("2025-01-01", 0.0), ("2025-01-02", -0.5)]
    row = _row(tmp_path, "nested-a", "EURUSD", train_points)
    row["score_36m"] = -999.0
    outer_curve = tmp_path / "nested-a-outer.json"
    outer_curve.write_text(
        json.dumps(
            {
                "curve": {
                    "points": [
                        {"date": date_text, "equity_r": equity_r}
                        for date_text, equity_r in outer_points
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    suite = _suite(temporal_enabled=True)
    suite["portfolio"]["sizes"] = [1]
    suite["portfolio"]["base_optimizer_args"].update(
        {"candidate_limit": -1, "min_score": 0.0}
    )
    suite["temporal_validation"].update(
        {
            "train_months": 12,
            "test_months": 3,
            "step_months": 3,
            "seeds": [17],
            "inner_validation": {
                "train_months": 6,
                "test_months": 3,
                "step_months": 3,
                "embargo_days": 1,
                "minimum_units": 2,
            },
        }
    )
    receipt = {
        "execution_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0}
    }
    fold_reports = [
        {
            "fold": {
                "fold_id": "fold-01",
                "train_start": "2023-01-01",
                "train_end": "2024-12-31",
                "embargo_days": 1,
            },
            "records": [
                {
                    "attempt_id": "nested-a",
                    "outer_validation_status": "valid",
                    "train_result_path": row["full_backtest_result_path_36m"],
                    "train_curve_path": row[
                        "full_backtest_calendar_curve_path_36m"
                    ],
                    "outer_curve_path": str(outer_curve),
                    "cell_receipt": receipt,
                }
            ],
        }
    ]

    results = run_nested_cell_temporal_validation(
        rows=[row],
        fold_reports=fold_reports,
        suite=suite,
        account={},
        root=tmp_path / "nested-validation",
        backend="python",
    )

    assert len(results) == 1
    assert results[0]["selected_attempt_ids"] == ["nested-a"]
    assert results[0]["evidence_level"] == "level_ab_inner_consensus_frozen_cell_outer_test"
    assert results[0]["inner_fold_count"] >= 2
    assert results[0]["consensus_core_attempt_ids"] == ["nested-a"]
    frozen = json.loads(Path(results[0]["frozen_portfolio_path"]).read_text(encoding="utf-8"))
    assert frozen["selected_attempt_ids"] == ["nested-a"]
    assert "test_metrics" not in frozen
    assert results[0]["test_metrics"]["final_r"] == pytest.approx(-0.5)
    assert results[0]["cell_receipts"]["nested-a"] == receipt

    outer_curve.write_text(json.dumps({"curve": {"points": []}}), encoding="utf-8")
    with pytest.raises(PortfolioResearchError, match="missing frozen portfolio members"):
        run_nested_cell_temporal_validation(
            rows=[row],
            fold_reports=fold_reports,
            suite=suite,
            account={},
            root=tmp_path / "nested-missing-outer",
            backend="python",
        )

    suite["selection_policy"]["minimum_consensus_core_count"] = 2
    rejected = run_nested_cell_temporal_validation(
        rows=[row],
        fold_reports=fold_reports,
        suite=suite,
        account={},
        root=tmp_path / "nested-no-consensus",
        backend="python",
    )
    assert rejected[0]["status"] == "no_defensible_consensus"
    assert rejected[0]["selected_attempt_ids"] == []
    assert "test_metrics" not in rejected[0]


def test_nested_cell_validation_keeps_outer_no_signal_member_flat(
    tmp_path: Path,
) -> None:
    train_points = [
        ((date(2023, 1, 1) + timedelta(days=index)).isoformat(), index / 100.0)
        for index in range(730)
    ]
    row = _row(tmp_path, "nested-flat", "EURUSD", train_points)
    suite = _suite(temporal_enabled=True)
    suite["portfolio"]["sizes"] = [1]
    suite["portfolio"]["base_optimizer_args"].update(
        {"candidate_limit": -1, "min_score": 0.0}
    )
    suite["temporal_validation"].update(
        {
            "train_months": 12,
            "test_months": 3,
            "step_months": 3,
            "seeds": [17],
            "inner_validation": {
                "train_months": 6,
                "test_months": 3,
                "step_months": 3,
                "embargo_days": 1,
                "minimum_units": 2,
            },
        }
    )
    receipt = {
        "execution_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0}
    }
    fold_reports = [
        {
            "fold": {
                "fold_id": "fold-01",
                "train_start": "2023-01-01",
                "train_end": "2024-12-31",
                "test_start": "2025-01-01",
                "test_end": "2025-06-30",
                "embargo_days": 1,
            },
            "records": [
                {
                    "attempt_id": "nested-flat",
                    "train_validation_status": "valid",
                    "outer_validation_status": "nonviable",
                    "outer_terminal_outcome": {
                        "status": "nonviable",
                        "outcome": "no_valid_cell",
                    },
                    "train_result_path": row["full_backtest_result_path_36m"],
                    "train_curve_path": row[
                        "full_backtest_calendar_curve_path_36m"
                    ],
                    "cell_receipt": receipt,
                }
            ],
        }
    ]

    results = run_nested_cell_temporal_validation(
        rows=[row],
        fold_reports=fold_reports,
        suite=suite,
        account={},
        root=tmp_path / "nested-flat-validation",
        backend="python",
    )

    assert len(results) == 1
    assert results[0]["selected_attempt_ids"] == ["nested-flat"]
    assert results[0]["test_metrics"]["final_r"] == pytest.approx(0.0)
    frozen = json.loads(Path(results[0]["frozen_portfolio_path"]).read_text(encoding="utf-8"))
    assert frozen["selected_attempt_ids"] == ["nested-flat"]


def test_stability_domains_keep_full_window_and_temporal_evidence_separate(
    tmp_path: Path,
) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0)]
    candidates, _ = build_optimizer_candidates(
        [
            _row(tmp_path, "alpha", "EURUSD", points),
            _row(tmp_path, "beta", "GBPUSD", points),
            _row(tmp_path, "gamma", "XAUUSD", points),
        ],
        PortfolioOptimizerSpec(portfolio_size=2, min_fx_share=0, max_metal_share=2),
    )
    candidate_by_id = {candidate.attempt_id: candidate for candidate in candidates}
    full_window = [
        {"experiment_id": "full-1", "selected_attempt_ids": ["alpha", "beta"]},
        {"experiment_id": "full-2", "selected_attempt_ids": ["alpha", "beta"]},
    ]
    temporal = [
        {
            "experiment_id": "temporal-1",
            "objective": "stability",
            "portfolio_size": 2,
            "random_seed": 29,
            "fold": {"fold_id": "fold-01", "train_start": "2024-01-01"},
            "selected_attempt_ids": ["alpha", "beta"],
        },
        {
            "experiment_id": "temporal-2",
            "objective": "stability",
            "portfolio_size": 2,
            "random_seed": 29,
            "fold": {"fold_id": "fold-02", "train_start": "2024-02-01"},
            "selected_attempt_ids": ["beta", "gamma"],
        },
    ]

    full_similarity = portfolio_similarity(full_window, domain="full_window")
    temporal_similarity = portfolio_similarity(temporal, domain="temporal")
    frequency = selection_frequency(
        temporal,
        domain="temporal",
        candidate_by_id=candidate_by_id,
    )
    family_frequency = family_selection_frequency(
        temporal,
        domain="temporal",
        candidate_by_id=candidate_by_id,
    )
    churn = temporal_adjacent_churn(temporal)

    assert full_similarity[0]["jaccard"] == 1.0
    assert temporal_similarity[0]["jaccard"] == pytest.approx(1 / 3)
    assert temporal_similarity[0]["left_id"] == "fold-01:temporal-1"
    assert frequency[0]["attempt_id"] == "beta"
    assert frequency[0]["selection_frequency"] == 1.0
    assert all(row["domain"] == "temporal" for row in family_frequency)
    assert churn == [
        {
            "domain": "temporal",
            "objective": "stability",
            "portfolio_size": 2,
            "random_seed": 29,
            "previous_fold_id": "fold-01",
            "current_fold_id": "fold-02",
            "previous_count": 2,
            "current_count": 2,
            "retained_count": 1,
            "added_count": 1,
            "removed_count": 1,
            "jaccard": pytest.approx(1 / 3),
            "churn": pytest.approx(2 / 3),
        }
    ]


def test_consensus_core_requires_persistence_across_distinct_folds(tmp_path: Path) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0)]
    candidates, _ = build_optimizer_candidates(
        [
            _row(tmp_path, "single-fold-favorite", "EURUSD", points),
            _row(tmp_path, "persistent", "GBPUSD", points),
        ],
        PortfolioOptimizerSpec(portfolio_size=1, min_fx_share=0),
    )
    temporal = [
        {
            "fold": {"fold_id": "fold-01"},
            "selected_attempt_ids": ["single-fold-favorite", "persistent"],
        },
        {
            "fold": {"fold_id": "fold-01"},
            "selected_attempt_ids": ["single-fold-favorite"],
        },
        {
            "fold": {"fold_id": "fold-01"},
            "selected_attempt_ids": ["single-fold-favorite"],
        },
        {
            "fold": {"fold_id": "fold-02"},
            "selected_attempt_ids": ["persistent"],
        },
    ]

    evidence = build_consensus_evidence(
        temporal,
        candidate_by_id={candidate.attempt_id: candidate for candidate in candidates},
        policy={
            "minimum_consensus_selection_frequency": 0.5,
            "minimum_consensus_fold_frequency": 1.0,
        },
    )

    assert evidence["raw_core_attempt_ids"] == ["persistent"]
    rows = {row["attempt_id"]: row for row in evidence["strategy_rows"]}
    assert rows["single-fold-favorite"]["selection_frequency"] == 0.75
    assert rows["single-fold-favorite"]["fold_frequency"] == 0.5
    assert rows["single-fold-favorite"]["category"] == "conditional_sleeve"
    assert rows["persistent"]["category"] == "stable_core"


def test_behavioral_core_dedup_requires_direct_similarity_not_bridge_membership() -> None:
    consensus = {
        "core_attempt_ids": ["alpha", "beta", "gamma"],
        "core_count": 3,
        "strategy_rows": [
            {
                "attempt_id": "alpha",
                "category": "stable_core",
                "selection_count": 3,
                "selected_fold_count": 2,
                "score": 80.0,
                "full_window_return_r": 10.0,
            },
            {
                "attempt_id": "beta",
                "category": "stable_core",
                "selection_count": 2,
                "selected_fold_count": 2,
                "score": 70.0,
                "full_window_return_r": 9.0,
            },
            {
                "attempt_id": "gamma",
                "category": "stable_core",
                "selection_count": 1,
                "selected_fold_count": 2,
                "score": 60.0,
                "full_window_return_r": 8.0,
            },
        ],
    }

    refined = apply_behavioral_cluster_deduplication(
        consensus,
        assignments={
            "alpha": "behavior:alpha",
            "beta": "behavior:alpha",
            "gamma": "behavior:alpha",
        },
        attempt_ids=["alpha", "beta", "gamma"],
        similarity_matrix=[
            [1.0, 0.9, 0.1],
            [0.9, 1.0, 0.9],
            [0.1, 0.9, 1.0],
        ],
        threshold=0.8,
    )

    assert refined["core_attempt_ids"] == ["alpha", "gamma"]
    rows = {row["attempt_id"]: row for row in refined["strategy_rows"]}
    assert rows["beta"]["category"] == "excluded_behavioral_substitute"
    assert rows["beta"]["behavioral_duplicate_of_attempt_id"] == "alpha"
    assert rows["gamma"]["category"] == "stable_core"


def test_candidate_calendar_intersection_and_slicing(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "alpha", "EURUSD", [("2025-01-01", 1.0), ("2025-01-02", 2.0), ("2025-01-03", 3.0), ("2025-01-04", 4.0)]),
        _row(tmp_path, "beta", "GBPUSD", [("2025-01-02", 0.5), ("2025-01-03", 1.0), ("2025-01-04", 1.5), ("2025-01-05", 2.0)]),
    ]
    candidates, rejections = build_optimizer_candidates(
        rows, PortfolioOptimizerSpec(portfolio_size=1, min_fx_share=0)
    )

    intersected, calendar = intersect_candidate_calendar(candidates)

    assert rejections == {}
    assert calendar == {
        "common_effective_start": "2025-01-02",
        "common_effective_end": "2025-01-04",
        "calendar_day_count": 3,
    }
    assert [candidate.dates for candidate in intersected] == [
        ["2025-01-02", "2025-01-03", "2025-01-04"],
        ["2025-01-02", "2025-01-03", "2025-01-04"],
    ]
    assert slice_candidate(candidates[0], start="2025-01-03", end="2025-01-03").daily_r == [1.0]


def test_research_campaign_persists_snapshot_report_finalist_and_resumes(tmp_path: Path) -> None:
    points = [
        ("2025-01-01", 1.0),
        ("2025-01-02", 2.0),
        ("2025-01-03", 3.0),
        ("2025-01-04", 4.0),
    ]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", [(date_text, equity_r * 0.8) for date_text, equity_r in points]),
        _row(tmp_path, "gamma", "XAUUSD", [(date_text, equity_r * 0.6) for date_text, equity_r in points]),
    ]
    campaign_root = tmp_path / "campaign"
    suite = _suite()

    summary = run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="unit-campaign",
        suite_name="unit",
        suite=suite,
        rows=rows,
        account={"frozen_marker": "original"},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={"source": "unit-test"},
        optimizer_backend="python",
        experiment_limit=1,
    )

    snapshot_path = campaign_root / "inputs" / "candidate-snapshot.json"
    experiment_id = expand_experiments(suite)[0]["experiment_id"]
    result_path = campaign_root / "experiments" / experiment_id / "result.json"
    snapshot_before = snapshot_path.read_text(encoding="utf-8")
    result_before = result_path.read_text(encoding="utf-8")
    report = json.loads((campaign_root / "report.json").read_text(encoding="utf-8"))

    assert summary["candidate_count"] == 3
    assert summary["experiment_count"] == 1
    assert summary["fold_count"] == 0
    assert report["campaign"]["status"] == "complete"
    assert json.loads((campaign_root / "status.json").read_text(encoding="utf-8"))["status"] == "complete"
    assert json.loads(snapshot_before)["candidate_count"] == 3
    assert (campaign_root / "finalists" / "champion" / "portfolio.json").exists()
    assert (campaign_root / "analysis" / "downside-correlation.csv").exists()
    assert (campaign_root / "analysis" / "instrument-concentration.csv").exists()
    assert (campaign_root / "analysis" / "regime-roles.csv").exists()
    assert (campaign_root / "campaign-manifest.json").exists()
    assert not list(campaign_root.rglob("*profile-drop*"))
    assert json.loads(
        (campaign_root / "inputs" / "resolved-account.json").read_text(encoding="utf-8")
    ) == {"frozen_marker": "original"}

    resumed = run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="unit-campaign",
        suite_name="unit",
        suite=suite,
        rows=[],
        account={"frozen_marker": "changed"},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={"source": "changed-input-is-ignored-on-resume"},
        optimizer_backend="python",
        experiment_limit=1,
        resume=True,
    )

    assert resumed["candidate_count"] == 3
    assert snapshot_path.read_text(encoding="utf-8") == snapshot_before
    assert result_path.read_text(encoding="utf-8") == result_before
    assert json.loads(
        (campaign_root / "inputs" / "resolved-account.json").read_text(encoding="utf-8")
    ) == {"frozen_marker": "original"}
    progress_events = [
        json.loads(line)
        for line in (campaign_root / "progress.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert progress_events[-1]["event"] == "campaign_resume_complete"


def test_resume_rejects_tampered_terminal_report(tmp_path: Path) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0)]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", points),
        _row(tmp_path, "gamma", "XAUUSD", points),
    ]
    campaign_root = tmp_path / "campaign"
    run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="tamper-campaign",
        suite_name="unit",
        suite=_suite(),
        rows=rows,
        account={},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={"source": "unit-test"},
        optimizer_backend="python",
        experiment_limit=1,
    )
    report_path = campaign_root / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["candidate_count"] = 999
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(PortfolioResearchError, match="artifact changed"):
        run_research_campaign(
            campaign_root=campaign_root,
            campaign_id="tamper-campaign",
            suite_name="unit",
            suite=_suite(),
            rows=[],
            account={},
            corpus_health={"promotable": True, "status": "complete"},
            provenance={},
            optimizer_backend="python",
            experiment_limit=1,
            resume=True,
        )


def test_resume_rejects_candidate_snapshot_manifest_mismatch(tmp_path: Path) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0)]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", points),
        _row(tmp_path, "gamma", "XAUUSD", points),
    ]
    campaign_root = tmp_path / "campaign"
    run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="snapshot-tamper",
        suite_name="unit",
        suite=_suite(),
        rows=rows,
        account={},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={},
        optimizer_backend="python",
        experiment_limit=1,
    )
    snapshot_path = campaign_root / "inputs" / "candidate-snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["candidates"][0]["daily_r"][0] = 999.0
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    with pytest.raises(PortfolioResearchError, match="artifact changed"):
        run_research_campaign(
            campaign_root=campaign_root,
            campaign_id="snapshot-tamper",
            suite_name="unit",
            suite=_suite(),
            rows=[],
            account={},
            corpus_health={"promotable": True, "status": "complete"},
            provenance={},
            optimizer_backend="python",
            experiment_limit=1,
            resume=True,
        )


def test_report_rebuild_is_an_immutable_derivative(tmp_path: Path) -> None:
    points = [
        ("2025-01-01", 1.0),
        ("2025-01-02", 2.0),
        ("2025-01-03", 3.0),
        ("2025-01-04", 4.0),
    ]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", points),
        _row(tmp_path, "gamma", "XAUUSD", points),
    ]
    campaign_root = tmp_path / "campaign"
    run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="rebuild-campaign",
        suite_name="unit",
        suite=_suite(),
        rows=rows,
        account={},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={"source": "unit-test"},
        optimizer_backend="python",
        experiment_limit=1,
    )
    original_report = (campaign_root / "report.json").read_bytes()
    original_campaign = (campaign_root / "campaign.json").read_bytes()

    rebuilt = rebuild_research_report(campaign_root=campaign_root)

    rebuilt_path = Path(rebuilt["report_json"])
    assert rebuilt_path.parent.parent == campaign_root / "report-rebuilds"
    assert (rebuilt_path.parent / "manifest.json").is_file()
    assert (campaign_root / "report.json").read_bytes() == original_report
    assert (campaign_root / "campaign.json").read_bytes() == original_campaign


def test_finalist_package_is_content_addressed_and_fails_on_profile_drift(
    tmp_path: Path,
) -> None:
    campaign_root = tmp_path / "campaign"
    profile_path = tmp_path / "profile.json"
    profile_path.write_text('{"name":"Stable Trend"}', encoding="utf-8")
    profile_sha = hashlib.sha256(profile_path.read_bytes()).hexdigest()
    (campaign_root / "inputs").mkdir(parents=True)
    (campaign_root / "finalists" / "champion").mkdir(parents=True)
    campaign = {"campaign_id": "pack-me", "status": "complete", "promotable": True}
    finalist = {
        "promotion_eligible": True,
        "evidence_level": "level_ab_inner_consensus_frozen_cell_outer_test",
        "selected_attempt_ids": ["alpha"],
    }
    campaign_path = campaign_root / "campaign.json"
    report_path = campaign_root / "report.json"
    portfolio_path = campaign_root / "finalists" / "champion" / "portfolio.json"
    snapshot_path = campaign_root / "inputs" / "candidate-snapshot.json"
    campaign_path.write_text(json.dumps(campaign), encoding="utf-8")
    report_path.write_text(
        json.dumps(
            {
                "campaign": campaign,
                "provenance": {"level_c_cohort": {"verified": True}},
                "analysis": {"finalists": {"champion": finalist}},
            }
        ),
        encoding="utf-8",
    )
    portfolio_path.write_text(json.dumps(finalist), encoding="utf-8")
    snapshot_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "attempt_id": "alpha",
                        "candidate_name": "Stable Trend",
                        "source": {
                            "paths": {"profile_path": str(profile_path)},
                            "sha256": {"profile_path": profile_sha},
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    snapshot_manifest_path = campaign_root / "inputs" / "candidate-snapshot-manifest.json"
    snapshot_manifest_path.write_text(
        json.dumps(
            {
                "artifact_sha256": {
                    "inputs/candidate-snapshot.json": hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
                }
            }
        ),
        encoding="utf-8",
    )
    completion_paths = (
        campaign_path,
        report_path,
        snapshot_path,
        snapshot_manifest_path,
        portfolio_path,
    )
    (campaign_root / "campaign-manifest.json").write_text(
        json.dumps(
            {
                "artifact_sha256": {
                    str(path.relative_to(campaign_root)).replace("\\", "/"): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in completion_paths
                }
            }
        ),
        encoding="utf-8",
    )

    first = package_research_finalist(
        campaign_root=campaign_root, finalist_name="champion"
    )
    second = package_research_finalist(
        campaign_root=campaign_root, finalist_name="champion"
    )

    assert first == second
    assert Path(first["manifest_path"]).is_file()
    assert first["profile_count"] == 1
    profile_path.write_text('{"name":"Drifted"}', encoding="utf-8")
    with pytest.raises(PortfolioResearchError, match="missing or changed"):
        package_research_finalist(
            campaign_root=campaign_root, finalist_name="champion"
        )


def test_package_rejects_finalist_that_differs_from_completed_report(tmp_path: Path) -> None:
    campaign_root = tmp_path / "campaign"
    profile_path = tmp_path / "profile.json"
    profile_path.write_text('{"name":"Stable"}', encoding="utf-8")
    rows = [
        _row(tmp_path, "alpha", "EURUSD", [("2025-01-01", 1.0)]),
        _row(tmp_path, "beta", "GBPUSD", [("2025-01-01", 1.0)]),
        _row(tmp_path, "gamma", "XAUUSD", [("2025-01-01", 1.0)]),
    ]
    for row in rows:
        row["profile_path"] = str(profile_path)
    run_research_campaign(
        campaign_root=campaign_root,
        campaign_id="package-tamper",
        suite_name="unit",
        suite=_suite(),
        rows=rows,
        account={},
        corpus_health={"promotable": True, "status": "complete"},
        provenance={},
        optimizer_backend="python",
    )
    portfolio_path = campaign_root / "finalists" / "champion" / "portfolio.json"
    if not portfolio_path.exists():
        pytest.skip("unit optimizer did not produce a champion")
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    portfolio["selected_attempt_ids"] = ["gamma"]
    portfolio_path.write_text(json.dumps(portfolio), encoding="utf-8")

    with pytest.raises(PortfolioResearchError, match="artifact changed"):
        package_research_finalist(
            campaign_root=campaign_root, finalist_name="champion"
        )


def test_full_window_experiment_reapplies_score_and_positive_return_eligibility(
    tmp_path: Path,
) -> None:
    rows = [
        _row(tmp_path, "eligible", "EURUSD", [("2025-01-01", 1.0), ("2025-01-02", 2.0)]),
        _row(tmp_path, "low-score", "GBPUSD", [("2025-01-01", 10.0), ("2025-01-02", 20.0)]),
        _row(tmp_path, "negative", "USDJPY", [("2025-01-01", -1.0), ("2025-01-02", -2.0)]),
    ]
    rows[1]["score_36m"] = 20.0
    snapshot_candidates, _ = build_optimizer_candidates(
        rows,
        PortfolioOptimizerSpec(
            portfolio_size=1,
            min_score=float("-inf"),
            require_positive_source_return=False,
            min_fx_share=0,
        ),
    )
    spec = PortfolioOptimizerSpec(
        portfolio_size=1,
        min_score=50.0,
        objective_names=("return",),
        candidate_limit=10,
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
        max_instrument_share=1,
    )
    result = run_atomic_experiment(
        candidates=snapshot_candidates,
        spec=spec,
        experiment={"experiment_id": "eligibility", "objective": "return"},
        output_dir=tmp_path / "experiment",
        backend="python",
        progress=None,
    )

    assert result["selected_attempt_ids"] == ["eligible"]


def test_finalists_use_only_each_temporal_selection_own_oos_result(
    tmp_path: Path,
) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0), ("2025-01-03", 3.0)]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", points),
        _row(tmp_path, "gamma", "XAUUSD", points),
    ]
    candidates, _ = build_optimizer_candidates(
        rows, PortfolioOptimizerSpec(portfolio_size=2, min_fx_share=0, max_metal_share=2)
    )
    suite = _suite(temporal_enabled=True)
    suite["temporal_validation"].update(
        {"train_months": 1, "test_months": 1, "step_months": 1}
    )
    experiments = [
        {
            "experiment_id": "full-window-leaky",
            "portfolio_size": 2,
            "objective": "stability",
            "selected_attempt_ids": ["alpha", "beta"],
        }
    ]
    temporal = [
        {
            "experiment_id": "fold-selection",
            "portfolio_size": 2,
            "objective": "stability",
            "selected_attempt_ids": ["beta", "gamma"],
            "fold": {
                "fold_id": "fold-01",
                "test_start": "2025-01-03",
                "test_end": "2025-01-03",
            },
            "test_metrics": {"final_r": 2.0, "maxdd_r": 0.0},
        }
    ]

    analysis = analyze_campaign(
        candidates=candidates,
        suite=suite,
        account={},
        experiments=experiments,
        folds=[temporal[0]["fold"]],
        temporal_results=temporal,
        root=tmp_path / "analysis-campaign",
        promotable=True,
    )

    assert len(analysis["portfolio_evaluations"]) == 1
    finalist = analysis["finalists"]["champion"]
    assert set(finalist["selected_attempt_ids"]) == {"beta", "gamma"}
    assert finalist["evidence_level"] == "walk_forward_out_of_sample"
    assert finalist["test_scenarios"] == [
        {
            "fold_id": "fold-01",
            "test_start": "2025-01-03",
            "test_end": "2025-01-03",
            "metrics": {"final_r": 2.0, "maxdd_r": 0.0},
        }
    ]


def test_selection_policy_rejects_one_off_temporal_portfolio_and_removes_stale_finalist(
    tmp_path: Path,
) -> None:
    points = [("2025-01-01", 1.0), ("2025-01-02", 2.0), ("2025-01-03", 3.0)]
    rows = [
        _row(tmp_path, "alpha", "EURUSD", points),
        _row(tmp_path, "beta", "GBPUSD", points),
    ]
    candidates, _ = build_optimizer_candidates(
        rows, PortfolioOptimizerSpec(portfolio_size=2, min_fx_share=0)
    )
    suite = _suite(temporal_enabled=True)
    suite["selection_policy"].update(
        {"minimum_selection_support": 2, "minimum_oos_scenarios": 2}
    )
    fold = {
        "fold_id": "fold-01",
        "test_start": "2025-01-03",
        "test_end": "2025-01-03",
    }
    temporal = [
        {
            "experiment_id": "fold-selection",
            "portfolio_size": 2,
            "objective": "stability",
            "selected_attempt_ids": ["alpha", "beta"],
            "fold": fold,
            "test_metrics": {"final_r": 2.0, "maxdd_r": 0.0},
        }
    ]
    root = tmp_path / "analysis-campaign"
    stale_finalist = root / "finalists" / "champion" / "portfolio.json"
    stale_finalist.parent.mkdir(parents=True)
    stale_finalist.write_text("{}", encoding="utf-8")

    analysis = analyze_campaign(
        candidates=candidates,
        suite=suite,
        account={},
        experiments=[],
        folds=[fold],
        temporal_results=temporal,
        root=root,
        promotable=True,
    )

    assert analysis["finalists"] == {
        "champion": None,
        "conservative": None,
        "return_alternate": None,
    }
    assert set(analysis["portfolio_evaluations"][0]["gate_reasons"]) == {
        "selection_support_below_minimum",
        "oos_scenario_count_below_minimum",
    }
    assert not stale_finalist.exists()


def test_risk_weight_multiplier_scales_only_risk_objective_terms() -> None:
    suite = _suite()
    experiment = expand_experiments(suite)[0]
    baseline = build_experiment_spec(
        suite,
        {**experiment, "risk_weight_multiplier": 1.0},
        account={},
        name_prefix="unit",
    )
    doubled = build_experiment_spec(
        suite,
        {**experiment, "risk_weight_multiplier": 2.0},
        account={},
        name_prefix="unit",
    )

    baseline_weights = objective_weights_for_spec(baseline)
    doubled_weights = objective_weights_for_spec(doubled)

    assert doubled.risk_weight_multiplier == 2.0
    for objective, weights in baseline_weights.items():
        for term, value in weights.items():
            expected = value if term == "final_r" else value * 2.0
            assert doubled_weights[objective][term] == pytest.approx(expected)
