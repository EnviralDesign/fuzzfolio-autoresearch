import json
from pathlib import Path

import pytest

from autoresearch import portfolio_optimizer as portfolio_optimizer_module
from autoresearch.portfolio_optimizer import (
    PortfolioOptimizerSpec,
    PortfolioSearch,
    build_optimizer_candidates,
    run_optimizer_backend,
)


def _write_attempt_artifacts(
    tmp_path: Path,
    name: str,
    daily_equity: list[float],
    *,
    avg_hold: float = 12.0,
    p90_hold: float = 24.0,
    max_hold: float = 48.0,
) -> tuple[Path, Path]:
    result_path = tmp_path / f"{name}-result.json"
    curve_path = tmp_path / f"{name}-calendar.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "best_cell_path_metrics": {
                            "avg_holding_hours": avg_hold,
                            "p90_holding_hours": p90_hold,
                            "max_holding_hours": max_hold,
                            "path_quality": 0.8,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    points = []
    for index, equity in enumerate(daily_equity, start=1):
        points.append(
            {
                "date": f"2026-01-{index:02d}",
                "equity_r": equity,
                "open_trade_count": 1 if index < len(daily_equity) else 0,
                "closed_trade_count": 1,
            }
        )
    curve_path.write_text(json.dumps({"curve": {"points": points}}), encoding="utf-8")
    return result_path, curve_path


def _row(
    tmp_path: Path,
    attempt_id: str,
    instrument: str,
    daily_equity: list[float],
    *,
    score: float = 70.0,
    avg_hold: float = 12.0,
) -> dict:
    result_path, curve_path = _write_attempt_artifacts(
        tmp_path,
        attempt_id,
        daily_equity,
        avg_hold=avg_hold,
        p90_hold=avg_hold * 2,
        max_hold=avg_hold * 3,
    )
    return {
        "attempt_id": attempt_id,
        "run_id": f"run-{attempt_id}",
        "candidate_name": attempt_id,
        "score_36m": score,
        "full_backtest_validation_status_36m": "valid",
        "full_backtest_result_path_36m": str(result_path),
        "full_backtest_calendar_curve_path_36m": str(curve_path),
        "instruments_36m": [instrument],
        "trade_count_36m": len(daily_equity),
        "trades_per_month_36m": 10,
        "selected_stop_loss_percent_36m": 1.0,
    }


def test_optimizer_filters_long_hold_candidates(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "fast", "EURUSD", [1, 2, 3], avg_hold=12),
        _row(tmp_path, "slow", "GBPUSD", [10, 20, 30], avg_hold=80),
    ]
    spec = PortfolioOptimizerSpec(portfolio_size=1, max_avg_hold_hours=48)

    candidates, rejections = build_optimizer_candidates(rows, spec)

    assert [candidate.attempt_id for candidate in candidates] == ["fast"]
    assert rejections["avg_hold_too_long"] == 1


def test_optimizer_filters_unsupported_and_blocked_instruments(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "darwin-ok", "US500", [1, 2, 3]),
        _row(tmp_path, "unsupported", "RUSS2000", [1, 2, 3]),
        _row(tmp_path, "blocked", "XTIUSD", [1, 2, 3]),
    ]
    spec = PortfolioOptimizerSpec(
        allowed_asset_classes=("fx", "metal", "index", "commodity"),
        allowed_instruments=("EURUSD", "US500", "XTIUSD"),
        blocked_instruments=("XTIUSD",),
    )

    candidates, rejections = build_optimizer_candidates(rows, spec)

    assert [candidate.attempt_id for candidate in candidates] == ["darwin-ok"]
    assert rejections["unsupported_instrument"] == 1
    assert rejections["blocked_instrument"] == 1


def test_optimizer_selects_portfolio_from_source_calendar_curves(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    candidates, _ = build_optimizer_candidates(rows, spec)
    search = PortfolioSearch(candidates, spec)

    variants = search.optimize()

    selected = set(variants["stability"]["selected_attempt_ids"])
    assert selected == {"smooth-a", "smooth-b"}
    metrics = variants["stability"]["metrics"]
    assert metrics["constraint_violations"] == {}
    assert metrics["max_daily_loss_streak"] == 0


def test_optimizer_backend_dispatch_preserves_python_default(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    candidates, _ = build_optimizer_candidates(rows, spec)

    search, variants, pareto_front, used_backend = run_optimizer_backend(
        candidates,
        spec,
    )

    assert used_backend == "python"
    assert isinstance(search, PortfolioSearch)
    assert set(variants["stability"]["selected_attempt_ids"]) == {"smooth-a", "smooth-b"}
    assert pareto_front


def test_optimizer_backend_auto_falls_back_to_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    candidates, _ = build_optimizer_candidates(rows, spec)
    events: list[dict] = []

    def broken_pyo3(*args, **kwargs):
        raise RuntimeError("extension unavailable")

    monkeypatch.setattr(portfolio_optimizer_module, "_run_pyo3_optimizer", broken_pyo3)

    _, variants, _, used_backend = run_optimizer_backend(
        candidates,
        spec,
        backend="auto",
        progress_callback=events.append,
    )

    assert used_backend == "python"
    assert set(variants["stability"]["selected_attempt_ids"]) == {"smooth-a", "smooth-b"}
    assert any(event.get("event") == "rust_optimizer_fallback" for event in events)


def test_pareto_front_keeps_nondominated_archived_portfolios(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "weak-lumpy", "USDJPY", [1, -3, 2, 2], score=65),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    candidates, _ = build_optimizer_candidates(rows, spec)
    search = PortfolioSearch(candidates, spec)

    search.record_archive(
        ["smooth-a", "smooth-b"],
        objective_name="balanced",
        label="good",
    )
    search.record_archive(
        ["smooth-a", "weak-lumpy"],
        objective_name="balanced",
        label="dominated",
    )

    front = search.pareto_front()

    assert [item["archive_label"] for item in front] == ["good"]
    assert front[0]["metrics"]["neg_months"] == 0


def _equity_from_returns(returns: list[float]) -> list[float]:
    equity: list[float] = []
    total = 0.0
    for value in returns:
        total += value
        equity.append(total)
    return equity


def test_zero_diversification_weights_keep_legacy_selection(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    base_kwargs = dict(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        max_per_family=1,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
    )
    legacy_spec = PortfolioOptimizerSpec(**base_kwargs)
    zero_specs = [
        PortfolioOptimizerSpec(
            **base_kwargs,
            correlation_penalty_weight=0.0,
            diversification_mode="penalty",
            portfolio_sharpe_weight=0.0,
        ),
        PortfolioOptimizerSpec(
            **base_kwargs,
            correlation_penalty_weight=0.0,
            diversification_mode="marginal_sharpe",
            portfolio_sharpe_weight=0.0,
        ),
    ]
    legacy_candidates, _ = build_optimizer_candidates(rows, legacy_spec)
    legacy_variants = PortfolioSearch(legacy_candidates, legacy_spec).optimize()

    for spec in zero_specs:
        candidates, _ = build_optimizer_candidates(rows, spec)
        variants = PortfolioSearch(candidates, spec).optimize()
        assert (
            variants["stability"]["selected_attempt_ids"]
            == legacy_variants["stability"]["selected_attempt_ids"]
        )
        assert (
            variants["stability"]["objective_score"]
            == legacy_variants["stability"]["objective_score"]
        )
        assert variants["stability"]["diversification"]["correlation_penalty"] == 0.0
        assert variants["stability"]["diversification"]["portfolio_sharpe_term"] == 0.0


def test_penalty_mode_prefers_uncorrelated_candidate_over_clone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        portfolio_optimizer_module,
        "DEFAULT_OBJECTIVES",
        {"return": {"final_r": 1.0, "maxdd_r": -2.0}},
    )
    clone_returns = [2.0, -0.5, 2.0, -0.5, 2.0, -0.5]
    uncorr_returns = [-0.2, 0.6, -0.2, 0.6, -0.2, 0.6]
    rows = [
        _row(tmp_path, "clone-a", "EURUSD", _equity_from_returns(clone_returns), score=90),
        _row(tmp_path, "clone-b", "GBPUSD", _equity_from_returns(clone_returns), score=89),
        _row(tmp_path, "uncorr-c", "USDJPY", _equity_from_returns(uncorr_returns), score=60),
    ]
    base_kwargs = dict(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
    )

    legacy_spec = PortfolioOptimizerSpec(**base_kwargs)
    legacy_candidates, _ = build_optimizer_candidates(rows, legacy_spec)
    legacy_variants = PortfolioSearch(legacy_candidates, legacy_spec).optimize()
    assert set(legacy_variants["return"]["selected_attempt_ids"]) == {"clone-a", "clone-b"}

    penalty_spec = PortfolioOptimizerSpec(
        **base_kwargs,
        correlation_penalty_weight=10.0,
        diversification_mode="penalty",
    )
    penalty_candidates, _ = build_optimizer_candidates(rows, penalty_spec)
    penalty_variants = PortfolioSearch(penalty_candidates, penalty_spec).optimize()
    selected = set(penalty_variants["return"]["selected_attempt_ids"])
    assert "uncorr-c" in selected
    assert selected != {"clone-a", "clone-b"}
    diversification = penalty_variants["return"]["diversification"]
    assert diversification["correlation_penalty"] == pytest.approx(0.0, abs=1e-9)
    assert penalty_variants["return"]["metrics"]["avg_positive_pair_corr"] == pytest.approx(
        0.0, abs=1e-9
    )


def test_marginal_sharpe_mode_prefers_anticorrelated_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        portfolio_optimizer_module,
        "DEFAULT_OBJECTIVES",
        {"return": {"final_r": 1.0, "maxdd_r": -2.0}},
    )
    seed_returns = [2.0, -0.5, 2.0, -0.5, 2.0, -0.5]
    correlated_returns = [1.5, -0.3, 1.5, -0.3, 1.5, -0.3]
    anti_returns = [-0.2, 0.6, -0.2, 0.6, -0.2, 0.6]
    rows = [
        _row(tmp_path, "seed", "EURUSD", _equity_from_returns(seed_returns), score=90),
        _row(
            tmp_path,
            "corr-high-r",
            "GBPUSD",
            _equity_from_returns(correlated_returns),
            score=80,
        ),
        _row(tmp_path, "anti-low-r", "USDJPY", _equity_from_returns(anti_returns), score=60),
    ]
    base_kwargs = dict(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("return",),
        random_starts=0,
        max_swaps=0,
        min_fx_share=0,
    )

    legacy_spec = PortfolioOptimizerSpec(**base_kwargs)
    legacy_candidates, _ = build_optimizer_candidates(rows, legacy_spec)
    legacy_variants = PortfolioSearch(legacy_candidates, legacy_spec).optimize()
    assert set(legacy_variants["return"]["selected_attempt_ids"]) == {"seed", "corr-high-r"}

    sharpe_spec = PortfolioOptimizerSpec(
        **base_kwargs,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=5.0,
    )
    sharpe_candidates, _ = build_optimizer_candidates(rows, sharpe_spec)
    sharpe_variants = PortfolioSearch(sharpe_candidates, sharpe_spec).optimize()
    assert set(sharpe_variants["return"]["selected_attempt_ids"]) == {"seed", "anti-low-r"}
    diversification = sharpe_variants["return"]["diversification"]
    assert diversification["portfolio_sharpe"] == pytest.approx(0.95 / 0.85, rel=1e-9)
    assert diversification["portfolio_sharpe_term"] == pytest.approx(
        5.0 * (0.95 / 0.85), rel=1e-9
    )


def test_correlation_objective_avoids_disk_io_and_reuses_pair_matrix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [
        _row(tmp_path, "smooth-a", "EURUSD", [1, 2, 3, 4], score=65),
        _row(tmp_path, "smooth-b", "XAUUSD", [0.5, 1.5, 2.5, 3.5], score=65),
        _row(tmp_path, "lumpy", "USDJPY", [8, 1, 9, 2], score=95),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=2,
        candidate_limit=3,
        objective_names=("stability",),
        random_starts=0,
        max_swaps=4,
        min_fx_share=0,
        max_metal_share=2,
        max_index_share=2,
        max_instrument_share=1,
        correlation_penalty_weight=5.0,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=2.0,
    )
    candidates, _ = build_optimizer_candidates(rows, spec)

    io_calls = {"count": 0}
    real_reader = portfolio_optimizer_module.read_json_if_exists

    def counting_reader(path):
        io_calls["count"] += 1
        return real_reader(path)

    pearson_calls = {"count": 0}
    real_pearson = portfolio_optimizer_module.pearson_corr

    def counting_pearson(first, second):
        pearson_calls["count"] += 1
        return real_pearson(first, second)

    monkeypatch.setattr(
        portfolio_optimizer_module, "read_json_if_exists", counting_reader
    )
    monkeypatch.setattr(portfolio_optimizer_module, "pearson_corr", counting_pearson)

    search = PortfolioSearch(candidates, spec)
    search.optimize()

    assert io_calls["count"] == 0
    max_pairs = len(candidates) * (len(candidates) - 1) // 2
    assert 0 < pearson_calls["count"] <= max_pairs


def test_optimizer_metrics_include_account_realistic_lot_floor(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "tiny-risk", "EURUSD", [1, 0.5, 1.5], score=70),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=1,
        min_fx_share=0,
        account={
            "account_size_usd": 100.0,
            "risk_per_trade_pct": 0.1,
            "min_lot": 0.01,
            "lot_step": 0.01,
            "notional_usd_per_lot": 100000.0,
            "leverage": 500.0,
            "stop_out_level_pct": 50.0,
            "margin_call_level_pct": 100.0,
        },
    )
    candidates, _ = build_optimizer_candidates(rows, spec)
    search = PortfolioSearch(candidates, spec)

    metrics = search.metrics(["tiny-risk"])
    account = metrics["account_initial"]

    assert account["starting_balance"] == 100.0
    assert account["final_balance"] == 115.0
    assert account["min_lot_forced_trades"] == 3
    assert account["min_lot_forced_trade_pct"] == 100.0
    assert account["max_actual_risk_pct"] == 10.0
    assert account["max_actual_risk_multiple"] == 100.0
    assert account["blown"] is False


def test_optimizer_account_current_basis_compounds_from_balance(tmp_path: Path) -> None:
    rows = [
        _row(tmp_path, "compound", "EURUSD", [1, 2], score=70),
    ]
    spec = PortfolioOptimizerSpec(
        portfolio_size=1,
        min_fx_share=0,
        account={
            "account_size_usd": 1000.0,
            "risk_per_trade_pct": 1.0,
            "min_lot": 0.0,
            "lot_step": 0.0001,
            "notional_usd_per_lot": 100000.0,
            "leverage": 500.0,
            "stop_out_level_pct": 50.0,
        },
    )
    candidates, _ = build_optimizer_candidates(rows, spec)
    search = PortfolioSearch(candidates, spec)

    metrics = search.metrics(["compound"])

    assert metrics["account_initial"]["final_balance"] == 1020.0
    assert metrics["account_current"]["final_balance"] == 1020.1
