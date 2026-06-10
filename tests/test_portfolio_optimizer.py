import json
from pathlib import Path

from autoresearch.portfolio_optimizer import (
    PortfolioOptimizerSpec,
    PortfolioSearch,
    build_optimizer_candidates,
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
