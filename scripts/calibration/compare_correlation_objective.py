"""Compare legacy vs correlation-aware portfolio optimization on the real corpus.

Builds the optimizer candidate pool from runs/derived/attempt-catalog.csv
canonical rows with a valid 36mo full backtest and an existing calendar curve,
then runs the portfolio optimizer twice with identical search settings:

1. legacy: correlation_penalty_weight=0, portfolio_sharpe_weight=0
2. corr-aware: penalty + marginal Sharpe weights enabled

and prints selected members, avg pair correlation, total R, max DD,
positive-month rate, and portfolio daily Sharpe for both runs.

Usage:
    uv run python scripts/calibration/compare_correlation_objective.py
        [--objective balanced] [--portfolio-size 20]
        [--correlation-penalty-weight 1500] [--portfolio-sharpe-weight 400]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from autoresearch.portfolio_optimizer import (  # noqa: E402
    PortfolioOptimizerSpec,
    PortfolioSearch,
    build_optimizer_candidates,
)

CATALOG_PATH = REPO_ROOT / "runs" / "derived" / "attempt-catalog.csv"


def load_canonical_rows() -> list[dict]:
    rows: list[dict] = []
    with CATALOG_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("is_canonical_attempt") or "").strip().lower() not in {
                "true",
                "1",
            }:
                continue
            if str(row.get("full_backtest_validation_status_36m") or "") != "valid":
                continue
            if not str(row.get("full_backtest_calendar_curve_path_36m") or "").strip():
                continue
            # The CSV stores list columns as JSON text; the optimizer expects lists.
            for key in ("instruments_36m", "instruments"):
                raw = str(row.get(key) or "").strip()
                if raw.startswith("["):
                    try:
                        row[key] = json.loads(raw)
                    except json.JSONDecodeError:
                        pass
            rows.append(row)
    return rows


def summarize(label: str, search: PortfolioSearch, variant: dict) -> dict:
    selected_ids = list(variant.get("selected_attempt_ids") or [])
    metrics = variant.get("metrics") or {}
    month_count = int(metrics.get("month_count") or 0)
    pos_months = int(metrics.get("pos_months") or 0)
    summary = {
        "label": label,
        "selected": selected_ids,
        "objective_score": float(variant.get("objective_score") or 0.0),
        "final_r": float(metrics.get("final_r") or 0.0),
        "maxdd_r": float(metrics.get("maxdd_r") or 0.0),
        "avg_pair_corr": float(metrics.get("avg_pair_corr") or 0.0),
        "max_pair_corr": float(metrics.get("max_pair_corr") or 0.0),
        "avg_positive_pair_corr": float(metrics.get("avg_positive_pair_corr") or 0.0),
        "portfolio_sharpe": float(metrics.get("portfolio_sharpe") or 0.0),
        "positive_month_rate": (pos_months / month_count) if month_count else 0.0,
        "constraint_violations": metrics.get("constraint_violations") or {},
        "diversification": variant.get("diversification") or {},
    }
    return summary


def print_summary(summary: dict) -> None:
    print(f"\n=== {summary['label']} ===")
    print(f"objective_score:        {summary['objective_score']:.3f}")
    print(f"final_r:                {summary['final_r']:.2f}")
    print(f"maxdd_r:                {summary['maxdd_r']:.2f}")
    print(f"avg_pair_corr:          {summary['avg_pair_corr']:.4f}")
    print(f"max_pair_corr:          {summary['max_pair_corr']:.4f}")
    print(f"avg_positive_pair_corr: {summary['avg_positive_pair_corr']:.4f}")
    print(f"portfolio_sharpe:       {summary['portfolio_sharpe']:.4f}")
    print(f"positive_month_rate:    {summary['positive_month_rate']:.2%}")
    print(f"constraint_violations:  {summary['constraint_violations']}")
    diversification = summary["diversification"]
    if diversification:
        print(
            "diversification terms:  "
            f"penalty={diversification.get('correlation_penalty'):.4f} "
            f"sharpe_term={diversification.get('portfolio_sharpe_term'):.4f}"
        )
    print("selected members:")
    for attempt_id in summary["selected"]:
        print(f"  - {attempt_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objective", default="balanced")
    parser.add_argument("--portfolio-size", type=int, default=20)
    parser.add_argument("--candidate-limit", type=int, default=120)
    parser.add_argument("--swap-candidate-limit", type=int, default=80)
    parser.add_argument("--max-swaps", type=int, default=8)
    parser.add_argument("--random-starts", type=int, default=1)
    parser.add_argument("--correlation-penalty-weight", type=float, default=1500.0)
    parser.add_argument("--portfolio-sharpe-weight", type=float, default=400.0)
    args = parser.parse_args()

    if not CATALOG_PATH.exists():
        print(f"attempt catalog not found: {CATALOG_PATH}")
        return 1
    rows = load_canonical_rows()
    print(f"canonical catalog rows with valid 36mo backtest + curve path: {len(rows)}")

    legacy_spec = PortfolioOptimizerSpec(
        portfolio_name="corr-objective-compare-legacy",
        portfolio_size=args.portfolio_size,
        candidate_limit=args.candidate_limit,
        swap_candidate_limit=args.swap_candidate_limit,
        objective_names=(args.objective,),
        max_swaps=args.max_swaps,
        random_starts=args.random_starts,
    )
    corr_spec = replace(
        legacy_spec,
        portfolio_name="corr-objective-compare-aware",
        correlation_penalty_weight=args.correlation_penalty_weight,
        diversification_mode="marginal_sharpe",
        portfolio_sharpe_weight=args.portfolio_sharpe_weight,
    )

    started = time.time()
    candidates, rejections = build_optimizer_candidates(rows, legacy_spec)
    print(
        f"candidate pool: {len(candidates)} retained "
        f"(rejections: {rejections}) in {time.time() - started:.1f}s"
    )
    if not candidates:
        print("no candidates survived the optimizer filters")
        return 1

    summaries = []
    for label, spec in (("legacy", legacy_spec), ("corr-aware", corr_spec)):
        run_started = time.time()
        search = PortfolioSearch(candidates, spec)
        variants = search.optimize()
        variant = variants.get(args.objective)
        if variant is None:
            print(f"objective {args.objective!r} produced no variant")
            return 1
        summary = summarize(label, search, variant)
        summary["runtime_seconds"] = time.time() - run_started
        summaries.append(summary)
        print_summary(summary)
        print(f"runtime: {summary['runtime_seconds']:.1f}s")

    legacy, aware = summaries
    overlap = set(legacy["selected"]) & set(aware["selected"])
    print("\n=== comparison ===")
    print(f"member overlap: {len(overlap)}/{len(legacy['selected'])}")
    print(
        "avg_pair_corr:    "
        f"legacy={legacy['avg_pair_corr']:.4f} aware={aware['avg_pair_corr']:.4f}"
    )
    print(
        "portfolio_sharpe: "
        f"legacy={legacy['portfolio_sharpe']:.4f} aware={aware['portfolio_sharpe']:.4f}"
    )
    print(
        "final_r:          "
        f"legacy={legacy['final_r']:.2f} aware={aware['final_r']:.2f}"
    )
    print(
        "maxdd_r:          "
        f"legacy={legacy['maxdd_r']:.2f} aware={aware['maxdd_r']:.2f}"
    )
    print(
        "pos_month_rate:   "
        f"legacy={legacy['positive_month_rate']:.2%} aware={aware['positive_month_rate']:.2%}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
