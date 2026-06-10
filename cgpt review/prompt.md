# CGPT Review Packet

Please review the optimizer account-realism R1 packet.

## Start Here

- `cgpt review/optimizer-account-realism-r1/optimizer-account-realism-r1.md`
- `runs/derived/portfolio-optimization/codex-account-sim-darwin-r1/portfolio-optimization.md`
- `runs/derived/portfolio-optimization/codex-account-sim-darwin-r1/portfolio-variant-comparison.csv`
- `runs/derived/portfolio-optimization/codex-account-sim-coinexx-r1/portfolio-optimization.md`
- `runs/derived/portfolio-optimization/codex-account-sim-coinexx-r1/portfolio-variant-comparison.csv`

## What Changed

The portfolio optimizer now reports account-realistic simulation metrics in addition to R-calendar metrics:

```text
initial-risk and current-balance compounding
min-lot forced trade rate
max actual risk after lot rounding
margin pressure and liquidation flags
account final balance / return / USD drawdown
```

The account simulation is kept off the hot objective path and computed only for archived/report portfolios.

Validation:

```powershell
uv run --no-sync pytest tests\test_portfolio_optimizer.py tests\test_portfolio_risk_sizing.py
```

Result: `8 passed`.

## Main Read

Darwin:

```text
Current Darwin-only v4 still wins on final/account return.
New account-aware variant improves drawdown and weekly stability but does not clearly replace it.
No min-lot pressure at the configured Darwin scale.
```

Coinexx:

```text
New account-aware variant looks better than the current Coinexx basket.
Max DD roughly halves, negative months go to zero, peak open positions drop, and max forced risk drops from 6.48% to 4.00%.
Min-lot pressure remains unavoidable at $100, but basket selection can reduce the damage.
```

## Request

Please review whether the next optimizer iteration should be:

1. A true joint selection + per-strategy risk-grid optimizer.
2. A broader account-aware search with the current global-risk simulation.
3. A Play Hand-side change instead, such as account-aware candidate generation or broker-specific search lanes.

Specific questions are in `cgpt review/optimizer-account-realism-r1/optimizer-account-realism-r1.md`.

