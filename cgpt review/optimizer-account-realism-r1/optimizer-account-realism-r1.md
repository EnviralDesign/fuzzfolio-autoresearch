# Optimizer Account Realism R1

Generated: 2026-05-31

## What Changed

This pass makes the portfolio optimizer report account-realistic consequences instead of only portfolio-R outcomes.

Added optimizer metrics:

- Initial-risk account simulation.
- Current-balance compounding simulation.
- Min-lot forced trade rate.
- Max actual risk percent after broker lot rounding.
- Margin pressure / liquidation flags.
- Account final balance, return percent, and USD drawdown.

The objective path stays fast: account simulation is excluded from hot scoring and computed only for archived/report portfolios.

Validation:

```text
uv run --no-sync pytest tests\test_portfolio_optimizer.py tests\test_portfolio_risk_sizing.py
8 passed
```

## Fleet State

The AutoResearch Play Hand loops were stopped to free resources:

```text
play hand - det: Stopped
play hand - det 2: Stopped
play hand - evo: Stopped
play hand - evo 2: Stopped
play hand XAUUSD - evo: Stopped
```

## Darwinex Result

Run:

```text
runs/derived/portfolio-optimization/codex-account-sim-darwin-r1
```

Comparison:

| Portfolio | Final R | Max DD R | Neg Months | Neg Weeks | Worst Week R | Open Avg/Peak | Trades/Mo | Initial Account |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| codex-optimizer-v4-darwin-only-deployable | 1654.04 | 29.23 | 0 | 30 | -16.39 | 3.7 / 23 | 150.4 | $243,547 / +143.5% |
| codex-stability-public-dxz-v4-holdtime-balanced | 1155.40 | 40.20 | 2 | 46 | -19.35 | 3.5 / 23 | 104.2 | $200,239 / +100.2% |
| new deployable variant | 1590.66 | 24.00 | 0 | 29 | -15.64 | 4.2 / 23 | 158.9 | $236,369 / +136.4% |

Read:

- The current Darwin-only optimized basket still leads on raw and account return.
- The new variant lowers drawdown and slightly improves weekly stability, but gives up too much final return to replace the Darwin basket immediately.
- Darwin has no min-lot forcing problem at the configured $100k / 0.1% risk scale.

## Coinexx Result

Run:

```text
runs/derived/portfolio-optimization/codex-account-sim-coinexx-r1
```

Comparison:

| Portfolio | Final R | Max DD R | Neg Months | Neg Weeks | Worst Week R | Open Avg/Peak | Trades/Mo | Initial Account | Min-Lot Forced | Max Actual Risk |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| codex-stability-coinexx-v4-holdtime-balanced | 1155.40 | 40.20 | 2 | 46 | -19.35 | 3.5 / 23 | 104.2 | $3,537 / +3437.1% | 100.0% | 6.48% |
| new deployable variant | 1191.31 | 20.34 | 0 | 40 | -12.31 | 1.8 / 14 | 56.6 | $4,045 / +3944.6% | 100.0% | 4.00% |

Read:

- Coinexx improves materially with the account-aware basket.
- The main win is not just higher return: max DD drops almost in half, negative months go to zero, peak open positions drop from 23 to 14, and max actual forced risk falls from 6.48% to 4.00%.
- The min-lot constraint remains unavoidable at $100 with 0.01 lot minimum; every closed trade is forced above target risk. The optimizer can still reduce the damage by preferring lower concurrency and lower stop-distance pressure.

## Current Assessment

The account-realistic optimizer metrics are useful and exposed information the R-only optimizer could not show.

Immediate conclusions:

1. Keep the current Darwin-only v4 basket as the main public trust signal unless a broader search beats it on both return and stability.
2. Treat the Coinexx basket as a separate small-account problem; min-lot pressure changes the optimal basket.
3. Do not merge Darwin and Coinexx optimization targets too aggressively. Shared strategies can exist, but the selection pressure is different.
4. The next high-value improvement is a true joint selection + risk-sizing loop, not only post-hoc per-strategy risk sizing.

## Recommended Next Iteration

Add a bounded risk-aware optimizer mode:

```text
selection step:
  choose candidate set from R-calendar stability, hold hygiene, diversity, and account constraints

risk step:
  assign per-strategy risk from a limited grid, e.g. 0.125 / 0.25 / 0.5 / 0.75 / 1.0 / 1.5

account evaluation:
  simulate account curve with per-strategy risk, min-lot rounding, margin, and compounding

search move:
  accept candidate swaps only if portfolio and account metrics improve on the selected objective
```

This would let the optimizer answer questions the current workflow still handles manually:

- Which strategies deserve higher/lower risk?
- Does a high-return but choppy strategy still help after realistic sizing?
- Can small-account min-lot pressure be reduced through lower stop-distance strategies?
- Which portfolio is best for Darwin public signal versus Coinexx small-account growth?

## Questions For Pro

1. For Darwin, should the next run optimize primarily for `final_r` with stability constraints, or for account return / max USD drawdown directly?
2. For Coinexx, should min-lot forced risk be treated as a hard constraint, or just a penalty since it is unavoidable at $100?
3. Should the next optimizer mode use a small discrete risk grid per strategy, or start with portfolio-level risk tiers only?
4. Should Darwin and Coinexx remain separate optimizer profiles from here forward?
5. Is the current bounded search enough for this iteration, or should we run a broader account-aware search after adding the risk grid?

