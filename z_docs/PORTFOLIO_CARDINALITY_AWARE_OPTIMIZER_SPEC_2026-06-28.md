# Portfolio Cardinality-Aware Optimizer Spec

Date: 2026-06-28

## Problem

The current AutoResearch portfolio optimizer requires a fixed portfolio size, such as 20, 30, or 40 strategies. This is useful for controlled comparisons, but it can force the search away from the real portfolio optimum.

The true optimum may not be a round number. It might be 26, 33, or "20 high-conviction strategies plus 7 low-risk diversifiers." Raw strategy count is not itself the goal; it is a proxy for diversification, operational complexity, trade frequency, and margin pressure.

We need a first-class optimizer mode that lets portfolio size become part of the optimization problem while still discouraging bloated, overfit, or operationally noisy portfolios.

## Goal

Add a cardinality-aware portfolio optimization mode that can search across portfolio sizes and either:

1. Select the best portfolio from a bounded size sweep, or
2. Directly optimize portfolio utility with strategy count as a penalized variable.

The output should make the size tradeoff visible: where return, drawdown, calendar stability, open-position pressure, trade frequency, and correlation stop improving enough to justify more strategies.

## Non-Goals

- Do not remove the existing fixed-size optimizer path.
- Do not allow unbounded strategy counts.
- Do not relax trading-quality filters such as hold-time, asset eligibility, source calendar requirements, or score thresholds by default.
- Do not deploy to Fuzzfolio automatically.
- Do not treat raw final R as the only objective.

## CLI Proposal

Keep existing fixed-size behavior:

```powershell
uv run optimize-portfolio --portfolio-size 30 --objective deployable --optimizer-backend auto
```

Add cardinality sweep mode:

```powershell
uv run optimize-portfolio `
  --portfolio-size-min 12 `
  --portfolio-size-max 50 `
  --portfolio-size-step 2 `
  --objective deployable `
  --optimizer-backend auto `
  --output-dir runs/derived/portfolio-optimization/<name>
```

Add cardinality-penalty mode:

```powershell
uv run optimize-portfolio `
  --portfolio-size-min 12 `
  --portfolio-size-max 50 `
  --objective deployable `
  --cardinality-mode penalized `
  --strategy-count-penalty 1.0 `
  --optimizer-backend auto `
  --output-dir runs/derived/portfolio-optimization/<name>
```

Optional flags:

```text
--min-effective-strategy-count <float>
--max-effective-strategy-count <float>
--strategy-count-penalty <float>
--open-position-penalty <float>
--trade-frequency-penalty <float>
--elbow-min-delta-rdd <float>
--elbow-min-delta-neg-weeks <int>
```

## Objective Model

The optimizer should distinguish raw strategy count from effective diversification.

Suggested portfolio utility:

```text
utility =
  return_to_dd_score
  + calendar_stability_bonus
  + down_period_contribution_bonus
  + diversification_bonus
  - strategy_count_penalty
  - effective_duplicate_penalty
  - avg_open_position_penalty
  - peak_open_position_penalty
  - trade_frequency_penalty
  - worst_day_week_penalty
```

Do not count "more strategies" as good by itself. Count it as good only when the marginal strategy improves the portfolio after costs.

## Effective Strategy Count

Raw count is insufficient because 40 correlated strategies may behave like 15 strategies.

Add an `effective_strategy_count` metric derived from daily return correlation or sameness:

```text
effective_strategy_count ~= N / (1 + average_positive_correlation * (N - 1))
```

This does not need to be perfect; it just needs to expose when a larger portfolio is actually diverse versus merely bigger.

Also report:

- Raw selected count
- Effective selected count
- Average pair correlation
- Average positive pair correlation
- Max pair correlation
- Top family concentration
- Top instrument concentration
- Top asset-class concentration

## Search Modes

### Mode A: Size Sweep

Run the existing optimizer independently for multiple sizes.

Example:

```text
sizes = 12, 14, 16, ..., 50
```

For each size:

- Run existing optimizer with the same candidate pool and constraints.
- Record best variants for each objective.
- Produce a comparison table and elbow analysis.

Pros:

- Low implementation risk.
- Reuses current Rust/PyO3 selector.
- Easy to inspect.

Cons:

- More total optimizer invocations.
- Still picks a fixed size per run.

This is the recommended first implementation.

### Mode B: Add/Drop/Swap Search

Extend the selector to support variable-size moves:

- Add one candidate.
- Drop one selected strategy.
- Swap selected with unselected.
- Optionally add/drop pairs for local jumps.

Stop when no move improves penalized utility.

Pros:

- More direct cardinality optimization.
- Can naturally stop at non-round sizes.

Cons:

- More code complexity.
- Greater risk of local optima.
- Needs careful progress/debug output.

Recommended after Mode A validates the usefulness of cardinality optimization.

## Elbow Detection

For sweep mode, produce an "elbow" recommendation.

The elbow is the smallest portfolio size where adding more strategies no longer produces enough improvement after operational costs.

Possible rule:

```text
Prefer smaller size if next larger size improves:
  final R by < 5%
  and R/DD by < 5%
  and negative weeks by <= 1
  while increasing avg open positions or trades/month by > 10%
```

The report should show:

- Best raw return size
- Best R/DD size
- Best calendar-stability size
- Best deployable size
- Recommended elbow size
- Reason for recommendation

## Required Metrics

For every evaluated size and objective:

- Final R
- Max DD R
- Return/DD
- Positive months
- Negative months
- Positive weeks
- Negative weeks
- Worst week R
- Worst day R
- Max daily loss streak
- Top-day gain share
- Average open positions
- Peak open positions
- Trades/month
- Mean average hold
- Max average hold
- Max p90 hold
- Max single hold
- Average pair correlation
- Average positive pair correlation
- Max pair correlation
- Effective strategy count
- Raw strategy count
- Top instrument share
- Top family share
- Asset-class mix

## Output Artifacts

Write these under the optimizer output directory:

```text
portfolio-cardinality-summary.md
portfolio-cardinality-summary.json
portfolio-cardinality-comparison.csv
portfolio-size-elbow.csv
selected-recommended.csv
selected-best-return.csv
selected-best-stability.csv
selected-best-deployable.csv
build-portfolio-recommended.json
```

If `--export-bundle` or a follow-up `build-portfolio` step is requested, only export the recommended candidate and any explicit alternates.

## Progress Reporting

For long runs, emit JSONL progress:

```json
{
  "event": "cardinality_size_complete",
  "size": 30,
  "objective": "deployable",
  "elapsed_sec": 123.4,
  "final_r": 2178.49,
  "maxdd_r": 23.40,
  "return_to_dd": 93.10,
  "neg_weeks": 23,
  "avg_open_positions": 4.90,
  "effective_strategy_count": 24.7
}
```

Also emit a final:

```json
{
  "event": "cardinality_recommendation",
  "recommended_size": 30,
  "recommended_objective": "deployable",
  "reason": "Best stability-adjusted improvement before open-position pressure accelerates."
}
```

## Suggested First Experiment

Use the current post-catchup corpus:

```powershell
uv run optimize-portfolio `
  --optimizer-backend auto `
  --portfolio-size-min 12 `
  --portfolio-size-max 50 `
  --portfolio-size-step 2 `
  --objective deployable `
  --objective stability `
  --candidate-scope promoted `
  --min-score 45 `
  --max-avg-hold-hours 48 `
  --max-p90-hold-hours 144 `
  --max-single-hold-hours 336 `
  --allowed-asset-classes fx,metal,index `
  --output-dir runs/derived/portfolio-optimization/cardinality-sweep-YYYYMMDD
```

Compare against:

- Current deployed 20-strategy portfolio.
- Latest fixed-size 30 deployable candidate.
- Latest fixed-size 40 deployable candidate.

## Acceptance Criteria

- Existing fixed-size optimizer behavior remains unchanged.
- `--optimizer-backend auto` still records whether PyO3 or Python was used.
- Sweep mode completes and writes comparison artifacts.
- Report identifies recommended size and explains why.
- Effective strategy count is reported.
- Larger portfolios are not automatically preferred unless they improve stability-adjusted utility after complexity costs.
- The recommendation can choose the current deployed size, a larger size, or a smaller size.
- Tests cover:
  - Fixed-size backward compatibility.
  - Sweep range parsing.
  - Elbow recommendation on synthetic metrics.
  - Effective strategy count calculation.
  - JSON/CSV artifact creation.

## Implementation Notes

Start with Mode A. It is mostly orchestration and reporting over the existing selector, so it has a better risk/reward profile.

Mode B can be considered after Mode A produces useful evidence that the optimum often lands between our manually chosen fixed sizes.

The key design principle:

```text
Add a strategy only if its marginal contribution beats its complexity cost.
```

