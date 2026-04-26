# Backtest Honesty and Gold Exploration Notes - 2026-04-23

## Scope

User prompt:

- Build toward a gold strategy with lowest entry timeframe M1 or M5.
- Target 15-60 trades/month, preferably more only if quality stays high.
- Prefer common exit ranges around 1:1, 1:2, 1:3, or 1:4.
- Buy-only is acceptable; bidirectional is also acceptable.

Secondary goal:

- Act as an explorer and note where the Fuzzfolio / autoresearch system may overstate strategy quality, especially profile-drop backtests.
- Focus on tight stop-loss / huge take-profit selection and repeated consecutive-bar entries.

This file is intentionally notes-only. No code changes were made for these findings.

## High-Level Takeaways

1. I did not find evidence that deep replay is obviously "cheating" on intrabar TP/SL ambiguity. The Python fallback resolves bars where TP and SL are both touched as a loss.
2. I did find a structural reason the leaderboard likes tiny stops and high reward multiples: the default deep-replay matrix is `0.02..0.50%` stop loss and `0.5..12.5R`, and best-cell selection maximizes expectancy per resolved trade before quality scoring.
3. The replay engine resolves every qualifying signal independently. Consecutive or overlapping signals are counted as separate trades, with no same-profile cooldown, no "already in position" constraint, and no "do not add while underwater" constraint.
4. The system already computes useful behavior diagnostics like `max_consecutive_signal_run`, `signal_coverage_ratio`, `bars_per_signal`, and open-trade counts in detail curves, but the quality score does not appear to penalize bursty or concurrent execution shape directly.
5. Profile-drop scoring rewards cadence up to 60 trades/month, but it does not penalize cadence above 60. High-cadence profiles can keep excellent scores if path metrics remain strong.
6. Profile-drop cards can therefore be truthful about the current replay model while still misleading for live deployment if live EA execution has different concurrency/cooldown behavior or account exposure limits.

## Corpus Evidence

Source: `C:\repos\fuzzfolio-autoresearch\runs\derived\attempt-catalog.csv` plus each row's `full-backtest-36mo-result.json`.

Current catalog summary:

- 11,510 attempts
- 10,647 scored attempts
- 10,565 attempts with valid 36-month full backtests according to `full-backtest-audit.json`
- 630 unique 36-month strategy keys

Ranked-slice stats from 36-month full backtests:

| Slice | Count | Stop <= 0.06 and reward >= 5 | Median stop | Median reward | Median trades/month | Median max consecutive signal run |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Top 100 | 100 | 75% | 0.04% | 12.5R | 12.66 | 6 |
| Top 500 | 500 | 64.6% | 0.06% | 12.5R | 19.81 | 7 |
| Top 1000 | 1000 | 56.8% | 0.06% | 12.0R | 25.41 | 7 |
| Gold rows containing XAU | 317 | 19.9% | 0.18% | 12.5R | 67.80 | 11 |

Notable outlier:

- Current rank 1 36-month full backtest is `M5|XRPUSD`, score `99.1068`, stop `0.02%`, reward `9.0R`, `4238.69` trades/month, `max_consecutive_signal_run = 152381`, `signal_density = 0.9999`, median holding hours `0.0`.
- That is a very strong example of the system ranking an execution shape that is probably useless live, even if the replay math is internally consistent.

Gold-specific observations:

- Highest-scoring XAU-containing strategies are mostly H4 baskets, not the requested M1/M5 single-gold style.
- Best single-symbol `M5|XAUUSD` row found in the corpus:
  - Candidate: `xau_usd_macd_stochrsi_m15`
  - Profile: `69e041425768c7d693a3`
  - Score 36m: `90.5143`
  - Trades/month: `97.217`
  - Best cell: `0.24%` stop, `12.5R`, `3.0%` take profit
  - `max_consecutive_signal_run = 3`
  - This is interesting structurally, but it misses both the requested trade-rate band and the preferred reward range.
- I found no existing full-backtested `M1`/`M5` XAUUSD corpus candidate with `15..60` trades/month and best-cell `reward_multiple <= 4`.

## Code-Level Findings

### Replay signal semantics

Source: `C:\repos\Trading-Dashboard\shared\python\fuzzfolio_core\fuzzfolio_core\compute\signal_derivation.py`

- `derive_replay_style_signals` emits a signal when a closed bar's long or short score exceeds threshold.
- Entry is on the next bar open: `entry_index = signal_index + 1`.
- Long and short signal checks are independent.
- There is no replay-side cooldown, deduplication by active position, or "one open trade per profile" gate.
- Live alert helper `should_emit_for_bar_watermark` emits at most once per side per signal bar, but that only prevents duplicate emissions for the same bar. It does not prevent a new signal on the next bar.

Implication:

- The user's observed live cascade is not surprising. The replay model will score those cascades as independent trades too.
- If the EA or broker account is intended to avoid stacking entries from the same strategy, current backtests do not model that.

### Trade outcome semantics

Source: `C:\repos\Trading-Dashboard\shared\python\fuzzfolio_core\fuzzfolio_core\compute\deep_replay.py`

- `_resolve_cell_trade_outcomes_python` iterates every signal independently and appends one resolved trade outcome per signal.
- `_resolve_signal_outcome_detail` scans future bars from the entry index until TP or SL is hit.
- If both TP and SL are hit inside the same bar, it returns `"loss"`.

Implication:

- Conservative same-bar ambiguity is good.
- The bigger issue is independent overlapping trade resolution, not TP/SL ordering.

### Default matrix shape

Source: `C:\repos\Trading-Dashboard\shared\python\fuzzfolio_core\fuzzfolio_core\models\deep_replay.py`

Default `DeepReplayMatrixConfig`:

- `sl_step_percent = 0.02`
- `sl_rows = 25`
- `reward_step_r = 0.5`
- `reward_columns = 25`

Because missing starts default to the step value, the default grid is:

- Stop loss: `0.02%, 0.04%, ... 0.50%`
- Reward multiple: `0.5R, 1.0R, ... 12.5R`

Implication:

- The optimizer is routinely invited to pick tiny stops and up to `12.5R`.
- A user preferring 1:1..1:4 should not trust a default profile-drop cell unless the matrix was explicitly constrained.

### Best-cell and score selection

Source: `C:\repos\Trading-Dashboard\shared\python\fuzzfolio_core\fuzzfolio_core\compute\deep_replay.py` and `quality_score.py`

- Best-cell selection considers cells with at least 3 resolved trades and prefers higher `avg_net_r_per_closed_trade`, then more resolved trades on a tie.
- Quality score includes expectancy, profit factor, trade support, path quality, and matrix robustness.
- The profile-drop cadence component has a healthy zone from 10 to 60 trades/month, but saturates at 1.0 at or above 60. It is not an overtrading penalty.
- The score does not appear to include direct penalties for:
  - max consecutive signal run
  - signal coverage/density above an execution-safe range
  - max concurrent open trades
  - same-profile overlapping positions
  - very short median holding time / immediate churn
  - reward multiple above a user/deployment policy cap

Implication:

- A profile can be "high quality" under current math while being unsuitable for live EA execution.
- The card should probably show execution-shape warnings, and the score should likely demote severe burst/concurrency cases.

## Direct Gold Exploration

CLI used:

`C:\repos\Trading-Dashboard\harness\fuzzfolio_agent\cli\target\release\fuzzfolio-agent-cli.exe --base-url http://localhost:7946/api/dev --auth-profile robot`

Environment note:

- `uv run fuzzfolio-agent-cli` was not available from `C:\repos\Trading-Dashboard`; direct exe path worked.
- `indicators --mode ... --pretty` is rejected; some CLI commands support `--pretty`, but this one does not.

### Probe 1: Fresh M5 MACD cross + RSI mean reversion

Profile created:

- Local file: `C:\repos\Trading-Dashboard\evals\codex-xau-m5-macd-rsi-profile.json`
- Cloud profile: `69ea74cc0a6da783c68e`
- Indicators: `MACD_CROSSOVER`, `RSI_MEAN_REVERSION`
- Instrument: `XAUUSD`
- Timeframe: `M5`

12-month long-only, constrained 1R..4R matrix:

- Artifact: `C:\repos\Trading-Dashboard\evals\codex-xau-m5-macd-rsi-12m-long-r1to4`
- Best cell: `0.2%` stop, `2.0R`, `0.4%` take profit
- Trades: 10 over 11.92 months, `0.84/month`
- Quality score: `3.3181`
- `max_consecutive_signal_run = 1`

Assessment:

- Good event-like behavior, but far too sparse and weak.

### Probe 2: Existing XAUUSD M5 MACD cross + StochRSI profile, constrained exits

Profile:

- Cloud profile: `69e041425768c7d693a3`
- Exported local file: `C:\repos\Trading-Dashboard\evals\codex-xau-existing-macd-stochrsi-profile.json`
- Indicators: `MACD_CROSSOVER`, `STOCHRSI_TREND`
- Instrument: `XAUUSD`
- Timeframe: `M5`

12-month both-direction, constrained 1R..4R matrix:

- Artifact: `C:\repos\Trading-Dashboard\evals\codex-xau-m5-existing-macd-stochrsi-12m-r1to4`
- Best cell: `0.5%` stop, `4.0R`, `2.0%` take profit
- Trades/month: `166.78`
- Quality score: `78.0153`
- `max_consecutive_signal_run = 3`
- `time_under_water_ratio = 0.2094`

12-month long-only, constrained 1R..4R matrix, alert threshold 90:

- Artifact: `C:\repos\Trading-Dashboard\evals\codex-xau-m5-existing-macd-stochrsi-12m-r1to4-nt90-long`
- Best cell: `0.8%` stop, `4.0R`, `3.2%` take profit
- Trades/month: `82.05`
- Quality score: `62.6208`
- `max_consecutive_signal_run = 1`
- Max drawdown: `13.4428R`

12-month long-only, constrained 1R..4R matrix, alert threshold 99:

- Artifact: `C:\repos\Trading-Dashboard\evals\codex-xau-m5-existing-macd-stochrsi-12m-r1to4-nt99-long`
- Best cell: `0.8%` stop, `4.0R`, `3.2%` take profit
- Trades/month: `79.70`
- Quality score: `62.8441`
- `max_consecutive_signal_run = 1`
- Max drawdown: `12.7563R`

Assessment:

- This is the best live-shaped lead I found in this short pass because it uses an event-like MACD cross and has no consecutive-bar cascade in long-only mode.
- It still overtrades the requested band and has large R drawdown. It is not a final deployable answer.
- Raising alert threshold did almost nothing for cadence, suggesting the profile's qualifying events are already near-max score when they occur.

## Candidate Direction From This Pass

Best lead family:

- XAUUSD M5 MACD crossover plus a trend/oscillator confirmation.
- Force long-only first.
- Constrain matrix to `1R..4R`.
- Tune the signal condition itself, not just `notificationThreshold`, because threshold did not materially reduce signal count.

Next search moves:

1. Try slower MACD cross parameters or MA crossover parameters to reduce event frequency.
2. Add a regime filter that does not itself persist as a repeated entry trigger, such as higher-timeframe trend confirmation.
3. Keep `lookbackBars = 1` on event indicators.
4. Use `direction-mode long` for gold first.
5. Explicitly run the matrix as `reward_step_r=1 reward_columns=4`; otherwise default profile-drop will drift back toward `12.5R`.

## Recommended System Improvements To Consider

### P0/P1: Make backtest assumptions visible

Profile-drop cards should show:

- Best-cell stop, reward multiple, and take-profit percent
- Trades/month
- `max_consecutive_signal_run`
- signal coverage ratio or bars per signal
- max concurrent open trades from the path/detail curve when available
- whether replay permits overlapping same-profile positions

If the replay allows overlapping trades, the card should say so.

### P1: Add execution-shape penalties

Quality scoring should consider penalizing:

- `max_consecutive_signal_run` above a small threshold
- signal density above a deployable range
- high max open trade count
- trades/month above the target range, not just below it
- median holding time near zero for non-scalping profiles

This can encourage exploration away from persistent oscillator-only profiles without hard-coding brittle EA rules.

### P1: Add an alternate replay policy

Add a deep-replay option for same-profile position policy:

- `independent_signals` current behavior
- `one_open_per_profile`
- `one_open_per_direction`
- `cooldown_bars=N`
- `skip_if_existing_trade_underwater`

Then profile drops can compare "raw signal edge" versus "deployable execution edge".

### P1: Make matrix policy explicit in research prompts and cards

Default matrix currently searches to `12.5R`.

For this user's prompt, the matrix should be explicitly constrained to:

- `reward_step_r = 1`
- `reward_columns = 4`
- stop-loss rows chosen for the instrument/timeframe

The current default search space is a major reason high-R winners dominate.

### P2: Add corpus views for these diagnostics

Useful derived charts:

- score vs reward multiple
- score vs stop-loss percent
- score vs max consecutive signal run
- score vs trades/month with overtrading highlighted
- score vs max open trades
- profile-drop shortlist filtered to 1R..4R cells

## Bottom Line

The system is not necessarily lying in the narrow sense of fabricating profitable paths. It is more likely telling the truth about a permissive replay model that is missing deployment constraints and then ranking those results as if they were deployment-ready.

The two suspicious patterns the user raised are real:

- Top performers are heavily biased toward tiny stops and large reward multiples.
- Consecutive-bar and overlapping entry behavior is modeled as independent trades, not constrained live execution.

For gold M5, I found a promising family but not a final strategy that meets all constraints. The nearest lead is `69e041425768c7d693a3` under a 1R..4R constrained matrix and long-only replay, but it still runs about `80/month` and carries large R drawdown.

## Revision: Indicator Substrate Is Probably The Bigger Root Cause

After reviewing the indicator catalog and implementation pattern more closely, the strongest hypothesis is no longer "the grid allows 12.5R, so winners drift there." That is a symptom-level explanation. The deeper issue is that the current catalog is heavily weighted toward state/context indicators and very light on true entry triggers.

Current catalog shape:

- `53` actual indicator JSON definitions, excluding `timeframes.json`.
- `49/53` use range-based continuous scoring.
- Only `4/53` are non-range indicators: `MA_CROSSOVER`, `MACD_CROSSOVER`, `STOCH_CROSSOVER`, and `CANDLESTICK_PATTERNS`.
- Of those, the clean trigger set is really only `3`: `MA_CROSSOVER`, `MACD_CROSSOVER`, and `STOCH_CROSSOVER`.
- `CANDLESTICK_PATTERNS` is technically event-like, but the default `patterns` value is `[]`, so unless profile creation populates it, it emits neutral scores. Its implementation also depends on TA-Lib's broad candlestick catalog rather than simpler price-action shapes.

Behavioral split:

- State/context indicators: RSI, CCI, CMO, MOM, Stoch, StochF, StochRSI, ULTOSC, WILLR, ADX, DI, MA distance, MA slope, MA spread, SAR, ATR, Bollinger/Keltner/Donchian position, volume/flow indicators.
- Trigger indicators: MA cross, MACD cross, Stoch cross.
- Semi-trigger but actually stateful: Donchian/Keltner breakout variants. They score position outside a channel range, not only the first break bar, so they can remain true across consecutive bars.
- Weak/bug-prone trigger: candlestick scanner, due to empty default pattern selection and generic TA-Lib pattern semantics.

This changes the interpretation of the high-R skew:

- A 1R/2R strategy requires precise entry timing.
- A 12R strategy can survive with weak timing if the state stack identifies market conditions that occasionally precede a tail move.
- Random combinations of state indicators tend to say "conditions look favorable" rather than "enter now."
- If the entry timing is vague, the matrix naturally discovers tiny-stop/high-reward cells because that payoff geometry can monetize occasional tail outcomes without needing high hit rate.

Explorer sampling reinforces this:

- The CLI prompt seed path samples `5..10` indicator IDs uniformly from the whole catalog.
- With only `3` clean trigger indicators out of `53`, a random 5-indicator seed has only about a `26%` chance of containing even one clean trigger; a 10-indicator seed is still only about `47%`.
- Even when a trigger is present, it is just one equal-weight component inside a larger state stack. The scoring engine does not currently distinguish "fresh trigger fired" from "persistent state remains true."

The better fix is not to cap R. The better fix is to add and promote better entry-trigger primitives, then make the explorer intentionally compose profiles as:

1. Regime/context filter, often higher timeframe.
2. Setup/stretch condition, often mid timeframe.
3. Entry trigger, usually lowest timeframe.

Candidate additions that would directly rebalance the substrate:

- `RSI_CROSSBACK`: event when RSI crosses back above an oversold threshold for long, or below overbought for short. This is much more entry-like than "RSI is between 20 and 40."
- `STOCHRSI_CROSSBACK`: event when StochRSI leaves the extreme zone, useful for gold reversal timing.
- `PRICE_RECLAIM_MA`: event when price closes back above/below a moving average after being stretched on the other side.
- `WICK_REJECTION`: explicit candle-shape trigger for long lower wick / short upper wick relative to candle body and recent range. This addresses the weakness of generic TA-Lib candlestick patterns.
- `ENGULF_OR_PINBAR_SIMPLE`: hand-rolled simple price-action trigger, not the full TA-Lib candlestick zoo.
- `SWING_PIVOT_REVERSAL`: event after a local low/high forms and price confirms by breaking the prior candle high/low.
- `CHANNEL_REENTRY`: after closing outside a Bollinger/Keltner/Donchian band, trigger only when price closes back inside. This converts persistent band-extreme state into a reversal entry event.
- `BREAKOUT_FIRST_CLOSE`: one-bar event for the first close outside a prior channel, separate from the existing stateful channel-position breakout.
- `PULLBACK_RESUME`: in an uptrend/downtrend, trigger when price pulls back to MA/VWAP/channel midline and then resumes by closing back through a short trigger level.

Scoring/explorer implications:

- Temporal breadth and tail dependency already exist, so the immediate gap is not just another penalty.
- The exploration fabric should know indicator roles: `context`, `setup`, `trigger`, `filter`.
- Candidate generation should require or strongly prefer at least one trigger-role indicator on the lowest entry timeframe.
- Existing state indicators should remain available; they are useful, but they should mostly frame context/setup rather than carry entry timing by themselves.
- The secondary score should classify high-R winners as `tail-capture` versus `calibrated-entry` rather than treating high R as automatically bad.
