# Indicator Trigger Balance Tasks

Date: 2026-04-23

## Goal

Rebalance Fuzzfolio's indicator substrate so exploration can discover strategies with more precise entries and more natural risk/reward geometry.

The current catalog is heavily weighted toward state/context indicators. That is useful for regime and setup detection, but it leaves the explorer short on true "enter now" primitives. This likely contributes to high-R, low-win-rate strategies because vague state stacks can identify conditions that sometimes precede tail moves, while failing to time normal `1R..4R` entries cleanly.

## Implementation Handoff

When an agent starts implementing any indicator from this document, it should use the `fuzzfolio-indicator-implementer` skill:

`C:\Users\envir\.codex\skills\private\fuzzfolio-indicator-implementer\SKILL.md`

That skill is the operating procedure for indicator work in `C:\repos\Trading-Dashboard`. In particular, it requires:

- starting inside `C:\repos\Trading-Dashboard`
- running a clean-worktree safety check before edits or Airtable changes
- reading the repo implementation guide first:
  - `C:\repos\Trading-Dashboard\z_docs\scoring\indicator-implementation-guide-v2.md`
  - `C:\repos\Trading-Dashboard\z_docs\scoring\implementation-tracker.md`
- using the Rust-first path for reusable/classical indicator primitives
- verifying metadata discovery, backend profile-save validation, and indicator-specific tests
- using Airtable lifecycle updates only when the clean-worktree gate passes

This document is the product/strategy task list. The skill and Trading-Dashboard repo guides are the implementation source of truth.

The target strategy shape should become:

1. Higher/mid-timeframe context or regime filter.
2. Setup/stretch condition.
3. Lower-timeframe trigger indicator.

## Current Catalog Diagnosis

Current Trading-Dashboard catalog check: `59` indicator definitions, excluding `timeframes.json`.

Role metadata is now present on the catalog:

- `18` context indicators
- `29` setup indicators
- `2` filter indicators
- `10` trigger indicators

Current trigger catalog:

- `BREAKOUT_FIRST_CLOSE`
- `CHANNEL_REENTRY`
- `MA_CROSSOVER`
- `MACD_CROSSOVER`
- `PRICE_RECLAIM_MA`
- `RSI_CROSSBACK`
- `STOCH_CROSSOVER`
- `STOCHRSI_CROSSBACK`
- `WICK_REJECTION`
- `CANDLESTICK_PATTERNS`

This means the immediate problem is no longer only "build trigger indicators." The more urgent task is to verify that autoresearch actually recognizes and preferentially uses these trigger-role indicators during seed selection, mutation, and scoring explanation.

## Smoke Finding: Catalog/Runtime Mismatch

A controlled `XAUUSD` smoke run on 2026-04-23 used the existing trigger-backed seed:

- `ADX`
- `BBANDS_POSITION_MEAN_REVERSION`
- `RSI_CROSSBACK`
- `CHANNEL_REENTRY`

Profile scaffold and validation passed, but deep replay returned no scored candidate. The replay artifact included:

`Scoring analysis could not be completed: Unknown indicator type: RSI_CROSSBACK`

Local Trading-Dashboard source contains constants, tests, and factory registrations for the new trigger indicators, but the active scoring runtime reached by `fuzzfolio-agent-cli` did not recognize at least `RSI_CROSSBACK`.

Treat this as the first implementation blocker:

- Verify the active dev/backend scoring runtime is running the same code that contains `RSI_CROSSBACK`.
- Verify packaged/shared `fuzzfolio_core` used by the backend includes `trigger_indicators.py` and `indicator_factory.py` registrations.
- Re-run a one-indicator replay for each trigger ID before judging strategy quality:
  - `RSI_CROSSBACK`
  - `STOCHRSI_CROSSBACK`
  - `PRICE_RECLAIM_MA`
  - `CHANNEL_REENTRY`
  - `BREAKOUT_FIRST_CLOSE`
  - `WICK_REJECTION`
- Do not conclude "trigger-backed gold is too sparse" until these indicators are accepted by the runtime.

`CANDLESTICK_PATTERNS` remains event-like in concept, but weak as a practical trigger:

  - default `patterns` is `[]`
  - depends on broad TA-Lib pattern names
  - does not capture simple custom wick/rejection shapes well
- Older Donchian/Keltner breakout variants are stateful channel-position scores, not first-event triggers; use `BREAKOUT_FIRST_CLOSE` when the intent is a fresh breakout event.
- If the explorer seed path samples `5..10` indicators uniformly from the whole catalog, many random seeds will still be all setup/context even though trigger indicators now exist.

## Priority 1: Verify And Wire Existing Trigger Indicators

These are already present in `C:\repos\Trading-Dashboard\shared\constants\indicators` and should be exploited before adding more primitives.

### Existing reused-primitive triggers

- `RSI_CROSSBACK`
- `STOCHRSI_CROSSBACK`
- `PRICE_RECLAIM_MA`
- `CHANNEL_REENTRY`
- `BREAKOUT_FIRST_CLOSE`

Verification tasks:

- Confirm each indicator has working backend implementation, metadata discovery, profile-save validation, and replay output.
- Confirm default `lookbackBars: 1` behavior is truly event-like and does not stay true across stale state.
- Confirm the autoresearch controller treats these IDs as `trigger`, not setup.
- Confirm profile generation can intentionally choose one of these on the lowest entry timeframe.
- Run controlled comparisons against state-only siblings:
  - `RSI_MEAN_REVERSION` + `RSI_CROSSBACK`
  - `STOCHRSI_MEAN_REVERSION` + `STOCHRSI_CROSSBACK`
  - `BBANDS_POSITION_MEAN_REVERSION` + `CHANNEL_REENTRY`
  - Donchian/Keltner state + `BREAKOUT_FIRST_CLOSE`

Why this matters:

- These convert "condition is true" into "entry event happened."
- This directly targets repeated-bar entry cadence without applying a blunt trade cap.
- These should give the system a path toward viable `1R..4R` cells because entries can be timed more narrowly.

### Existing price-action trigger

- `WICK_REJECTION`

Verification tasks:

- Confirm it detects the simple long-wick rejection shape that `CANDLESTICK_PATTERNS` misses.
- Confirm it is usable on `M1` and `M5` without exploding false positives.
- Confirm it can pair with oscillator stretch and channel excursion setups.

## Priority 2: Add Remaining Custom Price-Action Triggers

These fill gaps left by generic TA-Lib candlestick patterns and the first `WICK_REJECTION` primitive.

### `SIMPLE_PINBAR_OR_ENGULF`

Purpose: provide a small, auditable price-action trigger set.

Patterns:

- pinbar/rejection candle
- bullish/bearish engulfing by body
- outside bar with directional close

Why it helps:

- Easier to reason about than a giant selectable TA-Lib candlestick catalog.
- Better suited for automated exploration because semantics are tight and visible.

### `SWING_PIVOT_REVERSAL`

Purpose: detect local pivot confirmation.

Long trigger:

- A local low forms over `left/right` bars.
- Price confirms by breaking above the pivot candle high or prior candle high.

Short trigger:

- A local high forms.
- Price confirms by breaking below the pivot candle low or prior candle low.

Why it helps:

- Converts visible reversal structure into an event.
- Useful when paired with higher-timeframe stretch/context indicators.

## Priority 3: Add Trend-Continuation Entry Triggers

These are needed so trigger-role coverage is not only mean-reversion.

### `PULLBACK_RESUME`

Purpose: detect continuation after a pullback in an established trend.

Long trigger:

- Higher/mid-timeframe trend context is bullish.
- Price pulls back to MA, channel midline, or VWAP-like anchor.
- Current close resumes above a short trigger level.

Short trigger:

- Bearish trend context.
- Price pulls back upward to MA/midline.
- Current close resumes lower.

Why it helps:

- Gives the explorer a non-mean-reversion way to use precise entries.

### `MOMENTUM_REACCELERATION`

Purpose: trigger when momentum resumes after compression.

Long trigger:

- Momentum or MACD histogram was contracting or near zero.
- Current bar expands positive beyond threshold.

Short trigger:

- Momentum was contracting or near zero.
- Current bar expands negative beyond threshold.

Why it helps:

- Reuses MACD histogram/MOM concepts.
- Provides an event-like companion to trend-following state indicators.

## Priority 4: Add Indicator Role Metadata

Add or derive a role layer beyond the existing `strategyRole`.

Suggested fields:

- `signalRole`: `context`, `setup`, `trigger`, `filter`
- `signalPersistence`: `state`, `event`, `event-with-lookback`
- `preferredTimeframeRole`: `higher-context`, `mid-setup`, `entry`

Initial classification:

- `trigger`: MA cross, MACD cross, Stoch cross, all new crossback/reclaim/reentry/first-close/price-action triggers.
- `setup`: RSI range, StochRSI range, CCI, CMO, MFI, WILLR, BBands/Keltner/Donchian position.
- `context`: ADX, MA slope, MA spread, trend variants, higher-timeframe momentum/trend state.
- `filter`: ATR volatility, ADX threshold-style usage, volume/liquidity gates.

Why it helps:

- Lets generation intentionally assemble coherent profiles.
- Lets scoring distinguish fresh triggers from persistent state.
- Reduces accidental all-state profiles.

## Priority 5: Update Explorer Candidate Generation

Change prompt seed / candidate generation rules so random exploration is role-balanced.

Baseline generation rule:

- Require at least one `trigger` indicator on the lowest entry timeframe.
- Prefer one `setup` indicator on the entry or mid timeframe.
- Prefer one `context` or `filter` indicator on a higher timeframe.
- Avoid selecting `5..10` unstructured indicators from the entire catalog.

Suggested templates:

- Mean reversion:
  - higher timeframe trend/range filter
  - oscillator or band stretch setup
  - crossback/reentry/wick trigger
- Trend continuation:
  - higher timeframe trend filter
  - pullback setup
  - reclaim/resume trigger
- Breakout:
  - volatility/compression filter
  - channel boundary setup
  - first-close breakout trigger

## Priority 6: Add Diagnostics, Not Hard Caps

Do not cap reward multiple as the main fix.

Instead, add diagnostics that classify strategy style:

- `trigger_coverage`: whether the profile has a trigger-role indicator.
- `state_only_profile`: true if all components are state/context.
- `max_signal_run`: already available in replay artifacts.
- `median_signal_run`: useful companion to max.
- `tail_capture_label`: high-R winner whose normal-R cells are weak.
- `calibrated_entry_label`: profile that performs reasonably under normal-R cells.

Use these to explain why a profile scores well, not to automatically reject it.

## Implementation Order

1. Verify existing trigger indicator implementations end to end:
   - `RSI_CROSSBACK`
   - `STOCHRSI_CROSSBACK`
   - `PRICE_RECLAIM_MA`
   - `CHANNEL_REENTRY`
   - `BREAKOUT_FIRST_CLOSE`
   - `WICK_REJECTION`
2. Ensure autoresearch role detection includes all existing trigger IDs.
3. Update explorer seed generation to prefer at least one entry trigger.
4. Add or verify `signalRole` / `signalPersistence` metadata is present for every indicator.
5. Add derived diagnostics for state-only versus trigger-backed profiles.
6. Add remaining custom price-action or continuation triggers only after the existing trigger set is being used effectively.
7. Re-run corpus analysis comparing:
    - state-only profiles
    - trigger-backed profiles
    - high-R tail-capture profiles
    - profiles whose `1R..4R` cells remain competitive

## Success Criteria

- New strategies are no longer dominated by all-state indicator stacks.
- A meaningful share of candidates contain a lowest-timeframe trigger.
- More candidates show viable `1R..4R` cells without manually capping reward multiple.
- High-R winners can still survive, but are classified as tail-capture rather than silently treated as calibrated entries.
- Live cadence should improve because trigger-backed profiles fire on fresh events instead of repeated bars inside the same stale condition.
