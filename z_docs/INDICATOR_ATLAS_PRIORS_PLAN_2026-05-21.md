# Indicator Atlas And Priors Plan

This is the bridge from blind indicator generation to recipe-driven generation. Treat every score here as a prior, not a universal truth about an indicator.

## Implemented Layer 0: Static Catalog Audit

Command:

```powershell
uv run build-indicator-atlas
```

Default behavior has no required CLI arguments. It reads the Fuzzfolio indicator catalog from `C:\repos\Trading-Dashboard` unless a configured workspace root, `AUTORESEARCH_FUZZFOLIO_WORKSPACE_ROOT`, `--workspace-root`, or `--catalog-path` overrides it.

Artifacts:

- `runs/derived/indicator-atlas/indicator-atlas.json`
- `runs/derived/indicator-atlas/indicator-atlas.csv`
- `runs/derived/indicator-atlas/indicator-dependencies.json`
- `runs/derived/indicator-atlas/indicator-pair-matrix.csv`
- `runs/derived/indicator-atlas/recipe-priors.json`

The audit records:

- indicator id, namespace, TA function, base indicator, strategy role, signal role, signal persistence, and preferred timeframe role
- implementation factory mapping and expected scaffoldability
- TA/config parameter count, sweepable parameters, default ring values, and theoretical internal parameter cardinality
- static prior bucket and static prior score
- base-indicator, implementation-class, TA-function, namespace, role, and strategy dependencies
- static anchor-trigger compatibility priors
- static recipe-slot priors plus the 80/15/5 guided/uncertain/wild exploration policy

## Current Live Baseline

As of the first live run:

- 87 catalog indicators were found.
- 87/87 map to implementation classes.
- 23 are triggers, 36 setup, 20 context, and 8 filters.
- The static buckets are 43 high static prior, 34 context dependent, and 10 low prior broad space.
- The largest raw parameter spaces confirm that exhaustive search is the wrong model. Examples include `KST_CROSSOVER`, `ROLLING_VOLUME_PROFILE_CONTEXT`, `GARCH_VOLATILITY_REGIME`, `NVO_VOLUME_IMPULSE`, `STOCHRSI_CROSSBACK`, `WAVETREND_CROSSOVER`, `BREAKOUT_FIRST_CLOSE`, `CHANNEL_REENTRY`, `MARKET_MODE_TRANSITION`, and `WICK_REJECTION`.

## Interpretation Rules

Do not delete an indicator because the static atlas gives it a low prior. The static atlas only answers:

- Is the catalog/factory wiring coherent?
- Which roles and recipes does the metadata say this indicator belongs to?
- How large is the raw internal parameter space?
- Which indicators share implementation/base/TA-function dependencies?
- Which anchors and triggers should be cheap to test first?

It does not answer:

- whether the signal fires at a useful density
- whether the signal has forward response
- whether a pair improves score retention
- whether a recipe survives 12 month or 36 month validation

## Next Layer 1: Signal Behavior Atlas

No-P&L command:

```powershell
uv run build-signal-atlas
```

Default target:

- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- timeframes: `M5`, `M15`
- bars: 5000 per Signal Replay call
- replay source: `system` through `fuzzfolio-agent-cli replay simulate --source system`
- configs: current implementation uses defaults; default-ring expansion is a follow-up
- profiles: temporary catalog-derived profiles are created and deleted automatically

Metrics:

- signal/event count
- long/short balance
- percent active
- median bars between events
- average persistence
- high/low volatility occurrence
- trend/range occurrence
- overlap/redundancy with sibling indicators

## Current Layer 2: Forward Response

No-backend command:

```powershell
uv run build-forward-response-atlas
```

Artifacts:

- `runs/derived/forward-response-atlas/forward-response-atlas.json`
- `runs/derived/forward-response-atlas/forward-response-atlas.csv`
- `runs/derived/forward-response-atlas/forward-response-rollups.csv`
- `runs/derived/forward-response-atlas/forward-response-priors.csv`
- `runs/derived/forward-response-atlas/forward-response-issues.csv`

The priors file intentionally separates aggregate response from best conditional cell response. A trigger can be neutral globally but still useful in a specific instrument/timeframe/direction/horizon context.

For trigger-like indicators, profile events without full strategy construction:

- forward return after 1, 3, 6, 12, and 24 bars
- max favorable excursion
- max adverse excursion
- probability MFE exceeds MAE
- volatility-normalized response

This layer should rank signal shape, not final profitability.

## Next Layer 3: Anchor-Pair Probes

Use `indicator-pair-matrix.csv` as the static queue seed. Start with a deliberately small Trigger Atlas v1:

- trend anchors: `MA_SLOPE_TREND`, `ADX`, `KALMAN_VELOCITY_CONFIRM`
- mean-reversion anchors: `RSI_MEAN_REVERSION`, `BBANDS_POSITION_MEAN_REVERSION`
- compression anchors: `BOLLINGER_KELTNER_SQUEEZE_FILTER`, `TOBY_CRABEL_NARROW_RANGE`
- profile/value anchor: `ROLLING_VOLUME_PROFILE_CONTEXT`

Probe:

- anchor default plus trigger default
- anchor default plus trigger default ring
- 3 month screen first
- 12 month validation only for promoted subsets
- 36 month scrutiny only for the top decile or manually selected branches

## Generation Policy

Once empirical layers exist, AutoResearch generation should move from:

```text
random indicators -> profile -> sweep
```

to:

```text
recipe -> slots -> prior-weighted indicator choices -> preflight -> sweep
```

Sampling target:

- 80 percent from high/medium priors
- 15 percent from uncertain priors
- 5 percent wild exploration

The 5 percent wild lane is intentional. It protects against prematurely closing off weird but useful combinations.
