# fuzzfolio-autoresearch

CLI-first tooling for turning a large Fuzzfolio runs corpus into usable long-horizon evidence, shortlist artifacts, and a read-only dashboard.

The important shift is:

- heavy computation happens in CLI commands
- attempt-local caches are treated as first-class artifacts
- `36mo` evidence is the main promotion horizon
- the dashboard is only a viewer of already-computed data

Everything below assumes you are running from the repo root:

```powershell
cd C:\repos\fuzzfolio-autoresearch
```

## What This Repo Is For

There are two lanes:

1. Controller/runtime lane
   - autonomous research runs
   - per-run ledgers
   - scored attempts

2. Corpus analysis lane
   - corpus-wide attempt catalog
   - portable `12mo` and `36mo` scrutiny
   - attempt-local `36mo` full backtests
   - shortlist and promotion selection
   - derived charts and dashboard viewing

If you only care about surfacing the best validated strategies, the corpus analysis lane is the one that matters.

## Setup

Copy the local config templates:

```powershell
Copy-Item autoresearch.config.example.json autoresearch.config.json
Copy-Item .agentsecrets.example .agentsecrets
```

Then verify the stack:

```powershell
uv run autoresearch doctor
uv run autoresearch test-providers
```

## Daily Commands

Most useful root commands:

```powershell
uv run autoresearch calculate-full-backtests
uv run autoresearch build-attempt-catalog
uv run autoresearch audit-full-backtests
uv run autoresearch build-shortlist-report
uv run autoresearch build-portfolio
uv run autoresearch nuke-deep-caches
uv run autoresearch build-promotion-board
uv run autoresearch plot-corpus-score-vs-trades
uv run autoresearch dashboard
```

## Common Workflows

### 1. Cold-reset all rebuildable deep caches

Use this when deep-replay semantics changed and you want the next portfolio build to regenerate backtests, scrutiny, charts, and profile drops from source.

```powershell
uv run autoresearch nuke-deep-caches
uv run autoresearch build-portfolio
```

What it deletes:

- attempt-local `full-backtest-36mo-*`
- attempt-local `scrutiny-cache/`
- run-local `profile-drop-*.png` and manifests
- everything under `runs/derived/`

What it does not delete:

- original run folders
- attempt ledgers
- base sensitivity/source artifacts like `sensitivity-response.json`

### 2. Resume corpus backfill

This is the main heavy command. It fills missing attempt-local `36mo` full backtests, uses the detected dev sim-worker count by default, and is resumable when you do not pass `--force-rebuild`.

```powershell
uv run autoresearch calculate-full-backtests
```

Useful variants:

```powershell
uv run autoresearch calculate-full-backtests --require-scrutiny-36
uv run autoresearch calculate-full-backtests --attempt-id 20260327T205658482523Z-agentic-aebf78-attempt-00045
uv run autoresearch calculate-full-backtests --limit 10
uv run autoresearch calculate-full-backtests --max-workers 12
uv run autoresearch calculate-full-backtests --force-rebuild
uv run autoresearch calculate-full-backtests --json
```

Important flags:

- `--require-scrutiny-36`
  - only backfill attempts that already have `36mo` scrutiny
  - good for targeted completion
- `--limit`
  - smoke-test on a small slice
- `--attempt-id`
  - isolate a broken or interesting candidate
- `--force-rebuild`
  - recompute even if the full-backtest files already exist
  - not resumable in the normal sense
- `--no-use-dev-sim-worker-count`
  - disable auto sizing from the running dev sim-worker fleet

Artifacts and summaries written:

- attempt-local `full-backtest-36mo-curve.json`
- attempt-local `full-backtest-36mo-result.json`
- [runs/derived/full-backtest-failures.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/full-backtest-failures.json)
- refreshed catalog and audit files under `runs/derived`

### 3. Rebuild scrutiny caches

This heals or rebuilds portable attempt-local scrutiny caches. Use it when `12mo` or `36mo` scrutiny is missing or stale.

```powershell
uv run autoresearch hydrate-scrutiny-cache
```

Useful variants:

```powershell
uv run autoresearch hydrate-scrutiny-cache --lookback-months 36
uv run autoresearch hydrate-scrutiny-cache --attempt-id 20260327T205658482523Z-agentic-aebf78-attempt-00045
uv run autoresearch hydrate-scrutiny-cache --limit 20
uv run autoresearch hydrate-scrutiny-cache --force-rebuild
```

Important flags:

- `--lookback-months`
  - repeatable
  - defaults to `12` and `36`
- `--force-rebuild`
  - ignore current scrutiny caches and rebuild them

### 4. Rebuild the corpus catalog

This indexes the whole corpus and summarizes what evidence exists per attempt.

```powershell
uv run autoresearch build-attempt-catalog
```

Outputs:

- [attempt-catalog.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/attempt-catalog.json)
- [attempt-catalog.csv](/C:/repos/fuzzfolio-autoresearch/runs/derived/attempt-catalog.csv)
- [attempt-catalog-summary.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/attempt-catalog-summary.json)

Note: most mutating CLI commands now refresh the catalog automatically when they finish.

### 4. Audit whether the corpus is trustworthy yet

This is the readiness check. It does not generate cache.

```powershell
uv run autoresearch audit-full-backtests
```

Useful variants:

```powershell
uv run autoresearch audit-full-backtests --run-id 20260327T205658482523Z-agentic-aebf78
uv run autoresearch audit-full-backtests --attempt-id 20260327T205658482523Z-agentic-aebf78-attempt-00045
```

Output:

- [full-backtest-audit.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/full-backtest-audit.json)

What it tells you:

- how many attempts have valid local `36mo` full backtests
- how many are still missing
- whether artifacts validate cleanly
- whether shortlist and promotion outputs are still provisional

### 5. Build the shortlist report

This is the main selection command. It filters, applies similarity pressure, renders charts, and by default generates official profile-drop PNGs for the selected candidates.

```powershell
uv run autoresearch build-shortlist-report
```

Default behavior:

- uses valid local `36mo` full backtests
- applies a `36mo` score floor
- penalizes sameness
- penalizes drawdown
- enforces per-run and per-strategy caps
- writes charts and a JSON/CSV report
- renders official profile-drop PNGs for the shortlist

Useful variants:

```powershell
uv run autoresearch build-shortlist-report --shortlist-size 24
uv run autoresearch build-shortlist-report --shortlist-size 24 --no-generate-profile-drops
uv run autoresearch build-shortlist-report --min-trades-per-month 1
uv run autoresearch build-shortlist-report --trade-rate-bonus-weight 8 --trade-rate-bonus-target 4
uv run autoresearch build-shortlist-report --trade-rate-bonus-weight 15 --trade-rate-bonus-target 2
uv run autoresearch build-shortlist-report --shortlist-size 24 --profile-drop-workers 8
uv run autoresearch build-shortlist-report --max-sameness-to-board 0.72
uv run autoresearch build-shortlist-report --chart-trades-x-max 500
```

Important flags:

- `--shortlist-size`
  - widens or narrows the board
  - this is the easiest way to explore more names
- `--min-score-36`
  - hard score floor
- `--min-trades-per-month`
  - hard cadence floor
  - blunt but sometimes useful
- `--trade-rate-bonus-weight`
  - soft positive reward for higher cadence
  - better than a hard floor when you want more active names without forcing junk in
- `--trade-rate-bonus-target`
  - cadence level where the bonus saturates
- `--profile-drop-workers`
  - parallel worker count for official shortlist profile-drop packaging and rendering
- `--drawdown-penalty`
  - subtracts utility for deeper drawdowns
- `--novelty-penalty`
  - subtracts utility for similar names
- `--max-per-run`
  - cap from the same run
- `--max-per-strategy-key`
  - cap from the same normalized timeframe + instrument shape
- `--max-sameness-to-board`
  - hard similarity ceiling to the already selected board
- `--no-generate-profile-drops`
  - much faster for selection iteration

Outputs:

- [shortlist-report.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/shortlist-report.json)
- [shortlist-report.csv](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/shortlist-report.csv)
- chart PNGs under [shortlist-report/charts](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/charts)
- profile-drop PNGs under [shortlist-report/profile-drops](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/profile-drops)

Useful chart files:

- [corpus-score-vs-trades-36mo.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/charts/corpus-score-vs-trades-36mo.png)
- [shortlist-overlay-score-vs-trades-36mo.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/charts/shortlist-overlay-score-vs-trades-36mo.png)
- [corpus-score-vs-drawdown-36mo.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/charts/corpus-score-vs-drawdown-36mo.png)
- [shortlist-similarity-heatmap.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/charts/shortlist-similarity-heatmap.png)

### 6. Build a multi-sleeve portfolio

Use this when one shortlist is not enough and you want separate sleeves, such as:

- a quality sleeve that keeps rare high-score names even at low cadence
- a cadence sleeve that intentionally leans toward more active names
- a final union report with charts and optional official profile-drop PNGs

Start from the repo-root example config:

```powershell
Copy-Item portfolio.config.example.json portfolio.config.json
```

Main command:

```powershell
uv run autoresearch build-portfolio
```

Important config keys in `portfolio.config.json`:

- `catch_up_full_backtests`
- `catch_up_force_rebuild`
- `catch_up_require_scrutiny_36`
- `full_backtest_job_timeout_seconds`
- `profile_drop_workers`

Useful variants:

```powershell
uv run autoresearch build-portfolio --profile-drop-workers 8
uv run autoresearch build-portfolio --no-generate-profile-drops
uv run autoresearch build-portfolio --catch-up-full-backtests
uv run autoresearch build-portfolio --catch-up-full-backtests --catch-up-require-scrutiny-36
uv run autoresearch build-portfolio --portfolio-config .\portfolio.config.json
```

Important notes:

- `build-portfolio` wraps multiple shortlist-style sleeves from config
- sleeves are unioned, not collapsed into one weighted sum
- profile-drop PNG generation runs in parallel here too
- optional catch-up can backfill missing `36mo` full-backtests before portfolio selection

Outputs:

- `runs/derived/portfolio-report/<portfolio-name>/portfolio-report.json`
- `runs/derived/portfolio-report/<portfolio-name>/portfolio-report.csv`
- `runs/derived/portfolio-report/<portfolio-name>/charts/*`
- `runs/derived/portfolio-report/<portfolio-name>/profile-drops/*`

### 7. Build a stricter promotion board

Use this when you want a smaller, cleaner promotion gate rather than a richer shortlist/report package.

```powershell
uv run autoresearch build-promotion-board
```

Useful variants:

```powershell
uv run autoresearch build-promotion-board --board-size 12 --candidate-limit 250
uv run autoresearch build-promotion-board --require-full-backtest-36
uv run autoresearch build-promotion-board --max-per-run 1 --max-per-strategy-key 1
uv run autoresearch build-promotion-board --min-trades-per-month 1
uv run autoresearch build-promotion-board --hydrate-missing
```

Important flags:

- `--board-size`
- `--candidate-limit`
- `--min-score-36`
- `--min-trades-per-month`
- `--novelty-penalty`
- `--max-per-run`
- `--max-per-strategy-key`
- `--max-sameness-to-board`
- `--require-full-backtest-36`
  - strictest mode
- `--hydrate-missing`
  - heal missing scrutiny before ranking

Outputs:

- [promotion-board.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/promotion-board.json)
- [promotion-board.csv](/C:/repos/fuzzfolio-autoresearch/runs/derived/promotion-board.csv)

### 8. Generate the corpus trade-rate chart

This renders the attempt-level `36mo` score vs trades/month scatter.

```powershell
uv run autoresearch plot-corpus-score-vs-trades
```

Useful variants:

```powershell
uv run autoresearch plot-corpus-score-vs-trades --require-full-backtest-36
uv run autoresearch plot-corpus-score-vs-trades --x-axis-max 500
uv run autoresearch plot-corpus-score-vs-trades --x-axis-max -1
```

Outputs:

- [corpus-score-vs-trades-36mo.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/corpus-score-vs-trades-36mo.png)
- [corpus-score-vs-trades-36mo.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/corpus-score-vs-trades-36mo.json)

Default x-axis cap is `300 trades/month`.

### 9. Open the dashboard viewer

The dashboard is now read-only. It does not rebuild or refresh corpus data.

```powershell
uv run autoresearch dashboard
```

Useful variant:

```powershell
uv run autoresearch dashboard --port 47832
```

Notes:

- `--limit`, `--force-rebuild`, and `--no-refresh-on-start` are legacy no-ops kept for compatibility
- generate data first with the CLI, then open the viewer

## Controller Commands

These are still useful, but they are not the main corpus-analysis path:

```powershell
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
uv run autoresearch plot --all-runs
uv run autoresearch leaderboard
uv run autoresearch sync-profile-drop-pngs
uv run autoresearch prune-runs
uv run autoresearch stop-all-runs
uv run autoresearch purge-cloud-profiles
```

Use them when you are actively generating new runs or maintaining the local research environment.

## Derived Artifact Map

Main derived outputs live under [runs/derived](/C:/repos/fuzzfolio-autoresearch/runs/derived):

- [attempt-catalog.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/attempt-catalog.json)
- [attempt-catalog-summary.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/attempt-catalog-summary.json)
- [full-backtest-audit.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/full-backtest-audit.json)
- [full-backtest-failures.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/full-backtest-failures.json)
- [promotion-board.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/promotion-board.json)
- [shortlist-report/shortlist-report.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/shortlist-report/shortlist-report.json)
- [portfolio-report/default-portfolio/portfolio-report.json](/C:/repos/fuzzfolio-autoresearch/runs/derived/portfolio-report/default-portfolio/portfolio-report.json)
- [corpus-score-vs-trades-36mo.png](/C:/repos/fuzzfolio-autoresearch/runs/derived/corpus-score-vs-trades-36mo.png)

## Recommended Defaults

If you just want the practical sequence:

```powershell
uv run autoresearch calculate-full-backtests
uv run autoresearch audit-full-backtests
uv run autoresearch build-shortlist-report --shortlist-size 24
uv run autoresearch build-portfolio
uv run autoresearch build-promotion-board --board-size 12 --require-full-backtest-36 --max-per-run 1 --max-per-strategy-key 1
uv run autoresearch dashboard
```

If you want to iterate on shortlist shape quickly:

```powershell
uv run autoresearch build-shortlist-report --shortlist-size 24 --no-generate-profile-drops
uv run autoresearch build-shortlist-report --shortlist-size 24 --trade-rate-bonus-weight 8 --trade-rate-bonus-target 4 --no-generate-profile-drops
uv run autoresearch build-shortlist-report --shortlist-size 24 --min-trades-per-month 1 --no-generate-profile-drops
uv run autoresearch build-portfolio --no-generate-profile-drops
```

## Notes

- `36mo` is the primary long-horizon evidence target.
- The shortlist command is richer than the promotion board command.
- The promotion board is the stricter gate.
- The dashboard is read-only and should be treated as a viewer, not as an orchestrator.
- Most heavy commands support `--json` for machine-readable summaries.

For the exhaustive argument tables, see [cli.md](/C:/repos/fuzzfolio-autoresearch/cli.md).
