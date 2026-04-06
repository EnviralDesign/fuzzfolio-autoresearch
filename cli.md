# Autoresearch CLI Reference

All commands are run with `uv run autoresearch <command>`.

---

## doctor

Verify config, CLI, auth, and seed prompt.

```powershell
uv run autoresearch doctor
uv run autoresearch doctor --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--json` | flag | false | Print machine-readable JSON instead of human output. |

---

## test-providers

Smoke-test configured LLM provider profiles against a few one-shot JSON scenarios.

```powershell
uv run autoresearch test-providers
uv run autoresearch test-providers --profile openai-mini
uv run autoresearch test-providers --profile xai-grok --profile groq-oss-20b
uv run autoresearch test-providers --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--profile` | string (repeatable) | all profiles | Only test the named provider profile. Can be repeated to test multiple specific profiles. |
| `--json` | flag | false | Print machine-readable JSON. |

---

## run

Run the autonomous research controller.

```powershell
uv run autoresearch run --max-steps 20
uv run autoresearch run --max-steps 20 --json
uv run autoresearch run --max-steps 20 --plain-progress
uv run autoresearch run --explorer-profile openai-mini --supervisor-profile xai-grok
uv run autoresearch run --advisor-profile grok-420-multi-agent-0309 --advisor-every 20
uv run autoresearch run --no-advisor
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--max-steps` | int | from config | Step cap for this run. |
| `--explorer-profile` | string | from config | Override the configured explorer provider profile for this run. |
| `--supervisor-profile` | string | from config | Override the configured supervisor provider profile for this run. |
| `--advisor-profile` | string (repeatable) | from config | Override advisor provider profiles. Can be repeated. |
| `--advisor-every` | int | from config | Inject advisor guidance every N steps. |
| `--no-advisor` | flag | false | Disable periodic advisor guidance for this run. |
| `--json` | flag | false | Print machine-readable JSON instead of live console progress. |
| `--plain-progress` | flag | false | Use plain line-oriented output instead of Rich panels. |

---

## supervise

Run the supervised controller with config-backed policy defaults. The supervisor owns termination in this mode. When a supervised session hits its step cap, supervise starts a fresh isolated session if the outer time window is still open.

```powershell
uv run autoresearch supervise
uv run autoresearch supervise --max-steps 300 --window 23:00-05:00 --timezone America/Chicago
uv run autoresearch supervise --no-window
uv run autoresearch supervise --explorer-profile codex-mini
uv run autoresearch supervise --advisor-profile grok-420-multi-agent-0309 --advisor-every 20
uv run autoresearch supervise --no-advisor
uv run autoresearch supervise --json
uv run autoresearch supervise --plain-progress
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--max-steps` | int | from config | Per-session step cap before supervise starts a fresh isolated session. |
| `--window` | string | from config | Operating window in `HH:MM-HH:MM` format (e.g. `23:00-05:00`). |
| `--no-window` | flag | false | Disable windowing and run sessions around the clock. |
| `--timezone` | string | from config | IANA timezone for the operating window (e.g. `America/Chicago`). |
| `--explorer-profile` | string | from config | Override the configured explorer provider profile. |
| `--supervisor-profile` | string | from config | Override the configured supervisor provider profile. |
| `--advisor-profile` | string (repeatable) | from config | Override advisor provider profiles. Can be repeated. |
| `--advisor-every` | int | from config | Inject advisor guidance every N steps. |
| `--no-advisor` | flag | false | Disable periodic advisor guidance. |
| `--json` | flag | false | Print machine-readable JSON instead of live console progress. |
| `--plain-progress` | flag | false | Use plain line-oriented output instead of Rich panels. |

---

## plot

Generate a run-local or all-runs derived progress plot.

```powershell
uv run autoresearch plot
uv run autoresearch plot --run-id 20260324T181958Z-agentic
uv run autoresearch plot --all-runs
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string | latest run | Specific run id to render. Defaults to the latest discovered run. |
| `--all-runs` | flag | false | Render a derived aggregate plot across all runs. |

---

## leaderboard

Generate derived best-per-run leaderboard artifacts including classic bar leaderboard, model averages, tradeoff map, validation views, and similarity analysis.

```powershell
uv run autoresearch leaderboard
uv run autoresearch leaderboard --limit 10
uv run autoresearch leaderboard --force-rebuild
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--limit` | int | 15 | Maximum number of runs to show in the classic bar leaderboard. Validation and similarity analyze the full best-per-run set. |
| `--force-rebuild` | flag | false | Ignore cached validation artifacts and rebuild all derived validation/similarity inputs. |

---

## dashboard

Serve a local SPA for run, leaderboard, and backtest drilldown.

```powershell
uv run autoresearch dashboard
uv run autoresearch dashboard --host 0.0.0.0 --port 47832
uv run autoresearch dashboard --limit 50
uv run autoresearch dashboard --force-rebuild
uv run autoresearch dashboard --no-refresh-on-start
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--host` | string | `0.0.0.0` | Bind host. |
| `--port` | int | 47832 | Bind port. |
| `--limit` | int | 25 | Classic bar leaderboard display limit during refresh. Validation and similarity still analyze the full best-per-run set. |
| `--force-rebuild` | flag | false | Ignore cached validation artifacts when refreshing derived dashboard data. |
| `--no-refresh-on-start` | flag | false | Serve immediately using current derived artifacts instead of rebuilding on startup. |

---

## sync-profile-drop-pngs

Rebuild run-local profile-drop PNGs for each run's best scored attempt. Walks each run, finds its best scored attempt, ensures the backing profile exists in cloud storage, rebuilds a fresh profile-drop bundle, and renders side-by-side horizon cards.

```powershell
uv run autoresearch sync-profile-drop-pngs
uv run autoresearch sync-profile-drop-pngs --run-id 20260327T212626114512Z-agentic-0245ff
uv run autoresearch sync-profile-drop-pngs --lookback-months 36
uv run autoresearch sync-profile-drop-pngs --force-rebuild
uv run autoresearch sync-profile-drop-pngs --keep-temp
uv run autoresearch sync-profile-drop-pngs --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only process the named run id. Can be repeated to process multiple specific runs. |
| `--lookback-months` | int | 12 | Fixed deep-replay lookback window in months for rebuilt profile-drop cards. Always renders `12mo` and `36mo` cards plus the requested horizon. |
| `--force-rebuild` | flag | false | Ignore existing profile-drop PNG/manifests and rerender every requested horizon. |
| `--keep-temp` | flag | false | Keep temporary package bundles under each run directory instead of deleting them after a successful render. |
| `--json` | flag | false | Print machine-readable JSON. |

---

## nuke-deep-caches

Delete rebuildable deep-cache artifacts so the next `build-portfolio` starts cold from source.

```powershell
uv run autoresearch nuke-deep-caches
uv run autoresearch nuke-deep-caches --json
```

No filtering arguments. The command removes:

- attempt-local `full-backtest-36mo-curve.json`
- attempt-local `full-backtest-36mo-result.json`
- attempt-local `scrutiny-cache/`
- run-local `profile-drop-*.png` and `profile-drop-*.manifest.json`
- everything under `runs/derived/`

It preserves the original attempt/source artifacts such as `sensitivity-response.json`, ledgers, and run metadata.

Typical follow-up:

```powershell
uv run autoresearch build-portfolio
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--json` | flag | false | Print machine-readable reset summary. |

---

## reset-runs

Delete all run artifacts and recreate a clean empty runs state.

```powershell
uv run autoresearch reset-runs
```

No arguments.

---

## prune-runs

Delete low-signal run directories such as smoke tests or early dead runs. A "mapped point" is a scored attempt with a non-null `composite_score`.

```powershell
uv run autoresearch prune-runs
uv run autoresearch prune-runs --min-mapped-points 5
uv run autoresearch prune-runs --yes
uv run autoresearch prune-runs --preview 10
uv run autoresearch prune-runs --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--min-mapped-points` | int | 2 | Keep runs with at least this many mapped points (scored attempts). Runs with fewer are considered low-signal. |
| `--yes` | flag | false | Actually delete the matched runs. Without this flag the command only performs a dry run. |
| `--preview` | int | 20 | How many matched runs to include in the preview output. |
| `--json` | flag | false | Print machine-readable JSON. |

---

## stop-all-runs

Clear local queued Fuzzfolio research work (sweep/deep-replay/sim queues through the Trading-Dashboard harness) and optionally stop local autoresearch processes.

```powershell
uv run autoresearch stop-all-runs
uv run autoresearch stop-all-runs --stop-autoresearch
uv run autoresearch stop-all-runs --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--stop-autoresearch` | flag | false | Also stop local `autoresearch run` and `autoresearch supervise` Python processes. |
| `--json` | flag | false | Print machine-readable JSON. |

---

## purge-cloud-profiles

Delete saved scoring profiles from the currently configured Fuzzfolio account. Uses the existing configured Fuzzfolio auth profile and CLI session.

```powershell
uv run autoresearch purge-cloud-profiles
uv run autoresearch purge-cloud-profiles --yes
uv run autoresearch purge-cloud-profiles --preview 20
uv run autoresearch purge-cloud-profiles --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--yes` | flag | false | Actually delete the listed cloud profiles. Without this flag the command only performs a dry run. |
| `--preview` | int | 10 | How many profiles to include in the preview output. |
| `--json` | flag | false | Print machine-readable JSON. |

---

## calculate-full-backtests

Calculate 3-year backtest curves for all attempts that do not have them yet.

```powershell
uv run autoresearch calculate-full-backtests
uv run autoresearch calculate-full-backtests --run-ids 20260324T181958Z-agentic 20260325T101010Z-agentic
uv run autoresearch calculate-full-backtests --attempt-id 20260327T205658482523Z-agentic-aebf78-attempt-00045 --limit 1
uv run autoresearch calculate-full-backtests --require-scrutiny-36
uv run autoresearch calculate-full-backtests --force-rebuild
uv run autoresearch calculate-full-backtests --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-ids` | string (repeatable) | all runs | Specific run IDs to process. Defaults to all runs. |
| `--attempt-id` | string (repeatable) | null | Only process the named attempt id. |
| `--limit` | int | null | Optional cap on how many matched attempts to process after score sorting. |
| `--max-workers` | int | detected dev Sim Worker count | Maximum concurrent full-backtest jobs. |
| `--no-use-dev-sim-worker-count` | flag | false | Disable dev sim-worker auto sizing and fall back to `validation_max_concurrency` unless `--max-workers` is set. |
| `--require-scrutiny-36` | flag | false | Only materialize full backtests for attempts that already have `36mo` scrutiny. |
| `--force-rebuild` | flag | false | Recalculate even if the full-backtest file already exists. |
| `--json` | flag | false | Print machine-readable JSON. |

Behavior notes:

- The command now uses a queued worker loop with Rich progress for the heavy path.
- It first tries to materialize `full-backtest-36mo-*` from an existing attempt-local `36mo` scrutiny cache or matching legacy run-validation cache.
- Only candidates that cannot be seeded fall through to the dev backend for a real `sensitivity-basket` run.

---

## build-attempt-catalog

Build a corpus-wide attempt catalog and cache-coverage audit.

```powershell
uv run autoresearch build-attempt-catalog
uv run autoresearch build-attempt-catalog --run-id manual-mtf-pullback-regime-20260329
uv run autoresearch build-attempt-catalog --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only catalog the named run id. Can be repeated. |
| `--json` | flag | false | Print machine-readable JSON. |

Outputs under `runs/derived/`:

- `attempt-catalog.json`
- `attempt-catalog.csv`
- `attempt-catalog-summary.json`

---

## hydrate-scrutiny-cache

Heal or rebuild attempt-local scrutiny caches for selected attempts.

```powershell
uv run autoresearch hydrate-scrutiny-cache --limit 10
uv run autoresearch hydrate-scrutiny-cache --run-id manual-mtf-pullback-regime-20260329 --lookback-months 36
uv run autoresearch hydrate-scrutiny-cache --attempt-id <ATTEMPT_ID> --force-rebuild
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only process attempts from the named run id. |
| `--attempt-id` | string (repeatable) | null | Only process the named attempt id. |
| `--lookback-months` | int (repeatable) | `12, 36` | Scrutiny horizons to build. |
| `--limit` | int | all matched attempts | Optional cap after score sorting. |
| `--force-rebuild` | flag | false | Ignore existing attempt-local scrutiny artifacts and rebuild them. |
| `--json` | flag | false | Print machine-readable JSON. |

The command prefers portable attempt-local caches and will seed them from:

- existing attempt-local `scrutiny-cache/<horizon>`
- existing `full-backtest-36mo-*` files for `36mo`
- legacy `runs/derived/validation-cache/<run-id>/<horizon>` artifacts when they match the attempt

---

## build-shortlist-report

Build a diversified `36mo` shortlist, render charts, and optionally generate official profile-drop PNGs for the selected candidates.

```powershell
uv run autoresearch build-shortlist-report
uv run autoresearch build-shortlist-report --shortlist-size 24 --no-generate-profile-drops
uv run autoresearch build-shortlist-report --trade-rate-bonus-weight 8 --trade-rate-bonus-target 4
uv run autoresearch build-shortlist-report --profile-drop-workers 8
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only consider attempts from the named run id. |
| `--attempt-id` | string (repeatable) | null | Only consider the named attempt id. |
| `--candidate-limit` | int | -1 | Optional cap on ranked candidates before similarity/selection. |
| `--shortlist-size` | int | 12 | How many candidates to put on the shortlist. |
| `--min-score-36` | float | 40.0 | Minimum `36mo` score required for shortlist consideration. |
| `--min-retention-ratio` | float | 0.0 | Minimum `36m / 12m` score retention ratio when `12mo` scrutiny exists. |
| `--min-trades-per-month` | float | 0.0 | Minimum `36mo` trade cadence. |
| `--max-drawdown-r` | float | -1.0 | Maximum allowed `36mo` drawdown in R. Use `-1` to disable. |
| `--drawdown-penalty` | float | 0.65 | Penalty applied per R of `36mo` max drawdown during shortlist selection. |
| `--trade-rate-bonus-weight` | float | 0.0 | Optional positive utility bonus for higher `36mo` trade cadence. |
| `--trade-rate-bonus-target` | float | 8.0 | Cadence level where the bonus saturates. |
| `--novelty-penalty` | float | 18.0 | Penalty applied to max sameness during shortlist selection. |
| `--max-per-run` | int | 1 | Maximum shortlisted candidates per run. Use `-1` to disable. |
| `--max-per-strategy-key` | int | 1 | Maximum shortlisted candidates per normalized `timeframe + instrument-set`. Use `-1` to disable. |
| `--max-sameness-to-board` | float | 0.78 | Stop selecting once max sameness to the current board exceeds this ceiling. |
| `--require-full-backtest-36` | bool | true | Require valid local `36mo` full-backtest artifacts for shortlist candidates. |
| `--generate-profile-drops` | bool | true | Render official profile-drop PNGs for shortlisted candidates. |
| `--profile-drop-lookback-months` | int | 36 | Lookback used for shortlisted profile-drop PNG generation. |
| `--chart-trades-x-max` | float | 300.0 | Default cap for trades/month charts. Use a negative number to disable. |
| `--profile-drop-timeout-seconds` | int | 1800 | Per-candidate timeout for profile-drop packaging/rendering. |
| `--profile-drop-workers` | int | 4 | Concurrent workers for shortlisted profile-drop packaging/rendering. |
| `--force-rebuild-profile-drops` | flag | false | Re-render shortlisted profile-drop PNGs even if derived copies already exist. |
| `--json` | flag | false | Print machine-readable JSON. |

Outputs under `runs/derived/shortlist-report/`:

- `shortlist-report.json`
- `shortlist-report.csv`
- `charts/*`
- `profile-drops/*`

---

## build-portfolio

Build a config-driven multi-sleeve portfolio report, charts, and optional profile-drop PNGs.

```powershell
uv run autoresearch build-portfolio
uv run autoresearch build-portfolio --portfolio-config .\portfolio.config.json
uv run autoresearch build-portfolio --catch-up-full-backtests
uv run autoresearch build-portfolio --profile-drop-workers 8
uv run autoresearch build-portfolio --no-generate-profile-drops
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only consider attempts from the named run id. |
| `--attempt-id` | string (repeatable) | null | Only consider the named attempt id. |
| `--portfolio-config` | path | `portfolio.config.json` | JSON portfolio config path. Falls back to built-in defaults if the file is missing. |
| `--catch-up-full-backtests` | bool | config/default | Catch up missing `36mo` full-backtests before building the portfolio. |
| `--catch-up-force-rebuild` | bool | config/default | Force full-backtest rebuilds during the optional catch-up phase. |
| `--catch-up-require-scrutiny-36` | bool | config/default | Only catch up attempts that already have `36mo` scrutiny. |
| `--generate-profile-drops` | bool | config/default | Enable or disable final portfolio profile-drop PNG generation. |
| `--profile-drop-workers` | int | config/default | Override worker count for portfolio profile-drop packaging/rendering. |
| `--json` | flag | false | Print machine-readable JSON. |

Outputs under `runs/derived/portfolio-report/<portfolio-name>/`:

- `portfolio-report.json`
- `portfolio-report.csv`
- `charts/*`
- `profile-drops/*`

Default config source:

- `portfolio.config.json` at the repo root if present
- otherwise the built-in default two-sleeve portfolio
- see `portfolio.config.example.json` for a starting point
- `full_backtest_job_timeout_seconds` in the config controls the deep-replay wait timeout used by catch-up full-backtests

---

## build-promotion-board

Build a `36mo` promotion-oriented board that balances score against sameness.

```powershell
uv run autoresearch build-promotion-board
uv run autoresearch build-promotion-board --candidate-limit 200 --board-size 12
uv run autoresearch build-promotion-board --hydrate-missing --force-rebuild
uv run autoresearch build-promotion-board --require-full-backtest-36 --max-per-run 1 --max-per-strategy-key 1
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-id` | string (repeatable) | all runs | Only consider attempts from the named run id. |
| `--attempt-id` | string (repeatable) | null | Only consider the named attempt id. |
| `--candidate-limit` | int | 250 | Maximum candidate pool after score sorting. |
| `--board-size` | int | 12 | How many promotion candidates to select. |
| `--min-score-36` | float | 40.0 | Minimum `36mo` score required for inclusion. |
| `--min-retention-ratio` | float | 0.0 | Minimum `36m / 12m` retention ratio when `12mo` scrutiny exists. |
| `--min-trades-per-month` | float | 0.0 | Minimum `36mo` cadence required. |
| `--novelty-penalty` | float | 18.0 | Penalty applied to max sameness during greedy selection. |
| `--max-per-run` | int | 2 | Maximum selected candidates per run. Use `-1` to disable. |
| `--max-per-strategy-key` | int | 2 | Maximum selected candidates per normalized `timeframe + instrument-set`. Use `-1` to disable. |
| `--max-sameness-to-board` | float | 0.85 | Hard exclusion ceiling for max sameness to the current board. |
| `--require-full-backtest-36` | flag | false | Restrict the board to attempts with attempt-local `full-backtest-36mo-*` artifacts. |
| `--hydrate-missing` | flag | false | Heal missing long-horizon scrutiny for the candidate pool before ranking. |
| `--force-rebuild` | flag | false | Rebuild hydrated scrutiny artifacts instead of reusing caches. |
| `--json` | flag | false | Print machine-readable JSON. |

Outputs under `runs/derived/`:

- `promotion-board.json`
- `promotion-board.csv`

The JSON payload also includes:

- `filter_rejections`
- `selected_by_run`
- `selected_by_strategy_key`

---

## score

Score one sensitivity artifact directory.

```powershell
uv run autoresearch score ./runs/20260324T181958Z-agentic/attempts/attempt_001
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `artifact_dir` | path | (required) | Path to the artifact directory to score. |

---

## record-attempt

Score and append one artifact directory to the attempts ledger.

```powershell
uv run autoresearch record-attempt ./runs/20260324T181958Z-agentic/attempts/attempt_001
uv run autoresearch record-attempt ./attempts/attempt_001 --candidate-name "my-candidate" --run-id 20260324T181958Z-agentic
uv run autoresearch record-attempt ./attempts/attempt_001 --profile-ref prof_abc123
uv run autoresearch record-attempt ./attempts/attempt_001 --note "early test run"
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `artifact_dir` | path | (required) | Path to the artifact directory to record. |
| `--candidate-name` | string | null | Optional name for this candidate. |
| `--run-id` | string | `manual` | Run ID to associate this attempt with. Defaults to `manual`. |
| `--profile-ref` | string | null | Profile reference to attach to this attempt. |
| `--note` | string | null | Optional note to attach to this attempt. |

---

## rescore-attempts

Recompute scores for the existing attempts ledger using the current scoring config. Re-evaluates all attempts across all runs, updates their scores, and regenerates progress plots.

```powershell
uv run autoresearch rescore-attempts
```

No arguments.
