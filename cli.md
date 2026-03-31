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
| `--host` | string | `127.0.0.1` | Bind host. |
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
uv run autoresearch calculate-full-backtests --force-rebuild
uv run autoresearch calculate-full-backtests --json
```

| Argument | Type | Default | Description |
|---|---|---|---|
| `--run-ids` | string (repeatable) | all runs | Specific run IDs to process. Defaults to all runs. |
| `--force-rebuild` | flag | false | Recalculate even if the full-backtest file already exists. |
| `--json` | flag | false | Print machine-readable JSON. |

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
