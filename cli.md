# Autoresearch CLI Reference

Run commands from `C:\repos\fuzzfolio-autoresearch` with `uv run <command>`.

## doctor

```powershell
uv run doctor
uv run doctor --json
```

Checks local config, CLI access, auth, and seed prompt state.

## test-providers

```powershell
uv run test-providers
uv run test-providers --profile codex-54-mini
uv run test-providers --json
```

Arguments:

- `--profile`: repeatable provider profile filter.
- `--json`: print machine-readable output.

## play-hand

```powershell
uv run play-hand
uv run play-hand --coarse-mode evolutionary --sweep-budget high
uv run play-hand --dry-run --json
```

Common arguments:

- `--instrument`: pin one or more instruments.
- `--instrument-pool`: override the shuffle pool.
- `--timeframe`: base timeframe, default `M5`.
- `--min-indicators`, `--max-indicators`: dealt indicator count bounds.
- `--sweep-budget`: `low`, `medium`, or `high`; default high.
- `--max-sweep-permutations`: exact legacy deterministic cap override.
- `--max-reward-r`, `--max-r`: cap the searched reward/R cells.
- `--coarse-mode`: `deterministic` or `evolutionary`.
- `--no-instrument-scout`: skip late cross-instrument scouting.
- `--no-final-artifacts`: skip canonical finalization at wrap-up.
- `--final-profile-drop-count`: positive values finalize the canonical run winner; `0` skips final profile-drop rendering.
- `--final-profile-drop-workers`: final profile-drop worker count.
- `--dry-run`: write the run/plan without backend compute.
- `--json`: print a JSON summary.

## run

```powershell
uv run run --max-steps 20
uv run run --max-steps 20 --json
uv run run --explorer-profile codex-54-mini
```

Runs the legacy Explorer controller. On model finish or step cap it promotes the best scored attempt and calls the canonical finalization path.

Arguments:

- `--max-steps`: override configured step cap.
- `--explorer-profile`: override the explorer provider profile.
- `--json`: print machine-readable output.
- `--plain-progress`: use line-oriented progress.
- `--llm-request-snapshots`: write readable provider request snapshots under the run folder.

## finalize-corpus

```powershell
uv run finalize-corpus
uv run finalize-corpus --dry-run --json
uv run finalize-corpus --run-id 20260504T193003627238Z-playhand-v1
uv run finalize-corpus --attempt-id run-attempt-00012
```

Finalizes the same attempts the dashboard/build-portfolio expect to read. With no arguments, selection scope is `dashboard`: one canonical attempt per run, falling back to the best scored attempt.

Arguments:

- `--run-id`: repeatable run filter.
- `--attempt-id`: repeatable explicit attempt filter.
- `--scope`: `dashboard` or `all`; default `dashboard`.
- `--lookback-months`: profile-drop lookback, default `36`.
- `--profile-drop-workers`: concurrent profile-drop render workers.
- `--profile-drop-timeout-seconds`: per-attempt render/package timeout.
- `--force-rebuild`: rerender profile drops.
- `--allow-presentation-fallback`: allow operational fallback copy when metadata generation fails.
- `--dry-run`: report selected attempts without rendering.
- `--json`: print machine-readable output.

## build-portfolio

```powershell
uv run build-portfolio
uv run build-portfolio --portfolio-config portfolio.config.json
uv run build-portfolio --candidate-scope all --json
```

Builds the automatic portfolio report from the canonical candidate corpus.

Arguments:

- `--run-id`: repeatable run filter.
- `--attempt-id`: repeatable explicit attempt filter.
- `--portfolio-config`: JSON config path.
- `--candidate-scope`: `promoted` or `all`.
- `--catch-up-full-backtests`, `--no-catch-up-full-backtests`: override config.
- `--catch-up-force-rebuild`, `--no-catch-up-force-rebuild`: override config.
- `--catch-up-require-scrutiny-36`, `--no-catch-up-require-scrutiny-36`: override config.
- `--generate-profile-drops`, `--no-generate-profile-drops`: override config.
- `--export-bundle`, `--no-export-bundle`: override config.
- `--profile-drop-workers`: override configured render worker count.
- `--json`: print machine-readable output.

## dashboard

```powershell
uv run dashboard
uv run dashboard --host 127.0.0.1 --port 47832
```

Serves the dashboard SPA. The dashboard reads existing derived artifacts and can start local-only `finalize-corpus` or `build-portfolio` jobs from Portfolio Workbench Auto mode.

Arguments:

- `--host`: bind host, default `0.0.0.0`.
- `--port`: bind port, default `47832`.
- `--limit`, `--force-rebuild`, `--no-refresh-on-start`: legacy no-op compatibility flags.

## record-attempt

```powershell
uv run record-attempt .\runs\manual\evals\candidate-a --run-id manual
```

Scores and appends one artifact directory to a run ledger.

Arguments:

- `artifact_dir`: artifact directory to score.
- `--candidate-name`: optional display/source name.
- `--run-id`: target run id, default `manual`.
- `--profile-ref`: optional cloud profile ref.
- `--note`: optional ledger note.

## nuke-deep-caches

```powershell
uv run nuke-deep-caches
uv run nuke-deep-caches --json
```

Deletes rebuildable deep artifacts so `finalize-corpus` or `build-portfolio` can regenerate them from source. It preserves run ledgers, source artifacts, `runs/`, `runs_archive/`, `sweeps/`, and the main `.venv/`.
