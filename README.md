# fuzzfolio-autoresearch

Auto Research is the local strategy-generation and portfolio-assembly workspace for Fuzzfolio.

The repo now has one product shape:

1. Generate strategy runs.
2. Promote one canonical winner per run.
3. Finalize that winner with 36-month scrutiny/full-backtest artifacts, presentation metadata, and a profile-drop card.
4. Build a portfolio automatically, or assemble one manually in the dashboard from the canonical candidate corpus.

Play Hand is the preferred generator. Explorer is still available through `run` as a legacy advanced generator, but the local model fine-tuning branch has been removed.

## Setup

```powershell
cd C:\repos\fuzzfolio-autoresearch
Copy-Item autoresearch.config.example.json autoresearch.config.json
Copy-Item .agentsecrets.example .agentsecrets
uv run doctor
uv run test-providers
```

### Codex Provider Auth

Codex-backed provider profiles use AutoResearch's dedicated Codex home, not the default Codex CLI home. By default that home is:

```powershell
C:\repos\fuzzfolio-autoresearch\.codex-harness\codex-home
```

Login against that isolated home:

```powershell
$env:CODEX_HOME = "C:\repos\fuzzfolio-autoresearch\.codex-harness\codex-home"
codex login status
codex login --device-auth
Remove-Item Env:\CODEX_HOME
```

Set `AUTORESEARCH_CODEX_HOME` only if you want a different dedicated home. Set `AUTORESEARCH_CODEX_SOURCE_HOME` only for an explicit one-time bootstrap/mirror source; AutoResearch does not copy from the ambient default `CODEX_HOME`.

## Public Commands

- `doctor` checks config, Fuzzfolio CLI access, auth, and seed prompt state.
- `test-providers` smoke-tests configured remote/provider-backed LLM profiles.
- `play-hand` runs the deterministic strategy generator and finalizes the run winner.
- `run` runs the legacy Explorer controller and finalizes the best scored attempt when it exits.
- `finalize-corpus` catches up canonical attempts across existing runs.
- `build-portfolio` builds the automatic portfolio report from canonical candidates.
- `dashboard` opens the manual/auto portfolio workbench and local job controls.
- `record-attempt` manually scores and records an artifact directory.
- `nuke-deep-caches` deletes rebuildable deep artifacts.

See [cli.md](cli.md) for arguments.

## Two Paths

### Path A: Auto Build

```powershell
uv run play-hand
uv run finalize-corpus
uv run dashboard
```

`play-hand` creates a run, promotes a canonical attempt, runs the canonical finalization path, and writes final profile-drop artifacts. `finalize-corpus` catches up older canonical runs.

By default, Play Hand searches reward cells up to `4R`. Use `--max-reward-r 12.5` only when you explicitly want the full legacy high-R grid.

From there you can build the portfolio either way:

- In the dashboard, use Portfolio Workbench -> Auto Build. The dashboard writes a dashboard-owned config under `runs/derived/dashboard-portfolio-configs/`, starts a local `build-portfolio` job, shows status/logs, and can import the auto-selected result into the manual draft.
- At the terminal, run `uv run build-portfolio` directly. This uses the same canonical candidate corpus and writes the latest report under `runs/derived/portfolio-report/`.

### Path B: Manual Assembly

```powershell
uv run play-hand
uv run finalize-corpus
uv run dashboard
```

Open the dashboard Portfolio Workbench, use Manual mode to select canonical candidates, inspect the 36-month basket curve, and persist the draft selection. Auto Build mode is available in the same dashboard when you want the automatic `build-portfolio` path instead.

## Run Contract

Every generator should end with one canonical attempt per run. Canonical metadata is shared across Play Hand, Explorer, dashboard, and portfolio building:

- `runner`
- `attempt_role`
- `attempt_decision`
- `attempt_decision_reasons`
- `strategy_family_id`
- `canonical_attempt_id`
- `is_canonical_attempt`

Play Hand compatibility aliases remain in existing ledgers, but new readers should prefer the universal fields.

## Finalization

`finalize-corpus` is the catch-up command for dashboard/build-portfolio readiness. With no arguments it uses the same selection logic as the dashboard: one canonical attempt per run when present, otherwise the best scored attempt in that run.

Finalization prepares:

- the selected attempt's 36-month full backtest
- presentation metadata when a writer profile is configured
- attempt-local `profile-drop-36mo.png`
- refreshed derived corpus/catalog artifacts

Presentation metadata failures mark finalization incomplete by default. Use `--allow-presentation-fallback` only when ugly operational copy is acceptable.

## Dashboard Jobs

The dashboard exposes local-only job endpoints for desktop automation:

- `POST /api/jobs/finalize-corpus`
- `POST /api/jobs/build-portfolio`
- `GET /api/jobs/current`
- `GET /api/jobs/{id}`
- `POST /api/jobs/cancel`

Jobs run one at a time through the same direct `uv run finalize-corpus` and `uv run build-portfolio` commands used at the terminal. You do not need a separate AutoResearch worker process; `uv run dashboard` owns these local job subprocesses. Records and logs are written under `runs/derived/dashboard-jobs/`. Dashboard-owned portfolio configs are stored under `runs/derived/dashboard-portfolio-configs/`.

## Legacy Explorer

`uv run run` remains available for Explorer runs. Treat it as advanced/legacy. It no longer supports direct local Hugging Face/Gemma inference, local adapters, quantization settings, or fine-tuning/export workflows.

## Do Not Delete

The cleanup commands and docs assume these folders are part of the current working corpus:

- `runs/`
- `runs_archive/`
- `sweeps/`
- `.venv/`
- portfolio configs
- current derived corpus artifacts unless explicitly using `nuke-deep-caches`
