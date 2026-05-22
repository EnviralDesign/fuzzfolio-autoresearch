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
- `build-indicator-atlas` builds static indicator atlas, dependency, pair-matrix, and recipe-prior artifacts.
- `build-signal-atlas` runs Layer 1 signal-density/persistence profiling with temporary replay profiles.
- `build-forward-response-atlas` runs Layer 2 post-signal return, MFE/MAE, and volatility-normalized response profiling from signal-atlas raw artifacts.
- `build-anchor-pair-atlas` builds Layer 3 anchor-trigger pair priors, profile documents, and runnable probe manifests.
- `run-anchor-pair-probes` runs and scores the queued Layer 3 anchor-trigger sensitivity probes.
- `build-anchor-pair-timing-atlas` builds Layer 3b trigger `lookbackBars` timing-tolerance variants from completed Layer 3 probes.
- `run-anchor-pair-timing-probes` runs and scores the queued Layer 3b timing-tolerance probes.
- `build-recipe-priors` builds empirical recipe/slot sampling weights and a Play Hand seed-plan artifact from all Atlas layers.
- `build-discovery-pair-atlas` builds a broad ordered-pair discovery queue across generation-eligible indicators.
- `run-discovery-pair-probes` runs and scores the queued broad discovery-pair backend probes.
- `build-discovery-cluster-atlas` distills discovery-pair results into empirical indicator clusters and discovered recipe-template candidates.
- `build-discovery-recipe-validation-atlas` builds a 12-month validation queue from the strongest discovered recipe templates.
- `run-discovery-recipe-validation-probes` runs and scores the discovered recipe validation queue.
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

Play Hand defaults are tuned for long unattended runs: deep-replay jobs can wait up to `2400` seconds and sweeps can wait up to `7200` seconds before the run gives up.

### Indicator Atlas

Before spending backend compute, build the static indicator priors:

```powershell
uv run build-indicator-atlas
```

The command defaults to `C:\repos\Trading-Dashboard` when no workspace is configured and writes:

- `runs/derived/indicator-atlas/indicator-atlas.json`
- `runs/derived/indicator-atlas/indicator-atlas.csv`
- `runs/derived/indicator-atlas/indicator-dependencies.json`
- `runs/derived/indicator-atlas/indicator-pair-matrix.csv`
- `runs/derived/indicator-atlas/recipe-priors.json`

These are catalog priors, not empirical proof. They classify role metadata, implementation mapping, parameter-space size, anchor-trigger compatibility, and recipe-slot candidates so later signal and P&L probes can spend compute on better-conditioned combinations without deleting wild-card exploration.

The next empirical layer is the signal atlas:

```powershell
uv run build-signal-atlas
```

By default it profiles trigger indicators across `EURUSD`, `GBPUSD`, `USDJPY`, and `XAUUSD` on `M5` and `M15`, using Signal Replay score series rather than deep-replay P&L. It passes `source=system` through the Rust CLI so agent/operator atlas runs do not consume interactive replay usage, and writes density, balance, persistence, and spacing metrics under `runs/derived/signal-atlas/`.

Then build the forward-response layer without more backend replay calls:

```powershell
uv run build-forward-response-atlas
```

This reads the raw signal-atlas replay payloads, treats long/short event starts as trigger events, and measures 1/3/6/12/24-bar forward returns, MFE/MAE, and volatility-normalized response. Outputs live under `runs/derived/forward-response-atlas/`.

Then build the Layer 3 anchor-pair queue:

```powershell
uv run build-anchor-pair-atlas
```

This combines the static pair matrix, Layer 1 signal density, and Layer 2 forward-response priors into a ranked anchor-trigger probe queue. It writes profile JSONs and a PowerShell run script under `runs/derived/anchor-pair-atlas/` so the next backend batch can test controlled recipes such as trend anchor + trigger or compression anchor + trigger without hand-assembling profiles.

Run the first queued backend batch with:

```powershell
uv run run-anchor-pair-probes
```

The no-arg runner executes the top queued probes as a starter batch, scores each sensitivity output with the normal AutoResearch scorer, and writes `anchor-pair-probe-results.csv` plus `anchor-pair-probe-summary.json`. Use `--all` when you want the whole queued set.

After a full Layer 3 run, build and run the timing-tolerance layer:

```powershell
uv run build-anchor-pair-timing-atlas
uv run run-anchor-pair-timing-probes
```

Layer 3b keeps the same anchor and trigger pairings, then varies the trigger-side `lookbackBars` over `1`, `2`, and `3` while skipping the catalog baseline. It compares each timing variant against the original Layer 3 result and writes score, trade, signal-count, expectancy, and profit-factor deltas under `runs/derived/anchor-pair-timing-atlas/`. This is the first pass at finding pairs that looked weak only because the two signals were a few bars out of phase.

Then build the empirical recipe-prior layer:

```powershell
uv run build-recipe-priors
```

This consumes the static catalog atlas, signal atlas, forward-response atlas, Layer 3 pair results, and Layer 3b timing results. It writes weighted recipe-slot menus and a Play Hand seed plan under `runs/derived/recipe-priors/`. These weights bias future random selection; they do not remove lower-prior or wild-card indicators from exploration.

For a broader backend-heavy discovery pass, build the ordered-pair queue:

```powershell
uv run build-discovery-pair-atlas
```

This keeps the curated recipe priors, but adds an exploratory queue that considers every ordered pair of generation-eligible indicators on the default `M5`/`M15` panel. The default queue is a diversified `1536`-probe batch: about 25% proven-neighbor, 45% plausible-novel, 20% under-tested role-correct, and 10% wild exploration. Exact Layer 3 retests are marked in the matrix but not queued unless `--include-known-retests` is set. Use `--full` for the larger all-untested ordered-pair queue.

Run the queued backend batch with:

```powershell
uv run run-discovery-pair-probes
```

With no arguments the runner attempts every queued discovery probe, deleting temporary cloud profiles after each run and writing scored results under `runs/derived/discovery-pair-atlas/`. Use `--workers 20` or higher when you want several discovery probes in flight at once so a larger backend worker pool stays busy. Use `--limit` only when you deliberately want a partial run.

After the discovery probes finish, distill the empirical pair corpus:

```powershell
uv run build-discovery-cluster-atlas
```

This does not run more backend jobs. It looks across the completed discovery-pair scores, groups indicators that succeed with similar partners, builds a cluster-to-cluster compatibility matrix, and writes discovered recipe-template candidates under `runs/derived/discovery-cluster-atlas/`. These templates are exploratory evidence for the next Play Hand integration step; they still need 12-month and 36-month validation before being treated as durable recipes.

Then validate the strongest discovered templates over a longer window:

```powershell
uv run build-discovery-recipe-validation-atlas
uv run run-discovery-recipe-validation-probes --workers 32
```

The builder expands high/promising discovered recipe templates into a capped concrete queue, defaulting to `12`-month sensitivity probes under `runs/derived/discovery-recipe-validation-atlas/`. This is the retention gate before feeding discovered recipes back into Play Hand. After that, `build-discovery-pair-atlas --full` is the natural longer nighttime expansion if we want a broader first-generation cluster corpus.

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
