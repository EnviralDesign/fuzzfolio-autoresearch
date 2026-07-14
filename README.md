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
- `build-recipe-priors` builds empirical recipe/slot sampling weights and a Play Hand seed-plan artifact from all Atlas layers, including retained discovered recipe validation results when present.
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

### Play Hand Massive v2

Play Hand Massive v2 is the lab gateway path: in-memory queue, in-memory worker registry, no Redis/Appwrite/backend hot path, and no shards. In the AutoResearch process manager, start **play hand massive v2 - lab gateway** before starting a v2 coordinator.

When the gateway binds outside loopback, it uses `--token`, `FUZZFOLIO_LAB_GATEWAY_TOKEN`, or the per-user `FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE` fallback. Deep-replay v2 runs require `--tasks-per-lane 1`; scale finite campaigns with `--target-runs` and keep worker pressure bounded with `--active-runs`. The legacy `--lanes` flag remains as an alias for `--target-runs`.

The lab gateway defaults are cloud-tolerant: leases stay live for at least `600` seconds, workers are treated stale after `600` seconds without heartbeat, and stale records prune after `1800` seconds. Coordinators default to `--max-attempts 8` so transient Vast worker churn requeues work instead of quickly final-failing useful shards.

When `--instrument-pool-preset` is omitted, v2 uses the existing Play Hand default pool and still allows seed-template instrument narrowing. Named presets can be repeated or comma-separated: `core/default`, `fx`, `fx-major`, `fx-minor`, `metals`, `energies`, `indices`, `crypto`, `commodities`, `cfds`, and `all`. The `all` preset tracks the trimmed FuzzFolio Dukascopy constants set: all FX plus selected metals, energies, indices, and crypto.

Deep-replay worker completions must also be scoreable. If the coordinator cannot score a returned artifact, it records the attempt as failed and the campaign exits failed instead of treating an unusable artifact as a screened result.

By default v2 writes only canonical scoreable artifacts and compact JSON. It does not retain the verbose `lab-result.json`, `lab-worker-result.json`, or `sweep-shard-result.json` debug envelopes unless `--retain-raw-lab-artifacts` is set.

Coordinator stdout defaults to `--log-mode barrier`: bounded ASCII snapshots of gateway pressure and lane state, with explicit failure notices between snapshots. Use `--log-mode stream` when every JSONL event should also be printed with lane/task context, or `--log-mode quiet` when stdout should stay minimal.

Safe starting point:

```powershell
uv run play-hand-massive-v2 --mode finite --task-mode deep_replay --target-runs 128 --active-runs 32 --tasks-per-lane 1 --json
```

Use `--mode continuous --active-runs N` for a run-until-stopped campaign. Continuous mode keeps replacing completed runs until the process is stopped; interrupted lane folders are expected and can be cleaned with the incomplete-run cleanup tool.

Corpus maintenance commands are dry-run by default:

```powershell
uv run cleanup-playhand-lab-raw-artifacts --json
uv run cleanup-playhand-lab-raw-artifacts --yes --json
uv run compact-runs-json --json
uv run compact-runs-json --yes --json
```

The old `play-hand-massive` command is v1 legacy and still uses the FuzzFolio worker gateway. It should not be used for high-scale research runs; keep it only for compatibility while v2 finishes taking over the operational surface.

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

This consumes the static catalog atlas, signal atlas, forward-response atlas, Layer 3 pair results, Layer 3b timing results, and any retained discovered recipe validation rows. It writes weighted recipe-slot menus, negative-pair evidence, validated two-indicator template defaults, and a Play Hand seed plan under `runs/derived/recipe-priors/`. These weights bias future random selection; they do not remove lower-prior or wild-card indicators from exploration. Until a discovered family has 36-month retention evidence, the seed plan uses a conservative `60/25/15` guided/uncertain/wild split instead of the mature `80/15/5` split.

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
uv run build-discovery-recipe-scrutiny-atlas
```

The validation builder expands high/promising discovered recipe templates into a capped concrete queue, defaulting to `12`-month sensitivity probes under `runs/derived/discovery-recipe-validation-atlas/`. The scrutiny builder then turns retained 12-month rows into a smaller `36`-month queue under `runs/derived/discovery-recipe-scrutiny-atlas/` and copies the exact retained source profile documents when available; run it later with `uv run run-discovery-recipe-validation-probes --atlas-dir runs/derived/discovery-recipe-scrutiny-atlas --workers 32`. Re-run `uv run build-recipe-priors` after validation or scrutiny finishes; Play Hand reads the seed plan automatically, carries validated pair defaults into scaffolded profiles, forces guided seed-plan deals to contain at least two indicators, and hard-blocks only severe known pair collapses during slot/fill selection.

From there you can build the portfolio either way:

- In the dashboard, use Portfolio Workbench -> Auto Build. The dashboard writes a dashboard-owned config under `runs/derived/dashboard-portfolio-configs/`, starts a local `build-portfolio` job, shows status/logs, and can import the auto-selected result into the manual draft.
- At the terminal, run `uv run build-portfolio` directly. This uses the same canonical candidate corpus and writes the latest report under `runs/derived/portfolio-report/`.

### Portfolio Research Campaigns

Use `portfolio-research` for formal portfolio selection. Unlike a one-shot `optimize-portfolio` run, it heals stale full backtests without rendering corpus profile drops, fails closed on invalid promoted artifacts, freezes an immutable candidate snapshot, runs deterministic robustness experiments, performs train/embargo/test walk-forward selection, and writes finalist evidence under `runs/derived/portfolio-research/<campaign-id>/`.

Campaign analysis keeps full-window perturbations separate from temporal selections, derives fold-persistent consensus candidates, deduplicates exact and semantic strategy families, and runs Rust/PyO3 behavioral clustering over return, downside, worst-decile, and activity similarity. Consensus-core portfolios are jointly optimized and replayed across available folds, but remain explicitly diagnostic until an untouched outer holdout exists. `no_champion` and `no_defensible_consensus` are valid successful outcomes.

```powershell
# Preview freshness work and experiment count without mutation.
uv run portfolio-research --suite darwin-master-v1 --dry-run --json

# Formal Darwin campaign. The lab gateway must already be available.
uv run portfolio-research `
  --suite darwin-master-v1 `
  --full-backtest-backend lab-gateway `
  --gateway-url http://127.0.0.1:8799 `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --json

# Resume the newest interrupted Darwin campaign from its frozen snapshot.
uv run portfolio-research --suite darwin-master-v1 --resume --json

# Rebuild analysis and human/machine reports without rerunning optimization.
uv run portfolio-research-report --campaign-id latest --json
```

`--skip-catchup` and `--allow-incomplete-corpus` are diagnostic escape hatches. Their campaigns are marked non-promotable. `--experiment-limit` exists for bounded verification and should not be used as the basis for a promotion decision.

The current temporal layer freezes strategy definitions and execution cells, then selects portfolios using train-only calendar slices and scores them on untouched test slices. It does not remove lookahead from strategy generation or execution-cell selection; those require the later nested replay and historical AutoResearch layers recorded in the campaign report.

### Bounded Evidence Materialization

`calculate-full-backtests` can materialize immutable, plan-qualified evidence through the lab gateway. Each profile receives a content-addressed evidence plan with exact start/end bounds, horizon, role, and profile hash. Results are stored under `evidence/full-backtest/<plan-hash>/`; a 60-month or alternate-window result cannot overwrite or satisfy the legacy 36-month files.

### Fixed Existing-Corpus Cohorts

Freeze the compatible existing corpus before requesting five-year evidence. The command verifies the immutable portfolio-research candidate snapshot and its manifest, applies both the current development-universe contract and the authoritative corpus-exclusion ledger, records deterministic exclusions, and creates `runs/derived/fixed-corpus-cohorts/<cohort-id>/cohort.json`. The ledger check matters because a candidate's observed trades can omit retired instruments that were still configured in its attempt. Re-running with the same source succeeds; source, universe, or relevant exclusion drift fails closed.

```powershell
uv run freeze-fixed-corpus-cohort `
  --portfolio-research-campaign 20260710T210709Z-darwin-master-v1 `
  --cohort-id darwin-native-v2 `
  --json

# Use the complete frozen cohort. --scope canonical can narrow it but cannot expand it.
uv run calculate-full-backtests `
  --attempt-cohort runs/derived/fixed-corpus-cohorts/darwin-native-v2/cohort.json `
  --scope matched `
  --horizon-months 60 `
  --evidence-window-start 2021-07-14T00:00:00Z `
  --evidence-window-end 2026-07-14T00:00:00Z `
  --selection-data-end 2026-07-14T00:00:00Z `
  --campaign-plan-id darwin-existing-corpus-60m-v1 `
  --lake-url http://192.168.1.2:8010 `
  --full-backtest-backend lab-gateway `
  --dry-run --json
```

`nested-evidence --attempt-cohort <cohort.json>` uses the same validated IDs and records the cohort manifest ID in its evidence campaign identity. An explicit repeated `--attempt-id` is accepted only when it names the identical cohort.

```powershell
# Preview the frozen canonical cohort without submitting work.
uv run calculate-full-backtests `
  --scope canonical `
  --horizon-months 60 `
  --evidence-window-start 2021-07-08T23:59:59Z `
  --evidence-window-end 2026-07-08T23:59:59Z `
  --selection-data-end 2026-07-08T23:59:59Z `
  --evidence-role full_backtest `
  --campaign-plan-id corpus-60m-v1 `
  --lake-url http://192.168.1.2:8010 `
  --full-backtest-backend lab-gateway `
  --dry-run --json

# Run the same resumable materialization plan.
uv run calculate-full-backtests `
  --scope canonical `
  --horizon-months 60 `
  --evidence-window-start 2021-07-08T23:59:59Z `
  --evidence-window-end 2026-07-08T23:59:59Z `
  --selection-data-end 2026-07-08T23:59:59Z `
  --evidence-role full_backtest `
  --campaign-plan-id corpus-60m-v1 `
  --lake-url http://192.168.1.2:8010 `
  --full-backtest-backend lab-gateway `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --json
```

Use repeated `--attempt-id` arguments instead of `--scope canonical` for a predeclared stable core, portfolio cohort, or audit cohort. The retrospective older-segment pass uses the same command with `--evidence-role retrospective_historical_unseen`, exact older-segment bounds, and the frozen attempt IDs. Real custom evidence requires `--lake-url` or an explicit `--lake-manifest-sha256`; dry runs report `execution_ready: false` when that identity is absent. Custom bounded evidence is rejected on the legacy local executor because that path does not enforce the evidence plan. `--force-rebuild` is also rejected for immutable bounded plans; correct the campaign ID or window instead of overwriting evidence.

Train-only execution-cell selection and frozen-cell OOS evaluation use the resumable `nested-evidence` command. It creates deterministic train tasks, freezes the selected SL/reward cell before outer submission, and stores only redacted tracked-cell outer evidence. Inside each outer training window it then runs multiple inner temporal portfolio selections, derives a structurally deduplicated consensus core, jointly optimizes around that mandatory core using train-only curves, writes the frozen portfolio immutably, and only then evaluates it on outer curves. Weak consensus returns `no_defensible_consensus`; negative outer outcomes are never filtered from the evaluation universe.

Formal runs require the promoted MarketDataLake `coverage_sha256`. The coordinator freezes it into every train and outer plan. Lake list/archive/download APIs reject a changed identity, worker caches are namespaced by it, workers record the observed identity, and materialization rejects results that merely repeat the planned hash without proving what the worker observed.

```powershell
# Current three-year architecture check: two 24m-train/4m-test folds.
uv run nested-evidence `
  --campaign-id darwin-nested-36m-v1 `
  --suite darwin-master-v1 `
  --scope canonical `
  --start 2023-07-08 `
  --end 2026-07-08 `
  --train-months 24 `
  --test-months 4 `
  --step-months 4 `
  --embargo-days 15 `
  --selection-basis recommended_cell `
  --lake-url http://192.168.1.2:8010 `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --optimizer-backend auto `
  --json

# Five-year target geometry after lake coverage is verified.
uv run nested-evidence `
  --campaign-id darwin-nested-60m-v1 `
  --suite darwin-master-v1 `
  --scope canonical `
  --start 2021-07-08 `
  --end 2026-07-08 `
  --train-months 36 `
  --test-months 6 `
  --step-months 6 `
  --embargo-days 15 `
  --lake-url http://192.168.1.2:8010 `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --json
```

Nested execution requires a dedicated/empty lab-gateway result queue, a deployed lake image with expected-coverage enforcement, and workers built from the matching Trading-Dashboard worker contract. Use `--dry-run` to resolve the indexed cohort and outer-fold geometry without contacting the gateway. Inner-fold geometry comes from `temporal_validation.inner_validation` in the selected suite.

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
