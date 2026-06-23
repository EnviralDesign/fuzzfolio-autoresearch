# Autoresearch CLI Reference

Run commands from `C:\repos\fuzzfolio-autoresearch` with `uv run <command>`.

## doctor

```powershell
uv run doctor
uv run doctor --json
```

Checks local config, CLI access, auth, and seed prompt state.

For Codex-backed profiles, the JSON output includes `*_codex_home` and `*_codex_login_command`. Use that command to log into AutoResearch's isolated Codex home; a normal `codex login` without `CODEX_HOME` targets the default Codex home and is intentionally not used by AutoResearch.

## test-providers

```powershell
uv run test-providers
uv run test-providers --profile codex-54-mini
uv run test-providers --json
```

Arguments:

- `--profile`: repeatable provider profile filter.
- `--json`: print machine-readable output.

Codex profile results include `codex_home` and `codex_login_command`. If auth fails, log into that dedicated home and rerun the same `test-providers` command.

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
- `--max-reward-r`, `--max-r`: cap the searched reward/R cells. Default is `4`; pass `12.5` to opt into the full legacy 25-column grid.
- `--coarse-mode`: `deterministic` or `evolutionary`.
- `--no-instrument-scout`: skip late cross-instrument scouting.
- `--no-final-artifacts`: skip canonical finalization at wrap-up.
- `--final-profile-drop-count`: positive values finalize the canonical run winner; `0` skips final profile-drop rendering.
- `--final-profile-drop-workers`: final profile-drop worker count.
- `--job-timeout-seconds`: deep-replay job wait budget for Play Hand evaluations. Default `2400`.
- `--sweep-timeout-seconds`: sweep wait budget before Play Hand gives up on a sweep. Default `7200`.
- `--dry-run`: write the run/plan without backend compute.
- `--json`: print a JSON summary.

## play-hand-massive-v2

```powershell
uv run play-hand-massive-v2-gateway
uv run play-hand-massive-v2 --mode finite --task-mode deep_replay --target-runs 128 --active-runs 32 --tasks-per-lane 1 --json
uv run play-hand-massive-v2 --mode continuous --task-mode deep_replay --active-runs 64 --tasks-per-lane 1 --json
uv run play-hand-massive-v2-sim --workers 1000 --json
uv run play-hand-massive-v2-ws-sim --workers 1000 --json
```

Runs first-class Play Hand Massive v2 work through the in-memory lab gateway. This is the preferred scalable path for author research: no Redis/Appwrite/backend hot path and no shards.

The `play-hand-lab*` command names remain supported aliases while the v2 naming settles.

Common arguments:

- `--gateway-url`, `--gateway-token`: lab gateway connection details.
- `--token`: gateway bearer token. Non-loopback gateway binds auto-create/read `FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE` when neither `--token` nor `FUZZFOLIO_LAB_GATEWAY_TOKEN` is set.
- `--lease-ttl-seconds`, `--worker-stale-after-seconds`, `--worker-prune-after-seconds`: lab gateway lifecycle knobs. Defaults are `600`, `600`, and `1800` for cloud-worker tolerance.
- `--mode`: `finite` drains and exits after `--target-runs`; `continuous` replaces completed runs until stopped.
- `--task-mode`: `deep_replay` or `fake_compute`.
- `--target-runs`: total candidate run folders to create in finite mode. `--lanes` remains a compatibility alias.
- `--active-runs`: candidate runs kept in flight at once.
- `--tasks-per-lane`: replay tasks queued for each lane.
- `--max-attempts`: gateway lease attempts before a task is final-failed. Default: `8`.
- `--instrument-pool-preset`: named pool preset to deal across lanes. Repeat or comma-separate values such as `fx,metals`, `crypto`, or `all`.
- `--instrument-pool`: explicit comma-separated or repeatable instruments to add to the resolved pool.
- `--min-indicators`, `--max-indicators`: generated profile width.
- `--retain-raw-lab-artifacts`: keep verbose lab debug envelopes. By default v2 writes only canonical scoreable artifacts.
- `--log-mode`: `barrier` prints bounded ASCII lane snapshots plus explicit failure notices; `stream` prints every event with lane/task context; `quiet` suppresses event chatter. Default: `barrier`.
- `--barrier-interval-seconds`, `--barrier-lane-limit`: tune snapshot cadence and visible lane rows.
- `--dry-run`: write campaign/lane folders without enqueuing work.
- `--json`: print a machine-readable summary after coordinator logs.

## play-hand-massive legacy

```powershell
uv run play-hand-massive
uv run play-hand-massive --active-lanes 2 --sweep-budget low --target-worker-slots-per-lane 32 --adaptive-lanes
uv run play-hand-massive --dry-run --json
```

Runs the v1 backlog-oriented multi-lane Play Hand campaign through the FuzzFolio worker gateway. This path is legacy and should not be used for high-scale author research runs.

Defaults are conservative: `--active-lanes 2`, `--sweep-budget low`, `--target-worker-slots-per-lane 32`, `--adaptive-lanes` enabled. Adaptive mode fails closed when worker-gateway telemetry is unavailable unless `--adaptive-fail-open` is passed.

Common arguments:

- `--lanes`: total independent lanes. Default `12`.
- `--active-lanes`: max concurrent lanes during expand sweeps. Default `2`.
- `--sweep-budget`: `low`, `medium`, or `high`. Default `low`.
- `--adaptive-lanes` / `--no-adaptive-lanes`: gateway-aware lane window. Default enabled.
- `--adaptive-fail-open`: legacy fail-open adaptive behavior when gateway polls fail.
- `--staged-campaign` / `--no-staged-campaign`: rolling staged pipeline — baseline/scaffold lanes and expansion/sweep lanes run concurrently; each lane enters sweeps as soon as it passes baseline. Default enabled.
- `--scaffold-active-lanes`: concurrency cap for baseline/scaffold work only (not reduced by `--target-worker-slots-per-lane`). Default `2`.
- `--target-worker-slots-per-lane`: adaptive cap for expansion/sweep lane concurrency only. Worker count affects throughput, not whether sweeps start.
- `--remote-token-budget-multiplier`: remote permutation budget = healthy worker slots × multiplier. Default `2`.
- `--gateway-url`, `--gateway-token`, `--gateway-pool`: adaptive telemetry against the worker gateway.
- `--dry-run`: write campaign/lane folders without backend compute.
- `--json`: print machine-readable summary.

When infrastructure fails mid-campaign, unstarted lanes are reported as `not_started_backend_down` or `not_started_gateway_unhealthy` rather than failed strategy lanes.

## build-indicator-atlas

```powershell
uv run build-indicator-atlas
uv run build-indicator-atlas --json
uv run build-indicator-atlas --workspace-root C:\repos\Trading-Dashboard
```

Builds the first static Indicator Atlas layer from the Fuzzfolio catalog and implementation factory. With no arguments it uses the configured Fuzzfolio workspace root, `AUTORESEARCH_FUZZFOLIO_WORKSPACE_ROOT`, or `C:\repos\Trading-Dashboard`.

Outputs are written to `runs/derived/indicator-atlas/` by default:

- `indicator-atlas.json` and `indicator-atlas.csv`: per-indicator role metadata, implementation mapping, parameter cardinality, sweepable parameters, and static prior buckets.
- `indicator-dependencies.json`: base-indicator, implementation-class, TA function, namespace, role, and strategy groupings.
- `indicator-pair-matrix.csv`: static anchor-trigger compatibility priors for the first Trigger Atlas probes.
- `recipe-priors.json`: recipe-slot candidate priors and the 80/15/5 guided/wild sampling policy.

Arguments:

- `--workspace-root`: Trading-Dashboard workspace root.
- `--catalog-path`: direct path to `shared/constants/indicators.json`; overrides `--workspace-root`.
- `--out-dir`: output directory; default `runs/derived/indicator-atlas`.
- `--json`: print machine-readable output.

## build-signal-atlas

```powershell
uv run build-signal-atlas
uv run build-signal-atlas --max-indicators 3 --bar-limit 500 --json
uv run build-signal-atlas --indicator WICK_REJECTION --indicator BREAKOUT_FIRST_CLOSE
```

Builds Layer 1 signal behavior data from Signal Replay, not deep-replay P&L. The command creates temporary catalog-derived profiles, runs `replay simulate` for each selected indicator/timeframe/instrument cell, summarizes the long/short score series, and deletes the temporary profiles unless `--keep-profiles` is set.

Default panel:

- indicators: catalog `signalRole=trigger`
- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- timeframes: `M5`, `M15`
- bars per replay: `5000`
- replay source: `system` so agent/operator atlas runs do not consume interactive Signal Replay usage

Outputs are written to `runs/derived/signal-atlas/` by default:

- `signal-atlas.json`: full row-level signal metrics and rollups.
- `signal-atlas.csv`: compact per indicator/instrument/timeframe rows.
- `signal-atlas-summary.json`: run summary and per-indicator rollups.
- `signal-atlas-issues.csv`: flat, very sparse, saturated, one-sided, or failed cells.
- `request-manifest.json`: temporary profile and raw replay artifact bookkeeping.

Arguments:

- `--indicator`: explicit indicator id; repeatable. Overrides `--signal-role` selection.
- `--signal-role`: role filter when `--indicator` is omitted; default `trigger`.
- `--instrument`: instrument panel member; repeatable.
- `--timeframe`: timeframe panel member; repeatable.
- `--bar-limit`: bars per replay simulation; default `5000`.
- `--replay-source`: Signal Replay source passed through the Rust CLI; default `system`.
- `--max-indicators`: cap selected indicators after static-prior sorting.
- `--refresh-static-atlas`: rebuild the static atlas before selection.
- `--keep-profiles`: keep temporary cloud profiles for manual inspection.
- `--timeout-seconds`: per CLI call timeout.
- `--json`: print machine-readable output.

## build-forward-response-atlas

```powershell
uv run build-forward-response-atlas
uv run build-forward-response-atlas --horizon 3 --horizon 12 --json
```

Builds Layer 2 forward-response data from existing `runs/derived/signal-atlas/raw/*.json` replay payloads. It does not call the Fuzzfolio backend. The command treats long/short event starts as trigger events and measures:

- forward return after each configured horizon
- max favorable excursion
- max adverse excursion
- MFE > MAE rate
- volatility-normalized return using pre-event close-return volatility

Default settings:

- input: `runs/derived/signal-atlas/`
- output: `runs/derived/forward-response-atlas/`
- horizons: `1`, `3`, `6`, `12`, `24` bars
- volatility lookback: `48` bars
- minimum events for directional buckets: `30`

Outputs:

- `forward-response-atlas.json`: full Layer 2 artifact.
- `forward-response-atlas.csv`: per indicator/instrument/timeframe/direction/horizon rows.
- `forward-response-rollups.csv`: per indicator/direction/horizon rollups.
- `forward-response-priors.csv`: one row per indicator with aggregate response, best conditional cell, strong-cell count, and prior bucket.
- `forward-response-issues.csv`: no-event, low-sample, or negative-response review flags.

Arguments:

- `--signal-atlas-dir`: input signal-atlas directory.
- `--out-dir`: output directory.
- `--horizon`: forward horizon in bars; repeatable.
- `--vol-lookback`: pre-event volatility lookback in bars.
- `--min-events`: minimum sample count before assigning directional buckets.
- `--threshold`: active-score threshold; default `>0`.
- `--json`: print machine-readable output.

## build-anchor-pair-atlas

```powershell
uv run build-anchor-pair-atlas
uv run build-anchor-pair-atlas --max-pairs 12 --json
uv run build-anchor-pair-atlas --anchor MA_SLOPE_TREND --trigger WICK_REJECTION
```

Builds Layer 3 anchor-trigger pair priors from the existing static, signal, and forward-response Atlas artifacts. This command does not run the backend probes itself; it writes the ranked queue, profile JSONs, a machine-readable run manifest, and a PowerShell script that creates the queued profiles with the robot auth profile and runs `sensitivity-basket`.

Default settings:

- input: `runs/derived/indicator-atlas/`, `runs/derived/signal-atlas/`, and `runs/derived/forward-response-atlas/`
- output: `runs/derived/anchor-pair-atlas/`
- anchors: the static atlas anchor set
- triggers: all catalog `signalRole=trigger` indicators
- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- probe timeframes: `M5`, `M15`
- queued probes: `48`
- sensitivity lookback embedded in the manifest: `3` months

Outputs:

- `anchor-pair-atlas.json`: full Layer 3 artifact.
- `anchor-pair-matrix.csv`: all scored anchor-trigger-timeframe rows.
- `anchor-pair-queue.csv`: prioritized probe queue.
- `profiles/*.json`: temporary pair profile documents for queued probes.
- `anchor-pair-run-manifest.json`: create-profile and sensitivity-basket arguments.
- `run-anchor-pair-probes.ps1`: runnable backend batch script for the queued probes.
- `anchor-pair-summary.json`: compact summary and top queued probes.

Arguments:

- `--indicator-atlas-dir`, `--signal-atlas-dir`, `--forward-response-dir`: input artifact directories.
- `--out-dir`: output directory.
- `--workspace-root`, `--catalog-path`: Trading-Dashboard catalog overrides.
- `--refresh-static-atlas`: rebuild the static atlas first.
- `--anchor`: anchor indicator id; repeatable.
- `--trigger`: trigger indicator id; repeatable.
- `--instrument`: instrument panel member; repeatable.
- `--timeframe`: probe timeframe; repeatable.
- `--max-triggers`: cap trigger candidates after prior sorting.
- `--max-pairs`: cap queued probes.
- `--lookback-months`: sensitivity-basket lookback months embedded in the run manifest.
- `--quality-score-preset`: sensitivity quality preset embedded in the run manifest.
- `--execution-cost-mode`: sensitivity execution-cost mode embedded in the run manifest.
- `--no-profile-docs`: skip writing profile JSONs.
- `--json`: print machine-readable output.

## run-anchor-pair-probes

```powershell
uv run run-anchor-pair-probes
uv run run-anchor-pair-probes --limit 1 --json
uv run run-anchor-pair-probes --probe-id l3-001-rsi-mean-reversion-channel-reentry-m5
uv run run-anchor-pair-probes --all
```

Runs queued Layer 3 pair probes from `runs/derived/anchor-pair-atlas/anchor-pair-atlas.json`. For each selected probe, the command creates the temporary pair profile with the configured robot auth profile, runs `sensitivity-basket`, scores the output with AutoResearch's normal sensitivity scorer, and deletes the temporary cloud profile unless `--keep-profiles` is set.

Default settings:

- input: `runs/derived/anchor-pair-atlas/`
- selected probes: top `8` queued probes
- per-probe timeout: `2400` seconds
- existing `sensitivity-response.json` outputs are reused unless `--force` is set

Outputs:

- `anchor-pair-probe-results.csv`: one row per executed or reused probe with prior score, run status, composite score, and output directory.
- `anchor-pair-probe-summary.json`: compact run summary and top scored probes.
- per-probe sensitivity artifacts under `probe-results/<probe-id>/`.

Arguments:

- `--atlas-dir`: anchor-pair atlas directory.
- `--probe-id`: queued probe id to run; repeatable.
- `--limit`: number of queued probes to run when `--probe-id` is omitted.
- `--all`: run the whole queue.
- `--force`: rerun probes even if an output already exists.
- `--keep-profiles`: keep temporary cloud profiles.
- `--timeout-seconds`: per sensitivity-basket timeout.
- `--json`: print machine-readable output.

## build-anchor-pair-timing-atlas

```powershell
uv run build-anchor-pair-timing-atlas
uv run build-anchor-pair-timing-atlas --lookback-bars 2 --lookback-bars 3 --json
uv run build-anchor-pair-timing-atlas --base-probe-id l3-001-rsi-mean-reversion-channel-reentry-m5
```

Builds Layer 3b timing-tolerance variants from a completed Layer 3 anchor-pair run. The default pass reads `runs/derived/anchor-pair-atlas/`, uses the existing baseline scores from `anchor-pair-probe-results.csv`, and writes trigger-side `lookbackBars` variants for the whole queued set.

Default settings:

- input: `runs/derived/anchor-pair-atlas/`
- output: `runs/derived/anchor-pair-timing-atlas/`
- base probes: all queued Layer 3 probes with baseline results
- trigger lookback bars: `1`, `2`, `3`, skipping the catalog baseline unless `--include-baseline` is set
- sensitivity lookback embedded in the manifest: `3` months

Outputs:

- `anchor-pair-timing-atlas.json`: full Layer 3b artifact.
- `anchor-pair-timing-queue.csv`: queued timing variants with baseline score fields.
- `profiles/*.json`: temporary timing profile documents for queued probes.
- `anchor-pair-timing-run-manifest.json`: create-profile and sensitivity-basket arguments.
- `run-anchor-pair-timing-probes.ps1`: runnable backend batch script for the timing queue.
- `anchor-pair-timing-build-summary.json`: compact build summary.

Arguments:

- `--anchor-pair-atlas-dir`: input anchor-pair atlas directory.
- `--out-dir`: output directory.
- `--workspace-root`, `--catalog-path`: Trading-Dashboard catalog overrides.
- `--base-probe-id`: base Layer 3 probe id; repeatable.
- `--limit-base-pairs`: cap base probes for a narrower run.
- `--lookback-bars`: trigger `lookbackBars` value; repeatable.
- `--include-baseline`: rerun variants whose `lookbackBars` value matches the catalog baseline.
- `--lookback-months`: sensitivity-basket lookback months embedded in the run manifest.
- `--quality-score-preset`: sensitivity quality preset embedded in the run manifest.
- `--execution-cost-mode`: sensitivity execution-cost mode embedded in the run manifest.
- `--no-profile-docs`: skip writing profile JSONs.
- `--json`: print machine-readable output.

## run-anchor-pair-timing-probes

```powershell
uv run run-anchor-pair-timing-probes
uv run run-anchor-pair-timing-probes --limit 5
uv run run-anchor-pair-timing-probes --timing-probe-id l3b-001-l3-001-rsi-mean-reversion-channel-reentry-m5-tr-lb2
```

Runs queued Layer 3b timing variants from `runs/derived/anchor-pair-timing-atlas/anchor-pair-timing-atlas.json`. Unlike the base Layer 3 runner, the no-arg default runs the whole timing queue because this layer is meant to produce a complete baseline-vs-timing comparison cache.

Outputs:

- `anchor-pair-timing-results.csv`: one row per timing variant with baseline metrics, variant metrics, deltas, and timing bucket.
- `anchor-pair-timing-summary.json`: compact run summary, top scored variants, top improvements, rescued positives, and best variant by base probe.
- per-probe sensitivity artifacts under `probe-results/<timing-probe-id>/`.

Arguments:

- `--atlas-dir`: anchor-pair timing atlas directory.
- `--timing-probe-id`: queued timing probe id to run; repeatable.
- `--limit`: number of timing probes to run when `--timing-probe-id` is omitted.
- `--force`: rerun probes even if an output already exists.
- `--keep-profiles`: keep temporary cloud profiles.
- `--timeout-seconds`: per sensitivity-basket timeout.
- `--json`: print machine-readable output.

## build-recipe-priors

```powershell
uv run build-recipe-priors
uv run build-recipe-priors --max-slot-candidates 25 --json
```

Builds empirical recipe and slot sampling weights from the completed Atlas layers. This is the bridge from blind random indicator selection toward recipe-aware weighted selection: high-evidence indicators get more sampling weight, uncertain indicators stay available, and wild-card exploration is preserved. When discovered recipe validation results are present, retained discovered recipe pairs are folded into the same Play Hand seed plan as additional weighted recipes, including the validated profile defaults that worked in the probe.

Default settings:

- input: `runs/derived/indicator-atlas/`, `runs/derived/signal-atlas/`, `runs/derived/forward-response-atlas/`, `runs/derived/anchor-pair-atlas/`, `runs/derived/anchor-pair-timing-atlas/`, and, when present, `runs/derived/discovery-recipe-validation-atlas/` plus `runs/derived/discovery-recipe-scrutiny-atlas/`
- output: `runs/derived/recipe-priors/`
- max slot candidates per recipe slot: `40`
- max empirical pair candidates: `80`
- sampling policy: `60/25/15` guided/uncertain/wild until 36-month retained evidence exists, then `80/15/5`

Outputs:

- `recipe-priors.json`: full empirical recipe-prior artifact.
- `slot-indicator-priors.csv`: one row per recipe/slot/indicator candidate with sampling weight and evidence fields.
- `pair-priors.csv`: empirical anchor-trigger pair menu with score and timing policy.
- `pair-negative-priors.csv`: exact failed pair/timeframe evidence; severe 3m-positive validation collapses become hard unordered avoid-pairs for Play Hand slot/fill selection.
- `cluster-expansion-negative-priors.csv`: failed cluster-expansion families with tested, retained, failure, retained-rate, and soft-penalty fields for later downweighting.
- `retention-failures.csv`: 3-month positives that collapsed during validation.
- `play-hand-seed-plan.json`: compact weighted menu intended for Play Hand integration.
- `recipe-priors-summary.json`: compact counts, top slot menus, and top pair priors.

Arguments:

- `--indicator-atlas-dir`, `--signal-atlas-dir`, `--forward-response-dir`: input artifact directories.
- `--anchor-pair-dir`, `--anchor-pair-timing-dir`: input Layer 3 artifact directories.
- `--discovery-recipe-validation-dir`: optional validated discovered recipe input directory.
- `--out-dir`: output directory.
- `--max-slot-candidates`: maximum candidates kept per recipe slot.
- `--max-pair-candidates`: maximum empirical pair rows kept.
- `--json`: print machine-readable output.

## build-discovery-pair-atlas

```powershell
uv run build-discovery-pair-atlas
uv run build-discovery-pair-atlas --max-pairs 3000 --json
uv run build-discovery-pair-atlas --full
```

Builds the broad ordered-pair discovery layer. Unlike `build-anchor-pair-atlas`, this is not limited to a small curated anchor set. It scores every ordered pair of generation-eligible indicators for the selected timeframes, marks exact Layer 3 retests, then writes a diversified backend queue.

Default settings:

- input: `runs/derived/indicator-atlas/`, `runs/derived/signal-atlas/`, `runs/derived/forward-response-atlas/`, `runs/derived/recipe-priors/`, and `runs/derived/anchor-pair-atlas/`
- output: `runs/derived/discovery-pair-atlas/`
- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- timeframes: `M5`, `M15`
- max queued probes: `1536`
- lane mix: 25% proven-neighbor, 45% plausible-novel, 20% under-tested role-correct, 10% wild diversity
- exact known Layer 3 retests: excluded from the queue by default

Outputs:

- `discovery-pair-atlas.json`: full discovery-pair artifact with matrix rows, queue rows, and run manifest.
- `discovery-pair-matrix.csv`: one row per ordered pair/timeframe candidate.
- `discovery-pair-queue.csv`: selected backend queue.
- `discovery-pair-run-manifest.json`: CLI command manifest.
- `run-discovery-pair-probes.ps1`: direct PowerShell runner.
- `profiles/*.json`: temporary two-indicator profile documents for queued probes.
- `discovery-pair-summary.json`: compact counts and top queue rows.

Arguments:

- `--indicator-atlas-dir`, `--signal-atlas-dir`, `--forward-response-dir`: input artifact directories.
- `--recipe-priors-dir`, `--anchor-pair-dir`: empirical prior and known-pair directories.
- `--out-dir`: output directory.
- `--workspace-root`, `--catalog-path`: Trading-Dashboard catalog resolution overrides.
- `--refresh-static-atlas`: rebuild the static indicator atlas first.
- `--first`, `--second`: restrict the ordered pair sides by indicator id; repeatable.
- `--instrument`, `--timeframe`: override the panel; repeatable.
- `--max-pairs`: bounded queue size when `--full` is not used.
- `--full`: queue every eligible ordered pair/timeframe, excluding exact known retests unless requested.
- `--include-known-retests`: allow exact Layer 3 pairs into the runnable queue.
- `--random-seed`: deterministic diversity lane ordering.
- `--lookback-months`, `--quality-score-preset`, `--execution-cost-mode`: sensitivity-basket manifest settings.
- `--no-profile-docs`: skip writing runnable profile JSON documents.
- `--json`: print machine-readable output.

## run-discovery-pair-probes

```powershell
uv run run-discovery-pair-probes
uv run run-discovery-pair-probes --limit 100
uv run run-discovery-pair-probes --workers 32
uv run run-discovery-pair-probes --probe-id dp-0001-rsi-mean-reversi-toby-crabel-narr-m5
```

Runs and scores the queued broad discovery-pair backend probes. With no arguments it runs every queued probe, so use `--limit` only for an intentional partial pass.

Outputs:

- `discovery-pair-probe-results.csv`: one row per completed or skipped probe with backend score, trade, signal, expectancy, and profit-factor fields.
- `discovery-pair-probe-summary.json`: compact run summary, lane-level scoring counts, top scored pairs, and top scored pairs by lane.
- per-probe sensitivity artifacts under `probe-results/<probe-id>/`.

Arguments:

- `--atlas-dir`: discovery-pair atlas directory.
- `--probe-id`: queued probe id to run; repeatable.
- `--limit`: number of queued probes to run when `--probe-id` is omitted; default is all.
- `--force`: rerun probes even if an output already exists.
- `--keep-profiles`: keep temporary cloud profiles.
- `--timeout-seconds`: per sensitivity-basket timeout.
- `--job-timeout-seconds`: deep replay job wait timeout passed to `sensitivity-basket`.
- `--workers`: number of discovery probes to keep in flight concurrently.
- `--json`: print machine-readable output.

## build-discovery-cluster-atlas

```powershell
uv run build-discovery-cluster-atlas
uv run build-discovery-cluster-atlas --min-positive-score 55 --max-recipes 50
uv run build-discovery-cluster-atlas --json
```

Builds the offline clustering layer from completed discovery-pair backend results. The command groups indicators by empirical behavior: which partners they scored well with, which partner strategy roles worked, and which timeframes/lane types produced positives. It then builds a cluster-pair matrix and emits discovered recipe-template candidates.

This command does not run Fuzzfolio backend jobs. It consumes the backend evidence already cached by `run-discovery-pair-probes`.

Default settings:

- input: `runs/derived/discovery-pair-atlas/`
- output: `runs/derived/discovery-cluster-atlas/`
- positive pair threshold: `50`
- strong pair threshold: `70`
- minimum cluster similarity: `0.22`
- maximum discovered recipes: `32`

Outputs:

- `discovery-cluster-atlas.json`: full cluster artifact with first-side clusters, second-side clusters, cluster-pair rows, and recipes.
- `indicator-clusters.csv`: compact cluster membership and evidence rollups.
- `indicator-success-signatures.csv`: per-indicator success vectors.
- `cluster-pair-matrix.csv`: empirical cluster-to-cluster compatibility rows.
- `discovered-recipes.json`: recipe-template candidates for review and later Play Hand integration.
- `discovery-cluster-summary.json`: compact counts and top recipes.

Arguments:

- `--discovery-pair-dir`: input discovery-pair atlas directory.
- `--out-dir`: output directory.
- `--min-positive-score`: score threshold for positive evidence.
- `--strong-score`: score threshold for strong evidence.
- `--min-similarity`: behavioral similarity needed to join an existing cluster.
- `--min-shared-partners`: minimum shared successful partners needed for cluster assignment.
- `--max-recipes`: maximum discovered recipe templates to emit.
- `--json`: print machine-readable output.

## build-discovery-recipe-validation-atlas

```powershell
uv run build-discovery-recipe-validation-atlas
uv run build-discovery-recipe-validation-atlas --confidence high_candidate --max-pairs-per-recipe 12
uv run build-discovery-recipe-validation-atlas --lookback-months 36 --max-recipes 3
```

Builds the validation bridge from discovered cluster recipes to longer backend evidence. The command reads `discovered-recipes.json`, keeps high/promising recipe templates by default, expands each template into a capped set of concrete indicator pairs, and writes runnable 12-month sensitivity profiles.

This command does not run backend jobs. Use `run-discovery-recipe-validation-probes` after reviewing the queue.

Default settings:

- input: `runs/derived/discovery-cluster-atlas/`
- output: `runs/derived/discovery-recipe-validation-atlas/`
- included confidence buckets: `high_candidate`, `promising_candidate`
- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- timeframes: each recipe's top empirical timeframes
- max recipes: `8`
- max pairs per recipe: `8`
- validation lookback: `12` months

Outputs:

- `discovery-recipe-validation-atlas.json`: full validation queue artifact and run manifest.
- `discovery-recipe-validation-queue.csv`: concrete recipe/pair/timeframe validation rows.
- `discovery-recipe-validation-run-manifest.json`: create-profile and sensitivity-basket arguments.
- `run-discovery-recipe-validation-probes.ps1`: direct PowerShell runner.
- `profiles/*.json`: temporary two-indicator validation profiles.
- `discovery-recipe-validation-summary.json`: compact queue summary.

Arguments:

- `--cluster-atlas-dir`: input discovery-cluster atlas directory.
- `--out-dir`: output directory.
- `--workspace-root`, `--catalog-path`: Trading-Dashboard catalog resolution overrides.
- `--refresh-static-atlas`: rebuild the static indicator atlas before constructing profiles.
- `--confidence`: discovered recipe confidence bucket to include; repeatable.
- `--instrument`, `--timeframe`: override the panel; repeatable.
- `--max-recipes`: maximum discovered recipes to expand.
- `--max-pairs-per-recipe`: maximum concrete pairs queued per recipe.
- `--first-member-limit`, `--second-member-limit`: cluster members considered before queue capping.
- `--lookback-months`: validation lookback months embedded in the run manifest.
- `--job-timeout-seconds`: deep replay job wait timeout embedded in the run manifest.
- `--quality-score-preset`, `--execution-cost-mode`: sensitivity-basket manifest settings.
- `--no-profile-docs`: skip writing runnable profile JSON documents.
- `--json`: print machine-readable output.

## run-discovery-recipe-validation-probes

```powershell
uv run run-discovery-recipe-validation-probes
uv run run-discovery-recipe-validation-probes --workers 32
uv run run-discovery-recipe-validation-probes --limit 5
uv run run-discovery-recipe-validation-probes --probe-id drv-0001-r002-toby-crabel-narrow-rsi-crossback-m5
```

Runs and scores the queued discovered recipe validation probes. With no arguments it attempts every queued validation probe. The default queue is intentionally much smaller than the broad discovery-pair run, because this step is checking 12-month retention for discovered structures before they affect Play Hand.

Outputs:

- `discovery-recipe-validation-results.csv`: one row per completed or skipped validation probe.
- `discovery-recipe-validation-results-summary.json`: compact status, retention, and top-score summary.
- per-probe sensitivity artifacts under `probe-results/<probe-id>/`.

Arguments:

- `--atlas-dir`: discovery recipe validation atlas directory.
- `--probe-id`: queued validation probe id to run; repeatable.
- `--limit`: number of queued probes to run when `--probe-id` is omitted.
- `--force`: rerun probes even if an output already exists.
- `--keep-profiles`: keep temporary cloud profiles.
- `--timeout-seconds`: per sensitivity-basket timeout.
- `--job-timeout-seconds`: deep replay job wait timeout passed to `sensitivity-basket`.
- `--workers`: number of validation probes to keep in flight concurrently.
- `--json`: print machine-readable output.

## build-discovery-recipe-scrutiny-atlas

```powershell
uv run build-discovery-recipe-scrutiny-atlas
uv run build-discovery-recipe-scrutiny-atlas --max-rows 6 --json
uv run build-discovery-recipe-scrutiny-atlas --bucket retained_strong
```

Builds the 36-month scrutiny queue for discovered recipes that already retained during the 12-month validation pass. It does not run backend jobs; it writes another validation-compatible atlas so the existing runner can execute it with `--atlas-dir`. When the source validation profile exists, the builder copies that exact profile document and only rewrites the profile name/description, so scrutiny tests the retained 12-month profile rather than a freshly regenerated catalog-default profile.

Default settings:

- input: `runs/derived/discovery-recipe-validation-atlas/discovery-recipe-validation-results.csv`
- output: `runs/derived/discovery-recipe-scrutiny-atlas/`
- included source buckets: `retained_strong`, `retained`
- instruments: `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- scrutiny lookback: `36` months

Outputs:

- `discovery-recipe-validation-atlas.json`: validation-compatible 36-month queue artifact.
- `discovery-recipe-validation-queue.csv`: retained pair rows queued for scrutiny.
- `discovery-recipe-validation-run-manifest.json`: create-profile and sensitivity-basket arguments.
- `run-discovery-recipe-scrutiny-probes.ps1`: direct PowerShell runner.
- `profiles/*.json`: temporary two-indicator scrutiny profiles.
- `discovery-recipe-scrutiny-summary.json`: compact scrutiny queue summary.

Arguments:

- `--validation-atlas-dir`: input 12-month validation atlas directory.
- `--out-dir`: output directory.
- `--workspace-root`, `--catalog-path`: Trading-Dashboard catalog resolution overrides.
- `--refresh-static-atlas`: rebuild the static indicator atlas before constructing profiles.
- `--bucket`: 12-month retention bucket to promote; repeatable.
- `--instrument`, `--timeframe`: override the panel or restrict retained rows; repeatable.
- `--max-rows`: maximum retained validation rows to queue.
- `--lookback-months`: scrutiny lookback months embedded in the run manifest.
- `--job-timeout-seconds`: deep replay job wait timeout embedded in the run manifest.
- `--quality-score-preset`, `--execution-cost-mode`: sensitivity-basket manifest settings.
- `--no-profile-docs`: skip writing runnable profile JSON documents.
- `--json`: print machine-readable output.

After validation finishes, rebuild recipe priors to promote retained discovered structures into Play Hand's weighted seed plan:

```powershell
uv run build-recipe-priors
```

To run the 36-month scrutiny queue, use the same runner against the scrutiny atlas:

```powershell
uv run run-discovery-recipe-validation-probes --atlas-dir runs/derived/discovery-recipe-scrutiny-atlas --workers 32
uv run build-recipe-priors
```

`play-hand` reads `runs/derived/recipe-priors/play-hand-seed-plan.json` automatically when it exists. The discovered recipes are still priors, not hard filters: Play Hand keeps role-balanced fallback and policy exploration available. Guided seed-plan deals force at least two indicators, retained discovered-pair deals carry the validated timeframe/lookback/range/TA-Lib defaults into the scaffolded profile before normal sweeps vary it, and severe known negative pairs are avoided during both slot-menu selection and role-balanced fill.

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

## cleanup-playhand-lab-raw-artifacts

```powershell
uv run cleanup-playhand-lab-raw-artifacts --json
uv run cleanup-playhand-lab-raw-artifacts --yes --json
uv run cleanup-playhand-lab-raw-artifacts <run-id> --yes --json
```

Deletes redundant Play Hand Massive v2 raw debug files: `lab-result.json`, `lab-worker-result.json`, and `sweep-shard-result.json`. It only deletes a raw file when a canonical sibling artifact such as `sensitivity-response.json`, `deep-replay-job.json`, `sweep-results.json`, or `lab-failure.json` exists.

Arguments:

- `run_id`: optional v2 lane run IDs to target.
- `--older-than-minutes`: only touch older raw artifacts.
- `--yes`: apply deletion; omitted means dry run.
- `--preview`: matched runs to show.
- `--json`: print machine-readable summary.

## compact-runs-json

```powershell
uv run compact-runs-json --json
uv run compact-runs-json --yes --json
uv run compact-runs-json <run-id> --yes --workers 16 --json
```

Rewrites `.json` artifacts under `runs/` to semantic-equivalent compact JSON. It parses each file before rewriting, reports blocked parse/write errors, and preserves original mtimes after atomic replacement.

Arguments:

- `target`: optional run IDs or paths under `runs/`; omitted scans the whole `runs/` tree including `derived`.
- `--older-than-minutes`: only touch older JSON files.
- `--workers`: concurrent parse/rewrite workers. Default `8`.
- `--yes`: apply rewrite; omitted means dry run.
- `--preview`: changed files to show.
- `--json`: print machine-readable summary.
