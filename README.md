# fuzzfolio-autoresearch

This repo is a small autonomous research runtime for Fuzzfolio.

The core idea from the original `autoresearch` project is preserved:
- keep the runtime small
- log every attempt
- compute a running frontier
- regenerate a progress image so you can tell the loop is alive

What changed is the search surface. Instead of mutating a training script and optimizing a validation metric, the controller now:
- uses `fuzzfolio-agent-cli`
- evaluates scoring-profile candidates
- logs scored attempts to a per-run ledger
- computes frontier points procedurally from that run-local ledger
- renders progress artifacts per run
- generates all-runs aggregate views only on demand

## Current runtime

The new runtime lives in `autoresearch/` and exposes:

- `autoresearch doctor`
- `autoresearch test-providers`
- `autoresearch run`
- `autoresearch supervise`
- `autoresearch stop-all-runs`
- `autoresearch purge-cloud-profiles`
- `autoresearch prune-runs`
- `autoresearch score <artifact_dir>`
- `autoresearch record-attempt <artifact_dir>`
- `autoresearch plot`
- `autoresearch leaderboard`
- `autoresearch dashboard`
- `autoresearch sync-profile-drop-pngs`
- `autoresearch reset-runs`

## Local config

Checked-in examples:

- `autoresearch.config.example.json`
- `.agentsecrets.example`

Local-only files the runtime reads:

- `autoresearch.config.json`
- `.agentsecrets`

Keep both untracked. `.agentsecrets` is the place for per-profile provider keys and fallback Fuzzfolio login credentials.

## Quick start

1. Copy `autoresearch.config.example.json` to `autoresearch.config.json`.
2. Copy `.agentsecrets.example` to `.agentsecrets`.
3. Fill in the provider API keys you want to use.
4. Adjust Fuzzfolio settings if your local stack differs from defaults.
5. Run:

```powershell
uv run autoresearch doctor
uv run autoresearch test-providers

Supported provider types include `openai`, `xai`, `groq`, `openrouter`, `codex`, `openai_compatible`, `lmstudio`, and `minimax`.
LM Studio's OpenAI-compatible local server defaults to `http://localhost:1234/v1`, and authentication is off by default unless you enable "Require Authentication" in LM Studio's server settings.
MiniMax defaults to `https://api.minimax.io/v1` with `MINIMAX_API_KEY`; the standard coding/token-plan model name is `MiniMax-M2.7`.
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
uv run autoresearch stop-all-runs
uv run autoresearch purge-cloud-profiles
uv run autoresearch prune-runs
uv run autoresearch leaderboard
uv run autoresearch reset-runs
```

By default, `run` prints a compact live console trace so you can watch the controller think, act, and log attempts in real time.
If you want machine-readable output instead, use:

```powershell
uv run autoresearch run --max-steps 20 --json
```

If you want proc-man-friendly line-oriented output instead of Rich panels, use:

```powershell
uv run autoresearch run --max-steps 20 --plain-progress
uv run autoresearch supervise --plain-progress
```

`--plain-progress` keeps the live trace human-readable, but renders it as simple newline-delimited logs with dashed separators so log capture tools can follow it more reliably.

You can also override the configured provider profiles per invocation:

```powershell
uv run autoresearch run --max-steps 20 --explorer-profile openai-mini
uv run autoresearch supervise --explorer-profile grok-fast
```

The default live trace uses `rich` for colored panels and step/result tables so it is easier to watch during longer managed runs.
The default provider completion budget is intentionally a bit roomy because the agent sometimes needs to emit a full portable profile JSON in one action.
The controller also uses threshold-triggered context compaction modeled after `codex-rs`: once the live prompt estimate crosses the configured token threshold, it writes a checkpoint summary and rebuilds the active history from fresh run state plus a short recent tail.
Compaction thresholds can now also be set per provider profile with `providers.<name>.compact_trigger_tokens`. If omitted, the runtime falls back to the global `research.compact_trigger_tokens`.
The runtime now uses named LLM provider profiles instead of one global provider block. By default in the example config, the main explorer loop uses the `openai-mini` profile (`gpt-5.4-mini`).
In plain `run` mode, the controller now uses an explicit phase policy: most of the run is exploration with finish disabled, and only the last few steps become wrap-up. The prompt also carries a rolling next-score target so the explorer has a concrete stretch goal instead of repeatedly trying to stop.
The controller now also owns horizon policy by phase: early runs screen over shorter month-based windows, mid runs deepen evidence around one year, and late/wrap-up phases push survivors toward 2-3 year pressure tests. The worker is guided to think in weeks/months/years, not bars.
The controller also owns the active quality-score preset. By default it injects the `profile-drop` preset into deep-replay-backed evaluations and scaffolded sweeps so the agent does not need to remember or reason about preset selection mid-run.
The controller now treats sweeps as first-class search behavior instead of an optional side path. Early and mid phases explicitly encourage `sweep scaffold`, `sweep patch`, `sweep validate`, and then `sweep submit` around promising families before the run settles into manual profile tweaking only.
Instrument context is also coverage-aware now. The runtime asks the CLI for market coverage at a reference timeframe and surfaces buffered shortlist hints such as roughly `11` months for mid-phase work and `34` months for long-horizon wrap-up, so the agent naturally favors symbols that can actually satisfy the requested evidence horizon.
New runs also persist `run-metadata.json` with the active explorer profile and model name. Progress plots, progress indexes, and the derived leaderboard use that file when present so you can tell which model produced which run.
The controller now also writes `cli-help-catalog.json` per run from the real `fuzzfolio-agent-cli --help` surface. It uses that catalog as a shallow front-door guard for invalid command families and subcommands, and the agent can explicitly recover with `run_cli ["help"]` or `run_cli ["help", "<family>"]`.

If you want to change that preset later, set `research.quality_score_preset` in `autoresearch.config.json`.

## Multi-provider config

Provider selection now lives in two places:

- `llm.explorer_profile`

Optional **event-driven branch manager** (adjudication only; no research tools) lives under:

- `manager.enabled`
- `manager.profiles` (named `providers` entries)
- `manager.max_candidate_families_in_packet`

**Leader IDs** (provisional / validated) in the overlay are set only via **manager actions** or prior run state — there is no score-based leader recomputation in the controller. After each scored eval the controller updates validation evidence and budget mode mechanically; reseed windows and suppression are driven by the manager (or prior overlay state). Runtime state and `runtime-state.json` include a `manager` section with the last hook, rationale tail, applied actions, and an `invocation_incomplete` flag when the manager errored, returned invalid output, or an action failed to apply.

Each named profile is defined under `providers`.

Example:

```json
{
  "llm": {
    "explorer_profile": "openai-mini"
  },
  "providers": {
    "openai-mini": {
      "type": "openai",
      "model": "gpt-5.4-mini",
      "compact_trigger_tokens": 12000
    },
    "xai-grok": {
      "type": "xai",
      "model": "grok-4.20-reasoning",
      "compact_trigger_tokens": 40000
    }
  }
}
```

That profile-level compaction override is useful when one model has a much larger context window, a very different token cost, or a different quality/latency tradeoff than another.
Provider profiles can also tune generic rate-limit behavior with:

- `rate_limit_backoff_seconds`
- `rate_limit_max_retries`

If omitted, the runtime uses a provider-agnostic default backoff ladder of `15, 30, 60, 120, 180, 240, 300` seconds and then keeps retrying every `300` seconds until the configured retry ceiling is reached. Temporary rate-limit signals honor `Retry-After` when providers send it. Clear hard-quota/billing failures still fail fast instead of sleeping forever.

Secrets are matched by profile name in `.agentsecrets`:

```json
{
  "api_keys": {
    "openai_main": "...",
    "xai_main": "..."
  }
}
```

Profiles can reference those shared keys with `api_key_ref`:

```json
{
  "providers": {
    "openai-mini": { "type": "openai", "api_key_ref": "openai_main" },
    "xai-grok": { "type": "xai", "api_key_ref": "xai_main" },
    "groq-oss-20b": { "type": "groq", "api_key_ref": "groq_main", "model": "openai/gpt-oss-20b" }
  }
}
```

You can still set `providers.<profile>.api_key` directly in `.agentsecrets` if you want profile-specific overrides. `api_key_ref` is just the cleaner shared-key path.
The `codex` provider is different: it does not read an API key from `.agentsecrets`. Instead it talks to the local `codex app-server`, which uses the active managed auth in `~/.codex/auth.json` (for example from `codex login` / ChatGPT sign-in).

The current provider types are:

- `codex`
- `openai`
- `xai`
- `groq`
- `openrouter`
- `minimax`
- `openai_compatible`

`groq` is a first-class direct provider that defaults to:

- `api_base = "https://api.groq.com/openai/v1"`
- `api_key_env = "GROQ_API_KEY"`
- `transport = "chat_completions"`

That is the preferred path when you want Groq-hosted OSS models without going through OpenRouter routing.

`minimax` is a first-class direct provider that defaults to:

- `api_base = "https://api.minimax.io/v1"`
- `api_key_env = "MINIMAX_API_KEY"`
- `transport = "chat_completions"`

Example:

```json
{
  "providers": {
    "minimax-27": {
      "type": "minimax",
      "api_key_ref": "minimax_main",
      "model": "MiniMax-M2.7"
    }
  }
}
```

If your MiniMax token plan exposes the higher-throughput variant, you can swap the model name to `MiniMax-M2.7-highspeed` without changing the provider type.

`codex` is a first-class local managed-auth provider. It defaults to:

- `command = "codex"`
- `transport = "app_server"`
- `timeout_seconds = 180`

Example:

```json
{
  "providers": {
    "codex-mini": {
      "type": "codex",
      "command": "codex",
      "model": "gpt-5.4-mini"
    }
  }
}
```

Before using it, make sure the local Codex app is already signed in:

```powershell
codex login
uv run autoresearch test-providers --profile codex-mini
uv run autoresearch run --explorer-profile codex-mini
```

This path keeps ChatGPT/Codex OAuth under Codex itself rather than copying bearer tokens or API keys into `autoresearch`.

Profiles may also choose a transport explicitly when needed:

- `chat_completions`
- `responses`

This matters most for xAI multi-agent models, which require `transport: "responses"`.

## Sweep-Aware Search

The preferred deterministic sweep workflow is now:

```powershell
fuzzfolio-agent-cli sweep scaffold --profile-ref <PROFILE_ID> --instrument EURUSD --axis profile.notificationThreshold=70,75,80 --axis indicator[0].config.lookbackBars=1,2,3 --out .\sweep.json
fuzzfolio-agent-cli sweep patch --definition .\sweep.json --set lookback_months=3 --set top_n=8 --out .\sweep.tuned.json
fuzzfolio-agent-cli sweep validate --definition .\sweep.tuned.json
fuzzfolio-agent-cli sweep submit --definition .\sweep.tuned.json
```

This is intentionally easier than writing raw sweep JSON by hand. It lets the agent express sweep intent in terms of profile fields, indicator config fields, and TA-Lib params.

Two important knobs that are now easy to sweep:

- `profile.notificationThreshold`
  - the aggregate score threshold for triggering entries
- `indicator[N].config.lookbackBars`
  - signal persistence / how long a qualifying condition must persist

These are useful both for permissive early screening and for later hardening around a surviving family.

## Supervised Runs

`supervise` is the managed runner mode.

- It reads defaults from `autoresearch.config.json` if you do not pass flags.
- CLI flags override config values when you do pass them.
- The controller/session policy, not the agent, owns termination in this mode.
- `max_steps` is a per-session cap in this mode, not a whole-night cap.
- When a supervised session hits its step cap, supervise starts a brand-new isolated session if the outer time window is still open.
- New supervised sessions do not carry over prior conversation state; each one starts boxed-in and fresh with its own run directory.
- The outer supervise loop stops starting new sessions when the operating window closes or enters the configured soft-wrap zone.

Example:

```powershell
uv run autoresearch supervise
uv run autoresearch supervise --max-steps 300 --window 23:00-05:00 --timezone America/Chicago
```

If you need to halt local research work manually, use:

```powershell
uv run autoresearch stop-all-runs
uv run autoresearch stop-all-runs --stop-autoresearch
```

`stop-all-runs` is a local operator command. It does not use a remote server API key. By default it:

- clears the local dev Redis sweep/deep-replay/sim queues through the Trading-Dashboard harness
- leaves local autoresearch controller processes alone

Use `--stop-autoresearch` if you also want to kill local `autoresearch run` / `autoresearch supervise` Python processes.

If you want to wipe saved scoring profiles from the currently configured robot/cloud account, use:

```powershell
uv run autoresearch purge-cloud-profiles
uv run autoresearch purge-cloud-profiles --yes
```

This command uses the existing configured Fuzzfolio auth profile and CLI session. It is a dry run by default and only deletes when `--yes` is supplied.

If you want to prune local run directories that never really got off the ground, use:

```powershell
uv run autoresearch prune-runs
uv run autoresearch prune-runs --min-mapped-points 2 --yes
```

`mapped points` means scored attempts with a non-null `composite_score`, which matches what the progress plot can actually map. The command is a dry run by default and deletes matched runs only when `--yes` is supplied.

Config-backed example:

```json
{
  "supervise": {
    "max_steps": 200,
    "window_start": "23:00",
    "window_end": "05:00",
    "timezone": "America/Chicago",
    "stop_mode": "after_step",
    "soft_wrap_minutes": 30
  }
}
```

The operating window is checked at step boundaries. If the window closes while a model/tool step is already in flight, the controller lets that step finish cleanly and then stops before prompting again. Near the end of the window, the worker also gets a soft wrap-soon note so highly autonomous models start winding down instead of opening broad fresh exploration.

You can still use the module form if you want:

```powershell
uv run -- python -m autoresearch doctor
uv run -- python -m autoresearch run --max-steps 20
```

## Run directories

Every `autoresearch run` invocation creates a fresh timestamped run directory under `runs/`, for example:

- `runs/20260324T181958Z-agentic`

Each run directory now carries:

- `attempts.jsonl`
- `progress.png`
- `progress-index.json`
- `progress-index.csv`
- `run-metadata.json`
- `cli-help-catalog.json`

`run-metadata.json` is the forward-looking source of truth for model tracking. Older runs created before this file existed still render normally, but they cannot be tagged reliably after the fact because the runtime did not persist model identity for them.

Each run keeps its own artifacts:

- `attempts.jsonl`
- `profiles/`
- `evals/`
- `notes/`
- `seed-prompt.json`
- `controller-log.jsonl`
- `progress.png`

Each run writes its own first-class progress image at:

- `runs/<run-id>/progress.png`

The renderer uses one run-local ledger only:
- every scored attempt is a point
- frontier points are computed as the running best score
- non-frontier attempts are shown as faint gray dots

There is no live global ledger or live global progress image anymore. That makes concurrent runs much cleaner.

## Derived aggregate views

When you want a cross-run view, generate it explicitly:

```powershell
uv run autoresearch plot --all-runs
uv run autoresearch leaderboard
```

These write derived artifacts under:

- `runs/derived/progress-all-runs.png`
- `runs/derived/leaderboard.png`
- `runs/derived/leaderboard.json`
- `runs/derived/leaderboard-model-averages.png`
- `runs/derived/leaderboard-model-averages.json`
- `runs/derived/leaderboard-score-vs-trades.png`
- `runs/derived/leaderboard-score-vs-trades.json`
- `runs/derived/leaderboard-validation.json`
- `runs/derived/leaderboard-validation-12m-vs-36m.png`
- `runs/derived/leaderboard-validation-delta.png`

The leaderboard is best-per-run, sorted by `quality_score`.
The model-average leaderboard groups those best-per-run outcomes by explorer model and reports the mean score per model.
The score-vs-trades leaderboard maps each run's best candidate as a point, with trades per month on the x-axis and quality score on the y-axis. It filters out very low-scoring noise, clamps the visible x-axis to `200` trades/month, and overlays the upper-envelope frontier so the top-left to top-middle tradeoff is easier to read.
The validation scatter plots each run leader's `36m` score on the x-axis against its `12m` score on the y-axis so recent-only winners separate from candidates that survive scrutiny.
The validation delta chart ranks those same leaders by `36m - 12m` score, which makes score decay obvious at a glance.

Useful validation workflow:

```powershell
uv run autoresearch leaderboard
```

That single command now regenerates the classic leaderboards plus the two validation views above.
By default it heals missing or stale validation artifacts and reuses matching cached manifests. If you want to invalidate the validation/similarity cache on purpose, use:

```powershell
uv run autoresearch leaderboard --force-rebuild
```

Useful commands:

```powershell
uv run autoresearch test-providers
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
uv run autoresearch supervise --no-window
uv run autoresearch plot --all-runs
uv run autoresearch leaderboard
uv run autoresearch sync-profile-drop-pngs
uv run autoresearch dashboard
uv run autoresearch rescore-attempts
uv run autoresearch reset-runs
```

The local dashboard command serves a simple SPA over the existing run artifacts:

```powershell
uv run autoresearch dashboard
```

Default URL:

- `http://127.0.0.1:47832`

If you want the dashboard refresh path to ignore cached validation artifacts and rebuild them from scratch on startup, use:

```powershell
uv run autoresearch dashboard --force-rebuild
```

What it gives you:

- overview cards for run count, attempts, best score, profile-drop coverage, and curve coverage
- the existing derived leaderboard images with a refresh button that rebuilds them from source
- interactive score-vs-trade-rate and score-vs-drawdown views
- interactive `12m vs 36m` validation scatter and scrutiny-delta views
- a run list and model consistency table
- per-run attempt drilldown
- lightweight backtest inspection from `best-cell-path-detail.json`, including equity and drawdown curves
- per-attempt profile-drop previews that prefer `12mo` and `36mo` renders when present

`sync-profile-drop-pngs` walks each run, finds its best scored attempt, ensures the backing profile exists in cloud storage, rebuilds a fresh profile-drop bundle through the existing FuzzFolio package flow, and renders side-by-side horizon cards:

- `runs/<run-id>/profile-drop-12mo.png`
- `runs/<run-id>/profile-drop-36mo.png`

It defaults to a fixed `12` month replay window for every rebuilt card. Override with:

```powershell
uv run autoresearch sync-profile-drop-pngs --lookback-months 36
```

Examples:

```powershell
uv run autoresearch sync-profile-drop-pngs
uv run autoresearch sync-profile-drop-pngs --run-id 20260327T212626114512Z-agentic-0245ff
uv run autoresearch sync-profile-drop-pngs --lookback-months 24
uv run autoresearch sync-profile-drop-pngs --force-rebuild
```

When `--lookback-months` is neither `12` nor `36`, the command still always renders the standard `12mo` and `36mo` cards and will additionally render the requested custom horizon.
By default it heals missing or stale profile-drop renders using per-horizon manifests and skips runs that are already up to date. Use `--force-rebuild` when you intentionally want to rerender everything.

`supervise` already starts a fresh isolated session when the prior session stops with `step_limit_reached` and time remains in the outer supervise window. Set `"window_enabled": false` under `supervise`, or pass `--no-window`, to let supervise run around the clock.

If you also set `"auto_restart_terminal_sessions": true` under `supervise`, supervise will start a fresh isolated session after terminal session endings such as `finished` or `session_error`, not just after `step_limit_reached`.

## Scoring

The runtime now treats the CLI's scoring surface as authoritative:

- `quality_score` is the source-of-truth aggregate metric computed in Fuzzfolio itself
- `primary_score` and `composite_score` mirror that propagated `quality_score`
- each attempt record also stores supporting fields like `psr`, `dsr`, `k_ratio`, and `sharpe_r` when available

Attempts without a usable `quality_score` are kept for observability, but they are not treated as scored frontier points. The Python side no longer invents its own temporary scoring formula.

## Horizon Validity

Saved sensitivity artifacts now expose both requested and effective window information under `market_data_window`, including:

- `requested_window_start`
- `requested_window_end`
- `effective_window_start`
- `effective_window_end`
- `effective_window_days`
- `effective_window_months`

That means the worker and controller can reason about evidence horizon in months and days instead of bars. If a requested 24-month test only delivered 2.8 effective months, the run can now see that directly without digging through raw bar mechanics.

## Long-running behavior

The controller includes:
- a yield guard so the model cannot immediately declare success without logging meaningful work
- periodic checkpoint compaction so prompt state stays bounded over longer runs
- controller-owned horizon injection for sensitivity runs, so phase-appropriate `--lookback-months` is added automatically when the model omits it
