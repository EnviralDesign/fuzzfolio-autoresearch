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
- logs scored attempts to an append-only ledger
- computes frontier points procedurally from that ledger
- renders progress artifacts for each run and mirrors the latest one to `runs/progress.png`

## Current runtime

The new runtime lives in `autoresearch/` and exposes:

- `autoresearch doctor`
- `autoresearch run`
- `autoresearch supervise`
- `autoresearch score <artifact_dir>`
- `autoresearch record-attempt <artifact_dir>`
- `autoresearch plot`
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
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
uv run autoresearch reset-runs
```

By default, `run` prints a compact live console trace so you can watch the controller think, act, and log attempts in real time.
If you want machine-readable output instead, use:

```powershell
uv run autoresearch run --max-steps 20 --json
```

The default live trace uses `rich` for colored panels and step/result tables so it is easier to watch during longer managed runs.
The default provider completion budget is intentionally a bit roomy because the agent sometimes needs to emit a full portable profile JSON in one action.
The controller also uses threshold-triggered context compaction modeled after `codex-rs`: once the live prompt estimate crosses the configured token threshold, it writes a checkpoint summary and rebuilds the active history from fresh run state plus a short recent tail.
The runtime now uses named LLM provider profiles instead of one global provider block. By default in the example config, the main explorer loop uses the `openai-mini` profile (`gpt-5.4-mini`) while the supervisor guidance path uses the `xai-grok` profile (`grok-4.20-reasoning`).
In plain `run` mode, the controller now uses an explicit phase policy: most of the run is exploration with finish disabled, and only the last few steps become wrap-up. The prompt also carries a rolling next-score target so the explorer has a concrete stretch goal instead of repeatedly trying to stop.
The controller now also owns horizon policy by phase: early runs screen over shorter month-based windows, mid runs deepen evidence around one year, and late/wrap-up phases push survivors toward 2-3 year pressure tests. The worker is guided to think in weeks/months/years, not bars.

## Multi-provider config

Provider selection now lives in two places:

- `llm.explorer_profile`
- `llm.supervisor_profile`

Each named profile is defined under `providers`.

Example:

```json
{
  "llm": {
    "explorer_profile": "openai-mini",
    "supervisor_profile": "xai-grok"
  },
  "providers": {
    "openai-mini": {
      "type": "openai",
      "model": "gpt-5.4-mini"
    },
    "xai-grok": {
      "type": "xai",
      "model": "grok-4.20-reasoning"
    }
  }
}
```

Secrets are matched by profile name in `.agentsecrets`:

```json
{
  "providers": {
    "openai-mini": { "api_key": "..." },
    "xai-grok": { "api_key": "..." }
  }
}
```

The current provider types are:

- `openai`
- `xai`
- `openrouter`
- `openai_compatible`

## Supervised Runs

`supervise` is the managed runner mode.

- It reads defaults from `autoresearch.config.json` if you do not pass flags.
- CLI flags override config values when you do pass them.
- The supervisor, not the agent, owns termination in this mode.
- The run stops only on controller-owned conditions such as `max_steps`, fatal error, or the operating window closing at a step boundary.

Example:

```powershell
uv run autoresearch supervise
uv run autoresearch supervise --max-steps 300 --window 23:00-05:00 --timezone America/Chicago
```

Config-backed example:

```json
{
  "supervisor": {
    "max_steps": 200,
    "window_start": "23:00",
    "window_end": "05:00",
    "timezone": "America/Chicago",
    "stop_mode": "after_step"
  }
}
```

The operating window is checked at step boundaries. If the window closes while a model/tool step is already in flight, the controller lets that step finish cleanly, writes logs/checkpoints, and then stops before prompting again.

You can still use the module form if you want:

```powershell
uv run -- python -m autoresearch doctor
uv run -- python -m autoresearch run --max-steps 20
```

## Run directories

Every `autoresearch run` invocation creates a fresh timestamped run directory under `runs/`, for example:

- `runs/20260324T181958Z-agentic`

Each run keeps its own artifacts:

- `profiles/`
- `evals/`
- `notes/`
- `seed-prompt.json`
- `controller-log.jsonl`
- `progress.png`

The latest run directory is also written to:

- `runs/latest-run.txt`

## Attempts and plot

All attempts are appended to:

- `runs/attempts.jsonl`

Each run writes its own first-class progress image at:

- `runs/<run-id>/progress.png`

The latest generated run image is then mirrored to:

- `runs/progress.png`

The renderer uses one ledger only:
- every scored attempt is a point
- frontier points are computed as the running best score
- non-frontier attempts are shown as faint gray dots

Useful commands:

```powershell
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
uv run autoresearch plot
uv run autoresearch rescore-attempts
uv run autoresearch reset-runs
```

## Scoring

The runtime now treats the CLI's scoring surface as authoritative:

- `primary_score` is the CLI `rank_score`
- `composite_score` is `dsr` when present, otherwise `psr`, otherwise `rank_score`
- each attempt record also stores `psr`, `dsr`, `k_ratio`, and `sharpe_r` when available

The Python side no longer applies ad hoc penalties for trade count, signal count, or positive-cell coverage.

## Long-running behavior

The controller includes:
- a yield guard so the model cannot immediately declare success without logging meaningful work
- periodic checkpoint compaction so prompt state stays bounded over longer runs
- controller-owned horizon injection for sensitivity runs, so phase-appropriate `--lookback-months` is added automatically when the model omits it
