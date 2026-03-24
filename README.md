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

## Local config

Checked-in examples:

- `autoresearch.config.example.json`
- `.agentsecrets.example`

Local-only files the runtime reads:

- `autoresearch.config.json`
- `.agentsecrets`

Keep both untracked. `.agentsecrets` is the place for the provider key and fallback Fuzzfolio login credentials.

## Quick start

1. Copy `autoresearch.config.example.json` to `autoresearch.config.json`.
2. Copy `.agentsecrets.example` to `.agentsecrets`.
3. Fill in the provider API key.
4. Adjust Fuzzfolio settings if your local stack differs from defaults.
5. Run:

```powershell
uv run autoresearch doctor
uv run autoresearch run --max-steps 20
uv run autoresearch supervise
```

By default, `run` prints a compact live console trace so you can watch the controller think, act, and log attempts in real time.
If you want machine-readable output instead, use:

```powershell
uv run autoresearch run --max-steps 20 --json
```

The default live trace uses `rich` for colored panels and step/result tables so it is easier to watch during longer managed runs.

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
```

## Scoring

For now the primary score is `rank_score` from `fuzzfolio-agent-cli compare-sensitivity`.

Any extra scoring adjustments are intentionally light and modular so they can be replaced when better backend metrics land.

## Long-running behavior

The controller includes:
- a yield guard so the model cannot immediately declare success without logging meaningful work
- periodic checkpoint compaction so prompt state stays bounded over longer runs

## Legacy files

The original upstream training files are still present in the repo for reference during the transition, but the active runtime is now the Fuzzfolio operator flow.
