# fuzzfolio-autoresearch

This repo is a small autonomous research runtime for Fuzzfolio.

The core idea from the original `autoresearch` project is preserved:
- keep the runtime small
- log every attempt
- compute a running frontier
- regenerate a progress image so you can tell the loop is alive

What changed is the search surface. Instead of editing `train.py` and optimizing `val_bpb`, the controller now:
- uses `fuzzfolio-agent-cli`
- evaluates scoring-profile candidates
- logs scored attempts to an append-only ledger
- computes frontier points procedurally from that ledger
- renders `runs/progress.png`

## Current runtime

The new runtime lives in `autoresearch/` and exposes:

- `autoresearch doctor`
- `autoresearch run`
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
```

## Attempts and plot

All attempts are appended to:

- `runs/attempts.jsonl`

The progress image is regenerated at:

- `runs/progress.png`

The renderer uses one ledger only:
- every scored attempt is a point
- frontier points are computed as the running best score
- non-frontier attempts are shown as faint gray dots

## Scoring

For now the primary score is `rank_score` from `fuzzfolio-agent-cli compare-sensitivity`.

Any extra scoring adjustments are intentionally light and modular so they can be replaced when better backend metrics land.

## Long-running behavior

The controller includes:
- a yield guard so the model cannot immediately declare success without logging meaningful work
- periodic checkpoint compaction so prompt state stays bounded over longer runs

## Legacy files

The original upstream training files are still present in the repo for reference during the transition, but the active runtime is now the Fuzzfolio operator flow.
