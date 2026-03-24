# Fuzzfolio Autoresearch Program

This repo now runs an autonomous Fuzzfolio research loop instead of a model-training loop.

## Goal

Use the available tools to keep exploring the scoring-profile search space, evaluate candidates, and log every evaluated attempt into the shared attempts ledger. The repo computes the frontier procedurally from that ledger and regenerates the progress image.

## Core Rules

1. Use `fuzzfolio-agent-cli` as the main workflow surface.
2. Prefer deterministic tool actions over free-form speculation.
3. Every evaluated candidate should be logged once with its artifact directory and score.
4. The attempts ledger is append-only.
5. The progress plot is derived from all attempts:
   - new best scores become frontier points
   - non-frontier attempts become faint gray dots
6. Do not stop early just because you feel like you have a decent candidate.
7. If you think you are done, verify that you have actually logged meaningful attempts and met the run goal.

## Workflow

1. Verify auth with `fuzzfolio-agent-cli auth whoami --pretty`.
2. Fetch a fresh dealt hand with `fuzzfolio-agent-cli seed prompt --pretty`.
3. Create or update candidate profiles under the current run directory.
4. Evaluate candidates with `sensitivity` or `sensitivity-basket`.
5. Compare and refine candidates as needed.
6. Log evaluated attempts so the ledger and plot stay current.
7. Continue until the controller or the user explicitly stops the run.

## Notes

- Keep paths absolute.
- Keep scratch artifacts under the current run directory.
- Use the CLI's saved auth profile when available; fall back to `.agentsecrets` only when login is required.
- The current scoring system is temporary. Treat `rank_score` as the primary metric for now, with only light modular adjustments.
