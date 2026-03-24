# Fuzzfolio Autoresearch Program

This repo now runs an autonomous Fuzzfolio research loop instead of a model-training loop.

## Goal

Use the available tools to keep exploring the scoring-profile search space, evaluate candidates, and log every evaluated attempt into the shared attempts ledger. The repo computes the frontier procedurally from that ledger and regenerates the progress image.

## Core Rules

1. Use `fuzzfolio-agent-cli` as the main workflow surface.
2. Start fresh from the current run's seed hand. Do not branch inward from old saved profiles that existed before this run.
3. Only evolve scoring profiles that were created inside the current run directory.
4. Prefer deterministic tool actions over free-form speculation.
5. Every evaluated candidate should be logged once with its artifact directory and score.
6. The attempts ledger is append-only.
7. The progress plot is derived from all attempts:
   - new best scores become frontier points
   - non-frontier attempts become faint gray dots
8. Do not stop early just because you feel like you have a decent candidate.
9. If you think you are done, verify that you have actually logged meaningful attempts and met the run goal.

## Workflow

1. Read the fresh dealt hand and branch goals for this run.
2. Treat the dealt hand as a real constraint set, not loose inspiration.
3. If the hand is clearly redundant or poor, reshuffle at most once and record that choice in the run notes.
4. After a reshuffle, fully commit to the new hand. Do not half-follow two different hands.
5. Author a new portable profile JSON under the current run directory using only the seed-guided idea space for this run.
6. Create the profile through the CLI.
7. Evaluate candidates with `sensitivity` or `sensitivity-basket`.
8. Update only the profiles created during this run when branching or refining.
9. Log evaluated attempts so the ledger and plot stay current.
10. Continue until the controller or the user explicitly stops the run.

## Research Heuristics

- Prefer coherent candidate families over random indicator piles.
- Start from a market-behavior hypothesis, not just a list of indicators.
- Prefer clustered positive expectancy and supportive neighboring cells over isolated local spikes.
- Prefer sensible selectivity. Be skeptical of profiles that are too sparse to trust or so dense they look saturated.
- Treat path quality as separate from raw expectancy. A candidate with attractive score but ugly drawdown behavior is suspect.
- Use contrast branches intentionally. If one branch is sharp and selective, test a steadier counterweight rather than minor cosmetic variants only.
- Existing saved profiles may be inspected only if the user explicitly asks. They are not the candidate pool for autonomous runs.
- If you need a benchmark, evaluate it again inside the current run. Do not rely on prior-run artifacts as the current decision surface.

## Failure Labels

When rejecting or moving away from a branch, prefer a short explicit failure label:

- saturated signaling
- too sparse / too few signals
- isolated best-cell island
- weak neighbor support
- narrow instrument dependence
- incoherent regime logic
- ugly path quality
- collapse at longer horizon
- promising but still benchmark-inferior

## Selection Integrity

- A good incumbent benchmark is not the same thing as a successful new run candidate.
- If an old incumbent remains strongest, say so plainly instead of treating it as fresh discovery.
- Prefer keeping the best genuine current-run candidate, or keep nothing if the run did not produce a credible new winner.
- Do not quietly let prior-run artifacts or unchanged old profiles become the effective winner of the current run.

## Notes

- Keep paths absolute.
- Keep scratch artifacts under the current run directory.
- Use the CLI's saved auth profile when available; fall back to `.agentsecrets` only when login is required.
- Treat CLI probabilistic metrics as authoritative. Prefer `dsr` when available, otherwise `psr`, with `rank_score` as a fallback/secondary view.
- Existing saved profiles can be inspected only if the user explicitly asks. They are not the candidate pool for autonomous runs.
