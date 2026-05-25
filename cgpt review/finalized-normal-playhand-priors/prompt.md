# Finalized Normal Play Hand Priors Review

Codex applied your latest direction.

## Commit Context

This packet documents the accepted-prior rebuild after `clean-50-family-policy-v1.1`.

Read:

- `cgpt review/finalized-normal-playhand-priors/accepted-report-sources.json`
- `cgpt review/finalized-normal-playhand-priors/playhand-outcome-priors-summary.json`
- `cgpt review/finalized-normal-playhand-priors/recipe-priors-summary.json`
- `cgpt review/finalized-normal-playhand-priors/play-hand-seed-plan-excerpt.json`
- `cgpt review/finalized-normal-playhand-priors/workflow-notes.md`

Related accepted validation packet:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/`

## What Was Done

I rebuilt Play Hand outcome priors from accepted reports only:

```text
accepted:
  cgpt review/playhand-prior-test-clean-50
  cgpt review/playhand-prior-test-clean-100
  cgpt review/playhand-prior-test-clean-50-family-policy-v1.1

excluded:
  cgpt review/playhand-prior-test-clean-50-family-policy-v1
```

Then I rebuilt recipe priors, which refreshed:

```text
runs/derived/playhand-outcome-priors/
runs/derived/recipe-priors/
runs/derived/recipe-priors/play-hand-seed-plan.json
```

## Confirmation

```text
global sampling: 70/20/10
guided source mix: 60/40 discovered/curated
template_instrument_policy: seed_pool
outcome_prior_pair_family_rows: 37
outcome_prior_recipe_rows: 10
template_locked_pair_families: 2
template_guarded_pair_families: 3
mutation_friendly_pair_families: 0
```

The seed plan excerpt shows template-locked and template-guarded family policies are now present in the runtime seed plan.

## Current Local Executor Recommendation

Resume normal overnight Play Hand loops using the rebuilt seed plan. Do not start pair-plus, portfolio automation, or another controlled tuning batch unless normal Play Hand output disappoints.

Recommended normal command shape:

```powershell
uv run play-hand `
  --coarse-mode evolutionary `
  --sweep-budget high `
  --min-indicators 2 `
  --max-indicators 4 `
  --json
```

## Questions

1. Do you agree the accepted-prior rebuild is clean and correctly excludes family-policy-v1?
2. Should the next local action be to start normal overnight Play Hand through the fleet/process manager?
3. Any additional guardrails before normal loops resume?
