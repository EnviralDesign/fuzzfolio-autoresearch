# Finalized Normal Play Hand Priors Workflow Notes

## Pro Decision Applied

Pro accepted `clean-50-family-policy-v1.1` for outcome-prior backprop, with one hard constraint:

```text
Use accepted reports only:
  cgpt review/playhand-prior-test-clean-50
  cgpt review/playhand-prior-test-clean-100
  cgpt review/playhand-prior-test-clean-50-family-policy-v1.1

Exclude:
  cgpt review/playhand-prior-test-clean-50-family-policy-v1
```

## Commands Run

Outcome priors were rebuilt with explicit accepted report directories:

```powershell
uv run build-playhand-outcome-priors `
  --report-dir "cgpt review\playhand-prior-test-clean-50" `
  --report-dir "cgpt review\playhand-prior-test-clean-100" `
  --report-dir "cgpt review\playhand-prior-test-clean-50-family-policy-v1.1" `
  --json
```

Recipe priors were then rebuilt:

```powershell
uv run build-recipe-priors --json
```

## Output Paths

Generated local runtime artifacts:

```text
runs/derived/playhand-outcome-priors/playhand-outcome-priors.json
runs/derived/playhand-outcome-priors/playhand-outcome-priors-summary.json
runs/derived/recipe-priors/recipe-priors.json
runs/derived/recipe-priors/recipe-priors-summary.json
runs/derived/recipe-priors/play-hand-seed-plan.json
```

Git-visible review artifacts:

```text
cgpt review/finalized-normal-playhand-priors/accepted-report-sources.json
cgpt review/finalized-normal-playhand-priors/playhand-outcome-priors-summary.json
cgpt review/finalized-normal-playhand-priors/recipe-priors-summary.json
cgpt review/finalized-normal-playhand-priors/play-hand-seed-plan-excerpt.json
cgpt review/finalized-normal-playhand-priors/workflow-notes.md
cgpt review/finalized-normal-playhand-priors/prompt.md
```

## Confirmed Settings

```text
accepted reports: clean-50, clean-100, family-policy-v1.1
excluded reports: family-policy-v1
global sampling: 70/20/10
guided source mix: 60/40 discovered/curated
template instrument policy: seed_pool
template_locked pair families: present
template_guarded pair families: present
normal Play Hand consumes play-hand-seed-plan.json automatically
```

Outcome-prior summary:

```text
pair_family_rows: 37
recipe_rows: 10
template_locked_pair_families: 2
template_guarded_pair_families: 3
mutation_friendly_pair_families: 0
```

Recipe-prior summary:

```text
indicator_rows: 87
slot_indicator_rows: 660
pair_prior_rows: 58
discovered_validation_rows: 77
discovered_validation_retained_rows: 10
discovered_recipe_pair_rows: 10
outcome_prior_pair_family_rows: 37
outcome_prior_recipe_rows: 10
```

## Recommended Next Local Step

Resume normal overnight Play Hand loops with the rebuilt seed plan:

```powershell
uv run play-hand `
  --coarse-mode evolutionary `
  --sweep-budget high `
  --min-indicators 2 `
  --max-indicators 4 `
  --json
```

No pair-plus work, portfolio automation work, or additional controlled-batch tuning has been started.
