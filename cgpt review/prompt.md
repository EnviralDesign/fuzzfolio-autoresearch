# CGPT Review Packet

Please review the finalized normal Play Hand prior rebuild.

## Start Here

New finalized packet:

- `cgpt review/finalized-normal-playhand-priors/prompt.md`
- `cgpt review/finalized-normal-playhand-priors/accepted-report-sources.json`
- `cgpt review/finalized-normal-playhand-priors/playhand-outcome-priors-summary.json`
- `cgpt review/finalized-normal-playhand-priors/recipe-priors-summary.json`
- `cgpt review/finalized-normal-playhand-priors/play-hand-seed-plan-excerpt.json`
- `cgpt review/finalized-normal-playhand-priors/workflow-notes.md`

Accepted validation packet:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/`

## What Changed

Pro accepted `clean-50-family-policy-v1.1` for outcome-prior backprop, with `family-policy-v1` explicitly excluded.

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

## Confirmations

```text
accepted reports: clean-50, clean-100, family-policy-v1.1
excluded reports: family-policy-v1
global sampling: 70/20/10
guided source mix: 60/40 discovered/curated
template_instrument_policy: seed_pool
template_locked pair families: present
template_guarded pair families: present
normal Play Hand consumes the rebuilt seed plan automatically
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
discovered_validation_retained_rows: 10
discovered_recipe_pair_rows: 10
outcome_prior_pair_family_rows: 37
outcome_prior_recipe_rows: 10
```

## Request

Please confirm whether to resume normal overnight Play Hand loops now, using the rebuilt priors, or whether you want one more guardrail before fleet/process-manager execution.
