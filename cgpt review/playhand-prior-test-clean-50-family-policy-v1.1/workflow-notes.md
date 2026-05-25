# Workflow Notes

## What Ran

The `clean-50-family-policy-v1.1` source-mix confirmation batch ran seeds `201..250`:

```powershell
uv run play-hand `
  --seed <seed> `
  --coarse-mode evolutionary `
  --sweep-budget high `
  --min-indicators 2 `
  --max-indicators 4 `
  --final-profile-drop-count 0 `
  --json
```

The batch completed all 50 seeds with 0 execution failures.

## Why v1.1 Exists

Family-policy-v1 reduced family concentration, but it also under-exposed discovered/template priors. v1.1 added an explicit guided source mix inside the guided lane:

```json
{
  "discovery_recipe_validation": 0.60,
  "curated_recipe_prior": 0.40
}
```

Policy exploration remains separate from guided sampling.

## What Was Checked

The report packet confirms:

```text
batch_status.completed == 100% of requested seeds: 50/50
execution failures: 0
guided bucket fallback: 0
template materialization failures: 0
blank/unknown/policy rows excluded from pair-family concentration metrics
family classification rules included in report/comparisons
```

## What Was Not Done

I did not run:

```powershell
uv run build-playhand-outcome-priors --json
uv run build-recipe-priors --json
```

That was intentional. The user requested that v1.1 not be backpropped until Pro reviews this packet.

## Suggested Review Order

1. `recipe-performance-report.md`
2. `recipe-performance-comparison-family-policy-v1-vs-v1.1.md`
3. `recipe-performance-comparison-clean100-vs-family-policy-v1.1.md`
4. `recipe-performance-comparison-original-clean50-vs-family-policy-v1.1.md`
5. `recipe-performance-pairs.csv`
6. `recipe-performance-runs.csv`

## Local Executor Read

v1.1 looks operationally clean and fixes the v1 source-mix issue. Discovered validation priors are again exposed at the intended level and remain 100% promoted in this 50-seed run.

The unresolved question is policy interpretation:

```text
Accept v1.1 for outcome-prior backprop:
  argument: source mix is corrected, templates are strong, family concentration is capped.

Hold v1.1 as validation only:
  argument: headline promotion is below clean-50/clean-100, curated/policy lanes are weaker, unique promoted families narrowed.
```

My default is to wait for Pro before backpropagating v1.1.
