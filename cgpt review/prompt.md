# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as the technical/design reviewer. This packet covers the completed `clean-50-family-policy-v1` controlled Play Hand validation batch after the family-aware policy layer was implemented.

## Start Here

New packet:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-report.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-report.json`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-runs.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-pairs.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-recipes.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-dashboard.html`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/comparison-index.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/workflow-notes.md`

Comparison artifacts:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-comparison-clean50-vs-clean50.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/recipe-performance-comparison-clean100-vs-clean50.md`

The comparison filenames come from the script's batch-size labels. In this packet, the current `Clean 50` side is `clean-50-family-policy-v1`.

Previous context:

- `cgpt review/family-policy-v1/`
- `cgpt review/playhand-prior-test-clean-50/`
- `cgpt review/playhand-prior-test-clean-100/`

## What Ran

Controlled family-policy confirmation batch:

```powershell
uv run play-hand --seed <151..200> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Batch status:

```text
completed: 50/50
failed: 0
skipped: 0
```

## Headline Result

```text
promoted: 27/50
tombstoned: 23/50
promotion_rate: 54%
median_final_score: 43.9738
best_final_score: 75.8434
exact_template_branches: 10
selected_branch: 43 mutated, 7 exact_template
exact_template_rescues: 4
exact_template_outscored_mutated: 3
mutated_improved_over_exact: 3
```

Source hit rates from the report:

```text
discovered: 100%
curated: 35%
policy_exploration: 53%
```

Case counts:

```text
no_template_curated_recipe: 23
policy_exploration: 17
template_materialized_exact_passed_mutated_passed: 6
template_materialized_exact_passed_mutated_failed: 4
```

## Initial Interpretation

The run is operationally clean: all 50 seeds completed, no failures, exact-template materialization worked, and the exact branch still produced real rescues.

However, the headline promotion rate is materially lower than the previous controlled batches:

```text
original clean-50: 36/50 promoted, 72%
clean-100: 73/100 promoted, 73%
family-policy-v1 clean-50: 27/50 promoted, 54%
```

This may be because family-policy-v1 reduced replay/concentration and allowed more weaker curated or policy-exploration hands through. It may also indicate that the new caps/fill limits are too conservative or that the current controlled range sampled a harder distribution.

I have not rebuilt cached outcome priors from this new family-policy-v1 packet yet. I want your read before treating it as a new feedback source.

## Questions For Pro

1. Is this family-policy-v1 run clean enough to accept into `build-playhand-outcome-priors`, despite the lower promotion rate?
2. Do the lower headline results suggest a policy regression, or just less template replay/concentration?
3. Should the next step be to rebuild cached priors from this packet, or first tune family caps/fill limits?
4. Should we split the next diagnostic into guided-template-only, curated-only, and policy-exploration-only lanes?
5. Does the exact-template rescue rate still justify keeping the family-aware exact branch as-is?
6. Should normal overnight Play Hand resume now, while controlled prior refresh waits for this review?

## Current Local Executor Recommendation

Do not blindly backprop this batch yet. I would treat it as a clean validation packet and ask whether the lower promotion rate is acceptable evidence or a tuning signal. If you approve it, I will run:

```powershell
uv run build-playhand-outcome-priors --json
uv run build-recipe-priors --json
```

Then I will commit the rebuilt priors and resume normal overnight Play Hand loops.
