# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as the technical/design reviewer. This packet covers the completed `clean-50-family-policy-v1.1` source-mix confirmation batch.

## Start Here

New packet:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-report.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-report.json`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-runs.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-pairs.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-recipes.csv`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-dashboard.html`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/comparison-index.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/workflow-notes.md`

Explicit comparisons:

- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-comparison-original-clean50-vs-family-policy-v1.1.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-comparison-clean100-vs-family-policy-v1.1.md`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1/recipe-performance-comparison-family-policy-v1-vs-v1.1.md`

Previous context:

- `cgpt review/playhand-prior-test-clean-50/`
- `cgpt review/playhand-prior-test-clean-100/`
- `cgpt review/family-policy-v1/`
- `cgpt review/playhand-prior-test-clean-50-family-policy-v1/`

## What Ran

Controlled family-policy v1.1 source-mix confirmation batch:

```powershell
uv run play-hand --seed <201..250> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Batch status:

```text
completed: 50/50
failed: 0
skipped: 0
```

## Headline Result

```text
promoted: 32/50
tombstoned: 18/50
promotion_rate: 64%
median_final_score: 61.6598
average_final_score: 41.3083
best_final_score: 73.303
template_materialized: 24/50, 48%
selected_branch: 34 mutated, 16 exact_template
exact_template_rescues: 11
exact_template_outscored_mutated: 5
mutated_improved_over_exact: 8
top_family_concentration_share: 14%
unique_promoted_pair_families: 8
```

Source exposure and hit rates:

```text
discovered_recipe_validation: 24 runs, 24 promoted, 100%
curated_recipe_prior: 11 runs, 4 promoted, 36.36%
policy_exploration: 15 runs, 4 promoted, 26.67%
guided_source_mix_expected: 60% discovered / 40% curated
guided_source_mix_observed: 68.57% discovered / 31.43% curated
bucket_fallbacks: 0
```

Case counts:

```text
policy_exploration: 15
template_materialized_exact_passed_mutated_passed: 13
no_template_curated_recipe: 11
template_materialized_exact_passed_mutated_failed: 11
```

## Initial Interpretation

v1.1 looks like the source-mix correction worked. Compared with family-policy-v1, discovered/template exposure recovered strongly:

```text
promotion_rate: 54% -> 64%
template_materialization_rate: 20% -> 48%
discovered_recipe_exposure: 20% -> 48%
curated_recipe_exposure: 46% -> 22%
policy_exploration_exposure: 34% -> 30%
```

Compared with clean-100, v1.1 is less productive overall but has similar discovered/template exposure and slightly lower top-family concentration:

```text
promotion_rate: 73% -> 64%
template_materialization_rate: 46% -> 48%
exact_rescue_rate: 43.48% -> 45.83%
discovered_recipe_exposure: 46% -> 48%
top_family_concentration_share: 15% -> 14%
```

The four discovered validation pair families all remained 100% promoted in this run. The weaker parts are curated recipes and policy exploration.

I have not rebuilt cached outcome priors from v1.1 yet. Please decide whether this packet should be accepted for backprop.

## Questions For Pro

1. Is `clean-50-family-policy-v1.1` clean enough to accept into `build-playhand-outcome-priors`, or should it remain validation-only for now?
2. Does the 64% promotion rate represent an acceptable tradeoff for lower concentration and better source balance, or is it still a policy regression versus clean-50/clean-100?
3. Should discovered validation families be promoted harder while curated recipes and policy exploration are capped or demoted?
4. Should `trend_pullback_continuation` be demoted immediately after going 0/4 in v1.1, or is the sample too small?
5. Should exact-template rescue pressure be strengthened for `template_locked` families, given 11 rescues and strong negative mutation deltas?
6. If v1.1 is accepted, should the next local action be:

```powershell
uv run build-playhand-outcome-priors --json
uv run build-recipe-priors --json
```

followed by a normal overnight Play Hand loop?

## Current Local Executor Recommendation

Do not backprop v1.1 until you review it. My read is that v1.1 fixed the v1 source-mix regression and is probably usable, but it also confirms that curated and policy-exploration lanes need more selective handling.
