# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This packet covers the completed clean 100-seed Play Hand confirmation batch and compares it against the earlier clean 50-seed batch.

## What Changed Since Your Last Review

The clean 100-seed confirmation batch has finished and the report builder was tightened per your requests:

- The final packet only uses `batch_status.completed == 100`.
- `mutation_delta` is only computed when both exact-template and mutated branch scores exist.
- Blank, unknown, and policy-exploration rows are excluded from pair/template-family concentration metrics.
- Unique promoted pair/template families exclude blank/unknown families.
- Clean-50 and clean-100 family labels are computed independently before comparison.
- Report outputs now include family classification rules, data hygiene notes, mutation-delta metrics, source hit rates, family concentration, and clean-50 vs clean-100 comparison artifacts.

Command template:

```powershell
uv run play-hand --seed <seed> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Batch directory:

```text
runs/derived/playhand-prior-test-clean-100
```

Review packet directory:

```text
cgpt review/playhand-prior-test-clean-100
```

## New Report Artifacts

Please start with:

- `cgpt review/playhand-prior-test-clean-100/recipe-performance-report.md`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-comparison-clean50-vs-clean100.md`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-dashboard.html`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-report.json`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-comparison-clean50-vs-clean100.json`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-runs.csv`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-recipes.csv`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-pairs.csv`

Source batch artifacts copied for audit:

- `cgpt review/playhand-prior-test-clean-100/batch-status.json`
- `cgpt review/playhand-prior-test-clean-100/batch-run.log`
- `cgpt review/playhand-prior-test-clean-100/run-clean-100.ps1`

Reusable report builder:

- `scripts/build_playhand_prior_batch_report.py`

## Headline Clean-100 Results

The batch completed without execution failures:

- 100/100 runs completed.
- 73 promoted.
- 27 tombstoned.
- 0 failed.
- 73% promotion rate.
- Median final scrutiny score: 62.6199.
- Best final scrutiny score: 79.8725.

Guided prior materialization stayed healthy:

- 72 runs used `play_hand_seed_plan`.
- 28 runs used `role_balanced_policy_exploration`.
- 46 runs materialized exact retained-template branches.
- 0 retained templates failed to materialize.

Exact-template branch attribution stayed important:

- 69 selected the mutated branch.
- 31 selected the exact-template branch.
- 20 were `rescued_by_exact_template`.
- 11 were `exact_template_outscored_mutated`.
- 14 were `mutated_branch_selected` while a comparable exact template was present.

Case counts:

```text
policy_exploration: 28
no_template_curated_recipe: 26
template_materialized_exact_passed_mutated_passed: 24
template_materialized_exact_passed_mutated_failed: 20
template_materialized_both_failed: 1
template_materialized_exact_failed_mutated_passed: 1
```

Source hit rates:

```text
discovered_recipe_hit_rate: 45/46 = 97.83%
curated_recipe_hit_rate: 13/26 = 50.00%
policy_exploration_hit_rate: 15/28 = 53.57%
```

## Clean-50 vs Clean-100

The comparison artifact shows the core clean-50 signal mostly held:

```text
promotion_rate: 72% -> 73%
template_materialization_rate: 48% -> 46%
exact_rescue_rate: 29.17% -> 43.48%
mutation_improvement_rate: 33.33% -> 30.43%
policy_exploration_hit_rate: 33.33% -> 53.57%
discovered_recipe_hit_rate: 100% -> 97.83%
curated_recipe_hit_rate: 57.14% -> 50.00%
top_family_concentration_share: 12% -> 15%
unique_promoted_pair_families: 13 -> 16
median_final_score: 63.013 -> 62.6199
best_final_score: 75.1727 -> 79.8725
```

The main change from clean-50 is not collapse. It is a clearer split between discovered templates, exact-template rescue behavior, and policy exploration still producing occasional wins.

## Family-Level Read

Current top discovered/template families:

- `drs-0002-r006-rsi-crossback-willr-mean-reversi-m5`: 15/15 promoted, classified `template_locked`, 10 exact rescues, avg mutation delta -44.3961.
- `drs-0003-r006-willr-mean-reversi-rsi-crossback-m5`: 12/12 promoted, classified `template_guarded`, 4 exact rescues, avg mutation delta -22.285.
- `drs-0008-r003-mfi-trend-obv-mean-reversion-m15`: 7/7 promoted, classified `template_guarded`, 2 exact rescues, avg mutation delta -18.328.
- `drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5`: 6/6 promoted, classified `template_guarded`, 2 exact rescues, avg mutation delta -26.2874.
- `l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5`: 5/7 promoted, classified `unstable`.

Recipe-level highlights:

- `discovered_recipe_006`: 27/27 promoted, classified `template_locked`.
- `discovered_recipe_003`: 13/13 promoted, classified `template_guarded`.
- `discovered_recipe_002`: 3/3 promoted, but still thin.
- `mean_reversion_reclaim`: 7/11 promoted.
- `breakout_compression_release`: 3/4 promoted.
- `trend_pullback_continuation`: 1/7 promoted.
- `profile_value_context`: 2/4 promoted.

## My Current Read

This looks like confirmation that the prior/template loop is useful:

- The clean-50 promotion rate held on clean-100.
- Discovered recipe validation remained much stronger than curated recipe priors.
- Exact-template preservation was not optional; 20 clean-100 runs were rescued by exact template.
- Mutation still helps sometimes, but the family-level data is leaning more template-protective than mutation-friendly for the best retained families.
- Family concentration increased only modestly after excluding unknown/policy rows, and unique promoted pair families increased from 13 to 16.
- Policy exploration is still worth preserving because it promoted 15/28 in clean-100.

My tentative conclusion is: keep global `70/20/10`, add family caps and family mutation policies, and do not broaden discovery until Play Hand can feed these family outcomes back into priors cleanly.

## Questions For This Review

1. Does clean-100 confirm the clean-50 signal strongly enough to move from measurement/reporting into family-aware Play Hand policy?
2. Should `drs-0002` be treated as `template_locked` immediately, given 15/15 promoted and 10 exact rescues?
3. Should `drs-0003`, `drs-0008`, and `drs-0001` be `template_guarded` rather than mutation-friendly, given negative average mutation deltas?
4. Should `trend_pullback_continuation` and weak curated anchor-pair menus be demoted now, or only capped pending more data?
5. Is global `70/20/10` still right, or should we keep global policy stable and only adjust inside guided sampling?
6. What family caps would you use now: max share per exact pair/template family and max share per discovered recipe?
7. Should the next implementation branch be:
   - family-aware mutation policy (`template_locked`, `template_guarded`, `mutation_friendly`);
   - family concentration caps;
   - outcome backprop from Play Hand reports into recipe priors;
   - pair-plus / conditional third-indicator testing;
   - or another confirmation batch?
8. Is it time to start using these Play Hand batch results as negative evidence against weak curated recipes and mutation-heavy expansions?

## Verification

Commands run:

```powershell
uv run python scripts\build_playhand_prior_batch_report.py --batch-dir runs\derived\playhand-prior-test-clean-100 --review-dir "cgpt review\playhand-prior-test-clean-100"
uv run python scripts\build_playhand_prior_batch_report.py --batch-dir runs\derived\playhand-prior-test-clean-50 --out-dir runs\derived\report-smoke-clean50
uv run python scripts\build_playhand_prior_batch_report.py --batch-dir runs\derived\playhand-prior-test-clean-100 --out-dir runs\derived\report-smoke-clean100
uv run python -m py_compile scripts\build_playhand_prior_batch_report.py
```

The final clean-100 status is:

```text
completed: 100
failed: 0
status: completed
finished_at: 2026-05-24T17:17:48.3829003Z
```
