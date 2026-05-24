# Play Hand Prior Test Clean 100 Report

Generated: `2026-05-24T17:18:46.806946+00:00`

## Batch Result

- Runs completed: 100/100 with 0 failures.
- Promotions: 73 promoted, 27 tombstoned (73% promotion rate).
- Final score: median 62.6199, average 46.8045, best 79.8725.
- Template materialization: 46 exact-template branches (46%), 0 template-not-materialized rows.
- Branch selection: 69 mutated, 31 exact-template.
- Exact-template impact: 20 rescues, 11 exact-template outscored mutated, 14 mutated improved over an exact template.
- Source hit rates: discovered 98%, curated 50%, policy exploration 54%.
- Family concentration: top family share 15%; unique promoted pair/template families 16.

## Case Counts

- `policy_exploration`: 28
- `no_template_curated_recipe`: 26
- `template_materialized_exact_passed_mutated_passed`: 24
- `template_materialized_exact_passed_mutated_failed`: 20
- `template_materialized_both_failed`: 1
- `template_materialized_exact_failed_mutated_passed`: 1

## Top Recipes

| Recipe | Source | Runs | Promoted | Exact Selected | Mutated Selected | Best | Avg Positive |
|---|---:|---:|---:|---:|---:|---:|---:|
| discovered_recipe_006 | discovery_recipe_validation | 27 | 27 | 20 | 7 | 75.3234 | 66.5509 |
| unknown | unknown | 28 | 15 | 0 | 28 | 75.8862 | 59.722 |
| discovered_recipe_003 | discovery_recipe_validation | 13 | 13 | 8 | 5 | 79.8725 | 67.2731 |
| mean_reversion_reclaim | curated_recipe_prior | 11 | 7 | 0 | 11 | 71.3531 | 62.3782 |
| breakout_compression_release | curated_recipe_prior | 4 | 3 | 0 | 4 | 70.7145 | 66.2147 |
| discovered_recipe_002 | discovery_recipe_validation | 3 | 3 | 1 | 2 | 57.0037 | 55.1362 |
| profile_value_context | curated_recipe_prior | 4 | 2 | 0 | 4 | 64.9186 | 60.3127 |
| discovered_recipe_007 | discovery_recipe_validation | 1 | 1 | 1 | 0 | 73.1507 | 73.1507 |
| trend_pullback_continuation | curated_recipe_prior | 7 | 1 | 0 | 7 | 64.5722 | 64.5722 |
| discovered_recipe_001 | discovery_recipe_validation | 2 | 1 | 1 | 1 | 54.1516 | 54.1516 |

## Top Pair/Template Families

| Probe | Recipe | Class | Pair Source | Runs | Promoted | Exact Selected | Rescues | Avg Delta | Best |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | discovered_recipe_006 | template_locked | discovery_recipe_validation | 15 | 15 | 13 | 10 | -44.3961 | 73.2977 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | discovered_recipe_006 | template_guarded | discovery_recipe_validation | 12 | 12 | 7 | 4 | -22.285 | 75.3234 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | discovered_recipe_003 | template_guarded | discovery_recipe_validation | 7 | 7 | 4 | 2 | -18.328 | 71.7302 |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | discovered_recipe_003 | template_guarded | discovery_recipe_validation | 6 | 6 | 4 | 2 | -26.2874 | 79.8725 |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | mean_reversion_reclaim | unstable | anchor_pair_atlas | 7 | 5 | 0 | 0 | None | 65.4807 |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 2 | 2 | 0 | 0 | None | 70.7145 |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | discovered_recipe_002 | under_sampled | discovery_recipe_validation | 2 | 2 | 1 | 1 | -26.3822 | 54.485 |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | discovered_recipe_007 | under_sampled | discovery_recipe_validation | 1 | 1 | 1 | 1 | -73.1507 | 73.1507 |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 71.3531 |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 66.2054 |
| l3-031-rolling-volume-profile-context-pmo-crossover-m5 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 64.9186 |
| l3-008-ma-slope-trend-pmo-crossover-m5 | trend_pullback_continuation | unstable | anchor_pair_atlas | 3 | 1 | 0 | 0 | None | 64.5722 |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 58.7141 |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | discovered_recipe_002 | under_sampled | discovery_recipe_validation | 1 | 1 | 0 | 0 | 57.0037 | 57.0037 |
| l3-047-rolling-volume-profile-context-rsi-crossback-m5 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 55.7069 |

## Top Promoted Runs

| Seed | Score | Branch | Reason | Recipe | Pair |
|---:|---:|---|---|---|---|
| 136 | 79.8725 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 97 | 75.8862 | mutated | mutated_branch_selected |  |  |
| 76 | 75.3234 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |
| 114 | 73.2977 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 |
| 128 | 73.1507 | exact_template | rescued_by_exact_template | discovered_recipe_007 | drv-0058-r007-channel-reentry-bbands-position-tr-m15 |
| 68 | 71.7302 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 56 | 71.712 | mutated | mutated_branch_selected |  |  |
| 133 | 71.3531 | mutated | mutated_branch_selected | mean_reversion_reclaim | l3-020-bbands-position-mean-reversion-channel-reentry-m15 |
| 70 | 70.9631 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |
| 125 | 70.7768 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 54 | 70.7145 | mutated | mutated_branch_selected | breakout_compression_release | l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 |
| 129 | 70.6405 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |

## Family Classification Rules

```text
under_sampled: count < 3
template_locked: exact_rescue_rate >= 0.40
mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3
template_guarded: exact_selected_rate >= 0.40
unstable: otherwise
```

## Data Hygiene

- `batch_status.completed == batch_status.total`: True (100/100).
- `mutation_delta` is only computed when both `exact_template_score` and `mutated_score` are non-null.
- Policy-exploration and blank/unknown rows are excluded from pair/template-family concentration metrics.
- Unique promoted pair/template families excludes blank/unknown families.
- Clean-50 and current-batch family labels are computed independently before comparison.

## Interpretation

- The guided prior path is materializing real candidates and producing a high promotion rate in this controlled run.
- Exact-template control branches materially changed outcomes: several runs were promoted only because the retained template survived while the mutated branch failed.
- Mutations also improved on retained templates in multiple cases, which means the prior system is useful as a starting region rather than only as a replay mechanism.
- The next decision should focus on whether the promoted families are sufficiently diverse and whether exact-template rescues should feed back as stronger template-preservation priors.
