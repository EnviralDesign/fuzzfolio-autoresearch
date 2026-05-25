# Play Hand Prior Test Clean 50 Report

Generated: `2026-05-25T11:35:14.997910+00:00`

## Batch Result

- Runs completed: 50/50 with 0 failures.
- Promotions: 27 promoted, 23 tombstoned (54% promotion rate).
- Final score: median 43.9738, average 33.0925, best 75.8434.
- Template materialization: 10 exact-template branches (20%), 0 template-not-materialized rows.
- Branch selection: 43 mutated, 7 exact-template.
- Exact-template impact: 4 rescues, 3 exact-template outscored mutated, 3 mutated improved over an exact template.
- Source hit rates: discovered 100%, curated 35%, policy exploration 53%.
- Family concentration: top family share 8%; unique promoted pair/template families 13.

## Case Counts

- `no_template_curated_recipe`: 23
- `policy_exploration`: 17
- `template_materialized_exact_passed_mutated_passed`: 6
- `template_materialized_exact_passed_mutated_failed`: 4

## Top Recipes

| Recipe | Source | Runs | Promoted | Exact Selected | Mutated Selected | Best | Avg Positive |
|---|---:|---:|---:|---:|---:|---:|---:|
| unknown | unknown | 17 | 9 | 0 | 17 | 71.8764 | 61.2879 |
| discovered_recipe_006 | discovery_recipe_validation | 7 | 7 | 5 | 2 | 72.3354 | 64.6468 |
| breakout_compression_release | curated_recipe_prior | 5 | 3 | 0 | 5 | 71.3214 | 57.2075 |
| discovered_recipe_003 | discovery_recipe_validation | 2 | 2 | 1 | 1 | 75.8434 | 72.0331 |
| profile_value_context | curated_recipe_prior | 7 | 2 | 0 | 7 | 68.865 | 61.0062 |
| mean_reversion_reclaim | curated_recipe_prior | 3 | 2 | 0 | 3 | 52.2944 | 48.3852 |
| trend_pullback_continuation | curated_recipe_prior | 8 | 1 | 0 | 8 | 60.1512 | 60.1512 |
| discovered_recipe_005 | discovery_recipe_validation | 1 | 1 | 1 | 0 | 55.8845 | 55.8845 |

## Top Pair/Template Families

| Probe | Recipe | Class | Pair Source | Runs | Promoted | Exact Selected | Rescues | Avg Delta | Best |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | discovered_recipe_006 | template_locked | discovery_recipe_validation | 4 | 4 | 3 | 2 | -29.5526 | 72.3354 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | discovered_recipe_006 | template_guarded | discovery_recipe_validation | 3 | 3 | 2 | 1 | -26.7917 | 68.7066 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | discovered_recipe_003 | under_sampled | discovery_recipe_validation | 1 | 1 | 0 | 0 | 13.2146 | 75.8434 |
| l3-023-bollinger-keltner-squeeze-filter-market-mode-transition-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 71.3214 |
| l3-041-rolling-volume-profile-context-channel-reentry-m15 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 68.865 |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | discovered_recipe_003 | under_sampled | discovery_recipe_validation | 1 | 1 | 1 | 1 | -68.2227 | 68.2227 |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | trend_pullback_continuation | unstable | anchor_pair_atlas | 3 | 1 | 0 | 0 | None | 60.1512 |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | breakout_compression_release | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 56.8298 |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | discovered_recipe_005 | under_sampled | discovery_recipe_validation | 1 | 1 | 1 | 0 | -0.825 | 55.8845 |
| l3-036-rolling-volume-profile-context-ttf-dsl-transition-m15 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 53.1474 |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 2 | 1 | 0 | 0 | None | 52.2944 |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 44.4761 |
| l3-011-toby-crabel-narrow-range-ttf-dsl-transition-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 43.4714 |
| l3-008-ma-slope-trend-pmo-crossover-m5 | trend_pullback_continuation | under_sampled | anchor_pair_atlas | 2 | 0 | 0 | 0 | None | 0.0 |
| l3-032-rolling-volume-profile-context-toby-crabel-narrow-range-m5 | profile_value_context | under_sampled | anchor_pair_atlas | 2 | 0 | 0 | 0 | None | 0.0 |

## Top Promoted Runs

| Seed | Score | Branch | Reason | Recipe | Pair |
|---:|---:|---|---|---|---|
| 174 | 75.8434 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 183 | 72.3354 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |
| 167 | 71.8764 | mutated | mutated_branch_selected |  |  |
| 199 | 71.3214 | mutated | mutated_branch_selected | breakout_compression_release | l3-023-bollinger-keltner-squeeze-filter-market-mode-transition-m15 |
| 197 | 68.865 | mutated | mutated_branch_selected | profile_value_context | l3-041-rolling-volume-profile-context-channel-reentry-m15 |
| 177 | 68.7066 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 |
| 155 | 68.2227 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 178 | 67.0565 | mutated | mutated_branch_selected |  |  |
| 193 | 66.9036 | mutated | mutated_branch_selected |  |  |
| 179 | 66.6066 | mutated | mutated_branch_selected |  |  |
| 189 | 66.4992 | mutated | mutated_branch_selected |  |  |
| 157 | 64.9069 | mutated | mutated_branch_selected |  |  |

## Family Classification Rules

```text
under_sampled: count < 3
template_locked: exact_rescue_rate >= 0.40
mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3
template_guarded: exact_selected_rate >= 0.40
unstable: otherwise
```

## Data Hygiene

- `batch_status.completed == batch_status.total`: True (50/50).
- `mutation_delta` is only computed when both `exact_template_score` and `mutated_score` are non-null.
- Policy-exploration and blank/unknown rows are excluded from pair/template-family concentration metrics.
- Unique promoted pair/template families excludes blank/unknown families.
- Clean-50 and current-batch family labels are computed independently before comparison.

## Interpretation

- The guided prior path is materializing real candidates and producing a high promotion rate in this controlled run.
- Exact-template control branches materially changed outcomes: several runs were promoted only because the retained template survived while the mutated branch failed.
- Mutations also improved on retained templates in multiple cases, which means the prior system is useful as a starting region rather than only as a replay mechanism.
- The next decision should focus on whether the promoted families are sufficiently diverse and whether exact-template rescues should feed back as stronger template-preservation priors.
