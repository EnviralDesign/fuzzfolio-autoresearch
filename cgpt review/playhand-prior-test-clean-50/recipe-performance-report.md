# Play Hand Prior Test Clean 50 Report

Generated: `2026-05-23T15:42:12.313584+00:00`

## Batch Result

- Runs completed: 50/50 with 0 failures.
- Promotions: 36 promoted, 14 tombstoned (72% promotion rate).
- Final score: median 63.013, average 46.4158, best 75.1727.
- Template materialization: 24 exact-template branches, 0 template-not-materialized rows.
- Branch selection: 34 mutated, 16 exact-template.
- Exact-template impact: 7 rescues, 9 exact-template outscored mutated, 8 mutated improved over an exact template.

## Case Counts

- `template_materialized_exact_passed_mutated_passed`: 17
- `no_template_curated_recipe`: 14
- `policy_exploration`: 12
- `template_materialized_exact_passed_mutated_failed`: 7

## Top Recipes

| Recipe | Source | Runs | Promoted | Exact Selected | Mutated Selected | Best | Avg Positive |
|---|---:|---:|---:|---:|---:|---:|---:|
| discovered_recipe_003 | discovery_recipe_validation | 10 | 10 | 5 | 5 | 74.447 | 67.6769 |
| discovered_recipe_006 | discovery_recipe_validation | 9 | 9 | 7 | 2 | 74.7581 | 67.7536 |
| mean_reversion_reclaim | curated_recipe_prior | 8 | 5 | 0 | 8 | 75.1727 | 64.3968 |
| unknown | unknown | 12 | 4 | 0 | 12 | 64.1372 | 59.8406 |
| discovered_recipe_001 | discovery_recipe_validation | 3 | 3 | 3 | 0 | 54.3919 | 54.2313 |
| trend_pullback_continuation | curated_recipe_prior | 2 | 2 | 0 | 2 | 71.3145 | 58.2574 |
| breakout_compression_release | curated_recipe_prior | 4 | 1 | 0 | 4 | 73.6126 | 73.6126 |
| discovered_recipe_002 | discovery_recipe_validation | 1 | 1 | 0 | 1 | 64.186 | 64.186 |
| discovered_recipe_005 | discovery_recipe_validation | 1 | 1 | 1 | 0 | 55.8845 | 55.8845 |

## Top Pair/Template Families

| Probe | Recipe | Pair Source | Runs | Promoted | Exact Selected | Rescues | Best |
|---|---|---:|---:|---:|---:|---:|---:|
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | discovered_recipe_006 | discovery_recipe_validation | 6 | 6 | 5 | 2 | 74.7581 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | discovered_recipe_003 | discovery_recipe_validation | 6 | 6 | 1 | 0 | 74.447 |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | discovered_recipe_003 | discovery_recipe_validation | 4 | 4 | 4 | 4 | 68.3227 |
| unknown | unknown | unknown | 12 | 4 | 0 | 0 | 64.1372 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | discovered_recipe_006 | discovery_recipe_validation | 3 | 3 | 2 | 0 | 72.772 |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | discovered_recipe_001 | discovery_recipe_validation | 3 | 3 | 3 | 1 | 54.3919 |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | mean_reversion_reclaim | anchor_pair_atlas | 3 | 2 | 0 | 0 | 75.1727 |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | mean_reversion_reclaim | anchor_pair_atlas | 2 | 2 | 0 | 0 | 70.3513 |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | breakout_compression_release | anchor_pair_atlas | 1 | 1 | 0 | 0 | 73.6126 |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | trend_pullback_continuation | anchor_pair_atlas | 1 | 1 | 0 | 0 | 71.3145 |
| l3-030-bbands-position-mean-reversion-rsi-crossback-m5 | mean_reversion_reclaim | anchor_pair_atlas | 1 | 1 | 0 | 0 | 67.572 |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | discovered_recipe_002 | discovery_recipe_validation | 1 | 1 | 0 | 0 | 64.186 |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | discovered_recipe_005 | discovery_recipe_validation | 1 | 1 | 1 | 0 | 55.8845 |
| l3-022-adx-ttf-dsl-transition-m15 | trend_pullback_continuation | anchor_pair_atlas | 1 | 1 | 0 | 0 | 45.2002 |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | mean_reversion_reclaim | anchor_pair_atlas | 1 | 0 | 0 | 0 | 0.0 |

## Top Promoted Runs

| Seed | Score | Branch | Reason | Recipe | Pair |
|---:|---:|---|---|---|---|
| 36 | 75.1727 | mutated | mutated_branch_selected | mean_reversion_reclaim | l3-042-bbands-position-mean-reversion-rsi-crossback-m15 |
| 31 | 74.7581 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |
| 3 | 74.447 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 23 | 73.6126 | mutated | mutated_branch_selected | breakout_compression_release | l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 |
| 47 | 72.772 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 |
| 50 | 71.3145 | mutated | mutated_branch_selected | trend_pullback_continuation | l3-024-kalman-velocity-confirm-pmo-crossover-m15 |
| 30 | 70.3513 | mutated | mutated_branch_selected | mean_reversion_reclaim | l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 |
| 38 | 70.2632 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 35 | 69.0711 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 4 | 68.3227 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 43 | 68.3223 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 12 | 68.3184 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |

## Interpretation

- The guided prior path is materializing real candidates and producing a high promotion rate in this controlled run.
- Exact-template control branches materially changed outcomes: several runs were promoted only because the retained template survived while the mutated branch failed.
- Mutations also improved on retained templates in multiple cases, which means the prior system is useful as a starting region rather than only as a replay mechanism.
- The next decision should focus on whether the promoted families are sufficiently diverse and whether exact-template rescues should feed back as stronger template-preservation priors.
