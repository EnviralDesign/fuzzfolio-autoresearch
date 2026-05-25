# Play Hand Prior Test Clean 50 Report

Generated: `2026-05-25T22:40:02.733873+00:00`

## Batch Result

- Runs completed: 50/50 with 0 failures.
- Promotions: 32 promoted, 18 tombstoned (64% promotion rate).
- Final score: median 61.6598, average 41.3083, best 73.303.
- Template materialization: 24 exact-template branches (48%), 0 template-not-materialized rows.
- Branch selection: 34 mutated, 16 exact-template.
- Exact-template impact: 11 rescues, 5 exact-template outscored mutated, 8 mutated improved over an exact template.
- Source hit rates: discovered 100%, curated 36%, policy exploration 27%.
- Family concentration: top family share 14%; unique promoted pair/template families 8.

## Guided Source Mix

- Expected guided recipe source mix: `{"discovery_recipe_validation":0.6,"curated_recipe_prior":0.4}`
- Observed guided recipe source mix: `{"curated_recipe_prior":0.3143,"discovery_recipe_validation":0.6857}`
- Guided recipe runs by source: `{"discovery_recipe_validation":24,"curated_recipe_prior":11}`
- Template materialization rate by source: `{"curated_recipe_prior":0.0,"discovery_recipe_validation":1.0,"unknown":0.0}`
- Promotion rate by source: `{"curated_recipe_prior":0.3636,"discovery_recipe_validation":1.0,"unknown":0.2667}`

## Case Counts

- `policy_exploration`: 15
- `template_materialized_exact_passed_mutated_passed`: 13
- `no_template_curated_recipe`: 11
- `template_materialized_exact_passed_mutated_failed`: 11

## Top Recipes

| Recipe | Source | Runs | Promoted | Exact Selected | Mutated Selected | Best | Avg Positive |
|---|---:|---:|---:|---:|---:|---:|---:|
| discovered_recipe_003 | discovery_recipe_validation | 14 | 14 | 10 | 4 | 73.1268 | 67.8533 |
| discovered_recipe_006 | discovery_recipe_validation | 10 | 10 | 6 | 4 | 73.303 | 65.4298 |
| unknown | unknown | 15 | 4 | 0 | 15 | 68.0531 | 56.0463 |
| breakout_compression_release | curated_recipe_prior | 3 | 2 | 0 | 3 | 72.8843 | 60.2968 |
| mean_reversion_reclaim | curated_recipe_prior | 2 | 1 | 0 | 2 | 59.5044 | 59.5044 |
| profile_value_context | curated_recipe_prior | 2 | 1 | 0 | 2 | 56.8888 | 56.8888 |
| trend_pullback_continuation | curated_recipe_prior | 4 | 0 | 0 | 4 | 0.0 | None |

## Top Pair/Template Families

| Probe | Recipe | Class | Pair Source | Runs | Promoted | Exact Selected | Rescues | Avg Delta | Best |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | discovered_recipe_003 | template_locked | discovery_recipe_validation | 7 | 7 | 6 | 4 | -47.4368 | 72.2181 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | discovered_recipe_003 | template_guarded | discovery_recipe_validation | 7 | 7 | 4 | 2 | -18.2395 | 73.1268 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | discovered_recipe_006 | template_locked | discovery_recipe_validation | 5 | 5 | 3 | 3 | -32.4065 | 73.303 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | discovered_recipe_006 | template_locked | discovery_recipe_validation | 5 | 5 | 3 | 2 | -28.7395 | 72.8068 |
| l3-007-toby-crabel-narrow-range-market-mode-transition-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 2 | 1 | 0 | 0 | None | 72.8843 |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 59.5044 |
| l3-048-rolling-volume-profile-context-key-reversal-signal-m15 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 56.8888 |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | breakout_compression_release | under_sampled | anchor_pair_atlas | 1 | 1 | 0 | 0 | None | 47.7092 |
| l3-008-ma-slope-trend-pmo-crossover-m5 | trend_pullback_continuation | under_sampled | anchor_pair_atlas | 2 | 0 | 0 | 0 | None | 0.0 |
| l3-018-adx-market-mode-transition-m15 | trend_pullback_continuation | under_sampled | anchor_pair_atlas | 1 | 0 | 0 | 0 | None | 0.0 |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | mean_reversion_reclaim | under_sampled | anchor_pair_atlas | 1 | 0 | 0 | 0 | None | 0.0 |
| l3-046-rolling-volume-profile-context-toby-crabel-narrow-range-m15 | profile_value_context | under_sampled | anchor_pair_atlas | 1 | 0 | 0 | 0 | None | 0.0 |
| l3-010-adx-toby-crabel-narrow-range-m5 | trend_pullback_continuation | under_sampled | anchor_pair_atlas | 1 | 0 | 0 | 0 | None | 0.0 |

## Top Promoted Runs

| Seed | Score | Branch | Reason | Recipe | Pair |
|---:|---:|---|---|---|---|
| 246 | 73.303 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 |
| 215 | 73.1268 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 231 | 73.0629 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 |
| 224 | 72.8843 | mutated | mutated_branch_selected | breakout_compression_release | l3-007-toby-crabel-narrow-range-market-mode-transition-m15 |
| 229 | 72.8068 | mutated | mutated_branch_selected | discovered_recipe_006 | drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 |
| 209 | 72.2181 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 216 | 72.2181 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 243 | 72.2179 | exact_template | rescued_by_exact_template | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 210 | 70.9361 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 250 | 69.8456 | mutated | mutated_branch_selected | discovered_recipe_003 | drs-0008-r003-mfi-trend-obv-mean-reversion-m15 |
| 235 | 68.326 | exact_template | exact_template_outscored_mutated | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |
| 242 | 68.326 | exact_template | exact_template_outscored_mutated | discovered_recipe_003 | drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 |

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
