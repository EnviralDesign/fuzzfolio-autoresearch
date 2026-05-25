# Recipe Performance Comparison: Clean 100 vs Clean 50

Generated: `2026-05-25T22:41:44.165623+00:00`

## Family Classification Rules

```text
under_sampled: count < 3
template_locked: exact_rescue_rate >= 0.40
mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3
template_guarded: exact_selected_rate >= 0.40
unstable: otherwise
```

## Data Hygiene

- `mutation_delta` is null unless both branch scores exist.
- Policy-exploration and blank/unknown rows are excluded from pair/template-family concentration metrics.
- Unique promoted pair/template families excludes blank/unknown families.
- Previous and current family labels are computed independently before comparison.

## Metric Deltas

| Metric | Previous | Current | Delta |
|---|---:|---:|---:|
| `promotion_rate` | 0.73 | 0.64 | -0.09 |
| `template_materialization_rate` | 0.46 | 0.48 | 0.02 |
| `exact_rescue_rate` | 0.4348 | 0.4583 | 0.0235 |
| `mutation_improvement_rate` | 0.3043 | 0.3333 | 0.029 |
| `policy_exploration_hit_rate` | 0.5357 | 0.2667 | -0.269 |
| `discovered_recipe_exposure` | 0.46 | 0.48 | 0.02 |
| `curated_recipe_exposure` | 0.26 | 0.22 | -0.04 |
| `policy_exploration_exposure` | 0.28 | 0.3 | 0.02 |
| `discovered_recipe_hit_rate` | 0.9783 | 1.0 | 0.0217 |
| `curated_recipe_hit_rate` | 0.5 | 0.3636 | -0.1364 |
| `top_family_concentration_share` | 0.15 | 0.14 | -0.01 |
| `unique_promoted_pair_families` | 16 | 8 | -8.0 |
| `median_final_score` | 62.6199 | 61.6598 | -0.9601 |
| `average_final_score` | 46.8045 | 41.3083 | -5.4962 |
| `best_final_score` | 79.8725 | 73.303 | -6.5695 |

## Exposure Deltas

| Metric | Delta |
|---|---:|
| `discovered_recipe_exposure_delta` | 0.02 |
| `curated_recipe_exposure_delta` | -0.04 |
| `policy_exploration_exposure_delta` | 0.02 |

## Family Classification Changes

| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_guarded | template_locked | 6 | 7 | 1.0 | 0.5714 | 0.1429 | -47.4368 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_guarded | template_locked | 12 | 5 | 1.0 | 0.4 | 0.4 | -28.7395 |
| l3-007-toby-crabel-narrow-range-market-mode-transition-m15 | None | under_sampled | 0 | 2 | 0.5 | 0.0 | None | None |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | unstable | under_sampled | 7 | 1 | 1.0 | 0.0 | None | None |
| l3-048-rolling-volume-profile-context-key-reversal-signal-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | unstable | under_sampled | 3 | 2 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-018-adx-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | under_sampled | None | 2 | 0 | None | None | None | None |
| drv-0007-r001-stochf-trend-cmo-mean-reversion-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-003-bbands-position-mean-reversion-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-006-adx-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-011-toby-crabel-narrow-range-ttf-dsl-transition-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-013-kalman-velocity-confirm-market-mode-transition-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | under_sampled | None | 2 | 0 | None | None | None | None |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | under_sampled | None | 2 | 0 | None | None | None | None |
| l3-031-rolling-volume-profile-context-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-038-bbands-position-mean-reversion-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-043-rolling-volume-profile-context-pmo-crossover-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-047-rolling-volume-profile-context-rsi-crossback-m5 | under_sampled | None | 1 | 0 | None | None | None | None |

## Top Current Families

| Family | Class | Runs | Promoted | Promotion Rate | Rescue Rate | Mutated Win Rate | Avg Mutation Delta |
|---|---|---:|---:|---:|---:|---:|---:|
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_locked | 7 | 7 | 1.0 | 0.5714 | 0.1429 | -47.4368 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | template_guarded | 7 | 7 | 1.0 | 0.2857 | 0.4286 | -18.2395 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_locked | 5 | 5 | 1.0 | 0.6 | 0.4 | -32.4065 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_locked | 5 | 5 | 1.0 | 0.4 | 0.4 | -28.7395 |
| l3-007-toby-crabel-narrow-range-market-mode-transition-m15 | under_sampled | 2 | 1 | 0.5 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-048-rolling-volume-profile-context-key-reversal-signal-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | under_sampled | 2 | 0 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-018-adx-market-mode-transition-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-046-rolling-volume-profile-context-toby-crabel-narrow-range-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | None | 0 | 0 | None | None | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | None | 0 | 0 | None | None | None | None |
| drv-0007-r001-stochf-trend-cmo-mean-reversion-m5 | None | 0 | 0 | None | None | None | None |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | None | 0 | 0 | None | None | None | None |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | None | 0 | 0 | None | None | None | None |
| l3-003-bbands-position-mean-reversion-channel-reentry-m5 | None | 0 | 0 | None | None | None | None |
| l3-006-adx-pmo-crossover-m5 | None | 0 | 0 | None | None | None | None |
