# Recipe Performance Comparison: Clean 50 vs Clean 100

Generated: `2026-05-24T17:18:46.831759+00:00`

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
| `promotion_rate` | 0.72 | 0.73 | 0.01 |
| `template_materialization_rate` | 0.48 | 0.46 | -0.02 |
| `exact_rescue_rate` | 0.2917 | 0.4348 | 0.1431 |
| `mutation_improvement_rate` | 0.3333 | 0.3043 | -0.029 |
| `policy_exploration_hit_rate` | 0.3333 | 0.5357 | 0.2024 |
| `discovered_recipe_hit_rate` | 1.0 | 0.9783 | -0.0217 |
| `curated_recipe_hit_rate` | 0.5714 | 0.5 | -0.0714 |
| `top_family_concentration_share` | 0.12 | 0.15 | 0.03 |
| `unique_promoted_pair_families` | 13 | 16 | 3.0 |
| `median_final_score` | 63.013 | 62.6199 | -0.3931 |
| `average_final_score` | 46.4158 | 46.8045 | 0.3887 |
| `best_final_score` | 75.1727 | 79.8725 | 4.6998 |

## Family Classification Changes

| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_guarded | template_locked | 3 | 15 | 1.0 | 0.6667 | 0.1333 | -44.3961 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | mutation_friendly | template_guarded | 6 | 7 | 1.0 | 0.2857 | 0.4286 | -18.328 |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_locked | template_guarded | 4 | 6 | 1.0 | 0.3333 | 0.3333 | -26.2874 |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | under_sampled | unstable | 2 | 7 | 0.7143 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | None | unstable | 0 | 3 | 0.3333 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | template_guarded | under_sampled | 3 | 1 | 1.0 | 0.0 | 0.0 | -2.2208 |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | 1.0 | 57.0037 |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | None | under_sampled | 0 | 1 | 1.0 | 1.0 | 0.0 | -73.1507 |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-031-rolling-volume-profile-context-pmo-crossover-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-047-rolling-volume-profile-context-rsi-crossback-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| drv-0007-r001-stochf-trend-cmo-mean-reversion-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | 0.0 | 0.0 |
| l3-003-bbands-position-mean-reversion-channel-reentry-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-006-adx-pmo-crossover-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-011-toby-crabel-narrow-range-ttf-dsl-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-013-kalman-velocity-confirm-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-038-bbands-position-mean-reversion-pmo-crossover-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-043-rolling-volume-profile-context-pmo-crossover-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-046-rolling-volume-profile-context-toby-crabel-narrow-range-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-014-bollinger-keltner-squeeze-filter-toby-crabel-narrow-range-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-022-adx-ttf-dsl-transition-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-027-toby-crabel-narrow-range-candle-direction-index-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-030-bbands-position-mean-reversion-rsi-crossback-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | unstable | None | 3 | 0 | None | None | None | None |

## Top Current Families

| Family | Class | Runs | Promoted | Promotion Rate | Rescue Rate | Mutated Win Rate | Avg Mutation Delta |
|---|---|---:|---:|---:|---:|---:|---:|
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_locked | 15 | 15 | 1.0 | 0.6667 | 0.1333 | -44.3961 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_guarded | 12 | 12 | 1.0 | 0.3333 | 0.4167 | -22.285 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | template_guarded | 7 | 7 | 1.0 | 0.2857 | 0.4286 | -18.328 |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_guarded | 6 | 6 | 1.0 | 0.3333 | 0.3333 | -26.2874 |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | unstable | 7 | 5 | 0.7143 | 0.0 | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | under_sampled | 2 | 2 | 1.0 | 0.5 | 0.5 | -26.3822 |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | under_sampled | 2 | 2 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | unstable | 3 | 1 | 0.3333 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | 0.0 | -2.2208 |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | 1.0 | 57.0037 |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | under_sampled | 1 | 1 | 1.0 | 1.0 | 0.0 | -73.1507 |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-031-rolling-volume-profile-context-pmo-crossover-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-047-rolling-volume-profile-context-rsi-crossback-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | under_sampled | 2 | 0 | 0.0 | 0.0 | None | None |
| drv-0007-r001-stochf-trend-cmo-mean-reversion-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | 0.0 | 0.0 |
| l3-003-bbands-position-mean-reversion-channel-reentry-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-006-adx-pmo-crossover-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
