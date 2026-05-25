# Recipe Performance Comparison: Clean 100 vs Clean 50

Generated: `2026-05-25T11:35:15.022252+00:00`

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
| `promotion_rate` | 0.73 | 0.54 | -0.19 |
| `template_materialization_rate` | 0.46 | 0.2 | -0.26 |
| `exact_rescue_rate` | 0.4348 | 0.4 | -0.0348 |
| `mutation_improvement_rate` | 0.3043 | 0.3 | -0.0043 |
| `policy_exploration_hit_rate` | 0.5357 | 0.5294 | -0.0063 |
| `discovered_recipe_hit_rate` | 0.9783 | 1.0 | 0.0217 |
| `curated_recipe_hit_rate` | 0.5 | 0.3478 | -0.1522 |
| `top_family_concentration_share` | 0.15 | 0.08 | -0.07 |
| `unique_promoted_pair_families` | 16 | 13 | -3.0 |
| `median_final_score` | 62.6199 | 43.9738 | -18.6461 |
| `average_final_score` | 46.8045 | 33.0925 | -13.712 |
| `best_final_score` | 79.8725 | 75.8434 | -4.0291 |

## Family Classification Changes

| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_guarded | template_locked | 12 | 4 | 1.0 | 0.5 | 0.25 | -29.5526 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_locked | template_guarded | 15 | 3 | 1.0 | 0.3333 | 0.3333 | -26.7917 |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | under_sampled | unstable | 2 | 3 | 0.3333 | 0.0 | None | None |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | None | under_sampled | 0 | 2 | 0.5 | 0.0 | None | None |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_guarded | under_sampled | 6 | 1 | 1.0 | 1.0 | 0.0 | -68.2227 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | template_guarded | under_sampled | 7 | 1 | 1.0 | 0.0 | 1.0 | 13.2146 |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | 0.0 | -0.825 |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-023-bollinger-keltner-squeeze-filter-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-036-rolling-volume-profile-context-ttf-dsl-transition-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-041-rolling-volume-profile-context-channel-reentry-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | unstable | under_sampled | 3 | 2 | 0.0 | 0.0 | None | None |
| l3-032-rolling-volume-profile-context-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 2 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-014-bollinger-keltner-squeeze-filter-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-019-ma-slope-trend-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-029-rolling-volume-profile-context-channel-reentry-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-044-rolling-volume-profile-context-ma-crossover-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | under_sampled | None | 2 | 0 | None | None | None | None |
| drv-0007-r001-stochf-trend-cmo-mean-reversion-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drv-0008-r002-cmo-mean-reversion-stochf-trend-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drv-0058-r007-channel-reentry-bbands-position-tr-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-003-bbands-position-mean-reversion-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-006-adx-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | under_sampled | None | 2 | 0 | None | None | None | None |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | unstable | None | 7 | 0 | None | None | None | None |

## Top Current Families

| Family | Class | Runs | Promoted | Promotion Rate | Rescue Rate | Mutated Win Rate | Avg Mutation Delta |
|---|---|---:|---:|---:|---:|---:|---:|
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_locked | 4 | 4 | 1.0 | 0.5 | 0.25 | -29.5526 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_guarded | 3 | 3 | 1.0 | 0.3333 | 0.3333 | -26.7917 |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | unstable | 3 | 1 | 0.3333 | 0.0 | None | None |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | under_sampled | 2 | 1 | 0.5 | 0.0 | None | None |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | under_sampled | 1 | 1 | 1.0 | 1.0 | 0.0 | -68.2227 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | 1.0 | 13.2146 |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | 0.0 | -0.825 |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-011-toby-crabel-narrow-range-ttf-dsl-transition-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-023-bollinger-keltner-squeeze-filter-market-mode-transition-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-036-rolling-volume-profile-context-ttf-dsl-transition-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-041-rolling-volume-profile-context-channel-reentry-m15 | under_sampled | 1 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | under_sampled | 2 | 0 | 0.0 | 0.0 | None | None |
| l3-032-rolling-volume-profile-context-toby-crabel-narrow-range-m5 | under_sampled | 2 | 0 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-013-kalman-velocity-confirm-market-mode-transition-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-014-bollinger-keltner-squeeze-filter-toby-crabel-narrow-range-m5 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-019-ma-slope-trend-market-mode-transition-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | under_sampled | 1 | 0 | 0.0 | 0.0 | None | None |
