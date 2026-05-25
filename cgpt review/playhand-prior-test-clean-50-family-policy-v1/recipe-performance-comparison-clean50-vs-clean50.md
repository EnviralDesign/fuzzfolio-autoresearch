# Recipe Performance Comparison: Clean 50 vs Clean 50

Generated: `2026-05-25T11:35:14.768418+00:00`

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
| `promotion_rate` | 0.72 | 0.54 | -0.18 |
| `template_materialization_rate` | 0.48 | 0.2 | -0.28 |
| `exact_rescue_rate` | 0.2917 | 0.4 | 0.1083 |
| `mutation_improvement_rate` | 0.3333 | 0.3 | -0.0333 |
| `policy_exploration_hit_rate` | 0.3333 | 0.5294 | 0.1961 |
| `discovered_recipe_hit_rate` | 1.0 | 1.0 | 0.0 |
| `curated_recipe_hit_rate` | 0.5714 | 0.3478 | -0.2236 |
| `top_family_concentration_share` | 0.12 | 0.08 | -0.04 |
| `unique_promoted_pair_families` | 13 | 13 | 0.0 |
| `median_final_score` | 63.013 | 43.9738 | -19.0392 |
| `average_final_score` | 46.4158 | 33.0925 | -13.3233 |
| `best_final_score` | 75.1727 | 75.8434 | 0.6707 |

## Family Classification Changes

| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_guarded | template_locked | 6 | 4 | 1.0 | 0.5 | 0.25 | -29.5526 |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | under_sampled | unstable | 1 | 3 | 0.3333 | 0.0 | None | None |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | unstable | under_sampled | 3 | 2 | 0.5 | 0.0 | None | None |
| drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5 | template_locked | under_sampled | 4 | 1 | 1.0 | 1.0 | 0.0 | -68.2227 |
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | mutation_friendly | under_sampled | 6 | 1 | 1.0 | 0.0 | 1.0 | 13.2146 |
| l3-011-toby-crabel-narrow-range-ttf-dsl-transition-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-023-bollinger-keltner-squeeze-filter-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-036-rolling-volume-profile-context-ttf-dsl-transition-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-041-rolling-volume-profile-context-channel-reentry-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | None | under_sampled | 0 | 2 | 0.0 | 0.0 | None | None |
| l3-032-rolling-volume-profile-context-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 2 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-013-kalman-velocity-confirm-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-019-ma-slope-trend-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-029-rolling-volume-profile-context-channel-reentry-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-031-rolling-volume-profile-context-pmo-crossover-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-044-rolling-volume-profile-context-ma-crossover-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | template_guarded | None | 3 | 0 | None | None | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-022-adx-ttf-dsl-transition-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-027-toby-crabel-narrow-range-candle-direction-index-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-030-bbands-position-mean-reversion-rsi-crossback-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-035-rsi-mean-reversion-toby-crabel-narrow-range-m5 | under_sampled | None | 2 | 0 | None | None | None | None |

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
