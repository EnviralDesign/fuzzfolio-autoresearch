# Recipe Performance Comparison: Clean 50 vs Clean 50

Generated: `2026-05-25T22:41:43.890726+00:00`

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
| `promotion_rate` | 0.72 | 0.64 | -0.08 |
| `template_materialization_rate` | 0.48 | 0.48 | 0.0 |
| `exact_rescue_rate` | 0.2917 | 0.4583 | 0.1666 |
| `mutation_improvement_rate` | 0.3333 | 0.3333 | 0.0 |
| `policy_exploration_hit_rate` | 0.3333 | 0.2667 | -0.0666 |
| `discovered_recipe_exposure` | 0.48 | 0.48 | 0.0 |
| `curated_recipe_exposure` | 0.28 | 0.22 | -0.06 |
| `policy_exploration_exposure` | 0.24 | 0.3 | 0.06 |
| `discovered_recipe_hit_rate` | 1.0 | 1.0 | 0.0 |
| `curated_recipe_hit_rate` | 0.5714 | 0.3636 | -0.2078 |
| `top_family_concentration_share` | 0.12 | 0.14 | 0.02 |
| `unique_promoted_pair_families` | 13 | 8 | -5.0 |
| `median_final_score` | 63.013 | 61.6598 | -1.3532 |
| `average_final_score` | 46.4158 | 41.3083 | -5.1075 |
| `best_final_score` | 75.1727 | 73.303 | -1.8697 |

## Exposure Deltas

| Metric | Delta |
|---|---:|
| `discovered_recipe_exposure_delta` | 0.0 |
| `curated_recipe_exposure_delta` | -0.06 |
| `policy_exploration_exposure_delta` | 0.06 |

## Family Classification Changes

| Family | Previous | Current | Prev Count | Current Count | Current Promotion Rate | Current Rescue Rate | Current Mutated Win Rate | Avg Mutation Delta |
|---|---|---|---:|---:|---:|---:|---:|---:|
| drs-0008-r003-mfi-trend-obv-mean-reversion-m15 | mutation_friendly | template_guarded | 6 | 7 | 1.0 | 0.2857 | 0.4286 | -18.2395 |
| drs-0002-r006-rsi-crossback-willr-mean-reversi-m5 | template_guarded | template_locked | 3 | 5 | 1.0 | 0.6 | 0.4 | -32.4065 |
| drs-0003-r006-willr-mean-reversi-rsi-crossback-m5 | template_guarded | template_locked | 6 | 5 | 1.0 | 0.4 | 0.4 | -28.7395 |
| l3-007-toby-crabel-narrow-range-market-mode-transition-m15 | None | under_sampled | 0 | 2 | 0.5 | 0.0 | None | None |
| l3-021-toby-crabel-narrow-range-wavetrend-crossover-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-048-rolling-volume-profile-context-key-reversal-signal-m15 | None | under_sampled | 0 | 1 | 1.0 | 0.0 | None | None |
| l3-008-ma-slope-trend-pmo-crossover-m5 | None | under_sampled | 0 | 2 | 0.0 | 0.0 | None | None |
| l3-010-adx-toby-crabel-narrow-range-m5 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-018-adx-market-mode-transition-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-020-bbands-position-mean-reversion-channel-reentry-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| l3-046-rolling-volume-profile-context-toby-crabel-narrow-range-m15 | None | under_sampled | 0 | 1 | 0.0 | 0.0 | None | None |
| drs-0004-r001-thrust-bar-signal-channel-reentry-m5 | template_guarded | None | 3 | 0 | None | None | None | None |
| drs-0005-r002-channel-reentry-thrust-bar-signal-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-014-bollinger-keltner-squeeze-filter-toby-crabel-narrow-range-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-022-adx-ttf-dsl-transition-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-024-kalman-velocity-confirm-pmo-crossover-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-025-toby-crabel-narrow-range-key-reversal-signal-m15 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-027-toby-crabel-narrow-range-candle-direction-index-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-030-bbands-position-mean-reversion-rsi-crossback-m5 | under_sampled | None | 1 | 0 | None | None | None | None |
| l3-042-bbands-position-mean-reversion-rsi-crossback-m15 | unstable | None | 3 | 0 | None | None | None | None |

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
| drs-0009-r005-obv-mean-reversion-plus-di-trend-m5 | None | 0 | 0 | None | None | None | None |
| l3-001-rsi-mean-reversion-channel-reentry-m5 | None | 0 | 0 | None | None | None | None |
| l3-002-toby-crabel-narrow-range-pmo-crossover-m5 | None | 0 | 0 | None | None | None | None |
| l3-014-bollinger-keltner-squeeze-filter-toby-crabel-narrow-range-m5 | None | 0 | 0 | None | None | None | None |
| l3-016-rsi-mean-reversion-channel-reentry-m15 | None | 0 | 0 | None | None | None | None |
