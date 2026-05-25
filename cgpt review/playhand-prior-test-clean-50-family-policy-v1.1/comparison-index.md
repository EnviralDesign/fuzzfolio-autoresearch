# Family Policy v1.1 Comparison Index

This packet covers the `clean-50-family-policy-v1.1` source-mix confirmation batch.

## Primary Artifacts

- `recipe-performance-report.md`
- `recipe-performance-report.json`
- `recipe-performance-runs.csv`
- `recipe-performance-pairs.csv`
- `recipe-performance-recipes.csv`
- `recipe-performance-dashboard.html`
- `batch-status.json`
- `batch-run.log`
- `run-clean-50-family-policy-v1.1.ps1`

## Explicit Comparisons

- `recipe-performance-comparison-original-clean50-vs-family-policy-v1.1.md`
- `recipe-performance-comparison-clean100-vs-family-policy-v1.1.md`
- `recipe-performance-comparison-family-policy-v1-vs-v1.1.md`

The report script still uses generic internal batch labels like `Clean 50`, so these explicit filenames are the reliable names to use.

## Headline Metrics

```text
batch: clean-50-family-policy-v1.1
seeds: 201..250
completed: 50/50
failed: 0
promoted: 32
tombstoned: 18
promotion_rate: 64%
median_final_score: 61.6598
best_final_score: 73.303
template_materialized: 24/50, 48%
exact_template_rescues: 11
mutated_improved_over_exact: 8
top_family_concentration_share: 14%
unique_promoted_pair_families: 8
```

## Source Mix

```text
expected_guided_mix: discovery_recipe_validation 60%, curated_recipe_prior 40%
observed_guided_mix: discovery_recipe_validation 68.57%, curated_recipe_prior 31.43%
all-run exposure: discovered 24/50, curated 11/50, policy exploration 15/50
bucket_fallbacks: 0
```

Hit rates:

```text
discovered_recipe_validation: 24/24 promoted, 100%
curated_recipe_prior: 4/11 promoted, 36.36%
role_balanced_policy_exploration: 4/15 promoted, 26.67%
```

## Family Read

Top discovered pair/template families stayed strong:

```text
drs-0001 BBANDS_POSITION_TREND + MA_SPREAD_MEAN_REVERSION: 7/7 promoted, template_locked
drs-0008 MFI_TREND + OBV_MEAN_REVERSION: 7/7 promoted, template_guarded
drs-0002 RSI_CROSSBACK + WILLR_MEAN_REVERSION: 5/5 promoted, template_locked
drs-0003 WILLR_MEAN_REVERSION + RSI_CROSSBACK: 5/5 promoted, template_locked
```

Curated anchors remain mixed:

```text
breakout_compression_release: 2/3 promoted
mean_reversion_reclaim: 1/2 promoted
profile_value_context: 1/2 promoted
trend_pullback_continuation: 0/4 promoted
```

## Comparison Highlights

Compared with original clean-50:

```text
promotion_rate: 72% -> 64%
template_materialization_rate: 48% -> 48%
exact_rescue_rate: 29.17% -> 45.83%
discovered_recipe_exposure: 48% -> 48%
curated_recipe_exposure: 28% -> 22%
policy_exploration_exposure: 24% -> 30%
```

Compared with clean-100:

```text
promotion_rate: 73% -> 64%
template_materialization_rate: 46% -> 48%
exact_rescue_rate: 43.48% -> 45.83%
discovered_recipe_exposure: 46% -> 48%
top_family_concentration_share: 15% -> 14%
```

Compared with family-policy-v1:

```text
promotion_rate: 54% -> 64%
template_materialization_rate: 20% -> 48%
discovered_recipe_exposure: 20% -> 48%
curated_recipe_exposure: 46% -> 22%
policy_exploration_exposure: 34% -> 30%
```

## Main Question

Family-policy-v1.1 appears to fix the source-mix regression from v1: discovered/template exposure recovered while keeping family concentration capped. It is still below the original clean-50/clean-100 promotion rate, mostly because curated and policy-exploration lanes remain weaker.

Please review whether v1.1 is clean enough to accept into outcome-prior backprop, or whether we should hold it as a validation packet and first tune curated/policy handling.
