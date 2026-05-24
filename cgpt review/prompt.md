# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This packet now covers the family-aware Play Hand policy implementation requested after the clean-100 confirmation batch.

## Start Here

New packet:

- `cgpt review/family-policy-v1/acceptance-check.json`
- `cgpt review/family-policy-v1/playhand-outcome-priors.json`
- `cgpt review/family-policy-v1/pair-family-outcome-priors.csv`
- `cgpt review/family-policy-v1/recipe-outcome-priors.csv`
- `cgpt review/family-policy-v1/play-hand-seed-plan-excerpt.json`
- `cgpt review/family-policy-v1/playhand-outcome-priors-summary.json`
- `cgpt review/family-policy-v1/recipe-priors-summary.json`

Previous clean batch packet remains available:

- `cgpt review/playhand-prior-test-clean-100/recipe-performance-report.md`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-comparison-clean50-vs-clean100.md`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-dashboard.html`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-runs.csv`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-pairs.csv`
- `cgpt review/playhand-prior-test-clean-100/recipe-performance-recipes.csv`

## What Changed

I added a Play Hand outcome-prior layer that backprops clean-50 and clean-100 report outcomes into recipe priors and Play Hand seed-plan behavior.

New CLI:

```powershell
uv run build-playhand-outcome-priors --json
```

It reads report artifacts from `cgpt review/playhand-prior-test-clean-*` and emits:

```text
runs/derived/playhand-outcome-priors/playhand-outcome-priors.json
runs/derived/playhand-outcome-priors/pair-family-outcome-priors.csv
runs/derived/playhand-outcome-priors/recipe-outcome-priors.csv
runs/derived/playhand-outcome-priors/playhand-outcome-priors-summary.json
```

Then `uv run build-recipe-priors --json` consumes those outcome priors automatically and injects family policies into `runs/derived/recipe-priors/play-hand-seed-plan.json`.

## Implemented Policy

Pair/template families are classified as:

```text
under_sampled: count < 3
template_locked: exact_rescue_rate >= 0.40
mutation_friendly: mutated_win_rate >= 0.60 and avg_mutation_delta > 3
template_guarded: exact_selected_rate >= 0.40
unstable: otherwise
```

Current combined clean-50 + clean-100 outcome summary:

```text
pair_family_rows: 33
recipe_rows: 10
template_locked_pair_families: 2
template_guarded_pair_families: 2
mutation_friendly_pair_families: 0
```

The important acceptance row landed:

```text
drs-0002-r006-rsi-crossback-willr-mean-reversi-m5
  family_policy: template_locked
  count: 18
  promoted: 18
  exact_rescue_rate: 0.5556
  exact_selected_rate: 0.8333
  avg_mutation_delta: -44.3961
  recommended_max_indicators: 2
  role_balanced_fill_limit: 0
  family_cap_share: 0.15
```

The rebuilt seed plan preserves global sampling at:

```text
guided_prior_fraction: 0.70
uncertain_prior_fraction: 0.20
wild_exploration_fraction: 0.10
```

## Play Hand Behavior Change

When a selected seed-plan pair carries `playhand_family_policy`:

- `template_locked` caps the guided deal to the retained pair and blocks role-balanced fill by default.
- `template_guarded` still preserves the exact template branch and allows limited expansion.
- `mutation_friendly` would allow more expansion, but no family currently qualifies under the combined clean-50 + clean-100 rule.
- Policy exploration still uses the backend seed prompt pool, not seed-plan candidates.

This is intentionally family-aware rather than a global move from `70/20/10` to `80/15/5`.

## Files Changed

Core implementation:

- `autoresearch/playhand_outcome_priors.py`
- `autoresearch/recipe_priors.py`
- `autoresearch/play_hand.py`
- `autoresearch/__main__.py`
- `pyproject.toml`

Tests:

- `tests/test_playhand_outcome_priors.py`
- `tests/test_recipe_priors.py`
- `tests/test_play_hand.py`
- `tests/test_provider_trace.py`

## Verification

Commands run:

```powershell
uv run python -m py_compile autoresearch\playhand_outcome_priors.py autoresearch\recipe_priors.py autoresearch\play_hand.py autoresearch\__main__.py
uv run pytest tests/test_playhand_outcome_priors.py tests/test_recipe_priors.py tests/test_play_hand.py tests/test_provider_trace.py -q
uv run build-playhand-outcome-priors --json
uv run build-recipe-priors --json
```

Test result:

```text
91 passed
```

Seed-plan acceptance check:

```text
drs-0002 family_policy: template_locked
recommended_max_indicators: 2
role_balanced_fill_limit: 0
global sampling: 70/20/10
```

## Questions For Pro

1. Does this satisfy the requested family-aware policy branch before `clean-50-family-policy-v1`?
2. Are the classification thresholds right for v1, especially `template_locked = exact_rescue_rate >= 0.40` and `mutation_friendly = mutated_win_rate >= 0.60 plus avg_delta > 3`?
3. Should `drs-0008 MFI_TREND + OBV_MEAN_REVERSION` remain `unstable` under combined clean-50 + clean-100 because the average mutation delta is negative, or should it be manually treated as `template_guarded` because prior discussion considered it mutation-friendly?
4. Should unstable retained families be downweighted to `0.65`, or is that too punitive while the sample is still small?
5. Are the starting caps right: `0.15` per exact pair/template family and `0.30` per discovered recipe?
6. If this looks good, should the next command be a clean `50` seed confirmation batch named `playhand-prior-test-clean-50-family-policy-v1`?
