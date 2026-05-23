# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This pass implements the narrow branch you recommended after reviewing commit `4c218bd`: preserve the exact retained validation template as a final comparison branch, lower default prior eagerness to `70/20/10` while 36-month evidence is still limited, and harden seed-plan metadata lookup.

## Short Version

The prior pass fixed the materialization bug: guided Play Hand can now select seed-plan indicators instead of being gated by the backend seed prompt.

This pass fixes the next ambiguity:

> If a retained template materializes but the final Play Hand candidate fails, did the prior fail, or did Play Hand mutate/scout away from the retained template?

Play Hand now runs final 36-month scrutiny for both:

1. the normal mutated/swept branch;
2. the exact retained template branch, using the template validation basket when available.

It selects the better passing branch, or the better-scoring branch if neither passes, and records branch-level metadata.

## New Fixes In This Pass

1. **Exact-template final scrutiny branch**
   - When a selected pair has `recommended_profile_template`, Play Hand copies/registers an `exact_template.json` profile immediately after template defaults are applied.
   - Normal Play Hand continues through baseline, timing sweep, coarse sweep, focused sweep, and instrument scout as before.
   - Final scrutiny now evaluates both `mutated_final_36mo` and `exact_template_36mo`.
   - The exact branch uses `template_branch_instruments` from the retained template if available.

2. **Branch-aware final selection and metadata**
   - New metadata fields include:
     - `mutated_attempt_id`
     - `mutated_score`
     - `exact_template_attempt_id`
     - `exact_template_score`
     - `selected_final_branch`
     - `selected_final_phase`
     - `final_branch_scores`
     - `template_branch_instruments`
     - `template_branch_source_probe_id`
   - The selected branch becomes the canonical final attempt when it passes.

3. **Tiered 36-month maturity policy**
   - The seed-plan policy is no longer binary.
   - Current rebuilt policy is:

   ```json
   {
     "guided_prior_fraction": 0.7,
     "uncertain_prior_fraction": 0.2,
     "wild_exploration_fraction": 0.1,
     "maturity": "limited_36m_retention",
     "retained_36m_family_count": 3,
     "template_instrument_policy": "seed_pool"
   }
   ```

   - `0` retained 36m families: `60/25/15`
   - `1-3` distinct retained 36m families: `70/20/10`
   - `4+` distinct retained 36m families: `80/15/5`
   - Distinct families use unordered indicator pairs so opposite orderings do not inflate maturity.

4. **Seed-plan metadata hardening**
   - `_seed_plan_indicator_metadata()` now reads from `config.derived_root / "indicator-atlas"` instead of assuming `runs_root / "derived"`.

## Key Files To Review

- `autoresearch/play_hand.py`
- `autoresearch/recipe_priors.py`
- `tests/test_play_hand.py`
- `tests/test_recipe_priors.py`
- `cgpt review/exact-template-branch/dry-run-seed-3-summary.json`
- `cgpt review/exact-template-branch/dry-run-seed-3-run-metadata.json`
- `cgpt review/recipe-priors/play-hand-seed-plan.json`
- `cgpt review/recipe-priors/recipe-priors-summary.json`

## Smoke Result

New packet folder:

```text
cgpt review/exact-template-branch/
```

Dry-run command:

```powershell
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
```

Important result:

- Dealt from `play_hand_seed_plan`.
- Dealt indicators:
  - `MFI_TREND`
  - `OBV_MEAN_REVERSION`
  - `BBANDS_POSITION_TREND`
  - `MA_SPREAD_MEAN_REVERSION`
- Applied validated template defaults.
- Registered `exact_template`.
- Final branch scores include both:
  - `mutated_final_36mo`
  - `exact_template_36mo`
- Exact-template branch used:
  - `template_branch_source_probe_id`: `drs-0008-r003-mfi-trend-obv-mean-reversion-m15`
  - `template_branch_instruments`: `EURUSD, GBPUSD, USDJPY, XAUUSD`

Because this was a dry run, both final scores are null and the mutated branch wins the tie by design. Unit tests cover the non-null branch selection behavior.

## Verification

Commands run:

```powershell
uv run python -m py_compile autoresearch\play_hand.py autoresearch\recipe_priors.py
uv run pytest tests\test_play_hand.py tests\test_recipe_priors.py -q
uv run build-recipe-priors
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
uv run pytest -q
```

Results:

- Focused tests: `62 passed`
- Full suite: `327 passed`
- Rebuilt seed-plan maturity: `limited_36m_retention`, `70/20/10`, `retained_36m_family_count = 3`

## Questions For This Review

1. Is the exact-template branch implemented at the right point in the flow: after template defaults, before baseline/sweeps, then final comparison against the mutated branch?
2. Should the exact-template branch always use the template validation basket when available, even if the normal branch scouts down to one instrument?
3. Should exact-template branch selection be allowed to become canonical exactly as implemented, or should it be recorded as a separate "rescued by template" status?
4. Is `70/20/10` the right default now that the rebuilt seed plan has only three distinct retained 36-month families?
5. Is the next best action now the clean controlled 50-seed Play Hand batch, or should we do one real low-budget backend smoke of the exact-template final branch first?
6. After the clean batch, is the recipe performance report/dashboard still the next best branch?

## Current Opinion

This pass removes the largest remaining attribution problem before the clean rerun. A failed run can now say whether the exact retained template itself failed, whether the mutated branch failed, or whether mutation improved the retained template.

My suggested next step is a clean controlled 50-seed batch under the current `70/20/10` default, then a recipe performance report that compares materialized pairs, exact-template outcomes, mutated outcomes, selected branches, and final promotions/tombstones.
