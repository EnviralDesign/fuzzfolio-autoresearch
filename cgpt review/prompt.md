# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This is the follow-up pass after your recommendation to stop architecture looping and run a controlled Play Hand batch against the current 36-month recipe priors.

## Short Version

I started the controlled Play Hand batch, but stopped it after it exposed a more important integration issue:

Play Hand was loading the seed plan, selecting a recipe, and forcing at least two indicators, but it could only choose indicators that were already present in the backend seed prompt. That meant a discovered recipe could be selected while the exact retained pair/template could still be unavailable. In practice, most "guided" runs fell back to `role_balanced_fill`.

So the partial batch is not a clean efficacy test. It is a useful diagnostic.

## New Fixes In This Pass

1. **Seed-plan candidate augmentation in Play Hand**
   - Guided sampling now augments the selectable pool with indicators referenced by the seed plan.
   - This includes pair-menu `anchor_id` / `trigger_id`, discovered pair IDs, slot-menu IDs, and indicator defaults inside carried templates.
   - Policy exploration still uses the original backend seed prompt pool, preserving wild/random exploration.

2. **Template instrument seed pool**
   - `build-recipe-priors` now emits `template_instrument_policy: seed_pool`.
   - When a validated pair/template is selected and the user has not pinned instruments or an instrument pool, Play Hand uses the template's validation instruments as the instrument pool.
   - This does not force the exact basket. It just gives the validated pair a fair starting pool.

3. **Review artifacts for the interrupted batch and smokes**
   - New packet folder: `cgpt review/playhand-prior-test/`.
   - It includes partial batch seed results, summary JSON, event tail, pre-fix crash logs, smoke summaries, and notes.

## Key Files To Review

- `autoresearch/play_hand.py`
- `autoresearch/recipe_priors.py`
- `tests/test_play_hand.py`
- `cgpt review/playhand-prior-test/playhand-prior-test-notes.md`
- `cgpt review/playhand-prior-test/partial-batch-summary.json`
- `cgpt review/playhand-prior-test/partial-batch-seed-results.csv`
- `cgpt review/playhand-prior-test/smoke-runs-summary.json`
- `cgpt review/recipe-priors/play-hand-seed-plan.json`

## Partial Batch Finding

The intended 50-seed controlled batch was stopped after 37 completed seeds.

Partial pre-fix counts:

- Completed: 37/50.
- Promoted: 25.
- Tombstoned: 12.
- `play_hand_seed_plan` source: 30.
- `role_balanced_policy_exploration`: 7.
- Completed runs with a concrete dealt pair: 2.
- Completed runs with a carried pair template: 0.
- Completed runs whose selected slots were only `role_balanced_fill`: 23.
- Pre-fix crash seeds: 7 and 9, from the earlier missing `CliError` import that was already fixed.

Interpretation: these results should not be treated as the real prior efficacy test. The high promotion count is interesting, but the batch mostly tested recipe labels and role-balanced fill, not true pair/template-guided sampling.

## Smoke Results After Fix

Commands/paths are summarized in `cgpt review/playhand-prior-test/smoke-runs-summary.json`.

Important smokes:

- Dry run `20260523T000154028924Z-playhand-v1`
  - Dealt `BBANDS_POSITION_TREND + MA_SPREAD_MEAN_REVERSION`.
  - Applied the retained pair template.
  - Applied `template_instrument_pool_applied: true`.
  - Instrument pool became `EURUSD, GBPUSD, USDJPY, XAUUSD`.

- Forced-guided real backend smoke `20260522T235558735932Z-playhand-v1`
  - I temporarily set local ignored `guided_prior_fraction=1.0`, ran one low-budget backend smoke, then restored the seed plan.
  - It scaffolded, replayed, swept, and ran 36-month scrutiny with the retained pair/template path.
  - It still failed final 36-month scrutiny after sweeps/instrument selection, which raises the next design question: should exact templates be preserved as one branch rather than immediately being mutated away?

## Verification

Commands run:

```powershell
uv run python -m py_compile autoresearch\play_hand.py autoresearch\recipe_priors.py autoresearch\__main__.py
uv run pytest tests\test_play_hand.py tests\test_recipe_priors.py -q
uv run pytest -q
uv run build-recipe-priors --json
uv run play-hand --seed 2 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 2 --final-profile-drop-count 0 --json
uv run play-hand --seed 2 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
uv run play-hand --seed 2 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
```

Full test suite: 323 passed.

## Questions For This Review

1. Is seed-plan candidate augmentation the right fix, or should guided Play Hand bypass the backend seed prompt entirely?
2. Is `template_instrument_policy: seed_pool` the right default, or should retained templates use an `initial_basket` mode that starts with the full validation basket?
3. Should a validated template be preserved as a fixed baseline branch during Play Hand sweeps, so the process can compare "exact retained template" versus "mutated local best"?
4. Should the next clean measurement rerun the 50-seed controlled batch now, or first add that exact-template branch?
5. Given that only a small number of 36-month retained pairs exist, should the sampling policy remain `80/15/5`, or drop to `70/20/10` for this rerun?
6. After a clean rerun, is the next best branch still a recipe performance report/dashboard?

## Current Opinion

The right next step is no longer to interpret the partial batch as a signal-quality result. The partial batch found a real integration gap. The code now actually lets pair/template priors materialize, and the seed-pool policy gives them a more faithful instrument environment.

My bias: rerun a clean controlled batch after this patch, then build the recipe performance report from that clean run. The remaining open design question is whether to add "exact template as fixed branch" before that rerun.
