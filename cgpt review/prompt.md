# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This is the follow-up patch after your review of `00f264d`, where you correctly pointed out that the "exact template" branch was copying the expanded post-template scaffold rather than the retained validation profile.

## Short Version

That concrete issue is fixed now.

The exact-template branch now:

1. reads `recommended_profile_template.profile_path`;
2. copies/registers that retained validation profile as `exact_template.json` when the file exists;
3. uses the validation basket from the template for `exact_template_36mo`;
4. falls back to the post-template scaffold only if the retained profile file is missing;
5. records `exact_template_source` and `exact_template_source_profile_path`.

The real seed 3 smoke confirms the distinction:

- expanded mutated branch profile: 4 indicators;
- exact template profile: 2 retained-template indicators;
- both branches received real final 36-month attempt IDs;
- both branches passed;
- mutated branch scored higher and became canonical.

## New Fixes In This Pass

1. **Exact-template source correction**
   - Added a template source resolver for `profile_path`, `source_profile_path`, or `recommended_profile_template_path`.
   - If the retained profile file exists, `exact_template.json` is copied from that source profile.
   - Metadata records:
     - `exact_template_source = template_profile_path`
     - `exact_template_source_profile_path = <absolute retained profile path>`
   - Fallback source is explicitly labeled `post_template_scaffold_fallback`.

2. **Canonical selection reason**
   - Metadata now records `canonical_selection_reason`.
   - Possible examples:
     - `rescued_by_exact_template`
     - `exact_template_outscored_mutated`
     - `mutated_branch_selected`
     - `no_branch_passed_mutated_best_score`

3. **Tests**
   - Added tests for template-profile source resolution.
   - Existing branch selection tests remain in place.

## Key Files To Review

- `autoresearch/play_hand.py`
- `tests/test_play_hand.py`
- `cgpt review/exact-template-branch/notes.md`
- `cgpt review/exact-template-branch/real-smoke-seed-3-summary.json`
- `cgpt review/exact-template-branch/real-smoke-seed-3-run-metadata.json`
- `cgpt review/exact-template-branch/real-smoke-seed-3-attempts.jsonl`
- `cgpt review/exact-template-branch/real-smoke-seed-3-exact-template-profile.json`
- `cgpt review/exact-template-branch/real-smoke-seed-3-mutated-final-profile.json`

## Real Smoke

Command:

```powershell
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Run:

```text
runs/20260523T023608430426Z-playhand-v1
```

Important results:

- Dealt indicators:
  - `MFI_TREND`
  - `OBV_MEAN_REVERSION`
  - `BBANDS_POSITION_TREND`
  - `MA_SPREAD_MEAN_REVERSION`
- Exact retained template source:
  - `runs/derived/discovery-recipe-scrutiny-atlas/profiles/drs-0008-r003-mfi-trend-obv-mean-reversion-m15.json`
- Exact template profile indicators:
  - `MFI_TREND`
  - `OBV_MEAN_REVERSION`
- Mutated final profile indicators:
  - `BBANDS_POSITION_TREND`
  - `MFI_TREND`
  - `OBV_MEAN_REVERSION`
  - `MA_SPREAD_MEAN_REVERSION`
- `mutated_final_36mo`:
  - attempt `20260523T023608430426Z-playhand-v1-attempt-00009`
  - score `71.1089`
  - instruments `EURUSD`, `USDJPY`
- `exact_template_36mo`:
  - attempt `20260523T023608430426Z-playhand-v1-attempt-00010`
  - score `62.6208`
  - instruments `EURUSD`, `GBPUSD`, `USDJPY`, `XAUUSD`
- Selected branch:
  - `mutated`
  - `canonical_selection_reason = mutated_branch_selected`

Interpretation: the exact retained template itself still passed 36-month scrutiny, and in this run the expanded/mutated/scouted Play Hand branch improved on it.

## Verification

Commands run:

```powershell
uv run python -m py_compile autoresearch\play_hand.py autoresearch\recipe_priors.py
uv run pytest tests\test_play_hand.py tests\test_recipe_priors.py -q
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --dry-run --json
uv run play-hand --seed 3 --coarse-mode evolutionary --sweep-budget low --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
uv run pytest -q
```

Results:

- Focused tests: `64 passed`
- Full suite: `329 passed`
- Dry run confirmed exact profile has 2 indicators while expanded scaffold has 4.
- Real smoke confirmed two real final branch attempts and canonical selected-branch metadata.

## Questions For This Review

1. Does the corrected exact-template branch now answer the attribution question cleanly?
2. Is `canonical_selection_reason` enough for the later report/dashboard, or should there be a separate top-level `rescued_by_exact_template` boolean?
3. Should the clean controlled 50-seed batch be run now under the current `70/20/10` default?
4. For that batch, should we keep `max_indicators=4`, or temporarily cap at `2-3` to reduce expanded-hand noise while measuring pair-template priors?
5. After the clean batch, is the recipe performance report/dashboard still the next best branch?

## Current Opinion

I think this removes the blocker before the clean batch. The next useful move is the controlled 50-seed run, then a recipe performance report that compares:

- pair/template materialization rate;
- exact-template score;
- mutated-branch score;
- selected final branch;
- canonical selection reason;
- final promotion/tombstone status;
- recipe and pair family.
