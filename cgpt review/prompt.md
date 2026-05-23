# CGPT Review Packet

Please review the latest Fuzzfolio AutoResearch state as a technical/design reviewer. This packet follows the exact-template branch fix and the clean controlled 50-seed Play Hand batch that you recommended.

## What Changed Since Your Last Review

The exact-template branch fix was already landed and verified:

- Play Hand now copies the retained validation profile from `recommended_profile_template.profile_path`.
- The exact branch uses the retained template profile and validation basket.
- The mutated branch can still expand/scout/sweep normally.
- Final scrutiny compares both branches and records `selected_final_branch` plus `canonical_selection_reason`.

The clean 50-seed batch has now run to completion.

Command template:

```powershell
uv run play-hand --seed <seed> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Batch directory:

```text
runs/derived/playhand-prior-test-clean-50
```

Review packet directory:

```text
cgpt review/playhand-prior-test-clean-50
```

## New Report Artifacts

Please start with:

- `cgpt review/playhand-prior-test-clean-50/recipe-performance-report.md`
- `cgpt review/playhand-prior-test-clean-50/recipe-performance-dashboard.html`
- `cgpt review/playhand-prior-test-clean-50/recipe-performance-report.json`
- `cgpt review/playhand-prior-test-clean-50/recipe-performance-runs.csv`
- `cgpt review/playhand-prior-test-clean-50/recipe-performance-recipes.csv`
- `cgpt review/playhand-prior-test-clean-50/recipe-performance-pairs.csv`

Source batch artifacts copied for audit:

- `cgpt review/playhand-prior-test-clean-50/batch-status.json`
- `cgpt review/playhand-prior-test-clean-50/batch-run.log`
- `cgpt review/playhand-prior-test-clean-50/run-clean-50.ps1`

Reusable report builder:

- `scripts/build_playhand_prior_batch_report.py`

## Headline Results

The clean batch completed without execution failures:

- 50/50 runs completed.
- 36 promoted.
- 14 tombstoned.
- 0 failed.
- 72% promotion rate.
- Median final scrutiny score: 63.013.
- Best final scrutiny score: 75.1727.

Guided prior materialization appears to be working:

- 38 runs used `play_hand_seed_plan`.
- 12 runs used `role_balanced_policy_exploration`.
- 24 runs materialized exact retained-template branches.
- 0 rows had a retained template that failed to materialize.

Exact-template branch attribution is now useful:

- 34 selected the mutated branch.
- 16 selected the exact-template branch.
- 7 were `rescued_by_exact_template`.
- 9 were `exact_template_outscored_mutated`.
- 8 were `mutated_branch_selected` while an exact template was also present.

Case counts:

```text
template_materialized_exact_passed_mutated_passed: 17
template_materialized_exact_passed_mutated_failed: 7
no_template_curated_recipe: 14
policy_exploration: 12
```

The strongest recipe groups in this batch:

- `discovered_recipe_003`: 10/10 promoted, best 74.447, average positive 67.6769.
- `discovered_recipe_006`: 9/9 promoted, best 74.7581, average positive 67.7536.
- `mean_reversion_reclaim`: 5/8 promoted, best 75.1727.
- `discovered_recipe_001`: 3/3 promoted, exact-template selected all three.

The strongest pair/template families:

- `drs-0003-r006-willr-mean-reversi-rsi-crossback-m5`: 6/6 promoted, 5 exact-template selections, 2 rescues.
- `drs-0008-r003-mfi-trend-obv-mean-reversion-m15`: 6/6 promoted, best 74.447.
- `drs-0001-r003-bbands-position-tr-ma-spread-mean-rev-m5`: 4/4 promoted, 4 exact-template rescues.
- `drs-0002-r006-rsi-crossback-willr-mean-reversi-m5`: 3/3 promoted, best 72.772.
- `drs-0004-r001-thrust-bar-signal-channel-reentry-m5`: 3/3 promoted, 3 exact-template selections, 1 rescue.

## My Current Read

This looks like the first real evidence that the learning loop is doing something useful:

- The seed plan is no longer just labeling runs; it is materializing retained pair/template evidence.
- Discovered recipes are not decorative. They produced the cleanest recipe-level results in this batch.
- Exact-template controls were necessary. Seven promoted runs would have been lost if only the mutated branch had been evaluated.
- Mutations are also useful. Several runs improved materially over a passing exact template.

The obvious risk is diversity. A meaningful share of wins came from a small set of discovered recipe/pair families. That may be acceptable for a first prior-guided batch, but the next decision should be whether to:

- reward these families harder;
- protect exact-template branches more explicitly;
- increase exploration to avoid overconcentration;
- broaden discovery only after the report tells us where the current priors are too narrow.

## Questions For This Review

1. Does this clean batch validate that recipe priors and exact-template carry-forward are helping Play Hand, or is the 50-seed sample still too small/biased to draw that conclusion?
2. Should `rescued_by_exact_template` feed back into stronger template-preservation behavior, for example less mutation pressure or a guaranteed exact branch for those pair families?
3. Should `mutated_branch_selected` over a passing exact template increase mutation/exploitation weight for that family?
4. Is the current `70/20/10` maturity policy still right after this batch, or should it remain conservative until a larger batch proves diversity?
5. Do the discovered recipes deserve first-class status now, or should they remain capped so curated recipes and policy exploration stay alive?
6. Which next branch is highest leverage:
   - tune sampling weights from this batch;
   - add cluster soft penalties;
   - add template-preservation policy by pair family;
   - run a larger 100-200 seed prior-guided batch;
   - broaden discovery to more instruments/timeframes?
7. Is the report/dashboard shape capturing the right fields for future automated comparison, or should the next iteration add additional metrics before we keep running larger batches?

## Verification

Commands run:

```powershell
uv run python scripts\build_playhand_prior_batch_report.py --batch-dir runs\derived\playhand-prior-test-clean-50 --review-dir "cgpt review\playhand-prior-test-clean-50"
```

The report builder produced:

- `recipe-performance-report.json`
- `recipe-performance-runs.csv`
- `recipe-performance-recipes.csv`
- `recipe-performance-pairs.csv`
- `recipe-performance-report.md`
- `recipe-performance-dashboard.html`

The batch itself completed at:

```text
2026-05-23T15:27:27.8031908Z
```
