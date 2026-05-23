# Play Hand Prior Test Notes

This was originally intended to be a 50-seed controlled Play Hand batch with current recipe priors.

I stopped it after discovering the batch was not a clean test of pair/template priors: pre-fix Play Hand could only choose validated pair indicators if those indicators were already present in the backend seed prompt. That made most guided recipes fall through to role-balanced fill.

Partial batch before stop:
- Completed seeds: 37/50.
- Statuses: {'promoted': 25, 'tombstoned': 12}.
- Dealt indicator sources: {'play_hand_seed_plan': 30, 'role_balanced_policy_exploration': 7}.
- Completed runs with a concrete dealt pair: 2.
- Completed runs with a carried pair template: 0.
- Completed runs whose selected slots were only role_balanced_fill: 23.
- Pre-fix crash seeds: [7, 9].

Fixes added after finding this:
- Guided Play Hand now augments the selectable candidate pool with seed-plan indicator IDs while keeping policy exploration on the original backend seed prompt pool.
- Recipe-prior seed plans now declare `template_instrument_policy: seed_pool`.
- Guided Play Hand now applies template instruments as the instrument pool when a validated pair/template is selected and the user did not pin instruments.

Smokes:
- Unit tests cover seed-plan candidate augmentation and preservation of exploration fallback.
- Dry run `20260523T000154028924Z-playhand-v1` shows `BBANDS_POSITION_TREND + MA_SPREAD_MEAN_REVERSION`, `validated_template_applied`, and `template_instrument_pool_applied: true`.
- Forced-guided real backend smoke `20260522T235558735932Z-playhand-v1` proved the validated pair/template path can scaffold, replay, sweep, and run 36m scrutiny against the backend.

Conclusion: do not treat the partial 37-seed batch as the real efficacy read. It is useful as a diagnostic that exposed the candidate-pool limitation. The next clean measurement should rerun the controlled batch after this patch.
