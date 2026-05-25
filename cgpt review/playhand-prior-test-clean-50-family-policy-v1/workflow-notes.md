# Play Hand Workflow Notes

## Normal Overnight Play Hand

Use normal Play Hand loops when the goal is to generate more candidate profiles from the current prior system.

Recommended shape:

```powershell
uv run play-hand --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4
```

Normal runs should preserve:

- current `70/20/10` global seed-plan policy,
- backend seed-prompt exploration for policy-exploration lanes,
- exact-template branch evaluation when a retained template materializes,
- family-aware behavior from the current seed plan.

Normal overnight output is useful for corpus growth, but it should not automatically rewrite priors.

## Controlled Prior Refresh

Use controlled batches when the goal is to feed outcomes back into recipe/family priors.

Recommended shape:

```powershell
uv run play-hand --seed <seed> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json
```

Controlled refresh runs should:

- use fixed seed ranges,
- write per-seed `seed-*-summary.json` and `seed-*-status.json`,
- build a batch report with `scripts/build_playhand_prior_batch_report.py`,
- compare against prior clean batches,
- have Pro review before `build-playhand-outcome-priors` consumes the new packet.

## Current Open Question

The family-policy-v1 batch is operationally clean but has a lower promotion rate than the earlier clean-50 and clean-100 batches. I have not rebuilt cached outcome priors from this run yet. The next decision is whether this should be accepted into the priors as valid feedback or treated as a policy tuning diagnostic first.
