# V37 control-replay blocker

## Observed control

The immutable V37 run root is:

`C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37`

Its recorded parent-archive member counts are exactly `3, 3, 0, 0, 0`.

| Generation | Members | Archive SHA-256 |
| --- | ---: | --- |
| G1 | 3 | `1e711b1a2edfce331fbc6008b4c4b4271eef0bee7b8b79691d6bbb5a2f8ee7` |
| G2 | 3 | `e27b687498f3635cc2a11508cc9d5089c02b2d790c870ca1a7f2c099cb8e240a` |
| G3 | 0 | `f0413265d5ffae5d794dbf7d56ada3f0ebbe86c458a5e9b04401ff9e20f43543` |
| G4 | 0 | `30b2c48a0bb3f8b8826a644e794f8f52bf2ecfdf1805d306e06057fb4b47d22a` |
| G5 | 0 | `c0329f1427ab04226704fbd74ff6bf97b87bc7d335864a5e7420dcfc6a9704be` |

## Why replay cannot proceed

V37's launch identity records source commit `5fa623b88c641d4d886411bf195ee3ef386d6446`, but labels the launch worktree `dirty-uncommitted-g4-zero-offspring-receipt-validation-and-fast-ephemeral-playbook-notes`.

The retained G1 finalizer source contains 128 candidate-panel bundles and 512 panel-window behavior records. Every one has the legacy `temporal_realized_behavior_v1` payload but lacks both `identityMaterial` and `identitySha256`. Those values are required to authenticate cumulative direction behavior before the recorder can construct the equal-coverage archive. The retained source does not contain the dirty finalizer implementation that accepted those legacy rows.

Reconstructing a permissive compatibility path would be a new archive policy, not an exact replay. Reusing the saved cumulative archive would merely copy the historical result rather than reproduce its transition. Both are outside this task.

## Consequence

The archive-preservation counterfactual is not authorized. In particular, this phase does **not** produce a candidate-disposition ledger, classify a preservation defect, run variants 1–5, or recommend an archive repair. It also does not launch market work, workers, gateway/Vast capacity, or another generation.

The smallest next action, if the original dirty finalizer source or a complete source snapshot can be recovered, is to rerun the preflight unchanged. Otherwise the decision tree outcome is F: retained evidence cannot distinguish archive mechanics from variation failure without inventing historical authority.
