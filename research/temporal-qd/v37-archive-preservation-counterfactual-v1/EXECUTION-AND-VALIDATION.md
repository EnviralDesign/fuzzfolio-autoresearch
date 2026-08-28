# Execution and validation

Base: verified `origin/master` at `51c2f9175f441166e7fc997109e939a9f9103b5d`, with the published phase-1 commit integrated by ordinary cherry-pick as `e09e71f`.

Research branch: `research/v37-archive-preservation-counterfactual-v1`.

The local ignored output is:

`C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\v37-archive-preservation-counterfactual-v1\control-replay-preflight-20260828`

The real-run preflight verifies the historical `3,3,0,0,0` member trajectory and records `missingIdentityRecordCount = 512`. It emits no counterfactual result.

An isolated checkout of the exact launch commit `5fa623b88c641d4d886411bf195ee3ef386d6446` was also used to rebuild G1's cumulative archive from the retained finalizer source, selected members, and candidate-panel bundles. It fails at `cumulative_direction_behavior_reconstruction` with `window realized behavior identity mismatch`, confirming that the missing identity material is not a current-branch-only interpretation.

Two independent preflight outputs were byte-identical for both `control-replay-preflight.json` and `README.md`; each output's `CHECKSUMS.sha256` was recomputed successfully.

Targeted test:

```powershell
C:\repos\fuzzfolio-autoresearch\.venv\Scripts\python.exe -m pytest `
  tests\test_temporal_qd_v37_archive_preservation_counterfactual.py -q
```

Result: `2 passed`.

No market evaluation, worker, gateway, Vast, generation, archive mutation, production artifact rewrite, support weakening, or historical evidence rewrite occurred.
