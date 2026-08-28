# V37 archive-preservation counterfactual v1

This phase is terminally blocked before counterfactual execution.

The observed historical parent-archive trajectory is exactly `3 -> 3 -> 0 -> 0 -> 0`, with the recorded member IDs, cells, lanes, and archive hashes retained in the ignored audit artifact. But the exact reducer control cannot be reconstructed from the frozen V37 authority: all 512 retained generation-1 panel-window `realizedBehavior` records omit the `identityMaterial` and `identitySha256` fields needed by cumulative direction selection.

The launch identifies commit `5fa623b88c641d4d886411bf195ee3ef386d6446` but also records a dirty source worktree. That uncommitted finalizer source was not preserved. Replaying the recorded commit fails at the same direction-behavior identity boundary.

No ledger, archive counterfactual, market evaluation, worker/gateway/Vast operation, generation, archive mutation, or policy change was performed. The study must not infer a preservation diagnosis from the historical archive count alone.

Run the fail-closed preflight with:

```powershell
uv run python -m autoresearch.temporal_qd_v37_archive_preservation_counterfactual `
  --v37-root C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37 `
  --output-dir C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\v37-archive-preservation-counterfactual-v1\control-replay-preflight
```

See [CONTROL-REPLAY-BLOCKER.md](CONTROL-REPLAY-BLOCKER.md) for the exact evidence and stopping condition.
