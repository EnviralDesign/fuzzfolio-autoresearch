# PlayHand Memory Deployment Checklist

1. Confirm the target branch contains the latest ephemeral-worker commits before merging.
2. Require the PlayHand resilience workflow to pass.
3. Stop only the Phase 3 coordinator.
4. Leave the Lab Gateway, workers, and MarketDataLake running.
5. Pull `master`, run `uv sync`, and restart the Resume entry.
6. Confirm the first barrier includes `coordinator memory` with RSS/private bytes and retained-object counters.
7. Compare coordinator self-memory with Procman. If self-memory is low while Procman remains high, inspect Procman's process-tree accounting rather than changing research state.
8. Confirm `live-task-payloads` falls to zero after enqueue and `sample-bytes` remains bounded.
