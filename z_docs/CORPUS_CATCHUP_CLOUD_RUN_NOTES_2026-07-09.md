# Corpus Catchup Cloud Run Notes - 2026-07-09

Purpose: running notes for the long `finalize-corpus` catchup with lab-gateway workers, LAN workers, and one short Vast acceleration attempt. These are operational notes for later review, not a polished postmortem.

## Current Run

- AutoResearch `Corpus Catchup` is running through procman with `finalize-corpus`.
- `Lab Gateway` is running on `http://127.0.0.1:8799`.
- Vast instance `44281357` was rented for the run and destroyed early after lake pressure showed up.
- Current gateway side is drained: no queued tasks, no live tasks, no result backlog.
- Sager LAN workers completed real full-backtest work after the lake mutation lock cleared.
- Current remaining work is local corpus/profile-drop finalization, not cloud worker execution.

## Pressure And Problems Observed

1. Data lake pressure under cloud fanout.

   The single Vast worker burst drove the lake/status path hard enough that 181 lab-gateway tasks final-failed with 524 responses from `https://fuzzfoliodatalake.enviral-design.com/api/lake/status`. These were not strategy failures. They were infrastructure/lake availability failures.

   Immediate implication: the current procman command with `--full-backtest-workers 128` is too aggressive for this pathway unless the lake status/check/download behavior is coalesced, cached, or globally backpressured.

2. Lake mutation lock caused a long idle tail.

   The data lake reported `mutation_active=true` for nightly maintenance. Workers correctly treated this as retryable and preserved attempt budget, but the user-visible behavior was a long period where tasks looked stuck and workers repeatedly retried protected work.

   Better behavior would be for the coordinator/gateway layer to detect the lake mutation state centrally and pause full-backtest dispatch until the lake is available again.

3. Mac LAN workers were online but incompatible.

   Gateway saw 8 `mac-lan` workers online, but they did not advertise `full_backtest_cache`. They could not help this run and added incompatible-claim noise.

   Next action: refresh/fix the Mac worker image/capabilities before using it for corpus catchup work, or keep it out of this pool.

4. Sager LAN workers were useful after the lake cleared.

   Six `sager-lan` workers advertised `full_backtest_cache` and drained the remaining 22 live full-backtest tasks successfully after the lake mutation lock cleared.

5. Local finalization memory pressure is high.

   The local `finalize-corpus` child process peaked around 58 GB working set / 83 GB private bytes during the post-worker phase. System free memory dipped low enough that the machine was under real pressure.

   This is independent of Vast. Cloud workers help replay computation, but the final local corpus/profile-drop phase still needs streaming/index-memory hardening.

6. Profile-drop stage has repeated non-fatal failures.

   The log shows many `profiles create` and some `package` command failures through `fuzzfolio-agent-cli`. The process continues, but the procman log truncates the actual command/error text, making root cause hard to classify from the tail alone.

   Next action: after the run exits, collect full failure examples and determine whether these are expected rejects, dev API issues, malformed strategy exports, or a bug in profile/package generation.

7. Profile-drop packaging unexpectedly depends on the dev backend/replay path.

   During the apparent pause at roughly 65%, eight child `fuzzfolio-agent-cli package` processes were alive under `finalize-corpus`. They were calling the local FuzzFolio dev API and the dev-stack replay workers were active.

   Backend logs showed `Deep replay cell detail is pending...` messages, so this phase can wait on normal dev replay infrastructure even though the expensive full-backtest phase has already drained through the lab gateway. This is an important architecture/DX surprise.

   The package subprocesses rotated over time, so the process was not fully dead, but the parent emitted no new profile-drop progress lines during the long package batch. This made an active phase look stalled.

   The backend `profile-drop/sensitivity-basket` route processes basket instruments sequentially inside one request. A single package subprocess can therefore spend a long time on a multi-instrument 36-month bundle before the parent process gets one success/failure result.

8. Lab-inline scoring profile IDs produce noisy Appwrite errors.

   Backend logs showed attempts to fetch profile ids like `lab-inline:...:lane_...:focused` through Appwrite row lookup. Appwrite rejected them because row ids cannot contain those characters and must be at most 255 chars. The backend then returned `Profile not found`.

   This appears to be error-path noise or fallback behavior during profile-drop packaging, but it obscures real failures and should be cleaned up.

   Likely fix: `_cloud_profile_exists()` in AutoResearch should locally treat `lab-inline:` refs, oversized refs, or refs with invalid Appwrite row-id characters as non-cloud profile refs and return `False` without calling the backend.

9. Profile package failures include deep replay completion races.

   Backend logs showed `Deep replay job has not completed successfully` during active profile-drop packaging. The package command may be seeing a replay job before selected-cell detail is ready, then failing rather than polling/backing off robustly enough.

   This is separate from the `lab-inline` Appwrite noise. It should be audited after the run using full failed command output and backend replay job logs.

   Current AutoResearch retry logic does not classify this message as retryable. It only retries obvious transport/timeouts. That likely turns transient deep-replay readiness races into permanent profile-drop failures.

10. Current catchup command cannot finish cleanly once any profile drop fails.

   `cmd_render_corpus_profile_drops` marks the whole run `partial_failure` and exits nonzero if any profile-drop row fails. The current run already has more than 100 profile-drop failures, so continuing it may still produce useful cached/rendered artifacts but will not end as a clean success.

11. Procman/log DX during long phases is rough.

   For long local phases, procman liveness is not enough. RAM is not always reported on the parent process, and the child process can hold most of the memory. Progress requires log tailing plus manual process-tree/memory checks.

   Better behavior would include explicit phase summaries, child-process memory accounting, and less-truncated failure reporting.

12. Gateway stale worker cleanup worked, but old worker noise was confusing.

   Stale Vast workers eventually dropped out of the gateway snapshot. This is good, but during active triage the stale/registered/online distinction needs to stay visually clear in monitoring.

13. The current full-backtest repair status is uncertain until final summary.

   Gateway accepted 1874 completions and 181 final failures. Once catchup finishes, inspect whether these failures left missing/stale full-backtest artifacts that require a lower-concurrency repair pass.

## Safer Follow-Up Command Candidate

If a repair pass is needed, start with much lower lake pressure:

```powershell
uv run finalize-corpus --full-backtest-workers 6 --profile-drop-workers 4 --full-backtest-backend lab-gateway --full-backtest-gateway-url http://127.0.0.1:8799 --full-backtest-result-batch-size 50 --trading-dashboard-root C:\repos\Trading-Dashboard
```

Do not run this until the current catchup exits and the final summary is inspected.
