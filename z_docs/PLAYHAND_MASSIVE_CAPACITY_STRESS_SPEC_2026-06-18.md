# PlayHand Massive Capacity Stress Spec

Date: 2026-06-18

## Purpose

Drive PlayHand Massive toward reliable high-utilization operation across mixed local, LAN, and cloud replay worker pools.

The north star is simple: PlayHand Massive should convert available worker capacity into scored strategies with minimal idle time. A healthy system should be able to run with roughly 100 to 200 replay workers and keep them substantially saturated after a short ramp-up period, without requiring constant manual retuning of lane counts, shard sizes, or backend pressure settings.

## Current Problem Shape

Recent runs have shown poor practical utilization:

- Worker pools are available, but only a small fraction of workers stay busy.
- Utilization sometimes creeps upward, then falls back to low levels.
- Throughput can look better in backend or worker logs than in Stack Monitor, so UI parity must be verified.
- Scaling has required too much manual command tuning.
- Some fixes may need to flow through Docker Hub before Sager or Vast workers can participate.

The expected failure mode is not just "one obvious bug." The loop should identify whether the bottleneck is:

- PlayHand Massive orchestration.
- Queue pressure or shard sizing.
- Backend gateway latency or Redis contention.
- Worker contract or image drift.
- Stack Monitor/reporting mismatch.
- A deeper architectural mismatch where Fuzzfolio's normal online worker path is being stretched too far for this private autoresearch workload.

## Capacity Targets

Minimum acceptable direction:

- 100 workers can be provisioned and used meaningfully.
- 200 workers remains a plausible upper target.
- Short ramp-up is acceptable while lanes find strategies worth expanding.
- Brief dips are acceptable at wave boundaries.
- Sustained operation at 10 to 20 percent busy is not acceptable.
- Sustained operation that only occasionally spikes to 50 percent and then collapses is not acceptable.

Do not optimize for perfect 100 percent busy at all moments. Optimize for high sustained useful throughput without brittle micromanagement.

## Worker Pools

### Local Dev Stack

Use the Fuzzfolio dev stack workers as the local baseline. Keep the local worker count modest when Sager and Vast are active so the main PC remains responsive and does not distort backend or Redis pressure.

### Sager LAN

Use `$fuzzfolio-sager-replay-workers`.

Standard Sager pool:

- Pool: `sager-lan`
- Workers: `6`
- Image: `lucasmorgan/fuzzfolio-replay-worker:main`

Normal refresh command:

```powershell
ssh sager 'powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\envir\fuzzfolio-replay-workers\restart.ps1 -Workers 6 -Image lucasmorgan/fuzzfolio-replay-worker:main'
```

### Vast Burst

Use `$fuzzfolio-vast-replay-workers`.

Standard Vast setup:

- Template: `Fuzzfolio Replay Workers (vast-burst)`
- Image: `lucasmorgan/fuzzfolio-replay-worker`
- Runtime tag: `vast`
- Container size: `50 GB`
- CPU minimum: `32`
- Sort: `Price (inc.)`

Typical test capacity varies:

- Cheap single 32-core instance for smoke and low-cost validation.
- Multiple cheap instances for moderate stress.
- Larger Threadripper-style nodes for brief higher-capacity tests, often around 128 workers.

Vast instances usually start on their own. After 30 to 60 seconds they should appear in Vast's Instances page and then in Fuzzfolio Stack Monitor.

## Level-Set Procedure

Use this before a clean stress test or after a bug has been found.

1. Stop PlayHand Massive in the AutoResearch fleet process manager.
2. Stop or destroy external worker pools early:
   - Stop/restart Sager as needed.
   - Destroy Vast instances directly. Stopping first is not required.
3. Run Redis stop sweeps in the relevant process managers.
4. In the AutoResearch fleet, run `cleanup and complete play hand runs`.
5. Wait for Redis stop sweeps and cleanup to finish before starting the next run.
6. If code changed and workers need a new image, wait for CI/Docker Hub build completion before restarting Sager or Vast.

The intent is to clear stale leases, stale workers, incomplete runs, and outdated worker images before interpreting a new run.

## Code Change And Rebuild Loop

When a bug or bottleneck is found:

1. Stop PlayHand Massive.
2. Stop Sager and destroy Vast workers if they are no longer useful for the current test.
3. Start Redis stop sweeps and incomplete-run cleanup.
4. Make the code change.
5. Run targeted local tests.
6. If the change affects worker behavior, signatures, queue contracts, gateway behavior, or runtime images:
   - Commit and push the relevant repo when ready.
   - Monitor GitHub Actions until Docker images are built and pushed.
   - Re-pull/restart Sager.
   - Recreate Vast workers from the updated template/image path.
7. Restart PlayHand Massive only after the process managers are level-set and the intended worker pools are current.

Do not keep cloud workers idling while waiting on code changes or Docker builds unless the user explicitly wants that.

## PlayHand Massive Command Management

Prefer not to micromanage the command during normal stress tests. The long-term desired behavior is dynamic scaling of active lanes, shard size, and pressure based on worker availability and observed throughput.

If the command must change:

1. Edit the AutoResearch fleet `processes.json`.
2. Stop and restart the AutoResearch fleet process manager executable.
3. Start PlayHand Massive via the process manager REST API or UI.

Do not rely on process-manager reload behavior as the primary path until it has been proven robust.

## Observation Schedule

For a clean run:

1. Start worker pools.
2. Level-set process managers.
3. Start PlayHand Massive.
4. Observe immediately after startup.
5. Observe again at about:
   - 1 to 2 minutes.
   - 3 to 4 minutes.
   - 5 minutes.
   - 10 minutes.
   - 20 minutes.
6. Extend the observation interval if the system is healthy and needs longer-run evidence.
7. Cut the run short if a clear bug, stale condition, contract drift, reporting mismatch, or utilization collapse appears.

Use terminal sleeps for deliberate observation windows when the user asks for active babysitting. Do not leave sleeps running if the user wants to pause and discuss.

## What To Inspect

Inspect multiple views because any one view can lie or lag.

### AutoResearch Fleet

Use `$fuzzfolio-autoresearch-procman`.

Check:

- PlayHand Massive logs.
- Lane creation and completion cadence.
- Whether logs are stuck repeating a state such as `campaign lane_window`.
- Whether lanes are expanding, screening, sweeping, and finishing.
- Whether process restarts or crashes occurred.

### Fuzzfolio Dev Stack

Use `$fuzzfolio-dev-procman`.

Check:

- Backend logs.
- Worker gateway warnings.
- Blocking IO warnings.
- Redis stop sweep completion.
- Replay worker process health.

### Worker Pools

Check:

- Sager worker status/logs directly.
- Vast Instances page.
- Docker image tags and contract hashes.
- Worker registration, claims, heartbeats, completions.

### Stack Monitor And Frontend Parity

Use the Fuzzfolio Stack Monitor as a human-facing truth surface, but verify parity against backend or gateway data when behavior looks suspicious.

Confirm:

- Total workers by pool.
- Busy/idle/stale/dead counts.
- Queue length and pressure.
- Contract hash status.
- Whether Stack Monitor agrees with direct worker logs and backend snapshot behavior.

If Stack Monitor reports low busy count but workers are clearly active, treat that as a reporting bug or parity gap and fix it.

## Decision Rules

### Continue Monitoring

Continue the run when:

- Worker utilization is ramping upward.
- Lanes are producing new work.
- Busy counts fluctuate but throughput is real.
- Backend warnings are present but not causing claim or completion failures.
- Stack Monitor and direct logs broadly agree.

### Cut The Run Short

Stop and investigate when:

- Massive repeats the same state with idle workers.
- Busy workers remain far below available capacity after the ramp period.
- Gateway errors or blocking IO appear to cause claim failures.
- Worker contract hashes drift unexpectedly.
- External workers register but never claim work.
- Stack Monitor diverges materially from direct worker/backend evidence.
- PlayHand Massive requires manual parameter tuning that should be dynamic.

### Change Native Fuzzfolio Carefully

Fix bugs and remove accidental bottlenecks in Fuzzfolio's normal worker path when that clearly benefits both public product behavior and private autoresearch.

Avoid layering increasingly complex special cases into Fuzzfolio's normal online path solely to support PlayHand Massive. If the private autoresearch workload keeps forcing product-path complexity, treat that as a signal to pivot.

## In-Memory Off-Ramp

Do not start with the fully in-memory path by default. First identify and fix straightforward bugs, pressure issues, reporting gaps, and dynamic scaling problems in the current architecture.

Pivot toward a more dedicated in-memory or private autoresearch worker path if:

- Redis/backend gateway pressure remains the dominant bottleneck after obvious fixes.
- Maintaining high worker saturation requires repeatedly adding special logic to Fuzzfolio's public worker path.
- The orchestration needs are clearly private to PlayHand Massive and not useful to normal Fuzzfolio users.
- The cleanest design is to separate ephemeral private compute scheduling from durable user-facing Fuzzfolio compute paths.

The in-memory path does not need resumability at first. It may be acceptable for nightly, ephemeral PlayHand Massive runs where failed in-flight work can be discarded.

## End Conditions For A Successful Phase

A phase is successful when:

- PlayHand Massive runs from the AutoResearch fleet process manager.
- Local, Sager, and Vast worker pools can be brought up and torn down without manual browser or Docker Desktop work beyond authenticated Vast access.
- A clean run demonstrates materially higher sustained worker utilization than prior 10 to 20 percent behavior.
- The system scales dynamically enough that the user does not need to hand-tune every run for current cloud budget.
- Stack Monitor reflects the real worker/pool state closely enough to be trusted for operational decisions.
- Any worker-affecting changes have been pushed, built into Docker images, and rolled forward to Sager/Vast before stress conclusions are drawn.

## Non-Goals

- Do not optimize for public end-user unlimited compute. Public Fuzzfolio users should remain constrained by normal product limits.
- Do not overbuild resumability for ephemeral private PlayHand Massive work unless it becomes clearly necessary.
- Do not keep expensive Vast capacity running during code changes unless explicitly requested.
- Do not make Fuzzfolio's native worker architecture harder to reason about just to prop up a private stress workload.
