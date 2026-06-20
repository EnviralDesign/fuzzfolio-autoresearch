# PlayHand Lab Gateway Design Spec

Date: 2026-06-19

Status: first-class lab path implemented and locally validated; ready for controlled cloud-worker smoke testing, with cold-start and tiny-task limits documented.

## Purpose

Build a private PlayHand research control plane that can keep hundreds of remote workers busy without routing campaign traffic through the Fuzzfolio backend, Redis streams, Appwrite, or sweep shards.

This is not a product feature. It exists to turn available compute into completed PlayHand evaluations as quickly and predictably as possible, then write normal AutoResearch run artifacts for downstream corpus and portfolio tooling.

## Decision

Create a standalone PlayHand Lab Gateway owned by `fuzzfolio-autoresearch`.

The gateway is an explicit lab-only process:

- In-memory work queue.
- In-memory worker registry.
- In-memory lease table.
- Bounded result writer queue.
- Minimal HTTP/WebSocket worker protocol.
- No Redis in the hot path.
- No Appwrite in the hot path.
- No Fuzzfolio backend in the hot path.
- No workload shards.

Fuzzfolio remains the owner of reusable compute code and the worker image. The existing Fuzzfolio worker gateway remains the durable, user-facing product path.

## Current Implementation

AutoResearch owns:

- `autoresearch/play_hand_lab_gateway.py`: in-memory gateway, HTTP control endpoints, WebSocket worker hot path, synthetic saturation simulators.
- `autoresearch/play_hand_lab.py`: PlayHand Lab coordinator, lane setup, FuzzFolio profile hydration, task enqueue, result drain, run artifact writer.
- `uv run play-hand-lab-gateway`: gateway CLI.
- `uv run play-hand-lab`: coordinator CLI.
- `uv run play-hand-lab-ws-sim`: WebSocket saturation simulator.

Trading-Dashboard owns:

- `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`: persistent WebSocket worker mode.
- `FUZZFOLIO_WORKER_TRANSPORT=lab_http`: fallback/debug worker mode.
- Replay worker contract hashing for core replay execution semantics. Lab transport client files are intentionally excluded so private lab protocol churn does not invalidate the user-facing FuzzFolio worker contract.
- Lab workers additionally advertise `playhand_lab_protocol:playhand-lab-worker-v1`, and lab tasks require it so stale images fail by not receiving work instead of failing mid-task.
- Docker entrypoint validation for lab worker environment variables.
- `uv run worker-bootstrap-command --transport lab_ws`: generated VM/Lightning worker bootstrap commands that point workers at the lab gateway while fetching bootstrap scripts from the product worker gateway.
- `uv run worker-bootstrap-command --provider vast --transport lab_ws`: generated Vast template settings for lab workers.

AutoResearch procman owns local launch entries:

- `play hand lab - gateway`
- `play hand lab - cloudflared tunnel`
- `play hand lab - coordinator fake smoke`
- `play hand lab - coordinator deep replay`

The procman gateway/coordinator entries dot-source `scripts/play-hand-lab-procman-env.ps1`. If `FUZZFOLIO_LAB_GATEWAY_TOKEN` is not already present in the procman server environment, the helper creates or reads a per-user token file at `%LOCALAPPDATA%\FuzzfolioAutoResearch\play-hand-lab-gateway-token.txt`.

## Non-Goals

- Do not expose this as a normal Fuzzfolio user workflow.
- Do not add PlayHand campaign policy to `backend/app`.
- Do not preserve resumability across coordinator crashes in v1.
- Do not optimize for perfect utilization during wave boundaries.
- Do not add shard size, active shard count, shard pressure, or shard autoscaling as tuning knobs.
- Do not use Vast as part of the validation path.

## Architecture

```text
AutoResearch PlayHand process
  -> generates lane work
  -> pushes complete evaluation tasks into Lab Gateway
  -> receives completed results
  -> writes normal run metadata, attempts, summaries, and artifacts

Lab Gateway
  -> registers workers
  -> leases one complete task at a time
  -> accepts heartbeats
  -> accepts completions/failures
  -> requeues expired leases
  -> exposes compact saturation telemetry

Remote/local lab workers
  -> run existing Fuzzfolio compute kernel
  -> claim work from Lab Gateway
  -> return a complete result payload
```

The gateway may live in `autoresearch/play_hand_lab_gateway.py` with a CLI entry such as:

```powershell
uv run play-hand-lab-gateway --host 0.0.0.0 --port 8799 --token <token>
```

Workers should use the persistent WebSocket transport mode in the existing replay-worker image:

```text
FUZZFOLIO_WORKER_TRANSPORT=lab_ws
FUZZFOLIO_LAB_GATEWAY_URL=https://...
FUZZFOLIO_LAB_GATEWAY_TOKEN=...
```

`lab_http` exists as a fallback/debug transport, not the intended high-scale hot path.

VM/Lightning bootstrap commands need two URLs:

- `--gateway-url`: the public PlayHand Lab Gateway URL workers connect to.
- `--bootstrap-url`: the normal FuzzFolio worker-gateway URL used only to fetch `bootstrap.sh` or `bootstrap.ps1`.
- `--keepalive-target-url`: generated automatically from the bootstrap URL for VM/Lightning commands so the keepalive sidecar keeps pinging the product bootstrap endpoint even when worker traffic goes to the lab gateway.

Example:

```powershell
uv run worker-bootstrap-command --transport lab_ws --pool lightning-aws --image lucasmorgan/fuzzfolio-replay-worker:main --gateway-url https://<lab-gateway> --bootstrap-url https://backend.enviral-design.com/api/worker-gateway --lake-url https://fuzzfoliodatalake.enviral-design.com/ --workers 32
```

Vast templates do not need `--bootstrap-url` because the container entrypoint starts the worker directly:

```powershell
uv run worker-bootstrap-command --provider vast --transport lab_ws --pool vast-burst --gateway-url https://<lab-gateway> --lake-url https://fuzzfoliodatalake.enviral-design.com/
```

## No-Shards Work Model

A leased work item is a complete, meaningful evaluation unit.

Examples:

- Evaluate one candidate profile snapshot over one configured instrument/timeframe/lookback basket.
- Evaluate one generated mutation candidate.
- Evaluate one sweep candidate as a complete candidate, not as part of a shard.
- Evaluate one validation basket for a promoted candidate.

The scheduler may enqueue many independent candidate evaluations, but it must not create shard objects or use shard size as a scaling control. Backpressure is controlled by:

- Number of queued complete tasks.
- Number of active leases.
- Worker claim/completion rate.
- Result writer backlog.
- Lane generator backlog.

If individual work units are too small to amortize HTTP claim/complete overhead, that is a design failure to catch early. Do not hide it by reintroducing shards.

## Worker Protocol

Control endpoints:

- `POST /tasks`
- `GET /snapshot`
- `GET /results`

Worker hot path:

- `GET /ws` upgraded to WebSocket.
- Message `register`.
- Message `claim`.
- Message `lease_heartbeat`.
- Message `complete`.
- Message `fail`.

REST-style worker claim/complete was tested first and missed the local latency/saturation gates under concurrent worker load. Treat REST worker endpoints as a fallback/debug surface only; persistent WebSocket workers are the scalable v1 path.

Protocol rules:

- Claims should reuse the worker's persistent connection.
- Workers should not poll faster than the server-provided retry interval.
- Heartbeats should be coarse, roughly 15 to 30 seconds.
- Progress updates are optional in v1 and should be disabled by default.
- Completion is idempotent by `lease_id`.
- Duplicate completion after a lease was already accepted returns the accepted result status.
- Completion after lease expiry returns `lease_lost`.
- Heartbeat after lease expiry returns lost/false and requeues or fails the task before reporting state.
- Retryable failure returns work to the queue after a capped attempt count.
- Nonretryable failure marks the task failed and lets the lane decide whether to continue.

## Task Contract

Each task needs enough data for pure compute without profile CRUD:

- `task_id`
- `lane_id`
- `attempt_id`
- `task_kind`
- `profile_snapshot`
- `instrument_set`
- `timeframe`
- `lookback_months`
- `market_data_source`
- `evaluation_options`
- `candidate_params`
- `attempt_number`
- `created_at`
- `deadline_seconds`

Deep-replay task payloads carry a full FuzzFolio `ScoringProfile` as `inline_profile_snapshot`. AutoResearch may load old/exported/stored profile artifacts, but the coordinator must convert them to worker-valid full profiles before enqueue. Invalid profile snapshots fail before gateway enqueue.

Each result returns:

- `task_id`
- `lease_id`
- `worker_id`
- `started_at`
- `completed_at`
- `duration_seconds`
- `status`
- `score_summary`
- `metrics`
- `curve_summary`
- `error` when failed

Large artifacts should be written by the coordinator, not by workers into AutoResearch paths. The worker returns structured result data; the coordinator owns run-folder layout.

## Saturation Pass Criteria

Synthetic and local validation must demonstrate:

- With a non-starved backlog, steady-state worker busy rate is at least 90 percent for 100 logical workers.
- With a non-starved backlog, steady-state worker busy rate is at least 85 percent for 500 logical workers.
- With a non-starved backlog, steady-state worker busy rate is at least 80 percent for 1000 logical workers.
- Ramp to target saturation occurs within 60 seconds after workers register, excluding the first cold-start run.
- Claim p95 latency stays below 100 ms in local loopback simulation.
- Claim p99 latency stays below 250 ms in local loopback simulation.
- Completion p95 latency stays below 250 ms for normal result payloads.
- Coordinator memory remains bounded and predictable at 1000 workers plus the configured backlog.
- Coordinator CPU does not become the dominant bottleneck before 1000 synthetic workers.
- Result writer backlog does not grow without bound during sustained completion waves.
- Gateway request rate scales with active workers and completions, not with total campaign size.
- No Redis/Appwrite/backend calls appear in the worker hot path.
- Coordinator summaries keep an accurate `recorded_result_count` plus a bounded `recorded_results` sample; lane artifacts remain the durable source of full results.

These are lab acceptance gates, not final production promises. Missing any gate means stop and revise the architecture before adding more features.

## Early Failure Gates

Fail the design early if any of these appear during validation:

- Median task runtime is below 5 seconds under realistic compute, causing claim/complete traffic to dominate.
- Lane generation cannot keep queued work above `active_workers * 2` during steady state.
- A single global lock is held during claim, completion, file writes, or result ranking.
- Result payload size makes completion upload the dominant cost.
- Artifact writing serializes completions and drains slower than workers finish tasks.
- Worker startup creates a market-data download stampede.
- Heartbeat or telemetry traffic becomes comparable to completion traffic.
- Expired lease recovery creates duplicate accepted results.
- Coordinator memory grows with completed task history instead of bounded live state.
- One slow lane can starve unrelated lanes.
- Network bandwidth, reverse proxy limits, or TLS overhead dominate before local CPU does.
- Synthetic saturation succeeds but real worker parity tests disagree with existing Fuzzfolio CLI results.
- WebSocket synthetic saturation only passes when work units are long enough to amortize claim/complete round trips. Micro-tasks around 10 to 50 ms are intentionally treated as an early warning, not a green scale signal.

Any failure gate should produce a small reproduction artifact: config, worker count, simulated runtime distribution, request stats, and the first bottleneck observed.

## Intentional V1 Limits And Watch Items

- The gateway is in-memory and ephemeral. Gateway restart is a campaign-level failure in v1; the coordinator records `gateway_restarted` or `gateway_unreachable` instead of pretending the run is resumable.
- The gateway state machine currently uses one process-local lock. This is acceptable only while loopback saturation remains healthy for realistic task durations. If the lock shows up as the first bottleneck, split claim/completion/result paths before adding features.
- The result backlog is bounded. Dropped results increment `results_dropped`, and the coordinator treats observed result loss as a failed campaign status.
- The coordinator does not retain all completed result summaries in memory. It writes full per-lane artifacts and keeps only a bounded campaign-summary sample.
- The coordinator still prepares all lanes before enqueueing the first task. This is acceptable for the first cloud validation round, but if workers sit idle during lane prep, replace the up-front list build with a streaming producer that keeps a bounded worker-scaled backlog.
- Coordinator-side result artifact/scoring/render persistence is still serial in v1. That is acceptable for initial real-compute smoke tests, but sustained cloud runs must watch result-drain time and stop if workers finish faster than the coordinator can durably record results.
- Keepalive sidecars remain product-bootstrap oriented. They are useful for VM liveness, but lab worker health should be judged by gateway `/snapshot` worker and claim metrics.
- Lab gateway request bodies default to 64 MiB and are configurable with `--max-body-mb`; large deep-replay completions should still be watched because body-size headroom is not a substitute for result-spooling design.
- Deep-replay parity and market-data cache stampede behavior still need real compute validation before spending heavily on cloud workers.

## Validation Plan

### 1. Pure State-Machine Tests

No HTTP and no compute.

Validate:

- Register worker.
- Claim task.
- Complete task.
- Fail task.
- Requeue expired lease.
- Reject stale completion.
- Accept duplicate completion idempotently.
- Enforce retry cap.
- Bound queue sizes.
- Preserve lane fairness.

### 2. In-Process Saturation Simulator

Run thousands of virtual workers against the gateway state machine in-process.

Use configurable service-time distributions:

- Fixed 10 seconds.
- Fixed 30 seconds.
- Log-normal 5 to 120 seconds.
- Bursty completion waves.
- 1 percent worker crash rate.
- 5 percent retryable failure rate.

Pass only if saturation and backlog behavior meet the pass criteria without HTTP overhead.

### 3. HTTP Loopback Load Test

Run the real gateway HTTP server and virtual async workers on localhost.

Measure:

- Claim latency.
- Completion latency.
- Heartbeat latency.
- Event-loop lag.
- CPU.
- Memory.
- Accepted completions per second.
- Request errors.

This test is the first choke detector for the coordinator itself.

If REST claim/complete misses the gates, pivot to persistent WebSocket worker connections before adding product features. The current implementation did this after REST showed hundreds-of-ms p95 under concurrent worker load.

### 3b. WebSocket Loopback Load Test

Run the real gateway server with virtual WebSocket workers on localhost.

Measure:

- Claim message round-trip latency.
- Completion message round-trip latency.
- Worker busy rate.
- Registered worker count.
- Accepted completions per second.
- Request/control-plane errors.

This is the primary v1 scaling gate.

### 4. Fake-Compute Worker Image Test

Run the real Docker worker entrypoint with `lab_http` transport and a fake compute mode that sleeps for the task's simulated runtime.

Validate:

- Image starts cleanly.
- Transport registers and claims.
- Worker count scaling works locally.
- No Fuzzfolio backend or Redis configuration is required.
- Gateway sees stable saturation.

### 5. Real-Compute Parity Test

Run a small fixed corpus through both paths:

- Existing `fuzzfolio-agent-cli` evaluation path.
- New PlayHand Lab path.

Compare:

- Score summary.
- Replay metrics.
- Candidate ranking.
- Error behavior.
- Run artifact shape.

Do not scale until parity is proven on deterministic inputs.

### 6. Local and LAN Soak

Use local processes and Sager-style LAN workers, not Vast.

Start with:

- 1 worker.
- 4 workers.
- 16 workers.
- 64 synthetic workers.
- 250 synthetic workers.
- 1000 synthetic workers.

Real compute does not need to reach 1000 local workers. Synthetic loopback must prove the control plane can handle the shape before remote spend.

## Output Contract

The new path must continue writing AutoResearch-compatible artifacts:

- `runs/derived/play-hand-lab-campaigns/<campaign-id>/run-metadata.json`
- `runs/derived/play-hand-lab-campaigns/<campaign-id>/play-hand-lab-campaign-events.jsonl`
- `runs/derived/play-hand-lab-campaigns/<campaign-id>/play-hand-lab-campaign-summary.json`
- `runs/<lane-run-id>/run-metadata.json`
- `runs/<lane-run-id>/attempts.jsonl`
- `runs/<lane-run-id>/play-hand-lab-lane-events.jsonl`

Compatibility fields should preserve current downstream expectations:

- `runner`
- `generated_by_runner`
- `run_status`
- `strategy_family_id`
- `canonical_attempt_id`
- `is_canonical_attempt`
- `is_canonical_playhand_attempt`
- `attempt_role`
- `attempt_decision`
- `play_hand_role`
- `play_hand_stage`
- `play_hand_instrument`
- `play_hand_selected_instruments`
- `profile_path`
- `artifact_dir`

The folder name may change from `play-hand-massive` to `play-hand-lab`, but corpus tools should treat both as PlayHand-generated runs.

## Implementation Order

1. Write the state-machine models and tests.
2. Build the in-process simulator and saturation assertions.
3. Add the HTTP gateway wrapper around the tested state machine.
4. Add the HTTP loopback load test.
5. Add `lab_http` worker transport with fake compute.
6. Add real compute task execution by reusing Fuzzfolio replay code.
7. Add PlayHand evaluation backend adapter.
8. Write AutoResearch run artifacts from lab results.
9. Add parity tests against the existing CLI path.
10. Only then run LAN workers.

## Open Decisions

- Whether the first coordinator should be FastAPI, aiohttp, or a smaller ASGI stack.
- Whether result uploads need compression in v1.
- Whether fake compute simulation belongs in the worker image or only in tests.
- Whether a lightweight periodic campaign snapshot is worth adding without making durability a hot-path dependency.

## Kill Criteria

Stop this approach and revisit the architecture if:

- A single coordinator cannot pass the 1000-worker synthetic loopback test.
- Realistic tasks are too short to keep request overhead below useful compute time.
- Result persistence becomes the primary bottleneck before worker saturation.
- Matching existing Fuzzfolio replay results requires pulling backend/Appwrite concepts back into the hot path.
- The implementation starts recreating Redis streams, shards, or product gateway behavior under new names.

## Validation Results

Validated on 2026-06-20 UTC:

- AutoResearch focused tests: `28 passed`.
- Trading-Dashboard compute-service lab worker/contract/bootstrap tests: `54 passed`.
- Trading-Dashboard backend worker-gateway tests: `43 passed`.
- `scripts/processes.json` parses and procman reloads it.
- Procman gateway entry starts through `POST /processes/a1634501-c982-47cd-b89e-c10d7c210222/start`, serves `/healthz`, and stops cleanly.
- Real local fake-compute smoke passed through actual `play-hand-lab-gateway`, actual Trading-Dashboard `sim-worker-replay --no-sync` with `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`, and actual AutoResearch coordinator: four tasks enqueued, four claims, four accepted completions, four acks, zero failed/lost/duplicate/dropped results.
- Post-hardening real local fake-compute smoke passed through actual `play-hand-lab-gateway`, actual Trading-Dashboard `sim-worker-replay --no-sync` with `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`, and actual AutoResearch coordinator: four tasks enqueued, four claims, four accepted completions, four acks, zero failed/lost/duplicate/dropped results. It wrote campaign `runs/derived/play-hand-lab-campaigns/20260620T034103043682Z-playhand-lab-campaign-v1`.
- Real local deep-replay smoke passed through actual `play-hand-lab-gateway`, actual Trading-Dashboard `sim-worker-replay --no-sync` with `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`, and actual AutoResearch coordinator: one task enqueued, one claim, one accepted completion, one ack, zero failed/lost/duplicate/dropped results.
- Deep-replay smoke wrote lane output under `runs/20260620T010820231526Z-playhand-lab-lane-000-v1` and an eval artifact under that run's `evals/` directory.
- In-process state-machine simulation with 1000 virtual workers and 10000 target completions passed: saturated busy rate average `1.0`, 10999 accepted completions, zero failed/lost/duplicate/expired completions.
- WebSocket loopback simulation with 500 virtual workers, 1-second synthetic work, and 2500 target completions passed: average saturated busy rate about `0.979`, 2999 accepted completions, zero failed/lost/duplicate/expired completions.
- WebSocket loopback simulation with 1000 virtual workers, 1-second synthetic work, and 4000 target completions passed: average saturated busy rate about `0.984`, 4999 accepted completions, zero failed/lost/duplicate/expired completions.
- Post-hardening WebSocket loopback simulation with 1000 virtual workers, 1-second lognormal synthetic work, and 2000 target completions passed: average saturated busy rate `0.983`, warm claim p95 `14.5 ms`, completion p95 `13.3 ms`, 2999 accepted completions, zero failed/lost/duplicate/expired completions. The initial 1000-worker cold claim wave still measured about `244 ms` p95 and should be treated as ramp pressure, not steady-state latency.
- WebSocket loopback simulations with 10 to 50 ms synthetic work completed without errors but did not meet saturation gates. This is documented as the expected micro-task overhead failure mode and should not be used as evidence for real deep-replay saturation.

Observed and fixed during validation:

- Stored/exported profile snapshots were too compact for the worker's current `ScoringProfile` schema. The coordinator now hydrates them via FuzzFolio `StoredScoringProfile.to_full_profile()` before enqueue.
- Coordinator summaries could keep a stale gateway snapshot if the last result arrived after the previous poll. The coordinator now refreshes the final snapshot after the result loop.
- Procman entries originally depended on a token already existing in the procman process environment. The procman helper now creates/loads a local token file.
- Lab result ack failures could make a successfully written attempt look failed. Ack errors are now recorded separately and do not change terminal task state.
- Worker-reported failed completions could be counted as completed tasks. The gateway now normalizes failed result payloads to top-level failed results and failed task counts.
- Lab worker matching did not enforce required contract/capabilities. The gateway now filters claims by `required_worker_contract_hash` and `required_capabilities`.
- Product lease heartbeats could persist lab loop metadata as replay progress. Product lease heartbeats no longer receive shared-loop progress metadata.
- Lab HTTP heartbeat handling ignored successful error-status bodies. It now re-registers on `worker_not_registered` and raises `LeaseLost` on `lease_lost`.
- Lab worker bootstrap generation could prefer product gateway env vars in lab mode. Transport mode now decides whether lab or product env vars are used.
- Lab transport files are part of the replay worker contract hash. Until the lab protocol has a separate negotiated compatibility version with stronger coverage, this prevents mixed lab worker images from silently accepting incompatible tasks.
- Lab protocol compatibility was implicit. Lab workers now advertise `playhand_lab_protocol:playhand-lab-worker-v1`, and lab tasks require it.
- Expired leases could be completed or renewed before the periodic reaper ran. Completion, heartbeat, result reads, and snapshots now reap/reject expired leases immediately.
- VM/Lightning keepalive sidecars could ping the lab gateway for `/bootstrap.sh`. Generated commands and bootstrap scripts now carry `FUZZFOLIO_KEEPALIVE_TARGET_URL` so keepalive targets the product bootstrap endpoint.
- Lab gateway body-size defaults were too low for larger deep-replay result payloads. The default is now 64 MiB and can be set with `--max-body-mb`.
- Product worker-gateway prefetch could serve a stale buffered stream message after that message had been reclaimed and leased elsewhere. The prefetch path now drops buffered messages that already have an active lease before handing work to another worker.
- Coordinator summaries could grow with completed task count. They now retain an accurate count plus a bounded result sample.
- Cumulative gateway metrics could contaminate later campaign summaries. Summaries now report campaign-scoped task counts and metric deltas while preserving raw gateway counters for diagnostics.
- Deep-replay `--tasks-per-lane` values above `1` duplicated the same generated profile. The coordinator now rejects that configuration and requires scaling through `--lanes`.
- The coordinator acked results one lease at a time. It now batches successful result acks per read batch.
- Deep-replay worker completions with unscoreable artifacts could produce green campaigns with `lab_scoring_failed` attempts. Unscoreable deep-replay results now fail the task and campaign.
- The gateway now rejects non-loopback startup without a token, and HTTP/WebSocket failure payloads parse string booleans such as `"false"` as false.
- Lab WebSocket workers now send tokens only in the Authorization header, not in the URL query string.
- `fake_compute` task durations are finite and capped by `FUZZFOLIO_LAB_FAKE_COMPUTE_MAX_SECONDS` so load tests cannot accidentally monopolize workers indefinitely.

Remaining cloud-scale validation:

- Start gateway plus tunnel or a stable public endpoint.
- Launch multiple remote replay workers with `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`, `FUZZFOLIO_LAB_GATEWAY_URL`, and `FUZZFOLIO_LAB_GATEWAY_TOKEN`.
- Run the procman deep-replay coordinator entry or equivalent CLI with a larger lane count.
- Watch for reverse-proxy/WebSocket limits, bandwidth limits, and market-data cache stampedes. These are the next expected failure domains after local synthetic saturation.
