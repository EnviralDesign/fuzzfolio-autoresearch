# Temporal QD V5 fast-ephemeral iteration playbook

This is the operational playbook for a disposable native Temporal-QD V5
`fast-ephemeral-v1` run: a fresh G1 through G5 system test with real worker
evaluation, controlled burst capacity, timing telemetry, and strict stop
boundaries. It is for proving the end-to-end system and collecting evidence
for the next improvement. It is **not** a promotable research result and it
does not authorize a continuation beyond G5.

The mode is deliberately non-resumable. A crash, code change, authority
change, or material operational anomaly means: preserve evidence, tear down
paid capacity, and start a **full clean-slate** next version only after the
cause is understood. A new run directory alone is not a new campaign.

## Operating principles

1. **A run is immutable once started.** Pin the source revisions, release
   binaries, worker image/contract, frozen evidence authority, configuration,
   generation count, and fresh run root before launch. Never patch those in
   place while a run is active.
2. **Procman controls processes; artifacts and telemetry establish progress.**
   A live PID is not evidence that a generation is healthy. Read the native
   progress/stage-summary events, gateway queue and worker state, and the
   generation artifacts.
3. **Use paid capacity only for useful queued backtests.** Vast is a burst
   tool, not an idle standby pool. Verify workers register and complete work
   before scaling further, and destroy them when they are no longer useful.
4. **Stop cleanly at the first real boundary.** The normal terminal boundary
   is successful G5 finalization. The exceptional terminal boundary is a new
   bug, integrity failure, repeated infrastructure failure, or unexplained
   performance/telemetry anomaly. Do not continue past either boundary unless
   the user explicitly directs it.
5. **Fast does not mean blind.** Keep only bounded, cadence-based telemetry in
   the hot path. Do not reintroduce candidate-scale replay, hashing, or
   bookkeeping merely to make an operator dashboard richer.
6. **Ephemeral campaigns require a full clean slate.** `fast-ephemeral-v1` is
   a versioned experiment, not a resume of the previous version. Every new
   version must start with a fresh run root, a fresh supervisor command, zero
   paid Vast, **and a restarted Lab Gateway**. G1 task IDs are deterministic
   from the frozen authority, so a leftover gateway that still remembers a
   prior campaign's terminal task IDs will reject Fresh dispatch as duplicate
   enqueues even when healthz is green. Do not keep a "healthy" gateway, queue,
   or worker set from the previous version.

## Zero-context startup: the first 15 minutes

An operator arriving with no prior conversation context should be able to
orient without guessing or reading old terminal scrollback.

1. Read this playbook and the Temporal-QD section of the repository
   `README.md`. Work from the autoresearch repository; generated run data stays
   under its git-ignored `runs/` tree.
2. Inspect, but do not modify, repository state:

   ```powershell
   git status -sb
   git log -1 --oneline
   git remote -v
   ```

   Treat a dirty worktree as evidence, not permission to stage everything.
   Identify which branch and commit the intended launch must use. If the
   request does not name a launch or an approved resource ceiling, stop and ask
   rather than reviving the newest-looking run.
3. Discover the live control plane and paid fleet with the read-only commands
   in [Process-manager and fleet readiness](#3-process-manager-and-fleet-readiness).
   Do not infer state from an old process ID, a desktop window, a stale run
   directory, or a process-manager display name.
4. Find the current launch definition in `scripts/processes.json` and inspect
   its command, fresh run root, pinned authority/configuration, and binary
   bindings. A previous launch script is historical evidence, not a template to
   blindly rerun.
5. Locate the candidate run root under `runs/`. Read its root configuration,
   state, recent structured progress/stage-summary events, and per-generation
   receipts before touching a process. A `stopped`, `failed`, or incomplete
   fast-ephemeral root is never resumable.
6. Establish an explicit decision: **observe**, **prepare a fresh launch**,
   **halt and diagnose**, or **finish terminal cleanup**. Do not mix these
   states in one action. A previous gateway that is still `Running` is leftover
   control-plane memory, not permission to reuse it for the next version.

### Source-of-truth order

When sources disagree, trust them in this order:

1. Current immutable run artifacts and structured native progress/stage-summary
   events.
2. Current lab-gateway queue, worker, lease, completion, retry, and health
   telemetry.
3. Live Procman API state for process control.
4. Read-only Vast CLI instance state for paid capacity.
5. A process's stdout/stderr tail and previous operator notes.

Procman liveness, a worker registration, or a Vast instance being `running`
alone never proves useful computation. Conversely, a healthy artifact trail
outweighs a stale GUI status.

### Permission defaults

- A fresh G1–G5 launch, a run restart after any halt, a continuation beyond G5,
  an increase above the approved paid-capacity ceiling, or a deliberately
  aggressive lake-stress test requires explicit user direction.
- Once a run and capacity ceiling are approved, the operator may scale within
  that ceiling only when the evidence rules in this document say the capacity
  is useful. The default approved ceiling for a normal G1–G5 fast-ephemeral
  campaign is **two Vast instances**, each with **32, 48, or 64 effective
  CPU cores**, and **never more than $0.50/hr per instance**.
- The operator may always stop the supervisor and destroy paid Vast capacity at
  a halt, completion, idle boundary, or host-safety boundary. Cost control and
  infrastructure safety override throughput.
- Never edit a frozen run, discard a completed run root, disable a safety check,
  or change quality policy merely to make a live campaign continue without
  explicit user direction.

## What this run is expected to do

- G1 constructs a 4,000-candidate immigrant pool and deterministically selects
  a 1,024-member evaluation population.
- Each generation evaluates its frozen rotating panel, reduces and archives
  the resulting population, and prepares the next generation.
- Later generations may retain parents, backfill coverage, and add newly
  constructed candidates. The exact task count is dynamic; do not assume that
  every generation has the same number of replay tasks.
- A completed generation means more than completed worker jobs: campaign
  sealing/reduction, finalization, archive publication, and the next-generation
  transition must all succeed.

An empty or tiny survivor archive is a **quality signal**, not automatically a
runtime failure. Record it for post-run analysis. Do not weaken selection gates
mid-run to force survivor counts upward.

A later generation that starts from that empty archive is **immigrants-only**.
The frozen breeding-confidence receipt then names **0 offspring** and a full
immigrant quota (reason `empty_archive_immigrants_only`). That 0 is a real
count. Do not treat it as missing, default it, or trip because Python
`value or -1` is falsy. G1 G0 configs may omit the hashed receipt entirely;
the first evolved 0-offspring freeze is typically G4 after G3 archived nobody.
v35 halted there. v36 passed the same freeze and finished G5.

## Preflight: establish a new immutable launch

Perform these checks before the supervisor is started and before any Vast
instance is rented.

### 1. Source and binary readiness

- Confirm the intended repository branch, commit, and remote state. Record any
  unrelated dirty files; do not stage or overwrite them while preparing a run.
- Incorporate and test any prerequisite fix first. If Rust/native code changed,
  rebuild every role executable needed by the frozen run and pin the resulting
  binary identities in the new launch material.
- Use a **fresh** run directory and fresh process-manager command definition.
  Never point a new run at an abandoned `state.json`, old output root, or an
  older run script.
- Treat `fast-ephemeral-v1` as non-resumable. If the selected root is not
  unquestionably fresh, discard that root rather than attempting adoption.
- A new version is a **full clean slate**: new run root, new supervisor
  command, paid Vast at zero, and a Lab Gateway restarted through Procman so
  it has no prior `recent_terminal_task_ids`, queued tasks, live leases, or
  completion memory. Reusing the previous version's gateway is an incomplete
  launch. See [Clean-slate ephemeral restart](#clean-slate-ephemeral-restart).

### 2. Authority and transport readiness

- Freeze the exact rotating-evidence authority, input archive/transport
  descriptor, worker contract, worker image digest, catalog/preparation inputs,
  and configuration used by the run.
- Verify the worker-facing lake endpoint is the intended public, reachable
  HTTPS endpoint for remote workers. Do not substitute a local-only address.
- Verify the gateway accepts the frozen worker contract and reports healthy
  registrations before the campaign is allowed to consume burst capacity.
- Keep credentials in their normal secret stores. Never place tokens, headers,
  API keys, private endpoint details, or local secret-file paths in configs,
  screenshots, logs copied to Git, or this runbook.

### 3. Process-manager and fleet readiness

Discover live process and group IDs each time; do not reuse IDs from notes.

```powershell
Invoke-RestMethod http://127.0.0.1:47831/health
Invoke-RestMethod http://127.0.0.1:47831/groups
Invoke-RestMethod http://127.0.0.1:47831/processes
vastai show instances-v1 --raw
```

Before launch, explicitly decide which approved LAN/local pools are in scope.
Confirm any previously rented Vast instances are absent unless the user
explicitly wants them preserved. A stale worker repeatedly reconnecting to an
old gateway is not usable capacity.

For `fast-ephemeral-v1`, also confirm the Lab Gateway will be **restarted**
before the supervisor starts. A gateway that is already `Running` from the
previous version is dirty control-plane state. Discover its live Procman ID
and restart it; do not skip this because `/healthz` returns `{ok:true}`.

### 4. Artifact map and minimum observation set

Native V5 evidence is generation-oriented below the fresh run root. Names can
evolve, so discover them rather than hard-coding a historical path, but the
minimum questions are stable:

| Question | Evidence to inspect |
| --- | --- |
| What exact experiment is this? | Root config, frozen authority bindings, pinned binary/image/contract descriptors |
| Has a generation truly finished? | Generation finalization/archive result plus the following generation's admitted stage |
| Are workers doing useful work? | Gateway task counts, leases, completions, retries, and accepted result flow |
| What stage is slow or stalled? | Structured `native_v5_progress` and `native_v5_stage_summary` timing events |
| Is evolution retaining quality/diversity? | Evaluation population, provisional selection, rejection distribution, and archive/parent-material outputs |
| Why did the run halt? | First typed error, nearby log lines, terminal state, and bounded inputs/receipts needed to reproduce it |

For a standard non-native run, `attempts.jsonl` and `controller-log.jsonl` are
the primary evidence. Do not apply that flat-file assumption to V5: its
proposal, campaign, seal/reduction, and finalization records are part of the
generation tree.

### 5. Preflight decision

Launch only when all of these are true:

- the supervisor command is current and points at the new root;
- the previous supervisor is stopped and is not the command about to launch;
- the Lab Gateway was restarted for this version and its snapshot is empty
  (`queued_tasks`, `live_tasks`, `completed_tasks`, and
  `recent_terminal_task_ids` are all zero);
- gateway health and worker contract admission are green;
- the public lake route is valid for the workers that will use it;
- release binaries and the frozen authority match the launch material;
- no stale state can be mistaken for progress; and
- paid capacity is still zero, unless the user explicitly requested a
  deliberate early stress test.

If any item is false, fix the launch material or halt. Do not rent Vast to
debug a preflight failure.

## Clean-slate ephemeral restart

`fast-ephemeral-v1` G1 construction is deterministic from the frozen
authority. Two versioned campaigns with the same authority therefore mint the
**same task IDs**. Fresh gateway dispatch requires every pending task to be
newly enqueued. If the Lab Gateway still holds the previous version's
`recent_terminal_task_ids`, the first enqueue batch is rejected as duplicates
and the supervisor tripwires. v33 halted this way after a correct new run
root was pointed at a leftover v32 gateway.

Do not mix recent halt classes:

- v32: parent-admission / direction-aware member lacked bound realized behavior.
- v33: leftover gateway `recent_terminal_task_ids` caused Fresh duplicate enqueue.
- v34: backfill freeze proof cardinality (proofs for parents already covered).
- v35: 0-offspring immigrants-only receipt treated as missing (`or -1`).
- v36: G1–G5 completed. Empty G3+ archives were a quality signal, not a halt.
      Per-generation `quality-audit/audit-error.json` with
      `cumulative archive exceeds the control-document limit` was written
      on G1–G5 and is **not** a tripwire. The audit is best-effort after
      finalization; it refuses to ingest the large cumulative archive as a
      control document. Do not halt for that stderr line. A real halt is a
      `supervisor_tripwire` event or `state.tripwire` payload.
- v37: G1–G5 completed after the playbook notes. Same archive shape as v36.
      G5 is not done when the first 4,096-task panel finishes. It still needs
      seal, any rotating-merge backfill, prefinalizer, and archive, same as
      G2–G4. Stop only after `status=completed`.

A new ephemeral version is therefore a full clean slate, not a new folder on
an old control plane. The required sequence, using live Procman IDs:

1. Preserve the halted or completed run root. Do not resume it.
2. Confirm paid Vast is zero; destroy leftovers and re-read the fleet.
3. Stop the previous supervisor through its live Procman ID. Poll until
   `Stopped`.
4. Create the new run root, launch script, and `scripts/processes.json`
   command. Reload **only** that supervisor process definition
   (`POST /processes/<supervisor-id>/reload`). Never `POST /stack/reload`.
5. Restart the Lab Gateway through its live Procman ID
   (`POST /processes/<gateway-id>/restart`). Restart, do not "start if
   already running." A green leftover gateway is still the previous campaign.
6. Poll until the gateway is `Running`, then verify:
   - unauthenticated `GET http://127.0.0.1:8799/healthz` is `{ok:true}`;
   - authenticated snapshot shows `queued_tasks`, `live_tasks`,
     `completed_tasks`, and `recent_terminal_task_ids` all at **0**.
   If those counters are nonzero, the slate is not clean. Do not start the
   supervisor.
7. Start the new supervisor. Confirm its first artifacts land under the new
   run root.

Python `urllib` is the reliable client for Procman JSON and the Lab Gateway
snapshot on this host. `Invoke-RestMethod` can flatten process lists. Never
print `FUZZFOLIO_LAB_GATEWAY_TOKEN`, lake tokens, or Vast `extra_env`. Parse
`vastai show instances-v1 --raw` locally and emit only id, label, state,
effective cores, and price.

### Host tooling on this Windows workstation

Use the repository venv, not a PATH `python`:

```powershell
C:\repos\fuzzfolio-autoresearch\.venv\Scripts\python.exe
```

Bare `python` on this host is a Microsoft Store alias and fails with exit
`9009` (`Python was not found`). Each versioned run root also carries
`_status_probe.py`, `_gw_queue.py`, and `_vast_create.py`. Call those with
the venv interpreter from the autoresearch repo. Supervisor stdout lives in
Procman `GET /processes/<id>/logs`, not a `runRoot/supervisor/` folder.

During a native campaign, `state.json` often lags. `workerTasksCompleted`,
`uniqueCandidatesEvaluated`, and `lastProgress` may stay empty until a
generation boundary, and `updatedAt` may freeze through
`gateway_dispatch` `completion_wait`. Trust `native_v5_progress` events and
the authenticated gateway snapshot for live work counts.

```powershell
$processes = Invoke-RestMethod http://127.0.0.1:47831/processes
# Select the current Lab Gateway ID and supervisor ID from that live list.
Invoke-RestMethod -Method Post http://127.0.0.1:47831/processes/<supervisor-id>/stop
Invoke-RestMethod -Method Post http://127.0.0.1:47831/processes/<gateway-id>/restart
Invoke-RestMethod http://127.0.0.1:47831/processes
```

## Launch and scale sequence

### Start the control plane first

Follow [Clean-slate ephemeral restart](#clean-slate-ephemeral-restart) before
this sequence. Do not start a supervisor against a leftover gateway.

1. Confirm the previous supervisor is `Stopped`.
2. Restart the local gateway through its live Procman entry. Verify `/healthz`
   **and** an empty authenticated snapshot.
3. Start the fresh native supervisor through its discovered Procman entry.
4. Confirm it writes fresh structured native progress and stage-summary events
   under the **new** run root, not the previous version.

Use Procman only with IDs obtained from the live API. After a start, stop, or
reload request, poll the process/group state until the expected state is
visible. Do not use a broad stack reload casually: it stops all managed
processes before rereading definitions.

The safe control pattern is discover, act on one verified ID, then read back:

```powershell
$processes = Invoke-RestMethod http://127.0.0.1:47831/processes
# Inspect $processes and select the exact current gateway or supervisor ID.
Invoke-RestMethod -Method Post http://127.0.0.1:47831/processes/<verified-id>/start
Invoke-RestMethod http://127.0.0.1:47831/processes
Invoke-RestMethod "http://127.0.0.1:47831/processes/<verified-id>/logs?limit=200"
```

Use the same pattern with `/stop` at a halt. Never substitute a remembered ID,
a display name, or a direct old PowerShell launch script for the current
definition. If Procman itself is unavailable, diagnose that condition first;
do not bypass it by launching an unpinned historical command.

### Default Vast ramp

Replay evaluation is CPU-bound. The GPU on a Vast offer is irrelevant. Rent
for **effective CPU**, not advertised host cores.

1. Keep Vast at zero through preflight, native construction, and any period
   with no real replay queue.
2. Once backtests are dispatched and the queue has useful demand, rent **one**
   approved instance: prefer **EPYC 48 or 64 effective cores**; **32 effective
   cores** is acceptable if nothing larger is available. Never exceed
   **$0.50/hr** for any instance, regardless of advertised hardware.
3. Verify the new workers register with the expected contract **and** complete
   real tasks. Registration alone is not enough. `auto` worker count is
   roughly `cpu_cores_effective - FUZZFOLIO_WORKER_CPU_RESERVE` (default
   reserve 1), further capped by container memory. A 64-effective-core box
   should produce on the order of ~60 workers, not 9.
4. Add a **second** matching instance only when the queue remains meaningfully
   backlogged, lake/gateway health remains normal, and the first instance is
   already completing work. **Two** such instances is the normal throughput
   ceiling. Do not add a third unless the user explicitly authorizes a
   lake-stress or operational-debug exception.

Cheapest GPU-share slices are a trap. Vast `cpu_cores` is the **host** core
count. `cpu_cores_effective` is what the container actually gets. A listing
that shows `cpu_cores=80` with `gpu_frac=0.125` often delivers
`cpu_cores_effective=10`, which yields ~9 workers after CPU reserve. That is
fine for a cost-only smoke or a contract-admission debug. It is **not** the
shape to use when the campaign is expected to run smoothly at throughput.

An explicit lake-stress run may start two or three approved instances sooner,
but it still requires a green public route and active observation of completions,
retries, queue depth, and lake health. Stop at the first non-benign anomaly;
the purpose is to learn the real limit, not to force work through a damaged
service. Lake-stress still obeys the $0.50/hr cap unless the user raises it.

### Offer selection

Search live offers immediately before create. Offers are transient. Filter on
**effective** cores and a hard price cap:

```powershell
vastai search offers --raw --storage 50 --limit 12 -o 'cpu_cores_effective-,dph' 'cpu_cores_effective>=32 cpu_ram>=48 reliability>0.99 disk_space>=200 inet_down>=200 rentable=true verified=true dph<=0.50'
```

Selection order:

1. Reject any offer with `dph` (or `dph_total`) above **$0.50/hr**.
2. Prefer `cpu_cores_effective` of **64**, then **48**, then **32**. Do not
   chase 80+ host-core GPU-share slices that collapse to 8–12 effective cores.
3. Prefer **EPYC** (`cpu_name` containing EPYC) when price and reliability are
   comparable. A 32-effective-core EPYC is better than an 80-host-core Xeon
   GPU slice.
4. Confirm `cpu_ram` is enough for the expected worker count
   (`~768 MB * workers + 2048 MB reserve`). A 32-core box needs roughly 32 GB
   usable; 48/64-core boxes need correspondingly more.
5. Re-read `cpu_cores_effective` on the **created instance**, not just the
   offer. If it comes up as ~10, destroy it; that is a GPU-share slice, not
   the intended CPU box.

When replacing a live cheap fleet mid-run, do not destroy every box first.
Keep one existing instance working, add the first large box, wait until it
registers on the frozen contract and completes at least one real task, then
destroy the remaining cheap instance and add the second large box. Destroyed
workers drop from the gateway after roughly **10 minutes**; their leases are
reclaimed and requeued. Expect a temporary dip in `busy_worker_count` and a
matching rise in `expired_leases_requeued`. That is expected, not a halt, as
long as completions keep advancing on the new contract-matching workers.

### Vast CLI operating procedure

Use the Vast CLI as the source of truth for paid capacity. Do not use a browser
tab, a remembered instance ID, or an old run script to infer fleet state.

1. Confirm the CLI and inspect the live fleet before every capacity decision:

   ```powershell
   Get-Command vastai
   vastai --help
   vastai show instances-v1 --raw
   ```

   Treat an empty instance list as zero paid capacity. Reconcile any existing
   instances by their verified IDs, labels, state, and cost before creating
   another one. The raw payload includes `extra_env` with worker and lake
   tokens. Never print it. Filter to non-secret fields before logging.

2. Before renting, derive the worker image, bootstrap/on-start material,
   contract, storage, network settings, and run label from the **current frozen
   launch material**. Never copy those values from a historical run. Inspect
   the installed command syntax before using it:

   ```powershell
   vastai search offers --help
   vastai create instance --help
   ```

   Search only for offers meeting the run's approved hardware, reliability,
   storage, networking, image, and hourly-cost constraints. The binding CPU
   field is `cpu_cores_effective`, not `cpu_cores`. The binding price field
   is `dph` / `dph_total`; reject anything above $0.50/hr. Verify the offer
   ID immediately before creation; offers are transient.

3. Create only the user-authorized number of instances, using that fresh offer
   ID and the generated current worker bootstrap. Do not put bootstrap content,
   tokens, passwords, or private endpoints in shell history, logs, this
   playbook, or a committed script. The shape is intentionally illustrative:

   ```powershell
   vastai create instance <verified-offer-id> --image <current-worker-image> --disk <approved-gib> --label <fresh-run-label> --onstart <generated-bootstrap-file>
   ```

4. Verify the rental independently, then verify it is useful: re-run
   `vastai show instances-v1 --raw`; wait for worker registration with the
   expected contract; then require a real completion before treating the
   instance as usable. A created VM that never registers is paid idle capacity,
   not a successful scale-up.

5. Teardown is equally deliberate. Read the fleet, select the exact instance
   ID, destroy it, then read the fleet again:

   ```powershell
   vastai show instances-v1 --raw
   vastai destroy instance <verified-instance-id> -y --raw
   vastai show instances-v1 --raw
   ```

   Destroy rather than merely stopping an instance when the goal is cost
   cleanup; its local disk should be considered disposable. Never run a broad
   or wildcard teardown command. Keep any required evidence in the run corpus
   before destroying capacity.

### Scale-down rules

- Destroy paid instances when the replay queue is exhausted or no useful work
  is expected for roughly 30 minutes **and the campaign is not in a known-good
  native phase whose next step is dispatch**. Do not leave them running while
  building, diagnosing, waiting for a code change, or deciding what to do next.
- Destroy them immediately when the run halts, fails, reaches successful G5,
  or needs a new launch.
- Verify destruction with a separate read-only `vastai show instances-v1
  --raw` query. Destroyed workers typically drop from the gateway after about
  **10 minutes**; their leases are reclaimed and requeued. Do not treat that
  reclaim window as a stall if the replacement instance is already completing
  work.
- Record only aggregate capacity/cost/timing facts in public notes; never copy
  account identifiers, credentials, or private host details.

## Lifecycle decision table

Use this table to prevent the two common operational failures: spending on idle
capacity and treating an unfinished run as a completed experiment.

| Observed state | Operator action | Vast action | Next decision |
| --- | --- | --- | --- |
| No approved launch or no fresh root | Inspect and prepare only | Keep at zero | Ask for approval when inputs are ready |
| Preflight incomplete | Fix the named preflight failure; do not start supervisor | Keep at zero | Re-run preflight |
| G1 construction, no dispatched replay work | Observe native progress | Keep at zero | Wait for first real queue |
| Backtests queued; existing workers healthy and backlogged | Verify completions before scaling | Add a second 32/48/64-effective-core instance if still at one | Continue at tight cadence |
| Backtests queued; lake/gateway/retries abnormal | Halt new work and capture evidence | Destroy all paid capacity | Diagnose; do not scale around the fault |
| Healthy known-good bulk replay | Poll at normal cadence | Hold only useful capacity | Scale down as demand drops |
| Known-good long native phase with no replay queue | Observe stage telemetry | Keep if the next dispatch is this campaign's evaluation or the following generation; destroy only for a halt, G5, or a real debug gap | Resume scaling only at real dispatch |
| Generation boundary or new code path | Tight observation | Keep only capacity that has imminent work | Verify full transition |
| G5 terminal finalization succeeds | Stop supervisor and capture report | Destroy all paid capacity | Audit; do not start G6 |
| Any novel invariant/contract/transport failure | Halt, preserve evidence, classify | Destroy all paid capacity | Full clean-slate next version: new root plus restarted gateway |

"Keep running" is therefore a decision backed by advancing artifacts and useful
work, not a default response to a process that still exists.

## Adaptive observation cadence

The cadence is event-driven first and clock-driven second. Tighten observation
at a new or risk-bearing boundary; loosen it only after the relevant path has
already demonstrated stable forward progress.

| Situation | Normal cadence | What must advance |
| --- | --- | --- |
| Preflight, gateway startup, first worker registration | 1–2 minutes | Health, contract admission, registration |
| First dispatch after a new launch or new worker image | 1–3 minutes | Leased tasks become completed results |
| First time through a generation phase or a recently fixed path | 1–3 minutes | Structured stage event, artifacts, queue movement |
| Healthy high-volume replay/backtest bulk | 5 minutes | Completion rate, leases, queue/backlog, lake health |
| Known-good native construction or expected long reduction/finalization | 10–15 minutes | Cadence logs/stage timers; no stalled transition |
| Generation boundary: seal, reduction, finalizer, next proposal | 1–3 minutes | Completed generation and next-stage admission |
| Any warning, retry storm, error, missing telemetry, or worker churn | Immediately, then 1–2 minutes | Classify or halt; do not wait for a normal interval |

Every status report should state the generation, exact active stage, elapsed
stage time, completed/queued/leased task counts where relevant, active worker
count by pool, recent retry/error signal, and whether paid capacity is useful.
At each meaningful stage boundary, emit or collect a concise timing summary for
construction, freeze, dispatch/backtest, sealing/reduction, finalization, and
the generation transition.

### Known-good native dead zones

These are not stalls. Keep observing stage telemetry; keep Vast if the next
dispatch is this campaign.

- G1 construction of 4,000 accepted immigrants is typically ~2 minutes.
- Evolved construction of 1,024 accepted candidates is slower and can take
  7–35 minutes as the accept rate drops. G3 in v36 needed ~3,400 attempts.
- `ephemeral_publication` after G1 is short (~30–40s). After G2+ it writes
  `parent-material.jsonl` and can take 7–15 minutes with a 1–3 GB native
  process. A 0-byte file that later grows is normal.
- First-round `campaign_seal` of a ~2.6 GB `results.pack` commonly takes
  6–8 minutes at ~1.2–1.5 GB working set. Later 512-task backfill seals are
  shorter.
- After the first 4,096-task panel round, G2+ often freeze additional
  coverage: a small round (tens of tasks) then one or more 512-task rounds.
  That is rotating-merge backfill, not a duplicate-enqueue failure, as long
  as `duplicate_task_enqueues` stays 0 and proofs match scheduled rows.
- A 4,096-task evaluation round with two 64-effective-core boxes (~122
  workers) is typically 30–40 minutes.
- After G1 finalization, `quality-audit/audit-error.json` with
  `TemporalQDV5ControlPlaneError: cumulative archive exceeds the
  control-document limit` is the known v36/v37 diagnostic. The supervisor
  continues. Treat it as a missing quality-audit artifact, not a halt.

At successful G5, `state.json` may show `status=completed` with
`currentGenerationIndex=6` and **no** `generation-0006` directory. That is
the supervisor's next-index after finishing generation 5. It is not G6.
Stop. Do not start another generation.

Use this compact report form so a new operator can take over without interpreting
free-form prose:

```text
Run: <fresh run label> | G<index> | <exact stage> | stage elapsed <duration>
Work: queued <n> | leased <n> | completed <n> | retry/error delta <summary>
Fleet: local/LAN <n> | Vast <n instances / n registered workers / effective CPUs> | useful: yes/no
Evidence: latest progress/stage-summary <timestamp> | archive/finalization: pending/verified
Decision: continue / scale +1 / scale down / halt-and-diagnose
Reason: <one evidence-backed sentence>
```

## Halt protocol

### Successful terminal boundary

At successful G5 finalization:

1. Verify the G5 archive/finalization evidence and terminal stage summary.
   `status=completed` plus a G5 quality-audit directory is enough. A
   `currentGenerationIndex` of 6 without a `generation-0006` tree is still G5
   complete.
2. Stop the supervisor; do not allow an implicit G6 or continuation.
3. Destroy all Vast instances and verify the fleet is zero.
4. Preserve the run root and capture the compact timing, quality, rejection,
   archive, worker, lake, disk-growth, and cost summaries for audit.
5. Do not interpret operational success as proof of strategy quality. Review
   survivor rates, diversity, and economic distributions separately.

### New bug or non-benign operational boundary

Stop rather than attempt an in-place recovery when any of the following occurs:

- a native invariant, schema, identity, archive, or state-transition failure;
- a result/transport integrity failure that is not a documented transient retry;
- repeated lake/gateway errors, retry growth, or worker failures without
  sustained completions;
- a contract mismatch, unexpected worker image, or worker-facing endpoint
  mistake;
- telemetry that stops advancing while a stage remains active; or
- unexplained resource pressure that threatens the host or the rest of the
  household infrastructure.

The sequence is:

1. Stop new campaign work through the live Procman process ID.
2. Capture bounded evidence: current state, recent relevant logs, progress and
   stage summaries, gateway snapshot, worker/task counts, and the exact error.
   Redact secrets.
3. Destroy paid Vast capacity unless an explicit short-lived diagnostic requires
   it and the user authorizes that exception.
4. Classify the failure as candidate-deterministic, native/contract, worker,
   gateway/lake, or operational-resource related. Do not label an unknown
   infrastructure failure as a bad strategy candidate.
5. Fix and test the root cause outside the stopped run. Then create a new
   immutable run root **and** perform a full clean-slate restart of the Lab
   Gateway. Never resume fast-ephemeral state. Never reuse the previous
   version's gateway, queue, or worker set.

A single documented retry that resolves and does not affect throughput may be
observed. Repeated retries, backlog growth, or a service entering an odd state
are not reasons to keep adding compute.

## Evidence to retain for the next iteration

For every completed or halted G1–G5 iteration, preserve a compact run report
and the run root. At minimum retain:

- pinned source commits, release-binary identities, worker image/contract, and
  frozen authority identifiers;
- stage timings and progress cadence summaries;
- population counts at construction, evaluation, provisional selection, and
  archive retention; plus rejection-reason distributions;
- parent/immigrant mix, diversity/deduplication effects, and archive counts by
  generation;
- gateway/lake health, completion/retry/error counts, and worker contribution
  by pool;
- peak resource observations where available, disk growth, and aggregate paid
  capacity/cost duration; and
- the first error and minimal reproducer inputs for any halt.

When asking an external reviewer to audit a partial run, share only a curated
artifact bundle and repository commit references. Exclude credentials, host
access details, secret files, account information, raw private logs, and
multi-gigabyte worker packs unless the failure cannot be diagnosed otherwise.

## Quality loop after operational success

The G1–G5 smoke validates the system, not the quality of the evolutionary
substrate. An operationally clean run can still be scientifically unhelpful if
almost no evaluated candidates can become diverse, robust parents. Audit
candidate quality separately:

1. Locate the earliest funnel stage where candidates are lost: generation,
   structural selection, economic evaluation, eligibility, deduplication, or
   archive reduction.
2. Compare score, trade-support, directionality, and diversity distributions
   across rotating panels instead of inferring quality from survivor count alone.
3. Distinguish four hypotheses: the grammar/state-machine produces weak or
   repetitive programs; mutation/crossover degrades useful structures; quality
   gates misjudge viable candidates; or archive/deduplication collapses useful
   diversity.
4. Run the smallest controlled diagnostic needed to separate those hypotheses.
   Do not lower every gate merely to manufacture survivors.
5. Promote only a tested, measurable improvement. If it changes native code,
   workers, contracts, or authority, restart the next fast-ephemeral iteration
   from a new G1 root.

The quality conclusion must name the earliest supported failure point. For
example, "only two parents survived" alone cannot tell us whether the grammar
is weak, the mutation operator is destructive, the market criteria are strict,
or archive reduction is too aggressive. A useful audit ties every conclusion
to a measurable distribution and proposes the smallest discriminating rerun.

## Handoff and post-run record

Before leaving a live run for another operator, write a short handoff beside
the run (or in an approved operational note). It must answer:

1. What is the exact run root and immutable launch identity?
2. What stage and generation are active, and what artifact/event proves it?
3. What was the last observed queue/completion/retry state and time?
4. Which workers are approved and which paid instances are currently useful?
5. What is the next observation deadline and what result would trigger a halt?
6. Are there any known warnings, pending fixes, or explicit user constraints?

At terminal G5 or a halt, append the same facts plus the final decision. This
is intentionally a small structured record; do not paste secrets, long raw
logs, or whole worker result payloads into Git.

## Short operator checklist

- [ ] Read this playbook, the Temporal-QD README section, and the live state
      before taking a control action.
- [ ] Know whether the authorized state is observe, prepare, run, or halt.
- [ ] Fresh root; no attempted resume; Lab Gateway restarted for this version
      and snapshot task memory is empty.
- [ ] Current source and release binaries pinned.
- [ ] Frozen authority/config/worker contract/image recorded.
- [ ] Worker-facing public lake route and gateway admission verified.
- [ ] Procman IDs discovered live; stale definitions not reused.
- [ ] Vast is zero until useful replay work exists, except an explicitly
      authorized stress test.
- [ ] Throughput fleet is two instances of 32/48/64 **effective** cores;
      never more than $0.50/hr each; do not rent GPU-share host-core slices
      for a production G1–G5.
- [ ] First dispatch and each unfamiliar boundary observed tightly.
- [ ] Healthy bulk work observed at a relaxed cadence with stage telemetry.
- [ ] Paid capacity destroyed at idle, halt, and successful G5.
- [ ] G5 is terminal unless the user explicitly orders a separate continuation.
- [ ] Halted runs are diagnosed and replaced with a full clean-slate next
      version (new root, restarted gateway, zero Vast), never resumed.
- [ ] A compact handoff or terminal record identifies the evidence-backed next
      decision.
- [ ] Zero-offspring immigrants-only is a valid evolved freeze; do not halt
      merely because G3+ archived nobody.
