# Temporal QD Rust front-half architecture

Date: 2026-08-05

Status: implemented, selected as the pair-generation default, and admitted for
the complete G0 path at 64, 128, and 1,024 candidates on 2026-08-06.
`python_optimized_v1` remains an explicit exact oracle. Final admission still
requires the integrated non-empty-parent reproduction gate described below.
None of this work authorizes an economic campaign.

## Decision

Move the complete pre-market bidirectional generation into one Rust subsystem
call per generation. The stable boundary is the pair-mode work now entered
through generate_qd_generation: from a verified parent archive and frozen
generation authority through immutable proposal construction, global identity
accounting, optional G0 reduction, population finalization, evaluation
projection, and generation-journal publication.

    Python temporal QD supervisor
      -> Rust GenerateGeneration
           construct and validate proposals
           persist/recover proposal state
           G0 construct-pool projection and selection
           publish population and evaluation projection
      <- compact generation result and artifact identities
      -> Python freezes the worker campaign
      -> gateway / distributed worker replay
      -> Python rotating-evidence and QD archive transaction

This deliberately stops before campaign materialization and distributed replay.
Those paths are already coarse operations in the Python supervisor and define
the worker/economic contract. Moving them would enlarge the compatibility
surface without removing proposal-level Python/Rust ping-pong.

The Rust process may stay alive for a run and cache verified immutable
authorities, but GenerateGeneration is the only semantic front-half request.
There must not be one FFI or subprocess hop per mutation, candidate, G0 row, or
population member. The supervisor calls Rust once for a generation and does not
receive candidate documents back in memory; Rust writes the generation root and
returns identities and counters only.

This follows the 2026-08-05 optimization plan: first establish a compact typed
storage/restart boundary, then move a module-scale batched proposal kernel. It
does not port the multi-gigabyte Python representation directly.

## Current boundary map

The installed temporal-qd-supervisor command enters run_qd_supervisor. The
supervisor freezes its config, restores and validates its state and prior
generation artifacts, then invokes pair-mode generation for each unfinished
generation. Once the proposal population is complete, it freezes the screening
campaign, submits the finite task matrix to the Lab gateway, and only after
every required result is durable performs the rotating-evidence or ordinary
archive reduction.

The existing optimized pair generator already has the correct logical extent:

1. Open a frozen pair authority and reconstruct compact proposal state from the
   immutable proposal journal.
2. Deterministically schedule immigrants, offspring, or crossover and call the
   native validation/compiler authority.
3. Record exact accepted/rejected proposal entries and campaign-wide duplicate
   information.
4. If G0 is enabled, project the full constructed pool into no-market
   descriptors, select the evaluation subset, and retain the construction
   ledger.
5. Create population.json, evaluation-population.json, and
   generation-journal.json.

The existing Rust population finalizer is intentionally a narrower version of
this pattern: Python authors an exact shell and journal manifest, and Rust
authenticates existing proposal entries, copies canonical candidate byte ranges,
derives the population identity, and atomically publishes the population. The
new subsystem extends that boundary; it does not discard its byte-preserving
discipline.

Relevant current modules:

- autoresearch/temporal_qd_supervisor.py owns immutable supervisor config/state,
  the generation loop, campaign freeze, worker dispatch, and archive
  transaction.
- autoresearch/temporal_qd_evolution.py owns the public generation entrypoint
  and legacy fallback.
- autoresearch/temporal_qd_pair_generation.py owns optimized pair proposal
  construction, journal replay, G0 handoff, finalization, and the evaluation
  sidecar.
- autoresearch/temporal_qd_g0_bootstrap.py owns closed pre-economic projection,
  pool, ledger, and deterministic diversity selection.
- autoresearch/temporal_qd_population_finalizer.py and
  rust/temporal-qd-population-finalizer own the existing exact Rust
  finalization contract.
- autoresearch/temporal_qd_campaign.py and temporal_search.py own the
  post-seam task materialization and gateway-result durability.

## Exact GenerateGeneration contract

The Rust interface is a versioned request/response contract. It accepts paths
only for large immutable inputs, and every path has a declared identity that
Rust must verify before reading it.

### Request

`temporal_qd_native_generate_generation_manifest_v1` contains:

| Field | Requirement |
| --- | --- |
| Operation and output binding | `generate_generation`, generation index, output root, contract version, and final-newline policy. |
| Parent archive binding | Verified archive path and archive SHA. The archive's own closed identity validates its contents; generation config/runtime authority bind the expected pair policy. |
| Frozen supervisor policy | Normalized search parameters, proposal ceiling, policy SHA, and generation config SHA. |
| Pair authority | Exact pair-run-config payload/SHA and its frozen Dashboard native transport authority. |
| Construction evidence | Predeclared evidence context, frozen catalog identity, and operator implementation identity. |
| Identity state | Campaign-wide ledger root/hash or a sealed checkpoint that binds its identity indexes. |
| G0 binding | Explicit construction-pool size and evaluation-population size, or an explicit disabled value. |
| Resume binding | Existing generation artifacts/segment head and the expected immutable parent/config/authority identities. |

Pair mode does not receive Generator v2 source paths or the legacy subprocess
validator; that remains a separate Python compatibility path. No gateway URL,
market window, worker result, P&L, reserved evidence, or outer-tail input is
admitted to this request.

### Result

`temporal_qd_native_generate_generation_result_v1` mirrors the public information now
returned by pair generation:

- completed, generation index, config SHA, and output root;
- population, evaluation-population, and generation-journal identities;
- proposal, candidate, accepted-origin, duplicate, and proposal-slot counts;
- next continuation ordinal when applicable;
- G0 construction-pool count and the four bound G0 artifact identities when
  enabled; and
- a sealed generation-head identity that binds compact internal state to the
  compatibility artifacts.

It must not return profiles, proposals, parent members, or replay results.
Python continues by reopening the compact evaluation sidecar and calling the
existing campaign freezer.

## Ownership

| Owner | Responsibilities |
| --- | --- |
| AutoResearch policy | Deterministic proposal ordinal/seed schedule, parent selection, mutation and immigrant policy, global duplicate policy, G0 diversity selection, evidence-panel schedule, task fan-out, breeding/archive policy, and run orchestration. |
| Rust front half | Efficient typed implementation of AutoResearch generation policy, compact storage, journal/checkpoint recovery, canonical artifact assembly, and resource-aware batch execution. |
| Trading-Dashboard / FuzzFolio | Temporal-graph model parsing and resolution, catalog hydration and indicator semantics, native candidate validation, bidirectional v2-to-v3 compilation, aligned observations, replay/execution, costs, R accounting, and management-effect semantics. |
| Python supervisor | Frozen authority checks, run state.json, generation-boundary publication, campaign/task creation, gateway lifecycle, worker-result persistence, evidence/archive reduction, and terminal tripwire behavior. |

Rust may construct an AutoResearch proposal, but it must treat the
Dashboard-native response as opaque, identity-bound authority. The
Dashboard-authored raw profile, compiled v3/both profile, resolved program,
validation report, profile snapshot hash, program hash, and evaluator identity
must never be recreated by a second execution, indicator, or management
interpreter in AutoResearch.

The persistent Dashboard JSONL process should be owned directly by the Rust
front-half process during a generation. This replaces the current Python
transport layer, not the Dashboard implementation behind it. Requests remain
ordered and bounded. After request bytes have been written, timeout, broken
pipe, malformed response, or response-identity mismatch is a fail-closed native
session failure; it is not permission to invent a result or silently substitute
a local validator.

## Compatibility facade and internal representation

### Exact-compatibility phase

For the current public generation artifact contract, Rust must produce the same
bytes as the Python oracle for all semantic outputs:

- pair config and generation config;
- every proposal-journal entry, proposal ordinal, entry hash, and
  accepted/rejected disposition;
- global identity ledger and generation checkpoint material visible to current
  restart readers;
- G0 accepted pool, construction ledger, selection artifact, selection trace,
  and bindings;
- population.json, including candidate order, canonical candidate bytes,
  population hash, and final newline;
- evaluation-population.json, candidate projections, funnel subset, and G0
  selected-subset binding; and
- generation-journal.json and the public result payload.

The Dashboard JSONL request/response schemas and finite worker task contract
are compatibility boundaries as well. Values may be streamed or borrowed
internally, but their identity-bearing content cannot be reformatted or
reinterpreted. Unicode escaping, finite-number validation, negative zero, key
ordering, SHA-field removal, and platform newline policy are parity concerns.

### Freely redesignable internals

The following may change behind an opaque Rust-owned internal root, provided
they do not participate in a public artifact identity:

- typed in-memory structures, arena layout, batch size, deterministic local
  parallelism, caching, and hash indexes;
- immutable content-addressed objects for profile/module/pair/program/catalog
  material;
- packed or compressed object files, indexes, temporary files, and publication
  locks;
- sealed append-only proposal segments, compact checkpoints, and telemetry; and
- cached immutable pair authority and persistent native child-process lifecycle.

The existing object store and sealed journal are foundations, not yet
authoritative proposal state. Before either becomes authoritative it must bind
the campaign-wide identity ledger and prove the same split/restart behavior as
the current generation root.

There is no way to replace the rich current proposal journal with only compact
references and also claim byte identity with it. The first cutover therefore
uses a compatibility facade. A later compact public artifact format requires a
new schema/root and explicit reader/hydration adapters; it must not silently
change existing v3 paths or resume rules.

## Durability and restart ordering

For one proposal batch the durable order is:

1. Revalidate parent archive, frozen config, pair authority, catalog/evidence
   context, and native authority identity.
2. Construct deterministic proposals and obtain/verify Dashboard-native
   results.
3. Publish immutable content objects, if used, and verify them by content hash.
4. Seal an append-only proposal segment containing compact proposal records and
   the corresponding identity-ledger delta.
5. Advance the in-memory identity index only from the sealed segment.
6. Publish a checkpoint whose head binds the segment chain, accepted references,
   scheduling counters, and ledger identity.
7. Materialize or verify byte-identical compatibility proposal/checkpoint files.
8. After the target population exists, publish G0 artifacts, population,
   evaluation projection, generation journal, and a sealed generation head.

Every immutable publication is write-once, fsynced, and verified before a later
artifact may name it. Existing exact files are accepted; divergent files fail
closed. Private stale temporaries can be cleaned only when they are not a
published artifact. A segment that is durable but whose checkpoint publication
failed is a committed receipt: it must not be appended a second time. Reopening
must rebuild/reseal the checkpoint idempotently from the segment chain.

On restart, Rust validates the request binding and the complete compact/public
artifact chain before doing new native work. It reconstructs scheduling state
from the last verified checkpoint and performs only bounded *new native work*
and bounded crash-tail repair, but current integrity reopening still walks the
historical segment/checkpoint/public inventory. Because checkpoints retain
cumulative accepted references and pending rich-tail state, that validation is
linear-to-superlinear in historical bytes rather than O(1). This is deliberately
fail-closed and semantically exact, but it is a deferred storage/restart
optimization—not a claimed constant-time reopen. The outer Python supervisor
still validates completed generations and frozen sources before it creates a
gateway client. A failure remains a supervisor tripwire and leaves an
inspectable root; there is no automatic continuation, authority refresh,
Python fallback, or economic resubmission.

The front-half engine is frozen for one generation root. Same-engine restart is
required and admitted; switching engines in the middle of a partial generation
is deliberately unsupported. Python and Rust oracle runs therefore use
independent roots. A completed Python generation may feed a later Rust
generation through the verified public archive boundary, but a partial Python
root has no Rust segment chain and is never fabricated into one. Mixed or
divergent authorities, missing selected G0 references, ledger-head mismatch,
corrupted/truncated segments, path escape/symlink substitution, or ambiguous
native transport outcomes stop the run.

Before the Rust subprocess is built or launched for a valid generation width,
the shared Python seam enforces the same 12 GiB minimum host-available-memory
floor used by the Python proposal watchdog. It also requires output-volume free
space of 4 GiB plus 8 MiB per target construction candidate (about 35.25 GiB
for a 4,000-candidate G0 pool). This is a conservative prelaunch guard based on
the admitted artifact measurements. The native run does not yet have a
continuous host-pressure watchdog; measured shape-1,024 tree peak was only
517.6 MiB, so adding one is deferred rather than expanding this completed
optimization pass.

## Staged admission

1. **Golden corpus and contract harness.** Freeze representative 64, 128, and
   1,024 candidate roots, including G0, parent/offspring, crossover,
   split/restart, and corruption fixtures. Record exact public bytes and fresh
   process-tree performance baselines.
2. **Extend the admitted finalizer boundary.** Keep Python proposal production,
   but move exact sidecar/generation-journal assembly beside the existing Rust
   population finalizer. Require byte-exact artifacts and both-direction resume.
3. **Shadow compact state.** Write content-addressed objects and sealed proposal
   segments alongside the Python oracle. Hydrate and verify them against the
   rich journal; bind the compact checkpoint to the global identity ledger
   before relying on it for resume.
4. **Rust batch proposal kernel.** Rust directly drives the frozen Dashboard
   JSONL authority for a full generation while emitting the legacy facade.
   Compare independent Python and Rust roots byte-for-byte, not merely by final
   score or population hash.
5. **Default cutover.** Enable Rust only after 64/128/1,024 exact parity,
   uninterrupted and same-engine split restart, stale-authority, partial-write,
   path-safety, selected-G0-subset, and native transport-failure gates pass.
   Measure end-to-end wall time, CPU, I/O, peak process-tree RSS, and host
   headroom. Keep the Python oracle selectable until a complete normal
   parent/archive generation passes the integrated batch boundary and
   independent review approves the switch.

Each stage is reversible. A microbenchmark win, storage-only result, or faster
non-durable write is not admission evidence.

## Native admission result

The G0 implementation was admitted with independent Python and Rust roots at
64, 128, and 1,024 accepted candidates. Every gate ran an uninterrupted build,
a bounded split, and restart from the split root. The comparer streamed large
artifacts in place and required all of the following to be exact:

- public semantic trees and public bytes;
- proposal and candidate identities;
- campaign-wide identity-ledger records and file identity;
- G0 pool, ledger, selection, and trace identities; and
- uninterrupted versus restarted results for both engines.

| Accepted candidates | Python full wall | Rust full wall | Speedup | Python tree peak | Rust tree peak | Rust batch peak | Admission report SHA-256 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 64 | 263.50 s | 92.53 s | 2.85x | 255.1 MiB | 279.2 MiB | 117.9 MiB | `sha256:fdad34ba96c351c00e33992eb9428483e76810f6e63f94a161d7bf379d2467db` |
| 128 | 549.33 s | 189.47 s | 2.90x | 261.3 MiB | 283.5 MiB | 121.8 MiB | `sha256:1fb15a752cec774a3183c250fe6741e79d986b065659a51602561e86fd1e54f1` |
| 1,024 | 4,360.88 s | 1,521.31 s | 2.87x | 371.1 MiB | 517.6 MiB | 355.0 MiB | `sha256:ee921d288c5a78b506b111c7ad49c98d75332b152531841976cbab94e63f6436` |

At 1,024 candidates, restart wall time was 4,061.84 seconds for Python
and 1,288.42 seconds for Rust, a 3.15x improvement. The admission telemetry
measures the isolated worker and recursive child process tree. The Rust batch
owns the AutoResearch front-half loop; the persistent Dashboard JSONL child
remains the canonical FuzzFolio graph validation and compilation authority.

The admitted cutover changes only the pair-generation default to
`rust_native_v1`. Fallback remains forbidden. Selecting
`python_optimized_v1` is an explicit oracle operation, not an automatic error
recovery path.

At shape 1,024 the Python oracle roots occupied 2.92 GiB and the Rust roots
occupied 8.08 GiB. Of the Rust total, 5.24 GiB is private segment/checkpoint
material used for bounded restart rather than public generation output. This is
an accepted operational tradeoff for this cutover, not a semantic difference:
the public artifact trees remain byte-identical. A 4,000-to-1,024 construction
run was deliberately not extrapolated into an admission claim and still needs
an explicit free-space check before launch. Further storage optimization was
deferred after the requested 1,024 A/B boundary.

The verified non-empty-parent shape-64 uninterrupted canary is now exact. It
used the generation-2 archive from the retained 1,024-wide historical campaign,
whose 1.83 GiB public archive contains 11 selectable quality parents and 118
negative-novelty members. Python and Rust both produced 64 accepted candidates
from 68 proposals: 45 accepted mutations, 6 accepted crossovers, 13 immigrants,
and 4 rejected structural proposals. The complete public byte trees and
semantic projections are identical:

- public byte-tree SHA-256:
  `sha256:892a9b03c0d93d4fca7021e6e7639eea85607fed41d37529c51f8a020daf654c`;
- native source SHA-256:
  `sha256:cb0011580a8f17c95a1aa01156d68eb9a7b17d872613812aae2570880211bc8a`;
- Windows release executable SHA-256:
  `sha256:b113880b0d6ed97846cf3e1c834710dc95538511eef213602a441a0a8b41f448`;
- Rust wall time: 420.50 seconds;
- Rust batch peak RSS: 7,983,169,536 bytes (7.44 GiB); and
- complete isolated process-tree peak RSS: 8,118,317,056 bytes (7.56 GiB).

This replaces the pre-compaction 30--38 GiB parent-archive failure and remains
below the 12 GiB canary ceiling. The compact runtime retains rich pair JSON only
for selectable parents, shares it by reference, streams archive self-hashes,
drops raw staging values before proposal production, and keeps compact ledger
identity material for the full archive.

The final integrated non-empty-parent admission passed on 2026-08-07. Both
engines ran uninterrupted, stopped after 32 proposals, and resumed from the
same durable split root. Full and resumed result identities, public semantic
trees, public bytes, proposal/candidate/ledger identities, and parent-origin
evidence were exact within each engine and across engines. Full and restarted
trees share the same public byte-tree SHA-256 shown above. Both engines recorded
55 structural and 13 immigrant proposals, including 45 accepted mutations and
6 accepted crossovers. The immutable admission report is:

`C:\fuzzfolio-research\temporal-qd-native-parent-admission-64-20260807-v6\admission-report.json`

with report SHA-256
`sha256:f8dd5987182c2c9eff87428d04d0515ab11d2a00c921fd3123fc3c4347de30c9`.

| Engine | Full | Split | Resume | Three-leg total | Peak process-tree RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python oracle | 1,025.84 s | 873.12 s | 1,038.76 s | 2,937.72 s | 6.95 GiB |
| Rust native | 397.70 s | 273.81 s | 360.95 s | 1,032.47 s | 7.56 GiB |

Rust completed the full admission matrix 2.85x faster. Its measured peak stayed
below the 12 GiB canary ceiling. This closes the final front-half cutover gate;
`rust_native_v1` is admitted for the next economic campaign while
`python_optimized_v1` remains the explicit exact oracle.

## Explicit non-goals

- No economic campaign, worker-fleet change, Vast operation, or automatic launch
  follows from this decision.
- No rewrite of distributed replay, Lab gateway semantics, worker contracts, or
  worker-result durability is in scope.
- No duplicate FuzzFolio execution engine, indicator evaluator, native
  validator, graph compiler, or management interpreter is permitted in
  AutoResearch/Rust.
- No change to rotating evidence, cumulative breeder/archive semantics,
  selection economics, protected/outer-tail evidence policy, instrument scope,
  or G0 pre-economic restriction is authorized here.
- No compact public artifact cutover, deletion of rich historical artifacts, or
  removal of Python oracle paths occurs without a separate versioned artifact
  decision and reader-migration admission.
- No migration of the archive/reducer tail is implied; it remains a later,
  separately measured boundary after the front-half compact representation is
  admitted.

## Resolved constraints and deferred risks

- **Resolved:** Python deterministic RNG and canonical serialization behavior
  are explicit Rust contract surfaces with golden vectors, runtime transcript
  replay, and exact public-byte comparison.
- **Resolved:** the Dashboard persistent JSONL endpoint retains its ordered,
  non-retriable-after-write, fail-closed behavior under the Rust-owned child
  lifecycle.
- **Resolved:** G0 places every constructed identity into campaign-wide
  duplicate protection while only the selected subset enters market
  evaluation; the compact ledger and exact public facade preserve that
  distinction.
- **Deferred:** private checkpoint/segment growth and full-history integrity
  reopening remain material disk and restart costs. A content-addressed or
  compacted successor only helps after durability-equivalent producer and
  corruption/restart benchmarks; the previously rejected compact raw-adapter
  result is not an implementation shortcut or admission precedent.
