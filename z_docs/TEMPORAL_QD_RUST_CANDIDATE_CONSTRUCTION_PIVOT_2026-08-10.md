# Temporal QD Rust Candidate-Construction Pivot

Recorded: 2026-08-10 America/Chicago

## Why the fresh campaign stopped

The fresh v5 campaign at
`C:\fuzzfolio-research\temporal-qd-v5-fresh-4000x1024x5-20260810-v1`
stopped during generation-1 candidate construction after writing 1,216 of the
planned 4,000 proposal-journal entries. The supervisor tripwire was
`minimum_host_available_breached`; the QD process tree itself remained close
to 200 MiB RSS. The host-wide pressure was external to QD, but it exposed that
the remaining Python proposal loop is still too slow and too dependent on an
observational machine-wide guard.

No G0 Rust diversity reduction, market evaluation, or economic worker task had
started. Preserve this stopped run as a real v5 Python-oracle corpus.

## Observed Python candidate-construction envelope

- Approximately 0.93 candidates/second on one CPU core.
- Approximately 70-75 minutes projected for 4,000 candidates.
- Proposal-journal growth projected near 6.5 GiB for the initial pool.
- Observed QD process-tree peak near 210 MiB RSS; this phase is currently a
  speed problem rather than a QD memory problem.
- Dominant per-candidate spans were pair creation, pre-economic G0 projection,
  factory audit, payload construction, and candidate materialization.

One sampled accepted entry was 1,740,455 bytes. Its proposal carried a roughly
570 KiB frozen pair; the candidate then repeated effectively the same pair as
both `pairProposal` and `bidirectionalGenome`, while each frozen side repeated
about 220 KiB of static catalog authority. This is storage-contract bloat, not
strategy complexity. The Rust transaction should therefore use a new
content-addressed compact journal: seal shared authority objects once, store
candidate-specific deltas once, and retain an exact lossless projection to the
legacy rich entry for oracle testing and selected-worker handoff.

## New objective

Replace the entire v5 proposal-construction transaction with a Rust-owned,
bounded, parallel implementation. This is not a PyO3 micro-optimization and
must not retain a Python validator/compiler service behind the Rust loop.

The production Rust authority must own:

1. Deterministic proposal seeds and origin allocation.
2. Bidirectional evolvable genome construction.
3. Indicator, fuzzy evidence, event, management, protection, hold, temporal,
   topology, and graph-resource construction.
4. Later-generation mutation and compatible crossover, not only G0 immigrants.
5. Closed-schema v2 module compilation and v3 bidirectional compilation.
6. The construction-relevant native validation subset with exact oracle parity.
7. Frozen module/pair/candidate identities, audits, lineage, and hashes.
8. Global semantic/candidate duplicate accounting and collision dispositions.
9. Durable proposal-journal publication, restart adoption, and corruption
   rejection.
10. Phase timing, bounded worker concurrency, RSS, and I/O evidence.

The sealed shared authority must contain the complete static material required
to execute the Rust engine independently: catalog/capability data, contexts,
indicator and management policies, operator registries and finite domains,
compiler policy, budgets, and native authority snapshots. Opaque hashes without
their corresponding content are not an executable restart authority.

Historical/public identity and native execution authority are deliberately
separate. The exact legacy source operator implementation and per-side policy
identity snapshots remain byte-for-byte inputs to FrozenModule, pair, and
candidate identities. A new self-hashed native operator authority binds the
Rust registry, budgets, compiler-policy hash, and complete execution-only
policies. The enclosing shared authority binds both plus the full evolvable
authority. Native authority hashes must never leak into or perturb legacy
public candidate identities.

Per-side authority therefore has two layers: the original public policy
IdentitySnapshot, unchanged, and a separately sealed execution closure with
the full module policy, indicator policy, sorted seed names, semantic catalog
identity/timeframe policy, and resource-operator specification. Rust must parse
this exact typed envelope; heuristic nested-object discovery is not an admitted
authority boundary.

The compact journal must also record every construction attempt, not only
accepted candidates. Proposal ordinal and seed, origin, deterministic
disposition/reason/audit, accepted birth ordinal or accepted-record reference,
and identity-ledger effects must survive crash/restart exactly. Invalid/no-op
mutations, incompatible crossover, and global duplicates must never be silently
regenerated or skipped after a restart.

Later generations must remain fully native above the operator layer too. Rust
opens and validates the parent archive and identity ledger, applies the frozen
accepted-slot reproduction quota, selects direction-aware parent lanes, and
records parent/plan/step outcomes. Rejected or no-op attempts do not advance
the accepted 80/20 offspring/immigrant quota. Python may pass sealed path and
identity bindings, but it may not parse or select parents.

The current schedule has two distinct clocks and both are identity-bearing.
The immigrant/offspring choice follows accepted-slot deficits, while the
same-side crossover opportunity follows proposal-attempt ordinal 6 modulo 7.
Rejected and no-op proposals therefore advance the crossover cadence but not
the accepted-origin quota. Parent selection has its own monotonically
increasing draw ordinal; the primary parent, crossover mate, and every
same-parent mate retry each consume one draw, including the negative-novelty
slot cadence. Rust must reuse the admitted kernel scheduler/archive selector
and persist these counters rather than implementing a second approximation.

The immutable transaction receipt must authenticate the complete output tree:
shared authority, every-attempt journal and semantic root, compact accepted
records, identity ledger, content-addressed objects, G0 pool/selection/ledger,
selected projection index, streamed selected-only worker population and
evaluation population, generation journal/result, and all file/semantic
hashes. Receipt adoption still launches one native verifier process; it may
not trust a Python-side ``result exists`` shortcut. Adoption telemetry is a
separate non-persisted self-hashed stdout record so the original receipt and
public output bytes remain immutable.

Python remains available only through an explicit oracle/test runtime. Normal
launch and restart must have no silent Python fallback and no per-candidate
Python/Rust RPC.

## Admission order

1. Freeze diverse real Python oracle entries from the stopped 1,216-candidate
   corpus plus small hermetic fixtures.
2. Define a versioned Rust request/result/receipt authority.
3. Admit exact immigrant construction first at 1/4/16/64/128 widths.
4. Extend the same engine to every later-generation proposal family.
5. Prove exact seed, disposition, semantic, identity, accepted ordering,
   restart, and corruption parity against Python. The new compact production
   journal has its own canonical bytes; byte parity applies to bounded legacy
   projections and downstream public artifacts, not to intentionally removed
   repeated rich payloads.
6. Benchmark 1,024 and then 4,000 candidates for wall time, CPU, RSS, I/O, and
   diversity distribution.
7. Independently review the integrated cutover.
8. Hard-disable production Python proposal construction, commit/push, and
   launch a new five-generation campaign from a new root.

## RAM guard policy

Do not simply remove the host-wide RAM guard during implementation. After the
Rust proposal path and the already-Rust G0 funnel are admitted, measure every
remaining local generation and consolidation phase. If all AutoResearch-owned
phases are bounded, replace the blunt host-wide stop with process-tree-specific
hard limits or disable it for the canonical campaign. External workloads must
not falsely stop a low-memory QD process.

## Release gates

- Production construction must start no Python or Dashboard validator/compiler
  child process and must have no fallback branch.
- Worker-cap 1 and worker-cap 8 must produce identical canonical proposal,
  disposition, accepted-order, identity, and selection outputs.
- Bounded oracle gates at 1, 4, 16, 64, and 128 proposals must cover accepted,
  rejected, duplicate, mutation, crossover, no-op, crash/restart, corrupt
  object, stale authority, and path-alias cases.
- The compact 4,000-proposal authority/journal/object tree must remain below
  1 GiB; 512 MiB is preferred. The 4,000 pool must never be expanded into the
  legacy rich format as an intermediate transaction.
- Only selected evaluation candidates may be projected to legacy-rich worker
  payloads, and those projections must be identity-bound in the native receipt.
- Target envelopes are at most three minutes for 1,024 later-generation
  proposals and six minutes for 4,000 initial proposals, with 1 GiB target and
  2 GiB hard process-tree RSS. Any miss requires evidence and explicit review,
  not a weakened gate.
- Sealed adoption should complete within 20 seconds without reconstructing or
  rereading the proposal corpus.

## In-progress checkpoint: topology identity correction

During the Rust parity port, the current Python factory exposed a genuine
representation-invariance defect rather than a native mismatch. An event-bearing
setup node was authored with resource uses in ``[evidence_group, event]`` order,
while its canonical codec sorted those same uses to ``[event, evidence_group]``.
``semantic_topology_signature`` then replaced the canonical resource rows with
their original insertion-order kinds. The same canonical genome therefore had
lineage topology ``d16635…ee06`` before serialization and audit topology
``661951…21c2`` after decoding.

Fresh construction now sorts resource-kind multiplicity before hashing. The
corrected fresh pair identity is
``sha256:b9408a9b4ac3dcafeb32e3ece44383ef3662e0c53b28acd11b8f723d3bb6b64a``;
its program, compiled profile, native program, native validation report, and
execution semantics are unchanged. Historical artifacts retain an explicit
legacy projection for their literal ``d16635…`` lineage, but fresh Rust and
Python-oracle construction must never reproduce that split.

Current bounded gates at this checkpoint are:

- Python genome/topology tests: 12 passed, including codec-round-trip and
  resource-order permutation invariance.
- Real Python operator oracle: 8 passed; corpus
  ``sha256:0086d090124a3695d7dd2662a9b46323e5987bf2a91efea44d5ac12064c238db``.
- Rust v5 core: 10 passed, including the corrected fresh-pair golden and
  historical projection.
- Rust operator surface: 18 passed before the regenerated transcript gate;
  resource/temporal/hold/protection/topology/crossover implementations are
  present, but the integrated transaction is not yet admitted.
- Rust batch adoption: typed ordered attempt/accepted-record replay is present;
  full ledger/object/G0/evaluation semantic replay and fresh construction remain
  intentionally fail-closed.

The next seam is one native transaction joining the existing Rust proposal
scheduler and direction-aware parent selector to the v5 operator/compiler,
durable all-attempt journal, compact accepted records, G0/archive selection,
and selected-only evaluation handoff. Production cutover is still disabled.

## Post-reboot checkpoint: compact G0 transaction and complete operator oracle

The Codex desktop restart terminated the in-memory agents but did not lose any
workspace state. Four bounded Terra-max lanes were reconstituted without
duplicating file ownership: G0 core transaction, qd-batch bridge/publication,
later-generation operators, and oracle extension. The oracle lane has since
finished; the other three remain active. No campaign or wide corpus benchmark
has been launched.

The write-neutral Rust G0 transaction now exists in
``qd-kernel/src/v5_transaction.rs`` and compiles at its focused checkpoint. It
parses the sealed v5 authority once, reuses ``ProposalPlanner`` and the existing
identity-ledger seam, constructs candidates with a bounded 1..=8 worker cap,
and merges every state/ledger transition in proposal-ordinal order. It emits:

- a durable every-attempt journal and content-addressed outcome audits;
- an ordered compact accepted-journal semantic root;
- compact identity-ledger and schedule/state receipts;
- real content-addressed accepted-record references at
  ``v5-native/objects/sha256/<record-sha>.json``;
- the existing deterministic G0 accepted pool, selection, and campaign ledger;
- an ordered selected-projection index.

Replay reconstructs each accepted record from its exact authority and compact
delta before admitting any stored identity. A duplicate-retry fixture proves a
rejected proposal does not consume an accepted birth ordinal; a ceiling fixture
proves exact max-attempt termination. Cap 1 and cap 8 produce identical kernel
semantic artifacts. The cap is control-plane telemetry, not candidate or G0
semantic identity; the outer batch manifest/telemetry may still report it.

The qd-batch bridge has a prepublication receipt-last transaction boundary. It
authenticates artifact paths, byte lengths, file hashes, semantic roots,
authority, compact journals, selected projections, and outcome-audit objects
before linking the first public artifact. Publication is being converted from
test-scale byte vectors to file-backed streaming so ``population.json`` is
never retained in RAM. Private staging must live outside all receipt-owned
public namespaces and outside the exact invocation root; otherwise a power loss
after a hard link but before temporary cleanup could leave an extra file that
strands an otherwise valid receipt. A crash-after-receipt/before-result path
must authenticate the sealed tree and reconstruct only the tiny invocation
result, never candidates.

The corrected Python operator corpus is now
``sha256:c362bac1a60a879b677a92934a664bb19d4f04d55a64206a21eed9471ba6c96e``.
It contains real distinct-parent production witnesses for all three compatible
crossover ports (``entry_setup``, ``management_hub``, and ``exit_hub``), plus a
real terminal ``operation_rejected`` transcript. The focused oracle suite is
9 passed. Rust now reproduces all 152 exact legacy Python choice wrappers in
their exact ordering/hash; the focused operator suite is 20 passed. Remaining
operator gates are fixed-seed selection receipts, depth-1/2/3 recompile and
pair-identity progression, the three crossover children, and terminal
rejection replay.

The immediate remaining seam is selected-only native materialization:
``(authority, selected projection, compact delta, compact record)`` must first
replay the compact authority chain, then yield the rich evaluation candidate
and streamed population row expected by existing worker/publication contracts.
Only after that seam and full batch semantic replay are green should the same
scheduler be joined to the evolved mutation/crossover engine. Production fresh
construction remains deliberately fail-closed until both paths are complete.

### Post-reboot implementation update

The compact G0 transaction now exposes a selected-only materializer. It first
replays the selected projection against the exact compact record, delta, and
sealed authority, then reconstructs one rich evaluation candidate and one
self-hashed publication row. It is deliberately absent from the population-
sized transaction result: qd-batch must invoke it one selected ordinal at a
time, stream the canonical bytes into its private file-backed sink, and drop
the rich value before materializing the next row.

The batch publisher's former in-memory ``Vec<u8>`` path is now test-only.
Production can use only the file-backed staging/link/hash path. Staging lives
in a manifest-bound private same-volume directory outside every sealed public
namespace and outside the exact invocation root. The crash-after-receipt,
before-staging-cleanup adoption test is green, and unknown private stale files
are never swept.

The original unshipped v5 bridge incorrectly reused its outer manifest hash as
the publication request identity. That hash included ``threadCap`` and could
therefore make cap 1 and cap 8 produce different scientific identities. The
older compatibility request projection also omits the direction-aware v5
archive-policy authority. The replacement is a versioned, self-hashed,
cap-free v5 publication plan and
``temporal_qd_v5_g0_publication_request_v1`` semantic request identity. It
binds the complete v5 publication authority but excludes thread count, output
path, process timing, and other execution-only telemetry. The outer manifest
remains an invocation identity only.

Later-generation mutation parity has advanced beyond the earlier checkpoint:
Rust now matches all 152 exact operator wrappers, all four fixed-seed selection
receipts, and the real accepted depth-1/2/3 sequences. Every accepted child is
recompiled and re-identified before the next depth, and stale compiled-profile
reuse is rejected. The remaining operator oracle gates are the three real
distinct-parent crossover transcripts and the real terminal-rejection case.

Production construction remains fail-closed. The next integrated boundary is
a sealed ``v5_publication`` authority using the existing qd-kernel publication
schemas: qd-kernel owns semantic construction and validation, while qd-batch
owns only file-backed durable I/O and receipt-last publication. No parallel
batch-owned population/evaluation/generation schema is authorized.
