# Read-only performance and complexity audit

**Temporal QD v5 — `EnviralDesign/fuzzfolio-autoresearch`**
**Branch:** `codex/rust-native-v5-temporal-qd`
**Pinned source:** `e9b0f8297124f7db03eae8f9bef9cf04f6637697`

## Scope and evidence boundary

I inspected the immutable GitHub source snapshot at the specified commit. I did not modify files, build binaries, run the pipeline, access the active Windows run directory, or interact with Vast or the lake. The exact timings from the active run therefore come from the observed facts you supplied; substage timings below are explicitly labeled as modeled estimates until G1 instrumentation exists.

The pinned commit itself confirms that a full 1,024-member native prefinalizer can take roughly half an hour, and that gateway result retrieval was reduced to one candidate-window result per response because a result may approach the response-size ceiling.

## Executive judgment

The current pipeline is **not primarily memory-bound and probably not primarily SHA-256-throughput-bound**. It is dominated by four things:

1. **Fresh, trusted, same-process output is repeatedly treated as hostile imported evidence.** G0 construction is deeply replayed during transaction construction, replayed again by `qd-batch`, selected records are reconstructed again for publication, and the staged closure is replayed yet again before the first public link.
2. **The proposal is exploded into thousands of individually durable content-addressed files.** The code directly creates one outcome-audit object per attempt, up to one proposal-delta object per attempt, and one accepted-record object per accepted proposal, before the singleton ledgers, indexes, receipts, and four public streams. Each staged object is individually written, synchronized, hashed, validated, linked, and often revalidated.
3. **Restart “adoption” is actually a complete forensic audit of a hostile filesystem tree**, including canonical JSON validation, row self-hashes, every object’s bytes and path identity, a recursive exact-tree walk, reparsing and rehashing immutable objects, typed replay, and public-stream verification. It is not checkpoint adoption in the ordinary sense.
4. **The rotating prefinalizer contains a confirmed accidental quadratic join.** For every bundle, it re-reads, rehashes, reparses, and re-canonicalizes the entire evaluated-members file, then linearly searches it for one candidate. This likely explains most of the documented half-hour prefinalizer.

The strong safety model is defensible as an optional evidence profile. Its current implementation is not. Strict mode can retain complete byte-level reproducibility, adversarial validation, crash safety, and restart closure while replacing thousands of loose files and repeated checks with a packed transaction, one sequential authentication pass, and a Merkle/index closure.

A realistic optimized target—not a measured result—is:

| Profile          | Proposal construction/publication | Cold proposal adoption | Prefinalizer |
| ---------------- | --------------------------------: | ---------------------: | -----------: |
| Current          |                          ~100 min |                ~25 min |   ~25–35 min |
| Optimized strict |                        ~40–65 min |               ~3–8 min |     ~1–5 min |
| Durable-local    |                        ~25–45 min |             ~0.5–2 min |     ~1–5 min |
| Research-fast    |                        ~15–35 min |             ~15–60 sec |     ~1–5 min |

Those ranges are deliberately broad and non-additive. G1 instrumentation is needed before treating them as commitments.

---

# A–B. Stage-by-stage wall-time model and work classification

Notation:

* **N = 4,000** construction proposals
* **S = 1,024** evaluation candidates
* **W = 4** replay windows
* **T = S × W = 4,096** replay tasks
* **F = 16,142** observed proposal files
* **B ≈ 783 MiB** observed proposal bytes

Classification codes:

* **SCI** — scientifically necessary computation
* **REP** — reproducibility-critical
* **DUR** — restart/durability-critical
* **ADV** — adversarial-input/security hardening
* **RED** — redundant validation/bookkeeping
* **LEG** — legacy/transport compatibility
* **FS** — avoidable serialization/filesystem overhead

### One-generation critical-path model

| Stage                                                                           |                                 Current wall-time model | Classification              | Audit judgment                                                                                                                                     |
| ------------------------------------------------------------------------------- | ------------------------------------------------------: | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Invocation, source/config and shared-authority validation                       |                                  **0.5–2 min, modeled** | REP, ADV                    | Keep exact source/config/data identities. Validate them once at ingress, not repeatedly inside the same trusted process.                           |
| Construct, compile, validate and ordinally admit 4,000 proposals                |                                  **20–45 min, modeled** | SCI, REP                    | Real scientific work. Current utilization is constrained by an eight-thread cap, repeated OS-thread creation, and serial admission.                |
| Fresh transaction replay and accepted-record reconstruction                     |                                  **15–30 min, modeled** | REP once; RED when repeated | One deterministic replay is useful in strict mode. Multiple same-process replays are redundant.                                                    |
| Per-object materialization, canonical serialization, hashing and inventory      |                                  **20–35 min, modeled** | DUR, ADV, FS                | Logical evidence may be required; thousands of loose files and per-object durability barriers are not.                                             |
| Public pair/population/evaluation/journal staging, verification and publication |                                   **8–20 min, modeled** | REP, DUR, ADV, RED          | Four logical artifacts are reasonable. Their multiple complete re-reads and rehashes are not.                                                      |
| **Combined proposal construction/publication/seal**                             |                                  **~100 min, observed** | Mixed                       | The modeled components above are calibrated to the observed total; they are not separately measured.                                               |
| Proposal seal adoption after restart                                            |                                   **~25 min, observed** | DUR, ADV, RED               | Full hostile-tree audit. Effective cost is approximately **93 ms per file**, despite an average file size of only about **50 KiB**.                |
| Campaign freeze and 4,096-task matrix generation                                |                                    **1–5 min, modeled** | SCI, REP, FS                | Mostly deterministic task construction and duplicated profile serialization. Probably not a primary bottleneck.                                    |
| Gateway enqueue/startup                                                         |                                   **<1–3 min, modeled** | REP, DUR                    | Small relative to replay. Resume validation and task inventory matter operationally.                                                               |
| Remote replay and local durable result drain                                    |   **71.9 min uninterrupted, derived from 57 tasks/min** | SCI, REP, DUR               | This is the dominant necessary compute stage, but it does not explain an overnight run by itself.                                                  |
| Campaign source build                                                           |                                    **1–5 min, modeled** | REP, LEG, RED               | Primarily a format/authority bridge between freezer, gateway, and campaign seal. It can be folded into a compact campaign-input/output checkpoint. |
| Campaign seal and raw-result admission                                          |                                   **3–15 min, modeled** | REP, DUR, ADV               | Fresh mode intentionally reads each raw result once. This is a sensible trust boundary; retain it or move validation earlier into the dispatcher.  |
| Tail reduction                                                                  | **~3.2 sec for 1,024 candidates, repository benchmark** | SCI, REP                    | Already fast. Do not prioritize it.                                                                                                                |
| Panel sidecar/bundle production                                                 |                                    **1–5 min, modeled** | REP, some RED               | O(S) work is reasonable; separate receipt choreography may not be.                                                                                 |
| Rotating prefinalizer and funnel preparation                                    |                                  **~25–35 min current** | SCI, REP, severe RED        | Confirmed O(S²) implementation defect. Should fall to O(S) without changing outputs.                                                               |
| Generation finalizer, commit and state application                              |                                  **0.5–3 min, modeled** | REP, DUR, ADV               | A compact atomic generation commit is warranted. Multiple independently authenticated handoffs are excessive.                                      |
| State-application sidecar restart reconciliation                                |                     **seconds to low minutes, modeled** | DUR, REP                    | The old/pending/applied state distinction is useful. It should be embedded in the generation commit rather than another large validation surface.  |

The campaign freezer streams the task matrix, but each task embeds a cloned inline profile snapshot and is separately pretty-serialized and canonicalized for the task-array identity. That makes its cost proportional not just to 4,096 rows but also to repeated profile size.

The campaign seal has a better restart design than proposal adoption: a fresh execution reads every raw result exactly once, commits compact indexes and a seal, and a later current-runtime restart can authenticate the fixed execution receipt without revisiting raw blobs. That behavior is worth preserving.

### Modeled uninterrupted generation total

Using central estimates:

* Proposal: **100 min**
* Freeze/dispatch setup: **~4 min**
* Replay: **~72 min**
* Source build/seal/panels: **~8 min**
* Prefinalizer: **~30 min**
* Finalizer/state: **~2 min**

That gives approximately **216 minutes, or 3 hours 36 minutes**, without maintenance and without a restart. One proposal-adoption restart raises it to roughly **4 hours**.

The maintenance incident, not healthy replay, is therefore what converted a several-hour pipeline into an overnight one.

---

# C. Why construction and sealing took approximately 100 minutes

## 1. Fresh construction is replayed too many times

The construction kernel already ends by verifying the completed result through `verify_replay`. `qd-batch` then invokes another deep G0 replay before publication. Later, staged publication validation invokes typed replay again before the first public artifact is linked.

This matters because replay is not a cheap checksum. It reconstructs accepted compact records from proposal deltas, validates journals and identities, and rebuilds the typed transaction closure.

**Recommendation:** retain exactly one of these in strict mode:

* Either verify the typed transaction before serialization, or
* Verify the final serialized checkpoint from a clean reader.

Doing both several times in one process provides little additional protection. Research-fast should skip fresh deep replay entirely and reserve it for an explicit audit command.

## 2. Selected records are reconstructed and validated repeatedly

The fresh path carries enough typed information to publish selected candidates, but then reconstructs selected material from compact records and deltas for publication. The adoption path reconstructs those records again. This converts “compact durable evidence” into a repeated compilation/validation workload.

The reproducibility requirement is that compact evidence deterministically reconstructs the selected candidate. It does not require proving that fact at every intermediate call boundary.

**Recommendation:** cache the reconstructed accepted record or publication projection during the fresh transaction. Strict mode can perform one independent reconstruction from persisted bytes at the final checkpoint. Fast mode can retain rich selected rows directly.

## 3. The construction pool is artificially narrow and expensive to schedule

The construction cap is hard-bounded to eight workers. `construct_batch` creates a new scoped set of OS threads for each batch rather than keeping a persistent worker pool alive. At an effective cap of eight and 4,000 proposals, that is approximately:

* **500 scoped batches**
* **4,000 thread spawns**
* Serial ordinal merge and ledger admission after every batch

The deterministic ordinal commit is valuable. Recreating worker threads is not.

**Recommendation:** use one fixed Rayon or crossbeam worker pool. Workers produce ordinal-tagged results into a bounded buffer; one deterministic committer admits them in ordinal order. Benchmark 8, 16, 24 and 32 workers because the ideal cap may be limited by Dashboard compilation rather than core count.

## 4. The proposal is represented as thousands of separately durable objects

The G0 durable-object enum includes:

* `AttemptOutcomeAudit`
* `CompactProposalDelta`
* `CompactAcceptedRecord`
* Compact journals
* Identity and schedule-state ledgers
* Accepted pool
* Campaign ledger
* G0 selection
* Selected projection index
* Publication plan

The binding builder loops across outcome audits, proposal deltas, and accepted records and emits each as a separate content-addressed object.

At N=4,000, the first three families alone can produce approximately **12,000 objects** before singleton authorities and publication artifacts. The observed 16,142 files are therefore architecturally plausible. I cannot attribute the additional approximately 4,142 files exactly without the active artifact census.

Each staged object goes through some combination of:

1. Create private file
2. Write canonical bytes
3. Flush and `sync_all`
4. Compute a stable full-file digest
5. Reopen or re-digest for prepublication validation
6. Parse and re-canonicalize
7. Hard-link into the public closure
8. Verify the link target
9. Synchronize directory metadata
10. Repeat parts of the process during adoption

This is O(F) high-latency filesystem work, not merely O(B) streaming I/O.

## 5. The four public artifacts receive multiple complete byte passes

Staging a public artifact writes and synchronizes it, then hashes it. Opening it for a verifier first verifies it by digest. The verifier reads it. Closing is followed by another digest. Population and evaluation are verified individually, then all four streams are reopened and checked as a bundle, and staged-to-public linking adds source and destination verification.

Depending on the artifact and path, this is approximately **six to nine complete reads or digest passes after the initial write**.

A strict architecture needs:

* One canonical write
* One final logical validation
* One final cryptographic closure
* One atomic commit

It does not inherently need six to nine byte passes.

## 6. Restart verification repeats both byte-level and semantic work

Proposal adoption performs two broad phases that overlap:

* Inventory authentication: canonicalize inventory rows, validate row hashes, stat and hash each object, verify path identity.
* Object reconstruction: reopen objects, hash them again, parse and canonicalize them, build typed maps, replay the transaction, and reopen public streams.

It then recursively enumerates the entire output tree to reject extra files, non-regular entries, links, and reparse points.

That is appropriate when importing an evidence tree from an untrusted producer. It is excessive when reopening a local checkpoint that the same executable atomically committed moments earlier.

## 7. Raw SHA-256 bandwidth cannot explain the observed time

The observed tree averages approximately:

* **49.7 KiB per file**
* **200.4 KiB per proposal**

Even assuming a slow effective sequential hash rate of only 100 MiB/s, hashing the entire 783 MiB tree once would take approximately 7.8 seconds. Twenty complete sequential hash passes would be about 2.6 minutes.

The observed costs are:

* **~371.7 ms per file** across the 100-minute fresh path
* **~92.9 ms per file** across the 25-minute adoption path

That strongly implicates per-file creation, metadata, stable-identity checks, synchronization barriers, open/close churn, parsing/canonicalization, and reconstruction—not SHA-256 arithmetic alone.

NTFS and antivirus scanning may amplify each create/link/sync operation, but that is an unmeasured environmental amplifier, not the architectural root cause.

## 8. No primary construction O(N²) loop was found

The core construction path appears to be repeated O(N) work plus O(N log N) maps/sorts, not a clear O(N²) algorithm. Its problems are multiplicative:

* Several complete transaction replays
* Several serialization and hash passes
* Serial admission
* Repeated construction/reconstruction
* O(F) high-latency file operations

The confirmed accidental O(N²) path is downstream in prefinalization.

## 9. The prefinalizer has an actual quadratic bug

`all_bundles` initially loads evaluated members, then loops over bundles. Inside that loop it calls `receipt_rows` on evaluated members again. `receipt_rows` separately performs `file_hash` and `rows`, meaning two complete reads per invocation, and `rows` reparses and re-canonicalizes every row. It then performs a linear `.find()` for the current candidate.

With 1,024 bundles:

* Evaluated-members authentications: approximately **1,025**
* Complete file reads: approximately **2,050**
* Row parse/canonicalization operations: approximately **1,049,600**
* Candidate comparisons: roughly **0.52 million average**, up to **1.05 million worst-case**

If the evaluated-members file is E bytes, this path reads approximately **2,050 × E bytes**. A 25 MiB file would therefore create roughly 50 GiB of read traffic and JSON churn.

Strict mode does not require this. Load and authenticate the file once into a `BTreeMap<candidate_id, member>`, then perform O(log S) or O(1) lookups.

---

# D. Receipt and seal architecture quantified

## Proposal construction and publication

| Artifact/group                                                                   |                                       Approximate count at G0 |                     Approximate bytes | Current passes and durability operations                                                                                         | Complexity                              | Strict mode                                                   | Research-fast                                                            |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------: | ------------------------------------: | -------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Outcome-audit objects                                                            |                                                     N = 4,000 |              Unknown; part of 783 MiB | Canonical encode, individual write, file sync, hash, prepublication validation, link verification; rehashed/reparsed on adoption | O(N + bytes) with high per-file latency | Preserve logical rows, but pack them                          | Aggregate by reason; retain failures and selected lineage                |
| Proposal-delta objects                                                           |                                                       Up to N |                               Unknown | Same loose-object lifecycle                                                                                                      | O(N + bytes)                            | Preserve logical deltas in pack                               | Retain only accepted/selected deltas or a compact append stream          |
| Accepted-record objects                                                          | Up to accepted count; plausibly near N before 1,024 selection |                               Unknown | Same lifecycle plus deterministic reconstruction                                                                                 | O(N × reconstruction cost)              | Preserve all or all accepted in pack                          | Preserve selected 1,024 and optionally sampled rejected/overflow records |
| Singleton ledgers, journals, pool, selection, projection index, publication plan |                                                  Roughly 8–15 |                     Minority of total | Canonical write/hash plus receipt and inventory references                                                                       | O(N) for contained rows                 | Preserve                                                      | Collapse into one checkpoint header/index                                |
| Public pair/population/evaluation/journal streams                                |                                                             4 | Likely a substantial share of 783 MiB | Initial write plus approximately 6–9 complete validation/digest passes depending on stream                                       | O(B), repeated constant factor          | Preserve exact bytes; reduce to one final validation and hash | Preserve essential population/evaluation outputs; one hash               |
| Object inventory                                                                 |                                       One row per object/file |                                  O(F) | Canonical row validation, row self-hashes, path binding, full object digest; repeated on adoption                                | O(F log F + B)                          | One packed index/Merkle closure                               | One compact root manifest                                                |
| Proposal result/receipt/seal/adoption authority                                  |                                                    O(1) files |                                 Small | Self-hashes and cross-document closure checks                                                                                    | O(F) because they bind inventory        | Preserve one receipt-last commit                              | One non-audited checkpoint receipt                                       |
| **Observed complete proposal tree**                                              |                                              **16,142 files** |                          **~783 MiB** | Thousands of file syncs, links, directory operations and reopen validations                                                      | Dominated by F, not B                   | Replace loose CAS with chunked pack                           | Single pack or database/checkpoint                                       |

A strict packed form could reduce the file count from 16,142 to roughly:

* One checkpoint header
* One or several append-only data packs
* One index
* One receipt-last commit marker

For example, 128-proposal chunks would yield about 32 proposal chunks rather than more than 12,000 individually synchronized per-proposal files. A strict verifier could still authenticate every logical record and byte.

## Evaluation and finalization

| Artifact/group                                                   |                               Approximate count | Full-data passes / hashes / syncs                                                                                                                         | Complexity                                              | Strict necessity                                       | Fast-mode treatment                                                      |
| ---------------------------------------------------------------- | ----------------------------------------------: | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------ |
| Campaign task manifest                                           | 1 large file plus roughly 5–10 compact controls | One streaming write/hash, then inventory/receipt authentication; each of 4,096 tasks contains an inline cloned profile                                    | O(T × profile size)                                     | Deterministic task identity is necessary               | Deduplicate profiles into a candidate table and reference by ID          |
| Gateway raw result files                                         |       T = 4,096 plus journal/checkpoint/receipt | Per result: validate, deterministic-gzip encode, file `sync_all`, reopen/inflate/re-encode verification, journal append and `sync_data`, then acknowledge | O(T + result bytes), at least about 8,192 sync barriers | Durable acknowledgement and task binding are necessary | Chunk results and synchronize every N results or every few seconds       |
| Campaign source                                                  |                         O(1) source and receipt | Rebinds task manifest, freezer receipt, gateway root and gateway receipt                                                                                  | O(T) in source construction                             | Logical relationship is necessary                      | Make it a view inside campaign-output checkpoint                         |
| Campaign seal/index/inventory/tail transaction/execution receipt |                         Roughly 6–8 fixed files | Fresh path reads each raw result once, then writes index, inventory, seal and transaction through synchronized receipt-last publication                   | O(T + result bytes)                                     | Good strict trust boundary                             | Keep schema/task binding; collapse files into campaign-output checkpoint |
| Tail reduction                                                   |                             Few compact outputs | One bounded reduction over compact index                                                                                                                  | O(S)                                                    | Necessary                                              | Keep unchanged                                                           |
| Panel bundles and receipt                                        |                     O(S) logical rows/artifacts | Candidate-level bundle creation and receipt validation                                                                                                    | O(S)                                                    | Panel semantics necessary                              | Pack bundles; one receipt                                                |
| Evaluated-members file in current prefinalizer                   |                                          1 file | Approximately 2,050 complete reads at S=1,024, including about 1,025 full hashes and 1,025 parse/canonicalization passes                                  | **O(S² × row size)**                                    | Authenticate once, not per bundle                      | Same O(S) implementation                                                 |
| Prefinalizer base/resume transactions and funnel outputs         |                       Several compact artifacts | Exact schemas, self-hashes, chained restart closure                                                                                                       | Nominally O(S); currently dominated by quadratic join   | One atomic finalization checkpoint is enough           | One checkpoint and optional resume journal                               |
| Generation finalizer, generation commit and state sidecar        |                           Several compact files | Hash/path/schema closure and atomic state transition checks                                                                                               | O(number of final artifacts)                            | Generation/state atomicity necessary                   | Collapse into one commit plus one previous-state pointer                 |

The dispatcher’s durability design is strong but expensive: it writes each result as deterministic gzip, synchronizes it, reopens and semantically verifies it, then synchronizes the journal before acknowledgement.

---

# E. Recommended operating profiles

## 1. `strict_audited_v1`

Purpose: publishable evidence, adversarial imports, final campaign certification.

Preserve:

* Deterministic seed schedule and proposal ordinals
* Exact source commit, source/config/data identities
* Full logical attempt, accepted-record and selection evidence
* Byte-stable canonical artifacts
* Complete remote-result task binding and semantic validation
* Full restart closure
* Power-loss-safe atomic publication
* Exact generation-state transition
* Adversarial path and symlink checks at genuine external trust boundaries

Change the implementation:

* Store logical objects in one or several immutable packs with a deterministic index and Merkle/root hash.
* Authenticate the pack sequentially once rather than authenticating 16,000 paths.
* Perform one typed replay from final persisted bytes, not several same-process replays.
* Use chunk-level synchronization and one final atomic rename.
* Apply path/reparse checks to external roots and the pack itself, not every logical record.
* Cache canonical bytes and SHA descriptors after immutable write.
* Fix the prefinalizer to O(S).
* Use a persistent deterministic construction pool.

This profile can preserve stronger effective durability than the current loose tree because the transaction is either absent or atomically complete, rather than having thousands of independently committed objects whose closure must later be reconstructed.

## 2. `durable_local_v1` — recommended exploratory default

Purpose: normal data-science iteration where the machine and output root are trusted, but crashes and resumability still matter.

Preserve:

* Everything that determines the scientific result
* All selected candidates and their lineages
* Deterministic seeds and ordinal schedule
* Source/config/data identities
* Task identities and complete evaluation outputs
* Aggregate rejection/admission statistics
* State transition and generation record
* Crash-safe checkpoints every configurable number of proposals/results
* Full validation of remote worker results

Relax:

* No per-object path identity or exact-tree walk inside an owned local root
* No repeated same-process canonical-byte verification
* No per-object `fsync`
* No loose content-addressed object tree
* Deep proposal replay only on restart or explicit audit
* Attempt evidence packed into chunked JSONL/binary records rather than separate files

This profile should still be reproducible from its seed, source, configuration and data authorities. It is not adversarially certified.

## 3. `research_fast_v1`

Purpose: rapid hypothesis iteration and parameter exploration.

Preserve:

* Deterministic seeds and proposal ordinals
* Exact source commit and immutable config/data identities
* Selected 1,024 candidates and their rich material
* Evaluation task identities and essential replay results
* Selection/tail/final state outputs
* Aggregate rejection reason counts and representative failure samples
* Coarse crash-safe checkpoints
* Strict remote-result schema, task-binding and numerical validation

Disable or collapse:

* Per-attempt content-addressed files
* Full evidence for every unselected accepted proposal
* Fresh deep replay
* Same-process reopen/hash/canonicalization
* Exact local path inventories and hostile-tree checks
* Per-file syncs and directory syncs
* Separate proposal result, transaction, publication seal and adoption receipts
* Separate campaign source-build transaction
* Fine-grained gateway result journal syncs

Mandatory marking:

```json
{
  "evidenceProfile": "research_fast_v1",
  "audited": false,
  "strictEvidenceCompatible": false,
  "promotionPolicy": "rerun_from_seed_under_strict_audited_v1"
}
```

A strict finalizer must reject this profile. A promising result is promoted by rerunning it under strict mode from the same immutable source/config/data identities and deterministic seed. It must never be retroactively relabeled as strict evidence.

---

# F. Stages that should be collapsed

## Proposal publication + seal + adoption

These should become one **proposal-generation checkpoint**.

Recommended contents:

* Source/config/shared-authority identities
* Generation index and evidence profile
* Deterministic seed/ordinal schedule
* Attempt summary or packed attempt records
* Accepted/selected records
* Population and evaluation rows
* Selection and ledger state
* State transition input
* Deterministic index and root hash

Commit protocol:

1. Write private checkpoint pack or chunk set.
2. Validate the logical transaction once.
3. Compute final root.
4. Synchronize checkpoint.
5. Atomically rename/link into place.
6. Synchronize parent directory.
7. Write one receipt-last marker.

The process that created the checkpoint should continue using its in-memory typed result. It should not immediately “adopt” its own just-written checkpoint. Adoption only occurs after process restart or explicit audit.

## Campaign freeze + source build + seal + tail authority

These cannot safely be one atomic transaction because remote replay lies between their input and output halves. They should become **two**, not one:

### `campaign_input_checkpoint`

* Frozen selected population
* Cohort/window selection
* Task matrix
* Source/config/runtime identities
* Gateway execution plan

### `campaign_output_checkpoint`

* Gateway execution receipt
* Result inventory/root
* Evaluated members
* Campaign seal
* Panel bundle index/receipt
* Tail index and authority
* Task accounting

The current campaign-source-build artifact becomes an internal view joining those two checkpoints rather than another independently authenticated transaction.

## Prefinalizer base/resume + funnel assembly + finalizer

These should become one **generation-finalization checkpoint** with an optional append-only resume journal.

The base/resume distinction is useful while work is incomplete, but the committed state should expose only:

* Final selected/provisional/panel state
* Funnel result
* Generation record
* State patch
* Previous-state hash
* New-state hash
* One receipt-last generation commit

## Durable-boundary reduction

The present pipeline has approximately fifteen named authenticated handoffs. A clearer model would have four major per-generation boundaries:

1. Proposal checkpoint
2. Campaign-input checkpoint
3. Campaign-output checkpoint
4. Generation commit

Strict subreceipts can be embedded as records inside those checkpoints without becoming separate filesystem-level transactions.

---

# G. Configuration and feature boundaries

Do not fork the scientific implementation. Put evidence behavior behind a single runtime policy object carried in every manifest and receipt.

| Policy dimension          | Strict audited                           | Durable local                  | Research fast                 |
| ------------------------- | ---------------------------------------- | ------------------------------ | ----------------------------- |
| `evidenceProfile`         | `strict_audited_v1`                      | `durable_local_v1`             | `research_fast_v1`            |
| `artifactLayout`          | `chunked_pack_merkle_v1`                 | `chunked_pack_v1`              | `checkpoint_pack_v1`          |
| `attemptEvidence`         | `all`                                    | `all_packed`                   | `selected_plus_failures`      |
| `freshReplayVerification` | `once_from_persisted_checkpoint`         | `off_or_on_demand`             | `off`                         |
| `restartVerification`     | `full_pack_and_semantic`                 | `root_plus_required_semantics` | `checkpoint_root`             |
| `canonicalByteChecks`     | `external_ingress_and_final_commit`      | `final_commit`                 | `identity_bearing_outputs`    |
| `pathIdentityChecks`      | `external_roots_and_checkpoint`          | `external_inputs_only`         | `external_inputs_only`        |
| `treeInventory`           | `pack_index_and_root`                    | `pack_index`                   | `essential_artifact manifest` |
| `fsyncPolicy`             | `chunk_and_transaction`                  | `checkpoint`                   | `checkpoint`                  |
| `gatewayResultDurability` | `chunk_before_ack` or current per-result | `batch_or_interval`            | `batch_or_interval`           |
| `receiptGranularity`      | Four major checkpoints                   | Four major checkpoints         | Four major checkpoints        |
| `strictFinalizerAllowed`  | Yes                                      | No                             | No                            |

Implementation seams should be shared traits or services, for example:

* `EvidenceRecorder`
* `ArtifactSink`
* `DurabilityPolicy`
* `VerificationPolicy`
* `CheckpointReader`
* `ResultAdmissionPolicy`

Compile-time Cargo features should only control optional implementations or dependencies. The evidence profile must remain a runtime, identity-bearing field so a binary cannot silently emit fast evidence under a strict-looking schema.

---

# H. Gateway retry and maintenance audit

## Current behavior

The native dispatcher does not model lake maintenance as a distinct state. Non-200 HTTP responses, including 409 and 503, become errors. The main loop polls at a fixed interval and continues normal dispatch/resume logic.

The current result path is intentionally conservative:

* One result per response
* Validate task/result binding
* Encode deterministic gzip
* Synchronize result file
* Reopen, inflate and re-encode for verification
* Append and synchronize completion journal
* Acknowledge

This prevents result loss, but it cannot prevent a fleet-wide retry storm when the shared lake is unavailable.

The observed incident produced:

* **4,591 requeues**, about **1.12 requeues per generation task**
* **304 circuit-breaker activations**
* Long task-level cooldowns
* No permanent failures

This is safe but operationally wrong. A shared dependency outage should create one shared pause, not thousands of task-level state transitions.

## Recommended shared maintenance state machine

```text
HEALTHY
  -> DRAINING_FOR_MAINTENANCE
  -> PAUSED_MAINTENANCE
  -> PROBING
  -> RAMPING
  -> HEALTHY
```

### Scheduled maintenance

The supervisor should ingest the Unraid maintenance schedule or a maintenance sentinel exposed by the lake.

Before the window:

* Stop claiming new tasks.
* Stop enqueueing additional work.
* Allow short in-flight operations either to complete or park.
* Persist one campaign-level pause record.
* Do not consume task attempt budgets.

### Unscheduled 409/503 detection

Use one breaker keyed by the shared lake authority or endpoint.

Suggested trigger:

* A small quorum of independent workers reports maintenance-class 409/503 responses within a short interval, or
* A supervisor health probe fails with the same maintenance signature.

On open:

* Stop all task claiming and requeue mutation.
* Leave unfinished tasks pending without incrementing attempts.
* Suppress worker-level cooldown records.
* Persist `pauseReason`, first/last failure, affected authority, and current task accounting.

### Recovery probe

Use one supervisor-owned positive probe, not 264 worker probes.

The probe should perform a representative semantic lake read—not merely a TCP or generic HTTP health response. Resume after several consecutive successful probes.

Ramp gradually:

* 16 slots
* 64 slots
* Full 264 slots

This avoids a thundering herd immediately after maintenance.

### Vast scaling

At the observed fleet price:

* 8 idle hours cost **$6.91**
* 10 idle hours cost **$8.64**
* 12 idle hours cost **$10.37**
* A healthy uninterrupted 4,096-task replay at 57/min costs only about **$1.03**

For a scheduled or clearly extended maintenance window, the fleet should be paused or scaled down after draining. Retain at most a small sentinel capacity if rebuilding the fleet is costly.

### Attempt-budget semantics

Maintain separate counters:

* `scientificAttempt`
* `workerExecutionAttempt`
* `infrastructureAvailabilityEvent`

Lake maintenance should increment only the third. It must not consume per-task scientific or worker retry budgets.

### Result durability by profile

Strict mode can retain acknowledgement only after durable persistence, but it should persist results in small chunks rather than synchronizing both a result file and journal for every task.

Durable-local and research-fast can:

* Append results to a pack
* Synchronize every 16–64 results or every 2–5 seconds
* Write a chunk commit record
* Acknowledge only committed chunks

This keeps bounded possible replay after a process crash without thousands of synchronization barriers.

---

# I. Ranked optimization plan

Expected reductions below are directional and non-additive.

## 0. Add phase and filesystem instrumentation

**Expected reduction:** None directly; prevents optimizing the wrong component.
**Effort:** Low to medium.
**Tradeoff:** Minimal; avoid per-row logging that changes timings.
**Files:** All major crates, especially `qd-batch`, `qd-kernel`, `qd-gateway-dispatch`, `qd-campaign-seal`, `qd-rotating-prefinalizer`, `qd-generation-finalizer`.
**Proof:** G1 emits complete stage totals that reconcile to process wall time within 1%.
**Safe before next large run:** **Yes.**

## 1. Fix the prefinalizer O(S²) evaluated-members reload

**Expected reduction:** Approximately **25–30 minutes per generation**, likely taking prefinalization from roughly half an hour to low single-digit minutes.
**Effort:** Low.
**Tradeoff:** None. It is a pure implementation correction.
**Files:** `rust/temporal-qd/crates/qd-rotating-prefinalizer/src/v5.rs`, particularly `all_bundles`, `receipt_rows`, `rows`, and `file_hash`.
**Implementation:** Authenticate evaluated members once, build a candidate-ID map, reuse it for every bundle.
**Proof:** Byte-identical prefinalizer outputs for the same manifest; `evaluated_members.read_count` falls from approximately 2,050 to 2.
**Safe before next large run:** **Yes, after an output-equivalence test.**

## 2. Add the shared lake-maintenance gate

**Expected reduction:** Outage-dependent; potentially **hours** and $0.864 per paused fleet-hour.
**Effort:** Medium.
**Tradeoff:** None scientifically; improves attempt accounting.
**Files:** `qd-gateway-dispatch/src/lib.rs`, supervisor/control-plane gateway orchestration, and the actual gateway/lake service if it is outside this repository.
**Proof:** Mock 409/503 outage followed by recovery: one breaker activation, zero task-budget consumption, no mass requeues, exact task completion after resume.
**Safe before next large run:** **Yes, with fault-injection testing.**

## 3. Replace ephemeral eight-thread batches with a persistent deterministic pool

**Expected reduction:** **5–20 minutes** in proposal construction, depending on host cores and compile-service concurrency.
**Effort:** Medium.
**Tradeoff:** None if ordinal admission remains deterministic.
**Files:** `qd-kernel/src/v5_transaction.rs`; possibly `qd-runtime` transport limits.
**Proof:** Exact proposal/selection/state identities at worker caps 8, 16, 24 and 32; measure active-worker utilization and serial-commit fraction.
**Safe before next large run:** **Likely yes.**

## 4. Eliminate duplicate fresh same-process replay

**Expected reduction:** **15–35 minutes** from the current proposal path.
**Effort:** Medium.
**Tradeoff:** Research-fast loses immediate deep self-audit; strict retains one final persisted-byte replay.
**Files:** `qd-kernel/src/v5_transaction.rs`, `qd-batch/src/main.rs`, `qd-kernel/src/v5.rs`.
**Proof:** Strict mode performs exactly one persisted-byte replay and produces existing semantic/byte identities; fast mode produces identical scientific population, evaluation, selection and state identities.
**Safe before next large run:** **Yes for an explicitly non-audited profile; strict change requires equivalence tests.**

## 5. Cache immutable canonical bytes and hash descriptors

**Expected reduction:** **5–15 minutes fresh**, potentially another **5–10 minutes on adoption**.
**Effort:** Medium.
**Tradeoff:** Must prevent mutation after descriptor creation.
**Files:** `qd-batch/src/main.rs`, `qd-contract`, staged-artifact abstractions.
**Implementation:** After immutable write, carry `{length, sha256, canonicalValidated, stableHandleIdentity}` rather than rehashing before every consumer.
**Proof:** Hash-call and hash-byte counters drop substantially while tamper tests continue to fail closed.
**Safe before next large run:** **Yes in durable-local/fast; strict after TOCTOU tests.**

## 6. Replace loose per-attempt CAS files with chunked packs

**Expected reduction:** **15–35 minutes fresh** and approximately **15–22 minutes from cold adoption**.
**Effort:** High.
**Tradeoff:** Mid-chunk crash may repeat a bounded number of proposals; logical evidence remains complete in strict mode.
**Files:** `qd-batch`, `qd-kernel` durable bindings, `qd-contract`, adoption verifier.
**Proof:** Full logical-record audit, deterministic pack bytes/root, fault injection after every chunk, strict replay from pack, no divergent state.
**Safe before next large run:** **Not as the first urgent patch unless the proof suite is completed; excellent next tranche.**

## 7. Collapse proposal transaction/publication/seal/adoption into one checkpoint

**Expected reduction:** Additional **5–15 minutes** and a major reduction in restart/control-plane complexity.
**Effort:** High.
**Tradeoff:** Requires a schema migration or aliases for pre-1.0 artifacts.
**Files:** `qd-batch`, control plane, proposal schemas and finalizer consumers.
**Proof:** Crash-state matrix for absent/private/committed checkpoint; no externally visible partial generation.
**Safe before next large run:** **Better after the pack abstraction exists.**

## 8. Deduplicate candidate profiles in the task matrix

**Expected reduction:** **1–5 minutes** of freeze/serialization time plus potentially substantial disk/network reduction.
**Effort:** Medium to high because worker protocol changes may be needed.
**Tradeoff:** Task envelope is no longer fully self-contained unless the profile table is included in the same authenticated campaign checkpoint.
**Files:** `qd-campaign-freeze/src/lib.rs`, worker/gateway task protocol, campaign seal source builder.
**Proof:** Same task identities and worker outputs from inline-profile and profile-reference forms.
**Safe before next large run:** **Probably not necessary before the more important fixes.**

## 9. Chunk gateway result persistence and journal commits

**Expected reduction:** **2–10 minutes** of local coordination overhead and materially faster resume scans.
**Effort:** Medium.
**Tradeoff:** A process crash may require replaying the last uncommitted chunk. Strict mode can keep chunk size small.
**Files:** `qd-gateway-dispatch/src/lib.rs`.
**Proof:** Kill-after-every-result fault test; completed task set remains exact and no task is acknowledged without a committed chunk.
**Safe before next large run:** **Yes in fast/durable profiles after fault testing.**

## 10. Collapse campaign and finalization receipts

**Expected reduction:** Probably **2–10 minutes**, but the larger benefit is operational simplicity.
**Effort:** High.
**Tradeoff:** Fewer independently inspectable intermediate receipts; equivalent subrecords can remain embedded in checkpoints.
**Files:** `qd-campaign-freeze`, `qd-campaign-seal`, `qd-generation-finalizer`, `temporal_qd_v5_control_plane.py`.
**Proof:** Restart from every durable boundary; strict auditor can reconstruct the same logical evidence graph.
**Safe before next large run:** **Defer until after the high-confidence hotspots.**

### Best pre-next-run tranche

The safest high-value combination is:

1. Instrumentation
2. Prefinalizer O(S²) fix
3. Shared maintenance gate
4. Persistent construction worker pool
5. Explicit `research_fast_v1` switch that skips duplicate fresh replay but leaves the current strict path untouched

That tranche avoids a large storage-format rewrite while potentially removing tens of minutes per generation and hours during maintenance.

---

# J. Instrumentation required for G1 versus G0

## Event format

Emit low-frequency structured events to a non-semantic telemetry stream. Performance observations must not enter candidate, campaign, or generation semantic identities.

Suggested fields:

```json
{
  "schemaVersion": "temporal_qd_performance_event_v1",
  "sourceCommit": "e9b0f8297124f7db03eae8f9bef9cf04f6637697",
  "generationIndex": 1,
  "evidenceProfile": "strict_audited_v1",
  "stage": "proposal.publication.prepublication_replay",
  "startedMonotonicNs": 0,
  "elapsedNs": 0,
  "userCpuNs": 0,
  "kernelCpuNs": 0,
  "rssCurrentBytes": 0,
  "rssPeakBytes": 0,
  "ioReadBytes": 0,
  "ioWriteBytes": 0,
  "ioReadOps": 0,
  "ioWriteOps": 0,
  "semanticIdentityScope": "non_semantic_observation_only"
}
```

Write:

* One append-only `performance-events.jsonl`
* One `generation-performance-summary.json` per generation
* One campaign-level summary that reconciles all generation totals

Do not synchronize after every telemetry row. Buffer and commit telemetry at existing coarse checkpoints.

## Process-wide metrics

For every stage:

* Monotonic wall time
* User CPU time
* Kernel CPU time
* Current and peak RSS
* Thread count
* Voluntary/involuntary context switches where available
* Process read/write bytes
* Process read/write operation counts
* Open handle count
* Queue wait versus active work
* Stage result counts and bytes

On Windows, internal counters should be supplemented during proof benchmarks with ETW/WPR or equivalent tracing for file create/open/flush/link/rename latency.

## Proposal construction

Emit around `execute_v5_g0_transaction`, `execute_with_ledger`, and `construct_batch`:

* `proposal.requested_count`
* `proposal.constructed_count`
* `proposal.accepted_count`
* `proposal.selected_count`
* Rejection counts by exact reason
* `construct.worker_cap`
* `construct.thread_spawn_count`
* `construct.worker_active_ns`
* `construct.worker_idle_ns`
* `construct.batch_count`
* `construct.queue_wait_ns`
* `construct.scientific_materialization_ns`
* `construct.dashboard_compile_ns`
* `construct.dashboard_validation_ns`
* `construct.serial_admission_ns`
* `construct.birth_ordinal_rebuild_count`
* `construct.birth_ordinal_rebuild_ns`
* Per-stage p50/p95/p99 proposal latency

This will immediately show whether the eight-thread cap, serial admission, Dashboard transport, or candidate materialization is dominant.

## Replay and reconstruction

Around every call to transaction or record replay:

* `replay.invocation_count`
* `replay.accepted_record_count`
* `replay.reconstructed_candidate_count`
* `replay.compile_count`
* `replay.wall_ns`
* `replay.cpu_ns`
* `replay.canonical_bytes`
* `replay.cache_hit_count`
* `replay.cache_miss_count`
* `replay.reason = fresh_internal | prepublication | restart_adoption | explicit_audit`

The critical metric is not merely replay duration; it is **how many complete replay passes occur**.

## Serialization and hashing

Instrument the shared canonical JSON and SHA paths by artifact kind:

* `json.parse_calls`
* `json.parse_bytes`
* `json.parse_ns`
* `json.canonical_serialize_calls`
* `json.canonical_serialize_bytes`
* `json.canonical_serialize_ns`
* `sha256.calls`
* `sha256.bytes`
* `sha256.ns`
* `sha256.cache_hits`
* `artifact.full_byte_passes`
* `artifact.semantic_passes`

Track these separately for:

* Attempts
* Proposal deltas
* Accepted records
* Population
* Evaluation
* Journals
* Inventories
* Gateway results
* Evaluated members
* Panel bundles

## Filesystem and durability

Wrap all pipeline file operations:

* Create
* Open
* Stat/metadata
* Stable-identity check
* Read
* Write
* `sync_all`
* `sync_data`
* Directory sync
* Hard link
* Rename
* Remove
* Recursive tree enumeration

For each operation:

* Count
* Total bytes
* Total time
* p50/p95/p99 latency
* Artifact kind
* Fresh versus adoption
* Success/error code

Especially emit:

* `fs.file_sync_count`
* `fs.file_sync_ns`
* `fs.directory_sync_count`
* `fs.directory_sync_ns`
* `fs.hard_link_count`
* `fs.hard_link_ns`
* `fs.metadata_ops`
* `fs.metadata_ns`

This will confirm whether Windows synchronization or metadata latency accounts for the effective 93–372 ms per file.

## Proposal publication

In `execute_v5_fresh_transaction`, staged-artifact functions and the prepublication gate:

* Time to prepare publication
* Time to materialize selected candidates
* Time to generate each of the four public streams
* Time spent in individual verification
* Time spent in bundle verification
* Time spent in prepublication typed replay
* Time spent linking each artifact family
* Number of full reads per artifact
* Inventory rows and bytes by durable-object kind

The current observed 16,142-file total should be broken down exactly by `V5G0DurableObjectKind`.

## Adoption

Instrument separately:

* Receipt parse
* Inventory parse/canonicalization
* Inventory sidecar hash
* Object metadata/path validation
* Object byte hashing
* Exact-tree recursive walk
* Object reopen/parse/canonicalization
* Typed durable-artifact reconstruction
* Transaction replay
* Four public-stream verification
* Funnel projection verification
* Ledger/state promotion

Counters:

* Files authenticated
* Files reopened
* Files hashed more than once
* Bytes hashed
* Canonical rows
* Tree entries
* Symlink/reparse checks
* Duplicate full-data passes

## Campaign freeze

In `execute_v5_freeze_manifest`, `stream_task_manifest`, and `build_task`:

* Candidate count
* Window count
* Task count
* Task-manifest encoded bytes
* Inline profile bytes repeated
* Time cloning profiles
* Task build time
* Pretty-serialization time
* Canonical-hash time
* Inventory/receipt time

Add `unique_profile_bytes` and `inline_profile_bytes` so the duplication ratio is visible.

## Gateway

Emit:

* Enqueue request count/tasks/bytes/latency
* Result-poll request count
* Results per response
* HTTP status counts
* 409 and 503 counts by normalized reason
* Result payload compressed/uncompressed bytes
* Decode/validation time
* Gzip encoding time
* Result-file write and sync time
* Reopen/inflate/re-encode verification time
* Journal append and sync time
* Acknowledgement latency
* Pending/leased/running/completed counts
* Active worker slots
* Task throughput per minute
* Global maintenance state
* Breaker state transitions
* Time in maintenance pause
* Task-level requeues
* Infrastructure events incorrectly charged to attempt budgets

For G1, the critical target is that a maintenance period produces **one shared state transition**, not thousands of requeues.

## Campaign source and seal

Emit:

* Source-build task rows scanned
* Source-build wall/CPU time
* Raw result files read
* Compressed and uncompressed bytes
* Result decode/validation time
* Admitted/rejected result counts
* Index build time
* Inventory build time
* Campaign-seal write/sync time
* Tail transaction time
* Restart path selected: raw scan versus committed receipt

The repository already tracks `raw_scan_millis` and broad read metrics in the campaign-seal design; promote those into consistent generation telemetry.

## Prefinalizer

Emit at `receipt_rows`, `rows`, `file_hash`, and `all_bundles`:

* `evaluated_members.file_read_count`
* `evaluated_members.bytes_read`
* `evaluated_members.hash_count`
* `evaluated_members.row_parse_count`
* `evaluated_members.row_canonicalize_count`
* `bundle.member_lookup_count`
* `bundle.member_comparison_count`
* `bundle.map_build_ns`
* `bundle.join_ns`
* Base-manifest validation time
* Resume validation time
* Panel coverage time
* Funnel preparation time
* Final source construction time

The first corrected run should show:

* Evaluated-members reads: approximately **2**, not **2,050**
* Row parses: approximately **1,024**, not approximately **1.05 million**

## Generation finalizer and state application

Emit:

* Finalizer manifest validation
* Prefinalizer receipt authentication
* Funnel receipt authentication
* Generation-record build
* State-patch build
* Generation-commit write/sync
* Sidecar old/pending/applied reconciliation
* Identity-ledger promotion
* Previous/new state hashes
* Restart branch chosen

The control plane currently gives the generation commit, state patch, and state-application sidecar distinct schemas and reopens the sidecar as a canonical self-hashed object. That is useful correctness information, but its timings should be visible and eventually folded into one generation-commit transaction.

## G1 comparison summary

The G1 summary should report both absolute and normalized values:

* Wall seconds per proposal
* CPU seconds per proposal
* Wall seconds per selected candidate
* Files per proposal
* Bytes per proposal
* File syncs per proposal
* SHA bytes per unique semantic byte
* Canonical serialization bytes per unique semantic byte
* Replay passes per generation
* Adoption seconds per file and per MiB
* Result tasks per minute
* Prefinalizer evaluated-file read count
* Maintenance requeues per task
* Paid worker-hours versus productive worker-hours

Baseline observed G0 values to carry into the comparison:

| Metric                                 |   G0 baseline |
| -------------------------------------- | ------------: |
| Proposal construction/publication/seal |    ~6,000 sec |
| Proposal adoption                      |    ~1,500 sec |
| Proposal files                         |       ~16,142 |
| Proposal bytes                         |      ~783 MiB |
| Peak local Rust batch RAM              |      ~5.8 GiB |
| Replay throughput after recovery       | ~57 tasks/min |
| Uninterrupted replay projection        |     ~71.9 min |
| Maintenance requeues                   |         4,591 |
| Circuit activations                    |           304 |
| Permanent task failures                |             0 |

---

# Final conclusions

The pipeline has successfully achieved several hard things worth preserving:

* Bounded memory
* Deterministic construction and ordinal admission
* Fail-closed remote-result binding
* Crash-safe receipt-last publication
* Exact source/config/data identities
* One-scan campaign sealing with compact restart
* A fast native tail reducer
* No permanent task loss during a severe outage

But the proposal and finalization planes have crossed the line from reproducible research infrastructure into a **distributed notarization protocol executed against its own trusted output**.

The highest-confidence findings are:

1. **Fix the prefinalizer immediately.** Its current O(S²) file authentication is plainly redundant and likely costs close to half an hour per generation.
2. **Stop adopting newly created proposals in the same process.** Commit once and continue from typed memory.
3. **Do not store 12,000–16,000 logical records as independently synchronized files.** Pack them.
4. **Keep one strict replay, not several.**
5. **Use a persistent, configurable construction pool rather than 4,000 transient thread spawns behind an eight-worker ceiling.**
6. **Treat lake maintenance as one campaign-level availability state.** Do not mutate thousands of task attempts while paying for an idle fleet.
7. **Make evidence strength explicit and identity-bearing.** `research_fast_v1` must be visibly non-audited, and strict consumers must reject it.
8. **Reduce the durable state machine to four major per-generation checkpoints.**

The strongest strict guarantees do not require the current wall time. The immediate low-risk changes should remove roughly half an hour from prefinalization, prevent maintenance from consuming hours, and expose exactly how much of the 100-minute proposal stage is scientific construction versus repeated authentication. The subsequent pack/checkpoint work is the path to turning 25-minute restart adoption into a low-single-digit-minute strict verification—or a sub-minute trusted local resume—without sacrificing deterministic reproducibility.
