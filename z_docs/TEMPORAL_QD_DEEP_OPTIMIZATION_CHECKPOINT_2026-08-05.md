# Temporal QD deep-optimization checkpoint

Date: 2026-08-05

Status: bounded optimization checkpoint; no economic search launched.

Base semantic checkpoint:

```text
ffb5cc14453e407663fa5d22bd1ff3a15d2c7337
Admit rotating temporal QD bootstrap semantics
```

## Outcome

This pass established and reviewed the first compact storage, restart, and
rotating-tail boundaries without changing search economics or replacing the
legacy proposal path. The only production-connected optimization is an
explicit opt-in indexed rotating-tail mode. Legacy loading remains the
default and the semantic oracle.

No Vast instance, replay worker, market experiment, or new search campaign was
started.

## Admitted scope

### Indexed rotating-tail reduction

The supervisor now accepts:

```text
--tail-result-mode legacy   # default/oracle
--tail-result-mode indexed  # rotating evidence only
```

Indexed mode source-verifies each completed immutable result matrix and retains
a compact in-memory projection for the active transaction. Member loading,
provenance evidence, result descriptors, generation-funnel reduction, and
artifact capture reuse that projection rather than reopening the same worker
payloads independently.

The mode is operational rather than semantic. It does not participate in
campaign authority, frozen config, checkpoint, ledger, archive, or result
identity. A legacy run can therefore resume in indexed mode without changing
its research meaning.

The indexed path preserves exact candidate coverage and exact legacy output
bytes. Restart validation uses bounded per-generation projection retention,
and post-save validation does not rescan previously admitted generations.

### Experimental compact foundations

Two repository-owned but unintegrated foundations are retained for the next
front-half reconstruction:

- a typed immutable content-addressed object store with loose, packed, and
  compressed-packed backends;
- a sealed append-only proposal journal with compact checkpoints and bounded
  tail replay.

Packed publication is serialized with an interprocess lock across
refresh/select/publish. Cooperating overlapping writers cannot create duplicate
references that poison restart. Lock failure and crash residue fail closed;
manual removal of a stale lock may still be required after a process crash.

The sealed journal rejects symlink/reparse path escape, conflicting content
identity aliases, partial/corrupt artifacts, and ambiguous checkpoint failure.
An append that committed its segment but failed to publish the checkpoint
returns an explicit committed receipt; checkpoint recovery is idempotent.

Neither foundation is authoritative or used by the current proposal producer.

### Benchmark harness

The local A/B harness now runs each side in a fresh process against its own
verified input clone. It uses at least two counterbalanced repetitions
(`old -> new`, then `new -> old`) and median aggregation for hard gates.
Process-tree CPU and I/O counters remain observational because short-lived
descendant accounting cannot yet be guaranteed; they cannot be used as hard
admission thresholds. The default measurement label remains
`concurrent_load_provisional`.

## Measured evidence

All timings below were collected while the workstation could have unrelated
load. They are directional, not clean-machine admission measurements.

### Rotating tail

On a 24-task production-shaped fixture:

```text
legacy whole tail:                  8.2545 s
indexed whole tail including build: 4.3026 s
indexed same-process reuse:          0.0929 s
logical raw bytes, legacy:          411,420
logical raw bytes, indexed build:   205,710
tracemalloc peak, legacy:         2,394,047 B
tracemalloc peak, indexed:        2,063,530 B
```

The first indexed pass was about 48 percent faster and used less traced memory.
The 0.0929-second reuse number is a hot same-process reduction, not a full
generation runtime claim.

### Packed content storage

A 1,000-object storage-only fixture reduced approximately 1,000 loose files to
one pack plus one index and changed roughly 10.8 seconds of loose writes to
about 0.2 seconds of packed publication. A repetitive 100 MiB fixture compressed
to about 0.44 MiB. Bounded decompression reduced the traced 100 MiB `get_many`
peak from roughly 315 MiB to 108 MiB.

These are storage-boundary measurements. They do not prove an end-to-end
proposal-generation improvement.

### Sealed restart

On a bounded 55-entry replay with a checkpoint at entry 48:

```text
legacy replay: 55 entries, 285,626 B read, 49.12 ms, 689,411 B traced peak
sealed replay:   7 entries,  94,334 B read, 23.05 ms, 400,736 B traced peak
```

The reconstructed state was equivalent. This remains isolated until a future
proposal adapter defines the external-ledger binding and passes authentic
restart admission.

## Rejected front-half prototype

The attempted compact raw proposal adapter is deliberately excluded from this
checkpoint.

On 29 authentic G2 proposal records it reduced persisted size from about
100.3 MiB to 19.8 MiB and made cold exact retrieval about 26 times faster.
However, producer-hot compressed publication took about 1.084 seconds versus
0.044 seconds for the existing buffered, non-fsync rich write. The comparison
was not durability-equivalent, but it clearly did not establish an end-to-end
write-time win. A proposed prevalidated shortcut also could not bind detached
metadata to opaque raw bytes without restoring full validation. That shortcut
was quarantined and the unused prototype was removed rather than committed as
dead machinery.

No Rust/PyO3 proposal kernel was attempted. The August 5 plan correctly requires
a compact, admitted typed boundary before porting CPU-bound loops.

## Review and verification

An independent serial review found no P0 defects. Its P1 findings covered:

- packed concurrent-writer poisoning;
- journal path escape, identity alias ambiguity, and checkpoint-failure
  semantics;
- raw/metadata split-brain in the rejected front prototype;
- indexed candidate coverage;
- post-save historical raw rescans.

Each admitted-area P1 was corrected with adversarial coverage. The rejected
front prototype was removed from checkpoint scope.

Final focused verification command:

```powershell
uv run pytest -q `
  tests/test_temporal_qd_object_store.py `
  tests/test_temporal_qd_sealed_journal.py `
  tests/test_temporal_qd_optimization_benchmark.py `
  tests/test_temporal_qd_tail_result_index.py `
  tests/test_temporal_qd_tail_result_index_integration.py `
  tests/test_result_codec.py `
  tests/test_temporal_qd_rotating_evidence.py `
  tests/test_temporal_qd_funnel_adapter.py `
  tests/test_temporal_qd_evolution.py `
  tests/test_temporal_qd_supervisor.py `
  -k "not pair_g0_64_to_32_rotating_supervisor_restart_never_reschedules_construction"
```

The one excluded historical supervisor test is blocked before the changed path
by the workstation minimum-headroom guard. This checkpoint does not relabel it
as a passing test.

Final result after removing the rejected compact-front prototype:

```text
130 passed, 1 deselected in 40.72s
```

## What remains

- Run clean-machine 64/128/1,024 end-to-end proposal and consolidation A/Bs.
- Design a compact proposal boundary that wins a durability-equivalent
  producer benchmark before integration.
- Bind the sealed proposal checkpoint to the campaign identity ledger.
- Add bounded streaming/capability protection to the non-default no-source-
  verification tail-index loader.
- Decide whether the next material gain comes from a batched Rust proposal
  kernel, a compact cumulative archive reducer, or both.
- Keep legacy paths callable until at least one complete generation passes at
  scale.

The next operation is a review decision about continuing optimization. It is
not authorization to launch the next EURUSD campaign.
