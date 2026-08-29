# V37 archive terminalization — native trace contract

This research-only terminalization pass explains the already accepted V37
Variant-0 control. It does not change the finalizer's archive policy or launch
any market, worker, gateway, Vast, generation, or backfill evaluation.

## Durable code

- `rust/temporal-qd/crates/qd-generation-finalizer/src/bin/v37-archive-classifier-trace.rs`
  writes a self-hashed diagnostic trace from one sealed fast-ephemeral
  `source.json`.
- `autoresearch/temporal_qd_v37_archive_terminalization.py` binds five traces,
  the retained disposition ledger, and the Phase-1 descriptive table into the
  fixed-stream report.

The trace calls the same native support, direction, Pareto, and crowding code
used by Variant 0. Its unit control verifies that emitting diagnostics leaves
the cumulative and parent outputs byte-identical.

## Fixed-stream result

The historical breeding trajectory remains `3 -> 3 -> 0 -> 0 -> 0`. G3 has no
support-and-direction-eligible pre-Pareto row; the trace records no
Pareto/capacity loss in G1–G5. The six historical members all flip negative on
their immediately following retained-parent panel. Variant 2 is observational
memory only. Variants 3–5 are diagnostic overlays, not alternate archive
replays. The G1 admission cap is cleared; G2–G5 remain unresolved without the
explicitly unlaunched bounded backfill experiment.

## Reproduction boundary

Use new ignored output directories for every invocation. The report generator
requires native traces for all five sealed sources, the State-E disposition
ledger, and the Phase-1 candidate-evaluations JSONL. It fails if its output
directory already exists, a trace self-hash drifts, its trace/ledger parent
sets differ, or the six next-panel negative flips do not reconcile.
