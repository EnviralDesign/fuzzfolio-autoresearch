# Pre-Mega-Run Proposal Performance Gate

Do not launch a large AutoResearch campaign until the fully integrated
bidirectional proposal path has been benchmarked and reviewed.

## Current evidence

- Exact continuation construction: 220.447 seconds cold, 6.713 seconds warm
  (32.84x faster after the deterministic process-local cache is warm).
- Warm proposal construction: 512 proposals at 30.43 proposals/second.
- Proposal-ledger membership: 697x faster in the duplicate-heavy benchmark and
  1.71x faster in the accepted-heavy benchmark.
- Persistent native validation: 100 real requests through one server process in
  approximately 31.6 seconds.
- A prior 10-slot QD profile measured approximately 2.307 seconds/slot, with
  native validation accounting for approximately 0.859 seconds/call.

These measurements prove that continuation reconstruction and ledger lookup are
no longer the primary bottlenecks. They do not prove an equivalent end-to-end
speedup. Bidirectional proposals may require both changed-module validation and
canonical pair compilation.

## Required benchmark

Measure a representative finite pair-mode generation with separate wall-time,
CPU, and peak-memory attribution for:

1. proposal and parent selection;
2. typed grammar, indicator, and hold mutation;
3. v2 module validation;
4. v3 pair compilation and validation;
5. journal, funnel, and population persistence;
6. archive reduction;
7. dispatch preparation.

Report cold and warm behavior, duplicate-heavy and accepted-heavy behavior, and
exact restart parity. Do not extrapolate from repeated validation of one profile
alone.

## Decision before a mega run

Choose deliberately among:

- keeping the serial path if it is already a small fraction of total runtime;
- batching persistent authority requests;
- bounded local parallelism (normally one or two cores on the desktop);
- distributing inherently parallel compilation to Sager, Mac, or Vast workers;
- moving a proven hot loop to native/Rust code.

The chosen optimization must preserve candidate, proposal, program, journal,
and restart identities. Record the benchmark and decision in a new checkpoint
before starting the mega run.
