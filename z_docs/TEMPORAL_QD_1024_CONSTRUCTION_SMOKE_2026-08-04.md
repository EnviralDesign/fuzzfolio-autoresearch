# Temporal QD 1,024-immigrant construction smoke

Date: 2026-08-04

## Boundary

This was an immigrant-only, no-market construction witness.  It contacted no
lake, gateway, replay worker, market data, economic evaluator, or archive
reducer.  It was intended to answer three questions before any generation-path
optimization:

1. did the rich-immigrant correction remove the duplicate collapse;
2. where does construction CPU/wall time go;
3. how does retained process-tree memory scale on the workstation.

Authorities:

- AutoResearch commit:
  `2ebab97fe7eb609a5a562547f783f8949f2b8d2f`;
- FuzzFolio commit: `0cb5951c7b74188b2f16403e27efbdc7e59ae769`;
- pair-run config:
  `sha256:0de76efb91ff6109aea522dbb4643ea65c8637a045f74d096e8e2f8536763e17`;
- generation config:
  `sha256:7061f78cf15687352f4ad8c3699444549522a99cacbe09a97d5029b789090547`.

## Diversity result

All 1,024 proposals were accepted.  There were:

- 1,024 unique candidate identities;
- 1,024 unique pair-genome identities;
- zero candidate, pair-genome, or observed duplicate dispositions;
- a 100 percent construction acceptance ratio.

The streamed construction distribution is:

`sha256:fd78009d7f1339e9195d09febe4c954fd3d18adc4623b4077fde4765b1a13060`.

The selected seed roots were balanced on both sides.  Every grammar and
indicator depth from zero through four appeared on both sides.  Every seeded
evidence group, event binding, and hold kind appeared.  The sample exercised
all admitted grammar-operation families and all admitted indicator-learning
families, including family/timeframe/period/range changes, fuzzy-instance
insertion/removal, group membership, and contribution weights.  This rejects
the former 2,304-box duplicate-collapse mechanism at the requested population
size.  It does not by itself prove economic quality.

## Timing result

Construction ran for 2,248.84 seconds (37 minutes 28.84 seconds) and consumed
2,194.30 coordinator-process CPU seconds.  Median, p95, and p99 sampled
process-tree use were approximately 0.99, 1.06, and 1.10 cores.  The generation
was therefore overwhelmingly synchronous rather than workstation-saturating.

The largest measured exclusive wall-time buckets were:

| Phase | Total seconds | Mean ms/proposal or call | Share of run wall |
| --- | ---: | ---: | ---: |
| candidate materialization | 759.45 | 741.65 | 33.8% |
| funnel-audit construction | 422.78 | 412.87 | 18.8% |
| indicator-plan enumeration | 398.67 | 112.72 | 17.7% |
| side native validation/freeze | 139.93 | 68.33 | 6.2% |
| immigrant payload construction | 133.91 | 130.77 | 6.0% |

The first three buckets consumed 70.3 percent of run wall time.  Native pair
compile response waits consumed only 42.75 seconds total.  The evidence points
first to repeated Python materialization/audit/plan work, not to Rust/native
transport, for the later optimization discussion.

The 500 ms process-tree sampler was intentionally dense and is not free.  Its
2,831 collection calls occupied 393.26 seconds of background-thread wall time,
and resource-line writes occupied 24.35 seconds.  These overlap the generation
and raw samples remain complete, but the 37.5-minute result must be treated as
an instrumented diagnostic rather than a clean uninstrumented throughput
baseline.

## Memory result and stopped boundary

Retained RSS grew approximately linearly:

| Proposal | Process-tree RSS GiB | Private GiB |
| ---: | ---: | ---: |
| 64 | 0.67 | 0.63 |
| 256 | 2.11 | 2.08 |
| 512 | 4.05 | 4.02 |
| 768 | 5.98 | 5.95 |
| 1,000 | 7.71 | 7.71 |
| 1,024 | 7.89 | 7.88 |

All 1,024 proposals were durably written before finalization.  Canonical
population hashing then created a large transient serialization copy and
raised the process tree to 10,867,310,592 bytes (10.12 GiB).  The frozen 8 GiB
guard recorded `maximum_tree_rss_exceeded` in
`generation.finalize.hash_population`, sealed an error summary, and prevented
population persistence.  Host-available memory never fell below 27.76 GiB and
pagefile use did not show a sustained run-driven rise.

The result is a focused resource defect, not a diversity or architecture
contradiction: candidate retention is already too expensive for a normal work
PC, and whole-population canonical serialization adds an unacceptable
transient copy.

## Reporter correction

The first post-run report implementation loaded all proposal documents into a
Python list.  That recreated the same retention defect outside the guarded
generation and reached 15.7 GiB.  The reporter was terminated immediately;
host memory recovered and no evidence was lost.  It now streams exactly one
proposal document at a time.  Its distribution output exactly matches the
frozen native calibration distribution, and the 1,024 report completed with an
observed working set near 100 MiB and no stderr.

The final compact report is:

`sha256:e3cf635ff58d9bbfc761a6f51e3360cc41b05b1e3897dd777fe0d752f0fb3872`.

The 1,024 immutable proposal artifacts occupy 1,787,443,267 bytes (about 1.66
GiB).  Performance spans add 61,043,687 bytes and resource samples add
5,593,790 bytes.

## Decision seam

- Diversity correction: supported at 1,024/1,024 with zero duplicates.
- CPU diagnosis: sufficiently resolved to prioritize later work.
- RAM pressure: unacceptable for the workstation and reproduced precisely.
- Broad economic search: not launched.
- Candidate-generation optimization: not started in this checkpoint.

The next operation should be agreed after reviewing this evidence.  Likely
targets are streaming/compact retained proposal state, streaming population
hash/persistence, removal of redundant candidate/funnel copies, and then a
less intrusive benchmark that separates observability cost from generation
cost.
