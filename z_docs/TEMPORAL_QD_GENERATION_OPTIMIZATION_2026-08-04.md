# Temporal QD generation optimization admission

Date: 2026-08-04

## Boundary

This checkpoint optimizes deterministic bidirectional pair construction only.
It reads no market data and contacts no lake, gateway, replay worker, Vast
instance, economic evaluator, or archive reducer. The preserved legacy
implementation is the semantic oracle; performance artifacts are deliberately
excluded from candidate and generation identities.

The optimization addresses four measured scaling costs from the 1,024
immigrant diagnostic:

1. full proposal and candidate retention across the generation;
2. whole-population canonical serialization copies;
3. repeated candidate/funnel and native-response cloning;
4. repeated reconstruction of immutable grammar, catalog, policy, and
   indicator-learning authority.

Resource observation was also split into a 2-second detailed process-tree
sample and a 500-millisecond lightweight fail-closed RSS/headroom watchdog.
Main-thread generation CPU and telemetry-thread CPU are now reported
separately.

## Reversible implementation

`generate_pair_population` retains two explicit implementations:

- `optimized`, the admitted production default;
- `legacy`, the unchanged reference oracle during the deprecation window.

The optimized loop retains proposal paths and identity fields rather than all
accepted documents. It replays immutable proposal entries one at a time,
streams canonical population hashing and persistence, and caches only frozen
per-side construction authority. Generated profiles, plans, candidates, and
proposal payloads are not cached.

The legacy path is not removed. A run interrupted under legacy is explicitly
proven able to resume under optimized and finish with the same semantic bytes.

## Semantic admission

The focused oracle covers:

- exact result equality;
- byte-identical pair config, proposal journal, population, and generation
  journal;
- candidate, native-validation, population, and journal identities;
- optimized split/restart parity;
- legacy-to-optimized restart parity;
- global identity-ledger bytes and duplicate enforcement;
- explicit-parent scheduling through the ordinal-6 crossover route;
- the rich-immigrant collision tripwire.

The production-shaped 64-candidate real-authority benchmark compared 67
semantic artifacts totaling 186,190,780 bytes. All files and returned results
were exact. Both implementations produced:

- population:
  `sha256:84b9bd0197711c33c007158ab4954a27b77e984261b696ef36cfc40f8d297d1e`;
- generation journal:
  `sha256:29d7e5b18f15bdcbdaae4c3a0c1643c0c6088d68d3460bc0aad8449937d35d19`.

An independent root-agent rerun at eight candidates also compared all 11
semantic artifacts byte for byte and produced exact results:

- report:
  `C:\fuzzfolio-research\temporal-qd-root-audit-benchmark-8b-20260804\benchmark-report.json`;
- benchmark:
  `sha256:cb0414bbabd358b850e9de2f51ec1ba86422e61c010f3e9eed87f2178fae283f`;
- population:
  `sha256:d4a288cb4715f1161821125bb04f65b1bf86b00aaded201be6b6b0e66d7ab96d`;
- generation journal:
  `sha256:617358c7fa4d2c71f594ca5eb4ab4cb42d45c51e3974a9f82d76cc91cc8a6c33`.

After optimized became the default, 59 focused generation/authority/
observability/supervisor tests and all 81 `test_temporal_qd*.py` tests passed.

## Measured result

The 64-candidate real-authority comparison is the meaningful scaling sample:

| Metric | Legacy | Optimized | Change |
| --- | ---: | ---: | ---: |
| generation wall time | 143.288 s | 134.056 s | -6.4% |
| generation main-thread CPU | 117.328 s | 111.172 s | -5.2% |
| harness peak process-tree RSS | 811.4 MiB | 482.1 MiB | -40.6% |

The original 64-candidate harness ran both implementations sequentially in one
interpreter. Its semantic comparison and wall/CPU result remain valid, but
Python allocator residue from the legacy run makes the optimized RSS value a
conservative upper bound rather than a clean-process peak. The later
production-default 1,024 witness below is the authoritative memory result.

The eight-candidate independent rerun was artifact-exact but 4.3 percent
slower in optimized mode, with 4.5 percent lower peak RSS. At that size fixed
startup, sampling, and finalization costs dominate; it is retained as an
independent correctness witness, not a throughput claim.

The optimized trace also makes the remaining CPU shape clear. Indicator-plan
enumeration remains the largest per-proposal Python bucket. Streaming final
population construction intentionally trades some CPU and disk reads for the
large reduction in retained and transient memory. Those are later optimization
targets; they do not justify weakening immutable artifacts or restart checks.

## Full-scale production-default witness

After commit `5996fda094f72ec1bc6027caf1efc07897a4b8e6` was pushed, a fresh
1,024-candidate construction smoke exercised the optimized production default:

`C:\fuzzfolio-research\rich-immigrant-optimized-1024-20260804`

It completed all construction, streaming population hashing, population
persistence, generation-journal persistence, and report reduction:

- 1,024 proposals accepted;
- 1,024 unique candidate identities and pair semantics;
- zero candidate or pair duplicate dispositions;
- 2,022.359 seconds generation wall time (33 minutes 42.359 seconds);
- 1,792.766 seconds main-thread CPU;
- 148.875 seconds separately attributed telemetry-thread CPU;
- 208.4 MiB peak process-tree RSS;
- 144.4 MiB peak coordinator RSS;
- 31.5 GiB minimum host-available memory;
- resource guard status `within_limits`.

The previous implementation retained 7.89 GiB after proposal 1,024 and spiked
to 10.12 GiB while hashing. The optimized run stayed near 0.2 GiB during both
hashing and persistence, a reduction of approximately 98 percent at the former
failure seam. It also finished about 10.1 percent sooner than the prior
2,248.84-second run despite doing the population persistence that the prior run
never reached.

Full-scale identities and files:

- semantic population:
  `sha256:d6d35e84abfd578a2fb5843639153d5975bca00152fc6dc7436f5657e6e65d5b`;
- population file SHA-256:
  `sha256:a83aaa3375260ab8a676a66b62e66cdaa4992bd1c0ab10fbf5e969dce7bec8cf`;
- generation journal:
  `sha256:a964bd990e4cabb3342c573cc30f348a0e11f410b810b349468cd965b5f9d43e`;
- construction report:
  `sha256:d7ee17262e58f9be9aae3feae53de5808c3114b6caf50d16e1a53b1974134d1e`;
- proposal journal: 1,788,130,529 bytes;
- population: 1,199,513,098 bytes;
- performance evidence: 63,118,239 bytes.

The report identity was independently recomputed after the run and matched.
The leading remaining wall-time buckets were indicator-plan enumeration
(362.71 seconds), streamed population persistence (329.54 seconds), streamed
population hashing (285.75 seconds), candidate materialization (129.21
seconds), and immigrant payload construction (125.70 seconds). These provide a
precise next optimization map without reopening the admitted semantic path.

The reusable A/B harness was then hardened to launch each implementation in a
fresh interpreter and process tree. A four-candidate isolated harness smoke was
again result-exact and byte-exact across all seven semantic artifacts:

- benchmark:
  `sha256:e830ca59cfcb7a2d462a77f65013f543625f43ec4789efb86f1521b7d4591404`;
- legacy clean-process peak: 207.5 MiB;
- optimized clean-process peak: 185.3 MiB.

At that tiny size fixed costs dominate timing; this smoke admits measurement
isolation and semantic consistency, not a throughput conclusion.

## Admission and remaining proof

The optimized path is admitted as the production default, and the former
1,024-candidate memory defect is measured closed. The legacy oracle must remain
callable until at least one parent/archive generation completes under the
normal resource guard. Parent scheduling, crossover, ledger, and restart bytes
are already covered by bounded oracle tests; this remaining witness is about
operational scale rather than a known semantic contradiction.

No broad economic search was launched in this checkpoint.
