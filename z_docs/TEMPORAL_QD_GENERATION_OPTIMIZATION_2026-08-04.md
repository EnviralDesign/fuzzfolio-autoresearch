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

The eight-candidate independent rerun was artifact-exact but 4.3 percent
slower in optimized mode, with 4.5 percent lower peak RSS. At that size fixed
startup, sampling, and finalization costs dominate; it is retained as an
independent correctness witness, not a throughput claim.

The optimized trace also makes the remaining CPU shape clear. Indicator-plan
enumeration remains the largest per-proposal Python bucket. Streaming final
population construction intentionally trades some CPU and disk reads for the
large reduction in retained and transient memory. Those are later optimization
targets; they do not justify weakening immutable artifacts or restart checks.

## Admission and remaining proof

The optimized path is admitted as the production default. The legacy oracle
must remain callable until a fresh 1,024-candidate optimized construction smoke
and at least one parent/archive generation complete under the normal resource
guard. This checkpoint does not claim that the previous 1,024-candidate 10.12
GiB peak has already been measured away at full scale; it proves the semantic
cutover and a substantial bounded memory reduction without drift.

No broad economic search was launched in this checkpoint.
