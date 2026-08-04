# Temporal QD Rust population-finalizer admission

Date: 2026-08-04

## Boundary

This checkpoint moves one measured coordinator bottleneck to a standalone Rust
CLI: validation of the immutable proposal journal, canonical population hash
derivation, and atomic write-once population persistence. It does not move or
change proposal scheduling, grammar construction, indicator selection,
mutation, native module validation, identity-ledger behavior, economic
evaluation, archive reduction, or replay.

Python remains the exact semantic oracle and is callable with
`population_finalizer="python"`. The optimized generator now defaults to
`population_finalizer="rust"` only after the gates below passed. The legacy
generator continues to force its original Python finalization regardless of
the optimized-path default.

## Contract

Python authors two small operational inputs under
`performance/population-finalizer/`:

- a canonical population shell with an empty candidate array and fixed-length
  population-SHA placeholder;
- an identity-bound manifest containing the config/generation identity, every
  expected proposal-entry SHA, every accepted candidate identity and ordinal,
  exact file-set/count expectations, and the platform newline policy.

Rust then:

1. validates the manifest identity and shell file SHA;
2. rejects missing, extra, gapped, malformed, truncated, non-finite, or
   identity-divergent proposal entries;
3. validates every entry's embedded semantic SHA and exact config,
   generation, ordinal, disposition, candidate ID, and candidate identity;
4. sorts accepted candidates by the existing candidate-ID contract;
5. copies their already-canonical raw byte ranges without re-encoding them;
6. computes the semantic population SHA while writing one temporary artifact;
7. patches the fixed-length SHA placeholder, fsyncs, and atomically installs
   the artifact;
8. accepts an exact existing artifact and rejects a divergent one.

There is no silent Python fallback. The release executable is built or
resolved once, its source and executable SHA are frozen in an operational
authority record, and a different authority cannot resume the same output
root. `FUZZFOLIO_TEMPORAL_QD_POPULATION_FINALIZER` may point to a prebuilt
binary with the same contract version.

Admitted Rust authority:

- contract: `temporal_qd_population_finalizer_v1`;
- crate version: `0.1.0`;
- source SHA:
  `sha256:db088950a0a00b4ebc7fd9158e81cd4e69769e3de5d6cc5a2c61435717c7a0bd`;
- Windows release executable SHA:
  `sha256:c371b6d3088d90ea9338ff7a9f427607848b7342b60951e15880b69ea13bd13f`.

## Required parity gates

All three gates used real current proposal journals and existing Python-oracle
population artifacts. `artifactBytesExact` and `semanticHashExact` were true
at every scale.

| Candidates | Proposal journal | Population | Python semantic SHA | Rust native time |
| ---: | ---: | ---: | --- | ---: |
| 64 | 111,419,037 B | 74,745,058 B | `sha256:84b9bd0197711c33c007158ab4954a27b77e984261b696ef36cfc40f8d297d1e` | 0.999 s |
| 128 | 223,511,906 B | 149,952,706 B | `sha256:519d84329f526b75aa1e7c5cebf165e06d2d21ae71c7b71b1306b1fad1f4fe8a` | 1.808 s |
| 1,024 | 1,788,130,529 B | 1,199,513,098 B | `sha256:d6d35e84abfd578a2fb5843639153d5975bca00152fc6dc7436f5657e6e65d5b` | 16.538 s |

The exact 1,024 population file SHA was also preserved:

`sha256:a83aaa3375260ab8a676a66b62e66cdaa4992bd1c0ab10fbf5e969dce7bec8cf`

Evidence roots:

- `C:\fuzzfolio-research\rust-finalizer-parity-64-final-20260804`;
- `C:\fuzzfolio-research\rust-finalizer-parity-128-oracle-20260804`;
- `C:\fuzzfolio-research\rust-finalizer-parity-128-native-20260804`;
- `C:\fuzzfolio-research\rust-finalizer-parity-1024-20260804`.

## Restart and corruption gates

The admitted tests cover:

- Python-to-Rust and Rust-to-Python resume with identical semantic files;
- Rust verification of an exact existing population;
- recovery from an interrupted/stale finalizer temporary;
- divergent existing-population refusal;
- journal tamper, truncation, missing file, extra file, NaN, and duplicate
  candidate-ID rejection;
- Unicode, escaped slash/quote/control text, nested values, finite floats, and
  negative-zero byte parity;
- strict canonical top-level field ordering and derived-field removal.

Rust unit tests and clippy passed. The post-cutover focused Python surface
passed `119` tests.

## Measured finalizer improvement

On the prior 1,024 production witness, Python population hashing took 285.75
seconds and persistence took 329.54 seconds: 615.29 seconds combined. The
admitted Rust CLI performed the stricter full-journal verification and exact
population assembly in 16.538 seconds, a 37.2x reduction for this seam and
598.752 seconds removed.

Holding all earlier phases constant projects the 2,022.359-second full
construction witness down to approximately 1,423.607 seconds (23 minutes
43.607 seconds), or 29.6 percent end-to-end improvement. A fresh 1,024 run from
the committed Rust-default checkpoint is the final operational measurement;
this projection is not substituted for that witness.

## Fresh production-default 1,024 witness

The final operational measurement completed from committed checkpoint
`214283c3897da1fb7180ce60315b951dd2e0f1f9` under:

`C:\fuzzfolio-research\rich-immigrant-rust-finalizer-1024-20260804-v4`

The normal optimized generator defaulted to the Rust finalizer; Python was not
selected and no fallback occurred. The run froze the actual operational build
used by that root:

- contract: `temporal_qd_population_finalizer_v1`;
- source SHA:
  `sha256:db088950a0a00b4ebc7fd9158e81cd4e69769e3de5d6cc5a2c61435717c7a0bd`;
- executable SHA:
  `sha256:dfe98303e5ab2b53b2ccdc21c8eb5bc86f35d290a3e36afd0c552912a9de8626`.

The executable identity is deliberately per-root authority; the contract and
source identity match the admitted parity implementation, and the root cannot
resume under a different executable identity.

The fresh run produced:

| Measurement | Result |
| --- | ---: |
| Completed candidates / proposals | 1,024 / 1,024 |
| Unique candidate identities | 1,024 |
| Unique pair genomes | 1,024 |
| Duplicates | 0 |
| Acceptance ratio | 1.0 |
| End-to-end wall time | 1,448.433 seconds (24m 08.433s) |
| Rust population finalization | 12.678 seconds |
| Peak recursive process-tree RSS | 240.84 MiB |
| Peak coordinator RSS | 143.99 MiB |
| Peak process-tree CPU | 1.198 core equivalents |
| Minimum host-available memory | 24.30 GiB |
| Resource guard | within limits |

Against the 2,022.359-second Python-default production witness, the actual
fresh run removed 573.926 seconds (9m 33.926s), improved end-to-end wall time
by 28.4 percent, and ran 1.40x as fast. Normal variance in the unchanged
construction phases accounts for the difference from the controlled 29.6
percent projection. The finalizer itself is no longer a material generation
bottleneck.

## Admission

The narrow Rust finalizer is admitted as the optimized production default.
Python remains the exact selectable oracle. Broader Rust ports remain deferred
until separately profiled and admitted; this checkpoint does not authorize a
semantic rewrite of proposal construction.

The fresh operational witness is complete. Further optimization should target
measured proposal construction phases rather than this finalization seam.
