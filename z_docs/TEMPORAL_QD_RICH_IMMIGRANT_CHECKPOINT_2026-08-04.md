# Temporal QD rich-immigrant correctness checkpoint

Date: 2026-08-04

## Finding

The broad bidirectional bootstrap called its candidates random immigrants, but
the constructor selected only one authored seed root for each side.  Its
effective pair space was:

```text
(3 seed names * 4 evidence groups * 4 event bindings * 1 plan)^2
= 2,304 executable long/short pairs
```

An empty-quality four-generation campaign asking for 4,096 unique candidates
therefore had no possible completion path.  The observed generation-3
duplicate collapse was the expected coupon-collector end state of that finite
box, not evidence that the temporal substrate itself lacked variety.

## Correction

The pair factory now constructs each long and short module independently from
frozen, identity-bound choices for:

- seed root, evidence group, fresh event binding, management plan, and hold;
- zero through four typed grammar operations, selected by operation family
  before concrete plan;
- zero through four indicator-learning operations, selected by operator family
  before concrete plan;
- graph topology, event routes, indicator family/timeframe/parameters, fuzzy
  group structure and weights, and hold semantics already admitted by the
  search language.

The composed authored module is validated once at the native boundary and the
two valid sides are compiled once into the executable v3/both pair.  The
construction policy and implementation version are frozen into the pair-run
authority and candidate lineage.

Broad admission now fails closed unless conservative root-and-hold axes alone
provide at least four times the requested campaign capacity.  Grammar and
indicator entropy are deliberately excluded from this floor.  The generation
journal also records attempted and accepted construction distributions, and an
immigrant-only bootstrap trips after 512 attempts if semantic acceptance falls
below 25 percent.

## Native validator seam exposed and corrected

Rich construction found a valid module that closed a position into a flat
cooldown state before returning to the shared entry supervisor.  Composite
hold-route validation incorrectly required the immediate close destination to
be the supervisor itself, even though its abstract position analysis had
already proven the cooldown state position-absent.

FuzzFolio commit `0cb5951c7b74188b2f16403e27efbdc7e59ae769`
(`Accept proven-flat composite close routes`) makes v2 and v3 use the same
position-state proof.  It preserves rejection for missing, reason-limited, or
position-capable close destinations.  The focused native-core file passed 37
tests.

## Evidence

The pair authority was rebuilt against the exact FuzzFolio commit above and
the current real indicator catalog.  No market data, lake, gateway, worker, or
economic scoring path was contacted.

Capacity admission for a 4,096-candidate campaign:

- side root-and-hold capacity: 864 long and 864 short;
- conservative pair floor: 746,496;
- deterministic selector probe: 8,129 unique fingerprints from 8,192 seeds;
- capacity audit: `sha256:ce949f39a34b740884b7578763679d21280a4f4d7c54f25416f9d573b42f0c6b`.

The bounded restart admission produced 8/8 unique seed immigrants and two
identical 32-candidate offspring populations across interrupted/resumed and
uninterrupted paths:

- admission summary: `sha256:bb41df861fdb1a2053e2c840ae5bfef65f746cd756c7c66f6bab4ce724123a77`;
- resumed/uninterrupted journal:
  `sha256:33d7554600d6392b9314c6ce56ac2a661473d9a63b5fa1b7c21c7b1459d2b943`;
- resumed/uninterrupted population:
  `sha256:548f2159c2dc3426facf490dd27da3117d4df31a06a0de13d11bec85b9c7e5b6`.

A separate all-immigrant native sample produced 64 accepted candidates from
64 proposals with zero candidate, pair, or global-pair semantic duplicates:

- journal: `sha256:0463df6066544ccebfcc2170f7c6dc92188affe952bb3f885f0e707a339f2528`;
- population: `sha256:9ae56c3ba93ff0c63ec51d13196c7f543862e058c0d5c01055a8ae72665a4bf8`;
- construction distribution:
  `sha256:12218cd77a45c17325e0c24dfe83931d9e80faa0e8c6bc67689bacd07c362b93`.

The sample reached every seed archetype on both sides, applied depths zero
through four, used market-bar, elapsed-calendar, and no-hold alternatives, and
exercised graph topology plus indicator/fuzzy construction families.

Verification results:

- focused pair factory/generation/supervisor tests: 50 passed;
- complete AutoResearch collection: 1,582 passed, 10 failed in unrelated
  legacy `play_hand_lab` gateway/sweep/durability tests;
- syntax compilation and `git diff --check`: passed.

## Operational state and next seam

- The doomed broad supervisor was stopped and its artifacts were preserved.
- The paid Vast instance was destroyed; no paid worker is running.
- The Lab Gateway remains available locally.
- No replacement broad search has been launched.
- Candidate-generation performance optimization was not started.  It remains
  a separate follow-up to inspect jointly before another paid run.
- A fresh pair authority and worker image must be built from the checkpointed
  repository heads before the next search launch.

