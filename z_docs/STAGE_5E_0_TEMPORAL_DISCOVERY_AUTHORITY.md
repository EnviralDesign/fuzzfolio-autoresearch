# Stage 5E-0 Temporal Discovery Authority

## Status

```text
Decision: frozen
Implementation: pending native admission
Pilot launch: authorized only after all native and plan-only gates pass
Protected/reserved evidence: prohibited
Scale beyond the 256-program pilot: prohibited
```

## Purpose

Stage 5D-5 admitted the immutable worker-facing unit:

```text
one candidate × one development window × two fixed cost views
```

Stage 5E-0 adds the research controller above that unit. It proposes programs,
asks FuzzFolio to validate them, removes semantic duplicates before market
compute, screens them progressively, and preserves separate economic and novelty
archives.

FuzzFolio remains the sole authority for temporal schema, semantics, evaluator
capabilities, fidelity, profile identity, and program identity. AutoResearch owns
proposal mechanics and research selection only.

## Frozen Pilot

```text
unique canonical programs: 256
de-novo/substantially mutated share: 70%
seed-derived share: 30%
initial windows: 2
confirmation windows: 2
initial task ceiling: 512
confirmation survivor ceiling: 96
confirmation task ceiling: 192
total task ceiling: 704
attempts per task: 2
wall-clock authority: 7,200 seconds
```

Every worker task still evaluates both `research_conservative` and `none` from
one shared observation stream.

## Generation

Generation is deterministic from the frozen preparation, generator version, and
random seed. Candidate IDs derive from the FuzzFolio program identity.

The three mutation families are equally first-class:

```text
entry_context
graph_structure
management_closure
```

De-novo proposals must draw from all available families. Seed-derived proposals
use one or two bounded mutations. The generator may alter thresholds, recency,
streaks, Boolean structure, position age, unrealized-R logic, sessions, initial
protection, targets, trailing, activation, and typed management actions.

Hard invalidity is not trading taste. A proposal is rejected before market
compute only for canonical schema/semantic/evaluator/fidelity failure, missing
entry behavior, missing first-class management, malformed authority, or a
program identity already proposed.

## Funnel Journal

Every proposal attempt is recorded with:

```text
proposal ordinal and source mode
seed identity
mutation family/path/before/after
raw source-profile identity
FuzzFolio validation status and issues
program identity when available
accepted, invalid, or duplicate disposition
```

The journal is deterministic and immutable. It lets the pilot distinguish a weak
generator from weak economics or missing evaluator capability.

## Progressive Screening

All 256 programs run on initial development windows A and C. The controller then
forms:

```text
economic archive: up to 64
novelty archive: up to 64
confirmation union: deterministic union capped at 96
```

The confirmation union runs on development windows B and D.

## Economic Archive

Economic selection is Pareto-based rather than one opaque score. It favors:

```text
total conservative net R
worst-window conservative net R
profitable-window count
smaller maximum drawdown
smaller cost drag
```

Candidates require the frozen minimum trade evidence. Pareto layers and stable
candidate identities make every inclusion inspectable.

## Novelty Archive

Novelty is deterministic farthest-first selection over normalized behavioral
fingerprints including:

```text
entry frequency and entry hour
exposure
holding duration
win rate
transition entropy
MFE and MAE
equity-curve shape
action distribution
close-reason distribution
state occupancy
graph and management complexity
```

Novelty is not profitability. It preserves potentially complementary behavior
that should not be deleted merely because it is not an immediate P&L leader.

## Resolved-Program Deduplication

The pre-market validator deduplicates source programs. After worker catalog
hydration, the controller also groups candidates by the resolved FuzzFolio
program identity. Only one deterministic representative of a resolved duplicate
may enter archives; every collapsed identity remains in the report.

## Artifact Boundary

The controller writes immutable:

```text
preparation.json
discovery-authority.json
generation-journal.json
population.json
initial preparation and authority
initial aggregate and selection
confirmation preparation and authority
final aggregate and report
manifest with checksums
```

The admitted finite controller continues to own Gateway tasks, checkpoint/resume,
redelivery, materialization-before-acknowledgement, and result storage.

## Interpretation Boundary

The 256-program pilot is a search-admission experiment. It tests generation,
validation, deduplication, progressive selection, distribution, determinism,
performance, and artifact growth. It is not sufficient evidence that the grammar
can or cannot discover durable profitable strategies.

The pilot must report separately:

```text
proposal invalidity
duplicate collapse
insufficient trading evidence
worker/evaluator failure
runtime management rejection
poor economics
novel but uneconomic behavior
```

## Stop Boundary

After the final pilot report, stop before:

```text
5,000-program discovery
new evidence windows
reserved/protected evidence
portfolio construction
automatic promotion to production
```
