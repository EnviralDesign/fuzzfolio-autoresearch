# Temporal QD next-search and optimization plan

Date: 2026-08-05

Status: pre-optimization semantic checkpoint implemented and locally admitted;
checkpoint commit/push pending. Optimization and economic launch remain pending.
No new economic search is authorized by this document.

## Outcome sought

The next temporal QD campaign must search a materially richer strategy space
and select on evidence spread across regimes rather than repeatedly optimizing
the same narrow calendar. Before that campaign, the local single-coordinator
phases must be made fast enough that a larger bootstrap and multi-generation
run are operationally practical on a workstation.

The work has two deliberately separated checkpoints:

1. implement and admit the new search semantics, then commit and push a clean
   cross-repository checkpoint;
2. perform a measured, more ambitious optimization pass against that frozen
   semantic oracle.

The distributed replay middle remains outside the optimization scope. It is
already parallel and scales by adding compatible workers. The targets are the
local proposal/construction front and the local consolidation/archive tail.

## Preserved evidence and abandoned run

The broad run at:

```text
C:\fuzzfolio-research\temporal-qd-1024x5-20260804-v3\run\broad-1024x5
```

is retained as diagnostic evidence. Generations 1 through 3 are complete and
immutable. Generation 4 stopped before evaluation when the workstation guard
observed less than 12 GiB host-available memory during candidate construction.
The process tree peaked near 5.9 GiB RSS; the stop was caused by whole-host
headroom, not the separate 8 GiB process-tree ceiling. Generation 4 and 5 do
not provide economic evidence.

This run will not be resumed. It is not deleted, promoted, or treated as a
failed admission. Its completed generations remain useful for regression,
performance, diversity, and search-policy analysis.

## Decision 1: initial protection is part of the genome

The current bidirectional pair path leaves every initial stop at 1 percent and
every initial target at 2R. The execution engine already supports richer
catalog-backed protection, but the pair mutation authority does not expose it.
The next authority must make the initial management plan heritable and
mutable.

### Stop-locator gene

Admitted initial modes:

- fixed percent;
- indicator/ATR distance multiple;
- indicator price level when the catalog contract provides a safe scalar
  price locator.

Initial fixed-percent grid:

```text
0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00 percent
```

Initial distance-multiple grid:

```text
0.50, 0.75, 1.00, 1.50, 2.00, 2.50, 3.00, 4.00
```

### Target-locator gene

Admitted initial modes:

- reward multiple coupled to the resolved entry risk;
- fixed percent independent of the stop distance;
- indicator/ATR distance multiple;
- indicator price level when safely catalog-backed;
- no fixed target, leaving the position to graph-authored exits and management.

Initial coupled reward grid:

```text
0.25R, 0.50R, 0.75R, 1.00R, 1.50R, 2.00R, 3.00R, 4.00R, 6.00R
```

Initial independent target-percent grid:

```text
0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00, 5.00 percent
```

Sub-1R targets are valid research subjects. A literally negative reward
multiple remains invalid because a target on the loss side of entry is a stop
or discretionary loss exit, not a profit target.

### Mutation and construction policy

- Management is mutated as a coherent plan, not as unvalidated JSON fields.
- Most scalar mutations move to an adjacent grid value.
- A smaller share make a wider deterministic jump.
- A rarer mutation changes locator kind.
- G0 construction is stratified across coupled, decoupled, dynamic, and
  no-target plans so a valid but smaller subspace cannot disappear by chance.
- Crossover swaps coherent management components and must pass the unchanged
  native FuzzFolio validator.
- Existing break-even, trailing, target cancellation, target replacement,
  stop tightening, and discretionary exit machinery remains available.

The search must not hard-reject a large-stop/small-target strategy merely for
its reward ratio. Conservative costs, cost-to-target drag, effective resolved
R, drawdown, trade support, and cross-window stability make that design prove
itself. If it becomes an evolutionary sink despite weak robustness, later
policy may add a soft diversity or dominance pressure, but this plan does not
preemptively outlaw it.

## Decision 2: deterministic rotating development panels

The three fixed one-month 2021 discovery windows are retired for the next
campaign. The pre-tail calendar becomes a development and cross-validation
universe. The outer tail is the only evidence labelled untouched.

With roughly five years of history, four years of evolutionary exposure cannot
also coexist with disjoint 12-month and 36-month holdouts plus another outer
tail. Therefore the 12-month and 36-month stages remain valuable post-funnel
scrutiny, but they are not described as pristine holdouts when their dates
overlap the development universe.

For a four-year development universe divided into year and quarter cells, the
primary rotation is a Latin-square schedule:

| Panel | Year 1 | Year 2 | Year 3 | Year 4 |
| --- | --- | --- | --- | --- |
| 1 | Q1 | Q2 | Q3 | Q4 |
| 2 | Q2 | Q3 | Q4 | Q1 |
| 3 | Q3 | Q4 | Q1 | Q2 |
| 4 | Q4 | Q1 | Q2 | Q3 |

Every row contains four separated three-month blocks, or twelve total
candidate-months. Every candidate within one generation sees exactly the same
row. The schedule is materialized before launch, identity-bound, deterministic,
and cannot be changed by live results.

Campaigns longer than four generations use an authority-defined deterministic
continuation schedule. They must not silently draw unrestricted random windows
or reuse a panel without recording that reuse in campaign identity.

## Decision 3: fair current-panel and cumulative breeder evidence

Changing the panel each generation makes stale parent scores incomparable to
new child scores. Selection therefore maintains two separate evidence views:

- current-panel evidence for fair parent/child comparison;
- cumulative development evidence over every panel a candidate has been
  required to survive.

At each generation:

1. evaluate the new population and retained breeding candidates on the same
   current panel;
2. form a provisional, diversity-preserving survivor set from the current
   panel;
3. backfill provisional new survivors over earlier exposed panels;
4. compare breeding candidates only after they possess the same required
   cumulative panel coverage;
5. freeze the resulting archive and its evidence-coverage identity before
   constructing the next generation.

The provisional/backfill width is configurable and frozen. The initial target
is 128, subject to an admission check that it preserves enough occupied
behavioral cells. Cached immutable results are reused by content identity.

Selection uses conservative after-cost results. Aggregate return alone is not
enough. The archive must preserve per-panel results and include robust measures
such as median block result, worst-block result or lower-tail pressure, trade
support, drawdown, cost drag, long/short activation, and behavioral novelty.
No-cost results remain diagnostic.

The initial frozen robust-breeder defaults are:

- exact cumulative evidence coverage is mandatory;
- at least 4.0 closed trades per candidate-month on average across the required
  coverage;
- activity in at least 75 percent of required windows, without requiring every
  quarter to trade;
- quality lane: cumulative conservative net R greater than zero and median
  window conservative net R greater than zero;
- worst-window R, drawdown, cost drag, and novelty remain Pareto/objective
  dimensions rather than absolute nonnegative gates;
- a same-support frontier lane may occupy at most 20 percent of breeder width.

When quality is sparse or empty, bounded frontier parents remain available and
the unfilled proposal share becomes immigrants. The system must not silently
fall back to an all-immigrant generation merely because no candidate was
nonnegative in every historical quarter.

## Decision 4: configurable 4,000-to-1,024 G0 bootstrap

The next default campaign constructs 4,000 valid bidirectional immigrants and
deterministically reduces them to a 1,024-candidate G1 evaluation population.
Both numbers are explicit CLI and frozen-authority values rather than hidden
constants.

The bootstrap reduction is pre-economic. It may use:

- exact source/native validity;
- deterministic synthetic or no-market liveness already admitted by the
  repository;
- semantic uniqueness;
- graph topology and route shape;
- indicator/fuzzy-group composition;
- long/short activation potential;
- hold-policy shape;
- initial management-plan mode and scalar bucket.

It must not read historical P&L, worker results, reserved evidence, or the
outer tail. Selection is diversity-balanced rather than a top-return ranking.
All 4,000 construction records remain auditable, while only the selected 1,024
enter distributed market evaluation.

Every constructed semantic identity enters the campaign-wide construction
ledger, including candidates not selected for G1 evaluation. This prevents an
exact rejected bootstrap duplicate from consuming a later proposal slot and is
recorded explicitly in the selection artifact.

The population finalizer, Python reference path, Rust fast path, restart
checkpoint, and authority identities must distinguish construction-pool size
from evaluation-population size. A resume cannot change either value.

## Decision 5: instrument scope

The next controlled campaign remains EURUSD-only so the protection and calendar
changes can be interpreted without also changing the market universe.

The present temporal QD authority is single-instrument. The replay substrate can
execute any supported instrument, but the search materializer and reducer do
not yet aggregate one genome across several instruments. Before the first
trade-intended serious campaign, the orchestration layer must support:

```text
one portable strategy genome
  -> candidate x instrument x historical-window tasks
  -> one candidate record with per-instrument and aggregate evidence
```

The configured universe may contain one, several, or all approved instruments.
The design must retain instrument specialists as well as portable strategies;
it should not require every strategy to be profitable on every market. This is
the milestone immediately after the next EURUSD learning campaign, not part of
the current implementation batch.

## Repository ownership

Trading-Dashboard/FuzzFolio remains canonical for:

- authored and resolved temporal-graph models;
- catalog hydration and indicator execution;
- native validation;
- aligned observations;
- execution, costs, and R accounting;
- management-effect semantics.

AutoResearch owns:

- proposal scheduling and mutation policy;
- bootstrap diversification;
- historical-panel scheduling;
- task fan-out and evidence aggregation;
- breeding/archive policy;
- research artifacts and restart orchestration;
- performance-oriented Rust extensions for those orchestration functions.

No AutoResearch-specific duplicate execution engine, indicator evaluator, or
management interpreter may be added to Trading-Dashboard. Shared canonical
logic may be imported or exposed through a narrow API. If a performance change
must touch Trading-Dashboard, it must accelerate the canonical path used by the
rest of FuzzFolio and preserve its public semantics.

## Pre-optimization implementation checkpoint

The semantic batch is complete only after all of the following pass:

1. management kinds and scalar grids are reachable in immigrant and offspring
   construction;
2. coupled and decoupled plans pass real Dashboard validation and execution
   geometry tests;
3. sub-1R and no-target plans are covered without weakening cost accounting;
4. rotating panels are deterministic, non-clumped, identity-bound, and
   restart-exact;
5. current-panel parent comparison and cumulative survivor backfill cannot mix
   unequal evidence coverage;
6. the 4,000-to-1,024 bootstrap is configurable, deterministic, diversity-aware,
   and pre-economic;
7. Python/Rust finalizer parity remains exact for the new population shape;
8. corruption, partial-write, stale-authority, and resume mismatch cases fail
   closed;
9. focused AutoResearch and affected Dashboard suites pass;
10. no market experiment is needed for semantic admission.

After review, all affected repositories are committed and pushed as one named
checkpoint. The working trees must be clean except for the user's accepted
generated market-structure file.

### Local admission evidence

The pre-optimization semantic checkpoint passed its local no-market admission
on 2026-08-05:

- the complete affected Temporal QD, graph, authority, funnel, indicator,
  reachability, and structural-operator surface passed: `338 passed`;
- the G0 contract file passed `46` tests, including the independently timed
  4,000-to-1,024 compact selection/resource gate;
- the real EURUSD/M5 64-to-32 harness completed G1 and G2 through the rotating
  supervisor and generic generation funnel, then reopened an exact restart with
  unchanged G1 journal bytes;
- the Rust population finalizer passed `3` native tests plus adversarial path,
  symlink, pool, selection, ledger, and selected-reference probes;
- frozen `uv sync`, Python compilation, installed CLI help, and
  `git diff --check` passed;
- independent review approved the final capacity, restart, proof-authority,
  selected-subset, and immutable-publication contracts.

For transparency, the repository-wide historical sweep reported `1746 passed`
and `11 failed`. Ten failures are in untouched PlayHand Lab contract tests; the
remaining failure consumes an ignored stale `.tmp` pre-broad population whose
program schema is no longer canonical. None is in the modified checkpoint
surface, and this record does not claim the unrelated historical suite is fully
green.

## Clean-break optimization detour

Optimization begins only from the committed semantic checkpoint. It is allowed
to replace large AutoResearch modules rather than trim isolated Python calls,
but every replacement remains reversible until admitted.

### Measured architectural target

The preserved G1-G4 telemetry shows that the admitted Rust population finalizer
is no longer the limiting seam. It completes in tens of seconds. The dominant
cost is repeated expansion, cloning, hashing, persistence, reloading, and native
revalidation of multi-megabyte Python candidate documents:

- G2 proposal construction consumed about 121 minutes inclusive;
- G3 resume spent about 83 minutes semantically replaying 2,319 proposals;
- proposal journals grew to 6-7 GiB in G2/G3;
- rich populations and archives each grew beyond 2 GiB;
- median proposal entries were about 1.7 MiB, with p99 near 8.5 MiB;
- local post-worker consolidation and next-generation preparation added tens
  of minutes.

The primary reconstruction is therefore a versioned content-addressed genome
and archive store:

- store immutable catalog, authority, profile, module, pair, and program
  objects once by content hash;
- journal compact proposal deltas and references rather than repeated complete
  parents and children;
- retain compact archive members with candidate reference, evidence aggregate,
  descriptor, objectives, lane, and lineage references;
- hydrate a rich portable candidate only for evaluation, export, or explicit
  audit;
- preserve the exact canonical Dashboard-authored source profile and program.

The second reconstruction is sealed append-only proposal state: hash-chained
segments plus periodic compact checkpoints containing accepted references,
identity indexes, scheduling counters, and the journal head. Resume validates
the checkpoint and its tail rather than re-executing every prior mutation and
native validation.

Only after those representation and restart changes establish a compact typed
boundary should the proposal kernel and cumulative archive reducer move into
larger Rust/PyO3 modules. Porting the current multi-gigabyte representation
directly would accelerate the wrong architecture.

### Baseline

- Freeze representative 64-, 128-, and production-shape 1,024-candidate inputs.
- Record end-to-end wall time, main-thread CPU, telemetry CPU, peak process-tree
  RSS, host headroom, bytes read/written, and artifact sizes.
- Measure proposal construction and generation consolidation separately.
- Use fresh interpreter/process-tree runs to avoid Python allocator residue.

### Front-half targets

Rank and investigate complete module boundaries, including:

- indicator-operation enumeration and plan construction;
- graph/module mutation and structural deduplication;
- candidate materialization and canonical hashing;
- identity-ledger lookup and proposal-journal persistence;
- population projection and diversity reduction.

The expected implementation sequence is:

1. content-addressed compact objects and archive references;
2. append-only identity/proposal segments and sealed resume checkpoints;
3. a module-scale batched proposal kernel;
4. one-pass population, evaluation projection, and construction-distribution
   finalization;
5. compact cumulative-evidence and QD archive reduction;
6. bounded generation manifests that prevent supervisor state from growing
   with embedded historical result descriptors.

Rust/PyO3 is preferred for CPU-bound loops, immutable canonical transformations,
hashing, and compact typed data. Python remains the orchestration layer and the
semantic oracle until parity is proven. Crossing the language boundary for many
tiny calls is not a win; candidates or batches should cross as coherent typed
units.

### Tail targets

Rank and investigate complete consolidation boundaries, including:

- result reduction and qualification;
- behavior-descriptor calculation;
- Pareto/QD cell competition;
- cumulative-evidence joins and coverage checks;
- archive serialization and checkpoint finalization.

The distributed worker replay is not rewritten in this phase.

### Admission ladder for each replacement

1. establish an isolated old-path baseline;
2. prototype the narrow Rust boundary and stop early if it shows no material
   promise;
3. compare semantic results against the Python oracle;
4. prove exact 64/128/1,024 parity where byte identity is required and explicit
   semantic equivalence where representation intentionally changes;
5. prove uninterrupted, split/restart, legacy-to-new resume, corruption, and
   partial-journal behavior;
6. measure end-to-end gain, not microbenchmark gain alone;
7. switch the default only after independent review;
8. retain the old path until at least one complete generation passes at scale.

The optimization pass is successful only if it produces large measured local
wall-time gains without increasing workstation memory pressure, weakening
immutable evidence, changing search semantics, or creating split FuzzFolio
machinery.

## Launch boundary

No replacement search launches automatically after implementation or
optimization. Before launch, Codex reports:

- repository heads and clean state;
- exact authority and CLI settings;
- protection-mode and G0 diversity distributions;
- panel schedule and evidence-coverage rules;
- measured old/new construction and consolidation performance;
- memory/resource-guard settings;
- expected task count, candidate-months, worker fleet, and estimated cost.

The user then decides whether to launch the fresh EURUSD campaign.
