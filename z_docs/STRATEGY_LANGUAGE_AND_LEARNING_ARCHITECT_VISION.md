# FuzzFolio Strategy Language and Learning Architect Vision

## Status

This document records a possible future direction for FuzzFolio. It is not an
active implementation plan, an authorization to access reserved evaluation
data, or a reason to interrupt the current Phase 3 PlayHand campaign.

The immediate commitment remains unchanged: complete the first policy-honest
10,000-lane Phase 3 campaign, freeze and audit its corpus, and learn what the
current fuzzy-scoring strategy grammar can and cannot produce. That evidence
should determine whether this vision becomes a roadmap, is narrowed to a
smaller experiment, or remains deferred.

This vision complements `DEFERRED_ML_RESEARCH_DIRECTIONS.md`. That document
covers practical early uses of machine learning. This one captures the larger
idea: an inspectable strategy language and a constrained learning architect
capable of assembling temporal trading systems from reusable parts.

## The Project Arc

FuzzFolio grew from a recurring manual workflow. A promising indicator would
appear in a forum, article, or MQL5 project; it would be coded into a new EA;
the EA would be tested and optimized; and the idea would then be discarded,
mutated, or replaced by the next promising indicator.

Reusable MQL include files made implementation faster, but each strategy still
needed brittle custom logic. Indicator switches and shared EA scaffolding
helped, yet did not solve the cost of expressing and testing new ideas.

Fuzzy scoring was the major abstraction breakthrough. It allowed several
imperfect signals to contribute to one decision without requiring a new nest
of Boolean rules for every experiment. That made large-scale composition and
optimization possible and eventually became the foundation of FuzzFolio.

Fuzzy scoring remains useful, but it is also a constrained strategy language.
It is best at combining evidence that exists at approximately the same
decision point:

```text
indicator evidence -> weighted score -> threshold -> trade decision
```

Many real trading ideas have another dimension that this representation does
not express naturally: time and state. They depend on one event preparing a
setup, another confirming it later, and position-management behavior changing
after entry. They include cancellation, timeout, cooldown, partial closure,
stop movement, trailing activation, and different rules at different stages
of a trade.

The proposed next abstraction is not a rejection of fuzzy scoring. It is a
larger language in which fuzzy scoring becomes one powerful building block.

## Core Thesis

FuzzFolio could eventually represent a strategy as a typed, constrained,
temporal program. A learning system would not emit an opaque buy/sell model.
It would act as a learning architect that assembles an inspectable strategy
from an approved kit of parts.

The output would be a frozen strategy program that can be:

- read and explained;
- rendered as a state diagram;
- edited by a human;
- replayed deterministically;
- compared with ordinary PlayHand strategies;
- subjected to the same evidence and tail controls;
- compiled or interpreted for live execution;
- rejected when too complex, fragile, or dependent on one regime.

The aspirational result is machine-assisted discovery without surrendering
causality, auditability, risk limits, or human ownership.

## A Temporal Strategy Language

A strategy program could be modeled as a finite-state machine or typed graph:

```text
FLAT -> WATCHING -> ARMED -> POSITION_OPEN -> PROFIT_MANAGEMENT -> COOLDOWN
```

Each state would define the observations it may use, legal transitions, risk
limits, clocks, and actions. A transition might require Boolean evidence,
fuzzy evidence, a temporal sequence, or a combination of them.

### Feature and evidence primitives

- Raw and normalized price-derived features
- Existing FuzzFolio indicators
- Cross-timeframe context
- Fuzzy weighted evidence groups
- Regime or volatility state
- Session and calendar context
- Spread and execution-cost state
- Position state and unrealized return in R
- Bars elapsed in the current state or trade

### Temporal primitives

- Event A occurred before event B
- A condition persisted for N bars
- A confirmation must arrive within N bars
- A setup expires after N bars
- A transition requires a rising or falling sequence
- A failed transition activates a different branch
- A cooldown prevents immediate re-entry

### Trading and management actions

- Arm or cancel a setup
- Enter long or short
- Place a bounded protective stop
- Set or revise a profit target
- Reduce a position by a constrained fraction
- Move to break-even under explicit rules
- Activate one of a limited set of trailing policies
- Exit on invalidation, timeout, or risk limit
- Enter a bounded cooldown state

### Structural safety rules

The language should make invalid strategies difficult or impossible to
express. Examples include:

- no future or forming-bar leakage unless explicitly modeled;
- bounded exposure and position count;
- a protective risk policy present before entry;
- no impossible same-bar action ordering;
- no unbounded loops or state proliferation;
- limited temporal depth and total complexity;
- typed compatibility between features, guards, and actions;
- explicit timeframe and data-availability requirements;
- deterministic execution semantics shared by research and live runtimes.

## Example Strategy Program

A human-readable program might express the following:

1. Enter `WATCHING` when higher-timeframe trend evidence exceeds a fuzzy
   threshold.
2. Enter `ARMED` when lower-timeframe volatility compression persists for at
   least 12 bars.
3. Enter a position only if a breakout and momentum confirmation occur within
   the next 8 bars.
4. Cancel the setup if trend evidence reverses or the confirmation clock
   expires.
5. Reduce the position at +1R.
6. Enter `PROFIT_MANAGEMENT` only after a second impulse confirms continuation.
7. Trail the remainder using a selected volatility policy.
8. Exit on structural invalidation, trailing stop, or maximum holding time.
9. Enter a 20-bar cooldown after a losing trade.

This is recognizable as ordinary strategy logic, represented in a form that
software can safely compose, mutate, replay, compare, visualize, and edit.

## The Learning Architect

The preferred learner would construct or revise strategy programs. It would
not initially receive unconstrained control of every trade on every bar.

Several search methods could operate against the same language:

- grammar-guided random search;
- evolutionary or genetic programming;
- quality-diversity and novelty search;
- Bayesian or surrogate-guided proposal generation;
- hierarchical reinforcement learning;
- combinations in which simpler search establishes the baseline and RL must
  demonstrate additional value.

### Constrained toolkits

The first learners should not receive the entire indicator and action catalog.
Different populations could receive coherent kits such as:

- mean reversion, compression, timeout, and partial-exit tools;
- trend, pullback, momentum, and trailing-stop tools;
- breakout, failed-breakout, and reversal tools;
- cross-timeframe context with conservative entry and management tools.

This makes search tractable and encourages distinct behavioral families.
Later experiments could permit carefully selected cross-family composition.

Quality-diversity search is especially relevant. The goal should not be one
global historical winner. It should be a population of meaningfully different
systems occupying different behavioral niches: short and long holding periods,
trend and reversion behavior, selective and active strategies, or strategies
that survive different regime families.

### Where reinforcement learning may fit

Three levels are conceivable:

1. **Direct policy:** the learner chooses legal actions at each bar. This is
   powerful but difficult to interpret and most vulnerable to memorization or
   simulator exploitation.
2. **Strategy architect:** the learner assembles a frozen temporal program,
   which is then evaluated conventionally. This is the preferred starting
   point.
3. **Hierarchical hybrid:** an outer learner selects structure and modules,
   while a constrained inner policy chooses transitions within that structure.
   This is a possible later destination after frozen programs are understood.

The learner may be neural, evolutionary, symbolic, or hybrid. The important
architectural choice is that its output and legal behavior are constrained by
the language and the evidence contract.

## Data, Honesty, and the Reality Gap

This idea is more credible now because FuzzFolio has infrastructure that did
not exist during earlier experiments:

- a coherent five-year market-data lake;
- high measured parity between replay and live execution behavior;
- explicit cost, spread, and slippage assumptions;
- deterministic Rust-backed replay;
- distributed compute;
- immutable plans, receipts, and data identities;
- rotated development windows and an untouched outer tail;
- records of successful, failed, and nonviable research outcomes.

More history and more instruments make accidental fitting harder, but raw bar
count is not independent sample count. Bars, instruments, and FX crosses are
correlated. A flexible learner can test enough hypotheses to consume a large
historical dataset. More data therefore helps only when search capacity,
complexity, and evaluation feedback are also controlled.

A strategy architect should be evaluated on a distribution, not one aggregate
return. Its objective should favor:

- median performance across rotated windows;
- lower-tail and worst-window survival;
- instrument and regime breadth;
- sufficient independent trades;
- cost and slippage tolerance;
- stability under threshold and price perturbation;
- smooth degradation rather than performance cliffs;
- behavioral diversity relative to retained strategies;
- simple programs over unnecessarily complex ones.

It should penalize turnover, sparse lucky outcomes, excessive state count,
fragile temporal precision, one-instrument dependence, and strategies whose
performance disappears under modest execution perturbations.

The untouched tail remains consumable evidence. Once viewed or used to change
the learner, language, reward, or selection rule, it is no longer untouched.
Prospective paper and live observations remain irreplaceable because markets
are nonstationary and no amount of history guarantees the next regime.

The aim is not to prove that the future repeats. It is to create strategies
that remain useful, or at least fail gracefully, when it does not.

## Human Editing and Future Product UX

An opaque neural policy would be extremely difficult to explain or edit. A
typed strategy program creates a natural product model for a future FuzzFolio
interface.

Possible views include:

- a state and transition diagram;
- human-readable strategy pseudocode;
- editable evidence and fuzzy-score groups;
- transition guards and timeout controls;
- position-management timelines;
- per-trade explanations showing the active state and triggering evidence;
- validation overlays showing which states contribute in each regime;
- complexity, fragility, and behavioral-similarity diagnostics.

The UI is meaningful but downstream. The language, semantics, replay parity,
and strategy evidence must be correct before investing in a visual editor.
If the underlying representation is clean and typed, the UI becomes difficult
product work rather than an unsolved conceptual problem.

## Exploration Sequence

### Gate 0: learn from current Phase 3

Complete and audit the 10,000-lane fuzzy-scoring campaign. Determine whether
the current grammar produces robust strategies, where it repeatedly fails,
and which limitations are evidenced rather than merely suspected.

### Gate 1: language prototype

Define a small typed schema and deterministic interpreter. Manually encode a
few known strategy archetypes and prove replay parity. Do not introduce RL yet.

### Gate 2: simple search baseline

Use constrained random, evolutionary, or quality-diversity search to assemble
small programs. Establish what useful behavior the language can express and
how often search produces invalid, redundant, or fragile programs.

### Gate 3: learning architect experiment

Train a learner to propose frozen programs from constrained toolkits. Compare
it prospectively with the simpler search baseline under the same compute,
data, complexity, and evidence budget.

### Gate 4: hierarchical adaptation

Only if frozen learned programs demonstrate robust value, test constrained
state-transition policies or regime-conditioned expert selection. Preserve
inspectability and hard risk limits.

### Gate 5: product and deployment

Build human editing, visualization, compilation, and controlled live canaries
only after the representation and discovery process survive strict unseen
evaluation.

## Success and Stop Conditions

This direction earns continued investment if it produces strategies that are
more temporally expressive and measurably more robust than the current grammar
without requiring unbounded complexity.

It should be narrowed or stopped if:

- simple evolutionary search performs as well as the RL layer;
- apparent gains vanish under untouched evaluation;
- learned programs rely on excessive states or precise historical timing;
- simulator perturbations destroy performance;
- outputs cannot be explained or reproduced;
- infrastructure complexity grows faster than demonstrated research value.

An RL component is not the objective. Profitable, defensible, and deployable
trading behavior is the objective. The strategy language is valuable even if
RL fails because it can improve manual authoring, PlayHand search, evolutionary
discovery, replay explanation, and eventual FuzzFolio UX.

## The Long-Term Hope

The long-term hope is a system that can explore trading ideas with more
imagination than a fixed indicator template while remaining more disciplined
than unconstrained machine learning.

It would combine the original spirit of building an EA around a concrete idea,
the composability unlocked by fuzzy scoring, the temporal expressiveness of a
real trading state machine, and the scale of modern learning and replay
infrastructure.

The machine would search. The evidence system would judge. The resulting
strategy would remain something a human can understand, alter, reject, and
ultimately choose to trust.
