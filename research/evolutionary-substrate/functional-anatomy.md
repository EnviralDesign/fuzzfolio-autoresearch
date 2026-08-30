# Canonical Evolutionary Substrate — functional anatomy

This is a source-only anatomy, not a claim about profitability, historical
activation, or retained outcomes. It is pinned to AutoResearch
`51c2f9175f441166e7fc997109e939a9f9103b5d` and the read-only FuzzFolio engine
`2bd50ccb3af1700d286da88cbcaecb4aca24f1a2`. The generated
[`capability-ledger.json`](capability-ledger.json) binds every record to exact
file hashes.

The relevant construction route is:

```text
sealed authority → v5 operator selection → side-local genome transform
  → fresh bidirectional compile/admission → runtime graph → execution trace
  → candidate/archive-level outcome
```

It is deliberately not a component-credit route. The last arrow does not
identify a guard, topology edit, indicator, side, or action as causal.

| Functional region | Existing implementation | Evolution can manipulate | Fixed or unresolved boundary |
| --- | --- | --- | --- |
| Sensory | 88 catalog indicators and 7 catalog timeframes | Resource family, subject to frozen authority and compiled admission | Catalog presence is not frozen-authority eligibility or activation |
| Memory / temporal | Runtime fact history supports event age, streaks, predicate edges and action cooldowns | Typed grammar directly emits only a subset; V5 topology mutates graph regions | Several runtime guards have no direct grammar/seed route |
| Decision composition | 23 typed fragments, 7 ports, bounded graph build | V2 seed/fragment compilation; V5 topology add/remove/rewire | Fragment-to-V5 mutation equivalence is not claimed |
| Internal state | Ready/watch/pending/position/recovery graph states | Topology operations can insert/remove/rewire supported regions | No explicit capture/latch primitive identified |
| Actions / actuators | 8 next-open action models, execution status/reason transitions | Fragment/topology action paths and V5 family-specific transforms | Requested, applied, rejected, and closed are distinct observables |
| Homeostasis / risk | Management library, initial protection, hold policy, exits | Dedicated v5 hold and initial-protection families, selected scalar management paths | Dynamic portfolio/risk policy evolution is not established |
| Development | Typed grammar and V5 topology compiler/admission pipeline | Grammar seed, authority-bound one-step transforms, same-side crossover | No multi-step behavioral construction was launched here |
| Evolutionary credit / memory | Candidate IDs, parent references, plans, receipts, journals, archive reducer | Candidate/operator lineage is persisted | No component-local or side/portfolio causal credit path is established |
| Ecological / portfolio | Candidate/archive and portfolio code exist elsewhere | None established in this source-only atlas | Portfolio effects and cross-candidate credit require a later authorized study |

## 1. Sensory surface

The FuzzFolio catalog at `shared/constants/indicators.json` contains 88 named
indicator records and 7 timeframes. The v5 resource operator is implemented in
`rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs` as
`evolvable_resource_v1`; its static authority binds a catalog and timeframe
policy before it can construct a child.

- Existing: catalog metadata, indicator bindings, evidence groups/events and
  a resource-bound operator surface.
- Evolvable: resource selection/modification only after the sealed authority,
  catalog policy, and fresh compiler admission all agree.
- Fixed: a run's frozen policy is not reconstructed from the global catalog.
  This audit therefore does not call every catalog item “active.”
- V37/V38 use: no run artifacts were read. The source comparison shows the
  same 23-fragment grammar at both listed AutoResearch commits; historical
  catalog activation remains `unavailable_no_run_corpus_read`.
- Failure evidence: the ledger labels catalog membership, authority inclusion,
  compilation, activation, retention, and selection as distinct states.
- Unknown: exact indicator/timeframe membership in a particular frozen
  campaign authority, and all indicator-level credit.

## 2. Memory and temporal logic

The runtime's `guards.py`, `fact_history.py`, and `kernel.py` implement guards
for event age, condition streak, predicate edge, consecutive true, action
cooldown, state age, execution status/reason, boolean composition, and UTC
time windows. These are runtime vocabulary facts, not automatically reachable
evolutionary genes.

- Existing: 22 guard models and fact-history machinery.
- Evolvable directly: the typed grammar emits level, fresh event, event-age
  at-most, state-age-at-least, streak, rising predicate edge, position,
  unrealized-R, and execution-status paths.
- Fixed: the grammar's predicate edge is rising-only; its watch/abort timeout
  is fixed in the compiler shape rather than a general abstention policy.
- V37/V38 use: unavailable without opening run records; no inference is made
  from source presence.
- Failure evidence: `gap-matrix.json` confirms direct-route gaps for
  `utc_time_window`, `any`, `not`, `consecutive_true`, `event_age_window`,
  `action_cooldown_elapsed`, `state_age_at_most`, and `execution_reason_is`.
- Unknown: whether a missing direct route was deliberately suppressed by an
  operator policy or simply absent from its source vocabulary.

## 3. Decision composition

`grammar.rs` owns the sealed registry of 23 typed fragments. Its seven ports
are `Ready`, `Watch`, `EntryPending`, `PositionIdle`, `ManagementPending`,
`ExitPending`, and `Recovery`. Each fragment has resource closure, finite
choice domains, a maximum count, and an activation recipe.

- Existing: arming, gates, entries, management, exits, and cooldown compose
  into a bounded state/transition graph.
- Evolvable: V2 seeds select fragments; V5 topology has a separate graph
  program and 14 sealed operations.
- Fixed: grammar budgets cap states, transitions, groups, events, indicators,
  and guard depth. The grammar deliberately delegates native validation and
  bidirectional compilation at the boundary.
- V37/V38 use: source evidence only—both historical snapshots have 23
  fragments. Authored/compiled/activated/reduced/selected counts are not
  available without a prohibited run-corpus read.
- Failure evidence: compiled child admission is explicit in
  `v5_operators.rs`; a structurally valid edit may still fail after compiler
  re-admission.
- Unknown: distribution of fragment types among historical parents or
  survivors.

## 4. Internal state and graph topology

`v5_topology_operators.rs` exposes 14 sealed one-step transforms: setup,
entry branch, confirmation/rejection, timeout rearm, management region, and
exit region inserts/removes/rewires where appropriate. Every applied plan is
parent-hash-bound and re-admitted.

- Existing: stateful entry, position, management, exit, and recovery regions.
- Evolvable: topology mutation can add, remove, or rewire only enumerated
  regions; it is not an arbitrary graph editor.
- Fixed: all enumerated candidate placement guards are neutral `always` in
  the topology surface; temporal/guard semantics are supplied elsewhere.
- V37/V38 use: unavailable; there is no claim that a source-supported topology
  operator produced a historical child.
- Failure evidence: topology's own source matrix says unsupported plan shapes
  are rejected and children are post-transform validated.
- Unknown: minimum coordinated edit distance for a desired semantic phenotype;
  it depends on a concrete parent/authority pair.

## 5. Actions and actuators

FuzzFolio defines eight action models: enter, exit, move stop to break even,
tighten stop, set/cancel target, and activate/deactivate trailing—all at next
open. Grammar compilation also creates execution edges for filled, rejected,
canceled, applied, and closed states.

- Existing: action contracts and execution traces distinguish intent from
  execution outcome.
- Evolvable: matching fragment/topology regions can request actions; v5
  initial-protection and hold families are separate authority-bound families.
- Fixed: no source-only evidence supports action-local selection credit.
- V37/V38 use: unavailable without run records.
- Failure evidence: the reference fixture suite validates an evaluator-bound
  candidate and rejects a clock-only/non-management search candidate.
- Unknown: whether individual management actions were ever activated or
  retained in a historical run.

## 6. Homeostasis, risk, and management

Initial stops/targets and management libraries live in FuzzFolio management
models. The Rust layer contains `evolvable_hold_policy_v1`,
`evolvable_initial_protection_v1`, and a scalar dynamic-management operator
surface, all bound to a fresh compiled profile.

- Existing: initial protection, management plan library, hold choices,
  explicit exits, and selected dynamic management action paths.
- Evolvable: only the sealed v5 family transforms and their admitted child
  forms; static source does not prove operator sampling probabilities.
- Fixed: portfolio-wide capital allocation, shared risk budgeting, and
  selection policy are outside this local organism language.
- V37/V38 use: source-only unavailable.
- Failure evidence: the compiler refuses a stale compiled profile for a new
  program and treats rejected/no-op plans as first-class outcomes.
- Unknown: behavioral effect size and adverse selection pressure of each
  management mutation.

## 7. Development and construction

The native bridge (`temporal_qd_v5_native.py`) freezes execution authority and
receipts. `v5_operators.rs` then applies one side-local transform and requires
fresh pair recompilation before a subsequent evolved step.

- Existing: G0 construction, one-step later-generation mutation, same-side
  crossover, compiler/native validation, identity binding, and journal forms.
- Evolvable: resource, temporal, topology, hold, initial-protection, and
  same-side crossover family selection.
- Fixed: no construction/generation was run in Stage 4.5A; no operator
  calibration or policy was changed.
- V37/V38 use: source comparison only. V38 adds audit/report support relative
  to V37 but does not alter the listed grammar file.
- Failure evidence: every v5 plan carries a parent source hash and rejects
  authority or post-transform admission drift.
- Unknown: acceptance/no-op/rejection rates by parent, family, site, and
  coordinated neighborhood.

## 8. Evolutionary credit and memory

The Rust journal/proposal/v5 layers persist candidate identities, parent
references, plan receipts, disposition/reason fields, compact records, and
archive-facing projections. This is useful lineage observability, but it is
not causal attribution.

- Existing: candidate/operator lineage, compiler receipts, source identities,
  and runtime action/transition traces.
- Evolvable: operator choice is explicitly recorded and can be replayed under
  the sealed authority.
- Fixed: V4's accepted conclusion remains intact—there is no universal,
  context-free component surrogate score.
- V37/V38 use: authored vs compiled vs activated vs reduced vs selected is
  `unavailable` here because the source-only boundary forbids opening those
  run artifacts.
- Failure evidence: a later step cannot silently reuse a stale compiled
  profile after a genome mutation.
- Unknown: parent-conditioned, route/site-conditioned, suppression-aware
  credit; side credit; portfolio credit; learned operator priors.

## 9. Ecological and portfolio layer

AutoResearch contains archive and portfolio systems, but this source-only
atlas treats them as downstream context rather than an organism-local
evolutionary actuator.

- Existing: candidate/archive reducers and portfolio research code paths.
- Evolvable: none asserted in this atlas.
- Fixed: archive insertion, selection weighting, quality/risk/cost policy,
  and portfolio mechanisms were not modified or exercised.
- V37/V38 use: unavailable without a separately authorized artifact audit.
- Failure evidence: absence of component/side/portfolio credit fields in the
  source-bound ledger is intentional, not backfilled by an economic proxy.
- Unknown: cross-organism competition, coexistence, and portfolio-level
  externalities.
