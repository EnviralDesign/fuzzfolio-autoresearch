# Static prior and one-step neighbourhood design

## Status: designed, not executed

No deterministic production-prior pilot was executed in Stage 4.5A. The
source contains construction and compile-only seams, but this checkout has no
predeclared, inspectable frozen authority fixture that binds a specific catalog,
timeframe policy, parent, and compiler receipt without opening historical run
artifacts. Building a replacement constructor would widen scope and could
silently become a new evolutionary authority, so it is explicitly not built.

This is a precise authority blocker, not a claim that the operator is
unexecutable. Existing source tests establish language/contract behavior;
they do not supply a valid population/authority sample for a static prior.

## Design for a later bounded no-market construction pilot

Preconditions: a read-only, self-contained frozen authority fixture plus one
or more parent records with current compiler receipts. No market data, replay,
worker, gateway, archive mutation, or policy mutation is permitted.

| Arm | Attempts | Sampling rule | Purpose |
| --- | ---: | --- | --- |
| Production prior | 4,000 maximum | Exact authority-bound production selection | Observe what the current operator vocabulary actually proposes |
| Coverage-balanced | 4,000 maximum | Equal predeclared family/site cells; no weight update | Detect reachable but low-frequency local neighborhoods |

The combined cap is 8,000 attempts, below the 10,000-stage limit. Both arms
must use the same source hashes, authority, parent set, compiler boundary, and
deduplication identity. They may not be aggregated into a learned prior or
used to alter weights.

Required per-attempt fields:

```text
arm, attemptId, parentId, parentRole, side, operatorId, suboperation,
site/route, disposition, reasonCode, sameProgram, childIdentity,
preCompileStructure, compilerReceipt, phenotypeSignature, graphComplexity,
resourceDelta, stateDelta, actionDelta, distanceFromParent
```

Required aggregate counts: attempted, accepted, no-op, rejected, duplicate,
unique authored, resolved/compiled, unique phenotype, family/suboperation,
guard/action/state/resource/timeframe/indicator coverage, complexity, side,
and reason code. Any missing compiler receipt must be reported as a separate
state rather than treated as rejection or activation.

## One-step neighbourhood design

For each predeclared host parent, enumerate or select exactly one sealed
operator step at a time. Report the following classifications without making a
behavioral or economic claim:

| Classification | Rule |
| --- | --- |
| `no_effect` | no-op or same canonical program |
| `small_local_edit` | one admitted change with bounded local signature delta |
| `coherent_module_edit` | multiple related graph/resource fields change within one named region |
| `large_blast_radius` | edit affects more than one named functional region |
| `behavior_unknown` | compiled but no synthetic trace has been authorized |
| `static_dead_end` | deterministic rejection or no reachable valid child in the enumerated local set |
| `near_absorbing` | overwhelming no-op/reject outcome under a predeclared threshold; descriptive only |
| `invalid` | malformed/stale/authority-drift plan |

The predeclared host set must span at least: an entry-bearing parent, a
management-bearing parent, an exit/recovery-bearing parent, and a parent per
available side. Parent role is retained as context, not turned into a component
score. Crossover is reported separately because it has two ordered parents.

No existing market or historical run data is needed for this design, and none
was read to produce it.
