# Decision memo — what the substrate audit supports

## Demonstrated by source and compile-only checks

- The Rust grammar has 23 sealed fragment productions, 7 ports, bounded
  resources/choices, and native-plus-bidirectional compiler boundaries.
- The FuzzFolio runtime has a broader 22-guard / 8-action vocabulary than the
  Rust grammar directly emits.
- V5 has six primary authority-bound families and 14 sealed topology
  operations. Mutation is not an arbitrary graph edit; every child must pass
  fresh admission before it can be used as the next evolved state.
- Candidate and operator lineage/receipt fields exist, while component-local
  causal credit does not follow from those fields.
- Existing static tests pass: 2 atlas, 11 FuzzFolio construct/compile, and 32
  Rust grammar/operator contract tests.

## Strong source hypotheses

- The main near-side opportunity is not a giant language rewrite: several
  executable runtime guards lack a direct grammar → seed → mutation route.
- Some desired phenotypes need coordinated changes, not a scalar parameter
  tweak: falling-edge use, capture/latch semantics, explicit abstention/fallback,
  and side/portfolio credit all cross more than one boundary.
- Static operator reachability may be highly uneven by parent and site. That
  must be measured using a read-only, no-market construction pilot before it is
  described as an operator prior or bottleneck.

## Speculative and intentionally unclaimed

- That any direct-route gap improves, harms, or is neutral to market behavior.
- That any historical V37/V38 family was sampled often, activated at runtime,
  retained, selected, or economically useful.
- That a parent role, suppression label, side, indicator, action, or topology
  edit has an independent score.

## Narrow primitive priorities

| Priority | Primitive / study object | Why it is narrow | Falsification condition |
| --- | --- | --- | --- |
| 1 | Read-only authority-bound static prior + local-neighbourhood audit | Measures current reachability before changing language | No stable source/authority/parent/compile join can be formed |
| 2 | Direct grammar route for one runtime guard family, starting with a single predeclared missing semantic | Adds one production/seed/mutation route rather than a framework | Construct/compile-only fixtures cannot distinguish it from an existing route |
| 3 | Falling-edge direction choice if the direct-route audit confirms the rising-only bottleneck | Small extension to existing predicate-edge representation | A sealed current route already expresses falling edge without ambiguity |
| 4 | Explicit abstention/fallback semantics, separate from existing bounded rearm | Names a missing decision policy rather than changing selection | Synthetic trace cannot distinguish it from ordinary timeout/rearm |
| 5 | Capture/latch only after a concrete phenotype demonstrates persistent state is indispensable | Avoids generic memory machinery | Existing graph/state semantics express the proposed lifecycle locally |
| 6 | Route/site/parent-conditioned telemetry fields before any credit model | Observability, not a scoring surrogate | Required joins already exist in immutable records without ambiguity |

No priority authorizes a production grammar/operator/archive/selection policy
change in this stage. The first priority is an audit design, not a launch.
