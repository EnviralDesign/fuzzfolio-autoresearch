# Stage 4.5E Ground-Zero synthetic arena

## Mission

Test one narrow question without market data: does a one-step, compiler-admitted
static change produce a bounded and interpretable change under deterministic
facts and event traces?

The first witness is the active-negative-control long-side threshold change:

- parent: `qd_001958c8b3288892a458207c9b76`, long side;
- plan: `sha256:155ecfbc8ce76b46e784bd7e776e1e995abced8d494403d941aeb47ae07ad5c0`;
- construction: `evidence_threshold_mutate`, setup group
  `g_c1e8b5ed0642`, 70% to 75%; and
- compiled child: program `sha256:c44e0da69e039d85cb4a5ed38f8732261bab038a36d4c864c82f86e27dd70365`,
  executable semantic `sha256:a91fe10d66c782c9e292b855ed91f602566607ba44b8587d6f5c6084761c9725`.

The paired control witness is the same parent's short-side directional-event
insertion, plan `sha256:ce40abefd7a9494941e14462db3bdb70aee6cf4bc487cf87412d5ea52ccc1003`.
It isolates a route/resource addition from a scalar threshold change.

## Deterministic protocol

Use a single fixed seed and versioned JSONL fact stream. Feed each parent and
child exactly the same sequence: flat/no-signal, up-trend, range, down-trend,
a single event, a persistent event, event expiry, evidence at 70% then 75%,
setup starvation, timeout/rearm where the graph exposes it, session boundary,
successful and rejected action receipts, and a final no-signal tail. Supply
only minimal schema-valid values; no bars, prices, P&L, or market provider.

Run long and short in isolated scenarios and record at every tick: guard truth
and evaluated guards, state, transition, action intent/outcome, entry/exit
activation, abstention, and event expiry. Derive state residence, transition
counts, liveness, trap/near-absorption flags, and a bounded behavioral-distance
summary between parent and child.

## Predeclared interpretation

Keep a hypothesis only when the child changes the expected threshold/route
decision while preserving side isolation, deterministic replay, and recovery
from the no-signal tail. Reject it when behavior is identical despite a claimed
semantic change, when the difference appears outside the targeted mechanism,
or when either program traps without a documented graph reason. This validates
runtime observability, not profitability or a production change.
