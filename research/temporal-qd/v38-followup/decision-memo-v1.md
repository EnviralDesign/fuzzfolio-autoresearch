# V38 follow-up decision memo

Local source audit plus existing V38 artifacts only. No new market evaluation, generation, Vast instance, or 1024×5.

## 1. Do actual indicator parameter mutations exist, and were they responsible for V38 resource success?

They exist. `evolvable_resource_v1` emits `indicator_period_mutate` (fast/nominal/slow on `talibMeta` names containing `period`), plus range, timeframe, and lookback mutations. V38 accepted **8 period**, **8 range**, **5 lookback**, and **4 timeframe** children.

They were **not** the resource success. Accepted resource mass was structural:

- `indicator_instance_insert` 46
- `directional_event_insert` 41
- `indicator_substitute` 19
- `directional_event_substitute` 14

`directional_event_insert` is the only recovered kind with a clearly positive median parent-relative net R (**+5.08**, 16 beats, 8 absolute-positive). Period mutation: 1 parent beat, **0** absolute-positive, median Δ **-2.57**. The scorer flag “parameter-level repeatable positive tail” trips on a sparse timeframe cell (n=4, 1 absolute-positive). That is not a robust parameter-learning tail.

The only kind that beat all three archive parents was `indicator_instance_insert`, with median Δ **0.0**. Do not promote it.

## 2. Which parameter surfaces are still missing from “evolve everything”?

Catalog: 88 indicators, 79 with a period surface, 86 with any current parameter surface. Bound V38 parent instances: 17, of which 15 join to a catalog period surface.

Still missing, and **not** to be filled with generic numeric mutation:

- non-period TA sliders (`nbdev`, signal periods that fail the name-contains-`period` rule, etc.)
- enum/select `talibMeta` options
- instance-embedded `talibMeta` (V38 bound genomes store `id` + `talibConfig` and must join the catalog)

Any addition needs catalog authority, bounds/marks, ordering constraints, construction identity hashes, and replay tests. Proposal is in `indicator-parameter-evolution-coverage-v1.json`.

## 3. Is topology failure representation, specific operations, missing co-adaptation, or a mixture?

Mixture, dominated by **specific operations** plus **missing co-adaptation**, not “topology is an untyped blob.”

- Typed 14-operation grammar. Additive median Δ and destructive median Δ were both **0.0** at class level, so pooling hid the split.
- `insert_exit_region` accepted median Δ **-25.7**.
- `insert_management_region` median **-0.35** with the most parent losses.
- `insert_setup` median **+1.45**.
- 70/160 slots duplicate-collapsed; those plan bodies were not persisted on the fast-ephemeral path.

Representation is coarse at family-level scoring. The grammar is already typed. Co-adaptation was never given: topology children were judged with the parent controller frozen.

## 4. Is a topology-plus-local-settling experiment scientifically justified?

Yes, as an **experiment-only** overlay, not as production. The contrast between `insert_setup` and `insert_exit_region` is already enough to reject “all topology is noise,” and not enough to claim morphology search works. The four-arm matrix (clone / topology-only / resource-only control / topology+bounded resource settling) is the smallest test of the body/brain hypothesis. It is specified and parse-isolated; it is **not launched** here.

## 5. What caused the initial-protection catastrophic tail?

Worst accepted child `qd_e4dcc5fcc52872a11b24d532c1c5`, parent `qd_ed27f99ba0a8dfd7c76c69687efb`.

- `mutationClass=jump`, `site=stop`
- before `{kind: fixed_percent, percent: 1.0}` → after `{kind: fixed_percent, percent: 0.25}`
- panel-3 cumulative **-69.0R**, worst window **-31.4R**
- 260 trades, cost drag **52R**
- close reasons: 151 break-even stop, 77 stop-loss, 28 take-profit

This is **cost drag and churn from a four-fold tighter stop**, not one huge loss and not a kind-switch to an exotic locator. Jump-class stop mutations are the tail; adjacent (n=39) bottoms out at **-16.5R**. Target-site mutations did not produce the catastrophe (min **-11.4R**).

## 6. Is the 1:1 probe feasible with native semantics, and what would it teach?

The **full-system** probe is feasible now: native `reward_multiple = 1.0` on the real machine, compared with the parent clone. It teaches whether the strategy still pays after payout skew is removed, including cost and re-entry effects.

The **fixed-entry counterfactual** is **not** feasible from current `tradeSequence` fills. That needs a new worker artifact `temporal_qd_entry_opportunity_tape_v1`. Do not fake it. Do not make 1:1 a production gate.

## 7. Smallest next market experiment, if any?

After this evidence:

1. Do **not** start G6, continue V37, breed the V38 archive, reweight families, or run another 1024×5.
2. If a market experiment is run later, the smallest one is the four-arm topology co-adaptation matrix on frozen parents, with `insert_setup` vs `insert_exit_region` as explicit topology plans, plus a resource contrast of `directional_event_insert` vs `indicator_period_mutate`. Local compute only unless a later operator says otherwise.
3. Optional later diagnostic: full-system 1:1 protection probe on the same frozen parents. Not a gate.

This change does not launch that experiment.
