# V38 follow-up decision memo v2

Local source audit plus existing V38 artifacts only. No new market evaluation, generation, Vast instance, or 1024×5.

## 1. What actually drove V38 resource success?

`directional_event_insert` remains the strongest recovered operation: median parent-relative net R 5.081, 16 beats, 8 absolute-positive, 15 risk-qualified beats. The effect is not universal: it is a panel-3 positive tail around two archive parents and is inert around `qd_19e9`.

Parameter-level repeatable positive tail: **not_demonstrated**. Range-mutate's v1 'beat' is encoding dust and is a phenotype tie under v2.

## 2. Did independent panels preserve the event-insert tail?

Accepted event-insert children: 41. Entered backfill: 11. On the backfilled subset, both `qd_69e5` and `qd_ed27` still have at least one absolute-positive or parent-superior child on panel-1 and on panel-2: **True**. `qd_19e9` has no parent-superior or absolute-positive event-insert child on panel-3, and none of its five event-insert children entered the panel-1/2 backfill: **True**. Caveat: only the panel-3 provisional cohort was backfilled.

Event-insert motif interpretation: **heterogeneous_mixture**.

## 3. Coverage

122 period-like fields admitted; 62 non-period numeric and 46 enum/option fields excluded. Bound `hasBoundTalibMeta=true` on 17 of 17 parent-side occurrences.

## 4. Topology

Specific-operation effects are demonstrated. Missing co-adaptation remains a hypothesis. Recovered accepts are not complete operation-specific attempts.

## 5. Protection tail

Worst child `qd_e4dcc5fcc52872a11b24d532c1c5` reconstructed a complete stop+target pair, including the unchanged opposite locator. Cost-in-R channel: **both**. The 1:1 probe is not a production gate.

## 6. Co-adaptation contract

v2 JSON validates and self-hashes in Python and Rust. First-experiment slot count 30. Morphology nursery is deferred. This change does not launch the experiment.

## 7. Smallest next market experiment, if any?

Do not start G6, continue V37, breed the V38 archive, reweight families, or run another 1024×5. If a later operator authorizes compute, the smallest experiment is the four-arm first contrast on frozen parents: clone / topology-only / directional_event_insert control / topology then one directional_event_insert settling step.
