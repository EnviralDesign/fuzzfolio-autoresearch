# V38 resource suboperation heritability v1

Every resource slot is joined to parent, plan SHA, construction kind, and panel-3 economics. Rejected duplicate slots keep their attempt hashes; fast-ephemeral does not persist their plan bodies.

| kind | attempts | accepted | dup | beats | losses | abs+ | median Δ net R |
| --- | --- | --- | --- | --- | --- | --- | --- |
| directional_event_insert | 41 | 41 | 0 | 16 | 4 | 8 | 5.08083214617783 |
| directional_event_remove | 1 | 1 | 0 | 1 | 0 | 0 | 2.366628908478332 |
| directional_event_substitute | 14 | 14 | 0 | 5 | 1 | 2 | 0.34999999999999964 |
| evidence_group_create | 2 | 2 | 0 | 0 | 0 | 0 | 0.0 |
| evidence_threshold_mutate | 1 | 1 | 0 | 0 | 0 | 0 | 0.0 |
| indicator_instance_insert | 46 | 46 | 0 | 7 | 4 | 2 | 0.0 |
| indicator_lookback_mutate | 5 | 5 | 0 | 0 | 1 | 0 | 0.0 |
| indicator_period_mutate | 8 | 8 | 0 | 1 | 3 | 0 | -2.568931008051145 |
| indicator_range_mutate | 8 | 8 | 0 | 1 | 0 | 0 | 0.0 |
| indicator_substitute | 19 | 19 | 0 | 5 | 5 | 2 | -1.035665703138517 |
| indicator_timeframe_mutate | 4 | 4 | 0 | 1 | 1 | 1 | 0.0 |
| unrecovered | 11 | 0 | 11 | 0 | 0 | 0 |  |

Parameter-level repeatable positive tail: **True**
Kinds beating all archive parents: ['indicator_instance_insert']

Report sha: `sha256:338d3cebfa8994adf363cb2b18303092cb533d638fc10914afa2532c0bb5206c`
