# V38 resource suboperation heritability v2

Canonical metric identity uses JSON round-trip plus a 1e-12 R encoding floor. That floor is not an economic margin; it stops 9.77e-15R dust from counting as a beat.

| kind | accepted | unrec dup | comparable | full ties | beats | risk-qual beats | abs+ | support | direction | quality-like | archive | median Δ net R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| directional_event_insert | 41 | 0 | 25 | 5 | 16 | 15 | 8 | 23 | 5 | 1 | 0 | 5.081 |
| directional_event_remove | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 1 | 0 | 0 | 0 | 2.367 |
| directional_event_substitute | 14 | 0 | 7 | 1 | 5 | 4 | 2 | 11 | 3 | 1 | 0 | 0.35 |
| evidence_group_create | 2 | 0 | 1 | 1 | 0 | 0 | 0 | 2 | 0 | 0 | 0 | 0 |
| evidence_threshold_mutate | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 0 |
| indicator_instance_insert | 46 | 0 | 22 | 11 | 7 | 5 | 2 | 27 | 2 | 1 | 0 | 0 |
| indicator_lookback_mutate | 5 | 0 | 4 | 3 | 0 | 0 | 0 | 4 | 0 | 0 | 0 | 0 |
| indicator_period_mutate | 8 | 0 | 6 | 2 | 1 | 1 | 0 | 7 | 0 | 0 | 0 | -2.569 |
| indicator_range_mutate | 8 | 0 | 6 | 6 | 0 | 0 | 0 | 7 | 0 | 0 | 0 | 0 |
| indicator_substitute | 19 | 0 | 10 | 0 | 5 | 3 | 2 | 16 | 2 | 0 | 1 | -1.036 |
| indicator_timeframe_mutate | 4 | 0 | 3 | 1 | 1 | 0 | 1 | 3 | 0 | 0 | 0 | 0 |
| unrecovered | 0 | 11 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |

Accepted suboperation mix: `{'directional_event_insert': 41, 'directional_event_remove': 1, 'directional_event_substitute': 14, 'evidence_group_create': 2, 'evidence_threshold_mutate': 1, 'indicator_instance_insert': 46, 'indicator_lookback_mutate': 5, 'indicator_period_mutate': 8, 'indicator_range_mutate': 8, 'indicator_substitute': 19, 'indicator_timeframe_mutate': 4}`
Parameter-level repeatable positive tail: **not_demonstrated**
Kinds with at least one parent beat for every archive parent: ['indicator_instance_insert']

Report sha: `sha256:e08968441f0686428db1e18fd492a9511c6e240c9f0a276a6ed3b640383691c4`
