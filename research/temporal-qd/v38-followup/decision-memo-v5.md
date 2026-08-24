# V38 follow-up decision memo v5

Local contract repair only. No new market evaluation, generation, Vast host, or 1024x5.

## Pair receipts

Complete-block receipts now reconstruct both program legs, preserve the opposite side, and refuse to label compiler-policy SHA as native validation. FrozenPair.compile / native validation did not run; those fields are null.

Complete 2x2 blocks remain **3** case studies. `insert_setup` is a timing mutation; occupancy/freshness fields are frozen unevaluated.

## Archive gates

Focus child `qd_686f15941b1f07e6273929c8c2a0`: active 11/12 (0.9167, pass=True); trades 52 / 36.0 months avg 1.4444 vs 4.0, pass=False; cumulative 10.2245R, pass=True; median -0.0250R, pass=False; quality=False; frontier=False; capacityConsidered=False.

Binding causes: average_trades_per_month_below_minimum, median_window_conservative_net_not_positive. Competing members are not the binding exclusion.

## Task projection

Mutation pairs 265 + pair clones 5 = 270. Windows per panel 4 x panels 3 = 12 inspected windows. Projected inspected tasks **3240**. With future confirmation panel **4320**.

Balanced case-study coverage freezes lexicographic plan IDs. One child per filled cell is coverage, not repeatability. Proposal inspected tasks **756**; with confirmation **1008**.

## Do not authorize

No market evaluation, topology launch, resource-matrix launch, G6, V37/V38 continuation, 1024x5, family reweighting, gate changes, morphology nursery, or breeding from the V38 archive. Panels 1 and 2 already influenced design and are not untouched confirmation.
