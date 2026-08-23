# V38 initial-protection tail forensic v3

Catastrophic child `qd_e4dcc5fcc52872a11b24d532c1c5`: nominal reward multiple stayed 2.0; tighter stop caused 4x cost/R; trades 157→260; gross already negative; cost and churn both contributed; channel **both**.

Transition-level table:

| transition | accepted |
| --- | --- |
| stop_tightening | 18 |
| stop_widening | 16 |
| target_tightening | 25 |
| target_widening | 16 |
| locator_kind_switch | 8 |
| unclassified | 0 |

Do not infer that wider stops are universally better. 1:1 is not a production gate.

Report sha: `sha256:b12a12681877d6cdb30dc69d9670f805d4b1cbdd2fe9b585eb2183a5425c9253`
