# V38 initial-protection tail forensic v2

Worst accepted child: `qd_e4dcc5fcc52872a11b24d532c1c5` parent `qd_ed27f99ba0a8dfd7c76c69687efb` side `short`
- parent stop/target: `{'id': 'm_6c6b73521bda', 'ownerSide': 'short', 'initialStop': {'kind': 'fixed_percent', 'percent': 1.0}, 'initialTarget': {'kind': 'reward_multiple', 'multiple': 2.0}}`
- child stop/target: `{'id': 'm_6c6b73521bda', 'ownerSide': 'short', 'initialStop': {'kind': 'fixed_percent', 'percent': 0.25}, 'initialTarget': {'kind': 'reward_multiple', 'multiple': 2.0}}`
- implied R:R before `{'defined': True, 'method': 'reward_multiple', 'value': 2.0, 'stopKind': 'fixed_percent', 'targetKind': 'reward_multiple'}` after `{'defined': True, 'method': 'reward_multiple', 'value': 2.0, 'stopKind': 'fixed_percent', 'targetKind': 'reward_multiple'}`
- child gross/no-cost `-17.007204221672374` net `-69.00720422167237` cost `51.99999999999999` trades `260`
- parent gross/no-cost `4.440892098500626e-16` net `-7.8500000000000005` cost `7.850000000000001` trades `157`
- cost-in-R channel: **both**

The 1:1 probe is not a production gate.

Report sha: `sha256:dfa91495047a8ee52e68178c37ed995f6337c33dd03c93bcbec2b8ab347b362a`
