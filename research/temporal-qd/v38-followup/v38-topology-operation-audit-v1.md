# V38 topology operation audit v1

Topology is typed, not arbitrary graph corruption. V38 pooled 14 operations at family level.

| operation | attempts | accepted | dup | beats | losses | median Δ net R |
| --- | --- | --- | --- | --- | --- | --- |
| insert_confirmation_rejection | 8 | 8 | 0 | 1 | 1 | 0.0 |
| insert_entry_branch | 16 | 16 | 0 | 1 | 5 | 0.0 |
| insert_exit_region | 6 | 6 | 0 | 1 | 2 | -25.717728618486312 |
| insert_management_region | 18 | 18 | 0 | 2 | 6 | -0.3500000000000112 |
| insert_setup | 9 | 9 | 0 | 3 | 2 | 1.4499999999999922 |
| insert_timeout_rearm | 8 | 8 | 0 | 2 | 0 | 0.0 |
| remove_exit_region | 3 | 3 | 0 | 0 | 0 | 0.0 |
| remove_management_region | 5 | 5 | 0 | 1 | 1 | 0.0 |
| rewire_entry_branch | 8 | 8 | 0 | 0 | 0 | 0.0 |
| rewire_exit_region | 2 | 2 | 0 | 0 | 0 | 0.0 |
| rewire_management_region | 7 | 7 | 0 | 0 | 1 | 0.0 |
| unrecovered | 70 | 0 | 70 | 0 | 0 |  |

Additive median Δ: 0.0; destructive: 0.0; rewire: 0.0.
Additive less destructive than removal: **False**

Report sha: `sha256:b337c2e56fdda16126cc097d03966a835f6f6552280b6877a796e9d0cae08a96`
