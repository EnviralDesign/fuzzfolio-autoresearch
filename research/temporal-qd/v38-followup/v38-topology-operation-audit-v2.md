# V38 topology operation audit v2

Recovered accepted plans are not complete operation-specific attempts. Duplicate slots lack plan bodies on the fast-ephemeral path.

| operation | recovered | accepted | unrec dup | beats | losses | abs+ | median Δ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| insert_confirmation_rejection | 8 | 8 | 0 | 1 | 1 | 0 | 0 |
| insert_entry_branch | 16 | 16 | 0 | 1 | 5 | 1 | 0 |
| insert_exit_region | 6 | 6 | 0 | 1 | 2 | 0 | -25.72 |
| insert_management_region | 18 | 18 | 0 | 2 | 6 | 0 | -0.35 |
| insert_setup | 9 | 9 | 0 | 3 | 2 | 0 | 1.45 |
| insert_timeout_rearm | 8 | 8 | 0 | 2 | 0 | 0 | 0 |
| remove_exit_region | 3 | 3 | 0 | 0 | 0 | 0 | 0 |
| remove_management_region | 5 | 5 | 0 | 1 | 1 | 0 | 0 |
| rewire_entry_branch | 8 | 8 | 0 | 0 | 0 | 0 | 0 |
| rewire_exit_region | 2 | 2 | 0 | 0 | 0 | 0 | 0 |
| rewire_management_region | 7 | 7 | 0 | 0 | 1 | 0 | 0 |
| unrecovered | 0 | 0 | 70 | 0 | 0 | 0 |  |

Specific-operation effects are demonstrated. Missing co-adaptation remains a hypothesis, not a demonstrated cause.

Report sha: `sha256:7a002a8f8effa16b53fd44785b8a4003e4668ecc99a142f797768dfcf2e80907`
