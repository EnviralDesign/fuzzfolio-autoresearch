# Reference organisms — construct/compile-only classes

These are language probes, not strategies and not market experiments. No
price series, replay, worker, gateway, generation, archive, or policy mutation
was used. “Supported” means the cited source fixture/test covers the language
class, not that a phenotype is historically common or beneficial.

| ID | Purpose / required capability | Construction path | One-change path | Compile-only evidence | Smallest missing primitive when not direct |
| --- | --- | --- | --- | --- | --- |
| R1 | Level-arm → level-entry | `arm_level`, `enter_on_level` | Threshold/resource change subject to admission | Rust grammar vectors | — |
| R2 | Fresh-event arm → event entry | `arm_fresh_event`, `enter_on_event` | Event/resource change subject to admission | Rust grammar vectors | — |
| R3 | Delayed/streak-gated entry | `gate_delay`, `gate_streak` | Temporal/resource change subject to admission | Rust grammar operation matrix | — |
| R4 | Rising-edge confirmation | `gate_predicate_edge` | Threshold/resource change subject to admission | Rust grammar operation matrix | Falling edge requires a sealed direction choice |
| R5 | Explicit management request | `move_break_even` or `tighten_stop` | Topology or management-family transform | FuzzFolio search-candidate validation | — |
| R6 | Time-based exit | `exit_on_age` | Topology/temporal transform | FuzzFolio bidirectional candidate validation | — |
| R7 | Event-based exit | `exit_on_signal` | Event/resource/topology transform | Rust grammar vectors | — |
| R8 | Bounded cooldown/recovery | `cooldown` | Topology timeout-rearm insert/remove | Rust topology contract tests | Explicit abstention/fallback semantic |
| R9 | Entry branch addition | V5 `insert_entry_branch` | Direct topology insertion | Rust topology plan enumeration/admission | — |
| R10 | Confirmation/rejection/rearm | V5 `insert_confirmation_rejection`, `insert_timeout_rearm` | Direct topology insertion | Rust rearm/entry contract tests | Capture/latch state for persistent remembered condition |
| R11 | Management/exit region reshape | V5 insert/remove/rewire management or exit | Direct topology transform | Rust topology plan and semantic-trace tests | — |
| R12 | Context/timeframe/resource variation | Catalog indicator/timeframe binding | V5 resource family, fresh compile required | FuzzFolio representability inventory | Frozen-authority fixture to prove H4/D1 eligibility; no assumption made |

## Executed deterministic checks

| Check | Result | Scope |
| --- | --- | --- |
| `tests/test_generate_evolutionary_substrate_atlas.py` | 2 passed | Atlas source-count tripwires and two identical generations |
| FuzzFolio `test_temporal_graph_representability.py` + `test_temporal_search_candidate_validation.py` | 11 passed | Static representability, candidate validation, bidirectional compilation; no market input |
| `cargo test -p temporal-qd-kernel --test grammar_genome --test v5_operators_contract` | 32 passed | Grammar/golden vectors plus authority-bound v5 operators and topology; no worker or replay |

The Rust grammar suite includes an exhaustive Python operation matrix and
balanced/closed generated program checks. The v5 suite includes enumerated
admitted topology plans, content-bound plan rejection, parent-bound same-side
crossover, fresh recompilation/reidentification, and typed terminal rejection.

## Interpretation boundary

The checks establish construction and compile/admission reachability for the
cited surfaces. They do **not** establish a sampled prior, a mutation-rate
distribution, realized runtime frequency, retention, selection, or any
economic effect. Those remain the subject of the design-only Ground-Zero
protocol.
