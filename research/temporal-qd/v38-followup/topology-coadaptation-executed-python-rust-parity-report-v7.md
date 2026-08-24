# Executed Python/Rust parity report v7

Rust `topology_coadaptation_matrix_v7::validate` was run against the emitted Python corpus. Cargo test `python_rust_parity_corpus_agrees` passed.

| Fixture | Python | Rust | Match | Error class |
| --- | --- | --- | --- | --- |
| `canonical_fixture` | True | True | True |  |
| `receipt_id_drift_only` | False | False | True | TemporalDiscoveryContractError |
| `topology_plan_substitution` | False | False | True | TemporalDiscoveryContractError |
| `event_primitive_substitution` | False | False | True | TemporalDiscoveryContractError |
| `missing_semantic_delta` | False | False | True | TemporalDiscoveryContractError |
| `fake_event_attaches_without_node` | False | False | True | TemporalDiscoveryContractError |
| `sparse_fake_audit` | False | False | True | TemporalDiscoveryContractError |
| `wrong_parent` | False | False | True | TemporalDiscoveryContractError |
| `wrong_side` | False | False | True | TemporalDiscoveryContractError |
| `swapped_e_te` | False | False | True | TemporalDiscoveryContractError |
| `missing_receipt` | False | False | True | TemporalDiscoveryContractError |
| `extra_receipt` | False | False | True | TemporalDiscoveryContractError |
| `fake_pair_native_report` | False | False | True | TemporalDiscoveryContractError |
| `mislabeled_pair_identity_field` | False | False | True | TemporalDiscoveryContractError |
| `success_policy_drift` | False | False | True | TemporalDiscoveryContractError |
| `native_ran_false_while_compiled` | False | False | True | TemporalDiscoveryContractError |

