# Topology co-adaptation Python/Rust parity corpus v6

Shared canonical fixture plus adversarial mutations. Rust `topology_coadaptation_matrix_v6::validate` must accept/reject the same cases. No market compute.

| Case | Python accepted | Required Rust accepted |
| --- | --- | --- |
| `canonical_fixture` | True | True |
| `receipt_id_drift_only` | False | False |
| `topology_plan_substitution` | False | False |
| `event_primitive_substitution` | False | False |
| `missing_semantic_delta` | False | False |
| `fake_event_attaches_without_node` | False | False |
| `sparse_fake_audit` | False | False |
| `wrong_parent` | False | False |
| `wrong_side` | False | False |
| `swapped_e_te` | False | False |
| `missing_receipt` | False | False |
| `extra_receipt` | False | False |
| `fake_pair_native_report` | False | False |
| `mislabeled_pair_identity_field` | False | False |
