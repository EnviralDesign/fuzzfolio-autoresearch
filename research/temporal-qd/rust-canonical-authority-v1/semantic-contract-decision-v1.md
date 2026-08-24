# Rust/Dashboard semantic-contract decision

Decision identity: `sha256:88895559d40776dc23ea6d8c5d9e3d1a47785875a53f665faf5f5b35c0c338d9`

The retained V38 target reconstructs and recompiles exactly through the Rust authority. The pinned Dashboard oracle materializes `holdPolicy.onBreach=exit_next_open`, while Rust canonicalizes the same documented/default behavior to omission. That changes raw/program identity material but not the observed execution rule. Validation capability ordering and extra capability metadata are report-only differences.

Rust matches the explicit native contract and historical V38 identities. Dashboard is behaviorally equivalent for the reviewed difference but remains a historical/observational oracle, not an identity authority for new native work. Neither implementation demonstrates a semantic defect in this bounded corpus. Proceed with native authority v1 without changing Rust semantics or rewriting V37/V38 artifacts.
