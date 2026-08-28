# Python/Rust legacy-opening boundary

At source commit `5fa623b88c641d4d886411bf195ee3ef386d6446`, the Python
`build_cumulative_breeder_archive` path rejects V37's exact legacy window
payload with `window realized behavior identity mismatch`. It requires
per-window `identityMaterial` and `identitySha256`.

The native historical finalizer accepts that same frozen source shape. It calls
the Rust tail reducer's `aggregate_realized_behavior`, which validates the
retained behavior fields and derives aggregate identity material itself.

Across fresh native G1 replay, all 128 reconstructed aggregate identities were
self-consistent and exactly equal to their retained historical cumulative
counterparts. For example, candidate
`qd_0007e0be0059a7775661aa9be8a3` reconciled four retained source-window
record hashes to aggregate identity
`42bf1440bc765ac294660faa995836f9ee1ea9a2689430686f802773cae949ff`.

This is not a policy exception or synthetic identity: it is the historical Rust
aggregation implementation. The prior Python failure remains useful evidence
of a legacy parity-opening defect, but is not evidence that archive authority
is missing.
