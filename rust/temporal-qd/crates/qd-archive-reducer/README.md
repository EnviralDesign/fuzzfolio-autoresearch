# Temporal QD archive reducer

`temporal-qd-archive-reducer --manifest PATH` is the v5 evidence-ladder
archive boundary. It consumes only the authenticated tail-reducer result and
its canonical `evaluated-members.jsonl`; it never rereads task/result blobs or
calls Python.

The canonical-LF manifest is `temporal_qd_native_archive_reduction_manifest_v1`
with operation `reduce_evidence_ladder_archive`. It binds the tail result and
member-file SHA-256 values, source-population SHA-256, result-set SHA-256,
generation, capacity, and archive-policy authority. `archivePath` must be
`archive.json` and `resultPath` must be `archive-reduction-result.json`.

The reducer writes a self-hashed `temporal_qd_archive_v3` archive plus a
self-hashed `temporal_qd_native_archive_reduction_result_v1` receipt. Restarts
rehash the bound tail/member inputs and reopen both output identities. Input
symlinks are rejected.

The member stream is copied to a temporary spool and reduction retains only
ordering metadata/offsets in memory. It implements resolved-execution dedup,
Pareto fronts/crowding, quality/negative/observational lanes, v5 directional
quotas, capacity, prior-cell accounting, and survivor-family accounting.
