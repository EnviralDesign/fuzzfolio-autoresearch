# PlayHand Coordinator Memory Notes

Date: 2026-07-25

This note documents the process-local memory retainers addressed by the deep memory-bound patch.

## Confirmed retainers

1. The generic durable journal loader read the complete JSONL journal into one Python string, split it into a second list of strings, and reconstructed every historical register payload and terminal receipt before a later compaction pass. A long formal campaign could therefore create a multi-gigabyte startup peak whose allocator arenas remained committed afterward.
2. The campaign summary sampler retained up to 1,000 complete recorded results. Sweep-shard recorded results include ranked permutation payloads, so the sample itself could retain gigabytes even though it is only diagnostic output.
3. Executable task envelopes remained resident after the gateway accepted them. The gateway and append-only journal already own the executable payload at that point; the coordinator needs only task identity and lane-local merge metadata.
4. Completed shards inside an unfinished sweep phase retained their full `params_by_index` maps until the final shard arrived.

## Bounds introduced

- Replay formal PlayHand journals line by line and compact terminal records as they are encountered.
- Keep register and completion byte offsets for exact lazy restoration when crash recovery or duplicate verification needs a full historical record.
- Compact diagnostic recorded-result samples while keeping identity, score, artifact path, policy binding, and a bounded sweep summary.
- Release executable task envelopes immediately after successful idempotent gateway enqueue.
- Remove completed-shard parameter maps while a sweep remains active and deterministically rebuild them from the frozen axes/profile only for the final merge, verifying the existing SHA-256.
- Add coordinator self-memory and retained-object counters to barrier output so Procman process-tree accounting can be distinguished from Python coordinator memory.

No durable journal bytes, task identities, task hashes, worker payloads in transport, worker contracts, authority bindings, lane decisions, or campaign lineage are changed.
