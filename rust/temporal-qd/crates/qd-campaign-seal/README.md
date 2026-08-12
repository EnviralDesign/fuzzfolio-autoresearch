# Temporal QD campaign output

This crate owns the single durable boundary between a committed campaign input,
its completed gateway execution, and the generation tail.

The historical source-build, result-inventory, campaign-seal transaction,
panel-sidecar transaction, and rotating-receipt transaction have been removed
from the production path. The binary now accepts only one operation:

```text
temporal-qd-campaign-seal --campaign-output-manifest PATH
```

The manifest filename is fixed as `campaign-output-manifest.json`.

## Inputs

A fresh invocation authenticates two already committed inputs:

- `campaign-input-checkpoint.json`, which owns the candidate population and
  packed task matrix;
- the native gateway execution receipt, which owns the append-only
  `results.pack` and its committed completion journal.

Remote worker results remain an external trust boundary. Each committed pack
slice is decompressed and validated once against its task, worker material,
schema, and semantic identity before it enters the checkpoint.

## Durable output

One invocation publishes one receipt-last campaign-output checkpoint with four
scientific payload files:

- `campaign-output-checkpoint.json`
- `tail-result-index-v4.json`
- `evaluated-members.jsonl`
- `candidate-panel-bundles.jsonl`

Together with the fixed input manifest, the boundary therefore owns five
files. Runtime metrics report both the file count and logical bytes so campaign
artifact growth can be compared across runs.

The checkpoint is also the campaign receipt consumed by the rotating
prefinalizer. Small campaign-seal and directional-tail projections are embedded
inside it for scientific continuity; they are not separate durable
transactions.

## Restart behavior

A normal restart opens the committed checkpoint and its three scientific data
files. It does not rebuild a campaign source, rescan the gateway result pack,
reopen campaign-input payloads, or recreate panel/receipt sidecars.

The checkpoint and all rows are canonical and self-hashed. A caller that needs
a deeper audit can independently reopen the immutable input and gateway roots,
but forensic reauthentication is not part of ordinary restart.

## Deliberate limits

- Campaign freezing remains the previous durable boundary.
- The gateway remains responsible for durable acknowledgement and shared lake
  maintenance handling.
- This crate admits only current directional-tail v5 campaigns; retired CLI
  modes and historical source-manifest execution fail closed.
- Cumulative evidence and the next generation state remain the responsibility
  of the generation finalization boundary.
