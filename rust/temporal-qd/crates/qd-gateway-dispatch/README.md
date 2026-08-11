# Temporal QD native gateway dispatch

`temporal-qd-gateway-dispatch` is the bounded Rust owner of the existing Lab
gateway hand-off after a native campaign task matrix is frozen. It does not
construct candidates, modify campaign authority, publish a generation, or
invoke Python.

## Invocation

```text
temporal-qd-gateway-dispatch \
  --task-manifest <campaign-root>/task-manifest.json \
  --output-root <campaign-root> \
  --gateway-url http://127.0.0.1:47241 \
  --gateway-token-file <runtime-secret-file> \
  --fresh
```

Use `--resume` after an interrupted dispatch. The runtime token is used only
for HTTP authorization; it is never persisted. Bounded controls are explicit:
`--enqueue-batch-size`, `--result-batch-size`, `--max-request-bytes`,
`--max-response-bytes`, `--timeout-seconds`, and
`--poll-interval-millis`.

## Durable artifacts

The dispatcher writes only these additional artifacts under the campaign root:

- `.native-gateway-dispatch/tasks/<task-id>.json`: immutable self-hashed task
  objects.
- `.native-gateway-dispatch/task-index.jsonl` and `task-index.json`: compact
  task metadata and its self-hashed root.
- `.native-gateway-dispatch/completion-journal.jsonl`: fsynced, contiguous,
  self-hashed terminal completion entries.
- `failures/<task-id>.json`: immutable failure receipts, written before any
  acknowledgement.

Each completed material is persisted to the existing
`results/<task-id>.json.gz` deterministic Python-compatible gzip format, then
its compact record is fsynced to the completion journal before
`POST /results/ack`. A restart replays the sidecar and journal, revalidates the
gzip/material representation, and acknowledges a matching redelivery without
evaluating it again.

`checkpoint.json` remains the legacy checkpoint shape. It is streamed and
replaced exactly once, only after every immutable task has a durable terminal
record; it is never rewritten per completion.
