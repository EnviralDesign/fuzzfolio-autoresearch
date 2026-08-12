# Temporal QD native gateway dispatch

`temporal-qd-gateway-dispatch` drains one authenticated campaign-input task
pack into one durable gateway execution receipt. It does not construct
candidates, alter campaign authority, finalize a generation, or invoke Python.

## Invocation

```text
temporal-qd-gateway-dispatch \
  --campaign-input-checkpoint <campaign-root>/campaign-input-checkpoint.json \
  --output-root <campaign-root> \
  --gateway-url http://127.0.0.1:47241 \
  --gateway-token-file <runtime-secret-file> \
  --fresh
```

Use `--resume` after interruption. The token is runtime-only and is never
persisted. Bounded controls include enqueue/result batch sizes, request and
response byte limits, request/completion timeouts, poll interval, maintenance
probe interval, and the independent maintenance timeout.

## Fixed durable layout

Successful dispatch uses a fixed five-file sidecar under
`.native-gateway-dispatch`:

- `task-index.jsonl`
- `task-index.json`
- `completion-journal.jsonl`
- `results.pack`
- `execution-receipt.json`

Terminal task failures, when present, are written separately as
`failures/<task-id>.json` before acknowledgement.

The task pack is indexed in place; it is not copied into one file per task.
Successful results are deterministic gzip members appended to `results.pack`,
not one gzip file per task. Each completion-journal row binds the task and
attempt to the pack offset and length, compressed and uncompressed SHA-256,
and admitted result identity.

Therefore successful file count stays constant as task count grows. Logical
result bytes remain O(tasks), but per-result filesystem metadata, directory
entries, opens, renames, and synchronization barriers are removed.

## Commit and restart protocol

For each delivered result batch, the dispatcher:

1. validates task, lease, schema, worker material, and scientific result
   bindings;
2. appends deterministic gzip members and contiguous journal rows;
3. flushes and synchronizes the result pack and journal once for the batch;
4. reopens and verifies the newly committed pack slices; and
5. acknowledges only those durable completions.

Resume authenticates the campaign-input checkpoint, task index, journal, pack,
and execution receipt. A matching redelivery is acknowledged from the durable
record rather than evaluated or written again. Incomplete crash tails are
truncated to the last committed journal boundary.

## Shared lake-maintenance gate

Lake-wide 409/503 responses open one campaign-level maintenance gate rather
than creating thousands of task-level requeues. While the gate is open, the
dispatcher stops enqueue, result polling, and acknowledgement work, honors
maintenance/retry timing, and sends one bounded recovery probe at the configured
interval.

A positive probe closes the gate and normal dispatch resumes. Maintenance pause
time is excluded from the scientific completion timeout and is bounded by its
own maintenance timeout. Shared infrastructure unavailability is recorded in
telemetry; it does not consume candidate-window attempt budgets or mutate every
task into a retry state.

## Downstream boundary

The receipt-last gateway execution receipt binds the task index, completion
journal, and result pack. The campaign-output checkpoint consumes that receipt
and validates each remote result exactly once before publishing evaluated
members, panel bundles, and the tail result. Ordinary downstream restart does
not rescan the gateway pack.
