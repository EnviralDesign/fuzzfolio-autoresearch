# Temporal QD campaign seal

This crate implements the expensive boundary between a completed
candidate/window task matrix and the generation tail for the supervisor's
explicit Rust finalization mode. Python remains the default engine and oracle.

## Durability model

The input `temporal_qd_campaign_seal_source_v1` is a compact, self-hashed
handoff produced after Python has validated the authority, task manifest, and
checkpoint. It carries the exact task/result bindings and codec metadata but
not the large authority or checkpoint documents.

A fresh invocation performs these stages:

1. Read each immutable raw result blob exactly once, verifying compressed,
   uncompressed, and canonical-semantic sizes and SHA-256 identities.
2. Verify worker/task bindings and construct the exact Python v3 tail-index
   projection, including deterministic zlib gzip bytes.
3. Durably publish `tail-result-index-v3.json`,
   `raw-result-inventory.jsonl`, and finally `campaign-seal-result.json`.
4. Invoke the native tail reducer using only the compact index and evaluation
   population, then commit `generation-tail-transaction-result.json` last.

The campaign seal is the recovery boundary. After it exists, restart never
opens or stats a raw result path. It revalidates the source identity and compact
artifacts, then resumes or reopens the tail transaction. A separate future
audit operation should be responsible for deliberate full raw re-verification.

Runtime-only timing and byte counters are returned on stdout and excluded from
all durable identities. Set `TEMPORAL_QD_SEAL_REPORT_PEAK=1` to report the
Windows peak working set on stderr.

## Invocation

```text
temporal-qd-campaign-seal --manifest PATH
```

The test-only `tests/prepare_live_oracle.py` creates isolated 16/64/128-candidate
fixtures from a completed generation and preserves the Python-produced index as
an exact oracle. It reads live artifacts but writes only to the requested output
directory.

## Deliberate limits

- Python remains the authority/task/checkpoint gateway and oracle.
- The native parser admits only the frozen Python v3 successful-result contract
  and the exact v1/v2 deterministic rejection contracts. Unknown or legacy
  shapes fail closed.
- Python still freezes campaign authority, task/checkpoint completion, and the
  evaluation sidecar before invoking this boundary.
- Rotating cumulative evidence and final generation publication are committed
  by the separate native generation finalizer under the same supervisor
  transaction.
- Only Stage 5E7-v3 admitted results and the two existing deterministic
  rejection schemas are accepted. Legacy admitted evidence fails closed.
