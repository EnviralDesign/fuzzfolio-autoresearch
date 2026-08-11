# Temporal QD tail reducer

This crate is the native post-evaluation reducer used by the supervisor's
explicit Rust generation-finalization mode. Python remains the oracle and the
default engine.

## Boundary

Inputs are the immutable `temporal_qd_evaluation_population_v1` sidecar and
the source-verified `temporal_qd_tail_result_index_v3`. The sidecar provides
the exact candidate/profile and structural history used by Python. The index
provides the exact canonical, gzip-compressed `_window_record` projection for
every completed task, including deterministic evaluation rejections.

The reducer verifies both root self-hashes, each index-entry self-hash, each
compressed projection's semantic hash and canonical JSON, task/candidate/window
bindings, rotating metric equality, candidate/profile identities, exact
population coverage, and stable execution identities. It deliberately does
not read raw replay blobs, re-run worker validation, or infer strategy metrics.
Those remain on the Python side of the seam.

V5 uses the separately versioned `temporal_qd_tail_result_index_v4` contract.
Its admitted entries additionally bind `rawRotatingProvenance` to the raw
result identity and the conservative replay's `realizedBehavior` identity.
The reducer accepts v4 only when those compact bindings validate; it never
downgrades a v4 request to v3 or opens a raw result on restart.

The explicit operation is
`reduce_evaluated_members_and_provisional`, carried by
`temporal_qd_native_tail_reduction_manifest_v1`. Manifests are canonical JSON
with one LF and a `manifestSha256` over the body. `resultPath` is fixed to
`tail-reduction-result.json`.

## Durable output

The operation publishes two write-once files beside the manifest:

- `evaluated-members.jsonl`: one canonical Python-compatible member per line,
  in candidate-ID order. This keeps peak memory bounded and can be streamed by
  a future adapter.
- `tail-reduction-result.json`: self-hashed bindings, policy, result-set
  identity, rejection rows, member-file byte identity/count, and the exact
  deterministic provisional round-robin result.

The member file is fsynced and installed first; the result is the transaction
commit record. Restart verifies both artifacts and reuses them without
reduction. A missing commit can safely reconstruct and converge on an already
installed identical member file. Divergence, truncation, corruption, input
identity drift, and execution-identity drift fail closed.

## Verification and safe benchmark

From `rust/temporal-qd`:

```powershell
cargo test -p temporal-qd-tail-reducer -j 2
cargo clippy -p temporal-qd-tail-reducer --all-targets -j 2 -- -D warnings
```

The parity tests generate their inputs and expected values with the production
Python `_aggregate_candidate`, descriptor, objective, validity, result-set,
and provisional reducers. They cover admitted members, a warmup-rejected
candidate, a structural-invariant rejection, a strict member subset,
round-robin diversity, repeatable identities, restart reuse, and input/member/
result tampering.

The live-subset harness only reads the named campaign inputs and writes under
the caller-provided scratch directory:

Measured on 2026-08-07 against read-only G1 artifacts after the bounded
two-worker optimization pass (release Rust build):

| Candidates | Exact parity | Python oracle | Rust total wall | Python peak working set | Rust peak working set |
|---:|:---:|---:|---:|---:|---:|
| 64 | yes | 1.110 s | 0.310 s | 161.2 MB | 59.4 MB |
| 128 | yes | 2.311 s | 0.597 s | 220.8 MB | 109.5 MB |
| 1,024 | yes | 14.329 s | 3.160 s | 984.1 MB | 783.4 MB |

Python time is the in-process compact-input parse/validate/reduce/write span;
Rust time includes process startup. Peak working sets are self-reported by the
two processes. The 1,024-candidate row used the copied G1 fixture and is recorded
with exact input/output identities in
`z_docs/temporal-qd-tail-reducer-1024-ab-20260807.json`.
The reducer defaults to two CPU workers, the deliberate ceiling for this
campaign. Set `TEMPORAL_QD_TAIL_THREADS=1` for a serial diagnostic run; values
above two fail closed. `TEMPORAL_QD_TAIL_PROFILE=1` emits stage timings to
stderr. Accumulated per-candidate CPU stages can exceed their enclosing wall
time because two candidates are processed concurrently.

```powershell
$live = 'C:\fuzzfolio-research\temporal-qd-4000x1024x5-20260806-v1\run\broad-4000x1024x5\generations\generation-0001'
$bench = Join-Path $env:TEMP 'temporal-qd-tail-ab-64'
$script = 'C:\repos\fuzzfolio-autoresearch\rust\temporal-qd\crates\qd-tail-reducer\tests\benchmark_live_subset.py'
python $script prepare "$live\proposal\evaluation-population.json" "$live\campaign\screening-run\tail-result-index-v3.json" 64 $bench
python $script oracle $bench
$env:TEMPORAL_QD_TAIL_REPORT_PEAK = '1'
$env:TEMPORAL_QD_TAIL_THREADS = '2'
.\target\release\temporal-qd-tail-reducer.exe --manifest "$bench\manifest.json"
Remove-Item Env:\TEMPORAL_QD_TAIL_REPORT_PEAK
Remove-Item Env:\TEMPORAL_QD_TAIL_THREADS
python $script compare $bench
```

The harness's `provisionalLimit` is 128, matching this campaign's frozen
rotating-evidence contract. Always benchmark from a copied fixture; do not read
or mutate an active campaign for performance evidence.

## Production integration

The campaign-seal binary invokes this reducer after publishing its exact tail
index. The generation finalizer consumes the committed transaction and binds
the checkpoint to `resultSha256`, `evaluationPopulationSha256`, and
`tailResultIndexSha256`. Restart reopens the result only after verifying every
binding and the member-file identity; drift fails closed without Python
fallback.
