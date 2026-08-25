# Topology V2.4 production reducer and launch authority

V2.4 supersedes the summary-trusting V2.3 gate without rewriting any V2.3
artifact. It preserves the original scientific contract and frozen V1
replication rule exactly.

## What changed

- `temporal-qd-campaign-output-graph-json` reopens the complete production
  campaign-output graph for analysis. The compact checkpoint opener remains the
  restart boundary.
- `temporal_qd_topology_production_reducer_v2.py` accepts three checkpoint
  paths plus frozen authorities. It does not accept caller metrics or an
  identity-validity Boolean.
- The reducer derives the exact cohort/block/arm/parent/side join, panel-local
  economics and gates, cross-panel replication result, and bound mechanism
  evidence. Missing mechanism material is typed `unavailable`; it is never
  fabricated as zero.
- The V2 reporting projection preserves the frozen strict Boolean while making
  every declared reporting category reachable. Python and Rust outputs,
  including the self-hash, are compared as complete canonical objects.
- The V2.4 gate executes the reducer, reopens all three output graphs, checks all
  three no-market lifecycles, recomputes the 144-task mapping, invokes the Rust
  parity binary, and validates expanded cross-root evidence.

## Evidence scope

The integration proof uses 48 exact synthetic/no-market worker results per
panel, loopback HTTP only, durable gateway admission, campaign-output fresh and
restart opens, and bound-bundle tamper rejection. These economics are fixture
evidence, not scientific results.

Portable identities are compared across distinct roots for all three campaign
input checkpoints, task packs, cohorts, evaluated members, panel bundles, tail
indices, gateway receipts, and the complete reducer result. Freezer manifests
and campaign-output manifests/checkpoints are explicitly classified as
operational because they bind absolute paths; their transitive semantic
payloads are compared separately.

## Recompute the reducer

```powershell
.\.venv\Scripts\python.exe -m autoresearch.temporal_qd_topology_production_reducer_v2 `
  --campaign-output-checkpoint <panel-1-output-checkpoint> `
  --campaign-output-checkpoint <panel-2-output-checkpoint> `
  --campaign-output-checkpoint <panel-3-output-checkpoint> `
  --production-opener rust\temporal-qd\target\release\temporal-qd-campaign-output-graph-json.exe `
  --launch-control research\temporal-qd\rust-canonical-authority-v2-3\topology-production-launch-control-v1.json `
  --task-mapping research\temporal-qd\rust-canonical-authority-v2-3\topology-production-task-mapping-v1.json `
  --replication-rule research\temporal-qd\rust-canonical-authority-v2-3\topology-replication-survival-rule-v1.json `
  --scientific-contract research\temporal-qd\rust-canonical-authority-v2\topology-scientific-contract-v1.json `
  --analyzer-contract research\temporal-qd\rust-canonical-authority-v2-3\topology-post-run-analyzer-contract-v1.json `
  --output <analysis-output>
```

## Safety state

- `dispatchEnabled=false` remains frozen.
- Untouched confirmation remains `pending` and cannot rescue an inspected-panel
  failure.
- No real gateway, market evaluation, worker registration/launch, generation,
  or Vast instance was used for V2.4.
- A green V2.4 packaging gate authorizes the separately controlled case-study
  launch boundary; it does not itself dispatch or claim production scientific
  confirmation.
