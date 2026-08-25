# Topology V2.5 operator runbook

V2.5 supersedes the V2.4 scientific interpretation without changing its
three blocks, twelve P/T/E/TE candidates, three panels, 48 tasks per panel, or
144-task inspected package. V2.4 remains historical evidence.

The current launch control and V2.5 policy both retain
`dispatchEnabled=false`. Nothing in this package authorizes gateway contact,
worker registration, market evaluation, a generation, Vast compute, or an
untouched confirmation run.

## Policy

Panel-local `U_v2` requires complete, finite, identity-valid P/T/E/TE evidence;
strict TE net improvement over P, T, and E; TE worst-window return non-worse
than P, T, and E; and exact TE support, quality-lane, and pair-direction
eligibility. P/T/E eligibility is diagnostic and cannot veto a qualifying TE.

The versioned authority is `topology-panel-usefulness-policy-v2.json`. The
authenticated graph V2 opener reopens the archive and behavior-attribution
policy from each campaign's fixed freeze manifest. The reducer rejects policy
drift between panels.

## Recompute

Build the V2 production opener:

```powershell
cargo build --manifest-path rust/temporal-qd/Cargo.toml `
  -p temporal-qd-campaign-seal `
  --bin temporal-qd-campaign-output-graph-v2-json
```

Run `autoresearch.temporal_qd_topology_production_reducer_v3` with exactly
three `--campaign-output-checkpoint` arguments, the frozen V2.3 launch-control,
task-mapping, replication-rule, scientific-contract and analyzer-contract
paths, and the V2.5 panel policy. The opener must be the V2 binary above.

The no-market conformance proof recomputes to analysis identity:

```text
sha256:041dd9dc47d09775a21a1c9790032018a49acd7e67652549a20078b57da7553a
```

This is only pipeline conformance evidence. It is not a topology result.

## Interpretation

Strict cross-panel inference is unchanged:

```text
developmentQualified = U_v2(panel-3)
replicationSurviving = U_v2(panel-1) AND U_v2(panel-2)
inspectedPromising = developmentQualified AND replicationSurviving
```

No pooling, compensation, majority vote, or untouched-panel rescue is allowed.
Untouched confirmation remains pending and must later apply `U_v2` to the same
exact block.
