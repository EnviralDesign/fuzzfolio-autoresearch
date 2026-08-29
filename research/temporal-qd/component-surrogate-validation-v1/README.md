# V38 component-surrogate validation v1

This is an offline, provenance-first analysis of the retained V38
`directional_event_insert` cohort.  Its purpose is narrow: determine whether
the frozen corpus contains pre-outcome component evidence sufficient to test a
cheap component surrogate against the complete-strategy insertion result.

It does not replay a strategy, create a child or event, invoke a worker,
gateway, or Vast instance, launch calibration, or change archive or production
policy.

## Run

```powershell
uv run python -m autoresearch.temporal_qd_component_surrogate_validation `
  --event-forensic <v38-followup>/v38-directional-event-insert-forensic-v7.json `
  --multipanel <v38-followup>/v38-multipanel-suboperation-v7.json `
  --evaluated-members <v38>/score/evaluated-members.jsonl `
  --output-dir .tmp/artifacts/component-surrogate-validation-v1
```

The generator validates each retained report's canonical self-hash, binds all
accepted children to the frozen evaluated-member snapshots, recovers the exact
event binding/configuration where it is present, and writes all computed output
under `.tmp/artifacts`.

## Interpretation boundary

The retained V38 records have child/parent outcomes and frozen profiles, but
not frozen event-start timestamps, forward return/MFE/MAE sidecars, or parent
entry-opportunity timestamps.  The generator therefore freezes S0--S3 as
unavailable and returns `insufficient_retrospective_evidence`; it does not
turn descriptive outcome tables into an event ranking or selection gate.

An optional topology input is accepted only when its authenticated result hash
matches the prompt-specified authority.  No alternate topology fixture may be
substituted.
