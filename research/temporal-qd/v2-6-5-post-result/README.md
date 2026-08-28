# V2.6.5 post-inspected topology result

This is an offline terminalization and mechanism package for the sealed
144-task topology study. It does not run a replay, dispatch a worker, modify
an archive, regenerate a candidate, or reinterpret the authenticated result.

The source is the sealed salvage packet identified in
`source-evidence-manifest.json`. Its reducer result was complete and produced
the same authenticated analysis twice.

## Contents

- `terminal-inspected-decision.json` — the self-hashed, result-derived terminal
  decision: zero qualifying blocks, zero confirmation tasks, and no generation
  or resource study authorization.
- `topology-mechanism-forensic.json` — self-hashed observations for every
  block/panel/arm. Exact retained entry sequences are bound by count and hash;
  raw market records are deliberately not copied forward.
- `event-only-support-calibration-design.json` — a five-candidate, 60-task
  design-only support-calibration study for the sparse event signal. It is not
  launched.
- `bounded-topology-nursery-design.json` — design-only requirements for a
  future bounded setup/fallback motif. It explicitly records that the existing
  recovery-node operator alone does not wire a setup escape.
- `human-decision-memo.md` — compact interpretation and recommended order.
- `checksums.sha256` — output-file raw SHA-256 values.

The production source change in this branch is deliberately separate:
`autoresearch/temporal_qd_v2_6_4_run_authority.py` now checks every launch
control panel's checkpoint and task-matrix identities against the copied input
before snapshot/dispatch. The test file covers stale identities, duplicate
panel descriptors, cardinality drift, post-snapshot checkpoint drift, and the
existing no-arbitrary-authority-root boundary.
