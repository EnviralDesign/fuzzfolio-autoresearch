# Phase 3 PlayHand Authority Operator Guide

## Purpose

`phase3-playhand-authority` produces the immutable authority consumed by the
future Phase 3 PlayHand coordinator. It does not launch a coordinator, gateway,
worker, or replay task.

The command reads the verified Phase 2 Atlas authority capsule. A and B are the
only construction inputs for recipe, slot, pair, and negative-prior menus. C
and D are copied only as aggregate diagnostic counts. Their candidate rows,
rankings, and identities are never read by the menu-construction path.

The authority binds the untouched reserved tail `[2026-01-14T00:00:00Z,
2026-07-14T00:00:00Z)`, the exact Phase 3 campaign-policy manifest, the
generated v2 seed-plan digest, the common lake/universe/source identities, and
the Phase 2 runtime contract. It also binds the exact current Atlas generation
and run sequence used by formal policy-honest v2 negative-prior expiry. It
requires a finite `target_runs`; continuous mode is forbidden at this authority
layer.

Worker correctness and worker launch provenance are separate contracts. The
Lab Gateway enforces `worker_contract_sha256` and required capabilities when a
worker claims a task. It does not inspect or enforce an OCI image tag. The
authority therefore records the exact expected image as
`operator_launch_worker_image`: the operator must use it when creating the
Vast fleet, while claim-time correctness remains bound to the worker contract
hash.

## Inputs

- `--phase2-capsule-root`: the verified local Phase 2 authority capsule.
- `--policy-manifest`: one exact `phase3_campaign_policy_manifest_v1` JSON
  document. Its canonical manifest digest and raw file digest are both bound.
- `--authority-id`: a stable, new Phase 3 authority name.
- `--target-runs`: a positive finite campaign cap.
- `--out-dir`: a new, absent output directory.

The output directory contains:

- `phase3-playhand-authority.json`
- `play-hand-seed-plan.json`
- `phase3-playhand-authority-report.json`

All three are created once. Existing authority directories are never merged or
overwritten.

## Preflight

Run this before creating an authority. It writes nothing:

```powershell
.\.venv\Scripts\phase3-playhand-authority.exe `
  --phase2-capsule-root C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721 `
  --policy-manifest C:\path\to\phase3-campaign-policy.json `
  --authority-id phase3-darwin-rich-ab-v2 `
  --target-runs 1000 `
  --dry-run --json
```

## Build

After the dry run is valid, create a new authority directory:

```powershell
.\.venv\Scripts\phase3-playhand-authority.exe `
  --phase2-capsule-root C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721 `
  --policy-manifest C:\path\to\phase3-campaign-policy.json `
  --authority-id phase3-darwin-rich-ab-v2 `
  --target-runs 1000 `
  --out-dir C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\phase3-darwin-rich-ab-v2 `
  --json
```

## Audit

Re-derive and compare all authority content before a coordinator uses it:

```powershell
.\.venv\Scripts\phase3-playhand-authority.exe `
  --phase2-capsule-root C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721 `
  --policy-manifest C:\path\to\phase3-campaign-policy.json `
  --authority-path C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\phase3-darwin-rich-ab-v2\phase3-playhand-authority.json `
  --audit --json
```

Any policy, source, capsule, seed-plan, report, or authority drift fails closed.
Do not bypass that failure by editing an existing authority artifact. Create a
new authority only after the changed input is understood and approved.

## Runtime-Argument Preflight

This read-only command validates the authority and emits the exact semantic
arguments a future coordinator must use. It starts no coordinator or gateway:

```powershell
.\.venv\Scripts\phase3-playhand-authority.exe `
  --phase2-capsule-root C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721 `
  --policy-manifest C:\path\to\phase3-campaign-policy.json `
  --authority-path C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\phase3-darwin-rich-ab-v2\phase3-playhand-authority.json `
  --runtime-arguments --json
```

## Coordinator Handoff

The authority JSON contains `playhand_runtime_arguments`. A later Phase 3
coordinator must obtain that map through
`resolve_phase3_playhand_runtime_arguments(...)`, rather than assembling the
semantic command arguments itself. That resolver re-audits the capsule and
policy, checks the seed-plan digest, and rejects changes to any bound value,
including `current_atlas_generation`, `current_atlas_run_sequence`, and
`target_runs`.

Procman may add only operational controls such as gateway URL, active-run
capacity, polling, and resume. It must not override an authority value or grant
access to the reserved tail. The executable is installed by the next controlled
environment sync; do not run that sync while the Lab Gateway environment is
locked by an active service.

## Phase 3 Coordinator

`phase3-playhand` is the only supported Phase 3 launch path. It re-audits the
full capsule, authority, seed plan, policy, live worker contract, runtime
policy, and profile-model sources before it invokes the existing durable
PlayHand Lab coordinator. It binds construction to `2026-01-14T00:00:00Z`;
the reserved tail is unavailable to all Phase 3 construction tasks.

The checked-in policy is
`C:\repos\fuzzfolio-autoresearch\configs\phase3-campaign-policy.json`. It
binds the approved 60/25/15 lane allocation, diversity caps, and Phase 2 rich
prior generation/sequence. Do not replace it or pass an alternate semantic
configuration through Procman.

Fresh launch requires an absent campaign root:

```powershell
.\.venv\Scripts\phase3-playhand.exe `
  --authority-path C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\phase3-darwin-rich-ab-v2\phase3-playhand-authority.json `
  --phase2-capsule-root C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721 `
  --policy-manifest C:\repos\fuzzfolio-autoresearch\configs\phase3-campaign-policy.json `
  --fresh `
  --gateway-url http://127.0.0.1:8799 `
  --active-runs 128 `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --json
```

Resume uses the same authority inputs and `--resume`. It requires the exact
existing campaign directory, metadata, state, and durable execution journal;
it never creates replacement state. The adapter accepts only operational
gateway, capacity, polling, drain, display, and Trading Dashboard options.
It intentionally exposes no flags for campaign size, seeds, instrument menus,
profile choices, source identities, policy, or the construction cutoff.
Its JSON preflight/output repeats both `operator_launch_worker_image` for fleet
launch and `gateway_enforced_worker_contract_sha256` for claim-time
correctness; neither field implies that the gateway validates the image tag.

`--dry-run` performs the complete authority, live-contract, gateway, seed-plan,
and campaign-policy preflight without invoking the durable coordinator. It
does not create a campaign directory, journal, metadata, lane artifacts, or
gateway tasks, including when gateway validation fails. It is appropriate for
a controlled environment smoke only. The
## Process Manager

`scripts/processes.json` provides five normal operating controls, all using
direct `.venv\\Scripts` wrappers rather than `uv run`:

1. `Lab Gateway`
2. `Phase 3 PlayHand - Fresh`
3. `Phase 3 PlayHand - Resume`
4. `AutoResearch Dashboard`
5. `Phase 3 Authority Audit`

Fresh and resume share the same immutable authority, capsule, policy, gateway,
and Trading-Dashboard paths. They differ only by `--fresh` versus `--resume`.
The configured `--active-runs 128` is operational capacity; campaign ID, target
run count, construction cutoff, seed plan, source identities, lane policy, and
reserved-tail rule remain authority-bound and cannot be overridden by Procman.

The audit control is read-only and revalidates the authority against its
capsule and policy before a coordinator launch. Corpus maintenance, safe cleanup
previews, historical Level C audit, and manual Atlas stages are intentionally
kept outside the normal operating group. No Procman reload or process start is
part of this configuration change.
