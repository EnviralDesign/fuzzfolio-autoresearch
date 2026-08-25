# Rust-canonical topology V2.3 operator runbook

This runbook is operational guidance only. The committed launch control has
`dispatchEnabled: false`; do not contact a gateway or start workers without a
separate human authorization for the exact production action.

## Frozen inputs

- AutoResearch branch: `codex/rust-cutover-worker-seam-v2-3`
- FuzzFolio worker source: `0fbe84a9f7b73b97789c8370b268f4d01eeb37ce`
- Worker contract: `sha256:ae5d0e53aa19e1e241468c009e248457560ca63e2e3d785854750b028736c9df`
- Worker image: `sha256:1817ddc68b55433bb81c59572e51d5dddc40e2a95ac9004fafee979adbb913fe`
- Replication rule: `sha256:c8f878ccd03e7f9fb54228836a165d1f753a712d32bd423dcd48d31262e4db04`
- Launch control: `sha256:5887bd924bd76809d59627ee88d1962276fd11a88e20276476f44cc56008270b`
- Task mapping: `sha256:ef422b666734066052fd7bceebec4d5734572a72923c490b98401aaeeae2c8a9`

The three campaign-input checkpoints are:

| Panel | Checkpoint SHA-256 | Task-matrix SHA-256 | Shape |
| --- | --- | --- | --- |
| panel-1 | `sha256:c24687ab0fc60a72197c82c2734aba4b9f4dd64e88636b4acb61f27ef07a51af` | `sha256:3a7a634ed40adbdef62ef09b5635ebfe0c969635f1cc03de83f01e1b5080d218` | 12 × 4 = 48 |
| panel-2 | `sha256:2d5451718dfa6c20cd1fb625f0e22ee06c6961a28274d9c5788ec937d4a9d0e3` | `sha256:335007e7f075b82525d8f5cde84b02a03163044631f2c2f2922552560e875cd2` | 12 × 4 = 48 |
| panel-3 | `sha256:6d07a4aa8b935f0a535228c6a26761db89af172523eb5ec573427d35eb2b552c` | `sha256:8b1476734aec620e518ad88b459bebef7b0346ece87fb4df8a88ca9ac6a9e257` | 12 × 4 = 48 |

## Preflight

1. Require clean, pinned AutoResearch and FuzzFolio worktrees and verify every
   committed self-hash test.
2. Regenerate the package in two distinct absolute roots. Require byte equality
   for each checkpoint, task pack, cohort, launch control, mapping, and output
   template.
3. Rebuild all seven Rust control-plane binaries and freeze one complete runtime
   authority before opening a campaign output.
4. Confirm the launch gate is true while `dispatchEnabled` remains false and
   untouched confirmation remains `pending`.
5. Store the gateway bearer token in a restricted file outside the run and audit
   roots. Never place it in a command transcript, manifest, ZIP, or Git.

## Authorized fresh dispatch

For each panel, substitute an absolute package root, a new empty output root,
the authorized gateway URL, and a token file:

```powershell
temporal-qd-gateway-dispatch.exe `
  --campaign-input-checkpoint <package-root>\panel-1\campaign-input-checkpoint.json `
  --output-root <gateway-output-root>\panel-1 `
  --gateway-url <authorized-gateway-url> `
  --gateway-token-file <restricted-token-file> `
  --fresh
```

Never reuse an output root with `--fresh`. Run panels independently and retain
their exact execution receipts and result packs.

## Resume

Use the same checkpoint, gateway output root, gateway URL, and token authority,
changing only `--fresh` to `--resume`. Resume must reopen the existing task
index and completion journal; it must not create a second economic evaluation
for a durably completed task.

## Campaign output

Use `topology-production-output-templates-v1.json` to bind each exact checkpoint,
gateway receipt, panel authority, and cohort source. Build the complete native
runtime authority and call `run_native_campaign_output`. Reopen the same manifest
once and require `restart: true` before analysis.

## Post-run analysis

Reduce each complete P/T/E/TE block independently within panel 3, panel 1, and
panel 2 using `temporal_qd_topology_post_run_analyzer_v1.py`. A panel qualifies
only when its full useful-progressive-innovation predicate is true. Then apply:

```text
developmentQualified = U(panel-3)
replicationSurviving = U(panel-1) AND U(panel-2)
inspectedPromising = developmentQualified AND replicationSurviving
```

Do not pool or compensate across panels. Missing, duplicate, invalid, or
identity-drifted evidence is `incomplete_invalid`. Inspected success remains
pending until the separately predeclared untouched panel independently passes
the same local predicate.

## Immediate stop conditions

Stop without retrying or broadening scope when any checkpoint, task matrix,
worker contract/image, runtime authority, gateway receipt, result admission,
campaign output, panel identity, reducer self-hash, or cross-root comparison
drifts. Also stop for a dirty pinned worktree, an unexpected legacy job/result,
an attempted real dispatch before authorization, missing durable-before-ack
evidence, any market read during local conformance, or any temptation to treat
missing evidence as an empirical failure.
