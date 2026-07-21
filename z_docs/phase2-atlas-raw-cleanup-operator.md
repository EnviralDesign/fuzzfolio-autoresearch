# Phase 2 raw Atlas cleanup operator

`scripts/cleanup-phase2-atlas-raw-roots.ps1` is the only approved deletion
handoff for the four completed Phase 2 raw Atlas roots. It is deliberately not
registered in Procman.

The script has a fixed allowlist and cannot accept arbitrary deletion paths:

- `atlas-lc-a-e2ff10eedfa1084b`
- `atlas-lc-b-ea7140af612cf191`
- `atlas-lc-c-5c251563f9035fe9`
- `atlas-lc-d-8af1d45d58d5bb40`

Each must be an existing, non-reparse direct child of
`runs\derived\atlas-runs`. The script walks each tree without following
reparse points, counts its files/bytes, and refuses the operation if it sees a
link, junction, unsupported filesystem entry, missing authority record, or
unexpected target path.

## Preconditions

Before deletion, the script requires all of the following:

1. Verification of the local Phase 2 authority capsule.
2. Verification of the Y: archive copy of that capsule.
3. A final v2 Phase 3 authority audit against the local capsule and checked-in
   policy manifest.
4. A reachable Procman with **no** managed AutoResearch process running. This
   includes the Lab Gateway.
5. A `vastai show instances-v1 --raw` result with zero instances. An operator
   may explicitly record a nonzero Vast precondition with
   `-AllowNonZeroVastInstances`, but should use it only after confirming those
   instances cannot touch this repository.

The current Codex tool session is policy-blocked from performing the recursive
deletion. This script is an operator handoff for a reviewed PowerShell session;
it was added but has not been run with `-Apply`.

## Preview

Preview is the default and never deletes or writes a receipt. It first checks
Procman and Vast and proves target containment. When those runtime checks pass,
it verifies the capsule/authority and computes projected file and byte counts.
A blocked preview deliberately skips the expensive capsule and raw-tree walks.

```powershell
Set-Location C:\repos\fuzzfolio-autoresearch
.\scripts\cleanup-phase2-atlas-raw-roots.ps1
```

Review the JSON. `preview_ready_for_explicit_apply` means the current checks
passed. `preview_blocked` reports active Procman processes or nonzero Vast
instances. Any verification or path-protection failure returns a nonzero exit
code and makes no filesystem change.

## Apply

Stop the Lab Gateway and every other managed AutoResearch process through
Procman first. Destroy Vast capacity, then confirm both states again. Only then
run the explicit apply form in an interactive, reviewed shell:

```powershell
Set-Location C:\repos\fuzzfolio-autoresearch
.\scripts\cleanup-phase2-atlas-raw-roots.ps1 -Apply -Confirm
```

PowerShell asks for confirmation for every cutoff. The script deletes one root
at a time. After each successful removal it re-verifies the local capsule, the
archive capsule, and the final Phase 3 authority before continuing. It leaves
no symlink or junction at a removed root path.

On apply, incremental JSON receipts are written under
`runs\derived\cleanup-receipts`. Each receipt contains the exact roots,
timestamps, per-cutoff file/directory counts, reclaimed bytes, precondition
state, and post-delete verification results. An interrupted apply leaves the
last written receipt in place; do not restart blindly. Review it, investigate
the failed condition, and run the default preview again.

Do not add this operation to Procman, do not replace deleted roots with links
to Y:, and do not run it while an active campaign, gateway, or raw-Atlas audit
needs the source trees.
