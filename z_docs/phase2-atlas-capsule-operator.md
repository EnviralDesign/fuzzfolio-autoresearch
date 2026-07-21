# Phase 2 Atlas authority capsule operator guide

`phase2-atlas-capsule` is the retention handoff for the four completed Phase 2
Atlas cutoffs. It copies only authority records and direct published aggregate
tables. It never deletes or moves raw evidence.

The command requires four explicit Atlas roots and rejects anything outside the
configured Atlas root, any symlink/reparse point (including any existing
ancestor through the volume or UNC-share root), unfinished journal, missing
authority record, or destination collision. A dry run writes nothing.

Mapped-drive and UNC spellings are normalized to the same resolved namespace
for containment checks. The command still rejects a genuine resolved escape or
any reparse point in either lexical path's existing ancestry.

Use a reviewed shell with writers stopped. Create the empty destination parents
once, then run this exact sequence, reviewing each JSON response before moving
to the next command:

```powershell
$repo = 'C:\repos\fuzzfolio-autoresearch'
$capsuleRoot = "$repo\runs\derived\phase2-atlas-capsules"
$localCapsule = "$capsuleRoot\phase2-atlas-authority-capsule-20260721"
$archiveRoot = 'Y:\ED-BEAST\C\repos\fuzzfolio-autoresearch\runs_archive'
$archiveCapsule = "$archiveRoot\phase2-atlas-authority-capsule-20260721"
New-Item -ItemType Directory -Force $capsuleRoot, $archiveRoot | Out-Null
$roots = @(
  "$repo\runs\derived\atlas-runs\atlas-lc-a-e2ff10eedfa1084b",
  "$repo\runs\derived\atlas-runs\atlas-lc-b-ea7140af612cf191",
  "$repo\runs\derived\atlas-runs\atlas-lc-c-5c251563f9035fe9",
  "$repo\runs\derived\atlas-runs\atlas-lc-d-8af1d45d58d5bb40"
)
$rootArgs = $roots | ForEach-Object { @('--atlas-root', $_) }

uv run phase2-atlas-capsule --mode dry-run --repo-root $repo --capsule-root $capsuleRoot --destination $localCapsule @rootArgs --json
uv run phase2-atlas-capsule --mode build --repo-root $repo --capsule-root $capsuleRoot --destination $localCapsule @rootArgs --json
uv run phase2-atlas-capsule --mode verify --capsule-root $capsuleRoot --capsule $localCapsule --json
uv run phase2-atlas-capsule --mode copy --capsule $localCapsule --archive-root $archiveRoot --destination $archiveCapsule --json
uv run phase2-atlas-capsule --mode verify --archive-root $archiveRoot --capsule $archiveCapsule --json
uv run phase2-atlas-capsule --mode cleanup-preview --repo-root $repo --capsule-root $capsuleRoot --capsule $localCapsule --archive-root $archiveRoot --archive-capsule $archiveCapsule @rootArgs --json
```

`cleanup-preview` verifies the local capsule and, when supplied, the archive
capsule byte-for-byte, then re-hashes every retained source file against the
capsule manifest before listing immediate raw files and directories that a human
may separately approve for removal. There is intentionally no cleanup
apply/delete flag in this command.

The manifest is deterministic: its file entries contain the repository-relative
source path, capsule-relative destination path, SHA-256, and byte size. The
manifest excludes only itself; verification rejects missing, altered, extra, or
linked content, including empty extra directories. Failed builds never
recursively remove their temporary directory; an interrupted partial temp tree
is intentionally left for manual inspection rather than risking traversal of a
reparse point.
