[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
param(
    [switch]$Apply,
    [switch]$AllowNonZeroVastInstances,
    [string]$RepoRoot = 'C:\repos\fuzzfolio-autoresearch',
    [string]$LocalCapsule = 'C:\repos\fuzzfolio-autoresearch\runs\derived\phase2-atlas-capsules\phase2-atlas-authority-capsule-20260721',
    [string]$ArchiveRoot = 'Y:\ED-BEAST\C\repos\fuzzfolio-autoresearch\runs_archive',
    [string]$ArchiveCapsule = 'Y:\ED-BEAST\C\repos\fuzzfolio-autoresearch\runs_archive\phase2-atlas-authority-capsule-20260721',
    [string]$AuthorityPath = 'C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\phase3-darwin-rich-ab-v2\phase3-playhand-authority.json',
    [string]$PolicyManifest = 'C:\repos\fuzzfolio-autoresearch\configs\phase3-campaign-policy.json',
    [string]$ProcmanBase = 'http://127.0.0.1:47831',
    [string]$ReceiptDirectory = 'C:\repos\fuzzfolio-autoresearch\runs\derived\cleanup-receipts'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# This is deliberately the entire deletion allowlist. Do not add a wildcard or
# accept arbitrary root names here: each completed Phase 2 cutoff is explicit.
$KnownRoots = [ordered]@{
    A = 'atlas-lc-a-e2ff10eedfa1084b'
    B = 'atlas-lc-b-ea7140af612cf191'
    C = 'atlas-lc-c-5c251563f9035fe9'
    D = 'atlas-lc-d-8af1d45d58d5bb40'
}

function ConvertTo-JsonStable {
    param([Parameter(Mandatory)]$Value)
    $Value | ConvertTo-Json -Depth 32
}

function Write-Receipt {
    param(
        [Parameter(Mandatory)] [Collections.IDictionary]$Receipt,
        [switch]$Persist,
        [switch]$Quiet
    )

    $Receipt.completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    $json = ConvertTo-JsonStable -Value $Receipt
    if ($Persist) {
        $directory = [IO.Path]::GetFullPath($ReceiptDirectory)
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
        $path = Join-Path $directory ("phase2-atlas-raw-cleanup-{0}.json" -f $Receipt.started_at_utc.Replace(':', '').Replace('-', ''))
        [IO.File]::WriteAllText($path, $json + [Environment]::NewLine, [Text.Encoding]::UTF8)
        $Receipt.receipt_path = $path
        $json = ConvertTo-JsonStable -Value $Receipt
        [IO.File]::WriteAllText($path, $json + [Environment]::NewLine, [Text.Encoding]::UTF8)
    }
    if (-not $Quiet) {
        Write-Output $json
    }
}

function Test-ReparsePoint {
    param([Parameter(Mandatory)] [IO.FileSystemInfo]$Item)
    return (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0)
}

function Assert-NoReparsePoint {
    param(
        [Parameter(Mandatory)] [IO.FileSystemInfo]$Item,
        [Parameter(Mandatory)] [string]$Label
    )
    if (Test-ReparsePoint -Item $Item) {
        throw "$Label is a symlink or reparse point: $($Item.FullName)"
    }
}

function Resolve-Directory {
    param(
        [Parameter(Mandatory)] [string]$Path,
        [Parameter(Mandatory)] [string]$Label
    )
    $item = Get-Item -LiteralPath $Path -Force
    if (-not ($item -is [IO.DirectoryInfo])) {
        throw "$Label is not a directory: $Path"
    }
    Assert-NoReparsePoint -Item $item -Label $Label
    return [IO.Path]::GetFullPath($item.FullName).TrimEnd('\')
}

function Get-SafeAtlasTargets {
    param([Parameter(Mandatory)] [string]$ResolvedRepoRoot)

    $atlasRunsRoot = Resolve-Directory -Path (Join-Path $ResolvedRepoRoot 'runs\derived\atlas-runs') -Label 'Atlas runs root'
    $targets = @()
    foreach ($entry in $KnownRoots.GetEnumerator()) {
        $literal = Join-Path $atlasRunsRoot $entry.Value
        $item = Get-Item -LiteralPath $literal -Force
        if (-not ($item -is [IO.DirectoryInfo])) {
            throw "Atlas cutoff $($entry.Key) is not a directory: $literal"
        }
        Assert-NoReparsePoint -Item $item -Label "Atlas cutoff $($entry.Key)"
        $fullPath = [IO.Path]::GetFullPath($item.FullName).TrimEnd('\')
        $parent = $item.Parent.FullName.TrimEnd('\\')
        if (-not [string]::Equals($parent, $atlasRunsRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Atlas cutoff $($entry.Key) is not a direct child of the intended atlas-runs root: $fullPath"
        }
        if (-not [string]::Equals($item.Name, $entry.Value, [StringComparison]::Ordinal)) {
            throw "Atlas cutoff $($entry.Key) has an unexpected leaf name: $fullPath"
        }
        $targets += [pscustomobject]@{ cutoff = $entry.Key; path = $fullPath }
    }
    return $targets
}

function Measure-SafeDirectory {
    param([Parameter(Mandatory)] [string]$Path)

    $root = Get-Item -LiteralPath $Path -Force
    Assert-NoReparsePoint -Item $root -Label 'deletion target'
    $stack = [Collections.Generic.Stack[IO.DirectoryInfo]]::new()
    $stack.Push($root)
    [Int64]$bytes = 0
    [Int64]$files = 0
    [Int64]$directories = 0
    while ($stack.Count -gt 0) {
        $directory = $stack.Pop()
        Assert-NoReparsePoint -Item $directory -Label 'directory within deletion target'
        foreach ($child in $directory.EnumerateFileSystemInfos()) {
            Assert-NoReparsePoint -Item $child -Label 'item within deletion target'
            if ($child -is [IO.DirectoryInfo]) {
                $directories++
                $stack.Push($child)
            } elseif ($child -is [IO.FileInfo]) {
                $files++
                $bytes += $child.Length
            } else {
                throw "Unsupported filesystem item within deletion target: $($child.FullName)"
            }
        }
    }
    return [ordered]@{ files = $files; directories = $directories; bytes = $bytes }
}

function Invoke-JsonCommand {
    param(
        [Parameter(Mandatory)] [string]$Executable,
        [Parameter(Mandatory)] [string[]]$Arguments,
        [Parameter(Mandatory)] [string]$Label
    )
    if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
        throw "Missing $Label executable: $Executable"
    }
    $raw = & $Executable @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code ${LASTEXITCODE}: $($raw | Out-String)"
    }
    $text = ($raw | Out-String).Trim()
    try {
        return $text | ConvertFrom-Json -Depth 32
    } catch {
        throw "$Label did not return valid JSON: $text"
    }
}

function Invoke-AuthorityVerification {
    param([Parameter(Mandatory)] [string]$ResolvedRepoRoot)

    $venvScripts = Join-Path $ResolvedRepoRoot '.venv\Scripts'
    $capsuleExecutable = Join-Path $venvScripts 'phase2-atlas-capsule.exe'
    $authorityExecutable = Join-Path $venvScripts 'phase3-playhand-authority.exe'
    $capsuleRoot = [IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($LocalCapsule))
    $local = Invoke-JsonCommand -Executable $capsuleExecutable -Arguments @(
        '--mode', 'verify', '--capsule-root', $capsuleRoot, '--capsule', $LocalCapsule, '--json'
    ) -Label 'local capsule verification'
    $archive = Invoke-JsonCommand -Executable $capsuleExecutable -Arguments @(
        '--mode', 'verify', '--archive-root', $ArchiveRoot, '--capsule', $ArchiveCapsule, '--json'
    ) -Label 'archive capsule verification'
    $authority = Invoke-JsonCommand -Executable $authorityExecutable -Arguments @(
        '--phase2-capsule-root', $LocalCapsule,
        '--policy-manifest', $PolicyManifest,
        '--authority-path', $AuthorityPath,
        '--audit', '--json'
    ) -Label 'Phase 3 authority audit'
    return [ordered]@{
        local_capsule = $local
        archive_capsule = $archive
        phase3_authority = $authority
    }
}

function Get-ManagedProcessPrecondition {
    try {
        $null = Invoke-RestMethod -Uri "$ProcmanBase/health" -TimeoutSec 10
        $response = Invoke-RestMethod -Uri "$ProcmanBase/processes" -TimeoutSec 10
    } catch {
        throw "Unable to prove AutoResearch managed-process state through Procman at ${ProcmanBase}: $($_.Exception.Message)"
    }
    if ($response -is [Array]) {
        $processes = @($response)
    } elseif ($response.PSObject.Properties.Name -contains 'processes') {
        $processes = @($response.processes)
    } else {
        throw "Procman at ${ProcmanBase} returned no processes collection; refusing to infer an idle state."
    }
    $active = @($processes | Where-Object {
        $status = [string]$_.status
        $status -notin @('Stopped', 'Completed', 'Failed')
    } | ForEach-Object {
        [ordered]@{ id = [string]$_.id; name = [string]$_.name; status = [string]$_.status; pid = $_.pid }
    })
    return [ordered]@{ active_processes = $active; ready = ($active.Count -eq 0) }
}

function Get-VastPrecondition {
    $vast = Get-Command vastai -ErrorAction SilentlyContinue
    if ($null -eq $vast) {
        throw 'Vast CLI (vastai) is required to prove that no paid instances remain.'
    }
    $raw = & $vast.Source show instances-v1 --raw 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Vast CLI failed with exit code ${LASTEXITCODE}: $($raw | Out-String)"
    }
    try {
        $payload = (($raw | Out-String).Trim() | ConvertFrom-Json -Depth 32)
    } catch {
        throw "Vast CLI did not return valid JSON: $($raw | Out-String)"
    }
    if ($payload -is [Array]) {
        $instances = @($payload)
    } elseif ($payload.PSObject.Properties.Name -contains 'instances') {
        $instances = @($payload.instances)
    } else {
        throw 'Vast CLI payload has no instances collection; refusing to infer a zero-instance state.'
    }
    return [ordered]@{
        instance_count = $instances.Count
        explicit_operator_override = [bool]$AllowNonZeroVastInstances
        ready = ($instances.Count -eq 0 -or $AllowNonZeroVastInstances)
    }
}

$receipt = [ordered]@{
    schema_version = 'phase2_atlas_raw_cleanup_receipt_v1'
    operation = if ($Apply) { 'apply' } else { 'preview' }
    dry_run = -not $Apply
    started_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    known_cutoffs = @($KnownRoots.Keys)
    deletions = @()
    status = 'started'
}

try {
    $resolvedRepoRoot = Resolve-Directory -Path $RepoRoot -Label 'repository root'
    $receipt.repo_root = $resolvedRepoRoot
    $receipt.procman_precondition = Get-ManagedProcessPrecondition
    $receipt.vast_precondition = Get-VastPrecondition
    $targets = @(Get-SafeAtlasTargets -ResolvedRepoRoot $resolvedRepoRoot)
    $blockers = @()
    if (-not $receipt.procman_precondition.ready) { $blockers += 'AutoResearch managed processes are still running.' }
    if (-not $receipt.vast_precondition.ready) { $blockers += 'Vast has active instances and no explicit operator override was supplied.' }
    $receipt.precondition_blockers = $blockers
    if ($blockers.Count -gt 0) {
        $receipt.targets = @($targets | ForEach-Object {
            [ordered]@{ cutoff = $_.cutoff; path = $_.path; measurement = $null }
        })
        $receipt.measurement_skipped_reason = 'Runtime preconditions are not satisfied; no large raw-tree walk was performed.'
        if (-not $Apply) {
            $receipt.status = 'preview_blocked'
            Write-Receipt -Receipt $receipt
            exit 0
        }
        throw "Refusing -Apply because preconditions are not met: $($blockers -join ' ')"
    }
    $receipt.pre_delete_verification = Invoke-AuthorityVerification -ResolvedRepoRoot $resolvedRepoRoot
    $targetMeasurements = @()
    foreach ($target in $targets) {
        $targetMeasurements += [ordered]@{
            cutoff = $target.cutoff
            path = $target.path
            measurement = Measure-SafeDirectory -Path $target.path
        }
    }
    $receipt.targets = $targetMeasurements

    if (-not $Apply) {
        $receipt.status = 'preview_ready_for_explicit_apply'
        Write-Receipt -Receipt $receipt
        exit 0
    }
    # Prove that the incremental operator receipt can be written before any
    # destructive action. Subsequent writes update this same timestamped file.
    $receipt.status = 'apply_preconditions_passed'
    Write-Receipt -Receipt $receipt -Persist -Quiet

    foreach ($target in $targets) {
        # Re-measure immediately before each recursive removal to detect a link
        # introduced after preview and to record the actual reclaimable bytes.
        $before = Measure-SafeDirectory -Path $target.path
        if (-not $PSCmdlet.ShouldProcess($target.path, "Permanently remove completed Phase 2 Atlas cutoff $($target.cutoff)")) {
            throw "Deletion was not confirmed for cutoff $($target.cutoff)."
        }
        Remove-Item -LiteralPath $target.path -Recurse -Force -ErrorAction Stop
        $remaining = Get-Item -LiteralPath $target.path -Force -ErrorAction SilentlyContinue
        if ($null -ne $remaining) {
            throw "Deletion left a filesystem entry at $($target.path); refusing to continue."
        }
        $receipt.deletions += [ordered]@{
            cutoff = $target.cutoff
            path = $target.path
            removed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
            files = $before.files
            directories = $before.directories
            bytes_reclaimed = $before.bytes
            path_absent_after_delete = $true
        }
        # The compact evidence and its Phase 3 authority must remain valid
        # after every individual cutoff, not only after the final removal.
        $receipt."post_delete_verification_$($target.cutoff)" = Invoke-AuthorityVerification -ResolvedRepoRoot $resolvedRepoRoot
        Write-Receipt -Receipt $receipt -Persist -Quiet
    }
    $receipt.status = 'applied_complete'
    Write-Receipt -Receipt $receipt -Persist
} catch {
    $receipt.status = if ($Apply) { 'apply_blocked_or_failed' } else { 'preview_failed' }
    $receipt.error = $_.Exception.Message
    Write-Receipt -Receipt $receipt -Persist:$Apply
    exit 1
}
