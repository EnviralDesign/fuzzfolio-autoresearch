$ErrorActionPreference = "Continue"

$Root = "C:\repos\fuzzfolio-autoresearch"
$OutDir = Join-Path $Root "runs\derived\playhand-prior-test-clean-50-family-policy-v1.1"
$BatchLog = Join-Path $OutDir "batch-run.log"
$ProgressPath = Join-Path $OutDir "batch-progress.jsonl"
$StatusPath = Join-Path $OutDir "batch-status.json"
$Seeds = 201..250

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Write-JsonFile {
    param(
        [string] $Path,
        [hashtable] $Payload
    )
    $Payload | ConvertTo-Json -Depth 12 | Set-Content -Path $Path -Encoding UTF8
}

function Append-JsonLine {
    param([hashtable] $Payload)
    ($Payload | ConvertTo-Json -Depth 12 -Compress) | Add-Content -Path $ProgressPath -Encoding UTF8
}

function Read-Summary {
    param([string] $Path)
    if (-not (Test-Path $Path)) { return $null }
    try { return Get-Content -Path $Path -Raw | ConvertFrom-Json } catch { return $null }
}

Set-Location $Root

$startedAt = (Get-Date).ToUniversalTime().ToString("o")
$completed = 0
$failed = 0
$skipped = 0
$commandTemplate = "uv run play-hand --seed <seed> --coarse-mode evolutionary --sweep-budget high --min-indicators 2 --max-indicators 4 --final-profile-drop-count 0 --json"

Write-JsonFile -Path $StatusPath -Payload @{
    status = "running"
    started_at = $startedAt
    updated_at = (Get-Date).ToUniversalTime().ToString("o")
    pid = $PID
    total = $Seeds.Count
    completed = 0
    failed = 0
    skipped = 0
    current_seed = $null
    command_template = $commandTemplate
    seed_start = $Seeds[0]
    seed_end = $Seeds[-1]
}

"[$startedAt] Starting clean 50-seed Play Hand family-policy v1.1 source-mix confirmation batch" | Add-Content -Path $BatchLog -Encoding UTF8
"Command template: $commandTemplate" | Add-Content -Path $BatchLog -Encoding UTF8

foreach ($seed in $Seeds) {
    $seedLabel = "{0:D3}" -f $seed
    $seedLog = Join-Path $OutDir "seed-$seedLabel.log"
    $seedSummary = Join-Path $OutDir "seed-$seedLabel-summary.json"
    $seedStatus = Join-Path $OutDir "seed-$seedLabel-status.json"

    if (Test-Path $seedSummary) {
        $existing = Read-Summary -Path $seedSummary
        if ($null -ne $existing -and $existing.run_id) {
            $skipped += 1
            Append-JsonLine @{
                ts = (Get-Date).ToUniversalTime().ToString("o")
                seed = $seed
                status = "skipped_existing"
                run_id = $existing.run_id
                run_status = $existing.run_status
                final_scrutiny_score = $existing.final_scrutiny_score
                selected_final_branch = $existing.selected_final_branch
            }
            continue
        }
    }

    Write-JsonFile -Path $StatusPath -Payload @{
        status = "running"
        started_at = $startedAt
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        pid = $PID
        total = $Seeds.Count
        completed = $completed
        failed = $failed
        skipped = $skipped
        current_seed = $seed
        command_template = $commandTemplate
        seed_start = $Seeds[0]
        seed_end = $Seeds[-1]
    }

    $seedStarted = (Get-Date).ToUniversalTime().ToString("o")
    "[$seedStarted] Starting seed $seed" | Add-Content -Path $BatchLog -Encoding UTF8

    $args = @(
        "run", "play-hand",
        "--seed", "$seed",
        "--coarse-mode", "evolutionary",
        "--sweep-budget", "high",
        "--min-indicators", "2",
        "--max-indicators", "4",
        "--final-profile-drop-count", "0",
        "--json"
    )

    & uv @args *>&1 | Tee-Object -FilePath $seedLog
    $exitCode = $LASTEXITCODE
    $seedEnded = (Get-Date).ToUniversalTime().ToString("o")
    $text = ""
    if (Test-Path $seedLog) { $text = Get-Content -Path $seedLog -Raw }
    $match = [regex]::Match($text, '"run_id"\s*:\s*"([^"]+)"')
    $runId = $null
    if ($match.Success) { $runId = $match.Groups[1].Value }

    $summaryPath = $null
    $summary = $null
    if ($runId) {
        $candidateSummary = Join-Path $Root "runs\$runId\play-hand-summary.json"
        if (Test-Path $candidateSummary) {
            Copy-Item -Force $candidateSummary $seedSummary
            $summaryPath = $candidateSummary
            $summary = Read-Summary -Path $seedSummary
        }
    }

    if ($exitCode -eq 0 -and $null -ne $summary) {
        $completed += 1
        $record = @{
            ts = $seedEnded
            seed = $seed
            status = "completed"
            exit_code = $exitCode
            run_id = $summary.run_id
            run_status = $summary.run_status
            final_scrutiny_score = $summary.final_scrutiny_score
            selected_final_branch = $summary.selected_final_branch
            canonical_selection_reason = $summary.canonical_selection_reason
            exact_template_score = $summary.exact_template_score
            mutated_score = $summary.mutated_score
            dealt_recipe = $summary.dealt_recipe
            dealt_recipe_source = $summary.dealt_recipe_source
            dealt_pair_family_policy = $summary.dealt_pair_family_policy
            dealt_policy_target_count = $summary.dealt_policy_target_count
            dealt_indicator_count = $summary.dealt_indicator_count
            template_branch_source_probe_id = $summary.template_branch_source_probe_id
            summary_path = $summaryPath
        }
        Write-JsonFile -Path $seedStatus -Payload $record
        Append-JsonLine $record
        "[$seedEnded] Seed $seed completed run=$($summary.run_id) status=$($summary.run_status) score=$($summary.final_scrutiny_score) branch=$($summary.selected_final_branch)" | Add-Content -Path $BatchLog -Encoding UTF8
    } else {
        $failed += 1
        $record = @{
            ts = $seedEnded
            seed = $seed
            status = "failed"
            exit_code = $exitCode
            run_id = $runId
            summary_path = $summaryPath
            log_path = $seedLog
        }
        Write-JsonFile -Path $seedStatus -Payload $record
        Append-JsonLine $record
        "[$seedEnded] Seed $seed failed exit=$exitCode run=$runId log=$seedLog" | Add-Content -Path $BatchLog -Encoding UTF8
    }

    Write-JsonFile -Path $StatusPath -Payload @{
        status = "running"
        started_at = $startedAt
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        pid = $PID
        total = $Seeds.Count
        completed = $completed
        failed = $failed
        skipped = $skipped
        current_seed = $seed
        command_template = $commandTemplate
        seed_start = $Seeds[0]
        seed_end = $Seeds[-1]
    }
}

$finishedAt = (Get-Date).ToUniversalTime().ToString("o")
Write-JsonFile -Path $StatusPath -Payload @{
    status = "completed"
    started_at = $startedAt
    finished_at = $finishedAt
    updated_at = $finishedAt
    pid = $PID
    total = $Seeds.Count
    completed = $completed
    failed = $failed
    skipped = $skipped
    current_seed = $null
    command_template = $commandTemplate
    seed_start = $Seeds[0]
    seed_end = $Seeds[-1]
}
"[$finishedAt] Finished clean 50-seed Play Hand family-policy v1.1 source-mix confirmation batch completed=$completed failed=$failed skipped=$skipped" | Add-Content -Path $BatchLog -Encoding UTF8

