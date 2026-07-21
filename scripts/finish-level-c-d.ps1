[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runsRoot = Join-Path $repoRoot "runs"
$tradingDashboardRoot = "C:\repos\Trading-Dashboard"
$procmanBase = "http://127.0.0.1:47831"
$gatewayUrl = "http://127.0.0.1:8799"
$logRoot = Join-Path $repoRoot ".tmp\manual-level-c-d"
$cPlanPath = Join-Path $runsRoot "derived\level-c\control\execution-plan-C.json"
$dPlanPath = Join-Path $runsRoot "derived\level-c\control\execution-plan-D.json"
$dStageRoot = Join-Path $runsRoot "derived\level-c\campaigns\D\stages"

function Invoke-UvLogged {
    param(
        [Parameter(Mandatory)] [string[]]$Arguments,
        [Parameter(Mandatory)] [string]$Name
    )

    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $logPath = Join-Path $logRoot "$Name-$stamp.log"
    Write-Host "Logging to $logPath"
    & uv @Arguments 2>&1 | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE -ne 0) {
        throw "uv exited $LASTEXITCODE while running $Name. Preserve the log and do not reset the gateway or artifacts."
    }
}

function Assert-NoCutoffRunner {
    param([Parameter(Mandatory)] [ValidateSet("C", "D")] [string]$Cutoff)

    $pattern = "level-c-run-cutoff.*--cutoff\s+$Cutoff(?:\s|$)"
    $matches = @(Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and $_.CommandLine -match $pattern
    })
    if ($matches.Count -gt 0) {
        $pids = ($matches | ForEach-Object ProcessId) -join ", "
        throw "Cutoff $Cutoff already has a local runner (PID $pids)."
    }
}

function Ensure-LabGateway {
    $null = Invoke-RestMethod -Uri "$procmanBase/health"
    $processes = @(Invoke-RestMethod -Uri "$procmanBase/processes")
    $gateway = @($processes | Where-Object Name -eq "Lab Gateway")
    if ($gateway.Count -ne 1) {
        throw "Expected exactly one Lab Gateway procman entry; found $($gateway.Count)."
    }
    if ($gateway[0].status -ne "Running") {
        Write-Host "Starting Lab Gateway through procman..."
        $null = Invoke-RestMethod -Method Post -Uri "$procmanBase/processes/$($gateway[0].id)/start"
        $deadline = (Get-Date).AddSeconds(30)
        do {
            Start-Sleep -Seconds 1
            $current = @(Invoke-RestMethod -Uri "$procmanBase/processes") |
                Where-Object id -eq $gateway[0].id
        } until ($current.status -eq "Running" -or (Get-Date) -ge $deadline)
        if ($current.status -ne "Running") {
            throw "Lab Gateway did not reach Running within 30 seconds."
        }
    }
}

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
    foreach ($planPath in @($cPlanPath, $dPlanPath)) {
        if (-not (Test-Path -LiteralPath $planPath -PathType Leaf)) {
            throw "Missing authoritative execution plan: $planPath"
        }
    }

    Assert-NoCutoffRunner -Cutoff C
    Assert-NoCutoffRunner -Cutoff D
    $cPlan = Get-Content -Raw -LiteralPath $cPlanPath | ConvertFrom-Json
    $cCohortPath = [string]$cPlan.expected_artifacts.frozen_cohort.resolved_path
    if (-not (Test-Path -LiteralPath $cCohortPath -PathType Leaf)) {
        throw "Cutoff C is not terminal: frozen cohort is absent at $cCohortPath"
    }
    Invoke-UvLogged -Name "preflight-cutoff-c-audit" -Arguments @(
        "run", "level-c-audit",
        "--active-runs-root", $runsRoot,
        "--cutoff", "C",
        "--json"
    )

    Ensure-LabGateway

    $plan = Get-Content -Raw -LiteralPath $dPlanPath | ConvertFrom-Json
    $dRoots = @(
        [string]$plan.expected_artifacts.atlas_run.resolved_path,
        [string]$plan.expected_artifacts.playhand_campaign.resolved_path,
        [string]$plan.expected_artifacts.campaign_receipt.resolved_path,
        [string]$plan.expected_artifacts.frozen_cohort.resolved_path
    )
    $resume = (Test-Path -LiteralPath $dStageRoot) -or
        (@($dRoots | Where-Object { $_ -and (Test-Path -LiteralPath $_) }).Count -gt 0)

    $runArguments = @(
        "run", "level-c-run-cutoff",
        "--active-runs-root", $runsRoot,
        "--cutoff", "D",
        "--gateway-url", $gatewayUrl,
        "--atlas-active-probes", "512",
        "--playhand-active-runs", "256",
        "--nested-max-workers", "128",
        "--trading-dashboard-root", $tradingDashboardRoot,
        "--json"
    )
    if ($resume) {
        $runArguments += "--resume"
        Write-Host "Detected existing cutoff D state; strict resume is enabled."
    } else {
        Write-Host "Cutoff D is fresh; starting without --resume."
    }

    Write-Host "Start CLI-only Vast workers only after the gateway shows real queued work."
    Write-Host "Keep this terminal open until D exits."
    Invoke-UvLogged -Name "cutoff-d" -Arguments $runArguments

    Invoke-UvLogged -Name "cutoff-d-audit" -Arguments @(
        "run", "level-c-audit",
        "--active-runs-root", $runsRoot,
        "--cutoff", "D",
        "--json"
    )
    Invoke-UvLogged -Name "level-c-final-audit" -Arguments @(
        "run", "level-c-audit",
        "--active-runs-root", $runsRoot,
        "--json"
    )

    Write-Host "Cutoff D and the final Level C audit completed successfully."
    Write-Host "Destroy any remaining Vast instances, then continue with the Phase 2 prior comparison gate."
} finally {
    Pop-Location
}
