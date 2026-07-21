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

function Assert-NoAtlasRunner {
    param([Parameter(Mandatory)] [ValidateSet("C", "D")] [string]$Cutoff)

    $planName = "execution-plan-$Cutoff.json"
    $matches = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match "python|uv" -and
        $_.CommandLine -and
        $_.CommandLine -match "atlas-lab" -and
        $_.CommandLine -match [regex]::Escape($planName)
    })
    if ($matches.Count -gt 0) {
        $pids = ($matches | ForEach-Object ProcessId) -join ", "
        throw "Atlas cutoff $Cutoff already has a local runner (PID $pids)."
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

    Assert-NoAtlasRunner -Cutoff C
    Assert-NoAtlasRunner -Cutoff D
    $cPlan = Get-Content -Raw -LiteralPath $cPlanPath | ConvertFrom-Json
    $cAtlasRoot = [string]$cPlan.expected_artifacts.atlas_run.resolved_path
    $cSummaryPath = Join-Path $cAtlasRoot "atlas-lab-summary.json"
    if (-not (Test-Path -LiteralPath $cSummaryPath -PathType Leaf)) {
        throw "Atlas cutoff C is not terminal: summary is absent at $cSummaryPath"
    }
    $cSummary = Get-Content -Raw -LiteralPath $cSummaryPath | ConvertFrom-Json
    if ([string]$cSummary.status -ne "completed") {
        throw "Atlas cutoff C summary is not completed."
    }
    Invoke-UvLogged -Name "preflight-atlas-c-receipt-verification" -Arguments @(
        "run", "atlas-lab",
        "--execution-plan", $cPlanPath,
        "--resume",
        "--gateway-url", $gatewayUrl,
        "--active-probes", "512",
        "--trading-dashboard-root", $tradingDashboardRoot,
        "--json"
    )

    Ensure-LabGateway

    $plan = Get-Content -Raw -LiteralPath $dPlanPath | ConvertFrom-Json
    $dAtlasRoot = [string]$plan.expected_artifacts.atlas_run.resolved_path
    $resume = Test-Path -LiteralPath $dAtlasRoot

    $runArguments = @(
        "run", "atlas-lab",
        "--execution-plan", $dPlanPath,
        "--gateway-url", $gatewayUrl,
        "--active-probes", "512",
        "--trading-dashboard-root", $tradingDashboardRoot,
        "--json"
    )
    if ($resume) {
        $runArguments += "--resume"
        Write-Host "Detected existing Atlas D state; strict resume is enabled."
    } else {
        Write-Host "Atlas D is fresh; starting without --resume."
    }

    Write-Host "Start CLI-only Vast workers only after the gateway shows real queued work."
    Write-Host "Keep this terminal open until D exits."
    Invoke-UvLogged -Name "atlas-d" -Arguments $runArguments

    Invoke-UvLogged -Name "atlas-d-receipt-verification" -Arguments @(
        "run", "atlas-lab",
        "--execution-plan", $dPlanPath,
        "--resume",
        "--gateway-url", $gatewayUrl,
        "--active-probes", "512",
        "--trading-dashboard-root", $tradingDashboardRoot,
        "--json"
    )

    Write-Host "Atlas D and strict receipt verification completed successfully."
    Write-Host "Destroy any remaining Vast instances, then continue with the Phase 2 prior comparison gate."
} finally {
    Pop-Location
}
