param(
    [int]$WaitForPid = 0,
    [string]$GpuId = "1",
    [string]$PythonExe = ".\.venv-gpucheck-cu124\Scripts\python.exe",
    [string]$StatusLog = "data\training_runs\longcmp_queue_status.log"
)

$ErrorActionPreference = "Stop"

function Write-Status {
    param(
        [string]$Message
    )

    $line = "[{0}] {1}" -f ([DateTimeOffset]::Now.ToString("u")), $Message
    $line | Tee-Object -FilePath $StatusLog -Append
}

function Invoke-ComparisonRun {
    param(
        [string]$Profile,
        [string]$Label
    )

    $outputPath = "data\training_runs\{0}.json" -f $Label
    Write-Status ("starting {0} profile={1} output={2}" -f $Label, $Profile, $outputPath)

    $env:CUDA_VISIBLE_DEVICES = $GpuId
    & $PythonExe -m autoresearch run --max-steps 100 --explorer-profile $Profile --json 2>&1 |
        Tee-Object -FilePath $outputPath

    if ($LASTEXITCODE -ne 0) {
        Write-Status ("failed {0} exit={1}" -f $Label, $LASTEXITCODE)
        throw "Comparison run failed: $Label"
    }

    Write-Status ("completed {0}" -f $Label)
}

$statusDir = Split-Path -Parent $StatusLog
if ($statusDir -and -not (Test-Path $statusDir)) {
    New-Item -ItemType Directory -Path $statusDir | Out-Null
}

Write-Status "queue boot"

if ($WaitForPid -gt 0) {
    Write-Status ("waiting for existing pid {0}" -f $WaitForPid)
    while ($true) {
        $existing = Get-Process -Id $WaitForPid -ErrorAction SilentlyContinue
        if (-not $existing) {
            break
        }
        Start-Sleep -Seconds 30
    }
    Write-Status ("existing pid {0} finished" -f $WaitForPid)
}

$queue = @(
    @{ Profile = "gemma4-e4b-local-adapter-openv2"; Label = "longcmp_tuned_run2_step100" },
    @{ Profile = "gemma4-e4b-local-vanilla"; Label = "longcmp_vanilla_run1_step100" },
    @{ Profile = "gemma4-e4b-local-vanilla"; Label = "longcmp_vanilla_run2_step100" }
)

foreach ($item in $queue) {
    Invoke-ComparisonRun -Profile $item.Profile -Label $item.Label
}

Write-Status "queue finished"
