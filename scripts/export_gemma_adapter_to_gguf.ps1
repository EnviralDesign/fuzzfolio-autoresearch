param(
    [string]$ModelId = "google/gemma-4-E4B-it",
    [string]$AdapterDir = "data\training_runs\gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1\adapter",
    [string]$ExportRoot = "data\gguf_exports\gemma4_e4b_openv2",
    [string]$PythonExe = ".\.venv-gpucheck-cu124\Scripts\python.exe",
    [string]$LlamaCppDir = "",
    [string]$OutType = "f16",
    [string]$Device = "auto",
    [string]$LmsUserRepo = "local/gemma4-e4b-openv2-tuned",
    [string]$GpuId = "",
    [switch]$ImportToLmStudio,
    [switch]$ReportOnly,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Resolve-LlamaCppDir {
    param(
        [string]$RequestedDir
    )

    if ($RequestedDir) {
        return (Resolve-Path $RequestedDir).Path
    }

    $cacheRoot = Join-Path $env:LOCALAPPDATA "codex-cache"
    $cloneDir = Join-Path $cacheRoot "llama.cpp"
    if (-not (Test-Path $cloneDir)) {
        if ($DryRun) {
            Write-Host "[dry-run] would clone llama.cpp to $cloneDir"
            return $cloneDir
        }
        New-Item -ItemType Directory -Force -Path $cacheRoot | Out-Null
        git clone --depth 1 https://github.com/ggml-org/llama.cpp.git $cloneDir | Out-Host
    }
    return $cloneDir
}

function Invoke-Step {
    param(
        [string]$Label,
        [scriptblock]$Script
    )

    Write-Host "[$Label]"
    if ($DryRun) {
        return
    }
    & $Script
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Label (exit $LASTEXITCODE)"
    }
}

$repoRoot = (Resolve-Path ".").Path
$adapterDirAbs = (Resolve-Path $AdapterDir).Path
$exportRootAbs = Join-Path $repoRoot $ExportRoot
$mergedDir = Join-Path $exportRootAbs "merged_hf"
$ggufDir = Join-Path $exportRootAbs "gguf"
$ggufOut = Join-Path $ggufDir "gemma4-e4b-openv2-tuned-$OutType.gguf"
$manifestPath = Join-Path $exportRootAbs "export_manifest.json"
$llamaCppDirAbs = Resolve-LlamaCppDir -RequestedDir $LlamaCppDir
$converter = Join-Path $llamaCppDirAbs "convert_hf_to_gguf.py"

New-Item -ItemType Directory -Force -Path $mergedDir | Out-Null
New-Item -ItemType Directory -Force -Path $ggufDir | Out-Null

if ($GpuId) {
    $env:CUDA_VISIBLE_DEVICES = $GpuId
}

$manifest = @{
    model_id = $ModelId
    adapter_dir = $adapterDirAbs
    export_root = $exportRootAbs
    merged_dir = $mergedDir
    gguf_path = $ggufOut
    outtype = $OutType
    device = $Device
    llama_cpp_dir = $llamaCppDirAbs
    converter = $converter
    lmstudio_user_repo = $LmsUserRepo
    import_to_lmstudio = [bool]$ImportToLmStudio
    report_only = [bool]$ReportOnly
    dry_run = [bool]$DryRun
}
$manifest | ConvertTo-Json -Depth 4 | Set-Content -Encoding utf8 $manifestPath

Write-Host "Manifest: $manifestPath"
Write-Host "Merged HF: $mergedDir"
Write-Host "GGUF: $ggufOut"

$mergeArgs = @(
    "training\merge_adapter.py",
    "--model-id", $ModelId,
    "--adapter-dir", $adapterDirAbs,
    "--output-dir", $mergedDir,
    "--device", $Device
)
if ($ReportOnly) {
    $mergeArgs += "--report-only"
}

Invoke-Step -Label "merge adapter into HF weights" -Script {
    & $PythonExe @mergeArgs
}

if (-not $ReportOnly) {
    $convertArgs = @(
        $converter,
        $mergedDir,
        "--outfile", $ggufOut,
        "--outtype", $OutType,
        "--use-temp-file"
    )

    Invoke-Step -Label "convert merged HF model to GGUF" -Script {
        & $PythonExe @convertArgs
    }

    if ($ImportToLmStudio) {
        $importArgs = @(
            "import",
            "--yes",
            "--user-repo", $LmsUserRepo,
            "--hard-link",
            $ggufOut
        )
        Invoke-Step -Label "import GGUF into LM Studio" -Script {
            & lms @importArgs
        }
    }
}
