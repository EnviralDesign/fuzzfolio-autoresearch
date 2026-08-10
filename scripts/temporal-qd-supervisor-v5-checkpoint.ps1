$ErrorActionPreference = "Stop"

$repoRoot = "C:\repos\fuzzfolio-autoresearch"
$dashboardRoot = "C:\repos\Trading-Dashboard"
$campaignRoot = "C:\fuzzfolio-research\temporal-qd-v5-checkpoint-4000x1024x2-20260809-v1"
$legacyAuthorityRoot = "C:\fuzzfolio-research\temporal-qd-4000x1024x5-20260806-v1\authority"
$runRoot = Join-Path $campaignRoot "run\checkpoint-4000x1024x2-v2"

$dirty = @(git -C $repoRoot status --porcelain --untracked-files=no)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw "AutoResearch must be clean before freezing the checkpoint source commit."
}
$autoresearchCommit = (git -C $repoRoot rev-parse HEAD).Trim()
$executionEngineCommit = (git -C $dashboardRoot rev-parse HEAD).Trim()
if ($autoresearchCommit -notmatch "^[0-9a-f]{40}$" -or $executionEngineCommit -notmatch "^[0-9a-f]{40}$") {
    throw "Unable to resolve exact repository commits for the checkpoint."
}

$supervisor = Join-Path $repoRoot ".venv\Scripts\temporal-qd-supervisor.exe"
$arguments = @(
    "--run-root", $runRoot,
    "--initial-archive", (Join-Path $campaignRoot "authority\gen0-v5-directional-archive.json"),
    "--template-preparation", (Join-Path $campaignRoot "authority\rotating-evidence\panel-1-template-preparation.json"),
    "--parameters", (Join-Path $legacyAuthorityRoot "broad-4000x1024x5-parameters.json"),
    "--construction-catalog", (Join-Path $campaignRoot "authority\rotating-evidence\construction-catalog.json"),
    "--generation-count", "2",
    "--initial-construction-pool-size", "4000",
    "--evaluation-population-size", "1024",
    "--autoresearch-commit", $autoresearchCommit,
    "--execution-engine-commit", $executionEngineCommit,
    "--worker-contract-sha256", "sha256:84b66b41e61b12f7d1ba0739754b49deb6685004a94a3b2c8292fdbf7e7786da",
    "--gateway-url", "http://127.0.0.1:8799",
    "--evaluation-timeout-seconds", "86400",
    "--enqueue-batch-size", "128",
    "--bidirectional-pair-config", (Join-Path $legacyAuthorityRoot "pair-run-config.json"),
    "--evolvable-module-authority-config", (Join-Path $campaignRoot "authority\evolvable-authority-post-tripwire.json"),
    "--rotating-evidence-config", (Join-Path $campaignRoot "authority\rotating-evidence\rotating-evidence-config.json"),
    "--pair-generation-engine", "python_optimized_v1",
    "--pair-generation-timeout-seconds", "14400",
    "--tail-result-mode", "legacy",
    "--generation-funnel-enabled"
)

& $supervisor @arguments
exit $LASTEXITCODE
