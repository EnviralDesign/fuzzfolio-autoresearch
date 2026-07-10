param(
  [Parameter(Mandatory=$true)][string]$WorkerId,
  [string]$Pool = "local-76bb",
  [string]$GatewayUrl = "http://127.0.0.1:8799"
)

$ErrorActionPreference = "Stop"
$env:UV_CACHE_DIR = "C:\repos\fuzzfolio-autoresearch\.uv-cache"
$token = & uv --directory "C:\repos\fuzzfolio-autoresearch" run --no-sync python -c "from autoresearch.play_hand_lab_auth import load_lab_gateway_token; print(load_lab_gateway_token() or '')"
$token = [string]$token
$token = $token.Trim()
if ([string]::IsNullOrWhiteSpace($token)) {
  throw "FUZZFOLIO_LAB_GATEWAY_TOKEN is missing."
}

$env:FUZZFOLIO_WORKER_TRANSPORT = "lab_ws"
$env:FUZZFOLIO_LAB_GATEWAY_URL = $GatewayUrl
$env:FUZZFOLIO_LAB_GATEWAY_TOKEN = $token
$env:FUZZFOLIO_WORKER_ID = $WorkerId
$env:FUZZFOLIO_WORKER_POOL = $Pool

Set-Location "C:\repos\Trading-Dashboard\compute-service"
& "C:\repos\Trading-Dashboard\compute-service\.venv\Scripts\python.exe" -c "from app.cli import sim_worker_replay; sim_worker_replay()"
