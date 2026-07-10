param(
  [int]$Port = 8799,
  [string]$HostName = "127.0.0.1"
)

$ErrorActionPreference = "Stop"
Set-Location "C:\repos\fuzzfolio-autoresearch"
$env:UV_CACHE_DIR = "C:\repos\fuzzfolio-autoresearch\.uv-cache"

uv run play-hand-massive-v2-gateway `
  --host $HostName `
  --port $Port `
  --lease-ttl-seconds 600 `
  --max-recent-completions 5000 `
  --max-result-backlog 50000 `
  --max-result-backlog-mb 2048 `
  --result-backpressure-mb 512 `
  --max-recent-terminal-task-ids 100000 `
  --worker-stale-after-seconds 600 `
  --worker-prune-after-seconds 1800 `
  --lake-mutation-retry-after-seconds 90 `
  --lake-timeout-retry-after-seconds 45
