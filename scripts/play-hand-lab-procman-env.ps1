$ErrorActionPreference = 'Stop'

$tokenFile = $env:FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE
if (-not $tokenFile) {
    $tokenDir = Join-Path $env:LOCALAPPDATA 'FuzzfolioAutoResearch'
    $tokenFile = Join-Path $tokenDir 'play-hand-lab-gateway-token.txt'
}

if (-not $env:FUZZFOLIO_LAB_GATEWAY_TOKEN) {
    $tokenDir = Split-Path -Parent $tokenFile
    if ($tokenDir -and -not (Test-Path -LiteralPath $tokenDir)) {
        New-Item -ItemType Directory -Path $tokenDir -Force | Out-Null
    }

    $token = ''
    if (Test-Path -LiteralPath $tokenFile) {
        $token = (Get-Content -LiteralPath $tokenFile -Raw).Trim()
    }
    if (-not $token) {
        $token = -join (1..4 | ForEach-Object { [guid]::NewGuid().ToString('N') })
        Set-Content -LiteralPath $tokenFile -Value $token -Encoding ASCII -NoNewline
    }
    $env:FUZZFOLIO_LAB_GATEWAY_TOKEN = $token
}

if (-not $env:FUZZFOLIO_LAB_GATEWAY_TOKEN) {
    throw 'FUZZFOLIO_LAB_GATEWAY_TOKEN could not be loaded or generated.'
}

$env:FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE = $tokenFile
