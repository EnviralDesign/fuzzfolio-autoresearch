[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$Test
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$manifest = Join-Path $repoRoot 'rust\temporal-qd\Cargo.toml'
$targetDir = Join-Path $repoRoot 'rust\temporal-qd\target'
$env:CARGO_BUILD_JOBS = '2'

if ($Check) {
    & cargo fmt --manifest-path $manifest --check
    & cargo clippy --locked --jobs 2 --manifest-path $manifest --all-targets -- -D warnings
}

& cargo build --locked --release --jobs 2 --manifest-path $manifest --target-dir $targetDir -p temporal-qd-batch

if ($Test) {
    & cargo test --locked --jobs 2 --manifest-path $manifest
}
