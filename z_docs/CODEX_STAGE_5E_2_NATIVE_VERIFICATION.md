# Codex Stage 5E-2 native verification

Run this in PowerShell 7 from `C:\repos\fuzzfolio-autoresearch`. Do not modify or
commit files during the evidence pass. Do not start the Lab Gateway, workers, or
any search authority. Do not install ad hoc packages.

This runbook reads only the already-frozen Stage 5E-0/5E-1 artifacts and produces
synthetic Stage 5E-2 policy evidence. It must not read real or reserved market
data.

## 1. Versions and exact source state

```powershell
$ErrorActionPreference = 'Stop'

git rev-parse HEAD
git status --short
uv --version
uv run python --version
pwsh --version

git -C C:\repos\Trading-Dashboard rev-parse HEAD
git -C C:\repos\Trading-Dashboard status --short
```

The checked-out history must contain AutoResearch implementation commit
`dda96ca4cc7b18ab9512fa6011c36bd65f4a268e`. FuzzFolio must remain at
`8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69`, aside from the user's known local
generated-file and Stage 5C artifact state.

## 2. Frozen sync and repository regressions

```powershell
uv sync --frozen

$tests = Get-ChildItem -LiteralPath tests -Filter 'test_temporal*.py' |
  Sort-Object Name |
  ForEach-Object { $_.FullName }

uv run pytest -q $tests
$pytestExit = $LASTEXITCODE
"pytest_exit_code=$pytestExit"
if ($pytestExit -ne 0) { throw "Temporal regression suite failed." }

uv run python -m compileall -q autoresearch scripts\temporal_policy_v2_native_witness.py
```

## 3. Fixed inputs and fresh output root

```powershell
$stage5e0 = 'C:\repos\temporal-search-discovery-pilot\stage5e0-discovery-pilot-20260731T220000Z-r3'
$stage5e1 = 'C:\repos\temporal-search-discovery-pilot\stage5e1-search-quality-20260731T235022Z'
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$stage5e2 = "C:\repos\temporal-search-discovery-pilot\stage5e2-native-$stamp"

if (Test-Path -LiteralPath $stage5e2) {
  throw "Fresh output root already exists: $stage5e2"
}
New-Item -ItemType Directory -Path $stage5e2 | Out-Null
```

## 4. Historical activation causality

```powershell
uv run temporal-search-activation build `
  --discovery-root "$stage5e0\discovery" `
  --initial-result-root "$stage5e0\initial-run" `
  --confirmation-result-root "$stage5e0\confirmation-run" `
  --control-result-root "$stage5e1\control-run" `
  --output-root "$stage5e2\activation-causality"

uv run temporal-search-activation audit `
  --output-root "$stage5e2\activation-causality"
```

The result must report 256 candidates, 818 task results, 252 historical
management instances, and eight representative dossiers.

## 5. Real-validator generator v2 admission

```powershell
uv run temporal-search-policy-v2 generate `
  --source-preparation "$stage5e0\discovery\preparation.json" `
  --causality-root "$stage5e2\activation-causality" `
  --output-root "$stage5e2\generator-v2" `
  --validator-command-file "$stage5e0\validator-command.json" `
  --validator-timeout-seconds 60

uv run temporal-search-policy-v2 audit-generator `
  --output-root "$stage5e2\generator-v2"
```

The result must contain exactly 256 unique programs: 128
`broad_seed_mutation` and 128 `seed_derived`, with no accepted reachability
issue.

## 6. Native management witnesses

```powershell
uv run --project C:\repos\Trading-Dashboard\shared\python\fuzzfolio_core `
  python scripts\temporal_policy_v2_native_witness.py `
  --population "$stage5e2\generator-v2\population.json" `
  --output-root "$stage5e2\management-witnesses"

uv run temporal-search-policy-v2 audit-witnesses `
  --output-root "$stage5e2\management-witnesses"
```

Every authored capability must have a positive witness, both management families
must have one negative rejection witness, and every witness must restart exactly.

## 7. Generator and selector determinism

```powershell
uv run temporal-search-generator-v2-admission `
  --source-preparation "$stage5e0\discovery\preparation.json" `
  --causality-root "$stage5e2\activation-causality" `
  --native-generator-root "$stage5e2\generator-v2" `
  --output-path "$stage5e2\generator-v2-determinism.json"

uv run temporal-search-selector-v2-admission `
  --population "$stage5e2\generator-v2\population.json" `
  --output-root "$stage5e2\selector-v2-synthetic"
```

Generator ordinary repeat and `PYTHONHASHSEED=1..5` must be exact. Selector
original, reversed, five shuffled orders, and `PYTHONHASHSEED=1..5` must be exact.
The selector must return exactly 32 control candidates and at most 96 total
confirmation candidates.

## 8. Freeze and re-audit the checkpoint

```powershell
uv run temporal-search-stage5e2-checkpoint freeze `
  --root $stage5e2 `
  --autoresearch-commit dda96ca4cc7b18ab9512fa6011c36bd65f4a268e `
  --fuzzfolio-commit 8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69 `
  --worker-contract-sha256 sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9

uv run temporal-search-stage5e2-checkpoint audit --root $stage5e2

Get-ChildItem -LiteralPath $stage5e2 -File -Recurse |
  Sort-Object FullName |
  ForEach-Object {
    $hash = Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256
    [pscustomobject]@{
      RelativePath = $_.FullName.Substring($stage5e2.Length + 1)
      Length = $_.Length
      SHA256 = $hash.Hash
    }
  } |
  Format-Table -AutoSize
```

Return the complete version output, sync output, pytest output and exit code,
generator proposal dispositions, activation taxonomy, witness counts, selector
summary, checkpoint/manifest identities, and the recursive hash table.

Stop after returning the evidence. Do not define a fresh window, evidence plan,
authority, or distributed task. A large search remains blocked pending the user's
deep evidence-shape review.
