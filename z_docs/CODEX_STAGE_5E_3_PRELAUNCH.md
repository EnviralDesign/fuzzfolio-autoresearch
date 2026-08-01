# Codex runbook: Stage 5E-3 screening prelaunch

This runbook freezes and verifies the modest Stage 5E-3 campaign through the
screening-prelaunch checkpoint. It must stop before starting the Lab Gateway or
submitting E/F work.

## Frozen inputs

Require:

```text
FuzzFolio: 8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69
worker: sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9
Stage 5E-2 checkpoint: sha256:e9144bfa1e98d53a33382393f9fe294f29f36800a698538285bdd5b8ff90c4d1
```

Preserve all earlier evidence roots and the local FuzzFolio generated
`market-structure.json` edit. Do not use `git clean`.

## Repository gates

From AutoResearch:

```powershell
uv sync --frozen

$TemporalModules = @(Get-ChildItem autoresearch/temporal_discovery*.py, `
  autoresearch/temporal_search*.py | ForEach-Object { $_.FullName })

uv run python -m py_compile @TemporalModules `
  scripts/temporal_policy_v2_native_witness.py `
  scripts/temporal_stage5e3_native_validation.py

uv run --with pytest python -m pytest -q `
  tests/test_temporal_discovery.py `
  tests/test_temporal_discovery_prepare.py `
  tests/test_temporal_search.py `
  tests/test_temporal_search_preflight.py `
  tests/test_temporal_search_quality.py `
  tests/test_temporal_search_activation.py `
  tests/test_temporal_search_policy_v2.py `
  tests/test_temporal_search_selector_v2.py `
  tests/test_temporal_search_stage5e3.py `
  tests/test_processes_config.py
```

Require the hosted `Temporal search discovery controller` workflow to pass on
the exact implementation commit.

## Window freeze

Create a new absent external root and run:

```powershell
uv run temporal-search-stage5e3 select-windows --output-root <root>
```

Require metadata-only selection, 11 eligible unused Level-C month blocks, four
disjoint selected blocks, no A-D or protected overlap, and one frozen promoted
coverage cutoff. Resolve the union catalog scope and freeze one exact
`require_complete` v2 evidence plan for each selected block. Ranking must not
read or depend on the returned semantic hashes.

## Population and witnesses

Run generator v2 with the exact profile:

```text
stage5e3_modest_policy_validation
```

Require 128 unique programs, exactly 64 per source mode, all admitted static
checks, repeated and `PYTHONHASHSEED=1..5` identity equality, positive native
witness coverage for every authored capability, the two negative witnesses, and
exact restart for every witness.

## Screening plan-only

Freeze the E/F screening preparation and authority with:

```powershell
uv run temporal-search-stage5e3 prepare-screening `
  --root <root> `
  --source-preparation <Stage-5E-0-preparation> `
  --autoresearch-implementation-commit <exact-commit> `
  --fuzzfolio-commit 8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69 `
  --worker-contract-sha256 sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9
```

Run `scripts/temporal_stage5e3_native_validation.py` from the frozen FuzzFolio
Python environment against `<root>/screening-plan-only/task-manifest.json`.
Require 256/256 replay-evidence-plan and candidate-window job validations and an
exact independent matrix rehash.

## Final prelaunch freeze

Freeze and audit the top-level checkpoint only after the hosted workflow is
green. Require:

```text
status = screening_prelaunch_ready_awaiting_explicit_authorization
Gateway stopped and not contacted
screening result root absent
screeningStarted = false
confirmation authority absent
reservedEvidenceAccessed = false
largeSearchPermitted = false
```

Return the complete package identities, dates, distributions, worker/process
state, and hosted run. Stop for review. Do not start the Gateway or Screening
Fresh from this runbook.
