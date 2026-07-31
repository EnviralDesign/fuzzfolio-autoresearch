# Codex Runbook: Stage 5E-0 Temporal Discovery Pilot

## Purpose

Natively verify the FuzzFolio pre-market validator and AutoResearch deterministic
progressive discovery controller. If every gate verifies, run exactly the frozen
256-program non-reserved development pilot and stop after its final audit.

## Required Heads

Pull and require these exact starting heads before application:

```text
FuzzFolio main: 8b966a592e750434a75669725096f72926214995
AutoResearch master: 8b990da10329f7e94b33d730d73d988666143406
```

The AutoResearch discovery bundle is applied on top of that exact master head.
Do not patch or regenerate identities on failure.

```text
FuzzFolio: C:\repos\Trading-Dashboard
AutoResearch: C:\repos\fuzzfolio-autoresearch
```

Preserve byte-exact:

```text
C:\repos\Trading-Dashboard\backend\generated\public\market-structure.json
A7C2E4971EEC6979E71CD99C0893B5775447B273653E6021E697DA9C8DE11655
```

Preserve all existing evidence roots. Do not use `git clean`.

## Gate A — FuzzFolio Candidate Validation

From FuzzFolio:

```powershell
uv sync --project shared/python/fuzzfolio_data --frozen
uv run --project shared/python/fuzzfolio_data python -m py_compile `
  shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/search_validation.py `
  scripts/temporal_search_validate_candidate.py `
  shared/python/fuzzfolio_core/tests/test_temporal_search_candidate_validation.py
uv run --project shared/python/fuzzfolio_data --with pytest python -m pytest -q `
  shared/python/fuzzfolio_core/tests/test_temporal_search_candidate_validation.py `
  shared/python/fuzzfolio_core/tests/test_temporal_graph_validator.py `
  shared/python/fuzzfolio_core/tests/test_temporal_graph_identity.py `
  shared/python/fuzzfolio_core/tests/test_temporal_graph_management_plans.py `
  shared/python/fuzzfolio_core/tests/test_temporal_graph_scalar_runtime.py
```

Explicitly prove CLI exits:

```text
0 accepted valid explicit-management candidate
2 deterministic semantic/search rejection
1 malformed input or raw identity mismatch
```

No Lake, Gateway, worker, Redis, or Appwrite access is permitted in Gate A.

## Gate B — AutoResearch Pure Controller

From AutoResearch:

```powershell
uv sync --frozen
$DiscoveryModules = @(Get-ChildItem autoresearch/temporal_discovery*.py |
  ForEach-Object { $_.FullName })
uv run python -m py_compile @DiscoveryModules `
  tests/test_temporal_discovery.py `
  tests/test_temporal_discovery_prepare.py
uv run --with pytest python -m pytest -q `
  tests/test_temporal_discovery.py `
  tests/test_temporal_discovery_prepare.py `
  tests/test_temporal_search.py `
  tests/test_temporal_search_preflight.py `
  tests/test_processes_config.py
```

Require deterministic equality across repeated generation and shuffled result
arrival, distinct economic/novelty paths, atomic immutable outputs, audit success,
and exact 512/192/704 task ceilings.

## Gate C — Real 256-Program Plan-Only Preparation

Build a pilot-input JSON from:

```text
at least three diverse admitted temporal seed profiles
the four non-reserved development windows
two initial windows: A and C
two confirmation windows: B and D
one replay-evidence-plan-v2 template per window
current FuzzFolio commit
current worker contract identity and schema
all explicit prohibited/reserved evidence windows
generatorSeed = 20260731
barLimit = 5000
```

Use the exact published worker contract:

```text
sha256:65dfdee9ea742ff4474dc8829969a1bb6adc5db0214dcfebadac10f2fb9a925d
replay-worker-contract-v1
```

Create a new external root, never an existing Stage 5C/5D root:

```text
C:\repos\temporal-search-discovery-pilot\<UTC-run-id>
```

Run:

```powershell
uv run python -m autoresearch.temporal_discovery_prepare_cli `
  --pilot-input <pilot-input.json> `
  --output <root>\discovery-preparation.json
```

Create `<root>\validator-command.json` as the exact JSON string array invoking the
pulled FuzzFolio validator through its frozen uv environment. Then run generation:

```powershell
uv run python -m autoresearch.temporal_discovery_cli generate `
  --preparation <root>\discovery-preparation.json `
  --output-root <root>\discovery `
  --validator-command-file <root>\validator-command.json
```

Require:

```text
256 accepted unique program identities
70/30 source-mode allocation as frozen
all accepted candidates valid_evaluable
no raw/profile/program identity disagreement
initial authority = 256 candidates × windows A,C = 512 tasks
```

Audit and plan-only materialize the initial authority:

```powershell
uv run temporal-search-authority --authority-path `
  <root>\discovery\initial\authority.json --audit
uv run temporal-search --fresh --plan-only `
  --authority-path <root>\discovery\initial\authority.json `
  --output-root <root>\initial-run `
  --gateway-url http://127.0.0.1:8799
uv run python -m autoresearch.temporal_discovery_cli audit `
  --discovery-root <root>\discovery
```

Stop on any failure before economic execution.

## Gate D — Initial Distributed Screening

Only after A–C verify:

1. Confirm Procman Lab Gateway and dedicated temporal worker are healthy.
2. Point the local **Temporal Search - Fresh** control at the exact initial
   authority and `<root>\initial-run`. If the existing local control is fixed to
   an older preflight path, preserve that file and retarget only the local
   topology; do not change committed source.
3. Start Fresh once.
4. Wait for the finite controller to stop normally.
5. Audit materialized results, checkpoint, summary, Gateway acknowledgements,
   duplicates/redelivery, task count, attempts, timings, RSS, and artifact bytes.

Require exactly 512 completed candidate/window tasks and both cost views bound to
one stream in every task.

Then freeze the confirmation selection:

```powershell
uv run python -m autoresearch.temporal_discovery_cli select `
  --discovery-root <root>\discovery `
  --initial-result-root <root>\initial-run
```

Require economic archive <=64, novelty archive <=64, resolved-program duplicate
collapse recorded, and deterministic confirmation union <=96.

Audit and plan-only the confirmation authority before starting it.

## Gate E — Confirmation and Finalization

Retarget the local Fresh control to the immutable confirmation authority and a
new `<root>\confirmation-run`, then start it once. Require no more than 192 tasks.

Finalize:

```powershell
uv run python -m autoresearch.temporal_discovery_cli finalize `
  --discovery-root <root>\discovery `
  --initial-result-root <root>\initial-run `
  --confirmation-result-root <root>\confirmation-run
uv run python -m autoresearch.temporal_discovery_cli audit `
  --discovery-root <root>\discovery
```

Independently create and rehash `SHA256SUMS.txt` for the entire external run root.

## Required Return

Return:

1. exact repo heads and all sync/compile/test outputs;
2. validation CLI contract evidence;
3. preparation, discovery authority, population, and journal identities;
4. proposal funnel counts by source mode, mutation family, validation status,
   invalid reason, and duplicate disposition;
5. initial and confirmation finite authority identities/task counts;
6. complete Gateway/worker/ack/retry/redelivery evidence;
7. performance, RSS, and artifact growth;
8. resolved-program duplicate groups;
9. economic and novelty archives with transparent metrics/fingerprints;
10. final report, manifest, and checksum inventory;
11. confirmation that protected/reserved evidence was not accessed;
12. classification:

```text
verified pilot
focused defect
architecture contradiction
```

Stop after the final audit. Do not launch 5,000 programs, use protected evidence,
construct a portfolio, or promote any strategy.
