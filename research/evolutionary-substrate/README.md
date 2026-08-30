# Stage 4.5A — Canonical Evolutionary Substrate Atlas

## Result

This packet maps the source-pinned organism language from kernel/grammar to
seed/mutation/compiler/runtime/telemetry/credit reachability. It finds a real
language mismatch—runtime vocabulary is broader than the grammar's direct
evolutionary routes—without claiming that any mismatch has a market effect.

The work is intentionally source-only and compile-only. It performed no
market-data read, replay, worker/gateway/Vast call, generation/continuation,
archive or policy mutation, production-source change, or profitability
inference.

## Contents

| File | Purpose |
| --- | --- |
| [source-authority-map.json](source-authority-map.json) | Current/historical commit and source-hash identity map |
| [capability-ledger.json](capability-ledger.json) | 175 source-bound capability records |
| [gap-matrix.json](gap-matrix.json) | Confirmed, hypothesis, and unavailable reachability gaps |
| [functional-anatomy.md](functional-anatomy.md) | Kernel → ecology functional mapping |
| [historical-coverage.md](historical-coverage.md) | Explicit V37/V38 evidence boundary |
| [reference-organisms.md](reference-organisms.md) | 12 construct/compile-only organism classes |
| [compile-only-evidence.json](compile-only-evidence.json) | Executed static/compile test results |
| [static-pilot-and-neighborhood-design.md](static-pilot-and-neighborhood-design.md) | Unexecuted ≤8,000-attempt no-market design |
| [decision-memo.md](decision-memo.md) | Demonstrated vs hypothesis vs speculative claims |
| [ground-zero-delta-atlas.md](ground-zero-delta-atlas.md) | J1→J4 escalation design only |
| [thin-research-pod-spec.md](thin-research-pod-spec.md) | Local, no-infrastructure pod design |
| [fable-handoff.md](fable-handoff.md) | Focused review questions |

## Regeneration

Run from an isolated Stage 4.5A AutoResearch worktree and a pinned, read-only
FuzzFolio worktree. The command reads source/Git objects only.

```powershell
uv run --no-project python scripts/generate_evolutionary_substrate_atlas.py `
  --autoresearch-root <isolated-autoresearch-worktree> `
  --fuzzfolio-root <pinned-read-only-fuzzfolio-worktree> `
  --output-dir research/evolutionary-substrate
```

The source-count tripwires intentionally fail if the grammar, runtime model,
topology operation, primary operator, catalog indicator/timeframe, or port
counts change without revisiting the semantic mapping. Focused validation:

```powershell
$env:FUZZFOLIO_STAGE45A_SOURCE = '<pinned-read-only-fuzzfolio-worktree>'
uv run --no-project --with pytest python -m pytest `
  tests/test_generate_evolutionary_substrate_atlas.py -q
```

## Next authority needed

The only live evidence prerequisite identified is a self-contained, read-only
frozen authority + parent/receipt fixture. Until that exists, the bounded
production-prior and coverage-balanced construction pilot remains a design,
not an execution task.
