# Stage 4.5B — empirical reachability addendum

This addendum supersedes Stage 4.5A only for its stated evidence gap. V1 is
preserved as the accepted source-only skeleton; V2 adds compact, reproducible
authority and retained-artifact evidence without importing any raw market or
candidate-result payload into Git.

## Tracked outputs

| File | What it proves |
| --- | --- |
| `source-authority-map-v2.json` | Exact Git commit/blob identity, raw-worktree identity, checkout cleanliness, and line-ending/semantic comparison for current and historical source bindings. |
| `capability-ledger-v2.json` | Complete 23-fragment grammar domains, 88 catalog entries with frozen-side inclusion, six V5 operator closures, all 22 classified guards, and telemetry/credit paths. |
| `existing-construction-fixture-v2.json` | Content-addressed references to retained V37/V38 authority, parent, ledger, native manifest, and finalization artifacts; no raw copies. |
| `historical-coverage-v2.json` | Structural retained-artifact coverage by authored/compiled/operator-attempt/activation/reduction/parent/survivor state. |
| `stage-4.5b-protocols-and-evidence.md` | No-market arms, geometry rules, twelve reference-organism boundaries, Ground Zero, thin-Vast design, and Fable questions. |

## Regenerate the source and fixture outputs

Run only from the isolated V2 worktree. All source roots are clean and
read-only; all experimental output goes below AutoResearch `.tmp`.

```powershell
uv run --no-project python scripts/generate_evolutionary_substrate_atlas_v2.py `
  --autoresearch-root <current-autoresearch-master-worktree> `
  --fuzzfolio-root <current-fuzzfolio-main-worktree> `
  --historical-v37-root <autoresearch-v37-worktree> `
  --historical-v38-root <autoresearch-v38-worktree> `
  --historical-fuzzfolio-v38-root <fuzzfolio-v38-worktree> `
  --authority-root <retained-authority-directory> `
  --output-dir research/evolutionary-substrate

uv run --no-project python scripts/build_evolutionary_substrate_fixture.py `
  --authority-root <retained-authority-directory> `
  --v38-run-root <retained-v38-run-root> `
  --output research/evolutionary-substrate/existing-construction-fixture-v2.json
```

Targeted source-identity check:

```powershell
uv run --no-project --with pytest python -m pytest `
  tests/test_generate_evolutionary_substrate_atlas_v2.py -q
```

## Explicit non-claims

Nothing in V2 establishes profitability, runtime frequency, a learned
operator weight, component-local credit, a market-data result, or a preferred
strategy. A constructed/admitted child is only an authority-bound static
artifact. The no-market pilot summaries, logs, and result trees are ignored
artifacts referenced from the audit packet—not Git content.
