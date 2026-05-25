# Latest Pro Note After Finalized Priors

Pro approved the accepted-prior rebuild and gave green light to resume normal overnight Play Hand after the Codex restart.

Key instructions:

- Accepted-prior rebuild looks clean.
- Accepted sources are exactly:
  - `cgpt review/playhand-prior-test-clean-50`
  - `cgpt review/playhand-prior-test-clean-100`
  - `cgpt review/playhand-prior-test-clean-50-family-policy-v1.1`
- `family-policy-v1` is explicitly excluded.
- Rebuilt seed plan has:
  - global sampling `70/20/10`
  - guided source mix `60/40` discovered/curated
  - `seed_pool`
  - family policies present

Next local action after restart:

```powershell
uv run play-hand `
  --coarse-mode evolutionary `
  --sweep-budget high `
  --min-indicators 2 `
  --max-indicators 4 `
  --json
```

Use the fleet/process manager as intended. These are normal candidate-generation runs, not controlled outcome-prior refresh runs.

First 10 completed runs guardrail:

- failed run count
- template materialization count
- discovered/curated/policy source mix
- promoted/tombstoned count
- exact-template branches appearing
- template_locked hands staying compact

Do not stop for normal variance. Stop only for clear breakage: repeated execution failures, zero seed-plan usage, zero template materialization after many guided/template selections, or missing final summaries.

Important boundary:

```powershell
uv run build-playhand-outcome-priors --json
```

Do not run that after the overnight loop. Normal overnight runs are corpus growth and manual dashboard curation, not automatic feedback into priors.
