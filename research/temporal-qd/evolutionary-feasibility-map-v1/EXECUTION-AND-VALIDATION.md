# Execution and validation

All commands were run from the isolated `research/evolutionary-feasibility-map-v1`
worktree. They read retained local files only.

```powershell
uv run pytest tests\test_temporal_qd_evolutionary_feasibility_map.py -q
uv run python -m py_compile autoresearch\temporal_qd_evolutionary_feasibility_map.py

uv run python -m autoresearch.temporal_qd_evolutionary_feasibility_map `
  --cohort v37=C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37 `
  --cohort v38=C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38 `
  --reference topology=C:\repos\fuzzfolio-autoresearch\runs\topology-v2-5-launch-ready-20260825 `
  --output-dir C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\evolutionary-feasibility-map-v1\regeneration-g
```

The same final command was independently repeated to `regeneration-h`. Every
generated file had the same SHA-256 in both directories.

## Results

- Focused tests: `2 passed`.
- Python compilation: passed.
- Candidate-window observations: `22,656`; candidate evaluations: `5,664`.
- Exact duplicate candidate-window rows: `0`.
- Non-finite metrics: `0`.
- Gross − modeled cost = conservative net reconciliation failures: `0`.
- Candidate aggregate-to-window reconciliation failures: `0`.
- Bundle reconciliation: V37 `20,480/20,480` matched; V38 `2,164/2,176`
  matched with the remaining twelve documented clone-control rows retained from
  the authoritative V38 score file.
- Manifest self-hash, fixed/adaptive binning presence, and stability-method
  sanity checks: passed.

## Repository preflight

- Remote default branch verified: `origin/master` at
  `51c2f9175f441166e7fc997109e939a9f9103b5d` at preflight time.
- Base used: `282c06eb4be3c66cb59b0348d41adb70c7d64c65`.
- Normal authenticated publication path: `git push --dry-run origin
  HEAD:refs/heads/research/evolutionary-feasibility-map-v1` reported a valid
  new-branch push.

No market evaluation, worker, gateway, Vast instance, generation, archive
mutation, or production artifact rewrite occurred.
