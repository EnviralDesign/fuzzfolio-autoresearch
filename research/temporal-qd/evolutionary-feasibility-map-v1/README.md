# Temporal-QD evolutionary feasibility map v1

This is an offline, reproducible analysis of retained candidate/window outcomes. It maps the observable economic habitat before any change to evolution, support, quality, costs, the archive, or the grammar.

## Included inputs

- `v37`: `C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-4000x1024x5-20260818-v37` (20480 candidate-window rows)
- `v38`: `C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38` (2176 candidate-window rows)

## Reference-only inputs

- `topology`: excluded from economics — retained topology case-study economics are synthetic/no-market conformance values

## Regenerate

```powershell
uv run python -m autoresearch.temporal_qd_evolutionary_feasibility_map   --cohort v37=<V37_ROOT>   --cohort v38=<V38_ROOT>   --reference topology=<TOPOLOGY_ROOT>   --output-dir <IGNORED_OUTPUT_DIR>
```

The normalizer reads reduced `evaluated-members.jsonl` rows and reconciles them to paired `candidate-panel-bundles.jsonl` metrics where available. Large normalized row-level tables remain in the ignored output directory; compact tables, manifests, and this memo are suitable for code review.

No market evaluation, worker, gateway, Vast instance, generation, archive mutation, or production artifact rewrite is performed by this command.
