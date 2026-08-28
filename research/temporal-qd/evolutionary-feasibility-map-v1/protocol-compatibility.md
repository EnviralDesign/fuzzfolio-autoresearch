# Protocol compatibility and exclusions

## Included economic strata

- **v37** — `temporal_qd_v5_fast_ephemeral_current_panel_v37`; role `five_generation_current_panel_population`; 5120 candidate evaluations and 20480 candidate-window observations.
- **v38** — `temporal_qd_v5_fast_ephemeral_operator_matrix_v38`; role `frozen_parent_operator_matrix`; 544 candidate evaluations and 2176 candidate-window observations.

## Excluded references

- **topology** — retained topology case-study economics are synthetic/no-market conformance values

## Rules enforced

- V37 five-generation current-panel population and V38 frozen-parent matrix are reported as separate protocol groups.
- Candidate re-evaluations retain their distinct source evaluation identity; exact duplicate source rows are the only rows deduplicated.
- No development and untouched evidence are pooled into a shared decision metric.
- Missing panel fields are marked unavailable rather than imputed.
