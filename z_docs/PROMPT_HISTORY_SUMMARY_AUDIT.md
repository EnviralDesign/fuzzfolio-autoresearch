# Prompt History Summary Audit

This note records the prompt-visible summary policy for controller history messages.

## Common Rules

- Keep prompt-visible fields that directly help the next action.
- Keep compact handles: `candidate_name`, `profile_ref`, `attempt_id`, `artifact_dir`, `inspect_ref`.
- Keep controller guidance: `controller_hint`, readiness flags, next-step-relevant score/eval fields.
- Remove prompt-visible debug noise: `returncode`, `parsed_json_keys`, full fingerprints, full instance lineage, duplicate names, count fields that just restate visible arrays.
- Internal/raw logs may still retain richer fields for debugging and controller bookkeeping.

## Per Tool

| Tool | Keep in prompt-visible summary | Remove from prompt-visible summary | Internal only |
| --- | --- | --- | --- |
| `prepare_profile` | `tool`, `ok`, `candidate_name`, `indicator_ids`, `instruments`, `timeframe_summary`, `controller_hint` | duplicate names, counts | full fingerprint, full family id, instance ids |
| `mutate_profile` | `tool`, `ok`, `candidate_name`, `mutation_summary`, compact strategy fields, `controller_hint` when present | duplicate names, counts | full fingerprint, full family id, instance ids, raw mutation payload |
| `validate_profile` | `tool`, `ok`, `candidate_name`, compact strategy fields, `ready_for_registration`, `material_changes`, `controller_hint`, timeframe mismatch if present | duplicate names, return code, parsed-json previews | full fingerprint, full family id, instance ids |
| `register_profile` | `tool`, `ok`, `candidate_name`, `profile_ref`, `ready_to_evaluate`, `controller_hint` | duplicate names, count/debug fields | full fingerprint, full family id, instance ids |
| `evaluate_candidate` | `tool`, `ok`, `profile_ref`, `attempt_id`, `score`, `effective_window_months`, `trades_per_month`, `resolved_trades`, `artifact_dir`, timeframe mismatch/details when relevant | raw auto-log payload, low-level CLI fields | full retention/branch lifecycle detail |
| `run_parameter_sweep` | `tool`, `ok`, `inspect_ref`, `artifact_dir`, `best_score`, `quality_score_preset`, `controller_hint` | ranked raw sweep payloads | full ranked results, raw parsed-json payload |
| `inspect_artifact` | `tool`, `ok`, `artifact_dir`, `artifact_kind`, compact sweep/compare summary, compact attempt hint, `controller_hint` | full file listings and raw comparison payloads in normal summary view | raw file list, full compare payload, full resolution detail |
| `compare_artifacts` | `tool`, `ok`, top ranked preview rows, dominant deltas, suggested next move | full ranked comparison payload | full best payloads per artifact |

## Naming Policy

- Operational loop identity is `candidate_name`.
- Draft profile files should keep `profile.name == candidate_name` to avoid long fallback names.
- Richer final-public naming is a separate concern and is intentionally out of scope for this cleanup.
