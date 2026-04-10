# Opening Scaffold Continuation v2 Narrow

Purpose:

- Correct the remaining opening-step contract failure without dragging the adapter back toward broad scaffold plans.
- Teach exactly one clean first action:
  - `prepare_profile`
  - `mode=scaffold_from_seed`
  - required scaffold fields only
  - no chained `validate_profile` / `register_profile` / `evaluate_candidate`

Recommended starting adapter:

- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`

Narrow corrective files:

- train jsonl:
  - `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow.jsonl`
- val jsonl:
  - `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow.jsonl`
- train chat export:
  - `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow_compact_v2.jsonl`
- val chat export:
  - `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow_compact_v2.jsonl`

How this differs from v1:

- exact action count forced to `1`
- first action stripped to:
  - `tool`
  - `mode`
  - `indicator_ids`
  - `instruments`
  - `candidate_name`
  - `destination_path`
- reasoning rewritten to one short opening-step statement

Current slice size:

- train: `19`
- val: `2`

Interpretation:

- This is a precision corrective slice, not a standalone training corpus.
- Because val is tiny, use it only as a basic guardrail and rely on the held-out opening benchmark plus the fixed benchmark for the real decision.

Suggested bounded command:

```powershell
$env:CUDA_VISIBLE_DEVICES='1'
.\.venv-gpucheck-cu124\Scripts\python.exe training\train_lora.py `
  --model-id google/gemma-4-E4B-it `
  --train-file data\training_pipeline\targeted_slices\opening_scaffold_train_v2_narrow_compact_v2.jsonl `
  --val-file data\training_pipeline\targeted_slices\opening_scaffold_val_v2_narrow_compact_v2.jsonl `
  --output-dir data\training_runs\gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1 `
  --adapter-init-dir data\training_runs\gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1\adapter `
  --max-seq-length 896 `
  --per-device-train-batch-size 1 `
  --per-device-eval-batch-size 1 `
  --gradient-accumulation-steps 2 `
  --max-steps 16 `
  --learning-rate 8e-5 `
  --logging-steps 2 `
  --save-steps 8 `
  --eval-steps 8 `
  --gradient-checkpointing `
  --adapter-mode qlora `
  --quantization 4bit `
  --target-module-preset gemma4_language_regex
```

Why this shape:

- start again from the stronger pre-opening adapter, not the regressed v1 opening adapter
- fewer steps than v1 because the slice is narrower and smaller
- slightly lower learning rate because this is a surgical correction

Required follow-up evals:

1. `offline-opening-scaffold-valtest-v1`
2. `offline-forced-val16-v4`

Pass criteria:

- opening benchmark parse rate rises above `4 / 12`
- opening benchmark validator rate moves above `0 / 12`
- fixed benchmark stays near the current stronger baseline:
  - `json_parse_ok` near `16 / 16`
  - `validator_ok` near `13 / 16`
