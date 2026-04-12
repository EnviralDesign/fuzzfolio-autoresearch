# Exploration Judgment v1

This lane is for improving exploration quality after the controller/pathless contract work is already stable.

## 1. Build or refresh the review set

```powershell
.\.venv\Scripts\python.exe -m trainingdatapipeline.build_exploration_review_set `
  --out data\training_pipeline\review_sets\exploration_review_v1_candidates.jsonl `
  --sheet-out data\training_pipeline\review_sets\exploration_review_v1_sheet.md `
  --manifest-out data\training_pipeline\review_sets\exploration_review_v1_manifest.json `
  --labels-template-out data\training_pipeline\manual_labels\exploration_judgment_v1_template.jsonl
```

Then review rows by `review_id` and write a working labels file with decisions:

- `keep_gold`
- `rewrite_action`
- `drop_infra`
- `drop_mechanical`
- `drop_ambiguous`

For `rewrite_action`:

- provide one short `corrected_reasoning`
- provide one pathless `corrected_action`
- do not use `profile_path`, `destination_path`, or other legacy path-era fields

## 2. Build the curated dataset

```powershell
.\.venv\Scripts\python.exe -m trainingdatapipeline.build_exploration_judgment_dataset `
  --review-input data\training_pipeline\review_sets\exploration_review_v1_candidates.jsonl `
  --labels-input data\training_pipeline\manual_labels\exploration_judgment_v1.jsonl `
  --train-out data\training_pipeline\targeted_slices\exploration_judgment_train_v1.jsonl `
  --val-out data\training_pipeline\targeted_slices\exploration_judgment_val_v1.jsonl `
  --benchmark-out data\training_pipeline\benchmarks\exploration_judgment_holdout_v1.jsonl `
  --train-chat-out data\training_pipeline\targeted_slices\exploration_judgment_train_v1_compact_v2.jsonl `
  --val-chat-out data\training_pipeline\targeted_slices\exploration_judgment_val_v1_compact_v2.jsonl `
  --benchmark-chat-out data\training_pipeline\benchmarks\exploration_judgment_holdout_v1_compact_v2.jsonl `
  --manifest-out data\training_pipeline\targeted_slices\exploration_judgment_v1_manifest.json
```

Default builder behavior:

- manually curated rows are split into train / val / held-out benchmark
- train manual rows are duplicated deterministically up to `112` rows
- train anchors add:
  - `32` opening rows
  - `20` each of `validate_profile`, `register_profile`, `mutate_profile`, `evaluate_candidate`
- val anchors add:
  - `8` opening rows
  - `4` each of `validate_profile`, `register_profile`, `mutate_profile`, `evaluate_candidate`

## 3. Run one bounded continuation

Starting adapter:

- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`

Suggested command:

```powershell
$env:CUDA_VISIBLE_DEVICES='1'
.\.venv-gpucheck-cu124\Scripts\python.exe training\train_lora.py `
  --model-id google/gemma-4-E4B-it `
  --train-file data\training_pipeline\targeted_slices\exploration_judgment_train_v1_compact_v2.jsonl `
  --val-file data\training_pipeline\targeted_slices\exploration_judgment_val_v1_compact_v2.jsonl `
  --adapter-init-dir data\training_runs\gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1\adapter `
  --output-dir data\training_runs\gemma_e4b_explorationjudgment_v1_from_openv2_gpu1 `
  --max-seq-length 896 `
  --per-device-train-batch-size 1 `
  --per-device-eval-batch-size 1 `
  --gradient-accumulation-steps 2 `
  --max-steps 32 `
  --learning-rate 6e-5 `
  --logging-steps 2 `
  --save-steps 8 `
  --eval-steps 8 `
  --gradient-checkpointing `
  --adapter-mode qlora `
  --quantization 4bit `
  --target-module-preset gemma4_language_regex
```

## 4. Offline gates before export

Run these before promoting the adapter:

1. held-out opening benchmark with runtime intervention
2. active fixed follow-up benchmark with follow-up canonicalization
3. held-out exploration benchmark:
   - `data\training_pipeline\benchmarks\exploration_judgment_holdout_v1.jsonl`

Promotion rule:

- opening must not materially regress
- fixed follow-up must stay at or above the current practical floor
- exploration holdout must improve materially over the current tuned baseline

## 5. Export to GGUF and re-evaluate in LM Studio

```powershell
.\scripts\export_gemma_adapter_to_gguf.ps1 `
  -AdapterDir data\training_runs\gemma_e4b_explorationjudgment_v1_from_openv2_gpu1\adapter `
  -ExportRoot data\gguf_exports\gemma4_e4b_explorationjudgment_v1 `
  -GpuId 1 `
  -Device cuda `
  -ImportToLmStudio `
  -LmsUserRepo local/gemma4-e4b-explorationjudgment-v1
```

Then add an LM Studio profile in `autoresearch.config.json` and run:

1. one `10`-step smoke
2. one clean sequential `50`-step vanilla vs tuned comparison

Use the existing LM Studio vanilla baseline:

- `lmstudio-gemma-4-e4b-it`

## 6. Observed v1 outcome

The first full v1 pass completed, but it did not clear promotion gates.

- Curated dataset manifest:
  - `data\training_pipeline\targeted_slices\exploration_judgment_v1_manifest.json`
  - `73` validated manual rows kept
  - `204` train / `30` val / `24` held-out benchmark rows after anchors and duplication
- Continuation run:
  - `data\training_runs\gemma_e4b_explorationjudgment_v1_from_openv2_gpu1`
- Offline results:
  - opening benchmark held at `12 / 12` parseable, `12 / 12` validator-clean, `12 / 12` first-tool match
  - fixed follow-up slipped to `15 / 16` parseable, `14 / 16` validator-clean, `14 / 16` deterministic tool match
  - exploration holdout stayed flat versus the old adapter at `8 / 24` first-tool match and regressed from `21 / 24` to `19 / 24` validator-clean
- Promotion decision:
  - do not export or promote this adapter
  - do not rerun LM Studio tuned-vs-vanilla with this checkpoint

Practical read from the failed holdout:

- the curation slice appears to have over-taught mutate-style rewrites
- several new misses became invalid `mutate_profile` calls without `mutations`
- repeated-evaluate rows were dropped from the final manual set because the current controller correctly blocks them
