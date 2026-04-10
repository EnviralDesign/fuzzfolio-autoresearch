# Opening Scaffold Continuation v1

Purpose:

- Correct the current adapter's opening-step failure mode on first-turn scaffold prompts.
- Target the specific weakness measured in `offline-opening-scaffold-valtest-v1`.
- Do not use the held-out benchmark rows themselves for training.

Recommended starting adapter:

- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`

Training files:

- train: `data/training_pipeline/targeted_slices/opening_scaffold_train_v1_compact_v2.jsonl`
- val: `data/training_pipeline/targeted_slices/opening_scaffold_val_v1_compact_v2.jsonl`

Why these files as-is:

- They are already chat-format SFT exports in the same shape consumed by `training/train_lora.py`.
- They are non-holdout rows.
- The current harness can train directly on them without extra packaging or oversampling.

Current benchmark weakness this run targets:

- benchmark: `data/training_pipeline/benchmarks/offline_opening_scaffold_valtest_v1.jsonl`
- current adapter result:
- `json_parse_ok: 2 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 2 / 12`

Observed failure pattern:

- fenced JSON instead of one raw object
- trailing junk or duplicate-response suffixes
- generic field drift such as `profile_name` or `seed_indicators`
- omitted `prepare_profile.mode`
- extra follow-on actions instead of stopping after the scaffold action

Suggested first continuation command:

```powershell
$env:CUDA_VISIBLE_DEVICES='1'
.\.venv-gpucheck-cu124\Scripts\python.exe training\train_lora.py `
  --model-id google/gemma-4-E4B-it `
  --train-file data\training_pipeline\targeted_slices\opening_scaffold_train_v1_compact_v2.jsonl `
  --val-file data\training_pipeline\targeted_slices\opening_scaffold_val_v1_compact_v2.jsonl `
  --output-dir data\training_runs\gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1 `
  --adapter-init-dir data\training_runs\gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1\adapter `
  --max-seq-length 896 `
  --per-device-train-batch-size 1 `
  --per-device-eval-batch-size 1 `
  --gradient-accumulation-steps 2 `
  --max-steps 24 `
  --learning-rate 1e-4 `
  --logging-steps 2 `
  --save-steps 12 `
  --eval-steps 12 `
  --gradient-checkpointing `
  --adapter-mode qlora `
  --quantization 4bit `
  --target-module-preset gemma4_language_regex
```

Why this shape:

- `896` context is already proven on the Quadro path.
- The scaffold slice is small, so this should be treated as a targeted corrective continuation, not a broad retrain.
- `24` steps is enough to probe whether opening-step discipline moves without committing to a long heavy run.
- `1e-4` is intentionally conservative for continuation on a narrow slice.

Quick sizing notes:

- train file: `32` rows
- val file: `6` rows
- train message chars: median `2761.5`, p95 `3142`, max `3519`
- val message chars: median `2880`, p95 `2977`, max `3257`

Required follow-up evals after the run:

1. Opening benchmark:
   `offline-opening-scaffold-valtest-v1`
2. Existing fixed benchmark:
   `offline-forced-val16-v4`

Pass criteria for this continuation:

- opening benchmark parse rate rises materially above `2 / 12`
- opening benchmark first-tool match rises materially above `2 / 12`
- opening benchmark validator pass moves off zero
- fixed benchmark does not regress badly on parse or validator rate
