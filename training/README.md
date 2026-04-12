# Gemma Explorer Training

This directory holds the first adapter-first training scaffold for the explorer tune.

## Current Scope

- `train_lora.py`
  - Chat-format SFT on the exported dataset.
  - LoRA by default.
  - Optional QLoRA with `--qlora` if `bitsandbytes` works on the target machine.
- `eval_offline.py`
  - Dataset sanity checks.
  - Prediction validity checks against the offline relabel validator.
- `requirements-gemma.txt`
  - Minimal Python package list for the first pass.

This produces a LoRA adapter first, not a merged standalone checkpoint.

## Exploration-Judgment Curation v1

This is the next `yay-but-be-disciplined` lane after the pathless controller work.
The goal is to teach better search judgment, not more contract mechanics.

New helper paths:

- `trainingdatapipeline/build_exploration_review_set.py`
  - mines real runs into reviewer-oriented decision rows
  - emits stable `review_id` values
  - outputs a JSONL review set, Markdown sheet, and blank labels template
- `trainingdatapipeline/build_exploration_judgment_dataset.py`
  - reads the review-set JSONL plus manual labels
  - validates kept/rewritten targets against the offline controller validator
  - mixes curated manual rows with pathless opening and follow-up anchors
  - exports train / val / held-out benchmark JSONL plus compact-v2 chat exports

Current first-pass generated artifacts:

- review rows:
  - `data/training_pipeline/review_sets/exploration_review_v1_candidates.jsonl`
- review sheet:
  - `data/training_pipeline/review_sets/exploration_review_v1_sheet.md`
- blank labels template:
  - `data/training_pipeline/manual_labels/exploration_judgment_v1_template.jsonl`

Expected manual label decisions:

- `keep_gold`
- `rewrite_action`
- `drop_infra`
- `drop_mechanical`
- `drop_ambiguous`

For `rewrite_action` rows:

- provide one short `corrected_reasoning`
- provide one pathless `corrected_action`
- do not emit `profile_path`, `destination_path`, or other legacy path-era fields

The intended use is:

1. generate or refresh the review set
2. annotate rows by `review_id`
3. build the curated dataset
4. run one bounded exploration-judgment continuation from the current best adapter
5. export the new adapter to GGUF and re-evaluate in LM Studio

Observed v1 result:

- curated dataset manifest:
  - `data/training_pipeline/targeted_slices/exploration_judgment_v1_manifest.json`
  - `73` validated manual rows kept
  - `204` train / `30` val / `24` held-out benchmark rows
- continuation run:
  - `data/training_runs/gemma_e4b_explorationjudgment_v1_from_openv2_gpu1`
- offline gates:
  - opening held at `12 / 12` parseable, `12 / 12` validator-clean, `12 / 12` first-tool match
  - fixed follow-up regressed to `15 / 16` parseable, `14 / 16` validator-clean, `14 / 16` deterministic tool match
  - exploration holdout did not improve over the old adapter: `8 / 24` first-tool match on both, with validator-clean dropping from `21 / 24` to `19 / 24`
- decision:
  - do not export or promote `gemma_e4b_explorationjudgment_v1_from_openv2_gpu1`
  - mine a second judgment review slice before the next tune, with extra care around over-teaching mutate rewrites

## GGUF / LM Studio Export

This is a real `yay`: the repo now has a first-class export lane for turning the
current Gemma adapter into a merged GGUF artifact for faster local serving.

New helper paths:

- `training/merge_adapter.py`
  - loads the base Hugging Face model plus a PEFT LoRA adapter
  - merges with `merge_and_unload()`
  - saves merged Hugging Face weights plus tokenizer
- `scripts/export_gemma_adapter_to_gguf.ps1`
  - Windows-first orchestration wrapper
  - merges adapter -> converts merged HF weights to GGUF via `llama.cpp`
  - optionally imports the resulting `.gguf` into LM Studio with `lms import`

Current recommended adapter input:

- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`

Typical dry run:

```powershell
.\scripts\export_gemma_adapter_to_gguf.ps1 `
  -AdapterDir data\training_runs\gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1\adapter `
  -ExportRoot data\gguf_exports\gemma4_e4b_openv2 `
  -ReportOnly
```

Typical full export + LM Studio import:

```powershell
.\scripts\export_gemma_adapter_to_gguf.ps1 `
  -AdapterDir data\training_runs\gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1\adapter `
  -ExportRoot data\gguf_exports\gemma4_e4b_openv2 `
  -GpuId 1 `
  -Device cuda `
  -ImportToLmStudio `
  -LmsUserRepo local/gemma4-e4b-openv2-tuned
```

Practical notes:

- LM Studio import is straightforward once the `.gguf` exists.
- The adapter itself is still the durable training artifact; GGUF is the deployment artifact.
- `scripts/export_gemma_adapter_to_gguf.ps1` will auto-clone `llama.cpp` under
  `%LOCALAPPDATA%\codex-cache\llama.cpp` if no `-LlamaCppDir` is provided.
- The merge step can be heavier than the import step; use `-ReportOnly` first if
  you want to validate paths and the plan before loading model weights.

## Current Baseline Read

- This is a real `yay`.
- The live model-facing controller contract is now pathless across providers:
- local drafts use `candidate_name`
- registered profiles use `profile_ref`
- the controller/tool layer resolves filesystem paths internally
- true opening-step prompt tightening is now global for true step 1, not local-Gemma-only
- current active adapter baseline remains:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`

Current measured state after the global pathless migration:

- held-out opening scaffold benchmark with runtime intervention:
- `12 / 12` parseable
- `12 / 12` validator-clean
- `12 / 12` first-tool match
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_handle_compactv2_512tok.json`
- non-holdout opening grounding benchmark:
- `11 / 12` instrument grounding
- `11 / 12` candidate-handle grounding
- `0 / 12` forbidden `ALL`
- `11 / 12` opening grounding success
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_grounding_nonholdout_v1_summary_runtimeintervention_handle_compactv2_512tok.json`
- fixed follow-up benchmark under the pathless contract:
- `16 / 16` parseable
- `11 / 16` validator-clean
- `16 / 16` first-tool match
- `16 / 16` deterministic tool match
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimebaseline_handle_compactv2_512tok.json`

Practical interpretation:

- opening-step contract and grounding are no longer the main blocker
- the controller/runtime abstraction win mattered more than another opening-step tune
- the remaining adapter weakness is later-step argument binding under the new pathless contract, especially:
- `evaluate_candidate` omitting `instruments`
- occasional follow-up rows missing `candidate_name` or `profile_ref`

Live validation now proving the contract:

- one-step global-pathless smoke:
- `data/training_runs/pathless_global_live_smoke_step1.json`
- run:
- `runs/20260410T164756430144Z-agentic-85a2e5`
- first action executed successfully with:
- `prepare_profile`
- `candidate_name`
- no model-supplied path
- three-step global-pathless smoke:
- `data/training_runs/pathless_global_live_smoke_step3.json`
- run:
- `runs/20260410T164917421276Z-agentic-d53b59`
- progressed:
- `prepare_profile -> validate_profile -> register_profile`
- direct typed-tool smoke for draft evaluation via `candidate_name`:
- `data/training_runs/pathless_candidate_eval_smoke.json`
- run:
- `runs/20260410T165221143782Z-agentic-2315cc`
- proved `evaluate_candidate(candidate_name=...)` works on a local unregistered draft

Current next lane:

- do not reopen broad training yet
- keep the pathless live contract
- focus on a small pathless follow-up adaptation or prompt-tightening lane for:
- `validate_profile`
- `register_profile`
- `mutate_profile`
- `evaluate_candidate`

## Recommended Dataset Inputs

Current compact chat exports:

- `data/training_pipeline/final/v0_chat/train_compact.jsonl`
- `data/training_pipeline/final/v0_chat/val_compact.jsonl`
- `data/training_pipeline/final/v0_chat/test_compact.jsonl`

## Windows Notes

- Stay on Windows for the first smoke, per project decision.
- The biggest uncertainty is not CUDA itself but the exact PyTorch and `bitsandbytes` wheel combination.
- If `bitsandbytes` fails locally, use plain LoRA first and keep `--qlora` off.
- If GPU memory is tight, lower `--max-seq-length`, keep batch size at `1`, and raise gradient accumulation.

Current validation notes from April 8, 2026:

- Default transient `uv run --with torch ...` resolved to CPU-only `torch`, so do not treat that path as authoritative for GPU training.
- An isolated Windows venv with `torch==2.6.0+cu124` from the official PyTorch CUDA wheel index did expose both GPUs and allowed Gemma tokenizer/config access.
- The `RTX 3070 8GB` is not a viable E4B training target. It OOMs during quantized model load.
- The `Quadro RTX 5000 16GB` is the active local training target.
- The current working path is:
- `CUDA_VISIBLE_DEVICES=1`
- `--adapter-mode qlora`
- `--quantization 4bit`
- `--target-module-preset gemma4_language_regex`
- The language-model-only regex workaround avoided the multimodal `Gemma4ClippableLinear` wrapper path and completed the first end-to-end smoke run.

## First Smoke Suggestion

```powershell
.\.venv-gpucheck-cu124\Scripts\python.exe training/train_lora.py `
  --model-id google/gemma-4-E4B-it `
  --train-file data/training_pipeline/final/v0_chat_smoke/train_compact_smoke.jsonl `
  --val-file data/training_pipeline/final/v0_chat_smoke/val_compact_smoke.jsonl `
  --output-dir data/training_runs/gemma_e4b_smoke_gpu1_regex `
  --max-seq-length 256 `
  --per-device-train-batch-size 1 `
  --gradient-accumulation-steps 2 `
  --num-train-epochs 0.02 `
  --gradient-checkpointing `
  --adapter-mode qlora `
  --quantization 4bit `
  --target-module-preset gemma4_language_regex
```

Before launching, pin the training run to the Quadro:

```powershell
$env:CUDA_VISIBLE_DEVICES='1'
```

The smoke artifact directory is:

- `data/training_runs/gemma_e4b_smoke_gpu1_regex`

## Workaround Ladder

Run these in order:

1. `--report-only`
   - No real training launch.
   - Writes `run_config.json` and `env_report.json`.
   - Use this first to confirm the trainer is targeting Gemma 4 language-model modules only.
2. Plain LoRA fallback
   - `--adapter-mode lora --quantization none`
   - This avoids the 4-bit wrapper path entirely, but may be tighter on VRAM.
3. QLoRA retry with language-only preset
   - `--adapter-mode qlora --quantization 4bit --target-module-preset gemma4_language_regex`
   - This is the current working path because it avoids matching the multimodal tower wrappers.
4. Broad suffix fallback only if needed
   - `--target-module-preset gemma4_suffix`
   - Use only if the language-only regex path proves too narrow.

## Next Pilot Suggestion

Use the same Quadro-only QLoRA path and scale data modestly before changing sequence length:

```powershell
$env:CUDA_VISIBLE_DEVICES='1'
.\.venv-gpucheck-cu124\Scripts\python.exe training/train_lora.py `
  --model-id google/gemma-4-E4B-it `
  --train-file data/training_pipeline/final/v0_chat_pilot/train_compact_pilot128.jsonl `
  --val-file data/training_pipeline/final/v0_chat_pilot/val_compact_pilot32.jsonl `
  --output-dir data/training_runs/gemma_e4b_pilot_gpu1_regex `
  --max-seq-length 256 `
  --per-device-train-batch-size 1 `
  --per-device-eval-batch-size 1 `
  --gradient-accumulation-steps 2 `
  --num-train-epochs 1 `
  --max-steps 20 `
  --learning-rate 2e-4 `
  --logging-steps 2 `
  --save-steps 10 `
  --eval-steps 10 `
  --gradient-checkpointing `
  --adapter-mode qlora `
  --quantization 4bit `
  --target-module-preset gemma4_language_regex
```

## First Bounded Pilot Result

The first bounded Quadro pilot completed successfully with:

- `CUDA_VISIBLE_DEVICES=1`
- `128` train rows / `32` val rows
- `--max-seq-length 256`
- `--max-steps 20`
- `--adapter-mode qlora`
- `--quantization 4bit`
- `--target-module-preset gemma4_language_regex`

Artifact directory:

- `data/training_runs/gemma_e4b_pilot_gpu1_regex`

Observed metrics:

- `train_runtime: 686.9s`
- `train_steps_per_second: 0.029`
- eval at step `10`: `eval_loss 1.473`, `eval_mean_token_accuracy 0.7346`
- eval at step `20`: `eval_loss 0.4639`, `eval_mean_token_accuracy 0.9172`

Practical note:

- Two validation passes over `32` rows consumed roughly `260s` of wall-clock.
- For fast iteration, reduce the validation slice or evaluate only once near the end.
- The next useful probe is sequence length at `384` or `512`, not a different GPU path.

## Mixed Relabel Result

First focused relabel expansion:

- input batch: `64` curated C-grade near-miss controller-native rows
- teacher: `gemini-3-flash-preview`
- validated selections: `64 / 64`
- filtered kept set after dropping browse regressions: `59`
- kept by split: `44` train / `7` val / `8` test

The first mixed chat dataset is under:

- `data/training_pipeline/final/v0_chat_mix1`

Useful counts:

- full mix: `2004` train / `209` val
- mixed pilot slice: `160` train / `16` val
- pilot relabel additions: `44` train / `7` val

## 384 Context Probe Result

The first mixed `384`-token probe completed successfully on the Quadro after one transient model-load crash:

- `CUDA_VISIBLE_DEVICES=1`
- `160` train rows / `16` val rows
- `44` relabeled train additions and `7` relabeled val additions included
- `--max-seq-length 384`
- `--max-steps 20`
- `--adapter-mode qlora`
- `--quantization 4bit`
- `--target-module-preset gemma4_language_regex`

Artifact directory:

- `data/training_runs/gemma_e4b_mix1_probe384_gpu1_retry2`

Observed metrics:

- `train_runtime: 1024s`
- `train_steps_per_second: 0.020`
- end-only eval: `eval_loss 1.788`
- end-only eval: `eval_mean_token_accuracy 0.7353`
- end-only eval runtime: `77.06s`

Interpretation:

- `384` context is viable on the `Quadro RTX 5000 16GB`.
- It is slower than `256`, but still practical for controlled pilots.
- The next probe should be either `512` context or a longer-step run at `384`.

## Offline Sanity Checks

```powershell
uv run python training/eval_offline.py dataset-sanity --input data/training_pipeline/final/v0_chat/train_compact.jsonl
```

## Prompt Contract Findings

- The live controller prompt in `autoresearch/controller_protocol.py` is much larger than the early SFT/offline harness prompt.
- The harness now uses `SFT_SYSTEM_PROTOCOL`, a compressed controller-faithful prompt:
- exact JSON top-level shape
- exact allowed tool list
- explicit ban on invented tool names
- explicit ban on nested `parameters` wrappers
- deterministic typed-tool workflow reminders
- Do not train with the full live controller prompt on the current Quadro path unless sequence-length feasibility changes.
- On Gemma tokenization, the full live controller prompt alone is about `1623` tokens.

## Compact-v2 State Findings

- `prompt_state_compact_v2` is the current tighter SFT/offline-eval prompt-state path.
- Current contract-specialization token profile on a `256`-row sample:
- `system`: `362` tokens
- `user` median: `371` tokens
- `full sequence` median: `798` tokens
- `full sequence p95`: `857` tokens
- This is much better than the earlier compact state, but still too large to justify brute-force very long-context training on the Quadro.
- replay-handle enrichment in the v4 pipeline now carries:
- `profile_path`
- `profile_ref`
- `created_profile_ref`
- `attempt_id`
- `artifact_dir`
- `candidate_name`
- `instruments`
- `requested_horizon_months`
- `evaluation_mode`
- `compact-v2` also now exposes a small `handles` block sourced from the latest relevant raw replay steps and recent attempts.

## Critical Benchmark Insight

- Old compact prompt state plus old harness prompt:
- best offline deterministic benchmark result was roughly `4 / 16` deterministic tool match.
- New compact-v2 prompt state plus compressed SFT system prompt:
- existing adapter reached `15 / 16` deterministic tool match on the same fixed benchmark.
- But `validator_ok` stayed at `0 / 16`.
- Interpretation:
- the model can now infer the correct next tool from the compact-v2 state
- the compact-v2 state still omits some required handles for valid argument filling
- the next data fix is replay-state enrichment, not more blind training steps
- After replay-handle enrichment and a fresh v4 pipeline rebuild, the refreshed benchmark is:
- `data/training_pipeline/benchmarks/offline_forced_val16_v4.jsonl`
- Existing adapter result on that benchmark:
- `json_parse_ok: 13 / 16`
- `validator_ok: 11 / 16`
- `first_tool_match: 12 / 16`
- `deterministic_tool_match: 12 / 16`
- This is the strongest offline contract result so far because it combines compact-v2 state with the missing handle fields.

Likely missing handle fields:

- `destination_path`
- `profile_path`
- `profile_ref`
- `attempt_id`
- `artifact_dir`
- `candidate_name`
- `instruments`
- `requested_horizon_months`
- `evaluation_mode`

## Compact-v2 Continuation Result

- A bounded compact-v2 continuation lane is now practical on the Quadro:
- pilot run: `data/training_runs/gemma_e4b_contractspec_v4_compactv2_pilot256_len896_from384_gpu1`
- full contract slice run: `data/training_runs/gemma_e4b_contractspec_v4_compactv2_full1924_len896_from384_gpu1`
- `896` context with the refreshed compact-v2 contract slice is operationally fine.
- `40` steps on the full `1924`-row contract slice took about `5` minutes of optimizer runtime.
- However, neither continuation run improved the fixed v4 benchmark beyond:
- `json_parse_ok: 13 / 16`
- `validator_ok: 11 / 16`
- `first_tool_match: 12 / 16`
- `deterministic_tool_match: 12 / 16`

Current interpretation:

- compact-v2 state plus replay-handle enrichment delivered the real improvement
- additional generic contract-specialization SFT on top of that did not move the remaining failure pocket
- the next targeted data slice should focus on:
- argument binding for `evaluate_candidate` and `inspect_artifact`
- formatting cleanliness for trailing-fence / duplicate-JSON outputs

## Continuation Bug Fix

- The continuation path in `training/train_lora.py` previously reopened `--adapter-init-dir` via `PeftModel.from_pretrained(...)` without `is_trainable=True`.
- PEFT defaults that path to frozen adapters, so continuation runs could complete while not actually updating adapter weights.
- The trainer now reopens existing adapters with `is_trainable=True`.
- Training outputs now include `parameter_report.json` so continuation runs can prove nonzero trainable parameter counts.
- First verified continuation report:
- `data/training_runs/gemma_e4b_contractfix_v1_len896_from384_gpu1_trainable/parameter_report.json`
- `trainable_parameters: 36,700,160`

## Targeted Continuation Results

- First real trainable corrective continuation:
- run: `data/training_runs/gemma_e4b_contractfix_v1_len896_from384_gpu1_trainable`
- train slice: `256` non-benchmark compact-v2 rows emphasizing `evaluate_candidate` and `inspect_artifact`, with small `register_profile` / `validate_profile` anchors
- val slice: `32` rows with the fixed benchmark run excluded
- fixed benchmark result:
- `json_parse_ok: 15 / 16`
- `validator_ok: 13 / 16`
- `first_tool_match: 14 / 16`
- `deterministic_tool_match: 14 / 16`
- summary:
- `data/training_runs/gemma_e4b_contractfix_v1_len896_from384_gpu1_trainable/offline_forced_val16_v4_summary_compactv2_512tok.json`

- Second narrower continuation on `evaluate_candidate` only:
- run: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1`
- fixed benchmark result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `first_tool_match: 14 / 16`
- `deterministic_tool_match: 14 / 16`
- summary:
- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`

Current interpretation:

- The continuation bug fix was high leverage.
- Targeted continuation is now moving the benchmark again.
- The broader corrective continuation improved both tool match and validator pass rate.
- The narrower evaluate-only continuation eliminated the last parse failure on the fixed benchmark, but did not improve `evaluate_candidate` instrument binding beyond the broader corrective continuation.
- Remaining offline failures are now entirely `evaluate_candidate` rows missing `instruments`.

## Known Gaps

- No merged-checkpoint export yet.
- No GGUF / LM Studio export path yet.
- No online paired benchmark runner in this directory yet.
- Branch-lifecycle offline admissibility is still partial because replay records do not yet include a full branch overlay snapshot.
- Any real GPU launch should stay pinned to `CUDA_VISIBLE_DEVICES=1` so it stays on the `Quadro RTX 5000 16GB`.

## Python-Native Local Controller Validation

- A Python-native local profile is now configured in `autoresearch.config.json`:
- `gemma4-e4b-local-adapter`
- type: `transformers_local`
- base: `google/gemma-4-E4B-it`
- adapter: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- quantization: `4bit`

- Provider-only smoke is working:
- `python -m autoresearch test-providers --profile gemma4-e4b-local-adapter --json`
- both built-in JSON probes pass locally.

- First controller-path smoke succeeded:
- run: `runs/20260409T152402605204Z-agentic-cd93b8`
- one-step run returned valid first-pass JSON and executed:
- `prepare_profile`
- `validate_profile`
- `register_profile`

- Normal-phase controller smoke surfaced the next live failure pocket:
- run: `runs/20260409T160703542106Z-agentic-c4aaa8`
- the model returned a parseable early-phase scaffold action but omitted required `prepare_profile.mode`
- controller entered response repair as expected
- the expensive local repair path is still operationally problematic when first-pass mechanical errors survive

- Runtime shaping changes now landed for local inference:
- local providers use `SFT_SYSTEM_PROTOCOL` instead of the larger frontier-model system prompt
- local-provider malformed-output repair is shorter and capped more aggressively
- controller response repair and payload-shape repair use compact local-aware repair prompts
- controller can emit a compact replay-style runtime state packet for local providers using `compact_v2`

- Current instability to resolve before more heavy local-controller runs:
- run: `runs/20260409T162437734647Z-agentic-1f3192`
- the compact-runtime smoke never produced a raw payload and stalled/crashed before first decoded output
- shell log stopped at `provider_trace event=complete_json_start`
- this suggests a native/local-runtime failure before output capture, not a normal controller validation rejection

- User constraint now in force:
- pause further heavy GPU/controller-local runs until explicitly reopened
- the `Quadro RTX 5000 16GB` also drives the display setup and these runs make the machine nearly unusable

- Best next non-GPU work:
- add prompt/generation stats instrumentation to the local provider
- build a targeted opening-step scaffold corrective slice (`prepare_profile` with required `mode`)
- add an offline benchmark slice for first-step scaffold decisions before the next continuation run

## Opening-Step Scaffold Artifacts

- New non-GPU opening-step lane is now built from the v4 corpus.
- Builder:
- `trainingdatapipeline/build_opening_scaffold_slice.py`

- Generated slices:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v1.jsonl`
- `32` rows
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v1.jsonl`
- `6` rows
- compact-v2 chat exports:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v1_compact_v2.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v1_compact_v2.jsonl`

- Generated benchmark:
- `data/training_pipeline/benchmarks/offline_opening_scaffold_valtest_v1.jsonl`
- `12` rows
- registry id:
- `offline-opening-scaffold-valtest-v1`

- Important interpretation:
- the v4 corpus already contains clean first-step `prepare_profile` examples with `mode=scaffold_from_seed`
- the live local-controller miss on `prepare_profile.mode` therefore looks like an underweighted opening-step behavior, not a missing-data problem
- Offline eval result with the current best adapter:
- adapter: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- prediction file:
- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/offline_opening_scaffold_valtest_v1_predictions_compactv2_512tok.jsonl`
- summary:
- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 2 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 2 / 12`
- `avg_generated_tokens: 235.83`
- `avg_duration_seconds: 56.563`
- Failure pattern:
- all `12 / 12` outputs started with fenced JSON
- `10 / 12` rows failed JSON parse entirely
- `7 / 12` appended bad suffix text after the object
- the model often invented generic opening fields like `profile_name` and `seed_indicators`
- the `2` parseable rows still omitted required `prepare_profile.mode`
- interpretation:
- the opening-step weakness reproduces offline and is broader than the live `mode` miss alone
- this is now the clearest next continuation target, but it should train only on the non-holdout scaffold train/val slices, not the benchmark rows
- prepared continuation recipe:
- `training/run_recipes/opening_scaffold_continuation_v1.md`

## Opening-Scaffold Continuation Result

- run:
- `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1`
- starting adapter:
- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- config:
- `896` context
- `24` steps
- `1e-4` learning rate
- `32` train rows / `6` val rows
- `36,700,160` trainable parameters
- training runtime:
- `309.9s`

- opening benchmark after the continuation:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 4 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 4 / 12`

- fixed benchmark after the continuation:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 14 / 16`
- `validator_ok: 10 / 16`
- `deterministic_tool_match: 13 / 16`

- comparison to the prior best adapter:
- opening-step syntax and first-tool match improved somewhat
- the core opening-step contract still did not improve because every parseable `prepare_profile` still omitted required `mode`
- the stronger fixed follow-up benchmark regressed materially

- conclusion:
- this continuation is informative but not a keeper as the default adapter
- the next opening-step corrective lane should be narrower than the current scaffold slice:
- teach one clean `prepare_profile` object only
- enforce `mode=scaffold_from_seed`
- remove generic field drift like `profile_name` / `seed_indicators`
- suppress chained follow-on actions in the same opening response

## Opening-Scaffold Narrow Corrective Lane

- The builder now supports stricter opening-step selection and normalization:
- `trainingdatapipeline/build_opening_scaffold_slice.py`
- new controls:
- exact action-count filtering
- first-action field stripping
- deterministic opening-step reasoning rewrite

- Prepared narrow corrective artifacts:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow.jsonl`
- chat exports:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow_compact_v2.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow_compact_v2.jsonl`

- Narrow slice properties:
- train `19` rows
- val `2` rows
- exactly one action only
- first action reduced to:
- `tool`
- `mode`
- `indicator_ids`
- `instruments`
- `candidate_name`
- `destination_path`
- reasoning rewritten to:
- `Fresh run opening step. Create one seed-guided candidate scaffold now so it can be validated next.`

- Recommended next bounded continuation:
- `training/run_recipes/opening_scaffold_continuation_v2_narrow.md`
- important note:
- this should start from `gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1`, not from the regressed v1 opening adapter

## Opening-Scaffold v2 Narrow Result

- run:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1`
- starting adapter:
- `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- config:
- `896` context
- `16` steps
- `8e-5` learning rate
- `19` train rows / `2` val rows
- training runtime:
- `183.2s`

- opening benchmark after the continuation:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 3 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 3 / 12`

- fixed benchmark after the continuation:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`

- comparison:
- opening-step correction is still not solved; the model still misses `prepare_profile.mode`
- but this narrow continuation avoids the regression from the broader opening run
- it matches the prior best validator rate on the fixed benchmark and improves deterministic tool match there

- conclusion:
- this is the strongest offline adapter so far for follow-up behavior
- it is not yet a true opening-step fix
- if we keep pushing on opening-step correction, the next lane should add explicit negative/contrast supervision rather than just more positive scaffold examples

## Opening Contrast v1 Result

- new builder:
- `trainingdatapipeline/build_opening_contrast_dataset.py`
- non-holdout opening pool:
- `data/training_pipeline/targeted_slices/opening_scaffold_nonholdout_v1.jsonl`
- non-holdout failure predictions:
- `data/training_pipeline/targeted_slices/opening_scaffold_nonholdout_v1_predictions_openv2.jsonl`
- mixed dataset report:
- `data/training_pipeline/targeted_slices/opening_contrast_v1_summary.json`

- dataset composition:
- train `62`
- val `12`
- positives `21`
- corrective rows `21`
- anchors `32`
- no teacher gap-fill was required

- continuation run:
- `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1`
- starting adapter:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`
- config:
- `896` context
- `24` steps
- `6e-5` learning rate
- training runtime:
- `389.2s`

- held-out opening benchmark:
- summary:
- `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 1 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 1 / 12`

- held-out fixed follow-up benchmark:
- summary:
- `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- result:
- `json_parse_ok: 15 / 16`
- `validator_ok: 11 / 16`
- `deterministic_tool_match: 15 / 16`

- conclusion:
- this contrast mix is not a keeper
- it failed the opening benchmark materially and regressed the fixed-benchmark guardrails
- keep `gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1` as the strongest offline adapter
- do not repeat this exact contrast-training recipe; the next opening-step lane should be a different intervention, not just more of the same mix

- No heavy GPU follow-up is authorized yet.
- The next safe move is continued non-GPU prep for an opening-step corrective continuation using the existing scaffold train/val slices.

## Step-1 Runtime Intervention Result

- Runtime intervention landed in code:
- `autoresearch/controller_protocol.py`
- `autoresearch/controller.py`
- `training/eval_offline.py`
- `tests/test_controller_policy.py`
- new local profile alias for the current best adapter:
- `gemma4-e4b-local-adapter-openv2` in `autoresearch.config.json`

- Intervention contents:
- a local-only opening-step system prompt with one canonical `prepare_profile` shape
- deterministic opening-step canonicalization before controller validation
- a stronger local opening-step repair path
- offline eval support for the same opening-step prompt + canonicalizer lane

- Unit coverage:
- focused controller-policy tests for the canonicalizer and opening-step protocol selection passed

- Offline opening benchmark baseline recheck using the current best adapter:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimebaseline_compactv2_512tok.json`
- result:
- `json_parse_ok: 3 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 3 / 12`

- Offline opening benchmark with the full runtime intervention enabled:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_compactv2_512tok.json`
- result:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`

- Fixed follow-up benchmark check with the intervention flag off:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimebaseline_compactv2_512tok.json`
- result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`

- Live bounded local smoke:
- command lane:
- `.\\.venv-gpucheck-cu124\\Scripts\\python.exe -m autoresearch run --max-steps 1 --explorer-profile gemma4-e4b-local-adapter-openv2 --json`
- run:
- `runs/20260410T031917595414Z-agentic-14f163`
- outcome:
- the first local Gemma response was a single validator-clean `prepare_profile` action with no repair loop and no malformed-payload stall
- the action still failed at execution time because the model chose bad opening-step field values:
- `instruments: ["ALL"]`
- `destination_path: "C:\\profiles\\cand1.json"`
- concrete failure from the controller log:
- `Error: Unknown instrument(s): 'ALL'.`

- Interpretation:
- the runtime intervention solves the opening-step controller-contract problem
- the remaining live issue is narrower and now clearly about opening-step field grounding, not JSON discipline or missing `mode`
- the next lane should focus on grounding run-local destination paths and concrete seed instruments for fresh-run `prepare_profile`

## Step-1 Grounding Runtime Result

- Field-grounding intervention landed in code:
- `autoresearch/controller.py`
- `trainingdatapipeline/normalize_state.py`
- `training/eval_offline.py`
- `trainingdatapipeline/build_opening_grounding_benchmark.py`
- `training/benchmark_registry.json`

- Grounding intervention contents:
- local step-1 compact state now carries `seed.goal_summary` and `seed.timeframes`
- local step-1 compact state now carries `opening_grounding` with:
- `allowed_seed_instruments`
- `preferred_initial_instruments`
- `preferred_initial_instrument_rule`
- `candidate_name_hint`
- runtime canonicalization now safely rewrites:
- `instruments=["ALL"]` into controller-selected starter symbols when available
- off-run or placeholder profile outputs into the current run `profiles/` directory
- opening-step repair hints now explicitly forbid `ALL` and use `candidate_name` as the local draft handle

- Focused tests:
- `tests/test_controller_policy.py`
- current result: `26 / 26` passing

- New non-holdout opening grounding benchmark:
- reference:
- `data/training_pipeline/benchmarks/offline_opening_grounding_nonholdout_v1.jsonl`
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_grounding_nonholdout_v1_summary_runtimeintervention_compactv2_512tok.json`
- result:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `instrument_grounding_ok: 11 / 12`
- `candidate_handle_ok: 12 / 12`
- `uses_forbidden_instrument: 0`
- `opening_grounding_success: 11 / 12`

- Held-out opening contract benchmark remains intact:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`

- Fixed follow-up benchmark still holds:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`

- Live bounded local smokes:
- one-step run:
- `runs/20260410T044114301481Z-agentic-04a8dd`
- result:
- first `prepare_profile` executed successfully
- no repair loop
- no malformed payload stall
- no `ALL`
- destination path grounded inside the run `profiles/` directory
- three-step run:
- `runs/20260410T044233373841Z-agentic-83d87e`
- result:
- step 1 `prepare_profile` succeeded
- step 2 `validate_profile` succeeded
- step 3 `register_profile` succeeded

- Interpretation:
- step-1 runtime intervention plus grounding state is now good enough to make fresh-run local Gemma operational in the real loop
- the next remaining issues, if any, are downstream of opening bootstrap rather than another step-1 contract failure
- reopen training only if a later-step residual gap is clearly model-limited rather than fixable in controller/runtime state

## Candidate-Handle Contract Pivot

- Controller/tool contract now prefers `candidate_name` for run-owned local draft profiles and `profile_ref` for registered profiles.
- Raw path fields remain accepted as a backward-compatible fallback, but they are no longer the recommended model-facing contract.
- Prompt-visible typed-tool summaries now strip profile file paths and surface candidate handles instead.
- Opening-step protocol no longer teaches `destination_path`; local Gemma emits one `prepare_profile` with `candidate_name`, and the controller resolves the real run-local JSON path internally.

- Pathless contract checks with the current best adapter:
- held-out opening scaffold benchmark with runtime intervention:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_handle_compactv2_512tok.json`
- result:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`

- non-holdout opening grounding benchmark with runtime intervention:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_grounding_nonholdout_v1_summary_runtimeintervention_handle_compactv2_512tok.json`
- result:
- `instrument_grounding_ok: 11 / 12`
- `candidate_handle_ok: 12 / 12`
- `opening_grounding_success: 11 / 12`

- fixed follow-up benchmark after migrating the benchmark corpus to the handle-based contract:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimebaseline_handle_compactv2_512tok.json`
- result:
- `json_parse_ok: 15 / 16`
- `validator_ok: 11 / 16`
- `deterministic_tool_match: 14 / 16`

- Interpretation:
- The architectural path abstraction works in the live controller and keeps step 1 operational.
- The existing adapter still carries some path-era conditioning on later follow-up prompts, so the next model-improvement lane should be a small pathless follow-up adaptation pass or equivalent runtime prompt tightening, not a return to opening-step-only training.

## Pathless Follow-Up Reliability Result

- Status: complete and promoted.

- New controller/runtime changes:
- `autoresearch/controller.py`
- `autoresearch/controller_protocol.py`
- `autoresearch/__main__.py`
- `training/eval_offline.py`
- `trainingdatapipeline/normalize_state.py`

- What changed:
- controller prompt state now exposes a pathless `next_action_template` for deterministic post-opening steps
- follow-up runtime canonicalization now safely fills missing `candidate_name` / `profile_ref` / `instruments` only when controller state makes the repair deterministic
- generic later-step repair prompts now teach the pathless follow-up contract directly
- `build-portfolio` public report surfaces now strip `profile_path` / `destination_path` / `source_profile_path`
- bundle export now resolves local profile files internally from `run_id + candidate_name` or historical attempt metadata instead of requiring public `profile_path`

- Focused tests:
- `tests/test_controller_policy.py`
- `tests/test_portfolio.py`
- `tests/test_typed_tool_summaries.py`
- `tests/test_full_backtest_retries.py`
- current result: `60 / 60` passing

- Fixed follow-up benchmark with runtime follow-up canonicalizer:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimeintervention_handle_followup_compactv2_512tok.json`
- result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 16 / 16`
- `deterministic_tool_match: 16 / 16`
- `pathless_action_ok: 16 / 16`
- `runtime_canonicalized: 3`

- Opening benchmarks did not regress:
- held-out opening contract summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_pathlessfollowup_compactv2_512tok.json`
- result:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`
- non-holdout opening grounding summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_grounding_nonholdout_v1_summary_runtimeintervention_pathlessfollowup_compactv2_512tok.json`
- result:
- `instrument_grounding_ok: 11 / 12`
- `candidate_handle_ok: 12 / 12`
- `opening_grounding_success: 11 / 12`

- Live bounded local Gemma smokes:
- 3-step run:
- `runs/20260410T180659849988Z-agentic-efa690`
- result:
- `prepare_profile -> validate_profile -> register_profile`
- all three actions executed successfully under the pathless contract
- 4-step run:
- `runs/20260410T180918074100Z-agentic-283954`
- result:
- step 4 reached `evaluate_candidate`
- runtime follow-up canonicalization fired and produced:
- `{"tool":"evaluate_candidate","profile_ref":"69d93d4c9f30c0de80d5","instruments":["EURUSD"],"evaluation_mode":"screen","timeframe_policy":"profile_default"}`
- the action executed successfully

- Pathless portfolio smoke:
- config:
- `data/training_runs/pathless_portfolio_smoke_config.json`
- build result:
- `data/training_runs/pathless_portfolio_smoke_build.json`
- report:
- `runs/derived/portfolio-report/pathless-portfolio-smoke/portfolio-report.json`
- export result:
- `data/training_runs/pathless_portfolio_smoke_export.json`
- bundle result:
- `exported_profiles: 6`
- `missing_profiles: []`
- public `selected` rows in the smoke report are handle-first and do not include `profile_path`

- New interpretation:
- The next best lever is no longer broad follow-up fine-tuning by default.
- Controller/runtime tightening is now strong enough to carry pathless follow-up behavior for the current best adapter.
- Reopen a pathless follow-up adaptation pass only if future live failures remain after these controller/runtime interventions.
