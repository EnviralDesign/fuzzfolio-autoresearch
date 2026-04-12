# Gemma Explorer Tuning TODO

Status: active Codex work queue

Primary reference:

- See `z_docs/GEMMA_EXPLORER_TUNING_SPEC_V0.md` for full rationale, constraints, and implementation detail.

Default decisions locked in from current review:

- Tune explorer first.
- Keep manager on frontier models.
- Run a cheap-teacher relabel bake-off before locking a default teacher.
- Bake-off pool tested:
- `openai-54-mini`
- `gemini-3-flash-preview`
- `gemini-3.1-flash-lite-preview`
- `minimax-27`
- Default cheap relabel teacher for v0: `gemini-3-flash-preview`
- Secondary cross-check teacher for v0: `openai-54-mini`
- Do not use `gemini-3.1-flash-lite-preview` for primary relabeling in v0.
- `minimax-27` is now in-scope for the next corrective bake-off because it is already wired in the provider layer and cheap under the user's current plan.
- Judge relabel quality by constrained-output validity first, then manual inspection on sampled ambiguous states.
- Keep stronger profiles such as `openai-54` or `codex-54` available as escalation or cross-check paths if the cheap models are not good enough.
- Treat historical `run_cli` as evidence and recovery context, not positive explorer policy.
- Stay on Windows unless early validation shows a real blocker.
- Add raw explorer payload capture now so future dataset versions can learn literal repair patterns.
- Yield back to the user after the first meaningful smoke of either training-data generation or training, with cost/time notes if available.

Current live baseline:

- keep adapter baseline at `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`
- keep runtime profile `gemma4-e4b-local-adapter-openv2`
- live model-facing contract is now pathless across providers, not just local Gemma
- step-1 controller-contract reliability is now a runtime/controller strength, not a current blocker
- candidate-name abstraction is now live for run-owned local drafts
- step-1 grounding under the new handle-based contract is materially improved:
- opening grounding benchmark: `instrument_grounding_ok 11 / 12`, `candidate_handle_ok 11 / 12`, `opening_grounding_success 11 / 12`
- live one-step smoke executes `prepare_profile` successfully with only `candidate_name` and no model-supplied filesystem path
- live three-step smoke reaches `prepare_profile -> validate_profile -> register_profile` under the pathless controller contract
- direct typed-tool smoke also proves `evaluate_candidate(candidate_name=...)` works on a local unregistered draft
- remaining model gap is now later-step follow-up binding under the new handle contract:
- fixed follow-up handle benchmark: `json_parse_ok 16 / 16`, `validator_ok 11 / 16`, `deterministic_tool_match 16 / 16`
- next priority should shift to a small pathless follow-up adaptation or prompt-tightening lane before reopening broad training
- the next actual training lane should now be exploration judgment, not more path/contract work
- new reviewer-oriented curation tooling now exists:
- `trainingdatapipeline/build_exploration_review_set.py`
- `trainingdatapipeline/build_exploration_judgment_dataset.py`
- first review pack generated:
- `data/training_pipeline/review_sets/exploration_review_v1_candidates.jsonl`
- `data/training_pipeline/review_sets/exploration_review_v1_sheet.md`
- blank labels template:
- `data/training_pipeline/manual_labels/exploration_judgment_v1_template.jsonl`
- next human/Codex action for this lane:
- review by `review_id`
- keep `keep_gold` and `rewrite_action` only
- then build the curated dataset before any new Gemma continuation run

## High Priority

### 1. Early environment validation

- Status: mostly complete for v0 scaffold.
- Confirmed `uv` path works on Windows.
- Confirmed Hugging Face access and `google/gemma-4-E4B-it` tokenizer/config access.
- Confirmed training scaffold imports with transient package install.
- Remaining risk: exact CUDA PyTorch plus `bitsandbytes` runtime combination for a real QLoRA launch.

Reference:

- Spec Sections 12, 13, 16, and 17.

### 2. Add raw explorer payload capture

- Status: complete.

Why this matters:

- v0 recovery can already learn state-conditioned recovery.
- Raw capture is needed for future literal malformed-output-to-corrected-output supervision.

Reference:

- Spec Section 5.4 and Section 9.

### 3. Scaffold the training data pipeline package

- Status: complete and moved beyond placeholders.
- Current implemented modules:
- `discover_runs.py`
- `replay_controller_state.py`
- `extract_raw_steps.py`
- `normalize_state.py`
- `label_deterministic.py`
- `label_llm.py`
- `score_examples.py`
- `build_splits.py`
- `export_chat_format.py`
- `export_summary_report.py`
- `schemas.py`
- `rules.py`
- `validators.py`
- `replay_types.py`
- `offline_validator.py`
- `manual_audit_sample.py`

Reference:

- Spec Section 6 and Section 18 Phase 0.

### 4. Implement run discovery

- Status: complete.

Reference:

- Spec Section 5.3 and Section 7.1.

### 5. Implement controller-state replay

- Status: complete for v0.
- Added normalized result facts, trace event facts, action signatures, and timeframe status snapshots.

This is the central technical requirement for the project.

Reference:

- Spec Section 5.2 and Section 7.2.

### 6. Implement canonical example schema and raw step extraction

- Status: complete for v0.
- Current corpus output: `30,458` replayed steps across `224` processed runs.

Reference:

- Spec Section 7.3 and Section 8.

## Medium Priority

### 7. Implement deterministic labeling engine

- Status: complete for v0.
- Current outputs:
- `4,654` base-SFT positives
- `7,190` recovery examples
- recovery-heavy holdout of `23` runs / `2,095` records
- Lean on controller-native fields such as:
- `next_recommended_action`
- `ready_for_registration`
- `ready_to_evaluate`
- `timeframe_mismatch`
- repeated-action and branch-lifecycle constraints

Reference:

- Spec Section 7.5.

### 8. Implement scoring, filtering, and grade reports

- Status: complete for v0.

Reference:

- Spec Section 7.7.

### 9. Build manual audit sample output

- Status: mostly complete.
- Current audit sample emitted `97` examples because `controller_blocked` near-miss rejects and `prepare_profile::late` positives underflow in the current corpus.

Reference:

- Spec Section 19.

### 10. Implement frontier-assisted relabeling

- Status: scaffold complete, smoke complete, default teacher selected for v0.
- Add a cheap-model bake-off for relabeling teachers:
- `openai-54-mini`
- `gemini-3-flash-preview`
- `gemini-3.1-flash-lite-preview`
- Compare them on a small ambiguous-state batch before committing.
- Use normalized state only.
- Generate `2-4` constrained candidates.
- Run local validation and admissibility checks before keeping anything.
- Preserve multi-target cases when real ambiguity exists.

Current tiny smoke result after prompt tightening:

- `gemini-3-flash-preview`: `3/3` rows with at least one locally valid candidate
- `openai-54-mini`: `2/3`
- `gemini-3.1-flash-lite-preview`: `0/3`
- Default for the next relabel batch: `gemini-3-flash-preview`
- Keep `openai-54-mini` available for spot cross-checks or escalation on hard ambiguous states.
- next bake-off addition:
- compare `gemini-3-flash-preview` versus `minimax-27` on a corrective benchmark batch before changing the default cheap teacher
- use `openai-54-mini` only as a smaller adjudication or spot-check path, not the primary corrective teacher
- first focused relabel expansion completed:
- curated batch: `64` C-grade near-miss controller-native rows
- split mix: `48` train / `8` val / `8` test
- teacher: `gemini-3-flash-preview`
- validated selections: `64 / 64`
- wall-clock: `274.055s`
- filtered kept set after dropping browse regressions:
- `59` total kept
- `44` train / `7` val / `8` test
- dropped: `3` `list_dir`, `2` `read_file`
- openai cross-check on `8` rows:
- `6 / 8` validated
- only `1 / 6` first-tool match with Gemini on jointly valid rows, so keep OpenAI as a cross-check path, not the primary relabel source

Reference:

- Spec Section 7.6.

### 10b. Build a corrective teacher lane for discoverable extraction / generation defects

- Status: next active lane.
- This is not a replacement for deterministic labeling or replay normalization.
- Use it only after procedural extraction has already reconstructed the best prompt state we can derive.
- Purpose:
- repair narrow, repeated failure pockets where the correct fix is discoverable from current state
- add high-precision supervision for argument binding and clean JSON stopping behavior
- current v4 corrective failure pocket from the fixed benchmark:
- `2` rows where `evaluate_candidate` omits `instruments`
- `3` rows where output formatting adds trailing fence / duplicate JSON debris
- preferred workflow:
- build a small corrective batch directly from benchmark failures or audited rows
- ask cheap teachers for constrained replacements using normalized state only
- keep only locally valid outputs
- optionally use a stronger model only for adjudication on disagreements
- do not let corrective relabeling rewrite broad trajectories or override strong deterministic gold rows
- reproducibility path:
- `trainingdatapipeline/build_corrective_batch.py` builds a targeted relabel batch from failed offline benchmark predictions
- that batch can be fed directly into `trainingdatapipeline/label_llm.py --use-input-order`
- first corrective teacher comparison to run:
- `gemini-3-flash-preview`
- `minimax-27`
- evaluation criteria:
- local parse/validator pass rate
- issue-class resolution rate for `missing_evaluate_instruments` and `formatting_cleanliness`
- manual inspection on the tiny kept set before merging into train data
- corrective bake-off result on the first `5` benchmark-failure rows:
- batch source: `data/training_pipeline/corrective_batches/offline_forced_val16_v4_failures.jsonl`
- issue mix: `2` `missing_evaluate_instruments`, `3` `formatting_cleanliness`
- reproducible batch-builder now exists at `trainingdatapipeline/build_corrective_batch.py`
- after adding corrective-mode hints to `trainingdatapipeline/label_llm.py`:
- `gemini-3-flash-preview`: `5 / 5` locally valid and `5 / 5` exact first-tool matches in about `16.1s`
- `minimax-27`: `3 / 5` locally valid and `3 / 5` exact first-tool matches in about `137.5s`
- conclusion for v0:
- keep `gemini-3-flash-preview` as the default cheap corrective teacher
- keep `minimax-27` as an optional low-cost experimentation path, not the default corrective teacher
- important practical rule:
- when benchmark or deterministic reference rows already contain the correct target, prefer direct corrective SFT examples over spending teacher calls to rediscover the same gold
- reserve cheap-teacher correction for noisy extracted rows where the fix is discoverable from current state but the gold target is not already explicit

Reference:

- Spec Section 7.6, Section 11, and Section 15.1.

### 11. Build balancing and split assembly

- Status: complete for run-level v0 split assembly.

Reference:

- Spec Section 10.

### 12. Export dataset v0

- Status: strict single-target compact export complete for train/val/test.
- Multi-target export still pending.

### 12b. Build exploration-judgment review + dataset lane

- Status: first full v1 pass completed; offline gates failed, so do not promote/export this adapter.
- Purpose:
- mine real runs into judgment-heavy review rows instead of more contract-only supervision
- allow Codex/manual curation of `keep_gold` vs `rewrite_action` rows
- emit a small additive pathless dataset for exploration-quality fine-tuning
- new helpers:
- `trainingdatapipeline/build_exploration_review_set.py`
- `trainingdatapipeline/build_exploration_judgment_dataset.py`
- first generated review-set manifest:
- `data/training_pipeline/review_sets/exploration_review_v1_manifest.json`
- first generated review-sheet path:
- `data/training_pipeline/review_sets/exploration_review_v1_sheet.md`
- label template path:
- `data/training_pipeline/manual_labels/exploration_judgment_v1_template.jsonl`
- review-set v1 emitted:
- `150` candidate rows
- `47` source runs on the first pass, later refined to `45`
- current mix after builder tightening:
- `42` `strategically_weak_but_valid`
- `22` `productive_scored`
- remaining rows are mostly `ambiguous` / `stale_loop` and should be reviewed carefully, not assumed good
- next expected outputs after manual review:
- curated train / val / benchmark JSONL
- compact-v2 chat exports
- one bounded exploration-judgment continuation from the current best adapter
- current v1 realized outputs:
- curated dataset manifest:
  - `data/training_pipeline/targeted_slices/exploration_judgment_v1_manifest.json`
  - `73` validated manual rows kept
  - `204` train / `30` val / `24` held-out benchmark rows after anchors and duplication
- continuation run:
  - `data/training_runs/gemma_e4b_explorationjudgment_v1_from_openv2_gpu1`
- old-adapter exploration holdout baseline:
  - `8 / 24` first-tool match
  - `21 / 24` validator-clean
- new-adapter exploration holdout result:
  - `8 / 24` first-tool match
  - `19 / 24` validator-clean
- fixed follow-up result on the new adapter:
  - `15 / 16` parseable
  - `14 / 16` validator-clean
  - `14 / 16` deterministic tool match
- opening result on the new adapter:
  - `12 / 12` parseable
  - `12 / 12` validator-clean
  - `12 / 12` first-tool match
- interpretation:
  - the v1 curation slice preserved opening behavior but did not improve exploration
  - it likely over-taught mutate-heavy rewrites and introduced some invalid mutate tendencies
  - next lane should be a second judgment review slice built from the new comparison failures, not immediate export or LM Studio promotion
- first mixed chat dataset assembled:
- `data/training_pipeline/final/v0_chat_mix1`
- full mix counts: `2004` train / `209` val
- pilot mix counts: `160` train / `16` val
- pilot mix includes all filtered relabel additions: `44` train / `7` val

Reference:

- Spec Section 11.

## Lower Priority After Dataset v0

### 13. Build training harness

- Status: scaffold complete, first Quadro QLoRA smoke and first bounded pilot completed successfully.
- `training/train_lora.py`
- `training/eval_offline.py`
- `training/requirements-gemma.txt`
- `training/README.md`
- Real launch smoke status:
- `RTX 3070 8GB` OOM during quantized model load. Do not use it for Gemma E4B training.
- `Quadro RTX 5000 16GB` is the active local training target.
- Successful smoke completed with:
- `CUDA_VISIBLE_DEVICES=1`
- `--adapter-mode qlora`
- `--quantization 4bit`
- `--target-module-preset gemma4_language_regex`
- smoke output dir: `data/training_runs/gemma_e4b_smoke_gpu1_regex`
- smoke metrics snapshot:
- `train_runtime: 18.37s`
- `eval_runtime: 8.86s`
- `eval_loss: 13.58`
- `eval_mean_token_accuracy: 0.0142`
- first bounded pilot completed with:
- output dir: `data/training_runs/gemma_e4b_pilot_gpu1_regex`
- dataset slice: `128` train / `32` val
- `--max-steps 20`
- `train_runtime: 686.9s`
- `train_steps_per_second: 0.029`
- `eval_loss: 1.473 -> 0.464`
- `eval_mean_token_accuracy: 0.735 -> 0.917`
- note: validation over `32` rows added roughly `260s` of wall-clock across two evals, so eval cadence and val slice size are now first-order iteration costs
- Updated immediate execution ladder:
- first real retry uses `CUDA_VISIBLE_DEVICES=1` on the `Quadro RTX 5000 16GB` only
- first working path is `--adapter-mode qlora --quantization 4bit --target-module-preset gemma4_language_regex`
- this preset was validated offline to match `294` language-model modules and `0` vision/audio modules
- fallback if the working path regresses is plain LoRA: `--adapter-mode lora --quantization none`
- next controlled pilot target:
- keep the same Quadro-only QLoRA path
- reduce validation overhead for iteration runs, either by using a `16`-row val slice or evaluating only once near the end
- add a sequence-length probe at `384` or `512` before scaling dataset size much further
- keep `--max-steps` as the main pilot control knob instead of tiny fractional epochs
- first mixed `384` probe completed successfully after one transient crash during model load:
- output dir: `data/training_runs/gemma_e4b_mix1_probe384_gpu1_retry2`
- dataset slice: `160` train / `16` val with relabel additions mixed in
- `--max-seq-length 384`
- `--max-steps 20`
- `train_runtime: 1024s`
- `train_steps_per_second: 0.020`
- end-only eval:
- `eval_loss: 1.788`
- `eval_mean_token_accuracy: 0.735`
- `eval_runtime: 77.06s`
- interpretation:
- `384` context is viable on the Quadro with the working QLoRA path
- throughput drops materially versus `256`, but the run remains practical
- the earlier `384` crash looked transient because raw 4-bit Gemma load succeeded immediately afterward and the clean retry completed
- next controlled probe target:
- decide between `512` context probe versus a larger-step run at `384` on the mixed dataset
- build a small offline prediction-validity check for the new adapter before any broader controller benchmark
- contract-specialization export mismatch resolved:
- canonical `train_forced_contract_compact.jsonl` was stale at `225` rows
- rerun export restored the full `1,924` rows
- specialization continuation rerun on the full `1,924` rows completed, but it did not improve the fixed deterministic benchmark
- current interpretation:
- the remaining problem is not lack of specialization rows alone
- the bigger issues are prompt-contract drift and prompt-state truncation
- full live controller prompt is too large for local SFT at current practical sequence lengths:
- full controller prompt alone is about `1,623` tokens on Gemma
- this is not practical for the Quadro training window that has been working so far
- new shared prompt module added:
- `autoresearch/controller_protocol.py`
- live controller still uses full `SYSTEM_PROTOCOL`
- SFT and offline eval now use a compressed `SFT_SYSTEM_PROTOCOL` that preserves exact tool names, raw-JSON shape, no invented tools, and no `parameters` wrapper
- current compact-v2 state experiment:
- added `prompt_state_compact_v2` path in normalization/export/eval
- compact-v2 token profile on contract-specialization sample:
- system prompt: `362` tokens
- user state median: `371` tokens
- full sequence median: `798` tokens
- full sequence `p95`: `857` tokens
- Quadro smoke at `1536` context on compact-v2 fit but was too slow to be operational:
- `5` steps took about `54` minutes
- so the next usable lane is not longer-context brute force
- the highest-signal offline result so far:
- existing adapter `data/training_runs/gemma_e4b_mix1_probe384_gpu1_retry2/adapter`
- benchmarked with `prompt_variant=compact-v2`
- `first_tool_match: 15 / 16`
- `deterministic_tool_match: 15 / 16`
- but `validator_ok: 0 / 16`
- interpretation:
- once the state is compact and focused, the model usually chooses the correct tool
- it still misses required handles such as `profile_path`, `profile_ref`, `attempt_id`, `artifact_dir`, or `instruments`
- next required data fix:
- enrich replayed recent-step state with handle-carrying fields needed for deterministic follow-ups
- likely fields: `destination_path`, `profile_path`, `profile_ref`, `attempt_id`, `artifact_dir`, `candidate_name`, `instruments`, `requested_horizon_months`, `evaluation_mode`
- replay-handle enrichment completed in v4:
- replay now carries richer action signatures for `destination_path`, `profile_path`, `profile_ref`, `attempt_id`, `artifact_dir`, `candidate_name`, `instruments`, `requested_horizon_months`, and `evaluation_mode`
- compact-v2 now exposes a small `handles` block plus lean recent-step handle fields
- fresh pipeline rebuild completed:
- `replayed_steps_v4.jsonl`
- `examples_full_v4.jsonl`
- `deterministic_labels_v4.jsonl`
- `scored_examples_v4.jsonl`
- `final/v4`
- refreshed benchmark created:
- `data/training_pipeline/benchmarks/offline_forced_val16_v4.jsonl`
- highest-signal compact-v2 v4 benchmark result so far:
- adapter: `data/training_runs/gemma_e4b_mix1_probe384_gpu1_retry2/adapter`
- `json_parse_ok: 13 / 16`
- `validator_ok: 11 / 16`
- `first_tool_match: 12 / 16`
- `deterministic_tool_match: 12 / 16`
- interpretation:
- replay-handle enrichment converted the earlier compact-v2 result from mostly-correct tool choice with invalid arguments into mostly valid contract execution on the fixed benchmark
- remaining failures are narrow:
- `evaluate_candidate` still sometimes omits `instruments` even when exposed in handles
- `3` rows still produce parse-debris outputs with trailing fence/junk text
- compact-v2 specialization training experiments from the same starting adapter:
- pilot: `data/training_runs/gemma_e4b_contractspec_v4_compactv2_pilot256_len896_from384_gpu1`
- full contract slice: `data/training_runs/gemma_e4b_contractspec_v4_compactv2_full1924_len896_from384_gpu1`
- practical note:
- `896` context is a viable Quadro lane
- `40` steps on the full `1924`-row contract set took about `5` minutes of optimizer runtime
- outcome:
- neither compact-v2 continuation run improved the fixed v4 benchmark beyond `11 / 16` validator and `12 / 16` deterministic tool match
- next recommendation:
- do not spend more time on more-of-the-same contract SFT yet
- build a small explicit argument-binding slice for `evaluate_candidate` and `inspect_artifact`, plus a formatting-cleanliness slice for trailing-fence / duplicate-json outputs

Reference:

- Spec Section 12, Section 13, and Section 14.

### 14. Build offline eval harness

- Validate format, admissibility, tool choice, and recovery class correctness on held-out prompts.
- Status: materially in progress.
- Current additions:
- `training/eval_offline.py` now supports direct adapter generation and local admissibility validation.
- frozen benchmark slice: `data/training_pipeline/benchmarks/offline_forced_val16.jsonl`
- benchmark registry entry added in `training/benchmark_registry.json` as `offline-forced-val16-v1`
- important runtime note:
- use `max_new_tokens >= 512` for adapter-generation evals
- `256` caused truncation-driven JSON parse failures and understated validity
- current best fixed-benchmark result so far:
- adapter: `data/training_runs/gemma_e4b_mix1_probe384_gpu1_retry2/adapter`
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `first_tool_match: 4 / 16`
- `deterministic_tool_match: 4 / 16`
- dominant failure mode has shifted from syntax to transition choice:
- model still jumps ahead, restarts search, or invents near-plausible tools after forced controller states
- current blocker discovered through this harness:
- the first contract-specialization continuation run used `data/training_pipeline/contract_specialization/train_forced_contract_compact.jsonl`
- that file contains only `225` rows
- the source specialization set contains `1,924` rows
- a rerun export already exists as `train_forced_contract_compact_rerun.jsonl` with the full `1,924` rows
- next action: regenerate the canonical compact specialization file in place and rerun the continuation pass against the full dataset
- benchmark interpretation update:
- the old compact prompt made the adapter miss the right tool family
- the new compact-v2 prompt flips that:
- `first_tool_match: 15 / 16`
- `deterministic_tool_match: 15 / 16`
- but still `validator_ok: 0 / 16` because required handles are missing from the compact-v2 state
- immediate next action:
- enrich replay-state handles, rebuild the benchmark reference path, and re-run compact-v2 offline eval before committing to another real training pass
- v4 compact-v2 benchmark update after handle enrichment:
- benchmark id: `offline-forced-val16-v4`
- existing best adapter now reaches:
- `json_parse_ok: 13 / 16`
- `validator_ok: 11 / 16`
- `first_tool_match: 12 / 16`
- `deterministic_tool_match: 12 / 16`
- compact-v2 continuation runs at `896` context were practical but did not improve those scores
- current blocker is now localized argument binding and formatting cleanliness, not broad tool-choice confusion
- continuation-training bug discovered and fixed:
- `training/train_lora.py` previously reopened `--adapter-init-dir` with `PeftModel.from_pretrained(...)`
- PEFT defaults `is_trainable=False` there, so those continuation runs were effectively frozen
- trainer now reopens existing adapters with `is_trainable=True`
- parameter report added to training outputs so continuation runs prove nonzero trainable params
- first real trainable continuation on a non-benchmark compact-v2 corrective slice:
- slice builder: `trainingdatapipeline/build_targeted_slice.py`
- train slice: `256` rows balanced toward `evaluate_candidate` / `inspect_artifact` with small `register_profile` / `validate_profile` anchors
- val slice: `32` rows, benchmark run excluded
- run: `data/training_runs/gemma_e4b_contractfix_v1_len896_from384_gpu1_trainable`
- parameter report: `36,700,160` trainable params out of `5,787,888,160`
- fixed v4 benchmark improved to:
- `json_parse_ok: 15 / 16`
- `validator_ok: 13 / 16`
- `first_tool_match: 14 / 16`
- `deterministic_tool_match: 14 / 16`
- remaining failures after that run were fully localized to `evaluate_candidate`
- second narrower continuation on `evaluate_candidate` only:
- run: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1`
- benchmark tradeoff:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `first_tool_match: 14 / 16`
- `deterministic_tool_match: 14 / 16`
- all remaining failures became `evaluate_candidate` missing `instruments`
- current interpretation:
- the mainline bug fix was high leverage
- targeted continuation is now demonstrably moving the benchmark
- next mainline choice is between:
- favoring the broader corrective adapter for fewer missing-field failures
- or favoring the narrower evaluate adapter for perfect first-pass JSON on the fixed benchmark
- do not spend more time on cheap-teacher exploration before choosing that next benchmark lane
- global handle-contract migration now complete for active controller prompts, active benchmark exports, and active v4 dataset exports
- active migrated files were revalidated to contain `0` remaining model-facing legacy keys:
- `profile_path`
- `destination_path`
- `source_profile_path`
- opening benchmark under runtime intervention and the new handle contract remains perfect:
- `json_parse_ok 12 / 12`
- `validator_ok 12 / 12`
- `first_tool_match 12 / 12`
- opening grounding non-holdout benchmark under the handle contract:
- `instrument_grounding_ok 11 / 12`
- `candidate_handle_ok 11 / 12`
- `opening_grounding_success 11 / 12`
- fixed follow-up benchmark under the handle contract:
- `json_parse_ok 16 / 16`
- `validator_ok 11 / 16`
- `first_tool_match 16 / 16`
- `deterministic_tool_match 16 / 16`
- interpretation:
- the pathless migration did not break later-step tool choice
- the remaining weakness is still later-step field binding, especially `evaluate_candidate` missing `instruments`
- next lane should be small and surgical:
- pathless follow-up prompt tightening and/or adaptation for `validate_profile`, `register_profile`, `mutate_profile`, and `evaluate_candidate`

Reference:

- Spec Section 15.1.

### 15. Global pathless controller contract migration

- Status: complete for the live contract and active benchmark/data surfaces.
- Live contract rules now apply to all providers:
- local drafts use `candidate_name`
- registered profiles use `profile_ref`
- path-era fields remain only as internal fallback and historical-ingestion compatibility
- controller/prompt work completed:
- true opening-step strict prompt now applies across providers
- repair prompts now teach handles, not paths
- prompt-visible summaries strip path-era fields
- path-era model fields are salvageable only through the compatibility boundary
- pipeline/data work completed:
- active v4 splits and active benchmark files migrated to the handle contract
- replay/normalization now rewrites deterministic path-era handles into pathless prompt-visible forms
- active exported records verified to have zero remaining model-facing legacy keys:
- `profile_path`
- `destination_path`
- `source_profile_path`
- live checks completed:
- one-step smoke succeeded
- three-step smoke succeeded
- direct `evaluate_candidate(candidate_name=...)` typed-tool smoke succeeded
- cleanup note:
- internal execution/tool envelopes may still contain resolved filesystem paths for artifact/debug purposes
- treat that as secondary cleanup, not a live-contract blocker

Reference:

- Spec Section 6, Section 7, Section 8, and Section 15.

### 15. Build online eval harness

- Paired baseline-versus-tuned controller runs over a frozen benchmark registry.

Reference:

- Spec Section 15.2 and Section 17.

### 16. First smoke checkpoints

- Stop after the first meaningful training-data generation smoke and report:
- runtime
- example counts
- failure modes
- any measurable token/cost data if available

- Stop again after the first training smoke and report:
- whether training launched cleanly
- effective hardware path
- time/cost feel
- whether Windows remains viable

This is a required yield point back to the user.

Status:

- complete for the first smoke checkpoint
- Windows remains viable for v0
- the live working training path is the Quadro-only language-regex QLoRA configuration
- the first larger pilot is now complete
- the next checkpoint should answer how far sequence length can be pushed on the Quadro before throughput collapses

## Known Risks To Watch

- Windows-local Python/tooling mismatches
- Hugging Face auth or gated-model access
- Gemma tokenizer/runtime incompatibilities
- replay drift from live controller prompt semantics
- too much surviving historical `run_cli` contamination
- over-retention of `read_file`/`list_dir` churn
- missing raw explorer payload data in old runs

## Latest Runtime Status

- Python-native local inference profile now exists in `autoresearch.config.json` as `gemma4-e4b-local-adapter`.
- Local provider-only smoke is working:
- `python -m autoresearch test-providers --profile gemma4-e4b-local-adapter --json`
- both built-in JSON scenarios pass on the Quadro path.
- First controller-path smoke succeeded at `max_steps=1`:
- run: `runs/20260409T152402605204Z-agentic-cd93b8`
- first-pass JSON parsed and normalized
- typed actions executed cleanly:
- `prepare_profile`
- `validate_profile`
- `register_profile`
- Longer controller smokes exposed the remaining runtime gap:
- with a normal phase budget (`max_steps=10`), the local model entered early phase and returned a parseable opening payload, but omitted required `prepare_profile.mode`
- run: `runs/20260409T160703542106Z-agentic-c4aaa8`
- controller correctly triggered response repair, but that repair lane is still too expensive for local inference if the first response is mechanically wrong
- Local runtime prompt shaping work now landed:
- local providers use `SFT_SYSTEM_PROTOCOL` instead of the larger frontier-model system contract
- local provider malformed-output repair is cheaper and shorter
- controller response/payload-shape repair now uses a compact local-aware repair prompt
- controller can now emit a provider-gated compact runtime state packet built through the replay-style `compact_v2` normalizer
- Current blocker from the most recent compact-runtime smoke:
- run: `runs/20260409T162437734647Z-agentic-1f3192`
- the local process produced no raw payload and never advanced past `explorer_provider`
- the shell log ended after `provider_trace event=complete_json_start ...`
- this points to a native/local-runtime instability before first decoded output, not a normal controller validation failure
- important user constraint:
- pause additional heavy GPU/training/controller-local runs for now
- the Quadro RTX 5000 also drives the display stack and these heavy local runs make the machine near-unusable
- next work should stay non-heavy until the user explicitly reopens GPU-heavy testing

## Next Non-GPU Lane

- Instrument the local provider with lightweight prompt/generation stats so the next GPU attempt explains prompt token count and failure stage before any native crash.
- Build a targeted corrective dataset slice for opening-step `prepare_profile` scaffolds, especially the `mode=scaffold_from_seed` omission seen in live local runs.
- Add an offline benchmark slice for first-step scaffold actions so this failure class is measured before the next continuation pass.
- Review whether the compact runtime packet should be trimmed further before another local controller smoke.

## Opening-Scaffold Lane

- Non-GPU progress landed for the live opening-step failure pocket.
- New slice builder:
- `trainingdatapipeline/build_opening_scaffold_slice.py`
- Supports repeatable `--input`, step/tool/mode filtering, and comma-separated grades or `ANY`.
- New artifacts:
- train slice: `data/training_pipeline/targeted_slices/opening_scaffold_train_v1.jsonl`
- rows: `32`
- val slice: `data/training_pipeline/targeted_slices/opening_scaffold_val_v1.jsonl`
- rows: `6`
- benchmark slice: `data/training_pipeline/benchmarks/offline_opening_scaffold_valtest_v1.jsonl`
- rows: `12`
- chat exports:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v1_compact_v2.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v1_compact_v2.jsonl`
- benchmark registry entry added:
- `offline-opening-scaffold-valtest-v1`
- current interpretation:
- the corpus already contains clean opening-step scaffold examples with `mode=scaffold_from_seed`
- the live local failure is therefore more likely underweighting/generalization than outright data absence
- offline adapter benchmark now confirms the opening-step weakness is real and distinct:
- adapter: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- benchmark: `data/training_pipeline/benchmarks/offline_opening_scaffold_valtest_v1.jsonl`
- result:
- `json_parse_ok: 2 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 2 / 12`
- `avg_generated_tokens: 235.83`
- `avg_duration_seconds: 56.563`
- concrete failure pattern from the prediction file:
- `10 / 12` rows failed JSON parse
- all `12 / 12` outputs started with fenced JSON instead of a single raw object
- `7 / 12` appended bad suffix text after the object
- the model repeatedly invented generic opening vocabulary such as `profile_name` and `seed_indicators`
- the model often hallucinated multi-step follow-ons like `validate_profile` inside the same response instead of stopping after `prepare_profile`
- the `2` parseable rows still omitted required `prepare_profile.mode`
- current interpretation update:
- the remaining opening-step problem is mostly response-discipline and typed-field binding, not missing corpus coverage
- the next corrective training lane should be a narrow opening-step scaffold continuation, not more broad contract data
- first bounded opening-step corrective continuation completed:
- run: `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1`
- starting adapter: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- config:
- `896` context
- `24` steps
- `1e-4` learning rate
- `32` train / `6` val rows
- `36,700,160` trainable parameters
- training outcome:
- ran cleanly in about `309.9s`
- opening benchmark result after continuation:
- summary: `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- `json_parse_ok: 4 / 12` (up from `2 / 12`)
- `validator_ok: 0 / 12` (no improvement)
- `first_tool_match: 4 / 12` (up from `2 / 12`)
- fixed benchmark result after continuation:
- summary: `data/training_runs/gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- `json_parse_ok: 14 / 16` (down from `16 / 16`)
- `validator_ok: 10 / 16` (down from `13 / 16`)
- `deterministic_tool_match: 13 / 16` (down from `14 / 16`)
- interpretation:
- this run improved output discipline somewhat, but it did not fix the core opening-step contract
- parseable opening rows still fail because `prepare_profile.mode` is missing
- the continuation also regressed the stronger fixed follow-up benchmark
- decision:
- do not promote `gemma_e4b_openingscaffold_v1_from_evalcandfix_gpu1` as the new default adapter
- next opening-step data lane should be narrower than the current scaffold slice:
- force a single clean `prepare_profile` object
- include explicit `mode=scaffold_from_seed`
- suppress generic field drift such as `profile_name` and `seed_indicators`
- suppress follow-on action chaining in the same opening response
- narrow opening-step corrective lane now prepared:
- builder upgraded: `trainingdatapipeline/build_opening_scaffold_slice.py`
- new options support:
- exact action-count filtering
- first-action field stripping
- deterministic opening-step reasoning rewrite
- generated artifacts:
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_train_v2_narrow_compact_v2.jsonl`
- `data/training_pipeline/targeted_slices/opening_scaffold_val_v2_narrow_compact_v2.jsonl`
- counts:
- train `19`
- val `2`
- normalization applied:
- exact action count `1`
- only `tool`, `mode`, `indicator_ids`, `instruments`, `candidate_name`, `destination_path`
- reasoning rewritten to a short opening-step statement
- next recommended run recipe:
- `training/run_recipes/opening_scaffold_continuation_v2_narrow.md`
- narrow opening-step corrective continuation completed:
- run: `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1`
- starting adapter: `data/training_runs/gemma_e4b_evalcandfix_v1_len896_from_contractfix_gpu1/adapter`
- config:
- `896` context
- `16` steps
- `8e-5` learning rate
- `19` train / `2` val rows
- training runtime:
- `183.2s`
- opening benchmark result:
- summary: `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- `json_parse_ok: 3 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 3 / 12`
- fixed benchmark result:
- summary: `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`
- interpretation:
- the narrow slice did not solve the opening-step `mode` failure
- but unlike the broader opening continuation, it preserved the strong fixed benchmark and improved deterministic tool match there
- current recommendation:
- treat `gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1` as the new strongest offline adapter candidate for follow-up behavior
- do not treat opening-step correction as solved yet
- next opening-step lane should likely require even more explicit negative/contrast supervision for:
- no fenced JSON
- no generic fields like `profile_name` / `seed_indicators`
- no chained follow-on actions
- explicit `mode=scaffold_from_seed`
- opening contrast lane implemented and evaluated:
- new builder: `trainingdatapipeline/build_opening_contrast_dataset.py`
- non-holdout opening pool:
- `data/training_pipeline/targeted_slices/opening_scaffold_nonholdout_v1.jsonl`
- non-holdout opening predictions from current strongest adapter:
- `data/training_pipeline/targeted_slices/opening_scaffold_nonholdout_v1_predictions_openv2.jsonl`
- mixed dataset artifacts:
- `data/training_pipeline/targeted_slices/opening_contrast_train_v1.jsonl`
- `data/training_pipeline/targeted_slices/opening_contrast_val_v1.jsonl`
- chat exports:
- `data/training_pipeline/targeted_slices/opening_contrast_train_v1_compact_v2.jsonl`
- `data/training_pipeline/targeted_slices/opening_contrast_val_v1_compact_v2.jsonl`
- dataset report:
- `data/training_pipeline/targeted_slices/opening_contrast_v1_summary.json`
- dataset composition:
- train `62`
- val `12`
- positives `21`
- corrective rows `21`
- anchor rows `32`
- no teacher gap-fill was needed because the non-holdout failure pool was large enough
- corrective issue counts:
- `opening_formatting_cleanliness: 18`
- `generic_profile_name: 21`
- `generic_seed_indicators: 12`
- `opening_action_chaining: 17`
- `missing_prepare_mode: 3`
- bounded continuation completed:
- run: `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1`
- starting adapter: `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/adapter`
- config:
- `896` context
- `24` steps
- `6e-5` learning rate
- train runtime about `389.2s`
- held-out opening benchmark result:
- summary: `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1/offline_opening_scaffold_valtest_v1_summary_compactv2_512tok.json`
- `json_parse_ok: 1 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 1 / 12`
- held-out fixed follow-up benchmark result:
- summary: `data/training_runs/gemma_e4b_openingcontrast_v1_from_openv2_gpu1/offline_forced_val16_v4_summary_compactv2_512tok.json`
- `json_parse_ok: 15 / 16`
- `validator_ok: 11 / 16`
- `deterministic_tool_match: 15 / 16`
- decision:
- do not promote `gemma_e4b_openingcontrast_v1_from_openv2_gpu1`
- keep `gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1` as the strongest offline adapter
- interpretation:
- simply mixing positive openings, failure-derived corrective rows, and later-step anchors did not fix the opening-step `mode` failure
- this lane also regressed the fixed benchmark guardrails
- next opening-step work should not repeat this same training recipe; prefer stronger contrast construction or inference/runtime intervention
- no GPU continuation should be launched until the user explicitly reopens heavy runs
- best next non-GPU follow-up:
- keep packaging the opening-step corrective lane using the existing non-holdout scaffold train/val slices
- avoid training on the held-out `offline-opening-scaffold-valtest-v1` rows directly

## Step-1 Runtime Intervention Status

- Implemented local-only opening-step runtime intervention in:
- `autoresearch/controller_protocol.py`
- `autoresearch/controller.py`
- `training/eval_offline.py`
- `tests/test_controller_policy.py`
- Added isolated local profile alias in `autoresearch.config.json`:
- `gemma4-e4b-local-adapter-openv2`

- Runtime intervention contents:
- strict opening-step system prompt for local Gemma only
- deterministic opening-step canonicalization before controller validation
- stronger opening-step-specific local repair messaging
- offline eval support for the same opening-step prompt plus canonicalizer lane

- Validation:
- focused controller-policy tests passed for:
- `mode` insertion from `indicator_ids`
- `profile_name -> candidate_name`
- `seed_indicators -> indicator_ids`
- chained opening actions truncation
- true local-opening protocol selection

- Offline opening benchmark baseline recheck:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimebaseline_compactv2_512tok.json`
- result:
- `json_parse_ok: 3 / 12`
- `validator_ok: 0 / 12`
- `first_tool_match: 3 / 12`

- Offline opening benchmark with full runtime intervention:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_compactv2_512tok.json`
- result:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`

- Fixed follow-up benchmark recheck with intervention flag off:
- summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimebaseline_compactv2_512tok.json`
- result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`

- Bounded live controller smoke:
- profile:
- `gemma4-e4b-local-adapter-openv2`
- run:
- `runs/20260410T031917595414Z-agentic-14f163`
- outcome:
- first response was validator-clean and reached one `prepare_profile` with no repair loop
- no malformed-payload stall occurred
- action execution still failed because opening-step field grounding is not solved yet:
- `instruments: ["ALL"]`
- `destination_path: "C:\\profiles\\cand1.json"`
- controller-log failure:
- `Error: Unknown instrument(s): 'ALL'.`

- New interpretation:
- the runtime intervention solves the opening-step controller-contract problem
- the remaining live gap is now opening-step field grounding, not missing `mode`
- next lane should target:
- concrete seed instruments from run context
- run-local destination paths under the run `profiles/` directory
- possibly a deterministic post-parse canonicalizer for `ALL` and off-run `destination_path` when the repair is state-conditioned and safe

## Step-1 Grounding Runtime Result

- Status: complete and promoted.

- New benchmark:
- `data/training_pipeline/benchmarks/offline_opening_grounding_nonholdout_v1.jsonl`

- New implementation files:
- `autoresearch/controller.py`
- `trainingdatapipeline/normalize_state.py`
- `training/eval_offline.py`
- `trainingdatapipeline/build_opening_grounding_benchmark.py`
- `training/benchmark_registry.json`

- Measured offline result:
- held-out opening contract benchmark with runtime intervention:
- `json_parse_ok: 12 / 12`
- `validator_ok: 12 / 12`
- `first_tool_match: 12 / 12`
- opening grounding benchmark with runtime intervention:
- `instrument_grounding_ok: 11 / 12`
- `destination_path_ok: 11 / 12`
- `uses_forbidden_instrument: 0`
- `opening_grounding_success: 10 / 12`
- fixed follow-up benchmark still held:
- `json_parse_ok: 16 / 16`
- `validator_ok: 13 / 16`
- `deterministic_tool_match: 15 / 16`

- Live smoke result:
- one-step run `20260410T044114301481Z-agentic-04a8dd`:
- first `prepare_profile` executed successfully
- three-step run `20260410T044233373841Z-agentic-83d87e`:
- `prepare_profile -> validate_profile -> register_profile`

- New recommendation:
- do not reopen another opening-step-only corrective SFT lane by default
- move attention to downstream local-explorer behavior after successful bootstrap
- only reopen training if later failures look clearly model-limited rather than runtime/state-fixable

## Notes For Future Codex / Sub-Agents

- Start from this TODO for execution order.
- Refer back to `z_docs/GEMMA_EXPLORER_TUNING_SPEC_V0.md` before changing scope.
- Prefer mini sub-agents for bounded exploration, triage, and mechanical edits.
- Use full `gpt-5.4` class reasoning only for difficult design, review, and integration decisions.

## Pathless Follow-Up Reliability Result

- Status: complete and promoted.

- Implementation files:
- `autoresearch/controller.py`
- `autoresearch/controller_protocol.py`
- `autoresearch/__main__.py`
- `training/eval_offline.py`
- `trainingdatapipeline/normalize_state.py`

- Outcome:
- pathless follow-up controller shaping plus conservative runtime canonicalization fixed the active follow-up benchmark without another fine-tune
- current best summary:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_forced_val16_v4_summary_runtimeintervention_handle_followup_compactv2_512tok.json`
- result:
- `json_parse_ok: 16 / 16`
- `validator_ok: 16 / 16`
- `deterministic_tool_match: 16 / 16`
- `pathless_action_ok: 16 / 16`
- `runtime_canonicalized: 3`

- Opening did not regress:
- held-out opening contract:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_scaffold_valtest_v1_summary_runtimeintervention_pathlessfollowup_compactv2_512tok.json`
- `12 / 12` parseable
- `12 / 12` validator-clean
- non-holdout opening grounding:
- `data/training_runs/gemma_e4b_openingscaffold_v2_narrow_from_evalcandfix_gpu1/offline_opening_grounding_nonholdout_v1_summary_runtimeintervention_pathlessfollowup_compactv2_512tok.json`
- `11 / 12` instrument grounding
- `12 / 12` candidate-handle grounding

- Live local Gemma proof:
- run `20260410T180659849988Z-agentic-efa690`:
- `prepare_profile -> validate_profile -> register_profile`
- run `20260410T180918074100Z-agentic-283954`:
- reached `evaluate_candidate`
- follow-up runtime canonicalization fired and the evaluated action executed successfully

- Pathless portfolio smoke:
- config:
- `data/training_runs/pathless_portfolio_smoke_config.json`
- build artifact:
- `data/training_runs/pathless_portfolio_smoke_build.json`
- export artifact:
- `data/training_runs/pathless_portfolio_smoke_export.json`
- result:
- public report `selected` rows are handle-first
- bundle export still resolves local profiles successfully through internal fallback

- New default:
- do not reopen broad fine-tuning
- do not reopen opening-step-only tuning
- treat controller/runtime pathless follow-up support as the active baseline

- Next lane:
- inspect future live later-step misses and classify them into:
- still deterministic and controller-fixable
- ambiguous state-shaping issue
- genuine model-behavior gap
- only if failures land in the third bucket should we open a small pathless follow-up adaptation pass for:
- `validate_profile`
- `register_profile`
- `mutate_profile`
- `evaluate_candidate`
