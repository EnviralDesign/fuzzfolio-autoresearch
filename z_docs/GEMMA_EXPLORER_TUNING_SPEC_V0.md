# Gemma Explorer Tuning Spec v0

Status: draft for review

Owner goal: reduce wasted tokens, retries, and wall-clock time in `fuzzfolio-autoresearch` by tuning a local Gemma-based explorer that is materially better at the controller's JSON protocol, typed-tool selection, and one-step recovery behavior.

This spec is grounded in the current repository, current controller behavior, and the local runs corpus present in `C:\repos\fuzzfolio-autoresearch\runs` as of April 8, 2026.

Execution tracking for this spec lives in `z_docs/GEMMA_EXPLORER_TUNING_TODO.md`.

## 1. Decision Summary

This project will be treated as a controller-fluency and state-action learning project first.

Primary target:

- Tune the explorer only.
- Keep the manager on frontier models.
- Start with Gemma 4 E4B instruction-tuned plus LoRA/QLoRA adapters.
- Evaluate 26B only after the E4B pipeline, dataset, and eval harness are proven.

Primary objectives for the tuned explorer:

- Emit valid controller top-level JSON on the first try.
- Prefer typed tools over `run_cli`.
- Select valid typed tools with correct required fields.
- Recover from controller, tool, or policy failures in one step when possible.
- Increase useful progress per run, not just offline format accuracy.

Secondary objective:

- Improve branch-level next-action quality after the mechanical contract is stable.

## 2. Repo-Grounded Constraints

The design must align to the current controller contract, not an imagined future contract.

Relevant code ownership:

- Explorer prompt and controller packet assembly live in `autoresearch/controller.py`.
- Typed tool surface lives in `autoresearch/typed_tools.py`.
- Manager packet and manager separation live in `autoresearch/manager_packet.py`.
- Current manager/controller ownership is summarized in `z_docs/MANAGER_CONTROLLER_ARCHITECTURE.md`.

Important current controller facts:

- `_run_state_prompt()` in `autoresearch/controller.py` is the authoritative explorer-visible state packet.
- `_normalize_model_response()` accepts a narrow family of top-level shapes and normalizes them into `{reasoning, actions}`.
- `_validate_repeated_actions()`, `_validate_timeframe_mismatch_block()`, branch lifecycle validation, finish timing, and response repair already encode useful supervision signals.
- Typed tool results often include `next_recommended_action`, `ready_for_registration`, `ready_to_evaluate`, `candidate_summary`, `timeframe_mismatch`, and `auto_log`.
- Runtime traces contain explicit controller phases such as `response_repair`, `payload_shape_repair`, `response_guard`, `step_guard`, and `action_execution`.

This means the system itself already generates most of the labels we want. The pipeline should reuse those deterministic signals instead of inventing a parallel semantics layer.

## 3. Corpus Observations

Observed local corpus shape:

- `241` run directories under `runs/`
- `224` runs with `controller-log.jsonl`
- `209` runs with `attempts.jsonl`
- `232` runs with `seed-prompt.json`
- `133` runs with `runtime-trace.jsonl`
- `133` runs with `runtime-state.json`

Observed controller-log step distribution:

- `30458` total logged steps
- `11881` steps with empty reasoning
- `4072` steps containing at least one failed result
- `1488` steps with manager events

Observed historical action mix in controller logs:

- `run_cli: 24038`
- `read_file: 6982`
- `evaluate_candidate: 1938`
- `list_dir: 1910`
- `validate_profile: 1552`
- `register_profile: 1498`
- `prepare_profile: 1254`
- `inspect_artifact: 1090`
- `log_attempt: 860`
- `write_file: 627`
- `mutate_profile: 562`
- `run_parameter_sweep: 374`

Observed runtime-trace signals on the newer traced subset:

- `94 / 133` traced runs contain `response_repair`
- `47 / 133` traced runs contain `payload_shape_repair`
- `99 / 133` traced runs contain `step_guard`
- `1083` failed actions in traced runs
- `11350` typed-tool action completions in traced runs
- `5167` `run_cli` action completions in traced runs

Interpretation:

- The corpus is rich enough for supervision.
- The corpus is not uniform, so `controller-log.jsonl` must be the mandatory backbone.
- Historical runs still contain large amounts of `run_cli`, `read_file`, and `list_dir` behavior.
- That older behavior is useful as state and outcome evidence, but it should not define the positive target policy for the tuned explorer.

## 4. Scope for v0

In scope:

- A reproducible extraction pipeline over ignored local runs.
- A controller-faithful replay step that reconstructs explorer-visible state.
- Deterministic labeling and scoring for high-confidence examples.
- Frontier-assisted relabeling for ambiguous high-value states.
- A recovery-focused dataset slice.
- A versioned explorer dataset `v0`.
- A first-pass Gemma 4 E4B-it LoRA/QLoRA training stack.
- Offline and online evaluation harnesses.
- Deployment notes for adapter-first inference.

Out of scope for v0:

- Manager tuning.
- Joint explorer and manager tuning.
- Full-model fine-tuning.
- Treating historical `run_cli` plans as positive imitation targets.
- Benchmark optimization that is disconnected from controller productivity.
- LM Studio as the training environment.

## 5. Core Design Decisions

### 5.1 Positive targets will be typed-tool first

Historical `run_cli` steps are not the desired future explorer policy.

Policy for `run_cli` data:

- Keep `run_cli` results as state evidence when they reveal environment transitions, failures, or attempt outcomes.
- Keep selected `run_cli` episodes as recovery-context inputs when they expose a failure class the new explorer should avoid.
- Do not use `run_cli` actions as positive SFT targets for normal explorer behavior.
- Do not allow dataset balancing to collapse into `run_cli`, `read_file`, or `list_dir` imitation.

### 5.2 Prompt state must be rebuilt from the live controller view

The dataset will not define `prompt_state` by ad hoc log scraping alone.

Instead, extraction must reconstruct a prompt-state object that corresponds to what the explorer actually had available at that step, centered on the same concepts exposed by `_run_state_prompt()`:

- phase and horizon guidance
- score target
- manager guidance
- run outcome and working memory
- branch lifecycle run packet
- retention and exploit pacing status
- timeframe mismatch status
- seed hand and sticky catalog context
- run-owned profiles summary
- recent attempts summary
- frontier snapshot
- recent behavior digest

This is the most important grounding requirement in the spec.

### 5.3 `controller-log.jsonl` is the canonical extraction backbone

Required input per run:

- `controller-log.jsonl`

Optional enrichments:

- `runtime-trace.jsonl`
- `runtime-state.json`
- `attempts.jsonl`
- `seed-prompt.json`
- `run-metadata.json`
- `profiles/`
- `evals/`
- `checkpoint-summary.txt`

The pipeline must work when only the required file plus a subset of enrichments exists.

### 5.4 Recovery v0 is state-conditioned recovery, not raw-bad-payload imitation

The current run corpus exposes repair events and failure outcomes, but it does not appear to durably preserve every raw malformed explorer payload in the run directories.

Therefore:

- v0 recovery data will focus on "given this failure state, what is the correct next move?"
- literal malformed-output-to-corrected-output supervision will be limited to cases where the source payload is recoverable from artifacts
- a future data version may add explicit raw explorer payload capture to improve mechanical repair tuning further

## 6. Proposed Workspace Layout

New package:

- `trainingdatapipeline/`

Supporting directories:

- `trainingdatapipeline/__init__.py`
- `trainingdatapipeline/discover_runs.py`
- `trainingdatapipeline/replay_controller_state.py`
- `trainingdatapipeline/extract_raw_steps.py`
- `trainingdatapipeline/normalize_state.py`
- `trainingdatapipeline/label_deterministic.py`
- `trainingdatapipeline/label_llm.py`
- `trainingdatapipeline/score_examples.py`
- `trainingdatapipeline/build_splits.py`
- `trainingdatapipeline/export_chat_format.py`
- `trainingdatapipeline/export_summary_report.py`
- `trainingdatapipeline/schemas.py`
- `trainingdatapipeline/rules.py`
- `trainingdatapipeline/validators.py`
- `trainingdatapipeline/replay_types.py`

Training scripts:

- `training/train_lora.py`
- `training/eval_offline.py`
- `training/eval_online.py`
- `training/benchmark_registry.json`
- `training/README.md`
- `training/requirements-gemma.txt`

Suggested generated data layout:

- `data/training_pipeline/raw/runs_manifest.json`
- `data/training_pipeline/raw/replayed_steps.jsonl`
- `data/training_pipeline/normalized/examples_full.jsonl`
- `data/training_pipeline/normalized/examples_compact.jsonl`
- `data/training_pipeline/labeled/deterministic_labels.jsonl`
- `data/training_pipeline/labeled/llm_relabeled.jsonl`
- `data/training_pipeline/scored/scored_examples.jsonl`
- `data/training_pipeline/audits/manual_audit_sample.jsonl`
- `data/training_pipeline/final/v0/train.jsonl`
- `data/training_pipeline/final/v0/val.jsonl`
- `data/training_pipeline/final/v0/test.jsonl`
- `data/training_pipeline/final/v0/holdout_recovery.jsonl`
- `data/training_pipeline/final/v0/manifest.json`
- `data/training_pipeline/reports/v0_dataset_report.md`

## 7. Pipeline Stages

### 7.1 Run discovery

CLI examples:

- `python -m trainingdatapipeline.discover_runs --root C:\path\to\runs`
- `python -m trainingdatapipeline.extract_raw_steps --root C:\path\to\runs --out data/training_pipeline/raw/replayed_steps.jsonl`
- `python -m trainingdatapipeline.build_splits --input data/training_pipeline/scored/scored_examples.jsonl --out data/training_pipeline/final/v0`

Discovery responsibilities:

- Enumerate run roots from repo-local `runs/` and optional alternate roots.
- Record which required and optional artifacts exist.
- Record basic run metadata including timestamps where derivable from run ids and metadata files.
- Fail only when mandatory extraction inputs are absent for a run.

### 7.2 Controller-state replay

This stage is required and is the main addition beyond the original draft.

Inputs:

- step payloads from `controller-log.jsonl`
- run enrichments from `seed-prompt.json`, `attempts.jsonl`, `runtime-state.json`, `run-metadata.json`, profiles and eval artifacts where available

Outputs:

- one replayed per-step state object that approximates the explorer-visible packet at that step
- one compact normalized variant for trainability experiments

Rules:

- no future leakage
- no using post-step results inside pre-step prompt state
- use stable symbolic handles for local refs where possible
- normalize paths, UUIDs, timestamps, and boilerplate that are not semantically important

### 7.3 Raw step extraction

For each step, extract:

- `run_id`
- `step`
- `phase`
- original `reasoning`
- original `actions`
- `results`
- `manager_events`
- immediate failure markers
- immediate progress markers
- prior step summary window

Immediate derived fields:

- `first_pass_valid_top_level_shape`
- `response_guard_blocked`
- `response_repair_triggered`
- `payload_shape_repair_triggered`
- `step_guard_triggered`
- `contains_run_cli`
- `contains_typed_tool`
- `contains_read_file`
- `contains_list_dir`
- `contains_write_file`
- `contains_finish`
- `hard_action_failure`
- `auto_log_present`
- `timeframe_mismatch_present`
- `manager_hook_present`

### 7.4 State normalization

Two prompt variants must be produced.

Variant A: faithful replay state

- close to the live controller-visible packet
- includes run state, branch lifecycle, mismatch status, recent attempts, behavior digest, and recent-step summaries

Variant B: compact normalized state

- keeps only fields that materially affect next action choice
- removes long boilerplate and low-signal file path chatter
- compresses handles and repeated text into structured keys

### 7.5 Deterministic labeling

Build a rule engine over normalized state plus prior results.

Rule engine output:

- `allowed_next_tools`
- `preferred_next_tools`
- `forbidden_next_tools`
- `deterministic_target_response` when the step is strongly constrained

High-confidence deterministic cases include:

- after successful `prepare_profile` with `next_recommended_action=validate_profile`
- after successful `validate_profile` with `ready_for_registration=true`
- after successful `register_profile` with `created_profile_ref` and `next_recommended_action=evaluate_candidate`
- after successful `run_parameter_sweep` with `next_recommended_action=inspect_artifact` or `compare_artifacts`
- after explicit timeframe mismatch block, forbid repeating the same requested timeframe eval on unchanged profile
- after a hard failed action followed by `step_guard`, next step should pivot or recover, not continue the blocked sequence
- after repeated-action validation risk, forbid replaying the same plan again

Deterministic labels should use controller-native fields first:

- `next_recommended_action`
- `ready_for_registration`
- `ready_to_evaluate`
- `timeframe_mismatch`
- repeated-action guard semantics
- branch lifecycle and manager overlays

### 7.6 Frontier-assisted relabeling

Use a strong teacher only on ambiguous high-value states.

Inputs to teacher:

- normalized prompt state only
- explicit instruction to return `2-4` candidate controller-valid JSON responses

Selection procedure:

1. generate constrained candidates
2. run local controller-shape validation
3. run local admissibility checks against forbidden and required tool/state rules
4. optionally short-roll candidates through cheap replay heuristics
5. keep only admissible, high-scoring candidates

Retention policy:

- support single-target export
- preserve multi-target rows when multiple actions are genuinely acceptable
- mark all teacher-produced data in provenance

### 7.7 Scoring and filtering

Each example receives:

- mechanical score `0-5`
- policy score `0-5`
- recovery score `0-5`
- outcome contribution score `0-5`
- final grade `A/B/C/D/F`

Mechanical score rubric:

- exact top-level controller shape
- valid tool names
- required fields present
- valid typed-tool argument structure
- no unresolved placeholder leakage
- no invalid path or ref usage

Policy score rubric:

- respects typed-tool preference
- no `run_cli` except allowed fallback or evidence-derived recovery justification
- aligns with current phase and score target
- uses inspect/compare discipline instead of reflexive file reads
- respects branch lifecycle and mismatch blocking

Recovery score rubric:

- pivots after failures
- does not repeat a blocked move
- resolves mismatch or validation blockers
- reduces controller friction in one step

Outcome contribution rubric:

- immediate useful progress
- useful effect over next `1-3` steps
- supports scored attempt creation, valid branching, or credible frontier progress

Strong keeps for base SFT:

- typed-tool valid responses that executed cleanly
- repaired typed-tool responses where the repaired response is strong and the intended move is still desirable
- deterministic follow-up transitions
- one-step recovery turns
- steps that increase run-owned valid state, not just file inspection churn

Strong discards from positive SFT:

- pure `run_cli` imitation targets
- repeated `read_file` and `list_dir` churn without necessity
- malformed payloads as direct positive targets
- noisy loops and repeated stalls
- examples with future leakage risk

## 8. Canonical Example Schema

One canonical JSONL schema will be used for per-step examples.

Required fields:

- `example_id`
- `run_id`
- `step`
- `source_type`
- `split_hint`
- `phase`
- `prompt_state`
- `target_response`
- `target_actions`
- `target_reasoning_short`
- `prior_action_summary`
- `tool_results_summary`
- `branch_state_summary`
- `quality_labels`
- `mechanical_labels`
- `policy_labels`
- `recovery_labels`
- `provenance`
- `rejection_reasons`

Allowed `source_type` values:

- `realrun`
- `deterministic`
- `llm_relabeled`
- `synthetic_recovery`

Important schema rules:

- `prompt_state` must be pre-step state only
- `target_response` must be fully controller-valid
- `target_actions` must be a parsed list matching `target_response`
- `target_reasoning_short` should remain short and non-chain-of-thought
- `rejection_reasons` is required even when empty for auditability

## 9. Recovery Corpus Policy

Recovery data is a first-class deliverable because wasted retries are a core pain point.

Recovery examples should be mined from:

- `response_repair` episodes in runtime trace
- `payload_shape_repair` episodes
- `response_guard` blocks
- `step_guard` after failed actions
- timeframe mismatch blocks
- typed-tool argument failures that were corrected next step
- unnecessary read/list loops followed by a valid pivot

Supported failure classes:

- `invalid_json_shape`
- `missing_required_field`
- `wrong_tool_for_state`
- `invalid_cli_family_or_subcommand`
- `profile_ref_or_path_resolution_error`
- `timeframe_repeat_block`
- `exploit_dead_violation`
- `finish_denied`
- `repeated_stall`
- `overuse_of_read_file`

v0 policy:

- prioritize recovery examples whose correct next step is typed-tool based
- recovery targets should teach the new explorer how to avoid old churn patterns
- do not preserve old `run_cli` fallback behavior as the desired steady-state policy

## 10. Balancing and Split Policy

### 10.1 Balancing buckets

The final dataset must be quota-balanced across:

- phase: `early`, `mid`, `late`, `wrap_up`
- tool family: `prepare`, `mutate`, `validate`, `register`, `evaluate`, `sweep`, `inspect`, `compare`, `recovery`
- outcome class: `progressing`, `ambiguous`, `recovery`, `dead_end`
- controller state: provisional leader, validated leader, mismatch active, reseed active, wrap-up focus active
- instrument breadth: single instrument versus small basket
- manager involvement: manager hook absent versus present

Additional balancing rule:

- hard-cap `read_file` and `list_dir` positives
- exclude normal-policy `run_cli` positives entirely

### 10.2 Splits

Required split rules:

- split by `run_id`, never by random example
- hold out entire runs
- preserve a recovery-heavy holdout
- optionally preserve a later-time holdout when dates are reliable

Suggested v0 split:

- `70%` train runs
- `15%` validation runs
- `15%` test runs
- plus `holdout_recovery`
- plus a manual audit sample across all buckets

## 11. Export Format

Two export modes:

- strict single-target SFT
- multi-target export for ablations and offline admissibility metrics

Target format:

- system: controller protocol framing
- user: replayed prompt state
- assistant: exact controller-valid JSON with short reasoning only

Training target requirements:

- no long chain-of-thought
- concise reasoning only
- exact controller JSON
- deterministic validator must fail export when a target is invalid

## 12. Training Stack

Training environment:

- Python `3.11` preferred for the training workspace
- repo runtime currently declares `requires-python >=3.10` in `pyproject.toml`
- use a separate training environment rather than mutating the main runtime dependencies

Required packages:

- `torch`
- `transformers`
- `datasets`
- `peft`
- `trl`
- `accelerate`
- `bitsandbytes`
- `sentencepiece`
- `scipy`
- `numpy`
- `pandas`
- `orjson`

Optional packages:

- `wandb`
- `tensorboard`
- `unsloth` if Gemma target compatibility is verified

Training artifact policy:

- produce adapters first
- validate adapters in direct Python inference
- merge or export only after quality is proven

## 13. Hardware Guidance

Observed local GPUs:

- `Quadro RTX 5000` with `16384 MiB`
- `RTX 3070` with `8192 MiB`

Practical implication:

- Gemma 4 E4B-it QLoRA pilot is realistic on the `16 GB` card with conservative settings
- the `8 GB` card is not the primary training target
- 26B is not a sensible first local training target on this hardware

Recommended starting settings for E4B pilot:

- 4-bit QLoRA
- sequence length around `2048` to start
- small per-device batch size
- gradient accumulation
- gradient checkpointing enabled

Fallback levers if memory is tight:

- reduce sequence length
- reduce batch size
- increase gradient accumulation
- disable evaluation frequency before disabling checkpointing

## 14. Training Curriculum

### Stage 1: mechanical contract tuning

Dataset:

- A-grade mechanical examples
- deterministic typed-tool follow-up examples
- recovery examples for shape, field, and blocked-action fixes

Success metrics:

- first-pass valid top-level JSON rate up
- repair count down
- response-guard blocks down
- `run_cli` misuse down

### Stage 2: state-conditioned next-action tuning

Dataset:

- Stage 1 plus high-confidence policy examples
- selected ambiguous relabels

Success metrics:

- better typed-tool choice
- more inspect/compare discipline
- fewer repeated stalls

### Stage 3: recovery specialization

Dataset:

- modest oversampling of targeted recovery classes

Success metrics:

- higher one-step recovery rate
- lower mismatch repeats
- lower repeated-action violations

### Stage 4: manager tune

Deferred from v0.

## 15. Evaluation Harness

### 15.1 Offline evals

Required offline metrics:

- first-pass valid controller-shape rate
- exact or admissibility match on tool and fields
- top-k admissible rate for multi-target rows
- recovery correctness by failure class
- typed-tool selection rate
- forbidden-tool violation rate

### 15.2 Online evals

Required benchmark mode:

- baseline explorer versus tuned explorer
- same controller
- same manager
- same benchmark registry of seeds and run configs
- paired runs with fixed step budgets

Required online metrics:

- first-pass valid response rate
- repair count per run
- response-guard block count per run
- hard failure count per run
- useful typed actions per run
- time to first auto-logged attempt
- time to provisional leader
- time to validated leader
- final best admissible quality score
- token burn on failed or repaired turns

Success criterion for v0:

- improved controller productivity metrics must accompany any offline gains

## 16. Deployment Decision Record

Recommended first serving mode:

- Python-native inference with base Gemma model plus LoRA adapter loaded in `transformers`

Why:

- fastest iteration
- easiest parity with training
- easiest adapter ablations
- easiest correctness debugging

Deferred deployment modes:

- merged checkpoint for simpler packaging
- GGUF or LM Studio style export for desktop runtime compatibility

LM Studio position in this project:

- not the fine-tuning environment
- possible later inference/runtime target only

## 17. Exact Answers to the Required Questions

Exact file layout of local training workspace:

- use `trainingdatapipeline/`, `training/`, and `data/training_pipeline/` as defined in Section 6

Exact Python packages to install:

- use the package list in Section 12, in a separate training environment

Whether current hardware is sufficient for E4B QLoRA:

- yes for a pilot on the `16 GB` GPU
- no claim that it is comfortable for larger Gemma targets

Whether adapters should stay separate or be merged:

- separate first
- validate in Python-native inference first
- merge or export only after the adapter is proven

Whether LM Studio is part of training:

- no
- it is downstream inference packaging only if needed

Exact recipe for extracting good examples from noisy ignored runs:

- replay controller-visible state
- score and filter steps
- exclude `run_cli` as positive policy
- keep deterministic typed-tool transitions and recovery pivots
- relabel ambiguous high-value states only after local validation

Exact rule set for deterministic labels versus LLM relabeling:

- deterministic when controller-native fields strongly constrain the next tool or response
- LLM relabeling only for valuable ambiguous states that pass local admissibility checks

Exact first benchmark suite:

- a frozen registry of representative seeds and run configs sampled across `early`, `mid`, and recovery-heavy conditions, with fixed step budgets and paired baseline-versus-tuned runs

## 18. Implementation Order

Phase 0:

- create `trainingdatapipeline/`
- define canonical example schema
- define scoring rubric
- implement run discovery

Phase 1:

- implement controller-state replay
- emit replayed raw steps
- produce corpus coverage report

Phase 2:

- implement deterministic rules engine
- emit deterministic labels
- measure deterministic coverage

Phase 3:

- implement scoring and filtering
- produce grade reports
- emit manual audit sample

Phase 4:

- implement frontier-assisted relabeling on ambiguous high-value states
- validate all teacher outputs locally

Phase 5:

- balance buckets
- split by run
- export dataset `v0`

Phase 6:

- build training environment
- build `train_lora.py`
- build offline and online eval scripts

Phase 7:

- run Gemma E4B pilot
- verify held-out metrics and controller productivity metrics

Phase 8:

- decide whether to expand dataset, iterate recovery slices, or consider a stronger local model

## 19. Manual Audit Checklist

Before training `v0`, manually inspect at least `100` examples across buckets.

Audit questions:

- does prompt state match what the explorer actually knew?
- is the target valid controller JSON?
- is the target admissible under current controller rules?
- is there future leakage?
- is boilerplate overrepresented?
- is the target too prescriptive where multiple valid actions exist?
- should the row be multi-target?
- is this row teaching typed-tool discipline or teaching old churn?

## 20. Definition of Done

The v0 spec is satisfied when:

- the local pipeline can extract replayed examples from ignored runs without guesswork
- a versioned dataset `v0` exists with splits and audit report
- a Gemma E4B-it adapter training script can run a pilot
- offline and online eval harnesses exist
- deployment notes clearly state adapter-first Python-native inference
- the write-up recommends whether to continue to a stronger local Gemma or to defer

## 21. Follow-On Work After v0

Possible next steps after a successful explorer v0:

- separate manager dataset and manager tuning
- active-learning loop from fresh tuned-model failures
- explicit raw explorer payload capture for future repair-data versions
- preference optimization on ambiguous branches after SFT stabilizes
- dataset review dashboard over extracted examples
