# Manager / Controller / Explorer ownership

This document describes the **manager-authoritative** branch model: the explorer researches; the manager adjudicates overlay and lifecycle policy; the controller runs mechanics.

## Explorer (LLM + typed tools)

- **Researches**: profile prep, mutation, sweeps, evals, artifact inspection.
- **Surface**: [`autoresearch/typed_tools.py`](../autoresearch/typed_tools.py) — `PRIMARY_TYPED_TOOLS`, `ALL_CONTROLLER_TOOLS`, `normalized_tool_envelope`.
- **Prompts**: explorer system protocol and run-state packet are built in [`autoresearch/controller.py`](../autoresearch/controller.py) (`SYSTEM_PROTOCOL`, `_run_state_prompt`).

## Controller (`ResearchController`)

- **Orchestrates** the run loop, provider calls, tool dispatch, and persistence.
- **Deterministic mechanics**:
  - Tool/action validation and execution.
  - Paths, ledger (`attempts.jsonl`), scoring extraction, artifact resolution.
  - After each scored eval: [`autoresearch/branch_mechanics.py`](../autoresearch/branch_mechanics.py) records facts, builds validation evidence, updates `last_scored_validation_digest`, then syncs budget mode and overlay consistency (no score-based leader recomputation).
  - Runtime-state / trace (`runtime-state.json`, `_build_branch_runtime_snapshot`).
  - Finish / step guards, compaction, horizon injection (where not policy).

## Manager (LLM, event-driven)

- **Adjudicates** branch meaning: leaders (provisional/validated), suppression, reseed windows, explicit budget overrides, notes.
- **Does not** execute research tools or return explorer actions.
- **Control plane**: structured `ManagerAction` list applied by [`autoresearch/manager_actions.py`](../autoresearch/manager_actions.py).
- **Packets**: [`autoresearch/manager_packet.py`](../autoresearch/manager_packet.py).

## Branch mechanics (non-LLM)

- [`autoresearch/branch_mechanics.py`](../autoresearch/branch_mechanics.py) — `refresh_family_after_scored_eval`, `sync_branch_budget_mode`, `apply_overlay_provisional_leadership`, `mark_family_collapsed` (used when the manager requests `suppress_family`).

## Evidence and branch state models

- **Per-family state**: [`autoresearch/branch_lifecycle.py`](../autoresearch/branch_lifecycle.py) — `FamilyBranchState`, `BranchRunOverlay`.
- **Validation / coverage assembly**: [`autoresearch/validation_outcome.py`](../autoresearch/validation_outcome.py) — `ValidationOutcome`, `classify_coverage`, `build_validation_outcome`.
- **Policy knobs**: [`autoresearch/config.py`](../autoresearch/config.py) — `ResearchConfig`, `ManagerConfig`.

## Typed tests in repo

- Validation assembly: [`tests/test_validation_outcome.py`](../tests/test_validation_outcome.py).
- Manager actions: [`tests/test_manager_actions.py`](../tests/test_manager_actions.py).
