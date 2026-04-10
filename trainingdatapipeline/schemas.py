"""Shared schema constants for the training data pipeline."""

from __future__ import annotations

REQUIRED_RUN_ARTIFACTS = (
    "controller-log.jsonl",
)

OPTIONAL_RUN_ARTIFACTS = (
    "runtime-state.json",
    "runtime-trace.jsonl",
    "attempts.jsonl",
    "run-metadata.json",
    "seed-prompt.json",
    "checkpoint-summary.txt",
)

OPTIONAL_RUN_DIRS = (
    "profiles",
    "evals",
    "notes",
)

SOURCE_TYPES = (
    "realrun",
    "deterministic",
    "llm_relabeled",
    "synthetic_recovery",
)
