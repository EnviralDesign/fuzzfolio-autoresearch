"""Drift and determinism checks for the source-only substrate atlas."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate_evolutionary_substrate_atlas.py"
SPEC = importlib.util.spec_from_file_location("substrate_atlas", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_primary_rust_registry_tripwires_match_current_source() -> None:
    """A vocabulary change must require a mapping review, rather than silently drift."""

    assert len(MODULE.grammar_fragments((ROOT / MODULE.AR_FILES["grammar"]).read_text())) == 23
    assert len(MODULE.grammar_ports((ROOT / MODULE.AR_FILES["grammar"]).read_text())) == 7
    assert len(MODULE.topology_operations((ROOT / MODULE.AR_FILES["topology"]).read_text())) == 14
    assert len(MODULE.operator_families((ROOT / MODULE.AR_FILES["operators"]).read_text())) == 6


def test_atlas_generation_is_deterministic_when_fuzzfolio_source_is_supplied(
    tmp_path: Path,
) -> None:
    fuzzfolio_root = os.environ.get("FUZZFOLIO_STAGE45A_SOURCE")
    if not fuzzfolio_root:
        pytest.skip("set FUZZFOLIO_STAGE45A_SOURCE to the pinned read-only FuzzFolio worktree")
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert MODULE.main_for_test(ROOT, Path(fuzzfolio_root), first) == 0
    assert MODULE.main_for_test(ROOT, Path(fuzzfolio_root), second) == 0
    for name in (
        "source-authority-map.json",
        "capability-ledger.json",
        "gap-matrix.json",
        "generated-summary.md",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    ledger = json.loads((first / "capability-ledger.json").read_text())
    assert ledger["sourceCounts"]["ledgerCapabilities"] == 175
    assert ledger["sourceCounts"]["catalogIndicators"] == 88
    assert all(
        capability["historicalEvidence"]["v37"] == "unavailable_no_run_corpus_read"
        for capability in ledger["capabilities"]
    )
