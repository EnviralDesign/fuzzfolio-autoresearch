from __future__ import annotations

import json
from pathlib import Path

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_rust_canonical_topology_package_v1 import OUTPUT
from autoresearch.temporal_qd_rust_dashboard_differential_v1 import MATRIX_PARENTS, _classify


def _load(name: str) -> dict:
    return json.loads((OUTPUT / name).read_text(encoding="utf-8"))


def _assert_self_hash(value: dict, field: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field)
    assert canonical_sha256(unsigned) == stored


def test_dashboard_default_materialization_is_classified_for_semantic_review() -> None:
    assert (
        _classify(
            "/executionConfig/managementLibrary/plans/0/holdPolicy/onBreach",
            None,
            "exit_next_open",
            context="program",
        )
        == "normalization_difference_with_possible_semantic_effect"
    )


def test_differential_corpus_is_frozen_and_contains_all_matrix_parents() -> None:
    sweep = _load("v38-cross-compiler-sweep-v1.json")
    selected = set(sweep["selectionRule"]["selectedCandidateIds"])
    assert sweep["selectionRule"]["ruleFrozenBeforeOutcomes"] is True
    assert set(MATRIX_PARENTS) <= selected
    assert sweep["coverage"]["allFiveMatrixParentsIncluded"] is True


def test_native_topology_package_has_exact_blocks_candidates_and_task_matrix() -> None:
    blocks = _load("topology-native-blocks-v1.json")
    ledger = _load("candidate-ledger-v1.json")
    tasks = _load("inspected-task-index-v1.json")
    confirmation = _load("untouched-confirmation-preregistration-v1.json")
    go_nogo = _load("topology-launch-go-nogo-v1.json")

    _assert_self_hash(blocks, "blockSetSha256")
    _assert_self_hash(ledger, "ledgerSha256")
    _assert_self_hash(tasks, "taskIndexSha256")
    _assert_self_hash(confirmation, "preregistrationSha256")
    _assert_self_hash(go_nogo, "goNogoSha256")

    assert [(block["parentCandidateId"], block["side"]) for block in blocks["blocks"]] == [
        ("qd_ed27f99ba0a8dfd7c76c69687efb", "short"),
        ("qd_69e5a3407ab21e82d787eb48c8d5", "short"),
        ("qd_001958c8b3288892a458207c9b76", "long"),
    ]
    assert all([candidate["arm"] for candidate in block["candidates"]] == ["P", "T", "E", "TE"] for block in blocks["blocks"])
    assert ledger["candidateCount"] == len({row["candidateId"] for row in ledger["rows"]}) == 12
    assert tasks["taskCount"] == len({(row["candidateId"], row["windowId"]) for row in tasks["tasks"]}) == 144
    assert {row["panelId"] for row in tasks["tasks"]} == {"panel-1", "panel-2", "panel-3"}
    assert confirmation["projectedTaskCount"] == len(confirmation["projectedTasks"]) == 48
    assert confirmation["noOverlapWithInspectedPanels"] is True
    assert confirmation["executionDeferred"] is True
    assert go_nogo["readyForTopologyCaseStudyLaunch"] is True
    assert go_nogo["gates"]["noTaskDispatched"] is True
    assert go_nogo["gates"]["noMarketEvaluation"] is True
