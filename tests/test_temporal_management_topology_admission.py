from __future__ import annotations

import copy
from pathlib import Path

import pytest

from autoresearch.temporal_management_topology_admission import (
    DEFAULT_DASHBOARD_CORE_PYTHON,
    MANAGEMENT_TOPOLOGY_AB_SCHEMA,
    MANAGEMENT_TOPOLOGY_FIXTURE_ID,
    build_serial_management_profile,
    build_shared_hub_management_genome,
    build_shared_hub_management_profile,
    run_management_topology_ab_native,
    write_management_topology_ab_report,
)


CORE_PYTHON = Path(DEFAULT_DASHBOARD_CORE_PYTHON)


def test_fixture_topologies_are_explicitly_distinct_but_share_actions_and_windows() -> None:
    serial = build_serial_management_profile()
    shared = build_shared_hub_management_profile()
    genome = build_shared_hub_management_genome()
    genome.validate()
    assert serial["graph"]["initialStateId"] == "flat"
    assert "position_hub" in {state["id"] for state in shared["graph"]["states"]}
    assert len({edge.source_id for edge in genome.edges if edge.source_id == "hub"}) == 1
    serial_actions = {
        action["kind"]
        for transition in serial["graph"]["transitions"]
        for action in transition["actions"]
    }
    shared_actions = {
        action["kind"]
        for transition in shared["graph"]["transitions"]
        for action in transition["actions"]
    }
    assert {
        "enter_next_open",
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "set_target_next_open",
        "activate_trailing_stop_next_open",
        "exit_next_open",
    } <= serial_actions == shared_actions
    assert serial["executionConfig"] == shared["executionConfig"]


@pytest.mark.skipif(not CORE_PYTHON.is_file(), reason="Dashboard core native environment is unavailable")
def test_native_serial_vs_shared_hub_management_admission_is_immutable_and_liveness_positive() -> None:
    first = run_management_topology_ab_native(dashboard_core_python=CORE_PYTHON)
    second = run_management_topology_ab_native(dashboard_core_python=CORE_PYTHON)
    assert first == second
    assert first["schemaVersion"] == MANAGEMENT_TOPOLOGY_AB_SCHEMA
    assert first["fixtureId"] == MANAGEMENT_TOPOLOGY_FIXTURE_ID
    assert first["evidence"]["marketDataUsed"] is False
    assert first["reportSha256"].startswith("sha256:")
    assert first["admission"] == {
        "onePositionOnePendingEffect": True,
        "deterministicConflictResolution": True,
        "byteExactReplayAndRestart": True,
        "noInventedLiquidation": True,
        "independentRegionsMateriallyIncreaseLiveness": True,
        "semanticContradictions": [],
    }
    serial = first["serial"]
    shared = first["sharedHub"]
    assert serial["result"]["metrics"]["totalGrossR"] == shared["result"]["metrics"]["totalGrossR"]
    assert serial["result"]["metrics"]["totalNetR"] == shared["result"]["metrics"]["totalNetR"]
    assert serial["onePositionOneEffect"] and shared["onePositionOneEffect"]
    assert serial["splitRestartExact"] and shared["splitRestartExact"]
    assert shared["counts"]["appliedActions"] > serial["counts"]["appliedActions"]
    assert shared["counts"]["authoredActionDeadTransitions"] < serial["counts"]["authoredActionDeadTransitions"]
    assert shared["counts"]["authoredActionPriorityShadowedTransitions"] >= 1
    assert shared["attribution"]["collectionStatus"] == "complete"


@pytest.mark.skipif(not CORE_PYTHON.is_file(), reason="Dashboard core native environment is unavailable")
def test_immutable_report_hash_detects_any_posthoc_mutation() -> None:
    report = run_management_topology_ab_native(dashboard_core_python=CORE_PYTHON)
    tampered = copy.deepcopy(report)
    tampered["sharedHub"]["counts"]["appliedActions"] += 1
    from autoresearch.temporal_search import canonical_sha256

    stored = tampered.pop("reportSha256")
    assert canonical_sha256(tampered) != stored


@pytest.mark.skipif(not CORE_PYTHON.is_file(), reason="Dashboard core native environment is unavailable")
def test_versioned_report_writer_is_idempotent_and_refuses_overwrite(tmp_path: Path) -> None:
    target = tmp_path / "management-topology-ab.json"
    written = write_management_topology_ab_report(target, dashboard_core_python=CORE_PYTHON)
    assert (json_load := __import__("json").loads(target.read_text(encoding="utf-8")))
    assert json_load == written
    assert write_management_topology_ab_report(target, dashboard_core_python=CORE_PYTHON) == written
    target.write_text("{}\n", encoding="utf-8")
    with pytest.raises(FileExistsError, match="immutable content"):
        write_management_topology_ab_report(target, dashboard_core_python=CORE_PYTHON)
