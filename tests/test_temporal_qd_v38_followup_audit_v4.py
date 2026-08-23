from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_resource_suboperation_inventory import (
    INVENTORY_SCHEMA,
    validate_resource_suboperation_candidate_inventory,
)
from autoresearch.temporal_qd_resource_suboperation_launch import LAUNCH_SCHEMA
from autoresearch.temporal_qd_topology_coadaptation_v4 import (
    ARM_E,
    ARM_TE,
    BLOCK_CLASS_COMPLETE,
    BLOCK_CLASS_INCOMPLETE,
    attach_topology_coadaptation_matrix_v4,
    coadaptation_interaction,
    promising_coadaptation_observation,
    topology_coadaptation_v4_from_config,
    validate_topology_coadaptation_matrix_v4,
)
from autoresearch.temporal_qd_v38_followup_audit_v4 import (
    FOCUS_CHILD_ID,
    classify_gross_versus_cost,
)


V4_SPEC = Path("research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v4.json")
V4_INVENTORY = Path("research/temporal-qd/v38-followup/resource-suboperation-candidate-inventory-v1.json")
V4_EVENT = Path("research/temporal-qd/v38-followup/v38-directional-event-insert-forensic-v4.json")
V4_ARCHIVE = Path("research/temporal-qd/v38-followup/v38-cumulative-event-child-archive-forensic-v4.json")


def test_gross_versus_cost_classes_use_canonical_metric_identity() -> None:
    def case(*, trades: int, child_gross: float, parent_gross: float, child_net: float, parent_net: float) -> dict[str, object]:
        return {
            "relative": {"comparable": True, "beatParent": True},
            "metrics": {
                "tradeCount": trades,
                "grossNoCostNetR": child_gross,
                "cumulativeConservativeNetR": child_net,
            },
            "parentMetrics": {
                "grossNoCostNetR": parent_gross,
                "cumulativeConservativeNetR": parent_net,
            },
        }

    assert classify_gross_versus_cost(case(trades=0, child_gross=0, parent_gross=-1, child_net=0, parent_net=-1)) == "zero_trade_suppression"
    assert classify_gross_versus_cost(case(trades=8, child_gross=2, parent_gross=1, child_net=1.5, parent_net=0.5)) == "gross_improving_and_net_improving"
    assert classify_gross_versus_cost(case(trades=8, child_gross=-1, parent_gross=0, child_net=0.2, parent_net=0.0)) == "gross_worsening_net_improving_via_cost"
    assert classify_gross_versus_cost(case(trades=8, child_gross=1, parent_gross=1, child_net=0.4, parent_net=0.2)) == "gross_and_net_tie_or_pure_suppression"


def test_interaction_is_te_minus_t_minus_e_plus_p_without_pnl_margin() -> None:
    assert coadaptation_interaction(parent=-8.0, topology=-9.0, event=4.0, combined=3.0) == 0.0
    observation = promising_coadaptation_observation(
        parent_net=-8.0,
        topology_net=-9.0,
        event_net=4.0,
        combined_net=3.0,
        parent_worst=-3.0,
        topology_worst=-4.0,
        event_worst=-1.0,
        combined_worst=-1.5,
        metric_greater=lambda left, right: left > right,
        metric_not_worse=lambda left, right: left >= right,
    )
    assert observation["promising"] is False
    assert observation["teNetGreaterThanT"] is True
    assert observation["teNetGreaterThanE"] is False
    assert observation["parentBeatIsNotSufficient"] is True


def test_resource_inventory_cannot_masquerade_as_a_balanced_launch_matrix() -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="cannot masquerade"):
        validate_resource_suboperation_candidate_inventory({"schemaVersion": LAUNCH_SCHEMA})
    with pytest.raises(TemporalDiscoveryContractError, match="cannot masquerade"):
        validate_resource_suboperation_candidate_inventory(
            {"schemaVersion": "temporal_qd_resource_suboperation_matrix_v1"}
        )


def test_emitted_v4_inventory_is_not_a_launch_matrix() -> None:
    if not V4_INVENTORY.is_file():
        pytest.skip("v4 inventory is not on disk")
    payload = json.loads(V4_INVENTORY.read_text(encoding="utf-8"))
    validated = validate_resource_suboperation_candidate_inventory(payload)
    assert validated["schemaVersion"] == INVENTORY_SCHEMA
    assert validated["isBalancedLaunchMatrix"] is False
    assert validated["boundedTaskProjection"]["doNotLaunch"] is True
    assert validated["boundedTaskProjection"]["windowCount"] == 3
    assert validated["boundedTaskProjection"]["panelCount"] == 3
    assert validated["cloneSlots"]
    allowed = {"v38_accepted_recovered_authoritative_plan", "authoritative_enumerate_plans_applicable"}
    for slot in validated["slots"]:
        if slot["eligibility"] == "eligible":
            assert slot["source"] in allowed
            assert str(slot["planSha256"]).startswith("sha256:")
            assert str(slot["childProgramSha256"]).startswith("sha256:")
            assert slot["construction"]["kind"] == slot["lane"]


def test_emitted_v4_event_and_archive_forensics_are_self_hashed() -> None:
    if not V4_EVENT.is_file() or not V4_ARCHIVE.is_file():
        pytest.skip("v4 forensic reports are not on disk")
    event = json.loads(V4_EVENT.read_text(encoding="utf-8"))
    archive = json.loads(V4_ARCHIVE.read_text(encoding="utf-8"))
    event_body = {key: value for key, value in event.items() if key != "reportSha256"}
    archive_body = {key: value for key, value in archive.items() if key != "reportSha256"}
    assert event["reportSha256"] == canonical_sha256(event_body)
    assert archive["reportSha256"] == canonical_sha256(archive_body)
    assert event["populationOrIsNotSameChildPersistence"] is True
    assert event["shortSideReversesAcrossPanels"] is True
    partition = event["grossVersusCostPartitionOfPanel3ParentBeats"]
    assert partition["parentBeats"] == 16
    assert partition["gross_improving_and_net_improving"] == 10
    assert partition["gross_worsening_net_improving_via_cost"] == 3
    assert partition["gross_and_net_tie_or_pure_suppression"] == 1
    assert partition["zero_trade_suppression"] == 2
    short = event["sideByPanel"]["short"]
    assert short["panel-1"]["beats"] == 8
    assert short["panel-2"]["losses"] == 8
    assert short["panel-3"]["beats"] == 16
    breadth = event["breadth"]
    assert breadth["acceptedGenotypes"] == 25
    assert breadth["distinctResolvedPrograms"] == 25
    assert breadth["distinctRealizedPhenotypes"] == 17
    focus = archive["focusRecord"]
    assert focus["candidateId"] == FOCUS_CHILD_ID
    assert focus["reasonCode"] == "failed_cumulative_support_direction_or_economics"
    assert archive["notAdmittedToFinalArchiveIsNotATerminalExplanation"] is True
    assert archive["finalArchiveEventChildCount"] == 0
    assert archive["backfilledEventChildCount"] == 11


def test_emitted_v4_topology_contract_round_trips_and_complete_blocks_are_factorial() -> None:
    if not V4_SPEC.is_file():
        pytest.skip("v4 topology spec is not on disk")
    payload = json.loads(V4_SPEC.read_text(encoding="utf-8"))
    validated = validate_topology_coadaptation_matrix_v4(payload)
    assert validated["contractSha256"] == payload["contractSha256"]
    assert canonical_json(validated) == canonical_json(payload)
    complete = [block for block in validated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE]
    incomplete = [block for block in validated["blocks"] if block["classification"] == BLOCK_CLASS_INCOMPLETE]
    assert len(complete) == 3
    assert incomplete
    assert all(block["excludedFromPrimaryCoadaptationCalculation"] is False for block in complete)
    assert all(block["excludedFromPrimaryCoadaptationCalculation"] is True for block in incomplete)
    slots = {slot["slotId"]: slot for slot in validated["slots"]}
    plans = {plan["planId"]: plan for plan in validated["topologyPlans"]}
    for block in complete:
        for arm in ("exact_parent_clone", "topology_only_child", "event_only_control", "topology_then_topology_local_event"):
            slot = slots[block["armSlotIds"][arm]]
            assert slot["eligibility"] == "eligible"
            assert slot["parentCandidateId"] == block["parentCandidateId"]
            assert slot["side"] == block["side"]
        event_only = slots[block["armSlotIds"][ARM_E]]
        combined = slots[block["armSlotIds"][ARM_TE]]
        assert event_only["eventPrimitiveId"] == combined["eventPrimitiveId"] == block["eventPrimitiveId"]
        plan = plans[block["topologyPlanId"]]
        assert combined["settlingNodeId"] == plan["addedSetupNodeId"]
        assert combined["settlingNodeId"] in plan["topologySemanticDelta"]["addedNodes"]
        receipts = [item for item in validated["materializationReceipts"] if item["blockId"] == block["blockId"]]
        assert {item["arm"] for item in receipts} == {
            "exact_parent_clone",
            "topology_only_child",
            "event_only_control",
            "topology_then_topology_local_event",
        }
        te = next(item for item in receipts if item["arm"] == ARM_TE)
        assert te["eventAttachesToAddedSetupNode"] is True
        assert te["productionArchiveWrite"] is False
        assert str(te["genomeSha256"]).startswith("sha256:")
    assert validated["designScope"]["familyLevelInferenceForbidden"] is True
    assert validated["designScope"]["doNotLaunch"] is True
    assert topology_coadaptation_v4_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    attached = attach_topology_coadaptation_matrix_v4(
        {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"},
        payload,
    )
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    assert attached["configSha256"] != "x"


def test_incomplete_blocks_and_mismatched_te_targets_are_rejected() -> None:
    if not V4_SPEC.is_file():
        pytest.skip("v4 topology spec is not on disk")
    payload = json.loads(V4_SPEC.read_text(encoding="utf-8"))
    mutated = json.loads(canonical_json(payload))
    complete = next(block for block in mutated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    complete["classification"] = BLOCK_CLASS_INCOMPLETE
    complete["excludedFromPrimaryCoadaptationCalculation"] = False
    complete["incompletenessReason"] = "forced"
    with pytest.raises(TemporalDiscoveryContractError, match="incomplete blocks cannot enter qualification"):
        validate_topology_coadaptation_matrix_v4(mutated)

    mutated = json.loads(canonical_json(payload))
    complete = next(block for block in mutated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    te_id = complete["armSlotIds"][ARM_TE]
    for slot in mutated["slots"]:
        if slot["slotId"] == te_id:
            slot["settlingNodeId"] = "setup"
    with pytest.raises(TemporalDiscoveryContractError, match="added setup node"):
        validate_topology_coadaptation_matrix_v4(mutated)

    mutated = json.loads(canonical_json(payload))
    mutated["topologyPlans"][0]["topologyPlan"]["operation"] = "insert_exit_region"
    with pytest.raises(TemporalDiscoveryContractError, match="insert_exit_region"):
        validate_topology_coadaptation_matrix_v4(mutated)


def test_emitted_v4_reports_do_not_include_run_artifacts() -> None:
    repo = Path(".")
    tracked = {path.replace("\\", "/") for path in _git_tracked_files(repo)}
    assert not any(path == "runs" or path.startswith("runs/") for path in tracked)


def _git_tracked_files(repo: Path) -> list[str]:
    import subprocess

    result = subprocess.run(
        ["git", "ls-files"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]
