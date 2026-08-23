from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_resource_suboperation_launch import (
    validate_resource_suboperation_launch_manifest,
)
from autoresearch.temporal_qd_topology_coadaptation_v3 import (
    added_setup_node_id,
    attach_topology_coadaptation_matrix_v3,
    build_topology_coadaptation_matrix_v3,
    topology_coadaptation_v3_from_config,
    topology_plan_sha256,
    topology_semantic_delta_identity,
    validate_topology_coadaptation_matrix_v3,
)
from autoresearch.temporal_qd_v38_followup_audit_v3 import (
    build_event_insert_forensic_v3,
    build_multipanel_report_v3,
    exclusive_event_outcome,
    phenotype_identity,
)


def _sha(byte: str) -> str:
    return "sha256:" + (byte * 32)


def _plan(source: str) -> dict[str, object]:
    return {
        "schemaVersion": "evolvable_module_topology_plan_v1",
        "operatorSchema": "evolvable_module_topology_operator_v1",
        "operation": "insert_setup",
        "sourceGenomeSha256": source,
        "arguments": {"edgeId": "start_setup", "guard": {"kind": "always"}, "kind": "context"},
    }


def _coadaptation_contract() -> dict[str, object]:
    short = _sha("bb")
    plan = _plan(short)
    added = added_setup_node_id(plan)
    return build_topology_coadaptation_matrix_v3(
        parents=[
            {
                "candidateId": "parent_a",
                "role": "archive",
                "longProgramSha256": _sha("aa"),
                "shortProgramSha256": short,
            }
        ],
        rotating_evidence_sha256=_sha("cc"),
        topology_plans=[
            {
                "planId": "insert_setup|parent_a|short",
                "parentCandidateId": "parent_a",
                "side": "short",
                "topologyPlan": plan,
                "planSha256": topology_plan_sha256(plan),
                "addedSetupNodeId": added,
                "applicability": "source_genome_matches_parent_side_program",
                "topologySemanticDeltaIdentity": topology_semantic_delta_identity(plan),
            }
        ],
        event_primitives=[
            {
                "primitiveId": "event|parent_a|short|TEST",
                "parentCandidateId": "parent_a",
                "side": "short",
                "indicatorId": "TEST",
                "contract": {"kind": "raw_event"},
                "originalNodeId": "setup",
                "originalNodeZone": "setup",
                "source": "v38_recovered_directional_event_insert",
            }
        ],
        slots=[
            {
                "slotId": "clone|parent_a",
                "arm": "exact_parent_clone",
                "parentCandidateId": "parent_a",
                "side": None,
                "eligibility": "eligible",
                "topologyPlanId": None,
                "eventPrimitiveId": None,
                "settlingNodeId": None,
                "ineligibilityReason": None,
            },
            {
                "slotId": "topology_only|parent_a|short",
                "arm": "topology_only_child",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": "insert_setup|parent_a|short",
                "eventPrimitiveId": None,
                "settlingNodeId": None,
                "ineligibilityReason": None,
            },
            {
                "slotId": "event_only|parent_a|short",
                "arm": "event_only_control",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": None,
                "eventPrimitiveId": "event|parent_a|short|TEST",
                "settlingNodeId": "setup",
                "ineligibilityReason": None,
            },
            {
                "slotId": "topology_then_event|parent_a|short",
                "arm": "topology_then_topology_local_event",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": "insert_setup|parent_a|short",
                "eventPrimitiveId": "event|parent_a|short|TEST",
                "settlingNodeId": added,
                "ineligibilityReason": None,
            },
        ],
    )


def _event_slot(
    *,
    candidate_id: str,
    parent_id: str,
    side: str,
    zone: str,
    beat: bool,
    lost: bool,
    tie: bool,
    trades: int,
    cost: float,
    parent_trades: int,
    parent_cost: float,
    net: float,
    parent_net: float,
    support: bool = False,
    direction: bool = False,
    quality: bool = False,
    archive: bool = False,
    panel1_available: bool = False,
    panel1_beat: bool = False,
    panel1_abs: bool = False,
    panel2_available: bool = False,
    panel2_beat: bool = False,
    panel2_abs: bool = False,
    resolved: str = "prog-a",
) -> dict[str, object]:
    metrics = {
        "tradeCount": trades,
        "costDragR": cost,
        "cumulativeConservativeNetR": net,
        "worstWindowConservativeNetR": -1.0,
        "activeWindowFraction": 1.0,
        "combinedSupportPass": support,
        "directionEligible": direction,
        "currentPanelQualityLike": quality,
        "resolvedProgramSha256": resolved,
        "closeReasonFractions": {"stop": 1.0},
    }
    return {
        "candidateId": candidate_id,
        "parentCandidateId": parent_id,
        "parentRole": "archive",
        "disposition": "accepted",
        "constructionKind": "directional_event_insert",
        "metrics": metrics,
        "relative": {
            "comparable": True,
            "beatParent": beat,
            "lostToParent": lost,
            "fullEconomicPhenotypeTie": tie,
            "riskQualifiedBeat": beat and not lost,
            "absolutePositive": net > 0,
            "deltaCumulativeConservativeNetR": net - parent_net,
            "deltaWorstWindowConservativeNetR": 0.0,
        },
        "finalArchiveMember": archive,
        "archiveAdmissionOrEvictionReason": "cumulative_robust_quality" if archive else "not_admitted_to_final_archive",
        "plan": {
            "construction": {
                "kind": "directional_event_insert",
                "nodeId": "setup" if zone == "setup" else "entry",
                "indicatorId": "IND_" + candidate_id[-1],
                "contract": {"kind": "raw_event"},
            },
            "planSha256": _sha("11"),
        },
        "panelOutcomes": {
            "panel-1": {
                "available": panel1_available,
                "metrics": metrics if panel1_available else None,
                "relative": {
                    "beatParent": panel1_beat,
                    "absolutePositive": panel1_abs,
                    "riskQualifiedBeat": panel1_beat,
                    "lostToParent": False,
                    "fullEconomicPhenotypeTie": False,
                    "deltaCumulativeConservativeNetR": 1.0 if panel1_beat else 0.0,
                    "deltaWorstWindowConservativeNetR": 0.0,
                    "comparable": panel1_available,
                },
            },
            "panel-2": {
                "available": panel2_available,
                "metrics": metrics if panel2_available else None,
                "relative": {
                    "beatParent": panel2_beat,
                    "absolutePositive": panel2_abs,
                    "riskQualifiedBeat": panel2_beat,
                    "lostToParent": False,
                    "fullEconomicPhenotypeTie": False,
                    "deltaCumulativeConservativeNetR": 1.0 if panel2_beat else 0.0,
                    "deltaWorstWindowConservativeNetR": 0.0,
                    "comparable": panel2_available,
                },
            },
        },
    }


def test_qd19_unavailable_independent_panels_cannot_be_called_inert() -> None:
    qd19 = "qd_19e9a2130a8f91feea60349066ca"
    slot = _event_slot(
        candidate_id="child_19",
        parent_id=qd19,
        side="long",
        zone="entry",
        beat=False,
        lost=False,
        tie=True,
        trades=43,
        cost=1.0,
        parent_trades=43,
        parent_cost=1.0,
        net=-2.0,
        parent_net=-2.0,
    )
    parent_material = {
        qd19: {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("a2"),
            "longProgram": {"nodes": [{"id": "entry", "zone": "entry"}]},
        },
        "child_19": {
            "longProgramSha256": _sha("b1"),
            "shortProgramSha256": _sha("a2"),
            "longProgram": {"nodes": [{"id": "entry", "zone": "entry"}]},
        },
    }
    report = build_multipanel_report_v3(
        [slot],
        evaluated={"panel-1": {}, "panel-2": {}, "panel-3": {}},
        parent_material=parent_material,
    )
    assert report["qd19"]["panel3EventInsertionsEconomicallyTied"] is True
    assert report["qd19"]["independentPanelBehavior"] == "unobserved_not_backfilled"
    assert report["qd19e9GenuinelyInertAcrossPanels"] is None


def test_population_level_or_is_not_same_child_persistence() -> None:
    parent_a = "qd_69e5a3407ab21e82d787eb48c8d5"
    parent_b = "qd_ed27f99ba0a8dfd7c76c69687efb"
    child_a = _event_slot(
        candidate_id="child_a",
        parent_id=parent_a,
        side="short",
        zone="setup",
        beat=True,
        lost=False,
        tie=False,
        trades=5,
        cost=0.2,
        parent_trades=20,
        parent_cost=1.0,
        net=1.0,
        parent_net=-2.0,
        panel1_available=True,
        panel1_abs=True,
        panel1_beat=False,
        panel2_available=True,
        panel2_abs=True,
        panel2_beat=False,
    )
    child_b = _event_slot(
        candidate_id="child_b",
        parent_id=parent_b,
        side="short",
        zone="setup",
        beat=True,
        lost=False,
        tie=False,
        trades=4,
        cost=0.1,
        parent_trades=20,
        parent_cost=1.0,
        net=1.0,
        parent_net=-2.0,
        panel1_available=True,
        panel1_beat=True,
        panel1_abs=False,
        panel2_available=False,
    )
    parent_material = {
        parent_a: {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("a2"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        parent_b: {
            "longProgramSha256": _sha("b1"),
            "shortProgramSha256": _sha("b2"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        "child_a": {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("c1"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        "child_b": {
            "longProgramSha256": _sha("b1"),
            "shortProgramSha256": _sha("c2"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
    }
    report = build_multipanel_report_v3(
        [child_a, child_b],
        evaluated={"panel-1": {}, "panel-2": {}, "panel-3": {}},
        parent_material=parent_material,
    )
    assert report["populationOrIsNotSameChildPersistence"] is True
    assert report["sameChildParentSuperiorOnBothPanel1AndPanel2"] == []
    assert report["positiveTailPersistedAroundBothQd69e5AndQdEd27"] is None


def test_event_beat_loss_activity_cost_table_is_deterministic() -> None:
    parent_id = "qd_ed27f99ba0a8dfd7c76c69687efb"
    beat = _event_slot(
        candidate_id="beat",
        parent_id=parent_id,
        side="short",
        zone="setup",
        beat=True,
        lost=False,
        tie=False,
        trades=0,
        cost=0.0,
        parent_trades=10,
        parent_cost=1.0,
        net=1.0,
        parent_net=-2.0,
    )
    loss = _event_slot(
        candidate_id="loss",
        parent_id=parent_id,
        side="long",
        zone="entry",
        beat=False,
        lost=True,
        tie=False,
        trades=20,
        cost=2.0,
        parent_trades=10,
        parent_cost=1.0,
        net=-4.0,
        parent_net=-2.0,
    )
    tie = _event_slot(
        candidate_id="tie",
        parent_id=parent_id,
        side="long",
        zone="entry",
        beat=False,
        lost=False,
        tie=True,
        trades=10,
        cost=1.0,
        parent_trades=10,
        parent_cost=1.0,
        net=-2.0,
        parent_net=-2.0,
    )
    parent_material = {
        parent_id: {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("a2"),
            "longProgram": {"nodes": [{"id": "entry", "zone": "entry"}]},
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        "beat": {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("c1"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        "loss": {
            "longProgramSha256": _sha("c2"),
            "shortProgramSha256": _sha("a2"),
            "longProgram": {"nodes": [{"id": "entry", "zone": "entry"}]},
        },
        "tie": {
            "longProgramSha256": _sha("c3"),
            "shortProgramSha256": _sha("a2"),
            "longProgram": {"nodes": [{"id": "entry", "zone": "entry"}]},
        },
    }
    baselines = {
        parent_id: {
            "tradeCount": 10,
            "costDragR": 1.0,
            "cumulativeConservativeNetR": -2.0,
            "worstWindowConservativeNetR": -1.0,
            "role": "archive",
        }
    }
    first = build_event_insert_forensic_v3([beat, loss, tie], parent_material, baselines, {})
    second = build_event_insert_forensic_v3([beat, loss, tie], parent_material, baselines, {})
    assert first["reportSha256"] == second["reportSha256"]
    activity = first["activityCostMechanism"]
    assert activity["everyBeatTradeCountDeltaNegative"] is True
    assert activity["everyBeatCostDragDeltaNegative"] is True
    assert activity["everyLossTradeCountDeltaPositive"] is True
    assert activity["everyLossCostDragDeltaPositive"] is True
    assert activity["everyTieTradeCountDeltaZero"] is True
    assert activity["everyTieCostDragDeltaZero"] is True
    assert first["bySide"]["short"]["parentBeats"] == 1
    assert first["bySide"]["long"]["parentLosses"] == 1
    assert first["usefulOutcomesAreNotSideBalanced"] is True
    beat_case = next(case for case in first["cases"] if case["candidateId"] == "beat")
    assert exclusive_event_outcome(beat_case) == "inert_no_trade_suppression"


def test_phenotype_dedup_is_deterministic() -> None:
    metrics = {
        "tradeCount": 5,
        "cumulativeConservativeNetR": 1.25,
        "worstWindowConservativeNetR": -0.5,
        "costDragR": 0.2,
        "activeWindowFraction": 1.0,
        "directionEligible": False,
        "closeReasonFractions": {"stop": 1.0},
    }
    assert phenotype_identity(metrics) == phenotype_identity(dict(metrics))
    other = dict(metrics)
    other["tradeCount"] = 6
    assert phenotype_identity(metrics) != phenotype_identity(other)


def test_exact_panel_metrics_and_archive_reasons_are_emitted() -> None:
    parent_id = "qd_69e5a3407ab21e82d787eb48c8d5"
    slot = _event_slot(
        candidate_id="child_panel",
        parent_id=parent_id,
        side="short",
        zone="setup",
        beat=True,
        lost=False,
        tie=False,
        trades=5,
        cost=0.2,
        parent_trades=20,
        parent_cost=1.0,
        net=1.0,
        parent_net=-2.0,
        panel1_available=True,
        panel1_abs=True,
        panel2_available=True,
        panel2_abs=True,
    )
    parent_material = {
        parent_id: {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("a2"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
        "child_panel": {
            "longProgramSha256": _sha("a1"),
            "shortProgramSha256": _sha("c1"),
            "shortProgram": {"nodes": [{"id": "setup", "zone": "setup"}]},
        },
    }
    report = build_multipanel_report_v3(
        [slot],
        evaluated={"panel-1": {}, "panel-2": {}, "panel-3": {}},
        parent_material=parent_material,
    )
    child = report["children"][0]
    assert child["archiveAdmissionOrEvictionReason"] == "not_admitted_to_final_archive"
    assert child["panels"]["panel-3"]["tradeCount"] == 5
    assert child["panels"]["panel-3"]["costDragR"] == 0.2
    assert child["panels"]["panel-1"]["available"] is True
    assert "childCumulativeConservativeNetR" in child["panels"]["panel-1"]


def test_topology_plans_are_parent_bound_and_reject_another_parent() -> None:
    contract = _coadaptation_contract()
    mutated = json.loads(canonical_json(contract))
    mutated["topologyPlans"][0]["topologyPlan"]["sourceGenomeSha256"] = _sha("dd")
    mutated["topologyPlans"][0]["planSha256"] = topology_plan_sha256(mutated["topologyPlans"][0]["topologyPlan"])
    mutated["topologyPlans"][0]["addedSetupNodeId"] = added_setup_node_id(mutated["topologyPlans"][0]["topologyPlan"])
    mutated["topologyPlans"][0]["topologySemanticDeltaIdentity"] = topology_semantic_delta_identity(
        mutated["topologyPlans"][0]["topologyPlan"]
    )
    with pytest.raises(TemporalDiscoveryContractError, match="stale topology plan"):
        validate_topology_coadaptation_matrix_v3(mutated)


def test_topology_event_slot_targets_added_setup_node() -> None:
    contract = _coadaptation_contract()
    added = contract["slots"][3]["settlingNodeId"]
    assert added == contract["topologyPlans"][0]["addedSetupNodeId"]
    mutated = json.loads(canonical_json(contract))
    mutated["slots"][3]["settlingNodeId"] = "entry"
    with pytest.raises(TemporalDiscoveryContractError, match="newly added setup node"):
        validate_topology_coadaptation_matrix_v3(mutated)


def test_insert_exit_region_cannot_enter_event_settling_first_contrast() -> None:
    contract = _coadaptation_contract()
    mutated = json.loads(canonical_json(contract))
    mutated["topologyPlans"][0]["topologyPlan"]["operation"] = "insert_exit_region"
    with pytest.raises(TemporalDiscoveryContractError, match="insert_exit_region cannot enter"):
        validate_topology_coadaptation_matrix_v3(mutated)


def test_generic_example_topology_plan_shas_cannot_satisfy_launch_grade_slots() -> None:
    contract = _coadaptation_contract()
    mutated = json.loads(canonical_json(contract))
    mutated["topologyPlans"][0]["topologyPlan"]["v38ExampleOperatorPlanSha256"] = _sha("11")
    with pytest.raises(TemporalDiscoveryContractError, match="generic example topology plan SHAs"):
        validate_topology_coadaptation_matrix_v3(mutated)


def test_panel_1_and_2_are_labeled_replication_and_overlay_reseals() -> None:
    contract = _coadaptation_contract()
    assert contract["panelIdentities"]["replicationRole"] == "inspected_replication_not_untouched_confirmation"
    assert contract["panelIdentities"]["futureConfirmationPanel"]["createdInThisTask"] is False
    assert topology_coadaptation_v3_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    attached = attach_topology_coadaptation_matrix_v3(
        {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"},
        contract,
    )
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    assert attached["configSha256"] != "x"


def test_emitted_v3_contracts_round_trip_if_present() -> None:
    coadaptation = Path("research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v3.json")
    launch = Path("research/temporal-qd/v38-followup/resource-suboperation-launch-manifest-v1.json")
    if coadaptation.is_file():
        payload = json.loads(coadaptation.read_text(encoding="utf-8"))
        validated = validate_topology_coadaptation_matrix_v3(payload)
        assert validated["contractSha256"] == payload["contractSha256"]
        assert canonical_json(validated) == canonical_json(payload)
        assert payload["panelIdentities"]["replicationRole"] == "inspected_replication_not_untouched_confirmation"
        for slot in payload["slots"]:
            if slot["arm"] == "topology_then_topology_local_event" and slot["eligibility"] == "eligible":
                plan = next(item for item in payload["topologyPlans"] if item["planId"] == slot["topologyPlanId"])
                assert slot["settlingNodeId"] == plan["addedSetupNodeId"]
    if launch.is_file():
        payload = json.loads(launch.read_text(encoding="utf-8"))
        validated = validate_resource_suboperation_launch_manifest(payload)
        assert validated["contractSha256"] == payload["contractSha256"]
        assert validated["boundedTaskProjection"]["doNotLaunch"] is True
