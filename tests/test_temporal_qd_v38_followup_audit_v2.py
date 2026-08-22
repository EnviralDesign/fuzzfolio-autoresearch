from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_resource_suboperation_matrix import (
    build_resource_suboperation_matrix,
    validate_resource_suboperation_matrix,
)
from autoresearch.temporal_qd_topology_coadaptation import (
    FIRST_EXPERIMENT_JUSTIFICATION,
    attach_topology_coadaptation_matrix,
    build_topology_coadaptation_matrix,
    topology_coadaptation_from_config,
    validate_topology_coadaptation_matrix,
)
from autoresearch.temporal_qd_v38_followup_audit_v2 import (
    METRIC_IDENTITY_FLOOR_R,
    build_resource_report_v2,
    canonical_metric_greater,
    canonical_metrics_equal,
    relative_to_parent_v2,
)


def _sha(byte: str) -> str:
    return "sha256:" + (byte * 32)


def _coadaptation_contract() -> dict[str, object]:
    return build_topology_coadaptation_matrix(
        parents=[{"candidateId": "parent_a", "role": "archive"}],
        rotating_evidence_sha256=_sha("ab"),
        topology_plans=[
            {
                "planId": "insert_setup",
                "operation": "insert_setup",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"edgeId": "start_setup", "guard": {"kind": "always"}, "kind": "context"},
                "v38ExampleOperatorPlanSha256": _sha("11"),
            },
            {
                "planId": "insert_exit_region",
                "operation": "insert_exit_region",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"guard": {"kind": "always"}, "kind": "region", "priority": 1},
                "v38ExampleOperatorPlanSha256": _sha("22"),
            },
        ],
        first_experiment_justification=FIRST_EXPERIMENT_JUSTIFICATION,
    )


def test_canonical_metric_identity_does_not_count_numerical_dust() -> None:
    parent = -4.1499999999999995
    child = -4.14999999999999
    assert abs((child - parent) - 9.769962616701378e-15) < 1e-18
    assert canonical_metrics_equal(parent, child)
    assert not canonical_metric_greater(child, parent)
    assert METRIC_IDENTITY_FLOOR_R == 1e-12


def test_full_economic_phenotype_tie_and_dust_is_not_a_beat() -> None:
    parent = {
        "cumulativeConservativeNetR": -4.1499999999999995,
        "worstWindowConservativeNetR": -1.5499999999999923,
        "medianWindowConservativeNetR": -1.4500000000000033,
        "activeWindowFraction": 1.0,
        "authoredProgramSha256": "sha256:parent",
        "resolvedProgramSha256": "sha256:parent-resolved",
    }
    child = {
        "cumulativeConservativeNetR": -4.14999999999999,
        "worstWindowConservativeNetR": -1.5499999999999923,
        "medianWindowConservativeNetR": -1.4500000000000033,
        "activeWindowFraction": 1.0,
        "authoredProgramSha256": "sha256:child",
        "resolvedProgramSha256": "sha256:child-resolved",
    }
    relative = relative_to_parent_v2(child, parent)
    assert relative["comparable"] is True
    assert relative["beatParent"] is False
    assert relative["equalCumulativeNetOnly"] is True
    assert relative["fullEconomicPhenotypeTie"] is True
    assert relative["exactGenotypeIdentity"] is False
    assert relative["exactResolvedProgramIdentity"] is False


def test_v2_resource_report_is_deterministic_and_renames_mix() -> None:
    slots = [
        {
            "operatorFamily": "resource",
            "parentCandidateId": "parent_a",
            "parentRole": "archive",
            "disposition": "accepted",
            "recovered": True,
            "constructionKind": "indicator_timeframe_mutate",
            "metrics": {
                "cumulativeConservativeNetR": 1.0,
                "worstWindowConservativeNetR": 0.0,
                "medianWindowConservativeNetR": 0.5,
                "activeWindowFraction": 1.0,
                "combinedSupportPass": True,
                "directionEligible": True,
                "currentPanelQualityLike": True,
            },
            "relative": {
                "comparable": True,
                "beatParent": True,
                "lostToParent": False,
                "fullEconomicPhenotypeTie": False,
                "equalCumulativeNetOnly": False,
                "riskQualifiedBeat": True,
                "absolutePositive": True,
                "deltaCumulativeConservativeNetR": 2.0,
                "deltaWorstWindowConservativeNetR": 1.0,
            },
            "finalArchiveMember": False,
            "panelOutcomes": {},
        }
    ]
    report = build_resource_report_v2(slots, {"parent_a": {"role": "archive"}})
    assert "successfulChildrenMixture" not in report["answers"]
    assert "kindsWorkingAcrossAllArchiveParents" not in report["answers"]
    assert report["answers"]["acceptedSuboperationMix"]["indicator_timeframe_mutate"] == 1
    assert report["answers"]["parameterLevelRepeatablePositiveTail"] == "not_demonstrated"
    first = canonical_sha256({key: value for key, value in report.items() if key != "reportSha256"})
    second = canonical_sha256(
        {key: value for key, value in json.loads(canonical_json(report)).items() if key != "reportSha256"}
    )
    assert report["reportSha256"] == first == second


def test_coadaptation_v2_rejects_missing_extra_and_parameter_only_event_insert() -> None:
    contract = _coadaptation_contract()
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        validate_topology_coadaptation_matrix({**contract, "extraField": True})
    missing = {key: value for key, value in contract.items() if key != "parents"}
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        validate_topology_coadaptation_matrix(missing)
    mutated = json.loads(canonical_json(contract))
    mutated["settling"]["parameterOnly"]["eligibleKinds"] = [
        "directional_event_insert",
        "indicator_period_mutate",
        "indicator_range_mutate",
        "indicator_timeframe_mutate",
    ]
    with pytest.raises(TemporalDiscoveryContractError, match="parameter-only lane|drifted"):
        validate_topology_coadaptation_matrix(mutated)


def test_coadaptation_attach_reseals_and_absent_overlay_is_inert() -> None:
    assert topology_coadaptation_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    base = {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"}
    attached = attach_topology_coadaptation_matrix(base, _coadaptation_contract())
    assert attached["configSha256"] != "x"
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    assert attached["topologyCoadaptationMatrix"]["settling"]["parameterOnly"]["eligibleKinds"] == [
        "indicator_period_mutate",
        "indicator_range_mutate",
        "indicator_timeframe_mutate",
        "indicator_lookback_mutate",
    ]
    assert "directional_event_insert" not in attached["topologyCoadaptationMatrix"]["settling"]["parameterOnly"]["eligibleKinds"]
    assert attached["topologyCoadaptationMatrix"]["slotBudget"]["firstExperimentSlotCount"] == 6


def test_balanced_resource_suboperation_matrix_self_hashes() -> None:
    spec = build_resource_suboperation_matrix(
        parents=[{"candidateId": "parent_a", "role": "archive"}],
        children_per_eligible_cell=8,
    )
    assert spec["lanes"][0] == "directional_event_insert"
    assert spec["doNotSampleUniformlyFromFullPlanPool"] is True
    assert validate_resource_suboperation_matrix(spec)["contractSha256"] == spec["contractSha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        validate_resource_suboperation_matrix({**spec, "extra": 1})


def test_emitted_v2_spec_round_trips_if_present() -> None:
    path = Path("research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v2.json")
    if not path.is_file():
        pytest.skip("v2 spec has not been emitted yet")
    payload = json.loads(path.read_text(encoding="utf-8"))
    validated = validate_topology_coadaptation_matrix(payload)
    assert validated["contractSha256"] == payload["contractSha256"]
    assert canonical_json(validated) == canonical_json(payload)
