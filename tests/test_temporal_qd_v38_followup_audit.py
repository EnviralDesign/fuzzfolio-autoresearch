from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_topology_coadaptation import (
    ARMS,
    COADAPTATION_SCHEMA,
    FIRST_EXPERIMENT_JUSTIFICATION,
    attach_topology_coadaptation_matrix,
    build_topology_coadaptation_matrix,
    topology_coadaptation_from_config,
    validate_topology_coadaptation_matrix,
)
from autoresearch.temporal_qd_v38_followup_audit import (
    build_resource_report,
    implied_reward_to_risk,
    numbers_equal,
    relative_to_parent,
    resource_kind_bucket,
    summarize_construction,
    topology_operation_classes,
)


def test_canonical_numeric_equality_is_exact() -> None:
    assert numbers_equal(1.0, 1.0)
    assert numbers_equal(1, 1.0)
    assert not numbers_equal(1.0, 1.0 + 1e-12)
    assert not numbers_equal(True, 1)


def test_protection_forensic_counts_side_close_reasons() -> None:
    from autoresearch.temporal_qd_v38_followup_audit import _window_forensic

    forensic = _window_forensic(
        {
            "aggregate": {
                "costDragR": 52.0,
                "totalConservativeNetR": -69.0,
                "closeReasonDistribution": {"stop_loss": 0.3, "break_even_stop": 0.58},
                "windowRecords": [
                    {
                        "conservativeNetR": -31.0,
                        "trades": 2,
                        "realizedBehavior": {
                            "sides": {
                                "short": {
                                    "closeReasonCounts": {"stop_loss": 2, "break_even_stop": 1},
                                    "tradeSequence": [
                                        {"closeReason": "stop_loss", "netR": -1.0},
                                        {"closeReason": "break_even_stop", "netR": -0.2},
                                    ],
                                }
                            }
                        },
                    }
                ],
            }
        }
    )
    assert forensic["closeReasonCounts"]["stop_loss"] == 2
    assert forensic["lossMechanismHypothesis"] == "cost_drag_and_churn"
    assert forensic["tradeCount"] == 2
    assert numbers_equal(1.0, 1.0)
    assert numbers_equal(1, 1.0)
    assert not numbers_equal(1.0, 1.0 + 1e-12)
    assert not numbers_equal(True, 1)


def test_topology_classes_separate_additive_removal_and_rewire() -> None:
    assert "additive_complexification" in topology_operation_classes("insert_setup")
    assert "destructive_removal" in topology_operation_classes("remove_exit_region")
    assert "rewire" in topology_operation_classes("rewire_management_region")
    assert "confirmation_rearm" in topology_operation_classes("insert_timeout_rearm")


def test_resource_kind_buckets_and_protection_rr() -> None:
    assert resource_kind_bucket("indicator_period_mutate") == "parameter_level_indicator"
    assert resource_kind_bucket("indicator_substitute") == "indicator_structure"
    rr = implied_reward_to_risk(
        {"kind": "fixed_percent", "percent": 1.0},
        {"kind": "reward_multiple", "multiple": 1.0},
    )
    assert rr["defined"] is True
    assert rr["value"] == 1.0
    dynamic_multiple = implied_reward_to_risk(
        {"kind": "indicator_price_level", "bindingId": "x"},
        {"kind": "reward_multiple", "multiple": 2.0},
    )
    assert dynamic_multiple["defined"] is True
    assert dynamic_multiple["value"] == 2.0
    assert implied_reward_to_risk({"kind": "fixed_percent", "percent": 1.0}, {"kind": "none"})["defined"] is False


def test_construction_summary_and_parent_relative_identity() -> None:
    summary = summarize_construction(
        {
            "terminalOperatorPlan": {
                "operatorId": "evolvable_resource_v1",
                "construction": {
                    "kind": "indicator_period_mutate",
                    "indicatorInstanceId": "trend_trigger",
                    "change": {"parameter": "timeperiod", "before": 21, "after": 50, "choice": "slow"},
                },
            },
            "terminalOperatorApplication": {
                "applicationAudit": {
                    "mutationTrace": [{"before": {"timeperiod": 21}, "after": {"timeperiod": 50}}]
                }
            },
        }
    )
    assert summary["constructionKind"] == "indicator_period_mutate"
    parent = {"cumulativeConservativeNetR": -4.15, "worstWindowConservativeNetR": -1.55}
    child = {"cumulativeConservativeNetR": -4.15, "worstWindowConservativeNetR": -1.55}
    relative = relative_to_parent(child, parent)
    assert relative["economicTie"] is True
    assert relative["beatParent"] is False
    assert relative_to_parent({"cumulativeConservativeNetR": -3.0}, parent)["beatParent"] is True


def test_resource_report_counts_unrecovered_rejects() -> None:
    slots = [
        {
            "operatorFamily": "resource",
            "parentCandidateId": "parent_a",
            "parentRole": "archive",
            "disposition": "accepted",
            "recovered": True,
            "constructionKind": "indicator_period_mutate",
            "metrics": {"cumulativeConservativeNetR": 1.0, "worstWindowConservativeNetR": 0.0},
            "relative": {
                "comparable": True,
                "beatParent": True,
                "lostToParent": False,
                "economicTie": False,
                "absolutePositive": True,
                "deltaCumulativeConservativeNetR": 2.0,
                "deltaWorstWindowConservativeNetR": 1.0,
            },
        },
        {
            "operatorFamily": "resource",
            "parentCandidateId": "parent_a",
            "parentRole": "archive",
            "disposition": "rejected",
            "reasonCode": "duplicate_pair_genome",
            "canonicalCollapse": True,
            "recovered": False,
            "constructionKind": None,
            "metrics": None,
            "relative": {"comparable": False},
        },
    ]
    report = build_resource_report(slots, {"parent_a": {"role": "archive", "cumulativeConservativeNetR": -1.0}})
    period = next(item for item in report["bySuboperation"] if item["constructionKind"] == "indicator_period_mutate")
    unrecovered = next(item for item in report["bySuboperation"] if item["constructionKind"] == "unrecovered")
    assert period["uniqueAcceptedChildren"] == 1
    assert period["absolutePositiveChildren"] == 1
    assert unrecovered["duplicatePairGenomeCount"] == 1
    assert unrecovered["unrecoveredRejected"] == 1
    first = canonical_sha256({key: value for key, value in report.items() if key != "reportSha256"})
    second = canonical_sha256({key: value for key, value in json.loads(canonical_json(report)).items() if key != "reportSha256"})
    assert report["reportSha256"] == first == second


def _coadaptation_contract() -> dict[str, object]:
    return build_topology_coadaptation_matrix(
        parents=[{"candidateId": "parent_a", "role": "archive"}],
        rotating_evidence_sha256="sha256:" + ("ab" * 32),
        topology_plans=[
            {
                "planId": "insert_setup",
                "operation": "insert_setup",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"edgeId": "start_setup", "guard": {"kind": "always"}, "kind": "context"},
                "v38ExampleOperatorPlanSha256": "sha256:" + ("11" * 32),
            },
            {
                "planId": "insert_exit_region",
                "operation": "insert_exit_region",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"guard": {"kind": "always"}, "kind": "region", "priority": 1},
                "v38ExampleOperatorPlanSha256": "sha256:" + ("22" * 32),
            },
        ],
        first_experiment_justification=FIRST_EXPERIMENT_JUSTIFICATION,
    )


def test_coadaptation_overlay_is_inert_when_absent() -> None:
    assert topology_coadaptation_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    assert topology_coadaptation_from_config({}) is None


def test_coadaptation_rejects_production_archive_writes_and_wrong_arms() -> None:
    contract = _coadaptation_contract()
    with pytest.raises(TemporalDiscoveryContractError, match="production archive"):
        validate_topology_coadaptation_matrix({**contract, "productionArchiveWrite": True})
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema|arms"):
        validate_topology_coadaptation_matrix({**contract, "arms": list(ARMS)[:3]})


def test_coadaptation_attach_reseals_config_and_does_not_touch_production_matrix_field() -> None:
    from autoresearch.evidence_plan import canonical_sha256

    base = {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"}
    attached = attach_topology_coadaptation_matrix(base, _coadaptation_contract())
    assert "operatorFamilyMatrix" not in attached
    assert attached["topologyCoadaptationMatrix"]["schemaVersion"] == COADAPTATION_SCHEMA
    assert attached["topologyCoadaptationMatrix"]["arms"] == list(ARMS)
    assert base.get("topologyCoadaptationMatrix") is None
    assert attached["configSha256"] != "x"
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
