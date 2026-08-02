from __future__ import annotations

import json
import random
from pathlib import Path

import autoresearch.temporal_qd_evolution as qd_module
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_evolution import select_qd_archive
from autoresearch.temporal_search_policy_v2 import (
    GENERATOR_V2_PARAMETERS,
    _repair_profile,
    inspect_management_reachability,
)


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "QD fixture",
        "description": "QD fixture",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "plan",
                "plans": [
                    {
                        "id": "plan",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                        "initialTarget": {
                            "kind": "reward_multiple",
                            "multiple": 2.0,
                        },
                        "trailingStop": {
                            "activation": {"kind": "explicit"},
                            "anchor": {"kind": "bar_close"},
                            "distance": {
                                "kind": "fixed_initial_r",
                                "multiple": 1.0,
                            },
                            "minimumStepInitialR": 0.0,
                        },
                    }
                ],
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": ["data.completed_ohlc"],
            "initialStateId": "flat",
            "states": [
                {"id": "flat"},
                {"id": "entry_requested"},
                {"id": "open"},
                {"id": "protected"},
                {"id": "closed"},
            ],
            "evidenceGroups": [{"id": "signal", "indicatorInstanceIds": ["sig"]}],
            "eventBindings": [],
            "transitions": [
                {
                    "id": "flat_to_entry",
                    "sourceStateId": "flat",
                    "destinationStateId": "entry_requested",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": False},
                            {
                                "kind": "evidence_at_least",
                                "groupId": "signal",
                                "thresholdPercent": 60.0,
                            },
                        ],
                    },
                    "actions": [
                        {"kind": "enter_next_open", "managementPlanId": "plan"}
                    ],
                    "reasonCode": "entry",
                },
                {
                    "id": "entry_to_open",
                    "sourceStateId": "entry_requested",
                    "destinationStateId": "open",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "filled"},
                    "actions": [],
                    "reasonCode": "filled",
                },
                {
                    "id": "protect",
                    "sourceStateId": "open",
                    "destinationStateId": "protected",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {"kind": "unrealized_r_at_least", "value": 1.0},
                        ],
                    },
                    "actions": [{"kind": "move_stop_to_break_even_next_open"}],
                    "reasonCode": "protect",
                },
                {
                    "id": "open_closed",
                    "sourceStateId": "open",
                    "destinationStateId": "closed",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
                {
                    "id": "protected_closed",
                    "sourceStateId": "protected",
                    "destinationStateId": "closed",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
            ],
        },
    }


def _member(
    candidate_id: str,
    robust_return: float,
    drawdown: float,
    *,
    total_trades: int = 20,
    valid_for_quality: bool = True,
    complexity: float = 10.0,
) -> dict:
    profile, _ = _repair_profile(_profile(), parameters=GENERATOR_V2_PARAMETERS)
    profile["name"] = candidate_id
    candidate = {
        "candidateId": candidate_id,
        "sourceMode": "fixture",
        "seedId": "fixture",
        "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "programSha256": canonical_sha256({"program": profile}),
    }
    return {
        "candidateId": candidate_id,
        "generationIndex": 0,
        "candidate": candidate,
        "aggregate": {"totalTrades": total_trades},
        "descriptor": {
            "operatorFamilies": "none",
            "mutationDepth": "root",
            "entryEvents": "none",
            "managementActions": "one",
            "graphNodes": "medium",
            "tradeFrequency": "moderate",
            "medianHolding": "medium",
            "cellId": "one-cell",
        },
        "objectives": {
            "worstWindowConservativeNetR": robust_return,
            "maximumDrawdownR": drawdown,
            "structuralComplexity": complexity,
        },
        "cappedTradeSupport": float(min(total_trades, 20)),
        "finiteDataValidity": {
            "totalTrades": total_trades,
            "capTrades": 20,
            "isFiniteData": valid_for_quality,
            "passesSupportGate": valid_for_quality,
            "validForQuality": valid_for_quality,
        },
    }


def test_qd_archive_retains_multiobjective_cell_extremes() -> None:
    cells = select_qd_archive(
        [
            _member("candidate_a", 5.0, 5.0),
            _member("candidate_b", 3.0, 1.0),
            _member("candidate_c", 1.0, 4.0),
        ],
        cell_capacity=2,
    )
    assert len(cells) == 1
    assert {item["candidateId"] for item in cells[0]["members"]} == {
        "candidate_a",
        "candidate_b",
    }


def test_qd_archive_reduction_is_independent_of_completion_order() -> None:
    members = [
        _member("candidate_a", 5.0, 5.0),
        _member("candidate_b", 3.0, 1.0),
        _member("candidate_c", 1.0, 4.0),
    ]
    forward = select_qd_archive(members, cell_capacity=2)
    reverse = select_qd_archive(list(reversed(members)), cell_capacity=2)
    assert canonical_sha256(forward) == canonical_sha256(reverse)


def test_qd_invalid_high_return_member_cannot_displace_valid_pareto_member() -> None:
    valid = _member("candidate_valid", 1.0, 1.0)
    invalid = _member("candidate_invalid", 1_000_000.0, 0.0001)
    invalid["finiteDataValidity"] = {
        "isFiniteData": True,
        "passesSupportGate": False,
        "validForQuality": False,
    }
    cells = select_qd_archive([invalid, valid], cell_capacity=4)
    assert {item["candidateId"] for item in cells[0]["members"]} == {
        "candidate_valid",
        "candidate_invalid",
    }
    retained = {item["candidateId"]: item for item in cells[0]["members"]}
    assert retained["candidate_valid"]["archiveLane"] == "quality"
    assert retained["candidate_invalid"]["archiveLane"] == "observational"


def test_qd_identity_and_descriptors_exclude_scheduling_and_profitability() -> None:
    parent = _member("candidate_parent", 2.0, 1.0)["candidate"]
    profile = parent["sourceProfile"]
    lineage = [{"operatorId": "fixture_operator", "exactPlan": {"events": 3}}]
    material, identity, candidate_id = qd_module._candidate_identity(
        origin_kind="structural_offspring",
        parent=parent,
        profile=profile,
        ordered_lineage=lineage,
        origin_contract={"operatorVersion": "v1"},
    )
    assert not {
        "generationIndex",
        "workerId",
        "evaluationOrder",
        "archiveCell",
        "rngSchedule",
    } & set(material)
    assert identity == canonical_sha256(material)
    assert candidate_id.startswith("qd_")
    candidate = {**parent, "structuralOperatorHistory": lineage}
    left = qd_module.qd_behavior_descriptor(
        candidate,
        {
            "totalTrades": 10,
            "entryFrequencyPerThousand": 4.0,
            "medianHoldingBars": 48.0,
            "totalConservativeNetR": -100.0,
        },
    )
    right = qd_module.qd_behavior_descriptor(
        candidate,
        {
            "totalTrades": 10,
            "entryFrequencyPerThousand": 4.0,
            "medianHoldingBars": 48.0,
            "totalConservativeNetR": 1_000_000.0,
        },
    )
    assert left == right
    assert not {"profit", "score", "origin", "generation"} & set(left)


def test_qd_robust_objective_uses_worst_window_not_total_return() -> None:
    candidate = _member("candidate", 0.0, 1.0)["candidate"]
    high_total_fragile = qd_module._objective_row(
        candidate,
        {
            "totalConservativeNetR": 100.0,
            "worstWindowConservativeNetR": -2.0,
            "maxWindowDrawdownR": 1.0,
        },
    )
    lower_total_robust = qd_module._objective_row(
        candidate,
        {
            "totalConservativeNetR": 10.0,
            "worstWindowConservativeNetR": 1.0,
            "maxWindowDrawdownR": 1.0,
        },
    )
    assert lower_total_robust["worstWindowConservativeNetR"] > high_total_fragile[
        "worstWindowConservativeNetR"
    ]
    assert set(high_total_fragile) == {
        "worstWindowConservativeNetR",
        "maximumDrawdownR",
        "structuralComplexity",
    }


def test_qd_high_trade_support_cannot_pareto_dominate() -> None:
    low_support = _member("low_support", 1.0, 1.0, total_trades=8)
    high_support = _member("high_support", 1.0, 1.0, total_trades=500)
    assert not qd_module._dominates(high_support, low_support)
    assert not qd_module._dominates(low_support, high_support)


def test_qd_support_is_eligibility_then_capped_tie_break() -> None:
    insufficient = qd_module._finite_data_validity(
        {
            "totalTrades": 7,
            "tradeCountsByWindow": [4, 3],
            "totalObservations": 10,
            "worstWindowConservativeNetR": 1.0,
            "maxWindowDrawdownR": 1.0,
        },
        minimum_total_trades=8,
        minimum_trades_per_window=4,
        cap_trades=20,
    )
    eligible = qd_module._finite_data_validity(
        {
            "totalTrades": 40,
            "tradeCountsByWindow": [20, 20],
            "totalObservations": 10,
            "worstWindowConservativeNetR": 1.0,
            "maxWindowDrawdownR": 1.0,
        },
        minimum_total_trades=8,
        minimum_trades_per_window=4,
        cap_trades=20,
    )
    assert insufficient["passesSupportGate"] is False
    assert eligible["validForQuality"] is True
    assert qd_module._capped_trade_support(
        _member("twenty", 1.0, 1.0, total_trades=20)
    ) == qd_module._capped_trade_support(
        _member("many", 1.0, 1.0, total_trades=500)
    ) == 20.0


def test_qd_broad_archive_is_not_quality_breeding_pool() -> None:
    quality = _member("quality", 1.0, 1.0)
    observational = _member(
        "undersupported", 100.0, 0.01, valid_for_quality=False
    )
    negative = _member("negative", -0.5, 1.0)
    cells = select_qd_archive([quality, observational, negative])
    member_by_id = {item["candidateId"]: item for item in cells[0]["members"]}
    assert member_by_id["quality"]["archiveLane"] == "quality"
    assert member_by_id["undersupported"]["archiveLane"] == "observational"
    assert member_by_id["negative"]["archiveLane"] == "negative_novelty"
    archive = {"cells": cells}
    parents = qd_module._reproduction_cells(archive)
    assert [item["candidateId"] for item in parents[0]["members"]] == ["quality"]


def test_qd_negative_novelty_lane_is_capped_and_accounted() -> None:
    quality = _member("quality", 1.0, 1.0)
    cells = select_qd_archive(
        [quality, _member("negative_a", -0.1, 1.0), _member("negative_b", -0.2, 1.0)],
        cell_capacity=4,
    )
    assert cells[0]["negativeNoveltyMemberCount"] == 1
    entries = [
        {
            "originKind": "structural_offspring",
            "disposition": "accepted",
            "proposal": {
                "parentSelectionMode": "negative_novelty_exploration"
                if index in {9, 19}
                else "uniform_occupied_cell",
                "parentLane": "negative_novelty" if index in {9, 19} else "quality",
                "parentLaneReason": "fixture",
                "parentCellId": "one-cell",
                "desiredMutationDepth": 1,
                "steps": [],
            },
        }
        for index in range(20)
    ]
    accounting = qd_module._proposal_accounting(entries)
    assert accounting["negativeNoveltyParentSelectionCount"] == 2
    assert accounting["negativeNoveltyParentSelectionFraction"] == 0.10
    assert sum(qd_module._negative_novelty_slot(index) for index in range(100)) <= 10


def test_qd_rank_aware_parent_selection_is_deterministic() -> None:
    best = _member("best", 3.0, 1.0)
    best["paretoFront"] = 0
    best["crowdingDistance"] = 5.0
    weaker = _member("weaker", 1.0, 2.0)
    weaker["paretoFront"] = 1
    weaker["crowdingDistance"] = 0.0
    first = qd_module._rank_aware_parent_member(
        [weaker, best], rng=random.Random(17)
    )
    second = qd_module._rank_aware_parent_member(
        [best, weaker], rng=random.Random(17)
    )
    assert first["candidateId"] == second["candidateId"]
    assert qd_module._parent_member_order(best) < qd_module._parent_member_order(weaker)


def test_qd_default_cell_capacity_is_four() -> None:
    cells = select_qd_archive(
        [_member(f"candidate_{index}", 1.0 + index, 1.0) for index in range(5)]
    )
    assert len(cells[0]["members"]) == 4
    assert qd_module.DEFAULT_QD_PARAMETERS["cellCapacity"] == 4


def test_qd_global_ledger_rejects_evicted_exact_evaluation_and_reports_counts(
    tmp_path: Path,
) -> None:
    profile, _ = _repair_profile(_profile(), parameters=GENERATOR_V2_PARAMETERS)
    candidate = {
        "candidateIdentitySha256": canonical_sha256({"candidate": "once"}),
        "programSha256": canonical_sha256({"program": profile}),
        "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "profileSnapshotSha256": canonical_sha256(profile),
    }
    context = qd_module.qd_predeclared_evidence_context({})
    candidate["canonicalEvidenceIdentitySha256"] = (
        qd_module.qd_canonical_evidence_identity(candidate, context)
    )
    ledger_path = tmp_path / "identity-ledger.json"
    ledger = qd_module._load_identity_ledger(ledger_path)
    qd_module._ledger_accept(ledger, candidate)
    qd_module._save_identity_ledger(ledger_path, ledger)
    # The parent/archive can evict the member; ledger history remains authoritative.
    reason, checks = qd_module._ledger_duplicate_check(ledger, candidate)
    assert reason == "duplicate_candidate_identity_global"
    assert checks["canonicalEvidence"] is True
    qd_module._save_identity_ledger(ledger_path, ledger)
    restored = qd_module._load_identity_ledger(ledger_path)
    counters = qd_module._ledger_public_counts(restored)
    assert counters["uniqueIdentityCounts"]["candidateIdentity"] == 1
    assert counters["uniqueIdentityCounts"]["canonicalEvidence"] == 1
    assert counters["duplicateCounters"]["candidateIdentity"] == 1
    assert counters["proposalSlotCounters"]["duplicateRejections"] == 1


def test_qd_ledger_allows_same_program_only_for_changed_predeclared_evidence() -> None:
    profile, _ = _repair_profile(_profile(), parameters=GENERATOR_V2_PARAMETERS)
    common = {
        "programSha256": canonical_sha256({"program": profile}),
        "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "profileSnapshotSha256": canonical_sha256(profile),
    }
    first = {**common, "candidateIdentitySha256": canonical_sha256({"candidate": 1})}
    second = {**common, "candidateIdentitySha256": canonical_sha256({"candidate": 2})}
    first_context = qd_module.qd_predeclared_evidence_context({})
    second_context = {
        **first_context,
        "orderedWindowPlanSemantic": [{"windowId": "different-predeclared-window"}],
    }
    second_context["predeclaredEvidenceContextSha256"] = canonical_sha256(
        {
            key: value
            for key, value in second_context.items()
            if key != "predeclaredEvidenceContextSha256"
        }
    )
    first["canonicalEvidenceIdentitySha256"] = qd_module.qd_canonical_evidence_identity(
        first, first_context
    )
    second["canonicalEvidenceIdentitySha256"] = qd_module.qd_canonical_evidence_identity(
        second, second_context
    )
    ledger = qd_module._empty_identity_ledger()
    qd_module._ledger_accept(ledger, first)
    reason, checks = qd_module._ledger_duplicate_check(ledger, second)
    assert checks["program"] is True
    assert checks["canonicalEvidence"] is False
    assert reason is None


def test_qd_construction_registry_records_exact_trace_and_timeframe_evidence_rotation(
    tmp_path: Path, monkeypatch
) -> None:
    catalog_path = tmp_path / "construction-catalog.json"
    catalog_path.write_text(
        json.dumps(
            {
                "timeframes": {"M1": {}, "M5": {}, "M15": {}, "M30": {}, "H1": {}},
                "indicators": [
                    {
                        "meta": {
                            "id": "FIXTURE_INDICATOR",
                            "requiredPaddingBars": 10,
                        },
                        "config": {
                            "isActive": True,
                            "timeframe": "M5",
                            "lookbackBars": 14,
                        },
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    policy, registry = qd_module.qd_construction_operator_policy(catalog_path)
    assert registry is not None
    assert policy["enabled"] is True
    assert policy["catalog"]["catalogSha256"] == canonical_sha256(
        json.loads(catalog_path.read_text())
    )
    assert "graph_bound_indicator_timeframe_v3" in policy["enabledOperatorIds"]
    assert policy["conditionalOperatorEligibility"] == [
        {
            "operatorId": "graph_bound_indicator_timeframe_v3",
            "eligibility": "candidate_derived_request_is_contained_by_every_immutable_pre_attested_window_binding",
            "outOfScopeDisposition": "predeclared_lake_scope_rejected",
            "bindingPolicy": "reuse_only_no_local_semantic_rehash",
        }
    ]
    assert "indicator_family_substitution_v3" not in policy["enabledOperatorIds"]
    active_qd_operators = qd_module._operators(
        registry,
        construction_operator_ids=policy["enabledOperatorIds"],
    )
    assert "graph_bound_indicator_timeframe_v3" in active_qd_operators

    member = _member("construction-parent", 2.0, 1.0)
    parent = member["candidate"]
    profile = parent["sourceProfile"]
    profile["indicators"] = [
        {
            "meta": {"id": "FIXTURE_INDICATOR", "instanceId": "sig"},
            "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"},
        }
    ]
    parent["sourceProfileSha256"] = canonical_sha256(profile)
    parent["profileSnapshotSha256"] = parent["sourceProfileSha256"]
    parent["programSha256"] = canonical_sha256({"program": profile})

    scope_context = {
        "baseDecisionTimeframe": "M5",
        "orderedWindowPlanSemantic": [
            {
                "windowId": "development",
                "window": {
                    "analysisWindowStart": "2024-02-01T00:00:00Z",
                    "analysisWindowEnd": "2024-03-01T00:00:00Z",
                },
                "evidencePlanSemantic": {
                    "lake_window_binding": {
                        "window_semantic_sha256": "sha256:" + "e" * 64,
                        "request": {
                            "pairs": ["EURUSD"],
                            "timeframes": ["M5", "M15", "H1"],
                            "data_start": "2024-01-01T00:00:00Z",
                            "data_end": "2024-03-01T00:00:00Z",
                        },
                    }
                },
            }
        ],
    }
    for replacement in ("M1", "M30"):
        out_of_scope = json.loads(json.dumps(profile))
        out_of_scope["indicators"][0]["config"]["timeframe"] = replacement
        report = qd_module._predeclared_lake_scope_report(
            out_of_scope,
            scope_context,
            frozen_construction_catalog=registry.catalog.payload,
        )
        assert report["acceptable"] is False
        assert report["reason"] == "candidate_derived_request_outside_pre_attested_scope"
    for replacement in ("M15", "H1"):
        in_scope = json.loads(json.dumps(profile))
        in_scope["indicators"][0]["config"]["timeframe"] = replacement
        assert qd_module._predeclared_lake_scope_report(
            in_scope,
            scope_context,
            frozen_construction_catalog=registry.catalog.payload,
        )["acceptable"] is True

    class DeterministicRng:
        def random(self) -> float:
            return 0.0

        def randrange(self, stop: int) -> int:
            assert stop > 0
            return 0

    class Validator:
        def validate(self, *, candidate_id: str, source_profile: dict, expected_raw_source_profile_sha256: str) -> dict:
            assert canonical_sha256(source_profile) == expected_raw_source_profile_sha256
            program_sha = canonical_sha256({"program": source_profile})
            return {
                "candidateAcceptable": True,
                "status": "valid_evaluable",
                "programSha256": program_sha,
                "profileSnapshotSha256": expected_raw_source_profile_sha256,
                "validationReportSha256": canonical_sha256({"candidate": candidate_id}),
                "issues": [],
            }

    monkeypatch.setattr(qd_module, "_available_mutations", lambda _profile: {})
    operators = {
        "graph_bound_indicator_timeframe_v3": registry.get(
            "graph_bound_indicator_timeframe_v3"
        )
    }
    child, metadata = qd_module._structural_proposal(
        rng=DeterministicRng(),
        cells=[{"cellId": "fixture-cell", "members": [member]}],
        negative_novelty_cells=[],
        negative_novelty_slot=False,
        operators=operators,
        max_depth=16,
        plan_cache={},
        selection_state={"fixture-cell": {"selectionVisitCount": 0, "offspringAttemptCount": 0}},
        validator=Validator(),
        evidence_context=scope_context,
        frozen_construction_catalog=registry.catalog.payload,
    )
    assert child is not None
    step = metadata["steps"][0]
    assert step["operatorId"] == "graph_bound_indicator_timeframe_v3"
    assert step["evidenceScope"]["evidencePlanRotationRequired"] is True
    assert step["evidenceScope"]["lakeScopeRegenerationRequired"] is True
    assert step["plan"]["construction"]["after"] in {"M15", "H1"}
    assert step["mutationTrace"] == [
        {
            "operation": "substitute_graph_bound_indicator_timeframe",
            "indicatorInstanceId": "sig",
            "before": "M5",
            "after": step["plan"]["construction"]["after"],
            "evidenceLakeScope": {
                "regenerationRequired": True,
                "reason": "graph_bound_indicator_timeframe_changed",
            },
        }
    ]
    assert metadata["evidenceScope"]["evidencePlanRotationRequired"] is True
    assert metadata["evidenceScope"]["lakeScopeRegenerationRequired"] is True
    accounting = qd_module._proposal_accounting(
        [
            {
                "originKind": "structural_offspring",
                "disposition": "accepted",
                "proposal": metadata,
            }
        ]
    )
    assert accounting["constructionOperatorFamilyAttemptCounts"] == {
        "graph_bound_indicator_timeframe_v3": 1
    }
    assert accounting["constructionOperatorFamilyApplicationCounts"] == {
        "graph_bound_indicator_timeframe_v3": 1
    }


def test_qd_campaign_write_once_is_true_noop(tmp_path: Path) -> None:
    from autoresearch.temporal_qd_campaign import _write_once

    path = tmp_path / "campaign.json"
    _write_once(path, {"value": 1})
    before = path.stat().st_mtime_ns
    _write_once(path, {"value": 1})
    assert path.stat().st_mtime_ns == before


def test_qd_generation_is_exact_across_full_and_sliced_restart(
    tmp_path: Path, monkeypatch
) -> None:
    member = _member("candidate_parent", 2.0, 1.0)
    member["archiveLane"] = "quality"
    member["paretoFront"] = 0
    member["crowdingDistance"] = 0.0
    archive = {
        "schemaVersion": qd_module.QD_ARCHIVE_SCHEMA,
        "qdVersion": qd_module.QD_VERSION,
        "generationIndex": 0,
        "populationSha256": canonical_sha256({"population": "fixture"}),
        "resultSetSha256": canonical_sha256({"results": "fixture"}),
        "previousArchiveSha256": None,
        "policyName": qd_module.QD_POLICY_NAME,
        "policySha256": qd_module.QD_POLICY_SHA256,
        "cellCapacity": 4,
        "objectives": [],
        "candidateCountSeen": 1,
        "occupiedCellCount": 1,
        "memberCount": 1,
        "qualityMemberCount": 1,
        "observationalMemberCount": 0,
        "negativeNoveltyMemberCount": 0,
        "cells": [
            {
                "cellId": "one-cell",
                "descriptor": member["descriptor"],
                "candidateCountBeforeCapacity": 1,
                "qualityEligibleCountBeforeCapacity": 1,
                "negativeNoveltyEligibleCountBeforeCapacity": 0,
                "observationalCountBeforeCapacity": 0,
                "breedingEligibleMemberCount": 1,
                "negativeNoveltyMemberCount": 0,
                "selectionVisitCount": 0,
                "offspringAttemptCount": 0,
                "members": [member],
            }
        ],
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    archive_path = tmp_path / "archive.json"
    archive_path.write_text(
        json.dumps(archive, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    class FakeValidator:
        def __init__(self, command, *, timeout_seconds):
            assert command == ["fake-validator"]
            assert timeout_seconds == 60.0

        def validate(
            self,
            *,
            candidate_id,
            source_profile,
            expected_raw_source_profile_sha256,
        ):
            assert (
                canonical_sha256(source_profile) == expected_raw_source_profile_sha256
            )
            program_sha = canonical_sha256({"program": source_profile})
            snapshot_sha = canonical_sha256({"snapshot": source_profile})
            return {
                "candidateAcceptable": True,
                "status": "valid_evaluable",
                "programSha256": program_sha,
                "profileSnapshotSha256": snapshot_sha,
                "validationReportSha256": canonical_sha256(
                    {
                        "candidateId": candidate_id,
                        "programSha256": program_sha,
                        "snapshotSha256": snapshot_sha,
                    }
                ),
                "issues": [],
            }

    monkeypatch.setattr(qd_module, "SubprocessCandidateValidator", FakeValidator)

    class FakeContinuation:
        def __init__(self, **kwargs):
            self._next = int(kwargs["start_continuation_ordinal"])
            self.source_identity = {
                "schemaVersion": "fixture_source_v1",
                "sourceIdentitySha256": canonical_sha256({"fixture": "source"}),
            }

        @property
        def next_continuation_ordinal(self):
            return self._next

        def next_proposal(self):
            ordinal = self._next
            self._next += 1
            profile, _ = _repair_profile(_profile(), parameters=GENERATOR_V2_PARAMETERS)
            profile["name"] = f"immigrant_{ordinal}"
            profile["description"] = f"immigrant fixture {ordinal}"
            reachability = inspect_management_reachability(profile)
            value = {
                "schemaVersion": "temporal_generator_v2_qd_immigrant_proposal_v1",
                "sourceIdentitySha256": self.source_identity["sourceIdentitySha256"],
                "continuationOrdinal": ordinal,
                "generatorProposalOrdinal": 1000 + ordinal,
                "sourceMode": "seed_derived",
                "seedId": "fixture_seed",
                "rawSourceProfile": profile,
                "rawSourceProfileSha256": canonical_sha256(profile),
                "mutations": [{"family": "fixture", "ordinal": ordinal}],
                "activationAwareRepairs": [],
                "managementReachability": reachability,
            }
            value["immigrantProposalSha256"] = canonical_sha256(value)
            return value

    monkeypatch.setattr(qd_module, "ExactGeneratorV2Continuation", FakeContinuation)
    parameters = {
        **qd_module.DEFAULT_QD_PARAMETERS,
        "targetUniqueCandidates": 5,
        "maxProposalAttempts": 256,
    }
    full = qd_module.generate_qd_generation(
        parent_archive_path=archive_path,
        source_preparation_path=tmp_path / "source.json",
        base_generator_root=tmp_path / "base",
        confirmed_entry_admission_root=tmp_path / "admission",
        validator_command=["fake-validator"],
        output_root=tmp_path / "full",
        generation_index=1,
        parameters=parameters,
    )
    partial = qd_module.generate_qd_generation(
        parent_archive_path=archive_path,
        source_preparation_path=tmp_path / "source.json",
        base_generator_root=tmp_path / "base",
        confirmed_entry_admission_root=tmp_path / "admission",
        validator_command=["fake-validator"],
        output_root=tmp_path / "resumed",
        generation_index=1,
        parameters=parameters,
        max_new_proposals=2,
    )
    assert partial["completed"] is False
    resumed = qd_module.generate_qd_generation(
        parent_archive_path=archive_path,
        source_preparation_path=tmp_path / "source.json",
        base_generator_root=tmp_path / "base",
        confirmed_entry_admission_root=tmp_path / "admission",
        validator_command=["fake-validator"],
        output_root=tmp_path / "resumed",
        generation_index=1,
        parameters=parameters,
    )
    assert full["populationSha256"] == resumed["populationSha256"]
    assert full["journalSha256"] == resumed["journalSha256"]
    assert sum(full["originAcceptedCounts"].values()) == 5
    assert full["originProposalCounts"]["random_immigrant"] >= 1
