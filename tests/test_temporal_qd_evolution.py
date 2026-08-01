from __future__ import annotations

import json
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


def _member(candidate_id: str, net_r: float, drawdown: float) -> dict:
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
        "aggregate": {"totalTrades": 10},
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
            "riskAdjustedReturn": net_r,
            "maximumDrawdownR": drawdown,
            "evidenceSupport": 10.0,
            "structuralComplexity": 10.0,
        },
        "finiteDataValidity": {"validForPareto": True},
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
    invalid["finiteDataValidity"] = {"validForPareto": False}
    cells = select_qd_archive([invalid, valid], cell_capacity=8)
    assert [item["candidateId"] for item in cells[0]["members"]] == ["candidate_valid"]


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


def test_qd_generation_is_exact_across_full_and_sliced_restart(
    tmp_path: Path, monkeypatch
) -> None:
    member = _member("candidate_parent", 2.0, 1.0)
    archive = {
        "schemaVersion": qd_module.QD_ARCHIVE_SCHEMA,
        "qdVersion": qd_module.QD_VERSION,
        "generationIndex": 0,
        "populationSha256": canonical_sha256({"population": "fixture"}),
        "resultSetSha256": canonical_sha256({"results": "fixture"}),
        "previousArchiveSha256": None,
        "cellCapacity": 8,
        "objectives": [],
        "candidateCountSeen": 1,
        "occupiedCellCount": 1,
        "memberCount": 1,
        "paretoEligibleMemberCount": 1,
        "cells": [
            {
                "cellId": "one-cell",
                "descriptor": member["descriptor"],
                "candidateCountBeforeCapacity": 1,
                "finiteDataEligibleCountBeforeCapacity": 1,
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
