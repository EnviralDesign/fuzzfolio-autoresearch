from pathlib import Path
import json

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_operator_family_matrix import (
    DEFAULT_CHILDREN_PER_FAMILY,
    MATRIX_FAMILIES,
    MATRIX_SCHEMA,
    QUALIFICATION_REPORT_SCHEMA,
    attach_operator_family_matrix,
    construction_slot_count,
    evolved_fill_matches_manifest,
    freeze_operator_family_matrix_from_run,
    iter_construction_slots,
    score_operator_family_matrix,
    slot_at,
    validate_operator_family_matrix,
)
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA


def _contract(
    *, children: int = 2, parents: list[dict[str, str]] | None = None
) -> dict[str, object]:
    return {
        "schemaVersion": MATRIX_SCHEMA,
        "mode": "frozen_parent_one_change_v1",
        "includeCrossover": False,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "mutationDepth": 1,
        "childrenPerFamily": children,
        "families": ["hold", "resource"],
        "parents": parents
        or [
            {"candidateId": "parent_archive_a", "role": "archive"},
            {"candidateId": "parent_inactive", "role": "inactive_control"},
            {"candidateId": "parent_negative", "role": "active_negative_control"},
        ],
    }


def _evaluated_row(candidate_id: str, nets: list[float], trades: list[int]) -> dict[str, object]:
    return {
        "candidateId": candidate_id,
        "aggregate": {
            "windowRecords": [
                {"metrics": {"conservativeNetR": net, "closedTrades": trade}}
                for net, trade in zip(nets, trades, strict=True)
            ]
        },
    }


def test_matrix_rejects_crossover_and_production_depth_mix() -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="crossover"):
        validate_operator_family_matrix({**_contract(), "includeCrossover": True})
    with pytest.raises(TemporalDiscoveryContractError, match="mutation depth"):
        validate_operator_family_matrix({**_contract(), "mutationDepth": 2})


def test_construction_slots_are_balanced_and_one_change() -> None:
    contract = validate_operator_family_matrix(_contract(children=2))
    assert construction_slot_count(contract) == 12
    slots = iter_construction_slots(contract)
    assert [slot["proposalOrdinal"] for slot in slots] == list(range(12))
    assert slots[0] == {
        "proposalOrdinal": 0,
        "kind": "one_change",
        "parentCandidateId": "parent_archive_a",
        "parentRole": "archive",
        "operatorFamily": "hold",
        "childIndex": 0,
        "mutationDepth": 1,
    }
    assert slots[3]["operatorFamily"] == "resource"
    assert slots[3]["parentCandidateId"] == "parent_archive_a"
    assert slots[4]["parentCandidateId"] == "parent_inactive"
    assert slots[4]["operatorFamily"] == "hold"
    families_by_parent: dict[str, list[str]] = {}
    for slot in slots:
        families_by_parent.setdefault(slot["parentCandidateId"], []).append(
            slot["operatorFamily"]
        )
    for families in families_by_parent.values():
        assert families.count("hold") == 2
        assert families.count("resource") == 2
    assert slot_at(contract, 12) is None


def test_attach_overlay_does_not_keep_rotating_schedule_or_confidence() -> None:
    base = {
        "schemaVersion": PAIR_GENERATION_SCHEMA,
        "generationIndex": 2,
        "targetUniqueCandidates": 1024,
        "maxProposalAttempts": 4000,
        "runConfig": {"parentArchiveSha256": "sha256:" + ("a" * 64)},
        "pairPolicy": {"schemaVersion": "test"},
        "operatorImplementation": {"schemaVersion": "test"},
        "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
        "parentSchedule": {"schemaVersion": "temporal_qd_rotating_parent_schedule_v2"},
        "breedingConfidenceReceipt": {"desiredOffspringCandidateCount": 819},
        "reproductionAllocation": {
            "schemaVersion": "temporal_qd_reproduction_allocation_v2",
            "desiredAcceptedOffspringCount": 819,
            "desiredAcceptedImmigrantCount": 205,
        },
    }
    overlay = attach_operator_family_matrix(base, _contract(children=2))
    assert "parentSchedule" not in overlay
    assert "breedingConfidenceReceipt" not in overlay
    assert overlay["targetUniqueCandidates"] == 12
    assert overlay["maxProposalAttempts"] == 12
    assert overlay["mutationDepthProbabilities"] == {"1": 1.0}
    allocation = overlay["reproductionAllocation"]
    assert allocation["desiredAcceptedImmigrantCount"] == 0
    assert allocation["desiredAcceptedOffspringCount"] == 12
    assert allocation["minimumImmigrantNumerator"] == 0
    assert allocation["minimumImmigrantDenominator"] == 1
    hashed = {key: value for key, value in overlay.items() if key != "configSha256"}
    assert overlay["configSha256"] == canonical_sha256(hashed)


def test_attach_does_not_mutate_production_base_config() -> None:
    base = {
        "schemaVersion": PAIR_GENERATION_SCHEMA,
        "generationIndex": 2,
        "targetUniqueCandidates": 1024,
        "maxProposalAttempts": 4000,
        "runConfig": {"parentArchiveSha256": "sha256:" + ("a" * 64)},
        "pairPolicy": {"schemaVersion": "test"},
        "operatorImplementation": {"schemaVersion": "test"},
        "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
        "parentSchedule": {"schemaVersion": "temporal_qd_rotating_parent_schedule_v2"},
        "breedingConfidenceReceipt": {"desiredOffspringCandidateCount": 819},
        "reproductionAllocation": {
            "schemaVersion": "temporal_qd_reproduction_allocation_v2",
            "desiredAcceptedOffspringCount": 819,
            "desiredAcceptedImmigrantCount": 205,
        },
    }
    original = {
        "schemaVersion": PAIR_GENERATION_SCHEMA,
        "generationIndex": 2,
        "targetUniqueCandidates": 1024,
        "maxProposalAttempts": 4000,
        "runConfig": {"parentArchiveSha256": "sha256:" + ("a" * 64)},
        "pairPolicy": {"schemaVersion": "test"},
        "operatorImplementation": {"schemaVersion": "test"},
        "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
        "parentSchedule": {"schemaVersion": "temporal_qd_rotating_parent_schedule_v2"},
        "breedingConfidenceReceipt": {"desiredOffspringCandidateCount": 819},
        "reproductionAllocation": {
            "schemaVersion": "temporal_qd_reproduction_allocation_v2",
            "desiredAcceptedOffspringCount": 819,
            "desiredAcceptedImmigrantCount": 205,
        },
    }
    attach_operator_family_matrix(base, _contract(children=2))
    assert base == original
    assert "operatorFamilyMatrix" not in base


def test_freeze_spec_pins_archive_plus_controls(tmp_path: Path) -> None:
    generation = tmp_path / "generations" / "generation-0002" / "native-finalization"
    generation.mkdir(parents=True)
    archive = {
        "schemaVersion": "test-archive",
        "cells": [
            {"cellId": "cell-a", "members": [{"candidateId": "qd_archive_one"}]},
            {"cellId": "cell-b", "members": [{"candidateId": "qd_archive_two"}]},
        ],
    }
    (generation / "archive.json").write_text(
        canonical_json(archive) + "\n", encoding="utf-8", newline="\n"
    )
    evaluated_root = (
        tmp_path
        / "generations"
        / "generation-0002"
        / "campaign"
        / "proposal-current-panel"
        / "campaign-output"
    )
    evaluated_root.mkdir(parents=True)
    rows = [
        _evaluated_row("qd_archive_one", [1.0, 1.0], [2, 2]),
        _evaluated_row("qd_archive_two", [0.5, 0.5], [1, 1]),
        _evaluated_row("qd_inactive", [0.0, 0.0], [0, 0]),
        _evaluated_row("qd_active_negative", [-3.0, -1.0], [4, 3]),
        _evaluated_row("qd_active_positive", [2.0, 1.0], [3, 2]),
    ]
    (evaluated_root / "evaluated-members.jsonl").write_text(
        "".join(canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )
    contract = freeze_operator_family_matrix_from_run(
        tmp_path, source_generation=2, children_per_family=1, families=["hold"]
    )
    assert [parent["candidateId"] for parent in contract["parents"]] == [
        "qd_archive_one",
        "qd_archive_two",
        "qd_inactive",
        "qd_active_negative",
    ]
    assert contract["constructionSlotCount"] == 4
    assert DEFAULT_CHILDREN_PER_FAMILY == 32
    assert contract["families"] == ["hold"]


def test_qualification_report_uses_same_panel_parent_and_worst_window() -> None:
    contract = validate_operator_family_matrix(_contract(children=1))
    evaluated = {
        "parent_archive_a": _evaluated_row("parent_archive_a", [2.0, 1.0], [2, 2]),
        "parent_inactive": _evaluated_row("parent_inactive", [0.0, 0.0], [0, 0]),
        "parent_negative": _evaluated_row("parent_negative", [-1.0, -2.0], [3, 3]),
        "child_hold": _evaluated_row("child_hold", [3.0, 2.0], [2, 2]),
        "child_resource": _evaluated_row("child_resource", [-4.0, -5.0], [8, 8]),
    }
    slots = [
        {
            "candidateId": "child_hold",
            "parentCandidateId": "parent_archive_a",
            "operatorFamily": "hold",
        },
        {
            "candidateId": "child_resource",
            "parentCandidateId": "parent_archive_a",
            "operatorFamily": "resource",
        },
    ]
    report = score_operator_family_matrix(
        contract=contract, evaluated=evaluated, constructed_by_slot=slots
    )
    assert report["schemaVersion"] == QUALIFICATION_REPORT_SCHEMA
    by_family = {row["operatorFamily"]: row for row in report["familyYield"]}
    hold = by_family["hold"]["samePanelParentRelative"]
    resource = by_family["resource"]["samePanelParentRelative"]
    assert hold["medianParentRelativeConservativeNetR"] == 2.0
    assert hold["offspringBeatParentCount"] == 1
    assert resource["medianParentRelativeConservativeNetR"] == -12.0
    assert "hold" in report["passingFamilies"]
    assert "resource" not in report["passingFamilies"]
    assert report["qualification"] == "pass"
    assert "clone_control_is_parent_re_eval_on_frozen_panel" in report["limitations"]


def test_full_default_matrix_size() -> None:
    parents = [{"candidateId": f"p{index}", "role": "archive"} for index in range(3)] + [
        {"candidateId": "inactive", "role": "inactive_control"},
        {"candidateId": "negative", "role": "active_negative_control"},
    ]
    contract = validate_operator_family_matrix(
        {
            **_contract(children=DEFAULT_CHILDREN_PER_FAMILY),
            "families": list(MATRIX_FAMILIES),
            "parents": parents,
        }
    )
    assert construction_slot_count(contract) == 5 * 5 * 32
    assert contract["cloneParentCandidateIds"] == [
        "p0",
        "p1",
        "p2",
        "inactive",
        "negative",
    ]


def test_matrix_incomplete_fill_is_complete_after_every_slot() -> None:
    contract = validate_operator_family_matrix(_contract(children=1))
    slots = construction_slot_count(contract)
    config = {"operatorFamilyMatrix": contract}
    assert evolved_fill_matches_manifest(
        generation_config=config,
        requested_count=slots,
        max_attempts=slots,
        declared_evaluation_population_size=slots,
        accepted_count=1,
        attempt_count=slots,
        evaluation_population_size=1,
    )
    assert evolved_fill_matches_manifest(
        generation_config=config,
        requested_count=slots,
        max_attempts=slots,
        declared_evaluation_population_size=slots,
        accepted_count=0,
        attempt_count=slots,
        evaluation_population_size=0,
    )
    assert not evolved_fill_matches_manifest(
        generation_config=config,
        requested_count=slots,
        max_attempts=slots,
        declared_evaluation_population_size=slots,
        accepted_count=1,
        attempt_count=slots - 1,
        evaluation_population_size=1,
    )


def test_production_fill_still_requires_exact_width() -> None:
    assert evolved_fill_matches_manifest(
        generation_config={},
        requested_count=8,
        max_attempts=10,
        declared_evaluation_population_size=8,
        accepted_count=8,
        attempt_count=9,
        evaluation_population_size=8,
    )
    assert not evolved_fill_matches_manifest(
        generation_config={},
        requested_count=8,
        max_attempts=10,
        declared_evaluation_population_size=8,
        accepted_count=7,
        attempt_count=10,
        evaluation_population_size=7,
    )


def test_unfilled_slots_score_as_missing_children() -> None:
    contract = validate_operator_family_matrix(_contract(children=1))
    evaluated = {
        "parent_archive_a": _evaluated_row("parent_archive_a", [2.0, 1.0], [2, 2]),
        "parent_inactive": _evaluated_row("parent_inactive", [0.0, 0.0], [0, 0]),
        "parent_negative": _evaluated_row("parent_negative", [-1.0, -2.0], [3, 3]),
        "child_hold": _evaluated_row("child_hold", [3.0, 2.0], [2, 2]),
    }
    report = score_operator_family_matrix(
        contract=contract,
        evaluated=evaluated,
        constructed_by_slot=[
            {
                "candidateId": "child_hold",
                "parentCandidateId": "parent_archive_a",
                "operatorFamily": "hold",
            }
        ],
    )
    by_family = {row["operatorFamily"]: row for row in report["familyYield"]}
    assert by_family["hold"]["evaluatedChildCount"] == 1
    assert by_family["resource"]["evaluatedChildCount"] == 0
    assert "unfilled_slots_remain_on_their_declared_family" in report["limitations"]


def test_unique_evaluations_allow_matrix_accept_deficit() -> None:
    from autoresearch.temporal_qd_operator_family_matrix import unique_evaluations_meet_plan

    contract = _contract(children=2)
    assert unique_evaluations_meet_plan(
        matrix=None, unique_count=12, target_unique_evaluations=12
    )
    assert not unique_evaluations_meet_plan(
        matrix=None, unique_count=11, target_unique_evaluations=12
    )
    assert unique_evaluations_meet_plan(
        matrix=contract, unique_count=0, target_unique_evaluations=12
    )
    assert unique_evaluations_meet_plan(
        matrix=contract, unique_count=7, target_unique_evaluations=12
    )
    assert not unique_evaluations_meet_plan(
        matrix=contract, unique_count=13, target_unique_evaluations=12
    )
    assert not unique_evaluations_meet_plan(
        matrix=contract, unique_count=7, target_unique_evaluations=1024
    )


def test_matrix_source_identity_ledger_descriptor_uses_complete_sidecar(
    tmp_path: Path,
) -> None:
    from autoresearch.temporal_qd_operator_family_matrix import (
        FAST_EPHEMERAL_COMPLETE_SCHEMA,
        matrix_source_identity_ledger_descriptor,
    )

    proposal = tmp_path / "generations" / "generation-0002" / "proposal"
    proposal.mkdir(parents=True)
    ledger = proposal / "identity-ledger.json"
    ledger.write_bytes(b'{"schemaVersion":"test-ledger"}\n')
    complete = {
        "schemaVersion": FAST_EPHEMERAL_COMPLETE_SCHEMA,
        "executionMode": "fast-ephemeral-v1",
        "generationIndex": 2,
        "artifacts": {
            "identityLedger": {
                "byteLength": ledger.stat().st_size,
                "fileSha256": "sha256:" + ("a" * 64),
                "relativePath": "identity-ledger.json",
                "semanticSha256": "sha256:" + ("b" * 64),
            }
        },
    }
    (proposal / "COMPLETE.json").write_text(
        json.dumps(complete), encoding="utf-8"
    )
    contract = {
        **_contract(children=1),
        "sourceRunRoot": str(tmp_path),
        "sourceGenerationIndex": 2,
    }
    descriptor = matrix_source_identity_ledger_descriptor(contract)
    assert descriptor["absolutePath"] == str(ledger.resolve())
    assert descriptor["byteLength"] == ledger.stat().st_size
    assert descriptor["fileSha256"].startswith("sha256:")

