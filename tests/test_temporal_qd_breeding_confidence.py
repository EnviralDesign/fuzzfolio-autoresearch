"""Breeding-confidence offspring/immigrant budget freeze."""

from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_qd_pair_generation import (
    BREEDING_CONFIDENCE_POLICY_V1,
    breeding_confidence_quota_counts,
    build_pair_generation_config,
    select_breeding_confidence,
)


def _member(*, panel_ids: list[str], eligible: bool = True, lane: str = "quality") -> dict:
    return {
        "archiveLane": lane,
        "robustBreederEligible": eligible,
        "cumulativeEvidenceArchiveSha256": "sha256:" + "a" * 64,
        "cumulativeEvidence": {"requiredPanelIds": list(panel_ids)},
    }


def _archive(members: list[dict], *, generation_index: int = 3) -> dict:
    return {
        "generationIndex": generation_index,
        "cells": [{"cellId": "cell-0", "members": members}],
        "rotatingEvidenceTransaction": {
            "cumulativeArchiveSha256": "sha256:" + "a" * 64,
        },
    }


def _v5_run_config() -> dict:
    return {
        "archivePolicyAuthority": {"qdVersion": "temporal_qd_evolution_v5"},
        "runId": "breeding-confidence-fixture",
    }


def test_empty_archive_is_immigrants_only() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([]),
        target_unique_candidates=1024,
    )
    assert confidence["desiredOffspringCandidateCount"] == 0
    assert confidence["desiredImmigrantCandidateCount"] == 1024
    assert confidence["receipt"]["reason"] == "empty_archive_immigrants_only"
    assert confidence["immigrantNumerator"] == 1
    assert confidence["immigrantDenominator"] == 1


def test_one_panel_archive_is_twenty_percent_offspring() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([_member(panel_ids=["p1"])]),
        target_unique_candidates=1024,
    )
    assert confidence["desiredOffspringCandidateCount"] == 205
    assert confidence["desiredImmigrantCandidateCount"] == 819
    assert confidence["receipt"]["reason"] == "one_panel_parent_exploration_only"
    assert confidence["immigrantNumerator"] == 4
    assert confidence["immigrantDenominator"] == 5


def test_two_panel_archive_is_balanced() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([_member(panel_ids=["p1", "p2"])]),
        target_unique_candidates=1024,
    )
    assert confidence["desiredOffspringCandidateCount"] == 512
    assert confidence["desiredImmigrantCandidateCount"] == 512
    assert confidence["receipt"]["reason"] == "two_panel_parent_balanced_exploration"


def test_three_panel_archive_is_eighty_percent_offspring() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([_member(panel_ids=["p1", "p2", "p3"])]),
        target_unique_candidates=1024,
    )
    assert confidence["desiredOffspringCandidateCount"] == 819
    assert confidence["desiredImmigrantCandidateCount"] == 205
    assert confidence["receipt"]["reason"] == "three_plus_panel_parent_exploitation"


def test_four_panel_archive_matches_three_plus_tier() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([_member(panel_ids=["p1", "p2", "p3", "p4"])]),
        target_unique_candidates=1024,
    )
    assert confidence["desiredOffspringCandidateCount"] == 819
    assert confidence["desiredImmigrantCandidateCount"] == 205


def test_mixed_one_and_two_panel_parents_use_minimum() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive(
            [
                _member(panel_ids=["p1"]),
                _member(panel_ids=["p1", "p2"]),
            ]
        ),
        target_unique_candidates=1024,
    )
    receipt = confidence["receipt"]
    assert receipt["minimumQualifiedPanelCount"] == 1
    assert receipt["maximumQualifiedPanelCount"] == 2
    assert confidence["desiredOffspringCandidateCount"] == 205
    assert confidence["desiredImmigrantCandidateCount"] == 819
    assert receipt["reason"] == "one_panel_parent_exploration_only"


def test_malformed_panel_evidence_fails_closed() -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="panel evidence"):
        select_breeding_confidence(
            parent_archive=_archive(
                [
                    {
                        "archiveLane": "quality",
                        "robustBreederEligible": True,
                        "cumulativeEvidenceArchiveSha256": "sha256:" + "a" * 64,
                        "cumulativeEvidence": {"requiredPanelIds": []},
                    }
                ]
            ),
            target_unique_candidates=128,
        )


@pytest.mark.parametrize("target", (1, 5, 128, 1024))
def test_desired_counts_sum_exactly_to_target(target: int) -> None:
    for panels in (0, 1, 2, 3, 4):
        members = (
            []
            if panels == 0
            else [_member(panel_ids=[f"p{index}" for index in range(1, panels + 1)])]
        )
        confidence = select_breeding_confidence(
            parent_archive=_archive(members),
            target_unique_candidates=target,
        )
        assert (
            confidence["desiredOffspringCandidateCount"]
            + confidence["desiredImmigrantCandidateCount"]
            == target
        )


def test_one_panel_target_one_rounds_to_zero_offspring() -> None:
    offspring, immigrants = breeding_confidence_quota_counts(
        1, offspring_numerator=1, offspring_denominator=5
    )
    assert (offspring, immigrants) == (0, 1)


def test_same_config_and_archive_are_byte_identical() -> None:
    archive = _archive([_member(panel_ids=["p1", "p2"])])
    first = select_breeding_confidence(
        parent_archive=archive, target_unique_candidates=128
    )
    second = select_breeding_confidence(
        parent_archive=copy.deepcopy(archive), target_unique_candidates=128
    )
    assert first["receipt"] == second["receipt"]
    config_a = build_pair_generation_config(
        generation_index=4,
        target_unique_candidates=128,
        max_proposal_attempts=256,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=True,
        breeding_confidence=first,
    )
    config_b = build_pair_generation_config(
        generation_index=4,
        target_unique_candidates=128,
        max_proposal_attempts=256,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=True,
        breeding_confidence=second,
    )
    assert config_a == config_b
    assert config_a["configSha256"] == config_b["configSha256"]
    assert config_a["breedingConfidenceReceipt"] == first["receipt"]


def test_policy_change_changes_config_identity() -> None:
    archive = _archive([_member(panel_ids=["p1"])])
    baseline = select_breeding_confidence(
        parent_archive=archive, target_unique_candidates=128
    )
    altered_policy = copy.deepcopy(BREEDING_CONFIDENCE_POLICY_V1)
    altered_policy["tiers"][0]["offspringProposalFraction"] = 0.50
    altered = select_breeding_confidence(
        parent_archive=archive,
        target_unique_candidates=128,
        policy=altered_policy,
    )
    # Quota math still uses frozen rationals; identity must still move when the
    # hashed policy body changes even if counts stay the same for this archive.
    config_a = build_pair_generation_config(
        generation_index=4,
        target_unique_candidates=128,
        max_proposal_attempts=256,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=True,
        breeding_confidence=baseline,
    )
    config_b = build_pair_generation_config(
        generation_index=4,
        target_unique_candidates=128,
        max_proposal_attempts=256,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=True,
        breeding_confidence=altered,
    )
    assert config_a["configSha256"] != config_b["configSha256"]
    assert canonical_sha256(
        {key: value for key, value in config_a.items() if key != "configSha256"}
    ) == config_a["configSha256"]


def test_fast_ephemeral_and_durable_read_same_frozen_allocation() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive([_member(panel_ids=["p1"])]),
        target_unique_candidates=1024,
    )
    config = build_pair_generation_config(
        generation_index=4,
        target_unique_candidates=1024,
        max_proposal_attempts=2048,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=True,
        breeding_confidence=confidence,
    )
    allocation = config["reproductionAllocation"]
    assert allocation["desiredAcceptedOffspringCount"] == 205
    assert allocation["desiredAcceptedImmigrantCount"] == 819
    # Both execution modes consume reproductionAllocation only; there is no
    # second interpretation of the sealed counts.
    assert (
        allocation["desiredAcceptedOffspringCount"]
        == config["breedingConfidenceReceipt"]["desiredOffspringCandidateCount"]
    )
    assert (
        allocation["desiredAcceptedImmigrantCount"]
        == config["breedingConfidenceReceipt"]["desiredImmigrantCandidateCount"]
    )


def test_empty_quality_bootstrap_remains_immigrants_only() -> None:
    confidence = select_breeding_confidence(
        parent_archive=None, target_unique_candidates=128
    )
    config = build_pair_generation_config(
        generation_index=1,
        target_unique_candidates=128,
        max_proposal_attempts=256,
        run_config=_v5_run_config(),
        pair_policy={"enabled": True},
        operator_implementation_identity={"fixture": True},
        parent_archive=None,
        immigrant_construction_policy=None,
        global_identity_ledger_enabled=False,
        breeding_confidence=confidence,
    )
    assert config["reproductionAllocation"]["desiredAcceptedOffspringCount"] == 0
    assert config["reproductionAllocation"]["desiredAcceptedImmigrantCount"] == 128
    assert config["breedingConfidenceReceipt"]["reason"] == "empty_archive_immigrants_only"


def test_duplicate_panel_ids_count_as_distinct() -> None:
    confidence = select_breeding_confidence(
        parent_archive=_archive(
            [_member(panel_ids=["p1", "p1", "p2", "p2", "p2"])]
        ),
        target_unique_candidates=100,
    )
    assert confidence["receipt"]["minimumQualifiedPanelCount"] == 2
    assert confidence["desiredOffspringCandidateCount"] == 50
