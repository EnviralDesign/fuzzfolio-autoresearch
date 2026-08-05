from __future__ import annotations

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_pair_generation as pair_generation
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
import pytest


def _rotating_schedule_archive(*, parent_count: int, breeder_width: int) -> dict:
    if parent_count * 5 < breeder_width * 4:
        numerator, denominator = parent_count, breeder_width
    else:
        numerator, denominator = 4, 5
    schedule = {
        "schemaVersion": "temporal_qd_rotating_parent_schedule_v1",
        "breederWidth": breeder_width,
        "breederParentCount": parent_count,
        "maximumOffspringNumerator": 4,
        "maximumOffspringDenominator": 5,
        "offspringNumerator": numerator,
        "offspringDenominator": denominator,
        "immigrantsFillUnsupportedShare": True,
        "schedulingMethod": "deterministic_rational_prefix_balance",
    }
    schedule["scheduleSha256"] = canonical_sha256(schedule)
    return {
        "cells": ([{"members": [{} for _ in range(parent_count)]}] if parent_count else []),
        "rotatingEvidenceTransaction": {"parentSchedule": schedule},
    }


def test_live_pair_generation_forwards_the_validated_qd_parent_archive(tmp_path, monkeypatch) -> None:
    archive = {"cells": [], "archiveSha256": "sha256:" + "a" * 64}
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
        "policySha256": "sha256:" + "b" * 64,
    }
    seen = {}
    monkeypatch.setattr(qd, "_load_archive", lambda _: (archive, archive["archiveSha256"]))
    monkeypatch.setattr(qd, "_bidirectional_pair_policy", lambda _: policy)

    def fake_population(**kwargs):
        seen.update(kwargs)
        return {"completed": True}

    monkeypatch.setattr(pair_generation, "generate_pair_population", fake_population)
    result = qd.generate_qd_generation(
        parent_archive_path=tmp_path / "parent.json",
        output_root=tmp_path / "out",
        generation_index=1,
        parameters={**qd.DEFAULT_QD_PARAMETERS, "targetUniqueCandidates": 1},
        bidirectional_pair_policy=policy,
        bidirectional_pair_factory=object(),
        bidirectional_module_authority=object(),
        bidirectional_native_validator=object(),
        bidirectional_pair_compiler=object(),
        bidirectional_operator_implementation_identity={"fixture": True},
    )
    assert result == {"completed": True}
    assert seen["parent_archive"] is archive
    assert seen["identity_ledger_path"] == tmp_path / "out" / "identity-ledger.json"
    assert seen["max_proposal_attempts"] == qd.DEFAULT_QD_PARAMETERS["maxProposalAttempts"]


def test_legacy_parent_archive_keeps_exact_four_to_one_origin_schedule() -> None:
    origins = [
        pair_generation._scheduled_immigrant(
            has_parents=True, proposal_ordinal=ordinal, parent_schedule=None
        )
        for ordinal in range(10)
    ]
    assert [index for index, immigrant in enumerate(origins) if immigrant] == [4, 9]


def test_frontier_only_rotating_archive_earns_only_supported_offspring_share() -> None:
    archive = _rotating_schedule_archive(parent_count=25, breeder_width=128)
    schedule = pair_generation._rotating_parent_schedule(archive)
    offspring = sum(
        not pair_generation._scheduled_immigrant(
            has_parents=True,
            proposal_ordinal=ordinal,
            parent_schedule=schedule,
        )
        for ordinal in range(128)
    )
    assert offspring == 25


def test_sparse_quality_rotating_archive_fills_unsupported_share_with_immigrants() -> None:
    archive = _rotating_schedule_archive(parent_count=1, breeder_width=128)
    schedule = pair_generation._rotating_parent_schedule(archive)
    offspring = sum(
        not pair_generation._scheduled_immigrant(
            has_parents=True,
            proposal_ordinal=ordinal,
            parent_schedule=schedule,
        )
        for ordinal in range(128)
    )
    assert offspring == 1


def test_rotating_parent_schedule_rejects_self_hashed_under_scheduling() -> None:
    archive = _rotating_schedule_archive(parent_count=25, breeder_width=128)
    schedule = archive["rotatingEvidenceTransaction"]["parentSchedule"]
    schedule["offspringNumerator"] = 0
    schedule["scheduleSha256"] = canonical_sha256(
        {key: value for key, value in schedule.items() if key != "scheduleSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="policy is invalid"):
        pair_generation._rotating_parent_schedule(archive)
