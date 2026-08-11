from __future__ import annotations

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_pair_generation as pair_generation
import autoresearch.temporal_qd_native as native
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
import pytest


def _rotating_schedule_archive(*, parent_count: int, breeder_width: int) -> dict:
    schedule = {
        "schemaVersion": "temporal_qd_rotating_parent_schedule_v2",
        "breederWidth": breeder_width,
        "breederParentCount": parent_count,
        "minimumImmigrantNumerator": 1,
        "minimumImmigrantDenominator": 5,
        "parentSampling": "with_replacement_supported_parents_v1",
        "unsupportedParentPolicy": "immigrant_only_authority_bound_v1",
        "schedulingMethod": "accepted_quota_prefix_balance_v1",
    }
    schedule["scheduleSha256"] = canonical_sha256(schedule)
    return {
        "cells": ([{"members": [{} for _ in range(parent_count)]}] if parent_count else []),
        "rotatingEvidenceTransaction": {"parentSchedule": schedule},
    }


def _legacy_v1_schedule(*, parent_count: int, breeder_width: int) -> dict:
    numerator, denominator = (
        (parent_count, breeder_width)
        if parent_count * 5 < breeder_width * 4
        else (4, 5)
    )
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
    return schedule


def test_live_pair_generation_forwards_the_validated_qd_parent_archive(tmp_path, monkeypatch) -> None:
    archive = {
        "cells": [],
        "archiveSha256": "sha256:" + "a" * 64,
        "policyName": qd.QD_POLICY_NAME,
        "policySha256": qd.QD_POLICY_SHA256,
        "frozenPolicy": qd.QD_POLICY,
    }
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


def test_fresh_v5_pair_generation_requires_and_forwards_the_exact_authority(
    tmp_path, monkeypatch
) -> None:
    authority = qd.directional_qd_archive_policy_authority()
    archive = {
        "cells": [],
        "archiveSha256": "sha256:" + "a" * 64,
        **authority,
    }
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
        "policySha256": "sha256:" + "b" * 64,
    }
    seen = {}
    monkeypatch.setattr(qd, "_load_archive", lambda _: (archive, archive["archiveSha256"]))
    monkeypatch.setattr(qd, "_bidirectional_pair_policy", lambda _: policy)
    monkeypatch.setattr(
        pair_generation,
        "generate_v5_pair_population_python_oracle",
        lambda **kwargs: seen.update(kwargs) or {"completed": True},
    )
    common = {
        "parent_archive_path": tmp_path / "parent.json",
        "output_root": tmp_path / "out",
        "generation_index": 1,
        "parameters": {**qd.DEFAULT_QD_PARAMETERS, "targetUniqueCandidates": 1},
        "bidirectional_pair_policy": policy,
        "bidirectional_pair_factory": object(),
        "bidirectional_module_authority": object(),
        "bidirectional_native_validator": object(),
        "bidirectional_pair_compiler": object(),
        "bidirectional_operator_implementation_identity": {"fixture": True},
    }
    with pytest.raises(TemporalDiscoveryContractError, match="requires its exact archive policy authority"):
        qd.generate_qd_generation(**common)
    with pytest.raises(TemporalDiscoveryContractError, match="native v5 proposal transaction"):
        qd.generate_qd_generation(**common, archive_policy_authority=authority)
    assert qd.generate_v5_qd_generation_python_oracle(
        **common, archive_policy_authority=authority
    ) == {"completed": True}
    assert seen["archive_policy_authority"] == authority
    assert seen["run_config"]["archivePolicyAuthority"] == authority


def test_native_pair_selection_precomputes_config_and_never_falls_back(
    tmp_path, monkeypatch
) -> None:
    archive = {"cells": [], "archiveSha256": "sha256:" + "a" * 64}
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
        "policySha256": "sha256:" + "b" * 64,
    }

    def forbidden_archive_load(*_args, **_kwargs):
        pytest.fail("native generation must not load or materialize the parent archive")

    def forbidden_pair_materialization(_cls, *_args, **_kwargs):
        pytest.fail("native generation must not materialize FrozenPair parents in Python")

    monkeypatch.setattr(qd, "_load_archive", forbidden_archive_load)
    monkeypatch.setattr(
        qd.FrozenPair,
        "from_payload",
        classmethod(forbidden_pair_materialization),
    )
    monkeypatch.setattr(qd, "_bidirectional_pair_policy", lambda _: policy)
    monkeypatch.setattr(
        qd,
        "_load_identity_ledger",
        lambda _path: {"ledgerSha256": "sha256:" + "c" * 64},
    )
    python_calls = 0

    def python_population(**_kwargs):
        nonlocal python_calls
        python_calls += 1
        return {"completed": True}

    seen = {}

    def native_generation(**kwargs):
        seen.update(kwargs)
        raise native.TemporalQDNativeError("injected native failure")

    monkeypatch.setattr(pair_generation, "generate_pair_population", python_population)
    monkeypatch.setattr(qd, "run_native_generation", native_generation)
    pair_run_config = {
        "pairRunConfigSha256": canonical_sha256({}),
    }
    parent_schedule = _rotating_schedule_archive(
        parent_count=1, breeder_width=1
    )["rotatingEvidenceTransaction"]["parentSchedule"]
    with pytest.raises(TemporalDiscoveryContractError, match="injected native failure"):
        qd.generate_qd_generation(
            parent_archive_path=tmp_path / "parent.json",
            parent_archive_sha256=archive["archiveSha256"],
            parent_schedule=parent_schedule,
            output_root=tmp_path / "out",
            generation_index=1,
            parameters={**qd.DEFAULT_QD_PARAMETERS, "targetUniqueCandidates": 1},
            bidirectional_pair_run_config=pair_run_config,
            qd_publication_authority={
                "qdVersion": qd.QD_VERSION,
                "policyName": qd.QD_POLICY_NAME,
                "policySha256": qd.QD_POLICY_SHA256,
                "frozenPolicy": qd.QD_POLICY,
            },
            bidirectional_pair_policy=policy,
            bidirectional_operator_implementation_identity={"fixture": True},
        )

    assert python_calls == 0
    assert seen["native_execution_timeout_seconds"] == 3600
    config = seen["generation_config"]
    assert config["parentSchedule"] == parent_schedule
    assert seen["runtime_authority"]["pairGenerationConfigSha256"] == config[
        "configSha256"
    ]
    assert seen["parent_archive_sha256"] == archive["archiveSha256"]


def test_native_pair_selection_requires_prevalidated_parent_archive_identity(
    tmp_path,
) -> None:
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
        "policySha256": "sha256:" + "b" * 64,
    }
    runtime = native.build_pair_generation_runtime_config(
        engine=native.PAIR_GENERATION_RUNTIME_RUST,
    )

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="native QD parent archive identity",
    ):
        qd.generate_qd_generation(
            parent_archive_path=tmp_path / "parent.json",
            output_root=tmp_path / "out",
            generation_index=1,
            parameters={**qd.DEFAULT_QD_PARAMETERS, "targetUniqueCandidates": 1},
            pair_generation_runtime=runtime,
            bidirectional_pair_policy=policy,
        )


def test_legacy_parent_archive_keeps_exact_four_to_one_origin_schedule() -> None:
    origins = [
        pair_generation._scheduled_immigrant(
            has_parents=True, proposal_ordinal=ordinal, parent_schedule=None
        )
        for ordinal in range(10)
    ]
    assert [index for index, immigrant in enumerate(origins) if immigrant] == [4, 9]


def test_sparse_rotating_archive_keeps_the_frozen_80_20_evaluated_quota() -> None:
    archive = _rotating_schedule_archive(parent_count=25, breeder_width=128)
    schedule = pair_generation._rotating_parent_schedule(archive)
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=schedule, target_unique_candidates=128
    )
    assert allocation["desiredEvaluatedOffspringCount"] == 102
    assert allocation["desiredEvaluatedImmigrantCount"] == 26


def test_normal_rotating_archive_keeps_the_same_quota_and_parent_reservoir_policy() -> None:
    archive = _rotating_schedule_archive(parent_count=128, breeder_width=128)
    schedule = pair_generation._rotating_parent_schedule(archive)
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=schedule, target_unique_candidates=128
    )
    assert allocation["desiredEvaluatedOffspringCount"] == 102
    assert allocation["parentSampling"] == "with_replacement_supported_parents_v1"


def test_allocation_retries_the_same_origin_until_accepted_then_balances_exactly() -> None:
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=_rotating_schedule_archive(
            parent_count=25, breeder_width=128
        )["rotatingEvidenceTransaction"]["parentSchedule"],
        target_unique_candidates=128,
    )
    offspring = immigrants = 0
    for _ in range(128):
        if pair_generation._scheduled_immigrant_for_allocation(
            allocation=allocation,
            accepted_offspring=offspring,
            accepted_immigrants=immigrants,
        ):
            immigrants += 1
        else:
            offspring += 1
    assert (offspring, immigrants) == (102, 26)


def test_allocation_floor_applies_even_to_a_one_slot_micro_generation() -> None:
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=_rotating_schedule_archive(
            parent_count=1, breeder_width=1
        )["rotatingEvidenceTransaction"]["parentSchedule"],
        target_unique_candidates=1,
    )
    assert allocation["desiredEvaluatedOffspringCount"] == 0
    assert allocation["desiredEvaluatedImmigrantCount"] == 1


def test_accepted_quota_floor_is_exact_for_one_through_six_slots_and_restart() -> None:
    schedule = _rotating_schedule_archive(
        parent_count=1, breeder_width=1
    )["rotatingEvidenceTransaction"]["parentSchedule"]
    expected = [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (4, 2)]
    observed = []
    for target, quotas in enumerate(expected, start=1):
        allocation = pair_generation._frozen_reproduction_allocation(
            parent_schedule=schedule, target_unique_candidates=target
        )
        restarted = pair_generation._frozen_reproduction_allocation(
            parent_schedule=dict(schedule), target_unique_candidates=target
        )
        assert restarted == allocation
        observed.append((
            allocation["desiredEvaluatedOffspringCount"],
            allocation["desiredEvaluatedImmigrantCount"],
        ))
    assert observed == expected


def test_fresh_v5_allocation_and_accounting_name_the_worker_handoff_honestly() -> None:
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=_rotating_schedule_archive(parent_count=1, breeder_width=1)["rotatingEvidenceTransaction"]["parentSchedule"],
        target_unique_candidates=5,
        accepted_terminology=True,
    )
    assert allocation["schemaVersion"] == "temporal_qd_reproduction_allocation_v2"
    assert allocation["targetAcceptedCandidates"] == 5
    assert "targetEvaluatedCandidates" not in allocation
    entries = [{"originKind": "structural_offspring", "disposition": "accepted", "proposal": {"disposition": "materialized"}} for _ in range(4)]
    entries.append({"originKind": "random_immigrant", "disposition": "accepted", "proposal": {"disposition": "materialized"}})
    accounting = pair_generation._reproduction_allocation_accounting(entries, allocation=allocation)
    assert accounting["schemaVersion"] == "temporal_qd_reproduction_allocation_accounting_v2"
    assert accounting["origins"]["structural_offspring"] == {
        "targetAccepted": 4, "attempted": 4, "materialized": 4,
        "acceptedForEvaluation": 4, "rejected": 0, "rejectedByReason": {},
        "backfilled": 0, "deficitAccepted": 0,
    }
    assert "evaluated" not in accounting["origins"]["structural_offspring"]


def test_allocation_accounting_names_rejection_backfill_work() -> None:
    allocation = pair_generation._frozen_reproduction_allocation(
        parent_schedule=_rotating_schedule_archive(
            parent_count=4, breeder_width=5
        )["rotatingEvidenceTransaction"]["parentSchedule"],
        target_unique_candidates=5,
    )
    materialized = {"disposition": "materialized"}
    entries = [
        {
            "originKind": "structural_offspring",
            "disposition": "operation_rejected",
            "proposal": {"disposition": "operation_rejected"},
        },
        *(
            {
                "originKind": "structural_offspring",
                "disposition": "accepted",
                "proposal": materialized,
            }
            for _ in range(4)
        ),
        {
            "originKind": "random_immigrant",
            "disposition": "accepted",
            "proposal": materialized,
        },
    ]
    report = pair_generation._reproduction_allocation_accounting(
        entries, allocation=allocation
    )
    assert report["complete"] is True
    assert report["origins"]["structural_offspring"]["backfilled"] == 1
    assert report["origins"]["structural_offspring"]["rejected"] == 1


def test_rotating_parent_schedule_rejects_self_hashed_under_scheduling() -> None:
    archive = _rotating_schedule_archive(parent_count=25, breeder_width=128)
    schedule = archive["rotatingEvidenceTransaction"]["parentSchedule"]
    schedule["minimumImmigrantNumerator"] = 0
    schedule["scheduleSha256"] = canonical_sha256(
        {key: value for key, value in schedule.items() if key != "scheduleSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="policy is invalid"):
        pair_generation._rotating_parent_schedule(archive)


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    ((0, 0), (0, 1), (26, 128), (129, 128)),
)
def test_legacy_v1_schedule_rejects_rehashed_invalid_sparse_ratio(
    numerator: int, denominator: int
) -> None:
    schedule = _legacy_v1_schedule(parent_count=25, breeder_width=128)
    schedule["offspringNumerator"] = numerator
    schedule["offspringDenominator"] = denominator
    schedule["scheduleSha256"] = canonical_sha256(
        {key: value for key, value in schedule.items() if key != "scheduleSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="policy is invalid"):
        pair_generation._validate_rotating_parent_schedule(
            schedule, actual_parent_count=25
        )
