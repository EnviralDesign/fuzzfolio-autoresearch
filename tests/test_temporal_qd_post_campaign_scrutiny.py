from __future__ import annotations

import json
from pathlib import Path
import copy

import pytest

from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch import temporal_qd_evolution as qd_module
from autoresearch.temporal_qd_post_campaign_scrutiny import (
    BEHAVIOR_FAMILY_INDEX_SCHEMA,
    LEGACY_SCRUTINY_SCHEMA,
    SCRUTINY_SCHEMA,
    TemporalQDScrutinyError,
    _final_archive_incumbents,
    _observation_stream_consistency,
    _rank_validation,
    _require_expected_resolved_program,
    _require_worker_environment,
    _target_worker_contract,
    _source_union,
    _behavior_family_index,
    run_qd_post_campaign_scrutiny,
)
from autoresearch.temporal_qd_rotating_evidence import (
    ROTATING_EVIDENCE_INPUT_SCHEMA,
    build_rotating_evidence_contract,
)
from autoresearch.temporal_realized_behavior import (
    aggregate_realized_behavior,
    build_window_realized_behavior,
)


def _profile(candidate_id: str) -> dict:
    return {
        "version": "v3", "directionMode": "both", "instruments": ["EURUSD"],
        "graph": {"states": [{"id": "flat"}]}, "indicators": [],
        "executionConfig": {"managementLibrary": {"version": "temporal_management_v1", "plans": [], "defaultPlanId": "none"}},
        "name": candidate_id,
    }


def _source_candidate(candidate_id: str, *roles: str) -> dict:
    profile = _profile(candidate_id)
    return {
        "candidateId": candidate_id, "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "candidateIdentitySha256": "sha256:" + "a" * 63 + candidate_id[-1],
        "programSha256": "sha256:" + "b" * 63 + candidate_id[-1],
        "sourceCandidateIdentitySha256": "sha256:" + "a" * 63 + candidate_id[-1],
        "authoredProgramSha256": "sha256:" + "b" * 63 + candidate_id[-1],
        "sourceRoles": list(roles),
    }


def _archive_for_incumbent(candidate: dict, *, generation_index: int = 5) -> dict:
    """Build the smallest geometry-valid, current-policy frozen archive."""

    descriptor = {
        "operatorFamilies": "none",
        "mutationDepth": "root",
        "entryEvents": "none",
        "managementActions": "none",
        "graphNodes": "small",
        "tradeFrequency": "dormant",
        "medianHolding": "none",
    }
    descriptor["cellId"] = "|".join(
        descriptor[axis] for axis in qd_module.QD_DESCRIPTOR_AXES
    )
    archive = qd_module.canonical_empty_bidirectional_archive_template()
    archive.pop("archiveSha256")
    archive.update(
        {
            "generationIndex": generation_index,
            "candidateCountSeen": 1,
            "occupiedCellCount": 1,
            "memberCount": 1,
            "qualityMemberCount": 1,
            "cells": [{
                "cellId": descriptor["cellId"],
                "descriptor": dict(descriptor),
                "members": [{
                    "candidateId": candidate["candidateId"],
                    "candidate": {
                        key: candidate[key]
                        for key in (
                            "candidateId", "sourceProfile", "sourceProfileSha256",
                            "candidateIdentitySha256", "programSha256",
                        )
                    },
                    "aggregate": {"resolvedProgramSha256": "sha256:" + "d" * 64},
                    "archiveLane": "quality",
                    "descriptor": dict(descriptor),
                }],
            }],
        }
    )
    archive["archiveSha256"] = canonical_sha256(archive)
    return archive


def _archive_expectations(archive: dict) -> dict:
    return {
        "expected_generation_index": archive["generationIndex"],
        "expected_qd_version": archive["qdVersion"],
        "expected_policy_name": archive["policyName"],
        "expected_policy_sha256": archive["policySha256"],
        "expected_frozen_policy": archive["frozenPolicy"],
    }


def _realized_behavior(*, direction: str = "long") -> dict:
    trade = {
        "direction": direction,
        "tradeId": f"trade-{direction}", "positionId": f"position-{direction}",
        "entryClockIndex": 1, "exitClockIndex": 3,
        "entryTime": "2024-01-01T00:00:00Z", "exitTime": "2024-01-01T01:00:00Z",
        "holdingBars": 2, "holdingHours": 1.0, "closeReason": "target",
        "grossR": 1.0, "netR": 0.8,
    }
    window = build_window_realized_behavior(
        window_id="scrutiny-window",
        replay={
            "trades": [trade],
            "executionTraces": [{"tradeId": trade["tradeId"], "actionKind": "enter_next_open"}],
            "graphTraces": [{"direction": direction, "transitionId": "entry"}],
        },
        metrics={
            "tradesClosed": 1, "observationsProcessed": 10,
            "totalGrossR": 1.0, "totalNetR": 0.8,
            "terminalValuation": {"positionStatus": "no_open_position"},
        },
    )
    return aggregate_realized_behavior([{"realizedBehavior": window}])


def _behavior_fields(*, direction: str = "long") -> dict:
    return {
        "resolvedProgramSha256": "sha256:" + "e" * 64,
        "realizedBehavior": _realized_behavior(direction=direction),
    }


def test_worker_environment_identity_requires_every_digest_bound_field() -> None:
    expected = {
        "worker_contract_hash": "sha256:" + "a" * 64,
        "worker_contract_schema": "replay-worker-contract-v2",
        "worker_image_digest": "sha256:" + "b" * 64,
        "worker_image_identity_mode": "image_digest",
        "worker_rust_core_hash": "sha256:" + "c" * 64,
        "worker_rust_build_info": {"target": "x86_64"},
        "worker_runtime_platform": {"system": "Linux"},
    }
    _require_worker_environment(dict(expected), expected=expected)
    wrong = dict(expected); wrong["worker_image_digest"] = "sha256:" + "d" * 64
    with pytest.raises(TemporalQDScrutinyError, match="environment identity"):
        _require_worker_environment(wrong, expected=expected)


def test_final_archive_incumbent_merges_into_the_deterministic_source_union(tmp_path: Path) -> None:
    candidate = _source_candidate("incumbent-c", "new_proposal")
    archive = _archive_for_incumbent(candidate)
    path = tmp_path / "archive.json"
    path.write_text(json.dumps(archive), encoding="utf-8")

    incumbents, descriptor = _final_archive_incumbents(
        path, **_archive_expectations(archive)
    )
    union, frozen = _source_union(
        archive_incumbents=incumbents, new_proposals=[candidate], archive=descriptor,
        cohort={"cohortSha256": "sha256:" + "a" * 64},
    )

    assert [row["candidateId"] for row in union] == ["incumbent-c"]
    assert union[0]["sourceRoles"] == ["archive_incumbent", "new_proposal"]
    assert union[0]["authoredProgramSha256"] == candidate["programSha256"]
    assert union[0]["expectedResolvedProgramSha256"] == "sha256:" + "d" * 64
    assert frozen["archiveIncumbentCount"] == 1
    assert frozen["deduplicatedCandidateIdentityCount"] == 1
    _require_expected_resolved_program(union[0], "sha256:" + "d" * 64)
    with pytest.raises(TemporalQDScrutinyError, match="resolved program differs"):
        _require_expected_resolved_program(union[0], "sha256:" + "e" * 64)


def test_final_archive_incumbents_rejects_a_self_hashed_archive_from_another_generation(
    tmp_path: Path,
) -> None:
    candidate = _source_candidate("incumbent-c", "new_proposal")
    expected = _archive_for_incumbent(candidate, generation_index=5)
    swapped = _archive_for_incumbent(candidate, generation_index=4)
    path = tmp_path / "archive.json"
    path.write_text(json.dumps(swapped), encoding="utf-8")

    with pytest.raises(TemporalQDScrutinyError, match="generation index drifted"):
        _final_archive_incumbents(path, **_archive_expectations(expected))


def test_final_archive_incumbents_rejects_malformed_geometry_even_when_self_hashed(
    tmp_path: Path,
) -> None:
    candidate = _source_candidate("incumbent-c", "new_proposal")
    archive = _archive_for_incumbent(candidate)
    archive.pop("archiveSha256")
    archive["cells"][0]["members"][0]["descriptor"]["cellId"] = "wrong-cell"
    archive["archiveSha256"] = canonical_sha256(archive)
    path = tmp_path / "archive.json"
    path.write_text(json.dumps(archive), encoding="utf-8")

    with pytest.raises(TemporalQDScrutinyError, match="not an authoritative QD archive"):
        _final_archive_incumbents(path, **_archive_expectations(_archive_for_incumbent(candidate)))


def test_final_archive_incumbents_rejects_a_known_but_wrong_policy(
    tmp_path: Path,
) -> None:
    candidate = _source_candidate("incumbent-c", "new_proposal")
    expected = _archive_for_incumbent(candidate)
    archive = copy.deepcopy(expected)
    archive.pop("archiveSha256")
    archive.update(
        {
            "policyName": qd_module.LEGACY_QD_POLICY_NAME,
            "policySha256": qd_module.LEGACY_QD_POLICY_SHA256,
            "frozenPolicy": qd_module.LEGACY_QD_POLICY,
        }
    )
    archive["archiveSha256"] = canonical_sha256(archive)
    path = tmp_path / "archive.json"
    path.write_text(json.dumps(archive), encoding="utf-8")

    with pytest.raises(TemporalQDScrutinyError, match="policy drifted"):
        _final_archive_incumbents(path, **_archive_expectations(expected))


def test_behavior_family_index_reports_passenger_programs_without_deleting_sources() -> None:
    behavior = _realized_behavior(direction="long")
    rows = [
        {
            "candidateId": f"candidate-{index}", "mechanicallyValidReplay": True,
            "sourceCandidateIdentitySha256": "sha256:" + f"{index:064x}",
            "authoredProgramSha256": "sha256:" + f"{index + 10:064x}",
            "resolvedProgramSha256": "sha256:" + f"{index + 20:064x}",
            "resultSha256": "sha256:" + f"{index + 30:064x}",
            "realizedBehavior": copy.deepcopy(behavior),
        }
        for index in range(8)
    ]
    index = _behavior_family_index(
        stage="validation_12m", source_identity_sha256="sha256:" + "a" * 64,
        evaluation_sha256="sha256:" + "b" * 64, results=rows,
    )

    assert index["schemaVersion"] == BEHAVIOR_FAMILY_INDEX_SCHEMA
    assert index["reportingOnly"] is True
    assert index["candidateCount"] == 8
    assert index["summary"]["candidateDeletionCount"] == 0
    assert index["summary"]["passengerMutationCandidateCount"] == 7
    assert len(index["families"]) == 1
    family = index["families"][0]
    assert family["memberCount"] == 8
    assert family["exactGenotypeCount"] == 8
    assert family["exactAuthoredProgramCount"] == 8
    assert family["exactResolvedProgramCount"] == 8
    assert family["memberCandidateIdsForDisplay"] == [f"candidate-{index}" for index in range(8)]
    assert len(family["passengerCandidateIdsForDisplay"]) == 7


def test_behavior_family_index_keeps_side_swaps_separate_and_fails_closed_on_tampering() -> None:
    rows = [
        {
            "candidateId": side, "mechanicallyValidReplay": True,
            "sourceCandidateIdentitySha256": "sha256:" + char * 64,
            "authoredProgramSha256": "sha256:" + char * 64,
            "resolvedProgramSha256": "sha256:" + char * 64,
            "resultSha256": "sha256:" + char * 64,
            "realizedBehavior": _realized_behavior(direction=side),
        }
        for side, char in (("long", "a"), ("short", "b"))
    ]
    index = _behavior_family_index(
        stage="scrutiny_36m", source_identity_sha256="sha256:" + "c" * 64,
        evaluation_sha256="sha256:" + "d" * 64, results=rows,
    )
    assert len(index["families"]) == 2
    assert {next(iter(row["directionLaneMix"])) for row in index["families"]} == {
        "long_specialist", "short_specialist",
    }

    tampered = copy.deepcopy(rows)
    tampered[0]["realizedBehavior"]["identitySha256"] = "sha256:" + "e" * 64
    with pytest.raises(TemporalQDScrutinyError, match="identity mismatch"):
        _behavior_family_index(
            stage="scrutiny_36m", source_identity_sha256="sha256:" + "c" * 64,
            evaluation_sha256="sha256:" + "d" * 64, results=tampered,
        )


def test_shared_observation_stream_identity_fails_on_divergent_hashes() -> None:
    consistency = _observation_stream_consistency([
        {"mechanicallyValidReplay": True, "sharedObservationStreamId": "shared", "observationStreamSha256": "sha256:" + "a" * 64},
        {"mechanicallyValidReplay": True, "sharedObservationStreamId": "shared", "observationStreamSha256": "sha256:" + "b" * 64},
    ])
    assert consistency["valid"] is False
    assert consistency["divergentSharedObservationStreamIds"] == ["shared"]


def test_validation_ranking_is_strict_json_safe_for_rejections_and_zeroes() -> None:
    candidates = {
        candidate_id: {"candidateId": candidate_id, "sourceProfile": _profile(candidate_id),
            "sourceRoles": ["new_proposal"], "sourceCandidateIdentitySha256": "sha256:" + "a" * 64,
            "authoredProgramSha256": "sha256:" + "b" * 64}
        for candidate_id in ("zero", "rejected")
    }
    ranked, promoted = _rank_validation(
        results=[
            {
                "candidateId": "zero",
                "mechanicallyValidReplay": True, "terminalCompleteReplay": True,
                "netConservativeR": 0.0,
                "maxDrawdownR": 0.0,
                "closedTrades": 24,
                "unresolvedPosition": False,
                "unresolvedPendingEffect": False,
            },
            {"candidateId": "rejected", "mechanicallyValidReplay": False, "terminalCompleteReplay": False, "reason": "terminal_rejection"},
        ],
        candidates=candidates,
    )
    assert promoted == []
    assert [row["candidateId"] for row in ranked] == ["zero", "rejected"]
    json.dumps(ranked, allow_nan=False)


def test_validation_promotion_caps_at_128_with_descriptor_diversity() -> None:
    candidates = {}
    results = []
    for index in range(130):
        candidate_id = f"candidate-{index:03d}"
        profile = _profile(candidate_id)
        profile["indicators"] = [{"meta": {"id": f"FAMILY_{index:03d}"}}]
        candidates[candidate_id] = {"candidateId": candidate_id, "sourceProfile": profile,
            "sourceRoles": ["new_proposal"], "sourceCandidateIdentitySha256": "sha256:" + "a" * 64,
            "authoredProgramSha256": "sha256:" + "b" * 64}
        results.append({
            "candidateId": candidate_id, "mechanicallyValidReplay": True, "terminalCompleteReplay": True,
            "netConservativeR": float(200 - index), "maxDrawdownR": 1.0,
            "closedTrades": 30, "unresolvedPosition": False,
            "unresolvedPendingEffect": False,
        })

    ranked, promoted = _rank_validation(results=results, candidates=candidates)

    assert len(ranked) == 130
    assert len(promoted) == 128
    assert [row["candidateId"] for row in promoted] == [
        f"candidate-{index:03d}" for index in range(128)
    ]


def test_target_worker_contract_rejects_incomplete_runtime_identity(tmp_path: Path) -> None:
    import hashlib

    manifest = {
        "schema_version": "replay-worker-contract-v2", "git_sha": None, "git_dirty": False,
        "image_digest": "sha256:" + "1" * 64, "image_identity_mode": "image_digest",
        "pyproject_hash": "sha256:" + "2" * 64, "uv_lock_hash": None,
        "fuzzfolio_core_hash": "sha256:" + "3" * 64, "fuzzfolio_data_hash": "sha256:" + "4" * 64,
        "shared_constants_hash": None, "compute_service_hash": "sha256:" + "5" * 64,
        "rust_core_hash": "sha256:" + "6" * 64, "rust_build_info": {},
        "runtime_platform": {}, "capabilities": [],
    }
    manifest["contract_hash"] = "sha256:" + hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(TemporalQDScrutinyError, match="Rust build identity lacks"):
        _target_worker_contract(path)


def test_scrutiny_preregisters_policy_then_uses_fake_attestor_and_gateway(
    tmp_path: Path, monkeypatch
) -> None:
    campaign = tmp_path / "campaign"; campaign.mkdir()
    output = tmp_path / "separate-scrutiny"
    catalog = tmp_path / "catalog.json"; catalog.write_text("{}\n", encoding="utf-8")
    template = tmp_path / "template.json"
    template.write_text(json.dumps({"workerContract": {"workerContractSha256": "sha256:" + "a" * 64, "workerContractSchema": "replay-worker-contract-v1"},
        "candidates": [{"timeframe": "M5", "barLimit": 5000}]}), encoding="utf-8")
    contract_path = tmp_path / "worker-contract.json"
    contract = {"schema_version": "replay-worker-contract-v2", "git_sha": None, "git_dirty": False,
        "image_digest": "sha256:" + "1" * 64, "image_identity_mode": "image_digest",
        "pyproject_hash": "sha256:" + "2" * 64, "uv_lock_hash": None,
        "fuzzfolio_core_hash": "sha256:" + "3" * 64, "fuzzfolio_data_hash": "sha256:" + "4" * 64,
        "shared_constants_hash": None, "compute_service_hash": "sha256:" + "5" * 64,
        "rust_core_hash": "sha256:" + "6" * 64,
        "rust_build_info": {"crate_name": "fuzzfolio-rust-core", "crate_version": "0.1.0", "target_arch": "x86_64", "target_os": "linux"},
        "runtime_platform": {"python_implementation": "CPython", "python_version": "3.13.0", "python_cache_tag": "cpython-313", "system": "Linux", "machine": "x86_64"},
        "capabilities": []}
    hashed = {key: value for key, value in contract.items() if key not in {"git_sha", "git_dirty"}}
    import hashlib
    contract["contract_hash"] = "sha256:" + hashlib.sha256(json.dumps(hashed, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    generation = campaign / "generations" / "generation-0005" / "proposal"; generation.mkdir(parents=True)
    (generation / "evaluation-population.json").write_text(json.dumps({"evaluationPopulationSha256": "sha256:" + "b" * 64}), encoding="utf-8")
    rotating = build_rotating_evidence_contract({"schemaVersion": ROTATING_EVIDENCE_INPUT_SCHEMA,
        "developmentYears": [{"analysisWindowStart": f"{year}-01-01T00:00:00Z", "analysisWindowEnd": f"{year + 1}-01-01T00:00:00Z"} for year in range(2021, 2025)],
        "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"}})
    candidates = [
        _source_candidate("candidate-a", "new_proposal"),
        _source_candidate("candidate-b", "new_proposal"),
        # This carried final-archive incumbent is not part of the G5 new
        # proposal cohort and must still be replayed and promotable.
        _source_candidate("incumbent-c", "archive_incumbent"),
    ]
    cohort = {"cohortSha256": "sha256:" + "c" * 64}
    config = {"evaluation": {"predeclaredEvidenceContext": {"constructionCatalog": {"path": str(catalog), "catalogSha256": canonical_sha256({})}}, "templatePreparationPath": str(template)}}
    (campaign / "config.json").write_text(json.dumps(config), encoding="utf-8")

    import autoresearch.temporal_qd_post_campaign_scrutiny as subject
    source_universe = {
        "schemaVersion": "temporal_qd_post_campaign_scrutiny_source_universe_v1",
        "archive": {"path": str(campaign / "generations" / "generation-0005" / "archive.json"), "fileSha256": "sha256:" + "d" * 64, "archiveSha256": "sha256:" + "e" * 64, "memberCount": 1},
        "newProposalCohortSha256": cohort["cohortSha256"], "archiveIncumbentCount": 1,
        "newProposalCandidateCount": 2, "deduplicatedCandidateIdentityCount": 3,
        "candidates": [{key: row[key] for key in ("candidateId", "sourceCandidateIdentitySha256", "authoredProgramSha256", "sourceProfileSha256", "sourceRoles")} for row in candidates],
    }
    source_universe["sourceUniverseSha256"] = canonical_sha256(source_universe)
    monkeypatch.setattr(subject, "_load_source", lambda **_kwargs: (candidates, cohort, config, rotating, source_universe))
    monkeypatch.setattr(subject, "_controller_identity", lambda _repo: {
        "gitCommit": "a" * 40, "gitDirty": False, "dependencies": [],
        "dependencyBundleSha256": "sha256:" + "0" * 64,
    })
    monkeypatch.setattr(subject, "_common_binding_request", lambda **_kwargs: LakeWindowRequest(pairs=["EURUSD"], timeframes=["M5"], data_start="2020-01-01T00:00:00Z", data_end="2025-01-01T00:00:00Z"))

    attested: list[LakeWindowRequest] = []
    def fake_attestor(request: LakeWindowRequest, *, legacy_selection_manifest_sha256: str | None) -> LakeWindowBinding:
        assert legacy_selection_manifest_sha256 is None
        attested.append(request)
        return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + ("d" if len(attested) == 1 else "e") * 64, attestation_sha256="sha256:" + "f" * 64)

    class FakeGateway:
        calls: list[str] = []
        resume_flags: list[bool] = []
        def close(self) -> None:  # runner does not own injected gateways
            raise AssertionError("injected gateway must not be closed")
    gateway = FakeGateway()
    def fake_run(client, authority, *, output_root, **kwargs):
        assert client is gateway
        assert kwargs["summary_filename"] == "run-summary.json"
        gateway.calls.append(Path(output_root).name)
        gateway.resume_flags.append(bool(kwargs["resume"]))
        checkpoint_path = Path(output_root) / "checkpoint.json"
        checkpoint_path.write_text("{}\n", encoding="utf-8")
        return {"completedTaskCount": len(authority["candidates"]), "taskCount": len(authority["candidates"])}
    monkeypatch.setattr(subject, "run_temporal_search_tasks", fake_run)
    def fake_results(stage_root: Path, _authority, *, expected_worker_environment, candidate_sources):
        assert expected_worker_environment["worker_contract_hash"] == contract["contract_hash"]
        assert set(candidate_sources) == {row["candidateId"] for row in candidates if stage_root.name == "validation_12m" or row["candidateId"] != "candidate-b"}
        if stage_root.name == "validation_12m":
            return [
                {"candidateId": "candidate-a", "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": 1.0, "maxDrawdownR": 0.5, "closedTrades": 24, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "1" * 64, "sharedObservationStreamId": "sha256:" + "2" * 64, "resultSha256": "sha256:" + "3" * 64, **_behavior_fields()},
                {"candidateId": "candidate-b", "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": -0.1, "maxDrawdownR": 0.1, "closedTrades": 100, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "4" * 64, "sharedObservationStreamId": "sha256:" + "5" * 64, "resultSha256": "sha256:" + "6" * 64, **_behavior_fields(direction="short")},
                {"candidateId": "incumbent-c", "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": 3.0, "maxDrawdownR": 0.2, "closedTrades": 30, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "a" * 64, "sharedObservationStreamId": "sha256:" + "b" * 64, "resultSha256": "sha256:" + "c" * 64, **_behavior_fields()},
            ]
        return [
            {"candidateId": "candidate-a", "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": 2.0, "maxDrawdownR": 0.3, "closedTrades": 30, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "7" * 64, "sharedObservationStreamId": "sha256:" + "8" * 64, "resultSha256": "sha256:" + "9" * 64, **_behavior_fields()},
            {"candidateId": "incumbent-c", "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": 4.0, "maxDrawdownR": 0.2, "closedTrades": 30, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "d" * 64, "sharedObservationStreamId": "sha256:" + "e" * 64, "resultSha256": "sha256:" + "f" * 64, **_behavior_fields()},
        ]
    monkeypatch.setattr(subject, "_read_stage_results", fake_results)

    result = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert result["status"] == "completed"
    assert gateway.calls == ["validation_12m", "scrutiny_36m"]
    assert len(attested) == 2
    promotion = json.loads((output / "promotion-manifest.json").read_text(encoding="utf-8"))
    assert promotion["promotedCandidateIds"] == ["incumbent-c", "candidate-a"]
    assert promotion["promotedCandidates"][0]["sourceRoles"] == ["archive_incumbent"]
    assert json.loads((output / "promotion-policy.json").read_text(encoding="utf-8"))["criteria"]["minimumClosedTrades"] == 24
    source_identity = json.loads((output / "source-identity.json").read_text(encoding="utf-8"))
    assert source_identity["schemaVersion"] == SCRUTINY_SCHEMA
    assert source_identity["archiveIncumbentCandidateCount"] == 1
    assert source_identity["newProposalCandidateCount"] == 2
    assert source_identity["outerTailTouched"] is False
    assert source_identity["controllerIdentity"]["dependencyBundleSha256"].startswith("sha256:")
    sealed = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert sealed["promotionManifestSha256"] == promotion["promotionManifestSha256"]
    assert sealed["scrutinyStageOutcome"].startswith("sha256:")
    assert sealed["behaviorFamilyIndexManifestSha256"].startswith("sha256:")
    assert sealed["resultSha256"].startswith("sha256:")
    behavior_index_path = output / "validation_12m" / "behavior-family-index.json"
    behavior_index_before_restart = behavior_index_path.read_bytes()
    behavior_index = json.loads(behavior_index_before_restart)
    assert behavior_index["reportingOnly"] is True
    assert behavior_index["candidateCount"] == 3
    assert behavior_index["summary"]["candidateDeletionCount"] == 0

    # A completed restart reuses both frozen lake bindings and exact stage
    # checkpoints rather than creating a new authority or changing artifacts.
    restarted = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert restarted == result
    assert len(attested) == 2
    assert gateway.resume_flags == [False, False, True, True]
    assert behavior_index_path.read_bytes() == behavior_index_before_restart

    # The family index is sealed against the same source/result identities as
    # the stage.  A changed index is not silently accepted on restart.
    behavior_index_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(TemporalQDScrutinyError, match="divergent immutable file"):
        run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
            target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    behavior_index_path.write_bytes(behavior_index_before_restart)

    def fake_no_passer_results(stage_root: Path, _authority, *, expected_worker_environment, candidate_sources):
        assert stage_root.name == "validation_12m"
        return [
            {"candidateId": candidate_id, "mechanicallyValidReplay": True, "terminalCompleteReplay": True, "netConservativeR": -1.0,
             "maxDrawdownR": 1.0, "closedTrades": 30, "unresolvedPosition": False,
             "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "a" * 64,
             "sharedObservationStreamId": "sha256:" + "b" * 64, "resultSha256": "sha256:" + "c" * 64,
             **_behavior_fields()}
            for candidate_id in ("candidate-a", "candidate-b", "incumbent-c")
        ]
    monkeypatch.setattr(subject, "_read_stage_results", fake_no_passer_results)
    no_passer_output = tmp_path / "no-passers-scrutiny"
    gateway.calls.clear(); gateway.resume_flags.clear()
    no_passer = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=no_passer_output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert gateway.calls == ["validation_12m"]
    assert no_passer["scrutinyStageOutcome"] == "skipped_no_validation_passers"
    assert no_passer["scrutinyCandidateCount"] == 0

    # A resume reconstructs and byte-checks the full frozen union.  A carried
    # incumbent cannot be removed from a partial run silently.
    (output / "source-universe.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(TemporalQDScrutinyError, match="divergent immutable file"):
        run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
            target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)


def test_completed_legacy_scrutiny_is_readable_without_mutation(tmp_path: Path) -> None:
    output = tmp_path / "legacy-scrutiny"
    output.mkdir()
    source = {"schemaVersion": LEGACY_SCRUTINY_SCHEMA, "sourceIdentitySha256": "sha256:" + "1" * 64}
    result = {"schemaVersion": LEGACY_SCRUTINY_SCHEMA, "sourceIdentitySha256": source["sourceIdentitySha256"], "status": "completed"}
    result["resultSha256"] = canonical_sha256(result)
    (output / "source-identity.json").write_text(json.dumps(source), encoding="utf-8")
    result_path = output / "result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")

    returned = run_qd_post_campaign_scrutiny(
        campaign_root=tmp_path / "unused-campaign", generation_index=5, output_root=output,
        target_worker_contract_path=tmp_path / "unused-contract.json",
    )
    assert returned == result
    assert result_path.read_text(encoding="utf-8") == json.dumps(result)


def test_completed_v2_scrutiny_is_read_only_without_backfilling_behavior_index(tmp_path: Path) -> None:
    output = tmp_path / "v2-scrutiny"
    output.mkdir()
    schema = "temporal_qd_post_campaign_scrutiny_v2"
    source = {"schemaVersion": schema, "sourceIdentitySha256": "sha256:" + "2" * 64}
    result = {"schemaVersion": schema, "sourceIdentitySha256": source["sourceIdentitySha256"], "status": "completed"}
    result["resultSha256"] = canonical_sha256(result)
    (output / "source-identity.json").write_text(json.dumps(source), encoding="utf-8")
    result_path = output / "result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")

    returned = run_qd_post_campaign_scrutiny(
        campaign_root=tmp_path / "unused-campaign", generation_index=5, output_root=output,
        target_worker_contract_path=tmp_path / "unused-contract.json",
    )
    assert returned == result
    assert result_path.read_text(encoding="utf-8") == json.dumps(result)
    assert not (output / "behavior-family-index.json").exists()
