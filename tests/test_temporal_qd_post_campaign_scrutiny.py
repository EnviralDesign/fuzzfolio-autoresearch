from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_post_campaign_scrutiny import (
    TemporalQDScrutinyError,
    _observation_stream_consistency,
    _rank_validation,
    _require_worker_environment,
    _target_worker_contract,
    run_qd_post_campaign_scrutiny,
)
from autoresearch.temporal_qd_rotating_evidence import (
    ROTATING_EVIDENCE_INPUT_SCHEMA,
    build_rotating_evidence_contract,
)


def _profile(candidate_id: str) -> dict:
    return {
        "version": "v3", "directionMode": "both", "instruments": ["EURUSD"],
        "graph": {"states": [{"id": "flat"}]}, "indicators": [],
        "executionConfig": {"managementLibrary": {"version": "temporal_management_v1", "plans": [], "defaultPlanId": "none"}},
        "name": candidate_id,
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


def test_shared_observation_stream_identity_fails_on_divergent_hashes() -> None:
    consistency = _observation_stream_consistency([
        {"validReplay": True, "sharedObservationStreamId": "shared", "observationStreamSha256": "sha256:" + "a" * 64},
        {"validReplay": True, "sharedObservationStreamId": "shared", "observationStreamSha256": "sha256:" + "b" * 64},
    ])
    assert consistency["valid"] is False
    assert consistency["divergentSharedObservationStreamIds"] == ["shared"]


def test_validation_ranking_is_strict_json_safe_for_rejections_and_zeroes() -> None:
    candidates = {
        candidate_id: {"candidateId": candidate_id, "sourceProfile": _profile(candidate_id)}
        for candidate_id in ("zero", "rejected")
    }
    ranked, promoted = _rank_validation(
        results=[
            {
                "candidateId": "zero",
                "validReplay": True,
                "netConservativeR": 0.0,
                "maxDrawdownR": 0.0,
                "closedTrades": 24,
                "unresolvedPosition": False,
                "unresolvedPendingEffect": False,
            },
            {"candidateId": "rejected", "validReplay": False, "reason": "terminal_rejection"},
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
        candidates[candidate_id] = {"candidateId": candidate_id, "sourceProfile": profile}
        results.append({
            "candidateId": candidate_id, "validReplay": True,
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
    candidates = []
    for candidate_id in ("candidate-a", "candidate-b"):
        profile = _profile(candidate_id)
        candidates.append({"candidateId": candidate_id, "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)})
    cohort = {"cohortSha256": "sha256:" + "c" * 64}
    config = {"evaluation": {"predeclaredEvidenceContext": {"constructionCatalog": {"path": str(catalog), "catalogSha256": canonical_sha256({})}}, "templatePreparationPath": str(template)}}
    (campaign / "config.json").write_text(json.dumps(config), encoding="utf-8")

    import autoresearch.temporal_qd_post_campaign_scrutiny as subject
    monkeypatch.setattr(subject, "_load_source", lambda **_kwargs: (candidates, cohort, config, rotating))
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
        gateway.calls.append(Path(output_root).name)
        gateway.resume_flags.append(bool(kwargs["resume"]))
        checkpoint_path = Path(output_root) / "checkpoint.json"
        checkpoint_path.write_text("{}\n", encoding="utf-8")
        return {"completedTaskCount": len(authority["candidates"]), "taskCount": len(authority["candidates"])}
    monkeypatch.setattr(subject, "run_temporal_search_tasks", fake_run)
    def fake_results(stage_root: Path, _authority, *, expected_worker_environment):
        assert expected_worker_environment["worker_contract_hash"] == contract["contract_hash"]
        if stage_root.name == "validation_12m":
            return [
                {"candidateId": "candidate-a", "validReplay": True, "netConservativeR": 1.0, "maxDrawdownR": 0.5, "closedTrades": 24, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "1" * 64, "sharedObservationStreamId": "sha256:" + "2" * 64, "resultSha256": "sha256:" + "3" * 64},
                {"candidateId": "candidate-b", "validReplay": True, "netConservativeR": -0.1, "maxDrawdownR": 0.1, "closedTrades": 100, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "4" * 64, "sharedObservationStreamId": "sha256:" + "5" * 64, "resultSha256": "sha256:" + "6" * 64},
            ]
        return [{"candidateId": "candidate-a", "validReplay": True, "netConservativeR": 2.0, "maxDrawdownR": 0.3, "closedTrades": 30, "unresolvedPosition": False, "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "7" * 64, "sharedObservationStreamId": "sha256:" + "8" * 64, "resultSha256": "sha256:" + "9" * 64}]
    monkeypatch.setattr(subject, "_read_stage_results", fake_results)

    result = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert result["status"] == "completed"
    assert gateway.calls == ["validation_12m", "scrutiny_36m"]
    assert len(attested) == 2
    promotion = json.loads((output / "promotion-manifest.json").read_text(encoding="utf-8"))
    assert promotion["promotedCandidateIds"] == ["candidate-a"]
    assert json.loads((output / "promotion-policy.json").read_text(encoding="utf-8"))["criteria"]["minimumClosedTrades"] == 24
    source_identity = json.loads((output / "source-identity.json").read_text(encoding="utf-8"))
    assert source_identity["outerTailTouched"] is False
    assert source_identity["controllerIdentity"]["dependencyBundleSha256"].startswith("sha256:")
    sealed = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert sealed["promotionManifestSha256"] == promotion["promotionManifestSha256"]
    assert sealed["scrutinyStageOutcome"].startswith("sha256:")
    assert sealed["resultSha256"].startswith("sha256:")

    # A completed restart reuses both frozen lake bindings and exact stage
    # checkpoints rather than creating a new authority or changing artifacts.
    restarted = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert restarted == result
    assert len(attested) == 2
    assert gateway.resume_flags == [False, False, True, True]

    def fake_no_passer_results(stage_root: Path, _authority, *, expected_worker_environment):
        assert stage_root.name == "validation_12m"
        return [
            {"candidateId": candidate_id, "validReplay": True, "netConservativeR": -1.0,
             "maxDrawdownR": 1.0, "closedTrades": 30, "unresolvedPosition": False,
             "unresolvedPendingEffect": False, "observationStreamSha256": "sha256:" + "a" * 64,
             "sharedObservationStreamId": "sha256:" + "b" * 64, "resultSha256": "sha256:" + "c" * 64}
            for candidate_id in ("candidate-a", "candidate-b")
        ]
    monkeypatch.setattr(subject, "_read_stage_results", fake_no_passer_results)
    no_passer_output = tmp_path / "no-passers-scrutiny"
    gateway.calls.clear(); gateway.resume_flags.clear()
    no_passer = run_qd_post_campaign_scrutiny(campaign_root=campaign, generation_index=5, output_root=no_passer_output,
        target_worker_contract_path=contract_path, attestor=fake_attestor, client=gateway, expected_cohort_size=2)
    assert gateway.calls == ["validation_12m"]
    assert no_passer["scrutinyStageOutcome"] == "skipped_no_validation_passers"
    assert no_passer["scrutinyCandidateCount"] == 0
