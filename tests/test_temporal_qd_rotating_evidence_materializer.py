from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_rotating_evidence import validate_generation_template
import autoresearch.temporal_qd_rotating_evidence_materializer as materializer
from autoresearch.temporal_qd_rotating_evidence_materializer import materialize_qd_rotating_evidence
from autoresearch.temporal_search import build_authority


def _write(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _profile() -> dict:
    return {"version": "v2", "graph": {"kind": "temporal_graph_v1", "evidenceGroups": [{"indicatorInstanceIds": ["signal"]}]},
        "instruments": ["EURUSD"], "directionMode": "long", "isActive": False,
        "indicators": [{"meta": {"id": "FIXTURE", "instanceId": "signal"}, "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 14}}],
        "executionConfig": {"exitPolicy": {"selectedCell": {"rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0}}, "sizingPolicy": {"mode": "inherit_global"}}}


def _population() -> dict:
    profile = _profile()
    value = {"schemaVersion": "temporal_discovery_population_v2", "candidateCount": 1,
        "candidates": [{"candidateId": "seed-a", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)}]}
    value["populationSha256"] = canonical_sha256(value)
    return value


def _catalog() -> dict:
    return {"timeframes": {"M5": {}, "H1": {}, "D1": {}},
        "indicators": [{"meta": {"id": "FIXTURE", "requiredPaddingBars": 260}, "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 5}}]}


def _curriculum() -> dict:
    return {"schemaVersion": "temporal_qd_rotating_evidence_input_v1",
        "developmentYears": [{"analysisWindowStart": f"{year}-01-01T00:00:00Z", "analysisWindowEnd": f"{year + 1}-01-01T00:00:00Z"} for year in range(2021, 2025)],
        "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"}}


def _attestor(calls: list[LakeWindowRequest], *, variant: str = "a"):
    def attest(request: LakeWindowRequest, *, legacy_selection_manifest_sha256: str | None) -> LakeWindowBinding:
        assert legacy_selection_manifest_sha256 is None
        calls.append(request)
        return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + variant * 64, attestation_sha256="sha256:" + "b" * 64)
    return attest


def _materialize(tmp_path: Path, *, attestor):
    return materialize_qd_rotating_evidence(rotating_evidence_input_path=_write(tmp_path / "curriculum.json", _curriculum()),
        seed_population_path=_write(tmp_path / "population.json", _population()), construction_catalog_path=_write(tmp_path / "catalog.json", _catalog()),
        output_root=tmp_path / "external", worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5", attestor=attestor)


def test_materializes_all_quarters_panels_and_immutable_restart(tmp_path: Path) -> None:
    calls: list[LakeWindowRequest] = []
    first = _materialize(tmp_path, attestor=_attestor(calls))
    def must_not_attest(*_args, **_kwargs):
        raise AssertionError("idempotent restart must reuse frozen lake semantic bindings")
    second = _materialize(tmp_path, attestor=must_not_attest)
    assert first == second
    assert len(calls) == 16
    root = tmp_path / "external"
    manifest = json.loads((root / "materialization.json").read_text(encoding="utf-8"))
    assert manifest["coverage"] == {"developmentYearCount": 4, "quarterCount": 16, "quarterMonths": 3, "requestedHorizonMonths": 3, "effectiveHorizonMonths": 3, "allQuarterCoverageExact": True, "maxAttemptsPerTask": 8}
    assert len(manifest["quarters"]) == 16
    assert manifest["outerTail"] == {"analysisWindowStart": "2026-01-01T00:00:00Z", "touched": False, "label": "sole_untouched_evidence"}
    assert manifest["researchScrutiny"]["validation"]["label"] == "research_scrutiny_not_untouched"
    contract = manifest["rotatingEvidence"]
    for generation in range(1, 5):
        panel = f"panel-{generation}"
        template = json.loads((root / f"{panel}-template-preparation.json").read_text(encoding="utf-8"))
        validate_generation_template(template, contract, generation)
        assert len(template["developmentWindows"]) == 4
        assert template["bounds"]["maxAttempts"] == 8
        assert all(row["evidencePlan"]["requested_horizon_months"] == 3 for candidate in template["candidates"] for row in candidate["windowInputs"])
    master = json.loads((root / "development-universe-template-preparation.json").read_text(encoding="utf-8"))
    assert len(master["developmentWindows"]) == 16
    assert build_authority(master)["authorityId"] == manifest["templates"]["master"]["authorityId"]


def test_fails_closed_for_gap_and_attestation_drift(tmp_path: Path) -> None:
    broken = _curriculum(); broken["developmentYears"][0].update({"analysisWindowStart": "2020-01-01T00:00:00Z", "analysisWindowEnd": "2021-01-01T00:00:00Z"})
    with pytest.raises(TemporalDiscoveryContractError, match="gap"):
        materialize_qd_rotating_evidence(rotating_evidence_input_path=_write(tmp_path / "bad.json", broken),
            seed_population_path=_write(tmp_path / "population.json", _population()), construction_catalog_path=_write(tmp_path / "catalog.json", _catalog()),
            output_root=tmp_path / "external-bad", worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", base_timeframe="M5", attestor=_attestor([]))
    calls: list[LakeWindowRequest] = []
    _materialize(tmp_path, attestor=_attestor(calls, variant="a"))
    # A remote provider could now issue a fresh receipt.  It is deliberately
    # ignored: the persisted semantic binding is the restart authority.
    assert _materialize(tmp_path, attestor=_attestor(calls, variant="d"))
    assert len(calls) == 16


def test_rejects_panel_drift_before_materialization(tmp_path: Path) -> None:
    # A malformed input cannot supply panels; emulate drift through the frozen
    # contract validator by using an overlapping development year.  This must
    # fail before any attestation happens.
    bad = copy.deepcopy(_curriculum())
    bad["developmentYears"][1].update({"analysisWindowStart": "2021-12-01T00:00:00Z", "analysisWindowEnd": "2022-12-01T00:00:00Z"})
    calls: list[LakeWindowRequest] = []
    with pytest.raises(TemporalDiscoveryContractError, match="overlap"):
        materialize_qd_rotating_evidence(rotating_evidence_input_path=_write(tmp_path / "overlap.json", bad),
            seed_population_path=_write(tmp_path / "population.json", _population()), construction_catalog_path=_write(tmp_path / "catalog.json", _catalog()),
            output_root=tmp_path / "external-overlap", worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", base_timeframe="M5", attestor=_attestor(calls))
    assert not calls


def _rewrite_manifest(path: Path, mutate) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    mutate(payload)
    payload["materializationSha256"] = canonical_sha256({
        key: value for key, value in payload.items() if key != "materializationSha256"
    })
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@pytest.mark.parametrize("mutate, message", [
    (lambda value: value["quarters"][0].pop("remoteBinding"), "missing or malformed"),
    (lambda value: value["quarters"][0]["remoteBinding"]["request"].update({"coverage_policy": "allow_truncated"}), "semantic or coverage identity drifted"),
    (lambda value: value["quarters"][0]["remoteBinding"].update({"window_semantic_sha256": "sha256:" + "z" * 64}), "semantic or attestation identity is invalid"),
])
def test_restart_rejects_missing_mismatched_or_semantic_drifted_binding(tmp_path: Path, mutate, message: str) -> None:
    _materialize(tmp_path, attestor=_attestor([]))
    _rewrite_manifest(tmp_path / "external" / "materialization.json", mutate)
    with pytest.raises(TemporalDiscoveryContractError, match=message):
        _materialize(tmp_path, attestor=lambda *_args, **_kwargs: pytest.fail("must fail before re-attestation"))


def test_cli_main_forwards_explicit_per_task_retry_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(materializer, "materialize_qd_rotating_evidence", lambda **kwargs: captured.update(kwargs) or {"ok": True})
    code = materializer.main([
        "--rotating-evidence-input", str(tmp_path / "curriculum.json"), "--seed-population", str(tmp_path / "population.json"),
        "--construction-catalog", str(tmp_path / "catalog.json"), "--worker-contract-sha256", "sha256:" + "c" * 64,
        "--worker-contract-schema", "worker-v1", "--base-timeframe", "M5", "--max-attempts-per-task", "5",
        "--output-root", str(tmp_path / "out"),
    ])
    assert code == 0
    assert captured["max_attempts_per_task"] == 5


def test_retry_limit_is_exact_per_task_not_population_width(tmp_path: Path) -> None:
    population = _population()
    profile = _profile()
    population["candidates"].append({"candidateId": "seed-b", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)})
    population["candidateCount"] = 2
    population["populationSha256"] = canonical_sha256({key: value for key, value in population.items() if key != "populationSha256"})
    result = materialize_qd_rotating_evidence(
        rotating_evidence_input_path=_write(tmp_path / "curriculum.json", _curriculum()),
        seed_population_path=_write(tmp_path / "population.json", population), construction_catalog_path=_write(tmp_path / "catalog.json", _catalog()),
        output_root=tmp_path / "external", worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="worker-v1", base_timeframe="M5", max_attempts_per_task=3,
        attestor=_attestor([]),
    )
    master = json.loads(Path(result["masterTemplatePreparationPath"]).read_text(encoding="utf-8"))
    assert master["bounds"] == {"maxCandidates": 2, "maxDevelopmentWindows": 16, "maxTasks": 32, "maxAttempts": 3, "deadlineSeconds": 86400.0}


@pytest.mark.parametrize("crash_after", [
    "development-universe-template-preparation.json",
    "panel-1-template-preparation.json",
])
def test_partial_root_after_templates_reuses_immutable_checkpoint_without_provider_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash_after: str,
) -> None:
    original_write_once = materializer._write_once
    def crash_after_write(path: Path, payload: dict) -> None:
        original_write_once(path, payload)
        if path.name == crash_after:
            raise RuntimeError("simulated materializer crash")
    monkeypatch.setattr(materializer, "_write_once", crash_after_write)
    with pytest.raises(RuntimeError, match="simulated"):
        _materialize(tmp_path, attestor=_attestor([]))
    monkeypatch.setattr(materializer, "_write_once", original_write_once)
    assert _materialize(tmp_path, attestor=lambda *_args, **_kwargs: pytest.fail("partial restart must not re-attest"))


def test_partial_root_missing_or_incomplete_checkpoint_fails_before_provider_call(tmp_path: Path) -> None:
    root = tmp_path / "external"
    root.mkdir()
    (root / "partial-output.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="lacks immutable lake binding checkpoints"):
        _materialize(tmp_path, attestor=lambda *_args, **_kwargs: pytest.fail("must fail before provider call"))

    # A crash immediately after the first accepted attestation leaves one
    # immutable record.  It is still incomplete, so restart fails closed.
    tmp_second = tmp_path / "second"
    tmp_second.mkdir()
    calls = 0
    def crash_on_second(request: LakeWindowRequest, **kwargs) -> LakeWindowBinding:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated early crash")
        return _attestor([])(request, **kwargs)
    with pytest.raises(RuntimeError, match="early crash"):
        _materialize(tmp_second, attestor=crash_on_second)
    with pytest.raises(TemporalDiscoveryContractError, match="incomplete immutable lake binding checkpoints"):
        _materialize(tmp_second, attestor=lambda *_args, **_kwargs: pytest.fail("must fail before provider call"))


def test_atomic_checkpoint_publication_never_leaves_a_truncated_final_and_cleans_orphans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "binding-checkpoints" / "year-1-q1.json"
    payload = {"schemaVersion": "fixture", "value": 1}
    original_fsync = materializer.os.fsync
    monkeypatch.setattr(materializer.os, "fsync", lambda _descriptor: (_ for _ in ()).throw(OSError("simulated write interruption")))
    with pytest.raises(OSError, match="interruption"):
        materializer._publish_immutable_checkpoint(path, payload)
    assert not path.exists()
    assert not list(path.parent.glob(".*.checkpoint.tmp"))

    monkeypatch.setattr(materializer.os, "fsync", original_fsync)
    materializer._publish_immutable_checkpoint(path, payload)
    assert path.read_bytes() == materializer._canonical_bytes(payload)
    with pytest.raises(TemporalDiscoveryContractError, match="divergent immutable"):
        materializer._publish_immutable_checkpoint(path, {"schemaVersion": "fixture", "value": 2})

    orphan = path.parent / ".year-1-q2.json.stale.checkpoint.tmp"
    orphan.write_text("incomplete", encoding="utf-8")
    materializer._cleanup_orphan_checkpoint_temps(tmp_path)
    assert not orphan.exists()
