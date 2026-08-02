from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_stage5e7_v3_panel_bridge as bridge
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _profile(*, indicator_timeframe: str | None = None) -> dict:
    profile = {
        "version": "v2", "graph": {"kind": "temporal_graph_v1"}, "instruments": ["EURUSD"],
        "directionMode": "long", "isActive": False,
        "executionConfig": {"exitPolicy": {"evidenceStatus": "none", "selectedCell": {"rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0}, "sourceKind": "manual"}, "sizingPolicy": {"mode": "inherit_global"}},
    }
    if indicator_timeframe is not None:
        profile["indicators"] = [{
            "meta": {"id": "FIXTURE", "instanceId": "signal", "requiredPaddingBars": 10},
            "config": {"isActive": True, "timeframe": indicator_timeframe, "lookbackBars": 14},
        }]
    return profile


def _template() -> dict:
    profile = _profile()
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2", "profile_snapshot_sha256": canonical_sha256(profile),
        "analysis_window_start": "2024-02-01T00:00:00Z", "analysis_window_end": "2024-03-01T00:00:00Z",
        "execution_cell_sha256": canonical_sha256(profile["executionConfig"]["exitPolicy"]["selectedCell"]),
        "lake_window_binding": {"window_semantic_sha256": "sha256:" + "b" * 64, "request": {"data_start": "2024-01-01T00:00:00Z", "data_end": "2024-03-01T00:00:00Z", "pairs": ["EURUSD"], "timeframes": ["M5"]}},
    }
    plan["plan_id"] = canonical_sha256(plan)
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1", "authorityLabel": "frozen-development",
        "workerContract": {"workerContractSha256": "sha256:" + "c" * 64, "workerContractSchema": "replay-worker-contract-v1"},
        "candidates": [{"candidateId": "template", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile), "instrument": "EURUSD", "timeframe": "M5", "barLimit": 5000, "windowInputs": [{"windowId": "development", "evidencePlan": plan}]}],
        "developmentWindows": [{"windowId": "development", "analysisWindowStart": "2024-02-01T00:00:00Z", "analysisWindowEnd": "2024-03-01T00:00:00Z"}],
        "prohibitedEvidence": [{"windowId": "reserved", "analysisWindowStart": "2024-06-29T00:00:00Z", "analysisWindowEnd": "2024-07-01T00:00:00Z", "reason": "reserved"}],
        "bounds": {"maxCandidates": 1, "maxDevelopmentWindows": 1, "maxTasks": 1, "maxAttempts": 2, "deadlineSeconds": 60},
    }


def _catalog() -> dict:
    return {
        "timeframes": {"M1": {}, "M5": {}, "M30": {}, "H1": {}},
        "indicators": [
            {
                "meta": {"id": "FIXTURE", "requiredPaddingBars": 10},
                "config": {
                    "isActive": True,
                    "timeframe": "M5",
                    "lookbackBars": 14,
                },
            }
        ],
    }


def _reference(root: Path, *, profile: dict | None = None) -> Path:
    profile = _profile() if profile is None else profile
    candidates = [{"candidateId": f"qd_reference_{index}", "sourceMode": "legacy_reference", "seedId": "fixture", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile), "profileSnapshotSha256": canonical_sha256(profile), "programSha256": canonical_sha256({"program": index})} for index in range(64)]
    population = {"schemaVersion": "temporal_qd_generation_population_v3", "referencePopulationSchema": "stage5e7_v3_reference_population_v1", "candidateCount": 64, "candidates": candidates}
    population["populationSha256"] = canonical_sha256(population)
    panel = {"schemaVersion": "stage5e7_v3_tagged_reference_panel_v1", "referencePopulationSha256": population["populationSha256"]}
    panel["referencePanelSha256"] = canonical_sha256(panel)
    _write(root / "reference-population.json", population)
    _write(root / "reference-panel.json", panel)
    return root / "reference-population.json"


@pytest.mark.parametrize("replacement", ["M1", "M30"])
def test_freeze_rejects_panel_profile_outside_immutable_lake_scope(
    tmp_path: Path, replacement: str
) -> None:
    population = _reference(
        tmp_path / "repair", profile=_profile(indicator_timeframe=replacement)
    )
    template_path = tmp_path / "template.json"; _write(template_path, _template())
    catalog_path = tmp_path / "catalog.json"; _write(catalog_path, _catalog())

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="outside the immutable pre-attested evidence binding",
    ):
        bridge.freeze_finite_panel_campaign(
            population_path=population,
            template_preparation_path=template_path,
            worker_contract_sha256="sha256:" + "d" * 64,
            output_root=tmp_path / "external",
            construction_catalog_path=catalog_path,
        )


def test_freeze_rotates_each_panel_candidate_without_qd_relabeling(tmp_path: Path) -> None:
    population = _reference(tmp_path / "repair")
    template_path = tmp_path / "template.json"; _write(template_path, _template())
    with pytest.raises(TemporalDiscoveryContractError, match="worker-contract-sha256"):
        bridge.freeze_finite_panel_campaign(population_path=population, template_preparation_path=template_path, worker_contract_sha256="not-a-sha", output_root=tmp_path / "invalid")
    result = bridge.freeze_finite_panel_campaign(population_path=population, template_preparation_path=template_path, worker_contract_sha256="sha256:" + "d" * 64, output_root=tmp_path / "external")
    preparation = json.loads((Path(result["outputRoot"]) / "preparation.json").read_text(encoding="utf-8"))
    evaluation = json.loads((Path(result["outputRoot"]) / "evaluation-identity.json").read_text(encoding="utf-8"))
    campaign = json.loads((Path(result["outputRoot"]) / "campaign.json").read_text(encoding="utf-8"))
    assert result["panelKind"] == "repair_reference"
    assert preparation["authorityLabel"].endswith("repair_reference")
    assert len(preparation["candidates"]) == 64
    assert preparation["workerContract"]["workerContractSha256"] == "sha256:" + "d" * 64
    assert evaluation["effectiveWorkerContract"] == preparation["workerContract"]
    assert campaign["effectiveWorkerContract"] == preparation["workerContract"]
    assert {item["windowInputs"][0]["evidencePlan"]["profile_snapshot_sha256"] for item in preparation["candidates"]} == {canonical_sha256(_profile())}
    assert (Path(result["outputRoot"]) / "task-matrix" / "task-manifest.json").is_file()
    assert json.loads((Path(result["outputRoot"]) / "campaign.json").read_text())["canonicalQDConversion"] == "prohibited"


def test_seed_admission_requires_exact_coverage_and_is_explicit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    population = _reference(tmp_path / "repair")
    template_path = tmp_path / "template.json"; _write(template_path, _template())
    frozen = bridge.freeze_finite_panel_campaign(population_path=population, template_preparation_path=template_path, worker_contract_sha256="sha256:" + "d" * 64, output_root=tmp_path / "bridge")
    bridge_root = Path(frozen["outputRoot"])
    result_root = tmp_path / "results"
    _write(result_root / "authority.json", json.loads((bridge_root / "authority.json").read_text(encoding="utf-8")))
    _write(result_root / "task-manifest.json", json.loads((bridge_root / "task-matrix" / "task-manifest.json").read_text(encoding="utf-8")))
    monkeypatch.setattr(bridge, "load_authority_bound_panel_results", lambda **_kwargs: {})
    with pytest.raises(TemporalDiscoveryContractError, match="exact repaired-reference"):
        bridge.admit_repair_reference_seed(reference_population_path=population, result_root=result_root, panel_preparation_path=bridge_root / "preparation.json", output_root=tmp_path / "seed-empty")

    candidates = json.loads(population.read_text(encoding="utf-8"))["candidates"]
    resolved_profile = "sha256:" + "e" * 64
    resolved_program = "sha256:" + "f" * 64
    results = {
        item["candidateId"]: [{
            "analysisWindowStart": "2024-02-01T00:00:00Z",
            "analysisWindowEnd": "2024-03-01T00:00:00Z",
            "sourceProfileSnapshotSha256": item["profileSnapshotSha256"],
            "resolvedProfileSnapshotSha256": resolved_profile,
            "resolvedProgramSha256": resolved_program,
            "programSha256": resolved_program,
        }]
        for item in candidates
    }
    monkeypatch.setattr(
        bridge, "load_authority_bound_panel_results", lambda **_kwargs: results
    )
    monkeypatch.setattr(
        bridge,
        "_aggregate_candidate",
        lambda candidate, _windows: {
            "v3Admissible": True,
            "authoredProgramSha256": candidate["programSha256"],
            "sourceProfileSnapshotSha256": candidate["profileSnapshotSha256"],
            "resolvedProfileSnapshotSha256": resolved_profile,
            "resolvedProgramSha256": resolved_program,
            "programSha256": resolved_program,
        },
    )
    observed: dict[str, object] = {}
    def reduce(**kwargs: object) -> dict:
        observed.update(kwargs)
        return {"archiveSha256": "sha256:" + "d" * 64}
    monkeypatch.setattr(bridge, "build_qd_archive", reduce)
    result = bridge.admit_repair_reference_seed(reference_population_path=population, result_root=result_root, panel_preparation_path=bridge_root / "preparation.json", output_root=tmp_path / "seed")
    seed = json.loads((Path(result["outputRoot"]) / "seed-population.json").read_text(encoding="utf-8"))
    admission = json.loads((Path(result["outputRoot"]) / "admission.json").read_text(encoding="utf-8"))
    assert seed["qdVersion"] == "temporal_qd_evolution_v3"
    assert seed["legacyReferenceAdmissionBindingRequired"] is True
    assert all(item["sourceMode"] == "qd_stage5e7_v3_reference_seed_admitted" for item in seed["candidates"])
    assert all(
        item["legacyReferenceAdmissionBinding"]["admissionKind"]
        == "legacy_reference_result_attested"
        for item in seed["candidates"]
    )
    assert admission["v2ArchiveRanksUsed"] is False
    assert admission["effectiveWorkerContract"]["workerContractSha256"] == "sha256:" + "d" * 64
    assert seed["stage5e7V3SeedAdmissionContext"]["authorityId"] == frozen["authorityId"]
    assert observed["generation_index"] == 0
