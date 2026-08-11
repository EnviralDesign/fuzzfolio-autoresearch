from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evolvable_module_qd_authority import evolvable_behavior_attribution_requirement
from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_evolution import directional_qd_archive_policy_authority
from autoresearch.temporal_qd_rotating_evidence_materializer import materialize_qd_rotating_evidence
from autoresearch.temporal_qd_v5_ladder_materializer import materialize_qd_v5_ladder_stage_templates


def _write(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _profile() -> dict:
    return {"version": "v2", "graph": {"kind": "temporal_graph_v1", "evidenceGroups": [{"indicatorInstanceIds": ["signal"]}]}, "instruments": ["EURUSD"], "directionMode": "long", "isActive": False, "indicators": [{"meta": {"id": "FIXTURE", "instanceId": "signal"}, "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 14}}], "executionConfig": {"exitPolicy": {"selectedCell": {"rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0}}}}


def _rotating(tmp_path: Path) -> Path:
    from autoresearch.evidence_plan import canonical_sha256
    profile = _profile(); population = {"schemaVersion": "temporal_discovery_population_v2", "candidateCount": 1, "candidates": [{"candidateId": "seed", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)}]}; population["populationSha256"] = canonical_sha256(population)
    curriculum = {"schemaVersion": "temporal_qd_rotating_evidence_input_v1", "developmentYears": [{"analysisWindowStart": f"{year}-01-01T00:00:00Z", "analysisWindowEnd": f"{year+1}-01-01T00:00:00Z"} for year in range(2021, 2025)], "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"}, "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"}}
    catalog = {"timeframes": {"M5": {}, "H1": {}}, "indicators": [{"meta": {"id": "FIXTURE", "requiredPaddingBars": 20}, "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 5}}]}
    def attest(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + "a" * 64, attestation_sha256="sha256:" + "b" * 64)
    materialize_qd_rotating_evidence(rotating_evidence_input_path=_write(tmp_path / "curriculum.json", curriculum), seed_population_path=_write(tmp_path / "population.json", population), construction_catalog_path=_write(tmp_path / "catalog.json", catalog), output_root=tmp_path / "rotating", worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", base_timeframe="M5", attestor=attest)
    return tmp_path / "rotating"


def test_v5_ladder_resolves_fresh_stage_windows_restart_and_tamper(tmp_path: Path) -> None:
    rotating = _rotating(tmp_path)
    seen: list[LakeWindowRequest] = []
    def attest(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        seen.append(request)
        return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + ("d" if len(seen) == 1 else "e") * 64, attestation_sha256="sha256:" + "f" * 64)
    kwargs = dict(rotating_materialization_path=rotating / "materialization.json", panel_template_preparation_path=rotating / "panel-1-template-preparation.json", construction_catalog_path=tmp_path / "catalog.json", output_root=tmp_path / "ladder", worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", execution_engine_commit="a" * 40, archive_policy_authority=directional_qd_archive_policy_authority(), behavior_attribution_requirement=evolvable_behavior_attribution_requirement())
    result = materialize_qd_v5_ladder_stage_templates(**kwargs, attestor=attest)
    assert len(seen) == 2
    assert [request.data_end for request in seen] == ["2025-01-01T00:00:00Z", "2024-01-01T00:00:00Z"]
    assert all(request.coverage_policy == "require_complete" and request.data_start < request.data_end for request in seen)
    assert [row["requestedHorizonMonths"] for row in result["stages"]] == [12, 36]
    assert [row["evidenceRole"] for row in result["stages"]] == ["validation", "scrutiny"]
    assert materialize_qd_v5_ladder_stage_templates(**kwargs, attestor=lambda *_a, **_k: pytest.fail("restart called lake")) == result
    stage = tmp_path / "ladder" / "validation-template-preparation.json"
    payload = json.loads(stage.read_text(encoding="utf-8")); payload["authorityLabel"] = "tampered"; _write(stage, payload)
    with pytest.raises(TemporalDiscoveryContractError, match="committed stage template drifted"):
        materialize_qd_v5_ladder_stage_templates(**kwargs, attestor=lambda *_a, **_k: pytest.fail("tamper called lake"))


def test_v5_ladder_rejects_quarter_binding_reuse_before_publication(tmp_path: Path) -> None:
    rotating = _rotating(tmp_path)
    def reused(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + "a" * 64, attestation_sha256="sha256:" + "f" * 64)
    with pytest.raises(TemporalDiscoveryContractError, match="must not reuse"):
        materialize_qd_v5_ladder_stage_templates(rotating_materialization_path=rotating / "materialization.json", panel_template_preparation_path=rotating / "panel-1-template-preparation.json", construction_catalog_path=tmp_path / "catalog.json", output_root=tmp_path / "ladder", worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", execution_engine_commit="a" * 40, archive_policy_authority=directional_qd_archive_policy_authority(), behavior_attribution_requirement=evolvable_behavior_attribution_requirement(), attestor=reused)


@pytest.mark.parametrize("kind, message", [("null", "omitted canonical"), ("stale", "forged or mismatched")])
def test_v5_ladder_rejects_null_or_stale_lake_receipts(tmp_path: Path, kind: str, message: str) -> None:
    rotating = _rotating(tmp_path)
    def invalid(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        if kind == "null":
            return LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + "d" * 64)
        stale = LakeWindowRequest(pairs=request.pairs, timeframes=request.timeframes, data_start=request.data_start, data_end="2024-12-31T00:00:00Z")
        return LakeWindowBinding(request=stale, window_semantic_sha256="sha256:" + "d" * 64, attestation_sha256="sha256:" + "f" * 64)
    with pytest.raises(TemporalDiscoveryContractError, match=message):
        materialize_qd_v5_ladder_stage_templates(rotating_materialization_path=rotating / "materialization.json", panel_template_preparation_path=rotating / "panel-1-template-preparation.json", construction_catalog_path=tmp_path / "catalog.json", output_root=tmp_path / kind, worker_contract_sha256="sha256:" + "c" * 64, worker_contract_schema="worker-v1", execution_engine_commit="a" * 40, archive_policy_authority=directional_qd_archive_policy_authority(), behavior_attribution_requirement=evolvable_behavior_attribution_requirement(), attestor=invalid)
