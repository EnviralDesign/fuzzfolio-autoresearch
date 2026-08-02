from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import build_replay_evidence_plan, canonical_sha256
from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_stage5e7_v3_evidence_envelope import build_broad_evidence_envelope


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _profile(*, timeframe: str = "H1") -> dict:
    return {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1", "evidenceGroups": [{"indicatorInstanceIds": ["signal"]}]},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        # The abbreviated authored metadata intentionally omits catalog-owned
        # requiredPaddingBars.  The catalog below is the only authority.
        "indicators": [{
            "meta": {"id": "FIXTURE", "instanceId": "signal"},
            "config": {"isActive": True, "timeframe": timeframe, "lookbackBars": 14},
        }],
        "executionConfig": {
            "exitPolicy": {
                "evidenceStatus": "none",
                "selectedCell": {"rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0},
                "sourceKind": "manual",
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }


def _catalog() -> dict:
    return {
        "timeframes": {"M1": {}, "M5": {}, "M15": {}, "M30": {}, "H1": {}, "H4": {}, "D1": {}},
        "indicators": [{
            "meta": {"id": "FIXTURE", "requiredPaddingBars": 260},
            "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 5},
        }],
    }


def _binding(
    request: LakeWindowRequest, token: str = "a", *, legacy: str | None = None, attest: bool = True
) -> LakeWindowBinding:
    return LakeWindowBinding(
        request=request,
        window_semantic_sha256="sha256:" + token * 64,
        attestation_sha256=("sha256:" + ("b" if token == "a" else "c") * 64) if attest else None,
        legacy_selection_manifest_sha256=legacy,
    )


def _template() -> dict:
    profile = _profile()
    initial = _binding(LakeWindowRequest(
        pairs=["EURUSD"], timeframes=["M5", "H1"],
        data_start="2024-01-01T00:00:00Z", data_end="2024-03-01T00:00:00Z",
    ))
    plan = build_replay_evidence_plan(
        evidence_role="training", selection_data_end="2024-03-01T00:00:00Z",
        analysis_window_start="2024-02-01T00:00:00Z", analysis_window_end="2024-03-01T00:00:00Z",
        requested_horizon_months=1, profile_snapshot=profile,
        execution_cell_sha256=canonical_sha256(profile["executionConfig"]["exitPolicy"]["selectedCell"]),
        lake_window_binding=initial,
    ).model_dump(mode="json")
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "narrow-template",
        "workerContract": {"workerContractSha256": "sha256:" + "d" * 64, "workerContractSchema": "replay-worker-contract-v1"},
        "candidates": [{
            "candidateId": "template", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile),
            "instrument": "EURUSD", "timeframe": "M5", "barLimit": 5000,
            "windowInputs": [{"windowId": "development", "evidencePlan": plan}],
        }],
        "developmentWindows": [{"windowId": "development", "analysisWindowStart": "2024-02-01T00:00:00Z", "analysisWindowEnd": "2024-03-01T00:00:00Z"}],
        "prohibitedEvidence": [{"windowId": "reserved", "analysisWindowStart": "2024-06-01T00:00:00Z", "analysisWindowEnd": "2024-07-01T00:00:00Z", "reason": "reserved"}],
        "bounds": {"maxCandidates": 1, "maxDevelopmentWindows": 1, "maxTasks": 1, "maxAttempts": 2, "deadlineSeconds": 60},
    }


def _population(profile: dict) -> dict:
    candidate = {"candidateId": "seed_a", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)}
    payload = {"schemaVersion": "fixture_population_v1", "candidateCount": 1, "candidates": [candidate]}
    payload["populationSha256"] = canonical_sha256(payload)
    return payload


_ALL_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]


def test_broad_envelope_remotely_attests_catalog_hydrated_all_timeframe_scope(tmp_path: Path) -> None:
    template = _write(tmp_path / "template.json", _template())
    profile = _profile()
    population = _write(tmp_path / "population.json", _population(profile))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    observed: list[LakeWindowRequest] = []

    def fake_attestor(request: LakeWindowRequest, *, legacy_selection_manifest_sha256: str | None) -> LakeWindowBinding:
        assert legacy_selection_manifest_sha256 is None
        observed.append(request)
        return _binding(request, "e")

    root = tmp_path / "external-envelope"
    first = build_broad_evidence_envelope(
        source_preparation_path=template, seed_population_path=population,
        construction_catalog_path=catalog, output_root=root, worker_contract_sha256="sha256:" + "f" * 64, worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=_ALL_TIMEFRAMES, attestor=fake_attestor,
    )
    second = build_broad_evidence_envelope(
        source_preparation_path=template, seed_population_path=population,
        construction_catalog_path=catalog, output_root=root, worker_contract_sha256="sha256:" + "f" * 64, worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=_ALL_TIMEFRAMES, attestor=fake_attestor,
    )
    assert first == second
    assert len(observed) == 2
    request = observed[0]
    assert request.timeframes == ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
    # Catalog padding, not the absent authored metadata, controls the warmup.
    # The D1 child is the conservative maximum: (260 + 14 + 10) calendar
    # days of catalog-owned warmup, rather than the abbreviated profile's zero.
    assert request.data_start == "2023-04-23T00:00:00Z"
    manifest = json.loads((root / "evidence-envelope-manifest.json").read_text(encoding="utf-8"))
    assert manifest["constructionCatalog"]["catalogSha256"] == canonical_sha256(_catalog())
    assert manifest["constructionCatalog"]["catalogTimeframes"] == ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
    assert manifest["admittedEvidenceTimeframes"] == ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
    assert first["admittedEvidenceTimeframes"] == ["D1", "H1", "H4", "M1", "M15", "M30", "M5"]
    assert manifest["outputPreparation"]["preparationSha256"] == first["preparationSha256"]
    assert {row["variantId"].split("->")[-1] for row in manifest["memberVariants"] if row["variantId"] != "parent"} == {"M1", "M5", "M15", "M30", "H4", "D1"}
    output = json.loads((root / "preparation.json").read_text(encoding="utf-8"))
    plan = output["candidates"][0]["windowInputs"][0]["evidencePlan"]
    assert plan["lake_window_binding"]["request"] == request.canonical_payload()
    assert output["workerContract"]["workerContractSha256"] == "sha256:" + "f" * 64
    assert output["workerContract"]["workerContractSchema"] == "replay-worker-contract-v1"
    assert manifest["policyBinding"] == "bound_separately_by_panel_bridge_and_qd_supervisor"


def test_broad_envelope_rejects_unsupported_graph_timeframe(tmp_path: Path) -> None:
    template = _write(tmp_path / "template.json", _template())
    profile = _profile(timeframe="H8")
    population = _write(tmp_path / "population.json", _population(profile))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    with pytest.raises(TemporalDiscoveryContractError, match="absent from frozen catalog"):
        build_broad_evidence_envelope(
            source_preparation_path=template, seed_population_path=population,
            construction_catalog_path=catalog, output_root=tmp_path / "external", worker_contract_sha256="sha256:" + "f" * 64, worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=_ALL_TIMEFRAMES,
            attestor=lambda request, **_: _binding(request),
        )


def test_broad_envelope_rejects_forged_attestation_request(tmp_path: Path) -> None:
    template = _write(tmp_path / "template.json", _template())
    population = _write(tmp_path / "population.json", _population(_profile()))
    catalog = _write(tmp_path / "catalog.json", _catalog())

    def forged(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        return _binding(LakeWindowRequest(
            pairs=request.pairs, timeframes=request.timeframes,
            data_start="2024-01-25T00:00:00Z", data_end=request.data_end,
        ))

    with pytest.raises(TemporalDiscoveryContractError, match="forged or mismatched"):
        build_broad_evidence_envelope(
            source_preparation_path=template, seed_population_path=population,
            construction_catalog_path=catalog, output_root=tmp_path / "external", worker_contract_sha256="sha256:" + "f" * 64, worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=_ALL_TIMEFRAMES,
            attestor=forged,
        )


def test_broad_envelope_rejects_unattested_or_legacy_mismatched_remote_binding(tmp_path: Path) -> None:
    template_payload = _template()
    legacy = "sha256:" + "9" * 64
    template_payload["candidates"][0]["windowInputs"][0]["evidencePlan"]["lake_window_binding"]["legacy_selection_manifest_sha256"] = legacy
    old_plan = template_payload["candidates"][0]["windowInputs"][0]["evidencePlan"]
    old_plan.pop("plan_id")
    old_identity = dict(old_plan)
    old_identity.pop("lake_manifest_sha256", None)
    old_plan["plan_id"] = canonical_sha256(old_identity)
    template = _write(tmp_path / "template.json", template_payload)
    population = _write(tmp_path / "population.json", _population(_profile()))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    kwargs = {
        "source_preparation_path": template, "seed_population_path": population,
        "construction_catalog_path": catalog, "worker_contract_sha256": "sha256:" + "f" * 64,
        "worker_contract_schema": "replay-worker-contract-v1", "admitted_timeframes": _ALL_TIMEFRAMES,
    }
    with pytest.raises(TemporalDiscoveryContractError, match="omitted canonical attestation"):
        build_broad_evidence_envelope(
            **kwargs, output_root=tmp_path / "unattested",
            attestor=lambda request, **_: _binding(request, legacy=legacy, attest=False),
        )
    with pytest.raises(TemporalDiscoveryContractError, match="did not preserve legacy"):
        build_broad_evidence_envelope(
            **kwargs, output_root=tmp_path / "wrong-legacy",
            attestor=lambda request, **_: _binding(request, legacy=None),
        )


def test_broad_envelope_requires_explicit_worker_contract_schema(tmp_path: Path) -> None:
    template = _write(tmp_path / "template.json", _template())
    population = _write(tmp_path / "population.json", _population(_profile()))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    with pytest.raises(TemporalDiscoveryContractError, match="worker_contract_schema is required"):
        build_broad_evidence_envelope(
            source_preparation_path=template, seed_population_path=population,
            construction_catalog_path=catalog, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "f" * 64, worker_contract_schema="", admitted_timeframes=_ALL_TIMEFRAMES,
            attestor=lambda request, **_: _binding(request),
        )


@pytest.mark.parametrize(
    ("admitted", "message"),
    [([], "admitted_timeframes must be non-empty"), (["M5", "H8"], "absent from frozen catalog")],
)
def test_broad_envelope_rejects_empty_or_out_of_catalog_allowlist(
    tmp_path: Path, admitted: list[str], message: str
) -> None:
    template = _write(tmp_path / "template.json", _template())
    population = _write(tmp_path / "population.json", _population(_profile()))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    with pytest.raises(TemporalDiscoveryContractError, match=message):
        build_broad_evidence_envelope(
            source_preparation_path=template, seed_population_path=population,
            construction_catalog_path=catalog, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "f" * 64,
            worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=admitted,
            attestor=lambda request, **_: _binding(request),
        )


def test_broad_envelope_rejects_current_graph_parent_outside_admitted_subset(tmp_path: Path) -> None:
    template = _write(tmp_path / "template.json", _template())
    population = _write(tmp_path / "population.json", _population(_profile()))
    catalog = _write(tmp_path / "catalog.json", _catalog())
    with pytest.raises(TemporalDiscoveryContractError, match="outside the admitted evidence allowlist"):
        build_broad_evidence_envelope(
            source_preparation_path=template, seed_population_path=population,
            construction_catalog_path=catalog, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "f" * 64,
            worker_contract_schema="replay-worker-contract-v1", admitted_timeframes=["M1", "M5", "M15", "M30"],
            attestor=lambda request, **_: _binding(request),
        )
