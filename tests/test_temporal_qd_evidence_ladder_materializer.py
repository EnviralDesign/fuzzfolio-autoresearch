from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.lake_window import (
    LakeWindowBinding,
    LakeWindowRequest,
    lake_window_request_contains,
    resolve_replay_lake_window_request,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_evidence_ladder import (
    OUTER_TAIL_START,
    build_evidence_ladder,
    validate_template_discovery_windows,
    validate_template_stage_window,
)
from autoresearch.temporal_qd_evidence_ladder_materializer import materialize_qd_evidence_ladder
from autoresearch.temporal_search import build_authority


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _profile(*, timeframe: str = "H1") -> dict:
    return {
        "version": "v2",
        "graph": {
            "kind": "temporal_graph_v1",
            "evidenceGroups": [{"indicatorInstanceIds": ["signal"]}],
        },
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [{
            "meta": {"id": "FIXTURE", "instanceId": "signal"},
            "config": {"isActive": True, "timeframe": timeframe, "lookbackBars": 14},
        }],
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {"rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0},
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }


def _catalog() -> dict:
    return {
        "timeframes": {"M1": {}, "M5": {}, "M15": {}, "H1": {}, "D1": {}},
        "indicators": [{
            "meta": {"id": "FIXTURE", "requiredPaddingBars": 260},
            "config": {"isActive": True, "timeframe": "H1", "lookbackBars": 5},
        }],
    }


def _ladder_input() -> dict:
    return {
        "schemaVersion": "temporal_qd_evidence_ladder_input_v1",
        "frozenSeed": "materializer-fixture",
        "historicalMonthStarts": [
            "2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z",
            "2020-06-01T00:00:00Z", "2020-09-01T00:00:00Z",
        ],
        "validationWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2022-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2016-01-01T00:00:00Z", "analysisWindowEnd": "2019-01-01T00:00:00Z"},
    }


def _population() -> dict:
    profile = _profile()
    payload = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [{"candidateId": "seed-a", "sourceProfile": profile, "sourceProfileSha256": canonical_sha256(profile)}],
    }
    payload["populationSha256"] = canonical_sha256(payload)
    return payload


def _pair_config(
    timeframes: list[str],
    *,
    schema_version: str = "temporal_qd_bidirectional_pair_run_config_v1",
) -> dict:
    side = {
        "indicatorPolicy": {
            "schemaVersion": "temporal_indicator_learning_policy_v1",
            "policySha256": "sha256:" + "f" * 64,
            "timeframePolicy": timeframes,
        },
    }
    payload = {
        "schemaVersion": schema_version,
        "longModule": side,
        "shortModule": json.loads(json.dumps(side)),
    }
    payload["pairRunConfigSha256"] = canonical_sha256(payload)
    return payload


@pytest.mark.parametrize(
    "schema_version",
    [
        "temporal_qd_bidirectional_pair_run_config_v1",
        "temporal_qd_bidirectional_pair_run_config_v2",
    ],
)
def test_pair_config_identity_accepts_current_and_legacy_frozen_schemas(
    tmp_path: Path,
    schema_version: str,
) -> None:
    from autoresearch.temporal_qd_evidence_ladder_materializer import (
        _pair_config_identity,
    )

    path = _write(
        tmp_path / "pair.json",
        _pair_config(["M5", "H1"], schema_version=schema_version),
    )
    identity = _pair_config_identity(path)
    assert identity is not None
    assert identity["payload"]["schemaVersion"] == schema_version
    assert identity["pairRunConfigSha256"].startswith("sha256:")


def _attestor(observed: list[LakeWindowRequest]):
    def attest(request: LakeWindowRequest, *, legacy_selection_manifest_sha256: str | None) -> LakeWindowBinding:
        assert legacy_selection_manifest_sha256 is None
        observed.append(request)
        return LakeWindowBinding(
            request=request,
            window_semantic_sha256="sha256:" + "a" * 64,
            attestation_sha256="sha256:" + "b" * 64,
        )
    return attest


def test_materializer_is_deterministic_closed_contained_and_outer_tail_free(tmp_path: Path) -> None:
    ladder_path = _write(tmp_path / "ladder.json", _ladder_input())
    population_path = _write(tmp_path / "population.json", _population())
    catalog_path = _write(tmp_path / "catalog.json", _catalog())
    observed: list[LakeWindowRequest] = []
    root = tmp_path / "externally-frozen"

    first = materialize_qd_evidence_ladder(
        evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
        construction_catalog_path=catalog_path, output_root=root,
        worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5",
        attestor=_attestor(observed),
    )
    second = materialize_qd_evidence_ladder(
        evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
        construction_catalog_path=catalog_path, output_root=root,
        worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5",
        attestor=_attestor(observed),
    )
    assert first == second
    assert len(observed) == 10  # five exact windows per immutable re-run
    assert all(request.timeframes == ["D1", "H1", "M1", "M15", "M5"] for request in observed)

    config = json.loads((root / "evidence-ladder-config.json").read_text(encoding="utf-8"))
    assert config["evidenceLadderConfigSha256"] == canonical_sha256({key: value for key, value in config.items() if key != "evidenceLadderConfigSha256"})
    ladder = build_evidence_ladder(config)
    assert config["evidenceLadderSha256"] == ladder["evidenceLadderSha256"]
    assert config["discoveryTemplatePreparationPath"] == str((root / "discovery-template-preparation.json").resolve())

    manifest = json.loads((root / "materialization.json").read_text(encoding="utf-8"))
    assert manifest["remoteAttestationRequired"] is True
    assert manifest["outerTail"] == {"analysisWindowStart": OUTER_TAIL_START, "touched": False, "reservedEvidencePermitted": False}
    assert manifest["catalogCapabilityEnvelope"]["timeframePolicy"] == {
        "source": "frozen_construction_catalog_full_domain_without_pair_config",
        "pairRunConfigSha256": None,
        "timeframes": ["D1", "H1", "M1", "M15", "M5"],
    }
    for stage, expected_count in (("discovery", 3), ("validation", 1), ("scrutiny", 1)):
        template_path = Path(manifest["stages"][stage]["templatePath"])
        template = json.loads(template_path.read_text(encoding="utf-8"))
        authority = build_authority(template)
        assert authority["authorityId"] == manifest["stages"][stage]["templateAuthorityId"]
        assert len(template["developmentWindows"]) == expected_count
        if stage == "discovery":
            validate_template_discovery_windows(template, ladder)
        else:
            validate_template_stage_window(template, ladder, stage=stage)
        for row in manifest["stages"][stage]["windows"]:
            assert row["analysisWindowEnd"] <= OUTER_TAIL_START
            binding = row["remoteBinding"]["request"]
            assert all(lake_window_request_contains(binding, request["request"]) for request in row["reachableRequests"])

    tampered = json.loads((root / "validation-12m-template-preparation.json").read_text(encoding="utf-8"))
    tampered["authorityLabel"] = "substituted-template"
    with pytest.raises(TemporalDiscoveryContractError, match="preparation identity mismatch"):
        validate_template_stage_window(tampered, ladder, stage="validation")
    unknown = dict(config)
    unknown["unboundTemplatePath"] = "C:/not-authorized.json"
    unknown["evidenceLadderConfigSha256"] = canonical_sha256({key: value for key, value in unknown.items() if key != "evidenceLadderConfigSha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="unknown fields"):
        build_evidence_ladder(unknown)


def test_materializer_rejects_fabricated_or_unattested_lake_identity(tmp_path: Path) -> None:
    ladder_path = _write(tmp_path / "ladder.json", _ladder_input())
    population_path = _write(tmp_path / "population.json", _population())
    catalog_path = _write(tmp_path / "catalog.json", _catalog())

    def forged(request: LakeWindowRequest, **_: object) -> LakeWindowBinding:
        return LakeWindowBinding(
            request=LakeWindowRequest(pairs=request.pairs, timeframes=request.timeframes, data_start="2019-01-02T00:00:00Z", data_end=request.data_end),
            window_semantic_sha256="sha256:" + "d" * 64,
            attestation_sha256="sha256:" + "e" * 64,
        )

    with pytest.raises(TemporalDiscoveryContractError, match="forged or mismatched"):
        materialize_qd_evidence_ladder(
            evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
            construction_catalog_path=catalog_path, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "c" * 64,
            worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5", attestor=forged,
        )


def test_materializer_contains_catalog_capabilities_at_highest_pair_timeframe(tmp_path: Path) -> None:
    catalog = _catalog()
    catalog["indicators"].append({
        "meta": {"id": "GARCH_VOLATILITY_REGIME", "requiredPaddingBars": 1200},
        "config": {"isActive": True, "timeframe": "M5", "lookbackBars": 1},
    })
    ladder_path = _write(tmp_path / "ladder.json", _ladder_input())
    population_path = _write(tmp_path / "population.json", _population())
    catalog_path = _write(tmp_path / "catalog.json", catalog)
    pair_path = _write(tmp_path / "pair.json", _pair_config(["H1", "D1"]))
    observed: list[LakeWindowRequest] = []

    materialize_qd_evidence_ladder(
        evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
        construction_catalog_path=catalog_path, output_root=tmp_path / "external",
        worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5",
        bidirectional_pair_config_path=pair_path, attestor=_attestor(observed),
    )

    manifest = json.loads((tmp_path / "external" / "materialization.json").read_text(encoding="utf-8"))
    capability = manifest["catalogCapabilityEnvelope"]
    assert capability["admittedTimeframes"] == ["H1", "D1"]
    assert capability["lookbackBounds"] == {
        "policyChoicesMax": 5,
        "activeCatalogDefaultsMax": 5,
        "seedProfilesMax": 14,
        "maxReachable": 14,
    }
    assert capability["maxReachableEvidenceLookbackBars"] == 14
    assert capability["timeframePolicy"]["source"] == "bidirectional_pair_indicator_policy"
    assert capability["capabilityEnvelopeSha256"] == canonical_sha256(
        {key: value for key, value in capability.items() if key != "capabilityEnvelopeSha256"}
    )
    discovery = manifest["stages"]["discovery"]["windows"][0]
    expected = resolve_replay_lake_window_request(
        pairs=["EURUSD"], base_timeframe="M5",
        profile_snapshot={"indicators": [{
            "meta": {"id": "GARCH_VOLATILITY_REGIME"},
            "config": {"isActive": True, "timeframe": "D1", "lookbackBars": 14},
        }]},
        analysis_window_start=discovery["analysisWindowStart"],
        analysis_window_end=discovery["analysisWindowEnd"], frozen_catalog=catalog,
    )
    record = next(
        item for item in discovery["reachableRequests"]
        if item["dependencyKind"] == "catalog_capability"
        and item["indicatorId"] == "GARCH_VOLATILITY_REGIME"
        and item["timeframe"] == "D1"
    )
    assert record["lookbackBars"] == 14
    assert record["request"] == expected.canonical_payload()
    assert lake_window_request_contains(discovery["remoteBinding"]["request"], expected)
    assert all("D1" in request.timeframes for request in observed)


def test_materializer_rejects_malformed_seed_lookback_before_attestation(tmp_path: Path) -> None:
    population = _population()
    profile = population["candidates"][0]["sourceProfile"]
    profile["indicators"][0]["config"]["lookbackBars"] = "not-a-number"
    population["candidates"][0]["sourceProfileSha256"] = canonical_sha256(profile)
    population["populationSha256"] = canonical_sha256(
        {key: value for key, value in population.items() if key != "populationSha256"}
    )
    ladder_path = _write(tmp_path / "ladder.json", _ladder_input())
    population_path = _write(tmp_path / "population.json", population)
    catalog_path = _write(tmp_path / "catalog.json", _catalog())

    with pytest.raises(TemporalDiscoveryContractError, match="seed profile 'seed-a' indicator 0 lookbackBars is invalid"):
        materialize_qd_evidence_ladder(
            evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
            construction_catalog_path=catalog_path, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "c" * 64,
            worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5",
            attestor=_attestor([]),
        )


@pytest.mark.parametrize(
    ("timeframes", "mutate", "message"),
    [
        (["H1", "D1"], lambda pair: pair["shortModule"]["indicatorPolicy"].update({"timeframePolicy": ["H1"]}), "must be identical"),
        (["H1", "H4"], lambda _pair: None, "not backed by the frozen construction catalog"),
    ],
)
def test_materializer_fails_closed_for_pair_timeframe_policy_drift(
    tmp_path: Path, timeframes: list[str], mutate, message: str,
) -> None:
    pair = _pair_config(timeframes)
    mutate(pair)
    pair["pairRunConfigSha256"] = canonical_sha256(
        {key: value for key, value in pair.items() if key != "pairRunConfigSha256"}
    )
    ladder_path = _write(tmp_path / "ladder.json", _ladder_input())
    population_path = _write(tmp_path / "population.json", _population())
    catalog_path = _write(tmp_path / "catalog.json", _catalog())
    pair_path = _write(tmp_path / "pair.json", pair)

    with pytest.raises(TemporalDiscoveryContractError, match=message):
        materialize_qd_evidence_ladder(
            evidence_ladder_input_path=ladder_path, seed_population_path=population_path,
            construction_catalog_path=catalog_path, output_root=tmp_path / "external",
            worker_contract_sha256="sha256:" + "c" * 64,
            worker_contract_schema="replay-worker-contract-v1", base_timeframe="M5",
            bidirectional_pair_config_path=pair_path, attestor=_attestor([]),
        )
