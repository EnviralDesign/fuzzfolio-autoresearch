from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_campaign as qd_campaign

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.lake_window import LakeWindowBinding
from autoresearch.temporal_qd_campaign import freeze_qd_screening_campaign
from autoresearch.temporal_qd_evaluation_population import (
    load_evaluation_population,
    raw_file_sha256,
)
from autoresearch.temporal_qd_evolution import QD_POLICY_NAME, QD_POLICY_SHA256, qd_canonical_evidence_identity, qd_predeclared_evidence_context
from autoresearch.temporal_bidirectional_genome import IdentitySnapshot
from autoresearch.temporal_search import TemporalSearchContractError, build_authority, materialize_plan


def _write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _profile(*, indicator_timeframe: str | None = None) -> dict:
    profile = {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1"},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                }
            }
        },
    }
    if indicator_timeframe is not None:
        profile["indicators"] = [
            {
                "meta": {
                    "id": "FIXTURE",
                    "instanceId": "signal",
                    "requiredPaddingBars": 10,
                },
                "config": {
                    "isActive": True,
                    "timeframe": indicator_timeframe,
                    "lookbackBars": 14,
                },
            }
        ]
    return profile


def _template() -> dict:
    profile = _profile()
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": canonical_sha256(profile),
        "analysis_window_start": "2024-02-01T00:00:00Z",
        "analysis_window_end": "2024-03-01T00:00:00Z",
        "execution_cell_sha256": canonical_sha256(
            profile["executionConfig"]["exitPolicy"]["selectedCell"]
        ),
        "lake_window_binding": {
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "request": {
                "data_start": "2024-01-01T00:00:00Z",
                "data_end": "2024-03-01T00:00:00Z",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
    }
    plan["plan_id"] = canonical_sha256(plan)
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "frozen-development",
        "workerContract": {
            "workerContractSha256": "sha256:" + "c" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": "template",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": [{"windowId": "development", "evidencePlan": plan}],
            }
        ],
        "developmentWindows": [
            {
                "windowId": "development",
                "analysisWindowStart": "2024-02-01T00:00:00Z",
                "analysisWindowEnd": "2024-03-01T00:00:00Z",
            }
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2024-06-29T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "reserved",
            }
        ],
        "bounds": {
            "maxCandidates": 1,
            "maxDevelopmentWindows": 1,
            "maxTasks": 1,
            "maxAttempts": 2,
            "deadlineSeconds": 60,
        },
    }


def _catalog(*, required_padding_bars: int = 10) -> dict:
    return {
        "timeframes": {"M1": {}, "M5": {}, "M30": {}, "H1": {}},
        "indicators": [
            {
                "meta": {
                    "id": "FIXTURE",
                    "requiredPaddingBars": required_padding_bars,
                },
                "config": {
                    "isActive": True,
                    "timeframe": "M5",
                    "lookbackBars": 14,
                },
            }
        ],
    }


def _write_pair_evaluation_projection(population_path: Path, template: dict) -> dict:
    """Small immutable pair-sidecar fixture; the rich source is never decoded."""

    template["candidates"][0]["windowInputs"][0]["evidencePlan"]["coverage_policy"] = "require_complete"
    plan = template["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan.pop("plan_id", None)
    plan["plan_id"] = canonical_sha256(plan)
    profile = _profile()
    profile["version"] = "v3"
    profile["directionMode"] = "both"
    profile["graph"] = {"states": [], "transitions": []}
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": IdentitySnapshot.create(
            kind="pairCompiler", schema_version="pair_compiler_v1", payload={"test": True}
        ).canonical_payload(),
    }
    source_sha = canonical_sha256(profile)
    evidence = qd_predeclared_evidence_context(template)
    candidate = {
        "candidateId": "pair-0",
        "sourceMode": "qd_random_immigrant",
        "seedId": "fixture-seed",
        "candidateIdentitySha256": "sha256:" + "a" * 64,
        "programSha256": "sha256:" + "b" * 64,
        "sourceProfile": profile,
        "sourceProfileSha256": source_sha,
        "profileSnapshotSha256": source_sha,
        "structuralOperatorHistory": [],
        "proposalOrdinal": 0,
        "proposalEntrySha256": "sha256:" + "c" * 64,
    }
    candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(
        candidate, evidence
    )
    population_path.write_text('{"rich":"provenance"}\n', encoding="utf-8")
    projection = {
        "schemaVersion": "temporal_qd_evaluation_population_v1",
        "generationIndex": 1,
        "candidateCount": 1,
        "populationSha256": "sha256:" + "d" * 64,
        "populationFileSha256": raw_file_sha256(population_path),
        "pairGenerationConfigSha256": "sha256:" + "e" * 64,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "bidirectionalPairPolicy": policy,
        "pairPolicySha256": canonical_sha256(policy),
        "operatorImplementationSha256": canonical_sha256({"fixture": True}),
        "predeclaredEvidenceContextSha256": evidence["predeclaredEvidenceContextSha256"],
        "candidates": [candidate],
        "proposalAttempts": 1,
        "funnelEntries": [{
            "entrySha256": candidate["proposalEntrySha256"],
            "proposalOrdinal": 0,
            "originKind": "random_immigrant",
            "disposition": "accepted",
            "candidate": {"candidateId": "pair-0", "sourceProfileSha256": source_sha},
        }],
    }
    projection["evaluationPopulationSha256"] = canonical_sha256(projection)
    bindings = [{
        "candidateId": candidate["candidateId"],
        "proposalOrdinal": candidate["proposalOrdinal"],
        "proposalEntrySha256": candidate["proposalEntrySha256"],
        "candidateProjectionSha256": canonical_sha256(candidate),
    }]
    journal = {
        "schemaVersion": "temporal_qd_generation_journal_v3",
        "populationSha256": projection["populationSha256"],
        "configSha256": projection["pairGenerationConfigSha256"],
        "policyName": projection["policyName"],
        "policySha256": projection["policySha256"],
        "generationIndex": 1,
        "evaluationPopulationSha256": projection["evaluationPopulationSha256"],
        "populationFileSha256": projection["populationFileSha256"],
        "operatorImplementation": {"fixture": True},
        "predeclaredEvidenceContextSha256": projection[
            "predeclaredEvidenceContextSha256"
        ],
        "entrySha256s": [candidate["proposalEntrySha256"]],
        "proposalCount": 1,
        "acceptedCount": 1,
        "evaluationCandidateBindings": bindings,
    }
    journal["journalSha256"] = canonical_sha256(journal)
    _write(population_path.with_name("evaluation-population.json"), projection)
    _write(population_path.with_name("generation-journal.json"), journal)
    return projection


def test_sidecar_recomputes_canonical_evidence_when_context_is_embedded(
    tmp_path: Path,
) -> None:
    template = _template()
    population_path = tmp_path / "population.json"
    projection = _write_pair_evaluation_projection(population_path, template)
    context = qd_predeclared_evidence_context(template)
    projection["predeclaredEvidenceContext"] = context
    projection["candidates"][0]["canonicalEvidenceIdentitySha256"] = "sha256:" + "0" * 64
    projection["evaluationPopulationSha256"] = canonical_sha256(
        {key: value for key, value in projection.items() if key != "evaluationPopulationSha256"}
    )
    _write(population_path.with_name("evaluation-population.json"), projection)
    with pytest.raises(TemporalDiscoveryContractError, match="canonical evidence identity mismatch"):
        load_evaluation_population(population_path=population_path)


def test_pair_campaign_uses_compact_evaluation_population_without_loading_rich_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template = _template()
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    projection = _write_pair_evaluation_projection(population_path, template)
    _write(template_path, template)

    def no_full_population(_: Path) -> tuple[list[dict], str]:
        raise AssertionError("rich population must not be decoded")

    monkeypatch.setattr(qd_campaign, "_load_population", no_full_population)
    result = freeze_qd_screening_campaign(
        population_path=population_path,
        template_preparation_path=template_path,
        output_root=tmp_path / "campaign",
        execution_engine_commit="a" * 40,
    )
    campaign = json.loads((tmp_path / "campaign" / "campaign.json").read_text())
    assert result["evaluationPopulationSha256"] == projection["evaluationPopulationSha256"]
    assert campaign["evaluationPopulationSha256"] == projection["evaluationPopulationSha256"]
    assert result["taskCount"] == 1


def test_pair_campaign_fails_closed_when_rich_source_bytes_drift(
    tmp_path: Path,
) -> None:
    template = _template()
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    _write_pair_evaluation_projection(population_path, template)
    _write(template_path, template)
    population_path.write_text('{"rich":"tampered"}\n', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="raw source file identity"):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
        )


def test_pair_campaign_rejects_out_of_range_projection_ordinal_as_contract_error(
    tmp_path: Path,
) -> None:
    template = _template()
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    _write_pair_evaluation_projection(population_path, template)
    _write(template_path, template)
    projection_path = population_path.with_name("evaluation-population.json")
    journal_path = population_path.with_name("generation-journal.json")
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    projection.pop("evaluationPopulationSha256")
    projection["candidates"][0]["proposalOrdinal"] = 99
    projection["evaluationPopulationSha256"] = canonical_sha256(projection)
    journal.pop("journalSha256")
    journal["evaluationPopulationSha256"] = projection[
        "evaluationPopulationSha256"
    ]
    candidate = projection["candidates"][0]
    journal["evaluationCandidateBindings"] = [
        {
            "candidateId": candidate["candidateId"],
            "proposalOrdinal": candidate["proposalOrdinal"],
            "proposalEntrySha256": candidate["proposalEntrySha256"],
            "candidateProjectionSha256": canonical_sha256(candidate),
        }
    ]
    journal["journalSha256"] = canonical_sha256(journal)
    _write(projection_path, projection)
    _write(journal_path, journal)

    with pytest.raises(
        TemporalDiscoveryContractError, match="proposal ordinal is invalid"
    ):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
        )


def test_optimized_pre_sidecar_population_requires_a_fresh_truthful_root(
    tmp_path: Path,
) -> None:
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    _write(
        population_path,
        {
            "schemaVersion": "temporal_qd_generation_population_v3",
            "pairGenerationConfigSha256": "sha256:" + "a" * 64,
            "bidirectionalPairPolicy": {},
        },
    )
    _write(template_path, _template())
    with pytest.raises(TemporalDiscoveryContractError, match="fresh truthful root"):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
        )


def test_large_rotating_cohort_reaches_schema_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ReachedPopulationLoader(Exception):
        pass

    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    rotating_sha = "sha256:" + "b" * 64
    _write(
        population_path,
        {
            "schemaVersion": "temporal_qd_rotating_cohort_population_v1",
            "cohortRole": "retained_parent_current_panel",
            "proposalPopulation": False,
            "rotatingEvidenceSha256": rotating_sha,
        },
    )
    _write(template_path, _template())
    monkeypatch.setattr(qd_campaign, "_SMALL_POPULATION_FALLBACK_BYTES", 0)

    def reached_loader(_path: Path) -> tuple[list[dict], str]:
        raise ReachedPopulationLoader

    monkeypatch.setattr(qd_campaign, "_load_population", reached_loader)
    with pytest.raises(ReachedPopulationLoader):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
            rotating_evidence={"rotatingEvidenceSha256": rotating_sha},
            campaign_role="retained_parent_current_panel",
        )


@pytest.mark.parametrize("replacement", ["M1", "M30"])
def test_qd_campaign_refuses_timeframe_child_outside_pre_attested_scope(
    tmp_path: Path, replacement: str
) -> None:
    profile = _profile(indicator_timeframe=replacement)
    population = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [
            {
                "candidateId": "qd_timeframe_child",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "programSha256": "sha256:" + "d" * 64,
            }
        ],
    }
    population["populationSha256"] = canonical_sha256(population)
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    catalog_path = tmp_path / "catalog.json"
    template = _template()
    template["candidates"][0]["windowInputs"][0]["evidencePlan"]["coverage_policy"] = "require_complete"
    template["candidates"][0]["windowInputs"][0]["evidencePlan"]["plan_id"] = canonical_sha256(
        template["candidates"][0]["windowInputs"][0]["evidencePlan"]
    )
    _write(population_path, population)
    _write(template_path, template)
    _write(catalog_path, _catalog())

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="outside the immutable pre-attested evidence binding",
    ):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
            construction_catalog_path=catalog_path,
        )


def test_qd_campaign_rejects_abbreviated_profile_when_catalog_padding_exceeds_binding(
    tmp_path: Path,
) -> None:
    """The catalog, not authored indicator meta, sets the required warmup."""

    profile = _profile(indicator_timeframe="H1")
    profile["indicators"][0]["meta"].pop("requiredPaddingBars")
    population = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [
            {
                "candidateId": "qd_abbreviated_rsi",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "programSha256": "sha256:" + "d" * 64,
            }
        ],
    }
    population["populationSha256"] = canonical_sha256(population)
    template = _template()
    # An authored/default-zero calculation needs just one day (14+10 H1 bars)
    # and would accept this.  The frozen catalog requires 260 padding bars,
    # which moves the true day-aligned request back to 2024-01-20.
    template["candidates"][0]["windowInputs"][0]["evidencePlan"][
        "lake_window_binding"
    ]["request"] = {
        "data_start": "2024-01-31T00:00:00Z",
        "data_end": "2024-03-01T00:00:00Z",
        "pairs": ["EURUSD"],
        "timeframes": ["M5", "H1"],
    }
    plan = template["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["plan_id"] = canonical_sha256(plan)
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    catalog_path = tmp_path / "catalog.json"
    _write(population_path, population)
    _write(template_path, template)
    _write(catalog_path, _catalog(required_padding_bars=260))

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="outside the immutable pre-attested evidence binding",
    ):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
            construction_catalog_path=catalog_path,
        )


def test_qd_campaign_materializes_native_v3_bidirectional_candidate_without_weakening_evidence(
    tmp_path: Path,
) -> None:
    """The generic task authority transports, but does not reinterpret, v3."""

    profile = _profile()
    profile["version"] = "v3"
    profile["directionMode"] = "both"
    profile["graph"] = {
        "entryArbitration": {
            "modules": [
                {"direction": "long", "sourceProfileSnapshotSha256": "sha256:" + "a" * 64},
                {"direction": "short", "sourceProfileSnapshotSha256": "sha256:" + "b" * 64},
            ]
        }
    }
    population = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [{
            "candidateId": "native-v3-both",
            "sourceProfile": profile,
            "sourceProfileSha256": canonical_sha256(profile),
            "programSha256": "sha256:" + "c" * 64,
        }],
    }
    population["populationSha256"] = canonical_sha256(population)
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    catalog_path = tmp_path / "catalog.json"
    template = _template()
    template["candidates"][0]["windowInputs"][0]["evidencePlan"]["coverage_policy"] = "require_complete"
    template["candidates"][0]["windowInputs"][0]["evidencePlan"]["plan_id"] = canonical_sha256(
        template["candidates"][0]["windowInputs"][0]["evidencePlan"]
    )
    _write(population_path, population)
    _write(template_path, template)
    _write(catalog_path, _catalog())

    result = freeze_qd_screening_campaign(
        population_path=population_path,
        template_preparation_path=template_path,
        output_root=tmp_path / "campaign",
        execution_engine_commit="a" * 40,
        construction_catalog_path=catalog_path,
    )
    preparation_payload = json.loads((tmp_path / "campaign" / "preparation.json").read_text(encoding="utf-8"))
    authority = build_authority(preparation_payload)
    manifest = materialize_plan(authority, tmp_path / "materialized")
    assert manifest["authorityId"] == result["authorityId"]
    task = manifest["tasks"][0]["payload"]
    assert task["inline_profile_snapshot"] == profile
    assert task["required_worker_contract_hash"] == template["workerContract"]["workerContractSha256"]
    assert LakeWindowBinding.model_validate(task["evidence_plan"]["lake_window_binding"]) == LakeWindowBinding.model_validate(
        template["candidates"][0]["windowInputs"][0]["evidencePlan"]["lake_window_binding"]
    )

    rejected = json.loads((tmp_path / "campaign" / "preparation.json").read_text(encoding="utf-8"))
    for direction in ("long", None):
        bad = json.loads(json.dumps(rejected))
        candidate = bad["candidates"][0]
        candidate["sourceProfile"]["directionMode"] = direction
        candidate["sourceProfileSha256"] = canonical_sha256(candidate["sourceProfile"])
        with pytest.raises(TemporalSearchContractError, match="v3 source profile must use directionMode=both"):
            build_authority(bad)
