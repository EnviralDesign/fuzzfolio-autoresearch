from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_bidirectional_genome import FrozenModule, FrozenPair
import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_funnel_adapter as qd_funnel_adapter
import autoresearch.temporal_qd_pair_generation as pair_generation
import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_rotating_evidence import (
    ROTATING_EVIDENCE_INPUT_SCHEMA,
    build_candidate_window_evidence,
    build_rotating_evidence_contract,
)
from autoresearch.temporal_qd_rotating_evidence_materializer import (
    materialize_qd_rotating_evidence,
)
from autoresearch.temporal_generation_funnel import supervisor_funnel_snapshot
from test_temporal_qd_g0_bootstrap import (
    _Native as _g0_native,
    _PairCompiler as _G0PairCompiler,
    _pair as _g0_pair,
    _snapshot as _g0_snapshot,
    _source_profile as _g0_source_profile,
)
from test_temporal_qd_rotating_evidence_materializer import (
    _attestor as _rotating_attestor,
    _catalog as _rotating_catalog,
    _curriculum as _rotating_curriculum,
    _population as _rotating_seed_population,
)


def _write(path: Path, value: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def test_g0_record_counts_recover_legacy_native_pool_size_only_from_authority() -> None:
    binding = {
        "acceptedPoolSha256": "sha256:" + "a" * 64,
        "constructionPoolIdentitySha256": "sha256:" + "b" * 64,
        "ledgerSha256": "sha256:" + "c" * 64,
        "selectionSha256": "sha256:" + "d" * 64,
    }
    result = {
        "g0Bootstrap": binding,
        "constructedAcceptedCount": 4000,
    }
    config = {
        "g0Bootstrap": {
            "initialConstructionPoolSize": 4000,
            "evaluationPopulationSize": 1024,
        }
    }

    assert supervisor._g0_generation_record_fields(
        generation_result=result,
        config=config,
        generation_index=1,
    ) == {
        "g0Bootstrap": binding,
        "constructionPoolSize": 4000,
        "constructedAcceptedCount": 4000,
    }

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="construction counts drifted",
    ):
        supervisor._g0_generation_record_fields(
            generation_result={**result, "constructedAcceptedCount": 3999},
            config=config,
            generation_index=1,
        )


def test_completed_generation_counter_uses_total_rotating_worker_tasks(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(supervisor, "_validate_generation_artifacts", lambda **_: None)
    records = [
        {
            "generationIndex": 1,
            "candidateCount": 4,
            "taskCount": 16,
            "totalGenerationTaskCount": 24,
        },
        {
            "generationIndex": 2,
            "candidateCount": 4,
            "taskCount": 16,
            "totalGenerationTaskCount": 40,
        },
    ]
    state = {
        "completedGenerations": records,
        "uniqueCandidatesEvaluated": 8,
        "workerTasksCompleted": 64,
    }
    config = {
        "generationPlan": {"firstGenerationIndex": 1, "lastGenerationIndex": 5}
    }
    assert supervisor._validate_completed_generations(
        root=tmp_path, state=state, config=config
    ) == {1: records[0], 2: records[1]}


def test_completed_generation_validation_releases_indexed_cache_per_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_cache_sizes: list[int] = []

    def fake_validate_generation_artifacts(**kwargs) -> None:
        indexes = kwargs["tail_result_indexes"]
        assert indexes is not None
        seen_cache_sizes.append(len(indexes))
        indexes[
            tmp_path / f"generation-{kwargs['generation_record']['generationIndex']}"
        ] = {}

    monkeypatch.setattr(
        supervisor, "_validate_generation_artifacts", fake_validate_generation_artifacts
    )
    records = [
        {"generationIndex": 1, "candidateCount": 1, "taskCount": 1},
        {"generationIndex": 2, "candidateCount": 1, "taskCount": 1},
    ]
    state = {
        "completedGenerations": records,
        "uniqueCandidatesEvaluated": 2,
        "workerTasksCompleted": 2,
    }
    indexes: dict[Path, dict] = {}
    assert supervisor._validate_completed_generations(
        root=tmp_path,
        state=state,
        config={
            "generationPlan": {"firstGenerationIndex": 1, "lastGenerationIndex": 2}
        },
        tail_result_mode=supervisor.TAIL_RESULT_MODE_INDEXED,
        tail_result_indexes=indexes,
    ) == {1: records[0], 2: records[1]}
    assert seen_cache_sizes == [0, 0]
    assert indexes == {}


def test_rotating_task_upper_bounds_include_parent_fairness_and_backfill() -> None:
    contract = build_rotating_evidence_contract(
        {
            "schemaVersion": ROTATING_EVIDENCE_INPUT_SCHEMA,
            "developmentYears": [
                {
                    "analysisWindowStart": f"{year}-01-01T00:00:00Z",
                    "analysisWindowEnd": f"{year + 1}-01-01T00:00:00Z",
                }
                for year in range(2021, 2025)
            ],
            "validationWindow": {
                "analysisWindowStart": "2024-01-01T00:00:00Z",
                "analysisWindowEnd": "2025-01-01T00:00:00Z",
            },
            "scrutinyWindow": {
                "analysisWindowStart": "2021-01-01T00:00:00Z",
                "analysisWindowEnd": "2024-01-01T00:00:00Z",
            },
            "provisionalSurvivorCount": 128,
        }
    )
    bounds = supervisor._rotating_task_upper_bounds(
        contract=contract,
        first_generation_index=1,
        generation_count=5,
        proposal_width=1_024,
        initial_parent_count=0,
    )
    assert bounds["proposalCandidateEvaluations"] == 5_120
    assert bounds["retainedParentCandidatePanelsUpperBound"] == 512
    assert bounds["backfillCandidatePanelsUpperBound"] == 1_152
    assert bounds["workerTasksUpperBound"] == 27_136

    populated = supervisor._rotating_task_upper_bounds(
        contract=contract,
        first_generation_index=1,
        generation_count=5,
        proposal_width=1_024,
        initial_parent_count=300,
    )
    assert populated["initialParentCandidateCount"] == 300
    assert populated["retainedParentCandidatePanelsUpperBound"] == 812
    assert populated["workerTasksUpperBound"] == 28_336


def test_rotating_generation_transaction_g1_is_market_free_and_restart_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        supervisor, "_validate_frozen_sources", lambda _config, **_: []
    )
    templates = {
        f"panel-{index}": {
            "path": str(tmp_path / f"panel-{index}.json"),
            "preparationSha256": "sha256:" + str(index) * 64,
            "authorityId": "sha256:" + str(index + 4) * 64,
        }
        for index in range(1, 5)
    }
    contract = build_rotating_evidence_contract(
        {
            "schemaVersion": ROTATING_EVIDENCE_INPUT_SCHEMA,
            "developmentYears": [
                {
                    "analysisWindowStart": f"{year}-01-01T00:00:00Z",
                    "analysisWindowEnd": f"{year + 1}-01-01T00:00:00Z",
                }
                for year in range(2021, 2025)
            ],
            "validationWindow": {
                "analysisWindowStart": "2024-01-01T00:00:00Z",
                "analysisWindowEnd": "2025-01-01T00:00:00Z",
            },
            "scrutinyWindow": {
                "analysisWindowStart": "2021-01-01T00:00:00Z",
                "analysisWindowEnd": "2024-01-01T00:00:00Z",
            },
            "panelTemplates": templates,
            "provisionalSurvivorCount": 8,
        }
    )
    generation_root = tmp_path / "generations" / "generation-0001"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    proposal_root.mkdir(parents=True)
    campaign_root.mkdir(parents=True)
    proposal_campaign = {
        "schemaVersion": "fixture_campaign_v1",
        "campaignSha256": "sha256:" + "9" * 64,
        "taskCount": 4,
    }
    _write(campaign_root / "campaign.json", proposal_campaign)
    previous = qd.canonical_empty_bidirectional_archive_template()
    previous_path = tmp_path / "initial-archive.json"
    _write(previous_path, previous)
    profile = {"version": "v3", "directionMode": "both", "graph": {}}
    candidate = {
        "candidateId": "candidate-a",
        "candidateIdentitySha256": "sha256:" + "a" * 64,
        "programSha256": "sha256:" + "b" * 64,
        "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "profileSnapshotSha256": canonical_sha256(profile),
    }
    projection = {"candidates": [candidate]}
    monkeypatch.setattr(supervisor, "load_evaluation_population", lambda **_: projection)
    monkeypatch.setattr(
        supervisor,
        "hydrate_evaluation_candidate",
        lambda candidate, **_: dict(candidate),
    )
    member = {
        "candidateId": candidate["candidateId"],
        "candidate": dict(candidate),
            "aggregate": {"totalConservativeNetR": 4.0},
            "descriptor": {
                "operatorFamilies": "none",
                "mutationDepth": "root",
                "entryEvents": "none",
                "managementActions": "none",
                "graphNodes": "small",
                "tradeFrequency": "moderate",
                "medianHolding": "medium",
                "cellId": "none|root|none|none|small|moderate|medium",
                "structuralMeasurements": {"structuralComplexity": 2.0},
            },
        "objectives": {
            "worstWindowConservativeNetR": 1.0,
            "maximumDrawdownR": 0.5,
            "structuralComplexity": 2.0,
        },
        "finiteDataValidity": {
            "isFiniteData": True,
            "passesSupportGate": True,
            "validForQuality": True,
            "totalTrades": 48,
            "capTrades": 20,
        },
        "cappedTradeSupport": 20.0,
    }
    monkeypatch.setattr(
        supervisor,
        "load_qd_evaluated_members",
        lambda **_: {"members": [dict(member)]},
    )
    panel = contract["panels"][0]
    records = [
        build_candidate_window_evidence(
            candidate=candidate,
            panel=panel,
            window=window,
            metrics={
                "conservativeNetR": 1.0,
                "noCostNetR": 1.1,
                "maxDrawdownR": 0.5,
                "closedTrades": 12,
                "sourceProfileSnapshotSha256": candidate[
                    "profileSnapshotSha256"
                ],
                "resolvedProfileSnapshotSha256": "sha256:" + "7" * 64,
                "resolvedProgramSha256": "sha256:" + "8" * 64,
            },
            evidence_plan_semantic_sha256="sha256:" + "c" * 64,
            provenance={
                "authorityId": "sha256:" + "d" * 64,
                "taskMatrixSha256": "sha256:" + "e" * 64,
                "taskId": window["windowId"],
                "resultSha256": "sha256:" + "f" * 64,
            },
        )
        for window in panel["windows"]
    ]
    monkeypatch.setattr(
        supervisor,
        "_campaign_window_evidence",
        lambda **_: {candidate["candidateId"]: records},
    )
    config = {
        "rotatingEvidence": contract,
        "frozenSearchPolicy": {
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "capTrades": 20,
            "cellCapacity": 4,
        },
    }
    first = supervisor._complete_rotating_generation_transaction(
        root=tmp_path,
        generation_root=generation_root,
        generation_index=1,
        proposal_root=proposal_root,
        proposal_campaign_root=campaign_root,
        parent_archive_path=previous_path,
        archive_path=generation_root / "archive.json",
        config=config,
        client=object(),
    )
    second = supervisor._complete_rotating_generation_transaction(
        root=tmp_path,
        generation_root=generation_root,
        generation_index=1,
        proposal_root=proposal_root,
        proposal_campaign_root=campaign_root,
        parent_archive_path=previous_path,
        archive_path=generation_root / "archive.json",
        config=config,
        client=object(),
    )
    assert first == second
    assert first["qualityMemberCount"] == 1
    assert first["frontierMemberCount"] == 0
    ledger = json.loads(
        (generation_root / "evidence" / "generation-ledger.json").read_text()
    )
    assert ledger["proposalOnlyFunnelReporting"] is True
    assert ledger["retainedParentEvaluationCandidateIds"] == []
    checkpoint_path = generation_root / "evidence" / "checkpoint.json"
    corrupted = json.loads(checkpoint_path.read_text())
    corrupted["stage"] = "cumulative_backfill"
    _write(checkpoint_path, corrupted)
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        supervisor._complete_rotating_generation_transaction(
            root=tmp_path,
            generation_root=generation_root,
            generation_index=1,
            proposal_root=proposal_root,
            proposal_campaign_root=campaign_root,
            parent_archive_path=previous_path,
            archive_path=generation_root / "archive.json",
            config=config,
            client=object(),
        )


def _inputs(tmp_path: Path) -> dict:
    archive = {
        "schemaVersion": "temporal_qd_archive_v3",
        "qdVersion": supervisor.QD_VERSION,
        "policyName": supervisor.QD_POLICY_NAME,
        "policySha256": supervisor.QD_POLICY_SHA256,
        "frozenPolicy": supervisor.QD_POLICY,
        "generationIndex": 0,
        "populationSha256": canonical_sha256({"population": 0}),
        "resultSetSha256": canonical_sha256({"results": 0}),
        "previousArchiveSha256": None,
        "cellCapacity": 4,
        "candidateCountSeen": 1,
        "occupiedCellCount": 1,
        "memberCount": 1,
        "qualityMemberCount": 1,
        "observationalMemberCount": 0,
        "negativeNoveltyMemberCount": 0,
        "cells": [],
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    archive_path = tmp_path / "archive.json"
    _write(archive_path, archive)
    template_path = tmp_path / "template.json"
    _write(template_path, {"schemaVersion": "fixture_template_v1"})
    validator_path = tmp_path / "validator.json"
    _write(validator_path, ["fixture-validator"])
    catalog_path = tmp_path / "construction-catalog.json"
    _write(
        catalog_path,
        {
            "timeframes": {"M5": {"value": "M5"}},
            "indicators": [{"meta": {"id": "FIXTURE_INDICATOR"}}],
        },
    )
    parameters = {
        "version": supervisor.QD_VERSION,
        "seed": 7,
        "targetUniqueCandidates": 2,
        "immigrantProposalFraction": 0.2,
        "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
        "maxCumulativeStructuralDepth": 16,
        "maxProposalAttempts": 20,
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "cellCapacity": 4,
    }
    return {
        "initial_archive_path": archive_path,
        "source_preparation_path": tmp_path / "source.json",
        "base_generator_root": tmp_path / "generator",
        "confirmed_entry_admission_root": tmp_path / "admission",
        "template_preparation_path": template_path,
        "validator_command_file": validator_path,
        "parameters": parameters,
        "generation_count": 2,
        "autoresearch_commit": "a" * 40,
        "execution_engine_commit": "b" * 40,
        "worker_contract_sha256": "sha256:" + "c" * 64,
        "gateway_url": "http://127.0.0.1:8799",
        "gateway_token": "fixture",
        "construction_catalog_path": catalog_path,
        # These fixture campaigns cover the legacy Python materializer; make
        # that historical/oracle choice explicit now that Rust is the public
        # default.
        "generation_finalization_engine": supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
    }


def test_supervisor_restarts_exactly_at_completed_generation_boundaries(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[str, int, int]] = []
    campaign_runs: dict[str, tuple[Path, list[dict]]] = {}

    class FakeContinuation:
        def __init__(self, **kwargs):
            self.source_identity = {
                "schemaVersion": "fixture_immigrant_source_v1",
                "sourceIdentitySha256": canonical_sha256({"source": "fixture"}),
            }

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def close(self):
            pass

    def fake_generate(**kwargs):
        generation = int(kwargs["generation_index"])
        cursor = int(kwargs["immigrant_continuation_start"])
        root = Path(kwargs["output_root"])
        label = root.parents[2].name
        calls.append((label, generation, cursor))
        population = {
            "schemaVersion": "temporal_qd_generation_population_v3",
            "generationIndex": generation,
            "candidateCount": 2,
            "candidates": [],
        }
        population["populationSha256"] = canonical_sha256(population)
        journal = {
            "schemaVersion": "temporal_qd_generation_journal_v3",
            "generationIndex": generation,
            "nextImmigrantContinuationOrdinal": cursor + 1,
            "proposalSlots": {
                "targetUniqueCandidates": 2,
                "acceptedUniqueCandidates": 2,
                "proposalAttempts": 3,
                "remainingUniqueCandidateSlots": 0,
            },
            "uniqueIdentityCounts": {"candidateIdentity": generation * 2},
            "duplicateCounters": {"candidateIdentity": 0},
            "proposalSlotCounters": {"proposalsObserved": 3},
        }
        journal["journalSha256"] = canonical_sha256(journal)
        _write(root / "population.json", population)
        _write(root / "generation-journal.json", journal)
        return {
            "completed": True,
            "generationIndex": generation,
            "populationSha256": population["populationSha256"],
            "journalSha256": journal["journalSha256"],
            "proposalCount": 3,
            "candidateCount": 2,
            "originProposalCounts": {"structural_offspring": 2, "random_immigrant": 1},
            "originAcceptedCounts": {"structural_offspring": 1, "random_immigrant": 1},
            "proposalSlots": journal["proposalSlots"],
            "uniqueIdentityCounts": journal["uniqueIdentityCounts"],
            "duplicateCounters": journal["duplicateCounters"],
            "proposalSlotCounters": journal["proposalSlotCounters"],
            "nextImmigrantContinuationOrdinal": cursor + 1,
        }

    def fake_campaign(**kwargs):
        population = json.loads(Path(kwargs["population_path"]).read_text())
        generation = int(population["generationIndex"])
        root = Path(kwargs["output_root"])
        identity = {
            "schemaVersion": "fixture_qd_evaluation_identity_v1",
            "populationSha256": population["populationSha256"],
            "templatePreparationSha256": canonical_sha256(
                json.loads(Path(kwargs["template_preparation_path"]).read_text())
            ),
            "executionEngineCommit": kwargs["execution_engine_commit"],
            "workerContract": {
                "workerContractSha256": kwargs["worker_contract_sha256"]
            },
            "policySha256": supervisor.QD_POLICY_SHA256,
            "predeclaredEvidenceContextSha256": supervisor.qd_predeclared_evidence_context(
                json.loads(Path(kwargs["template_preparation_path"]).read_text()),
                worker_contract_sha256=kwargs["worker_contract_sha256"],
                construction_catalog=(
                    json.loads(Path(kwargs["construction_catalog_path"]).read_text())
                    if kwargs.get("construction_catalog_path")
                    else None
                ),
                construction_catalog_path=kwargs.get("construction_catalog_path"),
            )["predeclaredEvidenceContextSha256"],
        }
        identity["evaluationIdentitySha256"] = canonical_sha256(identity)
        preparation = {"schemaVersion": "fixture_preparation_v1", "generation": generation}
        authority = {"schemaVersion": "fixture_authority_v1", "generationIndex": generation}
        authority["authorityId"] = canonical_sha256(authority)
        tasks = [
            {
                "task_id": f"task-{generation}-{index}",
                "payload": {"candidate_id": f"candidate-{index}"},
            }
            for index in range(4)
        ]
        manifest = {
            "schemaVersion": "fixture_task_manifest_v1",
            "authorityId": authority["authorityId"],
            "taskCount": len(tasks),
            "tasks": tasks,
            "taskMatrixSha256": canonical_sha256(tasks),
        }
        campaign = {
            "schemaVersion": "fixture_qd_campaign_v1",
            "generationIndex": generation,
            "populationSha256": population["populationSha256"],
            "preparationSha256": canonical_sha256(preparation),
            "authorityId": authority["authorityId"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
        }
        campaign["campaignSha256"] = canonical_sha256(campaign)
        _write(root / "preparation.json", preparation)
        _write(root / "evaluation-identity.json", identity)
        _write(root / "authority.json", authority)
        _write(root / "campaign.json", campaign)
        _write(root / "screening-run" / "authority.json", authority)
        _write(root / "screening-run" / "task-manifest.json", manifest)
        _write(
            root / "screening-run" / "checkpoint.json",
            {
                "schemaVersion": "fixture_checkpoint_v1",
                "authorityId": authority["authorityId"],
                "taskMatrixSha256": manifest["taskMatrixSha256"],
                "completed": {},
                "journal": [],
            },
        )
        campaign_runs[authority["authorityId"]] = (root / "screening-run", tasks)
        return {
            "campaignSha256": campaign["campaignSha256"],
            "evaluationIdentitySha256": identity["evaluationIdentitySha256"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
            "taskCount": len(tasks),
        }

    def fake_search(client, authority, **kwargs):
        result_root, tasks = campaign_runs[authority["authorityId"]]
        checkpoint_path = result_root / "checkpoint.json"
        checkpoint = json.loads(checkpoint_path.read_text())
        completed_records = {}
        for task in tasks:
            task_id = task["task_id"]
            material = {"taskId": task_id, "candidateId": task["payload"]["candidate_id"]}
            result_path = result_root / "results" / f"{task_id}.json"
            _write(result_path, material)
            completed_records[task_id] = {
                "resultSha256": canonical_sha256(material),
                "resultPath": str(result_path.resolve()),
                "candidateId": task["payload"]["candidate_id"],
            }
        checkpoint["completed"] = completed_records
        checkpoint["journal"] = [
            {"taskId": task_id, **record}
            for task_id, record in sorted(completed_records.items())
        ]
        _write(checkpoint_path, checkpoint)
        _write(
            result_root / "summary.json",
            {
                "schemaVersion": "fixture_summary_v1",
                "authorityId": authority["authorityId"],
                "taskCount": len(tasks),
                "completedTaskCount": len(tasks),
            },
        )
        callback = kwargs["progress_callback"]
        # Deliberately report reverse completion order.  It is telemetry only.
        for completed, task in enumerate(reversed(tasks), start=1):
            callback(
                {
                    "taskId": task["task_id"],
                    "completedTaskCount": completed,
                    "taskCount": len(tasks),
                }
            )
        return {"completedTaskCount": len(tasks)}

    def fake_archive(**kwargs):
        generation = int(kwargs["generation_index"])
        output = Path(kwargs["output_path"])
        archive = {
            "schemaVersion": "temporal_qd_archive_v3",
            "generationIndex": generation,
            "resultSetSha256": canonical_sha256({"results": generation}),
        }
        archive["archiveSha256"] = canonical_sha256(archive)
        _write(output, archive)
        return {
            "archiveSha256": archive["archiveSha256"],
            "occupiedCellCount": generation + 1,
            "newCellCount": 1,
            "qualityMemberCount": 2,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
            "paretoAdmissionCount": 2,
            "paretoEvictionCount": 0,
        }

    monkeypatch.setattr(supervisor, "ExactGeneratorV2Continuation", FakeContinuation)
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(supervisor, "generate_qd_generation", fake_generate)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", fake_campaign)
    monkeypatch.setattr(supervisor, "run_temporal_search_tasks", fake_search)
    monkeypatch.setattr(supervisor, "build_qd_archive", fake_archive)

    inputs = _inputs(tmp_path)
    uninterrupted = supervisor.run_qd_supervisor(
        run_root=tmp_path / "uninterrupted", **inputs
    )
    paused = supervisor.run_qd_supervisor(
        run_root=tmp_path / "restarted",
        stop_after_generation=1,
        **inputs,
    )
    assert paused["status"] == "paused_at_generation_boundary"
    catalog_path = inputs["construction_catalog_path"]
    original_catalog = catalog_path.read_text(encoding="utf-8")
    _write(
        catalog_path,
        {
            "timeframes": {"M5": {"value": "M5"}, "M15": {"value": "M15"}},
            "indicators": [{"meta": {"id": "FIXTURE_INDICATOR"}}],
        },
    )
    with pytest.raises(TemporalDiscoveryContractError, match="frozen broad-run input"):
        supervisor.run_qd_supervisor(run_root=tmp_path / "restarted", **inputs)
    catalog_path.write_text(original_catalog, encoding="utf-8")
    resumed = supervisor.run_qd_supervisor(run_root=tmp_path / "restarted", **inputs)
    assert uninterrupted["status"] == resumed["status"] == "completed"
    full_state = json.loads((tmp_path / "uninterrupted" / "state.json").read_text())
    resumed_state = json.loads((tmp_path / "restarted" / "state.json").read_text())
    for full, restarted in zip(
        full_state["completedGenerations"],
        resumed_state["completedGenerations"],
        strict=True,
    ):
        ignored = {"completedAt", "archivePath", "artifacts"}
        assert {k: v for k, v in full.items() if k not in ignored} == {
            k: v for k, v in restarted.items() if k not in ignored
        }
        for artifact in ("population", "journal", "archive", "campaign", "evaluationIdentity"):
            identity = {
                "population": "populationSha256",
                "journal": "journalSha256",
                "archive": "archiveSha256",
                "campaign": "campaignSha256",
                "evaluationIdentity": "evaluationIdentitySha256",
            }[artifact]
            assert full["artifacts"][artifact][identity] == restarted["artifacts"][artifact][identity]
        assert [row["resultSha256"] for row in full["artifacts"]["results"]["records"]] == [
            row["resultSha256"] for row in restarted["artifacts"]["results"]["records"]
        ]
    assert [(generation, cursor) for _, generation, cursor in calls[:2]] == [
        (1, 0),
        (2, 1),
    ]
    assert [(generation, cursor) for _, generation, cursor in calls[2:]] == [
        (1, 0),
        (2, 1),
    ]
    assert resumed_state["uniqueCandidatesEvaluated"] == 4
    assert resumed_state["workerTasksCompleted"] == 8
    full_config = json.loads((tmp_path / "uninterrupted" / "config.json").read_text())
    resumed_config = json.loads((tmp_path / "restarted" / "config.json").read_text())
    assert full_config["configSha256"] == resumed_config["configSha256"]
    construction = full_config["constructionOperatorPolicy"]
    assert construction["enabled"] is True
    assert construction["catalog"]["path"] == str(inputs["construction_catalog_path"].resolve())
    assert construction["catalog"]["catalogSha256"] == canonical_sha256(
        json.loads(inputs["construction_catalog_path"].read_text())
    )
    calls_before_completed_restart = list(calls)
    completed_restart = supervisor.run_qd_supervisor(
        run_root=tmp_path / "restarted", **inputs
    )
    assert completed_restart["status"] == "completed"
    assert calls == calls_before_completed_restart

    result_path = Path(
        resumed_state["completedGenerations"][1]["artifacts"]["results"]["records"][0][
            "resultPath"
        ]
    )
    original_result = result_path.read_text(encoding="utf-8")
    result_path.write_text('{"tampered":true}\n', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="semantic identity mismatch"):
        supervisor.run_qd_supervisor(run_root=tmp_path / "restarted", **inputs)
    result_path.write_text(original_result, encoding="utf-8")
    assert supervisor.run_qd_supervisor(
        run_root=tmp_path / "restarted", **inputs
    )["status"] == "completed"

    summary_path = Path(
        resumed_state["completedGenerations"][1]["artifacts"]["summary"]["path"]
    )
    summary_path.unlink()
    with pytest.raises(TemporalDiscoveryContractError, match="missing QD evaluation summary"):
        supervisor.run_qd_supervisor(run_root=tmp_path / "restarted", **inputs)


def test_supervisor_refuses_mid_run_policy_change(tmp_path: Path, monkeypatch) -> None:
    class FakeContinuation:
        def __init__(self, **kwargs):
            self.source_identity = {"sourceIdentitySha256": canonical_sha256({"x": 1})}

    monkeypatch.setattr(supervisor, "ExactGeneratorV2Continuation", FakeContinuation)
    inputs = _inputs(tmp_path)
    # A pre-existing frozen config is enough to prove a changed policy is rejected
    # before a gateway client or generation is started.
    config, _ = supervisor._frozen_config(
        **{
            key: inputs[key]
            for key in (
                "initial_archive_path",
                "source_preparation_path",
                "base_generator_root",
                "confirmed_entry_admission_root",
                "template_preparation_path",
                "validator_command_file",
                "parameters",
                "generation_count",
                "autoresearch_commit",
                "execution_engine_commit",
                "worker_contract_sha256",
                "gateway_url",
            )
        },
        first_generation_index=1,
        initial_immigrant_continuation_ordinal=0,
        evaluation_timeout_seconds=86400.0,
        enqueue_batch_size=128,
        broad_admission=False,
    )
    run_root = tmp_path / "frozen"
    supervisor._write_once(run_root / "config.json", config)
    changed = {**inputs, "parameters": {**inputs["parameters"], "seed": 8}}
    try:
        supervisor.run_qd_supervisor(run_root=run_root, **changed)
    except TemporalDiscoveryContractError as exc:
        assert "frozen broad-run input" in str(exc)
    else:
        raise AssertionError("changed search policy was not rejected")


def test_supervisor_forwards_broad_admission_to_empty_quality_bootstrap(
    tmp_path: Path, monkeypatch
) -> None:
    class FakeContinuation:
        def __init__(self, **kwargs):
            self.source_identity = {
                "sourceIdentitySha256": canonical_sha256({"source": "fixture"})
            }

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def close(self):
            pass

    seen: list[bool] = []

    def fake_generate(**kwargs):
        seen.append(bool(kwargs["allow_empty_quality_bootstrap"]))
        raise RuntimeError("stop after generation input capture")

    monkeypatch.setattr(supervisor, "ExactGeneratorV2Continuation", FakeContinuation)
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(supervisor, "generate_qd_generation", fake_generate)
    inputs = _inputs(tmp_path)
    inputs["generation_count"] = supervisor.FRESH_BROAD_GENERATION_COUNT
    inputs["parameters"] = {
        **inputs["parameters"],
        "targetUniqueCandidates": supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "maxProposalAttempts": 1024,
    }
    ladder_input = {
        "schemaVersion": "temporal_qd_evidence_ladder_input_v1",
        "frozenSeed": "broad-fixture",
        "historicalMonthStarts": [
            "2020-01-01T00:00:00Z",
            "2020-03-01T00:00:00Z",
            "2020-06-01T00:00:00Z",
            "2020-09-01T00:00:00Z",
        ],
        "validationWindow": {
            "analysisWindowStart": "2021-01-01T00:00:00Z",
            "analysisWindowEnd": "2022-01-01T00:00:00Z",
        },
        "scrutinyWindow": {
            "analysisWindowStart": "2016-01-01T00:00:00Z",
            "analysisWindowEnd": "2019-01-01T00:00:00Z",
        },
    }
    ladder = supervisor.build_evidence_ladder(ladder_input)
    _write(
        inputs["template_preparation_path"],
        {"developmentWindows": ladder["discovery"]["windows"]},
    )
    validation_template = tmp_path / "validation-template.json"
    scrutiny_template = tmp_path / "scrutiny-template.json"
    _write(validation_template, {"developmentWindows": [ladder["validation"]["window"]]})
    _write(scrutiny_template, {"developmentWindows": [ladder["scrutiny"]["window"]]})
    inputs["evidence_ladder_config"] = {
        **ladder_input,
        "validationTemplatePreparationPath": str(validation_template),
        "scrutinyTemplatePreparationPath": str(scrutiny_template),
    }
    with pytest.raises(RuntimeError, match="input capture"):
        supervisor.run_qd_supervisor(
            run_root=tmp_path / "broad", broad_admission=True, **inputs
        )
    assert seen == [True]
    config = json.loads((tmp_path / "broad" / "config.json").read_text())
    assert config["emptyQualityBootstrapPolicy"] == {
        "activation": "only_when_generation_starts_without_quality_parent_cells",
        "enabledByBroadAdmission": True,
        "originSchedule": "generator_v2_random_immigrants_only",
    }
    assert config["broadAdmissionContract"] == {
        "schemaVersion": "temporal_qd_broad_admission_contract_v1",
        "generationCount": supervisor.FRESH_BROAD_GENERATION_COUNT,
        "candidatesPerGeneration": supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "candidateEvaluations": supervisor.FRESH_BROAD_CANDIDATE_EVALUATIONS,
        "discoveryWindowsPerCandidate": supervisor.FRESH_BROAD_DISCOVERY_WINDOWS_PER_CANDIDATE,
        "discoveryWorkerTasks": supervisor.FRESH_BROAD_DISCOVERY_WORKER_TASKS,
    }


def test_fresh_broad_admission_rejects_legacy_four_generation_shape(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    inputs["generation_count"] = supervisor.LEGACY_CONTINUATION_GENERATION_COUNT
    inputs["parameters"] = {
        **inputs["parameters"],
        "targetUniqueCandidates": supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "maxProposalAttempts": 1024,
    }

    with pytest.raises(TemporalDiscoveryContractError, match="five-generation"):
        supervisor.run_qd_supervisor(
            run_root=tmp_path / "legacy-shape-is-not-fresh-broad",
            broad_admission=True,
            **inputs,
        )


def test_pair_supervisor_freezes_rust_runtime_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _inputs(tmp_path)
    pair_config = {
        "schemaVersion": "fixture_pair_run_config_v1",
        "operatorImplementation": {"implementation": "fixture"},
    }
    monkeypatch.setattr(
        supervisor, "load_pair_run_config", lambda value: pair_config
    )

    config, _warnings = supervisor._frozen_config(
        initial_archive_path=inputs["initial_archive_path"],
        source_preparation_path=None,
        base_generator_root=None,
        confirmed_entry_admission_root=None,
        template_preparation_path=inputs["template_preparation_path"],
        validator_command_file=None,
        parameters=inputs["parameters"],
        generation_count=1,
        first_generation_index=1,
        initial_immigrant_continuation_ordinal=0,
        autoresearch_commit=inputs["autoresearch_commit"],
        execution_engine_commit=inputs["execution_engine_commit"],
        worker_contract_sha256=inputs["worker_contract_sha256"],
        gateway_url=inputs["gateway_url"],
        evaluation_timeout_seconds=60.0,
        enqueue_batch_size=100,
        broad_admission=False,
        construction_catalog_path=inputs["construction_catalog_path"],
        bidirectional_pair_config=pair_config,
    )

    assert config["pairGenerationRuntime"]["engine"] == (
        supervisor.PAIR_GENERATION_RUNTIME_RUST
    )
    assert config["pairGenerationRuntime"]["fallbackPolicy"] == "forbidden"


def test_pair_g0_supervisor_freezes_distinct_rust_finalization_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _inputs(tmp_path)
    pair_config = {
        "schemaVersion": "fixture_pair_run_config_v1",
        "operatorImplementation": {"implementation": "fixture"},
    }
    monkeypatch.setattr(
        supervisor, "load_pair_run_config", lambda value: pair_config
    )

    config, _warnings = supervisor._frozen_config(
        initial_archive_path=inputs["initial_archive_path"],
        source_preparation_path=None,
        base_generator_root=None,
        confirmed_entry_admission_root=None,
        template_preparation_path=inputs["template_preparation_path"],
        validator_command_file=None,
        parameters=inputs["parameters"],
        generation_count=1,
        first_generation_index=1,
        initial_immigrant_continuation_ordinal=0,
        autoresearch_commit=inputs["autoresearch_commit"],
        execution_engine_commit=inputs["execution_engine_commit"],
        worker_contract_sha256=inputs["worker_contract_sha256"],
        gateway_url=inputs["gateway_url"],
        evaluation_timeout_seconds=60.0,
        enqueue_batch_size=100,
        broad_admission=False,
        construction_catalog_path=inputs["construction_catalog_path"],
        bidirectional_pair_config=pair_config,
        pair_generation_engine=supervisor.PAIR_GENERATION_RUNTIME_PYTHON,
    )

    # v5 construction remains Python because it owns the live evolvable
    # factory/compiler.  Its independent G0 post-construction authority is
    # frozen to Rust with no fallback.
    assert config["pairGenerationRuntime"]["engine"] == (
        supervisor.PAIR_GENERATION_RUNTIME_PYTHON
    )
    assert config["g0FinalizationRuntime"]["engine"] == (
        supervisor.G0_FINALIZATION_RUNTIME_RUST
    )
    assert config["g0FinalizationRuntime"]["fallbackPolicy"] == "forbidden"

    # The separate post-construction authority is self-hashed inside the
    # already self-hashed supervisor config.  Rehashing only the outer config
    # must still reject a restart with a drifted G0 runtime.
    drifted = copy.deepcopy(config)
    drifted["g0FinalizationRuntime"]["executionTimeoutSeconds"] = 7200
    drifted["configSha256"] = canonical_sha256(
        {key: value for key, value in drifted.items() if key != "configSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        supervisor._validate_frozen_sources(drifted)


def test_pair_supervisor_generation_never_reads_or_forwards_legacy_validator(
    tmp_path: Path, monkeypatch
) -> None:
    """Pair generation must use only its frozen native authority contract."""

    class StopAfterPairGeneration(Exception):
        pass

    factory = object()
    operator = object()
    native_validator = object()
    compiler = object()
    pair_config = {
        "schemaVersion": "fixture_pair_run_config_v1",
        "operatorImplementation": {"implementation": "fixture"},
    }

    class FakePairAuthorityBundle:
        def __init__(self, frozen):
            assert frozen == pair_config
            self.factory = factory
            self.operator = operator
            self.validator = native_validator
            self.compiler = compiler

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def close(self):
            pass

    captured: dict = {}
    generation_kwargs: dict = {}
    pair_policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
    }

    def fake_pair_population(**kwargs):
        captured.update(kwargs)
        raise StopAfterPairGeneration

    def real_pair_generation(**kwargs):
        generation_kwargs.update(kwargs)
        return qd.generate_qd_generation(**kwargs)

    monkeypatch.setattr(
        supervisor, "load_pair_run_config", lambda value: pair_config
    )
    monkeypatch.setattr(
        supervisor, "PairAuthorityBundle", FakePairAuthorityBundle
    )
    monkeypatch.setattr(
        supervisor,
        "pair_policy_from_config",
        lambda frozen: pair_policy,
    )
    monkeypatch.setattr(qd, "_bidirectional_pair_policy", lambda _payload: pair_policy)
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(pair_generation, "generate_pair_population", fake_pair_population)

    # Keep the supervisor call real through the public generation boundary.
    # The pair-population stub stops immediately before any market or gateway
    # work, while proving that pair mode no longer needs legacy source kwargs.
    monkeypatch.setattr(supervisor, "generate_qd_generation", real_pair_generation)

    inputs = _inputs(tmp_path)
    inputs.update(
        {
            "generation_count": 1,
            "source_preparation_path": None,
            "base_generator_root": None,
            "confirmed_entry_admission_root": None,
                "validator_command_file": None,
                "bidirectional_pair_config": pair_config,
                "pair_generation_engine": supervisor.PAIR_GENERATION_RUNTIME_PYTHON,
            }
    )
    with pytest.raises(StopAfterPairGeneration):
        supervisor.run_qd_supervisor(run_root=tmp_path / "pair", **inputs)

    assert {
        "source_preparation_path",
        "base_generator_root",
        "confirmed_entry_admission_root",
        "validator_command",
        "validator_timeout_seconds",
    }.isdisjoint(generation_kwargs)
    assert captured["pair_policy"] == pair_policy
    assert captured["pair_factory"] is factory
    assert captured["module_authority"] is operator
    assert captured["native_validator"] is native_validator
    assert captured["pair_compiler"] is compiler
    assert captured["operator_implementation_identity"] == {
        "implementation": "fixture"
    }
    config = json.loads((tmp_path / "pair" / "config.json").read_text())
    assert "validator" not in config
    assert generation_kwargs["parent_archive_sha256"] == config["initialArchive"][
        "archiveSha256"
    ]


def test_broad_admission_refuses_missing_frozen_evidence_ladder(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    inputs["generation_count"] = supervisor.FRESH_BROAD_GENERATION_COUNT
    inputs["parameters"] = {
        **inputs["parameters"],
        "targetUniqueCandidates": supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "maxProposalAttempts": 1024,
    }
    with pytest.raises(TemporalDiscoveryContractError, match="frozen evidence ladder"):
        supervisor.run_qd_supervisor(
            run_root=tmp_path / "missing-ladder", broad_admission=True, **inputs
        )


def test_broad_admission_refuses_template_not_bound_to_three_discovery_windows(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    inputs["generation_count"] = supervisor.FRESH_BROAD_GENERATION_COUNT
    inputs["parameters"] = {
        **inputs["parameters"],
        "targetUniqueCandidates": supervisor.FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "maxProposalAttempts": 1024,
    }
    validation_template = tmp_path / "validation-template.json"
    scrutiny_template = tmp_path / "scrutiny-template.json"
    ladder_input = {
        "schemaVersion": "temporal_qd_evidence_ladder_input_v1",
        "frozenSeed": "template-binding-fixture",
        "historicalMonthStarts": [
            "2020-01-01T00:00:00Z",
            "2020-03-01T00:00:00Z",
            "2020-06-01T00:00:00Z",
            "2020-09-01T00:00:00Z",
        ],
        "validationWindow": {
            "analysisWindowStart": "2021-01-01T00:00:00Z",
            "analysisWindowEnd": "2022-01-01T00:00:00Z",
        },
        "scrutinyWindow": {
            "analysisWindowStart": "2016-01-01T00:00:00Z",
            "analysisWindowEnd": "2019-01-01T00:00:00Z",
        },
        "validationTemplatePreparationPath": str(validation_template),
        "scrutinyTemplatePreparationPath": str(scrutiny_template),
    }
    ladder = supervisor.build_evidence_ladder(ladder_input)
    _write(validation_template, {"developmentWindows": [ladder["validation"]["window"]]})
    _write(scrutiny_template, {"developmentWindows": [ladder["scrutiny"]["window"]]})
    with pytest.raises(TemporalDiscoveryContractError, match="exactly bind"):
        supervisor.run_qd_supervisor(
            run_root=tmp_path / "wrong-discovery-template",
            broad_admission=True,
            evidence_ladder_config=ladder_input,
            **inputs,
        )


def test_ladder_cohort_uses_only_ranked_quality_survivors_round_robin() -> None:
    def member(
        candidate_id: str, *, lane: str, front: int | None, crowding: float | None,
        quality: bool, net_r: float,
    ) -> dict:
        return {
            "candidateId": candidate_id,
            "candidate": {"candidateId": candidate_id},
            "archiveLane": lane,
            "paretoFront": front,
            "crowdingDistance": crowding,
            "finiteDataValidity": {
                "isFiniteData": True,
                "passesSupportGate": True,
                "validForQuality": quality,
            },
            "objectives": {
                "worstWindowConservativeNetR": net_r,
                "structuralComplexity": 1.0,
            },
        }

    archive = {
        "cells": [
            {
                "cellId": "cell-a",
                "members": [
                    # Candidate-ID ordering would put this ahead of z-first;
                    # archive Pareto/rank ordering must not.
                    member("a-later", lane="quality", front=1, crowding=0.0, quality=True, net_r=1.0),
                    member("z-first", lane="quality", front=0, crowding=0.0, quality=True, net_r=1.0),
                    member("negative", lane="negative_novelty", front=None, crowding=None, quality=True, net_r=-1.0),
                    member("observational", lane="observational", front=None, crowding=None, quality=False, net_r=2.0),
                ],
            },
            {
                "cellId": "cell-b",
                "members": [
                    member("b-first", lane="quality", front=0, crowding=0.0, quality=True, net_r=1.0),
                ],
            },
        ]
    }

    assert [row["candidateId"] for row in supervisor._ladder_cohort(archive, limit=4)] == [
        "z-first",
        "b-first",
        "a-later",
    ]


def test_evidence_ladder_fails_closed_without_quality_survivors(tmp_path: Path) -> None:
    archive_path = tmp_path / "archive.json"
    _write(
        archive_path,
        {
            "cells": [
                {
                    "cellId": "only-cell",
                    "members": [
                        {
                            "candidateId": "negative",
                            "candidate": {"candidateId": "negative"},
                            "archiveLane": "negative_novelty",
                            "finiteDataValidity": {
                                "isFiniteData": True,
                                "passesSupportGate": True,
                                "validForQuality": True,
                            },
                            "objectives": {
                                "worstWindowConservativeNetR": -1.0,
                                "structuralComplexity": 1.0,
                            },
                        },
                        {
                            "candidateId": "observational",
                            "candidate": {"candidateId": "observational"},
                            "archiveLane": "observational",
                            "finiteDataValidity": {
                                "isFiniteData": True,
                                "passesSupportGate": True,
                                "validForQuality": False,
                            },
                            "objectives": {
                                "worstWindowConservativeNetR": 1.0,
                                "structuralComplexity": 1.0,
                            },
                        },
                    ],
                }
            ]
        },
    )
    with pytest.raises(TemporalDiscoveryContractError, match="no diverse discovery survivors"):
        supervisor._run_evidence_ladder(
            root=tmp_path,
            config={
                "evidenceLadder": {
                    "evidenceLadderSha256": canonical_sha256({"ladder": "fixture"}),
                    "validation": {"maxDiverseSurvivorCount": 2},
                },
                "evidenceLadderExecution": {},
            },
            client=None,
            final_archive_path=archive_path,
        )


def test_completed_ladder_execution_reopens_all_stage_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "completed"
    ladder = {
        "evidenceLadderSha256": canonical_sha256({"ladder": "fixture"}),
        "outerTail": {"analysisWindowStart": "2024-01-01T00:00:00Z"},
    }
    execution = {
        "schemaVersion": "temporal_qd_evidence_ladder_execution_result_v1",
        "evidenceLadderSha256": ladder["evidenceLadderSha256"],
        "outerTail": ladder["outerTail"],
    }
    for stage in ("validation", "scrutiny"):
        stage_root = root / "evidence-ladder" / stage
        campaign_root = stage_root / "campaign"
        result_root = campaign_root / "screening-run"
        population = {"generationIndex": 0, "candidateCount": 2}
        population["populationSha256"] = canonical_sha256(population)
        preparation = {"schemaVersion": "fixture_preparation_v1"}
        authority = {"schemaVersion": "fixture_authority_v1"}
        authority["authorityId"] = canonical_sha256(authority)
        tasks = [
            {
                "task_id": f"{stage}-task-{index}",
                "payload": {"candidate_id": f"{stage}-candidate-{index}"},
            }
            for index in range(2)
        ]
        task_manifest = {
            "authorityId": authority["authorityId"],
            "tasks": tasks,
            "taskMatrixSha256": canonical_sha256(tasks),
        }
        evaluation_identity = {
            "populationSha256": population["populationSha256"],
            "templatePreparationSha256": canonical_sha256({"template": stage}),
        }
        evaluation_identity["evaluationIdentitySha256"] = canonical_sha256(
            evaluation_identity
        )
        campaign = {
            "generationIndex": 0,
            "candidateCount": 2,
            "populationSha256": population["populationSha256"],
            "preparationSha256": canonical_sha256(preparation),
            "authorityId": authority["authorityId"],
            "taskMatrixSha256": task_manifest["taskMatrixSha256"],
        }
        campaign["campaignSha256"] = canonical_sha256(campaign)
        archive = {"generationIndex": 0, "populationSha256": population["populationSha256"]}
        archive["archiveSha256"] = canonical_sha256(archive)
        _write(stage_root / "population.json", population)
        _write(campaign_root / "preparation.json", preparation)
        _write(campaign_root / "authority.json", authority)
        _write(campaign_root / "evaluation-identity.json", evaluation_identity)
        _write(campaign_root / "campaign.json", campaign)
        _write(result_root / "authority.json", authority)
        _write(result_root / "task-manifest.json", task_manifest)
        completed = {}
        for task in tasks:
            result = {"taskId": task["task_id"], "candidateId": task["payload"]["candidate_id"]}
            result_path = result_root / "results" / f"{task['task_id']}.json"
            _write(result_path, result)
            completed[task["task_id"]] = {
                "candidateId": task["payload"]["candidate_id"],
                "resultPath": str(result_path.resolve()),
                "resultSha256": canonical_sha256(result),
            }
        _write(
            result_root / "checkpoint.json",
            {
                "authorityId": authority["authorityId"],
                "taskMatrixSha256": task_manifest["taskMatrixSha256"],
                "completed": completed,
            },
        )
        _write(
            result_root / "summary.json",
            {
                "authorityId": authority["authorityId"],
                "taskCount": len(tasks),
                "completedTaskCount": len(tasks),
            },
        )
        _write(stage_root / "archive.json", archive)
        artifacts = supervisor._capture_screening_artifacts(
            population_path=stage_root / "population.json",
            archive_path=stage_root / "archive.json",
            campaign_root=campaign_root,
            generation_index=0,
            label=f"QD {stage} ladder",
        )
        execution[stage] = {
            "candidateCount": 2,
            "populationPath": str((stage_root / "population.json").resolve()),
            "populationSha256": population["populationSha256"],
            "campaignPath": str((stage_root / "campaign" / "campaign.json").resolve()),
            "campaignSha256": campaign["campaignSha256"],
            "archivePath": str((stage_root / "archive.json").resolve()),
            "archiveSha256": archive["archiveSha256"],
            "artifacts": artifacts,
        }
    execution["executionSha256"] = canonical_sha256(execution)
    execution_path = root / "evidence-ladder" / "execution.json"
    _write(execution_path, execution)
    state = {"evidenceLadderExecution": execution}
    config = {"evidenceLadder": ladder}

    supervisor._validate_evidence_ladder_execution(root=root, state=state, config=config)

    validation_result_root = root / "evidence-ladder" / "validation" / "campaign" / "screening-run"
    immutable_outputs = [
        root / "evidence-ladder" / "validation" / "campaign" / "preparation.json",
        root / "evidence-ladder" / "validation" / "campaign" / "authority.json",
        root / "evidence-ladder" / "validation" / "campaign" / "evaluation-identity.json",
        validation_result_root / "task-manifest.json",
        validation_result_root / "checkpoint.json",
        validation_result_root / "summary.json",
        validation_result_root / "results" / "validation-task-0.json",
    ]
    for artifact_path in immutable_outputs:
        original = artifact_path.read_text(encoding="utf-8")
        artifact_path.unlink()
        with pytest.raises(TemporalDiscoveryContractError):
            supervisor._validate_evidence_ladder_execution(
                root=root, state=state, config=config
            )
        artifact_path.write_text(original, encoding="utf-8")
        _write(artifact_path, {"drifted": True})
        with pytest.raises(TemporalDiscoveryContractError):
            supervisor._validate_evidence_ladder_execution(
                root=root, state=state, config=config
            )
        artifact_path.write_text(original, encoding="utf-8")

    execution_path.unlink()
    with pytest.raises(TemporalDiscoveryContractError, match="missing QD evidence ladder execution"):
        supervisor._validate_evidence_ladder_execution(root=root, state=state, config=config)
    _write(execution_path, execution)
    state["evidenceLadderExecution"] = {**execution, "outerTail": {"drifted": True}}
    with pytest.raises(TemporalDiscoveryContractError, match="disagrees with state"):
        supervisor._validate_evidence_ladder_execution(root=root, state=state, config=config)
    invalid_execution = {
        **execution,
        "executionSha256": canonical_sha256({"invalid": "execution"}),
    }
    _write(execution_path, invalid_execution)
    state["evidenceLadderExecution"] = invalid_execution
    with pytest.raises(TemporalDiscoveryContractError, match="execution identity mismatch"):
        supervisor._validate_evidence_ladder_execution(root=root, state=state, config=config)
    _write(execution_path, execution)
    state["evidenceLadderExecution"] = execution
    (root / "evidence-ladder" / "scrutiny" / "archive.json").unlink()
    with pytest.raises(TemporalDiscoveryContractError, match="missing QD scrutiny ladder archive"):
        supervisor._validate_evidence_ladder_execution(root=root, state=state, config=config)


def test_pair_g0_64_to_32_rotating_supervisor_restart_never_reschedules_construction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the G0 admission seam without a gateway, lake, or market call.

    G0's pool is built from real frozen pair entries instead of a hand-attested
    selection.  Only the evaluator and archive reducer are substituted: their
    immutable campaign artifacts still bind the exact 32-candidate, four-panel
    task matrix that the supervisor later reopens on restart.
    """
    # This bounded fixture proves supervisor restart semantics, not host
    # admission. Keep the production resource guard intact while making the
    # unit test independent of unrelated host workloads and CI machine size.
    monkeypatch.setattr(
        pair_generation.PerformanceTrace,
        "assert_resource_guard",
        lambda _self: None,
    )

    curriculum_path = tmp_path / "curriculum.json"
    seed_population_path = tmp_path / "seed-population.json"
    catalog_path = tmp_path / "catalog.json"
    _write(curriculum_path, _rotating_curriculum())
    _write(seed_population_path, _rotating_seed_population())
    catalog = _rotating_catalog()
    catalog["indicators"].extend(
        {
            "meta": {"id": identifier, "requiredPaddingBars": 260},
            "config": {"isActive": True, "timeframe": "M5", "lookbackBars": 14},
        }
        for identifier in ("FIXTURE_IMPL", "FIXTURE_IMPL_EVENT")
    )
    _write(catalog_path, catalog)
    rotating_root = tmp_path / "rotating"
    materialize_qd_rotating_evidence(
        rotating_evidence_input_path=curriculum_path,
        seed_population_path=seed_population_path,
        construction_catalog_path=catalog_path,
        output_root=rotating_root,
        worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1",
        base_timeframe="M5",
        attestor=_rotating_attestor([]),
    )
    rotating = json.loads(
        (rotating_root / "rotating-evidence-contract.json").read_text(
            encoding="utf-8"
        )
    )
    rotating_input = json.loads(
        (rotating_root / "rotating-evidence-config.json").read_text(
            encoding="utf-8"
        )
    )

    pair_config = {
        "schemaVersion": "fixture_pair_run_config_v1",
        "operatorImplementation": {"implementation": "g0-supervisor-fixture"},
    }
    pair_policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": _g0_pair(ordinal=0).pair_compiler.canonical_payload(),
    }

    class EvaluatablePairCompiler(_G0PairCompiler):
        def compile_pair(self, *, long_profile, short_profile, candidate_id):
            compiled = super().compile_pair(
                long_profile=long_profile,
                short_profile=short_profile,
                candidate_id=candidate_id,
            )
            profile = copy.deepcopy(compiled["profile"])
            profile["instruments"] = ["EURUSD"]
            validation = _g0_native().validate_v2(
                profile=profile, candidate_id=candidate_id
            )
            return {"profile": profile, "validation": validation}

    pair_compiler = EvaluatablePairCompiler()

    def evaluatable_pair(ordinal: int) -> FrozenPair:
        catalog_snapshot = _g0_snapshot(
            "catalog",
            {
                "catalog": {
                    "indicators": [
                        {"id": "FIXTURE_IMPL", "implementation": "FIXTURE_IMPL"},
                        {
                            "id": "FIXTURE_IMPL_EVENT",
                            "implementation": "FIXTURE_IMPL_EVENT",
                        },
                    ]
                },
                "catalogSha256": canonical_sha256({"catalog": "FIXTURE"}),
            },
        )

        def module(side: str) -> FrozenModule:
            profile = _g0_source_profile(
                side, family="FIXTURE", opaque=f"supervisor-g0-{ordinal}"
            )
            profile["instruments"] = ["EURUSD"]
            for indicator in profile["indicators"]:
                indicator["config"] = {
                    "isActive": True,
                    "timeframe": "M5",
                    "lookbackBars": 14,
                }
            profile["executionConfig"]["managementLibrary"]["plans"][0][
                "holdPolicy"
            ]["bars"] = ordinal + 1
            return FrozenModule.validate_native(
                program={
                    "schemaVersion": "temporal_typed_fragment_grammar_v2",
                    "grammarVersion": "3",
                    "direction": side,
                    "fragments": [],
                },
                profile=profile,
                grammar_context=_g0_snapshot("grammarContext", {"context": "fixture"}),
                catalog=catalog_snapshot,
                policy=_g0_snapshot("policy", {"policy": "fixture"}),
                native_authority_identity=_g0_snapshot(
                    "nativeAuthority", {"authority": "fixture"}
                ),
                native_validator=_g0_native(),
                candidate_id=f"g0-{ordinal}-{side}",
            )

        return FrozenPair.compile(
            long=module("long"),
            short=module("short"),
            pair_compiler_identity=_g0_snapshot(
                "pairCompiler", {"compiler": "g0-fixture", "mode": "live"}
            ),
            pair_compiler=pair_compiler,
            candidate_id=f"g0-pair-{ordinal}",
        )

    class FixturePairFactory:
        def __init__(self):
            # One frozen supervisor run may construct later generations with a
            # fresh authority bundle. Keep the factory's semantic sequence
            # global to this harness so G2 cannot collide with G0's ledger.
            self._ordinal = fixture_factory_ordinal[0]

        def create_pair(self, *, proposal_seed: str):
            del proposal_seed
            pair = evaluatable_pair(self._ordinal)
            self._ordinal += 1
            fixture_factory_ordinal[0] = self._ordinal
            return pair

    class FakePairAuthorityBundle:
        def __init__(self, frozen):
            assert frozen == pair_config
            self.factory = FixturePairFactory()
            self.operator = object()
            self.validator = object()
            self.compiler = pair_compiler

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def close(self):
            pass

    initial_archive = qd.canonical_empty_bidirectional_archive_template()
    initial_archive_path = tmp_path / "initial-archive.json"
    _write(initial_archive_path, initial_archive)
    fixture_factory_ordinal = [0]
    generation_calls: list[tuple[int, int | None, int | None]] = []
    campaign_runs: dict[str, tuple[Path, list[dict]]] = {}

    def fake_generate(**kwargs):
        generation = int(kwargs["generation_index"])
        construction = kwargs.get("initial_construction_pool_size")
        evaluation = kwargs.get("evaluation_population_size")
        generation_calls.append((generation, construction, evaluation))
        root = Path(kwargs["output_root"])
        if generation == 1:
            assert (construction, evaluation) == (64, 32)
            return qd.generate_qd_generation(**kwargs)
        assert generation == 2 and (construction, evaluation) == (None, None)
        return qd.generate_qd_generation(**kwargs)

    def fake_campaign(**kwargs):
        population_path = Path(kwargs["population_path"])
        population = json.loads(population_path.read_text(encoding="utf-8"))
        generation = int(population["generationIndex"])
        evaluation_population_path = population_path.with_name("evaluation-population.json")
        evaluation_population = (
            json.loads(evaluation_population_path.read_text(encoding="utf-8"))
            if evaluation_population_path.is_file()
            else None
        )
        panel = rotating["panels"][(generation - 1) % 4]
        candidate_count = int(population["candidateCount"])
        tasks = [
            {
                "task_id": f"g{generation}-c{candidate['candidateId']}-w{window['windowId']}",
                "payload": {
                    "candidate_id": candidate["candidateId"],
                    "analysis_window_start": window["analysisWindowStart"],
                    "analysis_window_end": window["analysisWindowEnd"],
                },
            }
            for candidate in population["candidates"]
            for window in panel["windows"]
        ]
        assert candidate_count == len(population["candidates"])
        root = Path(kwargs["output_root"])
        preparation = json.loads(
            Path(kwargs["template_preparation_path"]).read_text(encoding="utf-8")
        )
        authority = {
            "schemaVersion": "fixture_authority_v1",
            "generationIndex": generation,
            "developmentWindows": copy.deepcopy(panel["windows"]),
        }
        authority["authorityId"] = canonical_sha256(authority)
        identity = {
            "schemaVersion": "fixture_qd_evaluation_identity_v1",
            "populationSha256": population["populationSha256"],
            "templatePreparationSha256": canonical_sha256(preparation),
            "executionEngineCommit": kwargs["execution_engine_commit"],
            "workerContract": {"workerContractSha256": kwargs["worker_contract_sha256"]},
            "policySha256": supervisor.QD_POLICY_SHA256,
            "rotatingEvidence": kwargs["rotating_evidence"],
            **(
                {"evaluationPopulationSha256": evaluation_population["evaluationPopulationSha256"]}
                if evaluation_population is not None
                else {}
            ),
        }
        identity["evaluationIdentitySha256"] = canonical_sha256(identity)
        manifest = {
            "schemaVersion": "fixture_task_manifest_v1",
            "authorityId": authority["authorityId"],
            "taskCount": len(tasks),
            "tasks": tasks,
            "taskMatrixSha256": canonical_sha256(tasks),
        }
        campaign = {
            "schemaVersion": "fixture_qd_campaign_v1",
            "generationIndex": generation,
            "populationSha256": population["populationSha256"],
            "preparationSha256": canonical_sha256(preparation),
            "authorityId": authority["authorityId"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
            **(
                {"evaluationPopulationSha256": evaluation_population["evaluationPopulationSha256"]}
                if evaluation_population is not None
                else {}
            ),
        }
        campaign["campaignSha256"] = canonical_sha256(campaign)
        _write(root / "preparation.json", preparation)
        _write(root / "authority.json", authority)
        _write(root / "evaluation-identity.json", identity)
        _write(root / "campaign.json", campaign)
        _write(root / "screening-run" / "authority.json", authority)
        _write(root / "screening-run" / "task-manifest.json", manifest)
        _write(
            root / "screening-run" / "checkpoint.json",
            {
                "authorityId": authority["authorityId"],
                "taskMatrixSha256": manifest["taskMatrixSha256"],
                "completed": {},
                "journal": [],
            },
        )
        campaign_runs[authority["authorityId"]] = (root / "screening-run", tasks)
        return {
            "campaignSha256": campaign["campaignSha256"],
            "evaluationIdentitySha256": identity["evaluationIdentitySha256"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
            "taskCount": len(tasks),
        }

    def fake_search(_client, authority, **kwargs):
        result_root, tasks = campaign_runs[authority["authorityId"]]
        completed = {}
        for task in tasks:
            payload = task["payload"]
            stream_sha = canonical_sha256(
                {
                    "candidateId": payload["candidate_id"],
                    "windowStart": payload["analysis_window_start"],
                    "windowEnd": payload["analysis_window_end"],
                }
            )
            metrics = {
                "terminalAdjustedTotalNetR": 1.0,
                "terminalAdjustedMaxDrawdownR": 0.25,
            }
            replay = {
                "streamSha256": stream_sha,
                "graphTraces": [{"transitionId": "fixture-entry"}],
                "executionTraces": [
                    {"status": "scheduled"},
                    {"status": "filled"},
                    {"status": "closed"},
                ],
                "trades": [{"id": f"{task['task_id']}-trade-{ordinal}"} for ordinal in range(4)],
                "metrics": metrics,
            }
            material = {
                "schema_version": "temporal_graph_candidate_window_result_v1",
                "task_kind": "temporal_graph_candidate_window",
                "candidate_id": payload["candidate_id"],
                "analysis_window_start": payload["analysis_window_start"],
                "analysis_window_end": payload["analysis_window_end"],
                "observation_stream_sha256": stream_sha,
                "cost_view_results": {
                    "research_conservative": {
                        "cost_view": "research_conservative",
                        "observation_stream_sha256": stream_sha,
                        "replay_result": replay,
                    },
                    "none": {
                        "cost_view": "none",
                        "observation_stream_sha256": stream_sha,
                        "replay_result": copy.deepcopy(replay),
                    },
                },
            }
            result_path = result_root / "results" / f"{task['task_id']}.json"
            _write(result_path, material)
            completed[task["task_id"]] = {
                "candidateId": task["payload"]["candidate_id"],
                "resultPath": str(result_path.resolve()),
                "resultSha256": canonical_sha256(material),
            }
        checkpoint_path = result_root / "checkpoint.json"
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        checkpoint.update({"completed": completed, "journal": [{"taskId": task_id, **record} for task_id, record in completed.items()]})
        _write(checkpoint_path, checkpoint)
        _write(result_root / "summary.json", {"authorityId": authority["authorityId"], "taskCount": len(tasks), "completedTaskCount": len(tasks)})
        for count, task in enumerate(tasks, start=1):
            kwargs["progress_callback"]({"taskId": task["task_id"], "completedTaskCount": count, "taskCount": len(tasks)})
        return {"completedTaskCount": len(tasks)}

    def fake_rotating_archive(**kwargs):
        generation = int(kwargs["generation_index"])
        generation_root = Path(kwargs["generation_root"])
        archive = qd.canonical_empty_bidirectional_archive_template()
        archive["generationIndex"] = generation
        archive["resultSetSha256"] = canonical_sha256(
            {"generation": generation, "results": "fixture"}
        )
        archive.pop("archiveSha256", None)
        archive["archiveSha256"] = canonical_sha256(archive)
        _write(Path(kwargs["archive_path"]), archive)
        evidence = generation_root / "evidence"
        ledger = {"schemaVersion": "fixture_rotating_ledger_v1", "generationIndex": generation, "campaigns": []}
        ledger["ledgerSha256"] = canonical_sha256(ledger)
        checkpoint = {"schemaVersion": "fixture_rotating_checkpoint_v1", "generationIndex": generation}
        checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
        cumulative = {"schemaVersion": "fixture_cumulative_archive_v1", "generationIndex": generation}
        cumulative["archiveSha256"] = canonical_sha256(cumulative)
        _write(evidence / "generation-ledger.json", ledger)
        _write(evidence / "checkpoint.json", checkpoint)
        _write(evidence / "cumulative-archive.json", cumulative)
        return {
            "archiveSha256": archive["archiveSha256"],
            "resultSetSha256": archive["resultSetSha256"],
            "occupiedCellCount": generation,
            "newCellCount": 1,
            "qualityMemberCount": 0,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
            "paretoAdmissionCount": 0,
            "paretoEvictionCount": 0,
            "additionalWorkerTaskCount": 0,
            "rotatingEvidenceLedgerSha256": ledger["ledgerSha256"],
            "rotatingEvidenceCheckpointSha256": checkpoint["checkpointSha256"],
            "cumulativeArchiveSha256": cumulative["archiveSha256"],
            "frontierMemberCount": 0,
        }

    # Keep the real generic reducer in the path while retaining the ephemeral
    # proof authority it receives from the QD adapter for exact G0 assertions.
    generic_funnel_calls: list[dict | None] = []
    real_generic_funnel = qd_funnel_adapter.build_generation_funnel_artifact

    def capture_generic_funnel(**kwargs):
        authority = kwargs.get("g0_proof_authority")
        generic_funnel_calls.append(copy.deepcopy(authority))
        return real_generic_funnel(**kwargs)

    monkeypatch.setattr(supervisor, "load_pair_run_config", lambda _value: pair_config)
    monkeypatch.setattr(supervisor, "PairAuthorityBundle", FakePairAuthorityBundle)
    monkeypatch.setattr(supervisor, "pair_policy_from_config", lambda _value: pair_policy)
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(supervisor, "generate_qd_generation", fake_generate)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", fake_campaign)
    monkeypatch.setattr(supervisor, "run_temporal_search_tasks", fake_search)
    monkeypatch.setattr(supervisor, "_complete_rotating_generation_transaction", fake_rotating_archive)
    monkeypatch.setattr(
        qd_funnel_adapter, "build_generation_funnel_artifact", capture_generic_funnel
    )

    inputs = {
        "initial_archive_path": initial_archive_path,
        "source_preparation_path": None,
        "base_generator_root": None,
        "confirmed_entry_admission_root": None,
        "template_preparation_path": rotating_root / "panel-1-template-preparation.json",
        "validator_command_file": None,
        "parameters": {
            "version": supervisor.QD_VERSION,
            "seed": 17,
            "targetUniqueCandidates": 32,
            "immigrantProposalFraction": 0.2,
            "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
            "maxCumulativeStructuralDepth": 16,
            "maxProposalAttempts": 64,
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "capTrades": 20,
            "cellCapacity": 4,
        },
        "generation_count": 2,
        "autoresearch_commit": "a" * 40,
        "execution_engine_commit": "b" * 40,
        "worker_contract_sha256": "sha256:" + "c" * 64,
        "gateway_url": "http://127.0.0.1:8799",
            "gateway_token": "fixture",
            "bidirectional_pair_config": pair_config,
            "pair_generation_engine": supervisor.PAIR_GENERATION_RUNTIME_PYTHON,
            "rotating_evidence_config": rotating_input,
            "generation_finalization_engine": supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
        "initial_construction_pool_size": 64,
        "evaluation_population_size": 32,
        "generation_funnel_enabled": True,
    }
    run_root = tmp_path / "run"
    paused = supervisor.run_qd_supervisor(
        run_root=run_root, stop_after_generation=1, **inputs
    )
    assert paused["status"] == "paused_at_generation_boundary"
    assert generation_calls == [(1, 64, 32)]
    state = json.loads((run_root / "state.json").read_text(encoding="utf-8"))
    first = state["completedGenerations"][0]
    assert first["candidateCount"] == 32
    assert first["constructionPoolSize"] == first["constructedAcceptedCount"] == 64
    assert first["taskCount"] == 32 * len(rotating["panels"][0]["windows"])
    assert first["g0Bootstrap"] == json.loads(
        (run_root / "generations" / "generation-0001" / "proposal" / "evaluation-population.json").read_text(encoding="utf-8")
    )["g0Bootstrap"]
    g1_root = run_root / "generations" / "generation-0001"
    g1_proposal = g1_root / "proposal"
    g1_evaluation = json.loads(
        (g1_proposal / "evaluation-population.json").read_text(encoding="utf-8")
    )
    g1_funnel = json.loads((g1_root / "generation-funnel.json").read_text(encoding="utf-8"))
    assert supervisor_funnel_snapshot(g1_funnel)["funnelArtifactSha256"] == g1_funnel[
        "artifactSha256"
    ]
    assert g1_funnel["candidateCount"] == g1_funnel["attemptLedger"]["attemptCount"] == 32
    assert g1_funnel["attemptLedger"]["materializedCandidateCount"] == 32
    assert g1_funnel["proposalAccounting"]["dispositionCounts"] == {"accepted": 32}
    assert g1_funnel["proposalAccounting"]["originProposalCounts"] == {
        "random_immigrant": 32
    }
    assert g1_funnel["proposalAccounting"]["g0ConstructionProposalAccounting"][
        "dispositionCounts"
    ] == {"accepted": 64}
    selected_ids = {row["candidateId"] for row in g1_evaluation["candidates"]}
    selected_windows = {
        window["windowId"] for window in rotating["panels"][0]["windows"]
    }
    assert len(selected_ids) == 32
    assert {row["candidateId"] for row in g1_funnel["candidates"]} == selected_ids
    for row in g1_funnel["candidates"]:
        assert row["stages"]["evaluated"] == {
            "outcome": "evaluated",
            "reasons": [],
            "expectedWindowIds": sorted(selected_windows),
            "observedWindowIds": sorted(selected_windows),
        }
    g1_checkpoint = json.loads(
        (g1_root / "campaign" / "screening-run" / "checkpoint.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(g1_checkpoint["completed"]) == 32 * 4
    assert {
        row["candidateId"] for row in g1_checkpoint["completed"].values()
    } == selected_ids
    g0_root = g1_proposal / "g0-bootstrap"
    g0_pool = json.loads((g0_root / "accepted-pool.json").read_text(encoding="utf-8"))
    g0_selection = json.loads((g0_root / "selection.json").read_text(encoding="utf-8"))
    g0_ledger = json.loads(
        (g0_root / "campaign-construction-ledger.json").read_text(encoding="utf-8")
    )
    assert g1_evaluation["g0Bootstrap"] == {
        "constructionPoolIdentitySha256": g0_pool["constructionPoolIdentitySha256"],
        "acceptedPoolSha256": g0_pool["acceptedPoolSha256"],
        "selectionSha256": g0_selection["selectionSha256"],
        "ledgerSha256": g0_ledger["ledgerSha256"],
    }
    assert len(g0_pool["acceptedReferences"]) == len(g0_ledger["rows"]) == 64
    assert len(g0_selection["selected"]) == 32
    selected_by_id = {row["candidateId"]: row for row in g0_selection["selected"]}
    pool_by_reference = {
        row["referenceSha256"]: row for row in g0_pool["acceptedReferences"]
    }
    assert set(selected_by_id) == selected_ids
    assert len(generic_funnel_calls) == 1
    proof_authority = generic_funnel_calls[0]
    assert proof_authority is not None
    assert proof_authority["authoritySha256"] == canonical_sha256(
        {key: value for key, value in proof_authority.items() if key != "authoritySha256"}
    )
    assert len(proof_authority["proofs"]) == 32
    for proof in proof_authority["proofs"]:
        selection = selected_by_id[proof["candidateId"]]
        reference = pool_by_reference[selection["referenceSha256"]]
        assert proof["constructionProposalOrdinal"] == selection["proposalOrdinal"]
        assert proof["proposalEntrySha256"] == reference["acceptedPairEntrySha256"]
        assert {
            key: proof[key]
            for key in (
                "constructionPoolIdentitySha256",
                "acceptedPoolSha256",
                "selectionSha256",
                "ledgerSha256",
            )
        } == g1_evaluation["g0Bootstrap"]
    g1_journal_root = run_root / "generations" / "generation-0001" / "proposal" / "proposal-journal"
    g1_journal_bytes = {
        path.name: path.read_bytes() for path in sorted(g1_journal_root.glob("*.json"))
    }
    assert len(g1_journal_bytes) == 64
    for payload in g1_journal_bytes.values():
        entry = json.loads(payload)
        assert "identityChecks" not in entry
        assert "predeclaredLakeScope" not in entry
        assert "canonicalEvidenceIdentitySha256" not in entry["candidate"]
        assert entry["entrySha256"] == canonical_sha256(
            {key: value for key, value in entry.items() if key != "entrySha256"}
        )

    resumed = supervisor.run_qd_supervisor(run_root=run_root, **inputs)
    assert resumed["status"] == "completed"
    assert generation_calls == [(1, 64, 32), (2, None, None)]
    assert {
        path.name: path.read_bytes()
        for path in sorted(g1_journal_root.glob("*.json"))
    } == g1_journal_bytes
    state = json.loads((run_root / "state.json").read_text(encoding="utf-8"))
    assert [row["taskCount"] for row in state["completedGenerations"]] == [128, 128]
    assert state["workerTasksCompleted"] == 256
    g2_proposal = run_root / "generations" / "generation-0002" / "proposal"
    assert not (g2_proposal / "g0-bootstrap").exists()
    assert (g2_proposal / "evaluation-population.json").is_file()
    assert len(generic_funnel_calls) == 2
    assert generic_funnel_calls[1] is None

    calls_before_exact_restart = list(generation_calls)
    exact_restart = supervisor.run_qd_supervisor(run_root=run_root, **inputs)
    assert exact_restart["status"] == "completed"
    assert generation_calls == calls_before_exact_restart
    assert {
        path.name: path.read_bytes()
        for path in sorted(g1_journal_root.glob("*.json"))
    } == g1_journal_bytes


def test_generation_finalization_engine_branches_before_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected: list[str] = []

    def capture(**kwargs):
        selected.append(kwargs["finalization_engine"])
        return {"engine": kwargs["finalization_engine"]}

    monkeypatch.setattr(supervisor, "_run_rotating_generation_transaction", capture)
    assert supervisor._complete_rotating_generation_transaction()["engine"] == "python"
    assert (
        supervisor._complete_rotating_generation_transaction_native()["engine"]
        == "rust"
    )
    assert selected == ["python", "rust"]


def test_generation_finalization_engine_is_closed() -> None:
    assert supervisor._normalize_generation_finalization_engine("python") == "python"
    assert supervisor._normalize_generation_finalization_engine("rust") == "rust"
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="generation finalization engine must be one of",
    ):
        supervisor._normalize_generation_finalization_engine("fallback")


def test_native_prefinal_artifacts_preserve_result_authority_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation_root = tmp_path / "generation-0002"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"

    def self_hashed(payload: dict, field: str) -> dict:
        return supervisor._native_self_hash(payload, field)

    documents = {
        (generation_root / "proposal" / "generation-journal.json").resolve(): self_hashed(
            {"generationIndex": 2}, "journalSha256"
        ),
        (campaign_root / "preparation.json").resolve(): {"prepared": True},
        (campaign_root / "authority.json").resolve(): self_hashed(
            {"role": "campaign"}, "authorityId"
        ),
        (campaign_root / "evaluation-identity.json").resolve(): self_hashed(
            {"role": "identity"}, "evaluationIdentitySha256"
        ),
        (campaign_root / "campaign.json").resolve(): self_hashed(
            {"role": "campaign"}, "campaignSha256"
        ),
        (result_root / "task-manifest.json").resolve(): {"tasks": []},
        (result_root / "authority.json").resolve(): self_hashed(
            {"role": "result"}, "authorityId"
        ),
        (result_root / "checkpoint.json").resolve(): {"completed": {}},
        (result_root / "summary.json").resolve(): {"completedTaskCount": 0},
    }

    monkeypatch.setattr(
        supervisor,
        "_canonical_file",
        lambda path, *, name: documents[Path(path).resolve()],
    )
    monkeypatch.setattr(
        supervisor,
        "_results_descriptor",
        lambda **_kwargs: {"schemaVersion": "fixture_results_v1"},
    )
    evaluation_population = self_hashed(
        {
            "populationFileSha256": "sha256:" + "1" * 64,
            "populationSha256": "sha256:" + "2" * 64,
        },
        "evaluationPopulationSha256",
    )

    artifacts = supervisor._native_prefinal_artifact_ledger_base(
        generation_root=generation_root,
        generation_index=2,
        evaluation_population=evaluation_population,
        tail_result_index={"schemaVersion": "fixture_tail_index_v1"},
    )

    result_authority = documents[(result_root / "authority.json").resolve()]
    assert artifacts["resultAuthority"] == supervisor._self_hashed_descriptor(
        result_root / "authority.json",
        result_authority,
        field="authorityId",
        name="native pre-final result authority",
    )


def test_native_artifact_compatibility_accepts_only_result_authority_projection() -> None:
    current = {
        "schemaVersion": "temporal_qd_supervisor_generation_artifacts_v1",
        "resultAuthority": {
            "path": "C:/run/authority.json",
            "sha256": "sha256:" + "1" * 64,
            "authorityId": "sha256:" + "2" * 64,
        },
        "archive": {"archiveSha256": "sha256:" + "3" * 64},
    }
    legacy_native = copy.deepcopy(current)
    legacy_native["resultAuthority"].pop("authorityId")

    assert supervisor._generation_artifact_ledgers_match(
        recorded=legacy_native,
        current=current,
        allow_native_result_authority_identity_projection=True,
    )
    assert not supervisor._generation_artifact_ledgers_match(
        recorded=legacy_native,
        current=current,
        allow_native_result_authority_identity_projection=False,
    )

    tampered = copy.deepcopy(legacy_native)
    tampered["archive"].pop("archiveSha256")
    assert not supervisor._generation_artifact_ledgers_match(
        recorded=tampered,
        current=current,
        allow_native_result_authority_identity_projection=True,
    )


def test_screening_artifact_capture_can_use_compact_population_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    population_path = tmp_path / "proposal" / "population.json"
    evaluation_population_path = population_path.with_name(
        "evaluation-population.json"
    )
    evaluation_population_path.parent.mkdir(parents=True)
    evaluation_population_path.write_text("{}\n", encoding="utf-8")
    observed: list[bool] = []

    def stop_after_population_resolution(**kwargs):
        observed.append(kwargs["verify_population_file"])
        raise RuntimeError("population mode captured")

    monkeypatch.setattr(
        supervisor, "load_evaluation_population", stop_after_population_resolution
    )
    with pytest.raises(RuntimeError, match="population mode captured"):
        supervisor._capture_screening_artifacts(
            population_path=population_path,
            archive_path=tmp_path / "archive.json",
            campaign_root=tmp_path / "campaign",
            generation_index=2,
            label="native restart",
            verify_population_file=False,
        )

    assert observed == [False]


def test_native_generation_capture_skips_deep_auxiliary_campaign_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation_root = tmp_path / "generations" / "generation-0002"
    evidence_root = generation_root / "evidence"
    ledger_path = evidence_root / "generation-ledger.json"
    ledger_path.parent.mkdir(parents=True)
    ledger_path.write_text("{}\n", encoding="utf-8")
    journal = supervisor._native_self_hash(
        {"generationIndex": 2}, "journalSha256"
    )
    funnel = supervisor._native_self_hash(
        {"generationIndex": 2}, "artifactSha256"
    )
    ledger = supervisor._native_self_hash(
        {
            "campaigns": [
                {
                    "role": "prior_panel_backfill",
                    "campaignRoot": "C:/large-campaign",
                    "populationPath": "C:/large-population.json",
                    "artifacts": {"bound": True},
                }
            ]
        },
        "ledgerSha256",
    )
    checkpoint = supervisor._native_self_hash(
        {"generationIndex": 2}, "checkpointSha256"
    )
    cumulative = supervisor._native_self_hash(
        {"generationIndex": 2}, "archiveSha256"
    )
    documents = {
        (generation_root / "proposal" / "generation-journal.json").resolve(): journal,
        (generation_root / "generation-funnel.json").resolve(): funnel,
        ledger_path.resolve(): ledger,
        (evidence_root / "checkpoint.json").resolve(): checkpoint,
        (evidence_root / "cumulative-archive.json").resolve(): cumulative,
    }
    monkeypatch.setattr(
        supervisor,
        "_capture_screening_artifacts",
        lambda **_kwargs: {
            "schemaVersion": "temporal_qd_supervisor_generation_artifacts_v1"
        },
    )
    monkeypatch.setattr(
        supervisor,
        "_canonical_file",
        lambda path, *, name: documents[Path(path).resolve()],
    )
    monkeypatch.setattr(
        supervisor,
        "supervisor_funnel_snapshot",
        lambda _funnel: {"snapshotSha256": "sha256:" + "9" * 64},
    )
    monkeypatch.setattr(
        supervisor,
        "_rotating_campaign_artifacts",
        lambda **_kwargs: pytest.fail("compact native capture reopened rich campaign"),
    )

    artifacts = supervisor._capture_generation_artifacts(
        root=tmp_path,
        generation_index=2,
        generation_funnel_enabled=True,
        verify_population_file=False,
        verify_rotating_campaign_artifacts=False,
    )

    assert artifacts["rotatingEvidenceLedger"]["ledgerSha256"] == ledger[
        "ledgerSha256"
    ]


def test_native_cutover_cannot_silently_downgrade_to_python(tmp_path: Path) -> None:
    supervisor._require_irreversible_native_cutover_engine(
        root=tmp_path,
        generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
    )
    _write(tmp_path / "native-finalization-authority.json", {"frozen": True})
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="every restart must explicitly select the Rust",
    ):
        supervisor._require_irreversible_native_cutover_engine(
            root=tmp_path,
            generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
        )
    supervisor._require_irreversible_native_cutover_engine(
        root=tmp_path,
        generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_RUST,
    )

    (tmp_path / "native-finalization-authority.json").unlink()
    state = {
        "completedGenerations": [
            {"generationIndex": 1, "nativeGenerationFinalization": {"bound": True}}
        ]
    }
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="every restart must explicitly select the Rust",
    ):
        supervisor._require_irreversible_native_cutover_engine(
            root=tmp_path,
            generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
            state=state,
        )


def _identity_ledger_fixture(count: int) -> dict:
    value = {
        "schemaVersion": "temporal_qd_identity_ledger_v3",
        "uniqueCounts": {"candidateIdentity": count},
        "duplicateCounters": {"candidateIdentity": 0},
        "proposalSlotCounters": {
            "acceptedUniqueProposalSlots": count,
            "duplicateRejections": 0,
            "proposalsObserved": count,
        },
        "records": [{"candidateIdentitySha256": "sha256:" + f"{count:064x}"}],
    }
    value["ledgerSha256"] = canonical_sha256(value)
    return value


def _identity_ledger_record(ledger: dict) -> dict:
    return {
        "uniqueIdentityCounts": ledger["uniqueCounts"],
        "duplicateCounters": ledger["duplicateCounters"],
        "proposalSlotCounters": ledger["proposalSlotCounters"],
    }


def test_native_pair_identity_ledger_transaction_rolls_back_then_promotes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "run"
    state_path = root / "state.json"
    input_ledger = _identity_ledger_fixture(4)
    output_ledger = _identity_ledger_fixture(6)
    monkeypatch.setattr(
        supervisor,
        "_native_generation_output_ledger_sha256",
        lambda **_kwargs: output_ledger["ledgerSha256"],
    )
    _write(root / "identity-ledger.json", input_ledger)
    state = {
        "schemaVersion": "test_state_v1",
        "currentGenerationIndex": 2,
        "completedGenerations": [],
    }
    supervisor._save_state(state_path, state)

    supervisor._prepare_native_pair_identity_ledger_transaction(
        root=root,
        state=state,
        state_path=state_path,
        generation_index=2,
        input_ledger=input_ledger,
        input_ledger_sha256=input_ledger["ledgerSha256"],
    )
    output_path = supervisor._generation_identity_ledger_path(root, 2)
    _write(output_path, output_ledger)
    # Reproduce the native batch's pre-boundary mutable facade publication.
    _write(root / "identity-ledger.json", output_ledger)
    sealed, sealed_sha256 = supervisor._seal_native_pair_identity_ledger_output(
        root=root,
        state=state,
        state_path=state_path,
        generation_index=2,
        input_ledger=input_ledger,
        input_ledger_sha256=input_ledger["ledgerSha256"],
        generation_result=_identity_ledger_record(output_ledger),
    )
    assert sealed == output_ledger
    assert sealed_sha256 == output_ledger["ledgerSha256"]
    assert json.loads((root / "identity-ledger.json").read_text()) == input_ledger
    assert state["identityLedgerTransaction"]["phase"] == "proposal_committed"

    # An exact incomplete restart reuses the same frozen input transaction.
    supervisor._prepare_native_pair_identity_ledger_transaction(
        root=root,
        state=state,
        state_path=state_path,
        generation_index=2,
        input_ledger=input_ledger,
        input_ledger_sha256=input_ledger["ledgerSha256"],
    )
    assert state["identityLedgerTransaction"]["phase"] == "proposal_committed"

    supervisor._promote_native_pair_identity_ledger(
        root=root,
        state=state,
        state_path=state_path,
        generation_index=2,
        generation_record=_identity_ledger_record(output_ledger),
    )
    assert json.loads((root / "identity-ledger.json").read_text()) == output_ledger
    assert "identityLedgerTransaction" not in state


@pytest.mark.parametrize(
    "crash_point", ["ready_saved", "root_promoted", "transaction_cleared"]
)
def test_native_pair_identity_ledger_ready_crash_finishes_on_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    root = tmp_path / crash_point
    state_path = root / "state.json"
    input_ledger = _identity_ledger_fixture(4)
    output_ledger = _identity_ledger_fixture(6)
    record = {"generationIndex": 2, **_identity_ledger_record(output_ledger)}
    monkeypatch.setattr(
        supervisor,
        "_native_generation_output_ledger_sha256",
        lambda **_kwargs: output_ledger["ledgerSha256"],
    )
    _write(root / "identity-ledger.json", input_ledger)
    _write(supervisor._generation_identity_ledger_path(root, 2), output_ledger)
    state = {
        "schemaVersion": supervisor.SUPERVISOR_STATE_SCHEMA,
        "configSha256": None,
        "currentGenerationIndex": 3,
        "completedGenerations": [record],
        "identityLedgerTransaction": {
            "schemaVersion": "temporal_qd_identity_ledger_transaction_v1",
            "generationIndex": 2,
            "inputLedgerPath": str(
                supervisor._identity_ledger_input_snapshot_path(root, 2).resolve()
            ),
            "inputLedgerSha256": input_ledger["ledgerSha256"],
            "outputLedgerPath": str(
                supervisor._generation_identity_ledger_path(root, 2).resolve()
            ),
            "outputLedgerSha256": output_ledger["ledgerSha256"],
            "phase": "proposal_committed",
        },
    }
    _write(
        supervisor._identity_ledger_input_snapshot_path(root, 2), input_ledger
    )
    supervisor._save_state(state_path, state)

    def crash(step: str) -> None:
        if step == crash_point:
            raise RuntimeError("injected identity-ledger boundary crash")

    with pytest.raises(RuntimeError, match="boundary crash"):
        supervisor._promote_native_pair_identity_ledger(
            root=root,
            state=state,
            state_path=state_path,
            generation_index=2,
            generation_record=record,
            _after_step=crash,
        )
    persisted = supervisor._load_state(state_path, config_sha256=None)
    repaired, repaired_sha256 = supervisor._reconcile_native_pair_identity_ledger(
        root=root,
        state=persisted,
        state_path=state_path,
        completed_by_index={2: record},
    )
    assert repaired == output_ledger
    assert repaired_sha256 == output_ledger["ledgerSha256"]
    assert "identityLedgerTransaction" not in persisted
    assert json.loads((root / "identity-ledger.json").read_text()) == output_ledger


def test_native_pair_identity_ledger_restart_repairs_only_bound_crash_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "run"
    committed = _identity_ledger_fixture(4)
    incomplete = _identity_ledger_fixture(6)
    monkeypatch.setattr(
        supervisor,
        "_native_generation_output_ledger_sha256",
        lambda **_kwargs: committed["ledgerSha256"],
    )
    committed_path = supervisor._generation_identity_ledger_path(root, 1)
    incomplete_path = supervisor._generation_identity_ledger_path(root, 2)
    _write(committed_path, committed)
    _write(incomplete_path, incomplete)
    _write(root / "identity-ledger.json", incomplete)
    record = {"generationIndex": 1, **_identity_ledger_record(committed)}
    state = {
        "currentGenerationIndex": 2,
        "completedGenerations": [record],
    }
    repaired, repaired_sha256 = supervisor._reconcile_native_pair_identity_ledger(
        root=root,
        state=state,
        completed_by_index={1: record},
    )
    assert repaired == committed
    assert repaired_sha256 == committed["ledgerSha256"]
    assert json.loads((root / "identity-ledger.json").read_text()) == committed

    invented = _identity_ledger_fixture(99)
    _write(root / "identity-ledger.json", invented)
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="neither committed nor a bound crash-window output",
    ):
        supervisor._reconcile_native_pair_identity_ledger(
            root=root,
            state=state,
            completed_by_index={1: record},
        )


@pytest.mark.parametrize("crash_after", [1, 2, 3, 4, 5])
def test_native_publication_converges_after_every_step_without_new_batch(
    tmp_path: Path, crash_after: int
) -> None:
    root = tmp_path / f"run-{crash_after}"
    generation_root = root / "generations" / "generation-0002"
    native_root = generation_root / "native-finalization"
    cohort_sha256 = "sha256:" + "a" * 64
    rotating_sha256 = "sha256:" + "b" * 64
    outputs = {
        "cumulative-archive.json": {"kind": "cumulative", "archiveSha256": "sha256:" + "c" * 64},
        "archive.json": {"kind": "archive", "archiveSha256": "sha256:" + "d" * 64},
        "checkpoint.json": {
            "generationIndex": 2,
            "rotatingEvidenceSha256": rotating_sha256,
            "cohortSha256": cohort_sha256,
            "stage": "cumulative_archive",
            "checkpointSha256": "sha256:" + "e" * 64,
        },
        "generation-ledger.json": {"kind": "ledger", "ledgerSha256": "sha256:" + "f" * 64},
        "generation-funnel.json": {"kind": "funnel", "artifactSha256": "sha256:" + "1" * 64},
    }
    for name, payload in outputs.items():
        _write(native_root / name, payload)
    for name in (
        "generation-funnel-snapshot.json",
        "generation-record.json",
        "generation-state-patch.json",
    ):
        _write(native_root / name, {"kind": name})
    commit = {
        "schemaVersion": "temporal_qd_generation_commit_v1",
        "generationIndex": 2,
    }
    commit["commitSha256"] = canonical_sha256(commit)
    _write(native_root / "generation-commit.json", commit)
    manifest = {"schemaVersion": "fixture_manifest", "identity": "fixed"}
    _write(native_root / "manifest.json", manifest)
    manifest_bytes = (native_root / "manifest.json").read_bytes()
    batch_marker = generation_root / "proposal" / "native-batch-count.txt"
    batch_marker.parent.mkdir(parents=True, exist_ok=True)
    batch_marker.write_text("1\n", encoding="utf-8")
    prefinal = {
        "generationIndex": 2,
        "rotatingEvidenceSha256": rotating_sha256,
        "cohortSha256": cohort_sha256,
        "stage": "cumulative_backfill",
        "checkpointSha256": "sha256:" + "2" * 64,
    }
    _write(generation_root / "evidence" / "checkpoint.json", prefinal)

    def crash(_name: str, step: int) -> None:
        if step == crash_after:
            raise RuntimeError("injected publication crash")

    with pytest.raises(RuntimeError, match="injected publication crash"):
        supervisor._publish_native_generation_outputs(
            root=root,
            generation_index=2,
            _after_step=crash,
        )
    published = supervisor._publish_native_generation_outputs(
        root=root, generation_index=2
    )
    assert published["generation-commit.json"] == commit
    destinations = {
        "cumulative-archive.json": generation_root / "evidence" / "cumulative-archive.json",
        "archive.json": generation_root / "archive.json",
        "checkpoint.json": generation_root / "evidence" / "checkpoint.json",
        "generation-ledger.json": generation_root / "evidence" / "generation-ledger.json",
        "generation-funnel.json": generation_root / "generation-funnel.json",
    }
    for name, destination in destinations.items():
        assert json.loads(destination.read_text(encoding="utf-8")) == outputs[name]
        assert destination.read_bytes() == (native_root / name).read_bytes()
    assert (native_root / "manifest.json").read_bytes() == manifest_bytes
    assert batch_marker.read_text(encoding="utf-8") == "1\n"
    journal = json.loads(
        (native_root / "publication-journal.json").read_text(encoding="utf-8")
    )
    assert journal["completedSteps"] == list(outputs)
    assert journal["journalSha256"] == canonical_sha256(
        {key: value for key, value in journal.items() if key != "journalSha256"}
    )


def test_native_rotating_archive_result_uses_compact_committed_record(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    native_root = supervisor._native_finalization_root(root, 2)
    _write(native_root / "manifest.json", {"schemaVersion": "fixture_manifest"})
    record = {
        "generationIndex": 2,
        "archiveSha256": "sha256:" + "a" * 64,
        "cumulativeArchiveSha256": "sha256:" + "b" * 64,
        "occupiedCellCount": 3,
        "qualityMemberCount": 1,
        "frontierMemberCount": 2,
        "observationalMemberCount": 0,
        "negativeNoveltyMemberCount": 0,
        "newCellCount": 2,
        "paretoAdmissionCount": 3,
        "paretoEvictionCount": 1,
        "rotatingEvidenceLedgerSha256": "sha256:" + "c" * 64,
        "rotatingEvidenceCheckpointSha256": "sha256:" + "d" * 64,
        "taskCount": 16,
        "totalGenerationTaskCount": 21,
        "parentSchedule": {"schemaVersion": "fixture_schedule"},
    }
    record["generationRecordSha256"] = canonical_sha256(record)
    state_patch = {
        "generationIndex": 2,
        "generationRecordSha256": record["generationRecordSha256"],
        "generationRecord": record,
    }
    state_patch["statePatchSha256"] = canonical_sha256(state_patch)
    commit = {"generationIndex": 2, "commitSha256": "sha256:" + "e" * 64}

    result = supervisor._native_rotating_archive_result(
        root=root,
        generation_index=2,
        published={
            "generation-record.json": record,
            "generation-state-patch.json": state_patch,
            "generation-commit.json": commit,
        },
    )

    assert result["archiveSha256"] == record["archiveSha256"]
    assert result["memberCount"] == 3
    assert result["additionalWorkerTaskCount"] == 5
    assert result["nativeGenerationRecord"] == record
    assert result["nativeStatePatch"] == state_patch


def test_native_production_binding_joins_record_commit_and_state_patch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "run"
    native_root = supervisor._native_finalization_root(root, 2)
    record = {"generationIndex": 2, "archiveSha256": "sha256:" + "a" * 64}
    record["generationRecordSha256"] = canonical_sha256(record)
    state_patch = {
        "schemaVersion": "temporal_qd_generation_state_patch_v1",
        "generationIndex": 2,
        "generationRecord": record,
        "generationRecordSha256": record["generationRecordSha256"],
    }
    state_patch["statePatchSha256"] = canonical_sha256(state_patch)
    _write(native_root / "generation-record.json", record)
    _write(native_root / "generation-state-patch.json", state_patch)
    binary = tmp_path / "fixture.exe"
    binary.write_bytes(b"finalizer-v1")
    suffix = ".exe" if supervisor.os.name == "nt" else ""
    (tmp_path / f"temporal-qd-campaign-seal{suffix}").write_bytes(b"seal-v1")
    (tmp_path / f"temporal-qd-tail-reducer{suffix}").write_bytes(b"reducer-v1")
    authority = supervisor._freeze_native_finalization_runtime_authority(
        root=root, finalizer_binary=binary, state={"completedGenerations": []}
    )
    manifest = supervisor._native_self_hash(
        {
            "runtimeAuthoritySha256": authority["authoritySha256"],
            "sourceSha256": "sha256:" + "e" * 64,
        },
        "manifestSha256",
    )
    _write(native_root / "manifest.json", manifest)
    commit = {
        "commitSha256": "sha256:" + "c" * 64,
        "generationRecord": {
            "generationRecordSha256": record["generationRecordSha256"]
        },
        "statePatch": {"statePatchSha256": state_patch["statePatchSha256"]},
    }
    binding = supervisor._native_production_generation_binding(
        root=root,
        generation_index=2,
        manifest=manifest,
        commit=commit,
    )
    current = {**record, "nativeGenerationFinalization": binding}
    monkeypatch.setattr(
        supervisor,
        "_invoke_native_finalizer",
        lambda **_kwargs: {
            "restart": True,
            "restartValidation": "compact_commit_and_output_hashes",
            "commitSha256": commit["commitSha256"],
            "commit": commit,
        },
    )
    supervisor._validate_native_generation_binding(
        generation_record=current, binary=binary
    )

    tampered_binding = dict(binding)
    tampered_binding["generationRecordSha256"] = "sha256:" + "d" * 64
    tampered_binding.pop("bindingSha256")
    tampered_binding["bindingSha256"] = canonical_sha256(tampered_binding)
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="generation record binding drifted",
    ):
        supervisor._validate_native_generation_binding(
            generation_record={
                **record,
                "nativeGenerationFinalization": tampered_binding,
            },
            binary=binary,
        )

    binary.write_bytes(b"finalizer-v2")
    with pytest.raises(TemporalDiscoveryContractError, match="binary identity drifted"):
        supervisor._validate_native_generation_binding(
            generation_record=current,
            binary=binary,
        )


def test_python_boundary_adoption_requires_explicit_exact_durable_authority(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    binary_root = tmp_path / "bin"
    binary_root.mkdir()
    suffix = ".exe" if supervisor.os.name == "nt" else ""
    finalizer = binary_root / f"temporal-qd-generation-finalizer{suffix}"
    finalizer.write_bytes(b"finalizer-v1")
    (binary_root / f"temporal-qd-campaign-seal{suffix}").write_bytes(b"seal-v1")
    (binary_root / f"temporal-qd-tail-reducer{suffix}").write_bytes(b"reducer-v1")
    record = {
        "generationIndex": 1,
        "candidateCount": 2,
        "taskCount": 8,
        "archiveSha256": "sha256:" + "1" * 64,
        "resultSetSha256": "sha256:" + "2" * 64,
        "rotatingEvidenceLedgerSha256": "sha256:" + "3" * 64,
        "rotatingEvidenceCheckpointSha256": "sha256:" + "4" * 64,
        "cumulativeArchiveSha256": "sha256:" + "5" * 64,
        "generationFunnelArtifactSha256": "sha256:" + "6" * 64,
        "generationFunnelSnapshotSha256": "sha256:" + "7" * 64,
    }
    state = {
        "uniqueCandidatesEvaluated": 2,
        "workerTasksCompleted": 8,
        "completedGenerations": [record],
    }
    config = {
        "configSha256": "sha256:" + "a" * 64,
        "generationPlan": {"firstGenerationIndex": 1, "lastGenerationIndex": 5},
    }

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="explicitly authorize their one-time adoption",
    ):
        supervisor._prepare_native_finalization_adoption_authority(
            root=root,
            state=state,
            config=config,
            finalizer_binary=finalizer,
            requested_generations=(),
        )
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="exactly every unbound completed generation",
    ):
        supervisor._prepare_native_finalization_adoption_authority(
            root=root,
            state=state,
            config=config,
            finalizer_binary=finalizer,
            requested_generations=(2,),
        )

    authority = supervisor._prepare_native_finalization_adoption_authority(
        root=root,
        state=state,
        config=config,
        finalizer_binary=finalizer,
        requested_generations=(1,),
    )
    assert authority is not None
    assert authority["generationIndices"] == [1]
    assert authority["boundaries"] == [
        supervisor._python_boundary_adoption_descriptor(record)
    ]
    assert authority["authoritySha256"] == canonical_sha256(
        {key: value for key, value in authority.items() if key != "authoritySha256"}
    )
    assert (
        supervisor._prepare_native_finalization_adoption_authority(
            root=root,
            state=state,
            config=config,
            finalizer_binary=finalizer,
            requested_generations=(),
        )
        == authority
    )
    frozen = supervisor._freeze_native_finalization_runtime_authority(
        root=root,
        finalizer_binary=finalizer,
        state=state,
        authorized_adoption_generations=frozenset({1}),
    )
    assert frozen["authoritySha256"] == authority["runtimeAuthoritySha256"]

    drifted_state = {
        **state,
        "completedGenerations": [
            {**record, "archiveSha256": "sha256:" + "f" * 64}
        ],
    }
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="record or artifact identities drifted",
    ):
        supervisor._prepare_native_finalization_adoption_authority(
            root=root,
            state=drifted_state,
            config=config,
            finalizer_binary=finalizer,
            requested_generations=(),
        )


def test_native_admission_never_retrofits_an_unbound_record_without_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = {"generationIndex": 1}
    monkeypatch.setattr(
        supervisor,
        "_validate_completed_generation_ledger",
        lambda **_kwargs: {1: record},
    )
    audited = False

    def unexpected_audit(**_kwargs):
        nonlocal audited
        audited = True

    monkeypatch.setattr(supervisor, "_validate_generation_artifacts", unexpected_audit)
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="cannot silently adopt",
    ):
        supervisor._admit_completed_generations_native(
            root=tmp_path,
            state={"completedGenerations": [record]},
            state_path=tmp_path / "state.json",
            config={},
            binary=tmp_path / "finalizer.exe",
            deep_audit=False,
            tail_result_mode=supervisor.TAIL_RESULT_MODE_INDEXED,
            tail_result_indexes={},
        )
    assert audited is False


def test_native_invocation_rejects_binary_replacement_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "run"
    binary_root = tmp_path / "bin"
    binary_root.mkdir()
    suffix = ".exe" if supervisor.os.name == "nt" else ""
    finalizer = binary_root / f"temporal-qd-generation-finalizer{suffix}"
    finalizer.write_bytes(b"finalizer-v1")
    (binary_root / f"temporal-qd-campaign-seal{suffix}").write_bytes(b"seal-v1")
    (binary_root / f"temporal-qd-tail-reducer{suffix}").write_bytes(b"reducer-v1")
    authority = supervisor._freeze_native_finalization_runtime_authority(
        root=root,
        finalizer_binary=finalizer,
        state={"completedGenerations": []},
    )
    manifest_path = root / "generations" / "generation-0001" / "native-finalization" / "manifest.json"
    manifest = supervisor._native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": supervisor.NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": authority["authoritySha256"],
            "sourcePath": str((manifest_path.parent / "source.json").resolve()),
            "sourceSha256": "sha256:" + "a" * 64,
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    _write(manifest_path, manifest)

    def replace_during_run(*_args, **_kwargs):
        finalizer.write_bytes(b"finalizer-v2")
        return supervisor.subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps({"status": "committed"}),
            stderr="",
        )

    monkeypatch.setattr(supervisor.subprocess, "run", replace_during_run)
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="binary identity drifted during invocation",
    ):
        supervisor._invoke_native_finalizer(
            binary=finalizer,
            manifest_path=manifest_path,
        )


def test_native_authority_rotation_preserves_historical_binary_epoch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    suffix = ".exe" if supervisor.os.name == "nt" else ""

    def binaries(path: Path, version: str) -> Path:
        path.mkdir()
        finalizer = path / f"temporal-qd-generation-finalizer{suffix}"
        finalizer.write_bytes(f"finalizer-{version}".encode())
        (path / f"temporal-qd-campaign-seal{suffix}").write_bytes(
            f"seal-{version}".encode()
        )
        (path / f"temporal-qd-tail-reducer{suffix}").write_bytes(
            f"reducer-{version}".encode()
        )
        return finalizer

    first_binary = binaries(tmp_path / "epoch-1", "v1")
    second_binary = binaries(tmp_path / "epoch-2", "v2")
    first = supervisor._freeze_native_finalization_runtime_authority(
        root=root,
        finalizer_binary=first_binary,
        state={
            "stateSha256": "sha256:" + "1" * 64,
            "configSha256": "sha256:" + "c" * 64,
            "currentGenerationIndex": 1,
            "completedGenerations": [],
        },
    )
    superseded_file = (
        root
        / "generations"
        / "generation-0002"
        / "native-finalization"
        / "campaign-seal"
        / "partial.json"
    )
    superseded_file.parent.mkdir(parents=True)
    superseded_file.write_bytes(b"immutable-partial-attempt")
    superseded_bytes = superseded_file.read_bytes()
    rotation_state = {
        "stateSha256": "sha256:" + "2" * 64,
        "configSha256": "sha256:" + "c" * 64,
        "currentGenerationIndex": 2,
        "completedGenerations": [
            {"generationIndex": 1, "nativeGenerationFinalization": {}}
        ],
    }
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="binary identity drifted from frozen authority",
    ):
        supervisor._freeze_native_finalization_runtime_authority(
            root=root,
            finalizer_binary=second_binary,
            state=rotation_state,
        )

    second = supervisor._freeze_native_finalization_runtime_authority(
        root=root,
        finalizer_binary=second_binary,
        state=rotation_state,
        authorize_rotation=True,
    )
    assert (
        supervisor._freeze_native_finalization_runtime_authority(
            root=root,
            finalizer_binary=second_binary,
            state=rotation_state,
        )
        == second
    )
    assert first["authoritySha256"] != second["authoritySha256"]
    root_authority = json.loads(
        (root / "native-finalization-authority.json").read_text(encoding="utf-8")
    )
    assert root_authority == first
    assert (
        supervisor._native_finalization_authority_sha256(root, 1)
        == first["authoritySha256"]
    )
    assert (
        supervisor._native_finalization_authority_sha256(root, 2)
        == second["authoritySha256"]
    )
    assert superseded_file.read_bytes() == superseded_bytes
    assert supervisor._native_finalization_root(root, 1).name == "native-finalization"
    assert supervisor._native_finalization_root(root, 2) == (
        root
        / "generations"
        / "generation-0002"
        / "native-finalization"
        / "attempts"
        / second["authoritySha256"].removeprefix("sha256:")
    )
    rotation_files = list(
        (
            root
            / supervisor.NATIVE_FINALIZATION_AUTHORITY_HISTORY_DIR
            / "rotations"
        ).glob("*.json")
    )
    assert len(rotation_files) == 1
    rotation = json.loads(rotation_files[0].read_text(encoding="utf-8"))
    assert rotation["supersededIncompleteAttempt"]["fileCount"] == 1
    assert rotation["supersededIncompleteAttempt"]["totalBytes"] == len(
        superseded_bytes
    )
    assert (
        supervisor._pinned_native_authority_binary(
            root=root,
            authority_sha256=first["authoritySha256"],
            role="generationFinalizer",
        )
        == first_binary.resolve()
    )
    assert (
        supervisor._pinned_native_authority_binary(
            root=root,
            authority_sha256=second["authoritySha256"],
            role="generationFinalizer",
        )
        == second_binary.resolve()
    )

    manifest_path = (
        root
        / "generations"
        / "generation-0001"
        / "native-finalization"
        / "manifest.json"
    )
    manifest = supervisor._native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": supervisor.NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": first["authoritySha256"],
            "sourcePath": str((manifest_path.parent / "source.json").resolve()),
            "sourceSha256": "sha256:" + "a" * 64,
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    _write(manifest_path, manifest)
    supervisor._verify_pinned_native_invocation_binary(
        binary=first_binary,
        manifest_path=manifest_path,
        role="generationFinalizer",
    )
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="binary identity drifted during invocation",
    ):
        supervisor._verify_pinned_native_invocation_binary(
            binary=second_binary,
            manifest_path=manifest_path,
            role="generationFinalizer",
        )


def test_v5_completed_generation_seals_idempotent_lineage_unavailable_marker(
    tmp_path: Path,
) -> None:
    generation = tmp_path / "generations" / "generation-0002"
    for relative in (
        "proposal/generation-journal.json",
        "campaign/campaign.json",
        "archive.json",
        "evidence/generation-ledger.json",
    ):
        path = generation / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"sealed":true}\n', encoding="utf-8")
    config = {
        "configSha256": "sha256:" + "a" * 64,
        "evolvableModuleAuthority": {"authoritySha256": "sha256:" + "b" * 64},
    }
    first = supervisor._write_v5_lineage_unavailable_marker(
        root=tmp_path, generation_index=2, config=config
    )
    assert first is not None
    assert first["completedGenerationIndex"] == 2
    assert len(first["sourceArtifacts"]) == 4
    assert supervisor._write_v5_lineage_unavailable_marker(
        root=tmp_path, generation_index=2, config=config
    ) == first
    assert supervisor._write_v5_lineage_unavailable_marker(
        root=tmp_path,
        generation_index=9,
        config={"configSha256": "sha256:" + "c" * 64},
    ) is None
    (generation / "archive.json").unlink()
    with pytest.raises(TemporalDiscoveryContractError, match="source artifact is absent"):
        supervisor._write_v5_lineage_unavailable_marker(
            root=tmp_path, generation_index=2, config=config
        )


def test_v5_capacity_receipt_must_prove_frozen_campaign_supply() -> None:
    # This check consumes a receipt only after authority-open validation has
    # verified its sealed factory identity.  The supervisor additionally
    # prevents a smaller finite preview from standing in for the frozen G0 +
    # later-generation unique construction demand.
    receipt = {
        "compiledAdmittedCandidateCount": 8_096,
        "uniqueSemanticPairCount": 8_096,
    }
    supervisor._require_evolvable_capacity_receipt_supply(
        receipt, required_unique_candidates=8_096
    )
    for field in ("compiledAdmittedCandidateCount", "uniqueSemanticPairCount"):
        under_capacity = dict(receipt)
        under_capacity[field] = 8_095
        with pytest.raises(
            TemporalDiscoveryContractError,
            match="does not prove the frozen campaign candidate supply",
        ):
            supervisor._require_evolvable_capacity_receipt_supply(
                under_capacity, required_unique_candidates=8_096
            )
