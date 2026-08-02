from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)


def _write(path: Path, value: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
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
