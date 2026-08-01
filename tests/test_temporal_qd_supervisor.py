from __future__ import annotations

import json
from pathlib import Path

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
        "schemaVersion": "temporal_qd_archive_v2",
        "qdVersion": "temporal_qd_evolution_v2",
        "generationIndex": 0,
        "populationSha256": canonical_sha256({"population": 0}),
        "resultSetSha256": canonical_sha256({"results": 0}),
        "previousArchiveSha256": None,
        "cellCapacity": 8,
        "candidateCountSeen": 1,
        "occupiedCellCount": 1,
        "memberCount": 1,
        "paretoEligibleMemberCount": 1,
        "cells": [],
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    archive_path = tmp_path / "archive.json"
    _write(archive_path, archive)
    template_path = tmp_path / "template.json"
    _write(template_path, {"schemaVersion": "fixture_template_v1"})
    validator_path = tmp_path / "validator.json"
    _write(validator_path, ["fixture-validator"])
    parameters = {
        "version": "temporal_qd_evolution_v2",
        "seed": 7,
        "targetUniqueCandidates": 2,
        "immigrantProposalFraction": 0.2,
        "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
        "maxCumulativeStructuralDepth": 16,
        "maxProposalAttempts": 20,
        "minimumTotalTrades": 2,
        "minimumTradesPerWindow": 1,
        "cellCapacity": 8,
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
    }


def test_supervisor_restarts_exactly_at_completed_generation_boundaries(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[str, int, int]] = []

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
            "schemaVersion": "temporal_qd_generation_population_v2",
            "generationIndex": generation,
            "candidateCount": 2,
            "candidates": [],
        }
        population["populationSha256"] = canonical_sha256(population)
        journal = {
            "schemaVersion": "temporal_qd_generation_journal_v2",
            "generationIndex": generation,
            "nextImmigrantContinuationOrdinal": cursor + 1,
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
            "nextImmigrantContinuationOrdinal": cursor + 1,
        }

    def fake_campaign(**kwargs):
        population = json.loads(Path(kwargs["population_path"]).read_text())
        generation = int(population["generationIndex"])
        root = Path(kwargs["output_root"])
        identity = {
            "executionEngineCommit": kwargs["execution_engine_commit"],
            "workerContract": {
                "workerContractSha256": kwargs["worker_contract_sha256"]
            },
        }
        identity["evaluationIdentitySha256"] = canonical_sha256(identity)
        _write(root / "evaluation-identity.json", identity)
        _write(root / "authority.json", {"generationIndex": generation})
        _write(
            root / "screening-run" / "checkpoint.json",
            {"completed": {}},
        )
        return {
            "campaignSha256": canonical_sha256({"campaign": generation}),
            "evaluationIdentitySha256": identity["evaluationIdentitySha256"],
            "taskMatrixSha256": canonical_sha256({"tasks": generation}),
            "taskCount": 4,
        }

    def fake_search(client, authority, **kwargs):
        callback = kwargs["progress_callback"]
        # Deliberately report reverse completion order.  It is telemetry only.
        for completed, task_id in (
            (1, "task-b"),
            (2, "task-a"),
            (3, "task-d"),
            (4, "task-c"),
        ):
            callback(
                {"taskId": task_id, "completedTaskCount": completed, "taskCount": 4}
            )
        return {"completedTaskCount": 4}

    def fake_archive(**kwargs):
        generation = int(kwargs["generation_index"])
        output = Path(kwargs["output_path"])
        archive = {
            "schemaVersion": "temporal_qd_archive_v2",
            "generationIndex": generation,
            "resultSetSha256": canonical_sha256({"results": generation}),
        }
        archive["archiveSha256"] = canonical_sha256(archive)
        _write(output, archive)
        return {
            "archiveSha256": archive["archiveSha256"],
            "occupiedCellCount": generation + 1,
            "newCellCount": 1,
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
    resumed = supervisor.run_qd_supervisor(run_root=tmp_path / "restarted", **inputs)
    assert uninterrupted["status"] == resumed["status"] == "completed"
    full_state = json.loads((tmp_path / "uninterrupted" / "state.json").read_text())
    resumed_state = json.loads((tmp_path / "restarted" / "state.json").read_text())
    for full, restarted in zip(
        full_state["completedGenerations"],
        resumed_state["completedGenerations"],
        strict=True,
    ):
        ignored = {"completedAt", "archivePath"}
        assert {k: v for k, v in full.items() if k not in ignored} == {
            k: v for k, v in restarted.items() if k not in ignored
        }
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
