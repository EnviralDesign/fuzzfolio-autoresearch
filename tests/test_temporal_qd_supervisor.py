from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_pair_generation as pair_generation
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
