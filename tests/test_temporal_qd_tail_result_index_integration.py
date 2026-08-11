"""Integration seams for the opt-in rotating-tail result index.

The detailed index contract tests own malformed source/index coverage.  These
tests exercise the production consumers that normally caused repeated raw
gzip reads after a completed rotating campaign.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from test_temporal_qd_tail_result_index import (
    _checkpoint_record,
    _evidence_plan,
    _fixture,
    _profile,
    _result_for_task,
)

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_funnel_adapter as funnel_adapter
import autoresearch.temporal_qd_supervisor as supervisor
import autoresearch.temporal_qd_tail_result_index as tail_index
from autoresearch.result_codec import (
    canonical_json_bytes,
    read_json_object,
    write_gzip_json_once,
)
from autoresearch.temporal_discovery_results import (
    load_provenance_bound_window_evidence,
    load_stage_results,
)
from autoresearch.temporal_qd_evolution import load_qd_evaluated_members
from autoresearch.temporal_qd_funnel_adapter import build_qd_generation_funnel
from autoresearch.temporal_qd_rotating_evidence import (
    ROTATING_EVIDENCE_INPUT_SCHEMA,
    build_rotating_evidence_contract,
    panel_for_generation,
)
from autoresearch.temporal_search import (
    TEMPORAL_SEARCH_CHECKPOINT_SCHEMA,
    TEMPORAL_SEARCH_MANIFEST_SCHEMA,
    build_authority,
    build_task_matrix,
    canonical_sha256,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _campaign_fixture(
    tmp_path: Path,
    *,
    candidate_count: int = 1,
    window_count: int = 2,
    trade_rows: int = 3,
) -> tuple[Path, Path, dict, dict, dict, dict[str, dict], dict]:
    campaign_root = tmp_path / "campaign"
    (
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        panel,
        _paths,
    ) = _fixture(
        campaign_root,
        candidate_count=candidate_count,
        window_count=window_count,
        trade_rows=trade_rows,
    )
    _write_json(campaign_root / "authority.json", authority)
    _write_json(result_root / "task-manifest.json", manifest)
    _write_json(result_root / "checkpoint.json", checkpoint)
    return (
        campaign_root,
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        panel,
    )


def _accepted_funnel_entry(
    *, ordinal: int, candidate_id: str, profile_sha: str
) -> dict:
    return {
        "entrySha256": canonical_sha256({"entry": candidate_id}),
        "proposalOrdinal": ordinal,
        "originKind": "random_immigrant",
        "disposition": "accepted",
        "proposal": {
            "candidateId": candidate_id,
            "rawSourceProfileSha256": profile_sha,
        },
        "funnelCandidate": {
            "schemaVersion": "temporal_qd_proposal_funnel_stage_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": profile_sha,
            "staticReachability": {"outcome": "reachable", "reasons": []},
            "nativeValidation": {
                "outcome": "valid",
                "reasons": [],
                "resolvedProfileSha256": profile_sha,
                "programSha256": canonical_sha256({"resolved": candidate_id}),
                "validationReportSha256": canonical_sha256(
                    {"validation": candidate_id}
                ),
            },
            "admission": {
                "outcome": "admitted",
                "reasons": [],
                "canonicalEvidenceIdentitySha256": canonical_sha256(
                    {"evidence": candidate_id}
                ),
            },
        },
    }


def _funnel_inputs(
    *,
    authority: dict,
    manifest: dict,
    checkpoint: dict,
    candidates: dict[str, dict],
) -> dict:
    entries = [
        _accepted_funnel_entry(
            ordinal=ordinal,
            candidate_id=candidate_id,
            profile_sha=candidate["profileSnapshotSha256"],
        )
        for ordinal, (candidate_id, candidate) in enumerate(sorted(candidates.items()))
    ]
    return {
        "proposal_entries": entries,
        "proposal_accounting": {
            "dispositionCounts": {"accepted": len(entries)},
            "originProposalCounts": {"random_immigrant": len(entries)},
        },
        "population": {
            "candidateCount": len(entries),
            "candidates": [
                {
                    "candidateId": candidate_id,
                    "sourceProfileSha256": candidate["profileSnapshotSha256"],
                }
                for candidate_id, candidate in sorted(candidates.items())
            ],
        },
        "authority": authority,
        "task_manifest": manifest,
        "checkpoint": checkpoint,
        "archive": {
            "cells": [],
            "resolvedExecutionDeduplication": {"duplicates": []},
        },
        "minimum_total_trades": 1,
        "minimum_trades_per_window": 1,
    }


def _rotating_transaction_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, dict, dict]:
    """Build one real authority-bound, one-candidate rotating G1 matrix."""

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
            "provisionalSurvivorCount": 1,
            "breederWidth": 1,
        }
    )
    panel = panel_for_generation(contract, 1)
    authority_windows = [
        {
            "windowId": window["windowId"],
            "analysisWindowStart": window["analysisWindowStart"],
            "analysisWindowEnd": window["analysisWindowEnd"],
        }
        for window in panel["windows"]
    ]
    root = tmp_path / "run"
    generation_root = root / "generations" / "generation-0001"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"
    profile = _profile()
    profile_sha = canonical_sha256(profile)
    candidate_id = "candidate_a"
    program_sha = canonical_sha256({"program": candidate_id})
    candidate = {
        "candidateId": candidate_id,
        "candidateIdentitySha256": canonical_sha256({"candidate": candidate_id}),
        "sourceMode": "random_immigrant",
        "seedId": "fixture-seed",
        "programSha256": program_sha,
        "sourceProfile": copy.deepcopy(profile),
        "sourceProfileSha256": profile_sha,
        "profileSnapshotSha256": profile_sha,
    }
    cell = profile["executionConfig"]["exitPolicy"]["selectedCell"]
    preparation = {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "indexed-rotating-transaction-fixture",
        "workerContract": {
            "workerContractSha256": "sha256:" + "d" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": candidate_id,
                "sourceProfile": copy.deepcopy(profile),
                "sourceProfileSha256": profile_sha,
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 100,
                "windowInputs": [
                    {
                        "windowId": window["windowId"],
                        "evidencePlan": _evidence_plan(
                            profile_sha=profile_sha,
                            start=window["analysisWindowStart"],
                            end=window["analysisWindowEnd"],
                            window_id=window["windowId"],
                            cell=cell,
                        ),
                    }
                    for window in panel["windows"]
                ],
            }
        ],
        "developmentWindows": authority_windows,
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2025-01-01T00:00:00Z",
                "analysisWindowEnd": "2025-02-01T00:00:00Z",
                "reason": "fixture",
            }
        ],
        "bounds": {
            "maxCandidates": 1,
            "maxDevelopmentWindows": len(panel["windows"]),
            "maxTasks": len(panel["windows"]),
            "maxAttempts": 1,
            "deadlineSeconds": 60.0,
        },
    }
    authority = build_authority(preparation)
    tasks = build_task_matrix(authority)
    manifest = {
        "schemaVersion": TEMPORAL_SEARCH_MANIFEST_SCHEMA,
        "authorityId": authority["authorityId"],
        "taskCount": len(tasks),
        "tasks": tasks,
        "taskMatrixSha256": canonical_sha256(tasks),
    }
    completed: dict[str, dict] = {}
    for task in tasks:
        material = _result_for_task(
            task,
            profile_sha=profile_sha,
            net=1.0,
            trade_rows=3,
        )
        material["program_sha256"] = program_sha
        for cost_view in material["cost_view_results"].values():
            cost_view["replay_result"]["programSha256"] = program_sha
        result_path = result_root / "results" / f"{task['task_id']}.json.gz"
        metadata = write_gzip_json_once(result_path, material)
        completed[task["task_id"]] = _checkpoint_record(
            result_path=result_path,
            metadata=metadata,
            candidate_id=candidate_id,
        )
    checkpoint = {
        "schemaVersion": TEMPORAL_SEARCH_CHECKPOINT_SCHEMA,
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "completed": completed,
        "journal": [
            {"taskId": task_id, **record}
            for task_id, record in sorted(completed.items())
        ],
    }
    population = {
        "schemaVersion": "temporal_qd_generation_population_v3",
        "generationIndex": 1,
        "candidateCount": 1,
        "candidates": [candidate],
    }
    population["populationSha256"] = canonical_sha256(population)
    campaign = {"campaignSha256": canonical_sha256({"campaign": "fixture"})}
    _write_json(proposal_root / "population.json", population)
    _write_json(campaign_root / "authority.json", authority)
    _write_json(campaign_root / "campaign.json", campaign)
    _write_json(result_root / "task-manifest.json", manifest)
    _write_json(result_root / "checkpoint.json", checkpoint)
    initial_archive = qd.canonical_empty_bidirectional_archive_template()
    initial_archive_path = root / "initial-archive.json"
    _write_json(initial_archive_path, initial_archive)
    config = {
        "rotatingEvidence": contract,
        "frozenSearchPolicy": {
            "minimumTotalTrades": 1,
            "minimumTradesPerWindow": 1,
            "capTrades": 20,
            "cellCapacity": 4,
        },
    }
    return (
        root,
        generation_root,
        proposal_root,
        campaign_root,
        initial_archive_path,
        {
            "config": config,
            "candidate": candidate,
        },
    )


def test_verified_campaign_index_is_reused_by_stage_provenance_and_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        campaign_root,
        result_root,
        _authority,
        manifest,
        checkpoint,
        candidates,
        panel,
    ) = _campaign_fixture(tmp_path, candidate_count=2, window_count=2)

    legacy_stage = load_stage_results(result_root)
    legacy_evidence = load_provenance_bound_window_evidence(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=candidates,
    )
    legacy_descriptor = supervisor._results_descriptor(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
    )

    calls = 0
    real_read = tail_index.read_json_object

    def counted_read(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_read(*args, **kwargs)

    monkeypatch.setattr(tail_index, "read_json_object", counted_read)
    cache: dict[Path, dict] = {}
    index = supervisor._verified_tail_result_index(
        campaign_root=campaign_root,
        indexes=cache,
    )
    assert calls == len(manifest["tasks"])
    # The exact retained mapping is returned without another source scan.
    assert (
        supervisor._verified_tail_result_index(
            campaign_root=campaign_root,
            indexes=cache,
        )
        is index
    )
    assert calls == len(manifest["tasks"])

    def no_raw_read(*_args, **_kwargs):
        raise AssertionError("indexed tail consumer reopened a raw result blob")

    monkeypatch.setattr(tail_index, "read_json_object", no_raw_read)
    monkeypatch.setattr(supervisor, "read_json_object", no_raw_read)
    indexed_stage = load_stage_results(result_root, tail_result_index=index)
    indexed_evidence = load_provenance_bound_window_evidence(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=candidates,
        tail_result_index=index,
    )
    indexed_descriptor = supervisor._results_descriptor(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        tail_result_index=index,
    )
    assert canonical_json_bytes(indexed_stage) == canonical_json_bytes(legacy_stage)
    assert canonical_json_bytes(indexed_evidence) == canonical_json_bytes(
        legacy_evidence
    )
    assert canonical_json_bytes(indexed_descriptor) == canonical_json_bytes(
        legacy_descriptor
    )


def test_indexed_provenance_refuses_extra_candidate_coverage(
    tmp_path: Path,
) -> None:
    (
        _campaign_root,
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        panel,
    ) = _campaign_fixture(tmp_path, candidate_count=1, window_count=2)
    index = tail_index.build_tail_result_index(
        result_root=result_root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
    )
    with pytest.raises(
        tail_index.TemporalQDTailResultIndexError,
        match="population coverage mismatch",
    ):
        tail_index.load_indexed_provenance_bound_window_evidence(
            index=index,
            panel=panel,
            candidates={
                **candidates,
                "candidate_extra": {
                    "candidateId": "candidate_extra",
                    "profileSnapshotSha256": canonical_sha256({"extra": True}),
                },
            },
        )


def test_indexed_and_raw_provenance_accept_admitted_candidate_subset(
    tmp_path: Path,
) -> None:
    (
        _campaign_root,
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        panel,
    ) = _campaign_fixture(tmp_path, candidate_count=2, window_count=2)
    index = tail_index.build_tail_result_index(
        result_root=result_root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
    )
    admitted_id = sorted(candidates)[0]
    admitted = {admitted_id: candidates[admitted_id]}

    raw_evidence = load_provenance_bound_window_evidence(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=admitted,
    )
    indexed_evidence = load_provenance_bound_window_evidence(
        result_root=result_root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=admitted,
        tail_result_index=index,
    )

    assert set(raw_evidence) == {admitted_id}
    assert canonical_json_bytes(indexed_evidence) == canonical_json_bytes(
        raw_evidence
    )


def test_indexed_funnel_matches_raw_without_a_second_result_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        _campaign_root,
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        _panel,
    ) = _campaign_fixture(tmp_path, candidate_count=2, window_count=2)
    inputs = _funnel_inputs(
        authority=authority,
        manifest=manifest,
        checkpoint=checkpoint,
        candidates=candidates,
    )
    legacy = build_qd_generation_funnel(**inputs)
    index = tail_index.build_tail_result_index(
        result_root=result_root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
        include_funnel_projection=True,
    )

    def no_raw_read(*_args, **_kwargs):
        raise AssertionError("indexed funnel reopened a raw result blob")

    monkeypatch.setattr(funnel_adapter, "read_json_object", no_raw_read)
    indexed = build_qd_generation_funnel(
        **inputs,
        tail_result_index=index,
    )
    assert canonical_json_bytes(indexed) == canonical_json_bytes(legacy)


def test_indexed_qd_member_loader_matches_legacy_without_raw_result_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign_root = tmp_path / "evaluated-campaign"
    (
        result_root,
        authority,
        manifest,
        checkpoint,
        candidates,
        _panel,
        paths,
    ) = _fixture(campaign_root, candidate_count=1, window_count=2, trade_rows=3)
    candidate_id, tail_candidate = next(iter(candidates.items()))
    profile = _profile()
    profile_sha = canonical_sha256(profile)
    program_sha = canonical_sha256({"program": candidate_id})
    for task in manifest["tasks"]:
        path = next(path for path in paths if path.name.startswith(task["task_id"]))
        material, _metadata = read_json_object(path)
        material["program_sha256"] = program_sha
        for cost_view in material["cost_view_results"].values():
            cost_view["replay_result"]["programSha256"] = program_sha
        path.unlink()
        metadata = write_gzip_json_once(path, material)
        checkpoint["completed"][task["task_id"]] = _checkpoint_record(
            result_path=path,
            metadata=metadata,
            candidate_id=candidate_id,
        )
    checkpoint["journal"] = [
        {"taskId": task_id, **record}
        for task_id, record in sorted(checkpoint["completed"].items())
    ]
    _write_json(campaign_root / "authority.json", authority)
    _write_json(result_root / "task-manifest.json", manifest)
    _write_json(result_root / "checkpoint.json", checkpoint)
    population = {
        "schemaVersion": "temporal_qd_generation_population_v3",
        "generationIndex": 1,
        "candidateCount": 1,
        "candidates": [
            {
                "candidateId": candidate_id,
                "candidateIdentitySha256": tail_candidate["candidateIdentitySha256"],
                "sourceMode": "random_immigrant",
                "seedId": "fixture-seed",
                "programSha256": program_sha,
                "sourceProfile": copy.deepcopy(profile),
                "sourceProfileSha256": profile_sha,
                "profileSnapshotSha256": profile_sha,
            }
        ],
    }
    population["populationSha256"] = canonical_sha256(population)
    population_path = campaign_root / "population.json"
    _write_json(population_path, population)
    legacy = load_qd_evaluated_members(
        population_path=population_path,
        result_root=result_root,
        generation_index=1,
    )
    index = tail_index.build_tail_result_index(
        result_root=result_root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
    )

    def no_raw_read(*_args, **_kwargs):
        raise AssertionError("indexed member loader reopened a raw result blob")

    monkeypatch.setattr(tail_index, "read_json_object", no_raw_read)
    indexed = load_qd_evaluated_members(
        population_path=population_path,
        result_root=result_root,
        generation_index=1,
        tail_result_index=index,
    )
    assert canonical_json_bytes(indexed) == canonical_json_bytes(legacy)


def test_rotating_transaction_legacy_to_indexed_resume_is_exact_and_worker_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (
        root,
        generation_root,
        proposal_root,
        campaign_root,
        initial_archive_path,
        fixture,
    ) = _rotating_transaction_fixture(tmp_path)
    candidate = fixture["candidate"]
    monkeypatch.setattr(
        supervisor, "_validate_frozen_sources", lambda _config, **_kwargs: []
    )
    monkeypatch.setattr(
        supervisor,
        "load_evaluation_population",
        lambda **_kwargs: {"candidates": [copy.deepcopy(candidate)]},
    )
    monkeypatch.setattr(
        supervisor,
        "hydrate_evaluation_candidate",
        lambda value, **_kwargs: copy.deepcopy(value),
    )
    worker_campaign_calls: list[object] = []
    monkeypatch.setattr(
        supervisor,
        "_run_rotating_cohort_campaign",
        lambda **_kwargs: worker_campaign_calls.append(_kwargs),
    )
    archive_path = generation_root / "archive.json"
    legacy = supervisor._complete_rotating_generation_transaction(
        root=root,
        generation_root=generation_root,
        generation_index=1,
        proposal_root=proposal_root,
        proposal_campaign_root=campaign_root,
        parent_archive_path=initial_archive_path,
        archive_path=archive_path,
        config=fixture["config"],
        client=object(),
    )
    transaction_paths = [
        generation_root / "evidence" / "cohort.json",
        generation_root / "evidence" / "provisional.json",
        generation_root / "evidence" / "cumulative-archive.json",
        generation_root / "evidence" / "checkpoint.json",
        generation_root / "evidence" / "generation-ledger.json",
        archive_path,
    ]
    legacy_bytes = {path: path.read_bytes() for path in transaction_paths}
    # Remove the completed transaction to exercise a real indexed rebuild;
    # once all final artifacts exist, the production restart path correctly
    # reuses them without rebuilding or retaining an in-memory tail index.
    for path in transaction_paths:
        path.unlink()
    retained: dict[Path, dict] = {}
    indexed = supervisor._complete_rotating_generation_transaction(
        root=root,
        generation_root=generation_root,
        generation_index=1,
        proposal_root=proposal_root,
        proposal_campaign_root=campaign_root,
        parent_archive_path=initial_archive_path,
        archive_path=archive_path,
        config=fixture["config"],
        client=object(),
        tail_result_mode="indexed",
        tail_result_indexes=retained,
    )
    assert indexed == legacy
    assert {path: path.read_bytes() for path in transaction_paths} == legacy_bytes
    assert len(retained) == 1
    # A split/restart at this completed transaction boundary reuses the exact
    # retained mapping and does not launch parent/backfill worker campaigns.
    restarted = supervisor._complete_rotating_generation_transaction(
        root=root,
        generation_root=generation_root,
        generation_index=1,
        proposal_root=proposal_root,
        proposal_campaign_root=campaign_root,
        parent_archive_path=initial_archive_path,
        archive_path=archive_path,
        config=fixture["config"],
        client=object(),
        tail_result_mode="indexed",
        tail_result_indexes=retained,
    )
    assert restarted == legacy
    assert {path: path.read_bytes() for path in transaction_paths} == legacy_bytes
    assert worker_campaign_calls == []


def test_post_save_validation_receives_the_live_indexed_tail_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the real supervisor's post-save validation call boundary."""

    (
        source_campaign_root,
        _source_result_root,
        _source_authority,
        source_manifest,
        _source_checkpoint,
        _source_candidates,
        _source_panel,
    ) = _campaign_fixture(tmp_path / "source", candidate_count=1, window_count=2)
    root = tmp_path / "run"
    initial_archive = tmp_path / "initial-archive.json"
    _write_json(initial_archive, {})
    template = tmp_path / "template.json"
    _write_json(template, {})
    worker_contract = "sha256:" + "a" * 64
    config = {
        "configSha256": canonical_sha256({"postSave": "fixture"}),
        "initialArchive": {
            "archiveSha256": canonical_sha256({"initialArchive": "fixture"})
        },
        "generationPlan": {
            "firstGenerationIndex": 1,
            "lastGenerationIndex": 1,
            "generationCount": 1,
            "targetUniqueEvaluations": 1,
        },
        "evaluation": {
            "gatewayUrl": "http://127.0.0.1:8799",
            "predeclaredEvidenceContext": {},
            "timeoutSecondsPerGeneration": 60.0,
            "enqueueBatchSize": 1,
        },
        "repositories": {"executionEngineCommit": "b" * 40},
        "qdVersion": supervisor.QD_VERSION,
        "policyName": supervisor.QD_POLICY_NAME,
        "workerContractSha256": worker_contract,
        "policySha256": supervisor.QD_POLICY_SHA256,
        "frozenPolicy": supervisor.QD_POLICY,
        "validator": {"timeoutSeconds": 1.0},
        "frozenSearchPolicy": {},
        "broadAdmission": False,
        "rotatingEvidence": {"fixture": True},
    }
    archive_sha = canonical_sha256({"archive": "fixture"})
    result_set_sha = canonical_sha256({"resultSet": "fixture"})
    population_sha = canonical_sha256({"population": "fixture"})
    journal = {}
    journal["journalSha256"] = canonical_sha256(journal)
    validation_calls: list[dict] = []
    published_validation_calls: list[dict] = []
    transaction_caches: list[dict] = []
    raw_reads = 0
    real_read = tail_index.read_json_object

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def close(self) -> None:
            pass

    def counted_read(*args, **kwargs):
        nonlocal raw_reads
        raw_reads += 1
        return real_read(*args, **kwargs)

    def fake_generate(**kwargs):
        proposal_root = Path(kwargs["output_root"])
        _write_json(proposal_root / "generation-journal.json", journal)
        _write_json(
            proposal_root / "population.json", {"populationSha256": population_sha}
        )
        return {
            "completed": True,
            "populationSha256": population_sha,
            "journalSha256": journal["journalSha256"],
            "proposalCount": 1,
            "candidateCount": 1,
            "originProposalCounts": {"fixture": 1},
            "originAcceptedCounts": {"fixture": 1},
            "proposalSlots": {},
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
            "nextImmigrantContinuationOrdinal": 1,
        }

    def fake_campaign(**kwargs):
        campaign_root = Path(kwargs["output_root"])
        result_root = campaign_root / "screening-run"
        identity = {
            "executionEngineCommit": config["repositories"]["executionEngineCommit"],
            "workerContract": {"workerContractSha256": worker_contract},
            "policySha256": supervisor.QD_POLICY_SHA256,
            "evaluationIdentitySha256": canonical_sha256({"identity": "fixture"}),
        }
        _write_json(campaign_root / "evaluation-identity.json", identity)
        _write_json(campaign_root / "authority.json", {"authority": "fixture"})
        _write_json(result_root / "checkpoint.json", {"completed": {}})
        return {
            "campaignSha256": canonical_sha256({"campaign": "fixture"}),
            "evaluationIdentitySha256": identity["evaluationIdentitySha256"],
            "taskMatrixSha256": canonical_sha256({"tasks": "fixture"}),
            "taskCount": 1,
        }

    def fake_transaction(**kwargs):
        cache = kwargs["tail_result_indexes"]
        transaction_caches.append(cache)
        supervisor._verified_tail_result_index(
            campaign_root=source_campaign_root,
            indexes=cache,
        )
        _write_json(
            Path(kwargs["archive_path"]),
            {"archiveSha256": archive_sha, "resultSetSha256": result_set_sha},
        )
        return {
            "archiveSha256": archive_sha,
            "occupiedCellCount": 1,
            "newCellCount": 1,
            "qualityMemberCount": 1,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
            "paretoAdmissionCount": 1,
            "paretoEvictionCount": 0,
            "additionalWorkerTaskCount": 0,
            "rotatingEvidenceLedgerSha256": canonical_sha256({"ledger": "fixture"}),
            "rotatingEvidenceCheckpointSha256": canonical_sha256(
                {"checkpoint": "fixture"}
            ),
            "cumulativeArchiveSha256": canonical_sha256({"cumulative": "fixture"}),
            "frontierMemberCount": 0,
        }

    def fake_artifacts(**kwargs):
        campaign = fake_campaign(
            output_root=root / "generations" / "generation-0001" / "campaign"
        )
        archive_path = root / "generations" / "generation-0001" / "archive.json"
        return {
            "population": {"populationSha256": population_sha},
            "journal": {"journalSha256": journal["journalSha256"]},
            "campaign": {"campaignSha256": campaign["campaignSha256"]},
            "evaluationIdentity": {
                "evaluationIdentitySha256": campaign["evaluationIdentitySha256"]
            },
            "archive": {"archiveSha256": archive_sha, "path": str(archive_path)},
        }

    def fake_validate_completed(**kwargs):
        validation_calls.append(
            {
                **kwargs,
                "tailResultIndexPathsAtCall": tuple(
                    kwargs.get("tail_result_indexes") or {}
                ),
            }
        )
        return {}

    def fake_validate_published(**kwargs):
        published_validation_calls.append(
            {
                **kwargs,
                "tailResultIndexPathsAtCall": tuple(
                    kwargs.get("tail_result_indexes") or {}
                ),
            }
        )

    monkeypatch.setattr(supervisor, "_frozen_config", lambda **_kwargs: (config, []))
    monkeypatch.setattr(
        supervisor, "_validate_frozen_sources", lambda _config, **_kwargs: []
    )
    monkeypatch.setattr(
        supervisor, "_validate_completed_generations", fake_validate_completed
    )
    monkeypatch.setattr(
        supervisor, "_validate_published_generation_boundary", fake_validate_published
    )
    monkeypatch.setattr(
        supervisor, "template_for_generation", lambda *_args: {"path": str(template)}
    )
    monkeypatch.setattr(supervisor, "validate_generation_template", lambda *_args: None)
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(supervisor, "generate_qd_generation", fake_generate)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", fake_campaign)
    monkeypatch.setattr(
        supervisor,
        "run_temporal_search_tasks",
        lambda *_args, **_kwargs: {"completedTaskCount": 1},
    )
    monkeypatch.setattr(
        supervisor, "_complete_rotating_generation_transaction", fake_transaction
    )
    monkeypatch.setattr(supervisor, "_capture_generation_artifacts", fake_artifacts)
    monkeypatch.setattr(tail_index, "read_json_object", counted_read)

    result = supervisor.run_qd_supervisor(
        run_root=root,
        initial_archive_path=initial_archive,
        source_preparation_path=tmp_path / "source.json",
        base_generator_root=tmp_path / "generator",
        confirmed_entry_admission_root=tmp_path / "admission",
        template_preparation_path=template,
        validator_command_file=tmp_path / "validator.json",
        parameters={},
        generation_count=1,
        autoresearch_commit="c" * 40,
        execution_engine_commit="b" * 40,
        worker_contract_sha256=worker_contract,
        gateway_url="http://127.0.0.1:8799",
        tail_result_mode="indexed",
        generation_finalization_engine=supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
    )
    assert result["status"] == "completed"
    assert raw_reads == len(source_manifest["tasks"])
    assert len(validation_calls) == 1
    assert len(published_validation_calls) == 1
    post_save = published_validation_calls[-1]
    assert post_save["tail_result_mode"] == "indexed"
    assert post_save["tail_result_indexes"] is transaction_caches[0]
    assert post_save["tailResultIndexPathsAtCall"]


def test_indexed_post_save_validation_avoids_historical_raw_reopens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run two real artifact validations and count every raw source reopen.

    Generation production is deliberately lightweight, but the indexed artifact
    capture and published-boundary validation are the production code.  This
    catches the former G2 path, where validating all history after state-save
    reread G1 and then evicted G2's active retained index.
    """

    root = tmp_path / "run"
    initial_archive = tmp_path / "initial-archive.json"
    _write_json(initial_archive, {})
    template = tmp_path / "template.json"
    template_payload = {"schemaVersion": "fixture-template-v1"}
    _write_json(template, template_payload)
    template_sha = canonical_sha256(template_payload)
    worker_contract = "sha256:" + "a" * 64
    rotating_evidence = {
        "panelTemplates": {"fixture-panel": {"preparationSha256": template_sha}}
    }
    config = {
        "configSha256": canonical_sha256({"postSave": "two-generation"}),
        "initialArchive": {
            "archiveSha256": canonical_sha256({"initialArchive": "fixture"})
        },
        "generationPlan": {
            "firstGenerationIndex": 1,
            "lastGenerationIndex": 2,
            "generationCount": 2,
            "targetUniqueEvaluations": 2,
        },
        "evaluation": {
            "gatewayUrl": "http://127.0.0.1:8799",
            "predeclaredEvidenceContext": {},
            "timeoutSecondsPerGeneration": 60.0,
            "enqueueBatchSize": 1,
        },
        "repositories": {"executionEngineCommit": "b" * 40},
        "qdVersion": supervisor.QD_VERSION,
        "policyName": supervisor.QD_POLICY_NAME,
        "workerContractSha256": worker_contract,
        "policySha256": supervisor.QD_POLICY_SHA256,
        "frozenPolicy": supervisor.QD_POLICY,
        "validator": {"timeoutSeconds": 1.0},
        "frozenSearchPolicy": {},
        "broadAdmission": False,
        "rotatingEvidence": rotating_evidence,
    }
    generated_generations: list[int] = []
    campaign_runs: dict[str, tuple[Path, int]] = {}
    generation_read_counts: list[int] = []
    raw_reads = 0
    real_read = tail_index.read_json_object

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def close(self) -> None:
            pass

    def counted_read(*args, **kwargs):
        nonlocal raw_reads
        raw_reads += 1
        return real_read(*args, **kwargs)

    def fake_generate(**kwargs):
        generation = int(kwargs["generation_index"])
        generated_generations.append(generation)
        proposal_root = Path(kwargs["output_root"])
        population = {
            "schemaVersion": "fixture_qd_population_v1",
            "generationIndex": generation,
            "candidateCount": 1,
        }
        population["populationSha256"] = canonical_sha256(population)
        journal = {
            "schemaVersion": "fixture_qd_journal_v1",
            "generationIndex": generation,
            "nextImmigrantContinuationOrdinal": generation,
        }
        journal["journalSha256"] = canonical_sha256(journal)
        _write_json(proposal_root / "population.json", population)
        _write_json(proposal_root / "generation-journal.json", journal)
        return {
            "completed": True,
            "populationSha256": population["populationSha256"],
            "journalSha256": journal["journalSha256"],
            "proposalCount": 1,
            "candidateCount": 1,
            "originProposalCounts": {"fixture": 1},
            "originAcceptedCounts": {"fixture": 1},
            "proposalSlots": {},
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
            "nextImmigrantContinuationOrdinal": generation,
        }

    def fake_campaign(**kwargs):
        campaign_root = Path(kwargs["output_root"])
        population = json.loads(
            Path(kwargs["population_path"]).read_text(encoding="utf-8")
        )
        generation = int(population["generationIndex"])
        (
            result_root,
            authority,
            manifest,
            checkpoint,
            _candidates,
            _panel,
            _paths,
        ) = _fixture(campaign_root, candidate_count=1, window_count=2)
        preparation = template_payload
        campaign = {
            "schemaVersion": "fixture_qd_campaign_v1",
            "generationIndex": generation,
            "populationSha256": population["populationSha256"],
            "preparationSha256": canonical_sha256(preparation),
            "authorityId": authority["authorityId"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
        }
        campaign["campaignSha256"] = canonical_sha256(campaign)
        identity = {
            "schemaVersion": "fixture_qd_evaluation_identity_v1",
            "populationSha256": population["populationSha256"],
            "templatePreparationSha256": template_sha,
            "executionEngineCommit": config["repositories"]["executionEngineCommit"],
            "workerContract": {"workerContractSha256": worker_contract},
            "policySha256": supervisor.QD_POLICY_SHA256,
            "rotatingEvidence": rotating_evidence,
        }
        identity["evaluationIdentitySha256"] = canonical_sha256(identity)
        _write_json(campaign_root / "preparation.json", preparation)
        _write_json(campaign_root / "authority.json", authority)
        _write_json(campaign_root / "evaluation-identity.json", identity)
        _write_json(campaign_root / "campaign.json", campaign)
        _write_json(result_root / "authority.json", authority)
        _write_json(result_root / "task-manifest.json", manifest)
        _write_json(result_root / "checkpoint.json", checkpoint)
        campaign_runs[authority["authorityId"]] = (result_root, len(manifest["tasks"]))
        return {
            "campaignSha256": campaign["campaignSha256"],
            "evaluationIdentitySha256": identity["evaluationIdentitySha256"],
            "taskMatrixSha256": manifest["taskMatrixSha256"],
            "taskCount": len(manifest["tasks"]),
        }

    def fake_search(_client, authority, **kwargs):
        result_root, task_count = campaign_runs[authority["authorityId"]]
        assert Path(kwargs["output_root"]).resolve() == result_root.resolve()
        _write_json(
            result_root / "summary.json",
            {
                "schemaVersion": "fixture_summary_v1",
                "authorityId": authority["authorityId"],
                "taskCount": task_count,
                "completedTaskCount": task_count,
            },
        )
        return {"completedTaskCount": task_count}

    def fake_transaction(**kwargs):
        generation = int(kwargs["generation_index"])
        archive = {
            "schemaVersion": "fixture_qd_archive_v1",
            "generationIndex": generation,
            "resultSetSha256": canonical_sha256({"results": generation}),
        }
        archive["archiveSha256"] = canonical_sha256(archive)
        evidence_root = Path(kwargs["generation_root"]) / "evidence"
        ledger = {
            "schemaVersion": "fixture_rotating_ledger_v1",
            "generationIndex": generation,
            "campaigns": [],
        }
        ledger["ledgerSha256"] = canonical_sha256(ledger)
        checkpoint = {
            "schemaVersion": "fixture_rotating_checkpoint_v1",
            "generationIndex": generation,
        }
        checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
        cumulative = {
            "schemaVersion": "fixture_cumulative_archive_v1",
            "generationIndex": generation,
        }
        cumulative["archiveSha256"] = canonical_sha256(cumulative)
        _write_json(Path(kwargs["archive_path"]), archive)
        _write_json(evidence_root / "generation-ledger.json", ledger)
        _write_json(evidence_root / "checkpoint.json", checkpoint)
        _write_json(evidence_root / "cumulative-archive.json", cumulative)
        return {
            "archiveSha256": archive["archiveSha256"],
            "occupiedCellCount": generation,
            "newCellCount": 1,
            "qualityMemberCount": 1,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
            "paretoAdmissionCount": 1,
            "paretoEvictionCount": 0,
            "additionalWorkerTaskCount": 0,
            "rotatingEvidenceLedgerSha256": ledger["ledgerSha256"],
            "rotatingEvidenceCheckpointSha256": checkpoint["checkpointSha256"],
            "cumulativeArchiveSha256": cumulative["archiveSha256"],
            "frontierMemberCount": 0,
        }

    def capture_event(event: str, **_kwargs) -> None:
        if event == "generation_completed":
            generation_read_counts.append(raw_reads)

    monkeypatch.setattr(
        supervisor, "_frozen_config", lambda *_args, **_kwargs: (config, [])
    )
    monkeypatch.setattr(
        supervisor, "_validate_frozen_sources", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(
        supervisor,
        "template_for_generation",
        lambda *_args, **_kwargs: {"path": str(template)},
    )
    monkeypatch.setattr(
        supervisor, "validate_generation_template", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        supervisor,
        "panel_for_generation",
        lambda *_args, **_kwargs: {"panelId": "fixture-panel"},
    )
    monkeypatch.setattr(supervisor, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(supervisor, "generate_qd_generation", fake_generate)
    monkeypatch.setattr(supervisor, "freeze_qd_screening_campaign", fake_campaign)
    monkeypatch.setattr(supervisor, "run_temporal_search_tasks", fake_search)
    monkeypatch.setattr(
        supervisor, "_complete_rotating_generation_transaction", fake_transaction
    )
    monkeypatch.setattr(supervisor, "_event", capture_event)
    monkeypatch.setattr(tail_index, "read_json_object", counted_read)

    run_kwargs = {
        "initial_archive_path": initial_archive,
        "source_preparation_path": tmp_path / "source.json",
        "base_generator_root": tmp_path / "generator",
        "confirmed_entry_admission_root": tmp_path / "admission",
        "template_preparation_path": template,
        "validator_command_file": tmp_path / "validator.json",
        "parameters": {},
        "generation_count": 2,
        "autoresearch_commit": "c" * 40,
        "execution_engine_commit": "b" * 40,
        "worker_contract_sha256": worker_contract,
        "gateway_url": "http://127.0.0.1:8799",
        "tail_result_mode": "indexed",
        "generation_finalization_engine": supervisor.GENERATION_FINALIZATION_ENGINE_PYTHON,
    }
    first = supervisor.run_qd_supervisor(run_root=root, **run_kwargs)
    assert first["status"] == "completed"
    assert generated_generations == [1, 2]
    # Each generation's two raw blobs are admitted once.  In particular G2's
    # post-save boundary adds no G1 or G2 rereads (the old path reached eight).
    assert generation_read_counts == [2, 4]
    assert raw_reads == 4

    state_path = root / "state.json"
    state_before_restart = state_path.read_bytes()
    index_bytes = {
        generation: tail_index.tail_result_index_path(
            root
            / "generations"
            / f"generation-{generation:04d}"
            / "campaign"
            / "screening-run"
        ).read_bytes()
        for generation in (1, 2)
    }
    raw_reads = 0
    generation_read_counts.clear()
    restarted = supervisor.run_qd_supervisor(run_root=root, **run_kwargs)
    assert restarted["status"] == "completed"
    assert generated_generations == [1, 2]
    # A completed restart still performs the deliberate, bounded, one-time
    # source verification of both historical generation matrices.
    assert raw_reads == 4
    assert generation_read_counts == []
    assert state_path.read_bytes() == state_before_restart
    assert {
        generation: tail_index.tail_result_index_path(
            root
            / "generations"
            / f"generation-{generation:04d}"
            / "campaign"
            / "screening-run"
        ).read_bytes()
        for generation in (1, 2)
    } == index_bytes
