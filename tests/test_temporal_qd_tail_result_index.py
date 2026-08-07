from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_tail_result_index as tail_index
from autoresearch.result_codec import (
    canonical_json_bytes,
    semantic_sha256,
    write_gzip_json_once,
)
from autoresearch.temporal_discovery_results import (
    load_provenance_bound_window_evidence,
    load_stage_results,
)
from autoresearch.temporal_qd_tail_result_index import (
    TemporalQDTailResultIndexError,
    benchmark_tail_result_index_reuse,
    build_tail_result_index,
    load_indexed_funnel_projections,
    load_indexed_provenance_bound_window_evidence,
    load_indexed_stage_results,
    load_tail_result_index,
    tail_result_index_path,
)
from autoresearch.temporal_search import (
    TEMPORAL_SEARCH_CHECKPOINT_SCHEMA,
    TEMPORAL_SEARCH_MANIFEST_SCHEMA,
    _cost_view_path_sha256,
    _rejected_result_material,
    build_authority,
    build_task_matrix,
    canonical_sha256,
)


def _sha(value: object) -> str:
    return canonical_sha256(value)


def _profile() -> dict:
    return {
        "version": "v2",
        "instruments": ["EURUSD"],
        "directionMode": "both",
        "graph": {
            "kind": "temporal_graph_v1",
            "states": [{"id": "flat"}],
            "transitions": [],
        },
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                },
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
        "indicators": [],
    }


def _evidence_plan(
    *, profile_sha: str, start: str, end: str, window_id: str, cell: dict
) -> dict:
    value = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": profile_sha,
        "analysis_window_start": start,
        "analysis_window_end": end,
        "execution_cell_sha256": _sha(cell),
        "lake_window_binding": {
            "window_semantic_sha256": _sha({"window": window_id}),
            "request": {
                "data_start": start,
                "data_end": end,
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
    }
    value["plan_id"] = _sha(value)
    return value


def _authority_fixture(
    *, candidate_count: int = 1, window_count: int = 2
) -> tuple[dict, dict, dict[str, dict], dict]:
    if candidate_count < 1 or candidate_count > 12:
        raise ValueError("candidate_count must be between one and twelve")
    if window_count < 1 or window_count > 4:
        raise ValueError("window_count must be between one and four")

    profile = _profile()
    profile_sha = _sha(profile)
    cell = profile["executionConfig"]["exitPolicy"]["selectedCell"]
    all_windows = [
        ("window-a", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z"),
        ("window-b", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"),
        ("window-c", "2024-03-01T00:00:00Z", "2024-04-01T00:00:00Z"),
        ("window-d", "2024-04-01T00:00:00Z", "2024-05-01T00:00:00Z"),
    ]
    windows = all_windows[:window_count]
    candidates: dict[str, dict] = {}
    prepared_candidates: list[dict] = []
    for ordinal in range(candidate_count):
        candidate_id = "candidate_a" if ordinal == 0 else f"candidate_{ordinal:02d}"
        candidates[candidate_id] = {
            "candidateId": candidate_id,
            "candidateIdentitySha256": _sha({"candidate": candidate_id}),
            "programSha256": _sha({"authored-program": candidate_id}),
            "profileSnapshotSha256": profile_sha,
        }
        prepared_candidates.append(
            {
                "candidateId": candidate_id,
                "sourceProfile": profile,
                "sourceProfileSha256": profile_sha,
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 100,
                "windowInputs": [
                    {
                        "windowId": window_id,
                        "evidencePlan": _evidence_plan(
                            profile_sha=profile_sha,
                            start=start,
                            end=end,
                            window_id=window_id,
                            cell=cell,
                        ),
                    }
                    for window_id, start, end in windows
                ],
            }
        )
    preparation = {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "tail-index-fixture",
        "workerContract": {
            "workerContractSha256": "sha256:" + "d" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": prepared_candidates,
        "developmentWindows": [
            {
                "windowId": window_id,
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
            }
            for window_id, start, end in windows
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2024-06-01T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "fixture",
            }
        ],
        "bounds": {
            "maxCandidates": candidate_count,
            "maxDevelopmentWindows": window_count,
            "maxTasks": candidate_count * window_count,
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
        "taskMatrixSha256": _sha(tasks),
    }
    panel = {
        "panelId": "panel-1",
        "windows": [
            {
                "windowId": window_id,
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
            }
            for window_id, start, end in windows
        ],
    }
    return authority, manifest, candidates, panel


def _metrics(
    *, net: float, trades: int, projection_points: int = 2, counter_keys: int = 0
) -> dict:
    points = max(2, projection_points)
    curve = [round(net * point / (points - 1), 6) for point in range(points)]
    action_counts = {
        f"action_{ordinal:03d}": ordinal + 1 for ordinal in range(counter_keys)
    }
    close_reason_counts = {
        f"reason_{ordinal:03d}": ordinal + 1 for ordinal in range(counter_keys)
    }
    state_occupancy = {
        f"state_{ordinal:03d}": (ordinal + 1) / 10 for ordinal in range(counter_keys)
    }
    transition_counts = {
        f"transition_{ordinal:03d}": ordinal + 1 for ordinal in range(counter_keys)
    }
    return {
        "observationsProcessed": 10,
        "tradesClosed": trades,
        "wins": trades,
        "losses": 0,
        "flatTrades": 0,
        "unresolvedPosition": False,
        "unresolvedPendingEffect": False,
        "totalGrossR": net + 0.1,
        "totalNetR": net,
        "totalExecutionCostPercent": 0.0,
        "maxDrawdownR": max(0.0, -net),
        "averageHoldingBars": 4.0,
        "exposureRatio": 0.2,
        "transitionEntropy": 0.3,
        "winRate": 1.0 if trades else 0.0,
        "profitFactor": 2.0,
        "equityCurveR": curve,
        "actionCounts": action_counts,
        "closeReasonCounts": close_reason_counts,
        "stateOccupancy": state_occupancy,
        "transitionCounts": transition_counts,
        "terminalValuation": {
            "schemaVersion": "temporal_terminal_valuation_v1",
            "policy": "leave_open_mark_to_market_v1",
            "positionStatus": "no_open_position",
            "lastCompletedBarId": "fixture-last",
            "lastCompletedBarStart": None,
            "lastCompletedBarClose": None,
            "markPrice": 1.0,
            "exitCostPercent": 0.0,
            "pendingEffectStatus": "none",
            "pendingEffectCancellationTreatment": "not_applicable",
            "closedTradeCountDelta": 0,
        },
        "terminalAdjustedTotalGrossR": net + 0.1,
        "terminalAdjustedTotalNetR": net,
        "terminalAdjustedTotalExecutionCostPercent": 0.0,
        "terminalAdjustedEquityCurveR": list(curve),
        "terminalAdjustedMaxDrawdownR": max(0.0, -net),
    }


def _result_for_task(
    task: dict,
    *,
    profile_sha: str,
    net: float,
    synthetic_audit_rows: int = 0,
    projection_points: int = 2,
    trade_rows: int = 0,
    counter_keys: int = 0,
) -> dict:
    payload = task["payload"]
    start = payload["analysis_window_start"]
    end = payload["analysis_window_end"]
    metrics = _metrics(
        net=net,
        trades=max(3, trade_rows),
        projection_points=projection_points,
        counter_keys=counter_keys,
    )
    for terminal_key in ("lastCompletedBarStart", "lastCompletedBarClose"):
        metrics["terminalValuation"][terminal_key] = start
    evidence = {
        "schema_version": "temporal_graph_candidate_window_evidence_contract_v1",
        "analysis_window_start": start,
        "analysis_window_end": end,
        "analysis_window_end_exclusive": True,
        "requested_bar_limit": 100,
        "effective_bar_limit": 100,
        "observation_count": 10,
        "first_admitted_observation_timestamp": start,
        "last_admitted_observation_timestamp": start,
        "warmup_sufficient": True,
        "warmup_sufficiency": {"sufficient": True, "source": "fixture"},
        "excluded_provisional_count": 0,
        "excluded_outside_analysis_window_count": 0,
    }
    stream = payload["shared_observation_stream_id"]
    resolved_program = _sha({"resolved-program": task["task_id"]})
    trades = [
        {
            "direction": "long",
            "entryBarId": f"entry-{ordinal}",
            "exitBarId": f"exit-{ordinal}",
            "entryPhase": "open",
            "exitPhase": "close",
            "entryTime": start,
            "exitTime": start,
            "entryClockIndex": ordinal,
            "exitClockIndex": ordinal + 1,
            "entryPrice": 1.0,
            "exitPrice": 1.0 + ordinal / 10_000,
            "closeReason": "fixture_close",
            "maxFavorableExcursionR": round(0.25 + ordinal / 100, 6),
            "maxAdverseExcursionR": round(-0.15 - ordinal / 200, 6),
            "holdingBars": 1 + ordinal % 19,
            "holdingHours": float(1 + ordinal % 19),
        }
        for ordinal in range(trade_rows)
    ]
    common_replay = {
        "streamSha256": stream,
        "profileSnapshotSha256": profile_sha,
        "programSha256": resolved_program,
        "graphTraces": [],
        "executionTraces": [],
        "trades": trades,
    }
    conservative = {**common_replay, "metrics": metrics}
    no_cost_metrics = copy.deepcopy(metrics)
    no_cost_metrics["totalNetR"] = net + 0.1
    no_cost_metrics["terminalAdjustedTotalNetR"] = net + 0.1
    no_cost_metrics["equityCurveR"] = [
        round(value + 0.1 * point / (len(metrics["equityCurveR"]) - 1), 6)
        for point, value in enumerate(metrics["equityCurveR"])
    ]
    no_cost_metrics["terminalAdjustedEquityCurveR"] = list(
        no_cost_metrics["equityCurveR"]
    )
    no_cost = {**common_replay, "metrics": no_cost_metrics}
    result = {
        "schema_version": "temporal_graph_candidate_window_result_v1",
        "task_kind": task["task_kind"],
        "job_id": payload["job_id"],
        "authority_id": payload["authority_id"],
        "candidate_id": payload["candidate_id"],
        "evidence_plan_id": payload["evidence_plan"]["plan_id"],
        "lake_window_semantic_sha256": payload["lake_window_semantic_sha256"],
        "shared_observation_stream_id": stream,
        "analysis_window_start": start,
        "analysis_window_end": end,
        "source_profile_snapshot_sha256": profile_sha,
        "resolved_profile_snapshot_sha256": profile_sha,
        "program_sha256": resolved_program,
        "observation_stream_sha256": stream,
        "observation_summary": {
            "observation_count": 10,
            "first_bar_start": start,
            "last_bar_start": start,
        },
        "evidence_contract": evidence,
        "worker_attribution": {
            "worker_contract_hash": payload["required_worker_contract_hash"],
        },
        "cost_view_results": {
            "research_conservative": {
                "cost_view": "research_conservative",
                "observation_stream_sha256": stream,
                "replay_result": conservative,
            },
            "none": {
                "cost_view": "none",
                "observation_stream_sha256": stream,
                "replay_result": no_cost,
            },
        },
        "diagnostics": {
            "observation_count": 10,
            "requested_bar_limit": 100,
            "effective_bar_limit": 100,
            "warmup_sufficient": True,
            "warmup_sufficiency": evidence["warmup_sufficiency"],
            "first_admitted_observation_timestamp": start,
            "last_admitted_observation_timestamp": start,
            "excluded_provisional_count": 0,
            "excluded_outside_analysis_window_count": 0,
            "cost_view_decision_path_sha256": _cost_view_path_sha256(
                common_replay, name="fixture replay"
            ),
            "cost_view_path_parity": "matched",
            "cost_view_count": 2,
            "shared_stream_required": True,
        },
    }
    if synthetic_audit_rows:
        # High-entropy deterministic content makes the compressed fixture large
        # enough to exercise bounded decode and index-reuse accounting.
        result["syntheticAuditPayload"] = [
            hashlib.sha256(f"{task['task_id']}:{ordinal}".encode()).hexdigest()
            for ordinal in range(synthetic_audit_rows)
        ]
    return result


def _checkpoint_record(*, result_path: Path, metadata: dict, candidate_id: str) -> dict:
    return {
        "resultSha256": metadata["semanticSha256"],
        "resultPath": str(result_path.resolve()),
        "candidateId": candidate_id,
        "resultCodec": metadata["codec"],
        "resultSemanticSha256": metadata["semanticSha256"],
        "resultSemanticSizeBytes": metadata["semanticSizeBytes"],
        "resultUncompressedSha256": metadata["uncompressedSha256"],
        "resultUncompressedSizeBytes": metadata["uncompressedSizeBytes"],
        "resultBlobSha256": metadata["blobSha256"],
        "resultBlobSizeBytes": metadata["blobSizeBytes"],
    }


def _fixture(
    tmp_path: Path,
    *,
    candidate_count: int = 1,
    window_count: int = 2,
    synthetic_audit_rows: int = 0,
    projection_points: int = 2,
    trade_rows: int = 0,
    counter_keys: int = 0,
) -> tuple[Path, dict, dict, dict, dict[str, dict], dict, list[Path]]:
    root = tmp_path / "screening-run"
    authority, manifest, candidates, panel = _authority_fixture(
        candidate_count=candidate_count,
        window_count=window_count,
    )
    profile_sha = next(iter(candidates.values()))["profileSnapshotSha256"]
    completed: dict[str, dict] = {}
    paths: list[Path] = []
    for ordinal, task in enumerate(manifest["tasks"]):
        path = root / "results" / f"{task['task_id']}.json.gz"
        metadata = write_gzip_json_once(
            path,
            _result_for_task(
                task,
                profile_sha=profile_sha,
                net=1.0 + ordinal,
                synthetic_audit_rows=synthetic_audit_rows,
                projection_points=projection_points,
                trade_rows=trade_rows,
                counter_keys=counter_keys,
            ),
        )
        completed[task["task_id"]] = _checkpoint_record(
            result_path=path,
            metadata=metadata,
            candidate_id=task["payload"]["candidate_id"],
        )
        paths.append(path)
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
    return (
        root,
        authority,
        manifest,
        checkpoint,
        candidates,
        panel,
        paths,
    )


def _build(
    root: Path,
    authority: dict,
    manifest: dict,
    checkpoint: dict,
    *,
    funnel: bool = True,
) -> dict:
    return build_tail_result_index(
        result_root=root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
        include_funnel_projection=funnel,
    )


def test_index_projects_deterministic_warmup_rejection_without_replay_metrics(tmp_path: Path) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, paths = _fixture(tmp_path)
    task = manifest["tasks"][0]
    failure = {
        "status": "failed",
        "task_id": task["task_id"],
        "lane_id": task["lane_id"],
        "attempt_id": task["attempt_id"],
        "lease_id": "fixture-warmup-lease",
        "error": {"type": "AlignedScoringWarmupInsufficientError", "attempt": 8},
    }
    rejected = _rejected_result_material(task, failure)
    paths[0].unlink()
    metadata = write_gzip_json_once(paths[0], rejected)
    checkpoint["completed"][task["task_id"]] = {
        **_checkpoint_record(
            result_path=paths[0], metadata=metadata, candidate_id=task["payload"]["candidate_id"]
        ),
        "outcome": "rejected",
        "rejectionCode": "aligned_scoring_warmup_insufficient",
    }
    checkpoint["journal"] = [
        {"taskId": task_id, **record}
        for task_id, record in sorted(checkpoint["completed"].items())
    ]

    index = _build(root, authority, manifest, checkpoint, funnel=True)
    indexed = load_indexed_stage_results(index)
    record = indexed[task["payload"]["candidate_id"]][0]
    assert record["evaluationRejected"] is True
    assert record["rejection"]["reason_code"] == "aligned_scoring_warmup_insufficient"
    rejected_entry = next(entry for entry in index["entries"] if "rejection" in entry)
    assert "rotatingEvidenceMetrics" not in rejected_entry
    assert "funnelProjection" not in rejected_entry
    assert rejected_entry["task"]["taskId"] not in load_indexed_funnel_projections(index)


def test_index_is_byte_equivalent_to_legacy_stage_and_provenance_projections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, authority, manifest, checkpoint, candidates, panel, paths = _fixture(tmp_path)
    raw_before = {path: path.read_bytes() for path in paths}

    index = _build(root, authority, manifest, checkpoint)

    assert index["schemaVersion"] == "temporal_qd_tail_result_index_v3"
    assert index["taskCount"] == len(manifest["tasks"])
    assert index["sourceResultBlobBytes"] == sum(
        len(value) for value in raw_before.values()
    )
    assert "windowRecord" not in index["entries"][0]
    assert index["entries"][0]["stageProjection"]["codec"] == ("gzip-canonical-json-v1")
    assert tail_result_index_path(root).is_file()
    assert {path: path.read_bytes() for path in paths} == raw_before

    legacy_stage = load_stage_results(root)
    indexed_stage = load_indexed_stage_results(index)
    assert canonical_json_bytes(indexed_stage) == canonical_json_bytes(legacy_stage)

    legacy_evidence = load_provenance_bound_window_evidence(
        result_root=root,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=candidates,
    )
    indexed_evidence = load_indexed_provenance_bound_window_evidence(
        index=index,
        panel=panel,
        candidates=candidates,
    )
    assert canonical_json_bytes(indexed_evidence) == canonical_json_bytes(
        legacy_evidence
    )
    assert len(load_indexed_funnel_projections(index)) == len(manifest["tasks"])

    # Repeated adapter reads are pure index reads; they never reopen raw gzip.
    def no_raw_read(*_args, **_kwargs):
        raise AssertionError("indexed adapter reopened a raw result")

    monkeypatch.setattr(tail_index, "read_json_object", no_raw_read)
    same_transaction_reload = load_tail_result_index(
        result_root=root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
        verify_source_blobs=False,
    )
    assert same_transaction_reload == index
    assert load_indexed_stage_results(index) == indexed_stage
    assert (
        load_indexed_provenance_bound_window_evidence(
            index=index, panel=panel, candidates=candidates
        )
        == indexed_evidence
    )


def test_index_reopens_complete_legacy_inputs_without_rewriting_raw_blobs(
    tmp_path: Path,
) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, paths = _fixture(
        tmp_path
    )
    raw_before = {path: path.read_bytes() for path in paths}
    first = _build(root, authority, manifest, checkpoint)
    index_before = tail_result_index_path(root).read_bytes()

    second = _build(root, authority, manifest, checkpoint)
    reopened = load_tail_result_index(
        result_root=root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
    )

    assert second == first == reopened
    assert tail_result_index_path(root).read_bytes() == index_before
    assert {path: path.read_bytes() for path in paths} == raw_before


def test_index_fails_closed_on_raw_corruption_and_checkpoint_path_escape(
    tmp_path: Path,
) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, paths = _fixture(
        tmp_path
    )
    _build(root, authority, manifest, checkpoint)
    damaged = paths[0].read_bytes()[:-1]
    paths[0].write_bytes(damaged)

    with pytest.raises(
        TemporalQDTailResultIndexError, match="corrupt|truncated|non-canonical"
    ):
        load_tail_result_index(
            result_root=root,
            authority=authority,
            task_manifest=manifest,
            checkpoint=checkpoint,
        )

    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path / "escape"
    )
    escaped = copy.deepcopy(checkpoint)
    task_id = next(iter(escaped["completed"]))
    outside = tmp_path / "outside.json.gz"
    outside.write_bytes(b"not a result")
    escaped["completed"][task_id]["resultPath"] = str(outside.resolve())
    escaped["journal"] = [
        {"taskId": item, **record}
        for item, record in sorted(escaped["completed"].items())
    ]
    with pytest.raises(TemporalQDTailResultIndexError, match="escaped|outside"):
        _build(root, authority, manifest, escaped)


def test_index_rejects_extra_or_duplicate_task_material_and_divergent_partial_index(
    tmp_path: Path,
) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path
    )
    extra = root / "results" / "extra.json.gz"
    write_gzip_json_once(extra, {"extra": True})
    with pytest.raises(TemporalQDTailResultIndexError, match="exactly match"):
        _build(root, authority, manifest, checkpoint)

    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path / "duplicate"
    )
    duplicate = copy.deepcopy(manifest)
    duplicate["tasks"].append(copy.deepcopy(duplicate["tasks"][0]))
    duplicate["taskCount"] += 1
    duplicate["taskMatrixSha256"] = _sha(duplicate["tasks"])
    with pytest.raises(TemporalQDTailResultIndexError, match="task manifest"):
        _build(root, authority, duplicate, checkpoint)

    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path / "partial"
    )
    index_path = tail_result_index_path(root)
    index_path.write_bytes(b"{}")
    with pytest.raises(TemporalQDTailResultIndexError, match="schema|partial|invalid"):
        _build(root, authority, manifest, checkpoint)
    assert index_path.read_bytes() == b"{}"


def test_index_rejects_stale_checkpoint_and_mismatched_completed_identity(
    tmp_path: Path,
) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path
    )
    stale = copy.deepcopy(checkpoint)
    stale["authorityId"] = _sha({"wrong": "authority"})
    with pytest.raises(TemporalQDTailResultIndexError, match="authority|binding"):
        _build(root, authority, manifest, stale)

    mismatched = copy.deepcopy(checkpoint)
    task_id = next(iter(mismatched["completed"]))
    mismatched["completed"][task_id]["candidateId"] = "candidate_other"
    mismatched["journal"] = [
        {"taskId": item, **record}
        for item, record in sorted(mismatched["completed"].items())
    ]
    with pytest.raises(TemporalQDTailResultIndexError, match="candidate identity"):
        _build(root, authority, manifest, mismatched)

    missing = copy.deepcopy(checkpoint)
    missing_task_id = next(iter(missing["completed"]))
    missing["completed"].pop(missing_task_id)
    missing["journal"] = [
        {"taskId": item, **record}
        for item, record in sorted(missing["completed"].items())
    ]
    with pytest.raises(
        TemporalQDTailResultIndexError, match="exact completed task matrix"
    ):
        _build(root, authority, manifest, missing)


def test_index_rejects_a_symlinked_result_path_when_supported(tmp_path: Path) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, paths = _fixture(
        tmp_path
    )
    original = paths[0].read_bytes()
    outside = tmp_path / "outside-result.json.gz"
    outside.write_bytes(original)
    paths[0].unlink()
    try:
        paths[0].symlink_to(outside)
    except (NotImplementedError, OSError):
        pytest.skip("file symlinks are unavailable in this test environment")

    with pytest.raises(TemporalQDTailResultIndexError, match="symlink|junction"):
        _build(root, authority, manifest, checkpoint)


def test_divergent_complete_index_is_never_replaced(tmp_path: Path) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path
    )
    _build(root, authority, manifest, checkpoint)
    index_path = tail_result_index_path(root)
    divergent = json.loads(index_path.read_text(encoding="utf-8"))
    divergent["sourceResultBlobBytes"] += 1
    body = dict(divergent)
    body.pop("tailResultIndexSha256")
    divergent["tailResultIndexSha256"] = semantic_sha256(body)
    divergent_bytes = canonical_json_bytes(divergent)
    index_path.write_bytes(divergent_bytes)

    with pytest.raises(TemporalQDTailResultIndexError, match="source blob byte count"):
        _build(root, authority, manifest, checkpoint)
    assert index_path.read_bytes() == divergent_bytes


def test_rehashed_corrupt_compact_stage_projection_fails_closed(tmp_path: Path) -> None:
    root, authority, manifest, checkpoint, _candidates, _panel, _paths = _fixture(
        tmp_path
    )
    index = _build(root, authority, manifest, checkpoint)
    corrupt = copy.deepcopy(index)
    entry = corrupt["entries"][0]
    entry["stageProjection"]["blobBase64"] = "AAAA"
    entry_body = dict(entry)
    entry_body.pop("entrySha256")
    entry["entrySha256"] = semantic_sha256(entry_body)
    index_body = dict(corrupt)
    index_body.pop("tailResultIndexSha256")
    corrupt["tailResultIndexSha256"] = semantic_sha256(index_body)

    with pytest.raises(TemporalQDTailResultIndexError, match="corrupt|gzip"):
        load_indexed_stage_results(corrupt)

    tail_result_index_path(root).write_bytes(canonical_json_bytes(corrupt))
    with pytest.raises(TemporalQDTailResultIndexError, match="projection drifted"):
        load_tail_result_index(
            result_root=root,
            authority=authority,
            task_manifest=manifest,
            checkpoint=checkpoint,
        )


def test_bounded_large_fixture_reuse_benchmark_reports_provisional_evidence(
    tmp_path: Path,
) -> None:
    """Exercise a materially larger synthetic corpus without a live economic run."""

    root, authority, manifest, checkpoint, candidates, panel, _paths = _fixture(
        tmp_path,
        candidate_count=6,
        window_count=4,
        projection_points=128,
        trade_rows=64,
        counter_keys=32,
    )

    evidence = benchmark_tail_result_index_reuse(
        result_root=root,
        authority=authority,
        task_manifest=manifest,
        checkpoint=checkpoint,
        panel=panel,
        candidates=candidates,
    )

    source_blob_bytes = evidence["input"]["sourceRawBlobBytes"]
    assert evidence["schemaVersion"] == (
        "temporal_qd_tail_result_index_reuse_benchmark_v3"
    )
    assert evidence["timingsAreProvisional"] is True
    assert evidence["input"]["taskCount"] == 24
    assert source_blob_bytes > 100_000
    assert evidence["input"]["indexBytes"] > 0
    assert evidence["protocol"]["legacyStagePasses"] == 1
    assert evidence["protocol"]["legacyProvenancePasses"] == 1
    assert evidence["legacy"]["rawBlobBytesLogical"] == source_blob_bytes * 2
    assert evidence["indexed"]["preparationMode"] == "build"
    assert evidence["indexed"]["wholePhase"]["rawBlobBytesLogical"] == source_blob_bytes
    assert evidence["indexed"]["verifiedNoRawLoad"]["rawBlobBytesLogical"] == 0
    assert evidence["indexed"]["steadyReuse"]["rawBlobBytesLogical"] == 0
    assert evidence["indexed"]["wholePhaseRawBlobBytesLogical"] == source_blob_bytes
    assert "same-process reuse" in evidence["protocol"]["verifySourceBlobsFalseRule"]
    assert evidence["memoryComparison"]["wholePhaseTracedPeakNonRegressing"] is True
    for measurement in (
        evidence["legacy"],
        evidence["indexed"]["wholePhase"],
        evidence["indexed"]["verifiedNoRawLoad"],
        evidence["indexed"]["steadyReuse"],
    ):
        assert measurement["wallSeconds"] >= 0
        assert measurement["tracemallocPeakBytes"] >= 0
        assert measurement["recursiveRetainedBytes"] >= 0
