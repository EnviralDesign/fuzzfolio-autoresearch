from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from autoresearch import ledger, play_hand_lab
from autoresearch import play_hand_lab_throughput as throughput


def _phase3_runtime() -> play_hand_lab.PlayHandLabRuntimeConfig:
    return play_hand_lab.PlayHandLabRuntimeConfig(
        campaign_id="phase3-throughput-test",
        formal_authority_kind="phase3",
        as_of_date="2026-01-14T00:00:00Z",
        target_runs=1,
        active_runs=1,
    )


def test_attempt_cache_survives_append_without_reparsing(tmp_path: Path) -> None:
    path = tmp_path / "attempts.jsonl"
    throughput._ORIGINAL_APPEND_ATTEMPT_ROW(path, {"attempt_id": "a-1", "value": 1})

    before = throughput.throughput_diagnostics()
    first = ledger.load_attempts(path)
    second = ledger.load_attempts(path)
    assert first == second == [{"attempt_id": "a-1", "value": 1}]

    ledger.append_attempt_row(path, {"attempt_id": "a-2", "value": 2})
    third = ledger.load_attempts(path)
    assert [row["attempt_id"] for row in third] == ["a-1", "a-2"]
    after = throughput.throughput_diagnostics()
    assert after["attempt_cache_misses"] >= before["attempt_cache_misses"] + 1
    assert after["attempt_cache_hits"] >= before["attempt_cache_hits"] + 2


def test_attempt_cache_evicts_historical_ledgers_by_lru_budget(
    tmp_path: Path, monkeypatch
) -> None:
    first_path = tmp_path / "first.jsonl"
    second_path = tmp_path / "second.jsonl"
    throughput._ORIGINAL_APPEND_ATTEMPT_ROW(first_path, {"attempt_id": "first"})
    throughput._ORIGINAL_APPEND_ATTEMPT_ROW(second_path, {"attempt_id": "second"})
    monkeypatch.setattr(throughput, "_ATTEMPT_CACHE_MAX_ENTRIES", 1)
    monkeypatch.setattr(throughput, "_ATTEMPT_CACHE_MAX_SOURCE_BYTES", 1_000_000)
    with throughput._LOCK:
        throughput._ATTEMPT_CACHE.clear()
        throughput._ATTEMPT_CACHE_SOURCE_BYTES = 0

    ledger.load_attempts(first_path)
    ledger.load_attempts(second_path)

    diagnostics = throughput.throughput_diagnostics()
    assert diagnostics["attempt_cache_entries"] == 1
    assert diagnostics["attempt_cache_source_bytes"] == second_path.stat().st_size
    assert diagnostics["attempt_cache_evictions"] >= 1


def test_pending_sweep_receipt_stores_artifact_reference_not_ranked_payload(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    ranked = [
        {
            "permutation_index": 0,
            "child_job_id": "sweep-000000",
            "status": "success",
            "parameters": {"period": 14},
            "fitness_value": 77.0,
            "score": 77.0,
        }
    ]
    sweep_payload = {
        "sweep_id": "sweep",
        "shard_id": "shard",
        "mode": "lab_sweep_shard",
        "permutation_indices": [0],
        "outcome": "scored",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": ranked[0],
        "failed_permutations": [],
    }
    play_hand_lab.atomic_write_json(artifact_dir / "sweep-results.json", sweep_payload)
    task_id = "phase3-throughput-test-lane-00000-task-00001-focused-shard-0000"
    recorded = {
        "task_id": task_id,
        "attempt_id": task_id,
        "artifact_dir": str(artifact_dir),
        "score": 77.0,
        "score_basis": "lab_sweep_shard",
        "status": "success",
        "phase": "focused",
        "task_kind": "sweep_shard",
        "sweep_payload": sweep_payload,
    }
    lab_result = {
        "task_id": task_id,
        "lease_id": "lease-1",
        "status": "success",
        "result": {"status": "success", "result": {"sweep_id": "sweep"}},
    }

    compact = throughput._compact_pending_sweep_receipt(
        play_hand_lab,
        recorded=recorded,
        lab_result=lab_result,
        runtime=_phase3_runtime(),
    )

    assert "sweep_payload" not in compact
    assert compact["sweep_payload_artifact"] == "sweep-results.json"
    artifact = json.loads((artifact_dir / "sweep-results.json").read_text(encoding="utf-8"))
    assert "ranked" not in artifact
    receipt = json.loads(
        (artifact_dir / "task-result-receipt.json").read_text(encoding="utf-8")
    )
    assert "sweep_payload" not in receipt["recorded_result"]
    restored = throughput._recorded_with_sweep_payload(play_hand_lab, compact)
    assert restored["sweep_payload"]["ranked_permutations"][0]["score"] == 77.0


def test_campaign_state_is_coalesced_but_allocation_shape_forces_checkpoint(
    tmp_path: Path,
) -> None:
    runtime = _phase3_runtime()
    lane = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-throughput-test-lane-00000",
        run_dir=tmp_path / "lane",
    )
    path = tmp_path / "play-hand-lab-state.json"
    kwargs = {
        "runtime": runtime,
        "campaign_id": "phase3-throughput-test",
        "lanes": [lane],
        "history": play_hand_lab.LabCampaignHistory(),
        "next_lane_index": 1,
        "recorded_result_count": 0,
        "reserved_lane_indices": [],
    }

    play_hand_lab._write_campaign_state(path, **kwargs)
    first_bytes = path.read_bytes()
    before = throughput.throughput_diagnostics()
    play_hand_lab._write_campaign_state(path, **kwargs)
    after = throughput.throughput_diagnostics()
    assert path.read_bytes() == first_bytes
    assert after["campaign_state_writes_skipped"] >= before[
        "campaign_state_writes_skipped"
    ] + 1

    kwargs["next_lane_index"] = 2
    kwargs["reserved_lane_indices"] = [1]
    play_hand_lab._write_campaign_state(path, **kwargs)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["next_lane_index"] == 2
    assert payload["reserved_lane_indices"] == [1]


def test_formal_phase3_skips_expensive_progress_plot(monkeypatch) -> None:
    called = False

    def original(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(throughput, "_ORIGINAL_RENDER_LANE_PROGRESS", original)
    lane_ctx = SimpleNamespace(run_id="phase3-throughput-test-lane-00001")
    play_hand_lab._render_lane_progress_artifacts(config=object(), lane_ctx=lane_ctx)
    assert called is False


def test_barrier_exposes_result_pressure_and_drain_counters() -> None:
    rendered = play_hand_lab._format_lab_barrier_snapshot(
        barrier_index=1,
        campaign_id="phase3-throughput-test",
        runtime=_phase3_runtime(),
        lanes=[],
        tasks=[],
        snapshot={
            "result_backlog": 12,
            "result_backlog_bytes": 123 * 1024 * 1024,
            "result_backpressure_active": True,
        },
        metric_baseline={},
        recorded_result_count=0,
    )
    assert "result drain backlog=12 bytes=123MB pressure=ON" in rendered
