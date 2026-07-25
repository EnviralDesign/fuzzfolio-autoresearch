from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_memory_deep as deep_memory
from autoresearch.durable_execution import DurableExecutionJournal


def _journal(tmp_path: Path, *, execution_id: str = "deep-memory-test") -> DurableExecutionJournal:
    return DurableExecutionJournal(
        tmp_path / "play-hand-lab-execution-journal.json",
        execution_id=execution_id,
        lineage={"campaign_id": execution_id},
    )


def _lane(tmp_path: Path, *, index: int = 1) -> play_hand_lab.LabLaneState:
    run_dir = tmp_path / f"lane-{index:03d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    lane = play_hand_lab.LabLaneState(
        lane_id=f"lane_{index:03d}",
        lane_index=index,
        run_id=f"deep-memory-lane-{index:05d}",
        run_dir=run_dir,
    )
    profile = {
        "timeframe": "M5",
        "indicators": [
            {
                "meta": {
                    "id": "MA",
                    "instanceId": "indicator-1",
                }
            }
        ],
    }
    lane.profile_payload = profile
    lane.incumbent_profile_payload = profile
    lane.instruments = ["EURUSD"]
    lane.incumbent_instruments = ["EURUSD"]
    lane.timeframe = "M5"
    lane.incumbent_timeframe = "M5"
    return lane


def _sweep_fixture(
    tmp_path: Path,
    lane: play_hand_lab.LabLaneState,
) -> tuple[str, str, str, dict[str, Any], dict[str, Any], dict[str, Any]]:
    phase = "coarse"
    sweep_id = f"{lane.run_id}-{phase}-sweep"
    shard_id = f"{sweep_id}-shard-0000"
    task_id = f"{lane.run_id}-task-00001-{phase}-shard-0000"
    params_by_index = {
        0: {"indicator-1.timeperiod": 5},
        1: {"indicator-1.timeperiod": 10},
    }
    params_sha256 = play_hand_lab.canonical_sha256(
        play_hand_lab._canonical_params(params_by_index)
    )
    policy_assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
        "candidate_fallback_decisions": [
            {"attempt": index, "large": "p" * 200}
            for index in range(8)
        ],
    }
    full_spec = {
        "phase": phase,
        "task_kind": "sweep_shard",
        "sweep_id": sweep_id,
        "shard_id": shard_id,
        "profile_path": str((tmp_path / "profile.json").resolve()),
        "profile_ref": "profile:test",
        "instruments": ["EURUSD"],
        "timeframe": "M5",
        "lookback_months": 3,
        "analysis_window_start": "2025-01-01T00:00:00Z",
        "analysis_window_end": "2025-04-01T00:00:00Z",
        "evidence_plan": {"large": "e" * 2_000},
        "axes": ["indicator[0].talib.timeperiod=5,10"],
        "axis_key_map": {
            "indicator-1.timeperiod": "indicator[0].talib.timeperiod"
        },
        "axis_plan": {"max_permutations": 2},
        "expanded_permutation_count": 2,
        "permutation_start": 0,
        "permutation_count": 2,
        "params_by_index": params_by_index,
        "params_by_index_sha256": params_sha256,
        "result_detail": "summary",
        "policy_assignment": policy_assignment,
    }
    ranked = [
        {
            "permutation_index": index,
            "child_job_id": f"{sweep_id}-{index:06d}",
            "status": "success",
            "parameters": dict(params),
            "fitness": {"score_lab": 80.0 - index},
            "fitness_value": 80.0 - index,
            "score_lab": 80.0 - index,
            "score": 80.0 - index,
        }
        for index, params in params_by_index.items()
    ]
    sweep_payload = {
        "sweep_id": sweep_id,
        "shard_id": shard_id,
        "mode": "lab_sweep_shard",
        "permutation_indices": [0, 1],
        "outcome": "scored",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": ranked[0],
        "failed_permutations": [],
        "parameter_importance": [],
    }
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "sweep-results.json").write_text(
        json.dumps(sweep_payload),
        encoding="utf-8",
    )
    recorded = {
        "task_id": task_id,
        "attempt_id": f"{lane.run_id}-attempt-00001",
        "artifact_dir": str(artifact_dir.resolve()),
        "score": 80.0,
        "score_basis": "lab_sweep_shard",
        "status": "success",
        "phase": phase,
        "task_kind": "sweep_shard",
        "profile_path": str((tmp_path / "profile.json").resolve()),
        "profile_ref": "profile:test",
        "instruments": ["EURUSD"],
        "timeframe": "M5",
        "lookback_months": 3,
        "analysis_window_start": "2025-01-01T00:00:00Z",
        "analysis_window_end": "2025-04-01T00:00:00Z",
        "policy_assignment": policy_assignment,
        "sweep_payload": sweep_payload,
    }
    task_envelope = {
        "task_id": task_id,
        "lane_id": lane.lane_id,
        "attempt_id": task_id,
        "task_kind": "sweep_shard",
        "payload": {
            "schema_version": "sweep-shard-job-v1",
            "shard_id": shard_id,
            "sweep_id": sweep_id,
            "definition": {
                "base_profile_id": "profile:test",
                "axes": [
                    {
                        "target": "talib_param",
                        "indicator_instance_id": "indicator-1",
                        "param_key": "timeperiod",
                        "values": [5, 10],
                    }
                ],
                "instruments": ["EURUSD"],
                "lookback_months": 3,
                "analysis_window_start": "2025-01-01T00:00:00Z",
                "analysis_window_end": "2025-04-01T00:00:00Z",
            },
            "evidence_plan": {"large": "e" * 2_000},
            "base_profile_snapshot": lane.incumbent_profile_payload,
            "permutation_start": 0,
            "permutation_count": 2,
            "permutation_indices": [0, 1],
            "params_by_index": params_by_index,
            "result_detail": "summary",
            "policy_assignment": policy_assignment,
        },
    }
    return task_id, phase, sweep_id, full_spec, recorded, task_envelope


def test_playhand_journal_load_streams_and_keeps_terminal_payloads_lazy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = _journal(tmp_path)
    done_payload = {
        "task_id": "done-task",
        "lane_id": "lane-001",
        "payload": {"large": "x" * 10_000},
    }
    pending_payload = {
        "task_id": "pending-task",
        "lane_id": "lane-002",
        "payload": {"large": "y" * 10_000},
    }
    receipt = {
        "recorded_result": {
            "task_id": "done-task",
            "status": "success",
            "large": "z" * 10_000,
        }
    }
    writer.apply_batch(
        registrations=[
            ("done-task", done_payload),
            ("pending-task", pending_payload),
        ],
        completions=[("done-task", receipt)],
    )

    journal_path = writer.path
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        if path.resolve(strict=False) == journal_path.resolve(strict=False):
            raise AssertionError("PlayHand v2 journal load must not use read_text")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    reader = _journal(tmp_path)
    loaded = reader.load()

    cached_terminal = loaded["tasks"]["done-task"]
    assert cached_terminal["status"] == "terminal"
    assert "payload" not in cached_terminal
    assert loaded["tasks"]["pending-task"]["payload"] == pending_payload
    assert getattr(reader, "_play_hand_task_record_index")["done-task"][
        "status"
    ] == "terminal"

    def fail_full_scan(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("indexed terminal restore unexpectedly used a full scan")

    monkeypatch.setattr(
        deep_memory,
        "_ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK",
        fail_full_scan,
    )
    restored = reader.terminal("done-task")
    assert restored is not None
    assert restored["payload"] == done_payload
    assert restored["terminal_receipt"]["payload"] == receipt
    assert "payload" not in cached_terminal


def test_lane_state_projection_does_not_deepcopy_omitted_heavy_fields(
    tmp_path: Path,
) -> None:
    lane = _lane(tmp_path)
    lane.completed_task_ids.add("done")
    lane.failed_task_ids.add("failed")
    expected = deep_memory._ORIGINAL_LANE_STATE_PAYLOAD(lane)
    observed = play_hand_lab._lane_state_payload(lane)
    assert observed == expected

    class DeepCopyBomb(dict):
        def __deepcopy__(self, memo: dict[int, Any]) -> Any:
            raise AssertionError("omitted heavy field was traversed")

    lane.profile_payload = DeepCopyBomb({"large": "x"})
    lane.incumbent_profile_payload = DeepCopyBomb({"large": "y"})
    lane.task_specs = {"task": DeepCopyBomb({"large": "z"})}

    observed = play_hand_lab._lane_state_payload(lane)
    assert observed["profile_payload"] is None
    assert observed["incumbent_profile_payload"] is None
    assert observed["task_specs"] == {}


def test_campaign_state_load_uses_bytes_and_compacts_legacy_sweep_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lane = _lane(tmp_path)
    task_id, phase, _sweep_id, _spec, recorded, _task = _sweep_fixture(
        tmp_path, lane
    )
    lane.task_ids = [task_id]
    lane.completed_task_ids = {task_id}
    lane.phase_task_ids = {phase: [task_id, "still-live"]}
    lane.phase_results = {phase: [recorded]}
    lane.current_phase = phase

    runtime = play_hand_lab.PlayHandLabRuntimeConfig(
        campaign_id="deep-state-test",
    )
    state_path = tmp_path / "play-hand-lab-state.json"
    play_hand_lab._write_campaign_state(
        state_path,
        runtime=runtime,
        campaign_id="deep-state-test",
        lanes=[lane],
        history=play_hand_lab.LabCampaignHistory(),
        next_lane_index=2,
        recorded_result_count=1,
    )
    assert b"ranked_permutations" in state_path.read_bytes()

    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        if path.resolve(strict=False) == state_path.resolve(strict=False):
            raise AssertionError("PlayHand state load must not create a text copy")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    lanes, _history, _next, _reserved, _count = (
        play_hand_lab._load_campaign_state(
            state_path,
            runtime=runtime,
            campaign_id="deep-state-test",
        )
    )

    compact = lanes[0].phase_results[phase][0]
    assert compact["sweep_payload"] is None
    assert compact["sweep_payload_deferred"] is True
    assert "candidate_fallback_decisions" not in json.dumps(compact)


def test_durable_completion_compacts_active_sweep_spec_and_result(
    tmp_path: Path,
) -> None:
    lane = _lane(tmp_path)
    task_id, phase, _sweep_id, spec, recorded, task = _sweep_fixture(
        tmp_path, lane
    )
    expected_task = json.loads(play_hand_lab.canonical_json(task))
    play_hand_lab._register_task_spec(
        lane,
        task_id=task_id,
        phase=phase,
        task_kind="sweep_shard",
        spec=spec,
    )
    lane.phase_results[phase] = [recorded]

    journal = _journal(tmp_path)
    receipt = {"recorded_result": recorded}
    expected_receipt = json.loads(play_hand_lab.canonical_json(receipt))
    journal.apply_batch(registrations=[(task_id, task)])
    journal.apply_batch(completions=[(task_id, receipt)])

    compact_spec = lane.task_specs[task_id]
    assert compact_spec["task_kind"] == "sweep_shard"
    assert compact_spec["params_by_index"] == spec["params_by_index"]
    assert "policy_assignment" not in compact_spec
    assert "evidence_plan" not in compact_spec

    compact_result = lane.phase_results[phase][0]
    assert compact_result["sweep_payload"] is None
    assert compact_result["sweep_payload_deferred"] is True
    assert "candidate_fallback_decisions" not in json.dumps(compact_result)

    terminal = journal.terminal(task_id)
    assert terminal is not None
    assert terminal["payload"] == expected_task
    assert terminal["terminal_receipt"]["payload"] == expected_receipt


def test_compacted_sweep_state_merges_to_the_same_canonical_payload(
    tmp_path: Path,
) -> None:
    lane = _lane(tmp_path)
    task_id, phase, sweep_id, spec, recorded, _task = _sweep_fixture(
        tmp_path, lane
    )
    shard_id = str(spec["shard_id"])

    expected = deep_memory._ORIGINAL_MERGE_SWEEP_PARENT_RECEIPTS(
        lane=lane,
        phase=phase,
        sweep_id=sweep_id,
        task_specs={shard_id: (task_id, spec)},
        records_by_task_id={task_id: recorded},
    )

    compact_spec = deep_memory._compact_completed_sweep_spec(spec)
    compact_recorded = deep_memory._compact_recorded_result(recorded)
    actual = play_hand_lab._merge_sweep_parent_receipts(
        lane=lane,
        phase=phase,
        sweep_id=sweep_id,
        task_specs={shard_id: (task_id, compact_spec)},
        records_by_task_id={task_id: compact_recorded},
    )

    assert play_hand_lab.canonical_sha256(actual) == (
        play_hand_lab.canonical_sha256(expected)
    )


def test_recorded_result_samples_do_not_retain_sweep_or_fallback_graphs(
    tmp_path: Path,
) -> None:
    lane = _lane(tmp_path)
    _task_id, _phase, _sweep_id, _spec, recorded, _task = _sweep_fixture(
        tmp_path, lane
    )
    original_size = len(json.dumps(recorded))
    samples: list[dict[str, Any]] = []

    play_hand_lab._add_recorded_result_sample(samples, recorded)

    assert len(samples) == 1
    assert samples[0]["sweep_payload"] is None
    assert samples[0]["sweep_payload_deferred"] is True
    assert "candidate_fallback_decisions" not in json.dumps(samples[0])
    assert len(json.dumps(samples[0])) < original_size // 2
    assert isinstance(recorded["sweep_payload"], dict)
