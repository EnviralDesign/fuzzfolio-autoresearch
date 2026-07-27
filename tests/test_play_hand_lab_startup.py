from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_memory_deep
from autoresearch import play_hand_lab_startup
from autoresearch.durable_execution import DurableExecutionError, DurableExecutionJournal


def _journal(path: Path) -> DurableExecutionJournal:
    return DurableExecutionJournal(
        path,
        execution_id="phase3-startup-test",
        lineage={"campaign_id": "phase3-startup-test"},
    )


def test_frontier_loader_defers_terminal_hashing_but_validates_pending_tasks(
    tmp_path: Path,
) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    writer = _journal(path)
    terminal_id = "phase3-startup-test-lane-00000-task-00001"
    pending_id = "phase3-startup-test-lane-00001-task-00001"
    writer.apply_batch(
        registrations=[
            (
                terminal_id,
                {
                    "task_id": terminal_id,
                    "lane_id": "lane_000",
                    "payload": {"large": "x" * 20_000},
                },
            ),
            (
                pending_id,
                {
                    "task_id": pending_id,
                    "lane_id": "lane_001",
                    "payload": {"large": "y" * 20_000},
                },
            ),
        ],
        completions=[
            (
                terminal_id,
                {
                    "recorded_result": {
                        "task_id": terminal_id,
                        "status": "success",
                        "large": "z" * 20_000,
                    }
                },
            )
        ],
    )

    reader = _journal(path)
    view = reader.load()

    terminal = view["tasks"][terminal_id]
    pending = view["tasks"][pending_id]
    assert terminal["status"] == "terminal"
    assert "payload" not in terminal
    assert terminal["terminal_receipt"].keys() == {"receipt_sha256"}
    assert pending["status"] == "pending"
    assert pending["payload"]["task_id"] == pending_id
    diagnostics = play_hand_lab_startup.startup_diagnostics()
    assert diagnostics["terminal_record_hashes_deferred"] >= 1
    assert diagnostics["pending_task_records_validated"] >= 1

    restored = play_hand_lab_memory_deep._full_terminal_task_from_offsets(
        reader,
        terminal_id,
    )
    assert restored is not None
    assert restored["payload"]["task_id"] == terminal_id
    assert restored["terminal_receipt"]["payload"]["recorded_result"]["task_id"] == terminal_id


def test_frontier_loader_rejects_corruption_in_an_executable_pending_task(
    tmp_path: Path,
) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    writer = _journal(path)
    task_id = "phase3-startup-test-lane-00000-task-00001"
    writer.apply_batch(
        registrations=[
            (
                task_id,
                {
                    "task_id": task_id,
                    "lane_id": "lane_000",
                    "payload": {"value": 1},
                },
            )
        ]
    )
    raw = path.read_bytes()
    assert b'"value":1' in raw
    path.write_bytes(raw.replace(b'"value":1', b'"value":2', 1))

    with pytest.raises(
        DurableExecutionError,
        match="record identity mismatch|pending task payload conflicts",
    ):
        _journal(path).load()


def _profile(path: Path, instrument: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "profile": {
                    "indicators": [],
                    "instruments": [instrument],
                }
            }
        ),
        encoding="utf-8",
    )


def test_state_load_hydrates_only_active_profiles_and_compacts_legacy_sweeps(
    tmp_path: Path,
) -> None:
    runtime = play_hand_lab.PlayHandLabRuntimeConfig(
        campaign_id="phase3-startup-state",
        target_runs=2,
    )
    active_profile = tmp_path / "active-profile.json"
    terminal_profile = tmp_path / "terminal-profile.json"
    _profile(active_profile, "EURUSD")
    _profile(terminal_profile, "GBPUSD")

    active = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-startup-state-lane-00000",
        run_dir=tmp_path / "lane-00000",
        profile_path=active_profile,
        task_ids=["task-active"],
        current_phase="focused",
    )
    terminal = play_hand_lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="phase3-startup-state-lane-00001",
        run_dir=tmp_path / "lane-00001",
        profile_path=terminal_profile,
        task_ids=["task-terminal"],
        completed_task_ids={"task-terminal"},
        current_phase="tombstoned",
        terminal=True,
    )
    active_payload = play_hand_lab._lane_state_payload(active)
    active_payload["profile_payload"] = {"legacy": "x" * 1_000}
    active_payload["task_specs"] = {"task-active": {"large": "x" * 1_000}}
    active_payload["phase_results"] = {
        "focused": [
            {
                "task_id": "task-done",
                "task_kind": "sweep_shard",
                "artifact_dir": str(tmp_path / "artifact"),
                "sweep_payload": {"ranked": [{"score": 1.0}]},
            }
        ]
    }
    terminal_payload = play_hand_lab._lane_state_payload(terminal)
    terminal_payload["profile_payload"] = {"legacy": "y" * 1_000}

    state_path = tmp_path / "play-hand-lab-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "play-hand-lab-durable-state-v1",
                "lineage": play_hand_lab._campaign_state_lineage(
                    runtime,
                    "phase3-startup-state",
                ),
                "next_lane_index": 2,
                "reserved_lane_indices": [],
                "recorded_result_count": 2,
                "history": {},
                "campaign_policy_state": None,
                "lanes": [active_payload, terminal_payload],
            }
        ),
        encoding="utf-8",
    )

    lanes, _history, _next, _reserved, _recorded = (
        play_hand_lab_startup._load_campaign_state_fast(
            play_hand_lab,
            state_path,
            runtime=runtime,
            campaign_id="phase3-startup-state",
        )
    )

    assert lanes[0].profile_payload["instruments"] == ["EURUSD"]
    assert lanes[0].task_specs == {}
    compact_result = lanes[0].phase_results["focused"][0]
    assert "sweep_payload" not in compact_result
    assert compact_result["sweep_payload_artifact"] == "sweep-results.json"
    assert lanes[1].profile_payload is None
    assert lanes[1].phase_results == {}
    diagnostics = play_hand_lab_startup.startup_diagnostics()
    assert diagnostics["active_profiles_hydrated"] == 1
    assert diagnostics["terminal_profiles_skipped"] == 1
    assert play_hand_lab_startup._LOADED_STATE_SHAPES[
        play_hand_lab_startup._path_key(state_path)
    ] is None


def test_task_spec_hydration_skips_terminal_lanes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observed: list[Any] = []
    monkeypatch.setattr(
        play_hand_lab_startup,
        "_ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS",
        lambda lanes, _tasks: observed.extend(lanes),
    )
    active = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-startup-lane-00000",
        run_dir=tmp_path / "active",
    )
    terminal = play_hand_lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="phase3-startup-lane-00001",
        run_dir=tmp_path / "terminal",
        terminal=True,
    )

    play_hand_lab._hydrate_unresolved_lane_task_specs(
        [active, terminal],
        {},
    )

    assert observed == [active]


def _policy_fixture(
    tmp_path: Path,
) -> tuple[dict[str, Any], play_hand_lab.LabLaneState, dict[str, Any]]:
    execution = {
        "allocation_algorithm": "hamilton",
        "allocation_algorithm_version": "v1",
        "lane_tie_break_order": ["guided"],
        "candidate_tie_break_order": ["candidate_id"],
    }
    cap_decision = {"outcome": "accepted", "charges": {}}
    assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
        "policy_outcome_type": "policy_lane_selected",
        "allocation": {
            "lane_index": 0,
            "planned_lane_count": 1,
            "algorithm": "hamilton",
            "algorithm_version": "v1",
            "lane_tie_break_order": ["guided"],
            "candidate_tie_break_order": ["candidate_id"],
        },
        "candidate_attributes": {"candidate_id": "candidate-1"},
        "cap_decision": cap_decision,
        "negative_prior_runtime": {},
    }
    policy_state = {
        "policy_manifest_sha256": assignment["policy_manifest_sha256"],
        "execution": execution,
        "negative_prior_runtime": {},
        "lane_plan": ["guided"],
        "planned_lane_counts": {"guided": 1},
        "assigned_lane_counts": {"guided": 1},
        "used_lane_counts": {"guided": 1},
        "exhausted_lane_counts": {"guided": 0},
        "accounting": {
            dimension: {}
            for dimension in (
                "family",
                "recipe",
                "instrument",
                "timeframe",
                "indicator",
            )
        },
        "exhaustion_outcomes": {},
    }
    lane = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-policy-lane-00000",
        run_dir=tmp_path / "lane",
        task_ids=["task-terminal", "task-pending"],
        completed_task_ids={"task-terminal"},
        policy_assignment=assignment,
    )
    return policy_state, lane, cap_decision


def test_policy_resume_uses_lane_accounting_and_only_checks_live_task_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy_state, lane, cap_decision = _policy_fixture(tmp_path)
    monkeypatch.setattr(
        play_hand_lab,
        "_policy_lane_for_index",
        lambda _state, _index: "guided",
    )
    monkeypatch.setattr(
        play_hand_lab,
        "_policy_cap_decision",
        lambda _state, _attributes: cap_decision,
    )
    durable_tasks = {
        "task-terminal": {
            "task_id": "task-terminal",
            "payload_sha256": "sha256:" + "b" * 64,
            "status": "terminal",
            "terminal_receipt": {"receipt_sha256": "sha256:" + "c" * 64},
        },
        "task-pending": {
            "task_id": "task-pending",
            "payload_sha256": "sha256:" + "d" * 64,
            "status": "pending",
            "payload": {
                "task_id": "task-pending",
                "lane_id": "lane_000",
                "policy_assignment": lane.policy_assignment,
            },
        },
    }
    unresolved = [dict(durable_tasks["task-pending"]["payload"])]

    rebuilt = play_hand_lab_startup._recompute_policy_state_without_terminal_io(
        play_hand_lab,
        policy_state,
        lanes=[lane],
        unresolved_tasks=unresolved,
        durable_tasks_by_id=durable_tasks,
        pruned_lane_count=0,
    )

    assert rebuilt["assigned_lane_counts"] == {"guided": 1}
    assert rebuilt["used_lane_counts"] == {"guided": 1}
    diagnostics = play_hand_lab_startup.startup_diagnostics()
    assert diagnostics["policy_task_payload_reads_avoided"] == 1
    assert diagnostics["policy_metadata_reads_avoided"] == 1


def test_policy_resume_still_rejects_a_live_task_assignment_conflict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy_state, lane, cap_decision = _policy_fixture(tmp_path)
    monkeypatch.setattr(
        play_hand_lab,
        "_policy_lane_for_index",
        lambda _state, _index: "guided",
    )
    monkeypatch.setattr(
        play_hand_lab,
        "_policy_cap_decision",
        lambda _state, _attributes: cap_decision,
    )
    durable_tasks = {
        task_id: {
            "task_id": task_id,
            "status": "pending" if task_id == "task-pending" else "terminal",
        }
        for task_id in lane.task_ids
    }
    unresolved = [
        {
            "task_id": "task-pending",
            "lane_id": lane.lane_id,
            "policy_assignment": {"policy_lane": "wild"},
        }
    ]

    with pytest.raises(
        DurableExecutionError,
        match="durable journal task policy assignment mismatch",
    ):
        play_hand_lab_startup._recompute_policy_state_without_terminal_io(
            play_hand_lab,
            policy_state,
            lanes=[lane],
            unresolved_tasks=unresolved,
            durable_tasks_by_id=durable_tasks,
            pruned_lane_count=0,
        )


def test_first_unchanged_resume_checkpoint_is_not_rewritten(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "play-hand-lab-state.json"
    lane = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-state-lane-00000",
        run_dir=tmp_path / "lane",
        task_ids=["task-1"],
        current_phase="baseline",
    )
    shape = play_hand_lab_startup._state_shape(
        [lane],
        next_lane_index=1,
        reserved_lane_indices=[],
        recorded_result_count=0,
    )
    play_hand_lab_startup._LOADED_STATE_SHAPES[
        play_hand_lab_startup._path_key(path)
    ] = shape
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(
        play_hand_lab_startup,
        "_ORIGINAL_WRITE_CAMPAIGN_STATE",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    result = play_hand_lab._write_campaign_state(
        path,
        runtime=play_hand_lab.PlayHandLabRuntimeConfig(),
        campaign_id="phase3-state",
        lanes=[lane],
        history=play_hand_lab.LabCampaignHistory(),
        next_lane_index=1,
        recorded_result_count=0,
        reserved_lane_indices=[],
    )

    assert result is None
    assert calls == []
