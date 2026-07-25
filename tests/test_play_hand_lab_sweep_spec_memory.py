from __future__ import annotations

from autoresearch import play_hand_lab
from autoresearch.play_hand_lab_memory_deep import _compact_sweep_task_spec


def test_completed_sweep_spec_can_drop_rebuildable_params() -> None:
    spec = {
        "phase": "focused",
        "task_kind": "sweep_shard",
        "sweep_id": "sweep-1",
        "shard_id": "shard-1",
        "axes": ["talibConfig.RSI.timeperiod"],
        "axis_key_map": {"RSI.timeperiod": "talibConfig.RSI.timeperiod"},
        "axis_plan": {"max_permutations": 8},
        "expanded_permutation_count": 8,
        "permutation_budget_applied": False,
        "permutation_start": 0,
        "permutation_count": 8,
        "params_by_index": {index: {"RSI.timeperiod": index + 2} for index in range(8)},
        "params_by_index_sha256": "sha256:" + "a" * 64,
        "result_detail": "summary",
        "profile_path": "C:/large/profile.json",
        "profile_ref": "lab-inline:test",
        "evidence_plan": {"large": "x" * 1000},
        "policy_assignment": {"large": "y" * 1000},
    }

    _compact_sweep_task_spec(spec)

    assert "params_by_index" not in spec
    assert "profile_path" not in spec
    assert "evidence_plan" not in spec
    assert set(spec) == {
        "phase",
        "task_kind",
        "sweep_id",
        "shard_id",
        "axes",
        "axis_key_map",
        "axis_plan",
        "expanded_permutation_count",
        "permutation_budget_applied",
        "permutation_start",
        "permutation_count",
        "params_by_index_sha256",
        "result_detail",
    }


def test_rebuild_sweep_params_uses_frozen_axes_and_profile() -> None:
    lane = play_hand_lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="phase3-memory-lane-00001",
        run_dir=play_hand_lab.Path("."),
        profile_payload={
            "talibConfig": {
                "RSI": {
                    "timeperiod": 14,
                }
            }
        },
        incumbent_profile_payload={
            "talibConfig": {
                "RSI": {
                    "timeperiod": 14,
                }
            }
        },
    )
    spec = {
        "phase": "focused",
        "task_kind": "sweep_shard",
        "axes": ["talibConfig.RSI.timeperiod"],
        "axis_plan": {"max_permutations": 8},
        "expanded_permutation_count": 8,
        "permutation_start": 0,
        "permutation_count": 8,
    }

    rebuilt = play_hand_lab._rebuild_sweep_shard_params_by_index(lane, spec)

    assert rebuilt is not None
    assert len(rebuilt) == 8
