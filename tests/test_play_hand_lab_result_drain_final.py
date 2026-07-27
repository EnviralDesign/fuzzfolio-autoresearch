from __future__ import annotations

import json
from pathlib import Path

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_result_fastpath as fastpath
from autoresearch import play_hand_lab_startup
from autoresearch import play_hand_lab_throughput as throughput
from autoresearch.durable_execution import atomic_write_json_streaming
from autoresearch.evidence_plan import canonical_json
from autoresearch.scoring import CANONICAL_SCORE_LAB_VERSION


def test_streaming_atomic_json_matches_canonical_bytes(tmp_path: Path) -> None:
    payload = {
        "lanes": [
            {"lane_id": f"lane-{index}", "values": list(range(100))}
            for index in range(50)
        ],
        "schema_version": "test-v1",
    }
    path = tmp_path / "state.json"
    atomic_write_json_streaming(path, payload, buffer_bytes=1024)
    assert path.read_bytes() == canonical_json(payload).encode("utf-8")


def test_terminal_policy_compaction_keeps_resume_evidence_only() -> None:
    assignment = {
        "policy_lane": "guided",
        "policy_manifest_sha256": "sha256:" + "a" * 64,
        "policy_outcome_type": "policy_lane_selected",
        "allocation": {"lane_index": 1},
        "candidate_attributes": {"candidate_id": "candidate-1"},
        "cap_decision": {"outcome": "accepted", "charges": {}},
        "negative_prior_runtime": {"current_atlas_generation": "g"},
        "candidate_fallback_decisions": [{"large": "x" * 1000}],
        "negative_prior_decisions": [{"large": "y" * 1000}],
    }
    compact = play_hand_lab._compact_policy_assignment_snapshot(assignment)
    assert "candidate_fallback_decisions" not in compact
    assert "negative_prior_decisions" not in compact
    assert compact["candidate_attributes"] == assignment["candidate_attributes"]
    assert compact["cap_decision"] == assignment["cap_decision"]
    assert assignment["candidate_fallback_decisions"]


def test_streaming_state_load_compacts_terminal_policy_and_event_audit(
    tmp_path: Path,
) -> None:
    runtime = play_hand_lab.PlayHandLabRuntimeConfig(
        campaign_id="phase3-streaming-state",
        target_runs=1,
    )
    lane = play_hand_lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="phase3-streaming-state-lane-00000",
        run_dir=tmp_path / "lane",
        current_phase="tombstoned",
        terminal=True,
        policy_assignment={
            "policy_lane": "guided",
            "candidate_fallback_decisions": [{"large": "x" * 50_000}],
            "negative_prior_decisions": [{"large": "y" * 50_000}],
        },
        phase_lifecycle_events=[
            {"event": "terminal", "policy_assignment": {"large": "z" * 50_000}}
        ],
    )
    # Build a legacy-heavy payload directly; the current writer would compact a
    # terminal lane before persistence.
    lane_payload = play_hand_lab._lane_state_payload(lane)
    lane_payload["policy_assignment"] = dict(lane.policy_assignment)
    lane_payload["phase_lifecycle_events"] = list(lane.phase_lifecycle_events)
    state_path = tmp_path / "play-hand-lab-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "play-hand-lab-durable-state-v1",
                "lineage": play_hand_lab._campaign_state_lineage(
                    runtime,
                    "phase3-streaming-state",
                ),
                "next_lane_index": 1,
                "reserved_lane_indices": [],
                "recorded_result_count": 1,
                "history": {},
                "campaign_policy_state": None,
                "lanes": [lane_payload],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    lanes, _history, _next, _reserved, _count = (
        play_hand_lab_startup._load_campaign_state_fast(
            play_hand_lab,
            state_path,
            runtime=runtime,
            campaign_id="phase3-streaming-state",
        )
    )

    assert len(lanes) == 1
    assert "candidate_fallback_decisions" not in lanes[0].policy_assignment
    assert "negative_prior_decisions" not in lanes[0].policy_assignment
    assert lanes[0].phase_lifecycle_events == [{"event": "terminal"}]
    diagnostics = play_hand_lab_startup.startup_diagnostics()
    assert diagnostics["terminal_policy_assignments_compacted"] >= 1
    assert diagnostics["terminal_event_assignments_dropped"] >= 1
    assert diagnostics["campaign_state_stream_peak_bytes"] <= state_path.stat().st_size


def test_attempt_cache_projects_phase3_rows_and_compacts_policy(
    tmp_path: Path,
) -> None:
    path = tmp_path / "phase3-campaign-lane-00001" / "attempts.jsonl"
    assignment = {
        "policy_lane": "guided",
        "candidate_attributes": {"candidate_id": "candidate-1"},
        "candidate_fallback_decisions": [{"large": "x" * 100_000}],
        "negative_prior_decisions": [{"large": "y" * 100_000}],
    }
    throughput._ORIGINAL_APPEND_ATTEMPT_ROW(
        path,
        {
            "attempt_id": "a-1",
            "sequence": 1,
            "candidate_name": "candidate",
            "lab_campaign_task_id": "task-1",
            "lab_worker_result_sha256": "sha256:" + "a" * 64,
            "artifact_dir": str(tmp_path / "artifact"),
            "policy_assignment": assignment,
            "unused_large_field": "z" * 100_000,
        },
    )
    with throughput._LOCK:
        throughput._ATTEMPT_CACHE.clear()
        throughput._ATTEMPT_CACHE_ROWS = 0

    rows = throughput._cached_load_attempts(path)

    assert len(rows) == 1
    assert rows[0]["sequence"] == 1
    assert rows[0]["candidate_name"] == "candidate"
    assert "unused_large_field" not in rows[0]
    assert "candidate_fallback_decisions" not in rows[0]["policy_assignment"]
    assert "negative_prior_decisions" not in rows[0]["policy_assignment"]
    assert rows[0]["policy_assignment"]["candidate_attributes"] == {
        "candidate_id": "candidate-1"
    }


def test_phase3_attempt_append_persists_compact_policy_assignment(
    tmp_path: Path,
) -> None:
    path = tmp_path / "phase3-campaign-lane-00002" / "attempts.jsonl"
    throughput._cached_append_attempt_row(
        path,
        {
            "attempt_id": "a-1",
            "lab_campaign_task_id": "task-1",
            "policy_assignment": {
                "policy_lane": "guided",
                "candidate_fallback_decisions": [{"large": "x" * 10_000}],
                "negative_prior_decisions": [{"large": "y" * 10_000}],
            },
        },
    )
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["policy_assignment"] == {"policy_lane": "guided"}


def test_direct_score_keeps_only_canonical_summary_fields() -> None:
    score = fastpath._score_from_sensitivity(
        play_hand_lab,
        {
            "data": {
                "aggregate": {
                    "score_lab": {
                        "version": CANONICAL_SCORE_LAB_VERSION,
                        "score": 77.0,
                        "combiner": "canonical",
                    },
                    "best_cell_path_metrics": {"psr": 0.9},
                    "large_unused": "x" * 100_000,
                }
            }
        },
    )
    assert score is not None
    assert score.best_summary["score_lab"]["score"] == 77.0
    assert "large_unused" not in score.best_summary
