import json
from pathlib import Path

import pytest

from autoresearch.ledger import write_attempts, write_run_metadata
from autoresearch.playhand_health import (
    PLAY_HAND_HEALTH_VERSION,
    build_play_hand_evidence,
    build_play_hand_health,
    heal_play_hand_run_metadata,
)


def test_build_play_hand_health_marks_tombstoned_calendar_report_failure() -> None:
    metadata = {
        "runner": "play_hand_v1",
        "run_tombstoned": True,
        "tombstone_reason": "final_36mo_score_not_positive",
        "final_attempt_id": "run-1-attempt-00004",
        "final_scrutiny_score": 0.0,
        "final_scrutiny_passed": False,
        "calendar_gate_mode": "report",
        "calendar_gate": {
            "passed": False,
            "reasons": ["calendar_segments_insufficient"],
            "metrics": {"positive_segment_count": 2, "min_positive_segments": 3},
        },
        "play_hand_phase_scores": {
            "baseline": 63.5626,
            "lookback_top_3mo": 46.5363,
            "focused_top_3mo": 66.1468,
        },
    }

    health = build_play_hand_health(
        run_metadata=metadata,
        attempts=[{"attempt_id": "run-1-attempt-00004", "runner": "play_hand_v1"}],
        computed_at="2026-06-14T00:00:00+00:00",
    )

    assert health["version"] == PLAY_HAND_HEALTH_VERSION
    assert health["status"] == "tombstoned"
    assert health["scores"]["selected_final_score"] == 0.0
    assert health["calendar"]["passed"] is False
    assert "calendar_gate_failed_report_only" in health["reasons"]
    assert health["early_exit_inputs"]["lookback_delta_vs_baseline"] == pytest.approx(-17.0263)


def test_build_play_hand_health_marks_canonical_retained() -> None:
    health = build_play_hand_health(
        run_metadata={
            "runner": "play_hand_v1",
            "canonical_attempt_id": "run-1-attempt-00005",
            "final_attempt_id": "run-1-attempt-00005",
            "final_scrutiny_score": 62.5,
            "final_scrutiny_passed": True,
        },
        attempts=[{"attempt_id": "run-1-attempt-00005", "runner": "play_hand_v1"}],
        catalog_row={"full_backtest_validation_status_36m": "valid", "has_full_backtest_36m": True},
        computed_at="2026-06-14T00:00:00+00:00",
    )

    assert health["status"] == "canonical_retained"
    assert health["final"]["canonical_attempt_id"] == "run-1-attempt-00005"


def test_build_play_hand_evidence_treats_missing_artifacts_as_missing_not_failure() -> None:
    evidence = build_play_hand_evidence(
        run_metadata={
            "runner": "play_hand_v1",
            "final_attempt_id": "run-1-attempt-00002",
            "final_scrutiny_score": 55.0,
            "final_scrutiny_passed": True,
        },
        attempts=[{"attempt_id": "run-1-attempt-00002", "runner": "play_hand_v1"}],
    )
    health = build_play_hand_health(
        run_metadata={
            "runner": "play_hand_v1",
            "final_attempt_id": "run-1-attempt-00002",
            "final_scrutiny_score": 55.0,
            "final_scrutiny_passed": True,
        },
        attempts=[{"attempt_id": "run-1-attempt-00002", "runner": "play_hand_v1"}],
        computed_at="2026-06-14T00:00:00+00:00",
    )

    assert evidence["artifacts"]["missing"] == ["full_backtest_36m"]
    assert health["status"] == "missing_artifacts"
    assert "final_36mo_scrutiny_failed" not in health["reasons"]


def test_build_play_hand_health_keeps_non_final_run_in_progress() -> None:
    health = build_play_hand_health(
        run_metadata={
            "runner": "play_hand_v1",
            "run_status": "running",
        },
        attempts=[
            {
                "attempt_id": "run-1-attempt-00001",
                "run_id": "run-1",
                "runner": "play_hand_v1",
                "attempt_role": "seed",
            }
        ],
        computed_at="2026-06-14T00:00:00+00:00",
    )

    assert health["status"] == "in_progress"
    assert health["final"]["final_attempt_id"] is None
    assert health["scores"]["selected_final_score"] is None


def test_heal_play_hand_run_metadata_is_idempotent(tmp_path: Path) -> None:
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    write_run_metadata(
        run_dir,
        {
            "run_id": "run-1",
            "runner": "play_hand_v1",
            "canonical_attempt_id": "run-1-attempt-00001",
            "final_attempt_id": "run-1-attempt-00001",
            "final_scrutiny_score": 61.0,
            "final_scrutiny_passed": True,
        },
    )
    write_attempts(
        run_dir / "attempts.jsonl",
        [
            {
                "attempt_id": "run-1-attempt-00001",
                "run_id": "run-1",
                "runner": "play_hand_v1",
            }
        ],
    )

    first = heal_play_hand_run_metadata(run_dir)
    second = heal_play_hand_run_metadata(run_dir)
    metadata = json.loads((run_dir / "run-metadata.json").read_text(encoding="utf-8"))

    assert first["updated"] is True
    assert second["updated"] is False
    assert metadata["play_hand_health"] == first["play_hand_health"]
