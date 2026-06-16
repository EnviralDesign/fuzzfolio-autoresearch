import json
from pathlib import Path
from types import SimpleNamespace

import autoresearch.play_hand_massive as massive
from autoresearch.ledger import load_all_run_attempts, load_attempts, write_attempts
from autoresearch.portfolio import select_dashboard_preferred_attempt_rows


def test_normalize_massive_runtime_config_clamps_active_lanes() -> None:
    config = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=3,
            active_lanes=99,
            timeframe="m1",
            min_indicators=4,
            max_indicators=2,
            screen_months=0,
        )
    )

    assert config.lanes == 3
    assert config.active_lanes == 3
    assert config.timeframe == "M1"
    assert config.min_indicators == 4
    assert config.max_indicators == 4
    assert config.screen_months == 1


def test_should_expand_lane_keeps_dry_run_open() -> None:
    assert massive.should_expand_lane(
        baseline_score=None,
        baseline_floor=10.0,
        dry_run=True,
    )
    assert not massive.should_expand_lane(
        baseline_score=9.9,
        baseline_floor=10.0,
        dry_run=False,
    )
    assert massive.should_expand_lane(
        baseline_score=10.0,
        baseline_floor=10.0,
        dry_run=False,
    )


def test_adaptive_lane_window_uses_worker_slots_with_safe_fallbacks() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=20,
            active_lanes=12,
            adaptive_lanes=True,
            min_active_lanes=2,
            target_worker_slots_per_lane=8,
        )
    )

    assert massive._desired_active_lanes(runtime, {"ok": True, "slots": 32}) == 4
    assert massive._desired_active_lanes(runtime, {"ok": True, "slots": 1}) == 2
    assert massive._desired_active_lanes(runtime, {"ok": True, "slots": 0}) == 2
    assert massive._desired_active_lanes(runtime, {"ok": False}) == 12

    fixed = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(lanes=20, active_lanes=7)
    )
    assert massive._desired_active_lanes(fixed, {"ok": True, "slots": 64}) == 7


def test_cmd_play_hand_massive_dry_run_writes_first_class_lane_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(),
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            plot_lower_is_better=False,
        ),
    )
    monkeypatch.setattr(massive, "load_config", lambda: config)

    def fake_evaluate_profile(ctx, **_kwargs):
        attempts = load_attempts(ctx.attempts_path)
        attempt_id = f"{ctx.run_id}-attempt-{len(attempts) + 1:05d}"
        attempts.append(
            {
                "attempt_id": attempt_id,
                "sequence": len(attempts) + 1,
                "created_at": "2026-06-16T00:00:00+00:00",
                "run_id": ctx.run_id,
                "candidate_name": "baseline",
                "artifact_dir": str((ctx.evals_dir / "baseline").resolve()),
                "profile_ref": "profiles/baseline.json",
                "profile_path": str((ctx.profiles_dir / "baseline.json").resolve()),
                "primary_score": 12.5,
                "composite_score": 12.5,
                "score_basis": "dry-run-fake",
                "metrics": {},
                "best_summary": {},
                "sensitivity_snapshot_path": None,
                "runner": "play_hand_v1",
            }
        )
        write_attempts(ctx.attempts_path, attempts)
        return {"attempt_id": attempt_id, "score": 12.5}

    monkeypatch.setattr(massive, "_run_profile_evaluation", fake_evaluate_profile)

    exit_code = massive.cmd_play_hand_massive(
        lanes=2,
        active_lanes=2,
        instrument=["EURUSD"],
        timeframe="M1",
        sweep_budget="low",
        min_indicators=2,
        max_indicators=2,
        seed=42,
        dry_run=True,
        as_json=False,
    )

    assert exit_code == 0
    campaign_dirs = list(
        (tmp_path / "runs" / "derived" / massive.PLAY_HAND_MASSIVE_CAMPAIGNS_DIR).glob(
            "*-playhand-massive-campaign-v1"
        )
    )
    lane_dirs = sorted((tmp_path / "runs").glob("*-playhand-massive-lane-*-v1"))
    old_campaign_dirs = list((tmp_path / "runs").glob("*-playhand-massive-v1"))

    assert len(campaign_dirs) == 1
    assert len(lane_dirs) == 2
    assert old_campaign_dirs == []

    campaign_metadata = json.loads(
        (campaign_dirs[0] / "run-metadata.json").read_text(encoding="utf-8")
    )
    campaign_summary = json.loads(
        (campaign_dirs[0] / "play-hand-massive-campaign-summary.json").read_text(
            encoding="utf-8"
        )
    )
    campaign_events = (
        campaign_dirs[0] / "play-hand-massive-campaign-events.jsonl"
    ).read_text(encoding="utf-8")

    assert campaign_metadata["runner"] == massive.PLAY_HAND_MASSIVE_RUNNER
    assert campaign_metadata["run_kind"] == "play_hand_massive_campaign"
    assert campaign_metadata["lanes"] == 2
    assert campaign_metadata["active_lanes"] == 2
    assert sorted(campaign_metadata["lane_run_ids"]) == [path.name for path in lane_dirs]
    assert campaign_summary["lanes"] == 2
    assert len(campaign_summary["lane_results"]) == 2
    assert "\"status\": \"started\"" in campaign_events

    all_attempts = load_all_run_attempts(tmp_path / "runs")
    preferred_rows, preferred_info = select_dashboard_preferred_attempt_rows(all_attempts)
    assert len(all_attempts) == 2
    assert len(preferred_rows) == 2
    assert preferred_info["incomplete_playhand_dropped_count"] == 0

    for lane_dir in lane_dirs:
        metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
        attempts = (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
        assert metadata["runner"] == "play_hand_v1"
        assert metadata["generated_by_runner"] == massive.PLAY_HAND_MASSIVE_RUNNER
        assert metadata["run_kind"] == "play_hand_massive_lane"
        assert metadata["dry_run"] is True
        assert metadata["canonical_attempt_id"]
        assert len(attempts) == 1
        attempt = json.loads(attempts[0])
        assert attempt["run_id"] == lane_dir.name
        assert attempt["runner"] == "play_hand_v1"
        assert attempt["generated_by_runner"] == massive.PLAY_HAND_MASSIVE_RUNNER
        assert attempt["is_canonical_attempt"] is True
        assert attempt["is_canonical_playhand_attempt"] is True
