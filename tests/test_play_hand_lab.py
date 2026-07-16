from __future__ import annotations

import json
import random
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from autoresearch import play_hand_lab as lab


def _profile_payload() -> dict:
    return {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {
            "name": "Lab Smoke",
            "description": "Test profile",
            "directionMode": "both",
            "isActive": False,
            "version": "v1",
            "instruments": ["EURUSD"],
            "notificationThreshold": 80,
            "indicators": [
                {
                    "meta": {"id": "RSI", "instanceId": "test-rsi"},
                    "config": {
                        "label": "RSI",
                        "timeframe": "M5",
                        "lookbackBars": 1,
                        "isActive": True,
                        "weight": 1.0,
                        "talibConfig": [{"name": "timeperiod", "value": 14}],
                    },
                }
            ],
        },
    }


def _test_config(tmp_path: Path) -> SimpleNamespace:
    runs_root = tmp_path / "runs"
    return SimpleNamespace(
        repo_root=tmp_path,
        runs_root=runs_root,
        derived_root=runs_root / "derived",
        fuzzfolio=SimpleNamespace(workspace_root=None),
        research=SimpleNamespace(plot_lower_is_better=False),
    )


def _campaign_ctx(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        run_id="campaign-1",
        run_dir=tmp_path,
        events_path=tmp_path / "events.jsonl",
        io_lock=threading.RLock(),
    )


def _historical_seed_plan() -> dict:
    return {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "ADX",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {},
            }
        },
    }


def _write_historical_seed_plan(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "historical-seed-plan.json"
    path.write_text(
        json.dumps(payload if payload is not None else _historical_seed_plan()),
        encoding="utf-8",
    )
    return path


def _level_c_runtime(
    tmp_path: Path,
    *,
    seed_plan_payload: dict | None = None,
    **overrides,
) -> lab.PlayHandLabRuntimeConfig:
    seed_plan_path = (
        overrides["seed_plan_path"]
        if "seed_plan_path" in overrides
        else _write_historical_seed_plan(tmp_path, seed_plan_payload)
    )
    expected_seed_plan_sha256 = (
        overrides["expected_seed_plan_sha256"]
        if "expected_seed_plan_sha256" in overrides
        else lab._file_sha256(seed_plan_path)
    )
    values = {
        "as_of_date": "2025-06-30T00:00:00Z",
        "campaign_id": "formal-campaign-2025-06",
        "campaign_mode": "finite",
        "task_mode": "deep_replay",
        "pipeline_mode": "play_hand",
        "target_runs": 1,
        "active_runs": 1,
        "strict_scoring": True,
        "seed": 17,
        "worker_contract_hash": "sha256:" + "a" * 64,
        "lake_manifest_sha256": "sha256:" + "b" * 64,
        "seed_plan_path": seed_plan_path,
        "expected_seed_plan_sha256": expected_seed_plan_sha256,
        "research_generation_id": "generation-2025-06",
        "level_c_protocol_id": "sha256:" + "c" * 64,
        "cutoff_key": "A",
    }
    values.update(overrides)
    return lab.PlayHandLabRuntimeConfig(**values)


def test_normalize_runtime_loads_existing_gateway_token_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FUZZFOLIO_LAB_GATEWAY_TOKEN", raising=False)
    token_file = tmp_path / "gateway-token.txt"
    token_file.write_text("shared-token", encoding="ascii")
    monkeypatch.setenv("FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE", str(token_file))

    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.gateway_token == "shared-token"


def test_enqueue_gateway_tasks_retries_transient_request_errors(tmp_path: Path) -> None:
    class FlakyGateway:
        def __init__(self) -> None:
            self.calls = 0

        def enqueue_tasks(self, tasks):
            self.calls += 1
            if self.calls < 3:
                raise requests.exceptions.ReadTimeout("gateway timed out")
            return {"enqueued": len(tasks)}

    gateway = FlakyGateway()
    ctx = _campaign_ctx(tmp_path)

    result = lab._enqueue_gateway_tasks_with_retries(
        gateway,
        ctx,
        [{"task_id": "task-1"}],
        reason="test",
        failure_limit=3,
        retry_base_seconds=0.0,
    )

    events = [
        json.loads(line)
        for line in ctx.events_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert result == {"enqueued": 1}
    assert gateway.calls == 3
    assert [event["status"] for event in events] == ["task_enqueue_failed", "task_enqueue_failed"]
    assert events[0]["attempt"] == 1
    assert events[1]["attempt"] == 2


def test_runtime_event_payload_redacts_gateway_token_and_preserves_paths(tmp_path: Path) -> None:
    runtime = lab.PlayHandLabRuntimeConfig(
        gateway_token="super-secret-lab-token",
        profile_path=tmp_path / "profile.json",
        trading_dashboard_root=tmp_path / "Trading-Dashboard",
    )

    payload = lab._runtime_event_payload(runtime)

    assert payload["gateway_token"] == "[redacted]"
    assert payload["profile_path"] == str(tmp_path / "profile.json")
    assert payload["trading_dashboard_root"] == str(tmp_path / "Trading-Dashboard")
    assert "super-secret-lab-token" not in json.dumps(payload)


def test_normalize_runtime_defaults_to_cloud_tolerant_lab_attempts() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.max_attempts == 8


def test_normalize_runtime_requires_lake_identity_for_historical_mode() -> None:
    with pytest.raises(ValueError, match="exact lake_manifest_sha256"):
        lab._normalize_runtime(
            lab.PlayHandLabRuntimeConfig(as_of_date="2025-06-30T00:00:00Z")
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"campaign_mode": "continuous"}, "campaign_mode=finite"),
        ({"target_runs": 0}, "positive, explicit target_runs"),
        ({"strict_scoring": False}, "strict_scoring=True"),
        ({"seed": None}, "explicit seed"),
        ({"worker_contract_hash": None}, "explicit exact worker_contract_hash"),
        ({"lake_manifest_sha256": "sha256:not-a-hash"}, "exact lake_manifest_sha256"),
        ({"expected_seed_plan_sha256": None}, "exact expected_seed_plan_sha256"),
        ({"campaign_id": None}, "campaign_id"),
        ({"campaign_id": "unsafe/campaign"}, "campaign_id"),
        ({"research_generation_id": ""}, "research_generation_id"),
        ({"level_c_protocol_id": "level-c-v2"}, "level_c_protocol_id"),
        ({"cutoff_key": "cutoff-2025-06-30"}, "cutoff_key"),
    ],
)
def test_normalize_runtime_historical_mode_fails_closed_for_level_c_preconditions(
    tmp_path: Path,
    overrides: dict,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        lab._normalize_runtime(_level_c_runtime(tmp_path, **overrides))


def test_normalize_runtime_historical_mode_rejects_non_json_seed_plan(tmp_path: Path) -> None:
    non_json_path = tmp_path / "historical-seed-plan.txt"
    non_json_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="existing JSON seed plan file"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                seed_plan_path=non_json_path,
            )
        )


def test_normalize_runtime_historical_mode_rejects_malformed_seed_plan(tmp_path: Path) -> None:
    seed_plan_path = tmp_path / "historical-seed-plan.json"
    seed_plan_path.write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError, match="seed plan must be valid JSON"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                seed_plan_path=seed_plan_path,
                expected_seed_plan_sha256=lab._file_sha256(seed_plan_path),
            )
        )


def test_normalize_runtime_historical_mode_requires_matching_seed_plan_hash(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not match expected_seed_plan_sha256"):
        lab._normalize_runtime(
            _level_c_runtime(
                tmp_path,
                expected_seed_plan_sha256="sha256:" + "c" * 64,
            )
        )


def test_normalize_runtime_historical_mode_preserves_verified_level_c_lineage(
    tmp_path: Path,
) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path, target_runs=3))

    assert runtime.research_generation_id == "generation-2025-06"
    assert runtime.campaign_id == "formal-campaign-2025-06"
    assert runtime.level_c_protocol_id == "sha256:" + "c" * 64
    assert runtime.cutoff_key == "A"
    assert runtime.expected_seed_plan_sha256 == lab._file_sha256(runtime.seed_plan_path)
    assert runtime.terminal_lane_retention >= 3


def test_normalize_runtime_exploratory_campaign_id_is_optional_and_validated() -> None:
    assert lab._normalize_runtime(lab.PlayHandLabRuntimeConfig()).campaign_id is None
    assert (
        lab._normalize_runtime(lab.PlayHandLabRuntimeConfig(campaign_id="explore-42")).campaign_id
        == "explore-42"
    )
    with pytest.raises(ValueError, match="campaign_id"):
        lab._normalize_runtime(lab.PlayHandLabRuntimeConfig(campaign_id="../escape"))


def test_cmd_play_hand_lab_uses_explicit_historical_campaign_id_for_exact_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_config = _test_config(tmp_path)
    runtime = _level_c_runtime(tmp_path, campaign_id="formal-campaign-2025-06")
    captured: dict[str, object] = {}

    class FakeCli:
        def __init__(self, _config) -> None:
            pass

    class StopAfterCampaignSetup(Exception):
        pass

    def stop_after_metadata(campaign_ctx, **_kwargs) -> None:
        captured["campaign_id"] = campaign_ctx.run_id
        captured["campaign_dir"] = campaign_ctx.run_dir
        raise StopAfterCampaignSetup()

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", lambda **_kwargs: object())
    monkeypatch.setattr(lab, "_write_campaign_metadata", stop_after_metadata)

    with pytest.raises(StopAfterCampaignSetup):
        lab.cmd_play_hand_lab(runtime)

    expected_dir = (
        fake_config.runs_root
        / "derived"
        / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR
        / "formal-campaign-2025-06"
    )
    assert captured["campaign_id"] == "formal-campaign-2025-06"
    assert captured["campaign_dir"] == expected_dir
    assert expected_dir.is_dir()


def test_historical_campaign_path_rejects_conflicting_lineage(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path))
    campaign_dir = tmp_path / runtime.campaign_id
    campaign_dir.mkdir()
    (campaign_dir / "run-metadata.json").write_text(
        json.dumps(
            {
                **lab._historical_campaign_lineage(runtime),
                "research_generation_id": "generation-conflict",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting historical lineage: research_generation_id"):
        lab._reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)


def test_historical_campaign_path_rejects_resume_even_with_matching_lineage(tmp_path: Path) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path))
    campaign_dir = tmp_path / runtime.campaign_id
    campaign_dir.mkdir()
    (campaign_dir / "run-metadata.json").write_text(
        json.dumps(lab._historical_campaign_lineage(runtime)),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="resume behavior is not supported"):
        lab._reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)


def test_level_c_lineage_and_seed_hash_are_persisted_in_campaign_and_lane_metadata(
    tmp_path: Path,
) -> None:
    runtime = lab._normalize_runtime(_level_c_runtime(tmp_path))
    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    campaign_ctx = _campaign_ctx(campaign_dir)
    lane_dir = tmp_path / "lane"
    lane_dir.mkdir()
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-1",
        run_dir=lane_dir,
    )

    lab._write_campaign_metadata(
        campaign_ctx,
        runtime=runtime,
        status="starting",
        started_at="2025-06-30T00:00:00Z",
    )
    lab._write_lane_metadata(
        lane,
        campaign_ctx=campaign_ctx,
        runtime=runtime,
        status="queued",
        started_at="2025-06-30T00:00:00Z",
    )

    campaign_metadata = json.loads(
        (campaign_dir / "run-metadata.json").read_text(encoding="utf-8")
    )
    lane_metadata = json.loads(
        (lane_dir / "run-metadata.json").read_text(encoding="utf-8")
    )
    for metadata in [campaign_metadata, lane_metadata]:
        assert metadata["campaign_id"] == "campaign-1"
        assert metadata["research_generation_id"] == "generation-2025-06"
        assert metadata["level_c_protocol_id"] == "sha256:" + "c" * 64
        assert metadata["cutoff_key"] == "A"
        assert metadata["expected_seed_plan_sha256"] == runtime.expected_seed_plan_sha256
        assert metadata["play_hand_seed_plan_sha256"] == runtime.expected_seed_plan_sha256
        assert metadata["formal_historical_level_c"] is True


@pytest.mark.parametrize(
    ("initial_status", "target_runs", "expected_reason"),
    [
        ("stopped", 1, "historical_campaign_stopped"),
        ("completed", 2, "historical_campaign_incomplete"),
    ],
)
def test_historical_campaign_finalization_rejects_stopped_or_partial_promotion(
    tmp_path: Path,
    initial_status: str,
    target_runs: int,
    expected_reason: str,
) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-1",
        run_dir=tmp_path / "lane",
        terminal=True,
        run_promoted=True,
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=target_runs,
    )

    status, reason = lab._finalize_historical_campaign_status(
        initial_status,
        lanes=[lane],
        runtime=runtime,
    )

    assert status == "failed"
    assert reason == expected_reason
    assert lane.run_promoted is False
    assert lane.tombstone_reason == expected_reason
    assert lane.current_phase == "incomplete"


def test_historical_campaign_completion_accepts_terminal_research_rejections(
    tmp_path: Path,
) -> None:
    lanes = [
        lab.LabLaneState(
            lane_id="lane_000",
            lane_index=0,
            run_id="lane-0",
            run_dir=tmp_path / "lane-0",
            terminal=True,
            task_ids=["task-0"],
            completed_task_ids={"task-0"},
            tombstone_reason="validation_12mo_failed",
        ),
        lab.LabLaneState(
            lane_id="lane_001",
            lane_index=1,
            run_id="lane-1",
            run_dir=tmp_path / "lane-1",
            terminal=True,
            task_ids=["task-1"],
            completed_task_ids={"task-1"},
            tombstone_reason="final_36mo_failed",
        ),
        lab.LabLaneState(
            lane_id="lane_002",
            lane_index=2,
            run_id="lane-2",
            run_dir=tmp_path / "lane-2",
            terminal=True,
            task_ids=["task-2"],
            completed_task_ids={"task-2"},
            tombstone_reason="no_signal",
        ),
        lab.LabLaneState(
            lane_id="lane_003",
            lane_index=3,
            run_id="lane-3",
            run_dir=tmp_path / "lane-3",
            terminal=True,
            task_ids=["task-3"],
            completed_task_ids={"task-3"},
            tombstone_reason="no-valid-cell",
        ),
        lab.LabLaneState(
            lane_id="lane_004",
            lane_index=4,
            run_id="lane-4",
            run_dir=tmp_path / "lane-4",
            terminal=True,
            task_ids=["task-4"],
            completed_task_ids={"task-4"},
            tombstone_reason="nonviable",
        ),
    ]
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=len(lanes),
    )

    status, reason = lab._finalize_historical_campaign_status(
        "completed",
        lanes=lanes,
        runtime=runtime,
    )

    assert status == "completed"
    assert reason is None
    assert not any(lane.run_promoted for lane in lanes)


@pytest.mark.parametrize(
    ("terminal", "completed", "failed", "tombstone_reason"),
    [
        (False, set(), set(), None),
        (True, set(), {"task-0"}, "lab_stage_worker_failed"),
    ],
    ids=["incomplete", "infrastructure-failed"],
)
def test_historical_campaign_completion_rejects_incomplete_or_failed_lanes(
    tmp_path: Path,
    terminal: bool,
    completed: set[str],
    failed: set[str],
    tombstone_reason: str | None,
) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-0",
        run_dir=tmp_path / "lane-0",
        terminal=terminal,
        task_ids=["task-0"],
        completed_task_ids=completed,
        failed_task_ids=failed,
        tombstone_reason=tombstone_reason,
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        campaign_mode="finite",
        target_runs=1,
    )

    status, reason = lab._finalize_historical_campaign_status(
        "completed",
        lanes=[lane],
        runtime=runtime,
    )

    assert status == "failed"
    assert reason == "historical_campaign_incomplete"


def test_normalize_runtime_defaults_to_random_screen_and_validation_rung() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.screen_anchor_mode == "random"
    assert runtime.screen_anchor_envelope_months == 36
    assert runtime.validation_months == 12
    assert runtime.validation_min_score == 45.0
    assert runtime.scrutiny_months == 36
    assert runtime.final_min_score == 40.0


def test_normalize_runtime_resolves_instrument_pool_presets() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(
            instrument_pool_preset=["fx-major", "metals"],
            instrument_pool=["DE40"],
        )
    )

    assert runtime.instrument_pool_preset == ["fx-major", "metals"]
    assert runtime.instrument_pool == [
        "AUDUSD",
        "EURUSD",
        "GBPUSD",
        "USDCAD",
        "USDCHF",
        "USDJPY",
        "XAGUSD",
        "XAUUSD",
        "DE40",
    ]


def test_normalize_runtime_uses_target_and_active_runs() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(
            campaign_mode="finite",
            target_runs=512,
            active_runs=64,
            lanes=4,
        )
    )

    assert runtime.campaign_mode == "finite"
    assert runtime.target_runs == 512
    assert runtime.active_runs == 64
    assert runtime.lanes == 512


def test_normalize_runtime_continuous_has_no_target_by_default() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(campaign_mode="continuous")
    )

    assert runtime.campaign_mode == "continuous"
    assert runtime.target_runs is None
    assert runtime.active_runs == lab.DEFAULT_LAB_ACTIVE_RUNS


def test_normalize_runtime_defaults_to_barrier_logging() -> None:
    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.log_mode == "barrier"
    assert runtime.barrier_interval_seconds == 5.0
    assert runtime.barrier_lane_limit == 24
    assert runtime.terminal_lane_retention == 512


def test_lane_lifecycle_telemetry_tracks_phase_completion(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
    )

    lab._set_lane_phase(lane, "baseline")
    lab._register_task_spec(
        lane,
        task_id="task-1",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lab._register_task_spec(
        lane,
        task_id="task-2",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lane.completed_task_ids.add("task-1")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-1")

    assert lane.phase_task_counts["baseline_3mo"] == 2
    assert lane.phase_completed_task_counts["baseline_3mo"] == 1
    assert "baseline_3mo" not in lane.phase_completed_at

    lane.failed_task_ids.add("task-2")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-2")

    assert lane.phase_completed_at["baseline_3mo"]
    assert lane.phase_failed_task_counts["baseline_3mo"] == 1
    assert any(
        event["event"] == "phase_tasks_completed"
        and event["phase"] == "baseline_3mo"
        and event["status"] == "failed"
        for event in lane.phase_lifecycle_events
    )


def test_campaign_summary_includes_lane_lifecycle_telemetry(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
    )
    lab._set_lane_phase(lane, "baseline")
    lab._register_task_spec(
        lane,
        task_id="task-1",
        phase="baseline_3mo",
        task_kind="deep_replay",
        spec={},
    )
    lane.completed_task_ids.add("task-1")
    lab._refresh_lane_phase_result_counts(lane, task_id="task-1")

    campaign_dir = tmp_path / "campaign"
    campaign_dir.mkdir()
    campaign_ctx = SimpleNamespace(
        run_id="campaign-1",
        summary_path=campaign_dir / "play-hand-lab-campaign-summary.json",
    )

    summary = lab._write_summary(
        campaign_ctx,
        [lane],
        runtime=lab.PlayHandLabRuntimeConfig(),
        status="completed",
        started_at="2026-07-05T00:00:00+00:00",
        completed_at="2026-07-05T00:01:00+00:00",
        gateway_snapshot=None,
        recorded_results=[],
    )

    summary_lane = summary["lanes"][0]
    assert summary_lane["phase_started_at"]["baseline_3mo"]
    assert summary_lane["phase_completed_at"]["baseline_3mo"]
    assert summary_lane["phase_task_counts"]["baseline_3mo"] == 1
    assert summary_lane["phase_completed_task_counts"]["baseline_3mo"] == 1
    assert summary_lane["phase_lifecycle_events"]


def test_lab_barrier_snapshot_is_bounded_and_lane_oriented(tmp_path: Path) -> None:
    first_lane = lab.LabLaneState(
        lane_id="lane_007",
        lane_index=7,
        run_id="20260622-playhand-lab-lane-007-v1",
        run_dir=tmp_path / "lane-007",
        instruments=["EURUSD", "XAUUSD"],
        timeframe="M5",
    )
    first_lane.current_phase = "coarse"
    first_lane.task_ids = ["task-1", "task-2"]
    first_lane.completed_task_ids = {"task-1"}
    first_lane.best_score = 78.125
    first_lane.incumbent_phase = "baseline"
    hidden_lane = lab.LabLaneState(
        lane_id="lane_008",
        lane_index=8,
        run_id="20260622-playhand-lab-lane-008-v1",
        run_dir=tmp_path / "lane-008",
        instruments=["GBPUSD"],
        timeframe="M5",
    )
    hidden_lane.current_phase = "baseline"
    hidden_lane.task_ids = ["task-3"]

    text = lab._format_lab_barrier_snapshot(
        barrier_index=3,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(
            campaign_mode="continuous",
            active_runs=2,
            barrier_lane_limit=1,
        ),
        lanes=[first_lane, hidden_lane],
        tasks=[{"task_id": "task-1"}, {"task_id": "task-2"}, {"task_id": "task-3"}],
        snapshot={
            "worker_count": 4,
            "busy_worker_count": 2,
            "worker_slots": 4,
            "busy_slots": 2,
            "queued_tasks": 5,
            "live_tasks": 7,
            "completed_tasks": 11,
            "failed_tasks": 0,
            "result_backlog": 1,
            "metrics": {"tasks_enqueued": 13, "completions_accepted": 11},
        },
        metric_baseline={"tasks_enqueued": 3, "completions_accepted": 1},
        recorded_result_count=11,
    )

    lines = text.splitlines()
    assert lines[0].startswith("+")
    assert lines[-1].startswith("+")
    assert all(len(line) == lab.LAB_BARRIER_BOX_WIDTH for line in lines)
    assert "PlayHand Massive v2 barrier #0003" in text
    assert "workers=2/4 busy slots=2/4 sat=50%" in text
    assert "lane     | phase" in text
    assert "lane_007" in text
    assert "lane_007 | coarse" in text
    assert "coarse" in text
    assert "78.12" in text
    assert "1 more active lane(s) hidden" in text


def test_lab_barrier_snapshot_prefers_active_lanes_over_terminal_noise(tmp_path: Path) -> None:
    lanes: list[lab.LabLaneState] = []
    for index in range(8):
        lane = lab.LabLaneState(
            lane_id=f"lane_{index:03d}",
            lane_index=index,
            run_id=f"20260622-playhand-lab-lane-{index:03d}-v1",
            run_dir=tmp_path / f"lane-{index:03d}",
            instruments=["EURUSD"],
            timeframe="M5",
        )
        lane.current_phase = "scrutiny"
        lane.task_ids = [f"task-{index}"]
        lanes.append(lane)
    terminal_lane = lab.LabLaneState(
        lane_id="lane_099",
        lane_index=99,
        run_id="20260622-playhand-lab-lane-099-v1",
        run_dir=tmp_path / "lane-099",
        instruments=["GBPUSD"],
        timeframe="M5",
    )
    terminal_lane.terminal = True
    terminal_lane.current_phase = "tombstoned"
    terminal_lane.tombstone_reason = "early_exit_policy_enforced"
    terminal_lane.task_ids = ["task-terminal"]
    terminal_lane.completed_task_ids = {"task-terminal"}
    lanes.append(terminal_lane)

    text = lab._format_lab_barrier_snapshot(
        barrier_index=4,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(
            campaign_mode="continuous",
            active_runs=8,
            barrier_lane_limit=8,
        ),
        lanes=lanes,
        tasks=[{"task_id": f"task-{index}"} for index in range(8)],
        snapshot={},
        metric_baseline={},
        recorded_result_count=0,
    )

    assert "lane_000" in text
    assert "lane_007" in text
    assert "lane_099" not in text
    assert "terminal lanes summarized: 1 terminal, 0 promoted, 1 tombstoned" in text


def test_lab_barrier_snapshot_includes_pruned_lane_history(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_010",
        lane_index=10,
        run_id="20260622-playhand-lab-lane-010-v1",
        run_dir=tmp_path / "lane-010",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    lane.current_phase = "baseline"
    lane.task_ids = ["task-active"]
    history = lab.LabCampaignHistory(
        pruned_lane_count=10,
        pruned_task_count=25,
        pruned_completed_task_count=20,
        pruned_failed_task_count=3,
        pruned_promoted_lane_count=2,
        pruned_tombstoned_lane_count=8,
        best_score=81.25,
    )

    text = lab._format_lab_barrier_snapshot(
        barrier_index=5,
        campaign_id="campaign-1",
        runtime=lab.PlayHandLabRuntimeConfig(campaign_mode="continuous", active_runs=1),
        lanes=[lane],
        tasks=[{"task_id": "task-active"}],
        snapshot={},
        metric_baseline={},
        recorded_result_count=20,
        history=history,
    )

    assert "created=11 active=1 terminal=10" in text
    assert "promoted=2 tombstoned=8" in text
    assert "tasks=23/26 failed=3" in text
    assert "terminal lanes summarized: 10 terminal, 2 promoted, 8 tombstoned" in text


def test_compact_terminal_lane_state_drops_heavy_payloads(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_001",
        lane_index=1,
        run_id="run-1",
        run_dir=tmp_path,
        profile_payload={"large": "profile"},
        incumbent_profile_payload={"large": "incumbent"},
    )
    lane.terminal = True
    lane.task_ids = ["task-1"]
    lane.completed_task_ids.add("task-1")
    lane.task_specs["task-1"] = {"payload": "large"}
    lane.phase_rows.append({"row": 1})
    lane.phase_results["baseline"] = [{"result": 1}]
    lane.last_sweep_payload = {"large": "sweep"}
    lane.instrument_scout_result = {"large": "scout"}
    lane.best_score = 77.0

    lab._compact_terminal_lane_state(lane)

    assert lane.profile_payload is None
    assert lane.incumbent_profile_payload is None
    assert lane.last_sweep_payload is None
    assert lane.instrument_scout_result is None
    assert lane.task_specs == {}
    assert lane.phase_rows == []
    assert lane.phase_results == {}
    assert lane.best_score == 77.0
    assert lane.task_ids == ["task-1"]


def test_lab_failure_notice_includes_lane_task_phase_and_reason() -> None:
    line = lab._format_lab_event_notice(
        {
            "phase": "lab_result",
            "status": "failed",
            "run_id": "20260622-playhand-lab-lane-003-v1",
            "task_id": "task-123",
            "task_phase": "baseline",
            "task_kind": "deep_replay",
            "worker_id": "vast-worker-1",
            "lease_id": "lease-abc",
            "error": "remote data lake timeout",
        }
    )

    assert line is not None
    assert line.startswith("! lab_result failed")
    assert "lane=lane_003" in line
    assert "task_id=task-123" in line
    assert "task_phase=baseline" in line
    assert "worker_id=vast-worker-1" in line
    assert "reason=remote data lake timeout" in line
    assert lab._format_lab_event_notice({"phase": "lab_result", "status": "recorded"}) is None


def test_expand_sweep_params_enforces_permutation_budget() -> None:
    axes = [
        {"target": "profile_field", "param_key": "alpha", "values": list(range(100))},
        {"target": "profile_field", "param_key": "beta", "values": list(range(100))},
    ]

    params = lab._expand_sweep_params(axes, max_permutations=8)

    assert len(params) == 8
    assert params[0] == {"alpha": 0, "beta": 0}
    assert params[-1] == {"alpha": 99, "beta": 99}


def test_make_sweep_shard_tasks_honors_permutation_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_payload = {"indicators": [{"meta": {"instanceId": "indicator-1"}}]}
    axis_texts = [
        "indicator[0].talib.fast=1,2,3,4,5,6,7,8,9,10",
        "indicator[0].talib.slow=10,20,30,40,50,60,70,80,90,100",
        "indicator[0].config.threshold=0.1,0.2,0.3,0.4,0.5",
    ]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path,
        profile_path=tmp_path / "base.json",
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )

    axis_plan = SimpleNamespace(
        axes=axis_texts,
        selected_permutations=500,
        event_payload=lambda: {
            "selected_axes": axis_texts,
            "selected_permutations": 500,
            "max_permutations": 16,
            "search_mode": "evolutionary",
        },
    )
    monkeypatch.setattr(lab, "plan_sweep_axes", lambda *args, **kwargs: axis_plan)

    tasks = lab._make_sweep_shard_tasks(
        lane,
        phase="coarse_probe",
        runtime=lab.PlayHandLabRuntimeConfig(
            max_sweep_permutations=16,
            sweep_shard_size=4,
            worker_contract_hash="sha256:" + "a" * 64,
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        reward_matrix=None,
        worker_contract_hash="sha256:" + "a" * 64,
        profile_payload=profile_payload,
        profile_path=tmp_path / "base.json",
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        lookback_months=3,
        axis_texts=axis_texts,
        mode="evolutionary",
        analysis_window_start="2025-03-30T00:00:00Z",
        analysis_window_end="2025-06-30T00:00:00Z",
    )

    assert len(tasks) == 4
    assert sum(int(task["payload"]["permutation_count"]) for task in tasks) == 16
    assert max(len(task["payload"]["params_by_index"]) for task in tasks) == 4
    assert {task["payload"].get("result_detail") for task in tasks} == {"summary"}
    assert {task["payload"]["definition"]["lookback_months"] for task in tasks} == {None}
    assert {
        task["payload"]["evidence_plan"]["lake_manifest_sha256"] for task in tasks
    } == {"sha256:" + "b" * 64}
    first_spec = lane.task_specs[tasks[0]["task_id"]]
    assert first_spec["permutation_budget_applied"] is True
    assert first_spec["expanded_permutation_count"] == 16


def test_rank_sweep_permutations_accepts_compact_summary_results() -> None:
    payload = lab._rank_sweep_permutations(
        phase="coarse_probe",
        shard_results=[
            {
                "permutation_results": [
                    {
                        "permutation_index": 0,
                        "child_job_id": "child-0",
                        "status": "success",
                        "parameters": {"alpha": 1},
                        "result": {
                            "result_detail": "summary",
                            "aggregate": {"score_lab": {"score": 12.5}},
                            "full_result_omitted": True,
                        },
                    },
                    {
                        "permutation_index": 1,
                        "child_job_id": "child-1",
                        "status": "success",
                        "parameters": {"alpha": 2},
                        "fitness": {"score_lab": 9.0},
                    },
                ]
            }
        ],
    )

    assert payload["best"]["child_job_id"] == "child-0"
    assert payload["best"]["score"] == 12.5
    assert [item["score"] for item in payload["ranked"]] == [12.5, 9.0]


class _IndicatorIndexCli:
    def __init__(self, ids: list[str]):
        self.ids = ids

    def run(self, args, **_kwargs):
        assert args == ["indicators", "--mode", "index"]
        return SimpleNamespace(parsed_json={"data": {"ids": self.ids}})


def test_seed_indicators_filter_unscaffoldable_seed_plan_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "SPEARMAN_RANK_CORRELATION",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {
                    "trigger": [
                        {"indicator_id": "TTF_DSL_TRANSITION", "sampling_weight": 1.0},
                        {"indicator_id": "ADX", "sampling_weight": 1.0},
                    ]
                },
            }
        },
    }
    config = _test_config(tmp_path)
    monkeypatch.setattr(
        lab,
        "_load_play_hand_seed_plan",
        lambda _config, _seed_plan_path=None: (seed_plan, tmp_path / "seed-plan.json"),
    )

    indicators, loaded_seed_plan, _seed_plan_path = lab._seed_indicators(
        config=config,
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(min_indicators=2, max_indicators=2),
    )

    assert [indicator.id for indicator in indicators] == ["RSI", "ADX"]
    deal = lab._deal_lane(
        config=config,
        runtime=lab.PlayHandLabRuntimeConfig(
            min_indicators=2,
            max_indicators=2,
            instrument=["EURUSD"],
        ),
        seed_indicators=indicators,
        seed_plan=loaded_seed_plan,
        rng=random.Random(4),
    )

    assert set(deal["dealt"]) <= {"RSI", "ADX"}
    assert "SPEARMAN_RANK_CORRELATION" not in deal["dealt"]
    assert "TTF_DSL_TRANSITION" not in deal["dealt"]


def test_seed_indicators_uses_runtime_seed_plan_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    expected_path = tmp_path / "isolated" / "play-hand-seed-plan.json"
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "pair": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [
                    {
                        "anchor_id": "RSI",
                        "trigger_id": "ADX",
                        "pair_sampling_weight": 1.0,
                    }
                ],
                "slot_menus": {},
            }
        },
    }
    seen_paths: list[Path | None] = []

    def fake_load_seed_plan(_config, seed_plan_path=None):
        seen_paths.append(seed_plan_path)
        return seed_plan, expected_path

    monkeypatch.setattr(lab, "_load_play_hand_seed_plan", fake_load_seed_plan)

    indicators, loaded_seed_plan, loaded_seed_plan_path = lab._seed_indicators(
        config=_test_config(tmp_path),
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(
            seed_plan_path=expected_path,
            min_indicators=2,
            max_indicators=2,
        ),
    )

    assert seen_paths == [expected_path]
    assert loaded_seed_plan is seed_plan
    assert loaded_seed_plan_path == expected_path
    assert [indicator.id for indicator in indicators] == ["RSI", "ADX"]


def test_indicator_deal_metadata_is_json_safe_and_health_compatible() -> None:
    metadata = lab._indicator_deal_metadata(
        {
            "source": "play_hand_seed_plan",
            "reason": None,
            "recipe": "discovered_recipe_012",
            "recipe_source": "discovery_recipe_validation",
            "recipe_confidence": "high_candidate",
            "guided_recipe_source_bucket": "discovery_recipe_validation",
            "guided_recipe_source_bucket_matched": True,
            "guided_recipe_source_bucket_fallback": False,
            "indicators": [
                lab.SeedIndicator("MOM_MEAN_REVERSION"),
                {"indicator_id": "MFI_TREND"},
            ],
            "pair": {
                "anchor_id": "MOM_MEAN_REVERSION",
                "trigger_id": "MFI_TREND",
                "horizon_stability_bucket": "retained_36m",
            },
            "family_policy": {"family_policy": "template_guarded"},
            "policy_target_count": 2,
            "selected_slots": ["pair_menu"],
        }
    )

    assert metadata["indicator_deal"]["indicator_ids"] == ["MOM_MEAN_REVERSION", "MFI_TREND"]
    assert metadata["dealt_indicator_source"] == "play_hand_seed_plan"
    assert metadata["dealt_recipe"] == "discovered_recipe_012"
    assert metadata["dealt_recipe_source"] == "discovery_recipe_validation"
    assert metadata["dealt_recipe_pair"]["horizon_stability_bucket"] == "retained_36m"
    json.dumps(metadata)


def test_seed_indicators_reject_unscaffoldable_pinned_ids(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not scaffoldable"):
        lab._seed_indicators(
            config=_test_config(tmp_path),
            cli=_IndicatorIndexCli(["RSI", "ADX"]),
            campaign_ctx=_campaign_ctx(tmp_path),
            runtime=lab.PlayHandLabRuntimeConfig(
                indicator=["RSI", "SPEARMAN_RANK_CORRELATION"],
                min_indicators=2,
                max_indicators=2,
            ),
        )


def test_historical_seed_indicators_reject_undersized_plan_without_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    undersized_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "single": {
                "recipe_sampling_weight": 1.0,
                "pair_menu": [{"anchor_id": "RSI", "pair_sampling_weight": 1.0}],
                "slot_menus": {},
            }
        },
    }
    seed_plan_path = _write_historical_seed_plan(tmp_path, undersized_plan)
    runtime = _level_c_runtime(
        tmp_path,
        seed_plan_path=seed_plan_path,
        expected_seed_plan_sha256=lab._file_sha256(seed_plan_path),
        min_indicators=2,
        max_indicators=2,
    )

    def unexpected_seed_hand(*_args, **_kwargs):
        raise AssertionError("historical seed selection must not call _seed_hand")

    monkeypatch.setattr(lab, "_seed_hand", unexpected_seed_hand)
    with pytest.raises(RuntimeError, match="smaller than --min-indicators"):
        lab._seed_indicators(
            config=_test_config(tmp_path),
            cli=_IndicatorIndexCli(["RSI", "ADX", "MACD", "SMA"]),
            campaign_ctx=_campaign_ctx(tmp_path),
            runtime=runtime,
        )


def test_historical_lane_deal_uses_seed_plan_when_atlas_fraction_allows_exploration(
    tmp_path: Path,
) -> None:
    fallback_plan = _historical_seed_plan()
    fallback_plan["sampling_policy"] = {"guided_prior_fraction": 0.0}
    runtime = _level_c_runtime(
        tmp_path,
        seed_plan_payload=fallback_plan,
        min_indicators=1,
        max_indicators=1,
    )

    indicators, seed_plan, seed_plan_path = lab._seed_indicators(
        config=_test_config(tmp_path),
        cli=_IndicatorIndexCli(["RSI", "ADX"]),
        campaign_ctx=_campaign_ctx(tmp_path),
        runtime=runtime,
    )

    deal = lab._deal_lane(
        config=_test_config(tmp_path),
        runtime=runtime,
        seed_indicators=indicators,
        seed_plan=seed_plan,
        rng=random.Random(7),
    )

    assert seed_plan_path == runtime.seed_plan_path
    assert deal["indicator_deal"]["source"] == "play_hand_seed_plan"


def test_historical_lane_deal_rejects_role_balanced_fill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _level_c_runtime(tmp_path, min_indicators=1, max_indicators=1)
    monkeypatch.setattr(
        lab,
        "deal_seed_plan_indicators",
        lambda *_args, **_kwargs: {
            "source": "play_hand_seed_plan",
            "selected_slots": ["role_balanced_fill"],
            "indicators": [lab.SeedIndicator("RSI")],
        },
    )

    with pytest.raises(RuntimeError, match="rejects fallback indicator deals"):
        lab._deal_lane(
            config=_test_config(tmp_path),
            runtime=runtime,
            seed_indicators=[lab.SeedIndicator("RSI")],
            seed_plan=_historical_seed_plan(),
            rng=random.Random(7),
        )


def test_deep_replay_dry_run_uses_real_scaffold_for_profile_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = SimpleNamespace(
        config=_test_config(tmp_path),
        dry_run=True,
        profiles_dir=tmp_path / "profiles",
        events_path=tmp_path / "events.jsonl",
        run_id="lane-run",
        io_lock=threading.RLock(),
    )
    ctx.profiles_dir.mkdir()
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-run",
        run_dir=tmp_path / "lane-run",
    )

    def fake_scaffold_profile(scaffold_ctx, indicator_ids, instruments, timeframe, candidate_name):
        assert scaffold_ctx.dry_run is False
        assert indicator_ids == ["RSI"]
        assert instruments == ["EURUSD"]
        assert timeframe == "M5"
        profile_path = ctx.profiles_dir / f"{candidate_name}.json"
        profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
        return profile_path

    monkeypatch.setattr(lab, "_scaffold_profile", fake_scaffold_profile)
    monkeypatch.setattr(
        lab,
        "_worker_ready_profile_snapshot",
        lambda profile_payload, **_kwargs: profile_payload,
    )

    lab._prepare_lane_profile(
        ctx,
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="deep_replay", dry_run=True),
        lane=lane,
        seed_plan=None,
        deal={
            "dealt": ["RSI"],
            "dealt_entries": [lab.SeedIndicator("RSI")],
            "indicator_deal": {},
            "instruments": ["EURUSD"],
        },
        rng=random.Random(1),
    )

    assert lane.profile_path == ctx.profiles_dir / "lane_000_base.json"
    assert lane.indicator_ids == ["RSI"]


def test_deep_replay_tasks_are_self_contained_and_contract_pinned(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "runs" / "run-1",
        profile_path=profile_path,
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        tasks_per_lane=1,
        bar_limit=250,
        worker_contract_hash="sha256:" + "a" * 64,
        seed=123,
    )
    lab._sample_lane_screen_anchor(lane, runtime)

    tasks = lab._build_tasks(
        [lane],
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )

    assert len(tasks) == 1
    task = tasks[0]
    payload = task["payload"]
    assert task["task_kind"] == "deep_replay"
    assert payload["job_id"] == task["task_id"]
    assert payload["required_worker_contract_hash"] == runtime.worker_contract_hash
    assert payload["required_worker_contract_schema"] == "replay-worker-contract-v1"
    assert payload["required_capabilities"] == ["deep_replay"]
    assert task["required_worker_capabilities"] == [
        "deep_replay",
        lab.PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    ]
    assert payload["bar_limit"] == 250
    assert payload["inline_profile_snapshot"]["name"] == "Lab Smoke"
    assert payload["instruments"] == ["EURUSD"]
    assert payload["market_data_source"] == "lake_bars"
    assert payload["analysis_window_start"]
    assert payload["analysis_window_end"]
    assert payload["analysis_window_start"].endswith("Z")
    assert payload["analysis_window_end"].endswith("Z")
    assert payload["lookback_months"] is None
    assert payload["evidence_plan"]["evidence_role"] == "training"
    assert payload["evidence_plan"]["profile_snapshot_sha256"].startswith(
        "sha256:"
    )
    assert lane.screen_anchor_mode == "random"
    assert lane.screen_anchor_offset_days is not None
    assert lane.task_specs[task["task_id"]]["analysis_window_start"] == payload["analysis_window_start"]
    assert lane.task_specs[task["task_id"]]["analysis_window_end"] == payload["analysis_window_end"]


def test_fixed_as_of_date_bounds_screen_validation_and_scrutiny(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
        profile_path=tmp_path / "profile.json",
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        incumbent_profile_path=tmp_path / "profile.json",
        incumbent_profile_payload=_profile_payload()["profile"],
        incumbent_profile_ref="lab-inline:run-1:lane_000",
        incumbent_instruments=["EURUSD"],
        incumbent_timeframe="M5",
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        as_of_date="2025-06-30T00:00:00Z",
        lookback_months=3,
        validation_months=12,
        scrutiny_months=36,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    lab._sample_lane_screen_anchor(lane, runtime)

    validation = lab._enqueue_validation_stage(
        lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )[0]["payload"]
    scrutiny = lab._enqueue_final_stage(
        lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
    )[0]["payload"]

    assert lane.screen_anchor_mode == "fixed_as_of"
    assert lane.screen_analysis_window_end == "2025-06-30T00:00:00Z"
    assert validation["analysis_window_end"] == "2025-06-30T00:00:00Z"
    assert validation["analysis_window_start"] == "2024-06-30T00:00:00Z"
    assert scrutiny["analysis_window_end"] == "2025-06-30T00:00:00Z"
    assert scrutiny["analysis_window_start"] == "2022-06-30T00:00:00Z"
    assert validation["evidence_plan"]["selection_data_end"] == validation["analysis_window_end"]
    assert scrutiny["evidence_plan"]["selection_data_end"] == scrutiny["analysis_window_end"]
    assert validation["lookback_months"] is None
    assert scrutiny["lookback_months"] is None
    assert validation["evidence_plan"]["data_availability_cutoff"] == runtime.as_of_date
    assert scrutiny["evidence_plan"]["data_availability_cutoff"] == runtime.as_of_date
    assert scrutiny["evidence_plan"]["evidence_role"] == "training"


def test_historical_replay_and_sweep_tasks_require_explicit_bounds_and_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    profile_payload = _profile_payload()["profile"]
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "run-1",
        profile_path=profile_path,
        profile_payload=profile_payload,
        profile_ref="lab-inline:run-1:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
    )
    runtime = lab.PlayHandLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        lake_manifest_sha256="sha256:" + "b" * 64,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    axis_texts = ["indicator[0].talib.timeperiod=7,14"]
    axis_plan = SimpleNamespace(
        axes=axis_texts,
        selected_permutations=2,
        event_payload=lambda: {"selected_axes": axis_texts},
    )
    monkeypatch.setattr(lab, "plan_sweep_axes", lambda *args, **kwargs: axis_plan)

    with pytest.raises(ValueError, match="require explicit analysis window bounds"):
        lab._deep_replay_job_payload(
            task_id="missing-bounds",
            lane=lane,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
        )
    with pytest.raises(ValueError, match="require explicit analysis window bounds"):
        lab._sweep_definition_payload(
            lane=lane,
            runtime=runtime,
            reward_matrix=None,
            axes=[],
            instruments=["EURUSD"],
            profile_ref=lane.profile_ref,
            profile_payload=profile_payload,
            lookback_months=3,
            analysis_window_start=None,
            analysis_window_end=None,
            mode="deterministic",
        )

    def assert_historical_evidence(payload: dict) -> None:
        evidence_plan = payload["evidence_plan"]
        assert payload["analysis_window_start"]
        assert payload["analysis_window_end"] == runtime.as_of_date
        assert payload["lookback_months"] is None
        assert evidence_plan["evidence_role"] == "training"
        assert evidence_plan["selection_data_end"] == runtime.as_of_date
        assert evidence_plan["data_availability_cutoff"] == runtime.as_of_date

    for phase, months in [
        ("baseline_3mo", 3),
        ("instrument_scout_EURUSD_12mo", 12),
        ("validation_12mo", 12),
        ("final_36mo", 36),
    ]:
        start, end = lab._runtime_as_of_window(runtime, months)
        task = lab._make_deep_replay_task(
            lane,
            phase=phase,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
            profile_payload=profile_payload,
            profile_path=profile_path,
            profile_ref=lane.profile_ref,
            instruments=["EURUSD"],
            timeframe="M5",
            lookback_months=months,
            analysis_window_start=start,
            analysis_window_end=end,
        )
        assert_historical_evidence(task["payload"])

    for phase in ["lookback_timing", "coarse_probe", "coarse_expand", "focused"]:
        start, end = lab._runtime_as_of_window(runtime, 3)
        tasks = lab._make_sweep_shard_tasks(
            lane,
            phase=phase,
            runtime=runtime,
            reward_matrix=None,
            worker_contract_hash=runtime.worker_contract_hash,
            profile_payload=profile_payload,
            profile_path=profile_path,
            profile_ref=lane.profile_ref,
            instruments=["EURUSD"],
            lookback_months=3,
            axis_texts=axis_texts,
            mode="deterministic",
            analysis_window_start=start,
            analysis_window_end=end,
        )
        assert tasks
        for task in tasks:
            assert_historical_evidence(task["payload"]["definition"])


def test_historical_execution_receipt_must_match_plan() -> None:
    plan = {
        "plan_id": "sha256:" + "a" * 64,
        "profile_snapshot_sha256": "sha256:" + "b" * 64,
        "execution_cell_sha256": None,
        "lake_manifest_sha256": "sha256:" + "c" * 64,
    }
    receipt = {
        "plan_id": plan["plan_id"],
        "profile_snapshot_sha256": plan["profile_snapshot_sha256"],
        "execution_cell_sha256": None,
        "observed_lake_manifest_sha256": plan["lake_manifest_sha256"],
    }

    assert lab._validated_execution_evidence(
        {"execution_evidence": receipt}, plan
    ) == receipt
    with pytest.raises(RuntimeError, match="omitted execution_evidence"):
        lab._validated_execution_evidence({}, plan)


def test_fake_compute_tasks_require_lab_protocol_capability(tmp_path: Path) -> None:
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="run-1",
        run_dir=tmp_path / "runs" / "run-1",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
    )

    tasks = lab._build_tasks(
        [lane],
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="fake_compute", tasks_per_lane=1),
        reward_matrix=None,
    )

    payload = tasks[0]["payload"]
    assert payload["required_capabilities"] == [
        lab.PLAY_HAND_LAB_FAKE_COMPUTE_CAPABILITY,
        lab.PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    ]


def test_play_hand_lab_validation_and_final_score_gates() -> None:
    runtime = lab.PlayHandLabRuntimeConfig(validation_min_score=45.0, final_min_score=40.0)

    validation = lab._validation_outcome(44.9, runtime)
    final = lab._lab_final_scrutiny_outcome(0.1, runtime)

    assert validation["passed"] is False
    assert validation["reason"] == "validation_score_below_45"
    assert "validation_12mo_failed" in validation["reasons"]
    assert lab._validation_outcome(45.0, runtime)["passed"] is True
    assert final["passed"] is False
    assert final["reason"] == "final_36mo_score_below_40"
    assert lab.PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON in final["reasons"]
    assert lab._lab_final_scrutiny_outcome(40.0, runtime)["passed"] is True


def test_play_hand_lab_validation_failure_tombstones_before_final(tmp_path: Path) -> None:
    fake_config = _test_config(tmp_path)
    lane_dir = fake_config.runs_root / "lane-validation-fail"
    lane_dir.mkdir(parents=True)
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    lane_ctx = _campaign_ctx(lane_dir)
    lane_ctx.attempts_path = lane_dir / "attempts.jsonl"
    runtime = lab.PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        pipeline_mode="play_hand",
        validation_min_score=45.0,
        worker_contract_hash="sha256:" + "a" * 64,
    )
    phase = lab._validation_phase(runtime)
    task_id = "lane-validation-fail-task-00001-validation_12mo"
    lane = lab.LabLaneState(
        lane_id="lane_000",
        lane_index=0,
        run_id="lane-validation-fail",
        run_dir=lane_dir,
        profile_path=profile_path,
        profile_payload=_profile_payload()["profile"],
        profile_ref="lab-inline:lane-validation-fail:lane_000",
        instruments=["EURUSD"],
        timeframe="M5",
        indicator_ids=["RSI"],
        incumbent_profile_path=profile_path,
        incumbent_profile_payload=_profile_payload()["profile"],
        incumbent_profile_ref="focused_top_3mo",
        incumbent_instruments=["EURUSD"],
        current_phase="validation",
    )
    lane.task_ids.append(task_id)
    lane.completed_task_ids.add(task_id)
    lane.phase_task_ids[phase] = [task_id]
    lab.write_run_metadata(lane_dir, {"run_status": "running"})

    follow_up = lab._advance_lane_after_result(
        config=fake_config,
        lane_ctx=lane_ctx,
        lane=lane,
        runtime=runtime,
        reward_matrix=None,
        worker_contract_hash=runtime.worker_contract_hash,
        recorded={"phase": phase, "score": 44.0, "status": "success"},
    )

    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    assert follow_up == []
    assert lane.terminal is True
    assert lane.tombstone_reason == "validation_score_below_45"
    assert "validation_12mo_failed" in lane.tombstone_reasons
    assert metadata["run_status"] == "tombstoned"
    assert metadata["final_scrutiny_passed"] is False
    assert "final_36mo" not in lane.phase_task_ids


def test_deep_replay_rejects_duplicate_tasks_per_lane() -> None:
    with pytest.raises(ValueError, match="tasks-per-lane 1"):
        lab._normalize_runtime(
            lab.PlayHandLabRuntimeConfig(task_mode="deep_replay", tasks_per_lane=2)
        )


def test_worker_ready_profile_snapshot_converts_stored_profile(tmp_path: Path, monkeypatch) -> None:
    class FakeScoringProfile:
        @classmethod
        def model_validate(cls, _payload):
            raise ValueError("not full")

    class FakeFullProfile:
        def model_dump(self, *, mode: str):
            assert mode == "json"
            return {
                "name": "Lab Smoke",
                "description": "Test profile",
                "instruments": ["EURUSD"],
                "isActive": False,
                "notificationThreshold": 80,
                "directionMode": "both",
                "version": "v1",
                "indicators": [
                    {
                        "meta": {
                            "id": "RSI",
                            "instanceId": "test-rsi",
                            "name": "Relative Strength Index",
                            "namespace": "TA-Lib",
                            "talibFunction": "RSI",
                            "supportsTradingMode": True,
                            "usesRangeConfiguration": True,
                            "description": "RSI",
                            "inputs": [],
                            "valueRange": {"min": 0, "max": 100},
                        },
                        "config": {"timeframe": "M5"},
                    }
                ],
            }

    class FakeStoredProfile:
        @classmethod
        def model_validate(cls, payload):
            assert payload["indicators"][0]["meta"]["id"] == "RSI"
            return cls()

        def to_full_profile(self):
            return FakeFullProfile()

    monkeypatch.setattr(
        lab,
        "_load_fuzzfolio_profile_models",
        lambda **_kwargs: (FakeScoringProfile, FakeStoredProfile),
    )

    snapshot = lab._worker_ready_profile_snapshot(
        _profile_payload(),
        config=_test_config(tmp_path),
        runtime=lab.PlayHandLabRuntimeConfig(task_mode="deep_replay"),
    )

    assert snapshot["indicators"][0]["meta"]["name"] == "Relative Strength Index"


def test_play_hand_lab_fake_compute_writes_lane_attempts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        tasks: list[dict] = []
        results: list[dict] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def drain_results(self, *, limit: int) -> list[dict]:
            drained = self.results[:limit]
            self.results = self.results[limit:]
            return drained

        def snapshot(self) -> dict:
            return {"ok": True, "completed_tasks": len(self.tasks), "queued_tasks": 0}

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=2,
            tasks_per_lane=2,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.1,
            max_wait_seconds=5.0,
        )
    )

    assert exit_code == 0
    campaign_dirs = list(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dirs = sorted(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    assert len(campaign_dirs) == 1
    assert len(lane_dirs) == 2

    summary = json.loads(
        (campaign_dirs[0] / "play-hand-lab-campaign-summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["status"] == "completed"
    assert summary["total_tasks"] == 4
    assert summary["completed_tasks"] == 4
    assert summary["generated_by_runner"] == lab.PLAY_HAND_LAB_RUNNER

    for lane_dir in lane_dirs:
        metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
        attempts = [
            json.loads(line)
            for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        assert metadata["generated_by_runner"] == lab.PLAY_HAND_LAB_RUNNER
        assert metadata["run_kind"] == "play_hand_lab_lane"
        assert metadata["completed_task_count"] == 2
        assert len(attempts) == 2
        assert {attempt["generated_by_runner"] for attempt in attempts} == {lab.PLAY_HAND_LAB_RUNNER}
        assert {attempt["attempt_role"] for attempt in attempts} == {"lab_smoke"}


def test_play_hand_lab_burst_drains_full_batches_and_coalesces_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)
    render_calls: list[Path] = []

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        read_limits: list[int] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            type(self).read_limits.append(limit)
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "gateway_id": "stable",
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.read_limits = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(
        lab,
        "render_progress_artifacts",
        lambda _attempts, output_path, **_kwargs: render_calls.append(output_path),
    )

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=4,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            result_batch_size=2,
            max_results_per_cycle=4,
            max_drain_seconds=60.0,
            poll_interval_seconds=5.0,
            max_wait_seconds=5.0,
        )
    )

    assert exit_code == 0
    assert FakeGateway.read_limits[:2] == [2, 2]
    assert len(render_calls) == 1


def test_play_hand_lab_retries_transient_result_read_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        read_calls = 0

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            type(self).read_calls += 1
            if type(self).read_calls == 1:
                raise requests.ConnectTimeout("temporary gateway accept stall")
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.read_calls = 0
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.1,
            max_wait_seconds=2.0,
            result_read_failure_limit=3,
        )
    )

    assert exit_code == 0
    assert FakeGateway.read_calls >= 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "completed"
    assert summary["completed_tasks"] == 1
    assert any(
        event["phase"] == "gateway"
        and event["status"] == "result_read_failed"
        and event["consecutive_failures"] == 1
        for event in events
    )


def test_play_hand_lab_rolls_finite_runs_with_active_run_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        enqueue_batches: list[int] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.enqueue_batches.append(len(tasks))
            start = len(self.tasks)
            self.tasks.extend(tasks)
            self.results.extend(
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{start + index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            )
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:1]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    FakeGateway.enqueue_batches = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            target_runs=5,
            active_runs=2,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    assert FakeGateway.enqueue_batches[0] == 2
    assert max(FakeGateway.enqueue_batches) <= 2
    assert sum(FakeGateway.enqueue_batches) == 5

    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["campaign_mode"] == "finite"
    assert summary["target_runs"] == 5
    assert summary["active_runs"] == 2
    assert summary["lane_count"] == 5
    assert summary["total_tasks"] == 5
    assert summary["completed_tasks"] == 5


def test_play_hand_lab_refreshes_gateway_snapshot_after_final_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.drain_calls = 0
            self.completed = False
            self.historical_completed = 100
            self.historical_results_dropped = 5

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            return {"enqueued": len(tasks)}

        def drain_results(self, *, limit: int) -> list[dict]:
            self.drain_calls += 1
            if self.drain_calls == 1 or self.completed:
                return []
            self.completed = True
            task = self.tasks[0]
            return [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
            ]

        def snapshot(self) -> dict:
            campaign_completed = len(self.tasks) if self.completed else 0
            return {
                "ok": True,
                "completed_tasks": self.historical_completed + campaign_completed,
                "queued_tasks": 0 if self.completed else len(self.tasks),
                "metrics": {
                    "completions_accepted": self.historical_completed + campaign_completed,
                    "results_dropped": self.historical_results_dropped,
                },
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    assert summary["completed_tasks"] == 1
    assert summary["gateway_snapshot"]["completed_tasks"] == 1
    assert summary["gateway_snapshot"]["raw_completed_tasks"] == 101
    assert summary["gateway_snapshot"]["metrics"]["results_dropped"] == 0
    assert summary["gateway_snapshot"]["raw_metrics"]["results_dropped"] == 5
    assert summary["gateway_snapshot"]["queued_tasks"] == 0


def test_play_hand_lab_records_terminal_worker_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "failed",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "status": "failed",
                        "error": "simulated worker failure",
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": 0,
                "failed_tasks": len(self.tasks),
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["status"] == "failed"
    assert summary["completed_tasks"] == 0
    assert summary["failed_tasks"] == 1
    assert metadata["run_status"] == "failed"
    assert metadata["failed_task_count"] == 1
    assert attempts[0]["run_status"] == "failed"
    assert attempts[0]["score_basis"] == "lab_worker_failed"


def test_play_hand_lab_scoring_warning_fails_deep_replay_task(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {"matrix": {"ok": True}},
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "failed_tasks": 0,
                "queued_tasks": 0,
                "metrics": {},
            }

    def fake_prepare_lane_profile(lane_ctx, *, runtime, lane, seed_plan, deal, rng) -> None:
        profile_path = lane_ctx.profiles_dir / "profile.json"
        profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
        lane.profile_path = profile_path
        lane.profile_payload = _profile_payload()["profile"]
        lane.profile_ref = f"lab-inline:{lane.run_id}:{lane.lane_id}"
        lane.instruments = ["EURUSD"]
        lane.timeframe = "M5"
        lane.indicator_ids = ["RSI"]

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        return (
            lab.AttemptScore(
                primary_score=None,
                composite_score=None,
                score_basis="lab_scoring_failed",
                metrics={},
                best_summary={"error": "simulated scoring failure"},
            ),
            {"error": "simulated scoring failure", "error_type": "RuntimeError"},
        )

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_seed_indicators", lambda **_kwargs: (["RSI"], None, None))
    monkeypatch.setattr(lab, "_deal_lane", lambda **_kwargs: object())
    monkeypatch.setattr(lab, "_prepare_lane_profile", fake_prepare_lane_profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="deep_replay",
            lanes=1,
            tasks_per_lane=1,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
            worker_contract_hash="sha256:" + "a" * 64,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["status"] == "failed"
    assert summary["completed_tasks"] == 0
    assert summary["failed_tasks"] == 1
    assert attempts[0]["run_status"] == "failed"
    assert attempts[0]["score_basis"] == "lab_scoring_failed"
    assert attempts[0]["lab_scoring_warning"]["error"] == "simulated scoring failure"


def test_play_hand_lab_ack_failure_does_not_turn_success_into_failed_attempt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            task = self.tasks[0]
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": "lease-1",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            raise RuntimeError("transient ack failure")

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "failed_tasks": 0,
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "completed"
    assert summary["completed_tasks"] == 1
    assert summary["failed_tasks"] == 0
    assert len(attempts) == 1
    assert attempts[0]["run_status"] == "screened"
    assert any(event["phase"] == "result_ack" and event["status"] == "failed" for event in events)


def test_play_hand_lab_summary_keeps_bounded_recorded_result_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    ack_calls: list[list[str]] = []

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            self.results = [
                {
                    "task_id": task["task_id"],
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "worker_id": "fake-worker",
                    "lease_id": f"lease-{index}",
                    "result": {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "task_id": task["task_id"],
                            "lane_id": task["lane_id"],
                            "attempt_id": task["attempt_id"],
                            "task_kind": "fake_compute",
                            "work_seconds": task["payload"]["work_seconds"],
                        },
                    },
                }
                for index, task in enumerate(tasks)
            ]
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            ack_calls.append(list(lease_ids))
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [
                result for result in self.results if result.get("lease_id") not in requested
            ]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks),
                "queued_tasks": 0,
                "metrics": {},
            }

    monkeypatch.setattr(lab, "SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT", 2)
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=3,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 0
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert summary["recorded_result_count"] == 3
    assert summary["recorded_results_sample_limit"] == 2
    assert summary["recorded_results_truncated"] is True
    assert len(summary["recorded_results"]) == 2
    assert len(attempts) == 3
    assert ack_calls == [["lease-0", "lease-1", "lease-2"]]


def test_play_hand_lab_fails_fast_when_gateway_result_read_dies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.tasks = list(tasks)
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            raise requests.ConnectionError("gateway is gone")

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": 0,
                "queued_tasks": len(self.tasks),
                "metrics": {},
            }

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="fake_compute",
            lanes=1,
            tasks_per_lane=1,
            indicator=["RSI"],
            profile_path=profile_path,
            fake_work_seconds=0.0,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
        )
    )

    assert exit_code == 2
    campaign_dir = next(
        (fake_config.runs_root / "derived" / lab.PLAY_HAND_LAB_CAMPAIGNS_DIR).glob(
            "*-playhand-lab-campaign-v1"
        )
    )
    summary = json.loads(
        (campaign_dir / "play-hand-lab-campaign-summary.json").read_text(encoding="utf-8")
    )
    events = [
        json.loads(line)
        for line in (campaign_dir / "play-hand-lab-campaign-events.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    assert summary["status"] == "gateway_unreachable"
    assert summary["recorded_result_count"] == 0
    assert any(event["phase"] == "gateway" and event["status"] == "result_read_failed" for event in events)


def test_play_hand_lab_pipeline_early_exits_after_bad_baseline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            start = len(self.tasks)
            self.tasks.extend(tasks)
            for index, task in enumerate(tasks):
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lane_id": task["lane_id"],
                        "attempt_id": task["attempt_id"],
                        "status": "success",
                        "worker_id": "fake-worker",
                        "lease_id": f"lease-{start + index}",
                        "result": {
                            "job_id": task["task_id"],
                            "status": "success",
                            "result": {"aggregate": {"score_lab": {"score": -1.0}}},
                        },
                    }
                )
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [result for result in self.results if result.get("lease_id") not in requested]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        return (
            lab.AttemptScore(
                primary_score=-1.0,
                composite_score=-1.0,
                score_basis="test",
                metrics={"score_lab": -1.0},
                best_summary={"score_lab": {"score": -1.0}},
            ),
            None,
        )

    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_worker_ready_profile_snapshot", lambda profile, **_kwargs: profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="deep_replay",
            pipeline_mode="play_hand",
            target_runs=1,
            active_runs=1,
            profile_path=profile_path,
            poll_interval_seconds=0.01,
            max_wait_seconds=2.0,
            worker_contract_hash="sha256:" + "a" * 64,
        )
    )

    assert exit_code == 0
    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert metadata["run_status"] == "tombstoned"
    assert metadata["tombstone_reason"] == lab.PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON
    assert metadata["completed_task_count"] == 1
    assert metadata["play_hand_phase_scores"]["baseline"] == -1.0
    assert len(attempts) == 1


def test_play_hand_lab_pipeline_promotes_good_lane_with_sweep_shards(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile_payload()), encoding="utf-8")
    fake_config = _test_config(tmp_path)

    class FakeCli:
        def __init__(self, _config):
            self.config = _config

    class FakeGateway:
        enqueue_batches: list[list[str]] = []

        def __init__(self, *, base_url: str, token: str | None = None):
            self.base_url = base_url
            self.token = token
            self.tasks: list[dict] = []
            self.results: list[dict] = []

        def health(self) -> dict:
            return {"ok": True}

        def _score_for_task(self, task: dict, offset: int = 0) -> float:
            task_id = str(task["task_id"])
            if "baseline_3mo" in task_id:
                return 60.0
            if "lookback_timing" in task_id:
                return 62.0 + offset
            if "coarse_probe" in task_id:
                return 66.0 + offset
            if "coarse_expand" in task_id:
                return 68.0 + offset
            if "focused" in task_id:
                return 70.0 + offset
            if "validation_12mo" in task_id:
                return 65.0
            if "instrument_scout" in task_id:
                return 58.0 + offset
            if "final_36mo" in task_id:
                return 72.0
            return 55.0

        def enqueue_tasks(self, tasks: list[dict]) -> dict:
            self.enqueue_batches.append([str(task.get("task_kind")) for task in tasks])
            start = len(self.tasks)
            self.tasks.extend(tasks)
            for index, task in enumerate(tasks):
                task_kind = str(task.get("task_kind"))
                if task_kind == "sweep_shard":
                    params_by_index = dict((task.get("payload") or {}).get("params_by_index") or {})
                    permutation_results = []
                    for raw_index, params in params_by_index.items():
                        permutation_index = int(raw_index)
                        score = self._score_for_task(task, offset=permutation_index % 3)
                        permutation_results.append(
                            {
                                "permutation_index": permutation_index,
                                "child_job_id": f"{task['task_id']}-{permutation_index}",
                                "status": "success",
                                "parameters": dict(params),
                                "result": {"aggregate": {"score_lab": {"score": score}}},
                                "completed_at": "2026-06-20T00:00:00Z",
                            }
                        )
                    worker_result = {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {
                            "shard_id": (task.get("payload") or {}).get("shard_id"),
                            "sweep_id": (task.get("payload") or {}).get("sweep_id"),
                            "status": "success",
                            "started_at": "2026-06-20T00:00:00Z",
                            "completed_at": "2026-06-20T00:00:01Z",
                            "permutation_results": permutation_results,
                            "failed_permutations": [],
                            "worker_attribution": {},
                        },
                    }
                else:
                    score = self._score_for_task(task)
                    worker_result = {
                        "job_id": task["task_id"],
                        "status": "success",
                        "result": {"aggregate": {"score_lab": {"score": score}}},
                    }
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lane_id": task["lane_id"],
                        "attempt_id": task["attempt_id"],
                        "status": "success",
                        "worker_id": "fake-worker",
                        "lease_id": f"lease-{start + index}",
                        "result": worker_result,
                    }
                )
            return {"enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict]:
            return self.results[:limit]

        def ack_results(self, lease_ids: list[str]) -> int:
            requested = set(lease_ids)
            before = len(self.results)
            self.results = [result for result in self.results if result.get("lease_id") not in requested]
            return before - len(self.results)

        def snapshot(self) -> dict:
            return {
                "ok": True,
                "completed_tasks": len(self.tasks) - len(self.results),
                "queued_tasks": len(self.results),
                "metrics": {},
            }

    def fake_score_lab_artifact(*, cli, artifact_dir, strict):
        path = str(artifact_dir)
        score = 60.0
        if "validation_12mo" in path:
            score = 65.0
        if "instrument_scout" in path:
            score = 58.0
        if "final_36mo" in path:
            score = 72.0
        return (
            lab.AttemptScore(
                primary_score=score,
                composite_score=score,
                score_basis="test",
                metrics={"score_lab": score},
                best_summary={"score_lab": {"score": score}},
            ),
            None,
        )

    FakeGateway.enqueue_batches = []
    monkeypatch.setattr(lab, "load_config", lambda: fake_config)
    monkeypatch.setattr(lab, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(lab, "LabGatewayClient", FakeGateway)
    monkeypatch.setattr(lab, "_worker_ready_profile_snapshot", lambda profile, **_kwargs: profile)
    monkeypatch.setattr(lab, "_score_lab_artifact", fake_score_lab_artifact)

    exit_code = lab.cmd_play_hand_lab(
        lab.PlayHandLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            task_mode="deep_replay",
            pipeline_mode="play_hand",
            target_runs=1,
            active_runs=1,
            profile_path=profile_path,
            max_sweep_permutations=4,
            coarse_probe_budget=2,
            sweep_shard_size=2,
            instrument_scout_size=1,
            instrument_scout_max_selected=1,
            poll_interval_seconds=0.01,
            max_wait_seconds=5.0,
            worker_contract_hash="sha256:" + "a" * 64,
        )
    )

    assert exit_code == 0
    assert any("sweep_shard" in batch for batch in FakeGateway.enqueue_batches)

    lane_dir = next(fake_config.runs_root.glob("*-playhand-lab-lane-*-v1"))
    metadata = json.loads((lane_dir / "run-metadata.json").read_text(encoding="utf-8"))
    attempts = [
        json.loads(line)
        for line in (lane_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert metadata["run_status"] == "promoted"
    assert metadata["validation_months"] == 12
    assert metadata["validation_min_score"] == 45.0
    assert metadata["screen_anchor_mode"] == "random"
    assert metadata["screen_analysis_window_start"]
    assert metadata["screen_analysis_window_end"]
    assert metadata["final_scrutiny_passed"] is True
    assert metadata["final_scrutiny_score"] == 72.0
    assert metadata["completed_task_count"] > 1
    assert "coarse_halving" in metadata
    assert {
        "baseline",
        "lookback_top_3mo",
        "coarse_top_3mo",
        "validation_12mo",
        "final_36mo",
    } <= set(metadata["play_hand_phase_scores"])
    assert any(attempt["lab_task_kind"] == "sweep_shard" for attempt in attempts)
    assert any(
        attempt["play_hand_phase"] == "validation_12mo"
        and attempt["requested_horizon_months"] == 12
        for attempt in attempts
    )
    assert any(
        attempt["play_hand_phase"] == "final_36mo"
        and attempt["requested_horizon_months"] == 36
        for attempt in attempts
    )
