import json
import time
from pathlib import Path
from types import SimpleNamespace

import autoresearch.play_hand_massive as massive
from autoresearch.ledger import load_all_run_attempts, load_attempts, write_attempts
from autoresearch.ledger import load_run_metadata, write_run_metadata
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
    assert massive._desired_active_lanes(runtime, {"ok": True, "slots": 0}) == 0
    assert massive._desired_active_lanes(runtime, {"ok": False}) == 2

    gateway_runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=20,
            active_lanes=12,
            adaptive_lanes=True,
            min_active_lanes=2,
            target_worker_slots_per_lane=8,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    assert massive._desired_active_lanes(gateway_runtime, {"ok": False}) == 0

    fail_open = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=20,
            active_lanes=12,
            adaptive_lanes=True,
            adaptive_fail_open=True,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    assert massive._desired_active_lanes(fail_open, {"ok": False}) == 12

    fixed = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(lanes=20, active_lanes=7, adaptive_lanes=False)
    )
    assert massive._desired_active_lanes(fixed, {"ok": True, "slots": 64}) == 7


def test_adaptive_lane_window_uses_pressure_to_expand_and_contract() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=20,
            active_lanes=12,
            adaptive_lanes=True,
            min_active_lanes=1,
            target_worker_slots_per_lane=32,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    snapshot = {"ok": True, "slots": 64}

    assert massive._desired_active_lanes(
        runtime,
        snapshot,
        {"ok": True, "active_leases": 8, "pending_by_queue": {"deep_replay_jobs": 8}},
    ) == 4
    assert massive._desired_active_lanes(
        runtime,
        snapshot,
        {"ok": True, "active_leases": 256, "pending_by_queue": {"deep_replay_jobs": 32}},
    ) == 1


def test_adaptive_sweep_shard_size_scales_with_worker_slots_and_pressure() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            gateway_url="https://example.com/api/worker-gateway",
            min_sweep_shard_size=4,
            max_sweep_shard_size=16,
            target_shards_per_worker_slot=3.0,
        )
    )
    snapshot = {"ok": True, "slots": 55}

    assert massive._adaptive_sweep_shard_size(
        runtime,
        snapshot,
        None,
        max_permutations=960,
    ) == 6
    assert massive._adaptive_sweep_shard_size(
        runtime,
        snapshot,
        {"ok": True, "active_leases": 4, "pending_by_queue": {"deep_replay_jobs": 8}},
        max_permutations=960,
    ) == 4
    assert massive._adaptive_sweep_shard_size(
        runtime,
        snapshot,
        {"ok": True, "active_leases": 220, "pending_by_queue": {"deep_replay_jobs": 20}},
        max_permutations=960,
    ) == 8


def test_effective_remote_token_budget_floors_to_one_lane_cost() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            remote_token_budget_multiplier=2.0,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    snapshot = {"ok": True, "slots": 64}
    lane_cost = 512

    assert massive._remote_token_budget(snapshot, runtime) == 128
    assert massive._effective_remote_token_budget(snapshot, runtime, lane_cost) == 512


def test_gateway_pressure_should_pause_on_saturated_or_degraded() -> None:
    assert massive._gateway_pressure_should_pause({"ok": True, "status": "saturated"})
    assert massive._gateway_pressure_should_pause({"ok": True, "status": "degraded"})
    assert not massive._gateway_pressure_should_pause({"ok": True, "status": "ok"})
    assert not massive._gateway_pressure_should_pause({"ok": False, "status": "saturated"})


def test_run_campaign_lane_executor_blocks_on_backend_down(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=3,
            active_lanes=2,
            adaptive_lanes=False,
            staged_campaign=False,
        )
    )
    config = SimpleNamespace(
        runs_root=Path("."),
        derived_root=Path("."),
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    ctx = SimpleNamespace(
        config=config,
        run_dir=Path("."),
        run_id="campaign-test",
    )
    ctx.run_dir = Path(".")
    metadata: dict = {}

    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": False, "reason": "backend_health_failed"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        massive,
        "_append_event",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        massive,
        "_run_lane",
        lambda *_args, **_kwargs: massive.MassiveLaneResult(
            lane_id="lane_001",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
        ),
    )

    results, state = massive._run_campaign_lane_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "low", "value": 64},
        sweep_budget_label="low",
        sweep_budget_value=64,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1, 2, 3],
    )

    assert state["consecutive_backend_health_failures"] >= 1
    assert any(result.status == "not_started_backend_down" for result in results)


def test_staged_expand_applies_remote_token_budget(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=3,
            active_lanes=3,
            adaptive_lanes=True,
            staged_campaign=True,
            gateway_url="https://example.com/api/worker-gateway",
            gateway_token="secret",
            remote_token_budget_multiplier=2.0,
            telemetry_interval_seconds=1,
        )
    )
    config = SimpleNamespace(
        runs_root=Path("."),
        derived_root=Path("."),
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    ctx = SimpleNamespace(config=config, run_dir=Path("."), run_id="campaign-test")
    ctx.run_dir = Path(".")
    metadata: dict = {}
    max_concurrent = 0
    current = 0
    lock = __import__("threading").Lock()

    def fake_run_lane(*_args, **kwargs):
        nonlocal max_concurrent, current
        with lock:
            current += 1
            max_concurrent = max(max_concurrent, current)
        time.sleep(0.05)
        with lock:
            current -= 1
        lane_index = kwargs["lane_index"]
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 64, "pool_count": 1, "worker_count": 1},
    )
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", lambda *_args, **_kwargs: None)

    survivors = {
        index: massive.MassiveLaneResult(
            lane_id=f"lane_{index:03d}",
            status="baseline_screened",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{index}",
            run_dir=str(Path(f"lane-{index}")),
        )
        for index in (1, 2, 3)
    }

    results, _state = massive._run_campaign_lane_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "low", "value": 64},
        sweep_budget_label="low",
        sweep_budget_value=64,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1, 2, 3],
        expand_from=survivors,
    )

    assert len(results) == 3
    assert max_concurrent == 1


def test_cmd_play_hand_massive_dry_run_writes_first_class_lane_runs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
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
        staged_campaign=False,
        adaptive_lanes=False,
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


def _campaign_test_context() -> tuple[SimpleNamespace, SimpleNamespace, dict]:
    config = SimpleNamespace(
        runs_root=Path("."),
        derived_root=Path("."),
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    ctx = SimpleNamespace(config=config, run_dir=Path("."), run_id="campaign-test")
    ctx.run_dir = Path(".")
    return config, ctx, {}


def test_baseline_window_ignores_sweep_slot_ratio() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=24,
            active_lanes=4,
            scaffold_active_lanes=4,
            target_worker_slots_per_lane=75,
        )
    )
    assert massive._baseline_window(runtime) == 4


def test_expand_window_floors_to_one_when_expand_ready_and_workers_exist() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=24,
            active_lanes=4,
            adaptive_lanes=True,
            min_active_lanes=1,
            target_worker_slots_per_lane=75,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    snapshot = {"ok": True, "slots": 7}
    assert massive._expand_window(runtime, snapshot, has_expand_ready=False) == 1
    assert massive._expand_window(runtime, snapshot, has_expand_ready=True) == 1

    two_worker = {"ok": True, "slots": 2}
    assert massive._expand_window(runtime, two_worker, has_expand_ready=True) == 1


def test_expand_window_scales_for_large_pools() -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=24,
            active_lanes=8,
            adaptive_lanes=True,
            min_active_lanes=1,
            target_worker_slots_per_lane=64,
            gateway_url="https://example.com/api/worker-gateway",
        )
    )
    snapshot = {"ok": True, "slots": 200}
    assert massive._expand_window(runtime, snapshot, has_expand_ready=True) == 4


def test_rolling_staged_passes_adaptive_shard_size_to_expansion(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=1,
            active_lanes=1,
            scaffold_active_lanes=1,
            adaptive_lanes=True,
            staged_campaign=True,
            min_sweep_shard_size=4,
            max_sweep_shard_size=16,
            target_shards_per_worker_slot=3.0,
            gateway_url="https://example.com/api/worker-gateway",
            telemetry_interval_seconds=3600,
        )
    )
    config, ctx, metadata = _campaign_test_context()
    expansion_shards: list[int | None] = []

    def fake_run_lane(*_args, **kwargs):
        lane_index = kwargs["lane_index"]
        if kwargs.get("stop_after_baseline"):
            return massive.MassiveLaneResult(
                lane_id=f"lane_{lane_index:03d}",
                status="baseline_screened",
                started_at="2026-06-17T00:00:00+00:00",
                run_id=f"run-{lane_index}",
                run_dir=str(Path(f"lane-{lane_index}")),
            )
        expansion_shards.append(kwargs.get("sweep_shard_size"))
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 55, "pool_count": 1, "worker_count": 55},
    )
    monkeypatch.setattr(
        massive,
        "_poll_worker_gateway_pressure",
        lambda _runtime: {"ok": True, "status": "ok", "active_leases": 4, "pending_by_queue": {"deep_replay_jobs": 8}},
    )
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", lambda *_args, **_kwargs: None)

    results, _state = massive._run_rolling_staged_campaign_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "high", "value": 960},
        sweep_budget_label="high",
        sweep_budget_value=960,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1],
    )

    assert [result.status for result in results] == ["completed"]
    assert expansion_shards == [4]


def test_rolling_staged_submits_expand_before_all_baselines_finish(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=6,
            active_lanes=4,
            scaffold_active_lanes=4,
            adaptive_lanes=True,
            staged_campaign=True,
            target_worker_slots_per_lane=75,
            gateway_url="https://example.com/api/worker-gateway",
            telemetry_interval_seconds=3600,
        )
    )
    config, ctx, metadata = _campaign_test_context()
    submissions: list[tuple[int, str]] = []
    baseline_started = __import__("threading").Event()
    expand_started = __import__("threading").Event()

    def fake_run_lane(*_args, **kwargs):
        lane_index = kwargs["lane_index"]
        if kwargs.get("stop_after_baseline"):
            submissions.append((lane_index, "baseline"))
            if lane_index == 1:
                baseline_started.set()
            if lane_index >= 4:
                baseline_started.wait(timeout=2)
            return massive.MassiveLaneResult(
                lane_id=f"lane_{lane_index:03d}",
                status="baseline_screened",
                started_at="2026-06-17T00:00:00+00:00",
                run_id=f"run-{lane_index}",
                run_dir=str(Path(f"lane-{lane_index}")),
            )
        submissions.append((lane_index, "expand"))
        expand_started.set()
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 7, "pool_count": 1, "worker_count": 7},
    )
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", lambda *_args, **_kwargs: None)

    results, _state = massive._run_rolling_staged_campaign_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "high", "value": 1024},
        sweep_budget_label="high",
        sweep_budget_value=1024,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1, 2, 3, 4, 5, 6],
    )

    expand_indices = [lane for lane, phase in submissions if phase == "expand"]
    baseline_indices = [lane for lane, phase in submissions if phase == "baseline"]
    assert expand_indices
    assert min(expand_indices) == 1
    assert max(baseline_indices) > min(expand_indices)
    assert len(results) == 6


def test_rolling_staged_baseline_concurrency_reaches_scaffold_cap(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=8,
            active_lanes=4,
            scaffold_active_lanes=4,
            adaptive_lanes=True,
            target_worker_slots_per_lane=75,
            gateway_url="https://example.com/api/worker-gateway",
            telemetry_interval_seconds=3600,
        )
    )
    config, ctx, metadata = _campaign_test_context()
    lock = __import__("threading").Lock()
    baseline_inflight = 0
    max_baseline_inflight = 0

    def fake_run_lane(*_args, **kwargs):
        nonlocal baseline_inflight, max_baseline_inflight
        if not kwargs.get("stop_after_baseline"):
            time.sleep(0.01)
            lane_index = kwargs["lane_index"]
            return massive.MassiveLaneResult(
                lane_id=f"lane_{lane_index:03d}",
                status="completed",
                started_at="2026-06-17T00:00:00+00:00",
                run_id=f"run-{lane_index}",
                run_dir=str(Path(f"lane-{lane_index}")),
            )
        with lock:
            baseline_inflight += 1
            max_baseline_inflight = max(max_baseline_inflight, baseline_inflight)
        time.sleep(0.05)
        with lock:
            baseline_inflight -= 1
        lane_index = kwargs["lane_index"]
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="baseline_screened",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 7, "pool_count": 1, "worker_count": 7},
    )
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", lambda *_args, **_kwargs: None)

    massive._run_rolling_staged_campaign_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "high", "value": 1024},
        sweep_budget_label="high",
        sweep_budget_value=1024,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=list(range(1, 9)),
    )

    assert max_baseline_inflight >= 4


def test_rolling_staged_pauses_on_gateway_pressure_without_mass_not_started(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=3,
            active_lanes=2,
            scaffold_active_lanes=2,
            adaptive_lanes=True,
            staged_campaign=True,
            gateway_url="https://example.com/api/worker-gateway",
            telemetry_interval_seconds=3600,
        )
    )
    config, ctx, metadata = _campaign_test_context()
    pressure_calls = {"count": 0}

    def fake_run_lane(*_args, **kwargs):
        lane_index = kwargs["lane_index"]
        time.sleep(0.05)
        if kwargs.get("stop_after_baseline"):
            return massive.MassiveLaneResult(
                lane_id=f"lane_{lane_index:03d}",
                status="baseline_screened",
                started_at="2026-06-17T00:00:00+00:00",
                run_id=f"run-{lane_index}",
                run_dir=str(Path(f"lane-{lane_index}")),
            )
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    def fake_pressure(_runtime):
        pressure_calls["count"] += 1
        if pressure_calls["count"] >= 2:
            return {"ok": True, "status": "saturated"}
        return {"ok": True, "status": "ok"}

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 7, "pool_count": 1, "worker_count": 7},
    )
    monkeypatch.setattr(massive, "_poll_worker_gateway_pressure", fake_pressure)
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", lambda *_args, **_kwargs: None)

    results, state = massive._run_rolling_staged_campaign_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "high", "value": 1024},
        sweep_budget_label="high",
        sweep_budget_value=1024,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1, 2, 3],
    )

    assert not any(result.status == "not_started_gateway_pressure" for result in results)
    assert state["pause_reason"] in {None, "gateway_pressure"}


def test_rolling_staged_recovers_from_gateway_pressure_pause(monkeypatch) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(
            lanes=2,
            active_lanes=1,
            scaffold_active_lanes=1,
            adaptive_lanes=True,
            staged_campaign=True,
            gateway_url="https://example.com/api/worker-gateway",
            telemetry_interval_seconds=1,
        )
    )
    config, ctx, metadata = _campaign_test_context()
    submissions: list[tuple[int, str]] = []
    pressure_calls = {"count": 0}
    lane_window_events = {"count": 0}

    def fake_run_lane(*_args, **kwargs):
        lane_index = kwargs["lane_index"]
        if kwargs.get("stop_after_baseline"):
            submissions.append((lane_index, "baseline"))
            return massive.MassiveLaneResult(
                lane_id=f"lane_{lane_index:03d}",
                status="baseline_screened",
                started_at="2026-06-17T00:00:00+00:00",
                run_id=f"run-{lane_index}",
                run_dir=str(Path(f"lane-{lane_index}")),
            )
        submissions.append((lane_index, "expand"))
        return massive.MassiveLaneResult(
            lane_id=f"lane_{lane_index:03d}",
            status="completed",
            started_at="2026-06-17T00:00:00+00:00",
            run_id=f"run-{lane_index}",
            run_dir=str(Path(f"lane-{lane_index}")),
        )

    def fake_pressure(_runtime):
        pressure_calls["count"] += 1
        if pressure_calls["count"] == 1:
            return {"ok": True, "status": "saturated"}
        return {"ok": True, "status": "ok"}

    def fake_append_event(_ctx, _phase, status, **_payload):
        if status == "lane_window":
            lane_window_events["count"] += 1
            if lane_window_events["count"] > 4:
                raise AssertionError("scheduler did not recover from gateway_pressure")

    monkeypatch.setattr(massive, "_run_lane", fake_run_lane)
    monkeypatch.setattr(
        massive,
        "_poll_worker_pool_snapshot",
        lambda _runtime: {"ok": True, "slots": 10, "pool_count": 1, "worker_count": 10},
    )
    monkeypatch.setattr(massive, "_poll_worker_gateway_pressure", fake_pressure)
    monkeypatch.setattr(
        massive,
        "_poll_local_backend_health",
        lambda *_args, **_kwargs: {"ok": True, "url": "http://localhost:7946/healthz"},
    )
    monkeypatch.setattr(massive, "write_run_metadata", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(massive, "_append_event", fake_append_event)

    results, state = massive._run_rolling_staged_campaign_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "high", "value": 1024},
        sweep_budget_label="high",
        sweep_budget_value=1024,
        reward_matrix=None,
        metadata=metadata,
        lane_indexes=[1, 2],
    )

    assert pressure_calls["count"] >= 2
    assert (1, "baseline") in submissions
    assert (1, "expand") in submissions
    assert [result.status for result in results] == ["completed", "completed"]
    assert state["pause_reason"] is None


def test_run_lane_expansion_preserves_baseline_profile_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(dry_run=True)
    )
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    campaign_dir = config.derived_root / "campaign"
    campaign_ctx = massive._new_play_hand_context(
        config=config,
        cli=SimpleNamespace(),
        run_id="campaign-test",
        run_dir=campaign_dir,
        event_name="events.jsonl",
        summary_name="summary.json",
        runtime=runtime,
    )
    campaign_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
    campaign_ctx.evals_dir.mkdir(parents=True, exist_ok=True)

    lane_dir = config.runs_root / "lane-1"
    profile_path = lane_dir / "profiles" / "baseline.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps({"indicators": []}), encoding="utf-8")
    write_run_metadata(
        lane_dir,
        {
            "runner": "play_hand_v1",
            "run_id": "lane-1",
            "run_status": "screened",
            "indicators": ["RSI_CROSSBACK"],
            "instruments": ["EURUSD"],
            "baseline_score": 12.5,
            "profile_path": str(profile_path),
            "profile_ref": "cloud-profile-baseline",
            "evaluation_timeframe": "M5",
        },
    )

    monkeypatch.setattr(massive, "build_timing_axes", lambda _payload: [])
    monkeypatch.setattr(massive, "build_coarse_axes", lambda _payload: [])

    result = massive._run_lane(
        campaign_ctx,
        runtime=runtime,
        lane_index=1,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "low", "value": 64},
        sweep_budget_label="low",
        max_sweep_permutations=64,
        reward_matrix=None,
        existing_lane_run_id="lane-1",
        existing_lane_run_dir=lane_dir,
    )

    assert result.status == "completed"
    assert result.error is None
    metadata = load_run_metadata(lane_dir)
    assert metadata["profile_ref"] == "cloud-profile-baseline"
    assert metadata["profile_path"] == str(profile_path)


def test_staged_baseline_defers_cloud_profile_cleanup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(dry_run=False)
    )
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    campaign_ctx = massive._new_play_hand_context(
        config=config,
        cli=SimpleNamespace(),
        run_id="campaign-test",
        run_dir=config.derived_root / "campaign",
        event_name="events.jsonl",
        summary_name="summary.json",
        runtime=runtime,
    )
    campaign_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
    campaign_ctx.evals_dir.mkdir(parents=True, exist_ok=True)

    def fake_scaffold(ctx, **_kwargs):
        profile_path = ctx.profiles_dir / "lane.json"
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(json.dumps({"indicators": []}), encoding="utf-8")
        ctx.registered_profile_refs.append("cloud-profile-baseline")
        return profile_path, "cloud-profile-baseline", {"indicators": []}, "M5"

    cleanup_calls: list[str] = []
    monkeypatch.setattr(
        massive,
        "_deal_lane",
        lambda **_kwargs: {"dealt": ["RSI_CROSSBACK"], "instruments": ["EURUSD"], "indicator_deal": {}},
    )
    monkeypatch.setattr(massive, "_scaffold_lane_profile", fake_scaffold)
    monkeypatch.setattr(
        massive,
        "_run_profile_evaluation",
        lambda *_args, **_kwargs: {"attempt_id": None, "score": 10.0},
    )
    monkeypatch.setattr(
        massive,
        "_cleanup_registered_profiles",
        lambda *_args, **_kwargs: cleanup_calls.append("cleanup") or {},
    )

    result = massive._run_lane(
        campaign_ctx,
        runtime=runtime,
        lane_index=1,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "low", "value": 64},
        sweep_budget_label="low",
        max_sweep_permutations=64,
        reward_matrix=None,
        stop_after_baseline=True,
    )

    assert result.status == "baseline_screened"
    assert cleanup_calls == []
    metadata = load_run_metadata(Path(result.run_dir or ""))
    assert metadata["cloud_profile_cleanup"]["skip_reason"] == "pending_lane_expansion"
    assert metadata["profile_ref"] == "cloud-profile-baseline"


def test_expansion_reclaims_baseline_profile_for_cleanup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = massive.normalize_massive_runtime_config(
        massive.MassiveRuntimeConfig(dry_run=False)
    )
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    campaign_ctx = massive._new_play_hand_context(
        config=config,
        cli=SimpleNamespace(),
        run_id="campaign-test",
        run_dir=config.derived_root / "campaign",
        event_name="events.jsonl",
        summary_name="summary.json",
        runtime=runtime,
    )
    campaign_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
    campaign_ctx.evals_dir.mkdir(parents=True, exist_ok=True)
    lane_dir = config.runs_root / "lane-1"
    profile_path = lane_dir / "profiles" / "baseline.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps({"indicators": []}), encoding="utf-8")
    write_run_metadata(
        lane_dir,
        {
            "runner": "play_hand_v1",
            "run_id": "lane-1",
            "run_status": "screened",
            "indicators": ["RSI_CROSSBACK"],
            "instruments": ["EURUSD"],
            "baseline_score": 12.5,
            "profile_path": str(profile_path),
            "profile_ref": "cloud-profile-baseline",
            "evaluation_timeframe": "M5",
        },
    )

    cleaned_refs: list[list[str]] = []

    def fake_cleanup(ctx, **_kwargs):
        cleaned_refs.append(list(ctx.registered_profile_refs))
        return {"status": "completed", "deleted_profile_refs": list(ctx.registered_profile_refs)}

    monkeypatch.setattr(massive, "build_timing_axes", lambda _payload: [])
    monkeypatch.setattr(massive, "build_coarse_axes", lambda _payload: [])
    monkeypatch.setattr(massive, "_cleanup_registered_profiles", fake_cleanup)

    result = massive._run_lane(
        campaign_ctx,
        runtime=runtime,
        lane_index=1,
        seed_indicators=[],
        seed_plan=None,
        seed_plan_path=None,
        budget={"label": "low", "value": 64},
        sweep_budget_label="low",
        max_sweep_permutations=64,
        reward_matrix=None,
        existing_lane_run_id="lane-1",
        existing_lane_run_dir=lane_dir,
    )

    assert result.status == "completed"
    assert cleaned_refs == [["cloud-profile-baseline"]]
