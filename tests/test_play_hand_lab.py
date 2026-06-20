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


def test_normalize_runtime_loads_existing_gateway_token_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FUZZFOLIO_LAB_GATEWAY_TOKEN", raising=False)
    token_file = tmp_path / "gateway-token.txt"
    token_file.write_text("shared-token", encoding="ascii")
    monkeypatch.setenv("FUZZFOLIO_LAB_GATEWAY_TOKEN_FILE", str(token_file))

    runtime = lab._normalize_runtime(lab.PlayHandLabRuntimeConfig())

    assert runtime.gateway_token == "shared-token"


def test_normalize_runtime_resolves_instrument_pool_presets() -> None:
    runtime = lab._normalize_runtime(
        lab.PlayHandLabRuntimeConfig(
            instrument_pool_preset=["fx-major", "metals"],
            instrument_pool=["US500"],
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
        "US500",
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
        lambda _config: (seed_plan, tmp_path / "seed-plan.json"),
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
    )

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
