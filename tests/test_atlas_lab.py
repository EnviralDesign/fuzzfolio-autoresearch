from __future__ import annotations

import csv
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from autoresearch.atlas_lab import (
    AtlasLabRuntimeConfig,
    DEFAULT_ATLAS_PROFILE,
    ProbeRunSpec,
    _formal_task_profile_payload,
    _compact_sensitivity_snapshot_for_atlas,
    _deep_replay_request_from_probe,
    _enqueue_gateway_tasks_with_retries,
    _make_signal_atlas_cell_task,
    _probe_artifact_roots,
    _row_from_signal_result,
    _run_durable_atlas_stage,
    _validate_historical_probe_summary,
    audit_or_rewind_atlas_lab_stages,
    _stamp_historical_recipe_prior_lineage,
    atlas_profile_config,
    build_signal_atlas_via_gateway,
    effective_atlas_build_profile,
    run_atlas_lab,
    run_probe_spec_via_gateway,
)
from autoresearch.durable_execution import DurableExecutionError, DurableExecutionJournal, artifact_receipt
from autoresearch.anchor_pair_atlas import (
    _probe_results_fieldnames,
    _result_row_from_score,
    _timing_result_row_from_score,
    _timing_results_fieldnames,
)
from autoresearch.config import (
    AppConfig,
    FuzzfolioConfig,
    LlmConfig,
    ManagerConfig,
    ProviderProfileConfig,
    ResearchConfig,
    SuperviseConfig,
)
from autoresearch.fuzzfolio import FuzzfolioCli
from autoresearch.instrument_universe import universe_provenance
from autoresearch.__main__ import build_parser
from autoresearch.evidence_plan import canonical_sha256


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        repo_root=tmp_path,
        config_path=tmp_path / "autoresearch.config.json",
        secrets_path=tmp_path / ".agentsecrets",
        llm=LlmConfig(explorer_profile="test"),
        providers={"test": ProviderProfileConfig()},
        fuzzfolio=FuzzfolioConfig(),
        research=ResearchConfig(),
        supervise=SuperviseConfig(),
        manager=ManagerConfig(),
    )


def _profile_doc() -> dict[str, Any]:
    return {
        "format": "fuzzfolio.scoring-profile",
        "profile": {
            "name": "Atlas Test",
            "directionMode": "both",
            "notificationThreshold": 83,
            "indicators": [
                {
                    "meta": {"id": "RSI"},
                    "config": {"label": "RSI", "timeframe": "M5"},
                }
            ],
            "instruments": ["EURUSD"],
        },
    }


def _historical_runtime_kwargs() -> dict[str, object]:
    universe = universe_provenance()
    return {
        "research_generation_id": "generation-001",
        "level_c_protocol_id": "sha256:" + "e" * 64,
        "cutoff_key": "A",
        "source_snapshot_sha256": "sha256:" + "f" * 64,
        "universe_id": str(universe["universe_id"]),
        "universe_manifest_sha256": str(universe["universe_hash"]),
        "execution_plan_path": Path("level-c-execution-plan.json"),
        "execution_plan_id": "sha256:" + "9" * 64,
    }


def test_atlas_cli_exposes_complete_historical_lineage_contract() -> None:
    args = build_parser().parse_args(
        [
            "atlas-lab",
            "--as-of-date",
            "2025-06-30T00:00:00Z",
            "--research-generation-id",
            "generation-001",
            "--level-c-protocol-id",
            "sha256:" + "e" * 64,
            "--cutoff-key",
            "A",
            "--lake-manifest-sha256",
            "sha256:" + "d" * 64,
            "--source-snapshot-sha256",
            "sha256:" + "f" * 64,
            "--universe-id",
            "universe-1",
            "--universe-manifest-sha256",
            "sha256:" + "a" * 64,
        ]
    )

    assert args.research_generation_id == "generation-001"
    assert args.level_c_protocol_id == "sha256:" + "e" * 64
    assert args.cutoff_key == "A"
    assert args.source_snapshot_sha256 == "sha256:" + "f" * 64
    assert args.universe_id == "universe-1"
    assert args.universe_manifest_sha256 == "sha256:" + "a" * 64


def test_deep_replay_request_preserves_atlas_sensitivity_semantics() -> None:
    request = _deep_replay_request_from_probe(
        probe_id="probe-1",
        profile_payload=_profile_doc()["profile"],
        manifest_probe={
            "sensitivity_basket_args": [
                "sensitivity-basket",
                "--profile-ref",
                "<PROFILE_ID>",
                "--timeframe",
                "M15",
                "--lookback-months",
                "12",
                "--instrument",
                "EURUSD",
                "--instrument",
                "GBPUSD",
                "--quality-score-preset",
                "profile-drop",
                "--execution-cost-mode",
                "research-conservative",
            ]
        },
        row={"probe_timeframe": "M5", "instruments": "XAUUSD"},
        runtime=AtlasLabRuntimeConfig(),
        worker_contract_hash="contract123",
    )

    assert request["inline_profile_snapshot"]["name"] == "Atlas Test"
    assert request["instruments"] == ["EURUSD", "GBPUSD"]
    assert request["timeframe"] == "M15"
    assert request["lookback_months"] == 12
    assert request["market_data_source"] == "lake_bars"
    assert request["required_worker_contract_hash"] == "contract123"
    assert request["required_capabilities"] == ["deep_replay"]
    assert request["options"]["include_per_instrument"] is True
    assert request["options"]["path_metrics_mode"] == "highlighted"
    assert request["options"]["quality_score_preset"] == "profile_drop"
    assert request["options"]["cost_model"]["mode"] == "research_conservative"


def test_deep_replay_request_maps_as_of_date_to_explicit_window() -> None:
    request = _deep_replay_request_from_probe(
        probe_id="probe-1",
        profile_payload=_profile_doc()["profile"],
        manifest_probe={
            "sensitivity_basket_args": [
                "sensitivity-basket",
                "--profile-ref",
                "<PROFILE_ID>",
                "--timeframe",
                "M5",
                "--lookback-months",
                "3",
                "--as-of-date",
                "2026-06-30T23:59:59Z",
                "--instrument",
                "EURUSD",
            ]
        },
        row={"probe_timeframe": "M5", "instruments": "XAUUSD"},
        runtime=AtlasLabRuntimeConfig(
            lake_manifest_sha256="sha256:" + "a" * 64
        ),
        worker_contract_hash="contract123",
    )

    assert request["lookback_months"] is None
    assert request["analysis_window_start"] == "2026-03-30T23:59:59Z"
    assert request["analysis_window_end"] == "2026-06-30T23:59:59Z"
    assert request["evidence_plan"]["evidence_role"] == "training"
    assert request["evidence_plan"]["requested_horizon_months"] == 3
    assert request["evidence_plan"]["lake_manifest_sha256"] == "sha256:" + "a" * 64


def test_signal_atlas_cell_task_is_bounded_in_historical_mode() -> None:
    task = _make_signal_atlas_cell_task(
        runtime=AtlasLabRuntimeConfig(
            as_of_date="2025-06-30T00:00:00Z",
            signal_lookback_months=3,
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        worker_contract_hash="contract123",
        task_id="signal-1",
        indicator_id="RSI",
        profile_id="profile-1",
        profile_payload=_profile_doc()["profile"],
        instrument="EURUSD",
        timeframe="M5",
        bar_limit=5000,
    )

    payload = task["payload"]
    assert payload["lookback_months"] is None
    assert payload["analysis_window_start"] == "2025-03-30T00:00:00Z"
    assert payload["analysis_window_end"] == "2025-06-30T00:00:00Z"
    assert payload["evidence_plan"]["selection_data_end"] == "2025-06-30T00:00:00Z"
    assert payload["evidence_plan"]["lake_manifest_sha256"] == "sha256:" + "b" * 64
    assert payload["inline_profile_snapshot"]["notificationThreshold"] == 83.0
    assert task["resolved_profile_snapshot"] == payload["inline_profile_snapshot"]
    assert payload["evidence_plan"]["profile_snapshot_sha256"] == canonical_sha256(
        payload["inline_profile_snapshot"]
    )


def test_historical_signal_result_preserves_observed_lake_receipt(tmp_path: Path) -> None:
    task = _make_signal_atlas_cell_task(
        runtime=AtlasLabRuntimeConfig(
            as_of_date="2025-06-30T00:00:00Z",
            lake_manifest_sha256="sha256:" + "b" * 64,
        ),
        worker_contract_hash="contract123",
        task_id="signal-1",
        indicator_id="RSI",
        profile_id="profile-1",
        profile_payload=_profile_doc()["profile"],
        instrument="EURUSD",
        timeframe="M5",
        bar_limit=5000,
    )
    plan = task["payload"]["evidence_plan"]
    receipt = {
        "plan_id": plan["plan_id"],
        "profile_snapshot_sha256": plan["profile_snapshot_sha256"],
        "execution_cell_sha256": plan["execution_cell_sha256"],
        "observed_lake_manifest_sha256": plan["lake_manifest_sha256"],
    }
    row = _row_from_signal_result(
        base_row={"evidence_plan": plan},
        lab_result={
            "result": {
                "result": {
                    "execution_evidence": receipt,
                    "raw": {
                        "data": {
                            "long_score": [0.0, 1.0],
                            "short_score": [0.0, 0.0],
                            "timestamp": [1, 2],
                        }
                    },
                }
            }
        },
        raw_path=tmp_path / "raw.json",
    )

    assert row["evidence_plan_id"] == plan["plan_id"]
    assert row["observed_lake_manifest_sha256"] == plan["lake_manifest_sha256"]


def test_historical_atlas_rejects_unbounded_signal_executor(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires signal_atlas_executor='gateway'"):
        run_atlas_lab(
            _config(tmp_path),
            run_id="historical-local",
            runtime=AtlasLabRuntimeConfig(
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "c" * 64,
                signal_atlas_executor="local",
                **_historical_runtime_kwargs(),
            ),
            phases=["build"],
        )


def test_historical_atlas_requires_explicit_protocol_bound_run_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="explicit protocol-bound run_id"):
        run_atlas_lab(
            _config(tmp_path),
            run_id="auto",
            runtime=AtlasLabRuntimeConfig(
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "c" * 64,
                signal_atlas_executor="gateway",
                **_historical_runtime_kwargs(),
            ),
            phases=["build"],
        )


def test_historical_atlas_requires_complete_formal_lineage(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="research_generation_id"):
        run_atlas_lab(
            _config(tmp_path),
            run_id="historical-missing-lineage",
            runtime=AtlasLabRuntimeConfig(
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "c" * 64,
                signal_atlas_executor="gateway",
                execution_plan_path=tmp_path / "execution-plan.json",
                execution_plan_id="sha256:" + "9" * 64,
            ),
            phases=["build"],
        )


def test_historical_atlas_existing_root_requires_explicit_resume(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "derived" / "atlas-runs" / "existing"
    run_root.mkdir(parents=True)
    (run_root / "atlas-lab-run.json").write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="pass --resume"):
        run_atlas_lab(
            _config(tmp_path),
            run_id="existing",
            runtime=AtlasLabRuntimeConfig(
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "d" * 64,
                signal_atlas_executor="gateway",
                **_historical_runtime_kwargs(),
            ),
            phases=["build"],
        )


def test_historical_atlas_uses_plan_worker_contract_without_live_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from autoresearch import atlas_lab as atlas

    config = _config(tmp_path)
    contract = "sha256:" + "a" * 64
    runtime = AtlasLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        lake_manifest_sha256="sha256:" + "d" * 64,
        worker_contract_hash=contract,
        signal_atlas_executor="gateway",
        **_historical_runtime_kwargs(),
    )
    monkeypatch.setattr(
        atlas,
        "validate_executor_runtime_binding",
        lambda *_args, **_kwargs: (
            {"worker_contract_hash": contract},
            {"generation": {"active_runs_root": str(config.runs_root)}},
        ),
    )
    monkeypatch.setattr(
        atlas,
        "resolve_atlas_worker_contract_hash",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live resolver called")),
    )

    result = run_atlas_lab(
        config,
        run_id="plan-contract",
        runtime=runtime,
        phases=["none"],
        gateway=object(),
    )
    assert result.status == "completed"


def test_historical_atlas_stage_resume_requires_exact_artifact_receipt(tmp_path: Path) -> None:
    run_root = tmp_path / "atlas-run"
    stage_root = run_root / "indicator-atlas"
    run_root.mkdir()
    journal = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="sha256:" + "1" * 64,
        lineage={"cutoff_key": "A"},
    )

    def build() -> str:
        stage_root.mkdir()
        (stage_root / "summary.json").write_text('{"status":"complete"}', encoding="utf-8")
        return "built"

    assert _run_durable_atlas_stage(
        journal=journal,
        run_root=run_root,
        stage="01-indicator-atlas",
        payload={"cutoff_key": "A"},
        artifact_roots=(stage_root,),
        action=build,
    ) == ("built", False)
    assert _run_durable_atlas_stage(
        journal=journal,
        run_root=run_root,
        stage="01-indicator-atlas",
        payload={"cutoff_key": "A"},
        artifact_roots=(stage_root,),
        action=lambda: (_ for _ in ()).throw(AssertionError("stage reran")),
    ) == (None, True)

    (stage_root / "summary.json").write_text('{"status":"partial"}', encoding="utf-8")
    with pytest.raises(DurableExecutionError, match="verification failed"):
        _run_durable_atlas_stage(
            journal=journal,
            run_root=run_root,
            stage="01-indicator-atlas",
            payload={"cutoff_key": "A"},
            artifact_roots=(stage_root,),
            action=lambda: None,
        )


def test_historical_atlas_resume_preserves_partial_stage_before_retry(tmp_path: Path) -> None:
    run_root = tmp_path / "atlas-run"
    stage_root = run_root / "signal-atlas"
    run_root.mkdir()
    journal = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="sha256:" + "2" * 64,
        lineage={"cutoff_key": "B"},
    )

    def crash() -> None:
        stage_root.mkdir()
        (stage_root / "partial.json").write_text("{}", encoding="utf-8")
        raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        _run_durable_atlas_stage(
            journal=journal,
            run_root=run_root,
            stage="02-signal-atlas",
            payload={"cutoff_key": "B"},
            artifact_roots=(stage_root,),
            action=crash,
        )

    def retry() -> None:
        stage_root.mkdir()
        (stage_root / "complete.json").write_text("{}", encoding="utf-8")

    _run_durable_atlas_stage(
        journal=journal,
        run_root=run_root,
        stage="02-signal-atlas",
        payload={"cutoff_key": "B"},
        artifact_roots=(stage_root,),
        action=retry,
    )
    assert (stage_root / "complete.json").is_file()
    assert list((run_root / "partial-stages").rglob("partial.json"))


def test_historical_recipe_priors_are_stamped_with_immutable_lineage(tmp_path: Path) -> None:
    recipe_priors_dir = tmp_path / "recipe-priors"
    recipe_priors_dir.mkdir()
    for name in (
        "play-hand-seed-plan.json",
        "recipe-priors.json",
        "recipe-priors-summary.json",
    ):
        (recipe_priors_dir / name).write_text(json.dumps({"recipes": {}}), encoding="utf-8")
    lineage = {
        **_historical_runtime_kwargs(),
        "as_of_date": "2025-06-30T00:00:00Z",
        "lake_manifest_sha256": "sha256:" + "d" * 64,
    }
    lineage.pop("execution_plan_path")

    _stamp_historical_recipe_prior_lineage(recipe_priors_dir, lineage=lineage)

    for name in (
        "play-hand-seed-plan.json",
        "recipe-priors.json",
        "recipe-priors-summary.json",
    ):
        payload = json.loads((recipe_priors_dir / name).read_text(encoding="utf-8"))
        assert payload["historical_lineage"] == lineage
    descriptor = json.loads((recipe_priors_dir / "level-c-lineage.json").read_text(encoding="utf-8"))
    assert descriptor["historical_lineage"] == lineage
    assert set(descriptor["artifact_sha256"]) == {
        "play-hand-seed-plan.json",
        "recipe-priors.json",
        "recipe-priors-summary.json",
    }


def test_compact_sensitivity_snapshot_keeps_score_fields_and_drops_heavy_payloads() -> None:
    snapshot = {
        "requested_timeframe": "M5",
        "data": {
            "aggregate": {
                "score_lab": {"version": "score_lab_v2_5_3", "score": 71.0},
                "quality_score": {"score": 71.0},
                "best_cell": {"stop_loss_percent": 0.02, "reward_multiple": 2.0},
                "behavior_summary": {"signal_count": 14},
                "aggregate_matrix": [{"payload": "x" * 10_000}],
                "path_points": [{"payload": "y" * 10_000}],
            }
        },
    }

    compact = _compact_sensitivity_snapshot_for_atlas(snapshot)

    assert compact == {
        "requested_timeframe": "M5",
        "data": {
            "aggregate": {
                "score_lab": {"version": "score_lab_v2_5_3", "score": 71.0},
                "quality_score": {"score": 71.0},
                "best_cell": {"stop_loss_percent": 0.02, "reward_multiple": 2.0},
                "behavior_summary": {"signal_count": 14},
            }
        },
    }


class FakeGateway:
    def __init__(self) -> None:
        self.gateway_id = "fake-gateway"
        self.results: list[dict[str, Any]] = []
        self.acked: list[str] = []
        self.enqueued_tasks: list[dict[str, Any]] = []
        self._lease_counter = itertools.count(1)

    def snapshot(self) -> dict[str, Any]:
        return {
            "gateway_id": self.gateway_id,
            "worker_slots": 2,
            "busy_slots": 0,
            "queued_tasks": 0,
            "result_backlog": len(self.results),
            "metrics": {"results_dropped": 0},
        }

    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            lease_id = f"lease-{next(self._lease_counter)}"
            task_kind = task["task_kind"]
            if task_kind == "deep_replay":
                payload = task["payload"]
                evidence_plan = payload.get("evidence_plan") or {}
                execution_evidence = (
                    {
                        "plan_id": evidence_plan["plan_id"],
                        "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
                        "execution_cell_sha256": evidence_plan["execution_cell_sha256"],
                        "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
                    }
                    if evidence_plan.get("lake_manifest_sha256")
                    else None
                )
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lease_id": lease_id,
                        "worker_id": "fake-worker",
                        "lane_id": task["lane_id"],
                        "attempt_id": task["attempt_id"],
                        "status": "success",
                        "result": {
                            "status": "success",
                            "request": payload,
                            "execution_evidence": execution_evidence,
                            "result": {
                                "aggregate": {
                                    "score_lab": {
                                        "version": "score_lab_v2_5_3",
                                        "score": 72.5,
                                        "combiner": "canonical",
                                    },
                                    "quality_score": {"score": 72.5},
                                    "best_cell": {
                                        "stop_loss_percent": 0.02,
                                        "reward_multiple": 2.0,
                                        "avg_net_r_per_closed_trade": 0.4,
                                        "resolved_trades": 18,
                                        "win_rate": 0.61,
                                        "profit_factor": 1.8,
                                    },
                                    "behavior_summary": {"signal_count": 44},
                                }
                            },
                        },
                    }
                )
            elif task_kind == "deep_replay_detail":
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lease_id": lease_id,
                        "worker_id": "fake-worker",
                        "lane_id": task["lane_id"],
                        "attempt_id": task["attempt_id"],
                        "status": "success",
                        "result": {
                            "status": "success",
                            "result": {
                                "cache_ready": True,
                                "cell_detail": {
                                    "basis": "selected_cell_detail",
                                    "stop_loss_percent": 0.02,
                                    "reward_multiple": 2.0,
                                    "points": [],
                                },
                            },
                        },
                    }
                )
        return {"status": "accepted", "enqueued": len(tasks)}

    def read_results(self, *, limit: int) -> list[dict[str, Any]]:
        batch = self.results[:limit]
        self.results = self.results[limit:]
        return batch

    def ack_results(self, lease_ids: list[str]) -> int:
        self.acked.extend(lease_ids)
        return len(lease_ids)


class FailedProbeGateway(FakeGateway):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            self.results.append(
                {
                    "task_id": task["task_id"],
                    "lease_id": f"lease-{next(self._lease_counter)}",
                    "worker_id": "failed-worker",
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "failed",
                    "result": {
                        "status": "failed",
                        "error": "EvidencePlanValidationError: profile mismatch",
                    },
                }
            )
        return {"status": "accepted", "enqueued": len(tasks)}


class NoValidCellProbeGateway(FakeGateway):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            payload = task["payload"]
            evidence_plan = payload.get("evidence_plan") or {}
            execution_evidence = {
                "plan_id": evidence_plan["plan_id"],
                "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
                "execution_cell_sha256": evidence_plan["execution_cell_sha256"],
                "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
            }
            terminal_result = {
                "schema": "fuzzfolio-replay-terminal-result-v1",
                "status": "nonviable",
                "outcome": "no_valid_cell",
                "diagnostics": {
                    "signal_count": 2,
                    "resolved_trade_count_max": 1,
                    "market_data_window": {"filtered_bar_count": 1200},
                    "analysis_notes": ["no viable matrix cell"],
                },
                "execution_evidence": execution_evidence,
                "worker_timing": {"schema": "fuzzfolio-replay-worker-timing-v1"},
            }
            self.results.append(
                {
                    "task_id": task["task_id"],
                    "lease_id": f"lease-{next(self._lease_counter)}",
                    "worker_id": "nonviable-worker",
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "failed",
                    "result": {
                        "status": "failed",
                        "error": "FullBacktestNoValidCellError: no best cell",
                        "terminal_result": terminal_result,
                    },
                }
            )
        return {"status": "accepted", "enqueued": len(tasks)}


class MissingBestCellSuccessGateway(FakeGateway):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            payload = task["payload"]
            evidence_plan = payload.get("evidence_plan") or {}
            execution_evidence = {
                "plan_id": evidence_plan["plan_id"],
                "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
                "execution_cell_sha256": evidence_plan["execution_cell_sha256"],
                "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
            }
            self.results.append(
                {
                    "task_id": task["task_id"],
                    "lease_id": f"lease-{next(self._lease_counter)}",
                    "worker_id": "malformed-worker",
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "result": {
                        "status": "success",
                        "request": payload,
                        "execution_evidence": execution_evidence,
                        "result": {
                            "aggregate": {
                                "score_lab": {"version": "score_lab_v2_5_3", "score": 0},
                                "quality_score": {"score": 0},
                                "behavior_summary": {"signal_count": 2},
                            }
                        },
                    },
                }
            )
        return {"status": "accepted", "enqueued": len(tasks)}


def _zero_signal_aggregate(*, total_signals: int = 0, resolved_trades: int = 0) -> dict[str, Any]:
    return {
        "best_cell": None,
        "recommended_cell": None,
        "signal_count": total_signals,
        "resolved_trade_count_max": resolved_trades,
        "instrument_count": 36,
        "market_data_window": {
            "loaded_bar_count": 14717,
            "filtered_bar_count": 14717,
            "instrument_window_count": 36,
            "requested_window_start": "2024-03-29T00:00:00+00:00",
            "requested_window_end": "2024-06-29T00:00:00+00:00",
        },
        "behavior_summary": {
            "eligible_bar_count": 671083,
            "bars_with_signal_count": total_signals,
            "long_signal_count": 0,
            "short_signal_count": 0,
            "signal_density": 0.0,
            "signal_coverage_ratio": 0.0,
        },
        "matrix": {
            "geometry": {
                "sl_rows": 1,
                "reward_columns": 1,
                "sl_start_percent": 0.02,
                "reward_start_r": 1.0,
            },
            "rows": [
                [
                    {
                        "stop_loss_percent": 0.02,
                        "reward_multiple": 1.0,
                        "take_profit_percent": 0.02,
                        "total_signals": total_signals,
                        "resolved_trades": resolved_trades,
                        "unresolved": 0,
                        "wins": 0,
                        "losses": 0,
                        "avg_net_r_per_closed_trade": None,
                        "profit_factor": None,
                        "win_rate": None,
                    }
                ]
            ],
        },
        "matrix_summary": {
            "total_cell_count": 1,
            "positive_cell_count": 0,
            "positive_cell_ratio": 0.0,
            "robust_cell": None,
        },
        "score_lab": {
            "version": "score_lab_v2_5_3",
            "score": 0.0,
            "status": "canonical",
        },
        "quality_score": None,
    }


class NoSignalAggregateSuccessGateway(FakeGateway):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            payload = task["payload"]
            evidence_plan = payload.get("evidence_plan") or {}
            execution_evidence = {
                "plan_id": evidence_plan["plan_id"],
                "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
                "execution_cell_sha256": evidence_plan["execution_cell_sha256"],
                "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
            }
            self.results.append(
                {
                    "task_id": task["task_id"],
                    "lease_id": f"lease-{next(self._lease_counter)}",
                    "worker_id": "zero-signal-worker",
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "result": {
                        "status": "success",
                        "status_detail": "Deep replay completed.",
                        "request": payload,
                        "execution_evidence": execution_evidence,
                        "result": {"aggregate": _zero_signal_aggregate()},
                    },
                }
            )
        return {"status": "accepted", "enqueued": len(tasks)}


class MixedNoBestAggregateSuccessGateway(NoSignalAggregateSuccessGateway):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        self.enqueued_tasks.extend(tasks)
        for task in tasks:
            payload = task["payload"]
            evidence_plan = payload.get("evidence_plan") or {}
            execution_evidence = {
                "plan_id": evidence_plan["plan_id"],
                "profile_snapshot_sha256": evidence_plan["profile_snapshot_sha256"],
                "execution_cell_sha256": evidence_plan["execution_cell_sha256"],
                "observed_lake_manifest_sha256": evidence_plan["lake_manifest_sha256"],
            }
            self.results.append(
                {
                    "task_id": task["task_id"],
                    "lease_id": f"lease-{next(self._lease_counter)}",
                    "worker_id": "mixed-worker",
                    "lane_id": task["lane_id"],
                    "attempt_id": task["attempt_id"],
                    "status": "success",
                    "result": {
                        "status": "success",
                        "request": payload,
                        "execution_evidence": execution_evidence,
                        "result": {
                            "aggregate": _zero_signal_aggregate(
                                total_signals=3,
                                resolved_trades=0,
                            )
                        },
                    },
                }
            )
        return {"status": "accepted", "enqueued": len(tasks)}


def _write_anchor_pair_probe_fixture(tmp_path: Path) -> tuple[Path, Path]:
    source_dir = tmp_path / "runs" / "derived" / "atlas-runs" / "test" / "anchor-pair-atlas"
    profile_dir = source_dir / "profiles"
    result_dir = source_dir / "probe-results" / "probe-1"
    profile_dir.mkdir(parents=True)
    profile_path = profile_dir / "probe-1.json"
    profile_path.write_text(json.dumps(_profile_doc()), encoding="utf-8")
    atlas_payload = {
        "queue_rows": [
            {
                "probe_id": "probe-1",
                "queue_rank": 1,
                "anchor_type": "trend",
                "anchor_id": "ANCHOR",
                "trigger_id": "TRIGGER",
                "probe_timeframe": "M5",
                "pair_prior_score": 80,
                "pair_prior_bucket": "probe_now",
                "instruments": "EURUSD",
            }
        ],
        "run_manifest": {
            "probes": [
                {
                    "probe_id": "probe-1",
                    "profile_path": str(profile_path),
                    "output_dir": str(result_dir),
                    "sensitivity_basket_args": [
                        "sensitivity-basket",
                        "--profile-ref",
                        "<PROFILE_ID>",
                        "--timeframe",
                        "M5",
                        "--lookback-months",
                        "12",
                        "--as-of-date",
                        "2025-06-30T00:00:00Z",
                        "--instrument",
                        "EURUSD",
                        "--quality-score-preset",
                        "profile-drop",
                        "--execution-cost-mode",
                        "research-conservative",
                        "--output-dir",
                        str(result_dir),
                    ],
                }
            ]
        },
    }
    (source_dir / "anchor-pair-atlas.json").write_text(json.dumps(atlas_payload), encoding="utf-8")
    return source_dir, result_dir


def test_run_probe_spec_via_gateway_writes_scoreable_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir = tmp_path / "runs" / "derived" / "atlas-runs" / "test" / "anchor-pair-atlas"
    profile_dir = source_dir / "profiles"
    result_dir = source_dir / "probe-results" / "probe-1"
    profile_dir.mkdir(parents=True)
    profile_path = profile_dir / "probe-1.json"
    profile_path.write_text(
        '{"format":"fuzzfolio.scoring-profile","profile":{"name":"Atlas Test","directionMode":"both","notificationThreshold":83,"indicators":[],"instruments":["EURUSD"]}}',
        encoding="utf-8",
    )
    atlas_payload = {
        "queue_rows": [
            {
                "probe_id": "probe-1",
                "queue_rank": 1,
                "anchor_type": "trend",
                "anchor_id": "ANCHOR",
                "trigger_id": "TRIGGER",
                "probe_timeframe": "M5",
                "pair_prior_score": 80,
                "pair_prior_bucket": "probe_now",
                "instruments": "EURUSD",
            }
        ],
        "run_manifest": {
            "probes": [
                {
                    "probe_id": "probe-1",
                    "profile_path": str(profile_path),
                    "output_dir": str(result_dir),
                    "sensitivity_basket_args": [
                        "sensitivity-basket",
                        "--profile-ref",
                        "<PROFILE_ID>",
                        "--timeframe",
                        "M5",
                        "--lookback-months",
                        "12",
                        "--as-of-date",
                        "2025-06-30T00:00:00Z",
                        "--instrument",
                        "EURUSD",
                        "--quality-score-preset",
                        "profile-drop",
                        "--execution-cost-mode",
                        "research-conservative",
                        "--output-dir",
                        str(result_dir),
                    ],
                }
            ]
        },
    }
    (source_dir / "anchor-pair-atlas.json").write_text(
        __import__("json").dumps(atlas_payload),
        encoding="utf-8",
    )

    def fake_score_artifact(self, artifact_dir: Path) -> dict[str, Any]:
        return {
            "best": {
                "score_lab": {
                    "version": "score_lab_v2_5_3",
                    "score": 72.5,
                    "combiner": "canonical",
                },
                "trades": 18,
            }
        }

    monkeypatch.setattr(FuzzfolioCli, "score_artifact", fake_score_artifact)

    gateway = FakeGateway()
    outcome = run_probe_spec_via_gateway(
        config,
        spec=ProbeRunSpec(
            kind="anchor_pair",
            source_dir=source_dir,
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        gateway=gateway,
        runtime=AtlasLabRuntimeConfig(
            active_probes=1,
            result_batch_size=10,
            as_of_date="2025-06-30T00:00:00Z",
            lake_manifest_sha256="sha256:" + "e" * 64,
        ),
        worker_contract_hash="contract123",
    )

    assert outcome.summary["result_counts"]["completed"] == 1
    assert outcome.summary["result_counts"]["scored"] == 1
    aggregate_task = gateway.enqueued_tasks[0]
    assert aggregate_task["task_id"].startswith("test-anchor_pair-")
    assert aggregate_task["task_id"].endswith("-probe-1-aggregate")
    assert aggregate_task["payload"]["job_id"] == aggregate_task["task_id"]
    inline_profile = aggregate_task["payload"]["inline_profile_snapshot"]
    evidence_plan = aggregate_task["payload"]["evidence_plan"]
    assert inline_profile["notificationThreshold"] == 83.0
    assert evidence_plan["profile_snapshot_sha256"] == canonical_sha256(inline_profile)
    detail_task = gateway.enqueued_tasks[1]
    assert detail_task["task_id"].startswith("test-anchor_pair-")
    assert detail_task["task_id"].endswith("-probe-1-detail")
    assert detail_task["payload"]["job_id"] == detail_task["task_id"]
    assert detail_task["payload"]["parent_job_id"] == aggregate_task["task_id"]
    assert (result_dir / "sensitivity-response.json").exists()
    assert not (result_dir / "deep-replay-job.json").exists()
    assert (result_dir / "best-cell-path-detail.json").exists()
    compact_snapshot = json.loads((result_dir / "sensitivity-response.json").read_text(encoding="utf-8"))
    assert compact_snapshot["data"]["aggregate"]["score_lab"]["score"] == 72.5
    assert "per_instrument" not in compact_snapshot["data"]
    results_csv_path = source_dir / "anchor-pair-probe-results.csv"
    results_csv = list(csv.DictReader(results_csv_path.open(encoding="utf-8")))
    assert len(results_csv) == 1
    assert results_csv[0]["evidence_plan_id"]
    assert results_csv[0]["observed_lake_manifest_sha256"] == "sha256:" + "e" * 64


def test_formal_probe_profile_payload_matches_evidence_plan_hash() -> None:
    runtime = AtlasLabRuntimeConfig(
        as_of_date="2025-06-30T00:00:00Z",
        lake_manifest_sha256="sha256:" + "e" * 64,
    )
    profile_payload = _formal_task_profile_payload(_profile_doc()["profile"], runtime=runtime)

    request = _deep_replay_request_from_probe(
        probe_id="probe-1",
        profile_payload=profile_payload,
        manifest_probe={
            "sensitivity_basket_args": [
                "sensitivity-basket",
                "--timeframe",
                "M5",
                "--lookback-months",
                "12",
                "--as-of-date",
                "2025-06-30T00:00:00Z",
                "--instrument",
                "EURUSD",
                "--output-dir",
                "out",
            ]
        },
        row={"probe_timeframe": "M5", "instruments": "EURUSD"},
        runtime=runtime,
        worker_contract_hash="contract123",
    )

    assert request["inline_profile_snapshot"]["notificationThreshold"] == 83.0
    assert request["evidence_plan"]["profile_snapshot_sha256"] == canonical_sha256(
        request["inline_profile_snapshot"]
    )


def test_historical_probe_worker_failure_fails_closed_without_stage_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, _result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    with pytest.raises(RuntimeError, match="Historical anchor_pair Atlas worker/result failed"):
        run_probe_spec_via_gateway(
            config,
            spec=ProbeRunSpec(
                kind="anchor_pair",
                source_dir=source_dir,
                atlas_filename="anchor-pair-atlas.json",
                results_filename="anchor-pair-probe-results.csv",
                summary_filename="anchor-pair-probe-summary.json",
                manifest_schema="anchor_pair_run_manifest_v1",
                result_fieldnames=_probe_results_fieldnames,
                row_builder=_result_row_from_score,
            ),
            gateway=FailedProbeGateway(),
            runtime=AtlasLabRuntimeConfig(
                active_probes=1,
                result_batch_size=10,
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "e" * 64,
            ),
            worker_contract_hash="contract123",
        )

    assert not (source_dir / "anchor-pair-probe-results.csv").exists()
    assert not (source_dir / "anchor-pair-probe-summary.json").exists()


def test_historical_probe_no_valid_cell_terminal_continues_as_nonviable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    gateway = NoValidCellProbeGateway()
    outcome = run_probe_spec_via_gateway(
        config,
        spec=ProbeRunSpec(
            kind="anchor_pair",
            source_dir=source_dir,
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        gateway=gateway,
        runtime=AtlasLabRuntimeConfig(
            active_probes=1,
            result_batch_size=10,
            as_of_date="2025-06-30T00:00:00Z",
            lake_manifest_sha256="sha256:" + "e" * 64,
        ),
        worker_contract_hash="contract123",
    )

    assert outcome.summary["result_counts"]["completed"] == 1
    assert outcome.summary["result_counts"]["scored"] == 0
    assert outcome.summary["result_counts"]["status_counts"] == {"nonviable": 1}
    rows = list(csv.DictReader((source_dir / "anchor-pair-probe-results.csv").open(encoding="utf-8")))
    assert rows[0]["status"] == "nonviable"
    assert rows[0]["terminal_outcome"] == "no_valid_cell"
    assert rows[0]["evidence_plan_id"]
    assert "no viable matrix cell" in rows[0]["terminal_reason"]
    assert (result_dir / "execution-evidence.json").exists()
    assert not any(task["task_kind"] == "deep_replay_detail" for task in gateway.enqueued_tasks)


def test_historical_probe_no_signal_success_aggregate_continues_as_nonviable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    gateway = NoSignalAggregateSuccessGateway()
    outcome = run_probe_spec_via_gateway(
        config,
        spec=ProbeRunSpec(
            kind="anchor_pair",
            source_dir=source_dir,
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        gateway=gateway,
        runtime=AtlasLabRuntimeConfig(
            active_probes=1,
            result_batch_size=10,
            as_of_date="2025-06-30T00:00:00Z",
            lake_manifest_sha256="sha256:" + "e" * 64,
        ),
        worker_contract_hash="contract123",
    )

    assert outcome.summary["result_counts"]["completed"] == 1
    assert outcome.summary["result_counts"]["scored"] == 0
    assert outcome.summary["result_counts"]["status_counts"] == {"nonviable": 1}
    rows = list(csv.DictReader((source_dir / "anchor-pair-probe-results.csv").open(encoding="utf-8")))
    assert rows[0]["status"] == "nonviable"
    assert rows[0]["terminal_outcome"] == "no_valid_cell"
    terminal_reason = json.loads(rows[0]["terminal_reason"])
    assert terminal_reason["reason"] == "aggregate_no_signal"
    assert terminal_reason["matrix_cell_count"] == 1
    assert terminal_reason["market_data_window"]["filtered_bar_count"] == 14717
    assert (result_dir / "execution-evidence.json").exists()
    assert not any(task["task_kind"] == "deep_replay_detail" for task in gateway.enqueued_tasks)


def test_historical_probe_missing_best_cell_without_terminal_fails_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, _result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    with pytest.raises(RuntimeError, match="detail-capable best cell"):
        run_probe_spec_via_gateway(
            config,
            spec=ProbeRunSpec(
                kind="anchor_pair",
                source_dir=source_dir,
                atlas_filename="anchor-pair-atlas.json",
                results_filename="anchor-pair-probe-results.csv",
                summary_filename="anchor-pair-probe-summary.json",
                manifest_schema="anchor_pair_run_manifest_v1",
                result_fieldnames=_probe_results_fieldnames,
                row_builder=_result_row_from_score,
            ),
            gateway=MissingBestCellSuccessGateway(),
            runtime=AtlasLabRuntimeConfig(
                active_probes=1,
                result_batch_size=10,
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "e" * 64,
            ),
            worker_contract_hash="contract123",
        )

    assert not (source_dir / "anchor-pair-probe-results.csv").exists()
    assert not (source_dir / "anchor-pair-probe-summary.json").exists()


def test_historical_probe_mixed_no_best_success_fails_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, _result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    with pytest.raises(RuntimeError, match="detail-capable best cell"):
        run_probe_spec_via_gateway(
            config,
            spec=ProbeRunSpec(
                kind="anchor_pair",
                source_dir=source_dir,
                atlas_filename="anchor-pair-atlas.json",
                results_filename="anchor-pair-probe-results.csv",
                summary_filename="anchor-pair-probe-summary.json",
                manifest_schema="anchor_pair_run_manifest_v1",
                result_fieldnames=_probe_results_fieldnames,
                row_builder=_result_row_from_score,
            ),
            gateway=MixedNoBestAggregateSuccessGateway(),
            runtime=AtlasLabRuntimeConfig(
                active_probes=1,
                result_batch_size=10,
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "e" * 64,
            ),
            worker_contract_hash="contract123",
        )

    assert not (source_dir / "anchor-pair-probe-results.csv").exists()
    assert not (source_dir / "anchor-pair-probe-summary.json").exists()


def test_historical_probe_summary_rejects_terminal_all_failed_reuse() -> None:
    with pytest.raises(RuntimeError, match="invalid accounting"):
        _validate_historical_probe_summary(
            spec=ProbeRunSpec(
                kind="anchor_pair",
                source_dir=Path("unused"),
                atlas_filename="anchor-pair-atlas.json",
                results_filename="anchor-pair-probe-results.csv",
                summary_filename="anchor-pair-probe-summary.json",
                manifest_schema="anchor_pair_run_manifest_v1",
                result_fieldnames=_probe_results_fieldnames,
                row_builder=_result_row_from_score,
            ),
            summary={
                "result_counts": {
                    "selected": 48,
                    "completed": 48,
                    "scored": 0,
                    "status_counts": {"failed": 48},
                }
            },
        )


def test_historical_probe_summary_accepts_terminal_all_nonviable_reuse() -> None:
    _validate_historical_probe_summary(
        spec=ProbeRunSpec(
            kind="anchor_pair",
            source_dir=Path("unused"),
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        summary={
            "result_counts": {
                "selected": 48,
                "completed": 48,
                "scored": 0,
                "status_counts": {"nonviable": 48},
            }
        },
    )


def test_durable_probe_stage_does_not_complete_on_formal_worker_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir, _result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    run_root = source_dir.parent
    journal = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="unit-execution",
        lineage={},
    )
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", lambda *args, **kwargs: {})

    with pytest.raises(RuntimeError, match="Historical anchor_pair Atlas worker/result failed"):
        _run_durable_atlas_stage(
            journal=journal,
            run_root=run_root,
            stage="05-anchor-pair-probes",
            payload={"stage": "05-anchor-pair-probes"},
            artifact_roots=(source_dir,),
            action=lambda: run_probe_spec_via_gateway(
                config,
                spec=ProbeRunSpec(
                    kind="anchor_pair",
                    source_dir=source_dir,
                    atlas_filename="anchor-pair-atlas.json",
                    results_filename="anchor-pair-probe-results.csv",
                    summary_filename="anchor-pair-probe-summary.json",
                    manifest_schema="anchor_pair_run_manifest_v1",
                    result_fieldnames=_probe_results_fieldnames,
                    row_builder=_result_row_from_score,
                ),
                gateway=FailedProbeGateway(),
                runtime=AtlasLabRuntimeConfig(
                    active_probes=1,
                    result_batch_size=10,
                    as_of_date="2025-06-30T00:00:00Z",
                    lake_manifest_sha256="sha256:" + "e" * 64,
                ),
                worker_contract_hash="contract123",
            ),
        )

    task_id = canonical_sha256({"execution": "unit-execution", "stage": "05-anchor-pair-probes"})
    assert journal.load()["tasks"][task_id]["status"] == "pending"
    assert journal.terminal(task_id) is None


def test_pending_probe_stage_quarantines_probe_outputs_without_moving_inputs(
    tmp_path: Path,
) -> None:
    source_dir, _result_dir = _write_anchor_pair_probe_fixture(tmp_path)
    spec = ProbeRunSpec(
        kind="anchor_pair",
        source_dir=source_dir,
        atlas_filename="anchor-pair-atlas.json",
        results_filename="anchor-pair-probe-results.csv",
        summary_filename="anchor-pair-probe-summary.json",
        manifest_schema="anchor_pair_run_manifest_v1",
        result_fieldnames=_probe_results_fieldnames,
        row_builder=_result_row_from_score,
    )
    run_root = source_dir.parent
    journal = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="unit-execution",
        lineage={},
    )
    stage = "05-anchor-pair-probes"
    task_id = canonical_sha256({"execution": "unit-execution", "stage": stage})
    journal.register(task_id, {"stage": stage})

    stale_probe_dir = source_dir / "probe-results" / "probe-1"
    stale_probe_dir.mkdir(parents=True, exist_ok=True)
    (stale_probe_dir / "sensitivity-response.json").write_text(
        json.dumps({"status": "stale"}),
        encoding="utf-8",
    )
    (source_dir / "anchor-pair-probe-results.csv").write_text("status\nstale\n", encoding="utf-8")
    (source_dir / "anchor-pair-probe-summary.json").write_text(
        json.dumps({"status": "stale"}),
        encoding="utf-8",
    )
    (source_dir / "missing-manifest").mkdir()
    (source_dir / "missing-manifest" / "stale.json").write_text("{}", encoding="utf-8")

    def action() -> dict[str, Any]:
        assert (source_dir / "anchor-pair-atlas.json").exists()
        assert (source_dir / "profiles" / "probe-1.json").exists()
        assert not (source_dir / "probe-results").exists()
        assert not (source_dir / "anchor-pair-probe-results.csv").exists()
        assert not (source_dir / "anchor-pair-probe-summary.json").exists()
        fresh_dir = source_dir / "probe-results" / "probe-1"
        fresh_dir.mkdir(parents=True)
        (fresh_dir / "sensitivity-response.json").write_text(
            json.dumps({"status": "fresh"}),
            encoding="utf-8",
        )
        (source_dir / "anchor-pair-probe-results.csv").write_text("status\nok\n", encoding="utf-8")
        (source_dir / "anchor-pair-probe-summary.json").write_text(
            json.dumps({"status": "fresh"}),
            encoding="utf-8",
        )
        return {"ok": True}

    result, reused = _run_durable_atlas_stage(
        journal=journal,
        run_root=run_root,
        stage=stage,
        payload={"stage": stage},
        artifact_roots=_probe_artifact_roots(spec),
        action=action,
    )

    assert result == {"ok": True}
    assert reused is False
    assert journal.terminal(task_id) is not None
    assert (source_dir / "anchor-pair-atlas.json").exists()
    assert (source_dir / "profiles" / "probe-1.json").exists()
    partial_roots = list((run_root / "partial-stages").glob(f"{stage}-*"))
    assert partial_roots
    moved_names = {path.name for path in partial_roots[0].iterdir()}
    assert "00-anchor-pair-probe-results.csv" in moved_names
    assert "01-anchor-pair-probe-summary.json" in moved_names
    assert "02-probe-results" in moved_names
    assert "04-missing-manifest" in moved_names
    assert not any("profiles" in name for name in moved_names)


def test_audit_or_rewind_atlas_lab_stages_marks_boundary_forward_pending(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "derived" / "atlas-runs" / "unit"
    journal = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="unit-execution",
        lineage={},
    )
    for stage in ["04-anchor-pair-atlas", "05-anchor-pair-probes", "06-anchor-pair-timing-atlas"]:
        task_id = canonical_sha256({"execution": "unit-execution", "stage": stage})
        journal.register(task_id, {"stage": stage})
        artifact_dir = run_root / stage
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / "artifact.json"
        artifact_path.write_text(json.dumps({"stage": stage}), encoding="utf-8")
        receipt = artifact_receipt([artifact_path], root=run_root)
        journal.complete(task_id, {"stage": stage, "artifact_receipt": receipt})

    dry_run = audit_or_rewind_atlas_lab_stages(
        run_root=run_root,
        execution_id="unit-execution",
        from_stage="05-anchor-pair-probes",
        apply=False,
    )
    assert [row["stage"] for row in dry_run["rewound"]] == [
        "05-anchor-pair-probes",
        "06-anchor-pair-timing-atlas",
    ]

    applied = audit_or_rewind_atlas_lab_stages(
        run_root=run_root,
        execution_id="unit-execution",
        from_stage="05-anchor-pair-probes",
        apply=True,
    )
    assert len(applied["rewound"]) == 2
    assert journal.terminal(canonical_sha256({"execution": "unit-execution", "stage": "04-anchor-pair-atlas"}))
    reopened = DurableExecutionJournal(
        run_root / "execution-journal.json",
        execution_id="unit-execution",
        lineage={},
    )
    assert reopened.terminal(
        canonical_sha256({"execution": "unit-execution", "stage": "05-anchor-pair-probes"})
    ) is None
    assert reopened.terminal(
        canonical_sha256({"execution": "unit-execution", "stage": "06-anchor-pair-timing-atlas"})
    ) is None


def test_historical_runtime_cutoff_rejects_mismatched_probe_manifest_cutoff() -> None:
    with pytest.raises(ValueError, match="does not match runtime cutoff"):
        _deep_replay_request_from_probe(
            probe_id="probe-1",
            profile_payload=_profile_doc()["profile"],
            manifest_probe={
                "sensitivity_basket_args": [
                    "sensitivity-basket",
                    "--timeframe",
                    "M5",
                    "--lookback-months",
                    "3",
                    "--as-of-date",
                    "2025-06-29T00:00:00Z",
                    "--instrument",
                    "EURUSD",
                ]
            },
            row={"probe_timeframe": "M5", "instruments": "EURUSD"},
            runtime=AtlasLabRuntimeConfig(as_of_date="2025-06-30T00:00:00Z"),
            worker_contract_hash="contract123",
        )


def test_historical_probe_rejects_unverified_existing_result_fallback(tmp_path: Path) -> None:
    config = _config(tmp_path)
    source_dir = tmp_path / "runs" / "derived" / "atlas-runs" / "test" / "anchor-pair-atlas"
    profile_dir = source_dir / "profiles"
    result_dir = source_dir / "probe-results" / "probe-1"
    profile_dir.mkdir(parents=True)
    result_dir.mkdir(parents=True)
    profile_path = profile_dir / "probe-1.json"
    profile_path.write_text(json.dumps(_profile_doc()), encoding="utf-8")
    (result_dir / "sensitivity-response.json").write_text("{}", encoding="utf-8")
    (source_dir / "anchor-pair-atlas.json").write_text(
        json.dumps(
            {
                "queue_rows": [
                    {
                        "probe_id": "probe-1",
                        "queue_rank": 1,
                        "anchor_type": "trend",
                        "anchor_id": "ANCHOR",
                        "trigger_id": "TRIGGER",
                        "probe_timeframe": "M5",
                        "pair_prior_score": 80,
                        "pair_prior_bucket": "probe_now",
                        "instruments": "EURUSD",
                    }
                ],
                "run_manifest": {
                    "probes": [
                        {
                            "probe_id": "probe-1",
                            "profile_path": str(profile_path),
                            "output_dir": str(result_dir),
                            "sensitivity_basket_args": [
                                "sensitivity-basket",
                                "--timeframe",
                                "M5",
                                "--lookback-months",
                                "3",
                                "--instrument",
                                "EURUSD",
                            ],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="cannot reuse an existing result"):
        run_probe_spec_via_gateway(
            config,
            spec=ProbeRunSpec(
                kind="anchor_pair",
                source_dir=source_dir,
                atlas_filename="anchor-pair-atlas.json",
                results_filename="anchor-pair-probe-results.csv",
                summary_filename="anchor-pair-probe-summary.json",
                manifest_schema="anchor_pair_run_manifest_v1",
                result_fieldnames=_probe_results_fieldnames,
                row_builder=_result_row_from_score,
            ),
            gateway=FakeGateway(),
            runtime=AtlasLabRuntimeConfig(
                active_probes=1,
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "f" * 64,
            ),
            worker_contract_hash="contract123",
        )


def test_enqueue_gateway_tasks_rejects_partial_acceptance() -> None:
    class PartialGateway:
        def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
            return {"status": "accepted", "submitted": len(tasks), "accepted": 1, "rejected": 1}

    with pytest.raises(RuntimeError, match="accepted 1 of 2"):
        _enqueue_gateway_tasks_with_retries(
            PartialGateway(),
            [{"task_id": "a"}, {"task_id": "b"}],
        )


def test_historical_signal_gateway_worker_failure_fails_closed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    indicator_dir = tmp_path / "indicator-atlas"
    indicator_dir.mkdir()
    (indicator_dir / "indicator-atlas.json").write_text(
        json.dumps(
            {
                "indicators": [
                    {
                        "id": "RSI",
                        "signal_role": "trigger",
                        "strategy_role": "trigger",
                        "static_prior_score": 75.0,
                        "static_prior_bucket": "candidate",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "autoresearch.atlas_lab.load_indicator_catalog",
        lambda **kwargs: (
            {
                "indicators": [
                    {
                        "meta": {"id": "RSI"},
                        "config": {"label": "RSI", "timeframe": "M5"},
                    }
                ]
            },
            tmp_path,
            tmp_path / "catalog.json",
        ),
    )

    class FailedSignalGateway:
        def __init__(self) -> None:
            self.enqueued_tasks: list[dict[str, Any]] = []
            self.results: list[dict[str, Any]] = []

        def snapshot(self) -> dict[str, Any]:
            return {
                "gateway_id": "failed-signal-gateway",
                "worker_slots": 1,
                "busy_slots": 0,
                "queued_tasks": 0,
                "result_backlog": len(self.results),
                "metrics": {"results_dropped": 0},
            }

        def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
            self.enqueued_tasks.extend(tasks)
            for task in tasks:
                self.results.append(
                    {
                        "task_id": task["task_id"],
                        "lease_id": f"lease-{len(self.results) + 1}",
                        "status": "failed",
                        "result": {
                            "status": "failed",
                            "error": (
                                "EvidencePlanValidationError: Resolved profile "
                                "snapshot SHA-256 does not match the evidence plan."
                            ),
                        },
                    }
                )
            return {"status": "accepted", "enqueued": len(tasks)}

        def read_results(self, *, limit: int) -> list[dict[str, Any]]:
            batch = self.results[:limit]
            self.results = self.results[limit:]
            return batch

        def ack_results(self, lease_ids: list[str]) -> int:
            return len(lease_ids)

    gateway = FailedSignalGateway()
    with pytest.raises(RuntimeError, match="Historical signal atlas.*EvidencePlanValidationError"):
        build_signal_atlas_via_gateway(
            config,
            indicator_atlas_dir=indicator_dir,
            out_dir=tmp_path / "signal-atlas",
            signal_role="trigger",
            instruments=["EURUSD"],
            timeframes=["M5"],
            max_indicators=1,
            gateway=gateway,
            runtime=AtlasLabRuntimeConfig(
                active_probes=1,
                result_batch_size=1,
                max_results_per_cycle=1,
                as_of_date="2025-06-30T00:00:00Z",
                lake_manifest_sha256="sha256:" + "f" * 64,
            ),
            worker_contract_hash="contract123",
        )

    assert len(gateway.enqueued_tasks) == 1
    assert not (tmp_path / "signal-atlas" / "signal-atlas.json").exists()


def test_run_probe_spec_via_gateway_reads_timing_queue_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir = tmp_path / "runs" / "derived" / "atlas-runs" / "test" / "anchor-pair-timing-atlas"
    profile_dir = source_dir / "profiles"
    result_dir = source_dir / "probe-results" / "timing-1"
    profile_dir.mkdir(parents=True)
    profile_path = profile_dir / "timing-1.json"
    profile_path.write_text(
        '{"format":"fuzzfolio.scoring-profile","profile":{"name":"Atlas Timing Test","directionMode":"both","notificationThreshold":83,"indicators":[],"instruments":["EURUSD"]}}',
        encoding="utf-8",
    )
    atlas_payload = {
        "timing_queue_rows": [
            {
                "timing_probe_id": "timing-1",
                "probe_id": "timing-1",
                "timing_rank": 1,
                "base_probe_id": "base-1",
                "base_queue_rank": 4,
                "variant_side": "anchor",
                "variant_indicator_id": "ANCHOR",
                "variant_lookback_bars": 8,
                "anchor_variant_lookback_bars": 8,
                "anchor_type": "trend",
                "anchor_id": "ANCHOR",
                "trigger_id": "TRIGGER",
                "probe_timeframe": "M5",
                "pair_prior_score": 80,
                "pair_prior_bucket": "probe_now",
                "baseline_status": "ok",
                "baseline_composite_score": 60,
                "instruments": "EURUSD",
            }
        ],
        "run_manifest": {
            "probes": [
                {
                    "probe_id": "timing-1",
                    "timing_probe_id": "timing-1",
                    "profile_path": str(profile_path),
                    "output_dir": str(result_dir),
                    "sensitivity_basket_args": [
                        "sensitivity-basket",
                        "--profile-ref",
                        "<PROFILE_ID>",
                        "--timeframe",
                        "M5",
                        "--lookback-months",
                        "12",
                        "--instrument",
                        "EURUSD",
                        "--output-dir",
                        str(result_dir),
                    ],
                }
            ]
        },
    }
    (source_dir / "anchor-pair-timing-atlas.json").write_text(
        json.dumps(atlas_payload),
        encoding="utf-8",
    )

    def fake_score_artifact(self, artifact_dir: Path) -> dict[str, Any]:
        return {
            "best": {
                "score_lab": {
                    "version": "score_lab_v2_5_3",
                    "score": 66.0,
                    "combiner": "canonical",
                },
                "trades": 14,
            }
        }

    monkeypatch.setattr(FuzzfolioCli, "score_artifact", fake_score_artifact)

    outcome = run_probe_spec_via_gateway(
        config,
        spec=ProbeRunSpec(
            kind="anchor_pair_timing",
            source_dir=source_dir,
            atlas_filename="anchor-pair-timing-atlas.json",
            results_filename="anchor-pair-timing-results.csv",
            summary_filename="anchor-pair-timing-summary.json",
            manifest_schema="anchor_pair_timing_run_manifest_v1",
            result_fieldnames=_timing_results_fieldnames,
            row_builder=_timing_result_row_from_score,
            queue_key="timing_queue_rows",
        ),
        gateway=FakeGateway(),
        runtime=AtlasLabRuntimeConfig(active_probes=1, result_batch_size=10),
        worker_contract_hash="contract123",
    )

    assert outcome.summary["result_counts"]["selected"] == 1
    assert outcome.summary["result_counts"]["completed"] == 1
    assert outcome.summary["result_counts"]["scored"] == 1
    results_text = (source_dir / "anchor-pair-timing-results.csv").read_text(encoding="utf-8")
    assert "anchor" in results_text
    assert results_text.count("\n") == 2


def test_run_probe_spec_via_gateway_skips_existing_without_double_count(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    source_dir = tmp_path / "runs" / "derived" / "atlas-runs" / "test" / "anchor-pair-atlas"
    profile_dir = source_dir / "profiles"
    result_dir = source_dir / "probe-results" / "probe-1"
    profile_dir.mkdir(parents=True)
    result_dir.mkdir(parents=True)
    profile_path = profile_dir / "probe-1.json"
    profile_path.write_text(
        '{"format":"fuzzfolio.scoring-profile","profile":{"name":"Atlas Test","directionMode":"both","notificationThreshold":83,"indicators":[],"instruments":["EURUSD"]}}',
        encoding="utf-8",
    )
    (result_dir / "sensitivity-response.json").write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "score_lab": {
                            "version": "score_lab_v2_5_3",
                            "score": 61.0,
                            "combiner": "canonical",
                        },
                        "quality_score": {"score": 61.0},
                        "best_cell": {
                            "avg_net_r_per_closed_trade": 0.2,
                            "resolved_trades": 10,
                            "profit_factor": 1.3,
                        },
                        "behavior_summary": {"signal_count": 12},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    atlas_payload = {
        "queue_rows": [
            {
                "probe_id": "probe-1",
                "queue_rank": 1,
                "anchor_type": "trend",
                "anchor_id": "ANCHOR",
                "trigger_id": "TRIGGER",
                "probe_timeframe": "M5",
                "pair_prior_score": 80,
                "pair_prior_bucket": "probe_now",
                "instruments": "EURUSD",
            }
        ],
        "run_manifest": {
            "probes": [
                {
                    "probe_id": "probe-1",
                    "profile_path": str(profile_path),
                    "output_dir": str(result_dir),
                    "sensitivity_basket_args": [
                        "sensitivity-basket",
                        "--profile-ref",
                        "<PROFILE_ID>",
                        "--timeframe",
                        "M5",
                        "--lookback-months",
                        "12",
                        "--instrument",
                        "EURUSD",
                        "--output-dir",
                        str(result_dir),
                    ],
                }
            ]
        },
    }
    (source_dir / "anchor-pair-atlas.json").write_text(json.dumps(atlas_payload), encoding="utf-8")

    def fake_score_artifact(self, artifact_dir: Path) -> dict[str, Any]:
        return {
            "best": {
                "score_lab": {
                    "version": "score_lab_v2_5_3",
                    "score": 61.0,
                    "combiner": "canonical",
                },
                "trades": 10,
            }
        }

    gateway = FakeGateway()
    monkeypatch.setattr(FuzzfolioCli, "score_artifact", fake_score_artifact)

    outcome = run_probe_spec_via_gateway(
        config,
        spec=ProbeRunSpec(
            kind="anchor_pair",
            source_dir=source_dir,
            atlas_filename="anchor-pair-atlas.json",
            results_filename="anchor-pair-probe-results.csv",
            summary_filename="anchor-pair-probe-summary.json",
            manifest_schema="anchor_pair_run_manifest_v1",
            result_fieldnames=_probe_results_fieldnames,
            row_builder=_result_row_from_score,
        ),
        gateway=gateway,
        runtime=AtlasLabRuntimeConfig(active_probes=1, result_batch_size=10),
        worker_contract_hash="contract123",
    )

    assert gateway.enqueued_tasks == []
    assert outcome.summary["result_counts"]["selected"] == 1
    assert outcome.summary["result_counts"]["completed"] == 1
    assert outcome.summary["result_counts"]["status_counts"] == {"skipped_existing": 1}
    csv_text = (source_dir / "anchor-pair-probe-results.csv").read_text(encoding="utf-8")
    assert csv_text.count("\n") == 2
    assert "skipped_existing_unscored" not in csv_text


def test_run_atlas_lab_build_phase_writes_json_safe_metadata(tmp_path: Path, monkeypatch) -> None:
    config = _config(tmp_path)
    called_builders: list[str] = []
    signal_kwargs: list[dict[str, Any]] = []

    def fake_builder(name: str):
        def _inner(*args, **kwargs) -> None:
            called_builders.append(name)
            if name == "signal":
                signal_kwargs.append(dict(kwargs))

        return _inner

    monkeypatch.setattr("autoresearch.atlas_lab.build_indicator_atlas", fake_builder("indicator"))
    monkeypatch.setattr("autoresearch.atlas_lab.build_signal_atlas", fake_builder("signal"))
    monkeypatch.setattr("autoresearch.atlas_lab.build_forward_response_atlas", fake_builder("forward"))
    monkeypatch.setattr("autoresearch.atlas_lab.build_anchor_pair_atlas", fake_builder("anchor_pair"))
    monkeypatch.setattr("autoresearch.atlas_lab.resolve_atlas_worker_contract_hash", lambda **kwargs: "contract123")

    result = run_atlas_lab(
        config,
        run_id="unit-run",
        runtime=AtlasLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            atlas_profile="rich",
        ),
        phases=["build"],
    )

    metadata_path = result.run_root / "atlas-lab-run.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert result.status == "completed"
    assert called_builders == ["indicator", "signal", "forward", "anchor_pair"]
    assert metadata["run_id"] == "unit-run"
    assert metadata["runtime"]["gateway_url"] == "http://127.0.0.1:8799"
    assert metadata["runtime"]["atlas_profile"] == "rich"
    assert metadata["atlas_profile"]["name"] == "rich"
    assert signal_kwargs[0]["signal_role"] == "trigger,setup,context,filter"
    assert "US500" not in signal_kwargs[0]["instruments"]
    assert "ETHUSD" not in signal_kwargs[0]["instruments"]
    assert signal_kwargs[0]["timeframes"] == ["M1", "M5", "M15", "H1"]
    assert metadata["paths"]["run_root"] == str(result.run_root)


def test_run_atlas_lab_build_phase_uses_bounded_signal_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    signal_kwargs: list[dict[str, Any]] = []

    def fake_builder(*args, **kwargs) -> None:
        if "signal_role" in kwargs:
            signal_kwargs.append(dict(kwargs))

    monkeypatch.setattr("autoresearch.atlas_lab.build_indicator_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_signal_atlas", fake_builder)
    monkeypatch.setattr("autoresearch.atlas_lab.build_forward_response_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_anchor_pair_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.resolve_atlas_worker_contract_hash", lambda **kwargs: "contract123")

    result = run_atlas_lab(
        config,
        run_id="unit-bounded-build",
        runtime=AtlasLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            atlas_profile="rich",
            signal_max_indicators=2,
            signal_instrument_limit=3,
            signal_timeframe_limit=2,
        ),
        phases=["build"],
    )

    metadata = json.loads((result.run_root / "atlas-lab-run.json").read_text(encoding="utf-8"))
    assert result.status == "completed"
    assert signal_kwargs[0]["max_indicators"] == 2
    assert signal_kwargs[0]["instruments"] == atlas_profile_config("rich")["signal_instruments"][:3]
    assert signal_kwargs[0]["timeframes"] == ["M1", "M5"]
    assert metadata["effective_build_profile"]["bounded_for_smoke"] is True


def test_run_atlas_lab_build_phase_can_use_gateway_signal_executor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    signal_kwargs: list[dict[str, Any]] = []

    def fake_gateway_builder(*args, **kwargs) -> None:
        signal_kwargs.append(dict(kwargs))

    def fail_local_builder(*args, **kwargs) -> None:
        raise AssertionError("local signal atlas builder should not be used")

    monkeypatch.setattr("autoresearch.atlas_lab.build_indicator_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_signal_atlas", fail_local_builder)
    monkeypatch.setattr("autoresearch.atlas_lab.build_signal_atlas_via_gateway", fake_gateway_builder)
    monkeypatch.setattr("autoresearch.atlas_lab.build_forward_response_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_anchor_pair_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.resolve_atlas_worker_contract_hash", lambda **kwargs: "contract123")

    result = run_atlas_lab(
        config,
        run_id="unit-gateway-signal-build",
        runtime=AtlasLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            atlas_profile="rich",
            signal_atlas_executor="gateway",
        ),
        phases=["build"],
        gateway=FakeGateway(),
    )

    metadata = json.loads((result.run_root / "atlas-lab-run.json").read_text(encoding="utf-8"))
    assert result.status == "completed"
    assert metadata["runtime"]["signal_atlas_executor"] == "gateway"
    assert signal_kwargs[0]["gateway"].__class__ is FakeGateway
    assert signal_kwargs[0]["worker_contract_hash"] == "contract123"
    assert signal_kwargs[0]["signal_role"] == "trigger,setup,context,filter"


def test_atlas_rich_profile_declares_broader_observation_surface() -> None:
    standard = atlas_profile_config("standard")
    rich_roles = atlas_profile_config("rich-roles")
    rich_timeframes = atlas_profile_config("rich-timeframes")
    rich_markets = atlas_profile_config("rich-markets")
    rich = atlas_profile_config("rich")
    rich_discovery = atlas_profile_config("rich-discovery")
    rich_plus_discovery = atlas_profile_config("rich-plus-discovery")

    assert standard["signal_roles"] == ["trigger"]
    assert rich_roles["signal_roles"] == ["trigger", "setup", "context", "filter"]
    assert rich_roles["signal_instruments"] == standard["signal_instruments"]
    assert rich_roles["signal_timeframes"] == standard["signal_timeframes"]
    assert rich_roles["timing_variant_sides"] == ["trigger", "anchor", "both"]
    assert rich_timeframes["signal_instruments"] == standard["signal_instruments"]
    assert rich_timeframes["signal_timeframes"] == ["M1", "M5", "M15", "H1"]
    assert len(rich_markets["signal_instruments"]) > len(standard["signal_instruments"])
    assert rich_markets["signal_timeframes"] == standard["signal_timeframes"]
    assert rich["signal_roles"] == ["trigger", "setup", "context", "filter"]
    assert standard["timing_variant_sides"] == ["trigger", "anchor"]
    assert rich["timing_variant_sides"] == ["trigger", "anchor", "both"]
    assert len(rich["signal_instruments"]) > len(standard["signal_instruments"])
    assert {"EURUSD", "XAUUSD", "XTIUSD", "DE40", "US30"}.issubset(
        set(rich["signal_instruments"])
    )
    assert "SOLUSD" not in rich_markets["signal_instruments"]
    assert "SOLUSD" not in rich["signal_instruments"]
    assert "SOLUSD" not in rich_discovery["signal_instruments"]
    assert "SOLUSD" not in rich_plus_discovery["signal_instruments"]
    assert rich["signal_timeframes"] == ["M1", "M5", "M15", "H1"]
    assert rich["discovery_timeframes"] == standard["discovery_timeframes"]
    assert rich["discovery_instruments"] == standard["discovery_instruments"]
    assert rich_discovery["signal_roles"] == rich["signal_roles"]
    assert rich_discovery["signal_instruments"] == rich["signal_instruments"]
    assert rich_discovery["signal_timeframes"] == standard["signal_timeframes"]
    assert rich_discovery["discovery_instruments"] == standard["discovery_instruments"]
    assert rich_discovery["discovery_timeframes"] == [
        "M1",
        "M5",
        "M15",
        "M30",
        "H1",
        "H4",
        "D1",
    ]
    assert rich_plus_discovery["signal_roles"] == rich["signal_roles"]
    assert rich_plus_discovery["signal_instruments"] == rich["signal_instruments"]
    assert rich_plus_discovery["signal_timeframes"] == rich["signal_timeframes"]
    assert rich_plus_discovery["discovery_instruments"] == standard["discovery_instruments"]
    assert rich_plus_discovery["discovery_timeframes"] == rich_discovery["discovery_timeframes"]


def test_atlas_lab_default_profile_is_current_winner() -> None:
    assert DEFAULT_ATLAS_PROFILE == "rich"
    assert AtlasLabRuntimeConfig().atlas_profile == "rich"
    assert atlas_profile_config(None)["name"] == "rich"


def test_effective_atlas_build_profile_can_bound_rich_surface_for_smoke() -> None:
    rich = atlas_profile_config("rich")
    runtime = AtlasLabRuntimeConfig(
        atlas_profile="rich",
        signal_max_indicators=2,
        signal_instrument_limit=3,
        signal_timeframe_limit=2,
    )

    effective = effective_atlas_build_profile(rich, runtime)

    assert effective["bounded_for_smoke"] is True
    assert effective["signal_max_indicators"] == 2
    assert effective["signal_instruments"] == rich["signal_instruments"][:3]
    assert effective["signal_timeframes"] == ["M1", "M5"]
    assert rich["signal_timeframes"] == ["M1", "M5", "M15", "H1"]


def test_run_atlas_lab_probes_phase_passes_profile_discovery_panel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    discovery_calls: list[dict[str, Any]] = []

    def fake_probe_runner(*args, **kwargs):
        spec = kwargs["spec"]
        return SimpleNamespace(summary={"kind": spec.kind, "status": "completed"})

    def fake_build_discovery_pair_atlas(*args, **kwargs) -> None:
        discovery_calls.append(dict(kwargs))

    monkeypatch.setattr("autoresearch.atlas_lab.resolve_atlas_worker_contract_hash", lambda **kwargs: "contract123")
    monkeypatch.setattr("autoresearch.atlas_lab.run_probe_spec_via_gateway", fake_probe_runner)
    monkeypatch.setattr("autoresearch.atlas_lab.build_anchor_pair_timing_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_pair_atlas", fake_build_discovery_pair_atlas)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_cluster_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_recipe_validation_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_recipe_scrutiny_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_recipe_priors", lambda *args, **kwargs: None)

    result = run_atlas_lab(
        config,
        run_id="unit-rich-discovery-probes",
        runtime=AtlasLabRuntimeConfig(
            gateway_url="http://127.0.0.1:8799",
            atlas_profile="rich-discovery",
        ),
        phases=["probes"],
        gateway=object(),
    )

    assert result.status == "completed"
    assert len(discovery_calls) == 1
    assert discovery_calls[0]["instruments"] == atlas_profile_config("standard")[
        "discovery_instruments"
    ]
    assert discovery_calls[0]["timeframes"] == [
        "M1",
        "M5",
        "M15",
        "M30",
        "H1",
        "H4",
        "D1",
    ]
    assert discovery_calls[0]["full_queue"] is True


def test_run_atlas_lab_recipe_priors_disable_playhand_feedback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    recipe_prior_calls: list[dict[str, Any]] = []

    def fake_probe_runner(*args, **kwargs):
        spec = kwargs["spec"]
        return SimpleNamespace(summary={"kind": spec.kind, "status": "completed"})

    def fake_build_recipe_priors(*args, **kwargs) -> None:
        recipe_prior_calls.append(dict(kwargs))

    monkeypatch.setattr("autoresearch.atlas_lab.resolve_atlas_worker_contract_hash", lambda **kwargs: "contract123")
    monkeypatch.setattr("autoresearch.atlas_lab.run_probe_spec_via_gateway", fake_probe_runner)
    monkeypatch.setattr("autoresearch.atlas_lab.build_anchor_pair_timing_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_pair_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_cluster_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_recipe_validation_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_discovery_recipe_scrutiny_atlas", lambda *args, **kwargs: None)
    monkeypatch.setattr("autoresearch.atlas_lab.build_recipe_priors", fake_build_recipe_priors)

    result = run_atlas_lab(
        config,
        run_id="unit-probes",
        runtime=AtlasLabRuntimeConfig(gateway_url="http://127.0.0.1:8799"),
        phases=["probes"],
        gateway=object(),
    )

    assert result.status == "completed"
    assert len(recipe_prior_calls) == 2
    assert all(call["include_playhand_outcome_priors"] is False for call in recipe_prior_calls)


def test_atlas_lab_cli_json_includes_pipeline_summaries(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from autoresearch import __main__ as cli

    monkeypatch.setattr(cli, "load_config", lambda: _config(tmp_path))
    monkeypatch.setattr(
        cli,
        "run_atlas_lab",
        lambda *args, **kwargs: SimpleNamespace(
            run_id="unit-atlas",
            run_root=tmp_path / "runs" / "derived" / "atlas-runs" / "unit-atlas",
            status="completed",
            summary_path=tmp_path / "summary.json",
            published_manifest_path=None,
            probe_summaries=[],
            pipeline_summaries=[
                {
                    "stage": "discovery_cluster",
                    "summary": {"result_counts": {"discovered_recipes": 12}},
                }
            ],
        ),
    )

    exit_code = cli.cmd_atlas_lab(
        run_id="unit-atlas",
        gateway_url="http://127.0.0.1:8799",
        gateway_token=None,
        trading_dashboard_root=None,
        atlas_profile="standard",
        worker_contract_hash=None,
        phases=["probes"],
        active_probes=1,
        enqueue_chunk_size=1,
        result_batch_size=1,
        max_results_per_cycle=1,
        max_drain_seconds=0.1,
        poll_interval_seconds=0.1,
        deadline_seconds=60,
        max_attempts=1,
        log_interval_seconds=1,
        limit=1,
        signal_max_indicators=None,
        signal_instrument_limit=None,
        signal_timeframe_limit=None,
        as_of_date=None,
        discovery_queue="default",
        discovery_cluster_min_similarity=0.5,
        discovery_cluster_min_shared_partners=1,
        discovery_cluster_max_recipes=32,
        discovery_validation_confidence="high_candidate,promising_candidate",
        discovery_validation_instruments=None,
        discovery_validation_timeframes=None,
        discovery_validation_max_recipes=8,
        discovery_validation_max_pairs_per_recipe=8,
        discovery_validation_first_member_limit=6,
        discovery_validation_second_member_limit=6,
        discovery_validation_diversity_penalty_scale=18.0,
        force=False,
        include_detail=True,
        compact_probe_artifacts=True,
        strict_parity=True,
        publish=False,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["pipeline_summaries"][0]["stage"] == "discovery_cluster"
    assert payload["pipeline_summaries"][0]["summary"]["result_counts"]["discovered_recipes"] == 12


def test_formal_atlas_cli_constructs_runtime_from_authority_plan(
    tmp_path: Path, monkeypatch
) -> None:
    from autoresearch import __main__ as cli
    from autoresearch import level_c_operator

    plan_path = tmp_path / "execution-plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    contract = "sha256:" + "a" * 64
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_config", lambda: _config(tmp_path))
    monkeypatch.setattr(
        level_c_operator,
        "executor_arguments_from_plan",
        lambda *_args, **_kwargs: (
            {
                "run_id": "formal-atlas",
                "as_of_date": "2026-01-01",
                "worker_contract_hash": contract,
                "signal_atlas_executor": "gateway",
                "publish": False,
                "execution_plan_path": str(plan_path.resolve()),
                "execution_plan_id": "sha256:" + "b" * 64,
            },
            {"atlas_phases": ["full"]},
        ),
    )

    def fake_run(*_args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            run_id="formal-atlas",
            run_root=tmp_path,
            status="completed",
            summary_path=tmp_path / "summary.json",
            published_manifest_path=None,
            probe_summaries=[],
            pipeline_summaries=[],
        )

    monkeypatch.setattr(cli, "run_atlas_lab", fake_run)
    assert cli.cmd_atlas_lab(
        run_id=None, gateway_url=None, gateway_token=None, trading_dashboard_root=None,
        atlas_profile="standard", worker_contract_hash=None, phases=None, active_probes=1,
        enqueue_chunk_size=1, result_batch_size=1, max_results_per_cycle=1,
        max_drain_seconds=0.1, poll_interval_seconds=0.1, deadline_seconds=60,
        max_attempts=1, log_interval_seconds=1, limit=None, signal_max_indicators=None,
        signal_instrument_limit=None, signal_timeframe_limit=None, as_of_date=None,
        execution_plan=plan_path, discovery_queue="full",
        discovery_cluster_min_similarity=0.5, discovery_cluster_min_shared_partners=1,
        discovery_cluster_max_recipes=32,
        discovery_validation_confidence="high_candidate,promising_candidate",
        discovery_validation_instruments=None, discovery_validation_timeframes=None,
        discovery_validation_max_recipes=8, discovery_validation_max_pairs_per_recipe=8,
        discovery_validation_first_member_limit=6, discovery_validation_second_member_limit=6,
        discovery_validation_diversity_penalty_scale=18.0, force=False,
        include_detail=True, compact_probe_artifacts=True, strict_parity=True,
        publish=False, as_json=True,
    ) == 0
    runtime = captured["runtime"]
    assert runtime.worker_contract_hash == contract
    assert captured["phases"] == ["full"]


def test_rewind_atlas_lab_stages_cli_arguments_parse(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "rewind-atlas-lab-stages",
            "--run-root",
            str(tmp_path),
            "--execution-id",
            "unit-execution",
            "--from-stage",
            "05-anchor-pair-probes",
            "--json",
        ]
    )

    assert args.command == "rewind-atlas-lab-stages"
    assert args.run_root == tmp_path
    assert args.execution_id == "unit-execution"
    assert args.from_stage == "05-anchor-pair-probes"
    assert args.apply is False
    assert args.json is True
