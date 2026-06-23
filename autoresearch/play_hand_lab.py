from __future__ import annotations

import copy
import concurrent.futures
import itertools
import json
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import requests
from rich.console import Console

from .play_hand_lab_auth import load_lab_gateway_token
from .config import AppConfig, load_config
from .fuzzfolio import CliError, FuzzfolioCli
from .ledger import (
    attempts_path_for_run_dir,
    load_attempts,
    load_run_metadata,
    make_attempt_record,
    write_attempts,
    write_run_metadata,
)
from .play_hand import (
    DEFAULT_INSTRUMENT_POOL,
    INSTRUMENT_SCOUT_DEFAULT_MAX_SELECTED,
    INSTRUMENT_SCOUT_DEFAULT_SIZE,
    INSTRUMENT_SCOUT_MIN_SCORE,
    INSTRUMENT_SCOUT_SCORE_TOLERANCE,
    PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET,
    PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON,
    PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON,
    PLAY_HAND_FINAL_SCRUTINY_MIN_SCORE,
    PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    PLAY_HAND_RUNNER,
    PLAY_HAND_SWEEP_PERMUTATION_LIMIT,
    PlayHandContext,
    SeedIndicator,
    _append_event,
    _as_float,
    _fallback_indicator_deal,
    _final_scrutiny_outcome,
    _load_json,
    _load_play_hand_seed_plan,
    _lowest_profile_timeframe,
    _merge_seed_indicator_candidates,
    _scaffold_profile,
    _seed_hand,
    _seed_plan_indicator_candidates,
    _seed_pair_template_instruments,
    _seed_plan_template_instrument_policy,
    _utc_stamp,
    _write_json,
    _play_hand_role_for_phase,
    apply_play_hand_profile_defaults,
    apply_role_timeframe_defaults,
    apply_seed_indicator_metadata,
    apply_seed_pair_template_defaults,
    build_coarse_axes,
    build_coarse_halving_decision,
    build_early_exit_decision,
    build_focused_axes,
    build_stage_acceptance_decision,
    build_timing_axes,
    deal_indicator_count,
    deal_instruments,
    deal_seed_plan_indicators,
    materialize_profile_variant,
    plan_sweep_axes,
    play_hand_reward_matrix,
    resolve_instrument_pool_presets,
    resolve_sweep_budget,
)
from .playhand_health import build_play_hand_evidence, build_play_hand_health
from .plotting import render_progress_artifacts
from .scoring import AttemptScore, build_attempt_score, load_sensitivity_snapshot


console = Console(safe_box=True)

PLAY_HAND_LAB_RUNNER = "play_hand_lab_v1"
PLAY_HAND_LAB_CAMPAIGN_SCHEMA_VERSION = "play_hand_lab_campaign_v1"
PLAY_HAND_LAB_LANE_SCHEMA_VERSION = "play_hand_lab_lane_v1"
PLAY_HAND_LAB_CAMPAIGNS_DIR = "play-hand-lab-campaigns"
PLAY_HAND_LAB_WORKER_PROTOCOL_VERSION = "playhand-lab-worker-v1"
PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY = f"playhand_lab_protocol:{PLAY_HAND_LAB_WORKER_PROTOCOL_VERSION}"
PLAY_HAND_LAB_FAKE_COMPUTE_CAPABILITY = "fake_compute"
SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT = 1000
DEFAULT_LAB_GATEWAY_URL = "http://127.0.0.1:8799"
DEFAULT_LAB_USER_ID = "autoresearch-lab"
DEFAULT_LAB_ACTIVE_RUNS = 64
DEFAULT_LAB_RESULT_BATCH_SIZE = 25
DEFAULT_LAB_RESULT_READ_FAILURE_LIMIT = 5
DEFAULT_LAB_SWEEP_SHARD_SIZE = 8
DEFAULT_LAB_SCRUTINY_MONTHS = 36
PLAY_HAND_LAB_PIPELINE_VERSION = "play_hand_lab_pipeline_v2"
PLAY_HAND_LAB_SCREEN_PIPELINE = "screen"
PLAY_HAND_LAB_PLAY_HAND_PIPELINE = "play_hand"
PLAY_HAND_LAB_STAGE_ORDER = (
    "baseline",
    "lookback",
    "coarse",
    "focused",
    "instrument_scout",
    "scrutiny",
    "artifacts",
)


@dataclass(frozen=True)
class PlayHandLabRuntimeConfig:
    gateway_url: str = DEFAULT_LAB_GATEWAY_URL
    gateway_token: str | None = None
    campaign_mode: Literal["finite", "continuous"] = "finite"
    task_mode: Literal["fake_compute", "deep_replay"] = "deep_replay"
    pipeline_mode: Literal["screen", "play_hand"] = "play_hand"
    target_runs: int | None = None
    active_runs: int | None = None
    lanes: int = 4
    tasks_per_lane: int = 1
    timeframe: str = "M5"
    instrument: list[str] | None = None
    instrument_pool: list[str] | None = None
    instrument_pool_preset: list[str] | None = None
    indicator: list[str] | None = None
    profile_path: Path | None = None
    min_indicators: int = 1
    max_indicators: int = 4
    seed: int | None = None
    lookback_months: int = 3
    bar_limit: int = 5000
    max_reward_r: float | None = None
    sweep_budget: str = "high"
    max_sweep_permutations: int | None = None
    sweep_shard_size: int = DEFAULT_LAB_SWEEP_SHARD_SIZE
    early_exit_mode: Literal["off", "report", "enforce"] = "enforce"
    coarse_halving_mode: Literal["off", "enforce"] = "enforce"
    coarse_probe_budget: int = PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET
    scrutiny_months: int = DEFAULT_LAB_SCRUTINY_MONTHS
    instrument_scout_size: int = INSTRUMENT_SCOUT_DEFAULT_SIZE
    instrument_scout_max_selected: int = INSTRUMENT_SCOUT_DEFAULT_MAX_SELECTED
    fake_work_seconds: float = 1.0
    deadline_seconds: float = 3600.0
    max_attempts: int = 4
    poll_interval_seconds: float = 1.0
    max_wait_seconds: float = 3600.0
    result_batch_size: int = DEFAULT_LAB_RESULT_BATCH_SIZE
    result_read_failure_limit: int = DEFAULT_LAB_RESULT_READ_FAILURE_LIMIT
    dry_run: bool = False
    strict_scoring: bool = False
    retain_raw_lab_artifacts: bool = False
    json_output: bool = False
    worker_contract_hash: str | None = None
    worker_contract_schema: str = "replay-worker-contract-v1"
    trading_dashboard_root: Path | None = None


@dataclass
class LabLaneState:
    lane_id: str
    lane_index: int
    run_id: str
    run_dir: Path
    profile_path: Path | None = None
    profile_payload: dict[str, Any] | None = None
    profile_ref: str | None = None
    instruments: list[str] = field(default_factory=list)
    timeframe: str = "M5"
    indicator_ids: list[str] = field(default_factory=list)
    task_ids: list[str] = field(default_factory=list)
    completed_task_ids: set[str] = field(default_factory=set)
    failed_task_ids: set[str] = field(default_factory=set)
    task_specs: dict[str, dict[str, Any]] = field(default_factory=dict)
    phase_task_ids: dict[str, list[str]] = field(default_factory=dict)
    phase_scores: dict[str, float | None] = field(default_factory=dict)
    phase_rows: list[dict[str, Any]] = field(default_factory=list)
    phase_results: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    current_phase: str = "queued"
    terminal: bool = False
    run_promoted: bool = False
    tombstone_reason: str | None = None
    tombstone_reasons: list[str] = field(default_factory=list)
    incumbent_profile_path: Path | None = None
    incumbent_profile_ref: str | None = None
    incumbent_profile_payload: dict[str, Any] | None = None
    incumbent_timeframe: str | None = None
    incumbent_instruments: list[str] = field(default_factory=list)
    incumbent_score: float | None = None
    incumbent_phase: str | None = None
    last_sweep_payload: dict[str, Any] | None = None
    last_sweep_axes: list[str] = field(default_factory=list)
    skip_focused_and_scout: bool = False
    instrument_scout_result: dict[str, Any] | None = None
    final_attempt_id: str | None = None
    best_score: float | None = None
    best_attempt_id: str | None = None


class LabGatewayClient:
    def __init__(self, *, base_url: str, token: str | None = None, timeout_seconds: float = 30.0) -> None:
        self.base_url = str(base_url or DEFAULT_LAB_GATEWAY_URL).rstrip("/")
        self.token = str(token or "").strip() or None
        self.timeout_seconds = max(float(timeout_seconds), 1.0)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def health(self) -> dict[str, Any]:
        response = requests.get(f"{self.base_url}/healthz", timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/tasks",
            json={"tasks": tasks},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def snapshot(self) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}/snapshot",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def read_results(self, *, limit: int) -> list[dict[str, Any]]:
        response = requests.get(
            f"{self.base_url}/results",
            params={"limit": max(int(limit), 1)},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        results = payload.get("results")
        return [item for item in results if isinstance(item, dict)] if isinstance(results, list) else []

    def ack_results(self, lease_ids: list[str]) -> int:
        response = requests.post(
            f"{self.base_url}/results/ack",
            json={"lease_ids": [str(lease_id) for lease_id in lease_ids if str(lease_id)]},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return 0
        return int(payload.get("acked") or 0)

    def drain_results(self, *, limit: int) -> list[dict[str, Any]]:
        results = self.read_results(limit=limit)
        self.ack_results([str(item.get("lease_id") or "") for item in results])
        return results


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_runtime(runtime: PlayHandLabRuntimeConfig) -> PlayHandLabRuntimeConfig:
    gateway_url = str(runtime.gateway_url or os.environ.get("FUZZFOLIO_LAB_GATEWAY_URL") or DEFAULT_LAB_GATEWAY_URL)
    token = runtime.gateway_token or load_lab_gateway_token(create=False)
    contract_hash = (
        runtime.worker_contract_hash
        or os.environ.get("FUZZFOLIO_REPLAY_WORKER_CONTRACT_HASH")
        or os.environ.get("FUZZFOLIO_WORKER_CONTRACT_HASH")
    )
    contract_schema = (
        str(
            runtime.worker_contract_schema
            or os.environ.get("FUZZFOLIO_REPLAY_WORKER_CONTRACT_SCHEMA")
            or "replay-worker-contract-v1"
        )
        .strip()
        or "replay-worker-contract-v1"
    )
    task_mode = str(runtime.task_mode or "deep_replay").strip().lower()
    if task_mode not in {"fake_compute", "deep_replay"}:
        raise ValueError("--task-mode must be fake_compute or deep_replay")
    pipeline_mode = str(runtime.pipeline_mode or PLAY_HAND_LAB_PLAY_HAND_PIPELINE).strip().lower()
    if task_mode == "fake_compute":
        pipeline_mode = PLAY_HAND_LAB_SCREEN_PIPELINE
    if pipeline_mode not in {PLAY_HAND_LAB_SCREEN_PIPELINE, PLAY_HAND_LAB_PLAY_HAND_PIPELINE}:
        raise ValueError("--pipeline-mode must be screen or play_hand")
    campaign_mode = str(runtime.campaign_mode or "finite").strip().lower()
    if campaign_mode not in {"finite", "continuous"}:
        raise ValueError("--mode must be finite or continuous")
    legacy_lanes = max(int(runtime.lanes), 1)
    requested_target_runs = (
        max(int(runtime.target_runs), 1)
        if runtime.target_runs is not None
        else (legacy_lanes if campaign_mode == "finite" else None)
    )
    target_runs = None if campaign_mode == "continuous" else requested_target_runs
    default_active_runs = (
        requested_target_runs
        if campaign_mode == "continuous" and requested_target_runs is not None
        else min(target_runs, DEFAULT_LAB_ACTIVE_RUNS)
        if target_runs is not None
        else DEFAULT_LAB_ACTIVE_RUNS
        if campaign_mode == "continuous"
        else min(legacy_lanes, DEFAULT_LAB_ACTIVE_RUNS)
    )
    active_runs = max(
        int(runtime.active_runs)
        if runtime.active_runs is not None
        else default_active_runs,
        1,
    )
    if campaign_mode == "finite" and target_runs is not None:
        active_runs = min(active_runs, target_runs)
    lanes = target_runs or active_runs
    tasks_per_lane = max(int(runtime.tasks_per_lane), 1)
    if task_mode == "deep_replay" and tasks_per_lane != 1:
        raise ValueError("Deep-replay lab mode requires --tasks-per-lane 1; increase --target-runs for more work.")
    min_indicators = max(int(runtime.min_indicators), 1)
    max_indicators = max(int(runtime.max_indicators), min_indicators)
    lookback_months = max(int(runtime.lookback_months), 1)
    bar_limit = max(int(runtime.bar_limit), 10)
    sweep_budget = resolve_sweep_budget(
        sweep_budget=runtime.sweep_budget,
        max_sweep_permutations=runtime.max_sweep_permutations,
    )
    early_exit_mode = str(runtime.early_exit_mode or "enforce").strip().lower()
    if early_exit_mode not in {"off", "report", "enforce"}:
        raise ValueError("--early-exit-mode must be off, report, or enforce")
    coarse_halving_mode = str(runtime.coarse_halving_mode or "enforce").strip().lower()
    if coarse_halving_mode not in {"off", "enforce"}:
        raise ValueError("--coarse-halving-mode must be off or enforce")
    return PlayHandLabRuntimeConfig(
        gateway_url=gateway_url.rstrip("/"),
        gateway_token=str(token).strip() if token else None,
        campaign_mode=campaign_mode,  # type: ignore[arg-type]
        task_mode=task_mode,  # type: ignore[arg-type]
        pipeline_mode=pipeline_mode,  # type: ignore[arg-type]
        target_runs=target_runs,
        active_runs=active_runs,
        lanes=lanes,
        tasks_per_lane=tasks_per_lane,
        timeframe=str(runtime.timeframe or "M5").strip().upper() or "M5",
        instrument=_clean_symbols(runtime.instrument),
        instrument_pool=resolve_instrument_pool_presets(
            presets=runtime.instrument_pool_preset,
            instrument_pool=runtime.instrument_pool,
        ),
        instrument_pool_preset=_clean_pool_names(runtime.instrument_pool_preset),
        indicator=_clean_symbols(runtime.indicator),
        profile_path=runtime.profile_path,
        min_indicators=min_indicators,
        max_indicators=max_indicators,
        seed=runtime.seed,
        lookback_months=lookback_months,
        bar_limit=bar_limit,
        max_reward_r=runtime.max_reward_r,
        sweep_budget=str(sweep_budget["label"]),
        max_sweep_permutations=int(sweep_budget["value"]),
        sweep_shard_size=max(int(runtime.sweep_shard_size), 1),
        early_exit_mode=early_exit_mode,  # type: ignore[arg-type]
        coarse_halving_mode=coarse_halving_mode,  # type: ignore[arg-type]
        coarse_probe_budget=max(int(runtime.coarse_probe_budget), 1),
        scrutiny_months=max(int(runtime.scrutiny_months), 1),
        instrument_scout_size=max(int(runtime.instrument_scout_size), 1),
        instrument_scout_max_selected=max(int(runtime.instrument_scout_max_selected), 1),
        fake_work_seconds=max(float(runtime.fake_work_seconds), 0.0),
        deadline_seconds=max(float(runtime.deadline_seconds), 1.0),
        max_attempts=max(int(runtime.max_attempts), 1),
        poll_interval_seconds=max(float(runtime.poll_interval_seconds), 0.1),
        max_wait_seconds=max(float(runtime.max_wait_seconds), 1.0),
        result_batch_size=max(int(runtime.result_batch_size), 1),
        result_read_failure_limit=max(int(runtime.result_read_failure_limit), 1),
        dry_run=bool(runtime.dry_run),
        strict_scoring=bool(runtime.strict_scoring),
        retain_raw_lab_artifacts=bool(runtime.retain_raw_lab_artifacts),
        json_output=bool(runtime.json_output),
        worker_contract_hash=str(contract_hash).strip() if contract_hash else None,
        worker_contract_schema=contract_schema,
        trading_dashboard_root=(
            Path(runtime.trading_dashboard_root).resolve()
            if runtime.trading_dashboard_root
            else (
                Path(os.environ["TRADING_DASHBOARD_ROOT"]).resolve()
                if os.environ.get("TRADING_DASHBOARD_ROOT")
                else None
            )
        ),
    )


def _clean_symbols(values: list[str] | tuple[str, ...] | None) -> list[str] | None:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        for part in str(value or "").split(","):
            token = part.strip().upper()
            if not token or token in seen:
                continue
            cleaned.append(token)
            seen.add(token)
    return cleaned or None


def _clean_pool_names(values: list[str] | tuple[str, ...] | None) -> list[str] | None:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        for part in str(value or "").split(","):
            token = part.strip().lower().replace("_", "-")
            if token and token not in seen:
                cleaned.append(token)
                seen.add(token)
    return cleaned or None


def _extract_scaffoldable_indicator_ids(payload: Any) -> set[str]:
    if not isinstance(payload, dict):
        return set()
    data = payload.get("data")
    candidates: Any = None
    if isinstance(data, dict):
        candidates = data.get("ids") or data.get("indicators")
    elif isinstance(data, list):
        candidates = data
    if candidates is None:
        candidates = payload.get("ids") or payload.get("indicators")
    if not isinstance(candidates, list):
        return set()
    ids: set[str] = set()
    for item in candidates:
        raw = item.get("id") if isinstance(item, dict) else item
        indicator_id = str(raw or "").strip().upper()
        if indicator_id:
            ids.add(indicator_id)
    return ids


def _load_scaffoldable_indicator_ids(cli: FuzzfolioCli) -> set[str]:
    result = cli.run(["indicators", "--mode", "index"])
    ids = _extract_scaffoldable_indicator_ids(result.parsed_json)
    if not ids:
        raise RuntimeError("FuzzFolio indicator index did not return scaffoldable indicator ids.")
    return ids


def _coerce_seed_indicator(value: Any) -> SeedIndicator | None:
    if isinstance(value, SeedIndicator):
        indicator = value
    else:
        indicator = SeedIndicator(id=str(value or ""))
    indicator_id = str(indicator.id or "").strip().upper()
    if not indicator_id:
        return None
    return replace(indicator, id=indicator_id)


def _filter_scaffoldable_seed_indicators(
    indicators: list[SeedIndicator] | list[Any],
    *,
    scaffoldable_indicator_ids: set[str],
) -> tuple[list[SeedIndicator], list[str]]:
    valid: list[SeedIndicator] = []
    invalid: list[str] = []
    seen: set[str] = set()
    for raw_indicator in indicators:
        indicator = _coerce_seed_indicator(raw_indicator)
        if indicator is None:
            continue
        indicator_id = indicator.id.upper()
        if indicator_id in seen:
            continue
        seen.add(indicator_id)
        if indicator_id not in scaffoldable_indicator_ids:
            invalid.append(indicator_id)
            continue
        valid.append(indicator)
    return valid, invalid


def _runtime_event_payload(runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    payload = asdict(runtime)
    for key in ("profile_path", "trading_dashboard_root"):
        value = payload.get(key)
        if isinstance(value, Path):
            payload[key] = str(value)
    return payload


def _derived_campaign_root(config: AppConfig) -> Path:
    return config.runs_root / "derived" / PLAY_HAND_LAB_CAMPAIGNS_DIR


def _default_trading_dashboard_root(config: AppConfig) -> Path:
    configured = getattr(config.fuzzfolio, "workspace_root", None)
    if configured:
        return Path(configured).resolve()
    return (config.repo_root.parent / "Trading-Dashboard").resolve()


def _trading_dashboard_root(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
) -> Path:
    return (runtime.trading_dashboard_root or _default_trading_dashboard_root(config)).resolve()


def _ensure_trading_dashboard_python_paths(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
) -> Path:
    root = _trading_dashboard_root(config=config, runtime=runtime)
    if not root.exists():
        raise RuntimeError(
            "Deep-replay lab mode requires Trading-Dashboard sources. Provide "
            "--trading-dashboard-root or set TRADING_DASHBOARD_ROOT."
        )
    shared_python = root / "shared" / "python"
    if not shared_python.exists():
        raise RuntimeError(f"Trading-Dashboard shared python path not found: {shared_python}")
    package_roots = [
        shared_python / "fuzzfolio_core",
        shared_python / "fuzzfolio_data",
        shared_python,
    ]
    for package_root in reversed(package_roots):
        if package_root.exists() and str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
    return root


def _resolve_worker_contract_hash(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
) -> str | None:
    if runtime.task_mode != "deep_replay":
        return None
    if runtime.worker_contract_hash:
        return runtime.worker_contract_hash
    root = _ensure_trading_dashboard_python_paths(config=config, runtime=runtime)
    shared_python = root / "shared" / "python"
    try:
        from fuzzfolio_core.contracts.worker_contract import build_replay_worker_contract
    except Exception as exc:
        raise RuntimeError(f"Could not load FuzzFolio worker contract helpers from {shared_python}: {exc}") from exc
    try:
        return build_replay_worker_contract(repo_root=root).contract_hash
    except Exception as exc:
        raise RuntimeError(f"Could not build replay worker contract hash from {root}: {exc}") from exc


def _load_fuzzfolio_profile_models(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
):
    root = _ensure_trading_dashboard_python_paths(config=config, runtime=runtime)
    shared_python = root / "shared" / "python"
    try:
        from fuzzfolio_core.models.scoringprofile import ScoringProfile, StoredScoringProfile
    except Exception as exc:
        raise RuntimeError(f"Could not load FuzzFolio profile models from {shared_python}: {exc}") from exc
    return ScoringProfile, StoredScoringProfile


def _worker_ready_profile_snapshot(
    profile_payload: dict[str, Any],
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, Any]:
    profile = _inner_profile_payload(profile_payload)
    ScoringProfile, StoredScoringProfile = _load_fuzzfolio_profile_models(
        config=config,
        runtime=runtime,
    )
    try:
        full_profile = ScoringProfile.model_validate(profile)
    except Exception as full_exc:
        try:
            stored_profile = StoredScoringProfile.model_validate(profile)
            full_profile = stored_profile.to_full_profile()
        except Exception as stored_exc:
            raise RuntimeError(
                "Deep-replay lab mode requires a valid FuzzFolio scoring profile. "
                "The profile was neither a full ScoringProfile nor a convertible "
                f"StoredScoringProfile. Full-profile error: {full_exc}; "
                f"stored-profile error: {stored_exc}"
            ) from stored_exc
    snapshot = full_profile.model_dump(mode="json")
    if not isinstance(snapshot, dict):
        raise RuntimeError("FuzzFolio profile model produced a non-object profile snapshot.")
    return snapshot


def _campaign_run_id() -> str:
    return f"{_utc_stamp()}-playhand-lab-campaign-v1"


def _lane_run_id(lane_index: int) -> str:
    return f"{_utc_stamp()}-playhand-lab-lane-{lane_index:03d}-v1"


def _new_context(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    run_id: str,
    run_dir: Path,
    runtime: PlayHandLabRuntimeConfig,
) -> PlayHandContext:
    return PlayHandContext(
        config=config,
        cli=cli,
        run_id=run_id,
        run_dir=run_dir,
        profiles_dir=run_dir / "profiles",
        evals_dir=run_dir / "evals",
        attempts_path=attempts_path_for_run_dir(run_dir),
        events_path=run_dir / "play-hand-lab-lane-events.jsonl",
        summary_path=run_dir / "play-hand-lab-lane-summary.json",
        dry_run=runtime.dry_run,
        job_timeout_seconds=PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
        sweep_timeout_seconds=PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    )


def _campaign_context(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    campaign_id: str,
    campaign_dir: Path,
    runtime: PlayHandLabRuntimeConfig,
) -> PlayHandContext:
    return PlayHandContext(
        config=config,
        cli=cli,
        run_id=campaign_id,
        run_dir=campaign_dir,
        profiles_dir=campaign_dir / "profiles",
        evals_dir=campaign_dir / "evals",
        attempts_path=campaign_dir / "attempts.jsonl",
        events_path=campaign_dir / "play-hand-lab-campaign-events.jsonl",
        summary_path=campaign_dir / "play-hand-lab-campaign-summary.json",
        dry_run=runtime.dry_run,
        job_timeout_seconds=PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
        sweep_timeout_seconds=PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    )


def _write_campaign_metadata(
    campaign_ctx: PlayHandContext,
    *,
    runtime: PlayHandLabRuntimeConfig,
    status: str,
    started_at: str,
    extra: dict[str, Any] | None = None,
) -> None:
    metadata = load_run_metadata(campaign_ctx.run_dir)
    metadata.update(
        {
            "schema_version": PLAY_HAND_LAB_CAMPAIGN_SCHEMA_VERSION,
            "runner": PLAY_HAND_RUNNER,
            "generated_by_runner": PLAY_HAND_LAB_RUNNER,
            "run_kind": "play_hand_lab_campaign",
            "run_id": campaign_ctx.run_id,
            "run_status": status,
            "created_at": metadata.get("created_at") or started_at,
            "started_at": started_at,
            "gateway_url": runtime.gateway_url,
            "campaign_mode": runtime.campaign_mode,
            "task_mode": runtime.task_mode,
            "pipeline_mode": runtime.pipeline_mode,
            "lanes": runtime.lanes,
            "target_runs": runtime.target_runs,
            "active_runs": runtime.active_runs,
            "tasks_per_lane": runtime.tasks_per_lane,
            "timeframe": runtime.timeframe,
            "lookback_months": runtime.lookback_months,
            "bar_limit": runtime.bar_limit,
            "instrument": runtime.instrument,
            "instrument_pool_preset": runtime.instrument_pool_preset,
            "instrument_pool": runtime.instrument_pool or list(DEFAULT_INSTRUMENT_POOL),
            "indicator": runtime.indicator,
            "profile_path": str(runtime.profile_path.resolve()) if runtime.profile_path else None,
            "min_indicators": runtime.min_indicators,
            "max_indicators": runtime.max_indicators,
            "seed": runtime.seed,
            "max_reward_r": runtime.max_reward_r,
            "sweep_budget": runtime.sweep_budget,
            "max_sweep_permutations": runtime.max_sweep_permutations,
            "sweep_shard_size": runtime.sweep_shard_size,
            "early_exit_mode": runtime.early_exit_mode,
            "coarse_halving_mode": runtime.coarse_halving_mode,
            "coarse_probe_budget": runtime.coarse_probe_budget,
            "scrutiny_months": runtime.scrutiny_months,
            "instrument_scout_size": runtime.instrument_scout_size,
            "instrument_scout_max_selected": runtime.instrument_scout_max_selected,
            "required_worker_contract_hash": runtime.worker_contract_hash,
            "required_worker_contract_schema": runtime.worker_contract_schema,
            "trading_dashboard_root": (
                str(runtime.trading_dashboard_root.resolve())
                if runtime.trading_dashboard_root
                else None
            ),
            "dry_run": runtime.dry_run,
            "design_note": (
                "First-class PlayHand Lab campaign: no Redis/Appwrite/backend hot path, "
                "workers claim complete self-contained tasks from the lab gateway."
            ),
        }
    )
    if extra:
        metadata.update(extra)
    write_run_metadata(campaign_ctx.run_dir, metadata)


def _write_lane_metadata(
    lane: LabLaneState,
    *,
    campaign_ctx: PlayHandContext,
    runtime: PlayHandLabRuntimeConfig,
    status: str,
    started_at: str,
    extra: dict[str, Any] | None = None,
) -> None:
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "schema_version": PLAY_HAND_LAB_LANE_SCHEMA_VERSION,
            "runner": PLAY_HAND_RUNNER,
            "generated_by_runner": PLAY_HAND_LAB_RUNNER,
            "run_kind": "play_hand_lab_lane",
            "run_id": lane.run_id,
            "run_status": status,
            "created_at": metadata.get("created_at") or started_at,
            "started_at": started_at,
            "lab_campaign_id": campaign_ctx.run_id,
            "parent_campaign_id": campaign_ctx.run_id,
            "campaign_dir": str(campaign_ctx.run_dir.resolve()),
            "lab_lane_id": lane.lane_id,
            "lab_lane_index": lane.lane_index,
            "gateway_url": runtime.gateway_url,
            "task_mode": runtime.task_mode,
            "pipeline_mode": runtime.pipeline_mode,
            "pipeline_version": PLAY_HAND_LAB_PIPELINE_VERSION,
            "current_phase": lane.current_phase,
            "timeframe": lane.timeframe,
            "lookback_months": runtime.lookback_months,
            "bar_limit": runtime.bar_limit,
            "instruments": list(lane.instruments),
            "indicators": list(lane.indicator_ids),
            "profile_path": str(lane.profile_path.resolve()) if lane.profile_path else None,
            "profile_ref": lane.profile_ref,
            "task_ids": list(lane.task_ids),
            "completed_task_count": len(lane.completed_task_ids),
            "failed_task_count": len(lane.failed_task_ids),
            "phase_scores": dict(lane.phase_scores),
            "phase_rows": list(lane.phase_rows),
            "terminal": lane.terminal,
            "run_promoted": lane.run_promoted,
            "tombstone_reason": lane.tombstone_reason,
            "tombstone_reasons": list(lane.tombstone_reasons),
            "incumbent_profile_path": (
                str(lane.incumbent_profile_path.resolve())
                if lane.incumbent_profile_path
                else None
            ),
            "incumbent_profile_ref": lane.incumbent_profile_ref,
            "incumbent_score": lane.incumbent_score,
            "incumbent_phase": lane.incumbent_phase,
            "instrument_scout": lane.instrument_scout_result,
            "final_attempt_id": lane.final_attempt_id,
            "best_score": lane.best_score,
            "best_attempt_id": lane.best_attempt_id,
            "dry_run": runtime.dry_run,
        }
    )
    if extra:
        metadata.update(extra)
    write_run_metadata(lane.run_dir, metadata)


def _seed_indicators(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    campaign_ctx: PlayHandContext,
    runtime: PlayHandLabRuntimeConfig,
) -> tuple[list[SeedIndicator], dict[str, Any] | None, Path | None]:
    seed_plan, seed_plan_path = _load_play_hand_seed_plan(config)
    pinned = [SeedIndicator(id=item) for item in runtime.indicator or []]
    scaffoldable_indicator_ids: set[str] | None = None
    if runtime.profile_path is None:
        scaffoldable_indicator_ids = _load_scaffoldable_indicator_ids(cli)

    def scaffoldable_pool(
        indicators: list[SeedIndicator] | list[Any],
        *,
        source: str,
        require_all: bool = False,
    ) -> tuple[list[SeedIndicator], list[str]]:
        if scaffoldable_indicator_ids is None:
            return [indicator for item in indicators if (indicator := _coerce_seed_indicator(item))], []
        valid, invalid = _filter_scaffoldable_seed_indicators(
            indicators,
            scaffoldable_indicator_ids=scaffoldable_indicator_ids,
        )
        if invalid:
            _append_event(
                campaign_ctx,
                "seed_indicators",
                "filtered_unscaffoldable",
                source=source,
                invalid_indicators=invalid[:50],
                invalid_count=len(invalid),
                valid_count=len(valid),
            )
        if require_all and invalid:
            raise ValueError(
                "Pinned PlayHand indicators are not scaffoldable by the current FuzzFolio CLI: "
                + ", ".join(invalid[:10])
            )
        return valid, invalid

    if pinned:
        valid, _invalid = scaffoldable_pool(pinned, source="pinned", require_all=True)
        if len(valid) < runtime.min_indicators:
            raise RuntimeError(
                "Pinned PlayHand indicator pool is smaller than --min-indicators after validation: "
                f"{len(valid)} < {runtime.min_indicators}."
            )
        return valid, seed_plan, seed_plan_path
    seed_plan_candidates = _seed_plan_indicator_candidates(config, seed_plan)
    if seed_plan_candidates:
        valid, _invalid = scaffoldable_pool(seed_plan_candidates, source="seed_plan")
        if len(valid) >= runtime.min_indicators:
            return valid, seed_plan, seed_plan_path
        _append_event(
            campaign_ctx,
            "seed_indicators",
            "seed_plan_pool_too_small",
            valid_count=len(valid),
            min_indicators=runtime.min_indicators,
        )
    try:
        seeded = _seed_hand(config, cli, campaign_ctx.run_dir)
    except Exception as exc:
        _append_event(
            campaign_ctx,
            "seed_hand",
            "fallback",
            error=str(exc)[:500],
            fallback_indicators=["RSI", "MACD", "SMA"],
        )
        seeded = [SeedIndicator("RSI"), SeedIndicator("MACD"), SeedIndicator("SMA")]
    valid, _invalid = scaffoldable_pool(seeded or [], source="seed_prompt")
    if len(valid) >= runtime.min_indicators:
        return valid, seed_plan, seed_plan_path
    fallback, _fallback_invalid = scaffoldable_pool(
        [SeedIndicator("RSI"), SeedIndicator("MACD"), SeedIndicator("SMA")],
        source="fallback",
    )
    if len(fallback) >= runtime.min_indicators:
        return fallback, seed_plan, seed_plan_path
    raise RuntimeError(
        "PlayHand Massive v2 could not build a scaffoldable indicator pool large enough "
        f"for --min-indicators {runtime.min_indicators}; valid fallback count={len(fallback)}."
    )


def _deal_lane(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any] | None,
    rng: random.Random,
) -> dict[str, Any]:
    seed_indicators = [indicator for item in seed_indicators if (indicator := _coerce_seed_indicator(item))]
    shuffled = list(seed_indicators)
    rng.shuffle(shuffled)
    allowed_seed_ids = {indicator.id.upper() for indicator in seed_indicators}
    seed_plan_candidates = [
        candidate
        for candidate in _seed_plan_indicator_candidates(config, seed_plan)
        if candidate.id.upper() in allowed_seed_ids
    ]
    guided_available_count = len(_merge_seed_indicator_candidates(shuffled, seed_plan_candidates))
    dealt_count = deal_indicator_count(
        available_count=max(len(shuffled), guided_available_count),
        min_indicators=runtime.min_indicators,
        max_indicators=runtime.max_indicators,
        rng=rng,
    )
    indicator_deal = deal_seed_plan_indicators(
        shuffled,
        target_count=dealt_count,
        seed_plan=seed_plan,
        rng=rng,
        seed_plan_candidates=seed_plan_candidates,
    )
    dealt_entries = list(indicator_deal.get("indicators") or [])
    if not dealt_entries:
        indicator_deal = _fallback_indicator_deal(
            shuffled,
            target_count=dealt_count,
            source="role_balanced",
            reason="empty_lab_guided_deal",
        )
        dealt_entries = list(indicator_deal.get("indicators") or [])
    template_instrument_policy = _seed_plan_template_instrument_policy(seed_plan)
    template_instrument_pool = _seed_pair_template_instruments(indicator_deal.get("pair"))
    effective_instrument_pool = runtime.instrument_pool
    if (
        template_instrument_policy == "seed_pool"
        and template_instrument_pool
        and not runtime.instrument
        and not runtime.instrument_pool
    ):
        effective_instrument_pool = template_instrument_pool
    instrument_deal = deal_instruments(
        instrument=runtime.instrument,
        instrument_pool=effective_instrument_pool,
        rng=rng,
    )
    return {
        "indicator_deal": indicator_deal,
        "dealt_entries": dealt_entries,
        "dealt": [indicator.id for indicator in dealt_entries],
        "instrument_deal": instrument_deal,
        "instruments": list(instrument_deal["instruments"]),
    }


def _inner_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload.get("profile") if isinstance(payload, dict) else None
    return dict(profile) if isinstance(profile, dict) else dict(payload)


def _prepare_lane_profile(
    ctx: PlayHandContext,
    *,
    runtime: PlayHandLabRuntimeConfig,
    lane: LabLaneState,
    seed_plan: dict[str, Any] | None,
    deal: dict[str, Any],
    rng: random.Random,
) -> None:
    if runtime.profile_path is not None:
        source_payload = _load_json(runtime.profile_path)
        if not source_payload:
            raise RuntimeError(f"Profile path is empty or invalid: {runtime.profile_path}")
        lane.profile_path = ctx.profiles_dir / f"{lane.lane_id}_profile.json"
        _write_json(lane.profile_path, source_payload)
        profile_payload = source_payload
        lane.indicator_ids = [
            str((item.get("meta") or {}).get("id") or "").strip()
            for item in _inner_profile_payload(profile_payload).get("indicators", [])
            if isinstance(item, dict)
        ]
        profile = _inner_profile_payload(profile_payload)
        lane.instruments = [str(item).strip().upper() for item in profile.get("instruments") or deal["instruments"]]
    else:
        scaffold_ctx = ctx
        if runtime.task_mode == "deep_replay" and getattr(ctx, "dry_run", False):
            try:
                scaffold_ctx = replace(ctx, dry_run=False)
            except TypeError:
                scaffold_ctx = copy.copy(ctx)
                scaffold_ctx.dry_run = False
        lane.profile_path = _scaffold_profile(
            scaffold_ctx,
            list(deal["dealt"]),
            list(deal["instruments"]),
            runtime.timeframe,
            f"{lane.lane_id}_base",
        )
        profile_payload = _load_json(lane.profile_path)
        metadata_changes = apply_seed_indicator_metadata(profile_payload, list(deal["dealt_entries"]))
        timeframe_changes = apply_role_timeframe_defaults(profile_payload, rng=rng)
        template_changes = apply_seed_pair_template_defaults(
            profile_payload,
            deal["indicator_deal"].get("pair"),
        )
        default_changes = apply_play_hand_profile_defaults(profile_payload, rng=rng)
        if metadata_changes or timeframe_changes or template_changes or default_changes:
            _write_json(lane.profile_path, profile_payload)
        lane.indicator_ids = list(deal["dealt"])
        lane.instruments = list(deal["instruments"])
        _append_event(
            ctx,
            "profile_scaffolded",
            "ready",
            profile_path=str(lane.profile_path),
            metadata_changes=metadata_changes,
            timeframe_changes=timeframe_changes,
            template_changes=template_changes,
            default_changes=default_changes,
        )
    lane.profile_payload = _inner_profile_payload(_load_json(lane.profile_path))
    if runtime.task_mode == "deep_replay":
        lane.profile_payload = _worker_ready_profile_snapshot(
            lane.profile_payload,
            config=ctx.config,
            runtime=runtime,
        )
    lane.timeframe = _lowest_profile_timeframe(lane.profile_payload, runtime.timeframe)
    lane.profile_ref = f"lab-inline:{lane.run_id}:{lane.lane_id}"
    lane.incumbent_profile_path = lane.profile_path
    lane.incumbent_profile_ref = lane.profile_ref
    lane.incumbent_profile_payload = _copy_profile_payload(lane.profile_payload)
    lane.incumbent_timeframe = lane.timeframe
    lane.incumbent_instruments = list(lane.instruments)
    lane.incumbent_phase = "scaffold"


def _reward_matrix_payload(reward_matrix: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(reward_matrix, dict):
        return None
    return {
        "sl_start_percent": 0.02,
        "sl_step_percent": 0.02,
        "sl_rows": 25,
        "reward_start_r": float(reward_matrix.get("reward_step_r") or 0.5),
        "reward_step_r": float(reward_matrix.get("reward_step_r") or 0.5),
        "reward_columns": int(reward_matrix.get("reward_columns") or 8),
    }


def _copy_profile_payload(profile_payload: dict[str, Any] | None) -> dict[str, Any]:
    return copy.deepcopy(profile_payload or {})


def _profile_direction_mode(profile_payload: dict[str, Any] | None) -> str:
    token = str((profile_payload or {}).get("directionMode") or "both").strip().lower()
    return token if token in {"both", "long", "short"} else "both"


def _task_phase(lane: LabLaneState, task_id: str) -> str:
    spec = lane.task_specs.get(task_id)
    return str((spec or {}).get("phase") or "lab")


def _record_phase_score(lane: LabLaneState, phase_key: str, score: Any) -> None:
    lane.phase_scores[phase_key] = _as_float(score)


def _append_phase_row(
    lane: LabLaneState,
    *,
    phase: str,
    status: str,
    score: Any = None,
    detail: str = "",
) -> None:
    lane.phase_rows.append(
        {
            "phase": phase,
            "status": status,
            "score": _as_float(score),
            "detail": detail,
        }
    )


def _lane_attempts(lane_ctx: PlayHandContext) -> list[dict[str, Any]]:
    return load_attempts(lane_ctx.attempts_path)


def _lane_early_exit_decision(
    lane: LabLaneState,
    *,
    lane_ctx: PlayHandContext,
    checkpoint: str,
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, Any] | None:
    if runtime.early_exit_mode not in {"report", "enforce"}:
        return None
    metadata = load_run_metadata(lane.run_dir)
    evidence = build_play_hand_evidence(run_metadata=metadata, attempts=_lane_attempts(lane_ctx))
    decision = build_early_exit_decision(
        checkpoint=checkpoint,
        evidence=evidence,
        mode=runtime.early_exit_mode,
    )
    policy = metadata.setdefault(
        "early_exit_policy",
        {"version": decision.get("version"), "mode": runtime.early_exit_mode, "decisions": []},
    )
    if not isinstance(policy, dict):
        policy = {"version": decision.get("version"), "mode": runtime.early_exit_mode, "decisions": []}
        metadata["early_exit_policy"] = policy
    decisions = policy.setdefault("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        policy["decisions"] = decisions
    decisions.append(decision)
    metadata["early_exit_policy"] = policy
    write_run_metadata(lane.run_dir, metadata)
    _append_event(
        lane_ctx,
        "early_exit",
        "enforced" if decision.get("enforced") else "reported",
        **decision,
    )
    return decision


def _mark_lane_tombstoned(
    lane: LabLaneState,
    *,
    lane_ctx: PlayHandContext,
    reason: str,
    reasons: list[str] | None = None,
) -> None:
    lane.terminal = True
    lane.run_promoted = False
    lane.current_phase = "tombstoned"
    lane.tombstone_reason = reason
    lane.tombstone_reasons = sorted({item for item in [reason, *(reasons or [])] if item})
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "run_status": "tombstoned",
            "run_tombstoned": True,
            "tombstone_reason": lane.tombstone_reason,
            "tombstone_reasons": lane.tombstone_reasons,
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "final_attempt_id": lane.final_attempt_id,
            "final_scrutiny_passed": False,
            "final_scrutiny_score": None,
            "canonical_attempt_id": None,
            "canonical_attempt_role": None,
            "canonical_score": None,
            "final_profile_ref": lane.incumbent_profile_ref,
            "final_profile_path": (
                str(lane.incumbent_profile_path.resolve())
                if lane.incumbent_profile_path
                else None
            ),
            "final_artifacts": {"status": "skipped", "reason": reason},
        }
    )
    metadata["play_hand_health"] = build_play_hand_health(
        run_metadata=metadata,
        attempts=_lane_attempts(lane_ctx),
    )
    write_run_metadata(lane.run_dir, metadata)
    _append_event(
        lane_ctx,
        "lane",
        "tombstoned",
        reason=reason,
        tombstone_reasons=lane.tombstone_reasons,
    )


def _mark_lane_promoted(
    lane: LabLaneState,
    *,
    lane_ctx: PlayHandContext,
    final_score: float | None,
) -> None:
    lane.terminal = True
    lane.run_promoted = True
    lane.current_phase = "promoted"
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "run_status": "promoted",
            "run_tombstoned": False,
            "tombstone_reason": None,
            "tombstone_reasons": [],
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "final_attempt_id": lane.final_attempt_id,
            "final_scrutiny_passed": True,
            "final_scrutiny_score": final_score,
            "selected_final_branch": "mutated",
            "selected_final_phase": "final_36mo",
            "canonical_attempt_id": lane.final_attempt_id,
            "canonical_attempt_role": "final",
            "canonical_candidate_name": "final_36mo",
            "canonical_score": final_score,
            "canonical_instruments": list(lane.incumbent_instruments),
            "final_profile_ref": lane.incumbent_profile_ref,
            "final_profile_path": (
                str(lane.incumbent_profile_path.resolve())
                if lane.incumbent_profile_path
                else None
            ),
            "final_artifacts": {"status": "skipped", "reason": "disabled"},
        }
    )
    metadata["play_hand_health"] = build_play_hand_health(
        run_metadata=metadata,
        attempts=_lane_attempts(lane_ctx),
    )
    write_run_metadata(lane.run_dir, metadata)
    _append_event(
        lane_ctx,
        "lane",
        "promoted",
        final_attempt_id=lane.final_attempt_id,
        final_score=final_score,
    )


def _axis_values(axis: str) -> tuple[str, list[Any]]:
    left, _, right = str(axis or "").partition("=")
    values: list[Any] = []
    for raw in right.split(","):
        text = raw.strip()
        if not text:
            continue
        try:
            numeric = float(text)
            values.append(int(numeric) if numeric.is_integer() else numeric)
        except ValueError:
            values.append(text)
    return left.strip(), values


def _axis_to_sweep_axis(profile_payload: dict[str, Any], axis: str) -> dict[str, Any] | None:
    key, values = _axis_values(axis)
    match = re.fullmatch(r"indicator\[(\d+)\]\.(config|talib)\.([A-Za-z0-9_]+)", key)
    if not match or not values:
        return None
    index = int(match.group(1))
    section = match.group(2)
    param_key = match.group(3)
    indicators = profile_payload.get("indicators")
    if not isinstance(indicators, list) or index >= len(indicators):
        return None
    indicator = indicators[index]
    if not isinstance(indicator, dict):
        return None
    meta = indicator.get("meta") if isinstance(indicator.get("meta"), dict) else {}
    instance_id = str(meta.get("instanceId") or "").strip()
    if not instance_id:
        return None
    return {
        "target": "talib_param" if section == "talib" else "config_field",
        "indicator_instance_id": instance_id,
        "param_key": param_key,
        "values": values,
        "lab_axis": axis,
        "lab_axis_key": key,
    }


def _sweep_axis_key(axis: dict[str, Any]) -> str:
    if axis.get("target") == "profile_field":
        return str(axis.get("param_key") or "")
    return f"{axis.get('indicator_instance_id')}.{axis.get('param_key')}"


def _params_for_flat_index(
    *,
    keys: list[str],
    values: list[list[Any]],
    flat_index: int,
) -> dict[str, Any]:
    remaining = int(flat_index)
    selected: list[Any] = []
    for candidates in reversed(values):
        count = len(candidates)
        selected.append(candidates[remaining % count])
        remaining //= count
    selected.reverse()
    return {key: value for key, value in zip(keys, selected)}


def _budgeted_flat_indices(total: int, max_count: int) -> list[int]:
    total = max(int(total), 0)
    max_count = max(int(max_count), 0)
    if total <= 0 or max_count <= 0:
        return []
    if total <= max_count:
        return list(range(total))
    if max_count == 1:
        return [0]
    indices = [
        int(round(index * (total - 1) / (max_count - 1)))
        for index in range(max_count)
    ]
    return sorted(dict.fromkeys(min(max(item, 0), total - 1) for item in indices))


def _expand_sweep_params(axes: list[dict[str, Any]], *, max_permutations: int | None = None) -> list[dict[str, Any]]:
    if not axes:
        return []
    keys = [_sweep_axis_key(axis) for axis in axes]
    values = [list(axis.get("values") or []) for axis in axes]
    if any(not key for key in keys) or any(not candidates for candidates in values):
        return []
    total_permutations = 1
    for candidates in values:
        total_permutations *= len(candidates)
    if max_permutations is not None:
        budget = max(int(max_permutations), 1)
        if total_permutations > budget:
            return [
                _params_for_flat_index(keys=keys, values=values, flat_index=flat_index)
                for flat_index in _budgeted_flat_indices(total_permutations, budget)
            ]
    return [
        {key: value for key, value in zip(keys, combination)}
        for combination in itertools.product(*values)
    ]


def _sanitize_sweep_axes_for_contract(axes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for axis in axes:
        cleaned.append(
            {
                "target": axis["target"],
                "indicator_instance_id": axis.get("indicator_instance_id"),
                "param_key": axis["param_key"],
                "values": list(axis.get("values") or []),
            }
        )
    return cleaned


def _find_numeric_value(payload: Any, key: str) -> float | None:
    if isinstance(payload, dict):
        if key in payload:
            score = _as_float(payload.get(key))
            if score is not None:
                return score
        for value in payload.values():
            found = _find_numeric_value(value, key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_numeric_value(item, key)
            if found is not None:
                return found
    return None


def _score_from_replay_payload(payload: Any) -> float | None:
    if not isinstance(payload, dict):
        return None
    paths = [
        ("aggregate", "score_lab", "score"),
        ("data", "aggregate", "score_lab", "score"),
        ("score_lab", "score"),
        ("scoreLab", "score"),
        ("quality_score", "score"),
        ("aggregate", "quality_score", "score"),
    ]
    for path in paths:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        score = _as_float(current)
        if score is not None:
            return score
    for key in ("score", "score_lab", "quality_score"):
        score = _find_numeric_value(payload, key)
        if score is not None:
            return score
    return None


def _sweep_payload_from_worker_result(worker_result: dict[str, Any]) -> dict[str, Any]:
    payload = worker_result.get("result") if isinstance(worker_result.get("result"), dict) else worker_result
    return payload if isinstance(payload, dict) else {}


def _parameter_importance_from_ranked(ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = [
        item
        for item in ranked
        if isinstance(item.get("parameters"), dict)
        and _as_float(item.get("fitness_value")) is not None
    ]
    if not scored:
        return []
    best_score = _as_float(scored[0].get("fitness_value")) or 0.0
    axis_values: dict[str, dict[Any, list[float]]] = {}
    for item in scored:
        score = _as_float(item.get("fitness_value"))
        if score is None:
            continue
        for axis, value in dict(item.get("parameters") or {}).items():
            axis_values.setdefault(str(axis), {}).setdefault(value, []).append(score)
    importance: list[dict[str, Any]] = []
    for axis, by_value in axis_values.items():
        means = [
            (value, sum(values) / len(values))
            for value, values in by_value.items()
            if values
        ]
        if not means:
            continue
        best_value, axis_best = max(means, key=lambda item: item[1])
        axis_worst = min(score for _value, score in means)
        spread = max(axis_best - axis_worst, 0.0)
        importance.append(
            {
                "axis": axis,
                "best_value": best_value,
                "importance_pct": min(
                    abs(spread / max(abs(best_score), 1.0)) * 100.0,
                    100.0,
                ),
            }
        )
    importance.sort(key=lambda item: float(item.get("importance_pct") or 0.0), reverse=True)
    return importance


def _rank_sweep_permutations(
    *,
    phase: str,
    shard_results: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for shard_result in shard_results:
        payload = _sweep_payload_from_worker_result(shard_result)
        for item in payload.get("permutation_results") or []:
            if not isinstance(item, dict):
                continue
            result_payload = item.get("result") if isinstance(item.get("result"), dict) else {}
            score = _score_from_replay_payload(result_payload)
            if score is None:
                score = _score_from_replay_payload(item)
            ranked.append(
                {
                    "permutation_index": item.get("permutation_index"),
                    "child_job_id": item.get("child_job_id"),
                    "status": item.get("status"),
                    "parameters": dict(item.get("parameters") or {}),
                    "fitness": {"score_lab": score},
                    "fitness_value": score,
                    "score_lab": score,
                    "score": score,
                }
            )
        for item in payload.get("failed_permutations") or []:
            if isinstance(item, dict):
                failed.append(dict(item))
    ranked.sort(
        key=lambda item: (
            _as_float(item.get("fitness_value")) is not None,
            _as_float(item.get("fitness_value")) or float("-inf"),
        ),
        reverse=True,
    )
    best = ranked[0] if ranked else None
    return {
        "sweep_id": f"lab-{phase}",
        "mode": "lab_sweep_shard",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": best,
        "failed_permutations": failed,
        "parameter_importance": _parameter_importance_from_ranked(ranked),
    }


def _merge_sweep_payloads(phase: str, payloads: list[dict[str, Any]]) -> dict[str, Any]:
    ranked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for payload in payloads:
        for item in payload.get("ranked_permutations") or payload.get("ranked") or []:
            if isinstance(item, dict):
                ranked.append(dict(item))
        for item in payload.get("failed_permutations") or []:
            if isinstance(item, dict):
                failed.append(dict(item))
    ranked.sort(
        key=lambda item: (
            _as_float(item.get("fitness_value")) is not None,
            _as_float(item.get("fitness_value")) or float("-inf"),
        ),
        reverse=True,
    )
    return {
        "sweep_id": f"lab-{phase}",
        "mode": "lab_sweep_shard",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": ranked[0] if ranked else None,
        "failed_permutations": failed,
        "parameter_importance": _parameter_importance_from_ranked(ranked),
    }


def _deep_replay_job_payload(
    *,
    task_id: str,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
    profile_payload: dict[str, Any] | None = None,
    profile_ref: str | None = None,
    instruments: list[str] | None = None,
    timeframe: str | None = None,
    lookback_months: int | None = None,
) -> dict[str, Any]:
    profile_payload = _copy_profile_payload(profile_payload or lane.profile_payload)
    job: dict[str, Any] = {
        "job_id": task_id,
        "user_id": DEFAULT_LAB_USER_ID,
        "profile_id": profile_ref or lane.profile_ref or lane.run_id,
        "inline_profile_snapshot": profile_payload,
        "artifact_persistence": "ephemeral",
        "source_kind": "workspace_attempt",
        "client_origin": PLAY_HAND_LAB_RUNNER,
        "retention_ttl_seconds": None,
        "retention_behavior": "ephemeral",
        "retention_reason": "autoresearch",
        "source_client_origin": PLAY_HAND_LAB_RUNNER,
        "workspace_id": lane.run_id,
        "workspace_attempt_id": task_id,
        "instruments": list(instruments or lane.instruments),
        "timeframe": str(timeframe or lane.timeframe),
        "market_data_source": "lake_bars",
        "lookback_months": int(lookback_months or runtime.lookback_months),
        "bar_limit": int(runtime.bar_limit),
        "alert_threshold": float(profile_payload.get("notificationThreshold") or 80.0),
        "view_mode": "overview",
        "direction_mode": _profile_direction_mode(profile_payload),
        "priority": "research",
        "work_class": "research_replay",
        "required_worker_contract_hash": worker_contract_hash,
        "required_worker_contract_schema": runtime.worker_contract_schema,
        "required_capabilities": ["deep_replay"],
    }
    matrix = _reward_matrix_payload(reward_matrix)
    if matrix:
        job["matrix"] = matrix
    job["options"] = {
        "include_entries": False,
        "include_per_instrument": True,
        "include_aggregate_matrix": True,
        "path_metrics_mode": "highlighted",
        "quality_score_preset": "default",
        "cost_model": {
            "mode": "research_conservative",
            "spread_bps": 2.0,
            "slippage_bps": 1.0,
            "commission_bps": 0.5,
        },
    }
    return job


def _register_task_spec(
    lane: LabLaneState,
    *,
    task_id: str,
    phase: str,
    task_kind: str,
    spec: dict[str, Any],
) -> None:
    lane.task_ids.append(task_id)
    lane.task_specs[task_id] = {"phase": phase, "task_kind": task_kind, **spec}
    lane.phase_task_ids.setdefault(phase, []).append(task_id)


def _make_deep_replay_task(
    lane: LabLaneState,
    *,
    phase: str,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
    profile_payload: dict[str, Any],
    profile_path: Path | None,
    profile_ref: str,
    instruments: list[str],
    timeframe: str,
    lookback_months: int,
    required_worker_capabilities: list[str] | None = None,
) -> dict[str, Any]:
    task_id = f"{lane.run_id}-task-{len(lane.task_ids) + 1:05d}-{phase}"
    capabilities = required_worker_capabilities or [
        "deep_replay",
        PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
    ]
    payload = _deep_replay_job_payload(
        task_id=task_id,
        lane=lane,
        runtime=runtime,
        reward_matrix=reward_matrix,
        worker_contract_hash=worker_contract_hash,
        profile_payload=profile_payload,
        profile_ref=profile_ref,
        instruments=instruments,
        timeframe=timeframe,
        lookback_months=lookback_months,
    )
    _register_task_spec(
        lane,
        task_id=task_id,
        phase=phase,
        task_kind="deep_replay",
        spec={
            "profile_path": str(profile_path.resolve()) if profile_path else None,
            "profile_ref": profile_ref,
            "profile_payload": _copy_profile_payload(profile_payload),
            "instruments": list(instruments),
            "timeframe": timeframe,
            "lookback_months": lookback_months,
        },
    )
    return {
        "task_id": task_id,
        "lane_id": lane.lane_id,
        "attempt_id": task_id,
        "task_kind": "deep_replay",
        "payload": payload,
        "required_worker_capabilities": capabilities,
        "deadline_seconds": runtime.deadline_seconds,
        "max_attempts": runtime.max_attempts,
    }


def _sweep_definition_payload(
    *,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    axes: list[dict[str, Any]],
    instruments: list[str],
    profile_ref: str,
    profile_payload: dict[str, Any],
    lookback_months: int,
    mode: str,
) -> dict[str, Any]:
    definition: dict[str, Any] = {
        "base_profile_id": profile_ref,
        "axes": _sanitize_sweep_axes_for_contract(axes),
        "instruments": list(instruments),
        "mode": "deterministic" if mode not in {"deterministic", "evolutionary"} else mode,
        "evolutionary_config": None,
        "lookback_months": int(lookback_months),
        "bar_limit": int(runtime.bar_limit),
        "alert_threshold": None,
        "view_mode": "overview",
        "direction_mode": _profile_direction_mode(profile_payload),
        "fitness_metric": "score_lab",
        "top_n": 0,
        "shard_size": int(runtime.sweep_shard_size),
        "quality_score_preset": "default",
        "cost_model": {
            "mode": "research_conservative",
            "spread_bps": 2.0,
            "slippage_bps": 1.0,
            "commission_bps": 0.5,
        },
    }
    matrix = _reward_matrix_payload(reward_matrix)
    if matrix:
        definition["matrix"] = matrix
    return definition


def _make_sweep_shard_tasks(
    lane: LabLaneState,
    *,
    phase: str,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
    profile_payload: dict[str, Any],
    profile_path: Path,
    profile_ref: str,
    instruments: list[str],
    lookback_months: int,
    axis_texts: list[str],
    mode: str,
) -> list[dict[str, Any]]:
    if not axis_texts:
        return []
    axis_plan = plan_sweep_axes(
        axis_texts,
        profile_payload=profile_payload,
        phase=phase,
        max_permutations=int(runtime.max_sweep_permutations or PLAY_HAND_SWEEP_PERMUTATION_LIMIT),
        search_mode=mode,
    )
    sweep_axes = [
        axis
        for axis_text in axis_plan.axes
        if (axis := _axis_to_sweep_axis(profile_payload, axis_text)) is not None
    ]
    params = _expand_sweep_params(
        sweep_axes,
        max_permutations=int(runtime.max_sweep_permutations or PLAY_HAND_SWEEP_PERMUTATION_LIMIT),
    )
    if not sweep_axes or not params:
        return []
    sweep_id = f"{lane.run_id}-{phase}-sweep"
    definition = _sweep_definition_payload(
        lane=lane,
        runtime=runtime,
        reward_matrix=reward_matrix,
        axes=sweep_axes,
        instruments=instruments,
        profile_ref=profile_ref,
        profile_payload=profile_payload,
        lookback_months=lookback_months,
        mode=mode,
    )
    tasks: list[dict[str, Any]] = []
    shard_size = max(int(runtime.sweep_shard_size), 1)
    for shard_index, start in enumerate(range(0, len(params), shard_size)):
        chunk = params[start : start + shard_size]
        task_id = f"{lane.run_id}-task-{len(lane.task_ids) + 1:05d}-{phase}-shard-{shard_index:04d}"
        shard_id = f"{sweep_id}-shard-{shard_index:04d}"
        params_by_index = {start + offset: param for offset, param in enumerate(chunk)}
        payload = {
            "schema_version": "sweep-shard-job-v1",
            "shard_id": shard_id,
            "sweep_id": sweep_id,
            "user_id": DEFAULT_LAB_USER_ID,
            "priority": "research",
            "work_class": "sweep_shard",
            "source_kind": "workspace_attempt",
            "client_origin": PLAY_HAND_LAB_RUNNER,
            "definition": definition,
            "base_profile_snapshot": _copy_profile_payload(profile_payload),
            "permutation_start": start,
            "permutation_count": len(chunk),
            "permutation_indices": list(params_by_index),
            "params_by_index": params_by_index,
            "retention_ttl_seconds": 86400,
            "required_worker_contract_hash": worker_contract_hash,
            "required_worker_contract_schema": runtime.worker_contract_schema,
            "required_capabilities": ["deep_replay", "sweep_shard"],
            "result_detail": "summary",
        }
        _register_task_spec(
            lane,
            task_id=task_id,
            phase=phase,
            task_kind="sweep_shard",
            spec={
                "sweep_id": sweep_id,
                "shard_id": shard_id,
                "profile_path": str(profile_path.resolve()),
                "profile_ref": profile_ref,
                "profile_payload": _copy_profile_payload(profile_payload),
                "instruments": list(instruments),
                "timeframe": lane.incumbent_timeframe or lane.timeframe,
                "lookback_months": lookback_months,
                "axes": list(axis_plan.axes),
                "axis_key_map": {
                    _sweep_axis_key(axis): str(axis.get("lab_axis_key") or "")
                    for axis in sweep_axes
                },
                "axis_plan": axis_plan.event_payload(),
                "expanded_permutation_count": len(params),
                "permutation_budget_applied": axis_plan.selected_permutations > len(params),
                "permutation_start": start,
                "permutation_count": len(chunk),
                "params_by_index": params_by_index,
            },
        )
        tasks.append(
            {
                "task_id": task_id,
                "lane_id": lane.lane_id,
                "attempt_id": task_id,
                "task_kind": "sweep_shard",
                "payload": payload,
                "required_worker_capabilities": [
                    "deep_replay",
                    "sweep_shard",
                    PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
                ],
                "deadline_seconds": runtime.deadline_seconds,
                "max_attempts": runtime.max_attempts,
            }
        )
    return tasks


def _build_tasks(
    lanes: list[LabLaneState],
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str | None = None,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for lane in lanes:
        if (
            runtime.task_mode == "deep_replay"
            and runtime.pipeline_mode == PLAY_HAND_LAB_PLAY_HAND_PIPELINE
        ):
            if not worker_contract_hash:
                raise RuntimeError("Deep-replay lab tasks require a worker contract hash.")
            lane.current_phase = "baseline"
            tasks.append(
                _make_deep_replay_task(
                    lane,
                    phase="baseline_3mo",
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                    profile_payload=lane.profile_payload or {},
                    profile_path=lane.profile_path,
                    profile_ref=lane.profile_ref or lane.run_id,
                    instruments=list(lane.instruments),
                    timeframe=lane.timeframe,
                    lookback_months=runtime.lookback_months,
                )
            )
            continue
        for task_index in range(runtime.tasks_per_lane):
            task_id = f"{lane.run_id}-task-{task_index + 1:05d}"
            if runtime.task_mode == "fake_compute":
                required_worker_capabilities = [
                    PLAY_HAND_LAB_FAKE_COMPUTE_CAPABILITY,
                    PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
                ]
                payload = {
                    "task_id": task_id,
                    "lane_id": lane.lane_id,
                    "attempt_id": task_id,
                    "task_kind": "fake_compute",
                    "work_seconds": runtime.fake_work_seconds,
                    "required_capabilities": required_worker_capabilities,
                }
            else:
                if not worker_contract_hash:
                    raise RuntimeError("Deep-replay lab tasks require a worker contract hash.")
                required_worker_capabilities = [
                    "deep_replay",
                    PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY,
                ]
                payload = _deep_replay_job_payload(
                    task_id=task_id,
                    lane=lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            _register_task_spec(
                lane,
                task_id=task_id,
                phase=runtime.task_mode,
                task_kind=runtime.task_mode,
                spec={
                    "profile_path": str(lane.profile_path.resolve()) if lane.profile_path else None,
                    "profile_ref": lane.profile_ref,
                    "profile_payload": _copy_profile_payload(lane.profile_payload),
                    "instruments": list(lane.instruments),
                    "timeframe": lane.timeframe,
                    "lookback_months": runtime.lookback_months,
                },
            )
            tasks.append(
                {
                    "task_id": task_id,
                    "lane_id": lane.lane_id,
                    "attempt_id": task_id,
                    "task_kind": runtime.task_mode,
                    "payload": payload,
                    "required_worker_capabilities": required_worker_capabilities,
                    "deadline_seconds": runtime.deadline_seconds,
                    "max_attempts": runtime.max_attempts,
                }
            )
    return tasks


def _sensitivity_response_from_worker_result(
    worker_result: dict[str, Any],
    *,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, Any]:
    result = worker_result.get("result")
    data = result if isinstance(result, dict) else worker_result
    return {
        "status": str(worker_result.get("status") or "success"),
        "message": "PlayHand Lab deep replay completed via lab worker gateway.",
        "requested_timeframe": runtime.timeframe,
        "effective_timeframe": lane.timeframe,
        "data": data,
    }


def _score_lab_artifact(
    *,
    cli: FuzzfolioCli,
    artifact_dir: Path,
    strict: bool,
) -> tuple[AttemptScore, dict[str, Any] | None]:
    try:
        compare_payload = cli.score_artifact(artifact_dir)
        snapshot = load_sensitivity_snapshot(artifact_dir)
        return build_attempt_score(compare_payload, snapshot), None
    except Exception as exc:
        if strict:
            raise
        return (
            AttemptScore(
                primary_score=None,
                composite_score=None,
                score_basis="lab_scoring_failed",
                metrics={},
                best_summary={"error": str(exc)[:500]},
            ),
            {"error": str(exc)[:1000], "error_type": type(exc).__name__},
        )


def _fake_attempt_score(result: dict[str, Any]) -> AttemptScore:
    return AttemptScore(
        primary_score=None,
        composite_score=None,
        score_basis="fake_compute_smoke",
        metrics={},
        best_summary={"status": result.get("status"), "task_id": result.get("task_id")},
    )


def _record_lab_result(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    lab_result: dict[str, Any],
    reward_matrix: dict[str, Any] | None,
) -> dict[str, Any]:
    task_id = str(lab_result.get("task_id") or "")
    task_spec = lane.task_specs.get(task_id, {})
    task_kind = str(task_spec.get("task_kind") or runtime.task_mode)
    phase = str(task_spec.get("phase") or task_kind)
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_{phase}_{task_id}_{_utc_stamp()}").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if runtime.retain_raw_lab_artifacts:
        _write_json(artifact_dir / "lab-result.json", lab_result)
        _write_json(artifact_dir / "lab-worker-result.json", dict(result_payload))
    score_warning: dict[str, Any] | None = None
    sensitivity_snapshot_path: Path | None = None
    sweep_payload: dict[str, Any] | None = None
    if task_kind == "deep_replay":
        sensitivity_snapshot_path = artifact_dir / "sensitivity-response.json"
        _write_json(
            sensitivity_snapshot_path,
            _sensitivity_response_from_worker_result(
                result_payload,
                lane=lane,
                runtime=runtime,
            ),
        )
        request_payload = result_payload.get("request") if isinstance(result_payload.get("request"), dict) else None
        if isinstance(request_payload, dict):
            _write_json(artifact_dir / "deep-replay-job.json", {"request": request_payload, **result_payload})
        attempt_score, score_warning = _score_lab_artifact(
            cli=cli,
            artifact_dir=artifact_dir,
            strict=runtime.strict_scoring,
        )
    elif task_kind == "sweep_shard":
        sweep_payload = _rank_sweep_permutations(
            phase=phase,
            shard_results=[result_payload],
        )
        if runtime.retain_raw_lab_artifacts:
            _write_json(artifact_dir / "sweep-shard-result.json", _sweep_payload_from_worker_result(result_payload))
        _write_json(artifact_dir / "sweep-results.json", sweep_payload)
        score = _as_float((sweep_payload.get("best") or {}).get("score"))
        attempt_score = AttemptScore(
            primary_score=score,
            composite_score=score,
            score_basis="lab_sweep_shard",
            metrics={"score_lab": score},
            best_summary=dict(sweep_payload.get("best") or {}),
        )
    else:
        attempt_score = _fake_attempt_score(result_payload)
    attempts = load_attempts(lane_ctx.attempts_path)
    play_hand_role = (
        _play_hand_role_for_phase(phase)
        if task_kind in {"deep_replay", "sweep_shard"}
        else "lab_smoke"
    )
    profile_path_raw = task_spec.get("profile_path")
    profile_path = Path(str(profile_path_raw)) if profile_path_raw else lane.profile_path
    record = make_attempt_record(
        config,
        lane_ctx.attempts_path,
        lane.run_id,
        artifact_dir,
        attempt_score,
        candidate_name=f"{lane.lane_id}_{phase}_{len(attempts) + 1:05d}",
        profile_ref=str(task_spec.get("profile_ref") or lane.profile_ref or ""),
        profile_path=profile_path,
        sensitivity_snapshot_path=sensitivity_snapshot_path,
        note=f"{PLAY_HAND_LAB_RUNNER}:{lane.lane_id}:{phase}:{task_id}",
        requested_horizon_months=int(task_spec.get("lookback_months") or runtime.lookback_months),
        requested_timeframe=runtime.timeframe,
        effective_timeframe=str(task_spec.get("timeframe") or lane.timeframe),
        max_reward_r=(
            reward_matrix.get("requested_max_reward_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        reward_matrix=dict(reward_matrix) if isinstance(reward_matrix, dict) else None,
        reward_step_r=(
            reward_matrix.get("reward_step_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        reward_columns=(
            reward_matrix.get("reward_columns")
            if isinstance(reward_matrix, dict)
            else None
        ),
        effective_max_reward_r=(
            reward_matrix.get("effective_max_reward_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        job_status=str(result_payload.get("status") or lab_result.get("status") or "success"),
        runner=PLAY_HAND_RUNNER,
        attempt_role=play_hand_role,
        attempt_decision="stage_candidate",
        attempt_decision_reasons=[f"play_hand_lab_{task_kind}_result"],
        strategy_family_id=lane.run_id,
        canonical_attempt_id=None,
        is_canonical_attempt=False,
        is_canonical_playhand_attempt=False,
        play_hand_role=play_hand_role,
        play_hand_stage=phase,
        play_hand_selected_instruments=list(task_spec.get("instruments") or lane.instruments),
    )
    row = asdict(record)
    row.update(
        {
            "generated_by_runner": PLAY_HAND_LAB_RUNNER,
            "lab_campaign_task_id": task_id,
            "lab_lane_id": lane.lane_id,
            "lab_task_kind": task_kind,
            "play_hand_phase": phase,
            "worker_id": lab_result.get("worker_id"),
            "worker_lease_id": lab_result.get("lease_id"),
            "run_status": "failed" if score_warning else "screened",
        }
    )
    if score_warning:
        row["lab_scoring_warning"] = score_warning
    attempts.append(row)
    write_attempts(lane_ctx.attempts_path, attempts)
    if record.composite_score is not None and (
        lane.best_score is None or record.composite_score > lane.best_score
    ):
        lane.best_score = record.composite_score
        lane.best_attempt_id = record.attempt_id
    render_progress_artifacts(
        attempts,
        lane.run_dir / "progress.png",
        run_metadata_path=lane.run_dir / "run-metadata.json",
        lower_is_better=config.research.plot_lower_is_better,
    )
    _append_event(
        lane_ctx,
        "lab_result",
        "recorded",
        task_id=task_id,
        artifact_dir=str(artifact_dir),
        attempt_id=record.attempt_id,
        score=record.composite_score,
        score_basis=record.score_basis,
        scoring_warning=score_warning,
    )
    return {
        "task_id": task_id,
        "attempt_id": record.attempt_id,
        "artifact_dir": str(artifact_dir),
        "score": record.composite_score,
        "score_basis": record.score_basis,
        "status": "failed" if score_warning else "success",
        "phase": phase,
        "task_kind": task_kind,
        "profile_path": str(profile_path.resolve()) if profile_path else None,
        "profile_ref": str(task_spec.get("profile_ref") or lane.profile_ref or ""),
        "instruments": list(task_spec.get("instruments") or lane.instruments),
        "timeframe": str(task_spec.get("timeframe") or lane.timeframe),
        "lookback_months": int(task_spec.get("lookback_months") or runtime.lookback_months),
        "sweep_payload": sweep_payload if task_kind == "sweep_shard" else None,
    }


def _is_failed_lab_result(lab_result: dict[str, Any]) -> bool:
    status = str(lab_result.get("status") or "").lower()
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    worker_status = str(result_payload.get("status") or "").lower()
    return status in {"failed", "error"} or worker_status in {"failed", "error"}


def _record_lab_failure(
    *,
    config: AppConfig,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    lab_result: dict[str, Any],
    reward_matrix: dict[str, Any] | None,
) -> dict[str, Any]:
    task_id = str(lab_result.get("task_id") or "")
    task_spec = lane.task_specs.get(task_id, {})
    phase = str(task_spec.get("phase") or runtime.task_mode)
    task_kind = str(task_spec.get("task_kind") or runtime.task_mode)
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    error = str(result_payload.get("error") or lab_result.get("error") or "lab_worker_failed")
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_failed_{task_id}_{_utc_stamp()}").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if runtime.retain_raw_lab_artifacts:
        _write_json(artifact_dir / "lab-result.json", lab_result)
    _write_json(
        artifact_dir / "lab-failure.json",
        {
            "task_id": task_id,
            "lane_id": lane.lane_id,
            "status": "failed",
            "error": error[:1000],
            "worker_id": lab_result.get("worker_id"),
            "worker_lease_id": lab_result.get("lease_id"),
        },
    )
    attempts = load_attempts(lane_ctx.attempts_path)
    play_hand_role = "lab_replay" if runtime.task_mode == "deep_replay" else "lab_smoke"
    attempt_score = AttemptScore(
        primary_score=None,
        composite_score=None,
        score_basis="lab_worker_failed",
        metrics={},
        best_summary={"status": "failed", "error": error[:500], "task_id": task_id},
    )
    record = make_attempt_record(
        config,
        lane_ctx.attempts_path,
        lane.run_id,
        artifact_dir,
        attempt_score,
        candidate_name=f"{lane.lane_id}_{runtime.task_mode}_failed_{len(attempts) + 1:05d}",
        profile_ref=lane.profile_ref,
        profile_path=lane.profile_path,
        sensitivity_snapshot_path=None,
        note=f"{PLAY_HAND_LAB_RUNNER}:{lane.lane_id}:{task_id}:failed",
        requested_horizon_months=runtime.lookback_months,
        requested_timeframe=runtime.timeframe,
        effective_timeframe=lane.timeframe,
        max_reward_r=(
            reward_matrix.get("requested_max_reward_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        reward_matrix=dict(reward_matrix) if isinstance(reward_matrix, dict) else None,
        reward_step_r=(
            reward_matrix.get("reward_step_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        reward_columns=(
            reward_matrix.get("reward_columns")
            if isinstance(reward_matrix, dict)
            else None
        ),
        effective_max_reward_r=(
            reward_matrix.get("effective_max_reward_r")
            if isinstance(reward_matrix, dict)
            else None
        ),
        job_status="failed",
        runner=PLAY_HAND_RUNNER,
        attempt_role=play_hand_role,
        attempt_decision="failed_candidate",
        attempt_decision_reasons=["play_hand_lab_worker_failed"],
        strategy_family_id=lane.run_id,
        canonical_attempt_id=None,
        is_canonical_attempt=False,
        is_canonical_playhand_attempt=False,
        play_hand_role=play_hand_role,
        play_hand_stage="PlayHand Lab",
        play_hand_selected_instruments=list(lane.instruments),
    )
    row = asdict(record)
    row.update(
        {
            "generated_by_runner": PLAY_HAND_LAB_RUNNER,
            "lab_campaign_task_id": task_id,
            "lab_lane_id": lane.lane_id,
            "worker_id": lab_result.get("worker_id"),
            "worker_lease_id": lab_result.get("lease_id"),
            "run_status": "failed",
            "lab_failure": {"error": error[:1000]},
        }
    )
    attempts.append(row)
    write_attempts(lane_ctx.attempts_path, attempts)
    render_progress_artifacts(
        attempts,
        lane.run_dir / "progress.png",
        run_metadata_path=lane.run_dir / "run-metadata.json",
        lower_is_better=config.research.plot_lower_is_better,
    )
    _append_event(
        lane_ctx,
        "lab_result",
        "failed",
        task_id=task_id,
        artifact_dir=str(artifact_dir),
        attempt_id=record.attempt_id,
        error=error[:1000],
    )
    return {
        "task_id": task_id,
        "attempt_id": record.attempt_id,
        "artifact_dir": str(artifact_dir),
        "score": None,
        "score_basis": "lab_worker_failed",
        "status": "failed",
        "phase": phase,
        "task_kind": task_kind,
    }


def _phase_terminal(lane: LabLaneState, phase: str) -> bool:
    task_ids = lane.phase_task_ids.get(phase) or []
    return bool(task_ids) and all(
        task_id in lane.completed_task_ids or task_id in lane.failed_task_ids
        for task_id in task_ids
    )


def _phase_failed(lane: LabLaneState, phase: str) -> bool:
    return any(task_id in lane.failed_task_ids for task_id in lane.phase_task_ids.get(phase) or [])


def _write_stage_metadata(lane: LabLaneState, lane_ctx: PlayHandContext) -> None:
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "pipeline_version": PLAY_HAND_LAB_PIPELINE_VERSION,
            "current_phase": lane.current_phase,
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "stage_incumbent": {
                "profile_path": (
                    str(lane.incumbent_profile_path.resolve())
                    if lane.incumbent_profile_path
                    else None
                ),
                "profile_ref": lane.incumbent_profile_ref,
                "evaluation_timeframe": lane.incumbent_timeframe,
                "instruments": list(lane.incumbent_instruments),
                "score": lane.incumbent_score,
                "phase": lane.incumbent_phase,
            },
            "instrument_scout": lane.instrument_scout_result,
        }
    )
    write_run_metadata(lane.run_dir, metadata)


def _map_sweep_parameters_to_profile_axes(
    lane: LabLaneState,
    *,
    phase: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    axis_key_map: dict[str, str] = {}
    for task_id in lane.phase_task_ids.get(phase) or []:
        spec = lane.task_specs.get(task_id) or {}
        mapping = spec.get("axis_key_map")
        if isinstance(mapping, dict):
            axis_key_map.update({str(key): str(value) for key, value in mapping.items() if value})
    mapped: dict[str, Any] = {}
    for key, value in parameters.items():
        mapped_key = axis_key_map.get(str(key), str(key))
        mapped[mapped_key] = value
    return mapped


def _map_parameter_importance_to_profile_axes(
    lane: LabLaneState,
    *,
    phase: str,
    importance: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    axis_key_map: dict[str, str] = {}
    for task_id in lane.phase_task_ids.get(phase) or []:
        spec = lane.task_specs.get(task_id) or {}
        mapping = spec.get("axis_key_map")
        if isinstance(mapping, dict):
            axis_key_map.update({str(key): str(value) for key, value in mapping.items() if value})
    mapped: list[dict[str, Any]] = []
    for item in importance:
        if not isinstance(item, dict):
            continue
        clone = dict(item)
        axis = str(clone.get("axis") or "")
        clone["axis"] = axis_key_map.get(axis, axis)
        mapped.append(clone)
    return mapped


def _materialize_sweep_winner(
    *,
    config: AppConfig,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    phase: str,
    sweep_payload: dict[str, Any],
) -> tuple[Path, str, dict[str, Any], dict[str, Any]] | None:
    best = sweep_payload.get("best")
    if not isinstance(best, dict):
        ranked = sweep_payload.get("ranked_permutations") or sweep_payload.get("ranked") or []
        best = ranked[0] if isinstance(ranked, list) and ranked and isinstance(ranked[0], dict) else {}
    parameters = best.get("parameters") if isinstance(best.get("parameters"), dict) else {}
    if not parameters or lane.incumbent_profile_path is None:
        return None
    mapped = _map_sweep_parameters_to_profile_axes(lane, phase=phase, parameters=dict(parameters))
    output_path = lane_ctx.profiles_dir / f"{phase}_top.json"
    materialize_profile_variant(
        lane.incumbent_profile_path,
        output_path,
        mapped,
        name_suffix=f"[{phase} top]",
    )
    worker_payload = _worker_ready_profile_snapshot(
        _inner_profile_payload(_load_json(output_path)),
        config=config,
        runtime=runtime,
    )
    profile_ref = f"lab-inline:{lane.run_id}:{lane.lane_id}:{phase}"
    return output_path, profile_ref, worker_payload, mapped


def _accept_stage_candidate(
    lane: LabLaneState,
    *,
    lane_ctx: PlayHandContext,
    stage_key: str,
    phase_score_key: str,
    candidate_profile_path: Path,
    candidate_profile_ref: str,
    candidate_profile_payload: dict[str, Any],
    candidate_timeframe: str,
    candidate_score: Any,
    detail: str,
) -> dict[str, Any]:
    decision = build_stage_acceptance_decision(
        stage=stage_key,
        incumbent_score=lane.incumbent_score,
        candidate_score=candidate_score,
    )
    decision.update(
        {
            "candidate_profile_path": str(candidate_profile_path.resolve()),
            "candidate_profile_ref": candidate_profile_ref,
            "candidate_evaluation_timeframe": candidate_timeframe,
            "incumbent_profile_path": (
                str(lane.incumbent_profile_path.resolve())
                if lane.incumbent_profile_path
                else None
            ),
            "incumbent_profile_ref": lane.incumbent_profile_ref,
            "incumbent_phase": lane.incumbent_phase,
        }
    )
    metadata = load_run_metadata(lane.run_dir)
    decisions = metadata.setdefault("stage_acceptance_decisions", [])
    if not isinstance(decisions, list):
        decisions = []
        metadata["stage_acceptance_decisions"] = decisions
    decisions.append(decision)
    write_run_metadata(lane.run_dir, metadata)
    if decision.get("accepted"):
        lane.incumbent_profile_path = candidate_profile_path
        lane.incumbent_profile_ref = candidate_profile_ref
        lane.incumbent_profile_payload = _copy_profile_payload(candidate_profile_payload)
        lane.incumbent_timeframe = candidate_timeframe
        lane.incumbent_score = _as_float(candidate_score)
        lane.incumbent_phase = phase_score_key
    _append_event(
        lane_ctx,
        "stage_acceptance",
        "accepted" if decision.get("accepted") else "rejected",
        **decision,
    )
    _append_phase_row(
        lane,
        phase=stage_key.replace("_", " "),
        status="top evaluated" if decision.get("accepted") else "top rejected",
        score=candidate_score,
        detail=detail,
    )
    _record_phase_score(lane, phase_score_key, candidate_score)
    return decision


def _enqueue_lookback_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> list[dict[str, Any]]:
    profile_payload = lane.incumbent_profile_payload or lane.profile_payload or {}
    axes = build_timing_axes(profile_payload)
    if not axes or lane.incumbent_profile_path is None or lane.incumbent_profile_ref is None:
        lane.current_phase = "lookback_skipped"
        _append_phase_row(lane, phase="lookback", status="skipped", detail="no timing axes")
        return []
    lane.current_phase = "lookback"
    return _make_sweep_shard_tasks(
        lane,
        phase="lookback_timing",
        runtime=runtime,
        reward_matrix=reward_matrix,
        worker_contract_hash=worker_contract_hash,
        profile_payload=profile_payload,
        profile_path=lane.incumbent_profile_path,
        profile_ref=lane.incumbent_profile_ref,
        instruments=list(lane.incumbent_instruments or lane.instruments),
        lookback_months=runtime.lookback_months,
        axis_texts=axes,
        mode="deterministic",
    )


def _enqueue_coarse_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
    phase: str = "coarse",
    budget: int | None = None,
) -> list[dict[str, Any]]:
    profile_payload = lane.incumbent_profile_payload or lane.profile_payload or {}
    axes = build_coarse_axes(profile_payload)
    if not axes or lane.incumbent_profile_path is None or lane.incumbent_profile_ref is None:
        lane.current_phase = "coarse_skipped"
        _append_phase_row(lane, phase="coarse", status="skipped", detail="no numeric talib axes")
        return []
    lane.current_phase = phase
    effective_runtime = runtime
    if budget is not None:
        effective_runtime = replace(runtime, max_sweep_permutations=max(int(budget), 1))
    return _make_sweep_shard_tasks(
        lane,
        phase=phase,
        runtime=effective_runtime,
        reward_matrix=reward_matrix,
        worker_contract_hash=worker_contract_hash,
        profile_payload=profile_payload,
        profile_path=lane.incumbent_profile_path,
        profile_ref=lane.incumbent_profile_ref,
        instruments=list(lane.incumbent_instruments or lane.instruments),
        lookback_months=runtime.lookback_months,
        axis_texts=axes,
        mode="evolutionary",
    )


def _enqueue_focused_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> list[dict[str, Any]]:
    if not lane.last_sweep_payload or lane.incumbent_profile_path is None or lane.incumbent_profile_ref is None:
        return []
    importance = _map_parameter_importance_to_profile_axes(
        lane,
        phase=str(lane.last_sweep_payload.get("source_phase") or "coarse"),
        importance=_parameter_importance_from_ranked(lane.last_sweep_payload.get("ranked_permutations") or []),
    )
    axes = build_focused_axes(
        importance,
        lane.last_sweep_axes,
    )
    if not axes:
        lane.current_phase = "focused_skipped"
        _append_phase_row(lane, phase="focused", status="skipped", detail="no high-impact axes")
        return []
    lane.current_phase = "focused"
    return _make_sweep_shard_tasks(
        lane,
        phase="focused",
        runtime=runtime,
        reward_matrix=reward_matrix,
        worker_contract_hash=worker_contract_hash,
        profile_payload=lane.incumbent_profile_payload or {},
        profile_path=lane.incumbent_profile_path,
        profile_ref=lane.incumbent_profile_ref,
        instruments=list(lane.incumbent_instruments or lane.instruments),
        lookback_months=runtime.lookback_months,
        axis_texts=axes,
        mode="deterministic",
    )


def _enqueue_instrument_scout_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
    rng: random.Random,
) -> list[dict[str, Any]]:
    if lane.incumbent_profile_ref is None or lane.incumbent_profile_payload is None:
        return []
    current = list(lane.incumbent_instruments or lane.instruments)
    pool = list(runtime.instrument_pool or DEFAULT_INSTRUMENT_POOL)
    candidates = [symbol for symbol in pool if symbol not in current]
    rng.shuffle(candidates)
    selected_candidates = candidates[: max(int(runtime.instrument_scout_size), 1)]
    scout_instruments = current[:1] + selected_candidates
    scout_instruments = list(dict.fromkeys([symbol for symbol in scout_instruments if symbol]))
    lane.current_phase = "instrument_scout"
    tasks: list[dict[str, Any]] = []
    for instrument in scout_instruments:
        tasks.append(
            _make_deep_replay_task(
                lane,
                phase=f"instrument_scout_{instrument}_{runtime.lookback_months}mo",
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
                profile_payload=lane.incumbent_profile_payload,
                profile_path=lane.incumbent_profile_path,
                profile_ref=lane.incumbent_profile_ref,
                instruments=[instrument],
                timeframe=lane.incumbent_timeframe or lane.timeframe,
                lookback_months=runtime.lookback_months,
            )
        )
    return tasks


def _enqueue_final_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> list[dict[str, Any]]:
    if lane.incumbent_profile_ref is None or lane.incumbent_profile_payload is None:
        return []
    lane.current_phase = "scrutiny"
    return [
        _make_deep_replay_task(
            lane,
            phase="final_36mo",
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
            profile_payload=lane.incumbent_profile_payload,
            profile_path=lane.incumbent_profile_path,
            profile_ref=lane.incumbent_profile_ref,
            instruments=list(lane.incumbent_instruments or lane.instruments),
            timeframe=lane.incumbent_timeframe or lane.timeframe,
            lookback_months=runtime.scrutiny_months,
        )
    ]


def _advance_lane_after_result(
    *,
    config: AppConfig,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str | None,
    recorded: dict[str, Any],
) -> list[dict[str, Any]]:
    if runtime.task_mode != "deep_replay" or runtime.pipeline_mode != PLAY_HAND_LAB_PLAY_HAND_PIPELINE:
        return []
    if lane.terminal:
        return []
    if not worker_contract_hash:
        raise RuntimeError("Deep-replay lab stage advancement requires a worker contract hash.")
    phase = str(recorded.get("phase") or "")
    if not phase:
        return []
    lane.phase_results.setdefault(phase, []).append(recorded)
    if not _phase_terminal(lane, phase):
        return []
    if _phase_failed(lane, phase):
        _mark_lane_tombstoned(
            lane,
            lane_ctx=lane_ctx,
            reason="lab_stage_worker_failed",
            reasons=[phase],
        )
        return []

    if phase == "baseline_3mo":
        score = _as_float(recorded.get("score"))
        lane.incumbent_score = score
        lane.incumbent_phase = "baseline_3mo"
        _record_phase_score(lane, "baseline", score)
        _append_phase_row(lane, phase="baseline", status="evaluated", score=score, detail=lane.incumbent_profile_ref or "")
        _write_stage_metadata(lane, lane_ctx)
        decision = _lane_early_exit_decision(
            lane,
            lane_ctx=lane_ctx,
            checkpoint="after_baseline",
            runtime=runtime,
        )
        if decision and decision.get("terminal"):
            _mark_lane_tombstoned(
                lane,
                lane_ctx=lane_ctx,
                reason=PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON,
                reasons=list(decision.get("enforce_reasons") or decision.get("rules_fired") or []),
            )
            return []
        tasks = _enqueue_lookback_stage(
            lane,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
        )
        if not tasks:
            tasks = _enqueue_coarse_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        if not tasks:
            tasks = _enqueue_final_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        return tasks

    if phase in {"lookback_timing", "coarse", "coarse_probe", "coarse_expand", "focused"}:
        payload = _merge_sweep_payloads(
            phase,
            [
                result.get("sweep_payload")
                for result in lane.phase_results.get(phase, [])
                if isinstance(result.get("sweep_payload"), dict)
            ],
        )
        payload["source_phase"] = phase
        lane.last_sweep_payload = payload
        lane.last_sweep_axes = list((lane.task_specs.get((lane.phase_task_ids.get(phase) or [""])[0]) or {}).get("axes") or [])
        materialized = _materialize_sweep_winner(
            config=config,
            lane_ctx=lane_ctx,
            lane=lane,
            runtime=runtime,
            phase=phase,
            sweep_payload=payload,
        )
        if not materialized:
            _append_phase_row(lane, phase=phase.replace("_", " "), status="skipped", detail="no sweep winner")
            _write_stage_metadata(lane, lane_ctx)
            if phase == "lookback_timing":
                return _enqueue_coarse_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                ) or _enqueue_final_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            if phase in {"coarse", "coarse_expand", "coarse_probe"}:
                return _enqueue_final_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            return _enqueue_instrument_scout_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
                rng=random.Random(f"instrument-scout:{runtime.seed}:{lane.lane_index}"),
            )
        candidate_path, candidate_ref, candidate_payload, params = materialized
        candidate_score = _as_float((payload.get("best") or {}).get("score"))
        candidate_timeframe = _lowest_profile_timeframe(candidate_payload, runtime.timeframe)
        stage_key = {
            "lookback_timing": "lookback",
            "coarse": "coarse",
            "coarse_probe": "coarse_probe",
            "coarse_expand": "coarse_expand",
            "focused": "focused",
        }[phase]
        phase_score_key = {
            "lookback_timing": "lookback_top_3mo",
            "coarse": "coarse_top_3mo",
            "coarse_probe": "coarse_probe_top_3mo",
            "coarse_expand": "coarse_top_3mo",
            "focused": "focused_top_3mo",
        }[phase]
        _accept_stage_candidate(
            lane,
            lane_ctx=lane_ctx,
            stage_key=stage_key,
            phase_score_key=phase_score_key,
            candidate_profile_path=candidate_path,
            candidate_profile_ref=candidate_ref,
            candidate_profile_payload=candidate_payload,
            candidate_timeframe=candidate_timeframe,
            candidate_score=candidate_score,
            detail=f"{len(params)} params",
        )
        _write_stage_metadata(lane, lane_ctx)
        checkpoint = {
            "lookback_timing": "after_lookback_top",
            "coarse": "after_coarse_top",
            "coarse_probe": "after_coarse_top",
            "coarse_expand": "after_coarse_top",
            "focused": "after_focused_top",
        }[phase]
        decision = _lane_early_exit_decision(
            lane,
            lane_ctx=lane_ctx,
            checkpoint=checkpoint,
            runtime=runtime,
        )
        if decision and decision.get("terminal"):
            _mark_lane_tombstoned(
                lane,
                lane_ctx=lane_ctx,
                reason=PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON,
                reasons=list(decision.get("enforce_reasons") or decision.get("rules_fired") or []),
            )
            return []
        if phase == "lookback_timing":
            if runtime.coarse_halving_mode == "enforce":
                return _enqueue_coarse_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                    phase="coarse_probe",
                    budget=runtime.coarse_probe_budget,
                ) or _enqueue_final_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            return _enqueue_coarse_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            ) or _enqueue_final_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        if phase == "coarse_probe":
            halving = build_coarse_halving_decision(
                mode=runtime.coarse_halving_mode,
                total_budget=int(runtime.max_sweep_permutations or PLAY_HAND_SWEEP_PERMUTATION_LIMIT),
                probe_budget=runtime.coarse_probe_budget,
                incumbent_score=lane.phase_scores.get("baseline"),
                probe_score=candidate_score,
            )
            metadata = load_run_metadata(lane.run_dir)
            metadata["coarse_halving"] = halving
            write_run_metadata(lane.run_dir, metadata)
            if halving.get("expanded"):
                return _enqueue_coarse_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                    phase="coarse_expand",
                    budget=max(int(halving.get("expand_budget") or 1), 1),
                )
            lane.skip_focused_and_scout = True
            return _enqueue_final_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        if phase in {"coarse", "coarse_expand"}:
            return _enqueue_focused_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            ) or _enqueue_instrument_scout_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
                rng=random.Random(f"instrument-scout:{runtime.seed}:{lane.lane_index}"),
            )
        if phase == "focused":
            return _enqueue_instrument_scout_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
                rng=random.Random(f"instrument-scout:{runtime.seed}:{lane.lane_index}"),
            )

    if phase.startswith("instrument_scout_"):
        scout_phase_ids = [
            task_id
            for scout_phase, task_ids in lane.phase_task_ids.items()
            if scout_phase.startswith("instrument_scout_")
            for task_id in task_ids
        ]
        if not scout_phase_ids or not all(
            task_id in lane.completed_task_ids or task_id in lane.failed_task_ids
            for task_id in scout_phase_ids
        ):
            return []
        scout_results = [
            item
            for phase_name, items in lane.phase_results.items()
            if phase_name.startswith("instrument_scout_")
            for item in items
            if item.get("status") == "success"
        ]
        ranked = sorted(
            scout_results,
            key=lambda item: _as_float(item.get("score")) or float("-inf"),
            reverse=True,
        )
        accepted = [
            item
            for item in ranked
            if (_as_float(item.get("score")) or float("-inf")) >= INSTRUMENT_SCOUT_MIN_SCORE
        ][: max(int(runtime.instrument_scout_max_selected), 1)]
        if not accepted and ranked:
            top_score = _as_float(ranked[0].get("score")) or float("-inf")
            accepted = [
                item
                for item in ranked
                if (_as_float(item.get("score")) or float("-inf")) >= top_score - INSTRUMENT_SCOUT_SCORE_TOLERANCE
            ][: max(int(runtime.instrument_scout_max_selected), 1)]
        selected = [
            str((item.get("instruments") or [""])[0]).strip().upper()
            for item in accepted
            if item.get("instruments")
        ]
        if selected:
            lane.incumbent_instruments = selected
        lane.instrument_scout_result = {
            "version": "instrument_scout_v1",
            "status": "completed",
            "selected_instruments": list(lane.incumbent_instruments),
            "accepted": accepted,
            "rejected": [item for item in ranked if item not in accepted],
        }
        _append_phase_row(
            lane,
            phase="instrument scout",
            status="completed",
            detail=", ".join(lane.incumbent_instruments),
        )
        _write_stage_metadata(lane, lane_ctx)
        return _enqueue_final_stage(
            lane,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
        )

    if phase == "final_36mo":
        final_score = _as_float(recorded.get("score"))
        lane.final_attempt_id = str(recorded.get("attempt_id") or "") or None
        _record_phase_score(lane, "final_36mo", final_score)
        outcome = _final_scrutiny_outcome({"score": final_score})
        _append_phase_row(
            lane,
            phase="scrutiny",
            status="evaluated" if outcome.get("passed") else "failed",
            score=final_score,
            detail=f"{runtime.scrutiny_months}mo on {', '.join(lane.incumbent_instruments)}",
        )
        if outcome.get("passed"):
            _mark_lane_promoted(lane, lane_ctx=lane_ctx, final_score=final_score)
        else:
            _mark_lane_tombstoned(
                lane,
                lane_ctx=lane_ctx,
                reason=str(outcome.get("reason") or PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON),
                reasons=list(outcome.get("reasons") or []),
            )
        return []
    return []


def _read_gateway_results(gateway: Any, *, limit: int) -> list[dict[str, Any]]:
    reader = getattr(gateway, "read_results", None)
    if callable(reader):
        return reader(limit=limit)
    return gateway.drain_results(limit=limit)


def _ack_gateway_results(gateway: Any, lease_ids: list[str]) -> bool:
    acker = getattr(gateway, "ack_results", None)
    if callable(acker):
        acker(lease_ids)
    return True


def _safe_ack_gateway_results(
    gateway: Any,
    campaign_ctx: PlayHandContext,
    *,
    lease_ids: list[str],
    task_id: str,
) -> bool:
    try:
        return _ack_gateway_results(gateway, lease_ids)
    except Exception as exc:
        _append_event(
            campaign_ctx,
            "result_ack",
            "failed",
            task_id=task_id,
            lease_ids=[lease_id for lease_id in lease_ids if lease_id],
            error=str(exc)[:1000],
        )
        return False


def _add_recorded_result_sample(recorded_results: list[dict[str, Any]], recorded: dict[str, Any]) -> None:
    if len(recorded_results) < max(int(SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT), 0):
        recorded_results.append(recorded)


def _snapshot_metrics(snapshot: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(snapshot, dict):
        return {}
    metrics = snapshot.get("metrics")
    if not isinstance(metrics, dict):
        return {}
    output: dict[str, int] = {}
    for key, value in metrics.items():
        try:
            output[str(key)] = int(value or 0)
        except (TypeError, ValueError):
            continue
    return output


def _metric_delta(snapshot: dict[str, Any] | None, baseline: dict[str, int], key: str) -> int:
    metrics = _snapshot_metrics(snapshot)
    return max(int(metrics.get(key, 0)) - int(baseline.get(key, 0)), 0)


def _campaign_gateway_snapshot(
    snapshot: dict[str, Any] | None,
    *,
    metric_baseline: dict[str, int],
    lanes: list[LabLaneState],
) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    scoped = dict(snapshot)
    raw_metrics = _snapshot_metrics(snapshot)
    scoped["raw_metrics"] = dict(raw_metrics)
    scoped["metrics"] = {
        key: max(int(value) - int(metric_baseline.get(key, 0)), 0)
        for key, value in raw_metrics.items()
    }
    for key in ("queued_tasks", "active_leases", "completed_tasks", "failed_tasks", "live_tasks", "result_backlog"):
        if key in snapshot:
            scoped[f"raw_{key}"] = snapshot.get(key)
    total_tasks = sum(len(lane.task_ids) for lane in lanes)
    completed_tasks = sum(len(lane.completed_task_ids) for lane in lanes)
    failed_tasks = sum(len(lane.failed_task_ids) for lane in lanes)
    scoped["total_tasks"] = total_tasks
    scoped["completed_tasks"] = completed_tasks
    scoped["failed_tasks"] = failed_tasks
    scoped["live_tasks"] = max(total_tasks - completed_tasks - failed_tasks, 0)
    return scoped


def _write_summary(
    campaign_ctx: PlayHandContext,
    lanes: list[LabLaneState],
    *,
    runtime: PlayHandLabRuntimeConfig,
    status: str,
    started_at: str,
    completed_at: str | None,
    gateway_snapshot: dict[str, Any] | None,
    recorded_results: list[dict[str, Any]],
    recorded_result_count: int | None = None,
) -> dict[str, Any]:
    total_tasks = sum(len(lane.task_ids) for lane in lanes)
    completed_tasks = sum(len(lane.completed_task_ids) for lane in lanes)
    failed_tasks = sum(len(lane.failed_task_ids) for lane in lanes)
    total_recorded_results = max(
        int(recorded_result_count) if recorded_result_count is not None else len(recorded_results),
        len(recorded_results),
    )
    summary = {
        "schema_version": PLAY_HAND_LAB_CAMPAIGN_SCHEMA_VERSION,
        "runner": PLAY_HAND_RUNNER,
        "generated_by_runner": PLAY_HAND_LAB_RUNNER,
        "campaign_id": campaign_ctx.run_id,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "gateway_url": runtime.gateway_url,
        "campaign_mode": runtime.campaign_mode,
        "task_mode": runtime.task_mode,
        "pipeline_mode": runtime.pipeline_mode,
        "pipeline_version": PLAY_HAND_LAB_PIPELINE_VERSION,
        "target_runs": runtime.target_runs,
        "active_runs": runtime.active_runs,
        "lane_count": len(lanes),
        "total_tasks": total_tasks,
        "completed_tasks": completed_tasks,
        "failed_tasks": failed_tasks,
        "recorded_result_count": total_recorded_results,
        "recorded_results_sample_limit": max(int(SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT), 0),
        "recorded_results_truncated": total_recorded_results > len(recorded_results),
        "best_score": max(
            [lane.best_score for lane in lanes if lane.best_score is not None],
            default=None,
        ),
        "lanes": [
            {
                "lane_id": lane.lane_id,
                "run_id": lane.run_id,
                "run_dir": str(lane.run_dir.resolve()),
                "task_ids": list(lane.task_ids),
                "completed_task_count": len(lane.completed_task_ids),
                "failed_task_count": len(lane.failed_task_ids),
                "current_phase": lane.current_phase,
                "terminal": lane.terminal,
                "run_promoted": lane.run_promoted,
                "tombstone_reason": lane.tombstone_reason,
                "phase_scores": dict(lane.phase_scores),
                "best_score": lane.best_score,
                "best_attempt_id": lane.best_attempt_id,
                "instruments": list(lane.instruments),
                "indicators": list(lane.indicator_ids),
            }
            for lane in lanes
        ],
        "recorded_results": recorded_results,
        "gateway_snapshot": gateway_snapshot,
    }
    _write_json(campaign_ctx.summary_path, summary)
    return summary


def cmd_play_hand_lab(runtime: PlayHandLabRuntimeConfig | None = None) -> int:
    runtime = _normalize_runtime(runtime or PlayHandLabRuntimeConfig())
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    gateway = LabGatewayClient(base_url=runtime.gateway_url, token=runtime.gateway_token)
    worker_contract_hash = _resolve_worker_contract_hash(config=config, runtime=runtime)
    if worker_contract_hash and worker_contract_hash != runtime.worker_contract_hash:
        runtime = replace(runtime, worker_contract_hash=worker_contract_hash)
    started_at = _now_iso()
    campaign_id = _campaign_run_id()
    campaign_dir = _derived_campaign_root(config) / campaign_id
    campaign_ctx = _campaign_context(
        config=config,
        cli=cli,
        campaign_id=campaign_id,
        campaign_dir=campaign_dir,
        runtime=runtime,
    )
    campaign_dir.mkdir(parents=True, exist_ok=True)
    _write_campaign_metadata(campaign_ctx, runtime=runtime, status="starting", started_at=started_at)
    _append_event(
        campaign_ctx,
        "campaign",
        "starting",
        runtime=_runtime_event_payload(runtime),
        worker_contract_hash=worker_contract_hash,
    )

    try:
        health = gateway.health()
        if not health.get("ok"):
            raise RuntimeError(f"Lab gateway health check failed: {health}")
    except Exception as exc:
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="failed",
            started_at=started_at,
            extra={"failed_reason": "gateway_unreachable", "error": str(exc)[:1000]},
        )
        raise

    try:
        seed_indicators, seed_plan, seed_plan_path = _seed_indicators(
            config=config,
            cli=cli,
            campaign_ctx=campaign_ctx,
            runtime=runtime,
        )
    except Exception as exc:
        _append_event(
            campaign_ctx,
            "seed_indicators",
            "failed",
            error=str(exc)[:1000],
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="failed",
            started_at=started_at,
            extra={"failed_reason": "seed_indicator_preflight_failed", "error": str(exc)[:1000]},
        )
        raise
    reward_matrix = play_hand_reward_matrix(runtime.max_reward_r)
    lanes: list[LabLaneState] = []
    tasks: list[dict[str, Any]] = []
    lanes_by_task: dict[str, LabLaneState] = {}
    lane_contexts: dict[str, PlayHandContext] = {}
    target_runs = runtime.target_runs if runtime.campaign_mode == "finite" else None
    active_runs = max(int(runtime.active_runs or 1), 1)
    observed_worker_slots = 0

    def enqueue_chunk_run_limit() -> int:
        pressure_slots = observed_worker_slots if observed_worker_slots > 0 else active_runs
        return max(1, min(active_runs, max(16, pressure_slots * 2)))

    def lane_prepare_worker_count(count: int) -> int:
        raw = os.getenv("PLAY_HAND_LAB_PREPARE_WORKERS")
        if raw:
            try:
                return max(1, min(int(count), int(raw)))
            except ValueError:
                pass
        cpu_count = os.cpu_count() or 4
        return max(1, min(int(count), max(4, min(16, cpu_count))))

    def build_lane(lane_index: int) -> tuple[LabLaneState, PlayHandContext]:
        lane_id = f"lane_{lane_index:03d}"
        run_id = _lane_run_id(lane_index)
        run_dir = config.runs_root / run_id
        lane_cli = FuzzfolioCli(config.fuzzfolio)
        lane_ctx = _new_context(
            config=config,
            cli=lane_cli,
            run_id=run_id,
            run_dir=run_dir,
            runtime=runtime,
        )
        lane_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
        lane_ctx.evals_dir.mkdir(parents=True, exist_ok=True)
        lane = LabLaneState(
            lane_id=lane_id,
            lane_index=lane_index,
            run_id=run_id,
            run_dir=run_dir,
        )
        lane_rng = random.Random(f"play-hand-lab:{runtime.seed}:{lane_index}")
        deal = _deal_lane(
            config=config,
            runtime=runtime,
            seed_indicators=seed_indicators,
            seed_plan=seed_plan,
            rng=lane_rng,
        )
        _prepare_lane_profile(
            lane_ctx,
            runtime=runtime,
            lane=lane,
            seed_plan=seed_plan,
            deal=deal,
            rng=lane_rng,
        )
        _write_lane_metadata(
            lane,
            campaign_ctx=campaign_ctx,
            runtime=runtime,
            status="queued",
            started_at=started_at,
            extra={
                "play_hand_seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
                "play_hand_seed_plan_loaded": seed_plan is not None,
                "reward_matrix": reward_matrix,
                "required_worker_contract_hash": worker_contract_hash,
                "required_worker_contract_schema": runtime.worker_contract_schema,
            },
        )
        _append_event(
            campaign_ctx,
            lane_id,
            "prepared",
            lane_run_id=run_id,
            lane_run_dir=str(run_dir.resolve()),
            indicators=lane.indicator_ids,
            instruments=lane.instruments,
            timeframe=lane.timeframe,
        )
        return lane, lane_ctx

    def register_lane(lane: LabLaneState, lane_ctx: PlayHandContext) -> None:
        lanes.append(lane)
        lane_contexts[lane.run_id] = lane_ctx

    def prepare_lane(lane_index: int) -> LabLaneState:
        lane, lane_ctx = build_lane(lane_index)
        register_lane(lane, lane_ctx)
        return lane

    def prepare_lanes(start_index: int, count: int) -> list[LabLaneState]:
        if count <= 0:
            return []
        if count == 1:
            return [prepare_lane(start_index)]
        prepared: list[tuple[int, LabLaneState, PlayHandContext]] = []
        max_workers = lane_prepare_worker_count(count)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_by_offset = {
                executor.submit(build_lane, start_index + offset): offset
                for offset in range(count)
            }
            for future in concurrent.futures.as_completed(future_by_offset):
                offset = future_by_offset[future]
                lane, lane_ctx = future.result()
                prepared.append((offset, lane, lane_ctx))
        prepared.sort(key=lambda item: item[0])
        for _offset, lane, lane_ctx in prepared:
            register_lane(lane, lane_ctx)
        return [lane for _offset, lane, _lane_ctx in prepared]

    def lane_terminal_count(lane: LabLaneState) -> int:
        return len(lane.completed_task_ids) + len(lane.failed_task_ids)

    def lane_run_status(lane: LabLaneState) -> str:
        if lane.terminal:
            if lane.run_promoted:
                return "promoted"
            if lane.tombstone_reason:
                return "tombstoned"
        terminal_count = lane_terminal_count(lane)
        if terminal_count >= len(lane.task_ids) and lane.failed_task_ids:
            return "failed"
        if terminal_count >= len(lane.task_ids):
            return "completed"
        return "running"

    def lane_is_active(lane: LabLaneState) -> bool:
        return bool(lane.task_ids) and lane_terminal_count(lane) < len(lane.task_ids)

    def active_lane_count() -> int:
        return sum(1 for lane in lanes if lane_is_active(lane))

    def can_create_more() -> bool:
        return runtime.campaign_mode == "continuous" or target_runs is None or len(lanes) < target_runs

    def top_up_run_count() -> int:
        deficit = max(active_runs - active_lane_count(), 0)
        if deficit <= 0 or not can_create_more():
            return 0
        if target_runs is not None:
            deficit = min(deficit, max(target_runs - len(lanes), 0))
        return min(deficit, enqueue_chunk_run_limit())

    def enqueue_lanes(new_lanes: list[LabLaneState]) -> None:
        if not new_lanes:
            return
        new_tasks = _build_tasks(
            new_lanes,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
        )
        enqueue_result = gateway.enqueue_tasks(new_tasks)
        tasks.extend(new_tasks)
        for lane in new_lanes:
            for task_id in lane.task_ids:
                lanes_by_task[task_id] = lane
            _write_lane_metadata(
                lane,
                campaign_ctx=campaign_ctx,
                runtime=runtime,
                status="running",
                started_at=started_at,
            )
        _append_event(
            campaign_ctx,
            "gateway",
            "tasks_enqueued",
            enqueue_result=enqueue_result,
            task_count=len(new_tasks),
            total_enqueued_task_count=len(tasks),
            created_run_count=len(lanes),
            active_run_count=active_lane_count(),
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="running",
            started_at=started_at,
            extra={
                "enqueued_task_count": len(tasks),
                "created_run_count": len(lanes),
                "active_run_count": active_lane_count(),
            },
        )

    def enqueue_existing_tasks(new_tasks: list[dict[str, Any]], *, reason: str) -> None:
        if not new_tasks:
            return
        enqueue_result = gateway.enqueue_tasks(new_tasks)
        tasks.extend(new_tasks)
        for task in new_tasks:
            task_id = str(task.get("task_id") or "")
            lane_id = str(task.get("lane_id") or "")
            lane = next((candidate for candidate in lanes if candidate.lane_id == lane_id), None)
            if task_id and lane is not None:
                lanes_by_task[task_id] = lane
        _append_event(
            campaign_ctx,
            "gateway",
            "tasks_enqueued",
            reason=reason,
            enqueue_result=enqueue_result,
            task_count=len(new_tasks),
            total_enqueued_task_count=len(tasks),
            active_run_count=active_lane_count(),
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="running",
            started_at=started_at,
            extra={
                "enqueued_task_count": len(tasks),
                "created_run_count": len(lanes),
                "active_run_count": active_lane_count(),
            },
        )

    def create_and_enqueue_more() -> int:
        count = top_up_run_count()
        if count <= 0:
            return 0
        start_index = len(lanes)
        new_lanes = prepare_lanes(start_index, count)
        enqueue_lanes(new_lanes)
        return len(new_lanes)

    if runtime.dry_run:
        dry_run_count = target_runs if target_runs is not None else active_runs
        for lane_index in range(max(int(dry_run_count or 1), 1)):
            prepare_lane(lane_index)
        tasks = _build_tasks(
            lanes,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
        )
        summary = _write_summary(
            campaign_ctx,
            lanes,
            runtime=runtime,
            status="dry_run",
            started_at=started_at,
            completed_at=_now_iso(),
            gateway_snapshot=None,
            recorded_results=[],
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="dry_run",
            started_at=started_at,
            extra={"summary": summary},
        )
        if runtime.json_output:
            print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
        return 0

    gateway_metric_baseline: dict[str, int] = {}
    try:
        baseline_snapshot = gateway.snapshot()
        gateway_metric_baseline = _snapshot_metrics(baseline_snapshot)
        observed_worker_slots = max(int(baseline_snapshot.get("worker_slots") or 0), 0)
    except requests.RequestException as exc:
        _append_event(campaign_ctx, "gateway", "baseline_snapshot_failed", error=str(exc)[:500])

    create_and_enqueue_more()
    recorded_results: list[dict[str, Any]] = []
    recorded_result_count = 0
    deadline = None if runtime.campaign_mode == "continuous" else time.monotonic() + runtime.max_wait_seconds
    last_snapshot: dict[str, Any] | None = None
    initial_gateway_id: str | None = None
    gateway_restarted = False
    result_loss_detected = False
    gateway_unreachable = False
    interrupted = False
    consecutive_result_read_failures = 0
    try:
        last_snapshot = gateway.snapshot()
        initial_gateway_id = (
            str(last_snapshot.get("gateway_id"))
            if isinstance(last_snapshot.get("gateway_id"), str)
            else None
        )
    except requests.RequestException as exc:
        _append_event(campaign_ctx, "gateway", "initial_snapshot_failed", error=str(exc)[:500])

    try:
        while deadline is None or time.monotonic() < deadline:
            try:
                result_batch = _read_gateway_results(gateway, limit=runtime.result_batch_size)
            except requests.RequestException as exc:
                consecutive_result_read_failures += 1
                _append_event(
                    campaign_ctx,
                    "gateway",
                    "result_read_failed",
                    error=str(exc)[:1000],
                    consecutive_failures=consecutive_result_read_failures,
                    failure_limit=runtime.result_read_failure_limit,
                )
                if consecutive_result_read_failures >= runtime.result_read_failure_limit:
                    gateway_unreachable = True
                    break
                time.sleep(runtime.poll_interval_seconds)
                continue
            consecutive_result_read_failures = 0
            ack_lease_ids: list[str] = []
            for lab_result in result_batch:
                task_id = str(lab_result.get("task_id") or "")
                lane = lanes_by_task.get(task_id)
                lease_id = str(lab_result.get("lease_id") or "")
                if lane is None:
                    _append_event(
                        campaign_ctx,
                        "result_reader",
                        "unknown_task",
                        task_id=task_id,
                        lease_id=lease_id,
                    )
                    ack_lease_ids.append(lease_id)
                    continue
                if task_id in lane.completed_task_ids or task_id in lane.failed_task_ids:
                    ack_lease_ids.append(lease_id)
                    continue
                recorded_successfully = False
                new_stage_tasks: list[dict[str, Any]] = []
                try:
                    if _is_failed_lab_result(lab_result):
                        recorded = _record_lab_failure(
                            config=config,
                            lane_ctx=lane_contexts[lane.run_id],
                            lane=lane,
                            runtime=runtime,
                            lab_result=lab_result,
                            reward_matrix=reward_matrix,
                        )
                        lane.failed_task_ids.add(task_id)
                    else:
                        recorded = _record_lab_result(
                            config=config,
                            cli=cli,
                            lane_ctx=lane_contexts[lane.run_id],
                            lane=lane,
                            runtime=runtime,
                            lab_result=lab_result,
                            reward_matrix=reward_matrix,
                        )
                        if recorded.get("status") == "failed":
                            lane.failed_task_ids.add(task_id)
                        else:
                            lane.completed_task_ids.add(task_id)
                    new_stage_tasks = _advance_lane_after_result(
                        config=config,
                        lane_ctx=lane_contexts[lane.run_id],
                        lane=lane,
                        runtime=runtime,
                        reward_matrix=reward_matrix,
                        worker_contract_hash=worker_contract_hash,
                        recorded=recorded,
                    )
                    recorded_result_count += 1
                    _add_recorded_result_sample(recorded_results, recorded)
                    recorded_successfully = True
                except Exception as exc:
                    _append_event(
                        campaign_ctx,
                        "result_writer",
                        "failed",
                        task_id=task_id,
                        lane_id=lane.lane_id,
                        error=str(exc)[:1000],
                    )
                    if runtime.strict_scoring:
                        raise
                    if recorded_successfully:
                        ack_lease_ids.append(lease_id)
                        continue
                    failure_result = dict(lab_result)
                    original_result = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
                    failure_result["status"] = "failed"
                    failure_result["result"] = {
                        **dict(original_result),
                        "status": "failed",
                        "error": f"result_writer_failed: {str(exc)[:500]}",
                    }
                    recorded = _record_lab_failure(
                        config=config,
                        lane_ctx=lane_contexts[lane.run_id],
                        lane=lane,
                        runtime=runtime,
                        lab_result=failure_result,
                        reward_matrix=reward_matrix,
                    )
                    recorded_result_count += 1
                    _add_recorded_result_sample(recorded_results, recorded)
                    lane.failed_task_ids.add(task_id)
                ack_lease_ids.append(lease_id)
                if new_stage_tasks:
                    enqueue_existing_tasks(new_stage_tasks, reason=f"stage:{_task_phase(lane, task_id)}")
                terminal_count_for_lane = lane_terminal_count(lane)
                _write_lane_metadata(
                    lane,
                    campaign_ctx=campaign_ctx,
                    runtime=runtime,
                    status=lane_run_status(lane),
                    started_at=started_at,
                )
            if ack_lease_ids:
                _safe_ack_gateway_results(
                    gateway,
                    campaign_ctx,
                    lease_ids=ack_lease_ids,
                    task_id="batch",
                )
            create_and_enqueue_more()
            completed_count = sum(len(lane.completed_task_ids) for lane in lanes)
            failed_count = sum(len(lane.failed_task_ids) for lane in lanes)
            terminal_count = completed_count + failed_count
            if runtime.campaign_mode == "finite" and not can_create_more() and tasks and terminal_count >= len(tasks):
                break
            try:
                last_snapshot = gateway.snapshot()
                current_gateway_id = (
                    str(last_snapshot.get("gateway_id"))
                    if isinstance(last_snapshot.get("gateway_id"), str)
                    else None
                )
                if initial_gateway_id and current_gateway_id and current_gateway_id != initial_gateway_id:
                    gateway_restarted = True
                    _append_event(
                        campaign_ctx,
                        "gateway",
                        "restarted",
                        initial_gateway_id=initial_gateway_id,
                        current_gateway_id=current_gateway_id,
                    )
                    break
                if _metric_delta(last_snapshot, gateway_metric_baseline, "results_dropped") > 0:
                    result_loss_detected = True
                    _append_event(
                        campaign_ctx,
                        "gateway",
                        "result_loss_detected",
                        results_dropped=_metric_delta(last_snapshot, gateway_metric_baseline, "results_dropped"),
                    )
                    break
            except requests.RequestException as exc:
                _append_event(campaign_ctx, "gateway", "snapshot_failed", error=str(exc)[:500])
            time.sleep(runtime.poll_interval_seconds)
    except KeyboardInterrupt:
        interrupted = True
        _append_event(campaign_ctx, "campaign", "interrupted")

    completed_count = sum(len(lane.completed_task_ids) for lane in lanes)
    failed_count = sum(len(lane.failed_task_ids) for lane in lanes)
    terminal_count = completed_count + failed_count
    if interrupted:
        status = "stopped"
    elif gateway_unreachable:
        status = "gateway_unreachable"
    elif gateway_restarted:
        status = "gateway_restarted"
    elif result_loss_detected:
        status = "result_loss"
    elif runtime.campaign_mode == "finite" and tasks and terminal_count >= len(tasks) and not can_create_more():
        status = "failed" if failed_count else "completed"
    else:
        status = "timeout"
    completed_at = _now_iso()
    try:
        last_snapshot = gateway.snapshot()
        metrics = last_snapshot.get("metrics") if isinstance(last_snapshot.get("metrics"), dict) else {}
        if _metric_delta(last_snapshot, gateway_metric_baseline, "results_dropped") > 0 and status == "completed":
            status = "result_loss"
    except Exception as exc:
        _append_event(campaign_ctx, "gateway", "final_snapshot_failed", error=str(exc)[:500])
    summary = _write_summary(
        campaign_ctx,
        lanes,
        runtime=runtime,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        gateway_snapshot=_campaign_gateway_snapshot(
            last_snapshot,
            metric_baseline=gateway_metric_baseline,
            lanes=lanes,
        ),
        recorded_results=recorded_results,
        recorded_result_count=recorded_result_count,
    )
    _write_campaign_metadata(
        campaign_ctx,
        runtime=runtime,
        status=status,
        started_at=started_at,
        extra={
            "completed_at": completed_at,
            "completed_task_count": completed_count,
            "failed_task_count": failed_count,
            "total_task_count": len(tasks),
            "summary_path": str(campaign_ctx.summary_path.resolve()),
        },
    )
    _append_event(campaign_ctx, "campaign", status, summary_path=str(campaign_ctx.summary_path.resolve()))
    if runtime.json_output:
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    else:
        console.print(
            "[bold]PlayHand Lab[/bold] "
            f"{status}: {completed_count}/{len(tasks)} completed, {failed_count} failed, "
            f"campaign={campaign_ctx.run_id}"
        )
    return 0 if status in {"completed", "stopped"} else 2


__all__ = [
    "DEFAULT_LAB_GATEWAY_URL",
    "PLAY_HAND_LAB_CAMPAIGNS_DIR",
    "PLAY_HAND_LAB_CAMPAIGN_SCHEMA_VERSION",
    "PLAY_HAND_LAB_LANE_SCHEMA_VERSION",
    "PLAY_HAND_LAB_RUNNER",
    "PLAY_HAND_LAB_FAKE_COMPUTE_CAPABILITY",
    "PLAY_HAND_LAB_WORKER_PROTOCOL_CAPABILITY",
    "PLAY_HAND_LAB_WORKER_PROTOCOL_VERSION",
    "SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT",
    "LabGatewayClient",
    "LabLaneState",
    "PlayHandLabRuntimeConfig",
    "cmd_play_hand_lab",
]
