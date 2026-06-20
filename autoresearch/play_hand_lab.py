from __future__ import annotations

import copy
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import requests
from rich.console import Console

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
    PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    PLAY_HAND_RUNNER,
    PlayHandContext,
    SeedIndicator,
    _append_event,
    _fallback_indicator_deal,
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
    apply_play_hand_profile_defaults,
    apply_role_timeframe_defaults,
    apply_seed_indicator_metadata,
    apply_seed_pair_template_defaults,
    deal_indicator_count,
    deal_instruments,
    deal_seed_plan_indicators,
    play_hand_reward_matrix,
)
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


@dataclass(frozen=True)
class PlayHandLabRuntimeConfig:
    gateway_url: str = DEFAULT_LAB_GATEWAY_URL
    gateway_token: str | None = None
    task_mode: Literal["fake_compute", "deep_replay"] = "deep_replay"
    lanes: int = 4
    tasks_per_lane: int = 1
    timeframe: str = "M5"
    instrument: list[str] | None = None
    instrument_pool: list[str] | None = None
    indicator: list[str] | None = None
    profile_path: Path | None = None
    min_indicators: int = 1
    max_indicators: int = 4
    seed: int | None = None
    lookback_months: int = 3
    bar_limit: int = 5000
    max_reward_r: float | None = None
    fake_work_seconds: float = 1.0
    deadline_seconds: float = 3600.0
    max_attempts: int = 2
    poll_interval_seconds: float = 1.0
    max_wait_seconds: float = 3600.0
    result_batch_size: int = 500
    dry_run: bool = False
    strict_scoring: bool = False
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
    token = runtime.gateway_token or os.environ.get("FUZZFOLIO_LAB_GATEWAY_TOKEN")
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
    lanes = max(int(runtime.lanes), 1)
    tasks_per_lane = max(int(runtime.tasks_per_lane), 1)
    if task_mode == "deep_replay" and tasks_per_lane != 1:
        raise ValueError("Deep-replay lab mode requires --tasks-per-lane 1; increase --lanes for more work.")
    min_indicators = max(int(runtime.min_indicators), 1)
    max_indicators = max(int(runtime.max_indicators), min_indicators)
    lookback_months = max(int(runtime.lookback_months), 1)
    bar_limit = max(int(runtime.bar_limit), 10)
    return PlayHandLabRuntimeConfig(
        gateway_url=gateway_url.rstrip("/"),
        gateway_token=str(token).strip() if token else None,
        task_mode=task_mode,  # type: ignore[arg-type]
        lanes=lanes,
        tasks_per_lane=tasks_per_lane,
        timeframe=str(runtime.timeframe or "M5").strip().upper() or "M5",
        instrument=_clean_symbols(runtime.instrument),
        instrument_pool=_clean_symbols(runtime.instrument_pool),
        indicator=_clean_symbols(runtime.indicator),
        profile_path=runtime.profile_path,
        min_indicators=min_indicators,
        max_indicators=max_indicators,
        seed=runtime.seed,
        lookback_months=lookback_months,
        bar_limit=bar_limit,
        max_reward_r=runtime.max_reward_r,
        fake_work_seconds=max(float(runtime.fake_work_seconds), 0.0),
        deadline_seconds=max(float(runtime.deadline_seconds), 1.0),
        max_attempts=max(int(runtime.max_attempts), 1),
        poll_interval_seconds=max(float(runtime.poll_interval_seconds), 0.1),
        max_wait_seconds=max(float(runtime.max_wait_seconds), 1.0),
        result_batch_size=max(int(runtime.result_batch_size), 1),
        dry_run=bool(runtime.dry_run),
        strict_scoring=bool(runtime.strict_scoring),
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
            "task_mode": runtime.task_mode,
            "lanes": runtime.lanes,
            "tasks_per_lane": runtime.tasks_per_lane,
            "timeframe": runtime.timeframe,
            "lookback_months": runtime.lookback_months,
            "bar_limit": runtime.bar_limit,
            "instrument": runtime.instrument,
            "instrument_pool": runtime.instrument_pool or list(DEFAULT_INSTRUMENT_POOL),
            "indicator": runtime.indicator,
            "profile_path": str(runtime.profile_path.resolve()) if runtime.profile_path else None,
            "min_indicators": runtime.min_indicators,
            "max_indicators": runtime.max_indicators,
            "seed": runtime.seed,
            "max_reward_r": runtime.max_reward_r,
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
                "no shards, workers claim complete self-contained tasks from the lab gateway."
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


def _deep_replay_job_payload(
    *,
    task_id: str,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> dict[str, Any]:
    profile_payload = dict(lane.profile_payload or {})
    job: dict[str, Any] = {
        "job_id": task_id,
        "user_id": DEFAULT_LAB_USER_ID,
        "profile_id": lane.profile_ref or lane.run_id,
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
        "instruments": list(lane.instruments),
        "timeframe": lane.timeframe,
        "market_data_source": "lake_bars",
        "lookback_months": int(runtime.lookback_months),
        "bar_limit": int(runtime.bar_limit),
        "alert_threshold": float(profile_payload.get("notificationThreshold") or 80.0),
        "view_mode": "overview",
        "direction_mode": str(profile_payload.get("directionMode") or "both"),
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


def _build_tasks(
    lanes: list[LabLaneState],
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str | None = None,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for lane in lanes:
        for task_index in range(runtime.tasks_per_lane):
            task_id = f"{lane.run_id}-task-{task_index + 1:05d}"
            lane.task_ids.append(task_id)
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
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_{task_id}_{_utc_stamp()}").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_json(artifact_dir / "lab-result.json", lab_result)
    _write_json(artifact_dir / "lab-worker-result.json", dict(result_payload))
    score_warning: dict[str, Any] | None = None
    if runtime.task_mode == "deep_replay":
        _write_json(
            artifact_dir / "sensitivity-response.json",
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
    else:
        attempt_score = _fake_attempt_score(result_payload)
    attempts = load_attempts(lane_ctx.attempts_path)
    play_hand_role = "lab_replay" if runtime.task_mode == "deep_replay" else "lab_smoke"
    record = make_attempt_record(
        config,
        lane_ctx.attempts_path,
        lane.run_id,
        artifact_dir,
        attempt_score,
        candidate_name=f"{lane.lane_id}_{runtime.task_mode}_{len(attempts) + 1:05d}",
        profile_ref=lane.profile_ref,
        profile_path=lane.profile_path,
        sensitivity_snapshot_path=(
            artifact_dir / "sensitivity-response.json"
            if runtime.task_mode == "deep_replay"
            else None
        ),
        note=f"{PLAY_HAND_LAB_RUNNER}:{lane.lane_id}:{task_id}",
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
        job_status=str(result_payload.get("status") or lab_result.get("status") or "success"),
        runner=PLAY_HAND_RUNNER,
        attempt_role=play_hand_role,
        attempt_decision="screened_candidate",
        attempt_decision_reasons=["play_hand_lab_worker_result"],
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
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    error = str(result_payload.get("error") or lab_result.get("error") or "lab_worker_failed")
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_failed_{task_id}_{_utc_stamp()}").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
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
    }


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
        "task_mode": runtime.task_mode,
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
    for lane_index in range(runtime.lanes):
        lane_id = f"lane_{lane_index:03d}"
        run_id = _lane_run_id(lane_index)
        run_dir = config.runs_root / run_id
        lane_ctx = _new_context(
            config=config,
            cli=cli,
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
        lanes.append(lane)

    tasks = _build_tasks(
        lanes,
        runtime=runtime,
        reward_matrix=reward_matrix,
        worker_contract_hash=worker_contract_hash,
    )
    if runtime.dry_run:
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
        gateway_metric_baseline = _snapshot_metrics(gateway.snapshot())
    except requests.RequestException as exc:
        _append_event(campaign_ctx, "gateway", "baseline_snapshot_failed", error=str(exc)[:500])

    enqueue_result = gateway.enqueue_tasks(tasks)
    _append_event(campaign_ctx, "gateway", "tasks_enqueued", enqueue_result=enqueue_result, task_count=len(tasks))
    for lane in lanes:
        _write_lane_metadata(lane, campaign_ctx=campaign_ctx, runtime=runtime, status="running", started_at=started_at)
    _write_campaign_metadata(
        campaign_ctx,
        runtime=runtime,
        status="running",
        started_at=started_at,
        extra={"enqueued_task_count": len(tasks)},
    )

    lanes_by_task = {task_id: lane for lane in lanes for task_id in lane.task_ids}
    lane_contexts = {
        lane.run_id: _new_context(
            config=config,
            cli=cli,
            run_id=lane.run_id,
            run_dir=lane.run_dir,
            runtime=runtime,
        )
        for lane in lanes
    }
    recorded_results: list[dict[str, Any]] = []
    recorded_result_count = 0
    deadline = time.monotonic() + runtime.max_wait_seconds
    last_snapshot: dict[str, Any] | None = None
    initial_gateway_id: str | None = None
    gateway_restarted = False
    result_loss_detected = False
    gateway_unreachable = False
    try:
        last_snapshot = gateway.snapshot()
        initial_gateway_id = (
            str(last_snapshot.get("gateway_id"))
            if isinstance(last_snapshot.get("gateway_id"), str)
            else None
        )
    except requests.RequestException as exc:
        _append_event(campaign_ctx, "gateway", "initial_snapshot_failed", error=str(exc)[:500])

    while time.monotonic() < deadline:
        try:
            result_batch = _read_gateway_results(gateway, limit=runtime.result_batch_size)
        except requests.RequestException as exc:
            gateway_unreachable = True
            _append_event(campaign_ctx, "gateway", "result_read_failed", error=str(exc)[:1000])
            break
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
            lane_terminal_count = len(lane.completed_task_ids) + len(lane.failed_task_ids)
            _write_lane_metadata(
                lane,
                campaign_ctx=campaign_ctx,
                runtime=runtime,
                status=(
                    "failed"
                    if lane_terminal_count >= len(lane.task_ids) and lane.failed_task_ids
                    else "completed"
                    if lane_terminal_count >= len(lane.task_ids)
                    else "running"
                ),
                started_at=started_at,
            )
        if ack_lease_ids:
            _safe_ack_gateway_results(
                gateway,
                campaign_ctx,
                lease_ids=ack_lease_ids,
                task_id="batch",
            )
        completed_count = sum(len(lane.completed_task_ids) for lane in lanes)
        failed_count = sum(len(lane.failed_task_ids) for lane in lanes)
        terminal_count = completed_count + failed_count
        if terminal_count >= len(tasks):
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
            metrics = last_snapshot.get("metrics") if isinstance(last_snapshot.get("metrics"), dict) else {}
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

    completed_count = sum(len(lane.completed_task_ids) for lane in lanes)
    failed_count = sum(len(lane.failed_task_ids) for lane in lanes)
    terminal_count = completed_count + failed_count
    if gateway_unreachable:
        status = "gateway_unreachable"
    elif gateway_restarted:
        status = "gateway_restarted"
    elif result_loss_detected:
        status = "result_loss"
    elif terminal_count >= len(tasks):
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
    return 0 if status == "completed" else 2


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
