from __future__ import annotations

import concurrent.futures
import json
import math
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from rich.console import Console
from rich.table import Table

from .config import AppConfig, load_config
from .fuzzfolio import FuzzfolioCli
from .ledger import (
    attempts_path_for_run_dir,
    load_attempts,
    load_run_metadata,
    write_attempts,
    write_run_metadata,
)
from .play_hand import (
    DEFAULT_INSTRUMENT_POOL,
    PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    PLAY_HAND_DEFAULT_SWEEP_TIMEOUT_SECONDS,
    PLAY_HAND_COARSE_HALVING_DEFAULT_PROBE_BUDGET,
    PLAY_HAND_RUNNER,
    PlayHandContext,
    PlayHandStage,
    SeedIndicator,
    _append_event,
    _cleanup_registered_profiles,
    _fallback_indicator_deal,
    _load_json,
    _load_play_hand_seed_plan,
    _lowest_profile_timeframe,
    _merge_seed_indicator_candidates,
    _parameter_importance,
    _register_profile,
    _run_sweep,
    _scaffold_profile,
    _seed_hand,
    _seed_pair_template_instruments,
    _seed_plan_indicator_candidates,
    _seed_plan_template_instrument_policy,
    _top_sweep_score,
    _utc_stamp,
    _write_json,
    apply_play_hand_profile_defaults,
    apply_role_timeframe_defaults,
    apply_seed_indicator_metadata,
    apply_seed_pair_template_defaults,
    build_coarse_axes,
    build_focused_axes,
    build_timing_axes,
    deal_indicator_count,
    deal_instruments,
    deal_seed_plan_indicators,
    play_hand_reward_matrix,
    resolve_sweep_budget,
)

console = Console(safe_box=True)

PLAY_HAND_MASSIVE_RUNNER = "play_hand_massive_v1"
PLAY_HAND_MASSIVE_SCHEMA_VERSION = "play_hand_massive_campaign_v1"
PLAY_HAND_MASSIVE_LANE_SCHEMA_VERSION = "play_hand_massive_lane_v1"
PLAY_HAND_MASSIVE_CAMPAIGNS_DIR = "play-hand-massive-campaigns"
DEFAULT_MASSIVE_LANES = 12
DEFAULT_MASSIVE_ACTIVE_LANES = 2
DEFAULT_SCAFFOLD_ACTIVE_LANES = 2
DEFAULT_REMOTE_TOKEN_BUDGET_MULTIPLIER = 2.0
DEFAULT_MAX_NO_WORKER_WAIT_SECONDS = 300
DEFAULT_BASELINE_FLOOR = 0.0
CAMPAIGN_GATEWAY_UNHEALTHY_THRESHOLD = 2
CAMPAIGN_BACKEND_DOWN_THRESHOLD = 1


@dataclass(frozen=True)
class MassiveRuntimeConfig:
    lanes: int = DEFAULT_MASSIVE_LANES
    active_lanes: int = DEFAULT_MASSIVE_ACTIVE_LANES
    timeframe: str = "M5"
    instrument: list[str] | None = None
    instrument_pool: list[str] | None = None
    sweep_budget: str | None = "low"
    max_sweep_permutations: int | None = None
    max_reward_r: float | None = None
    min_indicators: int = 1
    max_indicators: int = 4
    seed: int | None = None
    screen_months: int = 3
    coarse_mode: str = "evolutionary"
    run_focused: bool = False
    baseline_floor: float | None = DEFAULT_BASELINE_FLOOR
    job_timeout_seconds: int = PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS
    sweep_timeout_seconds: int = PLAY_HAND_DEFAULT_SWEEP_TIMEOUT_SECONDS
    keep_cloud_profiles: bool = False
    adaptive_lanes: bool = True
    adaptive_fail_open: bool = False
    min_active_lanes: int = 1
    target_worker_slots_per_lane: int = 32
    scaffold_active_lanes: int = DEFAULT_SCAFFOLD_ACTIVE_LANES
    staged_campaign: bool = True
    remote_token_budget_multiplier: float = DEFAULT_REMOTE_TOKEN_BUDGET_MULTIPLIER
    max_no_worker_wait_seconds: int = DEFAULT_MAX_NO_WORKER_WAIT_SECONDS
    backend_health_timeout_seconds: int = 5
    gateway_url: str | None = None
    gateway_token: str | None = None
    gateway_pool: list[str] | None = None
    telemetry_interval_seconds: int = 30
    dry_run: bool = False


@dataclass
class MassiveLaneResult:
    lane_id: str
    status: str
    started_at: str
    run_id: str | None = None
    run_dir: str | None = None
    completed_at: str | None = None
    indicators: list[str] = field(default_factory=list)
    instruments: list[str] = field(default_factory=list)
    baseline_score: float | None = None
    coarse_score: float | None = None
    focused_score: float | None = None
    best_score: float | None = None
    best_attempt_id: str | None = None
    best_profile_path: str | None = None
    best_profile_ref: str | None = None
    skipped_reason: str | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane_id": self.lane_id,
            "status": self.status,
            "started_at": self.started_at,
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "completed_at": self.completed_at,
            "indicators": self.indicators,
            "instruments": self.instruments,
            "baseline_score": self.baseline_score,
            "coarse_score": self.coarse_score,
            "focused_score": self.focused_score,
            "best_score": self.best_score,
            "best_attempt_id": self.best_attempt_id,
            "best_profile_path": self.best_profile_path,
            "best_profile_ref": self.best_profile_ref,
            "skipped_reason": self.skipped_reason,
            "error": self.error,
        }


def normalize_massive_runtime_config(config: MassiveRuntimeConfig) -> MassiveRuntimeConfig:
    lanes = max(1, int(config.lanes))
    active_lanes = max(1, min(int(config.active_lanes), lanes))
    timeframe = str(config.timeframe or "M5").strip().upper() or "M5"
    min_indicators = max(1, int(config.min_indicators))
    max_indicators = max(min_indicators, int(config.max_indicators))
    screen_months = max(1, int(config.screen_months))
    job_timeout_seconds = max(1, int(config.job_timeout_seconds))
    sweep_timeout_seconds = max(1, int(config.sweep_timeout_seconds))
    min_active_lanes = max(1, min(int(config.min_active_lanes), active_lanes))
    target_worker_slots_per_lane = max(1, int(config.target_worker_slots_per_lane))
    scaffold_active_lanes = max(1, min(int(config.scaffold_active_lanes), active_lanes))
    remote_token_budget_multiplier = max(0.1, float(config.remote_token_budget_multiplier))
    max_no_worker_wait_seconds = max(0, int(config.max_no_worker_wait_seconds))
    backend_health_timeout_seconds = max(1, int(config.backend_health_timeout_seconds))
    telemetry_interval_seconds = max(1, int(config.telemetry_interval_seconds))
    baseline_floor = (
        None
        if config.baseline_floor is None
        else float(config.baseline_floor)
    )
    return MassiveRuntimeConfig(
        lanes=lanes,
        active_lanes=active_lanes,
        timeframe=timeframe,
        instrument=config.instrument,
        instrument_pool=config.instrument_pool,
        sweep_budget=config.sweep_budget,
        max_sweep_permutations=config.max_sweep_permutations,
        max_reward_r=config.max_reward_r,
        min_indicators=min_indicators,
        max_indicators=max_indicators,
        seed=config.seed,
        screen_months=screen_months,
        coarse_mode=config.coarse_mode,
        run_focused=bool(config.run_focused),
        baseline_floor=baseline_floor,
        job_timeout_seconds=job_timeout_seconds,
        sweep_timeout_seconds=sweep_timeout_seconds,
        keep_cloud_profiles=bool(config.keep_cloud_profiles),
        adaptive_lanes=bool(config.adaptive_lanes),
        adaptive_fail_open=bool(config.adaptive_fail_open),
        min_active_lanes=min_active_lanes,
        target_worker_slots_per_lane=target_worker_slots_per_lane,
        scaffold_active_lanes=scaffold_active_lanes,
        staged_campaign=bool(config.staged_campaign),
        remote_token_budget_multiplier=remote_token_budget_multiplier,
        max_no_worker_wait_seconds=max_no_worker_wait_seconds,
        backend_health_timeout_seconds=backend_health_timeout_seconds,
        gateway_url=str(config.gateway_url).strip() if config.gateway_url else None,
        gateway_token=str(config.gateway_token).strip() if config.gateway_token else None,
        gateway_pool=[
            str(item).strip()
            for item in list(config.gateway_pool or [])
            if str(item).strip()
        ]
        or None,
        telemetry_interval_seconds=telemetry_interval_seconds,
        dry_run=bool(config.dry_run),
    )


def lane_seed(base_seed: int | None, lane_index: int) -> str:
    if base_seed is None:
        return f"play-hand-massive:lane:{lane_index}"
    return f"play-hand-massive:{base_seed}:lane:{lane_index}"


def should_expand_lane(
    *,
    baseline_score: float | None,
    baseline_floor: float | None,
    dry_run: bool,
) -> bool:
    if dry_run or baseline_floor is None or baseline_score is None:
        return True
    return float(baseline_score) >= float(baseline_floor)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _score(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _gateway_token(runtime: MassiveRuntimeConfig) -> str | None:
    for value in (
        runtime.gateway_token,
        os.environ.get("FUZZFOLIO_WORKER_GATEWAY_TOKEN"),
        os.environ.get("WORKER_GATEWAY_API_TOKEN"),
    ):
        token = str(value or "").strip()
        if token:
            return token
    return None


def _gateway_url(runtime: MassiveRuntimeConfig) -> str | None:
    for value in (
        runtime.gateway_url,
        os.environ.get("FUZZFOLIO_WORKER_GATEWAY_URL"),
        os.environ.get("WORKER_GATEWAY_URL"),
    ):
        url = str(value or "").strip()
        if url:
            return url.rstrip("/")
    return None


def _backend_root_url(config: AppConfig) -> str:
    base = str(config.fuzzfolio.base_url or "").strip().rstrip("/")
    if not base:
        return "http://localhost:7946"
    for suffix in ("/api/dev", "/api"):
        if base.endswith(suffix):
            return base[: -len(suffix)]
    parsed = urlparse(base)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "http://localhost:7946"


def _poll_local_backend_health(
    config: AppConfig,
    *,
    timeout_seconds: int = 5,
) -> dict[str, Any]:
    url = f"{_backend_root_url(config)}/healthz"
    try:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        return {
            "ok": True,
            "url": url,
            "status_code": response.status_code,
        }
    except (requests.RequestException, ValueError) as exc:
        return {
            "ok": False,
            "url": url,
            "reason": "backend_health_failed",
            "error": str(exc)[:500],
        }


def _estimate_lane_remote_permutations(max_sweep_permutations: int) -> int:
    cap = max(1, int(max_sweep_permutations))
    return cap * 2


def _remote_token_budget(
    snapshot: dict[str, Any] | None,
    runtime: MassiveRuntimeConfig,
) -> int | None:
    if not snapshot or not snapshot.get("ok"):
        return None
    try:
        slots = int(snapshot.get("slots") or 0)
    except (TypeError, ValueError):
        slots = 0
    if slots <= 0:
        return 0
    return int(math.ceil(slots * float(runtime.remote_token_budget_multiplier)))


def _effective_remote_token_budget(
    snapshot: dict[str, Any] | None,
    runtime: MassiveRuntimeConfig,
    lane_remote_cost: int,
) -> int | None:
    token_budget = _remote_token_budget(snapshot, runtime)
    if token_budget is None:
        return None
    if token_budget <= 0:
        return 0
    return max(token_budget, max(1, int(lane_remote_cost)))


def _poll_worker_pool_snapshot(runtime: MassiveRuntimeConfig) -> dict[str, Any]:
    url = _gateway_url(runtime)
    token = _gateway_token(runtime)
    if not url:
        return {"ok": False, "reason": "gateway_url_missing"}
    if not token:
        return {"ok": False, "reason": "gateway_token_missing", "gateway_url": url}
    try:
        response = requests.get(
            f"{url}/pools",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        return {
            "ok": False,
            "reason": "gateway_poll_failed",
            "gateway_url": url,
            "error": str(exc)[:500],
        }
    pools = list(payload.get("pools") or []) if isinstance(payload, dict) else []
    wanted_pools = set(runtime.gateway_pool or [])
    selected_pools = [
        pool
        for pool in pools
        if isinstance(pool, dict)
        and (not wanted_pools or str(pool.get("pool") or "") in wanted_pools)
    ]
    slots = 0
    worker_count = 0
    for pool in selected_pools:
        try:
            slots += int(pool.get("slots") or 0)
        except (TypeError, ValueError):
            pass
        try:
            worker_count += int(pool.get("worker_count") or 0)
        except (TypeError, ValueError):
            pass
    return {
        "ok": True,
        "gateway_url": url,
        "server_time": payload.get("server_time") if isinstance(payload, dict) else None,
        "pool_filter": sorted(wanted_pools),
        "pool_count": len(selected_pools),
        "worker_count": worker_count,
        "slots": slots,
        "pools": [
            {
                "pool": pool.get("pool"),
                "transport": pool.get("transport"),
                "contract_hash": pool.get("contract_hash"),
                "worker_count": pool.get("worker_count"),
                "slots": pool.get("slots"),
            }
            for pool in selected_pools
        ],
    }


def _desired_active_lanes(
    runtime: MassiveRuntimeConfig,
    snapshot: dict[str, Any] | None,
) -> int:
    if not runtime.adaptive_lanes:
        return runtime.active_lanes
    if not snapshot or not snapshot.get("ok"):
        if runtime.adaptive_fail_open:
            return runtime.active_lanes
        return 0 if _gateway_url(runtime) else runtime.min_active_lanes
    slots = 0
    try:
        slots = int(snapshot.get("slots") or 0)
    except (TypeError, ValueError):
        slots = 0
    if slots <= 0:
        return 0
    desired = int(math.ceil(slots / float(runtime.target_worker_slots_per_lane)))
    return max(runtime.min_active_lanes, min(runtime.active_lanes, desired))


def _derived_campaign_root(config: AppConfig) -> Path:
    derived_root = getattr(config, "derived_root", None)
    if isinstance(derived_root, Path):
        return derived_root / PLAY_HAND_MASSIVE_CAMPAIGNS_DIR
    if derived_root:
        return Path(str(derived_root)) / PLAY_HAND_MASSIVE_CAMPAIGNS_DIR
    return config.runs_root / "derived" / PLAY_HAND_MASSIVE_CAMPAIGNS_DIR


def _campaign_run_id() -> str:
    return f"{_utc_stamp()}-playhand-massive-campaign-v1"


def _lane_run_id(lane_index: int) -> str:
    return f"{_utc_stamp()}-playhand-massive-lane-{lane_index:03d}-v1"


def _new_play_hand_context(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    run_id: str,
    run_dir: Path,
    event_name: str,
    summary_name: str,
    runtime: MassiveRuntimeConfig,
) -> PlayHandContext:
    return PlayHandContext(
        config=config,
        cli=cli,
        run_id=run_id,
        run_dir=run_dir,
        profiles_dir=run_dir / "profiles",
        evals_dir=run_dir / "evals",
        attempts_path=attempts_path_for_run_dir(run_dir),
        events_path=run_dir / event_name,
        summary_path=run_dir / summary_name,
        dry_run=runtime.dry_run,
        job_timeout_seconds=runtime.job_timeout_seconds,
        sweep_timeout_seconds=runtime.sweep_timeout_seconds,
    )


def _write_lane_metadata(
    ctx: PlayHandContext,
    *,
    campaign_id: str,
    campaign_dir: Path,
    lane_id: str,
    lane_index: int,
    runtime: MassiveRuntimeConfig,
    sweep_budget_label: str,
    sweep_budget_value: int,
    budget: dict[str, Any],
    reward_matrix: dict[str, Any] | None,
    seed_plan_path: Path | None,
    seed_plan_loaded: bool,
    status: str,
    extra: dict[str, Any] | None = None,
) -> None:
    existing = load_run_metadata(ctx.run_dir)
    metadata: dict[str, Any] = {
        "schema_version": PLAY_HAND_MASSIVE_LANE_SCHEMA_VERSION,
        "runner": PLAY_HAND_RUNNER,
        "generated_by_runner": PLAY_HAND_MASSIVE_RUNNER,
        "run_kind": "play_hand_massive_lane",
        "run_id": ctx.run_id,
        "run_status": status,
        "created_at": existing.get("created_at") or _now_iso(),
        "massive_campaign_id": campaign_id,
        "parent_campaign_id": campaign_id,
        "campaign_dir": str(campaign_dir.resolve()),
        "massive_lane_id": lane_id,
        "massive_lane_index": lane_index,
        "lanes": runtime.lanes,
        "active_lanes": runtime.active_lanes,
        "timeframe": runtime.timeframe,
        "instrument": runtime.instrument,
        "instrument_pool": runtime.instrument_pool or list(DEFAULT_INSTRUMENT_POOL),
        "screen_months": runtime.screen_months,
        "coarse_mode": runtime.coarse_mode,
        "focused": runtime.run_focused,
        "baseline_floor": runtime.baseline_floor,
        "sweep_budget": sweep_budget_label,
        "sweep_budget_value": sweep_budget_value,
        "sweep_budget_source": budget.get("source"),
        "max_sweep_permutations": sweep_budget_value,
        "reward_matrix": reward_matrix,
        "deep_replay_job_timeout_seconds": runtime.job_timeout_seconds,
        "sweep_timeout_seconds": runtime.sweep_timeout_seconds,
        "adaptive_lanes": runtime.adaptive_lanes,
        "min_active_lanes": runtime.min_active_lanes,
        "target_worker_slots_per_lane": runtime.target_worker_slots_per_lane,
        "gateway_url": _gateway_url(runtime),
        "gateway_pool": runtime.gateway_pool,
        "telemetry_interval_seconds": runtime.telemetry_interval_seconds,
        "seed": runtime.seed,
        "play_hand_seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
        "play_hand_seed_plan_loaded": seed_plan_loaded,
        "dry_run": runtime.dry_run,
        "design_note": (
            "First-class PlayHand Massive lane: screened candidate run managed by "
            "a campaign controller; catch-up/final artifacts run outside this loop."
        ),
    }
    if extra:
        metadata.update(extra)
    write_run_metadata(ctx.run_dir, metadata)


def _finalize_lane_attempts(
    ctx: PlayHandContext,
    result: MassiveLaneResult,
    *,
    campaign_id: str,
    reward_matrix: dict[str, Any] | None,
) -> dict[str, Any]:
    attempts = load_attempts(ctx.attempts_path)
    if not attempts:
        return {
            "attempt_count": 0,
            "updated_count": 0,
            "canonical_attempt_id": None,
        }
    canonical_attempt_id = (
        result.best_attempt_id
        if result.status == "completed" and result.best_attempt_id
        else None
    )
    canonical_candidate_name = None
    updated = 0
    matrix = dict(reward_matrix) if isinstance(reward_matrix, dict) else None
    selected_instruments = [str(item).strip().upper() for item in result.instruments if str(item).strip()]
    for attempt in attempts:
        attempt_id = str(attempt.get("attempt_id") or "").strip()
        is_canonical = bool(canonical_attempt_id and attempt_id == canonical_attempt_id)
        prior = dict(attempt)
        stage = str(attempt.get("massive_lane_stage") or attempt.get("play_hand_stage") or "").strip()
        attempt["runner"] = PLAY_HAND_RUNNER
        attempt["generated_by_runner"] = PLAY_HAND_MASSIVE_RUNNER
        attempt["massive_campaign_id"] = campaign_id
        attempt["massive_lane_id"] = result.lane_id
        attempt["attempt_role"] = "screened_candidate"
        attempt["play_hand_role"] = "screened_candidate"
        attempt["play_hand_stage"] = stage or "screened_candidate"
        attempt["attempt_decision"] = "screened_canonical" if is_canonical else "screened_alternate"
        attempt["attempt_decision_reasons"] = (
            ["massive_lane_best_screened_candidate"]
            if is_canonical
            else ["massive_lane_screened_alternate"]
        )
        attempt["strategy_family_id"] = ctx.run_id
        attempt["canonical_attempt_id"] = canonical_attempt_id
        attempt["is_canonical_attempt"] = is_canonical
        attempt["is_canonical_playhand_attempt"] = is_canonical
        attempt["run_status"] = "screened"
        if selected_instruments:
            attempt["play_hand_selected_instruments"] = selected_instruments
        if matrix:
            attempt["max_reward_r"] = matrix.get("requested_max_reward_r")
            attempt["reward_matrix"] = matrix
            attempt["reward_step_r"] = matrix.get("reward_step_r")
            attempt["reward_columns"] = matrix.get("reward_columns")
            attempt["effective_max_reward_r"] = matrix.get("effective_max_reward_r")
        if attempt != prior:
            updated += 1
        if is_canonical:
            canonical_candidate_name = str(attempt.get("candidate_name") or "").strip() or None
    write_attempts(ctx.attempts_path, attempts)
    return {
        "attempt_count": len(attempts),
        "updated_count": updated,
        "canonical_attempt_id": canonical_attempt_id,
        "canonical_candidate_name": canonical_candidate_name,
    }


def _retag_attempt(
    ctx: PlayHandContext,
    *,
    attempt_id: str | None,
    lane_id: str,
    lane_stage: str,
    campaign_id: str,
) -> None:
    if not attempt_id:
        return
    with ctx.io_lock:
        attempts = load_attempts(ctx.attempts_path)
        changed = False
        for attempt in attempts:
            if attempt.get("attempt_id") != attempt_id:
                continue
            attempt["runner"] = PLAY_HAND_RUNNER
            attempt["generated_by_runner"] = PLAY_HAND_MASSIVE_RUNNER
            attempt["note"] = f"{PLAY_HAND_MASSIVE_RUNNER}:{lane_id}:{lane_stage}"
            attempt["massive_campaign_id"] = campaign_id
            attempt["massive_lane_id"] = lane_id
            attempt["massive_lane_stage"] = lane_stage
            attempt["attempt_role"] = "screened_candidate"
            attempt["attempt_decision"] = "screened_candidate"
            attempt["play_hand_role"] = "screened_candidate"
            attempt["play_hand_stage"] = lane_stage
            changed = True
            break
        if changed:
            write_attempts(ctx.attempts_path, attempts)


def _lane_phase(lane_id: str, phase: str) -> str:
    return f"{lane_id}_{phase}"


def _deal_lane(
    *,
    config: AppConfig,
    runtime: MassiveRuntimeConfig,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any] | None,
    rng: random.Random,
) -> dict[str, Any]:
    shuffled = list(seed_indicators)
    rng.shuffle(shuffled)
    seed_plan_candidates = _seed_plan_indicator_candidates(config, seed_plan)
    guided_available_count = len(
        _merge_seed_indicator_candidates(shuffled, seed_plan_candidates)
    )
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
            reason="empty_guided_deal",
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
        "template_instrument_policy": template_instrument_policy,
        "template_instrument_pool": template_instrument_pool,
    }


def _scaffold_lane_profile(
    ctx: PlayHandContext,
    *,
    lane_id: str,
    deal: dict[str, Any],
    runtime: MassiveRuntimeConfig,
    rng: random.Random,
    stage: PlayHandStage,
) -> tuple[Path, str, dict[str, Any], str]:
    profile_path = _scaffold_profile(
        ctx,
        list(deal["dealt"]),
        list(deal["instruments"]),
        runtime.timeframe,
        f"{lane_id}_base",
    )
    profile_payload = _load_json(profile_path)
    metadata_changes = apply_seed_indicator_metadata(profile_payload, deal["dealt_entries"])
    timeframe_changes = apply_role_timeframe_defaults(profile_payload, rng=rng)
    template_changes = apply_seed_pair_template_defaults(
        profile_payload,
        deal["indicator_deal"].get("pair"),
    )
    default_changes = apply_play_hand_profile_defaults(profile_payload, rng=rng)
    if metadata_changes or timeframe_changes or template_changes or default_changes:
        _write_json(profile_path, profile_payload)
    evaluation_timeframe = _lowest_profile_timeframe(profile_payload, runtime.timeframe)
    profile_ref = _register_profile(ctx, profile_path)
    _append_event(
        ctx,
        lane_id,
        "profile_registered",
        stage=stage,
        profile_path=str(profile_path),
        profile_ref=profile_ref,
        evaluation_timeframe=evaluation_timeframe,
        metadata_changes=metadata_changes,
        timeframe_changes=timeframe_changes,
        template_changes=template_changes,
        default_changes=default_changes,
    )
    return profile_path, profile_ref, profile_payload, evaluation_timeframe


def _remember_best(
    result: MassiveLaneResult,
    *,
    score: float | None,
    attempt_id: str | None,
    profile_path: str | Path | None,
    profile_ref: str | None,
) -> None:
    if score is None:
        return
    if result.best_score is None or score > result.best_score:
        result.best_score = score
        result.best_attempt_id = attempt_id
        result.best_profile_path = str(profile_path) if profile_path else None
        result.best_profile_ref = profile_ref


def _run_lane(
    campaign_ctx: PlayHandContext,
    *,
    runtime: MassiveRuntimeConfig,
    lane_index: int,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any] | None,
    seed_plan_path: Path | None,
    budget: dict[str, Any],
    sweep_budget_label: str,
    max_sweep_permutations: int,
    reward_matrix: dict[str, Any] | None,
    stop_after_baseline: bool = False,
    existing_lane_run_id: str | None = None,
    existing_lane_run_dir: Path | None = None,
) -> MassiveLaneResult:
    lane_id = f"lane_{lane_index:03d}"
    lane_run_id = existing_lane_run_id or _lane_run_id(lane_index)
    lane_run_dir = existing_lane_run_dir or (campaign_ctx.config.runs_root / lane_run_id)
    lane_ctx = _new_play_hand_context(
        config=campaign_ctx.config,
        cli=campaign_ctx.cli,
        run_id=lane_run_id,
        run_dir=lane_run_dir,
        event_name="play-hand-massive-lane-events.jsonl",
        summary_name="play-hand-massive-lane-summary.json",
        runtime=runtime,
    )
    lane_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
    lane_ctx.evals_dir.mkdir(parents=True, exist_ok=True)
    started_at = _now_iso()
    result = MassiveLaneResult(
        lane_id=lane_id,
        status="running",
        started_at=started_at,
        run_id=lane_run_id,
        run_dir=str(lane_run_dir.resolve()),
    )
    lane_rng = random.Random(lane_seed(runtime.seed, lane_index))
    stages = {
        "scaffold": PlayHandStage(1, 4, "Massive lane scaffold"),
        "baseline": PlayHandStage(2, 4, "Massive lane baseline"),
        "coarse": PlayHandStage(3, 4, "Massive lane coarse sweep"),
        "focused": PlayHandStage(4, 4, "Massive lane focused sweep"),
    }
    _write_lane_metadata(
        lane_ctx,
        campaign_id=campaign_ctx.run_id,
        campaign_dir=campaign_ctx.run_dir,
        lane_id=lane_id,
        lane_index=lane_index,
        runtime=runtime,
        sweep_budget_label=sweep_budget_label,
        sweep_budget_value=max_sweep_permutations,
        budget=budget,
        reward_matrix=reward_matrix,
        seed_plan_path=seed_plan_path,
        seed_plan_loaded=seed_plan is not None,
        status="running",
        extra={"started_at": started_at},
    )
    _append_event(
        campaign_ctx,
        lane_id,
        "started",
        lane_run_id=lane_run_id,
        lane_run_dir=str(lane_run_dir.resolve()),
    )
    try:
        if existing_lane_run_dir is not None:
            lane_metadata = load_run_metadata(lane_run_dir)
            result.indicators = list(lane_metadata.get("indicators") or result.indicators)
            result.instruments = list(lane_metadata.get("instruments") or result.instruments)
            result.baseline_score = _score(lane_metadata.get("baseline_score"))
            profile_path = Path(str(lane_metadata.get("profile_path") or lane_run_dir / "profiles"))
            profile_ref = str(lane_metadata.get("profile_ref") or "")
            profile_payload = _load_json(profile_path) if profile_path.exists() else {}
            evaluation_timeframe = str(
                lane_metadata.get("evaluation_timeframe") or runtime.timeframe
            )
            if not profile_ref:
                raise RuntimeError("expand lane missing profile_ref in lane metadata")
        else:
            deal = _deal_lane(
                config=lane_ctx.config,
                runtime=runtime,
                seed_indicators=seed_indicators,
                seed_plan=seed_plan,
                rng=lane_rng,
            )
            result.indicators = list(deal["dealt"])
            result.instruments = list(deal["instruments"])
            _append_event(
                lane_ctx,
                lane_id,
                "dealt",
                stage=stages["scaffold"],
                indicators=result.indicators,
                instruments=result.instruments,
                indicator_deal_source=deal["indicator_deal"].get("source"),
                dealt_recipe=deal["indicator_deal"].get("recipe"),
            )
            profile_path, profile_ref, profile_payload, evaluation_timeframe = _scaffold_lane_profile(
                lane_ctx,
                lane_id=lane_id,
                deal=deal,
                runtime=runtime,
                rng=lane_rng,
                stage=stages["scaffold"],
            )
            baseline = _run_profile_evaluation(
                lane_ctx,
                runtime=runtime,
                stage=stages["baseline"],
                lane_id=lane_id,
                lane_stage="baseline_3mo",
                profile_ref=profile_ref,
                profile_path=profile_path,
                instruments=result.instruments,
                timeframe=evaluation_timeframe,
                reward_matrix=reward_matrix,
            )
            result.baseline_score = _score(baseline.get("score"))
            _retag_attempt(
                lane_ctx,
                attempt_id=baseline.get("attempt_id"),
                lane_id=lane_id,
                lane_stage="baseline_3mo",
                campaign_id=campaign_ctx.run_id,
            )
            _remember_best(
                result,
                score=result.baseline_score,
                attempt_id=baseline.get("attempt_id"),
                profile_path=profile_path,
                profile_ref=profile_ref,
            )
            _write_lane_metadata(
                lane_ctx,
                campaign_id=campaign_ctx.run_id,
                campaign_dir=campaign_ctx.run_dir,
                lane_id=lane_id,
                lane_index=lane_index,
                runtime=runtime,
                sweep_budget_label=sweep_budget_label,
                sweep_budget_value=max_sweep_permutations,
                budget=budget,
                reward_matrix=reward_matrix,
                seed_plan_path=seed_plan_path,
                seed_plan_loaded=seed_plan is not None,
                status="running",
                extra={
                    "indicators": result.indicators,
                    "instruments": result.instruments,
                    "baseline_score": result.baseline_score,
                    "profile_path": str(profile_path),
                    "profile_ref": profile_ref,
                    "evaluation_timeframe": evaluation_timeframe,
                },
            )
            if not should_expand_lane(
                baseline_score=result.baseline_score,
                baseline_floor=runtime.baseline_floor,
                dry_run=runtime.dry_run,
            ):
                result.status = "skipped"
                result.skipped_reason = "baseline_below_floor"
                _append_event(
                    lane_ctx,
                    lane_id,
                    "skipped",
                    stage=stages["baseline"],
                    reason=result.skipped_reason,
                    baseline_score=result.baseline_score,
                    baseline_floor=runtime.baseline_floor,
                )
                return result
            if stop_after_baseline:
                result.status = "baseline_screened"
                _append_event(
                    lane_ctx,
                    lane_id,
                    "baseline_screened",
                    stage=stages["baseline"],
                    baseline_score=result.baseline_score,
                )
                return result

        current_profile_path = profile_path
        current_profile_ref = profile_ref
        current_profile_payload = profile_payload
        current_timeframe = evaluation_timeframe
        timing_axes = build_timing_axes(current_profile_payload)
        if timing_axes:
            timing_sweep = _run_sweep(
                lane_ctx,
                stage=stages["coarse"],
                phase=_lane_phase(lane_id, "timing"),
                profile_ref=current_profile_ref,
                profile_payload=current_profile_payload,
                instruments=result.instruments,
                axes=timing_axes,
                mode="deterministic",
                sweep_budget=sweep_budget_label,
                max_permutations=max_sweep_permutations,
                reward_matrix=reward_matrix,
            )
            timing_materialized = _materialize_best(
                lane_ctx,
                stages["coarse"],
                current_profile_path,
                timing_sweep,
                _lane_phase(lane_id, "timing"),
                runtime.timeframe,
            )
            if timing_materialized is not None:
                current_profile_path, current_profile_ref, current_profile_payload, current_timeframe = timing_materialized

        coarse_axes = build_coarse_axes(current_profile_payload)
        if not coarse_axes:
            result.status = "completed"
            result.skipped_reason = "no_coarse_axes"
            _append_event(lane_ctx, lane_id, "completed", stage=stages["coarse"], reason="no_coarse_axes")
            return result
        coarse_sweep = _run_sweep(
            lane_ctx,
            stage=stages["coarse"],
            phase=_lane_phase(lane_id, "coarse"),
            profile_ref=current_profile_ref,
            profile_payload=current_profile_payload,
            instruments=result.instruments,
            axes=coarse_axes,
            mode=runtime.coarse_mode,
            sweep_budget=sweep_budget_label,
            max_permutations=max_sweep_permutations,
            reward_matrix=reward_matrix,
        )
        coarse_top_score = _top_sweep_score(coarse_sweep["result"])
        coarse_materialized = _materialize_best(
            lane_ctx,
            stages["coarse"],
            current_profile_path,
            coarse_sweep,
            _lane_phase(lane_id, "coarse"),
            runtime.timeframe,
        )
        if coarse_materialized is not None:
            current_profile_path, current_profile_ref, current_profile_payload, current_timeframe = coarse_materialized
            coarse_eval = _run_profile_evaluation(
                lane_ctx,
                runtime=runtime,
                stage=stages["coarse"],
                lane_id=lane_id,
                lane_stage="coarse_top_3mo",
                profile_ref=current_profile_ref,
                profile_path=current_profile_path,
                instruments=result.instruments,
                timeframe=current_timeframe,
                reward_matrix=reward_matrix,
            )
            result.coarse_score = _score(coarse_eval.get("score"))
            _retag_attempt(
                lane_ctx,
                attempt_id=coarse_eval.get("attempt_id"),
                lane_id=lane_id,
                lane_stage="coarse_top_3mo",
                campaign_id=campaign_ctx.run_id,
            )
            _remember_best(
                result,
                score=result.coarse_score,
                attempt_id=coarse_eval.get("attempt_id"),
                profile_path=current_profile_path,
                profile_ref=current_profile_ref,
            )
        elif coarse_top_score is not None:
            result.coarse_score = _score(coarse_top_score)

        if runtime.run_focused:
            focused_axes = build_focused_axes(
                _parameter_importance(coarse_sweep["result"]),
                list(coarse_sweep.get("axes") or coarse_axes),
            )
            if focused_axes:
                focused_sweep = _run_sweep(
                    lane_ctx,
                    stage=stages["focused"],
                    phase=_lane_phase(lane_id, "focused"),
                    profile_ref=current_profile_ref,
                    profile_payload=current_profile_payload,
                    instruments=result.instruments,
                    axes=focused_axes,
                    mode="deterministic",
                    sweep_budget=sweep_budget_label,
                    max_permutations=max_sweep_permutations,
                    reward_matrix=reward_matrix,
                )
                focused_materialized = _materialize_best(
                    lane_ctx,
                    stages["focused"],
                    current_profile_path,
                    focused_sweep,
                    _lane_phase(lane_id, "focused"),
                    runtime.timeframe,
                )
                if focused_materialized is not None:
                    current_profile_path, current_profile_ref, current_profile_payload, current_timeframe = focused_materialized
                    focused_eval = _run_profile_evaluation(
                        lane_ctx,
                        runtime=runtime,
                        stage=stages["focused"],
                        lane_id=lane_id,
                        lane_stage="focused_top_3mo",
                        profile_ref=current_profile_ref,
                        profile_path=current_profile_path,
                        instruments=result.instruments,
                        timeframe=current_timeframe,
                        reward_matrix=reward_matrix,
                    )
                    result.focused_score = _score(focused_eval.get("score"))
                    _retag_attempt(
                        lane_ctx,
                        attempt_id=focused_eval.get("attempt_id"),
                        lane_id=lane_id,
                        lane_stage="focused_top_3mo",
                        campaign_id=campaign_ctx.run_id,
                    )
                    _remember_best(
                        result,
                        score=result.focused_score,
                        attempt_id=focused_eval.get("attempt_id"),
                        profile_path=current_profile_path,
                        profile_ref=current_profile_ref,
                    )
        result.status = "completed"
        return result
    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)[:2000]
        _append_event(lane_ctx, lane_id, "failed", error=result.error)
        return result
    finally:
        result.completed_at = _now_iso()
        attempt_metadata = _finalize_lane_attempts(
            lane_ctx,
            result,
            campaign_id=campaign_ctx.run_id,
            reward_matrix=reward_matrix,
        )
        cleanup = _cleanup_registered_profiles(
            lane_ctx,
            keep_cloud_profiles=runtime.keep_cloud_profiles,
            reason="play_hand_massive_lane_completed",
        )
        canonical_attempt_id = attempt_metadata.get("canonical_attempt_id")
        lane_run_status = (
            "screened"
            if canonical_attempt_id
            else ("screened_out" if result.status == "skipped" else result.status)
        )
        _write_lane_metadata(
            lane_ctx,
            campaign_id=campaign_ctx.run_id,
            campaign_dir=campaign_ctx.run_dir,
            lane_id=lane_id,
            lane_index=lane_index,
            runtime=runtime,
            sweep_budget_label=sweep_budget_label,
            sweep_budget_value=max_sweep_permutations,
            budget=budget,
            reward_matrix=reward_matrix,
            seed_plan_path=seed_plan_path,
            seed_plan_loaded=seed_plan is not None,
            status=lane_run_status,
            extra={
                "completed_at": result.completed_at,
                "lane_status": result.status,
                "lane_result": result.as_dict(),
                "canonical_attempt_id": canonical_attempt_id,
                "canonical_attempt_role": "screened_candidate" if canonical_attempt_id else None,
                "canonical_candidate_name": attempt_metadata.get("canonical_candidate_name"),
                "canonical_score": result.best_score if canonical_attempt_id else None,
                "strategy_family_id": lane_ctx.run_id,
                "attempt_metadata": attempt_metadata,
                "cloud_profile_cleanup": cleanup,
            },
        )
        _write_json(
            lane_ctx.summary_path,
            {
                "run_id": lane_ctx.run_id,
                "runner": PLAY_HAND_RUNNER,
                "generated_by_runner": PLAY_HAND_MASSIVE_RUNNER,
                "campaign_id": campaign_ctx.run_id,
                "lane_result": result.as_dict(),
                "attempt_metadata": attempt_metadata,
                "cloud_profile_cleanup": cleanup,
            },
        )
        _append_event(lane_ctx, lane_id, result.status, result=result.as_dict())
        _append_event(
            campaign_ctx,
            lane_id,
            result.status,
            lane_run_id=lane_ctx.run_id,
            canonical_attempt_id=canonical_attempt_id,
            best_score=result.best_score,
            result=result.as_dict(),
        )


def _materialize_best(
    ctx: PlayHandContext,
    stage: PlayHandStage,
    source_profile_path: Path,
    sweep: dict[str, Any],
    phase: str,
    fallback_timeframe: str,
) -> tuple[Path, str, dict[str, Any], str] | None:
    from .play_hand import _materialize_and_register_best_sweep_candidate

    materialized = _materialize_and_register_best_sweep_candidate(
        ctx,
        stage=stage,
        source_profile_path=source_profile_path,
        sweep_payload=sweep["result"],
        phase=phase,
    )
    if materialized is None:
        return None
    profile_path, profile_ref, _parameters = materialized
    profile_payload = _load_json(profile_path)
    timeframe = _lowest_profile_timeframe(profile_payload, fallback_timeframe)
    return profile_path, profile_ref, profile_payload, timeframe


def _run_profile_evaluation(
    ctx: PlayHandContext,
    *,
    runtime: MassiveRuntimeConfig,
    stage: PlayHandStage,
    lane_id: str,
    lane_stage: str,
    profile_ref: str,
    profile_path: Path,
    instruments: list[str],
    timeframe: str,
    reward_matrix: dict[str, Any] | None,
) -> dict[str, Any]:
    from .play_hand import _evaluate_profile

    return _evaluate_profile(
        ctx,
        stage=stage,
        phase=_lane_phase(lane_id, lane_stage),
        profile_ref=profile_ref,
        profile_path=profile_path,
        instruments=instruments,
        timeframe=timeframe,
        lookback_months=runtime.screen_months,
        reward_matrix=reward_matrix,
    )


def _render_campaign_table(results: list[MassiveLaneResult]) -> None:
    table = Table(title="Play Hand Massive", show_lines=False)
    table.add_column("Lane")
    table.add_column("Status")
    table.add_column("Baseline", justify="right")
    table.add_column("Coarse", justify="right")
    table.add_column("Focused", justify="right")
    table.add_column("Best", justify="right")
    table.add_column("Detail")
    for item in sorted(results, key=lambda result: result.lane_id):
        def fmt(value: float | None) -> str:
            return f"{value:.2f}" if value is not None else ""

        table.add_row(
            item.lane_id,
            item.status,
            fmt(item.baseline_score),
            fmt(item.coarse_score),
            fmt(item.focused_score),
            fmt(item.best_score),
            item.skipped_reason or item.error or item.best_attempt_id or "",
        )
    console.print(table)


def _mark_unstarted_lanes(
    *,
    pending_lane_indexes: list[int],
    reason: str,
    started_at: str,
) -> list[MassiveLaneResult]:
    results: list[MassiveLaneResult] = []
    for lane_index in pending_lane_indexes:
        lane_id = f"lane_{lane_index:03d}"
        results.append(
            MassiveLaneResult(
                lane_id=lane_id,
                status=reason,
                started_at=started_at,
                skipped_reason=reason,
            )
        )
    return results


def _run_campaign_lane_executor(
    *,
    ctx: PlayHandContext,
    runtime: MassiveRuntimeConfig,
    config: AppConfig,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any] | None,
    seed_plan_path: Path | None,
    budget: dict[str, Any],
    sweep_budget_label: str,
    sweep_budget_value: int,
    reward_matrix: dict[str, Any] | None,
    metadata: dict[str, Any],
    lane_indexes: list[int],
    stop_after_baseline: bool = False,
    expand_from: dict[int, MassiveLaneResult] | None = None,
    max_active_override: int | None = None,
) -> tuple[list[MassiveLaneResult], dict[str, Any]]:
    results: list[MassiveLaneResult] = []
    pending_lane_indexes = list(lane_indexes)
    in_flight: dict[concurrent.futures.Future[MassiveLaneResult], int] = {}
    last_snapshot: dict[str, Any] | None = None
    last_backend_health: dict[str, Any] | None = None
    desired_active_lanes = runtime.active_lanes
    next_telemetry_at = 0.0
    consecutive_bad_gateway_polls = 0
    consecutive_backend_health_failures = 0
    pause_until = 0.0
    pause_reason: str | None = None
    in_flight_remote_permutations = 0
    lane_remote_cost = _estimate_lane_remote_permutations(sweep_budget_value)
    no_worker_since: float | None = None
    expand_from = expand_from or {}
    executor_cap = max_active_override or runtime.active_lanes

    def refresh_lane_window(force: bool = False) -> int:
        nonlocal last_snapshot, last_backend_health, desired_active_lanes, next_telemetry_at
        nonlocal consecutive_bad_gateway_polls, consecutive_backend_health_failures
        nonlocal pause_until, pause_reason, no_worker_since
        now = time.monotonic()
        if force or now >= next_telemetry_at:
            if not runtime.dry_run:
                last_backend_health = _poll_local_backend_health(
                    config,
                    timeout_seconds=runtime.backend_health_timeout_seconds,
                )
                if last_backend_health.get("ok"):
                    consecutive_backend_health_failures = 0
                else:
                    consecutive_backend_health_failures += 1
                    if consecutive_backend_health_failures >= CAMPAIGN_BACKEND_DOWN_THRESHOLD:
                        pause_reason = "backend_down"
                        pause_until = now + runtime.telemetry_interval_seconds
            elif last_backend_health is None:
                last_backend_health = {"ok": True, "reason": "dry_run_skipped"}

            if runtime.adaptive_lanes:
                last_snapshot = _poll_worker_pool_snapshot(runtime)
                desired_active_lanes = _desired_active_lanes(runtime, last_snapshot)
                if last_snapshot.get("ok"):
                    consecutive_bad_gateway_polls = 0
                    try:
                        slots = int(last_snapshot.get("slots") or 0)
                    except (TypeError, ValueError):
                        slots = 0
                    if (
                        slots <= 0
                        and _gateway_url(runtime)
                        and runtime.max_no_worker_wait_seconds > 0
                    ):
                        if no_worker_since is None:
                            no_worker_since = now
                        elif now - no_worker_since >= runtime.max_no_worker_wait_seconds:
                            pause_reason = "no_workers"
                            pause_until = now + runtime.telemetry_interval_seconds
                    else:
                        no_worker_since = None
                elif _gateway_url(runtime):
                    consecutive_bad_gateway_polls += 1
                    no_worker_since = None
                    if consecutive_bad_gateway_polls >= CAMPAIGN_GATEWAY_UNHEALTHY_THRESHOLD:
                        pause_reason = "gateway_unhealthy"
                        pause_until = now + runtime.telemetry_interval_seconds
            else:
                desired_active_lanes = runtime.active_lanes
                no_worker_since = None

            next_telemetry_at = now + runtime.telemetry_interval_seconds
            _append_event(
                ctx,
                "campaign",
                "lane_window",
                desired_active_lanes=desired_active_lanes,
                running_lanes=len(in_flight),
                pending_lanes=len(pending_lane_indexes),
                worker_pool_snapshot=last_snapshot,
                backend_health=last_backend_health,
                pause_reason=pause_reason,
                pause_until=round(pause_until, 3) if pause_until else None,
                in_flight_remote_permutations=in_flight_remote_permutations,
            )
            metadata["last_worker_pool_snapshot"] = last_snapshot
            metadata["last_backend_health"] = last_backend_health
            metadata["desired_active_lanes"] = desired_active_lanes
            metadata["campaign_pause_reason"] = pause_reason
            metadata["in_flight_remote_permutations"] = in_flight_remote_permutations
            write_run_metadata(ctx.run_dir, metadata)
        return desired_active_lanes

    def submission_allowed() -> bool:
        if runtime.dry_run:
            return True
        if time.monotonic() < pause_until:
            return False
        if consecutive_backend_health_failures >= CAMPAIGN_BACKEND_DOWN_THRESHOLD:
            return False
        if (
            runtime.adaptive_lanes
            and _gateway_url(runtime)
            and consecutive_bad_gateway_polls >= CAMPAIGN_GATEWAY_UNHEALTHY_THRESHOLD
        ):
            return False
        if pause_reason == "no_workers":
            return False
        return True

    def lane_submission_reason() -> str | None:
        if consecutive_backend_health_failures >= CAMPAIGN_BACKEND_DOWN_THRESHOLD:
            return "not_started_backend_down"
        if pause_reason == "no_workers":
            return "not_started_no_workers"
        if (
            runtime.adaptive_lanes
            and _gateway_url(runtime)
            and consecutive_bad_gateway_polls >= CAMPAIGN_GATEWAY_UNHEALTHY_THRESHOLD
        ):
            return "not_started_gateway_unhealthy"
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=executor_cap) as executor:
        refresh_lane_window(force=True)
        while pending_lane_indexes or in_flight:
            desired = refresh_lane_window()
            effective_desired = min(desired, executor_cap)
            if not stop_after_baseline:
                token_budget = _effective_remote_token_budget(
                    last_snapshot,
                    runtime,
                    lane_remote_cost,
                )
            else:
                token_budget = None

            while pending_lane_indexes and len(in_flight) < effective_desired:
                if not submission_allowed():
                    blocked_reason = lane_submission_reason()
                    if blocked_reason:
                        blocked_indexes = list(pending_lane_indexes)
                        pending_lane_indexes.clear()
                        results.extend(
                            _mark_unstarted_lanes(
                                pending_lane_indexes=blocked_indexes,
                                reason=blocked_reason,
                                started_at=_now_iso(),
                            )
                        )
                        metadata["blocked_lane_count"] = len(blocked_indexes)
                        metadata["blocked_lane_reason"] = blocked_reason
                        write_run_metadata(ctx.run_dir, metadata)
                    break

                if (
                    token_budget is not None
                    and in_flight_remote_permutations + lane_remote_cost > token_budget
                ):
                    break

                lane_index = pending_lane_indexes.pop(0)
                prior = expand_from.get(lane_index)
                future = executor.submit(
                    _run_lane,
                    ctx,
                    runtime=runtime,
                    lane_index=lane_index,
                    seed_indicators=seed_indicators,
                    seed_plan=seed_plan,
                    seed_plan_path=seed_plan_path,
                    budget=budget,
                    sweep_budget_label=sweep_budget_label,
                    max_sweep_permutations=sweep_budget_value,
                    reward_matrix=reward_matrix,
                    stop_after_baseline=stop_after_baseline,
                    existing_lane_run_id=prior.run_id if prior else None,
                    existing_lane_run_dir=Path(prior.run_dir) if prior and prior.run_dir else None,
                )
                in_flight[future] = lane_index
                if token_budget is not None:
                    in_flight_remote_permutations += lane_remote_cost
                _append_event(
                    ctx,
                    "campaign",
                    "lane_submitted",
                    lane_index=lane_index,
                    running_lanes=len(in_flight),
                    desired_active_lanes=effective_desired,
                    pending_lanes=len(pending_lane_indexes),
                    stop_after_baseline=stop_after_baseline,
                    expand_lane=prior is not None,
                )

            if not in_flight and pending_lane_indexes and not submission_allowed():
                blocked_reason = lane_submission_reason() or "not_started_backend_down"
                blocked_indexes = list(pending_lane_indexes)
                pending_lane_indexes.clear()
                results.extend(
                    _mark_unstarted_lanes(
                        pending_lane_indexes=blocked_indexes,
                        reason=blocked_reason,
                        started_at=_now_iso(),
                    )
                )
                metadata["blocked_lane_count"] = len(blocked_indexes)
                metadata["blocked_lane_reason"] = blocked_reason
                write_run_metadata(ctx.run_dir, metadata)
                break

            if not in_flight:
                if pending_lane_indexes and token_budget is not None:
                    time.sleep(1.0)
                    continue
                if not pending_lane_indexes:
                    break
                time.sleep(1.0)
                continue

            done, _pending = concurrent.futures.wait(
                set(in_flight),
                timeout=1.0,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                lane_index = in_flight.pop(future, None)
                lane_result = future.result()
                results.append(lane_result)
                if token_budget is not None and lane_index is not None:
                    in_flight_remote_permutations = max(
                        0,
                        in_flight_remote_permutations - lane_remote_cost,
                    )
                completed = len(results)
                _append_event(
                    ctx,
                    "campaign",
                    "lane_finished",
                    completed_lanes=completed,
                    total_lanes=len(lane_indexes),
                    active_lanes=runtime.active_lanes,
                    desired_active_lanes=desired_active_lanes,
                    running_lanes=len(in_flight),
                    pending_lanes=len(pending_lane_indexes),
                    lane_result=lane_result.as_dict(),
                )
                metadata["completed_lanes"] = completed
                metadata["lane_results"] = [result.as_dict() for result in results]
                metadata["desired_active_lanes"] = desired_active_lanes
                metadata["in_flight_remote_permutations"] = in_flight_remote_permutations
                write_run_metadata(ctx.run_dir, metadata)

    campaign_state = {
        "last_snapshot": last_snapshot,
        "last_backend_health": last_backend_health,
        "pause_reason": pause_reason,
        "consecutive_bad_gateway_polls": consecutive_bad_gateway_polls,
        "consecutive_backend_health_failures": consecutive_backend_health_failures,
    }
    return results, campaign_state


def _run_campaign_lanes(
    *,
    ctx: PlayHandContext,
    runtime: MassiveRuntimeConfig,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any] | None,
    seed_plan_path: Path | None,
    budget: dict[str, Any],
    sweep_budget_label: str,
    sweep_budget_value: int,
    reward_matrix: dict[str, Any] | None,
    metadata: dict[str, Any],
) -> list[MassiveLaneResult]:
    config = ctx.config
    lane_indexes = list(range(1, runtime.lanes + 1))
    if runtime.staged_campaign and not runtime.dry_run:
        scaffold_cap = max(1, min(runtime.scaffold_active_lanes, runtime.active_lanes))
        baseline_results, baseline_state = _run_campaign_lane_executor(
            ctx=ctx,
            runtime=runtime,
            config=config,
            seed_indicators=seed_indicators,
            seed_plan=seed_plan,
            seed_plan_path=seed_plan_path,
            budget=budget,
            sweep_budget_label=sweep_budget_label,
            sweep_budget_value=sweep_budget_value,
            reward_matrix=reward_matrix,
            metadata=metadata,
            lane_indexes=lane_indexes,
            stop_after_baseline=True,
            max_active_override=scaffold_cap,
        )
        metadata["campaign_stage"] = "baseline_screened"
        metadata["baseline_stage_state"] = baseline_state
        survivors = {
            int(result.lane_id.split("_")[-1]): result
            for result in baseline_results
            if result.status == "baseline_screened" and result.run_dir
        }
        metadata["baseline_survivor_count"] = len(survivors)
        write_run_metadata(ctx.run_dir, metadata)
        if not survivors:
            return baseline_results
        expand_results, expand_state = _run_campaign_lane_executor(
            ctx=ctx,
            runtime=runtime,
            config=config,
            seed_indicators=seed_indicators,
            seed_plan=seed_plan,
            seed_plan_path=seed_plan_path,
            budget=budget,
            sweep_budget_label=sweep_budget_label,
            sweep_budget_value=sweep_budget_value,
            reward_matrix=reward_matrix,
            metadata=metadata,
            lane_indexes=sorted(survivors),
            expand_from=survivors,
            max_active_override=runtime.active_lanes,
        )
        metadata["campaign_stage"] = "expanded"
        metadata["expand_stage_state"] = expand_state
        write_run_metadata(ctx.run_dir, metadata)
        expand_by_lane = {result.lane_id: result for result in expand_results}
        merged: list[MassiveLaneResult] = []
        for baseline in baseline_results:
            expanded = expand_by_lane.get(baseline.lane_id)
            merged.append(expanded or baseline)
        return merged

    results, _state = _run_campaign_lane_executor(
        ctx=ctx,
        runtime=runtime,
        config=config,
        seed_indicators=seed_indicators,
        seed_plan=seed_plan,
        seed_plan_path=seed_plan_path,
        budget=budget,
        sweep_budget_label=sweep_budget_label,
        sweep_budget_value=sweep_budget_value,
        reward_matrix=reward_matrix,
        metadata=metadata,
        lane_indexes=lane_indexes,
    )
    return results


def cmd_play_hand_massive(
    *,
    lanes: int = DEFAULT_MASSIVE_LANES,
    active_lanes: int = DEFAULT_MASSIVE_ACTIVE_LANES,
    instrument: list[str] | None = None,
    instrument_pool: list[str] | None = None,
    timeframe: str = "M5",
    sweep_budget: str | None = "low",
    max_sweep_permutations: int | None = None,
    max_reward_r: float | None = None,
    min_indicators: int = 1,
    max_indicators: int = 4,
    seed: int | None = None,
    screen_months: int = 3,
    coarse_mode: str = "evolutionary",
    focused: bool = False,
    baseline_floor: float | None = DEFAULT_BASELINE_FLOOR,
    job_timeout_seconds: int = PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    sweep_timeout_seconds: int = PLAY_HAND_DEFAULT_SWEEP_TIMEOUT_SECONDS,
    keep_cloud_profiles: bool = False,
    adaptive_lanes: bool = True,
    adaptive_fail_open: bool = False,
    min_active_lanes: int = 1,
    target_worker_slots_per_lane: int = 32,
    scaffold_active_lanes: int = DEFAULT_SCAFFOLD_ACTIVE_LANES,
    staged_campaign: bool = True,
    remote_token_budget_multiplier: float = DEFAULT_REMOTE_TOKEN_BUDGET_MULTIPLIER,
    max_no_worker_wait_seconds: int = DEFAULT_MAX_NO_WORKER_WAIT_SECONDS,
    backend_health_timeout_seconds: int = 5,
    gateway_url: str | None = None,
    gateway_token: str | None = None,
    gateway_pool: list[str] | None = None,
    telemetry_interval_seconds: int = 30,
    dry_run: bool = False,
    as_json: bool = False,
) -> int:
    runtime = normalize_massive_runtime_config(
        MassiveRuntimeConfig(
            lanes=lanes,
            active_lanes=active_lanes,
            instrument=instrument,
            instrument_pool=instrument_pool,
            timeframe=timeframe,
            sweep_budget=sweep_budget,
            max_sweep_permutations=max_sweep_permutations,
            max_reward_r=max_reward_r,
            min_indicators=min_indicators,
            max_indicators=max_indicators,
            seed=seed,
            screen_months=screen_months,
            coarse_mode=coarse_mode,
            run_focused=focused,
            baseline_floor=baseline_floor,
            job_timeout_seconds=job_timeout_seconds,
            sweep_timeout_seconds=sweep_timeout_seconds,
            keep_cloud_profiles=keep_cloud_profiles,
            adaptive_lanes=adaptive_lanes,
            adaptive_fail_open=adaptive_fail_open,
            min_active_lanes=min_active_lanes,
            target_worker_slots_per_lane=target_worker_slots_per_lane,
            scaffold_active_lanes=scaffold_active_lanes,
            staged_campaign=staged_campaign,
            remote_token_budget_multiplier=remote_token_budget_multiplier,
            max_no_worker_wait_seconds=max_no_worker_wait_seconds,
            backend_health_timeout_seconds=backend_health_timeout_seconds,
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            gateway_pool=gateway_pool,
            telemetry_interval_seconds=telemetry_interval_seconds,
            dry_run=dry_run,
        )
    )
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    run_id = _campaign_run_id()
    run_dir = _derived_campaign_root(config) / run_id
    ctx = _new_play_hand_context(
        config=config,
        cli=cli,
        run_id=run_id,
        run_dir=run_dir,
        event_name="play-hand-massive-campaign-events.jsonl",
        summary_name="play-hand-massive-campaign-summary.json",
        runtime=runtime,
    )
    ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
    ctx.evals_dir.mkdir(parents=True, exist_ok=True)

    if not runtime.dry_run:
        cli.ensure_login()
    seed_indicators = (
        [
            SeedIndicator("RSI_CROSSBACK", "trigger", "event-with-lookback", "entry"),
            SeedIndicator("STOCH_CROSSOVER", "trigger", "event-with-lookback", "entry"),
            SeedIndicator("MA_SLOPE_TREND", "context", "state", "higher-context"),
            SeedIndicator("ADX", "filter", "state", "higher-context"),
            SeedIndicator("WICK_REJECTION", "trigger", "event-with-lookback", "entry"),
        ]
        if runtime.dry_run
        else _seed_hand(config, cli, run_dir)
    )
    seed_plan, seed_plan_path = _load_play_hand_seed_plan(config)
    budget = resolve_sweep_budget(
        sweep_budget=runtime.sweep_budget,
        max_sweep_permutations=runtime.max_sweep_permutations,
    )
    sweep_budget_label = str(budget["label"])
    sweep_budget_value = int(budget["value"])
    reward_matrix = play_hand_reward_matrix(runtime.max_reward_r)
    metadata: dict[str, Any] = {
        "schema_version": PLAY_HAND_MASSIVE_SCHEMA_VERSION,
        "runner": PLAY_HAND_MASSIVE_RUNNER,
        "run_kind": "play_hand_massive_campaign",
        "run_id": run_id,
        "run_status": "running",
        "created_at": _now_iso(),
        "campaign_dir": str(run_dir.resolve()),
        "runs_root": str(config.runs_root.resolve()),
        "lanes": runtime.lanes,
        "active_lanes": runtime.active_lanes,
        "timeframe": runtime.timeframe,
        "instrument": runtime.instrument,
        "instrument_pool": runtime.instrument_pool or list(DEFAULT_INSTRUMENT_POOL),
        "screen_months": runtime.screen_months,
        "coarse_mode": runtime.coarse_mode,
        "focused": runtime.run_focused,
        "baseline_floor": runtime.baseline_floor,
        "sweep_budget": sweep_budget_label,
        "sweep_budget_value": sweep_budget_value,
        "sweep_budget_source": budget.get("source"),
        "max_sweep_permutations": sweep_budget_value,
        "reward_matrix": reward_matrix,
        "deep_replay_job_timeout_seconds": runtime.job_timeout_seconds,
        "sweep_timeout_seconds": runtime.sweep_timeout_seconds,
        "adaptive_lanes": runtime.adaptive_lanes,
        "adaptive_fail_open": runtime.adaptive_fail_open,
        "min_active_lanes": runtime.min_active_lanes,
        "target_worker_slots_per_lane": runtime.target_worker_slots_per_lane,
        "scaffold_active_lanes": runtime.scaffold_active_lanes,
        "staged_campaign": runtime.staged_campaign,
        "remote_token_budget_multiplier": runtime.remote_token_budget_multiplier,
        "max_no_worker_wait_seconds": runtime.max_no_worker_wait_seconds,
        "backend_health_timeout_seconds": runtime.backend_health_timeout_seconds,
        "gateway_url": _gateway_url(runtime),
        "gateway_pool": runtime.gateway_pool,
        "telemetry_interval_seconds": runtime.telemetry_interval_seconds,
        "seed": runtime.seed,
        "play_hand_seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
        "play_hand_seed_plan_loaded": seed_plan is not None,
        "dry_run": runtime.dry_run,
        "design_note": (
            "Backlog-oriented PlayHand campaign: many independent lanes, no inline "
            "36-month catch-up/profile-drop tail work."
        ),
    }
    write_run_metadata(run_dir, metadata)
    _append_event(
        ctx,
        "campaign",
        "started",
        lanes=runtime.lanes,
        active_lanes=runtime.active_lanes,
        sweep_budget=sweep_budget_label,
        sweep_budget_value=sweep_budget_value,
    )

    started_perf = time.perf_counter()
    results = _run_campaign_lanes(
        ctx=ctx,
        runtime=runtime,
        seed_indicators=seed_indicators,
        seed_plan=seed_plan,
        seed_plan_path=seed_plan_path,
        budget=budget,
        sweep_budget_label=sweep_budget_label,
        sweep_budget_value=sweep_budget_value,
        reward_matrix=reward_matrix,
        metadata=metadata,
    )

    cleanup = _cleanup_registered_profiles(
        ctx,
        keep_cloud_profiles=runtime.keep_cloud_profiles,
        reason="play_hand_massive_completed",
    )
    elapsed_seconds = round(time.perf_counter() - started_perf, 3)
    completed_results = [result for result in results if result.status == "completed"]
    failed_results = [result for result in results if result.status == "failed"]
    infrastructure_blocked = [
        result
        for result in results
        if result.status.startswith("not_started_")
    ]
    best_result = max(
        (result for result in results if result.best_score is not None),
        key=lambda result: float(result.best_score or float("-inf")),
        default=None,
    )
    summary = {
        "run_id": run_id,
        "runner": PLAY_HAND_MASSIVE_RUNNER,
        "campaign_dir": str(run_dir.resolve()),
        "elapsed_seconds": elapsed_seconds,
        "lanes": runtime.lanes,
        "active_lanes": runtime.active_lanes,
        "adaptive_lanes": runtime.adaptive_lanes,
        "desired_active_lanes": metadata.get("desired_active_lanes"),
        "last_worker_pool_snapshot": metadata.get("last_worker_pool_snapshot"),
        "completed_lanes": len(completed_results),
        "failed_lanes": len(failed_results),
        "infrastructure_blocked_lanes": len(infrastructure_blocked),
        "campaign_pause_reason": metadata.get("campaign_pause_reason"),
        "last_backend_health": metadata.get("last_backend_health"),
        "best_lane": best_result.as_dict() if best_result is not None else None,
        "cloud_profile_cleanup": cleanup,
        "lane_results": [result.as_dict() for result in sorted(results, key=lambda item: item.lane_id)],
    }
    metadata.update(
        {
            "run_status": (
                "completed"
                if completed_results and not failed_results
                else ("infrastructure_blocked" if infrastructure_blocked and not completed_results else "partial")
            ),
            "completed_at": _now_iso(),
            "elapsed_seconds": elapsed_seconds,
            "completed_lanes": len(completed_results),
            "failed_lanes": len(failed_results),
            "best_lane": summary["best_lane"],
            "cloud_profile_cleanup": cleanup,
            "desired_active_lanes": summary["desired_active_lanes"],
            "last_worker_pool_snapshot": summary["last_worker_pool_snapshot"],
            "lane_run_ids": [
                result.run_id for result in results if result.run_id
            ],
            "lane_results": summary["lane_results"],
        }
    )
    write_run_metadata(run_dir, metadata)
    _write_json(ctx.summary_path, summary)
    if as_json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
    else:
        _render_campaign_table(results)
        console.print(f"[bold]Campaign dir[/]: {run_dir}")
    return 0 if completed_results else 1


__all__ = [
    "DEFAULT_BASELINE_FLOOR",
    "DEFAULT_MASSIVE_ACTIVE_LANES",
    "DEFAULT_MASSIVE_LANES",
    "DEFAULT_REMOTE_TOKEN_BUDGET_MULTIPLIER",
    "DEFAULT_SCAFFOLD_ACTIVE_LANES",
    "PLAY_HAND_MASSIVE_CAMPAIGNS_DIR",
    "PLAY_HAND_MASSIVE_LANE_SCHEMA_VERSION",
    "PLAY_HAND_MASSIVE_RUNNER",
    "MassiveRuntimeConfig",
    "cmd_play_hand_massive",
    "_desired_active_lanes",
    "_poll_local_backend_health",
    "_effective_remote_token_budget",
    "_remote_token_budget",
    "_run_campaign_lane_executor",
    "lane_seed",
    "normalize_massive_runtime_config",
    "should_expand_lane",
]
