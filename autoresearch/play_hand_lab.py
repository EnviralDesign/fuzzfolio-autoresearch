from __future__ import annotations

import calendar
import copy
import concurrent.futures
import hashlib
import itertools
import json
import math
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

import requests
from rich.console import Console

try:  # pragma: no cover - exercised when optional C extension is installed.
    import orjson as _orjson
except Exception:  # pragma: no cover - stdlib fallback for unusual environments.
    _orjson = None

from .play_hand_lab_auth import load_lab_gateway_token
from .config import AppConfig, load_config
from .fuzzfolio import CliError, FuzzfolioCli
from .evidence_plan import build_replay_evidence_plan, canonical_json, canonical_sha256
from .durable_execution import (
    DurableExecutionError,
    DurableExecutionJournal,
    artifact_receipt,
    atomic_write_json,
    validate_artifact_receipt,
)
from .ledger import (
    append_attempt_row,
    attempts_path_for_run_dir,
    load_attempts,
    load_run_metadata,
    make_attempt_record,
    write_run_metadata,
)
from .level_c_operator import (
    PROFILE_MODEL_SOURCE_FILES,
    validate_executor_runtime_binding,
    validate_profile_model_source_lock,
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
    PLAY_HAND_DEFAULT_JOB_TIMEOUT_SECONDS,
    PLAY_HAND_RUNNER,
    PLAY_HAND_SWEEP_PERMUTATION_LIMIT,
    PlayHandContext,
    SeedIndicator,
    _append_event,
    _as_float,
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
    _play_hand_role_for_phase,
    apply_play_hand_profile_defaults,
    apply_role_timeframe_defaults,
    apply_seed_indicator_metadata,
    apply_seed_pair_template_defaults,
    apply_seed_pair_timing_hints,
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
    sample_screen_anchor,
)
from .playhand_health import build_play_hand_evidence, build_play_hand_health
from .plotting import render_progress_artifacts
from .recipe_priors import (
    campaign_diversity_cap_count,
    canonical_campaign_candidate_attributes,
    canonical_campaign_candidate_id,
    ordered_campaign_policy_conflicts,
    validate_seed_plan_campaign_policy,
)
from .scoring import AttemptScore, build_attempt_score, load_sensitivity_snapshot


console = Console(safe_box=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()

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
DEFAULT_LAB_MAX_RESULTS_PER_CYCLE = 1000
DEFAULT_LAB_MAX_DRAIN_SECONDS = 0.5
DEFAULT_LAB_RESULT_READ_FAILURE_LIMIT = 5
DEFAULT_LAB_ENQUEUE_FAILURE_LIMIT = 5
DEFAULT_LAB_ENQUEUE_RETRY_BASE_SECONDS = 1.0
DEFAULT_LAB_TERMINAL_LANE_RETENTION = 512
_EXACT_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_LINEAGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}$")
_SAFE_CAMPAIGN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}$")
DEFAULT_LAB_LOG_MODE = "barrier"
DEFAULT_LAB_BARRIER_INTERVAL_SECONDS = 5.0
DEFAULT_LAB_BARRIER_LANE_LIMIT = 24
DEFAULT_LAB_SWEEP_SHARD_SIZE = 8
POLICY_HONEST_LAB_STATE_SCHEMA_VERSION = "play-hand-policy-runtime-v1"
POLICY_EXHAUSTION_OUTCOME = "policy_lane_exhausted"
POLICY_CANDIDATE_FALLBACK_ATTEMPTS = 64
DEFAULT_LAB_SCRUTINY_MONTHS = 36
DEFAULT_LAB_VALIDATION_MONTHS = 12
DEFAULT_LAB_VALIDATION_MIN_SCORE = 45.0
DEFAULT_LAB_FINAL_MIN_SCORE = 40.0
DEFAULT_LAB_SCREEN_ANCHOR_MODE = "random"
DEFAULT_LAB_SCREEN_ANCHOR_ENVELOPE_MONTHS = 36
PLAY_HAND_LAB_PIPELINE_VERSION = "play_hand_lab_pipeline_v3"
PLAY_HAND_LAB_SCREEN_PIPELINE = "screen"
PLAY_HAND_LAB_PLAY_HAND_PIPELINE = "play_hand"
PLAY_HAND_LAB_STAGE_ORDER = (
    "baseline",
    "lookback",
    "coarse",
    "focused",
    "validation",
    "instrument_scout",
    "scrutiny",
    "artifacts",
)
TERMINAL_OUTCOME_PROMOTED = "promoted"
TERMINAL_OUTCOME_RESEARCH_NONVIABLE = "research_nonviable"
TERMINAL_OUTCOME_INFRASTRUCTURE_FAILURE = "infrastructure_failure"
TERMINAL_OUTCOME_INCOMPLETE = "incomplete"
_WORKER_RESULT_DELIVERY_FIELDS = frozenset(
    {
        "accepted_at",
        "accepted_at_wall",
        "delivery_id",
        "delivered_at",
        "lease_id",
        "read_at",
        "worker_id",
    }
)
_WORKER_RESULT_RUNTIME_FIELDS = frozenset(
    {
        "client_origin",
        "completed_at",
        "current_step",
        "current_step_started_at",
        "duration_seconds",
        "expires_at",
        "heartbeat_at",
        "job_id",
        "lake_cache",
        "promoted_at",
        "promoted_from_job_id",
        "requested_at",
        "requested_by_user_id",
        "retention_behavior",
        "retention_reason",
        "retention_ttl_seconds",
        "source_client_origin",
        "source_kind",
        "started_at",
        "steps_completed",
        "subprocess_progress",
        "total_steps",
        "worker_id",
        "worker_lease_id",
        "worker_pool",
        "worker_queue_name",
        "worker_transport",
        "workspace_attempt_id",
        "workspace_id",
    }
)


def _worker_result_identity_payload(lab_result: dict[str, Any]) -> dict[str, Any]:
    """Return the replay-evidence projection used for duplicate-result checks.

    A gateway can redeliver one completed task through a different lease or worker,
    and retries naturally produce fresh cache and performance timing telemetry.  None
    of those values changes the replay evidence.  The inner result remains intact
    except for its performance section, so a changed score, aggregate, execution
    receipt, request, or terminal detail still conflicts fail-closed.
    """

    payload = {
        key: value
        for key, value in lab_result.items()
        if key not in _WORKER_RESULT_DELIVERY_FIELDS
    }
    worker_result = payload.get("result")
    if not isinstance(worker_result, dict):
        return payload

    normalized_worker_result = {
        key: value
        for key, value in worker_result.items()
        if key not in _WORKER_RESULT_RUNTIME_FIELDS
    }
    replay_result = normalized_worker_result.get("result")
    if isinstance(replay_result, dict) and "performance" in replay_result:
        normalized_replay_result = dict(replay_result)
        normalized_replay_result.pop("performance", None)
        normalized_worker_result["result"] = normalized_replay_result
    payload["result"] = normalized_worker_result
    return payload


def _worker_result_identity(lab_result: dict[str, Any]) -> str:
    return canonical_sha256(_worker_result_identity_payload(lab_result))


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
    seed_plan_path: Path | None = None
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
    validation_months: int = DEFAULT_LAB_VALIDATION_MONTHS
    validation_min_score: float = DEFAULT_LAB_VALIDATION_MIN_SCORE
    scrutiny_months: int = DEFAULT_LAB_SCRUTINY_MONTHS
    final_min_score: float = DEFAULT_LAB_FINAL_MIN_SCORE
    screen_anchor_mode: Literal["now", "random"] = DEFAULT_LAB_SCREEN_ANCHOR_MODE
    screen_anchor_envelope_months: int = DEFAULT_LAB_SCREEN_ANCHOR_ENVELOPE_MONTHS
    as_of_date: str | None = None
    campaign_id: str | None = None
    lake_manifest_sha256: str | None = None
    research_generation_id: str | None = None
    level_c_protocol_id: str | None = None
    cutoff_key: str | None = None
    source_snapshot_sha256: str | None = None
    universe_id: str | None = None
    universe_manifest_sha256: str | None = None
    expected_seed_plan_sha256: str | None = None
    current_atlas_generation: str | None = None
    current_atlas_run_sequence: int | None = None
    formal_authority_kind: Literal["level_c", "phase3"] | None = None
    phase3_authority_path: Path | None = None
    phase2_capsule_root: Path | None = None
    campaign_policy_manifest_path: Path | None = None
    phase3_authority_id: str | None = None
    phase3_authority_sha256: str | None = None
    campaign_policy_manifest_sha256: str | None = None
    campaign_policy_source_file_sha256: str | None = None
    operator_launch_worker_image: str | None = None
    execution_plan_path: Path | None = None
    execution_plan_id: str | None = None
    resume: bool = False
    instrument_scout_size: int = INSTRUMENT_SCOUT_DEFAULT_SIZE
    instrument_scout_max_selected: int = INSTRUMENT_SCOUT_DEFAULT_MAX_SELECTED
    fake_work_seconds: float = 1.0
    deadline_seconds: float = 3600.0
    max_attempts: int = 8
    poll_interval_seconds: float = 1.0
    max_wait_seconds: float = 3600.0
    result_batch_size: int = DEFAULT_LAB_RESULT_BATCH_SIZE
    max_results_per_cycle: int = DEFAULT_LAB_MAX_RESULTS_PER_CYCLE
    max_drain_seconds: float = DEFAULT_LAB_MAX_DRAIN_SECONDS
    result_read_failure_limit: int = DEFAULT_LAB_RESULT_READ_FAILURE_LIMIT
    enqueue_failure_limit: int = DEFAULT_LAB_ENQUEUE_FAILURE_LIMIT
    enqueue_retry_base_seconds: float = DEFAULT_LAB_ENQUEUE_RETRY_BASE_SECONDS
    terminal_lane_retention: int = DEFAULT_LAB_TERMINAL_LANE_RETENTION
    dry_run: bool = False
    strict_scoring: bool = False
    retain_raw_lab_artifacts: bool = False
    json_output: bool = False
    log_mode: Literal["barrier", "stream", "quiet"] = "barrier"
    barrier_interval_seconds: float = DEFAULT_LAB_BARRIER_INTERVAL_SECONDS
    barrier_lane_limit: int = DEFAULT_LAB_BARRIER_LANE_LIMIT
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
    phase_started_at: dict[str, str] = field(default_factory=dict)
    phase_completed_at: dict[str, str] = field(default_factory=dict)
    phase_task_counts: dict[str, int] = field(default_factory=dict)
    phase_completed_task_counts: dict[str, int] = field(default_factory=dict)
    phase_failed_task_counts: dict[str, int] = field(default_factory=dict)
    phase_lifecycle_events: list[dict[str, Any]] = field(default_factory=list)
    current_phase: str = "queued"
    terminal: bool = False
    run_promoted: bool = False
    tombstone_reason: str | None = None
    tombstone_reasons: list[str] = field(default_factory=list)
    terminal_outcome_category: str | None = None
    incumbent_profile_path: Path | None = None
    incumbent_profile_ref: str | None = None
    incumbent_profile_payload: dict[str, Any] | None = None
    incumbent_timeframe: str | None = None
    incumbent_instruments: list[str] = field(default_factory=list)
    incumbent_score: float | None = None
    incumbent_phase: str | None = None
    screen_anchor_mode: str = DEFAULT_LAB_SCREEN_ANCHOR_MODE
    screen_analysis_window_start: str | None = None
    screen_analysis_window_end: str | None = None
    screen_anchor_offset_days: int | None = None
    last_sweep_payload: dict[str, Any] | None = None
    last_sweep_axes: list[str] = field(default_factory=list)
    skip_focused_and_scout: bool = False
    instrument_scout_result: dict[str, Any] | None = None
    final_attempt_id: str | None = None
    best_score: float | None = None
    best_attempt_id: str | None = None
    policy_assignment: dict[str, Any] = field(default_factory=dict)


@dataclass
class LabCampaignHistory:
    pruned_lane_count: int = 0
    pruned_task_count: int = 0
    pruned_completed_task_count: int = 0
    pruned_failed_task_count: int = 0
    pruned_promoted_lane_count: int = 0
    pruned_tombstoned_lane_count: int = 0
    best_score: float | None = None
    campaign_policy_state: dict[str, Any] | None = None


_TASK_SPEC_STATE_OMIT_KEYS = frozenset({"profile_payload", "params_by_index"})
_PROFILE_SNAPSHOT_KEYS = (
    ("base_profile_snapshot", "base_profile_snapshot_sha256"),
    ("inline_profile_snapshot", "inline_profile_snapshot_sha256"),
)


def _canonical_params(params_by_index: Mapping[Any, Any]) -> dict[str, Any]:
    """Normalize params_by_index keys to strings for content hashing."""
    return {str(key): value for key, value in params_by_index.items()}


def _store_profile_blob(campaign_dir: Path, profile: Mapping[str, Any]) -> str:
    digest = canonical_sha256(dict(profile))
    hex_digest = digest.removeprefix("sha256:")
    blob_dir = Path(campaign_dir) / "profile-blobs"
    blob_path = blob_dir / f"{hex_digest}.json"
    if not blob_path.is_file():
        atomic_write_json(blob_path, dict(profile))
    return digest


def _load_profile_blob(campaign_dir: Path, digest: str) -> dict[str, Any]:
    token = str(digest or "").strip()
    hex_digest = token.removeprefix("sha256:")
    if not hex_digest:
        raise DurableExecutionError("profile blob digest is required")
    blob_path = Path(campaign_dir) / "profile-blobs" / f"{hex_digest}.json"
    if not blob_path.is_file():
        raise DurableExecutionError(f"profile blob missing: {blob_path}")
    try:
        payload = json.loads(blob_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DurableExecutionError(f"profile blob unreadable: {blob_path}") from exc
    if not isinstance(payload, dict):
        raise DurableExecutionError(f"profile blob must be a JSON object: {blob_path}")
    return payload


def _profile_snapshot_containers(task: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [task]
    nested = task.get("payload")
    if isinstance(nested, dict):
        containers.append(nested)
    return containers


def _detach_task_profile_snapshots(task: dict[str, Any], campaign_dir: Path) -> dict[str, Any]:
    detached = copy.deepcopy(task)
    for container in _profile_snapshot_containers(detached):
        for snapshot_key, sha_key in _PROFILE_SNAPSHOT_KEYS:
            value = container.get(snapshot_key)
            if isinstance(value, dict) and value:
                container[sha_key] = _store_profile_blob(campaign_dir, value)
                del container[snapshot_key]
    return detached


def _attach_task_profile_snapshots(task: dict[str, Any], campaign_dir: Path) -> dict[str, Any]:
    attached = copy.deepcopy(task)
    for container in _profile_snapshot_containers(attached):
        for snapshot_key, sha_key in _PROFILE_SNAPSHOT_KEYS:
            existing = container.get(snapshot_key)
            if isinstance(existing, dict) and existing:
                continue
            digest = container.get(sha_key)
            if isinstance(digest, str) and digest.strip():
                container[snapshot_key] = _load_profile_blob(campaign_dir, digest)
    return attached


def _lane_state_payload(lane: LabLaneState) -> dict[str, Any]:
    payload = asdict(lane)
    for key in ("run_dir", "profile_path", "incumbent_profile_path"):
        value = payload.get(key)
        payload[key] = str(value) if value is not None else None
    for key in ("completed_task_ids", "failed_task_ids"):
        payload[key] = sorted(getattr(lane, key))
    # Profiles reload from disk on resume; omit heavy copies from durable state.
    payload["profile_payload"] = None
    payload["incumbent_profile_payload"] = None
    slim_specs: dict[str, dict[str, Any]] = {}
    for task_id, spec in (payload.get("task_specs") or {}).items():
        if not isinstance(spec, dict):
            continue
        slim_specs[str(task_id)] = {
            key: value
            for key, value in spec.items()
            if key not in _TASK_SPEC_STATE_OMIT_KEYS
        }
    payload["task_specs"] = slim_specs
    return payload


def _rebuild_sweep_shard_params_by_index(
    lane: LabLaneState,
    task_spec: Mapping[str, Any],
) -> dict[int, dict[str, Any]] | None:
    """Rebuild params_by_index omitted from durable state using axes + profile."""
    if str(task_spec.get("task_kind") or "") != "sweep_shard":
        return None
    existing = task_spec.get("params_by_index")
    if isinstance(existing, dict):
        return {
            _sweep_permutation_index(index, context="persisted sweep task params"): dict(params)
            for index, params in existing.items()
            if isinstance(params, dict)
        }
    axis_texts = task_spec.get("axes")
    if not isinstance(axis_texts, list) or not axis_texts:
        return None
    profile_payload = lane.incumbent_profile_payload or lane.profile_payload or {}
    sweep_axes = [
        axis
        for axis_text in axis_texts
        if isinstance(axis_text, str)
        and (axis := _axis_to_sweep_axis(profile_payload, axis_text)) is not None
    ]
    if not sweep_axes:
        return None
    expanded_count = task_spec.get("expanded_permutation_count")
    try:
        max_permutations = int(expanded_count) if expanded_count is not None else None
    except (TypeError, ValueError):
        max_permutations = None
    if max_permutations is None:
        axis_plan = task_spec.get("axis_plan")
        if isinstance(axis_plan, dict) and axis_plan.get("max_permutations") is not None:
            try:
                max_permutations = int(axis_plan["max_permutations"])
            except (TypeError, ValueError):
                max_permutations = None
    params = _expand_sweep_params(sweep_axes, max_permutations=max_permutations)
    if not params:
        return None
    start = _sweep_permutation_index(
        task_spec.get("permutation_start"), context="persisted sweep task spec"
    )
    count = _sweep_permutation_index(
        task_spec.get("permutation_count"), context="persisted sweep task spec"
    )
    if count <= 0 or start < 0 or start + count > len(params):
        return None
    chunk = params[start : start + count]
    return {start + offset: dict(param) for offset, param in enumerate(chunk)}


def _hydrate_lane_profiles(lane: LabLaneState) -> None:
    """Reload lane profile payloads from disk and restore slimmed sweep params."""
    if lane.profile_payload is None and lane.profile_path is not None and lane.profile_path.is_file():
        lane.profile_payload = _inner_profile_payload(_load_json(lane.profile_path))
    if (
        lane.incumbent_profile_payload is None
        and lane.incumbent_profile_path is not None
        and lane.incumbent_profile_path.is_file()
    ):
        lane.incumbent_profile_payload = _inner_profile_payload(
            _load_json(lane.incumbent_profile_path)
        )
    for task_spec in lane.task_specs.values():
        if not isinstance(task_spec, dict):
            continue
        if str(task_spec.get("task_kind") or "") != "sweep_shard":
            continue
        if isinstance(task_spec.get("params_by_index"), dict):
            continue
        try:
            rebuilt = _rebuild_sweep_shard_params_by_index(lane, task_spec)
        except DurableExecutionError:
            continue
        if rebuilt is not None:
            expected_sha = task_spec.get("params_by_index_sha256")
            if isinstance(expected_sha, str) and expected_sha:
                observed_sha = canonical_sha256(_canonical_params(rebuilt))
                if observed_sha != expected_sha:
                    raise DurableExecutionError(
                        "rebuilt params_by_index does not match params_by_index_sha256"
                    )
            task_spec["params_by_index"] = rebuilt


def _lane_state_from_payload(payload: dict[str, Any]) -> LabLaneState:
    values = dict(payload)
    values["run_dir"] = Path(str(values["run_dir"])).resolve(strict=False)
    for key in ("profile_path", "incumbent_profile_path"):
        values[key] = Path(str(values[key])).resolve(strict=False) if values.get(key) else None
    for key in ("completed_task_ids", "failed_task_ids"):
        values[key] = {str(item) for item in values.get(key) or []}
    lane = LabLaneState(**values)
    _hydrate_lane_profiles(lane)
    return lane


def _campaign_state_lineage(runtime: PlayHandLabRuntimeConfig, campaign_id: str) -> dict[str, Any]:
    semantic_runtime = {
        key: value
        for key, value in asdict(runtime).items()
        if key
        not in {
            "gateway_url",
            "gateway_token",
            "poll_interval_seconds",
            "max_wait_seconds",
            "result_batch_size",
            "max_results_per_cycle",
            "max_drain_seconds",
            "result_read_failure_limit",
            "enqueue_failure_limit",
            "enqueue_retry_base_seconds",
            "terminal_lane_retention",
            "json_output",
            "log_mode",
            "barrier_interval_seconds",
            "barrier_lane_limit",
            "resume",
        }
    }
    semantic_runtime = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in semantic_runtime.items()
    }
    return {
        "campaign_id": campaign_id,
        "formal_authority_kind": runtime.formal_authority_kind,
        "phase3_authority_id": runtime.phase3_authority_id,
        "phase3_authority_sha256": runtime.phase3_authority_sha256,
        "execution_plan_id": runtime.execution_plan_id,
        "research_generation_id": runtime.research_generation_id,
        "level_c_protocol_id": runtime.level_c_protocol_id,
        "cutoff_key": runtime.cutoff_key,
        "source_snapshot_sha256": runtime.source_snapshot_sha256,
        "universe_id": runtime.universe_id,
        "universe_manifest_sha256": runtime.universe_manifest_sha256,
        "seed": runtime.seed,
        "target_runs": runtime.target_runs,
        "semantic_runtime_sha256": canonical_sha256(semantic_runtime),
    }


def _write_campaign_state(
    path: Path,
    *,
    runtime: PlayHandLabRuntimeConfig,
    campaign_id: str,
    lanes: list[LabLaneState],
    history: LabCampaignHistory,
    next_lane_index: int,
    recorded_result_count: int,
    reserved_lane_indices: list[int] | None = None,
) -> None:
    history_payload = asdict(history)
    policy_state = history_payload.pop("campaign_policy_state", None)
    atomic_write_json(
        path,
        {
            "schema_version": "play-hand-lab-durable-state-v1",
            "lineage": _campaign_state_lineage(runtime, campaign_id),
            "next_lane_index": int(next_lane_index),
            "reserved_lane_indices": sorted({int(item) for item in reserved_lane_indices or []}),
            "recorded_result_count": int(recorded_result_count),
            "history": history_payload,
            "campaign_policy_state": policy_state,
            "lanes": [_lane_state_payload(lane) for lane in lanes],
        },
    )


def _load_campaign_state(
    path: Path,
    *,
    runtime: PlayHandLabRuntimeConfig,
    campaign_id: str,
) -> tuple[list[LabLaneState], LabCampaignHistory, int, list[int], int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DurableExecutionError(f"PlayHand durable state is unreadable: {path}") from exc
    if payload.get("schema_version") != "play-hand-lab-durable-state-v1":
        raise DurableExecutionError("PlayHand durable state schema mismatch")
    if payload.get("lineage") != _campaign_state_lineage(runtime, campaign_id):
        raise DurableExecutionError("PlayHand durable state lineage mismatch")
    lanes = [
        _lane_state_from_payload(dict(item))
        for item in payload.get("lanes") or []
        if isinstance(item, dict)
    ]
    history = LabCampaignHistory(**dict(payload.get("history") or {}))
    policy_state = payload.get("campaign_policy_state")
    if policy_state is not None:
        if not isinstance(policy_state, dict):
            raise DurableExecutionError("PlayHand durable campaign policy state is invalid")
        history.campaign_policy_state = dict(policy_state)
    return (
        lanes,
        history,
        int(payload.get("next_lane_index") or 0),
        sorted({int(item) for item in payload.get("reserved_lane_indices") or []}),
        int(payload.get("recorded_result_count") or 0),
    )


def _lane_allocation_checkpoint(_name: str) -> None:
    """No-op seam used by crash/restart tests around durable lane allocation."""


def _result_consumption_checkpoint(_name: str) -> None:
    """No-op seam used by crash/restart tests around result consumption."""


def _policy_lane_counts(
    campaign_policy: dict[str, Any],
    *,
    campaign_size: int,
) -> tuple[dict[str, int], list[str]]:
    """Apply the manifest's Hamilton allocation contract without local policy rules."""
    execution = campaign_policy.get("execution")
    lanes = campaign_policy.get("lanes")
    if not isinstance(execution, dict) or not isinstance(lanes, dict):
        raise DurableExecutionError("validated campaign policy is missing allocation fields")
    lane_order = [str(item) for item in execution.get("lane_tie_break_order") or []]
    if not lane_order or set(lane_order) != {"guided", "uncertain", "wild"}:
        raise DurableExecutionError("validated campaign policy has no deterministic lane order")
    if campaign_size <= 0:
        raise DurableExecutionError("policy-honest campaigns require a positive finite lane budget")
    raw_quotas = {
        lane: float(campaign_size) * float((lanes.get(lane) or {}).get("fraction"))
        for lane in lane_order
    }
    counts = {lane: int(math.floor(raw_quotas[lane])) for lane in lane_order}
    remaining = campaign_size - sum(counts.values())
    tie_order = {lane: index for index, lane in enumerate(lane_order)}
    remainders = sorted(
        lane_order,
        key=lambda lane: (-(raw_quotas[lane] - counts[lane]), tie_order[lane]),
    )
    for lane in remainders[:remaining]:
        counts[lane] += 1
    lane_plan = [
        lane
        for lane in lane_order
        for _ in range(counts[lane])
    ]
    if len(lane_plan) != campaign_size:
        raise DurableExecutionError("policy lane allocation did not consume the campaign budget")
    return counts, lane_plan


def _new_campaign_policy_state(
    campaign_policy: dict[str, Any] | None,
    *,
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, Any] | None:
    if campaign_policy is None:
        return None
    if runtime.campaign_mode != "finite" or runtime.target_runs is None:
        raise DurableExecutionError(
            "policy-honest v2 seed plans require a finite campaign with explicit target_runs"
        )
    planned_lane_counts, lane_plan = _policy_lane_counts(
        campaign_policy,
        campaign_size=int(runtime.target_runs),
    )
    attribute_contract = campaign_policy.get("diversity_attribute_contract")
    candidate_identity = campaign_policy.get("candidate_identity")
    diversity_enforcement = campaign_policy.get("diversity_enforcement")
    if (
        not isinstance(attribute_contract, dict)
        or not isinstance(candidate_identity, dict)
        or not isinstance(diversity_enforcement, dict)
    ):
        raise DurableExecutionError("validated campaign policy is missing diversity fields")
    dimensions = ("family", "recipe", "instrument", "timeframe", "indicator")
    cap_limits = {
        dimension: campaign_diversity_cap_count(
            campaign_policy,
            dimension=dimension,
            target_runs=int(runtime.target_runs),
        )
        for dimension in dimensions
    }
    negative_prior_runtime = _policy_negative_prior_runtime(
        campaign_policy,
        runtime=runtime,
    )
    return {
        "schema_version": POLICY_HONEST_LAB_STATE_SCHEMA_VERSION,
        "policy_manifest_sha256": campaign_policy.get("manifest_sha256"),
        "execution": copy.deepcopy(campaign_policy.get("execution")),
        "candidate_identity": copy.deepcopy(candidate_identity),
        "diversity_attribute_contract": copy.deepcopy(attribute_contract),
        "diversity_enforcement": copy.deepcopy(diversity_enforcement),
        "negative_prior_runtime": negative_prior_runtime,
        "campaign_size": int(runtime.target_runs),
        "planned_lane_counts": planned_lane_counts,
        "lane_plan": lane_plan,
        "cap_limits": cap_limits,
        "assigned_lane_counts": {lane: 0 for lane in planned_lane_counts},
        "used_lane_counts": {lane: 0 for lane in planned_lane_counts},
        "exhausted_lane_counts": {lane: 0 for lane in planned_lane_counts},
        "accounting": {dimension: {} for dimension in dimensions},
        "exhaustion_outcomes": {},
    }


def _campaign_policy_state(
    campaign_policy: dict[str, Any] | None,
    *,
    runtime: PlayHandLabRuntimeConfig,
    persisted: dict[str, Any] | None,
) -> dict[str, Any] | None:
    expected = _new_campaign_policy_state(campaign_policy, runtime=runtime)
    if expected is None:
        if persisted is not None:
            raise DurableExecutionError(
                "durable campaign has policy-honest state but the seed plan is legacy"
            )
        return None
    if persisted is None:
        return expected
    required_identity = (
        "schema_version",
        "policy_manifest_sha256",
        "execution",
        "candidate_identity",
        "diversity_attribute_contract",
        "diversity_enforcement",
        "negative_prior_runtime",
        "campaign_size",
        "planned_lane_counts",
        "lane_plan",
        "cap_limits",
    )
    if any(persisted.get(key) != expected.get(key) for key in required_identity):
        raise DurableExecutionError(
            "PlayHand durable campaign policy state does not match the v2 seed-plan policy"
        )
    for key in ("assigned_lane_counts", "used_lane_counts", "exhausted_lane_counts", "accounting"):
        if not isinstance(persisted.get(key), dict):
            raise DurableExecutionError(f"PlayHand durable campaign policy state is missing {key}")
    return copy.deepcopy(persisted)


def _policy_negative_prior_runtime(
    campaign_policy: dict[str, Any],
    *,
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, Any]:
    generation = str(runtime.current_atlas_generation or "").strip()
    run_sequence = runtime.current_atlas_run_sequence
    supplied = bool(generation) or run_sequence is not None
    if supplied:
        if not generation:
            raise DurableExecutionError(
                "policy-honest current Atlas generation is missing"
            )
        if (
            isinstance(run_sequence, bool)
            or not isinstance(run_sequence, int)
            or run_sequence < 0
        ):
            raise DurableExecutionError(
                "policy-honest current Atlas run sequence is invalid"
            )
        return {
            "current_atlas_generation": generation,
            "current_atlas_run_sequence": run_sequence,
            "binding_source": "runtime_authority",
        }
    if runtime.as_of_date:
        raise DurableExecutionError(
            "formal policy-honest v2 requires plan-bound current Atlas generation and run sequence"
        )
    expiry = campaign_policy.get("negative_prior_expiry")
    anchor = expiry.get("anchor") if isinstance(expiry, dict) else None
    if not isinstance(anchor, dict):
        raise DurableExecutionError("validated campaign policy has no negative-prior anchor")
    return {
        "current_atlas_generation": anchor.get("generation"),
        "current_atlas_run_sequence": anchor.get("run_sequence"),
        "binding_source": "policy_anchor_exploratory_compatibility",
    }


def _policy_lane_for_index(policy_state: dict[str, Any], lane_index: int) -> str:
    lane_plan = policy_state.get("lane_plan")
    if not isinstance(lane_plan, list) or lane_index < 0 or lane_index >= len(lane_plan):
        raise DurableExecutionError(f"policy lane index is outside the finite campaign: {lane_index}")
    lane = str(lane_plan[lane_index])
    if lane not in {"guided", "uncertain", "wild"}:
        raise DurableExecutionError(f"policy lane plan contains an invalid lane: {lane}")
    return lane


def _policy_candidate_attributes(
    deal: dict[str, Any],
    *,
    policy_state: dict[str, Any],
) -> dict[str, Any] | None:
    indicator_deal = deal.get("indicator_deal") if isinstance(deal, dict) else None
    indicator_deal = indicator_deal if isinstance(indicator_deal, dict) else {}
    pair = indicator_deal.get("pair")
    pair = pair if isinstance(pair, dict) else {}
    contract = policy_state.get("diversity_attribute_contract")
    if not isinstance(contract, dict):
        raise DurableExecutionError("policy state has no diversity attribute contract")
    expected_dimensions = {"family", "recipe", "instrument", "timeframe", "indicator"}
    if set(contract) != expected_dimensions or any(
        not isinstance(contract.get(dimension), dict)
        or not str((contract.get(dimension) or {}).get("candidate_attribute") or "")
        for dimension in expected_dimensions
    ):
        raise DurableExecutionError("policy state diversity attribute contract is unsupported")

    raw_indicators = indicator_deal.get("indicators")
    if isinstance(raw_indicators, (list, tuple, set, frozenset)):
        indicator_ids: Any = [
            (
                item.id
                if isinstance(item, SeedIndicator)
                else (
                    item.get("id") or item.get("indicator_id")
                    if isinstance(item, dict)
                    else item
                )
            )
            for item in raw_indicators
        ]
    else:
        indicator_ids = raw_indicators
    raw_candidate = {
        "recipe_id": indicator_deal.get("recipe"),
        "canonical_pair_family_id": pair.get("canonical_pair_family_id"),
        "instrument": deal.get("primary_instrument"),
        "timeframe": deal.get("timeframe"),
        "indicator_ids": indicator_ids,
    }
    # recipe_priors owns typed missing/blank/value normalization and identity.
    attributes = canonical_campaign_candidate_attributes(raw_candidate)
    attributes["candidate_id"] = canonical_campaign_candidate_id(raw_candidate)
    return attributes


def _policy_attribute_accounting_key(value: Any) -> str:
    """Encode a helper-normalized JSON value for a durable count map key."""
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DurableExecutionError("policy candidate attribute is not canonical JSON") from exc


def _policy_dimension_charge_values(
    attributes: dict[str, Any],
    *,
    dimension: str,
    contract: dict[str, Any],
) -> list[Any]:
    contract_entry = contract.get(dimension)
    if not isinstance(contract_entry, dict):
        raise DurableExecutionError(f"policy state has no {dimension} attribute contract")
    attribute_name = str(contract_entry.get("candidate_attribute") or "")
    if attribute_name not in attributes:
        raise DurableExecutionError(
            f"policy candidate is missing {dimension} source attribute"
        )
    value = attributes[attribute_name]
    if dimension != "indicator":
        return [value]
    if not isinstance(value, dict):
        raise DurableExecutionError("policy candidate indicator attribute is invalid")
    indicator_values = value.get("values")
    if value.get("state") == "present" and isinstance(indicator_values, list) and indicator_values:
        return indicator_values
    # Typed absent and explicitly empty indicator lists must not evade accounting.
    return [value]


def _policy_candidate_sort_key(
    attributes: dict[str, Any],
    *,
    policy_state: dict[str, Any],
) -> tuple[str, ...]:
    execution = policy_state.get("execution")
    if not isinstance(execution, dict):
        raise DurableExecutionError("policy state has no execution contract")
    order = execution.get("candidate_tie_break_order")
    if not isinstance(order, list):
        raise DurableExecutionError("policy state has no candidate tie-break order")
    values = {
        field: (
            str(attributes.get(field) or "")
            if field == "candidate_id"
            else _policy_attribute_accounting_key(attributes.get(field))
        )
        for field in order
    }
    return tuple(values.get(str(field), "") for field in order)


def _policy_cap_decision(
    policy_state: dict[str, Any],
    attributes: dict[str, Any],
) -> dict[str, Any]:
    accounting = policy_state.get("accounting")
    cap_limits = policy_state.get("cap_limits")
    contract = policy_state.get("diversity_attribute_contract")
    if (
        not isinstance(accounting, dict)
        or not isinstance(cap_limits, dict)
        or not isinstance(contract, dict)
    ):
        raise DurableExecutionError("policy state has no cap accounting")
    blocked_by_dimension: dict[str, list[dict[str, Any]]] = {}
    charges: dict[str, list[dict[str, Any]]] = {}
    for dimension in ("family", "recipe", "instrument", "timeframe", "indicator"):
        values = _policy_dimension_charge_values(
            attributes,
            dimension=dimension,
            contract=contract,
        )
        charges[dimension] = [
            {
                "accounting_key": _policy_attribute_accounting_key(value),
                "attribute": copy.deepcopy(value),
            }
            for value in values
        ]
        counts = accounting.get(dimension)
        if not isinstance(counts, dict):
            raise DurableExecutionError(f"policy state has no {dimension} accounting")
        limit = int(cap_limits.get(dimension) or 0)
        for charge in charges[dimension]:
            accounting_key = str(charge["accounting_key"])
            current = int(counts.get(accounting_key) or 0)
            if current + 1 > limit:
                blocked_by_dimension.setdefault(dimension, []).append(
                    {
                        "dimension": dimension,
                        "attribute": copy.deepcopy(charge["attribute"]),
                        "accounting_key": accounting_key,
                        "current_count": current,
                        "cap_limit": limit,
                    }
                )
    blocked = [
        conflict
        for dimension in ordered_campaign_policy_conflicts(set(blocked_by_dimension))
        for conflict in blocked_by_dimension[dimension]
    ]
    return {
        "outcome": "accepted" if not blocked else "cap_blocked",
        "charges": charges,
        "blocked": blocked,
    }


def _record_policy_assignment(
    policy_state: dict[str, Any],
    *,
    lane: str,
    cap_decision: dict[str, Any] | None,
    exhaustion_outcome: str | None = None,
) -> None:
    assigned = policy_state.get("assigned_lane_counts")
    used = policy_state.get("used_lane_counts")
    exhausted = policy_state.get("exhausted_lane_counts")
    if not isinstance(assigned, dict) or not isinstance(used, dict) or not isinstance(exhausted, dict):
        raise DurableExecutionError("policy state has no lane counters")
    assigned[lane] = int(assigned.get(lane) or 0) + 1
    if cap_decision is None or cap_decision.get("outcome") != "accepted":
        exhausted[lane] = int(exhausted.get(lane) or 0) + 1
        outcomes = policy_state.setdefault("exhaustion_outcomes", {})
        if not isinstance(outcomes, dict):
            raise DurableExecutionError("policy state exhaustion outcomes are invalid")
        outcome = exhaustion_outcome or POLICY_EXHAUSTION_OUTCOME
        outcomes[outcome] = int(outcomes.get(outcome) or 0) + 1
        return
    used[lane] = int(used.get(lane) or 0) + 1
    accounting = policy_state.get("accounting")
    if not isinstance(accounting, dict):
        raise DurableExecutionError("policy state has no accounting")
    for dimension, values in (cap_decision.get("charges") or {}).items():
        counts = accounting.get(dimension)
        if not isinstance(counts, dict):
            raise DurableExecutionError(f"policy state has no {dimension} accounting")
        for charge in values:
            if not isinstance(charge, dict):
                raise DurableExecutionError("policy cap decision has invalid charge metadata")
            accounting_key = str(charge.get("accounting_key") or "")
            if not accounting_key:
                raise DurableExecutionError("policy cap decision has no accounting key")
            counts[accounting_key] = int(counts.get(accounting_key) or 0) + 1


def _durable_task_policy_assignment(task: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read the assignment from either a task spec or its LabTask envelope."""
    assignment = task.get("policy_assignment")
    if isinstance(assignment, dict):
        return assignment
    payload = task.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("policy_assignment"), dict):
        return dict(payload["policy_assignment"])
    return None


def _recompute_campaign_policy_state_from_durable_lanes(
    policy_state: dict[str, Any],
    *,
    lanes: list[LabLaneState],
    unresolved_tasks: list[dict[str, Any]],
    durable_tasks_by_id: Mapping[str, Any],
    pruned_lane_count: int,
) -> dict[str, Any]:
    """Rebuild mutable policy accounting and reject contradictory durable evidence."""
    if pruned_lane_count:
        raise DurableExecutionError(
            "policy-honest resume cannot verify pruned lane assignments"
        )
    rebuilt = copy.deepcopy(policy_state)
    lane_plan = rebuilt.get("lane_plan")
    planned = rebuilt.get("planned_lane_counts")
    if not isinstance(lane_plan, list) or not isinstance(planned, dict):
        raise DurableExecutionError("policy state has no durable lane allocation")
    dimensions = ("family", "recipe", "instrument", "timeframe", "indicator")
    rebuilt["assigned_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["used_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["exhausted_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["accounting"] = {dimension: {} for dimension in dimensions}
    rebuilt["exhaustion_outcomes"] = {}

    lanes_by_task: dict[str, LabLaneState] = {}
    seen_lane_indices: set[int] = set()
    for lane in sorted(lanes, key=lambda item: item.lane_index):
        if lane.lane_index in seen_lane_indices:
            raise DurableExecutionError(
                f"duplicate durable policy lane index: {lane.lane_index}"
            )
        seen_lane_indices.add(lane.lane_index)
        expected_lane = _policy_lane_for_index(rebuilt, lane.lane_index)
        assignment = lane.policy_assignment
        if not isinstance(assignment, dict) or not assignment:
            raise DurableExecutionError(
                f"durable policy lane has no assignment: {lane.lane_id}"
            )
        if (
            assignment.get("policy_lane") != expected_lane
            or assignment.get("policy_manifest_sha256")
            != rebuilt.get("policy_manifest_sha256")
        ):
            raise DurableExecutionError(
                f"durable policy lane assignment mismatch: {lane.lane_id}"
            )
        allocation = assignment.get("allocation")
        execution = rebuilt.get("execution")
        expected_allocation = {
            "lane_index": lane.lane_index,
            "planned_lane_count": planned.get(expected_lane),
            "algorithm": (execution or {}).get("allocation_algorithm"),
            "algorithm_version": (execution or {}).get("allocation_algorithm_version"),
            "lane_tie_break_order": (execution or {}).get("lane_tie_break_order"),
            "candidate_tie_break_order": (execution or {}).get(
                "candidate_tie_break_order"
            ),
        }
        if not isinstance(execution, dict) or allocation != expected_allocation:
            raise DurableExecutionError(
                f"durable policy lane allocation mismatch: {lane.lane_id}"
            )
        if assignment.get("negative_prior_runtime") != rebuilt.get(
            "negative_prior_runtime"
        ):
            raise DurableExecutionError(
                f"durable policy negative-prior binding mismatch: {lane.lane_id}"
            )
        lane_metadata = load_run_metadata(lane.run_dir)
        if lane_metadata.get("policy_assignment") != assignment:
            raise DurableExecutionError(
                f"durable lane metadata policy assignment mismatch: {lane.lane_id}"
            )

        outcome = str(assignment.get("policy_outcome_type") or "")
        cap_decision = assignment.get("cap_decision")
        if outcome == "policy_lane_selected":
            attributes = assignment.get("candidate_attributes")
            if not isinstance(attributes, dict) or not isinstance(cap_decision, dict):
                raise DurableExecutionError(
                    f"durable selected policy lane has incomplete accounting: {lane.lane_id}"
                )
            recomputed_cap_decision = _policy_cap_decision(rebuilt, attributes)
            if (
                recomputed_cap_decision.get("outcome") != "accepted"
                or cap_decision != recomputed_cap_decision
            ):
                raise DurableExecutionError(
                    f"durable policy cap decision mismatch: {lane.lane_id}"
                )
            _record_policy_assignment(
                rebuilt,
                lane=expected_lane,
                cap_decision=recomputed_cap_decision,
            )
        elif outcome in {POLICY_EXHAUSTION_OUTCOME, "policy_cap_exhausted"}:
            if cap_decision is not None or lane.task_ids or not lane.terminal:
                raise DurableExecutionError(
                    f"durable exhausted policy lane is contradictory: {lane.lane_id}"
                )
            _record_policy_assignment(
                rebuilt,
                lane=expected_lane,
                cap_decision=None,
                exhaustion_outcome=outcome,
            )
        else:
            raise DurableExecutionError(
                f"durable policy lane has unsupported outcome: {lane.lane_id}"
            )
        if int(rebuilt["assigned_lane_counts"].get(expected_lane) or 0) > int(
            planned.get(expected_lane) or 0
        ):
            raise DurableExecutionError(
                f"durable policy lane quota exceeded: {expected_lane}"
            )

        for task_id in lane.task_ids:
            task_spec = lane.task_specs.get(task_id)
            if not isinstance(task_spec, dict):
                durable_task = durable_tasks_by_id.get(task_id)
                if not isinstance(durable_task, dict):
                    raise DurableExecutionError(
                        f"durable policy lane has no task spec: {task_id}"
                    )
                task_spec = durable_task.get("payload")
                if (
                    not isinstance(task_spec, dict)
                    or str(task_spec.get("task_id") or "") != task_id
                    or str(task_spec.get("lane_id") or "") != lane.lane_id
                ):
                    raise DurableExecutionError(
                        f"durable policy lane has no task spec: {task_id}"
                    )
            if not isinstance(task_spec, dict):
                raise DurableExecutionError(
                    f"durable policy lane has no task spec: {task_id}"
                )
            if _durable_task_policy_assignment(task_spec) != assignment:
                raise DurableExecutionError(
                    f"durable task policy assignment mismatch: {task_id}"
                )
            if task_id in lanes_by_task:
                raise DurableExecutionError(f"duplicate durable policy task id: {task_id}")
            lanes_by_task[task_id] = lane

    for task in unresolved_tasks:
        task_id = str(task.get("task_id") or "")
        lane = lanes_by_task.get(task_id)
        if (
            lane is None
            or _durable_task_policy_assignment(task) != lane.policy_assignment
        ):
            raise DurableExecutionError(
                f"durable journal task policy assignment mismatch: {task_id or '<missing>'}"
            )

    mutable_fields = (
        "assigned_lane_counts",
        "used_lane_counts",
        "exhausted_lane_counts",
        "accounting",
        "exhaustion_outcomes",
    )
    if any(policy_state.get(field) != rebuilt.get(field) for field in mutable_fields):
        raise DurableExecutionError(
            "durable campaign policy counters do not match persisted lane assignments"
        )
    return rebuilt


def _response_json_payload(response: requests.Response) -> Any:
    if _orjson is not None:
        return _orjson.loads(response.content)
    return response.json()


class LabGatewayClient:
    def __init__(self, *, base_url: str, token: str | None = None, timeout_seconds: float = 30.0) -> None:
        self.base_url = str(base_url or DEFAULT_LAB_GATEWAY_URL).rstrip("/")
        self.token = str(token or "").strip() or None
        self.timeout_seconds = max(float(timeout_seconds), 1.0)
        self.session = requests.Session()

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def close(self) -> None:
        self.session.close()

    def health(self) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/healthz", timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = _response_json_payload(response)
        return payload if isinstance(payload, dict) else {}

    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/tasks",
            json={"tasks": tasks},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = _response_json_payload(response)
        return payload if isinstance(payload, dict) else {}

    def snapshot(self) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/snapshot",
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = _response_json_payload(response)
        return payload if isinstance(payload, dict) else {}

    def read_results(self, *, limit: int) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/results",
            params={"limit": max(int(limit), 1)},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = _response_json_payload(response)
        if not isinstance(payload, dict):
            return []
        results = payload.get("results")
        return [item for item in results if isinstance(item, dict)] if isinstance(results, list) else []

    def ack_results(self, lease_ids: list[str]) -> int:
        response = self.session.post(
            f"{self.base_url}/results/ack",
            json={"lease_ids": [str(lease_id) for lease_id in lease_ids if str(lease_id)]},
            headers=self._headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = _response_json_payload(response)
        if not isinstance(payload, dict):
            return 0
        return int(payload.get("acked") or 0)

    def drain_results(self, *, limit: int) -> list[dict[str, Any]]:
        results = self.read_results(limit=limit)
        self.ack_results([str(item.get("lease_id") or "") for item in results])
        return results


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_lane_lifecycle_event(
    lane: LabLaneState,
    event: str,
    *,
    phase: str,
    at: str | None = None,
    **extra: Any,
) -> None:
    entry: dict[str, Any] = {
        "at": at or _now_iso(),
        "event": str(event),
        "phase": str(phase),
        "current_phase": lane.current_phase,
    }
    entry.update({key: value for key, value in extra.items() if value is not None})
    lane.phase_lifecycle_events.append(entry)


def _set_lane_phase(lane: LabLaneState, phase: str, *, event: str = "phase_entered", **extra: Any) -> None:
    phase = str(phase)
    previous_phase = lane.current_phase
    lane.current_phase = phase
    now = _now_iso()
    if phase not in lane.phase_started_at:
        lane.phase_started_at[phase] = now
    if previous_phase != phase or extra:
        _append_lane_lifecycle_event(
            lane,
            event,
            phase=phase,
            at=now,
            previous_phase=previous_phase,
            **extra,
        )


def _record_lane_phase_tasks_started(
    lane: LabLaneState,
    *,
    phase: str,
    task_kind: str,
) -> None:
    phase = str(phase)
    now = _now_iso()
    if phase not in lane.phase_started_at:
        lane.phase_started_at[phase] = now
        _append_lane_lifecycle_event(
            lane,
            "phase_tasks_started",
            phase=phase,
            at=now,
            task_kind=task_kind,
            task_count=len(lane.phase_task_ids.get(phase) or []),
        )
    lane.phase_task_counts[phase] = len(lane.phase_task_ids.get(phase) or [])


def _refresh_lane_phase_result_counts(lane: LabLaneState, *, task_id: str) -> None:
    phase = _task_phase(lane, task_id)
    phase_task_ids = lane.phase_task_ids.get(phase) or []
    if not phase_task_ids:
        return
    completed_count = sum(1 for item in phase_task_ids if item in lane.completed_task_ids)
    failed_count = sum(1 for item in phase_task_ids if item in lane.failed_task_ids)
    lane.phase_task_counts[phase] = len(phase_task_ids)
    lane.phase_completed_task_counts[phase] = completed_count
    lane.phase_failed_task_counts[phase] = failed_count
    if completed_count + failed_count < len(phase_task_ids):
        return
    if phase in lane.phase_completed_at:
        return
    now = _now_iso()
    lane.phase_completed_at[phase] = now
    _append_lane_lifecycle_event(
        lane,
        "phase_tasks_completed",
        phase=phase,
        at=now,
        task_count=len(phase_task_ids),
        completed_task_count=completed_count,
        failed_task_count=failed_count,
        status="failed" if failed_count else "completed",
    )


def _is_exact_sha256(value: Any) -> bool:
    return bool(_EXACT_SHA256_RE.fullmatch(str(value or "").strip()))


def _safe_lineage_identity(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_LINEAGE_ID_RE.fullmatch(normalized):
        raise ValueError(
            f"Historical PlayHand requires a safe, explicit {field_name}."
        )
    return normalized


def _safe_campaign_id(value: Any, *, historical: bool) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_CAMPAIGN_ID_RE.fullmatch(normalized):
        requirement = "Historical PlayHand requires" if historical else "PlayHand campaign_id must be"
        raise ValueError(f"{requirement} a safe, explicit campaign_id.")
    return normalized


def _load_exact_historical_seed_plan(
    seed_plan_path: Path | str | None,
    *,
    expected_sha256: str,
) -> tuple[dict[str, Any], Path, str]:
    if seed_plan_path is None:
        raise ValueError("Historical PlayHand requires one explicit JSON seed_plan_path.")
    path = Path(seed_plan_path).expanduser().resolve()
    if path.suffix.lower() != ".json" or not path.is_file():
        raise ValueError("Historical PlayHand requires one existing JSON seed plan file.")
    try:
        raw_bytes = path.read_bytes()
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Historical PlayHand seed plan must be valid JSON.") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("recipes"), dict):
        raise ValueError("Historical PlayHand seed plan must be a JSON object with recipes.")
    actual_sha256 = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError("Historical PlayHand seed plan SHA-256 does not match expected_seed_plan_sha256.")
    return payload, path, actual_sha256


def _is_phase3_formal_runtime(runtime: PlayHandLabRuntimeConfig) -> bool:
    return str(runtime.formal_authority_kind or "").strip().lower() == "phase3"


def _phase3_authority_runtime_arguments(
    runtime: PlayHandLabRuntimeConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Re-resolve the alternate Phase 3 authority before a formal call runs."""
    if (
        runtime.phase3_authority_path is None
        or runtime.phase2_capsule_root is None
        or runtime.campaign_policy_manifest_path is None
    ):
        raise ValueError(
            "Phase 3 PlayHand requires authority, capsule, and campaign-policy paths."
        )
    from .phase3_authority import (
        Phase3AuthorityError,
        resolve_phase3_playhand_runtime_arguments,
    )

    try:
        arguments = resolve_phase3_playhand_runtime_arguments(
            authority_path=runtime.phase3_authority_path,
            phase2_capsule_root=runtime.phase2_capsule_root,
            policy_manifest_path=runtime.campaign_policy_manifest_path,
        )
        authority_path = Path(runtime.phase3_authority_path).expanduser().resolve(strict=True)
        authority = json.loads(authority_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, Phase3AuthorityError) as exc:
        raise ValueError(f"Phase 3 PlayHand authority validation failed: {exc}") from exc
    if not isinstance(authority, dict):
        raise ValueError("Phase 3 PlayHand authority must be a JSON object.")
    return arguments, authority


def _validate_phase3_runtime_contract(
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, str]:
    """Validate the Phase 3 authority without treating it as a Level C plan."""
    arguments, authority = _phase3_authority_runtime_arguments(runtime)
    if arguments.get("formal_authority_kind") != "phase3":
        raise ValueError("Phase 3 authority does not declare the Phase 3 formal runtime.")
    authority_id = str(authority.get("authority_id") or "").strip()
    if not _is_exact_sha256(authority_id):
        raise ValueError("Phase 3 authority has no exact authority_id.")
    if runtime.phase3_authority_id != authority_id:
        raise ValueError("Phase 3 authority identity differs from the runtime binding.")
    authority_path = Path(runtime.phase3_authority_path).expanduser().resolve(strict=True)
    authority_sha256 = _file_sha256(authority_path)
    if runtime.phase3_authority_sha256 != authority_sha256:
        raise ValueError("Phase 3 authority file digest differs from the runtime binding.")
    expected_fields = (
        "campaign_mode",
        "task_mode",
        "pipeline_mode",
        "target_runs",
        "campaign_id",
        "seed",
        "as_of_date",
        "expected_seed_plan_sha256",
        "lake_manifest_sha256",
        "source_snapshot_sha256",
        "universe_id",
        "universe_manifest_sha256",
        "worker_contract_hash",
        "operator_launch_worker_image",
        "current_atlas_generation",
        "current_atlas_run_sequence",
        "campaign_policy_manifest_sha256",
        "campaign_policy_source_file_sha256",
        "tasks_per_lane",
        "timeframe",
        "instrument",
        "instrument_pool",
        "instrument_pool_preset",
        "indicator",
        "profile_path",
        "min_indicators",
        "max_indicators",
        "lookback_months",
        "bar_limit",
        "max_reward_r",
        "sweep_budget",
        "max_sweep_permutations",
        "sweep_shard_size",
        "early_exit_mode",
        "coarse_halving_mode",
        "coarse_probe_budget",
        "validation_months",
        "validation_min_score",
        "scrutiny_months",
        "final_min_score",
        "screen_anchor_mode",
        "screen_anchor_envelope_months",
        "instrument_scout_size",
        "instrument_scout_max_selected",
        "deadline_seconds",
        "max_attempts",
        "strict_scoring",
        "retain_raw_lab_artifacts",
        "worker_contract_schema",
    )
    conflicts = [
        field_name
        for field_name in expected_fields
        if getattr(runtime, field_name) != arguments.get(field_name)
    ]
    if conflicts:
        raise ValueError(
            "Phase 3 PlayHand runtime conflicts with immutable authority: "
            + ", ".join(conflicts)
        )
    if runtime.seed_plan_path is None:
        raise ValueError("Phase 3 PlayHand requires the authority-bound seed-plan path.")
    if Path(runtime.seed_plan_path).expanduser().resolve(strict=True) != Path(
        str(arguments.get("seed_plan_path") or "")
    ).expanduser().resolve(strict=True):
        raise ValueError("Phase 3 PlayHand seed-plan path differs from immutable authority.")
    if runtime.level_c_protocol_id != authority_id or runtime.execution_plan_id != authority_id:
        raise ValueError("Phase 3 PlayHand durable authority identifiers differ from immutable authority.")
    if runtime.cutoff_key != "P3":
        raise ValueError("Phase 3 PlayHand requires cutoff_key=P3.")
    if runtime.research_generation_id != arguments.get("current_atlas_generation"):
        raise ValueError("Phase 3 PlayHand research generation differs from immutable authority.")
    return {
        "campaign_id": str(arguments["campaign_id"]),
        "research_generation_id": str(arguments["current_atlas_generation"]),
        "level_c_protocol_id": authority_id,
        "cutoff_key": "P3",
        "seed_plan_sha256": str(arguments["expected_seed_plan_sha256"]),
    }


def _validate_historical_runtime_contract(
    runtime: PlayHandLabRuntimeConfig,
) -> dict[str, str]:
    if _is_phase3_formal_runtime(runtime):
        return _validate_phase3_runtime_contract(runtime)
    if runtime.formal_authority_kind is not None:
        raise ValueError("Historical PlayHand formal_authority_kind must be level_c or phase3.")
    if not _is_exact_sha256(runtime.lake_manifest_sha256):
        raise ValueError("Historical PlayHand requires an exact lake_manifest_sha256.")
    if str(runtime.campaign_mode or "").strip().lower() != "finite":
        raise ValueError("Historical PlayHand requires campaign_mode=finite.")
    if str(runtime.task_mode or "").strip().lower() != "deep_replay":
        raise ValueError("Historical PlayHand requires task_mode=deep_replay.")
    if str(runtime.pipeline_mode or "").strip().lower() != PLAY_HAND_LAB_PLAY_HAND_PIPELINE:
        raise ValueError("Historical PlayHand requires pipeline_mode=play_hand.")
    if (
        isinstance(runtime.target_runs, bool)
        or not isinstance(runtime.target_runs, int)
        or runtime.target_runs <= 0
    ):
        raise ValueError("Historical PlayHand requires a positive, explicit target_runs count.")
    if runtime.strict_scoring is not True:
        raise ValueError("Historical PlayHand requires strict_scoring=True.")
    if runtime.seed is None:
        raise ValueError("Historical PlayHand requires an explicit seed.")
    if not _is_exact_sha256(runtime.worker_contract_hash):
        raise ValueError("Historical PlayHand requires an explicit exact worker_contract_hash.")
    if runtime.indicator:
        raise ValueError("Historical PlayHand derives indicators exclusively from its seed plan.")
    if not _is_exact_sha256(runtime.expected_seed_plan_sha256):
        raise ValueError("Historical PlayHand requires an exact expected_seed_plan_sha256.")
    if not _is_exact_sha256(runtime.source_snapshot_sha256):
        raise ValueError("Historical PlayHand requires an exact source_snapshot_sha256.")
    _safe_lineage_identity(runtime.universe_id, field_name="universe_id")
    if not _is_exact_sha256(runtime.universe_manifest_sha256):
        raise ValueError("Historical PlayHand requires an exact universe_manifest_sha256.")
    if not _is_exact_sha256(runtime.execution_plan_id) or runtime.execution_plan_path is None:
        raise ValueError("Historical PlayHand requires one authoritative execution plan.")

    campaign_id = _safe_campaign_id(runtime.campaign_id, historical=True)
    research_generation_id = _safe_lineage_identity(
        runtime.research_generation_id,
        field_name="research_generation_id",
    )
    level_c_protocol_id = str(runtime.level_c_protocol_id or "").strip()
    if not _is_exact_sha256(level_c_protocol_id):
        raise ValueError(
            "Historical PlayHand requires level_c_protocol_id to be an exact sha256: identity."
        )
    cutoff_key = str(runtime.cutoff_key or "").strip()
    if cutoff_key not in {"A", "B", "C", "D"}:
        raise ValueError("Historical PlayHand requires cutoff_key to be one of A, B, C, or D.")
    seed_plan, _seed_plan_path, seed_plan_sha256 = _load_exact_historical_seed_plan(
        runtime.seed_plan_path,
        expected_sha256=str(runtime.expected_seed_plan_sha256).strip(),
    )
    campaign_policy = validate_seed_plan_campaign_policy(seed_plan)
    if campaign_policy is not None:
        try:
            _policy_negative_prior_runtime(campaign_policy, runtime=runtime)
        except DurableExecutionError as exc:
            raise ValueError(str(exc)) from exc
    return {
        "campaign_id": campaign_id,
        "research_generation_id": research_generation_id,
        "level_c_protocol_id": level_c_protocol_id,
        "cutoff_key": cutoff_key,
        "seed_plan_sha256": seed_plan_sha256,
    }


def _historical_lineage_payload(runtime: PlayHandLabRuntimeConfig) -> dict[str, str] | None:
    if not runtime.as_of_date:
        return None
    return {
        "campaign_id": str(runtime.campaign_id),
        "formal_authority_kind": str(runtime.formal_authority_kind or "level_c"),
        "phase3_authority_id": str(runtime.phase3_authority_id or ""),
        "phase3_authority_sha256": str(runtime.phase3_authority_sha256 or ""),
        "research_generation_id": str(runtime.research_generation_id),
        "level_c_protocol_id": str(runtime.level_c_protocol_id),
        "cutoff_key": str(runtime.cutoff_key),
        "as_of_date": str(runtime.as_of_date),
        "lake_manifest_sha256": str(runtime.lake_manifest_sha256),
        "expected_seed_plan_sha256": str(runtime.expected_seed_plan_sha256),
        "source_snapshot_sha256": str(runtime.source_snapshot_sha256),
        "universe_id": str(runtime.universe_id),
        "universe_manifest_sha256": str(runtime.universe_manifest_sha256),
        "execution_plan_id": str(runtime.execution_plan_id),
    }


def _require_historical_task_evidence(
    *,
    runtime: PlayHandLabRuntimeConfig,
    analysis_window_start: str | None,
    analysis_window_end: str | None,
    evidence_plan: Any,
) -> None:
    if not runtime.as_of_date:
        return
    if not analysis_window_start or not analysis_window_end:
        raise ValueError("Historical PlayHand tasks require explicit analysis window bounds.")
    if analysis_window_end != runtime.as_of_date:
        raise ValueError("Historical PlayHand task analysis_window_end must equal as_of_date.")
    if evidence_plan is None:
        raise ValueError("Historical PlayHand tasks require an evidence plan.")
    payload = evidence_plan.model_dump(mode="json")
    if payload.get("evidence_role") != "training":
        raise ValueError("Historical PlayHand tasks require selection-consuming training evidence.")
    if payload.get("selection_data_end") != runtime.as_of_date:
        raise ValueError("Historical PlayHand evidence selection_data_end must equal as_of_date.")
    if payload.get("data_availability_cutoff") != runtime.as_of_date:
        raise ValueError(
            "Historical PlayHand evidence data_availability_cutoff must equal as_of_date."
        )


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
    log_mode = str(runtime.log_mode or DEFAULT_LAB_LOG_MODE).strip().lower()
    if log_mode not in {"barrier", "stream", "quiet"}:
        raise ValueError("--log-mode must be barrier, stream, or quiet")
    screen_anchor_mode = str(runtime.screen_anchor_mode or DEFAULT_LAB_SCREEN_ANCHOR_MODE).strip().lower()
    if screen_anchor_mode not in {"now", "random"}:
        raise ValueError("--screen-anchor-mode must be now or random")
    validation_months = max(int(runtime.validation_months), 1)
    scrutiny_months = max(int(runtime.scrutiny_months), 1)
    screen_anchor_envelope_months = max(
        int(runtime.screen_anchor_envelope_months),
        lookback_months,
    )
    as_of_date = str(runtime.as_of_date or "").strip() or None
    historical_contract: dict[str, str] | None = None
    if as_of_date:
        parsed_as_of = datetime.fromisoformat(as_of_date.replace("Z", "+00:00"))
        if parsed_as_of.tzinfo is None:
            parsed_as_of = parsed_as_of.replace(tzinfo=timezone.utc)
        as_of_date = _utc_iso(parsed_as_of)
        historical_contract = _validate_historical_runtime_contract(runtime)
    campaign_id = (
        historical_contract["campaign_id"]
        if historical_contract
        else (
            _safe_campaign_id(runtime.campaign_id, historical=False)
            if runtime.campaign_id is not None and str(runtime.campaign_id).strip()
            else None
        )
    )
    result_batch_size = max(int(runtime.result_batch_size), 1)
    max_results_per_cycle = max(int(runtime.max_results_per_cycle), result_batch_size)
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
        seed_plan_path=Path(runtime.seed_plan_path).expanduser().resolve()
        if runtime.seed_plan_path
        else None,
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
        validation_months=validation_months,
        validation_min_score=float(runtime.validation_min_score),
        scrutiny_months=scrutiny_months,
        final_min_score=float(runtime.final_min_score),
        screen_anchor_mode=screen_anchor_mode,  # type: ignore[arg-type]
        screen_anchor_envelope_months=screen_anchor_envelope_months,
        as_of_date=as_of_date,
        campaign_id=campaign_id,
        lake_manifest_sha256=(
            str(runtime.lake_manifest_sha256).strip()
            if runtime.lake_manifest_sha256
            else None
        ),
        research_generation_id=(
            historical_contract["research_generation_id"]
            if historical_contract
            else (str(runtime.research_generation_id).strip() if runtime.research_generation_id else None)
        ),
        level_c_protocol_id=(
            historical_contract["level_c_protocol_id"]
            if historical_contract
            else (str(runtime.level_c_protocol_id).strip() if runtime.level_c_protocol_id else None)
        ),
        cutoff_key=(
            historical_contract["cutoff_key"]
            if historical_contract
            else (str(runtime.cutoff_key).strip() if runtime.cutoff_key else None)
        ),
        expected_seed_plan_sha256=(
            historical_contract["seed_plan_sha256"]
            if historical_contract
            else (
                str(runtime.expected_seed_plan_sha256).strip()
                if runtime.expected_seed_plan_sha256
                else None
            )
        ),
        current_atlas_generation=(
            str(runtime.current_atlas_generation).strip()
            if runtime.current_atlas_generation
            else None
        ),
        current_atlas_run_sequence=(
            int(runtime.current_atlas_run_sequence)
            if runtime.current_atlas_run_sequence is not None
            and not isinstance(runtime.current_atlas_run_sequence, bool)
            else runtime.current_atlas_run_sequence
        ),
        formal_authority_kind=(
            str(runtime.formal_authority_kind).strip().lower()
            if runtime.formal_authority_kind
            else None
        ),
        phase3_authority_path=(
            Path(runtime.phase3_authority_path).expanduser().resolve()
            if runtime.phase3_authority_path
            else None
        ),
        phase2_capsule_root=(
            Path(runtime.phase2_capsule_root).expanduser().resolve()
            if runtime.phase2_capsule_root
            else None
        ),
        campaign_policy_manifest_path=(
            Path(runtime.campaign_policy_manifest_path).expanduser().resolve()
            if runtime.campaign_policy_manifest_path
            else None
        ),
        phase3_authority_id=(
            str(runtime.phase3_authority_id).strip()
            if runtime.phase3_authority_id
            else None
        ),
        phase3_authority_sha256=(
            str(runtime.phase3_authority_sha256).strip()
            if runtime.phase3_authority_sha256
            else None
        ),
        campaign_policy_manifest_sha256=(
            str(runtime.campaign_policy_manifest_sha256).strip()
            if runtime.campaign_policy_manifest_sha256
            else None
        ),
        campaign_policy_source_file_sha256=(
            str(runtime.campaign_policy_source_file_sha256).strip()
            if runtime.campaign_policy_source_file_sha256
            else None
        ),
        operator_launch_worker_image=(
            str(runtime.operator_launch_worker_image).strip()
            if runtime.operator_launch_worker_image
            else None
        ),
        source_snapshot_sha256=(
            str(runtime.source_snapshot_sha256).strip()
            if runtime.source_snapshot_sha256
            else None
        ),
        universe_id=str(runtime.universe_id).strip() if runtime.universe_id else None,
        universe_manifest_sha256=(
            str(runtime.universe_manifest_sha256).strip()
            if runtime.universe_manifest_sha256
            else None
        ),
        execution_plan_path=(
            Path(runtime.execution_plan_path).expanduser().resolve()
            if runtime.execution_plan_path
            else None
        ),
        execution_plan_id=(
            str(runtime.execution_plan_id).strip() if runtime.execution_plan_id else None
        ),
        resume=bool(runtime.resume),
        instrument_scout_size=max(int(runtime.instrument_scout_size), 1),
        instrument_scout_max_selected=max(int(runtime.instrument_scout_max_selected), 1),
        fake_work_seconds=max(float(runtime.fake_work_seconds), 0.0),
        deadline_seconds=max(float(runtime.deadline_seconds), 1.0),
        max_attempts=max(int(runtime.max_attempts), 1),
        poll_interval_seconds=max(float(runtime.poll_interval_seconds), 0.1),
        max_wait_seconds=max(float(runtime.max_wait_seconds), 1.0),
        result_batch_size=result_batch_size,
        max_results_per_cycle=max_results_per_cycle,
        max_drain_seconds=max(float(runtime.max_drain_seconds), 0.0),
        result_read_failure_limit=max(int(runtime.result_read_failure_limit), 1),
        enqueue_failure_limit=max(int(runtime.enqueue_failure_limit), 1),
        enqueue_retry_base_seconds=max(float(runtime.enqueue_retry_base_seconds), 0.0),
        terminal_lane_retention=max(
            int(runtime.terminal_lane_retention),
            int(target_runs or 0) if historical_contract else 0,
        ),
        dry_run=bool(runtime.dry_run),
        strict_scoring=bool(runtime.strict_scoring),
        retain_raw_lab_artifacts=bool(runtime.retain_raw_lab_artifacts),
        json_output=bool(runtime.json_output),
        log_mode=log_mode,  # type: ignore[arg-type]
        barrier_interval_seconds=max(float(runtime.barrier_interval_seconds), 1.0),
        barrier_lane_limit=max(int(runtime.barrier_lane_limit), 1),
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


_SENSITIVE_EVENT_KEY_PARTS = ("authorization", "password", "secret", "token")


def _redact_sensitive_event_payload(value: Any, *, key: str | None = None) -> Any:
    if key and any(part in key.lower() for part in _SENSITIVE_EVENT_KEY_PARTS):
        return "[redacted]"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(child_key): _redact_sensitive_event_payload(child_value, key=str(child_key))
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_sensitive_event_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_sensitive_event_payload(item) for item in value]
    return value


def _runtime_event_payload(runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    payload = _redact_sensitive_event_payload(asdict(runtime))
    return payload if isinstance(payload, dict) else {}


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
        from fuzzfolio_core.models import common as common_module
        from fuzzfolio_core.models import indicator as indicator_module
        from fuzzfolio_core.models import scoringprofile as scoringprofile_module
        from fuzzfolio_core.models.scoringprofile import ScoringProfile, StoredScoringProfile
    except Exception as exc:
        raise RuntimeError(f"Could not load FuzzFolio profile models from {shared_python}: {exc}") from exc
    loaded_modules = {
        "common.py": common_module,
        "indicator.py": indicator_module,
        "scoringprofile.py": scoringprofile_module,
    }
    for relative in PROFILE_MODEL_SOURCE_FILES:
        expected_path = (root / relative).resolve(strict=False)
        module = loaded_modules[expected_path.name]
        observed_path = Path(str(getattr(module, "__file__", ""))).resolve(strict=False)
        if observed_path != expected_path:
            raise RuntimeError(
                f"Loaded FuzzFolio profile model source mismatch: expected {expected_path}, "
                f"observed {observed_path}"
            )
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


def _historical_campaign_lineage(runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    return {
        "campaign_id": runtime.campaign_id,
        "formal_authority_kind": runtime.formal_authority_kind or "level_c",
        "phase3_authority_id": runtime.phase3_authority_id or "",
        "phase3_authority_sha256": runtime.phase3_authority_sha256 or "",
        "as_of_date": runtime.as_of_date,
        "lake_manifest_sha256": runtime.lake_manifest_sha256,
        "research_generation_id": runtime.research_generation_id,
        "level_c_protocol_id": runtime.level_c_protocol_id,
        "cutoff_key": runtime.cutoff_key,
        "expected_seed_plan_sha256": runtime.expected_seed_plan_sha256,
        "source_snapshot_sha256": runtime.source_snapshot_sha256,
        "universe_id": runtime.universe_id,
        "universe_manifest_sha256": runtime.universe_manifest_sha256,
        "execution_plan_id": runtime.execution_plan_id,
        "formal_historical_level_c": True,
    }


def _reject_existing_historical_campaign_path(
    campaign_dir: Path,
    *,
    runtime: PlayHandLabRuntimeConfig,
) -> None:
    """Fail closed unless an existing formal campaign has exact durable resume state."""
    if not campaign_dir.exists():
        return
    try:
        metadata = load_run_metadata(campaign_dir)
    except Exception as exc:
        raise ValueError(
            "Historical PlayHand campaign path already exists without readable lineage metadata."
        ) from exc
    if not isinstance(metadata, dict):
        raise ValueError(
            "Historical PlayHand campaign path already exists without readable lineage metadata."
        )
    expected = _historical_campaign_lineage(runtime)
    observed = dict(metadata)
    observed["campaign_id"] = observed.get("campaign_id") or observed.get("run_id")
    conflicts = [
        field_name
        for field_name, expected_value in expected.items()
        if observed.get(field_name) != expected_value
    ]
    if conflicts:
        raise ValueError(
            "Historical PlayHand campaign path contains conflicting historical lineage: "
            + ", ".join(conflicts)
            + "."
        )
    if runtime.resume:
        state_path = campaign_dir / "play-hand-lab-state.json"
        journal_path = campaign_dir / "play-hand-lab-execution-journal.json"
        if not state_path.is_file() or not journal_path.is_file():
            raise ValueError(
                "Historical PlayHand resume requires both durable state and execution journal."
            )
        return
    raise ValueError(
        "Historical PlayHand campaign path already exists; pass --resume to continue it."
    )


def _lane_run_id(lane_index: int, *, campaign_id: str | None = None) -> str:
    if campaign_id:
        return f"{campaign_id}-lane-{lane_index:05d}"
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
            "campaign_id": campaign_ctx.run_id,
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
            "validation_months": runtime.validation_months,
            "validation_min_score": runtime.validation_min_score,
            "scrutiny_months": runtime.scrutiny_months,
            "final_min_score": runtime.final_min_score,
            "screen_anchor_mode": runtime.screen_anchor_mode,
            "screen_anchor_envelope_months": runtime.screen_anchor_envelope_months,
            "as_of_date": runtime.as_of_date,
            "formal_authority_kind": runtime.formal_authority_kind or "level_c",
            "phase3_authority_id": runtime.phase3_authority_id or "",
            "phase3_authority_sha256": runtime.phase3_authority_sha256 or "",
            "operator_launch_worker_image": runtime.operator_launch_worker_image,
            "lake_manifest_sha256": runtime.lake_manifest_sha256,
            "research_generation_id": runtime.research_generation_id,
            "level_c_protocol_id": runtime.level_c_protocol_id,
            "cutoff_key": runtime.cutoff_key,
            "expected_seed_plan_sha256": runtime.expected_seed_plan_sha256,
            "source_snapshot_sha256": runtime.source_snapshot_sha256,
            "universe_id": runtime.universe_id,
            "universe_manifest_sha256": runtime.universe_manifest_sha256,
            "execution_plan_id": runtime.execution_plan_id,
            "execution_plan_path": str(runtime.execution_plan_path) if runtime.execution_plan_path else None,
            "play_hand_seed_plan_path": (
                str(runtime.seed_plan_path.resolve())
                if runtime.as_of_date and runtime.seed_plan_path
                else metadata.get("play_hand_seed_plan_path")
            ),
            "play_hand_seed_plan_sha256": (
                runtime.expected_seed_plan_sha256
                if runtime.as_of_date
                else metadata.get("play_hand_seed_plan_sha256")
            ),
            "formal_historical_level_c": bool(runtime.as_of_date),
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
            "campaign_id": campaign_ctx.run_id,
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
            "validation_months": runtime.validation_months,
            "validation_min_score": runtime.validation_min_score,
            "scrutiny_months": runtime.scrutiny_months,
            "final_min_score": runtime.final_min_score,
            "screen_anchor_mode": lane.screen_anchor_mode,
            "screen_analysis_window_start": lane.screen_analysis_window_start,
            "screen_analysis_window_end": lane.screen_analysis_window_end,
            "screen_anchor_offset_days": lane.screen_anchor_offset_days,
            "screen_anchor_envelope_months": runtime.screen_anchor_envelope_months,
            "as_of_date": runtime.as_of_date,
            "lake_manifest_sha256": runtime.lake_manifest_sha256,
            "research_generation_id": runtime.research_generation_id,
            "level_c_protocol_id": runtime.level_c_protocol_id,
            "cutoff_key": runtime.cutoff_key,
            "expected_seed_plan_sha256": runtime.expected_seed_plan_sha256,
            "source_snapshot_sha256": runtime.source_snapshot_sha256,
            "universe_id": runtime.universe_id,
            "universe_manifest_sha256": runtime.universe_manifest_sha256,
            "execution_plan_id": runtime.execution_plan_id,
            "play_hand_seed_plan_path": (
                str(runtime.seed_plan_path.resolve())
                if runtime.as_of_date and runtime.seed_plan_path
                else metadata.get("play_hand_seed_plan_path")
            ),
            "play_hand_seed_plan_sha256": (
                runtime.expected_seed_plan_sha256
                if runtime.as_of_date
                else metadata.get("play_hand_seed_plan_sha256")
            ),
            "formal_historical_level_c": bool(runtime.as_of_date),
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
            "phase_started_at": dict(lane.phase_started_at),
            "phase_completed_at": dict(lane.phase_completed_at),
            "phase_task_counts": dict(lane.phase_task_counts),
            "phase_completed_task_counts": dict(lane.phase_completed_task_counts),
            "phase_failed_task_counts": dict(lane.phase_failed_task_counts),
            "phase_lifecycle_events": list(lane.phase_lifecycle_events),
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
    emit_events: bool = True,
) -> tuple[list[SeedIndicator], dict[str, Any] | None, Path | None]:
    if runtime.as_of_date:
        seed_plan, seed_plan_path, _seed_plan_sha256 = _load_exact_historical_seed_plan(
            runtime.seed_plan_path,
            expected_sha256=str(runtime.expected_seed_plan_sha256 or ""),
        )
    else:
        seed_plan, seed_plan_path = _load_play_hand_seed_plan(config, runtime.seed_plan_path)
    if isinstance(seed_plan, dict):
        # v2 plans carry their policy digest in-band. The shared validator is
        # intentionally the sole schema/digest authority; v1 returns None.
        validate_seed_plan_campaign_policy(seed_plan)
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
        if invalid and emit_events:
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
                "PlayHand indicators are not scaffoldable by the current FuzzFolio CLI: "
                + ", ".join(invalid[:10])
            )
        return valid, invalid

    if runtime.as_of_date:
        seed_plan_candidates = _seed_plan_indicator_candidates(config, seed_plan)
        if not seed_plan_candidates:
            raise RuntimeError(
                "Historical PlayHand seed plan has no usable indicator candidates."
            )
        valid, _invalid = scaffoldable_pool(
            seed_plan_candidates,
            source="historical_seed_plan",
            require_all=True,
        )
        if len(valid) < runtime.min_indicators:
            raise RuntimeError(
                "Historical PlayHand seed plan is smaller than --min-indicators after validation: "
                f"{len(valid)} < {runtime.min_indicators}."
            )
        return valid, seed_plan, seed_plan_path

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
        if emit_events:
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
        if emit_events:
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
    policy_lane: str | None = None,
    negative_prior_runtime: dict[str, Any] | None = None,
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
    campaign_policy = (
        validate_seed_plan_campaign_policy(seed_plan)
        if isinstance(seed_plan, dict)
        else None
    )
    deal_seed_plan = seed_plan
    if runtime.as_of_date and isinstance(seed_plan, dict) and campaign_policy is None:
        # Formal runs use the frozen plan's guided distribution only. Atlas's
        # legacy exploration fraction would otherwise select a generic fallback.
        # Policy-honest v2 plans carry their own deterministic lane assignment.
        sampling_policy = seed_plan.get("sampling_policy")
        deal_seed_plan = {
            **seed_plan,
            "sampling_policy": {
                **(sampling_policy if isinstance(sampling_policy, dict) else {}),
                "guided_prior_fraction": 1.0,
            },
        }
    indicator_deal = deal_seed_plan_indicators(
        shuffled,
        target_count=dealt_count,
        seed_plan=deal_seed_plan,
        rng=rng,
        seed_plan_candidates=seed_plan_candidates,
        policy_lane=policy_lane,
        negative_prior_runtime=negative_prior_runtime,
    )
    dealt_entries = list(indicator_deal.get("indicators") or [])
    if runtime.as_of_date and campaign_policy is None:
        selected_slots = [str(slot) for slot in indicator_deal.get("selected_slots") or []]
        if (
            indicator_deal.get("source") != "play_hand_seed_plan"
            or any(slot.startswith("role_balanced") for slot in selected_slots)
        ):
            raise RuntimeError(
                "Historical PlayHand rejects fallback indicator deals; the seed plan must produce a guided deal."
            )
    if not dealt_entries:
        if runtime.as_of_date:
            raise RuntimeError("Historical PlayHand seed plan produced an empty indicator deal.")
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
        "primary_instrument": instrument_deal.get("primary_instrument"),
        "timeframe": str(runtime.timeframe).strip().upper(),
    }


def _select_policy_lane_deal(
    *,
    config: AppConfig,
    runtime: PlayHandLabRuntimeConfig,
    seed_indicators: list[SeedIndicator],
    seed_plan: dict[str, Any],
    lane_index: int,
    policy_state: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Choose a cap-compliant candidate from one preassigned v2 lane."""
    policy_lane = _policy_lane_for_index(policy_state, lane_index)
    candidate_rows: list[tuple[tuple[str, ...], dict[str, Any], dict[str, Any], int]] = []
    candidate_attempts: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    negative_prior_runtime = policy_state.get("negative_prior_runtime")
    if not isinstance(negative_prior_runtime, dict):
        raise DurableExecutionError("policy state has no negative-prior runtime binding")
    for attempt in range(POLICY_CANDIDATE_FALLBACK_ATTEMPTS):
        candidate_rng = random.Random(
            f"play-hand-lab-policy:{runtime.seed}:{lane_index}:candidate:{attempt}"
        )
        deal = _deal_lane(
            config=config,
            runtime=runtime,
            seed_indicators=seed_indicators,
            seed_plan=seed_plan,
            rng=candidate_rng,
            policy_lane=policy_lane,
            negative_prior_runtime=negative_prior_runtime,
        )
        indicator_deal = deal.get("indicator_deal") if isinstance(deal, dict) else None
        indicator_deal = indicator_deal if isinstance(indicator_deal, dict) else {}
        if indicator_deal.get("policy_outcome_type") == POLICY_EXHAUSTION_OUTCOME:
            candidate_attempts.append(
                {
                    "attempt": attempt,
                    "outcome": POLICY_EXHAUSTION_OUTCOME,
                    "negative_prior_decisions": copy.deepcopy(
                        indicator_deal.get("negative_prior_decisions") or []
                    ),
                }
            )
            continue
        attributes = _policy_candidate_attributes(deal, policy_state=policy_state)
        if attributes is None:
            candidate_attempts.append(
                {
                    "attempt": attempt,
                    "outcome": str(
                        (deal.get("indicator_deal") or {}).get("policy_outcome_type")
                        or "policy_candidate_attribute_missing"
                    ),
                    "negative_prior_decisions": copy.deepcopy(
                        (deal.get("indicator_deal") or {}).get("negative_prior_decisions")
                        or []
                    ),
                }
            )
            continue
        candidate_id = str(attributes["candidate_id"])
        if candidate_id in seen_candidates:
            continue
        seen_candidates.add(candidate_id)
        candidate_rows.append(
            (
                _policy_candidate_sort_key(attributes, policy_state=policy_state),
                deal,
                attributes,
                attempt,
            )
        )
    candidate_rows.sort(key=lambda row: row[0])
    for sort_key, deal, attributes, attempt in candidate_rows:
        cap_decision = _policy_cap_decision(policy_state, attributes)
        candidate_attempts.append(
            {
                "attempt": attempt,
                "candidate_id": attributes["candidate_id"],
                "candidate_tie_break_key": list(sort_key),
                "cap_decision": copy.deepcopy(cap_decision),
                "negative_prior_decisions": copy.deepcopy(
                    (deal.get("indicator_deal") or {}).get("negative_prior_decisions")
                    or []
                ),
            }
        )
        if cap_decision["outcome"] == "accepted":
            execution = policy_state.get("execution") or {}
            return deal, {
                "policy_lane": policy_lane,
                "policy_manifest_sha256": policy_state.get("policy_manifest_sha256"),
                "policy_outcome_type": "policy_lane_selected",
                "allocation": {
                    "lane_index": lane_index,
                    "planned_lane_count": (policy_state.get("planned_lane_counts") or {}).get(
                        policy_lane
                    ),
                    "algorithm": execution.get("allocation_algorithm"),
                    "algorithm_version": execution.get("allocation_algorithm_version"),
                    "lane_tie_break_order": execution.get("lane_tie_break_order"),
                    "candidate_tie_break_order": execution.get("candidate_tie_break_order"),
                },
                "candidate_attributes": attributes,
                "cap_decision": cap_decision,
                "candidate_fallback_decisions": candidate_attempts,
                "negative_prior_decisions": copy.deepcopy(
                    (deal.get("indicator_deal") or {}).get("negative_prior_decisions") or []
                ),
                "negative_prior_runtime": copy.deepcopy(negative_prior_runtime),
            }
    exhaustion_outcome = (
        "policy_cap_exhausted" if candidate_rows else POLICY_EXHAUSTION_OUTCOME
    )
    execution = policy_state.get("execution") or {}
    return None, {
        "policy_lane": policy_lane,
        "policy_manifest_sha256": policy_state.get("policy_manifest_sha256"),
        "policy_outcome_type": exhaustion_outcome,
        "allocation": {
            "lane_index": lane_index,
            "planned_lane_count": (policy_state.get("planned_lane_counts") or {}).get(
                policy_lane
            ),
            "algorithm": execution.get("allocation_algorithm"),
            "algorithm_version": execution.get("allocation_algorithm_version"),
            "lane_tie_break_order": execution.get("lane_tie_break_order"),
            "candidate_tie_break_order": execution.get("candidate_tie_break_order"),
        },
        "candidate_fallback_decisions": candidate_attempts,
        "negative_prior_decisions": [
            decision
            for candidate in candidate_attempts
            for decision in candidate.get("negative_prior_decisions") or []
        ],
        "negative_prior_runtime": copy.deepcopy(negative_prior_runtime),
    }


def _indicator_id_from_deal_entry(entry: Any) -> str | None:
    if isinstance(entry, SeedIndicator):
        indicator_id = entry.id
    elif isinstance(entry, dict):
        indicator_id = entry.get("id") or entry.get("indicator_id")
    else:
        indicator_id = entry
    indicator_id = str(indicator_id or "").strip().upper()
    return indicator_id or None


def _indicator_deal_metadata(indicator_deal: dict[str, Any] | None) -> dict[str, Any]:
    deal = indicator_deal if isinstance(indicator_deal, dict) else {}
    indicator_ids = [
        indicator_id
        for item in deal.get("indicators") or []
        if (indicator_id := _indicator_id_from_deal_entry(item))
    ]
    payload = {
        "indicator_ids": indicator_ids,
        "source": deal.get("source"),
        "reason": deal.get("reason"),
        "recipe": deal.get("recipe"),
        "recipe_source": deal.get("recipe_source"),
        "recipe_confidence": deal.get("recipe_confidence"),
        "guided_recipe_source_mix_expected": deal.get("guided_recipe_source_mix_expected"),
        "guided_recipe_source_bucket": deal.get("guided_recipe_source_bucket"),
        "guided_recipe_source_bucket_matched": deal.get("guided_recipe_source_bucket_matched"),
        "guided_recipe_source_bucket_fallback": deal.get("guided_recipe_source_bucket_fallback"),
        "pair": deal.get("pair"),
        "family_policy": deal.get("family_policy"),
        "policy_target_count": deal.get("policy_target_count"),
        "selected_slots": deal.get("selected_slots"),
        "policy_lane": deal.get("policy_lane"),
        "policy_manifest_sha256": deal.get("policy_manifest_sha256"),
        "policy_outcome_type": deal.get("policy_outcome_type"),
        "negative_prior_decisions": deal.get("negative_prior_decisions"),
    }
    return {
        "indicator_deal": payload,
        "dealt_indicator_source": payload["source"],
        "dealt_indicator_source_reason": payload["reason"],
        "dealt_recipe": payload["recipe"],
        "dealt_recipe_source": payload["recipe_source"],
        "dealt_recipe_confidence": payload["recipe_confidence"],
        "guided_recipe_source_mix_expected": payload["guided_recipe_source_mix_expected"],
        "guided_recipe_source_bucket": payload["guided_recipe_source_bucket"],
        "guided_recipe_source_bucket_matched": payload["guided_recipe_source_bucket_matched"],
        "guided_recipe_source_bucket_fallback": payload["guided_recipe_source_bucket_fallback"],
        "dealt_recipe_pair": payload["pair"],
        "dealt_pair_family_policy": payload["family_policy"],
        "dealt_policy_target_count": payload["policy_target_count"],
        "dealt_recipe_slots": payload["selected_slots"],
        "policy_lane": payload["policy_lane"],
        "policy_manifest_sha256": payload["policy_manifest_sha256"],
        "policy_outcome_type": payload["policy_outcome_type"],
        "negative_prior_decisions": payload["negative_prior_decisions"],
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
        timing_hint_changes = apply_seed_pair_timing_hints(
            profile_payload,
            deal["indicator_deal"].get("pair"),
        )
        default_changes = apply_play_hand_profile_defaults(profile_payload, rng=rng)
        if metadata_changes or timeframe_changes or template_changes or timing_hint_changes or default_changes:
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
            timing_hint_changes=timing_hint_changes,
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


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _subtract_calendar_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 - max(int(months), 0)
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _sample_lane_screen_anchor(lane: LabLaneState, runtime: PlayHandLabRuntimeConfig) -> None:
    lane.screen_anchor_mode = str(runtime.screen_anchor_mode or DEFAULT_LAB_SCREEN_ANCHOR_MODE)
    lane.screen_analysis_window_start = None
    lane.screen_analysis_window_end = None
    lane.screen_anchor_offset_days = None
    if runtime.as_of_date:
        end = datetime.fromisoformat(runtime.as_of_date.replace("Z", "+00:00"))
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        end = end.astimezone(timezone.utc)
        start = _subtract_calendar_months(end, int(runtime.lookback_months))
        lane.screen_anchor_mode = "fixed_as_of"
        lane.screen_analysis_window_start = _utc_iso(start)
        lane.screen_analysis_window_end = _utc_iso(end)
        return
    if lane.screen_anchor_mode != "random":
        return

    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    anchor = sample_screen_anchor(
        mode=lane.screen_anchor_mode,
        screen_months=int(runtime.lookback_months),
        max_offset_months=max(int(runtime.screen_anchor_envelope_months) - int(runtime.lookback_months), 0),
        seed=f"{runtime.seed}:{lane.run_id}:{lane.lane_index}",
        now=now,
    )
    offset_days = int(anchor.get("offset_days") or 0)
    as_of_date = str(anchor.get("as_of_date") or "").strip()
    end = (
        datetime.fromisoformat(as_of_date).replace(tzinfo=timezone.utc)
        if as_of_date
        else now
    )
    start = _subtract_calendar_months(end, int(runtime.lookback_months))
    lane.screen_anchor_offset_days = offset_days
    lane.screen_analysis_window_start = _utc_iso(start)
    lane.screen_analysis_window_end = _utc_iso(end)


def _lane_screen_window(lane: LabLaneState) -> tuple[str | None, str | None]:
    return lane.screen_analysis_window_start, lane.screen_analysis_window_end


def _runtime_as_of_window(
    runtime: PlayHandLabRuntimeConfig, months: int
) -> tuple[str | None, str | None]:
    if not runtime.as_of_date:
        return None, None
    end = datetime.fromisoformat(runtime.as_of_date.replace("Z", "+00:00"))
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end = end.astimezone(timezone.utc)
    return _utc_iso(_subtract_calendar_months(end, months)), _utc_iso(end)


def _validation_phase(runtime: PlayHandLabRuntimeConfig) -> str:
    return f"validation_{int(runtime.validation_months)}mo"


def _score_gate_outcome(
    *,
    score: Any,
    min_score: float,
    missing_reason: str,
    below_reason_prefix: str,
    failed_reason: str,
) -> dict[str, Any]:
    numeric = _as_float(score)
    if numeric is None:
        return {
            "passed": False,
            "score": None,
            "reason": missing_reason,
            "reasons": [failed_reason, missing_reason],
        }
    if numeric < float(min_score):
        threshold_text = f"{float(min_score):g}"
        reason = f"{below_reason_prefix}_{threshold_text}"
        return {
            "passed": False,
            "score": numeric,
            "reason": reason,
            "reasons": [failed_reason, reason],
        }
    return {"passed": True, "score": numeric, "reason": None, "reasons": []}


def _validation_outcome(score: Any, runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    months = int(runtime.validation_months)
    return _score_gate_outcome(
        score=score,
        min_score=float(runtime.validation_min_score),
        missing_reason="missing_validation_score",
        below_reason_prefix="validation_score_below",
        failed_reason=f"validation_{months}mo_failed",
    )


def _lab_final_scrutiny_outcome(score: Any, runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    return _score_gate_outcome(
        score=score,
        min_score=float(runtime.final_min_score),
        missing_reason="missing_final_36mo_score",
        below_reason_prefix="final_36mo_score_below",
        failed_reason=PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON,
    )


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
    outcome_category: str,
    reasons: list[str] | None = None,
) -> None:
    lane.terminal = True
    lane.run_promoted = False
    _set_lane_phase(lane, "tombstoned", event="lane_tombstoned", reason=reason)
    lane.tombstone_reason = reason
    lane.terminal_outcome_category = outcome_category
    lane.tombstone_reasons = sorted({item for item in [reason, *(reasons or [])] if item})
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "run_status": "tombstoned",
            "run_tombstoned": True,
            "tombstone_reason": lane.tombstone_reason,
            "tombstone_reasons": lane.tombstone_reasons,
            "terminal_outcome_category": lane.terminal_outcome_category,
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "phase_started_at": dict(lane.phase_started_at),
            "phase_completed_at": dict(lane.phase_completed_at),
            "phase_task_counts": dict(lane.phase_task_counts),
            "phase_completed_task_counts": dict(lane.phase_completed_task_counts),
            "phase_failed_task_counts": dict(lane.phase_failed_task_counts),
            "phase_lifecycle_events": list(lane.phase_lifecycle_events),
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
        terminal_outcome_category=lane.terminal_outcome_category,
    )


def _mark_lane_promoted(
    lane: LabLaneState,
    *,
    lane_ctx: PlayHandContext,
    final_score: float | None,
) -> None:
    lane.terminal = True
    lane.run_promoted = True
    lane.terminal_outcome_category = TERMINAL_OUTCOME_PROMOTED
    _set_lane_phase(lane, "promoted", event="lane_promoted", final_score=final_score)
    metadata = load_run_metadata(lane.run_dir)
    metadata.update(
        {
            "run_status": "promoted",
            "run_tombstoned": False,
            "tombstone_reason": None,
            "tombstone_reasons": [],
            "terminal_outcome_category": lane.terminal_outcome_category,
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "phase_started_at": dict(lane.phase_started_at),
            "phase_completed_at": dict(lane.phase_completed_at),
            "phase_task_counts": dict(lane.phase_task_counts),
            "phase_completed_task_counts": dict(lane.phase_completed_task_counts),
            "phase_failed_task_counts": dict(lane.phase_failed_task_counts),
            "phase_lifecycle_events": list(lane.phase_lifecycle_events),
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


def _sweep_permutation_index(value: Any, *, context: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise DurableExecutionError(f"{context} is missing a valid permutation_index") from exc


def _sweep_rank_key(item: dict[str, Any]) -> tuple[int, float, int, str]:
    score = _as_float(item.get("fitness_value"))
    index = _sweep_permutation_index(item.get("permutation_index"), context="sweep rank")
    return (
        0 if score is not None else 1,
        -(score if score is not None else 0.0),
        index,
        str(item.get("child_job_id") or ""),
    )


def _validated_sweep_shard_payload(
    *,
    worker_result: dict[str, Any],
    task_spec: dict[str, Any],
) -> dict[str, Any]:
    """Bind one worker shard result to its pre-persisted deterministic task spec."""
    payload = _sweep_payload_from_worker_result(worker_result)
    expected_sweep_id = str(task_spec.get("sweep_id") or "")
    expected_shard_id = str(task_spec.get("shard_id") or "")
    if not expected_sweep_id or not expected_shard_id:
        raise DurableExecutionError("sweep shard task is missing authoritative sweep identity")
    if payload.get("sweep_id") != expected_sweep_id:
        raise DurableExecutionError("sweep shard result sweep_id does not match task spec")
    if payload.get("shard_id") != expected_shard_id:
        raise DurableExecutionError("sweep shard result shard_id does not match task spec")
    if payload.get("status") != "success":
        raise DurableExecutionError("successful sweep task returned a non-success shard result")
    expected_detail = str(task_spec.get("result_detail") or "summary")
    if payload.get("result_detail") != expected_detail:
        raise DurableExecutionError("sweep shard result_detail does not match task spec")
    if not isinstance(payload.get("started_at"), str) or not isinstance(payload.get("completed_at"), str):
        raise DurableExecutionError("sweep shard result is missing lifecycle timestamps")
    permutation_results = payload.get("permutation_results")
    failed_permutations = payload.get("failed_permutations")
    if not isinstance(permutation_results, list) or not isinstance(failed_permutations, list):
        raise DurableExecutionError("sweep shard result is missing canonical permutation collections")

    start = _sweep_permutation_index(
        task_spec.get("permutation_start"), context="sweep shard task spec"
    )
    count = _sweep_permutation_index(
        task_spec.get("permutation_count"), context="sweep shard task spec"
    )
    if count <= 0:
        raise DurableExecutionError("sweep shard task spec has an invalid permutation_count")
    expected_indices = set(range(start, start + count))
    raw_expected_params = task_spec.get("params_by_index")
    if not isinstance(raw_expected_params, dict):
        raise DurableExecutionError("sweep shard task spec is missing params_by_index")
    expected_params: dict[int, dict[str, Any]] = {}
    for raw_index, raw_params in raw_expected_params.items():
        index = _sweep_permutation_index(raw_index, context="sweep shard task params")
        if not isinstance(raw_params, dict):
            raise DurableExecutionError("sweep shard task params are malformed")
        expected_params[index] = dict(raw_params)
    if set(expected_params) != expected_indices:
        raise DurableExecutionError("sweep shard task params do not cover its planned permutations")

    seen_indices: set[int] = set()

    def validate_entry(entry: Any, *, collection: str, status: str) -> None:
        if not isinstance(entry, dict):
            raise DurableExecutionError(f"sweep shard {collection} entry is malformed")
        index = _sweep_permutation_index(entry.get("permutation_index"), context=f"sweep shard {collection}")
        if index not in expected_indices or index in seen_indices:
            raise DurableExecutionError("sweep shard result has duplicate or unexpected permutation evidence")
        seen_indices.add(index)
        if entry.get("status") != status:
            raise DurableExecutionError(f"sweep shard {collection} entry has an invalid status")
        expected_child_job_id = f"{expected_sweep_id}-{index:06d}"
        if entry.get("child_job_id") != expected_child_job_id:
            raise DurableExecutionError("sweep shard result child_job_id does not match task spec")
        params = entry.get("parameters")
        if not isinstance(params, dict) or canonical_sha256(params) != canonical_sha256(expected_params[index]):
            raise DurableExecutionError("sweep shard result parameters do not match task spec")
        if status == "success":
            result = entry.get("result")
            if not isinstance(result, dict):
                raise DurableExecutionError("successful sweep shard entry is missing replay evidence")
            if entry.get("result_detail") != expected_detail or result.get("result_detail") != expected_detail:
                raise DurableExecutionError("sweep shard entry result_detail does not match task spec")
        elif not str(entry.get("error") or "").strip():
            raise DurableExecutionError("failed sweep shard entry is missing an error")

    for entry in permutation_results:
        validate_entry(entry, collection="permutation_results", status="success")
    for entry in failed_permutations:
        validate_entry(entry, collection="failed_permutations", status="error")
    if seen_indices != expected_indices:
        raise DurableExecutionError("sweep shard result does not cover every planned permutation")

    return dict(payload)


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
    source_sweep_id: str | None = None
    source_shard_id: str | None = None
    permutation_indices: list[int] = []
    for shard_result in shard_results:
        payload = _sweep_payload_from_worker_result(shard_result)
        sweep_id = str(payload.get("sweep_id") or "")
        shard_id = str(payload.get("shard_id") or "")
        if source_sweep_id is None:
            source_sweep_id = sweep_id or None
        elif sweep_id and sweep_id != source_sweep_id:
            raise DurableExecutionError("sweep rank received mixed sweep identities")
        if source_shard_id is None:
            source_shard_id = shard_id or None
        elif shard_id and shard_id != source_shard_id:
            raise DurableExecutionError("sweep rank received mixed shard identities")
        for item in payload.get("permutation_results") or []:
            if not isinstance(item, dict):
                continue
            permutation_index = _sweep_permutation_index(
                item.get("permutation_index"), context="sweep permutation result"
            )
            permutation_indices.append(permutation_index)
            result_payload = item.get("result") if isinstance(item.get("result"), dict) else {}
            score = _score_from_replay_payload(result_payload)
            if score is None:
                score = _score_from_replay_payload(item)
            ranked.append(
                {
                    "permutation_index": permutation_index,
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
                permutation_indices.append(
                    _sweep_permutation_index(
                        item.get("permutation_index"), context="failed sweep permutation"
                    )
                )
                failed.append(dict(item))
    ranked.sort(key=_sweep_rank_key)
    failed.sort(
        key=lambda item: (
            _sweep_permutation_index(item.get("permutation_index"), context="failed sweep permutation"),
            str(item.get("child_job_id") or ""),
        )
    )
    scored = [item for item in ranked if _as_float(item.get("fitness_value")) is not None]
    best = scored[0] if scored else None
    return {
        "sweep_id": source_sweep_id or f"lab-{phase}",
        "shard_id": source_shard_id,
        "mode": "lab_sweep_shard",
        "permutation_indices": sorted(permutation_indices),
        "outcome": "scored" if best is not None else "no_scored_permutation",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": best,
        "failed_permutations": failed,
        "parameter_importance": _parameter_importance_from_ranked(ranked),
    }


def _merge_sweep_payloads(
    phase: str,
    payloads: list[dict[str, Any]],
    *,
    expected_sweep_id: str | None = None,
    expected_shards: dict[str, set[int]] | None = None,
) -> dict[str, Any]:
    ranked: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    seen_shards: set[str] = set()
    seen_indices: set[int] = set()
    for payload in payloads:
        sweep_id = str(payload.get("sweep_id") or "")
        shard_id = str(payload.get("shard_id") or "")
        if expected_sweep_id and sweep_id != expected_sweep_id:
            raise DurableExecutionError("sweep merge received an unexpected sweep identity")
        if expected_shards is not None:
            if shard_id not in expected_shards or shard_id in seen_shards:
                raise DurableExecutionError("sweep merge received an unexpected or duplicate shard")
            payload_indices = {
                _sweep_permutation_index(index, context="sweep shard receipt")
                for index in (payload.get("permutation_indices") or [])
            }
            if payload_indices != expected_shards[shard_id]:
                raise DurableExecutionError("sweep shard receipt does not cover its planned permutations")
            seen_shards.add(shard_id)
        for item in payload.get("ranked_permutations") or payload.get("ranked") or []:
            if isinstance(item, dict):
                index = _sweep_permutation_index(item.get("permutation_index"), context="sweep merge")
                if index in seen_indices:
                    raise DurableExecutionError("sweep merge received duplicate permutation evidence")
                seen_indices.add(index)
                ranked.append(dict(item))
        for item in payload.get("failed_permutations") or []:
            if isinstance(item, dict):
                index = _sweep_permutation_index(item.get("permutation_index"), context="failed sweep merge")
                if index in seen_indices:
                    raise DurableExecutionError("sweep merge received duplicate permutation evidence")
                seen_indices.add(index)
                failed.append(dict(item))
    if expected_shards is not None:
        if seen_shards != set(expected_shards):
            raise DurableExecutionError("sweep merge is missing one or more shard receipts")
        expected_indices = set().union(*expected_shards.values()) if expected_shards else set()
        if seen_indices != expected_indices:
            raise DurableExecutionError("sweep merge is missing one or more permutation receipts")
    ranked.sort(key=_sweep_rank_key)
    failed.sort(
        key=lambda item: (
            _sweep_permutation_index(item.get("permutation_index"), context="failed sweep merge"),
            str(item.get("child_job_id") or ""),
        )
    )
    scored = [item for item in ranked if _as_float(item.get("fitness_value")) is not None]
    return {
        "sweep_id": expected_sweep_id or f"lab-{phase}",
        "mode": "lab_sweep_shard",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": scored[0] if scored else None,
        "failed_permutations": failed,
        "outcome": "scored" if scored else "no_scored_permutation",
        "parameter_importance": _parameter_importance_from_ranked(ranked),
    }


def _sweep_parent_task_groups(
    lane: LabLaneState,
    *,
    phase: str,
) -> dict[str, dict[str, tuple[str, dict[str, Any]]]]:
    """Return the pre-persisted sweep graph, bucketed by authoritative parent ID."""
    groups: dict[str, dict[str, tuple[str, dict[str, Any]]]] = {}
    seen_task_ids: set[str] = set()
    for task_id in lane.phase_task_ids.get(phase) or []:
        if task_id in seen_task_ids:
            raise DurableExecutionError("sweep phase task graph has a duplicate task ID")
        seen_task_ids.add(task_id)
        spec = lane.task_specs.get(task_id)
        if not isinstance(spec, dict) or spec.get("task_kind") != "sweep_shard":
            raise DurableExecutionError("sweep phase contains a non-sweep task spec")
        sweep_id = str(spec.get("sweep_id") or "")
        shard_id = str(spec.get("shard_id") or "")
        if not sweep_id or not shard_id:
            raise DurableExecutionError("sweep phase task is missing deterministic shard identity")
        parent = groups.setdefault(sweep_id, {})
        if shard_id in parent:
            raise DurableExecutionError("sweep phase task graph has a duplicate shard ID")
        parent[shard_id] = (task_id, spec)
    if not groups:
        raise DurableExecutionError("sweep phase is missing deterministic shard tasks")
    return groups


def _normalize_persisted_sweep_shard(
    *,
    phase: str,
    task_id: str,
    task_spec: dict[str, Any],
    recorded: dict[str, Any],
) -> dict[str, Any]:
    """Rebind a persisted shard receipt to its deterministic task graph entry.

    Older receipts predate explicit parent/shard fields in ``sweep-results.json``.
    They are accepted only when their exact task-scoped permutation evidence proves
    that they are the legacy representation of this same deterministic shard.
    """
    payload = recorded.get("sweep_payload")
    if not isinstance(payload, dict):
        raise DurableExecutionError(f"sweep task {task_id} is missing its shard receipt")
    expected_sweep_id = str(task_spec.get("sweep_id") or "")
    expected_shard_id = str(task_spec.get("shard_id") or "")
    if not expected_sweep_id or not expected_shard_id:
        raise DurableExecutionError("sweep task spec is missing deterministic parent identity")
    source_sweep_id = str(payload.get("sweep_id") or "")
    source_shard_id = str(payload.get("shard_id") or "")
    legacy_unbound = (
        source_sweep_id == f"lab-{phase}"
        and not source_shard_id
        and payload.get("permutation_indices") is None
    )
    if not legacy_unbound and (
        source_sweep_id != expected_sweep_id or source_shard_id != expected_shard_id
    ):
        raise DurableExecutionError("persisted sweep shard identity does not match task spec")

    start = _sweep_permutation_index(
        task_spec.get("permutation_start"), context="persisted sweep task spec"
    )
    count = _sweep_permutation_index(
        task_spec.get("permutation_count"), context="persisted sweep task spec"
    )
    if count <= 0:
        raise DurableExecutionError("persisted sweep task spec has an invalid permutation_count")
    expected_indices = set(range(start, start + count))
    raw_params = task_spec.get("params_by_index")
    if not isinstance(raw_params, dict):
        raise DurableExecutionError("persisted sweep task spec is missing params_by_index")
    expected_params = {
        _sweep_permutation_index(index, context="persisted sweep task params"): params
        for index, params in raw_params.items()
        if isinstance(params, dict)
    }
    if set(expected_params) != expected_indices:
        raise DurableExecutionError("persisted sweep task params do not cover planned permutations")

    ranked = payload.get("ranked_permutations")
    alias = payload.get("ranked")
    if ranked is None:
        ranked = alias
    elif alias is not None and canonical_sha256(ranked) != canonical_sha256(alias):
        raise DurableExecutionError("persisted sweep shard ranked aliases disagree")
    failed = payload.get("failed_permutations")
    if not isinstance(ranked, list) or not isinstance(failed, list):
        raise DurableExecutionError("persisted sweep shard collections are malformed")

    seen_indices: set[int] = set()

    def validate_entry(entry: Any, *, status: str) -> dict[str, Any]:
        if not isinstance(entry, dict):
            raise DurableExecutionError("persisted sweep shard entry is malformed")
        index = _sweep_permutation_index(
            entry.get("permutation_index"), context="persisted sweep shard entry"
        )
        if index not in expected_indices or index in seen_indices:
            raise DurableExecutionError("persisted sweep shard has duplicate or unexpected evidence")
        seen_indices.add(index)
        if entry.get("status") != status:
            raise DurableExecutionError("persisted sweep shard entry has an invalid status")
        if entry.get("child_job_id") != f"{expected_sweep_id}-{index:06d}":
            raise DurableExecutionError("persisted sweep shard child_job_id conflicts with task spec")
        params = entry.get("parameters")
        if not isinstance(params, dict) or canonical_sha256(params) != canonical_sha256(expected_params[index]):
            raise DurableExecutionError("persisted sweep shard parameters conflict with task spec")
        clone = dict(entry)
        if status == "success":
            score = _as_float(clone.get("score"))
            fitness_value = _as_float(clone.get("fitness_value"))
            if (score is None) != (fitness_value is None) or (
                score is not None and score != fitness_value
            ):
                raise DurableExecutionError("persisted sweep shard score fields disagree")
            clone["score"] = score
            clone["fitness_value"] = fitness_value
        elif not str(clone.get("error") or "").strip():
            raise DurableExecutionError("persisted failed sweep entry is missing an error")
        return clone

    normalized_ranked = [validate_entry(entry, status="success") for entry in ranked]
    normalized_failed = [validate_entry(entry, status="error") for entry in failed]
    if seen_indices != expected_indices:
        raise DurableExecutionError("persisted sweep shard does not cover its planned permutations")
    if not legacy_unbound:
        source_indices = {
            _sweep_permutation_index(index, context="persisted sweep shard identity")
            for index in (payload.get("permutation_indices") or [])
        }
        if source_indices != expected_indices:
            raise DurableExecutionError("persisted sweep shard identity does not cover planned permutations")
    return {
        "sweep_id": expected_sweep_id,
        "shard_id": expected_shard_id,
        "mode": "lab_sweep_shard",
        "permutation_indices": sorted(expected_indices),
        "ranked_permutations": normalized_ranked,
        "ranked": normalized_ranked,
        "failed_permutations": normalized_failed,
    }


def _merge_sweep_parent_receipts(
    *,
    lane: LabLaneState,
    phase: str,
    sweep_id: str,
    task_specs: dict[str, tuple[str, dict[str, Any]]],
    records_by_task_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    expected_task_ids = {task_id for task_id, _spec in task_specs.values()}
    if set(records_by_task_id) != expected_task_ids:
        raise DurableExecutionError("sweep parent is missing or has duplicate shard receipts")
    expected_shards: dict[str, set[int]] = {}
    payloads: list[dict[str, Any]] = []
    for shard_id, (task_id, task_spec) in sorted(task_specs.items()):
        start = _sweep_permutation_index(
            task_spec.get("permutation_start"), context="sweep parent task spec"
        )
        count = _sweep_permutation_index(
            task_spec.get("permutation_count"), context="sweep parent task spec"
        )
        if count <= 0:
            raise DurableExecutionError("sweep parent task spec has an invalid permutation_count")
        expected_shards[shard_id] = set(range(start, start + count))
        payloads.append(
            _normalize_persisted_sweep_shard(
                phase=phase,
                task_id=task_id,
                task_spec=task_spec,
                recorded=records_by_task_id[task_id],
            )
        )
    return _merge_sweep_payloads(
        phase,
        payloads,
        expected_sweep_id=sweep_id,
        expected_shards=expected_shards,
    )


def _merge_phase_sweep_receipts(lane: LabLaneState, *, phase: str) -> dict[str, Any]:
    """Merge each declared parent independently, then select across parents deterministically."""
    groups = _sweep_parent_task_groups(lane, phase=phase)
    records_by_parent: dict[str, dict[str, dict[str, Any]]] = {
        sweep_id: {} for sweep_id in groups
    }
    task_to_parent = {
        task_id: sweep_id
        for sweep_id, shards in groups.items()
        for task_id, _spec in shards.values()
    }
    for recorded in lane.phase_results.get(phase, []):
        if not isinstance(recorded, dict):
            raise DurableExecutionError("sweep phase contains a malformed recorded result")
        task_id = str(recorded.get("task_id") or "")
        sweep_id = task_to_parent.get(task_id)
        if sweep_id is None:
            raise DurableExecutionError("sweep phase contains a stale or cross-parent shard receipt")
        bucket = records_by_parent[sweep_id]
        if task_id in bucket:
            raise DurableExecutionError("sweep phase contains a duplicate shard receipt")
        bucket[task_id] = recorded

    parent_payloads = [
        _merge_sweep_parent_receipts(
            lane=lane,
            phase=phase,
            sweep_id=sweep_id,
            task_specs=groups[sweep_id],
            records_by_task_id=records_by_parent[sweep_id],
        )
        for sweep_id in sorted(groups)
    ]
    if len(parent_payloads) == 1:
        return parent_payloads[0]

    ranked = [
        dict(item)
        for payload in parent_payloads
        for item in payload.get("ranked_permutations") or []
        if isinstance(item, dict)
    ]
    failed = [
        dict(item)
        for payload in parent_payloads
        for item in payload.get("failed_permutations") or []
        if isinstance(item, dict)
    ]
    ranked.sort(key=_sweep_rank_key)
    failed.sort(
        key=lambda item: (
            _sweep_permutation_index(item.get("permutation_index"), context="phase sweep merge"),
            str(item.get("child_job_id") or ""),
        )
    )
    scored = [item for item in ranked if _as_float(item.get("fitness_value")) is not None]
    return {
        "sweep_id": f"lab-{phase}-parents",
        "mode": "lab_sweep_parent_selection",
        "parent_sweeps": parent_payloads,
        "ranked_permutations": ranked,
        "ranked": ranked,
        "best": scored[0] if scored else None,
        "failed_permutations": failed,
        "outcome": "scored" if scored else "no_scored_permutation",
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
    analysis_window_start: str | None = None,
    analysis_window_end: str | None = None,
) -> dict[str, Any]:
    profile_payload = _copy_profile_payload(profile_payload or lane.profile_payload)
    if bool(analysis_window_start) != bool(analysis_window_end):
        raise ValueError("Evidence-bound replay requires both analysis window bounds.")
    evidence_plan = None
    if analysis_window_start and analysis_window_end:
        evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=f"playhand-lab:{lane.run_id}",
            evidence_role="training",
            selection_data_end=analysis_window_end,
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
            requested_horizon_months=int(lookback_months or runtime.lookback_months),
            profile_snapshot=profile_payload,
            lake_manifest_sha256=runtime.lake_manifest_sha256,
            data_availability_cutoff=(
                runtime.as_of_date if runtime.as_of_date else analysis_window_end
            ),
        )
    _require_historical_task_evidence(
        runtime=runtime,
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
        evidence_plan=evidence_plan,
    )
    evidence_payload = evidence_plan.model_dump(mode="json") if evidence_plan else None
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
        "lookback_months": (
            None
            if evidence_plan
            else int(lookback_months or runtime.lookback_months)
        ),
        "analysis_window_start": analysis_window_start,
        "analysis_window_end": analysis_window_end,
        "evidence_plan": evidence_payload,
        "bar_limit": int(runtime.bar_limit),
        "alert_threshold": float(profile_payload.get("notificationThreshold") or 80.0),
        "view_mode": "overview",
        "direction_mode": _profile_direction_mode(profile_payload),
        "priority": "research",
        "work_class": "research_replay",
        "required_worker_contract_hash": worker_contract_hash,
        "required_worker_contract_schema": runtime.worker_contract_schema,
        "required_capabilities": ["deep_replay"],
        "policy_assignment": copy.deepcopy(lane.policy_assignment),
    }
    lineage = _historical_lineage_payload(runtime)
    if lineage:
        job["research_lineage"] = lineage
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
    _record_lane_phase_tasks_started(lane, phase=phase, task_kind=task_kind)


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
    analysis_window_start: str | None = None,
    analysis_window_end: str | None = None,
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
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
    )
    _register_task_spec(
        lane,
        task_id=task_id,
        phase=phase,
        task_kind="deep_replay",
        spec={
            "profile_path": str(profile_path.resolve()) if profile_path else None,
            "profile_ref": profile_ref,
            "instruments": list(instruments),
            "timeframe": timeframe,
            "lookback_months": lookback_months,
                "analysis_window_start": analysis_window_start,
                "analysis_window_end": analysis_window_end,
                "evidence_plan": payload.get("evidence_plan"),
                "policy_assignment": copy.deepcopy(lane.policy_assignment),
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
    analysis_window_start: str | None,
    analysis_window_end: str | None,
    mode: str,
) -> dict[str, Any]:
    if bool(analysis_window_start) != bool(analysis_window_end):
        raise ValueError("Evidence-bound sweep requires both analysis window bounds.")
    evidence_plan = None
    if analysis_window_start and analysis_window_end:
        evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=f"playhand-lab:{lane.run_id}",
            evidence_role="training",
            selection_data_end=analysis_window_end,
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
            requested_horizon_months=int(lookback_months),
            profile_snapshot=profile_payload,
            lake_manifest_sha256=runtime.lake_manifest_sha256,
            data_availability_cutoff=(
                runtime.as_of_date if runtime.as_of_date else analysis_window_end
            ),
        )
    _require_historical_task_evidence(
        runtime=runtime,
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
        evidence_plan=evidence_plan,
    )
    evidence_payload = evidence_plan.model_dump(mode="json") if evidence_plan else None
    definition: dict[str, Any] = {
        "base_profile_id": profile_ref,
        "axes": _sanitize_sweep_axes_for_contract(axes),
        "instruments": list(instruments),
        "mode": "deterministic" if mode not in {"deterministic", "evolutionary"} else mode,
        "evolutionary_config": None,
        "lookback_months": None if evidence_plan else int(lookback_months),
        "analysis_window_start": analysis_window_start,
        "analysis_window_end": analysis_window_end,
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
        "evidence_plan": evidence_payload,
        "policy_assignment": copy.deepcopy(lane.policy_assignment),
    }
    lineage = _historical_lineage_payload(runtime)
    if lineage:
        definition["research_lineage"] = lineage
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
    analysis_window_start: str | None = None,
    analysis_window_end: str | None = None,
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
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
        mode=mode,
    )
    tasks: list[dict[str, Any]] = []
    shard_size = max(int(runtime.sweep_shard_size), 1)
    shared_snapshot = _copy_profile_payload(profile_payload)
    for shard_index, start in enumerate(range(0, len(params), shard_size)):
        chunk = params[start : start + shard_size]
        task_id = f"{lane.run_id}-task-{len(lane.task_ids) + 1:05d}-{phase}-shard-{shard_index:04d}"
        shard_id = f"{sweep_id}-shard-{shard_index:04d}"
        params_by_index = {start + offset: param for offset, param in enumerate(chunk)}
        params_by_index_sha256 = canonical_sha256(_canonical_params(params_by_index))
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
            "evidence_plan": definition.get("evidence_plan"),
            "base_profile_snapshot": shared_snapshot,
            "permutation_start": start,
            "permutation_count": len(chunk),
            "permutation_indices": list(params_by_index),
            "params_by_index": params_by_index,
            "retention_ttl_seconds": 86400,
            "required_worker_contract_hash": worker_contract_hash,
            "required_worker_contract_schema": runtime.worker_contract_schema,
            "required_capabilities": ["deep_replay", "sweep_shard"],
            "result_detail": "summary",
            "policy_assignment": copy.deepcopy(lane.policy_assignment),
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
                "instruments": list(instruments),
                "timeframe": lane.incumbent_timeframe or lane.timeframe,
                "lookback_months": lookback_months,
                "analysis_window_start": analysis_window_start,
                "analysis_window_end": analysis_window_end,
                "evidence_plan": definition.get("evidence_plan"),
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
                "params_by_index_sha256": params_by_index_sha256,
                "result_detail": "summary",
                "policy_assignment": copy.deepcopy(lane.policy_assignment),
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
        if lane.terminal:
            continue
        if (
            runtime.task_mode == "deep_replay"
            and runtime.pipeline_mode == PLAY_HAND_LAB_PLAY_HAND_PIPELINE
        ):
            if not worker_contract_hash:
                raise RuntimeError("Deep-replay lab tasks require a worker contract hash.")
            _set_lane_phase(lane, "baseline")
            analysis_window_start, analysis_window_end = _lane_screen_window(lane)
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
                    analysis_window_start=analysis_window_start,
                    analysis_window_end=analysis_window_end,
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
                    "policy_assignment": copy.deepcopy(lane.policy_assignment),
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
                    "instruments": list(lane.instruments),
                    "timeframe": lane.timeframe,
                    "lookback_months": runtime.lookback_months,
                    "policy_assignment": copy.deepcopy(lane.policy_assignment),
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


def _validated_execution_evidence(
    result_payload: dict[str, Any],
    evidence_plan: dict[str, Any],
) -> dict[str, Any] | None:
    if not evidence_plan:
        return None
    nested = result_payload.get("result")
    nested = nested if isinstance(nested, dict) else {}
    receipt = result_payload.get("execution_evidence") or nested.get(
        "execution_evidence"
    )
    if not evidence_plan.get("lake_manifest_sha256") and not isinstance(receipt, dict):
        return None
    if not isinstance(receipt, dict):
        raise RuntimeError("Evidence-bound historical result omitted execution_evidence")
    expected = {
        "plan_id": evidence_plan.get("plan_id"),
        "profile_snapshot_sha256": evidence_plan.get("profile_snapshot_sha256"),
        "execution_cell_sha256": evidence_plan.get("execution_cell_sha256"),
        "observed_lake_manifest_sha256": evidence_plan.get("lake_manifest_sha256"),
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise RuntimeError(
                f"Historical execution receipt {key} mismatch: expected {value!r}, observed {receipt.get(key)!r}"
            )
    return dict(receipt)


TASK_RESULT_RECEIPT_SCHEMA = "play-hand-lab-task-result-receipt-v2"
PHASE3_LEGACY_FOLLOW_ON_RECEIPT_MIGRATION_SCHEMA = (
    "phase3-retained-follow-on-receipt-migration-v1"
)


def _task_artifact_receipt(artifact_dir: Path) -> dict[str, Any]:
    receipt_path = artifact_dir / "task-result-receipt.json"
    files = [
        path
        for path in artifact_dir.rglob("*")
        if path.is_file() and path != receipt_path
    ]
    return artifact_receipt(files, root=artifact_dir)


def _write_task_result_receipt(
    path: Path,
    *,
    task_id: str,
    worker_result_sha256: str,
    recorded_result: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": TASK_RESULT_RECEIPT_SCHEMA,
        "task_id": task_id,
        "worker_result_sha256": worker_result_sha256,
        "artifact_receipt": _task_artifact_receipt(path.parent),
        "recorded_result": recorded_result,
    }
    return _persist_task_result_receipt(
        path,
        payload,
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
    )


def _seal_task_result_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Detach and seal one receipt from its caller-owned nested objects."""
    unsigned = dict(payload)
    unsigned.pop("receipt_sha256", None)
    try:
        canonical = json.loads(canonical_json(unsigned))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DurableExecutionError("task result receipt is not canonical JSON") from exc
    if not isinstance(canonical, dict):
        raise DurableExecutionError("task result receipt must be a JSON object")
    canonical["receipt_sha256"] = canonical_sha256(canonical)
    return canonical


def _persist_task_result_receipt(
    path: Path,
    payload: Mapping[str, Any],
    *,
    task_id: str,
    worker_result_sha256: str,
) -> dict[str, Any]:
    sealed = _seal_task_result_receipt(payload)
    atomic_write_json(path, sealed)
    return _validate_task_result_receipt(
        path,
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
    )


def _validate_task_result_receipt_payload(
    receipt: Any,
    *,
    task_id: str,
    worker_result_sha256: str | None = None,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise DurableExecutionError(f"task result receipt is unreadable: {task_id}")
    supplied = dict(receipt)
    receipt_sha256 = str(supplied.pop("receipt_sha256", ""))
    if (
        supplied.get("schema_version") != TASK_RESULT_RECEIPT_SCHEMA
        or receipt_sha256 != canonical_sha256(supplied)
        or supplied.get("task_id") != task_id
        or not isinstance(supplied.get("recorded_result"), dict)
        or not isinstance(supplied.get("artifact_receipt"), dict)
    ):
        raise DurableExecutionError(f"task result receipt conflicts for task {task_id}")
    if worker_result_sha256 is not None and supplied.get("worker_result_sha256") != worker_result_sha256:
        raise DurableExecutionError(f"worker result identity conflicts for task {task_id}")
    migration = supplied.get("compatibility_migration")
    if migration is not None:
        _validate_phase3_legacy_follow_on_receipt_migration(
            migration,
            task_id=task_id,
            derived_tasks=supplied.get("derived_tasks"),
        )
    if "derived_tasks" in supplied:
        supplied["derived_tasks"] = _validated_receipt_derived_tasks(
            supplied,
            task_id=task_id,
            required=False,
        )
    validate_artifact_receipt(supplied["artifact_receipt"])
    supplied["receipt_sha256"] = receipt_sha256
    return supplied


def _validate_task_result_receipt(
    path: Path,
    *,
    task_id: str,
    worker_result_sha256: str | None = None,
) -> dict[str, Any]:
    return _validate_task_result_receipt_payload(
        _load_json(path),
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
    )


def _validate_phase3_legacy_follow_on_receipt_migration(
    migration: Any,
    *,
    task_id: str,
    derived_tasks: Any,
) -> None:
    if not isinstance(migration, dict) or set(migration) != {
        "schema_version",
        "legacy_receipt_sha256",
        "legacy_payload_sha256",
        "derived_tasks_sha256",
    }:
        raise DurableExecutionError(
            f"legacy follow-on receipt migration is malformed for task {task_id}"
        )
    if migration.get("schema_version") != PHASE3_LEGACY_FOLLOW_ON_RECEIPT_MIGRATION_SCHEMA:
        raise DurableExecutionError(
            f"legacy follow-on receipt migration schema conflicts for task {task_id}"
        )
    for key in (
        "legacy_receipt_sha256",
        "legacy_payload_sha256",
        "derived_tasks_sha256",
    ):
        if not isinstance(migration.get(key), str) or not str(migration[key]).startswith("sha256:"):
            raise DurableExecutionError(
                f"legacy follow-on receipt migration identity is malformed for task {task_id}"
            )
    validated = _validated_receipt_derived_tasks(
        {"derived_tasks": derived_tasks},
        task_id=task_id,
        required=True,
    )
    raw_graph_sha256 = canonical_sha256(derived_tasks)
    normalized_graph_sha256 = canonical_sha256(validated)
    if migration["derived_tasks_sha256"] not in {
        raw_graph_sha256,
        normalized_graph_sha256,
    }:
        raise DurableExecutionError(
            f"legacy follow-on receipt migration task graph conflicts for task {task_id}"
        )


def _legacy_phase3_receipt_recorded_result(
    receipt_path: Path,
    *,
    task_id: str,
    worker_result_sha256: str,
    recovered_row: Mapping[str, Any],
) -> dict[str, Any]:
    """Admit only the pre-follow-on-binding receipt shape for a Phase 3 replay."""
    raw = _load_json(receipt_path)
    if not isinstance(raw, dict):
        raise DurableExecutionError(f"task result receipt is unreadable: {task_id}")
    if set(raw) != {
        "schema_version",
        "task_id",
        "worker_result_sha256",
        "artifact_receipt",
        "recorded_result",
        "derived_tasks",
        "receipt_sha256",
    }:
        raise DurableExecutionError(
            f"legacy Phase 3 receipt shape conflicts for task {task_id}"
        )
    unsigned = dict(raw)
    legacy_receipt_sha256 = str(unsigned.pop("receipt_sha256", ""))
    if (
        raw.get("schema_version") != TASK_RESULT_RECEIPT_SCHEMA
        or raw.get("task_id") != task_id
        or raw.get("worker_result_sha256") != worker_result_sha256
        or not isinstance(raw.get("recorded_result"), dict)
        or not isinstance(raw.get("artifact_receipt"), dict)
        or not isinstance(raw.get("derived_tasks"), list)
        or not legacy_receipt_sha256.startswith("sha256:")
        or legacy_receipt_sha256 == canonical_sha256(unsigned)
    ):
        raise DurableExecutionError(
            f"legacy Phase 3 receipt cannot be proven for task {task_id}"
        )
    _validated_receipt_derived_tasks(raw, task_id=task_id, required=True)
    validate_artifact_receipt(raw["artifact_receipt"])
    recorded = dict(raw["recorded_result"])
    expected = {
        "task_id": task_id,
        "attempt_id": recovered_row.get("attempt_id"),
        "artifact_dir": str(Path(str(recovered_row.get("artifact_dir") or "")).resolve(strict=False)),
        "score": recovered_row.get("composite_score"),
        "score_basis": recovered_row.get("score_basis"),
        "status": "failed" if recovered_row.get("lab_scoring_warning") else "success",
        "phase": recovered_row.get("play_hand_phase"),
        "task_kind": recovered_row.get("lab_task_kind"),
        "profile_path": recovered_row.get("profile_path"),
        "profile_ref": recovered_row.get("profile_ref"),
        "instruments": list(recovered_row.get("play_hand_selected_instruments") or []),
        "timeframe": recovered_row.get("effective_timeframe"),
        "lookback_months": recovered_row.get("requested_horizon_months"),
        "analysis_window_start": recovered_row.get("analysis_window_start"),
        "analysis_window_end": recovered_row.get("analysis_window_end"),
        "evidence_plan_id": recovered_row.get("evidence_plan_id"),
        "evidence_role": recovered_row.get("evidence_role"),
        "policy_assignment": recovered_row.get("policy_assignment"),
    }
    for key, value in expected.items():
        if recorded.get(key) != value:
            raise DurableExecutionError(
                f"legacy Phase 3 receipt evidence conflicts for task {task_id}: {key}"
            )
    return recorded


def _upgrade_legacy_phase3_follow_on_receipt(
    receipt_path: Path,
    *,
    task_id: str,
    worker_result_sha256: str,
    recorded_result: Mapping[str, Any],
    derived_tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    raw = _load_json(receipt_path)
    if not isinstance(raw, dict):
        raise DurableExecutionError(f"task result receipt is unreadable: {task_id}")
    provisional = dict(raw)
    legacy_receipt_sha256 = str(provisional.pop("receipt_sha256", ""))
    expected_keys = {
        "schema_version",
        "task_id",
        "worker_result_sha256",
        "artifact_receipt",
        "recorded_result",
        "derived_tasks",
    }
    if (
        set(provisional) != expected_keys
        or provisional.get("schema_version") != TASK_RESULT_RECEIPT_SCHEMA
        or provisional.get("task_id") != task_id
        or provisional.get("worker_result_sha256") != worker_result_sha256
        or provisional.get("recorded_result") != dict(recorded_result)
        or not legacy_receipt_sha256.startswith("sha256:")
        or legacy_receipt_sha256 == canonical_sha256(provisional)
    ):
        raise DurableExecutionError(
            f"legacy Phase 3 receipt cannot be migrated for task {task_id}"
        )
    validate_artifact_receipt(provisional["artifact_receipt"])
    stored_tasks = _validated_receipt_derived_tasks(
        provisional,
        task_id=task_id,
        required=True,
    )
    canonical_tasks = _validated_receipt_derived_tasks(
        {"derived_tasks": derived_tasks},
        task_id=task_id,
        required=True,
    )
    if stored_tasks != canonical_tasks:
        raise DurableExecutionError(
            f"legacy Phase 3 follow-on graph conflicts for task {task_id}"
        )
    provisional["compatibility_migration"] = {
        "schema_version": PHASE3_LEGACY_FOLLOW_ON_RECEIPT_MIGRATION_SCHEMA,
        "legacy_receipt_sha256": legacy_receipt_sha256,
        "legacy_payload_sha256": canonical_sha256(provisional),
        "derived_tasks_sha256": canonical_sha256(canonical_tasks),
    }
    return _persist_task_result_receipt(
        receipt_path,
        provisional,
        task_id=task_id,
        worker_result_sha256=worker_result_sha256,
    )


def _terminal_receipt_for_result(
    recorded: dict[str, Any],
    lab_result: dict[str, Any],
    *,
    derived_tasks: list[dict[str, Any]] | None = None,
    allow_legacy_phase3_receipt_migration: bool = False,
) -> dict[str, Any]:
    task_id = str(lab_result.get("task_id") or "")
    artifact_dir = Path(str(recorded.get("artifact_dir") or "")).resolve(strict=False)
    receipt_path = artifact_dir / "task-result-receipt.json"
    worker_result_sha256 = _worker_result_identity(lab_result)
    try:
        receipt = _validate_task_result_receipt(
            receipt_path,
            task_id=task_id,
            worker_result_sha256=worker_result_sha256,
        )
    except DurableExecutionError:
        if not allow_legacy_phase3_receipt_migration or derived_tasks is None:
            raise
        return _upgrade_legacy_phase3_follow_on_receipt(
            receipt_path,
            task_id=task_id,
            worker_result_sha256=worker_result_sha256,
            recorded_result=recorded,
            derived_tasks=derived_tasks,
        )
    if derived_tasks is None:
        return receipt
    canonical_tasks = _validated_receipt_derived_tasks(
        {"derived_tasks": derived_tasks},
        task_id=task_id,
        required=True,
    )
    existing = receipt.get("derived_tasks")
    if existing is not None and existing != canonical_tasks:
        raise DurableExecutionError(
            f"derived task receipt conflicts for source task {task_id}"
        )
    updated = copy.deepcopy(receipt)
    updated["derived_tasks"] = canonical_tasks
    return _persist_task_result_receipt(
        receipt_path,
        updated,
        task_id=task_id,
        worker_result_sha256=_worker_result_identity(lab_result),
    )


def _validated_receipt_derived_tasks(
    receipt: Mapping[str, Any],
    *,
    task_id: str,
    required: bool = False,
) -> list[dict[str, Any]] | None:
    raw = receipt.get("derived_tasks")
    if raw is None:
        if required:
            raw = []
        else:
            return None
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise DurableExecutionError(
            f"derived task receipt is malformed for source task {task_id}"
        )
    tasks = [
        _normalize_derived_task_receipt_item(copy.deepcopy(item), task_id=task_id)
        for item in raw
    ]
    derived_ids = [str(item.get("task_id") or "") for item in tasks]
    if any(not derived_id for derived_id in derived_ids) or len(set(derived_ids)) != len(derived_ids):
        raise DurableExecutionError(
            f"derived task receipt has invalid task identities for source task {task_id}"
        )
    return tasks


def _normalize_derived_task_receipt_item(
    task: dict[str, Any],
    *,
    task_id: str,
) -> dict[str, Any]:
    payload = task.get("payload")
    if not isinstance(payload, dict):
        return task
    params_by_index = payload.get("params_by_index")
    if not isinstance(params_by_index, dict):
        return task
    if not all(
        isinstance(index, int) or (isinstance(index, str) and index.isdecimal())
        for index in params_by_index
    ):
        return task
    normalized: dict[int, Any] = {}
    for raw_index, params in params_by_index.items():
        index = int(raw_index)
        if index in normalized:
            raise DurableExecutionError(
                f"derived task receipt has conflicting params_by_index keys for source task {task_id}"
            )
        normalized[index] = params
    payload["params_by_index"] = normalized
    return task


def _record_lab_result(
    *,
    config: AppConfig,
    cli: FuzzfolioCli,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    lab_result: dict[str, Any],
    reward_matrix: dict[str, Any] | None,
    render_progress: bool = True,
    allow_legacy_phase3_receipt_migration: bool = False,
) -> dict[str, Any]:
    task_id = str(lab_result.get("task_id") or "")
    task_spec = lane.task_specs.get(task_id, {})
    task_kind = str(task_spec.get("task_kind") or runtime.task_mode)
    phase = str(task_spec.get("phase") or task_kind)
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    evidence_plan = task_spec.get("evidence_plan")
    evidence_plan = evidence_plan if isinstance(evidence_plan, dict) else {}
    execution_evidence = _validated_execution_evidence(result_payload, evidence_plan)
    artifact_key = canonical_sha256({"task_id": task_id})[-20:]
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_{phase}_{artifact_key}").resolve()
    result_identity = _worker_result_identity(lab_result)
    attempts = load_attempts(lane_ctx.attempts_path)
    matches = [
        row
        for row in attempts
        if str(row.get("lab_campaign_task_id") or "") == task_id
    ]
    if len(matches) > 1:
        raise DurableExecutionError(f"duplicate ledger rows exist for task {task_id}")
    recovered_row = matches[0] if matches else None
    if recovered_row is not None:
        if recovered_row.get("lab_worker_result_sha256") != result_identity:
            raise DurableExecutionError(
                f"ledger result identity conflicts with duplicate task {task_id}"
            )
        artifact_dir = Path(str(recovered_row.get("artifact_dir") or "")).resolve(
            strict=False
        )
        if not artifact_dir.is_dir():
            raise DurableExecutionError(
                f"ledger artifact is missing for duplicate task {task_id}"
            )
    receipt_path = artifact_dir / "task-result-receipt.json"
    if receipt_path.is_file():
        try:
            receipt = _validate_task_result_receipt(
                receipt_path,
                task_id=task_id,
                worker_result_sha256=result_identity,
            )
            return dict(receipt["recorded_result"])
        except DurableExecutionError:
            if not allow_legacy_phase3_receipt_migration or recovered_row is None:
                raise
            return _legacy_phase3_receipt_recorded_result(
                receipt_path,
                task_id=task_id,
                worker_result_sha256=result_identity,
                recovered_row=recovered_row,
            )
    if recovered_row is not None:
        row = recovered_row
        recorded = {
                "task_id": task_id,
                "attempt_id": row.get("attempt_id"),
                "artifact_dir": str(artifact_dir),
                "score": row.get("composite_score"),
                "score_basis": row.get("score_basis"),
                "status": "failed" if row.get("lab_scoring_warning") else "success",
                "phase": phase,
                "task_kind": task_kind,
                "profile_path": row.get("profile_path"),
                "profile_ref": row.get("profile_ref"),
                "instruments": list(task_spec.get("instruments") or lane.instruments),
                "timeframe": str(task_spec.get("timeframe") or lane.timeframe),
                "lookback_months": int(task_spec.get("lookback_months") or runtime.lookback_months),
                "analysis_window_start": task_spec.get("analysis_window_start"),
                "analysis_window_end": task_spec.get("analysis_window_end"),
                "evidence_plan_id": evidence_plan.get("plan_id"),
                "evidence_role": evidence_plan.get("evidence_role"),
                "policy_assignment": copy.deepcopy(
                    task_spec.get("policy_assignment") or lane.policy_assignment
                ),
                "sweep_payload": (
                    _load_json(artifact_dir / "sweep-results.json")
                    if task_kind == "sweep_shard" and (artifact_dir / "sweep-results.json").is_file()
                    else None
                ),
        }
        _write_task_result_receipt(
            receipt_path,
            task_id=task_id,
            worker_result_sha256=result_identity,
            recorded_result=recorded,
        )
        return recorded
    if artifact_dir.exists():
        partial_root = lane_ctx.evals_dir / "partial-artifacts"
        partial_root.mkdir(parents=True, exist_ok=True)
        artifact_dir.replace(partial_root / f"{artifact_dir.name}-{_utc_stamp()}")
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
        validated_shard_payload = _validated_sweep_shard_payload(
            worker_result=result_payload,
            task_spec=task_spec,
        )
        sweep_payload = _rank_sweep_permutations(
            phase=phase,
            shard_results=[validated_shard_payload],
        )
        if runtime.retain_raw_lab_artifacts:
            _write_json(artifact_dir / "sweep-shard-result.json", validated_shard_payload)
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
        _write_json(artifact_dir / "fake-result.json", dict(result_payload))
        attempt_score = _fake_attempt_score(result_payload)
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
            "analysis_window_start": task_spec.get("analysis_window_start"),
            "analysis_window_end": task_spec.get("analysis_window_end"),
            "evidence_plan_id": evidence_plan.get("plan_id"),
            "evidence_role": evidence_plan.get("evidence_role"),
            "evidence_plan": evidence_plan or None,
            "execution_evidence": execution_evidence,
            "worker_id": lab_result.get("worker_id"),
            "worker_lease_id": lab_result.get("lease_id"),
            "lab_worker_result_sha256": result_identity,
            "run_status": "failed" if score_warning else "screened",
            "policy_assignment": copy.deepcopy(
                task_spec.get("policy_assignment") or lane.policy_assignment
            ),
        }
    )
    if score_warning:
        row["lab_scoring_warning"] = score_warning
    append_attempt_row(lane_ctx.attempts_path, row)
    if record.composite_score is not None and (
        lane.best_score is None or record.composite_score > lane.best_score
    ):
        lane.best_score = record.composite_score
        lane.best_attempt_id = record.attempt_id
    if render_progress:
        attempts.append(row)
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
    recorded = {
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
        "analysis_window_start": task_spec.get("analysis_window_start"),
        "analysis_window_end": task_spec.get("analysis_window_end"),
        "evidence_plan_id": evidence_plan.get("plan_id"),
        "evidence_role": evidence_plan.get("evidence_role"),
        "policy_assignment": copy.deepcopy(
            task_spec.get("policy_assignment") or lane.policy_assignment
        ),
        "sweep_payload": sweep_payload if task_kind == "sweep_shard" else None,
    }
    _write_task_result_receipt(
        receipt_path,
        task_id=task_id,
        worker_result_sha256=result_identity,
        recorded_result=recorded,
    )
    return recorded


def _is_failed_lab_result(lab_result: dict[str, Any]) -> bool:
    status = str(lab_result.get("status") or "").lower()
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    worker_status = str(result_payload.get("status") or "").lower()
    return status in {"failed", "error"} or worker_status in {"failed", "error"}


def _validated_no_valid_cell_terminal(
    lab_result: dict[str, Any], evidence_plan: dict[str, Any]
) -> dict[str, Any] | None:
    payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    worker_result = payload.get("result") if isinstance(payload.get("result"), dict) else payload
    terminal = worker_result.get("terminal_result")
    if not isinstance(terminal, dict) or terminal.get("outcome") != "no_valid_cell":
        return None
    if (
        terminal.get("schema") != "fuzzfolio-replay-terminal-result-v1"
        or terminal.get("status") != "nonviable"
    ):
        raise DurableExecutionError("malformed no_valid_cell terminal result")
    diagnostics = terminal.get("diagnostics")
    market_window = diagnostics.get("market_data_window") if isinstance(diagnostics, dict) else None
    try:
        filtered_bar_count = int(market_window.get("filtered_bar_count") or 0)
    except (AttributeError, TypeError, ValueError):
        filtered_bar_count = 0
    if (
        not isinstance(diagnostics, dict)
        or diagnostics.get("signal_count") != 0
        or diagnostics.get("resolved_trade_count_max") != 0
        or not isinstance(market_window, dict)
        or filtered_bar_count <= 0
    ):
        raise DurableExecutionError("no_valid_cell terminal result lacks canonical diagnostics")
    _validated_execution_evidence(terminal, evidence_plan)
    return terminal


def _record_lab_failure(
    *,
    config: AppConfig,
    lane_ctx: PlayHandContext,
    lane: LabLaneState,
    runtime: PlayHandLabRuntimeConfig,
    lab_result: dict[str, Any],
    reward_matrix: dict[str, Any] | None,
    terminal_outcome: dict[str, Any] | None = None,
    render_progress: bool = True,
) -> dict[str, Any]:
    task_id = str(lab_result.get("task_id") or "")
    task_spec = lane.task_specs.get(task_id, {})
    phase = str(task_spec.get("phase") or runtime.task_mode)
    task_kind = str(task_spec.get("task_kind") or runtime.task_mode)
    result_payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    error = str(result_payload.get("error") or lab_result.get("error") or "lab_worker_failed")
    is_nonviable = terminal_outcome is not None
    record_status = "nonviable" if is_nonviable else "failed"
    score_basis = "worker_no_valid_cell" if is_nonviable else "lab_worker_failed"
    artifact_key = canonical_sha256({"task_id": task_id})[-20:]
    artifact_dir = (lane_ctx.evals_dir / f"eval_lab_failed_{artifact_key}").resolve()
    result_identity = _worker_result_identity(lab_result)
    attempts = load_attempts(lane_ctx.attempts_path)
    matches = [
        row
        for row in attempts
        if str(row.get("lab_campaign_task_id") or "") == task_id
    ]
    if len(matches) > 1:
        raise DurableExecutionError(f"duplicate ledger rows exist for task {task_id}")
    recovered_row = matches[0] if matches else None
    if recovered_row is not None:
        if recovered_row.get("lab_worker_result_sha256") != result_identity:
            raise DurableExecutionError(
                f"ledger failure identity conflicts with duplicate task {task_id}"
            )
        artifact_dir = Path(str(recovered_row.get("artifact_dir") or "")).resolve(
            strict=False
        )
        if not artifact_dir.is_dir():
            raise DurableExecutionError(
                f"ledger artifact is missing for duplicate task {task_id}"
            )
    receipt_path = artifact_dir / "task-result-receipt.json"
    if receipt_path.is_file():
        receipt = _validate_task_result_receipt(
            receipt_path,
            task_id=task_id,
            worker_result_sha256=result_identity,
        )
        return dict(receipt["recorded_result"])
    if recovered_row is not None:
        row = recovered_row
        recorded = {
            "task_id": task_id,
            "attempt_id": row.get("attempt_id"),
            "artifact_dir": str(artifact_dir),
            "score": None,
            "score_basis": score_basis,
            "status": record_status,
            "phase": phase,
            "task_kind": task_kind,
        }
        _write_task_result_receipt(
            receipt_path,
            task_id=task_id,
            worker_result_sha256=result_identity,
            recorded_result=recorded,
        )
        return recorded
    if artifact_dir.exists():
        partial_root = lane_ctx.evals_dir / "partial-artifacts"
        partial_root.mkdir(parents=True, exist_ok=True)
        artifact_dir.replace(partial_root / f"{artifact_dir.name}-{_utc_stamp()}")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if runtime.retain_raw_lab_artifacts:
        _write_json(artifact_dir / "lab-result.json", lab_result)
    _write_json(
        artifact_dir / ("lab-terminal-result.json" if is_nonviable else "lab-failure.json"),
        {
            "task_id": task_id,
            "lane_id": lane.lane_id,
            "status": record_status,
            "error": error[:1000],
            "terminal_result": terminal_outcome,
            "worker_id": lab_result.get("worker_id"),
            "worker_lease_id": lab_result.get("lease_id"),
        },
    )
    play_hand_role = "lab_replay" if runtime.task_mode == "deep_replay" else "lab_smoke"
    attempt_score = AttemptScore(
        primary_score=None,
        composite_score=None,
        score_basis=score_basis,
        metrics={},
        best_summary={"status": record_status, "error": error[:500], "task_id": task_id},
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
        job_status=record_status,
        runner=PLAY_HAND_RUNNER,
        attempt_role=play_hand_role,
        attempt_decision=("nonviable_candidate" if is_nonviable else "failed_candidate"),
        attempt_decision_reasons=(
            ["worker_no_valid_cell"] if is_nonviable else ["play_hand_lab_worker_failed"]
        ),
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
            "lab_worker_result_sha256": result_identity,
            "run_status": record_status,
            "lab_terminal_outcome": terminal_outcome,
            "lab_failure": None if is_nonviable else {"error": error[:1000]},
        }
    )
    append_attempt_row(lane_ctx.attempts_path, row)
    if render_progress:
        attempts.append(row)
        render_progress_artifacts(
            attempts,
            lane.run_dir / "progress.png",
            run_metadata_path=lane.run_dir / "run-metadata.json",
            lower_is_better=config.research.plot_lower_is_better,
        )
    _append_event(
        lane_ctx,
        "lab_result",
        record_status,
        task_id=task_id,
        lane_id=lane.lane_id,
        task_phase=phase,
        task_kind=task_kind,
        worker_id=lab_result.get("worker_id"),
        lease_id=lab_result.get("lease_id"),
        artifact_dir=str(artifact_dir),
        attempt_id=record.attempt_id,
        error=error[:1000],
    )
    recorded = {
        "task_id": task_id,
        "attempt_id": record.attempt_id,
        "artifact_dir": str(artifact_dir),
        "score": None,
        "score_basis": score_basis,
        "status": record_status,
        "phase": phase,
        "task_kind": task_kind,
    }
    _write_task_result_receipt(
        receipt_path,
        task_id=task_id,
        worker_result_sha256=result_identity,
        recorded_result=recorded,
    )
    return recorded


def _render_lane_progress_artifacts(*, config: AppConfig, lane_ctx: PlayHandContext) -> None:
    attempts = load_attempts(lane_ctx.attempts_path)
    render_progress_artifacts(
        attempts,
        lane_ctx.run_dir / "progress.png",
        run_metadata_path=lane_ctx.run_dir / "run-metadata.json",
        lower_is_better=config.research.plot_lower_is_better,
    )


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
            "screen_anchor_mode": lane.screen_anchor_mode,
            "screen_analysis_window_start": lane.screen_analysis_window_start,
            "screen_analysis_window_end": lane.screen_analysis_window_end,
            "screen_anchor_offset_days": lane.screen_anchor_offset_days,
            "phase_rows": list(lane.phase_rows),
            "play_hand_phase_scores": dict(lane.phase_scores),
            "phase_started_at": dict(lane.phase_started_at),
            "phase_completed_at": dict(lane.phase_completed_at),
            "phase_task_counts": dict(lane.phase_task_counts),
            "phase_completed_task_counts": dict(lane.phase_completed_task_counts),
            "phase_failed_task_counts": dict(lane.phase_failed_task_counts),
            "phase_lifecycle_events": list(lane.phase_lifecycle_events),
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
    if _as_float(best.get("score")) is None:
        return None
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
        _set_lane_phase(lane, "lookback_skipped", event="phase_skipped", reason="no timing axes")
        _append_phase_row(lane, phase="lookback", status="skipped", detail="no timing axes")
        return []
    _set_lane_phase(lane, "lookback")
    analysis_window_start, analysis_window_end = _lane_screen_window(lane)
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
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
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
        _set_lane_phase(lane, "coarse_skipped", event="phase_skipped", reason="no numeric talib axes")
        _append_phase_row(lane, phase="coarse", status="skipped", detail="no numeric talib axes")
        return []
    _set_lane_phase(lane, phase)
    effective_runtime = runtime
    if budget is not None:
        effective_runtime = replace(runtime, max_sweep_permutations=max(int(budget), 1))
    analysis_window_start, analysis_window_end = _lane_screen_window(lane)
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
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
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
        _set_lane_phase(lane, "focused_skipped", event="phase_skipped", reason="no high-impact axes")
        _append_phase_row(lane, phase="focused", status="skipped", detail="no high-impact axes")
        return []
    _set_lane_phase(lane, "focused")
    analysis_window_start, analysis_window_end = _lane_screen_window(lane)
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
        analysis_window_start=analysis_window_start,
        analysis_window_end=analysis_window_end,
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
    _set_lane_phase(lane, "instrument_scout")
    tasks: list[dict[str, Any]] = []
    analysis_window_start, analysis_window_end = _runtime_as_of_window(
        runtime, runtime.validation_months
    )
    for instrument in scout_instruments:
        tasks.append(
            _make_deep_replay_task(
                lane,
                phase=f"instrument_scout_{instrument}_{runtime.validation_months}mo",
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
                profile_payload=lane.incumbent_profile_payload,
                profile_path=lane.incumbent_profile_path,
                profile_ref=lane.incumbent_profile_ref,
                instruments=[instrument],
                timeframe=lane.incumbent_timeframe or lane.timeframe,
                lookback_months=runtime.validation_months,
                analysis_window_start=analysis_window_start,
                analysis_window_end=analysis_window_end,
            )
        )
    return tasks


def _enqueue_validation_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> list[dict[str, Any]]:
    if lane.incumbent_profile_ref is None or lane.incumbent_profile_payload is None:
        return []
    _set_lane_phase(lane, "validation")
    phase = _validation_phase(runtime)
    analysis_window_start, analysis_window_end = _runtime_as_of_window(
        runtime, runtime.validation_months
    )
    return [
        _make_deep_replay_task(
            lane,
            phase=phase,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
            profile_payload=lane.incumbent_profile_payload,
            profile_path=lane.incumbent_profile_path,
            profile_ref=lane.incumbent_profile_ref,
            instruments=list(lane.incumbent_instruments or lane.instruments),
            timeframe=lane.incumbent_timeframe or lane.timeframe,
            lookback_months=runtime.validation_months,
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
        )
    ]


def _enqueue_final_stage(
    lane: LabLaneState,
    *,
    runtime: PlayHandLabRuntimeConfig,
    reward_matrix: dict[str, Any] | None,
    worker_contract_hash: str,
) -> list[dict[str, Any]]:
    if lane.incumbent_profile_ref is None or lane.incumbent_profile_payload is None:
        return []
    _set_lane_phase(lane, "scrutiny")
    analysis_window_start, analysis_window_end = _runtime_as_of_window(
        runtime, runtime.scrutiny_months
    )
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
            analysis_window_start=analysis_window_start,
            analysis_window_end=analysis_window_end,
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
    if any(row.get("status") == "nonviable" for row in lane.phase_results.get(phase, [])):
        _mark_lane_tombstoned(
            lane,
            lane_ctx=lane_ctx,
            reason="worker_no_valid_cell",
            outcome_category=TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
            reasons=[phase],
        )
        return []
    if _phase_failed(lane, phase):
        _mark_lane_tombstoned(
            lane,
            lane_ctx=lane_ctx,
            reason="lab_stage_worker_failed",
            outcome_category=TERMINAL_OUTCOME_INFRASTRUCTURE_FAILURE,
            reasons=[phase],
        )
        return []
    if phase not in {"lookback_timing", "coarse", "coarse_probe", "coarse_expand", "focused"} and recorded.get("score") is None:
        _mark_lane_tombstoned(
            lane,
            lane_ctx=lane_ctx,
            reason="canonical_score_missing",
            outcome_category=TERMINAL_OUTCOME_INFRASTRUCTURE_FAILURE,
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
                outcome_category=TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
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
            tasks = _enqueue_validation_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        return tasks

    if phase in {"lookback_timing", "coarse", "coarse_probe", "coarse_expand", "focused"}:
        payload = _merge_phase_sweep_receipts(lane, phase=phase)
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
            _append_phase_row(
                lane,
                phase=phase.replace("_", " "),
                status="nonviable",
                detail=str(payload.get("outcome") or "no_scored_permutation"),
            )
            _write_stage_metadata(lane, lane_ctx)
            if phase == "lookback_timing":
                return _enqueue_coarse_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                ) or _enqueue_validation_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            if phase in {"coarse", "coarse_expand", "coarse_probe"}:
                return _enqueue_validation_stage(
                    lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                )
            return _enqueue_validation_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
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
                outcome_category=TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
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
                ) or _enqueue_validation_stage(
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
            ) or _enqueue_validation_stage(
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
            return _enqueue_validation_stage(
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
            ) or _enqueue_validation_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )
        if phase == "focused":
            return _enqueue_validation_stage(
                lane,
                runtime=runtime,
                reward_matrix=reward_matrix,
                worker_contract_hash=worker_contract_hash,
            )

    if phase == _validation_phase(runtime):
        validation_score = _as_float(recorded.get("score"))
        lane.incumbent_score = validation_score
        lane.incumbent_phase = _validation_phase(runtime)
        _record_phase_score(lane, _validation_phase(runtime), validation_score)
        outcome = _validation_outcome(validation_score, runtime)
        _append_phase_row(
            lane,
            phase="validation",
            status="evaluated" if outcome.get("passed") else "failed",
            score=validation_score,
            detail=f"{runtime.validation_months}mo min={runtime.validation_min_score:g}",
        )
        _write_stage_metadata(lane, lane_ctx)
        if not outcome.get("passed"):
            _mark_lane_tombstoned(
                lane,
                lane_ctx=lane_ctx,
                reason=str(outcome.get("reason") or "validation_failed"),
                outcome_category=TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
                reasons=list(outcome.get("reasons") or []),
            )
            return []
        if lane.skip_focused_and_scout:
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
        ) or _enqueue_final_stage(
            lane,
            runtime=runtime,
            reward_matrix=reward_matrix,
            worker_contract_hash=worker_contract_hash,
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
        outcome = _lab_final_scrutiny_outcome(final_score, runtime)
        _append_phase_row(
            lane,
            phase="scrutiny",
            status="evaluated" if outcome.get("passed") else "failed",
            score=final_score,
            detail=(
                f"{runtime.scrutiny_months}mo min={runtime.final_min_score:g} "
                f"on {', '.join(lane.incumbent_instruments)}"
            ),
        )
        if outcome.get("passed"):
            _mark_lane_promoted(lane, lane_ctx=lane_ctx, final_score=final_score)
        else:
            _mark_lane_tombstoned(
                lane,
                lane_ctx=lane_ctx,
                reason=str(outcome.get("reason") or PLAY_HAND_FINAL_SCRUTINY_FAILED_REASON),
                outcome_category=TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
                reasons=list(outcome.get("reasons") or []),
            )
        return []
    return []


def _read_gateway_results(gateway: Any, *, limit: int) -> list[dict[str, Any]]:
    reader = getattr(gateway, "read_results", None)
    if callable(reader):
        return reader(limit=limit)
    return gateway.drain_results(limit=limit)


def _ack_gateway_results(gateway: Any, lease_ids: list[str]) -> int | None:
    acker = getattr(gateway, "ack_results", None)
    if callable(acker):
        acknowledged = acker(lease_ids)
        if acknowledged is None:
            return None
        if isinstance(acknowledged, bool) or not isinstance(acknowledged, int):
            raise DurableExecutionError("gateway returned an invalid acknowledgement count")
        return acknowledged
    return None


def _safe_ack_gateway_results(
    gateway: Any,
    campaign_ctx: PlayHandContext,
    *,
    lease_ids: list[str],
    task_id: str,
    require_exact_count: bool = False,
) -> bool:
    try:
        acknowledged = _ack_gateway_results(gateway, lease_ids)
        if require_exact_count and acknowledged != len(lease_ids):
            raise DurableExecutionError(
                "gateway acknowledgement count mismatch: "
                f"expected {len(lease_ids)}, observed {acknowledged!r}"
            )
        return True
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


def _enqueue_gateway_tasks_with_retries(
    gateway: Any,
    campaign_ctx: PlayHandContext,
    tasks: list[dict[str, Any]],
    *,
    reason: str,
    failure_limit: int,
    retry_base_seconds: float,
) -> dict[str, Any]:
    if not tasks:
        return {}
    max_failures = max(int(failure_limit), 1)
    retry_base = max(float(retry_base_seconds), 0.0)
    for attempt in range(1, max_failures + 1):
        try:
            result = gateway.enqueue_tasks(tasks)
            return result if isinstance(result, dict) else {}
        except requests.RequestException as exc:
            _append_event(
                campaign_ctx,
                "gateway",
                "task_enqueue_failed",
                reason=reason,
                error=str(exc)[:1000],
                attempt=attempt,
                failure_limit=max_failures,
                task_count=len(tasks),
            )
            if attempt >= max_failures:
                raise
            if retry_base > 0:
                time.sleep(min(retry_base * attempt, 30.0))
    return {}


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


def _lane_history_totals(
    lanes: list[LabLaneState],
    history: LabCampaignHistory | None = None,
) -> dict[str, Any]:
    history = history or LabCampaignHistory()
    retained_completed = sum(len(lane.completed_task_ids) for lane in lanes)
    retained_failed = sum(len(lane.failed_task_ids) for lane in lanes)
    retained_promoted = sum(1 for lane in lanes if lane.run_promoted)
    retained_tombstoned = sum(1 for lane in lanes if lane.tombstone_reason)
    best_scores = [lane.best_score for lane in lanes if lane.best_score is not None]
    if history.best_score is not None:
        best_scores.append(history.best_score)
    return {
        "lane_count": int(history.pruned_lane_count) + len(lanes),
        "retained_lane_count": len(lanes),
        "pruned_lane_count": int(history.pruned_lane_count),
        "total_tasks": int(history.pruned_task_count) + sum(len(lane.task_ids) for lane in lanes),
        "completed_tasks": int(history.pruned_completed_task_count) + retained_completed,
        "failed_tasks": int(history.pruned_failed_task_count) + retained_failed,
        "terminal_lanes": int(history.pruned_lane_count) + sum(1 for lane in lanes if lane.terminal),
        "promoted_lanes": int(history.pruned_promoted_lane_count) + retained_promoted,
        "tombstoned_lanes": int(history.pruned_tombstoned_lane_count) + retained_tombstoned,
        "best_score": max(best_scores, default=None),
    }


def _compact_terminal_lane_state(lane: LabLaneState) -> None:
    terminal_task_count = len(lane.completed_task_ids) + len(lane.failed_task_ids)
    if not lane.terminal and (not lane.task_ids or terminal_task_count < len(lane.task_ids)):
        return
    lane.profile_payload = None
    lane.incumbent_profile_payload = None
    lane.last_sweep_payload = None
    lane.instrument_scout_result = None
    lane.task_specs.clear()
    lane.phase_rows.clear()
    lane.phase_results.clear()


def _campaign_gateway_snapshot(
    snapshot: dict[str, Any] | None,
    *,
    metric_baseline: dict[str, int],
    lanes: list[LabLaneState],
    history: LabCampaignHistory | None = None,
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
    totals = _lane_history_totals(lanes, history)
    total_tasks = int(totals["total_tasks"])
    completed_tasks = int(totals["completed_tasks"])
    failed_tasks = int(totals["failed_tasks"])
    scoped["total_tasks"] = total_tasks
    scoped["completed_tasks"] = completed_tasks
    scoped["failed_tasks"] = failed_tasks
    scoped["live_tasks"] = max(total_tasks - completed_tasks - failed_tasks, 0)
    return scoped


LAB_BARRIER_BOX_WIDTH = 100


def _ascii_clip(value: Any, width: int, *, collapse_whitespace: bool = True) -> str:
    text = str(value or "")
    if collapse_whitespace:
        text = re.sub(r"\s+", " ", text).strip()
    else:
        text = re.sub(r"[\r\n\t]+", " ", text)
    text = text.encode("ascii", "replace").decode("ascii")
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def _box_row(
    text: Any,
    *,
    width: int = LAB_BARRIER_BOX_WIDTH,
    preserve_spacing: bool = False,
) -> str:
    inner = max(width - 4, 1)
    clipped = _ascii_clip(text, inner, collapse_whitespace=not preserve_spacing)
    return f"| {clipped:<{inner}} |"


def _box_rule(*, width: int = LAB_BARRIER_BOX_WIDTH) -> str:
    return "+" + ("-" * max(width - 2, 1)) + "+"


def _format_columns(values: list[tuple[Any, int]]) -> str:
    return " | ".join(_ascii_clip(value, width).ljust(width) for value, width in values)


def _percent(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0%"
    return f"{(numerator / denominator) * 100.0:.0f}%"


def _score_text(score: float | None) -> str:
    return f"{score:.2f}" if isinstance(score, (int, float)) else "-"


def _lane_terminal_count_for_log(lane: LabLaneState) -> int:
    return len(lane.completed_task_ids) + len(lane.failed_task_ids)


def _lane_run_status_for_log(lane: LabLaneState) -> str:
    if lane.terminal:
        if lane.run_promoted:
            return "promoted"
        if lane.tombstone_reason:
            return "tombstoned"
    terminal_count = _lane_terminal_count_for_log(lane)
    if terminal_count >= len(lane.task_ids) and lane.failed_task_ids:
        return "failed"
    if terminal_count >= len(lane.task_ids) and lane.task_ids:
        return "completed"
    if lane.task_ids:
        return "running"
    return "queued"


def _lane_is_active_for_log(lane: LabLaneState) -> bool:
    return bool(lane.task_ids) and _lane_terminal_count_for_log(lane) < len(lane.task_ids)


def _lane_best_score_for_log(lane: LabLaneState) -> float | None:
    candidates: list[float] = []
    if isinstance(lane.best_score, (int, float)):
        candidates.append(float(lane.best_score))
    for score in lane.phase_scores.values():
        if isinstance(score, (int, float)):
            candidates.append(float(score))
    return max(candidates) if candidates else None


def _lane_detail_for_log(lane: LabLaneState) -> str:
    if lane.tombstone_reason:
        return lane.tombstone_reason
    if lane.run_promoted:
        return lane.final_attempt_id or lane.best_attempt_id or "canonical"
    if lane.failed_task_ids:
        return f"failed={len(lane.failed_task_ids)}"
    if lane.incumbent_phase and lane.incumbent_phase != "scaffold":
        return f"incumbent={lane.incumbent_phase}"
    return lane.run_id


def _format_lane_symbols(lane: LabLaneState) -> str:
    symbols = list(lane.incumbent_instruments or lane.instruments)
    if len(symbols) > 3:
        return ",".join(symbols[:3]) + f"+{len(symbols) - 3}"
    return ",".join(symbols) if symbols else "-"


def _format_lab_barrier_snapshot(
    *,
    barrier_index: int,
    campaign_id: str,
    runtime: PlayHandLabRuntimeConfig,
    lanes: list[LabLaneState],
    tasks: list[dict[str, Any]],
    snapshot: dict[str, Any] | None,
    metric_baseline: dict[str, int],
    recorded_result_count: int,
    status: str | None = None,
    history: LabCampaignHistory | None = None,
) -> str:
    scoped_snapshot = _campaign_gateway_snapshot(
        snapshot,
        metric_baseline=metric_baseline,
        lanes=lanes,
        history=history,
    ) or {}
    totals = _lane_history_totals(lanes, history)
    total_tasks = int(totals["total_tasks"])
    completed_tasks = int(totals["completed_tasks"])
    failed_tasks = int(totals["failed_tasks"])
    active_lanes = [lane for lane in lanes if _lane_is_active_for_log(lane)]
    terminal_lanes = [lane for lane in lanes if lane.terminal]
    terminal_lane_count = int(totals["terminal_lanes"])
    promoted_lanes = int(totals["promoted_lanes"])
    tombstoned_lanes = int(totals["tombstoned_lanes"])
    best_score = totals["best_score"]
    worker_slots = int(scoped_snapshot.get("worker_slots") or 0)
    busy_slots = int(scoped_snapshot.get("busy_slots") or 0)
    worker_count = int(
        scoped_snapshot.get("online_worker_count")
        or scoped_snapshot.get("worker_count")
        or 0
    )
    busy_workers = int(scoped_snapshot.get("busy_worker_count") or 0)
    queued_tasks = int(scoped_snapshot.get("queued_tasks") or 0)
    live_tasks = int(scoped_snapshot.get("live_tasks") or 0)
    result_backlog = int(scoped_snapshot.get("result_backlog") or 0)
    gateway_done = int(scoped_snapshot.get("completed_tasks") or 0)
    gateway_failed = int(scoped_snapshot.get("failed_tasks") or 0)
    enqueued_delta = _metric_delta(scoped_snapshot, metric_baseline, "tasks_enqueued")
    accepted_delta = _metric_delta(scoped_snapshot, metric_baseline, "completions_accepted")
    result_loss_delta = _metric_delta(scoped_snapshot, metric_baseline, "results_dropped")
    incompatible_delta = _metric_delta(scoped_snapshot, metric_baseline, "incompatible_claims")
    raw_busy_phases = scoped_snapshot.get("busy_slots_by_phase")
    busy_phase_parts: list[str] = []
    if isinstance(raw_busy_phases, dict):
        for phase, count in sorted(raw_busy_phases.items(), key=lambda item: (-int(item[1] or 0), str(item[0]))):
            try:
                phase_count = int(count)
            except (TypeError, ValueError):
                continue
            if phase_count > 0:
                busy_phase_parts.append(f"{str(phase)[:24]}={phase_count}")

    header_status = f" status={status}" if status else ""
    lines = [
        _box_rule(),
        _box_row(
            f"PlayHand Massive v2 barrier #{barrier_index:04d}{header_status} "
            f"campaign={campaign_id} mode={runtime.campaign_mode} "
            f"target={runtime.target_runs or 'continuous'} active-runs={runtime.active_runs}"
        ),
        _box_row(
            "gateway "
            f"workers={busy_workers}/{worker_count} busy slots={busy_slots}/{worker_slots} "
            f"sat={_percent(busy_slots, worker_slots)} queued={queued_tasks} live={live_tasks} "
            f"done={gateway_done} failed={gateway_failed} result-backlog={result_backlog}"
        ),
    ]
    if busy_phase_parts:
        visible_phases = busy_phase_parts[:5]
        if len(busy_phase_parts) > len(visible_phases):
            visible_phases.append(f"+{len(busy_phase_parts) - len(visible_phases)} more")
        lines.append(_box_row("worker phases " + " ".join(visible_phases)))
    lines.extend(
        [
        _box_row(
            "lanes "
            f"created={int(totals['lane_count'])} active={len(active_lanes)} terminal={terminal_lane_count} "
            f"promoted={promoted_lanes} tombstoned={tombstoned_lanes} "
            f"tasks={completed_tasks + failed_tasks}/{total_tasks or len(tasks)} failed={failed_tasks} "
            f"recorded={recorded_result_count} best={_score_text(best_score)}"
        ),
        _box_row(
            "gateway deltas "
            f"enqueued={enqueued_delta} completions={accepted_delta} "
            f"dropped={result_loss_delta} incompatible-claims={incompatible_delta}"
        ),
        _box_rule(),
        _box_row(
            _format_columns(
                [
                    ("lane", 8),
                    ("phase", 13),
                    ("status", 10),
                    ("score", 6),
                    ("tasks", 7),
                    ("symbols", 13),
                    ("detail", 21),
                ]
            ),
            preserve_spacing=True,
        ),
        ]
    )
    lane_limit = max(int(runtime.barrier_lane_limit), 1)
    active_ordered_lanes = sorted(active_lanes, key=lambda lane: lane.lane_index)
    ordered_lanes = (
        active_ordered_lanes
        if active_ordered_lanes
        else sorted(
            lanes,
            key=lambda lane: (
                0 if not lane.terminal else 1,
                lane.lane_index,
            ),
        )
    )
    visible_lanes = ordered_lanes[:lane_limit]
    for lane in visible_lanes:
        lines.append(
            _box_row(
                _format_columns(
                    [
                        (lane.lane_id, 8),
                        (lane.current_phase, 13),
                        (_lane_run_status_for_log(lane), 10),
                        (_score_text(_lane_best_score_for_log(lane)), 6),
                        (f"{_lane_terminal_count_for_log(lane)}/{len(lane.task_ids)}", 7),
                        (_format_lane_symbols(lane), 13),
                        (_lane_detail_for_log(lane), 21),
                    ]
                ),
                preserve_spacing=True,
            )
        )
    hidden_count = max(len(ordered_lanes) - len(visible_lanes), 0)
    if hidden_count:
        lines.append(
            _box_row(
                f"... {hidden_count} more active lane(s) hidden; raise --barrier-lane-limit to show more ..."
            )
        )
    if active_ordered_lanes and terminal_lane_count:
        lines.append(
            _box_row(
                "terminal lanes summarized: "
                f"{terminal_lane_count} terminal, {promoted_lanes} promoted, "
                f"{tombstoned_lanes} tombstoned"
            )
        )
    if not lanes:
        lines.append(_box_row("No lanes prepared yet."))
    lines.append(_box_rule())
    return "\n".join(lines)


def _lab_event_lane_id(event: dict[str, Any]) -> str | None:
    lane_id = event.get("lane_id")
    if lane_id:
        return str(lane_id)
    run_id = str(event.get("run_id") or "")
    match = re.search(r"playhand-lab-lane-(\d+)", run_id)
    return f"lane_{match.group(1)}" if match else None


def _format_lab_event_fields(event: dict[str, Any], *, include_status: bool) -> str:
    fields: list[str] = []
    lane_id = _lab_event_lane_id(event)
    if lane_id:
        fields.append(f"lane={lane_id}")
    for key in ("task_id", "attempt_id", "task_kind", "worker_id", "lease_id"):
        value = event.get(key)
        if value:
            fields.append(f"{key}={_ascii_clip(value, 48)}")
    task_phase = event.get("task_phase")
    if task_phase:
        fields.append(f"task_phase={_ascii_clip(task_phase, 48)}")
    score = event.get("score")
    if isinstance(score, (int, float)):
        fields.append(f"score={score:.4f}")
    if include_status:
        fields.insert(0, f"event={event.get('phase')}/{event.get('status')}")
    return " ".join(fields)


def _format_lab_event_notice(event: dict[str, Any]) -> str | None:
    phase = str(event.get("phase") or "")
    status = str(event.get("status") or "")
    error = event.get("error") or event.get("reason") or event.get("scoring_warning")
    important_statuses = {
        "failed",
        "baseline_snapshot_failed",
        "final_snapshot_failed",
        "initial_snapshot_failed",
        "interrupted",
        "restarted",
        "result_loss_detected",
        "result_read_failed",
        "snapshot_failed",
    }
    is_important = status in important_statuses or status.endswith("_failed")
    if phase == "campaign" and status in {"gateway_restarted", "gateway_unreachable", "result_loss", "timeout"}:
        is_important = True
    if not is_important:
        return None
    label = f"! {phase} {status}"
    fields = _format_lab_event_fields(event, include_status=False)
    detail = f" reason={_ascii_clip(error, 120)}" if error else ""
    return _ascii_clip(f"{label} {fields}{detail}", 240)


def _format_lab_stream_event(event: dict[str, Any]) -> str:
    phase = str(event.get("phase") or "")
    status = str(event.get("status") or "")
    fields = _format_lab_event_fields(event, include_status=False)
    error = event.get("error") or event.get("reason") or event.get("scoring_warning")
    detail = f" detail={_ascii_clip(error, 120)}" if error else ""
    return _ascii_clip(f"{phase} {status} {fields}{detail}", 240)


def _configure_lab_event_output(ctx: PlayHandContext, runtime: PlayHandLabRuntimeConfig) -> None:
    if runtime.log_mode == "quiet":
        ctx.event_print_mode = "quiet"
        ctx.event_formatter = None
        return
    ctx.event_print_mode = "formatted"
    ctx.event_formatter = (
        _format_lab_stream_event
        if runtime.log_mode == "stream"
        else _format_lab_event_notice
    )


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
    history: LabCampaignHistory | None = None,
) -> dict[str, Any]:
    totals = _lane_history_totals(lanes, history)
    total_tasks = int(totals["total_tasks"])
    completed_tasks = int(totals["completed_tasks"])
    failed_tasks = int(totals["failed_tasks"])
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
        "as_of_date": runtime.as_of_date,
        "lake_manifest_sha256": runtime.lake_manifest_sha256,
        "research_generation_id": runtime.research_generation_id,
        "level_c_protocol_id": runtime.level_c_protocol_id,
        "cutoff_key": runtime.cutoff_key,
        "expected_seed_plan_sha256": runtime.expected_seed_plan_sha256,
        "formal_historical_level_c": bool(runtime.as_of_date),
        "lane_count": int(totals["lane_count"]),
        "retained_lane_count": int(totals["retained_lane_count"]),
        "pruned_lane_count": int(totals["pruned_lane_count"]),
        "total_tasks": total_tasks,
        "completed_tasks": completed_tasks,
        "failed_tasks": failed_tasks,
        "recorded_result_count": total_recorded_results,
        "recorded_results_sample_limit": max(int(SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT), 0),
        "recorded_results_truncated": total_recorded_results > len(recorded_results),
        "best_score": totals["best_score"],
        "lanes_truncated": int(totals["pruned_lane_count"]) > 0,
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
                "terminal_outcome_category": lane.terminal_outcome_category,
                "phase_scores": dict(lane.phase_scores),
                "phase_started_at": dict(lane.phase_started_at),
                "phase_completed_at": dict(lane.phase_completed_at),
                "phase_task_counts": dict(lane.phase_task_counts),
                "phase_completed_task_counts": dict(lane.phase_completed_task_counts),
                "phase_failed_task_counts": dict(lane.phase_failed_task_counts),
                "phase_lifecycle_events": list(lane.phase_lifecycle_events),
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


def _historical_lane_has_legitimate_terminal_outcome(lane: LabLaneState) -> bool:
    """Whether a formal lane reached a research outcome without operational loss."""
    if not lane.terminal or lane.failed_task_ids:
        return False
    if not lane.task_ids or not set(lane.task_ids).issubset(lane.completed_task_ids):
        return False
    return lane.terminal_outcome_category in {
        TERMINAL_OUTCOME_PROMOTED,
        TERMINAL_OUTCOME_RESEARCH_NONVIABLE,
    }


def _historical_campaign_has_legitimate_terminal_outcomes(
    lanes: list[LabLaneState],
    *,
    runtime: PlayHandLabRuntimeConfig,
) -> bool:
    target_runs = runtime.target_runs
    if (
        not runtime.as_of_date
        or runtime.campaign_mode != "finite"
        or isinstance(target_runs, bool)
        or not isinstance(target_runs, int)
        or target_runs <= 0
        or len(lanes) != target_runs
    ):
        return False
    return all(_historical_lane_has_legitimate_terminal_outcome(lane) for lane in lanes)


def _finalize_historical_campaign_status(
    status: str,
    *,
    lanes: list[LabLaneState],
    runtime: PlayHandLabRuntimeConfig,
) -> tuple[str, str | None]:
    if not runtime.as_of_date:
        return status, None
    if status == "completed" and _historical_campaign_has_legitimate_terminal_outcomes(
        lanes,
        runtime=runtime,
    ):
        return status, None

    reason = (
        "historical_campaign_stopped"
        if status == "stopped"
        else "historical_campaign_incomplete"
    )
    for lane in lanes:
        if not lane.run_promoted:
            continue
        lane.run_promoted = False
        lane.terminal = True
        lane.tombstone_reason = reason
        lane.terminal_outcome_category = TERMINAL_OUTCOME_INCOMPLETE
        if reason not in lane.tombstone_reasons:
            lane.tombstone_reasons.append(reason)
        _set_lane_phase(
            lane,
            "incomplete",
            event="historical_promotion_revoked",
            reason=reason,
        )
    return "failed", reason


def preflight_play_hand_lab(runtime: PlayHandLabRuntimeConfig) -> dict[str, Any]:
    """Validate a PlayHand launch without creating state or enqueueing work."""
    runtime = _normalize_runtime(runtime)
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    gateway = LabGatewayClient(base_url=runtime.gateway_url, token=runtime.gateway_token)
    worker_contract_hash = (
        str(runtime.worker_contract_hash)
        if runtime.as_of_date
        else _resolve_worker_contract_hash(config=config, runtime=runtime)
    )
    campaign_id = runtime.campaign_id or _campaign_run_id()
    campaign_dir = _derived_campaign_root(config) / campaign_id
    if runtime.as_of_date:
        _reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)

    health = gateway.health()
    if not health.get("ok"):
        raise RuntimeError(f"Lab gateway health check failed: {health}")

    campaign_ctx = _campaign_context(
        config=config,
        cli=cli,
        campaign_id=campaign_id,
        campaign_dir=campaign_dir,
        runtime=runtime,
    )
    seed_indicators, seed_plan, seed_plan_path = _seed_indicators(
        config=config,
        cli=cli,
        campaign_ctx=campaign_ctx,
        runtime=runtime,
        emit_events=False,
    )
    if runtime.as_of_date and (seed_plan is None or seed_plan_path is None):
        raise RuntimeError("Historical PlayHand did not load its frozen Atlas seed plan.")
    campaign_policy = (
        validate_seed_plan_campaign_policy(seed_plan)
        if isinstance(seed_plan, dict)
        else None
    )
    play_hand_reward_matrix(runtime.max_reward_r)
    return {
        "campaign_id": campaign_id,
        "campaign_dir": str(campaign_dir),
        "gateway_ok": True,
        "seed_indicator_count": len(seed_indicators),
        "seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
        "worker_contract_hash": worker_contract_hash,
        "campaign_policy_bound": campaign_policy is not None,
    }


def cmd_play_hand_lab(runtime: PlayHandLabRuntimeConfig | None = None) -> int:
    runtime = _normalize_runtime(runtime or PlayHandLabRuntimeConfig())
    config = load_config()
    if runtime.as_of_date:
        if not _is_phase3_formal_runtime(runtime):
            if runtime.execution_plan_path is None:
                raise ValueError("Historical PlayHand requires one authoritative execution plan.")
            _expected_arguments, authoritative_plan = validate_executor_runtime_binding(
                runtime.execution_plan_path,
                executor="playhand",
                observed={
                    **asdict(runtime),
                    "execution_plan_path": runtime.execution_plan_path,
                },
                config=config,
            )
            authoritative_root = Path(
                str((authoritative_plan.get("generation") or {}).get("active_runs_root") or "")
            ).resolve(strict=False)
            if config.runs_root.resolve(strict=False) != authoritative_root:
                raise ValueError("Historical PlayHand config runs_root conflicts with authoritative execution plan.")
            bound_contract = authoritative_plan.get("bound_contract") or {}
            validate_profile_model_source_lock(
                bound_contract.get("profile_model_source_lock") or {},
                _trading_dashboard_root(config=config, runtime=runtime),
            )
    cli = FuzzfolioCli(config.fuzzfolio)
    gateway = LabGatewayClient(base_url=runtime.gateway_url, token=runtime.gateway_token)
    worker_contract_hash = (
        str(runtime.worker_contract_hash)
        if runtime.as_of_date
        else _resolve_worker_contract_hash(config=config, runtime=runtime)
    )
    if not runtime.as_of_date and worker_contract_hash and worker_contract_hash != runtime.worker_contract_hash:
        runtime = replace(runtime, worker_contract_hash=worker_contract_hash)
    started_at = _now_iso()
    campaign_id = runtime.campaign_id or _campaign_run_id()
    campaign_dir = _derived_campaign_root(config) / campaign_id
    state_path = campaign_dir / "play-hand-lab-state.json"
    journal_path = campaign_dir / "play-hand-lab-execution-journal.json"
    if runtime.as_of_date:
        _reject_existing_historical_campaign_path(campaign_dir, runtime=runtime)
    campaign_ctx = _campaign_context(
        config=config,
        cli=cli,
        campaign_id=campaign_id,
        campaign_dir=campaign_dir,
        runtime=runtime,
    )
    _configure_lab_event_output(campaign_ctx, runtime)
    campaign_dir.mkdir(
        parents=True,
        exist_ok=not bool(runtime.as_of_date) or bool(runtime.resume),
    )
    journal = DurableExecutionJournal(
        journal_path,
        execution_id=str(runtime.execution_plan_id or campaign_id),
        lineage=_campaign_state_lineage(runtime, campaign_id),
    )
    journal_payload = journal.load(create=not bool(runtime.resume))
    durable_tasks_by_id = journal_payload["tasks"]
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
        if runtime.as_of_date:
            if seed_plan is None or seed_plan_path is None:
                raise RuntimeError("Historical PlayHand did not load its frozen Atlas seed plan.")
            _write_campaign_metadata(
                campaign_ctx,
                runtime=runtime,
                status="starting",
                started_at=started_at,
                extra={
                    "play_hand_seed_plan_path": str(seed_plan_path.resolve()),
                    "play_hand_seed_plan_sha256": _file_sha256(seed_plan_path),
                },
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
    campaign_policy = (
        validate_seed_plan_campaign_policy(seed_plan)
        if isinstance(seed_plan, dict)
        else None
    )
    reward_matrix = play_hand_reward_matrix(runtime.max_reward_r)
    lanes: list[LabLaneState] = []
    tasks: list[dict[str, Any]] = []
    lanes_by_task: dict[str, LabLaneState] = {}
    lane_contexts: dict[str, PlayHandContext] = {}
    history = LabCampaignHistory()
    target_runs = runtime.target_runs if runtime.campaign_mode == "finite" else None
    active_runs = max(int(runtime.active_runs or 1), 1)
    observed_worker_slots = 0
    next_lane_index = 0
    reserved_lane_indices: list[int] = []
    recorded_result_count = 0
    if runtime.resume:
        lanes, history, next_lane_index, reserved_lane_indices, recorded_result_count = _load_campaign_state(
            state_path,
            runtime=runtime,
            campaign_id=campaign_id,
        )
        for lane in lanes:
            lane_cli = FuzzfolioCli(config.fuzzfolio)
            lane_ctx = _new_context(
                config=config,
                cli=lane_cli,
                run_id=lane.run_id,
                run_dir=lane.run_dir,
                runtime=runtime,
            )
            _configure_lab_event_output(lane_ctx, runtime)
            lane_contexts[lane.run_id] = lane_ctx
            for task_id in lane.task_ids:
                lanes_by_task[task_id] = lane
        tasks = [
            _attach_task_profile_snapshots(dict(item["payload"]), campaign_dir)
            for _task_id, item in sorted(durable_tasks_by_id.items())
            if isinstance(item, dict) and item.get("status") != "terminal"
        ]
    history.campaign_policy_state = _campaign_policy_state(
        campaign_policy,
        runtime=runtime,
        persisted=history.campaign_policy_state,
    )

    def persist_campaign_state() -> None:
        _write_campaign_state(
            state_path,
            runtime=runtime,
            campaign_id=campaign_id,
            lanes=lanes,
            history=history,
            next_lane_index=next_lane_index,
            reserved_lane_indices=reserved_lane_indices,
            recorded_result_count=recorded_result_count,
        )

    def apply_journal_batch(
        *,
        registrations: list[tuple[str, dict[str, Any]]] | None = None,
        completions: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> None:
        detached_registrations = [
            (task_id, _detach_task_profile_snapshots(task, campaign_dir))
            for task_id, task in (registrations or ())
        ]
        updated = journal.apply_batch(
            registrations=detached_registrations,
            completions=completions or (),
        )
        durable_tasks_by_id.clear()
        durable_tasks_by_id.update(updated["tasks"])

    if runtime.resume:
        recovered_tasks: list[dict[str, Any]] = []
        for lane in lanes:
            for task_id in list(lane.task_ids):
                terminal = durable_tasks_by_id.get(task_id)
                if not isinstance(terminal, dict) or terminal.get("status") != "terminal":
                    terminal = None
                if terminal is None:
                    if task_id in lane.completed_task_ids or task_id in lane.failed_task_ids:
                        raise DurableExecutionError(
                            f"campaign state marks task terminal without a journal receipt: {task_id}"
                        )
                    continue
                terminal_receipt = terminal.get("terminal_receipt") or {}
                receipt_payload = _validate_task_result_receipt_payload(
                    terminal_receipt.get("payload"),
                    task_id=task_id,
                )
                recorded = dict(receipt_payload["recorded_result"])
                local_receipt = _validate_task_result_receipt(
                    Path(str(recorded.get("artifact_dir") or "")) / "task-result-receipt.json",
                    task_id=task_id,
                )
                if local_receipt != receipt_payload:
                    raise DurableExecutionError(
                        f"journal and artifact receipts conflict for task {task_id}"
                    )
                if history.campaign_policy_state is not None and recorded.get(
                    "policy_assignment"
                ) != lane.policy_assignment:
                    raise DurableExecutionError(
                        f"terminal receipt policy assignment mismatch: {task_id}"
                    )
                receipt_derived_tasks = _validated_receipt_derived_tasks(
                    receipt_payload,
                    task_id=task_id,
                )
                already_terminal = (
                    task_id in lane.completed_task_ids or task_id in lane.failed_task_ids
                )
                if already_terminal:
                    if receipt_derived_tasks:
                        recovered_tasks.extend(receipt_derived_tasks)
                    continue
                if recorded.get("status") == "failed":
                    lane.failed_task_ids.add(task_id)
                else:
                    lane.completed_task_ids.add(task_id)
                _refresh_lane_phase_result_counts(lane, task_id=task_id)
                recomputed_tasks = _advance_lane_after_result(
                    config=config,
                    lane_ctx=lane_contexts[lane.run_id],
                    lane=lane,
                    runtime=runtime,
                    reward_matrix=reward_matrix,
                    worker_contract_hash=worker_contract_hash,
                    recorded=recorded,
                )
                if (
                    receipt_derived_tasks is not None
                    and receipt_derived_tasks != recomputed_tasks
                ):
                    raise DurableExecutionError(
                        f"derived task receipt conflicts with recovered transition: {task_id}"
                    )
                recovered_tasks.extend(
                    receipt_derived_tasks
                    if receipt_derived_tasks is not None
                    else recomputed_tasks
                )
        recovered_registrations: list[tuple[str, dict[str, Any]]] = []
        for task in recovered_tasks:
            task_id = str(task["task_id"])
            existing_durable_task = durable_tasks_by_id.get(task_id)
            detached_task = _detach_task_profile_snapshots(task, campaign_dir)
            if existing_durable_task is not None:
                if (
                    not isinstance(existing_durable_task, dict)
                    or existing_durable_task.get("payload_sha256")
                    != journal.task_payload_sha256(detached_task)
                ):
                    raise DurableExecutionError(
                        f"task payload conflicts with durable graph: {task_id}"
                    )
            else:
                recovered_registrations.append((task_id, task))
            if not any(
                str(existing.get("task_id") or "") == str(task["task_id"])
                for existing in tasks
            ):
                tasks.append(task)
            lane = next(
                candidate
                for candidate in lanes
                if candidate.lane_id == str(task.get("lane_id") or "")
            )
            lanes_by_task[str(task["task_id"])] = lane
        if recovered_registrations:
            apply_journal_batch(registrations=recovered_registrations)
        if history.campaign_policy_state is not None:
            history.campaign_policy_state = (
                _recompute_campaign_policy_state_from_durable_lanes(
                    history.campaign_policy_state,
                    lanes=lanes,
                    unresolved_tasks=tasks,
                    durable_tasks_by_id=durable_tasks_by_id,
                    pruned_lane_count=history.pruned_lane_count,
                )
            )
        persist_campaign_state()

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
        run_id = _lane_run_id(
            lane_index,
            campaign_id=campaign_ctx.run_id if runtime.as_of_date else None,
        )
        run_dir = config.runs_root / run_id
        lane_cli = FuzzfolioCli(config.fuzzfolio)
        lane_ctx = _new_context(
            config=config,
            cli=lane_cli,
            run_id=run_id,
            run_dir=run_dir,
            runtime=runtime,
        )
        _configure_lab_event_output(lane_ctx, runtime)
        lane_ctx.profiles_dir.mkdir(parents=True, exist_ok=True)
        lane_ctx.evals_dir.mkdir(parents=True, exist_ok=True)
        lane = LabLaneState(
            lane_id=lane_id,
            lane_index=lane_index,
            run_id=run_id,
            run_dir=run_dir,
        )
        policy_state = history.campaign_policy_state
        if policy_state is not None:
            if not isinstance(seed_plan, dict):
                raise DurableExecutionError("policy-honest campaign is missing its v2 seed plan")
            deal, lane.policy_assignment = _select_policy_lane_deal(
                config=config,
                runtime=runtime,
                seed_indicators=seed_indicators,
                seed_plan=seed_plan,
                lane_index=lane_index,
                policy_state=policy_state,
            )
            policy_lane = str(lane.policy_assignment["policy_lane"])
            if deal is None:
                _record_policy_assignment(
                    policy_state,
                    lane=policy_lane,
                    cap_decision=None,
                    exhaustion_outcome=str(lane.policy_assignment["policy_outcome_type"]),
                )
                lane.terminal = True
                lane.tombstone_reason = str(lane.policy_assignment["policy_outcome_type"])
                lane.tombstone_reasons.append(lane.tombstone_reason)
                lane.terminal_outcome_category = POLICY_EXHAUSTION_OUTCOME
                _set_lane_phase(
                    lane,
                    POLICY_EXHAUSTION_OUTCOME,
                    event="policy_lane_exhausted",
                    policy_assignment=copy.deepcopy(lane.policy_assignment),
                )
                _write_lane_metadata(
                    lane,
                    campaign_ctx=campaign_ctx,
                    runtime=runtime,
                    status=POLICY_EXHAUSTION_OUTCOME,
                    started_at=started_at,
                    extra={
                        "policy_assignment": copy.deepcopy(lane.policy_assignment),
                        "campaign_policy_accounting": copy.deepcopy(policy_state),
                    },
                )
                _append_event(
                    campaign_ctx,
                    lane_id,
                    POLICY_EXHAUSTION_OUTCOME,
                    lane_run_id=run_id,
                    policy_assignment=copy.deepcopy(lane.policy_assignment),
                )
                return lane, lane_ctx
            lane_rng = random.Random(f"play-hand-lab:{runtime.seed}:{lane_index}:profile")
        else:
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
        if policy_state is not None:
            _record_policy_assignment(
                policy_state,
                lane=str(lane.policy_assignment["policy_lane"]),
                cap_decision=dict(lane.policy_assignment["cap_decision"]),
            )
        _sample_lane_screen_anchor(lane, runtime)
        _write_lane_metadata(
            lane,
            campaign_ctx=campaign_ctx,
            runtime=runtime,
            status="queued",
            started_at=started_at,
            extra={
                **_indicator_deal_metadata(
                    deal.get("indicator_deal") if isinstance(deal, dict) else None
                ),
                "play_hand_seed_plan_path": str(seed_plan_path) if seed_plan_path else None,
                "play_hand_seed_plan_sha256": (
                    _file_sha256(seed_plan_path) if seed_plan_path else None
                ),
                "play_hand_seed_plan_loaded": seed_plan is not None,
                "reward_matrix": reward_matrix,
                "required_worker_contract_hash": worker_contract_hash,
                "required_worker_contract_schema": runtime.worker_contract_schema,
                "policy_assignment": copy.deepcopy(lane.policy_assignment),
                "campaign_policy_accounting": (
                    copy.deepcopy(policy_state) if policy_state is not None else None
                ),
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
            screen_analysis_window_start=lane.screen_analysis_window_start,
            screen_analysis_window_end=lane.screen_analysis_window_end,
            policy_assignment=copy.deepcopy(lane.policy_assignment),
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
        if count == 1 or history.campaign_policy_state is not None:
            # Cap accounting is a mutable campaign-level reservation. Keep v2
            # preparation ordered so a crash can only recover the same lane.
            return [prepare_lane(start_index + offset) for offset in range(count)]
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

    def created_run_count() -> int:
        return max(next_lane_index, int(history.pruned_lane_count) + len(lanes))

    def total_task_count() -> int:
        return int(history.pruned_task_count) + sum(len(lane.task_ids) for lane in lanes)

    def completed_task_count() -> int:
        return int(history.pruned_completed_task_count) + sum(len(lane.completed_task_ids) for lane in lanes)

    def failed_task_count() -> int:
        return int(history.pruned_failed_task_count) + sum(len(lane.failed_task_ids) for lane in lanes)

    def can_create_more() -> bool:
        return runtime.campaign_mode == "continuous" or target_runs is None or created_run_count() < target_runs

    def top_up_run_count() -> int:
        deficit = max(active_runs - active_lane_count(), 0)
        if deficit <= 0 or not can_create_more():
            return 0
        if target_runs is not None:
            deficit = min(deficit, max(target_runs - created_run_count(), 0))
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
        apply_journal_batch(
            registrations=[(str(task["task_id"]), task) for task in new_tasks]
        )
        _lane_allocation_checkpoint("after_task_registration")
        for lane in new_lanes:
            if lane.lane_index in reserved_lane_indices:
                reserved_lane_indices.remove(lane.lane_index)
        persist_campaign_state()
        _lane_allocation_checkpoint("before_gateway_enqueue")
        enqueue_result = _enqueue_gateway_tasks_with_retries(
            gateway,
            campaign_ctx,
            new_tasks,
            reason="lane_top_up",
            failure_limit=runtime.enqueue_failure_limit,
            retry_base_seconds=runtime.enqueue_retry_base_seconds,
        )
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
            total_enqueued_task_count=total_task_count(),
            created_run_count=created_run_count(),
            active_run_count=active_lane_count(),
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="running",
            started_at=started_at,
            extra={
                "enqueued_task_count": total_task_count(),
                "created_run_count": created_run_count(),
                "active_run_count": active_lane_count(),
            },
        )

    def enqueue_existing_tasks(
        new_tasks: list[dict[str, Any]],
        *,
        reason: str,
        journal_registered: bool = False,
        state_persisted: bool = False,
    ) -> None:
        if not new_tasks:
            return
        if not journal_registered:
            apply_journal_batch(
                registrations=[(str(task["task_id"]), task) for task in new_tasks]
            )
        if not state_persisted:
            persist_campaign_state()
        enqueue_result = _enqueue_gateway_tasks_with_retries(
            gateway,
            campaign_ctx,
            new_tasks,
            reason=reason,
            failure_limit=runtime.enqueue_failure_limit,
            retry_base_seconds=runtime.enqueue_retry_base_seconds,
        )
        existing_task_ids = {str(task.get("task_id") or "") for task in tasks}
        tasks.extend(
            task
            for task in new_tasks
            if str(task.get("task_id") or "") not in existing_task_ids
        )
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
            total_enqueued_task_count=total_task_count(),
            active_run_count=active_lane_count(),
        )
        _write_campaign_metadata(
            campaign_ctx,
            runtime=runtime,
            status="running",
            started_at=started_at,
            extra={
                "enqueued_task_count": total_task_count(),
                "created_run_count": created_run_count(),
                "active_run_count": active_lane_count(),
            },
        )

    def create_and_enqueue_more() -> int:
        nonlocal next_lane_index
        count = top_up_run_count()
        if count <= 0:
            return 0
        start_index = next_lane_index
        new_indices = list(range(start_index, start_index + count))
        reserved_lane_indices.extend(new_indices)
        next_lane_index += count
        persist_campaign_state()
        _lane_allocation_checkpoint("after_index_reservation")
        new_lanes = prepare_lanes(start_index, count)
        persist_campaign_state()
        _lane_allocation_checkpoint("after_lane_registration")
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

    recorded_results: list[dict[str, Any]] = []
    deadline = None if runtime.campaign_mode == "continuous" else time.monotonic() + runtime.max_wait_seconds
    last_snapshot: dict[str, Any] | None = None
    initial_gateway_id: str | None = None
    gateway_restarted = False
    result_loss_detected = False
    gateway_unreachable = False
    interrupted = False
    consecutive_result_read_failures = 0
    barrier_index = 0
    last_barrier_at = 0.0
    dirty_progress_run_ids: set[str] = set()
    last_progress_render_at = 0.0

    def mark_progress_dirty(lane: LabLaneState) -> None:
        dirty_progress_run_ids.add(lane.run_id)

    def flush_dirty_progress(*, force: bool = False) -> None:
        nonlocal last_progress_render_at
        if not dirty_progress_run_ids:
            return
        now = time.monotonic()
        if not force and (now - last_progress_render_at) < runtime.barrier_interval_seconds:
            return
        last_progress_render_at = now
        for run_id in sorted(list(dirty_progress_run_ids)):
            lane_ctx = lane_contexts.get(run_id)
            if lane_ctx is None:
                dirty_progress_run_ids.discard(run_id)
                continue
            try:
                _render_lane_progress_artifacts(config=config, lane_ctx=lane_ctx)
            except Exception as exc:
                _append_event(
                    campaign_ctx,
                    "progress",
                    "render_failed",
                    run_id=run_id,
                    error=str(exc)[:1000],
                )
            else:
                dirty_progress_run_ids.discard(run_id)

    def terminal_lane_ready_for_retention(lane: LabLaneState) -> bool:
        return bool(lane.task_ids) and (lane.terminal or lane_terminal_count(lane) >= len(lane.task_ids))

    def render_before_prune(lane: LabLaneState) -> None:
        if lane.run_id not in dirty_progress_run_ids:
            return
        lane_ctx = lane_contexts.get(lane.run_id)
        if lane_ctx is None:
            dirty_progress_run_ids.discard(lane.run_id)
            return
        try:
            _render_lane_progress_artifacts(config=config, lane_ctx=lane_ctx)
        except Exception as exc:
            _append_event(
                campaign_ctx,
                "progress",
                "render_failed",
                run_id=lane.run_id,
                error=str(exc)[:1000],
            )
        dirty_progress_run_ids.discard(lane.run_id)

    def prune_terminal_lane_history() -> None:
        retention = max(int(runtime.terminal_lane_retention), 0)
        terminal_lanes = [lane for lane in lanes if terminal_lane_ready_for_retention(lane)]
        terminal_task_ids = {
            task_id
            for lane in terminal_lanes
            for task_id in lane.task_ids
            if task_id in lane.completed_task_ids or task_id in lane.failed_task_ids
        }
        if terminal_task_ids:
            tasks[:] = [
                task
                for task in tasks
                if str(task.get("task_id") or "") not in terminal_task_ids
            ]
        for lane in terminal_lanes:
            _compact_terminal_lane_state(lane)
        overflow = len(terminal_lanes) - retention
        if overflow <= 0:
            return
        prune_ids = {
            lane.run_id
            for lane in sorted(terminal_lanes, key=lambda candidate: candidate.lane_index)[:overflow]
        }
        retained_lanes: list[LabLaneState] = []
        pruned_task_ids: set[str] = set()
        for lane in lanes:
            if lane.run_id not in prune_ids:
                retained_lanes.append(lane)
                continue
            render_before_prune(lane)
            history.pruned_lane_count += 1
            history.pruned_task_count += len(lane.task_ids)
            history.pruned_completed_task_count += len(lane.completed_task_ids)
            history.pruned_failed_task_count += len(lane.failed_task_ids)
            if lane.run_promoted:
                history.pruned_promoted_lane_count += 1
            if lane.tombstone_reason:
                history.pruned_tombstoned_lane_count += 1
            if lane.best_score is not None:
                history.best_score = (
                    lane.best_score
                    if history.best_score is None
                    else max(history.best_score, lane.best_score)
                )
            pruned_task_ids.update(lane.task_ids)
            lane_contexts.pop(lane.run_id, None)
            dirty_progress_run_ids.discard(lane.run_id)
        if len(retained_lanes) != len(lanes):
            lanes[:] = retained_lanes
        for task_id in pruned_task_ids:
            lanes_by_task.pop(task_id, None)

    def emit_barrier_snapshot(*, force: bool = False, status: str | None = None) -> None:
        nonlocal barrier_index, last_barrier_at
        if runtime.log_mode != "barrier":
            return
        now = time.monotonic()
        if not force and (now - last_barrier_at) < runtime.barrier_interval_seconds:
            return
        barrier_index += 1
        last_barrier_at = now
        print(
            _format_lab_barrier_snapshot(
                barrier_index=barrier_index,
                campaign_id=campaign_ctx.run_id,
                runtime=runtime,
                lanes=lanes,
                tasks=tasks,
                snapshot=last_snapshot,
                metric_baseline=gateway_metric_baseline,
                recorded_result_count=recorded_result_count,
                status=status,
                history=history,
            ),
            flush=True,
        )

    def process_result_batch(
        result_batch: list[dict[str, Any]],
        *,
        defer_enqueues: bool = False,
    ) -> list[dict[str, Any]]:
        nonlocal recorded_result_count
        ack_lease_ids: list[str] = []
        deferred_tasks: list[dict[str, Any]] = []
        deferred_tasks_by_reason: dict[str, list[dict[str, Any]]] = {}
        pending_completions: list[tuple[str, dict[str, Any]]] = []
        pending_completion_receipts_by_task: dict[str, dict[str, Any]] = {}
        pending_registrations: list[tuple[str, dict[str, Any]]] = []
        touched_lanes: dict[str, LabLaneState] = {}
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
                if defer_enqueues:
                    raise DurableExecutionError(
                        f"retained Phase 3 result references unknown task: {task_id or '<missing>'}"
                    )
                ack_lease_ids.append(lease_id)
                continue
            if task_id in lane.completed_task_ids or task_id in lane.failed_task_ids:
                pending_receipt = pending_completion_receipts_by_task.get(task_id)
                terminal = durable_tasks_by_id.get(task_id)
                terminal_receipt = (
                    {"payload": pending_receipt}
                    if pending_receipt is not None
                    else (
                        (terminal or {}).get("terminal_receipt")
                        if isinstance(terminal, dict)
                        else None
                    )
                )
                stored = _validate_task_result_receipt_payload(
                    (terminal_receipt or {}).get("payload")
                    if isinstance(terminal_receipt, dict)
                    else None,
                    task_id=task_id,
                    worker_result_sha256=_worker_result_identity(lab_result),
                )
                recorded = dict(stored["recorded_result"])
                local = _validate_task_result_receipt(
                    Path(str(recorded.get("artifact_dir") or "")) / "task-result-receipt.json",
                    task_id=task_id,
                    worker_result_sha256=_worker_result_identity(lab_result),
                )
                if local != stored:
                    raise DurableExecutionError(
                        f"duplicate result conflicts with journal receipt for task {task_id}"
                    )
                ack_lease_ids.append(lease_id)
                continue
            recorded_successfully = False
            new_stage_tasks: list[dict[str, Any]] = []
            try:
                task_spec = lane.task_specs.get(task_id, {})
                evidence_plan = (
                    task_spec.get("evidence_plan")
                    if isinstance(task_spec.get("evidence_plan"), dict)
                    else {}
                )
                terminal_outcome = _validated_no_valid_cell_terminal(lab_result, evidence_plan)
                if terminal_outcome is not None:
                    recorded = _record_lab_failure(
                        config=config,
                        lane_ctx=lane_contexts[lane.run_id],
                        lane=lane,
                        runtime=runtime,
                        lab_result=lab_result,
                        reward_matrix=reward_matrix,
                        terminal_outcome=terminal_outcome,
                        render_progress=False,
                    )
                    mark_progress_dirty(lane)
                    lane.completed_task_ids.add(task_id)
                elif _is_failed_lab_result(lab_result):
                    recorded = _record_lab_failure(
                        config=config,
                        lane_ctx=lane_contexts[lane.run_id],
                        lane=lane,
                        runtime=runtime,
                        lab_result=lab_result,
                        reward_matrix=reward_matrix,
                        render_progress=False,
                    )
                    mark_progress_dirty(lane)
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
                        render_progress=False,
                        allow_legacy_phase3_receipt_migration=defer_enqueues,
                    )
                    mark_progress_dirty(lane)
                    if recorded.get("status") == "failed":
                        lane.failed_task_ids.add(task_id)
                    else:
                        if (
                            runtime.task_mode == "deep_replay"
                            and task_spec.get("task_kind") != "sweep_shard"
                            and recorded.get("score") is None
                        ):
                            raise DurableExecutionError(
                                f"task {task_id} lacks a canonical score and validated terminal outcome"
                            )
                        lane.completed_task_ids.add(task_id)
                _refresh_lane_phase_result_counts(lane, task_id=task_id)
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
                terminal_receipt_payload = _terminal_receipt_for_result(
                    recorded,
                    lab_result,
                    derived_tasks=new_stage_tasks if defer_enqueues else None,
                    allow_legacy_phase3_receipt_migration=defer_enqueues,
                )
                pending_completions.append((task_id, terminal_receipt_payload))
                pending_completion_receipts_by_task[task_id] = terminal_receipt_payload
                pending_registrations.extend(
                    (str(derived_task["task_id"]), derived_task)
                    for derived_task in new_stage_tasks
                )
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
                    render_progress=False,
                )
                mark_progress_dirty(lane)
                recorded_result_count += 1
                _add_recorded_result_sample(recorded_results, recorded)
                lane.failed_task_ids.add(task_id)
                _refresh_lane_phase_result_counts(lane, task_id=task_id)
                terminal_receipt_payload = _terminal_receipt_for_result(
                    recorded,
                    failure_result,
                    derived_tasks=[] if defer_enqueues else None,
                )
                pending_completions.append((task_id, terminal_receipt_payload))
                pending_completion_receipts_by_task[task_id] = terminal_receipt_payload
            ack_lease_ids.append(lease_id)
            touched_lanes[lane.run_id] = lane
            if new_stage_tasks:
                deferred_tasks.extend(new_stage_tasks)
                reason = f"stage:{_task_phase(lane, task_id)}"
                deferred_tasks_by_reason.setdefault(reason, []).extend(new_stage_tasks)

        if pending_completions or pending_registrations:
            apply_journal_batch(
                registrations=pending_registrations,
                completions=pending_completions,
            )
            if defer_enqueues:
                _result_consumption_checkpoint("after_source_terminal_receipt")
                _result_consumption_checkpoint("after_derived_task_registration")
            for task in deferred_tasks:
                task_id = str(task["task_id"])
                if not any(
                    str(existing.get("task_id") or "") == task_id
                    for existing in tasks
                ):
                    tasks.append(task)
                lane = next(
                    candidate
                    for candidate in lanes
                    if candidate.lane_id == str(task.get("lane_id") or "")
                )
                lanes_by_task[task_id] = lane
            for lane in touched_lanes.values():
                _write_lane_metadata(
                    lane,
                    campaign_ctx=campaign_ctx,
                    runtime=runtime,
                    status=lane_run_status(lane),
                    started_at=started_at,
                )
            persist_campaign_state()
            if defer_enqueues:
                _result_consumption_checkpoint("before_result_ack")
        if ack_lease_ids:
            acked = _safe_ack_gateway_results(
                gateway,
                campaign_ctx,
                lease_ids=ack_lease_ids,
                task_id="batch",
                require_exact_count=defer_enqueues,
            )
            if defer_enqueues and not acked:
                raise DurableExecutionError(
                    "retained Phase 3 results could not be acknowledged before enqueue"
                )
        if not defer_enqueues:
            for reason, new_tasks in deferred_tasks_by_reason.items():
                enqueue_existing_tasks(
                    new_tasks,
                    reason=reason,
                    journal_registered=True,
                    state_persisted=True,
                )
        prune_terminal_lane_history()
        if not defer_enqueues:
            create_and_enqueue_more()
        return deferred_tasks

    if runtime.resume and _is_phase3_formal_runtime(runtime):
        while True:
            retained_batch = _read_gateway_results(
                gateway,
                limit=runtime.result_batch_size,
            )
            if not retained_batch:
                break
            process_result_batch(retained_batch, defer_enqueues=True)

    resume_tasks_to_enqueue = (
        [
            _attach_task_profile_snapshots(dict(item["payload"]), campaign_dir)
            for item in journal.unresolved()
        ]
        if runtime.resume
        else []
    )
    if runtime.resume and reserved_lane_indices:
        existing_indices = {lane.lane_index for lane in lanes}
        missing_indices = [
            lane_index
            for lane_index in reserved_lane_indices
            if lane_index not in existing_indices
        ]
        recovered_lanes = [prepare_lane(lane_index) for lane_index in missing_indices]
        recovered_lanes.extend(
            lane
            for lane in lanes
            if lane.lane_index in reserved_lane_indices and lane not in recovered_lanes
        )
        persist_campaign_state()
        _lane_allocation_checkpoint("after_lane_registration")
        enqueue_lanes(sorted(recovered_lanes, key=lambda lane: lane.lane_index))
        recovered_task_ids = {
            task_id for lane in recovered_lanes for task_id in lane.task_ids
        }
        resume_tasks_to_enqueue = [
            task
            for task in resume_tasks_to_enqueue
            if str(task.get("task_id") or "") not in recovered_task_ids
        ]

    if runtime.resume and _is_phase3_formal_runtime(runtime):
        deadline = (
            None
            if runtime.campaign_mode == "continuous"
            else time.monotonic() + runtime.max_wait_seconds
        )

    try:
        if runtime.resume and resume_tasks_to_enqueue:
            _enqueue_gateway_tasks_with_retries(
                gateway,
                campaign_ctx,
                resume_tasks_to_enqueue,
                reason="resume_unresolved",
                failure_limit=runtime.enqueue_failure_limit,
                retry_base_seconds=runtime.enqueue_retry_base_seconds,
            )
        else:
            create_and_enqueue_more()
    except requests.RequestException as exc:
        gateway_unreachable = True
        _append_event(campaign_ctx, "gateway", "initial_task_enqueue_failed", error=str(exc)[:1000])

    try:
        last_snapshot = gateway.snapshot()
        initial_gateway_id = (
            str(last_snapshot.get("gateway_id"))
            if isinstance(last_snapshot.get("gateway_id"), str)
            else None
        )
    except requests.RequestException as exc:
        _append_event(campaign_ctx, "gateway", "initial_snapshot_failed", error=str(exc)[:500])
    emit_barrier_snapshot(force=True)

    try:
        while not gateway_unreachable and (deadline is None or time.monotonic() < deadline):
            cycle_started_at = time.monotonic()
            cycle_result_count = 0
            read_failed = False
            while cycle_result_count < runtime.max_results_per_cycle:
                limit = min(runtime.result_batch_size, runtime.max_results_per_cycle - cycle_result_count)
                try:
                    result_batch = _read_gateway_results(gateway, limit=limit)
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
                    read_failed = True
                    break
                consecutive_result_read_failures = 0
                if not result_batch:
                    break
                process_result_batch(result_batch)
                cycle_result_count += len(result_batch)
                if len(result_batch) < limit:
                    break
                if runtime.max_drain_seconds > 0 and (time.monotonic() - cycle_started_at) >= runtime.max_drain_seconds:
                    break
            if gateway_unreachable:
                break
            if read_failed:
                continue
            create_and_enqueue_more()
            completed_count = completed_task_count()
            failed_count = failed_task_count()
            terminal_count = completed_count + failed_count
            current_total_tasks = total_task_count()
            if (
                runtime.campaign_mode == "finite"
                and not can_create_more()
                and current_total_tasks
                and terminal_count >= current_total_tasks
            ):
                break
            flush_dirty_progress()
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
                emit_barrier_snapshot()
            except requests.RequestException as exc:
                _append_event(campaign_ctx, "gateway", "snapshot_failed", error=str(exc)[:500])
            if cycle_result_count <= 0:
                time.sleep(runtime.poll_interval_seconds)
    except requests.RequestException as exc:
        gateway_unreachable = True
        _append_event(campaign_ctx, "gateway", "task_enqueue_exhausted", error=str(exc)[:1000])
    except KeyboardInterrupt:
        interrupted = True
        _append_event(campaign_ctx, "campaign", "interrupted")

    flush_dirty_progress(force=True)
    completed_count = completed_task_count()
    failed_count = failed_task_count()
    terminal_count = completed_count + failed_count
    if interrupted:
        status = "stopped"
    elif gateway_unreachable:
        status = "gateway_unreachable"
    elif gateway_restarted:
        status = "gateway_restarted"
    elif result_loss_detected:
        status = "result_loss"
    elif runtime.campaign_mode == "finite" and total_task_count() and terminal_count >= total_task_count() and not can_create_more():
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
    status, historical_failure_reason = _finalize_historical_campaign_status(
        status,
        lanes=lanes,
        runtime=runtime,
    )
    if historical_failure_reason:
        _append_event(
            campaign_ctx,
            "campaign",
            "historical_completion_failed",
            reason=historical_failure_reason,
        )
        for lane in lanes:
            lane_ctx = lane_contexts.get(lane.run_id)
            if lane_ctx is None:
                continue
            _write_lane_metadata(
                lane,
                campaign_ctx=campaign_ctx,
                runtime=runtime,
                status="failed",
                started_at=started_at,
                extra={"historical_completion_failure_reason": historical_failure_reason},
            )
    emit_barrier_snapshot(force=True, status=status)
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
            history=history,
        ),
        recorded_results=recorded_results,
        recorded_result_count=recorded_result_count,
        history=history,
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
            "total_task_count": total_task_count(),
            "summary_path": str(campaign_ctx.summary_path.resolve()),
            "historical_completion_failure_reason": historical_failure_reason,
        },
    )
    _append_event(campaign_ctx, "campaign", status, summary_path=str(campaign_ctx.summary_path.resolve()))
    if runtime.json_output:
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
    else:
        console.print(
            "[bold]PlayHand Lab[/bold] "
            f"{status}: {completed_count}/{total_task_count()} completed, {failed_count} failed, "
            f"campaign={campaign_ctx.run_id}"
        )
    return 0 if status == "completed" or (status == "stopped" and not runtime.as_of_date) else 2


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
