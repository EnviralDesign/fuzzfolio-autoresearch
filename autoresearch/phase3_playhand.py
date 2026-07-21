"""Bounded Phase 3 PlayHand coordinator adapter.

Phase 3 is deliberately not a Level C cutoff.  This command re-audits the
immutable Phase 3 authority and passes its complete semantic runtime to the
existing durable PlayHand Lab coordinator.  Only transport and pacing knobs
remain operator-controlled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Mapping

from .config import load_config
from .level_c_operator import validate_profile_model_source_lock
from .phase3_authority import Phase3AuthorityError, resolve_phase3_playhand_runtime_arguments
from .play_hand_lab import (
    PLAY_HAND_LAB_CAMPAIGNS_DIR,
    PlayHandLabRuntimeConfig,
    _resolve_worker_contract_hash,
    _trading_dashboard_root,
    cmd_play_hand_lab,
    preflight_play_hand_lab,
)
from .runtime_policy_lock import RuntimePolicyLockError, validate_runtime_policy_lock


class Phase3PlayHandError(ValueError):
    """Raised when a Phase 3 coordinator launch does not match its authority."""


_RUNTIME_CONFIG_FIELDS = {field.name for field in fields(PlayHandLabRuntimeConfig)}
_SEMANTIC_RUNTIME_FIELDS = (
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


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _load_authority(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase3PlayHandError(f"invalid Phase 3 authority: {path}") from exc
    if not isinstance(payload, dict):
        raise Phase3PlayHandError("Phase 3 authority must be a JSON object")
    return payload


def _campaign_root(config: Any, campaign_id: str) -> Path:
    return Path(config.runs_root) / "derived" / PLAY_HAND_LAB_CAMPAIGNS_DIR / campaign_id


def _validate_launch_mode(*, campaign_root: Path, fresh: bool, resume: bool) -> None:
    if fresh == resume:
        raise Phase3PlayHandError("Phase 3 PlayHand requires exactly one of --fresh or --resume")
    if fresh:
        if campaign_root.exists() or campaign_root.is_symlink():
            raise Phase3PlayHandError(
                f"Phase 3 fresh campaign root already exists: {campaign_root}"
            )
        return
    if not campaign_root.is_dir() or campaign_root.is_symlink():
        raise Phase3PlayHandError(
            f"Phase 3 resume requires its existing durable campaign root: {campaign_root}"
        )
    required = (
        "run-metadata.json",
        "play-hand-lab-state.json",
        "play-hand-lab-execution-journal.json",
    )
    missing = [name for name in required if not (campaign_root / name).is_file()]
    if missing:
        raise Phase3PlayHandError(
            "Phase 3 resume requires the exact durable campaign files: "
            + ", ".join(missing)
        )


def _validate_live_runtime_contract(
    *,
    authority: Mapping[str, Any],
    runtime: PlayHandLabRuntimeConfig,
    config: Any,
) -> None:
    bound = authority.get("bound_contract")
    if not isinstance(bound, Mapping):
        raise Phase3PlayHandError("Phase 3 authority has no bound runtime contract")
    enforcement = authority.get("worker_execution_enforcement")
    if not isinstance(enforcement, Mapping):
        raise Phase3PlayHandError("Phase 3 authority has no worker enforcement metadata")
    if (
        enforcement.get("gateway_enforced_worker_contract_sha256")
        != runtime.worker_contract_hash
        or enforcement.get("gateway_claim_correctness")
        != "worker_contract_sha256_and_required_capabilities"
        or enforcement.get("worker_image_gateway_claim_enforced") is not False
    ):
        raise Phase3PlayHandError("Phase 3 worker claim enforcement metadata is invalid")
    if (
        bound.get("operator_launch_worker_image")
        != runtime.operator_launch_worker_image
        or enforcement.get("operator_launch_worker_image")
        != runtime.operator_launch_worker_image
        or enforcement.get("operator_launch_provenance")
        != "exact_image_required_before_worker_launch"
    ):
        raise Phase3PlayHandError("Phase 3 operator worker-image provenance is invalid")
    try:
        root = _trading_dashboard_root(config=config, runtime=runtime)
        # Passing no hash forces a live rebuild rather than trusting the bound one.
        observed_contract = _resolve_worker_contract_hash(
            config=config,
            runtime=replace(runtime, worker_contract_hash=None),
        )
        if observed_contract != runtime.worker_contract_hash:
            raise Phase3PlayHandError(
                "live replay worker contract differs from the immutable Phase 3 authority"
            )
        validate_profile_model_source_lock(
            bound.get("profile_model_source_lock") or {}, root
        )
        validate_runtime_policy_lock(
            bound.get("runtime_policy_lock") or {},
            config,
            worker_contract_sha256=str(observed_contract),
            # The frozen engine policy is contract-attested. The source root is
            # validated independently by the profile-model source lock above.
            trading_dashboard_root=None,
        )
    except (RuntimePolicyLockError, ValueError, RuntimeError) as exc:
        if isinstance(exc, Phase3PlayHandError):
            raise
        raise Phase3PlayHandError(
            f"live Phase 3 engine, scoring, cost, profile, or worker contract drift: {exc}"
        ) from exc


def prepare_phase3_playhand_runtime(args: argparse.Namespace) -> tuple[PlayHandLabRuntimeConfig, dict[str, Any]]:
    """Resolve one authority into a runnable durable PlayHand runtime, without starting it."""
    authority_path = Path(args.authority_path).expanduser().resolve(strict=True)
    capsule_root = Path(args.phase2_capsule_root).expanduser().resolve(strict=True)
    policy_path = Path(args.policy_manifest).expanduser().resolve(strict=True)
    try:
        semantic = resolve_phase3_playhand_runtime_arguments(
            authority_path=authority_path,
            phase2_capsule_root=capsule_root,
            policy_manifest_path=policy_path,
        )
    except (OSError, Phase3AuthorityError) as exc:
        raise Phase3PlayHandError(f"Phase 3 authority preflight failed: {exc}") from exc
    authority = _load_authority(authority_path)
    authority_id = str(authority.get("authority_id") or "").strip()
    if not authority_id.startswith("sha256:") or len(authority_id) != 71:
        raise Phase3PlayHandError("Phase 3 authority has no exact authority_id")
    if semantic.get("as_of_date") != "2026-01-14T00:00:00Z":
        raise Phase3PlayHandError("Phase 3 authority does not enforce the reserved-tail cutoff")
    if semantic.get("tail_access") != "forbidden_during_phase3_construction":
        raise Phase3PlayHandError("Phase 3 authority permits reserved-tail construction access")
    unknown = set(_SEMANTIC_RUNTIME_FIELDS) - set(semantic)
    if unknown:
        raise Phase3PlayHandError(
            "Phase 3 authority omits semantic runtime fields: " + ", ".join(sorted(unknown))
        )
    invalid = set(_SEMANTIC_RUNTIME_FIELDS) - _RUNTIME_CONFIG_FIELDS
    if invalid:
        raise Phase3PlayHandError(
            "Phase 3 adapter does not recognize authority runtime fields: "
            + ", ".join(sorted(invalid))
        )
    runtime_values = {field: semantic[field] for field in _SEMANTIC_RUNTIME_FIELDS}
    runtime_values.update(
        {
            "gateway_url": args.gateway_url,
            "gateway_token": args.gateway_token,
            "active_runs": args.active_runs,
            "poll_interval_seconds": args.poll_interval_seconds,
            "max_wait_seconds": args.max_wait_seconds,
            "result_batch_size": args.result_batch_size,
            "max_results_per_cycle": args.max_results_per_cycle,
            "max_drain_seconds": args.max_drain_seconds,
            "result_read_failure_limit": args.result_read_failure_limit,
            "enqueue_failure_limit": args.enqueue_failure_limit,
            "enqueue_retry_base_seconds": args.enqueue_retry_base_seconds,
            "terminal_lane_retention": args.terminal_lane_retention,
            "dry_run": bool(args.dry_run),
            "json_output": bool(args.json),
            "log_mode": args.log_mode,
            "barrier_interval_seconds": args.barrier_interval_seconds,
            "barrier_lane_limit": args.barrier_lane_limit,
            "trading_dashboard_root": args.trading_dashboard_root,
            "formal_authority_kind": "phase3",
            "phase3_authority_path": authority_path,
            "phase2_capsule_root": capsule_root,
            "campaign_policy_manifest_path": policy_path,
            "phase3_authority_id": authority_id,
            "phase3_authority_sha256": _sha256(authority_path),
            "execution_plan_id": authority_id,
            "level_c_protocol_id": authority_id,
            "cutoff_key": "P3",
            "research_generation_id": semantic["current_atlas_generation"],
            "seed_plan_path": Path(str(semantic["seed_plan_path"])),
            "resume": bool(args.resume),
        }
    )
    runtime = PlayHandLabRuntimeConfig(**runtime_values)
    config = load_config()
    _validate_launch_mode(
        campaign_root=_campaign_root(config, str(runtime.campaign_id)),
        fresh=bool(args.fresh),
        resume=bool(args.resume),
    )
    _validate_live_runtime_contract(authority=authority, runtime=runtime, config=config)
    return runtime, {
        "authority_id": authority_id,
        "authority_path": str(authority_path),
        "campaign_id": runtime.campaign_id,
        "as_of_date": runtime.as_of_date,
        "target_runs": runtime.target_runs,
        "operator_launch_worker_image": runtime.operator_launch_worker_image,
        "gateway_enforced_worker_contract_sha256": runtime.worker_contract_hash,
        "reserved_tail": semantic["reserved_tail"],
    }


def cmd_phase3_playhand(args: argparse.Namespace) -> int:
    runtime, payload = prepare_phase3_playhand_runtime(args)
    if bool(args.dry_run):
        preflight = preflight_play_hand_lab(runtime)
        exit_code = 0
        payload = {**payload, "preflight": preflight}
    else:
        exit_code = cmd_play_hand_lab(runtime)
    if bool(args.json):
        status = "completed" if exit_code == 0 else "failed"
        print(
            json.dumps(
                {"status": status, "exit_code": exit_code, **payload},
                ensure_ascii=True,
                sort_keys=True,
            )
        )
    return exit_code
