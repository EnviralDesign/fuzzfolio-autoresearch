"""Immutable Phase 3 PlayHand authority built from the Phase 2 Atlas capsule.

This module deliberately separates the two allowed development cutoffs from the
two validation cutoffs.  A and B are the sole construction sources for the
seed menus.  C and D are retained only as aggregate, non-candidate diagnostic
records, so their individual ranks cannot feed selection by accident.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .recipe_priors import (
    POLICY_HONEST_SEED_PLAN_SCHEMA_VERSION,
    CampaignPolicyValidationError,
    validate_campaign_policy_manifest,
    validate_seed_plan_campaign_policy,
)
from .play_hand import DEFAULT_INSTRUMENT_POOL
from .runtime_policy_lock import build_runtime_policy_lock, policy_lock_provenance


PHASE3_AUTHORITY_SCHEMA_VERSION = "phase3_playhand_authority_v2"
PHASE3_AUTHORITY_REPORT_SCHEMA_VERSION = "phase3_playhand_authority_report_v2"
PHASE3_PLAYHAND_RUNTIME_SCHEMA_VERSION = "phase3_playhand_runtime_arguments_v2"
PHASE2_CAPSULE_MANIFEST_NAME = "phase2-atlas-authority-capsule-manifest.json"
PHASE3_AUTHORITY_FILENAME = "phase3-playhand-authority.json"
PHASE3_SEED_PLAN_FILENAME = "play-hand-seed-plan.json"
PHASE3_REPORT_FILENAME = "phase3-playhand-authority-report.json"
DEVELOPMENT_CUTOFFS = ("A", "B")
VALIDATION_CUTOFFS = ("C", "D")
ALL_CUTOFFS = DEVELOPMENT_CUTOFFS + VALIDATION_CUTOFFS
RESERVED_TAIL = {
    "start": "2026-01-14T00:00:00Z",
    "end": "2026-07-14T00:00:00Z",
    "semantics": "[start,end)",
    "use": "reserved_untouched_outer_evaluation_only",
}

# These are the complete non-operational PlayHand settings for a Phase 3
# campaign.  The adapter supplies only transport and scheduling controls.
# Keeping this map here makes a change to any research semantic invalidate an
# existing authority on its next audit instead of silently changing a resume.
PHASE3_PLAYHAND_SEMANTIC_DEFAULTS = {
    "tasks_per_lane": 1,
    "timeframe": "M5",
    "instrument": None,
    # Never leave the research universe to a later coordinator default. The
    # authority rederives this list on audit, and an existing authority fails
    # closed if the configured native research universe changes.
    "instrument_pool": list(DEFAULT_INSTRUMENT_POOL),
    "instrument_pool_preset": None,
    "indicator": None,
    "profile_path": None,
    "min_indicators": 1,
    "max_indicators": 4,
    "lookback_months": 3,
    "bar_limit": 5000,
    "max_reward_r": None,
    "sweep_budget": "high",
    "max_sweep_permutations": 1024,
    "sweep_shard_size": 8,
    "early_exit_mode": "enforce",
    "coarse_halving_mode": "enforce",
    "coarse_probe_budget": 128,
    "validation_months": 12,
    "validation_min_score": 45.0,
    "scrutiny_months": 36,
    "final_min_score": 40.0,
    "screen_anchor_mode": "random",
    "screen_anchor_envelope_months": 36,
    "instrument_scout_size": 5,
    "instrument_scout_max_selected": 3,
    "deadline_seconds": 3600.0,
    "max_attempts": 8,
    "strict_scoring": True,
    "retain_raw_lab_artifacts": False,
    "worker_contract_schema": "replay-worker-contract-v1",
}


class Phase3AuthorityError(ValueError):
    """Raised when an authority cannot be constructed or revalidated."""


@dataclass(frozen=True)
class Phase3AuthorityBuildResult:
    authority_path: Path
    seed_plan_path: Path
    report_path: Path
    authority: dict[str, Any]
    report: dict[str, Any]


def _canonical_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _canonical_digest(payload: object) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _raw_file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _phase3_seed(authority_name: str) -> int:
    """Derive a stable, nonzero PlayHand seed from the immutable authority name."""
    raw = hashlib.sha256(f"phase3-playhand-seed-v1:{authority_name}".encode("utf-8")).digest()
    return int.from_bytes(raw[:8], "big") % 2_147_483_646 + 1


def _verify_phase2_capsule(capsule_root: Path) -> None:
    """Verify every capsule file before treating any cutoff as authority input."""
    try:
        from .phase2_atlas_capsule import CapsuleError, verify_capsule

        verify_capsule(capsule_root)
    except CapsuleError as exc:
        raise Phase3AuthorityError(f"Phase 2 capsule verification failed: {exc}") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase3AuthorityError(f"invalid {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise Phase3AuthorityError(f"{label} must be a JSON object: {path}")
    return payload


def _require_mapping(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise Phase3AuthorityError(f"{label} must be an object")
    return value


def _require_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Phase3AuthorityError(f"{label} must be a non-empty string")
    return value.strip()


def _require_positive_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise Phase3AuthorityError(f"{label} must be a positive integer")
    return value


def _require_nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise Phase3AuthorityError(f"{label} must be a non-negative integer")
    return value


def _require_sha256(value: object, *, label: str) -> str:
    token = _require_string(value, label=label)
    if not token.startswith("sha256:") or len(token) != 71:
        raise Phase3AuthorityError(f"{label} must be an exact sha256 identity")
    try:
        int(token.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise Phase3AuthorityError(f"{label} must be an exact sha256 identity") from exc
    return token


def _is_reparse_point(path: Path) -> bool:
    try:
        attributes = path.lstat().st_file_attributes
    except AttributeError:
        attributes = 0
    return path.is_symlink() or bool(attributes & getattr(os, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _reject_reparse_ancestors(path: Path, *, label: str) -> None:
    cursor = Path(os.path.abspath(path))
    while True:
        if cursor.exists() or cursor.is_symlink():
            if _is_reparse_point(cursor):
                raise Phase3AuthorityError(f"{label} contains a symlink or reparse point: {cursor}")
        parent = cursor.parent
        if parent == cursor:
            return
        cursor = parent


def _require_under(path: Path, root: Path, *, label: str) -> Path:
    _reject_reparse_ancestors(path, label=label)
    if not path.exists():
        raise Phase3AuthorityError(f"missing {label}: {path}")
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise Phase3AuthorityError(f"{label} is outside the Phase 2 capsule: {path}") from exc
    return resolved


def _stable_json_key(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _numeric_mean(values: Iterable[object]) -> float | None:
    numbers = [float(value) for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 6)


def _merge_menu_rows(rows: Iterable[tuple[str, Mapping[str, Any]]], *, key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Merge A/B rows without importing any C/D identity or score."""
    buckets: dict[tuple[str, ...], list[tuple[str, Mapping[str, Any]]]] = {}
    for cutoff, row in rows:
        key = tuple(str(row.get(field) or "").strip() for field in key_fields)
        if not any(key):
            raise Phase3AuthorityError(f"development menu row has no identity for {key_fields}")
        buckets.setdefault(key, []).append((cutoff, row))
    merged: list[dict[str, Any]] = []
    for key in sorted(buckets):
        members = sorted(buckets[key], key=lambda item: (item[0], _stable_json_key(item[1])))
        result = copy.deepcopy(dict(members[0][1]))
        result["development_cutoffs"] = [cutoff for cutoff, _ in members]
        for field in ("sampling_weight", "recipe_slot_score", "pair_sampling_weight", "pair_sampling_score"):
            average = _numeric_mean([row.get(field) for _, row in members])
            if average is not None:
                result[field] = average
        merged.append(result)
    merged.sort(
        key=lambda row: (
            -float(row.get("sampling_weight") or row.get("pair_sampling_weight") or 0.0),
            _stable_json_key(row),
        )
    )
    return merged


def _merge_templates(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        copied = copy.deepcopy(dict(row))
        unique.setdefault(_stable_json_key(copied), copied)
    return [unique[key] for key in sorted(unique)]


def _merge_development_seed_plans(seed_plans: Mapping[str, Mapping[str, Any]], *, policy: dict[str, Any], generation_id: str) -> dict[str, Any]:
    recipes_by_name: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    negatives: list[tuple[str, Mapping[str, Any]]] = []
    for cutoff in DEVELOPMENT_CUTOFFS:
        seed_plan = seed_plans[cutoff]
        recipes = _require_mapping(seed_plan.get("recipes"), label=f"{cutoff} seed plan recipes")
        for recipe_name, recipe in recipes.items():
            if isinstance(recipe, Mapping):
                recipes_by_name.setdefault(str(recipe_name), []).append((cutoff, recipe))
        for row in seed_plan.get("negative_pairs") or []:
            if isinstance(row, Mapping):
                negatives.append((cutoff, row))

    recipes: dict[str, Any] = {}
    for recipe_name in sorted(recipes_by_name):
        members = sorted(recipes_by_name[recipe_name], key=lambda item: item[0])
        base = copy.deepcopy(dict(members[0][1]))
        base["development_cutoffs"] = [cutoff for cutoff, _ in members]
        slot_names = sorted(
            {
                str(slot_name)
                for _, recipe in members
                for slot_name in _require_mapping(recipe.get("slot_menus") or {}, label=f"{recipe_name} slot menus")
            }
        )
        base["slot_menus"] = {
            slot_name: _merge_menu_rows(
                (
                    (cutoff, row)
                    for cutoff, recipe in members
                    for row in (_require_mapping(recipe.get("slot_menus") or {}, label=f"{recipe_name} slot menus").get(slot_name) or [])
                    if isinstance(row, Mapping)
                ),
                key_fields=("indicator_id", "source", "sampling_lane"),
            )
            for slot_name in slot_names
        }
        base["pair_menu"] = _merge_menu_rows(
            (
                (cutoff, row)
                for cutoff, recipe in members
                for row in (recipe.get("pair_menu") or [])
                if isinstance(row, Mapping)
            ),
            key_fields=("canonical_pair_family_id", "ordered_pair_id", "probe_timeframe"),
        )
        base["recommended_templates"] = _merge_templates(
            row
            for _, recipe in members
            for row in (recipe.get("recommended_templates") or [])
            if isinstance(row, Mapping)
        )
        recipes[recipe_name] = base

    negative_rows = _merge_menu_rows(
        negatives,
        key_fields=("recipe", "ordered_pair_id", "probe_timeframe", "negative_reason_category"),
    )
    for row in negative_rows:
        row["expires_after_atlas_runs"] = int(row.get("expires_after_atlas_runs") or 1)

    sampling_policy = {
        "guided_prior_fraction": policy["lanes"]["guided"]["fraction"],
        "uncertain_prior_fraction": policy["lanes"]["uncertain"]["fraction"],
        "wild_exploration_fraction": policy["lanes"]["wild"]["fraction"],
        "interpretation": "Formal Phase 3 lane quotas are enforced at campaign planning time.",
        "maturity": "development_ab_structural_prior",
        "template_instrument_policy": "seed_pool",
    }
    payload = {
        "schema_version": POLICY_HONEST_SEED_PLAN_SCHEMA_VERSION,
        "feature_schema_version": "atlas_feature_vector_v1",
        "historical_lineage": {
            "construction_cutoffs": list(DEVELOPMENT_CUTOFFS),
            "candidate_selection_scope": "A_B_development_only",
            "generation_id": generation_id,
            "validation_cutoffs_excluded_from_selection": list(VALIDATION_CUTOFFS),
        },
        "sampling_policy": sampling_policy,
        "campaign_policy_manifest": policy,
        "campaign_policy_sha256": policy["manifest_sha256"],
        "negative_pairs": negative_rows,
        "recipes": recipes,
    }
    try:
        validate_seed_plan_campaign_policy(payload, expected_policy_sha256=policy["manifest_sha256"])
    except CampaignPolicyValidationError as exc:
        raise Phase3AuthorityError(f"invalid generated Phase 3 seed plan: {exc}") from exc
    return payload


def _summary_diagnostic(summary: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only aggregate counts.  Candidate rows are intentionally never read."""
    return {
        "result_counts": copy.deepcopy(_require_mapping(summary.get("result_counts"), label="recipe priors result_counts")),
        "discovered_recipe_validation": copy.deepcopy(
            _require_mapping(summary.get("discovered_recipe_validation"), label="recipe priors discovery summary")
        ),
        "negative_priors": copy.deepcopy(_require_mapping(summary.get("negative_priors"), label="recipe priors negative summary")),
    }


def _source_record(capsule_root: Path, cutoff: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    root = _require_under(capsule_root / "atlas-roots" / cutoff, capsule_root, label=f"cutoff {cutoff} root")
    summary_path = _require_under(root / "atlas-lab-summary.json", capsule_root, label=f"cutoff {cutoff} Atlas summary")
    seed_path = _require_under(root / "recipe-priors" / "play-hand-seed-plan.json", capsule_root, label=f"cutoff {cutoff} seed plan")
    priors_summary_path = _require_under(root / "recipe-priors" / "recipe-priors-summary.json", capsule_root, label=f"cutoff {cutoff} recipe-priors summary")
    atlas_summary = _load_object(summary_path, label=f"cutoff {cutoff} Atlas summary")
    if str(atlas_summary.get("status") or "").strip().lower() != "completed":
        raise Phase3AuthorityError(f"cutoff {cutoff} Atlas summary is not completed")
    seed_plan = _load_object(seed_path, label=f"cutoff {cutoff} seed plan")
    priors_summary = _load_object(priors_summary_path, label=f"cutoff {cutoff} recipe-priors summary")
    lineage = _require_mapping(seed_plan.get("historical_lineage"), label=f"cutoff {cutoff} seed plan lineage")
    record = {
        "cutoff": cutoff,
        "atlas_root": str(root.relative_to(capsule_root).as_posix()),
        "atlas_summary_sha256": _raw_file_digest(summary_path),
        "seed_plan_sha256": _raw_file_digest(seed_path),
        "recipe_priors_summary_sha256": _raw_file_digest(priors_summary_path),
        "execution_plan_id": _require_string(lineage.get("execution_plan_id"), label=f"cutoff {cutoff} execution_plan_id"),
        "lake_manifest_sha256": _require_string(lineage.get("lake_manifest_sha256"), label=f"cutoff {cutoff} lake manifest"),
        "source_snapshot_sha256": _require_string(lineage.get("source_snapshot_sha256"), label=f"cutoff {cutoff} source snapshot"),
        "universe_id": _require_string(lineage.get("universe_id"), label=f"cutoff {cutoff} universe id"),
        "universe_manifest_sha256": _require_string(lineage.get("universe_manifest_sha256"), label=f"cutoff {cutoff} universe manifest"),
        "research_generation_id": _require_string(
            lineage.get("research_generation_id"),
            label=f"cutoff {cutoff} research generation",
        ),
    }
    return record, seed_plan, priors_summary, atlas_summary


def _load_level_c_plan(capsule_root: Path, cutoff: str) -> tuple[dict[str, Any], str]:
    path = _require_under(
        capsule_root / "level-c-control" / f"execution-plan-{cutoff}.json",
        capsule_root,
        label=f"cutoff {cutoff} execution plan",
    )
    return _load_object(path, label=f"cutoff {cutoff} execution plan"), _raw_file_digest(path)


def _shared_authority_contract(plans: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    contracts = {
        cutoff: _require_mapping(plan.get("bound_contract"), label=f"cutoff {cutoff} bound contract")
        for cutoff, plan in plans.items()
    }
    canonical = _stable_json_key(contracts["A"])
    if any(_stable_json_key(contract) != canonical for contract in contracts.values()):
        raise Phase3AuthorityError("Phase 2 cutoffs do not share one exact runtime authority contract")
    return copy.deepcopy(contracts["A"])


def _live_execution_contract(
    phase2_contract: Mapping[str, Any],
    *,
    worker_image: str,
    trading_dashboard_root: Path | str | None,
) -> dict[str, Any]:
    """Rebind execution only; Phase 2 remains immutable selection provenance."""
    from .config import load_config
    from .level_c_operator import (
        _configured_trading_dashboard_root,
        build_profile_model_source_lock,
    )

    config = load_config()
    root = (
        Path(trading_dashboard_root).expanduser().resolve(strict=True)
        if trading_dashboard_root is not None
        else _configured_trading_dashboard_root(config).resolve(strict=True)
    )
    shared_python = root / "shared" / "python"
    for package_root in reversed(
        [shared_python / "fuzzfolio_core", shared_python / "fuzzfolio_data", shared_python]
    ):
        if package_root.exists() and str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
    try:
        from fuzzfolio_core.contracts.worker_contract import build_replay_worker_contract
    except Exception as exc:
        raise Phase3AuthorityError(
            f"could not load replay worker contract builder from {shared_python}: {exc}"
        ) from exc
    worker_contract = build_replay_worker_contract(repo_root=root)
    runtime_lock = build_runtime_policy_lock(
        config,
        worker_contract_sha256=worker_contract.contract_hash,
    )
    identities = policy_lock_provenance(runtime_lock)
    rebound = copy.deepcopy(dict(phase2_contract))
    rebound.update(identities)
    rebound.update(
        {
            "worker_contract_id": worker_contract.schema_version,
            "worker_contract_sha256": worker_contract.contract_hash,
            "worker_image": _require_string(worker_image, label="execution worker image"),
            "runtime_policy_lock": runtime_lock,
            "profile_model_source_root": str(root),
            "profile_model_source_lock": build_profile_model_source_lock(root),
        }
    )
    return rebound


def _verify_capsule_manifest(capsule_root: Path) -> tuple[dict[str, Any], str]:
    manifest_path = _require_under(capsule_root / PHASE2_CAPSULE_MANIFEST_NAME, capsule_root, label="Phase 2 capsule manifest")
    manifest = _load_object(manifest_path, label="Phase 2 capsule manifest")
    if manifest.get("schema_version") != "phase2_atlas_authority_capsule_v1":
        raise Phase3AuthorityError("Phase 2 capsule manifest has an unsupported schema")
    listed = {str(row.get("capsule_relative_path") or "") for row in manifest.get("files") or [] if isinstance(row, dict)}
    required = {f"atlas-roots/{cutoff}/recipe-priors/play-hand-seed-plan.json" for cutoff in ALL_CUTOFFS}
    if not required.issubset(listed):
        raise Phase3AuthorityError("Phase 2 capsule manifest does not attest every cutoff seed plan")
    return manifest, _raw_file_digest(manifest_path)


def _validate_common_lineage(records: Mapping[str, Mapping[str, Any]], plans: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    keys = (
        "lake_manifest_sha256",
        "source_snapshot_sha256",
        "universe_id",
        "universe_manifest_sha256",
        "research_generation_id",
    )
    common = {key: records["A"][key] for key in keys}
    for cutoff in ALL_CUTOFFS:
        if any(records[cutoff][key] != common[key] for key in keys):
            raise Phase3AuthorityError("Phase 2 cutoff source identities differ")
        if records[cutoff]["execution_plan_id"] != plans[cutoff].get("plan_id"):
            raise Phase3AuthorityError(f"cutoff {cutoff} seed lineage does not match its execution plan")
    return common


def _policy_runtime_anchor(policy: Mapping[str, Any], *, common_lineage: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact Atlas state used to expire Phase 3 negative priors."""
    expiry = _require_mapping(policy.get("negative_prior_expiry"), label="campaign policy negative-prior expiry")
    anchor = _require_mapping(expiry.get("anchor"), label="campaign policy negative-prior anchor")
    generation = _require_string(anchor.get("generation"), label="campaign policy anchor generation")
    run_sequence = _require_nonnegative_int(
        anchor.get("run_sequence"), label="campaign policy anchor run sequence"
    )
    if generation != common_lineage["research_generation_id"]:
        raise Phase3AuthorityError(
            "campaign policy Atlas generation does not match the completed Phase 2 cutoffs"
        )
    return {
        "current_atlas_generation": generation,
        "current_atlas_run_sequence": run_sequence,
    }


def _phase3_playhand_runtime_arguments(
    *,
    authority_name: str,
    target_runs: int,
    seed_plan_sha256: str,
    common_lineage: Mapping[str, Any],
    bound_contract: Mapping[str, Any],
    policy_anchor: Mapping[str, Any],
    policy: Mapping[str, Any],
    policy_source_file_sha256: str,
) -> dict[str, Any]:
    """Build the non-operational arguments a Phase 3 coordinator must use.

    The future coordinator supplies operational transport settings separately.
    These values are frozen because changing any of them changes research
    semantics or negative-prior expiry behaviour.
    """
    worker_contract = _require_sha256(
        bound_contract.get("worker_contract_sha256"), label="bound worker contract"
    )
    operator_launch_worker_image = _require_string(
        bound_contract.get("worker_image"), label="operator launch worker image"
    )
    return {
        "schema_version": PHASE3_PLAYHAND_RUNTIME_SCHEMA_VERSION,
        "formal_authority_kind": "phase3",
        "campaign_mode": "finite",
        "task_mode": "deep_replay",
        "pipeline_mode": "play_hand",
        "target_runs": target_runs,
        "campaign_id": authority_name,
        "seed": _phase3_seed(authority_name),
        "as_of_date": RESERVED_TAIL["start"],
        "seed_plan_filename": PHASE3_SEED_PLAN_FILENAME,
        "expected_seed_plan_sha256": seed_plan_sha256,
        "campaign_policy_schema_version": _require_string(
            policy.get("schema_version"), label="campaign policy schema version"
        ),
        "campaign_policy_manifest_sha256": _require_sha256(
            policy.get("manifest_sha256"), label="campaign policy manifest"
        ),
        "campaign_policy_source_file_sha256": _require_sha256(
            policy_source_file_sha256, label="campaign policy source file"
        ),
        "lake_manifest_sha256": common_lineage["lake_manifest_sha256"],
        "source_snapshot_sha256": common_lineage["source_snapshot_sha256"],
        "universe_id": common_lineage["universe_id"],
        "universe_manifest_sha256": common_lineage["universe_manifest_sha256"],
        "worker_contract_hash": worker_contract,
        "operator_launch_worker_image": operator_launch_worker_image,
        "current_atlas_generation": policy_anchor["current_atlas_generation"],
        "current_atlas_run_sequence": policy_anchor["current_atlas_run_sequence"],
        "reserved_tail": copy.deepcopy(RESERVED_TAIL),
        "tail_access": "forbidden_during_phase3_construction",
        **copy.deepcopy(PHASE3_PLAYHAND_SEMANTIC_DEFAULTS),
    }


def _validate_formal_v2_runtime_preflight(
    *,
    policy: Mapping[str, Any],
    runtime_arguments: Mapping[str, Any],
) -> None:
    """Exercise the live policy-honest v2 runtime binding without starting work."""
    if runtime_arguments.get("campaign_policy_schema_version") != policy.get("schema_version"):
        raise Phase3AuthorityError("formal v2 PlayHand policy schema drift")
    if runtime_arguments.get("campaign_policy_manifest_sha256") != policy.get("manifest_sha256"):
        raise Phase3AuthorityError("formal v2 PlayHand policy manifest drift")
    from .play_hand_lab import PlayHandLabRuntimeConfig, _policy_negative_prior_runtime

    runtime = PlayHandLabRuntimeConfig(
        campaign_mode=str(runtime_arguments["campaign_mode"]),
        task_mode=str(runtime_arguments["task_mode"]),
        pipeline_mode=str(runtime_arguments["pipeline_mode"]),
        target_runs=int(runtime_arguments["target_runs"]),
        expected_seed_plan_sha256=str(runtime_arguments["expected_seed_plan_sha256"]),
        current_atlas_generation=str(runtime_arguments["current_atlas_generation"]),
        current_atlas_run_sequence=runtime_arguments["current_atlas_run_sequence"],
    )
    try:
        observed = _policy_negative_prior_runtime(dict(policy), runtime=runtime)
    except Exception as exc:  # The runtime owns the exact policy-honest error.
        raise Phase3AuthorityError(f"formal v2 PlayHand runtime preflight failed: {exc}") from exc
    expected = {
        "current_atlas_generation": runtime_arguments["current_atlas_generation"],
        "current_atlas_run_sequence": runtime_arguments["current_atlas_run_sequence"],
        "binding_source": "runtime_authority",
    }
    if observed != expected:
        raise Phase3AuthorityError("formal v2 PlayHand runtime preflight drift")


def build_phase3_authority_payload(
    *,
    phase2_capsule_root: Path | str,
    policy_manifest_path: Path | str,
    authority_id: str,
    target_runs: int,
    execution_worker_image: str | None = None,
    trading_dashboard_root: Path | str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Rebuild deterministic Phase 3 authority content without writing files."""
    capsule_root = Path(phase2_capsule_root).expanduser().resolve(strict=True)
    _reject_reparse_ancestors(capsule_root, label="Phase 2 capsule root")
    _verify_phase2_capsule(capsule_root)
    normalized_authority_id = _require_string(authority_id, label="authority_id")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-" for character in normalized_authority_id):
        raise Phase3AuthorityError("authority_id contains unsupported characters")
    target = _require_positive_int(target_runs, label="target_runs")
    policy_path = Path(policy_manifest_path).expanduser().resolve(strict=True)
    _reject_reparse_ancestors(policy_path, label="campaign policy manifest")
    try:
        policy = validate_campaign_policy_manifest(_load_object(policy_path, label="campaign policy manifest"))
    except CampaignPolicyValidationError as exc:
        raise Phase3AuthorityError(f"invalid campaign policy manifest: {exc}") from exc
    capsule_manifest, capsule_manifest_sha256 = _verify_capsule_manifest(capsule_root)

    records: dict[str, dict[str, Any]] = {}
    seed_plans: dict[str, dict[str, Any]] = {}
    summaries: dict[str, dict[str, Any]] = {}
    plans: dict[str, dict[str, Any]] = {}
    execution_plan_hashes: dict[str, str] = {}
    for cutoff in ALL_CUTOFFS:
        record, seed_plan, summary, _atlas_summary = _source_record(capsule_root, cutoff)
        plan, plan_hash = _load_level_c_plan(capsule_root, cutoff)
        records[cutoff] = record
        seed_plans[cutoff] = seed_plan
        summaries[cutoff] = summary
        plans[cutoff] = plan
        execution_plan_hashes[cutoff] = plan_hash

    common_lineage = _validate_common_lineage(records, plans)
    phase2_bound_contract = _shared_authority_contract(plans)
    bound_contract = (
        _live_execution_contract(
            phase2_bound_contract,
            worker_image=execution_worker_image,
            trading_dashboard_root=trading_dashboard_root,
        )
        if execution_worker_image
        else phase2_bound_contract
    )
    worker_contract_sha256 = _require_sha256(
        bound_contract.get("worker_contract_sha256"), label="bound worker contract"
    )
    operator_launch_worker_image = _require_string(
        bound_contract.get("worker_image"), label="operator launch worker image"
    )
    phase3_bound_contract = copy.deepcopy(bound_contract)
    phase3_bound_contract.pop("worker_image", None)
    phase3_bound_contract["operator_launch_worker_image"] = operator_launch_worker_image
    policy_anchor = _policy_runtime_anchor(policy, common_lineage=common_lineage)
    seed_plan = _merge_development_seed_plans(
        {cutoff: seed_plans[cutoff] for cutoff in DEVELOPMENT_CUTOFFS},
        policy=policy,
        generation_id=normalized_authority_id,
    )
    seed_plan_bytes = _canonical_bytes(seed_plan)
    seed_plan_sha256 = "sha256:" + hashlib.sha256(seed_plan_bytes).hexdigest()
    runtime_arguments = _phase3_playhand_runtime_arguments(
        authority_name=normalized_authority_id,
        target_runs=target,
        seed_plan_sha256=seed_plan_sha256,
        common_lineage=common_lineage,
        bound_contract=bound_contract,
        policy_anchor=policy_anchor,
        policy=policy,
        policy_source_file_sha256=_raw_file_digest(policy_path),
    )
    _validate_formal_v2_runtime_preflight(
        policy=policy,
        runtime_arguments=runtime_arguments,
    )
    validation_diagnostics = {
        cutoff: {
            "recipe_priors_summary_sha256": records[cutoff]["recipe_priors_summary_sha256"],
            "aggregate": _summary_diagnostic(summaries[cutoff]),
            "candidate_feedback": "forbidden",
        }
        for cutoff in VALIDATION_CUTOFFS
    }
    authority_without_id = {
        "schema_version": PHASE3_AUTHORITY_SCHEMA_VERSION,
        "authority_name": normalized_authority_id,
        "phase2_capsule": {
            "capsule_identity_sha256": _require_string(capsule_manifest.get("capsule_identity_sha256"), label="capsule identity"),
            "manifest_sha256": capsule_manifest_sha256,
        },
        **(
            {
                "execution_rebinding": {
                    "mode": "current_contract_with_phase2_selection_provenance",
                    "phase2_selection_contract_sha256": _canonical_digest(
                        phase2_bound_contract
                    ),
                    "operator_launch_worker_image": operator_launch_worker_image,
                }
            }
            if execution_worker_image
            else {}
        ),
        "development_construction": {
            "allowed_cutoffs": list(DEVELOPMENT_CUTOFFS),
            "selection_inputs": {
                cutoff: {
                    **records[cutoff],
                    "execution_plan_sha256": execution_plan_hashes[cutoff],
                }
                for cutoff in DEVELOPMENT_CUTOFFS
            },
            "construction_algorithm": "deterministic_ab_menu_union_mean_weights_v1",
        },
        "validation_diagnostics": {
            "allowed_cutoffs": list(VALIDATION_CUTOFFS),
            "candidate_feedback": "forbidden",
            "diagnostics": validation_diagnostics,
        },
        "reserved_tail": copy.deepcopy(RESERVED_TAIL),
        "bound_contract": phase3_bound_contract,
        "worker_execution_enforcement": {
            "gateway_claim_correctness": "worker_contract_sha256_and_required_capabilities",
            "gateway_enforced_worker_contract_sha256": worker_contract_sha256,
            "operator_launch_provenance": "exact_image_required_before_worker_launch",
            "operator_launch_worker_image": operator_launch_worker_image,
            "worker_image_gateway_claim_enforced": False,
        },
        "source_identities": common_lineage,
        "campaign_policy": {
            "schema_version": policy["schema_version"],
            "manifest_sha256": policy["manifest_sha256"],
            "source_file_sha256": _raw_file_digest(policy_path),
        },
        "seed_plan": {
            "schema_version": seed_plan["schema_version"],
            "filename": PHASE3_SEED_PLAN_FILENAME,
            "sha256": seed_plan_sha256,
        },
        "campaign": {
            "mode": "finite",
            "target_runs": target,
            "continuous_mode": "forbidden",
        },
        "playhand_runtime_arguments": runtime_arguments,
    }
    authority = {**authority_without_id, "authority_id": _canonical_digest(authority_without_id)}
    report = {
        "schema_version": PHASE3_AUTHORITY_REPORT_SCHEMA_VERSION,
        "authority_id": authority["authority_id"],
        "authority_name": normalized_authority_id,
        "candidate_selection_cutoffs": list(DEVELOPMENT_CUTOFFS),
        "validation_only_cutoffs": list(VALIDATION_CUTOFFS),
        "candidate_feedback_from_validation": "forbidden",
        "reserved_tail": copy.deepcopy(RESERVED_TAIL),
        "target_runs": target,
        "policy_manifest_sha256": policy["manifest_sha256"],
        "seed_plan_sha256": seed_plan_sha256,
        "current_atlas_generation": runtime_arguments["current_atlas_generation"],
        "current_atlas_run_sequence": runtime_arguments["current_atlas_run_sequence"],
        "worker_execution_enforcement": copy.deepcopy(
            authority["worker_execution_enforcement"]
        ),
        "recipe_count": len(seed_plan["recipes"]),
        "slot_menu_row_count": sum(
            len(rows)
            for recipe in seed_plan["recipes"].values()
            for rows in (_require_mapping(recipe.get("slot_menus"), label="generated slot menus")).values()
        ),
        "pair_menu_row_count": sum(len(recipe.get("pair_menu") or []) for recipe in seed_plan["recipes"].values()),
        "negative_prior_count": len(seed_plan["negative_pairs"]),
        "validation_diagnostics": validation_diagnostics,
    }
    return authority, seed_plan, report


def resolve_phase3_playhand_runtime_arguments(
    *,
    authority_path: Path | str,
    phase2_capsule_root: Path | str,
    policy_manifest_path: Path | str,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return exact Phase 3 coordinator arguments after full authority audit.

    This is the handoff from immutable plan construction to a later PlayHand
    coordinator.  It intentionally accepts no semantic override: Procman may
    later add only transport and concurrency controls around this result.
    """
    validate_phase3_authority(
        authority_path=authority_path,
        phase2_capsule_root=phase2_capsule_root,
        policy_manifest_path=policy_manifest_path,
    )
    source = Path(authority_path).expanduser().resolve(strict=True)
    authority = _load_object(source, label="Phase 3 authority")
    runtime = _require_mapping(
        authority.get("playhand_runtime_arguments"),
        label="Phase 3 PlayHand runtime arguments",
    )
    if runtime.get("schema_version") != PHASE3_PLAYHAND_RUNTIME_SCHEMA_VERSION:
        raise Phase3AuthorityError("Phase 3 PlayHand runtime arguments have an unsupported schema")
    resolved = copy.deepcopy(runtime)
    seed_path = source.parent / str(resolved.pop("seed_plan_filename", ""))
    if seed_path.name != PHASE3_SEED_PLAN_FILENAME or not seed_path.is_file():
        raise Phase3AuthorityError("Phase 3 PlayHand seed plan path is invalid")
    if _raw_file_digest(seed_path) != resolved.get("expected_seed_plan_sha256"):
        raise Phase3AuthorityError("Phase 3 PlayHand seed plan digest drift")
    resolved["seed_plan_path"] = str(seed_path.resolve(strict=True))
    if overrides:
        unexpected = [key for key in overrides if key not in resolved]
        if unexpected:
            raise Phase3AuthorityError(
                "Phase 3 PlayHand runtime does not permit unknown authority overrides: "
                + ", ".join(sorted(unexpected))
            )
        conflicts = [
            key
            for key, value in overrides.items()
            if key in resolved and value != resolved[key]
        ]
        if conflicts:
            raise Phase3AuthorityError(
                "Phase 3 PlayHand runtime conflicts with immutable authority: "
                + ", ".join(sorted(conflicts))
            )
    _validate_formal_v2_runtime_preflight(
        policy=validate_campaign_policy_manifest(
            _load_object(Path(policy_manifest_path).expanduser().resolve(strict=True), label="campaign policy manifest")
        ),
        runtime_arguments=resolved,
    )
    return resolved


def _atomic_write(path: Path, payload: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_bytes(_canonical_bytes(payload))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def create_phase3_authority(
    *,
    phase2_capsule_root: Path | str,
    policy_manifest_path: Path | str,
    authority_id: str,
    target_runs: int,
    out_dir: Path | str,
    execution_worker_image: str | None = None,
    trading_dashboard_root: Path | str | None = None,
) -> Phase3AuthorityBuildResult:
    """Create a new immutable authority directory. Existing output never merges."""
    target = Path(out_dir).expanduser()
    _reject_reparse_ancestors(target, label="Phase 3 authority output")
    if target.exists():
        raise Phase3AuthorityError(f"Phase 3 authority output already exists: {target}")
    authority, seed_plan, report = build_phase3_authority_payload(
        phase2_capsule_root=phase2_capsule_root,
        policy_manifest_path=policy_manifest_path,
        authority_id=authority_id,
        target_runs=target_runs,
        execution_worker_image=execution_worker_image,
        trading_dashboard_root=trading_dashboard_root,
    )
    target.mkdir(parents=True, exist_ok=False)
    try:
        authority_path = target / PHASE3_AUTHORITY_FILENAME
        seed_plan_path = target / PHASE3_SEED_PLAN_FILENAME
        report_path = target / PHASE3_REPORT_FILENAME
        _atomic_write(seed_plan_path, seed_plan)
        if _raw_file_digest(seed_plan_path) != authority["seed_plan"]["sha256"]:
            raise Phase3AuthorityError("written Phase 3 seed plan digest differs from authority")
        _atomic_write(authority_path, authority)
        _atomic_write(report_path, report)
    except Exception:
        # Preserve the partial immutable evidence for diagnosis; a retry must use
        # a fresh output directory rather than silently replacing any bytes.
        raise
    return Phase3AuthorityBuildResult(authority_path, seed_plan_path, report_path, authority, report)


def validate_phase3_authority(
    *,
    authority_path: Path | str,
    phase2_capsule_root: Path | str,
    policy_manifest_path: Path | str,
) -> dict[str, Any]:
    """Fail closed unless the persisted authority exactly rederives from inputs."""
    source = Path(authority_path).expanduser().resolve(strict=True)
    _reject_reparse_ancestors(source, label="Phase 3 authority")
    authority = _load_object(source, label="Phase 3 authority")
    if authority.get("schema_version") != PHASE3_AUTHORITY_SCHEMA_VERSION:
        raise Phase3AuthorityError("Phase 3 authority has an unsupported schema")
    authority_id = _require_string(authority.get("authority_name"), label="authority_name")
    rebinding = authority.get("execution_rebinding")
    execution_worker_image = None
    trading_dashboard_root = None
    if rebinding is not None:
        rebinding = _require_mapping(rebinding, label="Phase 3 execution rebinding")
        if rebinding.get("mode") != "current_contract_with_phase2_selection_provenance":
            raise Phase3AuthorityError("Phase 3 execution rebinding mode is invalid")
        execution_worker_image = _require_string(
            rebinding.get("operator_launch_worker_image"),
            label="Phase 3 execution worker image",
        )
        bound = _require_mapping(authority.get("bound_contract"), label="bound contract")
        trading_dashboard_root = _require_string(
            bound.get("profile_model_source_root"),
            label="Phase 3 profile-model source root",
        )
    campaign = _require_mapping(authority.get("campaign"), label="campaign")
    expected, seed_plan, report = build_phase3_authority_payload(
        phase2_capsule_root=phase2_capsule_root,
        policy_manifest_path=policy_manifest_path,
        authority_id=authority_id,
        target_runs=_require_positive_int(campaign.get("target_runs"), label="campaign.target_runs"),
        execution_worker_image=execution_worker_image,
        trading_dashboard_root=trading_dashboard_root,
    )
    if authority != expected:
        raise Phase3AuthorityError("Phase 3 authority differs from its rederived inputs")
    seed_path = source.parent / PHASE3_SEED_PLAN_FILENAME
    report_path = source.parent / PHASE3_REPORT_FILENAME
    if _raw_file_digest(seed_path) != expected["seed_plan"]["sha256"]:
        raise Phase3AuthorityError("Phase 3 seed plan digest drift")
    if _load_object(seed_path, label="Phase 3 seed plan") != seed_plan:
        raise Phase3AuthorityError("Phase 3 seed plan content drift")
    if _load_object(report_path, label="Phase 3 authority report") != report:
        raise Phase3AuthorityError("Phase 3 authority report drift")
    return {
        "status": "valid",
        "authority_id": expected["authority_id"],
        "seed_plan_sha256": expected["seed_plan"]["sha256"],
        "target_runs": expected["campaign"]["target_runs"],
    }


def cmd_phase3_playhand_authority(args: argparse.Namespace) -> int:
    """CLI entrypoint for plan build, dry-run, and audit only."""
    selected = sum(bool(value) for value in (args.dry_run, args.audit, args.runtime_arguments))
    if selected > 1:
        raise Phase3AuthorityError(
            "--dry-run, --audit, and --runtime-arguments are mutually exclusive"
        )
    if bool(args.runtime_arguments):
        runtime = resolve_phase3_playhand_runtime_arguments(
            authority_path=args.authority_path,
            phase2_capsule_root=args.phase2_capsule_root,
            policy_manifest_path=args.policy_manifest,
        )
        payload = {"status": "runtime_arguments_valid", "runtime_arguments": runtime}
    elif bool(args.audit):
        payload = validate_phase3_authority(
            authority_path=args.authority_path,
            phase2_capsule_root=args.phase2_capsule_root,
            policy_manifest_path=args.policy_manifest,
        )
    elif bool(args.dry_run):
        authority, _seed_plan, report = build_phase3_authority_payload(
            phase2_capsule_root=args.phase2_capsule_root,
            policy_manifest_path=args.policy_manifest,
            authority_id=args.authority_id,
            target_runs=args.target_runs,
            execution_worker_image=args.execution_worker_image,
            trading_dashboard_root=args.trading_dashboard_root,
        )
        payload = {"status": "dry_run_valid", "authority": authority, "report": report}
    else:
        result = create_phase3_authority(
            phase2_capsule_root=args.phase2_capsule_root,
            policy_manifest_path=args.policy_manifest,
            authority_id=args.authority_id,
            target_runs=args.target_runs,
            out_dir=args.out_dir,
            execution_worker_image=args.execution_worker_image,
            trading_dashboard_root=args.trading_dashboard_root,
        )
        payload = {
            "status": "created",
            "authority_path": str(result.authority_path),
            "seed_plan_path": str(result.seed_plan_path),
            "report_path": str(result.report_path),
            "authority_id": result.authority["authority_id"],
            "seed_plan_sha256": result.authority["seed_plan"]["sha256"],
        }
    if bool(args.json):
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    else:
        print(f"Phase 3 authority {payload['status']}: {payload.get('authority_id', '')}")
    return 0
