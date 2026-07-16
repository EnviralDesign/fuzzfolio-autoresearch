"""Authoritative, declarative execution plans for frozen Level C cutoffs.

This module deliberately does not start Atlas, PlayHand, or any worker.  It
only binds one frozen Level C cutoff to the active generation and produces an
immutable payload that an executor can consume without accepting substitute
dates, identifiers, or provenance.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_json, canonical_sha256, canonical_timestamp
from .generation_archive import (
    GENERATION_MANIFEST_NAME,
    GENERATION_SCHEMA_NAME,
    GENERATION_SCHEMA_VERSION,
)
from .instrument_universe import universe_provenance
from .level_c_protocol import (
    LevelCProtocolError,
    load_level_c_protocol,
    load_level_c_protocol_authority,
)


LEVEL_C_EXECUTION_PLAN_SCHEMA = "autoresearch-level-c-execution-plan-v1"
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CUTOFF_KEYS = frozenset({"A", "B", "C", "D"})
_GEOMETRY_FIELDS = (
    "selection_start",
    "selection_end",
    "training_start",
    "training_end",
    "embargo_start",
    "embargo_end",
    "embargo_days",
    "outer_test_start",
    "outer_test_end",
    "geometry_sha256",
)
_PROVENANCE_IDENTITIES = (
    "lake_semantic_sha256",
    "source_snapshot_sha256",
    "universe_id",
    "universe_manifest_sha256",
    "worker_contract_id",
    "worker_contract_sha256",
    "worker_image",
    "engine_id",
    "engine_sha256",
    "scoring_policy_id",
    "scoring_policy_sha256",
    "cost_policy_id",
    "cost_policy_sha256",
)
_HASH_IDENTITIES = frozenset(
    field for field in _PROVENANCE_IDENTITIES if field.endswith("_sha256")
)


class LevelCOperatorError(RuntimeError):
    """Raised when a Level C execution plan cannot be authoritatively bound."""


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LevelCOperatorError(f"{label} must be a JSON object")
    return value


def _require_identifier(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    if not _SAFE_ID_RE.fullmatch(token):
        raise LevelCOperatorError(f"{label} must be a safe identifier")
    return token


def _require_sha256(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    if not _SHA256_RE.fullmatch(token):
        raise LevelCOperatorError(f"{label} must be an exact sha256 identity")
    return token


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=True))
    except ValueError:
        return False
    return True


def _active_runs_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise LevelCOperatorError("active runs root must not be a symlink")
    root = requested.resolve(strict=False)
    if not root.is_dir():
        raise LevelCOperatorError("active runs root must be a real directory")
    return root


def _load_generation_manifest(active_runs_root: Path) -> tuple[dict[str, Any], str]:
    path = active_runs_root / GENERATION_MANIFEST_NAME
    if not path.is_file() or path.is_symlink():
        raise LevelCOperatorError("active runs root is missing a real generation-manifest.json")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCOperatorError("active generation manifest is unreadable JSON") from exc
    manifest = dict(_require_mapping(payload, label="active generation manifest"))
    if (
        manifest.get("schema_name") != GENERATION_SCHEMA_NAME
        or manifest.get("schema_version") != GENERATION_SCHEMA_VERSION
    ):
        raise LevelCOperatorError("active generation manifest schema is unsupported")
    _require_identifier(manifest.get("new_generation_id"), label="generation new_generation_id")
    canonical_timestamp(manifest.get("created_at"))
    linkage = _require_mapping(manifest.get("archive_linkage"), label="generation archive_linkage")
    _require_identifier(linkage.get("archive_id"), label="generation archive_linkage.archive_id")
    for field in ("archive_manifest_path", "archived_runs_root", "archive_prepared_at"):
        if not str(linkage.get(field) or "").strip():
            raise LevelCOperatorError(f"generation archive_linkage.{field} is required")
    for field in ("source_runs_root", "destination_runs_root"):
        if not str(manifest.get(field) or "").strip():
            raise LevelCOperatorError(f"generation {field} is required")
    recorded = Path(str(manifest["destination_runs_root"])).expanduser()
    if recorded.is_symlink():
        raise LevelCOperatorError("generation manifest destination_runs_root must not be a symlink")
    recorded_root = recorded.resolve(strict=False)
    if recorded_root != active_runs_root:
        raise LevelCOperatorError("generation manifest destination_runs_root does not match active runs root")
    _require_mapping(manifest.get("archived_inventory"), label="generation archived_inventory")
    if not isinstance(manifest.get("restore_instructions"), (Mapping, list)):
        raise LevelCOperatorError("generation restore_instructions are required")
    provenance = _require_mapping(manifest.get("provenance"), label="generation provenance")
    for field in _PROVENANCE_IDENTITIES:
        value = provenance.get(field)
        if field in _HASH_IDENTITIES:
            _require_sha256(value, label=f"generation provenance.{field}")
        else:
            _require_identifier(value, label=f"generation provenance.{field}")
    return manifest, _sha256_bytes(raw)


def _load_protocol(path: Path | str, *, expected_manifest_id: str) -> dict[str, Any]:
    try:
        return load_level_c_protocol(
            Path(path), expected_manifest_id=expected_manifest_id
        )
    except LevelCProtocolError as exc:
        raise LevelCOperatorError(f"invalid frozen Level C protocol: {exc}") from exc


def _validate_protocol_binding(
    protocol: Mapping[str, Any], generation: Mapping[str, Any], generation_sha256: str
) -> None:
    if protocol.get("status") != "frozen":
        raise LevelCOperatorError("Level C protocol must be frozen")
    if protocol.get("research_generation_id") != generation.get("new_generation_id"):
        raise LevelCOperatorError("protocol research_generation_id does not match active generation")
    if protocol.get("research_generation_manifest_sha256") != generation_sha256:
        raise LevelCOperatorError("protocol research_generation_manifest_sha256 does not match raw active generation manifest")
    provenance = _require_mapping(generation.get("provenance"), label="generation provenance")
    for field in _PROVENANCE_IDENTITIES:
        if protocol.get(field) != provenance.get(field):
            raise LevelCOperatorError(f"protocol {field} does not match generation provenance")
    live_universe = universe_provenance()
    if protocol.get("universe_id") != live_universe["universe_id"]:
        raise LevelCOperatorError("protocol universe_id does not match live universe provenance")
    if protocol.get("universe_manifest_sha256") != live_universe["universe_hash"]:
        raise LevelCOperatorError("protocol universe_manifest_sha256 does not match live universe provenance")


def _resolve_expected_artifacts(
    expected: Any, active_runs_root: Path
) -> dict[str, dict[str, str]]:
    locations = _require_mapping(expected, label="protocol expected_artifact_locations")
    if not locations:
        raise LevelCOperatorError("protocol expected_artifact_locations cannot be empty")
    resolved: dict[str, dict[str, str]] = {}
    for name, raw_location in sorted(locations.items()):
        key = _require_identifier(name, label="expected artifact name")
        location = str(raw_location or "").strip().replace("\\", "/")
        candidate = Path(location)
        if (
            not location
            or candidate.is_absolute()
            or candidate.drive
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise LevelCOperatorError(f"expected artifact {key} must be a non-empty relative path")
        target = (active_runs_root / candidate).resolve(strict=False)
        if not _path_within(target, active_runs_root):
            raise LevelCOperatorError(f"expected artifact {key} escapes active runs root")
        resolved[key] = {
            "relative_path": candidate.as_posix(),
            "resolved_path": str(target),
        }
    return resolved


def _plan_identity(payload: Mapping[str, Any]) -> str:
    identity = dict(payload)
    identity.pop("plan_id", None)
    return canonical_sha256(identity)


def build_level_c_execution_plan(
    active_runs_root: Path | str,
    protocol_path: Path | str,
    authority_path: Path | str,
    cutoff_key: str,
) -> dict[str, Any]:
    """Build one non-executing, source-bound Level C cutoff plan.

    Dates, run identifiers, campaign identifiers, and provenance are derived
    only from the loaded manifests; the public API intentionally has no
    parameters through which callers can override them.
    """
    root = _active_runs_root(active_runs_root)
    generation, generation_sha256 = _load_generation_manifest(root)
    try:
        authority = load_level_c_protocol_authority(
            Path(authority_path),
            generation_manifest_path=root / GENERATION_MANIFEST_NAME,
            protocol_path=Path(protocol_path),
        )
    except LevelCProtocolError as exc:
        raise LevelCOperatorError(f"invalid Level C protocol authority: {exc}") from exc
    protocol = _load_protocol(
        protocol_path, expected_manifest_id=authority["protocol_manifest_id"]
    )
    _validate_protocol_binding(protocol, generation, generation_sha256)
    key = str(cutoff_key or "").strip()
    if key not in _CUTOFF_KEYS:
        raise LevelCOperatorError("cutoff_key must select exactly one of A, B, C, or D")
    matches = [plan for plan in protocol["cutoff_plans"] if plan["cutoff_key"] == key]
    if len(matches) != 1:
        raise LevelCOperatorError("frozen protocol must contain exactly one selected cutoff")
    cutoff = matches[0]
    geometry = {field: cutoff[field] for field in _GEOMETRY_FIELDS}
    artifacts = _resolve_expected_artifacts(cutoff["expected_artifact_locations"], root)
    atlas_artifact = artifacts.get("atlas_run")
    if atlas_artifact is None:
        raise LevelCOperatorError("protocol cutoff is missing its atlas_run artifact location")
    seed_plan_path = str(
        Path(atlas_artifact["resolved_path"])
        / "recipe-priors"
        / "play-hand-seed-plan.json"
    )
    common = {
        "as_of_date": cutoff["selection_end"],
        "research_generation_id": generation["new_generation_id"],
        "level_c_protocol_id": protocol["protocol_manifest_id"],
        "cutoff_key": key,
        "lake_manifest_sha256": protocol["lake_semantic_sha256"],
        "source_snapshot_sha256": protocol["source_snapshot_sha256"],
        "universe_id": protocol["universe_id"],
        "universe_manifest_sha256": protocol["universe_manifest_sha256"],
        "worker_contract_hash": protocol["worker_contract_sha256"],
    }
    payload: dict[str, Any] = {
        "schema_version": LEVEL_C_EXECUTION_PLAN_SCHEMA,
        "execution_mode": "declarative-only",
        "generation": {
            "active_runs_root": str(root),
            "manifest_path": GENERATION_MANIFEST_NAME,
            "research_generation_id": generation["new_generation_id"],
            "raw_manifest_sha256": generation_sha256,
        },
        "protocol": {
            "protocol_manifest_id": protocol["protocol_manifest_id"],
            "protocol_path": str(Path(protocol_path).expanduser().resolve(strict=False)),
            "authority_id": authority["authority_id"],
            "authority_path": str(Path(authority_path).expanduser().resolve(strict=False)),
        },
        "cutoff": {
            "cutoff_key": key,
            "role": cutoff["role"],
            "atlas_run_id": cutoff["atlas_run_id"],
            "playhand_campaign_id": cutoff["playhand_campaign_id"],
            "cohort_id": cutoff["cohort_id"],
            "seed": cutoff["seed"],
            "geometry": geometry,
        },
        "expected_artifacts": artifacts,
        "bound_contract": {
            "worker_contract_id": protocol["worker_contract_id"],
            "worker_contract_sha256": protocol["worker_contract_sha256"],
            "worker_image": protocol["worker_image"],
            "engine_id": protocol["engine_id"],
            "engine_sha256": protocol["engine_sha256"],
            "scoring_policy_id": protocol["scoring_policy_id"],
            "scoring_policy_sha256": protocol["scoring_policy_sha256"],
            "cost_policy_id": protocol["cost_policy_id"],
            "cost_policy_sha256": protocol["cost_policy_sha256"],
            "no_global_priors": True,
            "no_outer_feedback": True,
        },
        "atlas_arguments": {
            **common,
            "run_id": cutoff["atlas_run_id"],
            "signal_atlas_executor": "gateway",
            "publish": False,
        },
        "playhand_arguments": {
            **common,
            "campaign_id": cutoff["playhand_campaign_id"],
            "seed": cutoff["seed"],
            "campaign_mode": "finite",
            "task_mode": "deep_replay",
            "pipeline_mode": "play_hand",
            "strict_scoring": True,
            "seed_plan_path": seed_plan_path,
        },
        "playhand_deferred_binding": {
            "argument": "expected_seed_plan_sha256",
            "source_path": str(
                Path(atlas_artifact["resolved_path"])
                / "recipe-priors"
                / "level-c-lineage.json"
            ),
            "source_field": "artifact_sha256.play-hand-seed-plan.json",
            "required_before_execution": True,
        },
    }
    payload["plan_id"] = _plan_identity(payload)
    return payload


def validate_level_c_execution_plan(
    payload: Mapping[str, Any],
    *,
    active_runs_root: Path | str | None = None,
    protocol_path: Path | str | None = None,
    authority_path: Path | str | None = None,
) -> dict[str, Any]:
    """Validate a plan hash and, when sources are supplied, re-derive it.

    Supplying both source locations makes this an authoritative validator: a
    modified payload with a recomputed ``plan_id`` still fails because the
    current frozen manifests mechanically derive a different plan.
    """
    plan = dict(_require_mapping(payload, label="Level C execution plan"))
    if plan.get("schema_version") != LEVEL_C_EXECUTION_PLAN_SCHEMA:
        raise LevelCOperatorError("unknown Level C execution plan schema")
    if plan.get("execution_mode") != "declarative-only":
        raise LevelCOperatorError("Level C execution plan must be declarative-only")
    plan_id = _require_sha256(plan.get("plan_id"), label="execution plan plan_id")
    if plan_id != _plan_identity(plan):
        raise LevelCOperatorError("Level C execution plan hash mismatch")
    supplied_sources = (
        active_runs_root is not None,
        protocol_path is not None,
        authority_path is not None,
    )
    if any(supplied_sources) and not all(supplied_sources):
        raise LevelCOperatorError(
            "active_runs_root, protocol_path, and authority_path must be supplied together"
        )
    if all(supplied_sources):
        cutoff = _require_mapping(plan.get("cutoff"), label="execution plan cutoff")
        expected = build_level_c_execution_plan(
            active_runs_root,
            protocol_path,
            authority_path,
            str(cutoff.get("cutoff_key") or ""),
        )
        if canonical_json(plan) != canonical_json(expected):
            raise LevelCOperatorError("Level C execution plan does not match authoritative sources")
    return plan


def load_level_c_execution_plan(
    path: Path | str,
    *,
    active_runs_root: Path | str | None = None,
    protocol_path: Path | str | None = None,
    authority_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load and validate a serialized Level C execution plan."""
    source = Path(path).expanduser().resolve(strict=False)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCOperatorError(f"invalid Level C execution plan file: {source}") from exc
    return validate_level_c_execution_plan(
        payload,
        active_runs_root=active_runs_root,
        protocol_path=protocol_path,
        authority_path=authority_path,
    )


def create_level_c_execution_plan(path: Path | str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Atomically create a plan file once; an existing target is never reused."""
    supplied = dict(_require_mapping(payload, label="Level C execution plan"))
    generation = _require_mapping(supplied.get("generation"), label="execution plan generation")
    protocol = _require_mapping(supplied.get("protocol"), label="execution plan protocol")
    plan = validate_level_c_execution_plan(
        supplied,
        active_runs_root=str(generation.get("active_runs_root") or ""),
        protocol_path=str(protocol.get("protocol_path") or ""),
        authority_path=str(protocol.get("authority_path") or ""),
    )
    target = Path(path).expanduser().resolve(strict=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise LevelCOperatorError(f"Level C execution plan already exists: {target}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json(plan).encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise LevelCOperatorError(f"Level C execution plan already exists: {target}") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return load_level_c_execution_plan(target)
