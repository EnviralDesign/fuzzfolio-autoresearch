"""Operator-facing orchestration for one frozen Level C generation.

This module coordinates existing Atlas, PlayHand, cohort, nested-evidence, and
portfolio APIs. It deliberately contains no research or replay implementation.
"""

from __future__ import annotations

import hashlib
import argparse
import json
import os
import re
import sqlite3
import tempfile
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .atlas_lab import AtlasLabRuntimeConfig, run_atlas_lab
from .catalog_index import CATALOG_INDEX_SCHEMA_VERSION
from .config import AppConfig, load_config
from .evidence_plan import canonical_json, canonical_sha256
from .generation_archive import (
    ARCHIVE_SCHEMA_NAME,
    ARCHIVE_SCHEMA_VERSION,
    GENERATION_MANIFEST_NAME,
    GENERATION_SCHEMA_NAME,
    GENERATION_SCHEMA_VERSION,
)
from .level_c import freeze_level_c_cohort, validate_level_c_cohort
from .level_c_operator import (
    build_level_c_execution_plan,
    build_profile_model_source_lock,
    create_level_c_execution_plan,
    executor_arguments_from_plan,
    load_authoritative_level_c_execution_plan,
    resolve_level_c_atlas_run_root,
)
from .level_c_protocol import (
    LEVEL_C_PROTOCOL_SCHEMA,
    build_initial_four_cutoff_plans,
    create_level_c_protocol,
    create_level_c_protocol_authority,
    load_level_c_protocol,
    load_level_c_protocol_authority,
)
from .play_hand_lab import (
    PlayHandLabRuntimeConfig,
    _worker_ready_profile_snapshot,
    cmd_play_hand_lab,
)
from .runtime_policy_lock import build_runtime_policy_lock, policy_lock_provenance


BOOTSTRAP_RECEIPT_SCHEMA = "autoresearch-level-c-bootstrap-receipt-v1"
STAGE_RECEIPT_SCHEMA = "autoresearch-level-c-stage-receipt-v1"
DEVELOPMENT_POLICY_SCHEMA = "autoresearch-level-c-development-policy-v1"
AUDIT_SCHEMA = "autoresearch-level-c-audit-v1"
CONTROL_RELATIVE = Path("derived") / "level-c" / "control"
_GENERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
STAGES = (
    "atlas",
    "playhand",
    "frozen_cohort",
    "training_evidence",
    "frozen_cells",
    "frozen_portfolio",
    "selected_outer",
    "final_report",
)
NESTED_STAGES = STAGES[3:]
NON_PROMOTABLE_OUTCOMES = frozenset(
    {"no_candidate", "nonviable", "no_consensus", "no_champion"}
)
def _operator_semantics(plan: Mapping[str, Any]) -> dict[str, Any]:
    arguments = plan.get("workflow_arguments")
    if not isinstance(arguments, Mapping):
        raise LevelCWorkflowError("execution plan is missing workflow arguments")
    return dict(arguments)


class LevelCWorkflowError(RuntimeError):
    pass


def _sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _require_generation_id(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    if not _GENERATION_ID_RE.fullmatch(token):
        raise LevelCWorkflowError(f"{label} must be a safe non-empty generation identity")
    return token


def _file_sha256(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        raise LevelCWorkflowError(f"required regular file is missing: {path}")
    return _sha256_bytes(path.read_bytes())


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCWorkflowError(f"invalid {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise LevelCWorkflowError(f"{label} must be a JSON object")
    return payload


def _create_or_verify(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    encoded = canonical_json(normalized).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != encoded:
            raise LevelCWorkflowError(f"immutable artifact drift: {path}")
        return normalized
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise LevelCWorkflowError(f"immutable artifact raced into existence: {path}") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return normalized


def _require_sha256(value: str, *, label: str) -> str:
    token = str(value or "").strip().lower()
    if len(token) != 71 or not token.startswith("sha256:"):
        raise LevelCWorkflowError(f"{label} must be a sha256 identity")
    try:
        int(token[7:], 16)
    except ValueError as exc:
        raise LevelCWorkflowError(f"{label} must be a sha256 identity") from exc
    return token


def _control_paths(active_root: Path) -> dict[str, Path]:
    control = active_root / CONTROL_RELATIVE
    return {
        "control": control,
        "archive_receipt": control / "archive-linkage.json",
        "protocol": control / "protocol.json",
        "authority": control / "protocol-authority.json",
        "bootstrap": control / "bootstrap-result.json",
        "development_policy": control / "development-policy.json",
    }


def _assert_bootstrap_root(active_root: Path) -> None:
    active_root.mkdir(parents=True, exist_ok=True)
    if not active_root.is_dir() or active_root.is_symlink():
        raise LevelCWorkflowError("active runs root must be a real directory")
    allowed_top = {"derived", GENERATION_MANIFEST_NAME}
    unexpected_top = [path for path in active_root.iterdir() if path.name not in allowed_top]
    if unexpected_top:
        raise LevelCWorkflowError("active runs root is not empty")
    derived = active_root / "derived"
    if derived.exists():
        if not derived.is_dir() or derived.is_symlink():
            raise LevelCWorkflowError("active derived root is invalid")
        unexpected = [path for path in derived.iterdir() if path.name != "level-c"]
        if unexpected:
            raise LevelCWorkflowError("active derived root contains non-bootstrap data")
        level_c_root = derived / "level-c"
        if level_c_root.exists():
            unexpected_level_c = [path for path in level_c_root.iterdir() if path.name != "control"]
            if unexpected_level_c:
                raise LevelCWorkflowError("active Level C root contains ambiguous partial data")
            control = level_c_root / "control"
            if control.exists():
                allowed_control = {
                    "archive-linkage.json",
                    "protocol.json",
                    "protocol-authority.json",
                    "bootstrap-result.json",
                    *(f"execution-plan-{key}.json" for key in "ABCD"),
                }
                unexpected_control = [
                    path for path in control.iterdir() if path.name not in allowed_control
                ]
                if unexpected_control:
                    raise LevelCWorkflowError("bootstrap control root contains ambiguous partial data")


def _assert_bootstrap_partial_order(
    active_root: Path, *, allow_archive_generation_handoff: bool = False
) -> None:
    paths = _control_paths(active_root)
    generation_path = active_root / GENERATION_MANIFEST_NAME
    if (
        allow_archive_generation_handoff
        and generation_path.exists()
        and not paths["archive_receipt"].exists()
    ):
        downstream = [
            paths["protocol"],
            paths["authority"],
            *(paths["control"] / f"execution-plan-{key}.json" for key in "ABCD"),
            paths["bootstrap"],
        ]
        for path in downstream:
            if path.exists() or path.is_symlink():
                raise LevelCWorkflowError(
                    f"ambiguous bootstrap partial state: {path.name} exists after a missing predecessor"
                )
        return
    ordered = [
        paths["archive_receipt"],
        generation_path,
        paths["protocol"],
        paths["authority"],
        *(paths["control"] / f"execution-plan-{key}.json" for key in "ABCD"),
        paths["bootstrap"],
    ]
    missing_seen = False
    for path in ordered:
        exists = path.exists() or path.is_symlink()
        if not exists:
            missing_seen = True
        elif missing_seen:
            raise LevelCWorkflowError(
                f"ambiguous bootstrap partial state: {path.name} exists after a missing predecessor"
            )


def _archive_generation_handoff(active_root: Path, new_generation_id: str) -> dict[str, Any] | None:
    path = active_root / GENERATION_MANIFEST_NAME
    paths = _control_paths(active_root)
    if not path.exists() and not path.is_symlink():
        return None
    if paths["archive_receipt"].exists() or paths["archive_receipt"].is_symlink():
        return None
    if not path.is_file() or path.is_symlink():
        raise LevelCWorkflowError("archive-generation handoff manifest is not a real file")
    raw = path.read_bytes()
    manifest = _load_json(path, label="archive-generation handoff manifest")
    if (
        manifest.get("schema_name") != GENERATION_SCHEMA_NAME
        or manifest.get("schema_version") != GENERATION_SCHEMA_VERSION
    ):
        raise LevelCWorkflowError("archive-generation handoff manifest schema is unsupported")
    if manifest.get("new_generation_id") != str(new_generation_id):
        raise LevelCWorkflowError("archive-generation handoff generation does not match requested generation")
    recorded_destination = Path(str(manifest.get("destination_runs_root") or "")).expanduser().resolve(strict=False)
    if recorded_destination != active_root.resolve(strict=False):
        raise LevelCWorkflowError("archive-generation handoff destination root does not match active runs root")
    linkage = manifest.get("archive_linkage")
    if not isinstance(linkage, Mapping):
        raise LevelCWorkflowError("archive-generation handoff archive_linkage is missing")
    archive_manifest_path = Path(str(linkage.get("archive_manifest_path") or "")).expanduser()
    if not archive_manifest_path.is_file() or archive_manifest_path.is_symlink():
        raise LevelCWorkflowError("archive-generation handoff archive manifest is missing")
    archive_manifest = _load_json(archive_manifest_path, label="archive-generation archive manifest")
    if (
        archive_manifest.get("schema_name") != ARCHIVE_SCHEMA_NAME
        or archive_manifest.get("schema_version") != ARCHIVE_SCHEMA_VERSION
        or archive_manifest.get("state") != "complete"
    ):
        raise LevelCWorkflowError("archive-generation handoff archive manifest is not complete")
    if (
        archive_manifest.get("archive_id") != linkage.get("archive_id")
        or archive_manifest.get("new_generation_id") != str(new_generation_id)
        or archive_manifest.get("destination_runs_root") != linkage.get("archived_runs_root")
        or archive_manifest.get("prepared_at") != linkage.get("archive_prepared_at")
        or not isinstance(archive_manifest.get("verified_archived_inventory"), Mapping)
    ):
        raise LevelCWorkflowError("archive-generation handoff archive manifest conflicts with linkage")
    if manifest.get("source_runs_root") != linkage.get("archived_runs_root"):
        raise LevelCWorkflowError("archive-generation handoff source root conflicts with linkage")
    provenance = dict(manifest.get("provenance") or {})
    prior_generation_id = _require_generation_id(
        provenance.get("prior_generation_id"),
        label="archive-generation handoff prior_generation_id",
    )
    if prior_generation_id == str(new_generation_id):
        raise LevelCWorkflowError("archive-generation handoff prior_generation_id must differ from successor generation")
    archive_provenance = archive_manifest.get("provenance")
    if not isinstance(archive_provenance, Mapping) or archive_provenance.get("prior_generation_id") != prior_generation_id:
        raise LevelCWorkflowError("archive-generation handoff prior_generation_id conflicts with archive manifest")
    archived_root = Path(str(linkage.get("archived_runs_root") or "")).expanduser().resolve(strict=True)
    archived_generation_path = archived_root / GENERATION_MANIFEST_NAME
    if not archived_generation_path.is_file() or archived_generation_path.is_symlink():
        raise LevelCWorkflowError("archive-generation handoff archived prior generation manifest is missing")
    archived_generation = _load_json(
        archived_generation_path, label="archive-generation archived prior generation manifest"
    )
    if archived_generation.get("new_generation_id") != prior_generation_id:
        raise LevelCWorkflowError("archive-generation handoff archive does not prove the prior generation")
    inventory_generation = (
        archive_manifest.get("verified_archived_inventory", {})
        .get("critical_artifacts", {})
        .get(GENERATION_MANIFEST_NAME)
    )
    if not isinstance(inventory_generation, Mapping) or inventory_generation.get("sha256") != _file_sha256(archived_generation_path).split(":", 1)[1]:
        raise LevelCWorkflowError("archive-generation handoff archived prior generation hash is not verified")
    return {
        "manifest_sha256": _sha256_bytes(raw),
        "archive_linkage": dict(linkage),
        "prior_generation_id": prior_generation_id,
        "successor_generation_id": str(new_generation_id),
        "provenance": provenance,
    }


def _create_or_replace_archive_handoff_generation_manifest(
    path: Path, payload: Mapping[str, Any], handoff: Mapping[str, Any] | None
) -> dict[str, Any]:
    normalized = dict(payload)
    encoded = canonical_json(normalized).encode("utf-8")
    if not path.exists() and not path.is_symlink():
        return _create_or_verify(path, normalized)
    if not path.is_file() or path.is_symlink():
        raise LevelCWorkflowError(f"immutable artifact drift: {path}")
    existing = path.read_bytes()
    if existing == encoded:
        return normalized
    if not isinstance(handoff, Mapping) or _sha256_bytes(existing) != handoff.get("manifest_sha256"):
        raise LevelCWorkflowError(f"immutable artifact drift: {path}")
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return normalized


def _relative_archived_file(path: Path, archive_root: Path, *, label: str) -> str:
    resolved = path.expanduser().resolve(strict=True)
    try:
        relative = resolved.relative_to(archive_root)
    except ValueError as exc:
        raise LevelCWorkflowError(f"{label} must be inside the archived runs root") from exc
    if resolved.is_symlink() or not resolved.is_file():
        raise LevelCWorkflowError(f"{label} must be a regular archived file")
    return relative.as_posix()


def _sha256_token(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token.startswith("sha256:") else f"sha256:{token}"


def _validate_attempt_catalog(path: Path) -> dict[str, Any]:
    source = path.resolve(strict=True)
    with source.open("rb") as handle:
        header = handle.read(16)
    if header != b"SQLite format 3\x00":
        raise LevelCWorkflowError("archived attempt catalog has an invalid SQLite header")
    try:
        connection = sqlite3.connect(f"{source.as_uri()}?mode=ro&immutable=1", uri=True)
        try:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required = {"metadata", "run_signatures", "attempt_rows"}
            if not required.issubset(tables):
                raise LevelCWorkflowError(
                    "archived attempt catalog is missing required tables: "
                    + ", ".join(sorted(required - tables))
                )
            attempt_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(attempt_rows)").fetchall()
            }
            required_attempt_columns = {
                "run_id",
                "row_key",
                "row_index",
                "attempt_id",
                "is_tombstoned",
                "has_full_backtest_36m",
                "row_json",
                "updated_at",
            }
            signature_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(run_signatures)"
                ).fetchall()
            }
            if not required_attempt_columns.issubset(attempt_columns) or not {
                "run_id",
                "signature_json",
                "row_count",
                "updated_at",
            }.issubset(signature_columns):
                raise LevelCWorkflowError("archived attempt catalog schema is incompatible")
            metadata = dict(
                connection.execute(
                    "SELECT key, value FROM metadata WHERE key IN ('schema_version', 'index_signature')"
                ).fetchall()
            )
            if str(metadata.get("schema_version") or "") != str(
                CATALOG_INDEX_SCHEMA_VERSION
            ):
                raise LevelCWorkflowError("archived attempt catalog schema version differs")
        finally:
            connection.close()
    except (sqlite3.Error, OSError) as exc:
        raise LevelCWorkflowError("archived attempt catalog is not immutably readable") from exc
    return {
        "schema_version": str(CATALOG_INDEX_SCHEMA_VERSION),
        "required_tables": ["attempt_rows", "metadata", "run_signatures"],
    }


def _validate_legacy_controls(
    *,
    path: Path,
    archive_root: Path,
    archive_id: str,
    controls_generation_id: str,
    catalog_path: Path,
    catalog_sha256: str,
    nested_report_path: Path,
    nested_report_sha256: str,
) -> dict[str, Any]:
    controls = _load_json(path, label="legacy controls")
    identity = controls.get("identity")
    integrity = controls.get("integrity")
    if (
        controls.get("schema") != "legacy-controls-manifest-v1"
        or not isinstance(identity, dict)
        or identity.get("schema") != "legacy-controls-manifest-v1"
        or not isinstance(integrity, dict)
    ):
        raise LevelCWorkflowError("legacy controls schema is invalid")
    controls_generation_id = _require_generation_id(
        controls_generation_id, label="legacy controls expected generation"
    )
    identity_hash = _sha256_bytes(
        json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    )
    if (
        integrity.get("identity_hash_algorithm") != "sha256"
        or integrity.get("self_hash") != identity_hash
        or integrity.get("manifest_id") != identity_hash
    ):
        raise LevelCWorkflowError("legacy controls self identity is invalid")
    context = identity.get("archive_context")
    if not isinstance(context, dict):
        raise LevelCWorkflowError("legacy controls archive context is missing")
    projected_root = Path(str(context.get("projected_archived_runs_root") or "")).resolve(
        strict=False
    )
    if (
        context.get("archive_id") != archive_id
        or context.get("new_research_generation_id") != controls_generation_id
        or projected_root != archive_root
        or context.get("archive_relative_base") != archive_root.name
        or context.get("reference_only") is not True
    ):
        raise LevelCWorkflowError("legacy controls archive context differs from bootstrap")
    expected_exclusion = {
        "active_seeding": "excluded",
        "active_candidate_scans": "excluded",
        "active_selection": "excluded",
        "active_optimizer_candidates": "excluded",
        "empirical_priors": "excluded",
        "permitted_use": "post_campaign_comparison_only",
        "copy_profiles": False,
        "edit_active_runs": False,
    }
    exclusion = identity.get("exclusion_contract")
    if not isinstance(exclusion, dict) or any(
        exclusion.get(key) != value for key, value in expected_exclusion.items()
    ):
        raise LevelCWorkflowError("legacy controls exclusion contract is invalid")
    sources = identity.get("source_artifacts")
    if not isinstance(sources, dict):
        raise LevelCWorkflowError("legacy controls source artifacts are missing")
    expected_sources = {
        "exact_catalog_database": (catalog_path, catalog_sha256),
        "nested_evidence_report": (nested_report_path, nested_report_sha256),
    }
    if not set(expected_sources).issubset(sources):
        raise LevelCWorkflowError("legacy controls required source artifacts are missing")
    verified_sources: dict[str, dict[str, str]] = {}
    for key, (actual_path, actual_hash) in expected_sources.items():
        record = sources.get(key)
        if not isinstance(record, dict):
            raise LevelCWorkflowError(f"legacy controls source artifact is missing: {key}")
        relative = Path(str(record.get("archive_relative_path") or ""))
        projected = Path(str(record.get("projected_archived_path") or "")).resolve(
            strict=False
        )
        expected_path = (archive_root / relative).resolve(strict=False)
        actual_relative = actual_path.resolve(strict=True).relative_to(archive_root)
        if (
            relative.is_absolute()
            or relative.as_posix() != actual_relative.as_posix()
            or projected != expected_path
            or actual_path.resolve(strict=True) != expected_path.resolve(strict=True)
            or _sha256_token(record.get("sha256")) != actual_hash
        ):
            raise LevelCWorkflowError(f"legacy controls source artifact differs: {key}")
        verified_sources[key] = {
            "relative_path": relative.as_posix(),
            "sha256": actual_hash,
        }
    return {
        "manifest_id": identity_hash,
        "control_set_id": str(identity.get("control_set_id") or ""),
        "verified_sources": verified_sources,
    }


def _validate_completed_nested_report(
    report: Mapping[str, Any], *, report_path: Path, controls: Mapping[str, Any]
) -> dict[str, Any]:
    identity = controls.get("identity") if isinstance(controls, Mapping) else None
    categories = identity.get("categories") if isinstance(identity, Mapping) else None
    campaign_controls = (
        categories.get("campaign_controls") if isinstance(categories, Mapping) else None
    )
    expected = campaign_controls.get("nested") if isinstance(campaign_controls, Mapping) else None
    if not isinstance(expected, Mapping):
        raise LevelCWorkflowError("legacy controls nested campaign contract is missing")
    folds = report.get("fold_results")
    if (
        report.get("status") != "complete"
        or report.get("campaign_id") != expected.get("campaign_id")
        or report.get("evidence_campaign_plan_id")
        != expected.get("evidence_campaign_plan_id")
        or report.get("attempt_cohort_manifest_id")
        != expected.get("attempt_cohort_manifest_id")
        or int(report.get("attempt_count") or 0) != int(expected.get("attempt_count") or 0)
        or int(report.get("portfolio_result_count") or 0)
        != int(expected.get("portfolio_result_count") or 0)
        or report.get("selection_basis") != expected.get("selection_basis")
        or not isinstance(folds, list)
        or len(folds) != int(expected.get("fold_count") or 0)
    ):
        raise LevelCWorkflowError("completed nested report campaign semantics differ")
    prior_test_end: datetime | None = None
    geometry: list[dict[str, Any]] = []
    expected_attempts: set[str] | None = None
    for index, row in enumerate(folds, start=1):
        fold = row.get("fold") if isinstance(row, Mapping) else None
        if not isinstance(fold, Mapping) or row.get("status") != "complete":
            raise LevelCWorkflowError("completed nested report contains an incomplete fold")
        train_start = datetime.fromisoformat(str(fold.get("train_start"))[:10])
        train_end = datetime.fromisoformat(str(fold.get("train_end"))[:10])
        test_start = datetime.fromisoformat(str(fold.get("test_start"))[:10])
        test_end = datetime.fromisoformat(str(fold.get("test_end"))[:10])
        train_months = (train_end.year - train_start.year) * 12 + train_end.month - train_start.month
        test_months = (test_end.year - test_start.year) * 12 + test_end.month - test_start.month
        if (
            str(fold.get("fold_id") or "") != f"fold-{index:02d}"
            or int(fold.get("embargo_days") or -1) != 15
            or train_months != 36
            or test_months != 6
            or test_start <= train_end
            or (prior_test_end is not None and test_start <= prior_test_end)
        ):
            raise LevelCWorkflowError("completed nested report fold geometry is invalid")
        records = row.get("records")
        if not isinstance(records, list) or len(records) != int(
            expected.get("attempt_count") or 0
        ):
            raise LevelCWorkflowError("completed nested report strategy accounting differs")
        attempt_ids = [str(record.get("attempt_id") or "") for record in records]
        if any(not value for value in attempt_ids) or len(set(attempt_ids)) != len(
            attempt_ids
        ):
            raise LevelCWorkflowError("completed nested report attempt membership is invalid")
        observed_attempts = set(attempt_ids)
        if expected_attempts is None:
            expected_attempts = observed_attempts
        elif observed_attempts != expected_attempts:
            raise LevelCWorkflowError("completed nested report fold membership differs")
        train_nonviable = sum(
            record.get("train_validation_status") == "nonviable" for record in records
        )
        outer_nonviable = sum(
            record.get("outer_validation_status") == "nonviable" for record in records
        )
        if (
            any(
                record.get("train_validation_status") not in {"valid", "nonviable"}
                for record in records
            )
            or train_nonviable != int(row.get("train_nonviable_count") or 0)
            or outer_nonviable != int(row.get("outer_nonviable_count") or 0)
            or int(row.get("outer_failed_count") or 0) != 0
        ):
            raise LevelCWorkflowError("completed nested report terminal accounting differs")
        prior_test_end = test_end
        geometry.append(dict(fold))
    portfolio_path = (
        report_path.parent / "portfolio-validation" / "nested-temporal-results.json"
    )
    try:
        portfolio_results = json.loads(portfolio_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelCWorkflowError("completed nested portfolio artifact is invalid") from exc
    if not isinstance(portfolio_results, list) or len(portfolio_results) != int(
        expected.get("portfolio_result_count") or 0
    ):
        raise LevelCWorkflowError("completed nested portfolio accounting differs")
    return {
        "campaign_id": report["campaign_id"],
        "fold_geometry": geometry,
        "attempt_count": len(expected_attempts or set()),
        "portfolio_result_count": len(portfolio_results),
    }


def bootstrap_level_c(
    *,
    config: AppConfig,
    active_runs_root: Path,
    archive_root: Path,
    archived_attempt_catalog: Path,
    archived_attempt_catalog_sha256: str,
    legacy_controls: Path,
    legacy_controls_sha256: str,
    completed_nested_report: Path,
    completed_nested_report_sha256: str,
    archive_id: str,
    new_generation_id: str,
    lake_semantic_sha256: str,
    source_snapshot_sha256: str,
    universe_id: str,
    universe_manifest_sha256: str,
    worker_contract_id: str,
    worker_contract_sha256: str,
    worker_image: str,
    global_seed: int,
) -> dict[str, Any]:
    active_root = active_runs_root.expanduser().resolve(strict=False)
    if active_root != config.runs_root.resolve(strict=False):
        raise LevelCWorkflowError("active runs root must match the configured runs root")
    archive = archive_root.expanduser().resolve(strict=True)
    if not archive.is_dir() or archive.is_symlink():
        raise LevelCWorkflowError("archive root must be a real directory")
    _assert_bootstrap_root(active_root)
    archive_generation_handoff = _archive_generation_handoff(
        active_root, str(new_generation_id)
    )
    _assert_bootstrap_partial_order(
        active_root,
        allow_archive_generation_handoff=archive_generation_handoff is not None,
    )
    paths = _control_paths(active_root)
    named_files = {
        "archived_attempt_catalog": (archived_attempt_catalog, archived_attempt_catalog_sha256),
        "completed_nested_report": (completed_nested_report, completed_nested_report_sha256),
    }
    verified: dict[str, dict[str, str]] = {}
    for name, (path, expected) in named_files.items():
        expected_hash = _require_sha256(expected, label=f"{name} sha256")
        observed = _file_sha256(path.expanduser().resolve(strict=True))
        if observed != expected_hash:
            raise LevelCWorkflowError(f"{name} hash mismatch")
        verified[name] = {
            "relative_path": _relative_archived_file(path, archive, label=name),
            "sha256": observed,
        }
    controls_path = legacy_controls.expanduser().resolve(strict=True)
    controls_expected_hash = _require_sha256(
        legacy_controls_sha256, label="legacy_controls sha256"
    )
    controls_observed_hash = _file_sha256(controls_path)
    if controls_observed_hash != controls_expected_hash:
        raise LevelCWorkflowError("legacy_controls hash mismatch")
    controls_payload = _load_json(controls_path, label="legacy controls")
    controls_generation_id = str(new_generation_id)
    if archive_generation_handoff is not None:
        controls_generation_id = str(archive_generation_handoff["prior_generation_id"])
    controls_validation = _validate_legacy_controls(
        path=controls_path,
        archive_root=archive,
        archive_id=str(archive_id),
        controls_generation_id=controls_generation_id,
        catalog_path=archived_attempt_catalog,
        catalog_sha256=verified["archived_attempt_catalog"]["sha256"],
        nested_report_path=completed_nested_report,
        nested_report_sha256=verified["completed_nested_report"]["sha256"],
    )
    catalog_validation = _validate_attempt_catalog(archived_attempt_catalog)
    verified["legacy_controls"] = {
        "path": str(controls_path),
        "sha256": controls_observed_hash,
        "manifest_id": controls_validation["manifest_id"],
    }
    nested_report = _load_json(completed_nested_report, label="completed nested report")
    report_validation = _validate_completed_nested_report(
        nested_report,
        report_path=completed_nested_report.resolve(strict=True),
        controls=controls_payload,
    )
    cutoff_plans = build_initial_four_cutoff_plans(
        completed_nested_report, global_seed=int(global_seed)
    )
    prepared_at = str(
        nested_report.get("completed_at")
        or nested_report.get("generated_at")
        or nested_report.get("created_at")
        or (controls_payload.get("identity") or {}).get("created_at_utc")
        or ""
    ).strip()
    if not prepared_at:
        raise LevelCWorkflowError("archive evidence has no stable preparation timestamp")
    archive_identity = {
        "schema_version": BOOTSTRAP_RECEIPT_SCHEMA,
        "archive_id": str(archive_id),
        "archived_runs_root": str(archive),
        "archive_prepared_at": prepared_at,
        "verified_artifacts": verified,
        "attempt_catalog_validation": catalog_validation,
        "completed_nested_report_validation": report_validation,
    }
    archive_receipt = {
        **archive_identity,
        "receipt_sha256": canonical_sha256(archive_identity),
    }
    _create_or_verify(paths["archive_receipt"], archive_receipt)

    runtime_lock = build_runtime_policy_lock(
        config, worker_contract_sha256=_require_sha256(worker_contract_sha256, label="worker contract")
    )
    policy = policy_lock_provenance(runtime_lock)
    profile_lock = build_profile_model_source_lock(
        Path(config.fuzzfolio.workspace_root or config.repo_root.parent / "Trading-Dashboard")
    )
    provenance = {
        "lake_semantic_sha256": _require_sha256(lake_semantic_sha256, label="lake semantic"),
        "source_snapshot_sha256": _require_sha256(source_snapshot_sha256, label="source snapshot"),
        "universe_id": str(universe_id),
        "universe_manifest_sha256": _require_sha256(universe_manifest_sha256, label="universe manifest"),
        "worker_contract_id": str(worker_contract_id),
        "worker_contract_sha256": _require_sha256(worker_contract_sha256, label="worker contract"),
        "worker_image": str(worker_image),
        **policy,
    }
    generation = {
        "schema_name": GENERATION_SCHEMA_NAME,
        "schema_version": GENERATION_SCHEMA_VERSION,
        "new_generation_id": str(new_generation_id),
        "created_at": prepared_at,
        "archive_linkage": {
            "archive_id": str(archive_id),
            "archive_manifest_path": str(paths["archive_receipt"]),
            "archived_runs_root": str(archive),
            "archive_prepared_at": prepared_at,
        },
        "source_runs_root": str(archive),
        "destination_runs_root": str(active_root),
        "archived_inventory": {
            "kind": "manual-atomic-archive-linkage",
            "verified_artifacts": verified,
        },
        "restore_instructions": [],
        "provenance": provenance,
    }
    if archive_generation_handoff is not None:
        generation["archive_generation_handoff"] = archive_generation_handoff
    generation_path = active_root / GENERATION_MANIFEST_NAME
    _create_or_replace_archive_handoff_generation_manifest(
        generation_path, generation, archive_generation_handoff
    )
    generation_sha256 = _file_sha256(generation_path)
    source_coverage_end = max(plan["outer_test_end"] for plan in cutoff_plans)
    protocol_identity = {
        "schema_version": LEVEL_C_PROTOCOL_SCHEMA,
        "protocol_name": "level-c-initial-four-cutoffs",
        "protocol_version": "v1",
        "status": "frozen",
        "research_generation_id": str(new_generation_id),
        "research_generation_manifest_sha256": generation_sha256,
        "source_coverage_end": source_coverage_end,
        **provenance,
        "global_seed": int(global_seed),
        "no_global_priors": True,
        "no_outer_feedback": True,
        "cutoff_plans": cutoff_plans,
    }
    if paths["protocol"].exists():
        protocol = load_level_c_protocol(paths["protocol"])
        for key, value in protocol_identity.items():
            if protocol.get(key) != value:
                raise LevelCWorkflowError(f"existing protocol drift: {key}")
    else:
        protocol = create_level_c_protocol(paths["protocol"], protocol_identity)
    if paths["authority"].exists():
        authority = load_level_c_protocol_authority(
            paths["authority"],
            generation_manifest_path=generation_path,
            protocol_path=paths["protocol"],
        )
    else:
        authority = create_level_c_protocol_authority(
            paths["authority"],
            generation_manifest_path=generation_path,
            protocol_path=paths["protocol"],
        )
    plans: dict[str, dict[str, Any]] = {}
    workflow_semantics: dict[str, Any] | None = None
    for key in "ABCD":
        expected = build_level_c_execution_plan(
            active_root, paths["protocol"], paths["authority"], key
        )
        plan_path = paths["control"] / f"execution-plan-{key}.json"
        if plan_path.exists():
            observed = load_authoritative_level_c_execution_plan(plan_path)
            if canonical_json(observed) != canonical_json(expected):
                raise LevelCWorkflowError(f"existing execution plan {key} drift")
        else:
            observed = create_level_c_execution_plan(plan_path, expected)
        observed_semantics = _operator_semantics(observed)
        if workflow_semantics is None:
            workflow_semantics = observed_semantics
        elif observed_semantics != workflow_semantics:
            raise LevelCWorkflowError("cutoff workflow semantics differ")
        plans[key] = {"path": str(plan_path), "plan_id": observed["plan_id"]}
    result_identity = {
        "schema_version": "autoresearch-level-c-bootstrap-result-v1",
        "generation_manifest_path": str(generation_path),
        "generation_manifest_sha256": generation_sha256,
        "archive_linkage_receipt_sha256": archive_receipt["receipt_sha256"],
        "protocol_path": str(paths["protocol"]),
        "protocol_manifest_id": protocol["protocol_manifest_id"],
        "authority_path": str(paths["authority"]),
        "authority_id": authority["authority_id"],
        "runtime_policy_lock_sha256": runtime_lock["policy_lock_sha256"],
        "profile_model_source_lock_sha256": profile_lock["source_lock_sha256"],
        "operator_semantics": workflow_semantics,
        "execution_plans": plans,
    }
    result = {**result_identity, "bootstrap_id": canonical_sha256(result_identity)}
    _create_or_verify(paths["bootstrap"], result)
    return result


def _artifact_receipts(paths: list[Path]) -> list[dict[str, str]]:
    receipts = []
    for path in paths:
        resolved = path.resolve(strict=True)
        receipts.append({"path": str(resolved), "sha256": _file_sha256(resolved)})
    return sorted(receipts, key=lambda row: row["path"])


def _stage_receipt_path(active_root: Path, cutoff: str, stage: str) -> Path:
    return active_root / "derived" / "level-c" / "campaigns" / cutoff / "stages" / f"{stage}.json"


def _assert_stage_receipt_prefix(active_root: Path, cutoff: str) -> None:
    missing_seen = False
    for stage in STAGES:
        path = _stage_receipt_path(active_root, cutoff, stage)
        if not path.exists():
            missing_seen = True
        elif missing_seen:
            raise LevelCWorkflowError(
                f"ambiguous cutoff state: {stage} receipt exists after a missing predecessor"
            )


def _validate_stage_receipt(path: Path, *, plan_id: str, stage: str) -> dict[str, Any]:
    payload = _load_json(path, label=f"{stage} stage receipt")
    identity = {key: value for key, value in payload.items() if key != "receipt_sha256"}
    if (
        payload.get("schema_version") != STAGE_RECEIPT_SCHEMA
        or payload.get("execution_plan_id") != plan_id
        or payload.get("stage") != stage
        or payload.get("receipt_sha256") != canonical_sha256(identity)
    ):
        raise LevelCWorkflowError(f"invalid {stage} stage receipt")
    for artifact in payload.get("artifacts") or []:
        path_value = Path(str(artifact.get("path") or ""))
        if _file_sha256(path_value) != artifact.get("sha256"):
            raise LevelCWorkflowError(f"{stage} stage artifact drift: {path_value}")
    return payload


def _validate_atlas_stage_root(
    plan: Mapping[str, Any],
    *,
    run_root: Path | None = None,
    summary_path: Path | None = None,
    receipt: Mapping[str, Any] | None = None,
) -> None:
    """Require Level C Atlas execution and receipts to name the canonical root."""
    expected_root = resolve_level_c_atlas_run_root(plan, require_lineage=True).resolve()
    expected_summary = (expected_root / "atlas-lab-summary.json").resolve(strict=True)
    if run_root is not None and run_root.resolve(strict=True) != expected_root:
        raise LevelCWorkflowError("Atlas run root differs from the authoritative execution plan")
    if summary_path is not None and summary_path.resolve(strict=True) != expected_summary:
        raise LevelCWorkflowError("Atlas summary path differs from the authoritative execution plan")
    if receipt is not None:
        artifacts = receipt.get("artifacts") if isinstance(receipt, Mapping) else None
        if not isinstance(artifacts, list) or len(artifacts) != 1:
            raise LevelCWorkflowError("Atlas stage receipt must contain exactly one summary artifact")
        recorded = Path(str(artifacts[0].get("path") or "")).resolve(strict=True)
        if recorded != expected_summary:
            raise LevelCWorkflowError("Atlas stage receipt artifact is outside the authoritative Atlas root")


def _write_stage_receipt(
    *, active_root: Path, cutoff: str, stage: str, plan_id: str, outcome: str, artifacts: list[Path]
) -> dict[str, Any]:
    identity = {
        "schema_version": STAGE_RECEIPT_SCHEMA,
        "execution_plan_id": plan_id,
        "cutoff_key": cutoff,
        "stage": stage,
        "outcome": outcome,
        "artifacts": _artifact_receipts(artifacts),
    }
    payload = {**identity, "receipt_sha256": canonical_sha256(identity)}
    return _create_or_verify(_stage_receipt_path(active_root, cutoff, stage), payload)


def _runtime_from_plan(cls: type, arguments: Mapping[str, Any], **operational: Any) -> Any:
    names = {field.name for field in fields(cls)}
    values = {key: value for key, value in arguments.items() if key in names}
    values.update({key: value for key, value in operational.items() if key in names and value is not None})
    for key in ("execution_plan_path", "profile_path", "seed_plan_path", "trading_dashboard_root"):
        if values.get(key) is not None:
            values[key] = Path(str(values[key]))
    return cls(**values)


def _playhand_stage_resume_mode(*, campaign_root: Path, cutoff_resume: bool) -> bool:
    """Scope resume to the durable PlayHand campaign, not its Level C predecessor."""
    root = Path(campaign_root)
    journal_path = root / "play-hand-lab-execution-journal.json"
    if not root.exists():
        # A cutoff may resume after Atlas completed while this downstream campaign
        # has never started. PlayHand must create its first durable journal then.
        return False
    if not root.is_dir() or root.is_symlink():
        raise LevelCWorkflowError("PlayHand campaign path is not a regular directory")
    if not journal_path.is_file() or journal_path.is_symlink():
        raise LevelCWorkflowError(
            "existing PlayHand campaign is missing its durable execution journal"
        )
    if not cutoff_resume:
        raise LevelCWorkflowError(
            "existing PlayHand campaign requires Level C cutoff --resume"
        )
    # cmd_play_hand_lab owns strict journal lineage/state validation. Supplying
    # resume=True here guarantees it loads rather than creates that journal.
    return True


def _level_c_profile_snapshot_resolver(
    *, config: AppConfig, plan_path: Path, plan: Mapping[str, Any]
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Reconstruct the plan-bound profile snapshot sent to deep-replay workers."""
    arguments, _ = executor_arguments_from_plan(plan_path, executor="playhand", config=config)
    bound_root = Path(str(plan["bound_contract"]["profile_model_source_root"]))
    runtime = _runtime_from_plan(
        PlayHandLabRuntimeConfig,
        arguments,
        execution_plan_path=plan_path,
        trading_dashboard_root=bound_root,
    )

    def resolve(profile_payload: dict[str, Any]) -> dict[str, Any]:
        return _worker_ready_profile_snapshot(
            profile_payload,
            config=config,
            runtime=runtime,
        )

    return resolve


def _month_span(start: str, end: str) -> int:
    left = datetime.fromisoformat(start.replace("Z", "+00:00"))
    right = datetime.fromisoformat(end.replace("Z", "+00:00"))
    return max(1, (right.year - left.year) * 12 + right.month - left.month)


def _validate_nested_report(
    report_path: Path,
    *,
    selected_only: bool,
    expected_attempt_ids: set[str] | None = None,
) -> dict[str, Any]:
    report = _load_json(report_path, label="nested evidence report")
    if report.get("status") != "complete":
        raise LevelCWorkflowError("nested evidence report is not complete")
    portfolio_results: list[dict[str, Any]] = []
    membership = report.get("membership")
    fold_results = report.get("fold_results") or []
    if expected_attempt_ids is not None:
        if not isinstance(membership, dict):
            raise LevelCWorkflowError("nested report membership contract is missing")
        requested = membership.get("requested_attempt_ids")
        if (
            not isinstance(requested, list)
            or len(requested) != len(set(requested))
            or set(requested) != expected_attempt_ids
        ):
            raise LevelCWorkflowError("nested requested membership differs from frozen cohort")
        eligible_by_fold = membership.get("training_eligible_attempt_ids_by_fold") or {}
        terminal_by_fold = membership.get("training_terminal_attempt_ids_by_fold") or {}
        selected_by_fold = membership.get("selected_attempt_ids_by_fold") or {}
        outer_by_fold = membership.get("outer_terminal_attempt_ids_by_fold") or {}
        expected_fold_ids = {
            str((row.get("fold") or {}).get("fold_id") or "")
            for row in fold_results
            if isinstance(row, Mapping)
        }
        if (
            not expected_fold_ids
            or "" in expected_fold_ids
            or set(eligible_by_fold) != expected_fold_ids
            or set(terminal_by_fold) != expected_fold_ids
            or set(selected_by_fold) != expected_fold_ids
            or set(outer_by_fold) != expected_fold_ids
        ):
            raise LevelCWorkflowError("nested membership fold sets are incomplete")
        for fold_id in expected_fold_ids:
            eligible = list(eligible_by_fold.get(fold_id) or [])
            terminal = list(terminal_by_fold.get(fold_id) or [])
            if (
                len(eligible) != len(set(eligible))
                or len(terminal) != len(set(terminal))
                or set(eligible) & set(terminal)
                or set(eligible) | set(terminal) != expected_attempt_ids
            ):
                raise LevelCWorkflowError("nested training membership is incomplete")
            selected = list(selected_by_fold.get(fold_id) or [])
            outer = list(outer_by_fold.get(fold_id) or [])
            if (
                len(selected) != len(set(selected))
                or len(outer) != len(set(outer))
                or set(selected) != set(outer)
                or not set(selected).issubset(set(eligible))
            ):
                raise LevelCWorkflowError("nested selected/outer membership differs")
    portfolio_path_value = str(report.get("portfolio_results_path") or "").strip()
    if portfolio_path_value:
        portfolio_path = Path(portfolio_path_value)
        if _file_sha256(portfolio_path) != report.get("portfolio_results_sha256"):
            raise LevelCWorkflowError("nested portfolio results artifact drift")
        portfolio_payload = json.loads(portfolio_path.read_text(encoding="utf-8"))
        if not isinstance(portfolio_payload, list) or not all(
            isinstance(row, dict) for row in portfolio_payload
        ):
            raise LevelCWorkflowError("nested portfolio results are malformed")
        portfolio_results = portfolio_payload
    for fold_result in fold_results:
        if fold_result.get("status") != "complete" or int(
            fold_result.get("outer_failed_count") or 0
        ):
            raise LevelCWorkflowError("nested fold is not terminally complete")
        records = fold_result.get("records") or []
        attempt_ids = [str(record.get("attempt_id") or "") for record in records]
        if (
            len(records) != int(fold_result.get("strategy_count") or 0)
            or not all(attempt_ids)
            or len(set(attempt_ids)) != len(attempt_ids)
        ):
            raise LevelCWorkflowError("nested fold strategy accounting is incomplete")
        train_nonviable = sum(
            record.get("train_validation_status") == "nonviable" for record in records
        )
        if (
            any(
                record.get("train_validation_status") not in {"valid", "nonviable"}
                for record in records
            )
            or train_nonviable != int(fold_result.get("train_nonviable_count") or 0)
        ):
            raise LevelCWorkflowError("nested fold training accounting is inconsistent")
        selected = {
            str(item)
            for result in portfolio_results
            if str((result.get("fold") or {}).get("fold_id") or "")
            == str((fold_result.get("fold") or {}).get("fold_id") or "")
            for item in result.get("selected_attempt_ids") or []
        }
        observed = {
            str(record.get("attempt_id") or "")
            for record in records
            if record.get("outer_validation_status") in {"valid", "nonviable"}
        }
        if selected_only and observed != selected:
            raise LevelCWorkflowError("outer evidence membership differs from frozen selection")
        outer_nonviable = sum(
            record.get("outer_validation_status") == "nonviable" for record in records
        )
        if outer_nonviable != int(fold_result.get("outer_nonviable_count") or 0):
            raise LevelCWorkflowError("nested fold outer accounting is inconsistent")
        for record in records:
            if record.get("train_validation_status") == "valid" and not record.get("cell_receipt"):
                raise LevelCWorkflowError("training-valid record is missing its frozen cell receipt")
    return report


def _default_stage_handler(
    stage: str,
    *,
    config: AppConfig,
    active_root: Path,
    cutoff: str,
    plan_path: Path,
    plan: dict[str, Any],
    resume: bool,
    gateway_url: str | None,
    gateway_token: str | None,
    atlas_active_probes: int | None,
    playhand_active_runs: int | None,
    nested_max_workers: int,
    trading_dashboard_root: Path | None,
) -> tuple[str, list[Path]]:
    expected = plan["expected_artifacts"]
    if stage == "atlas":
        arguments, loaded = executor_arguments_from_plan(plan_path, executor="atlas", config=config)
        run_id = str(arguments.pop("run_id"))
        runtime = _runtime_from_plan(
            AtlasLabRuntimeConfig,
            arguments,
            execution_plan_path=plan_path,
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            active_probes=atlas_active_probes,
            trading_dashboard_root=trading_dashboard_root,
            resume=resume,
        )
        result = run_atlas_lab(
            config, run_id=run_id, runtime=runtime, phases=list(loaded["atlas_phases"])
        )
        if result.status != "completed":
            raise LevelCWorkflowError(f"Atlas ended with status {result.status}")
        _validate_atlas_stage_root(
            plan,
            run_root=result.run_root,
            summary_path=result.summary_path,
        )
        return "complete", [result.summary_path]
    if stage == "playhand":
        arguments, _ = executor_arguments_from_plan(plan_path, executor="playhand", config=config)
        campaign_root = Path(expected["playhand_campaign"]["resolved_path"])
        playhand_resume = _playhand_stage_resume_mode(
            campaign_root=campaign_root,
            cutoff_resume=resume,
        )
        runtime = _runtime_from_plan(
            PlayHandLabRuntimeConfig,
            arguments,
            execution_plan_path=plan_path,
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            active_runs=playhand_active_runs,
            trading_dashboard_root=trading_dashboard_root,
            resume=playhand_resume,
        )
        if cmd_play_hand_lab(runtime) != 0:
            raise LevelCWorkflowError("finite PlayHand coordinator failed")
        summary = campaign_root / "play-hand-lab-summary.json"
        if not summary.is_file():
            alternatives = list(campaign_root.glob("*summary*.json"))
            if len(alternatives) != 1:
                raise LevelCWorkflowError("finite PlayHand summary is missing or ambiguous")
            summary = alternatives[0]
        return "complete", [summary]
    if stage == "frozen_cohort":
        cohort_path = Path(expected["frozen_cohort"]["resolved_path"])
        profile_snapshot_resolver = _level_c_profile_snapshot_resolver(
            config=config,
            plan_path=plan_path,
            plan=plan,
        )
        if cohort_path.exists():
            cohort = validate_level_c_cohort(
                cohort_path,
                profile_snapshot_resolver=profile_snapshot_resolver,
            )
        else:
            cohort = freeze_level_c_cohort(
                runs_root=active_root,
                atlas_run_root=resolve_level_c_atlas_run_root(plan, require_lineage=True),
                playhand_campaign_id=plan["cutoff"]["playhand_campaign_id"],
                as_of_date=plan["cutoff"]["geometry"]["selection_end"],
                lake_manifest_sha256=plan["atlas_arguments"]["lake_manifest_sha256"],
                output_path=cohort_path,
                cohort_id=plan["cutoff"]["cohort_id"],
                profile_snapshot_resolver=profile_snapshot_resolver,
            )
        outcome = str(cohort.get("outcome") or "")
        return outcome, [cohort_path]

    cohort_path = Path(expected["frozen_cohort"]["resolved_path"])
    cohort = validate_level_c_cohort(
        cohort_path,
        profile_snapshot_resolver=_level_c_profile_snapshot_resolver(
            config=config,
            plan_path=plan_path,
            plan=plan,
        ),
    )
    workflow = _operator_semantics(plan)
    nested_campaign_id = f"{plan['cutoff']['cohort_id']}-nested"
    nested_root = active_root / "derived" / "nested-evidence" / nested_campaign_id
    report_path = nested_root / "nested-evidence-report.json"
    if not cohort.get("candidates"):
        terminal_path = nested_root / "zero-candidate-result.json"
        terminal = {
            "schema_version": "autoresearch-level-c-zero-candidate-v1",
            "status": "non_promotable",
            "outcome": cohort.get("outcome"),
            "cohort_manifest_id": cohort.get("manifest_id"),
        }
        _create_or_verify(terminal_path, terminal)
        return "no_candidate", [terminal_path]
    from .nested_pipeline import (
        prepare_nested_pipeline,
        run_nested_final_report_phase,
        run_nested_frozen_cells_phase,
        run_nested_frozen_portfolio_phase,
        run_nested_selected_outer_phase,
        run_nested_training_phase,
    )

    geometry = plan["cutoff"]["geometry"]
    context = prepare_nested_pipeline(
        config=config,
        campaign_id=nested_campaign_id,
        suite_name=str(workflow["suite_name"]),
        suite_config_path=config.repo_root / str(workflow["suite_config_relative_path"]),
        run_ids=None,
        attempt_ids=None,
        scope="all",
        start=geometry["training_start"],
        end=geometry["outer_test_end"],
        train_months=_month_span(geometry["training_start"], geometry["training_end"]),
        test_months=_month_span(geometry["outer_test_start"], geometry["outer_test_end"]),
        step_months=_month_span(geometry["outer_test_start"], geometry["outer_test_end"]),
        embargo_days=int(geometry["embargo_days"]),
        selection_basis=str(workflow["selection_basis"]),
        max_workers=max(1, int(nested_max_workers)),
        gateway_url=gateway_url,
        gateway_token=gateway_token,
        lake_manifest_sha256=plan["atlas_arguments"]["lake_manifest_sha256"],
        trading_dashboard_root=trading_dashboard_root,
        optimizer_backend=str(workflow["optimizer_backend"]),
        attempt_cohort=cohort_path,
        execution_plan_id=plan["plan_id"],
        bound_worker_contract_hash=plan["bound_contract"]["worker_contract_sha256"],
        bound_trading_dashboard_root=Path(
            plan["bound_contract"]["profile_model_source_root"]
        ),
        profile_model_source_lock=plan["bound_contract"]["profile_model_source_lock"],
    )
    handlers = {
        "training_evidence": run_nested_training_phase,
        "frozen_cells": run_nested_frozen_cells_phase,
        "frozen_portfolio": run_nested_frozen_portfolio_phase,
        "selected_outer": run_nested_selected_outer_phase,
        "final_report": run_nested_final_report_phase,
    }
    phase_path = handlers[stage](context)
    phase = _load_json(phase_path, label=f"nested {stage} phase")
    if stage == "frozen_portfolio" and phase.get("status") in {
        "no_candidate",
        "no_consensus",
    }:
        return str(phase["status"]), [phase_path]
    if stage == "final_report":
        _validate_nested_report(
            report_path,
            selected_only=True,
            expected_attempt_ids={
                str(row.get("attempt_id") or "") for row in cohort.get("candidates") or []
            },
        )
        return "complete", [phase_path, report_path]
    return "complete", [phase_path]


def _development_policy(active_root: Path, plans: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    paths = _control_paths(active_root)
    receipts = {}
    for key in "AB":
        receipt = _validate_stage_receipt(
            _stage_receipt_path(active_root, key, "final_report"),
            plan_id=plans[key]["plan_id"],
            stage="final_report",
        )
        receipts[key] = receipt["receipt_sha256"]
    workflow = _operator_semantics(plans["A"])
    if _operator_semantics(plans["B"]) != workflow:
        raise LevelCWorkflowError("development cutoff workflow semantics differ")
    identity = {
        "schema_version": DEVELOPMENT_POLICY_SCHEMA,
        "development_cutoffs": receipts,
        "workflow_arguments": workflow,
        "no_validation_feedback": True,
    }
    payload = {**identity, "policy_sha256": canonical_sha256(identity)}
    return _create_or_verify(paths["development_policy"], payload)


def run_level_c_cutoff(
    *,
    config: AppConfig,
    active_runs_root: Path,
    cutoff: str,
    resume: bool,
    gateway_url: str | None = None,
    gateway_token: str | None = None,
    atlas_active_probes: int | None = None,
    playhand_active_runs: int | None = None,
    nested_max_workers: int = 32,
    trading_dashboard_root: Path | None = None,
    stage_handlers: Mapping[str, Callable[..., tuple[str, list[Path]]]] | None = None,
) -> dict[str, Any]:
    key = str(cutoff).upper()
    if key not in "ABCD":
        raise LevelCWorkflowError("cutoff must be A, B, C, or D")
    active_root = active_runs_root.resolve(strict=True)
    if active_root != config.runs_root.resolve(strict=False):
        raise LevelCWorkflowError("active runs root must match the configured runs root")
    paths = _control_paths(active_root)
    bootstrap = _load_json(paths["bootstrap"], label="bootstrap result")
    plan_path = paths["control"] / f"execution-plan-{key}.json"
    plan = load_authoritative_level_c_execution_plan(plan_path, config=config)
    workflow = _operator_semantics(plan)
    if bootstrap.get("operator_semantics") != workflow:
        raise LevelCWorkflowError("bootstrap operator semantics differ from this runtime")
    plans = {
        token: load_authoritative_level_c_execution_plan(
            paths["control"] / f"execution-plan-{token}.json", config=config
        )
        for token in "ABCD"
    }
    _assert_stage_receipt_prefix(active_root, key)
    if key in "CD":
        policy = _load_json(paths["development_policy"], label="development policy")
        identity = {field: value for field, value in policy.items() if field != "policy_sha256"}
        if (
            policy.get("schema_version") != DEVELOPMENT_POLICY_SCHEMA
            or policy.get("policy_sha256") != canonical_sha256(identity)
            or set((policy.get("development_cutoffs") or {}).keys()) != {"A", "B"}
            or policy.get("no_validation_feedback") is not True
        ):
            raise LevelCWorkflowError("frozen development policy is invalid")
        if policy.get("workflow_arguments") != workflow:
            raise LevelCWorkflowError("frozen development policy semantics differ")
        for development_key in "AB":
            receipt = _validate_stage_receipt(
                _stage_receipt_path(active_root, development_key, "final_report"),
                plan_id=plans[development_key]["plan_id"],
                stage="final_report",
            )
            if policy["development_cutoffs"].get(development_key) != receipt["receipt_sha256"]:
                raise LevelCWorkflowError("frozen development policy receipt drift")
    handlers = dict(stage_handlers or {})
    completed: list[str] = []
    for stage in STAGES:
        receipt_path = _stage_receipt_path(active_root, key, stage)
        if receipt_path.exists():
            if not resume:
                raise LevelCWorkflowError(f"cutoff already has stage state; pass --resume: {stage}")
            receipt = _validate_stage_receipt(
                receipt_path, plan_id=plan["plan_id"], stage=stage
            )
            if stage == "atlas" and stage not in handlers:
                _validate_atlas_stage_root(plan, receipt=receipt)
            completed.append(stage)
            if receipt.get("outcome") in NON_PROMOTABLE_OUTCOMES:
                break
            continue
        handler = handlers.get(stage, _default_stage_handler)
        outcome, artifacts = handler(
            stage,
            config=config,
            active_root=active_root,
            cutoff=key,
            plan_path=plan_path,
            plan=plan,
            resume=resume,
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            atlas_active_probes=atlas_active_probes,
            playhand_active_runs=playhand_active_runs,
            nested_max_workers=nested_max_workers,
            trading_dashboard_root=trading_dashboard_root,
        )
        normalized_outcome = str(outcome or "")
        if normalized_outcome not in {
            "complete",
            "candidates_frozen",
            "no_defensible_candidates",
            *NON_PROMOTABLE_OUTCOMES,
        }:
            raise LevelCWorkflowError(f"stage {stage} returned unsupported outcome {normalized_outcome}")
        if normalized_outcome == "no_defensible_candidates":
            normalized_outcome = "no_candidate"
        _write_stage_receipt(
            active_root=active_root,
            cutoff=key,
            stage=stage,
            plan_id=plan["plan_id"],
            outcome=normalized_outcome,
            artifacts=artifacts,
        )
        completed.append(stage)
        if normalized_outcome in NON_PROMOTABLE_OUTCOMES:
            break
    if key == "B" and completed and completed[-1] == "final_report":
        _development_policy(active_root, plans)
    return {
        "status": "complete" if completed and completed[-1] == "final_report" else "non_promotable",
        "cutoff": key,
        "execution_plan_id": plan["plan_id"],
        "completed_stages": completed,
        "development_policy_path": str(paths["development_policy"])
        if paths["development_policy"].exists()
        else None,
    }


def audit_level_c(
    *, config: AppConfig, active_runs_root: Path, cutoff: str | None = None
) -> dict[str, Any]:
    active_root = active_runs_root.expanduser().resolve(strict=True)
    paths = _control_paths(active_root)
    bootstrap = _load_json(paths["bootstrap"], label="bootstrap result")
    bootstrap_identity = {key: value for key, value in bootstrap.items() if key != "bootstrap_id"}
    if bootstrap.get("bootstrap_id") != canonical_sha256(bootstrap_identity):
        raise LevelCWorkflowError("bootstrap result drift")
    archive_receipt = _load_json(paths["archive_receipt"], label="archive linkage receipt")
    archive_identity = {
        key: value for key, value in archive_receipt.items() if key != "receipt_sha256"
    }
    if archive_receipt.get("receipt_sha256") != canonical_sha256(archive_identity):
        raise LevelCWorkflowError("archive linkage receipt drift")
    archive_root = Path(str(archive_receipt.get("archived_runs_root") or "")).resolve(strict=True)
    for artifact in (archive_receipt.get("verified_artifacts") or {}).values():
        source = (
            Path(str(artifact.get("path"))).resolve(strict=True)
            if artifact.get("path")
            else archive_root / str(artifact.get("relative_path") or "")
        )
        if _file_sha256(source) != artifact.get("sha256"):
            raise LevelCWorkflowError(f"archived bootstrap artifact drift: {source}")
    generation_path = active_root / GENERATION_MANIFEST_NAME
    if _file_sha256(generation_path) != bootstrap.get("generation_manifest_sha256"):
        raise LevelCWorkflowError("generation manifest drift")
    generation_manifest = _load_json(generation_path, label="generation manifest")
    protocol = load_level_c_protocol(paths["protocol"])
    authority = load_level_c_protocol_authority(
        paths["authority"], generation_manifest_path=generation_path, protocol_path=paths["protocol"]
    )
    if (
        bootstrap.get("protocol_manifest_id") != protocol.get("protocol_manifest_id")
        or bootstrap.get("authority_id") != authority.get("authority_id")
    ):
        raise LevelCWorkflowError("bootstrap authority identities drift")
    keys = [str(cutoff).upper()] if cutoff else list("ABCD")
    plans = {
        key: load_authoritative_level_c_execution_plan(
            paths["control"] / f"execution-plan-{key}.json", config=config
        )
        for key in keys
    }
    bootstrap_plans = bootstrap.get("execution_plans") or {}
    for key, plan in plans.items():
        bound_plan = bootstrap_plans.get(key) or {}
        if (
            bound_plan.get("plan_id") != plan.get("plan_id")
            or Path(str(bound_plan.get("path") or "")).resolve(strict=False)
            != (paths["control"] / f"execution-plan-{key}.json").resolve(strict=False)
        ):
            raise LevelCWorkflowError(f"bootstrap execution plan {key} drift")
        bound_contract = plan.get("bound_contract") or {}
        runtime_lock = bound_contract.get("runtime_policy_lock") or {}
        profile_lock = bound_contract.get("profile_model_source_lock") or {}
        if (
            bootstrap.get("runtime_policy_lock_sha256")
            != runtime_lock.get("policy_lock_sha256")
            or bootstrap.get("profile_model_source_lock_sha256")
            != profile_lock.get("source_lock_sha256")
        ):
            raise LevelCWorkflowError(f"bootstrap policy lock {key} drift")
        if bootstrap.get("operator_semantics") != _operator_semantics(plan):
            raise LevelCWorkflowError(f"bootstrap workflow semantics {key} drift")
    cutoff_results: dict[str, Any] = {}
    for key, plan in plans.items():
        _assert_stage_receipt_prefix(active_root, key)
        stages = []
        terminal = None
        for stage in STAGES:
            receipt_path = _stage_receipt_path(active_root, key, stage)
            if not receipt_path.exists():
                break
            receipt = _validate_stage_receipt(
                receipt_path, plan_id=plan["plan_id"], stage=stage
            )
            if stage == "atlas":
                _validate_atlas_stage_root(plan, receipt=receipt)
            stages.append(stage)
            terminal = receipt.get("outcome")
        if "frozen_cohort" in stages:
            cohort_path = Path(plan["expected_artifacts"]["frozen_cohort"]["resolved_path"])
            cohort = validate_level_c_cohort(
                cohort_path,
                profile_snapshot_resolver=_level_c_profile_snapshot_resolver(
                    config=config,
                    plan_path=paths["control"] / f"execution-plan-{key}.json",
                    plan=plan,
                ),
            )
            if int(cohort.get("candidate_count") or 0) != len(cohort.get("candidates") or []):
                raise LevelCWorkflowError("frozen cohort accounting mismatch")
        if "selected_outer" in stages:
            nested_id = f"{plan['cutoff']['cohort_id']}-nested"
            _validate_nested_report(
                active_root / "derived" / "nested-evidence" / nested_id / "nested-evidence-report.json",
                selected_only=True,
                expected_attempt_ids={
                    str(row.get("attempt_id") or "")
                    for row in validate_level_c_cohort(
                        Path(plan["expected_artifacts"]["frozen_cohort"]["resolved_path"]),
                        profile_snapshot_resolver=_level_c_profile_snapshot_resolver(
                            config=config,
                            plan_path=paths["control"] / f"execution-plan-{key}.json",
                            plan=plan,
                        ),
                    ).get("candidates")
                    or []
                },
            )
        cutoff_results[key] = {"stages": stages, "terminal_outcome": terminal}
    policy_result = None
    if paths["development_policy"].exists():
        policy = _load_json(paths["development_policy"], label="development policy")
        identity = {field: value for field, value in policy.items() if field != "policy_sha256"}
        if policy.get("policy_sha256") != canonical_sha256(identity):
            raise LevelCWorkflowError("development policy drift")
        if set((policy.get("development_cutoffs") or {}).keys()) != {"A", "B"}:
            raise LevelCWorkflowError("development policy contains validation feedback")
        if policy.get("workflow_arguments") != bootstrap.get("operator_semantics"):
            raise LevelCWorkflowError("development policy workflow semantics drift")
        all_plans = {
            key: load_authoritative_level_c_execution_plan(
                paths["control"] / f"execution-plan-{key}.json", config=config
            )
            for key in "AB"
        }
        for key, plan in all_plans.items():
            receipt = _validate_stage_receipt(
                _stage_receipt_path(active_root, key, "final_report"),
                plan_id=plan["plan_id"],
                stage="final_report",
            )
            if policy["development_cutoffs"].get(key) != receipt["receipt_sha256"]:
                raise LevelCWorkflowError("development policy receipt drift")
        policy_result = policy["policy_sha256"]
    identity = {
        "schema_version": AUDIT_SCHEMA,
        "status": "valid",
        "generation_manifest_sha256": bootstrap["generation_manifest_sha256"],
        "protocol_manifest_id": protocol["protocol_manifest_id"],
        "authority_id": authority["authority_id"],
        "cutoffs": cutoff_results,
        "development_policy_sha256": policy_result,
        "no_validation_feedback": True,
    }
    if "archive_generation_handoff" in generation_manifest:
        identity["archive_generation_handoff"] = generation_manifest["archive_generation_handoff"]
    return {**identity, "audit_sha256": canonical_sha256(identity)}


def format_audit(payload: Mapping[str, Any]) -> str:
    lines = [
        "Level C audit: valid",
        f"Protocol: {payload.get('protocol_manifest_id')}",
        f"Authority: {payload.get('authority_id')}",
    ]
    for key, result in (payload.get("cutoffs") or {}).items():
        lines.append(
            f"Cutoff {key}: {len(result.get('stages') or [])}/{len(STAGES)} stages; "
            f"outcome={result.get('terminal_outcome') or 'not_started'}"
        )
    return "\n".join(lines)


def add_level_c_cli(subparsers: argparse._SubParsersAction) -> None:
    bootstrap = subparsers.add_parser(
        "level-c-bootstrap", help="Bind a completed manual archive to a new Level C generation."
    )
    bootstrap.add_argument("--active-runs-root", type=Path, required=True)
    bootstrap.add_argument("--archive-root", type=Path, required=True)
    bootstrap.add_argument("--archived-attempt-catalog", type=Path, required=True)
    bootstrap.add_argument("--archived-attempt-catalog-sha256", required=True)
    bootstrap.add_argument("--legacy-controls", type=Path, required=True)
    bootstrap.add_argument("--legacy-controls-sha256", required=True)
    bootstrap.add_argument("--completed-nested-report", type=Path, required=True)
    bootstrap.add_argument("--completed-nested-report-sha256", required=True)
    bootstrap.add_argument("--archive-id", required=True)
    bootstrap.add_argument("--new-generation-id", required=True)
    bootstrap.add_argument("--lake-semantic-sha256", required=True)
    bootstrap.add_argument("--source-snapshot-sha256", required=True)
    bootstrap.add_argument("--universe-id", required=True)
    bootstrap.add_argument("--universe-manifest-sha256", required=True)
    bootstrap.add_argument("--worker-contract-id", required=True)
    bootstrap.add_argument("--worker-contract-sha256", required=True)
    bootstrap.add_argument("--worker-image", required=True)
    bootstrap.add_argument("--global-seed", type=int, required=True)
    bootstrap.add_argument("--json", action="store_true")

    run = subparsers.add_parser(
        "level-c-run-cutoff", help="Run one authoritative Level C cutoff stage graph."
    )
    run.add_argument("--active-runs-root", type=Path, required=True)
    run.add_argument("--cutoff", choices=list("ABCD"), required=True)
    run.add_argument("--resume", action="store_true")
    run.add_argument("--gateway-url")
    run.add_argument("--gateway-token")
    run.add_argument("--atlas-active-probes", type=int)
    run.add_argument("--playhand-active-runs", type=int)
    run.add_argument("--nested-max-workers", type=int, default=32)
    run.add_argument("--trading-dashboard-root", type=Path)
    run.add_argument("--json", action="store_true")

    audit = subparsers.add_parser(
        "level-c-audit", help="Non-mutating verification of Level C authority and stage receipts."
    )
    audit.add_argument("--active-runs-root", type=Path, required=True)
    audit.add_argument("--cutoff", choices=list("ABCD"))
    audit.add_argument("--json", action="store_true")


def dispatch_level_c_cli(args: argparse.Namespace) -> int | None:
    if args.command == "level-c-bootstrap":
        payload = bootstrap_level_c(
            config=load_config(),
            active_runs_root=args.active_runs_root,
            archive_root=args.archive_root,
            archived_attempt_catalog=args.archived_attempt_catalog,
            archived_attempt_catalog_sha256=args.archived_attempt_catalog_sha256,
            legacy_controls=args.legacy_controls,
            legacy_controls_sha256=args.legacy_controls_sha256,
            completed_nested_report=args.completed_nested_report,
            completed_nested_report_sha256=args.completed_nested_report_sha256,
            archive_id=args.archive_id,
            new_generation_id=args.new_generation_id,
            lake_semantic_sha256=args.lake_semantic_sha256,
            source_snapshot_sha256=args.source_snapshot_sha256,
            universe_id=args.universe_id,
            universe_manifest_sha256=args.universe_manifest_sha256,
            worker_contract_id=args.worker_contract_id,
            worker_contract_sha256=args.worker_contract_sha256,
            worker_image=args.worker_image,
            global_seed=args.global_seed,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        else:
            print(f"Level C bootstrap complete: {payload['bootstrap_id']}")
            print(f"Protocol: {payload['protocol_manifest_id']}")
        return 0
    if args.command == "level-c-run-cutoff":
        payload = run_level_c_cutoff(
            config=load_config(),
            active_runs_root=args.active_runs_root,
            cutoff=args.cutoff,
            resume=bool(args.resume),
            gateway_url=args.gateway_url,
            gateway_token=args.gateway_token,
            atlas_active_probes=args.atlas_active_probes,
            playhand_active_runs=args.playhand_active_runs,
            nested_max_workers=args.nested_max_workers,
            trading_dashboard_root=args.trading_dashboard_root,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        else:
            print(
                f"Level C cutoff {payload['cutoff']}: {payload['status']} "
                f"({len(payload['completed_stages'])}/{len(STAGES)} stages)"
            )
        return 0
    if args.command == "level-c-audit":
        payload = audit_level_c(
            config=load_config(), active_runs_root=args.active_runs_root, cutoff=args.cutoff
        )
        print(
            json.dumps(payload, ensure_ascii=True, sort_keys=True)
            if args.json
            else format_audit(payload)
        )
        return 0
    return None
