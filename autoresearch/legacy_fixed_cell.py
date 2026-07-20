"""Immutable, fixed-cell replay plans for the archived legacy benchmark.

This module deliberately has no discovery, selection, portfolio, catalog-refresh,
or archive-write path.  It converts the archived, hash-bound benchmark inputs into
current-authority tracked-cell replay tasks whose output directories are owned by a
new comparison root.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .corpus_lab_backtests import (
    LabBacktestConfig,
    build_full_backtest_lab_task,
    resolve_lab_backtest_config,
    run_lab_full_backtests,
)
from .corpus_tools import load_profile_snapshot
from .evidence_artifacts import write_immutable_json
from .evidence_plan import (
    build_execution_cell_sha256,
    build_replay_evidence_plan,
    canonical_sha256,
    normalize_evidence_profile_snapshot,
)
from .level_c_operator import (
    build_profile_model_source_lock,
    load_authoritative_level_c_execution_plan,
)
from .nested_evidence import FrozenExecutionCellReceipt
from .runtime_policy_lock import build_runtime_policy_lock, policy_lock_provenance
from .instrument_universe import research_eligibility_report, universe_provenance
from .archive_relocation import (
    ArchiveRelocationError,
    resolve_archive_path,
    resolve_archive_runs_root,
)


LEGACY_FIXED_COMPARISON_PLAN_SCHEMA = "autoresearch-legacy-fixed-comparison-plan-v2"
LEGACY_FIXED_COMPARISON_PREFLIGHT_SCHEMA = (
    "autoresearch-legacy-fixed-comparison-preflight-v2"
)
LEGACY_FIXED_COMPARISON_CANARY_PLAN_SCHEMA = (
    "autoresearch-legacy-fixed-comparison-canary-plan-v1"
)
LEGACY_FIXED_COMPARISON_EXECUTION_REPORT_SCHEMA = (
    "autoresearch-legacy-fixed-comparison-execution-report-v1"
)
LEGACY_COMPARISON_ROLE = "outer_test"
COMPARISON_RELATIVE_ROOT = Path("legacy-fixed-cell-comparisons")
WINDOW_START = "2023-01-14T00:00:00Z"
WINDOW_END = "2026-01-14T00:00:00Z"
REQUIRED_WORKER_IMAGE = "lucasmorgan/fuzzfolio-replay-worker:vast-sha-656f43da9df0"
REQUIRED_WORKER_CONTRACT_SHA256 = (
    "sha256:0f2e7284beedf34afc9463b242f562591b5840104b85629316f3fc715ec5fec3"
)
REQUIRED_LAKE_SEMANTIC_SHA256 = (
    "sha256:d66caba7e3b7c04bd93db15a296c95f2940bd57b3c436b0497aac9858b972a90"
)
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_WINDOWS_REPARSE_POINT = 0x0400


class LegacyFixedComparisonError(RuntimeError):
    """Raised when the archived benchmark cannot be proven safe to replay."""


@dataclass(frozen=True)
class PreparedLegacyFixedComparison:
    plan: dict[str, Any]
    output_root: Path
    legacy_controls: Path
    archive_runs_root: Path
    recorded_archive_runs_root: Path
    archive_id: str | None
    authority_execution_plan: Path
    comparison_id: str
    items: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]]
    cell_receipts_by_attempt_id: dict[str, dict[str, Any]]
    evidence_plans_by_attempt_id: dict[str, dict[str, Any]]
    task_ids_by_attempt_id: dict[str, str]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _require_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LegacyFixedComparisonError(f"{label} must be a JSON object")
    return dict(value)


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacyFixedComparisonError(f"{label} is unreadable: {path}") from exc
    return _require_mapping(payload, label=label)


def _expected_sha256(value: Any, *, label: str) -> str:
    token = str(value or "").strip().lower()
    if token and not token.startswith("sha256:"):
        token = f"sha256:{token}"
    if not _SHA256_RE.fullmatch(token):
        raise LegacyFixedComparisonError(f"{label} must be a SHA-256 identity")
    return token


def _require_safe_id(value: Any, *, label: str) -> str:
    token = str(value or "").strip()
    if not _SAFE_ID_RE.fullmatch(token):
        raise LegacyFixedComparisonError(f"{label} must be a safe identifier")
    return token


def _archive_path(
    raw_path: Any,
    *,
    legacy_runs_root: Path,
    archive_runs_root: Path,
    label: str,
) -> Path:
    source = Path(str(raw_path or "")).expanduser().resolve(strict=False)
    try:
        relative = source.relative_to(legacy_runs_root)
    except ValueError as exc:
        raise LegacyFixedComparisonError(
            f"{label} is not under the recorded legacy runs root: {source}"
        ) from exc
    projected = (archive_runs_root / relative).resolve(strict=False)
    try:
        projected.relative_to(archive_runs_root)
    except ValueError as exc:
        raise LegacyFixedComparisonError(f"{label} escapes the archive root") from exc
    return projected


def _validate_hash(path: Path, expected: Any, *, label: str) -> str:
    if not path.is_file():
        raise LegacyFixedComparisonError(f"{label} is missing: {path}")
    observed = _sha256_file(path)
    if observed != _expected_sha256(expected, label=f"{label} hash"):
        raise LegacyFixedComparisonError(f"{label} hash differs from the frozen source")
    return observed


def _require_file_sha256(path: Path, *, label: str) -> str:
    if not path.is_file():
        raise LegacyFixedComparisonError(f"{label} is missing: {path}")
    return _sha256_file(path)


def _cell_value(cell: Any, *names: str) -> float | None:
    if not isinstance(cell, Mapping):
        return None
    for name in names:
        try:
            value = cell.get(name)
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            return None
    return None


def _normalize_cell(cell: Any) -> dict[str, float]:
    stop_loss = _cell_value(cell, "stop_loss_percent", "stopLossPercent", "stop_loss")
    reward = _cell_value(cell, "reward_multiple", "rewardMultiple")
    if stop_loss is None or reward is None:
        raise LegacyFixedComparisonError("frozen execution cell is missing stop loss or reward")
    return {"stop_loss_percent": stop_loss, "reward_multiple": reward}


def _cells_match(left: Any, right: Any) -> bool:
    try:
        first = _normalize_cell(left)
        second = _normalize_cell(right)
    except LegacyFixedComparisonError:
        return False
    return all(abs(first[key] - second[key]) <= 1e-9 for key in first)


def _relative_to_archive(path: Path, archive_runs_root: Path) -> str:
    try:
        return path.resolve(strict=True).relative_to(archive_runs_root).as_posix()
    except (OSError, ValueError) as exc:
        raise LegacyFixedComparisonError(
            f"source path is not a regular archived file: {path}"
        ) from exc


def _validate_controls(
    controls_path: Path, archive_runs_root: Path, *, recorded_archive_runs_root: Path
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    controls = _load_json(controls_path, label="legacy controls")
    identity = _require_mapping(controls.get("identity"), label="legacy controls identity")
    integrity = _require_mapping(controls.get("integrity"), label="legacy controls integrity")
    if controls.get("schema") != "legacy-controls-manifest-v1" or identity.get(
        "schema"
    ) != "legacy-controls-manifest-v1":
        raise LegacyFixedComparisonError("legacy controls schema differs")
    identity_hash = "sha256:" + hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if (
        integrity.get("identity_hash_algorithm") != "sha256"
        or integrity.get("self_hash") != identity_hash
        or integrity.get("manifest_id") != identity_hash
    ):
        raise LegacyFixedComparisonError("legacy controls self identity differs")
    context = _require_mapping(identity.get("archive_context"), label="archive context")
    legacy_root = Path(str(context.get("pre_cutover_runs_root") or "")).resolve(
        strict=False
    )
    projected = Path(str(context.get("projected_archived_runs_root") or "")).resolve(
        strict=False
    )
    if (
        not legacy_root
        or projected != recorded_archive_runs_root
        or context.get("reference_only") is not True
    ):
        raise LegacyFixedComparisonError("legacy controls archive context differs")
    exclusion = _require_mapping(
        identity.get("exclusion_contract"), label="legacy controls exclusion contract"
    )
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
    if any(exclusion.get(key) != value for key, value in expected_exclusion.items()):
        raise LegacyFixedComparisonError("legacy controls exclusion contract differs")
    return controls, identity, legacy_root


def _source_file(
    *,
    sources: Mapping[str, Any],
    key: str,
    archive_runs_root: Path,
    recorded_archive_runs_root: Path,
) -> Path:
    record = _require_mapping(sources.get(key), label=f"legacy source {key}")
    relative = Path(str(record.get("archive_relative_path") or ""))
    if relative.is_absolute() or not relative.parts:
        raise LegacyFixedComparisonError(f"legacy source {key} has an unsafe path")
    path = (archive_runs_root / relative).resolve(strict=False)
    try:
        path.relative_to(archive_runs_root)
    except ValueError as exc:
        raise LegacyFixedComparisonError(f"legacy source {key} escapes archive root") from exc
    projected = Path(str(record.get("projected_archived_path") or "")).resolve(
        strict=False
    )
    if projected != (recorded_archive_runs_root / relative).resolve(strict=False):
        raise LegacyFixedComparisonError(f"legacy source {key} projected path differs")
    _validate_hash(path, record.get("sha256"), label=f"legacy source {key}")
    return path


def _load_catalog_rows(catalog_path: Path, attempt_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not attempt_ids:
        return {}
    # The controls bind the database file itself, not SQLite's mutable sidecars.
    # immutable=1 makes those sidecars deliberately invisible, so they cannot alter
    # the selected rows without a corresponding change to the hash-bound database.
    uri = f"file:{catalog_path.as_posix()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        raise LegacyFixedComparisonError("archived attempt catalog is not readable") from exc
    rows: dict[str, dict[str, Any]] = {}
    try:
        placeholders = ",".join("?" for _ in attempt_ids)
        query = (
            "SELECT attempt_id, row_json FROM attempt_rows "
            f"WHERE attempt_id IN ({placeholders})"
        )
        for attempt_id, row_json in connection.execute(query, sorted(attempt_ids)):
            if attempt_id in rows:
                raise LegacyFixedComparisonError(
                    f"archived catalog has duplicate attempt rows: {attempt_id}"
                )
            try:
                row = json.loads(row_json)
            except (TypeError, json.JSONDecodeError) as exc:
                raise LegacyFixedComparisonError(
                    f"archived catalog row is unreadable: {attempt_id}"
                ) from exc
            if not isinstance(row, dict) or str(row.get("attempt_id") or "") != attempt_id:
                raise LegacyFixedComparisonError(
                    f"archived catalog row identity differs: {attempt_id}"
                )
            rows[attempt_id] = row
    finally:
        connection.close()
    return rows


def _candidate_source_path(
    candidate: Mapping[str, Any],
    key: str,
    *,
    legacy_runs_root: Path,
    archive_runs_root: Path,
    attempt_id: str,
) -> tuple[Path, str]:
    source = _require_mapping(candidate.get("source"), label=f"cohort source {attempt_id}")
    paths = _require_mapping(source.get("paths"), label=f"cohort source paths {attempt_id}")
    hashes = _require_mapping(source.get("sha256"), label=f"cohort source hashes {attempt_id}")
    path = _archive_path(
        paths.get(key),
        legacy_runs_root=legacy_runs_root,
        archive_runs_root=archive_runs_root,
        label=f"cohort source {key} for {attempt_id}",
    )
    return path, _validate_hash(path, hashes.get(key), label=f"cohort source {key}")


def _receipt_from_source(
    *,
    comparison_plan_id: str,
    attempt_id: str,
    selection_basis: str,
    profile_path: Path,
    profile_sha256: str,
    result_path: Path,
    result_sha256: str,
    detail_path: Path,
    detail_sha256: str,
    archive_runs_root: Path,
    lake_manifest_sha256: str,
    source_kind: str,
    source_extra: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    profile_snapshot = load_profile_snapshot(profile_path)
    if not isinstance(profile_snapshot, dict):
        raise LegacyFixedComparisonError(f"source profile is unreadable: {attempt_id}")
    try:
        normalized_profile = normalize_evidence_profile_snapshot(profile_snapshot)
    except ValueError as exc:
        raise LegacyFixedComparisonError(f"source profile is invalid: {attempt_id}") from exc
    result = _load_json(result_path, label=f"source 36mo result {attempt_id}")
    detail = _load_json(detail_path, label=f"source cell detail {attempt_id}")
    aggregate = result.get("data")
    aggregate = aggregate.get("aggregate") if isinstance(aggregate, dict) else None
    aggregate = aggregate if isinstance(aggregate, dict) else {}
    summary = aggregate.get("matrix_summary")
    summary = summary if isinstance(summary, dict) else {}
    selected_cell = _normalize_cell(detail.get("cell"))
    if selection_basis == "recommended_cell":
        reference_cell = summary.get("robust_cell") or aggregate.get("recommended_cell")
    elif selection_basis == "best_cell":
        reference_cell = aggregate.get("best_cell") or summary.get("best_cell")
    else:
        raise LegacyFixedComparisonError("unsupported fixed-cell selection basis")
    if not _cells_match(selected_cell, reference_cell):
        raise LegacyFixedComparisonError(
            f"source cell is ambiguous or differs from its frozen result: {attempt_id}"
        )
    replay_job_path = result_path.parent / "deep-replay-job.json"
    replay_job_sha256 = _require_file_sha256(
        replay_job_path, label=f"source deep replay job {attempt_id}"
    )
    replay_job = _load_json(replay_job_path, label=f"source deep replay job {attempt_id}")
    replay_request = replay_job.get("request")
    if not isinstance(replay_request, dict):
        raise LegacyFixedComparisonError(
            f"source deep replay request is missing: {attempt_id}"
        )
    source_record: dict[str, Any] = {
        "kind": source_kind,
        "attempt_id": attempt_id,
        "profile_path": _relative_to_archive(profile_path, archive_runs_root),
        "profile_sha256": profile_sha256,
        "result_path": _relative_to_archive(result_path, archive_runs_root),
        "result_sha256": result_sha256,
        "detail_path": _relative_to_archive(detail_path, archive_runs_root),
        "detail_sha256": detail_sha256,
        "deep_replay_job_path": _relative_to_archive(
            replay_job_path, archive_runs_root
        ),
        "deep_replay_job_sha256": replay_job_sha256,
        "deep_replay_request_sha256": canonical_sha256(replay_request),
        "selection_basis": selection_basis,
        "execution_cell_sha256": build_execution_cell_sha256(selected_cell),
    }
    source_record.update(dict(source_extra or {}))
    receipt = FrozenExecutionCellReceipt(
        campaign_plan_id=comparison_plan_id,
        fold_id=f"legacy-fixed-comparison:{attempt_id}",
        profile_snapshot_sha256=canonical_sha256(normalized_profile),
        train_evidence_plan_id=canonical_sha256(source_record),
        selection_basis=selection_basis,
        execution_cell=selected_cell,
        execution_cell_sha256=source_record["execution_cell_sha256"],
        source=source_record,
        lake_manifest_sha256=lake_manifest_sha256,
    )
    return normalized_profile, receipt.model_dump(mode="json")


def _current_universe_terminal(
    *,
    attempt_id: str,
    profile_snapshot: Mapping[str, Any],
    receipt: Mapping[str, Any],
    memberships_for_attempt: list[str],
    membership_source: str,
) -> dict[str, Any] | None:
    """Return a typed terminal only when current authority excludes the profile."""

    source = _require_mapping(receipt.get("source"), label=f"source receipt {attempt_id}")
    report = research_eligibility_report(profile_snapshot.get("instruments"))
    if report["is_eligible"]:
        return None
    return {
        "attempt_id": attempt_id,
        "status": "unresolved_current_universe",
        "reason_code": "current_authority_ineligible_instrument",
        "memberships": memberships_for_attempt,
        "membership_source": membership_source,
        "instruments": report["instruments"],
        "ineligible_instruments": report["ineligible"],
        "unknown_instruments": report["unknown"],
        "lifecycle": report["lifecycle"],
        "current_universe": universe_provenance(),
        "legacy_source": source,
    }


def _load_authority(
    *, config: Any, authority_execution_plan: Path, trading_dashboard_root: Path
) -> dict[str, Any]:
    plan = load_authoritative_level_c_execution_plan(authority_execution_plan, config=config)
    bound = _require_mapping(plan.get("bound_contract"), label="authority bound contract")
    atlas_arguments = _require_mapping(
        plan.get("atlas_arguments"), label="authority Atlas arguments"
    )
    playhand_arguments = _require_mapping(
        plan.get("playhand_arguments"), label="authority PlayHand arguments"
    )
    if bound.get("worker_image") != REQUIRED_WORKER_IMAGE:
        raise LegacyFixedComparisonError("current authority worker image differs")
    if bound.get("worker_contract_sha256") != REQUIRED_WORKER_CONTRACT_SHA256:
        raise LegacyFixedComparisonError("current authority worker contract differs")
    if (
        atlas_arguments.get("lake_manifest_sha256") != REQUIRED_LAKE_SEMANTIC_SHA256
        or playhand_arguments.get("lake_manifest_sha256")
        != REQUIRED_LAKE_SEMANTIC_SHA256
    ):
        raise LegacyFixedComparisonError("current authority lake identity differs")
    runtime_lock = build_runtime_policy_lock(
        config,
        worker_contract_sha256=REQUIRED_WORKER_CONTRACT_SHA256,
        trading_dashboard_root=trading_dashboard_root,
    )
    profile_lock = build_profile_model_source_lock(trading_dashboard_root)
    policy_provenance = policy_lock_provenance(runtime_lock)
    return {
        "execution_plan_sha256": _sha256_file(authority_execution_plan),
        "worker_image": REQUIRED_WORKER_IMAGE,
        "worker_contract_sha256": REQUIRED_WORKER_CONTRACT_SHA256,
        "lake_semantic_sha256": REQUIRED_LAKE_SEMANTIC_SHA256,
        "runtime_policy_lock": runtime_lock,
        "profile_model_source_lock": profile_lock,
        **policy_provenance,
    }


def _verify_live_lake_identity(*, lake_url: str | None, lake_token: str | None) -> str:
    resolved_url = str(
        lake_url or os.environ.get("REMOTE_MARKET_DATA_LAKE_BASE_URL") or ""
    ).strip()
    if not resolved_url:
        raise LegacyFixedComparisonError(
            "live lake URL is required before fixed-cell comparison enqueue"
        )
    import requests

    headers = (
        {"Authorization": f"Bearer {lake_token}"}
        if str(lake_token or "").strip()
        else {}
    )
    try:
        response = requests.get(
            f"{resolved_url.rstrip('/')}/api/lake/manifest",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        observed = str((response.json() or {}).get("coverage_sha256") or "").strip()
    except requests.RequestException as exc:
        raise LegacyFixedComparisonError(
            "could not verify live lake semantic identity before enqueue"
        ) from exc
    if observed != REQUIRED_LAKE_SEMANTIC_SHA256:
        raise LegacyFixedComparisonError(
            "live lake semantic identity differs from the fixed comparison plan"
        )
    return observed


def _selected_memberships(
    *,
    identity: Mapping[str, Any],
    sources: Mapping[str, Any],
    archive_runs_root: Path,
    recorded_archive_runs_root: Path,
) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    categories = _require_mapping(identity.get("categories"), label="legacy categories")
    counts = _require_mapping(identity.get("counts"), label="legacy membership counts")
    memberships: dict[str, set[str]] = {}
    source_rows: dict[str, dict[str, str]] = {}
    for name, source_key in (
        ("june", "june_selected_membership"),
        ("july", "july_selected_membership"),
    ):
        category = _require_mapping(categories.get(name), label=f"{name} membership")
        attempt_ids = category.get("attempt_ids")
        if not isinstance(attempt_ids, list):
            raise LegacyFixedComparisonError(f"{name} membership attempt IDs are missing")
        normalized = [str(value or "").strip() for value in attempt_ids]
        if not all(normalized) or len(set(normalized)) != len(normalized):
            raise LegacyFixedComparisonError(f"{name} membership has duplicate or empty IDs")
        expected_count = int(counts.get(f"{name}_membership") or 0)
        if expected_count != len(normalized):
            raise LegacyFixedComparisonError(f"{name} membership count differs from controls")
        path = _source_file(
            sources=sources,
            key=source_key,
            archive_runs_root=archive_runs_root,
            recorded_archive_runs_root=recorded_archive_runs_root,
        )
        try:
            rows = list(csv.DictReader(path.open("r", encoding="utf-8", newline="")))
        except OSError as exc:
            raise LegacyFixedComparisonError(f"{name} membership CSV is unreadable") from exc
        by_id = {str(row.get("attempt_id") or "").strip(): row for row in rows}
        if len(by_id) != len(rows) or set(by_id) != set(normalized):
            raise LegacyFixedComparisonError(f"{name} membership CSV differs from controls")
        memberships[name] = set(normalized)
        source_rows.update(
            {
                attempt_id: {
                    "run_id": str(row.get("run_id") or "").strip(),
                    "profile_path": str(row.get("profile_path") or "").strip(),
                    "candidate_name": str(row.get("candidate_name") or "").strip(),
                    "membership_source": source_key,
                }
                for attempt_id, row in by_id.items()
            }
        )
    overlap = _require_mapping(categories.get("overlap"), label="legacy overlap")
    overlap_ids = {str(value or "").strip() for value in list(overlap.get("attempt_ids") or [])}
    if not overlap_ids or overlap_ids != memberships["june"] & memberships["july"]:
        raise LegacyFixedComparisonError("legacy June/July overlap differs from controls")
    if int(counts.get("unique_strategy_attempt_ids_across_june_july") or 0) != len(
        memberships["june"] | memberships["july"]
    ):
        raise LegacyFixedComparisonError("legacy membership union count differs")
    return memberships, source_rows


def _safe_archive_run_dir(
    *, archive_runs_root: Path, run_id: Any, label: str
) -> Path:
    safe_run_id = _require_safe_id(run_id, label=label)
    run_dir = (archive_runs_root / safe_run_id).resolve(strict=True)
    try:
        run_dir.relative_to(archive_runs_root)
    except ValueError as exc:
        raise LegacyFixedComparisonError(f"{label} escapes the archive root") from exc
    if not run_dir.is_dir():
        raise LegacyFixedComparisonError(f"{label} is not an archived run directory")
    return run_dir


def _assert_output_isolated(*, output_root: Path, archive_runs_root: Path) -> None:
    resolved_output = output_root.resolve(strict=False)
    resolved_archive = archive_runs_root.resolve(strict=True)
    for child, parent in ((resolved_output, resolved_archive), (resolved_archive, resolved_output)):
        try:
            child.relative_to(parent)
        except ValueError:
            continue
        raise LegacyFixedComparisonError(
            "comparison-owned output root must be disjoint from the archived runs root"
        )


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        stat_result = path.lstat()
    except FileNotFoundError:
        return False
    return path.is_symlink() or bool(
        int(getattr(stat_result, "st_file_attributes", 0)) & _WINDOWS_REPARSE_POINT
    )


def _assert_owned_output_path(
    *, output_path: Path, output_root: Path, archive_runs_root: Path
) -> None:
    """Reject existing symlink/junction traversal before any comparison write."""

    root = output_root.resolve(strict=False)
    archive = archive_runs_root.resolve(strict=True)
    logical_path = output_path.absolute()
    logical_root = output_root.absolute()
    try:
        relative = logical_path.relative_to(logical_root)
    except ValueError as exc:
        raise LegacyFixedComparisonError("comparison output path escapes its owned root") from exc
    current = logical_root
    for part in (".", *relative.parts):
        if part != ".":
            current = current / part
        if not current.exists() and not current.is_symlink():
            continue
        if _is_reparse_or_symlink(current):
            raise LegacyFixedComparisonError(
                f"comparison output path contains a symlink or junction: {current}"
            )
        resolved = current.resolve(strict=False)
        for candidate, forbidden in ((resolved, archive),):
            try:
                candidate.relative_to(forbidden)
            except ValueError:
                pass
            else:
                raise LegacyFixedComparisonError(
                    "comparison output path resolves inside the archived runs root"
                )
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise LegacyFixedComparisonError(
                "comparison output path resolves outside its owned root"
            ) from exc


def _comparison_task_id(task_specification: Mapping[str, Any]) -> str:
    digest = canonical_sha256(task_specification).removeprefix("sha256:")
    return f"legacy-fixed-comparison-{digest}"


def prepare_legacy_fixed_comparison(
    *,
    config: Any,
    legacy_controls: Path,
    archive_runs_root: Path,
    authority_execution_plan: Path,
    trading_dashboard_root: Path,
    comparison_id: str,
    archive_id: str | None = None,
) -> PreparedLegacyFixedComparison:
    """Validate every frozen source and build a deterministic, non-enqueuing plan."""

    comparison_id = _require_safe_id(comparison_id, label="comparison ID")
    recorded_archive_runs_root = archive_runs_root.resolve(strict=False)
    if archive_id is None:
        initial_controls = _load_json(
            legacy_controls.resolve(strict=True), label="legacy controls"
        )
        initial_identity = _require_mapping(
            initial_controls.get("identity"), label="legacy controls identity"
        )
        initial_context = _require_mapping(
            initial_identity.get("archive_context"), label="archive context"
        )
        recorded_archive_id = str(initial_context.get("archive_id") or "").strip()
        archive_id = (
            _require_safe_id(recorded_archive_id, label="archive ID")
            if recorded_archive_id
            else None
        )
    else:
        archive_id = _require_safe_id(archive_id, label="archive ID")
    if archive_id is None:
        archive_runs_root = archive_runs_root.resolve(strict=True)
        controls_path = legacy_controls.resolve(strict=True)
    else:
        try:
            archive_runs_root = resolve_archive_runs_root(
                recorded_archive_runs_root, archive_id=archive_id
            )
            controls_path = resolve_archive_path(legacy_controls, archive_id=archive_id).resolve(
                strict=True
            )
        except ArchiveRelocationError as exc:
            raise LegacyFixedComparisonError("archive relocation is invalid") from exc
    authority_path = authority_execution_plan.resolve(strict=True)
    trading_dashboard_root = trading_dashboard_root.resolve(strict=True)
    controls, identity, legacy_runs_root = _validate_controls(
        controls_path,
        archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
    )
    sources = _require_mapping(identity.get("source_artifacts"), label="legacy sources")
    catalog_path = _source_file(
        sources=sources,
        key="exact_catalog_database",
        archive_runs_root=archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
    )
    cohort_path = _source_file(
        sources=sources,
        key="fixed_cohort_manifest",
        archive_runs_root=archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
    )
    nested_report_path = _source_file(
        sources=sources,
        key="nested_evidence_report",
        archive_runs_root=archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
    )
    nested_report = _load_json(nested_report_path, label="archived nested report")
    if nested_report.get("status") != "complete":
        raise LegacyFixedComparisonError("archived nested report is not complete")
    cohort = _load_json(cohort_path, label="archived fixed cohort")
    cohort_ids_raw = cohort.get("attempt_ids")
    if not isinstance(cohort_ids_raw, list):
        raise LegacyFixedComparisonError("archived fixed cohort has no attempt IDs")
    cohort_ids = [str(value or "").strip() for value in cohort_ids_raw]
    if not all(cohort_ids) or len(set(cohort_ids)) != len(cohort_ids):
        raise LegacyFixedComparisonError("archived fixed cohort IDs are invalid")
    campaign_controls = _require_mapping(
        _require_mapping(identity.get("categories"), label="legacy categories").get(
            "campaign_controls"
        ),
        label="legacy campaign controls",
    )
    fixed_cohort_control = _require_mapping(
        campaign_controls.get("fixed_cohort"), label="legacy fixed cohort control"
    )
    expected_cohort_manifest_id = str(fixed_cohort_control.get("manifest_id") or "")
    if (
        cohort.get("manifest_id") != expected_cohort_manifest_id
        or nested_report.get("attempt_cohort_manifest_id") != expected_cohort_manifest_id
        or len(cohort_ids) != int(fixed_cohort_control.get("attempt_count") or 0)
        or len(cohort_ids) != 443
    ):
        raise LegacyFixedComparisonError("archived fixed cohort identity differs")
    memberships, selected_rows = _selected_memberships(
        identity=identity,
        sources=sources,
        archive_runs_root=archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
    )
    membership_union = memberships["june"] | memberships["july"]
    out_of_cohort_members = membership_union - set(cohort_ids)
    if len(out_of_cohort_members) != 12:
        raise LegacyFixedComparisonError("legacy comparison must have 12 out-of-cohort members")
    authority = _load_authority(
        config=config,
        authority_execution_plan=authority_path,
        trading_dashboard_root=trading_dashboard_root,
    )

    cohort_source = _require_mapping(cohort.get("source"), label="cohort source")
    snapshot_path = _archive_path(
        cohort_source.get("candidate_snapshot_path"),
        legacy_runs_root=legacy_runs_root,
        archive_runs_root=archive_runs_root,
        label="cohort candidate snapshot",
    )
    snapshot_sha256 = _validate_hash(
        snapshot_path,
        cohort_source.get("candidate_snapshot_sha256"),
        label="cohort candidate snapshot",
    )
    snapshot = _load_json(snapshot_path, label="cohort candidate snapshot")
    candidates = snapshot.get("candidates")
    if not isinstance(candidates, list):
        raise LegacyFixedComparisonError("cohort candidate snapshot has no candidates")
    candidates_by_attempt: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        attempt_id = str(candidate.get("attempt_id") or "").strip()
        if not attempt_id:
            continue
        if attempt_id in candidates_by_attempt:
            raise LegacyFixedComparisonError(
                f"cohort candidate snapshot is ambiguous: {attempt_id}"
            )
        candidates_by_attempt[attempt_id] = candidate
    if not set(cohort_ids).issubset(candidates_by_attempt):
        raise LegacyFixedComparisonError("cohort snapshot is missing a frozen attempt")

    catalog_rows = _load_catalog_rows(catalog_path, set(cohort_ids) | membership_union)
    if not set(cohort_ids).issubset(catalog_rows):
        raise LegacyFixedComparisonError("archived catalog is missing a cohort attempt")
    catalog_sha256 = _sha256_file(catalog_path)
    comparison_descriptor = {
        "schema": LEGACY_FIXED_COMPARISON_PLAN_SCHEMA,
        "comparison_id": comparison_id,
        "legacy_controls_sha256": _sha256_file(controls_path),
        "cohort_manifest_id": expected_cohort_manifest_id,
        "cohort_manifest_sha256": _sha256_file(cohort_path),
        "candidate_snapshot_sha256": snapshot_sha256,
        "nested_report_sha256": _sha256_file(nested_report_path),
        "analysis_window_start": WINDOW_START,
        "analysis_window_end": WINDOW_END,
        "requested_horizon_months": 36,
        "evidence_role": LEGACY_COMPARISON_ROLE,
        "authority": authority,
    }
    comparison_execution_plan_id = canonical_sha256(comparison_descriptor)
    output_root = (
        Path(config.derived_root).resolve(strict=False)
        / COMPARISON_RELATIVE_ROOT
        / comparison_id
    )
    _assert_output_isolated(
        output_root=output_root, archive_runs_root=archive_runs_root
    )

    tasks: list[dict[str, Any]] = []
    items: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    cell_receipts: dict[str, dict[str, Any]] = {}
    evidence_plans: dict[str, dict[str, Any]] = {}
    task_ids: dict[str, str] = {}
    task_attempt_ids: set[str] = set()

    def append_task(
        *,
        attempt_id: str,
        run_id: str,
        source_kind: str,
        profile_snapshot: dict[str, Any],
        receipt: dict[str, Any],
        source_record: Mapping[str, Any],
        memberships_for_attempt: list[str],
    ) -> None:
        if attempt_id in task_attempt_ids:
            raise LegacyFixedComparisonError(
                f"comparison has duplicate resolved attempt: {attempt_id}"
            )
        catalog_attempt = dict(catalog_rows[attempt_id])
        archive_run_dir = _safe_archive_run_dir(
            archive_runs_root=archive_runs_root,
            run_id=run_id,
            label=f"archived run for {attempt_id}",
        )
        clone_artifact_dir = output_root / "attempts" / canonical_sha256(
            {"attempt_id": attempt_id}
        ).removeprefix("sha256:")
        catalog_attempt["artifact_dir"] = str(clone_artifact_dir)
        source_replay_job = archive_runs_root / str(
            source_record.get("deep_replay_job_path") or ""
        )
        if source_replay_job.name != "deep-replay-job.json":
            raise LegacyFixedComparisonError(
                f"source replay request path is invalid: {attempt_id}"
            )
        _validate_hash(
            source_replay_job,
            source_record.get("deep_replay_job_sha256"),
            label=f"source deep replay job {attempt_id}",
        )
        source_replay_job_payload = _load_json(
            source_replay_job, label=f"source deep replay job {attempt_id}"
        )
        source_replay_request = source_replay_job_payload.get("request")
        if (
            not isinstance(source_replay_request, dict)
            or canonical_sha256(source_replay_request)
            != str(source_record.get("deep_replay_request_sha256") or "")
        ):
            raise LegacyFixedComparisonError(
                f"source deep replay request differs from attested receipt: {attempt_id}"
            )
        # Outputs remain comparison-owned.  The archived request is used only as a
        # read-only task-construction input, just as formal nested evidence does.
        catalog_attempt["_nested_source_artifact_dir"] = str(source_replay_job.parent)
        catalog_attempt["profile_path"] = str(
            archive_runs_root / str(source_record["profile_path"])
        )
        catalog_attempt["attempt_id"] = attempt_id
        row = {
            "attempt_id": attempt_id,
            "run_id": run_id,
            "candidate_name": str(catalog_attempt.get("candidate_name") or attempt_id),
            "comparison_source_kind": source_kind,
        }
        run_metadata_path = archive_run_dir / "run-metadata.json"
        run_metadata = (
            _load_json(run_metadata_path, label=f"archived run metadata {run_id}")
            if run_metadata_path.is_file()
            else {}
        )
        evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=comparison_execution_plan_id,
            evidence_role=LEGACY_COMPARISON_ROLE,
            selection_data_end=WINDOW_END,
            analysis_window_start=WINDOW_START,
            analysis_window_end=WINDOW_END,
            requested_horizon_months=36,
            profile_snapshot=profile_snapshot,
            execution_cell_sha256=str(receipt["execution_cell_sha256"]),
            lake_manifest_sha256=REQUIRED_LAKE_SEMANTIC_SHA256,
        )
        task_specification = {
            "attempt_id": attempt_id,
            "run_id": run_id,
            "source_kind": source_kind,
            "memberships": memberships_for_attempt,
            "output_artifact_dir": clone_artifact_dir.relative_to(output_root).as_posix(),
            "evidence_plan": evidence_plan.model_dump(mode="json"),
            "frozen_execution_cell": receipt,
            "legacy_source": dict(source_record),
        }
        task_id = _comparison_task_id(task_specification)
        tasks.append(
            {
                **task_specification,
                "task_id": task_id,
            }
        )
        items.append((archive_run_dir, catalog_attempt, row, run_metadata))
        cell_receipts[attempt_id] = receipt
        evidence_plans[attempt_id] = task_specification["evidence_plan"]
        task_ids[attempt_id] = task_id
        task_attempt_ids.add(attempt_id)

    for attempt_id in cohort_ids:
        candidate = candidates_by_attempt[attempt_id]
        profile_path, profile_sha256 = _candidate_source_path(
            candidate,
            "profile_path",
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            attempt_id=attempt_id,
        )
        result_path, result_sha256 = _candidate_source_path(
            candidate,
            "full_backtest_result_path_36m",
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            attempt_id=attempt_id,
        )
        curve_path, _curve_sha256 = _candidate_source_path(
            candidate,
            "full_backtest_curve_path_36m",
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            attempt_id=attempt_id,
        )
        detail_path = curve_path.with_name("full-backtest-36mo-recommended-cell-path-detail.json")
        detail_sha256 = _sha256_file(detail_path) if detail_path.is_file() else ""
        if not detail_sha256:
            raise LegacyFixedComparisonError(
                f"cohort recommended cell detail is missing: {attempt_id}"
            )
        profile_snapshot, receipt = _receipt_from_source(
            comparison_plan_id=comparison_execution_plan_id,
            attempt_id=attempt_id,
            selection_basis="recommended_cell",
            profile_path=profile_path,
            profile_sha256=profile_sha256,
            result_path=result_path,
            result_sha256=result_sha256,
            detail_path=detail_path,
            detail_sha256=detail_sha256,
            archive_runs_root=archive_runs_root,
            lake_manifest_sha256=REQUIRED_LAKE_SEMANTIC_SHA256,
            source_kind="cohort_legacy_36m_recommended",
            source_extra={
                "cohort_manifest_id": expected_cohort_manifest_id,
                "candidate_snapshot_sha256": snapshot_sha256,
            },
        )
        cohort_eligibility = research_eligibility_report(
            profile_snapshot.get("instruments")
        )
        if not cohort_eligibility["is_eligible"]:
            raise LegacyFixedComparisonError(
                f"frozen cohort source is outside the current authority universe: {attempt_id}"
            )
        append_task(
            attempt_id=attempt_id,
            run_id=str(catalog_rows[attempt_id].get("run_id") or ""),
            source_kind="cohort_legacy_36m_recommended",
            profile_snapshot=profile_snapshot,
            receipt=receipt,
            source_record=receipt["source"],
            memberships_for_attempt=sorted(
                name for name, values in memberships.items() if attempt_id in values
            ),
        )

    unresolved: list[dict[str, Any]] = []
    for attempt_id in sorted(membership_union - set(cohort_ids)):
        selected = selected_rows.get(attempt_id)
        if not selected:
            raise LegacyFixedComparisonError(
                f"membership source row is missing: {attempt_id}"
            )
        catalog_attempt = catalog_rows.get(attempt_id)
        if catalog_attempt is None:
            unresolved.append(
                {
                    "attempt_id": attempt_id,
                    "status": "unresolved_source",
                    "reason_code": "missing_catalog_attested_profile_and_36mo_cell_source",
                    "memberships": sorted(
                        name
                        for name, values in memberships.items()
                        if attempt_id in values
                    ),
                    "membership_source": selected["membership_source"],
                }
            )
            continue
        profile_path = _archive_path(
            selected["profile_path"],
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            label=f"selected membership profile {attempt_id}",
        )
        catalog_run_id = _require_safe_id(
            catalog_attempt.get("run_id"), label=f"catalog run {attempt_id}"
        )
        catalog_profile_path = _archive_path(
            catalog_attempt.get("profile_path"),
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            label=f"catalog profile {attempt_id}",
        )
        if (
            catalog_run_id
            != _require_safe_id(selected["run_id"], label=f"selected membership run {attempt_id}")
            or catalog_profile_path != profile_path
        ):
            raise LegacyFixedComparisonError(
                f"catalog-attested source differs from selected membership: {attempt_id}"
            )
        result_path = _archive_path(
            catalog_attempt.get("full_backtest_result_path_36m"),
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            label=f"catalog 36mo result {attempt_id}",
        )
        detail_path = _archive_path(
            catalog_attempt.get("full_backtest_recommended_curve_path_36m"),
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            label=f"catalog 36mo cell detail {attempt_id}",
        )
        manifest_path = result_path.with_name("full-backtest-36mo-manifest.json")
        if (
            result_path.name != "full-backtest-36mo-result.json"
            or detail_path.name != "full-backtest-36mo-recommended-cell-path-detail.json"
            or result_path.parent != detail_path.parent
        ):
            raise LegacyFixedComparisonError(
                f"catalog-attested 36mo source is structurally invalid: {attempt_id}"
            )
        profile_sha256 = _require_file_sha256(profile_path, label=f"catalog profile {attempt_id}")
        result_sha256 = _require_file_sha256(result_path, label=f"catalog result {attempt_id}")
        detail_sha256 = _require_file_sha256(detail_path, label=f"catalog cell detail {attempt_id}")
        manifest_sha256 = _require_file_sha256(manifest_path, label=f"catalog manifest {attempt_id}")
        manifest = _load_json(manifest_path, label=f"catalog 36mo manifest {attempt_id}")
        manifest_profile = _archive_path(
            manifest.get("source_profile_path"),
            legacy_runs_root=legacy_runs_root,
            archive_runs_root=archive_runs_root,
            label=f"catalog manifest profile {attempt_id}",
        )
        if (
            manifest.get("schema") != "autoresearch-full-backtest-provenance-v1"
            or str(manifest.get("attempt_id") or "") != attempt_id
            or manifest_profile != profile_path
        ):
            raise LegacyFixedComparisonError(
                f"catalog-attested 36mo manifest differs: {attempt_id}"
            )
        profile_snapshot, receipt = _receipt_from_source(
            comparison_plan_id=comparison_execution_plan_id,
            attempt_id=attempt_id,
            selection_basis="recommended_cell",
            profile_path=profile_path,
            profile_sha256=profile_sha256,
            result_path=result_path,
            result_sha256=result_sha256,
            detail_path=detail_path,
            detail_sha256=detail_sha256,
            archive_runs_root=archive_runs_root,
            lake_manifest_sha256=REQUIRED_LAKE_SEMANTIC_SHA256,
            source_kind="promoted_membership_36m_recommended",
            source_extra={
                "membership_source": selected["membership_source"],
                "manifest_path": _relative_to_archive(
                    manifest_path, archive_runs_root
                ),
                "manifest_sha256": manifest_sha256,
                "catalog_source_sha256": catalog_sha256,
                "catalog_row_sha256": canonical_sha256(catalog_attempt),
            },
        )
        memberships_for_attempt = sorted(
            name for name, values in memberships.items() if attempt_id in values
        )
        universe_terminal = _current_universe_terminal(
            attempt_id=attempt_id,
            profile_snapshot=profile_snapshot,
            receipt=receipt,
            memberships_for_attempt=memberships_for_attempt,
            membership_source=selected["membership_source"],
        )
        if universe_terminal is not None:
            unresolved.append(universe_terminal)
            continue
        append_task(
            attempt_id=attempt_id,
            run_id=catalog_run_id,
            source_kind="promoted_membership_36m_recommended",
            profile_snapshot=profile_snapshot,
            receipt=receipt,
            source_record=receipt["source"],
            memberships_for_attempt=memberships_for_attempt,
        )

    expected_unresolved = out_of_cohort_members - task_attempt_ids
    if {row["attempt_id"] for row in unresolved} != expected_unresolved:
        raise LegacyFixedComparisonError("legacy comparison unresolved accounting differs")
    if (
        len(tasks) != 450
        or len(cohort_ids) != 443
        or len(task_attempt_ids - set(cohort_ids)) != 7
        or len(unresolved) != 5
    ):
        raise LegacyFixedComparisonError(
            "legacy comparison must resolve 450 tasks and exactly 5 terminal sources"
        )
    identity_payload = {
        **comparison_descriptor,
        "execution_plan_id": comparison_execution_plan_id,
        "tasks": tasks,
        "terminal_outcomes": unresolved,
    }
    plan = {"plan_id": canonical_sha256(identity_payload), **identity_payload}
    preflight = {
        "schema": LEGACY_FIXED_COMPARISON_PREFLIGHT_SCHEMA,
        "plan_id": plan["plan_id"],
        "task_count": len(tasks),
        "cohort_task_count": len(cohort_ids),
        "out_of_cohort_task_count": len(tasks) - len(cohort_ids),
        "unresolved_source_count": sum(
            1 for row in unresolved if row.get("status") == "unresolved_source"
        ),
        "unresolved_current_universe_count": sum(
            1
            for row in unresolved
            if row.get("status") == "unresolved_current_universe"
        ),
        "unresolved_terminal_count": len(unresolved),
        "june_resolved_count": sum(
            1 for attempt_id in memberships["june"] if attempt_id in task_attempt_ids
        ),
        "july_resolved_count": sum(
            1 for attempt_id in memberships["july"] if attempt_id in task_attempt_ids
        ),
        "overlap_resolved_count": sum(
            1
            for attempt_id in memberships["june"] & memberships["july"]
            if attempt_id in task_attempt_ids
        ),
        "archive_read_only": True,
        "enqueued": False,
    }
    plan["preflight"] = preflight
    return PreparedLegacyFixedComparison(
        plan=plan,
        output_root=output_root,
        legacy_controls=controls_path,
        archive_runs_root=archive_runs_root,
        recorded_archive_runs_root=recorded_archive_runs_root,
        archive_id=archive_id,
        authority_execution_plan=authority_path,
        comparison_id=comparison_id,
        items=items,
        cell_receipts_by_attempt_id=cell_receipts,
        evidence_plans_by_attempt_id=evidence_plans,
        task_ids_by_attempt_id=task_ids,
    )


def write_legacy_fixed_comparison_plan(
    prepared: PreparedLegacyFixedComparison,
) -> dict[str, Any]:
    """Create only the plan and preflight artifacts owned by the comparison root."""

    plan_path = prepared.output_root / "comparison-plan.json"
    preflight_path = prepared.output_root / "preflight-report.json"
    _assert_owned_output_path(
        output_path=plan_path,
        output_root=prepared.output_root,
        archive_runs_root=prepared.archive_runs_root,
    )
    _assert_owned_output_path(
        output_path=preflight_path,
        output_root=prepared.output_root,
        archive_runs_root=prepared.archive_runs_root,
    )
    write_immutable_json(plan_path, prepared.plan)
    write_immutable_json(preflight_path, prepared.plan["preflight"])
    return {
        **prepared.plan["preflight"],
        "plan_path": str(plan_path),
        "preflight_path": str(preflight_path),
    }


def prepare_legacy_fixed_comparison_canary(
    *,
    parent: PreparedLegacyFixedComparison,
    task_count: int,
    attempt_ids: list[str] | None = None,
) -> PreparedLegacyFixedComparison:
    """Derive a sibling-root canary with distinct delivery IDs from a full plan.

    The evidence plans and frozen cells remain exactly those of the immutable
    parent. Only gateway delivery IDs and output destinations change so a
    canary cannot collide with the later complete comparison.
    """

    if task_count < 1:
        raise LegacyFixedComparisonError("comparison canary must select at least one task")
    parent_tasks = parent.plan.get("tasks")
    if not isinstance(parent_tasks, list) or len(parent_tasks) < task_count:
        raise LegacyFixedComparisonError("comparison canary task count exceeds parent plan")
    parent_plan_id = str(parent.plan.get("plan_id") or "")
    parent_execution_plan_id = str(parent.plan.get("execution_plan_id") or "")
    if not _SHA256_RE.fullmatch(parent_plan_id) or not _SHA256_RE.fullmatch(
        parent_execution_plan_id
    ):
        raise LegacyFixedComparisonError("comparison canary parent identity is invalid")

    canary_id = _require_safe_id(
        f"{parent.comparison_id}-canary-{task_count}", label="comparison canary ID"
    )
    output_root = parent.output_root.with_name(canary_id)
    _assert_output_isolated(
        output_root=output_root, archive_runs_root=parent.archive_runs_root
    )
    requested_attempt_ids = [str(value or "").strip() for value in (attempt_ids or [])]
    if requested_attempt_ids:
        if (
            len(requested_attempt_ids) != task_count
            or not all(requested_attempt_ids)
            or len(set(requested_attempt_ids)) != len(requested_attempt_ids)
        ):
            raise LegacyFixedComparisonError(
                "targeted comparison canary IDs must be unique and match task count"
            )
        requested_set = set(requested_attempt_ids)
        selected_tasks = [
            task
            for task in parent_tasks
            if str(task.get("attempt_id") or "") in requested_set
        ]
        if len(selected_tasks) != task_count:
            raise LegacyFixedComparisonError(
                "targeted comparison canary includes an unknown or unresolved attempt"
            )
    else:
        selected_tasks = parent_tasks[:task_count]
    items_by_attempt = {
        str(attempt.get("attempt_id") or ""): (run_dir, attempt, row, metadata)
        for run_dir, attempt, row, metadata in parent.items
    }
    if len(items_by_attempt) != len(parent.items):
        raise LegacyFixedComparisonError("comparison parent has ambiguous attempt items")

    selected_items: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    selected_receipts: dict[str, dict[str, Any]] = {}
    selected_evidence_plans: dict[str, dict[str, Any]] = {}
    delivery_task_ids: dict[str, str] = {}
    canary_tasks: list[dict[str, Any]] = []
    for parent_task in selected_tasks:
        if not isinstance(parent_task, dict):
            raise LegacyFixedComparisonError("comparison canary parent task is malformed")
        attempt_id = str(parent_task.get("attempt_id") or "")
        parent_task_id = str(parent_task.get("task_id") or "")
        if not attempt_id or not parent_task_id:
            raise LegacyFixedComparisonError("comparison canary parent task identity is missing")
        source_item = items_by_attempt.get(attempt_id)
        receipt = parent.cell_receipts_by_attempt_id.get(attempt_id)
        evidence_plan = parent.evidence_plans_by_attempt_id.get(attempt_id)
        if source_item is None or receipt is None or evidence_plan is None:
            raise LegacyFixedComparisonError(
                f"comparison canary parent task is incomplete: {attempt_id}"
            )
        run_dir, source_attempt, row, metadata = source_item
        source_artifact_dir = Path(str(source_attempt.get("artifact_dir") or "")).absolute()
        try:
            artifact_relative = source_artifact_dir.relative_to(parent.output_root.absolute())
        except ValueError as exc:
            raise LegacyFixedComparisonError(
                f"comparison canary parent output escapes its root: {attempt_id}"
            ) from exc
        clone_attempt = dict(source_attempt)
        clone_attempt["artifact_dir"] = str(output_root / artifact_relative)
        delivery_task_id = (
            "legacy-fixed-comparison-canary-"
            + canonical_sha256(
                {
                    "schema": LEGACY_FIXED_COMPARISON_CANARY_PLAN_SCHEMA,
                    "parent_plan_id": parent_plan_id,
                    "parent_task_id": parent_task_id,
                    "attempt_id": attempt_id,
                }
            ).removeprefix("sha256:")
        )
        if attempt_id in delivery_task_ids:
            raise LegacyFixedComparisonError("comparison canary selected a duplicate attempt")
        selected_items.append((run_dir, clone_attempt, dict(row), dict(metadata)))
        selected_receipts[attempt_id] = dict(receipt)
        selected_evidence_plans[attempt_id] = dict(evidence_plan)
        delivery_task_ids[attempt_id] = delivery_task_id
        canary_tasks.append(
            {
                "attempt_id": attempt_id,
                "parent_task_id": parent_task_id,
                "delivery_task_id": delivery_task_id,
                "evidence_plan_id": str(evidence_plan.get("plan_id") or ""),
                "execution_cell_sha256": str(
                    receipt.get("execution_cell_sha256") or ""
                ),
                "output_artifact_dir": artifact_relative.as_posix(),
            }
        )

    canary_descriptor = {
        "schema": LEGACY_FIXED_COMPARISON_CANARY_PLAN_SCHEMA,
        "comparison_id": canary_id,
        "parent_comparison_id": parent.comparison_id,
        "parent_plan_id": parent_plan_id,
        "parent_execution_plan_id": parent_execution_plan_id,
        "analysis_window_start": parent.plan.get("analysis_window_start"),
        "analysis_window_end": parent.plan.get("analysis_window_end"),
        "requested_horizon_months": parent.plan.get("requested_horizon_months"),
        "evidence_role": parent.plan.get("evidence_role"),
        "authority": parent.plan.get("authority"),
        "tasks": canary_tasks,
        "archive_read_only": True,
    }
    canary_plan = {
        "plan_id": canonical_sha256(canary_descriptor),
        **canary_descriptor,
        "preflight": {
            "schema": LEGACY_FIXED_COMPARISON_PREFLIGHT_SCHEMA,
            "plan_id": canonical_sha256(canary_descriptor),
            "task_count": len(selected_items),
            "parent_plan_id": parent_plan_id,
            "archive_read_only": True,
            "enqueued": False,
        },
    }
    return PreparedLegacyFixedComparison(
        plan=canary_plan,
        output_root=output_root,
        legacy_controls=parent.legacy_controls,
        archive_runs_root=parent.archive_runs_root,
        recorded_archive_runs_root=parent.recorded_archive_runs_root,
        archive_id=parent.archive_id,
        authority_execution_plan=parent.authority_execution_plan,
        comparison_id=canary_id,
        items=selected_items,
        cell_receipts_by_attempt_id=selected_receipts,
        evidence_plans_by_attempt_id=selected_evidence_plans,
        task_ids_by_attempt_id=delivery_task_ids,
    )


def _write_execution_report(
    *,
    prepared: PreparedLegacyFixedComparison,
    results: list[dict[str, Any]],
    calculated: int,
    parent_plan_id: str | None,
) -> Path:
    if len(results) != len(prepared.items):
        raise LegacyFixedComparisonError("comparison result accounting is incomplete")
    status_counts: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "")
        status_counts[status] = status_counts.get(status, 0) + 1
    allowed_statuses = {"calculated", "nonviable"}
    if set(status_counts) - allowed_statuses:
        raise LegacyFixedComparisonError("comparison result has an unknown terminal status")
    if calculated != status_counts.get("calculated", 0):
        raise LegacyFixedComparisonError("comparison calculated accounting differs")
    if sum(status_counts.values()) != len(prepared.items):
        raise LegacyFixedComparisonError("comparison terminal accounting differs")
    report = {
        "schema": LEGACY_FIXED_COMPARISON_EXECUTION_REPORT_SCHEMA,
        "status": "complete",
        "plan_id": prepared.plan["plan_id"],
        "parent_plan_id": parent_plan_id,
        "comparison_id": prepared.comparison_id,
        "analysis_window_start": prepared.plan.get("analysis_window_start"),
        "analysis_window_end": prepared.plan.get("analysis_window_end"),
        "authority": prepared.plan.get("authority"),
        "task_accounting": {
            "selected": len(prepared.items),
            "calculated": calculated,
            "nonviable": status_counts.get("nonviable", 0),
            "failed": 0,
        },
        "results": results,
    }
    report_path = prepared.output_root / "execution-report.json"
    _assert_owned_output_path(
        output_path=report_path,
        output_root=prepared.output_root,
        archive_runs_root=prepared.archive_runs_root,
    )
    write_immutable_json(report_path, report)
    return report_path


def _validate_pre_enqueue_task_construction(
    *,
    prepared: PreparedLegacyFixedComparison,
    config: Any,
    lab_config: LabBacktestConfig,
    campaign_plan_id: str,
) -> dict[str, dict[str, Any]]:
    """Build every exact worker payload before a comparison can enqueue any task."""

    prebuilt_tasks: dict[str, dict[str, Any]] = {}
    for run_dir, attempt, _row, run_metadata in prepared.items:
        attempt_id = str(attempt.get("attempt_id") or "")
        receipt_raw = prepared.cell_receipts_by_attempt_id.get(attempt_id)
        evidence_plan = prepared.evidence_plans_by_attempt_id.get(attempt_id)
        expected_task_id = prepared.task_ids_by_attempt_id.get(attempt_id)
        if not isinstance(receipt_raw, dict) or not isinstance(evidence_plan, dict):
            raise LegacyFixedComparisonError(
                f"comparison task is missing frozen evidence inputs: {attempt_id}"
            )
        try:
            receipt = FrozenExecutionCellReceipt.model_validate(receipt_raw)
            task = build_full_backtest_lab_task(
                config=config,
                run_dir=run_dir,
                attempt=attempt,
                run_metadata=run_metadata,
                lab_config=lab_config,
                batch_id="legacy-fixed-comparison-preenqueue",
                evidence_window_start=WINDOW_START,
                evidence_window_end=WINDOW_END,
                requested_horizon_months=36,
                evidence_role=LEGACY_COMPARISON_ROLE,
                selection_data_end=WINDOW_END,
                campaign_plan_id=campaign_plan_id,
                lake_manifest_sha256=REQUIRED_LAKE_SEMANTIC_SHA256,
                evidence_plan=evidence_plan,
                tracked_cell=receipt.execution_cell,
                task_id=expected_task_id,
            )
        except Exception as exc:
            raise LegacyFixedComparisonError(
                "comparison pre-enqueue task construction failed: "
                f"{attempt_id}: {exc}"
            ) from exc
        if str(task.get("task_id") or "") != expected_task_id:
            raise LegacyFixedComparisonError(
                f"comparison task identity differs during pre-enqueue validation: {attempt_id}"
            )
        prebuilt_tasks[attempt_id] = task
    if set(prebuilt_tasks) != set(prepared.task_ids_by_attempt_id):
        raise LegacyFixedComparisonError(
            "comparison pre-enqueue task accounting differs from the immutable plan"
        )
    return prebuilt_tasks


def execute_legacy_fixed_comparison(
    *,
    prepared: PreparedLegacyFixedComparison,
    config: Any,
    trading_dashboard_root: Path,
    gateway_url: str | None = None,
    gateway_token: str | None = None,
    lake_url: str | None = None,
    lake_token: str | None = None,
    max_workers: int = 1,
    canary_task_count: int | None = None,
    canary_attempt_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Execute a previously prepared plan; callers must opt in explicitly."""

    revalidated = prepare_legacy_fixed_comparison(
        config=config,
        legacy_controls=prepared.legacy_controls,
        archive_runs_root=prepared.recorded_archive_runs_root,
        authority_execution_plan=prepared.authority_execution_plan,
        trading_dashboard_root=trading_dashboard_root,
        comparison_id=prepared.comparison_id,
        archive_id=prepared.archive_id,
    )
    if revalidated.plan != prepared.plan:
        raise LegacyFixedComparisonError(
            "comparison source, authority, or plan identity differs before enqueue"
        )
    execution_prepared = revalidated
    parent_plan_id: str | None = None
    if canary_task_count is not None:
        execution_prepared = prepare_legacy_fixed_comparison_canary(
            parent=revalidated,
            task_count=int(canary_task_count),
            attempt_ids=canary_attempt_ids,
        )
        write_legacy_fixed_comparison_plan(execution_prepared)
        parent_plan_id = str(revalidated.plan["plan_id"])
    _verify_live_lake_identity(lake_url=lake_url, lake_token=lake_token)
    for _run_dir, attempt, _row, _metadata in execution_prepared.items:
        artifact_dir = Path(str(attempt["artifact_dir"]))
        _assert_owned_output_path(
            output_path=artifact_dir,
            output_root=execution_prepared.output_root,
            archive_runs_root=execution_prepared.archive_runs_root,
        )
        artifact_dir.mkdir(parents=True, exist_ok=True)
    live_lab = resolve_lab_backtest_config(
        gateway_url=gateway_url,
        gateway_token=gateway_token,
        trading_dashboard_root=trading_dashboard_root,
        worker_contract_hash=None,
    )
    if live_lab.worker_contract_hash != REQUIRED_WORKER_CONTRACT_SHA256:
        raise LegacyFixedComparisonError("live worker contract differs from comparison plan")
    lab_config = LabBacktestConfig(
        gateway_url=live_lab.gateway_url,
        gateway_token=live_lab.gateway_token,
        worker_contract_hash=REQUIRED_WORKER_CONTRACT_SHA256,
        worker_contract_schema=live_lab.worker_contract_schema,
        deadline_seconds=live_lab.deadline_seconds,
        poll_interval_seconds=live_lab.poll_interval_seconds,
        result_batch_size=live_lab.result_batch_size,
    )
    campaign_plan_id = str(
        execution_prepared.plan.get("parent_execution_plan_id")
        or execution_prepared.plan.get("execution_plan_id")
        or ""
    )
    if not _SHA256_RE.fullmatch(campaign_plan_id):
        raise LegacyFixedComparisonError("comparison execution plan identity is invalid")
    prebuilt_tasks = _validate_pre_enqueue_task_construction(
        prepared=execution_prepared,
        config=config,
        lab_config=lab_config,
        campaign_plan_id=campaign_plan_id,
    )
    results, calculated, failed = run_lab_full_backtests(
        config=config,
        items=execution_prepared.items,
        lab_config=lab_config,
        max_workers=max(1, int(max_workers)),
        requested_horizon_months=36,
        evidence_window_start=WINDOW_START,
        evidence_window_end=WINDOW_END,
        evidence_role=LEGACY_COMPARISON_ROLE,
        selection_data_end=WINDOW_END,
        campaign_plan_id=campaign_plan_id,
        lake_manifest_sha256=REQUIRED_LAKE_SEMANTIC_SHA256,
        cell_receipts_by_attempt_id=execution_prepared.cell_receipts_by_attempt_id,
        evidence_plans_by_attempt_id=execution_prepared.evidence_plans_by_attempt_id,
        task_ids_by_attempt_id=execution_prepared.task_ids_by_attempt_id,
        prebuilt_tasks_by_attempt_id=prebuilt_tasks,
    )
    if failed:
        raise LegacyFixedComparisonError(
            f"legacy comparison has {failed} failed fixed-cell replays"
        )
    report_path = _write_execution_report(
        prepared=execution_prepared,
        results=results,
        calculated=calculated,
        parent_plan_id=parent_plan_id,
    )
    return {
        "plan_id": revalidated.plan["plan_id"],
        "execution_plan_id": execution_prepared.plan["plan_id"],
        "parent_plan_id": parent_plan_id,
        "calculated": calculated,
        "failed": failed,
        "results": results,
        "report_path": str(report_path),
    }


def format_legacy_fixed_preflight(prepared: PreparedLegacyFixedComparison) -> dict[str, Any]:
    """Return a JSON-safe preflight without writing or contacting the gateway."""

    return {
        **prepared.plan["preflight"],
        "plan_id": prepared.plan["plan_id"],
        "comparison_id": prepared.plan["comparison_id"],
        "analysis_window_start": WINDOW_START,
        "analysis_window_end": WINDOW_END,
        "output_root": str(prepared.output_root),
        "task_ids_sha256": canonical_sha256(
            [task["task_id"] for task in prepared.plan["tasks"]]
        ),
        "terminal_outcomes": prepared.plan["terminal_outcomes"],
    }
