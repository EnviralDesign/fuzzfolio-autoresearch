from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from .catalog_index import iter_catalog_rows
from .config import AppConfig
from .corpus_lab_backtests import LabBacktestConfig, resolve_lab_backtest_config
from .evidence_plan import (
    canonical_json,
    canonical_sha256,
    normalize_evidence_profile_snapshot,
    validate_replay_evidence_plan,
)
from .fixed_cohort import (
    FIXED_COHORT_SCHEMA,
    fixed_cohort_attempt_ids,
    validate_fixed_corpus_cohort,
)
from .ledger import load_attempts, load_run_metadata
from .level_c import (
    LEVEL_C_COHORT_SCHEMA,
    ProfileSnapshotResolver,
    validate_level_c_cohort,
)
from .level_c_operator import validate_profile_model_source_lock
from .nested_gateway import (
    freeze_nested_gateway_cells_fold,
    run_nested_gateway_selected_outer_fold,
    run_nested_gateway_training_fold,
)
from .portfolio import _resolve_account_spec
from .portfolio_research import (
    load_research_suite,
    run_nested_cell_temporal_validation,
    temporal_folds,
)
from .play_hand_lab import PlayHandLabRuntimeConfig, _worker_ready_profile_snapshot


NESTED_PHASE_SCHEMA = "autoresearch-nested-evidence-phase-v1"
PHASE_NAMES = (
    "training-evidence",
    "frozen-cells",
    "frozen-portfolio",
    "selected-outer",
    "final-report",
)


class NestedPipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class NestedPipelineContext:
    config: AppConfig
    campaign_id: str
    campaign_plan_id: str
    execution_plan_id: str | None
    suite_name: str
    suite: dict[str, Any]
    account: dict[str, Any]
    campaign_root: Path
    cohort_path: Path | None
    cohort_manifest_id: str | None
    requested_attempt_ids: tuple[str, ...]
    items: tuple[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]], ...]
    catalog_rows: tuple[dict[str, Any], ...]
    folds: tuple[dict[str, Any], ...]
    train_months: int
    test_months: int
    selection_basis: str
    optimizer_backend: str
    max_workers: int
    lake_manifest_sha256: str
    lab_config: LabBacktestConfig
    preview: dict[str, Any]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _create_or_verify(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    encoded = canonical_json(normalized).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != encoded:
            raise NestedPipelineError(f"immutable nested phase artifact drift: {path}")
        return normalized
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise NestedPipelineError(f"nested phase artifact raced: {path}") from exc
    finally:
        temporary.unlink(missing_ok=True)
    return normalized


def _load_phase(context: NestedPipelineContext, name: str) -> dict[str, Any]:
    path = context.campaign_root / "phases" / f"{name}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NestedPipelineError(f"nested phase is missing or invalid: {name}") from exc
    identity = {key: value for key, value in payload.items() if key != "phase_sha256"}
    if (
        payload.get("schema_version") != NESTED_PHASE_SCHEMA
        or payload.get("phase") != name
        or payload.get("campaign_plan_id") != context.campaign_plan_id
        or payload.get("execution_plan_id") != context.execution_plan_id
        or payload.get("phase_sha256") != canonical_sha256(identity)
    ):
        raise NestedPipelineError(f"nested phase identity differs: {name}")
    return payload


def _reuse_phase_path(context: NestedPipelineContext, name: str) -> Path | None:
    path = context.campaign_root / "phases" / f"{name}.json"
    if not path.exists():
        return None
    _load_phase(context, name)
    return path


def _write_phase(
    context: NestedPipelineContext, name: str, body: Mapping[str, Any]
) -> tuple[dict[str, Any], Path]:
    if name not in PHASE_NAMES:
        raise NestedPipelineError(f"unknown nested phase: {name}")
    identity = {
        "schema_version": NESTED_PHASE_SCHEMA,
        "phase": name,
        "campaign_id": context.campaign_id,
        "campaign_plan_id": context.campaign_plan_id,
        "execution_plan_id": context.execution_plan_id,
        "cohort_manifest_id": context.cohort_manifest_id,
        "requested_attempt_ids": list(context.requested_attempt_ids),
        **dict(body),
    }
    payload = {**identity, "phase_sha256": canonical_sha256(identity)}
    path = context.campaign_root / "phases" / f"{name}.json"
    return _create_or_verify(path, payload), path


def _cohort_attempts(
    path: Path,
    runs_root: Path,
    *,
    profile_snapshot_resolver: ProfileSnapshotResolver | None = None,
) -> tuple[list[str], dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NestedPipelineError(f"attempt cohort is invalid: {path}") from exc
    schema = raw.get("schema") if isinstance(raw, dict) else None
    if schema == LEVEL_C_COHORT_SCHEMA:
        cohort = validate_level_c_cohort(
            path,
            relocated_runs_root=runs_root,
            profile_snapshot_resolver=profile_snapshot_resolver,
        )
        candidates = cohort.get("candidates") or []
        attempt_ids = [str(row.get("attempt_id") or "") for row in candidates]
    elif schema == FIXED_COHORT_SCHEMA:
        cohort = validate_fixed_corpus_cohort(path)
        attempt_ids = fixed_cohort_attempt_ids(cohort)
    else:
        raise NestedPipelineError("attempt cohort schema is unsupported")
    if any(not attempt_id for attempt_id in attempt_ids) or len(set(attempt_ids)) != len(
        attempt_ids
    ):
        raise NestedPipelineError("attempt cohort contains missing or duplicate attempt IDs")
    return attempt_ids, cohort


def _resolve_account(config: AppConfig, suite: Mapping[str, Any]) -> dict[str, Any]:
    preset_name = str(suite.get("account_preset") or "").strip()
    raw_path = str(suite.get("account_config") or "").strip()
    path = Path(raw_path).expanduser().resolve() if raw_path else config.repo_root / "portfolio.account-presets.json"
    presets: dict[str, Any] = {}
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise NestedPipelineError("account preset file must contain an object")
        presets = dict(payload.get("account_presets") or {})
    if preset_name and preset_name not in presets:
        raise NestedPipelineError(f"account preset is missing: {preset_name}")
    return _resolve_account_spec(preset_name or {}, presets, fallback={})


def _configured_dashboard_root(config: AppConfig) -> Path:
    configured = config.fuzzfolio.workspace_root
    return Path(configured).resolve() if configured else (config.repo_root.parent / "Trading-Dashboard").resolve()


def _live_worker_contract(root: Path) -> str:
    package_root = root / "shared" / "python" / "fuzzfolio_core"
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    try:
        from fuzzfolio_core.contracts.worker_contract import build_replay_worker_contract

        return str(build_replay_worker_contract(repo_root=root).contract_hash)
    except Exception as exc:
        raise NestedPipelineError(f"could not resolve live replay worker contract: {exc}") from exc


def _worker_ready_profile_snapshot_resolver(
    *, config: AppConfig, trading_dashboard_root: Path
) -> ProfileSnapshotResolver:
    runtime = PlayHandLabRuntimeConfig(
        task_mode="deep_replay",
        trading_dashboard_root=trading_dashboard_root,
    )

    def resolve(profile_payload: dict[str, Any]) -> dict[str, Any]:
        return _worker_ready_profile_snapshot(
            profile_payload,
            config=config,
            runtime=runtime,
        )

    return resolve


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _level_c_campaign_root(*, config: AppConfig, campaign_id: str) -> Path:
    token = str(campaign_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", token):
        raise NestedPipelineError("nested campaign ID must be a safe single path component")
    raw_base = config.derived_root / "nested-evidence"
    if raw_base.exists() and (not raw_base.is_dir() or raw_base.is_symlink()):
        raise NestedPipelineError("nested evidence root is not a regular directory")
    raw_campaign_root = raw_base / token
    if raw_campaign_root.exists() and (
        not raw_campaign_root.is_dir() or raw_campaign_root.is_symlink()
    ):
        raise NestedPipelineError("nested campaign root is not a regular directory")
    base = raw_base.resolve()
    campaign_root = raw_campaign_root.resolve()
    if campaign_root.parent != base:
        raise NestedPipelineError("nested campaign root escapes the derived evidence root")
    return campaign_root


def _level_c_nested_artifact_dir(
    *, campaign_root: Path, attempt_id: str, create: bool
) -> Path:
    """Resolve a deterministic, nested-owned evidence root for one source attempt."""
    token = canonical_sha256({"attempt_id": attempt_id}).removeprefix("sha256:")
    root = campaign_root / "attempt-evidence"
    if root.exists() and (not root.is_dir() or root.is_symlink()):
        raise NestedPipelineError("nested evidence root is not a regular directory")
    if create:
        root.mkdir(parents=True, exist_ok=True)
    target = root / token
    if target.exists() and (not target.is_dir() or target.is_symlink()):
        raise NestedPipelineError("nested attempt evidence root is not a regular directory")
    if create:
        target.mkdir(parents=True, exist_ok=True)
    if not _path_within(target, root):
        raise NestedPipelineError("nested attempt evidence root escapes the campaign")
    return target


def _resolve_level_c_recorded_path(
    value: Any,
    *,
    recorded_runs_root: Path,
    active_runs_root: Path,
    label: str,
) -> Path:
    """Rebase a cohort-attested in-tree path into the active generation root."""
    raw_value = str(value or "").strip()
    if not raw_value:
        raise NestedPipelineError(f"{label} is missing")
    raw = Path(raw_value).expanduser()
    if raw.is_absolute():
        try:
            relative = raw.resolve(strict=False).relative_to(
                recorded_runs_root.resolve(strict=False)
            )
        except ValueError as exc:
            raise NestedPipelineError(
                f"{label} escapes the recorded generation runs root: {raw}"
            ) from exc
    else:
        if any(part == ".." for part in raw.parts) or str(raw) in {"", "."}:
            raise NestedPipelineError(f"{label} must be an in-tree path")
        relative = raw
    resolved = (active_runs_root.resolve() / relative).resolve(strict=False)
    if not _path_within(resolved, active_runs_root):
        raise NestedPipelineError(f"{label} escapes the active generation runs root")
    return resolved


def _resolve_level_c_cohort_rows(
    *,
    cohort: Mapping[str, Any],
    runs_root: Path,
    requested_attempt_ids: list[str],
    profile_snapshot_resolver: ProfileSnapshotResolver,
) -> list[dict[str, Any]]:
    """Resolve frozen Level C PlayHand candidates without a mutable corpus index.

    A fresh formal PlayHand campaign is intentionally not added to the legacy corpus
    catalog. The validated Level C cohort is the authority for its exact lane and
    canonical attempt; this function revalidates that narrow evidence path before
    returning catalog-shaped rows for the existing nested executor.
    """
    recorded_runs_root_value = str(cohort.get("runs_root") or "").strip()
    if not recorded_runs_root_value:
        raise NestedPipelineError("Level C cohort omitted its recorded runs root")
    recorded_runs_root = Path(recorded_runs_root_value).expanduser()
    active_runs_root = runs_root.expanduser().resolve()
    candidates = list(cohort.get("candidates") or [])
    by_id = {
        str(candidate.get("attempt_id") or ""): candidate
        for candidate in candidates
        if isinstance(candidate, Mapping) and str(candidate.get("attempt_id") or "")
    }
    if set(by_id) != set(requested_attempt_ids) or len(by_id) != len(candidates):
        raise NestedPipelineError("Level C cohort candidate membership is ambiguous")

    rows: list[dict[str, Any]] = []
    for attempt_id in requested_attempt_ids:
        candidate = by_id[attempt_id]
        run_id = str(candidate.get("run_id") or "").strip()
        if not run_id or Path(run_id).name != run_id:
            raise NestedPipelineError(
                f"frozen cohort candidate has an unsafe run identity: {attempt_id}"
            )
        run_dir = _resolve_level_c_recorded_path(
            run_id,
            recorded_runs_root=recorded_runs_root,
            active_runs_root=active_runs_root,
            label=f"frozen cohort lane root {attempt_id}",
        )
        if not run_dir.is_dir() or run_dir.is_symlink():
            raise NestedPipelineError(f"frozen cohort lane root is invalid: {attempt_id}")
        metadata = load_run_metadata(run_dir)
        campaign_id = str(cohort.get("playhand_campaign_id") or "")
        if (
            str(metadata.get("run_id") or "") != run_id
            or str(metadata.get("canonical_attempt_id") or "") != attempt_id
            or str(metadata.get("parent_campaign_id") or "") != campaign_id
            or str(metadata.get("lab_campaign_id") or "") != campaign_id
            or str(metadata.get("as_of_date") or "") != str(cohort.get("as_of_date") or "")
            or str(metadata.get("lake_manifest_sha256") or "")
            != str(cohort.get("lake_manifest_sha256") or "")
            or metadata.get("terminal") is not True
            or int(metadata.get("failed_task_count") or 0) != 0
        ):
            raise NestedPipelineError(
                f"frozen cohort lane metadata differs: {attempt_id}"
            )
        matches = [
            row
            for row in load_attempts(run_dir / "attempts.jsonl")
            if str(row.get("attempt_id") or "") == attempt_id
        ]
        if len(matches) != 1:
            raise NestedPipelineError(
                f"frozen cohort canonical attempt is missing or ambiguous: {attempt_id}"
            )
        attempt = dict(matches[0])
        if (
            str(attempt.get("run_id") or "") != run_id
            or str(attempt.get("runner") or "") != "play_hand_v1"
            or str(attempt.get("play_hand_stage") or "") != "final_36mo"
        ):
            raise NestedPipelineError(
                f"frozen cohort canonical attempt identity differs: {attempt_id}"
            )
        expected_profile = _resolve_level_c_recorded_path(
            candidate.get("profile_path_relative_to_runs_root"),
            recorded_runs_root=recorded_runs_root,
            active_runs_root=active_runs_root,
            label=f"frozen cohort profile {attempt_id}",
        )
        recorded_profile = _resolve_level_c_recorded_path(
            attempt.get("profile_path"),
            recorded_runs_root=recorded_runs_root,
            active_runs_root=active_runs_root,
            label=f"frozen cohort canonical attempt profile {attempt_id}",
        )
        if (
            recorded_profile != expected_profile
            or not expected_profile.is_file()
            or expected_profile.is_symlink()
            or not _path_within(expected_profile, run_dir)
            or _sha256_file(expected_profile) != str(candidate.get("profile_sha256") or "")
        ):
            raise NestedPipelineError(
                f"frozen cohort profile identity differs: {attempt_id}"
            )
        try:
            authoring_profile = json.loads(expected_profile.read_text(encoding="utf-8"))
            plan = validate_replay_evidence_plan(attempt.get("evidence_plan"))
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise NestedPipelineError(
                f"frozen cohort canonical evidence is invalid: {attempt_id}"
            ) from exc
        if not isinstance(authoring_profile, dict):
            raise NestedPipelineError(f"frozen cohort profile is not an object: {attempt_id}")
        try:
            worker_ready_profile = profile_snapshot_resolver(authoring_profile)
        except Exception as exc:
            raise NestedPipelineError(
                f"frozen cohort worker-ready profile could not be resolved: {attempt_id}"
            ) from exc
        if not isinstance(worker_ready_profile, dict) or (
            canonical_sha256(normalize_evidence_profile_snapshot(worker_ready_profile))
            != plan.profile_snapshot_sha256
        ):
            raise NestedPipelineError(
                f"frozen cohort worker-ready profile differs from evidence: {attempt_id}"
            )
        receipt = attempt.get("execution_evidence")
        if (
            plan.plan_id != str(candidate.get("discovery_evidence_plan_id") or "")
            or str(attempt.get("evidence_plan_id") or "") != plan.plan_id
            or not isinstance(receipt, Mapping)
            or receipt.get("plan_id") != plan.plan_id
            or receipt.get("profile_snapshot_sha256") != plan.profile_snapshot_sha256
            or receipt.get("execution_cell_sha256") != plan.execution_cell_sha256
            or receipt.get("observed_lake_manifest_sha256") != plan.lake_manifest_sha256
        ):
            raise NestedPipelineError(
                f"frozen cohort execution evidence differs: {attempt_id}"
            )
        artifact_dir = _resolve_level_c_recorded_path(
            attempt.get("artifact_dir"),
            recorded_runs_root=recorded_runs_root,
            active_runs_root=active_runs_root,
            label=f"frozen cohort canonical attempt artifact {attempt_id}",
        )
        if (
            not artifact_dir.is_dir()
            or artifact_dir.is_symlink()
            or not _path_within(artifact_dir, run_dir)
            or not (artifact_dir / "deep-replay-job.json").is_file()
        ):
            raise NestedPipelineError(
                f"frozen cohort canonical attempt artifact is invalid: {attempt_id}"
            )
        attempt.update(
            {
                "profile_path": str(expected_profile),
                "artifact_dir": str(artifact_dir),
                # The lane metadata, not a mutable catalog flag, attests canonicality.
                "is_canonical_attempt": True,
                "is_canonical_playhand_attempt": True,
                "_worker_ready_profile_snapshot": dict(worker_ready_profile),
                "_nested_input_source": "level_c_frozen_playhand_evidence",
            }
        )
        rows.append(attempt)
    return rows


def prepare_nested_pipeline(
    *,
    config: AppConfig,
    campaign_id: str,
    suite_name: str,
    suite_config_path: Path | None,
    run_ids: list[str] | None,
    attempt_ids: list[str] | None,
    scope: str,
    start: str,
    end: str,
    train_months: int,
    test_months: int,
    step_months: int,
    embargo_days: int,
    selection_basis: str,
    max_workers: int,
    gateway_url: str | None,
    gateway_token: str | None,
    lake_manifest_sha256: str | None,
    lake_url: str | None = None,
    lake_token: str | None = None,
    trading_dashboard_root: Path | None,
    optimizer_backend: str,
    attempt_cohort: Path | None,
    execution_plan_id: str | None = None,
    bound_worker_contract_hash: str | None = None,
    bound_trading_dashboard_root: Path | None = None,
    profile_model_source_lock: Mapping[str, Any] | None = None,
    dry_run: bool = False,
) -> NestedPipelineContext:
    resolved_lake_manifest = str(lake_manifest_sha256 or "").strip()
    resolved_lake_url = str(
        lake_url or os.environ.get("REMOTE_MARKET_DATA_LAKE_BASE_URL") or ""
    ).strip()
    if not resolved_lake_manifest and resolved_lake_url:
        import requests

        headers = {"Authorization": f"Bearer {lake_token}"} if str(lake_token or "").strip() else {}
        response = requests.get(
            f"{resolved_lake_url.rstrip('/')}/api/lake/manifest",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        resolved_lake_manifest = str(response.json().get("coverage_sha256") or "").strip()
    if not resolved_lake_manifest and not dry_run:
        raise NestedPipelineError("nested evidence requires an exact lake manifest identity")
    resolved_lake_manifest = resolved_lake_manifest or "dry-run-unresolved"
    explicit = [str(value).strip() for value in (attempt_ids or []) if str(value).strip()]
    cohort: dict[str, Any] | None = None
    cohort_ids: list[str] | None = None
    cohort_path = attempt_cohort.resolve() if attempt_cohort is not None else None
    if dry_run and not hasattr(config, "fuzzfolio"):
        effective_contract = bound_worker_contract_hash or "dry-run-unresolved"
        lab_config = LabBacktestConfig(worker_contract_hash=effective_contract)
        profile_snapshot_resolver = None
    else:
        configured_root = _configured_dashboard_root(config)
        plan_root = (
            bound_trading_dashboard_root.expanduser().resolve()
            if bound_trading_dashboard_root is not None
            else configured_root
        )
        effective_root = (
            trading_dashboard_root.expanduser().resolve()
            if trading_dashboard_root
            else configured_root
        )
        if configured_root != plan_root or effective_root != plan_root:
            raise NestedPipelineError("alternate Trading-Dashboard root is not permitted")
        if profile_model_source_lock is not None:
            validate_profile_model_source_lock(profile_model_source_lock, effective_root)
        live_contract = _live_worker_contract(effective_root)
        if bound_worker_contract_hash and live_contract != bound_worker_contract_hash:
            raise NestedPipelineError("live replay worker contract differs from authority plan")
        effective_contract = bound_worker_contract_hash or live_contract
        profile_snapshot_resolver = _worker_ready_profile_snapshot_resolver(
            config=config,
            trading_dashboard_root=effective_root,
        )
        lab_config = resolve_lab_backtest_config(
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            trading_dashboard_root=effective_root,
            worker_contract_hash=effective_contract,
            deadline_seconds=3600,
            result_batch_size=max(25, int(max_workers) * 2),
        )
    if cohort_path is not None:
        cohort_ids, cohort = _cohort_attempts(
            cohort_path,
            config.runs_root,
            profile_snapshot_resolver=profile_snapshot_resolver,
        )
        if explicit and set(explicit) != set(cohort_ids):
            raise NestedPipelineError("explicit attempt IDs differ from the frozen cohort")
    requested = cohort_ids or explicit
    if len(set(requested)) != len(requested):
        raise NestedPipelineError("requested nested attempt IDs are duplicate or ambiguous")
    suite_path = suite_config_path.resolve() if suite_config_path else config.repo_root / "portfolio.research-suites.json"
    _document, suite = load_research_suite(suite_path, suite_name)
    folds = temporal_folds(
        start=start,
        end=end,
        train_months=int(train_months),
        test_months=int(test_months),
        step_months=int(step_months),
        embargo_days=int(embargo_days),
    )
    if not folds:
        raise NestedPipelineError("nested fold geometry does not fit the requested range")
    is_level_c_cohort = cohort is not None and cohort.get("schema") == LEVEL_C_COHORT_SCHEMA
    worker_ready_profiles: dict[str, dict[str, Any]] = {}
    if is_level_c_cohort:
        if profile_snapshot_resolver is None:
            raise NestedPipelineError(
                "Level C nested evidence requires the plan-bound profile snapshot resolver"
            )
        resolved_rows = _resolve_level_c_cohort_rows(
            cohort=cohort,
            runs_root=config.runs_root,
            requested_attempt_ids=requested,
            profile_snapshot_resolver=profile_snapshot_resolver,
        )
        worker_ready_profiles = {
            str(row["attempt_id"]): dict(row["_worker_ready_profile_snapshot"])
            for row in resolved_rows
        }
        input_resolution = "level_c_frozen_playhand_evidence"
    else:
        rows = list(
            iter_catalog_rows(
                config,
                run_ids=run_ids,
                attempt_ids=sorted(requested) if requested else None,
            )
        )
        if str(scope or "canonical").lower() == "canonical":
            rows = [
                row
                for row in rows
                if row.get("is_canonical_attempt") or row.get("is_canonical_playhand_attempt")
            ]
        if not requested:
            requested = [str(row.get("attempt_id") or "") for row in rows]
            if not requested:
                raise NestedPipelineError("no attempts matched the nested evidence scope")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get("attempt_id") or ""), []).append(dict(row))
        missing = sorted(set(requested) - set(grouped))
        ambiguous = sorted(key for key, values in grouped.items() if len(values) != 1)
        extra = sorted(set(grouped) - set(requested))
        if missing or ambiguous or extra:
            raise NestedPipelineError(
                f"frozen cohort catalog resolution failed: missing={missing}, ambiguous={ambiguous}, extra={extra}"
            )
        resolved_rows = [grouped[attempt_id][0] for attempt_id in requested]
        input_resolution = "attempt_catalog"

    if is_level_c_cohort:
        candidates = {
            str(candidate.get("attempt_id") or ""): candidate
            for candidate in cohort.get("candidates") or []
        }
        for row in resolved_rows:
            attempt_id = str(row.get("attempt_id") or "")
            candidate = candidates.get(attempt_id)
            if not isinstance(candidate, Mapping):
                raise NestedPipelineError("frozen cohort candidate identity is missing")
            expected_profile = (
                config.runs_root
                / str(candidate.get("profile_path_relative_to_runs_root") or "")
            ).resolve()
            recorded_profile = Path(str(row.get("profile_path") or "")).resolve()
            if (
                str(row.get("run_id") or "") != str(candidate.get("run_id") or "")
                or recorded_profile != expected_profile
                or _sha256_file(expected_profile) != candidate.get("profile_sha256")
            ):
                raise NestedPipelineError(
                    f"frozen cohort catalog identity differs: {attempt_id}"
                )
            try:
                authoring_profile = json.loads(expected_profile.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise NestedPipelineError(
                    f"frozen cohort profile is unreadable: {attempt_id}"
                ) from exc
            if not isinstance(authoring_profile, dict):
                raise NestedPipelineError(
                    f"frozen cohort profile is not an object: {attempt_id}"
                )
            try:
                worker_ready_profile = profile_snapshot_resolver(authoring_profile)
            except Exception as exc:
                raise NestedPipelineError(
                    f"frozen cohort worker-ready profile could not be resolved: {attempt_id}"
                ) from exc
            if not isinstance(worker_ready_profile, dict):
                raise NestedPipelineError(
                    f"frozen cohort worker-ready profile is not an object: {attempt_id}"
                )
            worker_ready_profiles[attempt_id] = dict(worker_ready_profile)

    cohort_manifest_id = str((cohort or {}).get("manifest_id") or "") or None
    campaign_plan_id = str(campaign_id)
    if cohort_manifest_id:
        campaign_plan_id += f":attempt-cohort:{cohort_manifest_id}"
    if execution_plan_id:
        campaign_plan_id += f":execution-plan:{execution_plan_id}"
    campaign_root = (
        _level_c_campaign_root(config=config, campaign_id=str(campaign_id))
        if is_level_c_cohort
        else config.derived_root / "nested-evidence" / str(campaign_id)
    )

    def materialize_item(
        row: dict[str, Any],
    ) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
        attempt = dict(row)
        attempt_id = str(attempt.get("attempt_id") or "")
        if attempt_id in worker_ready_profiles:
            attempt["_worker_ready_profile_snapshot"] = worker_ready_profiles[attempt_id]
        run_dir = config.runs_root / str(row["run_id"])
        if is_level_c_cohort:
            source_artifact_raw = Path(str(attempt.get("artifact_dir") or ""))
            if not source_artifact_raw.is_dir() or source_artifact_raw.is_symlink():
                raise NestedPipelineError(
                    f"Level C source artifact directory is invalid: {attempt_id}"
                )
            source_artifact_dir = source_artifact_raw.resolve()
            if _path_within(campaign_root, run_dir) or _path_within(
                run_dir, campaign_root
            ):
                raise NestedPipelineError(
                    f"Level C nested evidence overlaps frozen source evidence: {attempt_id}"
                )
            nested_artifact_dir = _level_c_nested_artifact_dir(
                campaign_root=campaign_root,
                attempt_id=attempt_id,
                create=not dry_run,
            )
            attempt["_nested_source_artifact_dir"] = str(source_artifact_dir)
            attempt["_nested_materialization_root"] = str(
                (campaign_root / "attempt-evidence").resolve()
            )
            attempt["artifact_dir"] = str(nested_artifact_dir)
        return run_dir, attempt, dict(attempt), load_run_metadata(run_dir)

    items = tuple(
        materialize_item(row)
        for row in resolved_rows
    )
    inner_config = dict((suite.get("temporal_validation") or {}).get("inner_validation") or {})
    inner_geometry = []
    for outer_fold in folds:
        outer_start = date.fromisoformat(str(outer_fold["train_start"])[:10])
        outer_end = date.fromisoformat(str(outer_fold["train_end"])[:10])
        total_months = max(1, (outer_end.year - outer_start.year) * 12 + outer_end.month - outer_start.month)
        inner = temporal_folds(
            start=str(outer_fold["train_start"]),
            end=str(outer_fold["train_end"]),
            train_months=int(inner_config.get("train_months") or max(1, total_months // 2)),
            test_months=int(inner_config.get("test_months") or max(1, total_months // 6)),
            step_months=int(inner_config.get("step_months") or inner_config.get("test_months") or max(1, total_months // 6)),
            embargo_days=int(inner_config.get("embargo_days", outer_fold.get("embargo_days") or 0)),
        )
        inner_geometry.append({"outer_fold_id": outer_fold["fold_id"], "inner_fold_count": len(inner), "inner_folds": inner})
    preview = {
        "status": "pending",
        "campaign_id": campaign_id,
        "evidence_campaign_plan_id": campaign_plan_id,
        "execution_plan_id": execution_plan_id,
        "attempt_cohort_manifest_id": cohort_manifest_id,
        "attempt_cohort_path": str(cohort_path) if cohort_path else None,
        "suite": suite_name,
        "suite_config": str(suite_path),
        "campaign_root": str(campaign_root),
        "scope": str(scope or "all").lower(),
        "attempt_count": len(items),
        "fold_count": len(folds),
        "planned_train_jobs": len(items) * len(folds),
        "planned_outer_jobs": len(items) * len(folds),
        "selection_basis": selection_basis,
        "optimizer_backend": optimizer_backend,
        "worker_contract_hash": effective_contract,
        "input_resolution": input_resolution,
        "materialization_root": (
            str((campaign_root / "attempt-evidence").resolve())
            if is_level_c_cohort
            else None
        ),
        "folds": folds,
        "inner_validation": inner_geometry,
    }
    return NestedPipelineContext(
        config=config,
        campaign_id=str(campaign_id),
        campaign_plan_id=campaign_plan_id,
        execution_plan_id=execution_plan_id,
        suite_name=suite_name,
        suite=suite,
        account=_resolve_account(config, suite),
        campaign_root=campaign_root,
        cohort_path=cohort_path,
        cohort_manifest_id=cohort_manifest_id,
        requested_attempt_ids=tuple(requested),
        items=items,
        catalog_rows=tuple(dict(item[1]) for item in items),
        folds=tuple(folds),
        train_months=int(train_months),
        test_months=int(test_months),
        selection_basis=selection_basis,
        optimizer_backend=optimizer_backend,
        max_workers=max(1, int(max_workers)),
        lake_manifest_sha256=resolved_lake_manifest,
        lab_config=lab_config,
        preview=preview,
    )


def _fold_kwargs(context: NestedPipelineContext, fold: dict[str, Any]) -> dict[str, Any]:
    return {
        "config": context.config,
        "items": list(context.items),
        "fold": fold,
        "campaign_plan_id": context.campaign_plan_id,
        "campaign_root": context.campaign_root,
        "lab_config": context.lab_config,
        "max_workers": context.max_workers,
        "train_horizon_months": context.train_months,
        "test_horizon_months": context.test_months,
        "selection_basis": context.selection_basis,
        "lake_manifest_sha256": context.lake_manifest_sha256,
        "emit": lambda message: print(message, file=sys.stderr, flush=True),
    }


def _reports_by_fold(
    context: NestedPipelineContext, reports: list[dict[str, Any]], *, phase: str
) -> dict[str, dict[str, Any]]:
    expected = {str(fold.get("fold_id") or "") for fold in context.folds}
    observed: dict[str, dict[str, Any]] = {}
    for report in reports:
        fold_id = str((report.get("fold") or {}).get("fold_id") or "")
        if not fold_id or fold_id in observed:
            raise NestedPipelineError(f"{phase} contains a missing or duplicate fold")
        observed[fold_id] = report
    if set(observed) != expected:
        raise NestedPipelineError(f"{phase} fold membership differs from the plan")
    return observed


def run_nested_training_phase(context: NestedPipelineContext) -> Path:
    reused = _reuse_phase_path(context, "training-evidence")
    if reused is not None:
        return reused
    reports = [
        run_nested_gateway_training_fold(**_fold_kwargs(context, fold))
        for fold in context.folds
    ]
    for report in _reports_by_fold(context, reports, phase="training evidence").values():
        if set(report.get("requested_attempt_ids") or []) != set(
            context.requested_attempt_ids
        ):
            raise NestedPipelineError("training evidence cohort membership differs")
    payload, path = _write_phase(
        context,
        "training-evidence",
        {
            "status": "complete",
            "training_requested_attempt_ids": list(context.requested_attempt_ids),
            "fold_reports": reports,
        },
    )
    if payload["training_requested_attempt_ids"] != list(context.requested_attempt_ids):
        raise NestedPipelineError("training request membership differs")
    return path


def run_nested_frozen_cells_phase(context: NestedPipelineContext) -> Path:
    reused = _reuse_phase_path(context, "frozen-cells")
    if reused is not None:
        return reused
    _load_phase(context, "training-evidence")
    reports = [
        freeze_nested_gateway_cells_fold(**_fold_kwargs(context, fold))
        for fold in context.folds
    ]
    reports_by_fold = _reports_by_fold(context, reports, phase="frozen cells")
    eligible: dict[str, list[str]] = {}
    terminal: dict[str, list[str]] = {}
    for fold_id, report in reports_by_fold.items():
        eligible[fold_id] = sorted(
            str(row.get("attempt_id"))
            for row in report.get("records") or []
            if row.get("train_validation_status") == "valid"
        )
        terminal[fold_id] = sorted(
            str(row.get("attempt_id"))
            for row in report.get("records") or []
            if row.get("train_validation_status") == "nonviable"
        )
        if (
            len(eligible[fold_id]) != len(set(eligible[fold_id]))
            or len(terminal[fold_id]) != len(set(terminal[fold_id]))
            or set(eligible[fold_id]) & set(terminal[fold_id])
            or set(eligible[fold_id]) | set(terminal[fold_id])
            != set(context.requested_attempt_ids)
        ):
            raise NestedPipelineError("training terminal membership is incomplete")
    _, path = _write_phase(
        context,
        "frozen-cells",
        {
            "status": "complete",
            "training_eligible_attempt_ids_by_fold": eligible,
            "training_terminal_attempt_ids_by_fold": terminal,
            "fold_reports": reports,
        },
    )
    return path


def run_nested_frozen_portfolio_phase(context: NestedPipelineContext) -> Path:
    reused = _reuse_phase_path(context, "frozen-portfolio")
    if reused is not None:
        phase = _load_phase(context, "frozen-portfolio")
        results_path = Path(str(phase.get("portfolio_results_path") or ""))
        if _sha256_file(results_path) != phase.get("portfolio_results_sha256"):
            raise NestedPipelineError("frozen portfolio artifact drift")
        return reused
    cells = _load_phase(context, "frozen-cells")
    reports = list(cells.get("fold_reports") or [])
    results = run_nested_cell_temporal_validation(
        rows=list(context.catalog_rows),
        fold_reports=reports,
        suite=context.suite,
        account=context.account,
        root=context.campaign_root / "portfolio-validation" / "frozen",
        backend=context.optimizer_backend,
        freeze_only=True,
    )
    selected_sets: dict[str, set[str]] = {
        str(fold.get("fold_id") or ""): set() for fold in context.folds
    }
    for result in results:
        fold_id = str((result.get("fold") or {}).get("fold_id") or "")
        if fold_id not in selected_sets:
            raise NestedPipelineError("frozen portfolio returned an unknown fold")
        selected_sets[fold_id].update(
            str(value) for value in result.get("selected_attempt_ids") or []
        )
    selected = {fold_id: sorted(values) for fold_id, values in selected_sets.items()}
    for fold_id, values in selected.items():
        eligible = set((cells.get("training_eligible_attempt_ids_by_fold") or {}).get(fold_id) or [])
        if not set(values).issubset(eligible):
            raise NestedPipelineError("frozen portfolio selected a training-ineligible member")
    results_path = (
        context.campaign_root
        / "portfolio-validation"
        / "frozen"
        / "nested-temporal-results.json"
    )
    _, path = _write_phase(
        context,
        "frozen-portfolio",
        {
            "status": "complete" if any(selected.values()) else "no_consensus",
            "selected_attempt_ids_by_fold": selected,
            "portfolio_results_path": str(results_path),
            "portfolio_results_sha256": _sha256_file(results_path),
        },
    )
    return path


def run_nested_selected_outer_phase(context: NestedPipelineContext) -> Path:
    reused = _reuse_phase_path(context, "selected-outer")
    if reused is not None:
        return reused
    cells = _load_phase(context, "frozen-cells")
    portfolio = _load_phase(context, "frozen-portfolio")
    selected_by_fold = portfolio.get("selected_attempt_ids_by_fold") or {}
    reports = []
    outer_sets: dict[str, list[str]] = {}
    for fold in context.folds:
        fold_id = str(fold.get("fold_id") or "")
        selected = list(selected_by_fold.get(fold_id) or [])
        eligible = set((cells.get("training_eligible_attempt_ids_by_fold") or {}).get(fold_id) or [])
        if not set(selected).issubset(eligible):
            raise NestedPipelineError("outer selection differs from training eligibility")
        report = run_nested_gateway_selected_outer_fold(
            **_fold_kwargs(context, fold), outer_selected_attempt_ids=selected
        )
        observed = sorted(
            str(row.get("attempt_id"))
            for row in report.get("records") or []
            if row.get("outer_validation_status") in {"valid", "nonviable"}
        )
        if observed != sorted(selected):
            raise NestedPipelineError("outer terminal membership differs from frozen selection")
        reports.append(report)
        outer_sets[fold_id] = observed
    _, path = _write_phase(
        context,
        "selected-outer",
        {
            "status": "complete",
            "selected_attempt_ids_by_fold": selected_by_fold,
            "outer_terminal_attempt_ids_by_fold": outer_sets,
            "fold_reports": reports,
        },
    )
    return path


def run_nested_final_report_phase(context: NestedPipelineContext) -> Path:
    reused = _reuse_phase_path(context, "final-report")
    if reused is not None:
        report_path = context.campaign_root / "nested-evidence-report.json"
        phase = _load_phase(context, "final-report")
        if _sha256_file(report_path) != phase.get("report_sha256"):
            raise NestedPipelineError("nested final report artifact drift")
        report = json.loads(report_path.read_text(encoding="utf-8"))
        portfolio_path = Path(str(report.get("portfolio_results_path") or ""))
        if (
            _sha256_file(portfolio_path) != phase.get("portfolio_results_sha256")
            or phase.get("portfolio_results_sha256")
            != report.get("portfolio_results_sha256")
        ):
            raise NestedPipelineError("nested final portfolio artifact drift")
        return reused
    outer = _load_phase(context, "selected-outer")
    portfolio = _load_phase(context, "frozen-portfolio")
    reports = list(outer.get("fold_reports") or [])
    results = run_nested_cell_temporal_validation(
        rows=list(context.catalog_rows),
        fold_reports=reports,
        suite=context.suite,
        account=context.account,
        root=context.campaign_root / "portfolio-validation" / "final",
        backend=context.optimizer_backend,
    )
    payload = {
        **context.preview,
        "status": "complete",
        "fold_results": reports,
        "portfolio_result_count": len(results),
        "portfolio_results_path": str(
            context.campaign_root
            / "portfolio-validation"
            / "final"
            / "nested-temporal-results.json"
        ),
        "portfolio_results_sha256": _sha256_file(
            context.campaign_root
            / "portfolio-validation"
            / "final"
            / "nested-temporal-results.json"
        ),
        "membership": {
            "requested_attempt_ids": list(context.requested_attempt_ids),
            "training_eligible_attempt_ids_by_fold": _load_phase(context, "frozen-cells").get(
                "training_eligible_attempt_ids_by_fold"
            ),
            "training_terminal_attempt_ids_by_fold": _load_phase(context, "frozen-cells").get(
                "training_terminal_attempt_ids_by_fold"
            ),
            "selected_attempt_ids_by_fold": portfolio.get("selected_attempt_ids_by_fold"),
            "outer_terminal_attempt_ids_by_fold": outer.get("outer_terminal_attempt_ids_by_fold"),
        },
    }
    report_path = context.campaign_root / "nested-evidence-report.json"
    _create_or_verify(report_path, payload)
    _, phase_path = _write_phase(
        context,
        "final-report",
        {
            "status": "complete" if results else "no_champion",
            "report_path": str(report_path),
            "report_sha256": _sha256_file(report_path),
            "portfolio_results_sha256": payload["portfolio_results_sha256"],
        },
    )
    return phase_path


def run_nested_pipeline(context: NestedPipelineContext) -> dict[str, Any]:
    run_nested_training_phase(context)
    run_nested_frozen_cells_phase(context)
    run_nested_frozen_portfolio_phase(context)
    run_nested_selected_outer_phase(context)
    run_nested_final_report_phase(context)
    return json.loads(
        (context.campaign_root / "nested-evidence-report.json").read_text(encoding="utf-8")
    )
