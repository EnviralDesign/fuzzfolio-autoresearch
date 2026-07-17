"""Operator-facing orchestration for one frozen Level C generation.

This module coordinates existing Atlas, PlayHand, cohort, nested-evidence, and
portfolio APIs. It deliberately contains no research or replay implementation.
"""

from __future__ import annotations

import hashlib
import argparse
import json
import os
import tempfile
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .atlas_lab import AtlasLabRuntimeConfig, run_atlas_lab
from .config import AppConfig, load_config
from .evidence_plan import canonical_json, canonical_sha256
from .generation_archive import (
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
)
from .level_c_protocol import (
    LEVEL_C_PROTOCOL_SCHEMA,
    build_initial_four_cutoff_plans,
    create_level_c_protocol,
    create_level_c_protocol_authority,
    load_level_c_protocol,
    load_level_c_protocol_authority,
)
from .play_hand_lab import PlayHandLabRuntimeConfig, cmd_play_hand_lab
from .runtime_policy_lock import build_runtime_policy_lock, policy_lock_provenance


BOOTSTRAP_RECEIPT_SCHEMA = "autoresearch-level-c-bootstrap-receipt-v1"
STAGE_RECEIPT_SCHEMA = "autoresearch-level-c-stage-receipt-v1"
DEVELOPMENT_POLICY_SCHEMA = "autoresearch-level-c-development-policy-v1"
AUDIT_SCHEMA = "autoresearch-level-c-audit-v1"
CONTROL_RELATIVE = Path("derived") / "level-c" / "control"
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


def _assert_bootstrap_partial_order(active_root: Path) -> None:
    paths = _control_paths(active_root)
    ordered = [
        paths["archive_receipt"],
        active_root / GENERATION_MANIFEST_NAME,
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


def _relative_archived_file(path: Path, archive_root: Path, *, label: str) -> str:
    resolved = path.expanduser().resolve(strict=True)
    try:
        relative = resolved.relative_to(archive_root)
    except ValueError as exc:
        raise LevelCWorkflowError(f"{label} must be inside the archived runs root") from exc
    if resolved.is_symlink() or not resolved.is_file():
        raise LevelCWorkflowError(f"{label} must be a regular archived file")
    return relative.as_posix()


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
    _assert_bootstrap_partial_order(active_root)
    paths = _control_paths(active_root)
    named_files = {
        "archived_attempt_catalog": (archived_attempt_catalog, archived_attempt_catalog_sha256),
        "legacy_controls": (legacy_controls, legacy_controls_sha256),
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
    nested_report = _load_json(completed_nested_report, label="completed nested report")
    if str(nested_report.get("status") or "").lower() != "complete":
        raise LevelCWorkflowError("archived nested report is not complete")
    cutoff_plans = build_initial_four_cutoff_plans(
        completed_nested_report, global_seed=int(global_seed)
    )
    prepared_at = str(
        nested_report.get("completed_at")
        or nested_report.get("generated_at")
        or nested_report.get("created_at")
        or ""
    ).strip()
    if not prepared_at:
        raise LevelCWorkflowError("completed nested report has no stable completion timestamp")
    archive_identity = {
        "schema_version": BOOTSTRAP_RECEIPT_SCHEMA,
        "archive_id": str(archive_id),
        "archived_runs_root": str(archive),
        "archive_prepared_at": prepared_at,
        "verified_artifacts": verified,
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
    generation_path = active_root / GENERATION_MANIFEST_NAME
    _create_or_verify(generation_path, generation)
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


def _month_span(start: str, end: str) -> int:
    left = datetime.fromisoformat(start.replace("Z", "+00:00"))
    right = datetime.fromisoformat(end.replace("Z", "+00:00"))
    return max(1, (right.year - left.year) * 12 + right.month - left.month)


def _validate_nested_report(report_path: Path, *, selected_only: bool) -> dict[str, Any]:
    report = _load_json(report_path, label="nested evidence report")
    if report.get("status") != "complete":
        raise LevelCWorkflowError("nested evidence report is not complete")
    portfolio_results: list[dict[str, Any]] = []
    portfolio_path_value = str(report.get("portfolio_results_path") or "").strip()
    if portfolio_path_value:
        portfolio_payload = json.loads(Path(portfolio_path_value).read_text(encoding="utf-8"))
        if not isinstance(portfolio_payload, list) or not all(
            isinstance(row, dict) for row in portfolio_payload
        ):
            raise LevelCWorkflowError("nested portfolio results are malformed")
        portfolio_results = portfolio_payload
    for fold_result in report.get("fold_results") or []:
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
        return "complete", [result.summary_path]
    if stage == "playhand":
        arguments, _ = executor_arguments_from_plan(plan_path, executor="playhand", config=config)
        runtime = _runtime_from_plan(
            PlayHandLabRuntimeConfig,
            arguments,
            execution_plan_path=plan_path,
            gateway_url=gateway_url,
            gateway_token=gateway_token,
            active_runs=playhand_active_runs,
            trading_dashboard_root=trading_dashboard_root,
            resume=resume,
        )
        if cmd_play_hand_lab(runtime) != 0:
            raise LevelCWorkflowError("finite PlayHand coordinator failed")
        campaign_root = Path(expected["playhand_campaign"]["resolved_path"])
        summary = campaign_root / "play-hand-lab-summary.json"
        if not summary.is_file():
            alternatives = list(campaign_root.glob("*summary*.json"))
            if len(alternatives) != 1:
                raise LevelCWorkflowError("finite PlayHand summary is missing or ambiguous")
            summary = alternatives[0]
        return "complete", [summary]
    if stage == "frozen_cohort":
        cohort_path = Path(expected["frozen_cohort"]["resolved_path"])
        if cohort_path.exists():
            cohort = validate_level_c_cohort(cohort_path)
        else:
            cohort = freeze_level_c_cohort(
                runs_root=active_root,
                atlas_run_root=Path(expected["atlas_run"]["resolved_path"]),
                playhand_campaign_id=plan["cutoff"]["playhand_campaign_id"],
                as_of_date=plan["cutoff"]["geometry"]["selection_end"],
                lake_manifest_sha256=plan["atlas_arguments"]["lake_manifest_sha256"],
                output_path=cohort_path,
                cohort_id=plan["cutoff"]["cohort_id"],
            )
        outcome = str(cohort.get("outcome") or "")
        return outcome, [cohort_path]

    cohort_path = Path(expected["frozen_cohort"]["resolved_path"])
    cohort = validate_level_c_cohort(cohort_path)
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
    if stage == "training_evidence" and not report_path.exists():
        from .__main__ import cmd_nested_evidence

        geometry = plan["cutoff"]["geometry"]
        exit_code = cmd_nested_evidence(
            campaign_id=nested_campaign_id,
            suite_name=str(workflow["suite_name"]),
            suite_config_path=None,
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
            lake_url=None,
            lake_token=None,
            lake_manifest_sha256=plan["atlas_arguments"]["lake_manifest_sha256"],
            trading_dashboard_root=trading_dashboard_root,
            optimizer_backend=str(workflow["optimizer_backend"]),
            dry_run=False,
            as_json=True,
            attempt_cohort=cohort_path,
        )
        if exit_code != 0:
            raise LevelCWorkflowError("nested evidence pipeline failed")
    report = _validate_nested_report(report_path, selected_only=stage in {"selected_outer", "final_report"})
    if stage == "frozen_portfolio":
        if int(report.get("portfolio_result_count") or 0) == 0:
            return "no_consensus", [report_path]
        portfolio_path = Path(str(report.get("portfolio_results_path") or ""))
        if not portfolio_path.is_file():
            raise LevelCWorkflowError("train-only frozen portfolio artifact is missing")
        return "complete", [report_path, portfolio_path]
    return "complete", [report_path]


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
        source = archive_root / str(artifact.get("relative_path") or "")
        if _file_sha256(source) != artifact.get("sha256"):
            raise LevelCWorkflowError(f"archived bootstrap artifact drift: {source}")
    generation_path = active_root / GENERATION_MANIFEST_NAME
    if _file_sha256(generation_path) != bootstrap.get("generation_manifest_sha256"):
        raise LevelCWorkflowError("generation manifest drift")
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
            stages.append(stage)
            terminal = receipt.get("outcome")
        if "frozen_cohort" in stages:
            cohort_path = Path(plan["expected_artifacts"]["frozen_cohort"]["resolved_path"])
            cohort = validate_level_c_cohort(cohort_path)
            if int(cohort.get("candidate_count") or 0) != len(cohort.get("candidates") or []):
                raise LevelCWorkflowError("frozen cohort accounting mismatch")
        if "selected_outer" in stages:
            nested_id = f"{plan['cutoff']['cohort_id']}-nested"
            _validate_nested_report(
                active_root / "derived" / "nested-evidence" / nested_id / "nested-evidence-report.json",
                selected_only=True,
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
