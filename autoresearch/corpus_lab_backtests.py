from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .dashboard import (
    FULL_BACKTEST_CALENDAR_CURVE_FILENAME,
    FULL_BACKTEST_CURVE_FILENAME,
    FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
    FULL_BACKTEST_RESULT_FILENAME,
    _reward_matrix_from_attempt,
)
from .corpus_tools import (
    FULL_BACKTEST_MANIFEST_FILENAME,
    build_full_backtest_provenance,
    canonical_strategy_definition,
    canonical_strategy_fingerprint,
)
from .execution_costs import execution_cost_manifest_payload
from .evidence_plan import (
    ReplayEvidencePlan,
    build_execution_cell_sha256,
    build_replay_evidence_plan,
    subtract_calendar_months,
    validate_replay_evidence_plan,
)
from .nested_evidence import FrozenExecutionCellReceipt
from .evidence_artifacts import (
    build_evidence_artifact_manifest,
    evidence_bundle_lock,
    evidence_artifact_paths,
    validate_evidence_artifact_bundle,
    write_immutable_json,
)
from .play_hand_lab import DEFAULT_LAB_GATEWAY_URL, LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token


FULL_BACKTEST_CACHE_TASK_KIND = "full_backtest_cache"
TERMINAL_OUTCOME_SCHEMA = "autoresearch-evidence-terminal-outcome-v1"
DEFAULT_RESULT_BATCH_SIZE = 25


@dataclass(frozen=True)
class LabBacktestConfig:
    gateway_url: str = DEFAULT_LAB_GATEWAY_URL
    gateway_token: str | None = None
    worker_contract_hash: str | None = None
    worker_contract_schema: str = "replay-worker-contract-v1"
    deadline_seconds: float = 3600.0
    poll_interval_seconds: float = 2.0
    result_batch_size: int = DEFAULT_RESULT_BATCH_SIZE


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _reuse_valid_evidence_bundle(
    artifact_dir: Path,
    plan: ReplayEvidencePlan,
    *,
    expected_outcome: str,
) -> dict[str, Any] | None:
    validation = validate_evidence_artifact_bundle(artifact_dir, plan)
    if validation.get("status") != "valid":
        return None
    terminal_outcome = validation.get("terminal_outcome")
    actual_outcome = (
        str(terminal_outcome.get("outcome"))
        if isinstance(terminal_outcome, dict)
        else "success"
    )
    if actual_outcome != expected_outcome:
        raise RuntimeError(
            "Existing immutable evidence outcome conflict: "
            f"expected={expected_outcome} observed={actual_outcome}"
        )
    paths = evidence_artifact_paths(artifact_dir, plan)
    payload = {
        "result_path": str(paths.result),
        "curve_path": str(paths.curve),
        "evidence_manifest_path": str(paths.manifest),
        "evidence_plan_id": plan.plan_id,
        "evidence_role": plan.evidence_role,
        "backend": "lab_gateway",
        "reused_existing_evidence": True,
    }
    if isinstance(terminal_outcome, dict):
        payload["terminal_outcome"] = dict(terminal_outcome)
    if plan.evidence_role == "outer_test":
        payload["cell_receipt_path"] = str(paths.cell_receipt)
    else:
        payload["calendar_curve_path"] = str(paths.calendar_curve)
        payload["recommended_curve_path"] = str(paths.recommended_curve)
    return payload


def _prepare_evidence_bundle_slot(
    artifact_dir: Path,
    plan: ReplayEvidencePlan,
    *,
    expected_outcome: str,
) -> dict[str, Any] | None:
    reused = _reuse_valid_evidence_bundle(
        artifact_dir,
        plan,
        expected_outcome=expected_outcome,
    )
    if reused is not None:
        return reused
    paths = evidence_artifact_paths(artifact_dir, plan)
    if paths.manifest.exists():
        validation = validate_evidence_artifact_bundle(artifact_dir, plan)
        raise RuntimeError(
            "Immutable evidence bundle has an invalid published manifest: "
            + ",".join(validation.get("reason_codes") or ["unknown"])
        )
    if paths.root.exists():
        shutil.rmtree(paths.root)
    return None


def materialize_no_valid_cell_lab_result(
    *,
    attempt: dict[str, Any],
    lab_result: dict[str, Any],
    task: dict[str, Any],
) -> dict[str, Any]:
    worker_result = _unwrap_worker_result(lab_result)
    terminal_result = worker_result.get("terminal_result")
    if (
        not isinstance(terminal_result, dict)
        or terminal_result.get("schema") != "fuzzfolio-replay-terminal-result-v1"
        or terminal_result.get("status") != "nonviable"
        or terminal_result.get("outcome") != "no_valid_cell"
    ):
        raise RuntimeError("Lab result is missing a worker-authored no-valid-cell outcome")
    diagnostics = terminal_result.get("diagnostics")
    if not isinstance(diagnostics, dict):
        raise RuntimeError("Worker-authored no-valid-cell outcome is missing diagnostics")
    market_window = diagnostics.get("market_data_window")
    if (
        diagnostics.get("signal_count") != 0
        or diagnostics.get("resolved_trade_count_max") != 0
        or not isinstance(market_window, dict)
        or int(market_window.get("filtered_bar_count") or 0) <= 0
    ):
        raise RuntimeError(
            "No-valid-cell outcome is missing zero-signal diagnostics or loaded bars"
        )
    task_payload = task.get("payload") if isinstance(task, dict) else None
    if not isinstance(task_payload, dict):
        raise RuntimeError("No-valid-cell task is missing its payload")
    plan = validate_replay_evidence_plan(task_payload.get("evidence_plan"))
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    execution_evidence = _validate_planned_execution_evidence(
        plan=plan,
        execution_evidence=terminal_result.get("execution_evidence"),
    )
    terminal_outcome = {"status": "nonviable", "outcome": "no_valid_cell"}
    result_payload = {
        "schema": TERMINAL_OUTCOME_SCHEMA,
        **terminal_outcome,
        "evidence_plan_id": plan.plan_id,
        "diagnostics": diagnostics,
    }
    request_record = {
        "request": task_payload,
        "status": "nonviable",
        "job_kind": FULL_BACKTEST_CACHE_TASK_KIND,
        "worker_id": lab_result.get("worker_id"),
    }
    provenance = {
        "schema": TERMINAL_OUTCOME_SCHEMA,
        "attempt_id": str(attempt.get("attempt_id") or ""),
        "run_id": str(attempt.get("run_id") or ""),
        "worker_id": lab_result.get("worker_id"),
        "worker_contract_hash": task_payload.get("required_worker_contract_hash"),
    }
    manifest = build_evidence_artifact_manifest(
        evidence_plan=plan,
        provenance=provenance,
        execution_evidence=execution_evidence,
        artifact_payloads={"result": result_payload, "job": request_record},
    )
    manifest["terminal_outcome"] = terminal_outcome
    paths = evidence_artifact_paths(artifact_dir, plan)
    with evidence_bundle_lock(paths.root):
        reused = _prepare_evidence_bundle_slot(
            artifact_dir,
            plan,
            expected_outcome="no_valid_cell",
        )
        if reused is not None:
            return reused
        write_immutable_json(paths.result, result_payload)
        write_immutable_json(paths.job, request_record)
        write_immutable_json(paths.manifest, manifest)
    return {
        "result_path": str(paths.result),
        "evidence_manifest_path": str(paths.manifest),
        "evidence_plan_id": plan.plan_id,
        "evidence_role": plan.evidence_role,
        "terminal_outcome": terminal_outcome,
        "backend": "lab_gateway",
    }


def _profile_snapshot_from_file(profile_path: Path | None) -> dict[str, Any]:
    if profile_path is None or not profile_path.exists():
        raise RuntimeError(f"Attempt is missing a local profile file: {profile_path}")
    payload = _load_json(profile_path)
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else None
    if isinstance(profile, dict):
        return dict(profile)
    profile_document = payload.get("profile_document")
    if isinstance(profile_document, dict) and isinstance(profile_document.get("profile"), dict):
        return dict(profile_document["profile"])
    if payload:
        return payload
    raise RuntimeError(f"Profile file is empty or invalid: {profile_path}")


def _request_payload_from_attempt(attempt: dict[str, Any]) -> dict[str, Any]:
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    payload = _load_json(artifact_dir / "deep-replay-job.json")
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    return dict(request) if isinstance(request, dict) else {}


def _option_payload(config: Any, request_payload: dict[str, Any]) -> dict[str, Any]:
    request_options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
    cost_payload = execution_cost_manifest_payload(config).get("execution_cost_model")
    if not isinstance(cost_payload, dict):
        cost_payload = request_options.get("cost_model") if isinstance(request_options.get("cost_model"), dict) else {}
    quality_score_preset = str(getattr(config.research, "quality_score_preset", "default") or "default").replace("-", "_")
    if quality_score_preset not in {"default", "profile_drop"}:
        quality_score_preset = "default"
    return {
        "include_entries": False,
        "include_per_instrument": bool(request_options.get("include_per_instrument", True)),
        "include_aggregate_matrix": True,
        "path_metrics_mode": str(request_options.get("path_metrics_mode") or "highlighted"),
        "quality_score_preset": quality_score_preset,
        "cost_model": cost_payload or {"mode": "research_conservative"},
    }


def _matrix_payload(attempt: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
    request_matrix = request_payload.get("matrix")
    matrix = dict(request_matrix) if isinstance(request_matrix, dict) else {}
    reward_matrix = _reward_matrix_from_attempt(attempt)
    if reward_matrix:
        matrix["reward_step_r"] = float(reward_matrix["reward_step_r"])
        matrix["reward_columns"] = int(reward_matrix["reward_columns"])
        matrix.setdefault("reward_start_r", matrix["reward_step_r"])
    matrix.setdefault("sl_step_percent", 0.02)
    matrix.setdefault("sl_rows", 25)
    matrix.setdefault("reward_step_r", 0.5)
    matrix.setdefault("reward_columns", 8)
    return matrix


def build_full_backtest_lab_task(
    *,
    config: Any,
    run_dir: Path,
    attempt: dict[str, Any],
    run_metadata: dict[str, Any] | None,
    lab_config: LabBacktestConfig,
    batch_id: str | None = None,
    evidence_window_end: str | None = None,
    evidence_window_start: str | None = None,
    requested_horizon_months: int = 36,
    evidence_role: str = "full_backtest",
    selection_data_end: str | None = None,
    campaign_plan_id: str | None = None,
    lake_manifest_sha256: str | None = None,
    evidence_plan: ReplayEvidencePlan | dict[str, Any] | None = None,
    tracked_cell: dict[str, float] | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    attempt_id = str(attempt.get("attempt_id") or "")
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    if not artifact_dir.exists():
        raise RuntimeError(f"Artifact directory does not exist: {artifact_dir}")
    profile_path_raw = str(attempt.get("profile_path") or "").strip()
    profile_path = Path(profile_path_raw).resolve() if profile_path_raw else None
    profile_snapshot = _profile_snapshot_from_file(profile_path)
    profile_threshold = profile_snapshot.get("notificationThreshold")
    try:
        effective_alert_threshold = float(
            80.0 if profile_threshold is None else profile_threshold
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Profile has invalid notificationThreshold: {profile_threshold!r}"
        ) from exc
    profile_snapshot["notificationThreshold"] = effective_alert_threshold
    request_payload = _request_payload_from_attempt(attempt)
    instruments = profile_snapshot.get("instruments")
    if not isinstance(instruments, list) or not instruments:
        instruments = request_payload.get("instruments")
    normalized_instruments = [
        str(item).strip().upper()
        for item in (instruments if isinstance(instruments, list) else [])
        if str(item).strip()
    ]
    if not normalized_instruments:
        raise RuntimeError(f"Could not resolve instruments for attempt {attempt_id}")
    timeframe = str(
        request_payload.get("timeframe")
        or attempt.get("requested_timeframe")
        or attempt.get("effective_timeframe")
        or ""
    ).strip().upper()
    if not timeframe:
        raise RuntimeError(f"Could not resolve timeframe for attempt {attempt_id}")

    if evidence_plan is None:
        window_end = evidence_window_end or _now_iso()
        window_start = evidence_window_start or subtract_calendar_months(
            window_end, requested_horizon_months
        )
        execution_cell_sha256 = (
            build_execution_cell_sha256(tracked_cell)
            if isinstance(tracked_cell, dict)
            else None
        )
        resolved_evidence_plan = build_replay_evidence_plan(
            campaign_plan_id=(
                campaign_plan_id
                if campaign_plan_id is not None
                else (f"corpus-full-backtest:{batch_id}" if batch_id else None)
            ),
            evidence_role=evidence_role,
            selection_data_end=selection_data_end or window_end,
            analysis_window_start=window_start,
            analysis_window_end=window_end,
            requested_horizon_months=requested_horizon_months,
            profile_snapshot=profile_snapshot,
            execution_cell_sha256=execution_cell_sha256,
            lake_manifest_sha256=lake_manifest_sha256,
        )
    else:
        resolved_evidence_plan = validate_replay_evidence_plan(evidence_plan)
    if resolved_evidence_plan.execution_cell_sha256 is not None:
        if not isinstance(tracked_cell, dict):
            raise RuntimeError("Evidence plan requires a frozen tracked cell")
        if (
            build_execution_cell_sha256(tracked_cell)
            != resolved_evidence_plan.execution_cell_sha256
        ):
            raise RuntimeError("Tracked cell does not match evidence plan")
    elif tracked_cell is not None:
        raise RuntimeError("Tracked cell requires an execution-cell-bound evidence plan")

    run_id = run_dir.name
    resolved_task_id = str(task_id or "").strip() or f"full-backtest-cache-{attempt_id}"
    if batch_id and task_id is None:
        resolved_task_id = f"{resolved_task_id}-{batch_id}"
    payload = {
        "job_id": resolved_task_id,
        "user_id": "autoresearch-corpus",
        "profile_id": str(attempt.get("profile_ref") or request_payload.get("profile_id") or f"local:{attempt_id}"),
        "inline_profile_snapshot": profile_snapshot,
        "artifact_persistence": "ephemeral",
        "source_kind": "workspace_attempt",
        "client_origin": "autoresearch_corpus_lab_cache_v1",
        "retention_behavior": "ephemeral",
        "retention_reason": "autoresearch",
        "workspace_id": run_id,
        "workspace_attempt_id": attempt_id,
        "instruments": normalized_instruments,
        "timeframe": timeframe,
        "market_data_source": str(request_payload.get("market_data_source") or "lake_bars"),
        "lookback_months": None,
        "analysis_window_start": resolved_evidence_plan.analysis_window_start,
        "analysis_window_end": resolved_evidence_plan.analysis_window_end,
        "evidence_plan": resolved_evidence_plan.model_dump(mode="json"),
        "tracked_cell": tracked_cell,
        "bar_limit": int(request_payload.get("bar_limit") or 5000),
        "alert_threshold": effective_alert_threshold,
        "view_mode": str(request_payload.get("view_mode") or "overview"),
        "direction_mode": str(
            profile_snapshot.get("directionMode")
            or request_payload.get("direction_mode")
            or "both"
        ),
        "matrix": _matrix_payload(attempt, request_payload),
        "options": _option_payload(config, request_payload),
        "requested_at": _now_iso(),
        "priority": "research",
        "work_class": "research_replay",
        "required_worker_contract_hash": lab_config.worker_contract_hash,
        "required_worker_contract_schema": lab_config.worker_contract_schema,
        "required_capabilities": ["deep_replay", FULL_BACKTEST_CACHE_TASK_KIND],
    }
    payload = {key: value for key, value in payload.items() if value is not None}
    return {
        "task_id": resolved_task_id,
        "lane_id": "corpus-full-backtest",
        "attempt_id": attempt_id,
        "task_kind": FULL_BACKTEST_CACHE_TASK_KIND,
        "payload": payload,
        "required_worker_capabilities": [FULL_BACKTEST_CACHE_TASK_KIND],
        "deadline_seconds": float(lab_config.deadline_seconds),
        "max_attempts": 3,
        "metadata": {
            "run_id": run_id,
            "attempt_id": attempt_id,
            "artifact_dir": str(artifact_dir),
            "run_metadata_runner": (run_metadata or {}).get("runner"),
            "source_profile_path": str(profile_path) if profile_path else None,
            "historical_request_alert_threshold": request_payload.get(
                "alert_threshold"
            ),
            "effective_alert_threshold": effective_alert_threshold,
            "evidence_plan_id": resolved_evidence_plan.plan_id,
            "evidence_role": resolved_evidence_plan.evidence_role,
            "canonical_profile_fingerprint": canonical_strategy_fingerprint(
                canonical_strategy_definition(
                    profile_snapshot,
                    instruments=normalized_instruments,
                    timeframe=timeframe,
                    direction_mode=str(
                        profile_snapshot.get("directionMode")
                        or request_payload.get("direction_mode")
                        or "both"
                    ),
                    alert_threshold=effective_alert_threshold,
                )
            ),
        },
    }


def _unwrap_worker_result(lab_result: dict[str, Any]) -> dict[str, Any]:
    payload = lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    nested = payload.get("result") if isinstance(payload.get("result"), dict) else None
    if isinstance(nested, dict) and (
        "sensitivity_response" in nested
        or "best_cell_detail" in nested
        or "calendar_curve" in nested
    ):
        return {
            **nested,
            **(
                {"execution_evidence": payload["execution_evidence"]}
                if isinstance(payload.get("execution_evidence"), dict)
                else {}
            ),
            **(
                {"completed_at": payload["completed_at"]}
                if payload.get("completed_at") is not None
                else {}
            ),
        }
    return payload


def _validate_planned_execution_evidence(
    *,
    plan: ReplayEvidencePlan,
    execution_evidence: Any,
) -> dict[str, Any]:
    if not isinstance(execution_evidence, dict):
        raise RuntimeError("Planned worker result is missing execution evidence")
    expected = {
        "plan_id": plan.plan_id,
        "profile_snapshot_sha256": plan.profile_snapshot_sha256,
        "execution_cell_sha256": plan.execution_cell_sha256,
    }
    for key, value in expected.items():
        if execution_evidence.get(key) != value:
            raise RuntimeError(f"Planned execution evidence {key} mismatch")
    if (
        plan.lake_manifest_sha256 is not None
        and execution_evidence.get("observed_lake_manifest_sha256")
        != plan.lake_manifest_sha256
    ):
        raise RuntimeError("Planned execution evidence lake coverage hash mismatch")
    return execution_evidence


def materialize_full_backtest_lab_result(
    *,
    attempt: dict[str, Any],
    lab_result: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    worker_result = _unwrap_worker_result(lab_result)
    sensitivity_response = worker_result.get("sensitivity_response")
    if not isinstance(sensitivity_response, dict):
        replay_result = worker_result.get("deep_replay_result")
        if not isinstance(replay_result, dict):
            raise RuntimeError("Lab full-backtest result did not include sensitivity_response")
        sensitivity_response = {
            "status": "success",
            "message": "Full backtest completed via lab worker gateway.",
            "data": replay_result,
        }
    best_detail = worker_result.get("best_cell_detail")
    if not isinstance(best_detail, dict):
        raise RuntimeError("Lab full-backtest result did not include best_cell_detail")
    calendar_curve = worker_result.get("calendar_curve")
    if not isinstance(calendar_curve, dict):
        calendar_curve = best_detail
    recommended_detail = worker_result.get("recommended_cell_detail")
    if not isinstance(recommended_detail, dict):
        raise RuntimeError("Lab full-backtest result did not include recommended_cell_detail")
    request_payload = worker_result.get("request") if isinstance(worker_result.get("request"), dict) else {}

    task_payload = task.get("payload") if isinstance(task, dict) else None
    task_payload = task_payload if isinstance(task_payload, dict) else request_payload
    profile_snapshot = task_payload.get("inline_profile_snapshot")
    profile_snapshot = profile_snapshot if isinstance(profile_snapshot, dict) else {}
    source_profile_path_raw = (
        (task.get("metadata") or {}).get("source_profile_path")
        if isinstance(task, dict) and isinstance(task.get("metadata"), dict)
        else None
    )
    provenance = build_full_backtest_provenance(
        attempt=attempt,
        profile_snapshot=profile_snapshot,
        request_payload=task_payload,
        result_payload=sensitivity_response,
        source_profile_path=Path(source_profile_path_raw)
        if source_profile_path_raw
        else None,
        worker_id=str(lab_result.get("worker_id") or "") or None,
        worker_pool=str(
            worker_result.get("worker_pool") or lab_result.get("worker_pool") or ""
        )
        or None,
    )
    aggregate = (
        sensitivity_response.get("data", {}).get("aggregate")
        if isinstance(sensitivity_response.get("data"), dict)
        else None
    )
    if isinstance(aggregate, dict):
        provenance["market_data_window"] = aggregate.get("market_data_window")
        aggregate["autoresearch_provenance"] = provenance
    execution_evidence = worker_result.get("execution_evidence")
    if isinstance(execution_evidence, dict):
        provenance["execution_evidence"] = execution_evidence

    evidence_plan_payload = task_payload.get("evidence_plan")
    resolved_evidence_plan = (
        validate_replay_evidence_plan(evidence_plan_payload)
        if isinstance(evidence_plan_payload, dict)
        else None
    )
    if (
        resolved_evidence_plan is not None
        and resolved_evidence_plan.evidence_role == "outer_test"
    ):
        raise RuntimeError(
            "Outer-test evidence must use materialize_outer_test_lab_result so "
            "best-cell matrix outputs cannot enter OOS artifacts."
        )
    if resolved_evidence_plan is not None:
        reused = _reuse_valid_evidence_bundle(
            artifact_dir,
            resolved_evidence_plan,
            expected_outcome="success",
        )
        if reused is not None:
            return reused
    if resolved_evidence_plan is not None:
        execution_evidence = _validate_planned_execution_evidence(
            plan=resolved_evidence_plan,
            execution_evidence=execution_evidence,
        )
        provenance["execution_evidence"] = execution_evidence
    request_record = {
        "request": request_payload,
        "status": lab_result.get("status") or "success",
        "job_kind": FULL_BACKTEST_CACHE_TASK_KIND,
        "worker_id": lab_result.get("worker_id"),
        "worker_pool": worker_result.get("worker_pool") or lab_result.get("worker_pool"),
        "completed_at": worker_result.get("completed_at"),
    }
    immutable_paths = None
    if resolved_evidence_plan is not None:
        immutable_paths = evidence_artifact_paths(artifact_dir, resolved_evidence_plan)
        immutable_manifest = build_evidence_artifact_manifest(
            evidence_plan=resolved_evidence_plan,
            provenance=provenance,
            execution_evidence=(
                execution_evidence if isinstance(execution_evidence, dict) else None
            ),
            artifact_payloads={
                "result": sensitivity_response,
                "curve": best_detail,
                "calendar_curve": calendar_curve,
                "recommended_curve": recommended_detail,
                "job": request_record,
            },
        )
        with evidence_bundle_lock(immutable_paths.root):
            reused = _prepare_evidence_bundle_slot(
                artifact_dir,
                resolved_evidence_plan,
                expected_outcome="success",
            )
            if reused is not None:
                return reused
            write_immutable_json(immutable_paths.result, sensitivity_response)
            write_immutable_json(immutable_paths.curve, best_detail)
            write_immutable_json(immutable_paths.calendar_curve, calendar_curve)
            write_immutable_json(immutable_paths.recommended_curve, recommended_detail)
            write_immutable_json(immutable_paths.job, request_record)
            write_immutable_json(immutable_paths.manifest, immutable_manifest)

    result_path = artifact_dir / FULL_BACKTEST_RESULT_FILENAME
    curve_path = artifact_dir / FULL_BACKTEST_CURVE_FILENAME
    calendar_path = artifact_dir / FULL_BACKTEST_CALENDAR_CURVE_FILENAME
    write_legacy_aliases = (
        resolved_evidence_plan is None
        or (
            resolved_evidence_plan.requested_horizon_months == 36
            and resolved_evidence_plan.evidence_role == "full_backtest"
        )
    )
    if write_legacy_aliases:
        _write_json(result_path, sensitivity_response)
        _write_json(curve_path, best_detail)
        _write_json(calendar_path, calendar_curve)
        _write_json(
            artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
            recommended_detail,
        )
        _write_json(artifact_dir / FULL_BACKTEST_MANIFEST_FILENAME, provenance)
    if write_legacy_aliases and isinstance(request_payload, dict) and request_payload:
        _write_json(
            artifact_dir / "full-backtest-36mo-deep-replay-job.json",
            request_record,
        )
        original_job_path = artifact_dir / "deep-replay-job.json"
        if not original_job_path.exists():
            _write_json(original_job_path, request_record)
    return {
        "curve_path": str(immutable_paths.curve if immutable_paths else curve_path),
        "result_path": str(immutable_paths.result if immutable_paths else result_path),
        "calendar_curve_path": str(
            immutable_paths.calendar_curve if immutable_paths else calendar_path
        ),
        "recommended_curve_path": str(
            immutable_paths.recommended_curve
            if immutable_paths
            else artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME
        ),
        "evidence_manifest_path": (
            str(immutable_paths.manifest) if immutable_paths else None
        ),
        "evidence_plan_id": (
            resolved_evidence_plan.plan_id if resolved_evidence_plan else None
        ),
        "backend": "lab_gateway",
    }


def materialize_outer_test_lab_result(
    *,
    attempt: dict[str, Any],
    lab_result: dict[str, Any],
    task: dict[str, Any],
    cell_receipt: FrozenExecutionCellReceipt | dict[str, Any],
) -> dict[str, Any]:
    if str(lab_result.get("task_id") or "") != str(task.get("task_id") or ""):
        raise RuntimeError("Outer-test gateway result task ID mismatch")
    worker_envelope = (
        lab_result.get("result") if isinstance(lab_result.get("result"), dict) else {}
    )
    if str(worker_envelope.get("job_kind") or "") != FULL_BACKTEST_CACHE_TASK_KIND:
        raise RuntimeError("Outer-test gateway result has the wrong job kind")
    task_payload = task.get("payload") if isinstance(task, dict) else None
    if not isinstance(task_payload, dict):
        raise RuntimeError("Outer-test task is missing its payload")
    plan = validate_replay_evidence_plan(task_payload.get("evidence_plan"))
    if plan.evidence_role != "outer_test":
        raise RuntimeError("Dedicated outer materializer requires outer_test evidence")
    receipt = (
        cell_receipt
        if isinstance(cell_receipt, FrozenExecutionCellReceipt)
        else FrozenExecutionCellReceipt.model_validate(cell_receipt)
    )
    if receipt.execution_cell_sha256 != plan.execution_cell_sha256:
        raise RuntimeError("Outer-test plan is not bound to the supplied cell receipt")
    if task_payload.get("tracked_cell") != receipt.execution_cell:
        raise RuntimeError("Outer-test task tracked cell differs from the frozen receipt")

    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    reused = _reuse_valid_evidence_bundle(
        artifact_dir,
        plan,
        expected_outcome="success",
    )
    if reused is not None:
        return reused

    worker_result = _unwrap_worker_result(lab_result)
    sensitivity_response = worker_result.get("sensitivity_response")
    data = sensitivity_response.get("data") if isinstance(sensitivity_response, dict) else None
    aggregate = data.get("aggregate") if isinstance(data, dict) else None
    tracked_result = (
        aggregate.get("tracked_cell_result") if isinstance(aggregate, dict) else None
    )
    tracked_detail = worker_result.get("tracked_cell_detail")
    if not isinstance(tracked_result, dict):
        raise RuntimeError("Outer-test replay did not include tracked_cell_result")
    if not isinstance(tracked_detail, dict):
        raise RuntimeError("Outer-test replay did not include tracked_cell_detail")
    detail_cell = tracked_detail.get("cell")
    if not isinstance(detail_cell, dict) or any(
        abs(float(detail_cell.get(key) or 0.0) - float(receipt.execution_cell[key]))
        > 1e-12
        for key in ("stop_loss_percent", "reward_multiple")
    ):
        raise RuntimeError("Outer-test tracked detail does not match the frozen cell")

    allowed_aggregate_keys = {
        "analysis_status",
        "market_data_window",
        "cost_model",
        "signal_count",
        "resolved_trade_count_max",
        "behavior_summary",
        "profile_revision",
    }
    redacted_aggregate = {
        key: aggregate[key]
        for key in allowed_aggregate_keys
        if isinstance(aggregate, dict) and key in aggregate
    }
    redacted_aggregate["tracked_cell_result"] = tracked_result
    redacted_result = {
        "status": "success",
        "message": "Frozen-cell outer-test evidence.",
        "requested_timeframe": sensitivity_response.get("requested_timeframe"),
        "effective_timeframe": sensitivity_response.get("effective_timeframe"),
        "data": {"aggregate": redacted_aggregate},
    }
    execution_evidence = _validate_planned_execution_evidence(
        plan=plan,
        execution_evidence=worker_result.get("execution_evidence"),
    )

    paths = evidence_artifact_paths(artifact_dir, plan)
    provenance = build_full_backtest_provenance(
        attempt=attempt,
        profile_snapshot=task_payload.get("inline_profile_snapshot") or {},
        request_payload=task_payload,
        result_payload=redacted_result,
        source_profile_path=Path(str((task.get("metadata") or {}).get("source_profile_path")))
        if (task.get("metadata") or {}).get("source_profile_path")
        else None,
        worker_id=str(lab_result.get("worker_id") or "") or None,
        worker_pool=str(lab_result.get("worker_pool") or "") or None,
    )
    provenance["execution_evidence"] = execution_evidence
    request_record = {
        "request": worker_result.get("request") or task_payload,
        "status": lab_result.get("status") or "success",
        "job_kind": FULL_BACKTEST_CACHE_TASK_KIND,
        "worker_id": lab_result.get("worker_id"),
        "completed_at": worker_result.get("completed_at"),
    }
    receipt_payload = receipt.model_dump(mode="json")
    manifest = build_evidence_artifact_manifest(
        evidence_plan=plan,
        provenance=provenance,
        execution_evidence=execution_evidence,
        artifact_payloads={
            "result": redacted_result,
            "curve": tracked_detail,
            "job": request_record,
            "cell_receipt": receipt_payload,
        },
    )
    manifest["evidence_level"] = "train_selected_cell_outer_test"
    manifest["cell_receipt"] = receipt_payload
    with evidence_bundle_lock(paths.root):
        reused = _prepare_evidence_bundle_slot(
            artifact_dir,
            plan,
            expected_outcome="success",
        )
        if reused is not None:
            return reused
        write_immutable_json(paths.result, redacted_result)
        write_immutable_json(paths.curve, tracked_detail)
        write_immutable_json(paths.job, request_record)
        write_immutable_json(paths.cell_receipt, receipt_payload)
        write_immutable_json(paths.manifest, manifest)
    return {
        "result_path": str(paths.result),
        "curve_path": str(paths.curve),
        "evidence_manifest_path": str(paths.manifest),
        "cell_receipt_path": str(paths.cell_receipt),
        "evidence_plan_id": plan.plan_id,
        "evidence_role": plan.evidence_role,
        "backend": "lab_gateway",
    }


def resolve_lab_backtest_config(
    *,
    gateway_url: str | None = None,
    gateway_token: str | None = None,
    trading_dashboard_root: Path | None = None,
    worker_contract_hash: str | None = None,
    deadline_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
    result_batch_size: int | None = None,
) -> LabBacktestConfig:
    token = gateway_token
    if token is None:
        token = load_lab_gateway_token()
    contract_hash = worker_contract_hash
    contract_resolution_errors: list[str] = []
    if contract_hash is None:
        root = trading_dashboard_root
        if root is None and os.environ.get("TRADING_DASHBOARD_ROOT"):
            root = Path(os.environ["TRADING_DASHBOARD_ROOT"]).expanduser().resolve()
        if root is not None:
            shared_python = root / "shared" / "python"
            try:
                import sys

                if str(shared_python) not in sys.path:
                    sys.path.insert(0, str(shared_python))
                from fuzzfolio_core.contracts.worker_contract import build_replay_worker_contract

                contract_hash = build_replay_worker_contract(repo_root=root).contract_hash
            except Exception as exc:
                contract_resolution_errors.append(
                    f"python import from {shared_python}: {type(exc).__name__}: {exc}"
                )
            if contract_hash is None:
                compute_root = root / "compute-service"
                try:
                    completed = subprocess.run(
                        [
                            "uv",
                            "run",
                            "python",
                            "-c",
                            (
                                "from app.workers.worker_contract import "
                                "current_replay_worker_contract_hash; "
                                "print(current_replay_worker_contract_hash())"
                            ),
                        ],
                        cwd=compute_root,
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    for line in reversed(completed.stdout.splitlines()):
                        value = line.strip()
                        if value.startswith("sha256:"):
                            contract_hash = value
                            break
                    if contract_hash is None:
                        contract_resolution_errors.append(
                            f"uv subprocess from {compute_root}: no sha256 hash in stdout"
                        )
                except Exception as exc:
                    contract_resolution_errors.append(
                        f"uv subprocess from {compute_root}: {type(exc).__name__}: {exc}"
                    )
    if contract_hash is None:
        detail = "; ".join(contract_resolution_errors) if contract_resolution_errors else "no Trading-Dashboard root provided"
        raise RuntimeError(
            "Could not resolve FuzzFolio replay worker contract hash for lab-gateway "
            "full-backtest work. Provide --trading-dashboard-root or "
            f"--full-backtest-worker-contract-hash. Details: {detail}"
        )
    return LabBacktestConfig(
        gateway_url=str(gateway_url or os.environ.get("FUZZFOLIO_LAB_GATEWAY_URL") or DEFAULT_LAB_GATEWAY_URL).rstrip("/"),
        gateway_token=str(token).strip() if token else None,
        worker_contract_hash=str(contract_hash).strip() if contract_hash else None,
        deadline_seconds=max(float(deadline_seconds or 3600.0), 1.0),
        poll_interval_seconds=max(float(poll_interval_seconds or 2.0), 0.1),
        result_batch_size=max(int(result_batch_size or DEFAULT_RESULT_BATCH_SIZE), 1),
    )


def run_lab_full_backtests(
    *,
    config: Any,
    items: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]],
    lab_config: LabBacktestConfig,
    max_workers: int,
    force_rebuild: bool = False,
    emit: Callable[[str], None] | None = None,
    requested_horizon_months: int = 36,
    evidence_window_start: str | None = None,
    evidence_window_end: str | None = None,
    evidence_role: str = "full_backtest",
    selection_data_end: str | None = None,
    campaign_plan_id: str | None = None,
    lake_manifest_sha256: str | None = None,
    cell_receipts_by_attempt_id: dict[str, FrozenExecutionCellReceipt | dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    _ = force_rebuild
    pending_items = list(items)
    task_by_id: dict[
        str, tuple[Path, dict[str, Any], dict[str, Any], float, dict[str, Any]]
    ] = {}
    results: list[dict[str, Any]] = []
    calculated = 0
    failed = 0
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    frozen_evidence_window_end = evidence_window_end or _now_iso()
    gateway = LabGatewayClient(base_url=lab_config.gateway_url, token=lab_config.gateway_token)
    try:
        preexisting_results = gateway.read_results(limit=1)
        if preexisting_results:
            raise RuntimeError(
                "Lab gateway already has unacknowledged results. Drain stale results or "
                "run corpus catch-up on a dedicated gateway before enqueueing cloud "
                "full-backtest work."
            )
        while pending_items or task_by_id:
            tasks: list[dict[str, Any]] = []
            while pending_items and len(task_by_id) + len(tasks) < max(1, int(max_workers)):
                run_dir, attempt, row, run_metadata = pending_items.pop(0)
                attempt_id = str(attempt.get("attempt_id") or "")
                cell_receipt_raw = (cell_receipts_by_attempt_id or {}).get(attempt_id)
                cell_receipt = (
                    FrozenExecutionCellReceipt.model_validate(cell_receipt_raw)
                    if isinstance(cell_receipt_raw, dict)
                    else cell_receipt_raw
                )
                try:
                    task = build_full_backtest_lab_task(
                        config=config,
                        run_dir=run_dir,
                        attempt=attempt,
                        run_metadata=run_metadata,
                        lab_config=lab_config,
                        batch_id=batch_id,
                        evidence_window_start=evidence_window_start,
                        evidence_window_end=frozen_evidence_window_end,
                        requested_horizon_months=requested_horizon_months,
                        evidence_role=evidence_role,
                        selection_data_end=selection_data_end,
                        campaign_plan_id=campaign_plan_id,
                        lake_manifest_sha256=lake_manifest_sha256,
                        tracked_cell=(
                            cell_receipt.execution_cell
                            if isinstance(cell_receipt, FrozenExecutionCellReceipt)
                            else None
                        ),
                    )
                except Exception as exc:
                    failed += 1
                    entry = {
                        "run_id": run_dir.name,
                        "attempt_id": attempt_id,
                        "candidate_name": row.get("candidate_name"),
                        "score_36m": row.get("score_36m"),
                        "composite_score": row.get("composite_score"),
                        "backend": "lab_gateway",
                        "status": "failed",
                        "error": f"could not build lab task: {exc}",
                    }
                    results.append(entry)
                    if emit is not None:
                        emit(f"  lab skipped: {run_dir.name} {attempt_id} {entry['error']}")
                    continue
                tasks.append(task)
                task_by_id[str(task["task_id"])] = (
                    run_dir,
                    attempt,
                    row,
                    time.time(),
                    {
                        "task": task,
                        "cell_receipt": (
                            cell_receipt.model_dump(mode="json")
                            if isinstance(cell_receipt, FrozenExecutionCellReceipt)
                            else None
                        ),
                    },
                )
            if tasks:
                response = gateway.enqueue_tasks(tasks)
                accepted = int(response.get("accepted") or response.get("enqueued") or 0)
                if accepted != len(tasks):
                    rejected_task_ids = [str(task.get("task_id") or "") for task in tasks[accepted:]]
                    for rejected_task_id in rejected_task_ids:
                        task_by_id.pop(rejected_task_id, None)
                    raise RuntimeError(
                        "Lab gateway rejected corpus full-backtest tasks "
                        f"accepted={accepted}/{len(tasks)}. Restart the gateway or retry after "
                        "terminal task retention clears."
                    )
                if emit is not None:
                    emit(f"lab enqueue accepted={accepted}/{len(tasks)} in_flight={len(task_by_id)} pending={len(pending_items)}")
            if not pending_items and not task_by_id:
                break
            drained = gateway.read_results(limit=lab_config.result_batch_size)
            ack_ids: list[str] = []
            unknown_result_count = 0
            if drained:
                for lab_result in drained:
                    task_id = str(lab_result.get("task_id") or "")
                    item = task_by_id.pop(task_id, None)
                    if item is None:
                        unknown_result_count += 1
                        continue
                    ack_ids.append(str(lab_result.get("lease_id") or ""))
                    run_dir, attempt, row, started_at, submitted = item
                    submitted_task = submitted.get("task") if isinstance(submitted, dict) else submitted
                    submitted_cell_receipt = (
                        submitted.get("cell_receipt") if isinstance(submitted, dict) else None
                    )
                    attempt_id = str(attempt.get("attempt_id") or "")
                    entry: dict[str, Any] = {
                        "run_id": run_dir.name,
                        "attempt_id": attempt_id,
                        "candidate_name": row.get("candidate_name"),
                        "score_36m": row.get("score_36m"),
                        "composite_score": row.get("composite_score"),
                        "duration_seconds": round(time.time() - started_at, 3),
                        "backend": "lab_gateway",
                    }
                    if str(lab_result.get("status") or "").lower() == "success":
                        try:
                            if submitted_cell_receipt is not None:
                                paths = materialize_outer_test_lab_result(
                                    attempt=attempt,
                                    lab_result=lab_result,
                                    task=submitted_task,
                                    cell_receipt=submitted_cell_receipt,
                                )
                            else:
                                paths = materialize_full_backtest_lab_result(
                                    attempt=attempt,
                                    lab_result=lab_result,
                                    task=submitted_task,
                                )
                            entry.update(paths)
                            entry["status"] = "calculated"
                            calculated += 1
                            if emit is not None:
                                emit(f"  lab done: {run_dir.name} {attempt_id} ({entry['duration_seconds']}s)")
                        except Exception as exc:
                            entry["status"] = "failed"
                            entry["error"] = str(exc)
                            failed += 1
                            if emit is not None:
                                emit(f"  lab materialize failed: {run_dir.name} {attempt_id} {exc}")
                    else:
                        worker_result = _unwrap_worker_result(lab_result)
                        error = str(
                            worker_result.get("error")
                            or lab_result.get("result")
                            or "lab worker failed"
                        )
                        terminal_result = worker_result.get("terminal_result")
                        if (
                            isinstance(terminal_result, dict)
                            and terminal_result.get("outcome") == "no_valid_cell"
                        ):
                            try:
                                paths = materialize_no_valid_cell_lab_result(
                                    attempt=attempt,
                                    lab_result=lab_result,
                                    task=submitted_task,
                                )
                                entry.update(paths)
                                entry["status"] = "nonviable"
                                calculated += 1
                                if emit is not None:
                                    emit(
                                        "  lab nonviable: "
                                        f"{run_dir.name} {attempt_id} "
                                        f"({entry['duration_seconds']}s)"
                                    )
                            except Exception as exc:
                                entry["status"] = "failed"
                                entry["error"] = str(exc)
                                failed += 1
                                if emit is not None:
                                    emit(
                                        "  lab terminal materialize failed: "
                                        f"{run_dir.name} {attempt_id} {exc}"
                                    )
                        else:
                            entry["status"] = "failed"
                            entry["error"] = error
                            failed += 1
                            if emit is not None:
                                emit(
                                    f"  lab failed: {run_dir.name} "
                                    f"{attempt_id} {entry['error']}"
                                )
                    results.append(entry)
                if ack_ids:
                    gateway.ack_results(ack_ids)
                if unknown_result_count:
                    raise RuntimeError(
                        "Lab gateway result backlog contains unrelated results ahead of corpus "
                        "full-backtest results. Drain stale results or run corpus catch-up on a "
                        "dedicated gateway before retrying."
                    )
                continue
            time.sleep(lab_config.poll_interval_seconds)
    finally:
        gateway.close()
    return results, calculated, failed
