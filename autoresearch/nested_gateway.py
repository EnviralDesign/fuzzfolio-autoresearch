from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Literal

from .corpus_lab_backtests import (
    LabBacktestConfig,
    _profile_snapshot_from_file,
    _unwrap_worker_result,
    build_full_backtest_lab_task,
    materialize_full_backtest_lab_result,
    materialize_no_valid_cell_lab_result,
    materialize_outer_test_lab_result,
)
from .evidence_artifacts import (
    evidence_artifact_paths,
    validate_evidence_artifact_bundle,
)
from .evidence_plan import canonical_sha256, validate_replay_evidence_plan
from .nested_evidence import (
    NestedEvidenceFold,
    build_nested_train_fold,
    freeze_nested_outer_test,
)
from .play_hand_lab import LabGatewayClient


def _window_start(value: Any) -> str:
    token = str(value).strip()
    return token if "T" in token else f"{token}T00:00:00Z"


def _window_end(value: Any) -> str:
    token = str(value).strip()
    if "T" in token:
        return token
    return f"{(date.fromisoformat(token[:10]) + timedelta(days=1)).isoformat()}T00:00:00Z"


def _no_valid_cell_outcome(validation: dict[str, Any]) -> dict[str, Any] | None:
    terminal = validation.get("terminal_outcome")
    if (
        validation.get("status") == "valid"
        and isinstance(terminal, dict)
        and terminal.get("status") == "nonviable"
        and terminal.get("outcome") == "no_valid_cell"
    ):
        return dict(terminal)
    return None


def _validation_stage_status(validation: dict[str, Any]) -> str:
    return "nonviable" if _no_valid_cell_outcome(validation) else str(validation.get("status") or "")


def _validate_nested_materialization_target(
    *, attempt: dict[str, Any], campaign_root: Path
) -> None:
    """Formal callers mark an attempt so replay evidence cannot write into source lanes."""
    raw_root = attempt.get("_nested_materialization_root")
    if raw_root is None:
        return
    raw_campaign_root = Path(campaign_root)
    raw_expected_root = raw_campaign_root / "attempt-evidence"
    raw_root_path = Path(str(raw_root))
    raw_artifact_dir = Path(str(attempt.get("artifact_dir") or ""))
    if any(
        path.is_symlink()
        for path in (
            raw_campaign_root,
            raw_expected_root,
            raw_root_path,
            raw_artifact_dir,
        )
    ):
        raise RuntimeError("formal nested artifact directory uses a symbolic link")
    root = raw_root_path.resolve()
    artifact_dir = raw_artifact_dir.resolve()
    expected_root = raw_expected_root.resolve()
    attempt_id = str(attempt.get("attempt_id") or "").strip()
    expected_target = expected_root / canonical_sha256(
        {"attempt_id": attempt_id}
    ).removeprefix("sha256:")
    try:
        root.relative_to(expected_root)
    except ValueError as exc:
        raise RuntimeError("formal nested artifact directory escapes its campaign root") from exc
    if (
        not attempt_id
        or root != expected_root
        or artifact_dir != expected_target
    ):
        raise RuntimeError("formal nested artifact directory is not campaign-owned")


def _materialize_nested_lab_result(
    *,
    attempt: dict[str, Any],
    task: dict[str, Any],
    lab_result: dict[str, Any],
    cell_receipt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if str(lab_result.get("status") or "").lower() == "success":
        if cell_receipt is not None:
            paths = materialize_outer_test_lab_result(
                attempt=attempt,
                lab_result=lab_result,
                task=task,
                cell_receipt=cell_receipt,
            )
        else:
            paths = materialize_full_backtest_lab_result(
                attempt=attempt,
                lab_result=lab_result,
                task=task,
            )
        return {"status": "calculated", **paths}

    worker_result = _unwrap_worker_result(lab_result)
    terminal_result = worker_result.get("terminal_result")
    if isinstance(terminal_result, dict) and terminal_result.get("outcome") == "no_valid_cell":
        paths = materialize_no_valid_cell_lab_result(
            attempt=attempt,
            lab_result=lab_result,
            task=task,
        )
        return {"status": "nonviable", **paths}

    error = str(
        worker_result.get("error")
        or lab_result.get("result")
        or "lab worker failed"
    )
    return {"status": "failed", "error": error}


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def _profile_for_attempt(attempt: dict[str, Any]) -> dict[str, Any]:
    resolved_snapshot = attempt.get("_worker_ready_profile_snapshot")
    if isinstance(resolved_snapshot, dict):
        profile = dict(resolved_snapshot)
    else:
        profile_path = Path(str(attempt.get("profile_path") or "")).resolve()
        profile = _profile_snapshot_from_file(profile_path)
    profile["notificationThreshold"] = float(
        profile.get("notificationThreshold")
        if profile.get("notificationThreshold") is not None
        else 80.0
    )
    return profile


def _cell_from_training_bundle(
    *,
    attempt: dict[str, Any],
    train_fold: NestedEvidenceFold,
    selection_basis: Literal["best_cell", "recommended_cell", "robust_cell"],
) -> dict[str, float]:
    paths = evidence_artifact_paths(
        Path(str(attempt.get("artifact_dir") or "")).resolve(),
        train_fold.train_plan,
    )
    detail_path = (
        paths.recommended_curve
        if selection_basis in {"recommended_cell", "robust_cell"}
        else paths.curve
    )
    payload = json.loads(detail_path.read_text(encoding="utf-8"))
    cell = payload.get("cell") if isinstance(payload, dict) else None
    if not isinstance(cell, dict):
        raise RuntimeError(f"Training detail is missing its selected cell: {detail_path}")
    try:
        return {
            "stop_loss_percent": float(cell["stop_loss_percent"]),
            "reward_multiple": float(cell["reward_multiple"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Training detail has an invalid selected cell: {detail_path}") from exc


def _run_outer_tasks(
    *,
    tasks: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]],
    lab_config: LabBacktestConfig,
    max_workers: int,
    emit: Callable[[str], None] | None,
) -> list[dict[str, Any]]:
    pending = list(tasks)
    active: dict[str, tuple[dict[str, Any], dict[str, Any], dict[str, Any], float]] = {}
    results: list[dict[str, Any]] = []
    gateway = LabGatewayClient(base_url=lab_config.gateway_url, token=lab_config.gateway_token)
    try:
        if gateway.read_results(limit=1):
            raise RuntimeError("Lab gateway has unacknowledged results before nested outer submission")
        while pending or active:
            batch: list[dict[str, Any]] = []
            while pending and len(active) + len(batch) < max(1, int(max_workers)):
                attempt, task, fold_payload = pending.pop(0)
                batch.append(task)
                active[str(task["task_id"])] = (attempt, task, fold_payload, time.time())
            if batch:
                response = gateway.enqueue_tasks(batch)
                accepted = int(response.get("accepted") or response.get("enqueued") or 0)
                if accepted != len(batch):
                    raise RuntimeError(f"Lab gateway accepted {accepted}/{len(batch)} nested outer tasks")
            drained = gateway.read_results(limit=lab_config.result_batch_size)
            if not drained:
                time.sleep(lab_config.poll_interval_seconds)
                continue
            ack_ids: list[str] = []
            for lab_result in drained:
                task_id = str(lab_result.get("task_id") or "")
                item = active.pop(task_id, None)
                if item is None:
                    raise RuntimeError(f"Nested gateway returned an unrelated task: {task_id}")
                attempt, task, fold_payload, started = item
                ack_ids.append(str(lab_result.get("lease_id") or ""))
                entry = {
                    "attempt_id": attempt.get("attempt_id"),
                    "task_id": task_id,
                    "duration_seconds": round(time.time() - started, 3),
                }
                try:
                    entry.update(
                        _materialize_nested_lab_result(
                            attempt=attempt,
                            lab_result=lab_result,
                            task=task,
                            cell_receipt=fold_payload["cell_receipt"],
                        )
                    )
                except Exception as exc:
                    entry.update({"status": "failed", "error": str(exc)})
                results.append(entry)
                if emit:
                    emit(f"nested outer {entry['status']}: {entry['attempt_id']}")
            gateway.ack_results(ack_ids)
    finally:
        gateway.close()
    return results


def _run_train_tasks(
    *,
    tasks: list[tuple[dict[str, Any], dict[str, Any]]],
    lab_config: LabBacktestConfig,
    max_workers: int,
    emit: Callable[[str], None] | None,
) -> list[dict[str, Any]]:
    pending = list(tasks)
    active: dict[str, tuple[dict[str, Any], dict[str, Any], float]] = {}
    results: list[dict[str, Any]] = []
    gateway = LabGatewayClient(base_url=lab_config.gateway_url, token=lab_config.gateway_token)
    try:
        if gateway.read_results(limit=1):
            raise RuntimeError("Lab gateway has unacknowledged results before nested train submission")
        while pending or active:
            batch: list[dict[str, Any]] = []
            while pending and len(active) + len(batch) < max(1, int(max_workers)):
                attempt, task = pending.pop(0)
                batch.append(task)
                active[str(task["task_id"])] = (attempt, task, time.time())
            if batch:
                response = gateway.enqueue_tasks(batch)
                accepted = int(response.get("accepted") or response.get("enqueued") or 0)
                if accepted != len(batch):
                    raise RuntimeError(f"Lab gateway accepted {accepted}/{len(batch)} nested train tasks")
            drained = gateway.read_results(limit=lab_config.result_batch_size)
            if not drained:
                time.sleep(lab_config.poll_interval_seconds)
                continue
            ack_ids: list[str] = []
            for lab_result in drained:
                task_id = str(lab_result.get("task_id") or "")
                item = active.pop(task_id, None)
                if item is None:
                    raise RuntimeError(f"Nested gateway returned an unrelated train task: {task_id}")
                attempt, task, started = item
                ack_ids.append(str(lab_result.get("lease_id") or ""))
                entry = {
                    "attempt_id": attempt.get("attempt_id"),
                    "task_id": task_id,
                    "duration_seconds": round(time.time() - started, 3),
                }
                try:
                    entry.update(
                        _materialize_nested_lab_result(
                            attempt=attempt,
                            lab_result=lab_result,
                            task=task,
                        )
                    )
                except Exception as exc:
                    entry.update({"status": "failed", "error": str(exc)})
                results.append(entry)
                if emit:
                    emit(f"nested train {entry['status']}: {entry['attempt_id']}")
            gateway.ack_results(ack_ids)
    finally:
        gateway.close()
    return results


def run_nested_gateway_fold(
    *,
    config: Any,
    items: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]],
    fold: dict[str, Any],
    campaign_plan_id: str,
    campaign_root: Path,
    lab_config: LabBacktestConfig,
    max_workers: int,
    train_horizon_months: int,
    test_horizon_months: int,
    selection_basis: Literal["best_cell", "recommended_cell", "robust_cell"] = "recommended_cell",
    lake_manifest_sha256: str | None = None,
    freeze_cells: bool = True,
    submit_outer: bool = True,
    outer_selected_attempt_ids: set[str] | list[str] | tuple[str, ...] | None = None,
    emit: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if submit_outer and not freeze_cells:
        raise ValueError("nested outer submission requires frozen execution cells")
    fold_id = str(fold.get("fold_id") or "").strip()
    if not fold_id:
        raise ValueError("nested fold requires fold_id")
    train_start = _window_start(fold["train_start"])
    train_end = _window_end(fold["train_end"])
    test_start = _window_start(fold["test_start"])
    test_end = _window_end(fold["test_end"])
    embargo_days = int(fold.get("embargo_days") or 0)
    state_path = Path(campaign_root) / fold_id / "nested-state.json"

    item_attempt_ids = [str(item[1].get("attempt_id") or "") for item in items]
    if any(not attempt_id for attempt_id in item_attempt_ids):
        raise ValueError("nested items require non-empty attempt_id values")
    if len(set(item_attempt_ids)) != len(item_attempt_ids):
        raise ValueError("nested items contain duplicate or ambiguous attempt_id values")
    for _run_dir, attempt, _row, _run_metadata in items:
        _validate_nested_materialization_target(
            attempt=attempt,
            campaign_root=Path(campaign_root),
        )
    selected_values = (
        list(outer_selected_attempt_ids) if outer_selected_attempt_ids is not None else None
    )
    if selected_values is not None and len(set(selected_values)) != len(selected_values):
        raise ValueError("outer selection contains duplicate or ambiguous attempt IDs")
    selected_ids = set(selected_values) if selected_values is not None else None
    unknown_selected = (selected_ids or set()) - set(item_attempt_ids)
    if unknown_selected:
        raise ValueError(
            "outer selection contains unknown attempt IDs: " + ", ".join(sorted(unknown_selected))
        )

    planned: list[tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], NestedEvidenceFold]] = []
    train_pending: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for run_dir, attempt, row, run_metadata in items:
        train_fold = build_nested_train_fold(
            campaign_plan_id=campaign_plan_id,
            fold_id=fold_id,
            profile_snapshot=_profile_for_attempt(attempt),
            train_start=train_start,
            train_end=train_end,
            train_horizon_months=train_horizon_months,
            embargo_days=embargo_days,
            lake_manifest_sha256=lake_manifest_sha256,
        )
        planned.append((run_dir, attempt, row, run_metadata, train_fold))
        validation = validate_evidence_artifact_bundle(
            Path(str(attempt.get("artifact_dir") or "")).resolve(),
            train_fold.train_plan,
        )
        if validation["status"] != "valid":
            train_task = build_full_backtest_lab_task(
                config=config,
                run_dir=run_dir,
                attempt=attempt,
                run_metadata=run_metadata,
                lab_config=lab_config,
                task_id=(
                    f"nested:{campaign_plan_id}:{fold_id}:"
                    f"{attempt.get('attempt_id')}:train:"
                    f"{train_fold.train_plan.plan_id[-16:]}"
                ),
                evidence_plan=train_fold.train_plan,
                profile_snapshot_override=_profile_for_attempt(attempt),
            )
            train_pending.append((attempt, train_task))

    train_results: list[dict[str, Any]] = []
    if train_pending:
        train_results = _run_train_tasks(
            tasks=train_pending,
            lab_config=lab_config,
            max_workers=max_workers,
            emit=emit,
        )
        failed_train = [row for row in train_results if row.get("status") == "failed"]
        if failed_train:
            raise RuntimeError(f"Nested train stage failed for {len(failed_train)} strategies")

    if not freeze_cells:
        payload = {
            "campaign_plan_id": campaign_plan_id,
            "fold": fold,
            "selection_basis": selection_basis,
            "strategy_count": len(planned),
            "requested_attempt_ids": sorted(item_attempt_ids),
            "train_reused_count": len(planned) - len(train_pending),
            "train_calculated_count": sum(
                row.get("status") == "calculated" for row in train_results
            ),
            "train_terminal_count": len(train_results),
            "status": "training_complete",
            "state_path": str(state_path),
        }
        _write_state(state_path, payload)
        return payload

    outer_tasks: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    fold_records: list[dict[str, Any]] = []
    for run_dir, attempt, _row, run_metadata, train_fold in planned:
        train_validation = validate_evidence_artifact_bundle(
            Path(str(attempt.get("artifact_dir") or "")).resolve(),
            train_fold.train_plan,
        )
        train_outcome = _no_valid_cell_outcome(train_validation)
        train_paths = evidence_artifact_paths(
            Path(str(attempt.get("artifact_dir") or "")).resolve(),
            train_fold.train_plan,
        )
        if train_outcome is not None:
            attempt_id = str(attempt.get("attempt_id") or "")
            if selected_ids is not None and attempt_id in selected_ids:
                raise ValueError(
                    f"outer selection contains training-ineligible attempt ID: {attempt_id}"
                )
            fold_records.append(
                {
                    "run_id": run_dir.name,
                    "attempt_id": attempt.get("attempt_id"),
                    **train_fold.model_dump(mode="json"),
                    "train_validation_status": "nonviable",
                    "train_terminal_outcome": train_outcome,
                    "train_result_path": str(train_paths.result),
                    "outer_validation_status": "not_applicable",
                    "stage_status": "train_nonviable",
                }
            )
            continue
        if train_validation["status"] != "valid":
            raise RuntimeError(f"Train evidence did not validate for {attempt.get('attempt_id')}")
        cell = _cell_from_training_bundle(
            attempt=attempt,
            train_fold=train_fold,
            selection_basis=selection_basis,
        )
        frozen_fold = freeze_nested_outer_test(
            train_fold,
            profile_snapshot=_profile_for_attempt(attempt),
            selected_cell=cell,
            selection_basis=selection_basis,
            test_start=test_start,
            test_end=test_end,
            test_horizon_months=test_horizon_months,
        )
        outer_plan = frozen_fold.outer_test_plan
        if outer_plan is None or frozen_fold.cell_receipt is None:
            raise RuntimeError("Nested fold did not freeze outer evidence")
        attempt_id = str(attempt.get("attempt_id") or "")
        selected_for_outer = (
            selected_ids is None
            or attempt_id in selected_ids
        )
        if not submit_outer:
            outer_stage_status = "pending_selection"
            outer_validation = {"status": "not_checked"}
        elif not selected_for_outer:
            outer_stage_status = "not_selected"
            outer_validation = {"status": "not_checked"}
        else:
            outer_validation = validate_evidence_artifact_bundle(
                Path(str(attempt.get("artifact_dir") or "")).resolve(), outer_plan
            )
            outer_stage_status = _validation_stage_status(outer_validation)
        outer_outcome = (
            _no_valid_cell_outcome(outer_validation) if selected_for_outer and submit_outer else None
        )
        record = {
            "run_id": run_dir.name,
            "attempt_id": attempt.get("attempt_id"),
            **frozen_fold.model_dump(mode="json"),
            "train_validation_status": "valid",
            "outer_validation_status": outer_stage_status,
            "train_result_path": str(
                evidence_artifact_paths(
                    Path(str(attempt.get("artifact_dir") or "")).resolve(),
                    train_fold.train_plan,
                ).result
            ),
            "train_curve_path": str(
                (
                    evidence_artifact_paths(
                        Path(str(attempt.get("artifact_dir") or "")).resolve(),
                        train_fold.train_plan,
                    ).recommended_curve
                    if selection_basis in {"recommended_cell", "robust_cell"}
                    else evidence_artifact_paths(
                        Path(str(attempt.get("artifact_dir") or "")).resolve(),
                        train_fold.train_plan,
                    ).curve
                )
            ),
            "outer_result_path": str(
                evidence_artifact_paths(
                    Path(str(attempt.get("artifact_dir") or "")).resolve(),
                    outer_plan,
                ).result
            ),
            "outer_curve_path": str(
                evidence_artifact_paths(
                    Path(str(attempt.get("artifact_dir") or "")).resolve(),
                    outer_plan,
                ).curve
            ),
        }
        if outer_outcome is not None:
            record["outer_terminal_outcome"] = outer_outcome
        fold_records.append(record)
        if not submit_outer or not selected_for_outer:
            continue
        if outer_stage_status in {"valid", "nonviable"}:
            continue
        task = build_full_backtest_lab_task(
            config=config,
            run_dir=run_dir,
            attempt=attempt,
            run_metadata=run_metadata,
            lab_config=lab_config,
            task_id=(
                f"nested:{campaign_plan_id}:{fold_id}:"
                f"{attempt.get('attempt_id')}:{outer_plan.plan_id[-16:]}"
            ),
            evidence_plan=outer_plan,
            profile_snapshot_override=_profile_for_attempt(attempt),
            tracked_cell=frozen_fold.cell_receipt.execution_cell,
        )
        outer_tasks.append((attempt, task, frozen_fold.model_dump(mode="json")))

    eligible_ids = {
        str(record.get("attempt_id") or "")
        for record in fold_records
        if record.get("train_validation_status") == "valid"
    }
    required_outer_ids = eligible_ids if selected_ids is None else selected_ids
    planned_outer_ids = {
        str(record.get("attempt_id") or "")
        for record in fold_records
        if record.get("outer_test_plan")
        and str(record.get("attempt_id") or "") in required_outer_ids
    }
    if submit_outer and planned_outer_ids != required_outer_ids:
        raise RuntimeError("outer submission set does not exactly match frozen selection")

    _write_state(
        state_path,
        {
            "campaign_plan_id": campaign_plan_id,
            "fold": fold,
            "selection_basis": selection_basis,
            "records": fold_records,
            "status": (
                "cells_frozen"
                if not submit_outer
                else "outer_pending" if outer_tasks else "complete"
            ),
        },
    )
    if not submit_outer:
        payload = {
            "campaign_plan_id": campaign_plan_id,
            "fold": fold,
            "selection_basis": selection_basis,
            "strategy_count": len(planned),
            "train_reused_count": len(planned) - len(train_pending),
            "train_calculated_count": sum(
                row.get("status") == "calculated" for row in train_results
            ),
            "train_nonviable_count": sum(
                record.get("train_validation_status") == "nonviable"
                for record in fold_records
            ),
            "records": fold_records,
            "outer_results": [],
            "status": "cells_frozen",
            "state_path": str(state_path),
        }
        _write_state(state_path, payload)
        return payload
    outer_results = _run_outer_tasks(
        tasks=outer_tasks,
        lab_config=lab_config,
        max_workers=max_workers,
        emit=emit,
    ) if outer_tasks else []
    failed_outer = [row for row in outer_results if row.get("status") == "failed"]
    for record in fold_records:
        if not record.get("outer_test_plan"):
            continue
        if (
            selected_ids is not None
            and str(record.get("attempt_id") or "") not in selected_ids
        ):
            continue
        outer_plan = validate_replay_evidence_plan(record["outer_test_plan"])
        attempt = next(
            item[1]
            for item in planned
            if str(item[1].get("attempt_id") or "")
            == str(record.get("attempt_id") or "")
        )
        outer_validation = validate_evidence_artifact_bundle(
            Path(str(attempt.get("artifact_dir") or "")).resolve(),
            outer_plan,
        )
        record["outer_validation_status"] = _validation_stage_status(outer_validation)
        outer_outcome = _no_valid_cell_outcome(outer_validation)
        if outer_outcome is not None:
            record["outer_terminal_outcome"] = outer_outcome
    final_status = "failed" if failed_outer else "complete"
    processed_outer_ids = {
        str(record.get("attempt_id") or "")
        for record in fold_records
        if record.get("outer_validation_status") in {"valid", "nonviable"}
        and str(record.get("attempt_id") or "") in required_outer_ids
    }
    if not failed_outer and processed_outer_ids != required_outer_ids:
        raise RuntimeError("outer result set does not exactly match frozen selection")
    outer_eligible_count = sum(
        1
        for record in fold_records
        if record.get("outer_test_plan")
        and (
            selected_ids is None
            or str(record.get("attempt_id") or "") in selected_ids
        )
    )
    payload = {
        "campaign_plan_id": campaign_plan_id,
        "fold": fold,
        "selection_basis": selection_basis,
        "strategy_count": len(planned),
        "train_reused_count": len(planned) - len(train_pending),
        "train_calculated_count": sum(row.get("status") == "calculated" for row in train_results),
        "train_nonviable_count": sum(
            record.get("train_validation_status") == "nonviable"
            for record in fold_records
        ),
        "outer_reused_count": outer_eligible_count - len(outer_tasks),
        "outer_calculated_count": sum(row.get("status") == "calculated" for row in outer_results),
        "outer_nonviable_count": sum(
            record.get("outer_validation_status") == "nonviable"
            for record in fold_records
        ),
        "outer_skipped_train_nonviable_count": len(planned) - outer_eligible_count,
        "outer_failed_count": len(failed_outer),
        "records": fold_records,
        "outer_results": outer_results,
        "status": final_status,
        "state_path": str(state_path),
    }
    _write_state(state_path, payload)
    if failed_outer:
        raise RuntimeError(f"Nested outer stage failed for {len(failed_outer)} strategies")
    return payload


def run_nested_gateway_training_fold(**kwargs: Any) -> dict[str, Any]:
    return run_nested_gateway_fold(
        **kwargs,
        freeze_cells=False,
        submit_outer=False,
    )


def freeze_nested_gateway_cells_fold(**kwargs: Any) -> dict[str, Any]:
    return run_nested_gateway_fold(
        **kwargs,
        freeze_cells=True,
        submit_outer=False,
    )


def run_nested_gateway_selected_outer_fold(
    *, outer_selected_attempt_ids: set[str] | list[str] | tuple[str, ...], **kwargs: Any
) -> dict[str, Any]:
    return run_nested_gateway_fold(
        **kwargs,
        freeze_cells=True,
        submit_outer=True,
        outer_selected_attempt_ids=outer_selected_attempt_ids,
    )
