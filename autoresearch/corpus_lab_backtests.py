from __future__ import annotations

import json
import os
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
from .execution_costs import execution_cost_manifest_payload
from .play_hand_lab import DEFAULT_LAB_GATEWAY_URL, LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token


FULL_BACKTEST_CACHE_TASK_KIND = "full_backtest_cache"
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
) -> dict[str, Any]:
    attempt_id = str(attempt.get("attempt_id") or "")
    artifact_dir = Path(str(attempt.get("artifact_dir") or "")).resolve()
    if not artifact_dir.exists():
        raise RuntimeError(f"Artifact directory does not exist: {artifact_dir}")
    profile_path_raw = str(attempt.get("profile_path") or "").strip()
    profile_path = Path(profile_path_raw).resolve() if profile_path_raw else None
    profile_snapshot = _profile_snapshot_from_file(profile_path)
    request_payload = _request_payload_from_attempt(attempt)
    instruments = request_payload.get("instruments")
    if not isinstance(instruments, list) or not instruments:
        instruments = profile_snapshot.get("instruments")
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

    run_id = run_dir.name
    task_id = f"full-backtest-cache-{attempt_id}"
    if batch_id:
        task_id = f"{task_id}-{batch_id}"
    payload = {
        "job_id": task_id,
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
        "lookback_months": 36,
        "analysis_window_start": None,
        "analysis_window_end": None,
        "bar_limit": int(request_payload.get("bar_limit") or 5000),
        "alert_threshold": float(request_payload.get("alert_threshold") or profile_snapshot.get("notificationThreshold") or 80.0),
        "view_mode": str(request_payload.get("view_mode") or "overview"),
        "direction_mode": str(request_payload.get("direction_mode") or profile_snapshot.get("directionMode") or "both"),
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
        "task_id": task_id,
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
        return nested
    return payload


def materialize_full_backtest_lab_result(
    *,
    attempt: dict[str, Any],
    lab_result: dict[str, Any],
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

    result_path = artifact_dir / FULL_BACKTEST_RESULT_FILENAME
    curve_path = artifact_dir / FULL_BACKTEST_CURVE_FILENAME
    calendar_path = artifact_dir / FULL_BACKTEST_CALENDAR_CURVE_FILENAME
    _write_json(result_path, sensitivity_response)
    _write_json(curve_path, best_detail)
    _write_json(calendar_path, calendar_curve)
    _write_json(artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME, recommended_detail)
    if isinstance(request_payload, dict) and request_payload:
        request_record = {
            "request": request_payload,
            "status": lab_result.get("status") or "success",
            "job_kind": FULL_BACKTEST_CACHE_TASK_KIND,
            "worker_id": lab_result.get("worker_id"),
            "worker_pool": worker_result.get("worker_pool") or lab_result.get("worker_pool"),
            "completed_at": worker_result.get("completed_at"),
        }
        _write_json(
            artifact_dir / "full-backtest-36mo-deep-replay-job.json",
            request_record,
        )
        original_job_path = artifact_dir / "deep-replay-job.json"
        if not original_job_path.exists():
            _write_json(original_job_path, request_record)
    return {
        "curve_path": str(curve_path),
        "result_path": str(result_path),
        "calendar_curve_path": str(calendar_path),
        "recommended_curve_path": str(artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME),
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
) -> tuple[list[dict[str, Any]], int, int]:
    _ = force_rebuild
    pending_items = list(items)
    task_by_id: dict[str, tuple[Path, dict[str, Any], dict[str, Any], float]] = {}
    results: list[dict[str, Any]] = []
    calculated = 0
    failed = 0
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
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
                try:
                    task = build_full_backtest_lab_task(
                        config=config,
                        run_dir=run_dir,
                        attempt=attempt,
                        run_metadata=run_metadata,
                        lab_config=lab_config,
                        batch_id=batch_id,
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
                task_by_id[str(task["task_id"])] = (run_dir, attempt, row, time.time())
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
                    run_dir, attempt, row, started_at = item
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
                            paths = materialize_full_backtest_lab_result(
                                attempt=attempt,
                                lab_result=lab_result,
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
                        entry["status"] = "failed"
                        worker_result = _unwrap_worker_result(lab_result)
                        entry["error"] = str(worker_result.get("error") or lab_result.get("result") or "lab worker failed")
                        failed += 1
                        if emit is not None:
                            emit(f"  lab failed: {run_dir.name} {attempt_id} {entry['error']}")
                    results.append(entry)
                if ack_ids:
                    gateway.ack_results(ack_ids)
                continue
            if drained and unknown_result_count:
                raise RuntimeError(
                    "Lab gateway result backlog contains unrelated results ahead of corpus "
                    "full-backtest results. Drain stale results or run corpus catch-up on a "
                    "dedicated gateway before retrying."
                )
            time.sleep(lab_config.poll_interval_seconds)
    finally:
        gateway.close()
    return results, calculated, failed
