from __future__ import annotations

import copy
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Mapping


_LOCK = threading.RLock()
_INSTALLED = False

_ORIGINAL_LOAD_ATTEMPTS: Any = None
_ORIGINAL_APPEND_ATTEMPT_ROW: Any = None
_ORIGINAL_RECORD_LAB_RESULT: Any = None
_ORIGINAL_NORMALIZE_PERSISTED_SWEEP_SHARD: Any = None
_ORIGINAL_RANK_SWEEP_PERMUTATIONS: Any = None
_ORIGINAL_MERGE_SWEEP_PAYLOADS: Any = None
_ORIGINAL_WRITE_CAMPAIGN_STATE: Any = None
_ORIGINAL_WRITE_LANE_METADATA: Any = None
_ORIGINAL_RENDER_LANE_PROGRESS: Any = None
_ORIGINAL_FORMAT_BARRIER: Any = None

# path -> ((mtime_ns, size), immutable-ish parsed rows)
_ATTEMPT_CACHE: dict[str, tuple[tuple[int, int], list[dict[str, Any]]]] = {}
_STATE_LAST_WRITE_AT: dict[str, float] = {}
_STATE_LAST_SHAPE: dict[str, tuple[int, tuple[int, ...], int]] = {}
_LANE_LAST_WRITE_AT: dict[str, float] = {}
_COUNTERS: dict[str, int] = {
    "attempt_cache_hits": 0,
    "attempt_cache_misses": 0,
    "campaign_state_writes": 0,
    "campaign_state_writes_skipped": 0,
    "lane_metadata_writes": 0,
    "lane_metadata_writes_skipped": 0,
    "progress_renders_skipped": 0,
    "sweep_receipts_compacted": 0,
}


def _path_key(path: Path | str) -> str:
    return str(Path(path).resolve(strict=False))


def _file_signature(path: Path | str) -> tuple[int, int]:
    try:
        stat = Path(path).stat()
    except OSError:
        return (0, 0)
    return (int(stat.st_mtime_ns), int(stat.st_size))


def _counter(name: str, amount: int = 1) -> None:
    with _LOCK:
        _COUNTERS[name] = int(_COUNTERS.get(name, 0)) + int(amount)


def throughput_diagnostics() -> dict[str, int]:
    with _LOCK:
        return dict(_COUNTERS)


def _phase3_runtime(runtime: Any) -> bool:
    return bool(
        runtime is not None
        and getattr(runtime, "formal_authority_kind", None) == "phase3"
        and getattr(runtime, "as_of_date", None)
    )


def _phase3_lane_context(lane_ctx: Any) -> bool:
    run_id = str(getattr(lane_ctx, "run_id", "") or "")
    return run_id.startswith("phase3-") and "-lane-" in run_id


def _checkpoint_interval_seconds() -> float:
    raw = os.getenv("PLAY_HAND_CAMPAIGN_CHECKPOINT_SECONDS", "20")
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return 20.0


def _lane_metadata_interval_seconds() -> float:
    raw = os.getenv("PLAY_HAND_LANE_METADATA_SECONDS", "60")
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return 60.0


def _all_lanes_terminal(lanes: Any) -> bool:
    if not isinstance(lanes, list) or not lanes:
        return False
    for lane in lanes:
        task_ids = list(getattr(lane, "task_ids", ()) or ())
        terminal_count = len(getattr(lane, "completed_task_ids", ()) or ()) + len(
            getattr(lane, "failed_task_ids", ()) or ()
        )
        if not bool(getattr(lane, "terminal", False)) and (
            not task_ids or terminal_count < len(task_ids)
        ):
            return False
    return True


def _cached_load_attempts(path: Path) -> list[dict[str, Any]]:
    key = _path_key(path)
    signature = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == signature:
            _COUNTERS["attempt_cache_hits"] += 1
            return [dict(row) for row in cached[1]]
    rows = _ORIGINAL_LOAD_ATTEMPTS(path)
    snapshot = [dict(row) for row in rows]
    with _LOCK:
        _ATTEMPT_CACHE[key] = (signature, snapshot)
        _COUNTERS["attempt_cache_misses"] += 1
    return [dict(row) for row in snapshot]


def _cached_append_attempt_row(path: Path, row: Mapping[str, Any]) -> None:
    key = _path_key(path)
    before = _file_signature(path)
    _ORIGINAL_APPEND_ATTEMPT_ROW(path, row)
    after = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == before:
            updated = [*cached[1], dict(row)]
            _ATTEMPT_CACHE[key] = (after, updated)
        else:
            _ATTEMPT_CACHE.pop(key, None)


def _without_ranked_alias(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = dict(payload)
    ranked = compact.get("ranked_permutations")
    alias = compact.get("ranked")
    if isinstance(ranked, list) and isinstance(alias, list) and alias == ranked:
        compact.pop("ranked", None)
    return compact


def _task_already_terminal(task_id: str) -> bool:
    try:
        from . import play_hand_lab_memory_deep as deep_memory

        journal = deep_memory._ACTIVE_PLAY_HAND_JOURNAL
        tasks = getattr(journal, "_tasks", None) if journal is not None else None
        task = tasks.get(task_id) if isinstance(tasks, dict) else None
        return isinstance(task, dict) and task.get("status") == "terminal"
    except Exception:
        return False


def _compact_pending_sweep_receipt(
    play_hand_lab: Any,
    *,
    recorded: dict[str, Any],
    lab_result: dict[str, Any],
    runtime: Any,
) -> dict[str, Any]:
    if not _phase3_runtime(runtime) or recorded.get("task_kind") != "sweep_shard":
        return recorded
    sweep_payload = recorded.get("sweep_payload")
    if not isinstance(sweep_payload, dict):
        return recorded
    task_id = str(recorded.get("task_id") or lab_result.get("task_id") or "")
    if not task_id or _task_already_terminal(task_id):
        return recorded

    artifact_dir = Path(str(recorded.get("artifact_dir") or "")).resolve(strict=False)
    sweep_path = artifact_dir / "sweep-results.json"
    compact_sweep = _without_ranked_alias(sweep_payload)
    if sweep_path.is_file():
        play_hand_lab.atomic_write_json(sweep_path, compact_sweep)

    compact_recorded = dict(recorded)
    compact_recorded.pop("sweep_payload", None)
    compact_recorded["sweep_payload_artifact"] = "sweep-results.json"
    play_hand_lab._write_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256=play_hand_lab._worker_result_identity(lab_result),
        recorded_result=compact_recorded,
    )
    _counter("sweep_receipts_compacted")
    return compact_recorded


def _recorded_with_sweep_payload(play_hand_lab: Any, recorded: Mapping[str, Any]) -> dict[str, Any]:
    clone = dict(recorded)
    if isinstance(clone.get("sweep_payload"), dict):
        return clone
    artifact_dir = Path(str(clone.get("artifact_dir") or "")).resolve(strict=False)
    relative = str(clone.get("sweep_payload_artifact") or "sweep-results.json")
    payload = play_hand_lab._load_json(artifact_dir / relative)
    if not isinstance(payload, dict):
        raise play_hand_lab.DurableExecutionError(
            f"sweep task {clone.get('task_id') or '<missing>'} has no readable sweep artifact"
        )
    clone["sweep_payload"] = payload
    return clone


def install_play_hand_throughput_bounds() -> None:
    """Remove nonessential I/O and large duplicate sweep state from the hot path."""

    global _INSTALLED
    global _ORIGINAL_LOAD_ATTEMPTS
    global _ORIGINAL_APPEND_ATTEMPT_ROW
    global _ORIGINAL_RECORD_LAB_RESULT
    global _ORIGINAL_NORMALIZE_PERSISTED_SWEEP_SHARD
    global _ORIGINAL_RANK_SWEEP_PERMUTATIONS
    global _ORIGINAL_MERGE_SWEEP_PAYLOADS
    global _ORIGINAL_WRITE_CAMPAIGN_STATE
    global _ORIGINAL_WRITE_LANE_METADATA
    global _ORIGINAL_RENDER_LANE_PROGRESS
    global _ORIGINAL_FORMAT_BARRIER

    with _LOCK:
        if _INSTALLED:
            return

        from . import ledger
        from . import play_hand_lab

        _ORIGINAL_LOAD_ATTEMPTS = ledger.load_attempts
        _ORIGINAL_APPEND_ATTEMPT_ROW = ledger.append_attempt_row
        _ORIGINAL_RECORD_LAB_RESULT = play_hand_lab._record_lab_result
        _ORIGINAL_NORMALIZE_PERSISTED_SWEEP_SHARD = (
            play_hand_lab._normalize_persisted_sweep_shard
        )
        _ORIGINAL_RANK_SWEEP_PERMUTATIONS = play_hand_lab._rank_sweep_permutations
        _ORIGINAL_MERGE_SWEEP_PAYLOADS = play_hand_lab._merge_sweep_payloads
        _ORIGINAL_WRITE_CAMPAIGN_STATE = play_hand_lab._write_campaign_state
        _ORIGINAL_WRITE_LANE_METADATA = play_hand_lab._write_lane_metadata
        _ORIGINAL_RENDER_LANE_PROGRESS = play_hand_lab._render_lane_progress_artifacts
        _ORIGINAL_FORMAT_BARRIER = play_hand_lab._format_lab_barrier_snapshot

        def rank_sweep_permutations(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return _without_ranked_alias(_ORIGINAL_RANK_SWEEP_PERMUTATIONS(*args, **kwargs))

        def merge_sweep_payloads(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return _without_ranked_alias(_ORIGINAL_MERGE_SWEEP_PAYLOADS(*args, **kwargs))

        def record_lab_result(*args: Any, **kwargs: Any) -> dict[str, Any]:
            recorded = _ORIGINAL_RECORD_LAB_RESULT(*args, **kwargs)
            lab_result = kwargs.get("lab_result")
            runtime = kwargs.get("runtime")
            if isinstance(recorded, dict) and isinstance(lab_result, dict):
                return _compact_pending_sweep_receipt(
                    play_hand_lab,
                    recorded=recorded,
                    lab_result=lab_result,
                    runtime=runtime,
                )
            return recorded

        def normalize_persisted_sweep_shard(*args: Any, **kwargs: Any) -> dict[str, Any]:
            recorded = kwargs.get("recorded")
            if isinstance(recorded, Mapping) and not isinstance(recorded.get("sweep_payload"), dict):
                kwargs = dict(kwargs)
                kwargs["recorded"] = _recorded_with_sweep_payload(play_hand_lab, recorded)
            return _ORIGINAL_NORMALIZE_PERSISTED_SWEEP_SHARD(*args, **kwargs)

        def write_campaign_state(*args: Any, **kwargs: Any) -> Any:
            runtime = kwargs.get("runtime")
            if not _phase3_runtime(runtime):
                return _ORIGINAL_WRITE_CAMPAIGN_STATE(*args, **kwargs)
            path = kwargs.get("path") or (args[0] if args else "")
            key = _path_key(path)
            lanes = kwargs.get("lanes")
            shape = (
                int(kwargs.get("next_lane_index") or 0),
                tuple(sorted(int(item) for item in (kwargs.get("reserved_lane_indices") or ()))),
                len(lanes) if isinstance(lanes, list) else 0,
            )
            now = time.monotonic()
            with _LOCK:
                last_at = _STATE_LAST_WRITE_AT.get(key)
                last_shape = _STATE_LAST_SHAPE.get(key)
            force = (
                last_at is None
                or last_shape != shape
                or _all_lanes_terminal(lanes)
                or (now - last_at) >= _checkpoint_interval_seconds()
            )
            if not force:
                _counter("campaign_state_writes_skipped")
                return None
            result = _ORIGINAL_WRITE_CAMPAIGN_STATE(*args, **kwargs)
            with _LOCK:
                _STATE_LAST_WRITE_AT[key] = time.monotonic()
                _STATE_LAST_SHAPE[key] = shape
                _COUNTERS["campaign_state_writes"] += 1
            return result

        def write_lane_metadata(*args: Any, **kwargs: Any) -> Any:
            runtime = kwargs.get("runtime")
            lane = args[0] if args else kwargs.get("lane")
            if not _phase3_runtime(runtime) or lane is None:
                return _ORIGINAL_WRITE_LANE_METADATA(*args, **kwargs)
            run_id = str(getattr(lane, "run_id", "") or "")
            status = str(kwargs.get("status") or "")
            terminal_statuses = {
                "completed",
                "failed",
                "incomplete",
                "policy_lane_exhausted",
                "promoted",
                "tombstoned",
            }
            now = time.monotonic()
            with _LOCK:
                last_at = _LANE_LAST_WRITE_AT.get(run_id)
            force = (
                last_at is None
                or bool(getattr(lane, "terminal", False))
                or status in terminal_statuses
                or (now - last_at) >= _lane_metadata_interval_seconds()
            )
            if not force:
                _counter("lane_metadata_writes_skipped")
                return None
            result = _ORIGINAL_WRITE_LANE_METADATA(*args, **kwargs)
            with _LOCK:
                _LANE_LAST_WRITE_AT[run_id] = time.monotonic()
                _COUNTERS["lane_metadata_writes"] += 1
            return result

        def render_lane_progress_artifacts(*args: Any, **kwargs: Any) -> Any:
            lane_ctx = kwargs.get("lane_ctx")
            if lane_ctx is None and len(args) >= 2:
                lane_ctx = args[1]
            enabled = str(os.getenv("PLAY_HAND_RENDER_PHASE3_PROGRESS", "0")).lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if _phase3_lane_context(lane_ctx) and not enabled:
                _counter("progress_renders_skipped")
                return None
            return _ORIGINAL_RENDER_LANE_PROGRESS(*args, **kwargs)

        def format_barrier(*args: Any, **kwargs: Any) -> str:
            rendered = _ORIGINAL_FORMAT_BARRIER(*args, **kwargs)
            snapshot = kwargs.get("snapshot")
            if not isinstance(snapshot, dict):
                return rendered
            backlog = int(snapshot.get("result_backlog") or 0)
            backlog_bytes = int(snapshot.get("result_backlog_bytes") or 0)
            pressure = bool(snapshot.get("result_backpressure_active"))
            diagnostics = throughput_diagnostics()
            detail = (
                f"result drain backlog={backlog} bytes={backlog_bytes // (1024 * 1024)}MB "
                f"pressure={'ON' if pressure else 'off'} state-skips="
                f"{diagnostics['campaign_state_writes_skipped']} metadata-skips="
                f"{diagnostics['lane_metadata_writes_skipped']} plot-skips="
                f"{diagnostics['progress_renders_skipped']} compact-sweeps="
                f"{diagnostics['sweep_receipts_compacted']}"
            )
            lines = rendered.splitlines()
            if lines:
                lines.insert(-1, play_hand_lab._box_row(detail))
            return "\n".join(lines)

        ledger.load_attempts = _cached_load_attempts
        ledger.append_attempt_row = _cached_append_attempt_row
        play_hand_lab.load_attempts = _cached_load_attempts
        play_hand_lab.append_attempt_row = _cached_append_attempt_row
        play_hand_lab._rank_sweep_permutations = rank_sweep_permutations
        play_hand_lab._merge_sweep_payloads = merge_sweep_payloads
        play_hand_lab._record_lab_result = record_lab_result
        play_hand_lab._normalize_persisted_sweep_shard = normalize_persisted_sweep_shard
        play_hand_lab._write_campaign_state = write_campaign_state
        play_hand_lab._write_lane_metadata = write_lane_metadata
        play_hand_lab._render_lane_progress_artifacts = render_lane_progress_artifacts
        play_hand_lab._format_lab_barrier_snapshot = format_barrier
        _INSTALLED = True


__all__ = [
    "install_play_hand_throughput_bounds",
    "throughput_diagnostics",
]
