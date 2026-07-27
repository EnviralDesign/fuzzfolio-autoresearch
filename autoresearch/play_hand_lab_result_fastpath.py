from __future__ import annotations

import copy
import json
import threading
import time
from collections import OrderedDict
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping


_LOCK = threading.RLock()
_INSTALLED = False
_IDENTITY_CACHE_LIMIT = 4096
_SCORE_CACHE_LIMIT = 512
# A receipt is cached only between its write and the immediately following
# validation. Keep this deliberately tiny and consume entries once.
_RECEIPT_CACHE_LIMIT = 128

_ORIGINAL_WORKER_RESULT_IDENTITY: Any = None
_ORIGINAL_WRITE_JSON: Any = None
_ORIGINAL_SCORE_LAB_ARTIFACT: Any = None
_ORIGINAL_WRITE_TASK_RESULT_RECEIPT: Any = None
_ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT: Any = None
_ORIGINAL_RECORD_LAB_RESULT: Any = None
_ORIGINAL_FORMAT_BARRIER: Any = None
_ORIGINAL_THROUGHPUT_COMPACT_SWEEP: Any = None

_IDENTITY_CACHE: OrderedDict[tuple[Any, ...], str] = OrderedDict()
_SCORE_CACHE: OrderedDict[str, Any] = OrderedDict()
_RECEIPT_CACHE: OrderedDict[str, tuple[int, int, str, str, dict[str, Any]]] = OrderedDict()

_COUNTERS: dict[str, float | int] = {
    "results_recorded": 0,
    "result_seconds_total": 0.0,
    "result_seconds_max": 0.0,
    "direct_scores": 0,
    "cli_score_fallbacks": 0,
    "identity_cache_hits": 0,
    "identity_cache_misses": 0,
    "receipt_cache_hits": 0,
    "receipt_cache_misses": 0,
    "receipt_cache_skipped_derived": 0,
    "receipt_cache_peak_entries": 0,
    "compact_job_artifacts": 0,
    "compact_sweep_artifacts": 0,
}


def result_fastpath_diagnostics() -> dict[str, float | int]:
    with _LOCK:
        snapshot = dict(_COUNTERS)
        snapshot["identity_cache_entries"] = len(_IDENTITY_CACHE)
        snapshot["score_cache_entries"] = len(_SCORE_CACHE)
        snapshot["receipt_cache_entries"] = len(_RECEIPT_CACHE)
        return snapshot


def _counter(name: str, amount: float | int = 1) -> None:
    with _LOCK:
        _COUNTERS[name] = _COUNTERS.get(name, 0) + amount


def _bounded_put(
    cache: OrderedDict[Any, Any],
    key: Any,
    value: Any,
    *,
    limit: int,
) -> None:
    with _LOCK:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > max(int(limit), 1):
            cache.popitem(last=False)


def _path_key(path: Path | str) -> str:
    return str(Path(path).resolve(strict=False))


def _identity_cache_key(lab_result: Mapping[str, Any]) -> tuple[Any, ...]:
    nested = lab_result.get("result")
    return (
        id(lab_result),
        id(nested),
        str(lab_result.get("task_id") or ""),
        str(lab_result.get("lease_id") or ""),
        str(lab_result.get("status") or ""),
    )


def _mapping_at_path(payload: Any, path: tuple[str, ...]) -> dict[str, Any] | None:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return dict(current) if isinstance(current, dict) else None



def _compact_score_summary(summary: Any) -> dict[str, Any]:
    if not isinstance(summary, Mapping):
        return {}
    keep = (
        "score_lab",
        "score_lab_payload",
        "quality_score",
        "quality_score_version",
        "quality_score_belief_basis",
        "best_cell",
        "best_cell_path_metrics",
        "dsr",
        "psr",
        "k_ratio",
        "sharpe_r",
        "terminal_result",
    )
    return {
        key: copy.deepcopy(summary[key])
        for key in keep
        if key in summary and summary[key] not in (None, "", [], {})
    }

def _score_from_sensitivity(play_hand_lab: Any, payload: Mapping[str, Any]) -> Any | None:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    aggregate = (
        _mapping_at_path(data, ("aggregate",))
        or _mapping_at_path(data, ("data", "aggregate"))
        or _mapping_at_path(data, ("score_lab",))
    )
    best = aggregate or data
    try:
        score = play_hand_lab.build_attempt_score(
            {"best": best, "data": data},
            dict(payload),
        )
    except Exception:
        return None
    basis = str(getattr(score, "score_basis", "") or "")
    if getattr(score, "composite_score", None) is None:
        return None
    if not basis.startswith(play_hand_lab.CANONICAL_SCORE_LAB_VERSION + ":"):
        return None
    return replace(score, best_summary=_compact_score_summary(score.best_summary))


def _compact_analysis_result(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    keep = (
        "aggregate",
        "score_lab",
        "quality_score",
        "analysis_status",
        "warnings",
        "profile_id",
        "timeframe",
        "best_cell",
        "matrix_summary",
        "terminal_result",
    )
    compact = {
        key: copy.deepcopy(payload[key])
        for key in keep
        if key in payload and payload[key] not in (None, "", [], {})
    }
    nested = payload.get("data")
    if isinstance(nested, dict):
        compact_nested = _compact_analysis_result(nested)
        if isinstance(compact_nested, dict) and compact_nested:
            compact["data"] = compact_nested
    compact["full_result_omitted"] = True
    return compact


def _compact_deep_replay_job_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = dict(payload)
    if isinstance(compact.get("result"), dict):
        compact["result"] = _compact_analysis_result(compact["result"])
    compact["full_result_omitted"] = True
    return compact


def _compact_recorded_sweep(recorded: Mapping[str, Any]) -> dict[str, Any]:
    compact = dict(recorded)
    if compact.get("task_kind") != "sweep_shard":
        return compact
    if isinstance(compact.get("sweep_payload"), dict):
        compact.pop("sweep_payload", None)
        compact["sweep_payload_artifact"] = str(
            compact.get("sweep_payload_artifact") or "sweep-results.json"
        )
    return compact


def _receipt_stat(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return int(stat.st_mtime_ns), int(stat.st_size)


def install_play_hand_result_fastpath() -> None:
    """Install the default single-process result drain optimized for large worker pools."""

    global _INSTALLED
    global _ORIGINAL_WORKER_RESULT_IDENTITY
    global _ORIGINAL_WRITE_JSON
    global _ORIGINAL_SCORE_LAB_ARTIFACT
    global _ORIGINAL_WRITE_TASK_RESULT_RECEIPT
    global _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT
    global _ORIGINAL_RECORD_LAB_RESULT
    global _ORIGINAL_FORMAT_BARRIER
    global _ORIGINAL_THROUGHPUT_COMPACT_SWEEP

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab
        from . import play_hand_lab_throughput as throughput

        _ORIGINAL_WORKER_RESULT_IDENTITY = play_hand_lab._worker_result_identity
        _ORIGINAL_WRITE_JSON = play_hand_lab._write_json
        _ORIGINAL_SCORE_LAB_ARTIFACT = play_hand_lab._score_lab_artifact
        _ORIGINAL_WRITE_TASK_RESULT_RECEIPT = play_hand_lab._write_task_result_receipt
        _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT = play_hand_lab._validate_task_result_receipt
        _ORIGINAL_RECORD_LAB_RESULT = play_hand_lab._record_lab_result
        _ORIGINAL_FORMAT_BARRIER = play_hand_lab._format_lab_barrier_snapshot
        _ORIGINAL_THROUGHPUT_COMPACT_SWEEP = throughput._compact_pending_sweep_receipt

        def worker_result_identity(lab_result: dict[str, Any]) -> str:
            key = _identity_cache_key(lab_result)
            with _LOCK:
                cached = _IDENTITY_CACHE.get(key)
                if cached is not None:
                    _IDENTITY_CACHE.move_to_end(key)
                    _COUNTERS["identity_cache_hits"] = int(
                        _COUNTERS["identity_cache_hits"]
                    ) + 1
                    return cached
            identity = _ORIGINAL_WORKER_RESULT_IDENTITY(lab_result)
            _counter("identity_cache_misses")
            _bounded_put(
                _IDENTITY_CACHE,
                key,
                identity,
                limit=_IDENTITY_CACHE_LIMIT,
            )
            return identity

        def write_json(path: Path, payload: Any) -> Any:
            target = Path(path)
            output = payload
            if target.name == "sensitivity-response.json" and isinstance(payload, dict):
                score = _score_from_sensitivity(play_hand_lab, payload)
                result = _ORIGINAL_WRITE_JSON(target, payload)
                if score is not None:
                    _bounded_put(
                        _SCORE_CACHE,
                        _path_key(target.parent),
                        score,
                        limit=_SCORE_CACHE_LIMIT,
                    )
                return result
            if target.name == "deep-replay-job.json" and isinstance(payload, dict):
                output = _compact_deep_replay_job_payload(payload)
                _counter("compact_job_artifacts")
            elif target.name == "sweep-results.json" and isinstance(payload, dict):
                output = throughput._without_ranked_alias(payload)
                _counter("compact_sweep_artifacts")
            return _ORIGINAL_WRITE_JSON(target, output)

        def score_lab_artifact(*, cli: Any, artifact_dir: Path, strict: bool) -> tuple[Any, Any]:
            key = _path_key(artifact_dir)
            with _LOCK:
                score = _SCORE_CACHE.pop(key, None)
            if score is not None:
                _counter("direct_scores")
                return score, None
            _counter("cli_score_fallbacks")
            return _ORIGINAL_SCORE_LAB_ARTIFACT(
                cli=cli,
                artifact_dir=artifact_dir,
                strict=strict,
            )

        def persist_task_result_receipt(
            path: Path,
            payload: Mapping[str, Any],
            *,
            task_id: str,
            worker_result_sha256: str,
        ) -> dict[str, Any]:
            sealed = play_hand_lab._seal_task_result_receipt(payload)
            if (
                sealed.get("task_id") != task_id
                or sealed.get("worker_result_sha256") != worker_result_sha256
            ):
                raise play_hand_lab.DurableExecutionError(
                    f"task result receipt conflicts for task {task_id}"
                )
            play_hand_lab.atomic_write_json(path, sealed)
            key = _path_key(path)
            with _LOCK:
                _RECEIPT_CACHE.pop(key, None)
            # Receipts containing derived task graphs may include full profile
            # snapshots and sweep definitions. They are returned directly to the
            # journal and must never become a multi-result cache.
            cacheable = (
                "derived_tasks" not in sealed
                and "compatibility_migration" not in sealed
            )
            stat = _receipt_stat(Path(path)) if cacheable else None
            if stat is not None:
                _bounded_put(
                    _RECEIPT_CACHE,
                    key,
                    (
                        stat[0],
                        stat[1],
                        task_id,
                        worker_result_sha256,
                        sealed,
                    ),
                    limit=_RECEIPT_CACHE_LIMIT,
                )
                with _LOCK:
                    _COUNTERS["receipt_cache_peak_entries"] = max(
                        int(_COUNTERS["receipt_cache_peak_entries"]),
                        len(_RECEIPT_CACHE),
                    )
            elif not cacheable:
                _counter("receipt_cache_skipped_derived")
            return sealed

        def validate_task_result_receipt(
            path: Path,
            *,
            task_id: str,
            worker_result_sha256: str | None = None,
        ) -> dict[str, Any]:
            key = _path_key(path)
            stat = _receipt_stat(Path(path))
            with _LOCK:
                # This is a transaction-local handoff, not a historical receipt
                # cache. Consume it once so completed task graphs cannot accumulate.
                cached = _RECEIPT_CACHE.pop(key, None)
                if cached is not None and stat == (cached[0], cached[1]):
                    if cached[2] == task_id and (
                        worker_result_sha256 is None
                        or cached[3] == worker_result_sha256
                    ):
                        _COUNTERS["receipt_cache_hits"] = int(
                            _COUNTERS["receipt_cache_hits"]
                        ) + 1
                        return cached[4]
            _counter("receipt_cache_misses")
            return _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT(
                path,
                task_id=task_id,
                worker_result_sha256=worker_result_sha256,
            )

        def write_task_result_receipt(
            path: Path,
            *,
            task_id: str,
            worker_result_sha256: str,
            recorded_result: dict[str, Any],
        ) -> dict[str, Any]:
            compact = _compact_recorded_sweep(recorded_result)
            if compact != recorded_result:
                try:
                    throughput._counter("sweep_receipts_compacted")
                except Exception:
                    pass
            return _ORIGINAL_WRITE_TASK_RESULT_RECEIPT(
                path,
                task_id=task_id,
                worker_result_sha256=worker_result_sha256,
                recorded_result=compact,
            )

        def compact_pending_sweep_receipt(
            play_hand_lab_module: Any,
            *,
            recorded: dict[str, Any],
            lab_result: dict[str, Any],
            runtime: Any,
        ) -> dict[str, Any]:
            compact = _compact_recorded_sweep(recorded)
            receipt_path = (
                Path(str(recorded.get("artifact_dir") or ""))
                / "task-result-receipt.json"
            )
            if receipt_path.is_file():
                return compact
            return _ORIGINAL_THROUGHPUT_COMPACT_SWEEP(
                play_hand_lab_module,
                recorded=recorded,
                lab_result=lab_result,
                runtime=runtime,
            )

        def record_lab_result(*args: Any, **kwargs: Any) -> dict[str, Any]:
            started = time.monotonic()
            try:
                return _ORIGINAL_RECORD_LAB_RESULT(*args, **kwargs)
            finally:
                elapsed = max(time.monotonic() - started, 0.0)
                with _LOCK:
                    _COUNTERS["results_recorded"] = int(
                        _COUNTERS["results_recorded"]
                    ) + 1
                    _COUNTERS["result_seconds_total"] = float(
                        _COUNTERS["result_seconds_total"]
                    ) + elapsed
                    _COUNTERS["result_seconds_max"] = max(
                        float(_COUNTERS["result_seconds_max"]),
                        elapsed,
                    )

        def format_barrier(*args: Any, **kwargs: Any) -> str:
            rendered = _ORIGINAL_FORMAT_BARRIER(*args, **kwargs)
            diagnostics = result_fastpath_diagnostics()
            count = int(diagnostics["results_recorded"])
            average_ms = (
                (float(diagnostics["result_seconds_total"]) / count) * 1000.0
                if count
                else 0.0
            )
            detail = (
                f"result fastpath avg={average_ms:.0f}ms "
                f"max={float(diagnostics['result_seconds_max']) * 1000.0:.0f}ms "
                f"direct-score={int(diagnostics['direct_scores'])} "
                f"cli-fallback={int(diagnostics['cli_score_fallbacks'])} "
                f"identity-hits={int(diagnostics['identity_cache_hits'])} "
                f"receipt-hits={int(diagnostics['receipt_cache_hits'])} "
                f"receipt-cache={int(diagnostics['receipt_cache_entries'])}/"
                f"{int(diagnostics['receipt_cache_peak_entries'])} "
                f"derived-cache-skips={int(diagnostics['receipt_cache_skipped_derived'])} "
                f"compact-jobs={int(diagnostics['compact_job_artifacts'])}"
            )
            lines = rendered.splitlines()
            if lines:
                lines.insert(-1, play_hand_lab._box_row(detail))
            return "\n".join(lines)

        throughput._compact_pending_sweep_receipt = compact_pending_sweep_receipt
        play_hand_lab._worker_result_identity = worker_result_identity
        play_hand_lab._write_json = write_json
        play_hand_lab._score_lab_artifact = score_lab_artifact
        play_hand_lab._persist_task_result_receipt = persist_task_result_receipt
        play_hand_lab._validate_task_result_receipt = validate_task_result_receipt
        play_hand_lab._write_task_result_receipt = write_task_result_receipt
        play_hand_lab._record_lab_result = record_lab_result
        play_hand_lab._format_lab_barrier_snapshot = format_barrier
        _INSTALLED = True


__all__ = [
    "install_play_hand_result_fastpath",
    "result_fastpath_diagnostics",
]
