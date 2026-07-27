from __future__ import annotations

import copy
import ctypes
import gc
import json
import os
import threading
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from . import play_hand_lab_memory as base_memory
from .durable_execution import (
    JOURNAL_SCHEMA,
    DurableExecutionError,
    DurableExecutionJournal,
    _record_sha256,
)
from .evidence_plan import canonical_sha256


_LOCK = threading.RLock()
_INSTALLED = False
_ACTIVE_PLAY_HAND_JOURNAL: DurableExecutionJournal | None = None
_RECORDED_SAMPLE_COUNT = 0
_RECORDED_SWEEP_SAMPLE_COUNT = 0
_RECORDED_SAMPLE_APPROX_BYTES = 0

_ORIGINAL_JOURNAL_LOAD_FROM_DISK: Any = None
_ORIGINAL_JOURNAL_LOAD: Any = None
_ORIGINAL_ADD_RECORDED_RESULT_SAMPLE: Any = None
_ORIGINAL_FORMAT_LAB_BARRIER_SNAPSHOT: Any = None
_ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD: Any = None
_ORIGINAL_HYDRATE_UNRESOLVED_LANE_TASK_SPECS: Any = None
_ORIGINAL_ADVANCE_LANE_AFTER_RESULT: Any = None
_ORIGINAL_MERGE_PHASE_SWEEP_RECEIPTS: Any = None
_ORIGINAL_READ_TASK_RECORD_FROM_DISK: Any = None

_SWEEP_PHASES = frozenset(
    {"lookback_timing", "coarse", "coarse_probe", "coarse_expand", "focused"}
)
_SWEEP_SPEC_RETAIN_KEYS = frozenset(
    {
        "phase",
        "task_kind",
        "sweep_id",
        "shard_id",
        "axes",
        "axis_key_map",
        "axis_plan",
        "expanded_permutation_count",
        "permutation_budget_applied",
        "permutation_start",
        "permutation_count",
        "params_by_index_sha256",
        "result_detail",
    }
)
_RECORDED_SAMPLE_KEYS = (
    "task_id",
    "attempt_id",
    "artifact_dir",
    "score",
    "score_basis",
    "status",
    "phase",
    "task_kind",
    "profile_path",
    "profile_ref",
    "instruments",
    "timeframe",
    "lookback_months",
    "analysis_window_start",
    "analysis_window_end",
    "evidence_plan_id",
    "evidence_role",
    "policy_assignment",
)


def _is_play_hand_journal(journal: DurableExecutionJournal) -> bool:
    return Path(journal.path).name == base_memory.PLAY_HAND_JOURNAL_FILENAME


def _decode_json_record(raw: bytes, *, path: Path) -> dict[str, Any]:
    try:
        record = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    if not isinstance(record, dict):
        raise DurableExecutionError("execution journal record is malformed")
    return record


def _read_record_at_offset(path: Path, offset: int) -> dict[str, Any]:
    try:
        with Path(path).open("rb") as handle:
            handle.seek(int(offset))
            raw = handle.readline()
    except OSError as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    if not raw:
        raise DurableExecutionError(f"execution journal record offset is invalid: {path}")
    record = _decode_json_record(raw, path=Path(path))
    if record.get("record_sha256") != _record_sha256(record):
        raise DurableExecutionError("execution journal record identity mismatch")
    return record


def _restore_register_payload(
    journal: DurableExecutionJournal,
    task_id: str,
    *,
    expected_payload_sha256: str | None = None,
) -> dict[str, Any]:
    offsets = getattr(journal, "_play_hand_register_offsets", None)
    offset = offsets.get(str(task_id)) if isinstance(offsets, dict) else None
    if offset is None:
        restored = _ORIGINAL_READ_TASK_RECORD_FROM_DISK(journal, str(task_id))
        if not isinstance(restored, dict) or not isinstance(restored.get("payload"), dict):
            raise DurableExecutionError(f"durable task payload is missing: {task_id}")
        payload = dict(restored["payload"])
        if (
            expected_payload_sha256
            and journal.task_payload_sha256(payload) != expected_payload_sha256
        ):
            raise DurableExecutionError(f"durable task payload conflicts: {task_id}")
        return payload
    record = _read_record_at_offset(journal.path, int(offset))
    payload = record.get("payload")
    payload_sha256 = str(record.get("payload_sha256") or "")
    if (
        record.get("record_type") != "register"
        or str(record.get("task_id") or "") != str(task_id)
        or not isinstance(payload, dict)
        or not payload_sha256
        or (expected_payload_sha256 and payload_sha256 != expected_payload_sha256)
    ):
        raise DurableExecutionError(f"durable task payload conflicts: {task_id}")
    return payload


def _full_terminal_task_from_offsets(
    journal: DurableExecutionJournal,
    task_id: str,
) -> dict[str, Any] | None:
    task_key = str(task_id)
    tasks = getattr(journal, "_tasks", None)
    cached = tasks.get(task_key) if isinstance(tasks, dict) else None
    if not isinstance(cached, dict) or cached.get("status") != "terminal":
        return None

    register_offsets = getattr(journal, "_play_hand_register_offsets", None)
    complete_offsets = getattr(journal, "_play_hand_complete_offsets", None)
    register_offset = (
        register_offsets.get(task_key) if isinstance(register_offsets, dict) else None
    )
    complete_offset = (
        complete_offsets.get(task_key) if isinstance(complete_offsets, dict) else None
    )
    if register_offset is None or complete_offset is None:
        return _ORIGINAL_READ_TASK_RECORD_FROM_DISK(journal, task_key)

    register = _read_record_at_offset(journal.path, int(register_offset))
    complete = _read_record_at_offset(journal.path, int(complete_offset))
    payload = register.get("payload")
    terminal_payload = complete.get("payload")
    if (
        register.get("record_type") != "register"
        or complete.get("record_type") != "complete"
        or str(register.get("task_id") or "") != task_key
        or str(complete.get("task_id") or "") != task_key
        or not isinstance(payload, dict)
        or not isinstance(terminal_payload, dict)
        or str(register.get("payload_sha256") or "")
        != str(cached.get("payload_sha256") or "")
        or str(complete.get("receipt_sha256") or "")
        != str((cached.get("terminal_receipt") or {}).get("receipt_sha256") or "")
    ):
        raise DurableExecutionError(f"execution journal terminal record conflicts: {task_key}")
    return {
        "task_id": task_key,
        "payload_sha256": register.get("payload_sha256"),
        "payload": payload,
        "status": "terminal",
        "terminal_receipt": {
            "receipt_sha256": complete.get("receipt_sha256"),
            "payload": terminal_payload,
        },
    }


def _load_play_hand_journal_streaming(
    journal: DurableExecutionJournal,
) -> dict[str, Any]:
    """Replay the journal without retaining historical terminal payloads.

    The generic loader reads the entire JSONL file into one giant string, splits it
    into a second giant list, and retains every register and completion payload until
    a later compaction pass. Formal campaigns can therefore reserve many gigabytes
    before the coordinator starts. This loader validates the same records one line at
    a time and drops a task's heavy payload as soon as its terminal record is seen.
    Register/complete offsets preserve exact lazy restoration.
    """

    global _ACTIVE_PLAY_HAND_JOURNAL

    path = Path(journal.path)
    tasks: dict[str, dict[str, Any]] = {}
    register_offsets: dict[str, int] = {}
    complete_offsets: dict[str, int] = {}
    header: dict[str, Any] | None = None
    compacted_ids: set[str] = set()

    try:
        handle = path.open("rb")
    except OSError as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc

    with handle:
        while True:
            offset = handle.tell()
            raw = handle.readline()
            if not raw:
                break
            if not raw.strip():
                continue
            record = _decode_json_record(raw, path=path)
            record_type = record.get("record_type")
            if record_type == "header":
                if header is not None:
                    raise DurableExecutionError("execution journal has duplicate header")
                if record.get("schema_version") != JOURNAL_SCHEMA:
                    return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
                if record.get("header_sha256") != _record_sha256(record):
                    raise DurableExecutionError("execution journal header identity mismatch")
                header = record
                continue
            if header is None:
                return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
            if record.get("record_sha256") != _record_sha256(record):
                raise DurableExecutionError("execution journal record identity mismatch")

            if record_type == "register":
                task_key = str(record.get("task_id") or "")
                payload = record.get("payload")
                payload_sha256 = record.get("payload_sha256")
                if (
                    not task_key
                    or not isinstance(payload, dict)
                    or not isinstance(payload_sha256, str)
                ):
                    raise DurableExecutionError(
                        "execution journal register record is malformed"
                    )
                existing = tasks.get(task_key)
                if existing is not None:
                    if existing.get("payload_sha256") != payload_sha256:
                        raise DurableExecutionError(
                            f"task payload conflicts with durable graph: {task_key}"
                        )
                    continue
                register_offsets[task_key] = int(offset)
                tasks[task_key] = {
                    "task_id": task_key,
                    "payload_sha256": payload_sha256,
                    "payload": payload,
                    "status": "pending",
                    "terminal_receipt": None,
                }
                continue

            if record_type == "complete":
                task_key = str(record.get("task_id") or "")
                terminal_payload = record.get("payload")
                receipt_sha256 = record.get("receipt_sha256")
                task = tasks.get(task_key)
                if not isinstance(task, dict):
                    raise DurableExecutionError(
                        f"terminal receipt references unknown task: {task_key}"
                    )
                if (
                    not isinstance(terminal_payload, dict)
                    or not isinstance(receipt_sha256, str)
                ):
                    raise DurableExecutionError(
                        "execution journal complete record is malformed"
                    )
                if task.get("status") == "terminal":
                    existing = task.get("terminal_receipt")
                    if (
                        not isinstance(existing, dict)
                        or existing.get("receipt_sha256") != receipt_sha256
                    ):
                        raise DurableExecutionError(
                            f"conflicting duplicate terminal receipt: {task_key}"
                        )
                    continue
                task.pop("payload", None)
                task["status"] = "terminal"
                task["terminal_receipt"] = {"receipt_sha256": receipt_sha256}
                complete_offsets[task_key] = int(offset)
                compacted_ids.add(task_key)
                continue

            if record_type == "revoke":
                task_key = str(record.get("task_id") or "")
                task = tasks.get(task_key)
                if not isinstance(task, dict):
                    raise DurableExecutionError(
                        f"revoke references unknown task: {task_key}"
                    )
                if task.get("status") != "terminal":
                    continue
                register_offset = register_offsets.get(task_key)
                if register_offset is None:
                    raise DurableExecutionError(
                        f"revoke has no register record offset: {task_key}"
                    )
                register = _read_record_at_offset(path, int(register_offset))
                payload = register.get("payload")
                if (
                    register.get("record_type") != "register"
                    or str(register.get("task_id") or "") != task_key
                    or not isinstance(payload, dict)
                    or str(register.get("payload_sha256") or "")
                    != str(task.get("payload_sha256") or "")
                ):
                    raise DurableExecutionError(
                        f"revoke register payload conflicts: {task_key}"
                    )
                task["payload"] = payload
                task["status"] = "pending"
                task["terminal_receipt"] = None
                complete_offsets.pop(task_key, None)
                compacted_ids.discard(task_key)
                continue

            raise DurableExecutionError(
                f"execution journal has unknown record_type: {record_type!r}"
            )

    if header is None:
        raise DurableExecutionError("execution journal is missing header")
    if (
        header.get("execution_id") != journal.execution_id
        or header.get("lineage") != journal.lineage
    ):
        raise DurableExecutionError("execution journal lineage mismatch")

    journal._header_sha256 = str(header["header_sha256"])
    journal._tasks = tasks
    setattr(journal, "_play_hand_register_offsets", register_offsets)
    setattr(journal, "_play_hand_complete_offsets", complete_offsets)
    setattr(journal, "_play_hand_compacted_terminal_ids", compacted_ids)
    setattr(journal, "_play_hand_memory_compaction_enabled", True)
    _ACTIVE_PLAY_HAND_JOURNAL = journal
    return journal._view()


def _compact_sweep_task_spec(spec: dict[str, Any]) -> None:
    if str(spec.get("task_kind") or "") != "sweep_shard":
        return
    compacted = {key: spec[key] for key in _SWEEP_SPEC_RETAIN_KEYS if key in spec}
    spec.clear()
    spec.update(compacted)


def _restore_sealed_sweep_params(
    play_hand_lab: Any,
    lane: Any,
    task_id: str,
    spec: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Restore a compacted shard from its sealed registration when available."""
    journal = _ACTIVE_PLAY_HAND_JOURNAL
    tasks = getattr(journal, "_tasks", None) if journal is not None else None
    durable_task = tasks.get(str(task_id)) if isinstance(tasks, dict) else None
    if not isinstance(durable_task, dict):
        return None

    expected_payload_sha256 = durable_task.get("payload_sha256")
    if not isinstance(expected_payload_sha256, str) or not expected_payload_sha256:
        raise DurableExecutionError(f"durable sweep task has no payload identity: {task_id}")
    envelope = _restore_register_payload(
        journal,
        str(task_id),
        expected_payload_sha256=expected_payload_sha256,
    )
    if (
        str(envelope.get("task_id") or "") != str(task_id)
        or str(envelope.get("lane_id") or "") != str(lane.lane_id)
        or str(envelope.get("task_kind") or "") != "sweep_shard"
    ):
        raise DurableExecutionError(f"durable sweep task identity mismatch: {task_id}")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise DurableExecutionError(f"durable sweep task has no params: {task_id}")
    for key in ("sweep_id", "shard_id"):
        expected = spec.get(key)
        if expected is not None and payload.get(key) != expected:
            raise DurableExecutionError(f"durable sweep task identity mismatch: {task_id}")
    params_by_index = payload.get("params_by_index")
    if not isinstance(params_by_index, dict):
        raise DurableExecutionError(f"durable sweep task has no params: {task_id}")
    canonical_params = play_hand_lab._canonical_params(params_by_index)
    expected_sha = spec.get("params_by_index_sha256")
    if not isinstance(expected_sha, str) or not expected_sha:
        raise DurableExecutionError(f"durable sweep task has no params identity: {task_id}")
    observed_sha = canonical_sha256(canonical_params)
    if observed_sha != expected_sha:
        raise DurableExecutionError(f"sealed sweep params conflict with task identity: {task_id}")
    return canonical_params


def _restore_sweep_params(play_hand_lab: Any, lane: Any, phase: str) -> list[str]:
    restored: list[str] = []
    for task_id in lane.phase_task_ids.get(phase) or []:
        spec = lane.task_specs.get(task_id)
        if not isinstance(spec, dict) or spec.get("task_kind") != "sweep_shard":
            continue
        if isinstance(spec.get("params_by_index"), dict):
            continue
        restored_params = _restore_sealed_sweep_params(
            play_hand_lab,
            lane,
            str(task_id),
            spec,
        )
        if restored_params is None:
            restored_params = play_hand_lab._rebuild_sweep_shard_params_by_index(lane, spec)
        if restored_params is None:
            raise DurableExecutionError(
                f"persisted sweep task is missing reconstructable params: {task_id}"
            )
        expected_sha = spec.get("params_by_index_sha256")
        observed_sha = canonical_sha256(play_hand_lab._canonical_params(restored_params))
        if isinstance(expected_sha, str) and expected_sha and observed_sha != expected_sha:
            raise DurableExecutionError(
                f"rebuilt sweep params conflict with task identity: {task_id}"
            )
        spec["params_by_index"] = restored_params
        restored.append(str(task_id))
    return restored


def _compact_recorded_result_sample(recorded: Mapping[str, Any]) -> dict[str, Any]:
    sample: dict[str, Any] = {}
    for key in _RECORDED_SAMPLE_KEYS:
        value = recorded.get(key)
        if value is None:
            continue
        sample[key] = copy.deepcopy(value)

    sweep = recorded.get("sweep_payload")
    if isinstance(sweep, dict):
        ranked = sweep.get("ranked_permutations") or sweep.get("ranked") or []
        failed = sweep.get("failed_permutations") or []
        best = sweep.get("best") if isinstance(sweep.get("best"), dict) else None
        compact_best = None
        if best is not None:
            compact_best = {
                key: copy.deepcopy(best.get(key))
                for key in (
                    "permutation_index",
                    "child_job_id",
                    "status",
                    "score",
                    "fitness_value",
                    "parameters",
                )
                if best.get(key) is not None
            }
        sample["sweep_summary"] = {
            "sweep_id": sweep.get("sweep_id"),
            "shard_id": sweep.get("shard_id"),
            "outcome": sweep.get("outcome"),
            "permutation_count": len(sweep.get("permutation_indices") or []),
            "scored_count": len(ranked) if isinstance(ranked, list) else 0,
            "failed_count": len(failed) if isinstance(failed, list) else 0,
            "best": compact_best,
        }
    return sample


def _process_memory_bytes() -> tuple[int | None, int | None]:
    """Return (working_set, private_commit) using only the standard library."""

    if os.name == "nt":
        try:
            from ctypes import wintypes

            size_t = ctypes.c_size_t

            class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", size_t),
                    ("WorkingSetSize", size_t),
                    ("QuotaPeakPagedPoolUsage", size_t),
                    ("QuotaPagedPoolUsage", size_t),
                    ("QuotaPeakNonPagedPoolUsage", size_t),
                    ("QuotaNonPagedPoolUsage", size_t),
                    ("PagefileUsage", size_t),
                    ("PeakPagefileUsage", size_t),
                    ("PrivateUsage", size_t),
                ]

            counters = PROCESS_MEMORY_COUNTERS_EX()
            counters.cb = ctypes.sizeof(counters)
            process = ctypes.windll.kernel32.GetCurrentProcess()
            ok = ctypes.windll.psapi.GetProcessMemoryInfo(
                process,
                ctypes.byref(counters),
                counters.cb,
            )
            if ok:
                return int(counters.WorkingSetSize), int(counters.PrivateUsage)
        except Exception:
            return None, None

    try:
        values: dict[str, int] = {}
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith(("VmRSS:", "VmData:")):
                key, raw = line.split(":", 1)
                values[key] = int(raw.strip().split()[0]) * 1024
        return values.get("VmRSS"), values.get("VmData")
    except Exception:
        return None, None


def memory_diagnostics() -> dict[str, Any]:
    working_set, private_commit = _process_memory_bytes()
    with base_memory._LOCK:
        tracked_task_ids = len(base_memory._TRACKED_TASK_PAYLOADS)
        tracked_copies = sum(
            len(copies) for copies in base_memory._TRACKED_TASK_PAYLOADS.values()
        )
        profile_cache = len(base_memory._PROFILE_BLOB_CACHE)
        attached_cache = len(base_memory._ATTACHED_TASK_CACHE)
    journal = _ACTIVE_PLAY_HAND_JOURNAL
    compacted_terminal = 0
    if journal is not None:
        compacted = getattr(journal, "_play_hand_compacted_terminal_ids", None)
        compacted_terminal = len(compacted) if isinstance(compacted, set) else 0
    with _LOCK:
        return {
            "working_set_bytes": working_set,
            "private_commit_bytes": private_commit,
            "tracked_task_ids": tracked_task_ids,
            "tracked_task_copies": tracked_copies,
            "profile_cache_entries": profile_cache,
            "attached_cache_entries": attached_cache,
            "compacted_terminal_tasks": compacted_terminal,
            "recorded_sample_count": _RECORDED_SAMPLE_COUNT,
            "recorded_sweep_sample_count": _RECORDED_SWEEP_SAMPLE_COUNT,
            "recorded_sample_approx_bytes": _RECORDED_SAMPLE_APPROX_BYTES,
        }


def _mb(value: int | None) -> str:
    if value is None:
        return "?"
    return f"{value / (1024 * 1024):.0f}"


def install_play_hand_lab_deep_memory_bounds() -> None:
    """Prevent historical journal, live envelopes, and summary samples from pinning RAM."""

    global _INSTALLED
    global _ORIGINAL_JOURNAL_LOAD_FROM_DISK
    global _ORIGINAL_JOURNAL_LOAD
    global _ORIGINAL_ADD_RECORDED_RESULT_SAMPLE
    global _ORIGINAL_FORMAT_LAB_BARRIER_SNAPSHOT
    global _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD
    global _ORIGINAL_HYDRATE_UNRESOLVED_LANE_TASK_SPECS
    global _ORIGINAL_ADVANCE_LANE_AFTER_RESULT
    global _ORIGINAL_MERGE_PHASE_SWEEP_RECEIPTS
    global _ORIGINAL_READ_TASK_RECORD_FROM_DISK

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab

        _ORIGINAL_JOURNAL_LOAD_FROM_DISK = DurableExecutionJournal._load_from_disk
        _ORIGINAL_JOURNAL_LOAD = DurableExecutionJournal.load
        _ORIGINAL_READ_TASK_RECORD_FROM_DISK = (
            base_memory._read_task_record_from_disk
        )
        _ORIGINAL_ADD_RECORDED_RESULT_SAMPLE = play_hand_lab._add_recorded_result_sample
        _ORIGINAL_FORMAT_LAB_BARRIER_SNAPSHOT = play_hand_lab._format_lab_barrier_snapshot
        _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD = (
            play_hand_lab._validate_task_result_receipt_payload
        )
        _ORIGINAL_HYDRATE_UNRESOLVED_LANE_TASK_SPECS = (
            play_hand_lab._hydrate_unresolved_lane_task_specs
        )
        _ORIGINAL_ADVANCE_LANE_AFTER_RESULT = play_hand_lab._advance_lane_after_result
        _ORIGINAL_MERGE_PHASE_SWEEP_RECEIPTS = (
            play_hand_lab._merge_phase_sweep_receipts
        )

        def load_from_disk(journal: DurableExecutionJournal) -> dict[str, Any]:
            if not _is_play_hand_journal(journal):
                return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
            return _load_play_hand_journal_streaming(journal)

        def load(journal: DurableExecutionJournal, *args: Any, **kwargs: Any) -> dict[str, Any]:
            global _ACTIVE_PLAY_HAND_JOURNAL
            result = _ORIGINAL_JOURNAL_LOAD(journal, *args, **kwargs)
            if _is_play_hand_journal(journal):
                _ACTIVE_PLAY_HAND_JOURNAL = journal
            return result

        def validate_task_result_receipt_payload(
            receipt: Any,
            *,
            task_id: str,
            worker_result_sha256: str | None = None,
        ) -> dict[str, Any]:
            resolved = receipt
            journal = _ACTIVE_PLAY_HAND_JOURNAL
            if resolved is None and journal is not None:
                terminal = _full_terminal_task_from_offsets(journal, task_id)
                terminal_receipt = (
                    terminal.get("terminal_receipt")
                    if isinstance(terminal, dict)
                    else None
                )
                if isinstance(terminal_receipt, dict):
                    resolved = terminal_receipt.get("payload")
            return _ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT_PAYLOAD(
                resolved,
                task_id=task_id,
                worker_result_sha256=worker_result_sha256,
            )

        class LazyTerminalTaskMapping(Mapping[str, Any]):
            def __init__(
                self,
                source: Mapping[str, Any],
                journal: DurableExecutionJournal | None,
            ) -> None:
                self._source = source
                self._journal = journal

            def __getitem__(self, key: str) -> Any:
                value = self._source[key]
                if (
                    self._journal is not None
                    and isinstance(value, dict)
                    and value.get("status") == "terminal"
                    and not isinstance(value.get("payload"), dict)
                ):
                    restored = _full_terminal_task_from_offsets(self._journal, str(key))
                    if restored is not None:
                        return restored
                return value

            def get(self, key: str, default: Any = None) -> Any:
                try:
                    return self[key]
                except KeyError:
                    return default

            def __iter__(self) -> Iterator[str]:
                return iter(self._source)

            def __len__(self) -> int:
                return len(self._source)

        def hydrate_unresolved_lane_task_specs(
            lanes: list[Any],
            durable_tasks_by_id: Mapping[str, Any],
        ) -> None:
            _ORIGINAL_HYDRATE_UNRESOLVED_LANE_TASK_SPECS(
                lanes,
                LazyTerminalTaskMapping(
                    durable_tasks_by_id,
                    _ACTIVE_PLAY_HAND_JOURNAL,
                ),
            )
            for lane in lanes:
                for task_id, spec in list(lane.task_specs.items()):
                    if (
                        task_id in lane.completed_task_ids
                        or task_id in lane.failed_task_ids
                    ):
                        phase = str(spec.get("phase") or "") if isinstance(spec, dict) else ""
                        if phase and not play_hand_lab._phase_terminal(lane, phase):
                            _compact_sweep_task_spec(spec)

        def advance_lane_after_result(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            result = _ORIGINAL_ADVANCE_LANE_AFTER_RESULT(*args, **kwargs)
            lane = kwargs.get("lane")
            recorded = kwargs.get("recorded")
            if lane is not None and isinstance(recorded, dict):
                task_id = str(recorded.get("task_id") or "")
                phase = str(recorded.get("phase") or "")
                if (
                    task_id
                    and phase in _SWEEP_PHASES
                    and not play_hand_lab._phase_terminal(lane, phase)
                ):
                    spec = lane.task_specs.get(task_id)
                    if isinstance(spec, dict):
                        _compact_sweep_task_spec(spec)
            return result

        def merge_phase_sweep_receipts(lane: Any, *, phase: str) -> dict[str, Any]:
            restored = _restore_sweep_params(play_hand_lab, lane, phase)
            try:
                return _ORIGINAL_MERGE_PHASE_SWEEP_RECEIPTS(lane, phase=phase)
            finally:
                for task_id in restored:
                    spec = lane.task_specs.get(task_id)
                    if isinstance(spec, dict):
                        spec.pop("params_by_index", None)

        def add_recorded_result_sample(
            recorded_results: list[dict[str, Any]],
            recorded: dict[str, Any],
        ) -> None:
            global _RECORDED_SAMPLE_COUNT
            global _RECORDED_SWEEP_SAMPLE_COUNT
            global _RECORDED_SAMPLE_APPROX_BYTES

            limit = max(int(play_hand_lab.SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT), 0)
            if len(recorded_results) >= limit:
                return
            sample = _compact_recorded_result_sample(recorded)
            recorded_results.append(sample)
            encoded_size = len(
                json.dumps(sample, ensure_ascii=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            with _LOCK:
                _RECORDED_SAMPLE_COUNT += 1
                _RECORDED_SAMPLE_APPROX_BYTES += encoded_size
                if "sweep_summary" in sample:
                    _RECORDED_SWEEP_SAMPLE_COUNT += 1

        def format_lab_barrier_snapshot(*args: Any, **kwargs: Any) -> str:
            rendered = _ORIGINAL_FORMAT_LAB_BARRIER_SNAPSHOT(*args, **kwargs)
            diagnostics = memory_diagnostics()
            detail = (
                "coordinator memory "
                f"rss={_mb(diagnostics['working_set_bytes'])}MB "
                f"private={_mb(diagnostics['private_commit_bytes'])}MB "
                f"live-task-payloads={diagnostics['tracked_task_ids']}/"
                f"{diagnostics['tracked_task_copies']} "
                f"journal-stubs={diagnostics['compacted_terminal_tasks']} "
                f"samples={diagnostics['recorded_sample_count']} "
                f"sample-bytes={_mb(diagnostics['recorded_sample_approx_bytes'])}MB"
            )
            lines = rendered.splitlines()
            if lines:
                lines.insert(-1, play_hand_lab._box_row(detail))
            return "\n".join(lines)

        DurableExecutionJournal._load_from_disk = load_from_disk
        DurableExecutionJournal.load = load
        base_memory._read_task_record_from_disk = _full_terminal_task_from_offsets
        play_hand_lab._validate_task_result_receipt_payload = (
            validate_task_result_receipt_payload
        )
        play_hand_lab._hydrate_unresolved_lane_task_specs = (
            hydrate_unresolved_lane_task_specs
        )
        play_hand_lab._advance_lane_after_result = advance_lane_after_result
        play_hand_lab._merge_phase_sweep_receipts = merge_phase_sweep_receipts
        play_hand_lab._add_recorded_result_sample = add_recorded_result_sample
        play_hand_lab._format_lab_barrier_snapshot = format_lab_barrier_snapshot
        _INSTALLED = True
        gc.collect()


__all__ = [
    "install_play_hand_lab_deep_memory_bounds",
    "memory_diagnostics",
]
