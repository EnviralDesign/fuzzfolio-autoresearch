from __future__ import annotations

import gc
import json
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping

from .durable_execution import DurableExecutionJournal


PLAY_HAND_JOURNAL_FILENAME = "play-hand-lab-execution-journal.json"

_LOCK = threading.RLock()
_INSTALLED = False
_PROFILE_BLOB_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_ATTACHED_TASK_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_TRACKED_TASK_PAYLOADS: dict[str, dict[int, dict[str, Any]]] = {}
_TASK_STUB_KEYS = (
    "task_id",
    "lane_id",
    "attempt_id",
    "task_kind",
    "phase",
)

_ORIGINAL_LOAD_PROFILE_BLOB: Any = None
_ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS: Any = None
_ORIGINAL_MAKE_DEEP_REPLAY_TASK: Any = None
_ORIGINAL_MAKE_SWEEP_SHARD_TASKS: Any = None
_ORIGINAL_BUILD_TASKS: Any = None
_ORIGINAL_VALIDATED_RECEIPT_DERIVED_TASKS: Any = None
_ORIGINAL_JOURNAL_UNRESOLVED: Any = None
_ORIGINAL_JOURNAL_TERMINAL: Any = None
_ORIGINAL_JOURNAL_APPLY_BATCH: Any = None


def _campaign_key(campaign_dir: Path) -> str:
    return str(Path(campaign_dir).resolve(strict=False))


def _is_play_hand_journal(journal: DurableExecutionJournal) -> bool:
    return Path(journal.path).name == PLAY_HAND_JOURNAL_FILENAME


def _task_key(task: Mapping[str, Any]) -> str:
    return str(task.get("task_id") or "")


def track_live_task_payloads(tasks: Iterable[dict[str, Any]]) -> None:
    """Track executable task envelopes without changing their serialized content.

    The coordinator's global task list is useful for live-task bookkeeping, but the
    full worker envelopes can be very large. Keep references only until the journal
    has durably recorded completion, then replace every tracked copy with a tiny
    identity stub. Tracking itself is content-neutral and idempotent.
    """

    with _LOCK:
        for task in tasks:
            if not isinstance(task, dict):
                continue
            task_id = _task_key(task)
            if not task_id:
                continue
            _TRACKED_TASK_PAYLOADS.setdefault(task_id, {})[id(task)] = task


def _untrack_task_payloads(tasks: Iterable[dict[str, Any]]) -> None:
    with _LOCK:
        for task in tasks:
            if not isinstance(task, dict):
                continue
            task_id = _task_key(task)
            tracked = _TRACKED_TASK_PAYLOADS.get(task_id)
            if not tracked:
                continue
            tracked.pop(id(task), None)
            if not tracked:
                _TRACKED_TASK_PAYLOADS.pop(task_id, None)


def _compact_task_envelope(task: dict[str, Any]) -> None:
    stub = {key: task[key] for key in _TASK_STUB_KEYS if key in task}
    task.clear()
    task.update(stub)


def release_checkpointed_task_payloads(task_ids: Iterable[str]) -> int:
    """Release full task envelopes after their terminal journal append succeeds.

    The durable journal and lane graph remain authoritative. This only compacts
    process-local copies held by the coordinator's global task projection. Repeated
    calls are safe, and unrelated live tasks are left untouched.
    """

    payloads: list[dict[str, Any]] = []
    with _LOCK:
        for task_id in {str(item) for item in task_ids if str(item)}:
            tracked = _TRACKED_TASK_PAYLOADS.pop(task_id, None)
            if tracked:
                payloads.extend(tracked.values())
    for task in payloads:
        _compact_task_envelope(task)
    return len(payloads)


def _compacted_terminal_ids(journal: DurableExecutionJournal) -> set[str]:
    existing = getattr(journal, "_play_hand_compacted_terminal_ids", None)
    if isinstance(existing, set):
        return existing
    created: set[str] = set()
    setattr(journal, "_play_hand_compacted_terminal_ids", created)
    return created


def _compact_terminal_task(journal: DurableExecutionJournal, task_id: str) -> bool:
    tasks = getattr(journal, "_tasks", None)
    if not isinstance(tasks, dict):
        return False
    task = tasks.get(str(task_id))
    if not isinstance(task, dict) or task.get("status") != "terminal":
        return False

    task.pop("payload", None)
    terminal_receipt = task.get("terminal_receipt")
    if isinstance(terminal_receipt, dict):
        receipt_sha256 = terminal_receipt.get("receipt_sha256")
        task["terminal_receipt"] = {"receipt_sha256": receipt_sha256}
    _compacted_terminal_ids(journal).add(str(task_id))
    return True


def _compact_terminal_cache(journal: DurableExecutionJournal) -> int:
    tasks = getattr(journal, "_tasks", None)
    if not isinstance(tasks, dict):
        return 0
    compacted = 0
    for task_id in list(tasks):
        compacted += int(_compact_terminal_task(journal, str(task_id)))
    return compacted


def _read_task_record_from_disk(
    journal: DurableExecutionJournal,
    task_id: str,
) -> dict[str, Any] | None:
    """Read one full task record without rebuilding the whole journal cache."""
    task_key = str(task_id)
    restored: dict[str, Any] | None = None
    try:
        handle = Path(journal.path).open("r", encoding="utf-8")
    except OSError:
        return None

    with handle:
        for raw in handle:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                return None
            if not isinstance(record, dict) or str(record.get("task_id") or "") != task_key:
                continue
            record_type = record.get("record_type")
            if record_type == "register":
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    return None
                restored = {
                    "task_id": task_key,
                    "payload_sha256": record.get("payload_sha256"),
                    "payload": payload,
                    "status": "pending",
                    "terminal_receipt": None,
                }
            elif record_type == "complete" and restored is not None:
                receipt_payload = record.get("payload")
                if not isinstance(receipt_payload, dict):
                    return None
                restored["status"] = "terminal"
                restored["terminal_receipt"] = {
                    "receipt_sha256": record.get("receipt_sha256"),
                    "payload": receipt_payload,
                }
            elif record_type == "revoke" and restored is not None:
                restored["status"] = "pending"
                restored["terminal_receipt"] = None
    return restored


def _restore_compacted_task(journal: DurableExecutionJournal, task_id: str) -> bool:
    task_key = str(task_id)
    if task_key not in _compacted_terminal_ids(journal):
        return False
    restored = _read_task_record_from_disk(journal, task_key)
    tasks = getattr(journal, "_tasks", None)
    cached = tasks.get(task_key) if isinstance(tasks, dict) else None
    if not isinstance(restored, dict) or not isinstance(cached, dict):
        return False
    cached.clear()
    cached.update(restored)
    _compacted_terminal_ids(journal).discard(task_key)
    return True


def _clone_attached_task(task: Mapping[str, Any]) -> dict[str, Any]:
    """Clone mutable envelope layers while sharing immutable heavy snapshots."""
    cloned = dict(task)
    payload = task.get("payload")
    if isinstance(payload, dict):
        cloned["payload"] = dict(payload)
    return cloned


def _cached_load_profile_blob(campaign_dir: Path, digest: str) -> dict[str, Any]:
    key = (_campaign_key(campaign_dir), str(digest))
    with _LOCK:
        cached = _PROFILE_BLOB_CACHE.get(key)
    if cached is None:
        loaded = _ORIGINAL_LOAD_PROFILE_BLOB(campaign_dir, digest)
        with _LOCK:
            cached = _PROFILE_BLOB_CACHE.setdefault(key, loaded)
    return cached


def _cached_attach_task_profile_snapshots(
    task: dict[str, Any],
    campaign_dir: Path,
) -> dict[str, Any]:
    task_id = str(task.get("task_id") or "")
    if not task_id:
        return _ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS(task, campaign_dir)
    key = (_campaign_key(campaign_dir), task_id)
    with _LOCK:
        cached = _ATTACHED_TASK_CACHE.get(key)
    if cached is None:
        attached = _ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS(task, campaign_dir)
        with _LOCK:
            cached = _ATTACHED_TASK_CACHE.setdefault(key, attached)
    cloned = _clone_attached_task(cached)
    track_live_task_payloads([cloned])
    return cloned


def release_resume_enqueue_memory(tasks: list[dict[str, Any]]) -> None:
    """Release the transient second resume list after synchronous enqueueing."""
    _untrack_task_payloads(tasks)
    tasks.clear()
    with _LOCK:
        _ATTACHED_TASK_CACHE.clear()
        _PROFILE_BLOB_CACHE.clear()
    gc.collect()


def install_play_hand_lab_memory_bounds() -> None:
    """Install in-process-only compaction without changing durable payloads."""
    global _INSTALLED
    global _ORIGINAL_LOAD_PROFILE_BLOB
    global _ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS
    global _ORIGINAL_MAKE_DEEP_REPLAY_TASK
    global _ORIGINAL_MAKE_SWEEP_SHARD_TASKS
    global _ORIGINAL_BUILD_TASKS
    global _ORIGINAL_VALIDATED_RECEIPT_DERIVED_TASKS
    global _ORIGINAL_JOURNAL_UNRESOLVED
    global _ORIGINAL_JOURNAL_TERMINAL
    global _ORIGINAL_JOURNAL_APPLY_BATCH

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab

        _ORIGINAL_LOAD_PROFILE_BLOB = play_hand_lab._load_profile_blob
        _ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS = play_hand_lab._attach_task_profile_snapshots
        _ORIGINAL_MAKE_DEEP_REPLAY_TASK = play_hand_lab._make_deep_replay_task
        _ORIGINAL_MAKE_SWEEP_SHARD_TASKS = play_hand_lab._make_sweep_shard_tasks
        _ORIGINAL_BUILD_TASKS = play_hand_lab._build_tasks
        _ORIGINAL_VALIDATED_RECEIPT_DERIVED_TASKS = (
            play_hand_lab._validated_receipt_derived_tasks
        )
        _ORIGINAL_JOURNAL_UNRESOLVED = DurableExecutionJournal.unresolved
        _ORIGINAL_JOURNAL_TERMINAL = DurableExecutionJournal.terminal
        _ORIGINAL_JOURNAL_APPLY_BATCH = DurableExecutionJournal.apply_batch

        def tracked_make_deep_replay_task(*args: Any, **kwargs: Any) -> dict[str, Any]:
            task = _ORIGINAL_MAKE_DEEP_REPLAY_TASK(*args, **kwargs)
            track_live_task_payloads([task])
            return task

        def tracked_make_sweep_shard_tasks(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            tasks = _ORIGINAL_MAKE_SWEEP_SHARD_TASKS(*args, **kwargs)
            track_live_task_payloads(tasks)
            return tasks

        def tracked_build_tasks(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            tasks = _ORIGINAL_BUILD_TASKS(*args, **kwargs)
            track_live_task_payloads(tasks)
            return tasks

        def tracked_validated_receipt_derived_tasks(
            *args: Any,
            **kwargs: Any,
        ) -> list[dict[str, Any]] | None:
            tasks = _ORIGINAL_VALIDATED_RECEIPT_DERIVED_TASKS(*args, **kwargs)
            if tasks:
                track_live_task_payloads(tasks)
            return tasks

        def bounded_unresolved(journal: DurableExecutionJournal) -> list[dict[str, Any]]:
            unresolved = _ORIGINAL_JOURNAL_UNRESOLVED(journal)
            if _is_play_hand_journal(journal):
                setattr(journal, "_play_hand_memory_compaction_enabled", True)
                _compact_terminal_cache(journal)
                gc.collect()
            return unresolved

        def bounded_terminal(
            journal: DurableExecutionJournal,
            task_id: str,
        ) -> dict[str, Any] | None:
            task_key = str(task_id)
            if _is_play_hand_journal(journal) and task_key in _compacted_terminal_ids(journal):
                restored = _read_task_record_from_disk(journal, task_key)
                if isinstance(restored, dict) and restored.get("status") == "terminal":
                    return dict(restored)
                return None
            return _ORIGINAL_JOURNAL_TERMINAL(journal, task_key)

        def bounded_apply_batch(
            journal: DurableExecutionJournal,
            *,
            registrations: Iterable[tuple[str, Mapping[str, Any]]] = (),
            completions: Iterable[tuple[str, Mapping[str, Any]]] = (),
            revocations: Iterable[str] = (),
        ) -> dict[str, Any]:
            registration_rows = list(registrations)
            completion_rows = list(completions)
            revocation_rows = [str(task_id) for task_id in revocations]
            play_hand_journal = _is_play_hand_journal(journal)
            enabled = bool(
                play_hand_journal
                or getattr(journal, "_play_hand_memory_compaction_enabled", False)
            )
            if play_hand_journal:
                setattr(journal, "_play_hand_memory_compaction_enabled", True)
            if enabled:
                for task_id in revocation_rows:
                    _restore_compacted_task(journal, task_id)
            result = _ORIGINAL_JOURNAL_APPLY_BATCH(
                journal,
                registrations=registration_rows,
                completions=completion_rows,
                revocations=revocation_rows,
            )
            if enabled:
                completed_task_ids: list[str] = []
                for task_id, _receipt in completion_rows:
                    task_key = str(task_id)
                    _compact_terminal_task(journal, task_key)
                    completed_task_ids.append(task_key)
                release_checkpointed_task_payloads(completed_task_ids)
            return result

        play_hand_lab._load_profile_blob = _cached_load_profile_blob
        play_hand_lab._attach_task_profile_snapshots = _cached_attach_task_profile_snapshots
        play_hand_lab._make_deep_replay_task = tracked_make_deep_replay_task
        play_hand_lab._make_sweep_shard_tasks = tracked_make_sweep_shard_tasks
        play_hand_lab._build_tasks = tracked_build_tasks
        play_hand_lab._validated_receipt_derived_tasks = (
            tracked_validated_receipt_derived_tasks
        )
        DurableExecutionJournal.unresolved = bounded_unresolved
        DurableExecutionJournal.terminal = bounded_terminal
        DurableExecutionJournal.apply_batch = bounded_apply_batch
        _INSTALLED = True


__all__ = [
    "PLAY_HAND_JOURNAL_FILENAME",
    "install_play_hand_lab_memory_bounds",
    "release_checkpointed_task_payloads",
    "release_resume_enqueue_memory",
    "track_live_task_payloads",
]
