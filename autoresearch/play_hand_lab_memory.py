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

_ORIGINAL_LOAD_PROFILE_BLOB: Any = None
_ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS: Any = None
_ORIGINAL_JOURNAL_UNRESOLVED: Any = None
_ORIGINAL_JOURNAL_TERMINAL: Any = None
_ORIGINAL_JOURNAL_APPLY_BATCH: Any = None


def _campaign_key(campaign_dir: Path) -> str:
    return str(Path(campaign_dir).resolve(strict=False))


def _is_play_hand_journal(journal: DurableExecutionJournal) -> bool:
    return Path(journal.path).name == PLAY_HAND_JOURNAL_FILENAME


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
    return _clone_attached_task(cached)


def release_resume_enqueue_memory(tasks: list[dict[str, Any]]) -> None:
    """Release the transient second resume list after synchronous enqueueing."""
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
    global _ORIGINAL_JOURNAL_UNRESOLVED
    global _ORIGINAL_JOURNAL_TERMINAL
    global _ORIGINAL_JOURNAL_APPLY_BATCH

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab

        _ORIGINAL_LOAD_PROFILE_BLOB = play_hand_lab._load_profile_blob
        _ORIGINAL_ATTACH_TASK_PROFILE_SNAPSHOTS = play_hand_lab._attach_task_profile_snapshots
        _ORIGINAL_JOURNAL_UNRESOLVED = DurableExecutionJournal.unresolved
        _ORIGINAL_JOURNAL_TERMINAL = DurableExecutionJournal.terminal
        _ORIGINAL_JOURNAL_APPLY_BATCH = DurableExecutionJournal.apply_batch

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
            enabled = bool(getattr(journal, "_play_hand_memory_compaction_enabled", False))
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
                for task_id, _receipt in completion_rows:
                    _compact_terminal_task(journal, str(task_id))
            return result

        play_hand_lab._load_profile_blob = _cached_load_profile_blob
        play_hand_lab._attach_task_profile_snapshots = _cached_attach_task_profile_snapshots
        DurableExecutionJournal.unresolved = bounded_unresolved
        DurableExecutionJournal.terminal = bounded_terminal
        DurableExecutionJournal.apply_batch = bounded_apply_batch
        _INSTALLED = True


__all__ = [
    "PLAY_HAND_JOURNAL_FILENAME",
    "install_play_hand_lab_memory_bounds",
    "release_resume_enqueue_memory",
]
