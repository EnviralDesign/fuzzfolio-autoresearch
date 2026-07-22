"""Atomic, content-addressed execution journals for formal research work."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import canonical_json, canonical_sha256


JOURNAL_SCHEMA = "autoresearch-durable-execution-v1"
ATOMIC_REPLACE_ATTEMPTS = 64
ATOMIC_REPLACE_RETRY_SECONDS = 0.025
ATOMIC_REPLACE_MAX_RETRY_SECONDS = 0.5


class DurableExecutionError(RuntimeError):
    """Raised when durable execution state is missing, partial, or conflicting."""


def _canonical_snapshot(payload: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    """Detach journal inputs from caller-owned nested data before persistence."""
    try:
        snapshot = json.loads(canonical_json(dict(payload)))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DurableExecutionError(f"{label} is not canonical JSON") from exc
    if not isinstance(snapshot, dict):
        raise DurableExecutionError(f"{label} must be a JSON object")
    return snapshot


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Replace one JSON file atomically after flushing its bytes to disk."""
    target = Path(path).resolve(strict=False)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json(dict(payload)).encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
            try:
                os.replace(temporary, target)
                break
            except PermissionError:
                if attempt + 1 >= ATOMIC_REPLACE_ATTEMPTS:
                    raise
                time.sleep(
                    min(
                        ATOMIC_REPLACE_RETRY_SECONDS * (2**attempt),
                        ATOMIC_REPLACE_MAX_RETRY_SECONDS,
                    )
                )
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def artifact_receipt(paths: Iterable[Path], *, root: Path) -> dict[str, Any]:
    """Create a stable receipt for an explicit set of artifact files."""
    resolved_root = Path(root).resolve(strict=True)
    files: dict[str, str] = {}
    for raw_path in sorted({Path(item).resolve(strict=False) for item in paths}):
        if not raw_path.is_file() or raw_path.is_symlink():
            raise DurableExecutionError(f"receipt artifact is missing or not a real file: {raw_path}")
        try:
            relative = raw_path.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise DurableExecutionError(f"receipt artifact escapes its root: {raw_path}") from exc
        files[relative] = _file_sha256(raw_path)
    if not files:
        raise DurableExecutionError("artifact receipt cannot be empty")
    receipt = {"root": str(resolved_root), "files": files}
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return receipt


def validate_artifact_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(receipt)
    expected_identity = str(payload.pop("receipt_sha256", ""))
    if expected_identity != canonical_sha256(payload):
        raise DurableExecutionError("artifact receipt identity mismatch")
    root = Path(str(payload.get("root") or "")).resolve(strict=False)
    files = payload.get("files")
    if not root.is_dir() or not isinstance(files, Mapping) or not files:
        raise DurableExecutionError("artifact receipt is incomplete")
    for relative, expected_sha256 in files.items():
        path = (root / str(relative)).resolve(strict=False)
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise DurableExecutionError("artifact receipt path escapes its root") from exc
        if not path.is_file() or path.is_symlink() or _file_sha256(path) != expected_sha256:
            raise DurableExecutionError(f"artifact receipt verification failed: {relative}")
    payload["receipt_sha256"] = expected_identity
    return payload


class DurableExecutionJournal:
    """One atomic journal whose task identities and terminal receipts are immutable."""

    def __init__(self, path: Path, *, execution_id: str, lineage: Mapping[str, Any]):
        self.path = Path(path).resolve(strict=False)
        self.execution_id = str(execution_id)
        self.lineage = _canonical_snapshot(lineage, label="execution journal lineage")

    def _new_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": JOURNAL_SCHEMA,
            "execution_id": self.execution_id,
            "lineage": self.lineage,
            "tasks": {},
        }
        payload["journal_identity"] = self._identity(payload)
        return payload

    @staticmethod
    def _identity(payload: Mapping[str, Any]) -> str:
        identity = dict(payload)
        identity.pop("journal_identity", None)
        return canonical_sha256(identity)

    def load(self, *, create: bool = False) -> dict[str, Any]:
        if not self.path.exists():
            if not create:
                raise DurableExecutionError(f"execution journal does not exist: {self.path}")
            payload = self._new_payload()
            atomic_write_json(self.path, payload)
            return payload
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DurableExecutionError(f"execution journal is unreadable: {self.path}") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != JOURNAL_SCHEMA:
            raise DurableExecutionError("execution journal schema mismatch")
        if payload.get("journal_identity") != self._identity(payload):
            raise DurableExecutionError("execution journal identity mismatch")
        if payload.get("execution_id") != self.execution_id or payload.get("lineage") != self.lineage:
            raise DurableExecutionError("execution journal lineage mismatch")
        if not isinstance(payload.get("tasks"), dict):
            raise DurableExecutionError("execution journal task graph is malformed")
        return payload

    def _save(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["journal_identity"] = self._identity(payload)
        atomic_write_json(self.path, payload)
        return payload

    @staticmethod
    def task_payload_sha256(payload: Mapping[str, Any]) -> str:
        task_payload = _canonical_snapshot(
            payload,
            label="execution journal task payload",
        )
        return canonical_sha256(task_payload)

    def register(self, task_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        journal = self.load(create=True)
        tasks = journal["tasks"]
        task_key = str(task_id)
        task_payload = _canonical_snapshot(payload, label="execution journal task payload")
        payload_sha256 = self.task_payload_sha256(task_payload)
        existing = tasks.get(task_key)
        if existing is not None:
            if existing.get("payload_sha256") != payload_sha256:
                raise DurableExecutionError(f"task payload conflicts with durable graph: {task_key}")
            return dict(existing)
        tasks[task_key] = {
            "task_id": task_key,
            "payload_sha256": payload_sha256,
            "payload": task_payload,
            "status": "pending",
            "terminal_receipt": None,
        }
        self._save(journal)
        return dict(tasks[task_key])

    def complete(self, task_id: str, receipt: Mapping[str, Any]) -> dict[str, Any]:
        journal = self.load()
        task = journal["tasks"].get(str(task_id))
        if not isinstance(task, dict):
            raise DurableExecutionError(f"terminal receipt references unknown task: {task_id}")
        terminal = _canonical_snapshot(receipt, label="execution journal terminal receipt")
        terminal_sha256 = canonical_sha256(terminal)
        if task.get("status") == "terminal":
            existing = task.get("terminal_receipt")
            if not isinstance(existing, dict) or existing.get("receipt_sha256") != terminal_sha256:
                raise DurableExecutionError(f"conflicting duplicate terminal receipt: {task_id}")
            return dict(task)
        task["status"] = "terminal"
        task["terminal_receipt"] = {
            "receipt_sha256": terminal_sha256,
            "payload": terminal,
        }
        self._save(journal)
        return dict(task)

    def unresolved(self) -> list[dict[str, Any]]:
        journal = self.load()
        return [
            dict(task)
            for _task_id, task in sorted(journal["tasks"].items())
            if isinstance(task, dict) and task.get("status") != "terminal"
        ]

    def terminal(self, task_id: str) -> dict[str, Any] | None:
        task = self.load()["tasks"].get(str(task_id))
        if not isinstance(task, dict) or task.get("status") != "terminal":
            return None
        return dict(task)
