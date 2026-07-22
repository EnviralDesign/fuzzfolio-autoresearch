"""Append-only, content-addressed execution journals for formal research work."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import canonical_json, canonical_sha256


JOURNAL_SCHEMA = "autoresearch-durable-execution-v2"
V1_JOURNAL_SCHEMA = "autoresearch-durable-execution-v1"
ATOMIC_REPLACE_ATTEMPTS = 64
ATOMIC_REPLACE_RETRY_SECONDS = 0.025
ATOMIC_REPLACE_MAX_RETRY_SECONDS = 0.5

V1_RETIRED_MESSAGE = (
    "execution journal uses retired v1 whole-file format; "
    "finish the campaign on prior code or start a new campaign"
)
_V1_RETIRED_MESSAGE = V1_RETIRED_MESSAGE


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


def _record_sha256(record: Mapping[str, Any]) -> str:
    body = dict(record)
    body.pop("record_sha256", None)
    body.pop("header_sha256", None)
    return canonical_sha256(body)


class DurableExecutionJournal:
    """Append-only JSONL journal whose task identities and terminal receipts are immutable."""

    def __init__(self, path: Path, *, execution_id: str, lineage: Mapping[str, Any]):
        self.path = Path(path).resolve(strict=False)
        self.execution_id = str(execution_id)
        self.lineage = _canonical_snapshot(lineage, label="execution journal lineage")
        self._tasks: dict[str, Any] | None = None
        self._header_sha256: str | None = None

    @staticmethod
    def _identity(payload: Mapping[str, Any]) -> str:
        """Content hash helper retained for callers that still seal whole JSON payloads."""
        identity = dict(payload)
        identity.pop("journal_identity", None)
        return canonical_sha256(identity)

    def _view(self) -> dict[str, Any]:
        if self._tasks is None:
            raise DurableExecutionError("execution journal cache is not loaded")
        view: dict[str, Any] = {
            "schema_version": JOURNAL_SCHEMA,
            "execution_id": self.execution_id,
            "lineage": self.lineage,
            # Detach the task map so callers can replace/clear their snapshot
            # without emptying the warm cache.
            "tasks": dict(self._tasks),
        }
        if self._header_sha256 is not None:
            view["journal_identity"] = self._header_sha256
        return view

    def _build_header(self) -> dict[str, Any]:
        header: dict[str, Any] = {
            "schema_version": JOURNAL_SCHEMA,
            "record_type": "header",
            "execution_id": self.execution_id,
            "lineage": self.lineage,
        }
        header["header_sha256"] = _record_sha256(header)
        return header

    def _write_header_file(self, header: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = canonical_json(header) + "\n"
        with self.path.open("xb") as handle:
            handle.write(line.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())

    def _append_records(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        payload = "".join(canonical_json(record) + "\n" for record in records)
        with self.path.open("ab") as handle:
            handle.write(payload.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())

    def _reject_v1_payload(self, payload: Mapping[str, Any]) -> None:
        if payload.get("schema_version") == V1_JOURNAL_SCHEMA:
            raise DurableExecutionError(_V1_RETIRED_MESSAGE)

    def _parse_first_record(self, text: str) -> tuple[dict[str, Any] | None, list[str]]:
        lines = text.splitlines()
        for index, raw in enumerate(lines):
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DurableExecutionError(f"execution journal is unreadable: {self.path}") from exc
            if not isinstance(record, dict):
                raise DurableExecutionError(f"execution journal is unreadable: {self.path}")
            return record, lines[index:]
        return None, []

    def _replay_lines(self, lines: list[str]) -> dict[str, Any]:
        tasks: dict[str, Any] = {}
        header: dict[str, Any] | None = None
        for raw in lines:
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DurableExecutionError(f"execution journal is unreadable: {self.path}") from exc
            if not isinstance(record, dict):
                raise DurableExecutionError("execution journal record is malformed")
            record_type = record.get("record_type")
            if record_type == "header":
                if header is not None:
                    raise DurableExecutionError("execution journal has duplicate header")
                if record.get("schema_version") != JOURNAL_SCHEMA:
                    raise DurableExecutionError("execution journal schema mismatch")
                expected = _record_sha256(record)
                if record.get("header_sha256") != expected:
                    raise DurableExecutionError("execution journal header identity mismatch")
                header = record
                continue
            if header is None:
                raise DurableExecutionError("execution journal is missing header")
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
                    raise DurableExecutionError("execution journal register record is malformed")
                existing = tasks.get(task_key)
                if existing is not None:
                    if existing.get("payload_sha256") != payload_sha256:
                        raise DurableExecutionError(
                            f"task payload conflicts with durable graph: {task_key}"
                        )
                    continue
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
                    raise DurableExecutionError("execution journal complete record is malformed")
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
                task["status"] = "terminal"
                task["terminal_receipt"] = {
                    "receipt_sha256": receipt_sha256,
                    "payload": terminal_payload,
                }
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
                task["status"] = "pending"
                task["terminal_receipt"] = None
                continue
            raise DurableExecutionError(
                f"execution journal has unknown record_type: {record_type!r}"
            )
        if header is None:
            raise DurableExecutionError("execution journal is missing header")
        if header.get("execution_id") != self.execution_id or header.get("lineage") != self.lineage:
            raise DurableExecutionError("execution journal lineage mismatch")
        self._header_sha256 = str(header["header_sha256"])
        self._tasks = tasks
        return self._view()

    def _load_from_disk(self) -> dict[str, Any]:
        try:
            text = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            raise DurableExecutionError(f"execution journal is unreadable: {self.path}") from exc

        first, lines = self._parse_first_record(text)
        if first is not None and (
            first.get("schema_version") == JOURNAL_SCHEMA
            and first.get("record_type") == "header"
        ):
            return self._replay_lines(lines)

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DurableExecutionError(f"execution journal is unreadable: {self.path}") from exc
        if isinstance(payload, dict):
            self._reject_v1_payload(payload)
        raise DurableExecutionError("execution journal schema mismatch")

    def load(self, *, create: bool = False) -> dict[str, Any]:
        if self._tasks is not None:
            return self._view()
        if not self.path.exists():
            if not create:
                raise DurableExecutionError(f"execution journal does not exist: {self.path}")
            header = self._build_header()
            self._write_header_file(header)
            self._header_sha256 = str(header["header_sha256"])
            self._tasks = {}
            return self._view()
        return self._load_from_disk()

    @staticmethod
    def task_payload_sha256(payload: Mapping[str, Any]) -> str:
        task_payload = _canonical_snapshot(
            payload,
            label="execution journal task payload",
        )
        return canonical_sha256(task_payload)

    def register(self, task_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        task_key = str(task_id)
        journal = self.apply_batch(registrations=[(task_key, payload)])
        return dict(journal["tasks"][task_key])

    def complete(self, task_id: str, receipt: Mapping[str, Any]) -> dict[str, Any]:
        task_key = str(task_id)
        journal = self.apply_batch(completions=[(task_key, receipt)])
        return dict(journal["tasks"][task_key])

    def revoke_terminal(self, task_id: str) -> dict[str, Any]:
        """Clear one terminal receipt via an append-only revoke record."""
        task_key = str(task_id)
        journal = self.apply_batch(revocations=[task_key])
        return dict(journal["tasks"][task_key])

    def apply_batch(
        self,
        *,
        registrations: Iterable[tuple[str, Mapping[str, Any]]] = (),
        completions: Iterable[tuple[str, Mapping[str, Any]]] = (),
        revocations: Iterable[str] = (),
    ) -> dict[str, Any]:
        """Apply registrations, completions, and revokes with one append + fsync."""

        registration_rows = list(registrations)
        completion_rows = list(completions)
        revocation_rows = [str(task_id) for task_id in revocations]
        self.load(create=bool(registration_rows))
        assert self._tasks is not None

        records: list[dict[str, Any]] = []
        shadow: dict[str, dict[str, Any]] = {}

        def lookup(task_key: str) -> dict[str, Any] | None:
            if task_key in shadow:
                return shadow[task_key]
            existing = self._tasks.get(task_key)
            return existing if isinstance(existing, dict) else None

        for raw_task_id, payload in registration_rows:
            task_key = str(raw_task_id)
            task_payload = _canonical_snapshot(
                payload,
                label="execution journal task payload",
            )
            payload_sha256 = self.task_payload_sha256(task_payload)
            existing = lookup(task_key)
            if existing is not None:
                if existing.get("payload_sha256") != payload_sha256:
                    raise DurableExecutionError(
                        f"task payload conflicts with durable graph: {task_key}"
                    )
                continue
            record: dict[str, Any] = {
                "record_type": "register",
                "task_id": task_key,
                "payload_sha256": payload_sha256,
                "payload": task_payload,
            }
            record["record_sha256"] = _record_sha256(record)
            records.append(record)
            shadow[task_key] = {
                "task_id": task_key,
                "payload_sha256": payload_sha256,
                "payload": task_payload,
                "status": "pending",
                "terminal_receipt": None,
            }

        for raw_task_id, receipt in completion_rows:
            task_key = str(raw_task_id)
            task = lookup(task_key)
            if not isinstance(task, dict):
                raise DurableExecutionError(
                    f"terminal receipt references unknown task: {task_key}"
                )
            terminal = _canonical_snapshot(
                receipt,
                label="execution journal terminal receipt",
            )
            terminal_sha256 = canonical_sha256(terminal)
            if task.get("status") == "terminal":
                existing_receipt = task.get("terminal_receipt")
                if (
                    not isinstance(existing_receipt, dict)
                    or existing_receipt.get("receipt_sha256") != terminal_sha256
                ):
                    raise DurableExecutionError(
                        f"conflicting duplicate terminal receipt: {task_key}"
                    )
                continue
            record = {
                "record_type": "complete",
                "task_id": task_key,
                "receipt_sha256": terminal_sha256,
                "payload": terminal,
            }
            record["record_sha256"] = _record_sha256(record)
            records.append(record)
            updated = dict(task)
            updated["status"] = "terminal"
            updated["terminal_receipt"] = {
                "receipt_sha256": terminal_sha256,
                "payload": terminal,
            }
            shadow[task_key] = updated

        for task_key in revocation_rows:
            task = lookup(task_key)
            if not isinstance(task, dict):
                raise DurableExecutionError(
                    f"revoke references unknown task: {task_key}"
                )
            if task.get("status") != "terminal":
                continue
            record = {
                "record_type": "revoke",
                "task_id": task_key,
            }
            record["record_sha256"] = _record_sha256(record)
            records.append(record)
            updated = dict(task)
            updated["status"] = "pending"
            updated["terminal_receipt"] = None
            shadow[task_key] = updated

        if records:
            self._append_records(records)
            self._tasks.update(shadow)
        return self._view()

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
