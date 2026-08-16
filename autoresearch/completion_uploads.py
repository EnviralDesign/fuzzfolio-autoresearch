from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from uuid import uuid4

COMPLETION_UPLOAD_SCHEMA_VERSION = "play_hand_lab_completion_upload_v1"


class CompletionUploadError(ValueError):
    def __init__(self, code: str, *, http_status: int = 400) -> None:
        super().__init__(code)
        self.code = str(code)
        self.http_status = int(http_status)


@dataclass(frozen=True, slots=True)
class CompletionUploadIdentity:
    lease_id: str
    worker_id: str
    status: str
    size_bytes: int
    sha256: str
    chunk_size_bytes: int
    chunk_count: int


@dataclass(slots=True)
class CompletionUpload:
    upload_id: str
    identity: CompletionUploadIdentity
    path: Path
    created_at: float
    updated_at: float
    received: dict[int, tuple[int, str]] = field(default_factory=dict)
    finalizing: bool = False
    aborted: bool = False

    def response_payload(self, *, status: Literal["created", "resumed"] = "resumed") -> dict:
        return {
            "status": status,
            "schema_version": COMPLETION_UPLOAD_SCHEMA_VERSION,
            "upload_id": self.upload_id,
            "size_bytes": self.identity.size_bytes,
            "sha256": self.identity.sha256,
            "chunk_size_bytes": self.identity.chunk_size_bytes,
            "chunk_count": self.identity.chunk_count,
            "received_chunk_indices": sorted(self.received),
        }


def _require_sha256(value: str, *, code: str) -> str:
    token = str(value or "")
    if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
        raise CompletionUploadError(code)
    return token


class CompletionUploadStore:
    """Bounded, process-local spool for proxy-safe completion uploads.

    The gateway's task/lease state is intentionally process-local too. On startup this store
    removes abandoned files from prior processes; it never attempts to resurrect an upload
    without the corresponding live lease authority.
    """

    def __init__(
        self,
        *,
        root: str | os.PathLike[str] | None,
        max_upload_bytes: int,
        max_total_bytes: int,
        max_chunk_bytes: int,
        ttl_seconds: float,
    ) -> None:
        configured_root = (
            Path(root)
            if root is not None and str(root).strip()
            else Path(tempfile.gettempdir()) / "play-hand-lab-completion-uploads"
        )
        self.root = configured_root.expanduser().resolve()
        self.max_upload_bytes = max(int(max_upload_bytes), 1)
        self.max_total_bytes = max(int(max_total_bytes), self.max_upload_bytes)
        self.max_chunk_bytes = max(int(max_chunk_bytes), 1)
        self.ttl_seconds = max(float(ttl_seconds), 1.0)
        self._lock = threading.RLock()
        self._uploads: dict[str, CompletionUpload] = {}
        self._upload_by_lease: dict[str, str] = {}
        self._reserved_bytes = 0
        self.root.mkdir(parents=True, exist_ok=True)
        self._remove_abandoned_startup_entries()

    def _remove_abandoned_startup_entries(self) -> None:
        for entry in self.root.iterdir():
            try:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
            except FileNotFoundError:
                continue

    def begin(self, identity: CompletionUploadIdentity, *, now: float | None = None) -> dict:
        timestamp = time.time() if now is None else float(now)
        self._validate_identity(identity)
        with self._lock:
            self.cleanup_stale(now=timestamp)
            existing_id = self._upload_by_lease.get(identity.lease_id)
            if existing_id is not None:
                existing = self._uploads.get(existing_id)
                if existing is None:
                    self._upload_by_lease.pop(identity.lease_id, None)
                elif existing.identity == identity and not existing.aborted:
                    existing.updated_at = timestamp
                    return existing.response_payload(status="resumed")
                else:
                    raise CompletionUploadError("completion_upload_identity_conflict", http_status=409)

            if self._reserved_bytes + identity.size_bytes > self.max_total_bytes:
                raise CompletionUploadError("completion_upload_spool_full", http_status=503)
            try:
                free_bytes = shutil.disk_usage(self.root).free
            except OSError:
                free_bytes = identity.size_bytes
            if free_bytes < identity.size_bytes:
                raise CompletionUploadError("completion_upload_disk_full", http_status=507)

            upload_id = str(uuid4())
            upload_root = self.root / upload_id
            upload_root.mkdir(mode=0o700)
            path = upload_root / "completion.json.part"
            try:
                with path.open("xb") as handle:
                    handle.truncate(identity.size_bytes)
            except Exception:
                shutil.rmtree(upload_root, ignore_errors=True)
                raise
            upload = CompletionUpload(
                upload_id=upload_id,
                identity=identity,
                path=path,
                created_at=timestamp,
                updated_at=timestamp,
            )
            self._uploads[upload_id] = upload
            self._upload_by_lease[identity.lease_id] = upload_id
            self._reserved_bytes += identity.size_bytes
            return upload.response_payload(status="created")

    def write_chunk(
        self,
        *,
        upload_id: str,
        lease_id: str,
        worker_id: str,
        index: int,
        payload: bytes,
        sha256: str,
        now: float | None = None,
    ) -> dict:
        timestamp = time.time() if now is None else float(now)
        chunk_sha256 = _require_sha256(sha256, code="invalid_completion_chunk_sha256")
        body = bytes(payload)
        with self._lock:
            upload = self._require_upload(upload_id, lease_id=lease_id, worker_id=worker_id)
            if upload.finalizing:
                raise CompletionUploadError("completion_upload_finalizing", http_status=409)
            if index < 0 or index >= upload.identity.chunk_count:
                raise CompletionUploadError("invalid_completion_chunk_index")
            expected_size = self._expected_chunk_size(upload.identity, index)
            if len(body) != expected_size:
                raise CompletionUploadError("invalid_completion_chunk_size")
            actual_sha256 = hashlib.sha256(body).hexdigest()
            if actual_sha256 != chunk_sha256:
                raise CompletionUploadError("completion_chunk_sha256_mismatch")
            previous = upload.received.get(index)
            if previous is not None:
                if previous != (len(body), chunk_sha256):
                    raise CompletionUploadError("completion_chunk_conflict", http_status=409)
                upload.updated_at = timestamp
                return {"status": "duplicate", "upload_id": upload.upload_id, "chunk_index": index}
            try:
                with upload.path.open("r+b", buffering=0) as handle:
                    handle.seek(index * upload.identity.chunk_size_bytes)
                    written = handle.write(body)
                    if written != len(body):
                        raise OSError("short completion chunk write")
            except FileNotFoundError as exc:
                raise CompletionUploadError("completion_upload_missing", http_status=409) from exc
            upload.received[index] = (len(body), chunk_sha256)
            upload.updated_at = timestamp
            return {"status": "accepted", "upload_id": upload.upload_id, "chunk_index": index}

    def prepare_finalize(
        self,
        *,
        upload_id: str,
        lease_id: str,
        worker_id: str,
        now: float | None = None,
    ) -> CompletionUpload:
        timestamp = time.time() if now is None else float(now)
        with self._lock:
            upload = self._require_upload(upload_id, lease_id=lease_id, worker_id=worker_id)
            if len(upload.received) != upload.identity.chunk_count:
                raise CompletionUploadError("completion_upload_incomplete", http_status=409)
            if upload.finalizing:
                raise CompletionUploadError("completion_upload_finalizing", http_status=409)
            upload.finalizing = True
            upload.updated_at = timestamp
            return upload

    def reset_finalizing(self, upload_id: str) -> None:
        with self._lock:
            upload = self._uploads.get(upload_id)
            if upload is not None and not upload.aborted:
                upload.finalizing = False
                upload.updated_at = time.time()

    def finish(self, upload_id: str) -> None:
        with self._lock:
            self._remove_locked(upload_id)

    def abort_lease(self, lease_id: str) -> None:
        with self._lock:
            upload_id = self._upload_by_lease.get(str(lease_id))
            if upload_id is None:
                return
            upload = self._uploads.get(upload_id)
            if upload is not None and upload.finalizing:
                upload.aborted = True
                return
            self._remove_locked(upload_id)

    def cleanup_stale(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else float(now)
        with self._lock:
            stale = [
                upload_id
                for upload_id, upload in self._uploads.items()
                if timestamp - upload.updated_at >= self.ttl_seconds
            ]
            for upload_id in stale:
                self._remove_locked(upload_id)
            return len(stale)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "active_uploads": len(self._uploads),
                "reserved_bytes": self._reserved_bytes,
                "max_total_bytes": self.max_total_bytes,
                "max_upload_bytes": self.max_upload_bytes,
                "max_chunk_bytes": self.max_chunk_bytes,
            }

    def _validate_identity(self, identity: CompletionUploadIdentity) -> None:
        if not identity.lease_id or not identity.worker_id:
            raise CompletionUploadError("completion_upload_identity_required")
        _require_sha256(identity.sha256, code="invalid_completion_upload_sha256")
        if identity.size_bytes <= 0 or identity.size_bytes > self.max_upload_bytes:
            raise CompletionUploadError("completion_upload_size_out_of_bounds", http_status=413)
        if identity.chunk_size_bytes <= 0 or identity.chunk_size_bytes > self.max_chunk_bytes:
            raise CompletionUploadError("completion_chunk_size_out_of_bounds", http_status=413)
        expected_count = (identity.size_bytes + identity.chunk_size_bytes - 1) // identity.chunk_size_bytes
        if identity.chunk_count != expected_count:
            raise CompletionUploadError("invalid_completion_chunk_count")

    def _require_upload(self, upload_id: str, *, lease_id: str, worker_id: str) -> CompletionUpload:
        upload = self._uploads.get(str(upload_id))
        if upload is None:
            raise CompletionUploadError("completion_upload_not_found", http_status=404)
        if upload.identity.lease_id != str(lease_id) or upload.identity.worker_id != str(worker_id):
            raise CompletionUploadError("completion_upload_owner_mismatch", http_status=403)
        if upload.aborted:
            raise CompletionUploadError("completion_upload_aborted", http_status=409)
        return upload

    @staticmethod
    def _expected_chunk_size(identity: CompletionUploadIdentity, index: int) -> int:
        start = index * identity.chunk_size_bytes
        return min(identity.chunk_size_bytes, identity.size_bytes - start)

    def _remove_locked(self, upload_id: str) -> None:
        upload = self._uploads.pop(str(upload_id), None)
        if upload is None:
            return
        if self._upload_by_lease.get(upload.identity.lease_id) == upload.upload_id:
            self._upload_by_lease.pop(upload.identity.lease_id, None)
        self._reserved_bytes = max(self._reserved_bytes - upload.identity.size_bytes, 0)
        shutil.rmtree(upload.path.parent, ignore_errors=True)


__all__ = [
    "COMPLETION_UPLOAD_SCHEMA_VERSION",
    "CompletionUpload",
    "CompletionUploadError",
    "CompletionUploadIdentity",
    "CompletionUploadStore",
]
