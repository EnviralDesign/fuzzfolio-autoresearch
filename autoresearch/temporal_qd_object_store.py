"""Immutable, versioned content-addressed objects for the Temporal QD redesign.

This module is intentionally not wired into proposal generation, supervision, or
archive reduction yet.  It establishes the persistence boundary those later
changes will consume: a typed namespace plus the SHA-256 of exact canonical
bytes is the complete durable object identity.

The existing :mod:`autoresearch.result_codec` canonical JSON implementation is
the Python oracle.  This module does not reinterpret, normalize, or replace it.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import Any, BinaryIO, TypeAlias
import zlib

from .result_codec import canonical_json_bytes as _oracle_canonical_json_bytes
from .result_codec import fsync_directory


OBJECT_STORE_SCHEMA = "temporal_qd_content_addressed_store_v1"
OBJECT_MANIFEST_SCHEMA = "temporal_qd_object_manifest_v1"
PACKED_OBJECT_STORE_SCHEMA = "temporal_qd_packed_object_store_v1"
PACKED_OBJECT_PACK_SCHEMA = "temporal_qd_object_pack_v1"
PACKED_OBJECT_INDEX_SCHEMA = "temporal_qd_object_pack_index_v1"
COMPRESSED_PACKED_OBJECT_STORE_SCHEMA = "temporal_qd_compressed_packed_object_store_v1"
COMPRESSED_PACKED_OBJECT_PACK_SCHEMA = "temporal_qd_compressed_object_pack_v1"
COMPRESSED_PACKED_OBJECT_INDEX_SCHEMA = "temporal_qd_compressed_object_pack_index_v1"
COMPRESSED_PACK_CODEC = "zlib-deflate-v1"
CANONICAL_JSON_CODEC = "canonical-json-v1"
RAW_BYTES_CODEC = "bytes-v1"

DEFAULT_CHUNK_BYTES = 1024 * 1024
# A compressed block is decoded incrementally, never into one raw-pack buffer.
# This limit prevents a tampered index from authorizing unbounded expansion.
MAX_COMPRESSED_PACK_RAW_BYTES = 512 * 1024 * 1024
PACKED_PUBLICATION_LOCK_TIMEOUT_SECONDS = 30.0
_PACKED_PUBLICATION_LOCK_POLL_SECONDS = 0.02
_SHA256 = re.compile(r"^sha256:([0-9a-f]{64})$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_MANIFEST_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_PACK_FILE_NAME = re.compile(r"^[0-9a-f]{64}\.pack$")
_PACK_INDEX_FILE_NAME = re.compile(r"^[0-9a-f]{64}\.index\.json$")
_COMPRESSED_PACK_FILE_NAME = re.compile(r"^[0-9a-f]{64}\.zpack$")
_COMPRESSED_PACK_INDEX_FILE_NAME = re.compile(r"^[0-9a-f]{64}\.zindex\.json$")

BytesLike: TypeAlias = bytes | bytearray | memoryview


class TemporalQDObjectStoreError(RuntimeError):
    """Base error for the isolated Temporal QD object-store contract."""


class ObjectStorePathError(TemporalQDObjectStoreError):
    """A path is unsafe, escapes the store, or crosses a symlink."""


class ObjectStoreIntegrityError(TemporalQDObjectStoreError):
    """Stored bytes are missing, corrupt, non-canonical, or identity-mismatched."""


class ObjectStoreConflictError(ObjectStoreIntegrityError):
    """A digest collision would require accepting non-identical immutable bytes."""


class ObjectStoreNotFoundError(ObjectStoreIntegrityError):
    """The requested immutable object has not been published."""


def canonical_json_bytes(value: Any) -> bytes:
    """Return the repository's exact finite canonical JSON representation.

    ``result_codec.canonical_json_bytes`` is deliberately reused rather than
    reimplemented.  A future optimized path must prove byte parity against this
    function before it can become a producer for this store.
    """

    try:
        return _oracle_canonical_json_bytes(value)
    except Exception as exc:  # ResultCodecError is intentionally not leaked.
        raise TemporalQDObjectStoreError("value is not finite canonical JSON") from exc


def sha256_bytes(value: bytes) -> str:
    """Return the namespaced SHA-256 token for exact bytes."""

    return "sha256:" + hashlib.sha256(value).hexdigest()


def _require_sha256(value: object, *, label: str) -> str:
    token = str(value)
    if not _SHA256.fullmatch(token):
        raise TemporalQDObjectStoreError(f"{label} must be sha256:<64 lowercase hex>")
    return token


def _digest_hex(token: str) -> str:
    matched = _SHA256.fullmatch(token)
    if matched is None:  # Defensive: every public caller validates ObjectRef.
        raise TemporalQDObjectStoreError("invalid SHA-256 object identity")
    return matched.group(1)


def _require_token(value: object, *, label: str) -> str:
    token = str(value)
    if not _TOKEN.fullmatch(token):
        raise TemporalQDObjectStoreError(
            f"{label} must start with lowercase a-z and contain only a-z, 0-9, _ or -"
        )
    return token


def _is_link_or_reparse(status: os.stat_result) -> bool:
    """Reject POSIX symlinks and Windows symlink/junction reparse points."""

    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    return stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point)


def _canonical_clone(value: Any, *, label: str) -> Any:
    """Detach JSON input while proving it is representable by the oracle."""

    try:
        return json.loads(canonical_json_bytes(value).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:  # pragma: no cover
        raise TemporalQDObjectStoreError(f"{label} is not canonical JSON") from exc


@dataclass(frozen=True)
class ObjectNamespace:
    """A semantic object type, schema version, and exact byte codec.

    Namespace components are independently path-safe.  The storage layout
    never accepts an arbitrary caller-supplied relative path.
    """

    object_type: str
    schema_version: int
    codec: str = CANONICAL_JSON_CODEC

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "object_type",
            _require_token(self.object_type, label="object namespace type"),
        )
        if isinstance(self.schema_version, bool) or int(self.schema_version) < 1:
            raise TemporalQDObjectStoreError("object namespace schema_version must be >= 1")
        object.__setattr__(self, "schema_version", int(self.schema_version))
        if self.codec not in {CANONICAL_JSON_CODEC, RAW_BYTES_CODEC}:
            raise TemporalQDObjectStoreError(
                f"unsupported object namespace codec: {self.codec!r}"
            )

    @property
    def namespace_id(self) -> str:
        return f"{self.object_type}.v{self.schema_version}.{self.codec}"

    def as_dict(self) -> dict[str, object]:
        return {
            "type": self.object_type,
            "schemaVersion": self.schema_version,
            "codec": self.codec,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ObjectNamespace":
        if set(payload) != {"type", "schemaVersion", "codec"}:
            raise TemporalQDObjectStoreError("object namespace has unexpected fields")
        return cls(
            object_type=str(payload["type"]),
            schema_version=payload["schemaVersion"],  # type: ignore[arg-type]
            codec=str(payload["codec"]),
        )


@dataclass(frozen=True)
class ObjectRef:
    """A compact immutable reference; it deliberately contains no local path."""

    namespace: ObjectNamespace
    sha256: str
    byte_length: int

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, ObjectNamespace):
            raise TemporalQDObjectStoreError("object reference namespace is invalid")
        object.__setattr__(self, "sha256", _require_sha256(self.sha256, label="object sha256"))
        if isinstance(self.byte_length, bool) or int(self.byte_length) < 0:
            raise TemporalQDObjectStoreError("object byte_length must be a non-negative integer")
        object.__setattr__(self, "byte_length", int(self.byte_length))

    @property
    def digest_hex(self) -> str:
        return _digest_hex(self.sha256)

    def as_dict(self) -> dict[str, object]:
        return {
            "namespace": self.namespace.as_dict(),
            "sha256": self.sha256,
            "byteLength": self.byte_length,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ObjectRef":
        if set(payload) != {"namespace", "sha256", "byteLength"}:
            raise TemporalQDObjectStoreError("object reference has unexpected fields")
        namespace = payload["namespace"]
        if not isinstance(namespace, Mapping):
            raise TemporalQDObjectStoreError("object reference namespace must be an object")
        return cls(
            namespace=ObjectNamespace.from_dict(namespace),
            sha256=str(payload["sha256"]),
            byte_length=payload["byteLength"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True)
class PreparedObject:
    """Exact immutable bytes prepared once for high-throughput batch publication."""

    ref: ObjectRef
    data: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.ref, ObjectRef):
            raise TemporalQDObjectStoreError("prepared object reference is invalid")
        if not isinstance(self.data, bytes):
            raise TemporalQDObjectStoreError("prepared object bytes must be immutable bytes")
        if len(self.data) != self.ref.byte_length:
            raise TemporalQDObjectStoreError("prepared object byte length disagrees with reference")
        if sha256_bytes(self.data) != self.ref.sha256:
            raise TemporalQDObjectStoreError("prepared object SHA-256 disagrees with reference")


@dataclass(frozen=True)
class BatchPutResult:
    """Publication receipts and logical payload-write accounting for a batch."""

    refs: tuple[ObjectRef, ...]
    created_count: int
    reused_count: int
    bytes_written: int


@dataclass(frozen=True)
class _Publication:
    ref: ObjectRef
    parent: Path
    created: bool
    bytes_written: int
    temporary: Path | None


@dataclass(frozen=True)
class PackedObjectLocation:
    """One immutable object slice inside a validated packed-store file."""

    ref: ObjectRef
    batch_id: str
    pack_path: Path
    pack_sha256: str
    pack_byte_length: int
    offset: int
    length: int


@dataclass(frozen=True)
class _PackIndex:
    batch_id: str
    index_path: Path
    pack_path: Path
    pack_sha256: str
    pack_byte_length: int
    locations: tuple[PackedObjectLocation, ...]


@dataclass(frozen=True)
class _PackSource:
    """A new immutable object supplied as prepared bytes or a fsynced temp file."""

    ref: ObjectRef
    data: bytes | None = None
    path: Path | None = None

    def __post_init__(self) -> None:
        if (self.data is None) == (self.path is None):
            raise TemporalQDObjectStoreError(
                "packed object source must contain exactly one of data or path"
            )
        if self.data is not None:
            if len(self.data) != self.ref.byte_length or sha256_bytes(self.data) != self.ref.sha256:
                raise TemporalQDObjectStoreError(
                    "packed in-memory source disagrees with immutable reference"
                )


@dataclass(frozen=True)
class CompressedPackedObjectLocation:
    """One raw-object slice in an independently compressed immutable block."""

    ref: ObjectRef
    batch_id: str
    pack_path: Path
    raw_pack_sha256: str
    raw_pack_byte_length: int
    compressed_pack_sha256: str
    compressed_pack_byte_length: int
    offset: int
    length: int


@dataclass(frozen=True)
class _CompressedPackIndex:
    batch_id: str
    index_path: Path
    pack_path: Path
    raw_pack_sha256: str
    raw_pack_byte_length: int
    compressed_pack_sha256: str
    compressed_pack_byte_length: int
    locations: tuple[CompressedPackedObjectLocation, ...]


def prepare_bytes(namespace: ObjectNamespace, payload: BytesLike) -> PreparedObject:
    """Prepare immutable raw bytes for one ``bytes-v1`` namespace."""

    if namespace.codec != RAW_BYTES_CODEC:
        raise TemporalQDObjectStoreError(
            "prepare_bytes requires a bytes-v1 namespace; use prepare_json for JSON"
        )
    data = bytes(payload)
    return PreparedObject(
        ref=ObjectRef(namespace=namespace, sha256=sha256_bytes(data), byte_length=len(data)),
        data=data,
    )


def prepare_canonical_json_bytes(
    namespace: ObjectNamespace, payload: BytesLike
) -> PreparedObject:
    """Prepare already-encoded JSON only when it is byte-for-byte canonical."""

    if namespace.codec != CANONICAL_JSON_CODEC:
        raise TemporalQDObjectStoreError(
            "prepare_canonical_json_bytes requires a canonical-json-v1 namespace"
        )
    data = bytes(payload)
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ObjectStoreIntegrityError("canonical JSON bytes are not valid UTF-8 JSON") from exc
    if canonical_json_bytes(parsed) != data:
        raise ObjectStoreIntegrityError("JSON bytes are valid but not exact canonical bytes")
    return PreparedObject(
        ref=ObjectRef(namespace=namespace, sha256=sha256_bytes(data), byte_length=len(data)),
        data=data,
    )


def prepare_json(namespace: ObjectNamespace, value: Any) -> PreparedObject:
    """Prepare canonical JSON with the established Python representation oracle."""

    if namespace.codec != CANONICAL_JSON_CODEC:
        raise TemporalQDObjectStoreError(
            "prepare_json requires a canonical-json-v1 namespace; use prepare_bytes"
        )
    return prepare_canonical_json_bytes(namespace, canonical_json_bytes(value))


def _ref_sort_key(ref: ObjectRef) -> tuple[str, int, str, str, int]:
    return (
        ref.namespace.object_type,
        ref.namespace.schema_version,
        ref.namespace.codec,
        ref.sha256,
        ref.byte_length,
    )


def build_manifest(
    *,
    manifest_type: str,
    manifest_version: int,
    refs: Iterable[ObjectRef],
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a sorted, duplicate-free manifest and seal its canonical identity."""

    manifest_type = str(manifest_type)
    if not _MANIFEST_TYPE.fullmatch(manifest_type):
        raise TemporalQDObjectStoreError("manifest_type contains unsafe characters")
    if isinstance(manifest_version, bool) or int(manifest_version) < 1:
        raise TemporalQDObjectStoreError("manifest_version must be >= 1")
    collected = list(refs)
    if any(not isinstance(ref, ObjectRef) for ref in collected):
        raise TemporalQDObjectStoreError("manifest references must all be ObjectRef values")
    ordered = sorted(collected, key=_ref_sort_key)
    if any(
        _ref_sort_key(left) == _ref_sort_key(right)
        for left, right in zip(ordered, ordered[1:])
    ):
        raise TemporalQDObjectStoreError("manifest references must be unique")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TemporalQDObjectStoreError("manifest metadata must be a JSON object")
    payload: dict[str, Any] = {
        "schemaVersion": OBJECT_MANIFEST_SCHEMA,
        "manifestType": manifest_type,
        "manifestVersion": int(manifest_version),
        "metadata": _canonical_clone(dict(metadata or {}), label="manifest metadata"),
        "objectRefs": [ref.as_dict() for ref in ordered],
    }
    payload["manifestSha256"] = sha256_bytes(canonical_json_bytes(payload))
    return payload


def validate_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Fail closed unless a manifest is exact, sorted, and self-authenticating."""

    cloned = _canonical_clone(dict(payload), label="manifest")
    if not isinstance(cloned, dict):  # pragma: no cover - retained for defensive clarity.
        raise TemporalQDObjectStoreError("manifest must be a JSON object")
    expected_fields = {
        "schemaVersion",
        "manifestType",
        "manifestVersion",
        "metadata",
        "objectRefs",
        "manifestSha256",
    }
    if set(cloned) != expected_fields:
        raise ObjectStoreIntegrityError("manifest has unexpected or missing fields")
    if cloned["schemaVersion"] != OBJECT_MANIFEST_SCHEMA:
        raise ObjectStoreIntegrityError("manifest schema version is unsupported")
    if not isinstance(cloned["manifestType"], str) or not _MANIFEST_TYPE.fullmatch(
        cloned["manifestType"]
    ):
        raise ObjectStoreIntegrityError("manifest type is invalid")
    if isinstance(cloned["manifestVersion"], bool) or not isinstance(
        cloned["manifestVersion"], int
    ) or cloned["manifestVersion"] < 1:
        raise ObjectStoreIntegrityError("manifest version is invalid")
    if not isinstance(cloned["metadata"], dict):
        raise ObjectStoreIntegrityError("manifest metadata must be an object")
    raw_refs = cloned["objectRefs"]
    if not isinstance(raw_refs, list):
        raise ObjectStoreIntegrityError("manifest objectRefs must be a list")
    try:
        refs = [
            ObjectRef.from_dict(item)
            for item in raw_refs
            if isinstance(item, Mapping)
        ]
    except TemporalQDObjectStoreError as exc:
        raise ObjectStoreIntegrityError("manifest contains an invalid object reference") from exc
    if len(refs) != len(raw_refs):
        raise ObjectStoreIntegrityError("manifest object reference must be an object")
    ordered = sorted(refs, key=_ref_sort_key)
    if [item.as_dict() for item in ordered] != raw_refs:
        raise ObjectStoreIntegrityError("manifest references are not deterministically sorted")
    if any(
        _ref_sort_key(left) == _ref_sort_key(right)
        for left, right in zip(ordered, ordered[1:])
    ):
        raise ObjectStoreIntegrityError("manifest has duplicate object references")
    supplied = _require_sha256(cloned["manifestSha256"], label="manifest SHA-256")
    body = dict(cloned)
    body.pop("manifestSha256")
    expected = sha256_bytes(canonical_json_bytes(body))
    if supplied != expected:
        raise ObjectStoreIntegrityError(
            f"manifest SHA-256 mismatch: expected {expected}, received {supplied}"
        )
    return cloned


class VerifiedObjectReader(AbstractContextManager["VerifiedObjectReader"]):
    """A bounded-memory reader which verifies identity on EOF or close.

    Closing a partially consumed reader drains it in chunks before returning, so
    callers cannot accidentally treat an unverified prefix as a hydrated object.
    """

    def __init__(self, store: "TemporalQDObjectStore", ref: ObjectRef) -> None:
        self._store = store
        self.ref = ref
        self._handle = store._open_regular_file(store.path_for(ref))
        self._digest = hashlib.sha256()
        self._length = 0
        self._verified = False
        self._closed = False

    def __enter__(self) -> "VerifiedObjectReader":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise
        return False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed object reader")
        data = self._handle.read(size)
        if data:
            self._digest.update(data)
            self._length += len(data)
        else:
            self._finish_verification()
        return data

    def iter_chunks(self, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES) -> Iterator[bytes]:
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        while True:
            chunk = self.read(chunk_bytes)
            if not chunk:
                return
            yield chunk

    def verify(self) -> None:
        if self._closed:
            if not self._verified:
                raise ValueError("closed object reader did not verify")
            return
        for _chunk in self.iter_chunks():
            pass

    def _finish_verification(self) -> None:
        if self._verified:
            return
        observed = "sha256:" + self._digest.hexdigest()
        if self._length != self.ref.byte_length or observed != self.ref.sha256:
            raise ObjectStoreIntegrityError(
                "object bytes do not match immutable reference: "
                f"expected ({self.ref.sha256}, {self.ref.byte_length}), "
                f"observed ({observed}, {self._length})"
            )
        self._verified = True

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.verify()
        finally:
            self._handle.close()
            self._closed = True


class TemporalQDObjectStore:
    """Durable content-addressed objects rooted at one explicit local directory.

    Objects are published with a fully fsynced temporary file and ``os.link``.
    Hard-link publication is create-only: an existing destination is accepted
    only after exact byte comparison, never replaced.  Directory fsync remains
    best-effort on Windows through the existing repository helper.
    """

    def __init__(self, root: Path | str) -> None:
        requested = Path(root)
        try:
            existing_status: os.stat_result | None = os.lstat(requested)
        except FileNotFoundError:
            existing_status = None
        if existing_status is not None and _is_link_or_reparse(existing_status):
            raise ObjectStorePathError(
                "object-store root may not itself be a symlink or junction"
            )
        requested.mkdir(parents=True, exist_ok=True)
        try:
            requested_status = os.lstat(requested)
        except FileNotFoundError as exc:  # pragma: no cover - only a concurrent remover.
            raise ObjectStorePathError("object-store root disappeared during creation") from exc
        if _is_link_or_reparse(requested_status) or not stat.S_ISDIR(requested_status.st_mode):
            raise ObjectStorePathError("object-store root must be a real directory")
        self.root = requested.resolve(strict=True)
        self._objects_root = self._ensure_directory(("objects",))
        self._staging_root = self._ensure_directory((".staging",))

    @contextmanager
    def _packed_publication_lock(self) -> Iterator[None]:
        """Serialize packed refresh/select/publish across cooperating processes.

        An empty lock directory is created atomically, so only its owner can
        enter the critical section.  A process crash leaves the directory in
        place and intentionally fails future publishers closed rather than
        allowing an overlapping batch to poison immutable indexes.  Recovery
        of such an abandoned lock is an explicit operator action.
        """

        lock_path = self._lexical_path((".packed-publication.lock",))
        deadline = time.monotonic() + PACKED_PUBLICATION_LOCK_TIMEOUT_SECONDS
        acquired = False
        while not acquired:
            try:
                os.mkdir(lock_path)
            except FileExistsError:
                try:
                    self._assert_path_components_safe(lock_path, final_kind="directory")
                except ObjectStoreNotFoundError:
                    # A releasing owner won the race; retry acquisition.
                    continue
                if time.monotonic() >= deadline:
                    raise TemporalQDObjectStoreError(
                        "timed out waiting for packed publication lock; "
                        "refusing concurrent immutable-index publication"
                    )
                time.sleep(_PACKED_PUBLICATION_LOCK_POLL_SECONDS)
            else:
                acquired = True
        try:
            # Detect an immediate path swap before using the lock.
            self._assert_path_components_safe(lock_path, final_kind="directory")
            yield
        finally:
            if acquired:
                try:
                    status = os.lstat(lock_path)
                except FileNotFoundError as exc:
                    raise ObjectStorePathError(
                        "packed publication lock disappeared before release"
                    ) from exc
                if _is_link_or_reparse(status) or not stat.S_ISDIR(status.st_mode):
                    raise ObjectStorePathError(
                        "packed publication lock was replaced with an unsafe path"
                    )
                try:
                    os.rmdir(lock_path)
                except OSError as exc:
                    raise TemporalQDObjectStoreError(
                        "cannot release packed publication lock; refusing further publication"
                    ) from exc

    def path_for(self, ref: ObjectRef) -> Path:
        """Return the deterministic path for a valid reference, not a user path."""

        if not isinstance(ref, ObjectRef):
            raise TemporalQDObjectStoreError("path_for requires an ObjectRef")
        digest = ref.digest_hex
        return self._lexical_path(
            (
                "objects",
                ref.namespace.object_type,
                f"v{ref.namespace.schema_version}",
                ref.namespace.codec,
                digest[:2],
                digest,
            )
        )

    def put_json(self, namespace: ObjectNamespace, value: Any) -> ObjectRef:
        return self.put_prepared(prepare_json(namespace, value))

    def put_canonical_json_bytes(
        self, namespace: ObjectNamespace, payload: BytesLike
    ) -> ObjectRef:
        return self.put_prepared(prepare_canonical_json_bytes(namespace, payload))

    def put_bytes(self, namespace: ObjectNamespace, payload: BytesLike) -> ObjectRef:
        return self.put_prepared(prepare_bytes(namespace, payload))

    def put_prepared(self, prepared: PreparedObject) -> ObjectRef:
        """Publish one already-prepared object without reserializing or rehashing it."""

        return self.put_many((prepared,)).refs[0]

    def put_many(self, prepared_objects: Iterable[PreparedObject]) -> BatchPutResult:
        """Publish prepared objects with one caller boundary and grouped directory fsync.

        The method consumes the iterable once, never reserializes JSON or
        recomputes prepared identities, coalesces duplicate entries within the
        batch, and fsyncs each affected object directory at most once.
        """

        refs: list[ObjectRef] = []
        directories: set[Path] = set()
        published_temps: list[Path] = []
        seen: dict[ObjectRef, bytes] = {}
        created_count = 0
        reused_count = 0
        bytes_written = 0
        try:
            for prepared in prepared_objects:
                if not isinstance(prepared, PreparedObject):
                    raise TemporalQDObjectStoreError(
                        "put_many requires PreparedObject entries"
                    )
                refs.append(prepared.ref)
                prior = seen.get(prepared.ref)
                if prior is not None:
                    if prior != prepared.data:
                        raise ObjectStoreConflictError(
                            "two batch objects share a reference but differ in exact bytes"
                        )
                    reused_count += 1
                    continue
                seen[prepared.ref] = prepared.data
                publication = self._publish_prepared(prepared)
                directories.add(publication.parent)
                if publication.temporary is not None:
                    published_temps.append(publication.temporary)
                if publication.created:
                    created_count += 1
                    bytes_written += publication.bytes_written
                else:
                    reused_count += 1
            self._sync_directories(directories)
        except Exception:
            # Published-but-not-yet-synced temps are deliberately retained.
            # They are ignored on restart and make interrupted publication
            # auditable without ever being mistaken for a canonical object.
            raise
        else:
            for temporary in published_temps:
                temporary.unlink(missing_ok=True)
        return BatchPutResult(
            refs=tuple(refs),
            created_count=created_count,
            reused_count=reused_count,
            bytes_written=bytes_written,
        )

    def put_stream(
        self,
        namespace: ObjectNamespace,
        source: BinaryIO,
        *,
        expected_sha256: str | None = None,
        expected_byte_length: int | None = None,
        chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    ) -> ObjectRef:
        """Stream one raw object into a fsynced staging file, then publish it once.

        Streaming is reserved for ``bytes-v1`` namespaces.  Canonical JSON is
        intentionally materialized through the Python oracle, preserving exact
        semantics rather than introducing a second streaming JSON encoder.
        """

        if namespace.codec != RAW_BYTES_CODEC:
            raise TemporalQDObjectStoreError(
                "put_stream is for bytes-v1 namespaces; canonical JSON uses put_json"
            )
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        if expected_sha256 is not None:
            expected_sha256 = _require_sha256(expected_sha256, label="expected SHA-256")
        if expected_byte_length is not None and (
            isinstance(expected_byte_length, bool) or int(expected_byte_length) < 0
        ):
            raise TemporalQDObjectStoreError("expected byte length must be non-negative")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="stream.", suffix=".tmp", dir=self._staging_root
        )
        temporary = Path(temporary_name)
        try:
            observed_sha256, observed_length = self._write_stream_and_fsync(
                descriptor, source, chunk_bytes=chunk_bytes
            )
            ref = ObjectRef(
                namespace=namespace,
                sha256=observed_sha256,
                byte_length=observed_length,
            )
            if expected_sha256 is not None and ref.sha256 != expected_sha256:
                raise ObjectStoreIntegrityError(
                    f"stream SHA-256 mismatch: expected {expected_sha256}, observed {ref.sha256}"
                )
            if expected_byte_length is not None and ref.byte_length != int(
                expected_byte_length
            ):
                raise ObjectStoreIntegrityError(
                    "stream byte length mismatch: "
                    f"expected {expected_byte_length}, observed {ref.byte_length}"
                )
            parent = self._ensure_object_parent(ref)
            target = parent / ref.digest_hex
            if self._path_exists(target):
                self._verify_existing_file_equals(
                    target, temporary, ref, expected_bytes=None
                )
                temporary.unlink(missing_ok=True)
                self._sync_directories({parent})
                return ref
            try:
                os.link(temporary, target)
            except FileExistsError:
                self._verify_existing_file_equals(
                    target, temporary, ref, expected_bytes=None
                )
                temporary.unlink(missing_ok=True)
            else:
                # A linked staging file remains until name durability succeeds.
                pass
            # A racer may have published the exact bytes.  Synchronize its
            # directory too before returning this accepted durable reference.
            self._sync_directories({parent})
            temporary.unlink(missing_ok=True)
            return ref
        except Exception:
            # Do not erase an incomplete/failed staging file.  It is ignored by
            # all readers and can be inspected after a restart.
            raise

    def open(self, ref: ObjectRef) -> VerifiedObjectReader:
        """Open a bounded-memory reader which verifies exact identity on close."""

        return VerifiedObjectReader(self, ref)

    def iter_bytes(
        self, ref: ObjectRef, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES
    ) -> Iterator[bytes]:
        with self.open(ref) as reader:
            yield from reader.iter_chunks(chunk_bytes=chunk_bytes)

    def get_bytes(self, ref: ObjectRef) -> bytes:
        """Hydrate exact verified bytes; use ``iter_bytes`` for large artifacts."""

        with self.open(ref) as reader:
            return reader.read()

    def get_json(self, ref: ObjectRef) -> Any:
        """Hydrate JSON only if stored bytes are the exact canonical representation."""

        if ref.namespace.codec != CANONICAL_JSON_CODEC:
            raise TemporalQDObjectStoreError("get_json requires a canonical-json-v1 reference")
        data = self.get_bytes(ref)
        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:  # Defensive after put checks.
            raise ObjectStoreIntegrityError("stored canonical JSON cannot be decoded") from exc
        if canonical_json_bytes(value) != data:
            raise ObjectStoreIntegrityError("stored JSON is not exact canonical bytes")
        return value

    def get_many(self, refs: Iterable[ObjectRef]) -> Iterator[tuple[ObjectRef, bytes]]:
        """Sequential bulk hydration without a per-object caller/API boundary."""

        for ref in refs:
            yield ref, self.get_bytes(ref)

    def verify(self, ref: ObjectRef) -> None:
        with self.open(ref) as reader:
            reader.verify()

    def verify_many(self, refs: Iterable[ObjectRef]) -> tuple[ObjectRef, ...]:
        verified: list[ObjectRef] = []
        for ref in refs:
            self.verify(ref)
            verified.append(ref)
        return tuple(verified)

    def put_manifest(
        self,
        namespace: ObjectNamespace,
        *,
        manifest_type: str,
        manifest_version: int,
        refs: Iterable[ObjectRef],
        metadata: Mapping[str, Any] | None = None,
    ) -> ObjectRef:
        if namespace.codec != CANONICAL_JSON_CODEC:
            raise TemporalQDObjectStoreError("manifests require a canonical-json-v1 namespace")
        return self.put_json(
            namespace,
            build_manifest(
                manifest_type=manifest_type,
                manifest_version=manifest_version,
                refs=refs,
                metadata=metadata,
            ),
        )

    def get_manifest(self, ref: ObjectRef) -> dict[str, Any]:
        payload = self.get_json(ref)
        if not isinstance(payload, Mapping):
            raise ObjectStoreIntegrityError("stored manifest root must be an object")
        return validate_manifest(payload)

    def partial_temp_paths(self) -> tuple[Path, ...]:
        """List ignored stale temporary files without deleting restart evidence."""

        found: list[Path] = []
        for root in (self._staging_root, self._objects_root):
            found.extend(self._walk_partial_temps(root))
        return tuple(sorted(found, key=lambda item: item.as_posix()))

    def _publish_prepared(self, prepared: PreparedObject) -> _Publication:
        parent = self._ensure_object_parent(prepared.ref)
        target = parent / prepared.ref.digest_hex
        if self._path_exists(target):
            self._verify_existing_file_equals(target, None, prepared.ref, expected_bytes=prepared.data)
            return _Publication(
                ref=prepared.ref,
                parent=parent,
                created=False,
                bytes_written=0,
                temporary=None,
            )
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{prepared.ref.digest_hex}.", suffix=".tmp", dir=parent
        )
        temporary = Path(temporary_name)
        try:
            self._write_bytes_and_fsync(descriptor, prepared.data)
            try:
                os.link(temporary, target)
            except FileExistsError:
                self._verify_existing_file_equals(
                    target, None, prepared.ref, expected_bytes=prepared.data
                )
                temporary.unlink(missing_ok=True)
                return _Publication(
                    ref=prepared.ref,
                    parent=parent,
                    created=False,
                    bytes_written=0,
                    temporary=None,
                )
            return _Publication(
                ref=prepared.ref,
                parent=parent,
                created=True,
                bytes_written=len(prepared.data),
                temporary=temporary,
            )
        except Exception:
            # A write/fsync interruption leaves a non-addressable ``.tmp`` file
            # rather than a plausible-but-partial final object.
            raise

    def _ensure_object_parent(self, ref: ObjectRef) -> Path:
        digest = ref.digest_hex
        return self._ensure_directory(
            (
                "objects",
                ref.namespace.object_type,
                f"v{ref.namespace.schema_version}",
                ref.namespace.codec,
                digest[:2],
            )
        )

    def _lexical_path(self, components: tuple[str, ...]) -> Path:
        if any(not component or component in {".", ".."} or "/" in component or "\\" in component for component in components):
            raise ObjectStorePathError("unsafe object-store path component")
        candidate = self.root.joinpath(*components)
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:  # pragma: no cover - components are validated above.
            raise ObjectStorePathError("object path escapes store root") from exc
        return candidate

    def _ensure_directory(self, components: tuple[str, ...]) -> Path:
        current = self.root
        for component in components:
            candidate = self._lexical_path(tuple(current.relative_to(self.root).parts + (component,)))
            created = False
            try:
                os.mkdir(candidate)
                created = True
            except FileExistsError:
                pass
            self._assert_existing_directory(candidate)
            if created:
                fsync_directory(current)
            current = candidate
        return current

    def _assert_existing_directory(self, path: Path) -> None:
        self._assert_path_components_safe(path, final_kind="directory")

    def _assert_path_components_safe(self, path: Path, *, final_kind: str) -> None:
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise ObjectStorePathError("path escapes object-store root") from exc
        try:
            root_status = os.lstat(self.root)
        except FileNotFoundError as exc:  # pragma: no cover - constructor creates it.
            raise ObjectStorePathError("object-store root disappeared") from exc
        if _is_link_or_reparse(root_status) or not stat.S_ISDIR(root_status.st_mode):
            raise ObjectStorePathError("object-store root is no longer a real directory")
        current = self.root
        for index, component in enumerate(relative.parts):
            current = current / component
            try:
                status = os.lstat(current)
            except FileNotFoundError as exc:
                raise ObjectStoreNotFoundError(f"object path is missing: {path}") from exc
            if _is_link_or_reparse(status):
                raise ObjectStorePathError(
                    f"symlink or junction is forbidden inside object store: {current}"
                )
            final = index == len(relative.parts) - 1
            if final:
                if final_kind == "directory" and not stat.S_ISDIR(status.st_mode):
                    raise ObjectStorePathError(f"object directory is not a directory: {current}")
                if final_kind == "file" and not stat.S_ISREG(status.st_mode):
                    raise ObjectStorePathError(f"object path is not a regular file: {current}")
            elif not stat.S_ISDIR(status.st_mode):
                raise ObjectStorePathError(f"object path parent is not a directory: {current}")
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.root)
        except (OSError, ValueError) as exc:
            raise ObjectStorePathError("object path resolves outside store root") from exc

    def _open_regular_file(self, path: Path) -> BinaryIO:
        self._assert_path_components_safe(path, final_kind="file")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ObjectStoreNotFoundError(f"object is missing: {path}") from exc
        except OSError as exc:
            raise ObjectStorePathError(f"cannot safely open object: {path}") from exc
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode):
                raise ObjectStorePathError(f"opened object is not a regular file: {path}")
            return os.fdopen(descriptor, "rb")
        except Exception:
            os.close(descriptor)
            raise

    def _verify_existing_file_equals(
        self,
        path: Path,
        incoming_path: Path | None,
        ref: ObjectRef,
        *,
        expected_bytes: bytes | None,
    ) -> None:
        """Verify an existing final object before accepting a duplicate publisher."""

        digest = hashlib.sha256()
        observed_length = 0
        expected_offset = 0
        mismatch = False
        incoming: BinaryIO | None = None
        try:
            if incoming_path is not None:
                incoming = incoming_path.open("rb")
            with self._open_regular_file(path) as existing:
                while True:
                    chunk = existing.read(DEFAULT_CHUNK_BYTES)
                    if not chunk:
                        break
                    digest.update(chunk)
                    observed_length += len(chunk)
                    if expected_bytes is not None:
                        if chunk != expected_bytes[
                            expected_offset : expected_offset + len(chunk)
                        ]:
                            mismatch = True
                        expected_offset += len(chunk)
                    elif incoming is not None:
                        if chunk != incoming.read(len(chunk)):
                            mismatch = True
                if incoming is not None and incoming.read(1):
                    mismatch = True
        finally:
            if incoming is not None:
                incoming.close()
        observed_sha256 = "sha256:" + digest.hexdigest()
        if observed_length != ref.byte_length or observed_sha256 != ref.sha256:
            raise ObjectStoreIntegrityError(
                "existing immutable object is corrupt or divergent: "
                f"{path} (observed {observed_sha256}/{observed_length}, "
                f"expected {ref.sha256}/{ref.byte_length})"
            )
        if mismatch:
            raise ObjectStoreConflictError(
                "existing object has the same SHA-256 token but not exact bytes"
            )

    @staticmethod
    def _path_exists(path: Path) -> bool:
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _write_bytes_and_fsync(descriptor: int, data: bytes) -> None:
        try:
            view = memoryview(data)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:  # pragma: no cover - ordinary files do not do this.
                    raise OSError("short write while publishing object")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _write_stream_and_fsync(
        descriptor: int, source: BinaryIO, *, chunk_bytes: int
    ) -> tuple[str, int]:
        digest = hashlib.sha256()
        total = 0
        try:
            while True:
                chunk = source.read(chunk_bytes)
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TemporalQDObjectStoreError("stream must yield bytes")
                if len(chunk) == 0:
                    break
                view = memoryview(chunk)
                digest.update(view)
                total += len(view)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:  # pragma: no cover - ordinary files do not do this.
                        raise OSError("short write while streaming object")
                    view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return "sha256:" + digest.hexdigest(), total

    @staticmethod
    def _sync_directories(directories: Iterable[Path]) -> None:
        for directory in sorted(set(directories), key=lambda item: item.as_posix()):
            fsync_directory(directory)

    def _walk_partial_temps(self, root: Path) -> list[Path]:
        """Walk only real directories; symlinked trees are never followed."""

        pending: list[Path] = []
        stack = [root]
        while stack:
            directory = stack.pop()
            self._assert_existing_directory(directory)
            with os.scandir(directory) as entries:
                for entry in entries:
                    status = entry.stat(follow_symlinks=False)
                    if _is_link_or_reparse(status):
                        continue
                    candidate = Path(entry.path)
                    if stat.S_ISDIR(status.st_mode):
                        stack.append(candidate)
                    elif stat.S_ISREG(status.st_mode) and entry.name.endswith(".tmp"):
                        pending.append(candidate)
        return pending


def _packed_batch_identity(
    refs: Sequence[ObjectRef], *, pack_sha256: str, pack_byte_length: int
) -> str:
    """Deterministic name for one immutable batch and its exact pack bytes."""

    ordered = tuple(sorted(refs, key=_ref_sort_key))
    if tuple(refs) != ordered:
        raise TemporalQDObjectStoreError("packed batch references must be sorted")
    if any(
        _ref_sort_key(left) == _ref_sort_key(right)
        for left, right in zip(ordered, ordered[1:])
    ):
        raise TemporalQDObjectStoreError("packed batch references must be unique")
    pack_sha256 = _require_sha256(pack_sha256, label="packed batch pack SHA-256")
    if isinstance(pack_byte_length, bool) or int(pack_byte_length) < 0:
        raise TemporalQDObjectStoreError("packed batch pack byte length must be non-negative")
    return sha256_bytes(
        canonical_json_bytes(
            {
                "schemaVersion": PACKED_OBJECT_PACK_SCHEMA,
                "objectRefs": [ref.as_dict() for ref in ordered],
                "packSha256": pack_sha256,
                "packByteLength": int(pack_byte_length),
            }
        )
    )


def _compressed_packed_batch_identity(
    refs: Sequence[ObjectRef],
    *,
    raw_pack_sha256: str,
    raw_pack_byte_length: int,
    compressed_pack_sha256: str,
    compressed_pack_byte_length: int,
) -> str:
    """Deterministic identity for exact raw objects plus their zlib block."""

    ordered = tuple(sorted(refs, key=_ref_sort_key))
    if tuple(refs) != ordered:
        raise TemporalQDObjectStoreError("compressed packed batch references must be sorted")
    if any(
        _ref_sort_key(left) == _ref_sort_key(right)
        for left, right in zip(ordered, ordered[1:])
    ):
        raise TemporalQDObjectStoreError("compressed packed batch references must be unique")
    raw_pack_sha256 = _require_sha256(
        raw_pack_sha256, label="compressed packed raw SHA-256"
    )
    compressed_pack_sha256 = _require_sha256(
        compressed_pack_sha256, label="compressed packed SHA-256"
    )
    for value, label in (
        (raw_pack_byte_length, "compressed packed raw byte length"),
        (compressed_pack_byte_length, "compressed packed byte length"),
    ):
        if isinstance(value, bool) or int(value) < 0:
            raise TemporalQDObjectStoreError(f"{label} must be non-negative")
    return sha256_bytes(
        canonical_json_bytes(
            {
                "schemaVersion": COMPRESSED_PACKED_OBJECT_PACK_SCHEMA,
                "compressionCodec": COMPRESSED_PACK_CODEC,
                "objectRefs": [ref.as_dict() for ref in ordered],
                "rawPackSha256": raw_pack_sha256,
                "rawPackByteLength": int(raw_pack_byte_length),
                "compressedPackSha256": compressed_pack_sha256,
                "compressedPackByteLength": int(compressed_pack_byte_length),
            }
        )
    )


class PackedVerifiedObjectReader(AbstractContextManager["PackedVerifiedObjectReader"]):
    """A bounded reader for one indexed pack slice, verified on EOF or close."""

    def __init__(self, store: "PackedTemporalQDObjectStore", ref: ObjectRef) -> None:
        self._store = store
        self.ref = ref
        self.location = store.location_for(ref)
        self._handle = store._open_pack_file(self.location)
        self._handle.seek(self.location.offset)
        self._remaining = self.location.length
        self._digest = hashlib.sha256()
        self._length = 0
        self._verified = False
        self._closed = False

    def __enter__(self) -> "PackedVerifiedObjectReader":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise
        return False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed packed object reader")
        if self._remaining == 0:
            self._finish_verification()
            return b""
        wanted = self._remaining if size is None or size < 0 else min(int(size), self._remaining)
        if wanted == 0:
            return b""
        data = self._handle.read(wanted)
        if len(data) != wanted:
            raise ObjectStoreIntegrityError(
                "packed object is truncated before indexed length: "
                f"{self.location.pack_path}"
            )
        self._digest.update(data)
        self._length += len(data)
        self._remaining -= len(data)
        if self._remaining == 0:
            self._finish_verification()
        return data

    def iter_chunks(self, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES) -> Iterator[bytes]:
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        while self._remaining:
            yield self.read(min(chunk_bytes, self._remaining))

    def verify(self) -> None:
        while self._remaining:
            self.read(min(DEFAULT_CHUNK_BYTES, self._remaining))
        self._finish_verification()

    def _finish_verification(self) -> None:
        if self._verified:
            return
        observed = "sha256:" + self._digest.hexdigest()
        if self._length != self.ref.byte_length or observed != self.ref.sha256:
            raise ObjectStoreIntegrityError(
                "packed object bytes do not match immutable reference: "
                f"expected ({self.ref.sha256}, {self.ref.byte_length}), "
                f"observed ({observed}, {self._length})"
            )
        self._verified = True

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.verify()
        finally:
            self._handle.close()
            self._closed = True


class PackedTemporalQDObjectStore(TemporalQDObjectStore):
    """Immutable packed backend compatible with the loose-store value surface.

    A packed write first publishes a fully fsynced payload pack and only then a
    canonical immutable index.  An interrupted pack therefore remains an
    unreachable orphan; no index can ever name a missing or partial pack.  The
    loose :class:`TemporalQDObjectStore` stays the Python semantic oracle and
    remains available unchanged.
    """

    def __init__(self, root: Path | str) -> None:
        super().__init__(root)
        self._packs_root = self._ensure_directory(("packs",))
        self._indexes_root = self._ensure_directory(("indexes",))
        self._locations: dict[ObjectRef, PackedObjectLocation] = {}
        self._indexes: dict[str, _PackIndex] = {}
        self._last_batch_read_metrics = {
            "objectsRead": 0,
            "objectBytesRead": 0,
            "packFileOpens": 0,
        }
        self._refresh_indexes()

    @property
    def last_batch_read_metrics(self) -> dict[str, int]:
        """Physical pack-open and logical object-read counts from ``get_many``."""

        return dict(self._last_batch_read_metrics)

    def location_for(self, ref: ObjectRef) -> PackedObjectLocation:
        if not isinstance(ref, ObjectRef):
            raise TemporalQDObjectStoreError("packed location lookup requires an ObjectRef")
        location = self._locations.get(ref)
        if location is None:
            raise ObjectStoreNotFoundError(f"packed object is missing: {ref.sha256}")
        return location

    def path_for(self, ref: ObjectRef) -> Path:
        """Return the immutable pack file containing ``ref`` (not a loose path)."""

        return self.location_for(ref).pack_path

    def put_prepared(self, prepared: PreparedObject) -> ObjectRef:
        return self.put_many((prepared,)).refs[0]

    def put_many(self, prepared_objects: Iterable[PreparedObject]) -> BatchPutResult:
        """Publish all new objects in one pack and one index, preserving input refs."""

        # A single lock covers discovery through final index publication.  If
        # two writers share one new ref, the second refresh happens only after
        # the first index is durable and therefore packs just its true delta.
        with self._packed_publication_lock():
            self._refresh_indexes()
            refs: list[ObjectRef] = []
            sources: dict[ObjectRef, _PackSource] = {}
            reused_count = 0
            for prepared in prepared_objects:
                if not isinstance(prepared, PreparedObject):
                    raise TemporalQDObjectStoreError("put_many requires PreparedObject entries")
                refs.append(prepared.ref)
                known = self._locations.get(prepared.ref)
                if known is not None:
                    self._verify_location_against_bytes(known, prepared.data)
                    reused_count += 1
                    continue
                prior = sources.get(prepared.ref)
                if prior is not None:
                    if prior.data != prepared.data:
                        raise ObjectStoreConflictError(
                            "two packed batch objects share a reference but differ in exact bytes"
                        )
                    reused_count += 1
                    continue
                sources[prepared.ref] = _PackSource(ref=prepared.ref, data=prepared.data)
            if not sources:
                return BatchPutResult(
                    refs=tuple(refs),
                    created_count=0,
                    reused_count=reused_count,
                    bytes_written=0,
                )
            ordered_sources = tuple(
                sources[ref] for ref in sorted(sources, key=_ref_sort_key)
            )
            bytes_written = self._publish_new_sources(ordered_sources)
            return BatchPutResult(
                refs=tuple(refs),
                created_count=len(ordered_sources),
                reused_count=reused_count,
                bytes_written=bytes_written,
            )

    def put_stream(
        self,
        namespace: ObjectNamespace,
        source: BinaryIO,
        *,
        expected_sha256: str | None = None,
        expected_byte_length: int | None = None,
        chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    ) -> ObjectRef:
        """Stream one raw object through a fsynced staging file into one pack."""

        if namespace.codec != RAW_BYTES_CODEC:
            raise TemporalQDObjectStoreError(
                "put_stream is for bytes-v1 namespaces; canonical JSON uses put_json"
            )
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        if expected_sha256 is not None:
            expected_sha256 = _require_sha256(expected_sha256, label="expected SHA-256")
        if expected_byte_length is not None and (
            isinstance(expected_byte_length, bool) or int(expected_byte_length) < 0
        ):
            raise TemporalQDObjectStoreError("expected byte length must be non-negative")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="packed-stream.", suffix=".tmp", dir=self._staging_root
        )
        temporary = Path(temporary_name)
        try:
            observed_sha256, observed_length = self._write_stream_and_fsync(
                descriptor, source, chunk_bytes=chunk_bytes
            )
            ref = ObjectRef(namespace, observed_sha256, observed_length)
            if expected_sha256 is not None and ref.sha256 != expected_sha256:
                raise ObjectStoreIntegrityError(
                    f"stream SHA-256 mismatch: expected {expected_sha256}, observed {ref.sha256}"
                )
            if expected_byte_length is not None and ref.byte_length != int(
                expected_byte_length
            ):
                raise ObjectStoreIntegrityError(
                    "stream byte length mismatch: "
                    f"expected {expected_byte_length}, observed {ref.byte_length}"
                )
            with self._packed_publication_lock():
                self._refresh_indexes()
                known = self._locations.get(ref)
                if known is not None:
                    self._verify_location_against_path(known, temporary)
                    temporary.unlink(missing_ok=True)
                    return ref
                self._publish_new_sources((_PackSource(ref=ref, path=temporary),))
                temporary.unlink(missing_ok=True)
                return ref
        except Exception:
            # The staging object is never addressable.  Retaining it gives
            # restart/audit evidence rather than pretending an incomplete
            # stream was a published immutable object.
            raise

    def open(self, ref: ObjectRef) -> PackedVerifiedObjectReader:
        return PackedVerifiedObjectReader(self, ref)

    def get_many(self, refs: Iterable[ObjectRef]) -> Iterator[tuple[ObjectRef, bytes]]:
        """Hydrate in caller order while holding a shared pack open when possible."""

        def iterator() -> Iterator[tuple[ObjectRef, bytes]]:
            active_path: Path | None = None
            active_handle: BinaryIO | None = None
            objects_read = 0
            object_bytes_read = 0
            pack_file_opens = 0
            try:
                for ref in refs:
                    location = self.location_for(ref)
                    if active_path != location.pack_path:
                        if active_handle is not None:
                            active_handle.close()
                        active_handle = self._open_pack_file(location)
                        active_path = location.pack_path
                        pack_file_opens += 1
                    data = self._read_location_bytes(active_handle, location)
                    objects_read += 1
                    object_bytes_read += len(data)
                    yield ref, data
            finally:
                if active_handle is not None:
                    active_handle.close()
                self._last_batch_read_metrics = {
                    "objectsRead": objects_read,
                    "objectBytesRead": object_bytes_read,
                    "packFileOpens": pack_file_opens,
                }

        return iterator()

    def partial_temp_paths(self) -> tuple[Path, ...]:
        """List ignored partial pack/index/stream files without deleting them."""

        found: list[Path] = []
        for root in (self._staging_root, self._packs_root, self._indexes_root):
            found.extend(self._walk_partial_temps(root))
        return tuple(sorted(found, key=lambda item: item.as_posix()))

    def _pack_path(self, batch_id: str) -> Path:
        digest = _digest_hex(_require_sha256(batch_id, label="packed batch identity"))
        return self._lexical_path(("packs", f"{digest}.pack"))

    def _index_path(self, batch_id: str) -> Path:
        digest = _digest_hex(_require_sha256(batch_id, label="packed batch identity"))
        return self._lexical_path(("indexes", f"{digest}.index.json"))

    def _refresh_indexes(self) -> None:
        """Validate every immutable index and pack metadata without hydrating payloads."""

        discovered_indexes: dict[str, _PackIndex] = {}
        discovered_locations: dict[ObjectRef, PackedObjectLocation] = {}
        self._assert_existing_directory(self._indexes_root)
        with os.scandir(self._indexes_root) as entries:
            index_entries = sorted(entries, key=lambda entry: entry.name)
        for entry in index_entries:
            status = entry.stat(follow_symlinks=False)
            if _is_link_or_reparse(status):
                raise ObjectStorePathError(
                    f"symlink or junction is forbidden in packed index root: {entry.path}"
                )
            if stat.S_ISDIR(status.st_mode):
                raise ObjectStorePathError(f"packed index root contains a directory: {entry.path}")
            if not stat.S_ISREG(status.st_mode):
                raise ObjectStorePathError(f"packed index root contains a non-file: {entry.path}")
            if entry.name.endswith(".tmp"):
                continue
            matched = _PACK_INDEX_FILE_NAME.fullmatch(entry.name)
            if matched is None:
                raise ObjectStoreIntegrityError(
                    f"unexpected immutable packed-index filename: {entry.name}"
                )
            index = self._load_index(Path(entry.path), batch_hex=matched.group()[:64])
            if index.batch_id in discovered_indexes:
                raise ObjectStoreIntegrityError("duplicate packed batch identity")
            for location in index.locations:
                prior = discovered_locations.get(location.ref)
                if prior is not None:
                    raise ObjectStoreIntegrityError(
                        "duplicate immutable object reference across packed indexes: "
                        f"{location.ref.sha256}"
                    )
                discovered_locations[location.ref] = location
            discovered_indexes[index.batch_id] = index
        self._indexes = discovered_indexes
        self._locations = discovered_locations

    def _load_index(self, index_path: Path, *, batch_hex: str) -> _PackIndex:
        try:
            with self._open_regular_file(index_path) as handle:
                raw = handle.read()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObjectStoreIntegrityError(
                f"packed index is partial or corrupt: {index_path}"
            ) from exc
        if not isinstance(payload, dict) or raw != canonical_json_bytes(payload):
            raise ObjectStoreIntegrityError(
                f"packed index is not exact canonical JSON: {index_path}"
            )
        expected_fields = {
            "schemaVersion",
            "batchId",
            "packFile",
            "packSha256",
            "packByteLength",
            "entries",
            "indexSha256",
        }
        if set(payload) != expected_fields or payload.get("schemaVersion") != PACKED_OBJECT_INDEX_SCHEMA:
            raise ObjectStoreIntegrityError("packed index schema/fields are invalid")
        batch_id = _require_sha256(payload.get("batchId"), label="packed index batch identity")
        if batch_id[7:] != batch_hex:
            raise ObjectStoreIntegrityError("packed index filename disagrees with batch identity")
        supplied_index_sha = _require_sha256(
            payload.get("indexSha256"), label="packed index SHA-256"
        )
        body = dict(payload)
        body.pop("indexSha256")
        expected_index_sha = sha256_bytes(canonical_json_bytes(body))
        if supplied_index_sha != expected_index_sha:
            raise ObjectStoreIntegrityError("packed index identity is stale or tampered")
        pack_file = payload.get("packFile")
        expected_pack_file = f"{batch_hex}.pack"
        if not isinstance(pack_file, str) or pack_file != expected_pack_file or not _PACK_FILE_NAME.fullmatch(pack_file):
            raise ObjectStoreIntegrityError("packed index pack filename is unsafe or divergent")
        pack_sha256 = _require_sha256(payload.get("packSha256"), label="packed pack SHA-256")
        pack_byte_length = self._require_nonnegative_integer(
            payload.get("packByteLength"), label="packed pack byte length"
        )
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list) or not raw_entries:
            raise ObjectStoreIntegrityError("packed index entries must be a non-empty list")
        locations: list[PackedObjectLocation] = []
        expected_offset = 0
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, Mapping) or set(raw_entry) != {
                "ref",
                "offset",
                "length",
            }:
                raise ObjectStoreIntegrityError("packed index entry fields are invalid")
            raw_ref = raw_entry.get("ref")
            if not isinstance(raw_ref, Mapping):
                raise ObjectStoreIntegrityError("packed index reference must be an object")
            try:
                ref = ObjectRef.from_dict(raw_ref)
            except TemporalQDObjectStoreError as exc:
                raise ObjectStoreIntegrityError("packed index reference is invalid") from exc
            offset = self._require_nonnegative_integer(raw_entry.get("offset"), label="packed offset")
            length = self._require_nonnegative_integer(raw_entry.get("length"), label="packed length")
            if length != ref.byte_length or offset != expected_offset:
                raise ObjectStoreIntegrityError(
                    "packed index has a truncated, overlapping, or non-contiguous entry"
                )
            expected_offset += length
            locations.append(
                PackedObjectLocation(
                    ref=ref,
                    batch_id=batch_id,
                    pack_path=self._pack_path(batch_id),
                    pack_sha256=pack_sha256,
                    pack_byte_length=pack_byte_length,
                    offset=offset,
                    length=length,
                )
            )
        ordered = sorted(locations, key=lambda location: _ref_sort_key(location.ref))
        if locations != ordered:
            raise ObjectStoreIntegrityError("packed index references are not deterministically sorted")
        if any(
            _ref_sort_key(left.ref) == _ref_sort_key(right.ref)
            for left, right in zip(ordered, ordered[1:])
        ):
            raise ObjectStoreIntegrityError("packed index contains duplicate object references")
        if expected_offset != pack_byte_length:
            raise ObjectStoreIntegrityError("packed index entries do not exactly cover pack bytes")
        expected_batch = _packed_batch_identity(
            [location.ref for location in locations],
            pack_sha256=pack_sha256,
            pack_byte_length=pack_byte_length,
        )
        if batch_id != expected_batch:
            raise ObjectStoreIntegrityError("packed index batch identity diverges from references")
        pack_path = self._pack_path(batch_id)
        self._assert_path_components_safe(pack_path, final_kind="file")
        if pack_path.stat().st_size != pack_byte_length:
            raise ObjectStoreIntegrityError("packed file is truncated, stale, or length-mismatched")
        return _PackIndex(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            pack_sha256=pack_sha256,
            pack_byte_length=pack_byte_length,
            locations=tuple(locations),
        )

    @staticmethod
    def _require_nonnegative_integer(value: Any, *, label: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ObjectStoreIntegrityError(f"{label} must be a non-negative integer")
        return int(value)

    def _open_pack_file(self, location: PackedObjectLocation) -> BinaryIO:
        handle = self._open_regular_file(location.pack_path)
        try:
            if os.fstat(handle.fileno()).st_size != location.pack_byte_length:
                raise ObjectStoreIntegrityError("packed file changed length after index validation")
            return handle
        except Exception:
            handle.close()
            raise

    def _read_location_bytes(self, handle: BinaryIO, location: PackedObjectLocation) -> bytes:
        handle.seek(location.offset)
        remaining = location.length
        chunks: list[bytes] = []
        digest = hashlib.sha256()
        while remaining:
            chunk = handle.read(min(DEFAULT_CHUNK_BYTES, remaining))
            if not chunk:
                raise ObjectStoreIntegrityError("packed object is truncated during batch hydration")
            chunks.append(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) != location.ref.byte_length or "sha256:" + digest.hexdigest() != location.ref.sha256:
            raise ObjectStoreIntegrityError("packed object read disagrees with immutable reference")
        return data

    def _verify_location_against_bytes(
        self, location: PackedObjectLocation, expected: bytes
    ) -> None:
        if len(expected) != location.ref.byte_length:
            raise ObjectStoreConflictError("packed duplicate has a mismatched byte length")
        with self.open(location.ref) as reader:
            offset = 0
            for chunk in reader.iter_chunks():
                if chunk != expected[offset : offset + len(chunk)]:
                    raise ObjectStoreConflictError(
                        "packed duplicate has matching reference but divergent exact bytes"
                    )
                offset += len(chunk)
        if offset != len(expected):  # pragma: no cover - reader verifies this first.
            raise ObjectStoreConflictError("packed duplicate ended before expected bytes")

    def _verify_location_against_path(
        self, location: PackedObjectLocation, expected_path: Path
    ) -> None:
        self._assert_path_components_safe(expected_path, final_kind="file")
        with expected_path.open("rb") as expected, self.open(location.ref) as reader:
            for chunk in reader.iter_chunks():
                if chunk != expected.read(len(chunk)):
                    raise ObjectStoreConflictError(
                        "packed duplicate has matching reference but divergent exact bytes"
                    )
            if expected.read(1):
                raise ObjectStoreConflictError("packed duplicate has extra expected bytes")

    def _publish_new_sources(self, sources: Sequence[_PackSource]) -> int:
        if not sources:
            return 0
        ordered = tuple(sorted(sources, key=lambda source: _ref_sort_key(source.ref)))
        if tuple(sources) != ordered:
            raise TemporalQDObjectStoreError("packed publication sources must be sorted")
        temporary_pack, entries, pack_sha256, pack_length = self._write_pack_temp(ordered)
        try:
            batch_id = _packed_batch_identity(
                [source.ref for source in ordered],
                pack_sha256=pack_sha256,
                pack_byte_length=pack_length,
            )
            pack_path = self._pack_path(batch_id)
            index_path = self._index_path(batch_id)
            pack_created = self._publish_staged_file(
                temporary_pack,
                pack_path,
                expected_sha256=pack_sha256,
                expected_length=pack_length,
            )
            index_payload = self._index_payload(
                batch_id=batch_id,
                pack_sha256=pack_sha256,
                pack_length=pack_length,
                entries=entries,
            )
            self._publish_immutable_bytes(index_path, canonical_json_bytes(index_payload))
        except Exception:
            # An already-published pack with no index is an intentionally
            # harmless orphan.  Its former temporary is removed only after the
            # durable pack link succeeds, never before.
            raise
        finally:
            # Publish helpers normally unlink after a durable link.  This
            # closes the exceptional paths too, including failed index writes.
            temporary_pack.unlink(missing_ok=True)
        # ``put_many`` already refreshed all prior immutable indexes before it
        # selected new refs.  Install this known-good, just-published index
        # directly instead of reparsing every historical index and retaining a
        # second full decoded copy during a large batch.  A subsequent write
        # and every reopen still execute the full fail-closed scan.
        self._install_published_index(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            pack_sha256=pack_sha256,
            pack_byte_length=pack_length,
            entries=entries,
        )
        # If a prior interrupted writer already published this exact pack, the
        # current writer only made its orphan reachable by publishing the
        # matching index.  Do not claim a second physical pack write.
        return pack_length if pack_created else 0

    def _install_published_index(
        self,
        *,
        batch_id: str,
        index_path: Path,
        pack_path: Path,
        pack_sha256: str,
        pack_byte_length: int,
        entries: Sequence[tuple[ObjectRef, int, int]],
    ) -> None:
        """Add a locally published index without a second all-index decode."""

        locations = tuple(
            PackedObjectLocation(
                ref=ref,
                batch_id=batch_id,
                pack_path=pack_path,
                pack_sha256=pack_sha256,
                pack_byte_length=pack_byte_length,
                offset=offset,
                length=length,
            )
            for ref, offset, length in entries
        )
        known_batch = self._indexes.get(batch_id)
        if known_batch is not None:
            if (
                known_batch.pack_path != pack_path
                or known_batch.pack_sha256 != pack_sha256
                or known_batch.pack_byte_length != pack_byte_length
                or known_batch.locations != locations
            ):
                raise ObjectStoreIntegrityError(
                    "published packed batch disagrees with a prior in-memory index"
                )
            return
        for location in locations:
            if location.ref in self._locations:
                raise ObjectStoreIntegrityError(
                    "published packed batch duplicates an immutable object reference"
                )
        index = _PackIndex(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            pack_sha256=pack_sha256,
            pack_byte_length=pack_byte_length,
            locations=locations,
        )
        self._indexes[batch_id] = index
        self._locations.update({location.ref: location for location in locations})

    def _write_pack_temp(
        self, sources: Sequence[_PackSource]
    ) -> tuple[Path, list[tuple[ObjectRef, int, int]], str, int]:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="pack.", suffix=".tmp", dir=self._packs_root
        )
        temporary = Path(temporary_name)
        pack_digest = hashlib.sha256()
        offset = 0
        entries: list[tuple[ObjectRef, int, int]] = []
        succeeded = False
        try:
            for source in sources:
                object_digest = hashlib.sha256()
                object_length = 0
                for chunk in self._source_chunks(source):
                    view = memoryview(chunk)
                    pack_digest.update(view)
                    object_digest.update(view)
                    object_length += len(view)
                    self._write_view(descriptor, view)
                observed = "sha256:" + object_digest.hexdigest()
                if object_length != source.ref.byte_length or observed != source.ref.sha256:
                    raise ObjectStoreIntegrityError(
                        "packed source changed while creating immutable pack"
                    )
                entries.append((source.ref, offset, object_length))
                offset += object_length
            os.fsync(descriptor)
            succeeded = True
        finally:
            try:
                os.close(descriptor)
            except Exception:
                succeeded = False
                raise
            finally:
                if not succeeded:
                    temporary.unlink(missing_ok=True)
        return temporary, entries, "sha256:" + pack_digest.hexdigest(), offset

    def _source_chunks(self, source: _PackSource) -> Iterator[bytes]:
        if source.data is not None:
            for start in range(0, len(source.data), DEFAULT_CHUNK_BYTES):
                yield source.data[start : start + DEFAULT_CHUNK_BYTES]
            return
        assert source.path is not None
        # Use the same no-follow open primitive as normal reads so a staging
        # path cannot be swapped for a link between validation and opening.
        with self._open_regular_file(source.path) as handle:
            while True:
                chunk = handle.read(DEFAULT_CHUNK_BYTES)
                if not chunk:
                    return
                yield chunk

    @staticmethod
    def _write_view(descriptor: int, view: memoryview) -> None:
        while view:
            written = os.write(descriptor, view)
            if written <= 0:  # pragma: no cover - ordinary files do not do this.
                raise OSError("short write while creating packed object file")
            view = view[written:]

    def _publish_staged_file(
        self,
        temporary: Path,
        target: Path,
        *,
        expected_sha256: str,
        expected_length: int,
    ) -> bool:
        """Create-only publish of one fsynced staged pack/index file."""

        if self._path_exists(target):
            self._verify_file_against_path(
                target,
                temporary,
                expected_sha256=expected_sha256,
                expected_length=expected_length,
            )
            temporary.unlink(missing_ok=True)
            self._sync_directories({target.parent})
            return False
        try:
            os.link(temporary, target)
        except FileExistsError:
            self._verify_file_against_path(
                target,
                temporary,
                expected_sha256=expected_sha256,
                expected_length=expected_length,
            )
            temporary.unlink(missing_ok=True)
            self._sync_directories({target.parent})
            return False
        self._sync_directories({target.parent})
        temporary.unlink(missing_ok=True)
        return True

    def _publish_immutable_bytes(self, target: Path, data: bytes) -> bool:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            self._write_bytes_and_fsync(descriptor, data)
            return self._publish_staged_file(
                temporary,
                target,
                expected_sha256=sha256_bytes(data),
                expected_length=len(data),
            )
        finally:
            # A failed index temp must not be mistaken for an index on reopen.
            temporary.unlink(missing_ok=True)

    def _verify_file_against_path(
        self,
        target: Path,
        expected_path: Path,
        *,
        expected_sha256: str,
        expected_length: int,
    ) -> None:
        digest = hashlib.sha256()
        length = 0
        mismatch = False
        with expected_path.open("rb") as expected, self._open_regular_file(target) as actual:
            while True:
                chunk = actual.read(DEFAULT_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                length += len(chunk)
                if chunk != expected.read(len(chunk)):
                    mismatch = True
            if expected.read(1):
                mismatch = True
        observed = "sha256:" + digest.hexdigest()
        if length != expected_length or observed != expected_sha256:
            raise ObjectStoreIntegrityError(
                f"immutable packed file is stale, truncated, or tampered: {target}"
            )
        if mismatch:
            raise ObjectStoreConflictError(
                f"immutable packed file differs despite matching expected identity: {target}"
            )

    def _index_payload(
        self,
        *,
        batch_id: str,
        pack_sha256: str,
        pack_length: int,
        entries: Sequence[tuple[ObjectRef, int, int]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schemaVersion": PACKED_OBJECT_INDEX_SCHEMA,
            "batchId": batch_id,
            "packFile": self._pack_path(batch_id).name,
            "packSha256": pack_sha256,
            "packByteLength": pack_length,
            "entries": [
                {"ref": ref.as_dict(), "offset": offset, "length": length}
                for ref, offset, length in entries
            ],
        }
        payload["indexSha256"] = sha256_bytes(canonical_json_bytes(payload))
        return payload


# Alternative spelling retained solely for future backend injection sites.
TemporalQDPackedObjectStore = PackedTemporalQDObjectStore


class CompressedPackedVerifiedObjectReader(
    AbstractContextManager["CompressedPackedVerifiedObjectReader"]
):
    """Verified reader for one raw slice extracted from a compressed block.

    Extraction verifies the complete compressed block before exposing bytes, but
    retains only the requested object's raw bytes.  This makes ``open`` safe
    for a large batch without materializing the entire decompressed pack.
    """

    def __init__(self, store: "CompressedPackedTemporalQDObjectStore", ref: ObjectRef) -> None:
        self._store = store
        self.ref = ref
        self.location = store.location_for(ref)
        self._data = store._decode_block(self.location.batch_id, (self.location,))[ref]
        self._buffer = io.BytesIO(self._data)
        self._digest = hashlib.sha256()
        self._length = 0
        self._verified = False
        self._closed = False

    def __enter__(self) -> "CompressedPackedVerifiedObjectReader":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise
        return False

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed compressed object reader")
        data = self._buffer.read(size)
        if data:
            self._digest.update(data)
            self._length += len(data)
        if self._buffer.tell() == len(self._data):
            self._finish_verification()
        return data

    def iter_chunks(self, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES) -> Iterator[bytes]:
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        while self._buffer.tell() < len(self._data):
            yield self.read(chunk_bytes)

    def verify(self) -> None:
        while self._buffer.tell() < len(self._data):
            self.read(DEFAULT_CHUNK_BYTES)
        self._finish_verification()

    def _finish_verification(self) -> None:
        if self._verified:
            return
        observed = "sha256:" + self._digest.hexdigest()
        if self._length != self.ref.byte_length or observed != self.ref.sha256:
            raise ObjectStoreIntegrityError(
                "compressed packed object bytes do not match immutable reference: "
                f"expected ({self.ref.sha256}, {self.ref.byte_length}), "
                f"observed ({observed}, {self._length})"
            )
        self._verified = True

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.verify()
        finally:
            self._buffer.close()
            self._closed = True


class CompressedPackedTemporalQDObjectStore(PackedTemporalQDObjectStore):
    """Optional immutable zlib-block backend for highly repetitive batch blobs.

    Object identities remain the existing SHA-256/length of *uncompressed*
    exact bytes.  Every new ``put_many`` batch writes a single deterministic
    zlib stream over its ref-sorted raw objects, then publishes a canonical
    immutable index only after the compressed pack is durable.  The loose and
    uncompressed packed stores intentionally remain independent oracles.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        max_raw_block_bytes: int = MAX_COMPRESSED_PACK_RAW_BYTES,
    ) -> None:
        if (
            isinstance(max_raw_block_bytes, bool)
            or not isinstance(max_raw_block_bytes, int)
            or max_raw_block_bytes < 1
        ):
            raise ValueError("max_raw_block_bytes must be a positive integer")
        # Do not call PackedTemporalQDObjectStore.__init__: it would scan the
        # uncompressed schema before the compressed roots have been selected.
        TemporalQDObjectStore.__init__(self, root)
        self._max_raw_block_bytes = max_raw_block_bytes
        self._packs_root = self._ensure_directory(("compressed-packs",))
        self._indexes_root = self._ensure_directory(("compressed-indexes",))
        self._locations: dict[ObjectRef, CompressedPackedObjectLocation] = {}
        self._indexes: dict[str, _CompressedPackIndex] = {}
        self._last_batch_read_metrics = {
            "objectsRead": 0,
            "objectBytesRead": 0,
            "packFileOpens": 0,
        }
        self._refresh_indexes()

    @property
    def last_batch_read_metrics(self) -> dict[str, int]:
        return dict(self._last_batch_read_metrics)

    def location_for(self, ref: ObjectRef) -> CompressedPackedObjectLocation:
        if not isinstance(ref, ObjectRef):
            raise TemporalQDObjectStoreError("compressed packed lookup requires an ObjectRef")
        location = self._locations.get(ref)
        if location is None:
            raise ObjectStoreNotFoundError(f"compressed packed object is missing: {ref.sha256}")
        return location

    def path_for(self, ref: ObjectRef) -> Path:
        """Return the immutable compressed block containing ``ref``."""

        return self.location_for(ref).pack_path

    def put_many(self, prepared_objects: Iterable[PreparedObject]) -> BatchPutResult:
        """Write all distinct new refs in this call into one zlib block."""

        with self._packed_publication_lock():
            self._refresh_indexes()
            refs: list[ObjectRef] = []
            sources: dict[ObjectRef, _PackSource] = {}
            reused_count = 0
            for prepared in prepared_objects:
                if not isinstance(prepared, PreparedObject):
                    raise TemporalQDObjectStoreError("put_many requires PreparedObject entries")
                refs.append(prepared.ref)
                known = self._locations.get(prepared.ref)
                if known is not None:
                    self._verify_location_against_bytes(known, prepared.data)
                    reused_count += 1
                    continue
                prior = sources.get(prepared.ref)
                if prior is not None:
                    if prior.data != prepared.data:
                        raise ObjectStoreConflictError(
                            "two compressed packed batch objects share a reference "
                            "but differ in exact bytes"
                        )
                    reused_count += 1
                    continue
                sources[prepared.ref] = _PackSource(ref=prepared.ref, data=prepared.data)
            if not sources:
                return BatchPutResult(
                    refs=tuple(refs),
                    created_count=0,
                    reused_count=reused_count,
                    bytes_written=0,
                )
            ordered_sources = tuple(
                sources[ref] for ref in sorted(sources, key=_ref_sort_key)
            )
            raw_preflight_bytes = sum(source.ref.byte_length for source in ordered_sources)
            if raw_preflight_bytes > self._max_raw_block_bytes:
                raise ObjectStoreIntegrityError(
                    "compressed packed batch exceeds raw-block safety limit"
                )
            bytes_written = self._publish_compressed_sources(ordered_sources)
            return BatchPutResult(
                refs=tuple(refs),
                created_count=len(ordered_sources),
                reused_count=reused_count,
                bytes_written=bytes_written,
            )

    def put_stream(
        self,
        namespace: ObjectNamespace,
        source: BinaryIO,
        *,
        expected_sha256: str | None = None,
        expected_byte_length: int | None = None,
        chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    ) -> ObjectRef:
        """Stage one raw stream, then compress it into its own immutable block."""

        if namespace.codec != RAW_BYTES_CODEC:
            raise TemporalQDObjectStoreError(
                "put_stream is for bytes-v1 namespaces; canonical JSON uses put_json"
            )
        if chunk_bytes < 1:
            raise ValueError("chunk_bytes must be positive")
        if expected_sha256 is not None:
            expected_sha256 = _require_sha256(expected_sha256, label="expected SHA-256")
        if expected_byte_length is not None and (
            isinstance(expected_byte_length, bool) or int(expected_byte_length) < 0
        ):
            raise TemporalQDObjectStoreError("expected byte length must be non-negative")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="compressed-stream.", suffix=".tmp", dir=self._staging_root
        )
        temporary = Path(temporary_name)
        try:
            observed_sha256, observed_length = self._write_stream_and_fsync(
                descriptor, source, chunk_bytes=chunk_bytes
            )
            ref = ObjectRef(namespace, observed_sha256, observed_length)
            if expected_sha256 is not None and ref.sha256 != expected_sha256:
                raise ObjectStoreIntegrityError(
                    f"stream SHA-256 mismatch: expected {expected_sha256}, observed {ref.sha256}"
                )
            if expected_byte_length is not None and ref.byte_length != int(
                expected_byte_length
            ):
                raise ObjectStoreIntegrityError(
                    "stream byte length mismatch: "
                    f"expected {expected_byte_length}, observed {ref.byte_length}"
                )
            if ref.byte_length > self._max_raw_block_bytes:
                raise ObjectStoreIntegrityError(
                    "stream exceeds compressed packed raw-block safety limit"
                )
            with self._packed_publication_lock():
                self._refresh_indexes()
                known = self._locations.get(ref)
                if known is not None:
                    self._verify_location_against_path(known, temporary)
                    temporary.unlink(missing_ok=True)
                    return ref
                self._publish_compressed_sources((_PackSource(ref=ref, path=temporary),))
                temporary.unlink(missing_ok=True)
                return ref
        except Exception:
            # A failed staging source is ignored by readers and retained for
            # audit/recovery, exactly like the loose and packed backends.
            raise

    def open(self, ref: ObjectRef) -> CompressedPackedVerifiedObjectReader:
        return CompressedPackedVerifiedObjectReader(self, ref)

    def get_many(self, refs: Iterable[ObjectRef]) -> Iterator[tuple[ObjectRef, bytes]]:
        """Decode each requested compressed block once and preserve caller order."""

        def iterator() -> Iterator[tuple[ObjectRef, bytes]]:
            refs_tuple = tuple(refs)
            groups: dict[str, dict[ObjectRef, CompressedPackedObjectLocation]] = {}
            objects_read = 0
            object_bytes_read = 0
            pack_file_opens = 0
            try:
                for ref in refs_tuple:
                    location = self.location_for(ref)
                    groups.setdefault(location.batch_id, {})[ref] = location
                values: dict[ObjectRef, bytes] = {}
                for batch_id, locations in groups.items():
                    values.update(self._decode_block(batch_id, tuple(locations.values())))
                    pack_file_opens += 1
                for ref in refs_tuple:
                    data = values[ref]
                    objects_read += 1
                    object_bytes_read += len(data)
                    yield ref, data
            finally:
                self._last_batch_read_metrics = {
                    "objectsRead": objects_read,
                    "objectBytesRead": object_bytes_read,
                    "packFileOpens": pack_file_opens,
                }

        return iterator()

    def partial_temp_paths(self) -> tuple[Path, ...]:
        found: list[Path] = []
        for root in (self._staging_root, self._packs_root, self._indexes_root):
            found.extend(self._walk_partial_temps(root))
        return tuple(sorted(found, key=lambda item: item.as_posix()))

    def _pack_path(self, batch_id: str) -> Path:
        digest = _digest_hex(_require_sha256(batch_id, label="compressed batch identity"))
        return self._lexical_path(("compressed-packs", f"{digest}.zpack"))

    def _index_path(self, batch_id: str) -> Path:
        digest = _digest_hex(_require_sha256(batch_id, label="compressed batch identity"))
        return self._lexical_path(("compressed-indexes", f"{digest}.zindex.json"))

    def _refresh_indexes(self) -> None:
        """Fail closed on every index while avoiding raw-block hydration."""

        discovered_indexes: dict[str, _CompressedPackIndex] = {}
        discovered_locations: dict[ObjectRef, CompressedPackedObjectLocation] = {}
        self._assert_existing_directory(self._indexes_root)
        with os.scandir(self._indexes_root) as entries:
            index_entries = sorted(entries, key=lambda entry: entry.name)
        for entry in index_entries:
            status = entry.stat(follow_symlinks=False)
            if _is_link_or_reparse(status):
                raise ObjectStorePathError(
                    f"symlink or junction is forbidden in compressed index root: {entry.path}"
                )
            if stat.S_ISDIR(status.st_mode):
                raise ObjectStorePathError(
                    f"compressed index root contains a directory: {entry.path}"
                )
            if not stat.S_ISREG(status.st_mode):
                raise ObjectStorePathError(
                    f"compressed index root contains a non-file: {entry.path}"
                )
            if entry.name.endswith(".tmp"):
                continue
            matched = _COMPRESSED_PACK_INDEX_FILE_NAME.fullmatch(entry.name)
            if matched is None:
                raise ObjectStoreIntegrityError(
                    f"unexpected immutable compressed-index filename: {entry.name}"
                )
            index = self._load_index(Path(entry.path), batch_hex=matched.group()[:64])
            if index.batch_id in discovered_indexes:
                raise ObjectStoreIntegrityError("duplicate compressed packed batch identity")
            for location in index.locations:
                if location.ref in discovered_locations:
                    raise ObjectStoreIntegrityError(
                        "duplicate immutable object reference across compressed indexes: "
                        f"{location.ref.sha256}"
                    )
                discovered_locations[location.ref] = location
            discovered_indexes[index.batch_id] = index
        self._indexes = discovered_indexes
        self._locations = discovered_locations

    def _load_index(self, index_path: Path, *, batch_hex: str) -> _CompressedPackIndex:
        try:
            with self._open_regular_file(index_path) as handle:
                raw = handle.read()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ObjectStoreIntegrityError(
                f"compressed packed index is partial or corrupt: {index_path}"
            ) from exc
        if not isinstance(payload, dict) or raw != canonical_json_bytes(payload):
            raise ObjectStoreIntegrityError(
                f"compressed packed index is not exact canonical JSON: {index_path}"
            )
        expected_fields = {
            "schemaVersion",
            "batchId",
            "packFile",
            "compressionCodec",
            "rawPackSha256",
            "rawPackByteLength",
            "compressedPackSha256",
            "compressedPackByteLength",
            "entries",
            "indexSha256",
        }
        if (
            set(payload) != expected_fields
            or payload.get("schemaVersion") != COMPRESSED_PACKED_OBJECT_INDEX_SCHEMA
            or payload.get("compressionCodec") != COMPRESSED_PACK_CODEC
        ):
            raise ObjectStoreIntegrityError("compressed packed index schema/fields are invalid")
        batch_id = _require_sha256(
            payload.get("batchId"), label="compressed index batch identity"
        )
        if batch_id[7:] != batch_hex:
            raise ObjectStoreIntegrityError(
                "compressed index filename disagrees with batch identity"
            )
        supplied_index_sha = _require_sha256(
            payload.get("indexSha256"), label="compressed index SHA-256"
        )
        body = dict(payload)
        body.pop("indexSha256")
        if supplied_index_sha != sha256_bytes(canonical_json_bytes(body)):
            raise ObjectStoreIntegrityError("compressed index identity is stale or tampered")
        pack_file = payload.get("packFile")
        expected_pack_file = f"{batch_hex}.zpack"
        if (
            not isinstance(pack_file, str)
            or pack_file != expected_pack_file
            or not _COMPRESSED_PACK_FILE_NAME.fullmatch(pack_file)
        ):
            raise ObjectStoreIntegrityError(
                "compressed index pack filename is unsafe or divergent"
            )
        raw_pack_sha256 = _require_sha256(
            payload.get("rawPackSha256"), label="compressed raw pack SHA-256"
        )
        raw_pack_byte_length = self._require_nonnegative_integer(
            payload.get("rawPackByteLength"), label="compressed raw pack byte length"
        )
        if raw_pack_byte_length > self._max_raw_block_bytes:
            raise ObjectStoreIntegrityError(
                "compressed raw pack declared length exceeds safety limit"
            )
        compressed_pack_sha256 = _require_sha256(
            payload.get("compressedPackSha256"), label="compressed pack SHA-256"
        )
        compressed_pack_byte_length = self._require_nonnegative_integer(
            payload.get("compressedPackByteLength"),
            label="compressed pack byte length",
        )
        # zlib's incompressible expansion is tiny; this bounds a malicious
        # declared compressed length before any block is opened or decoded.
        if compressed_pack_byte_length > raw_pack_byte_length + max(
            1024 * 1024, raw_pack_byte_length // 100 + 65_536
        ):
            raise ObjectStoreIntegrityError(
                "compressed pack declared length is implausibly larger than raw bytes"
            )
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list) or not raw_entries:
            raise ObjectStoreIntegrityError("compressed index entries must be a non-empty list")
        pack_path = self._pack_path(batch_id)
        locations: list[CompressedPackedObjectLocation] = []
        expected_offset = 0
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, Mapping) or set(raw_entry) != {
                "ref",
                "offset",
                "length",
            }:
                raise ObjectStoreIntegrityError("compressed index entry fields are invalid")
            raw_ref = raw_entry.get("ref")
            if not isinstance(raw_ref, Mapping):
                raise ObjectStoreIntegrityError("compressed index reference must be an object")
            try:
                ref = ObjectRef.from_dict(raw_ref)
            except TemporalQDObjectStoreError as exc:
                raise ObjectStoreIntegrityError("compressed index reference is invalid") from exc
            offset = self._require_nonnegative_integer(
                raw_entry.get("offset"), label="compressed raw offset"
            )
            length = self._require_nonnegative_integer(
                raw_entry.get("length"), label="compressed raw length"
            )
            if length != ref.byte_length or offset != expected_offset:
                raise ObjectStoreIntegrityError(
                    "compressed index has a truncated, overlapping, or non-contiguous entry"
                )
            expected_offset += length
            locations.append(
                CompressedPackedObjectLocation(
                    ref=ref,
                    batch_id=batch_id,
                    pack_path=pack_path,
                    raw_pack_sha256=raw_pack_sha256,
                    raw_pack_byte_length=raw_pack_byte_length,
                    compressed_pack_sha256=compressed_pack_sha256,
                    compressed_pack_byte_length=compressed_pack_byte_length,
                    offset=offset,
                    length=length,
                )
            )
        ordered = sorted(locations, key=lambda location: _ref_sort_key(location.ref))
        if locations != ordered:
            raise ObjectStoreIntegrityError(
                "compressed index references are not deterministically sorted"
            )
        if any(
            _ref_sort_key(left.ref) == _ref_sort_key(right.ref)
            for left, right in zip(ordered, ordered[1:])
        ):
            raise ObjectStoreIntegrityError("compressed index contains duplicate object references")
        if expected_offset != raw_pack_byte_length:
            raise ObjectStoreIntegrityError(
                "compressed index entries do not exactly cover raw pack bytes"
            )
        expected_batch = _compressed_packed_batch_identity(
            [location.ref for location in locations],
            raw_pack_sha256=raw_pack_sha256,
            raw_pack_byte_length=raw_pack_byte_length,
            compressed_pack_sha256=compressed_pack_sha256,
            compressed_pack_byte_length=compressed_pack_byte_length,
        )
        if batch_id != expected_batch:
            raise ObjectStoreIntegrityError(
                "compressed index batch identity diverges from block metadata"
            )
        self._assert_path_components_safe(pack_path, final_kind="file")
        if pack_path.stat().st_size != compressed_pack_byte_length:
            raise ObjectStoreIntegrityError(
                "compressed packed file is truncated, stale, or length-mismatched"
            )
        return _CompressedPackIndex(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            raw_pack_sha256=raw_pack_sha256,
            raw_pack_byte_length=raw_pack_byte_length,
            compressed_pack_sha256=compressed_pack_sha256,
            compressed_pack_byte_length=compressed_pack_byte_length,
            locations=tuple(locations),
        )

    def _open_compressed_pack_file(self, index: _CompressedPackIndex) -> BinaryIO:
        handle = self._open_regular_file(index.pack_path)
        try:
            if os.fstat(handle.fileno()).st_size != index.compressed_pack_byte_length:
                raise ObjectStoreIntegrityError(
                    "compressed packed file changed length after index validation"
                )
            return handle
        except Exception:
            handle.close()
            raise

    def _decode_block(
        self,
        batch_id: str,
        locations: Sequence[CompressedPackedObjectLocation],
    ) -> dict[ObjectRef, bytes]:
        """Stream one zlib block once, validating it while retaining requested slices."""

        index = self._indexes.get(batch_id)
        if index is None:
            raise ObjectStoreNotFoundError(f"compressed packed batch is missing: {batch_id}")
        requested: dict[ObjectRef, CompressedPackedObjectLocation] = {}
        for location in locations:
            known = self._locations.get(location.ref)
            if known != location or location.batch_id != batch_id:
                raise ObjectStoreIntegrityError(
                    "compressed packed location is absent or disagrees with index"
                )
            requested[location.ref] = location
        if not requested:
            return {}
        selected = tuple(sorted(requested.values(), key=lambda location: location.offset))
        captures = {location.ref: bytearray() for location in selected}
        selected_index = 0
        raw_length = 0
        raw_digest = hashlib.sha256()
        compressed_digest = hashlib.sha256()
        decoder = zlib.decompressobj(wbits=zlib.MAX_WBITS)

        def decode_allowance() -> int:
            # Never let a small compressed chunk expand into the entire raw
            # block in one allocation.  The +1 preserves an immediate guard
            # against output beyond the declared exact raw length.
            return min(
                DEFAULT_CHUNK_BYTES,
                max(1, index.raw_pack_byte_length - raw_length + 1),
            )

        def consume_raw(raw: bytes) -> None:
            nonlocal raw_length, selected_index
            if not raw:
                return
            start = raw_length
            end = start + len(raw)
            if end > index.raw_pack_byte_length:
                raise ObjectStoreIntegrityError(
                    "compressed block expands beyond declared raw length"
                )
            raw_digest.update(raw)
            raw_length = end
            while selected_index < len(selected):
                location = selected[selected_index]
                location_end = location.offset + location.length
                if location_end <= start:
                    selected_index += 1
                    continue
                if location.offset >= end:
                    break
                slice_start = max(location.offset, start) - start
                slice_end = min(location_end, end) - start
                captures[location.ref].extend(raw[slice_start:slice_end])
                if location_end <= end:
                    selected_index += 1
                else:
                    break

        try:
            with self._open_compressed_pack_file(index) as handle:
                compressed_remaining = index.compressed_pack_byte_length
                while compressed_remaining:
                    if decoder.eof:
                        raise ObjectStoreIntegrityError(
                            "compressed block contains trailing bytes after zlib stream"
                        )
                    chunk = handle.read(min(DEFAULT_CHUNK_BYTES, compressed_remaining))
                    if not chunk:
                        raise ObjectStoreIntegrityError("compressed block is truncated during read")
                    compressed_remaining -= len(chunk)
                    compressed_digest.update(chunk)
                    pending = chunk
                    while pending:
                        raw = decoder.decompress(pending, decode_allowance())
                        pending = decoder.unconsumed_tail
                        consume_raw(raw)
                        if decoder.eof:
                            if decoder.unused_data or pending:
                                raise ObjectStoreIntegrityError(
                                    "compressed block has trailing or ambiguous zlib bytes"
                                )
                            break
                        if pending and not raw:
                            raise ObjectStoreIntegrityError(
                                "compressed block decoder made no bounded progress"
                            )
                consume_raw(decoder.flush(decode_allowance()))
        except zlib.error as exc:
            raise ObjectStoreIntegrityError("compressed block cannot be safely decompressed") from exc
        if not decoder.eof:
            raise ObjectStoreIntegrityError("compressed block is truncated before zlib EOF")
        if raw_length != index.raw_pack_byte_length:
            raise ObjectStoreIntegrityError(
                "compressed block raw length disagrees with immutable index"
            )
        observed_raw_sha = "sha256:" + raw_digest.hexdigest()
        if observed_raw_sha != index.raw_pack_sha256:
            raise ObjectStoreIntegrityError(
                "compressed block raw SHA-256 disagrees with immutable index"
            )
        observed_compressed_sha = "sha256:" + compressed_digest.hexdigest()
        if observed_compressed_sha != index.compressed_pack_sha256:
            raise ObjectStoreIntegrityError(
                "compressed block SHA-256 disagrees with immutable index"
            )
        values: dict[ObjectRef, bytes] = {}
        for location in selected:
            # Move each capture into its final immutable value before handling
            # the next one.  Keeping both full dictionaries alive until the
            # end would briefly double a large arbitrary-order get_many read.
            data = bytes(captures.pop(location.ref))
            if len(data) != location.length or sha256_bytes(data) != location.ref.sha256:
                raise ObjectStoreIntegrityError(
                    "compressed object slice disagrees with immutable reference"
                )
            values[location.ref] = data
        return values

    def _verify_location_against_bytes(
        self, location: CompressedPackedObjectLocation, expected: bytes
    ) -> None:
        if len(expected) != location.ref.byte_length:
            raise ObjectStoreConflictError("compressed packed duplicate has mismatched length")
        actual = self._decode_block(location.batch_id, (location,))[location.ref]
        if actual != expected:
            raise ObjectStoreConflictError(
                "compressed packed duplicate has matching reference but divergent exact bytes"
            )

    def _verify_location_against_path(
        self, location: CompressedPackedObjectLocation, expected_path: Path
    ) -> None:
        self._assert_path_components_safe(expected_path, final_kind="file")
        actual = self._decode_block(location.batch_id, (location,))[location.ref]
        with self._open_regular_file(expected_path) as expected:
            if expected.read() != actual:
                raise ObjectStoreConflictError(
                    "compressed packed duplicate has matching reference but divergent exact bytes"
                )

    def _publish_compressed_sources(self, sources: Sequence[_PackSource]) -> int:
        if not sources:
            return 0
        ordered = tuple(sorted(sources, key=lambda source: _ref_sort_key(source.ref)))
        if tuple(sources) != ordered:
            raise TemporalQDObjectStoreError(
                "compressed packed publication sources must be sorted"
            )
        if sum(source.ref.byte_length for source in ordered) > self._max_raw_block_bytes:
            raise ObjectStoreIntegrityError(
                "compressed packed batch exceeds raw-block safety limit"
            )
        (
            temporary_pack,
            entries,
            raw_pack_sha256,
            raw_pack_length,
            compressed_pack_sha256,
            compressed_pack_length,
        ) = self._write_compressed_pack_temp(ordered)
        try:
            batch_id = _compressed_packed_batch_identity(
                [source.ref for source in ordered],
                raw_pack_sha256=raw_pack_sha256,
                raw_pack_byte_length=raw_pack_length,
                compressed_pack_sha256=compressed_pack_sha256,
                compressed_pack_byte_length=compressed_pack_length,
            )
            pack_path = self._pack_path(batch_id)
            index_path = self._index_path(batch_id)
            pack_created = self._publish_staged_file(
                temporary_pack,
                pack_path,
                expected_sha256=compressed_pack_sha256,
                expected_length=compressed_pack_length,
            )
            index_payload = self._compressed_index_payload(
                batch_id=batch_id,
                raw_pack_sha256=raw_pack_sha256,
                raw_pack_length=raw_pack_length,
                compressed_pack_sha256=compressed_pack_sha256,
                compressed_pack_length=compressed_pack_length,
                entries=entries,
            )
            self._publish_immutable_bytes(index_path, canonical_json_bytes(index_payload))
        except Exception:
            # A durable compressed pack with no final index is unreachable and
            # safely ignored after interruption; never overwrite it.
            raise
        finally:
            temporary_pack.unlink(missing_ok=True)
        self._install_compressed_published_index(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            raw_pack_sha256=raw_pack_sha256,
            raw_pack_length=raw_pack_length,
            compressed_pack_sha256=compressed_pack_sha256,
            compressed_pack_length=compressed_pack_length,
            entries=entries,
        )
        return raw_pack_length if pack_created else 0

    def _install_compressed_published_index(
        self,
        *,
        batch_id: str,
        index_path: Path,
        pack_path: Path,
        raw_pack_sha256: str,
        raw_pack_length: int,
        compressed_pack_sha256: str,
        compressed_pack_length: int,
        entries: Sequence[tuple[ObjectRef, int, int]],
    ) -> None:
        locations = tuple(
            CompressedPackedObjectLocation(
                ref=ref,
                batch_id=batch_id,
                pack_path=pack_path,
                raw_pack_sha256=raw_pack_sha256,
                raw_pack_byte_length=raw_pack_length,
                compressed_pack_sha256=compressed_pack_sha256,
                compressed_pack_byte_length=compressed_pack_length,
                offset=offset,
                length=length,
            )
            for ref, offset, length in entries
        )
        known_batch = self._indexes.get(batch_id)
        if known_batch is not None:
            if (
                known_batch.pack_path != pack_path
                or known_batch.raw_pack_sha256 != raw_pack_sha256
                or known_batch.raw_pack_byte_length != raw_pack_length
                or known_batch.compressed_pack_sha256 != compressed_pack_sha256
                or known_batch.compressed_pack_byte_length != compressed_pack_length
                or known_batch.locations != locations
            ):
                raise ObjectStoreIntegrityError(
                    "published compressed batch disagrees with prior in-memory index"
                )
            return
        for location in locations:
            if location.ref in self._locations:
                raise ObjectStoreIntegrityError(
                    "published compressed batch duplicates immutable object reference"
                )
        self._indexes[batch_id] = _CompressedPackIndex(
            batch_id=batch_id,
            index_path=index_path,
            pack_path=pack_path,
            raw_pack_sha256=raw_pack_sha256,
            raw_pack_byte_length=raw_pack_length,
            compressed_pack_sha256=compressed_pack_sha256,
            compressed_pack_byte_length=compressed_pack_length,
            locations=locations,
        )
        self._locations.update({location.ref: location for location in locations})

    def _write_compressed_pack_temp(
        self, sources: Sequence[_PackSource]
    ) -> tuple[Path, list[tuple[ObjectRef, int, int]], str, int, str, int]:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix="zpack.", suffix=".tmp", dir=self._packs_root
        )
        temporary = Path(temporary_name)
        raw_digest = hashlib.sha256()
        compressed_digest = hashlib.sha256()
        compressor = zlib.compressobj(
            level=1,
            method=zlib.DEFLATED,
            wbits=zlib.MAX_WBITS,
            memLevel=8,
            strategy=zlib.Z_DEFAULT_STRATEGY,
        )
        raw_offset = 0
        compressed_length = 0
        entries: list[tuple[ObjectRef, int, int]] = []

        def write_compressed(data: bytes) -> None:
            nonlocal compressed_length
            if not data:
                return
            compressed_digest.update(data)
            compressed_length += len(data)
            self._write_view(descriptor, memoryview(data))

        succeeded = False
        try:
            for source in sources:
                object_digest = hashlib.sha256()
                object_length = 0
                for chunk in self._source_chunks(source):
                    raw_digest.update(chunk)
                    object_digest.update(chunk)
                    object_length += len(chunk)
                    write_compressed(compressor.compress(chunk))
                observed = "sha256:" + object_digest.hexdigest()
                if object_length != source.ref.byte_length or observed != source.ref.sha256:
                    raise ObjectStoreIntegrityError(
                        "compressed packed source changed while creating immutable block"
                    )
                entries.append((source.ref, raw_offset, object_length))
                raw_offset += object_length
            write_compressed(compressor.flush(zlib.Z_FINISH))
            os.fsync(descriptor)
            succeeded = True
        finally:
            try:
                os.close(descriptor)
            except Exception:
                succeeded = False
                raise
            finally:
                if not succeeded:
                    temporary.unlink(missing_ok=True)
        return (
            temporary,
            entries,
            "sha256:" + raw_digest.hexdigest(),
            raw_offset,
            "sha256:" + compressed_digest.hexdigest(),
            compressed_length,
        )

    def _compressed_index_payload(
        self,
        *,
        batch_id: str,
        raw_pack_sha256: str,
        raw_pack_length: int,
        compressed_pack_sha256: str,
        compressed_pack_length: int,
        entries: Sequence[tuple[ObjectRef, int, int]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schemaVersion": COMPRESSED_PACKED_OBJECT_INDEX_SCHEMA,
            "batchId": batch_id,
            "packFile": self._pack_path(batch_id).name,
            "compressionCodec": COMPRESSED_PACK_CODEC,
            "rawPackSha256": raw_pack_sha256,
            "rawPackByteLength": raw_pack_length,
            "compressedPackSha256": compressed_pack_sha256,
            "compressedPackByteLength": compressed_pack_length,
            "entries": [
                {"ref": ref.as_dict(), "offset": offset, "length": length}
                for ref, offset, length in entries
            ],
        }
        payload["indexSha256"] = sha256_bytes(canonical_json_bytes(payload))
        return payload


# Alternative spelling retained solely for future backend injection sites.
TemporalQDCompressedPackedObjectStore = CompressedPackedTemporalQDObjectStore


def _duplicate_write(path: Path, data: bytes) -> int:
    """Durable old-style full artifact write used only by the bounded benchmark."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        TemporalQDObjectStore._write_bytes_and_fsync(descriptor, data)
    except Exception:
        raise
    fsync_directory(path.parent)
    return len(data)


def _synthetic_canonical_json(target_bytes: int) -> bytes:
    if target_bytes < 1024 * 1024:
        raise ValueError("benchmark payloads must be at least one MiB")
    overhead = len(canonical_json_bytes({"payload": ""}))
    return canonical_json_bytes({"payload": "x" * (target_bytes - overhead)})


def _measure(callable_: Any) -> tuple[Any, float, int]:
    """Measure one bounded action without retaining a prior allocator trace."""

    import tracemalloc

    tracemalloc.start()
    started = time.perf_counter()
    result = callable_()
    elapsed = time.perf_counter() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return result, elapsed, peak


def benchmark_duplicate_writes_vs_cas(
    output_root: Path | str,
    *,
    repetitions: int = 3,
    small_payload_bytes: int = 2 * 1024 * 1024,
    medium_payload_bytes: int = 6 * 1024 * 1024,
) -> dict[str, Any]:
    """Boundedly compare duplicated writes with prepared-object CAS reuse.

    The benchmark records logical payload bytes actually emitted by each path,
    wall time, and Python traced peak memory.  It never deletes an existing
    directory: callers must provide a dedicated fresh output root.
    """

    if isinstance(repetitions, bool) or int(repetitions) < 2:
        raise ValueError("benchmark repetitions must be at least two")
    root = Path(output_root)
    if root.exists() and any(root.iterdir()):
        raise TemporalQDObjectStoreError("benchmark output root must be empty")
    root.mkdir(parents=True, exist_ok=True)
    duplicate_root = root / "duplicated"
    cas_root = root / "cas"
    store = TemporalQDObjectStore(cas_root)
    namespace = ObjectNamespace("benchmark_payload", 1, CANONICAL_JSON_CODEC)
    cases: dict[str, Any] = {}
    for name, requested_size in (
        ("small", int(small_payload_bytes)),
        ("medium", int(medium_payload_bytes)),
    ):
        payload = _synthetic_canonical_json(requested_size)
        prepared = prepare_canonical_json_bytes(namespace, payload)

        def write_duplicates() -> int:
            return sum(
                _duplicate_write(
                    duplicate_root / name / f"{ordinal:04d}.json", payload
                )
                for ordinal in range(int(repetitions))
            )

        duplicate_bytes, duplicate_wall, duplicate_peak = _measure(write_duplicates)
        cas_result, cas_wall, cas_peak = _measure(
            lambda: store.put_many(prepared for _ in range(int(repetitions)))
        )
        if not isinstance(cas_result, BatchPutResult):  # pragma: no cover
            raise AssertionError("CAS benchmark unexpectedly returned no batch receipt")
        cases[name] = {
            "payloadBytes": len(payload),
            "repetitions": int(repetitions),
            "duplicated": {
                "wallSeconds": duplicate_wall,
                "bytesWritten": duplicate_bytes,
                "peakTracedBytes": duplicate_peak,
            },
            "casReuse": {
                "wallSeconds": cas_wall,
                "bytesWritten": cas_result.bytes_written,
                "peakTracedBytes": cas_peak,
                "createdCount": cas_result.created_count,
                "reusedCount": cas_result.reused_count,
            },
            # A 2 MiB allowance covers platform/file-buffer accounting while
            # still catching a regression proportional to duplicate documents.
            "memoryWithinBound": cas_peak <= duplicate_peak + 2 * 1024 * 1024,
        }
    return {
        "schemaVersion": "temporal_qd_object_store_reuse_benchmark_v1",
        "cases": cases,
    }


def _measure_detailed(callable_: Any) -> tuple[Any, dict[str, int | float | None]]:
    """Bounded wall/CPU/heap/RSS observation for isolated storage benchmarks."""

    import tracemalloc

    try:
        import psutil

        process = psutil.Process()
        rss_before: int | None = int(process.memory_info().rss)
    except Exception:  # pragma: no cover - psutil is an installed project dependency.
        process = None
        rss_before = None
    tracemalloc.start()
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    result = callable_()
    cpu_seconds = time.process_time() - cpu_started
    wall_seconds = time.perf_counter() - wall_started
    _current, traced_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = int(process.memory_info().rss) if process is not None else None
    return result, {
        "wallSeconds": wall_seconds,
        "cpuSeconds": cpu_seconds,
        "peakTracedBytes": traced_peak,
        "rssBeforeBytes": rss_before,
        "rssAfterBytes": rss_after,
        "rssDeltaBytes": (
            rss_after - rss_before
            if rss_before is not None and rss_after is not None
            else None
        ),
    }


def _artifact_metrics(root: Path) -> dict[str, int]:
    """Count regular final files and conservative filesystem allocation estimates."""

    file_count = 0
    logical_bytes = 0
    allocated_bytes = 0
    for directory, _subdirs, names in os.walk(root, followlinks=False):
        for name in names:
            path = Path(directory) / name
            status = path.lstat()
            if _is_link_or_reparse(status) or not stat.S_ISREG(status.st_mode):
                continue
            size = int(status.st_size)
            file_count += 1
            logical_bytes += size
            blocks = int(getattr(status, "st_blocks", 0) or 0)
            allocated_bytes += blocks * 512 if blocks else ((size + 4095) // 4096) * 4096
    return {
        "fileCount": file_count,
        "logicalArtifactBytes": logical_bytes,
        "allocatedArtifactBytes": allocated_bytes,
    }


def benchmark_loose_vs_packed(
    output_root: Path | str,
    *,
    object_count: int = 1000,
    small_payload_bytes: int = 512,
    coarse_payload_bytes: int = 3072,
) -> dict[str, Any]:
    """Compare fresh loose and packed stores for hundreds/thousands of objects.

    Objects are distinct canonical JSON documents so the measurement isolates
    file-layout amortization rather than duplicate-object CAS reuse.  ``bytesWritten``
    is logical payload emission; allocation metrics additionally reflect the
    filesystem's per-file granularity and are the relevant disk-pressure signal
    for many small artifacts.
    """

    if isinstance(object_count, bool) or int(object_count) < 100:
        raise ValueError("packed-store benchmark object_count must be at least 100")
    if (
        isinstance(small_payload_bytes, bool)
        or isinstance(coarse_payload_bytes, bool)
        or int(small_payload_bytes) < 32
        or int(coarse_payload_bytes) < int(small_payload_bytes)
    ):
        raise ValueError("packed-store benchmark payload sizes are invalid")
    root = Path(output_root)
    if root.exists() and any(root.iterdir()):
        raise TemporalQDObjectStoreError("packed benchmark output root must be empty")
    root.mkdir(parents=True, exist_ok=True)
    namespace = ObjectNamespace("packed_benchmark", 1, CANONICAL_JSON_CODEC)
    cases: dict[str, Any] = {}
    for label, payload_bytes in (
        ("small", int(small_payload_bytes)),
        ("coarse", int(coarse_payload_bytes)),
    ):
        prepared = tuple(
            prepare_json(
                namespace,
                {
                    "ordinal": ordinal,
                    "payload": "x" * payload_bytes,
                },
            )
            for ordinal in range(int(object_count))
        )
        loose = TemporalQDObjectStore(root / label / "loose")
        packed = PackedTemporalQDObjectStore(root / label / "packed")
        loose_result, loose_write = _measure_detailed(lambda: loose.put_many(prepared))
        packed_result, packed_write = _measure_detailed(lambda: packed.put_many(prepared))
        if not isinstance(loose_result, BatchPutResult) or not isinstance(
            packed_result, BatchPutResult
        ):
            raise AssertionError("object-store benchmark did not receive batch receipts")
        refs = tuple(item.ref for item in prepared)
        _loose_values, loose_read = _measure_detailed(lambda: tuple(loose.get_many(refs)))
        _packed_values, packed_read = _measure_detailed(
            lambda: tuple(packed.get_many(refs))
        )
        loose_artifacts = _artifact_metrics(loose.root)
        packed_artifacts = _artifact_metrics(packed.root)
        cases[label] = {
            "objectCount": int(object_count),
            "payloadBytes": sum(item.ref.byte_length for item in prepared),
            "loose": {
                "write": loose_write,
                "read": loose_read,
                "bytesWritten": loose_result.bytes_written,
                "createdCount": loose_result.created_count,
                **loose_artifacts,
                "readFileOpens": len(refs),
            },
            "packed": {
                "write": packed_write,
                "read": packed_read,
                "bytesWritten": packed_result.bytes_written,
                "createdCount": packed_result.created_count,
                **packed_artifacts,
                "readFileOpens": packed.last_batch_read_metrics["packFileOpens"],
            },
            "memoryWithinBound": (
                int(packed_write["peakTracedBytes"] or 0)
                <= int(loose_write["peakTracedBytes"] or 0) + 2 * 1024 * 1024
            ),
            "allocatedBytesWithinBound": (
                packed_artifacts["allocatedArtifactBytes"]
                <= loose_artifacts["allocatedArtifactBytes"]
            ),
        }
    return {
        "schemaVersion": "temporal_qd_packed_object_store_benchmark_v1",
        "cases": cases,
    }


def benchmark_compressed_packed_vs_uncompressed(
    output_root: Path | str,
    *,
    repetitive_object_count: int = 100,
    repetitive_total_payload_bytes: int = 100 * 1024 * 1024,
    small_object_count: int = 1000,
    small_payload_bytes: int = 512,
) -> dict[str, Any]:
    """Measure one zlib block against loose files and ordinary packed bytes.

    The default repetitive case is deliberately close to a 100 MiB batch of
    distinct canonical JSON records sharing the same template body.  It shows
    the useful compression case without changing ObjectRef input semantics.
    Timings are observations, not a portability claim; callers should require
    both a material storage reduction and a whole-batch wall-time win before
    choosing this optional backend for a production representation.
    """

    for value, label, minimum in (
        (repetitive_object_count, "repetitive_object_count", 2),
        (repetitive_total_payload_bytes, "repetitive_total_payload_bytes", 1024),
        (small_object_count, "small_object_count", 100),
        (small_payload_bytes, "small_payload_bytes", 32),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{label} must be an integer >= {minimum}")
    root = Path(output_root)
    if root.exists() and any(root.iterdir()):
        raise TemporalQDObjectStoreError(
            "compressed packed benchmark output root must be empty"
        )
    root.mkdir(parents=True, exist_ok=True)
    namespace = ObjectNamespace("compressed_packed_benchmark", 1, CANONICAL_JSON_CODEC)

    def drain(store: TemporalQDObjectStore, refs: tuple[ObjectRef, ...]) -> int:
        total = 0
        for _ref, data in store.get_many(refs):
            total += len(data)
        return total

    def pack_bytes(directory: Path, suffix: str) -> int:
        return sum(path.stat().st_size for path in directory.glob(f"*{suffix}"))

    def run_case(
        label: str,
        prepared: tuple[PreparedObject, ...],
    ) -> dict[str, Any]:
        direct = TemporalQDObjectStore(root / label / "direct")
        packed = PackedTemporalQDObjectStore(root / label / "packed")
        compressed = CompressedPackedTemporalQDObjectStore(root / label / "compressed")
        direct_result, direct_write = _measure_detailed(lambda: direct.put_many(prepared))
        packed_result, packed_write = _measure_detailed(lambda: packed.put_many(prepared))
        compressed_result, compressed_write = _measure_detailed(
            lambda: compressed.put_many(prepared)
        )
        if not all(
            isinstance(result, BatchPutResult)
            for result in (direct_result, packed_result, compressed_result)
        ):
            raise AssertionError("compressed benchmark did not receive batch receipts")
        refs = tuple(item.ref for item in prepared)
        direct_total, direct_read = _measure_detailed(lambda: drain(direct, refs))
        packed_total, packed_read = _measure_detailed(lambda: drain(packed, refs))
        compressed_total, compressed_read = _measure_detailed(
            lambda: drain(compressed, refs)
        )
        payload_bytes = sum(item.ref.byte_length for item in prepared)
        if (direct_total, packed_total, compressed_total) != (
            payload_bytes,
            payload_bytes,
            payload_bytes,
        ):
            raise AssertionError("compressed benchmark read did not hydrate exact payloads")
        direct_artifacts = _artifact_metrics(direct.root)
        packed_artifacts = _artifact_metrics(packed.root)
        compressed_artifacts = _artifact_metrics(compressed.root)
        whole_batch = {
            "direct": direct_write["wallSeconds"] + direct_read["wallSeconds"],
            "packed": packed_write["wallSeconds"] + packed_read["wallSeconds"],
            "compressed": compressed_write["wallSeconds"]
            + compressed_read["wallSeconds"],
        }
        compressed_pack_bytes = pack_bytes(compressed.root / "compressed-packs", ".zpack")
        packed_pack_bytes = pack_bytes(packed.root / "packs", ".pack")
        storage_materially_reduced = compressed_pack_bytes <= packed_pack_bytes * 0.80
        wall_materially_won = whole_batch["compressed"] <= whole_batch["packed"] * 0.90
        return {
            "objectCount": len(prepared),
            "payloadBytes": payload_bytes,
            "direct": {
                "bytesWritten": direct_result.bytes_written,
                "readFileOpens": len(refs),
                "write": direct_write,
                "read": direct_read,
                **direct_artifacts,
            },
            "packed": {
                "bytesWritten": packed_result.bytes_written,
                "packBytes": packed_pack_bytes,
                "readFileOpens": packed.last_batch_read_metrics["packFileOpens"],
                "write": packed_write,
                "read": packed_read,
                **packed_artifacts,
            },
            "compressed": {
                "bytesWritten": compressed_result.bytes_written,
                "packBytes": compressed_pack_bytes,
                "readFileOpens": compressed.last_batch_read_metrics["packFileOpens"],
                "write": compressed_write,
                "read": compressed_read,
                **compressed_artifacts,
            },
            "wholeBatchWallSeconds": whole_batch,
            "marginalCompressionWallSecondsVsPacked": (
                whole_batch["compressed"] - whole_batch["packed"]
            ),
            "compressedPackBytesSavedVsPacked": packed_pack_bytes - compressed_pack_bytes,
            "compressedPackStorageReductionRatioVsPacked": (
                1.0 - compressed_pack_bytes / packed_pack_bytes
                if packed_pack_bytes
                else None
            ),
            "storageMateriallyReduced": storage_materially_reduced,
            "wholeBatchWallMateriallyWonVsPacked": wall_materially_won,
            "materiallyWinsStorageAndWall": storage_materially_reduced and wall_materially_won,
        }

    repetitive_payload_chars = max(
        32,
        repetitive_total_payload_bytes // repetitive_object_count - 96,
    )
    repetitive = tuple(
        prepare_json(
            namespace,
            {
                "ordinal": ordinal,
                "template": "repetitive-canonical-blob-v1",
                "payload": "x" * repetitive_payload_chars,
            },
        )
        for ordinal in range(repetitive_object_count)
    )
    small = tuple(
        prepare_json(
            namespace,
            {
                "ordinal": ordinal,
                "template": "small-repetitive-canonical-blob-v1",
                "payload": "x" * small_payload_bytes,
            },
        )
        for ordinal in range(small_object_count)
    )
    cases = {
        "repetitive100MiB": run_case("repetitive100MiB", repetitive),
        "small1000": run_case("small1000", small),
    }
    return {
        "schemaVersion": "temporal_qd_compressed_packed_benchmark_v1",
        "compressionCodec": COMPRESSED_PACK_CODEC,
        "cases": cases,
        # Raw storage copying is intentionally not a proxy for the existing
        # rich-object render/parse/canonicalization path.  The front's
        # authentic end-to-end gate decides representation admission.
        "recommendation": (
            "requires-authentic-end-to-end-gate"
            if all(case["storageMateriallyReduced"] for case in cases.values())
            else "bail-no-material-storage-win"
        ),
    }


__all__ = [
    "BatchPutResult",
    "CANONICAL_JSON_CODEC",
    "COMPRESSED_PACK_CODEC",
    "COMPRESSED_PACKED_OBJECT_INDEX_SCHEMA",
    "COMPRESSED_PACKED_OBJECT_PACK_SCHEMA",
    "COMPRESSED_PACKED_OBJECT_STORE_SCHEMA",
    "CompressedPackedObjectLocation",
    "CompressedPackedTemporalQDObjectStore",
    "CompressedPackedVerifiedObjectReader",
    "DEFAULT_CHUNK_BYTES",
    "OBJECT_MANIFEST_SCHEMA",
    "OBJECT_STORE_SCHEMA",
    "MAX_COMPRESSED_PACK_RAW_BYTES",
    "PACKED_OBJECT_INDEX_SCHEMA",
    "PACKED_OBJECT_PACK_SCHEMA",
    "PACKED_OBJECT_STORE_SCHEMA",
    "ObjectNamespace",
    "ObjectRef",
    "ObjectStoreConflictError",
    "ObjectStoreIntegrityError",
    "ObjectStoreNotFoundError",
    "ObjectStorePathError",
    "PreparedObject",
    "PackedObjectLocation",
    "PackedTemporalQDObjectStore",
    "PackedVerifiedObjectReader",
    "RAW_BYTES_CODEC",
    "TemporalQDObjectStore",
    "TemporalQDCompressedPackedObjectStore",
    "TemporalQDPackedObjectStore",
    "TemporalQDObjectStoreError",
    "VerifiedObjectReader",
    "benchmark_duplicate_writes_vs_cas",
    "benchmark_compressed_packed_vs_uncompressed",
    "benchmark_loose_vs_packed",
    "build_manifest",
    "canonical_json_bytes",
    "prepare_bytes",
    "prepare_canonical_json_bytes",
    "prepare_json",
    "sha256_bytes",
    "validate_manifest",
]
