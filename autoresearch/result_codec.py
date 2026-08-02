"""Deterministic, verified JSON result blobs.

Temporal search result payloads have a semantic identity independent from the
on-disk representation.  New payloads use a single deterministic gzip blob;
the helpers here keep the legacy JSON reader available for historical runs.
"""

from __future__ import annotations

from collections.abc import Mapping
import errno
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from typing import Any


GZIP_JSON_CODEC = "gzip-json-v1"
LEGACY_JSON_CODEC = "json-v0"


class ResultCodecError(RuntimeError):
    pass


def _unsupported_windows_directory_sync_error(error: int) -> bool:
    """Return whether Windows cannot flush this directory handle.

    Windows filesystems do not uniformly implement directory flushes.  These
    values mean that the directory flush is unavailable, not that a file
    payload failed to reach stable storage.  A real failure to write or fsync
    the payload itself is never handled here and remains fatal.
    """
    return error in {
        1,  # ERROR_INVALID_FUNCTION
        5,  # ERROR_ACCESS_DENIED (directory FlushFileBuffers unsupported)
        50,  # ERROR_NOT_SUPPORTED
        87,  # ERROR_INVALID_PARAMETER
    }


def _fsync_directory_windows(directory: Path) -> bool:
    """Best-effort native directory flush for Windows, safely returning false.

    ``os.open`` cannot reliably open a directory on Windows.  Open a directory
    handle with ``FILE_FLAG_BACKUP_SEMANTICS`` instead, then request
    ``FlushFileBuffers``.  NTFS/ReFS installations which support that flush
    get durable name publication.  Other Windows filesystems reject it with a
    documented unsupported error and use the safe no-directory-fsync fallback.
    """
    try:
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    except (AttributeError, OSError):
        # This also makes an accidental platform override in a test harmless.
        return False

    invalid_handle = ctypes.c_void_p(-1).value
    create_file = kernel32.CreateFileW
    flush_buffers = kernel32.FlushFileBuffers
    close_handle = kernel32.CloseHandle
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    flush_buffers.argtypes = [ctypes.c_void_p]
    flush_buffers.restype = ctypes.c_int
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    generic_read = 0x80000000
    generic_write = 0x40000000
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    file_share_delete = 0x00000004
    open_existing = 3
    file_flag_backup_semantics = 0x02000000
    handle = create_file(
        str(directory),
        generic_read | generic_write,
        file_share_read | file_share_write | file_share_delete,
        None,
        open_existing,
        file_flag_backup_semantics,
        None,
    )
    if handle == invalid_handle:
        error = ctypes.get_last_error()
        if _unsupported_windows_directory_sync_error(error):
            return False
        raise OSError(error, f"could not open directory for durable sync: {directory}")
    try:
        if flush_buffers(handle):
            return True
        error = ctypes.get_last_error()
        if _unsupported_windows_directory_sync_error(error):
            return False
        raise OSError(error, f"could not fsync directory: {directory}")
    finally:
        close_handle(handle)


def fsync_directory(directory: Path | str) -> bool:
    """Synchronize a containing directory when the host filesystem supports it.

    This is deliberately separate from file fsync.  A payload fsync is
    required and failures propagate; a directory fsync persists a name change
    but has no portable Windows/Python guarantee.  The boolean reports whether
    the platform performed the directory sync.
    """
    target = Path(directory)
    if os.name == "nt":
        return _fsync_directory_windows(target)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(os.fspath(target), flags)
    except OSError as exc:
        unsupported = {
            errno.EINVAL,
            getattr(errno, "ENOTSUP", errno.EINVAL),
            getattr(errno, "EOPNOTSUPP", errno.EINVAL),
        }
        if exc.errno in unsupported:
            return False
        raise
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


def canonical_json_bytes(value: Any) -> bytes:
    """Return canonical JSON bytes used for semantic identity."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ResultCodecError("value must be finite canonical JSON") from exc


def pretty_json_bytes(value: Any) -> bytes:
    """Return the deterministic human-readable JSON representation."""
    try:
        return (
            json.dumps(
                value,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ResultCodecError("value must be finite JSON") from exc


def sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def semantic_sha256(value: Any) -> str:
    return sha256(canonical_json_bytes(value))


def _gzip_bytes(uncompressed: bytes) -> bytes:
    # GzipFile rather than gzip.compress keeps the OS header byte stable too.
    output = io.BytesIO()
    with gzip.GzipFile(
        fileobj=output,
        mode="wb",
        filename="",
        compresslevel=9,
        mtime=0,
    ) as handle:
        handle.write(uncompressed)
    return output.getvalue()


def gzip_json_bytes(value: Any) -> tuple[bytes, dict[str, Any]]:
    """Encode a deterministic pretty JSON document and its audit metadata."""
    semantic = canonical_json_bytes(value)
    uncompressed = pretty_json_bytes(value)
    blob = _gzip_bytes(uncompressed)
    return blob, {
        "codec": GZIP_JSON_CODEC,
        "semanticSha256": sha256(semantic),
        "semanticSizeBytes": len(semantic),
        "uncompressedSha256": sha256(uncompressed),
        "uncompressedSizeBytes": len(uncompressed),
        "blobSha256": sha256(blob),
        "blobSizeBytes": len(blob),
    }


def _json_value(uncompressed: bytes, *, path: Path) -> Any:
    try:
        return json.loads(uncompressed.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResultCodecError(f"invalid JSON result blob: {path}") from exc


def _metadata_for_legacy(raw: bytes, value: Any) -> dict[str, Any]:
    semantic = canonical_json_bytes(value)
    return {
        "codec": LEGACY_JSON_CODEC,
        "semanticSha256": sha256(semantic),
        "semanticSizeBytes": len(semantic),
        "uncompressedSha256": sha256(raw),
        "uncompressedSizeBytes": len(raw),
        "blobSha256": sha256(raw),
        "blobSizeBytes": len(raw),
    }


def _matches_expected(
    actual: Mapping[str, Any], expected: Mapping[str, Any], *, path: Path
) -> None:
    for key in (
        "codec",
        "semanticSha256",
        "semanticSizeBytes",
        "uncompressedSha256",
        "uncompressedSizeBytes",
        "blobSha256",
        "blobSizeBytes",
    ):
        if key in expected and expected[key] != actual[key]:
            raise ResultCodecError(f"result metadata mismatch for {key}: {path}")


def read_json(
    path: Path | str,
    *,
    expected: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Read legacy ``.json`` or verified deterministic ``.json.gz`` data.

    A gzip blob must reproduce exactly from its decoded semantic JSON.  This
    rejects corrupt, truncated, trailing, or non-deterministic representations
    even when an expected hash was not supplied by an older checkpoint.
    """
    target = Path(path)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise ResultCodecError(f"unable to read result blob: {target}") from exc
    if target.name.endswith(".json.gz"):
        try:
            uncompressed = gzip.decompress(raw)
        except (OSError, EOFError) as exc:
            raise ResultCodecError(f"unable to decompress result blob: {target}") from exc
        if _gzip_bytes(uncompressed) != raw:
            raise ResultCodecError(
                f"result blob is not canonical deterministic gzip: {target}"
            )
        value = _json_value(uncompressed, path=target)
        _, metadata = gzip_json_bytes(value)
    elif target.suffix == ".json":
        value = _json_value(raw, path=target)
        metadata = _metadata_for_legacy(raw, value)
    else:
        raise ResultCodecError(f"unsupported result blob extension: {target}")
    if expected is not None:
        _matches_expected(metadata, expected, path=target)
    return value, metadata


def read_json_object(
    path: Path | str,
    *,
    expected: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    value, metadata = read_json(path, expected=expected)
    if not isinstance(value, Mapping):
        raise ResultCodecError(f"result JSON root must be an object: {path}")
    return dict(value), metadata


def _verify_existing_or_raise(path: Path, blob: bytes) -> None:
    try:
        existing = path.read_bytes()
    except FileNotFoundError:
        return
    if existing != blob:
        raise ResultCodecError(f"refusing to overwrite divergent immutable blob: {path}")


def write_gzip_json_once(path: Path | str, value: Any) -> dict[str, Any]:
    """Durably materialize a gzip JSON blob exactly once and verify it.

    ``os.link`` publishes the fully fsynced temporary file without replacing an
    existing destination.  A concurrent writer may only win if it wrote the
    identical deterministic bytes.
    """
    target = Path(path)
    if not target.name.endswith(".json.gz"):
        raise ResultCodecError("compressed result paths must end with .json.gz")
    blob, metadata = gzip_json_bytes(value)
    target.parent.mkdir(parents=True, exist_ok=True)
    _verify_existing_or_raise(target, blob)
    if not target.exists():
        fd, temporary_name = tempfile.mkstemp(
            prefix=target.name + ".", suffix=".tmp", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                _verify_existing_or_raise(target, blob)
            else:
                # The payload is already fsynced above.  Flush the directory
                # entry before removing its temporary hard-link so a crash
                # cannot lose the newly published immutable name on hosts
                # which support directory fsync.
                fsync_directory(target.parent)
            finally:
                temporary.unlink(missing_ok=True)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    # Decode and compare every independently auditable representation before a
    # caller is allowed to checkpoint/acknowledge a gateway completion.
    _, verified = read_json(target, expected=metadata)
    return verified


__all__ = [
    "GZIP_JSON_CODEC",
    "LEGACY_JSON_CODEC",
    "ResultCodecError",
    "canonical_json_bytes",
    "fsync_directory",
    "gzip_json_bytes",
    "pretty_json_bytes",
    "read_json",
    "read_json_object",
    "semantic_sha256",
    "sha256",
    "write_gzip_json_once",
]
