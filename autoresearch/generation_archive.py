"""Fail-closed, restart-safe archive cutovers for an autoresearch ``runs`` root.

The only run-data mutation here is a same-volume whole-root rename.  The local
lock serialises cutovers, but is not a supervisor writer fence: apply requires
an external, fresh quiescence attestation as well.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat as stat_module
import tempfile
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator


ARCHIVE_MANIFEST_NAME = "archive-manifest.json"
GENERATION_MANIFEST_NAME = "generation-manifest.json"
DERIVED_DIRECTORY_NAME = "derived"
INVENTORY_SCHEMA_NAME = "autoresearch.generation.inventory"
INVENTORY_SCHEMA_VERSION = 1
INTENT_SCHEMA_NAME = "autoresearch.generation.cutover-intent"
INTENT_SCHEMA_VERSION = 1
ARCHIVE_SCHEMA_NAME = "autoresearch.generation.archive-manifest"
ARCHIVE_SCHEMA_VERSION = 3
GENERATION_SCHEMA_NAME = "autoresearch.generation.manifest"
GENERATION_SCHEMA_VERSION = 1
QUIESCENCE_SCHEMA_NAME = "autoresearch.generation.quiescence"
QUIESCENCE_SCHEMA_VERSION = 1
QUIESCENCE_CONSUMPTION_SCHEMA_NAME = "autoresearch.generation.quiescence-consumption"
QUIESCENCE_CONSUMPTION_SCHEMA_VERSION = 1
QUIESCENCE_MAX_AGE = timedelta(minutes=5)
_INTENT_PREFIX = ".generation-cutover-"
_INTENT_SUFFIX = ".intent.json"
_CONSUMED_NONCE_PREFIX = ".generation-quiescence-"
_CONSUMED_NONCE_SUFFIX = ".consumed.json"
_LOCK_NAME = ".generation-cutover.lock"
# The legacy production corpus universally contains only this catalog.  Legacy
# controls and report artifacts are additive caller-supplied checks.
_DEFAULT_CRITICAL = (
    "derived/attempt-catalog.sqlite",
)
_FORMAL_LEVEL_C_CONTROL_DIR = "derived/level-c/control"
_FORMAL_LEVEL_C_CRITICAL = (
    GENERATION_MANIFEST_NAME,
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/archive-linkage.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/bootstrap-result.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/protocol.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/protocol-authority.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/execution-plan-A.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/execution-plan-B.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/execution-plan-C.json",
    f"{_FORMAL_LEVEL_C_CONTROL_DIR}/execution-plan-D.json",
)
DEFAULT_CRITICAL_ARTIFACT_ALLOWLIST = _DEFAULT_CRITICAL
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NONCE = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_RESERVED_NAMES = {
    "con", "prn", "aux", "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}


class GenerationArchiveError(RuntimeError):
    """The requested cutover is not provably safe to apply or resume."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_safe_id(value: str, *, field_name: str = "id") -> str:
    if (
        not isinstance(value, str)
        or not _SAFE_ID.fullmatch(value)
        or value in {".", ".."}
        or value.endswith(".")
        or value.casefold() in _WINDOWS_RESERVED_NAMES
    ):
        raise ValueError(
            f"{field_name} must be 1-128 ASCII letters, digits, '.', '_' or '-', "
            "begin with a letter or digit, and not be '.' or '..'"
        )
    return value


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        return (json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("manifest data must be JSON serializable") from exc


def _best_effort_flush_directory(path: Path) -> None:
    """Ask the platform to persist a directory entry; failure is non-fatal.

    Windows has no portable directory fsync through ``os``.  Its directory
    handle/FlushFileBuffers path is attempted with FILE_FLAG_BACKUP_SEMANTICS;
    network filesystems and some Windows versions may reject it.
    """
    try:
        if os.name == "nt":
            import ctypes

            kernel32 = ctypes.windll.kernel32
            handle = kernel32.CreateFileW(
                str(path), 0x80000000, 0x00000001 | 0x00000002 | 0x00000004,
                None, 3, 0x02000000, None,
            )
            if handle not in (0, -1):
                try:
                    kernel32.FlushFileBuffers(handle)
                finally:
                    kernel32.CloseHandle(handle)
            return
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        pass


def _durable_mkdir(path: Path, *, parents: bool = False) -> None:
    path.mkdir(parents=parents, exist_ok=False)
    _best_effort_flush_directory(path.parent)


def _durable_replace(source: Path, destination: Path) -> None:
    os.replace(source, destination)
    _best_effort_flush_directory(destination.parent)
    if source.parent != destination.parent:
        _best_effort_flush_directory(source.parent)


def _atomic_json_write(path: Path, payload: Mapping[str, Any]) -> None:
    """Write and replace metadata, with best-effort post-replace durability."""
    if not path.parent.exists():
        _durable_mkdir(path.parent, parents=True)
    data = _canonical_json_bytes(payload)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    # A stale temp is interruption evidence; run data is never deleted.
    _durable_replace(temporary, path)


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> str:
    data = _canonical_json_bytes(payload)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = path.read_bytes()
        if existing != data:
            raise GenerationArchiveError(f"immutable file already differs: {path}")
        return hashlib.sha256(existing).hexdigest()
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    _best_effort_flush_directory(path.parent)
    return hashlib.sha256(data).hexdigest()


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GenerationArchiveError(f"invalid or truncated {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise GenerationArchiveError(f"{label} must contain an object: {path}")
    return payload


def _require_schema(payload: Mapping[str, Any], *, name: str, version: int, label: str) -> None:
    if payload.get("schema_name") != name or payload.get("schema_version") != version:
        raise GenerationArchiveError(f"unsupported {label} schema")


def _resolve(path: Path | str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _is_within(candidate: Path, parent: Path) -> bool:
    try:
        candidate.relative_to(parent)
    except ValueError:
        return False
    return True


def _snapshot(stat_result: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        stat_result.st_dev, stat_result.st_ino, stat_result.st_mode,
        stat_result.st_size, stat_result.st_mtime_ns, stat_result.st_ctime_ns,
    )


def _inventory_read_hook(path: Path, bytes_read: int) -> None:
    """Test seam invoked while a bounded inventory stream is being read."""


def _stream_regular_file(path: Path, *, invoke_hook: bool) -> tuple[os.stat_result, str]:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise GenerationArchiveError(f"could not read regular file during inventory: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat_module.S_ISREG(before.st_mode):
            raise GenerationArchiveError(f"unsupported filesystem entry in runs root: {path}")
        digest = hashlib.sha256()
        bytes_read = 0
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            digest.update(block)
            bytes_read += len(block)
            if invoke_hook:
                _inventory_read_hook(path, bytes_read)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _snapshot(before) != _snapshot(after):
        raise GenerationArchiveError(f"regular file changed while inventorying: {path}")
    return after, digest.hexdigest()


def _hash_regular_file(path: Path) -> tuple[os.stat_result, str]:
    """Hash twice by descriptor so a concurrent same-size substitution fails closed."""
    first_stat, first_digest = _stream_regular_file(path, invoke_hook=True)
    second_stat, second_digest = _stream_regular_file(path, invoke_hook=False)
    if _snapshot(first_stat) != _snapshot(second_stat) or first_digest != second_digest:
        raise GenerationArchiveError(f"regular file changed while inventorying: {path}")
    return second_stat, second_digest


def _require_relative_artifact(value: Path | str, root: Path) -> tuple[str, Path]:
    raw = Path(value)
    if raw.is_absolute() or any(part == ".." for part in raw.parts):
        raise ValueError(f"critical artifact must be relative to runs root: {value!s}")
    candidate = _resolve(root / raw)
    if not _is_within(candidate, root):
        raise ValueError(f"critical artifact escapes runs root: {value!s}")
    return candidate.relative_to(root).as_posix(), candidate


def _is_formal_level_c_generation(root: Path) -> bool:
    control = root / _FORMAL_LEVEL_C_CONTROL_DIR
    if not control.exists():
        return False
    if not control.is_dir() or control.is_symlink():
        raise GenerationArchiveError("formal Level C control path must be a real directory")
    manifest_path = root / GENERATION_MANIFEST_NAME
    if not manifest_path.exists():
        raise GenerationArchiveError("formal Level C control exists without generation-manifest.json")
    manifest = _read_json(manifest_path, label="generation manifest")
    generation_id = str(manifest.get("new_generation_id") or "")
    if not generation_id.startswith("level-c"):
        raise GenerationArchiveError("formal Level C control exists without a level-c generation manifest")
    return True


def _default_critical_artifacts(root: Path) -> tuple[tuple[str, ...], str]:
    """Return mandatory evidence anchors for the detected generation shape."""

    if _is_formal_level_c_generation(root):
        return _FORMAL_LEVEL_C_CRITICAL, "formal_level_c_defaults"
    return _DEFAULT_CRITICAL, "mandatory_defaults"


def _normalize_critical_artifacts(
    root: Path, critical_artifacts: Mapping[str, str] | Iterable[str | Path] | None,
) -> tuple[dict[str, str | None], str]:
    defaults, default_source = _default_critical_artifacts(root)
    result = {item: None for item in defaults}
    additions = 0
    source = default_source
    if critical_artifacts is None:
        return result, source
    if isinstance(critical_artifacts, Mapping):
        source = f"{default_source}_plus_caller_hashes"
        for item, expected in critical_artifacts.items():
            relative, _ = _require_relative_artifact(item, root)
            if not isinstance(expected, str) or not _SHA256.fullmatch(expected.lower()):
                raise ValueError(f"critical artifact hash must be a SHA-256 hex digest: {item}")
            if relative not in result:
                result[relative] = expected.lower()
                additions += 1
    else:
        source = f"{default_source}_plus_caller_allowlist"
        for item in critical_artifacts:
            relative, _ = _require_relative_artifact(item, root)
            if relative not in result:
                result[relative] = None
                additions += 1
    return result, source if additions else default_source


def _scan_tree(root: Path, requested: Mapping[str, str | None]) -> tuple[dict[str, int | str], dict[str, dict[str, Any]]]:
    """Walk one directory at a time and stream-hash every regular file twice."""
    digest = hashlib.sha256()
    file_count = file_bytes = derived_file_count = derived_bytes = 0
    active_run_count = 0
    artifacts: dict[str, dict[str, Any]] = {}
    for current, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        directory_names.sort()
        file_names.sort()
        current_path = Path(current)
        if current_path == root:
            active_run_count = sum(1 for name in directory_names if name != DERIVED_DIRECTORY_NAME)
        for name in directory_names:
            path = current_path / name
            if path.is_symlink():
                raise GenerationArchiveError(f"symlinks are not supported in runs roots: {path}")
            details = path.stat()
            if not path.is_dir():
                raise GenerationArchiveError(f"unsupported filesystem entry in runs root: {path}")
            digest.update(_canonical_json_bytes({
                "path": path.relative_to(root).as_posix(), "kind": "d", "bytes": 0,
                "mtime_ns": details.st_mtime_ns, "mode": details.st_mode & 0o777,
            }))
        for name in file_names:
            path = current_path / name
            if path.is_symlink():
                raise GenerationArchiveError(f"symlinks are not supported in runs roots: {path}")
            details, content_sha256 = _hash_regular_file(path)
            relative = path.relative_to(root).as_posix()
            file_count += 1
            file_bytes += details.st_size
            if relative.split("/", 1)[0] == DERIVED_DIRECTORY_NAME:
                derived_file_count += 1
                derived_bytes += details.st_size
            digest.update(_canonical_json_bytes({
                "path": relative, "kind": "f", "bytes": details.st_size,
                "mtime_ns": details.st_mtime_ns, "mode": details.st_mode & 0o777,
                "content_sha256": content_sha256,
            }))
            if relative in requested:
                artifacts[relative] = {"bytes": details.st_size, "sha256": content_sha256}
    return {
        "top_level_active_run_count": active_run_count,
        "recursive_file_count": file_count,
        "recursive_file_bytes": file_bytes,
        "derived_file_count": derived_file_count,
        "derived_file_bytes": derived_bytes,
        # Retained for callers; it now commits content as well as metadata.
        "metadata_tree_sha256": digest.hexdigest(),
        "content_tree_sha256": digest.hexdigest(),
    }, artifacts


def build_inventory(
    runs_root: Path | str,
    *,
    critical_artifacts: Mapping[str, str] | Iterable[str | Path] | None = None,
) -> dict[str, Any]:
    root = _resolve(runs_root)
    if not root.is_dir() or root.is_symlink():
        raise GenerationArchiveError(f"active runs root must be a real directory: {root}")
    requested, source = _normalize_critical_artifacts(root, critical_artifacts)
    tree, artifacts = _scan_tree(root, requested)
    for relative, expected in requested.items():
        details = artifacts.get(relative)
        if details is None:
            raise GenerationArchiveError(f"required critical artifact is missing: {relative}")
        if expected is not None and details["sha256"] != expected:
            raise GenerationArchiveError(f"critical artifact SHA-256 mismatch: {relative}")
    result: dict[str, Any] = {
        "schema_name": INVENTORY_SCHEMA_NAME,
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "runs_root": str(root), **tree,
        "critical_artifact_source": source,
        "critical_artifacts": artifacts,
    }
    identity_payload = dict(result)
    identity_payload.pop("runs_root", None)
    result["inventory_identity"] = hashlib.sha256(_canonical_json_bytes(identity_payload)).hexdigest()
    return result


def _load_inventory(inventory: Mapping[str, Any]) -> dict[str, Any]:
    copied = dict(inventory)
    _require_schema(copied, name=INVENTORY_SCHEMA_NAME, version=INVENTORY_SCHEMA_VERSION, label="inventory")
    identity = copied.get("inventory_identity")
    if not isinstance(identity, str) or not _SHA256.fullmatch(identity):
        raise GenerationArchiveError("inventory has no valid deterministic identity")
    identity_payload = dict(copied)
    identity_payload.pop("inventory_identity", None)
    identity_payload.pop("runs_root", None)
    if hashlib.sha256(_canonical_json_bytes(identity_payload)).hexdigest() != identity:
        raise GenerationArchiveError("inventory identity does not match its contents")
    return copied


def _inventory_identity(inventory: Mapping[str, Any]) -> str:
    return str(_load_inventory(inventory)["inventory_identity"])


def _checkpoint(name: str) -> None:
    """Crash-injection seam; tests may replace this function with an exception."""


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """Acquire an advisory process lock; external writer quiescence is separate."""
    if not path.parent.exists():
        _durable_mkdir(path.parent, parents=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if os.name == "nt":
            import msvcrt
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"0")
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise GenerationArchiveError("another archive cutover holds the exclusive lock") from exc
            unlock = lambda: msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise GenerationArchiveError("another archive cutover holds the exclusive lock") from exc
            unlock = lambda: fcntl.flock(fd, fcntl.LOCK_UN)
        try:
            yield
        finally:
            unlock()
    finally:
        os.close(fd)


class GenerationArchiveService:
    """Own a fixed active ``runs`` root and its sibling archive root."""

    def __init__(self, runs_root: Path | str, archive_root: Path | str | None = None) -> None:
        self.runs_root = _resolve(runs_root)
        self.archive_root = _resolve(archive_root or self.runs_root.parent / "runs_archive")
        self._validate_roots()

    def _validate_roots(self) -> None:
        if self.archive_root.parent != self.runs_root.parent:
            raise GenerationArchiveError("archive root must be a sibling of the active runs root")
        if self.archive_root == self.runs_root or _is_within(self.archive_root, self.runs_root):
            raise GenerationArchiveError("archive root cannot be nested under the active runs root")
        if self.archive_root.exists() and (not self.archive_root.is_dir() or self.archive_root.is_symlink()):
            raise GenerationArchiveError(f"archive root must be a real directory when present: {self.archive_root}")
        if self.runs_root.exists() and (not self.runs_root.is_dir() or self.runs_root.is_symlink()):
            raise GenerationArchiveError(f"active runs root must be a real directory: {self.runs_root}")

    def _paths(self, archive_id: str) -> tuple[Path, Path, Path]:
        archive_dir = self.archive_root / archive_id
        return archive_dir, archive_dir / "runs", archive_dir / ARCHIVE_MANIFEST_NAME

    def _intent_path(self, archive_id: str) -> Path:
        return self.archive_root / f"{_INTENT_PREFIX}{archive_id}{_INTENT_SUFFIX}"

    def _consumed_nonce_path(self, nonce: str) -> Path:
        return self.archive_root / f"{_CONSUMED_NONCE_PREFIX}{nonce}{_CONSUMED_NONCE_SUFFIX}"

    def _assert_same_volume(self) -> None:
        if not self.archive_root.exists():
            _durable_mkdir(self.archive_root, parents=True)
        source, destination = self.runs_root.stat(), self.archive_root.stat()
        if source.st_dev != destination.st_dev or self.runs_root.drive.casefold() != self.archive_root.drive.casefold():
            raise GenerationArchiveError("archive root is on a different volume; atomic rename is required")

    def _restore_instructions(self, archive_id: str) -> dict[str, Any]:
        _, archived, _ = self._paths(archive_id)
        return {
            "mode": "dry-run only", "archived_runs_root": str(archived),
            "required_precondition": "A restore destination must not already exist.",
        }

    def dry_run(
        self, archive_id: str, new_generation_id: str, *, provenance: Mapping[str, Any],
        critical_artifacts: Mapping[str, str] | Iterable[str | Path] | None = None,
    ) -> dict[str, Any]:
        archive_id = validate_safe_id(archive_id, field_name="archive_id")
        new_generation_id = validate_safe_id(new_generation_id, field_name="new_generation_id")
        if not isinstance(provenance, Mapping):
            raise ValueError("provenance must be a mapping")
        _canonical_json_bytes(dict(provenance))
        archive_dir, destination, manifest_path = self._paths(archive_id)
        manifest = self._read_manifest_if_present(manifest_path)
        if manifest is not None:
            self._validate_manifest(manifest, archive_id, new_generation_id, manifest_path)
            if manifest.get("provenance") != dict(provenance):
                raise GenerationArchiveError("provenance conflicts with the archive manifest")
            inventory = self._archived_inventory(destination, manifest) if manifest["state"] == "complete" else manifest["inventory"]
            _inventory_identity(inventory)
        elif self.runs_root.exists():
            inventory = build_inventory(self.runs_root, critical_artifacts=critical_artifacts)
        else:
            raise GenerationArchiveError("active runs root is missing without a valid prepared archive manifest")
        return {
            "dry_run": True, "archive_id": archive_id, "new_generation_id": new_generation_id,
            "source_runs_root": str(self.runs_root), "archive_root": str(self.archive_root),
            "destination_runs_root": str(destination), "archive_manifest_path": str(manifest_path),
            "archive_directory_exists": archive_dir.exists(), "inventory": inventory,
            "inventory_identity": _inventory_identity(inventory),
            "quiescence_required": {
                "contract": "external writers must be stopped and attest via the provenance cutover_quiescence marker",
                "marker_fields": ["marker_path", "marker_sha256", "issuer_id", "nonce", "issued_at"],
            },
            "writer_fence_limitations": (
                "This module has no full supervisor writer fence. Windows directory-handle "
                "share modes do not prevent child-file writes; a supervisor-owned writer "
                "lease/fence remains required in addition to the quiescence proof."
            ),
            "restore_instructions": self._restore_instructions(archive_id),
        }

    def _read_manifest_if_present(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        if not path.is_file() or path.is_symlink():
            raise GenerationArchiveError(f"archive manifest is not a regular file: {path}")
        return self._load_archive_manifest(_read_json(path, label="archive manifest"))

    def _load_archive_manifest(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        copied = dict(manifest)
        _require_schema(copied, name=ARCHIVE_SCHEMA_NAME, version=ARCHIVE_SCHEMA_VERSION, label="archive manifest")
        if copied.get("state") not in {"prepared", "complete"}:
            raise GenerationArchiveError("archive manifest has unsupported state")
        inventory = copied.get("inventory")
        if not isinstance(inventory, Mapping):
            raise GenerationArchiveError("archive manifest inventory is missing")
        _load_inventory(inventory)
        return copied

    def _validate_manifest(self, manifest: Mapping[str, Any], archive_id: str, generation_id: str, path: Path) -> None:
        manifest = self._load_archive_manifest(manifest)
        _, destination, _ = self._paths(archive_id)
        if (
            manifest.get("archive_id") != archive_id or manifest.get("new_generation_id") != generation_id
            or manifest.get("source_runs_root") != str(self.runs_root)
            or manifest.get("archive_root") != str(self.archive_root)
            or manifest.get("destination_runs_root") != str(destination)
            or not isinstance(manifest.get("provenance"), Mapping)
        ):
            raise GenerationArchiveError(f"archive manifest conflicts with requested cutover: {path}")

    def _prepared_manifest(self, archive_id: str, new_generation_id: str, inventory: Mapping[str, Any], provenance: Mapping[str, Any], quiescence: Mapping[str, Any]) -> dict[str, Any]:
        _, destination, _ = self._paths(archive_id)
        return {
            "schema_name": ARCHIVE_SCHEMA_NAME, "schema_version": ARCHIVE_SCHEMA_VERSION,
            "state": "prepared", "archive_id": archive_id, "new_generation_id": new_generation_id,
            "prepared_at": utc_now(), "generation_manifest_created_at": utc_now(), "completed_at": None,
            "source_runs_root": str(self.runs_root), "archive_root": str(self.archive_root),
            "destination_runs_root": str(destination), "inventory": dict(inventory),
            "provenance": dict(provenance), "quiescence": dict(quiescence),
            "restore_instructions": self._restore_instructions(archive_id), "generation_manifest": None,
        }

    def _parse_fresh_utc(self, value: object) -> None:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise GenerationArchiveError("quiescence marker issued_at must be a UTC Z timestamp")
        try:
            issued = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as exc:
            raise GenerationArchiveError("quiescence marker issued_at is invalid") from exc
        age = datetime.now(timezone.utc) - issued
        if age < timedelta(0) or age > QUIESCENCE_MAX_AGE:
            raise GenerationArchiveError("quiescence marker issued_at is outside the strict freshness window")

    def _reviewed_inventory_identity(self, provenance: Mapping[str, Any]) -> str:
        identity = provenance.get("reviewed_inventory_identity")
        if not isinstance(identity, str) or not _SHA256.fullmatch(identity):
            raise GenerationArchiveError("apply requires a lowercase reviewed_inventory_identity from the reviewed preview")
        return identity

    def _quiescence(self, provenance: Mapping[str, Any], archive_id: str, generation_id: str, inventory_identity: str) -> dict[str, Any]:
        if provenance.get("reviewed_inventory_identity") != inventory_identity:
            raise GenerationArchiveError("active inventory differs from the pinned reviewed preview; provenance.reviewed_inventory_identity must match exactly")
        proof = provenance.get("cutover_quiescence")
        if not isinstance(proof, Mapping):
            raise GenerationArchiveError("apply requires an explicit cutover_quiescence marker contract")
        marker_name, marker_digest = proof.get("marker_path"), proof.get("marker_sha256")
        if not isinstance(marker_name, str) or not isinstance(marker_digest, str) or not _SHA256.fullmatch(marker_digest):
            raise GenerationArchiveError("cutover_quiescence requires marker_path and lowercase marker_sha256")
        marker = _resolve(marker_name)
        if _is_within(marker, self.runs_root) or _is_within(marker, self.archive_root):
            raise GenerationArchiveError("quiescence marker must be outside active and archive roots")
        if not marker.is_file() or marker.is_symlink() or _hash_regular_file(marker)[1] != marker_digest:
            raise GenerationArchiveError("quiescence marker is missing or does not match marker_sha256")
        payload = _read_json(marker, label="quiescence marker")
        _require_schema(payload, name=QUIESCENCE_SCHEMA_NAME, version=QUIESCENCE_SCHEMA_VERSION, label="quiescence marker")
        required = {
            "state": "quiesced", "writer_scope": "all-writers-stopped", "runs_root": str(self.runs_root),
            "archive_root": str(self.archive_root), "archive_id": archive_id,
            "new_generation_id": generation_id, "inventory_identity": inventory_identity,
        }
        if any(payload.get(key) != value for key, value in required.items()):
            raise GenerationArchiveError("quiescence marker does not prove this exact cutover and reviewed_inventory_identity")
        if not isinstance(payload.get("issuer_id"), str) or not payload["issuer_id"].strip():
            raise GenerationArchiveError("quiescence marker requires a non-empty issuer_id")
        if not isinstance(payload.get("nonce"), str) or not _NONCE.fullmatch(payload["nonce"]):
            raise GenerationArchiveError("quiescence marker requires a cryptographic 32-byte lowercase-hex nonce")
        self._parse_fresh_utc(payload.get("issued_at"))
        return {"marker_path": str(marker), "marker_sha256": marker_digest, "marker": payload}

    def _consume_quiescence(self, quiescence: Mapping[str, Any]) -> dict[str, Any]:
        marker = quiescence.get("marker")
        if not isinstance(marker, Mapping):
            raise GenerationArchiveError("quiescence marker is missing from the durable proof")
        nonce = str(marker["nonce"])
        payload = {
            "schema_name": QUIESCENCE_CONSUMPTION_SCHEMA_NAME,
            "schema_version": QUIESCENCE_CONSUMPTION_SCHEMA_VERSION,
            "nonce": nonce, "issuer_id": marker["issuer_id"], "issued_at": marker["issued_at"],
            "runs_root": marker["runs_root"], "archive_root": marker["archive_root"],
            "archive_id": marker["archive_id"], "new_generation_id": marker["new_generation_id"],
            "inventory_identity": marker["inventory_identity"], "marker_sha256": quiescence["marker_sha256"],
        }
        path = self._consumed_nonce_path(nonce)
        digest = _write_immutable_json(path, payload)
        result = dict(quiescence)
        result["consumption"] = {"path": str(path), "sha256": digest}
        return result

    def _recorded_quiescence(self, manifest: Mapping[str, Any], provenance: Mapping[str, Any]) -> dict[str, Any]:
        recorded, requested = manifest.get("quiescence"), provenance.get("cutover_quiescence")
        if not isinstance(recorded, Mapping) or not isinstance(requested, Mapping):
            raise GenerationArchiveError("durable cutover quiescence proof is missing")
        if (recorded.get("marker_path") != requested.get("marker_path") or recorded.get("marker_sha256") != requested.get("marker_sha256") or not isinstance(recorded.get("marker"), Mapping) or not isinstance(recorded.get("consumption"), Mapping)):
            raise GenerationArchiveError("durable cutover quiescence proof conflicts with provenance")
        return dict(recorded)

    def _intent(self, archive_id: str) -> dict[str, Any] | None:
        path = self._intent_path(archive_id)
        if not path.exists():
            return None
        return self._load_intent(_read_json(path, label="cutover intent"))

    def _load_intent(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        copied = dict(payload)
        _require_schema(copied, name=INTENT_SCHEMA_NAME, version=INTENT_SCHEMA_VERSION, label="cutover intent")
        return copied

    def _write_intent(self, archive_id: str, manifest: Mapping[str, Any]) -> None:
        _write_immutable_json(self._intent_path(archive_id), {
            "schema_name": INTENT_SCHEMA_NAME, "schema_version": INTENT_SCHEMA_VERSION,
            "archive_id": archive_id, "prepared_manifest": dict(manifest),
        })

    def _prepared_from_intent(self, intent: Mapping[str, Any], archive_id: str, generation_id: str) -> dict[str, Any]:
        prepared = intent.get("prepared_manifest")
        if not isinstance(prepared, dict):
            raise GenerationArchiveError("cutover intent is missing a repairable prepared manifest")
        self._validate_manifest(prepared, archive_id, generation_id, self._intent_path(archive_id))
        if prepared.get("state") != "prepared":
            raise GenerationArchiveError("cutover intent prepared manifest has invalid state")
        return prepared

    def _repair_or_prepare(self, archive_id: str, generation_id: str, provenance: Mapping[str, Any], critical_artifacts: Mapping[str, str] | Iterable[str | Path] | None) -> dict[str, Any]:
        archive_dir, destination, manifest_path = self._paths(archive_id)
        intent = self._intent(archive_id)
        try:
            manifest = self._read_manifest_if_present(manifest_path)
        except GenerationArchiveError:
            if intent is None:
                raise
            manifest = None
        if manifest is not None:
            self._validate_manifest(manifest, archive_id, generation_id, manifest_path)
            return manifest
        if intent is not None:
            manifest = self._prepared_from_intent(intent, archive_id, generation_id)
            if manifest.get("provenance") != dict(provenance):
                raise GenerationArchiveError("provenance conflicts with the durable cutover intent")
            if not archive_dir.exists():
                _durable_mkdir(archive_dir)
                _checkpoint("archive_dir_created")
            if not archive_dir.is_dir() or archive_dir.is_symlink() or (destination.exists() and (not destination.is_dir() or destination.is_symlink())):
                raise GenerationArchiveError("durable intent conflicts with archive destination state")
            _atomic_json_write(manifest_path, manifest)
            _checkpoint("archive_manifest_written")
            return manifest
        if archive_dir.exists():
            raise GenerationArchiveError(f"archive destination already exists without durable intent: {archive_dir}")
        if not self.runs_root.exists():
            raise GenerationArchiveError("active runs root is missing without durable cutover state")
        reviewed_identity = self._reviewed_inventory_identity(provenance)
        quiescence = self._quiescence(provenance, archive_id, generation_id, reviewed_identity)
        if not self.archive_root.exists():
            _durable_mkdir(self.archive_root)
        elif not self.archive_root.is_dir() or self.archive_root.is_symlink():
            raise GenerationArchiveError("archive root is not a real directory")
        quiescence = self._consume_quiescence(quiescence)
        inventory = build_inventory(self.runs_root, critical_artifacts=critical_artifacts)
        if _inventory_identity(inventory) != reviewed_identity:
            raise GenerationArchiveError("active inventory differs from the pinned reviewed preview; refusing prepare")
        self._assert_same_volume()
        manifest = self._prepared_manifest(archive_id, generation_id, inventory, provenance, quiescence)
        self._write_intent(archive_id, manifest)
        _durable_mkdir(archive_dir)
        _checkpoint("archive_dir_created")
        _atomic_json_write(manifest_path, manifest)
        _checkpoint("archive_manifest_written")
        return manifest

    def _archived_inventory(self, destination: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
        inventory = manifest.get("inventory")
        if not isinstance(inventory, Mapping):
            raise GenerationArchiveError("archive manifest inventory is missing")
        artifacts = inventory.get("critical_artifacts")
        if not isinstance(artifacts, Mapping):
            raise GenerationArchiveError("archive manifest critical artifact inventory is malformed")
        expected: dict[str, str] = {}
        for relative, details in artifacts.items():
            if not isinstance(relative, str) or not isinstance(details, Mapping) or not isinstance(details.get("sha256"), str):
                raise GenerationArchiveError("archive manifest critical artifact inventory is malformed")
            expected[relative] = str(details["sha256"])
        source = inventory.get("critical_artifact_source")
        if source in {"mandatory_defaults", "formal_level_c_defaults"}:
            return build_inventory(destination)
        if source in {
            "mandatory_defaults_plus_caller_allowlist",
            "formal_level_c_defaults_plus_caller_allowlist",
        }:
            return build_inventory(destination, critical_artifacts=tuple(expected))
        if source in {
            "mandatory_defaults_plus_caller_hashes",
            "formal_level_c_defaults_plus_caller_hashes",
        }:
            return build_inventory(destination, critical_artifacts=expected)
        raise GenerationArchiveError("archive manifest critical artifact source is malformed")

    def _generation_manifest(self, archive_id: str, generation_id: str, archive: Mapping[str, Any]) -> dict[str, Any]:
        _, destination, manifest_path = self._paths(archive_id)
        return {
            "schema_name": GENERATION_SCHEMA_NAME, "schema_version": GENERATION_SCHEMA_VERSION,
            "new_generation_id": generation_id,
            "created_at": archive.get("generation_manifest_created_at", archive["prepared_at"]),
            "archive_linkage": {"archive_id": archive_id, "archive_manifest_path": str(manifest_path), "archived_runs_root": str(destination), "archive_prepared_at": archive["prepared_at"]},
            "source_runs_root": str(destination), "destination_runs_root": str(self.runs_root),
            "archived_inventory": archive["inventory"], "restore_instructions": archive["restore_instructions"],
            "provenance": archive["provenance"],
        }

    def _load_generation_manifest(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        copied = dict(payload)
        _require_schema(copied, name=GENERATION_SCHEMA_NAME, version=GENERATION_SCHEMA_VERSION, label="generation manifest")
        return copied

    def _assert_active_state(self, archive_id: str, generation_id: str, archive: Mapping[str, Any], *, require_generation_manifest: bool, allow_truncated_generation_manifest: bool = False) -> None:
        if not self.runs_root.is_dir() or self.runs_root.is_symlink():
            raise GenerationArchiveError("new active root is not a real directory")
        derived = self.runs_root / DERIVED_DIRECTORY_NAME
        if not derived.is_dir() or derived.is_symlink():
            raise GenerationArchiveError("new active derived path is not a real directory")
        if any(derived.iterdir()):
            raise GenerationArchiveError("new active derived directory contains unexpected data; refusing recovery")
        generation_path = self.runs_root / GENERATION_MANIFEST_NAME
        names = {path.name for path in self.runs_root.iterdir()}
        allowed = {DERIVED_DIRECTORY_NAME, GENERATION_MANIFEST_NAME} if generation_path.exists() else {DERIVED_DIRECTORY_NAME}
        if names != allowed:
            raise GenerationArchiveError("new active root contains unexpected data; refusing recovery")
        if generation_path.exists():
            if not generation_path.is_file() or generation_path.is_symlink():
                raise GenerationArchiveError("generation manifest differs from durable cutover state")
            actual = generation_path.read_bytes()
            expected = _canonical_json_bytes(self._generation_manifest(archive_id, generation_id, archive))
            if actual != expected:
                try:
                    self._load_generation_manifest(_read_json(generation_path, label="generation manifest"))
                except GenerationArchiveError:
                    if allow_truncated_generation_manifest:
                        return
                raise GenerationArchiveError("generation manifest differs from durable cutover state")
            self._load_generation_manifest(_read_json(generation_path, label="generation manifest"))
        elif require_generation_manifest:
            raise GenerationArchiveError("complete archive is missing its generation manifest")

    def _ensure_active_root(self, archive_id: str, generation_id: str, archive: Mapping[str, Any]) -> None:
        if not self.runs_root.exists():
            _durable_mkdir(self.runs_root)
            _checkpoint("active_root_created")
        derived = self.runs_root / DERIVED_DIRECTORY_NAME
        if not derived.exists():
            _durable_mkdir(derived)
            _checkpoint("active_derived_created")
        self._assert_active_state(
            archive_id, generation_id, archive, require_generation_manifest=False,
            allow_truncated_generation_manifest=True,
        )

    def _ensure_generation_manifest(self, archive_id: str, generation_id: str, archive: Mapping[str, Any]) -> str:
        path = self.runs_root / GENERATION_MANIFEST_NAME
        expected = self._generation_manifest(archive_id, generation_id, archive)
        data = _canonical_json_bytes(expected)
        if path.exists():
            if path.read_bytes() != data:
                try:
                    self._load_generation_manifest(_read_json(path, label="generation manifest"))
                except GenerationArchiveError:
                    _atomic_json_write(path, expected)
                    _checkpoint("generation_manifest_written")
                    digest = hashlib.sha256(data).hexdigest()
                else:
                    raise GenerationArchiveError("generation manifest differs from durable cutover state")
            else:
                self._load_generation_manifest(_read_json(path, label="generation manifest"))
                digest = hashlib.sha256(data).hexdigest()
        else:
            digest = _write_immutable_json(path, expected)
            _checkpoint("generation_manifest_written")
        self._assert_active_state(archive_id, generation_id, archive, require_generation_manifest=True)
        return digest

    def cutover(self, archive_id: str, new_generation_id: str, *, provenance: Mapping[str, Any], critical_artifacts: Mapping[str, str] | Iterable[str | Path] | None = None) -> dict[str, Any]:
        archive_id = validate_safe_id(archive_id, field_name="archive_id")
        new_generation_id = validate_safe_id(new_generation_id, field_name="new_generation_id")
        if not isinstance(provenance, Mapping):
            raise ValueError("provenance must be a mapping")
        with _exclusive_lock(self.archive_root.parent / _LOCK_NAME):
            manifest = self._repair_or_prepare(archive_id, new_generation_id, provenance, critical_artifacts)
            self._validate_manifest(manifest, archive_id, new_generation_id, self._paths(archive_id)[2])
            if manifest.get("provenance") != dict(provenance):
                raise GenerationArchiveError("provenance conflicts with the prepared archive manifest")
            identity = _inventory_identity(manifest["inventory"])
            _, destination, manifest_path = self._paths(archive_id)
            if manifest["state"] == "complete":
                self._recorded_quiescence(manifest, provenance)
                verified = self._archived_inventory(destination, manifest)
                recorded = manifest.get("verified_archived_inventory")
                if not isinstance(recorded, Mapping) or _inventory_identity(verified) != _inventory_identity(recorded):
                    raise GenerationArchiveError("complete archive inventory no longer matches its manifest")
                self._assert_active_state(archive_id, new_generation_id, manifest, require_generation_manifest=True)
                return {"resumed": True, "already_complete": True, "manifest": manifest}
            source_exists, destination_exists = self.runs_root.exists(), destination.exists()
            resumed = not source_exists or destination_exists
            if source_exists and not destination_exists:
                self._recorded_quiescence(manifest, provenance)
                current = build_inventory(self.runs_root, critical_artifacts=critical_artifacts)
                if _inventory_identity(current) != identity:
                    raise GenerationArchiveError("active inventory differs from pinned reviewed preview; refusing rename")
                self._assert_same_volume()
                _durable_replace(self.runs_root, destination)
                _checkpoint("runs_renamed")
            elif not source_exists and destination_exists:
                self._recorded_quiescence(manifest, provenance)
            elif source_exists and destination_exists:
                self._recorded_quiescence(manifest, provenance)
            else:
                raise GenerationArchiveError("both active source and archive destination are missing")
            archived = self._archived_inventory(destination, manifest)
            if _inventory_identity(archived) != identity:
                raise GenerationArchiveError("archived inventory differs from pinned reviewed preview")
            self._ensure_active_root(archive_id, new_generation_id, manifest)
            generation_hash = self._ensure_generation_manifest(archive_id, new_generation_id, manifest)
            completed = dict(manifest)
            completed.update({"state": "complete", "completed_at": utc_now(), "verified_archived_inventory": archived, "verified_active_run_count": 0, "generation_manifest": {"path": str(self.runs_root / GENERATION_MANIFEST_NAME), "sha256": generation_hash}})
            _atomic_json_write(manifest_path, completed)
            _checkpoint("archive_manifest_completed")
            return {"resumed": resumed, "already_complete": False, "manifest": completed}

    def restore_plan(self, archive_id: str, *, destination_runs_root: Path | str | None = None) -> dict[str, Any]:
        archive_id = validate_safe_id(archive_id, field_name="archive_id")
        _, archived, path = self._paths(archive_id)
        manifest = self._load_archive_manifest(_read_json(path, label="archive manifest"))
        if manifest.get("state") != "complete" or not archived.is_dir() or archived.is_symlink():
            raise GenerationArchiveError("only a complete archive with a runs directory can be restored")
        destination = _resolve(destination_runs_root) if destination_runs_root else self.runs_root
        return {"dry_run": True, "archive_id": archive_id, "source_archived_runs_root": str(archived), "proposed_destination_runs_root": str(destination), "destination_exists": destination.exists(), "required_preconditions": ["Choose an empty, non-existent destination.", "This API never performs a restore or removes current runs."], "inventory": manifest.get("verified_archived_inventory", manifest.get("inventory"))}


__all__ = ["ARCHIVE_MANIFEST_NAME", "DEFAULT_CRITICAL_ARTIFACT_ALLOWLIST", "DERIVED_DIRECTORY_NAME", "GENERATION_MANIFEST_NAME", "GenerationArchiveError", "GenerationArchiveService", "QUIESCENCE_MAX_AGE", "build_inventory", "utc_now", "validate_safe_id"]
