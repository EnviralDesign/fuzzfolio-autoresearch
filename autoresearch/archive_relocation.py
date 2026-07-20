"""Immutable, content-verified resolution for relocated generation archives.

Receipts are deliberately stored outside an archive.  They bind one stable archive
id and its original archive root to a real replacement root, while the completed
archive manifest remains the authoritative inventory contract.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat as stat_module
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .evidence_artifacts import write_immutable_json
from .evidence_plan import canonical_sha256
from .generation_archive import (
    ARCHIVE_MANIFEST_NAME,
    ARCHIVE_SCHEMA_NAME,
    ARCHIVE_SCHEMA_VERSION,
    validate_safe_id,
)


RELOCATION_RECEIPT_SCHEMA = "autoresearch.archive-relocation-receipt-v1"
RELOCATION_PREFLIGHT_SCHEMA = "autoresearch.archive-relocation-preflight-v1"
RELOCATION_CONTENT_INVENTORY_SCHEMA = "autoresearch.archive-relocation-content-inventory-v1"
_WINDOWS_REPARSE_POINT = 0x0400
DEFAULT_RELOCATION_RECEIPTS_ROOT = (
    Path(__file__).resolve().parent.parent / "archive-relocation-receipts"
)


class ArchiveRelocationError(RuntimeError):
    """Raised when a recorded relocation cannot be proven safe and unchanged."""


@dataclass(frozen=True)
class ResolvedArchiveRelocation:
    archive_id: str
    original_archive_root: Path
    destination_archive_root: Path
    original_archive_directory: Path
    destination_archive_directory: Path
    runs_root: Path
    receipt_path: Path
    inventory_identity: str


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        details = path.lstat()
    except FileNotFoundError:
        return False
    return path.is_symlink() or bool(
        int(getattr(details, "st_file_attributes", 0)) & _WINDOWS_REPARSE_POINT
    )


def _resolved(path: Path | str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _lexical_absolute(path: Path | str) -> Path:
    return Path(os.path.abspath(os.fspath(Path(path).expanduser())))


def _assert_no_reparse_components(path: Path | str, *, label: str) -> Path:
    """Reject links/reparse points before resolving any existing path component."""

    absolute = _lexical_absolute(path)
    for component in reversed((absolute, *absolute.parents)):
        if _is_reparse_or_symlink(component):
            raise ArchiveRelocationError(
                f"{label} has a symlink or reparse-point ancestor: {component}"
            )
    return absolute


def _assert_real_directory(path: Path | str, *, label: str) -> Path:
    raw = _assert_no_reparse_components(path, label=label)
    try:
        resolved = raw.resolve(strict=True)
    except OSError as exc:
        raise ArchiveRelocationError(f"{label} must be a real directory: {raw}") from exc
    if not resolved.is_dir() or _is_reparse_or_symlink(resolved):
        raise ArchiveRelocationError(f"{label} must be a real directory: {raw}")
    return resolved


def _assert_regular_file(path: Path, *, label: str) -> Path:
    raw = _assert_no_reparse_components(path, label=label)
    if not raw.is_file():
        raise ArchiveRelocationError(f"{label} must be a regular file: {raw}")
    return raw.resolve(strict=True)


def _json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArchiveRelocationError(f"{label} is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise ArchiveRelocationError(f"{label} must be a JSON object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def _registry_root(*, create: bool) -> Path:
    root = _assert_no_reparse_components(
        DEFAULT_RELOCATION_RECEIPTS_ROOT, label="relocation registry root"
    )
    if create:
        root.mkdir(parents=True, exist_ok=True)
        return _assert_real_directory(root, label="relocation registry root")
    if root.exists():
        return _assert_real_directory(root, label="relocation registry root")
    return root


def _registry_record_path(
    path: Path | str, *, label: str, create_registry: bool
) -> Path:
    root = _registry_root(create=create_registry)
    supplied = Path(path).expanduser()
    candidate = root / supplied if not supplied.is_absolute() else _lexical_absolute(supplied)
    candidate = _assert_no_reparse_components(candidate, label=label)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ArchiveRelocationError(f"{label} must live within the relocation registry") from exc
    return candidate


def _receipt_path(archive_id: str, *, create_registry: bool = False) -> Path:
    return _registry_root(create=create_registry) / f"{archive_id}.json"


def _write_registry_record(path: Path, payload: Mapping[str, Any], *, label: str) -> None:
    path = _registry_record_path(path, label=label, create_registry=True)
    _assert_no_reparse_components(path.parent, label=f"{label} parent")
    path.parent.mkdir(parents=True, exist_ok=True)
    _assert_real_directory(path.parent, label=f"{label} parent")
    if path.exists() or path.is_symlink():
        _assert_regular_file(path, label=label)
    try:
        write_immutable_json(path, payload)
    except RuntimeError as exc:
        raise ArchiveRelocationError(f"{label} conflicts with an existing record") from exc
    _assert_regular_file(path, label=label)


def _require_within(path: Path, root: Path, *, label: str) -> Path:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArchiveRelocationError(f"{label} escapes the recorded archive root: {path}") from exc
    return path


def _assert_outside_archives(path: Path, *archive_directories: Path) -> None:
    for archive_directory in archive_directories:
        try:
            path.resolve(strict=False).relative_to(archive_directory.resolve(strict=False))
        except ValueError:
            continue
        raise ArchiveRelocationError(
            "relocation record must live outside source and destination archives"
        )


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _content_file(path: Path) -> tuple[int, str]:
    """Hash one regular file once and reject replacement or mutation during the read."""

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ArchiveRelocationError(f"could not read relocation content file: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat_module.S_ISREG(before.st_mode):
            raise ArchiveRelocationError(f"unsupported relocation filesystem entry: {path}")
        digest = hashlib.sha256()
        while True:
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    snapshot = lambda value: (  # noqa: E731 - compact immutable stat comparison
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if snapshot(before) != snapshot(after):
        raise ArchiveRelocationError(f"relocation content changed while hashing: {path}")
    return int(after.st_size), digest.hexdigest()


def _build_content_inventory(root: Path | str) -> dict[str, Any]:
    """Build relocation-only identity from paths, byte sizes, and file bytes."""

    resolved = _assert_real_directory(root, label="relocation content root")
    records: list[tuple[str, str, int, str | None]] = []
    def _walk_error(error: OSError) -> None:
        location = getattr(error, "filename", None) or resolved
        raise ArchiveRelocationError(
            f"could not traverse relocation content tree: {location}"
        ) from error

    for current, directory_names, file_names in os.walk(
        resolved, topdown=True, followlinks=False, onerror=_walk_error
    ):
        directory_names.sort()
        file_names.sort()
        current_path = Path(current)
        for name in directory_names:
            path = current_path / name
            if _is_reparse_or_symlink(path) or not path.is_dir():
                raise ArchiveRelocationError(
                    f"relocation content contains a symlink, reparse point, or unsupported entry: {path}"
                )
            records.append((path.relative_to(resolved).as_posix(), "directory", 0, None))
        for name in file_names:
            path = current_path / name
            if _is_reparse_or_symlink(path):
                raise ArchiveRelocationError(
                    f"relocation content contains a symlink or reparse point: {path}"
                )
            size, content_sha256 = _content_file(path)
            records.append(
                (path.relative_to(resolved).as_posix(), "file", size, content_sha256)
            )
    records.sort(key=lambda item: (item[0], item[1]))
    digest = hashlib.sha256()
    file_count = directory_count = file_bytes = 0
    for relative, kind, size, content_sha256 in records:
        record: dict[str, Any] = {"path": relative, "kind": kind}
        if kind == "file":
            file_count += 1
            file_bytes += size
            record.update({"bytes": size, "content_sha256": content_sha256})
        else:
            directory_count += 1
        digest.update(_canonical_bytes(record))
    return {
        "schema": RELOCATION_CONTENT_INVENTORY_SCHEMA,
        "directory_count": directory_count,
        "file_count": file_count,
        "file_bytes": file_bytes,
        "content_tree_sha256": digest.hexdigest(),
    }


def _load_complete_manifest(
    *, archive_id: str, original_archive_root: Path, destination_archive_root: Path
) -> tuple[Path, dict[str, Any], Path, str]:
    destination_directory = destination_archive_root / archive_id
    _assert_real_directory(destination_directory, label="relocated archive directory")
    manifest_path = _assert_regular_file(
        destination_directory / ARCHIVE_MANIFEST_NAME, label="relocated archive manifest"
    )
    manifest = _json(manifest_path, label="relocated archive manifest")
    if (
        manifest.get("schema_name") != ARCHIVE_SCHEMA_NAME
        or manifest.get("schema_version") != ARCHIVE_SCHEMA_VERSION
        or manifest.get("state") != "complete"
        or manifest.get("archive_id") != archive_id
    ):
        raise ArchiveRelocationError("relocated archive manifest identity is not complete")
    if _resolved(str(manifest.get("archive_root") or "")) != original_archive_root:
        raise ArchiveRelocationError("relocated archive manifest has the wrong original archive root")
    expected_runs = original_archive_root / archive_id / "runs"
    if _resolved(str(manifest.get("destination_runs_root") or "")) != expected_runs:
        raise ArchiveRelocationError("relocated archive manifest has the wrong archive directory identity")
    inventory = manifest.get("verified_archived_inventory")
    if not isinstance(inventory, Mapping) or not isinstance(inventory.get("inventory_identity"), str):
        raise ArchiveRelocationError("relocated archive manifest has no verified archived inventory")
    return destination_directory, manifest, destination_directory / "runs", _sha256(manifest_path)


def _validate_destination_metadata(
    *, archive_id: str, original_archive_root: Path, destination_archive_root: Path,
    expected_manifest_sha256: str | None = None,
    expected_inventory_identity: str | None = None,
) -> tuple[Path, Path, str, str]:
    destination_directory, manifest, runs_root, manifest_sha256 = _load_complete_manifest(
        archive_id=archive_id,
        original_archive_root=original_archive_root,
        destination_archive_root=destination_archive_root,
    )
    if expected_manifest_sha256 is not None and manifest_sha256 != expected_manifest_sha256:
        raise ArchiveRelocationError("relocated archive manifest hash drifted")
    verified = manifest["verified_archived_inventory"]
    expected_identity = str(verified["inventory_identity"])
    if expected_inventory_identity is not None and expected_identity != expected_inventory_identity:
        raise ArchiveRelocationError("relocated archive inventory identity conflicts with receipt")
    runs_root = _assert_real_directory(runs_root, label="relocated archive runs root")
    return destination_directory, runs_root, manifest_sha256, expected_identity


def preflight_archive_relocation(
    *, archive_id: str, original_archive_root: Path | str,
    destination_archive_root: Path | str, report_path: Path | str,
) -> dict[str, Any]:
    """Prove source/destination byte equivalence and persist a compact report."""

    archive_id = validate_safe_id(archive_id, field_name="archive_id")
    original = _assert_real_directory(
        original_archive_root, label="original archive root"
    )
    destination = _assert_real_directory(destination_archive_root, label="relocation destination archive root")
    if original == destination:
        raise ArchiveRelocationError("relocation destination must differ from the original archive root")
    destination_directory, _runs_root, manifest_sha256, inventory_identity = _validate_destination_metadata(
        archive_id=archive_id,
        original_archive_root=original,
        destination_archive_root=destination,
    )
    original_directory = _assert_real_directory(
        original / archive_id, label="original archive directory"
    )
    report = _registry_record_path(
        report_path, label="relocation preflight report", create_registry=True
    )
    _assert_outside_archives(report, original_directory, destination_directory)
    source_inventory = _build_content_inventory(original_directory)
    destination_inventory = _build_content_inventory(destination_directory)
    if source_inventory != destination_inventory:
        raise ArchiveRelocationError(
            "relocation source and destination content-only inventories differ"
        )
    identity = {
        "schema": RELOCATION_PREFLIGHT_SCHEMA,
        "archive_id": archive_id,
        "original_archive_root": str(original),
        "destination_archive_root": str(destination),
        "original_archive_directory": str(original_directory),
        "destination_archive_directory": str(destination_directory),
        "archive_manifest_sha256": manifest_sha256,
        "verified_inventory_identity": inventory_identity,
        "content_inventory": source_inventory,
    }
    payload = {**identity, "report_sha256": canonical_sha256(identity)}
    _write_registry_record(report, payload, label="relocation preflight report")
    return {**payload, "report_path": str(report), "mode": "preflight"}


def _load_preflight_report(path: Path | str) -> tuple[Path, dict[str, Any]]:
    registered_path = _registry_record_path(
        path, label="relocation preflight report", create_registry=False
    )
    report_path = _assert_regular_file(
        registered_path, label="relocation preflight report"
    )
    report = _json(report_path, label="relocation preflight report")
    identity = {key: value for key, value in report.items() if key != "report_sha256"}
    if (
        report.get("schema") != RELOCATION_PREFLIGHT_SCHEMA
        or report.get("report_sha256") != canonical_sha256(identity)
    ):
        raise ArchiveRelocationError("relocation preflight report identity drifted")
    inventory = report.get("content_inventory")
    if (
        not isinstance(inventory, Mapping)
        or inventory.get("schema") != RELOCATION_CONTENT_INVENTORY_SCHEMA
        or not isinstance(inventory.get("content_tree_sha256"), str)
    ):
        raise ArchiveRelocationError("relocation preflight content inventory is invalid")
    return report_path, report


def register_archive_relocation(
    *, archive_id: str, original_archive_root: Path | str,
    destination_archive_root: Path | str, preflight_report: Path | str,
) -> dict[str, Any]:
    """Consume an exact preflight after source deletion and publish the receipt."""

    archive_id = validate_safe_id(archive_id, field_name="archive_id")
    original = _assert_real_directory(
        original_archive_root, label="original archive root"
    )
    destination = _assert_real_directory(
        destination_archive_root, label="relocation destination archive root"
    )
    report_path, preflight = _load_preflight_report(preflight_report)
    _assert_outside_archives(
        report_path, original / archive_id, destination / archive_id
    )
    if (
        preflight.get("archive_id") != archive_id
        or preflight.get("original_archive_root") != str(original)
        or preflight.get("destination_archive_root") != str(destination)
        or preflight.get("original_archive_directory") != str(original / archive_id)
        or preflight.get("destination_archive_directory") != str(destination / archive_id)
    ):
        raise ArchiveRelocationError("relocation preflight report conflicts with registration")
    if (original / archive_id).exists() or (original / archive_id).is_symlink():
        raise ArchiveRelocationError("original archive directory still exists; delete the local copy first")
    directory, _runs_root, manifest_sha256, inventory_identity = _validate_destination_metadata(
        archive_id=archive_id,
        original_archive_root=original,
        destination_archive_root=destination,
        expected_manifest_sha256=str(preflight.get("archive_manifest_sha256") or ""),
        expected_inventory_identity=str(preflight.get("verified_inventory_identity") or ""),
    )
    destination_inventory = _build_content_inventory(directory)
    if destination_inventory != preflight.get("content_inventory"):
        raise ArchiveRelocationError(
            "relocation destination content differs from preflight identity"
        )
    registry = _registry_root(create=True)
    report_relative = report_path.relative_to(registry).as_posix()
    identity = {
        "schema": RELOCATION_RECEIPT_SCHEMA,
        "archive_id": archive_id,
        "original_archive_root": str(original),
        "destination_archive_root": str(destination),
        "original_archive_directory": str(original / archive_id),
        "destination_archive_directory": str(directory),
        "archive_manifest_sha256": manifest_sha256,
        "verified_inventory_identity": inventory_identity,
        "content_inventory": destination_inventory,
        "preflight_report_relative_path": report_relative,
        "preflight_report_sha256": _sha256(report_path),
        "preflight_report_identity": str(preflight["report_sha256"]),
    }
    receipt = {**identity, "receipt_sha256": canonical_sha256(identity)}
    receipt_path = _receipt_path(archive_id, create_registry=True)
    _assert_outside_archives(
        receipt_path, original / archive_id, destination / archive_id
    )
    _write_registry_record(receipt_path, receipt, label="relocation receipt")
    return {**receipt, "receipt_path": str(receipt_path)}


def _validate_receipt_preflight_binding(receipt: Mapping[str, Any]) -> None:
    report_path, report = _load_preflight_report(
        str(receipt.get("preflight_report_relative_path") or "")
    )
    _assert_outside_archives(
        report_path,
        Path(str(receipt.get("original_archive_directory") or "")),
        Path(str(receipt.get("destination_archive_directory") or "")),
    )
    if (
        _sha256(report_path) != receipt.get("preflight_report_sha256")
        or report.get("report_sha256") != receipt.get("preflight_report_identity")
        or report.get("archive_id") != receipt.get("archive_id")
        or report.get("original_archive_root") != receipt.get("original_archive_root")
        or report.get("destination_archive_root") != receipt.get("destination_archive_root")
        or report.get("original_archive_directory")
        != receipt.get("original_archive_directory")
        or report.get("destination_archive_directory")
        != receipt.get("destination_archive_directory")
        or report.get("archive_manifest_sha256")
        != receipt.get("archive_manifest_sha256")
        or report.get("verified_inventory_identity")
        != receipt.get("verified_inventory_identity")
        or report.get("content_inventory") != receipt.get("content_inventory")
    ):
        raise ArchiveRelocationError(
            "relocation receipt conflicts with its immutable preflight report"
        )


def resolve_archive_relocation(
    *, archive_id: str,
) -> ResolvedArchiveRelocation | None:
    """Load and re-verify the persisted receipt for an archive id, if registered."""

    archive_id = validate_safe_id(archive_id, field_name="archive_id")
    receipt_path = _receipt_path(archive_id)
    if not receipt_path.exists() and not receipt_path.is_symlink():
        return None
    receipt_path = _assert_regular_file(receipt_path, label="relocation receipt")
    receipt = _json(receipt_path, label="relocation receipt")
    identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if receipt.get("receipt_sha256") != canonical_sha256(identity):
        raise ArchiveRelocationError("relocation receipt drifted")
    if receipt.get("schema") != RELOCATION_RECEIPT_SCHEMA or receipt.get("archive_id") != archive_id:
        raise ArchiveRelocationError("relocation receipt archive identity is invalid")
    _validate_receipt_preflight_binding(receipt)
    original = _assert_real_directory(
        str(receipt.get("original_archive_root") or ""),
        label="original archive root",
    )
    destination = _assert_real_directory(
        str(receipt.get("destination_archive_root") or ""), label="relocation destination archive root"
    )
    if original == destination:
        raise ArchiveRelocationError("relocation receipt does not move the archive")
    if (original / archive_id).exists() or (original / archive_id).is_symlink():
        raise ArchiveRelocationError("original archive directory reappeared after relocation")
    if receipt.get("original_archive_directory") != str(original / archive_id):
        raise ArchiveRelocationError("relocation receipt has the wrong original archive directory")
    directory, runs_root, manifest_sha256, inventory_identity = _validate_destination_metadata(
        archive_id=archive_id,
        original_archive_root=original,
        destination_archive_root=destination,
        expected_manifest_sha256=str(receipt.get("archive_manifest_sha256") or ""),
        expected_inventory_identity=str(receipt.get("verified_inventory_identity") or ""),
    )
    if receipt.get("destination_archive_directory") != str(directory):
        raise ArchiveRelocationError("relocation receipt has the wrong destination archive directory")
    return ResolvedArchiveRelocation(
        archive_id=archive_id,
        original_archive_root=original,
        destination_archive_root=destination,
        original_archive_directory=original / archive_id,
        destination_archive_directory=directory,
        runs_root=runs_root,
        receipt_path=receipt_path,
        inventory_identity=inventory_identity,
    )


def resolve_archive_path(
    recorded_path: Path | str, *, archive_id: str,
) -> Path:
    """Safely rebase a recorded archive-contained path when its receipt exists."""

    relocation = resolve_archive_relocation(archive_id=archive_id)
    raw = Path(recorded_path).expanduser()
    if any(part == ".." for part in raw.parts):
        raise ArchiveRelocationError(f"recorded archive path escapes by traversal: {raw}")
    if relocation is None:
        return raw.resolve(strict=False)
    recorded = raw.resolve(strict=False)
    try:
        relative = recorded.relative_to(relocation.original_archive_directory)
    except ValueError:
        # Controls may be deliberately stored outside the archive. They are not
        # rebased, but an archive root passed to a caller is checked separately.
        return recorded
    candidate = (relocation.destination_archive_directory / relative).resolve(strict=False)
    _require_within(candidate, relocation.destination_archive_directory, label="recorded archive path")
    return candidate


def resolve_archive_runs_root(
    recorded_runs_root: Path | str, *, archive_id: str,
) -> Path:
    """Resolve a recorded archive runs root, rejecting a mismatched receipt/root."""

    relocation = resolve_archive_relocation(archive_id=archive_id)
    raw = Path(recorded_runs_root).expanduser()
    if relocation is None:
        return _assert_real_directory(raw, label="archive runs root")
    recorded = raw.resolve(strict=False)
    expected = relocation.original_archive_directory / "runs"
    if recorded != expected:
        raise ArchiveRelocationError("recorded archive runs root conflicts with relocation receipt")
    return relocation.runs_root


__all__ = [
    "ArchiveRelocationError",
    "DEFAULT_RELOCATION_RECEIPTS_ROOT",
    "RELOCATION_RECEIPT_SCHEMA",
    "ResolvedArchiveRelocation",
    "preflight_archive_relocation",
    "register_archive_relocation",
    "resolve_archive_path",
    "resolve_archive_relocation",
    "resolve_archive_runs_root",
]
