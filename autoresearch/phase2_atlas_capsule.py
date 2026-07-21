"""Fail-closed retention capsule for completed Phase 2 Atlas evidence.

This module deliberately has no delete operation.  It builds a compact copy of
the direct, published stage aggregates and authority records; raw task trees
remain untouched and can only be identified by the cleanup preview.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .evidence_plan import canonical_sha256


CAPSULE_MANIFEST_NAME = "phase2-atlas-authority-capsule-manifest.json"
MASTER_PLAN_RELATIVE_PATH = Path("z_docs/PHASE3_RESEARCH_AND_OPERATIONS_MASTER_PLAN_2026-07-21.md")
EXPECTED_CUTOFFS = frozenset({"A", "B", "C", "D"})
ROOT_AUTHORITY_FILES = (
    "atlas-lab-run.json",
    "atlas-lab-summary.json",
    "execution-journal.json",
)
STAGE_DIRECTORIES = (
    "indicator-atlas",
    "signal-atlas",
    "forward-response-atlas",
    "anchor-pair-atlas",
    "anchor-pair-timing-atlas",
    "discovery-pair-atlas",
    "discovery-cluster-atlas",
    "discovery-recipe-validation-atlas",
    "discovery-recipe-scrutiny-atlas",
    "recipe-priors",
)
CONTROL_REQUIRED_FILES = ("protocol.json", "protocol-authority.json")
FORENSIC_REPORT_FILENAMES = (
    "PHASE-2-ATLAS-FORENSIC-COMPARISON-REPORT.md",
    "SECOND-ANALYSIS-ATLAS-FORENSIC-REVIEW-20260721.md",
)
AGGREGATE_SUFFIXES = frozenset({".csv", ".json", ".md"})


class CapsuleError(ValueError):
    """Raised before any mutation when a capsule cannot be trusted."""


@dataclass(frozen=True)
class CapsuleEntry:
    source: Path
    capsule_relative_path: str


@dataclass(frozen=True)
class _VerifiedTempDirectory:
    path: Path
    device: int
    inode: int


def _canonical_json(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_reparse_point(path: Path) -> bool:
    try:
        attributes = path.lstat().st_file_attributes
    except AttributeError:
        attributes = 0
    return path.is_symlink() or bool(attributes & getattr(os, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _reject_reparse_path(path: Path, *, label: str) -> None:
    if _is_reparse_point(path):
        raise CapsuleError(f"{label} is a symlink or reparse point: {path}")


def _reject_reparse_ancestors(path: Path, *, label: str) -> None:
    """Reject every existing lexical ancestor through the volume or UNC-share root."""
    cursor = _absolute_without_resolving(path)
    while True:
        if cursor.exists() or cursor.is_symlink():
            _reject_reparse_path(cursor, label=f"{label} ancestor")
        parent = cursor.parent
        if parent == cursor:
            return
        cursor = parent


def _resolve_existing(path: Path, *, label: str) -> Path:
    if not path.exists():
        raise CapsuleError(f"missing {label}: {path}")
    _reject_reparse_ancestors(path, label=label)
    return path.resolve()


def _absolute_without_resolving(path: Path) -> Path:
    return Path(os.path.abspath(path))


def _resolve_for_containment(path: Path) -> Path:
    """Normalize mapped-drive and UNC spellings without requiring a new path to exist."""
    return path.resolve(strict=False)


def _validate_existing_ancestors(root: Path, path: Path, *, label: str) -> tuple[Path, Path]:
    """Reject links in every existing component and prove resolved containment."""
    configured_root = _resolve_existing(root, label="configured root")
    if not configured_root.is_dir():
        raise CapsuleError(f"configured root is not a directory: {configured_root}")
    candidate = _absolute_without_resolving(path)
    _reject_reparse_ancestors(candidate, label=label)
    resolved_candidate = _resolve_for_containment(candidate)
    try:
        resolved_candidate.relative_to(configured_root)
    except ValueError as exc:
        raise CapsuleError(f"{label} is outside its configured root: {path}") from exc
    return candidate, configured_root


def _require_under(path: Path, root: Path, *, label: str) -> Path:
    _validate_existing_ancestors(root, path, label=label)
    resolved = _resolve_existing(path, label=label)
    configured_root = _resolve_existing(root, label="configured root")
    try:
        resolved.relative_to(configured_root)
    except ValueError as exc:
        raise CapsuleError(f"{label} is outside its configured root: {path}") from exc
    return resolved


def _relative_to_repo(path: Path, repo_root: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapsuleError(f"invalid {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise CapsuleError(f"invalid {label}; expected an object: {path}")
    return payload


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and value.startswith("sha256:") and len(value) == 71 and all(
        character in "0123456789abcdef" for character in value[7:]
    )


def _validate_successful_terminal_receipt(receipt: object, *, task_id: object, root: Path) -> None:
    if not isinstance(receipt, dict):
        raise CapsuleError(f"terminal receipt is malformed at {task_id}: {root}")
    payload = receipt.get("payload")
    if not isinstance(payload, dict) or receipt.get("receipt_sha256") != canonical_sha256(payload):
        raise CapsuleError(f"terminal receipt identity is invalid at {task_id}: {root}")
    outcome = payload.get("outcome")
    if outcome is not None and outcome not in {"complete", "completed", "success", "succeeded"}:
        raise CapsuleError(f"terminal receipt is not successful at {task_id}: {root}")
    if not isinstance(payload.get("stage"), str) or not payload["stage"].strip():
        raise CapsuleError(f"terminal receipt has no successful stage at {task_id}: {root}")
    artifact_receipt = payload.get("artifact_receipt")
    if not isinstance(artifact_receipt, dict):
        raise CapsuleError(f"terminal receipt has no artifact receipt at {task_id}: {root}")
    receipt_identity = artifact_receipt.get("receipt_sha256")
    receipt_payload = dict(artifact_receipt)
    receipt_payload.pop("receipt_sha256", None)
    files = receipt_payload.get("files")
    if (
        not isinstance(receipt_payload.get("root"), str)
        or not receipt_payload["root"].strip()
        or not isinstance(files, dict)
        or not files
        or receipt_identity != canonical_sha256(receipt_payload)
        or any(not isinstance(relative, str) or not relative or not _is_sha256(digest) for relative, digest in files.items())
    ):
        raise CapsuleError(f"terminal artifact receipt is malformed at {task_id}: {root}")


def _validate_completed_root(root: Path, atlas_runs_root: Path) -> str:
    _require_under(root, atlas_runs_root, label="Atlas root")
    if not root.is_dir():
        raise CapsuleError(f"Atlas root is not a directory: {root}")
    for filename in ROOT_AUTHORITY_FILES:
        _require_under(root / filename, root, label=f"required authority file {filename}")
    run = _load_json(root / "atlas-lab-run.json", label="Atlas run record")
    summary = _load_json(root / "atlas-lab-summary.json", label="Atlas summary")
    if run.get("status") != "completed" or summary.get("status") != "completed":
        raise CapsuleError(f"Atlas root is not completed: {root}")
    lineage = summary.get("historical_lineage")
    if not isinstance(lineage, dict) or not isinstance(lineage.get("cutoff_key"), str):
        raise CapsuleError(f"Atlas root is missing historical cutoff authority: {root}")
    cutoff = lineage["cutoff_key"].upper()
    journal = _load_json(root / "execution-journal.json", label="execution journal")
    if journal.get("schema_version") != "autoresearch-durable-execution-v1":
        raise CapsuleError(f"execution journal schema is invalid: {root}")
    journal_identity = journal.get("journal_identity")
    journal_payload = dict(journal)
    journal_payload.pop("journal_identity", None)
    if journal_identity != canonical_sha256(journal_payload):
        raise CapsuleError(f"execution journal identity is invalid: {root}")
    tasks = journal.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        raise CapsuleError(f"execution journal has no durable tasks: {root}")
    for task_id, task in tasks.items():
        if not isinstance(task, dict) or task.get("status") != "terminal":
            raise CapsuleError(f"execution journal is active or incomplete at {task_id}: {root}")
        _validate_successful_terminal_receipt(task.get("terminal_receipt"), task_id=task_id, root=root)
    return cutoff


def _direct_aggregate_files(stage_root: Path) -> list[Path]:
    _reject_reparse_path(stage_root, label="Atlas stage directory")
    if not stage_root.is_dir():
        raise CapsuleError(f"missing required Atlas stage directory: {stage_root}")
    selected: list[Path] = []
    for child in sorted(stage_root.iterdir(), key=lambda item: item.name):
        _reject_reparse_path(child, label="Atlas stage item")
        if child.is_file() and child.suffix.lower() in AGGREGATE_SUFFIXES:
            selected.append(child)
    if not selected:
        raise CapsuleError(f"missing published aggregate tables in stage: {stage_root}")
    return selected


def _stage_entries(root: Path, cutoff: str) -> list[CapsuleEntry]:
    entries: list[CapsuleEntry] = []
    for filename in ROOT_AUTHORITY_FILES:
        source = root / filename
        entries.append(CapsuleEntry(source, f"atlas-roots/{cutoff}/{filename}"))
    events = root / "atlas-lab-events.jsonl"
    if events.exists():
        _reject_reparse_path(events, label="Atlas event journal")
        if not events.is_file():
            raise CapsuleError(f"Atlas event journal is not a file: {events}")
        entries.append(CapsuleEntry(events, f"atlas-roots/{cutoff}/{events.name}"))
    for stage_name in STAGE_DIRECTORIES:
        for source in _direct_aggregate_files(root / stage_name):
            entries.append(CapsuleEntry(source, f"atlas-roots/{cutoff}/{stage_name}/{source.name}"))
    return entries


def discover_phase2_atlas_roots(atlas_roots: Iterable[Path], *, atlas_runs_root: Path) -> dict[str, Path]:
    """Validate exactly the four explicit, completed A-D roots and label them by authority."""
    configured_root = _resolve_existing(atlas_runs_root, label="configured Atlas root")
    provided = list(atlas_roots)
    if len(provided) != 4:
        raise CapsuleError("exactly four explicit --atlas-root paths are required")
    discovered: dict[str, Path] = {}
    for raw_root in provided:
        root = _require_under(raw_root, configured_root, label="Atlas root")
        cutoff = _validate_completed_root(root, configured_root)
        if cutoff not in EXPECTED_CUTOFFS:
            raise CapsuleError(f"unexpected Atlas cutoff authority {cutoff!r}: {root}")
        if cutoff in discovered:
            raise CapsuleError(f"duplicate Atlas cutoff authority {cutoff}: {root}")
        discovered[cutoff] = root
    if frozenset(discovered) != EXPECTED_CUTOFFS:
        raise CapsuleError("explicit Atlas roots must contain completed cutoff authorities A, B, C, and D")
    return dict(sorted(discovered.items()))


def _plan_entries(
    *, repo_root: Path, atlas_roots: Iterable[Path], atlas_runs_root: Path, forensic_root: Path, control_root: Path
) -> tuple[list[CapsuleEntry], dict[str, Path]]:
    roots = discover_phase2_atlas_roots(atlas_roots, atlas_runs_root=atlas_runs_root)
    entries: list[CapsuleEntry] = []
    for cutoff, root in roots.items():
        entries.extend(_stage_entries(root, cutoff))
    control_root = _require_under(control_root, repo_root, label="Level C control root")
    for filename in CONTROL_REQUIRED_FILES:
        _require_under(control_root / filename, control_root, label=f"required control authority {filename}")
    for cutoff in sorted(roots):
        filename = f"execution-plan-{cutoff}.json"
        _require_under(control_root / filename, control_root, label=f"required execution authority {filename}")
    for source in sorted(control_root.iterdir(), key=lambda item: item.name):
        _reject_reparse_path(source, label="Level C control item")
        if source.is_file() and source.suffix.lower() == ".json":
            entries.append(CapsuleEntry(source, f"level-c-control/{source.name}"))
    forensic_root = _require_under(forensic_root, repo_root, label="forensic reports root")
    if not forensic_root.is_dir():
        raise CapsuleError(f"forensic reports root is not a directory: {forensic_root}")
    report_names: set[str] = set()
    for source in sorted(forensic_root.rglob("*"), key=lambda item: item.as_posix()):
        _reject_reparse_path(source, label="forensic report item")
        if source.is_file() and source.suffix.lower() in AGGREGATE_SUFFIXES:
            entries.append(CapsuleEntry(source, f"forensics/{source.relative_to(forensic_root).as_posix()}"))
            report_names.add(source.name)
    missing_reports = sorted(set(FORENSIC_REPORT_FILENAMES) - report_names)
    if missing_reports:
        raise CapsuleError(f"missing required forensic report(s): {', '.join(missing_reports)}")
    master_plan = _require_under(repo_root / MASTER_PLAN_RELATIVE_PATH, repo_root, label="master plan")
    entries.append(CapsuleEntry(master_plan, MASTER_PLAN_RELATIVE_PATH.as_posix()))
    return sorted(entries, key=lambda entry: entry.capsule_relative_path), roots


def _manifest_for(entries: list[CapsuleEntry], *, repo_root: Path, roots: dict[str, Path]) -> dict[str, Any]:
    if len({entry.capsule_relative_path for entry in entries}) != len(entries):
        raise CapsuleError("capsule selection has a destination-path collision")
    files = [
        {
            "capsule_relative_path": entry.capsule_relative_path,
            "sha256": _sha256(entry.source),
            "size_bytes": entry.source.stat().st_size,
            "source_relative_path": _relative_to_repo(entry.source, repo_root),
        }
        for entry in entries
    ]
    payload: dict[str, Any] = {
        "schema_version": "phase2_atlas_authority_capsule_v1",
        "cutoffs": [
            {"cutoff": cutoff, "source_relative_root": _relative_to_repo(root, repo_root)}
            for cutoff, root in sorted(roots.items())
        ],
        "files": files,
        "manifest_excludes_self": True,
    }
    payload["capsule_identity_sha256"] = hashlib.sha256(_canonical_json(payload)).hexdigest()
    return payload


def create_capsule_plan(
    *, repo_root: Path, atlas_roots: Iterable[Path], atlas_runs_root: Path | None = None,
    forensic_root: Path | None = None, control_root: Path | None = None,
) -> dict[str, Any]:
    repo = _resolve_existing(repo_root, label="repository root")
    if not repo.is_dir():
        raise CapsuleError(f"repository root is not a directory: {repo}")
    entries, roots = _plan_entries(
        repo_root=repo,
        atlas_roots=atlas_roots,
        atlas_runs_root=atlas_runs_root or repo / "runs/derived/atlas-runs",
        forensic_root=forensic_root or repo / "runs/derived/phase2-atlas-forensics",
        control_root=control_root or repo / "runs/derived/level-c/control",
    )
    return _manifest_for(entries, repo_root=repo, roots=roots)


def _validate_new_destination(destination: Path, capsule_root: Path) -> tuple[Path, Path]:
    candidate, root = _validate_existing_ancestors(capsule_root, destination, label="capsule destination")
    if candidate.exists():
        raise CapsuleError(f"capsule destination collision: {candidate}")
    parent = candidate.parent
    if not parent.exists():
        raise CapsuleError(f"capsule destination parent does not exist: {parent}")
    _validate_existing_ancestors(root, parent, label="capsule destination parent")
    return candidate, root


def _create_verified_temp_directory(destination: Path, capsule_root: Path) -> _VerifiedTempDirectory:
    temp = destination.parent / f".{destination.name}.phase2-capsule-tmp"
    _validate_existing_ancestors(capsule_root, temp, label="temporary capsule destination")
    if temp.exists() or temp.is_symlink():
        raise CapsuleError(f"temporary capsule destination collision: {temp}")
    try:
        temp.mkdir()
    except FileExistsError as exc:
        raise CapsuleError(f"temporary capsule destination collision: {temp}") from exc
    _validate_existing_ancestors(capsule_root, temp, label="temporary capsule destination")
    _reject_reparse_path(temp, label="temporary capsule destination")
    stat_result = temp.lstat()
    if not temp.is_dir():
        raise CapsuleError(f"temporary capsule destination is not an ordinary directory: {temp}")
    return _VerifiedTempDirectory(path=temp, device=stat_result.st_dev, inode=stat_result.st_ino)


def _verify_temp_directory(temp: _VerifiedTempDirectory, capsule_root: Path) -> None:
    _validate_existing_ancestors(capsule_root, temp.path, label="temporary capsule destination")
    _reject_reparse_path(temp.path, label="temporary capsule destination")
    current = temp.path.lstat()
    if not temp.path.is_dir() or (current.st_dev, current.st_ino) != (temp.device, temp.inode):
        raise CapsuleError(f"temporary capsule destination changed after creation: {temp.path}")


def _discard_empty_verified_temp_directory(temp: _VerifiedTempDirectory, capsule_root: Path) -> None:
    """Never recursively remove a failed build tree; preserve it for manual inspection."""
    if not temp.path.exists() and not temp.path.is_symlink():
        return
    _verify_temp_directory(temp, capsule_root)
    try:
        temp.path.rmdir()
    except OSError:
        pass


def _write_capsule(
    entries: list[CapsuleEntry], manifest: dict[str, Any], destination: Path, capsule_root: Path
) -> None:
    temp = _create_verified_temp_directory(destination, capsule_root)
    try:
        for entry in entries:
            _verify_temp_directory(temp, capsule_root)
            target = temp.path / Path(entry.capsule_relative_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            _validate_existing_ancestors(temp.path, target.parent, label="temporary capsule content")
            if target.exists() or target.is_symlink():
                raise CapsuleError(f"temporary capsule file collision: {target}")
            shutil.copyfile(entry.source, target)
        _verify_temp_directory(temp, capsule_root)
        (temp.path / CAPSULE_MANIFEST_NAME).write_bytes(_canonical_json(manifest))
        _verify_temp_directory(temp, capsule_root)
        _validate_new_destination(destination, capsule_root)
        temp.path.rename(destination)
    except Exception:
        _discard_empty_verified_temp_directory(temp, capsule_root)
        raise


def build_capsule(
    *, repo_root: Path, atlas_roots: Iterable[Path], destination: Path, capsule_root: Path,
    atlas_runs_root: Path | None = None, forensic_root: Path | None = None, control_root: Path | None = None,
) -> dict[str, Any]:
    repo = _resolve_existing(repo_root, label="repository root")
    destination, configured_capsule_root = _validate_new_destination(destination, capsule_root)
    entries, roots = _plan_entries(
        repo_root=repo, atlas_roots=atlas_roots,
        atlas_runs_root=atlas_runs_root or repo / "runs/derived/atlas-runs",
        forensic_root=forensic_root or repo / "runs/derived/phase2-atlas-forensics",
        control_root=control_root or repo / "runs/derived/level-c/control",
    )
    manifest = _manifest_for(entries, repo_root=repo, roots=roots)
    _write_capsule(entries, manifest, destination, configured_capsule_root)
    verification = verify_capsule(destination)
    return {"destination": str(destination), "manifest": manifest, "verification": verification}


def _read_manifest(capsule: Path) -> dict[str, Any]:
    capsule = _resolve_existing(capsule, label="capsule")
    if not capsule.is_dir():
        raise CapsuleError(f"capsule is not a directory: {capsule}")
    manifest = _load_json(capsule / CAPSULE_MANIFEST_NAME, label="capsule manifest")
    if manifest.get("schema_version") != "phase2_atlas_authority_capsule_v1":
        raise CapsuleError(f"unsupported capsule manifest: {capsule}")
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise CapsuleError(f"capsule manifest has no files: {capsule}")
    identity_payload = dict(manifest)
    identity = identity_payload.pop("capsule_identity_sha256", None)
    if not isinstance(identity, str) or hashlib.sha256(_canonical_json(identity_payload)).hexdigest() != identity:
        raise CapsuleError(f"capsule manifest identity does not match: {capsule}")
    return manifest


def verify_capsule(capsule: Path) -> dict[str, Any]:
    capsule = _resolve_existing(capsule, label="capsule")
    manifest = _read_manifest(capsule)
    expected: set[str] = set()
    total_size = 0
    for row in manifest["files"]:
        if not isinstance(row, dict):
            raise CapsuleError(f"invalid capsule manifest file entry: {capsule}")
        relative = row.get("capsule_relative_path")
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise CapsuleError(f"unsafe capsule manifest path: {relative!r}")
        if relative in expected:
            raise CapsuleError(f"duplicate capsule manifest path: {relative}")
        expected.add(relative)
        path = capsule / relative
        _require_under(path, capsule, label="capsule file")
        if not path.is_file() or path.stat().st_size != row.get("size_bytes") or _sha256(path) != row.get("sha256"):
            raise CapsuleError(f"capsule file verification failed: {relative}")
        total_size += path.stat().st_size
    actual: set[str] = set()
    actual_directories: set[str] = set()
    for path in capsule.rglob("*"):
        _reject_reparse_path(path, label="capsule content")
        if path.is_file():
            actual.add(path.relative_to(capsule).as_posix())
        elif path.is_dir():
            actual_directories.add(path.relative_to(capsule).as_posix())
    unexpected = actual - expected - {CAPSULE_MANIFEST_NAME}
    missing = expected - actual
    expected_directories = {
        parent.as_posix()
        for relative in expected
        for parent in Path(relative).parents
        if parent != Path(".")
    }
    unexpected_directories = actual_directories - expected_directories
    if unexpected or missing or unexpected_directories:
        raise CapsuleError(
            "capsule content differs from manifest; "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}, "
            f"unexpected_directories={sorted(unexpected_directories)}"
        )
    return {"capsule": str(capsule), "file_count": len(expected), "size_bytes": total_size, "verified": True}


def _require_manifest_matches_roots(manifest: dict[str, Any], *, repo_root: Path, roots: dict[str, Path]) -> None:
    recorded = manifest.get("cutoffs")
    if not isinstance(recorded, list):
        raise CapsuleError("capsule manifest has no cutoff authority map")
    expected = [
        {"cutoff": cutoff, "source_relative_root": _relative_to_repo(root, repo_root)}
        for cutoff, root in sorted(roots.items())
    ]
    if recorded != expected:
        raise CapsuleError("capsule cutoff authority does not match the requested raw roots")


def _validate_retained_sources_against_manifest(manifest: dict[str, Any], *, repo_root: Path) -> None:
    """Prove the compact capsule still represents every retained source before cleanup review."""
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise CapsuleError("capsule manifest has no retained source files")
    for row in files:
        if not isinstance(row, dict):
            raise CapsuleError("capsule manifest has an invalid retained source entry")
        source_relative = row.get("source_relative_path")
        if (
            not isinstance(source_relative, str)
            or not source_relative
            or Path(source_relative).is_absolute()
            or ".." in Path(source_relative).parts
        ):
            raise CapsuleError(f"unsafe retained source path: {source_relative!r}")
        source = repo_root / source_relative
        _require_under(source, repo_root, label="retained source file")
        if not source.is_file() or source.stat().st_size != row.get("size_bytes") or _sha256(source) != row.get("sha256"):
            raise CapsuleError(f"retained source file verification failed: {source_relative}")


def copy_capsule(*, capsule: Path, destination: Path, archive_root: Path) -> dict[str, Any]:
    source = _resolve_existing(capsule, label="source capsule")
    verify_capsule(source)
    destination, configured_archive_root = _validate_new_destination(destination, archive_root)
    manifest = _read_manifest(source)
    entries = [CapsuleEntry(source / row["capsule_relative_path"], row["capsule_relative_path"]) for row in manifest["files"]]
    _write_capsule(entries, manifest, destination, configured_archive_root)
    verification = verify_capsule(destination)
    return {"source": str(source), "destination": str(destination), "verification": verification}


def cleanup_preview(
    *, repo_root: Path, atlas_roots: Iterable[Path], capsule: Path, capsule_root: Path,
    archive_capsule: Path | None = None, archive_root: Path | None = None,
    atlas_runs_root: Path | None = None,
) -> dict[str, Any]:
    repo = _resolve_existing(repo_root, label="repository root")
    local_root = _resolve_existing(capsule_root, label="configured capsule root")
    local_capsule = _require_under(capsule, local_root, label="local capsule")
    local_verification = verify_capsule(local_capsule)
    archive_verification: dict[str, Any] | None = None
    if archive_capsule is not None or archive_root is not None:
        if archive_capsule is None or archive_root is None:
            raise CapsuleError("--archive-capsule and --archive-root must be supplied together")
        configured_archive_root = _resolve_existing(archive_root, label="configured archive root")
        archived = _require_under(archive_capsule, configured_archive_root, label="archive capsule")
        archive_verification = verify_capsule(archived)
        if (local_capsule / CAPSULE_MANIFEST_NAME).read_bytes() != (archived / CAPSULE_MANIFEST_NAME).read_bytes():
            raise CapsuleError("archive capsule manifest does not exactly match the local capsule")
    roots = discover_phase2_atlas_roots(
        atlas_roots, atlas_runs_root=atlas_runs_root or repo / "runs/derived/atlas-runs"
    )
    manifest = _read_manifest(local_capsule)
    _require_manifest_matches_roots(manifest, repo_root=repo, roots=roots)
    _validate_retained_sources_against_manifest(manifest, repo_root=repo)
    candidates: list[dict[str, str]] = []
    for cutoff, root in roots.items():
        for item in sorted(root.iterdir(), key=lambda child: child.name):
            _reject_reparse_path(item, label="cleanup candidate")
            candidates.append({
                "cutoff": cutoff,
                "kind": "directory" if item.is_dir() else "file",
                "raw_path": _relative_to_repo(item, repo),
            })
    return {
        "command": "phase2-atlas-capsule cleanup-preview",
        "delete_operation_available": False,
        "local_capsule_verification": local_verification,
        "archive_capsule_verification": archive_verification,
        "candidate_removals": candidates,
        "ready_for_human_review_only": True,
    }


def _path(value: str) -> Path:
    return Path(value)


def add_phase2_atlas_capsule_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "phase2-atlas-capsule",
        help="Build, verify, copy, or safely preview retention of a Phase 2 Atlas authority capsule.",
    )
    parser.add_argument("--mode", required=True, choices=("dry-run", "build", "copy", "verify", "cleanup-preview"))
    parser.add_argument("--repo-root", type=_path, default=Path.cwd())
    parser.add_argument("--atlas-root", type=_path, action="append", default=[])
    parser.add_argument("--atlas-runs-root", type=_path, default=None)
    parser.add_argument("--capsule-root", type=_path, default=None)
    parser.add_argument("--destination", type=_path, default=None)
    parser.add_argument("--capsule", type=_path, default=None)
    parser.add_argument("--archive-root", type=_path, default=None)
    parser.add_argument("--archive-capsule", type=_path, default=None)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")


def cmd_phase2_atlas_capsule(args: argparse.Namespace) -> int:
    repo = args.repo_root
    capsule_root = args.capsule_root or repo / "runs/derived/phase2-atlas-capsules"
    atlas_runs_root = args.atlas_runs_root or repo / "runs/derived/atlas-runs"
    try:
        if args.mode == "dry-run":
            if args.destination is None:
                raise CapsuleError("--destination is required for dry-run")
            _validate_new_destination(args.destination, capsule_root)
            manifest = create_capsule_plan(repo_root=repo, atlas_roots=args.atlas_root, atlas_runs_root=atlas_runs_root)
            payload: dict[str, Any] = {"dry_run": True, "ready": True, "destination": str(args.destination.absolute()), "manifest": manifest}
        elif args.mode == "build":
            if args.destination is None:
                raise CapsuleError("--destination is required for build")
            payload = {"dry_run": False, "ready": True, **build_capsule(repo_root=repo, atlas_roots=args.atlas_root, destination=args.destination, capsule_root=capsule_root, atlas_runs_root=atlas_runs_root)}
        elif args.mode == "copy":
            if args.capsule is None or args.destination is None or args.archive_root is None:
                raise CapsuleError("copy requires --capsule, --destination, and --archive-root")
            payload = {"dry_run": False, "ready": True, **copy_capsule(capsule=args.capsule, destination=args.destination, archive_root=args.archive_root)}
        elif args.mode == "verify":
            if args.capsule is None:
                raise CapsuleError("verify requires --capsule")
            configured_root = args.archive_root or capsule_root
            checked = _require_under(args.capsule, _resolve_existing(configured_root, label="configured capsule root"), label="capsule")
            payload = {"dry_run": True, "ready": True, **verify_capsule(checked)}
        else:
            if args.capsule is None:
                raise CapsuleError("cleanup-preview requires --capsule")
            payload = cleanup_preview(repo_root=repo, atlas_roots=args.atlas_root, capsule=args.capsule, capsule_root=capsule_root, archive_capsule=args.archive_capsule, archive_root=args.archive_root, atlas_runs_root=atlas_runs_root)
            payload["dry_run"] = True
            payload["ready"] = True
    except CapsuleError as exc:
        payload = {"dry_run": args.mode in {"dry-run", "verify", "cleanup-preview"}, "ready": False, "error": str(exc)}
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=True, indent=None if args.json else 2, sort_keys=True))
    return 0
