"""Compact, sealed proposal-state journal for a future Temporal QD cutover.

This module is deliberately not wired into the live Temporal QD generator yet.
It is a small, Python-only reference implementation for the representation
described by the optimization plan:

* a proposal attempt is a compact content reference plus a small delta and
  explicit legacy provenance/accounting fields;
* callers append a *batch* as one immutable, hash-chained segment;
* periodic immutable checkpoints capture the state needed to continue;
* resume verifies the latest checkpoint anchor and replays only later segments.

The existing rich proposal journal remains the production authority.  The
``replay_legacy_full_history`` helper is intentionally retained as a simple
full-history oracle for parity and bounded performance measurements.
"""

from __future__ import annotations

import gc
import json
import os
import re
import stat
import tempfile
import time
import tracemalloc
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .result_codec import fsync_directory
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256

SEALED_PROPOSAL_JOURNAL_BINDING_SCHEMA = "temporal_qd_sealed_proposal_journal_binding_v2"
SEALED_PROPOSAL_SEGMENT_SCHEMA = "temporal_qd_sealed_proposal_segment_v2"
SEALED_PROPOSAL_ENTRY_SCHEMA = "temporal_qd_sealed_proposal_delta_v2"
SEALED_PROPOSAL_CHECKPOINT_SCHEMA = "temporal_qd_sealed_proposal_checkpoint_v2"
SEALED_PROPOSAL_STATE_SCHEMA = "temporal_qd_sealed_proposal_state_v2"
SEALED_PROPOSAL_REPLAY_MEASUREMENT_SCHEMA = "temporal_qd_sealed_proposal_replay_measurement_v2"

_BINDING_FILENAME = "sealed-journal-binding.json"
_SEGMENT_DIRECTORY = "segments"
_CHECKPOINT_DIRECTORY = "checkpoints"
_TEMPORARY_SUFFIX = ".sealed-journal.tmp"
_SEGMENT_NAME = re.compile(r"^(?P<ordinal>[0-9]{12})\.segment\.json$")
_CHECKPOINT_NAME = re.compile(r"^(?P<count>[0-9]{12})\.checkpoint\.json$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_COUNTER_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")
_DISPOSITION = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_ORIGIN_KINDS = frozenset({"random_immigrant", "structural_offspring"})

# A compact entry must never accidentally become another multi-megabyte rich
# proposal journal.  The limit is intentionally generous for a mutation audit,
# while still two orders of magnitude below the observed rich-entry p99.
MAX_COMPACT_ENTRY_BYTES = 256 * 1024


class SealedProposalJournalError(TemporalDiscoveryContractError):
    """Raised when sealed proposal state is partial, malformed, or divergent."""


@dataclass(frozen=True)
class JournalHead:
    """The sealed segment/entry pair from which a resume continues."""

    proposal_count: int
    segment_ordinal: int | None
    segment_sha256: str | None
    entry_sha256: str | None

    def as_payload(self) -> dict[str, Any]:
        return {
            "proposalCount": self.proposal_count,
            "segmentOrdinal": self.segment_ordinal,
            "segmentSha256": self.segment_sha256,
            "entrySha256": self.entry_sha256,
        }


@dataclass(frozen=True)
class AcceptedProposalReference:
    """The compact retained state for one accepted proposal."""

    proposal_ordinal: int
    entry_sha256: str
    legacy_entry_sha256: str
    origin_kind: str
    disposition: str
    content_ref: Any
    candidate_identity_sha256: str
    semantic_identity_sha256: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "proposalOrdinal": self.proposal_ordinal,
            "entrySha256": self.entry_sha256,
            "legacyEntrySha256": self.legacy_entry_sha256,
            "originKind": self.origin_kind,
            "disposition": self.disposition,
            "contentRef": _clone_json(self.content_ref, name="accepted content reference"),
            "candidateIdentitySha256": self.candidate_identity_sha256,
            "semanticIdentitySha256": self.semantic_identity_sha256,
        }


@dataclass(frozen=True)
class BatchAppendResult:
    """Committed receipt for one coherent proposal-batch publication.

    Reaching this result means the immutable segment is durable authority;
    ``segment_committed`` is therefore always true.  A checkpoint is only a
    resumability accelerator.  If its best-effort automatic publication fails,
    the receipt records that separately instead of making a committed append
    look like a failed call.  Callers must not re-append the batch in that
    case: they may retry ``seal_checkpoint`` or simply reopen the journal.
    """

    segment_ordinal: int
    first_proposal_ordinal: int
    last_proposal_ordinal: int
    entry_sha256s: tuple[str, ...]
    segment_sha256: str
    segment_committed: bool
    checkpoint_attempted: bool
    checkpoint_written: bool
    checkpoint_error: str | None
    proposal_count: int
    accepted_count: int

    def as_payload(self) -> dict[str, Any]:
        return {
            "segmentOrdinal": self.segment_ordinal,
            "firstProposalOrdinal": self.first_proposal_ordinal,
            "lastProposalOrdinal": self.last_proposal_ordinal,
            "entrySha256s": list(self.entry_sha256s),
            "segmentSha256": self.segment_sha256,
            "segmentCommitted": self.segment_committed,
            "checkpointAttempted": self.checkpoint_attempted,
            "checkpointWritten": self.checkpoint_written,
            "checkpointError": self.checkpoint_error,
            "proposalCount": self.proposal_count,
            "acceptedCount": self.accepted_count,
        }


@dataclass
class _JournalState:
    proposal_count: int = 0
    accepted_refs: list[AcceptedProposalReference] = field(default_factory=list)
    candidate_identity_index: dict[str, AcceptedProposalReference] = field(
        default_factory=dict
    )
    semantic_identity_index: dict[str, AcceptedProposalReference] = field(
        default_factory=dict
    )
    scheduling_counters: dict[str, int] = field(default_factory=dict)
    origin_proposal_counts: dict[str, int] = field(default_factory=dict)
    origin_accepted_counts: dict[str, int] = field(default_factory=dict)
    disposition_counts: dict[str, int] = field(default_factory=dict)
    head: JournalHead = field(
        default_factory=lambda: JournalHead(
            proposal_count=0,
            segment_ordinal=None,
            segment_sha256=None,
            entry_sha256=None,
        )
    )
    last_checkpoint_proposal_count: int = 0

    def copy(self) -> _JournalState:
        accepted = [
            AcceptedProposalReference(
                proposal_ordinal=ref.proposal_ordinal,
                entry_sha256=ref.entry_sha256,
                legacy_entry_sha256=ref.legacy_entry_sha256,
                origin_kind=ref.origin_kind,
                disposition=ref.disposition,
                content_ref=_clone_json(ref.content_ref, name="accepted content reference"),
                candidate_identity_sha256=ref.candidate_identity_sha256,
                semantic_identity_sha256=ref.semantic_identity_sha256,
            )
            for ref in self.accepted_refs
        ]
        candidate_index = {
            ref.candidate_identity_sha256: ref for ref in accepted
        }
        semantic_index = {ref.semantic_identity_sha256: ref for ref in accepted}
        return _JournalState(
            proposal_count=self.proposal_count,
            accepted_refs=accepted,
            candidate_identity_index=candidate_index,
            semantic_identity_index=semantic_index,
            scheduling_counters=dict(self.scheduling_counters),
            origin_proposal_counts=dict(self.origin_proposal_counts),
            origin_accepted_counts=dict(self.origin_accepted_counts),
            disposition_counts=dict(self.disposition_counts),
            head=JournalHead(**self.head.__dict__),
            last_checkpoint_proposal_count=self.last_checkpoint_proposal_count,
        )


def _canonical_bytes(value: Any, *, name: str) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SealedProposalJournalError(f"{name} must be finite canonical JSON") from exc


def _clone_json(value: Any, *, name: str) -> Any:
    try:
        return json.loads(_canonical_bytes(value, name=name).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SealedProposalJournalError(f"{name} must be finite canonical JSON") from exc


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SealedProposalJournalError(f"{name} must be an object")
    cloned = _clone_json(dict(value), name=name)
    if not isinstance(cloned, dict):  # defensive; JSON object always decodes to dict.
        raise SealedProposalJournalError(f"{name} must be an object")
    return cloned


def _integer(value: Any, *, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = f" at least {minimum}" if minimum else ""
        raise SealedProposalJournalError(f"{name} must be an integer{qualifier}")
    return value


def _sha256(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise SealedProposalJournalError(f"{name} must be an exact sha256 identity")
    return value


def _bool(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise SealedProposalJournalError(f"{name} must be a boolean")
    return value


def _origin_kind(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or value not in _ORIGIN_KINDS:
        raise SealedProposalJournalError(
            f"{name} must be one of {sorted(_ORIGIN_KINDS)}"
        )
    return value


def _disposition(value: Any, *, name: str) -> str:
    """Validate a persisted outcome token without owning outcome policy.

    The live generator owns the full set of rejection reasons, including
    future ledger-derived reasons.  The sealed layer only requires a stable,
    canonical outcome token and enforces the one semantic rule it owns:
    ``accepted`` must agree with the identity-bearing accepted state.
    """

    if not isinstance(value, str) or not _DISPOSITION.fullmatch(value):
        raise SealedProposalJournalError(
            f"{name} must be a canonical snake-case disposition token"
        )
    return value


def _content_ref(value: Any, *, name: str) -> Any:
    """Normalize a content reference without allowing a rich candidate inline."""

    if isinstance(value, str):
        return {"contentSha256": _sha256(value, name=name)}
    reference = _mapping(value, name=name)
    # ``sha256`` is the canonical field of the companion ObjectRef type.  More
    # than one spelling is permitted only when each one names the *same*
    # immutable content.  Silent precedence would let an adapter accidentally
    # bind audit metadata to a different object than the one it hydrates.
    supplied_aliases = {
        key: _sha256(reference[key], name=f"{name} {key}")
        for key in ("contentSha256", "objectSha256", "sha256")
        if key in reference
    }
    if not supplied_aliases:
        raise SealedProposalJournalError(f"{name} lacks a content identity")
    identities = set(supplied_aliases.values())
    if len(identities) != 1:
        raise SealedProposalJournalError(
            f"{name} has conflicting content identity aliases"
        )
    supplied = next(iter(identities))
    # Preserve useful typed-store metadata, but require an actual immutable
    # content identity rather than treating an arbitrary proposal object as a
    # reference.
    if "contentSha256" not in reference:
        reference["contentSha256"] = supplied
    return reference


def _counter_delta(value: Any, *, name: str) -> dict[str, int]:
    if value is None:
        return {}
    raw = _mapping(value, name=name)
    result: dict[str, int] = {}
    for key, amount in raw.items():
        if not isinstance(key, str) or not _COUNTER_NAME.fullmatch(key):
            raise SealedProposalJournalError(
                f"{name} keys must be safe scheduling-counter identifiers"
            )
        result[key] = _integer(amount, name=f"{name}.{key}")
    return dict(sorted(result.items()))


def _apply_counter_delta(
    counters: dict[str, int], delta: Mapping[str, int]
) -> dict[str, int]:
    result = dict(counters)
    for key, amount in delta.items():
        result[key] = result.get(key, 0) + amount
    return dict(sorted(result.items()))


def _identity_without(payload: Mapping[str, Any], field: str) -> str:
    material = dict(payload)
    supplied = _sha256(material.pop(field, None), name=field)
    expected = canonical_sha256(material)
    if supplied != expected:
        raise SealedProposalJournalError(f"sealed proposal journal {field} mismatch")
    return supplied


class _DuplicateObjectKey(ValueError):
    pass


def _no_duplicate_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateObjectKey(key)
        result[key] = value
    return result


def _is_link_or_reparse(status: os.stat_result) -> bool:
    """Reject POSIX symlinks and Windows symlink/junction reparse points."""

    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    return stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point)


def _optional_lstat(path: Path, *, name: str) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SealedProposalJournalError(f"could not inspect {name}: {path}") from exc


def _require_real_directory(path: Path, *, name: str) -> None:
    status = _optional_lstat(path, name=name)
    if status is None:
        raise SealedProposalJournalError(f"{name} is missing: {path}")
    if _is_link_or_reparse(status):
        raise SealedProposalJournalError(
            f"symlink or junction is forbidden for {name}: {path}"
        )
    if not stat.S_ISDIR(status.st_mode):
        raise SealedProposalJournalError(f"{name} is not a real directory: {path}")


def _lexical_absolute_path(path: Path | str) -> Path:
    """Make a lexical absolute path without resolving through a link."""

    return Path(os.path.abspath(os.fspath(path)))


def _ensure_real_directory_tree(path: Path, *, name: str) -> Path:
    """Create one directory component at a time without adopting a reparse path."""

    absolute = _lexical_absolute_path(path)
    anchor = Path(absolute.anchor)
    if not absolute.anchor:
        raise SealedProposalJournalError(f"{name} must be an absolute path")
    _require_real_directory(anchor, name=f"{name} filesystem anchor")
    current = anchor
    for component in absolute.parts[1:]:
        if not component or component in {".", ".."}:
            raise SealedProposalJournalError(f"{name} has an unsafe path component")
        candidate = current / component
        status = _optional_lstat(candidate, name=name)
        if status is None:
            try:
                os.mkdir(candidate)
            except FileExistsError:
                pass
            except OSError as exc:
                raise SealedProposalJournalError(
                    f"could not create {name} component: {candidate}"
                ) from exc
        _require_real_directory(candidate, name=name)
        current = candidate
    return absolute


def _relative_journal_path(root: Path, path: Path) -> tuple[str, ...]:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise SealedProposalJournalError("sealed journal path escapes its root") from exc
    if any(
        not component or component in {".", ".."} or "/" in component or "\\" in component
        for component in relative.parts
    ):
        raise SealedProposalJournalError("sealed journal path has an unsafe component")
    return relative.parts


def _assert_path_components_safe(
    root: Path, path: Path, *, final_kind: str, name: str
) -> None:
    """Refuse a root, intermediate, or final artifact that is a link/reparse point."""

    relative_parts = _relative_journal_path(root, path)
    _require_real_directory(root, name="sealed journal root")
    current = root
    if not relative_parts:
        if final_kind != "directory":
            raise SealedProposalJournalError("sealed journal root cannot be a file artifact")
        return
    for index, component in enumerate(relative_parts):
        current = current / component
        status = _optional_lstat(current, name=name)
        if status is None:
            raise SealedProposalJournalError(f"{name} is missing: {current}")
        if _is_link_or_reparse(status):
            raise SealedProposalJournalError(
                f"symlink or junction is forbidden inside sealed journal: {current}"
            )
        final = index == len(relative_parts) - 1
        if final:
            if final_kind == "directory" and not stat.S_ISDIR(status.st_mode):
                raise SealedProposalJournalError(f"{name} is not a real directory: {current}")
            if final_kind == "file" and not stat.S_ISREG(status.st_mode):
                raise SealedProposalJournalError(f"{name} is not a regular file: {current}")
        elif not stat.S_ISDIR(status.st_mode):
            raise SealedProposalJournalError(
                f"sealed journal path parent is not a real directory: {current}"
            )
    # Match the object-store no-follow guard: lstat every component above,
    # then ensure the kernel's resolved target remains below our verified
    # lexical root.  The descriptor-based read path adds a final no-follow
    # guard for the artifact itself.
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise SealedProposalJournalError(
            f"sealed journal path resolves outside its root: {path}"
        ) from exc


def _ensure_journal_directory(root: Path, path: Path, *, name: str) -> None:
    """Safely create a journal-local directory after checking each component."""

    relative_parts = _relative_journal_path(root, path)
    _require_real_directory(root, name="sealed journal root")
    current = root
    for component in relative_parts:
        candidate = current / component
        status = _optional_lstat(candidate, name=name)
        if status is None:
            try:
                os.mkdir(candidate)
            except FileExistsError:
                pass
            except OSError as exc:
                raise SealedProposalJournalError(
                    f"could not create sealed journal directory: {candidate}"
                ) from exc
        _assert_path_components_safe(root, candidate, final_kind="directory", name=name)
        current = candidate


def _read_regular_bytes(root: Path, path: Path, *, name: str) -> bytes:
    """Read one regular artifact through a no-follow descriptor and recheck it."""

    _assert_path_components_safe(root, path, final_kind="file", name=name)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise SealedProposalJournalError(f"could not safely open {name}: {path}") from exc
    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise SealedProposalJournalError(f"opened {name} is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    # Detect a root/intermediate/final swap that happened after the descriptor
    # was opened.  The bytes may have been safe, but no state is accepted from
    # a path whose current topology is no longer trusted.
    _assert_path_components_safe(root, path, final_kind="file", name=name)
    return raw


def _read_canonical_json(
    root: Path, path: Path, *, name: str
) -> tuple[dict[str, Any], int]:
    raw = _read_regular_bytes(root, path, name=name)
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(decoded, object_pairs_hook=_no_duplicate_object_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateObjectKey) as exc:
        raise SealedProposalJournalError(f"{name} is corrupt or partially written: {path}") from exc
    if not isinstance(value, dict):
        raise SealedProposalJournalError(f"{name} must be a JSON object: {path}")
    if raw != _canonical_bytes(value, name=name):
        raise SealedProposalJournalError(f"{name} is not canonical sealed JSON: {path}")
    return value, len(raw)


def _publish_immutable_json(
    root: Path, path: Path, payload: Mapping[str, Any], *, name: str
) -> None:
    """Publish complete canonical bytes once, refusing every divergent retry.

    A final path is created by hard-linking a fully fsynced temporary file.  It
    is never replaced.  A crash can leave only the private temporary name,
    which startup removes; a truncated final path is treated as corruption, not
    as a recoverable retry.
    """

    encoded = _canonical_bytes(dict(payload), name=name)
    _ensure_journal_directory(root, path.parent, name=f"sealed {name} directory")
    status = _optional_lstat(path, name=f"sealed {name}")
    if status is not None:
        _assert_path_components_safe(root, path, final_kind="file", name=f"sealed {name}")
        existing = _read_regular_bytes(root, path, name=f"sealed {name}")
        if existing != encoded:
            raise SealedProposalJournalError(
                f"refusing to overwrite divergent sealed {name}: {path}"
            )
        return

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=_TEMPORARY_SUFFIX, dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        _assert_path_components_safe(
            root, temporary, final_kind="file", name=f"sealed {name} temporary"
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        _assert_path_components_safe(
            root, temporary, final_kind="file", name=f"sealed {name} temporary"
        )
        try:
            _assert_path_components_safe(
                root, path.parent, final_kind="directory", name=f"sealed {name} directory"
            )
            os.link(temporary, path)
        except FileExistsError:
            _assert_path_components_safe(
                root, path, final_kind="file", name=f"concurrently sealed {name}"
            )
            existing = _read_regular_bytes(root, path, name=f"concurrently sealed {name}")
            if existing != encoded:
                raise SealedProposalJournalError(
                    f"refusing to overwrite divergent sealed {name}: {path}"
                )
        else:
            _assert_path_components_safe(
                root, path, final_kind="file", name=f"sealed {name}"
            )
            fsync_directory(path.parent)
        published = _read_regular_bytes(root, path, name=f"sealed {name}")
        if published != encoded:
            raise SealedProposalJournalError(
                f"refusing to accept divergent sealed {name}: {path}"
            )
    finally:
        temporary_status = _optional_lstat(temporary, name=f"sealed {name} temporary")
        if temporary_status is not None and (
            not _is_link_or_reparse(temporary_status)
            and stat.S_ISREG(temporary_status.st_mode)
        ):
            try:
                _assert_path_components_safe(
                    root,
                    temporary,
                    final_kind="file",
                    name=f"sealed {name} temporary",
                )
            except SealedProposalJournalError:
                # A path swap means it is safer to leave the private temp
                # for a later inspection than to unlink through it.
                pass
            else:
                temporary.unlink(missing_ok=True)


def _cleanup_private_temporaries(root: Path) -> None:
    """Discard only private, never-published failed-write artifacts."""

    for directory in (root, root / _SEGMENT_DIRECTORY, root / _CHECKPOINT_DIRECTORY):
        status = _optional_lstat(directory, name="sealed temporary directory")
        if status is None:
            continue
        _assert_path_components_safe(
            root, directory, final_kind="directory", name="sealed temporary directory"
        )
        with os.scandir(directory) as entries:
            candidates = list(entries)
        for entry in candidates:
            path = Path(entry.path)
            entry_status = entry.stat(follow_symlinks=False)
            if _is_link_or_reparse(entry_status):
                raise SealedProposalJournalError(
                    f"symlink or junction is forbidden inside sealed journal: {path}"
                )
            if not entry.name.endswith(_TEMPORARY_SUFFIX):
                continue
            # The suffix is exclusively emitted by _publish_immutable_json and
            # no published artifact can carry it.
            if stat.S_ISREG(entry_status.st_mode):
                _assert_path_components_safe(
                    root, path, final_kind="file", name="sealed private temporary"
                )
                path.unlink(missing_ok=True)
            else:
                raise SealedProposalJournalError(
                    f"sealed private temporary is not a regular file: {path}"
                )


def _segment_path(root: Path, ordinal: int) -> Path:
    return root / _SEGMENT_DIRECTORY / f"{ordinal:012d}.segment.json"


def _checkpoint_path(root: Path, proposal_count: int) -> Path:
    return root / _CHECKPOINT_DIRECTORY / f"{proposal_count:012d}.checkpoint.json"


def _owned_paths(
    root: Path, directory: Path, *, pattern: re.Pattern[str], kind: str
) -> list[tuple[int, Path]]:
    status = _optional_lstat(directory, name=f"sealed {kind} directory")
    if status is None:
        return []
    _assert_path_components_safe(
        root, directory, final_kind="directory", name=f"sealed {kind} directory"
    )
    discovered: list[tuple[int, Path]] = []
    with os.scandir(directory) as entries:
        candidates = list(entries)
    for entry in candidates:
        path = Path(entry.path)
        entry_status = entry.stat(follow_symlinks=False)
        if _is_link_or_reparse(entry_status):
            raise SealedProposalJournalError(
                f"symlink or junction is forbidden inside sealed journal: {path}"
            )
        if entry.name.endswith(_TEMPORARY_SUFFIX):
            # Startup should already have removed these.  Seeing one now means a
            # concurrent writer is in-flight; do not interpret it as authority.
            continue
        match = pattern.fullmatch(entry.name)
        if not stat.S_ISREG(entry_status.st_mode) or match is None:
            raise SealedProposalJournalError(
                f"sealed {kind} directory contains an unexpected artifact: {entry.name}"
            )
        _assert_path_components_safe(root, path, final_kind="file", name=f"sealed {kind}")
        identifier_text = (
            match.group("ordinal")
            if "ordinal" in match.re.groupindex
            else match.group("count")
        )
        assert identifier_text is not None
        identifier = int(identifier_text)
        discovered.append((identifier, path))
    return sorted(discovered)


def _segment_paths(root: Path) -> list[Path]:
    rows = _owned_paths(
        root, root / _SEGMENT_DIRECTORY, pattern=_SEGMENT_NAME, kind="segment"
    )
    for expected, (ordinal, path) in enumerate(rows):
        if ordinal != expected:
            raise SealedProposalJournalError(
                f"sealed proposal segments are reordered, duplicated, or gapped at {path.name}"
            )
    return [path for _, path in rows]


def _checkpoint_paths(root: Path) -> list[tuple[int, Path]]:
    rows = _owned_paths(
        root, root / _CHECKPOINT_DIRECTORY, pattern=_CHECKPOINT_NAME, kind="checkpoint"
    )
    if len({count for count, _ in rows}) != len(rows):
        raise SealedProposalJournalError("sealed proposal checkpoints contain duplicate counts")
    return rows


def _binding_payload(
    *, authority: Mapping[str, Any], config: Mapping[str, Any], checkpoint_interval_entries: int
) -> dict[str, Any]:
    authority_snapshot = _mapping(authority, name="sealed journal authority")
    config_snapshot = _mapping(config, name="sealed journal config")
    interval = _integer(
        checkpoint_interval_entries,
        name="sealed journal checkpoint interval",
        minimum=1,
    )
    payload = {
        "schemaVersion": SEALED_PROPOSAL_JOURNAL_BINDING_SCHEMA,
        "authority": authority_snapshot,
        "authoritySha256": canonical_sha256(authority_snapshot),
        "config": config_snapshot,
        "configSha256": canonical_sha256(config_snapshot),
        "checkpointIntervalEntries": interval,
    }
    payload["journalBindingSha256"] = canonical_sha256(payload)
    return payload


def _open_or_create_binding(
    *, root: Path, expected: Mapping[str, Any]
) -> tuple[dict[str, Any], int]:
    path = root / _BINDING_FILENAME
    if _optional_lstat(path, name="sealed journal binding") is None:
        # A root with authored state but no binding has no trustworthy authority
        # anchor.  Never adopt it as a newly initialized journal.
        if _segment_paths(root) or _checkpoint_paths(root):
            raise SealedProposalJournalError(
                "sealed proposal journal has state but lacks its immutable authority/config binding"
            )
        _publish_immutable_json(root, path, expected, name="journal binding")
        return dict(expected), len(_canonical_bytes(expected, name="journal binding"))
    actual, bytes_read = _read_canonical_json(
        root, path, name="sealed journal binding"
    )
    if _identity_without(actual, "journalBindingSha256") != expected["journalBindingSha256"]:
        raise SealedProposalJournalError(
            "sealed proposal journal authority/config binding mismatch"
        )
    if actual != dict(expected):
        raise SealedProposalJournalError(
            "sealed proposal journal authority/config binding diverged"
        )
    return actual, bytes_read


def _normalise_append_entry(
    value: Mapping[str, Any], *, proposal_ordinal: int, previous_entry_sha256: str | None
) -> dict[str, Any]:
    requested = _mapping(value, name="sealed proposal batch entry")
    allowed = {
        "contentRef",
        "delta",
        "originKind",
        "disposition",
        "legacyEntrySha256",
        "accepted",
        "candidateIdentitySha256",
        "semanticIdentitySha256",
        "schedulingDelta",
    }
    unexpected = set(requested).difference(allowed)
    if unexpected:
        raise SealedProposalJournalError(
            f"sealed proposal batch entry has unknown fields: {sorted(unexpected)}"
        )
    missing = {
        "contentRef",
        "delta",
        "originKind",
        "disposition",
        "legacyEntrySha256",
        "accepted",
    }.difference(requested)
    if missing:
        raise SealedProposalJournalError(
            f"sealed proposal batch entry lacks required fields: {sorted(missing)}"
        )
    accepted = _bool(requested["accepted"], name="sealed proposal accepted")
    origin_kind = _origin_kind(requested["originKind"], name="sealed proposal originKind")
    disposition = _disposition(
        requested["disposition"], name="sealed proposal disposition"
    )
    if accepted != (disposition == "accepted"):
        raise SealedProposalJournalError(
            "sealed proposal accepted state and disposition disagree"
        )
    content_ref = _content_ref(requested["contentRef"], name="sealed proposal contentRef")
    delta = _mapping(requested["delta"], name="sealed proposal delta")
    entry: dict[str, Any] = {
        "schemaVersion": SEALED_PROPOSAL_ENTRY_SCHEMA,
        "proposalOrdinal": proposal_ordinal,
        "previousEntrySha256": previous_entry_sha256,
        "legacyEntrySha256": _sha256(
            requested["legacyEntrySha256"], name="sealed proposal legacy entry identity"
        ),
        "originKind": origin_kind,
        "disposition": disposition,
        "contentRef": content_ref,
        "delta": delta,
        "accepted": accepted,
        "schedulingDelta": _counter_delta(
            requested.get("schedulingDelta"), name="sealed proposal schedulingDelta"
        ),
    }
    if accepted:
        entry["candidateIdentitySha256"] = _sha256(
            requested.get("candidateIdentitySha256"),
            name="sealed accepted candidate identity",
        )
        entry["semanticIdentitySha256"] = _sha256(
            requested.get("semanticIdentitySha256"),
            name="sealed accepted semantic identity",
        )
    elif (
        "candidateIdentitySha256" in requested
        or "semanticIdentitySha256" in requested
    ):
        raise SealedProposalJournalError(
            "rejected sealed proposal entries cannot claim accepted identity indexes"
        )
    if len(_canonical_bytes(entry, name="sealed proposal entry")) > MAX_COMPACT_ENTRY_BYTES:
        raise SealedProposalJournalError(
            "sealed proposal entry exceeds the compact-entry byte ceiling"
        )
    entry["entrySha256"] = canonical_sha256(entry)
    return entry


def _validate_entry_shape(entry: Mapping[str, Any]) -> None:
    accepted = _bool(entry.get("accepted"), name="sealed proposal entry accepted")
    expected = {
        "schemaVersion",
        "proposalOrdinal",
        "previousEntrySha256",
        "legacyEntrySha256",
        "originKind",
        "disposition",
        "contentRef",
        "delta",
        "accepted",
        "schedulingDelta",
        "entrySha256",
    }
    if accepted:
        expected.update({"candidateIdentitySha256", "semanticIdentitySha256"})
    if set(entry) != expected:
        raise SealedProposalJournalError("sealed proposal entry fields are not exact")
    if entry.get("schemaVersion") != SEALED_PROPOSAL_ENTRY_SCHEMA:
        raise SealedProposalJournalError("sealed proposal entry schema is unsupported")
    _integer(entry.get("proposalOrdinal"), name="sealed proposal ordinal")
    previous = entry.get("previousEntrySha256")
    if previous is not None:
        _sha256(previous, name="sealed proposal previous entry identity")
    _sha256(entry.get("legacyEntrySha256"), name="sealed proposal legacy entry identity")
    _origin_kind(entry.get("originKind"), name="sealed proposal originKind")
    disposition = _disposition(
        entry.get("disposition"), name="sealed proposal disposition"
    )
    if accepted != (disposition == "accepted"):
        raise SealedProposalJournalError(
            "sealed proposal accepted state and disposition disagree"
        )
    _content_ref(entry.get("contentRef"), name="sealed proposal contentRef")
    _mapping(entry.get("delta"), name="sealed proposal delta")
    _counter_delta(entry.get("schedulingDelta"), name="sealed proposal schedulingDelta")
    if accepted:
        _sha256(
            entry.get("candidateIdentitySha256"),
            name="sealed accepted candidate identity",
        )
        _sha256(
            entry.get("semanticIdentitySha256"),
            name="sealed accepted semantic identity",
        )
    if len(_canonical_bytes(dict(entry), name="sealed proposal entry")) > MAX_COMPACT_ENTRY_BYTES:
        raise SealedProposalJournalError(
            "sealed proposal entry exceeds the compact-entry byte ceiling"
        )
    _identity_without(entry, "entrySha256")


def _apply_verified_entry(state: _JournalState, entry: Mapping[str, Any]) -> None:
    _validate_entry_shape(entry)
    ordinal = _integer(entry["proposalOrdinal"], name="sealed proposal ordinal")
    if ordinal != state.proposal_count:
        raise SealedProposalJournalError(
            "sealed proposal entry ordinal is reordered, duplicated, or gapped"
        )
    if entry["previousEntrySha256"] != state.head.entry_sha256:
        raise SealedProposalJournalError("sealed proposal entry hash chain diverged")
    origin_kind = _origin_kind(entry["originKind"], name="sealed proposal originKind")
    disposition = _disposition(
        entry["disposition"], name="sealed proposal disposition"
    )
    state.scheduling_counters = _apply_counter_delta(
        state.scheduling_counters,
        _counter_delta(entry["schedulingDelta"], name="sealed proposal schedulingDelta"),
    )
    state.origin_proposal_counts[origin_kind] = (
        state.origin_proposal_counts.get(origin_kind, 0) + 1
    )
    state.disposition_counts[disposition] = (
        state.disposition_counts.get(disposition, 0) + 1
    )
    if entry["accepted"]:
        candidate_identity = _sha256(
            entry["candidateIdentitySha256"], name="sealed accepted candidate identity"
        )
        semantic_identity = _sha256(
            entry["semanticIdentitySha256"], name="sealed accepted semantic identity"
        )
        if candidate_identity in state.candidate_identity_index:
            raise SealedProposalJournalError(
                "sealed proposal journal has a duplicate accepted candidate identity"
            )
        if semantic_identity in state.semantic_identity_index:
            raise SealedProposalJournalError(
                "sealed proposal journal has a duplicate accepted semantic identity"
            )
        reference = AcceptedProposalReference(
            proposal_ordinal=ordinal,
            entry_sha256=_sha256(entry["entrySha256"], name="sealed proposal entry identity"),
            legacy_entry_sha256=_sha256(
                entry["legacyEntrySha256"], name="sealed proposal legacy entry identity"
            ),
            origin_kind=origin_kind,
            disposition=disposition,
            content_ref=_content_ref(entry["contentRef"], name="sealed proposal contentRef"),
            candidate_identity_sha256=candidate_identity,
            semantic_identity_sha256=semantic_identity,
        )
        state.accepted_refs.append(reference)
        state.candidate_identity_index[candidate_identity] = reference
        state.semantic_identity_index[semantic_identity] = reference
        state.origin_accepted_counts[origin_kind] = (
            state.origin_accepted_counts.get(origin_kind, 0) + 1
        )
    state.proposal_count += 1
    state.head = JournalHead(
        proposal_count=state.proposal_count,
        segment_ordinal=state.head.segment_ordinal,
        segment_sha256=state.head.segment_sha256,
        entry_sha256=_sha256(entry["entrySha256"], name="sealed proposal entry identity"),
    )


def _segment_payload(
    *, binding: Mapping[str, Any], state_before: _JournalState, entries: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], _JournalState]:
    if not entries:
        raise SealedProposalJournalError("sealed proposal batch cannot be empty")
    state_after = state_before.copy()
    normalized_entries: list[dict[str, Any]] = []
    for requested in entries:
        if not isinstance(requested, Mapping):
            raise SealedProposalJournalError("sealed proposal batch entries must be objects")
        entry = _normalise_append_entry(
            requested,
            proposal_ordinal=state_after.proposal_count,
            previous_entry_sha256=state_after.head.entry_sha256,
        )
        _apply_verified_entry(state_after, entry)
        normalized_entries.append(entry)
    segment_ordinal = (
        0
        if state_before.head.segment_ordinal is None
        else state_before.head.segment_ordinal + 1
    )
    payload: dict[str, Any] = {
        "schemaVersion": SEALED_PROPOSAL_SEGMENT_SCHEMA,
        "journalBindingSha256": binding["journalBindingSha256"],
        "authoritySha256": binding["authoritySha256"],
        "configSha256": binding["configSha256"],
        "segmentOrdinal": segment_ordinal,
        "startProposalOrdinal": state_before.proposal_count,
        "endProposalOrdinal": state_after.proposal_count,
        "priorSegmentSha256": state_before.head.segment_sha256,
        "priorEntrySha256": state_before.head.entry_sha256,
        "entryCount": len(normalized_entries),
        "acceptedEntryCount": sum(1 for entry in normalized_entries if entry["accepted"]),
        "entries": normalized_entries,
        "journalHeadEntrySha256": state_after.head.entry_sha256,
    }
    payload["segmentSha256"] = canonical_sha256(payload)
    state_after.head = JournalHead(
        proposal_count=state_after.proposal_count,
        segment_ordinal=segment_ordinal,
        segment_sha256=payload["segmentSha256"],
        entry_sha256=state_after.head.entry_sha256,
    )
    return payload, state_after


def _validate_segment_envelope(
    segment: Mapping[str, Any], *, binding: Mapping[str, Any]
) -> None:
    expected = {
        "schemaVersion",
        "journalBindingSha256",
        "authoritySha256",
        "configSha256",
        "segmentOrdinal",
        "startProposalOrdinal",
        "endProposalOrdinal",
        "priorSegmentSha256",
        "priorEntrySha256",
        "entryCount",
        "acceptedEntryCount",
        "entries",
        "journalHeadEntrySha256",
        "segmentSha256",
    }
    if set(segment) != expected:
        raise SealedProposalJournalError("sealed proposal segment fields are not exact")
    if segment.get("schemaVersion") != SEALED_PROPOSAL_SEGMENT_SCHEMA:
        raise SealedProposalJournalError("sealed proposal segment schema is unsupported")
    for key in ("journalBindingSha256", "authoritySha256", "configSha256"):
        if segment.get(key) != binding.get(key):
            raise SealedProposalJournalError("sealed proposal segment authority/config binding mismatch")
    _integer(segment.get("segmentOrdinal"), name="sealed segment ordinal")
    start = _integer(segment.get("startProposalOrdinal"), name="sealed segment start ordinal")
    end = _integer(segment.get("endProposalOrdinal"), name="sealed segment end ordinal")
    if end <= start:
        raise SealedProposalJournalError("sealed proposal segment is empty or reverses its ordinal")
    prior_segment = segment.get("priorSegmentSha256")
    prior_entry = segment.get("priorEntrySha256")
    if prior_segment is not None:
        _sha256(prior_segment, name="sealed prior segment identity")
    if prior_entry is not None:
        _sha256(prior_entry, name="sealed prior entry identity")
    entries = segment.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SealedProposalJournalError("sealed proposal segment entries are invalid")
    if _integer(segment.get("entryCount"), name="sealed segment entry count", minimum=1) != len(entries):
        raise SealedProposalJournalError("sealed proposal segment entry count mismatch")
    accepted_count = sum(
        1 for entry in entries if isinstance(entry, Mapping) and entry.get("accepted") is True
    )
    if _integer(segment.get("acceptedEntryCount"), name="sealed segment accepted count") != accepted_count:
        raise SealedProposalJournalError("sealed proposal segment accepted count mismatch")
    _sha256(segment.get("journalHeadEntrySha256"), name="sealed segment journal head")
    _identity_without(segment, "segmentSha256")


def _apply_verified_segment(
    state: _JournalState, segment: Mapping[str, Any], *, binding: Mapping[str, Any]
) -> None:
    _validate_segment_envelope(segment, binding=binding)
    expected_ordinal = (
        0 if state.head.segment_ordinal is None else state.head.segment_ordinal + 1
    )
    if segment["segmentOrdinal"] != expected_ordinal:
        raise SealedProposalJournalError(
            "sealed proposal segment ordinal is reordered, duplicated, or gapped"
        )
    if segment["startProposalOrdinal"] != state.proposal_count:
        raise SealedProposalJournalError("sealed proposal segment start ordinal diverged")
    if segment["priorSegmentSha256"] != state.head.segment_sha256:
        raise SealedProposalJournalError("sealed proposal segment hash chain diverged")
    if segment["priorEntrySha256"] != state.head.entry_sha256:
        raise SealedProposalJournalError("sealed proposal segment entry chain diverged")
    for entry in segment["entries"]:
        if not isinstance(entry, Mapping):
            raise SealedProposalJournalError("sealed proposal segment entry is not an object")
        _apply_verified_entry(state, entry)
    if segment["endProposalOrdinal"] != state.proposal_count:
        raise SealedProposalJournalError("sealed proposal segment end ordinal diverged")
    if segment["journalHeadEntrySha256"] != state.head.entry_sha256:
        raise SealedProposalJournalError("sealed proposal segment journal head diverged")
    state.head = JournalHead(
        proposal_count=state.proposal_count,
        segment_ordinal=segment["segmentOrdinal"],
        segment_sha256=segment["segmentSha256"],
        entry_sha256=state.head.entry_sha256,
    )


def _state_payload(state: _JournalState, *, binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": SEALED_PROPOSAL_STATE_SCHEMA,
        "journalBindingSha256": binding["journalBindingSha256"],
        "authoritySha256": binding["authoritySha256"],
        "configSha256": binding["configSha256"],
        "proposalCount": state.proposal_count,
        "acceptedCount": len(state.accepted_refs),
        "acceptedRefs": [ref.as_payload() for ref in state.accepted_refs],
        "candidateIdentityIndex": {
            identity: ref.proposal_ordinal
            for identity, ref in sorted(state.candidate_identity_index.items())
        },
        "semanticIdentityIndex": {
            identity: ref.proposal_ordinal
            for identity, ref in sorted(state.semantic_identity_index.items())
        },
        "schedulingCounters": dict(sorted(state.scheduling_counters.items())),
        "originProposalCounts": dict(sorted(state.origin_proposal_counts.items())),
        "originAcceptedCounts": dict(sorted(state.origin_accepted_counts.items())),
        "dispositionCounts": dict(sorted(state.disposition_counts.items())),
        "journalHead": state.head.as_payload(),
    }


def _checkpoint_payload(state: _JournalState, *, binding: Mapping[str, Any]) -> dict[str, Any]:
    payload = _state_payload(state, binding=binding)
    payload["schemaVersion"] = SEALED_PROPOSAL_CHECKPOINT_SCHEMA
    payload["checkpointSha256"] = canonical_sha256(payload)
    return payload


def _reference_from_payload(value: Any, *, proposal_count: int) -> AcceptedProposalReference:
    raw = _mapping(value, name="sealed accepted reference")
    expected = {
        "proposalOrdinal",
        "entrySha256",
        "legacyEntrySha256",
        "originKind",
        "disposition",
        "contentRef",
        "candidateIdentitySha256",
        "semanticIdentitySha256",
    }
    if set(raw) != expected:
        raise SealedProposalJournalError("sealed accepted reference fields are not exact")
    ordinal = _integer(raw["proposalOrdinal"], name="sealed accepted reference ordinal")
    if ordinal >= proposal_count:
        raise SealedProposalJournalError("sealed accepted reference is beyond its checkpoint head")
    origin_kind = _origin_kind(
        raw["originKind"], name="sealed accepted reference originKind"
    )
    disposition = _disposition(
        raw["disposition"], name="sealed accepted reference disposition"
    )
    if disposition != "accepted":
        raise SealedProposalJournalError(
            "sealed accepted reference disposition is not accepted"
        )
    return AcceptedProposalReference(
        proposal_ordinal=ordinal,
        entry_sha256=_sha256(raw["entrySha256"], name="sealed accepted reference entry identity"),
        legacy_entry_sha256=_sha256(
            raw["legacyEntrySha256"], name="sealed accepted reference legacy entry identity"
        ),
        origin_kind=origin_kind,
        disposition=disposition,
        content_ref=_content_ref(raw["contentRef"], name="sealed accepted reference contentRef"),
        candidate_identity_sha256=_sha256(
            raw["candidateIdentitySha256"], name="sealed accepted candidate identity"
        ),
        semantic_identity_sha256=_sha256(
            raw["semanticIdentitySha256"], name="sealed accepted semantic identity"
        ),
    )


def _head_from_payload(value: Any, *, proposal_count: int) -> JournalHead:
    raw = _mapping(value, name="sealed checkpoint journal head")
    expected = {"proposalCount", "segmentOrdinal", "segmentSha256", "entrySha256"}
    if set(raw) != expected:
        raise SealedProposalJournalError("sealed checkpoint journal head fields are not exact")
    if _integer(raw["proposalCount"], name="sealed checkpoint head proposal count") != proposal_count:
        raise SealedProposalJournalError("sealed checkpoint journal head proposal count diverged")
    if proposal_count == 0:
        if any(raw[key] is not None for key in ("segmentOrdinal", "segmentSha256", "entrySha256")):
            raise SealedProposalJournalError("empty sealed checkpoint has a non-empty journal head")
        return JournalHead(0, None, None, None)
    ordinal = _integer(raw["segmentOrdinal"], name="sealed checkpoint segment ordinal")
    return JournalHead(
        proposal_count=proposal_count,
        segment_ordinal=ordinal,
        segment_sha256=_sha256(raw["segmentSha256"], name="sealed checkpoint segment identity"),
        entry_sha256=_sha256(raw["entrySha256"], name="sealed checkpoint entry identity"),
    )


def _count_map(
    value: Any,
    *,
    name: str,
    key_validator: Any,
) -> dict[str, int]:
    raw = _mapping(value, name=name)
    result: dict[str, int] = {}
    for key, count in raw.items():
        key_validator(key, name=f"{name} key")
        # A deterministic folded count never stores zero-valued categories.
        result[key] = _integer(count, name=f"{name}.{key}", minimum=1)
    return dict(sorted(result.items()))


def _state_from_checkpoint(
    checkpoint: Mapping[str, Any], *, binding: Mapping[str, Any], filename_count: int
) -> _JournalState:
    expected = {
        "schemaVersion",
        "journalBindingSha256",
        "authoritySha256",
        "configSha256",
        "proposalCount",
        "acceptedCount",
        "acceptedRefs",
        "candidateIdentityIndex",
        "semanticIdentityIndex",
        "schedulingCounters",
        "originProposalCounts",
        "originAcceptedCounts",
        "dispositionCounts",
        "journalHead",
        "checkpointSha256",
    }
    if set(checkpoint) != expected:
        raise SealedProposalJournalError("sealed proposal checkpoint fields are not exact")
    if checkpoint.get("schemaVersion") != SEALED_PROPOSAL_CHECKPOINT_SCHEMA:
        raise SealedProposalJournalError("sealed proposal checkpoint schema is unsupported")
    for key in ("journalBindingSha256", "authoritySha256", "configSha256"):
        if checkpoint.get(key) != binding.get(key):
            raise SealedProposalJournalError(
                "sealed proposal checkpoint authority/config binding mismatch"
            )
    _identity_without(checkpoint, "checkpointSha256")
    proposal_count = _integer(
        checkpoint.get("proposalCount"), name="sealed checkpoint proposal count"
    )
    if proposal_count != filename_count:
        raise SealedProposalJournalError("sealed proposal checkpoint filename is stale or divergent")
    refs_value = checkpoint.get("acceptedRefs")
    if not isinstance(refs_value, list):
        raise SealedProposalJournalError("sealed checkpoint accepted references are invalid")
    refs = [
        _reference_from_payload(item, proposal_count=proposal_count)
        for item in refs_value
    ]
    if refs != sorted(refs, key=lambda ref: ref.proposal_ordinal):
        raise SealedProposalJournalError("sealed checkpoint accepted references are not ordered")
    if len({ref.proposal_ordinal for ref in refs}) != len(refs):
        raise SealedProposalJournalError("sealed checkpoint repeats an accepted proposal ordinal")
    candidate_index: dict[str, AcceptedProposalReference] = {}
    semantic_index: dict[str, AcceptedProposalReference] = {}
    for ref in refs:
        if ref.candidate_identity_sha256 in candidate_index:
            raise SealedProposalJournalError(
                "sealed checkpoint has duplicate accepted candidate identities"
            )
        if ref.semantic_identity_sha256 in semantic_index:
            raise SealedProposalJournalError(
                "sealed checkpoint has duplicate accepted semantic identities"
            )
        candidate_index[ref.candidate_identity_sha256] = ref
        semantic_index[ref.semantic_identity_sha256] = ref
    if _integer(checkpoint.get("acceptedCount"), name="sealed checkpoint accepted count") != len(refs):
        raise SealedProposalJournalError("sealed checkpoint accepted count diverged")
    expected_candidate_index = {
        identity: ref.proposal_ordinal for identity, ref in candidate_index.items()
    }
    expected_semantic_index = {
        identity: ref.proposal_ordinal for identity, ref in semantic_index.items()
    }
    raw_candidate_index = _mapping(
        checkpoint.get("candidateIdentityIndex"), name="sealed candidate identity index"
    )
    raw_semantic_index = _mapping(
        checkpoint.get("semanticIdentityIndex"), name="sealed semantic identity index"
    )
    for index, name in (
        (raw_candidate_index, "sealed candidate identity index"),
        (raw_semantic_index, "sealed semantic identity index"),
    ):
        for identity, ordinal in index.items():
            _sha256(identity, name=f"{name} identity")
            _integer(ordinal, name=f"{name} ordinal")
    if raw_candidate_index != expected_candidate_index:
        raise SealedProposalJournalError("sealed candidate identity index diverged")
    if raw_semantic_index != expected_semantic_index:
        raise SealedProposalJournalError("sealed semantic identity index diverged")
    origin_proposal_counts = _count_map(
        checkpoint.get("originProposalCounts"),
        name="sealed checkpoint origin proposal counts",
        key_validator=_origin_kind,
    )
    origin_accepted_counts = _count_map(
        checkpoint.get("originAcceptedCounts"),
        name="sealed checkpoint origin accepted counts",
        key_validator=_origin_kind,
    )
    disposition_counts = _count_map(
        checkpoint.get("dispositionCounts"),
        name="sealed checkpoint disposition counts",
        key_validator=_disposition,
    )
    if sum(origin_proposal_counts.values()) != proposal_count:
        raise SealedProposalJournalError("sealed checkpoint origin proposal counts diverged")
    if sum(origin_accepted_counts.values()) != len(refs):
        raise SealedProposalJournalError("sealed checkpoint origin accepted counts diverged")
    if sum(disposition_counts.values()) != proposal_count:
        raise SealedProposalJournalError("sealed checkpoint disposition counts diverged")
    expected_origin_accepted_counts: dict[str, int] = {}
    for ref in refs:
        expected_origin_accepted_counts[ref.origin_kind] = (
            expected_origin_accepted_counts.get(ref.origin_kind, 0) + 1
        )
    if origin_accepted_counts != expected_origin_accepted_counts:
        raise SealedProposalJournalError(
            "sealed checkpoint accepted-reference origin counts diverged"
        )
    if disposition_counts.get("accepted", 0) != len(refs):
        raise SealedProposalJournalError(
            "sealed checkpoint accepted disposition count diverged"
        )
    for origin, accepted_count in origin_accepted_counts.items():
        if accepted_count > origin_proposal_counts.get(origin, 0):
            raise SealedProposalJournalError(
                "sealed checkpoint accepted origin count exceeds proposal count"
            )
    return _JournalState(
        proposal_count=proposal_count,
        accepted_refs=refs,
        candidate_identity_index=candidate_index,
        semantic_identity_index=semantic_index,
        scheduling_counters=_counter_delta(
            checkpoint.get("schedulingCounters"), name="sealed checkpoint scheduling counters"
        ),
        origin_proposal_counts=origin_proposal_counts,
        origin_accepted_counts=origin_accepted_counts,
        disposition_counts=disposition_counts,
        head=_head_from_payload(checkpoint.get("journalHead"), proposal_count=proposal_count),
        last_checkpoint_proposal_count=proposal_count,
    )


def _validate_checkpoint_anchor(
    *,
    root: Path,
    state: _JournalState,
    segment_paths: Sequence[Path],
    binding: Mapping[str, Any],
) -> int:
    """Verify the one sealed segment a checkpoint claims as its durable head."""

    if state.proposal_count == 0:
        return 0
    assert state.head.segment_ordinal is not None
    if state.head.segment_ordinal >= len(segment_paths):
        raise SealedProposalJournalError(
            "sealed proposal checkpoint head references a truncated or stale segment"
    )
    segment, bytes_read = _read_canonical_json(
        root,
        segment_paths[state.head.segment_ordinal],
        name="sealed checkpoint head segment",
    )
    _validate_segment_envelope(segment, binding=binding)
    if (
        segment.get("segmentOrdinal") != state.head.segment_ordinal
        or segment.get("segmentSha256") != state.head.segment_sha256
        or segment.get("endProposalOrdinal") != state.proposal_count
        or segment.get("journalHeadEntrySha256") != state.head.entry_sha256
    ):
        raise SealedProposalJournalError(
            "sealed proposal checkpoint head is stale, reordered, or divergent"
        )
    return bytes_read


def _resume(
    *, root: Path, binding: Mapping[str, Any], use_checkpoint: bool, retain_legacy_history: bool
) -> tuple[_JournalState, dict[str, int], list[dict[str, Any]] | None]:
    """Return compact state and I/O counters; legacy mode deliberately scans all."""

    segment_paths = _segment_paths(root)
    bytes_read = 0
    segments_read = 0
    entries_replayed = 0
    state = _JournalState()
    start_segment = 0
    if use_checkpoint:
        checkpoint_rows = _checkpoint_paths(root)
        if checkpoint_rows:
            filename_count, path = checkpoint_rows[-1]
            checkpoint, checkpoint_bytes = _read_canonical_json(
                root, path, name="sealed proposal checkpoint"
            )
            bytes_read += checkpoint_bytes
            state = _state_from_checkpoint(
                checkpoint, binding=binding, filename_count=filename_count
            )
            bytes_read += _validate_checkpoint_anchor(
                root=root, state=state, segment_paths=segment_paths, binding=binding
            )
            segments_read += 1 if state.proposal_count else 0
            start_segment = 0 if state.head.segment_ordinal is None else state.head.segment_ordinal + 1

    legacy_history: list[dict[str, Any]] | None = [] if retain_legacy_history else None
    for expected_ordinal, path in enumerate(segment_paths[start_segment:], start=start_segment):
        segment, segment_bytes = _read_canonical_json(
            root, path, name="sealed proposal segment"
        )
        bytes_read += segment_bytes
        segments_read += 1
        if segment.get("segmentOrdinal") != expected_ordinal:
            raise SealedProposalJournalError(
                "sealed proposal segments are reordered, duplicated, or gapped"
            )
        _apply_verified_segment(state, segment, binding=binding)
        entries_replayed += int(segment["entryCount"])
        if legacy_history is not None:
            # The old implementation's defining cost: it retains every rich
            # attempt while rebuilding state.  Preserve this oracle behavior
            # only for parity/measurement; it is never used by the new path.
            legacy_history.extend(_clone_json(segment["entries"], name="legacy proposal entries"))
    return state, {
        "bytesRead": bytes_read,
        "segmentsRead": segments_read,
        "entriesReplayed": entries_replayed,
    }, legacy_history


class SealedProposalJournal:
    """A batch-oriented, append-only compact proposal-state journal.

    ``open`` is the resume boundary.  The caller supplies the frozen authority
    and generation config on every open; a byte-identical immutable binding is
    required.  ``append_batch`` accepts JSON-compatible proposal deltas, so a
    later Rust/PyO3 producer can cross the boundary once per coherent batch.
    """

    def __init__(
        self,
        *,
        root: Path,
        binding: Mapping[str, Any],
        state: _JournalState,
        resume_metrics: Mapping[str, int],
    ) -> None:
        self._root = root
        self._binding = _mapping(binding, name="sealed journal binding")
        self._state = state
        self._resume_metrics = dict(resume_metrics)

    @classmethod
    def open(
        cls,
        *,
        root: Path | str,
        authority: Mapping[str, Any],
        config: Mapping[str, Any],
        checkpoint_interval_entries: int = 128,
    ) -> SealedProposalJournal:
        journal_root = _ensure_real_directory_tree(
            Path(root), name="sealed journal root"
        )
        _cleanup_private_temporaries(journal_root)
        expected_binding = _binding_payload(
            authority=authority,
            config=config,
            checkpoint_interval_entries=checkpoint_interval_entries,
        )
        binding, binding_bytes = _open_or_create_binding(
            root=journal_root, expected=expected_binding
        )
        state, metrics, _ = _resume(
            root=journal_root,
            binding=binding,
            use_checkpoint=True,
            retain_legacy_history=False,
        )
        metrics["bytesRead"] += binding_bytes
        return cls(
            root=journal_root,
            binding=binding,
            state=state,
            resume_metrics=metrics,
        )

    @property
    def root(self) -> Path:
        return self._root

    @property
    def resume_metrics(self) -> dict[str, int]:
        """Bytes and compact-tail work consumed by the most recent ``open``."""

        return dict(self._resume_metrics)

    def snapshot(self) -> dict[str, Any]:
        """Return detached compact restart state, never proposal documents."""

        return _state_payload(self._state, binding=self._binding)

    def append_batch(self, entries: Sequence[Mapping[str, Any]]) -> BatchAppendResult:
        """Atomically seal one non-empty batch of compact proposal deltas.

        Caller entry contract::

            {
                "contentRef": {"contentSha256": "sha256:...", ...},
                "delta": { ...small proposal/operator audit... },
                "originKind": "random_immigrant" | "structural_offspring",
                "disposition": "accepted" | "<canonical_rejection_token>",
                "legacyEntrySha256": "sha256:...",
                "accepted": bool,
                "candidateIdentitySha256": "sha256:...",  # if accepted
                "semanticIdentitySha256": "sha256:...",   # if accepted
                "schedulingDelta": {"proposalsObserved": 1, ...},
            }

        No proposal ordinal is caller supplied: it is derived from the sealed
        head, preventing a Python/Rust boundary caller from accidentally
        reusing or skipping an ordinal.  ``legacyEntrySha256`` is the rich
        journal entry identity the adapter compacted; it is always explicit,
        even for a rejected proposal.

        A returned ``BatchAppendResult`` is a committed segment receipt.  If
        its ``checkpointError`` is populated, the batch remains committed and
        must not be appended again; retry ``seal_checkpoint`` (which is
        idempotent) or reopen to rebuild from the segment.
        """

        if isinstance(entries, (str, bytes)) or not isinstance(entries, Sequence):
            raise SealedProposalJournalError("sealed proposal batch must be a sequence")
        payload, state_after = _segment_payload(
            binding=self._binding, state_before=self._state, entries=entries
        )
        segment_ordinal = int(payload["segmentOrdinal"])
        _publish_immutable_json(
            self._root,
            _segment_path(self._root, segment_ordinal),
            payload,
            name="proposal segment",
        )
        self._state = state_after
        checkpoint_attempted = False
        checkpoint_written = False
        checkpoint_error: str | None = None
        interval = int(self._binding["checkpointIntervalEntries"])
        if (
            self._state.proposal_count - self._state.last_checkpoint_proposal_count
            >= interval
        ):
            checkpoint_attempted = True
            try:
                self.seal_checkpoint()
            except (OSError, SealedProposalJournalError) as exc:
                # The segment above is already committed.  Checkpoints are
                # optional accelerators, so report their failure in the
                # receipt instead of making retry semantics ambiguous.
                checkpoint_error = f"{type(exc).__name__}: {exc}"
            else:
                checkpoint_written = True
        return BatchAppendResult(
            segment_ordinal=segment_ordinal,
            first_proposal_ordinal=int(payload["startProposalOrdinal"]),
            last_proposal_ordinal=int(payload["endProposalOrdinal"]) - 1,
            entry_sha256s=tuple(entry["entrySha256"] for entry in payload["entries"]),
            segment_sha256=str(payload["segmentSha256"]),
            segment_committed=True,
            checkpoint_attempted=checkpoint_attempted,
            checkpoint_written=checkpoint_written,
            checkpoint_error=checkpoint_error,
            proposal_count=self._state.proposal_count,
            accepted_count=len(self._state.accepted_refs),
        )

    def seal_checkpoint(self) -> dict[str, Any]:
        """Publish an idempotent immutable checkpoint at the current segment head."""

        payload = _checkpoint_payload(self._state, binding=self._binding)
        _publish_immutable_json(
            self._root,
            _checkpoint_path(self._root, self._state.proposal_count),
            payload,
            name="proposal checkpoint",
        )
        self._state.last_checkpoint_proposal_count = self._state.proposal_count
        return _clone_json(payload, name="sealed proposal checkpoint")


def open_sealed_proposal_journal(
    *,
    root: Path | str,
    authority: Mapping[str, Any],
    config: Mapping[str, Any],
    checkpoint_interval_entries: int = 128,
) -> SealedProposalJournal:
    """Convenience batch API for callers that do not need the class name."""

    return SealedProposalJournal.open(
        root=root,
        authority=authority,
        config=config,
        checkpoint_interval_entries=checkpoint_interval_entries,
    )


def replay_legacy_full_history(
    *,
    root: Path | str,
    authority: Mapping[str, Any],
    config: Mapping[str, Any],
    checkpoint_interval_entries: int = 128,
) -> dict[str, Any]:
    """Replay every sealed segment and retain its entries as a simple oracle.

    This intentionally does *not* use a checkpoint.  It remains available for
    parity during staged adoption and models the old all-history resume shape.
    """

    journal_root = _ensure_real_directory_tree(
        Path(root), name="sealed journal root"
    )
    _cleanup_private_temporaries(journal_root)
    expected_binding = _binding_payload(
        authority=authority,
        config=config,
        checkpoint_interval_entries=checkpoint_interval_entries,
    )
    binding, binding_bytes = _open_or_create_binding(root=journal_root, expected=expected_binding)
    state, metrics, history = _resume(
        root=journal_root,
        binding=binding,
        use_checkpoint=False,
        retain_legacy_history=True,
    )
    metrics["bytesRead"] += binding_bytes
    return {
        "state": _state_payload(state, binding=binding),
        "metrics": metrics,
        # Do not expose rich content as a public new-path result.  The count is
        # enough to prove that the full-history oracle retained every attempt.
        "retainedHistoryEntryCount": len(history or []),
    }


def _measure_call(callback: Any) -> tuple[Any, dict[str, int | float]]:
    gc.collect()
    tracemalloc.start()
    started = time.perf_counter_ns()
    try:
        result = callback()
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, {
        "elapsedMs": elapsed_ms,
        "peakTracedMemoryBytes": peak,
    }


def measure_replay(
    *,
    root: Path | str,
    authority: Mapping[str, Any],
    config: Mapping[str, Any],
    checkpoint_interval_entries: int = 128,
) -> dict[str, Any]:
    """Boundedly compare checkpoint-tail resume with the legacy replay oracle.

    The report deliberately gives both bytes-read and ``tracemalloc`` peak
    figures.  The new state retains only accepted references/indexes/counters,
    while the oracle retains every entry it rereads; ``memoryNonRegression`` is
    therefore an executable assertion of the representation goal.
    """

    legacy, legacy_timing = _measure_call(
        lambda: replay_legacy_full_history(
            root=root,
            authority=authority,
            config=config,
            checkpoint_interval_entries=checkpoint_interval_entries,
        )
    )
    sealed, sealed_timing = _measure_call(
        lambda: open_sealed_proposal_journal(
            root=root,
            authority=authority,
            config=config,
            checkpoint_interval_entries=checkpoint_interval_entries,
        )
    )
    sealed_state = sealed.snapshot()
    legacy_state = legacy["state"]
    sealed_metrics = {**sealed.resume_metrics, **sealed_timing}
    legacy_metrics = {**legacy["metrics"], **legacy_timing}
    bytes_saved = int(legacy_metrics["bytesRead"]) - int(sealed_metrics["bytesRead"])
    return {
        "schemaVersion": SEALED_PROPOSAL_REPLAY_MEASUREMENT_SCHEMA,
        "stateEquivalent": sealed_state == legacy_state,
        "legacyFullHistory": {
            **legacy_metrics,
            "retainedHistoryEntryCount": legacy["retainedHistoryEntryCount"],
        },
        "sealedCheckpointTail": sealed_metrics,
        "bytesReadSaved": bytes_saved,
        "memoryNonRegression": int(sealed_metrics["peakTracedMemoryBytes"])
        <= int(legacy_metrics["peakTracedMemoryBytes"]),
    }


__all__ = [
    "MAX_COMPACT_ENTRY_BYTES",
    "SEALED_PROPOSAL_CHECKPOINT_SCHEMA",
    "SEALED_PROPOSAL_ENTRY_SCHEMA",
    "SEALED_PROPOSAL_JOURNAL_BINDING_SCHEMA",
    "SEALED_PROPOSAL_REPLAY_MEASUREMENT_SCHEMA",
    "SEALED_PROPOSAL_SEGMENT_SCHEMA",
    "SEALED_PROPOSAL_STATE_SCHEMA",
    "AcceptedProposalReference",
    "BatchAppendResult",
    "JournalHead",
    "SealedProposalJournal",
    "SealedProposalJournalError",
    "measure_replay",
    "open_sealed_proposal_journal",
    "replay_legacy_full_history",
]
