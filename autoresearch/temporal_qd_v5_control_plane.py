"""Thin, fail-closed subprocess bridges for the native Temporal QD v5 plane.

This module is deliberately plumbing, not an alternate implementation of the
research pipeline.  It writes only compact canonical manifests, invokes the
pinned Rust binaries, and validates receipt-last outputs.  In particular it
never enumerates candidates, task rows, gateway results, or archive members.
Those loops remain inside the Rust transactions named by the frozen runtime
authority.
"""

from __future__ import annotations

import json
import hashlib
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import temporal_qd_native as native
from .result_codec import canonical_json_bytes
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_qd_campaign_native import (
    freeze_qd_v5_campaign_native,
    freeze_qd_v5_evidence_ladder_archive_native,
)
from .temporal_qd_evaluation_population import raw_file_sha256
from .temporal_qd_v5_native_tail import (
    build_v5_directional_tail_authority,
    validate_v5_directional_tail_authority,
)
from .temporal_qd_v5_native import (
    TemporalQDV5NativeError,
    V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
    V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA,
    V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA,
    V5_G0_NATIVE_V5_INVOCATION_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
    V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
    V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA,
    V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
    V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA,
    V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA,
    V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
    V5_PROPOSAL_MANIFEST_SCHEMA,
    V5_PROPOSAL_OPERATION,
    V5_PROPOSAL_RESULT_FILENAME,
    V5_PROPOSAL_RESULT_SCHEMA,
    V5_PROPOSAL_GENERATION_G0,
    V5_PROPOSAL_GENERATION_EVOLVED,
    validate_v5_generation_construction_adapter,
    validate_v5_proposal_manifest,
    validate_v5_proposal_result,
)


CONTRACT_VERSION = "temporal_qd_native_foundation_v1"
RUNTIME_AUTHORITY_SCHEMA = "temporal_qd_native_finalization_runtime_authority_v1"
GATEWAY_RECEIPT_SCHEMA = "temporal_qd_native_gateway_execution_receipt_v2"
GATEWAY_RESULT_SCHEMA = "temporal_qd_native_gateway_dispatch_result_v1"
CAMPAIGN_SOURCE_BUILD_MANIFEST_SCHEMA = (
    "temporal_qd_campaign_seal_source_build_manifest_v1"
)
CAMPAIGN_SOURCE_BUILD_RESULT_SCHEMA = "temporal_qd_campaign_seal_source_build_result_v1"
CAMPAIGN_SOURCE_BUILD_RECEIPT_SCHEMA = "temporal_qd_campaign_seal_source_build_receipt_v1"
CAMPAIGN_SEAL_EXECUTION_SCHEMA = "temporal_qd_campaign_seal_execution_v2"
CAMPAIGN_SEAL_EXECUTION_RECEIPT_SCHEMA = (
    "temporal_qd_campaign_seal_execution_receipt_v2"
)
CAMPAIGN_SEAL_SCHEMA = "temporal_qd_campaign_seal_v1"
TAIL_TRANSACTION_SCHEMA = "temporal_qd_generation_tail_transaction_v1"
TAIL_RESULT_SCHEMA = "temporal_qd_native_tail_reduction_result_v1"
TAIL_AUTHORITY_RECEIPT_SCHEMA = "temporal_qd_tail_authority_receipt_v1"
PANEL_SIDECAR_RESULT_SCHEMA = (
    "temporal_qd_v5_candidate_panel_bundle_sidecar_result_v1"
)
PANEL_SIDECAR_RECEIPT_SCHEMA = (
    "temporal_qd_v5_candidate_panel_bundle_sidecar_receipt_v1"
)
PANEL_SIDECAR_DESCRIPTOR_SCHEMA = (
    "temporal_qd_v5_candidate_panel_bundle_sidecar_descriptor_v1"
)
CAMPAIGN_RECEIPT_INPUT_SCHEMA = "temporal_qd_v5_rotating_campaign_receipt_input_v2"
CAMPAIGN_RECEIPT_SCHEMA = "temporal_qd_v5_rotating_campaign_receipt_v2"
PREFINALIZER_EXECUTION_SCHEMA = "temporal_qd_v5_rotating_prefinalizer_execution_v2"
PREFINALIZER_EXECUTION_RECEIPT_SCHEMA = (
    "temporal_qd_v5_rotating_prefinalizer_execution_receipt_v2"
)
PREFINALIZER_RESULT_SCHEMA = "temporal_qd_v5_rotating_prefinalizer_result_v1"
FINALIZER_EXECUTION_SCHEMA = "temporal_qd_generation_finalization_execution_v1"
FINALIZER_SOURCE_SCHEMA = "temporal_qd_generation_finalization_source_v2"
FINALIZER_MANIFEST_SCHEMA = "temporal_qd_generation_finalization_manifest_v2"
GENERATION_COMMIT_SCHEMA = "temporal_qd_generation_commit_v1"
GENERATION_RECORD_SCHEMA = "temporal_qd_generation_record_v2"
GENERATION_STATE_PATCH_SCHEMA = "temporal_qd_generation_state_patch_v2"
GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA = (
    "temporal_qd_v5_generation_state_application_sidecar_v1"
)
GENERATION_STATE_APPLICATION_SIDECAR_FILENAME = (
    "generation-state-application-sidecar.json"
)
_V5_PROPOSAL_STATE_AUTHORITY_SCHEMA = {
    "generationKind",
    "proposalManifestSha256",
    "proposalReceiptSha256",
    "generationJournalSha256",
    "inputIdentityLedgerSha256",
    "outputIdentityLedgerRelativePath",
    "outputIdentityLedgerSha256",
    "outputIdentityLedgerFileSha256",
}
ARCHIVE_REDUCTION_RESULT_SCHEMA = "temporal_qd_native_archive_reduction_result_v1"
ARCHIVE_REDUCTION_MANIFEST_SCHEMA = "temporal_qd_native_archive_reduction_manifest_v2"
ARCHIVE_REDUCTION_OPERATION = "reduce_evidence_ladder_archive"
NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_manifest_v3"
)
NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_result_v3"
)
NATIVE_V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_transaction_v3"
)
NATIVE_V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_receipt_v3"
)
NATIVE_V5_LADDER_AUTHORITY_SCHEMA = "temporal_qd_v5_native_evidence_ladder_authority_v1"
EVOLVED_ATTEMPT_CHAIN_INPUT_SCHEMA = (
    "temporal_qd_v5_evolved_attempt_adapter_chain_input_v1"
)
EVOLVED_ATTEMPT_ROW_SCHEMA = "temporal_qd_v5_proposal_funnel_entry_v1"
EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA = (
    "temporal_qd_v5_evolved_attempt_stream_receipt_v1"
)
G0_ATTEMPT_CHAIN_INPUT_SCHEMA = "temporal_qd_v5_g0_funnel_source_chain_input_v1"
G0_SELECTED_ATTEMPT_STREAM_RECEIPT_SCHEMA = (
    "temporal_qd_v5_g0_selected_attempt_stream_receipt_v1"
)
G0_SELECTED_ATTEMPT_ROW_SCHEMA = "temporal_qd_v5_g0_selected_proposal_attempt_v1"
FUNNEL_REDUCTION_INPUT_SCHEMA = "temporal_qd_v5_native_funnel_reduction_input_v2"
FUNNEL_REDUCTION_SOURCE_SCHEMA = "temporal_qd_native_funnel_reduction_source_v1"
FUNNEL_ASSEMBLY_EXECUTION_SCHEMA = (
    "temporal_qd_v5_native_funnel_assembly_execution_v2"
)
FUNNEL_ASSEMBLY_RECEIPT_SCHEMA = (
    "temporal_qd_v5_native_funnel_assembly_receipt_v2"
)
FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA = (
    "temporal_qd_v5_native_funnel_assembly_receipt_binding_v1"
)
PREFINALIZER_BASE_MANIFEST_SCHEMA = "temporal_qd_v5_rotating_prefinalizer_manifest_v2"
PREFINALIZER_RESUME_MANIFEST_SCHEMA = (
    "temporal_qd_v5_rotating_prefinalizer_resume_manifest_v2"
)
PREFINALIZER_TASK_PLAN_SCHEMA = "temporal_qd_v5_rotating_prefinalizer_task_plan_v2"
PREFINALIZER_TASK_SELECTION_SCHEMA = (
    "temporal_qd_v5_native_rich_candidate_selection_v2"
)
PREFINALIZER_TASK_DESCRIPTOR_SCHEMA = (
    "temporal_qd_v5_native_rich_candidate_jsonl_descriptor_v1"
)
PREFINALIZER_TASK_SELECTION_DOCUMENT_SCHEMA = (
    "temporal_qd_v5_non_proposal_task_selection_v2"
)
PREFINALIZER_TASK_SELECTION_RECEIPT_SCHEMA = (
    "temporal_qd_v5_non_proposal_task_selection_receipt_v2"
)

# Every document this thin plane opens is a compact authority, receipt, or
# invocation record.  Candidate/task row streams stay inside Rust and must
# never be admitted through one of these readers.
_CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES = 1_048_576
_CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES = 1_048_576
_CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES = 262_144
# The manifest carries only frozen static authority but is intentionally larger
# than a result/receipt.  Keep its bounded transport budget distinct from every
# other compact leaf; candidate/task streams remain forbidden here.
_CURRENT_V5_INVOCATION_MANIFEST_LIMIT_BYTES = 2 * 1_048_576

_RUNTIME_ROLES = frozenset(
    {
        "campaignFreeze",
        "gatewayDispatch",
        "campaignSeal",
        "tailReducer",
        "rotatingPrefinalizer",
        "generationFinalizer",
        "archiveReducer",
    }
)


class TemporalQDV5ControlPlaneError(TemporalDiscoveryContractError):
    """A native v5 control-plane binary or durable receipt was not trustworthy."""


def _sha(value: object, *, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise TemporalQDV5ControlPlaneError(f"{name} must be a sha256 identity")
    return value


def _mapping(value: object, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalQDV5ControlPlaneError(f"{name} must be an object")
    return dict(value)


def _positive(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TemporalQDV5ControlPlaneError(f"{name} must be a positive integer")
    return value


def _nonnegative(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TemporalQDV5ControlPlaneError(f"{name} must be a nonnegative integer")
    return value


def _real_path(path: Path | str, *, name: str, directory: bool = False) -> Path:
    """Require an absolute, non-link file/directory without path aliasing."""

    candidate = Path(path)
    if not candidate.is_absolute():
        raise TemporalQDV5ControlPlaneError(f"{name} path must be absolute")
    candidate = Path(os.path.abspath(str(candidate)))
    try:
        status = candidate.lstat()
    except FileNotFoundError as exc:
        raise TemporalQDV5ControlPlaneError(f"{name} is unavailable: {candidate}") from exc
    except OSError as exc:
        raise TemporalQDV5ControlPlaneError(f"could not inspect {name}: {candidate}") from exc
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    expected_kind = stat.S_ISDIR(status.st_mode) if directory else stat.S_ISREG(status.st_mode)
    if stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point) or not expected_kind:
        noun = "directory" if directory else "regular file"
        raise TemporalQDV5ControlPlaneError(f"{name} is not a real {noun}: {candidate}")
    _reject_aliased_ancestors(candidate, name=name)
    return candidate


def native_v5_transport_path_matches(
    reported_path: object, expected_path: Path | str
) -> bool:
    """Match the only alternate Windows spelling Rust may return for a path.

    ``std::fs::canonicalize`` returns an extended-length ``\\\\?\\C:\\...``
    spelling on Windows.  That spelling is part of Rust's self-hashed transport
    descriptor, so Python must never strip it or re-hash a rewritten document.
    Accept only the exact extended-drive form of the already-safe ordinary
    path supplied to Rust; UNC paths, case/separator aliases, relative paths,
    and every other alternate spelling remain invalid.
    """

    if not isinstance(reported_path, str):
        return False
    try:
        expected = os.fspath(expected_path)
    except TypeError:
        return False
    if not isinstance(expected, str) or not expected:
        return False
    if reported_path == expected:
        return True
    if os.name != "nt":
        return False
    # The expected path comes from _real_path()/Path.resolve(), which produces
    # the ordinary drive-rooted spelling.  Do not canonicalize user-provided
    # descriptor text here: that would silently admit a distinct authority.
    if (
        len(expected) < 3
        or not expected[0].isalpha()
        or expected[1] != ":"
        or expected[2] != "\\"
        or "/" in expected
    ):
        return False
    return reported_path == "\\\\?\\" + expected


def native_v5_archive_transport_path_matches(
    reported_path: object, expected_path: Path | str
) -> bool:
    """Compatibility name for the archive certifier's transport check."""

    return native_v5_transport_path_matches(reported_path, expected_path)


def _native_v5_rust_canonical_directory_transport(path: Path) -> str:
    """Spell a verified directory exactly as ``std::fs::canonicalize`` does."""

    ordinary = str(path)
    if os.name != "nt":
        return ordinary
    if (
        len(ordinary) < 3
        or not ordinary[0].isalpha()
        or ordinary[1] != ":"
        or ordinary[2] != "\\"
        or "/" in ordinary
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 directory has no supported Rust transport spelling"
        )
    return "\\\\?\\" + ordinary


def _reject_aliased_ancestors(path: Path, *, name: str) -> None:
    """Refuse a link/reparse component anywhere in a control-plane path."""

    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    current = path
    while True:
        try:
            status = current.lstat()
        except OSError as exc:
            raise TemporalQDV5ControlPlaneError(
                f"could not inspect {name} path component: {current}"
            ) from exc
        attributes = getattr(status, "st_file_attributes", 0)
        if stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point):
            raise TemporalQDV5ControlPlaneError(
                f"{name} path contains a link/reparse component: {current}"
            )
        parent = current.parent
        if parent == current:
            return
        current = parent


def _real_directory(path: Path | str, *, name: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise TemporalQDV5ControlPlaneError(f"{name} path must be absolute")
    candidate = Path(os.path.abspath(str(candidate)))
    existing = candidate
    while not existing.exists():
        parent = existing.parent
        if parent == existing:
            raise TemporalQDV5ControlPlaneError(f"{name} has no existing parent")
        existing = parent
    _reject_aliased_ancestors(existing, name=name)
    candidate.mkdir(parents=True, exist_ok=True)
    return _real_path(candidate, name=name, directory=True)


def _canonical_line(value: Mapping[str, Any]) -> bytes:
    return canonical_json_bytes(dict(value)) + b"\n"


def _read_compact_bytes(
    path: Path | str,
    *,
    name: str,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> tuple[Path, bytes]:
    """Read at most ``maximum_bytes + 1`` from one compact control file."""

    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes < 1
    ):
        raise TemporalQDV5ControlPlaneError(
            f"{name} compact-document limit is invalid"
        )
    checked = _real_path(path, name=name)
    try:
        if checked.stat().st_size > maximum_bytes:
            raise TemporalQDV5ControlPlaneError(
                f"{name} exceeds the control-document limit"
            )
        with checked.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
    except TemporalQDV5ControlPlaneError:
        raise
    except OSError as exc:
        raise TemporalQDV5ControlPlaneError(f"could not read {name}") from exc
    if len(raw) > maximum_bytes:
        raise TemporalQDV5ControlPlaneError(f"{name} exceeds the control-document limit")
    return checked, raw


def _read_canonical_object(
    path: Path | str,
    *,
    name: str,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> dict[str, Any]:
    try:
        _, raw = _read_compact_bytes(
            path, name=name, maximum_bytes=maximum_bytes
        )
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDV5ControlPlaneError(f"could not parse {name}") from exc
    output = _mapping(value, name=name)
    if _canonical_line(output) != raw:
        raise TemporalQDV5ControlPlaneError(f"{name} must be canonical JSON plus LF")
    return output


def _read_json_object(path: Path | str, *, name: str) -> dict[str, Any]:
    try:
        _, raw = _read_compact_bytes(path, name=name)
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDV5ControlPlaneError(f"could not parse {name}") from exc
    return _mapping(value, name=name)


def _read_bounded_pretty_object(
    path: Path | str, *, name: str, maximum_bytes: int = 1_048_576
) -> dict[str, Any]:
    """Read one small Rust pretty-JSON control document, never a row stream."""

    _, raw = _read_compact_bytes(
        path, name=name, maximum_bytes=maximum_bytes
    )
    try:
        value = _mapping(json.loads(raw), name=name)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDV5ControlPlaneError(f"could not parse {name}") from exc
    expected = (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    if raw != expected:
        raise TemporalQDV5ControlPlaneError(
            f"{name} must be canonical pretty JSON plus LF"
        )
    return value


def _read_bounded_canonical_object(
    path: Path | str, *, name: str, maximum_bytes: int = 1_048_576
) -> dict[str, Any]:
    """Read one bounded canonical receipt without admitting an archive payload."""

    _, raw = _read_compact_bytes(
        path, name=name, maximum_bytes=maximum_bytes
    )
    try:
        value = _mapping(json.loads(raw), name=name)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDV5ControlPlaneError(f"could not parse {name}") from exc
    if _canonical_line(value) != raw:
        raise TemporalQDV5ControlPlaneError(f"{name} must be canonical JSON plus LF")
    return value


def _write_canonical_once(path: Path | str, value: Mapping[str, Any], *, name: str) -> Path:
    target = Path(path)
    if not target.is_absolute():
        raise TemporalQDV5ControlPlaneError(f"{name} path must be absolute")
    target = Path(os.path.abspath(str(target)))
    _real_directory(target.parent, name=f"{name} parent")
    encoded = _canonical_line(value)
    if len(encoded) > _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES:
        raise TemporalQDV5ControlPlaneError(f"{name} exceeds the control-document limit")
    if target.exists():
        _, existing = _read_compact_bytes(target, name=name)
        if existing != encoded:
            raise TemporalQDV5ControlPlaneError(f"{name} write-once content drifted")
        return target
    try:
        with target.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _, existing = _read_compact_bytes(target, name=name)
        if existing != encoded:
            raise TemporalQDV5ControlPlaneError(f"{name} write-once content drifted")
    except OSError as exc:
        raise TemporalQDV5ControlPlaneError(f"could not publish {name}") from exc
    return _real_path(target, name=name)


def _descriptor(
    path: Path | str,
    *,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> dict[str, Any]:
    checked, raw = _read_compact_bytes(
        path,
        name="native v5 execution artifact",
        maximum_bytes=maximum_bytes,
    )
    return {
        "path": str(checked),
        "rawSha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "sizeBytes": len(raw),
    }


def _require_bound_file(
    *,
    path: Path | str,
    raw_sha256: object,
    size_bytes: object,
    name: str,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> Path:
    """Recheck a compact descriptor without parsing or enumerating its rows."""

    checked, raw = _read_compact_bytes(
        path, name=name, maximum_bytes=maximum_bytes
    )
    expected_sha256 = _sha(raw_sha256, name=f"{name} raw identity")
    expected_size = _nonnegative(size_bytes, name=f"{name} byte length")
    if (
        len(raw) != expected_size
        or "sha256:" + hashlib.sha256(raw).hexdigest() != expected_sha256
    ):
        raise TemporalQDV5ControlPlaneError(f"{name} descriptor drifted")
    return checked


def _read_bound_canonical_object(
    value: object, *, name: str
) -> tuple[dict[str, Any], Path]:
    """Reopen one exact `{path, rawSha256, sizeBytes}` control binding."""

    descriptor = _mapping(value, name=f"{name} descriptor")
    if set(descriptor) != {"path", "rawSha256", "sizeBytes"}:
        raise TemporalQDV5ControlPlaneError(f"{name} descriptor schema drifted")
    path = _require_bound_file(
        path=descriptor.get("path"),
        raw_sha256=descriptor.get("rawSha256"),
        size_bytes=descriptor.get("sizeBytes"),
        name=name,
    )
    return _read_canonical_object(path, name=name), path


def _validate_runtime_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    authority = _mapping(value, name="native v5 runtime authority")
    expected = {
        "schemaVersion",
        "generationFinalizationEngine",
        "contractVersion",
        "binaries",
        "authoritySha256",
    }
    if set(authority) != expected or authority.get("schemaVersion") != RUNTIME_AUTHORITY_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 runtime authority schema drifted")
    supplied = _sha(authority.get("authoritySha256"), name="native v5 runtime authority")
    body = dict(authority)
    body.pop("authoritySha256")
    if canonical_sha256(body) != supplied:
        raise TemporalQDV5ControlPlaneError("native v5 runtime authority identity drifted")
    if authority.get("generationFinalizationEngine") != "rust" or authority.get("contractVersion") != CONTRACT_VERSION:
        raise TemporalQDV5ControlPlaneError("native v5 runtime authority is incompatible")
    binaries = _mapping(authority.get("binaries"), name="native v5 runtime binaries")
    if set(binaries) != _RUNTIME_ROLES:
        raise TemporalQDV5ControlPlaneError("native v5 runtime authority role set drifted")
    for role, descriptor in binaries.items():
        item = _mapping(descriptor, name=f"native v5 {role} binary descriptor")
        if set(item) != {"path", "bytes", "fileSha256"}:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {role} binary descriptor is malformed"
            )
        _sha(item.get("fileSha256"), name=f"native v5 {role} binary identity")
        _nonnegative(item.get("bytes"), name=f"native v5 {role} binary size")
    return authority


def pinned_runtime_binary(
    *, runtime_authority: Mapping[str, Any], role: str
) -> Path:
    """Resolve one frozen binary and recheck its bytes before a subprocess call."""

    authority = _validate_runtime_authority(runtime_authority)
    if role not in _RUNTIME_ROLES:
        raise TemporalQDV5ControlPlaneError("native v5 runtime role is unsupported")
    descriptor = _mapping(authority["binaries"][role], name=f"native v5 {role} binary")
    path = _real_path(str(descriptor["path"]), name=f"native v5 {role} binary")
    if path.stat().st_size != descriptor["bytes"] or raw_file_sha256(path) != descriptor["fileSha256"]:
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {role} binary identity drifted"
        )
    return path


def _run_pinned(
    *,
    runtime_authority: Mapping[str, Any],
    role: str,
    command: list[str],
    timeout_seconds: int,
) -> dict[str, Any]:
    binary = pinned_runtime_binary(runtime_authority=runtime_authority, role=role)
    if not command or command[0] != str(binary):
        raise TemporalQDV5ControlPlaneError("native v5 subprocess binary binding drifted")
    before = raw_file_sha256(binary)
    try:
        completed = native._run_checked(
            command,
            cwd=Path.cwd(),
            raise_on_nonzero=False,
            timeout=_positive(timeout_seconds, name="native v5 subprocess timeout"),
            stdout_limit_bytes=_CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES,
            stderr_limit_bytes=_CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES,
        )
    except (OSError, native.TemporalQDNativeError) as exc:
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {role} subprocess failed: {exc}"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()[-4000:]
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {role} subprocess failed: {detail}"
        )
    if raw_file_sha256(binary) != before:
        raise TemporalQDV5ControlPlaneError(f"native v5 {role} binary changed during execution")
    try:
        value = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {role} stdout is not canonical JSON"
        ) from exc
    output = _mapping(value, name=f"native v5 {role} stdout")
    if _canonical_line(output) != completed.stdout:
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {role} stdout is not canonical JSON plus LF"
        )
    return output


def _self_hashed(value: Mapping[str, Any], *, field: str, name: str) -> dict[str, Any]:
    output = _mapping(value, name=name)
    supplied = _sha(output.get(field), name=f"{name} identity")
    body = dict(output)
    body.pop(field)
    if canonical_sha256(body) != supplied:
        raise TemporalQDV5ControlPlaneError(f"{name} identity drifted")
    return output


def _exact_evolved_invocation_document(
    *,
    value: object,
    expected_relative_path: str,
    document_schema: str,
    identity_field: str,
    name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Reopen one v3 evolved invocation document at its frozen location."""

    descriptor = _mapping(value, name=f"native v5 {name} descriptor")
    if set(descriptor) != {
        "schemaVersion",
        "documentSchemaVersion",
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    }:
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {name} descriptor schema drifted"
        )
    semantic_sha256 = _sha(
        descriptor.get("semanticSha256"), name=f"native v5 {name} semantic identity"
    )
    file_sha256 = _sha(
        descriptor.get("fileSha256"), name=f"native v5 {name} file identity"
    )
    byte_length = _nonnegative(
        descriptor.get("byteLength"), name=f"native v5 {name} byte length"
    )
    maximum_bytes = (
        _CURRENT_V5_INVOCATION_MANIFEST_LIMIT_BYTES
        if document_schema == V5_PROPOSAL_MANIFEST_SCHEMA
        else _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES
    )
    if document_schema not in {
        V5_PROPOSAL_MANIFEST_SCHEMA,
        V5_PROPOSAL_RESULT_SCHEMA,
        V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
    }:
        raise TemporalQDV5ControlPlaneError(
            "native v5 invocation document schema is unsupported"
        )
    path = _require_bound_file(
        path=descriptor.get("absolutePath"),
        raw_sha256=file_sha256,
        size_bytes=byte_length,
        name=f"native v5 {name}",
        maximum_bytes=maximum_bytes,
    )
    if (
        descriptor.get("schemaVersion") != V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA
        or descriptor.get("documentSchemaVersion") != document_schema
        or descriptor.get("relativePath") != expected_relative_path
    ):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {name} descriptor binding drifted"
        )
    document = _self_hashed(
        _read_canonical_object(
            path,
            name=f"native v5 {name}",
            maximum_bytes=maximum_bytes,
        ),
        field=identity_field,
        name=f"native v5 {name}",
    )
    if (
        document.get("schemaVersion") != document_schema
        or document.get(identity_field) != semantic_sha256
    ):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 {name} semantic binding drifted"
        )
    return (
        {
            "schemaVersion": V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
            "documentSchemaVersion": document_schema,
            "relativePath": expected_relative_path,
            "absolutePath": str(path),
            "semanticSha256": semantic_sha256,
            "fileSha256": file_sha256,
            "byteLength": byte_length,
        },
        document,
    )


def _validated_evolved_adapter_chain(
    *, construction_adapter: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Validate the v3-only outer proposal chain before Rust opens it.

    This purposefully validates documents and descriptor identities, but never
    opens the public population or fragment payload.  The prefinalizer owns
    the one permitted full attempt-stream scan.
    """

    adapter = _mapping(construction_adapter, name="native v5 evolved construction adapter")
    expected_fields = {
        "schemaVersion",
        "operation",
        "completed",
        "generationKind",
        "generationIndex",
        "generationConfigSha256",
        "authoritySha256",
        "attemptCount",
        "acceptedCandidateCount",
        "selectedEvaluationCandidateCount",
        "publicationPlanSha256",
        "publicationRequestSha256",
        "proposalResultSha256",
        "proposalReceiptSha256",
        "outputInventorySha256",
        "population",
        "evaluationPopulation",
        "generationJournal",
        "identityLedger",
        "evolvedPublicationFragments",
        "nativeV5Invocation",
        "adapterSha256",
    }
    if set(adapter) != expected_fields:
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved construction adapter schema drifted"
        )
    _self_hashed(
        adapter,
        field="adapterSha256",
        name="native v5 evolved construction adapter",
    )
    if (
        adapter.get("schemaVersion")
        != V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
        or adapter.get("operation") != V5_PROPOSAL_OPERATION
        or adapter.get("completed") is not True
        or adapter.get("generationKind") != "evolved"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved construction adapter is incompatible"
        )
    generation_index = _positive(
        adapter.get("generationIndex"), name="native v5 evolved generation index"
    )
    for field in (
        "generationConfigSha256",
        "authoritySha256",
        "publicationPlanSha256",
        "publicationRequestSha256",
        "proposalResultSha256",
        "proposalReceiptSha256",
        "outputInventorySha256",
    ):
        _sha(adapter.get(field), name=f"native v5 evolved {field}")
    for field in (
        "attemptCount",
        "acceptedCandidateCount",
        "selectedEvaluationCandidateCount",
    ):
        _nonnegative(adapter.get(field), name=f"native v5 evolved {field}")
    if (
        adapter["acceptedCandidateCount"] > adapter["attemptCount"]
        or adapter["selectedEvaluationCandidateCount"]
        > adapter["acceptedCandidateCount"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 evolved attempt accounting drifted")
    fragment = _mapping(
        adapter.get("evolvedPublicationFragments"),
        name="native v5 evolved publication fragments",
    )
    if set(fragment) != {
        "schemaVersion",
        "coreSchemaVersion",
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    } or (
        fragment.get("schemaVersion")
        != V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA
        or fragment.get("coreSchemaVersion")
        != V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved fragment descriptor schema drifted"
        )
    fragment_root = _sha(
        fragment.get("semanticSha256"), name="native v5 evolved fragment semantic identity"
    )
    expected_fragment_relative = (
        "v5-native/objects/sha256/" + fragment_root.removeprefix("sha256:") + ".json"
    )
    fragment_path_value = fragment.get("absolutePath")
    if not isinstance(fragment_path_value, str) or not Path(fragment_path_value).is_absolute():
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved fragment object path is invalid"
        )
    _nonnegative(
        fragment.get("byteLength"), name="native v5 evolved fragment byte length"
    )
    _sha(
        fragment.get("fileSha256"), name="native v5 evolved fragment file identity"
    )
    if fragment.get("relativePath") != expected_fragment_relative:
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved fragment descriptor binding drifted"
        )
    invocation = _mapping(
        adapter.get("nativeV5Invocation"), name="native v5 evolved invocation"
    )
    if set(invocation) != {
        "schemaVersion",
        "proposalManifest",
        "proposalResult",
        "proposalReceiptSha256",
        "outputInventorySha256",
    } or invocation.get("schemaVersion") != V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 evolved invocation schema drifted")
    manifest_hint = _mapping(
        invocation.get("proposalManifest"), name="native v5 evolved manifest hint"
    )
    manifest_semantic = _sha(
        manifest_hint.get("semanticSha256"), name="native v5 evolved manifest identity"
    )
    invocation_root = (
        "native-batch/v5-proposal/" + manifest_semantic.removeprefix("sha256:")
    )
    manifest_descriptor, manifest = _exact_evolved_invocation_document(
        value=manifest_hint,
        expected_relative_path=f"{invocation_root}/manifest.json",
        document_schema=V5_PROPOSAL_MANIFEST_SCHEMA,
        identity_field="manifestSha256",
        name="evolved proposal manifest",
    )
    proposal_root = _real_path(
        manifest.get("outputRoot"), name="native v5 evolved proposal root", directory=True
    )
    if (
        manifest.get("generationKind") != "evolved"
        or manifest.get("resultPath") != V5_PROPOSAL_RESULT_FILENAME
        or Path(manifest_descriptor["absolutePath"])
        != proposal_root / manifest_descriptor["relativePath"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 evolved manifest path drifted")
    expected_fragment_path = proposal_root / expected_fragment_relative
    # This object can contain the full candidate-scale attempt fragment.  Its
    # raw bytes are authenticated and reopened only by the pinned Rust
    # extractor; Python merely transports the exact receipt-addressed path.
    if fragment_path_value != str(expected_fragment_path):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved fragment descriptor escaped its proposal root"
        )
    result_descriptor, result = _exact_evolved_invocation_document(
        value=invocation.get("proposalResult"),
        expected_relative_path=f"{invocation_root}/{V5_PROPOSAL_RESULT_FILENAME}",
        document_schema=V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
        identity_field="resultSha256",
        name="evolved proposal result",
    )
    if (
        Path(result_descriptor["absolutePath"])
        != proposal_root / result_descriptor["relativePath"]
        or result.get("manifestSha256") != manifest_descriptor["semanticSha256"]
        or result_descriptor["semanticSha256"] != adapter["proposalResultSha256"]
        or invocation.get("proposalReceiptSha256") != adapter["proposalReceiptSha256"]
        or invocation.get("outputInventorySha256") != adapter["outputInventorySha256"]
        or result.get("receiptSha256") != adapter["proposalReceiptSha256"]
        or result.get("outputInventorySha256") != adapter["outputInventorySha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved invocation receipt binding drifted"
        )
    return adapter, manifest_descriptor, result_descriptor


def _validated_g0_adapter_chain(
    *, construction_adapter: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Validate the v3/v4 G0 proposal chain without opening any row payload."""

    adapter = _mapping(construction_adapter, name="native v5 G0 construction adapter")
    invocation = _mapping(adapter.get("nativeV5Invocation"), name="native v5 G0 invocation")
    if (
        adapter.get("schemaVersion") != V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
        or adapter.get("operation") != V5_PROPOSAL_OPERATION
        or adapter.get("completed") is not True
        or adapter.get("generationKind") != V5_PROPOSAL_GENERATION_G0
        or adapter.get("generationIndex") != 1
        or invocation.get("schemaVersion") != V5_G0_NATIVE_V5_INVOCATION_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError("native v5 G0 construction adapter is incompatible")
    manifest_hint = _mapping(
        invocation.get("proposalManifest"), name="native v5 G0 manifest hint"
    )
    manifest_semantic = _sha(
        manifest_hint.get("semanticSha256"), name="native v5 G0 manifest identity"
    )
    invocation_root = "native-batch/v5-proposal/" + manifest_semantic.removeprefix("sha256:")
    manifest_descriptor, manifest = _exact_evolved_invocation_document(
        value=manifest_hint,
        expected_relative_path=f"{invocation_root}/manifest.json",
        document_schema=V5_PROPOSAL_MANIFEST_SCHEMA,
        identity_field="manifestSha256",
        name="G0 proposal manifest",
    )
    proposal_root = _real_path(
        manifest.get("outputRoot"), name="native v5 G0 proposal root", directory=True
    )
    result_descriptor, result = _exact_evolved_invocation_document(
        value=invocation.get("proposalResult"),
        expected_relative_path=f"{invocation_root}/{V5_PROPOSAL_RESULT_FILENAME}",
        document_schema=V5_PROPOSAL_RESULT_SCHEMA,
        identity_field="resultSha256",
        name="G0 proposal result",
    )
    if (
        manifest.get("generationKind") != V5_PROPOSAL_GENERATION_G0
        or manifest.get("generationIndex") != 1
        or manifest.get("resultPath") != V5_PROPOSAL_RESULT_FILENAME
        or Path(manifest_descriptor["absolutePath"])
        != proposal_root / manifest_descriptor["relativePath"]
        or Path(result_descriptor["absolutePath"])
        != proposal_root / result_descriptor["relativePath"]
        or result.get("manifestSha256") != manifest_descriptor["semanticSha256"]
        or result_descriptor["semanticSha256"] != adapter.get("proposalResultSha256")
        or invocation.get("proposalReceiptSha256") != adapter.get("proposalReceiptSha256")
        or invocation.get("outputInventorySha256") != adapter.get("outputInventorySha256")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 G0 invocation binding drifted")
    try:
        checked_manifest = validate_v5_proposal_manifest(manifest)
        checked_result = validate_v5_proposal_result(result, manifest=checked_manifest)
        checked_adapter = validate_v5_generation_construction_adapter(
            adapter, result=checked_result, manifest=checked_manifest
        )
    except TemporalQDV5NativeError as exc:
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 construction adapter receipt chain drifted"
        ) from exc
    if checked_adapter != adapter:
        raise TemporalQDV5ControlPlaneError("native v5 G0 construction adapter drifted")
    return adapter, manifest_descriptor, result_descriptor


def extract_native_v5_evolved_attempt_chain(
    *,
    runtime_authority: Mapping[str, Any],
    construction_adapter: Mapping[str, Any],
    output_root: Path | str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Have the pinned prefinalizer extract the full evolved attempt stream.

    The v3 outer manifest/result/invocation chain is written as a small,
    self-hashed handoff.  Rust authenticates and streams the only candidate
    bearing data, then commits the stream and its compact accounting in a
    receipt-last object.  Python observes no attempt rows and returns only the
    receipt authority that the funnel assembler reopens in Rust.
    """

    authority = _validate_runtime_authority(runtime_authority)
    adapter, manifest_descriptor, result_descriptor = _validated_evolved_adapter_chain(
        construction_adapter=construction_adapter
    )
    root = _real_directory(output_root, name="native v5 evolved attempt root")
    adapter_path = _write_canonical_once(
        root / "evolved-construction-adapter.json",
        adapter,
        name="native v5 evolved construction adapter",
    )
    chain_input = {
        "schemaVersion": EVOLVED_ATTEMPT_CHAIN_INPUT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifest": _descriptor(
            manifest_descriptor["absolutePath"],
            maximum_bytes=_CURRENT_V5_INVOCATION_MANIFEST_LIMIT_BYTES,
        ),
        "result": _descriptor(result_descriptor["absolutePath"]),
        "adapter": _descriptor(adapter_path),
    }
    chain_input["inputSha256"] = canonical_sha256(chain_input)
    chain_path = _write_canonical_once(
        root / "evolved-attempt-chain-input.json",
        chain_input,
        name="native v5 evolved attempt chain input",
    )
    attempts_path = root / "proposal-attempts.jsonl"
    receipt_path = root / "proposal-attempts-receipt.json"
    binary = pinned_runtime_binary(runtime_authority=authority, role="rotatingPrefinalizer")
    stdout = _run_pinned(
        runtime_authority=authority,
        role="rotatingPrefinalizer",
        command=[
            str(binary),
            "extract-evolved-chain",
            str(chain_path),
            str(attempts_path),
            str(receipt_path),
        ],
        timeout_seconds=timeout_seconds,
    )
    receipt = _self_hashed(
        _read_canonical_object(
            receipt_path, name="native v5 evolved attempt stream receipt"
        ),
        field="receiptSha256",
        name="native v5 evolved attempt stream receipt",
    )
    expected_receipt_fields = {
        "schemaVersion",
        "inputSha256",
        "proposalResultSha256",
        "proposalReceiptSha256",
        "outputInventorySha256",
        "fragmentBundleSha256",
        "evaluationPopulationSha256",
        "attemptStream",
        "proposalAccounting",
        "receiptSha256",
    }
    if (
        set(receipt) != expected_receipt_fields
        or receipt.get("schemaVersion") != EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA
        or stdout != receipt
        or receipt.get("inputSha256") != chain_input["inputSha256"]
        or receipt.get("proposalResultSha256") != adapter["proposalResultSha256"]
        or receipt.get("proposalReceiptSha256") != adapter["proposalReceiptSha256"]
        or receipt.get("outputInventorySha256") != adapter["outputInventorySha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved attempt stream receipt schema/binding drifted"
        )
    receipt_stream = _mapping(
        receipt.get("attemptStream"), name="native v5 evolved attempt stream descriptor"
    )
    if set(receipt_stream) != {
        "path",
        "rawSha256",
        "sizeBytes",
        "recordCount",
        "rowSchema",
    } or receipt_stream.get("rowSchema") != EVOLVED_ATTEMPT_ROW_SCHEMA:
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved attempt stream descriptor schema drifted"
        )
    stream = {
        "path": str(attempts_path.resolve()),
        "rawSha256": _sha(
            receipt_stream.get("rawSha256"),
            name="native v5 evolved attempt stream identity",
        ),
        "sizeBytes": _nonnegative(
            receipt_stream.get("sizeBytes"),
            name="native v5 evolved attempt stream byte length",
        ),
        "recordCount": _positive(
            receipt_stream.get("recordCount"), name="native v5 evolved attempt stream count"
        ),
    }
    if (
        receipt_stream.get("path") != str(attempts_path.resolve())
        or stream["recordCount"] != adapter["attemptCount"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved attempt stream descriptor drifted"
        )
    evaluation_population = _mapping(
        adapter.get("evaluationPopulation"),
        name="native v5 evolved evaluation-population descriptor",
    )
    if receipt.get("evaluationPopulationSha256") != _sha(
        evaluation_population.get("semanticSha256"),
        name="native v5 evolved evaluation-population identity",
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evolved attempt receipt evaluation-population binding drifted"
        )
    _sha(
        receipt.get("fragmentBundleSha256"),
        name="native v5 evolved fragment-bundle identity",
    )
    accounting = _native_v5_proposal_accounting(
        receipt.get("proposalAccounting"), attempt_count=stream["recordCount"]
    )
    receipt_descriptor = _descriptor(receipt_path)
    proposal_attempt_authority = {
        "kind": "evolved",
        "receiptPath": str(receipt_path),
        "receiptFileSha256": receipt_descriptor["rawSha256"],
        "receiptSizeBytes": receipt_descriptor["sizeBytes"],
        "receiptSha256": receipt["receiptSha256"],
    }
    return {
        "adapter": adapter,
        "adapterPath": str(adapter_path),
        "chainInput": chain_input,
        "chainInputPath": str(chain_path),
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "proposalAttemptAuthority": proposal_attempt_authority,
    }


def _native_v5_g0_selected_proposal_accounting(
    value: object, *, selected_count: int
) -> dict[str, Any]:
    """Validate the receipt's compact selected and construction accounting."""

    accounting = _mapping(value, name="native v5 G0 selected proposal accounting")
    if set(accounting) != {
        "proposalAttemptCount",
        "dispositionCounts",
        "originProposalCounts",
        "g0ConstructionProposalAccounting",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 G0 proposal accounting schema drifted")
    if _positive(
        accounting.get("proposalAttemptCount"),
        name="native v5 G0 selected proposal count",
    ) != selected_count:
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 selected proposal count drifted"
        )

    def counts(name: str) -> dict[str, int]:
        raw = _mapping(accounting.get(name), name=f"native v5 G0 {name}")
        output: dict[str, int] = {}
        for key, count in raw.items():
            if not isinstance(key, str) or not key:
                raise TemporalQDV5ControlPlaneError(f"native v5 G0 {name} key is invalid")
            output[key] = _nonnegative(count, name=f"native v5 G0 {name} count")
        if sum(output.values()) != selected_count:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 G0 {name} does not account for every selected proposal"
            )
        return output

    construction = _mapping(
        accounting.get("g0ConstructionProposalAccounting"),
        name="native v5 G0 construction proposal accounting",
    )
    expected_construction_fields = {
        "proposalAttemptCount",
        "acceptedCount",
        "selectedCount",
        "attemptJournalSha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "campaignLedgerSha256",
        "compactIdentityLedgerSha256",
    }
    if set(construction) != expected_construction_fields:
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 construction accounting schema drifted"
        )
    attempts = _nonnegative(
        construction.get("proposalAttemptCount"),
        name="native v5 G0 construction attempt count",
    )
    accepted = _nonnegative(
        construction.get("acceptedCount"), name="native v5 G0 construction accepted count"
    )
    if (
        attempts < accepted
        or accepted < selected_count
        or construction.get("selectedCount") != selected_count
    ):
        raise TemporalQDV5ControlPlaneError("native v5 G0 construction accounting drifted")
    for field in expected_construction_fields - {
        "proposalAttemptCount",
        "acceptedCount",
        "selectedCount",
    }:
        _sha(construction.get(field), name=f"native v5 G0 construction {field}")
    return {
        "proposalAttemptCount": selected_count,
        "dispositionCounts": counts("dispositionCounts"),
        "originProposalCounts": counts("originProposalCounts"),
        "g0ConstructionProposalAccounting": construction,
    }


def extract_native_v5_g0_selected_attempts(
    *,
    runtime_authority: Mapping[str, Any],
    construction_adapter: Mapping[str, Any],
    output_root: Path | str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Seal the G0 selected-attempt authority through Rust only.

    The public G0 projection stream is an input to the Rust extractor, but
    Python neither opens it nor reconstructs a funnel source.  The returned
    authority names only the receipt-last selected-attempt stream.
    """

    authority = _validate_runtime_authority(runtime_authority)
    adapter, manifest_descriptor, result_descriptor = _validated_g0_adapter_chain(
        construction_adapter=construction_adapter
    )
    proposal_result = _read_canonical_object(
        result_descriptor["absolutePath"], name="native v5 G0 proposal result"
    )
    root = _real_directory(output_root, name="native v5 G0 selected-attempt root")
    adapter_path = _write_canonical_once(
        root / "g0-construction-adapter.json",
        adapter,
        name="native v5 G0 construction adapter",
    )
    chain_input = {
        "schemaVersion": G0_ATTEMPT_CHAIN_INPUT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifest": _descriptor(
            manifest_descriptor["absolutePath"],
            maximum_bytes=_CURRENT_V5_INVOCATION_MANIFEST_LIMIT_BYTES,
        ),
        "result": _descriptor(result_descriptor["absolutePath"]),
        "adapter": _descriptor(adapter_path),
    }
    chain_input["inputSha256"] = canonical_sha256(chain_input)
    chain_path = _write_canonical_once(
        root / "g0-selected-attempt-chain-input.json",
        chain_input,
        name="native v5 G0 selected-attempt chain input",
    )
    attempts_path = root / "g0-selected-proposal-attempts.jsonl"
    receipt_path = root / "g0-selected-attempts-receipt.json"
    binary = pinned_runtime_binary(runtime_authority=authority, role="rotatingPrefinalizer")
    stdout = _run_pinned(
        runtime_authority=authority,
        role="rotatingPrefinalizer",
        command=[
            str(binary),
            "extract-g0-selected-attempts",
            str(chain_path),
            str(attempts_path),
            str(receipt_path),
        ],
        timeout_seconds=timeout_seconds,
    )
    receipt = _self_hashed(
        _read_canonical_object(receipt_path, name="native v5 G0 selected-attempt receipt"),
        field="receiptSha256",
        name="native v5 G0 selected-attempt receipt",
    )
    expected_receipt_fields = {
        "schemaVersion",
        "contractVersion",
        "generationIndex",
        "inputSha256",
        "proposalManifestSha256",
        "proposalResultSha256",
        "proposalReceiptSha256",
        "outputInventorySha256",
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "selectedProjectionIndexSha256",
        "ordering",
        "attemptStream",
        "proposalAccounting",
        "receiptSha256",
    }
    g0_fragments = _mapping(
        adapter.get("g0FunnelFragments"), name="native v5 G0 funnel fragments"
    )
    g0_projection = _mapping(
        adapter.get("g0FunnelProjectionStream"),
        name="native v5 G0 funnel projection stream",
    )
    projection_stream = _mapping(
        g0_projection.get("stream"), name="native v5 G0 funnel projection stream artifact"
    )
    if (
        set(receipt) != expected_receipt_fields
        or receipt.get("schemaVersion") != G0_SELECTED_ATTEMPT_STREAM_RECEIPT_SCHEMA
        or receipt.get("contractVersion") != CONTRACT_VERSION
        or receipt.get("generationIndex") != 1
        or receipt.get("inputSha256") != chain_input["inputSha256"]
        or receipt.get("proposalManifestSha256") != manifest_descriptor["semanticSha256"]
        or receipt.get("proposalResultSha256") != adapter["proposalResultSha256"]
        or receipt.get("proposalReceiptSha256") != adapter["proposalReceiptSha256"]
        or receipt.get("outputInventorySha256") != adapter["outputInventorySha256"]
        or receipt.get("g0FunnelFragmentsSha256") != g0_fragments.get("semanticSha256")
        or receipt.get("g0FunnelProjectionStreamReceiptSha256")
        != projection_stream.get("semanticSha256")
        or receipt.get("selectedProjectionIndexSha256")
        != proposal_result.get("selectedProjectionIndexSha256")
        or receipt.get("ordering") != "candidate_id_ascending_v1"
        or stdout != receipt
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 selected-attempt receipt schema/binding drifted"
        )
    stream = _mapping(
        receipt.get("attemptStream"), name="native v5 G0 selected-attempt stream"
    )
    if set(stream) != {
        "relativePath",
        "rowSchema",
        "rawSha256",
        "sizeBytes",
        "recordCount",
    } or (
        stream.get("relativePath") != "g0-selected-proposal-attempts.jsonl"
        or stream.get("rowSchema") != G0_SELECTED_ATTEMPT_ROW_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 selected-attempt stream schema drifted"
        )
    selected_count = _positive(
        stream.get("recordCount"), name="native v5 G0 selected-attempt count"
    )
    _nonnegative(stream.get("sizeBytes"), name="native v5 G0 stream byte length")
    _sha(stream.get("rawSha256"), name="native v5 G0 stream file identity")
    if selected_count != adapter.get("selectedEvaluationCandidateCount"):
        raise TemporalQDV5ControlPlaneError(
            "native v5 G0 selected-attempt stream binding drifted"
        )
    _native_v5_g0_selected_proposal_accounting(
        receipt.get("proposalAccounting"), selected_count=selected_count
    )
    receipt_descriptor = _descriptor(receipt_path)
    return {
        "adapter": adapter,
        "adapterPath": str(adapter_path),
        "chainInput": chain_input,
        "chainInputPath": str(chain_path),
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "proposalAttemptAuthority": {
            "kind": "g0_selected",
            "receiptPath": str(receipt_path),
            "receiptFileSha256": receipt_descriptor["rawSha256"],
            "receiptSizeBytes": receipt_descriptor["sizeBytes"],
            "receiptSha256": receipt["receiptSha256"],
        },
    }


def _native_v5_proposal_accounting(
    value: object, *, attempt_count: int
) -> dict[str, Any]:
    """Validate the compact journal accounting without touching attempt rows."""

    accounting = _mapping(value, name="native v5 proposal accounting")
    if set(accounting) != {
        "proposalAttemptCount",
        "originProposalCounts",
        "dispositionCounts",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 proposal accounting schema drifted")
    if _nonnegative(
        accounting.get("proposalAttemptCount"),
        name="native v5 proposal attempt count",
    ) != attempt_count:
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal accounting attempt count drifted"
        )

    def counts(name: str) -> dict[str, int]:
        source = _mapping(accounting.get(name), name=f"native v5 {name}")
        output: dict[str, int] = {}
        for key, count in source.items():
            if not isinstance(key, str) or not key:
                raise TemporalQDV5ControlPlaneError(f"native v5 {name} key is invalid")
            output[key] = _nonnegative(count, name=f"native v5 {name} count")
        if sum(output.values()) != attempt_count:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} does not account for every proposal attempt"
            )
        return output

    return {
        "proposalAttemptCount": attempt_count,
        "originProposalCounts": counts("originProposalCounts"),
        "dispositionCounts": counts("dispositionCounts"),
    }


def assemble_native_v5_funnel_reduction_source(
    *,
    runtime_authority: Mapping[str, Any],
    proposal_attempt_authority: Mapping[str, Any],
    generation_index: int,
    evaluation_panel: Mapping[str, Any],
    campaign_seal: Mapping[str, Any],
    tail_authority: Mapping[str, Any],
    tail_result_index: Mapping[str, Any],
    minimum_total_trades: int,
    minimum_trades_per_window: int,
    output_root: Path | str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Join native attempts to the sealed tail without reopening either stream.

    The funnel source itself is candidate-scale.  The compact assembly receipt
    is the only Python-visible result and is later carried verbatim into the
    v2 prefinalizer base manifest.
    """

    authority = _validate_runtime_authority(runtime_authority)
    generation = _positive(generation_index, name="native v5 funnel generation index")
    proposal_authority = _mapping(
        proposal_attempt_authority, name="native v5 proposal-attempt authority"
    )
    if set(proposal_authority) != {
        "kind",
        "receiptPath",
        "receiptFileSha256",
        "receiptSizeBytes",
        "receiptSha256",
    } or proposal_authority.get("kind") not in {"g0_selected", "evolved"}:
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal-attempt authority schema drifted"
        )
    receipt_path = _require_bound_file(
        path=proposal_authority.get("receiptPath"),
        raw_sha256=proposal_authority.get("receiptFileSha256"),
        size_bytes=proposal_authority.get("receiptSizeBytes"),
        name="native v5 proposal-attempt receipt",
    )
    receipt = _self_hashed(
        _read_bounded_canonical_object(
            receipt_path, name="native v5 proposal-attempt receipt"
        ),
        field="receiptSha256",
        name="native v5 proposal-attempt receipt",
    )
    expected_receipt_schema = (
        G0_SELECTED_ATTEMPT_STREAM_RECEIPT_SCHEMA
        if proposal_authority["kind"] == "g0_selected"
        else EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA
    )
    if (
        receipt.get("schemaVersion") != expected_receipt_schema
        or receipt.get("receiptSha256") != proposal_authority.get("receiptSha256")
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal-attempt receipt authority drifted"
        )
    receipt_descriptor = _descriptor(receipt_path)
    proposal_authority = {
        "kind": proposal_authority["kind"],
        "receiptPath": str(receipt_path),
        "receiptFileSha256": receipt_descriptor["rawSha256"],
        "receiptSizeBytes": receipt_descriptor["sizeBytes"],
        "receiptSha256": receipt["receiptSha256"],
    }
    panel = _mapping(evaluation_panel, name="native v5 evaluation panel")
    if not isinstance(panel.get("panelId"), str) or not panel["panelId"]:
        raise TemporalQDV5ControlPlaneError("native v5 evaluation panel is invalid")
    seal = _self_hashed(
        _mapping(campaign_seal, name="native v5 campaign seal"),
        field="campaignSealSha256",
        name="native v5 campaign seal",
    )
    if seal.get("schemaVersion") != CAMPAIGN_SEAL_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 funnel campaign seal schema drifted")
    tail = validate_v5_directional_tail_authority(
        _mapping(tail_authority, name="native v5 directional tail authority"),
        runtime_authority_sha256=authority["authoritySha256"],
        generation_index=generation,
    )
    index = _mapping(tail_result_index, name="native v5 tail-result index")
    if set(index) != {
        "path",
        "relativePath",
        "rawSha256",
        "sizeBytes",
        "tailResultIndexSha256",
    } or index.get("relativePath") != "tail-result-index-v4.json":
        raise TemporalQDV5ControlPlaneError("native v5 tail-result index schema drifted")
    index_path = index.get("path")
    if not isinstance(index_path, str) or not Path(index_path).is_absolute():
        raise TemporalQDV5ControlPlaneError("native v5 tail-result index path is invalid")
    _sha(index.get("rawSha256"), name="native v5 tail-result index raw identity")
    _nonnegative(index.get("sizeBytes"), name="native v5 tail-result index byte length")
    index_sha256 = _sha(
        index.get("tailResultIndexSha256"), name="native v5 tail-result identity"
    )
    if seal.get("tailResultIndex", {}).get("sha256") != index_sha256:
        raise TemporalQDV5ControlPlaneError("native v5 tail-result index binding drifted")
    root = _real_directory(output_root, name="native v5 funnel reduction root")
    input_value = {
        "schemaVersion": FUNNEL_REDUCTION_INPUT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": generation,
        "proposalAttemptAuthority": proposal_authority,
        "evaluationPanel": panel,
        "tailAuthority": tail,
        "campaignSeal": seal,
        "tailResultIndex": {
            "path": str(index_path),
            "rawSha256": index["rawSha256"],
            "sizeBytes": index["sizeBytes"],
        },
        "minimumTotalTrades": _nonnegative(
            minimum_total_trades, name="native v5 minimum total trades"
        ),
        "minimumTradesPerWindow": _nonnegative(
            minimum_trades_per_window, name="native v5 minimum window trades"
        ),
    }
    input_value["inputSha256"] = canonical_sha256(input_value)
    input_path = _write_canonical_once(
        root / "funnel-reduction-input.json",
        input_value,
        name="native v5 funnel reduction input",
    )
    source_path = root / "funnel-reduction-source.json"
    binary = pinned_runtime_binary(runtime_authority=authority, role="rotatingPrefinalizer")
    stdout = _run_pinned(
        runtime_authority=authority,
        role="rotatingPrefinalizer",
        command=[str(binary), "assemble-funnel", str(input_path), str(source_path)],
        timeout_seconds=timeout_seconds,
    )
    if (
        set(stdout) != {"schemaVersion", "restart", "receipt"}
        or stdout.get("schemaVersion") != FUNNEL_ASSEMBLY_EXECUTION_SCHEMA
        or not isinstance(stdout.get("restart"), bool)
    ):
        raise TemporalQDV5ControlPlaneError("native v5 funnel assembly stdout drifted")
    assembly_receipt_path = root / "funnel-assembly-receipt.json"
    assembly_receipt = _self_hashed(
        _read_bounded_canonical_object(
            assembly_receipt_path, name="native v5 funnel assembly receipt"
        ),
        field="receiptSha256",
        name="native v5 funnel assembly receipt",
    )
    expected_receipt_fields = {
        "schemaVersion", "contractVersion", "generationIndex", "input", "source",
        "proposalAttemptReceiptSha256", "campaignSealSha256", "tailResultIndexSha256",
        "tailAuthoritySha256", "receiptSha256",
    }
    if (
        set(assembly_receipt) != expected_receipt_fields
        or assembly_receipt.get("schemaVersion") != FUNNEL_ASSEMBLY_RECEIPT_SCHEMA
        or assembly_receipt.get("contractVersion") != CONTRACT_VERSION
        or assembly_receipt.get("generationIndex") != generation
        or assembly_receipt.get("proposalAttemptReceiptSha256")
        != proposal_authority["receiptSha256"]
        or assembly_receipt.get("campaignSealSha256") != seal["campaignSealSha256"]
        or assembly_receipt.get("tailResultIndexSha256") != index_sha256
        or assembly_receipt.get("tailAuthoritySha256") != tail["tailAuthoritySha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 funnel assembly receipt drifted")

    def assembly_descriptor(
        value: object, *, schema: str, path: Path, semantic_field: str, semantic: str, name: str
    ) -> dict[str, Any]:
        descriptor = _mapping(value, name=f"native v5 {name} descriptor")
        expected = {"schemaVersion", "path", "rawSha256", "sizeBytes", semantic_field}
        if (
            set(descriptor) != expected
            or descriptor.get("schemaVersion") != schema
            or not native_v5_transport_path_matches(
                reported_path=descriptor.get("path"), expected_path=path
            )
            or descriptor.get(semantic_field) != semantic
        ):
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} descriptor binding drifted"
            )
        _sha(descriptor.get("rawSha256"), name=f"native v5 {name} raw identity")
        _nonnegative(descriptor.get("sizeBytes"), name=f"native v5 {name} byte length")
        _sha(descriptor.get(semantic_field), name=f"native v5 {name} identity")
        return descriptor

    input_descriptor = assembly_descriptor(
        assembly_receipt.get("input"),
        schema="temporal_qd_v5_native_funnel_assembly_input_descriptor_v1",
        path=input_path,
        semantic_field="inputSha256",
        semantic=input_value["inputSha256"],
        name="funnel input",
    )
    source_descriptor = assembly_descriptor(
        assembly_receipt.get("source"),
        schema="temporal_qd_v5_native_funnel_source_descriptor_v1",
        path=source_path,
        semantic_field="funnelSourceSha256",
        semantic=_sha(
            _mapping(
                assembly_receipt.get("source"), name="native v5 funnel source descriptor"
            ).get("funnelSourceSha256"),
            name="native v5 funnel source identity",
        ),
        name="funnel source",
    )
    stdout_receipt = _mapping(stdout.get("receipt"), name="native v5 funnel stdout receipt")
    if stdout_receipt != assembly_receipt:
        raise TemporalQDV5ControlPlaneError("native v5 funnel stdout/receipt drifted")
    assembly_receipt_descriptor = _descriptor(assembly_receipt_path)
    receipt_binding = {
        "schemaVersion": FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA,
        "path": str(assembly_receipt_path),
        "rawSha256": assembly_receipt_descriptor["rawSha256"],
        "sizeBytes": assembly_receipt_descriptor["sizeBytes"],
        "receiptSha256": assembly_receipt["receiptSha256"],
    }
    return {
        "input": input_value,
        "inputPath": str(input_path),
        "assemblyReceipt": assembly_receipt,
        "assemblyReceiptPath": str(assembly_receipt_path),
        "assemblyReceiptBinding": receipt_binding,
        "sourceDescriptor": source_descriptor,
        "inputDescriptor": input_descriptor,
    }


def _native_v5_state_basis(
    value: Mapping[str, Any], *, generation_index: int, config_sha256: str
) -> dict[str, Any]:
    """Validate the compact, candidate-free state basis consumed by Rust."""

    basis = _mapping(value, name="native v5 prefinalizer state basis")
    expected = {
        "schemaVersion",
        "configSha256",
        "generationIndex",
        "completedGenerationsSha256",
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
        "stateBasisSha256",
    }
    if set(basis) != expected or basis.get("schemaVersion") != (
        "temporal_qd_v5_generation_state_basis_v1"
    ):
        raise TemporalQDV5ControlPlaneError("native v5 state basis schema drifted")
    _self_hashed(
        basis, field="stateBasisSha256", name="native v5 prefinalizer state basis"
    )
    if (
        basis.get("configSha256") != config_sha256
        or basis.get("generationIndex") != generation_index
    ):
        raise TemporalQDV5ControlPlaneError("native v5 state basis binding drifted")
    for field in (
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
    ):
        _nonnegative(basis.get(field), name=f"native v5 state basis {field}")
    for field in (
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
    ):
        _mapping(basis.get(field), name=f"native v5 state basis {field}")
    _sha(
        basis.get("completedGenerationsSha256"),
        name="native v5 completed generation identity",
    )
    return basis


def _native_v5_completed_generation_records(
    value: object, *, state_basis: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Require the exact prior Rust-record array bound by the state basis.

    This is intentionally a compact state operation, not a legacy record
    reconstruction.  Each row remains the Rust-authored record verbatim so
    the prefinalizer can bind the same canonical root that the finalizer will
    later advance.
    """

    if not isinstance(value, list):
        raise TemporalQDV5ControlPlaneError(
            "native v5 completed generation records must be an array"
        )
    records: list[dict[str, Any]] = []
    for index, raw_record in enumerate(value):
        record = _self_hashed(
            _mapping(raw_record, name=f"native v5 completed generation record {index}"),
            field="generationRecordSha256",
            name=f"native v5 completed generation record {index}",
        )
        if record.get("schemaVersion") != GENERATION_RECORD_SCHEMA:
            raise TemporalQDV5ControlPlaneError(
                "native v5 completed generation record schema drifted"
            )
        if "nativeGenerationFinalization" in record or "nativeV5Construction" in record:
            raise TemporalQDV5ControlPlaneError(
                "native v5 completed generation record must remain an unwrapped Rust record"
            )
        records.append(record)
    if canonical_sha256(records) != state_basis["completedGenerationsSha256"]:
        raise TemporalQDV5ControlPlaneError(
            "native v5 completed generation records drifted from the state basis"
        )
    return records


def _reopen_native_v5_invocation_document(
    value: object,
    *,
    name: str,
    document_schema: str,
    semantic_field: str,
    expected_filename: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Reopen one fixed invocation control document without directory discovery.

    Native construction has already authenticated the proposal tree, but the
    prefinalizer reopens these two small documents directly.  Keep the same
    exact descriptor boundary here so a stale, aliased, or byte-substituted
    descriptor cannot be carried through Python and only fail much later in
    Rust.  This is intentionally limited to manifest/result documents; it
    never opens a proposal population, journal, or candidate row.
    """

    descriptor = _mapping(value, name=f"native v5 proposal {name} binding")
    expected_keys = {
        "schemaVersion",
        "documentSchemaVersion",
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    }
    if (
        set(descriptor) != expected_keys
        or descriptor.get("schemaVersion") != V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA
        or descriptor.get("documentSchemaVersion") != document_schema
    ):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 proposal {name} invocation binding drifted"
        )
    relative_path = descriptor.get("relativePath")
    if (
        not isinstance(relative_path, str)
        or not relative_path
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
        or relative_path.rsplit("/", 1)[-1] != expected_filename
    ):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 proposal {name} invocation relative path drifted"
        )
    absolute_path = descriptor.get("absolutePath")
    if not isinstance(absolute_path, str):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 proposal {name} invocation absolute path is invalid"
        )
    _sha(
        descriptor.get("semanticSha256"),
        name=f"native v5 proposal {name} semantic identity",
    )
    maximum_bytes = (
        _CURRENT_V5_INVOCATION_MANIFEST_LIMIT_BYTES
        if document_schema == V5_PROPOSAL_MANIFEST_SCHEMA
        else _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES
    )
    bound_path = _require_bound_file(
        path=absolute_path,
        raw_sha256=descriptor.get("fileSha256"),
        size_bytes=descriptor.get("byteLength"),
        name=f"native v5 proposal {name} invocation document",
        maximum_bytes=maximum_bytes,
    )
    document = _self_hashed(
        _read_bounded_canonical_object(
            bound_path,
            name=f"native v5 proposal {name} invocation document",
            maximum_bytes=maximum_bytes,
        ),
        field=semantic_field,
        name=f"native v5 proposal {name} invocation document",
    )
    if (
        document.get("schemaVersion") != document_schema
        or document.get(semantic_field) != descriptor["semanticSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            f"native v5 proposal {name} invocation document identity drifted"
        )
    return descriptor, document


def _native_v5_proposal_state_authority(
    value: Mapping[str, Any],
    *,
    native_v5_invocation: Mapping[str, Any],
    proposal_semantic_roots: Mapping[str, Any],
    identity_ledger_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Cross-bind the compact proposal closure required by the Rust finalizer.

    The native construction adapter was authenticated before it reached this
    bridge.  We nevertheless retain the named receipt/journal/ledger roots in
    the base manifest so a substituted compact descriptor cannot make it into
    the rotating transaction.  No proposal population or journal rows are
    opened here.
    """

    authority = _mapping(value, name="native v5 proposal state authority")
    if set(authority) != _V5_PROPOSAL_STATE_AUTHORITY_SCHEMA:
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal state authority schema drifted"
        )
    generation_kind = authority.get("generationKind")
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        expected_invocation_schema = V5_G0_NATIVE_V5_INVOCATION_SCHEMA
        if authority.get("inputIdentityLedgerSha256") is not None:
            raise TemporalQDV5ControlPlaneError(
                "native v5 G0 proposal state authority has an input ledger"
            )
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        expected_invocation_schema = V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA
        _sha(
            authority.get("inputIdentityLedgerSha256"),
            name="native v5 evolved proposal input identity ledger",
        )
    else:
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal state authority generation kind is invalid"
        )
    for field in (
        "proposalManifestSha256",
        "proposalReceiptSha256",
        "generationJournalSha256",
        "outputIdentityLedgerSha256",
        "outputIdentityLedgerFileSha256",
    ):
        _sha(authority.get(field), name=f"native v5 proposal state {field}")
    if authority.get("outputIdentityLedgerRelativePath") != (
        "proposal/v5-native/identity-ledger.json"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal output identity-ledger path drifted"
        )

    invocation = _mapping(native_v5_invocation, name="native v5 proposal invocation")
    expected_invocation_fields = {
        "schemaVersion",
        "proposalManifest",
        "proposalResult",
        "proposalReceiptSha256",
        "outputInventorySha256",
    }
    if (
        set(invocation) != expected_invocation_fields
        or invocation.get("schemaVersion") != expected_invocation_schema
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal invocation schema drifted"
        )
    proposal_manifest, manifest_document = _reopen_native_v5_invocation_document(
        invocation.get("proposalManifest"),
        name="manifest",
        document_schema=V5_PROPOSAL_MANIFEST_SCHEMA,
        semantic_field="manifestSha256",
        expected_filename="manifest.json",
    )
    proposal_result, result_document = _reopen_native_v5_invocation_document(
        invocation.get("proposalResult"),
        name="result",
        document_schema=(
            V5_PROPOSAL_RESULT_SCHEMA
            if generation_kind == V5_PROPOSAL_GENERATION_G0
            else V5_EVOLVED_PROPOSAL_RESULT_SCHEMA
        ),
        semantic_field="resultSha256",
        expected_filename=V5_PROPOSAL_RESULT_FILENAME,
    )
    invocation_root = (
        "native-batch/v5-proposal/"
        + manifest_document["manifestSha256"].removeprefix("sha256:")
    )
    if (
        proposal_manifest["relativePath"] != f"{invocation_root}/manifest.json"
        or proposal_result["relativePath"]
        != f"{invocation_root}/{V5_PROPOSAL_RESULT_FILENAME}"
        or result_document.get("manifestSha256")
        != manifest_document["manifestSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal invocation document cross-binding drifted"
        )
    if (
        proposal_manifest["semanticSha256"] != authority["proposalManifestSha256"]
        or invocation.get("proposalReceiptSha256") != authority["proposalReceiptSha256"]
        or result_document.get("receiptSha256")
        != invocation.get("proposalReceiptSha256")
        or result_document.get("outputInventorySha256")
        != invocation.get("outputInventorySha256")
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal invocation differs from the state authority"
        )
    _sha(
        invocation.get("proposalReceiptSha256"),
        name="native v5 invocation proposal receipt",
    )
    _sha(
        invocation.get("outputInventorySha256"),
        name="native v5 invocation output inventory",
    )

    roots = _mapping(proposal_semantic_roots, name="native v5 proposal semantic roots")
    if not roots:
        raise TemporalQDV5ControlPlaneError("native v5 proposal semantic roots are empty")
    for key, root in roots.items():
        if not isinstance(key, str) or not key:
            raise TemporalQDV5ControlPlaneError("native v5 proposal semantic root name is invalid")
        _sha(root, name=f"native v5 proposal semantic root {key}")
    if (
        roots.get("generationJournalSha256") != authority["generationJournalSha256"]
        or roots.get("proposalReceiptSha256") != authority["proposalReceiptSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal semantic roots differ from the state authority"
        )
    if identity_ledger_sha256 != authority["outputIdentityLedgerSha256"]:
        raise TemporalQDV5ControlPlaneError(
            "native v5 proposal identity ledger differs from the state authority"
        )
    construction = {
        "schemaVersion": "temporal_qd_v5_prefinalizer_proposal_construction_binding_v1",
        "proposalSemanticRoots": roots,
        "identityLedgerSha256": identity_ledger_sha256,
        "proposalReceiptSha256": authority["proposalReceiptSha256"],
        "identityLedger": {
            "semanticSha256": authority["outputIdentityLedgerSha256"],
            "fileSha256": authority["outputIdentityLedgerFileSha256"],
        },
        "nativeV5Invocation": invocation,
    }
    if generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        construction["inputIdentityLedgerSha256"] = authority[
            "inputIdentityLedgerSha256"
        ]
    return authority, construction


def _native_v5_archive_binding(value: object, *, name: str) -> dict[str, Any]:
    """Accept one already-certified archive transport descriptor.

    This is intentionally not a file helper.  The initial archive is
    certified by qd-archive-reducer and later archives arrive directly from a
    finalizer commit.  Reopening or hashing an archive here would recreate a
    Python archive path on the current-v5 route.
    """

    binding = _mapping(value, name=name)
    if set(binding) != {"path", "rawSha256", "sizeBytes", "archiveSha256"}:
        raise TemporalQDV5ControlPlaneError(f"{name} descriptor schema drifted")
    path = binding.get("path")
    if not isinstance(path, str) or not Path(path).is_absolute():
        raise TemporalQDV5ControlPlaneError(f"{name} path is invalid")
    _sha(binding.get("rawSha256"), name=f"{name} raw identity")
    _nonnegative(binding.get("sizeBytes"), name=f"{name} byte length")
    _sha(binding.get("archiveSha256"), name=f"{name} semantic identity")
    return dict(binding)


def _native_v5_prefinalizer_task_descriptor(
    *,
    root: Path,
    result_sha256: str,
    task_plan_sha256: str,
    task: Mapping[str, Any],
    semantic_authority_sha256: str,
    generation_index: int,
    round_index: int,
) -> dict[str, Any]:
    """Authenticate one Rust v2 non-proposal selection handoff.

    This deliberately opens only the compact, receipt-last selection document
    and its sibling receipt.  The candidate JSONL remains opaque: campaign
    freezer is the sole component that rederives the prefinalizer chain and
    opens those candidate rows.
    """

    checked_task = _self_hashed(
        _mapping(task, name="native v5 prefinalizer task"),
        field="taskSha256",
        name="native v5 prefinalizer task",
    )
    expected_task = {
        "taskOrdinal",
        "campaignRole",
        "panelId",
        "rotatingEvidenceSha256",
        "cohortSelection",
        "candidateCount",
        "candidateSetSha256",
        "sourceAuthority",
        "selectionDocumentSchema",
        "selectionDocumentRelativePath",
        "selectionReceiptRelativePath",
        "taskSha256",
    }
    if set(checked_task) != expected_task:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task schema drifted")
    ordinal = _nonnegative(
        checked_task.get("taskOrdinal"), name="native v5 prefinalizer task ordinal"
    )
    if (
        checked_task.get("campaignRole")
        not in {
            "retained_parent_current_panel",
            "prior_panel_backfill",
        }
        or not isinstance(checked_task.get("panelId"), str)
        or not checked_task["panelId"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task role/panel drifted")
    candidate_count = _positive(
        checked_task.get("candidateCount"), name="native v5 prefinalizer task count"
    )
    candidate_set = _sha(
        checked_task.get("candidateSetSha256"),
        name="native v5 prefinalizer task candidate-set identity",
    )
    rotating_evidence_sha256 = _sha(
        checked_task.get("rotatingEvidenceSha256"),
        name="native v5 prefinalizer task rotating-evidence identity",
    )
    source_authority = _mapping(
        checked_task.get("sourceAuthority"),
        name="native v5 prefinalizer task source authority",
    )
    selection = _mapping(
        checked_task.get("cohortSelection"), name="native v5 task cohort selection"
    )
    if set(selection) != {"schemaVersion", "candidateSetSha256", "candidateRows"} or (
        selection.get("schemaVersion") != PREFINALIZER_TASK_SELECTION_SCHEMA
        or selection.get("candidateSetSha256") != candidate_set
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer task selection schema drifted"
        )
    descriptor = _self_hashed(
        _mapping(selection.get("candidateRows"), name="native v5 task candidate rows"),
        field="descriptorSha256",
        name="native v5 task candidate rows",
    )
    expected_descriptor = {
        "schemaVersion",
        "path",
        "rawSha256",
        "sizeBytes",
        "recordCount",
        "rowSchema",
        "candidateSetSha256",
        "inputAuthoritySha256",
        "descriptorSha256",
    }
    expected_path = f"task-candidates/round-{round_index}-task-{ordinal}.jsonl"
    if (
        set(descriptor) != expected_descriptor
        or descriptor.get("schemaVersion") != PREFINALIZER_TASK_DESCRIPTOR_SCHEMA
        or descriptor.get("path") != expected_path
        or descriptor.get("rowSchema") != "temporal_qd_selected_rich_candidate_v1"
        or descriptor.get("candidateSetSha256") != candidate_set
        or descriptor.get("inputAuthoritySha256") != semantic_authority_sha256
        or descriptor.get("recordCount") != candidate_count
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer task descriptor schema drifted"
        )
    _sha(
        descriptor.get("rawSha256"),
        name="native v5 prefinalizer task descriptor raw identity",
    )
    _nonnegative(
        descriptor.get("sizeBytes"),
        name="native v5 prefinalizer task descriptor byte length",
    )

    expected_document_path = (
        f"task-selections/round-{round_index}-task-{ordinal}.selection.json"
    )
    expected_receipt_path = (
        f"task-selections/round-{round_index}-task-{ordinal}.receipt.json"
    )
    if (
        checked_task.get("selectionDocumentSchema")
        != PREFINALIZER_TASK_SELECTION_DOCUMENT_SCHEMA
        or checked_task.get("selectionDocumentRelativePath") != expected_document_path
        or checked_task.get("selectionReceiptRelativePath") != expected_receipt_path
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer task selection-handoff path/schema drifted"
        )
    document_path = _real_path(
        root / expected_document_path,
        name="native v5 prefinalizer task selection document",
    )
    receipt_path = _real_path(
        root / expected_receipt_path,
        name="native v5 prefinalizer task selection receipt",
    )
    document = _self_hashed(
        _read_canonical_object(
            document_path, name="native v5 prefinalizer task selection document"
        ),
        field="selectionDocumentSha256",
        name="native v5 prefinalizer task selection document",
    )
    expected_document = {
        "schemaVersion",
        "prefinalizerResultSha256",
        "taskPlanSha256",
        "taskSha256",
        "semanticAuthoritySha256",
        "generationIndex",
        "roundIndex",
        "campaignRole",
        "panelId",
        "rotatingEvidenceSha256",
        "candidateSetSha256",
        "candidateRows",
        "sourceAuthority",
        "selectionReceiptRelativePath",
        "selectionDocumentSha256",
    }
    if (
        set(document) != expected_document
        or document.get("schemaVersion") != PREFINALIZER_TASK_SELECTION_DOCUMENT_SCHEMA
        or document.get("prefinalizerResultSha256") != result_sha256
        or document.get("taskPlanSha256") != task_plan_sha256
        or document.get("taskSha256") != checked_task["taskSha256"]
        or document.get("semanticAuthoritySha256") != semantic_authority_sha256
        or document.get("generationIndex") != generation_index
        or document.get("roundIndex") != round_index
        or document.get("campaignRole") != checked_task["campaignRole"]
        or document.get("panelId") != checked_task["panelId"]
        or document.get("rotatingEvidenceSha256") != rotating_evidence_sha256
        or document.get("candidateSetSha256") != candidate_set
        or document.get("candidateRows") != descriptor
        or document.get("sourceAuthority") != source_authority
        or document.get("selectionReceiptRelativePath") != expected_receipt_path
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer task selection document binding drifted"
        )
    receipt = _self_hashed(
        _read_canonical_object(
            receipt_path, name="native v5 prefinalizer task selection receipt"
        ),
        field="receiptSha256",
        name="native v5 prefinalizer task selection receipt",
    )
    expected_receipt = {
        "schemaVersion",
        "selectionDocumentSha256",
        "prefinalizerResultSha256",
        "taskPlanSha256",
        "taskSha256",
        "semanticAuthoritySha256",
        "generationIndex",
        "roundIndex",
        "campaignRole",
        "panelId",
        "rotatingEvidenceSha256",
        "candidateSetSha256",
        "candidateRowsSha256",
        "receiptSha256",
    }
    if (
        set(receipt) != expected_receipt
        or receipt.get("schemaVersion") != PREFINALIZER_TASK_SELECTION_RECEIPT_SCHEMA
        or receipt.get("selectionDocumentSha256")
        != document["selectionDocumentSha256"]
        or receipt.get("prefinalizerResultSha256") != result_sha256
        or receipt.get("taskPlanSha256") != task_plan_sha256
        or receipt.get("taskSha256") != checked_task["taskSha256"]
        or receipt.get("semanticAuthoritySha256") != semantic_authority_sha256
        or receipt.get("generationIndex") != generation_index
        or receipt.get("roundIndex") != round_index
        or receipt.get("campaignRole") != checked_task["campaignRole"]
        or receipt.get("panelId") != checked_task["panelId"]
        or receipt.get("rotatingEvidenceSha256") != rotating_evidence_sha256
        or receipt.get("candidateSetSha256") != candidate_set
        or receipt.get("candidateRowsSha256") != descriptor["descriptorSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer task selection receipt binding drifted"
        )
    return {
        "campaignRole": checked_task["campaignRole"],
        "panelId": checked_task["panelId"],
        "taskOrdinal": ordinal,
        "candidateCount": candidate_count,
        "candidateSetSha256": candidate_set,
        "selectionSha256": document["selectionDocumentSha256"],
        "selectionPath": str(document_path),
        "selectionReceiptPath": str(receipt_path),
        "selectionReceiptSha256": receipt["receiptSha256"],
        "selectionDescriptor": descriptor,
    }


def _validated_native_v5_prefinalizer_result(
    *, root: Path, value: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    """Authenticate a receipt-last prefinalizer result and task sidecars."""

    result = _self_hashed(
        _mapping(value, name="native v5 prefinalizer result"),
        field="resultSha256",
        name="native v5 prefinalizer result",
    )
    expected = {
        "schemaVersion",
        "contractVersion",
        "baseManifestSha256",
        "manifestSha256",
        "semanticAuthoritySha256",
        "roundIndex",
        "previousResultSha256",
        "generationIndex",
        "status",
        "admittedCampaignLedger",
        "cohort",
        "provisional",
        "panelCoverage",
        "taskPlan",
        "funnelReductionSource",
        "selectedRichMembers",
        "finalizerSource",
        "finalizerManifest",
        "resultSha256",
    }
    if set(result) != expected or result.get("schemaVersion") != PREFINALIZER_RESULT_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer result schema drifted")
    if (
        result.get("contractVersion") != CONTRACT_VERSION
        or result.get("manifestSha256") != manifest.get("manifestSha256")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer result binding drifted")
    if manifest.get("schemaVersion") == PREFINALIZER_BASE_MANIFEST_SCHEMA:
        expected_base_manifest_sha256 = _sha(
            manifest.get("manifestSha256"), name="native v5 base manifest identity"
        )
        expected_previous_result_sha256: str | None = None
        expected_round_index = 0
        expected_generation_index = _positive(
            manifest.get("generationIndex"), name="native v5 base generation index"
        )
        expected_semantic_authority_sha256 = _sha(
            manifest.get("semanticAuthoritySha256"),
            name="native v5 base semantic authority",
        )
        finalizer_output_root_value = manifest.get("finalizerOutputRoot")
    elif manifest.get("schemaVersion") == PREFINALIZER_RESUME_MANIFEST_SCHEMA:
        base_manifest, _base_path = _read_bound_canonical_object(
            manifest.get("baseManifestBinding"), name="native v5 resume base manifest"
        )
        base_manifest = _self_hashed(
            base_manifest,
            field="manifestSha256",
            name="native v5 resume base manifest",
        )
        if base_manifest.get("schemaVersion") != PREFINALIZER_BASE_MANIFEST_SCHEMA:
            raise TemporalQDV5ControlPlaneError(
                "native v5 resume base manifest schema drifted"
            )
        previous_result, _previous_path = _read_bound_canonical_object(
            manifest.get("previousResultBinding"),
            name="native v5 resume previous result",
        )
        previous_result = _self_hashed(
            previous_result,
            field="resultSha256",
            name="native v5 resume previous result",
        )
        if (
            previous_result.get("schemaVersion") != PREFINALIZER_RESULT_SCHEMA
            or previous_result.get("baseManifestSha256")
            != base_manifest.get("manifestSha256")
            or previous_result.get("status") == "ready_for_finalizer"
        ):
            raise TemporalQDV5ControlPlaneError(
                "native v5 resume previous result binding drifted"
            )
        expected_base_manifest_sha256 = _sha(
            base_manifest.get("manifestSha256"),
            name="native v5 resume base manifest identity",
        )
        expected_previous_result_sha256 = _sha(
            previous_result.get("resultSha256"),
            name="native v5 resume previous result identity",
        )
        expected_round_index = _nonnegative(
            previous_result.get("roundIndex"),
            name="native v5 resume previous round",
        ) + 1
        expected_generation_index = _positive(
            base_manifest.get("generationIndex"),
            name="native v5 resume generation index",
        )
        expected_semantic_authority_sha256 = _sha(
            base_manifest.get("semanticAuthoritySha256"),
            name="native v5 resume semantic authority",
        )
        finalizer_output_root_value = base_manifest.get("finalizerOutputRoot")
    else:
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer manifest schema is incompatible"
        )
    if not isinstance(finalizer_output_root_value, str) or not finalizer_output_root_value:
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer finalizer output root is invalid"
        )
    finalizer_output_root = _real_path(
        finalizer_output_root_value,
        name="native v5 prefinalizer finalizer output root",
        directory=True,
    )
    if finalizer_output_root_value != str(finalizer_output_root):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer finalizer output root is not canonical"
        )
    if (
        result.get("baseManifestSha256") != expected_base_manifest_sha256
        or result.get("previousResultSha256") != expected_previous_result_sha256
        or result.get("roundIndex") != expected_round_index
        or result.get("generationIndex") != expected_generation_index
        or result.get("semanticAuthoritySha256")
        != expected_semantic_authority_sha256
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer result chain is discontinuous"
        )
    status = result.get("status")
    if status not in {
        "awaiting_retained_parent_current_panel",
        "awaiting_prior_panel_backfill",
        "ready_for_finalizer",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer result status is invalid")
    task_plan = _self_hashed(
        _mapping(result.get("taskPlan"), name="native v5 prefinalizer task plan"),
        field="taskPlanSha256",
        name="native v5 prefinalizer task plan",
    )
    if set(task_plan) != {
        "schemaVersion",
        "contractVersion",
        "semanticAuthoritySha256",
        "generationIndex",
        "roundIndex",
        "phase",
        "tasks",
        "taskCount",
        "taskPlanSha256",
    } or (
        task_plan.get("schemaVersion") != PREFINALIZER_TASK_PLAN_SCHEMA
        or task_plan.get("contractVersion") != CONTRACT_VERSION
        or task_plan.get("semanticAuthoritySha256")
        != result["semanticAuthoritySha256"]
        or task_plan.get("generationIndex") != result["generationIndex"]
        or task_plan.get("roundIndex") != result["roundIndex"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task plan drifted")
    round_index = _nonnegative(
        task_plan.get("roundIndex"), name="native v5 prefinalizer round index"
    )
    tasks = task_plan.get("tasks")
    if not isinstance(tasks, list) or task_plan.get("taskCount") != len(tasks):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task count drifted")
    selections = tuple(
        _native_v5_prefinalizer_task_descriptor(
            root=root,
            result_sha256=result["resultSha256"],
            task_plan_sha256=task_plan["taskPlanSha256"],
            task=_mapping(task, name="native v5 prefinalizer task"),
            semantic_authority_sha256=result["semanticAuthoritySha256"],
            generation_index=result["generationIndex"],
            round_index=round_index,
        )
        for task in tasks
    )
    if len({(item["campaignRole"], item["panelId"]) for item in selections}) != len(
        selections
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task pairs repeat")
    funnel = _self_hashed(
        _mapping(result.get("funnelReductionSource"), name="native v5 funnel source"),
        field="funnelSourceSha256",
        name="native v5 funnel source",
    )
    if funnel.get("schemaVersion") != FUNNEL_REDUCTION_SOURCE_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer funnel source drifted")
    if status == "ready_for_finalizer":
        if tasks or result.get("selectedRichMembers") is None:
            raise TemporalQDV5ControlPlaneError(
                "ready native v5 prefinalizer result lacks a closed finalizer basis"
            )
        selected = _self_hashed(
            _mapping(
                result.get("selectedRichMembers"),
                name="native v5 selected rich-member descriptor",
            ),
            field="selectedRichMembersSha256",
            name="native v5 selected rich-member descriptor",
        )
        selected_members = selected.get("members")
        if (
            selected.get("schemaVersion") != "temporal_qd_selected_rich_members_v1"
            or selected.get("generationIndex") != result["generationIndex"]
            or not isinstance(selected_members, list)
            or selected.get("memberCount") != len(selected_members)
        ):
            raise TemporalQDV5ControlPlaneError(
                "ready native v5 selected rich-member boundary drifted"
            )
        for name, identity_field, filename, schema in (
            (
                "finalizer source",
                "sourceSha256",
                "source.json",
                "temporal_qd_generation_finalization_source_v2",
            ),
            (
                "finalizer manifest",
                "manifestSha256",
                "manifest.json",
                "temporal_qd_generation_finalization_manifest_v2",
            ),
        ):
            binding = _mapping(
                result.get(
                    "finalizerSource"
                    if name == "finalizer source"
                    else "finalizerManifest"
                ),
                name=f"native v5 {name} binding",
            )
            expected_path = finalizer_output_root / filename
            if set(binding) != {"path", "sha256"} or binding.get("path") != str(
                expected_path
            ):
                raise TemporalQDV5ControlPlaneError(f"native v5 {name} binding drifted")
            document = _self_hashed(
                _read_canonical_object(expected_path, name=f"native v5 {name}"),
                field=identity_field,
                name=f"native v5 {name}",
            )
            if document.get("schemaVersion") != schema or document.get(identity_field) != binding.get("sha256"):
                raise TemporalQDV5ControlPlaneError(f"native v5 {name} identity drifted")
    elif (
        result.get("selectedRichMembers") is not None
        or result.get("finalizerSource") is not None
        or result.get("finalizerManifest") is not None
        or not selections
    ):
        raise TemporalQDV5ControlPlaneError(
            "awaiting native v5 prefinalizer result has an invalid finalizer boundary"
        )
    return result, selections


def build_native_v5_prefinalizer_base_manifest(
    *,
    runtime_authority: Mapping[str, Any],
    output_root: Path | str,
    generation_index: int,
    supervisor_config_sha256: str,
    generation_config_sha256: str,
    state_basis: Mapping[str, Any],
    completed_generation_records: list[Mapping[str, Any]],
    proposal_state_authority: Mapping[str, Any],
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    proposal_semantic_roots: Mapping[str, Any],
    identity_ledger_sha256: str,
    native_v5_invocation: Mapping[str, Any],
    funnel_reduction_input: Mapping[str, Any],
    funnel_assembly_receipt_binding: Mapping[str, Any],
    previous_parent_archive_binding: Mapping[str, Any],
    previous_cumulative_archive_binding: Mapping[str, Any] | None,
    proposal_campaign_receipt_path: Path | str,
    finalizer_output_root: Path | str,
) -> dict[str, Any]:
    """Write the frozen base manifest without opening population-shaped data."""

    authority = _validate_runtime_authority(runtime_authority)
    root = _real_directory(output_root, name="native v5 prefinalizer base root")
    finalizer_root = _real_directory(
        finalizer_output_root, name="native v5 prefinalizer finalizer output root"
    )
    finalizer_root_transport = _native_v5_rust_canonical_directory_transport(
        finalizer_root
    )
    generation = _positive(generation_index, name="native v5 prefinalizer generation")
    config_sha = _sha(
        supervisor_config_sha256, name="native v5 supervisor config identity"
    )
    generation_config_sha = _sha(
        generation_config_sha256, name="native v5 generation config identity"
    )
    state = _native_v5_state_basis(
        state_basis, generation_index=generation, config_sha256=config_sha
    )
    completed_records = _native_v5_completed_generation_records(
        completed_generation_records, state_basis=state
    )
    rotating = _self_hashed(
        _mapping(rotating_evidence, name="native v5 rotating evidence"),
        field="rotatingEvidenceSha256",
        name="native v5 rotating evidence",
    )
    policy = _self_hashed(
        _mapping(archive_policy_authority, name="native v5 archive policy authority"),
        field="policyBindingSha256",
        name="native v5 archive policy authority",
    )
    identity_ledger = _sha(
        identity_ledger_sha256, name="native v5 proposal identity ledger"
    )
    state_authority, construction_binding = _native_v5_proposal_state_authority(
        proposal_state_authority,
        native_v5_invocation=native_v5_invocation,
        proposal_semantic_roots=proposal_semantic_roots,
        identity_ledger_sha256=identity_ledger,
    )
    semantic_roots = construction_binding["proposalSemanticRoots"]
    funnel_input = _self_hashed(
        _mapping(funnel_reduction_input, name="native v5 funnel reduction input"),
        field="inputSha256",
        name="native v5 funnel reduction input",
    )
    if (
        set(funnel_input)
        != {
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "proposalAttemptAuthority",
            "evaluationPanel",
            "tailAuthority",
            "campaignSeal",
            "tailResultIndex",
            "minimumTotalTrades",
            "minimumTradesPerWindow",
            "inputSha256",
        }
        or funnel_input.get("schemaVersion") != FUNNEL_REDUCTION_INPUT_SCHEMA
        or funnel_input.get("contractVersion") != CONTRACT_VERSION
        or funnel_input.get("generationIndex") != generation
    ):
        raise TemporalQDV5ControlPlaneError("native v5 funnel input schema drifted")
    funnel_receipt_binding = _mapping(
        funnel_assembly_receipt_binding,
        name="native v5 funnel assembly receipt binding",
    )
    if set(funnel_receipt_binding) != {
        "schemaVersion", "path", "rawSha256", "sizeBytes", "receiptSha256"
    } or funnel_receipt_binding.get("schemaVersion") != FUNNEL_ASSEMBLY_RECEIPT_BINDING_SCHEMA:
        raise TemporalQDV5ControlPlaneError(
            "native v5 funnel assembly receipt binding schema drifted"
        )
    receipt_path_value = funnel_receipt_binding.get("path")
    if not isinstance(receipt_path_value, str) or not Path(receipt_path_value).is_absolute():
        raise TemporalQDV5ControlPlaneError(
            "native v5 funnel assembly receipt path is invalid"
        )
    _sha(
        funnel_receipt_binding.get("rawSha256"),
        name="native v5 funnel assembly receipt raw identity",
    )
    _nonnegative(
        funnel_receipt_binding.get("sizeBytes"),
        name="native v5 funnel assembly receipt byte length",
    )
    _sha(
        funnel_receipt_binding.get("receiptSha256"),
        name="native v5 funnel assembly receipt identity",
    )
    proposal_receipt = _self_hashed(
        _read_canonical_object(
            proposal_campaign_receipt_path, name="native v5 proposal campaign receipt"
        ),
        field="receiptSha256",
        name="native v5 proposal campaign receipt",
    )
    if proposal_receipt.get("schemaVersion") != CAMPAIGN_RECEIPT_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 proposal campaign receipt schema drifted")
    parent_binding = _native_v5_archive_binding(
        previous_parent_archive_binding, name="native v5 previous parent archive"
    )
    if previous_cumulative_archive_binding is None:
        cumulative_binding: dict[str, Any] | None = None
        cumulative_semantic = "sha256:" + "0" * 64
    else:
        cumulative_binding = _native_v5_archive_binding(
            previous_cumulative_archive_binding,
            name="native v5 previous cumulative archive",
        )
        cumulative_semantic = cumulative_binding["archiveSha256"]
    supervisor_binding = {
        "schemaVersion": "temporal_qd_v5_prefinalizer_supervisor_binding_v1",
        "supervisorConfigSha256": config_sha,
        "generationConfigSha256": generation_config_sha,
        "rotatingEvidence": rotating,
        "archivePolicyAuthority": policy,
    }
    # The compact input is the authority.  Its native receipt points at the
    # candidate-scale deterministic source, which only Rust may reopen.
    construction_binding["funnelReductionInput"] = funnel_input
    construction_binding["funnelAssemblyReceiptBinding"] = funnel_receipt_binding
    semantic_authority = canonical_sha256(
        {
            "schemaVersion": "temporal_qd_v5_rotating_prefinalizer_semantic_authority_v1",
            "generationIndex": generation,
            "supervisorConfigSha256": config_sha,
            "generationConfigSha256": generation_config_sha,
            "stateBasisSha256": state["stateBasisSha256"],
            "completedGenerationRecordsSha256": canonical_sha256(completed_records),
            "proposalStateAuthority": state_authority,
            "proposalSemanticRoots": semantic_roots,
            "identityLedgerSha256": identity_ledger,
            "previousParentArchiveSha256": parent_binding["archiveSha256"],
            "previousCumulativeArchiveSha256": cumulative_semantic,
            "proposalCampaignSemanticReceiptSha256": proposal_receipt[
                "semanticReceiptSha256"
            ],
        }
    )
    manifest = {
        "schemaVersion": PREFINALIZER_BASE_MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": "prepare_native_v5_rotating_generation",
        "generationIndex": generation,
        "supervisorConfigBinding": supervisor_binding,
        "stateBasis": state,
        "completedGenerationRecords": completed_records,
        "proposalStateAuthority": state_authority,
        "proposalConstructionBinding": construction_binding,
        "previousParentArchiveBinding": parent_binding,
        "previousCumulativeArchiveBinding": cumulative_binding,
        "proposalCampaignReceiptBinding": _descriptor(proposal_campaign_receipt_path),
        "finalizerOutputRoot": finalizer_root_transport,
        "runtimeAuthoritySha256": authority["authoritySha256"],
        "semanticAuthoritySha256": semantic_authority,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    path = _write_canonical_once(
        root / "manifest.json", manifest, name="native v5 prefinalizer base manifest"
    )
    return {"manifest": manifest, "manifestPath": str(path)}


def _validated_native_v5_prefinalizer_execution_receipt(
    *,
    value: Mapping[str, Any],
    manifest_path: Path,
    output_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate the bounded v2 prefinalizer receipt without opening result rows.

    The internal result, selection documents, and finalizer source are all
    reopenable by Rust.  Python only needs their sealed descriptors to decide
    whether another campaign round is required and to invoke the finalizer.
    """

    receipt = _self_hashed(
        _mapping(value, name="native v5 prefinalizer execution receipt"),
        field="receiptSha256",
        name="native v5 prefinalizer execution receipt",
    )
    expected = {
        "schemaVersion", "contractVersion", "inputManifest", "internalResult", "status",
        "generationIndex", "roundIndex", "semanticAuthoritySha256", "baseManifestSha256",
        "previousResultSha256", "taskPlanSha256", "taskCount", "taskSelections",
        "finalizerSource", "finalizerManifest", "receiptSha256",
    }
    if (
        set(receipt) != expected
        or receipt.get("schemaVersion") != PREFINALIZER_EXECUTION_RECEIPT_SCHEMA
        or receipt.get("contractVersion") != CONTRACT_VERSION
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer execution receipt schema drifted"
        )

    def descriptor(
        value: object,
        *,
        schema: str,
        semantic_field: str,
        expected_path: Path | None,
        name: str,
    ) -> dict[str, Any]:
        item = _mapping(value, name=f"native v5 {name} descriptor")
        if set(item) != {
            "schemaVersion", "path", "rawSha256", "sizeBytes", semantic_field
        } or item.get("schemaVersion") != schema:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} descriptor schema drifted"
            )
        path_value = item.get("path")
        if not isinstance(path_value, str) or not Path(path_value).is_absolute():
            raise TemporalQDV5ControlPlaneError(f"native v5 {name} path is invalid")
        if expected_path is not None and not native_v5_transport_path_matches(
            path_value, expected_path
        ):
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} path binding drifted"
            )
        _sha(item.get("rawSha256"), name=f"native v5 {name} raw identity")
        _nonnegative(item.get("sizeBytes"), name=f"native v5 {name} byte length")
        _sha(item.get(semantic_field), name=f"native v5 {name} identity")
        return item

    input_manifest = descriptor(
        receipt.get("inputManifest"),
        schema="temporal_qd_v5_prefinalizer_input_manifest_descriptor_v1",
        semantic_field="manifestSha256",
        expected_path=manifest_path,
        name="prefinalizer input manifest",
    )
    internal_result = descriptor(
        receipt.get("internalResult"),
        schema="temporal_qd_v5_prefinalizer_internal_result_descriptor_v1",
        semantic_field="resultSha256",
        expected_path=output_root / "result.json",
        name="prefinalizer internal result",
    )
    _positive(receipt.get("generationIndex"), name="native v5 prefinalizer generation")
    _nonnegative(receipt.get("roundIndex"), name="native v5 prefinalizer round")
    for field in (
        "semanticAuthoritySha256", "baseManifestSha256", "taskPlanSha256"
    ):
        _sha(receipt.get(field), name=f"native v5 prefinalizer {field}")
    previous = receipt.get("previousResultSha256")
    if previous is not None:
        _sha(previous, name="native v5 prefinalizer previous result identity")
    if receipt.get("status") not in {
        "awaiting_retained_parent_current_panel",
        "awaiting_prior_panel_backfill",
        "ready_for_finalizer",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer status is invalid")
    task_count = _nonnegative(receipt.get("taskCount"), name="native v5 prefinalizer task count")
    raw_tasks = receipt.get("taskSelections")
    if not isinstance(raw_tasks, list) or len(raw_tasks) != task_count:
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer task selections drifted")

    selections: list[dict[str, Any]] = []
    selection_root_text = str(output_root)
    if os.name == "nt" and selection_root_text.startswith("\\\\?\\"):
        selection_root = Path(selection_root_text[4:])
        if not native_v5_transport_path_matches(selection_root_text, selection_root):
            raise TemporalQDV5ControlPlaneError(
                "native v5 prefinalizer selection root is invalid"
            )
    else:
        selection_root = Path(selection_root_text)
    for ordinal, raw in enumerate(raw_tasks):
        task = _mapping(raw, name="native v5 prefinalizer task selection")
        expected_task = {
            "taskOrdinal", "campaignRole", "panelId", "candidateCount",
            "candidateSetSha256", "selectionDocument", "selectionReceipt",
        }
        if set(task) != expected_task or task.get("taskOrdinal") != ordinal:
            raise TemporalQDV5ControlPlaneError(
                "native v5 prefinalizer task selection ordering drifted"
            )
        if task.get("campaignRole") not in {
            "retained_parent_current_panel", "prior_panel_backfill"
        } or not isinstance(task.get("panelId"), str) or not task["panelId"]:
            raise TemporalQDV5ControlPlaneError(
                "native v5 prefinalizer task selection role/panel is invalid"
            )
        candidate_count = _positive(
            task.get("candidateCount"), name="native v5 prefinalizer task candidate count"
        )
        candidate_set = _sha(
            task.get("candidateSetSha256"),
            name="native v5 prefinalizer task candidate-set identity",
        )
        document = descriptor(
            task.get("selectionDocument"),
            schema="temporal_qd_v5_prefinalizer_selection_document_descriptor_v1",
            semantic_field="selectionDocumentSha256",
            expected_path=None,
            name="prefinalizer task selection document",
        )
        selection_receipt = descriptor(
            task.get("selectionReceipt"),
            schema="temporal_qd_v5_prefinalizer_selection_receipt_descriptor_v1",
            semantic_field="receiptSha256",
            expected_path=None,
            name="prefinalizer task selection receipt",
        )
        for label, item in (("document", document), ("receipt", selection_receipt)):
            reported_task_path = str(item["path"])
            if os.name == "nt" and reported_task_path.startswith("\\\\?\\"):
                task_path = Path(reported_task_path[4:])
                if not native_v5_transport_path_matches(reported_task_path, task_path):
                    raise TemporalQDV5ControlPlaneError(
                        f"native v5 prefinalizer task selection {label} path is invalid"
                    )
            else:
                task_path = Path(reported_task_path)
            try:
                task_path.relative_to(selection_root)
            except ValueError as exc:
                raise TemporalQDV5ControlPlaneError(
                    f"native v5 prefinalizer task selection {label} escapes its root"
                ) from exc
        selections.append(
            {
                "taskOrdinal": ordinal,
                "campaignRole": task["campaignRole"],
                "panelId": task["panelId"],
                "candidateCount": candidate_count,
                "candidateSetSha256": candidate_set,
                "selectionPath": document["path"],
                "selectionSha256": document["selectionDocumentSha256"],
                "selectionReceiptPath": selection_receipt["path"],
                "selectionReceiptSha256": selection_receipt["receiptSha256"],
            }
        )
    if receipt["status"] == "ready_for_finalizer":
        if selections or receipt.get("finalizerSource") is None or receipt.get("finalizerManifest") is None:
            raise TemporalQDV5ControlPlaneError(
                "ready native v5 prefinalizer receipt lacks its finalizer boundary"
            )
        descriptor(
            receipt.get("finalizerSource"),
            schema="temporal_qd_v5_prefinalizer_finalizer_source_descriptor_v1",
            semantic_field="sourceSha256",
            expected_path=None,
            name="prefinalizer finalizer source",
        )
        descriptor(
            receipt.get("finalizerManifest"),
            schema="temporal_qd_v5_prefinalizer_finalizer_manifest_descriptor_v1",
            semantic_field="manifestSha256",
            expected_path=None,
            name="prefinalizer finalizer manifest",
        )
    elif (
        not selections
        or receipt.get("finalizerSource") is not None
        or receipt.get("finalizerManifest") is not None
    ):
        raise TemporalQDV5ControlPlaneError(
            "awaiting native v5 prefinalizer receipt has an invalid finalizer boundary"
        )
    return receipt, selections


def build_native_v5_prefinalizer_resume_manifest(
    *,
    runtime_authority: Mapping[str, Any],
    output_root: Path | str,
    base_manifest_path: Path | str,
    previous_execution_receipt: Mapping[str, Any],
    new_campaign_receipt_paths: tuple[Path | str, ...],
) -> dict[str, Any]:
    """Write one immutable native resume manifest from sealed receipt paths."""

    authority = _validate_runtime_authority(runtime_authority)
    root = _real_directory(output_root, name="native v5 prefinalizer resume root")
    base = _self_hashed(
        _read_canonical_object(base_manifest_path, name="native v5 prefinalizer base manifest"),
        field="manifestSha256",
        name="native v5 prefinalizer base manifest",
    )
    if base.get("schemaVersion") != PREFINALIZER_BASE_MANIFEST_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 resume base manifest schema drifted")
    previous, _selections = _validated_native_v5_prefinalizer_execution_receipt(
        value=previous_execution_receipt,
        manifest_path=_real_path(
            previous_execution_receipt.get("inputManifest", {}).get("path")
            if isinstance(previous_execution_receipt.get("inputManifest"), Mapping)
            else "",
            name="native v5 prior prefinalizer manifest",
        ),
        output_root=_real_directory(
            Path(
                str(
                    _mapping(
                        previous_execution_receipt.get("internalResult"),
                        name="native v5 prior internal result descriptor",
                    ).get("path")
                )
            ).parent,
            name="native v5 prior prefinalizer root",
        ),
    )
    if (
        previous.get("baseManifestSha256") != base["manifestSha256"]
        or previous.get("status") == "ready_for_finalizer"
    ):
        raise TemporalQDV5ControlPlaneError("native v5 resume receipt binding drifted")
    round_index = _nonnegative(
        previous.get("roundIndex"), name="native v5 prefinalizer previous round"
    ) + 1
    if not new_campaign_receipt_paths:
        raise TemporalQDV5ControlPlaneError("native v5 resume requires a new campaign receipt")
    receipts: list[dict[str, Any]] = []
    pairs: set[tuple[str, str]] = set()
    receipt_ids: set[str] = set()
    for receipt_path in new_campaign_receipt_paths:
        receipt = _self_hashed(
            _read_canonical_object(receipt_path, name="native v5 resumed campaign receipt"),
            field="receiptSha256",
            name="native v5 resumed campaign receipt",
        )
        pair = (str(receipt.get("campaignRole")), str(receipt.get("panelId")))
        if (
            receipt.get("schemaVersion") != CAMPAIGN_RECEIPT_SCHEMA
            or receipt.get("generationIndex") != base.get("generationIndex")
            or not pair[0]
            or not pair[1]
            or pair in pairs
            or receipt["receiptSha256"] in receipt_ids
        ):
            raise TemporalQDV5ControlPlaneError("native v5 resume receipt set drifted")
        pairs.add(pair)
        receipt_ids.add(receipt["receiptSha256"])
        receipts.append(_descriptor(receipt_path))
    manifest = {
        "schemaVersion": PREFINALIZER_RESUME_MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": "resume_native_v5_rotating_generation",
        "baseManifestBinding": _descriptor(base_manifest_path),
        "roundIndex": round_index,
        "previousResultBinding": {
            "path": previous["internalResult"]["path"],
            "rawSha256": previous["internalResult"]["rawSha256"],
            "sizeBytes": previous["internalResult"]["sizeBytes"],
        },
        "newCampaignReceiptBindings": receipts,
        "runtimeAuthoritySha256": authority["authoritySha256"],
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    path = _write_canonical_once(
        root / "manifest.json", manifest, name="native v5 prefinalizer resume manifest"
    )
    return {"manifest": manifest, "manifestPath": str(path)}


def run_native_v5_rotating_prefinalizer(
    *,
    runtime_authority: Mapping[str, Any],
    manifest_path: Path | str,
    timeout_seconds: int = 3_600,
) -> dict[str, Any]:
    """Execute a v2 prefinalizer transaction through its compact receipt.

    A full 1,024-member generation authenticates and reduces tens of
    megabytes of sealed member and bundle material.  Production G0 evidence
    shows that this bounded Rust pass can legitimately take roughly half an
    hour on the controller host, so the historical five-minute helper default
    is not an admissible generation-scale timeout.
    """

    authority = _validate_runtime_authority(runtime_authority)
    path = _real_path(manifest_path, name="native v5 prefinalizer manifest")
    root = _real_path(path.parent, name="native v5 prefinalizer root", directory=True)
    manifest = _self_hashed(
        _read_bounded_canonical_object(path, name="native v5 prefinalizer manifest"),
        field="manifestSha256",
        name="native v5 prefinalizer manifest",
    )
    if (
        manifest.get("schemaVersion")
        not in {PREFINALIZER_BASE_MANIFEST_SCHEMA, PREFINALIZER_RESUME_MANIFEST_SCHEMA}
        or manifest.get("contractVersion") != CONTRACT_VERSION
        or manifest.get("runtimeAuthoritySha256") != authority["authoritySha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer manifest is incompatible")
    binary = pinned_runtime_binary(runtime_authority=authority, role="rotatingPrefinalizer")
    execution = _run_pinned(
        runtime_authority=authority,
        role="rotatingPrefinalizer",
        command=[str(binary), str(path)],
        timeout_seconds=timeout_seconds,
    )
    if (
        set(execution) != {"schemaVersion", "restart", "receipt"}
        or execution.get("schemaVersion") != PREFINALIZER_EXECUTION_SCHEMA
        or not isinstance(execution.get("restart"), bool)
    ):
        raise TemporalQDV5ControlPlaneError("native v5 prefinalizer execution schema drifted")
    receipt_path = root / "execution-receipt.json"
    receipt, selections = _validated_native_v5_prefinalizer_execution_receipt(
        value=_read_bounded_canonical_object(
            receipt_path, name="native v5 prefinalizer execution receipt"
        ),
        manifest_path=path,
        output_root=root,
    )
    if execution.get("receipt") != receipt:
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer stdout/receipt drifted"
        )
    if receipt["inputManifest"]["manifestSha256"] != manifest["manifestSha256"]:
        raise TemporalQDV5ControlPlaneError(
            "native v5 prefinalizer receipt manifest binding drifted"
        )
    return {
        "manifest": manifest,
        "manifestPath": str(path),
        "execution": execution,
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "internalResultBinding": receipt["internalResult"],
        "taskSelections": selections,
        "outputRoot": str(root),
    }


def _validated_native_v5_state_application_sidecar(
    *,
    root: Path,
    source_sha256: str,
    manifest: Mapping[str, Any],
    commit: Mapping[str, Any],
    generation_record: Mapping[str, Any],
    state_patch: Mapping[str, Any],
    runtime_authority: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Authenticate the compact direct-state handoff authored by Rust.

    The sidecar deliberately binds only state and proposal identities.  It is
    not a Python artifact ledger and this verifier never opens archive,
    population, selected-member, or evidence-row payloads.
    """

    authority = _validate_runtime_authority(runtime_authority)
    record = _self_hashed(
        generation_record,
        field="generationRecordSha256",
        name="native v5 direct generation record",
    )
    patch = _self_hashed(
        state_patch,
        field="statePatchSha256",
        name="native v5 direct generation state patch",
    )
    if (
        record.get("schemaVersion") != GENERATION_RECORD_SCHEMA
        or patch.get("schemaVersion") != GENERATION_STATE_PATCH_SCHEMA
        or patch.get("generationRecord") != record
        or patch.get("generationRecordSha256") != record["generationRecordSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 direct generation record/state patch is incompatible"
        )
    generation = _positive(
        record.get("generationIndex"), name="native v5 direct generation index"
    )
    if patch.get("generationIndex") != generation:
        raise TemporalQDV5ControlPlaneError(
            "native v5 direct generation record/state patch generation drifted"
        )
    sidecar_path = _real_path(
        root / GENERATION_STATE_APPLICATION_SIDECAR_FILENAME,
        name="native v5 generation state-application sidecar",
    )
    sidecar = _self_hashed(
        _read_canonical_object(
            sidecar_path, name="native v5 generation state-application sidecar"
        ),
        field="sidecarSha256",
        name="native v5 generation state-application sidecar",
    )
    expected_sidecar_fields = {
        "schemaVersion",
        "contractVersion",
        "generationIndex",
        "generationKind",
        "configSha256",
        "stateBasisSha256",
        "completedGenerationsBeforeSha256",
        "semanticAuthoritySha256",
        "runtimeAuthoritySha256",
        "finalization",
        "proposalStateAuthority",
        "nextState",
        "identityLedgerPromotion",
        "sidecarSha256",
    }
    if (
        set(sidecar) != expected_sidecar_fields
        or sidecar.get("schemaVersion") != GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA
        or sidecar.get("contractVersion") != CONTRACT_VERSION
        or sidecar.get("generationIndex") != generation
        or sidecar.get("semanticAuthoritySha256")
        != manifest.get("semanticAuthoritySha256")
        or sidecar.get("runtimeAuthoritySha256") != authority["authoritySha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 generation state-application sidecar schema/binding drifted"
        )
    for field in (
        "configSha256",
        "stateBasisSha256",
        "completedGenerationsBeforeSha256",
        "semanticAuthoritySha256",
        "runtimeAuthoritySha256",
    ):
        _sha(sidecar.get(field), name=f"native v5 state-application {field}")

    finalization = _mapping(
        sidecar.get("finalization"), name="native v5 state-application finalization"
    )
    expected_finalization_fields = {
        "sourceSha256",
        "manifestSha256",
        "commitSha256",
        "generationRecordSha256",
        "statePatchSha256",
    }
    if (
        set(finalization) != expected_finalization_fields
        or finalization.get("sourceSha256") != source_sha256
        or finalization.get("manifestSha256") != manifest.get("manifestSha256")
        or finalization.get("commitSha256") != commit.get("commitSha256")
        or finalization.get("generationRecordSha256")
        != record["generationRecordSha256"]
        or finalization.get("statePatchSha256") != patch["statePatchSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 state-application finalization binding drifted"
        )
    for field in expected_finalization_fields:
        _sha(finalization.get(field), name=f"native v5 state-application {field}")

    generation_kind = sidecar.get("generationKind")
    if generation_kind not in {"g0", "evolved"}:
        raise TemporalQDV5ControlPlaneError(
            "native v5 state-application proposal generation kind drifted"
        )
    proposal_authority = _mapping(
        sidecar.get("proposalStateAuthority"),
        name="native v5 state-application proposal authority",
    )
    expected_proposal_authority_fields = {
        "proposalManifestSha256",
        "proposalReceiptSha256",
        "generationJournalSha256",
    }
    if set(proposal_authority) != expected_proposal_authority_fields:
        raise TemporalQDV5ControlPlaneError(
            "native v5 state-application proposal authority schema drifted"
        )
    for field in expected_proposal_authority_fields:
        _sha(
            proposal_authority.get(field),
            name=f"native v5 state-application proposal {field}",
        )

    next_state = _mapping(
        sidecar.get("nextState"), name="native v5 state-application next state"
    )
    expected_next_state_fields = {
        "stage",
        "currentGenerationIndex",
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
        "completedGenerationsSha256",
    }
    if (
        set(next_state) != expected_next_state_fields
        or next_state.get("stage") != "generation_proposal"
        or next_state.get("currentGenerationIndex")
        != patch.get("nextGenerationIndex")
        or next_state.get("uniqueCandidatesEvaluated")
        != patch.get("uniqueCandidatesEvaluated")
        or next_state.get("workerTasksCompleted") != patch.get("workerTasksCompleted")
        or next_state.get("nextImmigrantContinuationOrdinal")
        != patch.get("nextImmigrantContinuationOrdinal")
        or next_state.get("uniqueIdentityCounts") != patch.get("uniqueIdentityCounts")
        or next_state.get("duplicateCounters") != patch.get("duplicateCounters")
        or next_state.get("proposalSlotCounters") != patch.get("proposalSlotCounters")
        or next_state.get("completedGenerationsSha256")
        != patch.get("completedGenerationsSha256")
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 state-application next-state binding drifted"
        )
    for field in (
        "currentGenerationIndex",
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
    ):
        _nonnegative(next_state.get(field), name=f"native v5 next-state {field}")
    for field in (
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
    ):
        _mapping(next_state.get(field), name=f"native v5 next-state {field}")
    _sha(
        next_state.get("completedGenerationsSha256"),
        name="native v5 next-state completed generation identity",
    )

    promotion = _mapping(
        sidecar.get("identityLedgerPromotion"),
        name="native v5 state-application identity-ledger promotion",
    )
    expected_promotion_fields = {
        "inputIdentityLedgerSha256",
        "outputRelativePath",
        "outputIdentityLedgerSha256",
        "outputIdentityLedgerFileSha256",
    }
    if (
        set(promotion) != expected_promotion_fields
        or promotion.get("outputRelativePath")
        != "proposal/v5-native/identity-ledger.json"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 state-application identity-ledger binding drifted"
        )
    input_identity = promotion.get("inputIdentityLedgerSha256")
    if generation_kind == "g0":
        if input_identity is not None:
            raise TemporalQDV5ControlPlaneError(
                "native v5 G0 state-application sidecar has an input identity ledger"
            )
    else:
        _sha(
            input_identity, name="native v5 evolved input identity-ledger identity"
        )
    output_identity = _sha(
        promotion.get("outputIdentityLedgerSha256"),
        name="native v5 output identity-ledger identity",
    )
    output_file_identity = _sha(
        promotion.get("outputIdentityLedgerFileSha256"),
        name="native v5 output identity-ledger file identity",
    )
    # The finalizer sidecar and proposal authority bind this compact ledger
    # identity.  Do not reopen or hash the proposal output here: receipt
    # adoption in Rust owns its raw-byte verification, and a committed restart
    # must remain valid after the rich finalizer inputs have been discarded.
    output_path = (root.parent / str(promotion["outputRelativePath"])).resolve()
    return sidecar, {
        "relativePath": promotion["outputRelativePath"],
        "absolutePath": str(output_path),
        "semanticSha256": output_identity,
        "fileSha256": output_file_identity,
    }


def run_native_v5_generation_finalizer(
    *,
    runtime_authority: Mapping[str, Any],
    manifest_path: Path | str,
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    """Commit a v2 native finalization boundary and validate only its receipts.

    The finalizer owns archive and funnel construction.  This bridge reopens
    compact canonical control objects plus file identities; it intentionally
    does not load archive cells, selected members, or evidence rows.
    """

    authority = _validate_runtime_authority(runtime_authority)
    path = _real_path(manifest_path, name="native v5 finalizer manifest")
    root = _real_path(path.parent, name="native v5 finalizer root", directory=True)
    manifest = _self_hashed(
        _read_canonical_object(path, name="native v5 finalizer manifest"),
        field="manifestSha256",
        name="native v5 finalizer manifest",
    )
    expected_manifest_fields = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "runtimeAuthoritySha256",
        "semanticAuthoritySha256",
        "sourcePath",
        "sourceSha256",
        "resultPath",
        "manifestSha256",
    }
    source_path = root / "source.json"
    source_sha256 = _sha(
        manifest.get("sourceSha256"), name="native v5 finalizer source identity"
    )
    if (
        set(manifest) != expected_manifest_fields
        or manifest.get("schemaVersion") != FINALIZER_MANIFEST_SCHEMA
        or manifest.get("contractVersion") != CONTRACT_VERSION
        or manifest.get("operation") != "finalize_rotating_generation"
        or manifest.get("runtimeAuthoritySha256") != authority["authoritySha256"]
        or not native_v5_transport_path_matches(
            manifest.get("sourcePath"), source_path
        )
        or manifest.get("resultPath") != "generation-commit.json"
    ):
        raise TemporalQDV5ControlPlaneError("native v5 finalizer manifest is incompatible")
    # ``source.json`` is candidate-scale and deliberately opaque here.  Rust
    # authenticates it before fresh execution and can restart from the compact
    # commit after it is gone.  Python binds only the manifest's source root.
    binary = pinned_runtime_binary(runtime_authority=authority, role="generationFinalizer")
    execution = _run_pinned(
        runtime_authority=authority,
        role="generationFinalizer",
        command=[str(binary), str(path)],
        timeout_seconds=timeout_seconds,
    )
    expected_execution_fields = {
        "schemaVersion",
        "status",
        "sourceSha256",
        "manifestSha256",
        "generationIndex",
        "auxiliaryPlanSha256",
        "commitSha256",
        "restart",
        "rawResultReads",
        "elapsedMilliseconds",
        "commit",
    }
    if execution.get("restart") is True:
        expected_execution_fields.add("restartValidation")
    if (
        set(execution) != expected_execution_fields
        or execution.get("schemaVersion") != FINALIZER_EXECUTION_SCHEMA
        or execution.get("status") != "committed"
        or execution.get("sourceSha256") != source_sha256
        or execution.get("manifestSha256") != manifest["manifestSha256"]
        or _positive(
            execution.get("generationIndex"),
            name="native v5 finalizer execution generation index",
        )
        != execution.get("generationIndex")
        or not isinstance(execution.get("restart"), bool)
        or execution.get("rawResultReads") != 0
        or _nonnegative(
            execution.get("elapsedMilliseconds"),
            name="native v5 finalizer elapsed milliseconds",
        )
        != execution.get("elapsedMilliseconds")
        or (
            execution.get("restart") is True
            and execution.get("restartValidation")
            != "compact_commit_and_output_hashes"
        )
    ):
        raise TemporalQDV5ControlPlaneError("native v5 finalizer execution drifted")
    commit_path = _real_path(root / "generation-commit.json", name="native v5 generation commit")
    commit = _self_hashed(
        _read_canonical_object(commit_path, name="native v5 generation commit"),
        field="commitSha256",
        name="native v5 generation commit",
    )
    expected_commit_fields = {
        "schemaVersion",
        "contractVersion",
        "sourceSha256",
        "manifestSha256",
        "runtimeAuthoritySha256",
        "semanticAuthoritySha256",
        "generationIndex",
        "auxiliaryPlanSha256",
        "cumulativeArchive",
        "parentArchive",
        "generationFunnel",
        "generationFunnelSnapshot",
        "checkpoint",
        "ledger",
        "generationRecord",
        "statePatch",
        "restartValidation",
        "rawResultReads",
        "commitSha256",
    }
    if (
        set(commit) != expected_commit_fields
        or commit.get("schemaVersion") != GENERATION_COMMIT_SCHEMA
        or commit.get("contractVersion") != CONTRACT_VERSION
        or commit.get("sourceSha256") != source_sha256
        or commit.get("manifestSha256") != manifest["manifestSha256"]
        or commit.get("runtimeAuthoritySha256") != authority["authoritySha256"]
        or commit.get("semanticAuthoritySha256")
        != manifest["semanticAuthoritySha256"]
        or commit.get("rawResultReads") != 0
        or commit.get("restartValidation") != "compact_commit_and_output_hashes"
        or commit.get("commitSha256") != execution.get("commitSha256")
        or execution.get("commit") != commit
        or execution.get("auxiliaryPlanSha256") != commit.get("auxiliaryPlanSha256")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 generation commit binding drifted")
    generation = _positive(
        commit.get("generationIndex"), name="native v5 finalizer generation index"
    )
    if execution.get("generationIndex") != generation:
        raise TemporalQDV5ControlPlaneError(
            "native v5 finalizer execution/commit generation drifted"
        )

    descriptor_specs = {
        "cumulativeArchive": ("evidence/cumulative-archive.json", "archiveSha256"),
        "parentArchive": ("archive.json", "archiveSha256"),
        "generationFunnel": ("generation-funnel.json", "artifactSha256"),
        "generationFunnelSnapshot": (
            "generation-funnel-snapshot.json",
            "snapshotSha256",
        ),
        "checkpoint": ("evidence/checkpoint.json", "checkpointSha256"),
        "ledger": ("evidence/generation-ledger.json", "ledgerSha256"),
        "generationRecord": ("generation-record.json", "generationRecordSha256"),
        "statePatch": ("generation-state-patch.json", "statePatchSha256"),
    }
    artifacts: dict[str, dict[str, Any]] = {}
    compact_payloads: dict[str, dict[str, Any]] = {}
    for name, (relative_path, semantic_field) in descriptor_specs.items():
        descriptor = _mapping(commit.get(name), name=f"native v5 {name} descriptor")
        if set(descriptor) != {"path", semantic_field, "bytes", "fileSha256"}:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} descriptor schema drifted"
            )
        # The receipt-last Rust commit is the output-tree authority.  Do not
        # reopen or hash candidate-scale archive/funnel/evidence artifacts in
        # Python; only the two compact direct state objects below are opened.
        output_path = root / relative_path
        byte_length = _nonnegative(
            descriptor.get("bytes"), name=f"native v5 {name} byte length"
        )
        file_sha256 = _sha(
            descriptor.get("fileSha256"), name=f"native v5 {name} file identity"
        )
        semantic_sha256 = _sha(
            descriptor.get(semantic_field), name=f"native v5 {name} semantic identity"
        )
        if descriptor.get("path") != relative_path:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} descriptor binding drifted"
            )
        artifacts[name] = {
            "relativePath": relative_path,
            "absolutePath": str(output_path),
            "semanticSha256": semantic_sha256,
            "fileSha256": file_sha256,
            "byteLength": byte_length,
        }
        if name in {"generationRecord", "statePatch"}:
            output_path = _require_bound_file(
                path=output_path,
                raw_sha256=file_sha256,
                size_bytes=byte_length,
                name=f"native v5 compact {name} output",
            )
            payload = _self_hashed(
                _read_canonical_object(output_path, name=f"native v5 {name}"),
                field=semantic_field,
                name=f"native v5 {name}",
            )
            if payload.get(semantic_field) != semantic_sha256:
                raise TemporalQDV5ControlPlaneError(
                    f"native v5 {name} semantic binding drifted"
                )
            compact_payloads[name] = payload
    record = compact_payloads["generationRecord"]
    state_patch = compact_payloads["statePatch"]
    if (
        record.get("generationIndex") != generation
        or state_patch.get("generationIndex") != generation
        or state_patch.get("generationRecordSha256")
        != artifacts["generationRecord"]["semanticSha256"]
        or state_patch.get("generationRecord") != record
        or state_patch.get("runtimeAuthoritySha256") != authority["authoritySha256"]
        or state_patch.get("semanticAuthoritySha256")
        != manifest["semanticAuthoritySha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 generation record/state patch binding drifted"
        )
    state_application_sidecar, identity_ledger_promotion = (
        _validated_native_v5_state_application_sidecar(
            root=root,
            source_sha256=source_sha256,
            manifest=manifest,
            commit=commit,
            generation_record=record,
            state_patch=state_patch,
            runtime_authority=authority,
        )
    )
    return {
        "manifest": manifest,
        "manifestPath": str(path),
        "sourceSha256": source_sha256,
        "sourcePath": str(source_path),
        "execution": execution,
        "commit": commit,
        "commitPath": str(commit_path),
        "artifacts": artifacts,
        "generationRecord": record,
        "statePatch": state_patch,
        "stateApplicationSidecar": state_application_sidecar,
        "stateApplicationSidecarPath": str(
            root / GENERATION_STATE_APPLICATION_SIDECAR_FILENAME
        ),
        "identityLedgerPromotion": identity_ledger_promotion,
        "outputRoot": str(root),
    }


def run_native_v5_campaign_freeze(
    *,
    runtime_authority: Mapping[str, Any],
    evaluation_population_path: Path | str,
    evaluation_population_raw_sha256: str,
    template_preparation_path: Path | str,
    template_preparation_sha256: str,
    construction_catalog_path: Path | str,
    construction_catalog_sha256: str,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    behavior_attribution_requirement: Mapping[str, Any],
    campaign_role: str,
    panel_id: str,
    cohort_selection_path: Path | str | None = None,
    final_archive_reduction_result_path: Path | str | None = None,
    ladder_stage: str | None = None,
    ladder_candidate_limit: int | None = None,
    ladder_authority: Mapping[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Run the pinned native freezer and reopen its receipt-last boundary.

    The underlying bridge validates the v2 freezer transaction.  This wrapper
    adds the frozen runtime role binding and returns only compact receipts and
    paths for later native stages; it never opens a population candidate list.
    """

    authority = _validate_runtime_authority(runtime_authority)
    evaluation = _real_path(
        evaluation_population_path, name="native v5 campaign evaluation population"
    )
    supplied_evaluation_raw_sha256 = _sha(
        evaluation_population_raw_sha256,
        name="native v5 campaign evaluation population identity",
    )
    supplied_template_sha256 = _sha(
        template_preparation_sha256,
        name="native v5 campaign template preparation identity",
    )
    supplied_catalog_sha256 = _sha(
        construction_catalog_sha256,
        name="native v5 campaign construction catalog identity",
    )
    template = _real_path(
        template_preparation_path, name="native v5 campaign template preparation"
    )
    catalog = _real_path(
        construction_catalog_path, name="native v5 campaign construction catalog"
    )
    root = _real_directory(output_root, name="native v5 campaign freeze root")
    selection = (
        _real_path(cohort_selection_path, name="native v5 campaign cohort selection")
        if cohort_selection_path is not None
        else None
    )
    reduction_result = (
        _real_path(
            final_archive_reduction_result_path,
            name="native v5 ladder archive-reduction result",
        )
        if final_archive_reduction_result_path is not None
        else None
    )
    if not isinstance(execution_engine_commit, str) or len(execution_engine_commit) != 40:
        raise TemporalQDV5ControlPlaneError(
            "native v5 campaign execution-engine commit is invalid"
        )
    _sha(worker_contract_sha256, name="native v5 campaign worker contract")
    if not isinstance(campaign_role, str) or campaign_role not in {
        "proposal_current_panel",
        "retained_parent_current_panel",
        "prior_panel_backfill",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 campaign role is invalid")
    if not isinstance(panel_id, str) or not panel_id:
        raise TemporalQDV5ControlPlaneError("native v5 campaign panel identity is invalid")
    binary = pinned_runtime_binary(runtime_authority=authority, role="campaignFreeze")
    try:
        result = freeze_qd_v5_campaign_native(
            evaluation_population_path=evaluation,
            evaluation_population_raw_sha256=supplied_evaluation_raw_sha256,
            template_preparation_path=template,
            template_preparation_sha256=supplied_template_sha256,
            construction_catalog_path=catalog,
            construction_catalog_sha256=supplied_catalog_sha256,
            output_root=root,
            execution_engine_commit=execution_engine_commit,
            worker_contract_sha256=worker_contract_sha256,
            rotating_evidence=_mapping(
                rotating_evidence, name="native v5 rotating evidence"
            ),
            archive_policy_authority=_mapping(
                archive_policy_authority, name="native v5 archive policy authority"
            ),
            behavior_attribution_requirement=_mapping(
                behavior_attribution_requirement,
                name="native v5 behavior attribution requirement",
            ),
            campaign_role=campaign_role,
            panel_id=panel_id,
            cohort_selection_path=selection,
            final_archive_reduction_result_path=reduction_result,
            ladder_stage=ladder_stage,
            ladder_candidate_limit=ladder_candidate_limit,
            native_binary=binary,
            ladder_authority=(
                _mapping(ladder_authority, name="native v5 ladder authority")
                if ladder_authority is not None
                else None
            ),
            execution_timeout_seconds=_positive(
                timeout_seconds, name="native v5 campaign freeze timeout"
            ),
        )
    except TemporalDiscoveryContractError as exc:
        raise TemporalQDV5ControlPlaneError(str(exc)) from exc
    result = _mapping(result, name="native v5 campaign freeze result")
    expected_schema = (
        "temporal_qd_v5_native_evidence_ladder_freeze_result_v1"
        if reduction_result is not None
        else "temporal_qd_v5_native_campaign_freeze_result_v1"
    )
    if result.get("schemaVersion") != expected_schema or result.get("outputRoot") != str(root):
        raise TemporalQDV5ControlPlaneError("native v5 campaign freeze result drifted")
    manifest_path = _real_path(
        root / ".native-v5-campaign-freeze-manifest.json",
        name="native v5 campaign freeze manifest",
    )
    transaction_path = _real_path(
        root / "native-freeze-transaction.json",
        name="native v5 campaign freeze transaction",
    )
    receipt_path = _real_path(
        root / "native-freeze-receipt.json", name="native v5 campaign freeze receipt"
    )
    manifest = _read_json_object(manifest_path, name="native v5 campaign freeze manifest")
    transaction = _self_hashed(
        _read_canonical_object(
            transaction_path, name="native v5 campaign freeze transaction"
        ),
        field="transactionSha256",
        name="native v5 campaign freeze transaction",
    )
    receipt = _self_hashed(
        _read_canonical_object(
            receipt_path, name="native v5 campaign freeze receipt"
        ),
        field="receiptSha256",
        name="native v5 campaign freeze receipt",
    )
    if (
        transaction.get("schemaVersion")
        != "temporal_qd_v5_native_campaign_freeze_transaction_v2"
        or receipt.get("schemaVersion")
        != "temporal_qd_v5_native_campaign_freeze_receipt_v1"
        or transaction.get("manifestSha256") != manifest.get("manifestSha256")
        or receipt.get("manifestSha256") != manifest.get("manifestSha256")
        or receipt.get("transactionSha256") != transaction.get("transactionSha256")
        or receipt.get("nativeRuntimeAuthoritySha256")
        != manifest.get("nativeRuntimeAuthoritySha256")
        or result.get("campaignSha256") != receipt.get("campaignSha256")
        or result.get("taskMatrixSha256") != receipt.get("taskMatrixSha256")
        or result.get("taskCount") != receipt.get("taskCount")
        or transaction.get("evaluationPopulationRawSha256")
        != supplied_evaluation_raw_sha256
        or transaction.get("templatePreparationSha256") != supplied_template_sha256
        or transaction.get("constructionCatalogSha256") != supplied_catalog_sha256
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign freeze receipt drifted")
    # The campaign-freeze bridge freezes a role-specific authority.  Bind it
    # to the immutable supervisor runtime binary so a substituted freezer
    # cannot be smuggled through a valid-looking local receipt.
    freeze_runtime = _mapping(
        manifest.get("nativeRuntimeAuthority"), name="native v5 campaign runtime"
    )
    if freeze_runtime.get("binarySha256") != raw_file_sha256(binary):
        raise TemporalQDV5ControlPlaneError(
            "native v5 campaign freezer binary binding drifted"
        )
    if raw_file_sha256(binary) != _mapping(
        authority["binaries"]["campaignFreeze"],
        name="native v5 campaign freezer authority descriptor",
    ).get("fileSha256"):
        raise TemporalQDV5ControlPlaneError("native v5 campaign freezer changed")

    cohort_population_raw_sha256: str | None = None
    if reduction_result is None:
        receipt_inventory = _validate_native_v5_ladder_inventory(
            receipt.get("outputInventory"),
            expected_paths=_NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS,
            name="native v5 campaign freeze receipt",
        )
        cohort_population_raw_sha256 = next(
            row["rawSha256"]
            for row in receipt_inventory
            if row["relativePath"] == "cohort-population.json"
        )

    return {
        "result": result,
        "outputRoot": str(root),
        "manifest": manifest,
        "manifestPath": str(manifest_path),
        "transaction": transaction,
        "transactionPath": str(transaction_path),
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "cohortPopulationSha256": transaction.get("cohortPopulationSha256"),
        **(
            {"cohortPopulationRawSha256": cohort_population_raw_sha256}
            if cohort_population_raw_sha256 is not None
            else {}
        ),
    }


_NATIVE_V5_LADDER_TRANSACTION_INVENTORY_PATHS = (
    "ladder-archive-population.json",
    "cohort-selection.json",
    "cohort-selection.jsonl",
    "cohort-population.json",
    "preparation.json",
    "authority.json",
    "evaluation-identity.json",
    "campaign.json",
    "screening-run/authority.json",
    "screening-run/task-manifest.json",
    "screening-run/checkpoint.json",
    "native-freeze-result.json",
    "native-freeze-transaction.json",
    "native-freeze-receipt.json",
    "ladder-freeze-result.json",
)
_NATIVE_V5_LADDER_RECEIPT_INVENTORY_PATHS = (
    *_NATIVE_V5_LADDER_TRANSACTION_INVENTORY_PATHS,
    "ladder-freeze-transaction.json",
)
_NATIVE_V5_FREEZE_TRANSACTION_INVENTORY_PATHS = (
    "cohort-population.json",
    "preparation.json",
    "authority.json",
    "evaluation-identity.json",
    "campaign.json",
    "screening-run/authority.json",
    "screening-run/task-manifest.json",
    "screening-run/checkpoint.json",
    "native-freeze-result.json",
)
_NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS = (
    *_NATIVE_V5_FREEZE_TRANSACTION_INVENTORY_PATHS,
    "native-freeze-transaction.json",
)


def _native_v5_ladder_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """Use the v3 freezer's portable manifest projection, never an archive."""

    semantic = dict(manifest)
    semantic.pop("manifestSha256", None)
    semantic.pop("outputRoot", None)
    semantic.pop("templatePreparationPath", None)
    semantic.pop("constructionCatalogPath", None)
    archive_authority = semantic.get("archiveAuthority")
    if isinstance(archive_authority, Mapping):
        copied = dict(archive_authority)
        copied.pop("receiptPath", None)
        semantic["archiveAuthority"] = copied
    return canonical_sha256(semantic)


def _validate_native_v5_ladder_inventory(
    value: object, *, expected_paths: tuple[str, ...], name: str
) -> list[dict[str, Any]]:
    """Authenticate the complete bounded inventory without reopening payloads.

    The inventory contains candidate JSONL and archive-adjacent artifacts, but
    this control plane deliberately validates only its sealed descriptor rows.
    Rust is the sole component that streams/hashes those payloads.
    """

    if not isinstance(value, list) or len(value) != len(expected_paths):
        raise TemporalQDV5ControlPlaneError(f"{name} inventory shape drifted")
    rows: list[dict[str, Any]] = []
    for expected_path, raw in zip(expected_paths, value, strict=True):
        row = _mapping(raw, name=f"{name} inventory row")
        if (
            set(row) != {"relativePath", "rawSha256"}
            or row.get("relativePath") != expected_path
        ):
            raise TemporalQDV5ControlPlaneError(f"{name} inventory path drifted")
        _sha(row.get("rawSha256"), name=f"{name} inventory raw identity")
        rows.append(row)
    return rows


def _validated_native_v5_ladder_archive_authority(
    value: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], int]:
    """Open only the small receipt authorizing an archive-native ladder stage."""

    authority = _mapping(value, name="native v5 ladder archive authority")
    if set(authority) != {"kind", "receiptPath", "receiptSha256"}:
        raise TemporalQDV5ControlPlaneError(
            "native v5 ladder archive authority schema drifted"
        )
    kind = authority.get("kind")
    if kind not in {"generation_finalizer_commit", "qd_archive_reducer_result"}:
        raise TemporalQDV5ControlPlaneError("native v5 ladder archive authority kind is invalid")
    receipt_path_value = authority.get("receiptPath")
    if not isinstance(receipt_path_value, str):
        raise TemporalQDV5ControlPlaneError(
            "native v5 ladder archive receipt path is invalid"
        )
    receipt_path = _real_path(
        receipt_path_value, name="native v5 ladder archive receipt"
    )
    receipt_sha256 = _sha(
        authority.get("receiptSha256"), name="native v5 ladder archive receipt"
    )
    receipt = _read_bounded_canonical_object(
        receipt_path, name="native v5 ladder archive receipt"
    )
    if kind == "generation_finalizer_commit":
        receipt = _self_hashed(
            receipt,
            field="commitSha256",
            name="native v5 ladder finalizer commit",
        )
        if (
            receipt.get("schemaVersion") != GENERATION_COMMIT_SCHEMA
            or receipt.get("commitSha256") != receipt_sha256
        ):
            raise TemporalQDV5ControlPlaneError(
                "native v5 ladder finalizer commit binding drifted"
            )
        archive = _mapping(
            receipt.get("parentArchive"), name="native v5 ladder finalizer archive"
        )
        if (
            set(archive) != {"path", "archiveSha256", "bytes", "fileSha256"}
            or archive.get("path") != "archive.json"
        ):
            raise TemporalQDV5ControlPlaneError(
                "native v5 ladder finalizer archive descriptor drifted"
            )
        _sha(archive.get("archiveSha256"), name="native v5 ladder archive identity")
        _sha(archive.get("fileSha256"), name="native v5 ladder archive file identity")
        _nonnegative(archive.get("bytes"), name="native v5 ladder archive byte length")
    else:
        receipt = _self_hashed(
            receipt,
            field="resultSha256",
            name="native v5 ladder archive reducer result",
        )
        expected = {
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "manifestSha256",
            "tailAuthoritySha256",
            "archiveSha256",
            "archiveRawSha256",
            "archiveSizeBytes",
            "populationSha256",
            "resultSetSha256",
            "generationIndex",
            "candidateCountSeen",
            "occupiedCellCount",
            "memberCount",
            "qualityMemberCount",
            "observationalMemberCount",
            "negativeNoveltyMemberCount",
            "archivePath",
            "runtimeAuthoritySha256",
            "resultSha256",
        }
        if (
            set(receipt) != expected
            or receipt.get("schemaVersion") != ARCHIVE_REDUCTION_RESULT_SCHEMA
            or receipt.get("contractVersion") != CONTRACT_VERSION
            or receipt.get("operation") != ARCHIVE_REDUCTION_OPERATION
            or receipt.get("status") != "completed"
            or receipt.get("archivePath") != "archive.json"
            or receipt.get("resultSha256") != receipt_sha256
        ):
            raise TemporalQDV5ControlPlaneError(
                "native v5 ladder archive reducer receipt drifted"
            )
        for field in (
            "archiveSha256",
            "archiveRawSha256",
            "populationSha256",
            "resultSetSha256",
        ):
            _sha(receipt.get(field), name=f"native v5 ladder reducer {field}")
        _nonnegative(
            receipt.get("archiveSizeBytes"), name="native v5 ladder archive byte length"
        )
    generation = _positive(
        receipt.get("generationIndex"), name="native v5 ladder archive generation"
    )
    return (
        {
            "kind": kind,
            "receiptPath": str(receipt_path),
            "receiptSha256": receipt_sha256,
        },
        receipt,
        generation,
    )


def run_native_v5_evidence_ladder_archive_freeze(
    *,
    runtime_authority: Mapping[str, Any],
    archive_authority: Mapping[str, Any],
    ladder_stage: str,
    ladder_candidate_limit: int,
    ladder_authority: Mapping[str, Any],
    template_preparation_path: Path | str,
    construction_catalog_path: Path | str,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    behavior_attribution_requirement: Mapping[str, Any],
    campaign_role: str,
    panel_id: str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Run and reopen one archive-native evidence-ladder freezer v3 stage.

    Only compact receipts/manifests are decoded here.  In particular, this
    function never opens an archive, a cohort JSONL, a candidate object, or a
    worker result.
    """

    authority = _validate_runtime_authority(runtime_authority)
    archive, archive_receipt, generation = _validated_native_v5_ladder_archive_authority(
        archive_authority
    )
    if ladder_stage not in {"validation", "scrutiny"}:
        raise TemporalQDV5ControlPlaneError("native v5 ladder stage is invalid")
    limit = _positive(ladder_candidate_limit, name="native v5 ladder candidate limit")
    if (
        (ladder_stage == "validation" and archive["kind"] != "generation_finalizer_commit")
        or (ladder_stage == "scrutiny" and archive["kind"] != "qd_archive_reducer_result")
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 ladder stage/archive authority binding drifted"
        )
    ladder = _self_hashed(
        _mapping(ladder_authority, name="native v5 ladder authority"),
        field="ladderAuthoritySha256",
        name="native v5 ladder authority",
    )
    if (
        ladder.get("schemaVersion") != NATIVE_V5_LADDER_AUTHORITY_SCHEMA
        or ladder.get("stageOrder") != ["validation", "scrutiny"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 ladder authority schema drifted")
    stages = _mapping(ladder.get("stages"), name="native v5 ladder authority stages")
    stage_binding = _mapping(
        stages.get(ladder_stage), name="native v5 ladder stage authority"
    )
    if stage_binding.get("candidateLimit") != limit:
        raise TemporalQDV5ControlPlaneError("native v5 ladder stage limit drifted")
    template = _real_path(
        template_preparation_path, name="native v5 ladder template preparation"
    )
    catalog = _real_path(
        construction_catalog_path, name="native v5 ladder construction catalog"
    )
    root = _real_directory(output_root, name="native v5 ladder freeze root")
    rotating = _mapping(rotating_evidence, name="native v5 ladder rotating evidence")
    archive_policy = _mapping(
        archive_policy_authority, name="native v5 ladder archive policy authority"
    )
    behavior = _mapping(
        behavior_attribution_requirement,
        name="native v5 ladder behavior attribution requirement",
    )
    template_sha256 = _sha(
        stage_binding.get("templatePreparationSha256"),
        name="native v5 ladder template preparation identity",
    )
    catalog_sha256 = _sha(
        stage_binding.get("constructionCatalogSha256"),
        name="native v5 ladder construction catalog identity",
    )
    required_bindings = {
        "templatePreparationPath": str(template),
        "templatePreparationSha256": template_sha256,
        "constructionCatalogPath": str(catalog),
        "constructionCatalogSha256": catalog_sha256,
        "archivePolicyAuthority": archive_policy,
        "behaviorAttributionRequirement": behavior,
    }
    if any(stage_binding.get(key) != value for key, value in required_bindings.items()):
        raise TemporalQDV5ControlPlaneError(
            "native v5 ladder stage authority binding drifted"
        )
    if not isinstance(campaign_role, str) or campaign_role not in {
        "retained_parent_current_panel",
        "prior_panel_backfill",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 ladder campaign role is invalid")
    if not isinstance(panel_id, str) or not panel_id:
        raise TemporalQDV5ControlPlaneError("native v5 ladder panel identity is invalid")
    if not isinstance(execution_engine_commit, str) or len(execution_engine_commit) != 40:
        raise TemporalQDV5ControlPlaneError(
            "native v5 ladder execution-engine commit is invalid"
        )
    _sha(worker_contract_sha256, name="native v5 ladder worker contract")
    binary = pinned_runtime_binary(runtime_authority=authority, role="campaignFreeze")
    try:
        handoff = freeze_qd_v5_evidence_ladder_archive_native(
            archive_authority=archive,
            template_preparation_path=template,
            template_preparation_sha256=template_sha256,
            construction_catalog_path=catalog,
            construction_catalog_sha256=catalog_sha256,
            output_root=root,
            execution_engine_commit=execution_engine_commit,
            worker_contract_sha256=worker_contract_sha256,
            rotating_evidence=rotating,
            archive_policy_authority=archive_policy,
            behavior_attribution_requirement=behavior,
            campaign_role=campaign_role,
            panel_id=panel_id,
            ladder_stage=ladder_stage,
            ladder_candidate_limit=limit,
            ladder_authority=ladder,
            native_binary=binary,
            execution_timeout_seconds=_positive(
                timeout_seconds, name="native v5 ladder freeze timeout"
            ),
        )
    except TemporalDiscoveryContractError as exc:
        raise TemporalQDV5ControlPlaneError(str(exc)) from exc
    manifest_path = _real_path(
        handoff.get("manifestPath"), name="native v5 ladder freeze manifest"
    )
    manifest = _read_bounded_pretty_object(
        manifest_path, name="native v5 ladder freeze manifest"
    )
    expected_manifest = {
        "schemaVersion",
        "archiveAuthority",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthority",
        "templatePreparationPath",
        "templatePreparationSha256",
        "constructionCatalogPath",
        "constructionCatalogSha256",
        "outputRoot",
        "executionEngineCommit",
        "workerContractSha256",
        "campaignRole",
        "panelId",
        "rotatingEvidence",
        "archivePolicyAuthority",
        "behaviorAttributionRequirement",
        "nativeRuntimeAuthority",
        "nativeRuntimeAuthoritySha256",
        "manifestSha256",
    }
    if (
        set(manifest) != expected_manifest
        or manifest.get("schemaVersion") != NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA
        or manifest.get("manifestSha256") != _native_v5_ladder_manifest_sha256(manifest)
        or manifest.get("archiveAuthority") != archive
        or manifest.get("ladderStage") != ladder_stage
        or manifest.get("ladderCandidateLimit") != limit
        or manifest.get("ladderAuthority") != ladder
        or manifest.get("outputRoot") != str(root)
        or manifest != handoff.get("manifest")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 ladder manifest drifted")
    result_path = root / "ladder-freeze-result.json"
    transaction_path = root / "ladder-freeze-transaction.json"
    receipt_path = root / "ladder-freeze-receipt.json"
    result = _read_bounded_pretty_object(
        result_path, name="native v5 ladder freeze result"
    )
    transaction = _self_hashed(
        _read_bounded_pretty_object(
            transaction_path, name="native v5 ladder freeze transaction"
        ),
        field="transactionSha256",
        name="native v5 ladder freeze transaction",
    )
    receipt = _self_hashed(
        _read_bounded_pretty_object(
            receipt_path, name="native v5 ladder freeze receipt"
        ),
        field="receiptSha256",
        name="native v5 ladder freeze receipt",
    )
    expected_result = {
        "schemaVersion",
        "manifestSha256",
        "archiveAuthorityKind",
        "archiveAuthorityReceiptSha256",
        "archiveSha256",
        "ladderStage",
        "ladderCandidateLimit",
        "selectionSha256",
        "campaignSha256",
        "authorityId",
        "taskMatrixSha256",
    }
    expected_transaction = {
        "schemaVersion",
        "manifestSha256",
        "archiveAuthorityKind",
        "archiveAuthorityReceiptSha256",
        "archiveSha256",
        "archiveRawSha256",
        "archiveSizeBytes",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthoritySha256",
        "selectionSha256",
        "projectionRawSha256",
        "cohortPopulationSha256",
        "nativeFreezeReceiptSha256",
        "campaignSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "taskCount",
        "outputInventory",
        "transactionSha256",
    }
    expected_receipt = {
        "schemaVersion",
        "manifestSha256",
        "transactionSha256",
        "archiveAuthorityKind",
        "archiveAuthorityReceiptSha256",
        "archiveSha256",
        "archiveRawSha256",
        "archiveSizeBytes",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthoritySha256",
        "selectionSha256",
        "projectionRawSha256",
        "cohortPopulationSha256",
        "nativeFreezeReceiptSha256",
        "campaignSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "taskCount",
        "outputInventory",
        "receiptSha256",
    }
    if (
        set(result) != expected_result
        or result.get("schemaVersion") != NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA
        or result != handoff.get("result")
        or set(transaction) != expected_transaction
        or transaction.get("schemaVersion")
        != NATIVE_V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA
        or set(receipt) != expected_receipt
        or receipt.get("schemaVersion") != NATIVE_V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError("native v5 ladder receipt schema drifted")
    _validate_native_v5_ladder_inventory(
        transaction.get("outputInventory"),
        expected_paths=_NATIVE_V5_LADDER_TRANSACTION_INVENTORY_PATHS,
        name="native v5 ladder transaction",
    )
    _validate_native_v5_ladder_inventory(
        receipt.get("outputInventory"),
        expected_paths=_NATIVE_V5_LADDER_RECEIPT_INVENTORY_PATHS,
        name="native v5 ladder receipt",
    )
    shared_fields = (
        "manifestSha256",
        "archiveAuthorityKind",
        "archiveAuthorityReceiptSha256",
        "archiveSha256",
        "archiveRawSha256",
        "archiveSizeBytes",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthoritySha256",
        "selectionSha256",
        "projectionRawSha256",
        "cohortPopulationSha256",
        "nativeFreezeReceiptSha256",
        "campaignSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "taskCount",
    )
    if (
        any(receipt.get(field) != transaction.get(field) for field in shared_fields)
        or receipt.get("transactionSha256") != transaction.get("transactionSha256")
        or result.get("manifestSha256") != manifest.get("manifestSha256")
        or result.get("archiveAuthorityKind") != archive["kind"]
        or result.get("archiveAuthorityReceiptSha256") != archive["receiptSha256"]
        or result.get("ladderStage") != ladder_stage
        or result.get("ladderCandidateLimit") != limit
        or result.get("archiveSha256") != receipt.get("archiveSha256")
        or result.get("selectionSha256") != receipt.get("selectionSha256")
        or result.get("campaignSha256") != receipt.get("campaignSha256")
        or result.get("authorityId") != receipt.get("authorityId")
        or result.get("taskMatrixSha256") != receipt.get("taskMatrixSha256")
        or receipt.get("ladderAuthoritySha256") != ladder.get("ladderAuthoritySha256")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 ladder receipt binding drifted")
    for field in (
        "archiveSha256",
        "archiveRawSha256",
        "selectionSha256",
        "projectionRawSha256",
        "cohortPopulationSha256",
        "nativeFreezeReceiptSha256",
        "campaignSha256",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
    ):
        _sha(receipt.get(field), name=f"native v5 ladder {field}")
    _nonnegative(receipt.get("archiveSizeBytes"), name="native v5 ladder archive size")
    _positive(receipt.get("taskCount"), name="native v5 ladder task count")
    native_transaction = _self_hashed(
        _read_bounded_canonical_object(
            root / "native-freeze-transaction.json",
            name="native v5 nested freeze transaction",
        ),
        field="transactionSha256",
        name="native v5 nested freeze transaction",
    )
    native_receipt = _self_hashed(
        _read_bounded_canonical_object(
            root / "native-freeze-receipt.json",
            name="native v5 nested freeze receipt",
        ),
        field="receiptSha256",
        name="native v5 nested freeze receipt",
    )
    expected_native_transaction = {
        "schemaVersion",
        "manifestSha256",
        "nativeRuntimeAuthoritySha256",
        "evaluationPopulationRawSha256",
        "cohortPopulationSha256",
        "templatePreparationSha256",
        "constructionCatalogSha256",
        "preparationSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "campaignSha256",
        "taskMatrixSha256",
        "candidateCount",
        "windowCount",
        "taskCount",
        "campaignRole",
        "outputInventory",
        "transactionSha256",
    }
    expected_native_receipt = {
        "schemaVersion",
        "manifestSha256",
        "nativeRuntimeAuthoritySha256",
        "transactionSha256",
        "campaignSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "taskCount",
        "outputInventory",
        "semanticReceiptSha256",
        "receiptSha256",
    }
    if (
        set(native_transaction) != expected_native_transaction
        or native_transaction.get("schemaVersion")
        != "temporal_qd_v5_native_campaign_freeze_transaction_v2"
        or set(native_receipt) != expected_native_receipt
        or native_receipt.get("schemaVersion")
        != "temporal_qd_v5_native_campaign_freeze_receipt_v1"
        or native_receipt.get("transactionSha256")
        != native_transaction.get("transactionSha256")
        or native_receipt.get("receiptSha256")
        != receipt.get("nativeFreezeReceiptSha256")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 nested freeze receipt drifted")
    _validate_native_v5_ladder_inventory(
        native_transaction.get("outputInventory"),
        expected_paths=_NATIVE_V5_FREEZE_TRANSACTION_INVENTORY_PATHS,
        name="native v5 nested freeze transaction",
    )
    native_receipt_inventory = _validate_native_v5_ladder_inventory(
        native_receipt.get("outputInventory"),
        expected_paths=_NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS,
        name="native v5 nested freeze receipt",
    )
    nested_fields = (
        "cohortPopulationSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "campaignSha256",
        "taskMatrixSha256",
        "taskCount",
    )
    if any(
        native_transaction.get(field) != receipt.get(field)
        for field in nested_fields
    ) or any(native_receipt.get(field) != receipt.get(field) for field in nested_fields[1:]):
        raise TemporalQDV5ControlPlaneError("native v5 nested freeze binding drifted")
    if generation != archive_receipt.get("generationIndex"):
        raise TemporalQDV5ControlPlaneError("native v5 ladder archive generation drifted")
    return {
        "result": result,
        "outputRoot": str(root),
        "manifest": manifest,
        "manifestPath": str(manifest_path),
        "transaction": transaction,
        "transactionPath": str(transaction_path),
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "nativeFreezeReceipt": native_receipt,
        "nativeFreezeReceiptPath": str(root / "native-freeze-receipt.json"),
        "generationIndex": generation,
        "cohortPopulationPath": str(root / "cohort-population.json"),
        # This identity comes exclusively from the receipt-authenticated
        # inventory.  The next Rust stage reopens the population; Python must
        # not read or hash its candidate-bearing bytes.
        "cohortPopulationRawSha256": next(
            row["rawSha256"]
            for row in native_receipt_inventory
            if row["relativePath"] == "cohort-population.json"
        ),
        "taskManifestPath": str(root / "screening-run" / "task-manifest.json"),
    }


def run_native_gateway_dispatch(
    *,
    runtime_authority: Mapping[str, Any],
    task_manifest_path: Path | str,
    output_root: Path | str,
    gateway_url: str,
    mode: str,
    timeout_seconds: int,
    gateway_token: str | None = None,
    request_timeout_seconds: int = 30,
    poll_interval_millis: int = 250,
    enqueue_batch_size: int = 128,
    result_batch_size: int = 1,
    max_request_bytes: int = 64 * 1024 * 1024,
    max_response_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    """Dispatch one sealed task matrix through the compact v2 receipt.

    The receipt deliberately summarizes the result-inventory sidecar.  Python
    must not reopen that sidecar, the task manifest, checkpoint, or completion
    journal: the pinned dispatcher validates their bytes before it publishes
    the bounded receipt.  Candidate-window result payloads can individually
    approach the gateway response cap, so the production default drains one
    durable result per response while retaining batched task enqueue.
    """

    authority = _validate_runtime_authority(runtime_authority)
    task_manifest = _real_path(task_manifest_path, name="native v5 gateway task manifest")
    root = _real_directory(output_root, name="native v5 gateway output root")
    if not isinstance(gateway_url, str) or not gateway_url:
        raise TemporalQDV5ControlPlaneError("native v5 gateway URL is invalid")
    if mode not in {"fresh", "resume"}:
        raise TemporalQDV5ControlPlaneError("native v5 gateway mode is invalid")
    values = {
        "request timeout": request_timeout_seconds,
        "poll interval": poll_interval_millis,
        "enqueue batch": enqueue_batch_size,
        "result batch": result_batch_size,
        "maximum request bytes": max_request_bytes,
        "maximum response bytes": max_response_bytes,
    }
    for name, value in values.items():
        _positive(value, name=name)
    receipt_path = root / ".native-gateway-dispatch" / "execution-receipt.json"
    if mode == "fresh" and receipt_path.exists():
        raise TemporalQDV5ControlPlaneError(
            "native v5 gateway fresh dispatch found an immutable execution receipt"
        )
    binary = pinned_runtime_binary(runtime_authority=authority, role="gatewayDispatch")
    command = [
        str(binary),
        "--task-manifest",
        str(task_manifest),
        "--output-root",
        str(root),
        "--gateway-url",
        gateway_url,
        f"--{mode}",
        "--timeout-seconds",
        str(_positive(timeout_seconds, name="gateway timeout")),
        "--request-timeout-seconds",
        str(request_timeout_seconds),
        "--poll-interval-millis",
        str(poll_interval_millis),
        "--enqueue-batch-size",
        str(enqueue_batch_size),
        "--result-batch-size",
        str(result_batch_size),
        "--max-request-bytes",
        str(max_request_bytes),
        "--max-response-bytes",
        str(max_response_bytes),
    ]
    if gateway_token is not None:
        if not isinstance(gateway_token, str) or not gateway_token:
            raise TemporalQDV5ControlPlaneError("native v5 gateway token is invalid")
        command.extend(("--gateway-token", gateway_token))
    result = _run_pinned(
        runtime_authority=authority,
        role="gatewayDispatch",
        command=command,
        timeout_seconds=_positive(timeout_seconds, name="gateway timeout"),
    )
    expected_result = {
        "schemaVersion", "authorityId", "taskMatrixSha256", "taskCount",
        "completedTaskCount", "taskIndexRootSha256", "checkpointPath",
        "sidecarRoot", "createdTaskSidecar", "executionReceiptSha256",
        "semanticExecutionReceiptSha256", "executionReceiptPath", "telemetry",
    }
    if (
        set(result) != expected_result
        or result.get("schemaVersion") != GATEWAY_RESULT_SCHEMA
        or not isinstance(result.get("telemetry"), Mapping)
        or not isinstance(result.get("createdTaskSidecar"), bool)
    ):
        raise TemporalQDV5ControlPlaneError("native v5 gateway result schema drifted")
    receipt = _self_hashed(
        _read_bounded_canonical_object(
            receipt_path, name="native v5 gateway execution receipt"
        ),
        field="receiptSha256",
        name="native v5 gateway receipt",
    )
    expected_receipt = {
        "schemaVersion", "runtimeRoleSha256", "authorityId", "taskMatrixSha256",
        "sourceTaskManifestSha256", "taskIndexRootSha256",
        "completionJournalSemanticSha256", "checkpointSemanticSha256",
        "taskCount", "completedTaskCount", "resultInventoryRootSha256",
        "resultInventorySha256", "resultInventorySizeBytes", "resultInventoryCount",
        "completionJournalSha256", "checkpointSha256", "semanticReceiptSha256",
        "receiptSha256",
    }
    if set(receipt) != expected_receipt or receipt.get("schemaVersion") != GATEWAY_RECEIPT_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 gateway receipt schema drifted")
    semantic_fields = (
        "schemaVersion", "runtimeRoleSha256", "authorityId", "taskMatrixSha256",
        "sourceTaskManifestSha256", "taskIndexRootSha256",
        "completionJournalSemanticSha256", "checkpointSemanticSha256",
        "taskCount", "completedTaskCount", "resultInventoryRootSha256",
        "resultInventorySha256", "resultInventorySizeBytes", "resultInventoryCount",
    )
    semantic = {key: receipt[key] for key in semantic_fields}
    if canonical_sha256(semantic) != _sha(
        receipt.get("semanticReceiptSha256"), name="gateway semantic receipt"
    ):
        raise TemporalQDV5ControlPlaneError("native v5 gateway semantic receipt drifted")
    for field in (
        "runtimeRoleSha256", "taskMatrixSha256", "sourceTaskManifestSha256",
        "taskIndexRootSha256", "completionJournalSemanticSha256",
        "checkpointSemanticSha256", "resultInventoryRootSha256",
        "resultInventorySha256", "completionJournalSha256", "checkpointSha256",
    ):
        _sha(receipt.get(field), name=f"native v5 gateway {field}")
    for field in (
        "taskCount", "completedTaskCount", "resultInventorySizeBytes",
        "resultInventoryCount",
    ):
        _nonnegative(receipt.get(field), name=f"native v5 gateway {field}")
    runtime_role = canonical_sha256(
        {
            "schemaVersion": "temporal_qd_native_gateway_runtime_role_v1",
            "runtimeEpoch": "temporal_qd_native_gateway_dispatch_epoch_v1",
            "binaryRole": "temporal-qd-gateway-dispatch",
        }
    )
    if (
        receipt["runtimeRoleSha256"] != runtime_role
        or receipt["taskCount"] != receipt["completedTaskCount"]
        or result["authorityId"] != receipt["authorityId"]
        or result["taskMatrixSha256"] != receipt["taskMatrixSha256"]
        or result["taskIndexRootSha256"] != receipt["taskIndexRootSha256"]
        or result["taskCount"] != receipt["taskCount"]
        or result["completedTaskCount"] != receipt["completedTaskCount"]
        or result["executionReceiptSha256"] != receipt["receiptSha256"]
        or result["semanticExecutionReceiptSha256"] != receipt["semanticReceiptSha256"]
        or result["executionReceiptPath"] != str(receipt_path)
    ):
        raise TemporalQDV5ControlPlaneError("native v5 gateway receipt/result binding drifted")
    return {
        "result": result,
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "outputRoot": str(root),
        "checkpointPath": str(result["checkpointPath"]),
    }


def build_native_campaign_seal_source(
    *,
    runtime_authority: Mapping[str, Any],
    freezer_root: Path | str,
    gateway_output_root: Path | str,
    source_root: Path | str,
    funnel_projection_included: bool = True,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Join freezer and gateway through compact receipts only.

    ``campaign-seal-source.json`` is deliberately candidate-scale.  Its
    receipt carries the one semantic root needed by campaign-seal, so Python
    neither opens nor hashes the source on fresh execution or restart.
    """

    authority = _validate_runtime_authority(runtime_authority)
    freezer = _real_directory(freezer_root, name="native v5 freezer root")
    gateway_root = _real_directory(gateway_output_root, name="native v5 gateway root")
    destination = _real_directory(source_root, name="native v5 campaign source root")
    task_manifest = _real_path(
        freezer / "screening-run" / "task-manifest.json",
        name="native v5 freezer task manifest",
    )
    freezer_receipt_path = _real_path(
        freezer / "native-freeze-receipt.json", name="native v5 freezer receipt"
    )
    gateway_receipt_path = _real_path(
        gateway_root / ".native-gateway-dispatch" / "execution-receipt.json",
        name="native v5 gateway receipt",
    )
    freezer_receipt = _self_hashed(
        _read_bounded_canonical_object(
            freezer_receipt_path, name="native v5 freezer receipt"
        ),
        field="receiptSha256",
        name="native v5 freezer receipt",
    )
    gateway_receipt = _self_hashed(
        _read_bounded_canonical_object(
            gateway_receipt_path, name="native v5 gateway receipt"
        ),
        field="receiptSha256",
        name="native v5 gateway receipt",
    )
    if gateway_receipt.get("schemaVersion") != GATEWAY_RECEIPT_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 gateway receipt schema drifted")
    source_path = destination / "campaign-seal-source.json"
    manifest = {
        "schemaVersion": CAMPAIGN_SOURCE_BUILD_MANIFEST_SCHEMA,
        "taskManifestPath": str(task_manifest),
        "freezerReceiptPath": str(freezer_receipt_path),
        "gatewayOutputRoot": str(gateway_root),
        "gatewayReceiptPath": str(gateway_receipt_path),
        "sourcePath": str(source_path),
        "funnelProjectionIncluded": bool(funnel_projection_included),
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    manifest_path = _write_canonical_once(
        destination / "source-build-manifest.json",
        manifest,
        name="native v5 campaign source-build manifest",
    )
    binary = pinned_runtime_binary(runtime_authority=authority, role="campaignSeal")
    result = _run_pinned(
        runtime_authority=authority,
        role="campaignSeal",
        command=[str(binary), "--build-source-manifest", str(manifest_path)],
        timeout_seconds=timeout_seconds,
    )
    expected = {
        "schemaVersion", "sourcePath", "sourceSha256", "receiptPath", "receiptSha256",
        "authorityId", "taskMatrixSha256", "taskCount",
    }
    if (
        set(result) != expected
        or result.get("schemaVersion") != CAMPAIGN_SOURCE_BUILD_RESULT_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign source-build result schema drifted")
    receipt_path = destination / "source-build-receipt.json"
    receipt = _self_hashed(
        _read_bounded_canonical_object(
            receipt_path, name="native v5 campaign source-build receipt"
        ),
        field="receiptSha256",
        name="native v5 campaign source-build receipt",
    )
    receipt_expected = {
        "schemaVersion", "manifestSha256", "freezerReceiptSha256", "gatewayReceiptSha256",
        "sourceSha256", "authorityId", "taskMatrixSha256", "taskCount", "sourcePath",
        "receiptSha256",
    }
    if (
        set(receipt) != receipt_expected
        or receipt.get("schemaVersion") != CAMPAIGN_SOURCE_BUILD_RECEIPT_SCHEMA
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign source-build receipt schema drifted")
    for field in (
        "manifestSha256", "freezerReceiptSha256", "gatewayReceiptSha256", "sourceSha256",
        "taskMatrixSha256", "receiptSha256",
    ):
        _sha(receipt.get(field), name=f"native v5 campaign source-build {field}")
    _nonnegative(receipt.get("taskCount"), name="native v5 campaign source-build task count")
    if (
        result["sourcePath"] != str(source_path)
        or result["receiptPath"] != str(receipt_path)
        or receipt["manifestSha256"] != manifest["manifestSha256"]
        or receipt["freezerReceiptSha256"] != freezer_receipt["receiptSha256"]
        or receipt["gatewayReceiptSha256"] != gateway_receipt["receiptSha256"]
        or result["sourceSha256"] != receipt["sourceSha256"]
        or result["authorityId"] != receipt["authorityId"]
        or result["taskMatrixSha256"] != receipt["taskMatrixSha256"]
        or result["taskCount"] != receipt["taskCount"]
        or result["receiptSha256"] != receipt["receiptSha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign source-build binding drifted")
    return {
        "manifest": manifest,
        "manifestPath": str(manifest_path),
        "result": result,
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "sourcePath": str(source_path),
        "sourceSha256": receipt["sourceSha256"],
        "freezerRoot": str(freezer),
        "freezerReceipt": freezer_receipt,
        "gatewayOutputRoot": str(gateway_root),
        "gatewayReceipt": gateway_receipt,
    }


def _validated_native_v5_tail_authority_receipt(
    *,
    receipt_path: Path | str,
    receipt_sha256: object,
    runtime_authority_sha256: str,
    generation_index: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Reopen the compact tail→archive receipt, never its row-bearing inputs."""

    supplied_sha256 = _sha(
        receipt_sha256, name="native v5 tail-authority receipt identity"
    )
    path = _real_path(receipt_path, name="native v5 tail-authority receipt")
    if path.name != "tail-authority.json":
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail-authority receipt path is not fixed"
        )
    receipt = _self_hashed(
        _read_bounded_canonical_object(
            path, name="native v5 tail-authority receipt"
        ),
        field="tailAuthoritySha256",
        name="native v5 tail-authority receipt",
    )
    expected = {
        "schemaVersion",
        "generationIndex",
        "tailReductionManifestSha256",
        "evaluationPopulationSha256",
        "populationSha256",
        "tailResultIndexSha256",
        "taskMatrixSha256",
        "resultSetSha256",
        "runtimeAuthoritySha256",
        "tailReductionResult",
        "evaluatedMembers",
        "tailAuthoritySha256",
    }
    if (
        set(receipt) != expected
        or receipt.get("schemaVersion") != TAIL_AUTHORITY_RECEIPT_SCHEMA
        or receipt.get("tailAuthoritySha256") != supplied_sha256
        or receipt.get("generationIndex") != generation_index
        or receipt.get("runtimeAuthoritySha256") != runtime_authority_sha256
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail-authority receipt binding drifted"
        )
    for field in (
        "tailReductionManifestSha256",
        "evaluationPopulationSha256",
        "populationSha256",
        "tailResultIndexSha256",
        "taskMatrixSha256",
        "resultSetSha256",
        "runtimeAuthoritySha256",
    ):
        _sha(receipt.get(field), name=f"native v5 tail-authority {field}")
    tail_result = _mapping(
        receipt.get("tailReductionResult"),
        name="native v5 tail-authority tail-result descriptor",
    )
    if set(tail_result) != {"path", "rawSha256", "sizeBytes", "resultSha256"} or (
        tail_result.get("path") != "tail-reduction-result.json"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail-authority tail-result descriptor drifted"
        )
    _sha(
        tail_result.get("rawSha256"),
        name="native v5 tail-authority tail-result file identity",
    )
    _sha(
        tail_result.get("resultSha256"),
        name="native v5 tail-authority tail-result identity",
    )
    _nonnegative(
        tail_result.get("sizeBytes"),
        name="native v5 tail-authority tail-result byte length",
    )
    evaluated_members = _mapping(
        receipt.get("evaluatedMembers"),
        name="native v5 tail-authority evaluated-members descriptor",
    )
    if set(evaluated_members) != {"path", "rawSha256", "sizeBytes", "recordCount"} or (
        evaluated_members.get("path") != "evaluated-members.jsonl"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail-authority evaluated-members descriptor drifted"
        )
    _sha(
        evaluated_members.get("rawSha256"),
        name="native v5 tail-authority evaluated-members file identity",
    )
    _nonnegative(
        evaluated_members.get("sizeBytes"),
        name="native v5 tail-authority evaluated-members byte length",
    )
    _nonnegative(
        evaluated_members.get("recordCount"),
        name="native v5 tail-authority evaluated-members count",
    )
    return (
        {"receiptPath": str(path), "receiptSha256": supplied_sha256},
        receipt,
    )


def certify_native_v5_initial_archive(
    *,
    runtime_authority: Mapping[str, Any],
    archive_path: Path | str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Obtain the one Rust-certified transport descriptor for fresh v5 G0.

    This is intentionally a transport operation, not an archive validator in
    Python.  qd-archive-reducer owns archive parsing and hashing; the thin
    bridge admits only its bounded, self-hashed descriptor.
    """

    authority = _validate_runtime_authority(runtime_authority)
    archive = _real_path(archive_path, name="native v5 initial archive")
    binary = pinned_runtime_binary(runtime_authority=authority, role="archiveReducer")
    result = _run_pinned(
        runtime_authority=authority,
        role="archiveReducer",
        command=[str(binary), "--certify-archive", str(archive)],
        timeout_seconds=_positive(
            timeout_seconds, name="native v5 archive certification timeout"
        ),
    )
    descriptor = _self_hashed(
        result,
        field="descriptorSha256",
        name="native v5 archive transport descriptor",
    )
    expected = {
        "schemaVersion",
        "absolutePath",
        "documentSchemaVersion",
        "archiveSha256",
        "fileSha256",
        "sizeBytes",
        "descriptorSha256",
    }
    if (
        set(descriptor) != expected
        or descriptor.get("schemaVersion")
        != "temporal_qd_archive_transport_descriptor_v1"
        or not native_v5_archive_transport_path_matches(
            descriptor.get("absolutePath"), archive
        )
        or descriptor.get("documentSchemaVersion") != "temporal_qd_archive_v3"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 initial archive transport descriptor drifted"
        )
    _sha(descriptor.get("archiveSha256"), name="native v5 initial archive identity")
    _sha(descriptor.get("fileSha256"), name="native v5 initial archive file identity")
    _nonnegative(
        descriptor.get("sizeBytes"), name="native v5 initial archive byte length"
    )
    return descriptor


def run_native_v5_archive_reducer(
    *,
    runtime_authority: Mapping[str, Any],
    tail_authority: Mapping[str, Any],
    output_root: Path | str,
    cell_capacity: int,
    archive_policy_authority: Mapping[str, Any],
    direction_aware: bool,
    previous_archive_path: Path | str | None = None,
    generation_proposal_accounting: Mapping[str, Any] | None = None,
    bidirectional_pair_policy: Mapping[str, Any] | None = None,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Reduce one receipt-authorized native tail into a v5 archive.

    The tail authority is deliberately the only bridge from seal/tail to the
    reducer.  Python opens neither ``tail-reduction-result.json`` nor
    ``evaluated-members.jsonl``; qd-archive-reducer reopens both from its
    receipt-bound fixed paths.
    """

    authority = _validate_runtime_authority(runtime_authority)
    tail_reference = _mapping(tail_authority, name="native v5 tail authority reference")
    if set(tail_reference) != {"receiptPath", "receiptSha256"}:
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail authority reference schema drifted"
        )
    # The receipt must be compact and authenticated before it is carried into
    # the reducer manifest.  Its descriptors remain opaque here.
    receipt_path_value = tail_reference.get("receiptPath")
    if not isinstance(receipt_path_value, str):
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail authority receipt path is invalid"
        )
    receipt_probe = _self_hashed(
        _read_bounded_canonical_object(
            receipt_path_value, name="native v5 tail-authority receipt"
        ),
        field="tailAuthoritySha256",
        name="native v5 tail-authority receipt",
    )
    generation = _positive(
        receipt_probe.get("generationIndex"), name="native v5 tail-authority generation"
    )
    sealed_tail, tail_receipt = _validated_native_v5_tail_authority_receipt(
        receipt_path=receipt_path_value,
        receipt_sha256=tail_reference.get("receiptSha256"),
        runtime_authority_sha256=authority["authoritySha256"],
        generation_index=generation,
    )
    root = _real_directory(output_root, name="native v5 archive-reducer root")
    capacity = _positive(cell_capacity, name="native v5 archive cell capacity")
    if capacity > 32:
        raise TemporalQDV5ControlPlaneError(
            "native v5 archive cell capacity is outside 1..32"
        )
    if direction_aware is not True:
        raise TemporalQDV5ControlPlaneError(
            "current native v5 archive reduction requires directional policy"
        )
    policy_authority = _mapping(
        archive_policy_authority, name="native v5 archive policy authority"
    )
    if set(policy_authority) != {
        "qdVersion",
        "policyName",
        "policySha256",
        "frozenPolicy",
    }:
        raise TemporalQDV5ControlPlaneError(
            "native v5 archive policy authority schema drifted"
        )
    if not isinstance(policy_authority.get("qdVersion"), str) or not policy_authority[
        "qdVersion"
    ]:
        raise TemporalQDV5ControlPlaneError("native v5 archive QD version is invalid")
    archive_policy = {
        field: policy_authority[field]
        for field in ("policyName", "policySha256", "frozenPolicy")
    }
    if not isinstance(archive_policy["policyName"], str) or not archive_policy[
        "policyName"
    ]:
        raise TemporalQDV5ControlPlaneError("native v5 archive policy name is invalid")
    _sha(archive_policy["policySha256"], name="native v5 archive policy identity")
    _mapping(archive_policy["frozenPolicy"], name="native v5 frozen archive policy")
    manifest: dict[str, Any] = {
        "schemaVersion": ARCHIVE_REDUCTION_MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": ARCHIVE_REDUCTION_OPERATION,
        "tailAuthority": sealed_tail,
        "cellCapacity": capacity,
        "archivePolicy": archive_policy,
        "directionAware": True,
    }
    if previous_archive_path is not None:
        previous = _real_path(
            previous_archive_path, name="native v5 archive-reducer previous archive"
        )
        manifest["previousArchivePath"] = str(previous)
    if generation_proposal_accounting is not None:
        manifest["generationProposalAccounting"] = _mapping(
            generation_proposal_accounting,
            name="native v5 archive-reducer proposal accounting",
        )
    if bidirectional_pair_policy is not None:
        manifest["bidirectionalPairPolicy"] = _mapping(
            bidirectional_pair_policy,
            name="native v5 archive-reducer pair policy",
        )
    manifest["manifestSha256"] = canonical_sha256(manifest)
    manifest_path = _write_canonical_once(
        root / "archive-reduction-manifest.json",
        manifest,
        name="native v5 archive-reducer manifest",
    )
    binary = pinned_runtime_binary(runtime_authority=authority, role="archiveReducer")
    stdout = _run_pinned(
        runtime_authority=authority,
        role="archiveReducer",
        command=[str(binary), "--manifest", str(manifest_path)],
        timeout_seconds=_positive(timeout_seconds, name="native v5 archive-reducer timeout"),
    )
    result_path = root / "archive-reduction-result.json"
    result = _self_hashed(
        _read_bounded_canonical_object(
            result_path, name="native v5 archive-reducer result"
        ),
        field="resultSha256",
        name="native v5 archive-reducer result",
    )
    expected_result = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "status",
        "manifestSha256",
        "tailAuthoritySha256",
        "archiveSha256",
        "archiveRawSha256",
        "archiveSizeBytes",
        "populationSha256",
        "resultSetSha256",
        "generationIndex",
        "candidateCountSeen",
        "occupiedCellCount",
        "memberCount",
        "qualityMemberCount",
        "observationalMemberCount",
        "negativeNoveltyMemberCount",
        "archivePath",
        "runtimeAuthoritySha256",
        "resultSha256",
    }
    if (
        set(result) != expected_result
        or result.get("schemaVersion") != ARCHIVE_REDUCTION_RESULT_SCHEMA
        or result.get("contractVersion") != CONTRACT_VERSION
        or result.get("operation") != ARCHIVE_REDUCTION_OPERATION
        or result.get("status") != "completed"
        or result.get("manifestSha256") != manifest["manifestSha256"]
        or result.get("tailAuthoritySha256") != sealed_tail["receiptSha256"]
        or result.get("generationIndex") != generation
        or result.get("archivePath") != "archive.json"
        or result.get("runtimeAuthoritySha256") != authority["authoritySha256"]
        or stdout != result
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 archive-reducer result binding drifted"
        )
    for field in (
        "archiveSha256",
        "archiveRawSha256",
        "populationSha256",
        "resultSetSha256",
    ):
        _sha(result.get(field), name=f"native v5 archive-reducer {field}")
    for field in (
        "archiveSizeBytes",
        "candidateCountSeen",
        "occupiedCellCount",
        "memberCount",
        "qualityMemberCount",
        "observationalMemberCount",
        "negativeNoveltyMemberCount",
    ):
        _nonnegative(result.get(field), name=f"native v5 archive-reducer {field}")
    return {
        "manifest": manifest,
        "manifestPath": str(manifest_path),
        "result": result,
        "resultPath": str(result_path),
        "archiveAuthority": {
            "kind": "qd_archive_reducer_result",
            "receiptPath": str(result_path),
            "receiptSha256": result["resultSha256"],
        },
        "tailAuthority": tail_receipt,
        "tailAuthorityPath": sealed_tail["receiptPath"],
        "outputRoot": str(root),
    }


def run_native_campaign_seal(
    *,
    runtime_authority: Mapping[str, Any],
    source_build: Mapping[str, Any],
    evaluation_population_path: Path | str,
    output_root: Path | str,
    generation_index: int,
    minimum_total_trades: int,
    minimum_trades_per_window: int,
    cap_trades: int,
    provisional_limit: int,
    evaluation_population_sha256: str,
    tail_authority_only: bool = False,
    timeout_seconds: int = 900,
) -> dict[str, Any]:
    """Seal through the receipt-only v2 execution boundary.

    The former v1 return carried a candidate-bearing tail transaction and
    prompted Python to parse/hash the v4 index and evaluated-members JSONL.
    Current v5 accepts only the compact execution receipt; Rust reopens every
    payload named by it when the next native stage needs one.
    """

    if tail_authority_only:
        raise TemporalQDV5ControlPlaneError(
            "current native v5 rejects the retired tail_authority_only seal route"
        )
    authority = _validate_runtime_authority(runtime_authority)
    source = _mapping(source_build, name="native v5 source-build handoff")
    source_result = _mapping(
        source.get("result"), name="native v5 campaign source-build result"
    )
    source_receipt = _self_hashed(
        _mapping(source.get("receipt"), name="native v5 campaign source-build receipt"),
        field="receiptSha256",
        name="native v5 campaign source-build receipt",
    )
    expected_source_result = {
        "schemaVersion", "sourcePath", "sourceSha256", "receiptPath", "receiptSha256",
        "authorityId", "taskMatrixSha256", "taskCount",
    }
    if (
        set(source_result) != expected_source_result
        or source_result.get("schemaVersion") != CAMPAIGN_SOURCE_BUILD_RESULT_SCHEMA
        or source_result.get("sourcePath") != source.get("sourcePath")
        or source_result.get("sourceSha256") != source_receipt.get("sourceSha256")
        or source_result.get("receiptSha256") != source_receipt.get("receiptSha256")
        or source_result.get("receiptPath") != source.get("receiptPath")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign source-build handoff drifted")
    source_path_value = source_result.get("sourcePath")
    if not isinstance(source_path_value, str) or not Path(source_path_value).is_absolute():
        raise TemporalQDV5ControlPlaneError("native v5 campaign source path is invalid")
    source_sha256 = _sha(
        source_result.get("sourceSha256"), name="native v5 campaign source identity"
    )
    evaluation_value = evaluation_population_path
    evaluation = Path(evaluation_value)
    if not evaluation.is_absolute():
        raise TemporalQDV5ControlPlaneError("native v5 evaluation population path is invalid")
    evaluation = Path(os.path.abspath(str(evaluation)))
    # The freezer authenticated this candidate-bearing population.  The seal
    # transaction reopens it; Python receives only that inventory root.
    evaluation_sha256 = _sha(
        evaluation_population_sha256,
        name="native v5 receipt-bound evaluation population identity",
    )
    root = _real_directory(output_root, name="native v5 campaign seal root")
    generation = _positive(generation_index, name="native v5 generation index")
    numeric = {
        "minimum total trades": minimum_total_trades,
        "minimum trades per window": minimum_trades_per_window,
        "cap trades": cap_trades,
        "provisional limit": provisional_limit,
    }
    for name, value in numeric.items():
        _nonnegative(value, name=name)
    if provisional_limit < 1:
        raise TemporalQDV5ControlPlaneError("native v5 provisional limit must be positive")
    directional_tail_authority = validate_v5_directional_tail_authority(
        build_v5_directional_tail_authority(
            runtime_authority_sha256=_sha(
                authority["authoritySha256"], name="native v5 runtime authority"
            ),
            generation_index=generation,
        ),
        runtime_authority_sha256=authority["authoritySha256"],
        generation_index=generation,
    )
    manifest = {
        "schemaVersion": "temporal_qd_campaign_seal_manifest_v1",
        "contractVersion": CONTRACT_VERSION,
        "operation": "seal_completed_task_matrix_and_reduce_tail",
        "runtimeAuthoritySha256": authority["authoritySha256"],
        "sourcePath": str(source_path_value),
        "sourceSha256": source_sha256,
        "evaluationPopulationPath": str(evaluation),
        "evaluationPopulationSha256": evaluation_sha256,
        "generationIndex": generation,
        "minimumTotalTrades": minimum_total_trades,
        "minimumTradesPerWindow": minimum_trades_per_window,
        "capTrades": cap_trades,
        "provisionalLimit": provisional_limit,
        "directionalTailAuthority": directional_tail_authority,
        "resultPath": "generation-tail-transaction-result.json",
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    manifest_path = _write_canonical_once(
        root / "campaign-seal-manifest.json", manifest, name="native v5 campaign seal manifest"
    )
    binary = pinned_runtime_binary(runtime_authority=authority, role="campaignSeal")
    execution = _run_pinned(
        runtime_authority=authority,
        role="campaignSeal",
        command=[str(binary), "--manifest", str(manifest_path)],
        timeout_seconds=timeout_seconds,
    )
    if (
        set(execution)
        != {"schemaVersion", "restartedFromCommittedReceipt", "receipt", "runtimeMetrics"}
        or execution.get("schemaVersion") != CAMPAIGN_SEAL_EXECUTION_SCHEMA
        or not isinstance(execution.get("restartedFromCommittedReceipt"), bool)
        or not isinstance(execution.get("runtimeMetrics"), Mapping)
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign seal execution schema drifted")
    execution_receipt_path = root / "campaign-seal-execution-receipt.json"
    execution_receipt = _self_hashed(
        _read_bounded_canonical_object(
            execution_receipt_path, name="native v5 campaign seal execution receipt"
        ),
        field="receiptSha256",
        name="native v5 campaign seal execution receipt",
    )
    expected_execution_receipt = {
        "schemaVersion", "contractVersion", "manifestSha256", "runtimeAuthoritySha256",
        "sourceSha256", "campaignSeal", "generationTailTransaction", "tailResultIndex",
        "tailAuthority", "receiptSha256",
    }
    if (
        set(execution_receipt) != expected_execution_receipt
        or execution_receipt.get("schemaVersion") != CAMPAIGN_SEAL_EXECUTION_RECEIPT_SCHEMA
        or execution_receipt.get("contractVersion") != CONTRACT_VERSION
        or execution_receipt.get("manifestSha256") != manifest["manifestSha256"]
        or execution_receipt.get("runtimeAuthoritySha256") != authority["authoritySha256"]
        or execution_receipt.get("sourceSha256") != source_sha256
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 campaign seal execution receipt drifted"
        )
    stdout_receipt = _mapping(
        execution.get("receipt"), name="native v5 campaign seal stdout receipt"
    )
    if set(stdout_receipt) != {"receiptPath", "receiptSha256"} or (
        not native_v5_transport_path_matches(
            stdout_receipt.get("receiptPath"), execution_receipt_path
        )
        or stdout_receipt.get("receiptSha256") != execution_receipt["receiptSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 campaign seal stdout receipt binding drifted"
        )

    def artifact(
        value: object, *, name: str, path: str, semantic_field: str
    ) -> dict[str, Any]:
        descriptor = _mapping(value, name=f"native v5 {name} descriptor")
        expected = {"path", "rawSha256", "sizeBytes", semantic_field}
        if set(descriptor) != expected or descriptor.get("path") != path:
            raise TemporalQDV5ControlPlaneError(
                f"native v5 {name} descriptor schema drifted"
            )
        _sha(descriptor.get("rawSha256"), name=f"native v5 {name} raw identity")
        _sha(descriptor.get(semantic_field), name=f"native v5 {name} identity")
        _nonnegative(descriptor.get("sizeBytes"), name=f"native v5 {name} byte length")
        return descriptor

    campaign_seal_descriptor = artifact(
        execution_receipt.get("campaignSeal"),
        name="campaign seal",
        path="campaign-seal-result.json",
        semantic_field="campaignSealSha256",
    )
    transaction_descriptor = artifact(
        execution_receipt.get("generationTailTransaction"),
        name="tail transaction",
        path="generation-tail-transaction-result.json",
        semantic_field="transactionSha256",
    )
    index_descriptor = artifact(
        execution_receipt.get("tailResultIndex"),
        name="tail result index",
        path="tail-result-index-v4.json",
        semantic_field="tailResultIndexSha256",
    )
    tail_reference = _mapping(
        execution_receipt.get("tailAuthority"), name="native v5 tail authority receipt reference"
    )
    if (
        set(tail_reference) != {"receiptPath", "receiptSha256"}
        or tail_reference.get("receiptPath") != "tail-authority.json"
    ):
        raise TemporalQDV5ControlPlaneError("native v5 tail authority receipt reference drifted")
    tail_reference_sha256 = _sha(
        tail_reference.get("receiptSha256"), name="native v5 tail authority receipt identity"
    )
    # A campaign seal is a bounded control document.  Its self-hash and the
    # receipt descriptor bind the native funnel/panel inputs, while Rust owns
    # raw-byte checks for every candidate-scale tail sibling.
    campaign_seal = _self_hashed(
        _read_bounded_canonical_object(
            root / "campaign-seal-result.json", name="native v5 campaign seal"
        ),
        field="campaignSealSha256",
        name="native v5 campaign seal",
    )
    if (
        campaign_seal.get("schemaVersion") != CAMPAIGN_SEAL_SCHEMA
        or campaign_seal.get("manifestSha256") != manifest["manifestSha256"]
        or campaign_seal.get("campaignSealSha256")
        != campaign_seal_descriptor["campaignSealSha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign seal binding drifted")
    tail_authority_receipt, tail_authority_document = (
        _validated_native_v5_tail_authority_receipt(
            receipt_path=root / "tail-authority.json",
            receipt_sha256=tail_reference_sha256,
            runtime_authority_sha256=authority["authoritySha256"],
            generation_index=generation,
        )
    )
    if (
        tail_authority_document.get("tailResultIndexSha256")
        != index_descriptor["tailResultIndexSha256"]
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail authority/index binding drifted"
        )
    return {
        "sourceBuild": source,
        "manifest": manifest,
        "manifestPath": str(manifest_path),
        "execution": execution,
        "executionReceipt": execution_receipt,
        "executionReceiptPath": str(execution_receipt_path),
        "campaignSeal": campaign_seal,
        "campaignSealDescriptor": campaign_seal_descriptor,
        "directionalTailAuthority": directional_tail_authority,
        "tailAuthorityReceipt": tail_authority_receipt,
        "tailAuthorityReceiptDocument": tail_authority_document,
        "generationTailTransaction": transaction_descriptor,
        "tailResultIndex": {
            "path": str(root / "tail-result-index-v4.json"),
            "relativePath": "tail-result-index-v4.json",
            "rawSha256": index_descriptor["rawSha256"],
            "sizeBytes": index_descriptor["sizeBytes"],
            "tailResultIndexSha256": index_descriptor["tailResultIndexSha256"],
        },
        "outputRoot": str(root),
    }


def build_native_panel_bundle_sidecar(
    *,
    runtime_authority: Mapping[str, Any],
    panel_input: Mapping[str, Any],
    output_root: Path | str,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Have Rust materialize sealed bundle JSONL; Python never serializes rows."""

    root = _real_directory(output_root, name="native v5 panel bundle root")
    input_value = _mapping(panel_input, name="native v5 panel bundle input")
    expected_input_fields = {
        "schemaVersion",
        "contractVersion",
        "generationIndex",
        "campaignRole",
        "campaignSeal",
        "tailAuthority",
        "tailResultIndex",
        "directionalTailAuthority",
        "rotatingEvidence",
        "panel",
    }
    if (
        set(input_value) != expected_input_fields
        or input_value.get("schemaVersion")
        != "temporal_qd_v5_rotating_panel_bundle_input_v2"
        or input_value.get("contractVersion") != CONTRACT_VERSION
    ):
        raise TemporalQDV5ControlPlaneError("native v5 panel bundle input schema drifted")
    if "inputSha256" in input_value:
        raise TemporalQDV5ControlPlaneError("native v5 panel bundle input already has an identity")
    input_value["inputSha256"] = canonical_sha256(input_value)
    input_path = _write_canonical_once(
        root / "panel-bundle-input.json", input_value, name="native v5 panel bundle input"
    )
    sidecar_path = root / "candidate-panel-bundles.jsonl"
    receipt_path = root / "candidate-panel-bundles-receipt.json"
    binary = pinned_runtime_binary(runtime_authority=runtime_authority, role="rotatingPrefinalizer")
    result = _run_pinned(
        runtime_authority=runtime_authority,
        role="rotatingPrefinalizer",
        command=[
            str(binary), "build-panel-bundle-sidecar", str(input_path), str(sidecar_path), str(receipt_path)
        ],
        timeout_seconds=timeout_seconds,
    )
    expected = {"schemaVersion", "inputSha256", "receiptSha256", "candidatePanelBundles", "resultSha256"}
    if set(result) != expected or result.get("schemaVersion") != PANEL_SIDECAR_RESULT_SCHEMA:
        raise TemporalQDV5ControlPlaneError("native v5 panel sidecar result schema drifted")
    _self_hashed(result, field="resultSha256", name="native v5 panel sidecar result")
    if result["inputSha256"] != input_value["inputSha256"]:
        raise TemporalQDV5ControlPlaneError("native v5 panel sidecar input binding drifted")
    receipt = _self_hashed(
        _read_canonical_object(receipt_path, name="native v5 panel sidecar receipt"),
        field="receiptSha256",
        name="native v5 panel sidecar receipt",
    )
    expected_receipt = {
        "schemaVersion",
        "inputSha256",
        "panelReceiptSha256",
        "campaignSealSha256",
        "tailAuthoritySha256",
        "candidatePanelBundles",
        "receiptSha256",
    }
    if (
        set(receipt) != expected_receipt
        or receipt.get("schemaVersion") != PANEL_SIDECAR_RECEIPT_SCHEMA
        or receipt.get("receiptSha256") != result["receiptSha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 panel sidecar receipt drifted")
    descriptor = _self_hashed(
        _mapping(result.get("candidatePanelBundles"), name="native v5 panel sidecar descriptor"),
        field="descriptorSha256",
        name="native v5 panel sidecar descriptor",
    )
    if (
        descriptor.get("schemaVersion") != PANEL_SIDECAR_DESCRIPTOR_SCHEMA
        or descriptor.get("path") != str(sidecar_path)
        or descriptor.get("rowSchema") != "temporal_qd_candidate_panel_evidence_bundle_v1"
        or receipt.get("candidatePanelBundles") != descriptor
    ):
        raise TemporalQDV5ControlPlaneError("native v5 panel sidecar descriptor drifted")
    _sha(descriptor.get("rawSha256"), name="native v5 panel sidecar raw identity")
    _nonnegative(descriptor.get("sizeBytes"), name="native v5 panel sidecar byte length")
    _nonnegative(descriptor.get("recordCount"), name="native v5 panel sidecar record count")
    return {
        "input": input_value,
        "inputPath": str(input_path),
        "result": result,
        "receipt": receipt,
        "receiptPath": str(receipt_path),
        "candidatePanelBundles": descriptor,
    }


def _receipt_bound_execution_descriptor(
    *, path: Path | str, raw_sha256: object, name: str
) -> dict[str, Any]:
    """Project a Rust receipt's raw identity without rehashing its payload.

    Campaign-receipt v2 predates the compact descriptor transport and still
    wants a byte length.  A metadata probe is sufficient for that legacy
    field; Python must never reproduce its raw SHA from a task, journal, tail
    member stream, or panel bundle.
    """

    checked = _real_path(path, name=name)
    return {
        "path": str(checked),
        "rawSha256": _sha(raw_sha256, name=f"{name} raw identity"),
        "sizeBytes": checked.stat().st_size,
    }


def _campaign_execution_bindings(
    *,
    freeze: Mapping[str, Any],
    seal: Mapping[str, Any],
    sidecar: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Return v2 execution descriptors without reopening candidate payloads.

    Every candidate-bearing raw SHA originates in a Rust freezer/gateway/seal
    receipt.  This bridge only combines those compact roots with fixed paths;
    Rust's campaign receipt validator owns the later byte reauthentication.
    """

    freeze_root = _real_directory(
        freeze.get("outputRoot"), name="native v5 campaign freeze output root"
    )
    freeze_receipt = _self_hashed(
        _mapping(freeze.get("receipt"), name="native v5 campaign freeze receipt"),
        field="receiptSha256",
        name="native v5 campaign freeze receipt",
    )
    inventory_rows = _validate_native_v5_ladder_inventory(
        freeze_receipt.get("outputInventory"),
        expected_paths=_NATIVE_V5_FREEZE_RECEIPT_INVENTORY_PATHS,
        name="native v5 campaign freeze receipt",
    )
    inventory = {str(row["relativePath"]): row["rawSha256"] for row in inventory_rows}
    source_build = _mapping(
        seal.get("sourceBuild"), name="native v5 campaign seal source-build handoff"
    )
    gateway_root = _real_directory(
        source_build.get("gatewayOutputRoot"), name="native v5 gateway output root"
    )
    gateway_receipt = _self_hashed(
        _mapping(
            source_build.get("gatewayReceipt"),
            name="native v5 gateway execution receipt",
        ),
        field="receiptSha256",
        name="native v5 gateway execution receipt",
    )
    if gateway_receipt.get("schemaVersion") != GATEWAY_RECEIPT_SCHEMA:
        raise TemporalQDV5ControlPlaneError(
            "native v5 campaign gateway receipt schema drifted"
        )
    seal_root = _real_directory(
        seal.get("outputRoot"), name="native v5 campaign seal output root"
    )
    campaign_seal = _mapping(
        seal.get("campaignSealDescriptor"), name="native v5 campaign seal descriptor"
    )
    tail_transaction = _mapping(
        seal.get("generationTailTransaction"), name="native v5 tail transaction descriptor"
    )
    tail_index = _mapping(seal.get("tailResultIndex"), name="native v5 tail index")
    tail_receipt = _mapping(
        seal.get("tailAuthorityReceiptDocument"),
        name="native v5 tail-authority receipt",
    )
    evaluated_members = _mapping(
        tail_receipt.get("evaluatedMembers"),
        name="native v5 receipt-bound evaluated members",
    )
    bundles = _mapping(
        sidecar.get("candidatePanelBundles"), name="native v5 panel sidecar descriptor"
    )

    def sealed_artifact(
        descriptor: Mapping[str, Any], *, path: Path, semantic_field: str, name: str
    ) -> dict[str, Any]:
        expected = {"path", "rawSha256", "sizeBytes", semantic_field}
        if set(descriptor) != expected or descriptor.get("path") != path.name:
            raise TemporalQDV5ControlPlaneError(f"{name} descriptor schema drifted")
        _sha(descriptor.get(semantic_field), name=f"{name} identity")
        return {
            "path": str(path),
            "rawSha256": _sha(descriptor.get("rawSha256"), name=f"{name} raw identity"),
            "sizeBytes": _nonnegative(descriptor.get("sizeBytes"), name=f"{name} byte length"),
        }

    if set(tail_index) != {
        "path", "relativePath", "rawSha256", "sizeBytes", "tailResultIndexSha256"
    } or tail_index.get("relativePath") != "tail-result-index-v4.json":
        raise TemporalQDV5ControlPlaneError("native v5 tail index descriptor schema drifted")
    if set(evaluated_members) != {"path", "rawSha256", "sizeBytes", "recordCount"} or (
        evaluated_members.get("path") != "evaluated-members.jsonl"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evaluated-member descriptor schema drifted"
        )
    if set(bundles) != {
        "schemaVersion", "path", "rawSha256", "sizeBytes", "recordCount", "rowSchema", "descriptorSha256"
    }:
        raise TemporalQDV5ControlPlaneError(
            "native v5 panel-bundle descriptor schema drifted"
        )
    _self_hashed(
        bundles,
        field="descriptorSha256",
        name="native v5 panel-bundle descriptor",
    )

    return {
        # These are small control documents, so a direct canonical descriptor
        # is appropriate.  Everything below is candidate/task-scale and takes
        # its raw identity solely from its native receipt.
        "freezeManifest": _descriptor(
            _real_path(freeze.get("manifestPath"), name="native v5 freeze manifest")
        ),
        "freezeTransaction": _descriptor(
            _real_path(freeze.get("transactionPath"), name="native v5 freeze transaction")
        ),
        "campaign": _receipt_bound_execution_descriptor(
            path=freeze_root / "campaign.json",
            raw_sha256=inventory["campaign.json"],
            name="native v5 freeze campaign",
        ),
        "taskManifest": _receipt_bound_execution_descriptor(
            path=freeze_root / "screening-run" / "task-manifest.json",
            raw_sha256=inventory["screening-run/task-manifest.json"],
            name="native v5 freezer task manifest",
        ),
        "evaluationIdentity": _receipt_bound_execution_descriptor(
            path=freeze_root / "evaluation-identity.json",
            raw_sha256=inventory["evaluation-identity.json"],
            name="native v5 freeze evaluation identity",
        ),
        "gatewayCompletion": _receipt_bound_execution_descriptor(
            path=gateway_root / ".native-gateway-dispatch" / "completion-journal.jsonl",
            raw_sha256=gateway_receipt.get("completionJournalSha256"),
            name="native v5 gateway completion journal",
        ),
        "checkpoint": _receipt_bound_execution_descriptor(
            path=gateway_root / "checkpoint.json",
            raw_sha256=gateway_receipt.get("checkpointSha256"),
            name="native v5 gateway checkpoint",
        ),
        "campaignSeal": sealed_artifact(
            campaign_seal,
            path=seal_root / "campaign-seal-result.json",
            semantic_field="campaignSealSha256",
            name="native v5 campaign seal",
        ),
        "tailResultIndex": {
            "path": str(Path(str(tail_index["path"])).resolve()),
            "rawSha256": _sha(tail_index.get("rawSha256"), name="native v5 tail index raw identity"),
            "sizeBytes": _nonnegative(tail_index.get("sizeBytes"), name="native v5 tail index byte length"),
        },
        "tailTransaction": sealed_artifact(
            tail_transaction,
            path=seal_root / "generation-tail-transaction-result.json",
            semantic_field="transactionSha256",
            name="native v5 tail transaction",
        ),
        "evaluatedMembersJsonl": _receipt_bound_execution_descriptor(
            path=seal_root / "evaluated-members.jsonl",
            raw_sha256=evaluated_members.get("rawSha256"),
            name="native v5 evaluated-member JSONL",
        ),
        "candidatePanelBundlesJsonl": _receipt_bound_execution_descriptor(
            path=bundles.get("path"),
            raw_sha256=bundles.get("rawSha256"),
            name="native v5 panel-bundle JSONL",
        ),
    }


def build_native_rotating_campaign_receipt(
    *,
    runtime_authority: Mapping[str, Any],
    campaign_freeze: Mapping[str, Any],
    campaign_seal: Mapping[str, Any],
    panel_bundle_sidecar: Mapping[str, Any],
    output_root: Path | str,
    generation_index: int,
    campaign_role: str,
    panel_id: str,
    rotating_evidence_sha256: str,
    cohort_source: Mapping[str, Any],
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Build a v2 rotating campaign receipt from native receipts/descriptors.

    It is intentionally a compact join: no JSONL row is decoded, and no
    candidate array is accepted from Python.  Rust validates all execution
    bindings before committing the receipt, then future resumes use that
    receipt without reopening gateway raw results.
    """

    authority = _validate_runtime_authority(runtime_authority)
    freeze = _mapping(campaign_freeze, name="native v5 campaign freeze handoff")
    freeze_root = _real_directory(
        freeze.get("outputRoot"), name="native v5 campaign freeze output root"
    )
    freeze_manifest = _mapping(freeze.get("manifest"), name="native v5 campaign manifest")
    freeze_transaction = _self_hashed(
        _mapping(freeze.get("transaction"), name="native v5 campaign transaction"),
        field="transactionSha256",
        name="native v5 campaign transaction",
    )
    freeze_receipt = _self_hashed(
        _mapping(freeze.get("receipt"), name="native v5 campaign freeze receipt"),
        field="receiptSha256",
        name="native v5 campaign freeze receipt",
    )
    seal = _mapping(campaign_seal, name="native v5 campaign seal handoff")
    sidecar = _mapping(panel_bundle_sidecar, name="native v5 panel sidecar handoff")
    sidecar_result = _self_hashed(
        _mapping(sidecar.get("result"), name="native v5 panel sidecar result"),
        field="resultSha256",
        name="native v5 panel sidecar result",
    )
    sidecar_receipt = _self_hashed(
        _mapping(sidecar.get("receipt"), name="native v5 panel sidecar receipt"),
        field="receiptSha256",
        name="native v5 panel sidecar receipt",
    )
    generation = _positive(generation_index, name="native v5 campaign generation index")
    if campaign_role not in {
        "proposal_current_panel",
        "retained_parent_current_panel",
        "prior_panel_backfill",
    } or not isinstance(panel_id, str) or not panel_id:
        raise TemporalQDV5ControlPlaneError("native v5 campaign role/panel is invalid")
    rotating_sha = _sha(
        rotating_evidence_sha256, name="native v5 rotating evidence identity"
    )
    cohort = _mapping(cohort_source, name="native v5 campaign cohort source")
    if set(cohort) != {
        "kind",
        "sourceSemanticSha256",
        "candidateCount",
        "selectionSha256",
    }:
        raise TemporalQDV5ControlPlaneError("native v5 campaign cohort source drifted")
    kind = cohort.get("kind")
    if kind not in {"proposal_evaluation_population", "sealed_cohort_selection"}:
        raise TemporalQDV5ControlPlaneError("native v5 campaign cohort kind is invalid")
    cohort["sourceSemanticSha256"] = _sha(
        cohort.get("sourceSemanticSha256"), name="native v5 cohort source identity"
    )
    cohort["candidateCount"] = _positive(
        cohort.get("candidateCount"), name="native v5 cohort candidate count"
    )
    if kind == "proposal_evaluation_population":
        if cohort.get("selectionSha256") is not None:
            raise TemporalQDV5ControlPlaneError(
                "native v5 proposal cohort cannot name a selection"
            )
    else:
        cohort["selectionSha256"] = _sha(
            cohort.get("selectionSha256"), name="native v5 cohort selection identity"
        )
    if (
        freeze_manifest.get("manifestSha256") != freeze_transaction.get("manifestSha256")
        or freeze_receipt.get("manifestSha256") != freeze_manifest.get("manifestSha256")
        or freeze_receipt.get("transactionSha256")
        != freeze_transaction.get("transactionSha256")
        or freeze_transaction.get("campaignRole") != campaign_role
        or freeze_manifest.get("campaignRole") != campaign_role
        or freeze_manifest.get("panelId") != panel_id
        or freeze_transaction.get("candidateCount") != cohort["candidateCount"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign freezer handoff drifted")
    freeze_fields = {
        "transactionSha256": freeze_transaction.get("transactionSha256"),
        "cohortPopulationSha256": freeze_transaction.get("cohortPopulationSha256"),
        "preparationSha256": freeze_transaction.get("preparationSha256"),
        "authorityId": freeze_transaction.get("authorityId"),
        "evaluationIdentitySha256": freeze_transaction.get("evaluationIdentitySha256"),
        "campaignSha256": freeze_transaction.get("campaignSha256"),
        "taskMatrixSha256": freeze_transaction.get("taskMatrixSha256"),
        "candidateCount": freeze_transaction.get("candidateCount"),
        "windowCount": freeze_transaction.get("windowCount"),
        "taskCount": freeze_transaction.get("taskCount"),
    }
    for field in (
        "transactionSha256",
        "cohortPopulationSha256",
        "preparationSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "campaignSha256",
        "taskMatrixSha256",
    ):
        freeze_fields[field] = _sha(freeze_fields[field], name=f"native v5 freeze {field}")
    for field in ("candidateCount", "windowCount", "taskCount"):
        freeze_fields[field] = _positive(freeze_fields[field], name=f"native v5 freeze {field}")
    directional_tail_authority = validate_v5_directional_tail_authority(
        _mapping(
            seal.get("directionalTailAuthority"),
            name="native v5 directional tail authority",
        ),
        runtime_authority_sha256=authority["authoritySha256"],
        generation_index=generation,
    )
    campaign_seal_document = _self_hashed(
        _mapping(seal.get("campaignSeal"), name="native v5 campaign seal document"),
        field="campaignSealSha256",
        name="native v5 campaign seal document",
    )
    tail_transaction = _mapping(
        seal.get("generationTailTransaction"), name="native v5 tail transaction descriptor"
    )
    if set(tail_transaction) != {
        "path", "rawSha256", "sizeBytes", "transactionSha256"
    } or tail_transaction.get("path") != "generation-tail-transaction-result.json":
        raise TemporalQDV5ControlPlaneError(
            "native v5 tail transaction descriptor schema drifted"
        )
    tail_index = _mapping(seal.get("tailResultIndex"), name="native v5 tail index")
    if set(tail_index) != {
        "path", "relativePath", "rawSha256", "sizeBytes", "tailResultIndexSha256"
    } or tail_index.get("relativePath") != "tail-result-index-v4.json":
        raise TemporalQDV5ControlPlaneError("native v5 tail index descriptor schema drifted")
    seal_fields = {
        "directionalTailAuthoritySha256": _sha(
            directional_tail_authority.get("tailAuthoritySha256"),
            name="native v5 directional tail authority",
        ),
        "campaignSealSha256": _sha(
            campaign_seal_document.get("campaignSealSha256"),
            name="native v5 campaign seal identity",
        ),
        "tailResultIndexSha256": _sha(
            tail_index.get("tailResultIndexSha256"), name="native v5 tail index identity"
        ),
        "tailTransactionSha256": _sha(
            tail_transaction.get("transactionSha256"),
            name="native v5 tail transaction identity",
        ),
    }
    if (
        campaign_seal_document.get("tailResultIndex", {}).get("sha256")
        != seal_fields["tailResultIndexSha256"]
    ):
        raise TemporalQDV5ControlPlaneError("native v5 campaign seal handoff drifted")
    tail_authority_document = _mapping(
        seal.get("tailAuthorityReceiptDocument"),
        name="native v5 tail-authority receipt document",
    )
    evaluated = _mapping(
        tail_authority_document.get("evaluatedMembers"),
        name="native v5 receipt-bound evaluated members",
    )
    if set(evaluated) != {"path", "rawSha256", "sizeBytes", "recordCount"} or (
        evaluated.get("path") != "evaluated-members.jsonl"
    ):
        raise TemporalQDV5ControlPlaneError(
            "native v5 evaluated-member descriptor schema drifted"
        )
    bundles = _self_hashed(
        _mapping(sidecar.get("candidatePanelBundles"), name="native v5 panel bundles"),
        field="descriptorSha256",
        name="native v5 panel bundle descriptor",
    )
    if (
        sidecar_result.get("receiptSha256") != sidecar_receipt.get("receiptSha256")
        or sidecar_receipt.get("candidatePanelBundles") != bundles
        or sidecar_receipt.get("campaignSealSha256")
        != seal_fields["campaignSealSha256"]
        or sidecar_receipt.get("tailAuthoritySha256")
        != seal_fields["directionalTailAuthoritySha256"]
        or sidecar_result.get("candidatePanelBundles") != bundles
        # The frozen cohort includes every evaluated candidate, including
        # candidates whose windows terminate as deterministic rejections.
        # Panel bundles are emitted only for successfully evaluated members,
        # so their count must bind the tail reducer's evaluated-member stream,
        # not the original cohort size.
        or bundles.get("recordCount") != evaluated.get("recordCount")
    ):
        raise TemporalQDV5ControlPlaneError("native v5 panel sidecar handoff drifted")
    evaluated_fields = {
        "rawSha256": _sha(
            evaluated.get("rawSha256"), name="native v5 evaluated-member file identity"
        ),
        "sizeBytes": _nonnegative(
            evaluated.get("sizeBytes"), name="native v5 evaluated-member file size"
        ),
        "recordCount": _positive(
            evaluated.get("recordCount"), name="native v5 evaluated-member count"
        ),
        "rowSchema": "temporal_qd_evaluated_member_v1",
    }
    bundle_fields = {
        "rawSha256": _sha(
            bundles.get("rawSha256"), name="native v5 panel bundle file identity"
        ),
        "sizeBytes": _nonnegative(
            bundles.get("sizeBytes"), name="native v5 panel bundle file size"
        ),
        "recordCount": _positive(
            bundles.get("recordCount"), name="native v5 panel bundle count"
        ),
        "rowSchema": "temporal_qd_candidate_panel_evidence_bundle_v1",
    }
    execution_bindings = _campaign_execution_bindings(
        freeze=freeze,
        seal=seal,
        sidecar=sidecar,
    )
    input_value = {
        "schemaVersion": CAMPAIGN_RECEIPT_INPUT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": generation,
        "campaignRole": campaign_role,
        "panelId": panel_id,
        "rotatingEvidenceSha256": rotating_sha,
        "cohortSource": cohort,
        "campaignFreeze": freeze_fields,
        "campaignSeal": seal_fields,
        "evaluatedMembers": evaluated_fields,
        "candidatePanelBundles": bundle_fields,
        "runtimeAuthoritySha256": authority["authoritySha256"],
        "executionBindings": execution_bindings,
    }
    input_value["inputSha256"] = canonical_sha256(input_value)
    root = _real_directory(output_root, name="native v5 campaign receipt root")
    input_path = _write_canonical_once(
        root / "campaign-receipt-input.json", input_value, name="native v5 campaign receipt input"
    )
    receipt_path = root / "campaign-receipt.json"
    binary = pinned_runtime_binary(runtime_authority=authority, role="rotatingPrefinalizer")
    stdout = _run_pinned(
        runtime_authority=authority,
        role="rotatingPrefinalizer",
        command=[str(binary), "build-campaign-receipt", str(input_path), str(receipt_path)],
        timeout_seconds=timeout_seconds,
    )
    receipt = _self_hashed(
        _read_canonical_object(receipt_path, name="native v5 rotating campaign receipt"),
        field="receiptSha256",
        name="native v5 rotating campaign receipt",
    )
    semantic = {
        "schemaVersion": CAMPAIGN_RECEIPT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": generation,
        "campaignRole": campaign_role,
        "panelId": panel_id,
        "rotatingEvidenceSha256": rotating_sha,
        "cohortSource": cohort,
        "campaignFreeze": freeze_fields,
        "campaignSeal": seal_fields,
        "evaluatedMembers": evaluated_fields,
        "candidatePanelBundles": bundle_fields,
    }
    expected_receipt = {
        **semantic,
        "semanticReceiptSha256": canonical_sha256(semantic),
        "runtimeAuthoritySha256": authority["authoritySha256"],
        "executionBindings": execution_bindings,
    }
    expected_receipt["receiptSha256"] = canonical_sha256(expected_receipt)
    if receipt != expected_receipt or stdout != receipt:
        raise TemporalQDV5ControlPlaneError(
            "native v5 rotating campaign receipt binding drifted"
        )
    return {
        "input": input_value,
        "inputPath": str(input_path),
        "receipt": receipt,
        "receiptPath": str(receipt_path),
    }


__all__ = [
    "ARCHIVE_REDUCTION_MANIFEST_SCHEMA",
    "ARCHIVE_REDUCTION_RESULT_SCHEMA",
    "CAMPAIGN_RECEIPT_INPUT_SCHEMA",
    "CAMPAIGN_RECEIPT_SCHEMA",
    "CAMPAIGN_SEAL_EXECUTION_SCHEMA",
    "CAMPAIGN_SEAL_SCHEMA",
    "EVOLVED_ATTEMPT_CHAIN_INPUT_SCHEMA",
    "EVOLVED_ATTEMPT_STREAM_RECEIPT_SCHEMA",
    "G0_ATTEMPT_CHAIN_INPUT_SCHEMA",
    "G0_SELECTED_ATTEMPT_STREAM_RECEIPT_SCHEMA",
    "FINALIZER_EXECUTION_SCHEMA",
    "FINALIZER_MANIFEST_SCHEMA",
    "FINALIZER_SOURCE_SCHEMA",
    "FUNNEL_REDUCTION_INPUT_SCHEMA",
    "FUNNEL_REDUCTION_SOURCE_SCHEMA",
    "GENERATION_COMMIT_SCHEMA",
    "GENERATION_RECORD_SCHEMA",
    "GENERATION_STATE_APPLICATION_SIDECAR_FILENAME",
    "GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA",
    "GENERATION_STATE_PATCH_SCHEMA",
    "PREFINALIZER_EXECUTION_SCHEMA",
    "PREFINALIZER_BASE_MANIFEST_SCHEMA",
    "PREFINALIZER_RESUME_MANIFEST_SCHEMA",
    "PREFINALIZER_RESULT_SCHEMA",
    "NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA",
    "NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA",
    "NATIVE_V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA",
    "NATIVE_V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA",
    "NATIVE_V5_LADDER_AUTHORITY_SCHEMA",
    "TAIL_AUTHORITY_RECEIPT_SCHEMA",
    "TemporalQDV5ControlPlaneError",
    "assemble_native_v5_funnel_reduction_source",
    "certify_native_v5_initial_archive",
    "native_v5_archive_transport_path_matches",
    "native_v5_transport_path_matches",
    "build_native_rotating_campaign_receipt",
    "build_native_campaign_seal_source",
    "build_native_panel_bundle_sidecar",
    "build_native_v5_prefinalizer_base_manifest",
    "build_native_v5_prefinalizer_resume_manifest",
    "pinned_runtime_binary",
    "extract_native_v5_g0_selected_attempts",
    "extract_native_v5_evolved_attempt_chain",
    "run_native_campaign_seal",
    "run_native_v5_archive_reducer",
    "run_native_v5_generation_finalizer",
    "run_native_v5_rotating_prefinalizer",
    "run_native_v5_campaign_freeze",
    "run_native_v5_evidence_ladder_archive_freeze",
    "run_native_gateway_dispatch",
]
