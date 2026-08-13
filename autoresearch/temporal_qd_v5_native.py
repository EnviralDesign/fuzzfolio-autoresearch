"""Closed bridge for native v5 Temporal QD proposal construction.

This module is intentionally much smaller than the historical generation
bridge.  It writes one frozen manifest, starts one ``temporal-qd-batch``
process, and verifies its immutable receipt.  It never assembles, compiles,
validates, or projects a candidate in Python; the only Python implementation
is an explicit test/oracle path outside this production bridge.
"""

from __future__ import annotations

import os
from copy import deepcopy
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from . import temporal_qd_native as native
from .result_codec import canonical_json_bytes, sha256

V5_FROZEN_AUTHORITY_SCHEMA = "temporal_qd_v5_shared_construction_authority_v2"
V5_SHARED_AUTHORITY_SCHEMA = "temporal_qd_v5_shared_authority_object_v1"
V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA = "temporal_qd_v5_native_operator_authority_v1"
V5_TEMPORAL_DOMAINS_SCHEMA = "temporal_qd_v5_temporal_domains_v1"
V5_PROPOSAL_MANIFEST_SCHEMA = "temporal_qd_native_v5_proposal_construction_manifest_v1"
V5_PROPOSAL_RESULT_SCHEMA = "temporal_qd_native_v5_proposal_construction_result_v5"
V5_PROPOSAL_RECEIPT_SCHEMA = "temporal_qd_native_v5_proposal_construction_receipt_v5"
V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA = (
    "temporal_qd_native_v5_proposal_construction_summary_v1"
)
V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA = (
    "temporal_qd_native_v5_proposal_output_inventory_v2"
)
V5_PROPOSAL_OUTPUT_ROOT_SCHEMA = "temporal_qd_native_v5_proposal_output_root_v1"
V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA = (
    "temporal_qd_native_v5_proposal_object_store_closure_v2"
)
V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA = (
    "temporal_qd_native_v5_proposal_object_inventory_descriptor_v1"
)
V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA = (
    "temporal_qd_native_v5_proposal_object_inventory_row_v1"
)
V5_PROPOSAL_OBJECT_INVENTORY_PATH = "v5-native/object-inventory.jsonl"
V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA = (
    "temporal_qd_native_v5_proposal_adoption_evidence_v3"
)
V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA = (
    "temporal_qd_native_v5_proposal_adoption_telemetry_v3"
)
V5_EVOLVED_PROPOSAL_RESULT_SCHEMA = (
    "temporal_qd_native_v5_evolved_construction_result_v3"
)
V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA = (
    "temporal_qd_native_v5_evolved_construction_receipt_v3"
)
V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA = (
    "temporal_qd_native_v5_evolved_construction_summary_v1"
)
V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA = (
    "temporal_qd_native_v5_evolved_adoption_evidence_v2"
)
V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA = (
    "temporal_qd_native_v5_evolved_adoption_telemetry_v2"
)
V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA = (
    "temporal_qd_native_v5_proposal_execution_authority_v1"
)
V5_PROPOSAL_INPUTS_SCHEMA = "temporal_qd_native_v5_proposal_inputs_v1"
V5_PROPOSAL_INPUT_BINDING_SCHEMA = "temporal_qd_native_v5_proposal_input_binding_v1"
V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA = (
    "temporal_qd_native_v5_generation_construction_adapter_v3"
)
V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA = (
    "temporal_qd_native_v5_g0_funnel_fragments_descriptor_v1"
)
V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA = "temporal_qd_v5_g0_funnel_fragments_v1"
V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA = (
    "temporal_qd_native_v5_g0_funnel_projection_stream_descriptor_v1"
)
V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA = (
    "temporal_qd_v5_g0_funnel_projection_stream_receipt_v1"
)
V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA = (
    "temporal_qd_v5_proposal_funnel_entry_v1"
)
V5_G0_FUNNEL_PROJECTION_STREAM_PATH = "v5-native/g0-funnel-projections.jsonl"
V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA = (
    "temporal_qd_native_v5_evolved_generation_construction_adapter_v3"
)
V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA = (
    "temporal_qd_native_v5_evolved_publication_fragments_descriptor_v1"
)
V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA = (
    "temporal_qd_v5_evolved_publication_fragments_v2"
)
V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA = (
    "temporal_qd_native_v5_invocation_document_descriptor_v1"
)
V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA = (
    "temporal_qd_native_v5_evolved_invocation_descriptor_v1"
)
V5_G0_NATIVE_V5_INVOCATION_SCHEMA = (
    "temporal_qd_native_v5_g0_invocation_descriptor_v1"
)
V5_PROPOSAL_OPERATION = "native_v5_proposal_construction"
V5_PROPOSAL_RESULT_FILENAME = "v5-proposal-result.json"
V5_PROPOSAL_GENERATION_G0 = "g0"
V5_PROPOSAL_GENERATION_EVOLVED = "evolved"
V5_PROPOSAL_THREAD_CAP_MAXIMUM = 8
_V5_COMPACT_PROPOSAL_RESULT_LIMIT_BYTES = 1_048_576
_V5_COMPACT_PROPOSAL_STDOUT_LIMIT_BYTES = 1_048_576
_V5_COMPACT_PROPOSAL_STDERR_LIMIT_BYTES = 262_144


_V5_G0_REQUIRED_OUTPUT_ARTIFACTS: tuple[tuple[str, str, str | None], ...] = (
    # The semantic ordered-row root lives in a public root document and the
    # exact LF JSONL rows are independently authenticated.  Both are frozen
    # canonical paths: an adoption pass must never accept a root detached from
    # its actual attempt sequence.
    (
        "attemptJournal",
        "v5-native/attempt-journal-root.json",
        "attemptJournalSha256",
    ),
    ("attemptRows", "v5-native/attempts.jsonl", "attemptJournalSha256"),
    (
        "compactJournal",
        "v5-native/accepted-records.jsonl",
        "compactJournalSha256",
    ),
    ("identityLedger", "v5-native/identity-ledger.json", "identityLedgerSha256"),
    (
        "selectedProjectionIndex",
        "v5-native/selected-projections.jsonl",
        "selectedProjectionIndexSha256",
    ),
    (
        "sharedAuthority",
        "v5-native/authority/shared-authority.json",
        "expectedAuthoritySha256",
    ),
    (
        "g0FunnelProjectionStream",
        V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
        "g0FunnelProjectionStreamReceiptSha256",
    ),
    ("evaluationPopulation", "evaluation-population.json", None),
    ("generationJournal", "generation-journal.json", None),
    ("pairConfig", "pair-config.json", None),
    ("population", "population.json", None),
)

_V5_G0_BOOTSTRAP_REQUIRED_OUTPUT_ARTIFACTS: tuple[
    tuple[str, str, str | None], ...
] = (
    ("g0AcceptedPool", "g0-bootstrap/accepted-pool.json", None),
    (
        "g0CampaignConstructionLedger",
        "g0-bootstrap/campaign-construction-ledger.json",
        None,
    ),
    ("g0Selection", "g0-bootstrap/selection.json", None),
)

_V5_EVOLVED_REQUIRED_OUTPUT_ARTIFACTS: tuple[
    tuple[str, str, str | None], ...
] = (
    ("evaluationPopulation", "evaluation-population.json", None),
    ("generationJournal", "generation-journal.json", None),
    (
        "identityLedger",
        "v5-native/identity-ledger.json",
        "identityLedgerSha256",
    ),
    ("pairConfig", "pair-config.json", None),
    ("population", "population.json", None),
)


def _v5_required_output_artifacts(
    generation_kind: object,
) -> tuple[tuple[str, str, str | None], ...]:
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        return (
            _V5_G0_REQUIRED_OUTPUT_ARTIFACTS
            + _V5_G0_BOOTSTRAP_REQUIRED_OUTPUT_ARTIFACTS
        )
    if generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        return _V5_EVOLVED_REQUIRED_OUTPUT_ARTIFACTS
    raise TemporalQDV5NativeError("v5 output inventory generation kind is incompatible")


class TemporalQDV5NativeError(native.TemporalQDNativeError):
    """The closed native v5 transaction could not be created or verified."""


def _sha(value: object, *, name: str) -> str:
    try:
        return native._validate_exact_sha256(value, name=name)
    except native.TemporalQDNativeError as exc:
        raise TemporalQDV5NativeError(str(exc)) from exc


def _mapping(value: object, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalQDV5NativeError(f"{name} must be an object")
    return dict(value)


def _exact_keys(value: Mapping[str, Any], expected: set[str], *, name: str) -> None:
    if set(value) != expected:
        raise TemporalQDV5NativeError(f"{name} fields are not exact")


def _positive(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TemporalQDV5NativeError(f"{name} must be a positive integer")
    return value


def _nonnegative(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TemporalQDV5NativeError(f"{name} must be a nonnegative integer")
    return value


def _count_mapping(value: object, *, name: str) -> tuple[dict[str, Any], int]:
    counts = _mapping(value, name=name)
    if not counts:
        raise TemporalQDV5NativeError(f"{name} must not be empty")
    total = 0
    for key, count in counts.items():
        if not isinstance(key, str) or not key.strip():
            raise TemporalQDV5NativeError(f"{name} has an invalid key")
        total += _nonnegative(count, name=f"{name} {key}")
    return counts, total


def _absolute_safe_path(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TemporalQDV5NativeError(f"{name} must be a nonempty absolute path")
    path = Path(value)
    components = value.replace("\\", "/").split("/")
    if (
        not path.is_absolute()
        or any(component in {".", ".."} for component in components)
    ):
        raise TemporalQDV5NativeError(f"{name} must be a safe absolute path")
    return str(path)


def _v5_proposal_output_root(value: object, *, name: str) -> Path:
    raw = _absolute_safe_path(value, name=name)
    normalized = Path(os.path.abspath(raw))
    # Manifests record the exact normalized proposal-root descendant produced
    # by the safe directory creator.  A spelling alias (including `.`) must
    # not become a second authority for the same output tree.
    if raw != str(normalized):
        raise TemporalQDV5NativeError(f"{name} must be an exact resolved absolute path")
    return native._require_existing_real_directory_tree(normalized, name=name)


def _v5_proposal_output_root_sha256(value: object, *, name: str) -> str:
    root = _v5_proposal_output_root(value, name=name)
    return sha256(
        canonical_json_bytes(
            {
                "schemaVersion": V5_PROPOSAL_OUTPUT_ROOT_SCHEMA,
                "absolutePath": str(root),
            }
        )
    )


def build_v5_proposal_input_binding(
    *,
    kind: str,
    sealed_descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    """Project one Rust proposal binding from an authenticated descriptor.

    The descriptor comes from a Rust receipt/commit boundary.  This bridge
    deliberately performs no file open, stat, or hash: the batch transaction
    is the sole authority that reopens and authenticates the input bytes.
    """

    if kind not in {"parentArchive", "identityLedger"}:
        raise TemporalQDV5NativeError("v5 input binding kind is incompatible")
    descriptor = _mapping(sealed_descriptor, name=f"v5 {kind} sealed descriptor")
    _exact_keys(
        descriptor,
        {"absolutePath", "fileSha256", "semanticSha256", "byteLength"},
        name=f"v5 {kind} sealed descriptor",
    )
    raw_path = _absolute_safe_path(
        descriptor.get("absolutePath"), name=f"v5 {kind} descriptor path"
    )
    target = Path(os.path.abspath(raw_path))
    if raw_path != str(target):
        raise TemporalQDV5NativeError(
            f"v5 {kind} descriptor path must be exact-normalized"
        )
    file_sha256 = _sha(
        descriptor.get("fileSha256"), name=f"v5 {kind} descriptor file identity"
    )
    semantic_sha256 = _sha(
        descriptor.get("semanticSha256"),
        name=f"v5 {kind} descriptor semantic identity",
    )
    _nonnegative(
        descriptor.get("byteLength"), name=f"v5 {kind} descriptor byte length"
    )
    value: dict[str, Any] = {
        "schemaVersion": V5_PROPOSAL_INPUT_BINDING_SCHEMA,
        "kind": kind,
        "absolutePath": str(target),
        "fileSha256": file_sha256,
        "semanticSha256": semantic_sha256,
    }
    value["bindingSha256"] = sha256(canonical_json_bytes(value))
    return validate_v5_proposal_input_binding(value, expected_kind=kind)


def validate_v5_proposal_input_binding(
    value: object, *, expected_kind: str
) -> dict[str, Any]:
    binding = _mapping(value, name=f"v5 {expected_kind} input binding")
    _exact_keys(
        binding,
        {
            "schemaVersion",
            "kind",
            "absolutePath",
            "fileSha256",
            "semanticSha256",
            "bindingSha256",
        },
        name=f"v5 {expected_kind} input binding",
    )
    if (
        binding.get("schemaVersion") != V5_PROPOSAL_INPUT_BINDING_SCHEMA
        or binding.get("kind") != expected_kind
    ):
        raise TemporalQDV5NativeError(f"v5 {expected_kind} input binding is incompatible")
    _absolute_safe_path(binding.get("absolutePath"), name=f"v5 {expected_kind} path")
    _sha(binding.get("fileSha256"), name=f"v5 {expected_kind} file identity")
    _sha(binding.get("semanticSha256"), name=f"v5 {expected_kind} semantic identity")
    return _self_hashed_object(
        binding,
        identity_field="bindingSha256",
        name=f"v5 {expected_kind} input binding",
    )


def _safe_relative_output_path(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TemporalQDV5NativeError(f"{name} must be a nonempty relative path")
    if "\\" in value:
        raise TemporalQDV5NativeError(f"{name} must use canonical '/' separators")
    path = Path(value)
    if (
        path.is_absolute()
        or ":" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise TemporalQDV5NativeError(f"{name} must be a safe relative path")
    return value


def _validate_v5_object_store_inventory(value: object) -> dict[str, Any]:
    """Validate the bounded v2 object-store closure without opening JSONL.

    The complete immutable-object inventory is deliberately a Rust-owned
    canonical JSONL sidecar.  Its descriptor and the small, role-addressed
    root projection are all the Python control plane is allowed to consume.
    Rust reopens and streams the sidecar during fresh construction/adoption.
    """

    store = _self_hashed_object(
        _mapping(value, name="v5 proposal object-store inventory"),
        identity_field="objectStoreSha256",
        name="v5 proposal object-store inventory",
    )
    _exact_keys(
        store,
        {
            "schemaVersion",
            "relativeRoot",
            "inventory",
            "roots",
            "objectStoreSha256",
        },
        name="v5 proposal object-store inventory",
    )
    if (
        store.get("schemaVersion") != V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA
        or store.get("relativeRoot") != "v5-native/objects"
    ):
        raise TemporalQDV5NativeError("v5 proposal object-store inventory is incompatible")
    descriptor = _self_hashed_object(
        _mapping(store.get("inventory"), name="v5 proposal object inventory descriptor"),
        identity_field="descriptorSha256",
        name="v5 proposal object inventory descriptor",
    )
    _exact_keys(
        descriptor,
        {
            "schemaVersion",
            "rowSchemaVersion",
            "relativePath",
            "fileSha256",
            "byteLength",
            "objectCount",
            "objectByteCount",
            "descriptorSha256",
        },
        name="v5 proposal object inventory descriptor",
    )
    if (
        descriptor.get("schemaVersion") != V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA
        or descriptor.get("rowSchemaVersion") != V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA
        or descriptor.get("relativePath") != V5_PROPOSAL_OBJECT_INVENTORY_PATH
    ):
        raise TemporalQDV5NativeError(
            "v5 proposal object inventory descriptor is incompatible"
        )
    _sha(
        descriptor.get("fileSha256"),
        name="v5 proposal object inventory descriptor file identity",
    )
    for key in ("byteLength", "objectCount", "objectByteCount"):
        _nonnegative(
            descriptor.get(key), name=f"v5 proposal object inventory descriptor {key}"
        )
    roots = store.get("roots")
    if not isinstance(roots, list) or len(roots) > 4:
        raise TemporalQDV5NativeError(
            "v5 proposal object-store root projection is unbounded"
        )
    previous_role: str | None = None
    for index, item in enumerate(roots):
        object_entry = _mapping(item, name=f"v5 object-store root {index}")
        _exact_keys(
            object_entry,
            {"role", "relativePath", "objectSha256", "fileSha256", "byteLength"},
            name=f"v5 object-store root {index}",
        )
        role = object_entry.get("role")
        if (
            not isinstance(role, str)
            or not role
            or "/" in role
            or "\\" in role
            or (previous_role is not None and role <= previous_role)
        ):
            raise TemporalQDV5NativeError(
                "v5 proposal object-store root roles must be strictly ordered"
            )
        previous_role = role
        relative_path = _safe_relative_output_path(
            object_entry.get("relativePath"), name=f"v5 object-store root {index} path"
        )
        object_sha256 = _sha(
            object_entry.get("objectSha256"),
            name=f"v5 object-store root {index} semantic SHA",
        )
        expected_path = f"sha256/{object_sha256.removeprefix('sha256:')}.json"
        if relative_path != expected_path:
            raise TemporalQDV5NativeError(
                f"v5 object-store root {index} path/semantic identity drifted"
            )
        _sha(
            object_entry.get("fileSha256"), name=f"v5 object-store root {index} SHA"
        )
        _nonnegative(
            object_entry.get("byteLength"), name=f"v5 object-store root {index} bytes"
        )
    return store


def _validate_v5_output_inventory(
    value: object,
    *,
    semantic_roots: Mapping[str, Any],
    output_root: object,
    generation_kind: object,
) -> dict[str, Any]:
    """Validate immutable output inventory semantics before disk adoption.

    Every public compact head has a fixed path and both a semantic root and a
    raw-file hash/length.  The Rust adoption path additionally streams the
    tree and rejects anything not named by this inventory.
    """

    inventory = _self_hashed_object(
        _mapping(value, name="v5 proposal output inventory"),
        identity_field="outputInventorySha256",
        name="v5 proposal output inventory",
    )
    _exact_keys(
        inventory,
        {
            "schemaVersion",
            "outputRoot",
            "outputRootSha256",
            "artifacts",
            "objectStore",
            "outputInventorySha256",
        },
        name="v5 proposal output inventory",
    )
    if inventory.get("schemaVersion") != V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA:
        raise TemporalQDV5NativeError("v5 proposal output inventory schema is incompatible")
    if (
        inventory.get("outputRoot") != str(
            _v5_proposal_output_root(output_root, name="v5 proposal output root")
        )
        or inventory.get("outputRootSha256")
        != _v5_proposal_output_root_sha256(output_root, name="v5 proposal output root")
    ):
        raise TemporalQDV5NativeError("v5 proposal output inventory root binding drifted")
    artifacts = inventory.get("artifacts")
    required_artifacts = _v5_required_output_artifacts(generation_kind)
    if not isinstance(artifacts, list) or len(artifacts) != len(required_artifacts):
        raise TemporalQDV5NativeError(
            "v5 proposal output inventory artifact set is not exact"
        )
    by_kind: dict[str, dict[str, Any]] = {}
    paths: set[str] = set()
    previous_kind: str | None = None
    for index, item in enumerate(artifacts):
        artifact = _mapping(item, name=f"v5 output artifact {index}")
        _exact_keys(
            artifact,
            {"kind", "relativePath", "fileSha256", "byteLength", "semanticSha256"},
            name=f"v5 output artifact {index}",
        )
        kind = artifact.get("kind")
        if not isinstance(kind, str) or not kind or "/" in kind or "\\" in kind:
            raise TemporalQDV5NativeError(f"v5 output artifact {index} kind is invalid")
        if previous_kind is not None and kind <= previous_kind:
            raise TemporalQDV5NativeError("v5 output artifact kinds must be strictly ordered")
        previous_kind = kind
        relative_path = _safe_relative_output_path(
            artifact.get("relativePath"), name=f"v5 output artifact {kind} path"
        )
        if relative_path in paths:
            raise TemporalQDV5NativeError("v5 output artifact paths must be unique")
        paths.add(relative_path)
        _sha(artifact.get("fileSha256"), name=f"v5 output artifact {kind} SHA")
        _sha(artifact.get("semanticSha256"), name=f"v5 output artifact {kind} semantic SHA")
        _nonnegative(artifact.get("byteLength"), name=f"v5 output artifact {kind} bytes")
        by_kind[kind] = artifact
    for kind, required_path, semantic_key in required_artifacts:
        artifact = by_kind.get(kind)
        if (
            artifact is None
            or artifact["relativePath"] != required_path
            or (
                semantic_key is not None
                and artifact["semanticSha256"] != semantic_roots.get(semantic_key)
            )
        ):
            raise TemporalQDV5NativeError(f"v5 output artifact {kind} binding drifted")
    if len(by_kind) != len(required_artifacts):
        raise TemporalQDV5NativeError(
            "v5 proposal output inventory contains an undeclared artifact kind"
        )
    object_store = _validate_v5_object_store_inventory(inventory.get("objectStore"))
    required_object_roots: tuple[tuple[str, str, str], ...]
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        required_object_roots = (
            ("g0FunnelFragments", "g0FunnelFragmentsSha256", "v5 G0 funnel fragments root"),
            (
                "g0FunnelProjectionStreamReceipt",
                "g0FunnelProjectionStreamReceiptSha256",
                "v5 G0 funnel projection-stream receipt root",
            ),
            ("publicationPlan", "publicationPlanSha256", "v5 publication plan root"),
        )
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        required_object_roots = (
            (
                "publicationFragments",
                "publicationFragmentsSha256",
                "v5 evolved publication fragments root",
            ),
            (
                "publicationPlan",
                "publicationPlanSha256",
                "v5 publication plan root",
            ),
            (
                "publicationReceipt",
                "publicationReceiptSha256",
                "v5 evolved publication receipt root",
            ),
            ("transaction", "transactionSha256", "v5 evolved transaction root"),
        )
    else:
        raise TemporalQDV5NativeError("v5 proposal object-store generation kind is incompatible")
    roots = object_store["roots"]
    if not isinstance(roots, list) or len(roots) != len(required_object_roots):
        raise TemporalQDV5NativeError(
            "v5 proposal object-store root roles are not the exact current closure"
        )
    for root_entry, (role, semantic_key, label) in zip(
        roots, required_object_roots, strict=True
    ):
        entry = _mapping(root_entry, name=f"{label} descriptor")
        if entry.get("role") != role:
            raise TemporalQDV5NativeError(
                "v5 proposal object-store root roles are not the exact current closure"
            )
        object_sha256 = _sha(
            semantic_roots.get(semantic_key),
            name=label,
        )
        expected_path = f"sha256/{object_sha256.removeprefix('sha256:')}.json"
        if (
            entry.get("relativePath") != expected_path
            or entry.get("objectSha256") != object_sha256
        ):
            raise TemporalQDV5NativeError(
                f"{label} does not resolve to a real immutable object-store entry"
            )
    return inventory


def _validate_v5_adoption_telemetry_with_schema(
    value: object,
    *,
    manifest: Mapping[str, Any],
    expected_schema: str,
    label: str,
) -> dict[str, Any]:
    telemetry = _mapping(value, name=label)
    _exact_keys(
        telemetry,
        {
            "schemaVersion",
            "executionPath",
            "validationMode",
            "authenticationStrategy",
            "phases",
            "processCpuMilliseconds",
            "cpuUtilizationMilliCores",
            "publicArtifactBytesRead",
            "objectStoreBytesRead",
            "authenticatedFileCount",
            "io",
            "validationPasses",
            "parallelAuthenticationWorkers",
            "proposalReconstructionCount",
            "legacyRichExpansionCount",
            "processTree",
            "threadCap",
            "constructionPrefetchMultiplier",
        },
        name=label,
    )
    if (
        telemetry.get("schemaVersion") != expected_schema
        or telemetry.get("threadCap") != manifest["threadCap"]
        or telemetry.get("constructionPrefetchMultiplier") != 16
    ):
        raise TemporalQDV5NativeError(f"{label} is incompatible")
    execution_path = telemetry.get("executionPath")
    validation_mode = telemetry.get("validationMode")
    strategy = telemetry.get("authenticationStrategy")
    if (
        execution_path not in {"fresh", "sealed_restart", "receipt_recovery"}
        or validation_mode not in {"balanced", "strict"}
        or strategy
        not in {
            "fresh_publication_proof",
            "receipt_bound_content",
            "strict_deep_replay",
        }
        or (validation_mode == "strict" and strategy != "strict_deep_replay")
        or (
            validation_mode == "balanced"
            and execution_path == "fresh"
            and strategy != "fresh_publication_proof"
        )
        or (
            validation_mode == "balanced"
            and execution_path != "fresh"
            and strategy != "receipt_bound_content"
        )
    ):
        raise TemporalQDV5NativeError(
            f"{label} path/mode/authentication strategy is incompatible"
        )
    phases = _mapping(telemetry.get("phases"), name="v5 adoption phases")
    _exact_keys(
        phases,
        {
            "staticAuthorityMilliseconds",
            "constructionMilliseconds",
            "stagingMilliseconds",
            "prepublicationValidationMilliseconds",
            "publicationMilliseconds",
            "outputAuthenticationMilliseconds",
            "totalMilliseconds",
        },
        name="v5 adoption phases",
    )
    for key, item in phases.items():
        _nonnegative(item, name=f"v5 adoption phases {key}")
    for key in ("processCpuMilliseconds", "cpuUtilizationMilliCores"):
        item = telemetry.get(key)
        if item is not None:
            _nonnegative(item, name=f"v5 adoption telemetry {key}")
    for key in (
        "publicArtifactBytesRead",
        "objectStoreBytesRead",
        "authenticatedFileCount",
        "parallelAuthenticationWorkers",
    ):
        _nonnegative(telemetry.get(key), name=f"v5 adoption telemetry {key}")
    if telemetry["authenticatedFileCount"] <= 0:
        raise TemporalQDV5NativeError("v5 adoption must authenticate at least one file")
    if telemetry["parallelAuthenticationWorkers"] > manifest["threadCap"]:
        raise TemporalQDV5NativeError("v5 adoption authentication worker cap drifted")
    if strategy == "receipt_bound_content" and telemetry["parallelAuthenticationWorkers"] <= 0:
        raise TemporalQDV5NativeError(
            "v5 receipt-bound authentication records no workers"
        )
    io = _mapping(telemetry.get("io"), name="v5 adoption I/O telemetry")
    _exact_keys(
        io,
        {"filesReopened", "bytesRead", "bytesHashed", "bytesWritten", "jsonRowsParsed"},
        name="v5 adoption I/O telemetry",
    )
    for key, item in io.items():
        _nonnegative(item, name=f"v5 adoption I/O telemetry {key}")
    passes = _mapping(
        telemetry.get("validationPasses"), name="v5 adoption validation passes"
    )
    _exact_keys(
        passes,
        {
            "constructorReplay",
            "redundantFreshReplay",
            "publicationPrepareReplay",
            "stagedSemanticReplay",
            "stagedFinalRehash",
            "receiptBoundContentAuthentication",
            "deepOutputReplay",
        },
        name="v5 adoption validation passes",
    )
    for key, item in passes.items():
        _nonnegative(item, name=f"v5 adoption validation passes {key}")
    for key in ("proposalReconstructionCount", "legacyRichExpansionCount"):
        if _nonnegative(telemetry.get(key), name=f"v5 adoption telemetry {key}") != 0:
            raise TemporalQDV5NativeError(
                f"v5 adoption telemetry records forbidden nonzero {key}"
            )
    process_tree = _mapping(
        telemetry.get("processTree"), name="v5 adoption process-tree evidence"
    )
    _exact_keys(
        process_tree,
        {
            "measurement",
            "peakRssBytes",
            "peakPrivateBytes",
            "pythonChildCount",
            "dashboardChildCount",
        },
        name="v5 adoption process-tree evidence",
    )
    measurement = process_tree.get("measurement")
    if measurement == "windows_peak_process_memory_v1":
        for key in ("peakRssBytes", "peakPrivateBytes"):
            if _positive(process_tree.get(key), name=f"v5 Windows process-tree {key}") <= 0:
                raise TemporalQDV5NativeError(
                    f"v5 Windows process-tree {key} must be nonzero"
                )
    elif measurement == "unavailable_non_windows_v1":
        for key in ("peakRssBytes", "peakPrivateBytes"):
            if process_tree.get(key) is not None:
                raise TemporalQDV5NativeError(
                    f"v5 non-Windows process-tree {key} must be null"
                )
    else:
        raise TemporalQDV5NativeError("v5 process-tree measurement is incompatible")
    for key in ("pythonChildCount", "dashboardChildCount"):
        if _nonnegative(process_tree.get(key), name=f"v5 process-tree {key}") != 0:
            raise TemporalQDV5NativeError(
                f"v5 process-tree records forbidden nonzero {key}"
            )
    return telemetry


def _validate_v5_adoption_telemetry(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    return _validate_v5_adoption_telemetry_with_schema(
        value,
        manifest=manifest,
        expected_schema=V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        label="v5 proposal adoption telemetry",
    )


def _validate_v5_adoption_evidence_with_schemas(
    value: object,
    *,
    manifest: Mapping[str, Any],
    immutable_result: Mapping[str, Any],
    result_validator: Callable[..., dict[str, Any]],
    generation_kind: str,
    evidence_schema: str,
    telemetry_schema: str,
    label: str,
) -> dict[str, Any]:
    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != generation_kind:
        raise TemporalQDV5NativeError(f"{label} has an incompatible generation kind")
    checked_result = result_validator(
        immutable_result, manifest=checked_manifest
    )
    evidence = _mapping(value, name=label)
    _exact_keys(
        evidence,
        {
            "schemaVersion",
            "operation",
            "status",
            "authoritySha256",
            "expectedAuthoritySha256",
            "manifestSha256",
            "immutableResultSha256",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "nativeExecutableSha256",
            "nativeSourceSha256",
            "telemetry",
            "adoptionEvidenceSha256",
        },
        name=label,
    )
    batch = checked_manifest["executionAuthority"]["nativeBatchAuthority"]
    if (
        evidence.get("schemaVersion") != evidence_schema
        or evidence.get("operation") != V5_PROPOSAL_OPERATION
        or evidence.get("status") != "adopted"
        or evidence.get("authoritySha256") != checked_manifest["authoritySha256"]
        or evidence.get("expectedAuthoritySha256")
        != checked_manifest["expectedAuthoritySha256"]
        or evidence.get("manifestSha256") != checked_manifest["manifestSha256"]
        or evidence.get("immutableResultSha256") != checked_result["resultSha256"]
        or evidence.get("outputInventorySha256")
        != checked_result["outputInventorySha256"]
        or evidence.get("nativeBatchAuthoritySha256")
        != checked_manifest["executionAuthority"]["nativeBatchAuthoritySha256"]
        or evidence.get("nativeExecutableSha256") != batch["executableSha256"]
        or evidence.get("nativeSourceSha256") != batch["sourceSha256"]
    ):
        raise TemporalQDV5NativeError(f"{label} binding drifted")
    for key in (
        "authoritySha256",
        "expectedAuthoritySha256",
        "manifestSha256",
        "immutableResultSha256",
        "outputInventorySha256",
        "nativeBatchAuthoritySha256",
        "nativeExecutableSha256",
        "nativeSourceSha256",
    ):
        _sha(evidence.get(key), name=f"{label} {key}")
    _validate_v5_adoption_telemetry_with_schema(
        evidence.get("telemetry"),
        manifest=checked_manifest,
        expected_schema=telemetry_schema,
        label=f"{label} telemetry",
    )
    return _self_hashed_object(
        evidence,
        identity_field="adoptionEvidenceSha256",
        name=label,
    )


def validate_v5_proposal_adoption_evidence(
    value: object,
    *,
    manifest: Mapping[str, Any],
    immutable_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate stdout-only G0 execution/adoption evidence."""

    return _validate_v5_adoption_evidence_with_schemas(
        value,
        manifest=manifest,
        immutable_result=immutable_result,
        result_validator=validate_v5_proposal_result,
        generation_kind=V5_PROPOSAL_GENERATION_G0,
        evidence_schema=V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        telemetry_schema=V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        label="v5 proposal adoption evidence",
    )


def validate_v5_evolved_proposal_adoption_evidence(
    value: object,
    *,
    manifest: Mapping[str, Any],
    immutable_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate stdout-only later-generation execution/adoption evidence."""

    return _validate_v5_adoption_evidence_with_schemas(
        value,
        manifest=manifest,
        immutable_result=immutable_result,
        result_validator=validate_v5_evolved_proposal_result,
        generation_kind=V5_PROPOSAL_GENERATION_EVOLVED,
        evidence_schema=V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        telemetry_schema=V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        label="v5 evolved proposal adoption evidence",
    )


def _validate_v5_construction_summary(
    value: object,
    *,
    attempt_count: int,
    accepted_record_count: int,
) -> dict[str, Any]:
    summary = _mapping(value, name="v5 proposal construction summary")
    _exact_keys(
        summary,
        {
            "schemaVersion",
            "bytes",
            "attempts",
            "uniqueCounts",
        },
        name="v5 proposal construction summary",
    )
    if summary.get("schemaVersion") != V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA:
        raise TemporalQDV5NativeError("v5 proposal construction summary schema drifted")
    byte_counts = _mapping(summary["bytes"], name="v5 construction-summary bytes")
    _exact_keys(
        byte_counts,
        {
            "compactJournalBytes",
            "staticAuthorityBytes",
            "objectStoreBytes",
            "selectedProjectionBytes",
        },
        name="v5 construction-summary bytes",
    )
    for key, count in byte_counts.items():
        _nonnegative(count, name=f"v5 construction-summary bytes {key}")
    attempts = _mapping(summary["attempts"], name="v5 construction-summary attempts")
    _exact_keys(
        attempts,
        {"byDisposition", "byReason"},
        name="v5 construction-summary attempts",
    )
    dispositions, disposition_total = _count_mapping(
        attempts["byDisposition"], name="v5 construction-summary dispositions"
    )
    _, reason_total = _count_mapping(
        attempts["byReason"], name="v5 construction-summary reasons"
    )
    if (
        disposition_total != attempt_count
        or reason_total != attempt_count
        or dispositions.get("accepted") != accepted_record_count
    ):
        raise TemporalQDV5NativeError(
            "v5 proposal construction summary attempt counts drifted"
        )
    unique = _mapping(
        summary["uniqueCounts"], name="v5 construction-summary unique counts"
    )
    _exact_keys(
        unique,
        {"candidateCount", "programCount", "topologyCount", "resourceCount"},
        name="v5 construction-summary unique counts",
    )
    for key, count in unique.items():
        _nonnegative(count, name=f"v5 construction-summary unique count {key}")
    if unique["candidateCount"] != accepted_record_count:
        raise TemporalQDV5NativeError(
            "v5 proposal construction summary unique candidate count drifts from accepted records"
        )
    return summary


def _self_hashed_object(
    value: Mapping[str, Any], *, identity_field: str, name: str
) -> dict[str, Any]:
    result = dict(value)
    supplied = _sha(result.get(identity_field), name=f"{name} {identity_field}")
    material = {key: item for key, item in result.items() if key != identity_field}
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDV5NativeError(f"{name} identity mismatch")
    return result


def _identity_snapshot(
    value: object, *, name: str, expected_kind: str | None = None
) -> dict[str, Any]:
    """Authenticate a complete immutable snapshot, including its payload.

    A snapshot hash is deliberately not treated as a trusted pointer.  Its
    payload stays in the shared v5 authority so the Rust core can run after
    the source config files are gone, and this check prevents a self-rehashed
    outer authority from swapping one nested object silently.
    """

    snapshot = _mapping(value, name=name)
    _exact_keys(snapshot, {"schemaVersion", "kind", "payload", "sha256"}, name=name)
    if not isinstance(snapshot["schemaVersion"], str) or not snapshot["schemaVersion"]:
        raise TemporalQDV5NativeError(f"{name} schemaVersion is invalid")
    if not isinstance(snapshot["kind"], str) or not snapshot["kind"]:
        raise TemporalQDV5NativeError(f"{name} kind is invalid")
    if expected_kind is not None and snapshot["kind"] != expected_kind:
        raise TemporalQDV5NativeError(f"{name} kind is incompatible")
    payload = _mapping(snapshot["payload"], name=f"{name} payload")
    supplied = _sha(snapshot["sha256"], name=f"{name} sha256")
    if sha256(canonical_json_bytes(payload)) != supplied:
        raise TemporalQDV5NativeError(f"{name} payload identity mismatch")
    return {
        "schemaVersion": str(snapshot["schemaVersion"]),
        "kind": str(snapshot["kind"]),
        "payload": payload,
        "sha256": supplied,
    }


def _snapshot(*, schema_version: str, kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    body = _mapping(payload, name=f"v5 {kind} snapshot payload")
    return {
        "schemaVersion": schema_version,
        "kind": kind,
        "payload": body,
        "sha256": sha256(canonical_json_bytes(body)),
    }


def _indicator_policy_timeframe_policy(
    catalog: object,
    indicator_policy: object,
    *,
    name: str,
) -> tuple[dict[str, Any], dict[str, Any], list[str], str]:
    """Open the exact side policy used by the historical resource layer.

    ``IndicatorLearningCatalog`` canonicalizes a supplied policy by uppercasing
    it and sorting its distinct members.  We reproduce that historical rule
    here, but reject a duplicate source policy rather than quietly collapsing
    it: the sealed source policy is an authority, not a user convenience API.
    """

    catalog_body = _mapping(catalog, name=name)
    policy = _self_hashed_object(
        _mapping(indicator_policy, name=f"{name} indicator policy"),
        identity_field="policySha256",
        name=f"{name} indicator policy",
    )
    _exact_keys(
        policy,
        {
            "schemaVersion",
            "learningVersion",
            "catalogSha256",
            "timeframePolicy",
            "evidenceLookbackChoices",
            "maxBoundFuzzyInstancesPerDirection",
            "maxEvidenceGroupMembers",
            "operatorIds",
            "policySha256",
        },
        name=f"{name} indicator policy",
    )
    if policy.get("schemaVersion") != "temporal_indicator_learning_policy_v1":
        raise TemporalQDV5NativeError(f"{name} indicator policy schema is incompatible")
    timeframes = _mapping(catalog_body.get("timeframes"), name=f"{name} timeframes")
    raw_timeframe_policy = policy.get("timeframePolicy")
    if not isinstance(raw_timeframe_policy, list) or not raw_timeframe_policy:
        raise TemporalQDV5NativeError(f"{name} indicator timeframe policy is invalid")
    timeframe_policy: list[str] = []
    for raw in raw_timeframe_policy:
        if not isinstance(raw, str) or not raw.strip():
            raise TemporalQDV5NativeError(f"{name} indicator timeframe policy is invalid")
        timeframe_policy.append(raw.upper())
    if len(set(timeframe_policy)) != len(timeframe_policy):
        raise TemporalQDV5NativeError(
            f"{name} indicator timeframe policy contains duplicate frames"
        )
    available = {str(key).upper() for key in timeframes if str(key).strip()}
    if any(frame not in available for frame in timeframe_policy):
        raise TemporalQDV5NativeError(
            f"{name} indicator timeframe policy is not catalog-backed"
        )
    # This sorted form is the exact one held by IndicatorLearningCatalog and
    # therefore the one used by the historical public resource-plan identity.
    canonical_policy = sorted(timeframe_policy)
    catalog_identity = sha256(
        canonical_json_bytes(
            {"payload": catalog_body, "timeframePolicy": canonical_policy}
        )
    )
    if _sha(policy.get("catalogSha256"), name=f"{name} indicator catalog identity") != catalog_identity:
        raise TemporalQDV5NativeError(f"{name} indicator catalog/policy binding drifted")
    return catalog_body, policy, canonical_policy, catalog_identity


def _resource_operator_spec_sha256(
    catalog: object,
    *,
    indicator_policy: object,
    name: str,
) -> str:
    """Reproduce the historical policy-snapshot resource-spec identity.

    This is a small canonical hash calculation, not a candidate factory or
    Python validator.  Its timeframe set comes from the sealed indicator
    policy—not a bridge default—so the public policy identity follows exactly
    the source authority used to build it.
    """

    _, _, timeframe_policy, catalog_identity = _indicator_policy_timeframe_policy(
        catalog,
        indicator_policy,
        name=name,
    )
    specification = {
        "schemaVersion": "evolvable_module_resource_operator_plan_v1",
        "operatorVersion": "evolvable_module_resource_operators_v1",
        "catalogSha256": catalog_identity,
        "timeframePolicy": timeframe_policy,
        "rawEvents": "fresh_only_v1",
        "weights": {
            "positive": True,
            "normalizedWithinExclusiveGroup": True,
            "minimum": 0.25,
        },
    }
    specification["operatorSpecSha256"] = sha256(canonical_json_bytes(specification))
    return str(specification["operatorSpecSha256"])


def _temporal_domain_values(value: object, *, name: str) -> list[int]:
    values = value
    if not isinstance(values, list) or not values:
        raise TemporalQDV5NativeError(f"{name} must be a nonempty array")
    checked: list[int] = []
    for index, item in enumerate(values):
        checked.append(_nonnegative(item, name=f"{name} {index}"))
    if checked != sorted(set(checked)):
        raise TemporalQDV5NativeError(f"{name} must be strictly ordered and unique")
    return checked


def _temporal_domain_windows(value: object, *, name: str, maximum: int) -> list[list[int]]:
    values = value
    if not isinstance(values, list) or not values:
        raise TemporalQDV5NativeError(f"{name} must be a nonempty array")
    checked: list[list[int]] = []
    for index, item in enumerate(values):
        if not isinstance(item, list) or len(item) != 2:
            raise TemporalQDV5NativeError(f"{name} {index} must be a two-integer array")
        start = _nonnegative(item[0], name=f"{name} {index} start")
        end = _nonnegative(item[1], name=f"{name} {index} end")
        if start > maximum or end > maximum or start > end:
            raise TemporalQDV5NativeError(f"{name} {index} is outside its sealed range")
        checked.append([start, end])
    if len({tuple(item) for item in checked}) != len(checked):
        raise TemporalQDV5NativeError(f"{name} contains duplicate intervals")
    return checked


def _temporal_domains_from_operator_specification(
    specification: Mapping[str, Any], *, name: str
) -> dict[str, list[Any]]:
    domains = _mapping(specification.get("domains"), name=f"{name} domains")
    _exact_keys(
        domains,
        {
            "eventAges",
            "positionAges",
            "utcSessionWindows",
            "eventAgeWindows",
            "consecutiveCounts",
            "cooldownCounts",
        },
        name=f"{name} domains",
    )
    return {
        "eventAges": _temporal_domain_values(
            domains["eventAges"], name=f"{name} event ages"
        ),
        "positionAges": _temporal_domain_values(
            domains["positionAges"], name=f"{name} position ages"
        ),
        "utcSessionWindows": _temporal_domain_windows(
            domains["utcSessionWindows"],
            name=f"{name} UTC session windows",
            maximum=1439,
        ),
        "eventAgeWindows": _temporal_domain_windows(
            domains["eventAgeWindows"],
            name=f"{name} event age windows",
            maximum=1_000_000,
        ),
        "consecutiveCounts": _temporal_domain_values(
            domains["consecutiveCounts"], name=f"{name} consecutive counts"
        ),
        "cooldownCounts": _temporal_domain_values(
            domains["cooldownCounts"], name=f"{name} cooldown counts"
        ),
    }


def _temporal_operator_specification(
    value: object,
    *,
    compiler_policy_sha256: str,
    name: str,
) -> dict[str, Any]:
    specification = _self_hashed_object(
        _mapping(value, name=name),
        identity_field="operatorSpecSha256",
        name=name,
    )
    _exact_keys(
        specification,
        {
            "schemaVersion",
            "operatorVersion",
            "domains",
            "guardFamilies",
            "compilerPolicySha256",
            "nativeValidation",
            "operatorSpecSha256",
        },
        name=name,
    )
    if (
        specification.get("schemaVersion")
        != "evolvable_module_temporal_operator_plan_v1"
        or specification.get("operatorVersion")
        != "evolvable_module_temporal_operators_v1"
        or specification.get("compilerPolicySha256")
        != _sha(compiler_policy_sha256, name=f"{name} compiler policy SHA")
        or specification.get("nativeValidation") is not False
    ):
        raise TemporalQDV5NativeError(f"{name} binding is incompatible")
    guard_families = specification.get("guardFamilies")
    if (
        not isinstance(guard_families, list)
        or not guard_families
        or not all(isinstance(item, str) and item for item in guard_families)
        or len(set(guard_families)) != len(guard_families)
    ):
        raise TemporalQDV5NativeError(f"{name} guard families are invalid")
    _temporal_domains_from_operator_specification(specification, name=name)
    return specification


def _source_temporal_operator_specification(
    *, compiler_policy_sha256: str
) -> dict[str, Any]:
    """Open the public temporal operator specification exactly once at seal time.

    The v5 bridge does not copy grids from private implementation constants.
    The public operator object includes its compiler binding and self-hash, so
    changing the source operator cannot silently keep producing an old native
    closure.  Resume/adoption uses only the sealed object below and never
    imports this Python surface.
    """

    from .evolvable_module_temporal_operators import GenomeTemporalOperatorLayer

    return _temporal_operator_specification(
        GenomeTemporalOperatorLayer().specification,
        compiler_policy_sha256=compiler_policy_sha256,
        name="v5 source temporal operator specification",
    )


def _temporal_domains(value: object) -> dict[str, Any]:
    domains = _self_hashed_object(
        _mapping(value, name="v5 temporal domains"),
        identity_field="temporalDomainsSha256",
        name="v5 temporal domains",
    )
    _exact_keys(
        domains,
        {
            "schemaVersion",
            "eventAges",
            "positionAges",
            "utcSessionWindows",
            "eventAgeWindows",
            "consecutiveCounts",
            "cooldownCounts",
            "temporalDomainsSha256",
        },
        name="v5 temporal domains",
    )
    if domains.get("schemaVersion") != V5_TEMPORAL_DOMAINS_SCHEMA:
        raise TemporalQDV5NativeError("v5 temporal domains schema is incompatible")
    _temporal_domains_from_operator_specification(
        {
            "domains": {
                key: domains[key]
                for key in (
                    "eventAges",
                    "positionAges",
                    "utcSessionWindows",
                    "eventAgeWindows",
                    "consecutiveCounts",
                    "cooldownCounts",
                )
            }
        },
        name="v5 temporal domains",
    )
    return domains


def _build_v5_temporal_domains(
    specification: Mapping[str, Any], *, name: str
) -> dict[str, Any]:
    domains: dict[str, Any] = {
        "schemaVersion": V5_TEMPORAL_DOMAINS_SCHEMA,
        **_temporal_domains_from_operator_specification(specification, name=name),
    }
    domains["temporalDomainsSha256"] = sha256(canonical_json_bytes(domains))
    return _temporal_domains(domains)


def _pair_source_authority(value: object) -> dict[str, Any]:
    """Open only the self-authenticating source authority, not an enriched view."""

    run = _self_hashed_object(
        _mapping(value, name="v5 pair source authority"),
        identity_field="pairRunConfigSha256",
        name="v5 pair source authority",
    )
    if run.get("schemaVersion") != "temporal_qd_bidirectional_pair_run_config_v2":
        raise TemporalQDV5NativeError("v5 pair source authority schema is incompatible")
    required = {
        "schemaVersion",
        "longModule",
        "shortModule",
        "nativeAuthority",
        "nativeJsonlAuthority",
        "pairCompilerAuthority",
        "holdOperatorPolicy",
        "initialProtectionOperatorPolicy",
        "immigrantConstructionPolicy",
        "grammarRegistry",
        "operatorImplementation",
        "pairRunConfigSha256",
    }
    _exact_keys(run, required, name="v5 pair source authority")
    return run


def _evolvable_authority(value: object, *, pair_run_config_sha256: str) -> dict[str, Any]:
    authority = _mapping(value, name="v5 evolvable module authority")
    required = {
        "schemaVersion",
        "programKind",
        "codec",
        "pairRunConfigSha256",
        "catalogSha256",
        "compilerPolicy",
        "compilerPolicySha256",
        "budget",
        "capacityContract",
        "archivePolicyAuthority",
        "behaviorAttributionRequirement",
        "operatorRegistry",
        "authoritySha256",
    }
    optional = {"capacityReceipt"}
    if set(authority) - optional != required:
        raise TemporalQDV5NativeError("v5 evolvable module authority fields are not exact")
    if (
        authority.get("schemaVersion") != "temporal_qd_evolvable_module_authority_v1"
        or authority.get("programKind") != "evolvable_module_genome_v1"
        or authority.get("codec") != "evolvable_module_genome_json_v1"
        or authority.get("pairRunConfigSha256") != pair_run_config_sha256
    ):
        raise TemporalQDV5NativeError("v5 evolvable module authority is incompatible")
    supplied = _sha(authority.get("authoritySha256"), name="v5 evolvable authoritySha256")
    material = {
        key: item
        for key, item in authority.items()
        if key not in {"authoritySha256", "capacityReceipt"}
    }
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDV5NativeError("v5 evolvable module authority identity mismatch")
    compiler_policy = _mapping(authority["compilerPolicy"], name="v5 compiler policy")
    if sha256(canonical_json_bytes(compiler_policy)) != _sha(
        authority["compilerPolicySha256"], name="v5 compilerPolicySha256"
    ):
        raise TemporalQDV5NativeError("v5 compiler policy identity mismatch")
    budget = _mapping(authority["budget"], name="v5 evolvable authority budget")
    budget_keys = {
        "maxStates",
        "maxTransitions",
        "maxEvidenceGroups",
        "maxGroupMembers",
        "maxEvents",
        "maxIndicators",
        "maxEntryBranches",
        "maxManagementRegions",
        "maxExitRegions",
        "maxRecoveryRegions",
        "maxSccNodes",
        "maxTimeoutBars",
        "maxGuardDepth",
    }
    _exact_keys(budget, budget_keys, name="v5 evolvable authority budget")
    for key in budget_keys:
        _positive(budget[key], name=f"v5 evolvable authority budget {key}")
    return authority


def _v5_capacity_receipt(
    value: object,
    *,
    evolvable_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Authenticate the sealed capacity witness without reopening a factory.

    ``EvolvableModulePairAuthority.generation_bindings`` historically checked
    this receipt through a live factory.  The native v5 control path must not
    reopen that factory, compiler, or operator.  The receipt is nevertheless
    a self-hashed, authority-bound witness, so verify every static binding
    which is available from the frozen authority before carrying it forward.
    """

    receipt = _mapping(value, name="v5 evolvable capacity receipt")
    supplied = _sha(
        receipt.get("semanticReceiptSha256"),
        name="v5 evolvable capacity receipt identity",
    )
    material = {
        key: item for key, item in receipt.items() if key != "semanticReceiptSha256"
    }
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDV5NativeError("v5 evolvable capacity receipt identity mismatch")
    expected = {
        "schemaVersion": "temporal_qd_evolvable_module_capacity_receipt_v1",
        "authoritySha256": evolvable_authority["authoritySha256"],
        "pairRunConfigSha256": evolvable_authority["pairRunConfigSha256"],
        "catalogSha256": evolvable_authority["catalogSha256"],
        "programKind": evolvable_authority["programKind"],
        "codec": evolvable_authority["codec"],
        "compilerPolicySha256": evolvable_authority["compilerPolicySha256"],
        "operatorRegistrySha256": sha256(
            canonical_json_bytes(evolvable_authority["operatorRegistry"])
        ),
        "capacityContract": evolvable_authority["capacityContract"],
        "admission": "native_v2_then_compiled_v3_no_market_v1",
    }
    required = {
        *expected,
        "factoryPolicySha256",
        "noMarket",
        "previewStreamSize",
        "rawPreview",
        "compiledAdmittedCandidateCount",
        "uniqueSemanticPairCount",
        "uniqueCompiledV3ProfileCount",
        "nativeOrCompilerRejectionCounts",
        "perSide",
        "passed",
        "semanticReceiptSha256",
    }
    _exact_keys(receipt, required, name="v5 evolvable capacity receipt")
    if any(receipt.get(key) != expected_value for key, expected_value in expected.items()):
        raise TemporalQDV5NativeError("v5 evolvable capacity receipt binding drifted")
    _sha(
        receipt.get("factoryPolicySha256"),
        name="v5 evolvable capacity receipt factory policy",
    )
    if receipt.get("noMarket") is not True or receipt.get("passed") is not True:
        raise TemporalQDV5NativeError(
            "v5 evolvable capacity receipt is not a passing no-market admission"
        )
    return deepcopy(receipt)


def _source_operator_implementation(value: object) -> dict[str, Any]:
    """Preserve the historical source identity without treating it as executable."""

    implementation = _mapping(value, name="v5 source operator implementation")
    if implementation.get("schemaVersion") != "temporal_qd_pair_operator_implementation_v4":
        raise TemporalQDV5NativeError("v5 source operator implementation is incompatible")
    return implementation


def build_v5_native_operator_authority(
    *,
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive the Rust-executable operator closure from frozen static inputs.

    The legacy source operator remains a separately sealed public/audit
    identity.  This object is deliberately a different authority: it contains
    every static policy that Rust needs for construction, so no dashboard or
    Python validator authority becomes an executable dependency.
    """

    source = _pair_source_authority(pair_source_authority)
    pair_run_config_sha256 = _sha(
        source["pairRunConfigSha256"], name="v5 pair source authority identity"
    )
    evolvable = _evolvable_authority(
        evolvable_module_authority, pair_run_config_sha256=pair_run_config_sha256
    )
    source_operator = _source_operator_implementation(source["operatorImplementation"])
    temporal_operator_specification = _source_temporal_operator_specification(
        compiler_policy_sha256=evolvable["compilerPolicySha256"]
    )
    authority: dict[str, Any] = {
        "schemaVersion": V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA,
        "sourceOperatorImplementationSha256": sha256(
            canonical_json_bytes(source_operator)
        ),
        "factoryAuthoritySha256": evolvable["authoritySha256"],
        "compilerPolicySha256": evolvable["compilerPolicySha256"],
        "programKind": evolvable["programKind"],
        "codec": evolvable["codec"],
        "operatorRegistry": evolvable["operatorRegistry"],
        "budget": evolvable["budget"],
        "grammarRegistry": source["grammarRegistry"],
        "holdOperatorPolicy": source["holdOperatorPolicy"],
        "initialProtectionOperatorPolicy": source["initialProtectionOperatorPolicy"],
        "immigrantConstructionPolicy": source["immigrantConstructionPolicy"],
        "temporalOperatorSpecification": temporal_operator_specification,
        "temporalDomains": _build_v5_temporal_domains(
            temporal_operator_specification,
            name="v5 native temporal operator specification",
        ),
    }
    authority["nativeOperatorAuthoritySha256"] = sha256(canonical_json_bytes(authority))
    return _native_operator_authority(
        authority,
        source_operator_implementation=source_operator,
        evolvable_authority=evolvable,
        source=source,
    )


def build_v5_bidirectional_pair_policy(
    *, pair_source_authority: Mapping[str, Any]
) -> dict[str, Any]:
    """Derive the exact static pair-policy projection for a v5 invocation.

    This is intentionally the small, pure projection historically produced by
    ``pair_policy_from_config``.  It consumes only the source authority and
    never opens the authority's executable runtime.
    """

    source = _pair_source_authority(pair_source_authority)
    compiler_authority = _identity_snapshot(
        source["pairCompilerAuthority"],
        name="v5 pair source compiler authority",
        expected_kind="pairCompiler",
    )
    return {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": deepcopy(compiler_authority),
    }


def build_v5_generation_bindings(
    *,
    generation_run_config: Mapping[str, Any],
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Pure v5 equivalent of ``generation_bindings`` for the Rust boundary.

    The source pair authority remains a self-hashed historical/static input.
    We deliberately do not edit or rehash it.  Instead this adapts the
    caller-owned generation ``runConfig`` with the exact v5 archive, behavior,
    capacity, and operator identity projections.  It has no dependency on a
    live ``PairAuthorityBundle`` or candidate-producing object.
    """

    source = _pair_source_authority(pair_source_authority)
    pair_run_config_sha256 = _sha(
        source["pairRunConfigSha256"], name="v5 pair source authority identity"
    )
    evolvable = _evolvable_authority(
        evolvable_module_authority,
        pair_run_config_sha256=pair_run_config_sha256,
    )
    frozen = deepcopy(_mapping(generation_run_config, name="v5 generation run config"))
    archive = deepcopy(
        _mapping(
            evolvable["archivePolicyAuthority"],
            name="v5 archive policy authority",
        )
    )
    behavior = deepcopy(
        _mapping(
            evolvable["behaviorAttributionRequirement"],
            name="v5 behavior attribution requirement",
        )
    )
    behavior_sha256 = _sha(
        behavior.get("requirementSha256"),
        name="v5 behavior attribution requirement identity",
    )
    receipt = (
        _v5_capacity_receipt(
            evolvable["capacityReceipt"], evolvable_authority=evolvable
        )
        if "capacityReceipt" in evolvable
        else None
    )
    operator_implementation: dict[str, Any] = {
        "schemaVersion": "temporal_qd_evolvable_module_operator_implementation_v1",
        "authoritySha256": evolvable["authoritySha256"],
        "programKind": evolvable["programKind"],
        "codec": evolvable["codec"],
        "compilerPolicySha256": evolvable["compilerPolicySha256"],
        "operatorRegistry": deepcopy(evolvable["operatorRegistry"]),
        "budget": deepcopy(evolvable["budget"]),
        "capacityContract": deepcopy(evolvable["capacityContract"]),
        "archivePolicyAuthoritySha256": sha256(canonical_json_bytes(archive)),
        "behaviorAttributionRequirementSha256": behavior_sha256,
    }
    if receipt is not None:
        operator_implementation["capacityReceiptSha256"] = receipt[
            "semanticReceiptSha256"
        ]
    operator_implementation["operatorImplementationSha256"] = sha256(
        canonical_json_bytes(operator_implementation)
    )
    for field, expected in (
        ("archivePolicyAuthority", archive),
        ("behaviorAttributionRequirement", behavior),
    ):
        supplied = frozen.get(field)
        if supplied is not None and supplied != expected:
            raise TemporalQDV5NativeError(f"v5 generation {field} drifted")
        frozen[field] = expected
    if receipt is not None:
        supplied_receipt = frozen.get("capacityReceipt")
        if supplied_receipt is not None and supplied_receipt != receipt:
            raise TemporalQDV5NativeError("v5 generation capacityReceipt drifted")
        frozen["capacityReceipt"] = receipt
    supplied_operator = frozen.get("operatorImplementation")
    if supplied_operator is not None and supplied_operator != operator_implementation:
        raise TemporalQDV5NativeError("v5 generation operatorImplementation drifted")
    frozen["operatorImplementation"] = operator_implementation
    return {
        "runConfig": frozen,
        "archivePolicyAuthority": archive,
        "behaviorAttributionRequirement": behavior,
        "operatorImplementation": operator_implementation,
        "capacityReceipt": receipt,
    }


def _native_operator_authority(
    value: object,
    *,
    source_operator_implementation: Mapping[str, Any],
    evolvable_authority: Mapping[str, Any],
    source: Mapping[str, Any],
) -> dict[str, Any]:
    authority = _self_hashed_object(
        _mapping(value, name="v5 native operator authority"),
        identity_field="nativeOperatorAuthoritySha256",
        name="v5 native operator authority",
    )
    required = {
        "schemaVersion",
        "sourceOperatorImplementationSha256",
        "factoryAuthoritySha256",
        "compilerPolicySha256",
        "programKind",
        "codec",
        "operatorRegistry",
        "budget",
        "grammarRegistry",
        "holdOperatorPolicy",
        "initialProtectionOperatorPolicy",
        "immigrantConstructionPolicy",
        "temporalOperatorSpecification",
        "temporalDomains",
        "nativeOperatorAuthoritySha256",
    }
    _exact_keys(authority, required, name="v5 native operator authority")
    temporal_operator_specification = _temporal_operator_specification(
        authority.get("temporalOperatorSpecification"),
        compiler_policy_sha256=evolvable_authority["compilerPolicySha256"],
        name="v5 native temporal operator specification",
    )
    temporal_domains = _temporal_domains(authority.get("temporalDomains"))
    if temporal_domains != _build_v5_temporal_domains(
        temporal_operator_specification,
        name="v5 native temporal operator specification",
    ):
        raise TemporalQDV5NativeError(
            "v5 native temporal operator specification/domains drifted"
        )
    if (
        authority.get("schemaVersion") != V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA
        or authority.get("sourceOperatorImplementationSha256")
        != sha256(canonical_json_bytes(source_operator_implementation))
        or authority.get("factoryAuthoritySha256") != evolvable_authority["authoritySha256"]
        or authority.get("compilerPolicySha256")
        != evolvable_authority["compilerPolicySha256"]
        or authority.get("programKind") != evolvable_authority["programKind"]
        or authority.get("codec") != evolvable_authority["codec"]
        or authority.get("operatorRegistry") != evolvable_authority["operatorRegistry"]
        or authority.get("budget") != evolvable_authority["budget"]
        or authority.get("grammarRegistry") != source["grammarRegistry"]
        or authority.get("holdOperatorPolicy") != source["holdOperatorPolicy"]
        or authority.get("initialProtectionOperatorPolicy")
        != source["initialProtectionOperatorPolicy"]
        or authority.get("immigrantConstructionPolicy")
        != source["immigrantConstructionPolicy"]
    ):
        raise TemporalQDV5NativeError("v5 native operator authority binding drifted")
    return authority


def _side_snapshots(
    source_module: object,
    *,
    side: str,
    evolvable_authority: Mapping[str, Any],
    native_authority: Mapping[str, Any],
) -> dict[str, Any]:
    module = _mapping(source_module, name=f"v5 {side} source module")
    _exact_keys(
        module,
        {"catalog", "catalogSha256", "context", "indicatorPolicy", "policy", "seedNames"},
        name=f"v5 {side} source module",
    )
    context = _mapping(module["context"], name=f"v5 {side} frozen context")
    catalog = _mapping(module["catalog"], name=f"v5 {side} frozen catalog")
    catalog_sha256 = _sha(module["catalogSha256"], name=f"v5 {side} catalogSha256")
    _, indicator_policy, _, semantic_catalog_sha256 = _indicator_policy_timeframe_policy(
        catalog,
        module["indicatorPolicy"],
        name=f"v5 {side} frozen catalog",
    )
    if catalog_sha256 != semantic_catalog_sha256:
        raise TemporalQDV5NativeError(f"v5 {side} catalog/policy binding drifted")
    if not isinstance(module["seedNames"], list) or not all(
        isinstance(seed_name, str) and seed_name for seed_name in module["seedNames"]
    ):
        raise TemporalQDV5NativeError(f"v5 {side} seed names are invalid")
    # The public policy snapshot must remain byte-identical to historical
    # FrozenModule identities.  The execution-only static closure therefore
    # lives in adjacent sealed fields rather than being injected into it.
    resource_operator_spec_sha256 = _resource_operator_spec_sha256(
        catalog,
        indicator_policy=indicator_policy,
        name=f"v5 {side} frozen catalog",
    )
    policy_payload = {
        "authoritySha256": evolvable_authority["authoritySha256"],
        "side": side,
        "budget": evolvable_authority["budget"],
        "compilerPolicySha256": evolvable_authority["compilerPolicySha256"],
        "resourceOperatorSpecSha256": resource_operator_spec_sha256,
    }
    return {
        "grammarContext": _snapshot(
            schema_version="evolvable_module_context_v1",
            kind="grammarContext",
            payload={
                "authoritySha256": evolvable_authority["authoritySha256"],
                "side": side,
                "context": context,
            },
        ),
        "catalog": _snapshot(
            schema_version="evolvable_module_catalog_v1",
            kind="catalog",
            payload={"catalog": catalog, "catalogSha256": catalog_sha256, "side": side},
        ),
        "policy": _snapshot(
            schema_version="evolvable_module_policy_v1",
            kind="policy",
            payload=policy_payload,
        ),
        "nativeAuthority": _identity_snapshot(
            native_authority,
            name="v5 native authority snapshot",
            expected_kind="nativeAuthority",
        ),
        "budget": _mapping(evolvable_authority["budget"], name="v5 side budget"),
        "modulePolicy": _mapping(module["policy"], name=f"v5 {side} module policy"),
        "indicatorPolicy": indicator_policy,
        "seedNames": list(module["seedNames"]),
        "resourceOperatorSpecSha256": resource_operator_spec_sha256,
    }


def build_v5_frozen_authority(
    *,
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    bidirectional_pair_policy: Mapping[str, Any],
    native_operator_authority: Mapping[str, Any],
    qd_engine_version: str,
) -> dict[str, Any]:
    """Seal the complete static v5 authority needed after source removal.

    It intentionally embeds static content once.  Native candidate work never
    reopens the old pair/operator config or a validator process.  Historical
    source-authority snapshots remain evidence only; the Rust core must not
    interpret their command/interpreter metadata as an executable authority.
    """

    source = _pair_source_authority(pair_source_authority)
    pair_run_config_sha256 = _sha(
        source["pairRunConfigSha256"], name="v5 pair source authority identity"
    )
    evolvable = _evolvable_authority(
        evolvable_module_authority, pair_run_config_sha256=pair_run_config_sha256
    )
    source_operator = _source_operator_implementation(source["operatorImplementation"])
    pair_policy = _mapping(bidirectional_pair_policy, name="v5 bidirectional pair policy")
    _exact_keys(
        pair_policy,
        {"schemaVersion", "enabled", "compilerAuthority"},
        name="v5 bidirectional pair policy",
    )
    if (
        pair_policy.get("schemaVersion") != "temporal_qd_bidirectional_pair_policy_v1"
        or pair_policy.get("enabled") is not True
    ):
        raise TemporalQDV5NativeError("v5 bidirectional pair policy is incompatible")
    pair_compiler = _identity_snapshot(
        source["pairCompilerAuthority"],
        name="v5 pair compiler authority snapshot",
        expected_kind="pairCompiler",
    )
    if _identity_snapshot(
        pair_policy["compilerAuthority"],
        name="v5 pair policy compiler authority snapshot",
        expected_kind="pairCompiler",
    ) != pair_compiler:
        raise TemporalQDV5NativeError("v5 pair policy/compiler authority drifted")
    if not isinstance(qd_engine_version, str) or not qd_engine_version.strip():
        raise TemporalQDV5NativeError("v5 QD engine version is invalid")
    native_operator = _native_operator_authority(
        native_operator_authority,
        source_operator_implementation=source_operator,
        evolvable_authority=evolvable,
        source=source,
    )
    source_temporal_operator_specification = _source_temporal_operator_specification(
        compiler_policy_sha256=evolvable["compilerPolicySha256"]
    )
    if (
        native_operator["temporalOperatorSpecification"]
        != source_temporal_operator_specification
    ):
        raise TemporalQDV5NativeError(
            "v5 native temporal operator specification drifted from its source"
        )
    authority = {
        "schemaVersion": V5_FROZEN_AUTHORITY_SCHEMA,
        "qdEngineVersion": qd_engine_version,
        "pairRunConfigSha256": pair_run_config_sha256,
        "evolvableModuleAuthority": evolvable,
        "factoryAuthoritySha256": evolvable["authoritySha256"],
        "bidirectionalPairPolicy": pair_policy,
        "pairPolicySha256": sha256(canonical_json_bytes(pair_policy)),
        "compilerPolicySha256": evolvable["compilerPolicySha256"],
        "sourceOperatorImplementation": source_operator,
        "sourceOperatorImplementationSha256": sha256(canonical_json_bytes(source_operator)),
        "nativeOperatorAuthority": native_operator,
        "nativeOperatorAuthoritySha256": native_operator["nativeOperatorAuthoritySha256"],
        "temporalOperatorSpecSha256": native_operator[
            "temporalOperatorSpecification"
        ]["operatorSpecSha256"],
        "temporalDomainsSha256": native_operator["temporalDomains"][
            "temporalDomainsSha256"
        ],
        "grammarRegistry": source["grammarRegistry"],
        "holdOperatorPolicy": source["holdOperatorPolicy"],
        "initialProtectionOperatorPolicy": source["initialProtectionOperatorPolicy"],
        "immigrantConstructionPolicy": source["immigrantConstructionPolicy"],
        "long": _side_snapshots(
            source["longModule"],
            side="long",
            evolvable_authority=evolvable,
            native_authority=source["nativeAuthority"],
        ),
        "short": _side_snapshots(
            source["shortModule"],
            side="short",
            evolvable_authority=evolvable,
            native_authority=source["nativeAuthority"],
        ),
        "pairCompilerAuthority": pair_compiler,
    }
    shared = {
        "schemaVersion": V5_SHARED_AUTHORITY_SCHEMA,
        "authority": authority,
        "authoritySha256": sha256(canonical_json_bytes(authority)),
    }
    return validate_v5_frozen_authority(shared)


def validate_v5_frozen_authority(value: object) -> dict[str, Any]:
    shared = _mapping(value, name="v5 frozen authority")
    _exact_keys(
        shared,
        {"schemaVersion", "authority", "authoritySha256"},
        name="v5 frozen authority",
    )
    if shared.get("schemaVersion") != V5_SHARED_AUTHORITY_SCHEMA:
        raise TemporalQDV5NativeError("v5 frozen authority schema is incompatible")
    authority = _mapping(shared["authority"], name="v5 frozen authority payload")
    expected = {
        "schemaVersion",
        "qdEngineVersion",
        "pairRunConfigSha256",
        "evolvableModuleAuthority",
        "factoryAuthoritySha256",
        "bidirectionalPairPolicy",
        "pairPolicySha256",
        "compilerPolicySha256",
        "sourceOperatorImplementation",
        "sourceOperatorImplementationSha256",
        "nativeOperatorAuthority",
        "nativeOperatorAuthoritySha256",
        "temporalOperatorSpecSha256",
        "temporalDomainsSha256",
        "grammarRegistry",
        "holdOperatorPolicy",
        "initialProtectionOperatorPolicy",
        "immigrantConstructionPolicy",
        "long",
        "short",
        "pairCompilerAuthority",
    }
    _exact_keys(authority, expected, name="v5 frozen authority payload")
    if authority.get("schemaVersion") != V5_FROZEN_AUTHORITY_SCHEMA:
        raise TemporalQDV5NativeError("v5 frozen authority payload schema is incompatible")
    if not isinstance(authority.get("qdEngineVersion"), str) or not authority["qdEngineVersion"].strip():
        raise TemporalQDV5NativeError("v5 frozen authority QD engine version is invalid")
    pair_run_config_sha256 = _sha(
        authority.get("pairRunConfigSha256"), name="v5 frozen authority pair run config"
    )
    evolvable = _evolvable_authority(
        authority["evolvableModuleAuthority"],
        pair_run_config_sha256=pair_run_config_sha256,
    )
    if (
        authority.get("factoryAuthoritySha256") != evolvable["authoritySha256"]
        or authority.get("compilerPolicySha256") != evolvable["compilerPolicySha256"]
    ):
        raise TemporalQDV5NativeError("v5 frozen authority factory/compiler binding drifted")
    pair_policy = _mapping(
        authority["bidirectionalPairPolicy"], name="v5 frozen pair policy"
    )
    _exact_keys(
        pair_policy,
        {"schemaVersion", "enabled", "compilerAuthority"},
        name="v5 frozen pair policy",
    )
    if (
        pair_policy.get("schemaVersion") != "temporal_qd_bidirectional_pair_policy_v1"
        or pair_policy.get("enabled") is not True
    ):
        raise TemporalQDV5NativeError("v5 frozen pair policy is incompatible")
    if sha256(canonical_json_bytes(pair_policy)) != _sha(
        authority.get("pairPolicySha256"), name="v5 frozen pair policy identity"
    ):
        raise TemporalQDV5NativeError("v5 frozen pair policy identity mismatch")
    source_operator = _source_operator_implementation(
        authority["sourceOperatorImplementation"]
    )
    source_operator_sha256 = _sha(
        authority.get("sourceOperatorImplementationSha256"),
        name="v5 frozen source operator implementation identity",
    )
    if sha256(canonical_json_bytes(source_operator)) != source_operator_sha256:
        raise TemporalQDV5NativeError(
            "v5 frozen source operator implementation identity mismatch"
        )
    native_static_source = {
        "grammarRegistry": authority["grammarRegistry"],
        "holdOperatorPolicy": authority["holdOperatorPolicy"],
        "initialProtectionOperatorPolicy": authority[
            "initialProtectionOperatorPolicy"
        ],
        "immigrantConstructionPolicy": authority["immigrantConstructionPolicy"],
    }
    native_operator = _native_operator_authority(
        authority["nativeOperatorAuthority"],
        source_operator_implementation=source_operator,
        evolvable_authority=evolvable,
        source=native_static_source,
    )
    if authority.get("nativeOperatorAuthoritySha256") != native_operator[
        "nativeOperatorAuthoritySha256"
    ]:
        raise TemporalQDV5NativeError("v5 frozen native operator authority drifted")
    if authority.get("temporalOperatorSpecSha256") != native_operator[
        "temporalOperatorSpecification"
    ]["operatorSpecSha256"]:
        raise TemporalQDV5NativeError("v5 frozen temporal operator specification drifted")
    if authority.get("temporalDomainsSha256") != native_operator["temporalDomains"][
        "temporalDomainsSha256"
    ]:
        raise TemporalQDV5NativeError("v5 frozen temporal domains authority drifted")
    pair_compiler = _identity_snapshot(
        authority["pairCompilerAuthority"],
        name="v5 frozen pair compiler authority",
        expected_kind="pairCompiler",
    )
    compiler_from_policy = _identity_snapshot(
        pair_policy.get("compilerAuthority"),
        name="v5 frozen pair policy compiler authority",
        expected_kind="pairCompiler",
    )
    if compiler_from_policy != pair_compiler:
        raise TemporalQDV5NativeError("v5 frozen pair compiler authority drifted")
    for side in ("long", "short"):
        module = _mapping(authority[side], name=f"v5 frozen authority {side}")
        _exact_keys(
            module,
            {
                "grammarContext",
                "catalog",
                "policy",
                "nativeAuthority",
                "budget",
                "modulePolicy",
                "indicatorPolicy",
                "seedNames",
                "resourceOperatorSpecSha256",
            },
            name=f"v5 frozen authority {side}",
        )
        grammar = _identity_snapshot(
            module["grammarContext"],
            name=f"v5 frozen {side} grammar context",
            expected_kind="grammarContext",
        )
        catalog = _identity_snapshot(
            module["catalog"], name=f"v5 frozen {side} catalog", expected_kind="catalog"
        )
        policy = _identity_snapshot(
            module["policy"], name=f"v5 frozen {side} policy", expected_kind="policy"
        )
        native_snapshot = _identity_snapshot(
            module["nativeAuthority"],
            name=f"v5 frozen {side} native authority",
            expected_kind="nativeAuthority",
        )
        catalog_payload = _mapping(
            catalog["payload"], name=f"v5 frozen {side} catalog payload"
        )
        policy_payload = _mapping(
            policy["payload"], name=f"v5 frozen {side} policy payload"
        )
        _exact_keys(
            policy_payload,
            {
                "authoritySha256",
                "side",
                "budget",
                "compilerPolicySha256",
                "resourceOperatorSpecSha256",
            },
            name=f"v5 frozen {side} policy payload",
        )
        catalog_body, indicator_policy, _, semantic_catalog_sha256 = (
            _indicator_policy_timeframe_policy(
                catalog_payload.get("catalog"),
                module.get("indicatorPolicy"),
                name=f"v5 frozen {side} catalog body",
            )
        )
        if (
            grammar["payload"].get("side") != side
            or grammar["payload"].get("authoritySha256") != evolvable["authoritySha256"]
            or catalog["payload"].get("side") != side
            or catalog["payload"].get("catalogSha256") != evolvable["catalogSha256"]
            or policy["schemaVersion"] != "evolvable_module_policy_v1"
            or policy_payload.get("side") != side
            or policy_payload.get("authoritySha256") != evolvable["authoritySha256"]
            or policy_payload.get("budget") != evolvable["budget"]
            or policy_payload.get("compilerPolicySha256")
            != evolvable["compilerPolicySha256"]
            or policy_payload.get("resourceOperatorSpecSha256")
            != _resource_operator_spec_sha256(
                catalog_body,
                indicator_policy=indicator_policy,
                name=f"v5 frozen {side} catalog body",
            )
            or module.get("resourceOperatorSpecSha256")
            != policy_payload.get("resourceOperatorSpecSha256")
            or not isinstance(module.get("modulePolicy"), Mapping)
            or semantic_catalog_sha256 != evolvable["catalogSha256"]
            or not isinstance(module.get("seedNames"), list)
            or not all(
                isinstance(seed_name, str) and seed_name
                for seed_name in module["seedNames"]
            )
            or module["budget"] != evolvable["budget"]
            or native_snapshot["sha256"]
            != authority["long"]["nativeAuthority"]["sha256"]
        ):
            raise TemporalQDV5NativeError(f"v5 frozen {side} authority binding drifted")
    supplied = _sha(shared["authoritySha256"], name="v5 frozen authority authoritySha256")
    if sha256(canonical_json_bytes(authority)) != supplied:
        raise TemporalQDV5NativeError("v5 frozen authority identity mismatch")
    return {
        "schemaVersion": V5_SHARED_AUTHORITY_SCHEMA,
        "authority": authority,
        "authoritySha256": supplied,
    }


def build_v5_proposal_execution_authority(
    *,
    native_batch_authority: Mapping[str, Any],
    frozen_authority: Mapping[str, Any],
    generation_config_sha256: str,
) -> dict[str, Any]:
    batch = native.validate_native_authority(native_batch_authority)
    frozen = validate_v5_frozen_authority(frozen_authority)
    config_sha = _sha(generation_config_sha256, name="v5 generation configSha256")
    result: dict[str, Any] = {
        "schemaVersion": V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA,
        "nativeBatchAuthority": batch,
        "nativeBatchAuthoritySha256": batch["authoritySha256"],
        "expectedAuthoritySha256": frozen["authoritySha256"],
        "frozenAuthoritySha256": frozen["authoritySha256"],
        "generationConfigSha256": config_sha,
    }
    result["authoritySha256"] = sha256(canonical_json_bytes(result))
    return validate_v5_proposal_execution_authority(
        result,
        expected_authority_sha256=frozen["authoritySha256"],
        generation_config_sha256=config_sha,
        frozen_authority_sha256=frozen["authoritySha256"],
    )


def validate_v5_proposal_execution_authority(
    value: object,
    *,
    expected_authority_sha256: str,
    generation_config_sha256: str,
    frozen_authority_sha256: str,
) -> dict[str, Any]:
    execution = _mapping(value, name="v5 proposal execution authority")
    _exact_keys(
        execution,
        {
            "schemaVersion",
            "nativeBatchAuthority",
            "nativeBatchAuthoritySha256",
            "expectedAuthoritySha256",
            "frozenAuthoritySha256",
            "generationConfigSha256",
            "authoritySha256",
        },
        name="v5 proposal execution authority",
    )
    if execution.get("schemaVersion") != V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA:
        raise TemporalQDV5NativeError("v5 proposal execution authority schema is incompatible")
    batch = native.validate_native_authority(execution["nativeBatchAuthority"])
    if (
        execution.get("nativeBatchAuthoritySha256") != batch["authoritySha256"]
        or execution.get("expectedAuthoritySha256")
        != _sha(expected_authority_sha256, name="v5 expected authoritySha256")
        or execution.get("frozenAuthoritySha256")
        != _sha(frozen_authority_sha256, name="v5 frozen authoritySha256")
        or execution.get("generationConfigSha256")
        != _sha(generation_config_sha256, name="v5 generation configSha256")
    ):
        raise TemporalQDV5NativeError("v5 proposal execution authority binding drifted")
    return _self_hashed_object(
        execution,
        identity_field="authoritySha256",
        name="v5 proposal execution authority",
    )


def _validate_v5_full_generation_config(
    *,
    generation_config: Mapping[str, Any],
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    bidirectional_pair_policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the existing sealed v2 generation-config closure for v5.

    This is a pure control-plane check.  It neither produces a candidate nor
    changes/re-hashes the caller's config: the native transaction receives
    the exact v2 config that the supervisor sealed with
    ``build_pair_generation_config``.  In particular, the v5 archive,
    behavior, and capacity bindings belong to its ``runConfig`` rather than a
    bridge-authored parallel publication schema.
    """

    config = _mapping(generation_config, name="v5 generation config")
    for field in (
        "runConfig",
        "pairPolicy",
        "operatorImplementation",
        "reproductionAllocation",
    ):
        if field not in config:
            raise TemporalQDV5NativeError(
                f"v5 generation config lacks sealed {field}"
            )

    run_config = _mapping(config["runConfig"], name="v5 generation runConfig")
    allocation = _mapping(
        config["reproductionAllocation"], name="v5 reproduction allocation"
    )
    allocation_schema = allocation.get("schemaVersion")
    if allocation_schema == "temporal_qd_reproduction_allocation_v2":
        target_field = "targetAcceptedCandidates"
        offspring_field = "desiredAcceptedOffspringCount"
        immigrant_field = "desiredAcceptedImmigrantCount"
    elif allocation_schema == "temporal_qd_reproduction_allocation_v1":
        # The existing full v2 pair-generation config remains valid for
        # historical directional authorities whose frozen archive policy still
        # uses the evaluated terminology.  The kernel's shared allocation
        # reducer authenticates both versions; Python must transport exactly
        # what the supervisor sealed, not silently rename/re-hash it.
        target_field = "targetEvaluatedCandidates"
        offspring_field = "desiredEvaluatedOffspringCount"
        immigrant_field = "desiredEvaluatedImmigrantCount"
    else:
        raise TemporalQDV5NativeError(
            "v5 reproduction allocation schema is incompatible"
        )
    allocation_sha256 = _sha(
        allocation.get("allocationSha256"), name="v5 reproduction allocation identity"
    )
    if (
        sha256(
            canonical_json_bytes(
                {
                    key: item
                    for key, item in allocation.items()
                    if key != "allocationSha256"
                }
            )
        )
        != allocation_sha256
    ):
        raise TemporalQDV5NativeError("v5 reproduction allocation identity mismatch")
    target = _positive(
        allocation.get(target_field),
        name=f"v5 reproduction allocation {target_field}",
    )
    offspring = _nonnegative(
        allocation.get(offspring_field),
        name=f"v5 reproduction allocation {offspring_field}",
    )
    immigrants = _nonnegative(
        allocation.get(immigrant_field),
        name=f"v5 reproduction allocation {immigrant_field}",
    )
    if target != config.get("targetUniqueCandidates") or offspring + immigrants != target:
        raise TemporalQDV5NativeError("v5 reproduction allocation dimensions drifted")

    expected_bindings = build_v5_generation_bindings(
        generation_run_config=run_config,
        pair_source_authority=pair_source_authority,
        evolvable_module_authority=evolvable_module_authority,
    )
    if (
        config["runConfig"] != expected_bindings["runConfig"]
        or config["operatorImplementation"]
        != expected_bindings["operatorImplementation"]
        or config["pairPolicy"]
        != build_v5_bidirectional_pair_policy(
            pair_source_authority=pair_source_authority
        )
        or config["pairPolicy"] != dict(bidirectional_pair_policy)
    ):
        raise TemporalQDV5NativeError("v5 generation config authority binding drifted")
    return config


def build_v5_proposal_manifest(
    *,
    output_root: Path | str,
    generation_config: Mapping[str, Any],
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    bidirectional_pair_policy: Mapping[str, Any],
    native_operator_authority: Mapping[str, Any],
    qd_engine_version: str,
    native_batch_authority: Mapping[str, Any],
    evaluation_population_size: int,
    thread_cap: int = V5_PROPOSAL_THREAD_CAP_MAXIMUM,
    generation_kind: str = V5_PROPOSAL_GENERATION_G0,
    parent_archive_input: Mapping[str, Any] | None = None,
    identity_ledger_input: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Write the exact data contract for one Rust-only construction phase."""

    root = native._ensure_real_directory_tree(
        output_root, name="native v5 proposal output root"
    )
    config = _mapping(generation_config, name="v5 generation config")
    config_sha = _sha(config.get("configSha256"), name="v5 generation configSha256")
    if sha256(canonical_json_bytes({key: item for key, item in config.items() if key != "configSha256"})) != config_sha:
        raise TemporalQDV5NativeError("v5 generation config identity mismatch")
    if config.get("schemaVersion") != native.PAIR_GENERATION_SCHEMA:
        raise TemporalQDV5NativeError("v5 generation config schema is incompatible")
    generation_index = _positive(config.get("generationIndex"), name="v5 generationIndex")
    requested_count = _positive(
        config.get("targetUniqueCandidates"), name="v5 targetUniqueCandidates"
    )
    max_proposal_attempts = _positive(
        config.get("maxProposalAttempts"), name="v5 maxProposalAttempts"
    )
    evaluation_population_size = _positive(
        evaluation_population_size, name="v5 evaluationPopulationSize"
    )
    thread_cap = _positive(thread_cap, name="v5 threadCap")
    if thread_cap > V5_PROPOSAL_THREAD_CAP_MAXIMUM:
        raise TemporalQDV5NativeError(
            "v5 threadCap exceeds the bounded native construction maximum"
        )
    if evaluation_population_size > requested_count or max_proposal_attempts < requested_count:
        raise TemporalQDV5NativeError("v5 construction dimensions are inconsistent")
    if generation_kind not in {
        V5_PROPOSAL_GENERATION_G0,
        V5_PROPOSAL_GENERATION_EVOLVED,
    }:
        raise TemporalQDV5NativeError("v5 generation kind is incompatible")
    if generation_kind == V5_PROPOSAL_GENERATION_G0 and generation_index != 1:
        raise TemporalQDV5NativeError("v5 G0 construction must bind generation 1")
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if parent_archive_input is not None or identity_ledger_input is not None:
            raise TemporalQDV5NativeError("v5 G0 inputs must remain absent")
        parent_archive = None
        identity_ledger = None
    else:
        if parent_archive_input is None or identity_ledger_input is None:
            raise TemporalQDV5NativeError(
                "v5 evolved construction requires exact parent/archive ledger bindings"
            )
        parent_archive = validate_v5_proposal_input_binding(
            parent_archive_input, expected_kind="parentArchive"
        )
        identity_ledger = validate_v5_proposal_input_binding(
            identity_ledger_input, expected_kind="identityLedger"
        )
    config = _validate_v5_full_generation_config(
        generation_config=config,
        pair_source_authority=pair_source_authority,
        evolvable_module_authority=evolvable_module_authority,
        bidirectional_pair_policy=bidirectional_pair_policy,
    )
    frozen = build_v5_frozen_authority(
        pair_source_authority=pair_source_authority,
        evolvable_module_authority=evolvable_module_authority,
        bidirectional_pair_policy=bidirectional_pair_policy,
        native_operator_authority=native_operator_authority,
        qd_engine_version=qd_engine_version,
    )
    execution = build_v5_proposal_execution_authority(
        native_batch_authority=native_batch_authority,
        frozen_authority=frozen,
        generation_config_sha256=config_sha,
    )
    value: dict[str, Any] = {
        "schemaVersion": V5_PROPOSAL_MANIFEST_SCHEMA,
        "contractVersion": native.NATIVE_CONTRACT_VERSION,
        "operation": V5_PROPOSAL_OPERATION,
        "authoritySha256": execution["authoritySha256"],
        "executionAuthority": execution,
        "frozenAuthority": frozen,
        "expectedAuthoritySha256": frozen["authoritySha256"],
        "outputRoot": str(root),
        # Native v5 journals are one canonical UTF-8/LF byte stream on every
        # platform.  CRLF would make a restart's authenticated artifact bytes
        # host-dependent for no semantic benefit.
        "finalNewline": "lf",
        "generationConfig": config,
        "generationConfigSha256": config_sha,
        "generationIndex": generation_index,
        "generationKind": generation_kind,
        "requestedCount": requested_count,
        "evaluationPopulationSize": evaluation_population_size,
        "maxProposalAttempts": max_proposal_attempts,
        "threadCap": thread_cap,
        "inputs": {
            "schemaVersion": V5_PROPOSAL_INPUTS_SCHEMA,
            "parentArchive": parent_archive,
            "identityLedger": identity_ledger,
        },
        "resultPath": V5_PROPOSAL_RESULT_FILENAME,
    }
    value["manifestSha256"] = sha256(canonical_json_bytes(value))
    return validate_v5_proposal_manifest(value)


def validate_v5_proposal_manifest(value: object) -> dict[str, Any]:
    manifest = _mapping(value, name="v5 proposal manifest")
    _exact_keys(
        manifest,
        {
            "schemaVersion",
            "contractVersion",
            "operation",
            "authoritySha256",
            "executionAuthority",
            "frozenAuthority",
            "expectedAuthoritySha256",
            "outputRoot",
            "finalNewline",
            "generationConfig",
            "generationConfigSha256",
            "generationIndex",
            "generationKind",
            "requestedCount",
            "evaluationPopulationSize",
            "maxProposalAttempts",
            "threadCap",
            "inputs",
            "resultPath",
            "manifestSha256",
        },
        name="v5 proposal manifest",
    )
    if (
        manifest.get("schemaVersion") != V5_PROPOSAL_MANIFEST_SCHEMA
        or manifest.get("contractVersion") != native.NATIVE_CONTRACT_VERSION
        or manifest.get("operation") != V5_PROPOSAL_OPERATION
        or manifest.get("resultPath") != V5_PROPOSAL_RESULT_FILENAME
    ):
        raise TemporalQDV5NativeError("v5 proposal manifest is incompatible")
    if manifest.get("generationKind") not in {
        V5_PROPOSAL_GENERATION_G0,
        V5_PROPOSAL_GENERATION_EVOLVED,
    }:
        raise TemporalQDV5NativeError("v5 proposal generation kind is incompatible")
    _v5_proposal_output_root(
        manifest.get("outputRoot"), name="v5 proposal output root"
    )
    generation_index = _positive(manifest.get("generationIndex"), name="v5 generationIndex")
    requested_count = _positive(manifest.get("requestedCount"), name="v5 requestedCount")
    evaluation_population_size = _positive(
        manifest.get("evaluationPopulationSize"), name="v5 evaluationPopulationSize"
    )
    max_proposal_attempts = _positive(
        manifest.get("maxProposalAttempts"), name="v5 maxProposalAttempts"
    )
    thread_cap = _positive(manifest.get("threadCap"), name="v5 threadCap")
    if (
        thread_cap > V5_PROPOSAL_THREAD_CAP_MAXIMUM
        or evaluation_population_size > requested_count
        or max_proposal_attempts < requested_count
        or (
            manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0
            and generation_index != 1
        )
        or (
            manifest["generationKind"] == V5_PROPOSAL_GENERATION_EVOLVED
            and generation_index < 2
        )
    ):
        raise TemporalQDV5NativeError("v5 proposal manifest dimensions are inconsistent")
    config = _mapping(manifest["generationConfig"], name="v5 generation config")
    config_sha = _sha(manifest.get("generationConfigSha256"), name="v5 generation configSha256")
    if (
        config.get("configSha256") != config_sha
        or sha256(canonical_json_bytes({key: item for key, item in config.items() if key != "configSha256"}))
        != config_sha
        or config.get("generationIndex") != generation_index
        or config.get("targetUniqueCandidates") != requested_count
        or config.get("maxProposalAttempts") != max_proposal_attempts
    ):
        raise TemporalQDV5NativeError("v5 proposal manifest generation config binding drifted")
    frozen = validate_v5_frozen_authority(manifest["frozenAuthority"])
    expected_authority_sha256 = _sha(
        manifest.get("expectedAuthoritySha256"), name="v5 expectedAuthoritySha256"
    )
    if expected_authority_sha256 != frozen["authoritySha256"]:
        raise TemporalQDV5NativeError("v5 proposal expected authority binding drifted")
    execution = validate_v5_proposal_execution_authority(
        manifest["executionAuthority"],
        expected_authority_sha256=expected_authority_sha256,
        generation_config_sha256=config_sha,
        frozen_authority_sha256=frozen["authoritySha256"],
    )
    if manifest.get("authoritySha256") != execution["authoritySha256"]:
        raise TemporalQDV5NativeError("v5 proposal execution authority binding drifted")
    inputs = _mapping(manifest["inputs"], name="v5 proposal inputs")
    _exact_keys(
        inputs,
        {"schemaVersion", "parentArchive", "identityLedger"},
        name="v5 proposal inputs",
    )
    if inputs.get("schemaVersion") != V5_PROPOSAL_INPUTS_SCHEMA:
        raise TemporalQDV5NativeError("v5 proposal inputs schema is incompatible")
    if manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0:
        if inputs["parentArchive"] is not None or inputs["identityLedger"] is not None:
            raise TemporalQDV5NativeError("v5 G0 inputs must not carry legacy files")
    else:
        if inputs["parentArchive"] is None or inputs["identityLedger"] is None:
            raise TemporalQDV5NativeError(
                "v5 evolved inputs must bind parent archive and identity ledger"
            )
        validate_v5_proposal_input_binding(
            inputs["parentArchive"], expected_kind="parentArchive"
        )
        validate_v5_proposal_input_binding(
            inputs["identityLedger"], expected_kind="identityLedger"
        )
    if manifest.get("finalNewline") != "lf":
        raise TemporalQDV5NativeError("v5 proposal final newline is invalid")
    return _self_hashed_object(
        manifest, identity_field="manifestSha256", name="v5 proposal manifest"
    )


def _validate_v5_receipt(
    value: object, *, manifest: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    receipt = _mapping(value, name="v5 proposal receipt")
    _exact_keys(
        receipt,
        {
            "schemaVersion",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "attemptJournalSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "g0FunnelFragmentsSha256",
            "g0FunnelProjectionStreamReceiptSha256",
            "evaluationPopulationSize",
            "compactJournalSha256",
            "identityLedgerSha256",
            "selectedProjectionIndexSha256",
            "outputInventory",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "threadCap",
            "constructionSummary",
            "receiptSha256",
        },
        name="v5 proposal receipt",
    )
    if (
        receipt.get("schemaVersion") != V5_PROPOSAL_RECEIPT_SCHEMA
        or receipt.get("authoritySha256") != manifest["authoritySha256"]
        or receipt.get("manifestSha256") != manifest["manifestSha256"]
        or receipt.get("expectedAuthoritySha256") != manifest["expectedAuthoritySha256"]
        or receipt.get("generationConfigSha256") != manifest["generationConfigSha256"]
        or receipt.get("generationIndex") != manifest["generationIndex"]
        or receipt.get("requestedCount") != manifest["requestedCount"]
        or receipt.get("acceptedRecordCount") != manifest["requestedCount"]
        or not isinstance(receipt.get("attemptCount"), int)
        or isinstance(receipt.get("attemptCount"), bool)
        or receipt["attemptCount"] < manifest["requestedCount"]
        or receipt["attemptCount"] > manifest["maxProposalAttempts"]
        or receipt.get("attemptCount") != result.get("attemptCount")
        or receipt.get("evaluationPopulationSize") != manifest["evaluationPopulationSize"]
        or receipt.get("threadCap") != manifest["threadCap"]
        or receipt.get("nativeBatchAuthoritySha256")
        != manifest["executionAuthority"]["nativeBatchAuthoritySha256"]
    ):
        raise TemporalQDV5NativeError("v5 proposal receipt is incompatible with its manifest")
    for key in (
        "attemptJournalSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "compactJournalSha256",
        "identityLedgerSha256",
        "selectedProjectionIndexSha256",
    ):
        if receipt.get(key) != result.get(key):
            raise TemporalQDV5NativeError(f"v5 proposal receipt {key} differs from result")
        _sha(receipt.get(key), name=f"v5 proposal receipt {key}")
    output_inventory = _validate_v5_output_inventory(
        receipt.get("outputInventory"),
        semantic_roots={
            "attemptJournalSha256": receipt["attemptJournalSha256"],
            "publicationRequestSha256": receipt["publicationRequestSha256"],
            "publicationPlanSha256": receipt["publicationPlanSha256"],
            "g0FunnelFragmentsSha256": receipt["g0FunnelFragmentsSha256"],
            "g0FunnelProjectionStreamReceiptSha256": receipt[
                "g0FunnelProjectionStreamReceiptSha256"
            ],
            "compactJournalSha256": receipt["compactJournalSha256"],
            "identityLedgerSha256": receipt["identityLedgerSha256"],
            "selectedProjectionIndexSha256": receipt["selectedProjectionIndexSha256"],
            "expectedAuthoritySha256": receipt["expectedAuthoritySha256"],
        },
        output_root=manifest["outputRoot"],
        generation_kind=manifest["generationKind"],
    )
    if (
        receipt.get("outputInventorySha256")
        != output_inventory["outputInventorySha256"]
        or receipt.get("outputInventorySha256") != result.get("outputInventorySha256")
    ):
        raise TemporalQDV5NativeError("v5 proposal output inventory binding drifted")
    _validate_v5_construction_summary(
        receipt["constructionSummary"],
        attempt_count=receipt["attemptCount"],
        accepted_record_count=receipt["acceptedRecordCount"],
    )
    return _self_hashed_object(
        receipt, identity_field="receiptSha256", name="v5 proposal receipt"
    )


def validate_v5_proposal_result(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    checked_manifest = validate_v5_proposal_manifest(manifest)
    if (
        checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_G0
        or checked_manifest["generationIndex"] != 1
    ):
        raise TemporalQDV5NativeError(
            "v5 proposal result is incompatible with a non-G0 manifest"
        )
    result = _mapping(value, name="v5 proposal result")
    _exact_keys(
        result,
        {
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "attemptJournalSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "g0FunnelFragmentsSha256",
            "g0FunnelProjectionStreamReceiptSha256",
            "evaluationPopulationSize",
            "compactJournalSha256",
            "identityLedgerSha256",
            "selectedProjectionIndexSha256",
            "outputInventorySha256",
            "receipt",
            "receiptSha256",
            "resultSha256",
        },
        name="v5 proposal result",
    )
    if (
        result.get("schemaVersion") != V5_PROPOSAL_RESULT_SCHEMA
        or result.get("contractVersion") != native.NATIVE_CONTRACT_VERSION
        or result.get("operation") != V5_PROPOSAL_OPERATION
        or result.get("status") != "completed"
        or result.get("authoritySha256") != checked_manifest["authoritySha256"]
        or result.get("manifestSha256") != checked_manifest["manifestSha256"]
        or result.get("expectedAuthoritySha256")
        != checked_manifest["expectedAuthoritySha256"]
        or result.get("generationConfigSha256")
        != checked_manifest["generationConfigSha256"]
        or result.get("generationIndex") != checked_manifest["generationIndex"]
        or result.get("requestedCount") != checked_manifest["requestedCount"]
        or result.get("acceptedRecordCount") != checked_manifest["requestedCount"]
        or not isinstance(result.get("attemptCount"), int)
        or isinstance(result.get("attemptCount"), bool)
        or result["attemptCount"] < checked_manifest["requestedCount"]
        or result["attemptCount"] > checked_manifest["maxProposalAttempts"]
        or result.get("evaluationPopulationSize")
        != checked_manifest["evaluationPopulationSize"]
    ):
        raise TemporalQDV5NativeError("v5 proposal result is incompatible with its manifest")
    for key in (
        "authoritySha256",
        "manifestSha256",
        "expectedAuthoritySha256",
        "generationConfigSha256",
        "attemptJournalSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "compactJournalSha256",
        "identityLedgerSha256",
        "selectedProjectionIndexSha256",
        "outputInventorySha256",
        "receiptSha256",
    ):
        _sha(result.get(key), name=f"v5 proposal result {key}")
    receipt = _validate_v5_receipt(
        result["receipt"], manifest=checked_manifest, result=result
    )
    if receipt["receiptSha256"] != result["receiptSha256"]:
        raise TemporalQDV5NativeError("v5 proposal receipt/result identity mismatch")
    return _self_hashed_object(
        result, identity_field="resultSha256", name="v5 proposal result"
    )


def _validate_v5_evolved_construction_summary(
    value: object,
    *,
    attempt_count: int,
    accepted_record_count: int,
) -> dict[str, Any]:
    summary = _mapping(value, name="v5 evolved proposal construction summary")
    _exact_keys(
        summary,
        {"schemaVersion", "bytes", "attempts", "uniqueCounts"},
        name="v5 evolved proposal construction summary",
    )
    if summary.get("schemaVersion") != V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA:
        raise TemporalQDV5NativeError(
            "v5 evolved proposal construction summary schema drifted"
        )
    byte_counts = _mapping(
        summary["bytes"], name="v5 evolved construction-summary bytes"
    )
    _exact_keys(
        byte_counts,
        {"durableObjectBytes", "publicArtifactBytes"},
        name="v5 evolved construction-summary bytes",
    )
    for key, count in byte_counts.items():
        _nonnegative(count, name=f"v5 evolved construction-summary bytes {key}")
    attempts = _mapping(
        summary["attempts"], name="v5 evolved construction-summary attempts"
    )
    _exact_keys(
        attempts,
        {"byDisposition", "byReason"},
        name="v5 evolved construction-summary attempts",
    )
    _, disposition_total = _count_mapping(
        attempts["byDisposition"], name="v5 evolved construction-summary dispositions"
    )
    _, reason_total = _count_mapping(
        attempts["byReason"], name="v5 evolved construction-summary reasons"
    )
    if disposition_total != attempt_count or reason_total != attempt_count:
        raise TemporalQDV5NativeError(
            "v5 evolved proposal construction summary attempt counts drifted"
        )
    unique = _mapping(
        summary["uniqueCounts"], name="v5 evolved construction-summary unique counts"
    )
    _exact_keys(
        unique,
        {
            "candidateIdentityCount",
            "executableSemanticCount",
            "pairIdentityCount",
        },
        name="v5 evolved construction-summary unique counts",
    )
    for key, count in unique.items():
        if (
            _nonnegative(count, name=f"v5 evolved construction-summary unique count {key}")
            != accepted_record_count
        ):
            raise TemporalQDV5NativeError(
                "v5 evolved proposal construction summary unique count drifts from accepted records"
            )
    return summary


def _validate_v5_evolved_receipt(
    value: object, *, manifest: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    receipt = _mapping(value, name="v5 evolved proposal receipt")
    _exact_keys(
        receipt,
        {
            "schemaVersion",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "transactionSha256",
            "parentArchiveInputBindingSha256",
            "identityLedgerInputBindingSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "publicationReceiptSha256",
            "publicationFragmentsSha256",
            "evaluationPopulationSize",
            "identityLedgerSha256",
            "outputInventory",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "threadCap",
            "constructionSummary",
            "receiptSha256",
        },
        name="v5 evolved proposal receipt",
    )
    inputs = _mapping(manifest["inputs"], name="v5 evolved proposal manifest inputs")
    parent_binding = _mapping(
        inputs["parentArchive"], name="v5 evolved parent archive binding"
    )
    ledger_binding = _mapping(
        inputs["identityLedger"], name="v5 evolved identity ledger binding"
    )
    if (
        receipt.get("schemaVersion") != V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA
        or receipt.get("authoritySha256") != manifest["authoritySha256"]
        or receipt.get("manifestSha256") != manifest["manifestSha256"]
        or receipt.get("expectedAuthoritySha256")
        != manifest["expectedAuthoritySha256"]
        or receipt.get("generationConfigSha256") != manifest["generationConfigSha256"]
        or receipt.get("generationIndex") != manifest["generationIndex"]
        or receipt.get("requestedCount") != manifest["requestedCount"]
        or receipt.get("acceptedRecordCount") != manifest["requestedCount"]
        or not isinstance(receipt.get("attemptCount"), int)
        or isinstance(receipt.get("attemptCount"), bool)
        or receipt["attemptCount"] < manifest["requestedCount"]
        or receipt["attemptCount"] > manifest["maxProposalAttempts"]
        or receipt.get("attemptCount") != result.get("attemptCount")
        or receipt.get("evaluationPopulationSize")
        != manifest["evaluationPopulationSize"]
        or receipt.get("threadCap") != manifest["threadCap"]
        or receipt.get("parentArchiveInputBindingSha256")
        != parent_binding.get("bindingSha256")
        or receipt.get("identityLedgerInputBindingSha256")
        != ledger_binding.get("bindingSha256")
        or receipt.get("nativeBatchAuthoritySha256")
        != manifest["executionAuthority"]["nativeBatchAuthoritySha256"]
    ):
        raise TemporalQDV5NativeError(
            "v5 evolved proposal receipt is incompatible with its manifest"
        )
    for key in (
        "transactionSha256",
        "parentArchiveInputBindingSha256",
        "identityLedgerInputBindingSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "publicationReceiptSha256",
        "publicationFragmentsSha256",
        "identityLedgerSha256",
    ):
        if receipt.get(key) != result.get(key):
            raise TemporalQDV5NativeError(
                f"v5 evolved proposal receipt {key} differs from result"
            )
        _sha(receipt.get(key), name=f"v5 evolved proposal receipt {key}")
    output_inventory = _validate_v5_output_inventory(
        receipt.get("outputInventory"),
        semantic_roots={
            "transactionSha256": receipt["transactionSha256"],
            "publicationRequestSha256": receipt["publicationRequestSha256"],
            "publicationPlanSha256": receipt["publicationPlanSha256"],
            "publicationReceiptSha256": receipt["publicationReceiptSha256"],
            "publicationFragmentsSha256": receipt["publicationFragmentsSha256"],
            "identityLedgerSha256": receipt["identityLedgerSha256"],
            "expectedAuthoritySha256": receipt["expectedAuthoritySha256"],
        },
        output_root=manifest["outputRoot"],
        generation_kind=V5_PROPOSAL_GENERATION_EVOLVED,
    )
    if (
        receipt.get("outputInventorySha256")
        != output_inventory["outputInventorySha256"]
        or receipt.get("outputInventorySha256") != result.get("outputInventorySha256")
    ):
        raise TemporalQDV5NativeError(
            "v5 evolved proposal output inventory binding drifted"
        )
    _validate_v5_evolved_construction_summary(
        receipt["constructionSummary"],
        attempt_count=receipt["attemptCount"],
        accepted_record_count=receipt["acceptedRecordCount"],
    )
    return _self_hashed_object(
        receipt, identity_field="receiptSha256", name="v5 evolved proposal receipt"
    )


def validate_v5_evolved_proposal_result(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    checked_manifest = validate_v5_proposal_manifest(manifest)
    if (
        checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_EVOLVED
        or checked_manifest["generationIndex"] < 2
    ):
        raise TemporalQDV5NativeError(
            "v5 evolved result is incompatible with a non-evolved manifest"
        )
    result = _mapping(value, name="v5 evolved proposal result")
    _exact_keys(
        result,
        {
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "transactionSha256",
            "parentArchiveInputBindingSha256",
            "identityLedgerInputBindingSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "publicationReceiptSha256",
            "publicationFragmentsSha256",
            "evaluationPopulationSize",
            "identityLedgerSha256",
            "outputInventorySha256",
            "receipt",
            "receiptSha256",
            "resultSha256",
        },
        name="v5 evolved proposal result",
    )
    inputs = _mapping(
        checked_manifest["inputs"], name="v5 evolved proposal manifest inputs"
    )
    parent_binding = _mapping(
        inputs["parentArchive"], name="v5 evolved parent archive binding"
    )
    ledger_binding = _mapping(
        inputs["identityLedger"], name="v5 evolved identity ledger binding"
    )
    if (
        result.get("schemaVersion") != V5_EVOLVED_PROPOSAL_RESULT_SCHEMA
        or result.get("contractVersion") != native.NATIVE_CONTRACT_VERSION
        or result.get("operation") != V5_PROPOSAL_OPERATION
        or result.get("status") != "completed"
        or result.get("authoritySha256") != checked_manifest["authoritySha256"]
        or result.get("manifestSha256") != checked_manifest["manifestSha256"]
        or result.get("expectedAuthoritySha256")
        != checked_manifest["expectedAuthoritySha256"]
        or result.get("generationConfigSha256")
        != checked_manifest["generationConfigSha256"]
        or result.get("generationIndex") != checked_manifest["generationIndex"]
        or result.get("requestedCount") != checked_manifest["requestedCount"]
        or result.get("acceptedRecordCount") != checked_manifest["requestedCount"]
        or not isinstance(result.get("attemptCount"), int)
        or isinstance(result.get("attemptCount"), bool)
        or result["attemptCount"] < checked_manifest["requestedCount"]
        or result["attemptCount"] > checked_manifest["maxProposalAttempts"]
        or result.get("evaluationPopulationSize")
        != checked_manifest["evaluationPopulationSize"]
        or result.get("parentArchiveInputBindingSha256")
        != parent_binding.get("bindingSha256")
        or result.get("identityLedgerInputBindingSha256")
        != ledger_binding.get("bindingSha256")
    ):
        raise TemporalQDV5NativeError(
            "v5 evolved proposal result is incompatible with its manifest"
        )
    for key in (
        "authoritySha256",
        "manifestSha256",
        "expectedAuthoritySha256",
        "generationConfigSha256",
        "transactionSha256",
        "parentArchiveInputBindingSha256",
        "identityLedgerInputBindingSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "publicationReceiptSha256",
        "publicationFragmentsSha256",
        "identityLedgerSha256",
        "outputInventorySha256",
        "receiptSha256",
    ):
        _sha(result.get(key), name=f"v5 evolved proposal result {key}")
    receipt = _validate_v5_evolved_receipt(
        result["receipt"], manifest=checked_manifest, result=result
    )
    if receipt["receiptSha256"] != result["receiptSha256"]:
        raise TemporalQDV5NativeError(
            "v5 evolved proposal receipt/result identity mismatch"
        )
    return _self_hashed_object(
        result, identity_field="resultSha256", name="v5 evolved proposal result"
    )


def _validate_v5_result_for_manifest(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0:
        return validate_v5_proposal_result(value, manifest=checked_manifest)
    if checked_manifest["generationKind"] == V5_PROPOSAL_GENERATION_EVOLVED:
        return validate_v5_evolved_proposal_result(value, manifest=checked_manifest)
    raise TemporalQDV5NativeError("v5 proposal result has an incompatible generation kind")


def _v5_inventory_artifact_adapter(
    *,
    inventory: Mapping[str, Any],
    output_root: Path,
    kind: str,
) -> dict[str, Any]:
    """Project one already-validated immutable output inventory entry."""

    artifacts = inventory.get("artifacts")
    if not isinstance(artifacts, list):
        raise TemporalQDV5NativeError("v5 proposal output inventory artifacts are invalid")
    entry = next(
        (
            item
            for item in artifacts
            if isinstance(item, Mapping) and item.get("kind") == kind
        ),
        None,
    )
    if entry is None:
        raise TemporalQDV5NativeError(f"v5 proposal output inventory lacks {kind}")
    artifact = _mapping(entry, name=f"v5 proposal {kind} artifact")
    _exact_keys(
        artifact,
        {"kind", "relativePath", "fileSha256", "byteLength", "semanticSha256"},
        name=f"v5 proposal {kind} artifact",
    )
    relative_path = _safe_relative_output_path(
        artifact["relativePath"], name=f"v5 proposal {kind} path"
    )
    return {
        "relativePath": relative_path,
        "absolutePath": str(output_root / relative_path),
        "semanticSha256": _sha(
            artifact["semanticSha256"], name=f"v5 proposal {kind} semantic identity"
        ),
        "fileSha256": _sha(
            artifact["fileSha256"], name=f"v5 proposal {kind} file identity"
        ),
        "byteLength": _nonnegative(
            artifact["byteLength"], name=f"v5 proposal {kind} byte length"
        ),
    }


def _v5_object_store_root_adapter(
    *,
    inventory: Mapping[str, Any],
    output_root: Path,
    role: str,
    expected_semantic_sha256: str,
    name: str,
) -> dict[str, Any]:
    """Project one bounded, role-addressed immutable object descriptor.

    ``roots`` is the only public object-store projection in the v2 inventory.
    The full object closure stays in ``object-inventory.jsonl`` for Rust to
    authenticate and stream; Python must never open or enumerate that file.
    """

    object_store = _validate_v5_object_store_inventory(inventory.get("objectStore"))
    roots = object_store.get("roots")
    if not isinstance(roots, list):
        raise TemporalQDV5NativeError("v5 proposal object-store roots are invalid")
    matches = [
        _mapping(item, name=f"{name} object-store root")
        for item in roots
        if isinstance(item, Mapping) and item.get("role") == role
    ]
    if len(matches) != 1:
        raise TemporalQDV5NativeError(f"{name} does not resolve to one immutable root")
    entry = matches[0]
    _exact_keys(
        entry,
        {"role", "relativePath", "objectSha256", "fileSha256", "byteLength"},
        name=f"{name} object-store root",
    )
    semantic_sha256 = _sha(
        entry.get("objectSha256"), name=f"{name} object semantic identity"
    )
    if semantic_sha256 != expected_semantic_sha256:
        raise TemporalQDV5NativeError(f"{name} semantic root binding drifted")
    object_relative = _safe_relative_output_path(
        entry.get("relativePath"), name=f"{name} object path"
    )
    expected_relative = f"sha256/{semantic_sha256.removeprefix('sha256:')}.json"
    if object_relative != expected_relative:
        raise TemporalQDV5NativeError(f"{name} object path/semantic identity drifted")
    relative_path = _safe_relative_output_path(
        f"v5-native/objects/{object_relative}", name=f"{name} immutable object path"
    )
    return {
        "relativePath": relative_path,
        "absolutePath": str(output_root / relative_path),
        "semanticSha256": semantic_sha256,
        "fileSha256": _sha(
            entry.get("fileSha256"), name=f"{name} object file identity"
        ),
        "byteLength": _nonnegative(
            entry.get("byteLength"), name=f"{name} object byte length"
        ),
    }


def _v5_g0_funnel_fragments_descriptor_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the exact compact G0 funnel binding from sealed inventory.

    The persisted object is the core-owned binding wrapper, not a copied
    funnel payload.  Its content-addressed path, raw file identity, and
    semantic root are all named by the already-validated outer result/receipt
    chain, so callers never discover it through a namespace scan.
    """

    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_G0:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel-fragments descriptor is incompatible with evolved generation"
        )
    checked_result = validate_v5_proposal_result(result, manifest=checked_manifest)
    receipt = _mapping(checked_result["receipt"], name="v5 proposal receipt")
    root = _sha(
        checked_result["g0FunnelFragmentsSha256"],
        name="v5 G0 funnel-fragments root",
    )
    if receipt.get("g0FunnelFragmentsSha256") != root:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel-fragments receipt/result binding drifted"
        )
    inventory = _mapping(receipt["outputInventory"], name="v5 proposal output inventory")
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    object_descriptor = _v5_object_store_root_adapter(
        inventory=inventory,
        output_root=output_root,
        role="g0FunnelFragments",
        expected_semantic_sha256=root,
        name="v5 G0 funnel-fragments",
    )
    return {
        "schemaVersion": V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA,
        "coreSchemaVersion": V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA,
        **object_descriptor,
    }


def build_v5_g0_funnel_fragments_descriptor(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact G0-only compact funnel receipt descriptor."""

    return validate_v5_g0_funnel_fragments_descriptor(
        _v5_g0_funnel_fragments_descriptor_payload(result=result, manifest=manifest),
        result=result,
        manifest=manifest,
    )


def validate_v5_g0_funnel_fragments_descriptor(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the exact inventory projection for the G0 core receipt."""

    descriptor = _mapping(value, name="v5 G0 funnel-fragments descriptor")
    expected = _v5_g0_funnel_fragments_descriptor_payload(
        result=result, manifest=manifest
    )
    _exact_keys(
        descriptor,
        set(expected),
        name="v5 G0 funnel-fragments descriptor",
    )
    if descriptor != expected:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel-fragments descriptor binding drifted"
        )
    return descriptor


def _v5_g0_funnel_projection_stream_descriptor_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the fixed public JSONL stream and its exact receipt object.

    Both projections come only from the already-authenticated outer
    inventory.  The descriptor carries no copied receipt or funnel rows and
    never searches the object namespace.
    """

    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_G0:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel projection-stream descriptor is incompatible with evolved generation"
        )
    checked_result = validate_v5_proposal_result(result, manifest=checked_manifest)
    receipt = _mapping(checked_result["receipt"], name="v5 proposal receipt")
    root = _sha(
        checked_result["g0FunnelProjectionStreamReceiptSha256"],
        name="v5 G0 funnel projection-stream receipt root",
    )
    if receipt.get("g0FunnelProjectionStreamReceiptSha256") != root:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel projection-stream receipt/result binding drifted"
        )
    inventory = _mapping(receipt["outputInventory"], name="v5 proposal output inventory")
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    stream = _v5_inventory_artifact_adapter(
        inventory=inventory,
        output_root=output_root,
        kind="g0FunnelProjectionStream",
    )
    if (
        stream["relativePath"] != V5_G0_FUNNEL_PROJECTION_STREAM_PATH
        or stream["semanticSha256"] != root
    ):
        raise TemporalQDV5NativeError(
            "v5 G0 funnel projection-stream artifact binding drifted"
        )
    receipt_object = _v5_object_store_root_adapter(
        inventory=inventory,
        output_root=output_root,
        role="g0FunnelProjectionStreamReceipt",
        expected_semantic_sha256=root,
        name="v5 G0 funnel projection-stream receipt",
    )
    return {
        "schemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA,
        "coreReceiptSchemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA,
        "rowSchemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA,
        "stream": stream,
        "receiptObject": receipt_object,
    }


def build_v5_g0_funnel_projection_stream_descriptor(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact G0 public funnel projection-stream descriptor."""

    return validate_v5_g0_funnel_projection_stream_descriptor(
        _v5_g0_funnel_projection_stream_descriptor_payload(
            result=result, manifest=manifest
        ),
        result=result,
        manifest=manifest,
    )


def validate_v5_g0_funnel_projection_stream_descriptor(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the exact stream/receipt inventory projection for G0."""

    descriptor = _mapping(value, name="v5 G0 funnel projection-stream descriptor")
    expected = _v5_g0_funnel_projection_stream_descriptor_payload(
        result=result, manifest=manifest
    )
    _exact_keys(
        descriptor,
        set(expected),
        name="v5 G0 funnel projection-stream descriptor",
    )
    if descriptor != expected:
        raise TemporalQDV5NativeError(
            "v5 G0 funnel projection-stream descriptor binding drifted"
        )
    return descriptor


def _v5_evolved_publication_fragments_descriptor_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the one receipt-addressed all-attempt funnel object.

    This is deliberately an identity-only accessor: it never duplicates the
    core receipt payload or searches for a similarly shaped object.  The
    evolved outer receipt names the exact semantic root, and the already
    authenticated object-store inventory supplies the fixed content-addressed
    path, file hash, and byte length that a Rust prefinalizer can open.
    """

    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_EVOLVED:
        raise TemporalQDV5NativeError(
            "v5 evolved publication-fragments descriptor is incompatible with G0"
        )
    checked_result = validate_v5_evolved_proposal_result(
        result, manifest=checked_manifest
    )
    receipt = _mapping(checked_result["receipt"], name="v5 evolved proposal receipt")
    root = _sha(
        checked_result["publicationFragmentsSha256"],
        name="v5 evolved publication-fragments root",
    )
    if receipt.get("publicationFragmentsSha256") != root:
        raise TemporalQDV5NativeError(
            "v5 evolved publication-fragments receipt/result binding drifted"
        )
    inventory = _mapping(receipt["outputInventory"], name="v5 proposal output inventory")
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    object_descriptor = _v5_object_store_root_adapter(
        inventory=inventory,
        output_root=output_root,
        role="publicationFragments",
        expected_semantic_sha256=root,
        name="v5 evolved publication-fragments",
    )
    return {
        "schemaVersion": V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA,
        "coreSchemaVersion": V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA,
        **object_descriptor,
    }


def build_v5_evolved_publication_fragments_descriptor(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact evolved all-attempt funnel receipt descriptor."""

    return validate_v5_evolved_publication_fragments_descriptor(
        _v5_evolved_publication_fragments_descriptor_payload(
            result=result, manifest=manifest
        ),
        result=result,
        manifest=manifest,
    )


def validate_v5_evolved_publication_fragments_descriptor(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require an exact inventory projection for the v2 core funnel receipt."""

    descriptor = _mapping(value, name="v5 evolved publication-fragments descriptor")
    expected = _v5_evolved_publication_fragments_descriptor_payload(
        result=result, manifest=manifest
    )
    _exact_keys(
        descriptor,
        set(expected),
        name="v5 evolved publication-fragments descriptor",
    )
    if descriptor != expected:
        raise TemporalQDV5NativeError(
            "v5 evolved publication-fragments descriptor binding drifted"
        )
    return descriptor


def _v5_invocation_document_descriptor(
    *,
    value: Mapping[str, Any],
    document_schema_version: str,
    semantic_sha256: str,
    relative_path: str,
    output_root: Path,
    name: str,
) -> dict[str, Any]:
    """Bind one canonical invocation document without reopening a directory.

    Invocation control files are write-once canonical JSON lines.  Their
    semantic self-hash is already validated by the manifest/result chain, so
    the descriptor derives the exact stored file identity directly from those
    canonical bytes and one fixed receipt-addressed path.
    """

    document = _mapping(value, name=name)
    if document.get("schemaVersion") != document_schema_version:
        raise TemporalQDV5NativeError(f"{name} schema drifted")
    relative = _safe_relative_output_path(relative_path, name=f"{name} path")
    canonical_bytes = canonical_json_bytes(document) + b"\n"
    return {
        "schemaVersion": V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
        "documentSchemaVersion": document_schema_version,
        "relativePath": relative,
        "absolutePath": str(output_root / relative),
        "semanticSha256": _sha(semantic_sha256, name=f"{name} semantic identity"),
        "fileSha256": sha256(canonical_bytes),
        "byteLength": len(canonical_bytes),
    }


def _v5_evolved_native_v5_invocation_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Project fixed invocation documents for the native prefinalizer.

    The prefinalizer gets a complete, sealed route to the exact control-plane
    manifest/result pair.  It must never enumerate ``native-batch`` or search
    for a result marker whose name happens to look compatible.
    """

    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_EVOLVED:
        raise TemporalQDV5NativeError(
            "v5 evolved native invocation descriptor is incompatible with G0"
        )
    checked_result = validate_v5_evolved_proposal_result(
        result, manifest=checked_manifest
    )
    receipt = _mapping(checked_result["receipt"], name="v5 evolved proposal receipt")
    manifest_sha256 = _sha(
        checked_manifest["manifestSha256"], name="v5 evolved invocation manifest identity"
    )
    if checked_result["manifestSha256"] != manifest_sha256:
        raise TemporalQDV5NativeError(
            "v5 evolved native invocation result/manifest identity drifted"
        )
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    invocation_root = (
        "native-batch/v5-proposal/" + manifest_sha256.removeprefix("sha256:")
    )
    manifest_descriptor = _v5_invocation_document_descriptor(
        value=checked_manifest,
        document_schema_version=V5_PROPOSAL_MANIFEST_SCHEMA,
        semantic_sha256=manifest_sha256,
        relative_path=f"{invocation_root}/manifest.json",
        output_root=output_root,
        name="v5 evolved invocation proposal manifest",
    )
    result_descriptor = _v5_invocation_document_descriptor(
        value=checked_result,
        document_schema_version=V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
        semantic_sha256=checked_result["resultSha256"],
        relative_path=f"{invocation_root}/{checked_manifest['resultPath']}",
        output_root=output_root,
        name="v5 evolved invocation proposal result",
    )
    return {
        "schemaVersion": V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA,
        "proposalManifest": manifest_descriptor,
        "proposalResult": result_descriptor,
        "proposalReceiptSha256": _sha(
            checked_result["receiptSha256"],
            name="v5 evolved invocation proposal receipt identity",
        ),
        "outputInventorySha256": _sha(
            checked_result["outputInventorySha256"],
            name="v5 evolved invocation output inventory identity",
        ),
    }


def build_v5_evolved_native_v5_invocation_descriptor(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact evolved native invocation descriptor."""

    return validate_v5_evolved_native_v5_invocation_descriptor(
        _v5_evolved_native_v5_invocation_payload(result=result, manifest=manifest),
        result=result,
        manifest=manifest,
    )


def validate_v5_evolved_native_v5_invocation_descriptor(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the exact fixed-path evolved invocation projection."""

    descriptor = _mapping(value, name="v5 evolved native invocation descriptor")
    expected = _v5_evolved_native_v5_invocation_payload(
        result=result, manifest=manifest
    )
    _exact_keys(
        descriptor,
        set(expected),
        name="v5 evolved native invocation descriptor",
    )
    if descriptor != expected:
        raise TemporalQDV5NativeError(
            "v5 evolved native invocation descriptor binding drifted"
        )
    return descriptor


def _v5_g0_native_v5_invocation_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the fixed G0 invocation documents without discovery scans."""

    checked_manifest = validate_v5_proposal_manifest(manifest)
    if checked_manifest["generationKind"] != V5_PROPOSAL_GENERATION_G0:
        raise TemporalQDV5NativeError(
            "v5 G0 native invocation descriptor is incompatible with evolved generation"
        )
    checked_result = validate_v5_proposal_result(result, manifest=checked_manifest)
    manifest_sha256 = _sha(
        checked_manifest["manifestSha256"], name="v5 G0 invocation manifest identity"
    )
    if checked_result["manifestSha256"] != manifest_sha256:
        raise TemporalQDV5NativeError(
            "v5 G0 native invocation result/manifest identity drifted"
        )
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    invocation_root = (
        "native-batch/v5-proposal/" + manifest_sha256.removeprefix("sha256:")
    )
    manifest_descriptor = _v5_invocation_document_descriptor(
        value=checked_manifest,
        document_schema_version=V5_PROPOSAL_MANIFEST_SCHEMA,
        semantic_sha256=manifest_sha256,
        relative_path=f"{invocation_root}/manifest.json",
        output_root=output_root,
        name="v5 G0 invocation proposal manifest",
    )
    result_descriptor = _v5_invocation_document_descriptor(
        value=checked_result,
        document_schema_version=V5_PROPOSAL_RESULT_SCHEMA,
        semantic_sha256=checked_result["resultSha256"],
        relative_path=f"{invocation_root}/{checked_manifest['resultPath']}",
        output_root=output_root,
        name="v5 G0 invocation proposal result",
    )
    return {
        "schemaVersion": V5_G0_NATIVE_V5_INVOCATION_SCHEMA,
        "proposalManifest": manifest_descriptor,
        "proposalResult": result_descriptor,
        "proposalReceiptSha256": _sha(
            checked_result["receiptSha256"],
            name="v5 G0 invocation proposal receipt identity",
        ),
        "outputInventorySha256": _sha(
            checked_result["outputInventorySha256"],
            name="v5 G0 invocation output inventory identity",
        ),
    }


def build_v5_g0_native_v5_invocation_descriptor(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact G0 native invocation descriptor."""

    return validate_v5_g0_native_v5_invocation_descriptor(
        _v5_g0_native_v5_invocation_payload(result=result, manifest=manifest),
        result=result,
        manifest=manifest,
    )


def validate_v5_g0_native_v5_invocation_descriptor(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the exact fixed-path G0 invocation projection."""

    descriptor = _mapping(value, name="v5 G0 native invocation descriptor")
    expected = _v5_g0_native_v5_invocation_payload(
        result=result, manifest=manifest
    )
    _exact_keys(
        descriptor,
        set(expected),
        name="v5 G0 native invocation descriptor",
    )
    if descriptor != expected:
        raise TemporalQDV5NativeError(
            "v5 G0 native invocation descriptor binding drifted"
        )
    return descriptor


def _v5_generation_construction_adapter_payload(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive the supervisor-facing projection from the sealed v5 receipt."""

    checked_manifest = validate_v5_proposal_manifest(manifest)
    checked_result = _validate_v5_result_for_manifest(
        result, manifest=checked_manifest
    )
    receipt = _mapping(checked_result["receipt"], name="v5 proposal receipt")
    inventory = _mapping(receipt["outputInventory"], name="v5 proposal output inventory")
    output_root = _v5_proposal_output_root(
        checked_manifest["outputRoot"], name="v5 proposal output root"
    )
    population = _v5_inventory_artifact_adapter(
        inventory=inventory, output_root=output_root, kind="population"
    )
    evaluation_population = _v5_inventory_artifact_adapter(
        inventory=inventory, output_root=output_root, kind="evaluationPopulation"
    )
    generation_journal = _v5_inventory_artifact_adapter(
        inventory=inventory, output_root=output_root, kind="generationJournal"
    )
    identity_ledger = _v5_inventory_artifact_adapter(
        inventory=inventory, output_root=output_root, kind="identityLedger"
    )
    if identity_ledger["semanticSha256"] != checked_result["identityLedgerSha256"]:
        raise TemporalQDV5NativeError(
            "v5 generation adapter identity-ledger receipt binding drifted"
        )
    adapter = {
        "schemaVersion": V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
        "operation": V5_PROPOSAL_OPERATION,
        "completed": True,
        "generationKind": checked_manifest["generationKind"],
        "generationIndex": checked_result["generationIndex"],
        "generationConfigSha256": checked_result["generationConfigSha256"],
        "authoritySha256": checked_result["authoritySha256"],
        "attemptCount": checked_result["attemptCount"],
        "acceptedCandidateCount": checked_result["acceptedRecordCount"],
        "selectedEvaluationCandidateCount": checked_result[
            "evaluationPopulationSize"
        ],
        "publicationPlanSha256": checked_result["publicationPlanSha256"],
        "publicationRequestSha256": checked_result["publicationRequestSha256"],
        "proposalResultSha256": checked_result["resultSha256"],
        "proposalReceiptSha256": checked_result["receiptSha256"],
        "outputInventorySha256": checked_result["outputInventorySha256"],
        "population": population,
        "evaluationPopulation": evaluation_population,
        "generationJournal": generation_journal,
        "identityLedger": identity_ledger,
    }
    if checked_manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0:
        adapter["g0FunnelFragments"] = _v5_g0_funnel_fragments_descriptor_payload(
            result=checked_result,
            manifest=checked_manifest,
        )
        adapter["g0FunnelProjectionStream"] = (
            _v5_g0_funnel_projection_stream_descriptor_payload(
                result=checked_result,
                manifest=checked_manifest,
            )
        )
        adapter["nativeV5Invocation"] = _v5_g0_native_v5_invocation_payload(
            result=checked_result,
            manifest=checked_manifest,
        )
    elif checked_manifest["generationKind"] == V5_PROPOSAL_GENERATION_EVOLVED:
        adapter["schemaVersion"] = V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
        adapter["evolvedPublicationFragments"] = (
            _v5_evolved_publication_fragments_descriptor_payload(
                result=checked_result,
                manifest=checked_manifest,
            )
        )
        adapter["nativeV5Invocation"] = _v5_evolved_native_v5_invocation_payload(
            result=checked_result,
            manifest=checked_manifest,
        )
    return adapter


def build_v5_generation_construction_adapter(
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Create the exact, self-hashed v5 handoff for supervisor orchestration.

    The adapter is deliberately not shaped like a legacy pair-generation
    result.  Every artifact identity and path comes from the receipt's
    authenticated inventory after validating the raw manifest/result/receipt
    chain; it never opens or reconstructs a rich candidate/population.
    """

    value = _v5_generation_construction_adapter_payload(
        result=result, manifest=manifest
    )
    value["adapterSha256"] = sha256(canonical_json_bytes(value))
    return validate_v5_generation_construction_adapter(
        value, result=result, manifest=manifest
    )


def validate_v5_generation_construction_adapter(
    value: object,
    *,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Require an adapter to be the exact projection of a sealed receipt tree."""

    adapter = _mapping(value, name="v5 generation construction adapter")
    expected = _v5_generation_construction_adapter_payload(
        result=result, manifest=manifest
    )
    expected["adapterSha256"] = sha256(canonical_json_bytes(expected))
    _exact_keys(
        adapter,
        set(expected),
        name="v5 generation construction adapter",
    )
    if adapter != expected:
        raise TemporalQDV5NativeError(
            "v5 generation construction adapter binding drifted"
        )
    return _self_hashed_object(
        adapter,
        identity_field="adapterSha256",
        name="v5 generation construction adapter",
    )


def run_native_v5_proposal_construction(
    *,
    output_root: Path | str,
    generation_config: Mapping[str, Any],
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    bidirectional_pair_policy: Mapping[str, Any],
    native_operator_authority: Mapping[str, Any],
    qd_engine_version: str,
    evaluation_population_size: int,
    execution_timeout_seconds: int,
    thread_cap: int = V5_PROPOSAL_THREAD_CAP_MAXIMUM,
    generation_kind: str = V5_PROPOSAL_GENERATION_G0,
    parent_archive_input: Mapping[str, Any] | None = None,
    identity_ledger_input: Mapping[str, Any] | None = None,
    on_process_started: Callable[[Any], None] | None = None,
    on_execution_evidence: Callable[[Mapping[str, Any]], None] | None = None,
    _return_manifest: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]:
    """Run exactly one batch transaction, with no compatibility fallback.

    ``on_execution_evidence`` is an optional observability side channel. It
    receives a detached evidence mapping only after both the immutable tree
    and the stdout evidence have been fully authenticated; it never changes
    the receipt-derived return value or its identity.
    """

    root = native._ensure_real_directory_tree(
        output_root, name="native v5 proposal output root"
    )
    timeout = _positive(execution_timeout_seconds, name="v5 execution timeout")
    # Fresh v5 owns no compile-at-launch escape hatch.  The exact batch
    # executable must already be present and is sealed before the one native
    # proposal transaction begins.
    binary, batch_authority = native.require_native_batch()
    batch_authority = native.validate_native_authority(batch_authority)
    if native._sha256_file(binary) != batch_authority["executableSha256"]:
        raise TemporalQDV5NativeError("native v5 batch executable changed after authority verification")
    if native.native_source_sha256() != batch_authority["sourceSha256"]:
        raise TemporalQDV5NativeError("native v5 batch source changed after authority verification")
    manifest = build_v5_proposal_manifest(
        output_root=root,
        generation_config=generation_config,
        pair_source_authority=pair_source_authority,
        evolvable_module_authority=evolvable_module_authority,
        bidirectional_pair_policy=bidirectional_pair_policy,
        native_operator_authority=native_operator_authority,
        qd_engine_version=qd_engine_version,
        native_batch_authority=batch_authority,
        evaluation_population_size=evaluation_population_size,
        thread_cap=thread_cap,
        generation_kind=generation_kind,
        parent_archive_input=parent_archive_input,
        identity_ledger_input=identity_ledger_input,
    )
    invocation_root = native._ensure_real_directory_tree(
        root
        / "native-batch"
        / "v5-proposal"
        / manifest["manifestSha256"].removeprefix("sha256:"),
        name="native v5 proposal invocation root",
    )
    native._write_bytes_once(
        invocation_root / "authority.json", canonical_json_bytes(batch_authority) + b"\n"
    )
    native._write_bytes_once(
        invocation_root / "frozen-authority.json",
        canonical_json_bytes(manifest["frozenAuthority"]) + b"\n",
    )
    # The invocation directory is private control state only.  Rust owns the
    # complete public output tree, including the shared-authority artifact;
    # writing or copying it here would create a second publisher before the
    # receipt-last durable seal exists.
    manifest_path = invocation_root / "manifest.json"
    native._write_bytes_once(manifest_path, canonical_json_bytes(manifest) + b"\n")
    result_path = invocation_root / V5_PROPOSAL_RESULT_FILENAME
    # A result artifact alone is not a restart authority.  Even a perfectly
    # self-hashed result can outlive a replaced compact journal/object tree.
    # Rust therefore always receives this manifest: when the immutable result
    # already exists it performs a zero-reconstruction adoption pass that
    # streams the complete sealed inventory before emitting stdout evidence.
    had_existing_result = (
        native._existing_regular_file(
            result_path,
            name="native v5 proposal result",
            maximum_bytes=_V5_COMPACT_PROPOSAL_RESULT_LIMIT_BYTES,
        )
        is not None
    )
    completed = native._run_checked(
        (str(binary), "--manifest", str(manifest_path)),
        cwd=native._repo_root(),
        timeout=float(timeout),
        on_process_started=on_process_started,
        stdout_limit_bytes=_V5_COMPACT_PROPOSAL_STDOUT_LIMIT_BYTES,
        stderr_limit_bytes=_V5_COMPACT_PROPOSAL_STDERR_LIMIT_BYTES,
    )
    stdout_value = native._parse_canonical_json_line(
        completed.stdout, name="native v5 proposal stdout"
    )
    published = native._existing_regular_file(
        result_path,
        name="native v5 proposal result",
        maximum_bytes=_V5_COMPACT_PROPOSAL_RESULT_LIMIT_BYTES,
    )
    if published is None:
        raise TemporalQDV5NativeError(
            "completed native v5 transaction did not publish its immutable result"
        )
    artifact = _validate_v5_result_for_manifest(
        native._parse_canonical_json_line(
            published[0], name="native v5 proposal result"
        ),
        manifest=manifest,
    )
    stdout_schema = (
        stdout_value.get("schemaVersion") if isinstance(stdout_value, Mapping) else None
    )
    execution_evidence: dict[str, Any] | None = None
    evidence_schema = (
        V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA
        if manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0
        else V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA
    )
    if stdout_schema == evidence_schema:
        if manifest["generationKind"] == V5_PROPOSAL_GENERATION_G0:
            execution_evidence = validate_v5_proposal_adoption_evidence(
                stdout_value,
                manifest=manifest,
                immutable_result=artifact,
            )
        else:
            execution_evidence = validate_v5_evolved_proposal_adoption_evidence(
                stdout_value,
                manifest=manifest,
                immutable_result=artifact,
            )
    else:
        if had_existing_result:
            raise TemporalQDV5NativeError(
                "native v5 restart did not return immutable adoption evidence"
            )
        stdout = _validate_v5_result_for_manifest(stdout_value, manifest=manifest)
        if artifact != stdout:
            raise TemporalQDV5NativeError("native v5 stdout/result artifact disagreement")
    if native._sha256_file(binary) != batch_authority["executableSha256"]:
        raise TemporalQDV5NativeError("native v5 batch executable changed during execution")
    if native.native_source_sha256() != batch_authority["sourceSha256"]:
        raise TemporalQDV5NativeError("native v5 batch source changed during execution")
    if on_execution_evidence is not None:
        if execution_evidence is None:
            raise TemporalQDV5NativeError(
                "native v5 transaction did not return validated execution evidence"
            )
        try:
            on_execution_evidence(deepcopy(execution_evidence))
        except Exception as error:
            raise TemporalQDV5NativeError(
                "native v5 execution-evidence callback failed"
            ) from error
    if _return_manifest:
        return artifact, manifest
    return artifact


def run_native_v5_generation_construction(
    *,
    output_root: Path | str,
    generation_config: Mapping[str, Any],
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    bidirectional_pair_policy: Mapping[str, Any],
    native_operator_authority: Mapping[str, Any],
    qd_engine_version: str,
    evaluation_population_size: int,
    execution_timeout_seconds: int,
    thread_cap: int = V5_PROPOSAL_THREAD_CAP_MAXIMUM,
    generation_kind: str = V5_PROPOSAL_GENERATION_G0,
    parent_archive_input: Mapping[str, Any] | None = None,
    identity_ledger_input: Mapping[str, Any] | None = None,
    on_process_started: Callable[[Any], None] | None = None,
    on_execution_evidence: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run one native v5 transaction and return its exact supervisor adapter.

    This is the production entry point for callers which need construction
    identities and paths. It still starts exactly one batch process and
    deliberately has no Python construction or compatibility fallback.
    ``on_execution_evidence`` is forwarded as a non-semantic observability
    side channel.
    """

    outcome = run_native_v5_proposal_construction(
        output_root=output_root,
        generation_config=generation_config,
        pair_source_authority=pair_source_authority,
        evolvable_module_authority=evolvable_module_authority,
        bidirectional_pair_policy=bidirectional_pair_policy,
        native_operator_authority=native_operator_authority,
        qd_engine_version=qd_engine_version,
        evaluation_population_size=evaluation_population_size,
        execution_timeout_seconds=execution_timeout_seconds,
        thread_cap=thread_cap,
        generation_kind=generation_kind,
        parent_archive_input=parent_archive_input,
        identity_ledger_input=identity_ledger_input,
        on_process_started=on_process_started,
        on_execution_evidence=on_execution_evidence,
        _return_manifest=True,
    )
    if not isinstance(outcome, tuple) or len(outcome) != 2:
        raise TemporalQDV5NativeError("v5 proposal bridge did not retain its manifest")
    result, manifest = outcome
    return build_v5_generation_construction_adapter(
        result=result,
        manifest=manifest,
    )


__all__ = [
    "TemporalQDV5NativeError",
    "V5_FROZEN_AUTHORITY_SCHEMA",
    "V5_SHARED_AUTHORITY_SCHEMA",
    "V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA",
    "V5_TEMPORAL_DOMAINS_SCHEMA",
    "V5_PROPOSAL_MANIFEST_SCHEMA",
    "V5_PROPOSAL_RESULT_SCHEMA",
    "V5_PROPOSAL_RECEIPT_SCHEMA",
    "V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA",
    "V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA",
    "V5_PROPOSAL_OUTPUT_ROOT_SCHEMA",
    "V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA",
    "V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA",
    "V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA",
    "V5_PROPOSAL_OBJECT_INVENTORY_PATH",
    "V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA",
    "V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA",
    "V5_EVOLVED_PROPOSAL_RESULT_SCHEMA",
    "V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA",
    "V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA",
    "V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA",
    "V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA",
    "V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA",
    "V5_PROPOSAL_INPUTS_SCHEMA",
    "V5_PROPOSAL_INPUT_BINDING_SCHEMA",
    "V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA",
    "V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA",
    "V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA",
    "V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA",
    "V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA",
    "V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA",
    "V5_G0_FUNNEL_PROJECTION_STREAM_PATH",
    "V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA",
    "V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA",
    "V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA",
    "V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA",
    "V5_G0_NATIVE_V5_INVOCATION_SCHEMA",
    "V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA",
    "V5_PROPOSAL_OPERATION",
    "V5_PROPOSAL_RESULT_FILENAME",
    "V5_PROPOSAL_GENERATION_G0",
    "V5_PROPOSAL_GENERATION_EVOLVED",
    "V5_PROPOSAL_THREAD_CAP_MAXIMUM",
    "build_v5_native_operator_authority",
    "build_v5_bidirectional_pair_policy",
    "build_v5_generation_bindings",
    "build_v5_frozen_authority",
    "validate_v5_frozen_authority",
    "build_v5_proposal_execution_authority",
    "validate_v5_proposal_execution_authority",
    "build_v5_proposal_manifest",
    "validate_v5_proposal_manifest",
    "validate_v5_proposal_result",
    "validate_v5_evolved_proposal_result",
    "build_v5_generation_construction_adapter",
    "validate_v5_generation_construction_adapter",
    "build_v5_g0_funnel_fragments_descriptor",
    "validate_v5_g0_funnel_fragments_descriptor",
    "build_v5_g0_funnel_projection_stream_descriptor",
    "validate_v5_g0_funnel_projection_stream_descriptor",
    "build_v5_evolved_publication_fragments_descriptor",
    "validate_v5_evolved_publication_fragments_descriptor",
    "build_v5_evolved_native_v5_invocation_descriptor",
    "validate_v5_evolved_native_v5_invocation_descriptor",
    "build_v5_g0_native_v5_invocation_descriptor",
    "validate_v5_g0_native_v5_invocation_descriptor",
    "build_v5_proposal_input_binding",
    "validate_v5_proposal_input_binding",
    "validate_v5_proposal_adoption_evidence",
    "validate_v5_evolved_proposal_adoption_evidence",
    "run_native_v5_proposal_construction",
    "run_native_v5_generation_construction",
]
