"""Restartable, generation-boundary supervisor for broad temporal QD search.

The supervisor freezes search policy once, then repeats the admitted sequence:
generate a complete population, freeze its evaluation identity, evaluate every
candidate/window task, canonically reduce the results, and checkpoint the next
generation boundary.  Worker completion order never participates in proposal or
archive identity.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .play_hand_lab import LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_generator_v2_continuation import ExactGeneratorV2Continuation
from .temporal_qd_pair_factory import (
    PairAuthorityBundle,
    immigrant_capacity_audit,
    load_pair_run_config,
    pair_policy_from_config,
)
from .temporal_qd_pair_generation import (
    _rotating_parent_schedule,
    build_pair_generation_config,
)
from .temporal_qd_campaign import freeze_qd_screening_campaign
from .temporal_qd_evolution import (
    QD_IDENTITY_LEDGER_SCHEMA,
    QD_POLICY,
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_POPULATION_SCHEMA,
    QD_VERSION,
    _identity_payload,
    _load_archive,
    _parent_member_order,
    _quality_member,
    _normalize_parameters,
    _resolve_archive_policy_authority,
    _read,
    build_qd_archive,
    build_rotating_qd_parent_archive,
    generate_qd_generation,
    load_qd_evaluated_members,
    qd_construction_operator_policy,
    qd_predeclared_evidence_context,
    qd_canonical_evidence_identity,
)
from .temporal_qd_funnel_adapter import build_qd_generation_funnel
from .temporal_qd_native import (
    G0_FINALIZATION_RUNTIME_RUST,
    PAIR_GENERATION_RUNTIME_DEFAULT,
    PAIR_GENERATION_RUNTIME_PYTHON,
    PAIR_GENERATION_RUNTIME_RUST,
    TemporalQDNativeError,
    build_g0_finalization_runtime_config,
    build_pair_generation_runtime_config,
    load_legacy_v5_g0_finalization_runtime,
    validate_generation_manifest,
    validate_generation_result,
    validate_g0_finalization_runtime_config,
    validate_pair_generation_runtime_config,
)
from .temporal_qd_v5_native import (
    TemporalQDV5NativeError,
    V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
    V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA,
    V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
    V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA,
    V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA,
    V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA,
    V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA,
    V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
    V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA,
    V5_G0_NATIVE_V5_INVOCATION_SCHEMA,
    V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA,
    V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
    V5_PROPOSAL_MANIFEST_SCHEMA,
    V5_PROPOSAL_RESULT_SCHEMA,
    V5_PROPOSAL_RESULT_FILENAME,
    V5_PROPOSAL_GENERATION_EVOLVED,
    V5_PROPOSAL_GENERATION_G0,
    V5_PROPOSAL_OPERATION,
    V5_PROPOSAL_THREAD_CAP_MAXIMUM,
    build_v5_bidirectional_pair_policy,
    build_v5_proposal_input_binding,
    build_v5_generation_bindings,
    build_v5_native_operator_authority,
    run_native_v5_generation_construction,
    validate_v5_proposal_input_binding,
)
from .temporal_qd_v5_native_tail import (
    build_v5_directional_tail_authority,
    validate_v5_directional_tail_index,
    v5_directional_compact_window_evidence,
)
from .temporal_qd_v5_control_plane import (
    GENERATION_RECORD_SCHEMA,
    GENERATION_STATE_APPLICATION_SIDECAR_FILENAME,
    GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA,
    GENERATION_STATE_PATCH_SCHEMA,
    TemporalQDV5ControlPlaneError,
    build_native_campaign_seal_source,
    build_native_panel_bundle_sidecar,
    build_native_rotating_campaign_receipt,
    build_native_v5_prefinalizer_base_manifest,
    build_native_v5_prefinalizer_resume_manifest,
    certify_native_v5_initial_archive,
    native_v5_archive_transport_path_matches,
    native_v5_transport_path_matches,
    assemble_native_v5_funnel_reduction_source,
    extract_native_v5_evolved_attempt_chain,
    extract_native_v5_g0_selected_attempts,
    run_native_campaign_seal,
    run_native_gateway_dispatch,
    run_native_v5_campaign_freeze,
    run_native_v5_generation_finalizer,
    run_native_v5_rotating_prefinalizer,
    run_native_v5_evidence_ladder_archive_freeze,
    run_native_v5_archive_reducer,
)
from .temporal_qd_evidence_ladder import (
    build_evidence_ladder,
    validate_template_discovery_windows,
    validate_template_stage_window,
)
from .temporal_qd_rotating_evidence import (
    build_candidate_panel_bundle,
    build_cumulative_breeder_archive,
    build_current_panel_evaluation_cohort,
    build_generation_evidence_checkpoint,
    build_rotating_evidence_contract,
    panel_for_generation,
    reduce_provisional_diverse_survivors,
    required_panel_ids,
    template_for_generation,
    validate_generation_template,
    validate_panel_template,
    validate_rotating_evidence_contract,
)
from .temporal_qd_evaluation_population import (
    build_rotating_cohort_population,
    evaluation_population_path,
    hydrate_evaluation_candidate,
    load_evaluation_population,
)
from .temporal_discovery_results import load_provenance_bound_window_evidence
from .temporal_qd_tail_result_index import (
    build_tail_result_index,
    validate_tail_result_index,
)
from .temporal_generation_funnel import (
    GenerationFunnelContractError,
    supervisor_funnel_snapshot,
    write_generation_funnel_artifact,
)
from .temporal_search import run_temporal_search_tasks
from .temporal_proposal_lineage_artifact import write_proposal_lineage_unavailable
from .result_codec import ResultCodecError, canonical_json_bytes, read_json_object

# Invocation manifests carry the frozen static authority graph (not rows or
# result inventory).  The production G0 authority is about 1.1 MiB, so it has
# a separate, fixed 2 MiB transport budget.  Invocation results/receipts stay
# capped at 1 MiB; anything candidate-scale belongs in Rust-owned sidecars.
_NATIVE_V5_INVOCATION_MANIFEST_LIMIT_BYTES = 2 * 1_048_576
_NATIVE_V5_INVOCATION_RESULT_LIMIT_BYTES = 1_048_576


SUPERVISOR_VERSION = "temporal_qd_supervisor_v3"
SUPERVISOR_CONFIG_SCHEMA = "temporal_qd_supervisor_config_v3"
SUPERVISOR_STATE_SCHEMA = "temporal_qd_supervisor_state_v3"
_SHA256_LENGTH = 71
_GIT_SHA_LENGTH = 40

# Fresh broad campaigns and legacy continuations intentionally have different
# shapes.  Keep the derived totals here so every admission surface freezes the
# same authority rather than restating campaign arithmetic at call sites.
FRESH_BROAD_GENERATION_COUNT = 5
FRESH_BROAD_CANDIDATES_PER_GENERATION = 1_024
FRESH_BROAD_DISCOVERY_WINDOWS_PER_CANDIDATE = 3
FRESH_BROAD_CANDIDATE_EVALUATIONS = (
    FRESH_BROAD_GENERATION_COUNT * FRESH_BROAD_CANDIDATES_PER_GENERATION
)
FRESH_BROAD_DISCOVERY_WORKER_TASKS = (
    FRESH_BROAD_CANDIDATE_EVALUATIONS
    * FRESH_BROAD_DISCOVERY_WINDOWS_PER_CANDIDATE
)
LEGACY_CONTINUATION_GENERATION_COUNT = 4
LEGACY_CONTINUATION_CANDIDATE_EVALUATIONS = (
    LEGACY_CONTINUATION_GENERATION_COUNT
    * FRESH_BROAD_CANDIDATES_PER_GENERATION
)
LEGACY_CONTINUATION_DISCOVERY_WORKER_TASKS = (
    LEGACY_CONTINUATION_CANDIDATE_EVALUATIONS
    * FRESH_BROAD_DISCOVERY_WINDOWS_PER_CANDIDATE
)
QD_COST_VIEWS = {
    "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
    "research_conservative": {
        "spreadBps": 2.0,
        "slippageBps": 1.0,
        "commissionBps": 0.5,
    },
}

# This is deliberately an execution-only switch.  It is not part of the
# frozen config, campaign authority, checkpoint, ledger, or result identity:
# an interrupted run may resume in either mode without changing its semantic
# contract.  Legacy raw loading stays the default/oracle until the indexed
# path has a longer production parity history.
TAIL_RESULT_MODE_LEGACY = "legacy"
TAIL_RESULT_MODE_INDEXED = "indexed"
_TAIL_RESULT_MODES = frozenset(
    {TAIL_RESULT_MODE_LEGACY, TAIL_RESULT_MODE_INDEXED}
)
NATIVE_FINALIZATION_VALIDATION_NONE = "none"
# Retained only to recognize old completed-generation bindings internally.
# Fresh historical admission is deliberately not an accepted CLI/runtime mode.
NATIVE_FINALIZATION_VALIDATION_HISTORICAL = "rust_historical_admission"
_NATIVE_FINALIZATION_VALIDATION_MODES = frozenset({NATIVE_FINALIZATION_VALIDATION_NONE})
NATIVE_FINALIZATION_BINDING_SCHEMA = (
    "temporal_qd_native_generation_finalization_binding_v1"
)
NATIVE_FINALIZATION_ADMISSION_SCHEMA = (
    "temporal_qd_native_historical_generation_admission_v1"
)
NATIVE_FOUNDATION_CONTRACT_VERSION = "temporal_qd_native_foundation_v1"
NATIVE_FINALIZATION_RUNTIME_AUTHORITY_SCHEMA = (
    "temporal_qd_native_finalization_runtime_authority_v1"
)
NATIVE_FINALIZATION_AUTHORITY_ROTATION_SCHEMA = (
    "temporal_qd_native_finalization_authority_rotation_v1"
)
NATIVE_FINALIZATION_AUTHORITY_HISTORY_DIR = "native-finalization-authorities"
NATIVE_FINALIZATION_ADOPTION_AUTHORITY_SCHEMA = (
    "temporal_qd_python_boundary_adoption_authority_v1"
)
NATIVE_FINALIZATION_ADOPTION_AUTHORITY_FILE = (
    "native-finalization-adoption-authority.json"
)
NATIVE_V5_CONTROL_PLANE_RUNTIME_ROLES = frozenset(
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
GENERATION_FINALIZATION_ENGINE_PYTHON = "python"
GENERATION_FINALIZATION_ENGINE_RUST = "rust"
GENERATION_FINALIZATION_ENGINE_DEFAULT = GENERATION_FINALIZATION_ENGINE_RUST
_GENERATION_FINALIZATION_ENGINES = frozenset(
    {GENERATION_FINALIZATION_ENGINE_PYTHON, GENERATION_FINALIZATION_ENGINE_RUST}
)
NATIVE_V5_PROPOSAL_RUNTIME_SCHEMA = "temporal_qd_native_v5_proposal_runtime_v1"
NATIVE_V5_PROPOSAL_ENGINE = "rust_native_v5_transaction"
NATIVE_V5_SUPERVISOR_INVOCATION_SCHEMA = (
    "temporal_qd_native_v5_supervisor_invocation_v1"
)
NATIVE_V5_IDENTITY_LEDGER_TRANSACTION_SCHEMA = (
    "temporal_qd_native_v5_identity_ledger_transaction_v1"
)
NATIVE_V5_STATE_APPLICATION_PENDING_KEY = "nativeV5StateApplicationPending"
NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY = "nativeV5CommittedIdentityLedger"


def _v5_evolvable_authority(config: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return only a fresh directional v5 authority, never a near match.

    The distinction matters because a legacy pair authority may still use the
    generic Rust bridge.  The evolvable authority is the unambiguous cutover
    signal: it requires the one-shot v5 transaction and rejects every Python
    construction route.
    """

    authority = config.get("evolvableModuleAuthority")
    if authority is None:
        return None
    if not isinstance(authority, Mapping):
        raise TemporalDiscoveryContractError("evolvable v5 module authority is invalid")
    policy = authority.get("archivePolicyAuthority")
    if not isinstance(policy, Mapping):
        raise TemporalDiscoveryContractError(
            "evolvable v5 module authority lacks archive policy authority"
        )
    try:
        _name, _sha, _frozen, directional = _resolve_archive_policy_authority(policy)
    except TemporalDiscoveryContractError as exc:
        raise TemporalDiscoveryContractError(
            "evolvable v5 module authority archive policy is invalid"
        ) from exc
    if not directional:
        raise TemporalDiscoveryContractError(
            "evolvable module authority requires the exact v5 archive policy"
        )
    return authority


def _native_v5_proposal_enabled(config: Mapping[str, Any]) -> bool:
    """Tell fresh native-v5 configs apart from archived Python-era v5 runs.

    The runtime seal was introduced by this cutover.  Its absence therefore
    identifies a historical artifact that may be reopened read-only, but can
    never become a new Python production construction route.  A malformed
    fresh seal is rejected by ``_validate_native_v5_proposal_runtime`` before
    anything is allowed to run.
    """

    return "nativeV5ProposalRuntime" in config


def _native_v5_qd_engine_version(
    *, config: Mapping[str, Any], evolvable_authority: Mapping[str, Any]
) -> str:
    """Use the archive-authority version, never an independently supplied label."""

    archive_authority = evolvable_authority.get("archivePolicyAuthority")
    if not isinstance(archive_authority, Mapping):
        raise TemporalDiscoveryContractError("native v5 archive authority is unavailable")
    qd_engine_version = archive_authority.get("qdVersion")
    if not isinstance(qd_engine_version, str) or not qd_engine_version:
        raise TemporalDiscoveryContractError("native v5 archive QD version is invalid")
    if config.get("qdVersion") != qd_engine_version:
        raise TemporalDiscoveryContractError(
            "native v5 archive QD version drifted from the frozen supervisor"
        )
    return qd_engine_version


def _build_native_v5_proposal_runtime(
    *,
    pair_source_authority: Mapping[str, Any],
    evolvable_module_authority: Mapping[str, Any],
    generation_run_config: Mapping[str, Any],
    execution_timeout_seconds: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Seal the pure v5 control plane without opening a Python authority.

    The v5 bridge owns static authority projection.  This helper intentionally
    does not import or instantiate ``PairAuthorityBundle``: doing so would
    recreate the retired Dashboard/Python construction path before Rust has a
    chance to own the first proposal.
    """

    try:
        pair_policy = build_v5_bidirectional_pair_policy(
            pair_source_authority=pair_source_authority
        )
        bindings = build_v5_generation_bindings(
            generation_run_config=generation_run_config,
            pair_source_authority=pair_source_authority,
            evolvable_module_authority=evolvable_module_authority,
        )
        native_operator = build_v5_native_operator_authority(
            pair_source_authority=pair_source_authority,
            evolvable_module_authority=evolvable_module_authority,
        )
    except TemporalQDV5NativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    if (
        bindings.get("archivePolicyAuthority")
        != evolvable_module_authority.get("archivePolicyAuthority")
        or bindings.get("behaviorAttributionRequirement")
        != evolvable_module_authority.get("behaviorAttributionRequirement")
    ):
        raise TemporalDiscoveryContractError("native v5 generation bindings drifted")
    try:
        timeout = int(execution_timeout_seconds)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            "native v5 proposal execution timeout is invalid"
        ) from exc
    if isinstance(execution_timeout_seconds, bool) or timeout < 1:
        raise TemporalDiscoveryContractError(
            "native v5 proposal execution timeout is invalid"
        )
    runtime = {
        "schemaVersion": NATIVE_V5_PROPOSAL_RUNTIME_SCHEMA,
        "engine": NATIVE_V5_PROPOSAL_ENGINE,
        "executionTimeoutSeconds": timeout,
        "threadCap": V5_PROPOSAL_THREAD_CAP_MAXIMUM,
        "nativeOperatorAuthority": _clone(
            native_operator, name="native v5 operator authority"
        ),
        "nativeOperatorAuthoritySha256": native_operator[
            "nativeOperatorAuthoritySha256"
        ],
        "bidirectionalPairPolicy": _clone(
            pair_policy, name="native v5 bidirectional pair policy"
        ),
        "bidirectionalPairPolicySha256": canonical_sha256(pair_policy),
    }
    runtime["runtimeSha256"] = canonical_sha256(runtime)
    return runtime, _clone(bindings, name="native v5 generation bindings")


def _validate_native_v5_proposal_runtime(
    *, config: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebuild and compare the sealed v5 control plane before every phase."""

    evolvable = _v5_evolvable_authority(config)
    if evolvable is None:
        raise TemporalDiscoveryContractError("native v5 proposal runtime lacks v5 authority")
    source = config.get("bidirectionalPairSourceAuthority")
    run_config = config.get("bidirectionalPairGeneration")
    stored = config.get("nativeV5ProposalRuntime")
    if not isinstance(source, Mapping) or not isinstance(run_config, Mapping):
        raise TemporalDiscoveryContractError("native v5 pair source authority is unavailable")
    if not isinstance(stored, Mapping):
        raise TemporalDiscoveryContractError("native v5 proposal runtime is unavailable")
    material = _clone(stored, name="native v5 proposal runtime")
    supplied = material.pop("runtimeSha256", None)
    expected_fields = {
        "schemaVersion",
        "engine",
        "executionTimeoutSeconds",
        "threadCap",
        "nativeOperatorAuthority",
        "nativeOperatorAuthoritySha256",
        "bidirectionalPairPolicy",
        "bidirectionalPairPolicySha256",
    }
    if (
        set(material) != expected_fields
        or stored.get("schemaVersion") != NATIVE_V5_PROPOSAL_RUNTIME_SCHEMA
        or stored.get("engine") != NATIVE_V5_PROPOSAL_ENGINE
        or supplied != canonical_sha256(material)
    ):
        raise TemporalDiscoveryContractError("native v5 proposal runtime identity drifted")
    source_generation_run_config = _clone(
        source, name="native v5 source generation run config"
    )
    # Rebuild from the original sealed source, not from the enriched output
    # persisted in the supervisor config.  The latter is comparison evidence
    # only and must never become a second source authority.
    source_generation_run_config.pop("operatorImplementation", None)
    rebuilt, bindings = _build_native_v5_proposal_runtime(
        pair_source_authority=source,
        evolvable_module_authority=evolvable,
        generation_run_config=source_generation_run_config,
        execution_timeout_seconds=stored["executionTimeoutSeconds"],
    )
    if _clone(stored, name="native v5 proposal runtime") != rebuilt:
        raise TemporalDiscoveryContractError("native v5 proposal runtime authority drifted")
    return rebuilt, bindings


def _native_v5_proposal_root(root: Path, generation_index: int) -> Path:
    return root / "generations" / f"generation-{generation_index:04d}" / "proposal"


def _native_v5_identity_ledger_output_path(root: Path, generation_index: int) -> Path:
    return _native_v5_proposal_root(root, generation_index) / "v5-native" / "identity-ledger.json"


def _native_v5_identity_ledger_snapshot_path(root: Path, generation_index: int) -> Path:
    return _native_v5_proposal_root(root, generation_index) / "input-identity-ledger.json"


def _validated_native_v5_identity_ledger(
    path: Path, *, name: str
) -> tuple[dict[str, Any], str]:
    """Historical-only ledger reader.

    Current Rust-native v5 never calls this helper.  Its proposal ledger is a
    receipt-authenticated opaque artifact, carried by the adapter descriptor
    into the next native transaction.  Keep this reader for explicit legacy
    migration/oracle paths only; making it part of current-v5 admission would
    turn a candidate-scale identity ledger back into a Python control input.
    """

    ledger = _canonical_file(path, name=name)
    ledger_sha256 = _identity_payload(ledger, "identityLedgerSha256", name=name)
    return ledger, ledger_sha256


def _native_v5_adapter_artifact(
    *,
    adapter: Mapping[str, Any],
    name: str,
    relative_path: str,
    proposal_root: Path,
) -> dict[str, Any]:
    artifact = adapter.get(name)
    if not isinstance(artifact, Mapping):
        raise TemporalDiscoveryContractError(f"native v5 construction lacks {name} artifact")
    material = _clone(artifact, name=f"native v5 {name} artifact")
    if set(material) != {
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    }:
        raise TemporalDiscoveryContractError(f"native v5 {name} artifact is malformed")
    expected_path = Path(os.path.abspath(str(proposal_root / relative_path)))
    supplied_path = str(material["absolutePath"])
    if (
        material["relativePath"] != relative_path
        or supplied_path != str(Path(os.path.abspath(supplied_path)))
        or supplied_path != str(expected_path)
    ):
        raise TemporalDiscoveryContractError(f"native v5 {name} artifact path drifted")
    semantic_sha256 = _sha256(
        material["semanticSha256"], name=f"native v5 {name} semantic identity"
    )
    file_sha256 = _sha256(
        material["fileSha256"], name=f"native v5 {name} file identity"
    )
    byte_length = material["byteLength"]
    if (
        isinstance(byte_length, bool)
        or not isinstance(byte_length, int)
        or byte_length < 0
    ):
        raise TemporalDiscoveryContractError(f"native v5 {name} artifact descriptor drifted")
    # The Rust proposal receipt is the authority for the candidate-scale
    # artifact bytes.  Python must only retain this exact bounded descriptor:
    # a stat/hash/read here would reintroduce a per-population control loop and
    # would make a completed native receipt non-resumable after source pruning.
    return {
        "relativePath": relative_path,
        "absolutePath": str(expected_path),
        "semanticSha256": semantic_sha256,
        "fileSha256": file_sha256,
        "byteLength": byte_length,
    }


def _read_native_v5_compact_document_bytes(
    path: Path,
    *,
    name: str,
    maximum_bytes: int,
) -> bytes:
    """Read one receipt-addressed control document, never a row stream."""

    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes < 1
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {name} compact-document limit is invalid"
        )
    try:
        if path.stat().st_size > maximum_bytes:
            raise TemporalDiscoveryContractError(
                f"native v5 {name} invocation document exceeds the compact-document limit"
            )
        with path.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
    except TemporalDiscoveryContractError:
        raise
    except OSError as exc:
        raise TemporalDiscoveryContractError(
            f"could not read native v5 {name} invocation document"
        ) from exc
    if len(raw) > maximum_bytes:
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document exceeds the compact-document limit"
        )
    return raw


def _native_v5_invocation_document_limit(document_schema: str) -> int:
    """Return the distinct bounded transport budget for an invocation leaf."""

    if document_schema == V5_PROPOSAL_MANIFEST_SCHEMA:
        return _NATIVE_V5_INVOCATION_MANIFEST_LIMIT_BYTES
    if document_schema in {
        V5_PROPOSAL_RESULT_SCHEMA,
        V5_EVOLVED_PROPOSAL_RESULT_SCHEMA,
    }:
        return _NATIVE_V5_INVOCATION_RESULT_LIMIT_BYTES
    raise TemporalDiscoveryContractError(
        "native v5 invocation document schema is unsupported"
    )


def _native_v5_invocation_document(
    *,
    descriptor: object,
    proposal_root: Path,
    relative_path: str,
    document_schema: str,
    identity_field: str,
    name: str,
) -> dict[str, Any]:
    """Open one receipt-addressed control document at its only valid path.

    This is deliberately narrower than an artifact loader: evolved proposal
    invocation documents are compact canonical control objects, and their
    semantic identities are what authorise the Rust prefinalizer to reopen the
    proposal transaction.  The supervisor must not discover them by scanning a
    proposal directory or accept an alias/symlink in their place.
    """

    material = _clone(descriptor, name=f"native v5 {name} descriptor")
    expected_keys = {
        "schemaVersion",
        "documentSchemaVersion",
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    }
    if set(material) != expected_keys:
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation descriptor is malformed"
        )
    expected_path = Path(os.path.abspath(str(proposal_root / relative_path)))
    supplied_path = material["absolutePath"]
    if (
        material["schemaVersion"] != V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA
        or material["documentSchemaVersion"] != document_schema
        or material["relativePath"] != relative_path
        or not isinstance(supplied_path, str)
        or supplied_path != str(Path(os.path.abspath(supplied_path)))
        or supplied_path != str(expected_path)
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation descriptor path/schema drifted"
        )
    try:
        path_status = expected_path.lstat()
    except OSError as exc:
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document is unavailable"
        ) from exc
    if not os.path.isfile(expected_path) or os.path.islink(expected_path):
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document is not a regular file"
        )
    byte_length = material["byteLength"]
    semantic_sha256 = _sha256(
        material["semanticSha256"], name=f"native v5 {name} semantic identity"
    )
    file_sha256 = _sha256(
        material["fileSha256"], name=f"native v5 {name} file identity"
    )
    maximum_bytes = _native_v5_invocation_document_limit(document_schema)
    if (
        isinstance(byte_length, bool)
        or not isinstance(byte_length, int)
        or byte_length < 0
        or byte_length > maximum_bytes
        or path_status.st_size != byte_length
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document receipt drifted"
        )
    try:
        raw = _read_native_v5_compact_document_bytes(
            expected_path,
            name=name,
            maximum_bytes=maximum_bytes,
        )
        if "sha256:" + hashlib.sha256(raw).hexdigest() != file_sha256:
            raise TemporalDiscoveryContractError(
                f"native v5 {name} invocation document receipt drifted"
            )
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not parse native v5 {name} invocation document"
        ) from exc
    if not isinstance(document, Mapping) or canonical_json_bytes(dict(document)) + b"\n" != raw:
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document is not canonical"
        )
    checked = _clone(document, name=f"native v5 {name} invocation document")
    supplied_identity = _sha256(
        checked.pop(identity_field, None),
        name=f"native v5 {name} invocation identity",
    )
    if (
        checked.get("schemaVersion") != document_schema
        or supplied_identity != semantic_sha256
        or canonical_sha256(checked) != supplied_identity
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {name} invocation document identity drifted"
        )
    checked[identity_field] = supplied_identity
    return {
        "schemaVersion": V5_INVOCATION_DOCUMENT_DESCRIPTOR_SCHEMA,
        "documentSchemaVersion": document_schema,
        "relativePath": relative_path,
        "absolutePath": str(expected_path),
        "semanticSha256": semantic_sha256,
        "fileSha256": file_sha256,
        "byteLength": byte_length,
        "document": checked,
    }


def _validate_native_v5_generation_invocation_descriptor(
    *,
    adapter: Mapping[str, Any],
    proposal_root: Path,
    generation_kind: str,
) -> dict[str, Any]:
    """Validate the family-specific exact native invocation descriptor.

    Both current adapter families now bind their compact invocation documents
    by fixed receipt-addressed paths.  The descriptor schema intentionally
    differs by family so a G0 funnel receipt can never be replayed as an
    evolved all-attempt transaction (or the reverse).
    """

    raw_value = adapter.get("nativeV5Invocation")
    if not isinstance(raw_value, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 generation lacks its invocation descriptor"
        )
    value = _clone(raw_value, name="native v5 generation invocation")
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        invocation_schema = V5_G0_NATIVE_V5_INVOCATION_SCHEMA
        result_schema = V5_PROPOSAL_RESULT_SCHEMA
        family = "G0"
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        invocation_schema = V5_EVOLVED_NATIVE_V5_INVOCATION_SCHEMA
        result_schema = V5_EVOLVED_PROPOSAL_RESULT_SCHEMA
        family = "evolved"
    else:
        raise TemporalDiscoveryContractError("native v5 invocation generation kind is invalid")
    expected_keys = {
        "schemaVersion",
        "proposalManifest",
        "proposalResult",
        "proposalReceiptSha256",
        "outputInventorySha256",
    }
    if (
        set(value) != expected_keys
        or value["schemaVersion"] != invocation_schema
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {family} invocation descriptor shape drifted"
        )
    manifest_hint = _clone(
        value["proposalManifest"], name=f"native v5 {family} proposal manifest hint"
    )
    if not isinstance(manifest_hint, Mapping):
        raise TemporalDiscoveryContractError(
            f"native v5 {family} proposal manifest descriptor is invalid"
        )
    manifest_semantic = _sha256(
        manifest_hint.get("semanticSha256"),
        name=f"native v5 {family} proposal manifest identity",
    )
    invocation_root = "native-batch/v5-proposal/" + manifest_semantic.removeprefix(
        "sha256:"
    )
    manifest = _native_v5_invocation_document(
        descriptor=manifest_hint,
        proposal_root=proposal_root,
        relative_path=f"{invocation_root}/manifest.json",
        document_schema=V5_PROPOSAL_MANIFEST_SCHEMA,
        identity_field="manifestSha256",
        name=f"{family} proposal manifest",
    )
    result = _native_v5_invocation_document(
        descriptor=value["proposalResult"],
        proposal_root=proposal_root,
        relative_path=f"{invocation_root}/{V5_PROPOSAL_RESULT_FILENAME}",
        document_schema=result_schema,
        identity_field="resultSha256",
        name=f"{family} proposal result",
    )
    proposal_receipt_sha256 = _sha256(
        value["proposalReceiptSha256"],
        name=f"native v5 {family} proposal receipt identity",
    )
    output_inventory_sha256 = _sha256(
        value["outputInventorySha256"],
        name=f"native v5 {family} output inventory identity",
    )
    if (
        manifest["document"].get("generationKind") != generation_kind
        or manifest["document"].get("outputRoot") != str(proposal_root)
        or manifest["document"].get("resultPath") != V5_PROPOSAL_RESULT_FILENAME
        or result["document"].get("manifestSha256") != manifest["semanticSha256"]
        or result["semanticSha256"] != adapter["proposalResultSha256"]
        or result["document"].get("receiptSha256") != proposal_receipt_sha256
        or result["document"].get("outputInventorySha256") != output_inventory_sha256
        or proposal_receipt_sha256 != adapter["proposalReceiptSha256"]
        or output_inventory_sha256 != adapter["outputInventorySha256"]
        or (
            generation_kind == V5_PROPOSAL_GENERATION_G0
            and result["document"].get("g0FunnelFragmentsSha256")
            != adapter["g0FunnelFragments"]["semanticSha256"]
        )
        or (
            generation_kind == V5_PROPOSAL_GENERATION_G0
            and result["document"].get("g0FunnelProjectionStreamReceiptSha256")
            != adapter["g0FunnelProjectionStream"]["stream"]["semanticSha256"]
        )
    ):
        raise TemporalDiscoveryContractError(
            f"native v5 {family} invocation receipt binding drifted"
        )
    return {
        "schemaVersion": invocation_schema,
        "proposalManifest": {
            key: manifest[key]
            for key in (
                "schemaVersion",
                "documentSchemaVersion",
                "relativePath",
                "absolutePath",
                "semanticSha256",
                "fileSha256",
                "byteLength",
            )
        },
        "proposalResult": {
            key: result[key]
            for key in (
                "schemaVersion",
                "documentSchemaVersion",
                "relativePath",
                "absolutePath",
                "semanticSha256",
                "fileSha256",
                "byteLength",
            )
        },
        "proposalReceiptSha256": proposal_receipt_sha256,
        "outputInventorySha256": output_inventory_sha256,
    }


def _validate_native_v5_g0_funnel_projection_stream(
    *, adapter: Mapping[str, Any], proposal_root: Path
) -> dict[str, Any]:
    """Validate G0's fixed Rust-owned public funnel stream without reading rows.

    The adapter is the only public handoff for this JSONL.  Its companion
    receipt object authenticates the stream's bytes and ties them back to the
    G0 funnel fragments root.  Python deliberately does not deserialize the
    stream: candidate ordering and row semantics remain owned by the native
    transaction and its eventual native prefinalizer consumer.
    """

    value = _clone(
        adapter.get("g0FunnelProjectionStream"),
        name="native v5 G0 funnel projection stream",
    )
    expected_fields = {
        "schemaVersion",
        "coreReceiptSchemaVersion",
        "rowSchemaVersion",
        "stream",
        "receiptObject",
    }
    if (
        set(value) != expected_fields
        or value.get("schemaVersion")
        != V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA
        or value.get("coreReceiptSchemaVersion")
        != V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA
        or value.get("rowSchemaVersion") != V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA
    ):
        raise TemporalDiscoveryContractError(
            "native v5 G0 funnel projection stream descriptor drifted"
        )

    def bound_file(
        *, raw_value: object, relative_path: str, semantic_sha256: str, name: str
    ) -> dict[str, Any]:
        material = _clone(raw_value, name=name)
        expected = {
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        }
        expected_path = Path(os.path.abspath(str(proposal_root / relative_path)))
        supplied_path = material.get("absolutePath")
        byte_length = material.get("byteLength")
        if (
            set(material) != expected
            or material.get("relativePath") != relative_path
            or material.get("semanticSha256") != semantic_sha256
            or not isinstance(supplied_path, str)
            or supplied_path != str(Path(os.path.abspath(supplied_path)))
            or supplied_path != str(expected_path)
            or isinstance(byte_length, bool)
            or not isinstance(byte_length, int)
            or byte_length < 0
        ):
            raise TemporalDiscoveryContractError(f"native v5 {name} binding drifted")
        file_sha256 = _sha256(
            material.get("fileSha256"), name=f"native v5 {name} file identity"
        )
        return {
            "relativePath": relative_path,
            "absolutePath": str(expected_path),
            "semanticSha256": semantic_sha256,
            "fileSha256": file_sha256,
            "byteLength": byte_length,
        }

    stream_material = _clone(value.get("stream"), name="native v5 G0 funnel stream")
    stream_root = _sha256(
        stream_material.get("semanticSha256"),
        name="native v5 G0 funnel projection stream receipt identity",
    )
    stream = bound_file(
        raw_value=stream_material,
        relative_path=V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
        semantic_sha256=stream_root,
        name="G0 funnel projection stream",
    )
    receipt_relative_path = (
        "v5-native/objects/sha256/" + stream_root.removeprefix("sha256:") + ".json"
    )
    receipt_object = bound_file(
        raw_value=value.get("receiptObject"),
        relative_path=receipt_relative_path,
        semantic_sha256=stream_root,
        name="G0 funnel projection receipt object",
    )
    # The authenticated object-store entry is deliberately Core's wrapper,
    # not a bare receipt.  The Rust G0 extractor reopens and validates that
    # wrapper plus its nested receipt and stream.  Python transports only the
    # sealed descriptor; parsing it here would deserialize candidate-scale
    # provenance on every restart.
    fragments = adapter.get("g0FunnelFragments")
    if not isinstance(fragments, Mapping):
        raise TemporalDiscoveryContractError("native v5 G0 funnel fragments are unavailable")
    _sha256(
        fragments.get("semanticSha256"),
        name="native v5 G0 funnel fragments descriptor identity",
    )
    return {
        "schemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_DESCRIPTOR_SCHEMA,
        "coreReceiptSchemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_CORE_SCHEMA,
        "rowSchemaVersion": V5_G0_FUNNEL_PROJECTION_STREAM_ROW_SCHEMA,
        "stream": stream,
        "receiptObject": receipt_object,
    }


def _validate_native_v5_construction_adapter(
    *,
    value: Mapping[str, Any],
    proposal_root: Path,
    generation_index: int,
    generation_kind: str,
    generation_config_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate the exact bridge handoff without reconstructing a population.

    The bridge already authenticates the manifest/result/receipt/inventory
    chain.  The supervisor stores this self-hashed projection as bounded
    descriptor evidence; Rust owns reauthentication of every candidate-scale
    receipt-addressed artifact.
    """

    adapter = _clone(value, name="native v5 construction adapter")
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
        "adapterSha256",
    }
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        expected_fields.update(
            {
                "g0FunnelFragments",
                "g0FunnelProjectionStream",
                "nativeV5Invocation",
            }
        )
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        expected_fields.update(
            {"evolvedPublicationFragments", "nativeV5Invocation"}
        )
    if set(adapter) != expected_fields:
        raise TemporalDiscoveryContractError("native v5 construction adapter shape drifted")
    supplied_adapter_sha256 = _sha256(
        adapter.pop("adapterSha256"), name="native v5 construction adapter"
    )
    if supplied_adapter_sha256 != canonical_sha256(adapter):
        raise TemporalDiscoveryContractError("native v5 construction adapter identity drifted")
    adapter["adapterSha256"] = supplied_adapter_sha256
    expected_schema = (
        V5_EVOLVED_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
        if generation_kind == V5_PROPOSAL_GENERATION_EVOLVED
        else V5_GENERATION_CONSTRUCTION_ADAPTER_SCHEMA
    )
    if (
        adapter["schemaVersion"] != expected_schema
        or adapter["operation"] != V5_PROPOSAL_OPERATION
        or adapter["completed"] is not True
        or adapter["generationKind"] != generation_kind
        or adapter["generationIndex"] != generation_index
    ):
        raise TemporalDiscoveryContractError("native v5 construction adapter binding drifted")
    config_sha256 = _sha256(
        adapter["generationConfigSha256"], name="native v5 generation config"
    )
    if generation_config_sha256 is not None and config_sha256 != generation_config_sha256:
        raise TemporalDiscoveryContractError("native v5 construction generation config drifted")
    for field in (
        "authoritySha256",
        "publicationPlanSha256",
        "publicationRequestSha256",
        "proposalResultSha256",
        "proposalReceiptSha256",
        "outputInventorySha256",
    ):
        _sha256(adapter[field], name=f"native v5 {field}")
    counts: dict[str, int] = {}
    for field in (
        "attemptCount",
        "acceptedCandidateCount",
        "selectedEvaluationCandidateCount",
    ):
        count = adapter[field]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise TemporalDiscoveryContractError(f"native v5 {field} is invalid")
        counts[field] = count
    if (
        counts["acceptedCandidateCount"] > counts["attemptCount"]
        or counts["selectedEvaluationCandidateCount"]
        > counts["acceptedCandidateCount"]
    ):
        raise TemporalDiscoveryContractError("native v5 construction counts drifted")
    for name, relative_path in (
        ("population", "population.json"),
        ("evaluationPopulation", "evaluation-population.json"),
        ("generationJournal", "generation-journal.json"),
        ("identityLedger", "v5-native/identity-ledger.json"),
    ):
        adapter[name] = _native_v5_adapter_artifact(
            adapter=adapter,
            name=name,
            relative_path=relative_path,
            proposal_root=proposal_root,
        )
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        fragment_key = "g0FunnelFragments"
        fragment_schema = V5_G0_FUNNEL_FRAGMENTS_DESCRIPTOR_SCHEMA
        fragment_core_schema = V5_G0_FUNNEL_FRAGMENTS_CORE_SCHEMA
        fragment_label = "G0 funnel fragments"
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        fragment_key = "evolvedPublicationFragments"
        fragment_schema = V5_EVOLVED_PUBLICATION_FRAGMENTS_DESCRIPTOR_SCHEMA
        fragment_core_schema = V5_EVOLVED_PUBLICATION_FRAGMENTS_CORE_SCHEMA
        fragment_label = "evolved publication fragments"
    else:
        raise TemporalDiscoveryContractError("native v5 construction generation kind is invalid")
    if generation_kind in {
        V5_PROPOSAL_GENERATION_G0,
        V5_PROPOSAL_GENERATION_EVOLVED,
    }:
        fragments = _clone(
            adapter[fragment_key],
            name=f"native v5 {fragment_label}",
        )
        if set(fragments) != {
            "schemaVersion",
            "coreSchemaVersion",
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        }:
            raise TemporalDiscoveryContractError(
                f"native v5 {fragment_label} descriptor is malformed"
            )
        semantic_sha256 = _sha256(
            fragments["semanticSha256"],
            name=f"native v5 {fragment_label} semantic identity",
        )
        expected_relative_path = (
            "v5-native/objects/sha256/"
            + semantic_sha256.removeprefix("sha256:")
            + ".json"
        )
        expected_absolute_path = Path(
            os.path.abspath(str(proposal_root / expected_relative_path))
        )
        supplied_absolute_path = str(fragments["absolutePath"])
        byte_length = fragments["byteLength"]
        if (
            fragments["schemaVersion"]
            != fragment_schema
            or fragments["coreSchemaVersion"] != fragment_core_schema
            or fragments["relativePath"] != expected_relative_path
            or supplied_absolute_path
            != str(Path(os.path.abspath(supplied_absolute_path)))
            or Path(supplied_absolute_path) != expected_absolute_path
            or isinstance(byte_length, bool)
            or not isinstance(byte_length, int)
            or byte_length < 0
        ):
            raise TemporalDiscoveryContractError(
                f"native v5 {fragment_label} descriptor drifted"
            )
        adapter[fragment_key] = {
            "schemaVersion": fragment_schema,
            "coreSchemaVersion": fragment_core_schema,
            "relativePath": expected_relative_path,
            "absolutePath": str(expected_absolute_path),
            "semanticSha256": semantic_sha256,
            "fileSha256": _sha256(
                fragments["fileSha256"],
                name=f"native v5 {fragment_label} file identity",
            ),
            "byteLength": byte_length,
        }
        if generation_kind == V5_PROPOSAL_GENERATION_G0:
            adapter["g0FunnelProjectionStream"] = (
                _validate_native_v5_g0_funnel_projection_stream(
                    adapter=adapter,
                    proposal_root=proposal_root,
                )
            )
        adapter["nativeV5Invocation"] = (
            _validate_native_v5_generation_invocation_descriptor(
                adapter=adapter,
                proposal_root=proposal_root,
                generation_kind=generation_kind,
            )
        )
    return adapter


def _build_native_v5_supervisor_invocation(
    *,
    root: Path,
    generation_index: int,
    generation_config: Mapping[str, Any],
    generation_kind: str,
    evaluation_population_size: int,
    parent_archive_input: Mapping[str, Any] | None,
    identity_ledger_input: Mapping[str, Any] | None,
    construction_adapter: Mapping[str, Any],
) -> dict[str, Any]:
    proposal_root = _native_v5_proposal_root(root, generation_index)
    config = _clone(generation_config, name="native v5 generation config")
    config_sha256 = _sha256(config.get("configSha256"), name="native v5 generation config")
    config_material = _clone(config, name="native v5 generation config")
    config_material.pop("configSha256", None)
    if canonical_sha256(config_material) != config_sha256:
        raise TemporalDiscoveryContractError("native v5 generation config identity drifted")
    if config.get("generationIndex") != generation_index:
        raise TemporalDiscoveryContractError("native v5 invocation generation index drifted")
    if (
        isinstance(evaluation_population_size, bool)
        or not isinstance(evaluation_population_size, int)
        or evaluation_population_size < 1
    ):
        raise TemporalDiscoveryContractError("native v5 evaluation population size is invalid")
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if parent_archive_input is not None or identity_ledger_input is not None:
            raise TemporalDiscoveryContractError("native v5 G0 invocation has legacy inputs")
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        if not isinstance(parent_archive_input, Mapping) or not isinstance(
            identity_ledger_input, Mapping
        ):
            raise TemporalDiscoveryContractError("native v5 evolved invocation lacks inputs")
        parent_archive_input = validate_v5_proposal_input_binding(
            parent_archive_input, expected_kind="parentArchive"
        )
        identity_ledger_input = validate_v5_proposal_input_binding(
            identity_ledger_input, expected_kind="identityLedger"
        )
    else:
        raise TemporalDiscoveryContractError("native v5 invocation generation kind is invalid")
    adapter = _validate_native_v5_construction_adapter(
        value=construction_adapter,
        proposal_root=proposal_root,
        generation_index=generation_index,
        generation_kind=generation_kind,
        generation_config_sha256=config_sha256,
    )
    if adapter["selectedEvaluationCandidateCount"] != evaluation_population_size:
        raise TemporalDiscoveryContractError(
            "native v5 construction evaluation count drifted"
        )
    invocation = {
        "schemaVersion": NATIVE_V5_SUPERVISOR_INVOCATION_SCHEMA,
        "generationIndex": generation_index,
        "generationKind": generation_kind,
        "outputRoot": str(Path(os.path.abspath(str(proposal_root)))),
        "generationConfig": config,
        "generationConfigSha256": config_sha256,
        "evaluationPopulationSize": evaluation_population_size,
        "parentArchiveInput": _clone(
            parent_archive_input, name="native v5 parent archive input"
        )
        if parent_archive_input is not None
        else None,
        "identityLedgerInput": _clone(
            identity_ledger_input, name="native v5 identity-ledger input"
        )
        if identity_ledger_input is not None
        else None,
        "constructionAdapter": adapter,
    }
    invocation["invocationSha256"] = canonical_sha256(invocation)
    return invocation


def _validate_native_v5_supervisor_invocation(
    *, root: Path, value: Mapping[str, Any]
) -> dict[str, Any]:
    invocation = _clone(value, name="native v5 supervisor invocation")
    expected_fields = {
        "schemaVersion",
        "generationIndex",
        "generationKind",
        "outputRoot",
        "generationConfig",
        "generationConfigSha256",
        "evaluationPopulationSize",
        "parentArchiveInput",
        "identityLedgerInput",
        "constructionAdapter",
        "invocationSha256",
    }
    if set(invocation) != expected_fields:
        raise TemporalDiscoveryContractError("native v5 supervisor invocation shape drifted")
    supplied_sha256 = _sha256(
        invocation.pop("invocationSha256"), name="native v5 supervisor invocation"
    )
    if supplied_sha256 != canonical_sha256(invocation):
        raise TemporalDiscoveryContractError("native v5 supervisor invocation identity drifted")
    invocation["invocationSha256"] = supplied_sha256
    generation_index = invocation["generationIndex"]
    if (
        invocation["schemaVersion"] != NATIVE_V5_SUPERVISOR_INVOCATION_SCHEMA
        or isinstance(generation_index, bool)
        or not isinstance(generation_index, int)
        or generation_index < 1
    ):
        raise TemporalDiscoveryContractError("native v5 supervisor invocation is invalid")
    proposal_root = _native_v5_proposal_root(root, generation_index)
    if invocation["outputRoot"] != str(Path(os.path.abspath(str(proposal_root)))):
        raise TemporalDiscoveryContractError("native v5 supervisor invocation output root drifted")
    config = invocation["generationConfig"]
    if not isinstance(config, Mapping):
        raise TemporalDiscoveryContractError("native v5 supervisor generation config is invalid")
    config_sha256 = _sha256(
        invocation["generationConfigSha256"], name="native v5 supervisor config"
    )
    if config.get("configSha256") != config_sha256:
        raise TemporalDiscoveryContractError("native v5 supervisor generation config drifted")
    config_material = _clone(config, name="native v5 supervisor generation config")
    config_material.pop("configSha256", None)
    if (
        canonical_sha256(config_material) != config_sha256
        or config.get("generationIndex") != generation_index
    ):
        raise TemporalDiscoveryContractError("native v5 supervisor generation config identity drifted")
    evaluation_population_size = invocation["evaluationPopulationSize"]
    if (
        isinstance(evaluation_population_size, bool)
        or not isinstance(evaluation_population_size, int)
        or evaluation_population_size < 1
    ):
        raise TemporalDiscoveryContractError("native v5 supervisor evaluation size is invalid")
    generation_kind = invocation["generationKind"]
    parent_archive_input = invocation["parentArchiveInput"]
    identity_ledger_input = invocation["identityLedgerInput"]
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if parent_archive_input is not None or identity_ledger_input is not None:
            raise TemporalDiscoveryContractError("native v5 G0 invocation input drifted")
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        if not isinstance(parent_archive_input, Mapping) or not isinstance(
            identity_ledger_input, Mapping
        ):
            raise TemporalDiscoveryContractError("native v5 evolved invocation input drifted")
        invocation["parentArchiveInput"] = validate_v5_proposal_input_binding(
            parent_archive_input, expected_kind="parentArchive"
        )
        invocation["identityLedgerInput"] = validate_v5_proposal_input_binding(
            identity_ledger_input, expected_kind="identityLedger"
        )
    else:
        raise TemporalDiscoveryContractError("native v5 supervisor generation kind is invalid")
    invocation["constructionAdapter"] = _validate_native_v5_construction_adapter(
        value=invocation["constructionAdapter"],
        proposal_root=proposal_root,
        generation_index=generation_index,
        generation_kind=generation_kind,
        generation_config_sha256=config_sha256,
    )
    if (
        invocation["constructionAdapter"]["selectedEvaluationCandidateCount"]
        != evaluation_population_size
    ):
        raise TemporalDiscoveryContractError(
            "native v5 supervisor evaluation count drifted"
        )
    return invocation


def _reauthenticate_native_v5_supervisor_invocation(
    *, root: Path, config: Mapping[str, Any], invocation: Mapping[str, Any]
) -> dict[str, Any]:
    """Force the bridge's receipt-last adoption pass on every completed reopen."""

    checked = _validate_native_v5_supervisor_invocation(root=root, value=invocation)
    runtime, _bindings = _validate_native_v5_proposal_runtime(config=config)
    source = config.get("bidirectionalPairSourceAuthority")
    evolvable = _v5_evolvable_authority(config)
    if not isinstance(source, Mapping) or evolvable is None:
        raise TemporalDiscoveryContractError("native v5 invocation lacks frozen authority")
    qd_engine_version = _native_v5_qd_engine_version(
        config=config, evolvable_authority=evolvable
    )
    try:
        adapter = run_native_v5_generation_construction(
            output_root=checked["outputRoot"],
            generation_config=checked["generationConfig"],
            pair_source_authority=source,
            evolvable_module_authority=evolvable,
            bidirectional_pair_policy=runtime["bidirectionalPairPolicy"],
            native_operator_authority=runtime["nativeOperatorAuthority"],
            qd_engine_version=qd_engine_version,
            evaluation_population_size=checked["evaluationPopulationSize"],
            execution_timeout_seconds=runtime["executionTimeoutSeconds"],
            thread_cap=runtime["threadCap"],
            generation_kind=checked["generationKind"],
            parent_archive_input=checked["parentArchiveInput"],
            identity_ledger_input=checked["identityLedgerInput"],
        )
    except TemporalQDV5NativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    reopened = _validate_native_v5_construction_adapter(
        value=adapter,
        proposal_root=_native_v5_proposal_root(root, checked["generationIndex"]),
        generation_index=checked["generationIndex"],
        generation_kind=checked["generationKind"],
        generation_config_sha256=checked["generationConfigSha256"],
    )
    if reopened != checked["constructionAdapter"]:
        raise TemporalDiscoveryContractError(
            "native v5 receipt adoption disagrees with the completed generation"
        )
    return reopened


def _native_v5_proposal_archive_descriptor(
    value: Mapping[str, Any], *, name: str
) -> dict[str, Any]:
    """Accept the sole four-field archive transport used by qd-batch.

    Current-v5 Python never reopens an archive to make this binding.  Fresh
    G0 receives the reducer-certified initial descriptor and later generations
    receive the exact descriptor projected from the prior Rust finalizer
    commit.  Keeping the projection separate from the general supervisor
    archive records prevents an accidental path/hash fallback from creeping
    into proposal construction.
    """

    descriptor = _clone(value, name=name)
    if not isinstance(descriptor, Mapping) or set(descriptor) != {
        "absolutePath",
        "fileSha256",
        "semanticSha256",
        "byteLength",
    }:
        raise TemporalDiscoveryContractError(f"{name} descriptor schema drifted")
    path = descriptor.get("absolutePath")
    if not isinstance(path, str) or not Path(path).is_absolute():
        raise TemporalDiscoveryContractError(f"{name} path is invalid")
    _sha256(descriptor.get("fileSha256"), name=f"{name} file identity")
    _sha256(descriptor.get("semanticSha256"), name=f"{name} semantic identity")
    byte_length = descriptor.get("byteLength")
    if isinstance(byte_length, bool) or not isinstance(byte_length, int) or byte_length < 0:
        raise TemporalDiscoveryContractError(f"{name} byte length is invalid")
    return dict(descriptor)


def _native_v5_prefinalizer_archive_binding(
    value: Mapping[str, Any], *, name: str
) -> dict[str, Any]:
    """Project one finalizer/certifier archive descriptor for prefinalizer v2.

    The target ABI names its transport fields differently from qd-batch.  This
    is a name-only projection of an authenticated compact descriptor, never a
    file lookup or rehash.
    """

    descriptor = _native_v5_proposal_archive_descriptor(value, name=name)
    return {
        "path": descriptor["absolutePath"],
        "rawSha256": descriptor["fileSha256"],
        "sizeBytes": descriptor["byteLength"],
        "archiveSha256": descriptor["semanticSha256"],
    }


def _native_v5_archive_policy_binding(value: Mapping[str, Any]) -> dict[str, Any]:
    """Seal the frozen evolvable policy in the prefinalizer's exact ABI.

    The launch authority stores the policy body and its semantic policy hash.
    The Rust prefinalizer/finalizer additionally require a self-hashed binding
    wrapper.  This is a bounded control-object projection; it does not inspect
    any candidate, archive, or result payload.
    """

    policy = _clone(value, name="native v5 archive policy authority")
    if not isinstance(policy, Mapping) or set(policy) != {
        "qdVersion",
        "policyName",
        "policySha256",
        "frozenPolicy",
    }:
        raise TemporalDiscoveryContractError(
            "native v5 archive policy authority schema drifted"
        )
    _sha256(policy.get("policySha256"), name="native v5 archive policy identity")
    for field in ("qdVersion", "policyName"):
        if not isinstance(policy.get(field), str) or not policy[field]:
            raise TemporalDiscoveryContractError(
                f"native v5 archive policy {field} is invalid"
            )
    if not isinstance(policy.get("frozenPolicy"), Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 frozen archive policy is invalid"
        )
    return _native_self_hash(
        {
            "schemaVersion": "temporal_qd_archive_policy_binding_v1",
            "qdVersion": policy["qdVersion"],
            "policyName": policy["policyName"],
            "policySha256": policy["policySha256"],
            "frozenPolicy": policy["frozenPolicy"],
        },
        "policyBindingSha256",
    )


def _native_v5_archive_descriptor_from_finalizer_artifact(
    value: Mapping[str, Any], *, name: str, expected_relative_path: str
) -> dict[str, Any]:
    """Project a finalizer artifact descriptor for the next v5 transaction."""

    artifact = _clone(value, name=name)
    if not isinstance(artifact, Mapping) or set(artifact) != {
        "relativePath",
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    } or artifact.get("relativePath") != expected_relative_path:
        raise TemporalDiscoveryContractError(f"{name} descriptor schema drifted")
    return _native_v5_proposal_archive_descriptor(
        {
            "absolutePath": artifact["absolutePath"],
            "fileSha256": artifact["fileSha256"],
            "semanticSha256": artifact["semanticSha256"],
            "byteLength": artifact["byteLength"],
        },
        name=name,
    )


def _run_native_v5_generation(
    *,
    root: Path,
    config: Mapping[str, Any],
    generation_index: int,
    parent_archive_descriptor: Mapping[str, Any],
    parent_schedule: Mapping[str, Any] | None,
    identity_ledger_input: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Construct one generation solely through the receipt-authenticated Rust API."""

    runtime, bindings = _validate_native_v5_proposal_runtime(config=config)
    source = config.get("bidirectionalPairSourceAuthority")
    evolvable = _v5_evolvable_authority(config)
    if not isinstance(source, Mapping) or evolvable is None:
        raise TemporalDiscoveryContractError("native v5 construction lacks frozen authority")
    qd_engine_version = _native_v5_qd_engine_version(
        config=config, evolvable_authority=evolvable
    )
    g0 = config.get("g0Bootstrap")
    is_g0 = isinstance(g0, Mapping) and generation_index == 1
    if is_g0:
        generation_kind = V5_PROPOSAL_GENERATION_G0
        construction_width = int(g0["initialConstructionPoolSize"])
        evaluation_width = int(g0["evaluationPopulationSize"])
        parent_input = None
        if identity_ledger_input is not None:
            raise TemporalDiscoveryContractError("native v5 G0 cannot bind an identity ledger")
        generation_parent_schedule = None
    else:
        generation_kind = V5_PROPOSAL_GENERATION_EVOLVED
        construction_width = int(config["frozenSearchPolicy"]["targetUniqueCandidates"])
        evaluation_width = construction_width
        if not isinstance(identity_ledger_input, Mapping):
            raise TemporalDiscoveryContractError(
                "native v5 evolved generation lacks its frozen identity ledger"
            )
        try:
            parent_input = build_v5_proposal_input_binding(
                kind="parentArchive",
                sealed_descriptor=_native_v5_proposal_archive_descriptor(
                    parent_archive_descriptor,
                    name="native v5 evolved parent archive",
                ),
            )
        except TemporalQDV5NativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
        generation_parent_schedule = parent_schedule
    immigrant_policy = source.get("immigrantConstructionPolicy")
    if not isinstance(immigrant_policy, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 source authority lacks immutable immigrant construction policy"
        )
    generation_config = build_pair_generation_config(
        generation_index=generation_index,
        target_unique_candidates=construction_width,
        max_proposal_attempts=int(config["frozenSearchPolicy"]["maxProposalAttempts"]),
        run_config=bindings["runConfig"],
        pair_policy=runtime["bidirectionalPairPolicy"],
        operator_implementation_identity=bindings["operatorImplementation"],
        # Parent parsing and selection belongs to Rust for v5.  The frozen
        # schedule is control-plane material only; the parent archive itself
        # is bound as an opaque, receipt-validated native input below.
        parent_archive=None,
        immigrant_construction_policy=immigrant_policy,
        global_identity_ledger_enabled=(
            generation_kind == V5_PROPOSAL_GENERATION_EVOLVED
        ),
        parent_schedule=generation_parent_schedule,
    )
    proposal_root = _native_v5_proposal_root(root, generation_index)
    try:
        adapter = run_native_v5_generation_construction(
            output_root=proposal_root,
            generation_config=generation_config,
            pair_source_authority=source,
            evolvable_module_authority=evolvable,
            bidirectional_pair_policy=runtime["bidirectionalPairPolicy"],
            native_operator_authority=runtime["nativeOperatorAuthority"],
            qd_engine_version=qd_engine_version,
            evaluation_population_size=evaluation_width,
            execution_timeout_seconds=runtime["executionTimeoutSeconds"],
            thread_cap=runtime["threadCap"],
            generation_kind=generation_kind,
            parent_archive_input=parent_input,
            identity_ledger_input=identity_ledger_input,
        )
    except TemporalQDV5NativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    checked_adapter = _validate_native_v5_construction_adapter(
        value=adapter,
        proposal_root=proposal_root,
        generation_index=generation_index,
        generation_kind=generation_kind,
        generation_config_sha256=generation_config["configSha256"],
    )
    if (
        checked_adapter["selectedEvaluationCandidateCount"] != evaluation_width
        or checked_adapter["acceptedCandidateCount"] < evaluation_width
        or (is_g0 and checked_adapter["acceptedCandidateCount"] != construction_width)
    ):
        raise TemporalDiscoveryContractError(
            "native v5 construction receipt count disagrees with frozen dimensions"
        )
    invocation = _build_native_v5_supervisor_invocation(
        root=root,
        generation_index=generation_index,
        generation_config=generation_config,
        generation_kind=generation_kind,
        evaluation_population_size=evaluation_width,
        parent_archive_input=parent_input,
        identity_ledger_input=identity_ledger_input,
        construction_adapter=checked_adapter,
    )
    return {
        "completed": True,
        "generationKind": generation_kind,
        "populationSha256": checked_adapter["population"]["semanticSha256"],
        "evaluationPopulationSha256": checked_adapter["evaluationPopulation"][
            "semanticSha256"
        ],
        "journalSha256": checked_adapter["generationJournal"]["semanticSha256"],
        "proposalCount": checked_adapter["attemptCount"],
        "candidateCount": checked_adapter["selectedEvaluationCandidateCount"],
        "acceptedCandidateCount": checked_adapter["acceptedCandidateCount"],
        # These supervisor counters are intentionally a compact projection of
        # the receipt fields, not a reconstructed proposal journal.  Keeping
        # their v5 names makes it impossible to mistake them for legacy
        # per-origin Python accounting.
        "originProposalCounts": {
            "nativeV5AttemptCount": checked_adapter["attemptCount"]
        },
        "originAcceptedCounts": {
            "nativeV5AcceptedCandidateCount": checked_adapter[
                "acceptedCandidateCount"
            ]
        },
        "proposalSlots": {
            "targetUniqueCandidates": evaluation_width,
            "acceptedUniqueCandidates": checked_adapter[
                "selectedEvaluationCandidateCount"
            ],
            "proposalAttempts": checked_adapter["attemptCount"],
            "remainingUniqueCandidateSlots": 0,
        },
        "uniqueIdentityCounts": {
            "nativeV5SelectedEvaluationCandidateCount": checked_adapter[
                "selectedEvaluationCandidateCount"
            ]
        },
        "duplicateCounters": {},
        "proposalSlotCounters": {
            "nativeV5AttemptCount": checked_adapter["attemptCount"],
            "nativeV5AcceptedCandidateCount": checked_adapter[
                "acceptedCandidateCount"
            ],
            "nativeV5SelectedEvaluationCandidateCount": checked_adapter[
                "selectedEvaluationCandidateCount"
            ],
        },
        # Parent scheduling lives inside the immutable native generation
        # config.  The legacy continuation cursor has no v5 proposal role.
        "nextImmigrantContinuationOrdinal": 0,
        "nativeV5Construction": checked_adapter,
        "nativeV5Invocation": invocation,
    }


def _native_v5_campaign_timeout(value: object, *, name: str) -> int:
    """Accept the frozen integral timeout without silently rounding it."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TemporalDiscoveryContractError(f"native v5 {name} is invalid")
    timeout = int(value)
    if timeout < 1 or timeout != value:
        raise TemporalDiscoveryContractError(f"native v5 {name} is invalid")
    return timeout


def _run_native_v5_campaign_round(
    *,
    runtime_authority: Mapping[str, Any],
    config: Mapping[str, Any],
    generation_index: int,
    evaluation_population_path: Path,
    evaluation_population_raw_sha256: str,
    campaign_root: Path,
    panel: Mapping[str, Any],
    campaign_role: str,
    cohort_source: Mapping[str, Any],
    gateway_token: str | None,
    cohort_selection_path: Path | None = None,
    timeout_seconds: int = 900,
) -> dict[str, Any]:
    """Run one current-v5 campaign as sealed Rust-to-Rust handoffs only.

    This is deliberately a control-plane join.  It forwards an opaque native
    evaluation population or native selection descriptor to the freezer and
    carries only its compact receipts into dispatch, seal, sidecar, and the
    rotating campaign receipt.  No candidate, task, result, or archive row is
    decoded here.
    """

    if not _native_v5_proposal_enabled(config):
        raise TemporalDiscoveryContractError(
            "native v5 campaign round requires the current v5 proposal runtime"
        )
    if isinstance(generation_index, bool) or not isinstance(generation_index, int) or generation_index < 1:
        raise TemporalDiscoveryContractError("native v5 campaign generation is invalid")
    rotating = config.get("rotatingEvidence")
    if not isinstance(rotating, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 campaign round requires frozen rotating evidence"
        )
    current_panel = panel_for_generation(rotating, generation_index)
    if campaign_role in {
        "proposal_current_panel",
        "retained_parent_current_panel",
    }:
        expected_panel = current_panel
        if dict(panel) != expected_panel:
            raise TemporalDiscoveryContractError(
                "native v5 current-panel campaign drifted from frozen absolute generation mapping"
            )
    elif campaign_role == "prior_panel_backfill":
        panels = rotating.get("panels")
        panel_id_value = panel.get("panelId") if isinstance(panel, Mapping) else None
        expected_panel = next(
            (
                dict(item)
                for item in panels or []
                if isinstance(item, Mapping) and item.get("panelId") == panel_id_value
            ),
            None,
        )
        if expected_panel is None or dict(panel) != expected_panel:
            raise TemporalDiscoveryContractError(
                "native v5 prior-panel backfill names an unbound frozen panel"
            )
    else:
        raise TemporalDiscoveryContractError("native v5 campaign role is invalid")
    panel_id = expected_panel.get("panelId")
    if not isinstance(panel_id, str) or not panel_id:
        raise TemporalDiscoveryContractError("native v5 campaign panel is invalid")
    templates = rotating.get("panelTemplates")
    template = templates.get(panel_id) if isinstance(templates, Mapping) else None
    if (
        not isinstance(template, Mapping)
        or not isinstance(template.get("path"), str)
        or not isinstance(template.get("preparationSha256"), str)
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign lacks its frozen panel template"
        )
    construction = config.get("constructionOperatorPolicy")
    catalog = (
        construction.get("catalog", {}).get("path")
        if isinstance(construction, Mapping)
        and isinstance(construction.get("catalog"), Mapping)
        else None
    )
    if not isinstance(catalog, str) or not catalog:
        raise TemporalDiscoveryContractError(
            "native v5 campaign requires a frozen construction catalog"
        )
    catalog_binding = construction.get("catalog") if isinstance(construction, Mapping) else None
    if not isinstance(catalog_binding, Mapping) or not isinstance(
        catalog_binding.get("catalogSha256"), str
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign construction catalog lacks its frozen identity"
        )
    evaluation_raw_sha256 = _sha256(
        evaluation_population_raw_sha256,
        name="native v5 campaign evaluation population identity",
    )
    template_sha256 = _sha256(
        template["preparationSha256"],
        name="native v5 campaign template preparation identity",
    )
    catalog_sha256 = _sha256(
        catalog_binding["catalogSha256"],
        name="native v5 campaign construction catalog identity",
    )
    repositories = config.get("repositories")
    evaluation = config.get("evaluation")
    policy = config.get("frozenSearchPolicy")
    evolvable = _v5_evolvable_authority(config)
    if (
        not isinstance(repositories, Mapping)
        or not isinstance(evaluation, Mapping)
        or not isinstance(policy, Mapping)
        or evolvable is None
        or not isinstance(evolvable.get("archivePolicyAuthority"), Mapping)
        or not isinstance(evaluation.get("behaviorAttributionRequirement"), Mapping)
        or not isinstance(evaluation.get("gatewayUrl"), str)
        or not evaluation["gatewayUrl"]
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign lacks frozen execution authority"
        )
    execution_engine_commit = repositories.get("executionEngineCommit")
    worker_contract_sha256 = config.get("workerContractSha256")
    if not isinstance(execution_engine_commit, str) or not isinstance(
        worker_contract_sha256, str
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign frozen repository/worker binding is invalid"
        )
    numeric_names = {
        "minimumTotalTrades": "minimum total trades",
        "minimumTradesPerWindow": "minimum trades per window",
        "capTrades": "cap trades",
    }
    numeric: dict[str, int] = {}
    for key, label in numeric_names.items():
        value = policy.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TemporalDiscoveryContractError(f"native v5 {label} is invalid")
        numeric[key] = value
    provisional = rotating.get("provisionalReduction")
    provisional_limit = (
        provisional.get("maxCandidates") if isinstance(provisional, Mapping) else None
    )
    if (
        isinstance(provisional_limit, bool)
        or not isinstance(provisional_limit, int)
        or provisional_limit < 1
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign provisional limit is invalid"
        )
    if campaign_role == "proposal_current_panel":
        if cohort_selection_path is not None:
            raise TemporalDiscoveryContractError(
                "native v5 proposal campaign cannot supply a cohort selection"
            )
    elif campaign_role in {
        "retained_parent_current_panel",
        "prior_panel_backfill",
    }:
        if cohort_selection_path is None:
            raise TemporalDiscoveryContractError(
                "native v5 resumed campaign lacks its Rust cohort selection"
            )
    else:
        raise TemporalDiscoveryContractError("native v5 campaign role is invalid")

    # Each subprocess receives a directory chosen by the immutable generation
    # layout.  `exist_ok` only permits restart of the same receipt-last
    # transaction; all control documents below remain write-once.
    campaign_root.mkdir(parents=True, exist_ok=True)
    source_root = campaign_root / "campaign-seal-source"
    source_root.mkdir(parents=True, exist_ok=True)
    sidecar_root = campaign_root / "panel-bundle-sidecar"
    sidecar_root.mkdir(parents=True, exist_ok=True)
    receipt_root = campaign_root / "rotating-campaign-receipt"
    receipt_root.mkdir(parents=True, exist_ok=True)
    # The freezer seals its own empty screening-run checkpoint.  Gateway
    # completion compacts a mutable checkpoint, so it must publish beneath a
    # distinct operational root rather than mutate the freezer's receipt
    # inventory and make every restart fail closed.
    gateway_root = campaign_root / "gateway-dispatch"
    gateway_receipt = gateway_root / ".native-gateway-dispatch" / "execution-receipt.json"
    try:
        freeze = run_native_v5_campaign_freeze(
            runtime_authority=runtime_authority,
            evaluation_population_path=evaluation_population_path,
            evaluation_population_raw_sha256=evaluation_raw_sha256,
            template_preparation_path=Path(str(template["path"])),
            template_preparation_sha256=template_sha256,
            construction_catalog_path=Path(catalog),
            construction_catalog_sha256=catalog_sha256,
            output_root=campaign_root,
            execution_engine_commit=execution_engine_commit,
            worker_contract_sha256=worker_contract_sha256,
            rotating_evidence=rotating,
            archive_policy_authority=evolvable["archivePolicyAuthority"],
            behavior_attribution_requirement=evaluation[
                "behaviorAttributionRequirement"
            ],
            campaign_role=campaign_role,
            panel_id=panel_id,
            cohort_selection_path=cohort_selection_path,
            timeout_seconds=timeout_seconds,
        )
        gateway = run_native_gateway_dispatch(
            runtime_authority=runtime_authority,
            task_manifest_path=campaign_root / "screening-run" / "task-manifest.json",
            output_root=gateway_root,
            gateway_url=str(evaluation["gatewayUrl"]),
            mode="resume" if gateway_receipt.exists() else "fresh",
            timeout_seconds=_native_v5_campaign_timeout(
                evaluation.get("timeoutSecondsPerGeneration"),
                name="gateway timeout",
            ),
            gateway_token=gateway_token,
            enqueue_batch_size=_native_v5_campaign_timeout(
                evaluation.get("enqueueBatchSize"), name="enqueue batch size"
            ),
        )
        source = build_native_campaign_seal_source(
            runtime_authority=runtime_authority,
            freezer_root=Path(str(freeze["outputRoot"])),
            gateway_output_root=Path(str(gateway["outputRoot"])),
            source_root=source_root,
            funnel_projection_included=True,
            timeout_seconds=timeout_seconds,
        )
        seal = run_native_campaign_seal(
            runtime_authority=runtime_authority,
            source_build=source,
            # The native freezer owns the role-specific cohort population.
            # In particular, retained/backfill rows may not exist in the
            # proposal evaluation population.  The tail reducer reopens this
            # exact receipt-inventoried artifact; Python never decodes it.
            evaluation_population_path=(
                Path(str(freeze["outputRoot"])) / "cohort-population.json"
            ),
            evaluation_population_sha256=str(freeze["cohortPopulationSha256"]),
            output_root=campaign_root,
            generation_index=generation_index,
            minimum_total_trades=numeric["minimumTotalTrades"],
            minimum_trades_per_window=numeric["minimumTradesPerWindow"],
            cap_trades=numeric["capTrades"],
            provisional_limit=provisional_limit,
            timeout_seconds=timeout_seconds,
        )
        panel_input = {
            "schemaVersion": "temporal_qd_v5_rotating_panel_bundle_input_v2",
            "contractVersion": "temporal_qd_native_foundation_v1",
            "generationIndex": generation_index,
            "campaignRole": campaign_role,
            "campaignSeal": seal["campaignSeal"],
            "tailAuthority": seal["tailAuthorityReceipt"],
            "tailResultIndex": seal["tailResultIndex"],
            "directionalTailAuthority": seal["directionalTailAuthority"],
            "rotatingEvidence": dict(rotating),
            "panel": expected_panel,
        }
        sidecar = build_native_panel_bundle_sidecar(
            runtime_authority=runtime_authority,
            panel_input=panel_input,
            output_root=sidecar_root,
            timeout_seconds=timeout_seconds,
        )
        receipt = build_native_rotating_campaign_receipt(
            runtime_authority=runtime_authority,
            campaign_freeze=freeze,
            campaign_seal=seal,
            panel_bundle_sidecar=sidecar,
            output_root=receipt_root,
            generation_index=generation_index,
            campaign_role=campaign_role,
            panel_id=panel_id,
            rotating_evidence_sha256=str(rotating.get("rotatingEvidenceSha256") or ""),
            cohort_source=cohort_source,
            timeout_seconds=timeout_seconds,
        )
    except TemporalQDV5ControlPlaneError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    return {
        "campaignFreeze": freeze,
        "gatewayDispatch": gateway,
        "sourceBuild": source,
        "campaignSeal": seal,
        "panelBundleSidecar": sidecar,
        "campaignReceipt": receipt,
    }


def _native_v5_panel_by_id(
    *, rotating_evidence: Mapping[str, Any], panel_id: str
) -> dict[str, Any]:
    """Return one sealed panel authority without selecting any candidates."""

    if not isinstance(panel_id, str) or not panel_id:
        raise TemporalDiscoveryContractError("native v5 panel identity is invalid")
    panels = rotating_evidence.get("panels")
    if not isinstance(panels, list):
        raise TemporalDiscoveryContractError("native v5 rotating panels are unavailable")
    matches = [
        _clone(item, name="native v5 rotating panel")
        for item in panels
        if isinstance(item, Mapping) and item.get("panelId") == panel_id
    ]
    if len(matches) != 1:
        raise TemporalDiscoveryContractError("native v5 rotating panel binding drifted")
    return matches[0]


def _native_v5_proposal_state_authority_for_generation(
    *,
    root: Path,
    generation_index: int,
    generation_result: Mapping[str, Any],
    identity_ledger_input: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Project the eight-field Rust proposal authority from one sealed adapter.

    This is deliberately a compact root-to-root join.  It neither reads a
    proposal population nor reconstructs a journal; the adapter was already
    receipt-authenticated by ``_run_native_v5_generation``.
    """

    kind = generation_result.get("generationKind")
    if kind not in {V5_PROPOSAL_GENERATION_G0, V5_PROPOSAL_GENERATION_EVOLVED}:
        raise TemporalDiscoveryContractError("native v5 generation kind is invalid")
    adapter = _validate_native_v5_construction_adapter(
        value=_clone(
            generation_result.get("nativeV5Construction"),
            name="native v5 proposal construction adapter",
        ),
        proposal_root=_native_v5_proposal_root(root, generation_index),
        generation_index=generation_index,
        generation_kind=kind,
    )
    native_invocation = adapter.get("nativeV5Invocation")
    if not isinstance(native_invocation, Mapping) or not isinstance(
        native_invocation.get("proposalManifest"), Mapping
    ):
        raise TemporalDiscoveryContractError(
            "native v5 construction lacks its immutable invocation descriptor"
        )
    supervisor_invocation = generation_result.get("nativeV5Invocation")
    if not isinstance(supervisor_invocation, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 generation lacks its supervisor invocation"
        )
    if kind == V5_PROPOSAL_GENERATION_G0:
        if identity_ledger_input is not None or supervisor_invocation.get(
            "identityLedgerInput"
        ) is not None:
            raise TemporalDiscoveryContractError(
                "native v5 G0 proposal unexpectedly has an input identity ledger"
            )
        input_ledger_sha256: str | None = None
    else:
        supplied = supervisor_invocation.get("identityLedgerInput")
        if not isinstance(identity_ledger_input, Mapping) or not isinstance(
            supplied, Mapping
        ):
            raise TemporalDiscoveryContractError(
                "native v5 evolved proposal lacks its sealed input identity ledger"
            )
        try:
            checked_input = validate_v5_proposal_input_binding(
                identity_ledger_input, expected_kind="identityLedger"
            )
            checked_supplied = validate_v5_proposal_input_binding(
                supplied, expected_kind="identityLedger"
            )
        except TemporalQDV5NativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
        if checked_input != checked_supplied:
            raise TemporalDiscoveryContractError(
                "native v5 evolved input identity-ledger invocation drifted"
            )
        input_ledger_sha256 = _sha256(
            checked_input["semanticSha256"],
            name="native v5 evolved input identity-ledger identity",
        )
    proposal_manifest = native_invocation["proposalManifest"]
    ledger = adapter.get("identityLedger")
    journal = adapter.get("generationJournal")
    if not isinstance(ledger, Mapping) or not isinstance(journal, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 construction lacks compact ledger/journal descriptors"
        )
    proposal_manifest_sha256 = _sha256(
        proposal_manifest.get("semanticSha256"),
        name="native v5 proposal manifest identity",
    )
    proposal_receipt_sha256 = _sha256(
        adapter.get("proposalReceiptSha256"), name="native v5 proposal receipt identity"
    )
    generation_journal_sha256 = _sha256(
        journal.get("semanticSha256"), name="native v5 proposal journal identity"
    )
    output_ledger_sha256 = _sha256(
        ledger.get("semanticSha256"), name="native v5 proposal output ledger identity"
    )
    output_ledger_file_sha256 = _sha256(
        ledger.get("fileSha256"), name="native v5 proposal output ledger file identity"
    )
    if native_invocation.get("proposalReceiptSha256") != proposal_receipt_sha256:
        raise TemporalDiscoveryContractError(
            "native v5 proposal invocation receipt identity drifted"
        )
    authority = {
        "generationKind": kind,
        "proposalManifestSha256": proposal_manifest_sha256,
        "proposalReceiptSha256": proposal_receipt_sha256,
        "generationJournalSha256": generation_journal_sha256,
        "inputIdentityLedgerSha256": input_ledger_sha256,
        "outputIdentityLedgerRelativePath": "proposal/v5-native/identity-ledger.json",
        "outputIdentityLedgerSha256": output_ledger_sha256,
        "outputIdentityLedgerFileSha256": output_ledger_file_sha256,
    }
    semantic_roots = {
        "proposalReceiptSha256": proposal_receipt_sha256,
        "generationJournalSha256": generation_journal_sha256,
    }
    return adapter, authority, semantic_roots


def _complete_native_v5_generation(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    config: Mapping[str, Any],
    generation_index: int,
    generation_result: Mapping[str, Any],
    identity_ledger_input: Mapping[str, Any] | None,
    parent_archive_descriptor: Mapping[str, Any],
    previous_cumulative_archive_descriptor: Mapping[str, Any] | None,
    gateway_token: str | None,
) -> dict[str, Any]:
    """Run one entire current-v5 postproposal transaction through Rust.

    Python only writes/reopens compact manifests and receipts.  Candidate
    construction, campaign task enumeration, gateway result reduction, rich
    member selection, archive materialization, and finalization all remain in
    their pinned Rust transactions.
    """

    if not _native_v5_proposal_enabled(config):
        raise TemporalDiscoveryContractError(
            "native v5 postproposal completion requires the current v5 runtime"
        )
    rotating = config.get("rotatingEvidence")
    evolvable = _v5_evolvable_authority(config)
    policy = config.get("frozenSearchPolicy")
    if (
        not isinstance(rotating, Mapping)
        or evolvable is None
        or not isinstance(evolvable.get("archivePolicyAuthority"), Mapping)
        or not isinstance(policy, Mapping)
    ):
        raise TemporalDiscoveryContractError(
            "current native v5 postproposal requires frozen rotating authorities"
        )
    runtime_authority = _native_runtime_authority_for_generation(
        root=root, generation_index=generation_index
    )
    _require_native_v5_control_plane_runtime_authority(runtime_authority)
    adapter, proposal_state_authority, proposal_semantic_roots = (
        _native_v5_proposal_state_authority_for_generation(
            root=root,
            generation_index=generation_index,
            generation_result=generation_result,
            identity_ledger_input=identity_ledger_input,
        )
    )
    generation_kind = proposal_state_authority["generationKind"]
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    current_panel = panel_for_generation(rotating, generation_index)
    evaluation_population_path = Path(
        str(adapter["evaluationPopulation"]["absolutePath"])
    )
    # The adapter's receipt-authenticated descriptor is the sole Python
    # authority for this candidate-scale population.  The freezer reopens it
    # through Rust; probing it here would turn restart admission into a Python
    # file-validation path.
    state["stage"] = "native_v5_proposal_campaign"
    _save_state(state_path, state)
    proposal_campaign = _run_native_v5_campaign_round(
        runtime_authority=runtime_authority,
        config=config,
        generation_index=generation_index,
        evaluation_population_path=evaluation_population_path,
        evaluation_population_raw_sha256=str(
            adapter["evaluationPopulation"]["fileSha256"]
        ),
        campaign_root=(generation_root / "campaign" / "proposal-current-panel"),
        panel=current_panel,
        campaign_role="proposal_current_panel",
        cohort_source={
            "kind": "proposal_evaluation_population",
            "sourceSemanticSha256": adapter["evaluationPopulation"]["semanticSha256"],
            "candidateCount": adapter["selectedEvaluationCandidateCount"],
            "selectionSha256": None,
        },
        gateway_token=gateway_token,
    )

    state["stage"] = "native_v5_funnel_reduction"
    _save_state(state_path, state)
    extraction_root = generation_root / "prefinalizer" / "proposal-attempts"
    try:
        extraction = (
            extract_native_v5_g0_selected_attempts(
                runtime_authority=runtime_authority,
                construction_adapter=adapter,
                output_root=extraction_root,
            )
            if generation_kind == V5_PROPOSAL_GENERATION_G0
            else extract_native_v5_evolved_attempt_chain(
                runtime_authority=runtime_authority,
                construction_adapter=adapter,
                output_root=extraction_root,
            )
        )
        funnel = assemble_native_v5_funnel_reduction_source(
            runtime_authority=runtime_authority,
            proposal_attempt_authority=extraction["proposalAttemptAuthority"],
            generation_index=generation_index,
            evaluation_panel=current_panel,
            campaign_seal=proposal_campaign["campaignSeal"]["campaignSeal"],
            tail_authority=proposal_campaign["campaignSeal"]["directionalTailAuthority"],
            tail_result_index=proposal_campaign["campaignSeal"]["tailResultIndex"],
            minimum_total_trades=int(policy["minimumTotalTrades"]),
            minimum_trades_per_window=int(policy["minimumTradesPerWindow"]),
            output_root=(generation_root / "prefinalizer" / "funnel-reduction"),
        )
    except TemporalQDV5ControlPlaneError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc

    state["stage"] = "native_v5_rotating_prefinalizer"
    _save_state(state_path, state)
    state_basis = _native_v5_generation_state_basis(
        state=state, config=config, generation_index=generation_index
    )
    completed_records = state.get("completedGenerations")
    if not isinstance(completed_records, list):
        raise TemporalDiscoveryContractError(
            "native v5 postproposal completed generation records are invalid"
        )
    generation_config_sha256 = _sha256(
        adapter.get("generationConfigSha256"),
        name="native v5 proposal generation config identity",
    )
    try:
        base = build_native_v5_prefinalizer_base_manifest(
            runtime_authority=runtime_authority,
            # The v2 base uses Rust-canonical transport paths on Windows.
            # Keep it in a versioned operational root so a precommit manifest
            # written by the superseded normal-drive spelling is never
            # overwritten or silently adopted.
            output_root=(generation_root / "prefinalizer" / "base-v2"),
            generation_index=generation_index,
            supervisor_config_sha256=_sha256(
                config.get("configSha256"), name="native v5 supervisor config identity"
            ),
            generation_config_sha256=generation_config_sha256,
            state_basis=state_basis,
            completed_generation_records=completed_records,
            proposal_state_authority=proposal_state_authority,
            rotating_evidence=rotating,
            archive_policy_authority=_native_v5_archive_policy_binding(
                evolvable["archivePolicyAuthority"]
            ),
            proposal_semantic_roots=proposal_semantic_roots,
            identity_ledger_sha256=adapter["identityLedger"]["semanticSha256"],
            native_v5_invocation=adapter["nativeV5Invocation"],
            funnel_reduction_input=funnel["input"],
            funnel_assembly_receipt_binding=funnel["assemblyReceiptBinding"],
            previous_parent_archive_binding=_native_v5_prefinalizer_archive_binding(
                parent_archive_descriptor,
                name="native v5 previous parent archive",
            ),
            previous_cumulative_archive_binding=(
                _native_v5_prefinalizer_archive_binding(
                    previous_cumulative_archive_descriptor,
                    name="native v5 previous cumulative archive",
                )
                if previous_cumulative_archive_descriptor is not None
                else None
            ),
            proposal_campaign_receipt_path=Path(
                proposal_campaign["campaignReceipt"]["receiptPath"]
            ),
            finalizer_output_root=_native_finalization_root(root, generation_index),
        )
        prefinalizer = run_native_v5_rotating_prefinalizer(
            runtime_authority=runtime_authority,
            manifest_path=Path(base["manifestPath"]),
        )
    except TemporalQDV5ControlPlaneError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc

    while prefinalizer["receipt"]["status"] != "ready_for_finalizer":
        selections = tuple(prefinalizer["taskSelections"])
        if not selections:
            raise TemporalDiscoveryContractError(
                "native v5 prefinalizer awaited work without a native selection"
            )
        state["stage"] = "native_v5_rotating_campaign_round"
        _save_state(state_path, state)
        receipts: list[Path] = []
        for selection in sorted(selections, key=lambda item: int(item["taskOrdinal"])):
            panel = _native_v5_panel_by_id(
                rotating_evidence=rotating, panel_id=str(selection["panelId"])
            )
            round_root = (
                generation_root
                / "campaign"
                / "prefinalizer"
                / f"round-{int(prefinalizer['receipt']['roundIndex']):04d}"
                / f"task-{int(selection['taskOrdinal']):04d}"
            )
            campaign = _run_native_v5_campaign_round(
                runtime_authority=runtime_authority,
                config=config,
                generation_index=generation_index,
                evaluation_population_path=evaluation_population_path,
                evaluation_population_raw_sha256=str(
                    adapter["evaluationPopulation"]["fileSha256"]
                ),
                campaign_root=round_root,
                panel=panel,
                campaign_role=str(selection["campaignRole"]),
                cohort_source={
                    "kind": "sealed_cohort_selection",
                    "sourceSemanticSha256": selection["candidateSetSha256"],
                    "candidateCount": selection["candidateCount"],
                    "selectionSha256": selection["selectionSha256"],
                },
                cohort_selection_path=Path(selection["selectionPath"]),
                gateway_token=gateway_token,
            )
            receipts.append(Path(campaign["campaignReceipt"]["receiptPath"]))
        state["stage"] = "native_v5_rotating_prefinalizer"
        _save_state(state_path, state)
        try:
            resume = build_native_v5_prefinalizer_resume_manifest(
                runtime_authority=runtime_authority,
                output_root=(
                    generation_root
                    / "prefinalizer"
                    / f"round-{int(prefinalizer['receipt']['roundIndex']) + 1:04d}"
                ),
                base_manifest_path=Path(base["manifestPath"]),
                previous_execution_receipt=prefinalizer["receipt"],
                new_campaign_receipt_paths=tuple(receipts),
            )
            prefinalizer = run_native_v5_rotating_prefinalizer(
                runtime_authority=runtime_authority,
                manifest_path=Path(resume["manifestPath"]),
            )
        except TemporalQDV5ControlPlaneError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc

    finalizer_binding = prefinalizer["receipt"].get("finalizerManifest")
    if (
        not isinstance(finalizer_binding, Mapping)
        or set(finalizer_binding)
        != {"schemaVersion", "path", "rawSha256", "sizeBytes", "manifestSha256"}
        or finalizer_binding.get("schemaVersion")
        != "temporal_qd_v5_prefinalizer_finalizer_manifest_descriptor_v1"
        or not native_v5_transport_path_matches(
            finalizer_binding.get("path"),
            _native_finalization_root(root, generation_index) / "manifest.json",
        )
    ):
        raise TemporalDiscoveryContractError(
            "native v5 prefinalizer omitted its fixed finalizer-root manifest"
        )
    finalizer_manifest = Path(str(finalizer_binding["path"]))
    state["stage"] = "native_v5_generation_finalization"
    _save_state(state_path, state)
    try:
        finalization = run_native_v5_generation_finalizer(
            runtime_authority=runtime_authority, manifest_path=finalizer_manifest
        )
    except TemporalQDV5ControlPlaneError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    record = _apply_native_v5_state_application(
        root=root,
        state=state,
        state_path=state_path,
        config=config,
        generation_index=generation_index,
        generation_kind=generation_kind,
        finalization=finalization,
        construction_adapter=adapter,
    )
    return {
        "generationRecord": record,
        "proposalCampaign": proposal_campaign,
        "proposalAttemptExtraction": extraction,
        "funnelReduction": funnel,
        "prefinalizer": prefinalizer,
        "finalization": finalization,
    }


def _normalize_tail_result_mode(value: str) -> str:
    if not isinstance(value, str) or value not in _TAIL_RESULT_MODES:
        allowed = ", ".join(sorted(_TAIL_RESULT_MODES))
        raise TemporalDiscoveryContractError(
            f"tail result mode must be one of: {allowed}"
        )
    return value


def _normalize_native_finalization_validation(value: str) -> str:
    if value not in _NATIVE_FINALIZATION_VALIDATION_MODES:
        allowed = ", ".join(sorted(_NATIVE_FINALIZATION_VALIDATION_MODES))
        raise TemporalDiscoveryContractError(
            f"native finalization validation must be one of: {allowed}"
        )
    return value


def _normalize_generation_finalization_engine(value: str) -> str:
    if value not in _GENERATION_FINALIZATION_ENGINES:
        allowed = ", ".join(sorted(_GENERATION_FINALIZATION_ENGINES))
        raise TemporalDiscoveryContractError(
            f"generation finalization engine must be one of: {allowed}"
        )
    return value


def _require_irreversible_native_cutover_engine(
    *,
    root: Path,
    generation_finalization_engine: str,
    state: Mapping[str, Any] | None = None,
) -> None:
    native_authority_exists = any(
        (root / name).is_file()
        for name in (
            "native-finalization-authority.json",
            NATIVE_FINALIZATION_ADOPTION_AUTHORITY_FILE,
        )
    )
    native_boundary_exists = bool(
        state
        and any(
            isinstance(record, Mapping)
            and (
                isinstance(record.get("nativeGenerationFinalization"), Mapping)
                or record.get("schemaVersion") == GENERATION_RECORD_SCHEMA
            )
            for record in state.get("completedGenerations") or []
        )
    )
    if (
        native_authority_exists or native_boundary_exists
    ) and generation_finalization_engine != GENERATION_FINALIZATION_ENGINE_RUST:
        raise TemporalDiscoveryContractError(
            "run has crossed the native finalization boundary; every restart must "
            "explicitly select the Rust finalization engine"
        )


def _require_native_v5_finalization_engine(
    *,
    generation_finalization_engine: str,
    supplied_evolvable_authority: Mapping[str, Any] | None,
    persisted_config: Mapping[str, Any] | None,
) -> None:
    """Fail before authority hydration when a fresh/current v5 run picks Python.

    The old pre-cutover v5 artifacts deliberately have no
    ``nativeV5ProposalRuntime`` seal and remain readable as historical evidence.
    A supplied evolvable authority is a fresh v5 request; a persisted runtime
    seal is a current v5 restart.  Neither may route post-proposal work through
    the Python finalizer.
    """

    current_v5 = (
        supplied_evolvable_authority is not None
        or (
            isinstance(persisted_config, Mapping)
            and _native_v5_proposal_enabled(persisted_config)
        )
    )
    if (
        current_v5
        and generation_finalization_engine != GENERATION_FINALIZATION_ENGINE_RUST
    ):
        raise TemporalDiscoveryContractError(
            "fresh/current v5 requires generation_finalization_engine='rust'; "
            "Python finalization is oracle-only"
        )


def _verified_tail_result_index(
    *,
    campaign_root: Path,
    indexes: dict[Path, dict[str, Any]],
    include_funnel_projection: bool = False,
) -> dict[str, Any]:
    """Return one source-verified, retained index for a completed campaign.

    ``build_tail_result_index`` is intentionally the only source-blob reader
    on the indexed rotating-tail path.  On a restart it verifies an existing
    immutable index against every raw blob exactly once; for the rest of this
    supervisor transaction the returned mapping is reused in memory.
    """

    result_root = (campaign_root / "screening-run").resolve()
    existing = indexes.get(result_root)
    if existing is not None:
        checked = validate_tail_result_index(existing)
        if checked["funnelProjectionIncluded"] != include_funnel_projection:
            raise TemporalDiscoveryContractError(
                "retained tail result index funnel projection mode drifted"
            )
        # Keep returning the retained mapping itself.  The validator above is
        # a no-I/O integrity check; allocating a fresh top-level copy here
        # would defeat the transaction's explicit identity/retention boundary.
        return existing
    authority = _canonical_file(campaign_root / "authority.json", name="tail authority")
    task_manifest = _canonical_file(
        result_root / "task-manifest.json", name="tail task manifest"
    )
    checkpoint = _canonical_file(
        result_root / "checkpoint.json", name="tail task checkpoint"
    )
    index = build_tail_result_index(
        result_root=result_root,
        authority=authority,
        task_manifest=task_manifest,
        checkpoint=checkpoint,
        include_funnel_projection=include_funnel_projection,
    )
    indexes[result_root] = index
    return index


def _rotating_evidence_semantic_authority(
    *,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    construction_operator_policy: Mapping[str, Any] | None,
    base_decision_timeframe: str,
    cost_views: Mapping[str, Any],
) -> dict[str, Any]:
    authority = {
        "schemaVersion": "temporal_qd_rotating_evidence_semantic_authority_v1",
        "executionEngineCommit": _git_sha(
            execution_engine_commit, name="rotating execution engine commit"
        ),
        "workerContractSha256": _sha256(
            worker_contract_sha256, name="rotating worker contract"
        ),
        "constructionOperatorPolicySha256": (
            canonical_sha256(construction_operator_policy)
            if construction_operator_policy is not None
            else None
        ),
        "baseDecisionTimeframe": str(base_decision_timeframe).strip().upper(),
        "costViewsSha256": canonical_sha256(cost_views),
    }
    authority["authoritySha256"] = canonical_sha256(authority)
    if not authority["baseDecisionTimeframe"]:
        raise TemporalDiscoveryContractError(
            "rotating evidence semantic authority lacks a base timeframe"
        )
    return authority


def _require_continuation_evidence_semantics(
    continuation: Mapping[str, Any], current: Mapping[str, Any]
) -> None:
    source = continuation.get("sourceEvidenceSemanticAuthority")
    if not isinstance(source, Mapping) or _clone(
        source, name="source rotating evidence semantic authority"
    ) != _clone(current, name="current rotating evidence semantic authority"):
        raise TemporalDiscoveryContractError(
            "QD continuation rotating evidence semantics drifted"
        )


def _broad_admission_contract_values(generation_count: int) -> dict[str, int]:
    """Return the exact frozen values for an admissible broad-run block."""

    if generation_count == FRESH_BROAD_GENERATION_COUNT:
        candidate_evaluations = FRESH_BROAD_CANDIDATE_EVALUATIONS
        discovery_worker_tasks = FRESH_BROAD_DISCOVERY_WORKER_TASKS
    elif generation_count == LEGACY_CONTINUATION_GENERATION_COUNT:
        candidate_evaluations = LEGACY_CONTINUATION_CANDIDATE_EVALUATIONS
        discovery_worker_tasks = LEGACY_CONTINUATION_DISCOVERY_WORKER_TASKS
    else:
        raise TemporalDiscoveryContractError(
            "broad admission contract has an unsupported generation count"
        )
    return {
        "generationCount": generation_count,
        "candidatesPerGeneration": FRESH_BROAD_CANDIDATES_PER_GENERATION,
        "candidateEvaluations": candidate_evaluations,
        "discoveryWindowsPerCandidate": FRESH_BROAD_DISCOVERY_WINDOWS_PER_CANDIDATE,
        "discoveryWorkerTasks": discovery_worker_tasks,
    }


def _rotating_task_upper_bounds(
    *,
    contract: Mapping[str, Any],
    first_generation_index: int,
    generation_count: int,
    proposal_width: int,
    initial_parent_count: int,
) -> dict[str, int]:
    """Freeze truthful maxima for the multi-campaign generation transaction."""

    provisional = int(contract["provisionalReduction"]["maxCandidates"])
    breeder_width = int(contract["robustSelection"]["breederWidth"])
    if initial_parent_count < 0:
        raise TemporalDiscoveryContractError(
            "rotating initial parent count cannot be negative"
        )
    windows_per_panel = len(contract["panels"][0]["windows"])
    proposal_candidate_panels = generation_count * proposal_width
    retained_parent_candidate_panels = initial_parent_count + max(
        0, generation_count - 1
    ) * breeder_width
    backfill_candidate_panels = sum(
        provisional * max(0, len(required_panel_ids(contract, index)) - 1)
        for index in range(
            first_generation_index, first_generation_index + generation_count
        )
    )
    candidate_panels = (
        proposal_candidate_panels
        + retained_parent_candidate_panels
        + backfill_candidate_panels
    )
    return {
        "generationCount": generation_count,
        "candidatesPerGeneration": proposal_width,
        "proposalCandidateEvaluations": generation_count * proposal_width,
        "proposalCandidatePanels": proposal_candidate_panels,
        "initialParentCandidateCount": initial_parent_count,
        "retainedParentCandidatePanelsUpperBound": retained_parent_candidate_panels,
        "backfillCandidatePanelsUpperBound": backfill_candidate_panels,
        "totalCandidatePanelsUpperBound": candidate_panels,
        "windowsPerPanel": windows_per_panel,
        "workerTasksUpperBound": candidate_panels * windows_per_panel,
    }


def _immigrant_construction_capacity_requirement(
    config: Mapping[str, Any],
) -> int:
    """Return the one frozen pair-immigrant requirement for freeze and reopen.

    G0 constructs its full pool before selecting the normal evaluation width.
    Capacity therefore means ``pool + later normal widths``, not merely market
    evaluations.  Ordinary/continuation blocks retain their historical normal
    ``generation_count * width`` requirement.
    """

    plan = config.get("generationPlan")
    if not isinstance(plan, Mapping):
        raise TemporalDiscoveryContractError("QD generation plan is invalid")
    try:
        first = int(plan["firstGenerationIndex"])
        count = int(plan["generationCount"])
        width = int(plan["targetUniqueCandidatesPerGeneration"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError("QD generation plan capacity is invalid") from exc
    if any(isinstance(plan.get(key), bool) for key in (
        "firstGenerationIndex", "generationCount", "targetUniqueCandidatesPerGeneration"
    )) or first < 1 or count < 1 or width < 1:
        raise TemporalDiscoveryContractError("QD generation plan capacity is invalid")
    g0 = config.get("g0Bootstrap")
    if g0 is None:
        return count * width
    if not isinstance(g0, Mapping) or set(g0) != {
        "schemaVersion", "initialConstructionPoolSize", "evaluationPopulationSize", "activation"
    }:
        raise TemporalDiscoveryContractError("QD G0 bootstrap capacity binding is invalid")
    try:
        pool = int(g0["initialConstructionPoolSize"])
        evaluation_width = int(g0["evaluationPopulationSize"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError("QD G0 bootstrap capacity is invalid") from exc
    if (
        any(isinstance(g0.get(key), bool) for key in (
            "initialConstructionPoolSize", "evaluationPopulationSize"
        ))
        or g0.get("schemaVersion") != "temporal_qd_g0_bootstrap_config_v1"
        or g0.get("activation") != "generation_1_pair_random_immigrants_only"
        or first != 1
        or pool < 1
        or evaluation_width != width
        or pool < evaluation_width
    ):
        raise TemporalDiscoveryContractError("QD G0 bootstrap capacity binding is invalid")
    return pool + (count - 1) * width


def _require_frozen_immigrant_capacity_requirement(
    config: Mapping[str, Any], contract: Mapping[str, Any]
) -> int:
    """Recompute and bind the capacity total recorded at fresh freeze time."""

    required = _immigrant_construction_capacity_requirement(config)
    frozen = contract.get("immigrantConstructionCandidateRequirement")
    if frozen is None:
        # The explicit field was introduced with G0. Preserve no-G0 legacy
        # blocks, whose normal-width requirement is unambiguous.
        if config.get("g0Bootstrap") is not None:
            raise TemporalDiscoveryContractError(
                "QD G0 frozen construction capacity requirement is unavailable"
            )
        return required
    if isinstance(frozen, bool):
        raise TemporalDiscoveryContractError(
            "QD frozen construction capacity requirement drifted"
        )
    try:
        observed = int(frozen)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            "QD frozen construction capacity requirement drifted"
        ) from exc
    if observed != required:
        raise TemporalDiscoveryContractError(
            "QD frozen construction capacity requirement drifted"
        )
    return required


def _require_evolvable_capacity_receipt_supply(
    receipt: Mapping[str, Any], *, required_unique_candidates: int
) -> None:
    """Require an actual-factory receipt to prove the frozen campaign supply.

    A v5 receipt is not merely a factory-health signal: broad admission's
    G0-plus-later-generation construction requirement is frozen in the run
    contract. Its two admitted identity counts must both cover that exact
    requirement, otherwise a finite preview can make a false campaign-wide
    non-collision claim.
    """

    if isinstance(required_unique_candidates, bool) or required_unique_candidates < 1:
        raise TemporalDiscoveryContractError(
            "evolvable v5 capacity receipt requirement is invalid"
        )
    admitted = receipt.get("compiledAdmittedCandidateCount")
    unique_pairs = receipt.get("uniqueSemanticPairCount")
    if (
        not isinstance(admitted, int)
        or isinstance(admitted, bool)
        or not isinstance(unique_pairs, int)
        or isinstance(unique_pairs, bool)
        or admitted < required_unique_candidates
        or unique_pairs < required_unique_candidates
    ):
        raise TemporalDiscoveryContractError(
            "evolvable v5 capacity receipt does not prove the frozen campaign candidate supply"
        )


def _archive_member_count(archive: Mapping[str, Any]) -> int:
    cells = archive.get("cells")
    if not isinstance(cells, list):
        raise TemporalDiscoveryContractError("QD archive cells are invalid")
    total = 0
    for cell in cells:
        if not isinstance(cell, Mapping) or not isinstance(cell.get("members"), list):
            raise TemporalDiscoveryContractError("QD archive members are invalid")
        total += len(cell["members"])
    return total


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"{name} must be finite canonical JSON"
        ) from exc


def _encoded(value: Mapping[str, Any]) -> str:
    return (
        json.dumps(
            dict(value),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(
                f"refusing to change frozen broad-run input: {path}"
            )
        return
    _write_durable_new(path, encoded)


def _sync_directory(path: Path) -> None:
    """Persist the publication directory where the platform exposes that primitive.

    POSIX gives directory fsync a clear durability meaning.  Windows exposes the
    equivalent only through a directory handle with ``FILE_FLAG_BACKUP_SEMANTICS``;
    older filesystems can reject that flush, in which case the file flush plus the
    atomic replace is still the strongest available Python-level guarantee.
    """

    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return
    try:
        kernel32 = ctypes.windll.kernel32
        kernel32.CreateFileW.restype = ctypes.c_void_p
        kernel32.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        kernel32.FlushFileBuffers.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel32.CreateFileW(
            str(path),
            0x80000000,  # GENERIC_READ
            0x00000001 | 0x00000002 | 0x00000004,  # all common sharing modes
            None,
            3,  # OPEN_EXISTING
            0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
            None,
        )
        invalid = ctypes.c_void_p(-1).value
        if handle == invalid:
            raise OSError(ctypes.get_last_error(), "CreateFileW directory failed")
        try:
            if not kernel32.FlushFileBuffers(ctypes.c_void_p(handle)):
                raise OSError(ctypes.get_last_error(), "FlushFileBuffers directory failed")
        finally:
            kernel32.CloseHandle(ctypes.c_void_p(handle))
    except (AttributeError, OSError):
        # Some Windows filesystems do not permit flushing a directory handle.
        # The committed file was synchronised before publication either way.
        return


def _write_durable_new(path: Path, encoded: str) -> None:
    """Create one immutable JSON file only after its bytes have reached disk."""

    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=path.name + ".",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_text(encoding="utf-8") != encoded:
                raise TemporalDiscoveryContractError(
                    f"refusing to change frozen broad-run input: {path}"
                )
        _sync_directory(path.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _publish_committed_file(
    source: Path,
    destination: Path,
    *,
    replace_existing: bool = False,
) -> None:
    """Durably publish the exact bytes of one immutable native output."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_sha256 = _native_binary_file_sha256(source)
    if destination.is_file():
        if _native_binary_file_sha256(destination) == source_sha256:
            return
        if not replace_existing:
            source_payload = _canonical_file(
                source, name=f"committed native {source.name}"
            )
            existing_payload = _canonical_file(
                destination, name=f"published {source.name}"
            )
            if existing_payload != source_payload:
                raise TemporalDiscoveryContractError(
                    f"native publication destination diverged: {destination}"
                )

    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            dir=destination.parent,
            prefix=destination.name + ".",
            suffix=".tmp",
            delete=False,
        ) as handle:
            with source.open("rb") as source_handle:
                shutil.copyfileobj(source_handle, handle, length=1024 * 1024)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = Path(handle.name)
        if _native_binary_file_sha256(temporary) != source_sha256:
            raise TemporalDiscoveryContractError(
                f"native publication copy hash drifted: {destination}"
            )
        if destination.is_file():
            os.replace(temporary, destination)
            temporary = None
        else:
            try:
                os.link(temporary, destination)
            except FileExistsError:
                if _native_binary_file_sha256(destination) != source_sha256:
                    raise TemporalDiscoveryContractError(
                        f"native publication destination raced: {destination}"
                    )
        _sync_directory(destination.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _replace(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    try:
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256(value: Any, *, name: str) -> str:
    token = str(value or "").strip().lower()
    if (
        not token.startswith("sha256:")
        or len(token) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in token[7:])
    ):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical SHA-256")
    return token


def _git_sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip().lower()
    if len(token) != _GIT_SHA_LENGTH or any(
        character not in "0123456789abcdef" for character in token
    ):
        raise TemporalDiscoveryContractError(f"{name} must be a full Git SHA")
    return token


def _command(path: Path) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read validator command: {path}"
        ) from exc
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise TemporalDiscoveryContractError(
            "validator command must be a non-empty string array"
        )
    return list(value)


def _state_identity(state: Mapping[str, Any]) -> str:
    material = _clone(state, name="QD supervisor state")
    material.pop("stateSha256", None)
    return canonical_sha256(material)


def _save_state(path: Path, state: dict[str, Any]) -> None:
    state["updatedAt"] = _utc_now()
    state.pop("stateSha256", None)
    state["stateSha256"] = canonical_sha256(state)
    _replace(path, state)


def _load_state(path: Path, *, config_sha256: str) -> dict[str, Any]:
    state = _read(path, name="QD supervisor state")
    supplied = _sha256(state.get("stateSha256"), name="supervisor state identity")
    if _state_identity(state) != supplied:
        raise TemporalDiscoveryContractError("QD supervisor state identity mismatch")
    if state.get("schemaVersion") != SUPERVISOR_STATE_SCHEMA:
        raise TemporalDiscoveryContractError("unknown QD supervisor state schema")
    if state.get("configSha256") != config_sha256:
        raise TemporalDiscoveryContractError(
            "QD supervisor state is bound to a different frozen policy"
        )
    return state


def _event(event: str, **values: Any) -> None:
    print(
        json.dumps(
            {"at": _utc_now(), "event": event, **values},
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        ),
        flush=True,
    )


def _completed_task_count(checkpoint_path: Path) -> int:
    if not checkpoint_path.exists():
        return 0
    checkpoint = _read(checkpoint_path, name="temporal evaluation checkpoint")
    completed = checkpoint.get("completed") or {}
    if not isinstance(completed, Mapping):
        raise TemporalDiscoveryContractError(
            "temporal evaluation checkpoint completed set is invalid"
        )
    return len(completed)


def _canonical_file(path: Path, *, name: str) -> dict[str, Any]:
    if not path.is_file():
        raise TemporalDiscoveryContractError(f"missing {name}: {path}")
    return _read(path, name=name)


def _artifact_descriptor(path: Path, payload: Mapping[str, Any]) -> dict[str, str]:
    return {
        "path": str(path.resolve()),
        "sha256": canonical_sha256(payload),
    }


_V5_LINEAGE_UNAVAILABLE_REASONS = (
    "operator_application_not_sealed",
    "observed_execution_attribution_not_sealed",
    "canonical_evidence_components_not_sealed",
    "realized_behavior_evidence_binding_not_sealed",
    "retention_evidence_not_sealed",
    "full_ancestry_or_external_parent_evidence_not_sealed",
)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _write_v5_lineage_unavailable_marker(
    *, root: Path, generation_index: int, config: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Seal an explicit v5 causal-lineage abstention after finalization.

    This is intentionally absent for historical policies.  The bounded source
    set consists only of completed immutable artifacts already reopened by the
    supervisor; no aggregate is reverse-engineered into missing causality.
    """
    if not isinstance(config.get("evolvableModuleAuthority"), Mapping):
        return None
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    relative_paths = [
        "proposal/generation-journal.json",
        "campaign/campaign.json",
        "archive.json",
    ]
    evidence_ledger = generation_root / "evidence" / "generation-ledger.json"
    if evidence_ledger.is_file():
        relative_paths.append("evidence/generation-ledger.json")
    artifacts: list[dict[str, str]] = []
    for relative in relative_paths:
        path = generation_root / relative
        if not path.is_file():
            raise TemporalDiscoveryContractError(
                "v5 lineage-unavailable marker source artifact is absent"
            )
        artifacts.append({"relativePath": relative, "sha256": _file_digest(path)})
    return write_proposal_lineage_unavailable(
        generation_root=generation_root,
        campaign_id=f"v5:{config['configSha256']}",
        completed_generation_index=generation_index,
        reasons=_V5_LINEAGE_UNAVAILABLE_REASONS,
        source_artifacts=artifacts,
    )


def _pair_policy_authority(config: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Use the base pair authority for policy projection under enriched v5."""
    if isinstance(config.get("evolvableModuleAuthority"), Mapping):
        source = config.get("bidirectionalPairSourceAuthority")
        if not isinstance(source, Mapping):
            raise TemporalDiscoveryContractError(
                "evolvable v5 pair source authority is unavailable"
            )
        return source
    value = config.get("bidirectionalPairGeneration")
    return value if isinstance(value, Mapping) else None


def _self_hashed_descriptor(
    path: Path,
    payload: Mapping[str, Any],
    *,
    field: str,
    name: str,
) -> dict[str, str]:
    identity = _identity_payload(payload, field, name=name)
    descriptor = _artifact_descriptor(path, payload)
    descriptor[field] = identity
    return descriptor


def _result_record_codec_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "resultCodec": metadata["codec"],
        "resultSemanticSha256": metadata["semanticSha256"],
        "resultSemanticSizeBytes": metadata["semanticSizeBytes"],
        "resultUncompressedSha256": metadata["uncompressedSha256"],
        "resultUncompressedSizeBytes": metadata["uncompressedSizeBytes"],
        "resultBlobSha256": metadata["blobSha256"],
        "resultBlobSizeBytes": metadata["blobSizeBytes"],
    }


def _results_descriptor(
    *,
    result_root: Path,
    checkpoint: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    tail_result_index: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    tasks = task_manifest.get("tasks")
    completed = checkpoint.get("completed")
    if not isinstance(tasks, list) or not isinstance(completed, Mapping):
        raise TemporalDiscoveryContractError(
            "completed generation task manifest/checkpoint is invalid"
        )
    expected_tasks = {
        str(task.get("task_id")): task
        for task in tasks
        if isinstance(task, Mapping) and isinstance(task.get("task_id"), str)
    }
    if len(expected_tasks) != len(tasks) or set(completed) != set(expected_tasks):
        raise TemporalDiscoveryContractError(
            "completed generation checkpoint does not cover its exact task matrix"
        )
    indexed_entries: dict[str, Mapping[str, Any]] | None = None
    if tail_result_index is not None:
        indexed = validate_tail_result_index(tail_result_index)
        if (
            indexed["authorityId"] != task_manifest.get("authorityId")
            or indexed["taskManifestSha256"] != canonical_sha256(task_manifest)
            or indexed["checkpointSha256"] != canonical_sha256(checkpoint)
            or indexed["taskCount"] != len(expected_tasks)
        ):
            raise TemporalDiscoveryContractError(
                "indexed completed results descriptor binding drifted"
            )
        indexed_entries = {
            str(entry["task"]["taskId"]): entry for entry in indexed["entries"]
        }
        if set(indexed_entries) != set(expected_tasks):
            raise TemporalDiscoveryContractError(
                "indexed completed results descriptor task matrix drifted"
            )

    rows: list[dict[str, Any]] = []
    for task_id in sorted(expected_tasks):
        record = completed[task_id]
        task = expected_tasks[task_id]
        if not isinstance(record, Mapping) or not isinstance(task.get("payload"), Mapping):
            raise TemporalDiscoveryContractError("completed generation result record is invalid")
        expected_candidate = task["payload"].get("candidate_id")
        if record.get("candidateId") != expected_candidate:
            raise TemporalDiscoveryContractError(
                "completed generation result candidate identity mismatch"
            )
        raw_path = record.get("resultPath")
        if not isinstance(raw_path, str) or not raw_path:
            raise TemporalDiscoveryContractError("completed generation result path is missing")
        result_path = Path(raw_path)
        if result_path.resolve().parent != (result_root / "results").resolve():
            raise TemporalDiscoveryContractError(
                "completed generation result is outside its immutable result root"
            )
        if indexed_entries is None:
            try:
                material, metadata = read_json_object(result_path)
            except ResultCodecError as exc:
                raise TemporalDiscoveryContractError(
                    f"completed generation result is corrupt: {result_path}"
                ) from exc
            semantic_sha = canonical_sha256(material)
            if record.get("resultSha256") != semantic_sha:
                raise TemporalDiscoveryContractError(
                    "completed generation result semantic identity mismatch"
                )
            codec = _result_record_codec_metadata(metadata)
        else:
            entry = indexed_entries[task_id]
            indexed_task = entry["task"]
            raw_ref = entry["rawResultRef"]
            if (
                indexed_task["candidateId"] != expected_candidate
                or indexed_task["taskPayloadSha256"]
                != canonical_sha256(task["payload"])
                or raw_ref["relativePath"]
                != result_path.resolve().relative_to(result_root.resolve()).as_posix()
                or record.get("resultSha256") != raw_ref["resultSha256"]
            ):
                raise TemporalDiscoveryContractError(
                    "indexed completed generation result binding drifted"
                )
            semantic_sha = str(raw_ref["resultSha256"])
            codec = {
                "resultCodec": raw_ref["codec"],
                "resultSemanticSha256": semantic_sha,
                "resultSemanticSizeBytes": raw_ref["semanticSizeBytes"],
                "resultUncompressedSha256": raw_ref["uncompressedSha256"],
                "resultUncompressedSizeBytes": raw_ref["uncompressedSizeBytes"],
                "resultBlobSha256": raw_ref["blobSha256"],
                "resultBlobSizeBytes": raw_ref["blobSizeBytes"],
            }
        if any(record.get(key) != value for key, value in codec.items() if key in record):
            raise TemporalDiscoveryContractError(
                "completed generation result representation metadata mismatch"
            )
        rows.append(
            {
                "taskId": task_id,
                "checkpointRecordSha256": canonical_sha256(record),
                "resultPath": str(result_path.resolve()),
                "resultSha256": semantic_sha,
                **codec,
            }
        )
    descriptor = {
        "schemaVersion": "temporal_qd_supervisor_completed_results_v1",
        "taskCount": len(expected_tasks),
        "records": rows,
    }
    descriptor["resultsSha256"] = canonical_sha256(descriptor)
    return descriptor


def _rotating_campaign_artifacts(
    *,
    campaign_root: Path,
    population_path: Path | None = None,
    tail_result_index: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Reopen one evaluation-only campaign and its exact result matrix."""

    campaign = _canonical_file(campaign_root / "campaign.json", name="rotating campaign")
    authority = _canonical_file(campaign_root / "authority.json", name="rotating authority")
    identity = _canonical_file(
        campaign_root / "evaluation-identity.json", name="rotating evaluation identity"
    )
    result_root = campaign_root / "screening-run"
    manifest = _canonical_file(
        result_root / "task-manifest.json", name="rotating task manifest"
    )
    checkpoint = _canonical_file(
        result_root / "checkpoint.json", name="rotating task checkpoint"
    )
    summary = _canonical_file(result_root / "summary.json", name="rotating task summary")
    campaign_sha = _identity_payload(
        campaign, "campaignSha256", name="rotating campaign"
    )
    authority_id = _identity_payload(
        authority, "authorityId", name="rotating authority"
    )
    identity_sha = _identity_payload(
        identity, "evaluationIdentitySha256", name="rotating evaluation identity"
    )
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or manifest.get("taskMatrixSha256") != canonical_sha256(tasks):
        raise TemporalDiscoveryContractError("rotating campaign task matrix drifted")
    if any(
        value != authority_id
        for value in (
            campaign.get("authorityId"),
            manifest.get("authorityId"),
            checkpoint.get("authorityId"),
            summary.get("authorityId"),
        )
    ):
        raise TemporalDiscoveryContractError("rotating campaign authority binding drifted")
    results = _results_descriptor(
        result_root=result_root,
        checkpoint=checkpoint,
        task_manifest=manifest,
        tail_result_index=tail_result_index,
    )
    output: dict[str, Any] = {
        "schemaVersion": "temporal_qd_rotating_campaign_artifacts_v1",
        "campaignSha256": campaign_sha,
        "authorityId": authority_id,
        "evaluationIdentitySha256": identity_sha,
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "taskCount": len(tasks),
        "checkpointSha256": canonical_sha256(checkpoint),
        "summarySha256": canonical_sha256(summary),
        "results": results,
    }
    if population_path is not None:
        population = _canonical_file(population_path, name="rotating cohort population")
        output["population"] = _self_hashed_descriptor(
            population_path,
            population,
            field="populationSha256",
            name="rotating cohort population",
        )
        if campaign.get("populationSha256") != population["populationSha256"]:
            raise TemporalDiscoveryContractError(
                "rotating cohort campaign population binding drifted"
            )
    output["artifactsSha256"] = canonical_sha256(output)
    return output


def _capture_screening_artifacts(
    *,
    population_path: Path,
    archive_path: Path,
    campaign_root: Path,
    generation_index: int,
    label: str,
    generation_journal_path: Path | None = None,
    tail_result_index: Mapping[str, Any] | None = None,
    verify_population_file: bool = True,
) -> dict[str, Any]:
    """Reopen the immutable outputs common to every frozen screening campaign."""

    result_root = campaign_root / "screening-run"
    preparation_path = campaign_root / "preparation.json"
    authority_path = campaign_root / "authority.json"
    identity_path = campaign_root / "evaluation-identity.json"
    campaign_path = campaign_root / "campaign.json"
    task_manifest_path = result_root / "task-manifest.json"
    result_authority_path = result_root / "authority.json"
    checkpoint_path = result_root / "checkpoint.json"
    summary_path = result_root / "summary.json"

    evaluation_population_path_value = evaluation_population_path(population_path)
    evaluation_population: Mapping[str, Any] | None = None
    if evaluation_population_path_value.is_file():
        evaluation_population = load_evaluation_population(
            population_path=population_path,
            journal_path=generation_journal_path,
            verify_population_file=verify_population_file,
        )
        population = evaluation_population
    else:
        population = _canonical_file(population_path, name=f"{label} population")
    archive = _canonical_file(archive_path, name=f"{label} archive")
    preparation = _canonical_file(preparation_path, name=f"{label} preparation")
    authority = _canonical_file(authority_path, name=f"{label} authority")
    evaluation_identity = _canonical_file(identity_path, name=f"{label} evaluation identity")
    campaign = _canonical_file(campaign_path, name=f"{label} campaign")
    task_manifest = _canonical_file(task_manifest_path, name=f"{label} task manifest")
    result_authority = _canonical_file(result_authority_path, name=f"{label} result authority")
    checkpoint = _canonical_file(checkpoint_path, name=f"{label} checkpoint")
    summary = _canonical_file(
        summary_path,
        name="QD evaluation summary" if label == "QD generation" else f"{label} summary",
    )

    if int(population.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError(f"{label} population index mismatch")
    if int(archive.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError(f"{label} archive index mismatch")
    if int(campaign.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError(f"{label} campaign index mismatch")

    population_sha = _identity_payload(
        population, "evaluationPopulationSha256", name=f"{label} evaluation population"
    ) if evaluation_population is not None else _identity_payload(
        population, "populationSha256", name=f"{label} population"
    )
    if evaluation_population is not None:
        population_sha = str(evaluation_population["populationSha256"])
    if campaign.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError("QD campaign population binding mismatch")
    if evaluation_identity.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError(
            "QD evaluation identity population binding mismatch"
        )
    if evaluation_population is not None:
        projection_sha = evaluation_population["evaluationPopulationSha256"]
        if (
            campaign.get("evaluationPopulationSha256") != projection_sha
            or evaluation_identity.get("evaluationPopulationSha256") != projection_sha
        ):
            raise TemporalDiscoveryContractError(
                "QD campaign evaluation-population binding mismatch"
            )
    preparation_sha = canonical_sha256(preparation)
    if campaign.get("preparationSha256") != preparation_sha:
        raise TemporalDiscoveryContractError("QD campaign preparation binding mismatch")
    if evaluation_identity.get("templatePreparationSha256") is None:
        raise TemporalDiscoveryContractError(
            "QD evaluation identity template binding is missing"
        )

    manifest_tasks = task_manifest.get("tasks")
    if not isinstance(manifest_tasks, list):
        raise TemporalDiscoveryContractError("QD task manifest tasks are invalid")
    task_matrix_sha = canonical_sha256(manifest_tasks)
    if task_manifest.get("taskMatrixSha256") != task_matrix_sha:
        raise TemporalDiscoveryContractError("QD task manifest identity mismatch")
    if checkpoint.get("taskMatrixSha256") != task_matrix_sha:
        raise TemporalDiscoveryContractError("QD checkpoint task matrix mismatch")
    if task_manifest.get("authorityId") != authority.get("authorityId"):
        raise TemporalDiscoveryContractError("QD task manifest authority mismatch")
    if result_authority.get("authorityId") != authority.get("authorityId"):
        raise TemporalDiscoveryContractError("QD result authority mismatch")
    if checkpoint.get("authorityId") != authority.get("authorityId"):
        raise TemporalDiscoveryContractError("QD checkpoint authority mismatch")
    if summary.get("authorityId") != authority.get("authorityId"):
        raise TemporalDiscoveryContractError("QD summary authority mismatch")
    if summary.get("taskCount") != len(manifest_tasks) or summary.get(
        "completedTaskCount"
    ) != len(manifest_tasks):
        raise TemporalDiscoveryContractError("QD summary completion mismatch")
    if campaign.get("authorityId") != authority.get("authorityId"):
        raise TemporalDiscoveryContractError("QD campaign authority binding mismatch")
    if campaign.get("taskMatrixSha256") != task_matrix_sha:
        raise TemporalDiscoveryContractError("QD campaign task matrix binding mismatch")

    results = _results_descriptor(
        result_root=result_root,
        checkpoint=checkpoint,
        task_manifest=task_manifest,
        tail_result_index=tail_result_index,
    )
    output = {
        "schemaVersion": "temporal_qd_supervisor_generation_artifacts_v1",
        "population": (
            {
                "path": str(population_path.resolve()),
                "sha256": evaluation_population["populationFileSha256"],
                "populationSha256": population_sha,
            }
            if evaluation_population is not None
            else _self_hashed_descriptor(
                population_path,
                population,
                field="populationSha256",
                name=f"{label} population",
            )
        ),
        "archive": _self_hashed_descriptor(
            archive_path,
            archive,
            field="archiveSha256",
            name=f"{label} archive",
        ),
        "preparation": _artifact_descriptor(preparation_path, preparation),
        "authority": _self_hashed_descriptor(
            authority_path,
            authority,
            field="authorityId",
            name=f"{label} authority",
        ),
        "evaluationIdentity": _self_hashed_descriptor(
            identity_path,
            evaluation_identity,
            field="evaluationIdentitySha256",
            name=f"{label} evaluation identity",
        ),
        "campaign": _self_hashed_descriptor(
            campaign_path,
            campaign,
            field="campaignSha256",
            name=f"{label} campaign",
        ),
        "taskManifest": _artifact_descriptor(task_manifest_path, task_manifest),
        "resultAuthority": _self_hashed_descriptor(
            result_authority_path,
            result_authority,
            field="authorityId",
            name=f"{label} result authority",
        ),
        "checkpoint": _artifact_descriptor(checkpoint_path, checkpoint),
        "summary": _artifact_descriptor(summary_path, summary),
        "results": results,
        **(
            {
                "evaluationPopulation": _self_hashed_descriptor(
                    evaluation_population_path_value,
                    evaluation_population,
                    field="evaluationPopulationSha256",
                    name=f"{label} evaluation population",
                ),
            }
            if evaluation_population is not None
            else {}
        ),
        **(
            {"g0Bootstrap": _clone(evaluation_population["g0Bootstrap"], name=f"{label} G0 bootstrap binding")}
            if evaluation_population is not None and evaluation_population.get("g0Bootstrap") is not None
            else {}
        ),
    }
    return output


def _capture_generation_artifacts(
    *,
    root: Path,
    generation_index: int,
    generation_funnel_enabled: bool = False,
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    tail_result_indexes: dict[Path, dict[str, Any]] | None = None,
    verify_population_file: bool = True,
    verify_rotating_campaign_artifacts: bool = True,
) -> dict[str, Any]:
    tail_result_mode = _normalize_tail_result_mode(tail_result_mode)
    indexes = tail_result_indexes if tail_result_indexes is not None else {}
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    population_path = proposal_root / "population.json"
    journal_path = proposal_root / "generation-journal.json"
    archive_path = generation_root / "archive.json"
    proposal_tail_index = (
        _verified_tail_result_index(
            campaign_root=campaign_root,
            indexes=indexes,
            include_funnel_projection=generation_funnel_enabled,
        )
        if tail_result_mode == TAIL_RESULT_MODE_INDEXED
        else None
    )
    output = _capture_screening_artifacts(
        population_path=population_path,
        archive_path=archive_path,
        campaign_root=campaign_root,
        generation_index=generation_index,
        label="QD generation",
        generation_journal_path=journal_path,
        tail_result_index=proposal_tail_index,
        verify_population_file=verify_population_file,
    )
    journal = _canonical_file(journal_path, name="QD generation journal")
    if int(journal.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("generation journal index mismatch")
    output["journal"] = _self_hashed_descriptor(
        journal_path,
        journal,
        field="journalSha256",
        name="QD generation journal",
    )
    if generation_funnel_enabled:
        funnel_path = generation_root / "generation-funnel.json"
        funnel = _canonical_file(funnel_path, name="QD generation funnel")
        try:
            snapshot = supervisor_funnel_snapshot(funnel)
        except GenerationFunnelContractError as exc:
            raise TemporalDiscoveryContractError("QD generation funnel identity is invalid") from exc
        output["generationFunnel"] = _self_hashed_descriptor(
            funnel_path,
            funnel,
            field="artifactSha256",
            name="QD generation funnel",
        )
        output["generationFunnelSnapshot"] = {
            **snapshot,
            "snapshotSha256": snapshot["snapshotSha256"],
        }
    evidence_root = generation_root / "evidence"
    ledger_path = evidence_root / "generation-ledger.json"
    if ledger_path.is_file():
        ledger = _canonical_file(ledger_path, name="rotating generation ledger")
        _identity_payload(
            ledger, "ledgerSha256", name="rotating generation ledger"
        )
        if verify_rotating_campaign_artifacts:
            for binding in ledger.get("campaigns") or []:
                if not isinstance(binding, Mapping):
                    raise TemporalDiscoveryContractError(
                        "rotating generation campaign ledger is invalid"
                    )
                if binding.get("role") == "proposal_current_panel":
                    continue
                recorded_campaign_artifacts = binding.get("artifacts")
                if not isinstance(recorded_campaign_artifacts, Mapping):
                    raise TemporalDiscoveryContractError(
                        "rotating cohort campaign lacks its artifact ledger"
                    )
                current_campaign_artifacts = _rotating_campaign_artifacts(
                    campaign_root=Path(str(binding.get("campaignRoot") or "")),
                    population_path=Path(str(binding.get("populationPath") or "")),
                    tail_result_index=(
                        _verified_tail_result_index(
                            campaign_root=Path(
                                str(binding.get("campaignRoot") or "")
                            ),
                            indexes=indexes,
                        )
                        if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                        else None
                    ),
                )
                if _clone(
                    current_campaign_artifacts,
                    name="rotating campaign artifacts",
                ) != _clone(
                    recorded_campaign_artifacts,
                    name="recorded rotating campaign artifacts",
                ):
                    raise TemporalDiscoveryContractError(
                        "rotating campaign artifact ledger drifted"
                    )
        output["rotatingEvidenceLedger"] = _self_hashed_descriptor(
            ledger_path,
            ledger,
            field="ledgerSha256",
            name="rotating generation ledger",
        )
        rotating_checkpoint_path = evidence_root / "checkpoint.json"
        rotating_checkpoint = _canonical_file(
            rotating_checkpoint_path, name="rotating generation checkpoint"
        )
        output["rotatingEvidenceCheckpoint"] = _self_hashed_descriptor(
            rotating_checkpoint_path,
            rotating_checkpoint,
            field="checkpointSha256",
            name="rotating generation checkpoint",
        )
        cumulative_path = evidence_root / "cumulative-archive.json"
        cumulative = _canonical_file(
            cumulative_path, name="cumulative breeder archive"
        )
        output["cumulativeBreederArchive"] = _self_hashed_descriptor(
            cumulative_path,
            cumulative,
            field="archiveSha256",
            name="cumulative breeder archive",
        )
    return output


def _generation_artifact_ledgers_match(
    *,
    recorded: Any,
    current: Any,
    allow_native_result_authority_identity_projection: bool = False,
) -> bool:
    """Compare ledgers, including one closed native-production compatibility case.

    The first production native finalizer epoch bound the complete result-authority
    payload through its canonical file hash but omitted the redundant ``authorityId``
    projection from that artifact descriptor.  Reopening the authority still verifies
    the self-hash before this comparison.  Accept only that single-field projection
    difference so the committed epoch remains usable; all other drift stays closed.
    """

    if not isinstance(recorded, Mapping) or not isinstance(current, Mapping):
        return False
    recorded_ledger = _clone(recorded, name="recorded generation artifacts")
    current_ledger = _clone(current, name="current generation artifacts")
    if recorded_ledger == current_ledger:
        return True
    if not allow_native_result_authority_identity_projection:
        return False
    projected = _clone(
        current_ledger, name="native result-authority compatibility projection"
    )
    result_authority = projected.get("resultAuthority")
    if not isinstance(result_authority, dict) or "authorityId" not in result_authority:
        return False
    result_authority.pop("authorityId")
    return recorded_ledger == projected


def _validate_generation_artifacts(
    *,
    root: Path,
    generation_record: Mapping[str, Any],
    config: Mapping[str, Any],
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    tail_result_indexes: dict[Path, dict[str, Any]] | None = None,
) -> None:
    generation_index = int(generation_record.get("generationIndex", -1))
    if generation_index < 1:
        raise TemporalDiscoveryContractError("completed generation index is invalid")
    recorded = generation_record.get("artifacts")
    if not isinstance(recorded, Mapping) or recorded.get("schemaVersion") != (
        "temporal_qd_supervisor_generation_artifacts_v1"
    ):
        raise TemporalDiscoveryContractError(
            "completed generation lacks its immutable artifact ledger"
        )
    funnel_enabled = bool((config.get("generationFunnel") or {}).get("enabled"))
    native_binding = generation_record.get("nativeGenerationFinalization")
    native_production = (
        isinstance(native_binding, Mapping)
        and native_binding.get("authorityMode")
        == "native_production_compact_commit"
    )
    native_v5_construction = isinstance(
        generation_record.get("nativeV5Construction"), Mapping
    )
    current = _capture_generation_artifacts(
        root=root,
        generation_index=generation_index,
        generation_funnel_enabled=funnel_enabled,
        tail_result_mode=tail_result_mode,
        tail_result_indexes=tail_result_indexes,
        verify_population_file=not (native_production or native_v5_construction),
        verify_rotating_campaign_artifacts=not (
            native_production or native_v5_construction
        ),
    )
    if not _generation_artifact_ledgers_match(
        recorded=recorded,
        current=current,
        allow_native_result_authority_identity_projection=(
            native_production
        ),
    ):
        raise TemporalDiscoveryContractError(
            "completed generation artifact ledger drifted from immutable outputs"
        )
    evaluation_identity = _canonical_file(
        Path(current["evaluationIdentity"]["path"]), name="QD evaluation identity"
    )
    evaluation = config.get("evaluation") or {}
    repositories = config.get("repositories") or {}
    rotating = config.get("rotatingEvidence")
    expected_template_sha = evaluation.get("templatePreparationSha256")
    if rotating is not None:
        panel = panel_for_generation(rotating, generation_index)
        expected_template_sha = rotating["panelTemplates"][panel["panelId"]][
            "preparationSha256"
        ]
    if (
        evaluation_identity.get("templatePreparationSha256")
        != expected_template_sha
        or (
            rotating is None
            and evaluation_identity.get("predeclaredEvidenceContextSha256")
            != evaluation.get("predeclaredEvidenceContextSha256")
        )
        or evaluation_identity.get("executionEngineCommit")
        != repositories.get("executionEngineCommit")
        or evaluation_identity.get("policySha256") != config.get("policySha256")
        or (evaluation_identity.get("workerContract") or {}).get(
            "workerContractSha256"
        )
        != config.get("workerContractSha256")
    ):
        raise TemporalDiscoveryContractError(
            "completed generation evaluation identity drifted from frozen config"
        )
    if config.get("evidenceLadder") is not None and evaluation_identity.get(
        "evidenceLadder"
    ) != config.get("evidenceLadder"):
        raise TemporalDiscoveryContractError(
            "completed generation evidence ladder drifted from frozen config"
        )
    if rotating is not None:
        if evaluation_identity.get("rotatingEvidence") != rotating:
            raise TemporalDiscoveryContractError(
                "completed generation rotating evidence drifted from frozen config"
            )
        for field in (
            "rotatingEvidenceLedger",
            "rotatingEvidenceCheckpoint",
            "cumulativeBreederArchive",
        ):
            if field not in current:
                raise TemporalDiscoveryContractError(
                    "completed rotating generation lacks its full evidence ledger"
                )
        if (
            generation_record.get("rotatingEvidenceLedgerSha256")
            != current["rotatingEvidenceLedger"]["ledgerSha256"]
            or generation_record.get("rotatingEvidenceCheckpointSha256")
            != current["rotatingEvidenceCheckpoint"]["checkpointSha256"]
            or generation_record.get("cumulativeArchiveSha256")
            != current["cumulativeBreederArchive"]["archiveSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "completed rotating generation evidence identities disagree"
            )
    for field, identity in (
        ("population", "populationSha256"),
        ("journal", "journalSha256"),
        ("archive", "archiveSha256"),
        ("campaign", "campaignSha256"),
        ("evaluationIdentity", "evaluationIdentitySha256"),
    ):
        if generation_record.get(identity) != current[field][identity]:
            raise TemporalDiscoveryContractError(
                f"completed generation {field} identity disagrees with supervisor record"
            )
    if "evaluationPopulation" in current and generation_record.get(
        "evaluationPopulationSha256"
    ) != current["evaluationPopulation"]["evaluationPopulationSha256"]:
        raise TemporalDiscoveryContractError(
            "completed generation evaluation population identity disagrees with supervisor record"
        )
    expected_g0 = config.get("g0Bootstrap") if generation_index == 1 else None
    if expected_g0 is not None and native_v5_construction:
        adapter = _native_v5_recorded_adapter(
            root=root, generation_record=generation_record
        )
        if (
            adapter["generationKind"] != V5_PROPOSAL_GENERATION_G0
            or adapter["acceptedCandidateCount"]
            != int(expected_g0["initialConstructionPoolSize"])
            or adapter["selectedEvaluationCandidateCount"]
            != int(expected_g0["evaluationPopulationSize"])
        ):
            raise TemporalDiscoveryContractError(
                "completed native v5 G0 receipt disagrees with its frozen dimensions"
            )
    elif expected_g0 is not None:
        current_g0 = current.get("g0Bootstrap")
        recorded_g0 = generation_record.get("g0Bootstrap")
        if not isinstance(current_g0, Mapping) or current_g0 != recorded_g0:
            raise TemporalDiscoveryContractError("completed G0 bootstrap identities disagree with immutable outputs")
    elif native_v5_construction:
        adapter = _native_v5_recorded_adapter(
            root=root, generation_record=generation_record
        )
        if adapter["generationKind"] != V5_PROPOSAL_GENERATION_EVOLVED:
            raise TemporalDiscoveryContractError(
                "completed native v5 evolved generation has G0 construction kind"
            )
    elif "g0Bootstrap" in current or "g0Bootstrap" in generation_record:
        raise TemporalDiscoveryContractError("G0 bootstrap appeared outside its frozen generation-1 boundary")
    archive = _canonical_file(
        Path(current["archive"]["path"]), name="QD generation archive"
    )
    archive_result_set_sha = _sha256(
        archive.get("resultSetSha256"), name="QD generation archive result set"
    )
    if generation_record.get("resultSetSha256") != archive_result_set_sha:
        raise TemporalDiscoveryContractError(
            "completed generation result-set identity disagrees with immutable archive"
        )
    task_manifest = _canonical_file(
        Path(current["taskManifest"]["path"]), name="QD task manifest"
    )
    if generation_record.get("taskMatrixSha256") != task_manifest.get(
        "taskMatrixSha256"
    ):
        raise TemporalDiscoveryContractError(
            "completed generation task matrix identity disagrees with supervisor record"
        )
    if generation_record.get("taskCount") != task_manifest.get("taskCount"):
        raise TemporalDiscoveryContractError(
            "completed generation task count disagrees with immutable task manifest"
        )
    if funnel_enabled:
        snapshot = current["generationFunnelSnapshot"]
        if generation_record.get("generationFunnelArtifactSha256") != current[
            "generationFunnel"
        ]["artifactSha256"] or generation_record.get("generationFunnelSnapshotSha256") != snapshot["snapshotSha256"]:
            raise TemporalDiscoveryContractError(
                "completed generation funnel identity disagrees with supervisor record"
            )


def _validate_completed_generation_ledger(
    *, state: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[int, dict[str, Any]]:
    """Validate completed-generation state without reopening campaign artifacts."""

    completed = state.get("completedGenerations") or []
    if not isinstance(completed, list):
        raise TemporalDiscoveryContractError("completed QD generations are invalid")
    records: dict[int, dict[str, Any]] = {}
    for raw in completed:
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError("completed QD generation record is invalid")
        record = _clone(raw, name="completed QD generation record")
        index = int(record.get("generationIndex", -1))
        if index in records:
            raise TemporalDiscoveryContractError("completed QD generation index is duplicated")
        records[index] = record
    first = int(config["generationPlan"]["firstGenerationIndex"])
    last = int(config["generationPlan"]["lastGenerationIndex"])
    if any(index < first or index > last for index in records):
        raise TemporalDiscoveryContractError("completed QD generation is outside frozen bounds")
    if records:
        latest = max(records)
        if set(records) != set(range(first, latest + 1)):
            raise TemporalDiscoveryContractError("completed QD generations are not contiguous")
    if int(state.get("uniqueCandidatesEvaluated") or 0) != sum(
        int(record.get("candidateCount") or 0) for record in records.values()
    ):
        raise TemporalDiscoveryContractError(
            "QD supervisor candidate counter disagrees with completed generation records"
        )
    if int(state.get("workerTasksCompleted") or 0) != sum(
        int(
            record.get("totalGenerationTaskCount")
            if record.get("totalGenerationTaskCount") is not None
            else record.get("taskCount")
            or 0
        )
        for record in records.values()
    ):
        raise TemporalDiscoveryContractError(
            "QD supervisor worker-task counter disagrees with completed generation records"
        )
    return records


def _validate_completed_generations(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    tail_result_indexes: dict[Path, dict[str, Any]] | None = None,
) -> dict[int, dict[str, Any]]:
    """Revalidate every completed generation for restart/audit admission."""

    records = _validate_completed_generation_ledger(state=state, config=config)
    for index in sorted(records):
        if _native_v5_proposal_enabled(config):
            invocation = records[index].get("nativeV5Invocation")
            if not isinstance(invocation, Mapping):
                raise TemporalDiscoveryContractError(
                    "completed native v5 generation lacks receipt adoption authority"
                )
            _reauthenticate_native_v5_supervisor_invocation(
                root=root, config=config, invocation=invocation
            )
        _validate_generation_artifacts(
            root=root,
            generation_record=records[index],
            config=config,
            tail_result_mode=tail_result_mode,
            tail_result_indexes=tail_result_indexes,
        )
        _write_v5_lineage_unavailable_marker(
            root=root, generation_index=index, config=config
        )
        # A restart/audit pass never needs one generation's verified
        # projections while validating the next.  Release each boundary so
        # the retained in-memory set is bounded by one generation.
        if (
            tail_result_mode == TAIL_RESULT_MODE_INDEXED
            and tail_result_indexes is not None
        ):
            tail_result_indexes.clear()
    return records


def _canonical_json_line(value: Mapping[str, Any]) -> str:
    return (
        json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _write_canonical_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _canonical_json_line(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(
                f"refusing divergent native finalization input: {path}"
            )
        return
    _write_durable_new(path, encoded)


def _validated_identity_ledger(
    path: Path, *, name: str
) -> tuple[dict[str, Any], str]:
    ledger = _canonical_file(path, name=name)
    ledger_sha256 = _identity_payload(ledger, "ledgerSha256", name=name)
    return ledger, ledger_sha256


def _generation_identity_ledger_path(root: Path, generation_index: int) -> Path:
    return (
        root
        / "generations"
        / f"generation-{generation_index:04d}"
        / "proposal"
        / "identity-ledger.json"
    )


def _identity_ledger_input_snapshot_path(
    root: Path, generation_index: int
) -> Path:
    return (
        root
        / "generations"
        / f"generation-{generation_index:04d}"
        / "proposal"
        / "input-identity-ledger.json"
    )


def _native_generation_output_ledger_sha256(
    *,
    root: Path,
    generation_index: int,
    generation_record: Mapping[str, Any],
) -> str:
    native_base = (
        root
        / "generations"
        / f"generation-{generation_index:04d}"
        / "proposal"
        / "native-batch"
    )
    matches: list[str] = []
    for result_path in sorted(native_base.glob("*/generation-result.json")):
        manifest_path = result_path.parent / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = validate_generation_manifest(
                _canonical_file(
                    manifest_path, name="native generation ledger manifest"
                )
            )
            if (
                manifest.get("generationConfig", {}).get("generationIndex")
                != generation_index
            ):
                continue
            result = validate_generation_result(
                _canonical_file(
                    result_path, name="native generation ledger result"
                ),
                manifest=manifest,
            )
        except (TemporalQDNativeError, TemporalDiscoveryContractError):
            continue
        pair_result = result.get("pairGenerationResult")
        if not isinstance(pair_result, Mapping):
            continue
        if any(
            generation_record.get(field) is not None
            and pair_result.get(field) != generation_record.get(field)
            for field in ("populationSha256", "journalSha256")
        ):
            continue
        output_sha256 = result.get("outputIdentityLedgerSha256")
        if isinstance(output_sha256, str):
            matches.append(output_sha256)
    unique_matches = set(matches)
    if len(unique_matches) != 1:
        raise TemporalDiscoveryContractError(
            "generation does not have one unambiguous native output identity ledger"
        )
    return unique_matches.pop()


def _reconcile_native_pair_identity_ledger(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path | None = None,
    completed_by_index: Mapping[int, Mapping[str, Any]],
) -> tuple[dict[str, Any], str]:
    """Recover the committed ledger facade before another native invocation.

    The native batch maintains a mutable public-ledger facade while it runs.
    Supervisor state is the commit authority, so an incomplete generation may
    leave only either its frozen input or its exact native output at the root.
    Anything else is tampering, not a recoverable crash window.
    """

    root_ledger_path = root / "identity-ledger.json"
    transaction = state.get("identityLedgerTransaction")
    latest = max(completed_by_index) if completed_by_index else None
    if latest is not None:
        expected_path = _generation_identity_ledger_path(root, latest)
        expected, expected_sha256 = _validated_identity_ledger(
            expected_path, name="latest committed generation identity ledger"
        )
        if _native_generation_output_ledger_sha256(
            root=root,
            generation_index=latest,
            generation_record=completed_by_index[latest],
        ) != expected_sha256:
            raise TemporalDiscoveryContractError(
                "latest committed generation identity ledger binding drifted"
            )
    elif isinstance(transaction, Mapping):
        generation_index = int(transaction.get("generationIndex") or 0)
        expected_path = _identity_ledger_input_snapshot_path(
            root, generation_index
        )
        expected, expected_sha256 = _validated_identity_ledger(
            expected_path, name="frozen generation input identity ledger"
        )
        if transaction.get("inputLedgerSha256") != expected_sha256:
            raise TemporalDiscoveryContractError(
                "frozen generation input ledger transaction drifted"
            )
    else:
        return _validated_identity_ledger(
            root_ledger_path, name="campaign identity ledger"
        )

    root_ledger, root_sha256 = _validated_identity_ledger(
        root_ledger_path, name="campaign identity ledger"
    )
    if root_sha256 == expected_sha256:
        if (
            isinstance(transaction, Mapping)
            and transaction.get("phase") == "generation_boundary_ready"
        ):
            transaction_generation = int(transaction.get("generationIndex") or 0)
            if (
                latest is None
                or transaction_generation != latest
                or transaction.get("outputLedgerSha256") != expected_sha256
            ):
                raise TemporalDiscoveryContractError(
                    "ready identity-ledger transaction is not the latest committed generation"
                )
            state.pop("identityLedgerTransaction", None)
            if state_path is not None:
                _save_state(state_path, state)
        return expected, expected_sha256

    recoverable_sha256: set[str] = set()
    if isinstance(transaction, Mapping):
        if transaction.get("phase") == "generation_boundary_ready":
            transaction_generation = int(transaction.get("generationIndex") or 0)
            if (
                latest is None
                or transaction_generation != latest
                or transaction.get("outputLedgerSha256") != expected_sha256
            ):
                raise TemporalDiscoveryContractError(
                    "ready identity-ledger transaction is not the latest committed generation"
                )
            _input, input_sha256 = _validated_identity_ledger(
                _identity_ledger_input_snapshot_path(root, transaction_generation),
                name="ready generation input identity ledger",
            )
            if transaction.get("inputLedgerSha256") != input_sha256:
                raise TemporalDiscoveryContractError(
                    "ready generation input identity ledger binding drifted"
                )
            recoverable_sha256.add(input_sha256)
        output_sha256 = transaction.get("outputLedgerSha256")
        if isinstance(output_sha256, str):
            recoverable_sha256.add(output_sha256)
        generation_index = int(transaction.get("generationIndex") or 0)
        output_path = _generation_identity_ledger_path(root, generation_index)
        if output_path.is_file():
            _output, output_sha256 = _validated_identity_ledger(
                output_path, name="incomplete generation output identity ledger"
            )
            recoverable_sha256.add(output_sha256)
    current_generation = int(state.get("currentGenerationIndex") or 0)
    if current_generation not in completed_by_index and current_generation > 0:
        output_path = _generation_identity_ledger_path(root, current_generation)
        if output_path.is_file():
            _output, output_sha256 = _validated_identity_ledger(
                output_path, name="uncommitted generation output identity ledger"
            )
            recoverable_sha256.add(output_sha256)
    for completed_index in completed_by_index:
        prior_path = _generation_identity_ledger_path(root, completed_index)
        if prior_path.is_file():
            _prior, prior_sha256 = _validated_identity_ledger(
                prior_path, name="committed generation identity ledger"
            )
            recoverable_sha256.add(prior_sha256)
    if root_sha256 not in recoverable_sha256:
        raise TemporalDiscoveryContractError(
            "campaign identity ledger is neither committed nor a bound crash-window output"
        )
    _replace(root_ledger_path, expected)
    repaired, repaired_sha256 = _validated_identity_ledger(
        root_ledger_path, name="repaired campaign identity ledger"
    )
    if repaired != expected or repaired_sha256 != expected_sha256:
        raise TemporalDiscoveryContractError(
            "campaign identity ledger recovery did not converge"
        )
    if (
        isinstance(transaction, Mapping)
        and transaction.get("phase") == "generation_boundary_ready"
    ):
        transaction_generation = int(transaction.get("generationIndex") or 0)
        if (
            latest is None
            or transaction_generation != latest
            or transaction.get("outputLedgerSha256") != expected_sha256
        ):
            raise TemporalDiscoveryContractError(
                "ready identity-ledger transaction is not the latest committed generation"
            )
        state.pop("identityLedgerTransaction", None)
        if state_path is not None:
            _save_state(state_path, state)
    return expected, expected_sha256


def _prepare_native_pair_identity_ledger_transaction(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    input_ledger: Mapping[str, Any],
    input_ledger_sha256: str,
) -> None:
    snapshot_path = _identity_ledger_input_snapshot_path(root, generation_index)
    _write_once(snapshot_path, input_ledger)
    snapshot, snapshot_sha256 = _validated_identity_ledger(
        snapshot_path, name="frozen native generation input identity ledger"
    )
    if snapshot != input_ledger or snapshot_sha256 != input_ledger_sha256:
        raise TemporalDiscoveryContractError(
            "native generation input ledger snapshot drifted"
        )
    expected = {
        "schemaVersion": "temporal_qd_identity_ledger_transaction_v1",
        "generationIndex": generation_index,
        "inputLedgerPath": str(snapshot_path.resolve()),
        "inputLedgerSha256": input_ledger_sha256,
        "outputLedgerPath": str(
            _generation_identity_ledger_path(root, generation_index).resolve()
        ),
        "phase": "generation_proposal",
    }
    existing = state.get("identityLedgerTransaction")
    if existing is not None:
        comparable = {
            key: value
            for key, value in dict(existing).items()
            if key not in {"outputLedgerSha256", "phase"}
        }
        expected_comparable = {
            key: value
            for key, value in expected.items()
            if key not in {"outputLedgerSha256", "phase"}
        }
        if comparable != expected_comparable:
            raise TemporalDiscoveryContractError(
                "incomplete native identity-ledger transaction drifted"
            )
        expected.update(
            {
                key: existing[key]
                for key in ("outputLedgerSha256", "phase")
                if key in existing
            }
        )
    state["identityLedgerTransaction"] = expected
    _save_state(state_path, state)


def _seal_native_pair_identity_ledger_output(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    input_ledger: Mapping[str, Any],
    input_ledger_sha256: str,
    generation_result: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    output_path = _generation_identity_ledger_path(root, generation_index)
    output, output_sha256 = _validated_identity_ledger(
        output_path, name="native generation output identity ledger"
    )
    if _native_generation_output_ledger_sha256(
        root=root,
        generation_index=generation_index,
        generation_record=generation_result,
    ) != output_sha256:
        raise TemporalDiscoveryContractError(
            "native generation output identity ledger binding disagrees"
        )
    root_path = root / "identity-ledger.json"
    _root_ledger, root_sha256 = _validated_identity_ledger(
        root_path, name="post-generation campaign identity ledger"
    )
    if root_sha256 not in {input_ledger_sha256, output_sha256}:
        raise TemporalDiscoveryContractError(
            "native generation changed the campaign identity ledger divergently"
        )
    if root_sha256 == output_sha256:
        _replace(root_path, input_ledger)
    restored, restored_sha256 = _validated_identity_ledger(
        root_path, name="restored campaign identity ledger"
    )
    if restored != input_ledger or restored_sha256 != input_ledger_sha256:
        raise TemporalDiscoveryContractError(
            "native generation input ledger did not restore after proposal commit"
        )
    transaction = state.get("identityLedgerTransaction")
    if not isinstance(transaction, Mapping):
        raise TemporalDiscoveryContractError(
            "native generation lost its identity-ledger transaction"
        )
    state["identityLedgerTransaction"] = {
        **dict(transaction),
        "outputLedgerSha256": output_sha256,
        "phase": "proposal_committed",
    }
    _save_state(state_path, state)
    return output, output_sha256


def _promote_native_pair_identity_ledger(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    generation_record: Mapping[str, Any],
    _after_step: Any | None = None,
) -> None:
    transaction = state.get("identityLedgerTransaction")
    if (
        not isinstance(transaction, Mapping)
        or transaction.get("generationIndex") != generation_index
        or transaction.get("phase") != "proposal_committed"
    ):
        raise TemporalDiscoveryContractError(
            "native generation identity ledger is not ready for promotion"
        )
    output, output_sha256 = _validated_identity_ledger(
        _generation_identity_ledger_path(root, generation_index),
        name="committing generation identity ledger",
    )
    if (
        transaction.get("outputLedgerSha256") != output_sha256
        or _native_generation_output_ledger_sha256(
            root=root,
            generation_index=generation_index,
            generation_record=generation_record,
        )
        != output_sha256
    ):
        raise TemporalDiscoveryContractError(
            "committing generation identity ledger lost its state binding"
        )
    state["identityLedgerTransaction"] = {
        **dict(transaction),
        "phase": "generation_boundary_ready",
    }
    _save_state(state_path, state)
    if _after_step is not None:
        _after_step("ready_saved")
    _replace(root / "identity-ledger.json", output)
    promoted, promoted_sha256 = _validated_identity_ledger(
        root / "identity-ledger.json", name="promoted campaign identity ledger"
    )
    if promoted != output or promoted_sha256 != output_sha256:
        raise TemporalDiscoveryContractError(
            "committed generation identity ledger promotion did not converge"
        )
    if _after_step is not None:
        _after_step("root_promoted")
    state.pop("identityLedgerTransaction", None)
    _save_state(state_path, state)
    if _after_step is not None:
        _after_step("transaction_cleared")


def _native_v5_recorded_adapter(
    *, root: Path, generation_record: Mapping[str, Any]
) -> dict[str, Any]:
    invocation = generation_record.get("nativeV5Invocation")
    if not isinstance(invocation, Mapping):
        raise TemporalDiscoveryContractError(
            "completed native v5 generation lacks its immutable invocation"
        )
    checked = _validate_native_v5_supervisor_invocation(root=root, value=invocation)
    recorded_adapter = generation_record.get("nativeV5Construction")
    if not isinstance(recorded_adapter, Mapping) or recorded_adapter != checked[
        "constructionAdapter"
    ]:
        raise TemporalDiscoveryContractError(
            "completed native v5 generation construction binding drifted"
        )
    return checked["constructionAdapter"]


def _native_v5_identity_ledger_descriptor(
    value: object,
    *,
    name: str,
    expected_path: Path | None = None,
) -> dict[str, Any]:
    """Validate one opaque, receipt-addressed v5 ledger descriptor.

    This is deliberately a descriptor validator, not a file validator.  The
    identity ledger is candidate-scale state owned by qd-batch/finalizer; its
    bytes are reauthenticated by Rust when used.  Python may persist the
    sealed path and roots for restart, but may not open, hash, copy, or
    snapshot the ledger.
    """

    descriptor = _clone(value, name=name)
    expected_fields = {
        "absolutePath",
        "semanticSha256",
        "fileSha256",
        "byteLength",
    }
    if not isinstance(descriptor, Mapping) or set(descriptor) != expected_fields:
        raise TemporalDiscoveryContractError(f"{name} descriptor schema drifted")
    supplied_path = descriptor.get("absolutePath")
    if not isinstance(supplied_path, str) or not Path(supplied_path).is_absolute():
        raise TemporalDiscoveryContractError(f"{name} descriptor path is invalid")
    canonical_path = str(Path(os.path.abspath(supplied_path)))
    if supplied_path != canonical_path:
        raise TemporalDiscoveryContractError(f"{name} descriptor path is not canonical")
    if expected_path is not None and canonical_path != str(
        Path(os.path.abspath(str(expected_path)))
    ):
        raise TemporalDiscoveryContractError(f"{name} descriptor path drifted")
    semantic_sha256 = _sha256(
        descriptor.get("semanticSha256"), name=f"{name} semantic identity"
    )
    file_sha256 = _sha256(
        descriptor.get("fileSha256"), name=f"{name} file identity"
    )
    byte_length = descriptor.get("byteLength")
    if isinstance(byte_length, bool) or not isinstance(byte_length, int) or byte_length < 0:
        raise TemporalDiscoveryContractError(f"{name} descriptor byte length is invalid")
    return {
        "absolutePath": canonical_path,
        "semanticSha256": semantic_sha256,
        "fileSha256": file_sha256,
        "byteLength": byte_length,
    }


def _native_v5_identity_ledger_descriptor_from_adapter(
    *,
    adapter: Mapping[str, Any],
    root: Path,
    generation_index: int,
    name: str,
) -> dict[str, Any]:
    artifact = adapter.get("identityLedger")
    if not isinstance(artifact, Mapping):
        raise TemporalDiscoveryContractError(f"{name} is unavailable")
    material = _native_v5_identity_ledger_descriptor(
        {
            "absolutePath": artifact.get("absolutePath"),
            "semanticSha256": artifact.get("semanticSha256"),
            "fileSha256": artifact.get("fileSha256"),
            "byteLength": artifact.get("byteLength"),
        },
        name=name,
        expected_path=_native_v5_identity_ledger_output_path(root, generation_index),
    )
    if artifact.get("relativePath") != "v5-native/identity-ledger.json":
        raise TemporalDiscoveryContractError(f"{name} relative path drifted")
    return material


def _reconcile_native_v5_identity_ledger(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    completed_by_index: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Recover the opaque current-v5 ledger authority without reopening it.

    A completed direct Rust record deliberately has no Python wrapper.  The
    separately persisted descriptor is therefore the sole next-generation
    input authority.  It is bound to the latest compact finalizer sidecar and
    remains an immutable proposal output; Rust reauthenticates its bytes when
    an evolved proposal uses it.
    """

    # Old root/snapshot transactions are a pre-cutover representation.  They
    # must never be silently imported into a current-v5 run.
    if state.get("nativeV5IdentityLedgerTransaction") is not None:
        raise TemporalDiscoveryContractError(
            "current native v5 rejects legacy root identity-ledger transactions"
        )
    latest = max(completed_by_index) if completed_by_index else None
    raw_descriptor = state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY)
    if latest is None:
        if raw_descriptor is not None:
            raise TemporalDiscoveryContractError(
                "current native v5 has an unbound committed identity-ledger descriptor"
            )
        return None
    if not isinstance(raw_descriptor, Mapping):
        raise TemporalDiscoveryContractError(
            "current native v5 lacks its committed identity-ledger descriptor"
        )
    descriptor = _native_v5_identity_ledger_descriptor(
        raw_descriptor,
        name="current native v5 committed identity ledger",
        expected_path=_native_v5_identity_ledger_output_path(root, latest),
    )
    sidecar = _canonical_file(
        _native_finalization_root(root, latest)
        / GENERATION_STATE_APPLICATION_SIDECAR_FILENAME,
        name="latest committed native v5 state-application sidecar",
    )
    if (
        _identity_payload(
            sidecar,
            "sidecarSha256",
            name="latest committed native v5 state-application sidecar",
        )
        != sidecar.get("sidecarSha256")
        or sidecar.get("generationIndex") != latest
        or not isinstance(sidecar.get("identityLedgerPromotion"), Mapping)
    ):
        raise TemporalDiscoveryContractError(
            "latest native v5 identity-ledger sidecar drifted"
        )
    promotion = sidecar["identityLedgerPromotion"]
    if (
        promotion.get("outputRelativePath") != "proposal/v5-native/identity-ledger.json"
        or promotion.get("outputIdentityLedgerSha256")
        != descriptor["semanticSha256"]
        or promotion.get("outputIdentityLedgerFileSha256")
        != descriptor["fileSha256"]
    ):
        raise TemporalDiscoveryContractError(
            "current native v5 committed identity-ledger descriptor drifted"
        )
    # Canonicalise only the bounded state field before the next write; no
    # ledger byte access occurs here.
    if raw_descriptor != descriptor:
        state[NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = descriptor
        _save_state(state_path, state)
    return descriptor


def _build_native_v5_identity_ledger_input(
    *,
    generation_kind: str,
    committed_identity_ledger_descriptor: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Bind one prior immutable ledger descriptor for the native proposal."""

    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if committed_identity_ledger_descriptor is not None:
            raise TemporalDiscoveryContractError(
                "native v5 G0 cannot inherit an identity-ledger descriptor"
            )
        return None
    if generation_kind != V5_PROPOSAL_GENERATION_EVOLVED:
        raise TemporalDiscoveryContractError("native v5 identity-ledger kind is invalid")
    if not isinstance(committed_identity_ledger_descriptor, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 evolved generation lacks a committed identity-ledger descriptor"
        )
    descriptor = _native_v5_identity_ledger_descriptor(
        committed_identity_ledger_descriptor,
        name="native v5 evolved committed identity ledger",
    )
    try:
        return build_v5_proposal_input_binding(
            kind="identityLedger", sealed_descriptor=descriptor
        )
    except TemporalQDV5NativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc


def _legacy_reconcile_native_v5_identity_ledger(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    completed_by_index: Mapping[int, Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, str | None]:
    """Recover only the receipt-bound v5 ledger boundary after interruption.

    Unlike the legacy native bridge, the v5 batch never mutates the campaign
    root ledger during construction.  The supervisor publishes the exact
    receipt-addressed output only after the generation record is durable.
    That gives restart a small, explicit two-state repair window and makes
    every other root value a tripwire.
    """

    root_path = root / "identity-ledger.json"
    transaction = state.get("nativeV5IdentityLedgerTransaction")
    latest = max(completed_by_index) if completed_by_index else None
    expected_path: Path | None = None
    expected_sha256: str | None = None
    if latest is not None:
        expected_path = _native_v5_identity_ledger_output_path(root, latest)
        _ledger, expected_sha256 = _validated_native_v5_identity_ledger(
            expected_path, name="latest committed native v5 identity ledger"
        )
        latest_record = completed_by_index[latest]
        if latest_record.get("schemaVersion") == GENERATION_RECORD_SCHEMA:
            sidecar = _canonical_file(
                _native_finalization_root(root, latest)
                / GENERATION_STATE_APPLICATION_SIDECAR_FILENAME,
                name="latest committed native v5 state-application sidecar",
            )
            if (
                _identity_payload(
                    sidecar,
                    "sidecarSha256",
                    name="latest committed native v5 state-application sidecar",
                )
                != sidecar.get("sidecarSha256")
                or sidecar.get("generationIndex") != latest
                or not isinstance(sidecar.get("identityLedgerPromotion"), Mapping)
                or sidecar["identityLedgerPromotion"].get(
                    "outputIdentityLedgerSha256"
                )
                != expected_sha256
            ):
                raise TemporalDiscoveryContractError(
                    "latest committed native v5 state-application ledger drifted"
                )
        else:
            adapter = _native_v5_recorded_adapter(
                root=root, generation_record=latest_record
            )
            if (
                adapter["identityLedger"]["absolutePath"] != str(expected_path.resolve())
                or adapter["identityLedger"]["semanticSha256"] != expected_sha256
            ):
                raise TemporalDiscoveryContractError(
                    "latest committed native v5 identity ledger receipt drifted"
                )
    if transaction is not None and not isinstance(transaction, Mapping):
        raise TemporalDiscoveryContractError("native v5 identity-ledger transaction is invalid")
    if isinstance(transaction, Mapping):
        transaction = _clone(transaction, name="native v5 identity-ledger transaction")
        transaction_index = transaction.get("generationIndex")
        if (
            transaction.get("schemaVersion")
            != NATIVE_V5_IDENTITY_LEDGER_TRANSACTION_SCHEMA
            or isinstance(transaction_index, bool)
            or not isinstance(transaction_index, int)
            or transaction_index < 1
            or transaction.get("generationKind")
            not in {V5_PROPOSAL_GENERATION_G0, V5_PROPOSAL_GENERATION_EVOLVED}
            or transaction.get("phase")
            not in {
                "generation_proposal",
                "proposal_committed",
                "generation_boundary_ready",
            }
        ):
            raise TemporalDiscoveryContractError(
                "native v5 identity-ledger transaction is malformed"
            )
        output_path = _native_v5_identity_ledger_output_path(root, transaction_index)
        if transaction.get("outputLedgerPath") != str(output_path.resolve()):
            raise TemporalDiscoveryContractError(
                "native v5 identity-ledger output path drifted"
            )
        if transaction["generationKind"] == V5_PROPOSAL_GENERATION_G0:
            if transaction.get("inputLedgerPath") is not None or transaction.get(
                "inputLedgerSha256"
            ) is not None:
                raise TemporalDiscoveryContractError(
                    "native v5 G0 identity-ledger transaction has inputs"
                )
            input_path = None
            input_sha256 = None
        else:
            input_path = _native_v5_identity_ledger_snapshot_path(root, transaction_index)
            if transaction.get("inputLedgerPath") != str(input_path.resolve()):
                raise TemporalDiscoveryContractError(
                    "native v5 identity-ledger input path drifted"
                )
            _input, input_sha256 = _validated_native_v5_identity_ledger(
                input_path, name="native v5 frozen input identity ledger"
            )
            if transaction.get("inputLedgerSha256") != input_sha256:
                raise TemporalDiscoveryContractError(
                    "native v5 identity-ledger input drifted"
                )
        output_sha256 = transaction.get("outputLedgerSha256")
        if output_sha256 is not None:
            output_sha256 = _sha256(
                output_sha256, name="native v5 identity-ledger output"
            )
            _output, verified_output_sha256 = _validated_native_v5_identity_ledger(
                output_path, name="native v5 transaction output identity ledger"
            )
            if verified_output_sha256 != output_sha256:
                raise TemporalDiscoveryContractError(
                    "native v5 identity-ledger output drifted"
                )
        if transaction["phase"] == "generation_boundary_ready":
            if (
                latest is None
                or transaction_index != latest
                or expected_sha256 is None
                or output_sha256 != expected_sha256
            ):
                raise TemporalDiscoveryContractError(
                    "ready native v5 identity-ledger transaction is not committed"
                )
            if root_path.is_file():
                _root, root_sha256 = _validated_native_v5_identity_ledger(
                    root_path, name="campaign native v5 identity ledger"
                )
                if root_sha256 not in {input_sha256, expected_sha256}:
                    raise TemporalDiscoveryContractError(
                        "campaign native v5 identity ledger is tampered"
                    )
            elif input_sha256 is not None:
                raise TemporalDiscoveryContractError(
                    "campaign native v5 identity ledger is missing"
                )
            _publish_committed_file(
                expected_path, root_path, replace_existing=True
            )
            state.pop("nativeV5IdentityLedgerTransaction", None)
            _save_state(state_path, state)
        elif latest is not None and transaction_index <= latest:
            raise TemporalDiscoveryContractError(
                "native v5 identity-ledger transaction lags a completed generation"
            )
        else:
            # An unfinished invocation must not have published a new campaign
            # ledger.  It will be reauthenticated by the exact same native
            # manifest before we accept its output on the resumed attempt.
            if input_sha256 is None:
                if root_path.exists():
                    raise TemporalDiscoveryContractError(
                        "native v5 G0 transaction unexpectedly has a campaign ledger"
                    )
            else:
                _root, root_sha256 = _validated_native_v5_identity_ledger(
                    root_path, name="campaign native v5 identity ledger"
                )
                if root_sha256 != input_sha256:
                    raise TemporalDiscoveryContractError(
                        "native v5 unfinished transaction changed the campaign ledger"
                    )

    if latest is None:
        if transaction is None and root_path.exists():
            raise TemporalDiscoveryContractError(
                "native v5 campaign has an unbound identity ledger"
            )
        return None, None
    assert expected_path is not None and expected_sha256 is not None
    _committed, root_sha256 = _validated_native_v5_identity_ledger(
        root_path, name="committed native v5 campaign identity ledger"
    )
    if root_sha256 != expected_sha256:
        raise TemporalDiscoveryContractError(
            "committed native v5 campaign identity ledger drifted"
        )
    return _committed, expected_sha256


def _legacy_prepare_native_v5_identity_ledger_transaction(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    generation_kind: str,
    committed_identity_ledger_sha256: str | None,
) -> dict[str, Any] | None:
    """Freeze an evolved input ledger before the one-shot v5 invocation."""

    proposal_root = _native_v5_proposal_root(root, generation_index)
    output_path = _native_v5_identity_ledger_output_path(root, generation_index)
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if committed_identity_ledger_sha256 is not None or (root / "identity-ledger.json").exists():
            raise TemporalDiscoveryContractError(
                "native v5 G0 cannot inherit a campaign identity ledger"
            )
        input_path: Path | None = None
        input_sha256: str | None = None
        input_binding = None
    elif generation_kind == V5_PROPOSAL_GENERATION_EVOLVED:
        root_path = root / "identity-ledger.json"
        _ledger, input_sha256 = _validated_native_v5_identity_ledger(
            root_path, name="committed native v5 campaign identity ledger"
        )
        if input_sha256 != committed_identity_ledger_sha256:
            raise TemporalDiscoveryContractError(
                "native v5 committed identity ledger drifted before construction"
            )
        input_path = _native_v5_identity_ledger_snapshot_path(root, generation_index)
        _publish_committed_file(root_path, input_path)
        _snapshot, snapshot_sha256 = _validated_native_v5_identity_ledger(
            input_path, name="native v5 frozen input identity ledger"
        )
        if snapshot_sha256 != input_sha256:
            raise TemporalDiscoveryContractError(
                "native v5 frozen input identity ledger drifted"
            )
        try:
            input_binding = build_v5_proposal_input_binding(
                kind="identityLedger",
                sealed_descriptor={
                    "absolutePath": str(input_path.resolve()),
                    "fileSha256": _native_binary_file_sha256(input_path),
                    "semanticSha256": input_sha256,
                    "byteLength": input_path.stat().st_size,
                },
            )
        except TemporalQDV5NativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
    else:
        raise TemporalDiscoveryContractError("native v5 identity-ledger kind is invalid")
    expected = {
        "schemaVersion": NATIVE_V5_IDENTITY_LEDGER_TRANSACTION_SCHEMA,
        "generationIndex": generation_index,
        "generationKind": generation_kind,
        "inputLedgerPath": str(input_path.resolve()) if input_path is not None else None,
        "inputLedgerSha256": input_sha256,
        "outputLedgerPath": str(output_path.resolve()),
        "phase": "generation_proposal",
    }
    existing = state.get("nativeV5IdentityLedgerTransaction")
    if existing is not None:
        if not isinstance(existing, Mapping):
            raise TemporalDiscoveryContractError("native v5 identity-ledger transaction is invalid")
        comparable = {
            key: value
            for key, value in dict(existing).items()
            if key
            not in {
                "outputLedgerSha256",
                "outputLedgerFileSha256",
                "constructionAdapterSha256",
                "phase",
            }
        }
        if comparable != {key: value for key, value in expected.items() if key != "phase"}:
            raise TemporalDiscoveryContractError(
                "native v5 identity-ledger transaction drifted"
            )
        if existing.get("phase") not in {"generation_proposal", "proposal_committed"}:
            raise TemporalDiscoveryContractError(
                "native v5 identity-ledger transaction cannot resume"
            )
        expected.update(
            {
                key: existing[key]
                for key in (
                    "outputLedgerSha256",
                    "outputLedgerFileSha256",
                    "constructionAdapterSha256",
                    "phase",
                )
                if key in existing
            }
        )
    state["nativeV5IdentityLedgerTransaction"] = expected
    _save_state(state_path, state)
    return input_binding


def _legacy_seal_native_v5_identity_ledger_output(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    generation_kind: str,
    construction_adapter: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    adapter = _validate_native_v5_construction_adapter(
        value=construction_adapter,
        proposal_root=_native_v5_proposal_root(root, generation_index),
        generation_index=generation_index,
        generation_kind=generation_kind,
    )
    output_path = _native_v5_identity_ledger_output_path(root, generation_index)
    output, output_sha256 = _validated_native_v5_identity_ledger(
        output_path, name="native v5 proposal output identity ledger"
    )
    if (
        adapter["identityLedger"]["absolutePath"] != str(output_path.resolve())
        or adapter["identityLedger"]["semanticSha256"] != output_sha256
        or adapter["identityLedger"]["fileSha256"]
        != _native_binary_file_sha256(output_path)
    ):
        raise TemporalDiscoveryContractError(
            "native v5 proposal output identity-ledger receipt disagrees"
        )
    transaction = state.get("nativeV5IdentityLedgerTransaction")
    if (
        not isinstance(transaction, Mapping)
        or transaction.get("generationIndex") != generation_index
        or transaction.get("generationKind") != generation_kind
        or transaction.get("phase") not in {"generation_proposal", "proposal_committed"}
    ):
        raise TemporalDiscoveryContractError(
            "native v5 identity ledger is not ready for proposal commit"
        )
    if transaction.get("phase") == "proposal_committed":
        if (
            transaction.get("outputLedgerSha256") != output_sha256
            or transaction.get("constructionAdapterSha256") != adapter["adapterSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "native v5 proposal commit transaction drifted"
            )
        return output, output_sha256
    state["nativeV5IdentityLedgerTransaction"] = {
        **dict(transaction),
        "outputLedgerSha256": output_sha256,
        "outputLedgerFileSha256": adapter["identityLedger"]["fileSha256"],
        "constructionAdapterSha256": adapter["adapterSha256"],
        "phase": "proposal_committed",
    }
    _save_state(state_path, state)
    return output, output_sha256


def _legacy_promote_native_v5_identity_ledger(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    generation_index: int,
    generation_record: Mapping[str, Any],
) -> None:
    transaction = state.get("nativeV5IdentityLedgerTransaction")
    adapter = _native_v5_recorded_adapter(root=root, generation_record=generation_record)
    if (
        not isinstance(transaction, Mapping)
        or transaction.get("generationIndex") != generation_index
        or transaction.get("phase") != "proposal_committed"
        or transaction.get("constructionAdapterSha256") != adapter["adapterSha256"]
    ):
        raise TemporalDiscoveryContractError(
            "native v5 identity ledger is not ready for generation promotion"
        )
    output_path = _native_v5_identity_ledger_output_path(root, generation_index)
    _output, output_sha256 = _validated_native_v5_identity_ledger(
        output_path, name="committing native v5 identity ledger"
    )
    if (
        transaction.get("outputLedgerSha256") != output_sha256
        or adapter["identityLedger"]["semanticSha256"] != output_sha256
    ):
        raise TemporalDiscoveryContractError(
            "native v5 identity-ledger promotion binding drifted"
        )
    state["nativeV5IdentityLedgerTransaction"] = {
        **dict(transaction),
        "phase": "generation_boundary_ready",
    }
    _save_state(state_path, state)
    _publish_committed_file(
        output_path, root / "identity-ledger.json", replace_existing=True
    )
    _promoted, promoted_sha256 = _validated_native_v5_identity_ledger(
        root / "identity-ledger.json", name="promoted native v5 identity ledger"
    )
    if promoted_sha256 != output_sha256:
        raise TemporalDiscoveryContractError(
            "native v5 identity-ledger promotion did not converge"
        )
    state.pop("nativeV5IdentityLedgerTransaction", None)
    _save_state(state_path, state)


def _native_v5_generation_state_basis(
    *, state: Mapping[str, Any], config: Mapping[str, Any], generation_index: int
) -> dict[str, Any]:
    """Build the small, candidate-free Rust state basis for one v5 boundary."""

    if isinstance(generation_index, bool) or not isinstance(generation_index, int) or generation_index < 1:
        raise TemporalDiscoveryContractError("native v5 state-basis generation is invalid")
    config_sha256 = _sha256(
        config.get("configSha256"), name="native v5 state-basis config identity"
    )
    if state.get("configSha256") != config_sha256:
        raise TemporalDiscoveryContractError("native v5 state-basis config drifted")
    completed = state.get("completedGenerations")
    if not isinstance(completed, list) or not all(
        isinstance(record, Mapping) for record in completed
    ):
        raise TemporalDiscoveryContractError("native v5 state-basis completed records are invalid")
    counters: dict[str, int] = {}
    for field in (
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
    ):
        value = state.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TemporalDiscoveryContractError(f"native v5 state-basis {field} is invalid")
        counters[field] = value
    mappings: dict[str, dict[str, Any]] = {}
    for field in (
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
    ):
        value = state.get(field)
        if not isinstance(value, Mapping):
            raise TemporalDiscoveryContractError(f"native v5 state-basis {field} is invalid")
        mappings[field] = _clone(value, name=f"native v5 state-basis {field}")
    basis = {
        "schemaVersion": "temporal_qd_v5_generation_state_basis_v1",
        "configSha256": config_sha256,
        "generationIndex": generation_index,
        "completedGenerationsSha256": canonical_sha256(completed),
        **counters,
        **mappings,
    }
    basis["stateBasisSha256"] = canonical_sha256(basis)
    return basis


def _native_v5_state_application_marker(
    value: object, *, generation_index: int | None = None
) -> dict[str, Any]:
    """Validate the compact descriptor handoff across the crash window."""

    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError("native v5 state-application marker is invalid")
    marker = _clone(value, name="native v5 state-application marker")
    if set(marker) != {
        "generationIndex",
        "sidecarSha256",
        "identityLedger",
        "phase",
    } or (
        isinstance(marker.get("generationIndex"), bool)
        or not isinstance(marker.get("generationIndex"), int)
        or marker["generationIndex"] < 1
        or marker.get("phase") not in {"pending", "ledger_promoted"}
    ):
        raise TemporalDiscoveryContractError("native v5 state-application marker drifted")
    _sha256(marker.get("sidecarSha256"), name="native v5 state-application marker")
    if generation_index is not None and marker["generationIndex"] != generation_index:
        raise TemporalDiscoveryContractError("native v5 state-application marker generation drifted")
    marker["identityLedger"] = _native_v5_identity_ledger_descriptor(
        marker["identityLedger"],
        name="native v5 state-application marker identity ledger",
    )
    return marker


def _native_v5_direct_finalization_payloads(
    *, finalization: Mapping[str, Any], generation_index: int
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Require the direct Rust record/patch/sidecar, never a legacy wrapper."""

    raw_record = finalization.get("generationRecord")
    raw_patch = finalization.get("statePatch")
    raw_sidecar = finalization.get("stateApplicationSidecar")
    if not all(isinstance(value, Mapping) for value in (raw_record, raw_patch, raw_sidecar)):
        raise TemporalDiscoveryContractError(
            "native v5 direct finalization omitted its compact state receipts"
        )
    record = _clone(raw_record, name="native v5 direct generation record")
    patch = _clone(raw_patch, name="native v5 direct generation state patch")
    sidecar = _clone(
        raw_sidecar, name="native v5 generation state-application sidecar"
    )
    if (
        record.get("schemaVersion") != GENERATION_RECORD_SCHEMA
        or _identity_payload(
            record, "generationRecordSha256", name="native v5 direct generation record"
        )
        != record.get("generationRecordSha256")
        or patch.get("schemaVersion") != GENERATION_STATE_PATCH_SCHEMA
        or _identity_payload(
            patch, "statePatchSha256", name="native v5 direct generation state patch"
        )
        != patch.get("statePatchSha256")
        or sidecar.get("schemaVersion") != GENERATION_STATE_APPLICATION_SIDECAR_SCHEMA
        or _identity_payload(
            sidecar,
            "sidecarSha256",
            name="native v5 generation state-application sidecar",
        )
        != sidecar.get("sidecarSha256")
        or record.get("generationIndex") != generation_index
        or patch.get("generationIndex") != generation_index
        or sidecar.get("generationIndex") != generation_index
        or patch.get("generationRecord") != record
        or patch.get("generationRecordSha256") != record.get("generationRecordSha256")
    ):
        raise TemporalDiscoveryContractError(
            "native v5 direct generation state-application chain drifted"
        )
    return record, patch, sidecar


def _validate_native_v5_state_application(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
    generation_index: int,
    generation_kind: str,
    finalization: Mapping[str, Any],
    construction_adapter: Mapping[str, Any] | None,
    require_pre_state: bool,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any] | None,
]:
    """Cross-check Rust's sidecar against the exact proposal/state boundary.

    This is intentionally a compact authority check.  The Rust finalizer has
    already validated the native source and state patch; Python only compares
    immutable roots, a compact ledger identity, and absolute state values.
    """

    if generation_kind not in {
        V5_PROPOSAL_GENERATION_G0,
        V5_PROPOSAL_GENERATION_EVOLVED,
    }:
        raise TemporalDiscoveryContractError("native v5 state-application kind is invalid")
    record, patch, sidecar = _native_v5_direct_finalization_payloads(
        finalization=finalization, generation_index=generation_index
    )
    expected_fields = {
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
        set(sidecar) != expected_fields
        or sidecar.get("contractVersion") != NATIVE_FOUNDATION_CONTRACT_VERSION
        or sidecar.get("generationKind") != generation_kind
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application sidecar schema drifted"
        )
    basis = _native_v5_generation_state_basis(
        state=state, config=config, generation_index=generation_index
    )
    if require_pre_state and (
        sidecar.get("configSha256") != basis["configSha256"]
        or sidecar.get("stateBasisSha256") != basis["stateBasisSha256"]
        or sidecar.get("completedGenerationsBeforeSha256")
        != basis["completedGenerationsSha256"]
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application state basis drifted"
        )
    finalization_binding = sidecar.get("finalization")
    if not isinstance(finalization_binding, Mapping) or set(finalization_binding) != {
        "sourceSha256",
        "manifestSha256",
        "commitSha256",
        "generationRecordSha256",
        "statePatchSha256",
    }:
        raise TemporalDiscoveryContractError(
            "native v5 state-application finalization binding is invalid"
        )
    for field, value in finalization_binding.items():
        _sha256(value, name=f"native v5 state-application finalization {field}")
    manifest = finalization.get("manifest")
    commit = finalization.get("commit")
    if (
        not isinstance(manifest, Mapping)
        or not isinstance(commit, Mapping)
        or finalization_binding.get("sourceSha256")
        != finalization.get("sourceSha256")
        or finalization_binding.get("sourceSha256") != manifest.get("sourceSha256")
        or finalization_binding.get("sourceSha256") != commit.get("sourceSha256")
        or finalization_binding.get("manifestSha256") != manifest.get("manifestSha256")
        or finalization_binding.get("commitSha256") != commit.get("commitSha256")
        or finalization_binding.get("generationRecordSha256")
        != record.get("generationRecordSha256")
        or finalization_binding.get("statePatchSha256") != patch.get("statePatchSha256")
        or sidecar.get("semanticAuthoritySha256")
        != manifest.get("semanticAuthoritySha256")
        or sidecar.get("runtimeAuthoritySha256")
        != manifest.get("runtimeAuthoritySha256")
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application finalization authority drifted"
        )

    proposal_authority = sidecar.get("proposalStateAuthority")
    if not isinstance(proposal_authority, Mapping) or set(proposal_authority) != {
        "proposalManifestSha256",
        "proposalReceiptSha256",
        "generationJournalSha256",
    }:
        raise TemporalDiscoveryContractError(
            "native v5 state-application proposal authority is invalid"
        )
    for field, value in proposal_authority.items():
        _sha256(value, name=f"native v5 proposal state authority {field}")
    output_ledger_descriptor: dict[str, Any] | None = None
    if construction_adapter is not None:
        adapter = _validate_native_v5_construction_adapter(
            value=construction_adapter,
            proposal_root=_native_v5_proposal_root(root, generation_index),
            generation_index=generation_index,
            generation_kind=generation_kind,
        )
        invocation = adapter.get("nativeV5Invocation")
        if not isinstance(invocation, Mapping) or not isinstance(
            invocation.get("proposalManifest"), Mapping
        ):
            raise TemporalDiscoveryContractError(
                "native v5 construction lacks its proposal invocation authority"
            )
        if (
            proposal_authority.get("proposalManifestSha256")
            != invocation["proposalManifest"].get("semanticSha256")
            or proposal_authority.get("proposalReceiptSha256")
            != adapter.get("proposalReceiptSha256")
            or proposal_authority.get("generationJournalSha256")
            != adapter.get("generationJournal", {}).get("semanticSha256")
        ):
            raise TemporalDiscoveryContractError(
                "native v5 state-application proposal roots drifted"
            )
        output_ledger_descriptor = _native_v5_identity_ledger_descriptor_from_adapter(
            adapter=adapter,
            root=root,
            generation_index=generation_index,
            name="native v5 state-application proposal output ledger",
        )

    next_state = sidecar.get("nextState")
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
    if not isinstance(next_state, Mapping) or set(next_state) != expected_next_state_fields:
        raise TemporalDiscoveryContractError("native v5 state-application next state is invalid")
    if (
        next_state.get("stage") != "generation_proposal"
        or next_state.get("currentGenerationIndex") != patch.get("nextGenerationIndex")
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
        raise TemporalDiscoveryContractError(
            "native v5 state-application absolute next state drifted"
        )
    for field in (
        "currentGenerationIndex",
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
    ):
        value = next_state.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TemporalDiscoveryContractError(
                f"native v5 state-application {field} is invalid"
            )
    for field in ("uniqueIdentityCounts", "duplicateCounters", "proposalSlotCounters"):
        if not isinstance(next_state.get(field), Mapping):
            raise TemporalDiscoveryContractError(
                f"native v5 state-application {field} is invalid"
            )

    promotion = sidecar.get("identityLedgerPromotion")
    expected_promotion_fields = {
        "inputIdentityLedgerSha256",
        "outputRelativePath",
        "outputIdentityLedgerSha256",
        "outputIdentityLedgerFileSha256",
    }
    if (
        not isinstance(promotion, Mapping)
        or set(promotion) != expected_promotion_fields
        or promotion.get("outputRelativePath")
        != "proposal/v5-native/identity-ledger.json"
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application identity-ledger promotion is invalid"
        )
    input_ledger_sha256 = promotion.get("inputIdentityLedgerSha256")
    if generation_kind == V5_PROPOSAL_GENERATION_G0:
        if input_ledger_sha256 is not None:
            raise TemporalDiscoveryContractError(
                "native v5 G0 state application unexpectedly has an input ledger"
            )
        if (
            construction_adapter is not None
            and state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY) is not None
        ):
            raise TemporalDiscoveryContractError(
                "native v5 G0 state application has a prior committed ledger"
            )
    else:
        _sha256(input_ledger_sha256, name="native v5 state-application input ledger")
        if construction_adapter is not None:
            committed_input = _native_v5_identity_ledger_descriptor(
                state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY),
                name="native v5 state-application committed input ledger",
            )
            if committed_input["semanticSha256"] != input_ledger_sha256:
                raise TemporalDiscoveryContractError(
                    "native v5 state-application input ledger descriptor drifted"
                )
    output_ledger_sha256 = _sha256(
        promotion.get("outputIdentityLedgerSha256"),
        name="native v5 state-application output ledger",
    )
    output_ledger_file_sha256 = _sha256(
        promotion.get("outputIdentityLedgerFileSha256"),
        name="native v5 state-application output ledger file",
    )
    if output_ledger_descriptor is not None and (
        output_ledger_descriptor["semanticSha256"] != output_ledger_sha256
        or output_ledger_descriptor["fileSha256"] != output_ledger_file_sha256
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application output ledger descriptor drifted"
        )
    if state.get("nativeV5IdentityLedgerTransaction") is not None:
        raise TemporalDiscoveryContractError(
            "current native v5 rejects legacy identity-ledger transactions"
        )
    return record, patch, sidecar, dict(next_state), output_ledger_descriptor


def _apply_native_v5_state_application(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    config: Mapping[str, Any],
    generation_index: int,
    generation_kind: str,
    finalization: Mapping[str, Any],
    construction_adapter: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Mechanically apply a receipt-bound Rust v5 state boundary.

    The only mutable bridge is a three-step crash-safe authority transaction:
    save a compact pending marker, promote the immutable ledger *descriptor*,
    then append the untouched Rust record and absolute state patch fields.
    Ledger bytes stay in the native proposal tree and are never copied or
    opened by Python.
    """

    record, _patch, sidecar, next_state, adapter_ledger_descriptor = (
        _validate_native_v5_state_application(
        root=root,
        state=state,
        config=config,
        generation_index=generation_index,
        generation_kind=generation_kind,
        finalization=finalization,
        construction_adapter=construction_adapter,
        require_pre_state=True,
        )
    )
    if any(
        isinstance(existing, Mapping)
        and existing.get("generationIndex") == generation_index
        for existing in state.get("completedGenerations") or []
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state application would duplicate a completed generation"
        )
    existing_marker = state.get(NATIVE_V5_STATE_APPLICATION_PENDING_KEY)
    if adapter_ledger_descriptor is None:
        if existing_marker is None:
            raise TemporalDiscoveryContractError(
                "native v5 state-application recovery lacks its ledger descriptor marker"
            )
        marker = _native_v5_state_application_marker(
            existing_marker, generation_index=generation_index
        )
        if marker["sidecarSha256"] != sidecar["sidecarSha256"]:
            raise TemporalDiscoveryContractError(
                "native v5 state-application recovery marker drifted"
            )
        ledger_descriptor = marker["identityLedger"]
    else:
        ledger_descriptor = adapter_ledger_descriptor
        marker = {
            "generationIndex": generation_index,
            "sidecarSha256": sidecar["sidecarSha256"],
            "identityLedger": ledger_descriptor,
            "phase": "pending",
        }
    promotion = sidecar["identityLedgerPromotion"]
    if (
        ledger_descriptor["absolutePath"]
        != str(_native_v5_identity_ledger_output_path(root, generation_index).resolve())
        or ledger_descriptor["semanticSha256"]
        != promotion["outputIdentityLedgerSha256"]
        or ledger_descriptor["fileSha256"]
        != promotion["outputIdentityLedgerFileSha256"]
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application ledger descriptor binding drifted"
        )
    if existing_marker is None:
        state[NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = marker
        _save_state(state_path, state)
    else:
        marker = _native_v5_state_application_marker(
            existing_marker, generation_index=generation_index
        )
        if (
            marker["sidecarSha256"] != sidecar["sidecarSha256"]
            or marker["identityLedger"] != ledger_descriptor
        ):
            raise TemporalDiscoveryContractError("native v5 state-application marker drifted")

    if marker["phase"] == "pending":
        state[NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY] = ledger_descriptor
        state[NATIVE_V5_STATE_APPLICATION_PENDING_KEY] = {
            **marker,
            "phase": "ledger_promoted",
        }
        _save_state(state_path, state)
    else:
        current_descriptor = _native_v5_identity_ledger_descriptor(
            state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY),
            name="native v5 state-application promoted ledger descriptor",
            expected_path=_native_v5_identity_ledger_output_path(root, generation_index),
        )
        if current_descriptor != ledger_descriptor:
            raise TemporalDiscoveryContractError(
                "native v5 state-application promoted ledger descriptor drifted"
            )

    completed = list(state.get("completedGenerations") or [])
    expected_completed_sha256 = canonical_sha256([*completed, record])
    if next_state["completedGenerationsSha256"] != expected_completed_sha256:
        raise TemporalDiscoveryContractError(
            "native v5 state-application completed generation root drifted"
        )
    state.update(
        {
            "stage": next_state["stage"],
            "currentGenerationIndex": next_state["currentGenerationIndex"],
            "uniqueCandidatesEvaluated": next_state["uniqueCandidatesEvaluated"],
            "workerTasksCompleted": next_state["workerTasksCompleted"],
            "nextImmigrantContinuationOrdinal": next_state[
                "nextImmigrantContinuationOrdinal"
            ],
            "uniqueIdentityCounts": _clone(
                next_state["uniqueIdentityCounts"],
                name="native v5 state-application unique identity counts",
            ),
            "duplicateCounters": _clone(
                next_state["duplicateCounters"],
                name="native v5 state-application duplicate counters",
            ),
            "proposalSlotCounters": _clone(
                next_state["proposalSlotCounters"],
                name="native v5 state-application proposal slot counters",
            ),
            "completedGenerations": [*completed, record],
            NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY: ledger_descriptor,
        }
    )
    state.pop(NATIVE_V5_STATE_APPLICATION_PENDING_KEY, None)
    _save_state(state_path, state)
    return record


def _recover_native_v5_state_application(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    config: Mapping[str, Any],
    runtime_authority: Mapping[str, Any],
) -> bool:
    """Finish the sole permitted v5 crash window without rebuilding work.

    A marker may represent the old state with either the old or newly promoted
    ledger, or an already-applied state whose marker-clear was interrupted.
    In all cases the Rust finalizer is re-run first, which performs its own
    compact restart validation before Python changes supervisor state.
    """

    raw_marker = state.get(NATIVE_V5_STATE_APPLICATION_PENDING_KEY)
    if raw_marker is None:
        return False
    marker = _native_v5_state_application_marker(raw_marker)
    generation_index = marker["generationIndex"]
    manifest_path = _native_finalization_root(root, generation_index) / "manifest.json"
    try:
        finalization = run_native_v5_generation_finalizer(
            runtime_authority=runtime_authority,
            manifest_path=manifest_path,
        )
    except TemporalQDV5ControlPlaneError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    record, _patch, sidecar = _native_v5_direct_finalization_payloads(
        finalization=finalization, generation_index=generation_index
    )
    if sidecar.get("sidecarSha256") != marker["sidecarSha256"]:
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery sidecar drifted"
        )
    completed = state.get("completedGenerations")
    if not isinstance(completed, list):
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery completed records are invalid"
        )
    matching = [
        item
        for item in completed
        if isinstance(item, Mapping) and item.get("generationIndex") == generation_index
    ]
    if not matching:
        _apply_native_v5_state_application(
            root=root,
            state=state,
            state_path=state_path,
            config=config,
            generation_index=generation_index,
            generation_kind=str(sidecar.get("generationKind") or ""),
            finalization=finalization,
            construction_adapter=None,
        )
        return True
    if len(matching) != 1 or matching[0] != record:
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery completed record drifted"
        )
    next_state = sidecar.get("nextState")
    promotion = sidecar.get("identityLedgerPromotion")
    if not isinstance(next_state, Mapping) or not isinstance(promotion, Mapping):
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery receipt is malformed"
        )
    if (
        canonical_sha256(completed) != next_state.get("completedGenerationsSha256")
        or any(
            state.get(field) != next_state.get(field)
            for field in (
                "currentGenerationIndex",
                "uniqueCandidatesEvaluated",
                "workerTasksCompleted",
                "nextImmigrantContinuationOrdinal",
                "uniqueIdentityCounts",
                "duplicateCounters",
                "proposalSlotCounters",
            )
        )
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery applied state drifted"
        )
    descriptor = _native_v5_identity_ledger_descriptor(
        state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY),
        name="native v5 recovered committed ledger descriptor",
        expected_path=_native_v5_identity_ledger_output_path(root, generation_index),
    )
    if (
        marker.get("phase") != "ledger_promoted"
        or marker.get("identityLedger") != descriptor
        or descriptor["semanticSha256"]
        != promotion.get("outputIdentityLedgerSha256")
        or descriptor["fileSha256"]
        != promotion.get("outputIdentityLedgerFileSha256")
    ):
        raise TemporalDiscoveryContractError(
            "native v5 state-application recovery ledger descriptor drifted"
        )
    state.pop(NATIVE_V5_STATE_APPLICATION_PENDING_KEY, None)
    _save_state(state_path, state)
    return True


def _admit_completed_generations_native_v5(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[int, dict[str, Any]]:
    """Replay direct Rust v5 boundaries without legacy record wrapping.

    A current-v5 record is an exact ``temporal_qd_generation_record_v2`` from
    the finalizer.  The receipt-last sidecar is the sole state-application
    authority, so this admission never invokes the historical v4 artifact
    validator or invents ``nativeGenerationFinalization`` around the record.
    """

    completed = state.get("completedGenerations") or []
    if not isinstance(completed, list):
        raise TemporalDiscoveryContractError("native v5 completed records are invalid")
    records: dict[int, dict[str, Any]] = {}
    for raw_record in completed:
        if not isinstance(raw_record, Mapping):
            raise TemporalDiscoveryContractError("native v5 completed record is invalid")
        record = _clone(raw_record, name="native v5 completed generation record")
        if (
            record.get("schemaVersion") != GENERATION_RECORD_SCHEMA
            or "nativeGenerationFinalization" in record
            or _identity_payload(
                record,
                "generationRecordSha256",
                name="native v5 completed generation record",
            )
            != record.get("generationRecordSha256")
        ):
            raise TemporalDiscoveryContractError(
                "native v5 completed record is not the exact Rust finalizer record"
            )
        index = record.get("generationIndex")
        if isinstance(index, bool) or not isinstance(index, int) or index < 1:
            raise TemporalDiscoveryContractError("native v5 completed record index is invalid")
        if index in records:
            raise TemporalDiscoveryContractError("native v5 completed record index repeats")
        records[index] = record

    first = int(config["generationPlan"]["firstGenerationIndex"])
    last = int(config["generationPlan"]["lastGenerationIndex"])
    if any(index < first or index > last for index in records):
        raise TemporalDiscoveryContractError("native v5 completed record is outside bounds")
    if records:
        latest = max(records)
        if set(records) != set(range(first, latest + 1)):
            raise TemporalDiscoveryContractError("native v5 completed records are not contiguous")

    latest_sidecar: Mapping[str, Any] | None = None
    for generation_index in sorted(records):
        record = records[generation_index]
        candidate_count = record.get("candidateCount")
        task_count = record.get("totalGenerationTaskCount")
        if (
            isinstance(candidate_count, bool)
            or not isinstance(candidate_count, int)
            or candidate_count < 0
            or isinstance(task_count, bool)
            or not isinstance(task_count, int)
            or task_count < 0
        ):
            raise TemporalDiscoveryContractError(
                "native v5 completed record counters are invalid"
            )
        try:
            runtime_authority = _native_runtime_authority_for_generation(
                root=root, generation_index=generation_index
            )
            finalization = run_native_v5_generation_finalizer(
                runtime_authority=runtime_authority,
                manifest_path=_native_finalization_root(root, generation_index)
                / "manifest.json",
                committed_restart_only=True,
            )
        except TemporalQDV5ControlPlaneError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
        reopened, patch, sidecar = _native_v5_direct_finalization_payloads(
            finalization=finalization, generation_index=generation_index
        )
        if reopened != record:
            raise TemporalDiscoveryContractError(
                "native v5 finalizer replay disagrees with completed record"
            )
        expected_kind = (
            V5_PROPOSAL_GENERATION_G0
            if generation_index == 1 and isinstance(config.get("g0Bootstrap"), Mapping)
            else V5_PROPOSAL_GENERATION_EVOLVED
        )
        if sidecar.get("generationKind") != expected_kind:
            raise TemporalDiscoveryContractError(
                "native v5 finalizer replay generation kind drifted"
            )
        previous_records = [records[index] for index in range(first, generation_index)]
        if (
            sidecar.get("completedGenerationsBeforeSha256")
            != canonical_sha256(previous_records)
            or patch.get("completedGenerationsSha256")
            != canonical_sha256([*previous_records, record])
            or sidecar.get("nextState", {}).get("completedGenerationsSha256")
            != patch.get("completedGenerationsSha256")
            or record.get("archivePath") != "archive.json"
            or record.get("archiveSha256")
            != finalization.get("artifacts", {}).get("parentArchive", {}).get(
                "semanticSha256"
            )
            or record.get("cumulativeArchiveSha256")
            != finalization.get("artifacts", {}).get("cumulativeArchive", {}).get(
                "semanticSha256"
            )
        ):
            raise TemporalDiscoveryContractError(
                "native v5 finalizer replay boundary drifted"
            )
        latest_sidecar = sidecar

    if latest_sidecar is not None:
        next_state = latest_sidecar.get("nextState")
        if not isinstance(next_state, Mapping) or any(
            state.get(field) != next_state.get(field)
            for field in (
                "currentGenerationIndex",
                "uniqueCandidatesEvaluated",
                "workerTasksCompleted",
                "nextImmigrantContinuationOrdinal",
                "uniqueIdentityCounts",
                "duplicateCounters",
                "proposalSlotCounters",
            )
        ):
            raise TemporalDiscoveryContractError(
                "native v5 direct Rust state does not match the latest sidecar"
            )
    return records


def _native_self_hash(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    output = _clone(value, name="native finalization identity material")
    if field in output:
        raise TemporalDiscoveryContractError(
            f"native identity material already contains {field}"
        )
    output[field] = canonical_sha256(output)
    return output


def _native_binary_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise TemporalDiscoveryContractError(
            f"could not hash native finalization binary: {path}"
        ) from exc
    return "sha256:" + digest.hexdigest()


def _native_finalization_runtime_authority(
    finalizer_binary: Path, *, require_v5_control_plane_roles: bool = False
) -> dict[str, Any]:
    """Freeze the exact native binaries for one finalization authority epoch.

    Historical/v4 boundaries retain their original three-role authority shape.
    Fresh native-v5 control planes deliberately freeze every subprocess role up
    front: a missing or substituted binary is a contract failure, never a
    reason to fall back to a Python phase.
    """

    suffix = ".exe" if os.name == "nt" else ""
    binaries = {
        "campaignSeal": finalizer_binary.with_name(
            f"temporal-qd-campaign-seal{suffix}"
        ),
        "tailReducer": finalizer_binary.with_name(
            f"temporal-qd-tail-reducer{suffix}"
        ),
        "generationFinalizer": finalizer_binary,
    }
    if require_v5_control_plane_roles:
        binaries = {
            "campaignFreeze": finalizer_binary.with_name(
                f"temporal-qd-campaign-freeze{suffix}"
            ),
            "gatewayDispatch": finalizer_binary.with_name(
                f"temporal-qd-gateway-dispatch{suffix}"
            ),
            **binaries,
            "rotatingPrefinalizer": finalizer_binary.with_name(
                f"temporal-qd-rotating-prefinalizer{suffix}"
            ),
            "archiveReducer": finalizer_binary.with_name(
                f"temporal-qd-archive-reducer{suffix}"
            ),
        }
    descriptors: dict[str, Any] = {}
    for role, binary in binaries.items():
        resolved = binary.resolve()
        if not resolved.is_file():
            raise TemporalDiscoveryContractError(
                f"native finalization {role} binary is unavailable: {resolved}"
            )
        descriptors[role] = {
            "path": str(resolved),
            "bytes": resolved.stat().st_size,
            "fileSha256": _native_binary_file_sha256(resolved),
        }
    return _native_self_hash(
        {
            "schemaVersion": NATIVE_FINALIZATION_RUNTIME_AUTHORITY_SCHEMA,
            "generationFinalizationEngine": GENERATION_FINALIZATION_ENGINE_RUST,
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "binaries": descriptors,
        },
        "authoritySha256",
    )


def _require_native_v5_control_plane_runtime_authority(
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the complete runtime role set before opening a native-v5 phase."""

    checked = _validate_native_runtime_authority(authority)
    binaries = checked.get("binaries")
    if not isinstance(binaries, Mapping) or set(binaries) != NATIVE_V5_CONTROL_PLANE_RUNTIME_ROLES:
        raise TemporalDiscoveryContractError(
            "native v5 runtime authority lacks the complete control-plane binary set"
        )
    for role in NATIVE_V5_CONTROL_PLANE_RUNTIME_ROLES:
        descriptor = binaries.get(role)
        if not isinstance(descriptor, Mapping) or set(descriptor) != {
            "path",
            "bytes",
            "fileSha256",
        }:
            raise TemporalDiscoveryContractError(
                f"native v5 runtime authority {role} descriptor is malformed"
            )
    return checked


def _native_runtime_authority_history_path(root: Path, authority_sha256: str) -> Path:
    authority_sha256 = _sha256(
        authority_sha256, name="native finalization runtime authority"
    )
    return (
        root
        / NATIVE_FINALIZATION_AUTHORITY_HISTORY_DIR
        / f"{authority_sha256.removeprefix('sha256:')}.json"
    )


def _validate_native_runtime_authority(
    authority: Mapping[str, Any], *, expected_sha256: str | None = None
) -> dict[str, Any]:
    checked = _clone(authority, name="native finalization runtime authority")
    supplied = _identity_payload(
        checked,
        "authoritySha256",
        name="native finalization runtime authority",
    )
    material = dict(checked)
    material.pop("authoritySha256", None)
    if (
        checked.get("schemaVersion") != NATIVE_FINALIZATION_RUNTIME_AUTHORITY_SCHEMA
        or canonical_sha256(material) != supplied
        or (expected_sha256 is not None and supplied != expected_sha256)
    ):
        raise TemporalDiscoveryContractError(
            "native finalization runtime authority identity drifted"
        )
    return checked


def _load_native_runtime_authority(
    *, root: Path, authority_sha256: str
) -> dict[str, Any]:
    expected = _sha256(
        authority_sha256, name="native finalization runtime authority"
    )
    current_path = root / "native-finalization-authority.json"
    history_path = _native_runtime_authority_history_path(root, expected)
    for path in (current_path, history_path):
        if not path.is_file():
            continue
        authority = _canonical_file(
            path, name="native finalization runtime authority"
        )
        if authority.get("authoritySha256") != expected:
            if path == current_path:
                continue
            raise TemporalDiscoveryContractError(
                "historical native finalization authority path is misbound"
            )
        return _validate_native_runtime_authority(
            authority, expected_sha256=expected
        )
    raise TemporalDiscoveryContractError(
        "native finalization runtime authority epoch is unavailable"
    )


def _pinned_native_authority_binary(
    *, root: Path, authority_sha256: str, role: str
) -> Path:
    authority = _load_native_runtime_authority(
        root=root, authority_sha256=authority_sha256
    )
    descriptor = (authority.get("binaries") or {}).get(role)
    if not isinstance(descriptor, Mapping):
        raise TemporalDiscoveryContractError(
            f"native finalization authority lacks the {role} binary"
        )
    binary = Path(str(descriptor.get("path") or "")).resolve()
    if (
        not binary.is_file()
        or descriptor.get("bytes") != binary.stat().st_size
        or descriptor.get("fileSha256") != _native_binary_file_sha256(binary)
    ):
        raise TemporalDiscoveryContractError(
            f"native finalization {role} binary identity drifted"
        )
    return binary


def _native_runtime_authority_for_generation(
    *, root: Path, generation_index: int
) -> dict[str, Any]:
    if generation_index < 1:
        raise TemporalDiscoveryContractError(
            "native finalization authority generation must be positive"
        )
    root_authority = _validate_native_runtime_authority(
        _canonical_file(
            root / "native-finalization-authority.json",
            name="native finalization root runtime authority",
        )
    )
    active = root_authority
    chain_sha256 = root_authority["authoritySha256"]
    prior_effective_generation = 0
    rotation_root = (
        root / NATIVE_FINALIZATION_AUTHORITY_HISTORY_DIR / "rotations"
    )
    rotations: list[dict[str, Any]] = []
    if rotation_root.is_dir():
        for path in sorted(rotation_root.glob("*.json")):
            rotation = _canonical_file(
                path, name="native finalization authority rotation"
            )
            supplied = _identity_payload(
                rotation,
                "rotationSha256",
                name="native finalization authority rotation",
            )
            material = dict(rotation)
            material.pop("rotationSha256", None)
            if (
                rotation.get("schemaVersion")
                != NATIVE_FINALIZATION_AUTHORITY_ROTATION_SCHEMA
                or rotation.get("operation")
                != "activate_successor_runtime_authority"
                or canonical_sha256(material) != supplied
            ):
                raise TemporalDiscoveryContractError(
                    "native finalization authority rotation identity drifted"
                )
            for field_name in (
                "previousAuthoritySha256",
                "nextAuthoritySha256",
                "configSha256",
                "stateSha256",
            ):
                _sha256(
                    rotation.get(field_name),
                    name=f"native finalization authority rotation {field_name}",
                )
            attempt = rotation.get("supersededIncompleteAttempt")
            if not isinstance(attempt, Mapping):
                raise TemporalDiscoveryContractError(
                    "native finalization authority rotation lacks its superseded attempt"
                )
            attempt_material = dict(attempt)
            attempt_sha256 = _sha256(
                attempt_material.pop("descriptorSha256", None),
                name="native superseded incomplete attempt",
            )
            if (
                canonical_sha256(attempt_material) != attempt_sha256
                or attempt.get("generationIndex")
                != rotation.get("activeFromGenerationIndex")
                or attempt.get("runtimeAuthoritySha256")
                != rotation.get("previousAuthoritySha256")
            ):
                raise TemporalDiscoveryContractError(
                    "native finalization superseded attempt identity drifted"
                )
            rotations.append(rotation)
    rotations.sort(
        key=lambda value: (
            int(value.get("activeFromGenerationIndex") or -1),
            str(value.get("nextAuthoritySha256") or ""),
        )
    )
    for rotation in rotations:
        effective_generation = int(
            rotation.get("activeFromGenerationIndex") or -1
        )
        if (
            effective_generation <= prior_effective_generation
            or rotation.get("previousAuthoritySha256") != chain_sha256
        ):
            raise TemporalDiscoveryContractError(
                "native finalization authority rotation chain drifted"
            )
        successor = _load_native_runtime_authority(
            root=root,
            authority_sha256=_sha256(
                rotation.get("nextAuthoritySha256"),
                name="native finalization successor authority",
            ),
        )
        chain_sha256 = successor["authoritySha256"]
        prior_effective_generation = effective_generation
        if effective_generation <= generation_index:
            active = successor
    return active


def _freeze_native_finalization_runtime_authority(
    *,
    root: Path,
    finalizer_binary: Path,
    state: Mapping[str, Any],
    authorized_adoption_generations: frozenset[int] = frozenset(),
    authorize_rotation: bool = False,
    require_v5_control_plane_roles: bool = False,
) -> dict[str, Any]:
    authority = _native_finalization_runtime_authority(
        finalizer_binary,
        require_v5_control_plane_roles=require_v5_control_plane_roles,
    )
    authority_path = root / "native-finalization-authority.json"
    if not authority_path.exists() and state.get("completedGenerations"):
        unbound_generations = {
            int(record.get("generationIndex") or -1)
            for record in state["completedGenerations"]
            if not isinstance(record, Mapping)
            or not isinstance(record.get("nativeGenerationFinalization"), Mapping)
        }
        if not unbound_generations.issubset(authorized_adoption_generations):
            raise TemporalDiscoveryContractError(
                "existing run cannot adopt Rust binary authority without native completed boundaries"
            )
    if authority_path.is_file():
        previous = _validate_native_runtime_authority(
            _canonical_file(
                authority_path, name="native finalization runtime authority"
            )
        )
        _write_once(
            _native_runtime_authority_history_path(
                root, previous["authoritySha256"]
            ),
            previous,
        )
        current_generation = int(state.get("currentGenerationIndex") or 1)
        active = _native_runtime_authority_for_generation(
            root=root, generation_index=current_generation
        )
        if active != authority:
            if not authorize_rotation:
                raise TemporalDiscoveryContractError(
                    "native finalization binary identity drifted from frozen authority"
                )
            if current_generation < 1 or any(
                int(record.get("generationIndex") or -1) >= current_generation
                for record in state.get("completedGenerations") or []
                if isinstance(record, Mapping)
            ):
                raise TemporalDiscoveryContractError(
                    "native finalization authority rotation is not at an unpublished generation"
                )
            current_commit = (
                _native_finalization_root(root, current_generation)
                / "generation-commit.json"
            )
            if current_commit.is_file():
                raise TemporalDiscoveryContractError(
                    "native finalization authority rotation cannot cross a committed generation"
                )
            _write_once(
                _native_runtime_authority_history_path(
                    root, authority["authoritySha256"]
                ),
                authority,
            )
            superseded_attempt = _native_incomplete_attempt_descriptor(
                root=root,
                generation_index=current_generation,
                authority_sha256=active["authoritySha256"],
            )
            rotation = _native_self_hash(
                {
                    "schemaVersion": NATIVE_FINALIZATION_AUTHORITY_ROTATION_SCHEMA,
                    "operation": "activate_successor_runtime_authority",
                    "previousAuthoritySha256": active["authoritySha256"],
                    "nextAuthoritySha256": authority["authoritySha256"],
                    "activeFromGenerationIndex": current_generation,
                    "configSha256": _sha256(
                        state.get("configSha256"),
                        name="native finalization authority rotation config",
                    ),
                    "stateSha256": _sha256(
                        state.get("stateSha256"),
                        name="native finalization authority rotation state",
                    ),
                    "supersededIncompleteAttempt": superseded_attempt,
                },
                "rotationSha256",
            )
            _write_once(
                root
                / NATIVE_FINALIZATION_AUTHORITY_HISTORY_DIR
                / "rotations"
                / f"{authority['authoritySha256'].removeprefix('sha256:')}.json",
                rotation,
            )
    else:
        _write_once(authority_path, authority)
        _write_once(
            _native_runtime_authority_history_path(
                root, authority["authoritySha256"]
            ),
            authority,
        )
    frozen = _native_runtime_authority_for_generation(
        root=root,
        generation_index=int(state.get("currentGenerationIndex") or 1),
    )
    if frozen != authority:
        raise TemporalDiscoveryContractError(
            "native finalization binary identity drifted from frozen authority"
        )
    return frozen


def _python_boundary_adoption_descriptor(
    generation_record: Mapping[str, Any],
) -> dict[str, Any]:
    generation_index = int(generation_record.get("generationIndex") or -1)
    if generation_index < 1:
        raise TemporalDiscoveryContractError(
            "Python boundary adoption has an invalid generation index"
        )
    material = {
        key: value
        for key, value in generation_record.items()
        if key != "nativeGenerationFinalization"
    }
    identities: dict[str, str] = {}
    for field in (
        "archiveSha256",
        "resultSetSha256",
        "rotatingEvidenceLedgerSha256",
        "rotatingEvidenceCheckpointSha256",
        "cumulativeArchiveSha256",
        "generationFunnelArtifactSha256",
        "generationFunnelSnapshotSha256",
    ):
        identities[field] = _sha256(
            generation_record.get(field),
            name=f"Python generation {generation_index} {field}",
        )
    return {
        "generationIndex": generation_index,
        "pythonGenerationRecordSha256": canonical_sha256(material),
        "artifactIdentities": identities,
    }


def _prepare_native_finalization_adoption_authority(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
    finalizer_binary: Path,
    requested_generations: tuple[int, ...],
) -> dict[str, Any] | None:
    """Freeze an explicit, one-time authority for adopting Python boundaries.

    The authority is operational rather than part of the research identity.  It
    makes a crash-resumable operator decision durable while binding the exact
    pre-adoption generation records, their critical artifact identities, the
    frozen run config, and the complete native binary authority.
    """

    records = _validate_completed_generation_ledger(state=state, config=config)
    unbound = {
        index
        for index, record in records.items()
        if not isinstance(record.get("nativeGenerationFinalization"), Mapping)
    }
    requested = tuple(sorted(set(requested_generations)))
    if any(index < 1 for index in requested):
        raise TemporalDiscoveryContractError(
            "Python boundary adoption generations must be positive"
        )
    authority_path = root / NATIVE_FINALIZATION_ADOPTION_AUTHORITY_FILE
    if not authority_path.is_file():
        if not requested:
            if unbound:
                raise TemporalDiscoveryContractError(
                    "Rust resume has Python-completed boundaries; explicitly authorize "
                    "their one-time adoption"
                )
            return None
        if set(requested) != unbound:
            raise TemporalDiscoveryContractError(
                "Python boundary adoption must name exactly every unbound completed generation"
            )
        runtime_authority = _native_finalization_runtime_authority(finalizer_binary)
        authority = _native_self_hash(
            {
                "schemaVersion": NATIVE_FINALIZATION_ADOPTION_AUTHORITY_SCHEMA,
                "operation": "adopt_exact_python_completed_boundaries_once",
                "configSha256": _sha256(
                    config.get("configSha256"), name="Python boundary adoption config"
                ),
                "runtimeAuthoritySha256": runtime_authority["authoritySha256"],
                "generationIndices": list(requested),
                "boundaries": [
                    _python_boundary_adoption_descriptor(records[index])
                    for index in requested
                ],
            },
            "authoritySha256",
        )
        _write_once(authority_path, authority)
    authority = _canonical_file(
        authority_path, name="Python boundary adoption authority"
    )
    if authority.get("schemaVersion") != NATIVE_FINALIZATION_ADOPTION_AUTHORITY_SCHEMA:
        raise TemporalDiscoveryContractError(
            "Python boundary adoption authority schema drifted"
        )
    supplied_sha256 = _identity_payload(
        authority,
        "authoritySha256",
        name="Python boundary adoption authority",
    )
    material = dict(authority)
    material.pop("authoritySha256", None)
    if canonical_sha256(material) != supplied_sha256:
        raise TemporalDiscoveryContractError(
            "Python boundary adoption authority identity drifted"
        )
    authorized = tuple(authority.get("generationIndices") or [])
    if requested and tuple(requested) != authorized:
        raise TemporalDiscoveryContractError(
            "Python boundary adoption request conflicts with durable authority"
        )
    if not set(authorized).issubset(records):
        raise TemporalDiscoveryContractError(
            "Python boundary adoption authority names a missing completed generation"
        )
    if authority.get("configSha256") != config.get("configSha256"):
        raise TemporalDiscoveryContractError(
            "Python boundary adoption config identity drifted"
        )
    adoption_runtime_sha256 = _sha256(
        authority.get("runtimeAuthoritySha256"),
        name="Python boundary adoption runtime authority",
    )
    if unbound:
        runtime_authority = _native_finalization_runtime_authority(finalizer_binary)
        if adoption_runtime_sha256 != runtime_authority.get("authoritySha256"):
            raise TemporalDiscoveryContractError(
                "Python boundary adoption binary authority drifted"
            )
    else:
        for role in ("campaignSeal", "tailReducer", "generationFinalizer"):
            _pinned_native_authority_binary(
                root=root,
                authority_sha256=adoption_runtime_sha256,
                role=role,
            )
    expected_boundaries = [
        _python_boundary_adoption_descriptor(records[index]) for index in authorized
    ]
    if authority.get("boundaries") != expected_boundaries:
        raise TemporalDiscoveryContractError(
            "Python boundary adoption record or artifact identities drifted"
        )
    if not unbound.issubset(set(authorized)):
        raise TemporalDiscoveryContractError(
            "Python boundary adoption authority omits an unbound completed generation"
        )
    return authority


def _native_finalization_authority_sha256(
    root: Path, generation_index: int
) -> str:
    return _identity_payload(
        _native_runtime_authority_for_generation(
            root=root, generation_index=generation_index
        ),
        "authoritySha256",
        name="native finalization runtime authority",
    )


def _verify_pinned_native_invocation_binary(
    *, binary: Path, manifest_path: Path, role: str
) -> None:
    manifest = _canonical_file(manifest_path, name=f"native {role} manifest")
    runtime_authority_sha256 = manifest.get("runtimeAuthoritySha256")
    if runtime_authority_sha256 is None:
        # Historical compact manifests predate the production runtime pin.
        return
    runtime_authority_sha256 = _sha256(
        runtime_authority_sha256, name=f"native {role} runtime authority"
    )
    authority_path = next(
        (
            parent / "native-finalization-authority.json"
            for parent in (manifest_path.resolve().parent, *manifest_path.resolve().parents)
            if (parent / "native-finalization-authority.json").is_file()
        ),
        None,
    )
    if authority_path is None:
        raise TemporalDiscoveryContractError(
            f"native {role} manifest has no frozen runtime authority"
        )
    try:
        pinned = _pinned_native_authority_binary(
            root=authority_path.parent,
            authority_sha256=runtime_authority_sha256,
            role=role,
        )
    except TemporalDiscoveryContractError as exc:
        if "binary identity drifted" not in str(exc):
            raise
        raise TemporalDiscoveryContractError(
            f"native {role} binary identity drifted during invocation"
        ) from exc
    if binary.resolve() != pinned:
        raise TemporalDiscoveryContractError(
            f"native {role} binary identity drifted during invocation"
        )


def _native_finalization_root(root: Path, generation_index: int) -> Path:
    base = (
        root
        / "generations"
        / f"generation-{generation_index:04d}"
        / "native-finalization"
    )
    authority_path = root / "native-finalization-authority.json"
    if not authority_path.is_file():
        return base
    root_authority = _validate_native_runtime_authority(
        _canonical_file(
            authority_path, name="native finalization root runtime authority"
        )
    )
    active = _native_runtime_authority_for_generation(
        root=root, generation_index=generation_index
    )
    if active["authoritySha256"] == root_authority["authoritySha256"]:
        return base
    return (
        base
        / "attempts"
        / active["authoritySha256"].removeprefix("sha256:")
    )


def _native_incomplete_attempt_descriptor(
    *, root: Path, generation_index: int, authority_sha256: str
) -> dict[str, Any]:
    attempt_root = _native_finalization_root(root, generation_index)
    records: list[dict[str, Any]] = []
    if attempt_root.is_dir():
        for path in sorted(
            (item for item in attempt_root.rglob("*") if item.is_file()),
            key=lambda item: item.relative_to(attempt_root).as_posix(),
        ):
            relative = path.relative_to(attempt_root).as_posix()
            records.append(
                {
                    "relativePath": relative,
                    "bytes": path.stat().st_size,
                    "fileSha256": _native_binary_file_sha256(path),
                }
            )
    return _native_self_hash(
        {
            "schemaVersion": "temporal_qd_native_incomplete_attempt_descriptor_v1",
            "generationIndex": generation_index,
            "runtimeAuthoritySha256": _sha256(
                authority_sha256,
                name="native incomplete attempt runtime authority",
            ),
            "rootPath": str(attempt_root.resolve()),
            "fileCount": len(records),
            "totalBytes": sum(int(record["bytes"]) for record in records),
            "files": records,
        },
        "descriptorSha256",
    )


def _native_campaign_seal_manifest(
    *,
    root: Path,
    config: Mapping[str, Any],
    generation_index: int,
    evaluation_population: Mapping[str, Any],
    campaign_root: Path | None = None,
    evaluation_population_file: Path | None = None,
    seal_root: Path | None = None,
) -> tuple[dict[str, Any], Path]:
    """Freeze the full immutable task payload before native raw-result admission."""

    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    selected_campaign_root = campaign_root or generation_root / "campaign"
    result_root = selected_campaign_root / "screening-run"
    authority = _canonical_file(result_root / "authority.json", name="tail authority")
    task_manifest = _canonical_file(
        result_root / "task-manifest.json", name="tail task manifest"
    )
    checkpoint = _canonical_file(
        result_root / "checkpoint.json", name="tail task checkpoint"
    )
    tasks = task_manifest.get("tasks")
    completed = checkpoint.get("completed")
    if not isinstance(tasks, list) or not isinstance(completed, Mapping):
        raise TemporalDiscoveryContractError(
            "native campaign seal requires a completed immutable task matrix"
        )
    by_task = {
        str(task.get("task_id")): task
        for task in tasks
        if isinstance(task, Mapping) and isinstance(task.get("task_id"), str)
    }
    if len(by_task) != len(tasks) or set(by_task) != set(completed):
        raise TemporalDiscoveryContractError(
            "native campaign seal checkpoint does not cover the exact task matrix"
        )
    if task_manifest.get("taskMatrixSha256") != canonical_sha256(tasks):
        raise TemporalDiscoveryContractError(
            "native campaign seal task matrix identity drifted"
        )
    source_tasks: list[dict[str, Any]] = []
    for task_id in sorted(by_task):
        task = by_task[task_id]
        payload = task.get("payload")
        record = completed[task_id]
        if not isinstance(payload, Mapping) or not isinstance(record, Mapping):
            raise TemporalDiscoveryContractError(
                "native campaign seal task/checkpoint row is invalid"
            )
        evidence_plan = payload.get("evidence_plan")
        result_path = Path(str(record.get("resultPath") or ""))
        if (
            not isinstance(evidence_plan, Mapping)
            or not result_path.is_file()
            or result_path.resolve().parent != (result_root / "results").resolve()
            or record.get("candidateId") != payload.get("candidate_id")
        ):
            raise TemporalDiscoveryContractError(
                "native campaign seal task/result authority drifted"
            )
        raw_ref = {
            "schemaVersion": "temporal_qd_tail_raw_result_ref_v1",
            "relativePath": result_path.resolve()
            .relative_to(result_root.resolve())
            .as_posix(),
            "resultSha256": record.get("resultSemanticSha256"),
            "codec": record.get("resultCodec"),
            "semanticSizeBytes": record.get("resultSemanticSizeBytes"),
            "uncompressedSha256": record.get("resultUncompressedSha256"),
            "uncompressedSizeBytes": record.get("resultUncompressedSizeBytes"),
            "blobSha256": record.get("resultBlobSha256"),
            "blobSizeBytes": record.get("resultBlobSizeBytes"),
        }
        source_tasks.append(
            {
                "task": {
                    "taskId": task_id,
                    "candidateId": payload.get("candidate_id"),
                    "analysisWindowStart": payload.get("analysis_window_start"),
                    "analysisWindowEnd": payload.get("analysis_window_end"),
                    "evidencePlanSemanticSha256": evidence_plan.get("plan_id"),
                    "taskPayloadSha256": canonical_sha256(payload),
                },
                "taskPayloadBinding": {
                    "taskPayloadSha256": canonical_sha256(payload),
                    "barLimit": payload.get("bar_limit"),
                },
                "rawResultPath": str(result_path.resolve()),
                "rawResultRef": raw_ref,
                "resultBinding": {
                    "taskKind": task.get("task_kind"),
                    "jobId": payload.get("job_id"),
                    "authorityId": payload.get("authority_id"),
                    "candidateId": payload.get("candidate_id"),
                    "evidencePlanId": evidence_plan.get("plan_id"),
                    "lakeWindowSemanticSha256": payload.get(
                        "lake_window_semantic_sha256"
                    ),
                    "sharedObservationStreamId": payload.get(
                        "shared_observation_stream_id"
                    ),
                },
            }
        )
    selected_seal_root = seal_root or (
        _native_finalization_root(root, generation_index) / "campaign-seal"
    )
    source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_campaign_seal_source_v1",
            "authorityId": authority["authorityId"],
            "authoritySha256": canonical_sha256(authority),
            "taskMatrixSha256": task_manifest["taskMatrixSha256"],
            "taskManifestSha256": canonical_sha256(task_manifest),
            "taskManifestPath": str(
                (result_root / "task-manifest.json").resolve()
            ),
            "checkpointSha256": canonical_sha256(checkpoint),
            "taskCount": len(source_tasks),
            "funnelProjectionIncluded": True,
            "tasks": source_tasks,
        },
        "sourceSha256",
    )
    source_path = selected_seal_root / "source.json"
    _write_canonical_once(source_path, source)
    contract = validate_rotating_evidence_contract(config["rotatingEvidence"])
    manifest = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_campaign_seal_manifest_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "seal_completed_task_matrix_and_reduce_tail",
            "runtimeAuthoritySha256": _native_finalization_authority_sha256(
                root, generation_index
            ),
            "sourcePath": str(source_path.resolve()),
            "sourceSha256": source["sourceSha256"],
            "evaluationPopulationPath": str(
                (
                    evaluation_population_file
                    or generation_root / "proposal" / "evaluation-population.json"
                ).resolve()
            ),
            "evaluationPopulationSha256": evaluation_population[
                "evaluationPopulationSha256"
            ],
            "generationIndex": generation_index,
            "minimumTotalTrades": int(
                config["frozenSearchPolicy"]["minimumTotalTrades"]
            ),
            "minimumTradesPerWindow": int(
                config["frozenSearchPolicy"]["minimumTradesPerWindow"]
            ),
            "capTrades": int(config["frozenSearchPolicy"]["capTrades"]),
            "provisionalLimit": int(
                contract["provisionalReduction"]["maxCandidates"]
            ),
            "resultPath": "generation-tail-transaction-result.json",
            **(
                {
                    "directionalTailAuthority": build_v5_directional_tail_authority(
                        runtime_authority_sha256=_native_finalization_authority_sha256(
                            root, generation_index
                        ),
                        generation_index=generation_index,
                    )
                }
                if config.get("evolvableModuleAuthority") is not None
                else {}
            ),
        },
        "manifestSha256",
    )
    manifest_path = selected_seal_root / "manifest.json"
    _write_canonical_once(manifest_path, manifest)
    return manifest, manifest_path


def _native_prepared_finalizer_manifest(
    *,
    root: Path,
    config: Mapping[str, Any],
    generation_index: int,
    projection: Mapping[str, Any],
    cohort: Mapping[str, Any],
    provisional: Mapping[str, Any],
    bundles: list[Mapping[str, Any]],
    complete_bundle_snapshot: bool,
    auxiliary_plan: Mapping[str, Any] | None,
    auxiliary_campaign_receipts: list[Mapping[str, Any]],
    rich_members: list[Mapping[str, Any]],
    current_member_count: int,
    campaigns: list[Mapping[str, Any]],
    total_generation_task_count: int,
    proposal_tail_index: Mapping[str, Any],
    generation_record_extra: Mapping[str, Any],
) -> tuple[dict[str, Any], Path]:
    """Freeze the production pre-final transaction without Python outputs."""

    if (
        not complete_bundle_snapshot
        or auxiliary_plan is not None
        or auxiliary_campaign_receipts
    ):
        raise TemporalDiscoveryContractError(
            "native descriptor receipts are disabled until their full authority "
            "chain can be reopened"
        )

    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"
    evidence_root = generation_root / "evidence"
    native_root = _native_finalization_root(root, generation_index)
    contract = validate_rotating_evidence_contract(config["rotatingEvidence"])
    journal = _canonical_file(
        proposal_root / "generation-journal.json", name="native production journal"
    )
    authority = _canonical_file(campaign_root / "authority.json", name="QD authority")
    task_manifest = _canonical_file(
        result_root / "task-manifest.json", name="QD task manifest"
    )
    evaluation_checkpoint = _canonical_file(
        result_root / "checkpoint.json", name="QD evaluation checkpoint"
    )
    campaign = _canonical_file(campaign_root / "campaign.json", name="QD campaign")
    evaluation_identity = _canonical_file(
        campaign_root / "evaluation-identity.json", name="QD evaluation identity"
    )
    empty_archive = {
        "cells": [],
        "rotatingEvidenceTransaction": {
            "schemaVersion": "temporal_qd_prefinal_empty_archive_v1"
        },
        "resolvedExecutionDeduplication": {"duplicates": []},
    }
    pre_archive_funnel = build_qd_generation_funnel(
        proposal_entries=projection["funnelEntries"],
        proposal_accounting=journal,
        population=projection,
        authority=authority,
        task_manifest=task_manifest,
        checkpoint=evaluation_checkpoint,
        archive=empty_archive,
        minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]),
        minimum_trades_per_window=int(
            config["frozenSearchPolicy"]["minimumTradesPerWindow"]
        ),
        tail_result_index=proposal_tail_index,
    )
    funnel_source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_native_funnel_reduction_source_v1",
            "preArchiveProjection": True,
            "completenessPolicy": pre_archive_funnel["completenessPolicy"],
            "proposalAccounting": pre_archive_funnel["proposalAccounting"],
            "proposalAttempts": pre_archive_funnel["attemptLedger"]["attempts"],
            "candidateStageRows": pre_archive_funnel["candidates"],
        },
        "funnelSourceSha256",
    )
    first = int(config["generationPlan"]["firstGenerationIndex"])
    previous_archive_path = (
        Path(str(config["initialArchive"]["path"]))
        if generation_index == first
        else root
        / "generations"
        / f"generation-{generation_index - 1:04d}"
        / "archive.json"
    )
    previous_cumulative = (
        None
        if generation_index == first
        else _load_previous_cumulative_archive(previous_archive_path)
    )
    previous_archive = _canonical_file(
        previous_archive_path, name="native previous parent archive"
    )
    previous_summary = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_previous_parent_archive_summary_v1",
            "archiveSha256": _sha256(
                previous_archive.get("archiveSha256"), name="previous parent archive"
            ),
            "candidateCountSeen": int(previous_archive.get("candidateCountSeen") or 0),
            "memberCount": int(previous_archive.get("memberCount") or 0),
            "cellIds": sorted(
                str(row.get("cellId"))
                for row in previous_archive.get("cells") or []
                if isinstance(row, Mapping)
            ),
            **(
                {
                    "bidirectionalPairPolicy": _clone(
                        previous_archive["bidirectionalPairPolicy"],
                        name="previous bidirectional pair policy",
                    )
                }
                if previous_archive.get("bidirectionalPairPolicy") is not None
                else {}
            ),
        },
        "summarySha256",
    )
    archive_policy = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_archive_policy_binding_v1",
            "qdVersion": config["qdVersion"],
            "policyName": config["policyName"],
            "policySha256": config["policySha256"],
            "frozenPolicy": _clone(config["frozenPolicy"], name="frozen QD policy"),
        },
        "policyBindingSha256",
    )
    artifact_base = _native_prefinal_artifact_ledger_base(
        generation_root=generation_root,
        generation_index=generation_index,
        evaluation_population=projection,
        tail_result_index=proposal_tail_index,
    )
    completion_marker_path = native_root / "completion-marker.json"
    if completion_marker_path.is_file():
        completion_marker = _canonical_file(
            completion_marker_path, name="native completion marker"
        )
        _identity_payload(
            completion_marker,
            "markerSha256",
            name="native completion marker",
        )
        if completion_marker.get("generationIndex") != generation_index:
            raise TemporalDiscoveryContractError(
                "native completion marker generation drifted"
            )
    else:
        completion_marker = _native_self_hash(
            {
                "schemaVersion": "temporal_qd_native_completion_marker_v1",
                "generationIndex": generation_index,
                "completedAt": _utc_now(),
            },
            "markerSha256",
        )
        _write_canonical_once(completion_marker_path, completion_marker)
    record_base = {
        "populationSha256": projection["populationSha256"],
        "evaluationPopulationSha256": projection["evaluationPopulationSha256"],
        "journalSha256": journal["journalSha256"],
        "proposalCount": int(journal["proposalCount"]),
        "candidateCount": int(projection["candidateCount"]),
        "originProposalCounts": _clone(
            journal["originProposalCounts"], name="origin proposal counts"
        ),
        "originAcceptedCounts": _clone(
            journal["originAcceptedCounts"], name="origin accepted counts"
        ),
        "campaignSha256": campaign["campaignSha256"],
        "evaluationIdentitySha256": evaluation_identity[
            "evaluationIdentitySha256"
        ],
        "taskMatrixSha256": task_manifest["taskMatrixSha256"],
        "taskCount": len(task_manifest["tasks"]),
        "totalGenerationTaskCount": total_generation_task_count,
        "proposalSlots": _clone(journal["proposalSlots"], name="proposal slots"),
        "uniqueIdentityCounts": _clone(
            journal["uniqueIdentityCounts"], name="unique identity counts"
        ),
        "duplicateCounters": _clone(
            journal["duplicateCounters"], name="duplicate counters"
        ),
        "proposalSlotCounters": _clone(
            journal["proposalSlotCounters"], name="proposal slot counters"
        ),
        "nextImmigrantContinuationOrdinal": int(
            journal["nextImmigrantContinuationOrdinal"]
        ),
        "completedAt": completion_marker["completedAt"],
        **_clone(generation_record_extra, name="native generation record extras"),
    }
    publication_paths = {
        "archive": str((generation_root / "archive.json").resolve()),
        "generationFunnel": str((generation_root / "generation-funnel.json").resolve()),
        "rotatingEvidenceLedger": str(
            (evidence_root / "generation-ledger.json").resolve()
        ),
        "rotatingEvidenceCheckpoint": str((evidence_root / "checkpoint.json").resolve()),
        "cumulativeBreederArchive": str(
            (evidence_root / "cumulative-archive.json").resolve()
        ),
    }
    source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_source_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "generationIndex": generation_index,
            "rotatingEvidence": _clone(contract, name="rotating evidence contract"),
            "cohort": _clone(cohort, name="native cohort"),
            "provisional": _clone(provisional, name="native provisional"),
            "baselineCandidatePanelBundles": _clone(
                bundles, name="native baseline bundle snapshot"
            ),
            "completeBundleSnapshot": complete_bundle_snapshot,
            "auxiliaryPlan": (
                _clone(auxiliary_plan, name="native auxiliary plan")
                if auxiliary_plan is not None
                else None
            ),
            "auxiliaryCampaignReceipts": _clone(
                auxiliary_campaign_receipts,
                name="native auxiliary campaign receipts",
            ),
            "previousCumulativeArchive": previous_cumulative,
            "previousParentArchiveSummary": previous_summary,
            "archivePolicy": archive_policy,
            "richMembers": _clone(rich_members, name="native rich members"),
            "currentMemberCount": current_member_count,
            "cellCapacity": int(config["frozenSearchPolicy"]["cellCapacity"]),
            "campaigns": _clone(campaigns, name="native campaign bindings"),
            "artifactLedgerBase": artifact_base,
            "publicationPaths": publication_paths,
            "funnelReductionSource": funnel_source,
            "generationRecordBase": record_base,
            "stateTransitionBase": {
                "nextGenerationIndex": generation_index + 1,
                "nextStage": "generation_proposal",
                "candidateCountIncrement": int(projection["candidateCount"]),
                "workerTaskCountIncrement": total_generation_task_count,
                "nextImmigrantContinuationOrdinal": int(
                    journal["nextImmigrantContinuationOrdinal"]
                ),
            },
        },
        "sourceSha256",
    )
    source_path = native_root / "source.json"
    _write_canonical_once(source_path, source)
    manifest = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": _native_finalization_authority_sha256(
                root, generation_index
            ),
            "sourcePath": str(source_path.resolve()),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    manifest_path = native_root / "manifest.json"
    _write_canonical_once(manifest_path, manifest)
    return manifest, manifest_path


def _native_finalizer_manifest(
    *,
    root: Path,
    config: Mapping[str, Any],
    generation_record: Mapping[str, Any],
) -> tuple[dict[str, Any], Path]:
    """Build a historical parity manifest from an already-completed Python boundary.

    This deliberately is not a production finalization gateway: several source
    fields are projected from Python's completed outputs.  The caller may use
    it for migration comparison/admission only, never to skip Python work.
    """
    generation_index = int(generation_record["generationIndex"])
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    native_root = _native_finalization_root(root, generation_index)
    evidence_root = generation_root / "evidence"
    cohort = _canonical_file(evidence_root / "cohort.json", name="native cohort")
    provisional = _canonical_file(
        evidence_root / "provisional.json", name="native provisional survivors"
    )
    cumulative = _canonical_file(
        evidence_root / "cumulative-archive.json",
        name="native cumulative archive oracle",
    )
    checkpoint = _canonical_file(
        evidence_root / "checkpoint.json", name="native rotating checkpoint oracle"
    )
    ledger = _canonical_file(
        evidence_root / "generation-ledger.json",
        name="native rotating ledger oracle",
    )
    archive = _canonical_file(
        generation_root / "archive.json", name="native parent archive oracle"
    )
    funnel = _canonical_file(
        generation_root / "generation-funnel.json", name="native funnel oracle"
    )
    artifacts = _clone(
        generation_record.get("artifacts"), name="native generation artifact ledger"
    )
    if (
        config.get("rotatingEvidence") is None
        or not bool((config.get("generationFunnel") or {}).get("enabled"))
        or not isinstance(artifacts, Mapping)
    ):
        raise TemporalDiscoveryContractError(
            "Rust generation finalization requires rotating evidence and the immutable funnel"
        )
    first = int(config["generationPlan"]["firstGenerationIndex"])
    previous_archive_path = (
        Path(str(config["initialArchive"]["path"]))
        if generation_index == first
        else root
        / "generations"
        / f"generation-{generation_index - 1:04d}"
        / "archive.json"
    )
    previous_cumulative = (
        None
        if generation_index == first
        else _canonical_file(
            root
            / "generations"
            / f"generation-{generation_index - 1:04d}"
            / "evidence"
            / "cumulative-archive.json",
            name="previous cumulative archive",
        )
    )
    previous_archive = _canonical_file(
        previous_archive_path, name="native previous parent archive"
    )
    previous_summary = {
        "schemaVersion": "temporal_qd_previous_parent_archive_summary_v1",
        "archiveSha256": _sha256(
            previous_archive.get("archiveSha256"), name="previous parent archive"
        ),
        "candidateCountSeen": int(previous_archive.get("candidateCountSeen") or 0),
        "memberCount": int(previous_archive.get("memberCount") or 0),
        "cellIds": sorted(
            str(row.get("cellId"))
            for row in previous_archive.get("cells") or []
            if isinstance(row, Mapping)
        ),
        **(
            {
                "bidirectionalPairPolicy": _clone(
                    previous_archive["bidirectionalPairPolicy"],
                    name="previous bidirectional pair policy",
                )
            }
            if previous_archive.get("bidirectionalPairPolicy") is not None
            else {}
        ),
    }
    previous_summary = _native_self_hash(previous_summary, "summarySha256")
    archive_policy = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_archive_policy_binding_v1",
            "qdVersion": config["qdVersion"],
            "policyName": config["policyName"],
            "policySha256": config["policySha256"],
            "frozenPolicy": _clone(config["frozenPolicy"], name="frozen QD policy"),
        },
        "policyBindingSha256",
    )
    rich_members = [
        _clone(member, name="native rich parent member")
        for cell in archive.get("cells") or []
        if isinstance(cell, Mapping)
        for member in cell.get("members") or []
        if isinstance(member, Mapping)
    ]
    record_base = {
        key: _clone(value, name=f"generation record {key}")
        for key, value in generation_record.items()
        if key
        not in {
            "archiveSha256",
            "resultSetSha256",
            "rotatingEvidenceLedgerSha256",
            "rotatingEvidenceCheckpointSha256",
            "cumulativeArchiveSha256",
            "nativeGenerationFinalization",
        }
    }
    funnel_source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_native_funnel_reduction_source_v1",
            "completenessPolicy": _clone(
                funnel["completenessPolicy"], name="funnel completeness policy"
            ),
            "proposalAccounting": _clone(
                funnel["proposalAccounting"], name="funnel proposal accounting"
            ),
            "proposalAttempts": _clone(
                funnel["attemptLedger"]["attempts"], name="funnel attempts"
            ),
            "candidateStageRows": _clone(
                funnel["candidates"], name="funnel joined candidate stages"
            ),
        },
        "funnelSourceSha256",
    )
    source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_source_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "generationIndex": generation_index,
            "rotatingEvidence": _clone(
                config["rotatingEvidence"], name="rotating evidence contract"
            ),
            "cohort": cohort,
            "provisional": provisional,
            "baselineCandidatePanelBundles": _clone(
                cumulative["candidatePanelBundles"],
                name="native candidate panel bundles",
            ),
            "completeBundleSnapshot": True,
            "auxiliaryPlan": None,
            "auxiliaryCampaignReceipts": [],
            "previousCumulativeArchive": previous_cumulative,
            "previousParentArchiveSummary": previous_summary,
            "archivePolicy": archive_policy,
            "richMembers": rich_members,
            "currentMemberCount": int(
                archive.get("candidateCountReducedThisGeneration") or 0
            ),
            "cellCapacity": int(archive["cellCapacity"]),
            "campaigns": _clone(ledger["campaigns"], name="rotating campaigns"),
            "stageArtifacts": _clone(
                checkpoint["stageArtifacts"], name="rotating stage artifacts"
            ),
            "artifactLedger": artifacts,
            "funnelReductionSource": funnel_source,
            "generationRecordBase": record_base,
            "stateTransitionBase": {
                "nextGenerationIndex": generation_index + 1,
                "nextStage": "generation_proposal",
                "candidateCountIncrement": int(generation_record["candidateCount"]),
                "workerTaskCountIncrement": int(
                    generation_record.get("totalGenerationTaskCount")
                    or generation_record["taskCount"]
                ),
                "nextImmigrantContinuationOrdinal": int(
                    generation_record["nextImmigrantContinuationOrdinal"]
                ),
            },
        },
        "sourceSha256",
    )
    source_path = native_root / "source.json"
    _write_canonical_once(source_path, source)
    manifest = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": _native_finalization_authority_sha256(
                root, generation_index
            ),
            "sourcePath": str(source_path.resolve()),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    manifest_path = native_root / "manifest.json"
    _write_canonical_once(manifest_path, manifest)
    return manifest, manifest_path


def _native_prefinal_artifact_ledger_base(
    *,
    generation_root: Path,
    generation_index: int,
    evaluation_population: Mapping[str, Any],
    tail_result_index: Mapping[str, Any],
) -> dict[str, Any]:
    """Capture only artifacts which exist before archive/funnel finalization."""

    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"
    population_path = proposal_root / "population.json"
    evaluation_population_file = proposal_root / "evaluation-population.json"
    journal_path = proposal_root / "generation-journal.json"
    journal = _canonical_file(journal_path, name="native pre-final journal")
    preparation_path = campaign_root / "preparation.json"
    authority_path = campaign_root / "authority.json"
    identity_path = campaign_root / "evaluation-identity.json"
    campaign_path = campaign_root / "campaign.json"
    manifest_path = result_root / "task-manifest.json"
    result_authority_path = result_root / "authority.json"
    checkpoint_path = result_root / "checkpoint.json"
    summary_path = result_root / "summary.json"
    preparation = _canonical_file(preparation_path, name="native pre-final preparation")
    authority = _canonical_file(authority_path, name="native pre-final authority")
    identity = _canonical_file(identity_path, name="native pre-final evaluation identity")
    campaign = _canonical_file(campaign_path, name="native pre-final campaign")
    manifest = _canonical_file(manifest_path, name="native pre-final task manifest")
    result_authority = _canonical_file(
        result_authority_path, name="native pre-final result authority"
    )
    checkpoint = _canonical_file(checkpoint_path, name="native pre-final checkpoint")
    summary = _canonical_file(summary_path, name="native pre-final summary")
    if int(journal.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("native pre-final journal index drifted")
    results = _results_descriptor(
        result_root=result_root,
        checkpoint=checkpoint,
        task_manifest=manifest,
        tail_result_index=tail_result_index,
    )
    output: dict[str, Any] = {
        "schemaVersion": "temporal_qd_supervisor_generation_artifacts_v1",
        "population": {
            "path": str(population_path.resolve()),
            "sha256": evaluation_population["populationFileSha256"],
            "populationSha256": evaluation_population["populationSha256"],
        },
        "preparation": _artifact_descriptor(preparation_path, preparation),
        "authority": _self_hashed_descriptor(
            authority_path,
            authority,
            field="authorityId",
            name="native pre-final authority",
        ),
        "evaluationIdentity": _self_hashed_descriptor(
            identity_path,
            identity,
            field="evaluationIdentitySha256",
            name="native pre-final evaluation identity",
        ),
        "campaign": _self_hashed_descriptor(
            campaign_path,
            campaign,
            field="campaignSha256",
            name="native pre-final campaign",
        ),
        "taskManifest": _artifact_descriptor(manifest_path, manifest),
        "resultAuthority": _self_hashed_descriptor(
            result_authority_path,
            result_authority,
            field="authorityId",
            name="native pre-final result authority",
        ),
        "checkpoint": _artifact_descriptor(checkpoint_path, checkpoint),
        "summary": _artifact_descriptor(summary_path, summary),
        "results": results,
        "journal": _self_hashed_descriptor(
            journal_path,
            journal,
            field="journalSha256",
            name="native pre-final journal",
        ),
        "evaluationPopulation": _self_hashed_descriptor(
            evaluation_population_file,
            evaluation_population,
            field="evaluationPopulationSha256",
            name="native pre-final evaluation population",
        ),
    }
    if evaluation_population.get("g0Bootstrap") is not None:
        output["g0Bootstrap"] = _clone(
            evaluation_population["g0Bootstrap"], name="native pre-final G0 binding"
        )
    return output


def _native_independent_finalizer_manifest(
    *,
    root: Path,
    config: Mapping[str, Any],
    generation_index: int,
    tail_result_indexes: dict[Path, dict[str, Any]],
    tail_reducer_binary: Path,
    completed_at: str | None = None,
) -> tuple[dict[str, Any], Path]:
    """Build a native source without opening any completed finalization output."""

    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"
    native_root = _native_finalization_root(root, generation_index)
    evidence_root = generation_root / "evidence"
    contract = validate_rotating_evidence_contract(config["rotatingEvidence"])
    panel = panel_for_generation(contract, generation_index)
    # The compact projection and journal jointly freeze the 1.2 GB rich
    # population's byte identity.  Native finalization never consumes those
    # bytes, so reopening them here is both circular in responsibility and a
    # severe latency/RSS regression.  Candidate material is independently
    # reopened below from the append-only proposal journal.
    projection = load_evaluation_population(
        population_path=proposal_root / "population.json",
        journal_path=proposal_root / "generation-journal.json",
        verify_population_file=False,
    )
    journal = _canonical_file(
        proposal_root / "generation-journal.json", name="native pre-final journal"
    )
    cohort = _canonical_file(evidence_root / "cohort.json", name="native cohort")
    provisional = _canonical_file(
        evidence_root / "provisional.json", name="native provisional survivors"
    )
    proposal_tail_index = validate_tail_result_index(
        _canonical_file(
            result_root / "tail-result-index-v3.json",
            name="native pre-final tail result index",
        )
    )
    if proposal_tail_index.get("funnelProjectionIncluded") is not True:
        raise TemporalDiscoveryContractError(
            "native pre-final tail result index lacks funnel projections"
        )
    tail_result_indexes[result_root.resolve()] = proposal_tail_index
    tail_root = native_root / "tail-reduction"
    tail_manifest = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_native_tail_reduction_manifest_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "reduce_evaluated_members_and_provisional",
            "runtimeAuthoritySha256": _native_finalization_authority_sha256(
                root, generation_index
            ),
            "evaluationPopulationPath": str(
                (proposal_root / "evaluation-population.json").resolve()
            ),
            "evaluationPopulationSha256": projection[
                "evaluationPopulationSha256"
            ],
            "tailResultIndexPath": str(
                (result_root / "tail-result-index-v3.json").resolve()
            ),
            "tailResultIndexSha256": proposal_tail_index[
                "tailResultIndexSha256"
            ],
            "generationIndex": generation_index,
            "minimumTotalTrades": int(
                config["frozenSearchPolicy"]["minimumTotalTrades"]
            ),
            "minimumTradesPerWindow": int(
                config["frozenSearchPolicy"]["minimumTradesPerWindow"]
            ),
            "capTrades": int(config["frozenSearchPolicy"]["capTrades"]),
            "provisionalLimit": int(
                contract["provisionalReduction"]["maxCandidates"]
            ),
            "resultPath": "tail-reduction-result.json",
        },
        "manifestSha256",
    )
    tail_manifest_path = tail_root / "manifest.json"
    _write_canonical_once(tail_manifest_path, tail_manifest)
    _invoke_native_tail_reducer(
        binary=tail_reducer_binary, manifest_path=tail_manifest_path
    )
    tail_result = _canonical_file(
        tail_root / "tail-reduction-result.json", name="native tail reduction result"
    )
    _identity_payload(
        tail_result, "resultSha256", name="native tail reduction result"
    )
    if tail_result.get("manifestSha256") != tail_manifest["manifestSha256"]:
        raise TemporalDiscoveryContractError(
            "native tail reduction result manifest binding drifted"
        )
    native_provisional = tail_result.get("provisional")
    if (
        not isinstance(native_provisional, Mapping)
        or native_provisional.get("candidateCount") != provisional.get("candidateCount")
        or native_provisional.get("candidates") != provisional.get("candidates")
    ):
        raise TemporalDiscoveryContractError(
            "native provisional reduction differs from frozen Python oracle"
        )
    provisional_ids = [str(row["candidateId"]) for row in provisional["candidates"]]
    provisional_id_set = set(provisional_ids)
    current_members: dict[str, dict[str, Any]] = {}
    members_path = tail_root / "evaluated-members.jsonl"
    try:
        with members_path.open("r", encoding="utf-8", newline="") as handle:
            for line in handle:
                member = json.loads(line)
                candidate_id = str(member.get("candidateId"))
                if candidate_id in provisional_id_set:
                    if candidate_id in current_members:
                        raise TemporalDiscoveryContractError(
                            "native tail reduction emitted a duplicate provisional member"
                        )
                    current_members[candidate_id] = member
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            "could not stream native evaluated members"
        ) from exc
    new_candidates = {
        str(row["candidateId"]): row for row in projection["candidates"]
    }
    rich_members: list[dict[str, Any]] = []
    rich_candidates: dict[str, dict[str, Any]] = {}
    for candidate_id in provisional_ids:
        member = current_members.get(candidate_id)
        if member is None:
            raise TemporalDiscoveryContractError(
                "native provisional survivor lacks independently reduced member"
            )
        candidate = member["candidate"]
        if candidate_id in new_candidates:
            candidate = hydrate_evaluation_candidate(
                candidate, proposal_root=proposal_root / "proposal-journal"
            )
        member["candidate"] = candidate
        rich_candidates[candidate_id] = _clone(
            candidate, name="native provisional rich candidate"
        )
        rich_members.append(member)
    window_records = _campaign_window_evidence(
        campaign_root=campaign_root,
        panel=panel,
        candidates={
            candidate_id: new_candidates[candidate_id]
            for candidate_id in provisional_ids
            if candidate_id in new_candidates
        },
        tail_result_index=proposal_tail_index,
    )
    bundles = [
        build_candidate_panel_bundle(
            contract=contract,
            candidate=rich_candidates[candidate_id],
            panel_id=str(panel["panelId"]),
            records=window_records[candidate_id],
        )
        for candidate_id in sorted(rich_candidates)
    ]
    required = required_panel_ids(contract, generation_index)
    if required != [str(panel["panelId"])]:
        raise TemporalDiscoveryContractError(
            "independent historical migration currently requires descriptor-bound auxiliary receipts"
        )
    campaign = _canonical_file(campaign_root / "campaign.json", name="proposal campaign")
    campaigns = [
        {
            "role": "proposal_current_panel",
            "panelId": panel["panelId"],
            "campaignRoot": str(campaign_root.resolve()),
            "campaignSha256": campaign["campaignSha256"],
        }
    ]
    authority = _canonical_file(campaign_root / "authority.json", name="QD authority")
    task_manifest = _canonical_file(
        result_root / "task-manifest.json", name="QD task manifest"
    )
    checkpoint = _canonical_file(
        result_root / "checkpoint.json", name="QD evaluation checkpoint"
    )
    empty_rotating_archive = {
        "cells": [],
        "rotatingEvidenceTransaction": {
            "schemaVersion": "temporal_qd_prefinal_empty_archive_v1"
        },
        "resolvedExecutionDeduplication": {"duplicates": []},
    }
    pre_archive_funnel = build_qd_generation_funnel(
        proposal_entries=projection["funnelEntries"],
        proposal_accounting=journal,
        population=projection,
        authority=authority,
        task_manifest=task_manifest,
        checkpoint=checkpoint,
        archive=empty_rotating_archive,
        minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]),
        minimum_trades_per_window=int(
            config["frozenSearchPolicy"]["minimumTradesPerWindow"]
        ),
        tail_result_index=proposal_tail_index,
    )
    funnel_source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_native_funnel_reduction_source_v1",
            "preArchiveProjection": True,
            "completenessPolicy": pre_archive_funnel["completenessPolicy"],
            "proposalAccounting": pre_archive_funnel["proposalAccounting"],
            "proposalAttempts": pre_archive_funnel["attemptLedger"]["attempts"],
            "candidateStageRows": pre_archive_funnel["candidates"],
        },
        "funnelSourceSha256",
    )
    first = int(config["generationPlan"]["firstGenerationIndex"])
    if generation_index == first:
        previous_archive_path = Path(str(config["initialArchive"]["path"]))
        previous_cumulative = None
    else:
        previous_archive_path = (
            root
            / "generations"
            / f"generation-{generation_index - 1:04d}"
            / "archive.json"
        )
        previous_cumulative = _load_previous_cumulative_archive(previous_archive_path)
    previous_archive = _canonical_file(
        previous_archive_path, name="native previous parent archive"
    )
    previous_summary = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_previous_parent_archive_summary_v1",
            "archiveSha256": _sha256(
                previous_archive.get("archiveSha256"), name="previous parent archive"
            ),
            "candidateCountSeen": int(previous_archive.get("candidateCountSeen") or 0),
            "memberCount": int(previous_archive.get("memberCount") or 0),
            "cellIds": sorted(
                str(row.get("cellId"))
                for row in previous_archive.get("cells") or []
                if isinstance(row, Mapping)
            ),
            **(
                {
                    "bidirectionalPairPolicy": _clone(
                        previous_archive["bidirectionalPairPolicy"],
                        name="previous bidirectional pair policy",
                    )
                }
                if previous_archive.get("bidirectionalPairPolicy") is not None
                else {}
            ),
        },
        "summarySha256",
    )
    archive_policy = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_archive_policy_binding_v1",
            "qdVersion": config["qdVersion"],
            "policyName": config["policyName"],
            "policySha256": config["policySha256"],
            "frozenPolicy": _clone(config["frozenPolicy"], name="frozen QD policy"),
        },
        "policyBindingSha256",
    )
    artifact_base = _native_prefinal_artifact_ledger_base(
        generation_root=generation_root,
        generation_index=generation_index,
        evaluation_population=projection,
        tail_result_index=proposal_tail_index,
    )
    evaluation_identity = _canonical_file(
        campaign_root / "evaluation-identity.json", name="evaluation identity"
    )
    record_base = {
        "populationSha256": projection["populationSha256"],
        "evaluationPopulationSha256": projection["evaluationPopulationSha256"],
        "journalSha256": journal["journalSha256"],
        "proposalCount": int(journal["proposalCount"]),
        "candidateCount": int(projection["candidateCount"]),
        "originProposalCounts": _clone(
            journal["originProposalCounts"], name="origin proposal counts"
        ),
        "originAcceptedCounts": _clone(
            journal["originAcceptedCounts"], name="origin accepted counts"
        ),
        "campaignSha256": campaign["campaignSha256"],
        "evaluationIdentitySha256": evaluation_identity[
            "evaluationIdentitySha256"
        ],
        "taskMatrixSha256": task_manifest["taskMatrixSha256"],
        "taskCount": len(task_manifest["tasks"]),
        "totalGenerationTaskCount": len(task_manifest["tasks"]),
        "proposalSlots": _clone(journal["proposalSlots"], name="proposal slots"),
        "uniqueIdentityCounts": _clone(
            journal["uniqueIdentityCounts"], name="unique identity counts"
        ),
        "duplicateCounters": _clone(
            journal["duplicateCounters"], name="duplicate counters"
        ),
        "proposalSlotCounters": _clone(
            journal["proposalSlotCounters"], name="proposal slot counters"
        ),
        "nextImmigrantContinuationOrdinal": int(
            journal["nextImmigrantContinuationOrdinal"]
        ),
        "completedAt": completed_at or _utc_now(),
        **(
            {"g0Bootstrap": _clone(journal["g0Bootstrap"], name="G0 bootstrap")}
            if journal.get("g0Bootstrap") is not None
            else {}
        ),
    }
    publication_paths = {
        "archive": str((generation_root / "archive.json").resolve()),
        "generationFunnel": str(
            (generation_root / "generation-funnel.json").resolve()
        ),
        "rotatingEvidenceLedger": str(
            (evidence_root / "generation-ledger.json").resolve()
        ),
        "rotatingEvidenceCheckpoint": str(
            (evidence_root / "checkpoint.json").resolve()
        ),
        "cumulativeBreederArchive": str(
            (evidence_root / "cumulative-archive.json").resolve()
        ),
    }
    source = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_source_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "generationIndex": generation_index,
            "rotatingEvidence": _clone(contract, name="rotating evidence contract"),
            "cohort": cohort,
            "provisional": provisional,
            "baselineCandidatePanelBundles": bundles,
            "completeBundleSnapshot": True,
            "auxiliaryPlan": None,
            "auxiliaryCampaignReceipts": [],
            "previousCumulativeArchive": previous_cumulative,
            "previousParentArchiveSummary": previous_summary,
            "archivePolicy": archive_policy,
            "richMembers": rich_members,
            "currentMemberCount": int(tail_result["evaluatedMembers"]["memberCount"]),
            "cellCapacity": int(config["frozenSearchPolicy"]["cellCapacity"]),
            "campaigns": campaigns,
            "artifactLedgerBase": artifact_base,
            "publicationPaths": publication_paths,
            "funnelReductionSource": funnel_source,
            "generationRecordBase": record_base,
            "stateTransitionBase": {
                "nextGenerationIndex": generation_index + 1,
                "nextStage": "generation_proposal",
                "candidateCountIncrement": int(projection["candidateCount"]),
                "workerTaskCountIncrement": len(task_manifest["tasks"]),
                "nextImmigrantContinuationOrdinal": int(
                    journal["nextImmigrantContinuationOrdinal"]
                ),
            },
        },
        "sourceSha256",
    )
    source_path = native_root / "source.json"
    _write_canonical_once(source_path, source)
    manifest = _native_self_hash(
        {
            "schemaVersion": "temporal_qd_generation_finalization_manifest_v1",
            "contractVersion": NATIVE_FOUNDATION_CONTRACT_VERSION,
            "operation": "finalize_rotating_generation",
            "runtimeAuthoritySha256": _native_finalization_authority_sha256(
                root, generation_index
            ),
            "sourcePath": str(source_path.resolve()),
            "sourceSha256": source["sourceSha256"],
            "resultPath": "generation-commit.json",
        },
        "manifestSha256",
    )
    manifest_path = native_root / "manifest.json"
    _write_canonical_once(manifest_path, manifest)
    return manifest, manifest_path


def _invoke_native_finalizer(
    *, binary: Path, manifest_path: Path, timeout_seconds: float = 600.0
) -> dict[str, Any]:
    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="generationFinalizer"
    )
    try:
        result = subprocess.run(
            [str(binary.resolve()), str(manifest_path.resolve())],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TemporalDiscoveryContractError(
            "native generation finalizer invocation failed"
        ) from exc
    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="generationFinalizer"
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2_000:]
        raise TemporalDiscoveryContractError(
            f"native generation finalizer failed closed: {detail}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TemporalDiscoveryContractError(
            "native generation finalizer returned invalid JSON"
        ) from exc
    if not isinstance(payload, Mapping) or payload.get("status") != "committed":
        raise TemporalDiscoveryContractError(
            "native generation finalizer did not return a committed boundary"
        )
    return dict(payload)


def _publish_native_generation_outputs(
    *,
    root: Path,
    generation_index: int,
    load_published_payloads: bool = True,
    _after_step: Any | None = None,
) -> dict[str, Any]:
    """Publish one committed native boundary to legacy authority paths.

    Native files are immutable and committed first.  Publication is therefore
    an idempotent convergence step: an existing semantic equal is accepted and
    any divergent destination fails before supervisor state can advance.
    """

    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    native_root = _native_finalization_root(root, generation_index)
    commit = _canonical_file(
        native_root / "generation-commit.json",
        name="committed native generation commit",
    )
    commit_sha256 = _identity_payload(
        commit, "commitSha256", name="committed native generation commit"
    )
    destinations = {
        "cumulative-archive.json": generation_root / "evidence" / "cumulative-archive.json",
        "archive.json": generation_root / "archive.json",
        "checkpoint.json": generation_root / "evidence" / "checkpoint.json",
        "generation-ledger.json": generation_root / "evidence" / "generation-ledger.json",
        "generation-funnel.json": generation_root / "generation-funnel.json",
    }
    journal_path = native_root / "publication-journal.json"
    if journal_path.is_file():
        journal = _canonical_file(
            journal_path, name="native generation publication journal"
        )
        _identity_payload(
            journal,
            "journalSha256",
            name="native generation publication journal",
        )
        if (
            journal.get("generationIndex") != generation_index
            or journal.get("commitSha256") != commit_sha256
            or not isinstance(journal.get("completedSteps"), list)
        ):
            raise TemporalDiscoveryContractError(
                "native generation publication journal drifted"
            )
    else:
        journal = {
            "schemaVersion": "temporal_qd_native_publication_journal_v1",
            "generationIndex": generation_index,
            "commitSha256": commit_sha256,
            "completedSteps": [],
        }
        journal["journalSha256"] = canonical_sha256(journal)
        _write_once(journal_path, journal)

    completed_steps = list(journal["completedSteps"])
    published: dict[str, Any] = {}
    for source_name, destination in destinations.items():
        source_path = native_root / source_name
        if destination.is_file():
            source_file_sha256 = _native_binary_file_sha256(source_path)
            if _native_binary_file_sha256(destination) != source_file_sha256:
                payload = _canonical_file(
                    source_path, name=f"committed native {source_name}"
                )
                existing = _canonical_file(
                    destination, name=f"pre-final published {source_name}"
                )
                if existing != payload:
                    replaceable_checkpoint = (
                        source_name == "checkpoint.json"
                        and source_name not in completed_steps
                        and existing.get("generationIndex") == generation_index
                        and existing.get("rotatingEvidenceSha256")
                        == payload.get("rotatingEvidenceSha256")
                        and existing.get("cohortSha256") == payload.get("cohortSha256")
                        and existing.get("stage")
                        in {
                            "current_panel_evaluation",
                            "provisional_reduction",
                            "cumulative_backfill",
                        }
                    )
                    if not replaceable_checkpoint:
                        raise TemporalDiscoveryContractError(
                            f"native publication destination diverged: {destination}"
                        )
                _publish_committed_file(
                    source_path,
                    destination,
                    replace_existing=True,
                )
        else:
            _publish_committed_file(source_path, destination)
        if load_published_payloads:
            published[source_name] = _canonical_file(
                source_path, name=f"committed native {source_name}"
            )
        if source_name not in completed_steps:
            completed_steps.append(source_name)
            journal = {
                "schemaVersion": "temporal_qd_native_publication_journal_v1",
                "generationIndex": generation_index,
                "commitSha256": commit_sha256,
                "completedSteps": completed_steps,
            }
            journal["journalSha256"] = canonical_sha256(journal)
            _replace(journal_path, journal)
        if _after_step is not None:
            _after_step(source_name, len(completed_steps))
    if load_published_payloads:
        published["generation-funnel-snapshot.json"] = _canonical_file(
            native_root / "generation-funnel-snapshot.json",
            name="committed native generation funnel snapshot",
        )
    published["generation-record.json"] = _canonical_file(
        native_root / "generation-record.json", name="committed native generation record"
    )
    published["generation-state-patch.json"] = _canonical_file(
        native_root / "generation-state-patch.json", name="committed native state patch"
    )
    published["generation-commit.json"] = commit
    return published


def _native_rotating_archive_result(
    *,
    root: Path,
    generation_index: int,
    published: Mapping[str, Any],
) -> dict[str, Any]:
    """Project the small committed native record into the supervisor result."""

    record = published.get("generation-record.json")
    state_patch = published.get("generation-state-patch.json")
    commit = published.get("generation-commit.json")
    if not isinstance(record, Mapping) or not isinstance(state_patch, Mapping):
        raise TemporalDiscoveryContractError(
            "native generation publication omitted its committed record or state patch"
        )
    if not isinstance(commit, Mapping):
        raise TemporalDiscoveryContractError(
            "native generation publication omitted its committed commit"
        )
    record_sha256 = _identity_payload(
        record, "generationRecordSha256", name="committed native generation record"
    )
    _identity_payload(
        state_patch,
        "statePatchSha256",
        name="committed native generation state patch",
    )
    if (
        int(record.get("generationIndex") or -1) != generation_index
        or int(state_patch.get("generationIndex") or -1) != generation_index
        or state_patch.get("generationRecordSha256") != record_sha256
        or state_patch.get("generationRecord") != record
    ):
        raise TemporalDiscoveryContractError(
            "native generation record and state patch disagree"
        )
    count_fields = (
        "qualityMemberCount",
        "frontierMemberCount",
        "observationalMemberCount",
        "negativeNoveltyMemberCount",
    )
    member_count = sum(int(record.get(field) or 0) for field in count_fields)
    total_task_count = int(record.get("totalGenerationTaskCount") or 0)
    proposal_task_count = int(record.get("taskCount") or 0)
    if total_task_count < proposal_task_count:
        raise TemporalDiscoveryContractError(
            "native generation task accounting is invalid"
        )
    parent_schedule = record.get("parentSchedule")
    if not isinstance(parent_schedule, Mapping):
        raise TemporalDiscoveryContractError(
            "native generation parent schedule is missing"
        )
    manifest_path = _native_finalization_root(root, generation_index) / "manifest.json"
    return {
        "schemaVersion": "temporal_qd_rotating_parent_archive_result_v1",
        "archiveSha256": record["archiveSha256"],
        "parentSchedule": _clone(
            parent_schedule, name="native committed parent schedule"
        ),
        "cumulativeArchiveSha256": record["cumulativeArchiveSha256"],
        "occupiedCellCount": int(record["occupiedCellCount"]),
        "memberCount": member_count,
        "qualityMemberCount": int(record["qualityMemberCount"]),
        "frontierMemberCount": int(record["frontierMemberCount"]),
        "newCellCount": int(record["newCellCount"]),
        "paretoAdmissionCount": int(record["paretoAdmissionCount"]),
        "paretoEvictionCount": int(record["paretoEvictionCount"]),
        "observationalMemberCount": int(record["observationalMemberCount"]),
        "negativeNoveltyMemberCount": int(record["negativeNoveltyMemberCount"]),
        "rotatingEvidenceLedgerSha256": record[
            "rotatingEvidenceLedgerSha256"
        ],
        "rotatingEvidenceCheckpointSha256": record[
            "rotatingEvidenceCheckpointSha256"
        ],
        "additionalWorkerTaskCount": total_task_count - proposal_task_count,
        "nativeManifest": _canonical_file(
            manifest_path, name="native generation finalization manifest"
        ),
        "nativeCommit": dict(commit),
        "nativeGenerationRecord": dict(record),
        "nativeStatePatch": dict(state_patch),
    }


def _validate_native_migration_outputs(
    *, root: Path, generation_record: Mapping[str, Any], execution: Mapping[str, Any]
) -> dict[str, Any]:
    generation_index = int(generation_record["generationIndex"])
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    native_root = _native_finalization_root(root, generation_index)
    comparisons = (
        ("cumulative-archive.json", generation_root / "evidence" / "cumulative-archive.json"),
        ("archive.json", generation_root / "archive.json"),
        ("checkpoint.json", generation_root / "evidence" / "checkpoint.json"),
        ("generation-ledger.json", generation_root / "evidence" / "generation-ledger.json"),
        ("generation-funnel.json", generation_root / "generation-funnel.json"),
    )
    for native_name, oracle_path in comparisons:
        native = _canonical_file(native_root / native_name, name=f"native {native_name}")
        oracle = _canonical_file(oracle_path, name=f"Python oracle {native_name}")
        if native != oracle:
            raise TemporalDiscoveryContractError(
                f"native historical migration disagrees with Python oracle: {native_name}"
            )
    snapshot = _canonical_file(
        native_root / "generation-funnel-snapshot.json", name="native funnel snapshot"
    )
    if snapshot != generation_record["artifacts"]["generationFunnelSnapshot"]:
        raise TemporalDiscoveryContractError(
            "native historical migration funnel snapshot disagrees with Python oracle"
        )
    commit = _canonical_file(
        native_root / "generation-commit.json", name="native generation commit"
    )
    commit_sha = _sha256(commit.get("commitSha256"), name="native generation commit")
    if commit_sha != execution.get("commitSha256"):
        raise TemporalDiscoveryContractError(
            "native generation commit identity disagrees with execution result"
        )
    return commit


def _native_generation_binding(
    *,
    root: Path,
    generation_record: Mapping[str, Any],
    manifest: Mapping[str, Any],
    commit: Mapping[str, Any],
    adoption_authority: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    generation_index = int(generation_record["generationIndex"])
    native_root = _native_finalization_root(root, generation_index)
    record_material = {
        key: value
        for key, value in generation_record.items()
        if key != "nativeGenerationFinalization"
    }
    admission = _native_self_hash(
        {
            "schemaVersion": NATIVE_FINALIZATION_ADMISSION_SCHEMA,
            "generationIndex": generation_index,
            "migrationMode": (
                "explicit_one_time_python_boundary_adoption"
                if adoption_authority is not None
                else "deep_validated_python_boundary_to_native_commit"
            ),
            "pythonGenerationRecordSha256": canonical_sha256(record_material),
            "manifestSha256": manifest["manifestSha256"],
            "sourceSha256": manifest["sourceSha256"],
            "commitSha256": commit["commitSha256"],
            "archiveSha256": generation_record["archiveSha256"],
            "resultSetSha256": generation_record["resultSetSha256"],
            "rotatingEvidenceLedgerSha256": generation_record[
                "rotatingEvidenceLedgerSha256"
            ],
            "rotatingEvidenceCheckpointSha256": generation_record[
                "rotatingEvidenceCheckpointSha256"
            ],
            "cumulativeArchiveSha256": generation_record["cumulativeArchiveSha256"],
            "generationFunnelArtifactSha256": generation_record[
                "generationFunnelArtifactSha256"
            ],
            "generationFunnelSnapshotSha256": generation_record[
                "generationFunnelSnapshotSha256"
            ],
            **(
                {
                    "adoptionAuthoritySha256": adoption_authority[
                        "authoritySha256"
                    ],
                    "runtimeAuthoritySha256": adoption_authority[
                        "runtimeAuthoritySha256"
                    ],
                }
                if adoption_authority is not None
                else {}
            ),
        },
        "admissionSha256",
    )
    admission_path = native_root / "historical-admission.json"
    _write_once(admission_path, admission)
    return _native_self_hash(
        {
            "schemaVersion": NATIVE_FINALIZATION_BINDING_SCHEMA,
            "generationIndex": generation_index,
            "authorityMode": (
                "native_explicit_python_boundary_adoption"
                if adoption_authority is not None
                else "native_compact_commit"
            ),
            "commitPath": str((native_root / "generation-commit.json").resolve()),
            "commitSha256": commit["commitSha256"],
            "generationRecordSha256": commit["generationRecord"][
                "generationRecordSha256"
            ],
            "manifestPath": str((native_root / "manifest.json").resolve()),
            "manifestSha256": manifest["manifestSha256"],
            "admissionPath": str(admission_path.resolve()),
            "admissionSha256": admission["admissionSha256"],
            **(
                {
                    "adoptionAuthorityPath": str(
                        (root / NATIVE_FINALIZATION_ADOPTION_AUTHORITY_FILE).resolve()
                    ),
                    "adoptionAuthoritySha256": adoption_authority[
                        "authoritySha256"
                    ],
                    "runtimeAuthoritySha256": adoption_authority[
                        "runtimeAuthoritySha256"
                    ],
                }
                if adoption_authority is not None
                else {}
            ),
            "deepAuditAvailable": True,
        },
        "bindingSha256",
    )


def _native_production_generation_binding(
    *,
    root: Path,
    generation_index: int,
    manifest: Mapping[str, Any],
    commit: Mapping[str, Any],
) -> dict[str, Any]:
    native_root = _native_finalization_root(root, generation_index)
    runtime_authority_sha256 = _sha256(
        manifest.get("runtimeAuthoritySha256"),
        name="native production runtime authority",
    )
    if runtime_authority_sha256 != _native_finalization_authority_sha256(
        root, generation_index
    ):
        raise TemporalDiscoveryContractError(
            "native production manifest runtime authority drifted"
        )
    return _native_self_hash(
        {
            "schemaVersion": NATIVE_FINALIZATION_BINDING_SCHEMA,
            "generationIndex": generation_index,
            "authorityMode": "native_production_compact_commit",
            "commitPath": str((native_root / "generation-commit.json").resolve()),
            "commitSha256": commit["commitSha256"],
            "generationRecordSha256": commit["generationRecord"][
                "generationRecordSha256"
            ],
            "manifestPath": str((native_root / "manifest.json").resolve()),
            "manifestSha256": manifest["manifestSha256"],
            "runtimeAuthoritySha256": runtime_authority_sha256,
            "deepAuditAvailable": True,
        },
        "bindingSha256",
    )


def _validate_native_generation_binding(
    *,
    generation_record: Mapping[str, Any],
    binary: Path,
) -> None:
    binding = _clone(
        generation_record.get("nativeGenerationFinalization"),
        name="native generation binding",
    )
    if not isinstance(binding, Mapping) or binding.get("schemaVersion") != (
        NATIVE_FINALIZATION_BINDING_SCHEMA
    ):
        raise TemporalDiscoveryContractError(
            "completed generation lacks its native finalization binding"
        )
    supplied_binding = _sha256(
        binding.get("bindingSha256"), name="native generation binding"
    )
    material = dict(binding)
    material.pop("bindingSha256", None)
    if canonical_sha256(material) != supplied_binding:
        raise TemporalDiscoveryContractError(
            "native generation binding identity mismatch"
        )
    manifest_path = Path(str(binding.get("manifestPath") or ""))
    authority_path = next(
        (
            parent / "native-finalization-authority.json"
            for parent in (manifest_path.resolve().parent, *manifest_path.resolve().parents)
            if (parent / "native-finalization-authority.json").is_file()
        ),
        None,
    )
    if authority_path is None:
        raise TemporalDiscoveryContractError(
            "native generation binding has no runtime authority root"
        )
    binding_runtime_sha256 = _sha256(
        binding.get("runtimeAuthoritySha256"),
        name="native generation binding runtime authority",
    )
    bound_binary = _pinned_native_authority_binary(
        root=authority_path.parent,
        authority_sha256=binding_runtime_sha256,
        role="generationFinalizer",
    )
    active_runtime_authority = _native_finalization_runtime_authority(binary)
    if (
        binding_runtime_sha256 == active_runtime_authority["authoritySha256"]
        and bound_binary != binary.resolve()
    ):
        raise TemporalDiscoveryContractError(
            "active native generation binary path drifted"
        )
    if binding.get("authorityMode") == "native_production_compact_commit":
        manifest = _canonical_file(
            manifest_path,
            name="native production finalization manifest",
        )
        if (
            _identity_payload(
                manifest,
                "manifestSha256",
                name="native production finalization manifest",
            )
            != binding.get("manifestSha256")
            or manifest.get("runtimeAuthoritySha256")
            != binding.get("runtimeAuthoritySha256")
        ):
            raise TemporalDiscoveryContractError(
                "native production manifest runtime authority drifted"
            )
        record_material = {
            key: value
            for key, value in generation_record.items()
            if key not in {"nativeGenerationFinalization", "generationRecordSha256"}
        }
        if canonical_sha256(record_material) != generation_record.get(
            "generationRecordSha256"
        ):
            raise TemporalDiscoveryContractError(
                "native production generation record identity mismatch"
            )
        execution = _invoke_native_finalizer(
            binary=bound_binary, manifest_path=manifest_path
        )
        if (
            execution.get("restart") is not True
            or execution.get("restartValidation")
            != "compact_commit_and_output_hashes"
            or execution.get("commitSha256") != binding.get("commitSha256")
        ):
            raise TemporalDiscoveryContractError(
                "native production compact restart authority disagrees with binding"
            )
        execution_commit = execution.get("commit")
        if not isinstance(execution_commit, Mapping):
            raise TemporalDiscoveryContractError(
                "native production compact restart omitted its commit"
            )
        committed_record_sha256 = binding.get("generationRecordSha256")
        if (
            committed_record_sha256 != generation_record.get("generationRecordSha256")
            or (execution_commit.get("generationRecord") or {}).get(
                "generationRecordSha256"
            )
            != committed_record_sha256
        ):
            raise TemporalDiscoveryContractError(
                "native production generation record binding drifted"
            )
        commit_root = Path(str(binding["commitPath"])).parent
        committed_record = _canonical_file(
            commit_root / "generation-record.json",
            name="native committed generation record",
        )
        state_patch = _canonical_file(
            commit_root / "generation-state-patch.json",
            name="native committed generation state patch",
        )
        if (
            _identity_payload(
                committed_record,
                "generationRecordSha256",
                name="native committed generation record",
            )
            != committed_record_sha256
            or _identity_payload(
                state_patch,
                "statePatchSha256",
                name="native committed generation state patch",
            )
            != (execution_commit.get("statePatch") or {}).get(
                "statePatchSha256"
            )
            or state_patch.get("generationRecordSha256")
            != committed_record_sha256
            or state_patch.get("generationRecord") != committed_record
            or {
                key: value
                for key, value in generation_record.items()
                if key != "nativeGenerationFinalization"
            }
            != committed_record
        ):
            raise TemporalDiscoveryContractError(
                "native production generation record/state patch chain drifted"
            )
        return
    authority_mode = binding.get("authorityMode")
    if authority_mode not in {
        "native_compact_commit",
        "native_explicit_python_boundary_adoption",
    }:
        raise TemporalDiscoveryContractError(
            "native generation binding has an unknown authority mode"
        )
    admission = _canonical_file(
        Path(str(binding["admissionPath"])), name="native historical admission"
    )
    supplied_admission = _sha256(
        admission.get("admissionSha256"), name="native historical admission"
    )
    admission_material = dict(admission)
    admission_material.pop("admissionSha256", None)
    if (
        canonical_sha256(admission_material) != supplied_admission
        or supplied_admission != binding.get("admissionSha256")
    ):
        raise TemporalDiscoveryContractError(
            "native historical admission identity mismatch"
        )
    if authority_mode == "native_explicit_python_boundary_adoption":
        adoption_authority = _canonical_file(
            Path(str(binding["adoptionAuthorityPath"])),
            name="Python boundary adoption authority",
        )
        authority_material = dict(adoption_authority)
        authority_material.pop("authoritySha256", None)
        if (
            adoption_authority.get("schemaVersion")
            != NATIVE_FINALIZATION_ADOPTION_AUTHORITY_SCHEMA
            or canonical_sha256(authority_material)
            != adoption_authority.get("authoritySha256")
            or adoption_authority.get("authoritySha256")
            != binding.get("adoptionAuthoritySha256")
            or adoption_authority.get("runtimeAuthoritySha256")
            != binding.get("runtimeAuthoritySha256")
            or admission.get("adoptionAuthoritySha256")
            != binding.get("adoptionAuthoritySha256")
            or admission.get("runtimeAuthoritySha256")
            != binding.get("runtimeAuthoritySha256")
        ):
            raise TemporalDiscoveryContractError(
                "explicit Python boundary adoption authority drifted"
            )
        boundary = next(
            (
                row
                for row in adoption_authority.get("boundaries") or []
                if isinstance(row, Mapping)
                and row.get("generationIndex") == generation_record.get("generationIndex")
            ),
            None,
        )
        if boundary != _python_boundary_adoption_descriptor(generation_record):
            raise TemporalDiscoveryContractError(
                "explicit Python boundary adoption record identity drifted"
            )
        manifest = _canonical_file(
            Path(str(binding["manifestPath"])),
            name="native adopted-boundary finalization manifest",
        )
        if (
            manifest.get("runtimeAuthoritySha256")
            != binding.get("runtimeAuthoritySha256")
            or manifest.get("manifestSha256") != binding.get("manifestSha256")
        ):
            raise TemporalDiscoveryContractError(
                "explicit Python boundary adoption manifest authority drifted"
            )
    current_record_material = {
        key: value
        for key, value in generation_record.items()
        if key != "nativeGenerationFinalization"
    }
    if canonical_sha256(current_record_material) != admission.get(
        "pythonGenerationRecordSha256"
    ):
        raise TemporalDiscoveryContractError(
            "native admission no longer binds the completed generation record"
        )
    for field in (
        "archiveSha256",
        "resultSetSha256",
        "rotatingEvidenceLedgerSha256",
        "rotatingEvidenceCheckpointSha256",
        "cumulativeArchiveSha256",
        "generationFunnelArtifactSha256",
        "generationFunnelSnapshotSha256",
    ):
        if generation_record.get(field) != admission.get(field):
            raise TemporalDiscoveryContractError(
                f"native admission {field} disagrees with completed generation"
            )
    execution = _invoke_native_finalizer(
        binary=bound_binary, manifest_path=manifest_path
    )
    if (
        execution.get("restart") is not True
        or execution.get("restartValidation") != "compact_commit_and_output_hashes"
        or execution.get("commitSha256") != binding.get("commitSha256")
    ):
        raise TemporalDiscoveryContractError(
            "native compact restart authority disagrees with supervisor binding"
        )


def _admit_completed_generations_native(
    *,
    root: Path,
    state: dict[str, Any],
    state_path: Path,
    config: Mapping[str, Any],
    binary: Path,
    deep_audit: bool,
    tail_result_mode: str,
    tail_result_indexes: dict[Path, dict[str, Any]],
    adoption_authority: Mapping[str, Any] | None = None,
) -> dict[int, dict[str, Any]]:
    records = _validate_completed_generation_ledger(state=state, config=config)
    changed = False
    for generation_index in sorted(records):
        record = records[generation_index]
        if record.get("nativeGenerationFinalization") is None:
            authorized_generations = set(
                (adoption_authority or {}).get("generationIndices") or []
            )
            if generation_index not in authorized_generations:
                raise TemporalDiscoveryContractError(
                    "Rust resume cannot silently adopt a Python-completed boundary"
                )
            # Adoption always reopens and validates the complete Python boundary.
            # The ordinary compact-restart deep-audit preference cannot weaken
            # this one-time trust transition.
            _validate_generation_artifacts(
                root=root,
                generation_record=record,
                config=config,
                tail_result_mode=tail_result_mode,
                tail_result_indexes=tail_result_indexes,
            )
            manifest, manifest_path = _native_independent_finalizer_manifest(
                root=root,
                config=config,
                generation_index=generation_index,
                tail_result_indexes=tail_result_indexes,
                tail_reducer_binary=binary.with_name(
                    "temporal-qd-tail-reducer.exe"
                    if os.name == "nt"
                    else "temporal-qd-tail-reducer"
                ),
                completed_at=str(record["completedAt"]),
            )
            execution = _invoke_native_finalizer(
                binary=binary, manifest_path=manifest_path
            )
            commit = _validate_native_migration_outputs(
                root=root, generation_record=record, execution=execution
            )
            binding = _native_generation_binding(
                root=root,
                generation_record=record,
                manifest=manifest,
                commit=commit,
                adoption_authority=adoption_authority,
            )
            for state_record in state["completedGenerations"]:
                if int(state_record["generationIndex"]) == generation_index:
                    state_record["nativeGenerationFinalization"] = binding
                    break
            changed = True
            _event(
                "native_generation_explicitly_adopted",
                generationIndex=generation_index,
                commitSha256=commit["commitSha256"],
                adoptionAuthoritySha256=adoption_authority["authoritySha256"],
            )
        else:
            _validate_native_generation_binding(
                generation_record=record, binary=binary
            )
            if deep_audit:
                _validate_generation_artifacts(
                    root=root,
                    generation_record=record,
                    config=config,
                    tail_result_mode=tail_result_mode,
                    tail_result_indexes=tail_result_indexes,
                )
    if changed:
        _save_state(state_path, state)
    return _validate_completed_generation_ledger(state=state, config=config)


def _validate_completed_generations_native_engine(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
    binary: Path,
) -> dict[int, dict[str, Any]]:
    """Recover production generations from compact native commits only."""

    records = _validate_completed_generation_ledger(state=state, config=config)
    for generation_index in sorted(records):
        record = records[generation_index]
        _validate_native_generation_binding(generation_record=record, binary=binary)
        _publish_native_generation_outputs(
            root=root, generation_index=generation_index
        )
    return records


def _validate_published_generation_boundary(
    *,
    root: Path,
    state: Mapping[str, Any],
    config: Mapping[str, Any],
    generation_index: int,
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    tail_result_indexes: dict[Path, dict[str, Any]] | None = None,
) -> None:
    """Validate only the just-published immutable generation boundary.

    The run has already admitted every historical generation at startup.  At
    a new state-save boundary, reopening history would both duplicate source
    verification and evict the active generation's retained projection.
    """

    records = _validate_completed_generation_ledger(state=state, config=config)
    record = records.get(generation_index)
    if record is None:
        raise TemporalDiscoveryContractError(
            "published QD generation is missing from completed state"
        )
    _validate_generation_artifacts(
        root=root,
        generation_record=record,
        config=config,
        tail_result_mode=tail_result_mode,
        tail_result_indexes=tail_result_indexes,
    )


def _validate_evidence_ladder_execution(
    *, root: Path, state: Mapping[str, Any], config: Mapping[str, Any]
) -> None:
    """Reopen the immutable 12m/36m result bundle of a completed ladder run."""

    ladder = config.get("evidenceLadder")
    if ladder is None:
        if state.get("evidenceLadderExecution") is not None:
            raise TemporalDiscoveryContractError(
                "completed QD supervisor state has an unexpected evidence ladder execution"
            )
        return
    if not isinstance(ladder, Mapping):
        raise TemporalDiscoveryContractError("QD evidence ladder is invalid")
    recorded = state.get("evidenceLadderExecution")
    if not isinstance(recorded, Mapping):
        raise TemporalDiscoveryContractError(
            "completed QD supervisor state lacks evidence ladder execution"
        )
    execution_path = root / "evidence-ladder" / "execution.json"
    execution = _canonical_file(execution_path, name="QD evidence ladder execution")
    if _clone(execution, name="QD evidence ladder execution") != _clone(
        recorded, name="completed QD evidence ladder execution"
    ):
        raise TemporalDiscoveryContractError(
            "completed QD evidence ladder execution disagrees with state"
        )
    supplied_execution_sha = _sha256(
        execution.get("executionSha256"), name="QD evidence ladder execution"
    )
    material = _clone(execution, name="QD evidence ladder execution")
    material.pop("executionSha256", None)
    if canonical_sha256(material) != supplied_execution_sha:
        raise TemporalDiscoveryContractError("QD evidence ladder execution identity mismatch")
    if execution.get("schemaVersion") != "temporal_qd_evidence_ladder_execution_result_v1":
        raise TemporalDiscoveryContractError("QD evidence ladder execution schema is invalid")
    if execution.get("evidenceLadderSha256") != ladder.get("evidenceLadderSha256"):
        raise TemporalDiscoveryContractError("QD evidence ladder execution binding drifted")
    if _clone(execution.get("outerTail"), name="QD evidence ladder execution tail") != _clone(
        ladder.get("outerTail"), name="frozen QD evidence ladder tail"
    ):
        raise TemporalDiscoveryContractError("QD evidence ladder outer-tail binding drifted")

    for stage in ("validation", "scrutiny"):
        stage_record = execution.get(stage)
        if not isinstance(stage_record, Mapping):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} stage record is invalid"
            )
        stage_root = root / "evidence-ladder" / stage
        expected_paths = {
            "populationPath": stage_root / "population.json",
            "campaignPath": stage_root / "campaign" / "campaign.json",
            "archivePath": stage_root / "archive.json",
        }
        for field, expected_path in expected_paths.items():
            supplied_path = Path(str(stage_record.get(field) or ""))
            if supplied_path.resolve() != expected_path.resolve():
                raise TemporalDiscoveryContractError(
                    f"QD evidence ladder {stage} {field} is not bound to its run root"
                )
        recorded_artifacts = stage_record.get("artifacts")
        if not isinstance(recorded_artifacts, Mapping):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} lacks its immutable artifact ledger"
            )
        current_artifacts = _capture_screening_artifacts(
            population_path=expected_paths["populationPath"],
            archive_path=expected_paths["archivePath"],
            campaign_root=stage_root / "campaign",
            generation_index=0,
            label=f"QD {stage} ladder",
        )
        if _clone(current_artifacts, name=f"QD {stage} ladder artifacts") != _clone(
            recorded_artifacts, name=f"recorded QD {stage} ladder artifacts"
        ):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} artifact ledger drifted from immutable outputs"
            )
        population = _canonical_file(
            expected_paths["populationPath"], name=f"QD {stage} ladder population"
        )
        campaign = _canonical_file(
            expected_paths["campaignPath"], name=f"QD {stage} ladder campaign"
        )
        archive = _canonical_file(
            expected_paths["archivePath"], name=f"QD {stage} ladder archive"
        )
        population_sha = _identity_payload(
            population, "populationSha256", name=f"QD {stage} ladder population"
        )
        campaign_sha = _identity_payload(
            campaign, "campaignSha256", name=f"QD {stage} ladder campaign"
        )
        archive_sha = _identity_payload(
            archive, "archiveSha256", name=f"QD {stage} ladder archive"
        )
        if (
            stage_record.get("populationSha256") != population_sha
            or stage_record.get("campaignSha256") != campaign_sha
            or stage_record.get("archiveSha256") != archive_sha
        ):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} artifact identity drifted"
            )
        if (
            campaign.get("populationSha256") != population_sha
            or archive.get("populationSha256") != population_sha
        ):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} population binding drifted"
            )
        candidate_count = int(stage_record.get("candidateCount") or -1)
        if (
            candidate_count < 1
            or candidate_count != int(population.get("candidateCount") or -1)
            or candidate_count != int(campaign.get("candidateCount") or -1)
        ):
            raise TemporalDiscoveryContractError(
                f"QD evidence ladder {stage} candidate count drifted"
            )


def _continuation_binding(
    source_run_root: Path | str, *, _seen_roots: frozenset[Path] = frozenset()
) -> dict[str, Any]:
    """Read a completed fresh-five or legacy-four source without changing it."""

    root = Path(source_run_root).resolve()
    if root in _seen_roots:
        raise TemporalDiscoveryContractError("QD continuation chain contains a cycle")
    config = _canonical_file(root / "config.json", name="QD continuation source config")
    config_sha = _sha256(config.get("configSha256"), name="QD continuation source config")
    material = _clone(config, name="QD continuation source config")
    material.pop("configSha256", None)
    if canonical_sha256(material) != config_sha:
        raise TemporalDiscoveryContractError("QD continuation source config identity mismatch")
    plan = config.get("generationPlan") or {}
    source_first = int(plan.get("firstGenerationIndex") or -1)
    source_count = int(plan.get("generationCount") or -1)
    if source_first < 1 or source_count not in {
        FRESH_BROAD_GENERATION_COUNT,
        LEGACY_CONTINUATION_GENERATION_COUNT,
    }:
        raise TemporalDiscoveryContractError(
            "QD continuation source must be a completed fresh five-generation or legacy four-generation campaign"
        )
    if config.get("broadAdmission") is not True:
        raise TemporalDiscoveryContractError(
            "QD continuation source was not admitted as a broad campaign"
        )
    contract = config.get("broadAdmissionContract")
    rotating = config.get("rotatingEvidence")
    initial_parent_count = 0
    if isinstance(rotating, Mapping):
        initial_binding = config.get("initialArchive")
        if not isinstance(initial_binding, Mapping):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation source lacks its initial archive binding"
            )
        initial_archive, initial_sha = _load_archive(
            Path(str(initial_binding.get("path") or ""))
        )
        if initial_sha != initial_binding.get("archiveSha256"):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation initial archive drifted"
            )
        initial_parent_count = _archive_member_count(initial_archive)
    expected_contract = (
        _rotating_task_upper_bounds(
            contract=rotating,
            first_generation_index=source_first,
            generation_count=source_count,
            proposal_width=int(plan.get("targetUniqueCandidatesPerGeneration") or 0),
            initial_parent_count=initial_parent_count,
        )
        if isinstance(rotating, Mapping)
        else _broad_admission_contract_values(source_count)
    )
    if not isinstance(contract, Mapping) or contract.get(
        "schemaVersion"
    ) != "temporal_qd_broad_admission_contract_v1":
        raise TemporalDiscoveryContractError(
            "QD continuation source broad admission contract is unavailable"
        )
    if any(contract.get(key) != value for key, value in expected_contract.items()):
        raise TemporalDiscoveryContractError(
            "QD continuation source broad admission contract does not match its generation plan"
        )
    source_last = source_first + source_count - 1
    state = _load_state(root / "state.json", config_sha256=config_sha)
    if state.get("status") != "completed":
        raise TemporalDiscoveryContractError("QD continuation source campaign is not completed")
    completed = _validate_completed_generations(root=root, state=state, config=config)
    _validate_evidence_ladder_execution(root=root, state=state, config=config)
    expected_generations = set(range(source_first, source_last + 1))
    if set(completed) != expected_generations:
        raise TemporalDiscoveryContractError(
            "QD continuation source lacks its immutable contiguous fresh five-generation or legacy four-generation campaign"
        )
    latest = completed[source_last]
    archive_path = Path(str(latest.get("archivePath") or ""))
    if not archive_path.is_file():
        raise TemporalDiscoveryContractError("QD continuation source archive is missing")
    prior = config.get("continuationFrom")
    prior_binding: dict[str, Any] | None = None
    if prior is None:
        if source_first != 1:
            raise TemporalDiscoveryContractError(
                "QD root continuation source must begin at generation 1"
            )
    else:
        if source_count != LEGACY_CONTINUATION_GENERATION_COUNT:
            raise TemporalDiscoveryContractError(
                "QD fresh five-generation source cannot be a chained continuation"
            )
        if not isinstance(prior, Mapping):
            raise TemporalDiscoveryContractError("QD continuation prior-chain binding is invalid")
        prior_binding = _continuation_binding(
            str(prior.get("sourceRunRoot") or ""),
            _seen_roots=_seen_roots | frozenset({root}),
        )
        if _clone(prior_binding, name="reopened prior QD continuation binding") != _clone(
            prior, name="frozen prior QD continuation binding"
        ):
            raise TemporalDiscoveryContractError("QD continuation prior chain drifted")
        if source_first != int(prior_binding["sourceLastGenerationIndex"]) + 1:
            raise TemporalDiscoveryContractError(
                "QD continuation source does not begin immediately after its prior source"
            )
    semantic_authority = None
    if isinstance(rotating, Mapping):
        repositories = config.get("repositories")
        evaluation = config.get("evaluation")
        if not isinstance(repositories, Mapping) or not isinstance(evaluation, Mapping):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation source lacks evidence semantics"
            )
        construction = config.get("constructionOperatorPolicy")
        if construction is not None and not isinstance(construction, Mapping):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation construction policy is invalid"
            )
        cost_views = evaluation.get("costViews")
        predeclared_context = evaluation.get("predeclaredEvidenceContext")
        if not isinstance(cost_views, Mapping):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation cost views are invalid"
            )
        if not isinstance(predeclared_context, Mapping):
            raise TemporalDiscoveryContractError(
                "QD rotating continuation evidence context is invalid"
            )
        semantic_authority = _rotating_evidence_semantic_authority(
            execution_engine_commit=str(
                repositories.get("executionEngineCommit") or ""
            ),
            worker_contract_sha256=str(config.get("workerContractSha256") or ""),
            construction_operator_policy=construction,
            base_decision_timeframe=str(
                predeclared_context.get("baseDecisionTimeframe")
                or ""
            ),
            cost_views=cost_views,
        )
    return {
        "schemaVersion": "temporal_qd_generation_continuation_v1",
        "sourceRunRoot": str(root),
        "sourceConfigSha256": config_sha,
        "sourceStateSha256": state["stateSha256"],
        "sourceFirstGenerationIndex": source_first,
        "sourceLastGenerationIndex": source_last,
        "sourceArchivePath": str(archive_path.resolve()),
        "sourceArchiveSha256": latest["archiveSha256"],
        "nextImmigrantContinuationOrdinal": latest["nextImmigrantContinuationOrdinal"],
        **(
            {
                "rotatingEvidenceSha256": rotating["rotatingEvidenceSha256"],
                "sourceCumulativeArchiveSha256": latest[
                    "cumulativeArchiveSha256"
                ],
                "sourceEvidenceSemanticAuthority": semantic_authority,
            }
            if isinstance(rotating, Mapping)
            else {}
        ),
        **(
            {
                "priorContinuationFrom": prior_binding
            }
            if prior_binding is not None
            else {}
        ),
    }


def _resolve_g0_finalization_runtime_for_reopen(
    *,
    config: Mapping[str, Any],
    pair_runtime: Mapping[str, Any] | None,
    run_root: Path | None,
) -> dict[str, Any] | None:
    """Resolve only an explicit runtime or the singleton pre-cutover receipt.

    New configs must continue to carry ``g0FinalizationRuntime``.  The one
    preserved v5 checkpoint predates that field, so it can reopen only through
    the immutable migration receipt produced after native G0 sealing.
    """

    g0_bootstrap = config.get("g0Bootstrap")
    runtime_raw = config.get("g0FinalizationRuntime")
    if runtime_raw is not None:
        try:
            return validate_g0_finalization_runtime_config(runtime_raw)
        except TemporalQDNativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
    if not (
        isinstance(g0_bootstrap, Mapping)
        and pair_runtime is not None
        and pair_runtime.get("engine") == PAIR_GENERATION_RUNTIME_PYTHON
    ):
        return None
    if run_root is None:
        raise TemporalDiscoveryContractError(
            "Python-owned G0 construction lacks its frozen Rust finalization runtime"
        )
    try:
        return load_legacy_v5_g0_finalization_runtime(
            supervisor_config=config, run_root=run_root
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc


def _open_legacy_v5_g0_reopen_authority(
    *, root: Path
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Open the one receipt-authorized config that predates G0 runtime freezing.

    This runs before ``_frozen_config`` on restart.  Reconstructing the newer
    config would add ``g0FinalizationRuntime`` and make byte-exact write-once
    comparison reject the preserved checkpoint before its migration receipt
    can be examined.  The old config itself remains the immutable authority;
    the Rust runtime is an effective, separately verified value.
    """

    config_path = root / "config.json"
    if not config_path.exists():
        return None
    if not config_path.is_file():
        raise TemporalDiscoveryContractError(
            "QD supervisor frozen config is not a regular file"
        )
    config = _read(config_path, name="QD supervisor frozen config")
    if "g0FinalizationRuntime" in config:
        return None
    g0_bootstrap = config.get("g0Bootstrap")
    if not isinstance(g0_bootstrap, Mapping):
        return None
    if config.get("bidirectionalPairGeneration") is None:
        raise TemporalDiscoveryContractError(
            "legacy G0 runtime migration requires frozen bidirectional pair authority"
        )
    try:
        pair_runtime = validate_pair_generation_runtime_config(
            config.get("pairGenerationRuntime")
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    if pair_runtime["engine"] != PAIR_GENERATION_RUNTIME_PYTHON:
        # Current native-v5 configs do not carry the retired, independent G0
        # finalization runtime.  A sealed Rust proposal may stop before its
        # first generation commit, so config presence alone cannot classify
        # the run as the singleton pre-cutover Python migration.
        return None
    expected_config_sha = _sha256(
        config.get("configSha256"), name="legacy QD supervisor config"
    )
    material = _clone(config, name="legacy QD supervisor config")
    material.pop("configSha256", None)
    if canonical_sha256(material) != expected_config_sha:
        raise TemporalDiscoveryContractError(
            "legacy QD supervisor frozen config identity mismatch"
        )
    try:
        runtime = load_legacy_v5_g0_finalization_runtime(
            supervisor_config=config, run_root=root
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    if runtime["engine"] != G0_FINALIZATION_RUNTIME_RUST:
        raise TemporalDiscoveryContractError(
            "legacy G0 migration did not resolve the Rust finalization runtime"
        )
    return _clone(config, name="legacy QD supervisor frozen config"), runtime


def _validate_frozen_sources(
    config: Mapping[str, Any], *, run_root: Path | None = None
) -> list[str]:
    """Reopen every path-backed source before each phase can consume it."""

    expected_config_sha = _sha256(config.get("configSha256"), name="supervisor config")
    material = _clone(config, name="QD supervisor config")
    material.pop("configSha256", None)
    if canonical_sha256(material) != expected_config_sha:
        raise TemporalDiscoveryContractError("QD supervisor frozen config identity mismatch")
    continuation = config.get("continuationFrom")
    if continuation is not None:
        if not isinstance(continuation, Mapping) or _clone(
            _continuation_binding(str(continuation.get("sourceRunRoot") or "")),
            name="reopened QD continuation binding",
        ) != _clone(continuation, name="frozen QD continuation binding"):
            raise TemporalDiscoveryContractError("QD continuation source drifted")

    archive_binding = config.get("initialArchive")
    if not isinstance(archive_binding, Mapping):
        raise TemporalDiscoveryContractError("QD supervisor initial archive binding is invalid")
    pair_config = config.get("bidirectionalPairGeneration")
    pair_runtime = None
    if pair_config is not None:
        try:
            pair_runtime = validate_pair_generation_runtime_config(
                config.get("pairGenerationRuntime")
            )
        except TemporalQDNativeError as exc:
            raise TemporalDiscoveryContractError(str(exc)) from exc
    native_pair_generation = bool(
        pair_runtime is not None
        and pair_runtime["engine"] == PAIR_GENERATION_RUNTIME_RUST
    )
    v5_authority = _v5_evolvable_authority(config)
    native_v5_proposal_transaction = _native_v5_proposal_enabled(config)
    if native_v5_proposal_transaction:
        if not native_pair_generation:
            raise TemporalDiscoveryContractError(
                "fresh evolvable v5 construction requires the Rust-native v5 transaction"
            )
        runtime, bindings = _validate_native_v5_proposal_runtime(config=config)
        if (
            bindings.get("runConfig") != pair_config
            or bindings.get("archivePolicyAuthority")
            != v5_authority.get("archivePolicyAuthority")
            or bindings.get("behaviorAttributionRequirement")
            != v5_authority.get("behaviorAttributionRequirement")
            or bindings.get("operatorImplementation")
            != pair_config.get("operatorImplementation")
            or bindings.get("capacityReceipt") != pair_config.get("capacityReceipt")
            or runtime.get("engine") != NATIVE_V5_PROPOSAL_ENGINE
        ):
            raise TemporalDiscoveryContractError("native v5 generation bindings drifted")
    g0_bootstrap = config.get("g0Bootstrap")
    g0_runtime = _resolve_g0_finalization_runtime_for_reopen(
        config=config, pair_runtime=pair_runtime, run_root=run_root
    )
    if g0_runtime is not None:
        if (
            not isinstance(g0_bootstrap, Mapping)
            or pair_runtime is None
            or pair_runtime["engine"] != PAIR_GENERATION_RUNTIME_PYTHON
            or g0_runtime["engine"] != G0_FINALIZATION_RUNTIME_RUST
        ):
            raise TemporalDiscoveryContractError(
                "G0 finalization runtime must be the Rust-only post-construction authority"
            )
    elif (
        isinstance(g0_bootstrap, Mapping)
        and pair_runtime is not None
        and pair_runtime["engine"] == PAIR_GENERATION_RUNTIME_PYTHON
    ):
        raise TemporalDiscoveryContractError(
            "Python-owned G0 construction lacks its frozen Rust finalization runtime"
        )
    if native_v5_proposal_transaction:
        transport = archive_binding.get("transportDescriptor")
        if not isinstance(transport, Mapping) or set(transport) != {
            "schemaVersion",
            "absolutePath",
            "documentSchemaVersion",
            "archiveSha256",
            "fileSha256",
            "sizeBytes",
            "descriptorSha256",
        }:
            raise TemporalDiscoveryContractError(
                "QD supervisor native v5 initial archive transport is invalid"
            )
        transport_body = _clone(transport, name="native v5 initial archive transport")
        supplied_transport_sha = _sha256(
            transport_body.pop("descriptorSha256", None),
            name="native v5 initial archive transport identity",
        )
        path = archive_binding.get("path")
        if (
            transport.get("schemaVersion")
            != "temporal_qd_archive_transport_descriptor_v1"
            or not native_v5_archive_transport_path_matches(
                transport.get("absolutePath"), path
            )
            or transport.get("documentSchemaVersion") != "temporal_qd_archive_v3"
            or transport.get("archiveSha256") != archive_binding.get("archiveSha256")
            or canonical_sha256(transport_body) != supplied_transport_sha
        ):
            raise TemporalDiscoveryContractError(
                "QD supervisor native v5 initial archive transport drifted"
            )
        _sha256(transport.get("fileSha256"), name="native v5 initial archive file identity")
        size = transport.get("sizeBytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise TemporalDiscoveryContractError(
                "QD supervisor native v5 initial archive transport byte length is invalid"
            )
        # This cross-binding is a bounded config-only assertion.  Keep it in
        # the current-v5 path even though the archive/template/catalog bodies
        # are Rust-owned: otherwise a re-signed envelope could silently make
        # the evaluator's required attribution weaker than the proposal
        # authority that Rust will receive.
        evaluation = config.get("evaluation")
        if (
            not isinstance(evaluation, Mapping)
            or not isinstance(v5_authority, Mapping)
            or evaluation.get("behaviorAttributionRequirement")
            != v5_authority.get("behaviorAttributionRequirement")
        ):
            raise TemporalDiscoveryContractError(
                "evolvable v5 evaluation behavior attribution requirement drifted"
            )
        # Current v5 intentionally stops here.  Pair bundle hydration,
        # template/catalog parsing, and archive decoding belong either to the
        # historical branch below or to their receipt-bound Rust transaction.
        if run_root is not None:
            _require_native_v5_control_plane_runtime_authority(
                _native_runtime_authority_for_generation(
                    root=run_root,
                    generation_index=int(
                        (config.get("generationPlan") or {}).get(
                            "firstGenerationIndex", 1
                        )
                    ),
                )
            )
        return []
    elif native_pair_generation:
        archive_path = Path(str(archive_binding.get("path") or ""))
        if (
            not archive_path.is_file()
            or _sha256(
                archive_binding.get("archiveSha256"),
                name="frozen native initial archive",
            )
            != archive_binding.get("archiveSha256")
            or _sha256(
                archive_binding.get("resultSetSha256"),
                name="frozen native initial result set",
            )
            != archive_binding.get("resultSetSha256")
        ):
            raise TemporalDiscoveryContractError(
                "QD supervisor native initial archive binding is invalid"
            )
    else:
        archive, archive_sha = _load_archive(
            Path(str(archive_binding.get("path") or ""))
        )
        if archive_sha != archive_binding.get("archiveSha256") or (
            archive.get("resultSetSha256") != archive_binding.get("resultSetSha256")
        ):
            raise TemporalDiscoveryContractError("QD supervisor initial archive drifted")

    if pair_config is None:
        source_binding = config.get("immigrantSource")
        if not isinstance(source_binding, Mapping):
            raise TemporalDiscoveryContractError("QD supervisor immigrant source binding is invalid")
        source = ExactGeneratorV2Continuation(
            source_preparation_path=Path(str(source_binding.get("sourcePreparationPath") or "")),
            base_generator_root=Path(str(source_binding.get("baseGeneratorRoot") or "")),
            confirmed_entry_admission_root=Path(str(source_binding.get("confirmedEntryAdmissionRoot") or "")),
            start_continuation_ordinal=0,
        )
        if _clone(source.source_identity, name="reopened immigrant source") != _clone(source_binding.get("sourceIdentity"), name="frozen immigrant source"):
            raise TemporalDiscoveryContractError("QD supervisor immigrant source drifted")
    else:
        # Rebuilds the concrete typed/native authorities and validates every
        # frozen registry/catalog/transport identity before a resume can run.
        evolvable_config = config.get("evolvableModuleAuthority")
        source_pair_config = (
            config.get("bidirectionalPairSourceAuthority")
            if isinstance(evolvable_config, Mapping)
            else pair_config
        )
        if not isinstance(source_pair_config, Mapping):
            raise TemporalDiscoveryContractError(
                "evolvable v5 pair source authority is unavailable"
            )
        if not native_pair_generation:
            with PairAuthorityBundle(
                _clone(source_pair_config, name="frozen pair source authority")
            ) as bundle:
                if isinstance(evolvable_config, Mapping):
                    evolvable = bundle.open_evolvable_module_authority(
                        evolvable_config
                    )
                    bindings = evolvable.generation_bindings(pair_config)
                    if (
                        bindings["runConfig"] != pair_config
                        or bindings["archivePolicyAuthority"]
                        != evolvable_config.get("archivePolicyAuthority")
                        or bindings["behaviorAttributionRequirement"]
                        != evolvable_config.get("behaviorAttributionRequirement")
                        or bindings["operatorImplementation"]
                        != pair_config.get("operatorImplementation")
                        or bindings["capacityReceipt"]
                        != pair_config.get("capacityReceipt")
                    ):
                        raise TemporalDiscoveryContractError(
                            "evolvable v5 generation bindings drifted"
                        )
        if config.get("broadAdmission") is True:
            contract = config.get("broadAdmissionContract")
            if not isinstance(contract, Mapping):
                raise TemporalDiscoveryContractError(
                    "QD broad admission contract is unavailable"
                )
            required_unique_candidates = _require_frozen_immigrant_capacity_requirement(
                config, contract
            )
            if isinstance(evolvable_config, Mapping):
                receipt = contract.get("evolvableFactoryCapacityReceipt")
                if not isinstance(receipt, Mapping):
                    raise TemporalDiscoveryContractError(
                        "evolvable v5 broad admission lacks its sealed actual-factory capacity receipt"
                    )
                if receipt != pair_config.get("capacityReceipt"):
                    raise TemporalDiscoveryContractError(
                        "evolvable v5 broad admission capacity receipt drifted"
                    )
                _require_evolvable_capacity_receipt_supply(
                    receipt,
                    required_unique_candidates=required_unique_candidates,
                )
            else:
                current_capacity = immigrant_capacity_audit(
                    pair_config,
                    required_unique_candidates=required_unique_candidates,
                )
                if _clone(
                    current_capacity, name="reopened pair immigrant capacity audit"
                ) != _clone(
                    contract.get("immigrantConstructionCapacity"),
                    name="frozen pair immigrant capacity audit",
                ):
                    raise TemporalDiscoveryContractError(
                        "QD broad immigrant construction capacity audit drifted"
                    )

    validator_binding = config.get("validator")
    if pair_config is None:
        if not isinstance(validator_binding, Mapping):
            raise TemporalDiscoveryContractError("QD supervisor validator binding is invalid")
        command = _command(Path(str(validator_binding.get("commandFile") or "")))
        if command != validator_binding.get("command") or canonical_sha256(command) != validator_binding.get("commandSha256"):
            raise TemporalDiscoveryContractError("QD supervisor validator command drifted")
    else:
        command = []

    evaluation = config.get("evaluation")
    if not isinstance(evaluation, Mapping):
        raise TemporalDiscoveryContractError("QD supervisor evaluation binding is invalid")
    evolvable_config = config.get("evolvableModuleAuthority")
    if isinstance(evolvable_config, Mapping) and (
        evaluation.get("behaviorAttributionRequirement")
        != evolvable_config.get("behaviorAttributionRequirement")
    ):
        raise TemporalDiscoveryContractError(
            "evolvable v5 evaluation behavior attribution requirement drifted"
        )
    template_path = Path(str(evaluation.get("templatePreparationPath") or ""))
    template = _canonical_file(template_path, name="QD template preparation")
    if canonical_sha256(template) != evaluation.get("templatePreparationSha256"):
        raise TemporalDiscoveryContractError("QD supervisor template preparation drifted")
    construction = config.get("constructionOperatorPolicy")
    construction_catalog_payload: Mapping[str, Any] | None = None
    construction_catalog_path: Path | None = None
    if construction is not None:
        if not isinstance(construction, Mapping):
            raise TemporalDiscoveryContractError("QD construction operator policy is invalid")
        catalog = construction.get("catalog")
        if not isinstance(catalog, Mapping):
            raise TemporalDiscoveryContractError("QD construction catalog binding is invalid")
        construction_catalog_path = Path(str(catalog.get("path") or ""))
        current_policy, registry = qd_construction_operator_policy(
            construction_catalog_path
        )
        if _clone(current_policy, name="reopened construction policy") != _clone(
            construction, name="frozen construction policy"
        ):
            raise TemporalDiscoveryContractError("QD construction catalog or policy drifted")
        if registry is None:
            raise TemporalDiscoveryContractError("QD construction registry is unavailable")
        construction_catalog_payload = registry.catalog.payload
    evidence_context = qd_predeclared_evidence_context(
        template,
        worker_contract_sha256=config.get("workerContractSha256"),
        construction_catalog=construction_catalog_payload,
        construction_catalog_path=construction_catalog_path,
    )
    if _clone(evidence_context, name="reopened predeclared evidence context") != _clone(
        evaluation.get("predeclaredEvidenceContext"),
        name="frozen predeclared evidence context",
    ):
        raise TemporalDiscoveryContractError(
            "QD supervisor predeclared evidence context drifted"
        )
    if evidence_context["predeclaredEvidenceContextSha256"] != evaluation.get(
        "predeclaredEvidenceContextSha256"
    ):
        raise TemporalDiscoveryContractError(
            "QD supervisor predeclared evidence identity drifted"
        )
    ladder = config.get("evidenceLadder")
    ladder_execution = config.get("evidenceLadderExecution")
    if ladder is not None:
        validate_template_discovery_windows(template, ladder)
        if not isinstance(ladder_execution, Mapping):
            raise TemporalDiscoveryContractError("QD evidence ladder execution binding is invalid")
        for stage in ("validation", "scrutiny"):
            binding = ladder_execution.get(stage + "Template")
            if not isinstance(binding, Mapping):
                raise TemporalDiscoveryContractError(f"QD {stage} ladder template binding is invalid")
            stage_template = _canonical_file(Path(str(binding.get("path") or "")), name=f"QD {stage} ladder template")
            if canonical_sha256(stage_template) != binding.get("sha256"):
                raise TemporalDiscoveryContractError(f"QD {stage} ladder template drifted")
            validate_template_stage_window(stage_template, ladder, stage=stage)
    rotating = config.get("rotatingEvidence")
    if rotating is not None:
        rotating = validate_rotating_evidence_contract(rotating)
        for panel in rotating["panels"]:
            binding = rotating["panelTemplates"][panel["panelId"]]
            panel_template = _canonical_file(
                Path(binding["path"]), name=f"rotating {panel['panelId']} template"
            )
            validate_panel_template(panel_template, rotating, panel["panelId"])

    return command


def _frozen_config(
    *,
    initial_archive_path: Path,
    source_preparation_path: Path,
    base_generator_root: Path,
    confirmed_entry_admission_root: Path,
    template_preparation_path: Path,
    validator_command_file: Path | None,
    parameters: Mapping[str, Any],
    generation_count: int,
    first_generation_index: int,
    initial_immigrant_continuation_ordinal: int,
    autoresearch_commit: str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    gateway_url: str,
    evaluation_timeout_seconds: float,
    enqueue_batch_size: int,
    broad_admission: bool,
    generation_funnel_enabled: bool = False,
    construction_catalog_path: Path | str | None = None,
    bidirectional_pair_config: Mapping[str, Any] | None = None,
    pair_generation_engine: str | None = None,
    pair_generation_timeout_seconds: int = 3600,
    evidence_ladder_config: Mapping[str, Any] | None = None,
    rotating_evidence_config: Mapping[str, Any] | None = None,
    continuation_from: Mapping[str, Any] | None = None,
    initial_construction_pool_size: int | None = None,
    evaluation_population_size: int | None = None,
    evolvable_module_authority_config: Mapping[str, Any] | None = None,
    initial_archive_transport_descriptor: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    if generation_count < 1 or first_generation_index < 1:
        raise TemporalDiscoveryContractError(
            "QD supervisor requires positive generation bounds"
        )
    if initial_immigrant_continuation_ordinal < 0:
        raise TemporalDiscoveryContractError("initial immigrant cursor is negative")
    if not 1 <= enqueue_batch_size <= 1000:
        raise TemporalDiscoveryContractError("enqueue batch size is outside 1..1000")
    if evaluation_timeout_seconds < 60:
        raise TemporalDiscoveryContractError(
            "generation evaluation timeout must be at least 60 seconds"
        )
    normalized_parameters = _normalize_parameters(parameters)
    evaluation_target = generation_count * int(
        normalized_parameters["targetUniqueCandidates"]
    )
    is_continuation = continuation_from is not None
    expected_broad_generation_count = (
        LEGACY_CONTINUATION_GENERATION_COUNT
        if is_continuation
        else FRESH_BROAD_GENERATION_COUNT
    )
    if broad_admission and (
        generation_count != expected_broad_generation_count
        or int(normalized_parameters["targetUniqueCandidates"])
        != FRESH_BROAD_CANDIDATES_PER_GENERATION
        or (evidence_ladder_config is None and rotating_evidence_config is None)
    ):
        raise TemporalDiscoveryContractError(
                "fresh broad admission requires a frozen evidence ladder or rotating evidence contract and the frozen five-generation x 1,024-candidate contract; a continuation requires exactly four generations"
        )
    evolvable_authority = (
        _clone(
            evolvable_module_authority_config,
            name="evolvable module authority config",
        )
        if evolvable_module_authority_config is not None
        else None
    )
    # A current v5 run receives a Rust-certified archive transport, not an
    # archive payload.  Reading the archive here used to make fresh G0
    # creation a Python candidate/archive authority before the first native
    # transaction existed.  The certified descriptor is the only initial
    # archive material Python may carry; qd-batch reopens it when needed.
    initial_archive_transport: dict[str, Any] | None = None
    initial_archive: dict[str, Any] | None = None
    initial_parent_schedule: dict[str, Any] | None = None
    initial_parent_count = 0
    if evolvable_authority is not None:
        if initial_archive_transport_descriptor is None:
            raise TemporalDiscoveryContractError(
                "fresh current v5 requires a Rust-certified initial archive descriptor"
            )
        raw_transport = _clone(
            initial_archive_transport_descriptor,
            name="native v5 initial archive transport descriptor",
        )
        if not isinstance(raw_transport, Mapping) or set(raw_transport) != {
            "schemaVersion",
            "absolutePath",
            "documentSchemaVersion",
            "archiveSha256",
            "fileSha256",
            "sizeBytes",
            "descriptorSha256",
        }:
            raise TemporalDiscoveryContractError(
                "native v5 initial archive transport descriptor schema drifted"
            )
        descriptor_body = dict(raw_transport)
        descriptor_sha = _sha256(
            descriptor_body.pop("descriptorSha256", None),
            name="native v5 initial archive transport identity",
        )
        if (
            raw_transport.get("schemaVersion")
            != "temporal_qd_archive_transport_descriptor_v1"
            or not native_v5_archive_transport_path_matches(
                raw_transport.get("absolutePath"),
                str(initial_archive_path.resolve()),
            )
            or raw_transport.get("documentSchemaVersion") != "temporal_qd_archive_v3"
            or canonical_sha256(descriptor_body) != descriptor_sha
        ):
            raise TemporalDiscoveryContractError(
                "native v5 initial archive transport descriptor binding drifted"
            )
        initial_archive_sha = _sha256(
            raw_transport.get("archiveSha256"),
            name="native v5 initial archive transport semantic identity",
        )
        _sha256(
            raw_transport.get("fileSha256"),
            name="native v5 initial archive transport file identity",
        )
        size = raw_transport.get("sizeBytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise TemporalDiscoveryContractError(
                "native v5 initial archive transport byte length is invalid"
            )
        initial_archive_transport = dict(raw_transport)
    else:
        if initial_archive_transport_descriptor is not None:
            raise TemporalDiscoveryContractError(
                "historical supervisor cannot carry a native v5 initial archive descriptor"
            )
        initial_archive, initial_archive_sha = _load_archive(initial_archive_path)
        initial_parent_schedule = _rotating_parent_schedule(initial_archive)
        initial_parent_count = _archive_member_count(initial_archive)
    template = _read(template_preparation_path, name="QD template preparation")
    evidence_ladder = (
        build_evidence_ladder(evidence_ladder_config)
        if evidence_ladder_config is not None
        else None
    )
    if evidence_ladder_config is not None and rotating_evidence_config is not None:
        raise TemporalDiscoveryContractError("QD supervisor cannot combine legacy and rotating evidence")
    rotating_evidence = (
        build_rotating_evidence_contract(rotating_evidence_config)
        if rotating_evidence_config is not None else None
    )
    if continuation_from is not None and continuation_from.get(
        "rotatingEvidenceSha256"
    ) is not None:
        if (
            rotating_evidence is None
            or rotating_evidence["rotatingEvidenceSha256"]
            != continuation_from["rotatingEvidenceSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD continuation rotating evidence curriculum drifted"
            )
    if broad_admission:
        if evidence_ladder is not None:
            validate_template_discovery_windows(template, evidence_ladder)
        elif rotating_evidence is not None:
            validate_generation_template(
                template, rotating_evidence, first_generation_index
            )
        else:
            raise TemporalDiscoveryContractError(
                "broad admission requires frozen legacy or rotating evidence"
            )
    ladder_execution: dict[str, Any] | None = None
    if evidence_ladder is not None:
        validation_path = Path(str(evidence_ladder_config.get("validationTemplatePreparationPath") or ""))
        scrutiny_path = Path(str(evidence_ladder_config.get("scrutinyTemplatePreparationPath") or ""))
        validation_template = _read(validation_path, name="QD validation ladder template")
        scrutiny_template = _read(scrutiny_path, name="QD scrutiny ladder template")
        validate_template_stage_window(validation_template, evidence_ladder, stage="validation")
        validate_template_stage_window(scrutiny_template, evidence_ladder, stage="scrutiny")
        ladder_execution = {
            "schemaVersion": "temporal_qd_evidence_ladder_execution_v1",
            "validationTemplate": {"path": str(validation_path.resolve()), "sha256": canonical_sha256(validation_template)},
            "scrutinyTemplate": {"path": str(scrutiny_path.resolve()), "sha256": canonical_sha256(scrutiny_template)},
        }
    construction_policy, _construction_registry = qd_construction_operator_policy(
        construction_catalog_path
    )
    evidence_context = qd_predeclared_evidence_context(
        template,
        worker_contract_sha256=worker_contract_sha256,
        construction_catalog=(
            _construction_registry.catalog.payload
            if _construction_registry is not None
            else None
        ),
        construction_catalog_path=construction_catalog_path,
    )
    if continuation_from is not None and rotating_evidence is not None:
        _require_continuation_evidence_semantics(
            continuation_from,
            _rotating_evidence_semantic_authority(
                execution_engine_commit=execution_engine_commit,
                worker_contract_sha256=worker_contract_sha256,
                construction_operator_policy=(
                    construction_policy if construction_policy["enabled"] else None
                ),
                base_decision_timeframe=str(
                    evidence_context.get("baseDecisionTimeframe") or ""
                ),
                cost_views=QD_COST_VIEWS,
            ),
        )
    pair_source_authority: dict[str, Any] | None = None
    native_v5_proposal_runtime: dict[str, Any] | None = None
    evolvable_capacity_receipt: Mapping[str, Any] | None = None
    if evolvable_authority is not None:
        if bidirectional_pair_config is None:
            raise TemporalDiscoveryContractError(
                "evolvable module authority requires bidirectional pair mode"
            )
        policy_authority = evolvable_authority.get("archivePolicyAuthority")
        if not isinstance(policy_authority, Mapping):
            raise TemporalDiscoveryContractError(
                "evolvable module authority lacks archive policy authority"
            )
        _name, _sha, _policy, direction_aware = _resolve_archive_policy_authority(
            policy_authority
        )
        if not direction_aware:
            raise TemporalDiscoveryContractError(
                "evolvable module authority requires the exact v5 archive policy"
            )
        # The source must already be frozen.  Do not call
        # ``load_pair_run_config`` here: it opens a Python/Dashboard authority
        # and would recreate the retired construction path before the native
        # v5 manifest exists.
        pair_source_authority = _clone(
            bidirectional_pair_config, name="base pair source authority"
        )
        generation_run_config = _clone(
            pair_source_authority, name="v5 generation run config"
        )
        # The historical source implementation is evidence only.  The pure
        # v5 bridge derives its own Rust-executable implementation closure.
        generation_run_config.pop("operatorImplementation", None)
        native_v5_proposal_runtime, bindings = _build_native_v5_proposal_runtime(
            pair_source_authority=pair_source_authority,
            evolvable_module_authority=evolvable_authority,
            generation_run_config=generation_run_config,
            execution_timeout_seconds=pair_generation_timeout_seconds,
        )
        pair_authority = bindings["runConfig"]
        evolvable_capacity_receipt = bindings["capacityReceipt"]
    else:
        pair_authority = (
            load_pair_run_config(bidirectional_pair_config)
            if bidirectional_pair_config is not None
            else None
        )
    if pair_authority is None and pair_generation_engine is not None:
        raise TemporalDiscoveryContractError(
            "pair generation engine requires bidirectional pair mode"
        )
    try:
        pair_generation_runtime = (
            build_pair_generation_runtime_config(
                engine=pair_generation_engine or PAIR_GENERATION_RUNTIME_DEFAULT,
                execution_timeout_seconds=pair_generation_timeout_seconds,
            )
            if pair_authority is not None
            else None
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    if (
        evolvable_authority is not None
        and pair_generation_runtime["engine"] != PAIR_GENERATION_RUNTIME_RUST
    ):
        raise TemporalDiscoveryContractError(
            "fresh evolvable v5 construction requires the Rust-native v5 transaction; "
            "Python is an explicit oracle only"
        )
    archive_policy_authority = (
        evolvable_authority["archivePolicyAuthority"]
        if evolvable_authority is not None
        else None
    )
    policy_name, policy_sha256, frozen_policy, direction_aware = (
        _resolve_archive_policy_authority(archive_policy_authority)
    )
    if initial_archive is not None and (
        initial_archive.get("policyName") != policy_name
        or initial_archive.get("policySha256") != policy_sha256
        or initial_archive.get("frozenPolicy") != frozen_policy
    ):
        raise TemporalDiscoveryContractError(
            "initial archive policy does not match the frozen supervisor authority"
        )
    g0_enabled = bool(pair_authority is not None and not is_continuation and first_generation_index == 1)
    if g0_enabled and initial_construction_pool_size is None and evaluation_population_size is None:
        evaluation_population_size = int(normalized_parameters["targetUniqueCandidates"])
        initial_construction_pool_size = (
            4000 if evaluation_population_size == 1024 else evaluation_population_size
        )
    if g0_enabled and (
        initial_construction_pool_size is None
        or evaluation_population_size is None
        or isinstance(initial_construction_pool_size, bool)
        or isinstance(evaluation_population_size, bool)
        or int(initial_construction_pool_size) < 1
        or int(evaluation_population_size) < 1
        or int(initial_construction_pool_size) < int(evaluation_population_size)
        or int(evaluation_population_size) != int(normalized_parameters["targetUniqueCandidates"])
    ):
        raise TemporalDiscoveryContractError("G0 construction/evaluation sizes are invalid or drift from the frozen normal width")
    if g0_enabled and int(normalized_parameters["maxProposalAttempts"]) < int(initial_construction_pool_size):
        raise TemporalDiscoveryContractError("G0 construction pool exceeds the frozen generation-1 proposal ceiling")
    try:
        g0_finalization_runtime = (
            build_g0_finalization_runtime_config(
                engine=G0_FINALIZATION_RUNTIME_RUST,
                execution_timeout_seconds=pair_generation_timeout_seconds,
            )
            if g0_enabled
            and pair_generation_runtime is not None
            and pair_generation_runtime["engine"] == PAIR_GENERATION_RUNTIME_PYTHON
            else None
        )
    except TemporalQDNativeError as exc:
        raise TemporalDiscoveryContractError(str(exc)) from exc
    generation_plan = {
        "firstGenerationIndex": first_generation_index,
        "generationCount": generation_count,
        "lastGenerationIndex": first_generation_index + generation_count - 1,
        "targetUniqueCandidatesPerGeneration": normalized_parameters[
            "targetUniqueCandidates"
        ],
        "targetUniqueEvaluations": evaluation_target,
        "checkpointCadence": "every_proposal_and_completed_generation",
        "completeGenerationBeforeArchiveReduction": True,
        "workerCompletionOrderAffectsReduction": False,
        **(
            {
                "rotatingEvidenceTaskUpperBounds": _rotating_task_upper_bounds(
                    contract=rotating_evidence,
                    first_generation_index=first_generation_index,
                    generation_count=generation_count,
                    proposal_width=int(normalized_parameters["targetUniqueCandidates"]),
                    initial_parent_count=initial_parent_count,
                )
            }
            if rotating_evidence is not None
            else {}
        ),
    }
    g0_bootstrap = (
        {
            "schemaVersion": "temporal_qd_g0_bootstrap_config_v1",
            "initialConstructionPoolSize": int(initial_construction_pool_size),
            "evaluationPopulationSize": int(evaluation_population_size),
            "activation": "generation_1_pair_random_immigrants_only",
        }
        if g0_enabled
        else None
    )
    pair_capacity_requirement = _immigrant_construction_capacity_requirement(
        {
            "generationPlan": generation_plan,
            **({"g0Bootstrap": g0_bootstrap} if g0_bootstrap is not None else {}),
        }
    )
    pair_capacity_audit = None
    if broad_admission and pair_authority is not None:
        if evolvable_authority is not None:
            if not isinstance(evolvable_capacity_receipt, Mapping):
                raise TemporalDiscoveryContractError(
                    "evolvable v5 broad admission requires a sealed actual-factory capacity receipt"
                )
            _require_evolvable_capacity_receipt_supply(
                evolvable_capacity_receipt,
                required_unique_candidates=pair_capacity_requirement,
            )
            # The authority has already verified this receipt against its
            # actual factory, compiler and capacity contract.  Keep the
            # receipt as the v5 admission witness instead of applying the
            # legacy pair-factory audit to an enriched v5 run config.
            pair_capacity_audit = _clone(
                evolvable_capacity_receipt,
                name="evolvable v5 actual-factory capacity receipt",
            )
        else:
            pair_capacity_audit = immigrant_capacity_audit(
                pair_authority,
                required_unique_candidates=pair_capacity_requirement,
            )
    source = None if pair_authority is not None else ExactGeneratorV2Continuation(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        confirmed_entry_admission_root=confirmed_entry_admission_root,
        start_continuation_ordinal=initial_immigrant_continuation_ordinal,
    )
    if pair_authority is None and validator_command_file is None:
        raise TemporalDiscoveryContractError("legacy QD supervisor requires a validator command file")
    validator_command = _command(validator_command_file) if validator_command_file is not None else []
    config = {
        "schemaVersion": SUPERVISOR_CONFIG_SCHEMA,
        "supervisorVersion": SUPERVISOR_VERSION,
        "qdVersion": QD_VERSION,
        "policyName": policy_name,
        "policySha256": policy_sha256,
        "frozenPolicy": _clone(frozen_policy, name="frozen QD policy"),
        "broadAdmission": bool(broad_admission),
        **(
            {
                "broadAdmissionContract": {
                    "schemaVersion": "temporal_qd_broad_admission_contract_v1",
                    **(
                        _rotating_task_upper_bounds(
                            contract=rotating_evidence,
                            first_generation_index=first_generation_index,
                            generation_count=generation_count,
                            proposal_width=int(
                                normalized_parameters["targetUniqueCandidates"]
                            ),
                            initial_parent_count=initial_parent_count,
                        )
                        if rotating_evidence is not None
                        else _broad_admission_contract_values(
                            expected_broad_generation_count
                        )
                    ),
                    **(
                        {
                            "immigrantConstructionCandidateRequirement": pair_capacity_requirement,
                            **(
                                {"evolvableFactoryCapacityReceipt": pair_capacity_audit}
                                if evolvable_authority is not None
                                else {"immigrantConstructionCapacity": pair_capacity_audit}
                            ),
                        }
                        if pair_capacity_audit is not None
                        else {}
                    ),
                }
            }
            if broad_admission
            else {}
        ),
        "emptyQualityBootstrapPolicy": {
            "enabledByBroadAdmission": bool(broad_admission),
            "activation": "only_when_generation_starts_without_quality_parent_cells",
            "originSchedule": (
                "rich_bidirectional_random_immigrants_only_v1"
                if pair_authority is not None
                else "generator_v2_random_immigrants_only"
            ),
        },
        "repositories": {
            "autoresearchCommit": _git_sha(
                autoresearch_commit, name="AutoResearch commit"
            ),
            "executionEngineCommit": _git_sha(
                execution_engine_commit, name="execution engine commit"
            ),
        },
        "workerContractSha256": _sha256(worker_contract_sha256, name="worker contract"),
        "identityLedger": {
            "schemaVersion": QD_IDENTITY_LEDGER_SCHEMA,
            "policySha256": policy_sha256,
            "canonicalEvidenceIdentity": frozen_policy["identity"]["canonicalEvidence"],
        },
        **(
            {"constructionOperatorPolicy": construction_policy}
            if construction_policy["enabled"]
            else {}
        ),
        "initialArchive": {
            "path": str(initial_archive_path.resolve()),
            "archiveSha256": initial_archive_sha,
            **(
                {
                    "generationIndex": int(initial_archive["generationIndex"]),
                    "resultSetSha256": initial_archive["resultSetSha256"],
                }
                if initial_archive is not None
                else {}
            ),
            **(
                {"transportDescriptor": initial_archive_transport}
                if initial_archive_transport is not None
                else {}
            ),
            **(
                {
                    "parentSchedule": _clone(
                        initial_parent_schedule,
                        name="initial archive parent schedule",
                    )
                }
                if initial_parent_schedule is not None
                else {}
            ),
        },
        **({"continuationFrom": _clone(continuation_from, name="QD continuation binding")} if continuation_from is not None else {}),
        **({
            "immigrantSource": {
                "sourcePreparationPath": str(source_preparation_path.resolve()), "baseGeneratorRoot": str(base_generator_root.resolve()),
                "confirmedEntryAdmissionRoot": str(confirmed_entry_admission_root.resolve()), "sourceIdentity": source.source_identity,
                "initialContinuationOrdinal": initial_immigrant_continuation_ordinal,
            }
        } if source is not None else {
            "bidirectionalPairGeneration": pair_authority,
            **(
                {"bidirectionalPairSourceAuthority": pair_source_authority}
                if evolvable_authority is not None
                else {}
            ),
            "pairGenerationRuntime": pair_generation_runtime,
            **(
                {"evolvableModuleAuthority": evolvable_authority}
                if evolvable_authority is not None
                else {}
            ),
            **(
                {"nativeV5ProposalRuntime": native_v5_proposal_runtime}
                if native_v5_proposal_runtime is not None
                else {}
            ),
        }),
        **({"validator": {"commandFile": str(validator_command_file.resolve()), "command": validator_command, "commandSha256": canonical_sha256(validator_command), "timeoutSeconds": 60.0}} if pair_authority is None else {}),
        "evaluation": {
            "templatePreparationPath": str(template_preparation_path.resolve()),
            "templatePreparationSha256": canonical_sha256(template),
            "predeclaredEvidenceContext": evidence_context,
            "predeclaredEvidenceContextSha256": evidence_context[
                "predeclaredEvidenceContextSha256"
            ],
            "gatewayUrl": str(gateway_url).rstrip("/"),
            "timeoutSecondsPerGeneration": float(evaluation_timeout_seconds),
            "enqueueBatchSize": int(enqueue_batch_size),
            "costViews": _clone(QD_COST_VIEWS, name="frozen QD cost views"),
            **(
                {
                    "behaviorAttributionRequirement": _clone(
                        evolvable_authority["behaviorAttributionRequirement"],
                        name="v5 behavior attribution requirement",
                    )
                }
                if evolvable_authority is not None
                else {}
            ),
        },
        **({"evidenceLadder": evidence_ladder} if evidence_ladder is not None else {}),
        **({"rotatingEvidence": rotating_evidence} if rotating_evidence is not None else {}),
        **({"evidenceLadderExecution": ladder_execution} if ladder_execution is not None else {}),
        "generationPlan": generation_plan,
        **({"g0Bootstrap": g0_bootstrap} if g0_bootstrap is not None else {}),
        **(
            {"g0FinalizationRuntime": g0_finalization_runtime}
            if g0_finalization_runtime is not None
            else {}
        ),
        "frozenSearchPolicy": normalized_parameters,
        "operationalTripwires": [
            "determinism_drift",
            "evaluation_identity_mismatch",
            "checkpoint_corruption",
            "data_or_version_drift",
            "systemic_evaluator_failure",
        ],
        "nonTripwires": [
            "poor_early_returns",
            "low_early_archive_occupancy",
            "weak_early_operator_family",
            "early_immigrant_dominance",
        ],
        **(
            {
                "generationFunnel": {
                    "enabled": True,
                    "schemaVersion": "temporal_qd_generation_funnel_integration_v1",
                    "publication": "after_evaluation_activation_and_archive_before_generation_record",
                    "selectionInput": False,
                }
            }
            if generation_funnel_enabled
            else {}
        ),
    }
    config["configSha256"] = canonical_sha256(config)
    return config, validator_command


def _campaign_window_evidence(
    *,
    campaign_root: Path,
    panel: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
    tail_result_index: Mapping[str, Any] | None = None,
    direction_aware: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    result_root = campaign_root / "screening-run"
    return load_provenance_bound_window_evidence(
        result_root=result_root,
        task_manifest=_canonical_file(
            result_root / "task-manifest.json", name="rotating task manifest"
        ),
        checkpoint=_canonical_file(
            result_root / "checkpoint.json", name="rotating task checkpoint"
        ),
        panel=panel,
        candidates=candidates,
        tail_result_index=tail_result_index,
        direction_aware=direction_aware,
    )


def _run_rotating_cohort_campaign(
    *,
    root: Path,
    supervisor_root: Path,
    candidates: list[dict[str, Any]],
    generation_index: int,
    panel_id: str,
    campaign_role: str,
    template_path: Path,
    config: Mapping[str, Any],
    client: LabGatewayClient,
) -> tuple[dict[str, Any], Path, Path]:
    # A parent/backfill campaign can begin hours after the proposal campaign.
    # Reopen all path-backed authorities immediately before freezing another
    # task matrix so one generation cannot mix catalog/template semantics.
    _validate_frozen_sources(config, run_root=supervisor_root)
    cohort = build_rotating_cohort_population(
        candidates=candidates,
        generation_index=generation_index,
        panel_id=panel_id,
        cohort_role=campaign_role,
        rotating_evidence_sha256=config["rotatingEvidence"][
            "rotatingEvidenceSha256"
        ],
    )
    population_path = root / "population.json"
    _write_once(population_path, cohort)
    campaign_root = root / "campaign"
    result = freeze_qd_screening_campaign(
        population_path=population_path,
        template_preparation_path=template_path,
        output_root=campaign_root,
        execution_engine_commit=config["repositories"]["executionEngineCommit"],
        worker_contract_sha256=config["workerContractSha256"],
        construction_catalog_path=(
            (config.get("constructionOperatorPolicy") or {})
            .get("catalog", {})
            .get("path")
        ),
        rotating_evidence=config["rotatingEvidence"],
        campaign_role=campaign_role,
        panel_id=panel_id,
        archive_policy_authority=(
            config["evolvableModuleAuthority"]["archivePolicyAuthority"]
            if config.get("evolvableModuleAuthority") is not None
            else None
        ),
        behavior_attribution_requirement=(
            config["evaluation"].get("behaviorAttributionRequirement")
            if config.get("evolvableModuleAuthority") is not None
            else None
        ),
    )
    authority = _canonical_file(campaign_root / "authority.json", name="rotating authority")
    evaluation = run_temporal_search_tasks(
        client,
        authority,
        output_root=campaign_root / "screening-run",
        timeout_seconds=float(config["evaluation"]["timeoutSecondsPerGeneration"]),
        resume=True,
        enqueue_batch_size=int(config["evaluation"]["enqueueBatchSize"]),
        **(
            {
                "behavior_attribution_requirement": config["evaluation"][
                    "behaviorAttributionRequirement"
                ]
            }
            if config.get("evolvableModuleAuthority") is not None
            else {}
        ),
    )
    if evaluation.get("completedTaskCount") != result["taskCount"]:
        raise TemporalDiscoveryContractError(
            "rotating cohort campaign did not complete its exact task matrix"
        )
    return result, population_path, campaign_root


def _load_previous_cumulative_archive(parent_archive_path: Path) -> dict[str, Any] | None:
    parent, _ = _load_archive(parent_archive_path)
    binding = parent.get("rotatingEvidenceTransaction")
    candidate = parent_archive_path.parent / "evidence" / "cumulative-archive.json"
    if not candidate.is_file():
        if binding is not None:
            raise TemporalDiscoveryContractError(
                "rotating parent archive lost its cumulative evidence source"
            )
        return None
    payload = _canonical_file(candidate, name="previous cumulative breeder archive")
    cumulative_sha = _identity_payload(
        payload, "archiveSha256", name="previous cumulative breeder archive"
    )
    if (
        not isinstance(binding, Mapping)
        or binding.get("cumulativeArchiveSha256") != cumulative_sha
    ):
        raise TemporalDiscoveryContractError(
            "rotating parent archive cumulative evidence binding drifted"
        )
    return payload


def _invoke_native_tail_reducer(
    *, binary: Path, manifest_path: Path, timeout_seconds: float = 600.0
) -> dict[str, Any]:
    """Run the bounded native tail reducer and require its canonical result."""

    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="tailReducer"
    )
    try:
        result = subprocess.run(
            [str(binary.resolve()), "--manifest", str(manifest_path.resolve())],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TemporalDiscoveryContractError(
            f"native tail reducer could not execute: {exc}"
        ) from exc
    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="tailReducer"
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise TemporalDiscoveryContractError(
            f"native tail reducer failed ({result.returncode}): {detail}"
        )
    try:
        payload = json.loads(result.stdout)
    except ValueError as exc:
        raise TemporalDiscoveryContractError(
            "native tail reducer returned invalid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise TemporalDiscoveryContractError(
            "native tail reducer response is not an object"
        )
    return dict(payload)


def _invoke_native_campaign_seal(
    *, binary: Path, manifest_path: Path, timeout_seconds: float = 1_200.0
) -> dict[str, Any]:
    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="campaignSeal"
    )
    try:
        result = subprocess.run(
            [str(binary.resolve()), "--manifest", str(manifest_path.resolve())],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TemporalDiscoveryContractError(
            f"native campaign seal could not execute: {exc}"
        ) from exc
    _verify_pinned_native_invocation_binary(
        binary=binary, manifest_path=manifest_path, role="campaignSeal"
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2_000:]
        raise TemporalDiscoveryContractError(
            f"native campaign seal failed closed: {detail}"
        )
    try:
        payload = json.loads(result.stdout)
    except ValueError as exc:
        raise TemporalDiscoveryContractError(
            "native campaign seal returned invalid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise TemporalDiscoveryContractError(
            "native campaign seal response is not an object"
        )
    return dict(payload)


def _run_rotating_generation_transaction(
    *,
    root: Path,
    generation_root: Path,
    generation_index: int,
    proposal_root: Path,
    proposal_campaign_root: Path,
    parent_archive_path: Path,
    archive_path: Path,
    config: Mapping[str, Any],
    client: LabGatewayClient,
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    tail_result_indexes: dict[Path, dict[str, Any]] | None = None,
    finalization_engine: str = GENERATION_FINALIZATION_ENGINE_PYTHON,
    native_finalizer_binary: Path | None = None,
    generation_record_extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Complete one atomic proposal/reevaluation/backfill/archive transaction."""

    tail_result_mode = _normalize_tail_result_mode(tail_result_mode)
    finalization_engine = _normalize_generation_finalization_engine(
        finalization_engine
    )
    indexes = tail_result_indexes if tail_result_indexes is not None else {}
    native_campaign_seal_binary = (
        native_finalizer_binary.with_name(
            "temporal-qd-campaign-seal.exe"
            if os.name == "nt"
            else "temporal-qd-campaign-seal"
        )
        if native_finalizer_binary is not None
        else None
    )
    _validate_frozen_sources(config, run_root=root)
    contract = validate_rotating_evidence_contract(config["rotatingEvidence"])
    panel = panel_for_generation(contract, generation_index)
    evidence_root = generation_root / "evidence"
    cohort_path = evidence_root / "cohort.json"
    checkpoint_path = evidence_root / "checkpoint.json"
    provisional_path = evidence_root / "provisional.json"
    cumulative_path = evidence_root / "cumulative-archive.json"
    ledger_path = evidence_root / "generation-ledger.json"

    completed_paths = (
        cohort_path,
        provisional_path,
        cumulative_path,
        checkpoint_path,
        ledger_path,
        archive_path,
    )
    native_commit_path = (
        _native_finalization_root(root, generation_index)
        / "generation-commit.json"
    )
    if (
        finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST
        and native_commit_path.is_file()
    ):
        if native_finalizer_binary is None:
            raise TemporalDiscoveryContractError(
                "Rust finalization recovery binary is unavailable"
            )
        recovery_manifest_path = (
            _native_finalization_root(root, generation_index) / "manifest.json"
        )
        recovery_execution = _invoke_native_finalizer(
            binary=native_finalizer_binary,
            manifest_path=recovery_manifest_path,
        )
        if (
            recovery_execution.get("restart") is not True
            or recovery_execution.get("restartValidation")
            != "compact_commit_and_output_hashes"
        ):
            raise TemporalDiscoveryContractError(
                "native generation recovery did not reopen compact commit authority"
            )
        published = _publish_native_generation_outputs(
            root=root,
            generation_index=generation_index,
            load_published_payloads=False,
        )
        return _native_rotating_archive_result(
            root=root,
            generation_index=generation_index,
            published=published,
        )
    if all(path.is_file() for path in completed_paths):
        cohort = _canonical_file(cohort_path, name="rotating evaluation cohort")
        cohort_sha = _identity_payload(
            cohort, "cohortSha256", name="rotating evaluation cohort"
        )
        provisional = _canonical_file(
            provisional_path, name="rotating provisional survivors"
        )
        provisional_sha = _identity_payload(
            provisional,
            "provisionalSha256",
            name="rotating provisional survivors",
        )
        cumulative = _canonical_file(
            cumulative_path, name="cumulative breeder archive"
        )
        cumulative_sha = _identity_payload(
            cumulative, "archiveSha256", name="cumulative breeder archive"
        )
        checkpoint = _canonical_file(
            checkpoint_path, name="rotating generation checkpoint"
        )
        checkpoint_sha = _identity_payload(
            checkpoint,
            "checkpointSha256",
            name="rotating generation checkpoint",
        )
        ledger = _canonical_file(
            ledger_path, name="rotating generation ledger"
        )
        ledger_sha = _identity_payload(
            ledger, "ledgerSha256", name="rotating generation ledger"
        )
        archive, archive_sha = _load_archive(archive_path)
        _previous, previous_sha = _load_archive(parent_archive_path)
        campaigns = ledger.get("campaigns")
        stage_artifacts = checkpoint.get("stageArtifacts")
        transaction = archive.get("rotatingEvidenceTransaction")
        if (
            cohort.get("generationIndex") != generation_index
            or cohort.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or provisional.get("generationIndex") != generation_index
            or provisional.get("cohortSha256") != cohort_sha
            or cumulative.get("generationIndex") != generation_index
            or cumulative.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or checkpoint.get("generationIndex") != generation_index
            or checkpoint.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or checkpoint.get("stage") != "cumulative_archive"
            or checkpoint.get("cohortSha256") != cohort_sha
            or checkpoint.get("cumulativeArchiveSha256") != cumulative_sha
            or not isinstance(stage_artifacts, Mapping)
            or not isinstance(campaigns, list)
            or stage_artifacts.get("campaignsSha256")
            != canonical_sha256(campaigns)
            or stage_artifacts.get("parentArchiveSha256") != archive_sha
            or ledger.get("generationIndex") != generation_index
            or ledger.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or ledger.get("cohortSha256") != cohort_sha
            or ledger.get("provisionalSha256") != provisional_sha
            or ledger.get("cumulativeArchiveSha256") != cumulative_sha
            or ledger.get("parentArchiveSha256") != archive_sha
            or ledger.get("checkpointSha256") != checkpoint_sha
            or ledger.get("proposalOnlyFunnelReporting") is not True
            or archive.get("generationIndex") != generation_index
            or archive.get("previousArchiveSha256") != previous_sha
            or not isinstance(transaction, Mapping)
            or transaction.get("cumulativeArchiveSha256") != cumulative_sha
            or transaction.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or transaction.get("requiredPanelIds")
            != checkpoint.get("requiredPanelIds")
        ):
            raise TemporalDiscoveryContractError(
                "completed rotating generation transaction drifted"
            )
        additional_worker_task_count = 0
        for binding in campaigns:
            if not isinstance(binding, Mapping):
                raise TemporalDiscoveryContractError(
                    "completed rotating campaign binding is invalid"
                )
            campaign_root = Path(str(binding.get("campaignRoot") or ""))
            if binding.get("role") == "proposal_current_panel":
                if campaign_root.resolve() != proposal_campaign_root.resolve():
                    raise TemporalDiscoveryContractError(
                        "completed rotating proposal campaign root drifted"
                    )
                continue
            try:
                campaign_root.resolve().relative_to(evidence_root.resolve())
            except ValueError as exc:
                raise TemporalDiscoveryContractError(
                    "completed rotating auxiliary campaign escaped its generation"
                ) from exc
            campaign = _canonical_file(
                campaign_root / "campaign.json",
                name="completed rotating auxiliary campaign",
            )
            if campaign.get("campaignSha256") != binding.get("campaignSha256"):
                raise TemporalDiscoveryContractError(
                    "completed rotating auxiliary campaign identity drifted"
                )
            additional_worker_task_count += int(campaign.get("taskCount") or 0)
        parent_schedule = transaction.get("parentSchedule")
        if not isinstance(parent_schedule, Mapping):
            raise TemporalDiscoveryContractError(
                "completed rotating parent schedule is missing"
            )
        frontier_member_count = sum(
            member.get("archiveLane") == "rotating_frontier"
            for cell in archive.get("cells") or []
            if isinstance(cell, Mapping)
            for member in cell.get("members") or []
            if isinstance(member, Mapping)
        )
        native_restart: dict[str, Any] = {}
        if finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
            if native_finalizer_binary is None:
                raise TemporalDiscoveryContractError(
                    "Rust finalization restart binary is unavailable"
                )
            manifest_path = _native_finalization_root(
                root, generation_index
            ) / "manifest.json"
            execution = _invoke_native_finalizer(
                binary=native_finalizer_binary, manifest_path=manifest_path
            )
            if (
                execution.get("restart") is not True
                or execution.get("restartValidation")
                != "compact_commit_and_output_hashes"
            ):
                raise TemporalDiscoveryContractError(
                    "native generation restart did not use compact commit authority"
                )
            published = _publish_native_generation_outputs(
                root=root, generation_index=generation_index
            )
            manifest = _canonical_file(
                manifest_path, name="native generation finalization manifest"
            )
            native_restart = {
                "nativeManifest": manifest,
                "nativeCommit": published["generation-commit.json"],
                "nativeGenerationRecord": published["generation-record.json"],
                "nativeStatePatch": published["generation-state-patch.json"],
            }
        return {
            "schemaVersion": "temporal_qd_rotating_parent_archive_result_v1",
            "archiveSha256": archive_sha,
            "parentSchedule": _clone(
                parent_schedule, name="completed rotating parent schedule"
            ),
            "cumulativeArchiveSha256": cumulative_sha,
            "occupiedCellCount": int(archive["occupiedCellCount"]),
            "memberCount": int(archive["memberCount"]),
            "qualityMemberCount": int(archive["qualityMemberCount"]),
            "frontierMemberCount": int(frontier_member_count),
            "newCellCount": int(archive["newCellCount"]),
            "paretoAdmissionCount": int(archive["paretoAdmissionCount"]),
            "paretoEvictionCount": int(archive["paretoEvictionCount"]),
            "observationalMemberCount": int(
                archive["observationalMemberCount"]
            ),
            "negativeNoveltyMemberCount": int(
                archive["negativeNoveltyMemberCount"]
            ),
            "rotatingEvidenceLedgerSha256": ledger_sha,
            "rotatingEvidenceCheckpointSha256": checkpoint_sha,
            "additionalWorkerTaskCount": additional_worker_task_count,
            **native_restart,
        }

    projection = load_evaluation_population(
        population_path=proposal_root / "population.json",
        journal_path=proposal_root / "generation-journal.json",
    )
    new_candidates = {
        str(row["candidateId"]): row for row in projection["candidates"]
    }
    previous, _previous_sha = _load_archive(parent_archive_path)
    parent_candidates = {
        str(member["candidateId"]): _clone(
            member["candidate"], name="retained rotating parent"
        )
        for cell in previous.get("cells") or []
        for member in cell.get("members") or []
        if isinstance(member, Mapping) and isinstance(member.get("candidate"), Mapping)
    }
    cohort = build_current_panel_evaluation_cohort(
        new_candidates=list(new_candidates.values()),
        retained_parents=list(parent_candidates.values()),
        contract=contract,
        generation_index=generation_index,
    )
    _write_once(cohort_path, cohort)
    if checkpoint_path.is_file():
        existing_checkpoint = _canonical_file(
            checkpoint_path, name="rotating generation checkpoint"
        )
        _identity_payload(
            existing_checkpoint,
            "checkpointSha256",
            name="rotating generation checkpoint",
        )
        if (
            existing_checkpoint.get("rotatingEvidenceSha256")
            != contract["rotatingEvidenceSha256"]
            or existing_checkpoint.get("generationIndex") != generation_index
            or existing_checkpoint.get("cohortSha256") != cohort["cohortSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "rotating generation checkpoint drifted before restart"
            )
    checkpoint = build_generation_evidence_checkpoint(
        contract=contract,
        generation_index=generation_index,
        stage="current_panel_evaluation",
        cohort=cohort,
        stage_artifacts={
            "proposalCampaignSha256": _canonical_file(
                proposal_campaign_root / "campaign.json", name="proposal campaign"
            )["campaignSha256"]
        },
    )
    _replace(checkpoint_path, checkpoint)

    sealed_member_batch: dict[str, Any] | None = None
    if finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
        if native_finalizer_binary is None:
            raise TemporalDiscoveryContractError(
                "Rust generation finalization binary is unavailable"
            )
        seal_manifest, seal_manifest_path = _native_campaign_seal_manifest(
            root=root,
            config=config,
            generation_index=generation_index,
            evaluation_population=projection,
        )
        assert native_campaign_seal_binary is not None
        seal_execution = _invoke_native_campaign_seal(
            binary=native_campaign_seal_binary, manifest_path=seal_manifest_path
        )
        seal_root = seal_manifest_path.parent
        directional_tail_authority = seal_manifest.get("directionalTailAuthority")
        if config.get("evolvableModuleAuthority") is not None:
            if not isinstance(directional_tail_authority, Mapping):
                raise TemporalDiscoveryContractError(
                    "native v5 campaign seal lacks directional tail authority"
                )
            proposal_tail_index = validate_v5_directional_tail_index(
                _canonical_file(
                    seal_root / "tail-result-index-v4.json",
                    name="native sealed v5 directional tail index",
                ),
                authority=directional_tail_authority,
            )
        else:
            proposal_tail_index = validate_tail_result_index(
                _canonical_file(
                    seal_root / "tail-result-index-v3.json",
                    name="native sealed proposal tail index",
                )
            )
        if (
            seal_execution.get("transaction", {}).get("tailResultIndexSha256")
            != proposal_tail_index.get("tailResultIndexSha256")
            or seal_manifest.get("sourceSha256")
            != seal_execution.get("transaction", {}).get("sourceSha256")
        ):
            raise TemporalDiscoveryContractError(
                "native campaign seal transaction binding drifted"
            )
        sealed_members: list[dict[str, Any]] = []
        try:
            with (seal_root / "evaluated-members.jsonl").open(
                "r", encoding="utf-8", newline=""
            ) as handle:
                for line in handle:
                    member = json.loads(line)
                    if not isinstance(member, dict):
                        raise ValueError("member row is not an object")
                    sealed_members.append(member)
        except (OSError, ValueError) as exc:
            raise TemporalDiscoveryContractError(
                "native sealed evaluated members are unavailable"
            ) from exc
        sealed_member_batch = {"members": sealed_members}
        # A v4 native v5 tail is never installed into the legacy retained
        # index map: that map's verifier is intentionally v3-only and may
        # reopen raw results.  Downstream v5 routing consumes its compact
        # receipt directly instead.
        if config.get("evolvableModuleAuthority") is None:
            indexes[(proposal_campaign_root / "screening-run").resolve()] = (
                proposal_tail_index
            )
    else:
        proposal_tail_index = (
            _verified_tail_result_index(
                campaign_root=proposal_campaign_root,
                indexes=indexes,
                include_funnel_projection=bool(
                    (config.get("generationFunnel") or {}).get("enabled")
                ),
            )
            if tail_result_mode == TAIL_RESULT_MODE_INDEXED
            else None
        )

    member_batches = [
        sealed_member_batch
        if sealed_member_batch is not None
        else load_qd_evaluated_members(
            population_path=proposal_root / "population.json",
            result_root=proposal_campaign_root / "screening-run",
            generation_index=generation_index,
            generation_journal_path=proposal_root / "generation-journal.json",
            minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]),
            minimum_trades_per_window=int(
                config["frozenSearchPolicy"]["minimumTradesPerWindow"]
            ),
            cap_trades=int(config["frozenSearchPolicy"]["capTrades"]),
            tail_result_index=proposal_tail_index,
            direction_aware=(config.get("evolvableModuleAuthority") is not None),
        )
    ]
    campaign_bindings: list[dict[str, Any]] = [
        {
            "role": "proposal_current_panel",
            "panelId": panel["panelId"],
            "campaignRoot": str(proposal_campaign_root.resolve()),
            "campaignSha256": _canonical_file(
                proposal_campaign_root / "campaign.json", name="proposal campaign"
            )["campaignSha256"],
        }
    ]
    parent_campaign_root: Path | None = None
    if parent_candidates:
        template = contract["panelTemplates"][panel["panelId"]]
        result, population_path, parent_campaign_root = _run_rotating_cohort_campaign(
            root=evidence_root / "current-parents",
            supervisor_root=root,
            candidates=list(parent_candidates.values()),
            generation_index=generation_index,
            panel_id=str(panel["panelId"]),
            campaign_role="retained_parent_current_panel",
            template_path=Path(template["path"]),
            config=config,
            client=client,
        )
        member_batches.append(
            load_qd_evaluated_members(
                population_path=population_path,
                result_root=parent_campaign_root / "screening-run",
                generation_index=generation_index,
                minimum_total_trades=int(
                    config["frozenSearchPolicy"]["minimumTotalTrades"]
                ),
                minimum_trades_per_window=int(
                    config["frozenSearchPolicy"]["minimumTradesPerWindow"]
                ),
                cap_trades=int(config["frozenSearchPolicy"]["capTrades"]),
                direction_aware=(config.get("evolvableModuleAuthority") is not None),
                tail_result_index=(
                    _verified_tail_result_index(
                        campaign_root=parent_campaign_root,
                        indexes=indexes,
                    )
                    if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                    else None
                ),
            )
        )
        campaign_bindings.append(
            {
                "role": "retained_parent_current_panel",
                "panelId": panel["panelId"],
                "campaignRoot": str(parent_campaign_root.resolve()),
                "populationPath": str(population_path.resolve()),
                "campaignSha256": result["campaignSha256"],
            }
        )

    current_members: dict[str, dict[str, Any]] = {}
    for batch in member_batches:
        for member in batch["members"]:
            candidate_id = str(member["candidateId"])
            if candidate_id in current_members:
                raise TemporalDiscoveryContractError(
                    "current-panel proposal/parent union contains duplicate candidate"
                )
            current_members[candidate_id] = member
    cell_counts: dict[str, int] = {}
    for member in current_members.values():
        cell_id = str(member["descriptor"]["cellId"])
        cell_counts[cell_id] = cell_counts.get(cell_id, 0) + 1
    provisional_input = [
        {
            "candidateId": candidate_id,
            "candidateIdentitySha256": member["candidate"].get(
                "candidateIdentitySha256"
            ),
            "programSha256": member["candidate"].get("programSha256"),
            "profileSnapshotSha256": member["candidate"].get(
                "profileSnapshotSha256"
            ),
            "cellId": member["descriptor"]["cellId"],
            "costView": "research_conservative",
            "currentPanelRank": float(
                member["aggregate"].get("totalConservativeNetR") or 0.0
            ),
            "novelty": 1.0 / float(cell_counts[str(member["descriptor"]["cellId"])]),
        }
        for candidate_id, member in sorted(current_members.items())
    ]
    provisional = reduce_provisional_diverse_survivors(
        provisional_input,
        limit=int(contract["provisionalReduction"]["maxCandidates"]),
    )
    provisional_artifact = {
        "schemaVersion": "temporal_qd_provisional_survivors_v1",
        "generationIndex": generation_index,
        "panelId": panel["panelId"],
        "cohortSha256": cohort["cohortSha256"],
        "candidateCount": len(provisional),
        "candidates": provisional,
    }
    provisional_artifact["provisionalSha256"] = canonical_sha256(
        provisional_artifact
    )
    _write_once(provisional_path, provisional_artifact)
    checkpoint = build_generation_evidence_checkpoint(
        contract=contract,
        generation_index=generation_index,
        stage="provisional_reduction",
        cohort=cohort,
        provisional_candidate_ids=[row["candidateId"] for row in provisional],
        stage_artifacts={
            "provisionalSha256": provisional_artifact["provisionalSha256"]
        },
    )
    _replace(checkpoint_path, checkpoint)

    rich_candidates: dict[str, dict[str, Any]] = {}
    for row in provisional:
        candidate_id = str(row["candidateId"])
        candidate = current_members[candidate_id]["candidate"]
        if candidate_id in new_candidates:
            candidate = hydrate_evaluation_candidate(
                candidate,
                proposal_root=proposal_root / "proposal-journal",
            )
            current_members[candidate_id]["candidate"] = candidate
        rich_candidates[candidate_id] = _clone(candidate, name="provisional rich candidate")

    current_records: dict[str, list[dict[str, Any]]] = {}
    # Deterministic insufficient-warmup outcomes are deliberately absent from
    # ``current_members``.  Do not demand rotating evidence for a candidate
    # which has no replay evidence and is ineligible for provisional/breeder
    # selection.
    evaluated_new_candidates = {
        candidate_id: candidate
        for candidate_id, candidate in new_candidates.items()
        if candidate_id in current_members
    }
    if config.get("evolvableModuleAuthority") is not None:
        directional_tail_authority = seal_manifest.get("directionalTailAuthority")
        if not isinstance(directional_tail_authority, Mapping):
            raise TemporalDiscoveryContractError(
                "native v5 rotating evidence lacks directional tail authority"
            )
        new_records = v5_directional_compact_window_evidence(
            index=proposal_tail_index,
            authority=directional_tail_authority,
            panel=panel,
            candidates=evaluated_new_candidates,
        )
    else:
        new_records = _campaign_window_evidence(
            campaign_root=proposal_campaign_root,
            panel=panel,
            candidates=evaluated_new_candidates,
            tail_result_index=proposal_tail_index,
            direction_aware=False,
        )
    current_records.update(new_records)
    if parent_campaign_root is not None:
        current_records.update(
            _campaign_window_evidence(
                campaign_root=parent_campaign_root,
                panel=panel,
                candidates=parent_candidates,
                tail_result_index=(
                    _verified_tail_result_index(
                        campaign_root=parent_campaign_root,
                        indexes=indexes,
                    )
                    if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                    else None
                ),
                direction_aware=config.get("evolvableModuleAuthority") is not None,
            )
        )
    bundles: dict[str, dict[str, dict[str, Any]]] = {
        candidate_id: {} for candidate_id in rich_candidates
    }
    previous_cumulative = _load_previous_cumulative_archive(parent_archive_path)
    if previous_cumulative is not None:
        for bundle in previous_cumulative.get("candidatePanelBundles") or []:
            if not isinstance(bundle, Mapping):
                continue
            candidate_id = str(bundle.get("candidateId"))
            if candidate_id in bundles:
                bundles[candidate_id][str(bundle.get("panelId"))] = _clone(
                    bundle, name="previous candidate panel bundle"
                )
    for candidate_id in rich_candidates:
        bundles[candidate_id][str(panel["panelId"])] = build_candidate_panel_bundle(
            contract=contract,
            candidate=rich_candidates[candidate_id],
            panel_id=str(panel["panelId"]),
            records=current_records[candidate_id],
        )

    required = required_panel_ids(contract, generation_index)
    for backfill_panel_id in required:
        missing = [
            candidate_id
            for candidate_id in sorted(rich_candidates)
            if backfill_panel_id not in bundles[candidate_id]
        ]
        if not missing:
            continue
        template = contract["panelTemplates"][backfill_panel_id]
        result, population_path, backfill_campaign_root = _run_rotating_cohort_campaign(
            root=evidence_root / "backfill" / backfill_panel_id,
            supervisor_root=root,
            candidates=[rich_candidates[candidate_id] for candidate_id in missing],
            generation_index=generation_index,
            panel_id=backfill_panel_id,
            campaign_role="prior_panel_backfill",
            template_path=Path(template["path"]),
            config=config,
            client=client,
        )
        backfill_panel = next(
            row for row in contract["panels"] if row["panelId"] == backfill_panel_id
        )
        records = _campaign_window_evidence(
            campaign_root=backfill_campaign_root,
            panel=backfill_panel,
            candidates={candidate_id: rich_candidates[candidate_id] for candidate_id in missing},
            tail_result_index=(
                _verified_tail_result_index(
                    campaign_root=backfill_campaign_root, indexes=indexes
                )
                if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                else None
            ),
            direction_aware=config.get("evolvableModuleAuthority") is not None,
        )
        for candidate_id in missing:
            bundles[candidate_id][backfill_panel_id] = build_candidate_panel_bundle(
                contract=contract,
                candidate=rich_candidates[candidate_id],
                panel_id=backfill_panel_id,
                records=records[candidate_id],
            )
        campaign_bindings.append(
            {
                "role": "prior_panel_backfill",
                "panelId": backfill_panel_id,
                "campaignRoot": str(backfill_campaign_root.resolve()),
                "populationPath": str(population_path.resolve()),
                "campaignSha256": result["campaignSha256"],
                "candidateIds": missing,
            }
        )
    checkpoint = build_generation_evidence_checkpoint(
        contract=contract,
        generation_index=generation_index,
        stage="cumulative_backfill",
        cohort=cohort,
        provisional_candidate_ids=list(rich_candidates),
        stage_artifacts={
            "campaignsSha256": canonical_sha256(campaign_bindings),
            "requiredPanelIds": required,
        },
    )
    _replace(checkpoint_path, checkpoint)
    for binding in campaign_bindings:
        if binding["role"] == "proposal_current_panel":
            continue
        binding["artifacts"] = _rotating_campaign_artifacts(
            campaign_root=Path(binding["campaignRoot"]),
            population_path=Path(binding["populationPath"]),
            tail_result_index=(
                _verified_tail_result_index(
                    campaign_root=Path(binding["campaignRoot"]),
                    indexes=indexes,
                )
                if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                else None
            ),
        )

    if finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
        if native_finalizer_binary is None:
            raise TemporalDiscoveryContractError(
                "Rust generation finalization binary is unavailable"
            )
        if proposal_tail_index is None:
            proposal_tail_index = _verified_tail_result_index(
                campaign_root=proposal_campaign_root,
                indexes=indexes,
                include_funnel_projection=True,
            )
        required_set = set(required)
        if any(set(panel_bundles) != required_set for panel_bundles in bundles.values()):
            raise TemporalDiscoveryContractError(
                "native finalization requires exact complete panel coverage"
            )
        complete_bundles = [
            panel_bundle
            for _candidate_id, panel_bundles in sorted(bundles.items())
            for _panel_id, panel_bundle in sorted(panel_bundles.items())
        ]
        total_task_count = sum(
            int(
                _canonical_file(
                    Path(binding["campaignRoot"]) / "campaign.json",
                    name="native rotating campaign",
                )["taskCount"]
            )
            for binding in campaign_bindings
        )
        manifest, manifest_path = _native_prepared_finalizer_manifest(
            root=root,
            config=config,
            generation_index=generation_index,
            projection=projection,
            cohort=cohort,
            provisional=provisional_artifact,
            bundles=complete_bundles,
            complete_bundle_snapshot=True,
            auxiliary_plan=None,
            auxiliary_campaign_receipts=[],
            rich_members=[current_members[candidate_id] for candidate_id in rich_candidates],
            current_member_count=len(current_members),
            campaigns=campaign_bindings,
            total_generation_task_count=total_task_count,
            proposal_tail_index=proposal_tail_index,
            generation_record_extra=generation_record_extra or {},
        )
        execution = _invoke_native_finalizer(
            binary=native_finalizer_binary, manifest_path=manifest_path
        )
        published = _publish_native_generation_outputs(
            root=root,
            generation_index=generation_index,
            load_published_payloads=False,
        )
        commit = published["generation-commit.json"]
        if execution.get("commitSha256") != commit.get("commitSha256"):
            raise TemporalDiscoveryContractError(
                "native execution and committed publication identity disagree"
            )
        return _native_rotating_archive_result(
            root=root,
            generation_index=generation_index,
            published=published,
        )

    cumulative = build_cumulative_breeder_archive(
        contract=contract,
        generation_index=generation_index,
        provisional=provisional,
        bundles={
            candidate_id: [
                panel_bundles[panel_id] for panel_id in sorted(panel_bundles)
            ]
            for candidate_id, panel_bundles in bundles.items()
        },
        previous_archive=previous_cumulative,
        direction_aware=config.get("evolvableModuleAuthority") is not None,
    )
    _write_once(cumulative_path, cumulative)
    archive_result = build_rotating_qd_parent_archive(
        current_members=list(current_members.values()),
        cumulative_archive=cumulative,
        output_path=archive_path,
        generation_index=generation_index,
        previous_archive_path=parent_archive_path,
        bidirectional_pair_policy=(
            pair_policy_from_config(_pair_policy_authority(config))
            if _pair_policy_authority(config) is not None
            else None
        ),
        cell_capacity=int(config["frozenSearchPolicy"]["cellCapacity"]),
    )
    checkpoint = build_generation_evidence_checkpoint(
        contract=contract,
        generation_index=generation_index,
        stage="cumulative_archive",
        cohort=cohort,
        provisional_candidate_ids=list(rich_candidates),
        cumulative_archive=cumulative,
        stage_artifacts={
            "parentArchiveSha256": archive_result["archiveSha256"],
            "campaignsSha256": canonical_sha256(campaign_bindings),
        },
    )
    _replace(checkpoint_path, checkpoint)
    ledger = {
        "schemaVersion": "temporal_qd_rotating_generation_ledger_v1",
        "generationIndex": generation_index,
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"],
        "panelId": panel["panelId"],
        "cohortSha256": cohort["cohortSha256"],
        "provisionalSha256": provisional_artifact["provisionalSha256"],
        "cumulativeArchiveSha256": cumulative["archiveSha256"],
        "parentArchiveSha256": archive_result["archiveSha256"],
        "checkpointSha256": checkpoint["checkpointSha256"],
        "campaigns": campaign_bindings,
        "proposalCandidateIds": cohort["newProposalCandidateIds"],
        "retainedParentEvaluationCandidateIds": cohort[
            "retainedParentEvaluationCandidateIds"
        ],
        "proposalOnlyFunnelReporting": True,
    }
    ledger["ledgerSha256"] = canonical_sha256(ledger)
    _write_once(ledger_path, ledger)
    return {
        **archive_result,
        "rotatingEvidenceLedgerSha256": ledger["ledgerSha256"],
        "rotatingEvidenceCheckpointSha256": checkpoint["checkpointSha256"],
        "cumulativeArchiveSha256": cumulative["archiveSha256"],
        "additionalWorkerTaskCount": sum(
            int(
                _canonical_file(
                    Path(binding["campaignRoot"]) / "campaign.json",
                    name="rotating campaign",
                )["taskCount"]
            )
            for binding in campaign_bindings
            if binding["role"] != "proposal_current_panel"
        ),
    }


def _complete_rotating_generation_transaction(**kwargs: Any) -> dict[str, Any]:
    """Explicit Python-oracle materialization entry point for legacy evidence."""

    return _run_rotating_generation_transaction(
        **kwargs, finalization_engine=GENERATION_FINALIZATION_ENGINE_PYTHON
    )


def _complete_rotating_generation_transaction_native(
    **kwargs: Any,
) -> dict[str, Any]:
    """Opt-in Rust materialization entry point selected before Python completion."""

    return _run_rotating_generation_transaction(
        **kwargs, finalization_engine=GENERATION_FINALIZATION_ENGINE_RUST
    )


def _g0_generation_record_fields(
    *,
    generation_result: Mapping[str, Any],
    config: Mapping[str, Any],
    generation_index: int,
) -> dict[str, Any]:
    binding = generation_result.get("g0Bootstrap")
    if binding is None:
        return {}
    frozen = config.get("g0Bootstrap")
    if generation_index != 1 or not isinstance(frozen, Mapping):
        raise TemporalDiscoveryContractError(
            "G0 bootstrap result appeared outside its frozen generation-1 boundary"
        )
    configured_pool_size = frozen.get("initialConstructionPoolSize")
    constructed_accepted_count = generation_result.get(
        "constructedAcceptedCount"
    )
    reported_pool_size = generation_result.get("constructionPoolSize")
    if (
        isinstance(configured_pool_size, bool)
        or not isinstance(configured_pool_size, int)
        or configured_pool_size < 1
        or isinstance(constructed_accepted_count, bool)
        or not isinstance(constructed_accepted_count, int)
        or constructed_accepted_count != configured_pool_size
        or (
            reported_pool_size is not None
            and (
                isinstance(reported_pool_size, bool)
                or not isinstance(reported_pool_size, int)
                or reported_pool_size != configured_pool_size
            )
        )
    ):
        raise TemporalDiscoveryContractError(
            "G0 construction counts drifted from the frozen bootstrap authority"
        )
    return {
        "g0Bootstrap": _clone(binding, name="G0 bootstrap result binding"),
        # Early native-v1 results omitted this redundant top-level value even
        # though both the frozen construction width and accepted count were
        # identity-bound.  Reconstruct only after proving those authorities
        # agree; never guess from the selected evaluation width.
        "constructionPoolSize": configured_pool_size,
        "constructedAcceptedCount": constructed_accepted_count,
    }


def run_qd_supervisor(
    *,
    run_root: Path | str,
    initial_archive_path: Path | str,
    source_preparation_path: Path | str | None,
    base_generator_root: Path | str | None,
    confirmed_entry_admission_root: Path | str | None,
    template_preparation_path: Path | str,
    validator_command_file: Path | str | None,
    parameters: Mapping[str, Any],
    generation_count: int,
    autoresearch_commit: str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    gateway_url: str,
    gateway_token: str | None = None,
    first_generation_index: int = 1,
    initial_immigrant_continuation_ordinal: int = 0,
    evaluation_timeout_seconds: float = 86_400.0,
    enqueue_batch_size: int = 128,
    broad_admission: bool = False,
    stop_after_generation: int | None = None,
    construction_catalog_path: Path | str | None = None,
    generation_funnel_enabled: bool = False,
    bidirectional_pair_config: Mapping[str, Any] | None = None,
    pair_generation_engine: str | None = None,
    pair_generation_timeout_seconds: int = 3600,
    evidence_ladder_config: Mapping[str, Any] | None = None,
    rotating_evidence_config: Mapping[str, Any] | None = None,
    continuation_from: Mapping[str, Any] | None = None,
    initial_construction_pool_size: int | None = None,
    evaluation_population_size: int | None = None,
    tail_result_mode: str = TAIL_RESULT_MODE_LEGACY,
    native_finalization_validation: str = NATIVE_FINALIZATION_VALIDATION_NONE,
    generation_finalization_engine: str = GENERATION_FINALIZATION_ENGINE_DEFAULT,
    generation_finalizer_binary: Path | str | None = None,
    native_generation_deep_audit: bool = False,
    adopt_python_completed_generations: tuple[int, ...] = (),
    authorize_native_finalization_authority_rotation: bool = False,
    stop_before_evaluation_generation: int | None = None,
    evolvable_module_authority_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    tail_result_mode = _normalize_tail_result_mode(tail_result_mode)
    native_finalization_validation = _normalize_native_finalization_validation(
        native_finalization_validation
    )
    generation_finalization_engine = _normalize_generation_finalization_engine(
        generation_finalization_engine
    )
    root = Path(run_root)
    config_path = root / "config.json"
    persisted_config = (
        _canonical_file(config_path, name="existing QD supervisor config")
        if config_path.is_file()
        else None
    )
    _require_native_v5_finalization_engine(
        generation_finalization_engine=generation_finalization_engine,
        supplied_evolvable_authority=evolvable_module_authority_config,
        persisted_config=persisted_config,
    )
    native_finalizer_binary = (
        Path(generation_finalizer_binary)
        if generation_finalizer_binary is not None
        else None
    )
    if (
        native_finalization_validation == NATIVE_FINALIZATION_VALIDATION_HISTORICAL
        or generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST
    ) and native_finalizer_binary is None:
        raise TemporalDiscoveryContractError(
            "native generation finalization requires an explicit binary path"
        )
    if (
        generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST
        and native_finalization_validation == NATIVE_FINALIZATION_VALIDATION_HISTORICAL
    ):
        raise TemporalDiscoveryContractError(
            "Rust finalization engine and historical admission are separate modes"
        )
    if (
        adopt_python_completed_generations
        and generation_finalization_engine != GENERATION_FINALIZATION_ENGINE_RUST
    ):
        raise TemporalDiscoveryContractError(
            "Python boundary adoption is valid only with the Rust finalization engine"
        )
    root.mkdir(parents=True, exist_ok=True)
    _require_irreversible_native_cutover_engine(
        root=root,
        generation_finalization_engine=generation_finalization_engine,
    )
    legacy_reopen = _open_legacy_v5_g0_reopen_authority(root=root)
    effective_g0_finalization_runtime: dict[str, Any] | None = None
    if legacy_reopen is not None:
        # This old singleton is already sealed by its config, native receipt,
        # and migration receipt.  Do not reconstruct or rewrite it: the
        # missing field is part of its historical public bytes.
        config, effective_g0_finalization_runtime = legacy_reopen
        archive_binding = config.get("initialArchive")
        evaluation_binding = config.get("evaluation")
        generation_plan = config.get("generationPlan")
        if (
            not isinstance(archive_binding, Mapping)
            or not isinstance(evaluation_binding, Mapping)
            or not isinstance(generation_plan, Mapping)
            or not isinstance(generation_plan.get("firstGenerationIndex"), int)
            or isinstance(generation_plan.get("firstGenerationIndex"), bool)
            or int(generation_plan["firstGenerationIndex"]) < 1
        ):
            raise TemporalDiscoveryContractError(
                "legacy G0 migration lacks its frozen supervisor restart bindings"
            )
        initial_archive_file = Path(str(archive_binding.get("path") or ""))
        template_preparation_file = Path(
            str(evaluation_binding.get("templatePreparationPath") or "")
        )
        # This singleton is pair-owned, so legacy source/validator arguments
        # cannot influence a reopened phase.  They remain inert placeholders.
        source_preparation_file = root / ".pair-mode-unused-source.json"
        base_generator_dir = root / ".pair-mode-unused-generator"
        confirmed_entry_dir = root / ".pair-mode-unused-admission"
        first_generation_index = int(generation_plan["firstGenerationIndex"])
        initial_immigrant_continuation_ordinal = 0
        validator_command: list[str] = []
    else:
        initial_archive_file = Path(initial_archive_path)
        # Pair mode never constructs a v2 continuation.  These placeholders are
        # deliberately not persisted or opened in that mode.
        source_preparation_file = Path(source_preparation_path) if source_preparation_path is not None else root / ".pair-mode-unused-source.json"
        base_generator_dir = Path(base_generator_root) if base_generator_root is not None else root / ".pair-mode-unused-generator"
        confirmed_entry_dir = Path(confirmed_entry_admission_root) if confirmed_entry_admission_root is not None else root / ".pair-mode-unused-admission"
        template_preparation_file = Path(template_preparation_path)
        validator_file = Path(validator_command_file) if validator_command_file is not None else None
        initial_archive_transport_descriptor: dict[str, Any] | None = None
        if evolvable_module_authority_config is not None:
            # This in-memory authority is byte-identical to the authority
            # frozen below.  It exists solely to pin the certifier before its
            # descriptor becomes part of the write-once v5 config; Python
            # never opens or hashes the archive itself.
            if native_finalizer_binary is None:
                raise TemporalDiscoveryContractError(
                    "fresh current v5 requires the complete Rust runtime authority"
                )
            provisional_runtime = _native_finalization_runtime_authority(
                native_finalizer_binary,
                require_v5_control_plane_roles=True,
            )
            try:
                initial_archive_transport_descriptor = certify_native_v5_initial_archive(
                    runtime_authority=provisional_runtime,
                    archive_path=initial_archive_file,
                )
            except TemporalQDV5ControlPlaneError as exc:
                raise TemporalDiscoveryContractError(str(exc)) from exc
        config, validator_command = _frozen_config(
            initial_archive_path=initial_archive_file,
            source_preparation_path=source_preparation_file,
            base_generator_root=base_generator_dir,
            confirmed_entry_admission_root=confirmed_entry_dir,
            template_preparation_path=template_preparation_file,
            validator_command_file=validator_file,
            parameters=parameters,
            generation_count=generation_count,
            first_generation_index=first_generation_index,
            initial_immigrant_continuation_ordinal=initial_immigrant_continuation_ordinal,
            autoresearch_commit=autoresearch_commit,
            execution_engine_commit=execution_engine_commit,
            worker_contract_sha256=worker_contract_sha256,
            gateway_url=gateway_url,
            evaluation_timeout_seconds=evaluation_timeout_seconds,
            enqueue_batch_size=enqueue_batch_size,
            broad_admission=broad_admission,
            generation_funnel_enabled=generation_funnel_enabled,
            construction_catalog_path=construction_catalog_path,
            bidirectional_pair_config=bidirectional_pair_config,
            pair_generation_engine=pair_generation_engine,
            pair_generation_timeout_seconds=pair_generation_timeout_seconds,
            evidence_ladder_config=evidence_ladder_config,
            rotating_evidence_config=rotating_evidence_config,
            continuation_from=continuation_from,
            initial_construction_pool_size=initial_construction_pool_size,
            evaluation_population_size=evaluation_population_size,
            evolvable_module_authority_config=evolvable_module_authority_config,
            initial_archive_transport_descriptor=initial_archive_transport_descriptor,
        )
    if (
        tail_result_mode == TAIL_RESULT_MODE_INDEXED
        and config.get("rotatingEvidence") is None
    ):
        raise TemporalDiscoveryContractError(
            "indexed tail result mode is currently supported only by rotating evidence"
        )
    if (
        generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST
        and tail_result_mode != TAIL_RESULT_MODE_INDEXED
    ):
        raise TemporalDiscoveryContractError(
            "Rust generation finalization requires indexed tail authority"
        )
    state_path = root / "state.json"
    if legacy_reopen is None:
        _write_once(config_path, config)
        if config.get("evidenceLadder") is not None:
            _write_once(root / "evidence-ladder.json", config["evidenceLadder"])
        if config.get("rotatingEvidence") is not None:
            _write_once(root / "rotating-evidence.json", config["rotatingEvidence"])
    # Retain indexes only for the active supervisor transaction.  They are
    # source-verified once at admission and then reused by member, provenance,
    # funnel, and artifact reducers without reopening raw result blobs.
    tail_result_indexes: dict[Path, dict[str, Any]] = {}
    if state_path.exists():
        state = _load_state(state_path, config_sha256=config["configSha256"])
    else:
        state = {
            "schemaVersion": SUPERVISOR_STATE_SCHEMA,
            "configSha256": config["configSha256"],
            "status": "running",
            "stage": "initialized",
            "startedAt": _utc_now(),
            "updatedAt": _utc_now(),
            "currentGenerationIndex": first_generation_index,
            "nextImmigrantContinuationOrdinal": initial_immigrant_continuation_ordinal,
            "uniqueCandidatesEvaluated": 0,
            "workerTasksCompleted": 0,
            "uniqueIdentityCounts": {},
            "duplicateCounters": {},
            "proposalSlotCounters": {},
            "completedGenerations": [],
            "evaluationProgress": None,
            "tripwire": None,
        }
        _save_state(state_path, state)
    _require_irreversible_native_cutover_engine(
        root=root,
        generation_finalization_engine=generation_finalization_engine,
        state=state,
    )
    native_v5_control_plane = _native_v5_proposal_enabled(config)
    adoption_authority: Mapping[str, Any] | None = None
    runtime_authority: Mapping[str, Any] | None = None
    if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
        assert native_finalizer_binary is not None
        if native_v5_control_plane and adopt_python_completed_generations:
            raise TemporalDiscoveryContractError(
                "native v5 control-plane generations cannot adopt Python-completed boundaries"
            )
        if not native_v5_control_plane:
            adoption_authority = _prepare_native_finalization_adoption_authority(
                root=root,
                state=state,
                config=config,
                finalizer_binary=native_finalizer_binary,
                requested_generations=adopt_python_completed_generations,
            )
        runtime_authority = _freeze_native_finalization_runtime_authority(
            root=root,
            finalizer_binary=native_finalizer_binary,
            state=state,
            authorized_adoption_generations=frozenset(
                (adoption_authority or {}).get("generationIndices") or []
            ),
            authorize_rotation=authorize_native_finalization_authority_rotation,
            require_v5_control_plane_roles=native_v5_control_plane,
        )
        if native_v5_control_plane:
            _require_native_v5_control_plane_runtime_authority(runtime_authority)
            _recover_native_v5_state_application(
                root=root,
                state=state,
                state_path=state_path,
                config=config,
                runtime_authority=runtime_authority,
            )
    # This is deliberately before both the completed fast path and gateway
    # construction.  A restart must never treat a stale source, or a merely
    # self-claimed completed state, as permission to skip immutable work.
    validator_command = _validate_frozen_sources(config, run_root=root)
    if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
        assert native_finalizer_binary is not None
        if native_v5_control_plane:
            completed_by_index = _admit_completed_generations_native_v5(
                root=root, state=state, config=config
            )
        else:
            completed_by_index = _admit_completed_generations_native(
                root=root,
                state=state,
                state_path=state_path,
                config=config,
                binary=native_finalizer_binary,
                deep_audit=native_generation_deep_audit,
                tail_result_mode=tail_result_mode,
                tail_result_indexes=tail_result_indexes,
                adoption_authority=adoption_authority,
            )
    elif native_finalization_validation == NATIVE_FINALIZATION_VALIDATION_HISTORICAL:
        assert native_finalizer_binary is not None
        completed_by_index = _admit_completed_generations_native(
            root=root,
            state=state,
            state_path=state_path,
            config=config,
            binary=native_finalizer_binary,
            deep_audit=native_generation_deep_audit,
            tail_result_mode=tail_result_mode,
            tail_result_indexes=tail_result_indexes,
        )
    else:
        completed_by_index = _validate_completed_generations(
            root=root,
            state=state,
            config=config,
            tail_result_mode=tail_result_mode,
            tail_result_indexes=tail_result_indexes,
        )
    native_v5_proposal_transaction = _native_v5_proposal_enabled(config)
    native_pair_ledger_transaction = (
        (config.get("pairGenerationRuntime") or {}).get("engine")
        == PAIR_GENERATION_RUNTIME_RUST
        and not native_v5_proposal_transaction
    )
    committed_identity_ledger: dict[str, Any] | None = None
    committed_identity_ledger_sha256: str | None = None
    committed_native_v5_identity_ledger_descriptor: dict[str, Any] | None = None
    if native_v5_proposal_transaction:
        committed_native_v5_identity_ledger_descriptor = (
            _reconcile_native_v5_identity_ledger(
            root=root,
            state=state,
            state_path=state_path,
            completed_by_index=completed_by_index,
            )
        )
    elif native_pair_ledger_transaction:
        (
            committed_identity_ledger,
            committed_identity_ledger_sha256,
        ) = _reconcile_native_pair_identity_ledger(
            root=root,
            state=state,
            state_path=state_path,
            completed_by_index=completed_by_index,
        )
    # Completed generation validation does not share a live reduction
    # transaction with the next generation.  Release its retained projections
    # before opening the gateway or creating new work.
    tail_result_indexes.clear()
    if state.get("status") == "completed":
        expected_completed = int(config["generationPlan"]["generationCount"])
        if len(completed_by_index) != expected_completed:
            raise TemporalDiscoveryContractError(
                "completed QD supervisor state lacks a complete artifact ledger"
            )
        if int(state.get("uniqueCandidatesEvaluated") or 0) != int(
            config["generationPlan"]["targetUniqueEvaluations"]
        ):
            raise TemporalDiscoveryContractError(
                "completed QD supervisor state misses its frozen evaluation target"
            )
        _validate_evidence_ladder_execution(root=root, state=state, config=config)
        return {
            "schemaVersion": "temporal_qd_supervisor_result_v3",
            "status": "completed",
            "configSha256": config["configSha256"],
            "stateSha256": state["stateSha256"],
            "uniqueCandidatesEvaluated": state["uniqueCandidatesEvaluated"],
            "completedGenerationCount": len(state["completedGenerations"]),
            "uniqueIdentityCounts": state.get("uniqueIdentityCounts") or {},
            "duplicateCounters": state.get("duplicateCounters") or {},
            "proposalSlotCounters": state.get("proposalSlotCounters") or {},
            "runRoot": str(root.resolve()),
        }

    # Current v5 dispatch is a pinned Rust subprocess.  Do not even create
    # the historical Python gateway client for that path: a later resumed
    # stage must not obtain an ambient Python evaluation transport as a
    # fallback.  Legacy/oracle configurations retain their existing client.
    client: LabGatewayClient | None = None
    if not native_v5_proposal_transaction:
        client = LabGatewayClient(
            base_url=config["evaluation"]["gatewayUrl"],
            token=gateway_token,
            timeout_seconds=30.0,
        )
    try:
        first = int(config["generationPlan"]["firstGenerationIndex"])
        last = int(config["generationPlan"]["lastGenerationIndex"])
        parent_archive_path = initial_archive_file
        parent_archive_sha256 = config["initialArchive"]["archiveSha256"]
        parent_archive_descriptor: dict[str, Any] | None = None
        if native_v5_proposal_transaction:
            initial_transport = config["initialArchive"].get("transportDescriptor")
            if not isinstance(initial_transport, Mapping):
                raise TemporalDiscoveryContractError(
                    "native v5 initial archive transport descriptor is unavailable"
                )
            parent_archive_descriptor = _native_v5_proposal_archive_descriptor(
                {
                    "absolutePath": initial_transport.get("absolutePath"),
                    "fileSha256": initial_transport.get("fileSha256"),
                    "semanticSha256": initial_transport.get("archiveSha256"),
                    "byteLength": initial_transport.get("sizeBytes"),
                },
                name="native v5 initial archive",
            )
        parent_schedule = config["initialArchive"].get("parentSchedule")
        previous_cumulative_archive_path: Path | None = None
        previous_cumulative_archive_sha256: str | None = None
        previous_cumulative_archive_descriptor: dict[str, Any] | None = None
        immigrant_cursor = int(initial_immigrant_continuation_ordinal)
        if completed_by_index:
            latest = max(completed_by_index)
            if set(completed_by_index) != set(range(first, latest + 1)):
                raise TemporalDiscoveryContractError(
                    "completed QD generations are not contiguous"
                )
            latest_record = completed_by_index[latest]
            if native_v5_proposal_transaction:
                if latest_record.get("archivePath") != "archive.json":
                    raise TemporalDiscoveryContractError(
                        "native v5 completed record archive path drifted"
                    )
                try:
                    replay = run_native_v5_generation_finalizer(
                        runtime_authority=_native_runtime_authority_for_generation(
                            root=root, generation_index=latest
                        ),
                        manifest_path=_native_finalization_root(root, latest)
                        / "manifest.json",
                        committed_restart_only=True,
                    )
                except TemporalQDV5ControlPlaneError as exc:
                    raise TemporalDiscoveryContractError(str(exc)) from exc
                artifacts = replay.get("artifacts")
                if not isinstance(artifacts, Mapping):
                    raise TemporalDiscoveryContractError(
                        "native v5 completed finalizer lacks compact archive descriptors"
                    )
                parent_archive_descriptor = _native_v5_archive_descriptor_from_finalizer_artifact(
                    _clone(artifacts.get("parentArchive"), name="native v5 parent archive artifact"),
                    name="native v5 completed parent archive",
                    expected_relative_path="archive.json",
                )
                previous_cumulative_archive_descriptor = (
                    _native_v5_archive_descriptor_from_finalizer_artifact(
                        _clone(
                            artifacts.get("cumulativeArchive"),
                            name="native v5 cumulative archive artifact",
                        ),
                        name="native v5 completed cumulative archive",
                        expected_relative_path="evidence/cumulative-archive.json",
                    )
                )
                parent_archive_path = Path(parent_archive_descriptor["absolutePath"])
                previous_cumulative_archive_path = Path(
                    previous_cumulative_archive_descriptor["absolutePath"]
                )
                parent_archive_sha256 = parent_archive_descriptor["semanticSha256"]
                previous_cumulative_archive_sha256 = (
                    previous_cumulative_archive_descriptor["semanticSha256"]
                )
                parent_schedule = latest_record.get("parentSchedule")
                immigrant_cursor = int(
                    state.get("nextImmigrantContinuationOrdinal") or 0
                )
            else:
                parent_archive_path = Path(latest_record["archivePath"])
                parent_archive_sha256 = latest_record["archiveSha256"]
                parent_schedule = latest_record.get("parentSchedule")
                immigrant_cursor = int(
                    latest_record["nextImmigrantContinuationOrdinal"]
                )

        for generation_index in range(first, last + 1):
            if generation_index in completed_by_index:
                continue
            generation_root = (
                root / "generations" / f"generation-{generation_index:04d}"
            )
            proposal_root = generation_root / "proposal"
            campaign_root = generation_root / "campaign"
            result_root = campaign_root / "screening-run"
            archive_path = generation_root / "archive.json"
            generation_template_file = template_preparation_file
            if config.get("rotatingEvidence") is not None:
                template_binding = template_for_generation(config["rotatingEvidence"], generation_index)
                generation_template_file = Path(template_binding["path"])
                if not native_v5_proposal_transaction:
                    template_payload = _read(
                        generation_template_file, name="rotating QD panel template"
                    )
                    validate_generation_template(
                        template_payload, config["rotatingEvidence"], generation_index
                    )

            state.update(
                {
                    "status": "running",
                    "stage": "generating",
                    "currentGenerationIndex": generation_index,
                    "generationStartedAt": _utc_now(),
                    "evaluationProgress": None,
                    "tripwire": None,
                }
            )
            _save_state(state_path, state)
            _event(
                "generation_started",
                generationIndex=generation_index,
                parentArchive=str(parent_archive_path.resolve()),
                immigrantContinuationOrdinal=immigrant_cursor,
            )
            native_v5_identity_ledger_input: dict[str, Any] | None = None
            if native_v5_proposal_transaction:
                native_v5_generation_kind = (
                    V5_PROPOSAL_GENERATION_G0
                    if isinstance(config.get("g0Bootstrap"), Mapping)
                    and generation_index == 1
                    else V5_PROPOSAL_GENERATION_EVOLVED
                )
                native_v5_identity_ledger_input = _build_native_v5_identity_ledger_input(
                    generation_kind=native_v5_generation_kind,
                    committed_identity_ledger_descriptor=(
                        committed_native_v5_identity_ledger_descriptor
                    ),
                )
            elif native_pair_ledger_transaction:
                assert committed_identity_ledger is not None
                assert committed_identity_ledger_sha256 is not None
                _prepare_native_pair_identity_ledger_transaction(
                    root=root,
                    state=state,
                    state_path=state_path,
                    generation_index=generation_index,
                    input_ledger=committed_identity_ledger,
                    input_ledger_sha256=committed_identity_ledger_sha256,
                )
            # Do not let a file-backed source change while an earlier
            # generation is running and then silently feed a later phase.
            validator_command = _validate_frozen_sources(config, run_root=root)
            generation_kwargs = dict(
                parent_archive_path=parent_archive_path,
                parent_archive_sha256=parent_archive_sha256,
                parent_schedule=parent_schedule,
                output_root=proposal_root,
                generation_index=generation_index,
                immigrant_continuation_start=immigrant_cursor,
                allow_empty_quality_bootstrap=bool(config["broadAdmission"]),
                parameters=config["frozenSearchPolicy"],
                evidence_identity_context=config["evaluation"]["predeclaredEvidenceContext"],
                identity_ledger_path=root / "identity-ledger.json",
                construction_catalog_path=(
                    (config.get("constructionOperatorPolicy") or {})
                    .get("catalog", {})
                    .get("path")
                ),
                generation_funnel_enabled=bool(
                    (config.get("generationFunnel") or {}).get("enabled")
                ),
                qd_publication_authority={
                    "qdVersion": config["qdVersion"],
                    "policyName": config["policyName"],
                    "policySha256": config["policySha256"],
                    "frozenPolicy": config["frozenPolicy"],
                },
                archive_policy_authority=(
                    config["evolvableModuleAuthority"]["archivePolicyAuthority"]
                    if config.get("evolvableModuleAuthority") is not None
                    else None
                ),
            )
            if native_v5_proposal_transaction:
                if parent_archive_descriptor is None:
                    raise TemporalDiscoveryContractError(
                        "native v5 generation lacks its sealed parent archive descriptor"
                    )
                generation_result = _run_native_v5_generation(
                    root=root,
                    config=config,
                    generation_index=generation_index,
                    parent_archive_descriptor=parent_archive_descriptor,
                    parent_schedule=parent_schedule,
                    identity_ledger_input=native_v5_identity_ledger_input,
                )
            elif config.get("bidirectionalPairGeneration") is None:
                # Legacy generation owns the file-backed source and command
                # validator contract.  Pair mode has a distinct native
                # authority and intentionally carries no legacy validator.
                generation_kwargs.update(
                    source_preparation_path=source_preparation_file,
                    base_generator_root=base_generator_dir,
                    confirmed_entry_admission_root=confirmed_entry_dir,
                    validator_command=validator_command,
                    validator_timeout_seconds=float(
                        config["validator"]["timeoutSeconds"]
                    ),
                )
                generation_result = generate_qd_generation(**generation_kwargs)
            else:
                try:
                    pair_runtime = validate_pair_generation_runtime_config(
                        config.get("pairGenerationRuntime")
                    )
                except TemporalQDNativeError as exc:
                    raise TemporalDiscoveryContractError(str(exc)) from exc
                if pair_runtime["engine"] == PAIR_GENERATION_RUNTIME_PYTHON:
                    with PairAuthorityBundle(
                        config.get(
                            "bidirectionalPairSourceAuthority",
                            config["bidirectionalPairGeneration"],
                        )
                    ) as pair_authority:
                        evolvable = (
                            pair_authority.open_evolvable_module_authority(
                                config["evolvableModuleAuthority"]
                            )
                            if config.get("evolvableModuleAuthority") is not None
                            else None
                        )
                        bindings = (
                            evolvable.generation_bindings(
                                config["bidirectionalPairGeneration"]
                            )
                            if evolvable is not None
                            else None
                        )
                        if bindings is not None:
                            generation_kwargs["archive_policy_authority"] = bindings[
                                "archivePolicyAuthority"
                            ]
                        g0 = config.get("g0Bootstrap")
                        g0_runtime = None
                        if isinstance(g0, Mapping) and generation_index == 1:
                            reopened_g0_runtime = _resolve_g0_finalization_runtime_for_reopen(
                                config=config,
                                pair_runtime=pair_runtime,
                                run_root=root,
                            )
                            if (
                                effective_g0_finalization_runtime is not None
                                and reopened_g0_runtime
                                != effective_g0_finalization_runtime
                            ):
                                raise TemporalDiscoveryContractError(
                                    "legacy G0 effective runtime drifted after restart preflight"
                                )
                            g0_runtime = (
                                effective_g0_finalization_runtime
                                if effective_g0_finalization_runtime is not None
                                else reopened_g0_runtime
                            )
                            if (
                                g0_runtime is None
                                or g0_runtime["engine"]
                                != G0_FINALIZATION_RUNTIME_RUST
                            ):
                                raise TemporalDiscoveryContractError(
                                    "production G0 cannot select the Python finalization oracle"
                                )
                        generation_result = generate_qd_generation(
                            **generation_kwargs,
                            pair_generation_runtime=pair_runtime,
                            bidirectional_pair_run_config=(
                                bindings["runConfig"]
                                if bindings is not None
                                else config["bidirectionalPairGeneration"]
                            ),
                            bidirectional_pair_policy=pair_policy_from_config(
                                _pair_policy_authority(config)
                            ),
                            bidirectional_pair_factory=(
                                evolvable.factory if evolvable is not None else pair_authority.factory
                            ),
                            bidirectional_module_authority=(
                                evolvable.operator if evolvable is not None else pair_authority.operator
                            ),
                            bidirectional_native_validator=pair_authority.validator,
                            bidirectional_pair_compiler=pair_authority.compiler,
                            bidirectional_operator_implementation_identity=(
                                bindings["operatorImplementation"]
                                if bindings is not None
                                else config["bidirectionalPairGeneration"]["operatorImplementation"]
                            ),
                            initial_construction_pool_size=(int(g0["initialConstructionPoolSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                            evaluation_population_size=(int(g0["evaluationPopulationSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                            g0_finalization_runtime=g0_runtime,
                        )
                elif pair_runtime["engine"] == PAIR_GENERATION_RUNTIME_RUST:
                    g0 = config.get("g0Bootstrap")
                    generation_result = generate_qd_generation(
                        **generation_kwargs,
                        pair_generation_runtime=pair_runtime,
                        bidirectional_pair_run_config=config["bidirectionalPairGeneration"],
                        bidirectional_pair_policy=pair_policy_from_config(
                            _pair_policy_authority(config)
                        ),
                        bidirectional_operator_implementation_identity=config["bidirectionalPairGeneration"]["operatorImplementation"],
                        initial_construction_pool_size=(int(g0["initialConstructionPoolSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                        evaluation_population_size=(int(g0["evaluationPopulationSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                    )
                else:
                    raise TemporalDiscoveryContractError(
                        "pair generation runtime selected an unknown engine"
                    )
            if generation_result.get("completed") is not True:
                raise TemporalDiscoveryContractError(
                    "QD generation proposal manifest did not complete"
                )
            generation_output_identity_ledger: dict[str, Any] | None = None
            generation_output_identity_ledger_sha256: str | None = None
            if native_pair_ledger_transaction:
                assert committed_identity_ledger is not None
                assert committed_identity_ledger_sha256 is not None
                (
                    generation_output_identity_ledger,
                    generation_output_identity_ledger_sha256,
                ) = _seal_native_pair_identity_ledger_output(
                    root=root,
                    state=state,
                    state_path=state_path,
                    generation_index=generation_index,
                    input_ledger=committed_identity_ledger,
                    input_ledger_sha256=committed_identity_ledger_sha256,
                    generation_result=generation_result,
                )

            # A current native-v5 receipt may never fall through into the
            # historic Python campaign/evaluation/archive pipeline.  Its sole
            # continuation is the receipt-bound Rust campaign/prefinalizer/
            # finalizer chain below.
            if native_v5_proposal_transaction:
                if stop_before_evaluation_generation == generation_index:
                    raise TemporalDiscoveryContractError(
                        "current native v5 does not support the legacy Python "
                        "stop-before-evaluation canary"
                    )
                native_completion = _complete_native_v5_generation(
                    root=root,
                    state=state,
                    state_path=state_path,
                    config=config,
                    generation_index=generation_index,
                    generation_result=generation_result,
                    identity_ledger_input=native_v5_identity_ledger_input,
                    parent_archive_descriptor=parent_archive_descriptor,
                    previous_cumulative_archive_descriptor=previous_cumulative_archive_descriptor,
                    gateway_token=gateway_token,
                )
                generation_record = native_completion["generationRecord"]
                committed_native_v5_identity_ledger_descriptor = (
                    _native_v5_identity_ledger_descriptor(
                        state.get(NATIVE_V5_COMMITTED_IDENTITY_LEDGER_KEY),
                        name="native v5 committed postproposal identity ledger",
                        expected_path=_native_v5_identity_ledger_output_path(
                            root, generation_index
                        ),
                    )
                )
                expected_ledger = _native_v5_identity_ledger_descriptor_from_adapter(
                    adapter=_clone(
                        generation_result["nativeV5Construction"],
                        name="native v5 completed construction adapter",
                    ),
                    root=root,
                    generation_index=generation_index,
                    name="native v5 completed proposal identity ledger",
                )
                if committed_native_v5_identity_ledger_descriptor != expected_ledger:
                    raise TemporalDiscoveryContractError(
                        "native v5 postproposal identity-ledger descriptor drifted"
                    )
                if generation_record.get("archivePath") != "archive.json":
                    raise TemporalDiscoveryContractError(
                        "native v5 finalizer archive path drifted"
                    )
                final_artifacts = native_completion["finalization"].get("artifacts")
                if not isinstance(final_artifacts, Mapping):
                    raise TemporalDiscoveryContractError(
                        "native v5 finalizer lacks compact archive descriptors"
                    )
                parent_archive_descriptor = _native_v5_archive_descriptor_from_finalizer_artifact(
                    _clone(final_artifacts.get("parentArchive"), name="native v5 finalized parent artifact"),
                    name="native v5 finalized parent archive",
                    expected_relative_path="archive.json",
                )
                previous_cumulative_archive_descriptor = (
                    _native_v5_archive_descriptor_from_finalizer_artifact(
                        _clone(
                            final_artifacts.get("cumulativeArchive"),
                            name="native v5 finalized cumulative artifact",
                        ),
                        name="native v5 finalized cumulative archive",
                        expected_relative_path="evidence/cumulative-archive.json",
                    )
                )
                parent_archive_path = Path(parent_archive_descriptor["absolutePath"])
                previous_cumulative_archive_path = Path(
                    previous_cumulative_archive_descriptor["absolutePath"]
                )
                parent_archive_sha256 = parent_archive_descriptor["semanticSha256"]
                previous_cumulative_archive_sha256 = (
                    previous_cumulative_archive_descriptor["semanticSha256"]
                )
                parent_schedule = generation_record.get("parentSchedule")
                immigrant_cursor = int(state["nextImmigrantContinuationOrdinal"])
                completed_by_index[generation_index] = generation_record
                _event(
                    "generation_completed",
                    generationIndex=generation_index,
                    uniqueCandidatesEvaluated=state["uniqueCandidatesEvaluated"],
                    occupiedCellCount=generation_record.get("occupiedCellCount"),
                    newCellCount=generation_record.get("newCellCount"),
                    archiveSha256=parent_archive_sha256,
                )
                if stop_after_generation == generation_index:
                    return {
                        "schemaVersion": "temporal_qd_supervisor_result_v3",
                        "status": "paused_at_generation_boundary",
                        "generationIndex": generation_index,
                        "configSha256": config["configSha256"],
                        "stateSha256": state["stateSha256"],
                        "uniqueIdentityCounts": state.get("uniqueIdentityCounts") or {},
                        "duplicateCounters": state.get("duplicateCounters") or {},
                        "runRoot": str(root.resolve()),
                    }
                continue

            state["stage"] = "freezing_evaluation"
            _save_state(state_path, state)
            validator_command = _validate_frozen_sources(config, run_root=root)
            campaign_result = freeze_qd_screening_campaign(
                population_path=proposal_root / "population.json",
                template_preparation_path=generation_template_file,
                output_root=campaign_root,
                execution_engine_commit=config["repositories"]["executionEngineCommit"],
                worker_contract_sha256=config["workerContractSha256"],
                construction_catalog_path=(
                    (config.get("constructionOperatorPolicy") or {})
                    .get("catalog", {})
                    .get("path")
                ),
                evidence_ladder=config.get("evidenceLadder"),
                rotating_evidence=config.get("rotatingEvidence"),
                archive_policy_authority=(
                    config["evolvableModuleAuthority"]["archivePolicyAuthority"]
                    if config.get("evolvableModuleAuthority") is not None
                    else None
                ),
                behavior_attribution_requirement=(
                    config["evaluation"].get("behaviorAttributionRequirement")
                    if config.get("evolvableModuleAuthority") is not None
                    else None
                ),
            )
            evaluation_identity = _read(
                campaign_root / "evaluation-identity.json",
                name="QD evaluation identity",
            )
            if (
                evaluation_identity.get("executionEngineCommit")
                != config["repositories"]["executionEngineCommit"]
                or evaluation_identity.get("workerContract", {}).get(
                    "workerContractSha256"
                )
                != config["workerContractSha256"]
                or evaluation_identity.get("policySha256") != config["policySha256"]
                or (config.get("rotatingEvidence") is None and evaluation_identity.get("predeclaredEvidenceContextSha256")
                != config["evaluation"]["predeclaredEvidenceContextSha256"])
            ):
                raise TemporalDiscoveryContractError(
                    "frozen QD evaluation identity drifted from supervisor config"
                )

            if stop_before_evaluation_generation == generation_index:
                state["stage"] = "evaluation_frozen_canary"
                state["evaluationProgress"] = {
                    "campaignSha256": campaign_result["campaignSha256"],
                    "evaluationIdentitySha256": campaign_result[
                        "evaluationIdentitySha256"
                    ],
                    "completedTaskCount": _completed_task_count(
                        result_root / "checkpoint.json"
                    ),
                    "taskCount": campaign_result["taskCount"],
                    "resultRoot": str(result_root.resolve()),
                }
                _save_state(state_path, state)
                _event(
                    "generation_evaluation_frozen_canary",
                    generationIndex=generation_index,
                    taskCount=campaign_result["taskCount"],
                    campaignSha256=campaign_result["campaignSha256"],
                )
                return {
                    "schemaVersion": "temporal_qd_supervisor_result_v3",
                    "status": "paused_before_evaluation",
                    "generationIndex": generation_index,
                    "campaignSha256": campaign_result["campaignSha256"],
                    "taskCount": campaign_result["taskCount"],
                    "configSha256": config["configSha256"],
                    "stateSha256": state["stateSha256"],
                    "runRoot": str(root.resolve()),
                }

            state["stage"] = "evaluating"
            state["evaluationProgress"] = {
                "campaignSha256": campaign_result["campaignSha256"],
                "evaluationIdentitySha256": campaign_result["evaluationIdentitySha256"],
                "completedTaskCount": _completed_task_count(
                    result_root / "checkpoint.json"
                ),
                "taskCount": campaign_result["taskCount"],
                "resultRoot": str(result_root.resolve()),
            }
            _save_state(state_path, state)
            last_progress_write = 0.0
            last_progress_count = -1

            def progress(
                values: dict[str, Any],
                *,
                _generation_index: int = generation_index,
            ) -> None:
                nonlocal last_progress_write, last_progress_count
                completed = int(values["completedTaskCount"])
                now = time.monotonic()
                should_write = (
                    completed == int(values["taskCount"])
                    or completed - last_progress_count >= 25
                    or now - last_progress_write >= 30.0
                )
                if not should_write:
                    return
                state["evaluationProgress"] = {
                    **state["evaluationProgress"],
                    "completedTaskCount": completed,
                    "lastCompletedTaskId": values["taskId"],
                }
                _save_state(state_path, state)
                _event(
                    "evaluation_progress",
                    generationIndex=_generation_index,
                    completedTaskCount=completed,
                    taskCount=int(values["taskCount"]),
                )
                last_progress_count = completed
                last_progress_write = now

            authority = _read(campaign_root / "authority.json", name="QD authority")
            evaluation_result = run_temporal_search_tasks(
                client,
                authority,
                output_root=result_root,
                timeout_seconds=float(
                    config["evaluation"]["timeoutSecondsPerGeneration"]
                ),
                resume=True,
                enqueue_batch_size=int(config["evaluation"]["enqueueBatchSize"]),
                progress_callback=progress,
                include_selection_summary=(
                    generation_finalization_engine
                    != GENERATION_FINALIZATION_ENGINE_RUST
                ),
                **(
                    {
                        "behavior_attribution_requirement": config["evaluation"][
                            "behaviorAttributionRequirement"
                        ]
                    }
                    if config.get("evolvableModuleAuthority") is not None
                    else {}
                ),
            )
            if evaluation_result["completedTaskCount"] != campaign_result["taskCount"]:
                raise TemporalDiscoveryContractError(
                    "QD generation evaluation did not complete its exact task matrix"
                )

            state["stage"] = (
                "rotating_evidence_transaction"
                if config.get("rotatingEvidence") is not None
                else "reducing_archive"
            )
            _save_state(state_path, state)
            if config.get("rotatingEvidence") is not None:
                rotating_transaction = (
                    _complete_rotating_generation_transaction_native
                    if generation_finalization_engine
                    == GENERATION_FINALIZATION_ENGINE_RUST
                    else _complete_rotating_generation_transaction
                )
                archive_result = rotating_transaction(
                    root=root,
                    generation_root=generation_root,
                    generation_index=generation_index,
                    proposal_root=proposal_root,
                    proposal_campaign_root=campaign_root,
                    parent_archive_path=parent_archive_path,
                    archive_path=archive_path,
                    config=config,
                    client=client,
                    tail_result_mode=tail_result_mode,
                    tail_result_indexes=tail_result_indexes,
                    native_finalizer_binary=native_finalizer_binary,
                    generation_record_extra=_g0_generation_record_fields(
                        generation_result=generation_result,
                        config=config,
                        generation_index=generation_index,
                    ),
                )
            else:
                archive_result = build_qd_archive(
                    population_path=proposal_root / "population.json",
                    result_root=result_root,
                    output_path=archive_path,
                    generation_index=generation_index,
                    previous_archive_path=parent_archive_path,
                    generation_journal_path=proposal_root / "generation-journal.json",
                    cell_capacity=int(config["frozenSearchPolicy"]["cellCapacity"]),
                    minimum_total_trades=int(
                        config["frozenSearchPolicy"]["minimumTotalTrades"]
                    ),
                    minimum_trades_per_window=int(
                        config["frozenSearchPolicy"]["minimumTradesPerWindow"]
                    ),
                    cap_trades=int(config["frozenSearchPolicy"]["capTrades"]),
                    archive_policy_authority=(
                        config["evolvableModuleAuthority"][
                            "archivePolicyAuthority"
                        ]
                        if config.get("evolvableModuleAuthority") is not None
                        else None
                    ),
                )
            funnel_enabled = bool((config.get("generationFunnel") or {}).get("enabled"))
            if (
                funnel_enabled
                and generation_finalization_engine
                == GENERATION_FINALIZATION_ENGINE_PYTHON
            ):
                evaluation_population = load_evaluation_population(
                    population_path=proposal_root / "population.json",
                    journal_path=proposal_root / "generation-journal.json",
                )
                funnel = build_qd_generation_funnel(
                    proposal_entries=evaluation_population["funnelEntries"],
                    proposal_accounting=_canonical_file(
                        proposal_root / "generation-journal.json",
                        name="QD generation journal",
                    ),
                    population=evaluation_population,
                    authority=_canonical_file(campaign_root / "authority.json", name="QD authority"),
                    task_manifest=_canonical_file(result_root / "task-manifest.json", name="QD task manifest"),
                    checkpoint=_canonical_file(result_root / "checkpoint.json", name="QD evaluation checkpoint"),
                    archive=_canonical_file(archive_path, name="QD generation archive"),
                    minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]),
                    minimum_trades_per_window=int(config["frozenSearchPolicy"]["minimumTradesPerWindow"]),
                    tail_result_index=(
                        _verified_tail_result_index(
                            campaign_root=campaign_root,
                            indexes=tail_result_indexes,
                            include_funnel_projection=True,
                        )
                        if tail_result_mode == TAIL_RESULT_MODE_INDEXED
                        else None
                    ),
                )
                try:
                    write_generation_funnel_artifact(
                        generation_root / "generation-funnel.json", funnel
                    )
                except GenerationFunnelContractError as exc:
                    raise TemporalDiscoveryContractError("could not publish QD generation funnel") from exc
            artifacts = _capture_generation_artifacts(
                root=root,
                generation_index=generation_index,
                generation_funnel_enabled=funnel_enabled,
                tail_result_mode=tail_result_mode,
                tail_result_indexes=tail_result_indexes,
                verify_population_file=(
                    generation_finalization_engine
                    != GENERATION_FINALIZATION_ENGINE_RUST
                    and not native_v5_proposal_transaction
                ),
                verify_rotating_campaign_artifacts=(
                    generation_finalization_engine
                    != GENERATION_FINALIZATION_ENGINE_RUST
                    and not native_v5_proposal_transaction
                ),
            )
            if (
                artifacts["population"]["populationSha256"]
                != generation_result["populationSha256"]
                or artifacts["journal"]["journalSha256"]
                != generation_result["journalSha256"]
                or artifacts["campaign"]["campaignSha256"]
                != campaign_result["campaignSha256"]
                or artifacts["evaluationIdentity"]["evaluationIdentitySha256"]
                != campaign_result["evaluationIdentitySha256"]
                or artifacts["archive"]["archiveSha256"]
                != archive_result["archiveSha256"]
                or (
                    "evaluationPopulation" in artifacts
                    and artifacts["evaluationPopulation"]["evaluationPopulationSha256"]
                    != generation_result.get("evaluationPopulationSha256")
                )
            ):
                raise TemporalDiscoveryContractError(
                    "completed QD generation artifact identities disagree with phase output"
                )
            journal_sha = (
                generation_result["journalSha256"]
                if native_v5_proposal_transaction
                else _identity_payload(
                    _read(
                        proposal_root / "generation-journal.json",
                        name="QD generation journal",
                    ),
                    "journalSha256",
                    name="QD generation journal",
                )
            )
            generation_record = {
                "generationIndex": generation_index,
                "populationSha256": generation_result["populationSha256"],
                **(
                    {
                        "evaluationPopulationSha256": generation_result[
                            "evaluationPopulationSha256"
                        ]
                    }
                    if generation_result.get("evaluationPopulationSha256") is not None
                    else {}
                ),
                "journalSha256": journal_sha,
                "proposalCount": generation_result["proposalCount"],
                "candidateCount": generation_result["candidateCount"],
                **_g0_generation_record_fields(
                    generation_result=generation_result,
                    config=config,
                    generation_index=generation_index,
                ),
                "originProposalCounts": generation_result["originProposalCounts"],
                "originAcceptedCounts": generation_result["originAcceptedCounts"],
                **(
                    {
                        "reproductionAllocation": generation_result[
                            "reproductionAllocation"
                        ],
                        "reproductionAllocationAccounting": generation_result[
                            "reproductionAllocationAccounting"
                        ],
                    }
                    if generation_result.get("reproductionAllocation") is not None
                    and generation_result.get("reproductionAllocationAccounting")
                    is not None
                    else {}
                ),
                "campaignSha256": campaign_result["campaignSha256"],
                "evaluationIdentitySha256": campaign_result["evaluationIdentitySha256"],
                "taskMatrixSha256": campaign_result["taskMatrixSha256"],
                "taskCount": campaign_result["taskCount"],
                "totalGenerationTaskCount": int(campaign_result["taskCount"])
                + int(archive_result.get("additionalWorkerTaskCount") or 0),
                "archiveSha256": archive_result["archiveSha256"],
                **(
                    {"parentSchedule": archive_result["parentSchedule"]}
                    if archive_result.get("parentSchedule") is not None
                    else {}
                ),
                "resultSetSha256": _sha256(
                    _canonical_file(
                        archive_path, name="QD generation archive"
                    ).get("resultSetSha256"),
                    name="QD generation archive result set",
                ),
                "archivePath": artifacts["archive"]["path"],
                "occupiedCellCount": archive_result["occupiedCellCount"],
                "newCellCount": archive_result["newCellCount"],
                "qualityMemberCount": archive_result["qualityMemberCount"],
                "observationalMemberCount": archive_result["observationalMemberCount"],
                "negativeNoveltyMemberCount": archive_result[
                    "negativeNoveltyMemberCount"
                ],
                "paretoAdmissionCount": archive_result["paretoAdmissionCount"],
                "paretoEvictionCount": archive_result["paretoEvictionCount"],
                **(
                    {
                        "rotatingEvidenceLedgerSha256": archive_result[
                            "rotatingEvidenceLedgerSha256"
                        ],
                        "rotatingEvidenceCheckpointSha256": archive_result[
                            "rotatingEvidenceCheckpointSha256"
                        ],
                        "cumulativeArchiveSha256": archive_result[
                            "cumulativeArchiveSha256"
                        ],
                        "frontierMemberCount": archive_result[
                            "frontierMemberCount"
                        ],
                    }
                    if config.get("rotatingEvidence") is not None
                    else {}
                ),
                "proposalSlots": generation_result["proposalSlots"],
                "uniqueIdentityCounts": generation_result["uniqueIdentityCounts"],
                "duplicateCounters": generation_result["duplicateCounters"],
                "proposalSlotCounters": generation_result["proposalSlotCounters"],
                "nextImmigrantContinuationOrdinal": generation_result[
                    "nextImmigrantContinuationOrdinal"
                ],
                **(
                    {
                        "nativeV5Construction": _clone(
                            generation_result["nativeV5Construction"],
                            name="native v5 construction adapter",
                        ),
                        "nativeV5Invocation": _clone(
                            generation_result["nativeV5Invocation"],
                            name="native v5 supervisor invocation",
                        ),
                    }
                    if native_v5_proposal_transaction
                    else {}
                ),
                **(
                    {
                        "generationFunnelArtifactSha256": artifacts["generationFunnel"]["artifactSha256"],
                        "generationFunnelSnapshotSha256": artifacts["generationFunnelSnapshot"]["snapshotSha256"],
                    }
                    if funnel_enabled
                    else {}
                ),
                "artifacts": artifacts,
                "completedAt": _utc_now(),
            }
            native_state_patch: Mapping[str, Any] | None = None
            if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
                assert native_finalizer_binary is not None
                native_record = _clone(
                    archive_result.get("nativeGenerationRecord"),
                    name="native production generation record",
                )
                native_state_patch = _clone(
                    archive_result.get("nativeStatePatch"),
                    name="native production state patch",
                )
                if (
                    not isinstance(native_record, Mapping)
                    or not isinstance(native_state_patch, Mapping)
                    or not _generation_artifact_ledgers_match(
                        recorded=native_record.get("artifacts"),
                        current=artifacts,
                        allow_native_result_authority_identity_projection=True,
                    )
                    or native_state_patch.get("generationRecord") != native_record
                    or native_state_patch.get("generationRecordSha256")
                    != native_record.get("generationRecordSha256")
                ):
                    raise TemporalDiscoveryContractError(
                        "native generation record/state patch disagrees with published artifacts"
                    )
                generation_record = dict(native_record)
                generation_record["nativeGenerationFinalization"] = (
                    _native_production_generation_binding(
                        root=root,
                        generation_index=generation_index,
                        manifest=archive_result["nativeManifest"],
                        commit=archive_result["nativeCommit"],
                    )
                )
            elif native_finalization_validation == NATIVE_FINALIZATION_VALIDATION_HISTORICAL:
                assert native_finalizer_binary is not None
                native_manifest, native_manifest_path = _native_independent_finalizer_manifest(
                    root=root,
                    config=config,
                    generation_index=generation_index,
                    tail_result_indexes=tail_result_indexes,
                    tail_reducer_binary=native_finalizer_binary.with_name(
                        "temporal-qd-tail-reducer.exe"
                        if os.name == "nt"
                        else "temporal-qd-tail-reducer"
                    ),
                    completed_at=str(generation_record["completedAt"]),
                )
                native_execution = _invoke_native_finalizer(
                    binary=native_finalizer_binary,
                    manifest_path=native_manifest_path,
                )
                native_commit = _validate_native_migration_outputs(
                    root=root,
                    generation_record=generation_record,
                    execution=native_execution,
                )
                generation_record["nativeGenerationFinalization"] = (
                    _native_generation_binding(
                        root=root,
                        generation_record=generation_record,
                        manifest=native_manifest,
                        commit=native_commit,
                    )
                )
            completed_generations = list(state.get("completedGenerations") or [])
            completed_generations.append(generation_record)
            _write_v5_lineage_unavailable_marker(
                root=root, generation_index=generation_index, config=config
            )
            candidate_increment = int(generation_result["candidateCount"])
            worker_increment = int(campaign_result["taskCount"]) + int(
                archive_result.get("additionalWorkerTaskCount") or 0
            )
            next_immigrant_ordinal = int(
                generation_result["nextImmigrantContinuationOrdinal"]
            )
            if native_state_patch is not None:
                expected_patch = {
                    "nextGenerationIndex": generation_index + 1,
                    "nextStage": "generation_proposal",
                    "candidateCountIncrement": candidate_increment,
                    "workerTaskCountIncrement": worker_increment,
                    "nextImmigrantContinuationOrdinal": next_immigrant_ordinal,
                }
                if any(
                    native_state_patch.get(key) != value
                    for key, value in expected_patch.items()
                ):
                    raise TemporalDiscoveryContractError(
                        "native state transition differs from supervisor counters"
                    )
                candidate_increment = int(
                    native_state_patch["candidateCountIncrement"]
                )
                worker_increment = int(
                    native_state_patch["workerTaskCountIncrement"]
                )
                next_immigrant_ordinal = int(
                    native_state_patch["nextImmigrantContinuationOrdinal"]
                )
            state.update(
                {
                    "stage": "generation_boundary",
                    "completedGenerations": completed_generations,
                    "currentGenerationIndex": (
                        int(native_state_patch["nextGenerationIndex"])
                        if native_state_patch is not None
                        else generation_index + 1
                    ),
                    "nextImmigrantContinuationOrdinal": next_immigrant_ordinal,
                    "uniqueCandidatesEvaluated": int(
                        state.get("uniqueCandidatesEvaluated") or 0
                    )
                    + candidate_increment,
                    "workerTasksCompleted": int(state.get("workerTasksCompleted") or 0)
                    + worker_increment,
                    "uniqueIdentityCounts": generation_result["uniqueIdentityCounts"],
                    "duplicateCounters": generation_result["duplicateCounters"],
                    "proposalSlotCounters": generation_result["proposalSlotCounters"],
                    "evaluationProgress": None,
                }
            )
            if native_v5_proposal_transaction:
                # The current-v5 branch returns above after applying the Rust
                # sidecar.  Falling through would revive the retired
                # root-ledger copy/promotion path.
                raise TemporalDiscoveryContractError(
                    "current native v5 reached the historical generation boundary"
                )
            elif native_pair_ledger_transaction:
                _promote_native_pair_identity_ledger(
                    root=root,
                    state=state,
                    state_path=state_path,
                    generation_index=generation_index,
                    generation_record=generation_record,
                )
            _save_state(state_path, state)
            if native_v5_proposal_transaction:
                raise TemporalDiscoveryContractError(
                    "current native v5 reached the historical ledger boundary"
                )
            elif native_pair_ledger_transaction:
                assert generation_output_identity_ledger is not None
                assert generation_output_identity_ledger_sha256 is not None
                committed_identity_ledger = generation_output_identity_ledger
                committed_identity_ledger_sha256 = (
                    generation_output_identity_ledger_sha256
                )
            if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
                assert native_finalizer_binary is not None
                _validate_native_generation_binding(
                    generation_record=generation_record,
                    binary=native_finalizer_binary,
                )
                _validate_published_generation_boundary(
                    root=root,
                    state=state,
                    config=config,
                    generation_index=generation_index,
                    tail_result_mode=tail_result_mode,
                    tail_result_indexes=tail_result_indexes,
                )
            elif native_finalization_validation == NATIVE_FINALIZATION_VALIDATION_HISTORICAL:
                assert native_finalizer_binary is not None
                _validate_native_generation_binding(
                    generation_record=generation_record,
                    binary=native_finalizer_binary,
                )
                if native_generation_deep_audit:
                    _validate_published_generation_boundary(
                        root=root,
                        state=state,
                        config=config,
                        generation_index=generation_index,
                        tail_result_mode=tail_result_mode,
                        tail_result_indexes=tail_result_indexes,
                    )
            else:
                _validate_published_generation_boundary(
                    root=root,
                    state=state,
                    config=config,
                    generation_index=generation_index,
                    tail_result_mode=tail_result_mode,
                    tail_result_indexes=tail_result_indexes,
                )
            _event(
                "generation_completed",
                generationIndex=generation_index,
                uniqueCandidatesEvaluated=state["uniqueCandidatesEvaluated"],
                occupiedCellCount=archive_result["occupiedCellCount"],
                newCellCount=archive_result["newCellCount"],
                archiveSha256=archive_result["archiveSha256"],
            )
            completed_by_index[generation_index] = generation_record
            parent_archive_path = archive_path
            parent_archive_sha256 = archive_result["archiveSha256"]
            parent_schedule = archive_result.get("parentSchedule")
            immigrant_cursor = int(
                generation_result["nextImmigrantContinuationOrdinal"]
            )
            # The artifacts and state boundary have been captured.  The next
            # generation has a distinct result matrix, so retaining old
            # compact projections only inflates the long-running supervisor.
            tail_result_indexes.clear()
            if stop_after_generation == generation_index:
                return {
                    "schemaVersion": "temporal_qd_supervisor_result_v3",
                    "status": "paused_at_generation_boundary",
                    "generationIndex": generation_index,
                    "configSha256": config["configSha256"],
                    "stateSha256": state["stateSha256"],
                    "uniqueIdentityCounts": state.get("uniqueIdentityCounts") or {},
                    "duplicateCounters": state.get("duplicateCounters") or {},
                    "runRoot": str(root.resolve()),
                }

        if int(state["uniqueCandidatesEvaluated"]) != int(
            config["generationPlan"]["targetUniqueEvaluations"]
        ):
            raise TemporalDiscoveryContractError(
                "completed supervisor run does not meet its frozen evaluation target"
            )
        if config.get("evidenceLadder") is not None:
            state["stage"] = "evidence_ladder"
            _save_state(state_path, state)
            final_archive = Path(
                completed_by_index[int(config["generationPlan"]["lastGenerationIndex"])][
                    "archivePath"
                ]
            )
            state["evidenceLadderExecution"] = _run_evidence_ladder(
                root=root, config=config, client=client, final_archive_path=final_archive
            )
        state["status"] = "completed"
        state["stage"] = "completed"
        state["completedAt"] = _utc_now()
        _save_state(state_path, state)
        _event(
            "supervisor_completed",
            uniqueCandidatesEvaluated=state["uniqueCandidatesEvaluated"],
            completedGenerationCount=len(state["completedGenerations"]),
        )
        return {
            "schemaVersion": "temporal_qd_supervisor_result_v3",
            "status": "completed",
            "configSha256": config["configSha256"],
            "stateSha256": state["stateSha256"],
            "uniqueCandidatesEvaluated": state["uniqueCandidatesEvaluated"],
            "completedGenerationCount": len(state["completedGenerations"]),
            "uniqueIdentityCounts": state.get("uniqueIdentityCounts") or {},
            "duplicateCounters": state.get("duplicateCounters") or {},
            "proposalSlotCounters": state.get("proposalSlotCounters") or {},
            "runRoot": str(root.resolve()),
        }
    except Exception as exc:
        state["status"] = "stopped_by_tripwire"
        state["stage"] = "failed"
        state["tripwire"] = {
            "exceptionType": type(exc).__name__,
            "message": str(exc),
            "at": _utc_now(),
        }
        _save_state(state_path, state)
        _event(
            "supervisor_tripwire",
            exceptionType=type(exc).__name__,
            message=str(exc),
        )
        raise
    finally:
        if client is not None:
            client.close()


def run_qd_continuation(
    *,
    source_run_root: Path | str,
    run_root: Path | str,
    generation_count: int = LEGACY_CONTINUATION_GENERATION_COUNT,
    **kwargs: Any,
) -> dict[str, Any]:
    """Seed the next immutable four-generation campaign from a completed source.

    The source is reopened and validated on every new-run resume.  The new
    campaign has a distinct root/config/state, so it cannot rewrite source
    generation artifacts even if it is interrupted repeatedly.
    """

    if generation_count != LEGACY_CONTINUATION_GENERATION_COUNT:
        raise TemporalDiscoveryContractError(
            "QD continuation requires exactly four generations"
        )
    binding = _continuation_binding(source_run_root)
    return run_qd_supervisor(
        run_root=run_root,
        initial_archive_path=binding["sourceArchivePath"],
        first_generation_index=int(binding["sourceLastGenerationIndex"]) + 1,
        initial_immigrant_continuation_ordinal=int(
            binding["nextImmigrantContinuationOrdinal"]
        ),
        generation_count=LEGACY_CONTINUATION_GENERATION_COUNT,
        continuation_from=binding,
        **kwargs,
    )


def _ladder_cohort(archive: Mapping[str, Any], *, limit: int) -> list[dict[str, Any]]:
    """Round-robin quality survivors, retaining each cell's Pareto rank order."""

    buckets = [
        [
            _clone(member.get("candidate"), name="QD archive cohort candidate")
            for member in sorted(
                (
                    member
                    for member in cell.get("members") or []
                    if isinstance(member, Mapping)
                    and member.get("archiveLane") == "quality"
                    and _quality_member(member)
                    and isinstance(member.get("candidate"), Mapping)
                ),
                key=_parent_member_order,
            )
        ]
        for cell in sorted(archive.get("cells") or [], key=lambda item: str(item.get("cellId")))
        if isinstance(cell, Mapping)
    ]
    selected: list[dict[str, Any]] = []
    while len(selected) < limit and any(buckets):
        for bucket in buckets:
            if bucket and len(selected) < limit:
                selected.append(bucket.pop(0))
    return selected


def _ladder_population(
    *, candidates: list[dict[str, Any]], template: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, Any]:
    construction = config.get("constructionOperatorPolicy") or {}
    catalog_path = construction.get("catalog", {}).get("path")
    catalog = _read(Path(str(catalog_path)), name="QD ladder construction catalog") if catalog_path else None
    context = qd_predeclared_evidence_context(
        template,
        worker_contract_sha256=config["workerContractSha256"],
        construction_catalog=catalog,
        construction_catalog_path=catalog_path,
    )
    rebound = []
    for candidate in candidates:
        row = _clone(candidate, name="QD ladder candidate")
        row["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(row, context)
        rebound.append(row)
    output = {
        "schemaVersion": QD_POPULATION_SCHEMA,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="QD policy"),
        "generationIndex": 0,
        "targetUniqueCandidates": len(rebound),
        "candidateCount": len(rebound),
        "candidates": sorted(rebound, key=lambda item: str(item["candidateId"])),
        "authoredValidationBindingRequired": True,
        "predeclaredEvidenceContextSha256": context["predeclaredEvidenceContextSha256"],
    }
    output["populationSha256"] = canonical_sha256(output)
    return output


def _run_evidence_ladder(
    *, root: Path, config: Mapping[str, Any], client: LabGatewayClient, final_archive_path: Path
) -> dict[str, Any] | None:
    # A current v5 run may never reconstruct a ladder cohort/population in
    # Python.  The historic implementation below deliberately remains as an
    # explicit legacy/oracle path, but it is not a recovery route for the
    # Rust-native control plane.  Keep this guard before even opening the
    # final archive so a missing native ladder-selection handoff cannot
    # silently turn into an O(N) Python scan on a later restart.
    if _native_v5_proposal_enabled(config):
        raise TemporalDiscoveryContractError(
            "current native v5 evidence-ladder execution requires the Rust "
            "ladder selection/reduction transaction; Python ladder fallback "
            "is prohibited"
        )
    ladder = config.get("evidenceLadder")
    execution = config.get("evidenceLadderExecution")
    if ladder is None:
        return None
    if not isinstance(ladder, Mapping) or not isinstance(execution, Mapping):
        raise TemporalDiscoveryContractError("QD evidence ladder execution binding is missing")
    final_archive = _canonical_file(final_archive_path, name="QD discovery archive")
    ladder_root = root / "evidence-ladder"
    stages: dict[str, Any] = {"schemaVersion": "temporal_qd_evidence_ladder_execution_result_v1", "evidenceLadderSha256": ladder["evidenceLadderSha256"]}
    current_candidates = _ladder_cohort(final_archive, limit=int(ladder["validation"]["maxDiverseSurvivorCount"]))
    if not current_candidates:
        raise TemporalDiscoveryContractError("QD evidence ladder has no diverse discovery survivors for validation")
    for stage, limit in (("validation", int(ladder["validation"]["maxDiverseSurvivorCount"])), ("scrutiny", int(ladder["scrutiny"]["maxFinalistCount"]))):
        template_binding = execution[stage + "Template"]
        template_path = Path(str(template_binding.get("path") or ""))
        template = _canonical_file(template_path, name=f"QD {stage} template")
        if canonical_sha256(template) != template_binding.get("sha256"):
            raise TemporalDiscoveryContractError(f"QD {stage} template drifted")
        validate_template_stage_window(template, ladder, stage=stage)
        candidates = current_candidates[:limit]
        population = _ladder_population(candidates=candidates, template=template, config=config)
        stage_root = ladder_root / stage
        population_path = stage_root / "population.json"
        _write_once(population_path, population)
        campaign = freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=stage_root / "campaign",
            execution_engine_commit=config["repositories"]["executionEngineCommit"],
            worker_contract_sha256=config["workerContractSha256"],
            construction_catalog_path=(config.get("constructionOperatorPolicy") or {}).get("catalog", {}).get("path"),
        )
        authority = _canonical_file(stage_root / "campaign" / "authority.json", name=f"QD {stage} authority")
        result_root = stage_root / "campaign" / "screening-run"
        result = run_temporal_search_tasks(client, authority, output_root=result_root, timeout_seconds=float(config["evaluation"]["timeoutSecondsPerGeneration"]), resume=True, enqueue_batch_size=int(config["evaluation"]["enqueueBatchSize"]))
        if result["completedTaskCount"] != campaign["taskCount"]:
            raise TemporalDiscoveryContractError(f"QD {stage} evaluation did not complete")
        archive_path = stage_root / "archive.json"
        archive_result = build_qd_archive(population_path=population_path, result_root=result_root, output_path=archive_path, generation_index=0, cell_capacity=int(config["frozenSearchPolicy"]["cellCapacity"]), minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]), minimum_trades_per_window=int(config["frozenSearchPolicy"]["minimumTradesPerWindow"]), cap_trades=int(config["frozenSearchPolicy"]["capTrades"]))
        artifacts = _capture_screening_artifacts(
            population_path=population_path,
            archive_path=archive_path,
            campaign_root=stage_root / "campaign",
            generation_index=0,
            label=f"QD {stage} ladder",
        )
        stages[stage] = {
            "candidateCount": len(candidates),
            "populationPath": str(population_path.resolve()),
            "populationSha256": population["populationSha256"],
            "campaignPath": str((stage_root / "campaign" / "campaign.json").resolve()),
            "campaignSha256": campaign["campaignSha256"],
            "archivePath": str(archive_path.resolve()),
            "archiveSha256": archive_result["archiveSha256"],
            "artifacts": artifacts,
        }
        current_candidates = _ladder_cohort(_canonical_file(archive_path, name=f"QD {stage} archive"), limit=(int(ladder["scrutiny"]["maxFinalistCount"]) if stage == "validation" else limit))
        if stage == "validation" and not current_candidates:
            raise TemporalDiscoveryContractError("QD evidence ladder has no validation finalists for scrutiny")
    stages["outerTail"] = _clone(ladder["outerTail"], name="QD outer tail")
    stages["executionSha256"] = canonical_sha256(stages)
    _write_once(ladder_root / "execution.json", stages)
    return stages


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--initial-archive", type=Path)
    parser.add_argument(
        "--continue-from",
        type=Path,
        help=(
            "completed immutable fresh-five or legacy-four run root; creates "
            "the next separate contiguous four-generation run"
        ),
    )
    parser.add_argument("--source-preparation", type=Path)
    parser.add_argument("--base-generator-root", type=Path)
    parser.add_argument("--confirmed-entry-admission-root", type=Path)
    parser.add_argument("--template-preparation", type=Path, required=True)
    parser.add_argument("--validator-command-file", type=Path)
    parser.add_argument("--parameters", type=Path, required=True)
    parser.add_argument(
        "--construction-catalog",
        type=Path,
        required=True,
        help="canonical Stage 5E7-v3 construction catalog snapshot",
    )
    parser.add_argument("--generation-count", type=int, required=True)
    parser.add_argument("--initial-construction-pool-size", type=int, default=4000)
    parser.add_argument("--evaluation-population-size", type=int, default=1024)
    parser.add_argument("--first-generation-index", type=int, default=1)
    parser.add_argument("--initial-immigrant-continuation-ordinal", type=int, default=0)
    parser.add_argument("--autoresearch-commit", required=True)
    parser.add_argument("--execution-engine-commit", required=True)
    parser.add_argument("--worker-contract-sha256", required=True)
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8799")
    parser.add_argument("--gateway-token")
    parser.add_argument("--evaluation-timeout-seconds", type=float, default=86_400.0)
    parser.add_argument("--enqueue-batch-size", type=int, default=128)
    parser.add_argument("--broad-admission", action="store_true")
    parser.add_argument("--generation-funnel-enabled", action="store_true")
    parser.add_argument(
        "--tail-result-mode",
        choices=sorted(_TAIL_RESULT_MODES),
        default=TAIL_RESULT_MODE_LEGACY,
        help=(
            "operational rotating-tail reducer: legacy reopens raw results; "
            "indexed reuses one source-verified in-memory projection per campaign"
        ),
    )
    parser.add_argument(
        "--native-finalization-validation",
        choices=sorted(_NATIVE_FINALIZATION_VALIDATION_MODES),
        default=NATIVE_FINALIZATION_VALIDATION_NONE,
        help=(
            "legacy compatibility flag; fresh historical admission is disabled and "
            "only 'none' is accepted"
        ),
    )
    parser.add_argument(
        "--generation-finalization-engine",
        choices=sorted(_GENERATION_FINALIZATION_ENGINES),
        default=GENERATION_FINALIZATION_ENGINE_DEFAULT,
        help=(
            "generation boundary materializer; Rust is the fail-closed default. "
            "Python is an explicit legacy/oracle mode and is forbidden for fresh "
            "or current v5 runs"
        ),
    )
    parser.add_argument(
        "--generation-finalizer-binary",
        type=Path,
        help="prebuilt temporal-qd-generation-finalizer binary; required for Rust finalization",
    )
    parser.add_argument(
        "--native-generation-deep-audit",
        action="store_true",
        help="reopen rich generation artifacts even after compact native admission",
    )
    parser.add_argument(
        "--adopt-python-completed-generation",
        dest="adopt_python_completed_generations",
        action="append",
        type=int,
        default=[],
        help=(
            "explicit one-time Rust cutover authority for an already-completed "
            "Python generation; repeat for every unbound completed generation"
        ),
    )
    parser.add_argument(
        "--authorize-native-finalization-authority-rotation",
        action="store_true",
        help=(
            "explicitly activate a successor set of native finalization binaries "
            "at an unpublished generation while preserving prior authority epochs"
        ),
    )
    parser.add_argument(
        "--stop-before-evaluation-generation",
        type=int,
        help="freeze the named generation campaign and stop before enqueueing worker tasks",
    )
    parser.add_argument(
        "--evidence-ladder-config",
        type=Path,
        help="closed temporal_qd_evidence_ladder_input_v1 JSON; enables frozen 3m/12m/36m evidence gates",
    )
    parser.add_argument(
        "--rotating-evidence-config",
        type=Path,
        help="closed temporal_qd_rotating_evidence_input_v1 JSON; enables the rotating cumulative breeder transaction",
    )
    parser.add_argument("--stop-after-generation", type=int)
    parser.add_argument("--bidirectional-pair-config", type=Path, help="closed temporal_qd_bidirectional_pair_run_config_v1 JSON; opt-in only")
    parser.add_argument(
        "--evolvable-module-authority-config",
        type=Path,
        help=(
            "fresh closed temporal_qd_evolvable_module_authority_v1 JSON; requires "
            "the Rust-native v5 proposal transaction and Rust generation finalization"
        ),
    )
    parser.add_argument(
        "--pair-generation-engine",
        choices=(PAIR_GENERATION_RUNTIME_PYTHON, PAIR_GENERATION_RUNTIME_RUST),
        default=PAIR_GENERATION_RUNTIME_DEFAULT,
        help="frozen pair-generation engine; Rust is the admitted default and forbids fallback; Python remains the explicit oracle",
    )
    parser.add_argument(
        "--pair-generation-timeout-seconds",
        type=int,
        default=3600,
        help="frozen whole-process timeout for one native pair generation",
    )
    args = parser.parse_args()
    if args.bidirectional_pair_config is None and any(value is None for value in (args.source_preparation, args.base_generator_root, args.confirmed_entry_admission_root, args.validator_command_file)):
        parser.error("legacy mode requires --source-preparation, --base-generator-root, --confirmed-entry-admission-root, and --validator-command-file")
    parameters = _read(args.parameters, name="QD supervisor parameters")
    if args.continue_from is not None:
        if args.initial_archive is not None:
            parser.error("--continue-from derives the source campaign's immutable final archive; do not also pass --initial-archive")
        result = run_qd_continuation(
            source_run_root=args.continue_from,
            run_root=args.run_root,
            generation_count=args.generation_count,
            source_preparation_path=args.source_preparation,
            base_generator_root=args.base_generator_root,
            confirmed_entry_admission_root=args.confirmed_entry_admission_root,
            template_preparation_path=args.template_preparation,
            validator_command_file=args.validator_command_file,
            parameters=parameters,
            autoresearch_commit=args.autoresearch_commit,
            execution_engine_commit=args.execution_engine_commit,
            worker_contract_sha256=args.worker_contract_sha256,
            gateway_url=args.gateway_url,
            gateway_token=args.gateway_token or load_lab_gateway_token(create=False),
            evaluation_timeout_seconds=args.evaluation_timeout_seconds,
            enqueue_batch_size=args.enqueue_batch_size,
            broad_admission=args.broad_admission,
            stop_after_generation=args.stop_after_generation,
            construction_catalog_path=args.construction_catalog,
            generation_funnel_enabled=args.generation_funnel_enabled,
            bidirectional_pair_config=(
                _read(args.bidirectional_pair_config, name="bidirectional pair run config")
                if args.bidirectional_pair_config is not None else None
            ),
            pair_generation_engine=(
                args.pair_generation_engine
                if args.bidirectional_pair_config is not None
                else None
            ),
            pair_generation_timeout_seconds=args.pair_generation_timeout_seconds,
            evidence_ladder_config=(
                _read(args.evidence_ladder_config, name="QD evidence ladder config")
                if args.evidence_ladder_config is not None else None
            ),
            rotating_evidence_config=(
                _read(
                    args.rotating_evidence_config,
                    name="QD rotating evidence config",
                )
                if args.rotating_evidence_config is not None
                else None
            ),
            initial_construction_pool_size=args.initial_construction_pool_size,
            evaluation_population_size=args.evaluation_population_size,
            tail_result_mode=args.tail_result_mode,
            native_finalization_validation=args.native_finalization_validation,
            generation_finalization_engine=args.generation_finalization_engine,
            generation_finalizer_binary=args.generation_finalizer_binary,
            native_generation_deep_audit=args.native_generation_deep_audit,
            adopt_python_completed_generations=tuple(
                args.adopt_python_completed_generations
            ),
            authorize_native_finalization_authority_rotation=(
                args.authorize_native_finalization_authority_rotation
            ),
            stop_before_evaluation_generation=args.stop_before_evaluation_generation,
            evolvable_module_authority_config=(
                _read(
                    args.evolvable_module_authority_config,
                    name="evolvable module authority config",
                )
                if args.evolvable_module_authority_config is not None
                else None
            ),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    if args.initial_archive is None:
        parser.error("--initial-archive is required unless --continue-from is used")
    result = run_qd_supervisor(
        run_root=args.run_root,
        initial_archive_path=args.initial_archive,
        source_preparation_path=args.source_preparation,
        base_generator_root=args.base_generator_root,
        confirmed_entry_admission_root=args.confirmed_entry_admission_root,
        template_preparation_path=args.template_preparation,
        validator_command_file=args.validator_command_file,
        parameters=parameters,
        generation_count=args.generation_count,
        first_generation_index=args.first_generation_index,
        initial_immigrant_continuation_ordinal=args.initial_immigrant_continuation_ordinal,
        autoresearch_commit=args.autoresearch_commit,
        execution_engine_commit=args.execution_engine_commit,
        worker_contract_sha256=args.worker_contract_sha256,
        gateway_url=args.gateway_url,
        gateway_token=args.gateway_token or load_lab_gateway_token(create=False),
        evaluation_timeout_seconds=args.evaluation_timeout_seconds,
        enqueue_batch_size=args.enqueue_batch_size,
        broad_admission=args.broad_admission,
        stop_after_generation=args.stop_after_generation,
        construction_catalog_path=args.construction_catalog,
        generation_funnel_enabled=args.generation_funnel_enabled,
        bidirectional_pair_config=(
            _read(args.bidirectional_pair_config, name="bidirectional pair run config")
            if args.bidirectional_pair_config is not None else None
        ),
        pair_generation_engine=(
            args.pair_generation_engine
            if args.bidirectional_pair_config is not None
            else None
        ),
        pair_generation_timeout_seconds=args.pair_generation_timeout_seconds,
        evidence_ladder_config=(
            _read(args.evidence_ladder_config, name="QD evidence ladder config")
            if args.evidence_ladder_config is not None
            else None
        ),
        rotating_evidence_config=(
            _read(
                args.rotating_evidence_config,
                name="QD rotating evidence config",
            )
            if args.rotating_evidence_config is not None
            else None
        ),
        initial_construction_pool_size=args.initial_construction_pool_size,
        evaluation_population_size=args.evaluation_population_size,
        tail_result_mode=args.tail_result_mode,
        native_finalization_validation=args.native_finalization_validation,
        generation_finalization_engine=args.generation_finalization_engine,
        generation_finalizer_binary=args.generation_finalizer_binary,
        native_generation_deep_audit=args.native_generation_deep_audit,
        adopt_python_completed_generations=tuple(
            args.adopt_python_completed_generations
        ),
        authorize_native_finalization_authority_rotation=(
            args.authorize_native_finalization_authority_rotation
        ),
        stop_before_evaluation_generation=args.stop_before_evaluation_generation,
        evolvable_module_authority_config=(
            _read(
                args.evolvable_module_authority_config,
                name="evolvable module authority config",
            )
            if args.evolvable_module_authority_config is not None
            else None
        ),
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


__all__ = ["run_qd_continuation", "run_qd_supervisor"]
