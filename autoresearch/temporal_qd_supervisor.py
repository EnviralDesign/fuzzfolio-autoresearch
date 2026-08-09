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
from .temporal_qd_pair_generation import _rotating_parent_schedule
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
    PAIR_GENERATION_RUNTIME_DEFAULT,
    PAIR_GENERATION_RUNTIME_PYTHON,
    PAIR_GENERATION_RUNTIME_RUST,
    TemporalQDNativeError,
    build_pair_generation_runtime_config,
    validate_generation_manifest,
    validate_generation_result,
    validate_pair_generation_runtime_config,
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
from .result_codec import ResultCodecError, read_json_object

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
GENERATION_FINALIZATION_ENGINE_PYTHON = "python"
GENERATION_FINALIZATION_ENGINE_RUST = "rust"
_GENERATION_FINALIZATION_ENGINES = frozenset(
    {GENERATION_FINALIZATION_ENGINE_PYTHON, GENERATION_FINALIZATION_ENGINE_RUST}
)


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
            and isinstance(record.get("nativeGenerationFinalization"), Mapping)
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
    current = _capture_generation_artifacts(
        root=root,
        generation_index=generation_index,
        generation_funnel_enabled=funnel_enabled,
        tail_result_mode=tail_result_mode,
        tail_result_indexes=tail_result_indexes,
        verify_population_file=not native_production,
        verify_rotating_campaign_artifacts=not native_production,
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
    if expected_g0 is not None:
        current_g0 = current.get("g0Bootstrap")
        recorded_g0 = generation_record.get("g0Bootstrap")
        if not isinstance(current_g0, Mapping) or current_g0 != recorded_g0:
            raise TemporalDiscoveryContractError("completed G0 bootstrap identities disagree with immutable outputs")
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


def _native_finalization_runtime_authority(finalizer_binary: Path) -> dict[str, Any]:
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
) -> dict[str, Any]:
    authority = _native_finalization_runtime_authority(finalizer_binary)
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


def _validate_frozen_sources(config: Mapping[str, Any]) -> list[str]:
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
    if native_pair_generation:
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
    initial_archive, initial_archive_sha = _load_archive(initial_archive_path)
    initial_parent_schedule = _rotating_parent_schedule(initial_archive)
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
    pair_authority = load_pair_run_config(bidirectional_pair_config) if bidirectional_pair_config is not None else None
    pair_source_authority = (
        _clone(pair_authority, name="base pair source authority")
        if pair_authority is not None
        else None
    )
    evolvable_authority = (
        _clone(
            evolvable_module_authority_config,
            name="evolvable module authority config",
        )
        if evolvable_module_authority_config is not None
        else None
    )
    evolvable_capacity_receipt: Mapping[str, Any] | None = None
    if evolvable_authority is not None:
        if pair_authority is None:
            raise TemporalDiscoveryContractError(
                "evolvable module authority requires bidirectional pair mode"
            )
        policy_authority = evolvable_authority.get("archivePolicyAuthority")
        if not isinstance(policy_authority, Mapping):
            raise TemporalDiscoveryContractError(
                "evolvable module authority lacks archive policy authority"
            )
        _, _, _, direction_aware = _resolve_archive_policy_authority(policy_authority)
        if not direction_aware:
            raise TemporalDiscoveryContractError(
                "evolvable module authority requires the exact v5 archive policy"
            )
        if not isinstance(evolvable_authority.get("behaviorAttributionRequirement"), Mapping):
            raise TemporalDiscoveryContractError(
                "evolvable module authority lacks behavior attribution requirement"
            )
        # Freeze the authority-authored operator identity before hashing the
        # supervisor config.  The generation runtime reopens and verifies this
        # same material; it must never inherit the legacy pair operator label.
        with PairAuthorityBundle(pair_authority) as _pair_bundle:
            _evolvable = _pair_bundle.open_evolvable_module_authority(
                evolvable_authority
            )
            _binding_input = _clone(pair_authority, name="base pair generation config")
            # The base pair config's v4 operator identity names the legacy
            # factory.  It remains the source authority, but cannot be passed
            # through as the v5 generation operator identity.
            _binding_input.pop("operatorImplementation", None)
            _bindings = _evolvable.generation_bindings(_binding_input)
        pair_authority = _bindings["runConfig"]
        evolvable_capacity_receipt = _bindings["capacityReceipt"]
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
    if evolvable_authority is not None and pair_generation_runtime["engine"] != PAIR_GENERATION_RUNTIME_PYTHON:
        raise TemporalDiscoveryContractError(
            "evolvable module authority currently requires the explicit Python pair-generation oracle"
        )
    archive_policy_authority = (
        evolvable_authority["archivePolicyAuthority"]
        if evolvable_authority is not None
        else None
    )
    policy_name, policy_sha256, frozen_policy, direction_aware = (
        _resolve_archive_policy_authority(archive_policy_authority)
    )
    if (
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
                    initial_parent_count=_archive_member_count(initial_archive),
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
                            initial_parent_count=_archive_member_count(initial_archive),
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
            "generationIndex": int(initial_archive["generationIndex"]),
            "resultSetSha256": initial_archive["resultSetSha256"],
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
    _validate_frozen_sources(config)
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
    _validate_frozen_sources(config)
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
    new_records = _campaign_window_evidence(
        campaign_root=proposal_campaign_root,
        panel=panel,
        candidates=evaluated_new_candidates,
        tail_result_index=proposal_tail_index,
        direction_aware=config.get("evolvableModuleAuthority") is not None,
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
    """Python-oracle materialization entry point retained as the default."""

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
    generation_finalization_engine: str = GENERATION_FINALIZATION_ENGINE_PYTHON,
    generation_finalizer_binary: Path | str | None = None,
    native_generation_deep_audit: bool = False,
    adopt_python_completed_generations: tuple[int, ...] = (),
    authorize_native_finalization_authority_rotation: bool = False,
    stop_before_evaluation_generation: int | None = None,
    evolvable_module_authority_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    tail_result_mode = _normalize_tail_result_mode(tail_result_mode)
    if (
        evolvable_module_authority_config is not None
        and tail_result_mode == TAIL_RESULT_MODE_INDEXED
    ):
        raise TemporalDiscoveryContractError(
            "evolvable v5 authority requires raw rotating result provenance until direction-aware tail indexing is versioned"
        )
    native_finalization_validation = _normalize_native_finalization_validation(
        native_finalization_validation
    )
    generation_finalization_engine = _normalize_generation_finalization_engine(
        generation_finalization_engine
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
    root = Path(run_root)
    root.mkdir(parents=True, exist_ok=True)
    _require_irreversible_native_cutover_engine(
        root=root,
        generation_finalization_engine=generation_finalization_engine,
    )
    initial_archive_file = Path(initial_archive_path)
    # Pair mode never constructs a v2 continuation.  These placeholders are
    # deliberately not persisted or opened in that mode.
    source_preparation_file = Path(source_preparation_path) if source_preparation_path is not None else root / ".pair-mode-unused-source.json"
    base_generator_dir = Path(base_generator_root) if base_generator_root is not None else root / ".pair-mode-unused-generator"
    confirmed_entry_dir = Path(confirmed_entry_admission_root) if confirmed_entry_admission_root is not None else root / ".pair-mode-unused-admission"
    template_preparation_file = Path(template_preparation_path)
    validator_file = Path(validator_command_file) if validator_command_file is not None else None
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
    config_path = root / "config.json"
    state_path = root / "state.json"
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
    adoption_authority: Mapping[str, Any] | None = None
    if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
        assert native_finalizer_binary is not None
        adoption_authority = _prepare_native_finalization_adoption_authority(
            root=root,
            state=state,
            config=config,
            finalizer_binary=native_finalizer_binary,
            requested_generations=adopt_python_completed_generations,
        )
        _freeze_native_finalization_runtime_authority(
            root=root,
            finalizer_binary=native_finalizer_binary,
            state=state,
            authorized_adoption_generations=frozenset(
                (adoption_authority or {}).get("generationIndices") or []
            ),
            authorize_rotation=authorize_native_finalization_authority_rotation,
        )
    # This is deliberately before both the completed fast path and gateway
    # construction.  A restart must never treat a stale source, or a merely
    # self-claimed completed state, as permission to skip immutable work.
    validator_command = _validate_frozen_sources(config)
    if generation_finalization_engine == GENERATION_FINALIZATION_ENGINE_RUST:
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
    native_pair_ledger_transaction = (
        (config.get("pairGenerationRuntime") or {}).get("engine")
        == PAIR_GENERATION_RUNTIME_RUST
    )
    committed_identity_ledger: dict[str, Any] | None = None
    committed_identity_ledger_sha256: str | None = None
    if native_pair_ledger_transaction:
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
        parent_schedule = config["initialArchive"].get("parentSchedule")
        immigrant_cursor = int(initial_immigrant_continuation_ordinal)
        if completed_by_index:
            latest = max(completed_by_index)
            if set(completed_by_index) != set(range(first, latest + 1)):
                raise TemporalDiscoveryContractError(
                    "completed QD generations are not contiguous"
                )
            parent_archive_path = Path(completed_by_index[latest]["archivePath"])
            parent_archive_sha256 = completed_by_index[latest]["archiveSha256"]
            parent_schedule = completed_by_index[latest].get("parentSchedule")
            immigrant_cursor = int(
                completed_by_index[latest]["nextImmigrantContinuationOrdinal"]
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
                template_payload = _read(generation_template_file, name="rotating QD panel template")
                validate_generation_template(template_payload, config["rotatingEvidence"], generation_index)

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
            if native_pair_ledger_transaction:
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
            validator_command = _validate_frozen_sources(config)
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
            if config.get("bidirectionalPairGeneration") is None:
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

            state["stage"] = "freezing_evaluation"
            _save_state(state_path, state)
            validator_command = _validate_frozen_sources(config)
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
                ),
                verify_rotating_campaign_artifacts=(
                    generation_finalization_engine
                    != GENERATION_FINALIZATION_ENGINE_RUST
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
            journal = _read(
                proposal_root / "generation-journal.json",
                name="QD generation journal",
            )
            journal_sha = _identity_payload(
                journal, "journalSha256", name="QD generation journal"
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
            if native_pair_ledger_transaction:
                _promote_native_pair_identity_ledger(
                    root=root,
                    state=state,
                    state_path=state_path,
                    generation_index=generation_index,
                    generation_record=generation_record,
                )
            _save_state(state_path, state)
            if native_pair_ledger_transaction:
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
        default=GENERATION_FINALIZATION_ENGINE_PYTHON,
        help=(
            "generation boundary materializer; Python remains the default oracle, "
            "Rust is an explicit fail-closed opt-in and becomes mandatory for "
            "every restart after native cutover"
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
        help="fresh closed temporal_qd_evolvable_module_authority_v1 JSON; requires v5 archive and Python pair oracle",
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
