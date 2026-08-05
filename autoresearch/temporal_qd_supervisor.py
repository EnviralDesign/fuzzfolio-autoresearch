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
import json
import os
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
from .temporal_generation_funnel import (
    GenerationFunnelContractError,
    supervisor_funnel_snapshot,
    write_generation_funnel_artifact,
)
from .temporal_search import run_temporal_search_tasks
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
    *, campaign_root: Path, population_path: Path | None = None
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
        result_root=result_root, checkpoint=checkpoint, task_manifest=manifest
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
    *, root: Path, generation_index: int, generation_funnel_enabled: bool = False
) -> dict[str, Any]:
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    population_path = proposal_root / "population.json"
    journal_path = proposal_root / "generation-journal.json"
    archive_path = generation_root / "archive.json"
    output = _capture_screening_artifacts(
        population_path=population_path,
        archive_path=archive_path,
        campaign_root=campaign_root,
        generation_index=generation_index,
        label="QD generation",
        generation_journal_path=journal_path,
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


def _validate_generation_artifacts(
    *, root: Path, generation_record: Mapping[str, Any], config: Mapping[str, Any]
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
    current = _capture_generation_artifacts(
        root=root,
        generation_index=generation_index,
        generation_funnel_enabled=funnel_enabled,
    )
    if _clone(current, name="completed generation artifacts") != _clone(
        recorded, name="recorded completed generation artifacts"
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


def _validate_completed_generations(
    *, root: Path, state: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[int, dict[str, Any]]:
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
        _validate_generation_artifacts(
            root=root, generation_record=record, config=config
        )
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
    archive, archive_sha = _load_archive(Path(str(archive_binding.get("path") or "")))
    if archive_sha != archive_binding.get("archiveSha256") or (
        archive.get("resultSetSha256") != archive_binding.get("resultSetSha256")
    ):
        raise TemporalDiscoveryContractError("QD supervisor initial archive drifted")

    pair_config = config.get("bidirectionalPairGeneration")
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
        with PairAuthorityBundle(_clone(pair_config, name="frozen pair authority")):
            pass
        if config.get("broadAdmission") is True:
            contract = config.get("broadAdmissionContract")
            if not isinstance(contract, Mapping):
                raise TemporalDiscoveryContractError(
                    "QD broad admission contract is unavailable"
                )
            required_unique_candidates = _require_frozen_immigrant_capacity_requirement(
                config, contract
            )
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
    evidence_ladder_config: Mapping[str, Any] | None = None,
    rotating_evidence_config: Mapping[str, Any] | None = None,
    continuation_from: Mapping[str, Any] | None = None,
    initial_construction_pool_size: int | None = None,
    evaluation_population_size: int | None = None,
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
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="frozen QD policy"),
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
                            "immigrantConstructionCapacity": pair_capacity_audit,
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
            "policySha256": QD_POLICY_SHA256,
            "canonicalEvidenceIdentity": QD_POLICY["identity"]["canonicalEvidence"],
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
        },
        **({"continuationFrom": _clone(continuation_from, name="QD continuation binding")} if continuation_from is not None else {}),
        **({
            "immigrantSource": {
                "sourcePreparationPath": str(source_preparation_path.resolve()), "baseGeneratorRoot": str(base_generator_root.resolve()),
                "confirmedEntryAdmissionRoot": str(confirmed_entry_admission_root.resolve()), "sourceIdentity": source.source_identity,
                "initialContinuationOrdinal": initial_immigrant_continuation_ordinal,
            }
        } if source is not None else {"bidirectionalPairGeneration": pair_authority}),
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
    )
    authority = _canonical_file(campaign_root / "authority.json", name="rotating authority")
    evaluation = run_temporal_search_tasks(
        client,
        authority,
        output_root=campaign_root / "screening-run",
        timeout_seconds=float(config["evaluation"]["timeoutSecondsPerGeneration"]),
        resume=True,
        enqueue_batch_size=int(config["evaluation"]["enqueueBatchSize"]),
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


def _complete_rotating_generation_transaction(
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
) -> dict[str, Any]:
    """Complete one atomic proposal/reevaluation/backfill/archive transaction."""

    _validate_frozen_sources(config)
    contract = validate_rotating_evidence_contract(config["rotatingEvidence"])
    panel = panel_for_generation(contract, generation_index)
    evidence_root = generation_root / "evidence"
    cohort_path = evidence_root / "cohort.json"
    checkpoint_path = evidence_root / "checkpoint.json"
    provisional_path = evidence_root / "provisional.json"
    cumulative_path = evidence_root / "cumulative-archive.json"
    ledger_path = evidence_root / "generation-ledger.json"

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

    member_batches = [
        load_qd_evaluated_members(
            population_path=proposal_root / "population.json",
            result_root=proposal_campaign_root / "screening-run",
            generation_index=generation_index,
            generation_journal_path=proposal_root / "generation-journal.json",
            minimum_total_trades=int(config["frozenSearchPolicy"]["minimumTotalTrades"]),
            minimum_trades_per_window=int(
                config["frozenSearchPolicy"]["minimumTradesPerWindow"]
            ),
            cap_trades=int(config["frozenSearchPolicy"]["capTrades"]),
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
    new_records = _campaign_window_evidence(
        campaign_root=proposal_campaign_root,
        panel=panel,
        candidates=new_candidates,
    )
    current_records.update(new_records)
    if parent_campaign_root is not None:
        current_records.update(
            _campaign_window_evidence(
                campaign_root=parent_campaign_root,
                panel=panel,
                candidates=parent_candidates,
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
    )
    _write_once(cumulative_path, cumulative)
    archive_result = build_rotating_qd_parent_archive(
        current_members=list(current_members.values()),
        cumulative_archive=cumulative,
        output_path=archive_path,
        generation_index=generation_index,
        previous_archive_path=parent_archive_path,
        bidirectional_pair_policy=(
            pair_policy_from_config(config["bidirectionalPairGeneration"])
            if config.get("bidirectionalPairGeneration") is not None
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
    evidence_ladder_config: Mapping[str, Any] | None = None,
    rotating_evidence_config: Mapping[str, Any] | None = None,
    continuation_from: Mapping[str, Any] | None = None,
    initial_construction_pool_size: int | None = None,
    evaluation_population_size: int | None = None,
) -> dict[str, Any]:
    root = Path(run_root)
    root.mkdir(parents=True, exist_ok=True)
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
        evidence_ladder_config=evidence_ladder_config,
        rotating_evidence_config=rotating_evidence_config,
        continuation_from=continuation_from,
        initial_construction_pool_size=initial_construction_pool_size,
        evaluation_population_size=evaluation_population_size,
    )
    config_path = root / "config.json"
    state_path = root / "state.json"
    _write_once(config_path, config)
    if config.get("evidenceLadder") is not None:
        _write_once(root / "evidence-ladder.json", config["evidenceLadder"])
    if config.get("rotatingEvidence") is not None:
        _write_once(root / "rotating-evidence.json", config["rotatingEvidence"])
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
    # This is deliberately before both the completed fast path and gateway
    # construction.  A restart must never treat a stale source, or a merely
    # self-claimed completed state, as permission to skip immutable work.
    validator_command = _validate_frozen_sources(config)
    completed_by_index = _validate_completed_generations(
        root=root, state=state, config=config
    )
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
        immigrant_cursor = int(initial_immigrant_continuation_ordinal)
        if completed_by_index:
            latest = max(completed_by_index)
            if set(completed_by_index) != set(range(first, latest + 1)):
                raise TemporalDiscoveryContractError(
                    "completed QD generations are not contiguous"
                )
            parent_archive_path = Path(completed_by_index[latest]["archivePath"])
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
            # Do not let a file-backed source change while an earlier
            # generation is running and then silently feed a later phase.
            validator_command = _validate_frozen_sources(config)
            generation_kwargs = dict(
                parent_archive_path=parent_archive_path,
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
                with PairAuthorityBundle(config["bidirectionalPairGeneration"]) as pair_authority:
                    g0 = config.get("g0Bootstrap")
                    generation_result = generate_qd_generation(
                        **generation_kwargs,
                        bidirectional_pair_policy=pair_policy_from_config(config["bidirectionalPairGeneration"]),
                        bidirectional_pair_factory=pair_authority.factory,
                        bidirectional_module_authority=pair_authority.operator,
                        bidirectional_native_validator=pair_authority.validator,
                        bidirectional_pair_compiler=pair_authority.compiler,
                        bidirectional_operator_implementation_identity=config["bidirectionalPairGeneration"]["operatorImplementation"],
                        initial_construction_pool_size=(int(g0["initialConstructionPoolSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                        evaluation_population_size=(int(g0["evaluationPopulationSize"]) if isinstance(g0, Mapping) and generation_index == 1 else None),
                    )
            if generation_result.get("completed") is not True:
                raise TemporalDiscoveryContractError(
                    "QD generation proposal manifest did not complete"
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
                or evaluation_identity.get("policySha256") != QD_POLICY_SHA256
                or (config.get("rotatingEvidence") is None and evaluation_identity.get("predeclaredEvidenceContextSha256")
                != config["evaluation"]["predeclaredEvidenceContextSha256"])
            ):
                raise TemporalDiscoveryContractError(
                    "frozen QD evaluation identity drifted from supervisor config"
                )

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
                archive_result = _complete_rotating_generation_transaction(
                    root=root,
                    generation_root=generation_root,
                    generation_index=generation_index,
                    proposal_root=proposal_root,
                    proposal_campaign_root=campaign_root,
                    parent_archive_path=parent_archive_path,
                    archive_path=archive_path,
                    config=config,
                    client=client,
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
                )
            funnel_enabled = bool((config.get("generationFunnel") or {}).get("enabled"))
            if funnel_enabled:
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
                **({"g0Bootstrap": generation_result["g0Bootstrap"]} if generation_result.get("g0Bootstrap") is not None else {}),
                **({"constructionPoolSize": generation_result["constructionPoolSize"], "constructedAcceptedCount": generation_result["constructedAcceptedCount"]} if generation_result.get("g0Bootstrap") is not None else {}),
                "originProposalCounts": generation_result["originProposalCounts"],
                "originAcceptedCounts": generation_result["originAcceptedCounts"],
                "campaignSha256": campaign_result["campaignSha256"],
                "evaluationIdentitySha256": campaign_result["evaluationIdentitySha256"],
                "taskMatrixSha256": campaign_result["taskMatrixSha256"],
                "taskCount": campaign_result["taskCount"],
                "totalGenerationTaskCount": int(campaign_result["taskCount"])
                + int(archive_result.get("additionalWorkerTaskCount") or 0),
                "archiveSha256": archive_result["archiveSha256"],
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
            completed_generations = list(state.get("completedGenerations") or [])
            completed_generations.append(generation_record)
            state.update(
                {
                    "stage": "generation_boundary",
                    "completedGenerations": completed_generations,
                    "currentGenerationIndex": generation_index + 1,
                    "nextImmigrantContinuationOrdinal": generation_result[
                        "nextImmigrantContinuationOrdinal"
                    ],
                    "uniqueCandidatesEvaluated": int(
                        state.get("uniqueCandidatesEvaluated") or 0
                    )
                    + int(generation_result["candidateCount"]),
                    "workerTasksCompleted": int(state.get("workerTasksCompleted") or 0)
                    + int(campaign_result["taskCount"])
                    + int(archive_result.get("additionalWorkerTaskCount") or 0),
                    "uniqueIdentityCounts": generation_result["uniqueIdentityCounts"],
                    "duplicateCounters": generation_result["duplicateCounters"],
                    "proposalSlotCounters": generation_result["proposalSlotCounters"],
                    "evaluationProgress": None,
                }
            )
            _save_state(state_path, state)
            _validate_completed_generations(root=root, state=state, config=config)
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
            immigrant_cursor = int(
                generation_result["nextImmigrantContinuationOrdinal"]
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
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


__all__ = ["run_qd_continuation", "run_qd_supervisor"]
