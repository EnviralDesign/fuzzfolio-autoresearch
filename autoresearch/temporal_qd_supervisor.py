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
from .temporal_qd_campaign import freeze_qd_screening_campaign
from .temporal_qd_evolution import (
    QD_IDENTITY_LEDGER_SCHEMA,
    QD_POLICY,
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_VERSION,
    _identity_payload,
    _load_archive,
    _normalize_parameters,
    _read,
    build_qd_archive,
    generate_qd_generation,
    qd_construction_operator_policy,
    qd_predeclared_evidence_context,
)
from .temporal_search import run_temporal_search_tasks
from .result_codec import ResultCodecError, read_json_object

SUPERVISOR_VERSION = "temporal_qd_supervisor_v3"
SUPERVISOR_CONFIG_SCHEMA = "temporal_qd_supervisor_config_v3"
SUPERVISOR_STATE_SCHEMA = "temporal_qd_supervisor_state_v3"
_SHA256_LENGTH = 71
_GIT_SHA_LENGTH = 40


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


def _capture_generation_artifacts(
    *, root: Path, generation_index: int
) -> dict[str, Any]:
    generation_root = root / "generations" / f"generation-{generation_index:04d}"
    proposal_root = generation_root / "proposal"
    campaign_root = generation_root / "campaign"
    result_root = campaign_root / "screening-run"
    population_path = proposal_root / "population.json"
    journal_path = proposal_root / "generation-journal.json"
    archive_path = generation_root / "archive.json"
    preparation_path = campaign_root / "preparation.json"
    authority_path = campaign_root / "authority.json"
    identity_path = campaign_root / "evaluation-identity.json"
    campaign_path = campaign_root / "campaign.json"
    task_manifest_path = result_root / "task-manifest.json"
    result_authority_path = result_root / "authority.json"
    checkpoint_path = result_root / "checkpoint.json"
    summary_path = result_root / "summary.json"

    population = _canonical_file(population_path, name="QD generation population")
    journal = _canonical_file(journal_path, name="QD generation journal")
    archive = _canonical_file(archive_path, name="QD generation archive")
    preparation = _canonical_file(preparation_path, name="QD campaign preparation")
    authority = _canonical_file(authority_path, name="QD campaign authority")
    evaluation_identity = _canonical_file(identity_path, name="QD evaluation identity")
    campaign = _canonical_file(campaign_path, name="QD campaign")
    task_manifest = _canonical_file(task_manifest_path, name="QD task manifest")
    result_authority = _canonical_file(result_authority_path, name="QD result authority")
    checkpoint = _canonical_file(checkpoint_path, name="QD evaluation checkpoint")
    summary = _canonical_file(summary_path, name="QD evaluation summary")

    if int(population.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("generation population index mismatch")
    if int(journal.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("generation journal index mismatch")
    if int(archive.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("generation archive index mismatch")
    if int(campaign.get("generationIndex", -1)) != generation_index:
        raise TemporalDiscoveryContractError("generation campaign index mismatch")

    population_sha = _identity_payload(
        population, "populationSha256", name="QD generation population"
    )
    if campaign.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError("QD campaign population binding mismatch")
    if evaluation_identity.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError(
            "QD evaluation identity population binding mismatch"
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
    return {
        "schemaVersion": "temporal_qd_supervisor_generation_artifacts_v1",
        "population": _self_hashed_descriptor(
            population_path,
            population,
            field="populationSha256",
            name="QD generation population",
        ),
        "journal": _self_hashed_descriptor(
            journal_path,
            journal,
            field="journalSha256",
            name="QD generation journal",
        ),
        "archive": _self_hashed_descriptor(
            archive_path,
            archive,
            field="archiveSha256",
            name="QD generation archive",
        ),
        "preparation": _artifact_descriptor(preparation_path, preparation),
        "authority": _self_hashed_descriptor(
            authority_path,
            authority,
            field="authorityId",
            name="QD campaign authority",
        ),
        "evaluationIdentity": _self_hashed_descriptor(
            identity_path,
            evaluation_identity,
            field="evaluationIdentitySha256",
            name="QD evaluation identity",
        ),
        "campaign": _self_hashed_descriptor(
            campaign_path,
            campaign,
            field="campaignSha256",
            name="QD campaign",
        ),
        "taskManifest": _artifact_descriptor(task_manifest_path, task_manifest),
        "resultAuthority": _self_hashed_descriptor(
            result_authority_path,
            result_authority,
            field="authorityId",
            name="QD result authority",
        ),
        "checkpoint": _artifact_descriptor(checkpoint_path, checkpoint),
        "summary": _artifact_descriptor(summary_path, summary),
        "results": results,
    }


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
    current = _capture_generation_artifacts(root=root, generation_index=generation_index)
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
    if (
        evaluation_identity.get("templatePreparationSha256")
        != evaluation.get("templatePreparationSha256")
        or evaluation_identity.get("predeclaredEvidenceContextSha256")
        != evaluation.get("predeclaredEvidenceContextSha256")
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
        int(record.get("taskCount") or 0) for record in records.values()
    ):
        raise TemporalDiscoveryContractError(
            "QD supervisor worker-task counter disagrees with completed generation records"
        )
    return records


def _validate_frozen_sources(config: Mapping[str, Any]) -> list[str]:
    """Reopen every path-backed source before each phase can consume it."""

    expected_config_sha = _sha256(config.get("configSha256"), name="supervisor config")
    material = _clone(config, name="QD supervisor config")
    material.pop("configSha256", None)
    if canonical_sha256(material) != expected_config_sha:
        raise TemporalDiscoveryContractError("QD supervisor frozen config identity mismatch")

    archive_binding = config.get("initialArchive")
    if not isinstance(archive_binding, Mapping):
        raise TemporalDiscoveryContractError("QD supervisor initial archive binding is invalid")
    archive, archive_sha = _load_archive(Path(str(archive_binding.get("path") or "")))
    if archive_sha != archive_binding.get("archiveSha256") or (
        archive.get("resultSetSha256") != archive_binding.get("resultSetSha256")
    ):
        raise TemporalDiscoveryContractError("QD supervisor initial archive drifted")

    source_binding = config.get("immigrantSource")
    if not isinstance(source_binding, Mapping):
        raise TemporalDiscoveryContractError("QD supervisor immigrant source binding is invalid")
    source = ExactGeneratorV2Continuation(
        source_preparation_path=Path(str(source_binding.get("sourcePreparationPath") or "")),
        base_generator_root=Path(str(source_binding.get("baseGeneratorRoot") or "")),
        confirmed_entry_admission_root=Path(
            str(source_binding.get("confirmedEntryAdmissionRoot") or "")
        ),
        start_continuation_ordinal=0,
    )
    if _clone(source.source_identity, name="reopened immigrant source") != _clone(
        source_binding.get("sourceIdentity"), name="frozen immigrant source"
    ):
        raise TemporalDiscoveryContractError("QD supervisor immigrant source drifted")

    validator_binding = config.get("validator")
    if not isinstance(validator_binding, Mapping):
        raise TemporalDiscoveryContractError("QD supervisor validator binding is invalid")
    command = _command(Path(str(validator_binding.get("commandFile") or "")))
    if command != validator_binding.get("command") or canonical_sha256(command) != (
        validator_binding.get("commandSha256")
    ):
        raise TemporalDiscoveryContractError("QD supervisor validator command drifted")

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

    return command


def _frozen_config(
    *,
    initial_archive_path: Path,
    source_preparation_path: Path,
    base_generator_root: Path,
    confirmed_entry_admission_root: Path,
    template_preparation_path: Path,
    validator_command_file: Path,
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
    construction_catalog_path: Path | str | None = None,
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
    if broad_admission and (generation_count < 4 or evaluation_target < 10_000):
        raise TemporalDiscoveryContractError(
            "broad admission requires at least four generations and 10,000 unique evaluations"
        )
    initial_archive, initial_archive_sha = _load_archive(initial_archive_path)
    template = _read(template_preparation_path, name="QD template preparation")
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
    source = ExactGeneratorV2Continuation(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        confirmed_entry_admission_root=confirmed_entry_admission_root,
        start_continuation_ordinal=initial_immigrant_continuation_ordinal,
    )
    validator_command = _command(validator_command_file)
    config = {
        "schemaVersion": SUPERVISOR_CONFIG_SCHEMA,
        "supervisorVersion": SUPERVISOR_VERSION,
        "qdVersion": QD_VERSION,
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "frozenPolicy": _clone(QD_POLICY, name="frozen QD policy"),
        "broadAdmission": bool(broad_admission),
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
        "immigrantSource": {
            "sourcePreparationPath": str(source_preparation_path.resolve()),
            "baseGeneratorRoot": str(base_generator_root.resolve()),
            "confirmedEntryAdmissionRoot": str(
                confirmed_entry_admission_root.resolve()
            ),
            "sourceIdentity": source.source_identity,
            "initialContinuationOrdinal": initial_immigrant_continuation_ordinal,
        },
        "validator": {
            "commandFile": str(validator_command_file.resolve()),
            "command": validator_command,
            "commandSha256": canonical_sha256(validator_command),
            "timeoutSeconds": 60.0,
        },
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
            "costViews": {
                "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
                "research_conservative": {
                    "spreadBps": 2.0,
                    "slippageBps": 1.0,
                    "commissionBps": 0.5,
                },
            },
        },
        "generationPlan": {
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
        },
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
    }
    config["configSha256"] = canonical_sha256(config)
    return config, validator_command


def run_qd_supervisor(
    *,
    run_root: Path | str,
    initial_archive_path: Path | str,
    source_preparation_path: Path | str,
    base_generator_root: Path | str,
    confirmed_entry_admission_root: Path | str,
    template_preparation_path: Path | str,
    validator_command_file: Path | str,
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
) -> dict[str, Any]:
    root = Path(run_root)
    root.mkdir(parents=True, exist_ok=True)
    initial_archive_file = Path(initial_archive_path)
    source_preparation_file = Path(source_preparation_path)
    base_generator_dir = Path(base_generator_root)
    confirmed_entry_dir = Path(confirmed_entry_admission_root)
    template_preparation_file = Path(template_preparation_path)
    validator_file = Path(validator_command_file)
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
        construction_catalog_path=construction_catalog_path,
    )
    config_path = root / "config.json"
    state_path = root / "state.json"
    _write_once(config_path, config)
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
            generation_result = generate_qd_generation(
                parent_archive_path=parent_archive_path,
                source_preparation_path=source_preparation_file,
                base_generator_root=base_generator_dir,
                confirmed_entry_admission_root=confirmed_entry_dir,
                validator_command=validator_command,
                output_root=proposal_root,
                generation_index=generation_index,
                immigrant_continuation_start=immigrant_cursor,
                parameters=config["frozenSearchPolicy"],
                evidence_identity_context=config["evaluation"][
                    "predeclaredEvidenceContext"
                ],
                identity_ledger_path=root / "identity-ledger.json",
                validator_timeout_seconds=float(config["validator"]["timeoutSeconds"]),
                construction_catalog_path=(
                    (config.get("constructionOperatorPolicy") or {})
                    .get("catalog", {})
                    .get("path")
                ),
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
                template_preparation_path=template_preparation_file,
                output_root=campaign_root,
                execution_engine_commit=config["repositories"]["executionEngineCommit"],
                worker_contract_sha256=config["workerContractSha256"],
                construction_catalog_path=(
                    (config.get("constructionOperatorPolicy") or {})
                    .get("catalog", {})
                    .get("path")
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
                or evaluation_identity.get("policySha256") != QD_POLICY_SHA256
                or evaluation_identity.get("predeclaredEvidenceContextSha256")
                != config["evaluation"]["predeclaredEvidenceContextSha256"]
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

            state["stage"] = "reducing_archive"
            _save_state(state_path, state)
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
            artifacts = _capture_generation_artifacts(
                root=root, generation_index=generation_index
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
                "journalSha256": journal_sha,
                "proposalCount": generation_result["proposalCount"],
                "candidateCount": generation_result["candidateCount"],
                "originProposalCounts": generation_result["originProposalCounts"],
                "originAcceptedCounts": generation_result["originAcceptedCounts"],
                "campaignSha256": campaign_result["campaignSha256"],
                "evaluationIdentitySha256": campaign_result["evaluationIdentitySha256"],
                "taskMatrixSha256": campaign_result["taskMatrixSha256"],
                "taskCount": campaign_result["taskCount"],
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
                "proposalSlots": generation_result["proposalSlots"],
                "uniqueIdentityCounts": generation_result["uniqueIdentityCounts"],
                "duplicateCounters": generation_result["duplicateCounters"],
                "proposalSlotCounters": generation_result["proposalSlotCounters"],
                "nextImmigrantContinuationOrdinal": generation_result[
                    "nextImmigrantContinuationOrdinal"
                ],
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
                    + int(campaign_result["taskCount"]),
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--initial-archive", type=Path, required=True)
    parser.add_argument("--source-preparation", type=Path, required=True)
    parser.add_argument("--base-generator-root", type=Path, required=True)
    parser.add_argument("--confirmed-entry-admission-root", type=Path, required=True)
    parser.add_argument("--template-preparation", type=Path, required=True)
    parser.add_argument("--validator-command-file", type=Path, required=True)
    parser.add_argument("--parameters", type=Path, required=True)
    parser.add_argument(
        "--construction-catalog",
        type=Path,
        required=True,
        help="canonical Stage 5E7-v3 construction catalog snapshot",
    )
    parser.add_argument("--generation-count", type=int, required=True)
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
    parser.add_argument("--stop-after-generation", type=int)
    args = parser.parse_args()
    parameters = _read(args.parameters, name="QD supervisor parameters")
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
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


__all__ = ["run_qd_supervisor"]
