"""Verified, opt-in compact projections of Temporal QD worker results.

The immutable ``results/<task>.json.gz`` files remain the evidence source of
truth.  The rotating supervisor's explicit ``indexed`` operational mode scans
one fully completed authority-bound task matrix, validates every raw result
with the existing codec and worker contract, and publishes an immutable index
containing the exact ``_window_record`` projection used by the legacy aggregate
path.  ``legacy`` remains the default/oracle mode.

Consumers may then repeatedly construct the legacy stage-result and rotating
provenance projections from the index without reopening a raw result blob.  The
first index build is intentionally Python-oracle based; it is the compact,
typed boundary a later native reducer can consume after parity is established.
"""

from __future__ import annotations

import base64
import gc
import gzip
import io
import json
import os
import re
import stat
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from .result_codec import (
    ResultCodecError,
    canonical_json_bytes,
    fsync_directory,
    read_json_object,
    semantic_sha256,
    sha256,
)
from .temporal_discovery_base import _CANDIDATE, TemporalDiscoveryContractError
from .temporal_discovery_results import _window_record
from .temporal_search import (
    TEMPORAL_SEARCH_CHECKPOINT_SCHEMA,
    TEMPORAL_SEARCH_MANIFEST_SCHEMA,
    TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA,
    TEMPORAL_SEARCH_RESULT_SCHEMA,
    TemporalSearchContractError,
    build_task_matrix,
    validate_authority,
    validate_v3_candidate_window_result,
    validate_warmup_rejected_candidate_window_result,
)

TAIL_RESULT_INDEX_SCHEMA = "temporal_qd_tail_result_index_v3"
TAIL_RESULT_INDEX_FILENAME = "tail-result-index-v3.json"
TAIL_RESULT_ENTRY_SCHEMA = "temporal_qd_tail_result_index_entry_v3"
TAIL_RAW_RESULT_REF_SCHEMA = "temporal_qd_tail_raw_result_ref_v1"
TAIL_STAGE_PROJECTION_SCHEMA = "temporal_qd_tail_stage_projection_v1"

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_BASE64 = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")
_CODEC_RECORD_FIELDS = (
    "resultCodec",
    "resultSemanticSha256",
    "resultSemanticSizeBytes",
    "resultUncompressedSha256",
    "resultUncompressedSizeBytes",
    "resultBlobSha256",
    "resultBlobSizeBytes",
)
_ROTATING_METRIC_FIELDS = (
    "conservativeNetR",
    "noCostNetR",
    "maxDrawdownR",
    "trades",
    "observations",
    "v3Admissible",
    "resolvedProgramSha256",
    "resolvedProfileSnapshotSha256",
    "sourceProfileSnapshotSha256",
)


class TemporalQDTailResultIndexError(TemporalDiscoveryContractError):
    """The tail-result index or any source bound to it is not trustworthy."""


def tail_result_index_path(result_root: Path | str) -> Path:
    """Return the sole immutable index location for one screening result root."""

    return Path(result_root) / TAIL_RESULT_INDEX_FILENAME


def _is_link_or_reparse(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except FileNotFoundError:
        return False
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    return stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point)


def _require_real_directory(path: Path, *, name: str) -> Path:
    if _is_link_or_reparse(path):
        raise TemporalQDTailResultIndexError(f"{name} may not be a symlink or junction")
    try:
        status = os.stat(path)
    except OSError as exc:
        raise TemporalQDTailResultIndexError(f"{name} is unavailable: {path}") from exc
    if not stat.S_ISDIR(status.st_mode):
        raise TemporalQDTailResultIndexError(f"{name} must be a real directory: {path}")
    return path.resolve(strict=True)


def _require_real_regular_file(path: Path, *, name: str) -> None:
    if _is_link_or_reparse(path):
        raise TemporalQDTailResultIndexError(f"{name} may not be a symlink or junction")
    try:
        status = os.stat(path)
    except OSError as exc:
        raise TemporalQDTailResultIndexError(f"{name} is unavailable: {path}") from exc
    if not stat.S_ISREG(status.st_mode):
        raise TemporalQDTailResultIndexError(f"{name} must be a regular file: {path}")


def _canonical_clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(canonical_json_bytes(value).decode("utf-8"))
    except (
        ResultCodecError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise TemporalQDTailResultIndexError(
            f"{name} must be finite canonical JSON"
        ) from exc


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    material = _canonical_clone(value, name=name)
    if not isinstance(material, dict):
        raise TemporalQDTailResultIndexError(f"{name} must be an object")
    return material


def _mapping_view(value: Any, *, name: str) -> Mapping[str, Any]:
    """Check a JSON-object-shaped view without a dump/reparse copy.

    Indexes have already been canonicalized before publication and source-verified
    when loaded.  Hot adapters must not serialize the whole compact artifact
    merely to inspect it: that transient copy can exceed the retained index
    itself.  Callers still receive only data that is checked against the stored
    semantic identities below.
    """

    if not isinstance(value, Mapping):
        raise TemporalQDTailResultIndexError(f"{name} must be an object")
    return value


def _sha(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TemporalQDTailResultIndexError(f"{name} must be an exact sha256 identity")
    return value


def _integer(value: Any, *, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise TemporalQDTailResultIndexError(
            f"{name} must be an integer greater than or equal to {minimum}"
        )
    return value


def _result_codec_expected(record: Mapping[str, Any]) -> dict[str, Any]:
    present = [field for field in _CODEC_RECORD_FIELDS if field in record]
    if present and len(present) != len(_CODEC_RECORD_FIELDS):
        raise TemporalQDTailResultIndexError(
            "checkpoint result representation metadata is incomplete"
        )
    if not present:
        return {}
    return {
        "codec": record["resultCodec"],
        "semanticSha256": record["resultSemanticSha256"],
        "semanticSizeBytes": record["resultSemanticSizeBytes"],
        "uncompressedSha256": record["resultUncompressedSha256"],
        "uncompressedSizeBytes": record["resultUncompressedSizeBytes"],
        "blobSha256": record["resultBlobSha256"],
        "blobSizeBytes": record["resultBlobSizeBytes"],
    }


def _raw_ref(*, relative_path: str, metadata: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "codec",
        "semanticSha256",
        "semanticSizeBytes",
        "uncompressedSha256",
        "uncompressedSizeBytes",
        "blobSha256",
        "blobSizeBytes",
    }
    if set(metadata) != expected:
        raise TemporalQDTailResultIndexError("result codec metadata is incomplete")
    return {
        "schemaVersion": TAIL_RAW_RESULT_REF_SCHEMA,
        "relativePath": relative_path,
        "resultSha256": metadata["semanticSha256"],
        "codec": metadata["codec"],
        "semanticSizeBytes": metadata["semanticSizeBytes"],
        "uncompressedSha256": metadata["uncompressedSha256"],
        "uncompressedSizeBytes": metadata["uncompressedSizeBytes"],
        # ``blobSha256``/``blobSizeBytes`` are the future CAS object identity.
        # The index never copies the blob itself.
        "blobSha256": metadata["blobSha256"],
        "blobSizeBytes": metadata["blobSizeBytes"],
    }


def _validate_raw_ref(value: Any, *, name: str) -> Mapping[str, Any]:
    ref = _mapping_view(value, name=name)
    expected = {
        "schemaVersion",
        "relativePath",
        "resultSha256",
        "codec",
        "semanticSizeBytes",
        "uncompressedSha256",
        "uncompressedSizeBytes",
        "blobSha256",
        "blobSizeBytes",
    }
    if set(ref) != expected or ref["schemaVersion"] != TAIL_RAW_RESULT_REF_SCHEMA:
        raise TemporalQDTailResultIndexError(f"{name} schema is invalid")
    relative = ref.get("relativePath")
    if (
        not isinstance(relative, str)
        or not relative.startswith("results/")
        or "\\" in relative
        or PurePosixPath(relative).is_absolute()
        or PurePosixPath(relative).parts[:1] != ("results",)
        or len(PurePosixPath(relative).parts) != 2
        or ".." in PurePosixPath(relative).parts
    ):
        raise TemporalQDTailResultIndexError(f"{name} relative result path is unsafe")
    if not relative.endswith((".json", ".json.gz")):
        raise TemporalQDTailResultIndexError(f"{name} result extension is unsupported")
    _sha(ref.get("resultSha256"), name=f"{name}.resultSha256")
    _sha(ref.get("uncompressedSha256"), name=f"{name}.uncompressedSha256")
    _sha(ref.get("blobSha256"), name=f"{name}.blobSha256")
    if not isinstance(ref.get("codec"), str) or not ref["codec"]:
        raise TemporalQDTailResultIndexError(f"{name}.codec is invalid")
    for field in (
        "semanticSizeBytes",
        "uncompressedSizeBytes",
        "blobSizeBytes",
    ):
        _integer(ref.get(field), name=f"{name}.{field}")
    return ref


def _canonical_gzip_bytes(data: bytes) -> bytes:
    """Encode canonical JSON with a stable gzip header and compression level."""

    output = io.BytesIO()
    with gzip.GzipFile(
        fileobj=output,
        mode="wb",
        filename="",
        compresslevel=9,
        mtime=0,
    ) as handle:
        handle.write(data)
    return output.getvalue()


def _stage_projection(record: Mapping[str, Any]) -> dict[str, Any]:
    """Compact a full legacy stage record without changing its value semantics."""

    try:
        semantic = canonical_json_bytes(record)
    except ResultCodecError as exc:
        raise TemporalQDTailResultIndexError(
            "tail stage projection must be finite canonical JSON"
        ) from exc
    return {
        "schemaVersion": TAIL_STAGE_PROJECTION_SCHEMA,
        "codec": "gzip-canonical-json-v1",
        "semanticSha256": sha256(semantic),
        "semanticSizeBytes": len(semantic),
        "blobBase64": base64.b64encode(_canonical_gzip_bytes(semantic)).decode("ascii"),
    }


def _validate_stage_projection(value: Any, *, name: str) -> Mapping[str, Any]:
    projection = _mapping_view(value, name=name)
    expected = {
        "schemaVersion",
        "codec",
        "semanticSha256",
        "semanticSizeBytes",
        "blobBase64",
    }
    if (
        set(projection) != expected
        or projection.get("schemaVersion") != TAIL_STAGE_PROJECTION_SCHEMA
        or projection.get("codec") != "gzip-canonical-json-v1"
    ):
        raise TemporalQDTailResultIndexError(f"{name} schema is invalid")
    _sha(projection.get("semanticSha256"), name=f"{name}.semanticSha256")
    _integer(projection.get("semanticSizeBytes"), name=f"{name}.semanticSizeBytes")
    encoded = projection.get("blobBase64")
    if not isinstance(encoded, str) or not encoded:
        raise TemporalQDTailResultIndexError(f"{name}.blobBase64 is invalid")
    if (
        len(encoded) % 4 != 0
        or _BASE64.fullmatch(encoded) is None
        or encoded.endswith("===")
    ):
        raise TemporalQDTailResultIndexError(f"{name}.blobBase64 is invalid")
    return projection


def _decode_stage_projection(value: Any, *, name: str) -> dict[str, Any]:
    projection = _validate_stage_projection(value, name=name)
    encoded = projection["blobBase64"]
    assert isinstance(encoded, str)  # Proven by _validate_stage_projection.
    try:
        blob = base64.b64decode(encoded.encode("ascii"), validate=True)
        semantic = gzip.decompress(blob)
    except (UnicodeEncodeError, ValueError, OSError, EOFError) as exc:
        raise TemporalQDTailResultIndexError(f"{name} is corrupt or truncated") from exc
    if _canonical_gzip_bytes(semantic) != blob:
        raise TemporalQDTailResultIndexError(
            f"{name} is not deterministic canonical gzip"
        )
    try:
        record = json.loads(semantic.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDTailResultIndexError(f"{name} is invalid JSON") from exc
    try:
        canonical = canonical_json_bytes(record)
    except ResultCodecError as exc:
        raise TemporalQDTailResultIndexError(f"{name} is not canonical JSON") from exc
    if not isinstance(record, dict) or canonical != semantic:
        raise TemporalQDTailResultIndexError(f"{name} is not canonical JSON")
    if (
        len(semantic) != projection["semanticSizeBytes"]
        or sha256(semantic) != projection["semanticSha256"]
    ):
        raise TemporalQDTailResultIndexError(f"{name} semantic identity drifted")
    return record


def _relative_result_path(path: Path, *, result_root: Path, results_dir: Path) -> str:
    _require_real_regular_file(path, name="checkpoint result")
    target = path.resolve(strict=True)
    if target.parent != results_dir:
        raise TemporalQDTailResultIndexError(
            "checkpoint result escaped its immutable result directory"
        )
    try:
        relative = target.relative_to(result_root).as_posix()
    except ValueError as exc:  # pragma: no cover - parent equality already proves this.
        raise TemporalQDTailResultIndexError(
            "checkpoint result escaped its root"
        ) from exc
    if not relative.startswith("results/"):
        raise TemporalQDTailResultIndexError("checkpoint result is outside results/")
    return relative


def _resolve_checkpoint_result_path(
    record: Mapping[str, Any], *, result_root: Path, results_dir: Path
) -> tuple[Path, str]:
    raw_value = record.get("resultPath")
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise TemporalQDTailResultIndexError("checkpoint result path is required")
    supplied = Path(raw_value)
    candidates = (
        [supplied] if supplied.is_absolute() else [supplied, result_root / supplied]
    )
    for candidate in candidates:
        try:
            if not candidate.exists():
                continue
            relative = _relative_result_path(
                candidate, result_root=result_root, results_dir=results_dir
            )
        except TemporalQDTailResultIndexError:
            raise
        except OSError:
            continue
        return candidate.resolve(strict=True), relative
    raise TemporalQDTailResultIndexError(
        "checkpoint result is missing or outside its immutable result directory"
    )


def _validate_completed_journal(
    checkpoint: Mapping[str, Any],
    *,
    expected_task_ids: set[str],
    completed: Mapping[str, Any],
) -> None:
    if "journal" not in checkpoint:
        # Historical completed checkpoints did not always retain the journal.
        # The exact completed mapping is still sufficient to construct a new
        # verified index without rewriting those legacy artifacts.
        return
    journal = checkpoint.get("journal")
    if not isinstance(journal, list):
        raise TemporalQDTailResultIndexError("checkpoint journal must be an array")
    by_task: dict[str, Mapping[str, Any]] = {}
    for raw in journal:
        if not isinstance(raw, Mapping):
            raise TemporalQDTailResultIndexError("checkpoint journal entry is invalid")
        task_id = raw.get("taskId")
        if not isinstance(task_id, str) or not task_id or task_id in by_task:
            raise TemporalQDTailResultIndexError(
                "checkpoint journal has a missing or duplicate task"
            )
        by_task[task_id] = raw
    if set(by_task) != expected_task_ids:
        raise TemporalQDTailResultIndexError(
            "checkpoint journal does not cover the exact task matrix"
        )
    for task_id, record in completed.items():
        journal_record = dict(by_task[task_id])
        journal_record.pop("taskId", None)
        if _canonical_clone(journal_record, name="checkpoint journal record") != record:
            raise TemporalQDTailResultIndexError(
                "checkpoint journal diverged from completed result record"
            )


def _validated_sources(
    *,
    authority: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    authority_value = _mapping(authority, name="authority")
    manifest = _mapping(task_manifest, name="task manifest")
    checkpoint_value = _mapping(checkpoint, name="checkpoint")
    try:
        frozen_authority = validate_authority(authority_value)
        expected_tasks = build_task_matrix(frozen_authority)
    except TemporalSearchContractError as exc:
        raise TemporalQDTailResultIndexError(
            "tail index authority is invalid or stale"
        ) from exc

    required_manifest = {
        "schemaVersion",
        "authorityId",
        "taskCount",
        "tasks",
        "taskMatrixSha256",
    }
    if set(manifest) != required_manifest:
        raise TemporalQDTailResultIndexError("task manifest schema is invalid")
    if manifest.get("schemaVersion") != TEMPORAL_SEARCH_MANIFEST_SCHEMA:
        raise TemporalQDTailResultIndexError("task manifest schema version is invalid")
    if manifest.get("authorityId") != frozen_authority["authorityId"]:
        raise TemporalQDTailResultIndexError("task manifest authority binding drifted")
    if not isinstance(manifest.get("tasks"), list):
        raise TemporalQDTailResultIndexError("task manifest tasks must be an array")
    if _integer(manifest.get("taskCount"), name="task manifest taskCount") != len(
        expected_tasks
    ):
        raise TemporalQDTailResultIndexError("task manifest task count drifted")
    expected_matrix_sha = semantic_sha256(expected_tasks)
    if manifest.get("taskMatrixSha256") != expected_matrix_sha:
        raise TemporalQDTailResultIndexError("task manifest matrix identity drifted")
    if manifest["tasks"] != expected_tasks:
        raise TemporalQDTailResultIndexError(
            "task manifest tasks diverged from the frozen authority"
        )

    task_map: dict[str, dict[str, Any]] = {}
    for task in expected_tasks:
        task_id = task.get("task_id")
        if not isinstance(task_id, str) or not task_id or task_id in task_map:
            raise TemporalQDTailResultIndexError("task manifest has a duplicate task")
        task_map[task_id] = task

    if checkpoint_value.get("schemaVersion") != TEMPORAL_SEARCH_CHECKPOINT_SCHEMA:
        raise TemporalQDTailResultIndexError("checkpoint schema version is invalid")
    if (
        checkpoint_value.get("authorityId") != frozen_authority["authorityId"]
        or checkpoint_value.get("taskMatrixSha256") != expected_matrix_sha
    ):
        raise TemporalQDTailResultIndexError(
            "checkpoint authority or task-matrix binding drifted"
        )
    completed_raw = checkpoint_value.get("completed")
    if not isinstance(completed_raw, Mapping):
        raise TemporalQDTailResultIndexError(
            "checkpoint completed results must be an object"
        )
    completed: dict[str, dict[str, Any]] = {}
    for task_id, raw_record in completed_raw.items():
        if not isinstance(task_id, str) or not isinstance(raw_record, Mapping):
            raise TemporalQDTailResultIndexError(
                "checkpoint completed result is invalid"
            )
        if task_id in completed:
            raise TemporalQDTailResultIndexError(
                "checkpoint has a duplicate completed task"
            )
        completed[task_id] = _mapping(raw_record, name="checkpoint completed result")
    if set(completed) != set(task_map):
        raise TemporalQDTailResultIndexError(
            "checkpoint does not cover the exact completed task matrix"
        )
    _validate_completed_journal(
        checkpoint_value, expected_task_ids=set(task_map), completed=completed
    )
    return frozen_authority, manifest, checkpoint_value, task_map, completed


def _validate_result_binding(
    *,
    task_id: str,
    task: Mapping[str, Any],
    record: Mapping[str, Any],
    result: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> None:
    payload = task.get("payload")
    if not isinstance(payload, Mapping):
        raise TemporalQDTailResultIndexError("task payload is invalid")
    candidate_id = payload.get("candidate_id")
    if not isinstance(candidate_id, str) or not candidate_id:
        raise TemporalQDTailResultIndexError("task candidate identity is invalid")
    if record.get("candidateId") != candidate_id:
        raise TemporalQDTailResultIndexError(
            "checkpoint candidate identity does not match its task"
        )
    expected = {
        "schema_version": result.get("schema_version"),
        "task_kind": task.get("task_kind"),
        "job_id": payload.get("job_id"),
        "authority_id": payload.get("authority_id"),
        "candidate_id": candidate_id,
        "evidence_plan_id": (
            payload.get("evidence_plan", {}).get("plan_id")
            if isinstance(payload.get("evidence_plan"), Mapping)
            else None
        ),
        "lake_window_semantic_sha256": payload.get("lake_window_semantic_sha256"),
        "shared_observation_stream_id": payload.get("shared_observation_stream_id"),
    }
    if any(value is None for value in expected.values()):
        raise TemporalQDTailResultIndexError("task lacks a required worker binding")
    for field, expected_value in expected.items():
        if result.get(field) != expected_value:
            raise TemporalQDTailResultIndexError(
                f"raw result does not match task {task_id} for {field}"
            )
    if record.get("resultSha256") != metadata.get("semanticSha256"):
        raise TemporalQDTailResultIndexError(
            "checkpoint result semantic identity does not match its raw blob"
        )
    try:
        if result.get("schema_version") == TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA:
            validate_warmup_rejected_candidate_window_result(result, task_payload=payload)
        elif result.get("schema_version") == TEMPORAL_SEARCH_RESULT_SCHEMA:
            validate_v3_candidate_window_result(result, task_payload=payload)
        else:
            raise TemporalSearchContractError("raw result schema is unsupported")
    except TemporalSearchContractError as exc:
        raise TemporalQDTailResultIndexError(
            "raw result failed its candidate-window task validation"
        ) from exc


def _rotating_metrics(record: Mapping[str, Any]) -> dict[str, Any]:
    if record.get("v3Admissible") is not True:
        raise TemporalQDTailResultIndexError(
            "rotating tail index requires Stage 5E7-v3 result evidence"
        )
    try:
        return {
            "conservativeNetR": record["conservativeNetR"],
            "noCostNetR": record["noCostNetR"],
            "maxDrawdownR": record["maxDrawdownR"],
            "closedTrades": record["trades"],
            "observations": record["observations"],
            "v3Admissible": record["v3Admissible"],
            "resolvedProgramSha256": record["resolvedProgramSha256"],
            "resolvedProfileSnapshotSha256": record["resolvedProfileSnapshotSha256"],
            "sourceProfileSnapshotSha256": record["sourceProfileSnapshotSha256"],
        }
    except KeyError as exc:
        raise TemporalQDTailResultIndexError(
            "window record lacks rotating-evidence metrics"
        ) from exc


def _funnel_projection(
    *, result: Mapping[str, Any], result_sha256: str, record: Mapping[str, Any]
) -> dict[str, Any]:
    # Keep this lazy so the normal tail index has no funnel dependency/cost.
    from .temporal_qd_funnel_adapter import _result_behavior

    try:
        behavior = _result_behavior(
            result,
            result_sha=result_sha256,
            window_id=str(record["windowId"]),
        )
    except TemporalDiscoveryContractError as exc:
        raise TemporalQDTailResultIndexError(
            "raw result cannot supply its optional funnel projection"
        ) from exc
    return {
        "resultBehavior": behavior,
        "terminalAdjustedConservativeNetR": record["terminalAdjustedConservativeNetR"],
        "terminalAdjustedMaxDrawdownR": record["terminalAdjustedMaxDrawdownR"],
    }


def _entry(
    *,
    task_id: str,
    task: Mapping[str, Any],
    relative_path: str,
    metadata: Mapping[str, Any],
    result: Mapping[str, Any],
    include_funnel_projection: bool,
) -> dict[str, Any]:
    payload = task["payload"]
    assert isinstance(payload, Mapping)  # Proven by _validate_result_binding.
    try:
        record = _window_record(result)
    except (TemporalDiscoveryContractError, TemporalSearchContractError) as exc:
        raise TemporalQDTailResultIndexError(
            "raw result cannot produce the canonical tail window projection"
        ) from exc
    if (
        record.get("candidateId") != payload["candidate_id"]
        or record.get("analysisWindowStart") != payload["analysis_window_start"]
        or record.get("analysisWindowEnd") != payload["analysis_window_end"]
    ):
        raise TemporalQDTailResultIndexError(
            "window record does not match its validated task binding"
        )
    evidence_plan = payload.get("evidence_plan")
    if not isinstance(evidence_plan, Mapping):
        raise TemporalQDTailResultIndexError("task evidence plan is invalid")
    plan_id = _sha(
        evidence_plan.get("plan_id"), name="task evidence plan semantic identity"
    )
    result_sha = _sha(
        metadata.get("semanticSha256"), name="raw result semantic identity"
    )
    output: dict[str, Any] = {
        "schemaVersion": TAIL_RESULT_ENTRY_SCHEMA,
        "task": {
            "taskId": task_id,
            "candidateId": payload["candidate_id"],
            "analysisWindowStart": payload["analysis_window_start"],
            "analysisWindowEnd": payload["analysis_window_end"],
            "evidencePlanSemanticSha256": plan_id,
            "taskPayloadSha256": semantic_sha256(payload),
        },
        "rawResultRef": _raw_ref(relative_path=relative_path, metadata=metadata),
        "rawTaskProvenance": {
            "taskId": task_id,
            "resultSha256": result_sha,
        },
    }
    if record.get("evaluationRejected") is True:
        output["rejection"] = _canonical_clone(
            record.get("rejection"), name="tail warmup rejection"
        )
    else:
        output["stageProjection"] = _stage_projection(record)
        output["rotatingEvidenceMetrics"] = _rotating_metrics(record)
    if include_funnel_projection and record.get("evaluationRejected") is not True:
        output["funnelProjection"] = _funnel_projection(
            result=result, result_sha256=result_sha, record=record
        )
    output["entrySha256"] = semantic_sha256(output)
    return output


def _validate_entry(
    value: Any,
    *,
    include_funnel: bool,
) -> Mapping[str, Any]:
    entry = _mapping_view(value, name="tail result index entry")
    required = {
        "schemaVersion",
        "task",
        "rawResultRef",
        "rawTaskProvenance",
        "entrySha256",
    }
    rejected = "rejection" in entry
    if rejected:
        required.add("rejection")
    else:
        required.update({"stageProjection", "rotatingEvidenceMetrics"})
    if include_funnel and not rejected:
        required.add("funnelProjection")
    if set(entry) != required or entry.get("schemaVersion") != TAIL_RESULT_ENTRY_SCHEMA:
        raise TemporalQDTailResultIndexError(
            "tail result index entry schema is invalid"
        )
    supplied = _sha(entry.get("entrySha256"), name="tail result index entry identity")
    body = dict(entry)
    body.pop("entrySha256")
    if supplied != semantic_sha256(body):
        raise TemporalQDTailResultIndexError("tail result index entry identity drifted")
    task = _mapping_view(entry.get("task"), name="tail result index task")
    if set(task) != {
        "taskId",
        "candidateId",
        "analysisWindowStart",
        "analysisWindowEnd",
        "evidencePlanSemanticSha256",
        "taskPayloadSha256",
    }:
        raise TemporalQDTailResultIndexError("tail result index task schema is invalid")
    for field in ("taskId", "candidateId", "analysisWindowStart", "analysisWindowEnd"):
        if not isinstance(task.get(field), str) or not task[field]:
            raise TemporalQDTailResultIndexError(
                f"tail result index task {field} is invalid"
            )
    _sha(task.get("evidencePlanSemanticSha256"), name="tail result task plan identity")
    _sha(task.get("taskPayloadSha256"), name="tail result task payload identity")
    raw_ref = _validate_raw_ref(entry.get("rawResultRef"), name="tail raw result ref")
    if rejected:
        rejection = _mapping_view(entry.get("rejection"), name="tail warmup rejection")
        common_rejection_fields = {
            "schema_version", "disposition", "reason_code", "replay_executed",
            "worker_attempt_id", "worker_lease_id", "worker_error",
            "worker_error_sha256", "worker_completion_sha256",
        }
        v1 = rejection.get("schema_version") == "temporal_candidate_window_rejection_v1"
        v2 = rejection.get("schema_version") == "temporal_candidate_window_rejection_v2"
        expected_rejection_fields = common_rejection_fields | ({"replay_completed"} if v2 else set())
        if set(rejection) != expected_rejection_fields or rejection.get("disposition") != "rejected" or not (
            (v1 and rejection.get("reason_code") == "aligned_scoring_warmup_insufficient" and rejection.get("replay_executed") is False)
            or (v2 and rejection.get("reason_code") == "duplicate_break_even_execution_invariant" and rejection.get("replay_executed") is True and rejection.get("replay_completed") is False)
        ):
            raise TemporalQDTailResultIndexError("tail warmup rejection is invalid")
        _sha(rejection.get("worker_error_sha256"), name="tail warmup rejection error hash")
        _sha(rejection.get("worker_completion_sha256"), name="tail warmup rejection completion hash")
    else:
        _validate_stage_projection(
            entry.get("stageProjection"), name="tail stage projection"
        )
        metrics = _mapping_view(
            entry.get("rotatingEvidenceMetrics"), name="tail rotating metrics"
        )
        if set(metrics) != {
        "conservativeNetR",
        "noCostNetR",
        "maxDrawdownR",
        "closedTrades",
        "observations",
        "v3Admissible",
        "resolvedProgramSha256",
        "resolvedProfileSnapshotSha256",
        "sourceProfileSnapshotSha256",
        }:
            raise TemporalQDTailResultIndexError("tail rotating metrics schema is invalid")
        if metrics.get("v3Admissible") is not True:
            raise TemporalQDTailResultIndexError(
                "tail rotating metrics are not v3-admissible"
            )
        for field in (
        "resolvedProgramSha256",
        "resolvedProfileSnapshotSha256",
        "sourceProfileSnapshotSha256",
        ):
            _sha(metrics.get(field), name=f"tail rotating metrics {field}")
        for field in ("closedTrades", "observations"):
            _integer(metrics.get(field), name=f"tail rotating metrics {field}")
    provenance = _mapping_view(
        entry.get("rawTaskProvenance"), name="tail raw provenance"
    )
    if set(provenance) != {"taskId", "resultSha256"}:
        raise TemporalQDTailResultIndexError("tail raw provenance schema is invalid")
    if (
        provenance.get("taskId") != task["taskId"]
        or provenance.get("resultSha256") != raw_ref["resultSha256"]
    ):
        raise TemporalQDTailResultIndexError("tail raw provenance binding drifted")
    if include_funnel and not rejected:
        funnel = _mapping_view(
            entry.get("funnelProjection"), name="tail funnel projection"
        )
        if set(funnel) != {
            "resultBehavior",
            "terminalAdjustedConservativeNetR",
            "terminalAdjustedMaxDrawdownR",
        }:
            raise TemporalQDTailResultIndexError(
                "tail funnel projection schema is invalid"
            )
        behavior = _mapping_view(
            funnel.get("resultBehavior"), name="tail funnel behavior"
        )
        if behavior.get("resultSha256") != raw_ref["resultSha256"]:
            raise TemporalQDTailResultIndexError("tail funnel result identity drifted")
    return entry


def validate_tail_result_index(index: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a self-authenticating in-memory index without reopening blobs.

    Validation deliberately inspects the supplied canonical structure in place.
    It does not dump and parse the entire index merely to make a second copy;
    the index and its stage projection can therefore be retained together by a
    hot tail path without transient multi-megabyte clones.
    """

    payload = _mapping_view(index, name="tail result index")
    expected = {
        "schemaVersion",
        "authorityId",
        "authoritySha256",
        "taskMatrixSha256",
        "taskManifestSha256",
        "checkpointSha256",
        "taskCount",
        "funnelProjectionIncluded",
        "sourceResultBlobBytes",
        "entries",
        "tailResultIndexSha256",
    }
    if (
        set(payload) != expected
        or payload.get("schemaVersion") != TAIL_RESULT_INDEX_SCHEMA
    ):
        raise TemporalQDTailResultIndexError("tail result index schema is invalid")
    supplied = _sha(
        payload.get("tailResultIndexSha256"), name="tail result index identity"
    )
    body = dict(payload)
    body.pop("tailResultIndexSha256")
    if supplied != semantic_sha256(body):
        raise TemporalQDTailResultIndexError("tail result index identity drifted")
    if not isinstance(payload.get("authorityId"), str) or not payload["authorityId"]:
        raise TemporalQDTailResultIndexError(
            "tail result index authority ID is invalid"
        )
    for field in (
        "authoritySha256",
        "taskMatrixSha256",
        "taskManifestSha256",
        "checkpointSha256",
    ):
        _sha(payload.get(field), name=f"tail result index {field}")
    task_count = _integer(payload.get("taskCount"), name="tail result index taskCount")
    if not isinstance(payload.get("funnelProjectionIncluded"), bool):
        raise TemporalQDTailResultIndexError("tail funnel feature flag is invalid")
    blob_bytes = _integer(
        payload.get("sourceResultBlobBytes"), name="tail result source blob bytes"
    )
    entries_raw = payload.get("entries")
    if not isinstance(entries_raw, list) or len(entries_raw) != task_count:
        raise TemporalQDTailResultIndexError("tail result index task count is invalid")
    entries = [
        _validate_entry(
            entry,
            include_funnel=payload["funnelProjectionIncluded"],
        )
        for entry in entries_raw
    ]
    task_ids = [str(entry["task"]["taskId"]) for entry in entries]
    if task_ids != sorted(task_ids) or len(set(task_ids)) != len(task_ids):
        raise TemporalQDTailResultIndexError(
            "tail result index entries are not unique canonical task order"
        )
    if (
        sum(int(entry["rawResultRef"]["blobSizeBytes"]) for entry in entries)
        != blob_bytes
    ):
        raise TemporalQDTailResultIndexError(
            "tail result index source blob byte count drifted"
        )
    return dict(payload)


def _assert_result_directory_exact(
    *, results_dir: Path, expected_paths: set[Path]
) -> None:
    _require_real_directory(results_dir, name="immutable results directory")
    actual_paths: set[Path] = set()
    stems: set[str] = set()
    try:
        children = list(results_dir.iterdir())
    except OSError as exc:
        raise TemporalQDTailResultIndexError(
            "cannot enumerate immutable results"
        ) from exc
    for child in children:
        if _is_link_or_reparse(child):
            raise TemporalQDTailResultIndexError(
                "immutable results directory contains a symlink or junction"
            )
        if not child.is_file() or not child.name.endswith((".json", ".json.gz")):
            continue
        target = child.resolve(strict=True)
        stem = child.name.removesuffix(".gz").removesuffix(".json")
        if stem in stems:
            raise TemporalQDTailResultIndexError(
                "immutable results directory has duplicate result representations"
            )
        stems.add(stem)
        actual_paths.add(target)
    if actual_paths != expected_paths:
        raise TemporalQDTailResultIndexError(
            "immutable results directory does not exactly match completed tasks"
        )


def _write_index_once(path: Path, payload: Mapping[str, Any]) -> None:
    data = canonical_json_bytes(payload)
    if path.exists():
        _require_real_regular_file(path, name="tail result index")
        try:
            existing = path.read_bytes()
        except OSError as exc:
            raise TemporalQDTailResultIndexError(
                "cannot read existing tail result index"
            ) from exc
        if existing != data:
            raise TemporalQDTailResultIndexError(
                "refusing to overwrite divergent immutable tail result index"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    _require_real_directory(path.parent, name="tail result index directory")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            _require_real_regular_file(path, name="tail result index")
            if path.read_bytes() != data:
                raise TemporalQDTailResultIndexError(
                    "refusing to overwrite divergent immutable tail result index"
                )
        else:
            fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_index_file(path: Path) -> dict[str, Any]:
    _require_real_regular_file(path, name="tail result index")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDTailResultIndexError(
            "tail result index is partial or invalid"
        ) from exc
    if canonical_json_bytes(payload) != raw:
        raise TemporalQDTailResultIndexError(
            "tail result index is not an exact canonical representation"
        )
    return _mapping(payload, name="tail result index")


def _source_bindings(
    *,
    authority: Mapping[str, Any],
    manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> dict[str, str]:
    return {
        "authoritySha256": semantic_sha256(authority),
        "taskManifestSha256": semantic_sha256(manifest),
        "checkpointSha256": semantic_sha256(checkpoint),
    }


def _verify_index_against_sources(
    *,
    index: Mapping[str, Any],
    result_root: Path,
    task_map: Mapping[str, Mapping[str, Any]],
    completed: Mapping[str, Mapping[str, Any]],
) -> None:
    results_dir = _require_real_directory(
        result_root / "results", name="results directory"
    )
    entries = index["entries"]
    assert isinstance(entries, list)
    expected_paths: set[Path] = set()
    by_task = {str(entry["task"]["taskId"]): entry for entry in entries}
    if set(by_task) != set(task_map):
        raise TemporalQDTailResultIndexError("tail result index task matrix drifted")
    for task_id, record in completed.items():
        path, relative_path = _resolve_checkpoint_result_path(
            record, result_root=result_root, results_dir=results_dir
        )
        expected_paths.add(path)
        raw_ref = by_task[task_id]["rawResultRef"]
        if raw_ref["relativePath"] != relative_path:
            raise TemporalQDTailResultIndexError(
                "tail result index raw path binding drifted"
            )
    _assert_result_directory_exact(
        results_dir=results_dir, expected_paths=expected_paths
    )

    for task_id in sorted(task_map):
        task = task_map[task_id]
        checkpoint_record = completed[task_id]
        path, _relative_path = _resolve_checkpoint_result_path(
            checkpoint_record, result_root=result_root, results_dir=results_dir
        )
        try:
            result, metadata = read_json_object(
                path, expected=_result_codec_expected(checkpoint_record)
            )
        except ResultCodecError as exc:
            raise TemporalQDTailResultIndexError(
                "tail result source blob is corrupt, truncated, or non-canonical"
            ) from exc
        _validate_result_binding(
            task_id=task_id,
            task=task,
            record=checkpoint_record,
            result=result,
            metadata=metadata,
        )
        entry = by_task[task_id]
        expected_ref = _raw_ref(
            relative_path=entry["rawResultRef"]["relativePath"], metadata=metadata
        )
        if entry["rawResultRef"] != expected_ref:
            raise TemporalQDTailResultIndexError(
                "tail result raw content reference drifted"
            )
        rebuilt = _entry(
            task_id=task_id,
            task=task,
            relative_path=entry["rawResultRef"]["relativePath"],
            metadata=metadata,
            result=result,
            include_funnel_projection=bool(index["funnelProjectionIncluded"]),
        )
        if rebuilt != entry:
            raise TemporalQDTailResultIndexError(
                "tail result index projection drifted from canonical raw result"
            )


def build_tail_result_index(
    *,
    result_root: Path | str,
    authority: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    include_funnel_projection: bool = False,
) -> dict[str, Any]:
    """Build or reopen one verified immutable result index.

    A new build reads each completed raw blob exactly once, in sorted task-ID
    order.  Reopening a completed index verifies its source blobs once but never
    rewrites the raw result or index file.  Callers holding the returned mapping
    can use the indexed adapters repeatedly without further raw I/O.
    """

    if not isinstance(include_funnel_projection, bool):
        raise TemporalQDTailResultIndexError(
            "include_funnel_projection must be boolean"
        )
    root = _require_real_directory(Path(result_root), name="result root")
    frozen, manifest, checkpoint_value, task_map, completed = _validated_sources(
        authority=authority, task_manifest=task_manifest, checkpoint=checkpoint
    )
    index_file = tail_result_index_path(root)
    if index_file.exists():
        existing = load_tail_result_index(
            result_root=root,
            authority=authority,
            task_manifest=task_manifest,
            checkpoint=checkpoint,
            verify_source_blobs=True,
        )
        if existing["funnelProjectionIncluded"] != include_funnel_projection:
            raise TemporalQDTailResultIndexError(
                "existing tail result index funnel feature flag diverged"
            )
        return existing

    results_dir = _require_real_directory(root / "results", name="results directory")
    paths: dict[str, tuple[Path, str]] = {}
    expected_paths: set[Path] = set()
    for task_id, record in completed.items():
        path, relative_path = _resolve_checkpoint_result_path(
            record, result_root=root, results_dir=results_dir
        )
        paths[task_id] = (path, relative_path)
        expected_paths.add(path)
    if len(expected_paths) != len(paths):
        raise TemporalQDTailResultIndexError(
            "multiple completed tasks reference the same raw result blob"
        )
    _assert_result_directory_exact(
        results_dir=results_dir, expected_paths=expected_paths
    )

    entries: list[dict[str, Any]] = []
    source_blob_bytes = 0
    for task_id in sorted(task_map):
        task = task_map[task_id]
        checkpoint_record = completed[task_id]
        path, relative_path = paths[task_id]
        try:
            result, metadata = read_json_object(
                path, expected=_result_codec_expected(checkpoint_record)
            )
        except ResultCodecError as exc:
            raise TemporalQDTailResultIndexError(
                "tail result source blob is corrupt, truncated, or non-canonical"
            ) from exc
        _validate_result_binding(
            task_id=task_id,
            task=task,
            record=checkpoint_record,
            result=result,
            metadata=metadata,
        )
        entry = _entry(
            task_id=task_id,
            task=task,
            relative_path=relative_path,
            metadata=metadata,
            result=result,
            include_funnel_projection=include_funnel_projection,
        )
        entries.append(entry)
        source_blob_bytes += int(metadata["blobSizeBytes"])

    bindings = _source_bindings(
        authority=frozen, manifest=manifest, checkpoint=checkpoint_value
    )
    index: dict[str, Any] = {
        "schemaVersion": TAIL_RESULT_INDEX_SCHEMA,
        "authorityId": frozen["authorityId"],
        **bindings,
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "taskCount": len(entries),
        "funnelProjectionIncluded": include_funnel_projection,
        "sourceResultBlobBytes": source_blob_bytes,
        "entries": entries,
    }
    index["tailResultIndexSha256"] = semantic_sha256(index)
    validated = validate_tail_result_index(index)
    _write_index_once(index_file, validated)
    return validated


def load_tail_result_index(
    *,
    result_root: Path | str,
    authority: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    verify_source_blobs: bool = True,
) -> dict[str, Any]:
    """Load a completed index and fail closed on source/binding drift.

    ``verify_source_blobs`` is true by default for restart/audit callers.  Set
    it to false only after this *same process* has either built the exact index
    or loaded it with source verification enabled, and retains that verified
    in-memory index for the ensuing reuse.  It is not a restart, cross-process,
    or new-checkpoint shortcut.  The pure adapters below are the preferred
    repeated-use path because they do not reopen source blobs at all.
    """

    if not isinstance(verify_source_blobs, bool):
        raise TemporalQDTailResultIndexError("verify_source_blobs must be boolean")
    root = _require_real_directory(Path(result_root), name="result root")
    frozen, manifest, checkpoint_value, task_map, completed = _validated_sources(
        authority=authority, task_manifest=task_manifest, checkpoint=checkpoint
    )
    index = validate_tail_result_index(_read_index_file(tail_result_index_path(root)))
    bindings = _source_bindings(
        authority=frozen, manifest=manifest, checkpoint=checkpoint_value
    )
    if (
        index["authorityId"] != frozen["authorityId"]
        or index["taskMatrixSha256"] != manifest["taskMatrixSha256"]
        or any(index[key] != value for key, value in bindings.items())
        or index["taskCount"] != len(task_map)
    ):
        raise TemporalQDTailResultIndexError(
            "tail result index authority, manifest, or checkpoint binding drifted"
        )
    if verify_source_blobs:
        _verify_index_against_sources(
            index=index,
            result_root=root,
            task_map=task_map,
            completed=completed,
        )
    return index


def load_indexed_stage_results(
    index: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Reproduce ``load_stage_results`` from a verified in-memory index.

    The index retains compressed canonical stage projections, so materializing
    the legacy-compatible records here is the only full-record copy held by a
    tail phase.  The rotating adapter below reads compact metrics directly.
    """

    payload = validate_tail_result_index(index)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in payload["entries"]:
        task = entry["task"]
        if "rejection" in entry:
            record = {
                "economicsBasis": "not_evaluated_" + str(entry["rejection"]["reason_code"]),
                "v3Admissible": False,
                "evaluationRejected": True,
                "rejection": _canonical_clone(
                    entry["rejection"], name="indexed warmup rejection"
                ),
                "candidateId": task["candidateId"],
                "windowId": task["analysisWindowStart"] + "/" + task["analysisWindowEnd"],
                "analysisWindowStart": task["analysisWindowStart"],
                "analysisWindowEnd": task["analysisWindowEnd"],
            }
        else:
            record = _decode_stage_projection(
                entry["stageProjection"], name="indexed stage projection"
            )
        candidate_id = record.get("candidateId")
        if (
            not isinstance(candidate_id, str)
            or _CANDIDATE.fullmatch(candidate_id) is None
        ):
            raise TemporalQDTailResultIndexError(
                "indexed window candidate identity is invalid"
            )
        if (
            candidate_id != task["candidateId"]
            or record.get("analysisWindowStart") != task["analysisWindowStart"]
            or record.get("analysisWindowEnd") != task["analysisWindowEnd"]
            or (
                "rejection" not in entry
                and _rotating_metrics(record) != entry["rotatingEvidenceMetrics"]
            )
        ):
            raise TemporalQDTailResultIndexError(
                "indexed stage projection drifted from compact task evidence"
            )
        grouped.setdefault(candidate_id, []).append(record)
    for candidate_id, records in grouped.items():
        records.sort(
            key=lambda item: (
                str(item["analysisWindowStart"]),
                str(item["analysisWindowEnd"]),
            )
        )
    return grouped


def load_indexed_provenance_bound_window_evidence(
    *,
    index: Mapping[str, Any],
    panel: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Reproduce rotating provenance evidence without reopening raw results."""

    from .temporal_qd_rotating_evidence import build_candidate_window_evidence

    payload = validate_tail_result_index(index)
    panel_value = _mapping(panel, name="rotating evidence panel")
    windows_raw = panel_value.get("windows")
    if not isinstance(windows_raw, list):
        raise TemporalQDTailResultIndexError(
            "rotating evidence panel windows are invalid"
        )
    windows: dict[tuple[str, str], Mapping[str, Any]] = {}
    for raw_window in windows_raw:
        if not isinstance(raw_window, Mapping):
            raise TemporalQDTailResultIndexError(
                "rotating evidence panel window is invalid"
            )
        key = (
            str(raw_window.get("analysisWindowStart")),
            str(raw_window.get("analysisWindowEnd")),
        )
        if key in windows:
            raise TemporalQDTailResultIndexError(
                "rotating evidence panel repeats a window"
            )
        windows[key] = raw_window
    if not isinstance(panel_value.get("panelId"), str) or not panel_value["panelId"]:
        raise TemporalQDTailResultIndexError("rotating evidence panel ID is invalid")

    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in payload["entries"]:
        if "rejection" in entry:
            # No replay occurred, so this candidate is intentionally omitted
            # from rotating evidence and breeder/provisional eligibility.
            continue
        task = entry["task"]
        candidate_id = str(task["candidateId"])
        candidate = candidates.get(candidate_id)
        if candidate is None:
            # Archive eligibility may deliberately narrow a completed task
            # matrix (for example, after a structural execution invariant is
            # detected).  The index itself has already verified every entry;
            # only project evidence for the caller's admitted candidate set.
            continue
        if not isinstance(candidate, Mapping):
            raise TemporalQDTailResultIndexError(
                "indexed rotating evidence candidate provenance mismatch"
            )
        key = (
            str(task["analysisWindowStart"]),
            str(task["analysisWindowEnd"]),
        )
        window = windows.get(key)
        if window is None:
            raise TemporalQDTailResultIndexError(
                "indexed rotating evidence result is outside its panel"
            )
        candidate_snapshot = candidate.get("profileSnapshotSha256")
        metrics = entry["rotatingEvidenceMetrics"]
        if (
            not isinstance(candidate_snapshot, str)
            or metrics.get("sourceProfileSnapshotSha256") != candidate_snapshot
        ):
            raise TemporalQDTailResultIndexError(
                "indexed rotating evidence source profile does not match candidate"
            )
        grouped.setdefault(candidate_id, []).append(
            build_candidate_window_evidence(
                candidate=candidate,
                panel=panel_value,
                window=window,
                metrics=metrics,
                evidence_plan_semantic_sha256=task["evidencePlanSemanticSha256"],
                provenance={
                    "authorityId": payload["authorityId"],
                    "taskMatrixSha256": payload["taskMatrixSha256"],
                    **entry["rawTaskProvenance"],
                },
            )
        )
    for candidate_id, rows in grouped.items():
        rows.sort(key=lambda row: str(row["windowId"]))
        if len(rows) != len(windows):
            raise TemporalQDTailResultIndexError(
                f"indexed rotating evidence candidate {candidate_id} lacks complete panel coverage"
            )
    if set(grouped) != set(candidates):
        raise TemporalQDTailResultIndexError(
            "indexed rotating evidence population coverage mismatch"
        )
    return grouped


def load_indexed_funnel_projections(
    index: Mapping[str, Any], *, window_ids_by_task: Mapping[str, str] | None = None
) -> dict[str, dict[str, Any]]:
    """Return optional compact funnel rows, rebinding a caller's window IDs."""

    payload = validate_tail_result_index(index)
    if payload["funnelProjectionIncluded"] is not True:
        raise TemporalQDTailResultIndexError(
            "tail result index has no funnel projection"
        )
    output: dict[str, dict[str, Any]] = {}
    for entry in payload["entries"]:
        if "rejection" in entry:
            continue
        task_id = str(entry["task"]["taskId"])
        funnel = _mapping(entry["funnelProjection"], name="indexed funnel projection")
        behavior = _mapping(funnel["resultBehavior"], name="indexed funnel behavior")
        if window_ids_by_task is not None:
            window_id = window_ids_by_task.get(task_id)
            if not isinstance(window_id, str) or not window_id:
                raise TemporalQDTailResultIndexError(
                    "indexed funnel caller lacks a window ID for its task"
                )
            behavior["windowId"] = window_id
        output[task_id] = {
            "resultBehavior": behavior,
            "terminalAdjustedConservativeNetR": funnel[
                "terminalAdjustedConservativeNetR"
            ],
            "terminalAdjustedMaxDrawdownR": funnel["terminalAdjustedMaxDrawdownR"],
        }
    return output


def _recursive_size_bytes(value: Any, *, seen: set[int] | None = None) -> int:
    """Estimate retained Python-object bytes without following arbitrary objects."""

    visited = seen if seen is not None else set()
    object_id = id(value)
    if object_id in visited:
        return 0
    visited.add(object_id)
    size = sys.getsizeof(value)
    if isinstance(value, Mapping):
        return size + sum(
            _recursive_size_bytes(key, seen=visited)
            + _recursive_size_bytes(item, seen=visited)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return size + sum(_recursive_size_bytes(item, seen=visited) for item in value)
    return size


def _measure_tail_operation(operation: Any) -> tuple[Any, dict[str, Any]]:
    """Measure one bounded in-process operation without claiming host-wide RSS."""

    gc.collect()
    already_tracing = tracemalloc.is_tracing()
    if already_tracing:
        tracemalloc.reset_peak()
    else:
        tracemalloc.start()
    started = time.perf_counter()
    try:
        result = operation()
        elapsed = time.perf_counter() - started
        current, peak = tracemalloc.get_traced_memory()
    finally:
        if not already_tracing:
            tracemalloc.stop()
    return result, {
        "wallSeconds": elapsed,
        "tracemallocCurrentBytes": current,
        "tracemallocPeakBytes": peak,
        "recursiveRetainedBytes": _recursive_size_bytes(result),
    }


def benchmark_tail_result_index_reuse(
    *,
    result_root: Path | str,
    authority: Mapping[str, Any],
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    panel: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
    legacy_stage_passes: int = 1,
    legacy_provenance_passes: int = 1,
    include_funnel_projection: bool = False,
) -> dict[str, Any]:
    """Compare current raw-loader reuse with one verified compact index.

    The default one stage load plus one provenance load is the post-worker
    current-panel pattern in the rotating supervisor for a single campaign.
    ``rawBlobBytesLogical`` is deliberately *logical* codec input (sum of
    immutable gzip/blob sizes), rather than a host filesystem counter that would
    be distorted by cache state and unrelated workstation activity.

    The returned timings are intentionally labelled provisional.  The helper is
    for a frozen synthetic fixture or captured result root, not an economic run.
    """

    for value, name in (
        (legacy_stage_passes, "legacy_stage_passes"),
        (legacy_provenance_passes, "legacy_provenance_passes"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise TemporalQDTailResultIndexError(f"{name} must be a positive integer")
    if not isinstance(include_funnel_projection, bool):
        raise TemporalQDTailResultIndexError(
            "include_funnel_projection must be boolean"
        )

    from .temporal_discovery_results import (
        load_provenance_bound_window_evidence,
        load_stage_results,
    )

    root = _require_real_directory(Path(result_root), name="result root")

    def legacy_passes() -> dict[str, Any]:
        stage: dict[str, list[dict[str, Any]]] | None = None
        evidence: dict[str, list[dict[str, Any]]] | None = None
        for _ in range(legacy_stage_passes):
            stage = load_stage_results(root)
        for _ in range(legacy_provenance_passes):
            evidence = load_provenance_bound_window_evidence(
                result_root=root,
                task_manifest=task_manifest,
                checkpoint=checkpoint,
                panel=panel,
                candidates=candidates,
            )
        return {"stage": stage, "provenance": evidence}

    legacy_output, legacy_measurement = _measure_tail_operation(legacy_passes)
    legacy_stage_sha = semantic_sha256(legacy_output["stage"])
    legacy_provenance_sha = semantic_sha256(legacy_output["provenance"])
    # The alternatives must not coexist merely because this helper compares
    # them.  Preserve only their semantic parity witnesses before measuring the
    # compact whole phase in the same interpreter.
    del legacy_output
    gc.collect()

    index_file = tail_result_index_path(root)
    preparation_mode = "verified_load" if index_file.exists() else "build"

    def prepare_verified_index() -> dict[str, Any]:
        if preparation_mode == "build":
            return build_tail_result_index(
                result_root=root,
                authority=authority,
                task_manifest=task_manifest,
                checkpoint=checkpoint,
                include_funnel_projection=include_funnel_projection,
            )
        return load_tail_result_index(
            result_root=root,
            authority=authority,
            task_manifest=task_manifest,
            checkpoint=checkpoint,
            verify_source_blobs=True,
        )

    def indexed_whole_phase() -> dict[str, Any]:
        verified_index = prepare_verified_index()
        stage: dict[str, list[dict[str, Any]]] | None = None
        evidence: dict[str, list[dict[str, Any]]] | None = None
        for _ in range(legacy_stage_passes):
            stage = load_indexed_stage_results(verified_index)
        for _ in range(legacy_provenance_passes):
            evidence = load_indexed_provenance_bound_window_evidence(
                index=verified_index,
                panel=panel,
                candidates=candidates,
            )
        # Include the retained index: this is the steady-state shape a real
        # transaction holds while emitting legacy-compatible downstream rows.
        return {"index": verified_index, "stage": stage, "provenance": evidence}

    indexed_output, indexed_whole_measurement = _measure_tail_operation(
        indexed_whole_phase
    )
    verified_index = indexed_output["index"]
    if not isinstance(verified_index, Mapping):  # pragma: no cover - local invariant.
        raise TemporalQDTailResultIndexError("verified tail index is invalid")
    source_blob_bytes = int(verified_index["sourceResultBlobBytes"])
    if (
        semantic_sha256(indexed_output["stage"]) != legacy_stage_sha
        or semantic_sha256(indexed_output["provenance"]) != legacy_provenance_sha
    ):
        raise TemporalQDTailResultIndexError(
            "tail-index benchmark parity drifted from legacy projections"
        )

    # This is deliberately after the full source verification above.  It models
    # a later phase in the same process which only needs the immutable compact
    # projection and does not reopen every gzip result.
    no_raw_index, no_raw_load_measurement = _measure_tail_operation(
        lambda: load_tail_result_index(
            result_root=root,
            authority=authority,
            task_manifest=task_manifest,
            checkpoint=checkpoint,
            verify_source_blobs=False,
        )
    )
    if canonical_json_bytes(no_raw_index) != canonical_json_bytes(verified_index):
        raise TemporalQDTailResultIndexError(
            "tail result index changed after same-process source verification"
        )

    # The whole-phase measurement deliberately retains stage/evidence outputs.
    # Drop them before timing a subsequent pure reuse pass, retaining only the
    # source-verified index exactly as the fast-path rule requires.
    del indexed_output
    gc.collect()

    def indexed_steady_reuse() -> dict[str, Any]:
        stage: dict[str, list[dict[str, Any]]] | None = None
        evidence: dict[str, list[dict[str, Any]]] | None = None
        for _ in range(legacy_stage_passes):
            stage = load_indexed_stage_results(verified_index)
        for _ in range(legacy_provenance_passes):
            evidence = load_indexed_provenance_bound_window_evidence(
                index=verified_index,
                panel=panel,
                candidates=candidates,
            )
        return {"index": verified_index, "stage": stage, "provenance": evidence}

    _steady_output, steady_reuse_measurement = _measure_tail_operation(
        indexed_steady_reuse
    )
    return {
        "schemaVersion": "temporal_qd_tail_result_index_reuse_benchmark_v3",
        "timingsAreProvisional": True,
        "measurementScope": "in-process Python wall time and tracemalloc; no host-wide RSS claim",
        "protocol": {
            "legacyStagePasses": legacy_stage_passes,
            "legacyProvenancePasses": legacy_provenance_passes,
            "supervisorCurrentCampaignRawConsumers": [
                "load_stage_results",
                "load_provenance_bound_window_evidence",
            ],
            "verifySourceBlobsFalseRule": (
                "only after this process has built or loaded the exact index with "
                "verify_source_blobs=True and retains that verified index for same-process reuse"
            ),
        },
        "input": {
            "taskCount": int(verified_index["taskCount"]),
            "sourceRawBlobBytes": source_blob_bytes,
            "indexBytes": tail_result_index_path(root).stat().st_size,
        },
        "legacy": {
            **legacy_measurement,
            "rawBlobBytesLogical": source_blob_bytes
            * (legacy_stage_passes + legacy_provenance_passes),
        },
        "indexed": {
            "preparationMode": preparation_mode,
            "wholePhase": {
                **indexed_whole_measurement,
                "rawBlobBytesLogical": source_blob_bytes,
            },
            "verifiedNoRawLoad": {
                **no_raw_load_measurement,
                "rawBlobBytesLogical": 0,
            },
            "steadyReuse": {
                **steady_reuse_measurement,
                "rawBlobBytesLogical": 0,
            },
            "wholePhaseRawBlobBytesLogical": source_blob_bytes,
        },
        "memoryComparison": {
            "wholePhasePeakMinusLegacyBytes": int(
                indexed_whole_measurement["tracemallocPeakBytes"]
            )
            - int(legacy_measurement["tracemallocPeakBytes"]),
            "wholePhaseRetainedMinusLegacyBytes": int(
                indexed_whole_measurement["recursiveRetainedBytes"]
            )
            - int(legacy_measurement["recursiveRetainedBytes"]),
            "wholePhaseTracedPeakNonRegressing": int(
                indexed_whole_measurement["tracemallocPeakBytes"]
            )
            <= int(legacy_measurement["tracemallocPeakBytes"]),
        },
    }


__all__ = [
    "TAIL_RAW_RESULT_REF_SCHEMA",
    "TAIL_RESULT_ENTRY_SCHEMA",
    "TAIL_RESULT_INDEX_FILENAME",
    "TAIL_RESULT_INDEX_SCHEMA",
    "TAIL_STAGE_PROJECTION_SCHEMA",
    "TemporalQDTailResultIndexError",
    "benchmark_tail_result_index_reuse",
    "build_tail_result_index",
    "load_indexed_funnel_projections",
    "load_indexed_provenance_bound_window_evidence",
    "load_indexed_stage_results",
    "load_tail_result_index",
    "tail_result_index_path",
    "validate_tail_result_index",
]
