"""Small, fail-closed bridge for the native v5 directional tail contract.

The Rust campaign seal is the only component that reads a raw rotating result.
Python may freeze this authority into a seal manifest, but must not project a
raw result or synthesize side behavior for the native finalizer.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)


DIRECTIONAL_TAIL_AUTHORITY_SCHEMA = "temporal_qd_v5_directional_tail_authority_v1"
DIRECTIONAL_TAIL_INDEX_SCHEMA = "temporal_qd_tail_result_index_v4"
DIRECTIONAL_TAIL_ENTRY_SCHEMA = "temporal_qd_tail_result_index_entry_v4"
RAW_ROTATING_PROVENANCE_SCHEMA = "temporal_qd_v5_raw_rotating_provenance_v1"


class TemporalQDV5NativeTailError(TemporalDiscoveryContractError):
    """Raised when a v5 directional native-tail binding is malformed."""


def _sha(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:"):
        raise TemporalQDV5NativeTailError(f"{name} must be a sha256 identity")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise TemporalQDV5NativeTailError(f"{name} must be a sha256 identity") from exc
    return value


def build_v5_directional_tail_authority(
    *, runtime_authority_sha256: str, generation_index: int
) -> dict[str, Any]:
    """Return the self-hashed authority required for a v5 native tail.

    This is intentionally not an index builder.  Rust authenticates each raw
    rotating result exactly once and derives the indexed compact projection.
    """

    _sha(runtime_authority_sha256, name="v5 directional tail runtime authority")
    if isinstance(generation_index, bool) or not isinstance(generation_index, int) or generation_index < 1:
        raise TemporalQDV5NativeTailError("v5 directional tail generation index is invalid")
    value: dict[str, Any] = {
        "schemaVersion": DIRECTIONAL_TAIL_AUTHORITY_SCHEMA,
        "generationIndex": generation_index,
        "runtimeAuthoritySha256": runtime_authority_sha256,
        "tailResultIndexSchema": DIRECTIONAL_TAIL_INDEX_SCHEMA,
        "tailResultEntrySchema": DIRECTIONAL_TAIL_ENTRY_SCHEMA,
        "rawRotatingProvenanceSchema": RAW_ROTATING_PROVENANCE_SCHEMA,
    }
    value["tailAuthoritySha256"] = canonical_sha256(value)
    return value


def validate_v5_directional_tail_authority(
    value: Mapping[str, Any], *, runtime_authority_sha256: str, generation_index: int
) -> dict[str, Any]:
    """Validate the exact authority passed to the Rust campaign seal."""

    expected = build_v5_directional_tail_authority(
        runtime_authority_sha256=runtime_authority_sha256,
        generation_index=generation_index,
    )
    if not isinstance(value, Mapping) or set(value) != set(expected):
        raise TemporalQDV5NativeTailError("v5 directional tail authority fields are not exact")
    observed = dict(value)
    supplied = _sha(
        observed.get("tailAuthoritySha256"), name="v5 directional tail authority identity"
    )
    body = dict(observed)
    body.pop("tailAuthoritySha256")
    if canonical_sha256(body) != supplied:
        raise TemporalQDV5NativeTailError("v5 directional tail authority identity mismatch")
    if observed != expected:
        raise TemporalQDV5NativeTailError("v5 directional tail authority binding drifted")
    return observed


def validate_v5_directional_tail_index(
    value: Mapping[str, Any],
    *,
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the native-only v4 compact result authority.

    This deliberately does *not* inflate ``stageProjection`` or reopen a raw
    blob.  Those expensive checks occurred inside the Rust campaign seal; this
    bridge admits only its self-hashed compact receipt input.
    """

    runtime = _sha(
        authority.get("runtimeAuthoritySha256"), name="v5 directional tail runtime authority"
    )
    generation = authority.get("generationIndex")
    sealed_authority = validate_v5_directional_tail_authority(
        authority, runtime_authority_sha256=runtime, generation_index=generation
    )
    if not isinstance(value, Mapping):
        raise TemporalQDV5NativeTailError("v5 directional tail index is invalid")
    required = {
        "schemaVersion", "authorityId", "authoritySha256", "taskMatrixSha256",
        "taskManifestSha256", "checkpointSha256", "taskCount",
        "funnelProjectionIncluded", "sourceResultBlobBytes", "entries",
        "tailResultIndexSha256",
    }
    index = dict(value)
    if set(index) != required or index.get("schemaVersion") != DIRECTIONAL_TAIL_INDEX_SCHEMA:
        raise TemporalQDV5NativeTailError("v5 directional tail index fields are not exact")
    supplied = _sha(index.get("tailResultIndexSha256"), name="v5 directional tail index identity")
    body = dict(index)
    body.pop("tailResultIndexSha256")
    if canonical_sha256(body) != supplied:
        raise TemporalQDV5NativeTailError("v5 directional tail index identity mismatch")
    if isinstance(index.get("taskCount"), bool) or not isinstance(index.get("taskCount"), int) or index["taskCount"] < 1:
        raise TemporalQDV5NativeTailError("v5 directional tail task count is invalid")
    entries = index.get("entries")
    if not isinstance(entries, list) or len(entries) != index["taskCount"]:
        raise TemporalQDV5NativeTailError("v5 directional tail entry coverage is invalid")
    if not isinstance(index.get("funnelProjectionIncluded"), bool):
        raise TemporalQDV5NativeTailError("v5 directional tail funnel feature flag is invalid")
    for field in ("authoritySha256", "taskMatrixSha256", "taskManifestSha256", "checkpointSha256"):
        _sha(index.get(field), name=f"v5 directional tail {field}")
    expected_task_ids: set[str] = set()
    for entry in entries:
        _validate_v5_directional_tail_entry(
            entry, include_funnel=index["funnelProjectionIncluded"]
        )
        task_id = entry["task"]["taskId"]
        if task_id in expected_task_ids:
            raise TemporalQDV5NativeTailError("v5 directional tail repeats a task")
        expected_task_ids.add(task_id)
    # Preserve this explicit binding for callers constructing a later native
    # receipt; it prevents an accidentally supplied v4-shaped document from
    # becoming a generic indexed-tail fallback.
    if sealed_authority["tailResultIndexSchema"] != index["schemaVersion"]:
        raise TemporalQDV5NativeTailError("v5 directional tail authority/index schema drifted")
    return index


def _validate_v5_directional_tail_entry(value: Any, *, include_funnel: bool) -> None:
    if not isinstance(value, Mapping):
        raise TemporalQDV5NativeTailError("v5 directional tail entry is invalid")
    entry = dict(value)
    required = {
        "schemaVersion", "task", "rawResultRef", "rawTaskProvenance", "entrySha256"
    }
    rejected = "rejection" in entry
    if rejected:
        required.add("rejection")
    else:
        required.update({"stageProjection", "rotatingEvidenceMetrics", "rawRotatingProvenance"})
        if include_funnel:
            required.add("funnelProjection")
    if set(entry) != required or entry.get("schemaVersion") != DIRECTIONAL_TAIL_ENTRY_SCHEMA:
        raise TemporalQDV5NativeTailError("v5 directional tail entry fields are not exact")
    supplied = _sha(entry.get("entrySha256"), name="v5 directional tail entry identity")
    body = dict(entry)
    body.pop("entrySha256")
    if canonical_sha256(body) != supplied:
        raise TemporalQDV5NativeTailError("v5 directional tail entry identity mismatch")
    task = entry.get("task")
    raw = entry.get("rawResultRef")
    source = entry.get("rawTaskProvenance")
    if not isinstance(task, Mapping) or not isinstance(raw, Mapping) or not isinstance(source, Mapping):
        raise TemporalQDV5NativeTailError("v5 directional tail task provenance is invalid")
    if set(task) != {
        "taskId", "candidateId", "analysisWindowStart", "analysisWindowEnd",
        "evidencePlanSemanticSha256", "taskPayloadSha256",
    } or any(not isinstance(task.get(key), str) or not task[key] for key in (
        "taskId", "candidateId", "analysisWindowStart", "analysisWindowEnd"
    )):
        raise TemporalQDV5NativeTailError("v5 directional tail task is invalid")
    _sha(task.get("evidencePlanSemanticSha256"), name="v5 directional task plan")
    _sha(task.get("taskPayloadSha256"), name="v5 directional task payload")
    if set(raw) != {
        "schemaVersion", "relativePath", "codec", "resultSha256", "semanticSizeBytes",
        "uncompressedSha256", "uncompressedSizeBytes", "blobSha256", "blobSizeBytes",
    } or raw.get("schemaVersion") != "temporal_qd_tail_raw_result_ref_v1" or raw.get("codec") != "gzip-json-v1":
        raise TemporalQDV5NativeTailError("v5 directional raw result reference is invalid")
    for field in ("resultSha256", "uncompressedSha256", "blobSha256"):
        _sha(raw.get(field), name=f"v5 directional raw {field}")
    if any(isinstance(raw.get(field), bool) or not isinstance(raw.get(field), int) or raw[field] < 0 for field in (
        "semanticSizeBytes", "uncompressedSizeBytes", "blobSizeBytes"
    )):
        raise TemporalQDV5NativeTailError("v5 directional raw result sizes are invalid")
    if source != {"taskId": task["taskId"], "resultSha256": raw["resultSha256"]}:
        raise TemporalQDV5NativeTailError("v5 directional tail task provenance drifted")
    if rejected:
        return
    projection = entry.get("stageProjection")
    metrics = entry.get("rotatingEvidenceMetrics")
    if not isinstance(projection, Mapping) or set(projection) != {
        "schemaVersion", "codec", "semanticSha256", "semanticSizeBytes", "blobBase64"
    } or projection.get("schemaVersion") != "temporal_qd_tail_stage_projection_v1" or projection.get("codec") != "gzip-canonical-json-v1":
        raise TemporalQDV5NativeTailError("v5 directional stage projection is invalid")
    _sha(projection.get("semanticSha256"), name="v5 directional stage projection")
    if isinstance(projection.get("semanticSizeBytes"), bool) or not isinstance(projection.get("semanticSizeBytes"), int) or projection["semanticSizeBytes"] < 1 or not isinstance(projection.get("blobBase64"), str) or not projection["blobBase64"]:
        raise TemporalQDV5NativeTailError("v5 directional stage projection metadata is invalid")
    if not isinstance(metrics, Mapping) or set(metrics) != {
        "conservativeNetR", "noCostNetR", "maxDrawdownR", "closedTrades", "observations",
        "v3Admissible", "resolvedProgramSha256", "resolvedProfileSnapshotSha256",
        "sourceProfileSnapshotSha256",
    } or metrics.get("v3Admissible") is not True:
        raise TemporalQDV5NativeTailError("v5 directional rotating metrics are invalid")
    for field in ("resolvedProgramSha256", "resolvedProfileSnapshotSha256", "sourceProfileSnapshotSha256"):
        _sha(metrics.get(field), name=f"v5 directional rotating {field}")
    provenance = entry.get("rawRotatingProvenance")
    if not isinstance(provenance, Mapping) or set(provenance) != {
        "schemaVersion", "taskId", "resultSha256", "observationStreamSha256",
        "conservativeReplayStreamSha256", "realizedBehaviorSha256",
    }:
        raise TemporalQDV5NativeTailError("v5 raw rotating provenance fields are not exact")
    if (
        provenance.get("schemaVersion") != RAW_ROTATING_PROVENANCE_SCHEMA
        or provenance.get("taskId") != task["taskId"]
        or provenance.get("resultSha256") != raw["resultSha256"]
    ):
        raise TemporalQDV5NativeTailError("v5 raw rotating provenance task binding drifted")
    for field in ("observationStreamSha256", "conservativeReplayStreamSha256", "realizedBehaviorSha256"):
        _sha(provenance.get(field), name=f"v5 raw rotating provenance {field}")


def v5_directional_compact_window_evidence(
    *, index: Mapping[str, Any], authority: Mapping[str, Any], panel: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Build rotating evidence from v4 metrics without a Python tail reducer.

    The helper does not decode ``stageProjection``.  Direction-attributable
    behavior remains inside the native sealed member/provenance contract; this
    adapter passes only native-attested rotating metrics into the prefinalizer
    bridge.
    """

    from .temporal_qd_rotating_evidence import build_candidate_window_evidence

    indexed = validate_v5_directional_tail_index(index, authority=authority)
    windows = {
        (str(row.get("analysisWindowStart")), str(row.get("analysisWindowEnd"))): row
        for row in panel.get("windows") or []
        if isinstance(row, Mapping)
    }
    if not windows or len(windows) != len(panel.get("windows") or []):
        raise TemporalQDV5NativeTailError("v5 directional panel windows are invalid")
    output: dict[str, list[dict[str, Any]]] = {}
    for entry in indexed["entries"]:
        if "rejection" in entry:
            continue
        task = entry["task"]
        candidate_id = task["candidateId"]
        candidate = candidates.get(candidate_id)
        if candidate is None:
            continue
        key = (task["analysisWindowStart"], task["analysisWindowEnd"])
        window = windows.get(key)
        if window is None:
            raise TemporalQDV5NativeTailError("v5 directional tail result is outside its panel")
        metrics = entry["rotatingEvidenceMetrics"]
        if metrics.get("sourceProfileSnapshotSha256") != candidate.get("profileSnapshotSha256"):
            raise TemporalQDV5NativeTailError("v5 directional source profile binding drifted")
        output.setdefault(candidate_id, []).append(build_candidate_window_evidence(
            candidate=candidate, panel=panel, window=window, metrics=metrics,
            evidence_plan_semantic_sha256=task["evidencePlanSemanticSha256"],
            provenance={
                "authorityId": indexed["authorityId"],
                "taskMatrixSha256": indexed["taskMatrixSha256"],
                **entry["rawTaskProvenance"],
                "rawRotatingProvenanceSha256": canonical_sha256(entry["rawRotatingProvenance"]),
            },
        ))
    for candidate_id, rows in output.items():
        rows.sort(key=lambda row: str(row["windowId"]))
        if len(rows) != len(windows):
            raise TemporalQDV5NativeTailError(f"v5 directional candidate {candidate_id} lacks complete panel coverage")
    if set(output) != set(candidates):
        raise TemporalQDV5NativeTailError("v5 directional tail population coverage mismatch")
    return output


__all__ = [
    "DIRECTIONAL_TAIL_AUTHORITY_SCHEMA",
    "DIRECTIONAL_TAIL_ENTRY_SCHEMA",
    "DIRECTIONAL_TAIL_INDEX_SCHEMA",
    "RAW_ROTATING_PROVENANCE_SCHEMA",
    "TemporalQDV5NativeTailError",
    "build_v5_directional_tail_authority",
    "validate_v5_directional_tail_index",
    "validate_v5_directional_tail_authority",
    "v5_directional_compact_window_evidence",
]
