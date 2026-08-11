from __future__ import annotations

import copy

import pytest

from autoresearch import temporal_qd_v5_native_tail as tail


HASH = "sha256:" + "a" * 64


def _v4_index(authority: dict) -> dict:
    entry = {
        "schemaVersion": tail.DIRECTIONAL_TAIL_ENTRY_SCHEMA,
        "task": {"taskId": "task-1", "candidateId": "qd_fixture", "analysisWindowStart": "a", "analysisWindowEnd": "b", "evidencePlanSemanticSha256": HASH, "taskPayloadSha256": HASH},
        "rawResultRef": {
            "schemaVersion": "temporal_qd_tail_raw_result_ref_v1",
            "relativePath": "results/task-1.json.gz", "codec": "gzip-json-v1",
            "resultSha256": HASH, "semanticSizeBytes": 1, "uncompressedSha256": HASH,
            "uncompressedSizeBytes": 1, "blobSha256": HASH, "blobSizeBytes": 1,
        },
        "rawTaskProvenance": {"taskId": "task-1", "resultSha256": HASH},
        "rawRotatingProvenance": {"schemaVersion": tail.RAW_ROTATING_PROVENANCE_SCHEMA, "taskId": "task-1", "resultSha256": HASH, "observationStreamSha256": HASH, "conservativeReplayStreamSha256": HASH, "realizedBehaviorSha256": HASH},
        "stageProjection": {
            "schemaVersion": "temporal_qd_tail_stage_projection_v1",
            "codec": "gzip-canonical-json-v1", "semanticSha256": HASH,
            "semanticSizeBytes": 1, "blobBase64": "AA==",
        },
        "rotatingEvidenceMetrics": {
            "conservativeNetR": 0.0, "noCostNetR": 0.0, "maxDrawdownR": 0.0,
            "closedTrades": 0, "observations": 0, "v3Admissible": True,
            "resolvedProgramSha256": HASH, "resolvedProfileSnapshotSha256": HASH,
            "sourceProfileSnapshotSha256": HASH,
        },
    }
    from autoresearch.temporal_discovery_base import canonical_sha256

    entry["entrySha256"] = canonical_sha256(entry)
    index = {
        "schemaVersion": tail.DIRECTIONAL_TAIL_INDEX_SCHEMA,
        "authorityId": "fixture", "authoritySha256": HASH, "taskMatrixSha256": HASH,
        "taskManifestSha256": HASH, "checkpointSha256": HASH, "taskCount": 1,
        "funnelProjectionIncluded": False, "sourceResultBlobBytes": 1, "entries": [entry],
    }
    index["tailResultIndexSha256"] = canonical_sha256(index)
    return index


def test_directional_tail_authority_is_self_hashed_and_runtime_bound() -> None:
    authority = tail.build_v5_directional_tail_authority(
        runtime_authority_sha256=HASH, generation_index=3
    )
    assert tail.validate_v5_directional_tail_authority(
        authority, runtime_authority_sha256=HASH, generation_index=3
    ) == authority


def test_directional_tail_authority_rejects_self_rehashed_contract_downgrade() -> None:
    authority = tail.build_v5_directional_tail_authority(
        runtime_authority_sha256=HASH, generation_index=3
    )
    tampered = copy.deepcopy(authority)
    tampered["tailResultEntrySchema"] = "temporal_qd_tail_result_index_entry_v3"
    body = dict(tampered)
    body.pop("tailAuthoritySha256")
    from autoresearch.temporal_discovery_base import canonical_sha256

    tampered["tailAuthoritySha256"] = canonical_sha256(body)
    with pytest.raises(tail.TemporalQDV5NativeTailError, match="binding drifted"):
        tail.validate_v5_directional_tail_authority(
            tampered, runtime_authority_sha256=HASH, generation_index=3
        )


def test_directional_v4_index_rejects_self_rehashed_v3_downgrade() -> None:
    authority = tail.build_v5_directional_tail_authority(
        runtime_authority_sha256=HASH, generation_index=3
    )
    index = _v4_index(authority)
    assert tail.validate_v5_directional_tail_index(index, authority=authority) == index
    tampered = copy.deepcopy(index)
    tampered["schemaVersion"] = "temporal_qd_tail_result_index_v3"
    body = dict(tampered)
    body.pop("tailResultIndexSha256")
    from autoresearch.temporal_discovery_base import canonical_sha256

    tampered["tailResultIndexSha256"] = canonical_sha256(body)
    with pytest.raises(tail.TemporalQDV5NativeTailError, match="fields are not exact"):
        tail.validate_v5_directional_tail_index(tampered, authority=authority)
