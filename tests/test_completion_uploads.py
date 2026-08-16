from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import requests

from autoresearch.completion_uploads import (
    COMPLETION_UPLOAD_SCHEMA_VERSION,
    CompletionUploadError,
)
from autoresearch.ephemeral_worker_sessions import ephemeral_http_operation_allowed
from autoresearch.play_hand_lab_gateway import (
    LabGatewayConfig,
    LabTask,
    PlayHandLabGateway,
    _start_uvicorn_gateway_thread,
)


def _claimed_gateway(tmp_path: Path, *, max_upload_bytes: int = 4 * 1024 * 1024) -> tuple[PlayHandLabGateway, dict]:
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            completion_upload_root=str(tmp_path / "uploads"),
            max_completion_upload_bytes=max_upload_bytes,
            max_completion_upload_spool_bytes=max_upload_bytes + 1024,
            max_completion_upload_chunk_bytes=1024 * 1024,
        )
    )
    gateway.enqueue(LabTask(task_id="task-1", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    assert claim["status"] == "leased"
    return gateway, claim


def _envelope(result: dict) -> bytes:
    return json.dumps(
        {
            "schema_version": COMPLETION_UPLOAD_SCHEMA_VERSION,
            "worker_id": "worker-1",
            "status": "success",
            "result": result,
        },
        separators=(",", ":"),
    ).encode()


def test_completion_upload_is_out_of_order_resumable_idempotent_and_atomic(tmp_path: Path) -> None:
    gateway, claim = _claimed_gateway(tmp_path)
    lease_id = str(claim["lease_id"])
    payload = _envelope({"score": 42.0, "blob": "x" * 2048})
    chunk_size = 701
    chunks = [payload[offset : offset + chunk_size] for offset in range(0, len(payload), chunk_size)]
    begin = gateway.begin_completion_upload(
        "worker-1",
        lease_id,
        schema_version=COMPLETION_UPLOAD_SCHEMA_VERSION,
        status="success",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        chunk_size_bytes=chunk_size,
        chunk_count=len(chunks),
    )
    upload_id = str(begin["upload_id"])

    order = list(reversed(range(len(chunks))))
    first_index = order[0]
    first = gateway.write_completion_upload_chunk(
        "worker-1",
        lease_id,
        upload_id,
        index=first_index,
        payload=chunks[first_index],
        sha256=hashlib.sha256(chunks[first_index]).hexdigest(),
    )
    duplicate = gateway.write_completion_upload_chunk(
        "worker-1",
        lease_id,
        upload_id,
        index=first_index,
        payload=chunks[first_index],
        sha256=hashlib.sha256(chunks[first_index]).hexdigest(),
    )
    assert first["status"] == "accepted"
    assert duplicate["status"] == "duplicate"
    with pytest.raises(CompletionUploadError, match="completion_upload_incomplete"):
        gateway.finalize_completion_upload("worker-1", lease_id, upload_id)

    resumed = gateway.begin_completion_upload(
        "worker-1",
        lease_id,
        schema_version=COMPLETION_UPLOAD_SCHEMA_VERSION,
        status="success",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        chunk_size_bytes=chunk_size,
        chunk_count=len(chunks),
    )
    assert resumed["status"] == "resumed"
    assert resumed["received_chunk_indices"] == [first_index]
    for index in order[1:]:
        gateway.write_completion_upload_chunk(
            "worker-1",
            lease_id,
            upload_id,
            index=index,
            payload=chunks[index],
            sha256=hashlib.sha256(chunks[index]).hexdigest(),
        )

    completed = gateway.finalize_completion_upload("worker-1", lease_id, upload_id)
    assert completed["status"] == "accepted"
    assert gateway.read_results(limit=1)[0]["result"]["score"] == 42.0
    assert gateway.snapshot()["completion_uploads"]["active_uploads"] == 0
    repeated = gateway.finalize_completion_upload("worker-1", lease_id, upload_id)
    assert repeated["status"] == "duplicate"


def test_completion_upload_rejects_tamper_owner_and_identity_substitution(tmp_path: Path) -> None:
    gateway, claim = _claimed_gateway(tmp_path)
    lease_id = str(claim["lease_id"])
    payload = _envelope({"blob": "payload"})
    digest = hashlib.sha256(payload).hexdigest()
    begin = gateway.begin_completion_upload(
        "worker-1",
        lease_id,
        schema_version=COMPLETION_UPLOAD_SCHEMA_VERSION,
        status="success",
        size_bytes=len(payload),
        sha256=digest,
        chunk_size_bytes=len(payload),
        chunk_count=1,
    )
    upload_id = str(begin["upload_id"])
    with pytest.raises(CompletionUploadError, match="lease_lost"):
        gateway.write_completion_upload_chunk(
            "worker-2",
            lease_id,
            upload_id,
            index=0,
            payload=payload,
            sha256=digest,
        )
    with pytest.raises(CompletionUploadError, match="chunk_sha256_mismatch"):
        gateway.write_completion_upload_chunk(
            "worker-1",
            lease_id,
            upload_id,
            index=0,
            payload=payload,
            sha256="0" * 64,
        )
    with pytest.raises(CompletionUploadError, match="identity_conflict"):
        gateway.begin_completion_upload(
            "worker-1",
            lease_id,
            schema_version=COMPLETION_UPLOAD_SCHEMA_VERSION,
            status="success",
            size_bytes=len(payload),
            sha256="1" * 64,
            chunk_size_bytes=len(payload),
            chunk_count=1,
        )


def test_ephemeral_worker_permission_is_exact_for_completion_upload_routes() -> None:
    assert ephemeral_http_operation_allowed("POST", "/leases/l1/completion-upload")
    assert ephemeral_http_operation_allowed(
        "PUT", "/leases/l1/completion-upload/u1/chunks/0"
    )
    assert ephemeral_http_operation_allowed(
        "POST", "/leases/l1/completion-upload/u1/finalize"
    )
    assert not ephemeral_http_operation_allowed(
        "GET", "/leases/l1/completion-upload/u1/chunks/0"
    )
    assert not ephemeral_http_operation_allowed(
        "PUT", "/leases/l1/completion-upload/u1/chunks/not-an-index"
    )
    assert not ephemeral_http_operation_allowed(
        "POST", "/leases/l1/completion-upload/u1/delete"
    )


def test_real_http_completion_upload_accepts_more_than_100_mib(tmp_path: Path) -> None:
    logical_blob_bytes = 101 * 1024 * 1024
    max_upload_bytes = 110 * 1024 * 1024
    chunk_size = 8 * 1024 * 1024
    gateway = PlayHandLabGateway(
        LabGatewayConfig(
            completion_upload_root=str(tmp_path / "gateway-uploads"),
            max_completion_upload_bytes=max_upload_bytes,
            max_completion_upload_spool_bytes=max_upload_bytes + 1024,
            max_completion_upload_chunk_bytes=chunk_size,
        )
    )
    gateway.enqueue(LabTask(task_id="task-large", lane_id="lane-1", attempt_id="attempt-1"))
    gateway.register_worker("worker-1")
    claim = gateway.claim("worker-1")
    lease_id = str(claim["lease_id"])

    payload_path = tmp_path / "large-completion.json"
    prefix = (
        f'{{"schema_version":"{COMPLETION_UPLOAD_SCHEMA_VERSION}",'
        '"worker_id":"worker-1","status":"success","result":{"blob":"'
    ).encode()
    suffix = b'"}}'
    digest = hashlib.sha256()
    with payload_path.open("wb") as handle:
        for part in (prefix,):
            handle.write(part)
            digest.update(part)
        remaining = logical_blob_bytes
        block = b"x" * (1024 * 1024)
        while remaining:
            part = block[: min(len(block), remaining)]
            handle.write(part)
            digest.update(part)
            remaining -= len(part)
        handle.write(suffix)
        digest.update(suffix)
    size_bytes = payload_path.stat().st_size
    assert size_bytes > 100 * 1024 * 1024
    chunk_count = (size_bytes + chunk_size - 1) // chunk_size

    server, thread, base_url = _start_uvicorn_gateway_thread(
        gateway,
        token="secret",
        max_body_bytes=10 * 1024 * 1024,
    )
    headers = {"Authorization": "Bearer secret"}
    try:
        begin = requests.post(
            f"{base_url}/leases/{lease_id}/completion-upload",
            json={
                "schema_version": COMPLETION_UPLOAD_SCHEMA_VERSION,
                "worker_id": "worker-1",
                "status": "success",
                "size_bytes": size_bytes,
                "sha256": digest.hexdigest(),
                "chunk_size_bytes": chunk_size,
                "chunk_count": chunk_count,
            },
            headers=headers,
            timeout=10,
        )
        assert begin.status_code == 200, begin.text
        upload_id = str(begin.json()["upload_id"])
        with payload_path.open("rb") as handle:
            for index in range(chunk_count):
                chunk = handle.read(chunk_size)
                response = requests.put(
                    f"{base_url}/leases/{lease_id}/completion-upload/{upload_id}/chunks/{index}",
                    data=chunk,
                    headers={
                        **headers,
                        "content-type": "application/octet-stream",
                        "x-fuzzfolio-worker-id": "worker-1",
                        "x-fuzzfolio-chunk-sha256": hashlib.sha256(chunk).hexdigest(),
                    },
                    timeout=30,
                )
                assert response.status_code == 200, response.text
        finalized = requests.post(
            f"{base_url}/leases/{lease_id}/completion-upload/{upload_id}/finalize",
            json={"worker_id": "worker-1"},
            headers=headers,
            timeout=60,
        )
        assert finalized.status_code == 200, finalized.text
        assert finalized.json()["status"] == "accepted"
        result = gateway.read_results(limit=1)[0]
        assert len(result["result"]["blob"]) == logical_blob_bytes
        assert gateway.snapshot()["completion_uploads"]["active_uploads"] == 0
    finally:
        server.should_exit = True
        thread.join(timeout=10)
