from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path

import pytest

from autoresearch import temporal_graph_lab_native_probe as probe
from autoresearch import temporal_graph_lab_native_probe_preflight as preflight_module


def _completion(*, result_hash: str = "sha256:" + "a" * 64) -> dict:
    return {
        "task_id": "task-1",
        "lease_id": "lease-1",
        "worker_id": "worker-1",
        "lane_id": "lane-1",
        "attempt_id": "attempt-1",
        "status": "success",
        "accepted_at": 10.0,
        "accepted_at_wall": "2026-01-01T00:00:00Z",
        "result": {"status": "success", "result_hash": result_hash},
    }


def test_completion_identity_ignores_delivery_timestamp_but_not_material_change() -> None:
    first = _completion()
    redelivery = _completion()
    redelivery["accepted_at_wall"] = "2026-01-01T00:00:01Z"
    changed = _completion(result_hash="sha256:" + "b" * 64)

    assert probe._completion_identity(first) == probe._completion_identity(redelivery)
    assert probe._completion_identity(first) != probe._completion_identity(changed)


def test_unrelated_result_backlog_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="unrelated Lab result"):
        probe._assert_no_unrelated_results(
            [{"task_id": "other-task"}],
            task_id="task-1",
        )


def test_local_worker_parity_cross_checks_all_material_identities() -> None:
    local = {
        "sourceProfileSnapshotSha256": "sha256:" + "1" * 64,
        "resolvedProfileSnapshotSha256": "sha256:" + "2" * 64,
        "programSha256": "sha256:" + "3" * 64,
        "streamSha256": "sha256:" + "4" * 64,
        "resultSha256": "sha256:" + "5" * 64,
        "finalCheckpointSha256": "sha256:" + "6" * 64,
        "observationCount": 120,
    }
    execution_evidence = {
        "expected_window_semantic_sha256": "sha256:" + "7" * 64,
        "observed_window_semantic_sha256": "sha256:" + "7" * 64,
        "semantic_contract_id": "lake_window_semantic_digest_v2",
        "lake_window_request": {"pairs": ["EURUSD"], "timeframes": ["M5"]},
        "expected_attestation_sha256": "sha256:" + "8" * 64,
        "observed_attestation_sha256": "sha256:" + "8" * 64,
        "attestation_provenance_matches_freeze": True,
        "attestation_identity_role": "provenance_receipt_not_execution_authority",
    }
    local["executionEvidence"] = execution_evidence
    material = {
        "source_profile_snapshot_sha256": local["sourceProfileSnapshotSha256"],
        "resolved_profile_snapshot_sha256": local["resolvedProfileSnapshotSha256"],
        "program_sha256": local["programSha256"],
        "stream_sha256": local["streamSha256"],
        "replay_result_sha256": local["resultSha256"],
        "final_checkpoint_sha256": local["finalCheckpointSha256"],
        "observation_summary": {"observation_count": 120},
        "execution_evidence": dict(execution_evidence),
    }

    verified = probe._cross_check_local_evidence(local, material)
    assert verified["resultSha256"] == local["resultSha256"]
    assert verified["observationCount"] == 120
    assert (
        verified["executionEvidence"]["localObservedAttestationSha256"]
        == execution_evidence["observed_attestation_sha256"]
    )
    assert (
        verified["executionEvidence"]["workerObservedAttestationSha256"]
        == execution_evidence["observed_attestation_sha256"]
    )
    assert (
        verified["executionEvidence"]["observedAttestationMatchesAcrossLocalWorker"]
        is True
    )


def test_local_worker_parity_allows_receipt_provenance_rotation() -> None:
    local = {
        "sourceProfileSnapshotSha256": "sha256:" + "1" * 64,
        "resolvedProfileSnapshotSha256": "sha256:" + "2" * 64,
        "programSha256": "sha256:" + "3" * 64,
        "streamSha256": "sha256:" + "4" * 64,
        "resultSha256": "sha256:" + "5" * 64,
        "finalCheckpointSha256": "sha256:" + "6" * 64,
        "observationCount": 120,
        "executionEvidence": {
            "expected_window_semantic_sha256": "sha256:" + "7" * 64,
            "observed_window_semantic_sha256": "sha256:" + "7" * 64,
            "semantic_contract_id": "lake_window_semantic_digest_v2",
            "lake_window_request": {"pairs": ["EURUSD"], "timeframes": ["M5"]},
            "expected_attestation_sha256": "sha256:" + "8" * 64,
            "observed_attestation_sha256": "sha256:" + "9" * 64,
            "attestation_provenance_matches_freeze": False,
            "attestation_identity_role": (
                "provenance_receipt_not_execution_authority"
            ),
        },
    }
    worker_execution = dict(local["executionEvidence"])
    worker_execution["observed_attestation_sha256"] = "sha256:" + "a" * 64
    material = {
        "source_profile_snapshot_sha256": local["sourceProfileSnapshotSha256"],
        "resolved_profile_snapshot_sha256": local["resolvedProfileSnapshotSha256"],
        "program_sha256": local["programSha256"],
        "stream_sha256": local["streamSha256"],
        "replay_result_sha256": local["resultSha256"],
        "final_checkpoint_sha256": local["finalCheckpointSha256"],
        "observation_summary": {"observation_count": 120},
        "execution_evidence": worker_execution,
    }

    verified = probe._cross_check_local_evidence(local, material)

    assert (
        verified["executionEvidence"]["observedAttestationMatchesAcrossLocalWorker"]
        is False
    )
    assert (
        verified["executionEvidence"]["localAttestationProvenanceMatchesFreeze"]
        is False
    )
    assert (
        verified["executionEvidence"]["workerAttestationProvenanceMatchesFreeze"]
        is False
    )


def test_preflight_proves_both_incompatible_worker_exclusions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    preparation_path = tmp_path / "preparation.json"
    preparation_path.write_text(
        json.dumps({"schemaVersion": probe.PREPARATION_SCHEMA}),
        encoding="utf-8",
    )
    task = {
        "task_id": "task-1",
        "payload": {
            "required_worker_contract_hash": "sha256:" + "f" * 64,
        },
    }

    class FakeClient:
        def __init__(self, **kwargs):
            self.base_url = "http://gateway"
            self.timeout_seconds = 30.0
            self.session = object()
            self.closed = False

        def health(self):
            return {"ok": True}

        def read_results(self, *, limit):
            return []

        def enqueue_tasks(self, tasks):
            assert tasks == [task]
            return {"enqueued": 1}

        def close(self):
            self.closed = True

    claims: list[dict] = []

    def post_json(client, path, payload, *, token):
        if path == "/register":
            return {"status": "registered", "worker": payload}
        if path == "/claim":
            claims.append(dict(payload))
            return {"status": "no_work"}
        raise AssertionError(path)

    monkeypatch.setattr(preflight_module, "LabGatewayClient", FakeClient)
    monkeypatch.setattr(preflight_module, "_build_task_from_preparation", lambda value: task)
    monkeypatch.setattr(preflight_module, "_post_json", post_json)

    state_path = tmp_path / "state.json"
    result = preflight_module.preflight(
        Namespace(
            preparation=preparation_path,
            gateway_url="http://gateway",
            gateway_token="token",
            state_out=state_path,
            request_timeout_seconds=30.0,
        )
    )

    assert result["incompatibleWorkerExclusion"] == "verified"
    assert len(claims) == 2
    assert claims[0]["contract_hash"] != task["payload"]["required_worker_contract_hash"]
    assert claims[1]["capabilities"] == []
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(state["incompatibleWorkerExclusion"]) == 2
