import json

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_qd_rust_canonical_topology_package_v1 import (
    _load_launch_gate_evidence,
)


HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
SOURCE_COMMIT = "c" * 40


def _write_sealed(path, payload):
    sealed = dict(payload)
    sealed["reportSha256"] = canonical_sha256(sealed)
    path.write_text(canonical_json(sealed) + "\n", encoding="utf-8")


def _worker_contract():
    return {"workerContractSha256": HASH_A, "imageDigest": HASH_B}


def test_launch_readiness_requires_both_external_gate_reports(tmp_path) -> None:
    missing = _load_launch_gate_evidence(
        conformance_report_path=None,
        portability_report_path=None,
        worker_contract=_worker_contract(),
        source_commit=SOURCE_COMMIT,
    )
    assert missing["launchEvidenceComplete"] is False

    conformance = tmp_path / "conformance.json"
    cases = [
        {
            "case": f"tamper-{index}",
            "fuzzFolioRejected": True,
            "pythonAdmissionRejected": True,
            "rustAdmissionRejected": True,
        }
        for index in range(26)
    ]
    _write_sealed(
        conformance,
        {
            "schemaVersion": "temporal_qd_worker_seam_conformance_report_v2_1",
            "marketDataRead": False,
            "replayExecuted": True,
            "fullWorkerExecutionFixtureCount": 5,
            "exactWorkerResultsAcceptedByFuzzFolio": 5,
            "exactWorkerResultsAcceptedByPythonAdmission": 5,
            "exactWorkerResultsAcceptedByRustAdmission": 5,
            "workerContractHash": HASH_A,
            "workerImageDigest": HASH_B,
            "adversarialCases": cases,
            "adversarialRejectCount": len(cases),
        },
    )
    portability = tmp_path / "portability.json"
    _write_sealed(
        portability,
        {
            "schemaVersion": "temporal_qd_authority_cross_root_determinism_report_v1",
            "authorityProjectionMatches": True,
            "taskManifestMatches": True,
            "candidateIdsMatch": True,
            "artifactRawSha256Matches": True,
            "comparedArtifactCount": 15,
            "hostSpecificPathLeakCount": 0,
            "workerContractSha256": HASH_A,
            "sourceCommit": SOURCE_COMMIT,
        },
    )

    complete = _load_launch_gate_evidence(
        conformance_report_path=conformance,
        portability_report_path=portability,
        worker_contract=_worker_contract(),
        source_commit=SOURCE_COMMIT,
    )
    assert complete["launchEvidenceComplete"] is True


def test_launch_gate_rejects_a_tampered_report(tmp_path) -> None:
    conformance = tmp_path / "conformance.json"
    conformance.write_text(
        json.dumps({"reportSha256": HASH_A, "replayExecuted": True}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="identity is invalid"):
        _load_launch_gate_evidence(
            conformance_report_path=conformance,
            portability_report_path=None,
            worker_contract=_worker_contract(),
            source_commit=SOURCE_COMMIT,
        )
