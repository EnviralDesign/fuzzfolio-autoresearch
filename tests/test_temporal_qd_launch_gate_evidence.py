import json
import subprocess

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_qd_rust_canonical_topology_package_v1 import (
    ROOT,
    _git_blob_sha256,
    _load_launch_gate_evidence,
    _sha_file,
)


HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
SOURCE_COMMIT = subprocess.run(
    ["git", "rev-parse", "HEAD"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()


def _write_sealed(path, payload):
    sealed = dict(payload)
    sealed["reportSha256"] = canonical_sha256(sealed)
    path.write_text(canonical_json(sealed) + "\n", encoding="utf-8")


def _worker_contract():
    return {"workerContractSha256": HASH_A, "imageDigest": HASH_B}


def test_launch_readiness_requires_both_external_gate_reports(tmp_path) -> None:
    missing = _load_launch_gate_evidence(
        conformance_report_path=None,
        production_admission_report_path=None,
        portability_report_path=None,
        worker_contract=_worker_contract(),
        source_commit=SOURCE_COMMIT,
    )
    assert missing["launchEvidenceComplete"] is False

    conformance = tmp_path / "conformance.json"
    cases = [
        {
            "case": f"tamper-{index}",
            "taskSha256": HASH_A,
            "resultSha256": HASH_B,
            "fuzzFolioRejected": True,
            "pythonAdmissionRejected": True,
            "rustAdmissionRejected": True,
            "productionCampaignSealRejected": True,
        }
        for index in range(26)
    ]
    exact_fixtures = [
        {
            "candidateId": f"candidate-{index}",
            "taskId": f"task-{index}",
            "taskSha256": HASH_A,
            "resultSha256": HASH_B,
        }
        for index in range(12)
    ]
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
    production = tmp_path / "production-admission.json"
    _write_sealed(
        production,
        {
            "schemaVersion": "temporal_qd_production_result_admission_report_v1",
            "sourceCommit": SOURCE_COMMIT,
            "sourceHashes": {
                "campaignAdmissionAdapter": _git_blob_sha256(
                    SOURCE_COMMIT,
                    ROOT
                    / "rust/temporal-qd/crates/qd-campaign-seal/src/bin/temporal-qd-campaign-admission-jsonl.rs",
                ),
                "campaignSeal": _git_blob_sha256(
                    SOURCE_COMMIT,
                    ROOT / "rust/temporal-qd/crates/qd-campaign-seal/src/lib.rs"
                ),
                "gatewayDispatch": _git_blob_sha256(
                    SOURCE_COMMIT,
                    ROOT / "rust/temporal-qd/crates/qd-gateway-dispatch/src/lib.rs"
                ),
                "sharedReceiptValidator": _git_blob_sha256(
                    SOURCE_COMMIT,
                    ROOT / "rust/temporal-qd/crates/qd-campaign-freeze/src/lib.rs"
                ),
            },
            "productionAdmissionPolicy": "campaign_seal_shared_receipt_v2_2",
            "productionBinaryHashes": {
                "temporal-qd-campaign-admission-jsonl": HASH_A,
                "temporal-qd-precompiled-receipt-jsonl": HASH_B,
            },
            "workerContractHash": HASH_A,
            "workerImageDigest": HASH_B,
            "marketDataRead": False,
            "gatewayNetworkAccess": False,
            "taskDispatchCount": 0,
            "productionCampaignSealExactAcceptCount": 12,
            "productionCampaignSealAdversarialRejectCount": 26,
            "productionGatewayDispatchFixturePassed": True,
            "productionOfflineSealFixturePassed": True,
            "exactFixtures": exact_fixtures,
            "adversarialCases": [
                {
                    "case": f"tamper-{index}",
                    "taskSha256": HASH_A,
                    "resultSha256": HASH_B,
                    "productionCampaignSealRejected": True,
                }
                for index in range(26)
            ],
        },
    )
    production_payload = json.loads(production.read_text(encoding="utf-8"))
    _write_sealed(
        conformance,
        {
            "schemaVersion": "temporal_qd_worker_seam_conformance_report_v2_2",
            "marketDataRead": False,
            "replayExecuted": True,
            "fullWorkerExecutionFixtureCount": 12,
            "exactWorkerResultsAcceptedByFuzzFolio": 12,
            "exactWorkerResultsAcceptedByPythonAdmission": 12,
            "exactWorkerResultsAcceptedByRustAdmission": 12,
            "productionCampaignSealExactAcceptCount": 12,
            "productionCampaignSealAdversarialRejectCount": 26,
            "productionGatewayDispatchFixturePassed": True,
            "productionOfflineSealFixturePassed": True,
            "workerContractHash": HASH_A,
            "workerImageDigest": HASH_B,
            "exactFixtures": exact_fixtures,
            "productionAdmissionReport": {
                "logicalId": production.name,
                "rawSha256": _sha_file(production),
                "reportSha256": production_payload["reportSha256"],
            },
            "adversarialCases": cases,
            "adversarialRejectCount": len(cases),
        },
    )

    complete = _load_launch_gate_evidence(
        conformance_report_path=conformance,
        production_admission_report_path=production,
        portability_report_path=portability,
        worker_contract=_worker_contract(),
        source_commit=SOURCE_COMMIT,
    )
    assert complete["launchEvidenceComplete"] is True
    assert complete["productionCampaignSealAdmissionPassed"] is True
    assert complete["productionGatewayDispatchAdmissionPassed"] is True
    assert complete["productionOfflineCampaignSealPassed"] is True
    assert complete["allExactV2WorkerResultsAcceptedThroughProductionAdmission"] is True
    assert complete["allAdversarialV2ResultsRejectedThroughProductionAdmission"] is True

    mismatched_production = tmp_path / "mismatched-production-admission.json"
    mismatched_payload = dict(production_payload)
    mismatched_payload.pop("reportSha256")
    mismatched_payload["productionAdmissionPolicy"] = "different_valid_report"
    _write_sealed(mismatched_production, mismatched_payload)
    with pytest.raises(RuntimeError, match="not bound to worker-seam conformance"):
        _load_launch_gate_evidence(
            conformance_report_path=conformance,
            production_admission_report_path=mismatched_production,
            portability_report_path=portability,
            worker_contract=_worker_contract(),
            source_commit=SOURCE_COMMIT,
        )


def test_launch_gate_rejects_a_tampered_report(tmp_path) -> None:
    conformance = tmp_path / "conformance.json"
    conformance.write_text(
        json.dumps({"reportSha256": HASH_A, "replayExecuted": True}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="identity is invalid"):
        _load_launch_gate_evidence(
            conformance_report_path=conformance,
            production_admission_report_path=None,
            portability_report_path=None,
            worker_contract=_worker_contract(),
            source_commit=SOURCE_COMMIT,
        )
