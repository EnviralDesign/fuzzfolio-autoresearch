from __future__ import annotations

import hashlib
import json

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_rust_canonical_topology_package_v1 import OUTPUT_V2


WORKER_CONTRACT_SHA256 = "sha256:ae5d0e53aa19e1e241468c009e248457560ca63e2e3d785854750b028736c9df"
WORKER_IMAGE_DIGEST = "sha256:1817ddc68b55433bb81c59572e51d5dddc40e2a95ac9004fafee979adbb913fe"
WORKER_SOURCE_COMMIT = "0fbe84a9f7b73b97789c8370b268f4d01eeb37ce"
PRECOMPILED_CAPABILITY = "temporal_qd_precompiled_profile_execution_v1"


def _load(name: str) -> dict:
    return json.loads((OUTPUT_V2 / name).read_text(encoding="utf-8"))


def _assert_self_hash(value: dict, field: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field)
    assert canonical_sha256(unsigned) == stored


def test_v2_package_is_digest_bound_no_dispatch_and_self_consistent() -> None:
    tasks = _load("inspected-task-index-v1.json")
    go_nogo = _load("topology-launch-go-nogo-v1.json")
    package = _load("topology-launch-package-manifest-v1.json")

    _assert_self_hash(tasks, "taskIndexSha256")
    _assert_self_hash(go_nogo, "goNogoSha256")
    _assert_self_hash(package, "packageSha256")

    assert package["schemaVersion"] == "temporal_qd_topology_no_dispatch_launch_package_v3"
    assert package["dispatchEnabled"] is False
    assert package["inspectedTaskCount"] == 144
    assert package["workerContract"]["workerContractSha256"] == WORKER_CONTRACT_SHA256
    assert package["workerContract"]["imageDigest"] == WORKER_IMAGE_DIGEST
    assert package["workerContract"]["sourceGitCommit"] == WORKER_SOURCE_COMMIT
    assert PRECOMPILED_CAPABILITY in package["workerContract"]["capabilities"]

    assert tasks["taskCount"] == len(tasks["tasks"]) == 144
    assert len({(row["candidateId"], row["windowId"]) for row in tasks["tasks"]}) == 144
    assert {row["requiredWorkerContractSha256"] for row in tasks["tasks"]} == {
        WORKER_CONTRACT_SHA256
    }
    assert {row["requiredWorkerImageDigest"] for row in tasks["tasks"]} == {
        WORKER_IMAGE_DIGEST
    }

    assert go_nogo["readyForTopologyCaseStudyLaunch"] is True
    assert go_nogo["schemaVersion"] == "temporal_qd_topology_launch_go_nogo_v3"
    assert go_nogo["gates"]["allTasksUseCandidateWindowJobV2"] is True
    assert go_nogo["gates"]["dedicatedWorkerCapabilityRequired"] is True
    assert go_nogo["gates"]["immutableWorkerImageDigestBound"] is True
    assert go_nogo["gates"]["typedExecutionReceiptRequired"] is True
    assert go_nogo["gates"]["realWorkerReplayConformancePassed"] is True
    assert go_nogo["gates"]["expandedAdversarialAdmissionPassed"] is True
    assert go_nogo["gates"]["productionCampaignSealAdmissionPassed"] is True
    assert go_nogo["gates"]["productionGatewayDispatchAdmissionPassed"] is True
    assert go_nogo["gates"]["productionOfflineCampaignSealPassed"] is True
    assert (
        go_nogo["gates"][
            "allExactV2WorkerResultsAcceptedThroughProductionAdmission"
        ]
        is True
    )
    assert (
        go_nogo["gates"][
            "allAdversarialV2ResultsRejectedThroughProductionAdmission"
        ]
        is True
    )
    assert go_nogo["gates"]["crossRootDeterminismPassed"] is True
    assert go_nogo["gates"]["noTaskDispatched"] is True
    assert go_nogo["gates"]["noMarketEvaluation"] is True
    assert go_nogo["launchGateEvidence"]["launchEvidenceComplete"] is True
    assert package["launchGateEvidence"] == go_nogo["launchGateEvidence"]

    authorities = _load("evaluation-authorities-v1.json")
    result_admission = authorities["resultAdmissionAuthority"]
    _assert_self_hash(result_admission, "resultAdmissionAuthoritySha256")
    assert result_admission["productionAdmissionPolicy"] == "campaign_seal_shared_receipt_v2_2"
    assert "temporal_graph_candidate_window_result_v2" in result_admission[
        "admittedResultSchemas"
    ]


def test_v2_authority_contains_no_windows_host_paths() -> None:
    for path in OUTPUT_V2.iterdir():
        if path.is_file() and path.suffix in {".json", ".md"}:
            assert "C:\\\\" not in path.read_text(encoding="utf-8")


def test_v2_package_manifest_raw_hashes_match_every_committed_artifact() -> None:
    package = _load("topology-launch-package-manifest-v1.json")
    for artifact in package["artifacts"].values():
        # Git may materialize text files with CRLF on Windows; the package's
        # immutable raw hashes bind the generated LF bytes stored in Git.
        content = (OUTPUT_V2 / artifact["path"]).read_bytes().replace(b"\r\n", b"\n")
        assert f"sha256:{hashlib.sha256(content).hexdigest()}" == artifact["rawSha256"]
