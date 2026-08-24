from __future__ import annotations

import hashlib
import json

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_rust_canonical_topology_package_v1 import OUTPUT_V2


WORKER_CONTRACT_SHA256 = "sha256:3ea5b6e9c7803e6dc04e323d68209bc516d7d9207089e684169d7aa82ab74172"
WORKER_IMAGE_DIGEST = "sha256:95d7df7c40973c1d8744ef1274faf0d04ca170f07524d344544b40684b8e4193"
WORKER_SOURCE_COMMIT = "a7c5a359e046ff35734945dac2424c81f76e1fbf"
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

    assert package["schemaVersion"] == "temporal_qd_topology_no_dispatch_launch_package_v2"
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
    assert go_nogo["gates"]["allTasksUseCandidateWindowJobV2"] is True
    assert go_nogo["gates"]["dedicatedWorkerCapabilityRequired"] is True
    assert go_nogo["gates"]["immutableWorkerImageDigestBound"] is True
    assert go_nogo["gates"]["typedExecutionReceiptRequired"] is True
    assert go_nogo["gates"]["noTaskDispatched"] is True
    assert go_nogo["gates"]["noMarketEvaluation"] is True


def test_v2_package_manifest_raw_hashes_match_every_committed_artifact() -> None:
    package = _load("topology-launch-package-manifest-v1.json")
    for artifact in package["artifacts"].values():
        # Git may materialize text files with CRLF on Windows; the package's
        # immutable raw hashes bind the generated LF bytes stored in Git.
        content = (OUTPUT_V2 / artifact["path"]).read_bytes().replace(b"\r\n", b"\n")
        assert f"sha256:{hashlib.sha256(content).hexdigest()}" == artifact["rawSha256"]
