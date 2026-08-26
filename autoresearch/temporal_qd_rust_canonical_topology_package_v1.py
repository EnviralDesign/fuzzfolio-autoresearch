"""Freeze the Rust-canonical three-block topology study without dispatch.

The Rust JSONL seam authors every P/T/E/TE candidate. Python only transports
the sealed envelopes into the existing native campaign freezer and emits
content-addressed inspection/preregistration artifacts. No market evaluator,
gateway, generation controller, or worker dispatcher is invoked.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import build_replay_evidence_plan, canonical_json, canonical_sha256
from .temporal_qd_rust_dashboard_differential_v1 import JsonlProcess

ROOT = Path(__file__).resolve().parents[1]
RUST_ROOT = ROOT / "rust/temporal-qd"
# Preserve the published V1 import for historical audit callers. New package
# generation is explicitly directed to the V2 output and never rewrites V1.
OUTPUT = ROOT / "research/temporal-qd/rust-canonical-authority-v1"
OUTPUT_V2 = ROOT / "research/temporal-qd/rust-canonical-authority-v2"
V38 = ROOT / "runs/temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38"
PARENT_MATERIAL = V38 / "run/g2-parents-800/generations/generation-0003/proposal/parent-material.jsonl"
FROZEN_AUTHORITY = V38 / (
    "run/g2-parents-800/generations/generation-0003/proposal/native-batch/v5-proposal/"
    "490cac548bd735945219a8c3d85add4348d476f6190b238b2371255d37391c72/frozen-authority.json"
)
PAIR_RUN_CONFIG = ROOT / "runs/temporal-qd-v5-native-4000x1024x5-20260813-v1/authority/pair-run-config.json"
ROTATING = ROOT / "runs/temporal-qd-v5-native-4000x1024x5-20260813-v1/authority/rotating-evidence"
TOPOLOGY_SPEC = ROOT / "research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v7.json"
NATIVE_BIN = RUST_ROOT / "target/debug/temporal-qd-native-authority-jsonl.exe"
FREEZE_BIN = RUST_ROOT / "target/debug/temporal-qd-campaign-freeze.exe"

REQUEST_SCHEMA = "temporal_qd_native_authority_jsonl_request_v1"
DESIRED_BLOCKS = (
    ("qd_ed27f99ba0a8dfd7c76c69687efb", "short"),
    ("qd_69e5a3407ab21e82d787eb48c8d5", "short"),
    ("qd_001958c8b3288892a458207c9b76", "long"),
)
SOURCE_PATHS = (
    "rust/temporal-qd/Cargo.toml",
    "rust/temporal-qd/Cargo.lock",
    "rust/temporal-qd/crates/qd-kernel/src/v5.rs",
    "rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs",
    "rust/temporal-qd/crates/qd-kernel/src/bin/temporal-qd-native-authority-jsonl.rs",
    "rust/temporal-qd/crates/qd-campaign-freeze/Cargo.toml",
    "rust/temporal-qd/crates/qd-campaign-freeze/src/lib.rs",
    "rust/temporal-qd/crates/qd-campaign-freeze/src/bin/temporal-qd-precompiled-receipt-jsonl.rs",
    "rust/temporal-qd/crates/qd-campaign-seal/src/lib.rs",
    "rust/temporal-qd/crates/qd-campaign-seal/src/bin/temporal-qd-campaign-admission-jsonl.rs",
    "rust/temporal-qd/crates/qd-gateway-dispatch/src/lib.rs",
    "autoresearch/temporal_search.py",
    "autoresearch/temporal_qd_rust_canonical_topology_package_v1.py",
    "autoresearch/temporal_qd_worker_seam_conformance_v2.py",
)


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _git_blob_sha256(source_commit: str, path: Path) -> str:
    relative = path.relative_to(ROOT).as_posix()
    committed_blob = subprocess.run(
        ["git", "rev-parse", f"{source_commit}:{relative}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    worktree_blob = subprocess.run(
        ["git", "hash-object", "--path", relative, relative],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if committed_blob != worktree_blob:
        raise RuntimeError(
            f"result-admission source differs from {source_commit}: {relative}"
        )
    completed = subprocess.run(
        ["git", "show", f"{source_commit}:{relative}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return "sha256:" + hashlib.sha256(completed.stdout).hexdigest()


def _seal(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    result = dict(value)
    result[field] = canonical_sha256(result)
    return result


def _load_worker_contract(path: Path) -> dict[str, Any]:
    contract = _json(path)
    if contract.get("schema_version") != "replay-worker-contract-v2":
        raise RuntimeError("Rust-precompiled launch requires replay-worker-contract-v2")
    identity = dict(contract)
    supplied = identity.pop("contract_hash", None)
    identity.pop("git_sha", None)
    identity.pop("git_dirty", None)
    if supplied != canonical_sha256(identity):
        raise RuntimeError("worker contract admission hash does not match its fields")
    capabilities = set(contract.get("capabilities") or [])
    required_capability = "temporal_qd_precompiled_profile_execution_v2"
    if required_capability not in capabilities:
        raise RuntimeError("worker contract lacks the Rust-precompiled capability")
    git_sha = str(contract.get("git_sha") or "")
    if len(git_sha) != 40 or any(char not in "0123456789abcdef" for char in git_sha):
        raise RuntimeError("worker contract lacks an exact lowercase source Git commit")
    if contract.get("git_dirty") is not False:
        raise RuntimeError("worker contract source tree must be clean")
    image_digest = str(contract.get("image_digest") or "")
    rust_core_hash = str(contract.get("rust_core_hash") or "")
    if (
        contract.get("image_identity_mode") != "image_digest"
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", rust_core_hash)
        or not isinstance(contract.get("rust_build_info"), Mapping)
        or not contract["rust_build_info"]
        or not isinstance(contract.get("runtime_platform"), Mapping)
        or not contract["runtime_platform"]
    ):
        raise RuntimeError("worker contract lacks immutable image/Rust/runtime identity")
    return {
        "workerContractSchema": contract["schema_version"],
        "workerContractSha256": supplied,
        "imageDigest": image_digest,
        "imageIdentityMode": contract["image_identity_mode"],
        "sourceGitCommit": git_sha,
        "rustCoreHash": rust_core_hash,
        "rustBuildInfo": contract["rust_build_info"],
        "rustBuildInfoSha256": canonical_sha256(contract["rust_build_info"]),
        "runtimePlatform": contract["runtime_platform"],
        "runtimePlatformSha256": canonical_sha256(contract["runtime_platform"]),
        "capabilities": sorted(capabilities),
        "workerContractArtifact": {
            "artifactRole": "resolved_replay_worker_runtime_contract_v2",
            "logicalId": "worker-contract.json",
            "rawSha256": _sha_file(path),
        },
    }


def _load_launch_gate_evidence(
    *,
    conformance_report_path: Path | None,
    production_admission_report_path: Path | None,
    portability_report_path: Path | None,
    worker_contract: Mapping[str, Any],
    source_commit: str,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "conformanceReport": None,
        "productionAdmissionReport": None,
        "portabilityReport": None,
        "realWorkerReplayConformancePassed": False,
        "expandedAdversarialAdmissionPassed": False,
        "productionCampaignSealAdmissionPassed": False,
        "productionGatewayDispatchAdmissionPassed": False,
        "productionOfflineCampaignSealPassed": False,
        "allExactV2WorkerResultsAcceptedThroughProductionAdmission": False,
        "allAdversarialV2ResultsRejectedThroughProductionAdmission": False,
        "crossRootDeterminismPassed": False,
    }
    conformance_report: dict[str, Any] | None = None
    if conformance_report_path is not None:
        report = _json(conformance_report_path)
        conformance_report = report
        identity = dict(report)
        supplied = identity.pop("reportSha256", None)
        if supplied != canonical_sha256(identity):
            raise RuntimeError("worker-seam conformance report identity is invalid")
        adversarial = report.get("adversarialCases") or []
        real_worker_passed = (
            report.get("schemaVersion")
            == "temporal_qd_worker_seam_conformance_report_v2_2"
            and report.get("marketDataRead") is False
            and report.get("replayExecuted") is True
            and int(report.get("fullWorkerExecutionFixtureCount") or 0) >= 5
            and int(report.get("exactWorkerResultsAcceptedByFuzzFolio") or 0) >= 5
            and int(report.get("exactWorkerResultsAcceptedByPythonAdmission") or 0)
            >= 5
            and int(report.get("exactWorkerResultsAcceptedByRustAdmission") or 0) >= 5
            and report.get("workerContractHash")
            == worker_contract["workerContractSha256"]
            and report.get("workerImageDigest") == worker_contract["imageDigest"]
        )
        adversarial_passed = (
            len(adversarial) >= 26
            and int(report.get("adversarialRejectCount") or 0) == len(adversarial)
            and all(
                row.get("fuzzFolioRejected") is True
                and row.get("pythonAdmissionRejected") is True
                and row.get("rustAdmissionRejected") is True
                and row.get("productionCampaignSealRejected") is True
                for row in adversarial
            )
        )
        evidence.update(
            {
                "conformanceReport": {
                    "artifactRole": "worker_seam_conformance_v2_2",
                    "logicalId": "worker-seam-conformance.json",
                    "rawSha256": _sha_file(conformance_report_path),
                    "reportSha256": supplied,
                },
                "realWorkerReplayConformancePassed": real_worker_passed,
                "expandedAdversarialAdmissionPassed": adversarial_passed,
            }
        )
    if production_admission_report_path is not None:
        report = _json(production_admission_report_path)
        identity = dict(report)
        supplied = identity.pop("reportSha256", None)
        if supplied != canonical_sha256(identity):
            raise RuntimeError("production admission report identity is invalid")
        expected_source_hashes = {
            "campaignAdmissionAdapter": _git_blob_sha256(
                source_commit,
                ROOT
                / "rust/temporal-qd/crates/qd-campaign-seal/src/bin/temporal-qd-campaign-admission-jsonl.rs",
            ),
            "campaignSeal": _git_blob_sha256(
                source_commit,
                ROOT / "rust/temporal-qd/crates/qd-campaign-seal/src/lib.rs"
            ),
            "gatewayDispatch": _git_blob_sha256(
                source_commit,
                ROOT / "rust/temporal-qd/crates/qd-gateway-dispatch/src/lib.rs"
            ),
            "sharedReceiptValidator": _git_blob_sha256(
                source_commit,
                ROOT / "rust/temporal-qd/crates/qd-campaign-freeze/src/lib.rs"
            ),
        }
        adversarial = report.get("adversarialCases") or []
        production_raw_sha256 = _sha_file(production_admission_report_path)
        conformance_reference = (
            conformance_report.get("productionAdmissionReport")
            if conformance_report is not None
            else None
        )
        reports_cross_bound = (
            isinstance(conformance_reference, Mapping)
            and conformance_reference.get("rawSha256") == production_raw_sha256
            and conformance_reference.get("reportSha256") == supplied
            and conformance_report.get("exactFixtures")
            == report.get("exactFixtures")
            and [
                {
                    "case": row.get("case"),
                    "taskSha256": row.get("taskSha256"),
                    "resultSha256": row.get("resultSha256"),
                }
                for row in conformance_report.get("adversarialCases") or []
            ]
            == [
                {
                    "case": row.get("case"),
                    "taskSha256": row.get("taskSha256"),
                    "resultSha256": row.get("resultSha256"),
                }
                for row in adversarial
            ]
        )
        if conformance_report is not None and not reports_cross_bound:
            raise RuntimeError(
                "production admission report is not bound to worker-seam conformance"
            )
        exact_passed = int(
            report.get("productionCampaignSealExactAcceptCount") or 0
        ) >= 5
        adversarial_passed = (
            int(report.get("productionCampaignSealAdversarialRejectCount") or 0)
            == len(adversarial)
            and len(adversarial) >= 26
            and all(
                row.get("productionCampaignSealRejected") is True
                for row in adversarial
            )
        )
        source_bound = (
            report.get("schemaVersion")
            == "temporal_qd_production_result_admission_report_v1"
            and report.get("sourceCommit") == source_commit
            and report.get("sourceHashes") == expected_source_hashes
            and report.get("productionAdmissionPolicy")
            == "campaign_seal_shared_receipt_v2_2"
            and set(report.get("productionBinaryHashes") or {})
            == {
                "temporal-qd-campaign-admission-jsonl",
                "temporal-qd-precompiled-receipt-jsonl",
            }
            and report.get("marketDataRead") is False
            and report.get("gatewayNetworkAccess") is False
            and int(report.get("taskDispatchCount") or 0) == 0
            and report.get("workerContractHash")
            == worker_contract["workerContractSha256"]
            and report.get("workerImageDigest") == worker_contract["imageDigest"]
            and len(report.get("exactFixtures") or []) >= 5
            and reports_cross_bound
        )
        evidence.update(
            {
                "productionAdmissionReport": {
                    "artifactRole": "production_result_admission_v2_2",
                    "logicalId": "production-admission.json",
                    "rawSha256": production_raw_sha256,
                    "reportSha256": supplied,
                },
                "productionCampaignSealAdmissionPassed": source_bound
                and exact_passed,
                "productionGatewayDispatchAdmissionPassed": source_bound
                and report.get("productionGatewayDispatchFixturePassed") is True,
                "productionOfflineCampaignSealPassed": source_bound
                and report.get("productionOfflineSealFixturePassed") is True,
                "allExactV2WorkerResultsAcceptedThroughProductionAdmission": source_bound
                and exact_passed,
                "allAdversarialV2ResultsRejectedThroughProductionAdmission": source_bound
                and adversarial_passed,
            }
        )
    if portability_report_path is not None:
        report = _json(portability_report_path)
        identity = dict(report)
        supplied = identity.pop("reportSha256", None)
        if supplied != canonical_sha256(identity):
            raise RuntimeError("cross-root determinism report identity is invalid")
        portability_passed = (
            report.get("schemaVersion")
            == "temporal_qd_authority_cross_root_determinism_report_v1"
            and report.get("authorityProjectionMatches") is True
            and report.get("taskManifestMatches") is True
            and report.get("candidateIdsMatch") is True
            and report.get("artifactRawSha256Matches") is True
            and int(report.get("comparedArtifactCount") or 0) >= 15
            and int(report.get("hostSpecificPathLeakCount", -1)) == 0
            and report.get("workerContractSha256")
            == worker_contract["workerContractSha256"]
            and report.get("sourceCommit") == source_commit
        )
        evidence.update(
            {
                "portabilityReport": {
                    "artifactRole": "cross_root_determinism_v1",
                    "logicalId": "cross-root-determinism.json",
                    "rawSha256": _sha_file(portability_report_path),
                    "reportSha256": supplied,
                },
                "crossRootDeterminismPassed": portability_passed,
            }
        )
    evidence["launchEvidenceComplete"] = all(
        evidence[key]
        for key in (
            "realWorkerReplayConformancePassed",
            "expandedAdversarialAdmissionPassed",
            "productionCampaignSealAdmissionPassed",
            "productionGatewayDispatchAdmissionPassed",
            "productionOfflineCampaignSealPassed",
            "allExactV2WorkerResultsAcceptedThroughProductionAdmission",
            "allAdversarialV2ResultsRejectedThroughProductionAdmission",
            "crossRootDeterminismPassed",
        )
    )
    return evidence


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def _write_pretty(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True, separators=(",", ": ")
    )
    path.write_text(encoded + "\n", encoding="utf-8", newline="\n")


def _git(*args: str, binary: bool = False) -> str | bytes:
    result = subprocess.run(
        ["git", "-C", str(ROOT), *args], check=True, capture_output=True,
        text=not binary,
    )
    return result.stdout if binary else result.stdout.strip()


def _git_json(commit: str, relative: str) -> dict[str, Any]:
    raw = _git("show", f"{commit}:{relative}", binary=True)
    assert isinstance(raw, bytes)
    return json.loads(raw)


def _validate_self_hash(value: Mapping[str, Any], field: str) -> None:
    identity = dict(value)
    supplied = identity.pop(field, None)
    if supplied != canonical_sha256(identity):
        raise RuntimeError(f"preserved native artifact identity is invalid: {field}")


def _load_preserved_native_package(
    package_commit: str,
    *,
    expected_source_commit: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    prefix = "research/temporal-qd/rust-canonical-authority-v2"
    source = _git_json(package_commit, f"{prefix}/native-source-manifest-v1.json")
    authority = _git_json(package_commit, f"{prefix}/native-authority-v1.json")
    block_set = _git_json(package_commit, f"{prefix}/topology-native-blocks-v1.json")
    candidate_set = _git_json(
        package_commit, f"{prefix}/native-candidate-envelopes-v1.json"
    )
    for value, field in (
        (source, "sourceManifestSha256"),
        (authority, "authoritySha256"),
        (block_set, "blockSetSha256"),
        (candidate_set, "candidateSetSha256"),
    ):
        _validate_self_hash(value, field)
    if (
        source.get("rustSourceCommit") != expected_source_commit
        or authority.get("rustSourceCommit") != expected_source_commit
        or authority.get("sourceManifestSha256")
        != source.get("sourceManifestSha256")
        or block_set.get("nativeAuthoritySha256") != authority.get("authoritySha256")
        or candidate_set.get("nativeAuthoritySha256")
        != authority.get("authoritySha256")
    ):
        raise RuntimeError("preserved native package authority binding is invalid")
    blocks = block_set.get("blocks") or []
    candidates = candidate_set.get("candidates") or []
    _validate_native_blocks(blocks)
    flattened = [candidate for block in blocks for candidate in block["candidates"]]
    if canonical_json(flattened) != canonical_json(candidates):
        raise RuntimeError("preserved native block and candidate sets diverge")
    return source, authority, blocks, candidates


def _source_manifest(commit: str, *, allow_uncommitted: bool) -> dict[str, Any]:
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("Rust authority source commit must be a lowercase 40-character Git SHA")
    file_rows: list[dict[str, Any]] = []
    mismatches: list[str] = []
    for relative in SOURCE_PATHS:
        path = ROOT / relative
        if not path.is_file():
            raise RuntimeError(f"native authority source is missing: {relative}")
        worktree_sha = _sha_file(path)
        try:
            committed = _git("show", f"{commit}:{relative}", binary=True)
            assert isinstance(committed, bytes)
            commit_sha = "sha256:" + hashlib.sha256(committed).hexdigest()
            commit_blob = str(_git("rev-parse", f"{commit}:{relative}"))
        except subprocess.CalledProcessError:
            commit_sha = None
            commit_blob = None
        worktree_blob = str(_git("hash-object", "--path", relative, relative))
        if commit_blob != worktree_blob:
            mismatches.append(relative)
        file_rows.append(
            {
                "path": relative,
                "sha256": commit_sha or worktree_sha,
                "commitBlobSha256": commit_sha,
                "matchesCommitAfterGitFilters": commit_blob == worktree_blob,
            }
        )
    if mismatches and not allow_uncommitted:
        raise RuntimeError(f"authority source differs from {commit}: {mismatches}")
    return _seal(
        {
            "schemaVersion": "temporal_qd_native_source_manifest_v1",
            "repository": "https://github.com/EnviralDesign/fuzzfolio-autoresearch.git",
            "rustSourceCommit": commit,
            "sourceFiles": file_rows,
            "allSourceFilesMatchCommit": not mismatches,
            "developmentOverride": bool(mismatches),
        },
        "sourceManifestSha256",
    )


def _native_authority(shared: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    config = _json(PAIR_RUN_CONFIG)
    shared_contract = shared["authority"]
    by_path = {row["path"]: row["sha256"] for row in source["sourceFiles"]}
    authority = {
        "schemaVersion": "temporal_qd_native_compiler_authority_v1",
        "authorityVersion": "1",
        "rustSourceCommit": source["rustSourceCommit"],
        "rustSourceTreeSha256": canonical_sha256(source["sourceFiles"]),
        "cargoLockSha256": by_path["rust/temporal-qd/Cargo.lock"],
        "cargoWorkspaceSha256": by_path["rust/temporal-qd/Cargo.toml"],
        "historicalSharedAuthoritySha256": shared["authoritySha256"],
        "pairRunConfigSha256": config["pairRunConfigSha256"],
        "compilerPolicySha256": shared_contract["compilerPolicySha256"],
        "nativeOperatorAuthoritySha256": shared_contract["nativeOperatorAuthoritySha256"],
        "pairPolicySha256": shared_contract["pairPolicySha256"],
        "canonicalJsonContract": "temporal_qd_contract::canonical_json_line",
        "validationSchemaVersion": "temporal_search_candidate_validation_v1",
        "sourceManifestSha256": source["sourceManifestSha256"],
        "ownership": {
            "moduleValidation": "rust",
            "moduleLowering": "rust",
            "pairCompilation": "rust",
            "profileProgramReportIdentity": "rust",
            "candidatePairIdentity": "rust",
            "parentReconstructionAndLineage": "rust",
            "campaignCandidateMaterial": "rust",
            "dashboardRole": "historical_read_only_and_observational_compatibility_oracle",
        },
    }
    return _seal(authority, "authoritySha256")


def _parent_rows(candidate_ids: set[str]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    with PARENT_MATERIAL.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("candidateId") in candidate_ids:
                if row["candidateId"] in rows:
                    raise RuntimeError(f"duplicate retained parent row: {row['candidateId']}")
                rows[row["candidateId"]] = row
    missing = candidate_ids - set(rows)
    if missing:
        raise RuntimeError(f"missing retained parent rows: {sorted(missing)}")
    return rows


def _author_blocks(
    shared: Mapping[str, Any], native_authority: Mapping[str, Any]
) -> list[dict[str, Any]]:
    spec = _json(TOPOLOGY_SPEC)
    rows = _parent_rows({candidate_id for candidate_id, _ in DESIRED_BLOCKS})
    blocks = {(row["parentCandidateId"], row["side"]): row for row in spec["blocks"]}
    topology = {(row["parentCandidateId"], row["side"]): row for row in spec["topologyPlans"]}
    events = {(row["parentCandidateId"], row["side"]): row for row in spec["eventPrimitives"]}
    process = JsonlProcess([str(NATIVE_BIN)], cwd=ROOT)
    output: list[dict[str, Any]] = []
    try:
        for candidate_id, side in DESIRED_BLOCKS:
            key = (candidate_id, side)
            response = process.request(
                {
                    "schemaVersion": REQUEST_SCHEMA,
                    "requestId": f"topology-study-{candidate_id}-{side}",
                    "operation": "author_topology_study_block",
                    "candidateId": candidate_id,
                    "sharedAuthority": shared,
                    "parentMaterialRow": rows[candidate_id],
                    "block": blocks[key],
                    "topologyRecord": topology[key],
                    "eventPrimitive": events[key],
                    "nativeAuthority": native_authority,
                }
            )
            output.append(response["result"]["topologyBlock"])
    finally:
        process.close()
    if [len(row["candidates"]) for row in output] != [4, 4, 4]:
        raise RuntimeError("native authority did not produce three complete four-arm blocks")
    return output


def _candidate_contract(
    candidate: Mapping[str, Any],
    authority_sha: str,
    catalog_sha256: str,
) -> dict[str, Any]:
    identity = candidate["compiledPair"]["identityMaterial"]
    contract = {
        "schemaVersion": "temporal_qd_precompiled_profile_execution_v2",
        "candidateId": candidate["candidateId"],
        "authoritySha256": authority_sha,
        "candidatePayloadSha256": candidate["payloadSha256"],
        "rawProfileSha256": identity["rawProfileSha256"],
        "profileSnapshotSha256": identity["profileSnapshotSha256"],
        "programSha256": identity["programSha256"],
        "expectedResolvedProfileSnapshotSha256": identity[
            "profileSnapshotSha256"
        ],
        "expectedResolvedProgramSha256": identity["programSha256"],
        "validationReportSha256": identity["validationReportSha256"],
        "catalogSha256": catalog_sha256,
        "catalogResolutionMode": "verify_exact_no_rewrite_v1",
        "sourceProfileRewritePermitted": False,
        "resolvedProfileMustEqualAuthoredProfile": True,
        "compilerDisposition": "precompiled_rust_profile_no_recompile",
        "workerPermissions": [
            "parse",
            "schema_check",
            "verify_hashes",
            "verify_exact_catalog_inputs",
            "execute_supplied_v3_profile",
        ],
        "workerProhibitions": [
            "compile_long_short_pair",
            "rewrite_defaults_or_transitions",
            "assign_competing_identity",
            "remove_precompiled_contract",
        ],
    }
    return _seal(contract, "contractSha256")


def _validate_native_blocks(blocks: list[Mapping[str, Any]]) -> None:
    for block in blocks:
        if [candidate["arm"] for candidate in block["candidates"]] != ["P", "T", "E", "TE"]:
            raise RuntimeError(f"native topology arm chain drifted: {block['blockId']}")
        topology = block["operatorEvidence"]["topologyPlan"]["construction"]
        event = block["operatorEvidence"]["eventPlan"]["construction"]
        combined = block["operatorEvidence"]["combinedEventPlan"]["construction"]
        if topology.get("operation") != "insert_setup":
            raise RuntimeError(f"topology control is not insert_setup: {block['blockId']}")
        if event.get("indicatorId") != combined.get("indicatorId") or event.get("contract") != combined.get("contract"):
            raise RuntimeError(f"E and TE do not use the same event primitive: {block['blockId']}")
        if combined.get("nodeId") != block["addedSetupNodeId"]:
            raise RuntimeError(f"TE event is not bound to the added setup node: {block['blockId']}")
        opposite = "shortModule" if block["side"] == "long" else "longModule"
        opposite_programs = {canonical_sha256(candidate[opposite]["program"]) for candidate in block["candidates"]}
        opposite_identities = {candidate[opposite]["moduleIdentitySha256"] for candidate in block["candidates"]}
        if len(opposite_programs) != 1 or len(opposite_identities) != 1:
            raise RuntimeError(f"opposite side changed inside block: {block['blockId']}")
        for candidate in block["candidates"]:
            unsigned = dict(candidate)
            stored = unsigned.pop("payloadSha256")
            if canonical_sha256(unsigned) != stored:
                raise RuntimeError(f"candidate envelope self hash drifted: {candidate['candidateId']}")
            report = candidate["compiledPair"]["validationReport"]
            if report.get("candidateAcceptable") is not True or report.get("status") != "valid_evaluable":
                raise RuntimeError(f"Rust candidate validation failed: {candidate['candidateId']}")


def _panel_inputs(profile: Mapping[str, Any], panel: Mapping[str, Any], panel_id: str) -> list[dict[str, Any]]:
    exemplar = panel["candidates"][0]
    inputs: list[dict[str, Any]] = []
    for row in exemplar["windowInputs"]:
        old = row["evidencePlan"]
        plan = build_replay_evidence_plan(
            evidence_role=old["evidence_role"],
            selection_data_end=old["selection_data_end"],
            analysis_window_start=old["analysis_window_start"],
            analysis_window_end=old["analysis_window_end"],
            requested_horizon_months=old["requested_horizon_months"],
            profile_snapshot=dict(profile),
            campaign_plan_id=old["campaign_plan_id"],
            execution_cell_sha256=old["execution_cell_sha256"],
            lake_window_binding=old["lake_window_binding"],
            data_availability_cutoff=old["data_availability_cutoff"],
            coverage_policy=old["coverage_policy"],
        ).model_dump(mode="json", exclude_none=False)
        inputs.append(
            {
                "windowId": f"{panel_id}-{row['windowId']}",
                "panelId": panel_id,
                "sourceWindowId": row["windowId"],
                "evidencePlan": plan,
                "evidencePlanId": plan["plan_id"],
                "lakeWindowSemanticSha256": plan["lake_window_binding"]["window_semantic_sha256"],
            }
        )
    return inputs


def _campaign_authority(
    candidates: list[dict[str, Any]],
    native_authority: Mapping[str, Any],
    worker_contract: Mapping[str, Any],
    source_commit: str,
    catalog_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    panels = [_json(ROTATING / f"panel-{index}-template-preparation.json") for index in (1, 2, 3)]
    workers = {canonical_json(panel["workerContract"]) for panel in panels}
    if len(workers) != 1:
        raise RuntimeError("inspected panel worker contracts disagree")
    historical_panel_worker = copy.deepcopy(panels[0]["workerContract"])
    worker = copy.deepcopy(worker_contract)
    windows: list[dict[str, Any]] = []
    for index, panel in enumerate(panels, 1):
        for window in panel["developmentWindows"]:
            row = copy.deepcopy(window)
            row["sourceWindowId"] = row["windowId"]
            row["windowId"] = f"panel-{index}-{row['windowId']}"
            row["panelId"] = f"panel-{index}"
            windows.append(row)
    if len({row["windowId"] for row in windows}) != 12:
        raise RuntimeError("inspected panel windows are not unique")
    campaign_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        profile = candidate["compiledPair"]["profile"]
        identity = candidate["compiledPair"]["identityMaterial"]
        contract = _candidate_contract(
            candidate, native_authority["authoritySha256"], catalog_sha256
        )
        inputs: list[dict[str, Any]] = []
        for index, panel in enumerate(panels, 1):
            inputs.extend(_panel_inputs(profile, panel, f"panel-{index}"))
        campaign_candidates.append(
            {
                "candidateId": candidate["candidateId"],
                "candidatePayloadSha256": candidate["payloadSha256"],
                "nativeAuthoritySha256": native_authority["authoritySha256"],
                "sourceProfile": profile,
                "sourceProfileSha256": identity["rawProfileSha256"],
                "profileSnapshotSha256": identity["profileSnapshotSha256"],
                "programSha256": identity["programSha256"],
                "resolvedProfileSnapshotSha256": identity["profileSnapshotSha256"],
                "resolvedProgramSha256": identity["programSha256"],
                "precompiledProfileExecutionContract": contract,
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": inputs,
            }
        )
    catalog = ROTATING / "construction-catalog.json"
    cost_views = {
        "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
        "research_conservative": {"spreadBps": 2.0, "slippageBps": 1.0, "commissionBps": 0.5},
    }
    result_admission_sources = {
        "campaignAdmissionAdapter": ROOT
        / "rust/temporal-qd/crates/qd-campaign-seal/src/bin/temporal-qd-campaign-admission-jsonl.rs",
        "campaignSeal": ROOT / "rust/temporal-qd/crates/qd-campaign-seal/src/lib.rs",
        "gatewayDispatch": ROOT / "rust/temporal-qd/crates/qd-gateway-dispatch/src/lib.rs",
        "sharedReceiptValidator": ROOT / "rust/temporal-qd/crates/qd-campaign-freeze/src/lib.rs",
    }
    result_admission_authority = _seal(
        {
            "schemaVersion": "temporal_qd_result_admission_authority_v1",
            "sourceCommit": source_commit,
            "sourceHashes": {
                name: _git_blob_sha256(source_commit, path)
                for name, path in sorted(result_admission_sources.items())
            },
            "admittedResultSchemas": [
                "temporal_graph_candidate_window_result_v1",
                "temporal_graph_candidate_window_result_v2",
                "temporal_graph_candidate_window_rejected_result_v1",
            ],
            "precompiledReceiptSchema": "temporal_qd_precompiled_profile_execution_receipt_v2",
            "runtimeAttestationSchema": "temporal_qd_runtime_program_identity_attestation_v1",
            "productionAdmissionPolicy": "campaign_seal_shared_receipt_v2_2",
        },
        "resultAdmissionAuthoritySha256",
    )
    authorities = _seal(
        {
            "schemaVersion": "temporal_qd_topology_evaluation_authorities_v1",
            "nativeCompilerAuthoritySha256": native_authority["authoritySha256"],
            "resultAdmissionAuthority": result_admission_authority,
            "workerContract": worker,
            "workerContractCompatibilityObservation": {
                "panelTemplateSha256": historical_panel_worker[
                    "workerContractSha256"
                ],
                "historicalV38DiscoveryExpectedSha256": "sha256:40292e2a62171f1d13fda9c5e9ba953d3e04d4270845889caabb5aa80648f4c4",
                "disposition": "historical_observation_only_bind_new_digest_attested_precompiled_worker",
            },
            "costViews": cost_views,
            "costViewsSha256": canonical_sha256(cost_views),
            "constructionCatalog": {
                "path": catalog.relative_to(ROOT).as_posix(),
                "rawSha256": _sha_file(catalog),
            },
            "pairRunConfigSha256": native_authority["pairRunConfigSha256"],
            "sourceAuthority": "rust_canonical_native_v1",
        },
        "evaluationAuthoritiesSha256",
    )
    authority: dict[str, Any] = {
        "schemaVersion": "temporal_graph_candidate_window_authority_v1",
        "authorityLabel": "rust-canonical-topology-case-study-inspected-v1",
        "preparationSha256": canonical_sha256(
            {
                "nativeAuthoritySha256": native_authority["authoritySha256"],
                "candidatePayloads": [candidate["payloadSha256"] for candidate in candidates],
                "panels": [canonical_sha256(panel) for panel in panels],
            }
        ),
        "workerContract": worker,
        "bounds": {"maxCandidates": 12, "maxDevelopmentWindows": 12, "maxTasks": 144, "maxAttempts": 8, "deadlineSeconds": 86400.0},
        "taskContract": {
            "taskKind": "temporal_graph_candidate_window",
            "jobSchema": "temporal_graph_candidate_window_job_v2",
            "resultSchema": "temporal_graph_candidate_window_result_v2",
            "profileExecutionContract": "temporal_qd_precompiled_profile_execution_v2",
            "profileExecutionReceipt": "temporal_qd_precompiled_profile_execution_receipt_v2",
            "requiredWorkerCapability": "temporal_qd_precompiled_profile_execution_v2",
            "resultAdmissionAuthoritySha256": result_admission_authority[
                "resultAdmissionAuthoritySha256"
            ],
        },
        "prohibitedEvidence": panels[0]["prohibitedEvidence"],
        "developmentWindows": windows,
        "candidates": campaign_candidates,
        "executionPolicy": {
            "dispatchEnabled": False,
            "marketEvaluationPermitted": False,
            "generationPermitted": False,
            "productionArchiveWritePermitted": False,
            "workerMayCompilePair": False,
        },
        "evaluationAuthoritiesSha256": authorities["evaluationAuthoritiesSha256"],
    }
    authority["authorityId"] = canonical_sha256(authority)
    return authority, authorities


def _freeze(authority_path: Path, freeze_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    if freeze_root.exists():
        shutil.rmtree(freeze_root)
    freeze_root.mkdir(parents=True)
    manifest = {
        "schemaVersion": "temporal_qd_native_campaign_task_matrix_manifest_v1",
        "authorityPath": str(authority_path.resolve()),
        "outputRoot": str(freeze_root.resolve()),
        "behaviorAttributionRequirement": None,
    }
    manifest_path = freeze_root.parent / f"{freeze_root.name}-manifest.json"
    _write_pretty(manifest_path, manifest)
    result = subprocess.run(
        [str(FREEZE_BIN), "--manifest", str(manifest_path)],
        cwd=ROOT, check=True, text=True, capture_output=True,
    )
    freeze_result = json.loads(result.stdout.strip().splitlines()[-1])
    task_manifest = _json(freeze_root / "task-manifest.json")
    return freeze_result, task_manifest


def _task_index(task_manifest: Mapping[str, Any], windows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    panel_by_window = {row["windowId"]: row["panelId"] for row in windows}
    rows = []
    pairs: set[tuple[str, str]] = set()
    task_ids: set[str] = set()
    for task in task_manifest["tasks"]:
        payload = task["payload"]
        pair = (payload["candidate_id"], payload["window_id"])
        if pair in pairs or task["task_id"] in task_ids:
            raise RuntimeError("duplicate candidate/window or task ID in native freeze")
        pairs.add(pair)
        task_ids.add(task["task_id"])
        contract = payload["precompiled_profile_execution_contract"]
        if (
            payload.get("schema_version")
            != "temporal_graph_candidate_window_job_v2"
            or "temporal_qd_precompiled_profile_execution_v2"
            not in set(payload.get("required_capabilities") or [])
            or "temporal_qd_precompiled_profile_execution_v2"
            not in set(task.get("required_worker_capabilities") or [])
        ):
            raise RuntimeError("native freeze did not require the V2 precompiled worker path")
        if canonical_sha256(payload["inline_profile_snapshot"]) != payload["raw_source_profile_sha256"]:
            raise RuntimeError("native freeze mutated a Rust-canonical candidate profile")
        rows.append(
            {
                "taskId": task["task_id"],
                "candidateId": pair[0],
                "windowId": pair[1],
                "panelId": panel_by_window[pair[1]],
                "taskPayloadSha256": canonical_sha256(payload),
                "candidatePayloadSha256": contract["candidatePayloadSha256"],
                "precompiledExecutionContractSha256": contract["contractSha256"],
                "requiredWorkerContractSha256": payload[
                    "required_worker_contract_hash"
                ],
                "requiredWorkerImageDigest": payload[
                    "required_worker_image_digest"
                ],
                "requiredExecutionReceiptSchema": (
                    "temporal_qd_precompiled_profile_execution_receipt_v2"
                ),
                "rawProfileSha256": payload["raw_source_profile_sha256"],
                "profileSnapshotSha256": payload["normalized_profile_snapshot_sha256"],
                "programSha256": payload["authored_program_sha256"],
                "evidencePlanId": payload["evidence_plan"]["plan_id"],
                "lakeWindowSemanticSha256": payload["lake_window_semantic_sha256"],
            }
        )
    if len(rows) != 144 or len(pairs) != 144:
        raise RuntimeError(f"expected 144 unique inspected tasks, received {len(rows)}")
    return _seal(
        {
            "schemaVersion": "temporal_qd_topology_inspected_task_index_v1",
            "authorityId": task_manifest["authorityId"],
            "taskMatrixSha256": task_manifest["taskMatrixSha256"],
            "taskCount": 144,
            "candidateCount": 12,
            "panelCount": 3,
            "windowsPerPanel": 4,
            "tasks": rows,
            "dispatchEnabled": False,
            "workerDispatched": False,
            "marketEvaluationLaunched": False,
        },
        "taskIndexSha256",
    )


def _confirmation(candidates: list[Mapping[str, Any]], authorities: Mapping[str, Any]) -> dict[str, Any]:
    windows = [
        ("untouched-2027-q1", "2027-01-01T00:00:00Z", "2027-04-01T00:00:00Z", "2026-11-11T00:00:00Z"),
        ("untouched-2027-q2", "2027-04-01T00:00:00Z", "2027-07-01T00:00:00Z", "2027-02-09T00:00:00Z"),
        ("untouched-2027-q3", "2027-07-01T00:00:00Z", "2027-10-01T00:00:00Z", "2027-05-11T00:00:00Z"),
        ("untouched-2027-q4", "2027-10-01T00:00:00Z", "2028-01-01T00:00:00Z", "2027-08-11T00:00:00Z"),
    ]
    window_rows = [
        {
            "windowId": window_id,
            "analysisWindowStart": start,
            "analysisWindowEnd": end,
            "plannedLakeWindowRequest": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["H1", "M15", "M5"],
                "data_start": data_start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
        }
        for window_id, start, end, data_start in windows
    ]
    projected = []
    for candidate in candidates:
        identity = candidate["compiledPair"]["identityMaterial"]
        for window in window_rows:
            evidence_identity = canonical_sha256(
                {
                    "schemaVersion": "temporal_qd_future_evidence_plan_v1",
                    "evidenceRole": "untouched_confirmation",
                    "candidateId": candidate["candidateId"],
                    "profileSnapshotSha256": identity["profileSnapshotSha256"],
                    "window": window,
                    "coveragePolicy": "require_complete",
                    "bindingDisposition": "freeze_lake_attestation_only_after_complete_data_exists",
                }
            )
            projected.append(
                {
                    "candidateId": candidate["candidateId"],
                    "candidatePayloadSha256": candidate["payloadSha256"],
                    "windowId": window["windowId"],
                    "projectedTaskId": canonical_sha256(
                        {"candidateId": candidate["candidateId"], "windowId": window["windowId"], "evidencePlanId": evidence_identity}
                    ),
                    "evidencePlanId": evidence_identity,
                }
            )
    return _seal(
        {
            "schemaVersion": "temporal_qd_topology_untouched_confirmation_preregistration_v1",
            "cohort": [{"candidateId": row["candidateId"], "payloadSha256": row["payloadSha256"]} for row in candidates],
            "windows": window_rows,
            "projectedTasks": projected,
            "projectedTaskCount": 48,
            "noOverlapWithInspectedPanels": True,
            "panel4LatinSquareReused": False,
            "evaluationAuthoritiesSha256": authorities["evaluationAuthoritiesSha256"],
            "executionDeferred": True,
            "deferReason": "future_market_data_and_immutable_lake_attestations_are_incomplete",
            "dispatchEnabled": False,
        },
        "preregistrationSha256",
    )


def _semantic_decision(differential: Mapping[str, Any]) -> dict[str, Any]:
    return _seal(
        {
            "schemaVersion": "temporal_qd_rust_dashboard_semantic_decision_v1",
            "differentialTranscriptSha256": differential["transcriptSha256"],
            "languageNeutralContract": {
                "v2Normalization": "schema_parse_then_canonical_omission_of_default_equivalent_optional_fields",
                "candidateMetadata": "candidate_id_name_description_and_is_active_do_not_change_executable_graph_semantics",
                "transitionOrdering": "priority_then_declared_stable_order_with_ids_preserved_in_identity_material",
                "actionOrdering": "declared_action_order_is_executable_and_identity_bearing",
                "protectionNormalization": "stop_target_break_even_and_hold_forms_are canonicalized_by_native_policy",
                "defaultNullHandling": "omitted_hold_onBreach_and_explicit_exit_next_open_are_semantically_equivalent; native_identity_uses_omission",
                "resourceEventBinding": "exact_indicator_instance_event_output_and_evidence_group_bindings_are_executable",
                "v3ModuleSources": "long_and_short_native_profile_and_program_identities_are_embedded_without_recompilation",
                "v3Construction": "one_both_direction_supervisor_with_prefixed_modules_and_shared_management_library",
                "programIdentity": "normalized_executable_graph_execution_config_and_module_source_manifest",
                "validationIdentity": "validation_report_is_separate_from_profile_and_program_identity",
            },
            "differenceDisposition": {
                "holdPolicyOnBreach": "normalization_difference_with_possible_semantic_effect_reviewed_behaviorally_equivalent",
                "validationCapabilityOrdering": "canonical_ordering_or_validation_report_metadata_only",
                "dashboardRole": "historical_oracle_not_new_identity_authority",
            },
            "answers": {
                "implementationMatchingExplicitContract": "rust",
                "implementationMatchingHistoricalV38": "rust",
                "behaviorallyEquivalentDespiteIdentityDifferences": True,
                "semanticDefect": "none_demonstrated; dashboard_materializes_a_default_that_rust_canonicalizes_to_omission",
            },
            "phase1Decision": "proceed_with_unchanged_rust_semantics_and_versioned_authority_cutover",
        },
        "decisionSha256",
    )


def _science_contract(spec: Mapping[str, Any]) -> dict[str, Any]:
    return _seal(
        {
            "schemaVersion": "temporal_qd_topology_case_study_scientific_contract_v1",
            "arms": ["P", "T", "E", "TE"],
            "interactionIdentity": "TE_minus_T_minus_E_plus_P",
            "combinedOutperformsBothSingleMutations": "report_separately_as_TE_net_gt_T_and_TE_net_gt_E",
            "usefulProgressiveInnovation": "TE_net_gt_P_T_E_and_TE_worst_window_not_worse_than_P_T_E_with_unchanged_support_direction_quality_gates",
            "instrumentation": [
                "added_setup_node_occupancy", "bars_spent_in_setup", "entry_timestamp_and_bar_shift_vs_P",
                "changed_opportunity_count", "event_freshness_at_E", "event_freshness_at_TE",
                "transition_trace_through_added_node", "trade_count", "cost_drag", "support", "direction", "quality_status",
            ],
            "insertSetupDisposition": "timing_and_staging_mutation_not_assumed_behavior_preserving",
            "familyLevelInferencePermitted": False,
            "sourceSuccessContract": spec["successCalculation"],
        },
        "scientificContractSha256",
    )


def run(
    output_dir: Path,
    freeze_root: Path,
    *,
    source_commit: str,
    worker_contract_path: Path,
    catalog_sha256: str,
    conformance_report_path: Path | None = None,
    production_admission_report_path: Path | None = None,
    portability_report_path: Path | None = None,
    result_admission_source_commit: str | None = None,
    preserve_native_package_commit: str | None = None,
    allow_uncommitted_source: bool = False,
) -> dict[str, Path]:
    required = [FREEZE_BIN, FROZEN_AUTHORITY, PARENT_MATERIAL, TOPOLOGY_SPEC]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", catalog_sha256):
        raise RuntimeError("exact runtime indicator catalog SHA-256 is required")
    if preserve_native_package_commit is None:
        required.append(NATIVE_BIN)
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"required native/historical inputs are missing: {missing}")
    # V2 changes only the worker execution seam. Reuse the published immutable
    # compiler differential as historical input without copying or rewriting it.
    differential = _json(OUTPUT / "target-cross-compiler-transcript-v1.json")
    shared = _json(FROZEN_AUTHORITY)
    admission_source_commit = result_admission_source_commit or source_commit
    if preserve_native_package_commit is None:
        source = _source_manifest(
            source_commit, allow_uncommitted=allow_uncommitted_source
        )
        authority = _native_authority(shared, source)
        blocks = _author_blocks(shared, authority)
        _validate_native_blocks(blocks)
        candidates = [
            candidate for block in blocks for candidate in block["candidates"]
        ]
    else:
        source, authority, blocks, candidates = _load_preserved_native_package(
            preserve_native_package_commit,
            expected_source_commit=source_commit,
        )
    worker_contract = _load_worker_contract(worker_contract_path)
    launch_evidence = _load_launch_gate_evidence(
        conformance_report_path=conformance_report_path,
        production_admission_report_path=production_admission_report_path,
        portability_report_path=portability_report_path,
        worker_contract=worker_contract,
        source_commit=admission_source_commit,
    )
    if len(candidates) != 12 or len({row["candidateId"] for row in candidates}) != 12 or len({row["payloadSha256"] for row in candidates}) != 12:
        raise RuntimeError("native topology candidates are not exactly 12 unique identities/payloads")
    campaign, evaluation_authorities = _campaign_authority(
        candidates,
        authority,
        worker_contract,
        admission_source_commit,
        catalog_sha256,
    )
    paths = {
        "source": output_dir / "native-source-manifest-v1.json",
        "authority": output_dir / "native-authority-v1.json",
        "blocks": output_dir / "topology-native-blocks-v1.json",
        "candidates": output_dir / "native-candidate-envelopes-v1.json",
        "payloadIndex": output_dir / "candidate-payload-index-v1.json",
        "ledger": output_dir / "candidate-ledger-v1.json",
        "evaluationAuthorities": output_dir / "evaluation-authorities-v1.json",
        "campaignAuthority": output_dir / "inspected-campaign-authority-v1.json",
        "taskIndex": output_dir / "inspected-task-index-v1.json",
        "confirmation": output_dir / "untouched-confirmation-preregistration-v1.json",
        "semantic": output_dir / "semantic-contract-decision-v1.json",
        "semanticMemo": output_dir / "semantic-contract-decision-v1.md",
        "science": output_dir / "topology-scientific-contract-v1.json",
        "goNogo": output_dir / "topology-launch-go-nogo-v1.json",
        "package": output_dir / "topology-launch-package-manifest-v1.json",
    }
    _write(paths["source"], source)
    _write(paths["authority"], authority)
    _write(paths["blocks"], _seal({"schemaVersion": "temporal_qd_native_topology_blocks_v1", "nativeAuthoritySha256": authority["authoritySha256"], "blockCount": 3, "blocks": blocks}, "blockSetSha256"))
    _write(paths["candidates"], _seal({"schemaVersion": "temporal_qd_native_candidate_envelope_set_v1", "nativeAuthoritySha256": authority["authoritySha256"], "candidateCount": 12, "candidates": candidates}, "candidateSetSha256"))
    payload_index = _seal({"schemaVersion": "temporal_qd_native_candidate_payload_index_v1", "nativeAuthoritySha256": authority["authoritySha256"], "candidateCount": 12, "candidates": [{"candidateId": row["candidateId"], "payloadSha256": row["payloadSha256"], "blockId": row["blockId"], "arm": row["arm"]} for row in candidates]}, "payloadIndexSha256")
    _write(paths["payloadIndex"], payload_index)
    ledger = _seal({"schemaVersion": "temporal_qd_topology_candidate_ledger_v1", "candidateCount": 12, "rows": [{"candidateId": row["candidateId"], "candidatePayloadSha256": row["payloadSha256"], "candidateIdentitySha256": row["candidateIdentitySha256"], "pairIdentitySha256": row["compiledPair"]["pairIdentitySha256"], "rawProfileSha256": row["compiledPair"]["identityMaterial"]["rawProfileSha256"], "profileSnapshotSha256": row["compiledPair"]["identityMaterial"]["profileSnapshotSha256"], "programSha256": row["compiledPair"]["identityMaterial"]["programSha256"], "validationReportSha256": row["compiledPair"]["identityMaterial"]["validationReportSha256"], "parentCandidateId": row["parentCandidateId"], "blockId": row["blockId"], "arm": row["arm"]} for row in candidates]}, "ledgerSha256")
    _write(paths["ledger"], ledger)
    _write(paths["evaluationAuthorities"], evaluation_authorities)
    _write_pretty(paths["campaignAuthority"], campaign)
    freeze_result, task_manifest = _freeze(paths["campaignAuthority"], freeze_root)
    task_index = _task_index(task_manifest, campaign["developmentWindows"])
    _write(paths["taskIndex"], task_index)
    confirmation = _confirmation(candidates, evaluation_authorities)
    _write(paths["confirmation"], confirmation)
    semantic = _semantic_decision(differential)
    _write(paths["semantic"], semantic)
    paths["semanticMemo"].write_text(
        "# Rust/Dashboard semantic-contract decision\n\n"
        f"Decision identity: `{semantic['decisionSha256']}`\n\n"
        "The retained V38 target reconstructs and recompiles exactly through the Rust authority. "
        "The pinned Dashboard oracle materializes `holdPolicy.onBreach=exit_next_open`, while Rust "
        "canonicalizes the same documented/default behavior to omission. That changes raw/program "
        "identity material but not the observed execution rule. Validation capability ordering and "
        "extra capability metadata are report-only differences.\n\n"
        "Rust matches the explicit native contract and historical V38 identities. Dashboard is "
        "behaviorally equivalent for the reviewed difference but remains a historical/observational "
        "oracle, not an identity authority for new native work. Neither implementation demonstrates "
        "a semantic defect in this bounded corpus. Proceed with native authority v1 without changing "
        "Rust semantics or rewriting V37/V38 artifacts.\n",
        encoding="utf-8",
        newline="\n",
    )
    science = _science_contract(_json(TOPOLOGY_SPEC))
    _write(paths["science"], science)
    gates = {
        "exactRustCanonicalCandidateCount": 12,
        "rustValidationPassCount": 12,
        "completeBlockCount": 3,
        "workerConsumesFrozenProfileUnchanged": True,
        "campaignTaskValidationPassed": True,
        "allTasksUseCandidateWindowJobV2": True,
        "dedicatedWorkerCapabilityRequired": True,
        "immutableWorkerImageDigestBound": True,
        "typedExecutionReceiptRequired": True,
        "receiptAdmissionImplemented": True,
        "realWorkerReplayConformancePassed": launch_evidence[
            "realWorkerReplayConformancePassed"
        ],
        "expandedAdversarialAdmissionPassed": launch_evidence[
            "expandedAdversarialAdmissionPassed"
        ],
        "productionCampaignSealAdmissionPassed": launch_evidence[
            "productionCampaignSealAdmissionPassed"
        ],
        "productionGatewayDispatchAdmissionPassed": launch_evidence[
            "productionGatewayDispatchAdmissionPassed"
        ],
        "productionOfflineCampaignSealPassed": launch_evidence[
            "productionOfflineCampaignSealPassed"
        ],
        "allExactV2WorkerResultsAcceptedThroughProductionAdmission": launch_evidence[
            "allExactV2WorkerResultsAcceptedThroughProductionAdmission"
        ],
        "allAdversarialV2ResultsRejectedThroughProductionAdmission": launch_evidence[
            "allAdversarialV2ResultsRejectedThroughProductionAdmission"
        ],
        "crossRootDeterminismPassed": launch_evidence[
            "crossRootDeterminismPassed"
        ],
        "inspectedTaskCount": 144,
        "uniqueCandidateWindowCount": 144,
        "differentialEvidenceComplete": True,
        "untouchedConfirmationPreregistered": True,
        "untouchedProjectedTaskCount": 48,
        "noTaskDispatched": True,
        "noMarketEvaluation": True,
        "noGeneration": True,
        "noProductionArchiveWrite": True,
    }
    go_nogo = _seal(
        {
            "schemaVersion": "temporal_qd_topology_launch_go_nogo_v3",
            "readyForTopologyCaseStudyLaunch": launch_evidence[
                "launchEvidenceComplete"
            ],
            "gates": gates,
            "launchGateEvidence": launch_evidence,
            "authoritySha256": authority["authoritySha256"],
            "campaignAuthorityId": campaign["authorityId"],
            "taskMatrixSha256": freeze_result["taskMatrixSha256"],
            "taskIndexSha256": task_index["taskIndexSha256"],
            "workerContractSha256": worker_contract["workerContractSha256"],
            "workerImageDigest": worker_contract["imageDigest"],
            "workerSourceGitCommit": worker_contract["sourceGitCommit"],
            "preregistrationSha256": confirmation["preregistrationSha256"],
            "semanticDecisionSha256": semantic["decisionSha256"],
            "scientificContractSha256": science["scientificContractSha256"],
        },
        "goNogoSha256",
    )
    _write(paths["goNogo"], go_nogo)
    package = _seal(
        {
            "schemaVersion": "temporal_qd_topology_no_dispatch_launch_package_v3",
            "nativeAuthoritySha256": authority["authoritySha256"],
            "campaignAuthorityId": campaign["authorityId"],
            "candidateCount": 12,
            "inspectedTaskCount": 144,
            "untouchedProjectedTaskCount": 48,
            "workerContract": worker_contract,
            "launchGateEvidence": launch_evidence,
            "artifacts": {
                key: {"path": path.name, "rawSha256": _sha_file(path)}
                for key, path in paths.items()
                if key != "package"
            },
            "externalUncommittedTaskFreeze": {
                "storageDisposition": "external_uncommitted_raw_task_manifest",
                "taskManifestRawSha256": _sha_file(
                    freeze_root / "task-manifest.json"
                ),
                "rawTaskPackCommitted": False,
            },
            "dispatchEnabled": False,
        },
        "packageSha256",
    )
    _write(paths["package"], package)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_V2)
    parser.add_argument("--freeze-root", type=Path)
    parser.add_argument("--rust-source-commit", default=str(_git("rev-parse", "HEAD")))
    parser.add_argument("--worker-contract", type=Path, required=True)
    parser.add_argument("--catalog-sha256", required=True)
    parser.add_argument("--conformance-report", type=Path)
    parser.add_argument("--production-admission-report", type=Path)
    parser.add_argument("--portability-report", type=Path)
    parser.add_argument("--result-admission-source-commit")
    parser.add_argument("--preserve-native-package-commit")
    parser.add_argument("--allow-uncommitted-source", action="store_true")
    args = parser.parse_args()
    freeze_root = args.freeze_root or Path(tempfile.gettempdir()) / "fuzzfolio-rust-canonical-topology-inspected-v1"
    for name, path in run(
        args.output_dir,
        freeze_root,
        source_commit=args.rust_source_commit,
        worker_contract_path=args.worker_contract,
        catalog_sha256=args.catalog_sha256,
        conformance_report_path=args.conformance_report,
        production_admission_report_path=args.production_admission_report,
        portability_report_path=args.portability_report,
        result_admission_source_commit=args.result_admission_source_commit,
        preserve_native_package_commit=args.preserve_native_package_commit,
        allow_uncommitted_source=args.allow_uncommitted_source,
    ).items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
