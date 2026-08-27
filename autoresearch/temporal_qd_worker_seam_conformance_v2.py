"""Run the exact cross-repository Rust-precompiled seam without market data."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Callable

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_search import (
    TemporalSearchContractError,
    _validate_precompiled_execution_receipt,
)

ROOT = Path(__file__).resolve().parents[1]
RUST_ROOT = ROOT / "rust/temporal-qd"
RECEIPT_BIN = (
    RUST_ROOT / "target/debug/temporal-qd-precompiled-receipt-jsonl.exe"
)
CAMPAIGN_ADMISSION_BIN = (
    RUST_ROOT / "target/debug/temporal-qd-campaign-admission-jsonl.exe"
)
PRODUCTION_SOURCE_PATHS = {
    "campaignAdmissionAdapter": RUST_ROOT
    / "crates/qd-campaign-seal/src/bin/temporal-qd-campaign-admission-jsonl.rs",
    "campaignSeal": RUST_ROOT / "crates/qd-campaign-seal/src/lib.rs",
    "gatewayDispatch": RUST_ROOT / "crates/qd-gateway-dispatch/src/lib.rs",
    "sharedReceiptValidator": RUST_ROOT / "crates/qd-campaign-freeze/src/lib.rs",
}


def _sha_file(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _source_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _assert_tracked_worktree_clean(source_commit: str) -> None:
    if _source_commit() != source_commit:
        raise RuntimeError("production admission source commit changed during run")
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if status:
        raise RuntimeError(
            "production admission conformance requires a clean tracked worktree"
        )


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
            f"production admission source differs from {source_commit}: {relative}"
        )
    completed = subprocess.run(
        ["git", "show", f"{source_commit}:{relative}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return f"sha256:{hashlib.sha256(completed.stdout).hexdigest()}"


def _reseal_receipt(result: dict[str, Any]) -> None:
    receipt = result["precompiled_profile_execution_receipt"]
    identity = dict(receipt)
    identity.pop("receipt_sha256", None)
    receipt["receipt_sha256"] = canonical_sha256(identity)


def _reseal_attestation(result: dict[str, Any]) -> None:
    attestation = result.get("runtime_program_identity_attestation")
    if not isinstance(attestation, dict):
        return
    identity = dict(attestation)
    identity.pop("attestation_sha256", None)
    attestation["attestation_sha256"] = canonical_sha256(identity)


def _reseal_result(result: dict[str, Any]) -> None:
    if "artifact_sha256" not in result or "artifact_size_bytes" not in result:
        return
    material = copy.deepcopy(result)
    material.pop("artifact_sha256", None)
    material.pop("artifact_size_bytes", None)
    diagnostics = dict(material.get("diagnostics") or {})
    diagnostics.pop("artifact_size_bytes", None)
    material["diagnostics"] = diagnostics
    artifact_sha256 = canonical_sha256(material)
    size_bytes = 1
    for _ in range(16):
        frozen = dict(
            material,
            artifact_sha256=artifact_sha256,
            artifact_size_bytes=size_bytes,
        )
        frozen["diagnostics"] = dict(
            diagnostics,
            artifact_size_bytes=size_bytes,
        )
        next_size = len(canonical_json(frozen).encode("utf-8"))
        if next_size == size_bytes:
            result.clear()
            result.update(frozen)
            return
        size_bytes = next_size
    raise RuntimeError("tampered result artifact size did not converge")


def _rust_admits(task: dict[str, Any], result: dict[str, Any]) -> bool:
    request = {
        "schemaVersion": "temporal_qd_precompiled_receipt_admission_request_v1",
        "task": task,
        "result": result,
    }
    completed = subprocess.run(
        [str(RECEIPT_BIN)],
        input=json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n",
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    return completed.returncode == 0


def _python_admits(task: dict[str, Any], result: dict[str, Any]) -> bool:
    try:
        _validate_precompiled_execution_receipt(
            result,
            job=task["payload"],
            required=True,
        )
    except (KeyError, TypeError, TemporalSearchContractError):
        return False
    return True


def _campaign_seal_admits(task: dict[str, Any], result: dict[str, Any]) -> bool:
    request = {
        "schemaVersion": "temporal_qd_campaign_admission_request_v1",
        "task": task,
        "result": result,
    }
    completed = subprocess.run(
        [str(CAMPAIGN_ADMISSION_BIN)],
        input=json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n",
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    return completed.returncode == 0


def _build_admission_binaries() -> dict[str, str]:
    builds = (
        (
            "temporal-qd-campaign-freeze",
            "temporal-qd-precompiled-receipt-jsonl",
            RECEIPT_BIN,
        ),
        (
            "temporal-qd-campaign-seal",
            "temporal-qd-campaign-admission-jsonl",
            CAMPAIGN_ADMISSION_BIN,
        ),
    )
    hashes: dict[str, str] = {}
    for package, binary, path in builds:
        subprocess.run(
            ["cargo", "build", "-q", "-p", package, "--bin", binary],
            cwd=RUST_ROOT,
            check=True,
        )
        if not path.is_file():
            raise RuntimeError(f"built admission binary is unavailable: {binary}")
        hashes[binary] = _sha_file(path)
    return hashes


def _run_production_fixture_test(package: str, test_name: str) -> None:
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "-q",
            "-p",
            package,
            "--lib",
            test_name,
            "--",
            "--exact",
        ],
        cwd=RUST_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    combined = completed.stdout + completed.stderr
    if completed.returncode != 0:
        raise RuntimeError(
            f"production fixture test failed: {package}::{test_name}\n{combined}"
        )
    summaries = re.findall(
        r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
        r"(\d+) measured; (\d+) filtered out",
        combined,
    )
    if len(summaries) != 1 or summaries[0][:4] != ("1", "0", "0", "0"):
        raise RuntimeError(
            f"production fixture test did not execute exactly one test: "
            f"{package}::{test_name}\n{combined}"
        )


def _mutation(
    task: dict[str, Any],
    result: dict[str, Any],
    mutate: Callable[[dict[str, Any], dict[str, Any]], None],
    *,
    reseal: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    changed_task = copy.deepcopy(task)
    changed_result = copy.deepcopy(result)
    mutate(changed_task, changed_result)
    if reseal and changed_result.get("precompiled_profile_execution_receipt"):
        _reseal_receipt(changed_result)
    _reseal_attestation(changed_result)
    _reseal_result(changed_result)
    return changed_task, changed_result


def _replace_all_runtime_program_identities(
    _task: dict[str, Any],
    result: dict[str, Any],
    program_sha256: str,
) -> None:
    result["program_sha256"] = program_sha256
    receipt = result["precompiled_profile_execution_receipt"]
    receipt["resolved_program_sha256"] = program_sha256
    attestation = result["runtime_program_identity_attestation"]
    for key in (
        "observation_stream_program_sha256",
        "checkpoint_program_sha256",
        "graph_runtime_program_sha256",
        "execution_state_program_sha256",
    ):
        attestation[key] = program_sha256
    for view in result["cost_view_results"].values():
        replay = view["replay_result"]
        replay["programSha256"] = program_sha256
        replay["finalGraphRuntime"]["programSha256"] = program_sha256
        replay["finalExecutionState"]["programSha256"] = program_sha256
        for trade in replay.get("trades") or []:
            trade["programSha256"] = program_sha256
        for trace in replay.get("executionTraces") or []:
            trace["programSha256"] = program_sha256


def run(
    *,
    dashboard_root: Path,
    task_manifest: Path,
    worker_contract: Path,
    output: Path,
    fuzz_report: Path,
) -> dict[str, Any]:
    dashboard_python = dashboard_root / "compute-service/.venv/Scripts/python.exe"
    dashboard_script = dashboard_root / "scripts/temporal_qd_precompiled_conformance.py"
    if not dashboard_python.is_file() or not dashboard_script.is_file():
        raise RuntimeError("exact FuzzFolio conformance runtime is unavailable")
    source_commit = _source_commit()
    _assert_tracked_worktree_clean(source_commit)
    production_binary_hashes = _build_admission_binaries()
    fuzz_report_path = fuzz_report
    fuzz_report = json.loads(fuzz_report_path.read_text(encoding="utf-8"))
    fixtures = fuzz_report.get("executedFixtures") or []
    exact_image_complete = (
        len(fixtures) == 12
        and fuzz_report.get("replayExecuted") is True
        and fuzz_report.get("fullWorkerExecutionCandidateCount") == 12
        and fuzz_report.get("runtimeWorkerContractUsed") is True
        and fuzz_report.get("catalogVerificationExecuted") is True
        and fuzz_report.get("sourceProfileRewriteCount") == 0
        and fuzz_report.get("networkEnabled") is False
        and fuzz_report.get("marketDataRead") is False
        and fuzz_report.get("gatewayContact") is False
        and fuzz_report.get("taskDispatchCount") == 0
    )
    if not exact_image_complete:
        raise RuntimeError(
            "FuzzFolio exact-image conformance did not execute all twelve candidates "
            "through the runtime contract with network and dispatch disabled"
        )
    for executed in fixtures:
        if (
            not _python_admits(executed["task"], executed["result"])
            or not _rust_admits(executed["task"], executed["result"])
            or not _campaign_seal_admits(executed["task"], executed["result"])
        ):
            raise RuntimeError("a real FuzzFolio worker result was not admitted")
    fixture = fuzz_report["fixture"]
    task = fixture["task"]
    result = fixture["result"]
    if (
        not _python_admits(task, result)
        or not _rust_admits(task, result)
        or not _campaign_seal_admits(task, result)
    ):
        raise RuntimeError("exact FuzzFolio-produced receipt was not admitted")

    _run_production_fixture_test(
        "temporal-qd-gateway-dispatch",
        "tests::exact_v2_worker_result_is_durable_before_ack_and_runtime_tamper_is_not_acknowledged",
    )
    _run_production_fixture_test(
        "temporal-qd-campaign-seal",
        "tests::offline_index_reduction_accepts_exact_v2_and_preserves_raw_receipt_provenance",
    )

    historical_program_sha256 = fixture["historicalDashboardProgramSha256"]
    receipt_mutations: list[
        tuple[str, Callable[[dict[str, Any], dict[str, Any]], None], bool]
    ] = [
        ("contract_hash", lambda _t, r: r["precompiled_profile_execution_receipt"].update(precompiled_contract_sha256="sha256:" + "0" * 64), True),
        ("candidate_id", lambda _t, r: r["precompiled_profile_execution_receipt"].update(candidate_id="cross_candidate"), True),
        ("rust_authority", lambda _t, r: r["precompiled_profile_execution_receipt"].update(rust_authority_sha256="sha256:" + "0" * 64), True),
        ("raw_profile", lambda _t, r: r["precompiled_profile_execution_receipt"].update(raw_source_profile_sha256="sha256:" + "0" * 64), True),
        ("normalized_profile", lambda _t, r: r["precompiled_profile_execution_receipt"].update(normalized_source_profile_sha256="sha256:" + "0" * 64), True),
        ("authored_program", lambda _t, r: r["precompiled_profile_execution_receipt"].update(authored_program_sha256="sha256:" + "0" * 64), True),
        ("resolved_profile", lambda _t, r: r["precompiled_profile_execution_receipt"].update(resolved_profile_sha256="sha256:" + "0" * 64), True),
        ("resolved_program", lambda _t, r: r["precompiled_profile_execution_receipt"].update(resolved_program_sha256="sha256:" + "0" * 64), True),
        ("worker_contract_hash", lambda _t, r: r["precompiled_profile_execution_receipt"].update(worker_contract_hash="sha256:" + "0" * 64), True),
        ("missing_receipt", lambda _t, r: r.pop("precompiled_profile_execution_receipt"), False),
        ("source_rewritten", lambda _t, r: r["precompiled_profile_execution_receipt"].update(source_profile_rewritten=True), True),
        ("pair_recompiled", lambda _t, r: r["precompiled_profile_execution_receipt"].update(pair_recompile_attempted=True), True),
        ("unknown_receipt_field", lambda _t, r: r["precompiled_profile_execution_receipt"].update(unknown=True), True),
        ("contract_removed", lambda t, _r: t["payload"].pop("precompiled_profile_execution_contract"), False),
        (
            "dedicated_capability_removed",
            lambda t, _r: t["payload"]["required_capabilities"].remove(
                t["payload"]["precompiled_profile_execution_contract"][
                    "schemaVersion"
                ]
            ),
            False,
        ),
        ("legacy_worker_contract", lambda t, r: (t["payload"].update(required_worker_contract_schema="replay-worker-contract-v1"), r["precompiled_profile_execution_receipt"].update(worker_contract_schema="replay-worker-contract-v1")), True),
        ("worker_image_digest", lambda _t, r: r["precompiled_profile_execution_receipt"].update(worker_image_digest="sha256:" + "0" * 64), True),
        ("worker_image_identity_mode", lambda _t, r: r["precompiled_profile_execution_receipt"].update(worker_image_identity_mode="local_unattested"), True),
        ("worker_source_git_commit", lambda _t, r: r["precompiled_profile_execution_receipt"].update(worker_source_git_commit="0" * 40), True),
        ("worker_rust_core_hash", lambda _t, r: r["precompiled_profile_execution_receipt"].update(rust_core_hash="sha256:" + "0" * 64), True),
        ("rust_build_info", lambda _t, r: r["precompiled_profile_execution_receipt"]["rust_build_info"].update(target_os="tampered"), True),
        ("runtime_platform", lambda _t, r: r["precompiled_profile_execution_receipt"]["runtime_platform"].update(system="Tampered"), True),
        ("result_replay_program", lambda _t, r: r["cost_view_results"]["research_conservative"]["replay_result"].update(programSha256="sha256:" + "0" * 64), True),
        ("checkpoint_program", lambda _t, r: r["runtime_program_identity_attestation"].update(checkpoint_program_sha256="sha256:" + "0" * 64), True),
        ("observation_stream_program", lambda _t, r: r["runtime_program_identity_attestation"].update(observation_stream_program_sha256="sha256:" + "0" * 64), True),
        ("historical_python_program_substitution", lambda t, r: _replace_all_runtime_program_identities(t, r, historical_program_sha256), True),
    ]
    adversarial = []
    fuzz_batch = [
        {
            "case": f"exact:{executed['task']['payload']['candidate_id']}",
            "task": executed["task"],
            "result": executed["result"],
        }
        for executed in fixtures
    ]
    for name, mutate, reseal in receipt_mutations:
        changed_task, changed_result = _mutation(
            task, result, mutate, reseal=reseal
        )
        python_rejected = not _python_admits(changed_task, changed_result)
        rust_rejected = not _rust_admits(changed_task, changed_result)
        campaign_seal_rejected = not _campaign_seal_admits(
            changed_task, changed_result
        )
        if not python_rejected or not rust_rejected or not campaign_seal_rejected:
            raise RuntimeError(f"cross-repository tamper fixture was admitted: {name}")
        adversarial.append(
            {
                "case": name,
                "taskSha256": canonical_sha256(changed_task),
                "resultSha256": canonical_sha256(changed_result),
                "pythonAdmissionRejected": python_rejected,
                "rustAdmissionRejected": rust_rejected,
                "productionCampaignSealRejected": campaign_seal_rejected,
            }
        )
        fuzz_batch.append(
            {"case": name, "task": changed_task, "result": changed_result}
        )
    fuzz_batch_path = output.with_name(output.stem + ".adversarial-input.json")
    fuzz_admission_path = output.with_name(
        output.stem + ".fuzzfolio-adversarial.json"
    )
    fuzz_batch_path.parent.mkdir(parents=True, exist_ok=True)
    fuzz_batch_path.write_text(
        json.dumps(fuzz_batch, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    subprocess.run(
        [
            str(dashboard_python),
            str(dashboard_script),
            "--admission-batch",
            str(fuzz_batch_path),
            "--worker-contract",
            str(worker_contract),
            "--output",
            str(fuzz_admission_path),
        ],
        cwd=dashboard_root,
        check=True,
    )
    fuzz_admission = json.loads(fuzz_admission_path.read_text(encoding="utf-8"))
    fuzz_rows = {row["case"]: row for row in fuzz_admission["cases"]}
    for executed in fixtures:
        exact_name = f"exact:{executed['task']['payload']['candidate_id']}"
        if fuzz_rows[exact_name]["admitted"] is not True:
            raise RuntimeError(f"FuzzFolio rejected exact worker result: {exact_name}")
    for row in adversarial:
        fuzz_rejected = fuzz_rows[row["case"]]["admitted"] is False
        if not fuzz_rejected:
            raise RuntimeError(
                f"FuzzFolio admitted adversarial worker result: {row['case']}"
            )
        row["fuzzFolioRejected"] = True
    output.parent.mkdir(parents=True, exist_ok=True)
    production_report_path = output.with_name(
        output.stem + ".production-admission.json"
    )
    production_report: dict[str, Any] = {
        "schemaVersion": "temporal_qd_production_result_admission_report_v1",
        "sourceCommit": source_commit,
        "sourceHashes": {
            name: _git_blob_sha256(source_commit, path)
            for name, path in sorted(PRODUCTION_SOURCE_PATHS.items())
        },
        "productionAdmissionPolicy": "campaign_seal_shared_receipt_v2_2",
        "productionBinaryHashes": production_binary_hashes,
        "workerContractHash": fuzz_report["workerContractHash"],
        "workerImageDigest": fuzz_report["workerImageDigest"],
        "marketDataRead": False,
        "gatewayNetworkAccess": False,
        "taskDispatchCount": 0,
        "productionCampaignSealExactAcceptCount": len(fixtures),
        "productionCampaignSealAdversarialRejectCount": len(adversarial),
        "productionGatewayDispatchFixturePassed": True,
        "productionOfflineSealFixturePassed": True,
        "exactFixtures": [
            {
                "candidateId": executed["task"]["payload"]["candidate_id"],
                "taskId": executed["task"]["task_id"],
                "taskSha256": canonical_sha256(executed["task"]),
                "resultSha256": canonical_sha256(executed["result"]),
            }
            for executed in fixtures
        ],
        "adversarialCases": [
            {
                "case": row["case"],
                "taskSha256": row["taskSha256"],
                "resultSha256": row["resultSha256"],
                "productionCampaignSealRejected": row[
                    "productionCampaignSealRejected"
                ],
            }
            for row in adversarial
        ],
    }
    production_report["reportSha256"] = canonical_sha256(production_report)
    production_report_path.write_text(
        json.dumps(production_report, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
        newline="\n",
    )

    report: dict[str, Any] = {
        "schemaVersion": "temporal_qd_worker_seam_conformance_report_v2_3",
        "marketDataRead": False,
        "replayExecuted": True,
        "fullWorkerExecutionFixtureCount": len(fixtures),
        "fullWorkerExecutionCandidateCount": fuzz_report[
            "fullWorkerExecutionCandidateCount"
        ],
        "runtimeWorkerContractUsed": fuzz_report["runtimeWorkerContractUsed"],
        "catalogVerificationExecuted": fuzz_report[
            "catalogVerificationExecuted"
        ],
        "sourceProfileRewriteCount": fuzz_report["sourceProfileRewriteCount"],
        "networkEnabled": fuzz_report["networkEnabled"],
        "gatewayContact": fuzz_report["gatewayContact"],
        "taskDispatchCount": fuzz_report["taskDispatchCount"],
        "fullWorkerExecutionCandidateIds": [
            executed["task"]["payload"]["candidate_id"] for executed in fixtures
        ],
        "exactFixtureAcceptedByFuzzFolio": True,
        "exactFixtureAcceptedByPythonAdmission": True,
        "exactFixtureAcceptedByRustAdmission": True,
        "exactWorkerResultsAcceptedByFuzzFolio": len(fixtures),
        "exactWorkerResultsAcceptedByPythonAdmission": len(fixtures),
        "exactWorkerResultsAcceptedByRustAdmission": len(fixtures),
        "productionCampaignSealExactAcceptCount": len(fixtures),
        "productionCampaignSealAdversarialRejectCount": len(adversarial),
        "productionGatewayDispatchFixturePassed": True,
        "productionOfflineSealFixturePassed": True,
        "exactFixtures": production_report["exactFixtures"],
        "productionAdmissionReport": {
            "logicalId": production_report_path.name,
            "rawSha256": _sha_file(production_report_path),
            "reportSha256": production_report["reportSha256"],
        },
        "validatedTaskCount": fuzz_report["validatedTaskCount"],
        "workerContractHash": fuzz_report["workerContractHash"],
        "workerImageDigest": fuzz_report["workerImageDigest"],
        "fixtureTaskId": task["task_id"],
        "adversarialCases": adversarial,
        "adversarialRejectCount": len(adversarial),
    }
    report["reportSha256"] = canonical_sha256(report)
    output.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard-root", type=Path, required=True)
    parser.add_argument("--task-manifest", type=Path, required=True)
    parser.add_argument("--worker-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fuzz-report", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                dashboard_root=args.dashboard_root,
                task_manifest=args.task_manifest,
                worker_contract=args.worker_contract,
                output=args.output,
                fuzz_report=args.fuzz_report,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
