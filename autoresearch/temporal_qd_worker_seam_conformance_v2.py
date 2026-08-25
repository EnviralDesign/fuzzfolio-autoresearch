"""Run the exact cross-repository Rust-precompiled seam without market data."""

from __future__ import annotations

import argparse
import copy
import json
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
) -> dict[str, Any]:
    dashboard_python = dashboard_root / "compute-service/.venv/Scripts/python.exe"
    dashboard_script = dashboard_root / "scripts/temporal_qd_precompiled_conformance.py"
    if not dashboard_python.is_file() or not dashboard_script.is_file():
        raise RuntimeError("exact FuzzFolio conformance runtime is unavailable")
    if not RECEIPT_BIN.is_file():
        raise RuntimeError("Rust precompiled-receipt admission binary is unavailable")
    fuzz_report_path = output.with_suffix(".fuzzfolio.json")
    subprocess.run(
        [
            str(dashboard_python),
            str(dashboard_script),
            "--task-manifest",
            str(task_manifest),
            "--worker-contract",
            str(worker_contract),
            "--output",
            str(fuzz_report_path),
        ],
        cwd=dashboard_root,
        check=True,
    )
    fuzz_report = json.loads(fuzz_report_path.read_text(encoding="utf-8"))
    fixtures = fuzz_report.get("executedFixtures") or []
    if len(fixtures) < 5 or fuzz_report.get("replayExecuted") is not True:
        raise RuntimeError("FuzzFolio did not produce five real worker execution fixtures")
    for executed in fixtures:
        if not _python_admits(executed["task"], executed["result"]) or not _rust_admits(
            executed["task"], executed["result"]
        ):
            raise RuntimeError("a real FuzzFolio worker result was not admitted")
    fixture = fuzz_report["fixture"]
    task = fixture["task"]
    result = fixture["result"]
    if not _python_admits(task, result) or not _rust_admits(task, result):
        raise RuntimeError("exact FuzzFolio-produced receipt was not admitted")

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
        ("dedicated_capability_removed", lambda t, _r: t["payload"]["required_capabilities"].remove("temporal_qd_precompiled_profile_execution_v1"), False),
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
        if not python_rejected or not rust_rejected:
            raise RuntimeError(f"cross-repository tamper fixture was admitted: {name}")
        adversarial.append(
            {
                "case": name,
                "pythonAdmissionRejected": python_rejected,
                "rustAdmissionRejected": rust_rejected,
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
    report: dict[str, Any] = {
        "schemaVersion": "temporal_qd_worker_seam_conformance_report_v2_1",
        "marketDataRead": False,
        "replayExecuted": True,
        "fullWorkerExecutionFixtureCount": len(fixtures),
        "fullWorkerExecutionCandidateIds": [
            executed["task"]["payload"]["candidate_id"] for executed in fixtures
        ],
        "exactFixtureAcceptedByFuzzFolio": True,
        "exactFixtureAcceptedByPythonAdmission": True,
        "exactFixtureAcceptedByRustAdmission": True,
        "exactWorkerResultsAcceptedByFuzzFolio": len(fixtures),
        "exactWorkerResultsAcceptedByPythonAdmission": len(fixtures),
        "exactWorkerResultsAcceptedByRustAdmission": len(fixtures),
        "validatedTaskCount": fuzz_report["validatedTaskCount"],
        "workerContractHash": fuzz_report["workerContractHash"],
        "workerImageDigest": fuzz_report["workerImageDigest"],
        "fixtureTaskId": task["task_id"],
        "adversarialCases": adversarial,
        "adversarialRejectCount": len(adversarial),
    }
    report["reportSha256"] = canonical_sha256(report)
    output.parent.mkdir(parents=True, exist_ok=True)
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
    args = parser.parse_args()
    print(
        json.dumps(
            run(
                dashboard_root=args.dashboard_root,
                task_manifest=args.task_manifest,
                worker_contract=args.worker_contract,
                output=args.output,
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
