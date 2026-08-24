"""Run the exact cross-repository Rust-precompiled seam without market data."""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from .evidence_plan import canonical_sha256
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
    return changed_task, changed_result


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
    fixture = fuzz_report["fixture"]
    task = fixture["task"]
    result = fixture["result"]
    if not _python_admits(task, result) or not _rust_admits(task, result):
        raise RuntimeError("exact FuzzFolio-produced receipt was not admitted")

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
    ]
    adversarial = []
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
    report: dict[str, Any] = {
        "schemaVersion": "temporal_qd_worker_seam_conformance_report_v2",
        "marketDataRead": False,
        "replayExecuted": False,
        "exactFixtureAcceptedByFuzzFolio": True,
        "exactFixtureAcceptedByPythonAdmission": True,
        "exactFixtureAcceptedByRustAdmission": True,
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
