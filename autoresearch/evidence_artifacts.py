from __future__ import annotations

import json
import hashlib
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from .evidence_plan import ReplayEvidencePlan, validate_replay_evidence_plan
from .nested_evidence import FrozenExecutionCellReceipt


EVIDENCE_ARTIFACT_SCHEMA = "autoresearch-evidence-artifact-bundle-v1"
EVIDENCE_ROOT_DIRNAME = "evidence"


@dataclass(frozen=True)
class EvidenceArtifactPaths:
    root: Path
    result: Path
    curve: Path
    calendar_curve: Path
    recommended_curve: Path
    manifest: Path
    job: Path
    cell_receipt: Path


def evidence_artifact_paths(
    artifact_dir: Path,
    evidence_plan: ReplayEvidencePlan | dict[str, Any],
) -> EvidenceArtifactPaths:
    plan = validate_replay_evidence_plan(evidence_plan)
    digest = plan.plan_id.removeprefix("sha256:")
    root = (
        Path(artifact_dir)
        / EVIDENCE_ROOT_DIRNAME
        / "full-backtest"
        / digest
    )
    curve_name = (
        "tracked-cell-path-detail.json"
        if plan.evidence_role == "outer_test"
        else "best-cell-path-detail.json"
    )
    return EvidenceArtifactPaths(
        root=root,
        result=root / "result.json",
        curve=root / curve_name,
        calendar_curve=root / "calendar-curve.json",
        recommended_curve=root / "recommended-cell-path-detail.json",
        manifest=root / "manifest.json",
        job=root / "deep-replay-job.json",
        cell_receipt=root / "frozen-execution-cell.json",
    )


def build_evidence_artifact_manifest(
    *,
    evidence_plan: ReplayEvidencePlan | dict[str, Any],
    provenance: dict[str, Any],
    execution_evidence: dict[str, Any] | None,
    artifact_payloads: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = validate_replay_evidence_plan(evidence_plan)
    return {
        "schema": EVIDENCE_ARTIFACT_SCHEMA,
        "evidence_plan": plan.model_dump(mode="json"),
        "evidence_plan_id": plan.plan_id,
        "evidence_role": plan.evidence_role,
        "requested_horizon_months": plan.requested_horizon_months,
        "selection_data_end": plan.selection_data_end,
        "analysis_window_start": plan.analysis_window_start,
        "analysis_window_end": plan.analysis_window_end,
        "profile_snapshot_sha256": plan.profile_snapshot_sha256,
        "execution_cell_sha256": plan.execution_cell_sha256,
        "lake_manifest_sha256": plan.lake_manifest_sha256,
        "coverage_policy": plan.coverage_policy,
        "provenance": provenance,
        "execution_evidence": execution_evidence,
        "artifact_sha256": {
            name: canonical_payload_sha256(payload)
            for name, payload in sorted((artifact_payloads or {}).items())
        },
    }


def canonical_payload_sha256(payload: Any) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def write_immutable_json(path: Path, payload: Any) -> None:
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        try:
            existing = json.dumps(
                json.loads(existing),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        if existing != serialized:
            raise RuntimeError(f"Immutable evidence artifact already exists with different content: {path}")
        return
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid4().hex}.tmp"
    )
    temporary.write_text(serialized, encoding="utf-8")
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = path.read_text(encoding="utf-8")
            try:
                existing = json.dumps(
                    json.loads(existing),
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
            if existing != serialized:
                raise RuntimeError(
                    "Immutable evidence artifact was concurrently published with "
                    f"different content: {path}"
                )
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def evidence_bundle_lock(root: Path, *, timeout_seconds: float = 30.0):
    root.parent.mkdir(parents=True, exist_ok=True)
    lock_path = root.parent / f".{root.name}.publish.lock"
    handle = lock_path.open("a+b")
    if lock_path.stat().st_size == 0:
        handle.write(b"\0")
        handle.flush()
    deadline = time.monotonic() + max(float(timeout_seconds), 0.0)
    acquired = False
    try:
        while not acquired:
            handle.seek(0)
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out waiting for evidence bundle lock: {lock_path}"
                    )
                time.sleep(0.05)
        yield
    finally:
        if acquired:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _validate_terminal_outcome_bundle(
    paths: EvidenceArtifactPaths,
    plan: ReplayEvidencePlan,
) -> dict[str, Any] | None:
    if not paths.manifest.exists():
        return None
    try:
        manifest = json.loads(paths.manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    terminal = manifest.get("terminal_outcome") if isinstance(manifest, dict) else None
    if not isinstance(terminal, dict):
        return None

    required = {
        "result": paths.result,
        "manifest": paths.manifest,
        "job": paths.job,
    }
    missing = [name for name, path in required.items() if not path.exists()]
    reasons: list[str] = []
    if missing:
        reasons.append("missing_artifact")
    if manifest.get("evidence_plan_id") != plan.plan_id:
        reasons.append("evidence_plan_mismatch")
    if terminal.get("status") != "nonviable" or terminal.get("outcome") != "no_valid_cell":
        reasons.append("invalid_terminal_outcome")

    execution_evidence = manifest.get("execution_evidence")
    if not isinstance(execution_evidence, dict):
        reasons.append("missing_execution_evidence")
    else:
        if execution_evidence.get("plan_id") != plan.plan_id:
            reasons.append("execution_plan_mismatch")
        if (
            execution_evidence.get("profile_snapshot_sha256")
            != plan.profile_snapshot_sha256
        ):
            reasons.append("execution_profile_mismatch")
        if (
            execution_evidence.get("execution_cell_sha256")
            != plan.execution_cell_sha256
        ):
            reasons.append("execution_cell_mismatch")
        if (
            plan.lake_manifest_sha256 is not None
            and execution_evidence.get("observed_lake_manifest_sha256")
            != plan.lake_manifest_sha256
        ):
            reasons.append("execution_lake_coverage_mismatch")

    artifact_hashes = manifest.get("artifact_sha256")
    if not isinstance(artifact_hashes, dict):
        reasons.append("missing_artifact_hashes")
    else:
        for name in ("result", "job"):
            path = required[name]
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                reasons.append(f"invalid_{name}")
                continue
            if artifact_hashes.get(name) != canonical_payload_sha256(payload):
                reasons.append(f"{name}_hash_mismatch")

    result_payload: dict[str, Any] = {}
    if paths.result.exists():
        try:
            loaded = json.loads(paths.result.read_text(encoding="utf-8"))
            result_payload = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    if (
        result_payload.get("status") != "nonviable"
        or result_payload.get("outcome") != "no_valid_cell"
        or result_payload.get("evidence_plan_id") != plan.plan_id
    ):
        reasons.append("invalid_terminal_result")
    diagnostics = result_payload.get("diagnostics")
    market_window = diagnostics.get("market_data_window") if isinstance(diagnostics, dict) else None
    try:
        filtered_bar_count = int(market_window.get("filtered_bar_count") or 0) if isinstance(market_window, dict) else 0
    except (TypeError, ValueError, OverflowError):
        filtered_bar_count = 0
    if (
        not isinstance(diagnostics, dict)
        or diagnostics.get("signal_count") != 0
        or diagnostics.get("resolved_trade_count_max") != 0
        or not isinstance(market_window, dict)
        or filtered_bar_count <= 0
    ):
        reasons.append("invalid_terminal_diagnostics")

    return {
        "status": "valid" if not reasons else ("missing" if missing else "invalid"),
        "rebuild_required": bool(reasons),
        "reason_codes": sorted(set(reasons)),
        "missing": missing,
        "terminal_outcome": terminal,
        "evidence_plan_id": plan.plan_id,
        "requested_horizon_months": plan.requested_horizon_months,
        "analysis_window_start": plan.analysis_window_start,
        "analysis_window_end": plan.analysis_window_end,
        "paths": {
            "result": str(paths.result),
            "curve": str(paths.curve),
            "calendar_curve": str(paths.calendar_curve),
            "recommended_curve": str(paths.recommended_curve),
            "manifest": str(paths.manifest),
            "job": str(paths.job),
            "cell_receipt": str(paths.cell_receipt),
        },
    }


def validate_evidence_artifact_bundle(
    artifact_dir: Path,
    evidence_plan: ReplayEvidencePlan | dict[str, Any],
) -> dict[str, Any]:
    plan = validate_replay_evidence_plan(evidence_plan)
    paths = evidence_artifact_paths(artifact_dir, plan)
    terminal_validation = _validate_terminal_outcome_bundle(paths, plan)
    if terminal_validation is not None:
        return terminal_validation
    required = {
        "result": paths.result,
        "curve": paths.curve,
        "manifest": paths.manifest,
        "job": paths.job,
    }
    if plan.evidence_role == "outer_test":
        required["cell_receipt"] = paths.cell_receipt
    else:
        required["calendar_curve"] = paths.calendar_curve
        required["recommended_curve"] = paths.recommended_curve
    missing = [name for name, path in required.items() if not path.exists()]
    reasons: list[str] = []
    if missing:
        reasons.append("missing_artifact")
    manifest: dict[str, Any] = {}
    loaded_artifacts: dict[str, Any] = {}
    if paths.manifest.exists():
        try:
            loaded = json.loads(paths.manifest.read_text(encoding="utf-8"))
            manifest = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            reasons.append("invalid_manifest")
        if manifest and manifest.get("evidence_plan_id") != plan.plan_id:
            reasons.append("evidence_plan_mismatch")
        if manifest and manifest.get("requested_horizon_months") != plan.requested_horizon_months:
            reasons.append("horizon_mismatch")
        if manifest and (
            manifest.get("analysis_window_start") != plan.analysis_window_start
            or manifest.get("analysis_window_end") != plan.analysis_window_end
        ):
            reasons.append("window_mismatch")
        execution_evidence = (
            manifest.get("execution_evidence") if isinstance(manifest, dict) else None
        )
        if not isinstance(execution_evidence, dict):
            reasons.append("missing_execution_evidence")
        else:
            if execution_evidence.get("plan_id") != plan.plan_id:
                reasons.append("execution_plan_mismatch")
            if (
                execution_evidence.get("profile_snapshot_sha256")
                != plan.profile_snapshot_sha256
            ):
                reasons.append("execution_profile_mismatch")
            if (
                execution_evidence.get("execution_cell_sha256")
                != plan.execution_cell_sha256
            ):
                reasons.append("execution_cell_mismatch")
            if (
                plan.lake_manifest_sha256 is not None
                and execution_evidence.get("observed_lake_manifest_sha256")
                != plan.lake_manifest_sha256
            ):
                reasons.append("execution_lake_coverage_mismatch")
        artifact_hashes = manifest.get("artifact_sha256")
        if not isinstance(artifact_hashes, dict):
            reasons.append("missing_artifact_hashes")
        else:
            for name, path in required.items():
                if name == "manifest" or not path.exists():
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    reasons.append(f"invalid_{name}")
                    continue
                loaded_artifacts[name] = payload
                if artifact_hashes.get(name) != canonical_payload_sha256(payload):
                    reasons.append(f"{name}_hash_mismatch")
        curve_payload = loaded_artifacts.get("curve")
        curve = curve_payload.get("curve") if isinstance(curve_payload, dict) else None
        if not isinstance(curve, dict) or not isinstance(curve.get("points"), list):
            reasons.append("invalid_curve_shape")
        if plan.evidence_role == "outer_test":
            receipt_payload = loaded_artifacts.get("cell_receipt")
            try:
                receipt = FrozenExecutionCellReceipt.model_validate(receipt_payload)
            except (TypeError, ValueError):
                reasons.append("invalid_cell_receipt")
            else:
                if receipt.execution_cell_sha256 != plan.execution_cell_sha256:
                    reasons.append("cell_receipt_plan_mismatch")
    return {
        "status": "valid" if not reasons else ("missing" if missing else "invalid"),
        "rebuild_required": bool(reasons),
        "reason_codes": sorted(set(reasons)),
        "missing": missing,
        "evidence_plan_id": plan.plan_id,
        "requested_horizon_months": plan.requested_horizon_months,
        "analysis_window_start": plan.analysis_window_start,
        "analysis_window_end": plan.analysis_window_end,
        "paths": {name: str(path) for name, path in required.items()},
    }


def discover_evidence_artifact_bundles(
    artifact_dir: Path | str | None,
) -> list[dict[str, Any]]:
    if artifact_dir is None or not str(artifact_dir).strip():
        return []
    root = Path(artifact_dir) / EVIDENCE_ROOT_DIRNAME / "full-backtest"
    if not root.exists():
        return []
    records: list[dict[str, Any]] = []
    for manifest_path in sorted(root.glob("*/manifest.json")):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            plan_payload = payload.get("evidence_plan") if isinstance(payload, dict) else None
            plan = validate_replay_evidence_plan(plan_payload)
        except (OSError, ValueError, json.JSONDecodeError):
            records.append(
                {
                    "evidence_plan_id": None,
                    "validation_status": "invalid",
                    "validation_reason_codes": ["invalid_manifest"],
                    "manifest_path": str(manifest_path),
                }
            )
            continue
        validation = validate_evidence_artifact_bundle(artifact_dir, plan)
        records.append(
            {
                "evidence_plan_id": plan.plan_id,
                "campaign_plan_id": plan.campaign_plan_id,
                "evidence_role": plan.evidence_role,
                "selection_data_end": plan.selection_data_end,
                "analysis_window_start": plan.analysis_window_start,
                "analysis_window_end": plan.analysis_window_end,
                "requested_horizon_months": plan.requested_horizon_months,
                "profile_snapshot_sha256": plan.profile_snapshot_sha256,
                "execution_cell_sha256": plan.execution_cell_sha256,
                "lake_manifest_sha256": plan.lake_manifest_sha256,
                "coverage_policy": plan.coverage_policy,
                "validation_status": validation["status"],
                "validation_reason_codes": validation["reason_codes"],
                "result_path": validation["paths"]["result"],
                "curve_path": validation["paths"]["curve"],
                "calendar_curve_path": validation["paths"]["calendar_curve"],
                "recommended_curve_path": validation["paths"]["recommended_curve"],
                "manifest_path": validation["paths"]["manifest"],
                "job_path": validation["paths"]["job"],
                "cell_receipt_path": validation["paths"].get("cell_receipt"),
            }
        )
    return records
