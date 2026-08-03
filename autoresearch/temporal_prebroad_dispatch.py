"""Finite Gateway dispatcher for the separately materialized pre-broad matrix.

``temporal_prebroad_control`` deliberately cannot contact a Gateway.  This
module is its only dispatch-side companion: it accepts only the exact frozen
authority, required authority ID, and no-dispatch manifest produced there.
It does not create candidates, alter profiles, or widen the sixteen tasks.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping

from .play_hand_lab import LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token
from .play_hand_lab_gateway import LabTask
from .result_codec import ResultCodecError, write_gzip_json_once
from .temporal_prebroad_control import MANIFEST_SCHEMA, WINDOWS, validate_prebroad_authority
from .temporal_search import (
    TEMPORAL_SEARCH_JOB_SCHEMA,
    TEMPORAL_SEARCH_TASK_KIND,
    LabGatewayClientProtocol,
    TemporalSearchContractError,
    TemporalSearchTimeout,
    _REQUIRED_WORKER_CAPABILITIES,
    _read_checkpoint_result,
    _result_codec_fields,
    _result_material,
    _safe,
    _write_checkpoint,
    _write_json,
    canonical_sha256,
)


DISPATCH_CHECKPOINT_SCHEMA = "temporal_prebroad_dispatch_checkpoint_v1"
DISPATCH_RESULT_SCHEMA = "temporal_prebroad_dispatch_result_v1"
_BIDIRECTIONAL_EVALUATOR_ID = "bar_bidirectional_single_position_execution_v2"
_WORKER_CONTRACT_SCHEMA = "replay-worker-contract-v1"


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchContractError(f"JSON root must be an object: {path}")
    return value


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if len(token) != 71 or not token.startswith("sha256:") or any(
        char not in "0123456789abcdef" for char in token[7:]
    ):
        raise TemporalSearchContractError(f"{name} must be an exact lower-case sha256 identity")
    return token


def _required_authority_id(path: Path) -> str:
    try:
        return _sha(path.read_text(encoding="utf-8").strip(), name="required authority ID")
    except OSError as exc:
        raise TemporalSearchContractError(f"could not read required authority ID: {path}") from exc


def _expected_manifest(authority: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct the control module's deterministic, no-dispatch rows."""
    tasks: list[dict[str, Any]] = []
    for pair in authority["pairs"]:
        inputs = {entry["windowId"]: entry["evidencePlan"] for entry in pair["windowInputs"]}
        for window_id, start, end in WINDOWS:
            identity = {
                "authorityId": authority["authorityId"],
                "candidateId": pair["candidateId"],
                "windowId": window_id,
            }
            tasks.append(
                {
                    "taskId": "temporal-prebroad-" + canonical_sha256(identity)[7:39],
                    **identity,
                    "instrument": "EURUSD",
                    "timeframe": pair["timeframe"],
                    "barLimit": pair["barLimit"],
                    "analysisWindowStart": start,
                    "analysisWindowEnd": end,
                    "costViews": authority["costViews"],
                    "evidencePlan": inputs[window_id],
                    "maxAttempts": 1,
                    "deadlineSeconds": 900.0,
                }
            )
    return {
        "schemaVersion": MANIFEST_SCHEMA,
        "authorityId": authority["authorityId"],
        "taskCount": 16,
        "tasks": tasks,
        "taskMatrixSha256": canonical_sha256(tasks),
        # These attest to the *control* boundary.  Dispatch permission is
        # deliberately held by this separate module, never rewritten here.
        "dispatchPermitted": False,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def load_prebroad_dispatch_inputs(
    *,
    authority_path: Path | str,
    authority_id_path: Path | str,
    manifest_path: Path | str,
    native_reports: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the complete closed input triple before any client operation."""
    authority = validate_prebroad_authority(
        _read_object(Path(authority_path)), native_reports=native_reports
    )
    required_id = _required_authority_id(Path(authority_id_path))
    if authority["authorityId"] != required_id:
        raise TemporalSearchContractError("authority does not match the required frozen authority ID")
    worker_contract = authority["workerContract"]
    if worker_contract["workerContractSchema"] != _WORKER_CONTRACT_SCHEMA:
        raise TemporalSearchContractError("pre-broad authority requires replay-worker-contract-v1")
    _sha(worker_contract["workerContractSha256"], name="worker contract hash")
    manifest = _read_object(Path(manifest_path))
    expected = _expected_manifest(authority)
    if set(manifest) != set(expected) or manifest != expected:
        raise TemporalSearchContractError("pre-broad manifest is not the exact closed deterministic matrix")
    return authority, manifest


def build_prebroad_dispatch_tasks(
    authority: Mapping[str, Any], manifest: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Turn only the frozen manifest rows into Gateway-compatible LabTasks."""
    by_candidate = {str(pair["candidateId"]): pair for pair in authority["pairs"]}
    tasks: list[dict[str, Any]] = []
    for row in manifest["tasks"]:
        candidate_id = str(row["candidateId"])
        pair = by_candidate.get(candidate_id)
        if pair is None:
            raise TemporalSearchContractError("manifest references a candidate outside frozen authority")
        profile = pair["profile"]
        evidence_plan = row["evidencePlan"]
        shared_stream_id = canonical_sha256(
            {
                "candidateSnapshotSha256": pair["profileSha256"],
                "evidencePlanId": evidence_plan["plan_id"],
                "windowId": row["windowId"],
                "windowSemanticSha256": evidence_plan["lake_window_binding"]["window_semantic_sha256"],
            }
        )
        payload: dict[str, Any] = {
            "schema_version": TEMPORAL_SEARCH_JOB_SCHEMA,
            "job_id": row["taskId"],
            "candidate_id": candidate_id,
            "authority_id": authority["authorityId"],
            "lake_window_semantic_sha256": evidence_plan["lake_window_binding"]["window_semantic_sha256"],
            "shared_observation_stream_id": shared_stream_id,
            "user_id": "temporal-prebroad-dispatcher",
            "profile_id": candidate_id,
            "inline_profile_snapshot": profile,
            "instruments": [row["instrument"]],
            "timeframe": row["timeframe"],
            "bar_limit": row["barLimit"],
            "evaluator_id": _BIDIRECTIONAL_EVALUATOR_ID,
            "analysis_window_start": row["analysisWindowStart"],
            "analysis_window_end": row["analysisWindowEnd"],
            "evidence_plan": evidence_plan,
            "required_worker_contract_hash": authority["workerContract"]["workerContractSha256"],
            "required_worker_contract_schema": authority["workerContract"]["workerContractSchema"],
            "required_capabilities": list(_REQUIRED_WORKER_CAPABILITIES),
            "client_origin": "temporal_prebroad_dispatcher",
            "campaign_id": authority["authorityId"],
            "lane_id": candidate_id,
            "attempt_id": row["taskId"],
        }
        execution_config = profile.get("executionConfig") if isinstance(profile, Mapping) else None
        if isinstance(execution_config, Mapping):
            # Match the existing candidate/window worker envelope; accepted v3
            # profiles with a management library bind the whole config, while
            # legacy-cell profiles bind their frozen cell.
            if execution_config.get("managementLibrary") is not None:
                payload["execution_config_sha256"] = canonical_sha256(execution_config)
            else:
                exit_policy = execution_config.get("exitPolicy")
                if isinstance(exit_policy, Mapping) and isinstance(exit_policy.get("selectedCell"), Mapping):
                    payload["execution_cell"] = dict(exit_policy["selectedCell"])
        lab_task = LabTask(
            task_id=str(row["taskId"]),
            lane_id=candidate_id,
            attempt_id=str(row["taskId"]),
            task_kind=TEMPORAL_SEARCH_TASK_KIND,
            payload=payload,
            required_worker_capabilities=set(_REQUIRED_WORKER_CAPABILITIES),
            deadline_seconds=float(row["deadlineSeconds"]),
            max_attempts=int(row["maxAttempts"]),
        )
        task = lab_task.to_payload()
        # LabTask intentionally omits scheduling state from its wire snapshot;
        # restore the fixed requirements that Gateway uses for worker matching.
        task["required_worker_capabilities"] = list(_REQUIRED_WORKER_CAPABILITIES)
        tasks.append(task)
    if len(tasks) != 16 or len({task["task_id"] for task in tasks}) != 16:
        raise TemporalSearchContractError("pre-broad dispatcher requires exactly sixteen unique LabTasks")
    return tasks


def _journal_sha(authority_id: str, matrix_sha: str, journal: list[dict[str, Any]]) -> str:
    return canonical_sha256(
        {"authorityId": authority_id, "taskMatrixSha256": matrix_sha, "journal": journal}
    )


def _new_checkpoint(authority_id: str, matrix_sha: str, mode: str) -> dict[str, Any]:
    checkpoint = {
        "schemaVersion": DISPATCH_CHECKPOINT_SCHEMA,
        "authorityId": authority_id,
        "taskMatrixSha256": matrix_sha,
        "mode": mode,
        "completed": {},
        "journal": [],
    }
    checkpoint["journalSha256"] = _journal_sha(authority_id, matrix_sha, [])
    return checkpoint


def _load_checkpoint(path: Path, *, authority_id: str, matrix_sha: str, resume: bool) -> dict[str, Any]:
    if not path.exists():
        return _new_checkpoint(authority_id, matrix_sha, "resume" if resume else "fresh")
    if not resume:
        raise TemporalSearchContractError("fresh pre-broad dispatch refuses an existing checkpoint")
    checkpoint = _read_object(path)
    if set(checkpoint) != {
        "schemaVersion", "authorityId", "taskMatrixSha256", "mode", "completed", "journal", "journalSha256"
    }:
        raise TemporalSearchContractError("pre-broad dispatch checkpoint has an open schema")
    if (
        checkpoint.get("schemaVersion") != DISPATCH_CHECKPOINT_SCHEMA
        or checkpoint.get("authorityId") != authority_id
        or checkpoint.get("taskMatrixSha256") != matrix_sha
        or checkpoint.get("mode") not in {"fresh", "resume"}
    ):
        raise TemporalSearchContractError("checkpoint does not bind the frozen pre-broad authority and matrix")
    completed = checkpoint.get("completed")
    journal = checkpoint.get("journal")
    if not isinstance(completed, dict) or not isinstance(journal, list):
        raise TemporalSearchContractError("pre-broad dispatch checkpoint state is malformed")
    if checkpoint.get("journalSha256") != _journal_sha(authority_id, matrix_sha, journal):
        raise TemporalSearchContractError("pre-broad dispatch checkpoint journal hash mismatch")
    journal_by_id = {
        str(entry.get("taskId") or ""): entry
        for entry in journal
        if isinstance(entry, Mapping)
    }
    if (
        len(journal_by_id) != len(journal)
        or set(journal_by_id) != set(completed)
        or any(
            not isinstance(record, Mapping)
            or journal_by_id[task_id] != {"taskId": task_id, **record}
            for task_id, record in completed.items()
        )
    ):
        raise TemporalSearchContractError("pre-broad dispatch checkpoint journal does not exactly bind completions")
    return checkpoint


def _enqueue_exact(client: LabGatewayClientProtocol, tasks: list[dict[str, Any]], *, resume: bool) -> None:
    receipt = client.enqueue_tasks(tasks)
    if not isinstance(receipt, Mapping) or receipt.get("status") not in (None, "accepted"):
        raise TemporalSearchContractError("gateway did not provide an accepted enqueue receipt")
    expected = len(tasks)
    try:
        submitted = int(receipt.get("submitted", expected))
        accepted = int(receipt.get("accepted", receipt.get("enqueued", 0)))
        enqueued = int(receipt.get("enqueued", accepted))
        rejected = int(receipt.get("rejected", expected - enqueued))
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError("gateway enqueue receipt is malformed") from exc
    if submitted != expected or accepted != enqueued or not 0 <= enqueued <= expected or rejected != expected - enqueued:
        raise TemporalSearchContractError("gateway enqueue receipt does not exactly account for the pending task set")
    if enqueued == expected and rejected == 0:
        return
    if not resume:
        raise TemporalSearchContractError("fresh pre-broad dispatch requires every task to be newly enqueued")
    # The gateway's current public receipt is cardinality-only.  It must still
    # explicitly account for every submitted task as a duplicate or enqueue;
    # the task IDs are content-bound and cannot be substituted locally.
    if enqueued + rejected != expected:
        raise TemporalSearchContractError("resume duplicate enqueue receipt is not exact")


def _validate_material_bindings(
    task: Mapping[str, Any], material: Mapping[str, Any], *, worker_contract_hash: str
) -> None:
    """Bind worker/environment and market routing fields omitted by the generic validator."""
    job = task["payload"]
    attribution = material.get("worker_attribution")
    if (
        not isinstance(attribution, Mapping)
        or attribution.get("worker_contract_hash") != worker_contract_hash
    ):
        raise TemporalSearchContractError("pre-broad result worker contract attribution mismatch")
    summary = material.get("observation_summary")
    if not isinstance(summary, Mapping):
        raise TemporalSearchContractError("pre-broad result observation summary is required")
    instrument = str(summary.get("instrument") or "").strip().upper()
    timeframe = str(summary.get("timeframe") or "").strip().upper()
    if instrument != str(job["instruments"][0]).upper():
        raise TemporalSearchContractError("pre-broad result instrument does not match task")
    if timeframe != str(job["timeframe"]).upper():
        raise TemporalSearchContractError("pre-broad result timeframe does not match task")
    if material.get("profile_id") != job["profile_id"]:
        raise TemporalSearchContractError("pre-broad result profile does not match task")
    if material.get("execution_config_sha256") != job.get("execution_config_sha256"):
        raise TemporalSearchContractError("pre-broad result execution configuration does not match task")


def _revalidate_persisted_material(
    task: Mapping[str, Any], material: Mapping[str, Any], *, worker_contract_hash: str
) -> dict[str, Any]:
    """Re-run the complete task/result contract before trusting resume state."""
    completion = {
        "status": "success",
        "task_id": task["task_id"],
        "lane_id": task["lane_id"],
        "attempt_id": task["attempt_id"],
        "result": {
            "status": "success",
            "job_kind": TEMPORAL_SEARCH_TASK_KIND,
            "result": dict(material),
        },
    }
    validated = _result_material(task, completion)
    _validate_material_bindings(
        task, validated, worker_contract_hash=worker_contract_hash
    )
    return validated


def run_prebroad_dispatch(
    *,
    authority_path: Path | str,
    authority_id_path: Path | str,
    manifest_path: Path | str,
    output_root: Path | str,
    gateway_url: str | None = None,
    gateway_token: str | None = None,
    client: LabGatewayClientProtocol | None = None,
    resume: bool = False,
    timeout_seconds: float = 900.0,
    poll_interval_seconds: float = 0.25,
    native_reports: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Dispatch and durably consume only the exact sixteen pre-broad jobs."""
    authority, manifest = load_prebroad_dispatch_inputs(
        authority_path=authority_path,
        authority_id_path=authority_id_path,
        manifest_path=manifest_path,
        native_reports=native_reports,
    )
    tasks = build_prebroad_dispatch_tasks(authority, manifest)
    root = Path(output_root)
    checkpoint_path = root / "checkpoint.json"
    checkpoint = _load_checkpoint(
        checkpoint_path,
        authority_id=authority["authorityId"],
        matrix_sha=manifest["taskMatrixSha256"],
        resume=resume,
    )
    # Persist the empty checkpoint before the first POST.  A process crash
    # after enqueue but before a completion can therefore only be resumed,
    # never accidentally restarted as a fresh economic run.
    if not checkpoint_path.exists():
        _write_checkpoint(checkpoint_path, checkpoint)
    task_by_id = {task["task_id"]: task for task in tasks}
    worker_contract_hash = authority["workerContract"]["workerContractSha256"]
    completed = checkpoint["completed"]
    if set(completed) - set(task_by_id):
        raise TemporalSearchContractError("checkpoint contains a completion outside the frozen matrix")
    for task_id, record in completed.items():
        if not isinstance(record, Mapping):
            raise TemporalSearchContractError("checkpoint completion must be an object")
        expected_path = (root / "results" / f"{task_id}.json.gz").resolve()
        if Path(str(record.get("resultPath") or "")).resolve() != expected_path:
            raise TemporalSearchContractError("checkpoint result path escapes the frozen pre-broad result root")
        if record.get("candidateId") != task_by_id[task_id]["payload"]["candidate_id"]:
            raise TemporalSearchContractError("checkpoint candidate binding does not match frozen task")
        material = _read_checkpoint_result(record)
        revalidated = _revalidate_persisted_material(
            task_by_id[task_id],
            material,
            worker_contract_hash=worker_contract_hash,
        )
        if canonical_sha256(revalidated) != record.get("resultSha256"):
            raise TemporalSearchContractError("checkpoint result no longer binds its frozen task")
    owns_client = client is None
    gateway: LabGatewayClientProtocol
    if client is None:
        if not str(gateway_url or "").strip():
            raise TemporalSearchContractError("gateway_url is required when no client is supplied")
        gateway = LabGatewayClient(base_url=str(gateway_url), token=gateway_token)
    else:
        gateway = client

    def consume(completion: Mapping[str, Any]) -> None:
        task_id = str(completion.get("task_id") or "")
        task = task_by_id.get(task_id)
        if task is None:
            raise TemporalSearchContractError("unrelated pre-broad Gateway completion")
        if str(completion.get("status") or "").lower() != "success":
            lease = _safe(completion.get("lease_id"), name="completion.lease_id")
            failure = dict(completion)
            _write_json(root / "failures" / f"{task_id}.json", failure)
            if gateway.ack_results([lease]) != 1:
                raise TemporalSearchContractError(
                    "gateway did not acknowledge persisted failed pre-broad result"
                )
            detail = failure.get("error") or failure.get("result") or failure.get("status")
            encoded = json.dumps(
                detail,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            raise TemporalSearchContractError(
                f"failed pre-broad Gateway completion for {task_id}: {encoded[:2000]}"
            )
        lease = _safe(completion.get("lease_id"), name="completion.lease_id")
        # This invokes the existing exact routing, two-cost-view, artifact
        # identity, and Stage 5E7-v3 semantic evidence validation.
        material = _result_material(task, completion)
        _validate_material_bindings(
            task, material, worker_contract_hash=worker_contract_hash
        )
        digest = canonical_sha256(material)
        prior = completed.get(task_id)
        if prior is not None:
            if not isinstance(prior, Mapping) or prior.get("resultSha256") != digest:
                raise TemporalSearchContractError("conflicting duplicate pre-broad result")
            if canonical_sha256(_read_checkpoint_result(prior)) != digest:
                raise TemporalSearchContractError("conflicting duplicate pre-broad result")
        else:
            result_path = (root / "results" / f"{task_id}.json.gz").resolve()
            try:
                metadata = write_gzip_json_once(result_path, material)
            except ResultCodecError as exc:
                raise TemporalSearchContractError(
                    f"could not materialize immutable pre-broad result: {result_path}"
                ) from exc
            record = {
                "resultSha256": digest,
                "resultPath": str(result_path),
                "candidateId": task["payload"]["candidate_id"],
                **_result_codec_fields(metadata),
            }
            completed[task_id] = record
            entry = {"taskId": task_id, **record}
            checkpoint["completed"] = completed
            checkpoint["journal"] = [*checkpoint["journal"], entry]
            checkpoint["journalSha256"] = _journal_sha(
                authority["authorityId"], manifest["taskMatrixSha256"], checkpoint["journal"]
            )
            _write_checkpoint(checkpoint_path, checkpoint)
        # Never acknowledge a lease until its immutable gzip blob and its
        # hash-bound checkpoint/journal record have both been persisted.
        if gateway.ack_results([lease]) != 1:
            raise TemporalSearchContractError("gateway did not acknowledge persisted pre-broad result")

    try:
        # A redelivery is consumed before enqueuing to avoid a second economic
        # evaluation after a crash between persistence and acknowledgement.
        for completion in gateway.read_results(limit=max(32, len(tasks) * 2)):
            if not isinstance(completion, Mapping):
                raise TemporalSearchContractError("Gateway completion must be an object")
            consume(completion)
        pending = [task for task in tasks if task["task_id"] not in completed]
        if pending:
            _enqueue_exact(gateway, pending, resume=resume)
        deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
        while pending:
            if time.monotonic() >= deadline:
                raise TemporalSearchTimeout("timed out waiting for pre-broad Gateway results")
            results = gateway.read_results(limit=max(32, len(pending) * 2))
            if not results:
                time.sleep(max(float(poll_interval_seconds), 0.01))
                continue
            for completion in results:
                if not isinstance(completion, Mapping):
                    raise TemporalSearchContractError("Gateway completion must be an object")
                consume(completion)
                pending = [task for task in pending if task["task_id"] != completion.get("task_id")]
    finally:
        if owns_client and hasattr(gateway, "close"):
            gateway.close()  # type: ignore[attr-defined]

    result = {
        "schemaVersion": DISPATCH_RESULT_SCHEMA,
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "taskCount": len(tasks),
        "completedTaskCount": len(completed),
        "resume": resume,
    }
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run or resume the exact finite pre-broad bidirectional task matrix."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("fresh", "resume"):
        command = commands.add_parser(name)
        command.add_argument("--authority-path", type=Path, required=True)
        command.add_argument("--required-authority-id-path", type=Path, required=True)
        command.add_argument("--manifest-path", type=Path, required=True)
        command.add_argument("--output-root", type=Path, required=True)
        command.add_argument("--gateway-url", required=True)
        command.add_argument("--gateway-token")
        command.add_argument("--timeout-seconds", type=float, default=900.0)
        command.add_argument("--poll-interval-seconds", type=float, default=0.25)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    client: LabGatewayClient | None = None
    try:
        client = LabGatewayClient(
            base_url=args.gateway_url,
            token=args.gateway_token or load_lab_gateway_token(create=False),
            timeout_seconds=min(max(float(args.timeout_seconds), 5.0), 120.0),
        )
        health = client.health()
        if health.get("ok") is not True:
            raise TemporalSearchContractError("Lab Gateway health check did not return ok=true")
        result = run_prebroad_dispatch(
            authority_path=args.authority_path,
            authority_id_path=args.required_authority_id_path,
            manifest_path=args.manifest_path,
            output_root=args.output_root,
            client=client,
            resume=args.command == "resume",
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        print(json.dumps({"gateway": health, **result}, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_prebroad_dispatch_error_v1",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    finally:
        if client is not None:
            client.close()


__all__ = [
    "DISPATCH_CHECKPOINT_SCHEMA",
    "DISPATCH_RESULT_SCHEMA",
    "build_prebroad_dispatch_tasks",
    "load_prebroad_dispatch_inputs",
    "main",
    "run_prebroad_dispatch",
]


if __name__ == "__main__":
    raise SystemExit(main())
