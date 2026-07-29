from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import time
from typing import Any, Mapping, Protocol
from uuid import uuid4


TEMPORAL_GRAPH_REPLAY_TASK_KIND = "temporal_graph_replay"
TEMPORAL_GRAPH_REPLAY_CAPABILITY = "temporal_graph_replay_v1"
TEMPORAL_GRAPH_REPLAY_JOB_SCHEMA = "temporal_graph_replay_job_v1"
TEMPORAL_GRAPH_LAB_RESULT_SCHEMA = "temporal_graph_lab_result_v1"
TEMPORAL_GRAPH_LAB_ARTIFACT_MANIFEST_SCHEMA = (
    "temporal_graph_lab_artifact_manifest_v1"
)
REPLAY_EVIDENCE_PLAN_SCHEMA_V2 = "fuzzfolio.replay-evidence-plan.v2"
REPLAY_WORKER_CONTRACT_SCHEMA = "replay-worker-contract-v1"
BAR_SINGLE_POSITION_EVALUATOR_ID = "bar_single_position_execution_v1"
TEMPORAL_BAR_FILL_POLICY = "temporal_bar_fill_v1"
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,239}$")


class TemporalGraphLabError(RuntimeError):
    """Base class for the isolated distributed temporal replay lane."""


class TemporalGraphLabContractError(TemporalGraphLabError):
    pass


class TemporalGraphLabMaterializationError(TemporalGraphLabError):
    pass


class TemporalGraphLabTimeout(TemporalGraphLabError):
    pass


class LabGatewayClientProtocol(Protocol):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]: ...

    def read_results(self, *, limit: int) -> list[dict[str, Any]]: ...

    def ack_results(self, lease_ids: list[str]) -> int: ...


@dataclass(frozen=True, slots=True)
class ValidatedTemporalGraphLabResult:
    task_id: str
    lease_id: str
    completion: dict[str, Any]
    worker_envelope: dict[str, Any]
    material_result: dict[str, Any]


def _json_clone(value: Any) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise TemporalGraphLabContractError(
            "temporal Lab payload is not finite canonical JSON"
        ) from exc


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TemporalGraphLabContractError(
            "temporal Lab payload is not finite canonical JSON"
        ) from exc


def _pretty_json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TemporalGraphLabContractError(
            "temporal Lab payload is not finite JSON"
        ) from exc


def canonical_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def bytes_sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _require_sha256(value: Any, *, field_name: str) -> str:
    token = str(value or "").strip()
    if not _SHA256_RE.fullmatch(token):
        raise TemporalGraphLabContractError(
            f"{field_name} must be an exact sha256 identity"
        )
    return token


def _safe_id(value: Any, *, field_name: str) -> str:
    token = str(value or "").strip()
    if not _SAFE_ID_RE.fullmatch(token):
        raise TemporalGraphLabContractError(
            f"{field_name} must be a safe explicit identifier"
        )
    return token


def _timestamp(value: Any, *, field_name: str) -> str:
    token = str(value or "").strip()
    if not token:
        raise TemporalGraphLabContractError(f"{field_name} is required")
    try:
        parsed = datetime.fromisoformat(
            token[:-1] + "+00:00" if token.endswith("Z") else token
        )
    except ValueError as exc:
        raise TemporalGraphLabContractError(
            f"{field_name} is not an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise TemporalGraphLabContractError(
            f"{field_name} must include a timezone"
        )
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalGraphLabContractError(f"{field_name} must be an object")
    return _json_clone(dict(value))


def _field(payload: Mapping[str, Any], snake: str, camel: str | None = None) -> Any:
    if snake in payload:
        return payload[snake]
    if camel and camel in payload:
        return payload[camel]
    return None


def _normalize_profile(profile_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    profile = _mapping(profile_snapshot, field_name="source_profile")
    if profile.get("version") != "v2":
        raise TemporalGraphLabContractError(
            "distributed temporal replay requires profile version v2"
        )
    graph = _mapping(profile.get("graph"), field_name="source_profile.graph")
    if graph.get("kind") != "temporal_graph_v1":
        raise TemporalGraphLabContractError(
            "distributed temporal replay requires temporal_graph_v1"
        )
    instruments = profile.get("instruments")
    if not isinstance(instruments, list) or len(instruments) != 1:
        raise TemporalGraphLabContractError(
            "source temporal profile requires exactly one instrument"
        )
    normalized_instrument = str(instruments[0] or "").strip().upper()
    if not normalized_instrument:
        raise TemporalGraphLabContractError("source profile instrument is empty")
    profile["instruments"] = [normalized_instrument]
    if str(profile.get("directionMode") or "").strip().lower() not in {
        "long",
        "short",
    }:
        raise TemporalGraphLabContractError(
            "source temporal profile requires one direction"
        )
    if profile.get("isActive") is not False:
        raise TemporalGraphLabContractError(
            "distributed temporal research profile must be inactive"
        )
    return profile


def _normalize_execution_cell(value: Mapping[str, Any]) -> dict[str, float]:
    cell = _mapping(value, field_name="execution_cell")
    required = {"stopLossPercent", "rewardMultiple", "takeProfitPercent"}
    if set(cell) != required:
        raise TemporalGraphLabContractError(
            "execution_cell must contain exactly stopLossPercent, rewardMultiple, and takeProfitPercent"
        )
    try:
        stop = float(cell["stopLossPercent"])
        reward = float(cell["rewardMultiple"])
        target = float(cell["takeProfitPercent"])
    except (TypeError, ValueError) as exc:
        raise TemporalGraphLabContractError(
            "execution_cell values must be numeric"
        ) from exc
    if not all(math.isfinite(item) and item > 0.0 for item in (stop, reward, target)):
        raise TemporalGraphLabContractError(
            "execution_cell values must be finite and positive"
        )
    if abs(stop * reward - target) > 1e-9:
        raise TemporalGraphLabContractError(
            "takeProfitPercent must equal stopLossPercent * rewardMultiple"
        )
    return {
        "stopLossPercent": stop,
        "rewardMultiple": reward,
        "takeProfitPercent": target,
    }


def _validate_evidence_plan(
    evidence_plan: Mapping[str, Any],
    *,
    profile: dict[str, Any],
    execution_cell: dict[str, float],
    instrument: str,
    timeframe: str,
    analysis_window_start: str,
    analysis_window_end: str,
) -> dict[str, Any]:
    plan = _mapping(evidence_plan, field_name="evidence_plan")
    if plan.get("schema_version") != REPLAY_EVIDENCE_PLAN_SCHEMA_V2:
        raise TemporalGraphLabContractError(
            "temporal Lab task requires replay evidence plan v2"
        )
    if plan.get("lake_manifest_sha256") is not None:
        raise TemporalGraphLabContractError(
            "v2 evidence plan must use lake_window_binding as sole lake authority"
        )
    supplied_plan_id = _require_sha256(plan.get("plan_id"), field_name="plan_id")
    identity = dict(plan)
    identity.pop("plan_id", None)
    identity.pop("lake_manifest_sha256", None)
    if canonical_sha256(identity) != supplied_plan_id:
        raise TemporalGraphLabContractError("evidence plan identity mismatch")
    if plan.get("profile_snapshot_sha256") != canonical_sha256(profile):
        raise TemporalGraphLabContractError(
            "evidence plan profile snapshot identity mismatch"
        )
    if plan.get("execution_cell_sha256") != canonical_sha256(execution_cell):
        raise TemporalGraphLabContractError(
            "evidence plan execution cell identity mismatch"
        )
    if _timestamp(
        plan.get("analysis_window_start"),
        field_name="evidence_plan.analysis_window_start",
    ) != analysis_window_start or _timestamp(
        plan.get("analysis_window_end"),
        field_name="evidence_plan.analysis_window_end",
    ) != analysis_window_end:
        raise TemporalGraphLabContractError(
            "evidence plan analysis window does not match the task"
        )
    binding = _mapping(
        plan.get("lake_window_binding"),
        field_name="evidence_plan.lake_window_binding",
    )
    _require_sha256(
        binding.get("window_semantic_sha256"),
        field_name="lake window semantic identity",
    )
    request = _mapping(binding.get("request"), field_name="lake window request")
    pairs = {str(item).strip().upper() for item in request.get("pairs") or []}
    timeframes = {
        str(item).strip().upper() for item in request.get("timeframes") or []
    }
    if instrument not in pairs or timeframe not in timeframes:
        raise TemporalGraphLabContractError(
            "lake window binding does not cover the requested instrument/timeframe"
        )
    data_start = _timestamp(request.get("data_start"), field_name="lake data_start")
    data_end = _timestamp(request.get("data_end"), field_name="lake data_end")
    if (
        _parse_timestamp(data_start) > _parse_timestamp(analysis_window_start)
        or _parse_timestamp(data_end) < _parse_timestamp(analysis_window_end)
    ):
        raise TemporalGraphLabContractError(
            "lake window binding does not cover the analysis window"
        )
    return plan


def build_temporal_graph_lab_task(
    *,
    source_profile: Mapping[str, Any],
    temporal_source_profile_sha256: str,
    evidence_plan: Mapping[str, Any],
    execution_cell: Mapping[str, Any],
    worker_contract_hash: str,
    instrument: str,
    timeframe: str,
    analysis_window_start: Any,
    analysis_window_end: Any,
    profile_id: str,
    task_id: str | None = None,
    lane_id: str = "temporal_graph_replay",
    attempt_id: str | None = None,
    campaign_id: str | None = None,
    user_id: str = "temporal-research",
    bar_limit: int = 5000,
    cost_model: Mapping[str, Any] | None = None,
    expected_result_sha256: str | None = None,
    deadline_seconds: float = 900.0,
    max_attempts: int = 4,
) -> dict[str, Any]:
    profile = _normalize_profile(source_profile)
    temporal_source_hash = _require_sha256(
        temporal_source_profile_sha256,
        field_name="temporal_source_profile_sha256",
    )
    contract_hash = _require_sha256(
        worker_contract_hash,
        field_name="worker_contract_hash",
    )
    normalized_instrument = str(instrument or "").strip().upper()
    normalized_timeframe = str(timeframe or "").strip().upper()
    if normalized_instrument != profile["instruments"][0]:
        raise TemporalGraphLabContractError(
            "task instrument does not match the source profile"
        )
    if not normalized_timeframe:
        raise TemporalGraphLabContractError("timeframe is required")
    start = _timestamp(analysis_window_start, field_name="analysis_window_start")
    end = _timestamp(analysis_window_end, field_name="analysis_window_end")
    if _parse_timestamp(start) >= _parse_timestamp(end):
        raise TemporalGraphLabContractError(
            "analysis_window_start must be earlier than analysis_window_end"
        )
    cell = _normalize_execution_cell(execution_cell)
    selected_cell = (
        ((profile.get("executionConfig") or {}).get("exitPolicy") or {}).get(
            "selectedCell"
        )
    )
    if _normalize_execution_cell(selected_cell) != cell:
        raise TemporalGraphLabContractError(
            "task execution cell does not match the temporal profile"
        )
    plan = _validate_evidence_plan(
        evidence_plan,
        profile=profile,
        execution_cell=cell,
        instrument=normalized_instrument,
        timeframe=normalized_timeframe,
        analysis_window_start=start,
        analysis_window_end=end,
    )
    if isinstance(bar_limit, bool) or not 10 <= int(bar_limit) <= 1_000_000:
        raise TemporalGraphLabContractError("bar_limit is outside the admitted range")
    normalized_cost = _mapping(
        cost_model or {"mode": "research_conservative"},
        field_name="cost_model",
    )
    expected = (
        _require_sha256(expected_result_sha256, field_name="expected_result_sha256")
        if expected_result_sha256 is not None
        else None
    )
    resolved_task_id = _safe_id(
        task_id or f"temporal-{uuid4().hex[:16]}",
        field_name="task_id",
    )
    resolved_lane_id = _safe_id(lane_id, field_name="lane_id")
    resolved_attempt_id = _safe_id(
        attempt_id or resolved_task_id,
        field_name="attempt_id",
    )
    resolved_profile_id = _safe_id(profile_id, field_name="profile_id")
    resolved_user_id = _safe_id(user_id, field_name="user_id")
    resolved_campaign_id = (
        _safe_id(campaign_id, field_name="campaign_id")
        if campaign_id is not None
        else None
    )
    requested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    job = {
        "schema_version": TEMPORAL_GRAPH_REPLAY_JOB_SCHEMA,
        "job_id": resolved_task_id,
        "user_id": resolved_user_id,
        "profile_id": resolved_profile_id,
        "inline_profile_snapshot": profile,
        "temporal_source_profile_sha256": temporal_source_hash,
        "instruments": [normalized_instrument],
        "timeframe": normalized_timeframe,
        "market_data_source": "lake_bars",
        "analysis_window_start": start,
        "analysis_window_end": end,
        "lookback_months": None,
        "bar_limit": int(bar_limit),
        "cost_model": normalized_cost,
        "evaluator_id": BAR_SINGLE_POSITION_EVALUATOR_ID,
        "fill_policy": TEMPORAL_BAR_FILL_POLICY,
        "end_policy": "leave_open",
        "execution_cell": cell,
        "evidence_plan": plan,
        "expected_result_sha256": expected,
        "requested_at": requested_at,
        "priority": "research",
        "work_class": "research_replay",
        "required_worker_contract_hash": contract_hash,
        "required_worker_contract_schema": REPLAY_WORKER_CONTRACT_SCHEMA,
        "required_capabilities": [TEMPORAL_GRAPH_REPLAY_CAPABILITY],
        "client_origin": "temporal_graph_lab",
        "campaign_id": resolved_campaign_id,
        "lane_id": resolved_lane_id,
        "attempt_id": resolved_attempt_id,
    }
    return {
        "task_id": resolved_task_id,
        "lane_id": resolved_lane_id,
        "attempt_id": resolved_attempt_id,
        "task_kind": TEMPORAL_GRAPH_REPLAY_TASK_KIND,
        "payload": job,
        "required_worker_capabilities": [TEMPORAL_GRAPH_REPLAY_CAPABILITY],
        "deadline_seconds": max(float(deadline_seconds), 1.0),
        "max_attempts": max(int(max_attempts), 1),
    }


def _material_result_from_completion(
    completion: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    worker_envelope = _mapping(completion.get("result"), field_name="worker envelope")
    if str(worker_envelope.get("status") or "").strip().lower() != "success":
        raise TemporalGraphLabContractError(
            f"temporal worker did not return success: {worker_envelope.get('status')!r}"
        )
    if worker_envelope.get("job_kind") != TEMPORAL_GRAPH_REPLAY_TASK_KIND:
        raise TemporalGraphLabContractError("worker result has the wrong job kind")
    material = _mapping(
        worker_envelope.get("result"),
        field_name="temporal material result",
    )
    return worker_envelope, material


def validate_temporal_graph_lab_result(
    task: Mapping[str, Any],
    completion: Mapping[str, Any],
) -> ValidatedTemporalGraphLabResult:
    task_payload = _mapping(task, field_name="task")
    task_id = _safe_id(task_payload.get("task_id"), field_name="task_id")
    lane_id = _safe_id(task_payload.get("lane_id"), field_name="lane_id")
    attempt_id = _safe_id(task_payload.get("attempt_id"), field_name="attempt_id")
    if completion.get("task_id") != task_id:
        raise TemporalGraphLabContractError("completion task identity mismatch")
    if completion.get("lane_id") != lane_id:
        raise TemporalGraphLabContractError("completion lane identity mismatch")
    if completion.get("attempt_id") != attempt_id:
        raise TemporalGraphLabContractError("completion attempt identity mismatch")
    lease_id = _safe_id(completion.get("lease_id"), field_name="lease_id")
    worker_envelope, material = _material_result_from_completion(completion)
    job = _mapping(task_payload.get("payload"), field_name="task payload")

    if material.get("schema_version") != TEMPORAL_GRAPH_LAB_RESULT_SCHEMA:
        raise TemporalGraphLabContractError("unknown temporal Lab result schema")
    if material.get("task_kind") != TEMPORAL_GRAPH_REPLAY_TASK_KIND:
        raise TemporalGraphLabContractError("material result task kind mismatch")
    if material.get("job_id") != job.get("job_id"):
        raise TemporalGraphLabContractError("material result job identity mismatch")
    if material.get("profile_id") != job.get("profile_id"):
        raise TemporalGraphLabContractError("material result profile identity mismatch")
    if material.get("source_profile_snapshot_sha256") != job.get(
        "temporal_source_profile_sha256"
    ):
        raise TemporalGraphLabContractError(
            "material result temporal source profile identity mismatch"
        )
    evidence_plan = _mapping(job.get("evidence_plan"), field_name="job evidence plan")
    if material.get("evidence_plan_id") != evidence_plan.get("plan_id"):
        raise TemporalGraphLabContractError("material result evidence plan mismatch")
    binding = _mapping(
        evidence_plan.get("lake_window_binding"),
        field_name="job lake window binding",
    )
    if material.get("observed_window_semantic_sha256") != binding.get(
        "window_semantic_sha256"
    ):
        raise TemporalGraphLabContractError(
            "material result lake window identity mismatch"
        )
    worker_attribution = _mapping(
        material.get("worker_attribution"),
        field_name="worker attribution",
    )
    if worker_attribution.get("worker_contract_hash") != job.get(
        "required_worker_contract_hash"
    ):
        raise TemporalGraphLabContractError("worker contract identity mismatch")

    expected_result = job.get("expected_result_sha256")
    replay_result_sha = _require_sha256(
        material.get("replay_result_sha256"),
        field_name="replay_result_sha256",
    )
    if expected_result is not None and replay_result_sha != expected_result:
        raise TemporalGraphLabContractError("distributed/local replay parity mismatch")
    expected_parity = "matched" if expected_result is not None else "not_requested"
    if material.get("parity_status") != expected_parity:
        raise TemporalGraphLabContractError("material result parity status mismatch")

    replay = _mapping(material.get("replay_result"), field_name="replay result")
    cross_checks = (
        ("resultSha256", "replay_result_sha256"),
        ("profileSnapshotSha256", "resolved_profile_snapshot_sha256"),
        ("programSha256", "program_sha256"),
        ("streamSha256", "stream_sha256"),
        ("finalCheckpointSha256", "final_checkpoint_sha256"),
        ("costModelSha256", "cost_model_sha256"),
        ("evaluatorId", "evaluator_id"),
        ("fillPolicy", "fill_policy"),
        ("endPolicy", "end_policy"),
    )
    for replay_field, result_field in cross_checks:
        if replay.get(replay_field) != material.get(result_field):
            raise TemporalGraphLabContractError(
                f"replay/material cross-field mismatch: {replay_field}"
            )
    summary = _mapping(
        material.get("observation_summary"),
        field_name="observation summary",
    )
    if str(summary.get("instrument") or "").strip().upper() != str(
        (job.get("instruments") or [""])[0]
    ).strip().upper():
        raise TemporalGraphLabContractError("observation instrument mismatch")
    if str(summary.get("timeframe") or "").strip().upper() != str(
        job.get("timeframe") or ""
    ).strip().upper():
        raise TemporalGraphLabContractError("observation timeframe mismatch")
    if int(summary.get("observation_count") or 0) <= 0:
        raise TemporalGraphLabContractError("worker returned no observations")

    return ValidatedTemporalGraphLabResult(
        task_id=task_id,
        lease_id=lease_id,
        completion=_json_clone(dict(completion)),
        worker_envelope=worker_envelope,
        material_result=material,
    )


def _write_exact_or_reuse(path: Path, payload: bytes) -> None:
    if path.exists():
        existing = path.read_bytes()
        if existing != payload:
            raise TemporalGraphLabMaterializationError(
                f"immutable temporal artifact conflict: {path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def materialize_temporal_graph_lab_result(
    output_root: Path | str,
    task: Mapping[str, Any],
    validated: ValidatedTemporalGraphLabResult,
) -> dict[str, Any]:
    task_payload = _mapping(task, field_name="task")
    if task_payload.get("task_id") != validated.task_id:
        raise TemporalGraphLabMaterializationError(
            "validated result does not match materialization task"
        )
    task_bytes = _pretty_json_bytes(task_payload)
    result_bytes = _pretty_json_bytes(validated.material_result)
    job = _mapping(task_payload.get("payload"), field_name="task payload")
    evidence_plan = _mapping(job.get("evidence_plan"), field_name="evidence plan")
    worker = _mapping(
        validated.material_result.get("worker_attribution"),
        field_name="worker attribution",
    )
    files = {
        "request.json": bytes_sha256(task_bytes),
        "result.json": bytes_sha256(result_bytes),
    }
    manifest_identity = {
        "schema_version": TEMPORAL_GRAPH_LAB_ARTIFACT_MANIFEST_SCHEMA,
        "task_id": validated.task_id,
        "lane_id": task_payload.get("lane_id"),
        "attempt_id": task_payload.get("attempt_id"),
        "job_id": job.get("job_id"),
        "profile_id": job.get("profile_id"),
        "temporal_source_profile_sha256": job.get(
            "temporal_source_profile_sha256"
        ),
        "resolved_profile_snapshot_sha256": validated.material_result.get(
            "resolved_profile_snapshot_sha256"
        ),
        "program_sha256": validated.material_result.get("program_sha256"),
        "stream_sha256": validated.material_result.get("stream_sha256"),
        "replay_result_sha256": validated.material_result.get(
            "replay_result_sha256"
        ),
        "final_checkpoint_sha256": validated.material_result.get(
            "final_checkpoint_sha256"
        ),
        "evidence_plan_id": evidence_plan.get("plan_id"),
        "observed_window_semantic_sha256": validated.material_result.get(
            "observed_window_semantic_sha256"
        ),
        "worker_contract_hash": worker.get("worker_contract_hash"),
        "worker_capability": TEMPORAL_GRAPH_REPLAY_CAPABILITY,
        "expected_result_sha256": job.get("expected_result_sha256"),
        "parity_status": validated.material_result.get("parity_status"),
        "created_at": job.get("requested_at"),
        "completed_at": validated.worker_envelope.get("completed_at"),
        "files": files,
    }
    manifest = {
        **manifest_identity,
        "manifest_sha256": canonical_sha256(manifest_identity),
    }
    manifest_bytes = _pretty_json_bytes(manifest)
    bundle = (
        Path(output_root).expanduser().resolve()
        / "temporal-graph-replay"
        / validated.task_id
    )
    _write_exact_or_reuse(bundle / "request.json", task_bytes)
    _write_exact_or_reuse(bundle / "result.json", result_bytes)
    _write_exact_or_reuse(bundle / "manifest.json", manifest_bytes)
    return {"bundle_path": str(bundle), "manifest": manifest}


def run_temporal_graph_lab_tasks(
    client: LabGatewayClientProtocol,
    tasks: list[dict[str, Any]],
    *,
    output_root: Path | str,
    timeout_seconds: float = 900.0,
    poll_interval_seconds: float = 0.25,
) -> list[dict[str, Any]]:
    if not tasks:
        return []
    tasks_by_id: dict[str, dict[str, Any]] = {}
    for task in tasks:
        normalized = _mapping(task, field_name="task")
        task_id = _safe_id(normalized.get("task_id"), field_name="task_id")
        if task_id in tasks_by_id:
            raise TemporalGraphLabContractError(f"duplicate task ID: {task_id}")
        if normalized.get("task_kind") != TEMPORAL_GRAPH_REPLAY_TASK_KIND:
            raise TemporalGraphLabContractError("unexpected task kind")
        tasks_by_id[task_id] = normalized

    preexisting = client.read_results(limit=max(len(tasks_by_id) * 2, 32))
    if preexisting:
        raise TemporalGraphLabContractError(
            "Lab result backlog must be empty before isolated temporal enqueue"
        )
    enqueue_receipt = client.enqueue_tasks(list(tasks_by_id.values()))
    enqueued = int(enqueue_receipt.get("enqueued") or 0)
    if enqueued != len(tasks_by_id):
        raise TemporalGraphLabContractError(
            f"gateway enqueued {enqueued} of {len(tasks_by_id)} temporal tasks"
        )

    pending = set(tasks_by_id)
    completed: dict[str, dict[str, Any]] = {}
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while pending:
        if time.monotonic() >= deadline:
            raise TemporalGraphLabTimeout(
                "timed out waiting for temporal graph Lab results"
            )
        results = client.read_results(limit=max(len(pending) * 2, 8))
        if not results:
            time.sleep(max(float(poll_interval_seconds), 0.01))
            continue
        for completion in results:
            task_id = str(completion.get("task_id") or "").strip()
            if task_id not in tasks_by_id:
                raise TemporalGraphLabContractError(
                    f"unrelated Lab result encountered: {task_id or '<missing>'}"
                )
            validated = validate_temporal_graph_lab_result(
                tasks_by_id[task_id],
                completion,
            )
            artifact = materialize_temporal_graph_lab_result(
                output_root,
                tasks_by_id[task_id],
                validated,
            )
            acked = client.ack_results([validated.lease_id])
            if acked != 1:
                raise TemporalGraphLabContractError(
                    f"gateway did not acknowledge result lease {validated.lease_id}"
                )
            existing = completed.get(task_id)
            if existing is not None and existing["manifest"] != artifact["manifest"]:
                raise TemporalGraphLabContractError(
                    f"conflicting duplicate temporal result: {task_id}"
                )
            completed[task_id] = artifact
            pending.discard(task_id)
    return [completed[task_id] for task_id in tasks_by_id]


__all__ = [
    "BAR_SINGLE_POSITION_EVALUATOR_ID",
    "REPLAY_EVIDENCE_PLAN_SCHEMA_V2",
    "TEMPORAL_BAR_FILL_POLICY",
    "TEMPORAL_GRAPH_LAB_ARTIFACT_MANIFEST_SCHEMA",
    "TEMPORAL_GRAPH_LAB_RESULT_SCHEMA",
    "TEMPORAL_GRAPH_REPLAY_CAPABILITY",
    "TEMPORAL_GRAPH_REPLAY_JOB_SCHEMA",
    "TEMPORAL_GRAPH_REPLAY_TASK_KIND",
    "TemporalGraphLabContractError",
    "TemporalGraphLabError",
    "TemporalGraphLabMaterializationError",
    "TemporalGraphLabTimeout",
    "ValidatedTemporalGraphLabResult",
    "build_temporal_graph_lab_task",
    "bytes_sha256",
    "canonical_sha256",
    "materialize_temporal_graph_lab_result",
    "run_temporal_graph_lab_tasks",
    "validate_temporal_graph_lab_result",
]
