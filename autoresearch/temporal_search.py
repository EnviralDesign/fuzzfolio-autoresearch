"""Finite, authority-bound Stage 5D temporal candidate/window search.

This module deliberately has no profile mutation logic.  The controller freezes a
small candidate set, turns it into immutable jobs, and is the only component that
may journal, resume, deduplicate, materialize, or select results.  A worker only
receives one candidate/window/cost-view job and evaluates it.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Mapping, Protocol


TEMPORAL_SEARCH_AUTHORITY_SCHEMA = "temporal_graph_candidate_window_authority_v1"
TEMPORAL_SEARCH_PREPARATION_SCHEMA = "temporal_graph_candidate_window_preparation_v1"
TEMPORAL_SEARCH_TASK_KIND = "temporal_graph_candidate_window"
TEMPORAL_SEARCH_JOB_SCHEMA = "temporal_graph_candidate_window_job_v1"
TEMPORAL_SEARCH_CAPABILITY = "temporal_graph_candidate_window_v1"
TEMPORAL_SEARCH_RESULT_SCHEMA = "temporal_graph_candidate_window_result_v1"
TEMPORAL_SEARCH_CHECKPOINT_SCHEMA = "temporal_graph_candidate_window_checkpoint_v1"
TEMPORAL_SEARCH_MANIFEST_SCHEMA = "temporal_graph_candidate_window_manifest_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_COST_VIEWS = ("research_conservative", "none")
_REQUIRED_WORKER_CAPABILITIES = (
    TEMPORAL_SEARCH_CAPABILITY,
    "temporal_graph_replay_v1",
    "management.scalar.price_level.completed_bar",
    "management.scalar.price_distance.completed_bar",
    "management.initial.dynamic",
    "management.trailing.indicator",
    "management.action.dynamic",
)


class TemporalSearchError(RuntimeError):
    pass


class TemporalSearchContractError(TemporalSearchError):
    pass


class TemporalSearchTimeout(TemporalSearchError):
    pass


class LabGatewayClientProtocol(Protocol):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]: ...
    def read_results(self, *, limit: int) -> list[dict[str, Any]]: ...
    def ack_results(self, lease_ids: list[str]) -> int: ...


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(f"{name} must be finite canonical JSON") from exc


def canonical_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    ).hexdigest()


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalSearchContractError(f"{name} must be an object")
    return _clone(dict(value), name=name)


def _safe(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SAFE.fullmatch(token):
        raise TemporalSearchContractError(f"{name} must be a safe explicit identifier")
    return token


def _candidate_id(value: Any, *, name: str) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if not token or not token.replace("_", "").isalnum() or len(token) > 240:
        raise TemporalSearchContractError(f"{name} must be a stable candidate identifier")
    return token


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SHA.fullmatch(token):
        raise TemporalSearchContractError(f"{name} must be an exact sha256 identity")
    return token


def _stamp(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(token[:-1] + "+00:00" if token.endswith("Z") else token)
    except ValueError as exc:
        raise TemporalSearchContractError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise TemporalSearchContractError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(f"refusing to overwrite divergent immutable file: {path}")
    path.write_text(encoded, encoding="utf-8")


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically update mutable controller state; immutable evidence never uses this."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try: os.unlink(temporary)
        except FileNotFoundError: pass
        raise


def _normalized_window(raw: Mapping[str, Any], *, name: str) -> dict[str, Any]:
    window = _mapping(raw, name=name)
    allowed = {"windowId", "analysisWindowStart", "analysisWindowEnd"}
    if set(window) != allowed:
        raise TemporalSearchContractError(f"{name} must contain exactly {sorted(allowed)!r}")
    start = _stamp(window["analysisWindowStart"], name=f"{name}.analysisWindowStart")
    end = _stamp(window["analysisWindowEnd"], name=f"{name}.analysisWindowEnd")
    if _time(start) >= _time(end):
        raise TemporalSearchContractError(f"{name} start must precede end")
    return {"windowId": _safe(window["windowId"], name=f"{name}.windowId"), "analysisWindowStart": start, "analysisWindowEnd": end}


def _candidate_window_input(raw: Mapping[str, Any], *, name: str, candidate: Mapping[str, Any], window: Mapping[str, Any]) -> dict[str, Any]:
    item = _mapping(raw, name=name)
    if set(item) != {"windowId", "evidencePlan"}:
        raise TemporalSearchContractError(f"{name} must contain exactly windowId and evidencePlan")
    if _safe(item["windowId"], name=f"{name}.windowId") != window["windowId"]:
        raise TemporalSearchContractError(f"{name} does not match its development window")
    plan = _mapping(item["evidencePlan"], name=f"{name}.evidencePlan")
    plan_id = _sha(plan.get("plan_id") or plan.get("planId"), name=f"{name}.evidencePlan.planId")
    if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
        raise TemporalSearchContractError(f"{name} requires replay evidence plan v2")
    plan_identity = dict(plan)
    plan_identity.pop("plan_id", None)
    plan_identity.pop("lake_manifest_sha256", None)
    if canonical_sha256(plan_identity) != plan_id:
        raise TemporalSearchContractError(f"{name} evidence plan identity mismatch")
    if (plan.get("profile_snapshot_sha256") or plan.get("profileSnapshotSha256")) != candidate["sourceProfileSha256"]:
        raise TemporalSearchContractError(f"{name} evidence plan profile snapshot mismatch")
    if _stamp(plan.get("analysis_window_start") or plan.get("analysisWindowStart"), name=f"{name}.evidencePlan.analysisWindowStart") != window["analysisWindowStart"] or _stamp(plan.get("analysis_window_end") or plan.get("analysisWindowEnd"), name=f"{name}.evidencePlan.analysisWindowEnd") != window["analysisWindowEnd"]:
        raise TemporalSearchContractError(f"{name} evidence plan window mismatch")
    binding = _mapping(plan.get("lake_window_binding") or plan.get("lakeWindowBinding"), name=f"{name}.evidencePlan.lakeWindowBinding")
    _sha(binding.get("window_semantic_sha256") or binding.get("windowSemanticSha256"), name=f"{name}.evidencePlan.lakeWindowBinding.windowSemanticSha256")
    request = _mapping(binding.get("request"), name=f"{name}.evidencePlan.lakeWindowBinding.request")
    data_start = _stamp(request.get("data_start") or request.get("dataStart"), name=f"{name}.lakeBinding.dataStart")
    data_end = _stamp(request.get("data_end") or request.get("dataEnd"), name=f"{name}.lakeBinding.dataEnd")
    if _time(data_start) > _time(window["analysisWindowStart"]) or _time(data_end) < _time(window["analysisWindowEnd"]):
        raise TemporalSearchContractError(f"{name} lake binding does not cover the development window")
    pairs = {str(value).strip().upper() for value in request.get("pairs") or []}
    timeframes = {str(value).strip().upper() for value in request.get("timeframes") or []}
    if candidate["instrument"] not in pairs or candidate["timeframe"] not in timeframes:
        raise TemporalSearchContractError(f"{name} lake binding does not cover candidate instrument/timeframe")
    execution_config = _mapping(candidate["sourceProfile"].get("executionConfig"), name=f"{name}.sourceProfile.executionConfig")
    management_library = execution_config.get("managementLibrary")
    evidence_cell_sha256 = plan.get("execution_cell_sha256") or plan.get("executionCellSha256")
    if management_library is not None:
        if not isinstance(management_library, Mapping):
            raise TemporalSearchContractError(f"{name} managementLibrary must be an object")
        if evidence_cell_sha256 is not None:
            raise TemporalSearchContractError(f"{name} scalar-management evidence must not bind a legacy execution cell")
    else:
        exit_policy = _mapping(execution_config.get("exitPolicy"), name=f"{name}.sourceProfile.executionConfig.exitPolicy")
        selected_cell = _mapping(exit_policy.get("selectedCell"), name=f"{name}.sourceProfile.executionConfig.exitPolicy.selectedCell")
        expected_cell_sha256 = canonical_sha256(selected_cell)
        if evidence_cell_sha256 != expected_cell_sha256:
            raise TemporalSearchContractError(f"{name} legacy evidence execution-cell identity mismatch")
    return {"windowId": window["windowId"], "evidencePlan": plan, "evidencePlanId": plan_id, "lakeWindowSemanticSha256": binding.get("window_semantic_sha256") or binding.get("windowSemanticSha256")}


def _normalized_candidate(raw: Mapping[str, Any], *, name: str, windows: list[dict[str, Any]]) -> dict[str, Any]:
    candidate = _mapping(raw, name=name)
    allowed = {"candidateId", "sourceProfile", "sourceProfileSha256", "instrument", "timeframe", "barLimit", "windowInputs"}
    if set(candidate) != allowed:
        raise TemporalSearchContractError(f"{name} must contain exactly {sorted(allowed)!r}")
    profile = _mapping(candidate["sourceProfile"], name=f"{name}.sourceProfile")
    profile_sha = _sha(candidate["sourceProfileSha256"], name=f"{name}.sourceProfileSha256")
    if canonical_sha256(profile) != profile_sha:
        raise TemporalSearchContractError(f"{name} source profile identity mismatch")
    if profile.get("version") != "v2" or _mapping(profile.get("graph"), name=f"{name}.sourceProfile.graph").get("kind") != "temporal_graph_v1":
        raise TemporalSearchContractError(f"{name} must be a v2 temporal_graph_v1 profile")
    instruments = profile.get("instruments")
    instrument = str(candidate["instrument"] or "").strip().upper()
    if not isinstance(instruments, list) or instruments != [instrument] or not instrument:
        raise TemporalSearchContractError(f"{name} source profile must have exactly the declared instrument")
    timeframe = str(candidate["timeframe"] or "").strip().upper()
    if not timeframe:
        raise TemporalSearchContractError(f"{name}.timeframe is required")
    try:
        limit = int(candidate["barLimit"])
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(f"{name}.barLimit must be an integer") from exc
    if isinstance(candidate["barLimit"], bool) or not 10 <= limit <= 1_000_000:
        raise TemporalSearchContractError(f"{name}.barLimit is outside admitted bounds")
    base = {"candidateId": _candidate_id(candidate["candidateId"], name=f"{name}.candidateId"), "sourceProfile": profile, "sourceProfileSha256": profile_sha, "instrument": instrument, "timeframe": timeframe, "barLimit": limit}
    inputs = candidate["windowInputs"]
    if not isinstance(inputs, list) or len(inputs) != len(windows): raise TemporalSearchContractError(f"{name}.windowInputs must bind every development window exactly once")
    indexed = {_safe(_mapping(item, name=f"{name}.windowInputs").get("windowId"), name=f"{name}.windowInputs.windowId"): item for item in inputs}
    if len(indexed) != len(inputs) or set(indexed) != {window["windowId"] for window in windows}: raise TemporalSearchContractError(f"{name}.windowInputs does not exactly cover development windows")
    base["windowInputs"] = [_candidate_window_input(indexed[window["windowId"]], name=f"{name}.windowInputs[{window['windowId']}]", candidate=base, window=window) for window in windows]
    if len({entry["evidencePlanId"] for entry in base["windowInputs"]}) != len(base["windowInputs"]): raise TemporalSearchContractError(f"{name} evidence plan identities must be unique by candidate/window")
    return base


def _no_overlap(window: dict[str, Any], protected: list[dict[str, Any]]) -> None:
    for item in protected:
        if _time(window["analysisWindowStart"]) < _time(item["analysisWindowEnd"]) and _time(item["analysisWindowStart"]) < _time(window["analysisWindowEnd"]):
            raise TemporalSearchContractError(f"development window {window['windowId']!r} overlaps prohibited evidence {item['windowId']!r}")


def build_authority(preparation: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze a finite matrix; no profile mutation or date discovery is permitted."""
    payload = _mapping(preparation, name="preparation")
    if payload.get("schemaVersion") != TEMPORAL_SEARCH_PREPARATION_SCHEMA:
        raise TemporalSearchContractError("unknown temporal search preparation schema")
    allowed = {"schemaVersion", "authorityLabel", "workerContract", "candidates", "developmentWindows", "prohibitedEvidence", "bounds"}
    if set(payload) != allowed:
        raise TemporalSearchContractError(f"preparation must contain exactly {sorted(allowed)!r}")
    worker = _mapping(payload["workerContract"], name="workerContract")
    if set(worker) != {"workerContractSha256", "workerContractSchema"}:
        raise TemporalSearchContractError("workerContract must contain workerContractSha256 and workerContractSchema")
    worker_sha = _sha(worker["workerContractSha256"], name="workerContract.workerContractSha256")
    worker_schema = _safe(worker["workerContractSchema"], name="workerContract.workerContractSchema")
    bounds = _mapping(payload["bounds"], name="bounds")
    if set(bounds) != {"maxCandidates", "maxDevelopmentWindows", "maxTasks", "maxAttempts", "deadlineSeconds"}:
        raise TemporalSearchContractError("bounds must be closed and explicit")
    normalized_bounds: dict[str, int | float] = {}
    for key in ("maxCandidates", "maxDevelopmentWindows", "maxTasks", "maxAttempts"):
        try: normalized_bounds[key] = int(bounds[key])
        except (TypeError, ValueError) as exc: raise TemporalSearchContractError(f"bounds.{key} must be an integer") from exc
        if not 1 <= normalized_bounds[key] <= 100_000: raise TemporalSearchContractError(f"bounds.{key} is outside safe limits")
    try: normalized_bounds["deadlineSeconds"] = float(bounds["deadlineSeconds"])
    except (TypeError, ValueError) as exc: raise TemporalSearchContractError("bounds.deadlineSeconds must be numeric") from exc
    if not 1 <= normalized_bounds["deadlineSeconds"] <= 86_400: raise TemporalSearchContractError("bounds.deadlineSeconds is outside safe limits")
    protected_raw = payload["prohibitedEvidence"]
    if not isinstance(protected_raw, list): raise TemporalSearchContractError("prohibitedEvidence must be a list")
    protected: list[dict[str, Any]] = []
    for index, item in enumerate(protected_raw):
        current = _mapping(item, name=f"prohibitedEvidence[{index}]")
        if set(current) != {"windowId", "analysisWindowStart", "analysisWindowEnd", "reason"}: raise TemporalSearchContractError("prohibited evidence entries have a closed schema")
        protected.append({"windowId": _safe(current["windowId"], name="prohibitedEvidence.windowId"), "analysisWindowStart": _stamp(current["analysisWindowStart"], name="prohibitedEvidence.start"), "analysisWindowEnd": _stamp(current["analysisWindowEnd"], name="prohibitedEvidence.end"), "reason": str(current["reason"] or "").strip()})
    if not protected or any(not item["reason"] or _time(item["analysisWindowStart"]) >= _time(item["analysisWindowEnd"]) for item in protected): raise TemporalSearchContractError("prohibited evidence must explicitly identify non-empty protected/reserved windows")
    raw_windows = payload["developmentWindows"]
    if not isinstance(raw_windows, list) or not raw_windows: raise TemporalSearchContractError("developmentWindows must be non-empty")
    windows = [_normalized_window(item, name=f"developmentWindows[{index}]") for index, item in enumerate(raw_windows)]
    if len({x["windowId"] for x in windows}) != len(windows): raise TemporalSearchContractError("development window IDs must be unique")
    for window in windows: _no_overlap(window, protected)
    raw_candidates = payload["candidates"]
    if not isinstance(raw_candidates, list) or not raw_candidates: raise TemporalSearchContractError("candidates must be non-empty")
    candidates = [_normalized_candidate(item, name=f"candidates[{index}]", windows=windows) for index, item in enumerate(raw_candidates)]
    if len({x["candidateId"] for x in candidates}) != len(candidates): raise TemporalSearchContractError("candidate IDs must be unique")
    task_count = len(candidates) * len(windows)
    if len(candidates) > normalized_bounds["maxCandidates"] or len(windows) > normalized_bounds["maxDevelopmentWindows"] or task_count > normalized_bounds["maxTasks"]: raise TemporalSearchContractError("finite task matrix exceeds authority bounds")
    normalized_preparation = {"schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA, "authorityLabel": _safe(payload["authorityLabel"], name="authorityLabel"), "workerContract": {"workerContractSha256": worker_sha, "workerContractSchema": worker_schema}, "bounds": normalized_bounds, "prohibitedEvidence": protected, "developmentWindows": windows, "candidates": candidates}
    authority = {"schemaVersion": TEMPORAL_SEARCH_AUTHORITY_SCHEMA, "authorityLabel": normalized_preparation["authorityLabel"], "preparationSha256": canonical_sha256(normalized_preparation), "workerContract": normalized_preparation["workerContract"], "bounds": normalized_preparation["bounds"], "taskContract": {"taskKind": TEMPORAL_SEARCH_TASK_KIND, "jobSchema": TEMPORAL_SEARCH_JOB_SCHEMA, "capability": TEMPORAL_SEARCH_CAPABILITY, "resultSchema": TEMPORAL_SEARCH_RESULT_SCHEMA, "costViews": list(_COST_VIEWS), "requiredWorkerCapabilities": list(_REQUIRED_WORKER_CAPABILITIES)}, "prohibitedEvidence": protected, "developmentWindows": windows, "candidates": candidates, "executionPolicy": {"controllerOwns": ["generation", "validation", "checkpoint", "journal", "resume", "dedup", "materialization", "basic_selection"], "workerOnly": ["evaluate_immutable_job"], "mutationEnginePermitted": False, "longEconomicSearchPermitted": False, "reservedEvidencePermitted": False}}
    authority["authorityId"] = canonical_sha256(authority)
    return authority


def validate_authority(authority: Mapping[str, Any]) -> dict[str, Any]:
    current = _mapping(authority, name="authority")
    supplied = _sha(current.pop("authorityId", None), name="authority.authorityId")
    # Rebuild validates closed schemas and the exact authority identity.
    source_candidates = []
    for candidate in current.get("candidates", []):
        copied = _mapping(candidate, name="authority.candidate")
        copied["windowInputs"] = [{"windowId": entry.get("windowId"), "evidencePlan": entry.get("evidencePlan")} for entry in copied.get("windowInputs", [])]
        source_candidates.append(copied)
    preparation = {"schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA, "authorityLabel": current.get("authorityLabel"), "workerContract": current.get("workerContract"), "candidates": source_candidates, "developmentWindows": current.get("developmentWindows"), "prohibitedEvidence": current.get("prohibitedEvidence"), "bounds": current.get("bounds")}
    rebuilt = build_authority(preparation)
    if current != {key: value for key, value in rebuilt.items() if key != "authorityId"} or supplied != rebuilt["authorityId"]: raise TemporalSearchContractError("authority identity or immutable semantics mismatch")
    return rebuilt


def build_task_matrix(authority: Mapping[str, Any]) -> list[dict[str, Any]]:
    frozen = validate_authority(authority)
    tasks: list[dict[str, Any]] = []
    for candidate in frozen["candidates"]:
        for window in frozen["developmentWindows"]:
            input_by_window = {item["windowId"]: item for item in candidate["windowInputs"]}
            evidence = input_by_window[window["windowId"]]
            shared_id = canonical_sha256({"candidateSnapshotSha256": candidate["sourceProfileSha256"], "evidencePlanId": evidence["evidencePlanId"], "windowId": window["windowId"], "windowSemanticSha256": evidence["lakeWindowSemanticSha256"]})
            identity = {"authorityId": frozen["authorityId"], "candidateId": candidate["candidateId"], "windowId": window["windowId"]}
            task_id = "temporal-search-" + canonical_sha256(identity).removeprefix("sha256:")[:32]
            profile = candidate["sourceProfile"]
            execution_config = _mapping(profile.get("executionConfig"), name=f"candidate[{candidate['candidateId']}].sourceProfile.executionConfig")
            management_library = execution_config.get("managementLibrary")
            execution_binding: dict[str, Any]
            if management_library is not None:
                if not isinstance(management_library, Mapping):
                    raise TemporalSearchContractError("managementLibrary must be an object")
                execution_binding = {"execution_config_sha256": canonical_sha256(execution_config)}
            else:
                exit_policy = _mapping(execution_config.get("exitPolicy"), name="sourceProfile.executionConfig.exitPolicy")
                execution_binding = {"execution_cell": _mapping(exit_policy.get("selectedCell"), name="sourceProfile.executionConfig.exitPolicy.selectedCell")}
            job = {
                "schema_version": TEMPORAL_SEARCH_JOB_SCHEMA,
                "job_id": task_id,
                "candidate_id": candidate["candidateId"],
                "authority_id": frozen["authorityId"],
                "lake_window_semantic_sha256": evidence["lakeWindowSemanticSha256"],
                "shared_observation_stream_id": shared_id,
                "user_id": "temporal-search",
                "profile_id": candidate["candidateId"],
                "inline_profile_snapshot": profile,
                "instruments": [candidate["instrument"]],
                "timeframe": candidate["timeframe"],
                "bar_limit": candidate["barLimit"],
                "analysis_window_start": window["analysisWindowStart"],
                "analysis_window_end": window["analysisWindowEnd"],
                "evidence_plan": evidence["evidencePlan"],
                "required_worker_contract_hash": frozen["workerContract"]["workerContractSha256"],
                "required_worker_contract_schema": frozen["workerContract"]["workerContractSchema"],
                "required_capabilities": list(_REQUIRED_WORKER_CAPABILITIES),
                "client_origin": "temporal_search_controller",
                "campaign_id": frozen["authorityId"],
                "lane_id": candidate["candidateId"],
                "attempt_id": task_id,
                **execution_binding,
            }
            tasks.append({"task_id": task_id, "lane_id": candidate["candidateId"], "attempt_id": task_id, "task_kind": TEMPORAL_SEARCH_TASK_KIND, "payload": job, "required_worker_capabilities": list(_REQUIRED_WORKER_CAPABILITIES), "deadline_seconds": frozen["bounds"]["deadlineSeconds"], "max_attempts": frozen["bounds"]["maxAttempts"]})
    if len({task["task_id"] for task in tasks}) != len(tasks): raise TemporalSearchContractError("task identity collision")
    return tasks


def materialize_plan(authority: Mapping[str, Any], output_root: Path | str) -> dict[str, Any]:
    frozen = validate_authority(authority)
    tasks = build_task_matrix(frozen)
    root = Path(output_root)
    manifest = {"schemaVersion": TEMPORAL_SEARCH_MANIFEST_SCHEMA, "authorityId": frozen["authorityId"], "taskCount": len(tasks), "tasks": tasks, "taskMatrixSha256": canonical_sha256(tasks)}
    _write_json(root / "authority.json", frozen)
    _write_json(root / "task-manifest.json", manifest)
    checkpoint_path = root / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint = _mapping(json.loads(checkpoint_path.read_text(encoding="utf-8")), name="checkpoint")
        if checkpoint.get("schemaVersion") != TEMPORAL_SEARCH_CHECKPOINT_SCHEMA or checkpoint.get("authorityId") != frozen["authorityId"] or checkpoint.get("taskMatrixSha256") != manifest["taskMatrixSha256"]:
            raise TemporalSearchContractError("existing checkpoint does not bind this immutable authority and task matrix")
    else:
        _write_checkpoint(checkpoint_path, {"schemaVersion": TEMPORAL_SEARCH_CHECKPOINT_SCHEMA, "authorityId": frozen["authorityId"], "taskMatrixSha256": manifest["taskMatrixSha256"], "completed": {}, "journal": []})
    return manifest


def _result_material(task: Mapping[str, Any], completion: Mapping[str, Any]) -> dict[str, Any]:
    if str(completion.get("status") or "").lower() != "success": raise TemporalSearchContractError("worker completion is not successful")
    if completion.get("task_id") != task.get("task_id") or completion.get("lane_id") != task.get("lane_id") or completion.get("attempt_id") != task.get("attempt_id"): raise TemporalSearchContractError("completion routing identity mismatch")
    envelope = _mapping(completion.get("result"), name="worker envelope")
    if envelope.get("status") != "success" or envelope.get("job_kind") != TEMPORAL_SEARCH_TASK_KIND: raise TemporalSearchContractError("worker envelope does not prove a successful temporal candidate/window job")
    material = _mapping(envelope.get("result"), name="worker material result")
    job = _mapping(task.get("payload"), name="task payload")
    required = {"schema_version": TEMPORAL_SEARCH_RESULT_SCHEMA, "task_kind": TEMPORAL_SEARCH_TASK_KIND, "job_id": job["job_id"], "authority_id": job["authority_id"], "candidate_id": job["candidate_id"], "evidence_plan_id": job["evidence_plan"]["plan_id"], "lake_window_semantic_sha256": job["lake_window_semantic_sha256"], "shared_observation_stream_id": job["shared_observation_stream_id"]}
    for key, expected in required.items():
        if material.get(key) != expected: raise TemporalSearchContractError(f"worker material result mismatch for {key}")
    cost_results = _mapping(material.get("cost_view_results"), name="worker material cost_view_results")
    if set(cost_results) != set(_COST_VIEWS): raise TemporalSearchContractError("worker result must contain exactly both admitted cost views")
    stream_hashes: set[str] = set()
    for cost_view in _COST_VIEWS:
        replay = _mapping(cost_results[cost_view], name=f"worker material cost_view_results.{cost_view}")
        if replay.get("cost_view") not in (None, cost_view): raise TemporalSearchContractError(f"worker cost result mismatch for {cost_view}")
        stream_hashes.add(_sha(replay.get("observation_stream_sha256"), name=f"worker material cost_view_results.{cost_view}.observation_stream_sha256"))
    if len(stream_hashes) != 1: raise TemporalSearchContractError("both cost views must be evaluated from the identical observation stream")
    artifact_sha256 = _sha(material.get("artifact_sha256"), name="worker material artifact_sha256")
    artifact_size_bytes = material.get("artifact_size_bytes")
    if isinstance(artifact_size_bytes, bool) or not isinstance(artifact_size_bytes, int) or artifact_size_bytes < 1:
        raise TemporalSearchContractError("worker material artifact_size_bytes must be a positive integer")
    if len(json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")) != artifact_size_bytes:
        raise TemporalSearchContractError("worker material artifact byte count mismatch")
    artifact_identity = _clone(material, name="worker material artifact identity")
    artifact_identity.pop("artifact_sha256", None)
    artifact_identity.pop("artifact_size_bytes", None)
    diagnostics = _mapping(artifact_identity.get("diagnostics"), name="worker material diagnostics")
    diagnostics.pop("artifact_size_bytes", None)
    artifact_identity["diagnostics"] = diagnostics
    if canonical_sha256(artifact_identity) != artifact_sha256:
        raise TemporalSearchContractError("worker material artifact identity mismatch")
    return material


def run_temporal_search_tasks(client: LabGatewayClientProtocol, authority: Mapping[str, Any], *, output_root: Path | str, timeout_seconds: float = 900.0, poll_interval_seconds: float = 0.25, resume: bool = False) -> dict[str, Any]:
    """Execute an already finite plan; intentionally no search expansion occurs."""
    manifest = materialize_plan(authority, output_root)
    root = Path(output_root); checkpoint_path = root / "checkpoint.json"
    checkpoint = _mapping(json.loads(checkpoint_path.read_text(encoding="utf-8")), name="checkpoint")
    if not resume and checkpoint["completed"]: raise TemporalSearchContractError("non-resume temporal search already has completed tasks")
    tasks = {item["task_id"]: item for item in manifest["tasks"]}; completed = _mapping(checkpoint["completed"], name="checkpoint.completed")

    def consume(completion: Mapping[str, Any]) -> None:
        task_id = str(completion.get("task_id") or "")
        task = tasks.get(task_id)
        if task is None: raise TemporalSearchContractError("unrelated Lab result encountered")
        material = _result_material(task, completion)
        lease = _safe(completion.get("lease_id"), name="completion.lease_id")
        result_path = root / "results" / f"{task_id}.json"
        # Materialize before acknowledgement.  A redelivery of the same immutable
        # material is harmless; a different result for the task fails closed.
        _write_json(result_path, material)
        digest = canonical_sha256(material)
        prior = completed.get(task_id)
        if prior is not None and prior.get("resultSha256") != digest:
            raise TemporalSearchContractError("conflicting duplicate temporal search result")
        if prior is None:
            completed[task_id] = {"resultSha256": digest, "resultPath": str(result_path), "candidateId": task["payload"]["candidate_id"]}
            checkpoint["completed"] = completed
            checkpoint["journal"] = list(checkpoint.get("journal") or []) + [{"taskId": task_id, "resultSha256": digest}]
            _write_checkpoint(checkpoint_path, checkpoint)
        if client.ack_results([lease]) != 1:
            raise TemporalSearchContractError("gateway did not acknowledge temporal search result")

    # Consume a prior delivery before enqueue so restart after materialization is
    # idempotent and does not create a second economic evaluation.
    for completion in client.read_results(limit=max(8, len(tasks) * 2)):
        consume(completion)
    pending = [task for task_id, task in tasks.items() if task_id not in completed]
    if pending:
        receipt = client.enqueue_tasks(pending)
        if int(receipt.get("enqueued") or 0) != len(pending): raise TemporalSearchContractError("gateway did not enqueue the exact pending task set")
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while pending:
        if time.monotonic() >= deadline: raise TemporalSearchTimeout("timed out waiting for temporal search results")
        results = client.read_results(limit=max(8, len(pending) * 2))
        if not results:
            time.sleep(max(float(poll_interval_seconds), 0.01)); continue
        for completion in results:
            consume(completion)
            pending = [item for item in pending if item["task_id"] != str(completion.get("task_id") or "")]
    # Basic selection is intentionally transparent and optional: rank finite numeric score only.
    rows = []
    for task_id, item in completed.items():
        result = json.loads(Path(item["resultPath"]).read_text(encoding="utf-8")); score = result.get("selection_score")
        if isinstance(score, (int, float)) and not isinstance(score, bool): rows.append({"taskId": task_id, "candidateId": item["candidateId"], "selectionScore": float(score)})
    rows.sort(key=lambda row: (-row["selectionScore"], row["candidateId"], row["taskId"]))
    summary = {"schemaVersion": "temporal_graph_candidate_window_run_result_v1", "authorityId": manifest["authorityId"], "taskCount": len(tasks), "completedTaskCount": len(completed), "selection": rows}
    _write_json(root / "summary.json", summary)
    return summary


__all__ = [name for name in globals() if name.startswith("TEMPORAL_SEARCH_")] + ["TemporalSearchContractError", "TemporalSearchError", "TemporalSearchTimeout", "build_authority", "build_task_matrix", "canonical_sha256", "materialize_plan", "run_temporal_search_tasks", "validate_authority"]
