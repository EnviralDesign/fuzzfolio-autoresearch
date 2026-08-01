"""Freeze the Stage 5E-3 E/F screening midpoint without opening G/H."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

import httpx

from .play_hand_lab_auth import load_lab_gateway_token
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_discovery_results import (
    _aggregate_candidate,
    _distribution,
    _l1_distribution_distance,
    _log_distance,
    _result_set_sha256,
    fingerprint_distance,
    load_stage_results,
)
from .temporal_search import build_task_matrix, canonical_sha256, validate_authority
from .temporal_search_activation import (
    DEPTH_RANK,
    _authored_instances,
    _combine_instance,
    _group_summaries,
)
from .temporal_search_quality import (
    _behavior_record,
    _diversity_report,
    _management_rows,
    _numeric_summary,
    _pearson,
    _raw_stage_results,
    _structural_record,
)
from .temporal_search_selector_v2 import (
    SELECTOR_V2_PARAMETERS,
    SELECTOR_V2_VERSION,
    evaluate_policy_v2_envelope,
    select_policy_v2,
)


MIDPOINT_SCHEMA = "temporal_search_stage5e3_midpoint_v1"
MIDPOINT_MANIFEST_SCHEMA = "temporal_search_stage5e3_midpoint_manifest_v1"
SELECTOR_ENVELOPE_SCHEMA = "temporal_search_stage5e3_selector_envelope_v1"
ACTIVATION_SCHEMA = "temporal_search_stage5e3_activation_v1"
AGGREGATE_SCHEMA = "temporal_search_stage5e3_screening_aggregates_v1"
CANDIDATE_ANALYSIS_SCHEMA = "temporal_search_stage5e3_candidate_analysis_v1"
GATEWAY_FINAL_SCHEMA = "temporal_search_stage5e3_gateway_final_v1"
EXPECTED_WORKER_CONTRACT = (
    "sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9"
)
_SHA40 = re.compile(r"[0-9a-f]{40}")


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return value


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    ) + "\n"


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing divergent artifact: {path}")
    path.write_text(encoded, encoding="utf-8")


def _write_text_immutable(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != value:
        raise TemporalDiscoveryContractError(f"refusing divergent artifact: {path}")
    path.write_text(value, encoding="utf-8")


def _file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _safe_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        output = float(value)
    except (TypeError, ValueError):
        return 0.0
    return output if math.isfinite(output) else 0.0


def _activity_category(e_trades: int, f_trades: int) -> str:
    if e_trades > 0 and f_trades > 0:
        return "active_both"
    if e_trades > 0:
        return "active_e_only"
    if f_trades > 0:
        return "active_f_only"
    return "inactive_both"


def _average_ranks(values: Sequence[float]) -> list[float]:
    indexed = sorted(enumerate(float(value) for value in values), key=lambda row: row[1])
    ranks = [0.0] * len(indexed)
    cursor = 0
    while cursor < len(indexed):
        end = cursor + 1
        while end < len(indexed) and indexed[end][1] == indexed[cursor][1]:
            end += 1
        rank = ((cursor + 1) + end) / 2.0
        for index in range(cursor, end):
            ranks[indexed[index][0]] = rank
        cursor = end
    return ranks


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    return _pearson(_average_ranks(left), _average_ranks(right))


def _structural_family(candidate: Mapping[str, Any]) -> dict[str, Any]:
    row = _structural_record(candidate)
    shape = {
        "stateCount": row["stateCount"],
        "transitionCount": row["transitionCount"],
        "guardKindCounts": row["guardKindCounts"],
        "maximumGuardDepth": row["maximumGuardDepth"],
        "routeCount": row["routeCount"],
        "entryActionCount": row["entryActionCount"],
        "postEntryActionKinds": row["postEntryActionKinds"],
        "discretionaryExitCount": len(row["discretionaryExitTransitionIds"]),
    }
    return {"familySha256": canonical_sha256(shape), "shape": shape, "record": row}


def _management_family(candidate: Mapping[str, Any]) -> dict[str, Any]:
    profile = candidate["sourceProfile"]
    plans = _management_rows(profile)
    break_even = any(
        action.get("kind") == "move_stop_to_break_even_next_open"
        for transition in (profile.get("graph") or {}).get("transitions") or []
        for action in transition.get("actions") or []
    )
    shape = {"breakEvenAuthored": break_even, "plans": plans}
    return {"familySha256": canonical_sha256(shape), "shape": shape}


def _window_label_lookup(campaign: Mapping[str, Any]) -> dict[tuple[str, str], str]:
    lookup = {
        (str(row["analysisWindowStart"]), str(row["analysisWindowEnd"])): str(
            row["label"]
        )
        for row in campaign.get("windows") or []
        if row.get("label") in {"E", "F"}
    }
    if set(lookup.values()) != {"E", "F"} or len(lookup) != 2:
        raise TemporalDiscoveryContractError("campaign must define exact E/F windows")
    return lookup


def _final_gateway_snapshot(gateway_url: str) -> dict[str, Any]:
    token = load_lab_gateway_token(create=False)
    response = httpx.get(
        f"{gateway_url.rstrip('/')}/snapshot?include_workers=true",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10.0,
    )
    response.raise_for_status()
    snapshot = response.json()
    if not isinstance(snapshot, dict):
        raise TemporalDiscoveryContractError("Gateway snapshot root is invalid")
    zero_fields = (
        "queued_tasks",
        "active_leases",
        "live_tasks",
        "failed_tasks",
        "result_backlog",
        "result_backlog_bytes",
        "retained_task_count",
        "stale_worker_count",
    )
    if any(int(snapshot.get(key) or 0) != 0 for key in zero_fields):
        raise TemporalDiscoveryContractError("Gateway did not return fully empty")
    metrics = snapshot.get("metrics") or {}
    required_zero_metrics = (
        "duplicate_completions",
        "duplicate_task_enqueues",
        "expired_leases_requeued",
        "failed_completions",
        "failures_final",
        "failures_requeued",
        "incompatible_claims",
        "lost_completions",
        "results_dropped",
        "stale_worker_leases_final",
        "stale_worker_leases_requeued",
        "worker_instance_leases_final",
        "worker_instance_leases_requeued",
    )
    if any(int(metrics.get(key) or 0) != 0 for key in required_zero_metrics):
        raise TemporalDiscoveryContractError("Gateway recorded a terminal integrity fault")
    if (
        int(snapshot.get("completed_tasks") or 0) != 256
        or int(metrics.get("completions_accepted") or 0) != 256
        or int(metrics.get("results_acked") or 0) != 256
    ):
        raise TemporalDiscoveryContractError("Gateway completion accounting is incomplete")
    workers = list(snapshot.get("workers") or [])
    if not workers or any(
        worker.get("contract_hash") != EXPECTED_WORKER_CONTRACT
        or worker.get("online") is not True
        or worker.get("stale") is True
        for worker in workers
    ):
        raise TemporalDiscoveryContractError("final worker fleet is not exact")
    pool_counts = Counter(str(worker.get("pool") or "unknown") for worker in workers)
    value = {
        "schemaVersion": GATEWAY_FINAL_SCHEMA,
        "gatewayId": snapshot.get("gateway_id"),
        "completedTaskCount": 256,
        "materializedTaskCount": 256,
        "checkpointedTaskCount": 256,
        "acknowledgedTaskCount": 256,
        "onlineWorkerCount": len(workers),
        "workerSlotCount": int(snapshot.get("worker_slots") or 0),
        "workerPoolCounts": dict(sorted(pool_counts.items())),
        "workerContractSha256": EXPECTED_WORKER_CONTRACT,
        "queueEmpty": True,
        "leasesEmpty": True,
        "resultBacklogEmpty": True,
        "retainedTaskSetEmpty": True,
        "staleWorkerCount": 0,
        "gatewayMetrics": {key: metrics.get(key) for key in sorted(metrics)},
    }
    value["gatewayFinalSha256"] = canonical_sha256(value)
    return value


def _result_integrity(
    *,
    root: Path,
    run_root: Path,
    population_ids: set[str],
    label_lookup: Mapping[tuple[str, str], str],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    authority = validate_authority(
        _read(root / "screening" / "authority.json", name="screening authority")
    )
    tasks = build_task_matrix(authority)
    run_manifest = _read(run_root / "task-manifest.json", name="run task manifest")
    plan_manifest = _read(
        root / "screening-plan-only" / "task-manifest.json",
        name="prelaunch task manifest",
    )
    if (
        len(tasks) != 256
        or run_manifest.get("taskMatrixSha256") != canonical_sha256(tasks)
        or plan_manifest.get("taskMatrixSha256") != canonical_sha256(tasks)
        or run_manifest.get("tasks") != tasks
        or plan_manifest.get("tasks") != tasks
    ):
        raise TemporalDiscoveryContractError("screening task matrix drift")
    checkpoint = _read(run_root / "checkpoint.json", name="run checkpoint")
    completed = checkpoint.get("completed") or {}
    journal = checkpoint.get("journal") or []
    if len(completed) != 256 or len(journal) != 256:
        raise TemporalDiscoveryContractError("screening checkpoint is incomplete")
    task_ids = {task["task_id"] for task in tasks}
    if set(completed) != task_ids or {row.get("taskId") for row in journal} != task_ids:
        raise TemporalDiscoveryContractError("screening checkpoint task set drift")
    raw_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pool_counts: Counter[str] = Counter()
    window_counts: Counter[str] = Counter()
    result_files = []
    for task_id in sorted(task_ids):
        row = completed[task_id]
        path = Path(str(row.get("resultPath") or ""))
        try:
            path.resolve().relative_to((run_root / "results").resolve())
        except ValueError as exc:
            raise TemporalDiscoveryContractError("result escaped screening root") from exc
        payload = _read(path, name=f"screening result {task_id}")
        if canonical_sha256(payload) != row.get("resultSha256"):
            raise TemporalDiscoveryContractError(f"screening result hash drift: {task_id}")
        bounds = (
            str(payload.get("analysis_window_start") or ""),
            str(payload.get("analysis_window_end") or ""),
        )
        label = label_lookup.get(bounds)
        worker = payload.get("worker_attribution") or {}
        evidence = payload.get("execution_evidence") or {}
        candidate_id = str(payload.get("candidate_id") or "")
        if (
            label not in {"E", "F"}
            or payload.get("job_id") != task_id
            or payload.get("authority_id") != authority["authorityId"]
            or candidate_id not in population_ids
            or worker.get("worker_contract_hash") != EXPECTED_WORKER_CONTRACT
            or evidence.get("evidence_role") != "development_parity"
            or evidence.get("coverage_policy") != "require_complete"
            or evidence.get("data_availability_cutoff") != "2026-07-29T00:00:00Z"
        ):
            raise TemporalDiscoveryContractError(f"screening result contract drift: {task_id}")
        payload["_stage5e3WindowLabel"] = label
        raw_by_candidate[candidate_id].append(payload)
        pool_counts[str(worker.get("worker_pool") or "unknown")] += 1
        window_counts[label] += 1
        result_files.append(
            {
                "taskId": task_id,
                "candidateId": candidate_id,
                "windowLabel": label,
                "resultSha256": row["resultSha256"],
                "fileSha256": _file_sha256(path),
            }
        )
    if (
        set(raw_by_candidate) != population_ids
        or any(len(rows) != 2 for rows in raw_by_candidate.values())
        or window_counts != Counter({"E": 128, "F": 128})
    ):
        raise TemporalDiscoveryContractError("screening results do not cover exact E/F matrix")
    for candidate_id, rows in raw_by_candidate.items():
        if {row["_stage5e3WindowLabel"] for row in rows} != {"E", "F"}:
            raise TemporalDiscoveryContractError(f"candidate lacks E/F pair: {candidate_id}")
        rows.sort(key=lambda row: row["_stage5e3WindowLabel"])
    integrity = {
        "screeningAuthorityId": authority["authorityId"],
        "taskMatrixSha256": canonical_sha256(tasks),
        "taskCount": len(tasks),
        "resultCount": len(result_files),
        "windowTaskCounts": dict(sorted(window_counts.items())),
        "workerPoolTaskCounts": dict(sorted(pool_counts.items())),
        "workerContractSha256": EXPECTED_WORKER_CONTRACT,
        "resultInventorySha256": canonical_sha256(result_files),
        "materializedEqualsCheckpointed": True,
        "reservedEvidenceReferenced": False,
        "confirmationResultReferenced": False,
    }
    return integrity, dict(raw_by_candidate)


def _instance_window_active(window: Mapping[str, Any]) -> bool:
    return DEPTH_RANK[str(window["deepestReachedState"])] >= DEPTH_RANK[
        "activated_successfully"
    ]


def _instance_window_opportunity(
    instance: Mapping[str, Any], window: Mapping[str, Any]
) -> bool:
    eligible = int(window.get("positionEligibleCount") or 0) > 0
    if not eligible:
        return False
    if instance.get("activationMode") == "immediate":
        return True
    return window.get("unrealizedThresholdPossible") is not False and window.get(
        "positionAgePossible"
    ) is not False


def _activation_summary(instances: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    summaries = []
    for (management_type, activation_mode), group_iter in sorted(
        _groups(instances, lambda row: (row["managementType"], row["activationMode"])).items()
    ):
        group = list(group_iter)
        windows = [window for row in group for window in row["windowStates"]]
        activated_windows = [window for window in windows if _instance_window_active(window)]
        dormant_windows = [window for window in windows if not _instance_window_active(window)]
        opportunities = [
            window
            for row in group
            for window in row["windowStates"]
            if _instance_window_opportunity(row, window)
        ]
        rejection_reasons = sum(
            (Counter(row.get("rejectionReasonCounts") or {}) for row in group), Counter()
        )
        summaries.append(
            {
                "managementType": management_type,
                "activationMode": activation_mode,
                "authoredInstanceCount": len(group),
                "activatedInstanceCount": sum(bool(row["activated"]) for row in group),
                "neverActivatedInstanceCount": sum(not row["activated"] for row in group),
                "neverActivatedShare": sum(not row["activated"] for row in group)
                / len(group),
                "feasibleOpportunityInstanceCount": sum(
                    any(_instance_window_opportunity(row, window) for window in row["windowStates"])
                    for row in group
                ),
                "sourceStateOrPlanOccupiedWindowCount": sum(
                    int(window.get("sourceStateOccupancy") or 0) > 0
                    or int(window.get("positionEligibleCount") or 0) > 0
                    for window in windows
                ),
                "guardEvaluatedOrEligibleWindowCount": sum(
                    int(window.get("sourceStateOccupancy") or 0) > 0
                    or int(window.get("positionEligibleCount") or 0) > 0
                    for window in windows
                ),
                "intentScheduledCount": sum(
                    int(window.get("intentScheduledCount") or 0)
                    + int(window.get("trailingScheduleCount") or 0)
                    for window in windows
                ),
                "acceptedEffectCount": sum(
                    int(window.get("positionChangeCount") or 0)
                    + int(window.get("automaticOrExplicitActivationCount") or 0)
                    + int(window.get("trailingUpdateCount") or 0)
                    for window in windows
                ),
                "rejectedEffectCount": sum(
                    int(window.get("intentRejectedCount") or 0)
                    + int(window.get("rejectedEffectCount") or 0)
                    for window in windows
                ),
                "positionChangeCount": sum(
                    int(window.get("positionChangeCount") or 0)
                    + int(window.get("automaticOrExplicitActivationCount") or 0)
                    + int(window.get("trailingUpdateCount") or 0)
                    for window in windows
                ),
                "changedClosureCount": sum(
                    int(window.get("changedClosureCount") or 0) for window in windows
                ),
                "rejectionReasonCounts": dict(sorted(rejection_reasons.items())),
                "maximumFavorableExcursionR": {
                    "activatedWindows": _numeric_summary(
                        window.get("maximumFavorableExcursionR")
                        for window in activated_windows
                        if window.get("maximumFavorableExcursionR") is not None
                    ),
                    "dormantWindows": _numeric_summary(
                        window.get("maximumFavorableExcursionR")
                        for window in dormant_windows
                        if window.get("maximumFavorableExcursionR") is not None
                    ),
                    "feasibleOpportunityWindows": _numeric_summary(
                        window.get("maximumFavorableExcursionR")
                        for window in opportunities
                        if window.get("maximumFavorableExcursionR") is not None
                    ),
                },
                "maximumHoldingBars": {
                    "activatedWindows": _numeric_summary(
                        window.get("maximumHoldingBars")
                        for window in activated_windows
                        if window.get("maximumHoldingBars") is not None
                    ),
                    "dormantWindows": _numeric_summary(
                        window.get("maximumHoldingBars")
                        for window in dormant_windows
                        if window.get("maximumHoldingBars") is not None
                    ),
                    "feasibleOpportunityWindows": _numeric_summary(
                        window.get("maximumHoldingBars")
                        for window in opportunities
                        if window.get("maximumHoldingBars") is not None
                    ),
                },
            }
        )
    type_summary = []
    for management_type, group in sorted(
        _groups(instances, lambda row: row["managementType"]).items()
    ):
        count = len(group)
        dormant = sum(not row["activated"] for row in group)
        type_summary.append(
            {
                "managementType": management_type,
                "authoredInstanceCount": count,
                "activatedInstanceCount": count - dormant,
                "neverActivatedInstanceCount": dormant,
                "neverActivatedShare": dormant / count,
                "severeDormancy": count >= 20 and dormant / count >= 0.75,
            }
        )
    stability = Counter()
    for row in instances:
        active = sum(_instance_window_active(window) for window in row["windowStates"])
        stability[{0: "inactive_both", 1: "active_one_window", 2: "active_both"}[active]] += 1
    explicit = [
        row
        for row in instances
        if row["managementType"] == "trailing_stop"
        and row.get("activationMode") == "explicit"
    ]
    explicit_opportunity = any(
        _instance_window_opportunity(row, window)
        for row in explicit
        for window in row["windowStates"]
    )
    explicit_activation_count = sum(bool(row["activated"]) for row in explicit)
    return {
        "managementInstanceCount": len(instances),
        "managementTypeSummary": type_summary,
        "managementModeSummary": summaries,
        "activationStability": dict(sorted(stability.items())),
        "deepestStateCounts": dict(
            sorted(Counter(row["deepestReachedState"] for row in instances).items())
        ),
        "rejectionReasonCounts": dict(
            sorted(
                sum(
                    (Counter(row.get("rejectionReasonCounts") or {}) for row in instances),
                    Counter(),
                ).items()
            )
        ),
        "explicitTrailing": {
            "authoredInstanceCount": len(explicit),
            "feasibleOpportunityObserved": explicit_opportunity,
            "activatedInstanceCount": explicit_activation_count,
            "zeroActivationDespiteOpportunity": explicit_opportunity
            and explicit_activation_count == 0,
        },
        "groupSummaries": _group_summaries(instances),
    }


def _groups(values: Iterable[Any], key) -> dict[Any, list[Any]]:
    output: dict[Any, list[Any]] = defaultdict(list)
    for value in values:
        output[key(value)].append(value)
    return dict(output)


def _candidate_group_summary(
    rows: Sequence[Mapping[str, Any]], dimension: str, *, explode: bool = False
) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        values = row.get(dimension) if explode else [row.get(dimension)]
        for value in values or ["none"]:
            groups[str(value or "none")].append(row)
    output = []
    for value, group in sorted(groups.items()):
        output.append(
            {
                "value": value,
                "candidateCount": len(group),
                "activityCounts": dict(
                    sorted(Counter(str(row["activityCategory"]) for row in group).items())
                ),
                "totalTrades": _numeric_summary(row["totalTrades"] for row in group),
                "eTrades": _numeric_summary(row["windows"]["E"]["trades"] for row in group),
                "fTrades": _numeric_summary(row["windows"]["F"]["trades"] for row in group),
                "totalConservativeNetR": _numeric_summary(
                    row["totalConservativeNetR"] for row in group
                ),
                "worstWindowConservativeNetR": _numeric_summary(
                    row["worstWindowConservativeNetR"] for row in group
                ),
                "costDragR": _numeric_summary(row["costDragR"] for row in group),
                "managementActivationCount": _numeric_summary(
                    row["managementActivationCount"] for row in group
                ),
                "rejectedIntentCount": _numeric_summary(
                    row["rejectedIntentCount"] for row in group
                ),
            }
        )
    return output


def _markdown(report: Mapping[str, Any]) -> str:
    activity = report["activity"]
    selector = report["selectorGate"]
    failure = report["classification"]
    lines = [
        "# Stage 5E-3 E/F mandatory midpoint",
        "",
        f"Primary outcome: `{failure['primaryOutcome']}`",
        "",
        "## Execution integrity",
        "",
        "- 256/256 tasks materialized, checkpointed, and acknowledged.",
        "- Gateway returned to zero queue, leases, result backlog, and retained tasks.",
        "- Zero terminal failures, requeues, drops, incompatible claims, or lost completions.",
        "- Only E/F development evidence was used; G/H and reserved evidence remained untouched.",
        "",
        "## Activity",
        "",
        f"- Active in both E and F: {activity['activityCounts'].get('active_both', 0)}",
        f"- Active only in E: {activity['activityCounts'].get('active_e_only', 0)}",
        f"- Active only in F: {activity['activityCounts'].get('active_f_only', 0)}",
        f"- Inactive in both: {activity['activityCounts'].get('inactive_both', 0)}",
        "",
        "## Selector v2",
        "",
        f"- Active population: {selector['activePopulationCount']}",
        f"- Robust-envelope eligible: {selector['eligibleCandidateCount']} / "
        f"{selector['minimumEligibleCandidates']}",
        f"- Status: `{selector['status']}`",
        "- No archives, selected union, controls, confirmation cohort, or G/H authority were created.",
        "",
        "## Predeclared generator failure checks",
        "",
    ]
    for reason in failure["failureReasons"]:
        lines.append(f"- `{reason}`")
    lines.extend(
        [
            "",
            "## Decision boundary",
            "",
            "Stage 5E-3 stops here for deep review. Thresholds were not relaxed. "
            "G/H confirmation, broader search, candidate promotion, and production use remain blocked.",
            "",
        ]
    )
    return "\n".join(lines)


def freeze_stage5e3_midpoint(
    *,
    root: Path | str,
    output_root: Path | str,
    autoresearch_analysis_commit: str,
    gateway_url: str = "http://127.0.0.1:8799",
) -> dict[str, Any]:
    base = Path(root).resolve()
    run_root = base / "screening-run"
    output = Path(output_root).resolve()
    if output == base or base in output.parents:
        raise TemporalDiscoveryContractError("midpoint output must be outside prelaunch root")
    if not _SHA40.fullmatch(autoresearch_analysis_commit):
        raise TemporalDiscoveryContractError(
            "AutoResearch analysis commit must be an exact lowercase commit SHA"
        )
    campaign = _read(base / "campaign-spec.json", name="campaign spec")
    prelaunch = _read(base / "checkpoint.json", name="prelaunch checkpoint")
    population_payload = _read(base / "generator-v2" / "population.json", name="population")
    candidates = list(population_payload.get("candidates") or [])
    if len(candidates) != 128:
        raise TemporalDiscoveryContractError("Stage 5E-3 midpoint requires 128 candidates")
    candidate_map = {str(row["candidateId"]): row for row in candidates}
    if len(candidate_map) != 128:
        raise TemporalDiscoveryContractError("population candidate identities collide")
    label_lookup = _window_label_lookup(campaign)
    integrity, raw_by_candidate = _result_integrity(
        root=base,
        run_root=run_root,
        population_ids=set(candidate_map),
        label_lookup=label_lookup,
    )
    grouped_records = load_stage_results(run_root)
    record_by_label: dict[str, dict[str, Mapping[str, Any]]] = {}
    for candidate_id, records in grouped_records.items():
        mapping = {
            label_lookup[(str(row["analysisWindowStart"]), str(row["analysisWindowEnd"]))]: row
            for row in records
        }
        if set(mapping) != {"E", "F"}:
            raise TemporalDiscoveryContractError(f"aggregate lacks E/F: {candidate_id}")
        record_by_label[candidate_id] = mapping

    aggregates = []
    candidate_rows = []
    instances = []
    behaviors = {}
    structures = {}
    for candidate_id in sorted(candidate_map):
        candidate = candidate_map[candidate_id]
        records = record_by_label[candidate_id]
        ordered_records = [records["E"], records["F"]]
        aggregate = _aggregate_candidate(candidate, ordered_records)
        raw_rows = sorted(
            raw_by_candidate[candidate_id], key=lambda row: row["_stage5e3WindowLabel"]
        )
        behavior = _behavior_record(candidate_id, raw_rows, aggregate)
        candidate_instances = [
            _combine_instance(authored, raw_rows)
            for authored in _authored_instances(candidate)
        ]
        instances.extend(candidate_instances)
        management_activation_count = sum(
            sum(
                int(window.get("positionChangeCount") or 0)
                + int(window.get("automaticOrExplicitActivationCount") or 0)
                for window in row["windowStates"]
            )
            for row in candidate_instances
        )
        aggregate["managementActivationCount"] = management_activation_count
        aggregate["rejectedIntentCount"] = int(behavior["rejectedIntentCount"])
        aggregates.append(aggregate)
        structural = _structural_family(candidate)
        management = _management_family(candidate)
        structures[candidate_id] = structural["record"]
        behaviors[candidate_id] = behavior
        e_record = records["E"]
        f_record = records["F"]
        e_single = _aggregate_candidate(candidate, [e_record])
        f_single = _aggregate_candidate(candidate, [f_record])
        e_trades = int(e_record["trades"])
        f_trades = int(f_record["trades"])
        mutation_families = structural["record"]["mutationFamilies"]
        candidate_rows.append(
            {
                "candidateId": candidate_id,
                "sourceMode": candidate["sourceMode"],
                "seedId": candidate["seedId"],
                "mutationFamilies": mutation_families,
                "mutationFamilySignature": structural["record"]["mutationFamilySignature"],
                "structuralFamilySha256": structural["familySha256"],
                "managementFamilySha256": management["familySha256"],
                "managementFamily": management["shape"],
                "activityCategory": _activity_category(e_trades, f_trades),
                "windows": {
                    "E": {
                        "trades": e_trades,
                        "conservativeNetR": e_record["conservativeNetR"],
                        "noCostNetR": e_record["noCostNetR"],
                        "grossR": e_record["grossR"],
                        "maxDrawdownR": e_record["maxDrawdownR"],
                        "entryFrequencyPerThousand": e_single["entryFrequencyPerThousand"],
                        "exposureRatio": e_record["exposureRatio"],
                        "averageHoldingBars": e_single["averageHoldingBars"],
                    },
                    "F": {
                        "trades": f_trades,
                        "conservativeNetR": f_record["conservativeNetR"],
                        "noCostNetR": f_record["noCostNetR"],
                        "grossR": f_record["grossR"],
                        "maxDrawdownR": f_record["maxDrawdownR"],
                        "entryFrequencyPerThousand": f_single["entryFrequencyPerThousand"],
                        "exposureRatio": f_record["exposureRatio"],
                        "averageHoldingBars": f_single["averageHoldingBars"],
                    },
                },
                "totalTrades": aggregate["totalTrades"],
                "totalConservativeNetR": aggregate["totalConservativeNetR"],
                "totalNoCostNetR": aggregate["totalNoCostNetR"],
                "totalGrossR": e_record["grossR"] + f_record["grossR"],
                "worstWindowConservativeNetR": aggregate[
                    "worstWindowConservativeNetR"
                ],
                "maxWindowDrawdownR": aggregate["maxWindowDrawdownR"],
                "costDragR": aggregate["costDragR"],
                "costDragPerTrade": (
                    aggregate["costDragR"] / aggregate["totalTrades"]
                    if aggregate["totalTrades"]
                    else 0.0
                ),
                "managementActivationCount": management_activation_count,
                "rejectedIntentCount": int(behavior["rejectedIntentCount"]),
                "fingerprintDrift": fingerprint_distance(e_single, f_single),
                "entryFrequencyAbsoluteDelta": abs(
                    e_single["entryFrequencyPerThousand"]
                    - f_single["entryFrequencyPerThousand"]
                ),
                "exposureRatioAbsoluteDelta": abs(
                    float(e_record["exposureRatio"]) - float(f_record["exposureRatio"])
                ),
                "holdingDistance": _log_distance(
                    e_single["averageHoldingBars"], f_single["averageHoldingBars"]
                ),
                "routeUseDistance": _l1_distribution_distance(
                    _distribution(e_record["transitionCounts"]),
                    _distribution(f_record["transitionCounts"]),
                ),
            }
        )

    envelope = evaluate_policy_v2_envelope(
        population_candidates=candidates, screening_aggregates=aggregates
    )
    eligible_count = len(envelope["eligible"])
    required_eligible = int(SELECTOR_V2_PARAMETERS["minimumEligibleCandidates"])
    failed_checks = Counter()
    failure_signatures = Counter()
    for row in envelope["eligibility"]:
        failed = sorted(key for key, passed in row["checks"].items() if not passed)
        failed_checks.update(failed)
        failure_signatures["+".join(failed) or "eligible"] += 1
    if eligible_count >= required_eligible:
        selection = select_policy_v2(
            population_candidates=candidates, screening_aggregates=aggregates
        )
        selector_status = "robust_envelope_passed"
    else:
        selection = None
        selector_status = "robust_envelope_too_small_fail_closed"
    selector_artifact = {
        "schemaVersion": SELECTOR_ENVELOPE_SCHEMA,
        "selectorVersion": SELECTOR_V2_VERSION,
        "parameters": SELECTOR_V2_PARAMETERS,
        "screeningAggregateSetSha256": canonical_sha256(envelope["aggregates"]),
        "thresholds": envelope["thresholds"],
        "activePopulationCount": len(envelope["active"]),
        "eligibleCandidateCount": eligible_count,
        "minimumEligibleCandidates": required_eligible,
        "status": selector_status,
        "eligibility": envelope["eligibility"],
        "failedCheckCounts": dict(sorted(failed_checks.items())),
        "failureSignatureCounts": dict(sorted(failure_signatures.items())),
        "economicArchive": selection["economicArchive"] if selection else [],
        "admissibleNoveltyArchive": selection["admissibleNoveltyArchive"] if selection else [],
        "diagnosticPureNoveltyArchive": (
            selection["diagnosticPureNoveltyArchive"] if selection else []
        ),
        "selectedCandidateIds": selection["selectedCandidateIds"] if selection else [],
        "stratifiedControlCandidateIds": (
            selection["stratifiedControlCandidateIds"] if selection else []
        ),
        "confirmationCandidateIds": (
            selection["confirmationCandidateIds"] if selection else []
        ),
        "confirmationAuthorityFrozen": False,
        "confirmationTaskLaunchPermitted": False,
        "thresholdsRelaxed": False,
    }
    selector_artifact["selectorEnvelopeSha256"] = canonical_sha256(selector_artifact)

    instances.sort(key=lambda row: (row["candidateId"], row["managementType"], row["instanceId"]))
    activation_summary = _activation_summary(instances)
    activation_artifact = {
        "schemaVersion": ACTIVATION_SCHEMA,
        "populationSha256": population_payload["populationSha256"],
        "resultCount": 256,
        **activation_summary,
        "instances": [
            {key: value for key, value in row.items() if not key.startswith("_")}
            for row in instances
        ],
    }
    activation_artifact["activationSha256"] = canonical_sha256(activation_artifact)

    diversity, _pair_distances = _diversity_report(
        label="stage5e3-screening", behaviors=behaviors, structures=structures
    )
    reference = next(
        row
        for row in diversity["thresholdSweep"]
        if row["threshold"] == diversity["referenceThreshold"]
    )
    behaviorally_collapsed = (
        float(reference["largestClusterShare"]) >= 0.75
        or _safe_float(diversity["behavioralDistance"]["median"]) <= 0.15
    )
    activity_counts = Counter(row["activityCategory"] for row in candidate_rows)
    e_r = [float(row["windows"]["E"]["conservativeNetR"]) for row in candidate_rows]
    f_r = [float(row["windows"]["F"]["conservativeNetR"]) for row in candidate_rows]
    sign_counts = Counter(
        f"{('positive' if e > 0 else 'negative' if e < 0 else 'zero')}_"
        f"{('positive' if f > 0 else 'negative' if f < 0 else 'zero')}"
        for e, f in zip(e_r, f_r)
    )
    active_both_rows = [row for row in candidate_rows if row["activityCategory"] == "active_both"]
    economics = {
        "candidateCount": len(candidate_rows),
        "totalTrades": sum(int(row["totalTrades"]) for row in candidate_rows),
        "aggregateSums": {
            "grossR": math.fsum(float(row["totalGrossR"]) for row in candidate_rows),
            "noCostNetR": math.fsum(float(row["totalNoCostNetR"]) for row in candidate_rows),
            "conservativeNetR": math.fsum(
                float(row["totalConservativeNetR"]) for row in candidate_rows
            ),
            "costDragR": math.fsum(float(row["costDragR"]) for row in candidate_rows),
        },
        "candidateDistributions": {
            "totalTrades": _numeric_summary(row["totalTrades"] for row in candidate_rows),
            "grossR": _numeric_summary(row["totalGrossR"] for row in candidate_rows),
            "noCostNetR": _numeric_summary(row["totalNoCostNetR"] for row in candidate_rows),
            "conservativeNetR": _numeric_summary(
                row["totalConservativeNetR"] for row in candidate_rows
            ),
            "worstWindowConservativeNetR": _numeric_summary(
                row["worstWindowConservativeNetR"] for row in candidate_rows
            ),
            "costDragR": _numeric_summary(row["costDragR"] for row in candidate_rows),
            "costDragPerTrade": _numeric_summary(
                row["costDragPerTrade"]
                for row in candidate_rows
                if row["totalTrades"] > 0
            ),
        },
        "positiveNoCostCandidateCount": sum(row["totalNoCostNetR"] > 0 for row in candidate_rows),
        "positiveConservativeCandidateCount": sum(
            row["totalConservativeNetR"] > 0 for row in candidate_rows
        ),
        "costDominatedCandidateCount": sum(
            row["totalNoCostNetR"] > 0 and row["totalConservativeNetR"] <= 0
            for row in candidate_rows
        ),
    }
    stability = {
        "signPairCounts": dict(sorted(sign_counts.items())),
        "sameSignOrBothZeroCount": sum(
            count
            for key, count in sign_counts.items()
            if key in {"positive_positive", "negative_negative", "zero_zero"}
        ),
        "sameSignOrBothZeroRate": sum(
            count
            for key, count in sign_counts.items()
            if key in {"positive_positive", "negative_negative", "zero_zero"}
        )
        / len(candidate_rows),
        "eFRankCorrelationAll": _spearman(e_r, f_r),
        "eFRankCorrelationActiveBoth": _spearman(
            [float(row["windows"]["E"]["conservativeNetR"]) for row in active_both_rows],
            [float(row["windows"]["F"]["conservativeNetR"]) for row in active_both_rows],
        ),
        "worstWindowConservativeNetR": _numeric_summary(
            row["worstWindowConservativeNetR"] for row in candidate_rows
        ),
        "fingerprintDrift": _numeric_summary(row["fingerprintDrift"] for row in candidate_rows),
        "entryFrequencyAbsoluteDelta": _numeric_summary(
            row["entryFrequencyAbsoluteDelta"] for row in candidate_rows
        ),
        "exposureRatioAbsoluteDelta": _numeric_summary(
            row["exposureRatioAbsoluteDelta"] for row in candidate_rows
        ),
        "holdingDistance": _numeric_summary(row["holdingDistance"] for row in candidate_rows),
        "routeUseDistance": _numeric_summary(row["routeUseDistance"] for row in candidate_rows),
        "managementActivationStability": activation_summary["activationStability"],
        "behavioralDiversity": diversity,
        "behaviorallyCollapsed": behaviorally_collapsed,
        "behavioralCollapseRule": (
            "reference largest-cluster share >= 0.75 or median composite behavioral distance <= 0.15"
        ),
        "referenceLargestClusterShare": reference["largestClusterShare"],
    }
    severe_types = [
        row["managementType"]
        for row in activation_summary["managementTypeSummary"]
        if row["severeDormancy"]
    ]
    static_defect = any(
        candidate.get("managementReachability", {}).get("acceptable") is not True
        or candidate.get("managementReachability", {}).get("orphanManagementPlanIds")
        or any(
            action.get("reachableAfterEntry") is not True
            or action.get("staticallyDominated") is True
            for action in candidate.get("managementReachability", {}).get(
                "managementActions", []
            )
        )
        for candidate in candidates
    )
    failure_reasons = []
    if eligible_count < required_eligible:
        failure_reasons.append("fewer_than_32_robust_envelope_eligible_candidates")
    if behaviorally_collapsed:
        failure_reasons.append("behavioral_collapse")
    if severe_types:
        failure_reasons.append(
            "severe_management_dormancy:" + "+".join(sorted(severe_types))
        )
    if activation_summary["explicitTrailing"]["zeroActivationDespiteOpportunity"]:
        failure_reasons.append("explicit_trailing_zero_activation_despite_opportunity")
    if static_defect:
        failure_reasons.append("accepted_static_management_reachability_defect")
    classification = {
        "primaryOutcome": (
            "generator_real_market_validation_failed"
            if failure_reasons
            else "screening_policy_gate_passed_awaiting_review"
        ),
        "failureReasons": failure_reasons,
        "fewerThan32RobustEligible": eligible_count < required_eligible,
        "behaviorallyCollapsed": behaviorally_collapsed,
        "severeDormancyManagementTypes": severe_types,
        "explicitTrailingZeroActivationDespiteOpportunity": activation_summary[
            "explicitTrailing"
        ]["zeroActivationDespiteOpportunity"],
        "acceptedStaticReachabilityDefect": static_defect,
        "selectorEnrichmentTestable": selection is not None,
        "thresholdsRelaxed": False,
    }
    gateway = _final_gateway_snapshot(gateway_url)
    task_timings = [
        _safe_float(row.get("timing", {}).get("total_seconds"))
        for rows in raw_by_candidate.values()
        for row in rows
    ]
    candidate_artifact = {
        "schemaVersion": CANDIDATE_ANALYSIS_SCHEMA,
        "populationSha256": population_payload["populationSha256"],
        "candidateCount": len(candidate_rows),
        "candidates": candidate_rows,
    }
    candidate_artifact["candidateAnalysisSha256"] = canonical_sha256(candidate_artifact)
    aggregate_artifact = {
        "schemaVersion": AGGREGATE_SCHEMA,
        "populationSha256": population_payload["populationSha256"],
        "candidateCount": len(envelope["aggregates"]),
        "windowLabels": ["E", "F"],
        "aggregates": envelope["aggregates"],
    }
    aggregate_artifact["aggregateSetSha256"] = canonical_sha256(aggregate_artifact)
    report = {
        "schemaVersion": MIDPOINT_SCHEMA,
        "status": "mandatory_deep_review_g_and_h_blocked",
        "autoresearchAnalysisCommit": autoresearch_analysis_commit,
        "prelaunchCheckpointSha256": prelaunch["checkpointSha256"],
        "campaignSpecSha256": campaign["campaignSpecSha256"],
        "populationSha256": population_payload["populationSha256"],
        "integrity": integrity,
        "gatewayFinalSha256": gateway["gatewayFinalSha256"],
        "activity": {
            "candidateCount": len(candidate_rows),
            "activePopulationCount": sum(row["totalTrades"] > 0 for row in candidate_rows),
            "activityCounts": dict(sorted(activity_counts.items())),
            "bySourceMode": _candidate_group_summary(candidate_rows, "sourceMode"),
            "bySeed": _candidate_group_summary(candidate_rows, "seedId"),
            "byMutationFamily": _candidate_group_summary(
                candidate_rows, "mutationFamilies", explode=True
            ),
            "byStructuralFamily": _candidate_group_summary(
                candidate_rows, "structuralFamilySha256"
            ),
            "byManagementFamily": _candidate_group_summary(
                candidate_rows, "managementFamilySha256"
            ),
        },
        "activationSha256": activation_artifact["activationSha256"],
        "activationSummary": {
            key: activation_summary[key]
            for key in (
                "managementInstanceCount",
                "managementTypeSummary",
                "managementModeSummary",
                "activationStability",
                "deepestStateCounts",
                "rejectionReasonCounts",
                "explicitTrailing",
            )
        },
        "economics": economics,
        "stability": stability,
        "selectorGate": {
            key: selector_artifact[key]
            for key in (
                "selectorVersion",
                "thresholds",
                "activePopulationCount",
                "eligibleCandidateCount",
                "minimumEligibleCandidates",
                "status",
                "failedCheckCounts",
                "failureSignatureCounts",
                "confirmationAuthorityFrozen",
                "confirmationTaskLaunchPermitted",
                "thresholdsRelaxed",
            )
        },
        "classification": classification,
        "compute": {
            "taskTimingSeconds": _numeric_summary(task_timings),
            "workerPoolTaskCounts": integrity["workerPoolTaskCounts"],
            "onlineWorkerCountAtFinalEmpty": gateway["onlineWorkerCount"],
            "workerPoolCountsAtFinalEmpty": gateway["workerPoolCounts"],
        },
        "predeclaredConfirmationTest": campaign["predeclaredSelectorEnrichmentCriteria"],
        "confirmationState": {
            "authorityFrozen": False,
            "taskCount": 0,
            "resultCount": 0,
            "selectedCandidateCount": 0 if selection is None else len(selection["selectedCandidateIds"]),
            "controlCandidateCount": 0 if selection is None else len(selection["stratifiedControlCandidateIds"]),
            "launchPermitted": False,
        },
        "reservedEvidenceAccessed": False,
        "largeSearchPermitted": False,
        "candidatePromotionPermitted": False,
        "nextPermittedOperation": "deep user and architecture review only",
    }
    report["midpointSha256"] = canonical_sha256(report)

    _write_immutable(output / "screening-aggregates.json", aggregate_artifact)
    _write_immutable(output / "candidate-analysis.json", candidate_artifact)
    _write_immutable(output / "selector-envelope.json", selector_artifact)
    _write_immutable(output / "activation.json", activation_artifact)
    _write_immutable(output / "gateway-final.json", gateway)
    _write_immutable(output / "midpoint.json", report)
    _write_text_immutable(output / "midpoint.md", _markdown(report))
    files = []
    for path in sorted(item for item in output.rglob("*") if item.is_file()):
        if path.name == "manifest.json" and path.parent == output:
            continue
        files.append(
            {
                "relativePath": path.relative_to(output).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
        )
    manifest = {
        "schemaVersion": MIDPOINT_MANIFEST_SCHEMA,
        "midpointSha256": report["midpointSha256"],
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(output / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_search_stage5e3_midpoint_result_v1",
        "status": report["status"],
        "primaryOutcome": classification["primaryOutcome"],
        "midpointSha256": report["midpointSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "eligibleCandidateCount": eligible_count,
        "minimumEligibleCandidates": required_eligible,
        "screeningTaskCount": 256,
        "confirmationTaskCount": 0,
    }


def audit_stage5e3_midpoint(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root).resolve()
    report = _read(root / "midpoint.json", name="midpoint")
    supplied_report = str(report.pop("midpointSha256", ""))
    if canonical_sha256(report) != supplied_report:
        raise TemporalDiscoveryContractError("midpoint identity mismatch")
    manifest = _read(root / "manifest.json", name="midpoint manifest")
    supplied_manifest = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError("midpoint manifest identity mismatch")
    if manifest.get("midpointSha256") != supplied_report:
        raise TemporalDiscoveryContractError("midpoint/manifest identity mismatch")
    expected = set()
    for row in manifest.get("files") or []:
        path = root / str(row["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(row["length"])
            or _file_sha256(path) != row["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"midpoint file drift: {path}")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and not (path.name == "manifest.json" and path.parent == root)
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("midpoint file inventory drift")
    return {
        "schemaVersion": "temporal_search_stage5e3_midpoint_audit_v1",
        "ok": True,
        "status": report["status"],
        "primaryOutcome": report["classification"]["primaryOutcome"],
        "midpointSha256": supplied_report,
        "manifestSha256": supplied_manifest,
        "fileCount": manifest["fileCount"],
    }


__all__ = [
    "audit_stage5e3_midpoint",
    "freeze_stage5e3_midpoint",
]
