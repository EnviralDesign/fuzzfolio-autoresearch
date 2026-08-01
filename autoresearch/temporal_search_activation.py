"""Deterministic Stage 5E-2 management-activation causality.

The analysis is deliberately retrospective and read-only.  It consumes the
exact Stage 5E-0/5E-1 development artifacts and never contacts the Gateway,
market-data services, or a worker.  Its purpose is to explain how far each
authored break-even or trailing capability progressed, not to rescore or tune
the historical candidates.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from .temporal_search import canonical_sha256


ACTIVATION_REPORT_SCHEMA = "temporal_search_activation_causality_v1"
ACTIVATION_MANIFEST_SCHEMA = "temporal_search_activation_manifest_v1"
ACTIVATION_DOSSIER_SCHEMA = "temporal_search_activation_dossier_v1"

DEPTH_ORDER = (
    "source_state_never_occupied",
    "transition_never_evaluated",
    "guard_evaluated_but_never_true",
    "intent_never_scheduled",
    "intent_scheduled_but_rejected",
    "intent_accepted_no_position_change",
    "activated_successfully",
    "activated_and_changed_trade_closure",
)
DEPTH_RANK = {name: index for index, name in enumerate(DEPTH_ORDER)}


class TemporalSearchActivationError(RuntimeError):
    pass


def _clone(value: Any) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise TemporalSearchActivationError("value is not finite canonical JSON") from exc


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchActivationError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchActivationError(f"{name} root must be an object")
    return _clone(value)


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encoded(value)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchActivationError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(encoded, encoding="utf-8")


def _write_text_immutable(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != value:
        raise TemporalSearchActivationError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(value, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _refresh_manifest(root: Path, *, report_sha256: str) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    manifest = {
        "schemaVersion": ACTIVATION_MANIFEST_SCHEMA,
        "reportSha256": report_sha256,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    (root / "manifest.json").write_text(_encoded(manifest), encoding="utf-8")
    return manifest


def _safe_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _window_id(payload: Mapping[str, Any]) -> str:
    return (
        f"{payload.get('analysis_window_start')}/"
        f"{payload.get('analysis_window_end')}"
    )


def _load_result_stage(root: Path, *, stage: str) -> list[dict[str, Any]]:
    files = sorted((root / "results").glob("*.json"))
    if not files:
        raise TemporalSearchActivationError(f"no {stage} results under {root}")
    output = []
    seen: set[tuple[str, str]] = set()
    for path in files:
        payload = _read(path, name=f"{stage} result")
        if payload.get("schema_version") != "temporal_graph_candidate_window_result_v1":
            raise TemporalSearchActivationError(f"unexpected result schema: {path}")
        key = (str(payload.get("candidate_id") or ""), _window_id(payload))
        if key in seen:
            raise TemporalSearchActivationError(f"duplicate {stage} result {key!r}")
        seen.add(key)
        output.append({"stage": stage, "path": str(path), "payload": payload})
    return output


def _result_set_sha(rows: Sequence[Mapping[str, Any]]) -> str:
    material = []
    for row in sorted(
        rows,
        key=lambda item: (
            str(item["payload"].get("candidate_id") or ""),
            _window_id(item["payload"]),
        ),
    ):
        payload = row["payload"]
        material.append(
            {
                "candidateId": payload.get("candidate_id"),
                "windowId": _window_id(payload),
                "artifactSha256": payload.get("artifact_sha256"),
                "programSha256": payload.get("program_sha256"),
                "observationStreamSha256": payload.get("observation_stream_sha256"),
                "workerContractSha256": (
                    (payload.get("worker_attribution") or {}).get(
                        "worker_contract_hash"
                    )
                ),
            }
        )
    return canonical_sha256(material)


def _walk_guard(guard: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield guard
    child = guard.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard(child)
    for item in guard.get("guards") or []:
        if isinstance(item, Mapping):
            yield from _walk_guard(item)


def _guard_requirements(guard: Mapping[str, Any]) -> dict[str, Any]:
    unrealized: list[float] = []
    ages: list[int] = []
    for node in _walk_guard(guard):
        kind = node.get("kind")
        if kind == "unrealized_r_at_least":
            unrealized.append(_safe_float(node.get("value")))
        elif kind == "position_age_at_least":
            ages.append(int(node.get("events") or 0))
    return {
        "requiredUnrealizedR": max(unrealized) if unrealized else None,
        "requiredPositionAgeBars": max(ages) if ages else None,
    }


def _guard_statically_true(guard: Mapping[str, Any]) -> bool:
    kind = guard.get("kind")
    if kind == "state_age_at_least" and int(guard.get("events") or 0) <= 0:
        return True
    if kind == "all":
        children = [item for item in guard.get("guards") or [] if isinstance(item, Mapping)]
        return bool(children) and all(_guard_statically_true(item) for item in children)
    if kind == "any":
        return any(
            _guard_statically_true(item)
            for item in guard.get("guards") or []
            if isinstance(item, Mapping)
        )
    return False


def _reachable_states(profile: Mapping[str, Any]) -> set[str]:
    graph = profile.get("graph") or {}
    reached = {str(graph.get("initialStateId") or "")}
    changed = True
    while changed:
        changed = False
        for transition in graph.get("transitions") or []:
            source = str(transition.get("sourceStateId") or "")
            destination = str(transition.get("destinationStateId") or "")
            if source in reached and destination not in reached:
                reached.add(destination)
                changed = True
    return reached


def _transition_statically_dominated(
    profile: Mapping[str, Any], transition: Mapping[str, Any]
) -> bool:
    peers = sorted(
        (
            item
            for item in (profile.get("graph") or {}).get("transitions") or []
            if item.get("sourceStateId") == transition.get("sourceStateId")
            and item.get("eventClass") == transition.get("eventClass")
        ),
        key=lambda item: (int(item.get("priority") or 0), str(item.get("id") or "")),
    )
    for peer in peers:
        if peer.get("id") == transition.get("id"):
            return False
        guard = peer.get("guard")
        if isinstance(guard, Mapping) and _guard_statically_true(guard):
            return True
    return False


def _mutation_signature(candidate: Mapping[str, Any]) -> str:
    families = sorted(
        {
            str(item.get("family") or "")
            for item in candidate.get("mutationTrace") or []
            if str(item.get("family") or "")
        }
    )
    return "+".join(families) or "none"


def _entry_routes(profile: Mapping[str, Any], *, plan_id: str | None = None) -> list[str]:
    library = ((profile.get("executionConfig") or {}).get("managementLibrary") or {})
    default = library.get("defaultPlanId")
    output = []
    for transition in (profile.get("graph") or {}).get("transitions") or []:
        for action in transition.get("actions") or []:
            if action.get("kind") != "enter_next_open":
                continue
            assigned = action.get("managementPlanId") or default
            if plan_id is None or str(assigned or "") == str(plan_id):
                output.append(str(transition.get("id") or ""))
    return sorted(set(output))


def _base_instance(
    candidate: Mapping[str, Any],
    *,
    management_type: str,
    transition: Mapping[str, Any] | None = None,
    plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    profile = candidate["sourceProfile"]
    transition_id = str((transition or {}).get("id") or "") or None
    plan_id = str((plan or {}).get("id") or "") or None
    identity = {
        "candidateId": candidate["candidateId"],
        "managementType": management_type,
        "transitionId": transition_id,
        "planId": plan_id,
    }
    return {
        "instanceId": canonical_sha256(identity),
        **identity,
        "sourceMode": candidate.get("sourceMode"),
        "seedId": candidate.get("seedId"),
        "mutationFamilySignature": _mutation_signature(candidate),
        "sourceStateId": (transition or {}).get("sourceStateId"),
        "entryRoutes": _entry_routes(profile, plan_id=plan_id),
        "profileReachableStates": sorted(_reachable_states(profile)),
    }


def _compact_spec(value: Mapping[str, Any] | None) -> str | None:
    if not isinstance(value, Mapping):
        return None
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _authored_instances(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    profile = candidate["sourceProfile"]
    output: list[dict[str, Any]] = []
    for transition in (profile.get("graph") or {}).get("transitions") or []:
        for action in transition.get("actions") or []:
            if action.get("kind") != "move_stop_to_break_even_next_open":
                continue
            requirements = _guard_requirements(transition.get("guard") or {})
            output.append(
                {
                    **_base_instance(
                        candidate,
                        management_type="break_even",
                        transition=transition,
                    ),
                    "activationMode": "graph_action",
                    "activationThreshold": requirements.get("requiredUnrealizedR"),
                    "requiredPositionAgeBars": requirements.get(
                        "requiredPositionAgeBars"
                    ),
                    "anchorKind": "entry_price",
                    "anchorSpec": '{"kind":"entry_price"}',
                    "distanceKind": "break_even",
                    "distanceSpec": '{"kind":"break_even"}',
                    "transition": _clone(transition),
                    "staticallyDominated": _transition_statically_dominated(
                        profile, transition
                    ),
                    "staticallyReachable": str(transition.get("sourceStateId") or "")
                    in _reachable_states(profile),
                }
            )
    library = ((profile.get("executionConfig") or {}).get("managementLibrary") or {})
    for plan in library.get("plans") or []:
        if not isinstance(plan, Mapping) or not isinstance(plan.get("trailingStop"), Mapping):
            continue
        trailing = plan["trailingStop"]
        activation = trailing.get("activation") or {}
        activation_kind = str(activation.get("kind") or "unknown")
        threshold: Any = None
        age: Any = None
        if activation_kind in {"after_unrealized_r", "after_r_and_age"}:
            threshold = activation.get("value")
        if activation_kind in {"after_position_age", "after_r_and_age"}:
            age = activation.get("bars")
        explicit_actions = []
        for transition in (profile.get("graph") or {}).get("transitions") or []:
            if any(
                action.get("kind") == "activate_trailing_stop_next_open"
                for action in transition.get("actions") or []
            ):
                explicit_actions.append(str(transition.get("id") or ""))
        output.append(
            {
                **_base_instance(
                    candidate,
                    management_type="trailing_stop",
                    plan=plan,
                ),
                "activationMode": activation_kind,
                "activationThreshold": threshold,
                "requiredPositionAgeBars": age,
                "anchorKind": (trailing.get("anchor") or {}).get("kind"),
                "anchorSpec": _compact_spec(trailing.get("anchor")),
                "distanceKind": (trailing.get("distance") or {}).get("kind"),
                "distanceSpec": _compact_spec(trailing.get("distance")),
                "minimumStepInitialR": trailing.get("minimumStepInitialR"),
                "plan": _clone(plan),
                "explicitActivationTransitionIds": sorted(explicit_actions),
                "staticallyDominated": False,
                "staticallyReachable": bool(_entry_routes(profile, plan_id=str(plan.get("id")))),
            }
        )
    return output


def _replay(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        value = payload["cost_view_results"]["research_conservative"]["replay_result"]
    except (KeyError, TypeError) as exc:
        raise TemporalSearchActivationError("result lacks conservative replay") from exc
    if not isinstance(value, Mapping):
        raise TemporalSearchActivationError("conservative replay must be an object")
    return dict(value)


def _intent_transitions(replay: Mapping[str, Any]) -> dict[str, str]:
    output = {}
    for trace in replay.get("graphTraces") or []:
        transition_id = str(trace.get("transitionId") or "")
        for intent_id in trace.get("intentIds") or []:
            output[str(intent_id)] = transition_id
    return output


def _position_plan_map(replay: Mapping[str, Any]) -> dict[str, str]:
    output = {}
    for trade in replay.get("trades") or []:
        if trade.get("positionId") and trade.get("managementPlanId"):
            output[str(trade["positionId"])] = str(trade["managementPlanId"])
    final_position = (replay.get("finalExecutionState") or {}).get("position")
    if isinstance(final_position, Mapping) and final_position.get("positionId"):
        output[str(final_position["positionId"])] = str(
            final_position.get("managementPlanId") or ""
        )
    return output


def _max_trade(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = [_safe_float(row.get(key)) for row in rows]
    return max(values) if values else None


def _depth_max(values: Iterable[str]) -> str:
    return max(values, key=lambda value: DEPTH_RANK[value])


def _break_even_window(
    instance: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    replay = _replay(payload)
    metrics = replay.get("metrics") or {}
    transition_id = str(instance.get("transitionId") or "")
    source_state = str(instance.get("sourceStateId") or "")
    transition_traces = [
        trace
        for trace in replay.get("graphTraces") or []
        if str(trace.get("transitionId") or "") == transition_id
    ]
    intent_map = _intent_transitions(replay)
    execution = [
        trace
        for trace in replay.get("executionTraces") or []
        if trace.get("actionKind") == "move_stop_to_break_even_next_open"
        and intent_map.get(str(trace.get("intentId") or "")) == transition_id
    ]
    statuses = Counter(str(row.get("status") or "unknown") for row in execution)
    reasons = Counter(
        str(row.get("reasonCode") or "unknown")
        for row in execution
        if row.get("status") == "rejected"
    )
    trades = list(replay.get("trades") or [])
    applied_position_ids = {
        str(row.get("positionId"))
        for row in execution
        if row.get("status") == "applied" and row.get("positionId")
    }
    be_trades = [
        row
        for row in trades
        if row.get("breakEvenApplied") is True
        and str(row.get("positionId") or "") in applied_position_ids
    ]
    changed_closure = [
        row
        for row in be_trades
        if row.get("closeReason") in {"break_even_stop", "break_even_gap"}
    ]
    occupancy = int((metrics.get("stateOccupancy") or {}).get(source_state) or 0)
    if changed_closure:
        deepest = "activated_and_changed_trade_closure"
    elif statuses.get("applied") or be_trades:
        deepest = "activated_successfully"
    elif statuses.get("filled") or statuses.get("closed"):
        deepest = "intent_accepted_no_position_change"
    elif statuses.get("rejected"):
        deepest = "intent_scheduled_but_rejected"
    elif statuses.get("scheduled"):
        deepest = "intent_accepted_no_position_change"
    elif transition_traces:
        deepest = "intent_never_scheduled"
    elif occupancy <= 0:
        deepest = "source_state_never_occupied"
    elif instance.get("staticallyDominated"):
        deepest = "transition_never_evaluated"
    else:
        deepest = "guard_evaluated_but_never_true"
    required_r = instance.get("activationThreshold")
    required_age = instance.get("requiredPositionAgeBars")
    max_mfe = _max_trade(trades, "maxFavorableExcursionR")
    max_age = _max_trade(trades, "holdingBars")
    return {
        "windowId": _window_id(payload),
        "stage": payload.get("_causalityStage"),
        "deepestReachedState": deepest,
        "sourceStateOccupancy": occupancy,
        "transitionSelectedCount": len(transition_traces),
        "intentScheduledCount": int(statuses.get("scheduled") or 0),
        "intentRejectedCount": int(statuses.get("rejected") or 0),
        "positionChangeCount": int(statuses.get("applied") or 0),
        "positionEligibleCount": int(metrics.get("positionsOpened") or 0),
        "breakEvenTradeCount": len(be_trades),
        "changedClosureCount": len(changed_closure),
        "rejectionReasons": dict(sorted(reasons.items())),
        "maximumFavorableExcursionR": max_mfe,
        "requiredUnrealizedR": required_r,
        "unrealizedThresholdPossible": (
            None
            if required_r is None or max_mfe is None
            else max_mfe >= _safe_float(required_r)
        ),
        "maximumHoldingBars": max_age,
        "requiredPositionAgeBars": required_age,
        "positionAgePossible": (
            None
            if required_age is None or max_age is None
            else max_age >= _safe_float(required_age)
        ),
        "earlierCloseReasonCounts": dict(
            sorted(Counter(str(row.get("closeReason") or "unknown") for row in trades).items())
        ),
        "_dossier": {
            "graphTraces": _clone(transition_traces[:8]),
            "executionTraces": _clone(execution[:12]),
            "trades": _clone((changed_closure or be_trades or trades)[:6]),
        },
    }


def _trailing_window(
    instance: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    replay = _replay(payload)
    plan_id = str(instance.get("planId") or "")
    position_plans = _position_plan_map(replay)
    trades = [
        row
        for row in replay.get("trades") or []
        if str(row.get("managementPlanId") or "") == plan_id
    ]
    position_ids = {
        str(row.get("positionId") or "") for row in trades if row.get("positionId")
    }
    position_ids.update(
        position_id
        for position_id, assigned in position_plans.items()
        if assigned == plan_id
    )
    traces = [
        row
        for row in replay.get("executionTraces") or []
        if row.get("actionKind")
        in {
            "trailing_activation",
            "trailing_stop_schedule",
            "trailing_stop_update",
            "activate_trailing_stop_next_open",
        }
        and (not row.get("positionId") or str(row.get("positionId")) in position_ids)
    ]
    statuses = Counter(str(row.get("status") or "unknown") for row in traces)
    reasons = Counter(
        str(row.get("reasonCode") or "unknown")
        for row in traces
        if row.get("status") == "rejected"
    )
    schedules = sum(row.get("actionKind") == "trailing_stop_schedule" and row.get("status") == "scheduled" for row in traces)
    updates = sum(row.get("actionKind") == "trailing_stop_update" and row.get("status") == "applied" for row in traces)
    activations = sum(row.get("actionKind") in {"trailing_activation", "activate_trailing_stop_next_open"} and row.get("status") == "applied" for row in traces)
    rejected = sum(row.get("status") == "rejected" for row in traces)
    trailing_closes = [
        row
        for row in trades
        if row.get("closeReason") in {"trailing_stop", "trailing_stop_gap"}
    ]
    activation_mode = str(instance.get("activationMode") or "")
    immediate_entry_activation = len(position_ids) if activation_mode == "immediate" else 0
    max_mfe = _max_trade(trades, "maxFavorableExcursionR")
    max_age = _max_trade(trades, "holdingBars")
    required_r = instance.get("activationThreshold")
    required_age = instance.get("requiredPositionAgeBars")
    if trailing_closes:
        deepest = "activated_and_changed_trade_closure"
    elif updates or schedules or activations or immediate_entry_activation:
        deepest = "activated_successfully"
    elif rejected:
        deepest = "intent_scheduled_but_rejected"
    elif not position_ids:
        deepest = "source_state_never_occupied"
    elif activation_mode == "explicit" and not instance.get(
        "explicitActivationTransitionIds"
    ):
        deepest = "intent_never_scheduled"
    elif (
        required_r is not None
        and max_mfe is not None
        and max_mfe < _safe_float(required_r)
    ) or (
        required_age is not None
        and max_age is not None
        and max_age < _safe_float(required_age)
    ):
        deepest = "guard_evaluated_but_never_true"
    else:
        deepest = "intent_never_scheduled"
    return {
        "windowId": _window_id(payload),
        "stage": payload.get("_causalityStage"),
        "deepestReachedState": deepest,
        "positionsUsingPlan": len(position_ids),
        "positionEligibleCount": len(position_ids),
        "tradeCount": len(trades),
        "automaticOrExplicitActivationCount": activations + immediate_entry_activation,
        "trailingScheduleCount": schedules,
        "trailingUpdateCount": updates,
        "rejectedEffectCount": rejected,
        "changedClosureCount": len(trailing_closes),
        "rejectionReasons": dict(sorted(reasons.items())),
        "maximumFavorableExcursionR": max_mfe,
        "requiredUnrealizedR": required_r,
        "unrealizedThresholdPossible": (
            None
            if required_r is None or max_mfe is None
            else max_mfe >= _safe_float(required_r)
        ),
        "maximumHoldingBars": max_age,
        "requiredPositionAgeBars": required_age,
        "positionAgePossible": (
            None
            if required_age is None or max_age is None
            else max_age >= _safe_float(required_age)
        ),
        "earlierCloseReasonCounts": dict(
            sorted(Counter(str(row.get("closeReason") or "unknown") for row in trades).items())
        ),
        "_dossier": {
            "executionTraces": _clone(traces[:16]),
            "trades": _clone((trailing_closes or trades)[:6]),
        },
    }


def _public_window(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _clone(item) for key, item in value.items() if not key.startswith("_")}


def _combine_instance(
    instance: Mapping[str, Any], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    windows = []
    for payload in results:
        windows.append(
            _break_even_window(instance, payload)
            if instance["managementType"] == "break_even"
            else _trailing_window(instance, payload)
        )
    deepest = _depth_max(row["deepestReachedState"] for row in windows)
    rejection_reasons: Counter[str] = Counter()
    close_reasons: Counter[str] = Counter()
    for row in windows:
        rejection_reasons.update(row.get("rejectionReasons") or {})
        close_reasons.update(row.get("earlierCloseReasonCounts") or {})
    public = {
        key: _clone(value)
        for key, value in instance.items()
        if key not in {"transition", "plan"}
    }
    public.update(
        {
            "deepestReachedState": deepest,
            "windowCount": len(windows),
            "windowStates": [_public_window(row) for row in windows],
            "rejectionReasonCounts": dict(sorted(rejection_reasons.items())),
            "closeReasonCounts": dict(sorted(close_reasons.items())),
            "activated": DEPTH_RANK[deepest]
            >= DEPTH_RANK["activated_successfully"],
            "changedTradeClosure": deepest
            == "activated_and_changed_trade_closure",
        }
    )
    public["instanceSha256"] = canonical_sha256(public)
    public["_dossier"] = {
        "transition": _clone(instance.get("transition")),
        "plan": _clone(instance.get("plan")),
        "windows": [
            {
                "windowId": row["windowId"],
                "stage": row.get("stage"),
                "deepestReachedState": row["deepestReachedState"],
                **_clone(row.get("_dossier") or {}),
            }
            for row in windows
        ],
    }
    return public


def _group_summaries(instances: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    dimensions = (
        "sourceMode",
        "seedId",
        "mutationFamilySignature",
        "managementType",
        "activationMode",
        "activationThreshold",
        "anchorKind",
        "anchorSpec",
        "distanceKind",
        "distanceSpec",
        "entryRoutes",
    )
    output = {}
    for dimension in dimensions:
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in instances:
            value = row.get(dimension)
            token = "+".join(value) if isinstance(value, list) else str(value)
            groups[token].append(row)
        output[dimension] = [
            {
                "value": key,
                "instanceCount": len(group),
                "activatedCount": sum(bool(row["activated"]) for row in group),
                "changedClosureCount": sum(
                    bool(row["changedTradeClosure"]) for row in group
                ),
                "deepestStateCounts": dict(
                    sorted(Counter(row["deepestReachedState"] for row in group).items())
                ),
                "rejectionReasonCounts": dict(
                    sorted(
                        sum(
                            (Counter(row["rejectionReasonCounts"]) for row in group),
                            Counter(),
                        ).items()
                    )
                ),
            }
            for key, group in sorted(groups.items())
        ]
    return output


def _dossier_category(instance: Mapping[str, Any]) -> list[str]:
    deepest = instance["deepestReachedState"]
    management_type = instance["managementType"]
    output = []
    if management_type == "break_even":
        if deepest in {"source_state_never_occupied", "transition_never_evaluated"}:
            output.append("unreachable_break_even")
        if deepest == "guard_evaluated_but_never_true":
            output.append("guard_false_break_even")
        if deepest == "intent_scheduled_but_rejected":
            output.append("rejected_break_even")
        if DEPTH_RANK[deepest] >= DEPTH_RANK["activated_successfully"]:
            output.append("successful_break_even")
    else:
        mode = instance.get("activationMode")
        dormant = DEPTH_RANK[deepest] < DEPTH_RANK["activated_successfully"]
        if dormant and mode == "immediate":
            output.append("dormant_immediate_trailing")
        if dormant and mode in {
            "after_unrealized_r",
            "after_position_age",
            "after_r_and_age",
        }:
            output.append("dormant_threshold_trailing")
        if dormant and mode == "explicit":
            output.append("dormant_explicit_trailing")
        if not dormant:
            output.append("successful_trailing")
    return output


def _freeze_dossiers(
    root: Path, instances: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], str]:
    requested = (
        "unreachable_break_even",
        "guard_false_break_even",
        "rejected_break_even",
        "successful_break_even",
        "dormant_immediate_trailing",
        "dormant_threshold_trailing",
        "dormant_explicit_trailing",
        "successful_trailing",
    )
    selected = []
    for category in requested:
        candidates = [row for row in instances if category in _dossier_category(row)]
        if not candidates:
            selected.append({"category": category, "available": False})
            continue
        row = sorted(candidates, key=lambda item: (item["instanceId"], item["candidateId"]))[0]
        dossier = {
            "schemaVersion": ACTIVATION_DOSSIER_SCHEMA,
            "category": category,
            "available": True,
            "instance": {
                key: _clone(value)
                for key, value in row.items()
                if not key.startswith("_")
            },
            "traceEvidence": _clone(row.get("_dossier") or {}),
        }
        dossier["dossierSha256"] = canonical_sha256(dossier)
        path = root / "activation-dossiers" / f"{category}.json"
        _write_immutable(path, dossier)
        selected.append(
            {
                "category": category,
                "available": True,
                "candidateId": row["candidateId"],
                "instanceId": row["instanceId"],
                "deepestReachedState": row["deepestReachedState"],
                "relativePath": path.relative_to(root).as_posix(),
                "dossierSha256": dossier["dossierSha256"],
            }
        )
    dossier_set_sha = canonical_sha256(selected)
    return selected, dossier_set_sha


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Stage 5E-2 management activation causality",
        "",
        "This is read-only diagnosis over the immutable 818 Stage 5E-0/5E-1 task results.",
        "",
        "## Deepest reached states",
        "",
    ]
    for row in report["deepestStateSummary"]:
        lines.append(f"- `{row['deepestReachedState']}`: {row['instanceCount']}")
    lines.extend(["", "## Management types", ""])
    for row in report["managementTypeSummary"]:
        lines.append(
            f"- `{row['managementType']}`: {row['instanceCount']} authored; "
            f"{row['activatedCount']} activated; {row['changedClosureCount']} changed closure."
        )
    lines.extend(["", "## Rejections", ""])
    if report["rejectionReasonCounts"]:
        for reason, count in report["rejectionReasonCounts"].items():
            lines.append(f"- `{reason}`: {count}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Representative dossiers", ""])
    for dossier in report["representativeDossiers"]:
        if dossier.get("available"):
            lines.append(
                f"- `{dossier['category']}`: `{dossier['candidateId']}` / "
                f"`{dossier['deepestReachedState']}`."
            )
        else:
            lines.append(f"- `{dossier['category']}`: no matching historical instance.")
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            report["interpretationBoundary"],
            "",
        ]
    )
    return "\n".join(lines)


def build_activation_causality(
    *,
    discovery_root: Path | str,
    initial_result_root: Path | str,
    confirmation_result_root: Path | str,
    control_result_root: Path | str,
    output_root: Path | str,
) -> dict[str, Any]:
    discovery = Path(discovery_root)
    output = Path(output_root)
    population_payload = _read(discovery / "population.json", name="population")
    candidates = list(population_payload.get("candidates") or [])
    if len(candidates) != 256:
        raise TemporalSearchActivationError("causal audit requires exact 256-candidate population")
    candidate_map = {str(row["candidateId"]): row for row in candidates}
    stages = {
        "screening": _load_result_stage(Path(initial_result_root), stage="screening"),
        "selected_confirmation": _load_result_stage(
            Path(confirmation_result_root), stage="selected_confirmation"
        ),
        "deterministic_control": _load_result_stage(
            Path(control_result_root), stage="deterministic_control"
        ),
    }
    expected_counts = {"screening": 512, "selected_confirmation": 178, "deterministic_control": 128}
    observed_counts = {key: len(value) for key, value in stages.items()}
    if observed_counts != expected_counts:
        raise TemporalSearchActivationError(
            f"causal audit requires exact 818 task results: {observed_counts!r}"
        )
    by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str]] = set()
    for stage, rows in stages.items():
        for row in rows:
            payload = _clone(row["payload"])
            candidate_id = str(payload.get("candidate_id") or "")
            if candidate_id not in candidate_map:
                raise TemporalSearchActivationError(
                    f"result candidate is outside population: {candidate_id}"
                )
            key = (candidate_id, _window_id(payload))
            if key in seen:
                raise TemporalSearchActivationError(f"duplicate cross-stage result {key!r}")
            seen.add(key)
            payload["_causalityStage"] = stage
            by_candidate[candidate_id].append(payload)
    if set(by_candidate) != set(candidate_map):
        raise TemporalSearchActivationError("causal results do not cover exact population")
    instances = []
    for candidate_id in sorted(candidate_map):
        for authored in _authored_instances(candidate_map[candidate_id]):
            instances.append(_combine_instance(authored, by_candidate[candidate_id]))
    if not instances:
        raise TemporalSearchActivationError("population authored no management capabilities")
    instances.sort(key=lambda row: (row["candidateId"], row["managementType"], row["instanceId"]))
    dossiers, dossier_set_sha = _freeze_dossiers(output, instances)
    rejection_reasons = sum(
        (Counter(row["rejectionReasonCounts"]) for row in instances), Counter()
    )
    management_summary = []
    for management_type in sorted({row["managementType"] for row in instances}):
        group = [row for row in instances if row["managementType"] == management_type]
        management_summary.append(
            {
                "managementType": management_type,
                "instanceCount": len(group),
                "activatedCount": sum(bool(row["activated"]) for row in group),
                "changedClosureCount": sum(bool(row["changedTradeClosure"]) for row in group),
            }
        )
    state_counts = Counter(row["deepestReachedState"] for row in instances)
    report = {
        "schemaVersion": ACTIVATION_REPORT_SCHEMA,
        "populationSha256": population_payload.get("populationSha256"),
        "candidateCount": len(candidate_map),
        "taskResultCount": sum(observed_counts.values()),
        "taskCountsByStage": observed_counts,
        "resultSetSha256ByStage": {
            key: _result_set_sha(value) for key, value in sorted(stages.items())
        },
        "managementInstanceCount": len(instances),
        "managementTypeSummary": management_summary,
        "deepestStateSummary": [
            {"deepestReachedState": name, "instanceCount": int(state_counts.get(name) or 0)}
            for name in DEPTH_ORDER
        ],
        "rejectionReasonCounts": dict(sorted(rejection_reasons.items())),
        "groupSummaries": _group_summaries(instances),
        "instances": [
            {key: _clone(value) for key, value in row.items() if not key.startswith("_")}
            for row in instances
        ],
        "representativeDossiers": dossiers,
        "dossierSetSha256": dossier_set_sha,
        "causalMethod": {
            "deepestStateOrder": list(DEPTH_ORDER),
            "transitionEvaluationInference": (
                "A transition is known selected from graph traces. If its source state was occupied "
                "and it was not selected, it is classified guard-false unless a higher-priority "
                "statically-always transition dominates it. No unrecorded guard truth is invented."
            ),
            "trailingEntryActivation": (
                "Immediate trailing is active atomically at entry under the admitted evaluator; "
                "threshold and explicit activation require matching immutable trace or feasibility evidence."
            ),
        },
        "interpretationBoundary": (
            "This report diagnoses immutable development evidence only. It does not alter Stage 5E-1, "
            "does not claim profitability, and cannot validate generator or selector v2 on windows A-D."
        ),
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(output / "activation-causality.json", report)
    _write_text_immutable(output / "activation-causality.md", _markdown(report))
    manifest = _refresh_manifest(output, report_sha256=report["reportSha256"])
    return {
        "schemaVersion": "temporal_search_activation_causality_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "dossierSetSha256": dossier_set_sha,
        "candidateCount": len(candidate_map),
        "taskResultCount": sum(observed_counts.values()),
        "managementInstanceCount": len(instances),
        "dossierCount": sum(bool(row.get("available")) for row in dossiers),
    }


def audit_activation_causality(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root)
    report = _read(root / "activation-causality.json", name="activation report")
    supplied_report = str(report.pop("reportSha256", ""))
    if canonical_sha256(report) != supplied_report:
        raise TemporalSearchActivationError("activation report identity mismatch")
    report["reportSha256"] = supplied_report
    manifest = _read(root / "manifest.json", name="activation manifest")
    supplied_manifest = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalSearchActivationError("activation manifest identity mismatch")
    if manifest.get("reportSha256") != supplied_report:
        raise TemporalSearchActivationError("activation manifest/report mismatch")
    expected_paths = set()
    for entry in manifest.get("files") or []:
        path = root / str(entry["relativePath"])
        expected_paths.add(path.resolve())
        if not path.is_file():
            raise TemporalSearchActivationError(f"manifest file missing: {path}")
        if path.stat().st_size != int(entry["length"]) or _file_sha(path) != entry["sha256"]:
            raise TemporalSearchActivationError(f"manifest file mismatch: {path}")
    actual_paths = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual_paths != expected_paths:
        raise TemporalSearchActivationError("activation artifact inventory drift")
    return {
        "schemaVersion": "temporal_search_activation_audit_v1",
        "ok": True,
        "reportSha256": supplied_report,
        "manifestSha256": supplied_manifest,
        "fileCount": manifest.get("fileCount"),
        "dossierSetSha256": report.get("dossierSetSha256"),
    }


__all__ = [
    "ACTIVATION_DOSSIER_SCHEMA",
    "ACTIVATION_MANIFEST_SCHEMA",
    "ACTIVATION_REPORT_SCHEMA",
    "DEPTH_ORDER",
    "TemporalSearchActivationError",
    "audit_activation_causality",
    "build_activation_causality",
]
