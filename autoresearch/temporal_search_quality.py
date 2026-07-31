from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any

from .temporal_discovery_artifacts import hashlib_sha256
from .temporal_discovery_controller import audit_discovery
from .temporal_discovery_results import (
    _aggregate_candidate,
    _equity_shape,
    _result_set_sha256,
    fingerprint_distance,
    load_stage_results,
    select_economic_archive,
    select_novelty_archive,
)
from .temporal_discovery_validation import (
    _finite_preparation,
    _normalize_preparation,
)
from .temporal_search import build_authority, canonical_sha256, validate_authority


SEARCH_QUALITY_BINDING_SCHEMA = "temporal_search_quality_binding_v1"
SEARCH_QUALITY_PHASE_A_SCHEMA = "temporal_search_quality_phase_a_v1"
SEARCH_QUALITY_CONTROL_SELECTION_SCHEMA = (
    "temporal_search_quality_control_selection_v1"
)
SEARCH_QUALITY_CONTROL_ALGORITHM = "stage5e1-control-v1"
SEARCH_QUALITY_FINAL_SCHEMA = "temporal_search_quality_final_report_v1"


class TemporalSearchQualityError(RuntimeError):
    pass


def _read_json(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchQualityError(f"unable to read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchQualityError(f"{name} must be a JSON object")
    return value


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
        raise TemporalSearchQualityError("value is not finite canonical JSON") from exc


def _encoded(value: Mapping[str, Any]) -> str:
    return (
        json.dumps(
            dict(value),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchQualityError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(encoded, encoding="utf-8")


def _write_text_immutable(path: Path, value: str) -> None:
    encoded = value.rstrip() + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchQualityError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(encoded, encoding="utf-8")


def _refresh_manifest(root: Path, *, binding_id: str) -> dict[str, Any]:
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        data = path.read_bytes()
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": len(data),
                "sha256": hashlib_sha256(data),
            }
        )
    manifest = {
        "schemaVersion": "temporal_search_quality_manifest_v1",
        "bindingId": binding_id,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    (root / "manifest.json").write_text(_encoded(manifest), encoding="utf-8")
    return manifest


def _file_sha256(path: Path) -> str:
    return hashlib_sha256(path.read_bytes())


def _safe_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _quantile(sorted_values: Sequence[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = max(0.0, min(1.0, fraction)) * (len(sorted_values) - 1)
    left = int(math.floor(position))
    right = min(len(sorted_values) - 1, left + 1)
    weight = position - left
    return float(sorted_values[left] * (1.0 - weight) + sorted_values[right] * weight)


def _numeric_summary(values: Iterable[Any]) -> dict[str, Any]:
    ordered = sorted(_safe_float(value) for value in values)
    if not ordered:
        return {
            "count": 0,
            "minimum": None,
            "p10": None,
            "p25": None,
            "median": None,
            "mean": None,
            "p75": None,
            "p90": None,
            "maximum": None,
        }
    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "p10": _quantile(ordered, 0.10),
        "p25": _quantile(ordered, 0.25),
        "median": _quantile(ordered, 0.50),
        "mean": math.fsum(ordered) / len(ordered),
        "p75": _quantile(ordered, 0.75),
        "p90": _quantile(ordered, 0.90),
        "maximum": ordered[-1],
    }


def _categorical(values: Iterable[Any]) -> list[dict[str, Any]]:
    counts = Counter(str(value) for value in values)
    total = sum(counts.values())
    return [
        {
            "value": key,
            "count": counts[key],
            "share": counts[key] / total if total else 0.0,
        }
        for key in sorted(counts)
    ]


def _yield_row(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(entry.get("disposition") or "") for entry in entries)
    total = len(entries)
    return {
        "proposalCount": total,
        "acceptedCount": counts["accepted"],
        "rejectedCount": counts["rejected"],
        "duplicateProgramCount": counts["duplicate_program"],
        "acceptedRate": counts["accepted"] / total if total else 0.0,
        "rejectedRate": counts["rejected"] / total if total else 0.0,
        "duplicateProgramRate": (
            counts["duplicate_program"] / total if total else 0.0
        ),
    }


def _group_yield(
    entries: Sequence[Mapping[str, Any]],
    key_function: Any,
) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for entry in entries:
        keys = key_function(entry)
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            groups[str(key)].append(entry)
    return [
        {"group": key, **_yield_row(groups[key])}
        for key in sorted(groups)
    ]


def _generation_yield(journal: Mapping[str, Any]) -> dict[str, Any]:
    entries = list(journal.get("entries") or [])
    return {
        "schemaVersion": "temporal_search_quality_generation_yield_v1",
        "journalSha256": journal.get("journalSha256"),
        "overall": _yield_row(entries),
        "bySourceMode": _group_yield(
            entries, lambda entry: str(entry.get("sourceMode") or "")
        ),
        "bySeedId": _group_yield(
            entries, lambda entry: str(entry.get("seedId") or "")
        ),
        "byMutationFamily": _group_yield(
            entries,
            lambda entry: sorted(
                str(value) for value in entry.get("mutationFamilies") or []
            )
            or ["none"],
        ),
        "byMutationCount": _group_yield(
            entries, lambda entry: str(int(entry.get("mutationCount") or 0))
        ),
        "proposalOrdinals": [
            {
                "proposalOrdinal": int(entry.get("proposalOrdinal") or 0),
                "candidateId": entry.get("candidateId"),
                "sourceMode": entry.get("sourceMode"),
                "seedId": entry.get("seedId"),
                "mutationFamilies": sorted(entry.get("mutationFamilies") or []),
                "mutationCount": int(entry.get("mutationCount") or 0),
                "disposition": entry.get("disposition"),
                "validationStatus": entry.get("validationStatus"),
                "issueCodes": sorted(entry.get("issueCodes") or []),
            }
            for entry in sorted(
                entries, key=lambda item: int(item.get("proposalOrdinal") or 0)
            )
        ],
    }


def _walk_guard(
    guard: Mapping[str, Any],
    *,
    depth: int = 1,
) -> tuple[list[str], int, list[str]]:
    kind = str(guard.get("kind") or "unknown")
    kinds = [kind]
    scalar_tokens = []
    for key in sorted(guard):
        if key in {"guard", "guards", "kind"}:
            continue
        value = guard[key]
        if isinstance(value, (str, int, float, bool)) or value is None:
            scalar_tokens.append(f"guard:{kind}:{key}={json.dumps(value, sort_keys=True)}")
        elif isinstance(value, list) and all(
            isinstance(item, (str, int, float, bool)) or item is None
            for item in value
        ):
            scalar_tokens.append(
                f"guard:{kind}:{key}={json.dumps(value, sort_keys=True)}"
            )
    maximum = depth
    child = guard.get("guard")
    if isinstance(child, Mapping):
        child_kinds, child_depth, child_tokens = _walk_guard(
            child, depth=depth + 1
        )
        kinds.extend(child_kinds)
        scalar_tokens.extend(child_tokens)
        maximum = max(maximum, child_depth)
    for item in guard.get("guards") or []:
        if not isinstance(item, Mapping):
            continue
        child_kinds, child_depth, child_tokens = _walk_guard(
            item, depth=depth + 1
        )
        kinds.extend(child_kinds)
        scalar_tokens.extend(child_tokens)
        maximum = max(maximum, child_depth)
    return kinds, maximum, scalar_tokens


def _management_rows(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    execution = profile.get("executionConfig") or {}
    library = execution.get("managementLibrary") or {}
    rows = []
    for plan in library.get("plans") or []:
        if not isinstance(plan, Mapping):
            continue
        trailing = plan.get("trailingStop")
        rows.append(
            {
                "planId": plan.get("id"),
                "initialStopKind": (plan.get("initialStop") or {}).get("kind"),
                "initialTargetKind": (plan.get("initialTarget") or {}).get("kind"),
                "trailingActivationKind": (
                    (trailing or {}).get("activation") or {}
                ).get("kind"),
                "trailingAnchorKind": ((trailing or {}).get("anchor") or {}).get(
                    "kind"
                ),
                "trailingDistanceKind": (
                    (trailing or {}).get("distance") or {}
                ).get("kind"),
            }
        )
    return rows


def _structural_record(candidate: Mapping[str, Any]) -> dict[str, Any]:
    profile = candidate["sourceProfile"]
    graph = profile.get("graph") or {}
    transitions = list(graph.get("transitions") or [])
    guard_kinds: list[str] = []
    guard_tokens: list[str] = []
    maximum_guard_depth = 0
    action_rows = []
    transition_tokens = []
    entry_transition_ids = []
    discretionary_transition_ids = []
    post_entry_action_kinds = []
    for transition in transitions:
        guard = transition.get("guard") or {}
        kinds, depth, scalars = _walk_guard(guard)
        guard_kinds.extend(kinds)
        guard_tokens.extend(scalars)
        maximum_guard_depth = max(maximum_guard_depth, depth)
        actions = list(transition.get("actions") or [])
        action_kinds = sorted(str(action.get("kind") or "") for action in actions)
        transition_tokens.append(
            "transition:"
            + "|".join(
                (
                    str(transition.get("eventClass") or ""),
                    str(transition.get("sourceStateId") or ""),
                    str(transition.get("destinationStateId") or ""),
                    "+".join(sorted(kinds)),
                    "+".join(action_kinds),
                )
            )
        )
        for action in actions:
            kind = str(action.get("kind") or "")
            row = {
                "transitionId": transition.get("id"),
                "sourceStateId": transition.get("sourceStateId"),
                "destinationStateId": transition.get("destinationStateId"),
                "actionKind": kind,
                "managementPlanId": action.get("managementPlanId"),
            }
            action_rows.append(row)
            if kind == "enter_next_open":
                entry_transition_ids.append(str(transition.get("id") or ""))
            elif kind == "exit_next_open":
                discretionary_transition_ids.append(
                    str(transition.get("id") or "")
                )
            elif kind:
                post_entry_action_kinds.append(kind)
    indicators = list(profile.get("indicators") or [])
    indicator_bindings = [
        {
            "indicatorId": (item.get("meta") or {}).get("id"),
            "instanceId": (item.get("meta") or {}).get("instanceId"),
            "signalRole": (item.get("meta") or {}).get("signalRole"),
            "timeframe": (item.get("config") or {}).get("timeframe"),
        }
        for item in indicators
    ]
    management = _management_rows(profile)
    mutation_families = sorted(
        {
            str(item.get("family") or "")
            for item in candidate.get("mutationTrace") or []
            if str(item.get("family") or "")
        }
    )
    tokens = {
        f"stateCount={len(graph.get('states') or [])}",
        f"transitionCount={len(transitions)}",
        f"guardDepth={maximum_guard_depth}",
        f"routeCount={len(entry_transition_ids)}",
    }
    tokens.update(f"guardKind:{kind}" for kind in guard_kinds)
    tokens.update(guard_tokens)
    tokens.update(transition_tokens)
    tokens.update(
        "indicator:"
        + "|".join(
            str(row.get(key) or "")
            for key in ("indicatorId", "timeframe", "signalRole")
        )
        for row in indicator_bindings
    )
    tokens.update(
        "management:"
        + "|".join(
            str(row.get(key) or "")
            for key in (
                "initialStopKind",
                "initialTargetKind",
                "trailingActivationKind",
                "trailingAnchorKind",
                "trailingDistanceKind",
            )
        )
        for row in management
    )
    tokens.update(f"action:{row['actionKind']}" for row in action_rows)
    return {
        "candidateId": candidate["candidateId"],
        "sourceMode": candidate["sourceMode"],
        "seedId": candidate["seedId"],
        "proposalOrdinal": candidate["proposalOrdinal"],
        "mutationCount": len(candidate.get("mutationTrace") or []),
        "mutationFamilies": mutation_families,
        "mutationFamilySignature": "+".join(mutation_families) or "none",
        "stateCount": len(graph.get("states") or []),
        "transitionCount": len(transitions),
        "guardKinds": sorted(guard_kinds),
        "guardKindCounts": dict(sorted(Counter(guard_kinds).items())),
        "maximumGuardDepth": maximum_guard_depth,
        "routeCount": len(entry_transition_ids),
        "entryTransitionIds": sorted(entry_transition_ids),
        "actionRows": action_rows,
        "entryActionCount": sum(
            row["actionKind"] == "enter_next_open" for row in action_rows
        ),
        "managementPlanAssignments": sorted(
            {
                str(row["managementPlanId"])
                for row in action_rows
                if row.get("managementPlanId") is not None
            }
        ),
        "managementPlans": management,
        "postEntryActionKinds": sorted(post_entry_action_kinds),
        "discretionaryExitTransitionIds": sorted(discretionary_transition_ids),
        "indicatorBindings": indicator_bindings,
        "indicatorTimeframes": sorted(
            {str(row.get("timeframe") or "") for row in indicator_bindings}
        ),
        "structuralTokens": sorted(tokens),
        "structuralSha256": canonical_sha256(sorted(tokens)),
    }


def _structural_coverage(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    records = [_structural_record(candidate) for candidate in candidates]
    by_id = {record["candidateId"]: record for record in records}
    management = [row for record in records for row in record["managementPlans"]]
    action_rows = [row for record in records for row in record["actionRows"]]
    report = {
        "schemaVersion": "temporal_search_quality_structural_coverage_v1",
        "candidateCount": len(records),
        "stateCount": _numeric_summary(row["stateCount"] for row in records),
        "transitionCount": _numeric_summary(
            row["transitionCount"] for row in records
        ),
        "maximumGuardDepth": _numeric_summary(
            row["maximumGuardDepth"] for row in records
        ),
        "routeCount": _numeric_summary(row["routeCount"] for row in records),
        "guardKinds": _categorical(
            kind for row in records for kind in row["guardKinds"]
        ),
        "indicatorBindings": _categorical(
            f"{item.get('indicatorId')}@{item.get('timeframe')}:{item.get('signalRole')}"
            for row in records
            for item in row["indicatorBindings"]
        ),
        "indicatorTimeframeCombinations": _categorical(
            "+".join(row["indicatorTimeframes"]) for row in records
        ),
        "entryActions": _categorical(
            row["actionKind"]
            for row in action_rows
            if row["actionKind"] == "enter_next_open"
        ),
        "managementPlanAssignments": _categorical(
            value
            for row in records
            for value in row["managementPlanAssignments"]
        ),
        "initialStopTypes": _categorical(
            row.get("initialStopKind") for row in management
        ),
        "initialTargetTypes": _categorical(
            row.get("initialTargetKind") for row in management
        ),
        "trailingActivationModes": _categorical(
            row.get("trailingActivationKind") for row in management
        ),
        "trailingTypes": _categorical(
            f"{row.get('trailingAnchorKind')}|{row.get('trailingDistanceKind')}"
            for row in management
        ),
        "postEntryActions": _categorical(
            kind for row in records for kind in row["postEntryActionKinds"]
        ),
        "discretionaryExitCount": _numeric_summary(
            len(row["discretionaryExitTransitionIds"]) for row in records
        ),
        "mutationFamilies": _categorical(
            family for row in records for family in row["mutationFamilies"]
        ),
        "combinationCount": len(
            {
                (
                    row["stateCount"],
                    row["transitionCount"],
                    tuple(row["guardKinds"]),
                    row["routeCount"],
                    tuple(row["indicatorTimeframes"]),
                    tuple(
                        (
                            plan.get("initialStopKind"),
                            plan.get("initialTargetKind"),
                            plan.get("trailingActivationKind"),
                            plan.get("trailingAnchorKind"),
                            plan.get("trailingDistanceKind"),
                        )
                        for plan in row["managementPlans"]
                    ),
                    tuple(row["postEntryActionKinds"]),
                )
                for row in records
            }
        ),
        "candidates": records,
    }
    return report, by_id


def _raw_stage_results(result_root: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str, str]] = set()
    files = sorted((result_root / "results").glob("*.json"))
    if not files:
        raise TemporalSearchQualityError(
            f"no materialized results found under {result_root}"
        )
    for path in files:
        payload = _read_json(path, name="candidate/window result")
        if payload.get("schema_version") != "temporal_graph_candidate_window_result_v1":
            raise TemporalSearchQualityError(f"unexpected result schema: {path}")
        key = (
            str(payload.get("candidate_id") or ""),
            str(payload.get("analysis_window_start") or ""),
            str(payload.get("analysis_window_end") or ""),
        )
        if key in seen:
            raise TemporalSearchQualityError(
                f"duplicate candidate/window result: {key!r}"
            )
        seen.add(key)
        grouped[key[0]].append(payload)
    for candidate_id in grouped:
        grouped[candidate_id].sort(
            key=lambda item: (
                str(item.get("analysis_window_start") or ""),
                str(item.get("analysis_window_end") or ""),
            )
        )
    return dict(grouped)


def _distribution(counts: Mapping[str, Any]) -> dict[str, float]:
    values = {
        str(key): max(0.0, _safe_float(value))
        for key, value in sorted(counts.items(), key=lambda item: str(item[0]))
    }
    total = math.fsum(values[key] for key in sorted(values))
    if total <= 0:
        return {}
    return {key: values[key] / total for key in sorted(values)}


def _l1_distribution(
    left: Mapping[str, float], right: Mapping[str, float]
) -> float:
    keys = sorted(set(left) | set(right))
    return 0.5 * math.fsum(
        abs(_safe_float(left.get(key)) - _safe_float(right.get(key)))
        for key in keys
    )


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 0.0
    return 1.0 - (len(left & right) / len(left | right))


def _log_distance(left: float, right: float) -> float:
    a = math.log1p(max(0.0, abs(left)))
    b = math.log1p(max(0.0, abs(right)))
    return min(1.0, abs(a - b) / (1.0 + max(abs(a), abs(b))))


def _drawdown_timing(curve: Sequence[Any]) -> float:
    values = [_safe_float(value) for value in curve]
    if not values:
        return 0.0
    peak = 0.0
    worst = 0.0
    worst_index = 0
    for index, value in enumerate(values):
        peak = max(peak, value)
        drawdown = peak - value
        if drawdown > worst:
            worst = drawdown
            worst_index = index
    return worst_index / max(1, len(values) - 1)


def _behavior_record(
    candidate_id: str,
    raw_results: Sequence[Mapping[str, Any]],
    aggregate: Mapping[str, Any],
) -> dict[str, Any]:
    action_status: Counter[tuple[str, str]] = Counter()
    transition_action_status: Counter[tuple[str, str, str]] = Counter()
    route_counts: Counter[str] = Counter()
    close_counts: Counter[str] = Counter()
    entry_tokens: set[str] = set()
    exposure_tokens: set[str] = set()
    holding_bars: list[float] = []
    equity_shapes: list[list[float]] = []
    drawdown_timings: list[float] = []
    total_positions = 0
    total_trades = 0
    for payload in raw_results:
        window_key = (
            f"{payload.get('analysis_window_start')}/"
            f"{payload.get('analysis_window_end')}"
        )
        replay = payload["cost_view_results"]["research_conservative"][
            "replay_result"
        ]
        metrics = replay.get("metrics") or {}
        total_positions += int(metrics.get("positionsOpened") or 0)
        intent_transition: dict[str, str] = {}
        for trace in replay.get("graphTraces") or []:
            transition_id = str(trace.get("transitionId") or "unknown")
            route_counts[transition_id] += 1
            for intent_id in trace.get("intentIds") or []:
                intent_transition[str(intent_id)] = transition_id
        for trace in replay.get("executionTraces") or []:
            kind = str(trace.get("actionKind") or "unknown")
            status = str(trace.get("status") or "unknown")
            action_status[(kind, status)] += 1
            intent_id = str(trace.get("intentId") or "")
            transition_id = intent_transition.get(intent_id)
            if transition_id is not None:
                transition_action_status[(transition_id, kind, status)] += 1
            if trace.get("reasonCode") == "entry_filled":
                entry_tokens.add(
                    f"{window_key}|{str(trace.get('marketBarId') or '')}"
                )
        curve = list(metrics.get("equityCurveR") or [])
        equity_shapes.append(_equity_shape(curve))
        drawdown_timings.append(_drawdown_timing(curve))
        for trade in replay.get("trades") or []:
            total_trades += 1
            close_counts[str(trade.get("closeReason") or "unknown")] += 1
            holding_bars.append(_safe_float(trade.get("holdingBars")))
            entry = int(trade.get("entryClockIndex") or 0)
            exit_index = int(trade.get("exitClockIndex") or entry)
            for clock_index in range(entry, max(entry, exit_index)):
                exposure_tokens.add(f"{window_key}|{clock_index}")
    shape = [
        math.fsum(row[index] for row in equity_shapes) / len(equity_shapes)
        for index in range(len(equity_shapes[0]))
    ] if equity_shapes else [0.0] * 12
    action_counts = Counter()
    for (kind, _status), count in action_status.items():
        action_counts[kind] += count
    terminal_accepted_statuses = {"filled", "applied", "closed"}
    scheduled = {
        kind: action_status[(kind, "scheduled")] for kind in sorted(action_counts)
    }
    accepted = {
        kind: sum(
            count
            for (candidate_kind, status), count in action_status.items()
            if candidate_kind == kind and status in terminal_accepted_statuses
        )
        for kind in sorted(action_counts)
    }
    rejected = {
        kind: action_status[(kind, "rejected")] for kind in sorted(action_counts)
    }
    return {
        "candidateId": candidate_id,
        "windowCount": len(raw_results),
        "entryCount": len(entry_tokens),
        "exposureObservationCount": len(exposure_tokens),
        "holdingBars": _numeric_summary(holding_bars),
        "closeReasonCounts": dict(sorted(close_counts.items())),
        "closeReasonDistribution": _distribution(close_counts),
        "routeCounts": dict(sorted(route_counts.items())),
        "routeDistribution": _distribution(route_counts),
        "actionDistribution": _distribution(action_counts),
        "scheduledActionCounts": scheduled,
        "acceptedActionCounts": accepted,
        "rejectedActionCounts": rejected,
        "rejectedIntentCount": sum(rejected.values()),
        "acceptedIntentOrEffectCount": sum(accepted.values()),
        "rejectedIntentRate": (
            sum(rejected.values())
            / max(1, sum(rejected.values()) + sum(accepted.values()))
        ),
        "positionsOpened": total_positions,
        "tradesClosed": total_trades,
        "equityShape": shape,
        "drawdownTiming": (
            math.fsum(drawdown_timings) / len(drawdown_timings)
            if drawdown_timings
            else 0.0
        ),
        "costSensitivityR": _safe_float(aggregate.get("costDragR")),
        "averageExposureRatio": _safe_float(
            aggregate.get("averageExposureRatio")
        ),
        "averageHoldingBars": _safe_float(aggregate.get("averageHoldingBars")),
        "totalTrades": int(aggregate.get("totalTrades") or 0),
        "totalConservativeNetR": _safe_float(
            aggregate.get("totalConservativeNetR")
        ),
        "maxWindowDrawdownR": _safe_float(
            aggregate.get("maxWindowDrawdownR")
        ),
        "_entryTokens": entry_tokens,
        "_exposureTokens": exposure_tokens,
        "_transitionActionStatus": transition_action_status,
    }


def _public_behavior(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _clone(value)
        for key, value in record.items()
        if not key.startswith("_")
    }


def _behavior_distance(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> tuple[float, dict[str, float]]:
    left_shape = left["equityShape"]
    right_shape = right["equityShape"]
    components = {
        "entryOverlapDistance": _jaccard(
            left["_entryTokens"], right["_entryTokens"]
        ),
        "exposureOverlapDistance": _jaccard(
            left["_exposureTokens"], right["_exposureTokens"]
        ),
        "holdingDistance": _log_distance(
            _safe_float(left["averageHoldingBars"]),
            _safe_float(right["averageHoldingBars"]),
        ),
        "closeReasonDistance": _l1_distribution(
            left["closeReasonDistribution"], right["closeReasonDistribution"]
        ),
        "routeUseDistance": _l1_distribution(
            left["routeDistribution"], right["routeDistribution"]
        ),
        "actionUseDistance": _l1_distribution(
            left["actionDistribution"], right["actionDistribution"]
        ),
        "equityShapeDistance": math.fsum(
            abs(_safe_float(a) - _safe_float(b))
            for a, b in zip(left_shape, right_shape)
        )
        / max(1, len(left_shape)),
        "drawdownTimingDistance": abs(
            _safe_float(left["drawdownTiming"])
            - _safe_float(right["drawdownTiming"])
        ),
        "costSensitivityDistance": _log_distance(
            _safe_float(left["costSensitivityR"]),
            _safe_float(right["costSensitivityR"]),
        ),
    }
    return math.fsum(components.values()) / len(components), components


def _structural_distance(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> float:
    left_tokens = set(left["structuralTokens"])
    right_tokens = set(right["structuralTokens"])
    return _jaccard(left_tokens, right_tokens)


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = math.fsum(left) / len(left)
    right_mean = math.fsum(right) / len(right)
    numerator = math.fsum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    left_scale = math.sqrt(math.fsum((value - left_mean) ** 2 for value in left))
    right_scale = math.sqrt(
        math.fsum((value - right_mean) ** 2 for value in right)
    )
    if left_scale <= 0 or right_scale <= 0:
        return None
    return numerator / (left_scale * right_scale)


def _components(
    candidate_ids: Sequence[str],
    pair_distances: Mapping[tuple[str, str], float],
    threshold: float,
) -> list[list[str]]:
    parent = {candidate_id: candidate_id for candidate_id in candidate_ids}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    for (left, right), distance in sorted(pair_distances.items()):
        if distance <= threshold:
            union(left, right)
    groups: dict[str, list[str]] = defaultdict(list)
    for candidate_id in sorted(candidate_ids):
        groups[find(candidate_id)].append(candidate_id)
    return sorted(groups.values(), key=lambda group: (-len(group), group[0]))


def _distance_group_summary(values: Sequence[float]) -> dict[str, Any]:
    return _numeric_summary(values)


def _diversity_report(
    *,
    label: str,
    behaviors: Mapping[str, Mapping[str, Any]],
    structures: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[tuple[str, str], float]]:
    candidate_ids = sorted(behaviors)
    pair_distances: dict[tuple[str, str], float] = {}
    structural_distances = []
    behavioral_distances = []
    within_seed = []
    between_seed = []
    within_family = []
    between_family = []
    component_values: dict[str, list[float]] = defaultdict(list)
    for left_index, left_id in enumerate(candidate_ids):
        for right_id in candidate_ids[left_index + 1 :]:
            distance, components = _behavior_distance(
                behaviors[left_id], behaviors[right_id]
            )
            structural = _structural_distance(
                structures[left_id], structures[right_id]
            )
            key = (left_id, right_id)
            pair_distances[key] = distance
            structural_distances.append(structural)
            behavioral_distances.append(distance)
            for component, value in components.items():
                component_values[component].append(value)
            if structures[left_id]["seedId"] == structures[right_id]["seedId"]:
                within_seed.append(distance)
            else:
                between_seed.append(distance)
            if (
                structures[left_id]["mutationFamilySignature"]
                == structures[right_id]["mutationFamilySignature"]
            ):
                within_family.append(distance)
            else:
                between_family.append(distance)
    thresholds = [index / 20.0 for index in range(1, 20)]
    sweep = []
    component_cache: dict[float, list[list[str]]] = {}
    for threshold in thresholds:
        groups = _components(candidate_ids, pair_distances, threshold)
        component_cache[threshold] = groups
        sweep.append(
            {
                "threshold": threshold,
                "effectiveClusterCount": len(groups),
                "largestClusterShare": (
                    len(groups[0]) / len(candidate_ids) if candidate_ids else 0.0
                ),
                "singletonCount": sum(len(group) == 1 for group in groups),
            }
        )
    target_cluster_count = max(1, round(math.sqrt(len(candidate_ids))))
    reference = min(
        sweep,
        key=lambda row: (
            abs(row["effectiveClusterCount"] - target_cluster_count),
            row["threshold"],
        ),
    )
    reference_groups = component_cache[reference["threshold"]]
    minimum_major_size = max(3, math.ceil(len(candidate_ids) * 0.05))
    major_families = []
    for index, group in enumerate(reference_groups):
        if len(group) < minimum_major_size:
            continue
        medoid = min(
            group,
            key=lambda candidate_id: (
                math.fsum(
                    pair_distances.get(tuple(sorted((candidate_id, other))), 0.0)
                    for other in group
                    if other != candidate_id
                )
                / max(1, len(group) - 1),
                candidate_id,
            ),
        )
        major_families.append(
            {
                "familyId": f"{label}-family-{index + 1:02d}",
                "candidateCount": len(group),
                "candidateIds": group,
                "medoidCandidateId": medoid,
            }
        )
    return (
        {
            "label": label,
            "candidateCount": len(candidate_ids),
            "pairCount": len(pair_distances),
            "behavioralDistance": _numeric_summary(behavioral_distances),
            "structuralDistance": _numeric_summary(structural_distances),
            "componentDistances": {
                key: _numeric_summary(values)
                for key, values in sorted(component_values.items())
            },
            "thresholdSweep": sweep,
            "referenceThresholdRule": (
                "threshold minimizing absolute distance from round(sqrt(n)) "
                "clusters; lower threshold wins ties"
            ),
            "referenceThreshold": reference["threshold"],
            "referenceClusterCount": reference["effectiveClusterCount"],
            "minimumMajorFamilySize": minimum_major_size,
            "majorBehavioralFamilies": major_families,
            "withinSeedDistance": _distance_group_summary(within_seed),
            "betweenSeedDistance": _distance_group_summary(between_seed),
            "withinMutationFamilyDistance": _distance_group_summary(
                within_family
            ),
            "betweenMutationFamilyDistance": _distance_group_summary(
                between_family
            ),
            "structuralBehavioralPearsonCorrelation": _pearson(
                structural_distances, behavioral_distances
            ),
        },
        pair_distances,
    )


def _authored_capabilities(
    candidate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    profile = candidate["sourceProfile"]
    capabilities = []

    def append_guards(
        guard: Mapping[str, Any],
        *,
        transition: Mapping[str, Any],
        path: str,
    ) -> None:
        capabilities.append(
            {
                "capability": (
                    f"guard:{transition.get('id')}:{path}:"
                    f"{guard.get('kind') or 'unknown'}"
                ),
                "capabilityType": f"guard:{guard.get('kind') or 'unknown'}",
                "transitionId": transition.get("id"),
                "sourceStateId": transition.get("sourceStateId"),
            }
        )
        child = guard.get("guard")
        if isinstance(child, Mapping):
            append_guards(child, transition=transition, path=path + "/guard")
        for index, item in enumerate(guard.get("guards") or []):
            if isinstance(item, Mapping):
                append_guards(
                    item,
                    transition=transition,
                    path=f"{path}/guards/{index}",
                )

    for transition in (profile.get("graph") or {}).get("transitions") or []:
        capabilities.append(
            {
                "capability": f"transition:{transition.get('id')}",
                "capabilityType": "transition_route",
                "transitionId": transition.get("id"),
                "sourceStateId": transition.get("sourceStateId"),
            }
        )
        guard = transition.get("guard")
        if isinstance(guard, Mapping):
            append_guards(guard, transition=transition, path="guard")
        for action in transition.get("actions") or []:
            capabilities.append(
                {
                    "capability": f"action:{action.get('kind')}",
                    "capabilityType": str(action.get("kind") or "unknown"),
                    "transitionId": transition.get("id"),
                    "sourceStateId": transition.get("sourceStateId"),
                }
            )
    for indicator in profile.get("indicators") or []:
        meta = indicator.get("meta") or {}
        config = indicator.get("config") or {}
        capabilities.append(
            {
                "capability": (
                    f"indicator:{meta.get('instanceId')}@{config.get('timeframe')}"
                ),
                "capabilityType": "indicator_binding",
                "transitionId": None,
                "sourceStateId": None,
            }
        )
    for plan in _management_rows(profile):
        capabilities.append(
            {
                "capability": f"management_plan:{plan.get('planId')}",
                "capabilityType": "management_plan_assignment",
                "transitionId": None,
                "sourceStateId": None,
            }
        )
        stop_kind = str(plan.get("initialStopKind") or "none")
        target_kind = str(plan.get("initialTargetKind") or "none")
        capabilities.append(
            {
                "capability": f"initial_stop:{stop_kind}",
                "capabilityType": "initial_stop",
                "transitionId": None,
                "sourceStateId": None,
            }
        )
        capabilities.append(
            {
                "capability": f"initial_target:{target_kind}",
                "capabilityType": "initial_target",
                "transitionId": None,
                "sourceStateId": None,
            }
        )
        if plan.get("trailingActivationKind"):
            capabilities.append(
                {
                    "capability": (
                        "trailing:"
                        f"{plan.get('trailingActivationKind')}|"
                        f"{plan.get('trailingAnchorKind')}|"
                        f"{plan.get('trailingDistanceKind')}"
                    ),
                    "capabilityType": "trailing_stop",
                    "transitionId": None,
                    "sourceStateId": None,
                }
            )
    unique = {
        (
            row["capability"],
            row.get("transitionId"),
            row.get("sourceStateId"),
        ): row
        for row in capabilities
    }
    return [unique[key] for key in sorted(unique)]


def _activation_for_capability(
    capability: Mapping[str, Any],
    behavior: Mapping[str, Any],
    aggregate: Mapping[str, Any],
) -> dict[str, Any]:
    kind = capability["capabilityType"]
    state_occupancy = aggregate.get("stateOccupancyDistribution") or {}
    evaluated = False
    accepted = 0
    rejected = 0
    changed = 0
    closed = 0
    activation_count = 0
    if kind in {
        "enter_next_open",
        "exit_next_open",
        "move_stop_to_break_even_next_open",
    }:
        source = str(capability.get("sourceStateId") or "")
        evaluated = _safe_float(state_occupancy.get(source)) > 0.0
        transition_id = str(capability.get("transitionId") or "")
        transition_status = behavior.get("_transitionActionStatus") or {}
        accepted = int(
            transition_status.get((transition_id, kind, "scheduled")) or 0
        )
        rejected = int(
            transition_status.get((transition_id, kind, "rejected")) or 0
        )
        changed = sum(
            int(transition_status.get((transition_id, kind, status)) or 0)
            for status in ("filled", "applied", "closed")
        )
        activation_count = accepted + rejected
        close_map = behavior.get("closeReasonCounts") or {}
        if kind == "exit_next_open":
            closed = int(
                transition_status.get((transition_id, kind, "closed")) or 0
            )
        elif kind == "move_stop_to_break_even_next_open":
            closed = int(close_map.get("break_even_stop") or 0)
    elif kind == "transition_route" or kind.startswith("guard:"):
        source = str(capability.get("sourceStateId") or "")
        evaluated = _safe_float(state_occupancy.get(source)) > 0.0
        transition_id = str(capability.get("transitionId") or "")
        activation_count = int(
            (behavior.get("routeCounts") or {}).get(transition_id) or 0
        )
    elif kind == "indicator_binding":
        evaluated = int(behavior.get("windowCount") or 0) > 0
        activation_count = int(behavior.get("windowCount") or 0)
    elif kind == "management_plan_assignment":
        evaluated = int(behavior.get("positionsOpened") or 0) > 0
        activation_count = int(behavior.get("positionsOpened") or 0)
    elif kind == "initial_stop":
        configured = not str(capability.get("capability") or "").endswith(":none")
        evaluated = configured and int(behavior.get("positionsOpened") or 0) > 0
        accepted = int(behavior.get("positionsOpened") or 0) if configured else 0
        changed = accepted
        activation_count = accepted
        closed = sum(
            int((behavior.get("closeReasonCounts") or {}).get(key) or 0)
            for key in ("stop_loss", "stop_gap")
        )
    elif kind == "initial_target":
        configured = not str(capability.get("capability") or "").endswith(":none")
        evaluated = configured and int(behavior.get("positionsOpened") or 0) > 0
        accepted = int(behavior.get("positionsOpened") or 0) if configured else 0
        changed = accepted
        activation_count = accepted
        closed = int(
            (behavior.get("closeReasonCounts") or {}).get("take_profit") or 0
        )
    elif kind == "trailing_stop":
        evaluated = int(behavior.get("positionsOpened") or 0) > 0
        accepted = int(
            (behavior.get("scheduledActionCounts") or {}).get(
                "trailing_stop_schedule"
            )
            or 0
        ) + int(
            (behavior.get("acceptedActionCounts") or {}).get(
                "trailing_activation"
            )
            or 0
        )
        rejected = math.fsum(
            int((behavior.get("rejectedActionCounts") or {}).get(key) or 0)
            for key in (
                "trailing_stop_schedule",
                "trailing_activation",
                "trailing_stop_update",
            )
        )
        changed = int(
            (behavior.get("acceptedActionCounts") or {}).get(
                "trailing_stop_update"
            )
            or 0
        ) + int(
            (behavior.get("acceptedActionCounts") or {}).get(
                "trailing_activation"
            )
            or 0
        )
        activation_count = int(accepted) + int(rejected)
        closed = sum(
            int((behavior.get("closeReasonCounts") or {}).get(key) or 0)
            for key in ("trailing_stop", "trailing_stop_gap")
        )
    return {
        **dict(capability),
        "presentInProfile": True,
        "actuallyEvaluated": bool(evaluated),
        "activationCount": int(activation_count),
        "acceptedIntentOrEffectCount": int(accepted),
        "rejectedIntentOrEffectCount": int(rejected),
        "positionChangeCount": int(changed),
        "tradeCloseCount": int(closed),
        "neverActivated": (
            int(activation_count)
            + int(accepted)
            + int(rejected)
            + int(changed)
            + int(closed)
        )
        == 0,
    }


def _activation_report(
    *,
    candidates: Sequence[Mapping[str, Any]],
    behaviors: Mapping[str, Mapping[str, Any]],
    aggregates: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    by_candidate = {}
    rows = []
    for candidate in sorted(candidates, key=lambda item: item["candidateId"]):
        candidate_id = candidate["candidateId"]
        candidate_rows = [
            _activation_for_capability(
                capability,
                behaviors[candidate_id],
                aggregates[candidate_id],
            )
            for capability in _authored_capabilities(candidate)
        ]
        by_candidate[candidate_id] = candidate_rows
        rows.extend(
            {"candidateId": candidate_id, **row} for row in candidate_rows
        )
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["capabilityType"])].append(row)
    summary = []
    for capability_type in sorted(grouped):
        group = grouped[capability_type]
        summary.append(
            {
                "capabilityType": capability_type,
                "authoredCandidateCount": len(
                    {str(row["candidateId"]) for row in group}
                ),
                "authoredInstanceCount": len(group),
                "evaluatedInstanceCount": sum(
                    bool(row["actuallyEvaluated"]) for row in group
                ),
                "acceptedIntentOrEffectCount": sum(
                    int(row["acceptedIntentOrEffectCount"]) for row in group
                ),
                "activationCount": sum(int(row["activationCount"]) for row in group),
                "rejectedIntentOrEffectCount": sum(
                    int(row["rejectedIntentOrEffectCount"]) for row in group
                ),
                "positionChangeCount": sum(
                    int(row["positionChangeCount"]) for row in group
                ),
                "tradeCloseCount": sum(int(row["tradeCloseCount"]) for row in group),
                "neverActivatedInstanceCount": sum(
                    bool(row["neverActivated"]) for row in group
                ),
            }
        )
    return (
        {
            "schemaVersion": "temporal_search_quality_behavioral_activation_v1",
            "candidateCount": len(candidates),
            "capabilityInstanceCount": len(rows),
            "summaryByCapabilityType": summary,
            "candidateCapabilities": rows,
        },
        by_candidate,
    )


def _cohort_membership(selection: Mapping[str, Any]) -> dict[str, str]:
    economic = {str(row["candidateId"]) for row in selection["economicArchive"]}
    novelty = {str(row["candidateId"]) for row in selection["noveltyArchive"]}
    output = {}
    for candidate_id in sorted(economic | novelty):
        if candidate_id in economic and candidate_id in novelty:
            output[candidate_id] = "economic_and_novelty"
        elif candidate_id in economic:
            output[candidate_id] = "economic_only"
        else:
            output[candidate_id] = "novelty_only"
    return output


def _composition_report(
    *,
    selection: Mapping[str, Any],
    population: Mapping[str, Mapping[str, Any]],
    structures: Mapping[str, Mapping[str, Any]],
    behaviors: Mapping[str, Mapping[str, Any]],
    initial_aggregates: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    membership = _cohort_membership(selection)
    cohort_rows = []
    for cohort in ("economic_only", "novelty_only", "economic_and_novelty"):
        ids = sorted(
            candidate_id
            for candidate_id, value in membership.items()
            if value == cohort
        )
        cohort_rows.append(
            {
                "cohort": cohort,
                "candidateCount": len(ids),
                "candidateIds": ids,
                "sourceModes": _categorical(
                    population[candidate_id]["sourceMode"] for candidate_id in ids
                ),
                "seedIds": _categorical(
                    population[candidate_id]["seedId"] for candidate_id in ids
                ),
                "mutationFamilies": _categorical(
                    family
                    for candidate_id in ids
                    for family in structures[candidate_id]["mutationFamilies"]
                ),
            }
        )
    novelty_ids = {
        str(row["candidateId"]) for row in selection["noveltyArchive"]
    }
    trade_threshold = _quantile(
        sorted(
            _safe_float(initial_aggregates[candidate_id]["totalTrades"])
            for candidate_id in novelty_ids
        ),
        0.10,
    ) or 0.0
    drawdown_threshold = _quantile(
        sorted(
            _safe_float(initial_aggregates[candidate_id]["maxWindowDrawdownR"])
            for candidate_id in novelty_ids
        ),
        0.90,
    ) or 0.0
    exposure_threshold = _quantile(
        sorted(
            _safe_float(initial_aggregates[candidate_id]["averageExposureRatio"])
            for candidate_id in novelty_ids
        ),
        0.10,
    ) or 0.0
    diagnostics = []
    for row in selection["noveltyArchive"]:
        candidate_id = str(row["candidateId"])
        aggregate = initial_aggregates[candidate_id]
        behavior = behaviors[candidate_id]
        diagnostics.append(
            {
                "candidateId": candidate_id,
                "noveltyRank": row.get("noveltyRank"),
                "minimumArchiveDistance": row.get("minimumArchiveDistance"),
                "totalTrades": int(aggregate["totalTrades"]),
                "averageExposureRatio": aggregate["averageExposureRatio"],
                "maxWindowDrawdownR": aggregate["maxWindowDrawdownR"],
                "rejectedIntentCount": behavior["rejectedIntentCount"],
                "veryLowTradeCount": int(aggregate["totalTrades"])
                <= trade_threshold,
                "pathologicalDrawdown": _safe_float(
                    aggregate["maxWindowDrawdownR"]
                )
                >= drawdown_threshold,
                "rejectedManagementActions": behavior["rejectedIntentCount"] > 0,
                "nearZeroMarketParticipation": _safe_float(
                    aggregate["averageExposureRatio"]
                )
                <= exposure_threshold,
                "routeCount": structures[candidate_id]["routeCount"],
                "averageHoldingBars": aggregate["averageHoldingBars"],
            }
        )
    return {
        "schemaVersion": "temporal_search_quality_archive_composition_v1",
        "confirmationCandidateCount": len(membership),
        "cohorts": cohort_rows,
        "noveltyDriverThresholds": {
            "veryLowTradeCountP10": trade_threshold,
            "pathologicalDrawdownP90": drawdown_threshold,
            "nearZeroExposureP10": exposure_threshold,
        },
        "noveltyDriverCounts": {
            key: sum(bool(row[key]) for row in diagnostics)
            for key in (
                "veryLowTradeCount",
                "pathologicalDrawdown",
                "rejectedManagementActions",
                "nearZeroMarketParticipation",
            )
        },
        "noveltyCandidates": diagnostics,
    }


def _aggregate_map(
    *,
    population: Mapping[str, Mapping[str, Any]],
    result_root: Path,
) -> dict[str, dict[str, Any]]:
    results = load_stage_results(result_root)
    return {
        candidate_id: _aggregate_candidate(population[candidate_id], windows)
        for candidate_id, windows in sorted(results.items())
    }


def _drift_report(
    *,
    selection: Mapping[str, Any],
    initial: Mapping[str, Mapping[str, Any]],
    confirmation: Mapping[str, Mapping[str, Any]],
    normalized_preparation: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    membership = _cohort_membership(selection)
    economic_rank = {
        str(row["candidateId"]): int(row["economicRank"])
        for row in selection["economicArchive"]
    }
    novelty_rank = {
        str(row["candidateId"]): int(row["noveltyRank"])
        for row in selection["noveltyArchive"]
    }
    confirmation_economic = select_economic_archive(
        list(confirmation.values()),
        archive_size=normalized_preparation["screening"]["economicArchiveSize"],
        minimum_trades_per_window=normalized_preparation["screening"][
            "minimumTradesPerInitialWindowEconomic"
        ],
    )
    confirmation_novelty = select_novelty_archive(
        list(confirmation.values()),
        archive_size=normalized_preparation["screening"]["noveltyArchiveSize"],
        minimum_total_trades=normalized_preparation["screening"][
            "minimumTotalTradesNovelty"
        ],
    )
    confirmation_economic_rank = {
        str(row["candidateId"]): int(row["economicRank"])
        for row in confirmation_economic
    }
    confirmation_novelty_rank = {
        str(row["candidateId"]): int(row["noveltyRank"])
        for row in confirmation_novelty
    }
    rows = []
    by_id = {}
    for candidate_id in sorted(membership):
        left = initial[candidate_id]
        right = confirmation[candidate_id]
        row = {
            "candidateId": candidate_id,
            "cohort": membership[candidate_id],
            "initial": {
                "totalConservativeNetR": left["totalConservativeNetR"],
                "worstWindowConservativeNetR": left[
                    "worstWindowConservativeNetR"
                ],
                "totalTrades": left["totalTrades"],
                "maxWindowDrawdownR": left["maxWindowDrawdownR"],
                "costDragR": left["costDragR"],
                "economicRank": economic_rank.get(candidate_id),
                "noveltyRank": novelty_rank.get(candidate_id),
            },
            "confirmation": {
                "totalConservativeNetR": right["totalConservativeNetR"],
                "worstWindowConservativeNetR": right[
                    "worstWindowConservativeNetR"
                ],
                "totalTrades": right["totalTrades"],
                "maxWindowDrawdownR": right["maxWindowDrawdownR"],
                "costDragR": right["costDragR"],
                "economicRank": confirmation_economic_rank.get(candidate_id),
                "noveltyRank": confirmation_novelty_rank.get(candidate_id),
            },
            "delta": {
                "totalConservativeNetR": _safe_float(
                    right["totalConservativeNetR"]
                )
                - _safe_float(left["totalConservativeNetR"]),
                "worstWindowConservativeNetR": _safe_float(
                    right["worstWindowConservativeNetR"]
                )
                - _safe_float(left["worstWindowConservativeNetR"]),
                "totalTrades": int(right["totalTrades"]) - int(left["totalTrades"]),
                "maxWindowDrawdownR": _safe_float(right["maxWindowDrawdownR"])
                - _safe_float(left["maxWindowDrawdownR"]),
                "costDragR": _safe_float(right["costDragR"])
                - _safe_float(left["costDragR"]),
            },
            "behavioralFingerprintDistance": fingerprint_distance(left, right),
        }
        rows.append(row)
        by_id[candidate_id] = row
    summaries = []
    for cohort in ("economic_only", "novelty_only", "economic_and_novelty"):
        group = [row for row in rows if row["cohort"] == cohort]
        summaries.append(
            {
                "cohort": cohort,
                "candidateCount": len(group),
                "initialTotalConservativeNetR": _numeric_summary(
                    row["initial"]["totalConservativeNetR"] for row in group
                ),
                "confirmationTotalConservativeNetR": _numeric_summary(
                    row["confirmation"]["totalConservativeNetR"] for row in group
                ),
                "netRDrift": _numeric_summary(
                    row["delta"]["totalConservativeNetR"] for row in group
                ),
                "tradeCountDrift": _numeric_summary(
                    row["delta"]["totalTrades"] for row in group
                ),
                "drawdownDrift": _numeric_summary(
                    row["delta"]["maxWindowDrawdownR"] for row in group
                ),
                "costDragDrift": _numeric_summary(
                    row["delta"]["costDragR"] for row in group
                ),
                "behavioralFingerprintDistance": _numeric_summary(
                    row["behavioralFingerprintDistance"] for row in group
                ),
                "initialPositiveRate": (
                    sum(row["initial"]["totalConservativeNetR"] > 0 for row in group)
                    / len(group)
                    if group
                    else 0.0
                ),
                "confirmationPositiveRate": (
                    sum(
                        row["confirmation"]["totalConservativeNetR"] > 0
                        for row in group
                    )
                    / len(group)
                    if group
                    else 0.0
                ),
            }
        )
    return (
        {
            "schemaVersion": "temporal_search_quality_screening_confirmation_drift_v1",
            "candidateCount": len(rows),
            "cohortSummaries": summaries,
            "candidates": rows,
        },
        by_id,
    )


def _dossier_markdown(dossiers: Sequence[Mapping[str, Any]]) -> str:
    lines = ["# Stage 5E-1 representative dossiers", ""]
    for dossier in dossiers:
        aggregate = dossier["fourWindowEconomics"]
        lines.extend(
            [
                f"## {dossier['candidateId']}",
                "",
                "Roles: " + ", ".join(dossier["roles"]),
                "",
                (
                    f"Source: `{dossier['sourceMode']}` / `{dossier['seedId']}`; "
                    f"states `{dossier['graphOutline']['stateCount']}`, transitions "
                    f"`{dossier['graphOutline']['transitionCount']}`, routes "
                    f"`{dossier['graphOutline']['routeCount']}`."
                ),
                "",
                (
                    f"Four-window economics: `{aggregate['totalConservativeNetR']:.6f} R` "
                    f"conservative net, `{aggregate['worstWindowConservativeNetR']:.6f} R` "
                    f"worst window, `{aggregate['totalTrades']}` trades, "
                    f"`{aggregate['maxWindowDrawdownR']:.6f} R` maximum window drawdown, "
                    f"`{aggregate['costDragR']:.6f} R` cost drag."
                ),
                "",
                "Management: `"
                + json.dumps(dossier["managementBehavior"], sort_keys=True)
                + "`",
                "",
                "Actual action counts: `"
                + json.dumps(dossier["actualActionCounts"], sort_keys=True)
                + "`",
                "",
                "Actual route counts: `"
                + json.dumps(dossier["actualRouteDistribution"], sort_keys=True)
                + "`",
                "",
            ]
        )
    return "\n".join(lines)


def _phase_a_markdown(
    *,
    phase_a: Mapping[str, Any],
    generation: Mapping[str, Any],
    structural: Mapping[str, Any],
    activation: Mapping[str, Any],
    diversity: Mapping[str, Any],
    composition: Mapping[str, Any],
    drift: Mapping[str, Any],
) -> str:
    overall = generation["overall"]
    lines = [
        "# Stage 5E-1 Phase A: existing-evidence forensics",
        "",
        phase_a["interpretationBoundary"],
        "",
        "## Generation yield",
        "",
        (
            f"`{overall['acceptedCount']}` accepted, `{overall['rejectedCount']}` "
            f"rejected, and `{overall['duplicateProgramCount']}` duplicate program "
            f"from `{overall['proposalCount']}` proposals."
        ),
        "",
        "## Structural coverage",
        "",
        (
            f"`{structural['combinationCount']}` exact structural combinations "
            f"across `{structural['candidateCount']}` candidates; median states "
            f"`{structural['stateCount']['median']}`, median transitions "
            f"`{structural['transitionCount']['median']}`, median entry routes "
            f"`{structural['routeCount']['median']}`."
        ),
        "",
        "## Behavioral activation",
        "",
    ]
    for row in activation["summaryByCapabilityType"]:
        lines.append(
            f"- `{row['capabilityType']}`: {row['authoredInstanceCount']} authored, "
            f"{row['evaluatedInstanceCount']} evaluated, {row['activationCount']} "
            f"activations, {row['neverActivatedInstanceCount']} never activated, "
            f"{row['rejectedIntentOrEffectCount']} rejected effects."
        )
    lines.extend(["", "## Behavioral diversity", ""])
    for key in ("initialAC", "fourWindowABCD"):
        row = diversity[key]
        lines.append(
            f"- `{key}`: median distance `{row['behavioralDistance']['median']}`, "
            f"reference threshold `{row['referenceThreshold']}`, "
            f"`{row['referenceClusterCount']}` components, structural/behavioral "
            f"correlation `{row['structuralBehavioralPearsonCorrelation']}`."
        )
    lines.extend(["", "## Archive composition", ""])
    for row in composition["cohorts"]:
        lines.append(f"- `{row['cohort']}`: {row['candidateCount']} candidates.")
    lines.extend(["", "## A/C to B/D drift", ""])
    for row in drift["cohortSummaries"]:
        lines.append(
            f"- `{row['cohort']}`: median conservative-net drift "
            f"`{row['netRDrift']['median']}`, initial positive rate "
            f"`{row['initialPositiveRate']}`, confirmation positive rate "
            f"`{row['confirmationPositiveRate']}`."
        )
    lines.extend(
        [
            "",
            f"Phase A identity: `{phase_a['phaseAReportSha256']}`.",
            "",
        ]
    )
    return "\n".join(lines)


def _dossiers(
    *,
    population: Mapping[str, Mapping[str, Any]],
    four_window_aggregates: Mapping[str, Mapping[str, Any]],
    four_window_behaviors: Mapping[str, Mapping[str, Any]],
    structures: Mapping[str, Mapping[str, Any]],
    activations: Mapping[str, Sequence[Mapping[str, Any]]],
    drift: Mapping[str, Mapping[str, Any]],
    membership: Mapping[str, str],
    diversity: Mapping[str, Any],
    pair_distances: Mapping[tuple[str, str], float],
) -> list[dict[str, Any]]:
    candidate_ids = sorted(four_window_aggregates)
    roles: dict[str, set[str]] = defaultdict(set)
    best_total = max(
        candidate_ids,
        key=lambda candidate_id: (
            _safe_float(
                four_window_aggregates[candidate_id]["totalConservativeNetR"]
            ),
            candidate_id,
        ),
    )
    roles[best_total].add("best_total_conservative_r")
    best_worst = max(
        candidate_ids,
        key=lambda candidate_id: (
            _safe_float(
                four_window_aggregates[candidate_id][
                    "worstWindowConservativeNetR"
                ]
            ),
            candidate_id,
        ),
    )
    roles[best_worst].add("best_worst_window_r")
    stable = min(
        candidate_ids,
        key=lambda candidate_id: (
            statistics.pvariance(
                [
                    _safe_float(row["conservativeNetR"])
                    for row in four_window_aggregates[candidate_id]["windowRecords"]
                ]
            ),
            _safe_float(
                four_window_aggregates[candidate_id]["maxWindowDrawdownR"]
            ),
            candidate_id,
        ),
    )
    roles[stable].add("most_stable_candidate")
    mean_distance = {}
    for candidate_id in candidate_ids:
        values = [
            pair_distances[tuple(sorted((candidate_id, other)))]
            for other in candidate_ids
            if other != candidate_id
        ]
        mean_distance[candidate_id] = math.fsum(values) / max(1, len(values))
    most_novel = max(candidate_ids, key=lambda value: (mean_distance[value], value))
    roles[most_novel].add("most_behaviorally_novel_candidate")
    overlap_ids = [
        candidate_id
        for candidate_id in candidate_ids
        if membership[candidate_id] == "economic_and_novelty"
    ]
    best_overlap = max(
        overlap_ids,
        key=lambda candidate_id: (
            _safe_float(
                four_window_aggregates[candidate_id]["totalConservativeNetR"]
            ),
            candidate_id,
        ),
    )
    roles[best_overlap].add("best_economic_novelty_overlap")
    collapse = min(
        candidate_ids,
        key=lambda candidate_id: (
            _safe_float(drift[candidate_id]["delta"]["totalConservativeNetR"]),
            candidate_id,
        ),
    )
    roles[collapse].add("largest_screening_to_confirmation_collapse")
    improvement = max(
        candidate_ids,
        key=lambda candidate_id: (
            _safe_float(drift[candidate_id]["delta"]["totalConservativeNetR"]),
            candidate_id,
        ),
    )
    roles[improvement].add("largest_screening_to_confirmation_improvement")
    for family in diversity["majorBehavioralFamilies"]:
        roles[family["medoidCandidateId"]].add(
            f"medoid:{family['familyId']}"
        )
    output = []
    for candidate_id in sorted(roles):
        candidate = population[candidate_id]
        structure = structures[candidate_id]
        aggregate = four_window_aggregates[candidate_id]
        output.append(
            {
                "candidateId": candidate_id,
                "roles": sorted(roles[candidate_id]),
                "sourceMode": candidate["sourceMode"],
                "seedId": candidate["seedId"],
                "mutationFamilies": structure["mutationFamilies"],
                "graphOutline": {
                    "stateCount": structure["stateCount"],
                    "transitionCount": structure["transitionCount"],
                    "routeCount": structure["routeCount"],
                    "guardKinds": sorted(set(structure["guardKinds"])),
                    "entryTransitionIds": structure["entryTransitionIds"],
                    "discretionaryExitTransitionIds": structure[
                        "discretionaryExitTransitionIds"
                    ],
                },
                "managementBehavior": structure["managementPlans"],
                "capabilityActivation": list(activations[candidate_id]),
                "actualActionCounts": four_window_behaviors[candidate_id][
                    "acceptedActionCounts"
                ],
                "rejectedActionCounts": four_window_behaviors[candidate_id][
                    "rejectedActionCounts"
                ],
                "actualRouteDistribution": four_window_behaviors[candidate_id][
                    "routeDistribution"
                ],
                "closeReasonDistribution": four_window_behaviors[candidate_id][
                    "closeReasonDistribution"
                ],
                "fourWindowEconomics": {
                    key: aggregate[key]
                    for key in (
                        "totalConservativeNetR",
                        "worstWindowConservativeNetR",
                        "profitableWindowCount",
                        "totalTrades",
                        "maxWindowDrawdownR",
                        "costDragR",
                    )
                },
                "windowEconomics": [
                    {
                        key: row[key]
                        for key in (
                            "analysisWindowStart",
                            "analysisWindowEnd",
                            "trades",
                            "conservativeNetR",
                            "maxDrawdownR",
                        )
                    }
                    for row in aggregate["windowRecords"]
                ],
            }
        )
    return output


def prepare_search_quality_study(
    *,
    discovery_root: Path | str,
    initial_result_root: Path | str,
    confirmation_result_root: Path | str,
    evidence_path: Path | str,
    output_root: Path | str,
    expected_report_sha256: str,
    expected_manifest_sha256: str,
    expected_evidence_file_sha256: str,
    source_autoresearch_commit: str,
    analysis_autoresearch_commit: str,
    fuzzfolio_commit: str,
    worker_contract_sha256: str,
) -> dict[str, Any]:
    discovery = Path(discovery_root).resolve()
    initial_root = Path(initial_result_root).resolve()
    confirmation_root = Path(confirmation_result_root).resolve()
    evidence = Path(evidence_path).resolve()
    output = Path(output_root).resolve()
    if output == discovery or discovery in output.parents:
        raise TemporalSearchQualityError(
            "search-quality output must be a sibling outside the immutable discovery root"
        )
    audit = audit_discovery(discovery)
    if not audit.get("ok"):
        raise TemporalSearchQualityError("source discovery audit failed")
    final_report = _read_json(discovery / "final" / "report.json", name="final report")
    source_manifest = _read_json(discovery / "manifest.json", name="source manifest")
    if final_report.get("reportSha256") != expected_report_sha256:
        raise TemporalSearchQualityError("Stage 5E-0 report identity mismatch")
    if source_manifest.get("manifestSha256") != expected_manifest_sha256:
        raise TemporalSearchQualityError("Stage 5E-0 manifest identity mismatch")
    if _file_sha256(evidence) != expected_evidence_file_sha256.upper():
        raise TemporalSearchQualityError("Stage 5E-0 evidence file SHA-256 mismatch")
    population_payload = _read_json(discovery / "population.json", name="population")
    journal = _read_json(discovery / "generation-journal.json", name="journal")
    selection = _read_json(
        discovery / "screening" / "initial-selection.json", name="selection"
    )
    preparation_payload = _read_json(
        discovery / "preparation.json", name="preparation"
    )
    normalized_preparation = _normalize_preparation(preparation_payload)
    if (
        normalized_preparation["workerContract"]["workerContractSha256"]
        != worker_contract_sha256
    ):
        raise TemporalSearchQualityError("worker contract identity mismatch")
    population = {
        str(candidate["candidateId"]): candidate
        for candidate in population_payload["candidates"]
    }
    initial_results = load_stage_results(initial_root)
    confirmation_results = load_stage_results(confirmation_root)
    if _result_set_sha256(initial_results) != final_report["initialResultSetSha256"]:
        raise TemporalSearchQualityError("initial result-set identity mismatch")
    if (
        _result_set_sha256(confirmation_results)
        != final_report["confirmationResultSetSha256"]
    ):
        raise TemporalSearchQualityError("confirmation result-set identity mismatch")
    if set(initial_results) != set(population):
        raise TemporalSearchQualityError("initial results do not cover the population")
    confirmation_ids = set(selection["confirmationCandidateIds"])
    if set(confirmation_results) != confirmation_ids:
        raise TemporalSearchQualityError(
            "confirmation results do not exactly cover the confirmation union"
        )
    raw_initial = _raw_stage_results(initial_root)
    raw_confirmation = _raw_stage_results(confirmation_root)
    initial_aggregates = {
        candidate_id: _aggregate_candidate(population[candidate_id], windows)
        for candidate_id, windows in sorted(initial_results.items())
    }
    confirmation_aggregates = {
        candidate_id: _aggregate_candidate(population[candidate_id], windows)
        for candidate_id, windows in sorted(confirmation_results.items())
    }
    four_window_aggregates = {
        str(row["candidateId"]): row for row in final_report["candidateAggregates"]
    }
    if set(four_window_aggregates) != confirmation_ids:
        raise TemporalSearchQualityError("final four-window aggregates mismatch")
    generation_yield = _generation_yield(journal)
    structural_coverage, structures = _structural_coverage(
        list(population.values())
    )
    initial_behaviors = {
        candidate_id: _behavior_record(
            candidate_id,
            raw_initial[candidate_id],
            initial_aggregates[candidate_id],
        )
        for candidate_id in sorted(population)
    }
    four_window_behaviors = {
        candidate_id: _behavior_record(
            candidate_id,
            raw_initial[candidate_id] + raw_confirmation[candidate_id],
            four_window_aggregates[candidate_id],
        )
        for candidate_id in sorted(confirmation_ids)
    }
    evidence_behaviors = dict(initial_behaviors)
    evidence_aggregates = dict(initial_aggregates)
    for candidate_id in sorted(confirmation_ids):
        evidence_behaviors[candidate_id] = four_window_behaviors[candidate_id]
        evidence_aggregates[candidate_id] = four_window_aggregates[candidate_id]
    activation_report, activations = _activation_report(
        candidates=list(population.values()),
        behaviors=evidence_behaviors,
        aggregates=evidence_aggregates,
    )
    initial_diversity, _initial_pairs = _diversity_report(
        label="initial_ac",
        behaviors=initial_behaviors,
        structures=structures,
    )
    four_diversity, four_pairs = _diversity_report(
        label="four_window_abcd",
        behaviors=four_window_behaviors,
        structures={
            candidate_id: structures[candidate_id]
            for candidate_id in sorted(confirmation_ids)
        },
    )
    diversity_report = {
        "schemaVersion": "temporal_search_quality_behavioral_diversity_v1",
        "distanceDefinition": {
            "components": [
                "entry overlap",
                "exposure overlap",
                "holding distribution",
                "close-reason distribution",
                "route use",
                "action use",
                "equity shape",
                "drawdown timing",
                "cost sensitivity",
            ],
            "aggregation": "equal-weight arithmetic mean using math.fsum",
        },
        "initialAC": initial_diversity,
        "fourWindowABCD": four_diversity,
        "initialCandidateBehaviors": [
            _public_behavior(initial_behaviors[candidate_id])
            for candidate_id in sorted(initial_behaviors)
        ],
        "fourWindowCandidateBehaviors": [
            _public_behavior(four_window_behaviors[candidate_id])
            for candidate_id in sorted(four_window_behaviors)
        ],
    }
    composition_report = _composition_report(
        selection=selection,
        population=population,
        structures=structures,
        behaviors=initial_behaviors,
        initial_aggregates=initial_aggregates,
    )
    drift_report, drift = _drift_report(
        selection=selection,
        initial=initial_aggregates,
        confirmation=confirmation_aggregates,
        normalized_preparation=normalized_preparation,
    )
    membership = _cohort_membership(selection)
    dossiers = _dossiers(
        population=population,
        four_window_aggregates=four_window_aggregates,
        four_window_behaviors=four_window_behaviors,
        structures=structures,
        activations=activations,
        drift=drift,
        membership=membership,
        diversity=four_diversity,
        pair_distances=four_pairs,
    )
    binding = {
        "schemaVersion": SEARCH_QUALITY_BINDING_SCHEMA,
        "stage5e0ReportSha256": expected_report_sha256,
        "stage5e0ManifestSha256": expected_manifest_sha256,
        "stage5e0EvidenceFileSha256": expected_evidence_file_sha256.upper(),
        "stage5e0EvidenceFilePath": str(evidence),
        "discoveryAuthorityId": final_report["discoveryAuthorityId"],
        "initialAuthorityId": final_report["initialAuthorityId"],
        "confirmationAuthorityId": final_report["confirmationAuthorityId"],
        "initialResultSetSha256": final_report["initialResultSetSha256"],
        "confirmationResultSetSha256": final_report[
            "confirmationResultSetSha256"
        ],
        "populationSha256": population_payload["populationSha256"],
        "generationJournalSha256": journal["journalSha256"],
        "sourceAutoresearchCommit": source_autoresearch_commit,
        "analysisAutoresearchCommit": analysis_autoresearch_commit,
        "fuzzfolioCommit": fuzzfolio_commit,
        "workerContractSha256": worker_contract_sha256,
        "sourceFileSha256": {
            "population.json": _file_sha256(discovery / "population.json"),
            "generation-journal.json": _file_sha256(
                discovery / "generation-journal.json"
            ),
            "screening/initial-selection.json": _file_sha256(
                discovery / "screening" / "initial-selection.json"
            ),
            "final/report.json": _file_sha256(
                discovery / "final" / "report.json"
            ),
        },
    }
    binding["bindingId"] = canonical_sha256(binding)
    phase_a = {
        "schemaVersion": SEARCH_QUALITY_PHASE_A_SCHEMA,
        "bindingId": binding["bindingId"],
        "sourceCandidateCount": len(population),
        "initialResultCount": sum(len(rows) for rows in initial_results.values()),
        "confirmationCandidateCount": len(confirmation_ids),
        "confirmationResultCount": sum(
            len(rows) for rows in confirmation_results.values()
        ),
        "generationYieldSha256": canonical_sha256(generation_yield),
        "structuralCoverageSha256": canonical_sha256(structural_coverage),
        "behavioralActivationSha256": canonical_sha256(activation_report),
        "behavioralDiversitySha256": canonical_sha256(diversity_report),
        "archiveCompositionSha256": canonical_sha256(composition_report),
        "screeningConfirmationDriftSha256": canonical_sha256(drift_report),
        "dossierCount": len(dossiers),
        "interpretationBoundary": (
            "Existing-evidence search-quality forensics only. No candidate was "
            "modified, promoted, regenerated, or evaluated on reserved evidence."
        ),
    }
    phase_a["phaseAReportSha256"] = canonical_sha256(phase_a)
    _write_immutable(output / "binding.json", binding)
    _write_immutable(output / "phase-a" / "generation-yield.json", generation_yield)
    _write_immutable(
        output / "phase-a" / "structural-coverage.json", structural_coverage
    )
    _write_immutable(
        output / "phase-a" / "behavioral-activation.json", activation_report
    )
    _write_immutable(
        output / "phase-a" / "behavioral-diversity.json", diversity_report
    )
    _write_immutable(
        output / "phase-a" / "archive-composition.json", composition_report
    )
    _write_immutable(
        output / "phase-a" / "screening-confirmation-drift.json", drift_report
    )
    _write_immutable(
        output / "phase-a" / "representative-dossiers.json",
        {
            "schemaVersion": "temporal_search_quality_dossiers_v1",
            "dossierCount": len(dossiers),
            "dossiers": dossiers,
            "dossiersSha256": canonical_sha256(dossiers),
        },
    )
    _write_text_immutable(
        output / "phase-a" / "representative-dossiers.md",
        _dossier_markdown(dossiers),
    )
    _write_immutable(output / "phase-a" / "report.json", phase_a)
    _write_text_immutable(
        output / "phase-a" / "report.md",
        _phase_a_markdown(
            phase_a=phase_a,
            generation=generation_yield,
            structural=structural_coverage,
            activation=activation_report,
            diversity=diversity_report,
            composition=composition_report,
            drift=drift_report,
        ),
    )
    manifest = _refresh_manifest(output, binding_id=binding["bindingId"])
    return {
        "schemaVersion": "temporal_search_quality_prepare_result_v1",
        "bindingId": binding["bindingId"],
        "phaseAReportSha256": phase_a["phaseAReportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "candidateCount": len(population),
        "confirmationCandidateCount": len(confirmation_ids),
        "dossierCount": len(dossiers),
        "outputRoot": str(output),
    }


def _allocation(
    strata: Mapping[tuple[str, str], Sequence[str]], *, sample_size: int
) -> dict[tuple[str, str], int]:
    if sample_size <= 0:
        raise TemporalSearchQualityError("control sample size must be positive")
    nonempty = {key: list(values) for key, values in strata.items() if values}
    if len(nonempty) > sample_size:
        raise TemporalSearchQualityError(
            "control sample cannot assign at least one candidate per stratum"
        )
    population_size = sum(len(values) for values in nonempty.values())
    if sample_size > population_size:
        raise TemporalSearchQualityError("control sample exceeds source population")
    quotas = {
        key: sample_size * len(nonempty[key]) / population_size
        for key in sorted(nonempty)
    }
    allocation = {
        key: min(len(nonempty[key]), max(1, math.floor(quotas[key])))
        for key in sorted(nonempty)
    }
    while sum(allocation.values()) < sample_size:
        eligible = sorted(
            (
                key
                for key in nonempty
                if allocation[key] < len(nonempty[key])
            ),
            key=lambda value: (
                -(quotas[value] - math.floor(quotas[value])),
                value,
            ),
        )
        if not eligible:
            raise TemporalSearchQualityError("control allocation exhausted strata")
        deficit = sample_size - sum(allocation.values())
        for key in eligible[:deficit]:
            allocation[key] += 1
    while sum(allocation.values()) > sample_size:
        eligible = sorted(
            (key for key in nonempty if allocation[key] > 1),
            key=lambda value: (
                quotas[value] - math.floor(quotas[value]),
                value,
            ),
        )
        if not eligible:
            raise TemporalSearchQualityError("control allocation cannot preserve minima")
        surplus = sum(allocation.values()) - sample_size
        for key in eligible[:surplus]:
            allocation[key] -= 1
    return allocation


def select_control_candidate_ids(
    *,
    population_candidates: Sequence[Mapping[str, Any]],
    excluded_candidate_ids: Sequence[str],
    sample_size: int = 64,
) -> tuple[list[str], list[dict[str, Any]]]:
    excluded = {str(value) for value in excluded_candidate_ids}
    eligible = [
        {
            "candidateId": str(candidate["candidateId"]),
            "sourceMode": str(candidate["sourceMode"]),
            "seedId": str(candidate["seedId"]),
        }
        for candidate in population_candidates
        if str(candidate["candidateId"]) not in excluded
    ]
    strata: dict[tuple[str, str], list[str]] = defaultdict(list)
    for candidate in eligible:
        strata[(candidate["sourceMode"], candidate["seedId"])].append(
            candidate["candidateId"]
        )
    for key in strata:
        strata[key].sort()
    allocations = _allocation(strata, sample_size=sample_size)
    eligible_count = sum(len(values) for values in strata.values())
    selected = []
    rows = []
    for key in sorted(strata):
        ranked = sorted(
            strata[key],
            key=lambda candidate_id: (
                hashlib.sha256(
                    f"{SEARCH_QUALITY_CONTROL_ALGORITHM}|{candidate_id}".encode(
                        "utf-8"
                    )
                ).hexdigest(),
                candidate_id,
            ),
        )
        chosen = ranked[: allocations[key]]
        selected.extend(chosen)
        rows.append(
            {
                "sourceMode": key[0],
                "seedId": key[1],
                "eligibleCount": len(ranked),
                "quotaNumerator": sample_size * len(ranked),
                "quotaDenominator": eligible_count,
                "floorQuota": (sample_size * len(ranked)) // eligible_count,
                "remainderNumerator": (sample_size * len(ranked))
                % eligible_count,
                "allocatedCount": allocations[key],
                "selectedCandidateIds": chosen,
            }
        )
    if len(selected) != sample_size or len(set(selected)) != sample_size:
        raise TemporalSearchQualityError("control selection cardinality mismatch")
    return selected, rows


def freeze_control_study(
    *,
    discovery_root: Path | str,
    quality_root: Path | str,
    sample_size: int = 64,
) -> dict[str, Any]:
    discovery = Path(discovery_root).resolve()
    quality = Path(quality_root).resolve()
    binding = _read_json(quality / "binding.json", name="quality binding")
    phase_a = _read_json(quality / "phase-a" / "report.json", name="Phase A report")
    if phase_a.get("bindingId") != binding.get("bindingId"):
        raise TemporalSearchQualityError("Phase A binding mismatch")
    population = _read_json(discovery / "population.json", name="population")
    selection = _read_json(
        discovery / "screening" / "initial-selection.json", name="selection"
    )
    if population.get("populationSha256") != binding.get("populationSha256"):
        raise TemporalSearchQualityError("control source population mismatch")
    excluded_ids = sorted(str(value) for value in selection["confirmationCandidateIds"])
    selected_ids, strata = select_control_candidate_ids(
        population_candidates=list(population["candidates"]),
        excluded_candidate_ids=excluded_ids,
        sample_size=sample_size,
    )
    population_map = {
        str(candidate["candidateId"]): candidate
        for candidate in population["candidates"]
    }
    source_preparation = _read_json(
        discovery / "preparation.json", name="discovery preparation"
    )
    normalized = _normalize_preparation(source_preparation)
    control_preparation = _finite_preparation(
        normalized,
        candidates=[population_map[candidate_id] for candidate_id in selected_ids],
        window_ids=normalized["screening"]["confirmationWindowIds"],
        label_suffix=SEARCH_QUALITY_CONTROL_ALGORITHM,
        max_tasks=sample_size
        * len(normalized["screening"]["confirmationWindowIds"]),
    )
    authority = build_authority(control_preparation)
    validate_authority(authority)
    control_selection = {
        "schemaVersion": SEARCH_QUALITY_CONTROL_SELECTION_SCHEMA,
        "selectionAlgorithm": SEARCH_QUALITY_CONTROL_ALGORITHM,
        "selectionRule": (
            "stratify sourceMode x seedId; proportional largest-remainder "
            "allocation with at least one per nonempty stratum; within stratum "
            "ascending sha256('stage5e1-control-v1|' + candidateId), then candidateId"
        ),
        "sourcePopulationSha256": population["populationSha256"],
        "sourceCandidateCount": len(population["candidates"]),
        "excludedConfirmationCandidateIds": excluded_ids,
        "excludedConfirmationCandidateCount": len(excluded_ids),
        "eligibleCandidateCount": len(population["candidates"]) - len(excluded_ids),
        "sampleSize": sample_size,
        "strata": strata,
        "selectedCandidateIds": selected_ids,
        "controlAuthorityId": authority["authorityId"],
        "developmentWindowIds": normalized["screening"][
            "confirmationWindowIds"
        ],
        "workerContractSha256": binding["workerContractSha256"],
        "prohibitedEvidence": normalized["prohibitedEvidence"],
    }
    control_selection["selectionSha256"] = canonical_sha256(control_selection)
    _write_immutable(quality / "control" / "control-selection.json", control_selection)
    _write_immutable(quality / "control" / "preparation.json", control_preparation)
    _write_immutable(quality / "control" / "authority.json", authority)
    manifest = _refresh_manifest(quality, binding_id=binding["bindingId"])
    return {
        "schemaVersion": "temporal_search_quality_control_freeze_result_v1",
        "bindingId": binding["bindingId"],
        "selectionSha256": control_selection["selectionSha256"],
        "controlAuthorityId": authority["authorityId"],
        "controlCandidateCount": sample_size,
        "controlTaskCount": sample_size
        * len(normalized["screening"]["confirmationWindowIds"]),
        "manifestSha256": manifest["manifestSha256"],
    }


def _comparison_candidate(
    *,
    candidate_id: str,
    cohort: str,
    initial_aggregate: Mapping[str, Any],
    evaluation_aggregate: Mapping[str, Any],
    behavior: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "candidateId": candidate_id,
        "cohort": cohort,
        "totalConservativeNetR": evaluation_aggregate[
            "totalConservativeNetR"
        ],
        "worstWindowConservativeNetR": evaluation_aggregate[
            "worstWindowConservativeNetR"
        ],
        "positiveWindowCount": evaluation_aggregate["profitableWindowCount"],
        "totalTrades": evaluation_aggregate["totalTrades"],
        "maxWindowDrawdownR": evaluation_aggregate["maxWindowDrawdownR"],
        "costDragR": evaluation_aggregate["costDragR"],
        "fingerprintStabilityDistance": fingerprint_distance(
            initial_aggregate, evaluation_aggregate
        ),
        "rejectedIntentRate": behavior["rejectedIntentRate"],
        "rejectedIntentCount": behavior["rejectedIntentCount"],
        "acceptedIntentOrEffectCount": behavior["acceptedIntentOrEffectCount"],
    }


_COMPARISON_METRICS = (
    "totalConservativeNetR",
    "worstWindowConservativeNetR",
    "positiveWindowCount",
    "totalTrades",
    "maxWindowDrawdownR",
    "costDragR",
    "fingerprintStabilityDistance",
    "rejectedIntentRate",
)


def _comparison_cohort_summary(
    cohort: str, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    return {
        "cohort": cohort,
        "candidateCount": len(rows),
        "positiveAggregateRate": (
            sum(_safe_float(row["totalConservativeNetR"]) > 0 for row in rows)
            / len(rows)
            if rows
            else 0.0
        ),
        "atLeastOnePositiveWindowRate": (
            sum(int(row["positiveWindowCount"]) > 0 for row in rows) / len(rows)
            if rows
            else 0.0
        ),
        "metrics": {
            metric: _numeric_summary(row[metric] for row in rows)
            for metric in _COMPARISON_METRICS
        },
    }


def _cliffs_delta(left: Sequence[Any], right: Sequence[Any]) -> float | None:
    if not left or not right:
        return None
    greater = 0
    less = 0
    for left_value in left:
        a = _safe_float(left_value)
        for right_value in right:
            b = _safe_float(right_value)
            if a > b:
                greater += 1
            elif a < b:
                less += 1
    return (greater - less) / (len(left) * len(right))


def _comparison_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Stage 5E-1 search-quality calibration",
        "",
        f"Primary outcome: `{report['primaryOutcome']}`",
        "",
        report["interpretationBoundary"],
        "",
        "## Cohorts",
        "",
    ]
    for cohort in report["cohortSummaries"]:
        metrics = cohort["metrics"]
        lines.append(
            f"- `{cohort['cohort']}`: {cohort['candidateCount']} candidates; "
            f"median B/D conservative net R "
            f"`{metrics['totalConservativeNetR']['median']}`; positive aggregate "
            f"rate `{cohort['positiveAggregateRate']}`; median worst-window R "
            f"`{metrics['worstWindowConservativeNetR']['median']}`."
        )
    lines.extend(
        [
            "",
            "## Selection lift",
            "",
            "```json",
            json.dumps(report["selectionLift"], indent=2, sort_keys=True),
            "```",
            "",
            "## Classification evidence",
            "",
            "```json",
            json.dumps(report["classificationEvidence"], indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def finalize_search_quality_study(
    *,
    discovery_root: Path | str,
    initial_result_root: Path | str,
    confirmation_result_root: Path | str,
    control_result_root: Path | str,
    quality_root: Path | str,
) -> dict[str, Any]:
    discovery = Path(discovery_root).resolve()
    initial_root = Path(initial_result_root).resolve()
    confirmation_root = Path(confirmation_result_root).resolve()
    control_root = Path(control_result_root).resolve()
    quality = Path(quality_root).resolve()
    binding = _read_json(quality / "binding.json", name="quality binding")
    phase_a = _read_json(quality / "phase-a" / "report.json", name="Phase A report")
    if phase_a.get("bindingId") != binding.get("bindingId"):
        raise TemporalSearchQualityError("Phase A binding mismatch")
    control_selection = _read_json(
        quality / "control" / "control-selection.json",
        name="control selection",
    )
    control_authority = _read_json(
        quality / "control" / "authority.json", name="control authority"
    )
    validate_authority(control_authority)
    population_payload = _read_json(discovery / "population.json", name="population")
    selection = _read_json(
        discovery / "screening" / "initial-selection.json", name="selection"
    )
    population = {
        str(candidate["candidateId"]): candidate
        for candidate in population_payload["candidates"]
    }
    initial_results = load_stage_results(initial_root)
    confirmation_results = load_stage_results(confirmation_root)
    control_results = load_stage_results(control_root)
    selected_control_ids = set(control_selection["selectedCandidateIds"])
    if set(control_results) != selected_control_ids:
        raise TemporalSearchQualityError(
            "control results do not exactly cover frozen control candidates"
        )
    if any(len(rows) != 2 for rows in control_results.values()):
        raise TemporalSearchQualityError(
            "control candidates must each contain exactly B and D results"
        )
    raw_confirmation = _raw_stage_results(confirmation_root)
    raw_control = _raw_stage_results(control_root)
    for rows in raw_control.values():
        for payload in rows:
            if payload.get("authority_id") != control_authority["authorityId"]:
                raise TemporalSearchQualityError(
                    "control result authority identity mismatch"
                )
            worker = payload.get("worker_attribution") or {}
            if worker.get("worker_contract_hash") != binding["workerContractSha256"]:
                raise TemporalSearchQualityError(
                    "control result worker contract mismatch"
                )
            if (payload.get("execution_evidence") or {}).get(
                "evidence_role"
            ) != "development_parity":
                raise TemporalSearchQualityError(
                    "control result used an unexpected evidence role"
                )
    initial_aggregates = {
        candidate_id: _aggregate_candidate(population[candidate_id], rows)
        for candidate_id, rows in sorted(initial_results.items())
    }
    confirmation_aggregates = {
        candidate_id: _aggregate_candidate(population[candidate_id], rows)
        for candidate_id, rows in sorted(confirmation_results.items())
    }
    control_aggregates = {
        candidate_id: _aggregate_candidate(population[candidate_id], rows)
        for candidate_id, rows in sorted(control_results.items())
    }
    confirmation_behaviors = {
        candidate_id: _behavior_record(
            candidate_id, raw_confirmation[candidate_id], aggregate
        )
        for candidate_id, aggregate in sorted(confirmation_aggregates.items())
    }
    control_behaviors = {
        candidate_id: _behavior_record(
            candidate_id, raw_control[candidate_id], aggregate
        )
        for candidate_id, aggregate in sorted(control_aggregates.items())
    }
    membership = _cohort_membership(selection)
    rows = []
    for candidate_id in sorted(confirmation_aggregates):
        rows.append(
            _comparison_candidate(
                candidate_id=candidate_id,
                cohort=membership[candidate_id],
                initial_aggregate=initial_aggregates[candidate_id],
                evaluation_aggregate=confirmation_aggregates[candidate_id],
                behavior=confirmation_behaviors[candidate_id],
            )
        )
    for candidate_id in sorted(control_aggregates):
        rows.append(
            _comparison_candidate(
                candidate_id=candidate_id,
                cohort="deterministic_control",
                initial_aggregate=initial_aggregates[candidate_id],
                evaluation_aggregate=control_aggregates[candidate_id],
                behavior=control_behaviors[candidate_id],
            )
        )
    cohort_order = (
        "economic_only",
        "novelty_only",
        "economic_and_novelty",
        "deterministic_control",
    )
    summaries = [
        _comparison_cohort_summary(
            cohort, [row for row in rows if row["cohort"] == cohort]
        )
        for cohort in cohort_order
    ]
    selected_rows = [row for row in rows if row["cohort"] != "deterministic_control"]
    control_rows = [row for row in rows if row["cohort"] == "deterministic_control"]
    selected_summary = _comparison_cohort_summary(
        "all_selected", selected_rows
    )
    control_summary = summaries[-1]
    selection_lift = {
        "selectedCandidateCount": len(selected_rows),
        "controlCandidateCount": len(control_rows),
        "medianTotalConservativeNetRDelta": _safe_float(
            selected_summary["metrics"]["totalConservativeNetR"]["median"]
        )
        - _safe_float(
            control_summary["metrics"]["totalConservativeNetR"]["median"]
        ),
        "medianWorstWindowRDelta": _safe_float(
            selected_summary["metrics"]["worstWindowConservativeNetR"]["median"]
        )
        - _safe_float(
            control_summary["metrics"]["worstWindowConservativeNetR"]["median"]
        ),
        "positiveAggregateRateDelta": selected_summary["positiveAggregateRate"]
        - control_summary["positiveAggregateRate"],
        "positiveWindowParticipationRateDelta": selected_summary[
            "atLeastOnePositiveWindowRate"
        ]
        - control_summary["atLeastOnePositiveWindowRate"],
        "totalConservativeNetRCliffsDelta": _cliffs_delta(
            [row["totalConservativeNetR"] for row in selected_rows],
            [row["totalConservativeNetR"] for row in control_rows],
        ),
        "worstWindowRCliffsDelta": _cliffs_delta(
            [row["worstWindowConservativeNetR"] for row in selected_rows],
            [row["worstWindowConservativeNetR"] for row in control_rows],
        ),
        "drawdownCliffsDeltaLowerIsBetter": _cliffs_delta(
            [-_safe_float(row["maxWindowDrawdownR"]) for row in selected_rows],
            [-_safe_float(row["maxWindowDrawdownR"]) for row in control_rows],
        ),
    }
    activation = _read_json(
        quality / "phase-a" / "behavioral-activation.json",
        name="behavioral activation",
    )
    management_types = {
        "move_stop_to_break_even_next_open",
        "trailing_stop",
    }
    management_activation = []
    for item in activation["summaryByCapabilityType"]:
        if item["capabilityType"] not in management_types:
            continue
        count = int(item["authoredInstanceCount"])
        never = int(item["neverActivatedInstanceCount"])
        management_activation.append(
            {
                "capabilityType": item["capabilityType"],
                "authoredInstanceCount": count,
                "neverActivatedInstanceCount": never,
                "neverActivatedShare": never / count if count else 0.0,
            }
        )
    diversity = _read_json(
        quality / "phase-a" / "behavioral-diversity.json",
        name="behavioral diversity",
    )
    initial_diversity = diversity["initialAC"]
    reference_row = next(
        row
        for row in initial_diversity["thresholdSweep"]
        if row["threshold"] == initial_diversity["referenceThreshold"]
    )
    lift_verified = (
        _safe_float(selection_lift["medianTotalConservativeNetRDelta"]) > 0
        and _safe_float(selection_lift["medianWorstWindowRDelta"]) > 0
        and _safe_float(selection_lift["positiveWindowParticipationRateDelta"]) > 0
        and _safe_float(selection_lift["totalConservativeNetRCliffsDelta"])
        >= 0.147
    )
    severe_management_gap = any(
        row["authoredInstanceCount"] >= 20
        and row["neverActivatedShare"] >= 0.75
        for row in management_activation
    )
    behaviorally_collapsed = (
        reference_row["largestClusterShare"] >= 0.75
        or _safe_float(initial_diversity["behavioralDistance"]["median"]) <= 0.15
    )
    if len(control_rows) != 64 or len(selected_rows) != 89:
        primary_outcome = "insufficient_evidence"
    elif severe_management_gap:
        primary_outcome = "management_activation_gap"
    elif behaviorally_collapsed:
        primary_outcome = "generator_behaviorally_collapsed"
    elif lift_verified:
        primary_outcome = "selection_enrichment_verified"
    else:
        primary_outcome = "selection_has_no_observable_lift"
    classification_evidence = {
        "classificationRuleVersion": "stage5e1-primary-outcome-v1",
        "selectionLiftVerified": lift_verified,
        "selectionLiftThresholds": {
            "medianTotalConservativeNetRDelta": "> 0",
            "medianWorstWindowRDelta": "> 0",
            "positiveWindowParticipationRateDelta": "> 0",
            "totalConservativeNetRCliffsDelta": ">= 0.147",
        },
        "severeManagementActivationGap": severe_management_gap,
        "managementGapRule": (
            "at least 20 authored instances and at least 75% never activated "
            "for trailing or break-even management"
        ),
        "managementActivation": management_activation,
        "behaviorallyCollapsed": behaviorally_collapsed,
        "behavioralCollapseRule": (
            "reference largest-cluster share >= 0.75 or median composite "
            "behavioral distance <= 0.15"
        ),
        "referenceLargestClusterShare": reference_row["largestClusterShare"],
        "medianBehavioralDistance": initial_diversity["behavioralDistance"][
            "median"
        ],
    }
    report = {
        "schemaVersion": SEARCH_QUALITY_FINAL_SCHEMA,
        "bindingId": binding["bindingId"],
        "phaseAReportSha256": phase_a["phaseAReportSha256"],
        "controlSelectionSha256": control_selection["selectionSha256"],
        "controlAuthorityId": control_authority["authorityId"],
        "controlResultSetSha256": _result_set_sha256(control_results),
        "primaryOutcome": primary_outcome,
        "cohortSummaries": summaries,
        "allSelectedSummary": selected_summary,
        "selectionLift": selection_lift,
        "classificationEvidence": classification_evidence,
        "candidateComparisons": rows,
        "interpretationBoundary": (
            "Stage 5E-1 exhausts windows A-D as development evidence for "
            "generator or selector redesign. It does not promote a strategy and "
            "does not authorize broader search or reserved-evidence use."
        ),
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(quality / "final" / "report.json", report)
    _write_text_immutable(
        quality / "final" / "report.md", _comparison_markdown(report)
    )
    manifest = _refresh_manifest(quality, binding_id=binding["bindingId"])
    return {
        "schemaVersion": "temporal_search_quality_finalize_result_v1",
        "bindingId": binding["bindingId"],
        "controlAuthorityId": control_authority["authorityId"],
        "controlResultSetSha256": report["controlResultSetSha256"],
        "primaryOutcome": primary_outcome,
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
    }


def audit_search_quality_study(
    *,
    quality_root: Path | str,
) -> dict[str, Any]:
    root = Path(quality_root).resolve()
    binding = _read_json(root / "binding.json", name="binding")
    binding_identity = dict(binding)
    claimed_binding_id = binding_identity.pop("bindingId", None)
    if canonical_sha256(binding_identity) != claimed_binding_id:
        raise TemporalSearchQualityError("binding identity mismatch")
    phase_a = _read_json(root / "phase-a" / "report.json", name="Phase A report")
    phase_identity = dict(phase_a)
    claimed_phase_id = phase_identity.pop("phaseAReportSha256", None)
    if canonical_sha256(phase_identity) != claimed_phase_id:
        raise TemporalSearchQualityError("Phase A report identity mismatch")
    control_authority_id = None
    control_path = root / "control" / "control-selection.json"
    if control_path.exists():
        selection = _read_json(control_path, name="control selection")
        identity = dict(selection)
        claimed = identity.pop("selectionSha256", None)
        if canonical_sha256(identity) != claimed:
            raise TemporalSearchQualityError("control selection identity mismatch")
        authority = _read_json(root / "control" / "authority.json", name="control authority")
        validate_authority(authority)
        if authority["authorityId"] != selection["controlAuthorityId"]:
            raise TemporalSearchQualityError("control authority identity mismatch")
        control_authority_id = authority["authorityId"]
    final_report_sha256 = None
    final_path = root / "final" / "report.json"
    if final_path.exists():
        final_report = _read_json(final_path, name="final quality report")
        final_identity = dict(final_report)
        claimed_final = final_identity.pop("reportSha256", None)
        if canonical_sha256(final_identity) != claimed_final:
            raise TemporalSearchQualityError("final quality report identity mismatch")
        if final_report.get("bindingId") != binding["bindingId"]:
            raise TemporalSearchQualityError("final quality report binding mismatch")
        final_report_sha256 = claimed_final
    manifest = _refresh_manifest(root, binding_id=binding["bindingId"])
    return {
        "schemaVersion": "temporal_search_quality_audit_v1",
        "ok": True,
        "bindingId": binding["bindingId"],
        "phaseAReportSha256": phase_a["phaseAReportSha256"],
        "controlAuthorityId": control_authority_id,
        "finalReportSha256": final_report_sha256,
        "manifestSha256": manifest["manifestSha256"],
        "fileCount": manifest["fileCount"],
    }


__all__ = [
    "SEARCH_QUALITY_CONTROL_ALGORITHM",
    "TemporalSearchQualityError",
    "audit_search_quality_study",
    "finalize_search_quality_study",
    "freeze_control_study",
    "prepare_search_quality_study",
    "select_control_candidate_ids",
]
