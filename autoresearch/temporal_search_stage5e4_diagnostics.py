"""Read-only Stage 5E-4 diagnosis of the frozen E/F screening result."""

from __future__ import annotations

from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_search import canonical_sha256
from .temporal_search_activation import (
    _authored_instances,
    _break_even_window,
)
from .temporal_search_quality import _numeric_summary, _pearson
from .temporal_search_stage5e3_midpoint import (
    _management_family,
    _read,
    _result_integrity,
    _structural_family,
    _window_label_lookup,
    audit_stage5e3_midpoint,
)


DIAGNOSTIC_SCHEMA = "temporal_search_stage5e4_diagnostic_checkpoint_v1"
BREAK_EVEN_SCHEMA = "temporal_search_stage5e4_break_even_causal_audit_v1"
COST_SCHEMA = "temporal_search_stage5e4_cost_carriage_audit_v1"
MANIFEST_SCHEMA = "temporal_search_stage5e4_diagnostic_manifest_v1"
THRESHOLD_GRID_R = (0.25, 0.5, 0.75, 1.0)
_SHA40 = re.compile(r"[0-9a-f]{40}")


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


def _verify_embedded_identity(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    supplied = str(payload.pop(field, ""))
    if canonical_sha256(payload) != supplied:
        raise TemporalDiscoveryContractError(f"{field} identity mismatch")
    return supplied


def _safe_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _replay(payload: Mapping[str, Any], view: str = "research_conservative") -> dict:
    try:
        replay = payload["cost_view_results"][view]["replay_result"]
    except (KeyError, TypeError) as exc:
        raise TemporalDiscoveryContractError(f"result lacks {view} replay") from exc
    if not isinstance(replay, Mapping):
        raise TemporalDiscoveryContractError(f"{view} replay root is invalid")
    return dict(replay)


def _intent_transition_map(replay: Mapping[str, Any]) -> dict[str, str]:
    output = {}
    for trace in replay.get("graphTraces") or []:
        for intent_id in trace.get("intentIds") or []:
            output[str(intent_id)] = str(trace.get("transitionId") or "unknown")
    return output


def _break_even_traces(
    instance: Mapping[str, Any], replay: Mapping[str, Any]
) -> list[dict[str, Any]]:
    transition_id = str(instance.get("transitionId") or "")
    intent_map = _intent_transition_map(replay)
    return [
        dict(trace)
        for trace in replay.get("executionTraces") or []
        if trace.get("actionKind") == "move_stop_to_break_even_next_open"
        and intent_map.get(str(trace.get("intentId") or "")) == transition_id
    ]


def _guard_leaves(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    kind = str(value.get("kind") or "unknown")
    if kind in {"all", "any"}:
        output = []
        for child in value.get("guards") or []:
            output.extend(_guard_leaves(child if isinstance(child, Mapping) else None))
        return output
    return [
        {
            key: child
            for key, child in value.items()
            if key in {"kind", "value", "events", "bars", "expected"}
        }
    ]


def _causal_blocker(
    *,
    positions: int,
    source_occupancy: int,
    maximum_mfe_r: float | None,
    required_r: float | None,
    applied: int,
    rejected: int,
    selected: int,
    extra_guard_kinds: Sequence[str],
) -> str:
    if positions <= 0:
        return "no_open_positions"
    if applied > 0:
        return "activated"
    if rejected > 0:
        return "scheduled_but_rejected"
    threshold_reached = (
        required_r is None
        or maximum_mfe_r is not None
        and maximum_mfe_r >= required_r
    )
    if source_occupancy <= 0:
        return (
            "intrabar_mfe_at_or_above_threshold_wrong_source_state"
            if threshold_reached
            else "intrabar_mfe_below_threshold_and_wrong_source_state"
        )
    if not threshold_reached:
        return "intrabar_mfe_below_threshold"
    if extra_guard_kinds:
        return "intrabar_mfe_reached_extra_guard_or_close_mark_overlap_unresolved"
    if selected > 0:
        return "transition_selected_but_intent_not_applied"
    return "intrabar_mfe_reached_source_occupied_but_close_mark_overlap_unresolved"


def _count_at_most(values: Sequence[int], threshold: int) -> int:
    return sum(value <= threshold for value in values)


def _pacing_metrics(
    *, entry_clocks: Sequence[int], exit_clocks: Sequence[int], holding_bars: Sequence[int]
) -> dict[str, Any]:
    entries = sorted(int(value) for value in entry_clocks)
    exits = sorted(int(value) for value in exit_clocks)
    inter_entry = [right - left for left, right in zip(entries, entries[1:])]
    post_close = []
    for exit_clock in exits:
        index = bisect_right(entries, exit_clock)
        if index < len(entries):
            post_close.append(entries[index] - exit_clock)
    return {
        "entryCount": len(entries),
        "closedTradeCount": len(exits),
        "interEntryGapBars": _numeric_summary(inter_entry),
        "postCloseReentryGapBars": _numeric_summary(post_close),
        "holdingBars": _numeric_summary(holding_bars),
        "rapidInterEntryCounts": {
            str(limit): _count_at_most(inter_entry, limit)
            for limit in (1, 3, 6, 12, 24)
        },
        "rapidPostCloseReentryCounts": {
            str(limit): _count_at_most(post_close, limit)
            for limit in (1, 3, 6, 12, 24)
        },
        "shortHoldingCounts": {
            str(limit): _count_at_most(holding_bars, limit)
            for limit in (3, 6, 12, 24)
        },
        "interEntryGapCount": len(inter_entry),
        "postCloseReentryGapCount": len(post_close),
    }


def _window_trade_rows(
    *, candidate_id: str, label: str, replay: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    intent_map = _intent_transition_map(replay)
    trades = []
    for trade in replay.get("trades") or []:
        gross_r = _safe_float(trade.get("grossR"))
        net_r = _safe_float(trade.get("netR"))
        mfe_r = _safe_float(trade.get("maxFavorableExcursionR"))
        trades.append(
            {
                "candidateId": candidate_id,
                "windowLabel": label,
                "tradeId": trade.get("tradeId"),
                "positionId": trade.get("positionId"),
                "entryRoute": intent_map.get(
                    str(trade.get("openingIntentId") or ""), "unknown"
                ),
                "closeReason": str(trade.get("closeReason") or "unknown"),
                "entryClockIndex": int(trade.get("entryClockIndex") or 0),
                "exitClockIndex": int(trade.get("exitClockIndex") or 0),
                "holdingBars": int(trade.get("holdingBars") or 0),
                "grossR": gross_r,
                "conservativeNetR": net_r,
                "costDragR": gross_r - net_r,
                "maximumFavorableExcursionR": mfe_r,
                "maximumAdverseExcursionR": _safe_float(
                    trade.get("maxAdverseExcursionR")
                ),
                "favorableGivebackR": mfe_r - gross_r,
                "breakEvenApplied": trade.get("breakEvenApplied") is True,
                "trailingActive": bool((trade.get("trailing") or {}).get("active")),
                "classification": (
                    "gross_positive_cost_dominated"
                    if gross_r > 0 and net_r <= 0
                    else "gross_positive_net_positive"
                    if gross_r > 0 and net_r > 0
                    else "gross_nonpositive"
                ),
            }
        )
    entry_clocks = [
        int(trace.get("clockIndex") or 0)
        for trace in replay.get("executionTraces") or []
        if trace.get("actionKind") == "enter_next_open"
        and trace.get("status") == "filled"
    ]
    pacing = _pacing_metrics(
        entry_clocks=entry_clocks,
        exit_clocks=[row["exitClockIndex"] for row in trades],
        holding_bars=[row["holdingBars"] for row in trades],
    )
    return trades, pacing


def _aggregate_pacing(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {
        "entryCount": sum(int(row["entryCount"]) for row in rows),
        "closedTradeCount": sum(int(row["closedTradeCount"]) for row in rows),
        "interEntryGapCount": sum(int(row["interEntryGapCount"]) for row in rows),
        "postCloseReentryGapCount": sum(
            int(row["postCloseReentryGapCount"]) for row in rows
        ),
    }
    for name, limits in (
        ("rapidInterEntryCounts", (1, 3, 6, 12, 24)),
        ("rapidPostCloseReentryCounts", (1, 3, 6, 12, 24)),
        ("shortHoldingCounts", (3, 6, 12, 24)),
    ):
        output[name] = {
            str(limit): sum(int(row[name][str(limit)]) for row in rows)
            for limit in limits
        }
    return output


def _group_cost_summary(
    rows: Sequence[Mapping[str, Any]], dimension: str, *, explode: bool = False
) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        raw = row.get(dimension)
        values = raw if explode and isinstance(raw, list) else [raw]
        for value in values or ["none"]:
            groups[str(value or "none")].append(row)
    output = []
    for value, group in sorted(groups.items()):
        trade_count = sum(int(row["closedTradeCount"]) for row in group)
        gross_r = math.fsum(float(row["grossR"]) for row in group)
        net_r = math.fsum(float(row["conservativeNetR"]) for row in group)
        cost_r = gross_r - net_r
        rapid_count = sum(
            int(row["pacing"]["rapidPostCloseReentryCounts"]["3"])
            for row in group
        )
        gap_count = sum(
            int(row["pacing"]["postCloseReentryGapCount"]) for row in group
        )
        output.append(
            {
                "value": value,
                "candidateCount": len(group),
                "closedTradeCount": trade_count,
                "grossR": gross_r,
                "conservativeNetR": net_r,
                "costDragR": cost_r,
                "grossExpectancyPerTrade": _ratio(gross_r, trade_count),
                "conservativeExpectancyPerTrade": _ratio(net_r, trade_count),
                "costDragPerTrade": _ratio(cost_r, trade_count),
                "grossPositiveCandidateCount": sum(row["grossR"] > 0 for row in group),
                "costDominatedCandidateCount": sum(
                    row["grossR"] > 0 and row["conservativeNetR"] <= 0
                    for row in group
                ),
                "grossNonpositiveCandidateCount": sum(row["grossR"] <= 0 for row in group),
                "robustEnvelopeEligibleCount": sum(
                    bool(row["robustEnvelopeEligible"]) for row in group
                ),
                "rapidPostCloseReentryAtMost3Share": _ratio(rapid_count, gap_count),
                "candidateGrossExpectancyPerTrade": _numeric_summary(
                    row["grossExpectancyPerTrade"]
                    for row in group
                    if row["grossExpectancyPerTrade"] is not None
                ),
                "candidateConservativeExpectancyPerTrade": _numeric_summary(
                    row["conservativeExpectancyPerTrade"]
                    for row in group
                    if row["conservativeExpectancyPerTrade"] is not None
                ),
            }
        )
    return output


def _rank_band_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    active = sorted(
        (row for row in rows if int(row["closedTradeCount"]) > 0),
        key=lambda row: (int(row["closedTradeCount"]), str(row["candidateId"])),
    )
    output = []
    for band in range(4):
        start = len(active) * band // 4
        end = len(active) * (band + 1) // 4
        group = active[start:end]
        if not group:
            continue
        trades = sum(int(row["closedTradeCount"]) for row in group)
        gross = math.fsum(float(row["grossR"]) for row in group)
        net = math.fsum(float(row["conservativeNetR"]) for row in group)
        output.append(
            {
                "frequencyQuartile": band + 1,
                "candidateCount": len(group),
                "minimumTrades": min(int(row["closedTradeCount"]) for row in group),
                "maximumTrades": max(int(row["closedTradeCount"]) for row in group),
                "closedTradeCount": trades,
                "grossExpectancyPerTrade": _ratio(gross, trades),
                "conservativeExpectancyPerTrade": _ratio(net, trades),
                "costDominatedCandidateCount": sum(
                    row["grossR"] > 0 and row["conservativeNetR"] <= 0
                    for row in group
                ),
            }
        )
    return output


def _build_break_even_audit(
    *,
    candidates: Mapping[str, Mapping[str, Any]],
    raw_by_candidate: Mapping[str, Sequence[Mapping[str, Any]]],
    prior_activation: Mapping[str, Any],
    source_midpoint_sha256: str,
) -> dict[str, Any]:
    instances = []
    candidate_rows = []
    unique_trade_rows = []
    exact_applied_instance_count = 0
    for candidate_id in sorted(candidates):
        candidate = candidates[candidate_id]
        authored = [
            row
            for row in _authored_instances(candidate)
            if row["managementType"] == "break_even"
        ]
        if not authored:
            continue
        logical_trades = []
        exact_applied_positions: set[tuple[str, str]] = set()
        source_states = sorted({str(row.get("sourceStateId") or "") for row in authored})
        source_occupied_by_window = {}
        for payload in raw_by_candidate[candidate_id]:
            label = str(payload["_stage5e3WindowLabel"])
            replay = _replay(payload)
            trades = list(replay.get("trades") or [])
            for instance in authored:
                exact_applied_positions.update(
                    (label, str(row.get("positionId")))
                    for row in _break_even_traces(instance, replay)
                    if row.get("status") == "applied" and row.get("positionId")
                )
            source_occupied_by_window[label] = any(
                int((replay.get("metrics") or {}).get("stateOccupancy", {}).get(state) or 0)
                > 0
                for state in source_states
            )
            for trade in trades:
                position_id = str(trade.get("positionId") or "")
                mfe = _safe_float(trade.get("maxFavorableExcursionR"))
                logical_trades.append(
                    {
                        "candidateId": candidate_id,
                        "windowLabel": label,
                        "tradeId": trade.get("tradeId"),
                        "positionId": position_id,
                        "maximumFavorableExcursionR": mfe,
                        "holdingBars": int(trade.get("holdingBars") or 0),
                        "closeReason": str(trade.get("closeReason") or "unknown"),
                        "currentBreakEvenApplied": (
                            label,
                            position_id,
                        )
                        in exact_applied_positions,
                    }
                )
        # exact_applied_positions is complete only after both windows have been scanned.
        for row in logical_trades:
            row["currentBreakEvenApplied"] = (
                row["windowLabel"],
                row["positionId"],
            ) in exact_applied_positions
            row["postFill0_5RIntrabarExcursionOpportunity"] = (
                row["maximumFavorableExcursionR"] >= 0.5
            )
            row["currentRouteDidNotApplyOn0_5RIntrabarExcursionTrade"] = (
                row["postFill0_5RIntrabarExcursionOpportunity"]
                and not row["currentBreakEvenApplied"]
            )
        unique_trade_rows.extend(logical_trades)
        candidate_rows.append(
            {
                "candidateId": candidate_id,
                "sourceMode": candidate.get("sourceMode"),
                "seedId": candidate.get("seedId"),
                "authoredTransitionCount": len(authored),
                "sourceStateIds": source_states,
                "closedTradeCount": len(logical_trades),
                "exactAppliedTradeCount": sum(
                    row["currentBreakEvenApplied"] for row in logical_trades
                ),
                "thresholdReachTradeCounts": {
                    str(threshold): sum(
                        row["maximumFavorableExcursionR"] >= threshold
                        for row in logical_trades
                    )
                    for threshold in THRESHOLD_GRID_R
                },
                "postFill0_5RIntrabarExcursionOpportunityTradeCount": sum(
                    row["postFill0_5RIntrabarExcursionOpportunity"]
                    for row in logical_trades
                ),
                "currentRouteDidNotApplyOn0_5RIntrabarExcursionTradeCount": sum(
                    row["currentRouteDidNotApplyOn0_5RIntrabarExcursionTrade"]
                    for row in logical_trades
                ),
                "sourceLifecycleOccupiedByWindow": source_occupied_by_window,
            }
        )
        for instance in authored:
            guard_leaves = _guard_leaves(instance.get("transition", {}).get("guard"))
            extra_guard_kinds = sorted(
                {
                    str(row.get("kind") or "unknown")
                    for row in guard_leaves
                    if row.get("kind")
                    not in {"position_exists", "unrealized_r_at_least"}
                }
            )
            windows = []
            instance_applied = False
            for payload in raw_by_candidate[candidate_id]:
                label = str(payload["_stage5e3WindowLabel"])
                replay = _replay(payload)
                traces = _break_even_traces(instance, replay)
                statuses = Counter(str(row.get("status") or "unknown") for row in traces)
                applied_positions = {
                    str(row.get("positionId"))
                    for row in traces
                    if row.get("status") == "applied" and row.get("positionId")
                }
                trades = list(replay.get("trades") or [])
                positions_opened = int(
                    (replay.get("metrics") or {}).get("positionsOpened") or 0
                )
                exact_trades = [
                    row
                    for row in trades
                    if str(row.get("positionId") or "") in applied_positions
                ]
                source_state = str(instance.get("sourceStateId") or "")
                occupancy = int(
                    (replay.get("metrics") or {})
                    .get("stateOccupancy", {})
                    .get(source_state)
                    or 0
                )
                max_mfe = (
                    max(_safe_float(row.get("maxFavorableExcursionR")) for row in trades)
                    if trades
                    else None
                )
                required_r = (
                    None
                    if instance.get("activationThreshold") is None
                    else _safe_float(instance.get("activationThreshold"))
                )
                transition_selected = sum(
                    str(row.get("transitionId") or "")
                    == str(instance.get("transitionId") or "")
                    for row in replay.get("graphTraces") or []
                )
                delays = []
                entry_by_position = {
                    str(row.get("positionId") or ""): int(row.get("entryClockIndex") or 0)
                    for row in trades
                    if row.get("positionId")
                }
                for trace in traces:
                    if trace.get("status") != "applied" or not trace.get("positionId"):
                        continue
                    entry = entry_by_position.get(str(trace["positionId"]))
                    if entry is not None:
                        delays.append(int(trace.get("clockIndex") or 0) - entry)
                instance_applied = instance_applied or bool(applied_positions)
                windows.append(
                    {
                        "windowLabel": label,
                        "sourceStateOccupancy": occupancy,
                        "positionsOpened": positions_opened,
                        "closedTradeCount": len(trades),
                        "transitionSelectedCount": transition_selected,
                        "intentScheduledCount": int(statuses.get("scheduled") or 0),
                        "intentAppliedCount": int(statuses.get("applied") or 0),
                        "intentRejectedCount": int(statuses.get("rejected") or 0),
                        "exactAppliedTradeCount": len(exact_trades),
                        "changedClosureCount": sum(
                            str(row.get("closeReason") or "")
                            in {"break_even_stop", "break_even_gap"}
                            for row in exact_trades
                        ),
                        "maximumFavorableExcursionR": max_mfe,
                        "thresholdReachTradeCounts": {
                            str(threshold): sum(
                                _safe_float(row.get("maxFavorableExcursionR")) >= threshold
                                for row in trades
                            )
                            for threshold in THRESHOLD_GRID_R
                        },
                        "currentTriggerR": required_r,
                        "guardLeaves": guard_leaves,
                        "extraGuardKinds": extra_guard_kinds,
                        "causalBlocker": _causal_blocker(
                            positions=positions_opened,
                            source_occupancy=occupancy,
                            maximum_mfe_r=max_mfe,
                            required_r=required_r,
                            applied=int(statuses.get("applied") or 0),
                            rejected=int(statuses.get("rejected") or 0),
                            selected=transition_selected,
                            extra_guard_kinds=extra_guard_kinds,
                        ),
                        "observedCurrentRouteActivationDelayBars": _numeric_summary(
                            delays
                        ),
                        "timeFromEntryToClosureBars": _numeric_summary(
                            int(row.get("holdingBars") or 0) for row in trades
                        ),
                        "closureWithoutIntrabarThresholdReachReasonCounts": dict(
                            sorted(
                                Counter(
                                    str(row.get("closeReason") or "unknown")
                                    for row in trades
                                    if required_r is not None
                                    and _safe_float(row.get("maxFavorableExcursionR"))
                                    < required_r
                                ).items()
                            )
                        ),
                    }
                )
            exact_applied_instance_count += int(instance_applied)
            instances.append(
                {
                    "candidateId": candidate_id,
                    "instanceId": instance["instanceId"],
                    "transitionId": instance.get("transitionId"),
                    "sourceStateId": instance.get("sourceStateId"),
                    "destinationStateId": instance.get("transition", {}).get(
                        "destinationStateId"
                    ),
                    "sourceMode": candidate.get("sourceMode"),
                    "seedId": candidate.get("seedId"),
                    "currentTriggerR": instance.get("activationThreshold"),
                    "guardLeaves": guard_leaves,
                    "extraGuardKinds": extra_guard_kinds,
                    "exactlyActivated": instance_applied,
                    "windows": windows,
                }
            )
    blocker_counts = Counter(
        window["causalBlocker"] for row in instances for window in row["windows"]
    )
    prior_be = [
        row
        for row in prior_activation.get("instances") or []
        if row.get("managementType") == "break_even"
    ]
    threshold_summary = []
    for threshold in THRESHOLD_GRID_R:
        reached = [
            row
            for row in unique_trade_rows
            if row["maximumFavorableExcursionR"] >= threshold
        ]
        threshold_summary.append(
            {
                "thresholdR": threshold,
                "reachedTradeCount": len(reached),
                "reachedTradeShare": _ratio(len(reached), len(unique_trade_rows)),
                "candidateCountWithReach": len(
                    {str(row["candidateId"]) for row in reached}
                ),
                "windowCounts": dict(
                    sorted(Counter(str(row["windowLabel"]) for row in reached).items())
                ),
            }
        )
    value = {
        "schemaVersion": BREAK_EVEN_SCHEMA,
        "sourceMidpointSha256": source_midpoint_sha256,
        "logicalCandidateCount": len(candidate_rows),
        "authoredTransitionInstanceCount": len(instances),
        "priorMidpointReportedActivatedInstanceCount": sum(
            bool(row.get("activated")) for row in prior_be
        ),
        "exactTransitionAttributedActivatedInstanceCount": exact_applied_instance_count,
        "activationAttributionCorrection": (
            "break-even trade records are attributed only to the transition whose "
            "intent produced the applied execution trace"
        ),
        "causalBlockerWindowCounts": dict(sorted(blocker_counts.items())),
        "thresholdReachSummary": threshold_summary,
        "exactAppliedLogicalTradeCount": sum(
            row["currentBreakEvenApplied"] for row in unique_trade_rows
        ),
        "postFill0_5RIntrabarExcursionOpportunityTradeCount": sum(
            row["postFill0_5RIntrabarExcursionOpportunity"]
            for row in unique_trade_rows
        ),
        "currentRouteDidNotApplyOn0_5RIntrabarExcursionTradeCount": sum(
            row["currentRouteDidNotApplyOn0_5RIntrabarExcursionTrade"]
            for row in unique_trade_rows
        ),
        "thresholdTimingAvailability": {
            "firstThresholdCrossingClockPersisted": False,
            "reason": (
                "immutable replay results retain per-trade maximum excursion but not "
                "the observation clock of first threshold crossing"
            ),
            "exactCurrentRouteActivationDelayPersisted": True,
        },
        "causalLimits": [
            "the unrealized-R graph guard reads the completed-bar close while MFE records intrabar high or low excursion",
            "aggregate state occupancy and per-trade maximum excursion do not prove close-mark threshold and source-state overlap",
            "an intrabar MFE opportunity is not a claim that a canonical post-fill route would activate",
            "counterfactual activation does not estimate counterfactual P&L or closure",
            "management-shortening effects require a paired replay and are not inferred here",
        ],
        "candidates": candidate_rows,
        "instances": instances,
        "logicalTrades": unique_trade_rows,
    }
    value["breakEvenCausalAuditSha256"] = canonical_sha256(value)
    return value


def _build_cost_audit(
    *,
    candidates: Mapping[str, Mapping[str, Any]],
    raw_by_candidate: Mapping[str, Sequence[Mapping[str, Any]]],
    candidate_analysis: Mapping[str, Any],
    screening_aggregates: Mapping[str, Any],
    selector_envelope: Mapping[str, Any],
    source_midpoint_sha256: str,
) -> dict[str, Any]:
    analysis_map = {
        str(row["candidateId"]): row for row in candidate_analysis["candidates"]
    }
    aggregate_map = {
        str(row["candidateId"]): row for row in screening_aggregates["aggregates"]
    }
    eligibility_map = {
        str(row["candidateId"]): row for row in selector_envelope["eligibility"]
    }
    thresholds = selector_envelope["thresholds"]
    candidate_rows = []
    all_trades = []
    window_rows = []
    for candidate_id in sorted(candidates):
        candidate = candidates[candidate_id]
        candidate_trades = []
        pacing_rows = []
        for payload in raw_by_candidate[candidate_id]:
            label = str(payload["_stage5e3WindowLabel"])
            conservative = _replay(payload)
            trades, pacing = _window_trade_rows(
                candidate_id=candidate_id, label=label, replay=conservative
            )
            candidate_trades.extend(trades)
            pacing_rows.append(pacing)
            window_rows.append(
                {
                    "candidateId": candidate_id,
                    "windowLabel": label,
                    "observationsProcessed": int(
                        (conservative.get("metrics") or {}).get("observationsProcessed")
                        or 0
                    ),
                    "grossR": math.fsum(row["grossR"] for row in trades),
                    "conservativeNetR": math.fsum(
                        row["conservativeNetR"] for row in trades
                    ),
                    "closedTradeCount": len(trades),
                    "pacing": pacing,
                }
            )
        all_trades.extend(candidate_trades)
        closed = len(candidate_trades)
        gross = math.fsum(row["grossR"] for row in candidate_trades)
        net = math.fsum(row["conservativeNetR"] for row in candidate_trades)
        cost = gross - net
        pacing = _aggregate_pacing(pacing_rows)
        aggregate = aggregate_map[candidate_id]
        metadata = analysis_map[candidate_id]
        eligibility = eligibility_map[candidate_id]
        structural = _structural_family(candidate)
        management = _management_family(candidate)
        management_closes = sum(
            row["closeReason"]
            in {
                "break_even_stop",
                "break_even_gap",
                "trailing_stop",
                "trailing_stop_gap",
            }
            for row in candidate_trades
        )
        trailing_closes = sum(
            row["closeReason"] in {"trailing_stop", "trailing_stop_gap"}
            for row in candidate_trades
        )
        candidate_rows.append(
            {
                "candidateId": candidate_id,
                "sourceMode": candidate.get("sourceMode"),
                "seedId": candidate.get("seedId"),
                "mutationFamilies": metadata.get("mutationFamilies") or [],
                "structuralFamilySha256": structural["familySha256"],
                "managementFamilySha256": management["familySha256"],
                "closedTradeCount": closed,
                "positionsOpened": pacing["entryCount"],
                "grossR": gross,
                "conservativeNetR": net,
                "costDragR": cost,
                "grossExpectancyPerTrade": _ratio(gross, closed),
                "conservativeExpectancyPerTrade": _ratio(net, closed),
                "costDragPerTrade": _ratio(cost, closed),
                "economicsClassification": (
                    "gross_positive_cost_dominated"
                    if gross > 0 and net <= 0
                    else "gross_positive_net_positive"
                    if gross > 0 and net > 0
                    else "gross_nonpositive"
                ),
                "managementCloseShare": _ratio(management_closes, closed),
                "trailingCloseShare": _ratio(trailing_closes, closed),
                "pacing": pacing,
                "holdingBars": _numeric_summary(
                    row["holdingBars"] for row in candidate_trades
                ),
                "robustEnvelopeEligible": not any(
                    not bool(value) for value in eligibility["checks"].values()
                ),
                "robustEnvelopeChecks": eligibility["checks"],
                "robustEnvelopeMargins": {
                    "minimumTradesEveryWindow": min(
                        int(value) for value in aggregate["tradeCountsByWindow"]
                    )
                    - int(thresholds["minimumTradesEveryScreeningWindow"]),
                    "totalConservativeNetR": _safe_float(
                        aggregate["totalConservativeNetR"]
                    )
                    - _safe_float(thresholds["totalConservativeNetRMedian"]),
                    "worstWindowConservativeNetR": _safe_float(
                        aggregate["worstWindowConservativeNetR"]
                    )
                    - _safe_float(thresholds["worstWindowConservativeNetRMedian"]),
                    "maxWindowDrawdownR": _safe_float(
                        thresholds["maxWindowDrawdownRP75"]
                    )
                    - _safe_float(aggregate["maxWindowDrawdownR"]),
                    "costDragPerTrade": _safe_float(
                        thresholds["costDragPerTradeP75"]
                    )
                    - _safe_float(aggregate["costDragPerTrade"]),
                },
            }
        )
    trade_count = len(all_trades)
    gross_r = math.fsum(row["grossR"] for row in all_trades)
    net_r = math.fsum(row["conservativeNetR"] for row in all_trades)
    cost_r = gross_r - net_r
    close_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    route_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in all_trades:
        close_groups[str(row["closeReason"])].append(row)
        route_groups[str(row["entryRoute"])].append(row)

    def trade_group_summary(
        groups: Mapping[str, Sequence[Mapping[str, Any]]]
    ) -> list[dict[str, Any]]:
        output = []
        for value, group in sorted(groups.items()):
            count = len(group)
            gross = math.fsum(float(row["grossR"]) for row in group)
            net = math.fsum(float(row["conservativeNetR"]) for row in group)
            cost = gross - net
            output.append(
                {
                    "value": value,
                    "tradeCount": count,
                    "tradeShare": _ratio(count, trade_count),
                    "grossR": gross,
                    "conservativeNetR": net,
                    "costDragR": cost,
                    "grossExpectancyPerTrade": _ratio(gross, count),
                    "conservativeExpectancyPerTrade": _ratio(net, count),
                    "costDragPerTrade": _ratio(cost, count),
                    "holdingBars": _numeric_summary(row["holdingBars"] for row in group),
                    "maximumFavorableExcursionR": _numeric_summary(
                        row["maximumFavorableExcursionR"] for row in group
                    ),
                    "favorableGivebackR": _numeric_summary(
                        row["favorableGivebackR"] for row in group
                    ),
                }
            )
        return output

    active_rows = [row for row in candidate_rows if row["closedTradeCount"] > 0]
    rapid_shares = [
        _ratio(
            row["pacing"]["rapidPostCloseReentryCounts"]["3"],
            row["pacing"]["postCloseReentryGapCount"],
        )
        for row in active_rows
    ]
    correlation_rows = [
        (row, rapid)
        for row, rapid in zip(active_rows, rapid_shares)
        if rapid is not None and row["grossExpectancyPerTrade"] is not None
    ]
    value = {
        "schemaVersion": COST_SCHEMA,
        "sourceMidpointSha256": source_midpoint_sha256,
        "candidateCount": len(candidate_rows),
        "activeCandidateCount": len(active_rows),
        "closedTradeCount": trade_count,
        "grossR": gross_r,
        "conservativeNetR": net_r,
        "costDragR": cost_r,
        "grossExpectancyPerTrade": _ratio(gross_r, trade_count),
        "conservativeExpectancyPerTrade": _ratio(net_r, trade_count),
        "costDragPerTrade": _ratio(cost_r, trade_count),
        "economicsClassificationCounts": dict(
            sorted(Counter(row["economicsClassification"] for row in candidate_rows).items())
        ),
        "tradeClassificationCounts": dict(
            sorted(Counter(row["classification"] for row in all_trades).items())
        ),
        "closeReasonEconomics": trade_group_summary(close_groups),
        "entryRouteEconomics": trade_group_summary(route_groups),
        "frequencyQuartiles": _rank_band_summary(candidate_rows),
        "correlations": {
            "closedTradeCountVsCostDragR": _pearson(
                [float(row["closedTradeCount"]) for row in active_rows],
                [float(row["costDragR"]) for row in active_rows],
            ),
            "closedTradeCountVsGrossExpectancyPerTrade": _pearson(
                [float(row["closedTradeCount"]) for row in active_rows],
                [float(row["grossExpectancyPerTrade"]) for row in active_rows],
            ),
            "medianHoldingBarsVsGrossExpectancyPerTrade": _pearson(
                [float(row["holdingBars"]["median"]) for row in active_rows],
                [float(row["grossExpectancyPerTrade"]) for row in active_rows],
            ),
            "rapidPostCloseReentryAtMost3ShareVsGrossExpectancyPerTrade": _pearson(
                [float(rapid) for _row, rapid in correlation_rows],
                [float(row["grossExpectancyPerTrade"]) for row, _rapid in correlation_rows],
            ),
            "managementCloseShareVsGrossExpectancyPerTrade": _pearson(
                [float(row["managementCloseShare"] or 0.0) for row in active_rows],
                [float(row["grossExpectancyPerTrade"]) for row in active_rows],
            ),
        },
        "bySourceMode": _group_cost_summary(candidate_rows, "sourceMode"),
        "bySeed": _group_cost_summary(candidate_rows, "seedId"),
        "byMutationFamily": _group_cost_summary(
            candidate_rows, "mutationFamilies", explode=True
        ),
        "byStructuralFamily": _group_cost_summary(
            candidate_rows, "structuralFamilySha256"
        ),
        "byManagementFamily": _group_cost_summary(
            candidate_rows, "managementFamilySha256"
        ),
        "diagnosticBoundaries": {
            "costDragConcentrationCanBeObserved": True,
            "entryAndReentryPacingCanBeObserved": True,
            "singlePathManagementShorteningIsCausal": False,
            "reason": (
                "management-shortening claims require paired counterfactual replay; "
                "this audit reports associations and close-route economics only"
            ),
        },
        "candidates": candidate_rows,
        "windows": window_rows,
        "trades": all_trades,
    }
    value["costCarriageAuditSha256"] = canonical_sha256(value)
    return value


def _markdown(report: Mapping[str, Any]) -> str:
    be = report["breakEvenSummary"]
    cost = report["costSummary"]
    return "\n".join(
        [
            "# Stage 5E-4 read-only E/F diagnosis",
            "",
            f"Status: `{report['status']}`",
            "",
            "## Break-even causal audit",
            "",
            f"- Logical candidates with break-even: {be['logicalCandidateCount']}",
            f"- Authored transition instances: {be['authoredTransitionInstanceCount']}",
            "- Prior midpoint-reported activated instances: "
            f"{be['priorMidpointReportedActivatedInstanceCount']}",
            "- Exact transition-attributed activated instances: "
            f"{be['exactTransitionAttributedActivatedInstanceCount']}",
            "- Exact applied trades: "
            f"{be['exactAppliedLogicalTradeCount']}",
            "- Closed trades with intrabar MFE at or above 0.5R: "
            f"{be['postFill0_5RIntrabarExcursionOpportunityTradeCount']}",
            "- Such trades without an exact current-route application: "
            f"{be['currentRouteDidNotApplyOn0_5RIntrabarExcursionTradeCount']}",
            "- Activation caveat: the graph guard reads completed-bar close, not "
            "intrabar MFE",
            "",
            "## Cost carriage",
            "",
            f"- Closed trades: {cost['closedTradeCount']}",
            f"- Gross expectancy/trade: {cost['grossExpectancyPerTrade']:.6f} R",
            f"- Cost drag/trade: {cost['costDragPerTrade']:.6f} R",
            "- Conservative expectancy/trade: "
            f"{cost['conservativeExpectancyPerTrade']:.6f} R",
            "",
            "## Boundary",
            "",
            "This artifact reads only the already frozen E/F results. It does not read "
            "market data, run G/H, change selector thresholds, or estimate counterfactual P&L.",
            "",
        ]
    )


def freeze_stage5e4_diagnostics(
    *,
    prelaunch_root: Path | str,
    midpoint_root: Path | str,
    output_root: Path | str,
    autoresearch_analysis_commit: str,
) -> dict[str, Any]:
    prelaunch = Path(prelaunch_root).resolve()
    midpoint_path = Path(midpoint_root).resolve()
    output = Path(output_root).resolve()
    if not _SHA40.fullmatch(autoresearch_analysis_commit):
        raise TemporalDiscoveryContractError(
            "AutoResearch analysis commit must be an exact lowercase commit SHA"
        )
    if output.exists():
        raise TemporalDiscoveryContractError("Stage 5E-4 output root must be absent")
    if output == prelaunch or prelaunch in output.parents:
        raise TemporalDiscoveryContractError("diagnostic output must be outside prelaunch root")
    if output == midpoint_path or midpoint_path in output.parents:
        raise TemporalDiscoveryContractError("diagnostic output must be outside midpoint root")
    midpoint_audit = audit_stage5e3_midpoint(midpoint_path)
    if (
        midpoint_audit.get("ok") is not True
        or midpoint_audit.get("status") != "mandatory_deep_review_g_and_h_blocked"
    ):
        raise TemporalDiscoveryContractError("source midpoint is not the frozen stop state")
    midpoint = _read(midpoint_path / "midpoint.json", name="source midpoint")
    source_midpoint_sha256 = _verify_embedded_identity(midpoint, "midpointSha256")
    if (
        midpoint.get("reservedEvidenceAccessed") is not False
        or midpoint.get("confirmationState", {}).get("taskCount") != 0
        or midpoint.get("largeSearchPermitted") is not False
    ):
        raise TemporalDiscoveryContractError("source midpoint crossed the E/F boundary")
    campaign = _read(prelaunch / "campaign-spec.json", name="campaign spec")
    population = _read(
        prelaunch / "generator-v2" / "population.json", name="population"
    )
    candidates = {
        str(row["candidateId"]): row for row in population.get("candidates") or []
    }
    if len(candidates) != 128:
        raise TemporalDiscoveryContractError("Stage 5E-4 requires the exact 128 candidates")
    integrity, raw_by_candidate = _result_integrity(
        root=prelaunch,
        run_root=prelaunch / "screening-run",
        population_ids=set(candidates),
        label_lookup=_window_label_lookup(campaign),
    )
    candidate_analysis = _read(
        midpoint_path / "candidate-analysis.json", name="candidate analysis"
    )
    screening_aggregates = _read(
        midpoint_path / "screening-aggregates.json", name="screening aggregates"
    )
    selector_envelope = _read(
        midpoint_path / "selector-envelope.json", name="selector envelope"
    )
    prior_activation = _read(
        midpoint_path / "activation.json", name="prior activation"
    )
    _verify_embedded_identity(candidate_analysis, "candidateAnalysisSha256")
    _verify_embedded_identity(screening_aggregates, "aggregateSetSha256")
    _verify_embedded_identity(selector_envelope, "selectorEnvelopeSha256")
    _verify_embedded_identity(prior_activation, "activationSha256")
    break_even = _build_break_even_audit(
        candidates=candidates,
        raw_by_candidate=raw_by_candidate,
        prior_activation=prior_activation,
        source_midpoint_sha256=source_midpoint_sha256,
    )
    cost = _build_cost_audit(
        candidates=candidates,
        raw_by_candidate=raw_by_candidate,
        candidate_analysis=candidate_analysis,
        screening_aggregates=screening_aggregates,
        selector_envelope=selector_envelope,
        source_midpoint_sha256=source_midpoint_sha256,
    )
    report = {
        "schemaVersion": DIAGNOSTIC_SCHEMA,
        "status": "stage5e4_read_only_diagnostics_complete_review_required",
        "autoresearchAnalysisCommit": autoresearch_analysis_commit,
        "sourceMidpointSha256": source_midpoint_sha256,
        "sourceMidpointManifestSha256": midpoint_audit["manifestSha256"],
        "screeningIntegrity": integrity,
        "breakEvenCausalAuditSha256": break_even["breakEvenCausalAuditSha256"],
        "costCarriageAuditSha256": cost["costCarriageAuditSha256"],
        "breakEvenSummary": {
            key: break_even[key]
            for key in (
                "logicalCandidateCount",
                "authoredTransitionInstanceCount",
                "priorMidpointReportedActivatedInstanceCount",
                "exactTransitionAttributedActivatedInstanceCount",
                "exactAppliedLogicalTradeCount",
                "postFill0_5RIntrabarExcursionOpportunityTradeCount",
                "currentRouteDidNotApplyOn0_5RIntrabarExcursionTradeCount",
                "causalBlockerWindowCounts",
                "thresholdReachSummary",
                "thresholdTimingAvailability",
            )
        },
        "costSummary": {
            key: cost[key]
            for key in (
                "candidateCount",
                "activeCandidateCount",
                "closedTradeCount",
                "grossR",
                "conservativeNetR",
                "costDragR",
                "grossExpectancyPerTrade",
                "conservativeExpectancyPerTrade",
                "costDragPerTrade",
                "economicsClassificationCounts",
                "tradeClassificationCounts",
                "frequencyQuartiles",
                "correlations",
            )
        },
        "evidenceBoundary": {
            "marketDataRead": False,
            "gatewayContacted": False,
            "newCandidateTaskCount": 0,
            "confirmationTaskCount": 0,
            "reservedEvidenceAccessed": False,
            "selectorChanged": False,
            "generatorChanged": False,
            "fuzzfolioChanged": False,
        },
        "nextPermittedOperation": "deep review and generator-v3 design decision only",
    }
    report["diagnosticCheckpointSha256"] = canonical_sha256(report)
    _write_immutable(output / "break-even-causal-audit.json", break_even)
    _write_immutable(output / "cost-carriage-audit.json", cost)
    _write_immutable(output / "checkpoint.json", report)
    _write_text_immutable(output / "checkpoint.md", _markdown(report))
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
        "schemaVersion": MANIFEST_SCHEMA,
        "diagnosticCheckpointSha256": report["diagnosticCheckpointSha256"],
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(output / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_search_stage5e4_diagnostic_result_v1",
        "status": report["status"],
        "diagnosticCheckpointSha256": report["diagnosticCheckpointSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "breakEvenExactActivatedInstanceCount": break_even[
            "exactTransitionAttributedActivatedInstanceCount"
        ],
        "closedTradeCount": cost["closedTradeCount"],
        "confirmationTaskCount": 0,
    }


def audit_stage5e4_diagnostics(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root).resolve()
    report = _read(root / "checkpoint.json", name="Stage 5E-4 checkpoint")
    supplied_report = _verify_embedded_identity(report, "diagnosticCheckpointSha256")
    manifest = _read(root / "manifest.json", name="Stage 5E-4 manifest")
    supplied_manifest = _verify_embedded_identity(manifest, "manifestSha256")
    if manifest.get("diagnosticCheckpointSha256") != supplied_report:
        raise TemporalDiscoveryContractError("checkpoint/manifest identity mismatch")
    expected = set()
    for row in manifest.get("files") or []:
        path = root / str(row["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(row["length"])
            or _file_sha256(path) != row["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"diagnostic file drift: {path}")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("diagnostic manifest inventory mismatch")
    break_even = _read(
        root / "break-even-causal-audit.json", name="break-even causal audit"
    )
    cost = _read(root / "cost-carriage-audit.json", name="cost carriage audit")
    if (
        _verify_embedded_identity(break_even, "breakEvenCausalAuditSha256")
        != report["breakEvenCausalAuditSha256"]
        or _verify_embedded_identity(cost, "costCarriageAuditSha256")
        != report["costCarriageAuditSha256"]
    ):
        raise TemporalDiscoveryContractError("diagnostic component identity mismatch")
    return {
        "schemaVersion": "temporal_search_stage5e4_diagnostic_audit_v1",
        "ok": True,
        "status": report["status"],
        "diagnosticCheckpointSha256": supplied_report,
        "manifestSha256": supplied_manifest,
        "fileCount": len(expected),
    }


__all__ = [
    "audit_stage5e4_diagnostics",
    "freeze_stage5e4_diagnostics",
]
