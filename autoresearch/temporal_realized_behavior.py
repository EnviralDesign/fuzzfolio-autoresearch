"""Deterministic, read-only realized execution behavior projections.

This module deliberately consumes replay evidence only.  It does not affect
candidate construction, replay execution, or selection.  The projections make
long/short behavior visible and provide a cost-inclusive identity for scrutiny
and duplicate reporting.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)


REALIZED_BEHAVIOR_SCHEMA = "temporal_realized_behavior_v1"
REALIZED_BEHAVIOR_IDENTITY_SCHEMA = "temporal_realized_behavior_identity_v1"
REALIZED_BEHAVIOR_FAMILY_SCHEMA = "temporal_realized_behavior_family_v1"
_SIDES = ("long", "short")


def _finite(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be numeric") from exc
    if not math.isfinite(number):
        raise TemporalDiscoveryContractError(f"{name} must be finite")
    return number


def _nonnegative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be an integer") from exc
    if number < 0:
        raise TemporalDiscoveryContractError(f"{name} must be nonnegative")
    return number


def _distribution(counts: Mapping[str, int]) -> dict[str, float]:
    total = sum(counts.values())
    if total <= 0:
        return {}
    return {key: counts[key] / total for key in sorted(counts)}


def _count(target: dict[str, int], key: Any) -> None:
    label = str(key or "unknown")
    target[label] = target.get(label, 0) + 1


def _trace_side(
    trace: Mapping[str, Any], *, trade_sides: Mapping[str, str], position_sides: Mapping[str, str]
) -> str | None:
    direction = trace.get("direction")
    if direction is not None:
        if direction not in _SIDES:
            raise TemporalDiscoveryContractError("replay trace direction must be long or short")
        return str(direction)
    for key, lookup in (("tradeId", trade_sides), ("positionId", position_sides)):
        value = trace.get(key)
        if value is not None and str(value) in lookup:
            return lookup[str(value)]
    return None


def _is_conflict_abstention(trace: Mapping[str, Any]) -> bool:
    values = " ".join(
        str(trace.get(key) or "").lower()
        for key in ("transitionId", "reasonCode", "actionKind", "status")
    )
    return "conflict" in values and ("abstain" in values or "reject" in values)


def build_window_realized_behavior(
    *,
    replay: Mapping[str, Any],
    metrics: Mapping[str, Any],
    window_id: str,
    allow_legacy_sparse: bool = False,
) -> dict[str, Any]:
    """Project one conservative replay into direction-attributable evidence.

    A replay with no materialized trades (some legacy fixtures/artifacts) is
    valid but reports no side activity.  Once a trade exposes direction or R
    economics, malformed values fail closed rather than silently moving it to
    an ``unknown`` bucket.
    """
    raw_trades = replay.get("trades") or []
    if not isinstance(raw_trades, list):
        raise TemporalDiscoveryContractError("conservative replay trades must be an array")
    reported_trades = _nonnegative_int(metrics.get("tradesClosed", 0), name="metrics tradesClosed")
    observations = _nonnegative_int(metrics.get("observationsProcessed", 0), name="metrics observationsProcessed")
    sides: dict[str, dict[str, Any]] = {
        side: {
            "closedTrades": 0, "wins": 0, "losses": 0, "flatTrades": 0,
            "grossR": 0.0, "netR": 0.0, "costR": 0.0,
            "holdingBars": 0, "holdingHours": 0.0,
            "closeReasonCounts": {}, "actionCounts": {}, "transitionCounts": {},
            "tradeSequence": [], "terminalStatusCounts": {},
            "terminalDirectionCount": 0, "conflictAbstentions": 0,
        }
        for side in _SIDES
    }
    trade_sides: dict[str, str] = {}
    position_sides: dict[str, str] = {}
    complete_trade_economics = bool(raw_trades) and len(raw_trades) == reported_trades
    unattributed_closed_trades = 0
    conflict_abstentions = 0
    unattributed_conflict_abstentions = 0
    for index, raw in enumerate(raw_trades):
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError(f"replay trades[{index}] must be an object")
        direction = raw.get("direction")
        if direction not in _SIDES:
            if allow_legacy_sparse and direction is None:
                # Older closed-trade-only blobs predate direction attribution.
                # Keep them readable, explicitly unsupported for side metrics.
                unattributed_closed_trades += 1
                complete_trade_economics = False
                continue
            raise TemporalDiscoveryContractError(f"replay trades[{index}] direction must be long or short")
        side = sides[str(direction)]
        trade_id = raw.get("tradeId")
        position_id = raw.get("positionId")
        if trade_id is not None:
            trade_sides[str(trade_id)] = str(direction)
        if position_id is not None:
            position_sides[str(position_id)] = str(direction)
        gross_present, net_present = "grossR" in raw, "netR" in raw
        if gross_present != net_present:
            raise TemporalDiscoveryContractError(f"replay trades[{index}] grossR/netR must be paired")
        if gross_present:
            gross_r = _finite(raw["grossR"], name=f"replay trades[{index}] grossR")
            net_r = _finite(raw["netR"], name=f"replay trades[{index}] netR")
        else:
            complete_trade_economics = False
            gross_r = net_r = 0.0
        holding = _nonnegative_int(raw.get("holdingBars", 0), name=f"replay trades[{index}] holdingBars")
        hours = _finite(raw.get("holdingHours", 0.0), name=f"replay trades[{index}] holdingHours")
        if hours < 0.0:
            raise TemporalDiscoveryContractError(f"replay trades[{index}] holdingHours must be nonnegative")
        side["closedTrades"] += 1
        side["grossR"] += gross_r
        side["netR"] += net_r
        side["costR"] += gross_r - net_r
        side["holdingBars"] += holding
        side["holdingHours"] += hours
        if net_r > 0.0:
            side["wins"] += 1
        elif net_r < 0.0:
            side["losses"] += 1
        else:
            side["flatTrades"] += 1
        _count(side["closeReasonCounts"], raw.get("closeReason"))
        side["tradeSequence"].append({
            "entryClockIndex": _nonnegative_int(raw.get("entryClockIndex", 0), name=f"replay trades[{index}] entryClockIndex"),
            "exitClockIndex": _nonnegative_int(raw.get("exitClockIndex", 0), name=f"replay trades[{index}] exitClockIndex"),
            "entryTime": raw.get("entryTime"), "exitTime": raw.get("exitTime"),
            "holdingBars": holding, "holdingHours": hours,
            "closeReason": str(raw.get("closeReason") or "unknown"),
            "grossR": gross_r, "netR": net_r,
        })

    for key, label in (("executionTraces", "actionKind"), ("graphTraces", "transitionId")):
        traces = replay.get(key) or []
        if not isinstance(traces, list):
            raise TemporalDiscoveryContractError(f"conservative replay {key} must be an array")
        for index, trace in enumerate(traces):
            if not isinstance(trace, Mapping):
                raise TemporalDiscoveryContractError(f"conservative replay {key}[{index}] must be an object")
            side_name = _trace_side(trace, trade_sides=trade_sides, position_sides=position_sides)
            if side_name is not None and trace.get(label) is not None:
                _count(sides[side_name]["actionCounts" if key == "executionTraces" else "transitionCounts"], trace.get(label))
            if _is_conflict_abstention(trace):
                conflict_abstentions += 1
                if side_name is None:
                    unattributed_conflict_abstentions += 1
                else:
                    sides[side_name]["conflictAbstentions"] += 1

    terminal = metrics.get("terminalValuation")
    terminal_summary: dict[str, Any] = {
        "positionStatus": "unavailable",
        "direction": None,
    }
    if terminal is not None:
        if not isinstance(terminal, Mapping):
            raise TemporalDiscoveryContractError("metrics terminalValuation must be an object")
        terminal_status = str(terminal.get("positionStatus") or "unknown")
        terminal_direction = terminal.get("direction")
        terminal_summary = {
            "positionStatus": terminal_status,
            "direction": terminal_direction,
        }
        if terminal_direction is not None:
            if terminal_direction not in _SIDES:
                raise TemporalDiscoveryContractError("terminal direction must be long or short")
            side = sides[str(terminal_direction)]
            side["terminalDirectionCount"] += 1
            _count(side["terminalStatusCounts"], terminal_status)
            gross = _finite(terminal.get("grossR"), name="terminal grossR")
            net = _finite(terminal.get("netR"), name="terminal netR")
            side["grossR"] += gross
            side["netR"] += net
            side["costR"] += gross - net
        elif terminal_status not in {"no_open_position", "none", "unknown"}:
            # A terminal position status without an attributable direction is
            # unsafe to present as a side metric.
            raise TemporalDiscoveryContractError("terminal position status requires a direction")

    if complete_trade_economics:
        expected_gross = _finite(metrics.get("totalGrossR"), name="metrics totalGrossR")
        expected_net = _finite(metrics.get("totalNetR"), name="metrics totalNetR")
        closed_gross = math.fsum(side["grossR"] for side in sides.values())
        closed_net = math.fsum(side["netR"] for side in sides.values())
        # Terminal economics have already been included above, so remove them
        # for the raw closed-trade reconciliation.
        terminal_gross = sum(
            _finite((metrics.get("terminalValuation") or {}).get("grossR"), name="terminal grossR")
            for side in _SIDES
            if (metrics.get("terminalValuation") or {}).get("direction") == side
        )
        terminal_net = sum(
            _finite((metrics.get("terminalValuation") or {}).get("netR"), name="terminal netR")
            for side in _SIDES
            if (metrics.get("terminalValuation") or {}).get("direction") == side
        )
        if not math.isclose(closed_gross - terminal_gross, expected_gross, abs_tol=1e-9) or not math.isclose(closed_net - terminal_net, expected_net, abs_tol=1e-9):
            raise TemporalDiscoveryContractError("replay trade economics do not reconcile with metrics")

    for side in _SIDES:
        row = sides[side]
        row["active"] = bool(row["closedTrades"] or row["terminalDirectionCount"])
        row["exposureProxy"] = row["holdingBars"] / observations if observations else 0.0
        row["averageHoldingBars"] = row["holdingBars"] / row["closedTrades"] if row["closedTrades"] else 0.0
        row["closeReasonDistribution"] = _distribution(row["closeReasonCounts"])
        row["actionDistribution"] = _distribution(row["actionCounts"])
        row["transitionDistribution"] = _distribution(row["transitionCounts"])
    result = {
        "schemaVersion": REALIZED_BEHAVIOR_SCHEMA,
        "windowId": str(window_id),
        "reportedClosedTrades": reported_trades,
        "materializedClosedTrades": len(raw_trades),
        "unattributedClosedTrades": unattributed_closed_trades,
        "observations": observations,
        "terminal": terminal_summary,
        "conflictAbstentions": conflict_abstentions,
        "unattributedConflictAbstentions": unattributed_conflict_abstentions,
        "sides": sides,
    }
    return result


def aggregate_realized_behavior(windows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not windows:
        raise TemporalDiscoveryContractError("realized behavior requires at least one window")
    side_rows: dict[str, dict[str, Any]] = {
        side: {
            "closedTrades": 0, "wins": 0, "losses": 0, "flatTrades": 0,
            "grossR": 0.0, "netR": 0.0, "costR": 0.0, "holdingBars": 0,
            "holdingHours": 0.0, "activeWindowCount": 0, "closeReasonCounts": {},
            "actionCounts": {}, "transitionCounts": {}, "terminalStatusCounts": {},
            "terminalDirectionCount": 0, "conflictAbstentions": 0, "tradeSequence": [],
        } for side in _SIDES
    }
    total_observations = 0
    terminal_status_counts: dict[str, int] = {}
    terminal_direction_counts: dict[str, int] = {}
    conflict_abstentions = 0
    unattributed_conflict_abstentions = 0
    unattributed_closed_trades = 0
    window_signatures: list[dict[str, Any]] = []
    for index, window in enumerate(windows):
        behavior = window.get("realizedBehavior")
        if not isinstance(behavior, Mapping) or behavior.get("schemaVersion") != REALIZED_BEHAVIOR_SCHEMA:
            raise TemporalDiscoveryContractError(f"window {index} has no valid realized behavior")
        total_observations += _nonnegative_int(behavior.get("observations", 0), name="behavior observations")
        terminal = behavior.get("terminal") or {}
        if not isinstance(terminal, Mapping):
            raise TemporalDiscoveryContractError(f"window {index} terminal behavior is invalid")
        _count(terminal_status_counts, terminal.get("positionStatus"))
        terminal_direction = terminal.get("direction")
        if terminal_direction is not None:
            if terminal_direction not in _SIDES:
                raise TemporalDiscoveryContractError(f"window {index} terminal direction is invalid")
            _count(terminal_direction_counts, terminal_direction)
        conflict_abstentions += _nonnegative_int(behavior.get("conflictAbstentions", 0), name="behavior conflict abstentions")
        unattributed_conflict_abstentions += _nonnegative_int(behavior.get("unattributedConflictAbstentions", 0), name="behavior unattributed conflict abstentions")
        unattributed_closed = _nonnegative_int(behavior.get("unattributedClosedTrades", 0), name="behavior unattributed closed trades")
        signature_sides: dict[str, Any] = {}
        for side in _SIDES:
            source = (behavior.get("sides") or {}).get(side)
            if not isinstance(source, Mapping):
                raise TemporalDiscoveryContractError(f"window {index} realized behavior {side} side is invalid")
            target = side_rows[side]
            for key in ("closedTrades", "wins", "losses", "flatTrades", "holdingBars", "terminalDirectionCount", "conflictAbstentions"):
                target[key] += _nonnegative_int(source.get(key, 0), name=f"behavior {side} {key}")
            for key in ("grossR", "netR", "costR", "holdingHours"):
                target[key] += _finite(source.get(key, 0.0), name=f"behavior {side} {key}")
            if source.get("active") is True:
                target["activeWindowCount"] += 1
            for target_key in ("closeReasonCounts", "actionCounts", "transitionCounts", "terminalStatusCounts"):
                source_counts = source.get(target_key) or {}
                if not isinstance(source_counts, Mapping):
                    raise TemporalDiscoveryContractError(f"behavior {side} {target_key} must be an object")
                for name, count in source_counts.items():
                    target[target_key][str(name)] = target[target_key].get(str(name), 0) + _nonnegative_int(count, name=f"behavior {side} count")
            sequence = source.get("tradeSequence") or []
            if not isinstance(sequence, list):
                raise TemporalDiscoveryContractError(f"behavior {side} tradeSequence must be an array")
            if not all(isinstance(item, Mapping) for item in sequence):
                raise TemporalDiscoveryContractError(f"behavior {side} tradeSequence rows must be objects")
            target["tradeSequence"].extend(
                {"windowOrdinal": index, **dict(item)} for item in sequence
            )
            signature_sides[side] = {
                "active": source.get("active") is True,
                "tradeSequence": sequence,
                "actionDistribution": source.get("actionDistribution") or {},
                "transitionDistribution": source.get("transitionDistribution") or {},
                "closeReasonDistribution": source.get("closeReasonDistribution") or {},
                "terminalStatusCounts": source.get("terminalStatusCounts") or {},
                "conflictAbstentions": source.get("conflictAbstentions", 0),
            }
        # The count is window-level, not a side attribution.
        unattributed_closed_trades += unattributed_closed
        window_signatures.append({"windowOrdinal": index, "sides": signature_sides})
    for side in _SIDES:
        row = side_rows[side]
        if not math.isclose(row["grossR"] - row["netR"], row["costR"], abs_tol=1e-9):
            raise TemporalDiscoveryContractError(f"realized behavior {side} cost R does not reconcile")
        row["active"] = bool(row["closedTrades"] or row["terminalDirectionCount"])
        row["activeWindowFraction"] = row["activeWindowCount"] / len(windows)
        row["exposureProxy"] = row["holdingBars"] / total_observations if total_observations else 0.0
        row["averageHoldingBars"] = row["holdingBars"] / row["closedTrades"] if row["closedTrades"] else 0.0
        row["closeReasonDistribution"] = _distribution(row["closeReasonCounts"])
        row["actionDistribution"] = _distribution(row["actionCounts"])
        row["transitionDistribution"] = _distribution(row["transitionCounts"])
    identity_material = {
        "schemaVersion": REALIZED_BEHAVIOR_IDENTITY_SCHEMA,
        "totalObservations": total_observations,
        "windowSignatures": window_signatures,
        "terminalStatusCounts": terminal_status_counts,
        "terminalDirectionCounts": terminal_direction_counts,
        "conflictAbstentions": conflict_abstentions,
        "unattributedConflictAbstentions": unattributed_conflict_abstentions,
        "unattributedClosedTrades": unattributed_closed_trades,
        "sides": {
            side: {
                key: side_rows[side][key]
                for key in ("closedTrades", "wins", "losses", "flatTrades", "grossR", "netR", "costR", "holdingBars", "holdingHours", "active", "activeWindowCount", "exposureProxy", "terminalDirectionCount", "conflictAbstentions", "closeReasonDistribution", "actionDistribution", "transitionDistribution", "terminalStatusCounts")
            }
            for side in _SIDES
        },
    }
    return {
        "schemaVersion": REALIZED_BEHAVIOR_SCHEMA,
        "windowCount": len(windows),
        "totalObservations": total_observations,
        "terminalStatusCounts": terminal_status_counts,
        "terminalDirectionCounts": terminal_direction_counts,
        "conflictAbstentions": conflict_abstentions,
        "unattributedConflictAbstentions": unattributed_conflict_abstentions,
        "unattributedClosedTrades": unattributed_closed_trades,
        "sides": side_rows,
        "identityMaterial": identity_material,
        "identitySha256": canonical_sha256(identity_material),
    }


def validate_aggregate_realized_behavior_identity(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the aggregate side facts named by a realized-behavior hash."""
    if not isinstance(value, Mapping) or value.get("schemaVersion") != REALIZED_BEHAVIOR_SCHEMA:
        raise TemporalDiscoveryContractError("realized behavior schema is unsupported")
    identity = value.get("identityMaterial")
    if not isinstance(identity, Mapping) or canonical_sha256(identity) != value.get("identitySha256"):
        raise TemporalDiscoveryContractError("realized behavior identity mismatch")
    bound_sides = identity.get("sides")
    sides = value.get("sides")
    if not isinstance(bound_sides, Mapping) or not isinstance(sides, Mapping):
        raise TemporalDiscoveryContractError("realized behavior sides are invalid")
    fields = (
        "closedTrades", "wins", "losses", "flatTrades", "grossR", "netR", "costR",
        "holdingBars", "holdingHours", "active", "activeWindowCount", "exposureProxy",
        "terminalDirectionCount", "conflictAbstentions", "closeReasonDistribution",
        "actionDistribution", "transitionDistribution", "terminalStatusCounts",
    )
    for side in _SIDES:
        bound = bound_sides.get(side)
        observed = sides.get(side)
        if not isinstance(bound, Mapping) or not isinstance(observed, Mapping):
            raise TemporalDiscoveryContractError("realized behavior side is invalid")
        if any(bound.get(field) != observed.get(field) for field in fields):
            raise TemporalDiscoveryContractError("realized behavior side identity drifted")
    return dict(value)


def behavior_family_clusters(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Group equal realized identities for reporting; never delete candidates."""
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for index, candidate in enumerate(candidates):
        behavior = candidate.get("realizedBehavior")
        if not isinstance(behavior, Mapping):
            raise TemporalDiscoveryContractError(f"candidate {index} has no realized behavior")
        identity = behavior.get("identitySha256")
        if not isinstance(identity, str) or not identity.startswith("sha256:"):
            raise TemporalDiscoveryContractError(f"candidate {index} realized behavior identity is invalid")
        grouped.setdefault(identity, []).append(candidate)
    return [
        {
            "schemaVersion": REALIZED_BEHAVIOR_FAMILY_SCHEMA,
            "familyId": identity,
            "behaviorIdentitySha256": identity,
            "memberCount": len(members),
            "memberCandidateIds": sorted(str(member.get("candidateId") or "") for member in members),
            "exactProgramSha256s": sorted({str(member.get("resolvedProgramSha256") or member.get("programSha256") or "") for member in members}),
        }
        for identity, members in sorted(grouped.items())
    ]
