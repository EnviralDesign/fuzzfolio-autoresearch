"""Observational post-generation quality audit for native temporal QD v5 runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_direction_selection import (
    DEFAULT_DIRECTION_SELECTION_POLICY,
    LANE_HARMFUL_OPPOSITE_SIDE,
    classify_direction_selection,
)
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_rotating_evidence import robust_breeder_policy
from .temporal_qd_v5_control_plane import (
    _read_canonical_object,
    _real_path,
)

AUDIT_SCHEMA = "temporal_qd_generation_quality_audit_v1"
RUN_AUDIT_SCHEMA = "temporal_qd_run_quality_audit_v1"
NEAR_MISS_CAP = 32
COUNTERFACTUAL_CAPS = (128, 192, 256)

_LOG = logging.getLogger(__name__)

_DEFAULT_ROBUST = robust_breeder_policy()
_ACTIVE_WINDOW_FRACTION = float(_DEFAULT_ROBUST["minimumActiveWindowFraction"])
_TRADES_PER_MONTH = float(_DEFAULT_ROBUST["minimumAverageClosedTradesPerCandidateMonth"])

_PREFINALIZER_ROUND_DIR_NAMES = ("base-v2", "fast-base")
_SCREEN_CLASS_NAMES = (
    "UNSUPPORTED",
    "SUPPORTED_OTHER",
    "SUPPORTED_ECONOMIC_NEAR_MISS",
    "CURRENT_FRONTIER_READY",
    "CURRENT_QUALITY_READY",
)


def _write_canonical(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def _self_hash_audit(body: dict[str, Any]) -> dict[str, Any]:
    output = dict(body)
    output.pop("auditSha256", None)
    output["auditSha256"] = canonical_sha256(output)
    return output


def _generation_root(run_root: Path | str, generation_index: int) -> Path:
    return (
        Path(run_root).resolve()
        / "generations"
        / f"generation-{generation_index:04d}"
    )


def _read_jsonl(path: Path, *, name: str) -> list[dict[str, Any]]:
    checked = _real_path(path, name=name)
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for line_number, raw in enumerate(
        checked.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TemporalDiscoveryContractError(
                f"{name} line {line_number} is not valid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise TemporalDiscoveryContractError(
                f"{name} line {line_number} must be an object"
            )
        candidate_id = value.get("candidateId")
        if isinstance(candidate_id, str) and candidate_id:
            if candidate_id in seen_ids:
                raise TemporalDiscoveryContractError(
                    f"{name} contains duplicate candidateId {candidate_id}"
                )
            seen_ids.add(candidate_id)
        rows.append(value)
    return rows


def _optional_json(path: Path, *, name: str) -> dict[str, Any] | None:
    try:
        return _read_canonical_object(path, name=name)
    except Exception:
        return None


def _resolve_generation_file(
    generation_root: Path,
    relative: str,
    *,
    name: str,
) -> Path:
    return _real_path(generation_root / relative, name=name)


def _native_finalization_root(generation_root: Path) -> Path:
    candidate = generation_root / "native-finalization"
    if candidate.is_dir():
        return candidate
    return candidate


def _discover_latest_prefinalizer_round(generation_root: Path) -> Path | None:
    prefinalizer_root = generation_root / "prefinalizer"
    if not prefinalizer_root.is_dir():
        return None
    candidates: list[tuple[int, str, Path]] = []
    for child in prefinalizer_root.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        round_index = 0
        if name in _PREFINALIZER_ROUND_DIR_NAMES:
            round_index = 0
        elif name.startswith("round-"):
            try:
                round_index = int(name.removeprefix("round-"))
            except ValueError:
                continue
        elif name.startswith("fast-round-"):
            try:
                round_index = int(name.removeprefix("fast-round-"))
            except ValueError:
                continue
        else:
            continue
        result_path = child / "result.json"
        receipt_path = child / "execution-receipt.json"
        if not result_path.is_file() or not receipt_path.is_file():
            continue
        try:
            result = _read_canonical_object(result_path, name="prefinalizer result")
            receipt = _read_canonical_object(
                receipt_path, name="prefinalizer execution receipt"
            )
        except Exception:
            continue
        if result.get("status") != "ready_for_finalizer":
            continue
        if not isinstance(receipt, Mapping):
            continue
        candidates.append((round_index, name, child))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[-1][2]


def _load_threshold_policies(
    archive: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], Any]:
    direction_policy = DEFAULT_DIRECTION_SELECTION_POLICY
    robust = dict(_DEFAULT_ROBUST)
    if archive is None:
        return robust, direction_policy
    frozen = archive.get("frozenPolicy")
    if isinstance(frozen, Mapping):
        rotating = frozen.get("rotatingEvidence")
        if isinstance(rotating, Mapping):
            breeder = rotating.get("robustBreederPolicy")
            if isinstance(breeder, Mapping):
                robust = robust_breeder_policy(breeder)
        direction = frozen.get("directionSelectionPolicy")
        if isinstance(direction, Mapping):
            direction_policy = DEFAULT_DIRECTION_SELECTION_POLICY.__class__(
                **{
                    key: direction[key]
                    for key in (
                        "minimum_closed_trades_per_side",
                        "minimum_active_windows_per_side",
                        "minimum_acceptable_side_net_r",
                        "harmful_opposite_net_r",
                    )
                    if key in direction
                }
            )
    return robust, direction_policy


def _member_field(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    candidate = row.get("candidate")
    if isinstance(candidate, Mapping):
        for key in keys:
            if key in candidate:
                return candidate[key]
    aggregate = row.get("aggregate")
    if isinstance(aggregate, Mapping):
        for key in keys:
            if key in aggregate:
                return aggregate[key]
    return None


def _window_metrics_from_evaluated(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    aggregate = row.get("aggregate")
    if not isinstance(aggregate, Mapping):
        aggregate = row
    records = aggregate.get("windowRecords")
    if not isinstance(records, list):
        return []
    windows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        metrics = record.get("metrics")
        if isinstance(metrics, Mapping):
            source = metrics
        else:
            source = record
        try:
            windows.append(
                {
                    "conservativeNetR": float(
                        source.get("conservativeNetR", source.get("netR", 0.0))
                    ),
                    "closedTrades": int(source.get("closedTrades", source.get("trades", 0))),
                    "maxDrawdownR": float(source.get("maxDrawdownR", 0.0)),
                }
            )
        except (TypeError, ValueError):
            continue
    return windows


def _support_metrics(
    windows: Sequence[Mapping[str, Any]], *, covered_months: float
) -> dict[str, float]:
    if not windows:
        return {
            "activeWindowFraction": 0.0,
            "averageTradesPerMonth": 0.0,
            "cumulativeConservativeNetR": 0.0,
            "medianWindowConservativeNetR": 0.0,
        }
    active = sum(int(item.get("closedTrades") or 0) > 0 for item in windows)
    trades = sum(float(item.get("closedTrades") or 0.0) for item in windows)
    nets = [float(item.get("conservativeNetR") or 0.0) for item in windows]
    months = covered_months if covered_months > 0 else max(len(windows), 1)
    return {
        "activeWindowFraction": active / len(windows),
        "averageTradesPerMonth": trades / months,
        "cumulativeConservativeNetR": sum(nets),
        "medianWindowConservativeNetR": float(statistics.median(nets)),
    }


def _direction_selection_from_row(row: Mapping[str, Any], *, policy: Any) -> dict[str, Any] | None:
    aggregate = row.get("aggregate")
    if not isinstance(aggregate, Mapping):
        aggregate = row
    behavior = aggregate.get("realizedBehavior")
    if not isinstance(behavior, Mapping):
        return None
    try:
        return classify_direction_selection(behavior, policy=policy)
    except TemporalDiscoveryContractError:
        return None


def _direction_failure_bucket(selection: Mapping[str, Any]) -> str:
    if selection.get("selectionEligible") is True:
        raise ValueError("direction-eligible candidate has no direction failure")
    lane = selection.get("lane")
    if lane == LANE_HARMFUL_OPPOSITE_SIDE:
        return "failedDirectionHarmfulOpposite"
    sides = selection.get("sides")
    if isinstance(sides, Mapping):
        long_side = sides.get("long")
        short_side = sides.get("short")
        if (
            isinstance(long_side, Mapping)
            and isinstance(short_side, Mapping)
            and long_side.get("supported")
            and short_side.get("supported")
        ):
            return "failedDirectionMildNegativeOpposite"
    return "failedDirectionNoNonnegativeSide"


def _gate_flags(
    row: Mapping[str, Any],
    *,
    robust_policy: Mapping[str, Any],
    direction_policy: Any,
    windows: Sequence[Mapping[str, Any]] | None = None,
    covered_months: float | None = None,
) -> dict[str, bool]:
    if windows is None:
        windows = _window_metrics_from_evaluated(row)
    months = covered_months
    if months is None:
        months = float(_member_field(row, "coveredMonths") or max(len(windows), 1))
    metrics = _support_metrics(windows, covered_months=months)
    min_active = float(robust_policy["minimumActiveWindowFraction"])
    min_trades = float(robust_policy["minimumAverageClosedTradesPerCandidateMonth"])
    active_pass = metrics["activeWindowFraction"] >= min_active
    trades_pass = metrics["averageTradesPerMonth"] >= min_trades
    combined = active_pass and trades_pass
    direction = _direction_selection_from_row(row, policy=direction_policy)
    direction_eligible = direction is None or direction.get("selectionEligible") is True
    cumulative_positive = metrics["cumulativeConservativeNetR"] > 0
    median_positive = metrics["medianWindowConservativeNetR"] > 0
    quality_like = combined and direction_eligible and cumulative_positive and median_positive
    frontier_like = combined and direction_eligible and not quality_like
    return {
        "activeWindowFractionPass": active_pass,
        "averageTradesPerMonthPass": trades_pass,
        "combinedSupportPass": combined,
        "directionEligible": direction_eligible,
        "cumulativeNetPositive": cumulative_positive,
        "medianWindowNetPositive": median_positive,
        "currentPanelQualityLike": quality_like,
        "currentPanelFrontierLike": frontier_like,
        "_metrics": metrics,
        "_direction": direction,
    }


def _positive_economic_condition_count(flags: Mapping[str, bool]) -> int:
    return sum(
        1
        for key in (
            "cumulativeNetPositive",
            "medianWindowNetPositive",
            "directionEligible",
        )
        if flags.get(key)
    )


def _screen_class(flags: Mapping[str, bool]) -> int:
    if flags.get("currentPanelQualityLike"):
        return 4
    if flags.get("currentPanelFrontierLike"):
        return 3
    if flags.get("combinedSupportPass") and (
        flags.get("cumulativeNetPositive") or flags.get("medianWindowNetPositive")
    ):
        return 2
    if flags.get("combinedSupportPass"):
        return 1
    return 0


def _parse_parent_material(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for row in rows:
        pair_payload = row.get("pairPayload")
        proposal_delta = (
            pair_payload.get("proposalDelta")
            if isinstance(pair_payload, Mapping)
            else None
        )
        origin_kind = None
        scheduled_kind = None
        if isinstance(proposal_delta, Mapping):
            origin_kind = proposal_delta.get("originKind")
            scheduled_kind = proposal_delta.get("scheduledKind")
        parent = row.get("parent") if isinstance(row.get("parent"), Mapping) else {}
        mate = row.get("mate") if isinstance(row.get("mate"), Mapping) else {}
        operators: list[str] = []
        steps = row.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, Mapping):
                    continue
                application = step.get("application")
                if not isinstance(application, Mapping):
                    continue
                audit = application.get("applicationAudit")
                if not isinstance(audit, Mapping):
                    continue
                operator_id = audit.get("operatorId")
                if isinstance(operator_id, str) and operator_id:
                    operators.append(operator_id)
        parsed.append(
            {
                "candidateId": row.get("candidateId"),
                "originKind": origin_kind,
                "scheduledKind": scheduled_kind,
                "parentCandidateId": parent.get("candidateId"),
                "parentCandidateIdentitySha256": parent.get("candidateIdentitySha256"),
                "mateCandidateId": mate.get("candidateId"),
                "mutationDepth": row.get("mutationDepth"),
                "operatorIds": operators,
                "operatorSequence": " > ".join(operators) if operators else "",
                "terminalDisposition": row.get("terminalDisposition"),
                "terminalReasonCode": row.get("terminalReasonCode"),
                "proposalOrdinal": row.get("proposalOrdinal"),
            }
        )
    return parsed


def _index_evaluated(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate_id = _member_field(row, "candidateId")
        if not isinstance(candidate_id, str) or not candidate_id:
            raise TemporalDiscoveryContractError("evaluated member lacks candidateId")
        if candidate_id in indexed:
            raise TemporalDiscoveryContractError(
                f"evaluated members contain duplicate candidateId {candidate_id}"
            )
        indexed[candidate_id] = row
    return indexed


def _archive_member_index(archive: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    cells = archive.get("cells")
    if not isinstance(cells, list):
        return indexed
    for cell in cells:
        if not isinstance(cell, Mapping):
            continue
        members = cell.get("members")
        if not isinstance(members, list):
            continue
        for member in members:
            if not isinstance(member, Mapping):
                continue
            candidate_id = member.get("candidateId")
            if isinstance(candidate_id, str) and candidate_id:
                indexed[candidate_id] = {
                    **member,
                    "cellId": cell.get("cellId"),
                    "archiveLane": member.get("archiveLane"),
                }
    return indexed


def _cumulative_member_index(
    cumulative: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    members = cumulative.get("members")
    if not isinstance(members, list):
        return indexed
    for member in members:
        if not isinstance(member, Mapping):
            continue
        candidate_id = member.get("candidateId")
        if isinstance(candidate_id, str) and candidate_id:
            indexed[candidate_id] = member
    return indexed


def _attempt_telemetry(
    generation_root: Path, generation_index: int
) -> tuple[bool, dict[str, Any]]:
    prefinalizer = generation_root / "prefinalizer"
    evolved_attempts = prefinalizer / "proposal-attempts" / "proposal-attempts.jsonl"
    evolved_receipt = prefinalizer / "proposal-attempts" / "proposal-attempts-receipt.json"
    g0_attempts = prefinalizer / "g0-selected-proposal-attempts.jsonl"
    g0_receipt = prefinalizer / "g0-selected-attempts-receipt.json"
    attempts_path: Path | None = None
    if evolved_attempts.is_file() and evolved_receipt.is_file():
        attempts_path = evolved_attempts
    elif generation_index == 1 and g0_attempts.is_file() and g0_receipt.is_file():
        attempts_path = g0_attempts
    if attempts_path is None:
        return False, {}
    rows = _read_jsonl(attempts_path, name="proposal attempts")
    accepted = rejected = no_op = 0
    reasons: dict[str, int] = defaultdict(int)
    by_operator: dict[str, int] = defaultdict(int)
    by_depth: dict[str, int] = defaultdict(int)
    for row in rows:
        disposition = str(row.get("disposition") or "unknown")
        reasons[disposition] += 1
        if disposition in {"accepted", "materialized", "selected"}:
            accepted += 1
        elif disposition in {"no_op_proposal", "operation_rejected"}:
            no_op += 1
        else:
            rejected += 1
        operator = row.get("operatorId")
        if isinstance(operator, str) and operator:
            by_operator[operator] += 1
        depth = row.get("mutationDepth")
        if depth is not None:
            by_depth[str(depth)] += 1
    return True, {
        "attemptCount": len(rows),
        "acceptedAttemptCount": accepted,
        "rejectedAttemptCount": rejected,
        "noOpAttemptCount": no_op,
        "attemptReasons": dict(sorted(reasons.items())),
        "attemptsByOperator": dict(sorted(by_operator.items())),
        "attemptsByMutationDepth": dict(sorted(by_depth.items())),
    }


def _quantiles(values: Sequence[float]) -> dict[str, float | None] | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        value = ordered[0]
        return {key: value for key in ("min", "p10", "p25", "median", "p75", "p90", "max")}

    def q(frac: float) -> float:
        index = max(0, min(len(ordered) - 1, int(round(frac * (len(ordered) - 1)))))
        return ordered[index]

    return {
        "min": ordered[0],
        "p10": q(0.10),
        "p25": q(0.25),
        "median": statistics.median(ordered),
        "p75": q(0.75),
        "p90": q(0.90),
        "max": ordered[-1],
    }


def _inverse_simpson(clusters: Mapping[str, int]) -> float:
    total = sum(clusters.values())
    if total <= 0:
        return 0.0
    fractions = [count / total for count in clusters.values()]
    denominator = sum(fraction * fraction for fraction in fractions)
    if denominator <= 0:
        return 0.0
    return 1.0 / denominator


def _simulate_counterfactual_shortlist(
    evaluated: dict[str, dict[str, Any]],
    *,
    robust_policy: Mapping[str, Any],
    direction_policy: Any,
    cap: int,
) -> dict[str, Any]:
    by_cell: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for candidate_id, row in evaluated.items():
        cell_id = str(_member_field(row, "cellId") or "unknown")
        flags = _gate_flags(
            row, robust_policy=robust_policy, direction_policy=direction_policy
        )
        by_cell[cell_id].append((candidate_id, flags))
    for cell_id in by_cell:
        by_cell[cell_id].sort(
            key=lambda item: (
                -_screen_class(item[1]),
                -_positive_economic_condition_count(item[1]),
                0 if item[1].get("directionEligible") else 1,
                -item[1]["_metrics"]["activeWindowFraction"],
                -item[1]["_metrics"]["averageTradesPerMonth"],
                -item[1]["_metrics"]["cumulativeConservativeNetR"],
                item[0],
            )
        )

    def cell_key(cell_id: str) -> tuple[Any, ...]:
        best = by_cell[cell_id][0][1]
        return (
            -_screen_class(best),
            -_positive_economic_condition_count(best),
            0 if best.get("directionEligible") else 1,
            -best["_metrics"]["activeWindowFraction"],
            -best["_metrics"]["averageTradesPerMonth"],
            -best["_metrics"]["cumulativeConservativeNetR"],
            cell_id,
        )

    ordered_cells = sorted(by_cell, key=cell_key)
    pointers = {cell_id: 0 for cell_id in ordered_cells}
    selected: list[str] = []
    while len(selected) < cap:
        added = False
        for cell_id in ordered_cells:
            if len(selected) >= cap:
                break
            index = pointers[cell_id]
            members = by_cell[cell_id]
            if index < len(members):
                selected.append(members[index][0])
                pointers[cell_id] = index + 1
                added = True
        if not added:
            break
    quality_available = sum(
        1 for row in evaluated.values() if _gate_flags(row, robust_policy=robust_policy, direction_policy=direction_policy).get("currentPanelQualityLike")
    )
    frontier_available = sum(
        1 for row in evaluated.values() if _gate_flags(row, robust_policy=robust_policy, direction_policy=direction_policy).get("currentPanelFrontierLike")
    )
    selected_flags = [
        _gate_flags(evaluated[cid], robust_policy=robust_policy, direction_policy=direction_policy)
        for cid in selected
    ]
    quality_selected = sum(1 for flags in selected_flags if flags.get("currentPanelQualityLike"))
    frontier_selected = sum(1 for flags in selected_flags if flags.get("currentPanelFrontierLike"))
    return {
        "cap": cap,
        "occupiedCellsSelected": len({str(_member_field(evaluated[cid], "cellId") or "unknown") for cid in selected}),
        "currentQualityLikeAvailable": quality_available,
        "currentQualityLikeSelected": quality_selected,
        "currentFrontierLikeAvailable": frontier_available,
        "currentFrontierLikeSelected": frontier_selected,
        "qualityRecall": (quality_selected / quality_available) if quality_available else None,
        "frontierRecall": (frontier_selected / frontier_available) if frontier_available else None,
    }


def _aggregate_gate_counts(
    rows: dict[str, dict[str, Any]],
    *,
    robust_policy: Mapping[str, Any],
    direction_policy: Any,
) -> dict[str, int]:
    keys = (
        "activeWindowFractionPass",
        "averageTradesPerMonthPass",
        "combinedSupportPass",
        "directionEligible",
        "cumulativeNetPositive",
        "medianWindowNetPositive",
        "currentPanelQualityLike",
        "currentPanelFrontierLike",
    )
    counts = {key: 0 for key in keys}
    for row in rows.values():
        flags = _gate_flags(row, robust_policy=robust_policy, direction_policy=direction_policy)
        for key in keys:
            if flags.get(key):
                counts[key] += 1
    return counts


def _origin_kind(row: Mapping[str, Any]) -> str:
    origin = row.get("originKind") or row.get("scheduledKind")
    if isinstance(origin, str) and origin:
        if "immigrant" in origin:
            return "immigrant"
        if "offspring" in origin or "mutation" in origin or "crossover" in origin:
            return "offspring"
        if "parent" in origin or "retained" in origin:
            return "retained_parent"
    parent_id = row.get("parentCandidateId")
    if parent_id is None:
        return "immigrant"
    return "offspring"


def _yield_row(
    *,
    origin: str,
    constructed: int,
    evaluated_ids: set[str],
    evaluated: dict[str, dict[str, Any]],
    provisional_ids: set[str],
    cumulative_ids: set[str],
    quality_ids: set[str],
    frontier_ids: set[str],
    archive_ids: set[str],
    archive_cells: set[str],
    robust_policy: Mapping[str, Any],
    direction_policy: Any,
) -> dict[str, Any]:
    evaluated_count = len(evaluated_ids)
    resolved_programs = {
        str(_member_field(evaluated[cid], "resolvedProgramSha256", "programSha256"))
        for cid in evaluated_ids
        if cid in evaluated
        and _member_field(evaluated[cid], "resolvedProgramSha256", "programSha256")
    }
    behavior_identities = {
        str(_member_field(evaluated[cid], "behaviorIdentitySha256", "fingerprintSha256"))
        for cid in evaluated_ids
        if cid in evaluated
        and _member_field(evaluated[cid], "behaviorIdentitySha256", "fingerprintSha256")
    }
    zero_trade = 0
    support_pass = direction_pass = quality_like = cumulative_quality = archive_retained = 0
    nets: list[float] = []
    for cid in evaluated_ids:
        row = evaluated.get(cid)
        if row is None:
            continue
        flags = _gate_flags(row, robust_policy=robust_policy, direction_policy=direction_policy)
        windows = _window_metrics_from_evaluated(row)
        if not windows or all(int(item.get("closedTrades") or 0) == 0 for item in windows):
            zero_trade += 1
        if flags.get("combinedSupportPass"):
            support_pass += 1
        if flags.get("directionEligible"):
            direction_pass += 1
        if flags.get("currentPanelQualityLike"):
            quality_like += 1
        if cid in quality_ids:
            cumulative_quality += 1
        if cid in archive_ids:
            archive_retained += 1
        nets.append(flags["_metrics"]["cumulativeConservativeNetR"])
    return {
        "originKind": origin,
        "constructedCandidateCount": constructed,
        "evaluatedCandidateCount": evaluated_count,
        "uniqueResolvedProgramCount": len(resolved_programs),
        "uniqueBehaviorIdentityCount": len(behavior_identities),
        "zeroTradeCandidateCount": zero_trade,
        "supportPassCount": support_pass,
        "supportPassRate": (support_pass / evaluated_count) if evaluated_count else 0.0,
        "directionPassCount": direction_pass,
        "directionPassRate": (direction_pass / evaluated_count) if evaluated_count else 0.0,
        "currentPanelQualityLikeCount": quality_like,
        "currentPanelQualityLikeRate": (quality_like / evaluated_count) if evaluated_count else 0.0,
        "cumulativeQualityCount": cumulative_quality,
        "cumulativeQualityRate": (cumulative_quality / evaluated_count) if evaluated_count else 0.0,
        "archiveRetainedCount": archive_retained,
        "archiveRetainedRate": (archive_retained / evaluated_count) if evaluated_count else 0.0,
        "occupiedArchiveCellCount": len(archive_cells),
        "meanCurrentPanelConservativeNetR": (sum(nets) / len(nets)) if nets else None,
        "medianCurrentPanelConservativeNetR": float(statistics.median(nets)) if nets else None,
        "provisionalSelectedCount": len(evaluated_ids & provisional_ids),
        "cumulativePresentCount": len(evaluated_ids & cumulative_ids),
    }


def _cumulative_qualification(
    entering_ids: set[str],
    evaluated: dict[str, dict[str, Any]],
    cumulative: Mapping[str, Any] | None,
    archive_members: dict[str, dict[str, Any]],
    *,
    robust_policy: Mapping[str, Any],
    direction_policy: Any,
) -> dict[str, Any]:
    quality_ids = {
        str(value)
        for value in (cumulative.get("qualityCandidateIds") or [])
        if isinstance(value, str)
    } if cumulative else set()
    frontier_ids = {
        str(value)
        for value in (cumulative.get("frontierCandidateIds") or [])
        if isinstance(value, str)
    } if cumulative else set()
    cumulative_rows = _cumulative_member_index(cumulative or {})
    archive_ids = set(archive_members)
    resolved_seen: dict[str, str] = {}
    terminal = defaultdict(int)
    pass_counts = {
        "supportPass": 0,
        "directionPass": 0,
        "qualityEconomicsPass": 0,
        "frontierEconomicsPass": 0,
        "rawQualityEligible": 0,
        "rawFrontierEligible": 0,
        "paretoAdmitted": 0,
        "cellCapacityAdmitted": 0,
        "archiveRetained": 0,
    }
    for candidate_id in sorted(entering_ids):
        eval_row = evaluated.get(candidate_id, {})
        cum_row = cumulative_rows.get(candidate_id)
        windows = (
            cum_row.get("windowMetrics")
            if isinstance(cum_row, Mapping) and isinstance(cum_row.get("windowMetrics"), list)
            else _window_metrics_from_evaluated(eval_row)
        )
        months = float(
            (cum_row or {}).get("coveredMonths")
            or _member_field(eval_row, "coveredMonths")
            or max(len(windows), 1)
        )
        flags = _gate_flags(
            eval_row,
            robust_policy=robust_policy,
            direction_policy=direction_policy,
            windows=windows,
            covered_months=months,
        )
        if flags.get("combinedSupportPass"):
            pass_counts["supportPass"] += 1
        if flags.get("directionEligible"):
            pass_counts["directionPass"] += 1
        if flags.get("currentPanelQualityLike"):
            pass_counts["rawQualityEligible"] += 1
        if flags.get("currentPanelFrontierLike"):
            pass_counts["rawFrontierEligible"] += 1
        if flags.get("cumulativeNetPositive") and flags.get("medianWindowNetPositive"):
            pass_counts["qualityEconomicsPass"] += 1
        elif flags.get("combinedSupportPass") and flags.get("directionEligible"):
            pass_counts["frontierEconomicsPass"] += 1

        if not flags.get("activeWindowFractionPass"):
            terminal["failedActiveWindowFraction"] += 1
            continue
        if not flags.get("averageTradesPerMonthPass"):
            terminal["failedTradesPerMonth"] += 1
            continue
        direction = flags.get("_direction")
        if direction is not None and direction.get("selectionEligible") is not True:
            terminal[_direction_failure_bucket(direction)] += 1
            continue
        if not flags.get("cumulativeNetPositive"):
            terminal["failedCumulativeEconomics"] += 1
            continue
        if not flags.get("medianWindowNetPositive"):
            terminal["failedMedianEconomics"] += 1
            continue
        resolved = str(
            _member_field(cum_row or eval_row, "resolvedProgramSha256", "programSha256") or ""
        )
        if resolved:
            prior = resolved_seen.get(resolved)
            if prior is not None and prior != candidate_id:
                terminal["resolvedExecutionDeduplicated"] += 1
                continue
            resolved_seen[resolved] = candidate_id
        if candidate_id not in quality_ids and candidate_id not in frontier_ids:
            terminal["paretoRemoved"] += 1
            continue
        pass_counts["paretoAdmitted"] += 1
        if candidate_id not in archive_ids:
            terminal["cellCapacityRemoved"] += 1
            continue
        pass_counts["cellCapacityAdmitted"] += 1
        terminal["archiveRetained"] += 1
        pass_counts["archiveRetained"] += 1

    return {
        "enteringCohortSize": len(entering_ids),
        **pass_counts,
        **{key: terminal.get(key, 0) for key in (
            "failedActiveWindowFraction",
            "failedTradesPerMonth",
            "failedDirectionNoNonnegativeSide",
            "failedDirectionMildNegativeOpposite",
            "failedDirectionHarmfulOpposite",
            "failedCumulativeEconomics",
            "failedMedianEconomics",
            "paretoRemoved",
            "cellCapacityRemoved",
            "resolvedExecutionDeduplicated",
            "archiveRetained",
        )},
    }


def _stable_binding(path: Path, *, name: str, run_root: Path) -> dict[str, Any]:
    """Bind one artifact using semantic content independent of JSONL row order."""

    checked = _real_path(path, name=name)
    if checked.suffix == ".jsonl":
        rows = _read_jsonl(checked, name=name)
        rows.sort(key=lambda row: canonical_json(row))
        raw = "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")
    else:
        try:
            document = _read_canonical_object(checked, name=name)
            raw = (canonical_json(document) + "\n").encode("utf-8")
        except Exception:
            raw = checked.read_bytes()
    try:
        path_value = checked.relative_to(run_root.resolve()).as_posix()
    except ValueError:
        path_value = str(checked)
    return {
        "path": path_value,
        "rawSha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "sizeBytes": len(raw),
    }


def _build_source_bindings(
    paths: Mapping[str, Path | None], *, run_root: Path
) -> dict[str, Any]:
    bindings: dict[str, Any] = {}
    for key in sorted(paths):
        path = paths[key]
        if path is None:
            bindings[key] = None
            continue
        bindings[key] = _stable_binding(path, name=key, run_root=run_root)
    return bindings


def audit_temporal_qd_generation_quality(
    run_root: Path | str,
    generation_index: int,
    *,
    finalization: Mapping[str, Any] | None = None,
    generation_record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Audit one committed native v5 generation and return a compact summary."""

    root = Path(run_root).resolve()
    generation_root = _generation_root(root, generation_index)
    finalization_root = _native_finalization_root(generation_root)
    limitations: list[str] = []
    paths: dict[str, Path | None] = {}

    parent_material_path = _resolve_generation_file(
        generation_root, "proposal/parent-material.jsonl", name="parent material"
    )
    paths["parentMaterial"] = parent_material_path

    campaign_output = generation_root / "campaign" / "proposal-current-panel" / "campaign-output"
    evaluated_path = _resolve_generation_file(
        generation_root,
        "campaign/proposal-current-panel/campaign-output/evaluated-members.jsonl",
        name="evaluated members",
    )
    paths["evaluatedMembers"] = evaluated_path

    for key, relative in (
        ("candidatePanelBundles", "candidate-panel-bundles.jsonl"),
        ("campaignCheckpoint", "campaign-output-checkpoint.json"),
        ("campaignManifest", "campaign-output-manifest.json"),
    ):
        candidate = campaign_output / relative
        paths[key] = candidate if candidate.is_file() else None

    prefinalizer_round = _discover_latest_prefinalizer_round(generation_root)
    if prefinalizer_round is not None:
        paths["prefinalizerResult"] = prefinalizer_round / "result.json"
        paths["prefinalizerReceipt"] = prefinalizer_round / "execution-receipt.json"
        selected_path = prefinalizer_round / "selected-rich-members.jsonl"
        paths["selectedRichMembers"] = selected_path if selected_path.is_file() else None
    else:
        paths["prefinalizerResult"] = None
        paths["prefinalizerReceipt"] = None
        paths["selectedRichMembers"] = None
        limitations.append("latest ready prefinalizer round unavailable")

    if isinstance(finalization, Mapping):
        artifacts = finalization.get("artifacts")
        if isinstance(artifacts, Mapping):
            for key, artifact_key in (
                ("cumulativeArchive", "cumulativeArchive"),
                ("parentArchive", "parentArchive"),
            ):
                artifact = artifacts.get(artifact_key)
                if isinstance(artifact, Mapping) and artifact.get("absolutePath"):
                    paths[key] = Path(str(artifact["absolutePath"]))
        output_root = finalization.get("outputRoot")
        if isinstance(output_root, str):
            finalization_root = Path(output_root)
    else:
        fast_result = finalization_root / "fast-ephemeral-result.json"
        if fast_result.is_file():
            paths["fastEphemeralResult"] = fast_result
        elif (finalization_root / "generation-commit.json").is_file():
            paths["generationCommit"] = finalization_root / "generation-commit.json"

    for key, relative in (
        ("cumulativeArchive", "evidence/cumulative-archive.json"),
        ("parentArchive", "archive.json"),
        ("finalizerSource", "source.json"),
    ):
        if paths.get(key) is None:
            candidate = finalization_root / relative
            paths[key] = candidate if candidate.is_file() else None

    if generation_index > 1:
        previous_archive = _generation_root(root, generation_index - 1) / "native-finalization" / "archive.json"
        paths["previousParentArchive"] = previous_archive if previous_archive.is_file() else None
    else:
        paths["previousParentArchive"] = None

    parent_rows = _parse_parent_material(
        _read_jsonl(parent_material_path, name="parent material")
    )
    evaluated_rows = _read_jsonl(evaluated_path, name="evaluated members")
    evaluated = _index_evaluated(evaluated_rows)

    cumulative = (
        _read_canonical_object(paths["cumulativeArchive"], name="cumulative archive")
        if paths.get("cumulativeArchive") is not None
        else None
    )
    archive = (
        _read_canonical_object(paths["parentArchive"], name="parent archive")
        if paths.get("parentArchive") is not None
        else None
    )
    if cumulative is None:
        raise TemporalDiscoveryContractError("cumulative archive is required")
    if archive is None:
        raise TemporalDiscoveryContractError("final parent archive is required")

    robust_policy, direction_policy = _load_threshold_policies(archive)
    archive_members = _archive_member_index(archive)
    cumulative_members = _cumulative_member_index(cumulative)
    quality_ids = {
        str(value)
        for value in (cumulative.get("qualityCandidateIds") or [])
        if isinstance(value, str)
    }
    frontier_ids = {
        str(value)
        for value in (cumulative.get("frontierCandidateIds") or [])
        if isinstance(value, str)
    }

    provisional_ids: set[str] = set()
    if paths.get("selectedRichMembers") is not None:
        provisional_ids = {
            str(row.get("candidateId"))
            for row in _read_jsonl(paths["selectedRichMembers"], name="selected rich members")
            if isinstance(row.get("candidateId"), str)
        }
    elif prefinalizer_round is not None:
        result = _optional_json(paths["prefinalizerResult"], name="prefinalizer result")
        provisional = result.get("provisional") if isinstance(result, Mapping) else None
        if isinstance(provisional, Mapping):
            candidates = provisional.get("candidates")
            if isinstance(candidates, list):
                provisional_ids = {
                    str(row.get("candidateId"))
                    for row in candidates
                    if isinstance(row, Mapping) and isinstance(row.get("candidateId"), str)
                }

    attempt_available, attempt_stats = _attempt_telemetry(generation_root, generation_index)
    construction: dict[str, Any] = {
        "acceptedCandidateCount": len(parent_rows),
        "offspringCandidateCount": sum(1 for row in parent_rows if _origin_kind(row) == "offspring"),
        "immigrantCandidateCount": sum(1 for row in parent_rows if _origin_kind(row) == "immigrant"),
        "retainedParentEvaluationCount": sum(
            1 for row in parent_rows if _origin_kind(row) == "retained_parent"
        ),
        "uniqueCandidateIdentityCount": len(
            {
                str(_member_field(evaluated[cid], "candidateIdentitySha256"))
                for cid in evaluated
                if _member_field(evaluated[cid], "candidateIdentitySha256")
            }
        ),
        "uniqueAuthoredProgramCount": len(
            {
                str(_member_field(evaluated[cid], "programSha256"))
                for cid in evaluated
                if _member_field(evaluated[cid], "programSha256")
            }
        ),
        "uniqueResolvedProgramCount": len(
            {
                str(_member_field(evaluated[cid], "resolvedProgramSha256", "programSha256"))
                for cid in evaluated
                if _member_field(evaluated[cid], "resolvedProgramSha256", "programSha256")
            }
        ),
        "attemptTelemetryAvailable": attempt_available,
    }
    if attempt_available:
        construction.update(attempt_stats)

    evaluation = {
        "evaluatedCandidateCount": len(evaluated),
        "allEvaluated": _aggregate_gate_counts(
            evaluated, robust_policy=robust_policy, direction_policy=direction_policy
        ),
    }
    provisional_evaluated = {cid: evaluated[cid] for cid in sorted(provisional_ids) if cid in evaluated}
    provisional_selection = {
        "provisionalCandidateCount": len(provisional_ids),
        "provisionalEvaluatedCount": len(provisional_evaluated),
        "provisionalCohort": _aggregate_gate_counts(
            provisional_evaluated,
            robust_policy=robust_policy,
            direction_policy=direction_policy,
        ),
    }

    entering_ids = provisional_ids or set(evaluated)
    cumulative_qualification = _cumulative_qualification(
        entering_ids,
        evaluated,
        cumulative,
        archive_members,
        robust_policy=robust_policy,
        direction_policy=direction_policy,
    )

    by_origin: dict[str, list[str]] = defaultdict(list)
    for row in parent_rows:
        candidate_id = row.get("candidateId")
        if isinstance(candidate_id, str):
            by_origin[_origin_kind(row)].append(candidate_id)

    origin_yield = []
    for origin in sorted(by_origin):
        ids = set(by_origin[origin])
        archive_cells = {
            str(archive_members[cid].get("cellId"))
            for cid in ids
            if cid in archive_members and archive_members[cid].get("cellId") is not None
        }
        origin_yield.append(
            _yield_row(
                origin=origin,
                constructed=len(ids),
                evaluated_ids=ids & set(evaluated),
                evaluated=evaluated,
                provisional_ids=provisional_ids,
                cumulative_ids=set(cumulative_members),
                quality_ids=quality_ids,
                frontier_ids=frontier_ids,
                archive_ids=set(archive_members),
                archive_cells=archive_cells,
                robust_policy=robust_policy,
                direction_policy=direction_policy,
            )
        )

    parent_yield: list[dict[str, Any]] = []
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for row in parent_rows:
        parent_id = row.get("parentCandidateId")
        child_id = row.get("candidateId")
        if isinstance(parent_id, str) and isinstance(child_id, str):
            children_by_parent[parent_id].append(child_id)
    for parent_id in sorted(children_by_parent):
        child_ids = children_by_parent[parent_id]
        parent_eval = evaluated.get(parent_id)
        same_panel = parent_eval is not None
        parent_flags = (
            _gate_flags(parent_eval, robust_policy=robust_policy, direction_policy=direction_policy)
            if same_panel
            else None
        )
        child_eval_ids = set(child_ids) & set(evaluated)
        row = {
            "parentCandidateId": parent_id,
            "offspringConstructedCount": len(child_ids),
            "offspringEvaluatedCount": len(child_eval_ids),
            "samePanelParentComparisonAvailable": same_panel,
        }
        if same_panel and parent_flags is not None:
            child_quality = sum(
                1
                for cid in child_eval_ids
                if _gate_flags(
                    evaluated[cid],
                    robust_policy=robust_policy,
                    direction_policy=direction_policy,
                ).get("currentPanelQualityLike")
            )
            row.update(
                {
                    "parentCurrentPanelQualityLike": parent_flags.get("currentPanelQualityLike"),
                    "parentCurrentPanelConservativeNetR": parent_flags["_metrics"][
                        "cumulativeConservativeNetR"
                    ],
                    "offspringCurrentPanelQualityLikeCount": child_quality,
                    "offspringCurrentPanelQualityLikeRate": (
                        child_quality / len(child_eval_ids) if child_eval_ids else 0.0
                    ),
                }
            )
        parent_yield.append(row)

    operator_counts: dict[str, int] = defaultdict(int)
    sequence_counts: dict[str, int] = defaultdict(int)
    for row in parent_rows:
        for operator_id in row.get("operatorIds") or []:
            operator_counts[str(operator_id)] += 1
        sequence = row.get("operatorSequence")
        if isinstance(sequence, str) and sequence:
            sequence_counts[sequence] += 1
    operator_yield = [
        {"operatorId": operator_id, "occurrenceCount": operator_counts[operator_id]}
        for operator_id in sorted(operator_counts)
    ]
    operator_yield.extend(
        {
            "operatorSequence": sequence,
            "occurrenceCount": sequence_counts[sequence],
            "note": "occurrence counts overlap for multi-step children",
        }
        for sequence in sorted(sequence_counts)
        if " > " in sequence
    )

    behavior_clusters: dict[str, int] = defaultdict(int)
    parent_behavior: dict[str, set[str]] = defaultdict(set)
    operator_behavior: dict[str, set[str]] = defaultdict(set)
    for candidate_id, row in evaluated.items():
        behavior = str(
            _member_field(row, "behaviorIdentitySha256", "fingerprintSha256") or "unknown"
        )
        behavior_clusters[behavior] += 1
        for parent_row in parent_rows:
            if parent_row.get("candidateId") == candidate_id:
                parent_id = parent_row.get("parentCandidateId")
                if isinstance(parent_id, str):
                    parent_behavior[parent_id].add(behavior)
                for operator_id in parent_row.get("operatorIds") or []:
                    operator_behavior[str(operator_id)].add(behavior)

    metric_series = {
        "activeWindowFraction": [],
        "averageTradesPerMonth": [],
        "cumulativeConservativeNetR": [],
        "medianWindowConservativeNetR": [],
    }
    for row in evaluated.values():
        flags = _gate_flags(row, robust_policy=robust_policy, direction_policy=direction_policy)
        metrics = flags["_metrics"]
        for key in metric_series:
            metric_series[key].append(float(metrics[key if key != "medianWindowConservativeNetR" else "medianWindowConservativeNetR"]))
    if not evaluated:
        limitations.append("empty evaluated cohort produced null threshold quantiles")

    threshold_margins = {
        "activeWindowFraction": _quantiles(metric_series["activeWindowFraction"]),
        "averageTradesPerMonth": _quantiles(metric_series["averageTradesPerMonth"]),
        "cumulativeConservativeNetR": _quantiles(metric_series["cumulativeConservativeNetR"]),
        "medianWindowConservativeNetR": _quantiles(metric_series["medianWindowConservativeNetR"]),
        "nearThresholdBands": {
            "activeWindowFraction": {
                "below": sum(
                    1
                    for value in metric_series["activeWindowFraction"]
                    if _ACTIVE_WINDOW_FRACTION - 0.05 <= value < _ACTIVE_WINDOW_FRACTION
                ),
                "above": sum(
                    1
                    for value in metric_series["activeWindowFraction"]
                    if _ACTIVE_WINDOW_FRACTION <= value < _ACTIVE_WINDOW_FRACTION + 0.05
                ),
            },
            "averageTradesPerMonth": {
                "below": sum(
                    1
                    for value in metric_series["averageTradesPerMonth"]
                    if _TRADES_PER_MONTH - 1.0 <= value < _TRADES_PER_MONTH
                ),
                "above": sum(
                    1
                    for value in metric_series["averageTradesPerMonth"]
                    if _TRADES_PER_MONTH <= value < _TRADES_PER_MONTH + 1.0
                ),
            },
        },
    }

    counterfactual_selection = {
        "scope": "current_panel_shortlist_recall_only",
        "caps": {
            str(cap): _simulate_counterfactual_shortlist(
                evaluated,
                robust_policy=robust_policy,
                direction_policy=direction_policy,
                cap=cap,
            )
            for cap in COUNTERFACTUAL_CAPS
        },
    }

    incumbent_lifecycle: list[dict[str, Any]] = []
    retained_parents = sorted(
        {
            row.get("parentCandidateId")
            for row in parent_rows
            if isinstance(row.get("parentCandidateId"), str)
        }
    )
    for parent_id in retained_parents[:NEAR_MISS_CAP]:
        incumbent_lifecycle.append(
            {
                "candidateId": parent_id,
                "evaluatedOnCurrentPanel": parent_id in evaluated,
                "presentInProvisional": parent_id in provisional_ids,
                "presentInCumulativeQualification": parent_id in cumulative_members,
                "presentInFinalArchive": parent_id in archive_members,
                "terminalReason": (
                    "archive_retained"
                    if parent_id in archive_members
                    else (
                        "cumulative_only"
                        if parent_id in cumulative_members
                        else (
                            "provisional_only"
                            if parent_id in provisional_ids
                            else "dropped_before_provisional"
                        )
                    )
                ),
            }
        )

    archive_summary = {
        "memberCount": len(archive_members),
        "occupiedCellCount": archive.get("occupiedCellCount"),
        "qualityMemberCount": archive.get("qualityMemberCount"),
        "cells": [
            {
                "cellId": cell.get("cellId"),
                "memberCount": len(cell.get("members") or [])
                if isinstance(cell.get("members"), list)
                else 0,
            }
            for cell in sorted(
                (cell for cell in (archive.get("cells") or []) if isinstance(cell, Mapping)),
                key=lambda item: str(item.get("cellId")),
            )
        ],
    }

    body: dict[str, Any] = {
        "schemaVersion": AUDIT_SCHEMA,
        "generationIndex": generation_index,
        "sourceBindings": _build_source_bindings(paths, run_root=root),
        "construction": construction,
        "evaluation": evaluation,
        "provisionalSelection": provisional_selection,
        "incumbentLifecycle": incumbent_lifecycle,
        "cumulativeQualification": cumulative_qualification,
        "archive": archive_summary,
        "originYield": origin_yield,
        "parentYield": parent_yield,
        "operatorYield": operator_yield,
        "behaviorDiversity": {
            "effectiveBehaviorCount": _inverse_simpson(behavior_clusters),
            "evaluatedBehaviorIdentityCount": len(behavior_clusters),
            "parentContributionCounts": {
                parent_id: len(identities)
                for parent_id, identities in sorted(parent_behavior.items())
            },
            "operatorContributionCounts": {
                operator_id: len(identities)
                for operator_id, identities in sorted(operator_behavior.items())
            },
        },
        "thresholdMargins": threshold_margins,
        "counterfactualSelection": counterfactual_selection,
        "limitations": sorted(set(limitations)),
    }
    if generation_record is not None:
        body["generationRecordSha256"] = generation_record.get("generationRecordSha256")
    return _self_hash_audit(body)


def _build_run_quality_audit(run_root: Path, generation_index: int) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    for index in range(1, generation_index + 1):
        path = _generation_root(run_root, index) / "quality-audit" / "generation-quality-audit.json"
        if not path.is_file():
            continue
        audit = json.loads(path.read_text(encoding="utf-8"))
        summaries.append(
            {
                "generationIndex": index,
                "auditSha256": audit.get("auditSha256"),
                "evaluatedCandidateCount": audit.get("evaluation", {}).get(
                    "evaluatedCandidateCount"
                ),
                "archiveMemberCount": audit.get("archive", {}).get("memberCount"),
                "effectiveBehaviorCount": audit.get("behaviorDiversity", {}).get(
                    "effectiveBehaviorCount"
                ),
                "attemptTelemetryAvailable": audit.get("construction", {}).get(
                    "attemptTelemetryAvailable"
                ),
                "archiveRetained": audit.get("cumulativeQualification", {}).get(
                    "archiveRetained"
                ),
            }
        )
    trends: dict[str, Any] = {}
    if len(summaries) >= 2:
        first = summaries[0]
        last = summaries[-1]
        trends = {
            "evaluatedCandidateCountDelta": (last.get("evaluatedCandidateCount") or 0)
            - (first.get("evaluatedCandidateCount") or 0),
            "archiveMemberCountDelta": (last.get("archiveMemberCount") or 0)
            - (first.get("archiveMemberCount") or 0),
            "effectiveBehaviorCountDelta": (last.get("effectiveBehaviorCount") or 0.0)
            - (first.get("effectiveBehaviorCount") or 0.0),
        }
    body = {
        "schemaVersion": RUN_AUDIT_SCHEMA,
        "generationCount": len(summaries),
        "generationSummaries": summaries,
        "crossGenerationTrends": trends,
    }
    return _self_hash_audit(body)


def observe_generation_quality_audit(
    run_root: Path | str,
    generation_index: int,
    *,
    finalization: Mapping[str, Any] | None = None,
    generation_record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Write generation quality audit artifacts; never raise to callers."""

    generation_root = _generation_root(run_root, generation_index)
    audit_dir = generation_root / "quality-audit"
    success_path = audit_dir / "generation-quality-audit.json"
    error_path = audit_dir / "audit-error.json"
    try:
        audit = audit_temporal_qd_generation_quality(
            run_root,
            generation_index,
            finalization=finalization,
            generation_record=generation_record,
        )
        _write_canonical(success_path, audit)
        if generation_index == 5:
            run_audit = _build_run_quality_audit(Path(run_root).resolve(), generation_index)
            run_audit_dir = Path(run_root).resolve() / "quality-audit"
            _write_canonical(run_audit_dir / "run-quality-audit.json", run_audit)
        return {
            "status": "ok",
            "generationIndex": generation_index,
            "auditSha256": audit["auditSha256"],
            "path": str(success_path.resolve()),
        }
    except Exception as exc:
        category = exc.__class__.__name__
        completed_sections: list[str] = []
        missing_sections = [
            "construction",
            "evaluation",
            "cumulativeQualification",
            "archive",
        ]
        error_payload = _self_hash_audit(
            {
                "schemaVersion": AUDIT_SCHEMA,
                "generationIndex": generation_index,
                "category": category,
                "message": str(exc),
                "inputPath": str(generation_root.resolve()),
                "completedSections": completed_sections,
                "missingSections": missing_sections,
            }
        )
        try:
            _write_canonical(error_path, error_payload)
        except Exception as write_exc:
            _LOG.warning(
                "could not write generation quality audit error for generation %s: %s",
                generation_index,
                write_exc,
            )
        warning = (
            f"generation {generation_index} quality audit failed ({category}): {exc}"
        )
        _LOG.warning(warning)
        return {
            "status": "error",
            "generationIndex": generation_index,
            "warning": warning,
            "category": category,
            "errorPath": str(error_path.resolve()),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--generation-index", type=int, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    result = audit_temporal_qd_generation_quality(
        args.run_root, args.generation_index
    )
    encoded = canonical_json(result) + "\n"
    if args.output is not None:
        args.output.write_text(encoded, encoding="utf-8", newline="\n")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA",
    "RUN_AUDIT_SCHEMA",
    "audit_temporal_qd_generation_quality",
    "observe_generation_quality_audit",
]
