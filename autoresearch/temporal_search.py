"""Finite, authority-bound Stage 5D temporal candidate/window search.

This module deliberately has no profile mutation logic.  The controller freezes a
small candidate set, turns it into immutable jobs, and is the only component that
may journal, resume, deduplicate, materialize, or select results.  A worker only
receives one candidate/window/cost-view job and evaluates it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Protocol

from .result_codec import (
    ResultCodecError,
    read_json_object as _read_codec_json_object,
    semantic_sha256 as _semantic_sha256,
    write_gzip_json_once,
)


TEMPORAL_SEARCH_AUTHORITY_SCHEMA = "temporal_graph_candidate_window_authority_v1"
TEMPORAL_SEARCH_PREPARATION_SCHEMA = "temporal_graph_candidate_window_preparation_v1"
TEMPORAL_SEARCH_TASK_KIND = "temporal_graph_candidate_window"
TEMPORAL_SEARCH_JOB_SCHEMA = "temporal_graph_candidate_window_job_v1"
TEMPORAL_SEARCH_CAPABILITY = "temporal_graph_candidate_window_v1"
TEMPORAL_BIDIRECTIONAL_REPLAY_CAPABILITY = (
    "temporal_graph_bidirectional_replay_v1"
)
TEMPORAL_SEARCH_RESULT_SCHEMA = "temporal_graph_candidate_window_result_v1"
TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA = "temporal_graph_candidate_window_rejected_result_v1"
TEMPORAL_SEARCH_CHECKPOINT_SCHEMA = "temporal_graph_candidate_window_checkpoint_v1"
TEMPORAL_SEARCH_MANIFEST_SCHEMA = "temporal_graph_candidate_window_manifest_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_COST_VIEWS = ("research_conservative", "none")
_BAR_SINGLE_POSITION_EVALUATOR_ID = "bar_single_position_execution_v1"
_BAR_BIDIRECTIONAL_SINGLE_POSITION_EVALUATOR_ID = (
    "bar_bidirectional_single_position_execution_v2"
)
_REQUIRED_WORKER_CAPABILITIES = (
    TEMPORAL_SEARCH_CAPABILITY,
    TEMPORAL_BIDIRECTIONAL_REPLAY_CAPABILITY,
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


_WARMUP_REJECTION_CODE = "aligned_scoring_warmup_insufficient"
_WARMUP_ERROR_TYPE = "AlignedScoringWarmupInsufficientError"
_BREAK_EVEN_REJECTION_CODE = "duplicate_break_even_execution_invariant"
_BREAK_EVEN_ERROR_TYPE = "TemporalExecutionInvariantError"
_BREAK_EVEN_ERROR = "TemporalExecutionInvariantError: break-even may be applied only once"


class LabGatewayClientProtocol(Protocol):
    def enqueue_tasks(self, tasks: list[dict[str, Any]]) -> dict[str, Any]: ...
    def read_results(self, *, limit: int) -> list[dict[str, Any]]: ...
    def ack_results(self, lease_ids: list[str]) -> int: ...


def _clone(value: Any, *, name: str) -> Any:
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
        raise TemporalSearchContractError(
            f"{name} must be finite canonical JSON"
        ) from exc


def canonical_sha256(value: Any) -> str:
    return _semantic_sha256(value)


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
        raise TemporalSearchContractError(
            f"{name} must be a stable candidate identifier"
        )
    return token


def _evaluator_id_for_profile(profile: Mapping[str, Any]) -> str:
    """Select the worker evaluator from the already-admitted profile version.

    The candidate-window worker validates this independently.  Keeping the
    mapping at task construction prevents a v3/both snapshot from silently
    inheriting the legacy v2 evaluator default.
    """

    return (
        _BAR_BIDIRECTIONAL_SINGLE_POSITION_EVALUATOR_ID
        if profile.get("version") == "v3"
        else _BAR_SINGLE_POSITION_EVALUATOR_ID
    )


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SHA.fullmatch(token):
        raise TemporalSearchContractError(f"{name} must be an exact sha256 identity")
    return token


def _stamp(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(
            token[:-1] + "+00:00" if token.endswith("Z") else token
        )
    except ValueError as exc:
        raise TemporalSearchContractError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise TemporalSearchContractError(f"{name} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _finite_number(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalSearchContractError(f"{name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(f"{name} must be numeric") from exc
    if not math.isfinite(number):
        raise TemporalSearchContractError(f"{name} must be finite")
    return number


def _nonnegative_integer(value: Any, *, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise TemporalSearchContractError(
            f"{name} must be an integer greater than or equal to {minimum}"
        )
    return value


def _same_number(left: float, right: float, *, name: str) -> None:
    if not math.isclose(left, right, abs_tol=1e-9, rel_tol=1e-9):
        raise TemporalSearchContractError(f"{name} is inconsistent")


def _candidate_window_is_v3(material: Mapping[str, Any]) -> bool:
    """Return whether a result claims any Stage 5E7-v3-only economics/evidence."""
    if "evidence_contract" in material:
        return True
    cost_views = material.get("cost_view_results")
    if not isinstance(cost_views, Mapping):
        return False
    for item in cost_views.values():
        if not isinstance(item, Mapping):
            continue
        replay = item.get("replay_result")
        if not isinstance(replay, Mapping):
            continue
        metrics = replay.get("metrics")
        if isinstance(metrics, Mapping) and any(
            key in metrics
            for key in (
                "terminalValuation",
                "terminalAdjustedTotalGrossR",
                "terminalAdjustedTotalNetR",
                "terminalAdjustedTotalExecutionCostPercent",
                "terminalAdjustedEquityCurveR",
                "terminalAdjustedMaxDrawdownR",
            )
        ):
            return True
    return False


def is_v3_candidate_window_result(material: Mapping[str, Any]) -> bool:
    """Public, side-effect-free v3 discriminator for historical result readers."""
    return _candidate_window_is_v3(material)


def _path_rows(
    rows: Any,
    *,
    name: str,
    keys: tuple[str, ...],
    optional_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise TemporalSearchContractError(f"{name} must be an array")
    material: list[dict[str, Any]] = []
    for index, item in enumerate(rows):
        row = _mapping(item, name=f"{name}[{index}]")
        if any(key not in row for key in keys):
            raise TemporalSearchContractError(
                f"{name}[{index}] is missing non-cost path evidence"
            )
        if "intentIds" in keys and not isinstance(row["intentIds"], list):
            raise TemporalSearchContractError(
                f"{name}[{index}].intentIds must be an array"
            )
        material.append(
            {
                **{key: row[key] for key in keys},
                **{key: row[key] for key in optional_keys if key in row},
            }
        )
    return material


def _cost_view_trailing_projection(trailing: Any) -> dict[str, Any] | None:
    if trailing is None:
        return None
    row = _mapping(trailing, name="trailing state")
    return {
        "policy_sha256": row.get("policySha256"),
        "active": row.get("active", False),
        "suspended": row.get("suspended", False),
        "activation_count": row.get("activationCount", 0),
        "activation_clock_index": row.get("activationClockIndex"),
        "deactivation_count": row.get("deactivationCount", 0),
        "pending_stop_price": row.get("pendingStopPrice"),
        "pending_anchor_price": row.get("pendingAnchorPrice"),
        "pending_clock_index": row.get("pendingClockIndex"),
        "update_count": row.get("updateCount", 0),
        "last_applied_anchor_price": row.get("lastAppliedAnchorPrice"),
        "owns_current_stop": row.get("ownsCurrentStop", False),
    }


def _cost_view_position_projection(position: Any) -> dict[str, Any] | None:
    if position is None:
        return None
    row = _mapping(position, name="final execution position")
    return {
        "instrument": row.get("instrument"),
        "direction": row.get("direction"),
        "management_plan_id": row.get("managementPlanId"),
        "management_plan_sha256": row.get("managementPlanSha256"),
        "entry_bar_id": row.get("entryBarId"),
        "entry_time": row.get("entryTime"),
        "entry_clock_index": row.get("entryClockIndex"),
        "entry_price": row.get("entryPrice"),
        "stop_loss_percent": row.get("stopLossPercent"),
        "reward_multiple": row.get("rewardMultiple"),
        "take_profit_percent": row.get("takeProfitPercent"),
        "initial_stop_price": row.get("initialStopPrice"),
        "initial_target_price": row.get("initialTargetPrice"),
        "stop_price": row.get("stopPrice"),
        "target_price": row.get("targetPrice"),
        "trailing": _cost_view_trailing_projection(row.get("trailing")),
        "break_even_applied": row.get("breakEvenApplied", False),
        "stop_update_count": row.get("stopUpdateCount", 0),
        "target_update_count": row.get("targetUpdateCount", 0),
        "last_management_clock_index": row.get("lastManagementClockIndex"),
        "max_favorable_excursion_r": row.get("maxFavorableExcursionR", 0.0),
        "max_adverse_excursion_r": row.get("maxAdverseExcursionR", 0.0),
    }


def _cost_view_pending_effect_projection(effect: Any) -> dict[str, Any] | None:
    if effect is None:
        return None
    row = _mapping(effect, name="final pending effect")
    intent = _mapping(row.get("intent"), name="final pending effect intent")
    return {
        "transition_id": intent.get("transitionId"),
        "action_ordinal": intent.get("actionOrdinal"),
        "action_kind": intent.get("actionKind"),
        "timing_class": intent.get("timingClass"),
        "parameters": dict(_mapping(intent.get("parameters", {}), name="pending intent parameters")),
        "scheduled_clock_index": row.get("scheduledClockIndex"),
        "eligible_clock_index": row.get("eligibleClockIndex"),
        "expected_graph_state_id": row.get("expectedGraphStateId"),
        "scheduled_management_scalars": dict(
            _mapping(row.get("scheduledManagementScalars", {}), name="pending scalar snapshot")
        ),
    }


def _cost_view_final_execution_projection(state: Any) -> dict[str, Any] | None:
    if state is None:
        return None
    row = _mapping(state, name="final execution state")
    return {
        "instrument": row.get("instrument"),
        "direction": row.get("direction"),
        "last_execution_reason": row.get("lastExecutionReason"),
        "last_close_reason": row.get("lastCloseReason"),
        "last_market_bar_id": row.get("lastMarketBarId"),
        "last_bar_start": row.get("lastBarStart"),
        "last_clock_index": row.get("lastClockIndex"),
        "position": _cost_view_position_projection(row.get("position")),
        "pending_effect": _cost_view_pending_effect_projection(row.get("pendingEffect")),
    }


def _cost_view_path_sha256(replay: Mapping[str, Any], *, name: str) -> str:
    """Reproduce the worker's cost-invariant behavioral attestation exactly."""
    graph_rows = _path_rows(
        replay.get("graphTraces"),
        name=f"{name}.graphTraces",
        keys=(
            "eventSequence",
            "eventClass",
            "priorStateId",
            "nextStateId",
            "transitionId",
            "reasonCode",
            "intentIds",
        ),
    )
    execution_rows = _path_rows(
        replay.get("executionTraces"),
        name=f"{name}.executionTraces",
        keys=(
            "eventSequence",
            "clockIndex",
            "marketBarId",
            "phase",
            "effectKind",
            "status",
            "actionKind",
            "reasonCode",
            "price",
            "positionId",
            "tradeId",
        ),
    )
    trade_rows = _path_rows(
        replay.get("trades"),
        name=f"{name}.trades",
        keys=(
            "direction",
            "entryBarId",
            "exitBarId",
            "entryPhase",
            "exitPhase",
            "entryTime",
            "exitTime",
            "entryClockIndex",
            "exitClockIndex",
            "entryPrice",
            "exitPrice",
            "closeReason",
            "holdingBars",
            "holdingHours",
        ),
        optional_keys=(
            "managementPlanId",
            "managementPlanSha256",
            "stopLossPercent",
            "rewardMultiple",
            "takeProfitPercent",
            "initialStopPrice",
            "initialTargetPrice",
            "finalStopPrice",
            "targetPrice",
            "trailing",
            "breakEvenApplied",
            "stopUpdateCount",
            "targetUpdateCount",
            "lastManagementClockIndex",
            "maxFavorableExcursionR",
            "maxAdverseExcursionR",
        ),
    )
    graph_path = [
        {
            "event_sequence": row["eventSequence"],
            "event_class": row["eventClass"],
            "prior_state_id": row["priorStateId"],
            "next_state_id": row["nextStateId"],
            "transition_id": row["transitionId"],
            "reason_code": row["reasonCode"],
            "intent_count": len(row.get("intentIds") or ()),
        }
        for row in graph_rows
    ]
    execution_path = [
        {
            "event_sequence": row["eventSequence"],
            "clock_index": row["clockIndex"],
            "market_bar_id": row["marketBarId"],
            "phase": row["phase"],
            "effect_kind": row["effectKind"],
            "status": row["status"],
            "action_kind": row["actionKind"],
            "reason_code": row["reasonCode"],
            "price": row["price"],
            "position_present": row.get("positionId") is not None,
            "trade_present": row.get("tradeId") is not None,
        }
        for row in execution_rows
    ]
    trade_path = [
        {
            "direction": row["direction"],
            "management_plan_id": row.get("managementPlanId"),
            "management_plan_sha256": row.get("managementPlanSha256"),
            "entry_bar_id": row["entryBarId"],
            "exit_bar_id": row["exitBarId"],
            "entry_phase": row["entryPhase"],
            "exit_phase": row["exitPhase"],
            "entry_time": row["entryTime"],
            "exit_time": row["exitTime"],
            "entry_clock_index": row["entryClockIndex"],
            "exit_clock_index": row["exitClockIndex"],
            "entry_price": row["entryPrice"],
            "exit_price": row["exitPrice"],
            "stop_loss_percent": row.get("stopLossPercent"),
            "reward_multiple": row.get("rewardMultiple"),
            "take_profit_percent": row.get("takeProfitPercent"),
            "initial_stop_price": row.get("initialStopPrice"),
            "initial_target_price": row.get("initialTargetPrice"),
            "final_stop_price": row.get("finalStopPrice"),
            "target_price": row.get("targetPrice"),
            "trailing": _cost_view_trailing_projection(row.get("trailing")),
            "break_even_applied": row.get("breakEvenApplied", False),
            "stop_update_count": row.get("stopUpdateCount", 0),
            "target_update_count": row.get("targetUpdateCount", 0),
            "last_management_clock_index": row.get("lastManagementClockIndex"),
            "max_favorable_excursion_r": row.get("maxFavorableExcursionR", 0.0),
            "max_adverse_excursion_r": row.get("maxAdverseExcursionR", 0.0),
            "close_reason": row["closeReason"],
            "holding_bars": row["holdingBars"],
            "holding_hours": row["holdingHours"],
        }
        for row in trade_rows
    ]
    return canonical_sha256(
        {
            "schema_version": "temporal_graph_cost_view_path_v3",
            "graph_path": graph_path,
            "execution_path": execution_path,
            "trade_path": trade_path,
            "final_execution_state": _cost_view_final_execution_projection(
                replay.get("finalExecutionState")
            ),
        }
    )


def _max_drawdown(curve: list[float]) -> float:
    peak = 0.0
    drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    return drawdown


def _validate_terminal_metrics(
    metrics: Mapping[str, Any],
    *,
    name: str,
    expected_last_bar_start: str,
) -> None:
    """Verify the leave-open terminal economics supplied by the replay worker."""
    terminal = _mapping(metrics.get("terminalValuation"), name=f"{name}.terminalValuation")
    if terminal.get("schemaVersion") != "temporal_terminal_valuation_v1":
        raise TemporalSearchContractError(f"{name} terminal valuation schema is required")
    if terminal.get("policy") != "leave_open_mark_to_market_v1":
        raise TemporalSearchContractError(f"{name} terminal valuation policy is required")
    for key in (
        "lastCompletedBarId",
        "lastCompletedBarStart",
        "lastCompletedBarClose",
    ):
        if not isinstance(terminal.get(key), str) or not terminal[key].strip():
            raise TemporalSearchContractError(f"{name} terminal valuation {key} is required")
    if _stamp(
        terminal["lastCompletedBarStart"], name=f"{name}.terminalValuation.lastCompletedBarStart"
    ) != expected_last_bar_start:
        raise TemporalSearchContractError(f"{name} terminal valuation endpoint disagrees with evidence")
    _stamp(
        terminal["lastCompletedBarClose"], name=f"{name}.terminalValuation.lastCompletedBarClose"
    )
    if terminal.get("positionStatus") not in {"no_open_position", "open_position_marked"}:
        raise TemporalSearchContractError(f"{name} terminal position status is invalid")
    if terminal.get("pendingEffectStatus") not in {"none", "unresolved"}:
        raise TemporalSearchContractError(f"{name} terminal pending-effect status is invalid")
    if terminal.get("closedTradeCountDelta") != 0:
        raise TemporalSearchContractError(f"{name} terminal valuation must not add closed trades")
    mark_price = _finite_number(
        terminal.get("markPrice"), name=f"{name}.terminalValuation.markPrice"
    )
    if mark_price <= 0.0:
        raise TemporalSearchContractError(f"{name} terminal mark price must be positive")

    unresolved_position = metrics.get("unresolvedPosition")
    unresolved_pending = metrics.get("unresolvedPendingEffect")
    if not isinstance(unresolved_position, bool) or not isinstance(unresolved_pending, bool):
        raise TemporalSearchContractError(f"{name} unresolved status flags are required")
    has_position = terminal["positionStatus"] == "open_position_marked"
    if unresolved_position != has_position:
        raise TemporalSearchContractError(f"{name} terminal position status disagrees with replay")
    pending_unresolved = terminal["pendingEffectStatus"] == "unresolved"
    if unresolved_pending != pending_unresolved:
        raise TemporalSearchContractError(f"{name} terminal pending status disagrees with replay")
    expected_treatment = (
        "canceled_for_terminal_valuation_only"
        if pending_unresolved
        else "not_applicable"
    )
    if terminal.get("pendingEffectCancellationTreatment") != expected_treatment:
        raise TemporalSearchContractError(f"{name} terminal pending treatment is inconsistent")

    raw_gross = _finite_number(metrics.get("totalGrossR"), name=f"{name}.totalGrossR")
    raw_net = _finite_number(metrics.get("totalNetR"), name=f"{name}.totalNetR")
    raw_cost = _finite_number(
        metrics.get("totalExecutionCostPercent"), name=f"{name}.totalExecutionCostPercent"
    )
    adjusted_gross = _finite_number(
        metrics.get("terminalAdjustedTotalGrossR"),
        name=f"{name}.terminalAdjustedTotalGrossR",
    )
    adjusted_net = _finite_number(
        metrics.get("terminalAdjustedTotalNetR"),
        name=f"{name}.terminalAdjustedTotalNetR",
    )
    adjusted_cost = _finite_number(
        metrics.get("terminalAdjustedTotalExecutionCostPercent"),
        name=f"{name}.terminalAdjustedTotalExecutionCostPercent",
    )
    adjusted_drawdown = _finite_number(
        metrics.get("terminalAdjustedMaxDrawdownR"),
        name=f"{name}.terminalAdjustedMaxDrawdownR",
    )
    if raw_cost < 0.0 or adjusted_cost < 0.0 or adjusted_drawdown < 0.0:
        raise TemporalSearchContractError(f"{name} terminal economics must be nonnegative where required")
    curve_raw = metrics.get("terminalAdjustedEquityCurveR")
    if not isinstance(curve_raw, list):
        raise TemporalSearchContractError(f"{name}.terminalAdjustedEquityCurveR must be an array")
    curve = [
        _finite_number(value, name=f"{name}.terminalAdjustedEquityCurveR[{index}]")
        for index, value in enumerate(curve_raw)
    ]
    if not curve:
        raise TemporalSearchContractError(
            f"{name}.terminalAdjustedEquityCurveR must be a nonempty array"
        )
    _same_number(curve[-1], adjusted_net, name=f"{name} terminal adjusted equity end")
    _same_number(
        _max_drawdown(curve), adjusted_drawdown, name=f"{name} terminal adjusted drawdown"
    )

    if has_position:
        for key in ("positionId", "direction", "grossR", "netR"):
            if terminal.get(key) is None:
                raise TemporalSearchContractError(f"{name} terminal open position is incomplete")
        _sha(terminal["positionId"], name=f"{name}.terminalValuation.positionId")
        if terminal["direction"] not in {"long", "short"}:
            raise TemporalSearchContractError(f"{name} terminal direction is invalid")
        terminal_gross = _finite_number(terminal["grossR"], name=f"{name}.terminalValuation.grossR")
        terminal_net = _finite_number(terminal["netR"], name=f"{name}.terminalValuation.netR")
        exit_cost = _finite_number(
            terminal.get("exitCostPercent"), name=f"{name}.terminalValuation.exitCostPercent"
        )
    else:
        if any(
            terminal.get(key) is not None
            for key in ("positionId", "direction", "grossR", "netR")
        ):
            raise TemporalSearchContractError(f"{name} no-position terminal valuation must be zero")
        terminal_gross = 0.0
        terminal_net = 0.0
        exit_cost = _finite_number(
            terminal.get("exitCostPercent"), name=f"{name}.terminalValuation.exitCostPercent"
        )
        if exit_cost != 0.0:
            raise TemporalSearchContractError(f"{name} no-position terminal valuation must not charge exit cost")
    if exit_cost < 0.0:
        raise TemporalSearchContractError(f"{name} terminal exit cost must be nonnegative")
    _same_number(raw_gross + terminal_gross, adjusted_gross, name=f"{name} terminal gross total")
    _same_number(raw_net + terminal_net, adjusted_net, name=f"{name} terminal net total")
    _same_number(raw_cost + exit_cost, adjusted_cost, name=f"{name} terminal cost total")


def validate_v3_candidate_window_result(
    material: Mapping[str, Any],
    *,
    task_payload: Mapping[str, Any] | None = None,
) -> None:
    """Fail closed on the Stage 5E7-v3 evidence and terminal-economics contract.

    Legacy v1 artifacts intentionally do not enter this function.  They remain
    readable for audit, but cannot impersonate a v3 candidate/window result.
    """
    if not _candidate_window_is_v3(material):
        raise TemporalSearchContractError("candidate-window result is not Stage 5E7-v3 evidence")
    evidence = _mapping(material.get("evidence_contract"), name="worker material evidence_contract")
    if evidence.get("schema_version") != "temporal_graph_candidate_window_evidence_contract_v1":
        raise TemporalSearchContractError("candidate-window v3 evidence contract schema is required")
    start = _stamp(material.get("analysis_window_start"), name="worker material analysis_window_start")
    end = _stamp(material.get("analysis_window_end"), name="worker material analysis_window_end")
    if not _time(start) < _time(end):
        raise TemporalSearchContractError("candidate-window analysis interval must be half-open and nonempty")
    if (
        _stamp(evidence.get("analysis_window_start"), name="evidence analysis_window_start") != start
        or _stamp(evidence.get("analysis_window_end"), name="evidence analysis_window_end") != end
        or evidence.get("analysis_window_end_exclusive") is not True
    ):
        raise TemporalSearchContractError("candidate-window evidence interval is incomplete or inconsistent")
    if task_payload is not None:
        if start != _stamp(task_payload.get("analysis_window_start"), name="task analysis_window_start") or end != _stamp(task_payload.get("analysis_window_end"), name="task analysis_window_end"):
            raise TemporalSearchContractError("candidate-window result interval does not match task")

    # Keep authored input distinct from execution resolution.  The source
    # snapshot identifies the normalized authored profile passed into replay;
    # resolved profile/program identify the evaluator's actual executable.
    source_profile_snapshot = _sha(
        material.get("source_profile_snapshot_sha256"),
        name="worker material source_profile_snapshot_sha256",
    )
    resolved_profile_snapshot = _sha(
        material.get("resolved_profile_snapshot_sha256"),
        name="worker material resolved_profile_snapshot_sha256",
    )
    resolved_program = _sha(
        material.get("program_sha256"),
        name="worker material program_sha256",
    )

    requested = _nonnegative_integer(
        evidence.get("requested_bar_limit"), name="evidence requested_bar_limit", minimum=1
    )
    effective = _nonnegative_integer(
        evidence.get("effective_bar_limit"), name="evidence effective_bar_limit", minimum=1
    )
    if effective < requested:
        raise TemporalSearchContractError("candidate-window effective bar limit is below requested limit")
    if task_payload is not None and requested != _nonnegative_integer(
        task_payload.get("bar_limit"), name="task bar_limit", minimum=1
    ):
        raise TemporalSearchContractError("candidate-window requested bar limit does not match task")
    observation_count = _nonnegative_integer(
        evidence.get("observation_count"), name="evidence observation_count", minimum=1
    )
    first = _stamp(
        evidence.get("first_admitted_observation_timestamp"), name="evidence first observation"
    )
    last = _stamp(
        evidence.get("last_admitted_observation_timestamp"), name="evidence last observation"
    )
    if not (_time(start) <= _time(first) <= _time(last) < _time(end)):
        raise TemporalSearchContractError("candidate-window admitted observation endpoints are not complete half-open evidence")
    warmup = _mapping(evidence.get("warmup_sufficiency"), name="evidence warmup_sufficiency")
    if evidence.get("warmup_sufficient") is not True or warmup.get("sufficient") is not True:
        raise TemporalSearchContractError("candidate-window strict warmup evidence is insufficient")
    if warmup.get("source") == "prebuilt_stream":
        raise TemporalSearchContractError(
            "candidate-window strict warmup evidence must be measured, not a prebuilt-stream fallback"
        )
    excluded_provisional = _nonnegative_integer(
        evidence.get("excluded_provisional_count"), name="evidence excluded_provisional_count"
    )
    excluded_outside = _nonnegative_integer(
        evidence.get("excluded_outside_analysis_window_count"),
        name="evidence excluded_outside_analysis_window_count",
    )
    summary = _mapping(material.get("observation_summary"), name="worker material observation_summary")
    if (
        _nonnegative_integer(summary.get("observation_count"), name="observation summary count", minimum=1) != observation_count
        or _stamp(summary.get("first_bar_start"), name="observation summary first") != first
        or _stamp(summary.get("last_bar_start"), name="observation summary last") != last
    ):
        raise TemporalSearchContractError("candidate-window actual observation evidence disagrees with contract")

    diagnostics = _mapping(material.get("diagnostics"), name="worker material diagnostics")
    diagnostic_bindings = {
        "observation_count": observation_count,
        "requested_bar_limit": requested,
        "effective_bar_limit": effective,
        "warmup_sufficient": True,
        "warmup_sufficiency": warmup,
        "first_admitted_observation_timestamp": first,
        "last_admitted_observation_timestamp": last,
        "excluded_provisional_count": excluded_provisional,
        "excluded_outside_analysis_window_count": excluded_outside,
    }
    for key, expected in diagnostic_bindings.items():
        if diagnostics.get(key) != expected:
            raise TemporalSearchContractError(f"candidate-window diagnostics {key} does not match evidence contract")

    root_stream = _sha(material.get("observation_stream_sha256"), name="worker material observation_stream_sha256")
    cost_results = _mapping(material.get("cost_view_results"), name="worker material cost_view_results")
    if set(cost_results) != set(_COST_VIEWS):
        raise TemporalSearchContractError("candidate-window v3 result requires both cost views")
    path_hashes: list[str] = []
    for cost_view in _COST_VIEWS:
        item = _mapping(cost_results[cost_view], name=f"worker {cost_view} cost view")
        if item.get("cost_view") not in (None, cost_view):
            raise TemporalSearchContractError(f"candidate-window {cost_view} cost view label is inconsistent")
        if _sha(item.get("observation_stream_sha256"), name=f"{cost_view} cost-view stream") != root_stream:
            raise TemporalSearchContractError("candidate-window cost view observation identity mismatch")
        replay = _mapping(item.get("replay_result"), name=f"{cost_view} replay result")
        if _sha(replay.get("streamSha256"), name=f"{cost_view} replay stream") != root_stream:
            raise TemporalSearchContractError("candidate-window replay observation identity mismatch")
        if _sha(
            replay.get("profileSnapshotSha256"),
            name=f"{cost_view} replay profile snapshot",
        ) != resolved_profile_snapshot:
            raise TemporalSearchContractError(
                "candidate-window replay resolved profile identity mismatch"
            )
        if _sha(
            replay.get("programSha256"),
            name=f"{cost_view} replay program",
        ) != resolved_program:
            raise TemporalSearchContractError(
                "candidate-window replay program identity mismatch"
            )
        metrics = _mapping(replay.get("metrics"), name=f"{cost_view} metrics")
        if _nonnegative_integer(
            metrics.get("observationsProcessed"), name=f"{cost_view} observationsProcessed"
        ) != observation_count:
            raise TemporalSearchContractError("candidate-window replay observation count disagrees with evidence")
        _validate_terminal_metrics(
            metrics,
            name=f"{cost_view} metrics",
            expected_last_bar_start=last,
        )
        path_hashes.append(_cost_view_path_sha256(replay, name=f"{cost_view} replay"))
    if path_hashes[0] != path_hashes[1]:
        raise TemporalSearchContractError("candidate-window cost views diverged in non-cost route/path evidence")
    if (
        diagnostics.get("cost_view_decision_path_sha256") != path_hashes[0]
        or diagnostics.get("cost_view_path_parity") != "matched"
        or diagnostics.get("cost_view_count") != len(_COST_VIEWS)
        or diagnostics.get("shared_stream_required") is not True
    ):
        raise TemporalSearchContractError("candidate-window cost-view parity diagnostics are incomplete or inconsistent")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(encoded, encoding="utf-8")


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically update mutable controller state; immutable evidence never uses this."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    fd, temporary = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _normalized_window(raw: Mapping[str, Any], *, name: str) -> dict[str, Any]:
    window = _mapping(raw, name=name)
    allowed = {"windowId", "analysisWindowStart", "analysisWindowEnd"}
    if set(window) != allowed:
        raise TemporalSearchContractError(
            f"{name} must contain exactly {sorted(allowed)!r}"
        )
    start = _stamp(window["analysisWindowStart"], name=f"{name}.analysisWindowStart")
    end = _stamp(window["analysisWindowEnd"], name=f"{name}.analysisWindowEnd")
    if _time(start) >= _time(end):
        raise TemporalSearchContractError(f"{name} start must precede end")
    return {
        "windowId": _safe(window["windowId"], name=f"{name}.windowId"),
        "analysisWindowStart": start,
        "analysisWindowEnd": end,
    }


def _candidate_window_input(
    raw: Mapping[str, Any],
    *,
    name: str,
    candidate: Mapping[str, Any],
    window: Mapping[str, Any],
) -> dict[str, Any]:
    item = _mapping(raw, name=name)
    if set(item) != {"windowId", "evidencePlan"}:
        raise TemporalSearchContractError(
            f"{name} must contain exactly windowId and evidencePlan"
        )
    if _safe(item["windowId"], name=f"{name}.windowId") != window["windowId"]:
        raise TemporalSearchContractError(
            f"{name} does not match its development window"
        )
    plan = _mapping(item["evidencePlan"], name=f"{name}.evidencePlan")
    plan_id = _sha(
        plan.get("plan_id") or plan.get("planId"), name=f"{name}.evidencePlan.planId"
    )
    if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
        raise TemporalSearchContractError(f"{name} requires replay evidence plan v2")
    if "execution_cell_sha256" not in plan:
        raise TemporalSearchContractError(
            f"{name} v2 evidence plan must explicitly declare execution_cell_sha256"
        )
    plan_identity = dict(plan)
    plan_identity.pop("plan_id", None)
    plan_identity.pop("lake_manifest_sha256", None)
    if canonical_sha256(plan_identity) != plan_id:
        raise TemporalSearchContractError(f"{name} evidence plan identity mismatch")
    if (
        plan.get("profile_snapshot_sha256") or plan.get("profileSnapshotSha256")
    ) != candidate["sourceProfileSha256"]:
        raise TemporalSearchContractError(
            f"{name} evidence plan profile snapshot mismatch"
        )
    if (
        _stamp(
            plan.get("analysis_window_start") or plan.get("analysisWindowStart"),
            name=f"{name}.evidencePlan.analysisWindowStart",
        )
        != window["analysisWindowStart"]
        or _stamp(
            plan.get("analysis_window_end") or plan.get("analysisWindowEnd"),
            name=f"{name}.evidencePlan.analysisWindowEnd",
        )
        != window["analysisWindowEnd"]
    ):
        raise TemporalSearchContractError(f"{name} evidence plan window mismatch")
    binding = _mapping(
        plan.get("lake_window_binding") or plan.get("lakeWindowBinding"),
        name=f"{name}.evidencePlan.lakeWindowBinding",
    )
    _sha(
        binding.get("window_semantic_sha256") or binding.get("windowSemanticSha256"),
        name=f"{name}.evidencePlan.lakeWindowBinding.windowSemanticSha256",
    )
    request = _mapping(
        binding.get("request"), name=f"{name}.evidencePlan.lakeWindowBinding.request"
    )
    data_start = _stamp(
        request.get("data_start") or request.get("dataStart"),
        name=f"{name}.lakeBinding.dataStart",
    )
    data_end = _stamp(
        request.get("data_end") or request.get("dataEnd"),
        name=f"{name}.lakeBinding.dataEnd",
    )
    if _time(data_start) > _time(window["analysisWindowStart"]) or _time(
        data_end
    ) < _time(window["analysisWindowEnd"]):
        raise TemporalSearchContractError(
            f"{name} lake binding does not cover the development window"
        )
    pairs = {str(value).strip().upper() for value in request.get("pairs") or []}
    timeframes = {
        str(value).strip().upper() for value in request.get("timeframes") or []
    }
    if candidate["instrument"] not in pairs or candidate["timeframe"] not in timeframes:
        raise TemporalSearchContractError(
            f"{name} lake binding does not cover candidate instrument/timeframe"
        )
    execution_config = _mapping(
        candidate["sourceProfile"].get("executionConfig"),
        name=f"{name}.sourceProfile.executionConfig",
    )
    management_library = execution_config.get("managementLibrary")
    evidence_cell_sha256 = plan["execution_cell_sha256"]
    if evidence_cell_sha256 is not None:
        evidence_cell_sha256 = _sha(
            evidence_cell_sha256,
            name=f"{name}.evidencePlan.execution_cell_sha256",
        )
    if management_library is not None:
        if not isinstance(management_library, Mapping):
            raise TemporalSearchContractError(
                f"{name} managementLibrary must be an object"
            )
        if evidence_cell_sha256 is not None:
            raise TemporalSearchContractError(
                f"{name} scalar-management evidence must not bind a legacy execution cell"
            )
    else:
        exit_policy = _mapping(
            execution_config.get("exitPolicy"),
            name=f"{name}.sourceProfile.executionConfig.exitPolicy",
        )
        selected_cell = _mapping(
            exit_policy.get("selectedCell"),
            name=f"{name}.sourceProfile.executionConfig.exitPolicy.selectedCell",
        )
        expected_cell_sha256 = canonical_sha256(selected_cell)
        if evidence_cell_sha256 != expected_cell_sha256:
            raise TemporalSearchContractError(
                f"{name} legacy evidence execution-cell identity mismatch"
            )
    return {
        "windowId": window["windowId"],
        "evidencePlan": plan,
        "evidencePlanId": plan_id,
        "lakeWindowSemanticSha256": binding.get("window_semantic_sha256")
        or binding.get("windowSemanticSha256"),
    }


def _normalized_candidate(
    raw: Mapping[str, Any], *, name: str, windows: list[dict[str, Any]]
) -> dict[str, Any]:
    candidate = _mapping(raw, name=name)
    allowed = {
        "candidateId",
        "sourceProfile",
        "sourceProfileSha256",
        "instrument",
        "timeframe",
        "barLimit",
        "windowInputs",
    }
    if set(candidate) != allowed:
        raise TemporalSearchContractError(
            f"{name} must contain exactly {sorted(allowed)!r}"
        )
    profile = _mapping(candidate["sourceProfile"], name=f"{name}.sourceProfile")
    profile_sha = _sha(
        candidate["sourceProfileSha256"], name=f"{name}.sourceProfileSha256"
    )
    if canonical_sha256(profile) != profile_sha:
        raise TemporalSearchContractError(f"{name} source profile identity mismatch")
    profile_version = profile.get("version")
    if profile_version == "v2":
        if _mapping(
            profile.get("graph"), name=f"{name}.sourceProfile.graph"
        ).get("kind") != "temporal_graph_v1":
            raise TemporalSearchContractError(
                f"{name} must be a v2 temporal_graph_v1 profile"
            )
    elif profile_version == "v3":
        # A v3 profile is the native compiler's one-position bidirectional
        # snapshot.  Do not reinterpret its graph here: that would create a
        # second compiler.  Its frozen byte snapshot and evidence plan remain
        # subject to every existing candidate/window check below.
        if profile.get("directionMode") != "both":
            raise TemporalSearchContractError(
                f"{name} v3 source profile must use directionMode=both"
            )
    else:
        raise TemporalSearchContractError(
            f"{name} must be a v2 temporal_graph_v1 profile or a v3 bidirectional profile"
        )
    instruments = profile.get("instruments")
    instrument = str(candidate["instrument"] or "").strip().upper()
    if (
        not isinstance(instruments, list)
        or instruments != [instrument]
        or not instrument
    ):
        raise TemporalSearchContractError(
            f"{name} source profile must have exactly the declared instrument"
        )
    timeframe = str(candidate["timeframe"] or "").strip().upper()
    if not timeframe:
        raise TemporalSearchContractError(f"{name}.timeframe is required")
    try:
        limit = int(candidate["barLimit"])
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(
            f"{name}.barLimit must be an integer"
        ) from exc
    if isinstance(candidate["barLimit"], bool) or not 10 <= limit <= 1_000_000:
        raise TemporalSearchContractError(f"{name}.barLimit is outside admitted bounds")
    base = {
        "candidateId": _candidate_id(
            candidate["candidateId"], name=f"{name}.candidateId"
        ),
        "sourceProfile": profile,
        "sourceProfileSha256": profile_sha,
        "instrument": instrument,
        "timeframe": timeframe,
        "barLimit": limit,
    }
    inputs = candidate["windowInputs"]
    if not isinstance(inputs, list) or len(inputs) != len(windows):
        raise TemporalSearchContractError(
            f"{name}.windowInputs must bind every development window exactly once"
        )
    indexed = {
        _safe(
            _mapping(item, name=f"{name}.windowInputs").get("windowId"),
            name=f"{name}.windowInputs.windowId",
        ): item
        for item in inputs
    }
    if len(indexed) != len(inputs) or set(indexed) != {
        window["windowId"] for window in windows
    }:
        raise TemporalSearchContractError(
            f"{name}.windowInputs does not exactly cover development windows"
        )
    base["windowInputs"] = [
        _candidate_window_input(
            indexed[window["windowId"]],
            name=f"{name}.windowInputs[{window['windowId']}]",
            candidate=base,
            window=window,
        )
        for window in windows
    ]
    if len({entry["evidencePlanId"] for entry in base["windowInputs"]}) != len(
        base["windowInputs"]
    ):
        raise TemporalSearchContractError(
            f"{name} evidence plan identities must be unique by candidate/window"
        )
    return base


def _no_overlap(window: dict[str, Any], protected: list[dict[str, Any]]) -> None:
    for item in protected:
        if _time(window["analysisWindowStart"]) < _time(
            item["analysisWindowEnd"]
        ) and _time(item["analysisWindowStart"]) < _time(window["analysisWindowEnd"]):
            raise TemporalSearchContractError(
                f"development window {window['windowId']!r} overlaps prohibited evidence {item['windowId']!r}"
            )


def build_authority(preparation: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze a finite matrix; no profile mutation or date discovery is permitted."""
    payload = _mapping(preparation, name="preparation")
    if payload.get("schemaVersion") != TEMPORAL_SEARCH_PREPARATION_SCHEMA:
        raise TemporalSearchContractError("unknown temporal search preparation schema")
    allowed = {
        "schemaVersion",
        "authorityLabel",
        "workerContract",
        "candidates",
        "developmentWindows",
        "prohibitedEvidence",
        "bounds",
    }
    if set(payload) != allowed:
        raise TemporalSearchContractError(
            f"preparation must contain exactly {sorted(allowed)!r}"
        )
    worker = _mapping(payload["workerContract"], name="workerContract")
    if set(worker) != {"workerContractSha256", "workerContractSchema"}:
        raise TemporalSearchContractError(
            "workerContract must contain workerContractSha256 and workerContractSchema"
        )
    worker_sha = _sha(
        worker["workerContractSha256"], name="workerContract.workerContractSha256"
    )
    worker_schema = _safe(
        worker["workerContractSchema"], name="workerContract.workerContractSchema"
    )
    bounds = _mapping(payload["bounds"], name="bounds")
    if set(bounds) != {
        "maxCandidates",
        "maxDevelopmentWindows",
        "maxTasks",
        "maxAttempts",
        "deadlineSeconds",
    }:
        raise TemporalSearchContractError("bounds must be closed and explicit")
    normalized_bounds: dict[str, int | float] = {}
    for key in ("maxCandidates", "maxDevelopmentWindows", "maxTasks", "maxAttempts"):
        try:
            normalized_bounds[key] = int(bounds[key])
        except (TypeError, ValueError) as exc:
            raise TemporalSearchContractError(
                f"bounds.{key} must be an integer"
            ) from exc
        if not 1 <= normalized_bounds[key] <= 100_000:
            raise TemporalSearchContractError(f"bounds.{key} is outside safe limits")
    try:
        normalized_bounds["deadlineSeconds"] = float(bounds["deadlineSeconds"])
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(
            "bounds.deadlineSeconds must be numeric"
        ) from exc
    if not 1 <= normalized_bounds["deadlineSeconds"] <= 86_400:
        raise TemporalSearchContractError(
            "bounds.deadlineSeconds is outside safe limits"
        )
    protected_raw = payload["prohibitedEvidence"]
    if not isinstance(protected_raw, list):
        raise TemporalSearchContractError("prohibitedEvidence must be a list")
    protected: list[dict[str, Any]] = []
    for index, item in enumerate(protected_raw):
        current = _mapping(item, name=f"prohibitedEvidence[{index}]")
        if set(current) != {
            "windowId",
            "analysisWindowStart",
            "analysisWindowEnd",
            "reason",
        }:
            raise TemporalSearchContractError(
                "prohibited evidence entries have a closed schema"
            )
        protected.append(
            {
                "windowId": _safe(
                    current["windowId"], name="prohibitedEvidence.windowId"
                ),
                "analysisWindowStart": _stamp(
                    current["analysisWindowStart"], name="prohibitedEvidence.start"
                ),
                "analysisWindowEnd": _stamp(
                    current["analysisWindowEnd"], name="prohibitedEvidence.end"
                ),
                "reason": str(current["reason"] or "").strip(),
            }
        )
    if not protected or any(
        not item["reason"]
        or _time(item["analysisWindowStart"]) >= _time(item["analysisWindowEnd"])
        for item in protected
    ):
        raise TemporalSearchContractError(
            "prohibited evidence must explicitly identify non-empty protected/reserved windows"
        )
    raw_windows = payload["developmentWindows"]
    if not isinstance(raw_windows, list) or not raw_windows:
        raise TemporalSearchContractError("developmentWindows must be non-empty")
    windows = [
        _normalized_window(item, name=f"developmentWindows[{index}]")
        for index, item in enumerate(raw_windows)
    ]
    if len({x["windowId"] for x in windows}) != len(windows):
        raise TemporalSearchContractError("development window IDs must be unique")
    for window in windows:
        _no_overlap(window, protected)
    raw_candidates = payload["candidates"]
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise TemporalSearchContractError("candidates must be non-empty")
    candidates = [
        _normalized_candidate(item, name=f"candidates[{index}]", windows=windows)
        for index, item in enumerate(raw_candidates)
    ]
    if len({x["candidateId"] for x in candidates}) != len(candidates):
        raise TemporalSearchContractError("candidate IDs must be unique")
    task_count = len(candidates) * len(windows)
    if (
        len(candidates) > normalized_bounds["maxCandidates"]
        or len(windows) > normalized_bounds["maxDevelopmentWindows"]
        or task_count > normalized_bounds["maxTasks"]
    ):
        raise TemporalSearchContractError("finite task matrix exceeds authority bounds")
    normalized_preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": _safe(payload["authorityLabel"], name="authorityLabel"),
        "workerContract": {
            "workerContractSha256": worker_sha,
            "workerContractSchema": worker_schema,
        },
        "bounds": normalized_bounds,
        "prohibitedEvidence": protected,
        "developmentWindows": windows,
        "candidates": candidates,
    }
    authority = {
        "schemaVersion": TEMPORAL_SEARCH_AUTHORITY_SCHEMA,
        "authorityLabel": normalized_preparation["authorityLabel"],
        "preparationSha256": canonical_sha256(normalized_preparation),
        "workerContract": normalized_preparation["workerContract"],
        "bounds": normalized_preparation["bounds"],
        "taskContract": {
            "taskKind": TEMPORAL_SEARCH_TASK_KIND,
            "jobSchema": TEMPORAL_SEARCH_JOB_SCHEMA,
            "capability": TEMPORAL_SEARCH_CAPABILITY,
            "resultSchema": TEMPORAL_SEARCH_RESULT_SCHEMA,
            "costViews": list(_COST_VIEWS),
            "requiredWorkerCapabilities": list(_REQUIRED_WORKER_CAPABILITIES),
        },
        "prohibitedEvidence": protected,
        "developmentWindows": windows,
        "candidates": candidates,
        "executionPolicy": {
            "controllerOwns": [
                "generation",
                "validation",
                "checkpoint",
                "journal",
                "resume",
                "dedup",
                "materialization",
                "basic_selection",
            ],
            "workerOnly": ["evaluate_immutable_job"],
            "mutationEnginePermitted": False,
            "longEconomicSearchPermitted": False,
            "reservedEvidencePermitted": False,
        },
    }
    authority["authorityId"] = canonical_sha256(authority)
    return authority


def validate_authority(authority: Mapping[str, Any]) -> dict[str, Any]:
    current = _mapping(authority, name="authority")
    supplied = _sha(current.pop("authorityId", None), name="authority.authorityId")
    # Rebuild validates closed schemas and the exact authority identity.
    source_candidates = []
    for candidate in current.get("candidates", []):
        copied = _mapping(candidate, name="authority.candidate")
        copied["windowInputs"] = [
            {
                "windowId": entry.get("windowId"),
                "evidencePlan": entry.get("evidencePlan"),
            }
            for entry in copied.get("windowInputs", [])
        ]
        source_candidates.append(copied)
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": current.get("authorityLabel"),
        "workerContract": current.get("workerContract"),
        "candidates": source_candidates,
        "developmentWindows": current.get("developmentWindows"),
        "prohibitedEvidence": current.get("prohibitedEvidence"),
        "bounds": current.get("bounds"),
    }
    rebuilt = build_authority(preparation)
    if (
        current
        != {key: value for key, value in rebuilt.items() if key != "authorityId"}
        or supplied != rebuilt["authorityId"]
    ):
        raise TemporalSearchContractError(
            "authority identity or immutable semantics mismatch"
        )
    return rebuilt


def build_task_matrix(authority: Mapping[str, Any]) -> list[dict[str, Any]]:
    frozen = validate_authority(authority)
    return _build_task_matrix_validated(frozen)


def _build_task_matrix_validated(frozen: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build worker tasks from an authority already validated by this module."""

    tasks: list[dict[str, Any]] = []
    for candidate in frozen["candidates"]:
        for window in frozen["developmentWindows"]:
            input_by_window = {
                item["windowId"]: item for item in candidate["windowInputs"]
            }
            evidence = input_by_window[window["windowId"]]
            shared_id = canonical_sha256(
                {
                    "candidateSnapshotSha256": candidate["sourceProfileSha256"],
                    "evidencePlanId": evidence["evidencePlanId"],
                    "windowId": window["windowId"],
                    "windowSemanticSha256": evidence["lakeWindowSemanticSha256"],
                }
            )
            identity = {
                "authorityId": frozen["authorityId"],
                "candidateId": candidate["candidateId"],
                "windowId": window["windowId"],
            }
            task_id = (
                "temporal-search-"
                + canonical_sha256(identity).removeprefix("sha256:")[:32]
            )
            profile = candidate["sourceProfile"]
            execution_config = _mapping(
                profile.get("executionConfig"),
                name=f"candidate[{candidate['candidateId']}].sourceProfile.executionConfig",
            )
            management_library = execution_config.get("managementLibrary")
            execution_binding: dict[str, Any]
            if management_library is not None:
                if not isinstance(management_library, Mapping):
                    raise TemporalSearchContractError(
                        "managementLibrary must be an object"
                    )
                execution_binding = {
                    "execution_config_sha256": canonical_sha256(execution_config)
                }
            else:
                exit_policy = _mapping(
                    execution_config.get("exitPolicy"),
                    name="sourceProfile.executionConfig.exitPolicy",
                )
                execution_binding = {
                    "execution_cell": _mapping(
                        exit_policy.get("selectedCell"),
                        name="sourceProfile.executionConfig.exitPolicy.selectedCell",
                    )
                }
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
                "evaluator_id": _evaluator_id_for_profile(profile),
                "analysis_window_start": window["analysisWindowStart"],
                "analysis_window_end": window["analysisWindowEnd"],
                "evidence_plan": evidence["evidencePlan"],
                "required_worker_contract_hash": frozen["workerContract"][
                    "workerContractSha256"
                ],
                "required_worker_contract_schema": frozen["workerContract"][
                    "workerContractSchema"
                ],
                "required_capabilities": list(_REQUIRED_WORKER_CAPABILITIES),
                "client_origin": "temporal_search_controller",
                "campaign_id": frozen["authorityId"],
                "lane_id": candidate["candidateId"],
                "attempt_id": task_id,
                **execution_binding,
            }
            tasks.append(
                {
                    "task_id": task_id,
                    "lane_id": candidate["candidateId"],
                    "attempt_id": task_id,
                    "task_kind": TEMPORAL_SEARCH_TASK_KIND,
                    "payload": job,
                    "required_worker_capabilities": list(_REQUIRED_WORKER_CAPABILITIES),
                    "deadline_seconds": frozen["bounds"]["deadlineSeconds"],
                    "max_attempts": frozen["bounds"]["maxAttempts"],
                }
            )
    if len({task["task_id"] for task in tasks}) != len(tasks):
        raise TemporalSearchContractError("task identity collision")
    return tasks


def materialize_plan(
    authority: Mapping[str, Any], output_root: Path | str
) -> dict[str, Any]:
    frozen = validate_authority(authority)
    tasks = _build_task_matrix_validated(frozen)
    root = Path(output_root)
    manifest = {
        "schemaVersion": TEMPORAL_SEARCH_MANIFEST_SCHEMA,
        "authorityId": frozen["authorityId"],
        "taskCount": len(tasks),
        "tasks": tasks,
        "taskMatrixSha256": canonical_sha256(tasks),
    }
    _write_json(root / "authority.json", frozen)
    _write_json(root / "task-manifest.json", manifest)
    checkpoint_path = root / "checkpoint.json"
    if checkpoint_path.exists():
        checkpoint = _mapping(
            json.loads(checkpoint_path.read_text(encoding="utf-8")), name="checkpoint"
        )
        if (
            checkpoint.get("schemaVersion") != TEMPORAL_SEARCH_CHECKPOINT_SCHEMA
            or checkpoint.get("authorityId") != frozen["authorityId"]
            or checkpoint.get("taskMatrixSha256") != manifest["taskMatrixSha256"]
        ):
            raise TemporalSearchContractError(
                "existing checkpoint does not bind this immutable authority and task matrix"
            )
    else:
        _write_checkpoint(
            checkpoint_path,
            {
                "schemaVersion": TEMPORAL_SEARCH_CHECKPOINT_SCHEMA,
                "authorityId": frozen["authorityId"],
                "taskMatrixSha256": manifest["taskMatrixSha256"],
                "completed": {},
                "journal": [],
            },
        )
    return manifest


def _require_completion_routing(
    task: Mapping[str, Any], completion: Mapping[str, Any]
) -> None:
    if (
        completion.get("task_id") != task.get("task_id")
        or completion.get("lane_id") != task.get("lane_id")
        or completion.get("attempt_id") != task.get("attempt_id")
    ):
        raise TemporalSearchContractError("completion routing identity mismatch")


def _classified_deterministic_rejection(
    completion: Mapping[str, Any],
) -> Any | None:
    """Recognize only the explicit deterministic scoring exhaustion signal.

    A worker failure remains infrastructure-fatal by default.  The one exception
    is deliberately tied to the exact core exception field emitted by the
    gateway; matching a phrase in a traceback would hide worker bugs.
    """

    nested = completion.get("result")
    if isinstance(nested, Mapping) and (
        nested.get("status") == "failed"
        and nested.get("error_type") == _WARMUP_ERROR_TYPE
    ):
        return (_WARMUP_REJECTION_CODE, _clone(nested, name="nested worker warmup failure"))
    if isinstance(nested, Mapping) and (
        nested.get("status") == "failed"
        and nested.get("error_type") == _BREAK_EVEN_ERROR_TYPE
        and nested.get("error") == _BREAK_EVEN_ERROR
    ):
        return (_BREAK_EVEN_REJECTION_CODE, _clone(nested, name="nested worker invariant failure"))
    # Older gateway versions supplied the same exact type in the top-level
    # error mapping.  Retain that compatible closed form, not a substring scan.
    top_level = completion.get("error")
    if isinstance(top_level, Mapping) and top_level.get("type") == _WARMUP_ERROR_TYPE:
        return (_WARMUP_REJECTION_CODE, _clone(top_level, name="top-level worker warmup failure"))
    return None


def _finalize_rejected_material(material: dict[str, Any]) -> dict[str, Any]:
    """Bind a no-replay rejection to the same immutable artifact convention."""

    artifact = _clone(material, name="rejected candidate-window material")
    artifact["artifact_sha256"] = canonical_sha256(artifact)
    artifact_size = 1
    for _ in range(16):
        artifact["artifact_size_bytes"] = artifact_size
        next_size = len(
            json.dumps(
                artifact,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        )
        if next_size == artifact_size:
            return artifact
        artifact_size = next_size
    raise TemporalSearchContractError("could not stabilize rejected result byte count")


def _rejected_result_material(
    task: Mapping[str, Any], completion: Mapping[str, Any]
) -> dict[str, Any]:
    """Materialize the sole permitted terminal non-replay outcome.

    This is intentionally not a successful replay result: it carries no stream,
    execution identity, observation count, or economic metric.  Its immutable
    provenance makes the deterministic rejection restart-safe and auditable.
    """

    _require_completion_routing(task, completion)
    classified = _classified_deterministic_rejection(completion)
    if classified is None:
        raise TemporalSearchContractError("worker failure is not a classified warmup rejection")
    reason_code, error = classified
    replay_executed = reason_code == _BREAK_EVEN_REJECTION_CODE
    outcome = {
        "schema_version": (
            "temporal_candidate_window_rejection_v2"
            if replay_executed
            else "temporal_candidate_window_rejection_v1"
        ),
        "disposition": "rejected",
        "reason_code": reason_code,
        "replay_executed": replay_executed,
        "worker_attempt_id": completion["attempt_id"],
        "worker_lease_id": completion["lease_id"],
        "worker_error": error,
        "worker_error_sha256": canonical_sha256(error),
        "worker_completion_sha256": canonical_sha256(completion),
    }
    if replay_executed:
        outcome["replay_completed"] = False
    job = _mapping(task.get("payload"), name="task payload")
    return _finalize_rejected_material(
        {
            "schema_version": TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA,
            "task_kind": TEMPORAL_SEARCH_TASK_KIND,
            "job_id": job["job_id"],
            "authority_id": job["authority_id"],
            "candidate_id": job["candidate_id"],
            "evidence_plan_id": job["evidence_plan"]["plan_id"],
            "lake_window_semantic_sha256": job["lake_window_semantic_sha256"],
            "shared_observation_stream_id": job["shared_observation_stream_id"],
            "analysis_window_start": job["analysis_window_start"],
            "analysis_window_end": job["analysis_window_end"],
            "evaluation_outcome": outcome,
        }
    )


def is_warmup_rejected_candidate_window_result(material: Mapping[str, Any]) -> bool:
    return material.get("schema_version") == TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA


def validate_warmup_rejected_candidate_window_result(
    material: Mapping[str, Any], *, task_payload: Mapping[str, Any] | None = None
) -> None:
    """Validate a deterministic, no-replay warmup rejection artifact."""

    if material.get("schema_version") != TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA:
        raise TemporalSearchContractError("candidate-window result is not a warmup rejection")
    if material.get("task_kind") != TEMPORAL_SEARCH_TASK_KIND:
        raise TemporalSearchContractError("warmup rejection task kind is invalid")
    outcome = _mapping(material.get("evaluation_outcome"), name="warmup rejection outcome")
    expected_common = {
        "schema_version",
        "disposition",
        "reason_code",
        "replay_executed",
        "worker_attempt_id",
        "worker_lease_id",
        "worker_error",
        "worker_error_sha256",
        "worker_completion_sha256",
    }
    v1 = outcome.get("schema_version") == "temporal_candidate_window_rejection_v1"
    v2 = outcome.get("schema_version") == "temporal_candidate_window_rejection_v2"
    expected = expected_common | ({"replay_completed"} if v2 else set())
    if set(outcome) != expected or outcome.get("disposition") != "rejected":
        raise TemporalSearchContractError("warmup rejection outcome is invalid")
    if not (
        (v1 and outcome.get("reason_code") == _WARMUP_REJECTION_CODE and outcome.get("replay_executed") is False)
        or (v2 and outcome.get("reason_code") == _BREAK_EVEN_REJECTION_CODE and outcome.get("replay_executed") is True and outcome.get("replay_completed") is False)
    ):
        raise TemporalSearchContractError("rejection replay execution state is invalid")
    _safe(outcome.get("worker_attempt_id"), name="warmup rejection worker attempt")
    _safe(outcome.get("worker_lease_id"), name="warmup rejection worker lease")
    error = _clone(outcome.get("worker_error"), name="warmup rejection worker error")
    if canonical_sha256(error) != _sha(outcome.get("worker_error_sha256"), name="warmup rejection error hash"):
        raise TemporalSearchContractError("warmup rejection error identity mismatch")
    _sha(outcome.get("worker_completion_sha256"), name="warmup rejection completion hash")
    for key in (
        "job_id", "authority_id", "candidate_id", "evidence_plan_id",
        "lake_window_semantic_sha256", "shared_observation_stream_id",
        "analysis_window_start", "analysis_window_end",
    ):
        if not isinstance(material.get(key), str) or not material[key]:
            raise TemporalSearchContractError(f"warmup rejection {key} is required")
    if task_payload is not None:
        expected = {
            "job_id": task_payload.get("job_id"),
            "authority_id": task_payload.get("authority_id"),
            "candidate_id": task_payload.get("candidate_id"),
            "evidence_plan_id": (task_payload.get("evidence_plan") or {}).get("plan_id"),
            "lake_window_semantic_sha256": task_payload.get("lake_window_semantic_sha256"),
            "shared_observation_stream_id": task_payload.get("shared_observation_stream_id"),
            "analysis_window_start": task_payload.get("analysis_window_start"),
            "analysis_window_end": task_payload.get("analysis_window_end"),
        }
        if any(material.get(key) != value for key, value in expected.items()):
            raise TemporalSearchContractError("warmup rejection does not match task")
    artifact = _clone(material, name="warmup rejection artifact")
    supplied_sha = _sha(artifact.pop("artifact_sha256", None), name="warmup rejection artifact hash")
    supplied_size = artifact.pop("artifact_size_bytes", None)
    if not isinstance(supplied_size, int) or supplied_size < 1:
        raise TemporalSearchContractError("warmup rejection artifact byte count is invalid")
    if canonical_sha256(artifact) != supplied_sha:
        raise TemporalSearchContractError("warmup rejection artifact identity mismatch")
    if len(json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")) != supplied_size:
        raise TemporalSearchContractError("warmup rejection artifact byte count mismatch")


def _result_material(
    task: Mapping[str, Any], completion: Mapping[str, Any]
) -> dict[str, Any]:
    if str(completion.get("status") or "").lower() != "success":
        raise TemporalSearchContractError("worker completion is not successful")
    _require_completion_routing(task, completion)
    envelope = _mapping(completion.get("result"), name="worker envelope")
    if (
        envelope.get("status") != "success"
        or envelope.get("job_kind") != TEMPORAL_SEARCH_TASK_KIND
    ):
        raise TemporalSearchContractError(
            "worker envelope does not prove a successful temporal candidate/window job"
        )
    material = _mapping(envelope.get("result"), name="worker material result")
    job = _mapping(task.get("payload"), name="task payload")
    required = {
        "schema_version": TEMPORAL_SEARCH_RESULT_SCHEMA,
        "task_kind": TEMPORAL_SEARCH_TASK_KIND,
        "job_id": job["job_id"],
        "authority_id": job["authority_id"],
        "candidate_id": job["candidate_id"],
        "evidence_plan_id": job["evidence_plan"]["plan_id"],
        "lake_window_semantic_sha256": job["lake_window_semantic_sha256"],
        "shared_observation_stream_id": job["shared_observation_stream_id"],
    }
    for key, expected in required.items():
        if material.get(key) != expected:
            raise TemporalSearchContractError(
                f"worker material result mismatch for {key}"
            )
    cost_results = _mapping(
        material.get("cost_view_results"), name="worker material cost_view_results"
    )
    if set(cost_results) != set(_COST_VIEWS):
        raise TemporalSearchContractError(
            "worker result must contain exactly both admitted cost views"
        )
    stream_hashes: set[str] = set()
    for cost_view in _COST_VIEWS:
        replay = _mapping(
            cost_results[cost_view],
            name=f"worker material cost_view_results.{cost_view}",
        )
        if replay.get("cost_view") not in (None, cost_view):
            raise TemporalSearchContractError(
                f"worker cost result mismatch for {cost_view}"
            )
        stream_hashes.add(
            _sha(
                replay.get("observation_stream_sha256"),
                name=f"worker material cost_view_results.{cost_view}.observation_stream_sha256",
            )
        )
    if len(stream_hashes) != 1:
        raise TemporalSearchContractError(
            "both cost views must be evaluated from the identical observation stream"
        )
    validate_v3_candidate_window_result(material, task_payload=job)
    artifact_sha256 = _sha(
        material.get("artifact_sha256"), name="worker material artifact_sha256"
    )
    artifact_size_bytes = material.get("artifact_size_bytes")
    if (
        isinstance(artifact_size_bytes, bool)
        or not isinstance(artifact_size_bytes, int)
        or artifact_size_bytes < 1
    ):
        raise TemporalSearchContractError(
            "worker material artifact_size_bytes must be a positive integer"
        )
    if (
        len(
            json.dumps(
                material,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        )
        != artifact_size_bytes
    ):
        raise TemporalSearchContractError(
            "worker material artifact byte count mismatch"
        )
    artifact_identity = _clone(material, name="worker material artifact identity")
    artifact_identity.pop("artifact_sha256", None)
    artifact_identity.pop("artifact_size_bytes", None)
    diagnostics = _mapping(
        artifact_identity.get("diagnostics"), name="worker material diagnostics"
    )
    diagnostics.pop("artifact_size_bytes", None)
    artifact_identity["diagnostics"] = diagnostics
    if canonical_sha256(artifact_identity) != artifact_sha256:
        raise TemporalSearchContractError("worker material artifact identity mismatch")
    return material


def _result_codec_fields(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """Checkpoint names stay explicit about semantic and blob identities."""
    return {
        "resultCodec": metadata["codec"],
        "resultSemanticSha256": metadata["semanticSha256"],
        "resultSemanticSizeBytes": metadata["semanticSizeBytes"],
        "resultUncompressedSha256": metadata["uncompressedSha256"],
        "resultUncompressedSizeBytes": metadata["uncompressedSizeBytes"],
        "resultBlobSha256": metadata["blobSha256"],
        "resultBlobSizeBytes": metadata["blobSizeBytes"],
    }


def _result_codec_expected(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        metadata_key: record[record_key]
        for metadata_key, record_key in (
            ("codec", "resultCodec"),
            ("semanticSha256", "resultSemanticSha256"),
            ("semanticSizeBytes", "resultSemanticSizeBytes"),
            ("uncompressedSha256", "resultUncompressedSha256"),
            ("uncompressedSizeBytes", "resultUncompressedSizeBytes"),
            ("blobSha256", "resultBlobSha256"),
            ("blobSizeBytes", "resultBlobSizeBytes"),
        )
        if record_key in record
    }


def _read_checkpoint_result(record: Mapping[str, Any]) -> dict[str, Any]:
    raw_path = record.get("resultPath")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise TemporalSearchContractError("checkpoint result path is required")
    path = Path(raw_path)
    codec_fields = (
        "resultCodec",
        "resultSemanticSha256",
        "resultSemanticSizeBytes",
        "resultUncompressedSha256",
        "resultUncompressedSizeBytes",
        "resultBlobSha256",
        "resultBlobSizeBytes",
    )
    present_codec_fields = [key for key in codec_fields if key in record]
    if present_codec_fields and len(present_codec_fields) != len(codec_fields):
        raise TemporalSearchContractError(
            "checkpoint result representation metadata is incomplete"
        )
    try:
        payload, metadata = _read_codec_json_object(
            path,
            expected=_result_codec_expected(record),
        )
    except ResultCodecError as exc:
        raise TemporalSearchContractError(f"invalid materialized temporal result: {path}") from exc
    expected_sha = record.get("resultSha256")
    if expected_sha is not None and metadata["semanticSha256"] != expected_sha:
        raise TemporalSearchContractError(f"materialized temporal result hash drift: {path}")
    return payload


def run_temporal_search_tasks(
    client: LabGatewayClientProtocol,
    authority: Mapping[str, Any],
    *,
    output_root: Path | str,
    timeout_seconds: float = 900.0,
    poll_interval_seconds: float = 0.25,
    resume: bool = False,
    enqueue_batch_size: int = 128,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    include_selection_summary: bool = True,
) -> dict[str, Any]:
    """Execute an already finite plan; intentionally no search expansion occurs."""
    manifest = materialize_plan(authority, output_root)
    root = Path(output_root)
    checkpoint_path = root / "checkpoint.json"
    checkpoint = _mapping(
        json.loads(checkpoint_path.read_text(encoding="utf-8")), name="checkpoint"
    )
    if not resume and checkpoint["completed"]:
        raise TemporalSearchContractError(
            "non-resume temporal search already has completed tasks"
        )
    tasks = {item["task_id"]: item for item in manifest["tasks"]}
    completed = _mapping(checkpoint["completed"], name="checkpoint.completed")

    def persist(task: Mapping[str, Any], material: Mapping[str, Any]) -> None:
        digest = canonical_sha256(material)
        task_id = str(task["task_id"])
        prior = completed.get(task_id)
        if prior is not None:
            if not isinstance(prior, Mapping) or prior.get("resultSha256") != digest:
                raise TemporalSearchContractError("conflicting duplicate temporal search result")
            persisted = _read_checkpoint_result(prior)
            if canonical_sha256(persisted) != digest:
                raise TemporalSearchContractError("conflicting duplicate temporal search result")
            return
        result_path = root / "results" / f"{task_id}.json.gz"
        try:
            metadata = write_gzip_json_once(result_path, material)
        except ResultCodecError as exc:
            raise TemporalSearchContractError(
                f"could not materialize compressed temporal result: {result_path}"
            ) from exc
        record = {
            "resultSha256": digest,
            "resultPath": str(result_path),
            "candidateId": task["payload"]["candidate_id"],
            **_result_codec_fields(metadata),
        }
        if is_warmup_rejected_candidate_window_result(material):
            record["outcome"] = "rejected"
            record["rejectionCode"] = material["evaluation_outcome"]["reason_code"]
        completed[task_id] = record
        checkpoint["completed"] = completed
        checkpoint["journal"] = list(checkpoint.get("journal") or []) + [
            {"taskId": task_id, **record}
        ]
        _write_checkpoint(checkpoint_path, checkpoint)

    def consume(completion: Mapping[str, Any]) -> None:
        task_id = str(completion.get("task_id") or "")
        task = tasks.get(task_id)
        if task is None:
            raise TemporalSearchContractError("unrelated Lab result encountered")
        if str(completion.get("status") or "").lower() != "success":
            _require_completion_routing(task, completion)
            lease = _safe(completion.get("lease_id"), name="completion.lease_id")
            failure = _mapping(completion, name="failed worker completion")
            _write_json(root / "failures" / f"{task_id}.json", failure)
            if client.ack_results([lease]) != 1:
                raise TemporalSearchContractError(
                    "gateway did not acknowledge failed temporal search result"
                )
            if _classified_deterministic_rejection(failure) is not None:
                rejected = _rejected_result_material(task, failure)
                persist(task, rejected)
                if progress_callback is not None:
                    progress_callback(
                        {
                            "taskId": task_id,
                            "completedTaskCount": len(completed),
                            "taskCount": len(tasks),
                            "outcome": "rejected",
                            "rejectionCode": rejected["evaluation_outcome"]["reason_code"],
                        }
                    )
                return
            detail = (
                failure.get("error") or failure.get("result") or failure.get("status")
            )
            encoded_detail = json.dumps(
                detail,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            raise TemporalSearchContractError(
                f"worker completion failed for {task_id}: {encoded_detail[:2000]}"
            )
        material = _result_material(task, completion)
        lease = _safe(completion.get("lease_id"), name="completion.lease_id")
        persist(task, material)
        if client.ack_results([lease]) != 1:
            raise TemporalSearchContractError(
                "gateway did not acknowledge temporal search result"
            )
        if progress_callback is not None:
            progress_callback(
                {
                    "taskId": task_id,
                    "completedTaskCount": len(completed),
                    "taskCount": len(tasks),
                }
            )

    # Recover an already-acknowledged deterministic failure before enqueue.  The
    # failure receipt is durable independently of the checkpoint write, so a
    # crash between acknowledgement and checkpointing cannot resurrect a task.
    for task_id, task in tasks.items():
        if task_id in completed:
            continue
        failure_path = root / "failures" / f"{task_id}.json"
        if not failure_path.is_file():
            continue
        failure = _mapping(
            json.loads(failure_path.read_text(encoding="utf-8")),
            name="persisted failed worker completion",
        )
        if _classified_deterministic_rejection(failure) is not None:
            persist(task, _rejected_result_material(task, failure))

    # Consume a prior delivery before enqueue so restart after materialization is
    # idempotent and does not create a second economic evaluation.
    for completion in client.read_results(limit=max(8, len(tasks) * 2)):
        consume(completion)
    pending = [task for task_id, task in tasks.items() if task_id not in completed]
    if isinstance(enqueue_batch_size, bool) or not 1 <= int(enqueue_batch_size) <= 1000:
        raise TemporalSearchContractError(
            "enqueue batch size must be between 1 and 1000"
        )
    for start in range(0, len(pending), int(enqueue_batch_size)):
        batch = pending[start : start + int(enqueue_batch_size)]
        receipt = client.enqueue_tasks(batch)
        expected = len(batch)
        enqueued = int(receipt.get("enqueued") or 0)
        if enqueued != expected:
            # After a controller timeout, Resume may find pending tasks that the
            # Gateway still owns as queued, leased, or recently completed.  The
            # Gateway reports those exact task identities as explicit duplicate
            # rejections; they must not be evaluated a second time.  Fresh keeps
            # the stricter all-new enqueue contract.
            submitted = receipt.get("submitted")
            rejected = receipt.get("rejected")
            resume_duplicates_are_explicit = (
                resume
                and submitted is not None
                and rejected is not None
                and int(submitted) == expected
                and int(rejected) == expected - enqueued
                and 0 <= enqueued <= expected
            )
            if not resume_duplicates_are_explicit:
                raise TemporalSearchContractError(
                    "gateway did not enqueue the exact pending task set batch"
                )
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while pending:
        if time.monotonic() >= deadline:
            raise TemporalSearchTimeout("timed out waiting for temporal search results")
        results = client.read_results(limit=max(8, len(pending) * 2))
        if not results:
            time.sleep(max(float(poll_interval_seconds), 0.01))
            continue
        for completion in results:
            consume(completion)
            pending = [
                item
                for item in pending
                if item["task_id"] != str(completion.get("task_id") or "")
            ]
    rows = []
    # Basic selection is intentionally transparent and optional: rank finite
    # numeric score only.  Temporal QD delegates result reduction and full
    # result validation to its native campaign seal, so it explicitly skips
    # this otherwise redundant serial reopen of every compressed result.
    for task_id, item in completed.items():
        if not isinstance(item, Mapping):
            raise TemporalSearchContractError(
                "checkpoint completion must be an object"
            )
        if not include_selection_summary:
            continue
        result = _read_checkpoint_result(item)
        score = result.get("selection_score")
        if isinstance(score, (int, float)) and not isinstance(score, bool):
            rows.append(
                {
                    "taskId": task_id,
                    "candidateId": item["candidateId"],
                    "selectionScore": float(score),
                }
            )
    if include_selection_summary:
        rows.sort(
            key=lambda row: (-row["selectionScore"], row["candidateId"], row["taskId"])
        )
    summary = {
        "schemaVersion": "temporal_graph_candidate_window_run_result_v1",
        "authorityId": manifest["authorityId"],
        "taskCount": len(tasks),
        "completedTaskCount": len(completed),
        "selection": rows,
    }
    _write_json(root / "summary.json", summary)
    return summary


__all__ = [name for name in globals() if name.startswith("TEMPORAL_SEARCH_")] + [
    "TemporalSearchContractError",
    "TemporalSearchError",
    "TemporalSearchTimeout",
    "build_authority",
    "build_task_matrix",
    "canonical_sha256",
    "materialize_plan",
    "run_temporal_search_tasks",
    "validate_authority",
]
