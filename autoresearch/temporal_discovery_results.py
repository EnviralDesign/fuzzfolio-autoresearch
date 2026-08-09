from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import statistics
import subprocess
import tempfile
from typing import Any, Protocol

from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    is_v3_candidate_window_result,
    is_warmup_rejected_candidate_window_result,
    validate_authority,
    validate_v3_candidate_window_result,
    validate_warmup_rejected_candidate_window_result,
)

from .temporal_discovery_base import *
from .temporal_realized_behavior import (
    aggregate_realized_behavior,
    behavior_family_clusters,
    build_window_realized_behavior,
)

def _result_files(result_root: Path | str) -> list[Path]:
    root = Path(result_root)
    result_dir = root / "results"
    files = sorted(
        [*result_dir.glob("*.json"), *result_dir.glob("*.json.gz")],
        key=lambda path: path.name,
    )
    if not files:
        raise TemporalDiscoveryContractError(
            f"no materialized candidate/window results found under {root}"
        )
    sibling_stems: set[str] = set()
    for path in files:
        stem = path.name.removesuffix(".gz").removesuffix(".json")
        if stem in sibling_stems:
            raise TemporalDiscoveryContractError(
                f"ambiguous duplicate result representations: {stem}"
            )
        sibling_stems.add(stem)
    return files


def _metric(payload: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return default
        current = current.get(key)
    return default if current is None else current


def _finite_metric(
    payload: Mapping[str, Any],
    key: str,
    *,
    name: str,
    default: float = 0.0,
) -> float:
    value = payload.get(key, default)
    if value is None:
        value = default
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be numeric") from exc
    if not math.isfinite(number):
        raise TemporalDiscoveryContractError(f"{name} must be finite")
    return number


def _terminal_window_economics(
    metrics: Mapping[str, Any],
    *,
    name: str,
) -> dict[str, Any]:
    terminal = _mapping(metrics.get("terminalValuation"), name=f"{name} terminal valuation")
    return {
        "terminalValuation": terminal,
        "terminalPolicy": terminal.get("policy"),
        "terminalPolicySchemaVersion": terminal.get("schemaVersion"),
        "terminalLastCompletedBarId": terminal.get("lastCompletedBarId"),
        "terminalLastCompletedBarStart": terminal.get("lastCompletedBarStart"),
        "terminalLastCompletedBarClose": terminal.get("lastCompletedBarClose"),
        "terminalPositionStatus": terminal.get("positionStatus"),
        "terminalPendingEffectStatus": terminal.get("pendingEffectStatus"),
        "terminalPendingEffectCancellationTreatment": terminal.get(
            "pendingEffectCancellationTreatment"
        ),
        "terminalMarkPrice": _finite_metric(
            terminal, "markPrice", name=f"{name} terminal mark price"
        ),
        "terminalGrossR": (
            _finite_metric(terminal, "grossR", name=f"{name} terminal gross R")
            if terminal.get("grossR") is not None
            else 0.0
        ),
        "terminalNetR": (
            _finite_metric(terminal, "netR", name=f"{name} terminal net R")
            if terminal.get("netR") is not None
            else 0.0
        ),
        "terminalExitCostPercent": _finite_metric(
            terminal, "exitCostPercent", name=f"{name} terminal exit cost"
        ),
        "terminalAdjustedGrossR": _finite_metric(
            metrics,
            "terminalAdjustedTotalGrossR",
            name=f"{name} terminal adjusted gross R",
        ),
        "terminalAdjustedNetR": _finite_metric(
            metrics,
            "terminalAdjustedTotalNetR",
            name=f"{name} terminal adjusted net R",
        ),
        "terminalAdjustedExecutionCostPercent": _finite_metric(
            metrics,
            "terminalAdjustedTotalExecutionCostPercent",
            name=f"{name} terminal adjusted execution cost",
        ),
        "terminalAdjustedMaxDrawdownR": _finite_metric(
            metrics,
            "terminalAdjustedMaxDrawdownR",
            name=f"{name} terminal adjusted drawdown",
        ),
        "terminalAdjustedEquityCurveR": [
            _finite_metric(
                {"value": value},
                "value",
                name=f"{name} terminal adjusted equity curve",
            )
            for value in metrics.get("terminalAdjustedEquityCurveR") or []
        ],
    }


def _window_record(result: Mapping[str, Any]) -> dict[str, Any]:
    if is_warmup_rejected_candidate_window_result(result):
        try:
            validate_warmup_rejected_candidate_window_result(result)
        except TemporalSearchContractError as exc:
            raise TemporalDiscoveryContractError(
                "invalid deterministic candidate/window warmup rejection"
            ) from exc
        outcome = _mapping(result.get("evaluation_outcome"), name="warmup rejection outcome")
        reason_code = str(outcome["reason_code"])
        return {
            "economicsBasis": f"not_evaluated_{reason_code}",
            "v3Admissible": False,
            "evaluationRejected": True,
            "rejection": _clone(outcome, name="warmup rejection provenance"),
            "candidateId": str(result.get("candidate_id") or ""),
            "windowId": str(result.get("analysis_window_start")) + "/" + str(result.get("analysis_window_end")),
            "analysisWindowStart": result.get("analysis_window_start"),
            "analysisWindowEnd": result.get("analysis_window_end"),
        }
    v3_admissible = is_v3_candidate_window_result(result)
    if v3_admissible:
        try:
            validate_v3_candidate_window_result(result)
        except TemporalSearchContractError as exc:
            raise TemporalDiscoveryContractError(
                "invalid Stage 5E7-v3 candidate/window result"
            ) from exc
    cost_views = _mapping(
        result.get("cost_view_results"),
        name="cost_view_results",
    )
    if set(cost_views) != {"research_conservative", "none"}:
        raise TemporalDiscoveryContractError(
            "candidate result must contain exactly both cost views"
        )
    conservative = _mapping(
        cost_views["research_conservative"],
        name="conservative cost result",
    )
    no_cost = _mapping(cost_views["none"], name="no-cost result")
    conservative_replay = _mapping(
        conservative.get("replay_result"),
        name="conservative replay",
    )
    no_cost_replay = _mapping(
        no_cost.get("replay_result"),
        name="no-cost replay",
    )
    conservative_metrics = _mapping(
        conservative_replay.get("metrics"),
        name="conservative metrics",
    )
    no_cost_metrics = _mapping(
        no_cost_replay.get("metrics"),
        name="no-cost metrics",
    )
    if (
        conservative_replay.get("streamSha256")
        != no_cost_replay.get("streamSha256")
        or conservative_replay.get("streamSha256")
        != result.get("observation_stream_sha256")
    ):
        raise TemporalDiscoveryContractError(
            "cost views do not share the exact observation stream"
        )
    raw_closed_conservative_net_r = _finite_metric(
        conservative_metrics,
        "totalNetR",
        name="raw closed conservative net R",
    )
    raw_closed_no_cost_net_r = _finite_metric(
        no_cost_metrics,
        "totalNetR",
        name="raw closed no-cost net R",
    )
    raw_closed_gross_r = _finite_metric(
        conservative_metrics,
        "totalGrossR",
        name="raw closed conservative gross R",
    )
    raw_closed_max_drawdown_r = _finite_metric(
        conservative_metrics,
        "maxDrawdownR",
        name="raw closed conservative drawdown R",
    )
    conservative_terminal = (
        _terminal_window_economics(conservative_metrics, name="conservative")
        if v3_admissible
        else None
    )
    no_cost_terminal = (
        _terminal_window_economics(no_cost_metrics, name="no-cost")
        if v3_admissible
        else None
    )
    economic_conservative_net_r = (
        float(conservative_terminal["terminalAdjustedNetR"])
        if conservative_terminal is not None
        else raw_closed_conservative_net_r
    )
    economic_no_cost_net_r = (
        float(no_cost_terminal["terminalAdjustedNetR"])
        if no_cost_terminal is not None
        else raw_closed_no_cost_net_r
    )
    economic_gross_r = (
        float(conservative_terminal["terminalAdjustedGrossR"])
        if conservative_terminal is not None
        else raw_closed_gross_r
    )
    economic_drawdown_r = (
        float(conservative_terminal["terminalAdjustedMaxDrawdownR"])
        if conservative_terminal is not None
        else raw_closed_max_drawdown_r
    )
    economic_equity_curve = (
        list(conservative_terminal["terminalAdjustedEquityCurveR"])
        if conservative_terminal is not None
        else [
            _finite_metric(
                {"value": value},
                "value",
                name="raw closed equity curve",
            )
            for value in conservative_metrics.get("equityCurveR") or []
        ]
    )
    evidence_contract = (
        _clone(result.get("evidence_contract"), name="v3 evidence contract")
        if v3_admissible
        else None
    )
    trade_rows = conservative_replay.get("trades") or []
    if not isinstance(trade_rows, list):
        raise TemporalDiscoveryContractError(
            "conservative replay trades must be an array"
        )
    entry_hours: dict[str, int] = {}
    mfe_values: list[float] = []
    mae_values: list[float] = []
    holding_bars: list[int] = []
    for trade in trade_rows:
        if not isinstance(trade, Mapping):
            continue
        entry_time = trade.get("entryTime")
        if isinstance(entry_time, str):
            try:
                hour = datetime.fromisoformat(
                    entry_time.replace("Z", "+00:00")
                ).hour
                key = f"{hour:02d}"
                entry_hours[key] = entry_hours.get(key, 0) + 1
            except ValueError:
                pass
        mfe = trade.get("maxFavorableExcursionR")
        mae = trade.get("maxAdverseExcursionR")
        if isinstance(mfe, (int, float)) and not isinstance(mfe, bool):
            mfe_values.append(float(mfe))
        if isinstance(mae, (int, float)) and not isinstance(mae, bool):
            mae_values.append(float(mae))
        holding = trade.get("holdingBars")
        if isinstance(holding, int) and not isinstance(holding, bool) and holding >= 0:
            holding_bars.append(holding)

    realized_behavior = build_window_realized_behavior(
        replay=conservative_replay,
        metrics=conservative_metrics,
        window_id=(
            str(result.get("analysis_window_start"))
            + "/"
            + str(result.get("analysis_window_end"))
        ),
        allow_legacy_sparse=not v3_admissible,
    )

    return {
        "economicsBasis": (
            "stage5e7_v3_terminal_adjusted"
            if v3_admissible
            else "legacy_closed_trade_v1_not_v3_admissible"
        ),
        "v3Admissible": v3_admissible,
        "candidateId": str(result.get("candidate_id") or ""),
        "windowId": (
            str(result.get("analysis_window_start"))
            + "/"
            + str(result.get("analysis_window_end"))
        ),
        "analysisWindowStart": result.get("analysis_window_start"),
        "analysisWindowEnd": result.get("analysis_window_end"),
        # These are intentionally distinct identities.  The source snapshot is
        # the normalized authored profile selected by the candidate validator;
        # resolved profile/program are what the evaluator actually executed.
        "sourceProfileSnapshotSha256": result.get(
            "source_profile_snapshot_sha256"
        ),
        "resolvedProfileSnapshotSha256": result.get(
            "resolved_profile_snapshot_sha256"
        ),
        "resolvedProgramSha256": result.get("program_sha256"),
        # Retain the historic window name for generic consumers.  It is an
        # execution identity, never an assertion about the authored program.
        "programSha256": result.get("program_sha256"),
        "observationStreamSha256": result.get(
            "observation_stream_sha256"
        ),
        "observations": int(
            conservative_metrics.get("observationsProcessed") or 0
        ),
        "trades": int(conservative_metrics.get("tradesClosed") or 0),
        "wins": int(conservative_metrics.get("wins") or 0),
        "losses": int(conservative_metrics.get("losses") or 0),
        "flatTrades": int(conservative_metrics.get("flatTrades") or 0),
        # The compatibility names are economic values.  For v3 they are
        # terminal-adjusted and therefore include every unresolved position;
        # explicit raw names remain for closed-trade diagnostics.
        "conservativeNetR": economic_conservative_net_r,
        "noCostNetR": economic_no_cost_net_r,
        "grossR": economic_gross_r,
        "maxDrawdownR": economic_drawdown_r,
        "rawClosedConservativeNetR": raw_closed_conservative_net_r,
        "rawClosedNoCostNetR": raw_closed_no_cost_net_r,
        "rawClosedGrossR": raw_closed_gross_r,
        "rawClosedMaxDrawdownR": raw_closed_max_drawdown_r,
        "rawClosedCostViewDeltaR": (
            raw_closed_no_cost_net_r - raw_closed_conservative_net_r
        ),
        "rawClosedEquityCurveR": [
            _finite_metric(
                {"value": value},
                "value",
                name="raw closed equity curve",
            )
            for value in conservative_metrics.get("equityCurveR") or []
        ],
        "terminalAdjustedConservativeNetR": (
            economic_conservative_net_r if v3_admissible else None
        ),
        "terminalAdjustedNoCostNetR": (
            economic_no_cost_net_r if v3_admissible else None
        ),
        "terminalAdjustedGrossR": economic_gross_r if v3_admissible else None,
        "terminalAdjustedMaxDrawdownR": (
            economic_drawdown_r if v3_admissible else None
        ),
        "terminalAdjustedCostViewDeltaR": (
            economic_no_cost_net_r - economic_conservative_net_r
            if v3_admissible
            else None
        ),
        "terminalAdjustedEquityCurveR": (
            economic_equity_curve if v3_admissible else None
        ),
        "conservativeTerminal": conservative_terminal,
        "noCostTerminal": no_cost_terminal,
        "evidenceContract": evidence_contract,
        "evidenceContractEndpoints": (
            {
                "analysisWindowStart": evidence_contract["analysis_window_start"],
                "analysisWindowEnd": evidence_contract["analysis_window_end"],
                "firstAdmittedObservationTimestamp": evidence_contract[
                    "first_admitted_observation_timestamp"
                ],
                "lastAdmittedObservationTimestamp": evidence_contract[
                    "last_admitted_observation_timestamp"
                ],
                "observationCount": evidence_contract["observation_count"],
                "requestedBarLimit": evidence_contract["requested_bar_limit"],
                "effectiveBarLimit": evidence_contract["effective_bar_limit"],
            }
            if evidence_contract is not None
            else None
        ),
        "averageHoldingBars": conservative_metrics.get(
            "averageHoldingBars"
        ),
        "holdingBars": holding_bars,
        "medianHoldingBars": (
            float(statistics.median(holding_bars)) if holding_bars else None
        ),
        "exposureRatio": float(
            conservative_metrics.get("exposureRatio") or 0.0
        ),
        "transitionEntropy": float(
            conservative_metrics.get("transitionEntropy") or 0.0
        ),
        "winRate": conservative_metrics.get("winRate"),
        "profitFactor": conservative_metrics.get("profitFactor"),
        "actionCounts": _clone(
            conservative_metrics.get("actionCounts") or {},
            name="action counts",
        ),
        "closeReasonCounts": _clone(
            conservative_metrics.get("closeReasonCounts") or {},
            name="close reason counts",
        ),
        "stateOccupancy": _clone(
            conservative_metrics.get("stateOccupancy") or {},
            name="state occupancy",
        ),
        "transitionCounts": _clone(
            conservative_metrics.get("transitionCounts") or {},
            name="transition counts",
        ),
        "entryHourCounts": entry_hours,
        "averageMfeR": (
            sum(mfe_values) / len(mfe_values) if mfe_values else 0.0
        ),
        "averageMaeR": (
            sum(mae_values) / len(mae_values) if mae_values else 0.0
        ),
        "equityCurveR": economic_equity_curve,
        # The realized projection is derived only from immutable conservative
        # replay evidence.  It intentionally carries no authored/program
        # identity, so execution-equivalent genotypes can be reported together.
        "realizedBehavior": realized_behavior,
    }


def load_stage_results(
    result_root: Path | str,
    *,
    tail_result_index: Mapping[str, Any] | None = None,
    direction_aware: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Load canonical candidate/window records from raw blobs or a verified index.

    ``tail_result_index`` is deliberately an operational, in-memory reuse
    boundary.  Callers may supply it only after the matching immutable result
    matrix has been source-verified by ``temporal_qd_tail_result_index``.  It
    is not persisted as part of this loader's semantic output, and the raw
    path below remains the default/oracle.
    """

    if tail_result_index is not None:
        if direction_aware:
            # Indexed v1 intentionally predates exact per-direction behavior.
            # A v5 archive must not infer side safety from scalar metrics.
            raise TemporalDiscoveryContractError(
                "direction-aware rotating evidence requires raw result provenance"
            )
        # Import lazily: the index module uses ``_window_record`` from this
        # module to build its exact legacy-compatible projection.
        from .temporal_qd_tail_result_index import load_indexed_stage_results

        return load_indexed_stage_results(tail_result_index)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for path in _result_files(result_root):
        result = _read_json(path, name="candidate/window result")
        if result.get("schema_version") not in {
            "temporal_graph_candidate_window_result_v1",
            TEMPORAL_SEARCH_REJECTED_RESULT_SCHEMA,
        }:
            raise TemporalDiscoveryContractError(
                f"unexpected result schema: {path}"
            )
        record = _window_record(result)
        candidate_id = record["candidateId"]
        if not _CANDIDATE.fullmatch(candidate_id):
            raise TemporalDiscoveryContractError(
                "candidate result has an invalid candidate ID"
            )
        grouped.setdefault(candidate_id, []).append(record)
    for candidate_id in grouped:
        grouped[candidate_id].sort(
            key=lambda item: (
                str(item["analysisWindowStart"]),
                str(item["analysisWindowEnd"]),
            )
        )
    return grouped


def load_provenance_bound_window_evidence(
    *,
    result_root: Path | str,
    task_manifest: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    panel: Mapping[str, Any],
    candidates: Mapping[str, Mapping[str, Any]],
    tail_result_index: Mapping[str, Any] | None = None,
    direction_aware: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Derive authority-independent window digests from verified result data.

    The optional index is a same-transaction projection of this exact task
    matrix, already source-verified by its owner.  The default continues to
    reopen the immutable raw blobs for compatibility and audit use.
    """

    if tail_result_index is not None:
        if direction_aware:
            # The indexed v1 projection intentionally predates exact
            # per-direction realized behavior.  Do not infer v5 side safety
            # from scalar metrics; use the raw authority-bound result path.
            raise TemporalDiscoveryContractError(
                "direction-aware rotating evidence requires raw result provenance"
            )
        # Keep this import lazy for the same cycle-avoidance reason as the
        # stage loader above.
        from .temporal_qd_tail_result_index import (
            load_indexed_provenance_bound_window_evidence,
            validate_tail_result_index,
        )

        indexed = validate_tail_result_index(tail_result_index)
        if (
            indexed["authorityId"] != task_manifest.get("authorityId")
            or indexed["taskManifestSha256"] != canonical_sha256(task_manifest)
            or indexed["checkpointSha256"] != canonical_sha256(checkpoint)
        ):
            raise TemporalDiscoveryContractError(
                "indexed rotating evidence does not match its task matrix"
            )
        return load_indexed_provenance_bound_window_evidence(
            index=indexed,
            panel=panel,
            candidates=candidates,
        )

    from .temporal_qd_rotating_evidence import build_candidate_window_evidence

    tasks = task_manifest.get("tasks")
    completed = checkpoint.get("completed")
    if not isinstance(tasks, list) or not isinstance(completed, Mapping):
        raise TemporalDiscoveryContractError("rotating evidence task ledger is invalid")
    task_map = {
        str(row.get("task_id")): row
        for row in tasks
        if isinstance(row, Mapping) and isinstance(row.get("task_id"), str)
    }
    if len(task_map) != len(tasks) or set(task_map) != set(completed):
        raise TemporalDiscoveryContractError(
            "rotating evidence requires an exactly completed task matrix"
        )
    authority_id = task_manifest.get("authorityId")
    matrix_sha = task_manifest.get("taskMatrixSha256")
    if not isinstance(authority_id, str) or not isinstance(matrix_sha, str):
        raise TemporalDiscoveryContractError("rotating evidence task authority is invalid")
    windows = {
        (str(row.get("analysisWindowStart")), str(row.get("analysisWindowEnd"))): row
        for row in panel.get("windows") or []
        if isinstance(row, Mapping)
    }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for task_id in sorted(task_map):
        task = task_map[task_id]
        payload = task.get("payload")
        record = completed[task_id]
        if not isinstance(payload, Mapping) or not isinstance(record, Mapping):
            raise TemporalDiscoveryContractError("rotating evidence task record is invalid")
        candidate_id = str(payload.get("candidate_id") or "")
        candidate = candidates.get(candidate_id)
        if record.get("candidateId") != candidate_id:
            raise TemporalDiscoveryContractError("rotating evidence candidate provenance mismatch")
        if candidate is None:
            # The requested evidence population may be a strict, explicit
            # subset of the completed matrix after archive eligibility
            # filtering.  Preserve task/checkpoint provenance validation while
            # omitting candidates which cannot receive breeding rights.
            continue
        raw_path = record.get("resultPath")
        if not isinstance(raw_path, str):
            raise TemporalDiscoveryContractError("rotating evidence result path is missing")
        path = Path(raw_path)
        if path.resolve().parent != (Path(result_root) / "results").resolve():
            raise TemporalDiscoveryContractError("rotating evidence result escaped its authority root")
        raw = _read_json(path, name="rotating candidate/window result")
        result_sha = canonical_sha256(raw)
        if result_sha != record.get("resultSha256"):
            raise TemporalDiscoveryContractError("rotating evidence raw result identity mismatch")
        try:
            validate_v3_candidate_window_result(raw, task_payload=payload)
        except TemporalSearchContractError as exc:
            raise TemporalDiscoveryContractError(
                "rotating evidence raw result does not match its task"
            ) from exc
        if raw.get("candidate_id") != candidate_id:
            raise TemporalDiscoveryContractError(
                "rotating evidence raw result candidate does not match its task"
            )
        window_record = _window_record(raw)
        key = (
            str(window_record.get("analysisWindowStart")),
            str(window_record.get("analysisWindowEnd")),
        )
        window = windows.get(key)
        if window is None:
            raise TemporalDiscoveryContractError("rotating evidence result is outside its panel")
        candidate_snapshot = candidate.get("profileSnapshotSha256")
        if (
            not isinstance(candidate_snapshot, str)
            or window_record.get("sourceProfileSnapshotSha256")
            != candidate_snapshot
        ):
            raise TemporalDiscoveryContractError(
                "rotating evidence source profile does not match its candidate"
            )
        evidence_plan = payload.get("evidence_plan")
        plan_id = evidence_plan.get("plan_id") if isinstance(evidence_plan, Mapping) else None
        if not isinstance(plan_id, str) or not plan_id.startswith("sha256:"):
            raise TemporalDiscoveryContractError("rotating evidence plan identity is invalid")
        metrics = {
            "conservativeNetR": window_record["conservativeNetR"],
            "noCostNetR": window_record["noCostNetR"],
            "maxDrawdownR": window_record["maxDrawdownR"],
            "closedTrades": window_record["trades"],
            "observations": window_record["observations"],
            "v3Admissible": window_record["v3Admissible"],
            "resolvedProgramSha256": window_record["resolvedProgramSha256"],
            "resolvedProfileSnapshotSha256": window_record[
                "resolvedProfileSnapshotSha256"
            ],
            "sourceProfileSnapshotSha256": window_record[
                "sourceProfileSnapshotSha256"
            ],
            **(
                {"realizedBehavior": window_record["realizedBehavior"]}
                if direction_aware
                else {}
            ),
        }
        grouped.setdefault(candidate_id, []).append(
            build_candidate_window_evidence(
                candidate=candidate,
                panel=panel,
                window=window,
                metrics=metrics,
                evidence_plan_semantic_sha256=plan_id,
                provenance={
                    "authorityId": authority_id,
                    "taskMatrixSha256": matrix_sha,
                    "taskId": task_id,
                    "resultSha256": result_sha,
                },
            )
        )
    for candidate_id, rows in grouped.items():
        rows.sort(key=lambda row: str(row["windowId"]))
        if len(rows) != len(windows):
            raise TemporalDiscoveryContractError(
                f"rotating evidence candidate {candidate_id} lacks complete panel coverage"
            )
    if set(grouped) != set(candidates):
        raise TemporalDiscoveryContractError("rotating evidence population coverage mismatch")
    return grouped


def _result_set_sha256(
    grouped: Mapping[str, Sequence[Mapping[str, Any]]],
) -> str:
    material = [
        {
            "candidateId": candidate_id,
            "windows": list(grouped[candidate_id]),
        }
        for candidate_id in sorted(grouped)
    ]
    return canonical_sha256(material)


def _distribution(counts: Mapping[str, Any]) -> dict[str, float]:
    values = {
        str(key): max(0.0, float(value))
        for key, value in sorted(
            counts.items(),
            key=lambda item: str(item[0]),
        )
    }
    total = math.fsum(values[key] for key in sorted(values))
    if total <= 0.0:
        return {}
    return {key: values[key] / total for key in sorted(values)}


def _l1_distribution_distance(
    left: Mapping[str, float],
    right: Mapping[str, float],
) -> float:
    keys = sorted(set(left) | set(right))
    return 0.5 * math.fsum(
        abs(float(left.get(key, 0.0)) - float(right.get(key, 0.0)))
        for key in keys
    )


def _log_distance(left: float, right: float) -> float:
    numerator = abs(math.log1p(max(0.0, left)) - math.log1p(max(0.0, right)))
    denominator = 1.0 + max(
        abs(math.log1p(max(0.0, left))),
        abs(math.log1p(max(0.0, right))),
    )
    return min(1.0, numerator / denominator)


def _equity_shape(curve: Sequence[float], points: int = 12) -> list[float]:
    if not curve:
        return [0.0] * points
    values = [float(value) for value in curve]
    scale = max(1.0, max(abs(value) for value in values))
    if len(values) == 1:
        return [values[0] / scale] * points
    output: list[float] = []
    for index in range(points):
        position = index * (len(values) - 1) / max(1, points - 1)
        left = int(math.floor(position))
        right = min(len(values) - 1, left + 1)
        fraction = position - left
        interpolated = (
            values[left] * (1.0 - fraction)
            + values[right] * fraction
        )
        output.append(interpolated / scale)
    return output


def _require_candidate_execution_binding(
    candidate: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Bind v3 windows to authored input and one resolved execution.

    A candidate's ``sourceProfileSha256`` identifies exact raw authored JSON,
    while ``profileSnapshotSha256`` is its normalized authored replay input.
    A worker must echo the latter.  The profile/program it resolves for
    execution are intentionally separate from authored lineage, but must stay
    exact and stable for every window of the candidate.
    """
    authored_program = _sha(
        candidate.get("programSha256"),
        name="candidate authored program SHA-256",
    )
    source_profile_snapshot = _sha(
        candidate.get("profileSnapshotSha256"),
        name="candidate profile snapshot SHA-256",
    )
    if not windows:
        raise TemporalDiscoveryContractError(
            "candidate execution binding requires at least one result window"
        )
    resolved_profile_snapshot: str | None = None
    resolved_program: str | None = None
    for index, window in enumerate(windows):
        source_actual = _sha(
            window.get("sourceProfileSnapshotSha256"),
            name=f"result window {index} source profile snapshot SHA-256",
        )
        if source_actual != source_profile_snapshot:
            raise TemporalDiscoveryContractError(
                "result window source profile snapshot identity does not match "
                "candidate profile snapshot identity"
            )
        profile_actual = _sha(
            window.get("resolvedProfileSnapshotSha256"),
            name=f"result window {index} resolved profile snapshot SHA-256",
        )
        program_actual = _sha(
            window.get("resolvedProgramSha256"),
            name=f"result window {index} program SHA-256",
        )
        if (
            resolved_profile_snapshot is not None
            and profile_actual != resolved_profile_snapshot
        ):
            raise TemporalDiscoveryContractError(
                "result window resolved profile snapshot identity changed across windows"
            )
        if resolved_program is not None and program_actual != resolved_program:
            raise TemporalDiscoveryContractError(
                "result window resolved program identity changed across windows"
            )
        resolved_profile_snapshot = profile_actual
        resolved_program = program_actual
    assert resolved_profile_snapshot is not None
    assert resolved_program is not None
    return {
        "authoredProgramSha256": authored_program,
        "sourceProfileSnapshotSha256": source_profile_snapshot,
        "resolvedProfileSnapshotSha256": resolved_profile_snapshot,
        "resolvedProgramSha256": resolved_program,
    }


def _require_candidate_program_identity(
    candidate: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> str:
    """Compatibility wrapper returning the resolved execution program.

    New callers should use :func:`_require_candidate_execution_binding` so
    they cannot accidentally compare authored lineage to resolved execution.
    """
    return _require_candidate_execution_binding(candidate, windows)[
        "resolvedProgramSha256"
    ]


def _aggregate_candidate(
    candidate: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not windows:
        raise TemporalDiscoveryContractError(
            "candidate aggregate requires at least one window"
        )
    v3_flags = {bool(window.get("v3Admissible")) for window in windows}
    if len(v3_flags) != 1:
        raise TemporalDiscoveryContractError(
            "candidate aggregate must not mix legacy closed-trade and v3 terminal-adjusted windows"
        )
    v3_admissible = v3_flags == {True}
    execution_binding = (
        _require_candidate_execution_binding(candidate, windows)
        if v3_admissible
        else None
    )
    economics_basis = (
        "stage5e7_v3_terminal_adjusted"
        if v3_admissible
        else "legacy_closed_trade_v1_not_v3_admissible"
    )
    action_counts: dict[str, int] = {}
    close_counts: dict[str, int] = {}
    state_counts: dict[str, int] = {}
    transition_counts: dict[str, int] = {}
    entry_hour_counts: dict[str, int] = {}
    equity_shapes: list[list[float]] = []
    for window in windows:
        for target, source_key in (
            (action_counts, "actionCounts"),
            (close_counts, "closeReasonCounts"),
            (state_counts, "stateOccupancy"),
            (transition_counts, "transitionCounts"),
            (entry_hour_counts, "entryHourCounts"),
        ):
            for key, value in window[source_key].items():
                target[str(key)] = target.get(str(key), 0) + int(value)
        equity_shapes.append(_equity_shape(window.get("equityCurveR") or []))
    trades = sum(int(window["trades"]) for window in windows)
    observations = sum(int(window["observations"]) for window in windows)
    holds = [
        float(window["averageHoldingBars"])
        for window in windows
        if window["averageHoldingBars"] is not None
    ]
    holding_bars = [
        int(value)
        for window in windows
        for value in window.get("holdingBars") or []
    ]
    win_rates = [
        float(window["winRate"])
        for window in windows
        if window["winRate"] is not None
    ]
    program_ids = {
        str(window.get("resolvedProgramSha256", window.get("programSha256")))
        for window in windows
    }
    if len(program_ids) != 1:
        raise TemporalDiscoveryContractError(
            "candidate program identity changed across windows"
        )
    complexity = {
        "stateCount": len(
            (candidate["sourceProfile"].get("graph") or {}).get("states") or []
        ),
        "transitionCount": len(
            (candidate["sourceProfile"].get("graph") or {}).get("transitions") or []
        ),
        "indicatorCount": len(candidate["sourceProfile"].get("indicators") or []),
        "managementPlanCount": len(
            (
                (
                    candidate["sourceProfile"].get("executionConfig")
                    or {}
                ).get("managementLibrary")
                or {}
            ).get("plans")
            or []
        ),
    }
    terminal_evidence = [
        {
            "windowId": window["windowId"],
            "terminalPolicy": _metric(
                window,
                "conservativeTerminal",
                "terminalPolicy",
            ),
            "terminalPolicySchemaVersion": _metric(
                window,
                "conservativeTerminal",
                "terminalPolicySchemaVersion",
            ),
            "lastCompletedBarId": _metric(
                window,
                "conservativeTerminal",
                "terminalLastCompletedBarId",
            ),
            "lastCompletedBarStart": _metric(
                window,
                "conservativeTerminal",
                "terminalLastCompletedBarStart",
            ),
            "lastCompletedBarClose": _metric(
                window,
                "conservativeTerminal",
                "terminalLastCompletedBarClose",
            ),
            "positionStatus": _metric(
                window,
                "conservativeTerminal",
                "terminalPositionStatus",
            ),
            "pendingEffectStatus": _metric(
                window,
                "conservativeTerminal",
                "terminalPendingEffectStatus",
            ),
            "markPrice": _metric(
                window,
                "conservativeTerminal",
                "terminalMarkPrice",
            ),
            "terminalGrossR": _metric(
                window,
                "conservativeTerminal",
                "terminalGrossR",
            ),
            "terminalNetR": _metric(
                window,
                "conservativeTerminal",
                "terminalNetR",
            ),
            "terminalExitCostPercent": _metric(
                window,
                "conservativeTerminal",
                "terminalExitCostPercent",
            ),
            "terminalAdjustedMaxDrawdownR": _metric(
                window,
                "conservativeTerminal",
                "terminalAdjustedMaxDrawdownR",
            ),
            "noCostTerminalNetR": _metric(
                window,
                "noCostTerminal",
                "terminalNetR",
            ),
            "costViewTerminalDeltaR": window.get(
                "terminalAdjustedCostViewDeltaR"
            ),
            "evidenceEndpoints": window.get("evidenceContractEndpoints"),
        }
        for window in windows
    ]
    record = {
        "candidateId": candidate["candidateId"],
        "sourceMode": candidate["sourceMode"],
        "seedId": candidate["seedId"],
        "sourceProfileSha256": candidate["sourceProfileSha256"],
        "authoredProgramSha256": (
            execution_binding["authoredProgramSha256"]
            if execution_binding is not None
            else candidate.get("programSha256")
        ),
        "sourceProfileSnapshotSha256": (
            execution_binding["sourceProfileSnapshotSha256"]
            if execution_binding is not None
            else candidate.get("profileSnapshotSha256")
        ),
        "resolvedProfileSnapshotSha256": (
            execution_binding["resolvedProfileSnapshotSha256"]
            if execution_binding is not None
            else None
        ),
        "resolvedProgramSha256": next(iter(program_ids)),
        # Compatibility alias: aggregate program identity describes the
        # program which produced the measured result.
        "programSha256": next(iter(program_ids)),
        "windowCount": len(windows),
        "economicsBasis": economics_basis,
        "v3Admissible": v3_admissible,
        "tradeCountsByWindow": [int(window["trades"]) for window in windows],
        "totalTrades": trades,
        "totalObservations": observations,
        "totalConservativeNetR": sum(
            float(window["conservativeNetR"]) for window in windows
        ),
        "totalNoCostNetR": sum(
            float(window["noCostNetR"]) for window in windows
        ),
        "worstWindowConservativeNetR": min(
            float(window["conservativeNetR"]) for window in windows
        ),
        "profitableWindowCount": sum(
            float(window["conservativeNetR"]) > 0.0 for window in windows
        ),
        "maxWindowDrawdownR": max(
            float(window["maxDrawdownR"]) for window in windows
        ),
        "costDragR": sum(
            float(window["noCostNetR"]) - float(window["conservativeNetR"])
            for window in windows
        ),
        "totalRawClosedConservativeNetR": sum(
            float(window["rawClosedConservativeNetR"]) for window in windows
        ),
        "totalRawClosedNoCostNetR": sum(
            float(window["rawClosedNoCostNetR"]) for window in windows
        ),
        "worstWindowRawClosedConservativeNetR": min(
            float(window["rawClosedConservativeNetR"]) for window in windows
        ),
        "totalTerminalAdjustedConservativeNetR": (
            sum(float(window["terminalAdjustedConservativeNetR"]) for window in windows)
            if v3_admissible
            else None
        ),
        "totalTerminalAdjustedNoCostNetR": (
            sum(float(window["terminalAdjustedNoCostNetR"]) for window in windows)
            if v3_admissible
            else None
        ),
        "worstWindowTerminalAdjustedConservativeNetR": (
            min(float(window["terminalAdjustedConservativeNetR"]) for window in windows)
            if v3_admissible
            else None
        ),
        "maxWindowRawClosedDrawdownR": max(
            float(window["rawClosedMaxDrawdownR"]) for window in windows
        ),
        "totalRawClosedCostDragR": sum(
            float(window["rawClosedNoCostNetR"])
            - float(window["rawClosedConservativeNetR"])
            for window in windows
        ),
        "totalTerminalAdjustedCostDragR": (
            sum(
                float(window["terminalAdjustedNoCostNetR"])
                - float(window["terminalAdjustedConservativeNetR"])
                for window in windows
            )
            if v3_admissible
            else None
        ),
        "entryFrequencyPerThousand": (
            (trades / observations) * 1000.0 if observations else 0.0
        ),
        "averageExposureRatio": sum(
            float(window["exposureRatio"]) for window in windows
        )
        / len(windows),
        "averageHoldingBars": (
            sum(holds) / len(holds) if holds else 0.0
        ),
        "medianHoldingBars": (
            float(statistics.median(holding_bars)) if holding_bars else 0.0
        ),
        "averageWinRate": (
            sum(win_rates) / len(win_rates) if win_rates else 0.0
        ),
        "averageTransitionEntropy": sum(
            float(window["transitionEntropy"]) for window in windows
        )
        / len(windows),
        "averageMfeR": sum(
            float(window["averageMfeR"]) for window in windows
        )
        / len(windows),
        "averageMaeR": sum(
            float(window["averageMaeR"]) for window in windows
        )
        / len(windows),
        "equityShape": [
            sum(shape[index] for shape in equity_shapes) / len(equity_shapes)
            for index in range(len(equity_shapes[0]))
        ],
        "entryHourDistribution": _distribution(entry_hour_counts),
        "actionDistribution": _distribution(action_counts),
        "closeReasonDistribution": _distribution(close_counts),
        "stateOccupancyDistribution": _distribution(state_counts),
        "transitionDistribution": _distribution(transition_counts),
        "complexity": complexity,
        "terminalEvidence": terminal_evidence,
        "windowRecords": list(windows),
    }
    record["realizedBehavior"] = aggregate_realized_behavior(windows)
    record["behaviorIdentitySha256"] = record["realizedBehavior"]["identitySha256"]
    record["fingerprintSha256"] = canonical_sha256(
        {
            key: record[key]
            for key in (
                "entryFrequencyPerThousand",
                "averageExposureRatio",
                "averageHoldingBars",
                "averageWinRate",
                "averageTransitionEntropy",
                "averageMfeR",
                "averageMaeR",
                "equityShape",
                "entryHourDistribution",
                "actionDistribution",
                "closeReasonDistribution",
                "stateOccupancyDistribution",
                "complexity",
                "economicsBasis",
                "v3Admissible",
                "terminalEvidence",
            )
        }
    )
    return record


def fingerprint_distance(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> float:
    dimensions = [
        _log_distance(
            float(left["entryFrequencyPerThousand"]),
            float(right["entryFrequencyPerThousand"]),
        ),
        abs(
            float(left["averageExposureRatio"])
            - float(right["averageExposureRatio"])
        ),
        _log_distance(
            float(left["averageHoldingBars"]),
            float(right["averageHoldingBars"]),
        ),
        abs(float(left["averageWinRate"]) - float(right["averageWinRate"])),
        _log_distance(
            float(left["averageTransitionEntropy"]),
            float(right["averageTransitionEntropy"]),
        ),
        _log_distance(
            abs(float(left["averageMfeR"])),
            abs(float(right["averageMfeR"])),
        ),
        _log_distance(
            abs(float(left["averageMaeR"])),
            abs(float(right["averageMaeR"])),
        ),
        math.fsum(
            abs(float(a) - float(b))
            for a, b in zip(left["equityShape"], right["equityShape"])
        )
        / max(1, len(left["equityShape"])),
        _l1_distribution_distance(
            left["entryHourDistribution"],
            right["entryHourDistribution"],
        ),
        _l1_distribution_distance(
            left["actionDistribution"],
            right["actionDistribution"],
        ),
        _l1_distribution_distance(
            left["closeReasonDistribution"],
            right["closeReasonDistribution"],
        ),
        _l1_distribution_distance(
            left["stateOccupancyDistribution"],
            right["stateOccupancyDistribution"],
        ),
    ]
    left_complexity = left["complexity"]
    right_complexity = right["complexity"]
    complexity_delta = math.fsum(
        _log_distance(
            float(left_complexity[key]),
            float(right_complexity[key]),
        )
        for key in sorted(left_complexity)
    ) / max(1, len(left_complexity))
    dimensions.append(complexity_delta)
    return math.fsum(dimensions) / len(dimensions)


_ECONOMIC_OBJECTIVES = (
    ("totalConservativeNetR", "max"),
    ("worstWindowConservativeNetR", "max"),
    ("profitableWindowCount", "max"),
    ("maxWindowDrawdownR", "min"),
    ("costDragR", "min"),
)


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    no_worse = True
    strictly_better = False
    for key, direction in _ECONOMIC_OBJECTIVES:
        left_value = float(left[key])
        right_value = float(right[key])
        if direction == "max":
            if left_value < right_value:
                no_worse = False
                break
            if left_value > right_value:
                strictly_better = True
        else:
            if left_value > right_value:
                no_worse = False
                break
            if left_value < right_value:
                strictly_better = True
    return no_worse and strictly_better


def pareto_fronts(
    candidates: Sequence[Mapping[str, Any]],
) -> list[list[dict[str, Any]]]:
    remaining = [dict(item) for item in candidates]
    fronts: list[list[dict[str, Any]]] = []
    while remaining:
        front = [
            candidate
            for candidate in remaining
            if not any(
                _dominates(other, candidate)
                for other in remaining
                if other["candidateId"] != candidate["candidateId"]
            )
        ]
        front.sort(
            key=lambda item: (
                -float(item["totalConservativeNetR"]),
                -float(item["worstWindowConservativeNetR"]),
                float(item["maxWindowDrawdownR"]),
                float(item["costDragR"]),
                item["candidateId"],
            )
        )
        fronts.append(front)
        front_ids = {item["candidateId"] for item in front}
        remaining = [
            item for item in remaining
            if item["candidateId"] not in front_ids
        ]
    return fronts


def select_economic_archive(
    candidates: Sequence[Mapping[str, Any]],
    *,
    archive_size: int,
    minimum_trades_per_window: int,
) -> list[dict[str, Any]]:
    eligible = [
        dict(candidate)
        for candidate in candidates
        if candidate["tradeCountsByWindow"]
        and all(
            int(value) >= minimum_trades_per_window
            for value in candidate["tradeCountsByWindow"]
        )
    ]
    selected: list[dict[str, Any]] = []
    for front_index, front in enumerate(pareto_fronts(eligible)):
        for candidate in front:
            row = dict(candidate)
            row["paretoFront"] = front_index
            row["economicRank"] = len(selected)
            selected.append(row)
            if len(selected) >= archive_size:
                return selected
    return selected


def select_novelty_archive(
    candidates: Sequence[Mapping[str, Any]],
    *,
    archive_size: int,
    minimum_total_trades: int,
) -> list[dict[str, Any]]:
    eligible = sorted(
        [
            dict(candidate)
            for candidate in candidates
            if int(candidate["totalTrades"]) >= minimum_total_trades
        ],
        key=lambda candidate: candidate["candidateId"],
    )
    if not eligible:
        return []
    pair_distance: dict[tuple[str, str], float] = {}
    for left in eligible:
        for right in eligible:
            if left["candidateId"] >= right["candidateId"]:
                continue
            pair_distance[(left["candidateId"], right["candidateId"])] = (
                fingerprint_distance(left, right)
            )

    def distance(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
        key = tuple(sorted((left["candidateId"], right["candidateId"])))
        return pair_distance.get(key, 0.0)

    first = max(
        eligible,
        key=lambda candidate: (
            math.fsum(
                distance(candidate, other)
                for other in eligible
                if other["candidateId"] != candidate["candidateId"]
            )
            / max(1, len(eligible) - 1),
            candidate["candidateId"],
        ),
    )
    selected = [dict(first)]
    remaining = [
        candidate
        for candidate in eligible
        if candidate["candidateId"] != first["candidateId"]
    ]
    while remaining and len(selected) < archive_size:
        next_candidate = max(
            remaining,
            key=lambda candidate: (
                min(distance(candidate, chosen) for chosen in selected),
                math.fsum(
                    distance(candidate, chosen)
                    for chosen in selected
                )
                / len(selected),
                candidate["candidateId"],
            ),
        )
        row = dict(next_candidate)
        row["minimumArchiveDistance"] = min(
            distance(next_candidate, chosen) for chosen in selected
        )
        row["noveltyRank"] = len(selected)
        selected.append(row)
        remaining = [
            candidate
            for candidate in remaining
            if candidate["candidateId"] != next_candidate["candidateId"]
        ]
    selected[0]["minimumArchiveDistance"] = None
    selected[0]["noveltyRank"] = 0
    return selected


def _deduplicate_resolved_programs(
    aggregates: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_program: dict[str, list[dict[str, Any]]] = {}
    for aggregate in aggregates:
        resolved_program = str(
            aggregate.get("resolvedProgramSha256", aggregate["programSha256"])
        )
        by_program.setdefault(resolved_program, []).append(dict(aggregate))
    unique: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    for program_sha256 in sorted(by_program):
        rows = sorted(
            by_program[program_sha256],
            key=lambda item: item["candidateId"],
        )
        representative = rows[0]
        unique.append(representative)
        for duplicate in rows[1:]:
            duplicates.append(
                {
                    "programSha256": program_sha256,
                    "representativeCandidateId": representative["candidateId"],
                    "duplicateCandidateId": duplicate["candidateId"],
                }
            )
    unique.sort(key=lambda item: item["candidateId"])
    duplicates.sort(
        key=lambda item: (
            item["programSha256"],
            item["duplicateCandidateId"],
        )
    )
    return unique, duplicates


def _confirmation_union(
    economic: Sequence[Mapping[str, Any]],
    novelty: Sequence[Mapping[str, Any]],
    *,
    cap: int,
) -> list[str]:
    economic_ids = [item["candidateId"] for item in economic]
    novelty_ids = [item["candidateId"] for item in novelty]
    intersection = sorted(
        set(economic_ids) & set(novelty_ids),
        key=lambda candidate_id: (
            economic_ids.index(candidate_id),
            novelty_ids.index(candidate_id),
            candidate_id,
        ),
    )
    selected = list(intersection[:cap])
    economic_index = 0
    novelty_index = 0
    while len(selected) < cap and (
        economic_index < len(economic_ids)
        or novelty_index < len(novelty_ids)
    ):
        for source, index_name in (
            (economic_ids, "economic"),
            (novelty_ids, "novelty"),
        ):
            index = economic_index if index_name == "economic" else novelty_index
            while index < len(source) and source[index] in selected:
                index += 1
            if index < len(source) and len(selected) < cap:
                selected.append(source[index])
                index += 1
            if index_name == "economic":
                economic_index = index
            else:
                novelty_index = index
    return selected




__all__ = ['_result_files', '_metric', '_window_record', 'load_stage_results', '_result_set_sha256', '_distribution', '_l1_distribution_distance', '_log_distance', '_equity_shape', '_require_candidate_execution_binding', '_require_candidate_program_identity', '_aggregate_candidate', 'fingerprint_distance', '_ECONOMIC_OBJECTIVES', '_dominates', 'pareto_fronts', 'select_economic_archive', 'select_novelty_archive', '_deduplicate_resolved_programs', '_confirmation_union']
