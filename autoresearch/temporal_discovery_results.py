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
import subprocess
import tempfile
from typing import Any, Protocol

from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    validate_authority,
)

from .temporal_discovery_base import *

def _result_files(result_root: Path | str) -> list[Path]:
    root = Path(result_root)
    files = sorted((root / "results").glob("*.json"))
    if not files:
        raise TemporalDiscoveryContractError(
            f"no materialized candidate/window results found under {root}"
        )
    return files


def _metric(payload: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return default
        current = current.get(key)
    return default if current is None else current


def _window_record(result: Mapping[str, Any]) -> dict[str, Any]:
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
    trade_rows = conservative_replay.get("trades") or []
    if not isinstance(trade_rows, list):
        raise TemporalDiscoveryContractError(
            "conservative replay trades must be an array"
        )
    entry_hours: dict[str, int] = {}
    mfe_values: list[float] = []
    mae_values: list[float] = []
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

    return {
        "candidateId": str(result.get("candidate_id") or ""),
        "windowId": (
            str(result.get("analysis_window_start"))
            + "/"
            + str(result.get("analysis_window_end"))
        ),
        "analysisWindowStart": result.get("analysis_window_start"),
        "analysisWindowEnd": result.get("analysis_window_end"),
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
        "conservativeNetR": float(
            conservative_metrics.get("totalNetR") or 0.0
        ),
        "noCostNetR": float(no_cost_metrics.get("totalNetR") or 0.0),
        "grossR": float(conservative_metrics.get("totalGrossR") or 0.0),
        "maxDrawdownR": float(
            conservative_metrics.get("maxDrawdownR") or 0.0
        ),
        "averageHoldingBars": conservative_metrics.get(
            "averageHoldingBars"
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
        "equityCurveR": [
            float(value)
            for value in conservative_metrics.get("equityCurveR") or []
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ],
    }


def load_stage_results(
    result_root: Path | str,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for path in _result_files(result_root):
        result = _read_json(path, name="candidate/window result")
        if result.get("schema_version") != "temporal_graph_candidate_window_result_v1":
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
        for key, value in counts.items()
    }
    total = sum(values.values())
    if total <= 0.0:
        return {}
    return {key: value / total for key, value in values.items()}


def _l1_distribution_distance(
    left: Mapping[str, float],
    right: Mapping[str, float],
) -> float:
    keys = set(left) | set(right)
    return 0.5 * sum(
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


def _aggregate_candidate(
    candidate: Mapping[str, Any],
    windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not windows:
        raise TemporalDiscoveryContractError(
            "candidate aggregate requires at least one window"
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
    win_rates = [
        float(window["winRate"])
        for window in windows
        if window["winRate"] is not None
    ]
    program_ids = {
        str(window["programSha256"]) for window in windows
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
    record = {
        "candidateId": candidate["candidateId"],
        "sourceMode": candidate["sourceMode"],
        "seedId": candidate["seedId"],
        "sourceProfileSha256": candidate["sourceProfileSha256"],
        "programSha256": next(iter(program_ids)),
        "windowCount": len(windows),
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
        "windowRecords": list(windows),
    }
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
        sum(
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
    complexity_delta = sum(
        _log_distance(
            float(left_complexity[key]),
            float(right_complexity[key]),
        )
        for key in sorted(left_complexity)
    ) / max(1, len(left_complexity))
    dimensions.append(complexity_delta)
    return sum(dimensions) / len(dimensions)


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
    eligible = [
        dict(candidate)
        for candidate in candidates
        if int(candidate["totalTrades"]) >= minimum_total_trades
    ]
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
            sum(
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
                sum(distance(candidate, chosen) for chosen in selected)
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
        by_program.setdefault(
            str(aggregate["programSha256"]),
            [],
        ).append(dict(aggregate))
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




__all__ = ['_result_files', '_metric', '_window_record', 'load_stage_results', '_result_set_sha256', '_distribution', '_l1_distribution_distance', '_log_distance', '_equity_shape', '_aggregate_candidate', 'fingerprint_distance', '_ECONOMIC_OBJECTIVES', '_dominates', 'pareto_fronts', 'select_economic_archive', 'select_novelty_archive', '_deduplicate_resolved_programs', '_confirmation_union']
