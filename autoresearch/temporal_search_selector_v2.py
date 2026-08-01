"""Transparent robust-envelope selector for Stage 5E-2 and later campaigns."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    canonical_sha256,
)
from .temporal_discovery_results import fingerprint_distance
from .temporal_search_quality import select_control_candidate_ids


SELECTOR_V2_VERSION = "temporal_discovery_selector_v2_robust_envelope"
SELECTOR_V2_RESULT_SCHEMA = "temporal_discovery_selection_v2"
SELECTOR_V2_MANIFEST_SCHEMA = "temporal_discovery_selection_v2_manifest_v1"
SELECTOR_V2_PARAMETERS: dict[str, Any] = {
    "version": SELECTOR_V2_VERSION,
    "minimumTradesEveryScreeningWindow": 1,
    "minimumEligibleCandidates": 32,
    "economicArchiveCap": 32,
    "admissibleNoveltyArchiveCap": 32,
    "diagnosticNoveltyArchiveCap": 32,
    "selectedUnionCap": 64,
    "stratifiedControlCount": 32,
    "confirmationTaskCandidateCap": 96,
    "activePopulationDefinition": "total_trades_positive",
    "drawdownQuantile": 0.75,
    "costDragPerTradeQuantile": 0.75,
}


def _number(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise TemporalDiscoveryContractError(f"{name} must be finite")
    return result


def _integer(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be an integer") from exc
    if result < 0:
        raise TemporalDiscoveryContractError(f"{name} must be non-negative")
    return result


def _quantile_upper(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise TemporalDiscoveryContractError("quantile requires a non-empty cohort")
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
    return ordered[index]


def _normalize_aggregate(value: Mapping[str, Any]) -> dict[str, Any]:
    required_fingerprint = (
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
        "transitionDistribution",
        "complexity",
    )
    candidate_id = str(value.get("candidateId") or "")
    trades_by_window = value.get("tradeCountsByWindow")
    if not candidate_id or not isinstance(trades_by_window, Sequence) or isinstance(
        trades_by_window, (str, bytes)
    ):
        raise TemporalDiscoveryContractError(
            "selector aggregates require candidateId and tradeCountsByWindow"
        )
    row = {
        "candidateId": candidate_id,
        "sourceMode": str(value.get("sourceMode") or ""),
        "seedId": str(value.get("seedId") or ""),
        "tradeCountsByWindow": [
            _integer(item, name="tradeCountsByWindow") for item in trades_by_window
        ],
        "totalTrades": _integer(value.get("totalTrades"), name="totalTrades"),
        "totalConservativeNetR": _number(
            value.get("totalConservativeNetR"), name="totalConservativeNetR"
        ),
        "worstWindowConservativeNetR": _number(
            value.get("worstWindowConservativeNetR"),
            name="worstWindowConservativeNetR",
        ),
        "maxWindowDrawdownR": _number(
            value.get("maxWindowDrawdownR"), name="maxWindowDrawdownR"
        ),
        "costDragR": _number(value.get("costDragR"), name="costDragR"),
        "managementActivationCount": _integer(
            value.get("managementActivationCount", 0),
            name="managementActivationCount",
        ),
        "rejectedIntentCount": _integer(
            value.get("rejectedIntentCount", 0), name="rejectedIntentCount"
        ),
    }
    for key in required_fingerprint:
        if key not in value:
            raise TemporalDiscoveryContractError(
                f"selector aggregate lacks fingerprint field {key!r}"
            )
        row[key] = _clone(value[key], name=f"aggregate.{key}")
    row["costDragPerTrade"] = (
        row["costDragR"] / row["totalTrades"]
        if row["totalTrades"] > 0
        else 0.0
    )
    row["rejectedIntentRate"] = (
        row["rejectedIntentCount"] / row["totalTrades"]
        if row["totalTrades"] > 0
        else 0.0
    )
    return row


_OBJECTIVES = (
    ("totalConservativeNetR", "max"),
    ("worstWindowConservativeNetR", "max"),
    ("maxWindowDrawdownR", "min"),
    ("costDragPerTrade", "min"),
)


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    no_worse = True
    strictly_better = False
    for key, direction in _OBJECTIVES:
        lvalue = float(left[key])
        rvalue = float(right[key])
        if direction == "max":
            no_worse = no_worse and lvalue >= rvalue
            strictly_better = strictly_better or lvalue > rvalue
        else:
            no_worse = no_worse and lvalue <= rvalue
            strictly_better = strictly_better or lvalue < rvalue
    return no_worse and strictly_better


def _pareto_fronts(rows: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    remaining = [dict(item) for item in sorted(rows, key=lambda item: item["candidateId"])]
    fronts = []
    while remaining:
        front = [
            row
            for row in remaining
            if not any(
                other["candidateId"] != row["candidateId"]
                and _dominates(other, row)
                for other in remaining
            )
        ]
        front.sort(
            key=lambda item: (
                -float(item["totalConservativeNetR"]),
                -float(item["worstWindowConservativeNetR"]),
                float(item["maxWindowDrawdownR"]),
                float(item["costDragPerTrade"]),
                -int(item["managementActivationCount"]),
                float(item["rejectedIntentRate"]),
                item["candidateId"],
            )
        )
        fronts.append(front)
        selected = {item["candidateId"] for item in front}
        remaining = [item for item in remaining if item["candidateId"] not in selected]
    return fronts


def _novelty_archive(
    rows: Sequence[Mapping[str, Any]], *, cap: int
) -> list[dict[str, Any]]:
    pool = [dict(item) for item in sorted(rows, key=lambda item: item["candidateId"])]
    if not pool or cap <= 0:
        return []
    first = max(
        pool,
        key=lambda item: (
            float(item["averageTransitionEntropy"]),
            int(item["managementActivationCount"]),
            -float(item["rejectedIntentRate"]),
            item["candidateId"],
        ),
    )
    selected = [first]
    remaining = [item for item in pool if item["candidateId"] != first["candidateId"]]
    while remaining and len(selected) < cap:
        ranked = []
        for item in remaining:
            distance = min(fingerprint_distance(item, chosen) for chosen in selected)
            ranked.append((distance, item))
        ranked.sort(
            key=lambda pair: (
                -pair[0],
                -int(pair[1]["managementActivationCount"]),
                float(pair[1]["rejectedIntentRate"]),
                pair[1]["candidateId"],
            )
        )
        distance, chosen = ranked[0]
        chosen = dict(chosen)
        chosen["minimumDistanceAtSelection"] = distance
        selected.append(chosen)
        remaining = [
            item for item in remaining if item["candidateId"] != chosen["candidateId"]
        ]
    selected[0] = dict(selected[0])
    selected[0]["minimumDistanceAtSelection"] = None
    return selected


def _public_row(row: Mapping[str, Any], *, rank: int, archive: str) -> dict[str, Any]:
    return {
        "candidateId": row["candidateId"],
        "archive": archive,
        "rank": rank,
        "totalTrades": row["totalTrades"],
        "tradeCountsByWindow": list(row["tradeCountsByWindow"]),
        "totalConservativeNetR": row["totalConservativeNetR"],
        "worstWindowConservativeNetR": row["worstWindowConservativeNetR"],
        "maxWindowDrawdownR": row["maxWindowDrawdownR"],
        "costDragPerTrade": row["costDragPerTrade"],
        "managementActivationCount": row["managementActivationCount"],
        "rejectedIntentRate": row["rejectedIntentRate"],
        **(
            {"minimumDistanceAtSelection": row.get("minimumDistanceAtSelection")}
            if "novelty" in archive
            else {}
        ),
    }


def evaluate_policy_v2_envelope(
    *,
    population_candidates: Sequence[Mapping[str, Any]],
    screening_aggregates: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate the frozen robust envelope without relaxing its minimum size."""

    config = _clone(parameters or SELECTOR_V2_PARAMETERS, name="selector v2 parameters")
    if config != SELECTOR_V2_PARAMETERS:
        raise TemporalDiscoveryContractError(
            "Stage 5E-2 selector parameters must equal the repository constant"
        )
    population = [
        {
            "candidateId": str(item.get("candidateId") or ""),
            "sourceMode": str(item.get("sourceMode") or ""),
            "seedId": str(item.get("seedId") or ""),
        }
        for item in population_candidates
    ]
    population.sort(key=lambda item: item["candidateId"])
    if len(population) != len({item["candidateId"] for item in population}):
        raise TemporalDiscoveryContractError("population candidate IDs must be unique")
    aggregates = [_normalize_aggregate(item) for item in screening_aggregates]
    aggregates.sort(key=lambda item: item["candidateId"])
    aggregate_ids = {item["candidateId"] for item in aggregates}
    population_ids = {item["candidateId"] for item in population}
    if aggregate_ids != population_ids:
        raise TemporalDiscoveryContractError(
            "screening aggregates must cover the exact population"
        )
    window_counts = {len(item["tradeCountsByWindow"]) for item in aggregates}
    if len(window_counts) != 1 or next(iter(window_counts)) < 1:
        raise TemporalDiscoveryContractError(
            "screening aggregates require one shared non-empty window shape"
        )
    active = [item for item in aggregates if item["totalTrades"] > 0]
    if not active:
        raise TemporalDiscoveryContractError("selector v2 active population is empty")
    thresholds = {
        "activePopulationCount": len(active),
        "totalConservativeNetRMedian": statistics.median(
            item["totalConservativeNetR"] for item in active
        ),
        "worstWindowConservativeNetRMedian": statistics.median(
            item["worstWindowConservativeNetR"] for item in active
        ),
        "maxWindowDrawdownRP75": _quantile_upper(
            [item["maxWindowDrawdownR"] for item in active],
            float(config["drawdownQuantile"]),
        ),
        "costDragPerTradeP75": _quantile_upper(
            [item["costDragPerTrade"] for item in active],
            float(config["costDragPerTradeQuantile"]),
        ),
        "minimumTradesEveryScreeningWindow": int(
            config["minimumTradesEveryScreeningWindow"]
        ),
    }
    eligibility = []
    eligible = []
    for item in aggregates:
        checks = {
            "minimumTradesEveryWindow": all(
                count >= thresholds["minimumTradesEveryScreeningWindow"]
                for count in item["tradeCountsByWindow"]
            ),
            "totalRAtLeastActiveMedian": item["totalConservativeNetR"]
            >= thresholds["totalConservativeNetRMedian"],
            "worstWindowRAtLeastActiveMedian": item[
                "worstWindowConservativeNetR"
            ]
            >= thresholds["worstWindowConservativeNetRMedian"],
            "drawdownAtMostActiveP75": item["maxWindowDrawdownR"]
            <= thresholds["maxWindowDrawdownRP75"],
            "costDragPerTradeAtMostActiveP75": item["costDragPerTrade"]
            <= thresholds["costDragPerTradeP75"],
        }
        is_eligible = all(checks.values())
        eligibility.append(
            {
                "candidateId": item["candidateId"],
                "eligible": is_eligible,
                "checks": checks,
            }
        )
        if is_eligible:
            eligible.append(item)
    return {
        "parameters": config,
        "population": population,
        "aggregates": aggregates,
        "active": active,
        "thresholds": thresholds,
        "eligibility": eligibility,
        "eligible": eligible,
    }


def select_policy_v2(
    *,
    population_candidates: Sequence[Mapping[str, Any]],
    screening_aggregates: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    envelope = evaluate_policy_v2_envelope(
        population_candidates=population_candidates,
        screening_aggregates=screening_aggregates,
        parameters=parameters,
    )
    config = envelope["parameters"]
    population = envelope["population"]
    aggregates = envelope["aggregates"]
    active = envelope["active"]
    thresholds = envelope["thresholds"]
    eligibility = envelope["eligibility"]
    eligible = envelope["eligible"]
    if len(eligible) < int(config["minimumEligibleCandidates"]):
        raise TemporalDiscoveryContractError(
            "selector v2 robust envelope is too small; refusing to relax thresholds: "
            f"eligible={len(eligible)}, required={config['minimumEligibleCandidates']}"
        )

    economic = []
    for front_index, front in enumerate(_pareto_fronts(eligible)):
        for row in front:
            selected = dict(row)
            selected["paretoFront"] = front_index
            economic.append(selected)
            if len(economic) == int(config["economicArchiveCap"]):
                break
        if len(economic) == int(config["economicArchiveCap"]):
            break
    novelty = _novelty_archive(
        eligible, cap=int(config["admissibleNoveltyArchiveCap"])
    )
    diagnostic = _novelty_archive(
        aggregates, cap=int(config["diagnosticNoveltyArchiveCap"])
    )
    selected_ids = []
    for row in [*economic, *novelty]:
        if row["candidateId"] not in selected_ids:
            selected_ids.append(row["candidateId"])
    if len(selected_ids) > int(config["selectedUnionCap"]):
        raise TemporalDiscoveryContractError("selector v2 selected union exceeded cap")
    control_ids, control_strata = select_control_candidate_ids(
        population_candidates=population,
        excluded_candidate_ids=selected_ids,
        sample_size=int(config["stratifiedControlCount"]),
    )
    confirmation_ids = sorted([*selected_ids, *control_ids])
    if len(confirmation_ids) > int(config["confirmationTaskCandidateCap"]):
        raise TemporalDiscoveryContractError("selector v2 confirmation union exceeded cap")
    diagnostic_ids = [item["candidateId"] for item in diagnostic]
    result = {
        "schemaVersion": SELECTOR_V2_RESULT_SCHEMA,
        "selectorVersion": SELECTOR_V2_VERSION,
        "parameters": config,
        "inputPopulationSha256": canonical_sha256(population),
        "screeningAggregateSetSha256": canonical_sha256(aggregates),
        "thresholds": thresholds,
        "activePopulationCount": len(active),
        "eligibleCandidateCount": len(eligible),
        "eligibility": eligibility,
        "economicArchive": [
            {
                **_public_row(row, rank=index, archive="economic_promotion"),
                "paretoFront": row["paretoFront"],
            }
            for index, row in enumerate(economic)
        ],
        "admissibleNoveltyArchive": [
            _public_row(row, rank=index, archive="admissible_novelty")
            for index, row in enumerate(novelty)
        ],
        "diagnosticPureNoveltyArchive": [
            _public_row(row, rank=index, archive="diagnostic_pure_novelty_non_promotable")
            for index, row in enumerate(diagnostic)
        ],
        "selectedCandidateIds": sorted(selected_ids),
        "selectedCandidateCount": len(selected_ids),
        "stratifiedControlCandidateIds": control_ids,
        "stratifiedControlCount": len(control_ids),
        "controlStrata": control_strata,
        "confirmationCandidateIds": confirmation_ids,
        "confirmationCandidateCount": len(confirmation_ids),
        "diagnosticOnlyCandidateIds": sorted(set(diagnostic_ids) - set(selected_ids)),
        "selectionInputs": [
            "screening_results",
            "population_source_mode_and_seed_metadata",
            "frozen_selector_parameters",
        ],
        "prohibitedSelectionInputs": [
            "confirmation_results",
            "control_results",
            "activation_dossiers",
            "reserved_evidence",
        ],
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    result["selectionSha256"] = canonical_sha256(result)
    return result


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    ) + "\n"


def freeze_policy_v2_selection(
    *,
    population_candidates: Sequence[Mapping[str, Any]],
    screening_aggregates: Sequence[Mapping[str, Any]],
    output_root: Path | str,
) -> dict[str, Any]:
    result = select_policy_v2(
        population_candidates=population_candidates,
        screening_aggregates=screening_aggregates,
    )
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "selection.json"
    encoded = _encoded(result)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError("refusing divergent selector v2 artifact")
    path.write_text(encoded, encoding="utf-8")
    files = [
        {
            "relativePath": "selection.json",
            "length": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
        }
    ]
    manifest = {
        "schemaVersion": SELECTOR_V2_MANIFEST_SCHEMA,
        "selectionSha256": result["selectionSha256"],
        "fileCount": 1,
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    manifest_path = root / "manifest.json"
    encoded_manifest = _encoded(manifest)
    if (
        manifest_path.exists()
        and manifest_path.read_text(encoding="utf-8") != encoded_manifest
    ):
        raise TemporalDiscoveryContractError("refusing divergent selector v2 manifest")
    manifest_path.write_text(encoded_manifest, encoding="utf-8")
    return {
        "schemaVersion": "temporal_discovery_selection_v2_result_v1",
        "selectionSha256": result["selectionSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "eligibleCandidateCount": result["eligibleCandidateCount"],
        "selectedCandidateCount": result["selectedCandidateCount"],
        "stratifiedControlCount": result["stratifiedControlCount"],
        "confirmationCandidateCount": result["confirmationCandidateCount"],
    }


__all__ = [
    "SELECTOR_V2_PARAMETERS",
    "SELECTOR_V2_VERSION",
    "evaluate_policy_v2_envelope",
    "freeze_policy_v2_selection",
    "select_policy_v2",
]
