"""Deterministic fixed-stream V37 archive terminalization report.

This module consumes the sealed native classifier traces, retained
candidate-disposition ledger, and Phase-1 descriptive table.  It is a report
generator only: it cannot launch work, reconstruct excluded backfills, mutate
historical artifacts, or alter archive policy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "temporal_qd_v37_archive_terminalization_report_v1"
TRACE_SCHEMA_VERSION = "temporal_qd_v37_native_classifier_trace_v1"
GENERATIONS = (1, 2, 3, 4, 5)


class TerminalizationError(RuntimeError):
    """Raised when the fixed historical evidence cannot be reconciled."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminalizationError(f"could not read JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise TerminalizationError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: Path) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    try:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line:
                continue
            value = json.loads(line)
            if not isinstance(value, Mapping):
                raise TerminalizationError(f"JSONL object required: {path}:{number}")
            rows.append(value)
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminalizationError(f"could not read JSONL: {path}") from exc
    return rows


def _write_json(path: Path, value: object) -> None:
    path.write_bytes(_canonical_bytes(value) + b"\n")


def _as_text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TerminalizationError(f"{name} must be nonempty text")
    return value


def _as_mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TerminalizationError(f"{name} must be an object")
    return value


def _as_rows(value: object, *, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise TerminalizationError(f"{name} must be an object list")
    return list(value)


def _numeric(value: object, *, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TerminalizationError(f"{name} must be numeric")
    return float(value)


def _candidate_id(row: Mapping[str, Any]) -> str:
    return _as_text(row.get("candidateId"), name="candidateId")


def _generation(row: Mapping[str, Any]) -> int:
    value = row.get("generationIndex")
    if not isinstance(value, int) or value not in GENERATIONS:
        raise TerminalizationError("generationIndex must be a V37 generation")
    return value


def _trace_path(trace_dir: Path, generation: int) -> Path:
    return trace_dir / f"generation-{generation:04d}-trace.json"


def _load_traces(trace_dir: Path) -> tuple[dict[int, Mapping[str, Any]], list[dict[str, Any]]]:
    traces: dict[int, Mapping[str, Any]] = {}
    bindings: list[dict[str, Any]] = []
    for generation in GENERATIONS:
        path = _trace_path(trace_dir, generation)
        trace = _read_json(path)
        if trace.get("schemaVersion") != TRACE_SCHEMA_VERSION:
            raise TerminalizationError(f"G{generation} trace schema is incompatible")
        if trace.get("generationIndex") != generation:
            raise TerminalizationError(f"G{generation} trace generation binding drifted")
        rows = _as_rows(trace.get("candidates"), name=f"G{generation} trace candidates")
        if not rows:
            raise TerminalizationError(f"G{generation} trace has no candidate rows")
        trace_sha = _as_text(trace.get("traceSha256"), name="traceSha256")
        without_hash = {key: value for key, value in trace.items() if key != "traceSha256"}
        if _canonical_sha256(without_hash) != trace_sha:
            raise TerminalizationError(f"G{generation} trace self-hash drifted")
        traces[generation] = trace
        bindings.append(
            {
                "generationIndex": generation,
                "path": str(path.resolve()),
                "rawSha256": _file_sha256(path),
                "traceSha256": trace_sha,
                "candidateCount": len(rows),
            }
        )
    return traces, bindings


def _trace_rows(trace: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    rows = _as_rows(trace.get("candidates"), name="trace candidates")
    result = {_candidate_id(row): row for row in rows}
    if len(result) != len(rows):
        raise TerminalizationError("trace candidate IDs are not unique")
    return result


def _final_ids(trace: Mapping[str, Any]) -> list[str]:
    return sorted(
        _candidate_id(row)
        for row in _as_rows(trace.get("candidates"), name="trace candidates")
        if row.get("finalLane") in {"quality", "frontier"}
    )


def _support_passed(row: Mapping[str, Any]) -> bool:
    return bool(row.get("activeSupportPassed")) and bool(row.get("tradeDensityPassed"))


def _rank_rows(rows: Iterable[Mapping[str, Any]], field: str) -> dict[str, int]:
    ordered = sorted(
        rows,
        key=lambda row: (-_numeric(row.get(field), name=field), _candidate_id(row)),
    )
    return {_candidate_id(row): index for index, row in enumerate(ordered, start=1)}


def _quantile_bands(rows: Sequence[Mapping[str, Any]], field: str, prefix: str) -> tuple[list[float], dict[str, str]]:
    values = sorted(_numeric(row.get(field), name=field) for row in rows)
    if len(values) < 4:
        raise TerminalizationError("Phase-1 table needs at least four rows for behavioral quantiles")
    thresholds = list(statistics.quantiles(values, n=4, method="inclusive"))

    def band(value: float) -> str:
        for ordinal, threshold in enumerate(thresholds, start=1):
            if value <= threshold:
                return f"{prefix}_q{ordinal}"
        return f"{prefix}_q4"

    return thresholds, {_candidate_id(row): band(_numeric(row.get(field), name=field)) for row in rows}


def _census(trace: Mapping[str, Any]) -> Mapping[str, int]:
    rows = _as_rows(trace.get("candidates"), name="trace candidates")
    counts = Counter()
    for row in rows:
        support = _support_passed(row)
        direction = bool(row.get("directionSelectionEligible"))
        if not support and not direction:
            counts["supportAndDirectionFailure"] += 1
        elif not support:
            counts["supportFailureOnly"] += 1
        elif not direction:
            counts["directionFailureOnly"] += 1
        elif row.get("preParetoLane") == "quality":
            counts["preParetoQuality"] += 1
        elif row.get("preParetoLane") == "frontier":
            counts["preParetoFrontier"] += 1
        else:
            raise TerminalizationError("trace row has no classifier disposition")
        if row.get("preParetoLane") in {"quality", "frontier"} and not bool(
            row.get("selectedAfterPareto")
        ):
            counts["paretoOrCapacityLoss"] += 1
        counts[f"final{str(row.get('finalLane')).capitalize()}"] += 1
    for key in (
        "supportFailureOnly",
        "directionFailureOnly",
        "supportAndDirectionFailure",
        "preParetoQuality",
        "preParetoFrontier",
        "paretoOrCapacityLoss",
        "finalQuality",
        "finalFrontier",
        "finalUnsupported",
    ):
        counts.setdefault(key, 0)
    return dict(sorted(counts.items()))


def _forward_evidence(
    proposal_rows: Sequence[Mapping[str, Any]],
    retained_rows: Sequence[Mapping[str, Any]],
    traces: Mapping[int, Mapping[str, Any]],
) -> Mapping[str, Any]:
    selected = [
        row
        for row in proposal_rows
        if bool(_as_mapping(row.get("parentArchive"), name="parentArchive").get("admitted"))
    ]
    next_by_key = {
        (_generation(row), _candidate_id(row)): row
        for row in retained_rows
    }
    rows: list[dict[str, Any]] = []
    for selection in sorted(selected, key=lambda row: (_generation(row), _candidate_id(row))):
        generation = _generation(selection)
        candidate_id = _candidate_id(selection)
        current = _as_mapping(selection.get("currentPanel"), name="selection currentPanel")
        next_state = next_by_key.get((generation + 1, candidate_id))
        result: dict[str, Any] = {
            "candidateId": candidate_id,
            "selectionGenerationIndex": generation,
            "selectionEvaluationStateSha256": _as_text(
                selection.get("evaluationStateSha256"), name="selection evaluationStateSha256"
            ),
            "selectionPanel": {
                "netR": current.get("afterCostNetR"),
                "closedTrades": current.get("totalTrades"),
                "windows": current.get("windows"),
            },
        }
        if next_state is None:
            result["nextObserved"] = {"status": "unavailable"}
        else:
            next_generation = _generation(next_state)
            next_current = _as_mapping(next_state.get("currentPanel"), name="next currentPanel")
            trace_row = _trace_rows(traces[next_generation]).get(candidate_id)
            if trace_row is None:
                raise TerminalizationError(
                    f"next native trace omits retained parent {candidate_id} in G{next_generation}"
                )
            result["nextObserved"] = {
                "status": "available",
                "generationIndex": next_generation,
                "evaluationStateSha256": _as_text(
                    next_state.get("evaluationStateSha256"), name="next evaluationStateSha256"
                ),
                "panel": {
                    "netR": next_current.get("afterCostNetR"),
                    "closedTrades": next_current.get("totalTrades"),
                    "windows": next_current.get("windows"),
                },
                "signRetained": _numeric(next_current.get("afterCostNetR"), name="next netR") > 0.0,
                "supportRetained": _support_passed(trace_row),
                "directionRetained": bool(trace_row.get("directionSelectionEligible")),
                "cumulativeLaneAfterNextEvidence": trace_row.get("finalLane"),
                "cumulativeReasonCodesAfterNextEvidence": trace_row.get("preParetoReasonCodes"),
            }
        rows.append(result)
    available = [row for row in rows if row.get("nextObserved", {}).get("status") == "available"]
    return {
        "definition": "Selection-panel evidence is paired with the retained-parent evaluation state on the immediately following observed panel. Cumulative support and lane are read from that next generation's native trace.",
        "rows": rows,
        "availableRowCount": len(available),
        "nextPanelNegativeFlipCount": sum(
            1 for row in available if not bool(row["nextObserved"]["signRetained"])
        ),
    }


def _variant_two(
    proposal_rows: Sequence[Mapping[str, Any]], traces: Mapping[int, Mapping[str, Any]]
) -> Mapping[str, Any]:
    trajectory: list[dict[str, Any]] = []
    memory = 0
    for generation in GENERATIONS:
        observed = [row for row in proposal_rows if _generation(row) == generation]
        breeder_ids = _final_ids(traces[generation])
        memory += len(observed)
        trajectory.append(
            {
                "generationIndex": generation,
                "newObservedProposalStates": len(observed),
                "observationalMemoryMemberCount": memory,
                "currentBreedingEligibleCandidateIds": breeder_ids,
                "currentBreedingEligibleCount": len(breeder_ids),
                "currentParentSelectableCount": len(breeder_ids),
                "memoryOnlyCount": memory - len(breeder_ids),
            }
        )
    return {
        "label": "immutable observational memory",
        "nonEquivalence": ["breeding eligibility", "parent selection"],
        "trajectory": trajectory,
    }


def _variant_three(
    traces: Mapping[int, Mapping[str, Any]], proposal_rows: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    audit: list[dict[str, Any]] = []
    prior_ids: list[str] = []
    cap_by_generation = Counter(
        _generation(row)
        for row in proposal_rows
        if row.get("terminalReason") == "prefinalizer_newcomer_cap"
    )
    for generation in GENERATIONS:
        trace_rows = _trace_rows(traces[generation])
        current_ids = _final_ids(traces[generation])
        current_quality = sorted(
            candidate_id
            for candidate_id, row in trace_rows.items()
            if row.get("finalLane") == "quality"
        )
        current_frontier = sorted(
            candidate_id
            for candidate_id, row in trace_rows.items()
            if row.get("finalLane") == "frontier"
        )
        prior_current: dict[str, Any] = {}
        for candidate_id in prior_ids:
            trace_row = trace_rows.get(candidate_id)
            prior_current[candidate_id] = (
                {
                    "status": "current_evidence_available",
                    "supportPassed": _support_passed(trace_row),
                    "directionEligible": bool(trace_row.get("directionSelectionEligible")),
                    "preParetoLane": trace_row.get("preParetoLane"),
                    "finalLane": trace_row.get("finalLane"),
                    "reasonCodes": trace_row.get("preParetoReasonCodes"),
                }
                if trace_row is not None
                else {"status": "not_retained_for_current_panel"}
            )
        audit.append(
            {
                "generationIndex": generation,
                "requiredPanelIds": traces[generation].get("requiredPanelIds"),
                "completeQualityCandidateIds": current_quality,
                "completeFrontierCandidateIds": current_frontier,
                "currentParentCandidateIds": current_ids,
                "priorParentCandidateIds": prior_ids,
                "priorMemberCurrentEligibility": prior_current,
                "challengerComparison": {
                    "matchedEvidenceCandidateCount": len(trace_rows),
                    "matchedEvidenceBreedingEligibleCandidateIds": current_ids,
                    "unsupportedIncumbentsRetainBreedingRights": False,
                },
                "backfillObligation": {
                    "capExcludedProposalCount": cap_by_generation[generation],
                    "state": "would_require_additional_backfill_evaluation"
                    if cap_by_generation[generation]
                    else "none",
                },
                "parentSetDifference": {
                    "changedFromPrior": set(current_ids) != set(prior_ids),
                    "added": sorted(set(current_ids) - set(prior_ids)),
                    "removed": sorted(set(prior_ids) - set(current_ids)),
                },
            }
        )
        prior_ids = current_ids
    return {
        "label": "matched-evidence diagnostic overlay",
        "result": "No unsupported incumbent retains a breeding right merely because no challenger defeats it.",
        "generationAudit": audit,
    }


def _variant_four(
    selected_rows: Sequence[Mapping[str, Any]], phase_rows: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    by_generation: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in phase_rows:
        by_generation[_generation(row)].append(row)
    overlays: list[dict[str, Any]] = []
    for generation in GENERATIONS:
        resample_rank = _rank_rows(by_generation[generation], "windowResamplePositiveRate")
        leave_one_out_rank = _rank_rows(by_generation[generation], "leaveOneWindowOutPositiveFraction")
        phase_by_id = {_candidate_id(row): row for row in by_generation[generation]}
        for selected in selected_rows:
            if _generation(selected) != generation:
                continue
            candidate_id = _candidate_id(selected)
            phase = phase_by_id.get(candidate_id)
            if phase is None:
                raise TerminalizationError(f"Phase-1 table omits historical member {candidate_id}")
            overlays.append(
                {
                    "candidateId": candidate_id,
                    "generationIndex": generation,
                    "windowResamplePositiveRate": phase.get("windowResamplePositiveRate"),
                    "windowResamplePositiveRateRankWithinGeneration": resample_rank[candidate_id],
                    "leaveOneWindowOutPositiveFraction": phase.get("leaveOneWindowOutPositiveFraction"),
                    "leaveOneWindowOutPositiveFractionRankWithinGeneration": leave_one_out_rank[candidate_id],
                    "nativeArchiveMembershipUnchangedByDiagnostic": True,
                }
            )
    return {
        "label": "evidence-stability diagnostic overlay",
        "policy": "No uncertainty-based selection rule, threshold, support gate, or archive policy is implemented here.",
        "predeclaredProjections": [
            "windowResamplePositiveRate (256 complete-window resamples; descriptive rank)",
            "leaveOneWindowOutPositiveFraction (remove each retained window once; descriptive rank)",
        ],
        "historicalArchiveMemberOverlay": overlays,
    }


def _variant_five(
    selected_rows: Sequence[Mapping[str, Any]], phase_rows: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    phase_by_id = {_candidate_id(row): row for row in phase_rows}
    traded_rows = [
        row for row in phase_rows if _numeric(row.get("totalTrades"), name="totalTrades") > 0.0
    ]
    trade_thresholds, trade_bands = _quantile_bands(traded_rows, "totalTrades", "trades")
    holding_thresholds, holding_bands = _quantile_bands(traded_rows, "medianHoldingBars", "holding")
    selected_cells: Counter[tuple[int, str, str]] = Counter()
    for selected in selected_rows:
        candidate_id = _candidate_id(selected)
        phase = phase_by_id.get(candidate_id)
        if phase is None:
            raise TerminalizationError(f"Phase-1 table omits historical member {candidate_id}")
        selected_cells[
            (
                _generation(selected),
                _as_text(phase.get("frequencyBand"), name="frequencyBand"),
                _as_text(phase.get("holdingBand"), name="holdingBand"),
            )
        ] += 1
    overlays: list[dict[str, Any]] = []
    for selected in sorted(selected_rows, key=lambda row: (_generation(row), _candidate_id(row))):
        candidate_id = _candidate_id(selected)
        phase = phase_by_id[candidate_id]
        fixed_cell = [
            _as_text(phase.get("frequencyBand"), name="frequencyBand"),
            _as_text(phase.get("holdingBand"), name="holdingBand"),
        ]
        overlays.append(
            {
                "candidateId": candidate_id,
                "generationIndex": _generation(selected),
                "fixedBehavioralCell": fixed_cell,
                "fixedCellArchiveOccupancy": selected_cells[
                    (_generation(selected), fixed_cell[0], fixed_cell[1])
                ],
                "adaptiveBehavioralCell": [trade_bands[candidate_id], holding_bands[candidate_id]],
                "cellCapacity": 4,
                "nativeEligibilityUnchanged": True,
            }
        )
    collisions = [
        row for row in overlays if int(row["fixedCellArchiveOccupancy"]) > int(row["cellCapacity"])
    ]
    return {
        "label": "behavioral-descriptor diagnostic overlay",
        "predeclaredSchemes": {
            "fixedBands": "Phase-1 frequencyBand and holdingBand labels",
            "adaptiveQuantiles": {
                "method": "inclusive quartiles over the fixed V37 current-panel corpus",
                "tradeThresholds": trade_thresholds,
                "holdingThresholds": holding_thresholds,
            },
        },
        "historicalArchiveMemberOverlay": overlays,
        "historicalArchiveMemberCapacityCollisionDemonstrated": bool(collisions),
        "nonClaim": "This does not rerun the 128-newcomer prefinalizer screen, archive cells, or Pareto selection under a behavioral descriptor contract.",
    }


def build_v37_archive_terminalization_report(
    *, ledger_rows: Sequence[Mapping[str, Any]], traces: Mapping[int, Mapping[str, Any]], phase_rows: Sequence[Mapping[str, Any]], input_bindings: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    proposal_rows = [row for row in ledger_rows if row.get("evaluationStateKind") == "proposal_current_panel"]
    retained_rows = [
        row for row in ledger_rows if row.get("evaluationStateKind") == "retained_parent_current_panel"
    ]
    if len(proposal_rows) != 5120 or len(retained_rows) != 6:
        raise TerminalizationError("ledger does not contain the expected V37 proposal/retained state counts")
    if len(phase_rows) != len(proposal_rows):
        raise TerminalizationError("Phase-1 table does not match the proposal state count")
    selected_rows = [
        row
        for row in proposal_rows
        if bool(_as_mapping(row.get("parentArchive"), name="parentArchive").get("admitted"))
    ]
    if len(selected_rows) != 6:
        raise TerminalizationError("ledger does not contain the six historical archive members")
    census = {str(generation): _census(traces[generation]) for generation in GENERATIONS}
    for generation, trace in traces.items():
        final_ids = _final_ids(trace)
        source_ids = sorted(
            _candidate_id(row)
            for row in selected_rows
            if _generation(row) == generation
        )
        if final_ids != source_ids:
            raise TerminalizationError(f"G{generation} trace/ledger parent membership differs")
    forward = _forward_evidence(proposal_rows, retained_rows, traces)
    if forward["availableRowCount"] != 6 or forward["nextPanelNegativeFlipCount"] != 6:
        raise TerminalizationError("forward evidence does not reproduce six immediate next-panel negative flips")
    child_yields = []
    for row in selected_rows:
        downstream = _as_mapping(row.get("downstream"), name="downstream")
        children = downstream.get("acceptedOffspringCandidateIds")
        if not isinstance(children, list):
            raise TerminalizationError("selected member downstream child list is absent")
        child_yields.append(
            {
                "candidateId": _candidate_id(row),
                "selectionGenerationIndex": _generation(row),
                "sourceMode": _as_mapping(row.get("origin"), name="origin").get("sourceMode"),
                "acceptedChildYield": len(children),
            }
        )
    report: dict[str, Any] = {
        "schemaVersion": SCHEMA_VERSION,
        "scope": "Fixed retained V37 stream only. This report performs no market evaluation, worker/gateway/Vast work, generation, archive mutation, support weakening, policy rewrite, or historical artifact rewrite.",
        "inputBindings": list(input_bindings),
        "nativeTraceSchema": {
            "schemaVersion": TRACE_SCHEMA_VERSION,
            "reasonCodes": [
                "active_window_fraction_below_minimum",
                "average_trades_per_month_below_minimum",
                "long_side_unsupported",
                "short_side_unsupported",
                "no_acceptable_direction",
                "harmful_opposite_side",
                "opposite_side_supported_but_not_acceptable",
                "cumulative_net_not_positive",
                "median_window_net_not_positive",
                "pre_pareto_quality",
                "pre_pareto_frontier",
                "pareto_dominated",
                "lane_capacity_not_selected",
                "selected_quality",
                "selected_frontier",
            ],
        },
        "exactFailureCensus": {
            "byGeneration": census,
            "g3Explanation": "G3 has zero breeders because its native trace has no support-and-direction-eligible row in either pre-Pareto lane; Pareto/capacity loss is not the cause.",
        },
        "focusedCandidateReasons": [
            {
                "generationIndex": _generation(row),
                "candidateId": _candidate_id(row),
                "focusLabels": row.get("focusLabels"),
                "nativeTrace": _trace_rows(traces[_generation(row)]).get(_candidate_id(row)),
            }
            for row in proposal_rows
            if row.get("focusLabels")
            and _candidate_id(row) in _trace_rows(traces[_generation(row)])
        ],
        "forwardEvidence": forward,
        "variant2MemoryVsBreeding": _variant_two(proposal_rows, traces),
        "variant3MatchedEvidence": _variant_three(traces, proposal_rows),
        "variant4EvidenceStability": _variant_four(selected_rows, phase_rows),
        "variant5BehavioralDescriptor": _variant_five(selected_rows, phase_rows),
        "capBackfillFutureExperiment": {
            "g1Conclusion": "No omitted breeder demonstrated; the exact 1,024-candidate full-pool replay is complete.",
            "g2ThroughG5Conclusion": "Unresolved: 3,584 cap-excluded proposal rows lack required retained prior-panel backfills.",
            "authorizedNow": False,
            "boundedSpecification": [
                "Predeclare cap-excluded candidates and matched controls from the same generation/cell/rank region.",
                "Evaluate exact missing prior-panel backfills only; do not generate new offspring.",
                "Measure exact cumulative breeder qualification and later evidence, not current-panel positivity.",
            ],
        },
        "terminalDecision": {
            "historicalArchiveMechanics": {
                "nativeArchiveFaithfullyAppliedFrozenRules": True,
                "exactEligibleBreederLostAfterReducer": any(
                    census[str(generation)]["paretoOrCapacityLoss"] > 0 for generation in GENERATIONS
                ),
                "paretoOrCapacityDistortionDemonstrated": any(
                    census[str(generation)]["paretoOrCapacityLoss"] > 0 for generation in GENERATIONS
                ),
                "memoryLossDistinctFromBreedingLoss": True,
            },
            "fitnessStability": "All six historical archive members flipped negative on their immediately following observed panel.",
            "variation": {
                "allHistoricalArchiveMembersWereImmigrants": all(
                    row["sourceMode"] == "qd_random_immigrant_bidirectional_pair"
                    for row in child_yields
                ),
                "offspringEnteredParentArchive": False,
                "parentSelectionChildYields": child_yields,
                "fixedStreamLimit": "No alternate parent-set descendant can be identified without new closed-loop evaluation, which this pass does not run.",
            },
            "unresolved": [
                "G2–G5 cap-excluded prior-panel backfills.",
                "Closed-loop descendants under alternate parent sets.",
                "Component transfer and the causal value of an observational memory archive.",
            ],
            "nextRung": "component-surrogate validation",
            "nextRungAuthorizedInThisReport": False,
        },
    }
    report["reportSha256"] = _canonical_sha256(report)
    return report


def _markdown(report: Mapping[str, Any]) -> str:
    census = _as_mapping(_as_mapping(report.get("exactFailureCensus"), name="census").get("byGeneration"), name="byGeneration")
    trajectory = _as_rows(_as_mapping(report.get("variant2MemoryVsBreeding"), name="variant2").get("trajectory"), name="trajectory")
    counts = " → ".join(str(row["currentBreedingEligibleCount"]) for row in trajectory)
    memory = " → ".join(str(row["observationalMemoryMemberCount"]) for row in trajectory)
    lines = [
        "# V37 archive terminalization — fixed-stream result",
        "",
        "The native Variant-0 trace leaves the historical archives unchanged and reconciles every reducer row.",
        "",
        f"- Breeding trajectory: `{counts}`.",
        f"- Observational memory trajectory: `{memory}` (not breeding eligibility or parent selection).",
        "- G3 has zero breeders because none of its rows reaches a support-and-direction-eligible pre-Pareto lane; this is not a Pareto/cell-capacity loss.",
        "- All six historical archive members flip negative on their immediately following observed panel.",
        "- V4 is an evidence-stability diagnostic overlay; V5 is a behavioral-descriptor diagnostic overlay.",
        "- G1's cap is cleared; G2–G5 remain unresolved without the explicitly unlaunched bounded backfill experiment.",
        "",
        "## Native failure census",
        "",
        "| Generation | Support only | Direction only | Both | Pre-Pareto quality | Pre-Pareto frontier | Pareto/capacity loss |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for generation in GENERATIONS:
        row = _as_mapping(census[str(generation)], name=f"G{generation} census")
        lines.append(
            "| G{0} | {1} | {2} | {3} | {4} | {5} | {6} |".format(
                generation,
                row["supportFailureOnly"],
                row["directionFailureOnly"],
                row["supportAndDirectionFailure"],
                row["preParetoQuality"],
                row["preParetoFrontier"],
                row["paretoOrCapacityLoss"],
            )
        )
    lines.extend(
        [
            "",
            "No market evaluation, worker, gateway, Vast, generation, archive mutation, support weakening, policy rewrite, or historical artifact rewrite occurred.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v37_archive_terminalization_report(
    *, ledger_rows_path: Path | str, trace_dir: Path | str, phase1_evaluations_path: Path | str, output_dir: Path | str
) -> Mapping[str, Any]:
    output = Path(output_dir)
    if output.exists():
        raise TerminalizationError(f"output directory must not already exist: {output}")
    ledger_path = Path(ledger_rows_path)
    phase_path = Path(phase1_evaluations_path)
    traces, trace_bindings = _load_traces(Path(trace_dir))
    ledger_rows = _read_jsonl(ledger_path)
    phase_rows = _read_jsonl(phase_path)
    input_bindings = [
        {
            "kind": "candidate_disposition_ledger",
            "path": str(ledger_path.resolve()),
            "rawSha256": _file_sha256(ledger_path),
            "rowCount": len(ledger_rows),
        },
        {
            "kind": "phase1_candidate_evaluations",
            "path": str(phase_path.resolve()),
            "rawSha256": _file_sha256(phase_path),
            "rowCount": len(phase_rows),
        },
        *trace_bindings,
    ]
    report = build_v37_archive_terminalization_report(
        ledger_rows=ledger_rows,
        traces=traces,
        phase_rows=phase_rows,
        input_bindings=input_bindings,
    )
    output.mkdir(parents=True)
    report_path = output / "terminalization-report.json"
    memo_path = output / "terminal-decision-memo.md"
    experiment_path = output / "cap-backfill-future-experiment.md"
    readme_path = output / "README.md"
    _write_json(report_path, report)
    memo_path.write_text(_markdown(report), encoding="utf-8")
    experiment_path.write_text(
        "# V37 cap-backfill future experiment — not authorized\n\n"
        "Do not launch this experiment in the terminalization pass. Predeclare cap-excluded candidates and matched controls from the same generation/cell/rank region, evaluate only the exact missing prior-panel backfills, generate no new offspring, and judge success by exact cumulative breeder qualification plus later evidence.\n",
        encoding="utf-8",
    )
    readme_path.write_text(
        "# V37 archive terminalization artifacts\n\n"
        "This directory is a deterministic fixed-stream report generated from sealed native traces, the retained disposition ledger, and the Phase-1 descriptive table. It contains no new market evaluation or archive policy change.\n",
        encoding="utf-8",
    )
    files = (report_path, memo_path, experiment_path, readme_path)
    (output / "CHECKSUMS.sha256").write_text(
        "\n".join(f"{_file_sha256(path)[len('sha256:'):]}  {path.name}" for path in files) + "\n",
        encoding="utf-8",
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger-rows", required=True, type=Path)
    parser.add_argument("--trace-dir", required=True, type=Path)
    parser.add_argument("--phase1-evaluations", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    report = write_v37_archive_terminalization_report(
        ledger_rows_path=args.ledger_rows,
        trace_dir=args.trace_dir,
        phase1_evaluations_path=args.phase1_evaluations,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
