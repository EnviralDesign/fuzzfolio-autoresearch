"""Reproducible multi-objective quality-diversity evolution for temporal graphs.

The worker remains an immutable evaluator.  This controller owns QD archive
selection, family-first reproduction, random immigrants, native validation,
append-only proposal journaling, and exact restart from deterministic proposal
ordinals.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    TemporalDiscoveryGenerationExhausted,
    _clone,
    _get,
    _sha,
    canonical_sha256,
)
from .temporal_discovery_mutation import (
    _apply_option,
    _available_mutations,
    _public_mutation,
)
from .temporal_discovery_results import (
    _aggregate_candidate,
    _result_set_sha256,
    load_stage_results,
)
from .temporal_discovery_validation import SubprocessCandidateValidator
from .temporal_generator_v2_continuation import ExactGeneratorV2Continuation
from .temporal_operator_confirmed_entry import ConfirmedEntryStructuralOperator
from .temporal_operator_expansion import expanded_structural_operators
from .temporal_search_policy_v2 import inspect_management_reachability

QD_VERSION = "temporal_qd_evolution_v2"
QD_ARCHIVE_SCHEMA = "temporal_qd_archive_v2"
QD_CONFIG_SCHEMA = "temporal_qd_generation_config_v2"
QD_ENTRY_SCHEMA = "temporal_qd_proposal_entry_v2"
QD_CHECKPOINT_SCHEMA = "temporal_qd_generation_checkpoint_v2"
QD_POPULATION_SCHEMA = "temporal_qd_generation_population_v2"
QD_JOURNAL_SCHEMA = "temporal_qd_generation_journal_v2"
QD_MANIFEST_SCHEMA = "temporal_qd_generation_manifest_v2"

QD_OBJECTIVES = (
    ("riskAdjustedReturn", "max"),
    ("maximumDrawdownR", "min"),
    ("evidenceSupport", "max"),
    ("structuralComplexity", "min"),
)

PARAMETRIC_OPERATOR_VERSION = "temporal_parametric_mutation_operator_v1"
PARAMETRIC_FAMILY_IDS = {
    "entry_context": "parametric_entry_context_v1",
    "graph_structure": "parametric_graph_structure_v1",
    "management_closure": "parametric_management_closure_v1",
}
PARAMETRIC_OPERATOR_SPECS = {
    family_id: canonical_sha256(
        {
            "schemaVersion": "temporal_parametric_operator_spec_v1",
            "operatorId": family_id,
            "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
            "sourceFamily": source_family,
            "selectionPolicy": "uniform_occurrence_then_uniform_parameter_plan",
        }
    )
    for source_family, family_id in PARAMETRIC_FAMILY_IDS.items()
}

DEFAULT_QD_PARAMETERS: dict[str, Any] = {
    "version": QD_VERSION,
    "seed": 2026080101,
    "targetUniqueCandidates": 2500,
    "immigrantProposalFraction": 0.20,
    "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
    "maxCumulativeStructuralDepth": 16,
    "maxProposalAttempts": 20000,
    "minimumTotalTrades": 8,
    "minimumTradesPerWindow": 2,
    "cellCapacity": 8,
}


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


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


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(
                f"refusing to overwrite divergent immutable file: {path}"
            )
        return
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(encoded)
        temporary = Path(handle.name)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _replace(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(encoded)
        temporary = Path(handle.name)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _identity_payload(payload: Mapping[str, Any], field: str, *, name: str) -> str:
    current = _clone(payload, name=name)
    supplied = _sha(current.pop(field, None), name=f"{name} {field}")
    if canonical_sha256(current) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    return supplied


def _load_population(path: Path) -> tuple[list[dict[str, Any]], str]:
    payload = _read(path, name="QD source population")
    if "populationSha256" in payload:
        identity = _identity_payload(
            payload, "populationSha256", name="QD source population"
        )
        values = payload.get("candidates")
        expected = payload.get("candidateCount")
    elif "setSha256" in payload:
        identity = _identity_payload(payload, "setSha256", name="QD source set")
        values = payload.get("values")
        expected = payload.get("count")
    else:
        raise TemporalDiscoveryContractError(
            "QD source population lacks a canonical population identity"
        )
    if not isinstance(values, list) or int(expected or -1) != len(values):
        raise TemporalDiscoveryContractError("QD source population count mismatch")
    candidates = []
    for raw in values:
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError("QD candidate must be an object")
        candidate = _clone(raw, name="QD candidate")
        profile = candidate.get("sourceProfile")
        if not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("QD candidate profile is required")
        if canonical_sha256(profile) != _sha(
            candidate.get("sourceProfileSha256"),
            name="QD candidate source profile SHA-256",
        ):
            raise TemporalDiscoveryContractError(
                "QD candidate profile identity mismatch"
            )
        _sha(candidate.get("programSha256"), name="QD candidate program SHA-256")
        candidates.append(candidate)
    candidates.sort(key=lambda item: str(item["candidateId"]))
    if len({item["candidateId"] for item in candidates}) != len(candidates):
        raise TemporalDiscoveryContractError("QD candidate identities are not unique")
    return candidates, identity


def _finite(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric") from exc
    if not math.isfinite(result):
        raise TemporalDiscoveryContractError(f"{name} must be finite numeric")
    return result


def _bucket(value: float, bounds: Sequence[float], labels: Sequence[str]) -> str:
    for bound, label in zip(bounds, labels, strict=True):
        if value < bound:
            return label
    return labels[-1]


def _walk_guards(value: Any):
    if not isinstance(value, Mapping):
        return
    yield value
    child = value.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guards(child)
    for child in value.get("guards") or []:
        if isinstance(child, Mapping):
            yield from _walk_guards(child)


def _graph_structure(candidate: Mapping[str, Any]) -> dict[str, Any]:
    profile = candidate.get("sourceProfile") or {}
    graph = profile.get("graph") or {}
    transitions = list(graph.get("transitions") or [])
    history = list(candidate.get("structuralOperatorHistory") or [])
    operator_ids = sorted(
        {
            str(item.get("operatorId"))
            for item in history
            if isinstance(item, Mapping) and item.get("operatorId")
        }
    )
    entry_event_ids: set[str] = set()
    management_action_count = 0
    guard_count = 0
    for transition in transitions:
        if not isinstance(transition, Mapping):
            continue
        actions = transition.get("actions") or []
        is_entry = any(
            isinstance(action, Mapping) and action.get("kind") == "enter_next_open"
            for action in actions
        )
        management_action_count += sum(
            isinstance(action, Mapping)
            and action.get("kind")
            in {
                "exit_next_open",
                "move_stop_to_break_even_next_open",
                "move_stop_next_open",
            }
            for action in actions
        )
        for guard in _walk_guards(transition.get("guard")):
            guard_count += 1
            if is_entry and guard.get("kind") in {
                "fresh_event",
                "event_age_at_most",
                "event_age_window",
            }:
                event_id = str(guard.get("eventId") or "")
                if event_id:
                    entry_event_ids.add(event_id)
    state_count = len(graph.get("states") or [])
    transition_count = len(transitions)
    indicator_count = len(profile.get("indicators") or [])
    return {
        "operatorFamilyIds": operator_ids,
        "operatorFamilyCount": len(operator_ids),
        "mutationDepth": len(history),
        "entryEventCount": len(entry_event_ids),
        "managementActionCount": int(management_action_count),
        "stateCount": state_count,
        "transitionCount": transition_count,
        "graphNodeCount": state_count + transition_count,
        "guardCount": guard_count,
        "indicatorCount": indicator_count,
        "structuralComplexity": (
            state_count + transition_count + 0.25 * guard_count + 0.5 * indicator_count
        ),
    }


def qd_behavior_descriptor(
    candidate: Mapping[str, Any], aggregate: Mapping[str, Any]
) -> dict[str, Any]:
    trades = int(aggregate.get("totalTrades") or 0)
    trade_frequency = _finite(
        aggregate.get("entryFrequencyPerThousand", 0.0),
        name="trade frequency",
    )
    holding = _finite(aggregate.get("medianHoldingBars", 0.0), name="holding")
    structure = _graph_structure(candidate)
    descriptor = {
        "operatorFamilies": _bucket(
            structure["operatorFamilyCount"],
            (1, 2, 3, math.inf),
            ("none", "one", "two", "three_plus"),
        ),
        "mutationDepth": _bucket(
            structure["mutationDepth"],
            (1, 2, 3, math.inf),
            ("root", "one", "two", "three_plus"),
        ),
        "entryEvents": _bucket(
            structure["entryEventCount"],
            (1, 2, math.inf),
            ("none", "one", "two_plus"),
        ),
        "managementActions": _bucket(
            structure["managementActionCount"],
            (1, 2, 3, math.inf),
            ("none", "one", "two", "three_plus"),
        ),
        "graphNodes": _bucket(
            structure["graphNodeCount"],
            (9, 13, 19, math.inf),
            ("small", "medium", "large", "very_large"),
        ),
        "tradeFrequency": (
            "dormant"
            if trades == 0
            else _bucket(
                trade_frequency,
                (1.0, 4.0, 12.0, math.inf),
                ("very_sparse", "sparse", "moderate", "active"),
            )
        ),
        "medianHolding": (
            "none"
            if trades == 0
            else _bucket(
                holding,
                (24.0, 96.0, 384.0, 1536.0, math.inf),
                ("short", "medium", "long", "very_long", "extreme"),
            )
        ),
    }
    descriptor["structuralMeasurements"] = structure
    descriptor["cellId"] = "|".join(
        str(descriptor[key])
        for key in (
            "operatorFamilies",
            "mutationDepth",
            "entryEvents",
            "managementActions",
            "graphNodes",
            "tradeFrequency",
            "medianHolding",
        )
    )
    return descriptor


def _objective_row(
    candidate: Mapping[str, Any], aggregate: Mapping[str, Any]
) -> dict[str, float]:
    trades = int(aggregate.get("totalTrades") or 0)
    drawdown = max(
        0.0,
        _finite(aggregate.get("maxWindowDrawdownR"), name="drawdown R"),
    )
    total_return = _finite(aggregate.get("totalConservativeNetR"), name="total net R")
    return {
        "riskAdjustedReturn": total_return / max(1.0, drawdown),
        "maximumDrawdownR": drawdown,
        "evidenceSupport": float(trades),
        "structuralComplexity": float(
            _graph_structure(candidate)["structuralComplexity"]
        ),
    }


def _finite_data_validity(
    aggregate: Mapping[str, Any],
    *,
    minimum_total_trades: int,
    minimum_trades_per_window: int,
) -> dict[str, Any]:
    counts = [int(value) for value in aggregate.get("tradeCountsByWindow") or []]
    total = int(aggregate.get("totalTrades") or 0)
    checks = {
        "minimumTotalTrades": total >= minimum_total_trades,
        "minimumTradesEveryWindow": bool(counts)
        and all(value >= minimum_trades_per_window for value in counts),
        "positiveObservationSupport": int(aggregate.get("totalObservations") or 0) > 0,
    }
    return {
        "minimumTotalTrades": minimum_total_trades,
        "minimumTradesPerWindow": minimum_trades_per_window,
        "tradeCountsByWindow": counts,
        "totalTrades": total,
        "checks": checks,
        "validForPareto": all(checks.values()),
    }


def _dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    no_worse = True
    better = False
    for key, direction in QD_OBJECTIVES:
        lvalue = float(left["objectives"][key])
        rvalue = float(right["objectives"][key])
        if direction == "max":
            no_worse = no_worse and lvalue >= rvalue
            better = better or lvalue > rvalue
        else:
            no_worse = no_worse and lvalue <= rvalue
            better = better or lvalue < rvalue
    return no_worse and better


def _pareto_fronts(rows: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    remaining = [_clone(item, name="QD archive member") for item in rows]
    remaining.sort(key=lambda item: str(item["candidateId"]))
    fronts = []
    while remaining:
        front = [
            row
            for row in remaining
            if not any(
                other["candidateId"] != row["candidateId"] and _dominates(other, row)
                for other in remaining
            )
        ]
        front.sort(key=lambda item: str(item["candidateId"]))
        fronts.append(front)
        selected = {item["candidateId"] for item in front}
        remaining = [item for item in remaining if item["candidateId"] not in selected]
    return fronts


def _crowding_order(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) <= 2:
        return sorted(rows, key=lambda item: str(item["candidateId"]))
    distances = {str(item["candidateId"]): 0.0 for item in rows}
    for key, direction in QD_OBJECTIVES:
        ordered = sorted(
            rows,
            key=lambda item: (
                float(item["objectives"][key]),
                str(item["candidateId"]),
            ),
        )
        distances[str(ordered[0]["candidateId"])] = math.inf
        distances[str(ordered[-1]["candidateId"])] = math.inf
        low = float(ordered[0]["objectives"][key])
        high = float(ordered[-1]["objectives"][key])
        scale = high - low
        if scale <= 0:
            continue
        for index in range(1, len(ordered) - 1):
            candidate_id = str(ordered[index]["candidateId"])
            if math.isinf(distances[candidate_id]):
                continue
            before = float(ordered[index - 1]["objectives"][key])
            after = float(ordered[index + 1]["objectives"][key])
            distances[candidate_id] += abs(after - before) / scale
    return sorted(
        (_clone(item, name="crowding member") for item in rows),
        key=lambda item: (
            -distances[str(item["candidateId"])],
            str(item["candidateId"]),
        ),
    )


def select_qd_archive(
    members: Sequence[Mapping[str, Any]], *, cell_capacity: int = 8
) -> list[dict[str, Any]]:
    if not 1 <= cell_capacity <= 32:
        raise TemporalDiscoveryContractError("QD cell capacity must be 1..32")
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        groups[str(member["descriptor"]["cellId"])].append(member)
    cells = []
    for cell_id in sorted(groups):
        selected: list[dict[str, Any]] = []
        eligible = [
            row
            for row in groups[cell_id]
            if row.get("finiteDataValidity", {}).get("validForPareto") is True
        ]
        if eligible:
            fronts = _pareto_fronts(eligible)
            for front_index, front in enumerate(fronts):
                ranked = _crowding_order(front)
                remaining = cell_capacity - len(selected)
                for row in ranked[:remaining]:
                    row["paretoFront"] = front_index
                    row["retentionReason"] = "finite_data_pareto"
                    selected.append(row)
                if len(selected) == cell_capacity:
                    break
        else:
            fallback = sorted(
                groups[cell_id],
                key=lambda row: (
                    -float(row["objectives"]["evidenceSupport"]),
                    float(row["objectives"]["structuralComplexity"]),
                    str(row["candidateId"]),
                ),
            )[: min(2, cell_capacity)]
            for row in fallback:
                row = _clone(row, name="finite-data fallback")
                row["paretoFront"] = None
                row["retentionReason"] = "finite_data_fallback"
                selected.append(row)
        cells.append(
            {
                "cellId": cell_id,
                "descriptor": _clone(selected[0]["descriptor"], name="QD descriptor"),
                "candidateCountBeforeCapacity": len(groups[cell_id]),
                "finiteDataEligibleCountBeforeCapacity": len(eligible),
                "members": sorted(selected, key=lambda item: str(item["candidateId"])),
            }
        )
    return cells


def build_qd_archive(
    *,
    population_path: Path | str,
    result_root: Path | str,
    output_path: Path | str,
    generation_index: int,
    previous_archive_path: Path | str | None = None,
    generation_journal_path: Path | str | None = None,
    cell_capacity: int = 8,
    minimum_total_trades: int = 8,
    minimum_trades_per_window: int = 2,
) -> dict[str, Any]:
    if generation_index < 0:
        raise TemporalDiscoveryContractError("QD generation index must be nonnegative")
    candidates, population_sha = _load_population(Path(population_path))
    candidate_map = {item["candidateId"]: item for item in candidates}
    results = load_stage_results(result_root)
    if set(results) != set(candidate_map):
        raise TemporalDiscoveryContractError(
            "QD result set must exactly cover the source population"
        )
    members: dict[str, dict[str, Any]] = {}
    previous_sha = None
    previous_cell_ids: set[str] = set()
    prior_cell_accounting: dict[str, dict[str, int]] = {}
    prior_candidate_count_seen = 0
    previous_member_ids: set[str] = set()
    if previous_archive_path is not None:
        previous = _read(Path(previous_archive_path), name="previous QD archive")
        previous_sha = _identity_payload(
            previous, "archiveSha256", name="previous QD archive"
        )
        prior_candidate_count_seen = int(previous.get("candidateCountSeen") or 0)
        for cell in previous.get("cells") or []:
            cell_id = str(cell["cellId"])
            previous_cell_ids.add(cell_id)
            prior_cell_accounting[cell_id] = {
                "selectionVisitCount": int(cell.get("selectionVisitCount") or 0),
                "offspringAttemptCount": int(cell.get("offspringAttemptCount") or 0),
            }
            for member in cell.get("members") or []:
                previous_member_ids.add(str(member["candidateId"]))
                members[str(member["candidateId"])] = _clone(
                    member, name="previous QD member"
                )
    for candidate_id in sorted(candidate_map):
        candidate = candidate_map[candidate_id]
        aggregate = _aggregate_candidate(candidate, results[candidate_id])
        descriptor = qd_behavior_descriptor(candidate, aggregate)
        members[candidate_id] = {
            "candidateId": candidate_id,
            "generationIndex": generation_index,
            "candidate": candidate,
            "aggregate": aggregate,
            "descriptor": descriptor,
            "objectives": _objective_row(candidate, aggregate),
            "finiteDataValidity": _finite_data_validity(
                aggregate,
                minimum_total_trades=minimum_total_trades,
                minimum_trades_per_window=minimum_trades_per_window,
            ),
        }
    cells = select_qd_archive(list(members.values()), cell_capacity=cell_capacity)
    generation_accounting: dict[str, Any] = {}
    if generation_journal_path is not None:
        journal = _read(Path(generation_journal_path), name="QD generation journal")
        _identity_payload(journal, "journalSha256", name="QD generation journal")
        generation_accounting = {
            key: _clone(journal.get(key) or {}, name=f"QD journal {key}")
            for key in (
                "originProposalCounts",
                "originAcceptedCounts",
                "parentSelectionModeCounts",
                "parentCellSelectionCounts",
                "parentCellOffspringAttemptCounts",
                "operatorFamilyAttemptCounts",
                "operatorFamilyApplicationCounts",
                "mutationDepthAttemptCounts",
                "dispositionCounts",
            )
        }
    new_cell_counts = generation_accounting.get("parentCellSelectionCounts") or {}
    new_attempt_counts = (
        generation_accounting.get("parentCellOffspringAttemptCounts") or {}
    )
    for cell in cells:
        cell_id = str(cell["cellId"])
        previous_counts = prior_cell_accounting.get(cell_id, {})
        cell["selectionVisitCount"] = int(
            previous_counts.get("selectionVisitCount", 0)
        ) + int(new_cell_counts.get(cell_id, 0))
        cell["offspringAttemptCount"] = int(
            previous_counts.get("offspringAttemptCount", 0)
        ) + int(new_attempt_counts.get(cell_id, 0))
    selected_member_ids = {
        str(member["candidateId"]) for cell in cells for member in cell["members"]
    }
    admitted_ids = selected_member_ids - previous_member_ids
    evicted_ids = previous_member_ids - selected_member_ids
    family_survivors: Counter[str] = Counter()
    for cell in cells:
        for member in cell["members"]:
            if str(member["candidateId"]) not in admitted_ids:
                continue
            history = member["candidate"].get("structuralOperatorHistory") or []
            for operator_id in {
                str(item.get("operatorId"))
                for item in history
                if isinstance(item, Mapping) and item.get("operatorId")
            }:
                family_survivors[operator_id] += 1
    archive = {
        "schemaVersion": QD_ARCHIVE_SCHEMA,
        "qdVersion": QD_VERSION,
        "generationIndex": generation_index,
        "populationSha256": population_sha,
        "resultSetSha256": _result_set_sha256(results),
        "previousArchiveSha256": previous_sha,
        "cellCapacity": cell_capacity,
        "finiteDataPolicy": {
            "minimumTotalTrades": minimum_total_trades,
            "minimumTradesPerWindow": minimum_trades_per_window,
            "invalidCandidatesParticipateInPareto": False,
        },
        "objectives": [
            {"name": name, "direction": direction} for name, direction in QD_OBJECTIVES
        ],
        "candidateCountSeen": prior_candidate_count_seen + len(candidate_map),
        "candidateCountReducedThisGeneration": len(candidate_map),
        "occupiedCellCount": len(cells),
        "newCellCount": len(
            {str(cell["cellId"]) for cell in cells} - previous_cell_ids
        ),
        "memberCount": sum(len(cell["members"]) for cell in cells),
        "paretoEligibleMemberCount": sum(
            member["finiteDataValidity"]["validForPareto"] is True
            for cell in cells
            for member in cell["members"]
        ),
        "paretoAdmissionCount": len(admitted_ids),
        "paretoEvictionCount": len(evicted_ids),
        "survivingDescendantsByOperatorFamily": dict(sorted(family_survivors.items())),
        "generationProposalAccounting": generation_accounting,
        "cells": cells,
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    _write_once(Path(output_path), archive)
    return {
        "schemaVersion": "temporal_qd_archive_result_v1",
        "archiveSha256": archive["archiveSha256"],
        "candidateCountSeen": archive["candidateCountSeen"],
        "occupiedCellCount": archive["occupiedCellCount"],
        "memberCount": archive["memberCount"],
        "paretoEligibleMemberCount": archive["paretoEligibleMemberCount"],
        "newCellCount": archive["newCellCount"],
        "paretoAdmissionCount": archive["paretoAdmissionCount"],
        "paretoEvictionCount": archive["paretoEvictionCount"],
    }


def _normalize_parameters(parameters: Mapping[str, Any] | None) -> dict[str, Any]:
    value = _clone(parameters or DEFAULT_QD_PARAMETERS, name="QD parameters")
    required = {
        "version",
        "seed",
        "targetUniqueCandidates",
        "immigrantProposalFraction",
        "mutationDepthProbabilities",
        "maxCumulativeStructuralDepth",
        "maxProposalAttempts",
        "minimumTotalTrades",
        "minimumTradesPerWindow",
        "cellCapacity",
    }
    if set(value) != required or value["version"] != QD_VERSION:
        raise TemporalDiscoveryContractError("QD parameters have an unknown schema")
    value["seed"] = int(value["seed"])
    value["targetUniqueCandidates"] = int(value["targetUniqueCandidates"])
    value["immigrantProposalFraction"] = _finite(
        value["immigrantProposalFraction"], name="immigrant proposal fraction"
    )
    value["maxCumulativeStructuralDepth"] = int(value["maxCumulativeStructuralDepth"])
    value["maxProposalAttempts"] = int(value["maxProposalAttempts"])
    value["minimumTotalTrades"] = int(value["minimumTotalTrades"])
    value["minimumTradesPerWindow"] = int(value["minimumTradesPerWindow"])
    value["cellCapacity"] = int(value["cellCapacity"])
    probabilities = value["mutationDepthProbabilities"]
    if not isinstance(probabilities, Mapping) or set(probabilities) != {"1", "2", "3"}:
        raise TemporalDiscoveryContractError(
            "QD mutation depth probabilities require depths 1, 2, and 3"
        )
    value["mutationDepthProbabilities"] = {
        key: _finite(probabilities[key], name=f"mutation depth {key} probability")
        for key in ("1", "2", "3")
    }
    if not 1 <= value["targetUniqueCandidates"] <= 100_000:
        raise TemporalDiscoveryContractError("QD target is outside 1..100000")
    if value["immigrantProposalFraction"] != 0.20:
        raise TemporalDiscoveryContractError(
            "the initial QD immigrant proposal fraction is frozen at 0.20"
        )
    if abs(sum(value["mutationDepthProbabilities"].values()) - 1.0) > 1e-12 or value[
        "mutationDepthProbabilities"
    ] != {"1": 0.70, "2": 0.25, "3": 0.05}:
        raise TemporalDiscoveryContractError(
            "the initial QD mutation depth distribution is frozen at 70/25/5"
        )
    if not 3 <= value["maxCumulativeStructuralDepth"] <= 32:
        raise TemporalDiscoveryContractError(
            "maximum cumulative structural depth is outside 3..32"
        )
    if value["maxProposalAttempts"] < value["targetUniqueCandidates"]:
        raise TemporalDiscoveryContractError("QD proposal ceiling is below its target")
    if value["minimumTotalTrades"] < 1 or value["minimumTradesPerWindow"] < 1:
        raise TemporalDiscoveryContractError("QD finite-data floors must be positive")
    if not 1 <= value["cellCapacity"] <= 32:
        raise TemporalDiscoveryContractError("QD cell capacity is outside 1..32")
    return value


def _operators() -> dict[str, Any]:
    values = [ConfirmedEntryStructuralOperator(), *expanded_structural_operators()]
    return {item.operator_id: item for item in values}


def _load_archive(path: Path) -> tuple[dict[str, Any], str]:
    archive = _read(path, name="QD parent archive")
    archive_sha = _identity_payload(archive, "archiveSha256", name="QD parent archive")
    archive["archiveSha256"] = archive_sha
    if archive.get("schemaVersion") != QD_ARCHIVE_SCHEMA:
        raise TemporalDiscoveryContractError("unknown QD archive schema")
    return archive, archive_sha


def _reproduction_cells(archive: Mapping[str, Any]) -> list[dict[str, Any]]:
    eligible = []
    all_cells = []
    for cell in archive.get("cells") or []:
        members = list(cell.get("members") or [])
        if not members:
            continue
        all_cells.append(_clone(cell, name="QD reproduction cell"))
        if any(
            member.get("finiteDataValidity", {}).get("validForPareto")
            for member in members
        ):
            filtered = _clone(cell, name="eligible QD reproduction cell")
            filtered["members"] = [
                member
                for member in filtered["members"]
                if member.get("finiteDataValidity", {}).get("validForPareto") is True
            ]
            eligible.append(filtered)
    cells = eligible or all_cells
    if not cells:
        raise TemporalDiscoveryContractError("QD archive has no reproduction members")
    return sorted(cells, key=lambda item: str(item["cellId"]))


def _proposal_seed(config_sha: str, generation_index: int, ordinal: int) -> str:
    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_proposal_seed_v1",
            "configSha256": config_sha,
            "generationIndex": generation_index,
            "proposalOrdinal": ordinal,
        }
    )


def _proposal_rng(seed_sha: str) -> random.Random:
    return random.Random(int(seed_sha.removeprefix("sha256:"), 16))


def _origin_kind(proposal_ordinal: int) -> str:
    # One exact immigrant slot in every five proposals.  Accepted composition is
    # intentionally not quota-forced; both raw and accepted counts are reported.
    return "random_immigrant" if proposal_ordinal % 5 == 4 else "structural_offspring"


def _descriptor_coordinates(cell: Mapping[str, Any]) -> tuple[str, ...]:
    descriptor = cell.get("descriptor") or {}
    return tuple(
        str(descriptor.get(key))
        for key in (
            "operatorFamilies",
            "mutationDepth",
            "entryEvents",
            "managementActions",
            "graphNodes",
            "tradeFrequency",
            "medianHolding",
        )
    )


def _boundary_cell_ids(cells: Sequence[Mapping[str, Any]]) -> set[str]:
    coordinates = {str(cell["cellId"]): _descriptor_coordinates(cell) for cell in cells}
    neighbor_counts: dict[str, int] = {}
    for cell_id, left in coordinates.items():
        neighbor_counts[cell_id] = sum(
            sum(a != b for a, b in zip(left, right, strict=True)) == 1
            for other_id, right in coordinates.items()
            if other_id != cell_id
        )
    minimum = min(neighbor_counts.values(), default=0)
    return {key for key, value in neighbor_counts.items() if value == minimum}


def _initial_selection_state(
    cells: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    return {
        str(cell["cellId"]): {
            "selectionVisitCount": int(cell.get("selectionVisitCount") or 0),
            "offspringAttemptCount": int(cell.get("offspringAttemptCount") or 0),
        }
        for cell in cells
    }


def _select_parent(
    *,
    rng: random.Random,
    cells: Sequence[Mapping[str, Any]],
    selection_state: dict[str, dict[str, int]],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    draw = rng.random()
    if draw < 0.50:
        mode = "uniform_occupied_cell"
        pool = list(cells)
    elif draw < 0.80:
        mode = "low_visit_cell"
        minimum = min(
            selection_state[str(cell["cellId"])]["selectionVisitCount"]
            for cell in cells
        )
        pool = [
            cell
            for cell in cells
            if selection_state[str(cell["cellId"])]["selectionVisitCount"] == minimum
        ]
    else:
        mode = "sparse_descriptor_boundary"
        boundary = _boundary_cell_ids(cells)
        pool = [cell for cell in cells if str(cell["cellId"]) in boundary]
    cell = pool[rng.randrange(len(pool))]
    cell_id = str(cell["cellId"])
    members = sorted(cell["members"], key=lambda item: str(item["candidateId"]))
    member = members[rng.randrange(len(members))]
    selection_state[cell_id]["selectionVisitCount"] += 1
    selection_state[cell_id]["offspringAttemptCount"] += 1
    return cell, member, mode


def _mutation_depth(rng: random.Random) -> int:
    draw = rng.random()
    if draw < 0.70:
        return 1
    if draw < 0.95:
        return 2
    return 3


def _plan_occurrence_key(plan: Mapping[str, Any]) -> str:
    occurrence = {
        "operatorId": plan.get("operatorId"),
        "targetTransitionId": plan.get("targetTransitionId"),
        "targetGuardPath": plan.get("targetGuardPath"),
        "sourceGuardSha256": plan.get("sourceGuardSha256"),
        "confirmationClauseIndex": plan.get("confirmationClauseIndex"),
        "setupAnchorPath": plan.get("setupAnchorPath"),
        "setupClauseIndex": plan.get("setupClauseIndex"),
        "setupOccurrenceSha256": plan.get("setupOccurrenceSha256"),
    }
    return canonical_sha256(occurrence)


def _parametric_occurrence_key(option: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            "family": option.get("family"),
            "operator": option.get("operator"),
            "path": option.get("path"),
        }
    )


def _validation_record(validation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidateAcceptable": validation.get("candidateAcceptable"),
        "status": validation.get("status"),
        "programSha256": validation.get("programSha256"),
        "profileSnapshotSha256": validation.get("profileSnapshotSha256"),
        "validationReportSha256": validation.get("validationReportSha256"),
        "issueCodes": sorted(
            str(item.get("code"))
            for item in validation.get("issues") or []
            if isinstance(item, Mapping) and item.get("code")
        ),
    }


def _root_candidate_identity(candidate: Mapping[str, Any]) -> str:
    supplied = candidate.get("candidateIdentitySha256")
    if supplied is not None:
        return _sha(supplied, name="parent candidate identity")
    return canonical_sha256(
        {
            "schemaVersion": "temporal_qd_imported_root_identity_v1",
            "canonicalGraphSha256": canonical_sha256(
                candidate["sourceProfile"]["graph"]
            ),
            "sourceProfileSha256": candidate["sourceProfileSha256"],
            "programSha256": candidate["programSha256"],
        }
    )


def _candidate_identity(
    *,
    origin_kind: str,
    parent: Mapping[str, Any] | None,
    profile: Mapping[str, Any],
    ordered_lineage: Sequence[Mapping[str, Any]],
    origin_contract: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str]:
    graph_sha = canonical_sha256(profile["graph"])
    material = {
        "schemaVersion": "temporal_qd_candidate_identity_v1",
        "qdEngineVersion": QD_VERSION,
        "originKind": origin_kind,
        "canonicalParentGraphSha256": (
            canonical_sha256(parent["sourceProfile"]["graph"])
            if parent is not None
            else graph_sha
        ),
        "parentCandidateIdentitySha256": (
            _root_candidate_identity(parent) if parent is not None else None
        ),
        "completeOrderedMutationLineage": _clone(
            list(ordered_lineage), name="candidate ordered mutation lineage"
        ),
        "originContract": _clone(origin_contract, name="candidate origin contract"),
        "finalCanonicalGraphSha256": graph_sha,
        "finalSourceProfileSha256": canonical_sha256(profile),
    }
    identity_sha = canonical_sha256(material)
    return material, identity_sha, "qd_" + identity_sha.removeprefix("sha256:")[:28]


def _structural_proposal(
    *,
    rng: random.Random,
    cells: Sequence[Mapping[str, Any]],
    operators: Mapping[str, Any],
    max_depth: int,
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]],
    selection_state: dict[str, dict[str, int]],
    validator: Any | None,
    replay_steps: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    cell, parent_member, selection_mode = _select_parent(
        rng=rng, cells=cells, selection_state=selection_state
    )
    parent = parent_member["candidate"]
    parent_id = str(parent["candidateId"])
    history = _clone(
        parent.get("structuralOperatorHistory") or [],
        name="parent structural operator history",
    )
    depth = len(history)
    desired_depth = _mutation_depth(rng)
    base = {
        "originKind": "structural_offspring",
        "parentCellId": cell["cellId"],
        "parentCandidateId": parent_id,
        "parentProgramSha256": parent["programSha256"],
        "parentStructuralDepth": depth,
        "parentSelectionMode": selection_mode,
        "desiredMutationDepth": desired_depth,
        "parent": parent,
    }
    if depth + desired_depth > max_depth:
        return None, {**base, "proposalIssue": "maximum_structural_depth_reached"}
    current_profile = _clone(parent["sourceProfile"], name="structural parent profile")
    current_program = _sha(parent["programSha256"], name="structural parent program")
    steps: list[dict[str, Any]] = []
    for step_index in range(desired_depth):
        eligible: list[tuple[str, str, list[dict[str, Any]]]] = []
        profile_sha = canonical_sha256(current_profile)
        for operator_id, operator in sorted(operators.items()):
            key = (profile_sha, operator_id)
            if key not in plan_cache:
                plan_cache[key] = operator.enumerate_plans(current_profile)
            if plan_cache[key]:
                eligible.append((operator_id, "structural", plan_cache[key]))
        for source_family, options in sorted(
            _available_mutations(current_profile).items()
        ):
            if options:
                eligible.append(
                    (PARAMETRIC_FAMILY_IDS[source_family], "parametric", options)
                )
        if not eligible:
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "no_eligible_operator_family",
            }
        operator_id, operator_kind, plans = eligible[rng.randrange(len(eligible))]
        occurrence_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for raw_plan in plans:
            occurrence_key = (
                _plan_occurrence_key(raw_plan)
                if operator_kind == "structural"
                else _parametric_occurrence_key(raw_plan)
            )
            occurrence_groups[occurrence_key].append(raw_plan)
        occurrence_ids = sorted(occurrence_groups)
        occurrence_id = occurrence_ids[rng.randrange(len(occurrence_ids))]
        occurrence_plans = sorted(
            occurrence_groups[occurrence_id],
            key=lambda item: canonical_sha256(
                {key: value for key, value in item.items() if key != "_path"}
            ),
        )
        selected_plan = occurrence_plans[rng.randrange(len(occurrence_plans))]
        if operator_kind == "structural":
            operator = operators[operator_id]
            operator_version = operator.operator_version
            operator_spec_sha = operator.specification["operatorSpecSha256"]
            plan = _clone(selected_plan, name="QD structural plan")
            child = operator.preview(current_profile, plan)
        else:
            try:
                old_value = _get(current_profile, selected_plan["_path"])
            except (KeyError, IndexError):
                old_value = {"__absent__": True}
            public_mutation = _public_mutation(selected_plan, old_value)
            plan = {
                "schemaVersion": "temporal_parametric_operator_plan_v1",
                "operatorId": operator_id,
                "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
                "operatorSpecSha256": PARAMETRIC_OPERATOR_SPECS[operator_id],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "mutation": public_mutation,
            }
            plan["planSha256"] = canonical_sha256(plan)
            operator_version = PARAMETRIC_OPERATOR_VERSION
            operator_spec_sha = PARAMETRIC_OPERATOR_SPECS[operator_id]
            child = _apply_option(current_profile, selected_plan)
        reachability = inspect_management_reachability(child)
        step: dict[str, Any] = {
            "stepIndex": step_index,
            "operatorId": operator_id,
            "operatorKind": operator_kind,
            "operatorVersion": operator_version,
            "operatorSpecSha256": operator_spec_sha,
            "eligibleOperatorFamilyCount": len(eligible),
            "eligibleOperatorFamilyIds": [item[0] for item in eligible],
            "familyOccurrenceCount": len(occurrence_ids),
            "selectedOccurrenceSha256": occurrence_id,
            "occurrenceParameterPlanCount": len(occurrence_plans),
            "plan": _clone(plan, name="QD mutation plan"),
            "planSha256": plan["planSha256"],
            "parentSourceProfileSha256": canonical_sha256(current_profile),
            "childSourceProfileSha256": canonical_sha256(child),
            "managementReachabilitySha256": reachability["reachabilitySha256"],
            "managementReachabilityIssueCounts": reachability["issueCounts"],
        }
        if reachability["acceptable"] is not True:
            step["disposition"] = "static_reachability_rejected"
            steps.append(step)
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "intermediate_static_reachability_rejected",
            }
        if replay_steps is not None:
            if step_index >= len(replay_steps):
                raise TemporalDiscoveryContractError(
                    "QD replay lacks an intermediate step"
                )
            validation_record = replay_steps[step_index].get("validation")
            if not isinstance(validation_record, Mapping):
                raise TemporalDiscoveryContractError(
                    "QD replay intermediate validation is missing"
                )
            validation_record = _clone(
                validation_record, name="replayed intermediate validation"
            )
        else:
            if validator is None:
                raise TemporalDiscoveryContractError(
                    "QD structural validator is missing"
                )
            validation = validator.validate(
                candidate_id=(
                    "qd_intermediate_"
                    + step["childSourceProfileSha256"].removeprefix("sha256:")[:24]
                ),
                source_profile=child,
                expected_raw_source_profile_sha256=step["childSourceProfileSha256"],
            )
            validation_record = _validation_record(validation)
        step["validation"] = validation_record
        if validation_record.get("candidateAcceptable") is not True:
            step["disposition"] = "native_validator_rejected"
            steps.append(step)
            return None, {
                **base,
                "steps": steps,
                "proposalIssue": "intermediate_native_validator_rejected",
            }
        child_program = _sha(
            validation_record.get("programSha256"), name="intermediate program SHA-256"
        )
        if operator_kind == "structural":
            rebound, application = operator.apply(
                current_profile,
                plan,
                parent_validated_program_sha256=current_program,
                child_validated_program_sha256=child_program,
            )
            audit = operator.audit(current_profile, child, application)
            if (
                rebound != child
                or application["staticInvariantReport"]["allChecksPassed"] is not True
                or audit["allChecksPassed"] is not True
            ):
                raise TemporalDiscoveryContractError(
                    "QD intermediate structural audit failed"
                )
        else:
            rebound = _apply_option(current_profile, selected_plan)
            application = {
                "schemaVersion": "temporal_parametric_operator_application_v1",
                "operatorId": operator_id,
                "operatorVersion": operator_version,
                "operatorSpecSha256": operator_spec_sha,
                "plan": plan,
                "planSha256": plan["planSha256"],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "childSourceProfileSha256": canonical_sha256(child),
                "parentProgramSha256": current_program,
                "childProgramSha256": child_program,
            }
            application["applicationSha256"] = canonical_sha256(application)
            audit = {
                "schemaVersion": "temporal_parametric_operator_audit_v1",
                "operatorId": operator_id,
                "planSha256": plan["planSha256"],
                "checks": {
                    "single_exact_option_applied": rebound == child,
                    "source_profile_changed": current_profile != child,
                },
                "allChecksPassed": rebound == child and current_profile != child,
            }
            audit["auditSha256"] = canonical_sha256(audit)
            if audit["allChecksPassed"] is not True:
                raise TemporalDiscoveryContractError(
                    "QD parametric mutation audit failed"
                )
        step["applicationSha256"] = application["applicationSha256"]
        step["auditSha256"] = audit["auditSha256"]
        step["disposition"] = "applied"
        steps.append(step)
        history.append(
            {
                "operatorId": operator_id,
                "operatorKind": operator_kind,
                "operatorVersion": operator_version,
                "operatorSpecSha256": operator_spec_sha,
                "plan": _clone(plan, name="mutation history plan"),
                "planSha256": plan["planSha256"],
                "applicationSha256": application["applicationSha256"],
                "parentSourceProfileSha256": canonical_sha256(current_profile),
                "childSourceProfileSha256": canonical_sha256(child),
                "parentProgramSha256": current_program,
                "childProgramSha256": child_program,
            }
        )
        current_profile = child
        current_program = child_program
    if replay_steps is not None and len(replay_steps) != len(steps):
        raise TemporalDiscoveryContractError("QD replay has extra intermediate steps")
    identity_material, identity_sha, candidate_id = _candidate_identity(
        origin_kind="structural_offspring",
        parent=parent,
        profile=current_profile,
        ordered_lineage=history,
        origin_contract={
            "operatorRegistry": [
                {
                    "operatorId": step["operatorId"],
                    "operatorVersion": step["operatorVersion"],
                    "operatorSpecSha256": step["operatorSpecSha256"],
                }
                for step in steps
            ]
        },
    )
    return current_profile, {
        **base,
        "steps": steps,
        "completeStructuralOperatorHistory": history,
        "finalValidation": steps[-1]["validation"],
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "candidateId": candidate_id,
    }


def _immigrant_proposal(
    *,
    immigrant_source: ExactGeneratorV2Continuation,
) -> tuple[dict[str, Any], dict[str, Any]]:
    proposal = immigrant_source.next_proposal()
    profile = proposal["rawSourceProfile"]
    lineage = [
        {
            "kind": "generator_v2_mutation",
            "payload": item,
        }
        for item in proposal["mutations"]
    ] + [
        {
            "kind": "generator_v2_activation_aware_repair",
            "payload": item,
        }
        for item in proposal["activationAwareRepairs"]
    ]
    identity_material, identity_sha, candidate_id = _candidate_identity(
        origin_kind="random_immigrant",
        parent=None,
        profile=profile,
        ordered_lineage=lineage,
        origin_contract={
            "generatorSourceIdentitySha256": proposal["sourceIdentitySha256"],
            "immigrantProposalSchema": proposal["schemaVersion"],
        },
    )
    return profile, {
        "originKind": "random_immigrant",
        "immigrantProposal": proposal,
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "candidateId": candidate_id,
    }


def _proposal(
    *,
    config_sha: str,
    generation_index: int,
    proposal_ordinal: int,
    cells: Sequence[Mapping[str, Any]],
    immigrant_source: ExactGeneratorV2Continuation,
    operators: Mapping[str, Any],
    parameters: Mapping[str, Any],
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]],
    selection_state: dict[str, dict[str, int]],
    validator: Any | None,
    replay_steps: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    seed_sha = _proposal_seed(config_sha, generation_index, proposal_ordinal)
    rng = _proposal_rng(seed_sha)
    origin = _origin_kind(proposal_ordinal)
    if origin == "random_immigrant":
        profile, metadata = _immigrant_proposal(
            immigrant_source=immigrant_source,
        )
    else:
        profile, metadata = _structural_proposal(
            rng=rng,
            cells=cells,
            operators=operators,
            max_depth=int(parameters["maxCumulativeStructuralDepth"]),
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=validator,
            replay_steps=replay_steps,
        )
    public_metadata = {
        key: _clone(value, name=f"proposal metadata {key}")
        for key, value in metadata.items()
        if key not in {"parent"}
    }
    material = {
        "proposalSeedSha256": seed_sha,
        "generationIndex": generation_index,
        "proposalOrdinal": proposal_ordinal,
        "originKind": origin,
        **public_metadata,
        "rawSourceProfileSha256": (
            canonical_sha256(profile) if profile is not None else None
        ),
    }
    material["proposalMaterialSha256"] = canonical_sha256(material)
    metadata["proposalMaterial"] = material
    return profile, metadata


def _entry_path(root: Path, ordinal: int) -> Path:
    return root / "proposal-journal" / f"{ordinal:08d}.json"


def _load_entries(root: Path) -> list[dict[str, Any]]:
    paths = sorted((root / "proposal-journal").glob("*.json"))
    entries = []
    for ordinal, path in enumerate(paths):
        if path.name != f"{ordinal:08d}.json":
            raise TemporalDiscoveryContractError("QD proposal journal has a gap")
        entry = _read(path, name="QD proposal entry")
        supplied = _identity_payload(entry, "entrySha256", name="QD proposal entry")
        entry["entrySha256"] = supplied
        if (
            entry.get("schemaVersion") != QD_ENTRY_SCHEMA
            or int(entry.get("proposalOrdinal", -1)) != ordinal
        ):
            raise TemporalDiscoveryContractError("QD proposal entry routing mismatch")
        entries.append(entry)
    return entries


def _accepted_state(
    entries: Sequence[Mapping[str, Any]], archive: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], Counter[str], set[str], set[str]]:
    accepted = []
    counts: Counter[str] = Counter()
    programs = {
        str(member["candidate"]["programSha256"])
        for cell in archive["cells"]
        for member in cell["members"]
    }
    identities = {
        str(member["candidate"]["candidateId"])
        for cell in archive["cells"]
        for member in cell["members"]
    }
    for entry in entries:
        if entry.get("disposition") != "accepted":
            continue
        candidate = entry.get("candidate")
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("accepted QD entry lacks candidate")
        candidate = _clone(candidate, name="accepted QD candidate")
        candidate_id = str(candidate["candidateId"])
        if candidate_id in identities:
            raise TemporalDiscoveryContractError(
                "QD accepted candidate identity is duplicated"
            )
        if candidate["programSha256"] in programs:
            raise TemporalDiscoveryContractError("QD accepted program is duplicated")
        if (
            canonical_sha256(candidate["sourceProfile"])
            != candidate["sourceProfileSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD accepted profile identity mismatch"
            )
        programs.add(candidate["programSha256"])
        identity_material = candidate.get("candidateIdentityMaterial")
        if not isinstance(identity_material, Mapping) or canonical_sha256(
            identity_material
        ) != candidate.get("candidateIdentitySha256"):
            raise TemporalDiscoveryContractError(
                "QD candidate identity material diverged"
            )
        if (
            candidate_id
            != "qd_"
            + str(candidate["candidateIdentitySha256"]).removeprefix("sha256:")[:28]
        ):
            raise TemporalDiscoveryContractError("QD candidate ID is not content-bound")
        identities.add(candidate_id)
        counts[str(entry["originKind"])] += 1
        accepted.append(candidate)
    return accepted, counts, programs, identities


def _replay_entries(
    *,
    entries: Sequence[Mapping[str, Any]],
    config_sha: str,
    generation_index: int,
    cells: Sequence[Mapping[str, Any]],
    immigrant_source: ExactGeneratorV2Continuation,
    operators: Mapping[str, Any],
    parameters: Mapping[str, Any],
    selection_state: dict[str, dict[str, int]],
) -> None:
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for ordinal, entry in enumerate(entries):
        proposal_value = entry.get("proposal")
        if not isinstance(proposal_value, Mapping):
            raise TemporalDiscoveryContractError(
                "QD replay proposal material is missing"
            )
        replay_steps = (
            proposal_value.get("steps")
            if entry.get("originKind") == "structural_offspring"
            else None
        )
        profile, metadata = _proposal(
            config_sha=config_sha,
            generation_index=generation_index,
            proposal_ordinal=ordinal,
            cells=cells,
            immigrant_source=immigrant_source,
            operators=operators,
            parameters=parameters,
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=None,
            replay_steps=replay_steps,
        )
        if metadata["proposalMaterial"] != entry.get("proposal"):
            raise TemporalDiscoveryContractError(
                f"QD exact proposal replay diverged at ordinal {ordinal}"
            )
        if str(entry.get("originKind")) != _origin_kind(ordinal):
            raise TemporalDiscoveryContractError("QD replay origin schedule diverged")
        if entry.get("disposition") == "accepted":
            candidate = entry.get("candidate")
            if not isinstance(candidate, Mapping) or profile is None:
                raise TemporalDiscoveryContractError("QD accepted replay is incomplete")
            if canonical_sha256(profile) != candidate.get("sourceProfileSha256"):
                raise TemporalDiscoveryContractError(
                    "QD accepted replay profile diverged"
                )


def _accepted_candidate(
    *,
    profile: Mapping[str, Any],
    validation: Mapping[str, Any],
    metadata: Mapping[str, Any],
    generation_index: int,
    birth_ordinal: int,
    proposal_ordinal: int,
) -> dict[str, Any]:
    source_sha = canonical_sha256(profile)
    program_sha = _sha(validation.get("programSha256"), name="QD program SHA-256")
    candidate_id = str(metadata["candidateId"])
    identity_sha = _sha(
        metadata.get("candidateIdentitySha256"), name="QD candidate identity SHA-256"
    )
    identity_material = _clone(
        metadata["candidateIdentityMaterial"], name="QD candidate identity material"
    )
    if canonical_sha256(identity_material) != identity_sha or candidate_id != (
        "qd_" + identity_sha.removeprefix("sha256:")[:28]
    ):
        raise TemporalDiscoveryContractError("QD accepted candidate identity diverged")
    if metadata["originKind"] == "structural_offspring":
        structural_history = _clone(
            metadata["completeStructuralOperatorHistory"],
            name="complete structural history",
        )
        mutation_trace = []
        repairs = []
        seed_id = str(metadata["parent"].get("seedId") or "qd_parent")
        parent_ids = [metadata["parentCandidateId"]]
        parent_programs = [metadata["parentProgramSha256"]]
    else:
        immigrant = metadata["immigrantProposal"]
        structural_history = []
        mutation_trace = _clone(immigrant["mutations"], name="immigrant mutations")
        repairs = _clone(immigrant["activationAwareRepairs"], name="immigrant repairs")
        seed_id = str(immigrant["seedId"])
        parent_ids = []
        parent_programs = []
    lineage = {
        "schemaVersion": "temporal_qd_candidate_lineage_v2",
        "candidateId": candidate_id,
        "candidateIdentitySha256": identity_sha,
        "candidateSourceProfileSha256": source_sha,
        "candidateValidatedProgramSha256": program_sha,
        "originKind": metadata["originKind"],
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "parentCandidateIds": parent_ids,
        "parentProgramSha256s": parent_programs,
    }
    lineage["lineageSha256"] = canonical_sha256(lineage)
    return {
        "candidateId": candidate_id,
        "sourceMode": "qd_" + metadata["originKind"],
        "seedId": seed_id,
        "generationIndex": generation_index,
        "birthOrdinal": birth_ordinal,
        "proposalOrdinal": proposal_ordinal,
        "sourceProfile": _clone(profile, name="QD accepted profile"),
        "sourceProfileSha256": source_sha,
        "profileSnapshotSha256": _sha(
            validation.get("profileSnapshotSha256"), name="QD snapshot SHA-256"
        ),
        "programSha256": program_sha,
        "validationReportSha256": _sha(
            validation.get("validationReportSha256"), name="QD validation SHA-256"
        ),
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": identity_sha,
        "structuralDepth": len(structural_history),
        "structuralOperatorHistory": structural_history,
        "mutationTrace": mutation_trace,
        "activationAwareRepairs": repairs,
        "lineage": lineage,
    }


def _manifest(root: Path, *, population_sha: str) -> dict[str, Any]:
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
        "schemaVersion": QD_MANIFEST_SCHEMA,
        "populationSha256": population_sha,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_once(root / "manifest.json", manifest)
    return manifest


def _proposal_accounting(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    origin_proposals: Counter[str] = Counter()
    origin_accepted: Counter[str] = Counter()
    selection_modes: Counter[str] = Counter()
    selected_cells: Counter[str] = Counter()
    offspring_attempts: Counter[str] = Counter()
    family_attempts: Counter[str] = Counter()
    family_applications: Counter[str] = Counter()
    mutation_depths: Counter[str] = Counter()
    dispositions: Counter[str] = Counter()
    for entry in entries:
        origin = str(entry["originKind"])
        origin_proposals[origin] += 1
        disposition = str(entry["disposition"])
        dispositions[disposition] += 1
        if disposition == "accepted":
            origin_accepted[origin] += 1
        if origin != "structural_offspring":
            continue
        proposal = entry.get("proposal") or {}
        selection_modes[str(proposal.get("parentSelectionMode") or "unknown")] += 1
        cell_id = str(proposal.get("parentCellId") or "unknown")
        selected_cells[cell_id] += 1
        offspring_attempts[cell_id] += 1
        mutation_depths[str(proposal.get("desiredMutationDepth") or 0)] += 1
        for step in proposal.get("steps") or []:
            operator_id = str(step.get("operatorId") or "unknown")
            family_attempts[operator_id] += 1
            if step.get("disposition") == "applied":
                family_applications[operator_id] += 1
    return {
        "originProposalCounts": dict(sorted(origin_proposals.items())),
        "originAcceptedCounts": dict(sorted(origin_accepted.items())),
        "parentSelectionModeCounts": dict(sorted(selection_modes.items())),
        "parentCellSelectionCounts": dict(sorted(selected_cells.items())),
        "parentCellOffspringAttemptCounts": dict(sorted(offspring_attempts.items())),
        "operatorFamilyAttemptCounts": dict(sorted(family_attempts.items())),
        "operatorFamilyApplicationCounts": dict(sorted(family_applications.items())),
        "mutationDepthAttemptCounts": dict(sorted(mutation_depths.items())),
        "dispositionCounts": dict(sorted(dispositions.items())),
    }


def generate_qd_generation(
    *,
    parent_archive_path: Path | str,
    source_preparation_path: Path | str,
    base_generator_root: Path | str,
    confirmed_entry_admission_root: Path | str,
    validator_command: Sequence[str],
    output_root: Path | str,
    generation_index: int,
    immigrant_continuation_start: int = 0,
    parameters: Mapping[str, Any] | None = None,
    validator_timeout_seconds: float = 60.0,
    max_new_proposals: int | None = None,
) -> dict[str, Any]:
    if generation_index < 1:
        raise TemporalDiscoveryContractError("evolved QD generations begin at index 1")
    if immigrant_continuation_start < 0:
        raise TemporalDiscoveryContractError("immigrant continuation start is negative")
    root = Path(output_root)
    archive, archive_sha = _load_archive(Path(parent_archive_path))
    config_parameters = _normalize_parameters(parameters)
    operators = _operators()
    operator_specs = [
        {
            "operatorId": operator_id,
            "operatorVersion": operator.operator_version,
            "operatorSpecSha256": operator.specification["operatorSpecSha256"],
        }
        for operator_id, operator in sorted(operators.items())
    ] + [
        {
            "operatorId": operator_id,
            "operatorVersion": PARAMETRIC_OPERATOR_VERSION,
            "operatorSpecSha256": spec_sha,
        }
        for operator_id, spec_sha in sorted(PARAMETRIC_OPERATOR_SPECS.items())
    ]
    immigrant_source = ExactGeneratorV2Continuation(
        source_preparation_path=source_preparation_path,
        base_generator_root=base_generator_root,
        confirmed_entry_admission_root=confirmed_entry_admission_root,
        start_continuation_ordinal=immigrant_continuation_start,
    )
    config = {
        "schemaVersion": QD_CONFIG_SCHEMA,
        "qdVersion": QD_VERSION,
        "generationIndex": generation_index,
        "parentArchiveSha256": archive_sha,
        "immigrantSourceIdentity": immigrant_source.source_identity,
        "immigrantContinuationStart": immigrant_continuation_start,
        "operatorRegistry": operator_specs,
        "selectionPolicy": {
            "originSchedule": "four_archive_offspring_then_one_generator_v2_immigrant",
            "parentCellMixture": {
                "uniformOccupiedCell": 0.50,
                "lowVisitCell": 0.30,
                "sparseDescriptorBoundary": 0.20,
            },
            "parentWithinCell": "uniform_pareto_member",
            "operator": "uniform_eligible_family_then_uniform_occurrence_then_uniform_parameter_plan",
            "mutationDepth": {"one": 0.70, "two": 0.25, "three": 0.05},
        },
        "parameters": config_parameters,
        "marketEvidenceReadDuringGeneration": False,
        "gatewayContactedDuringGeneration": False,
    }
    config["configSha256"] = canonical_sha256(config)
    _write_once(root / "config.json", config)

    target = int(config_parameters["targetUniqueCandidates"])
    cells = _reproduction_cells(archive)
    selection_state = _initial_selection_state(cells)
    entries = _load_entries(root)
    _replay_entries(
        entries=entries,
        config_sha=config["configSha256"],
        generation_index=generation_index,
        cells=cells,
        immigrant_source=immigrant_source,
        operators=operators,
        parameters=config_parameters,
        selection_state=selection_state,
    )
    accepted, accepted_counts, seen_programs, seen_identities = _accepted_state(
        entries, archive
    )
    validator = SubprocessCandidateValidator(
        validator_command, timeout_seconds=validator_timeout_seconds
    )
    plan_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    new_proposals = 0
    while (
        len(accepted) < target
        and len(entries) < int(config_parameters["maxProposalAttempts"])
        and (max_new_proposals is None or new_proposals < max_new_proposals)
    ):
        ordinal = len(entries)
        profile, metadata = _proposal(
            config_sha=config["configSha256"],
            generation_index=generation_index,
            proposal_ordinal=ordinal,
            cells=cells,
            immigrant_source=immigrant_source,
            operators=operators,
            parameters=config_parameters,
            plan_cache=plan_cache,
            selection_state=selection_state,
            validator=validator,
        )
        proposal = metadata["proposalMaterial"]
        entry: dict[str, Any] = {
            "schemaVersion": QD_ENTRY_SCHEMA,
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "proposalOrdinal": ordinal,
            "originKind": metadata["originKind"],
            "proposal": proposal,
        }
        if profile is None:
            entry["disposition"] = str(metadata["proposalIssue"])
        elif str(metadata["candidateId"]) in seen_identities:
            entry["disposition"] = "duplicate_candidate_identity"
        else:
            reachability = inspect_management_reachability(profile)
            entry["managementReachabilitySha256"] = reachability["reachabilitySha256"]
            entry["managementReachabilityIssueCounts"] = reachability["issueCounts"]
            if reachability["acceptable"] is not True:
                entry["disposition"] = "static_reachability_rejected"
            else:
                if metadata["originKind"] == "structural_offspring":
                    validation_record = _clone(
                        metadata["finalValidation"], name="final structural validation"
                    )
                else:
                    source_sha = canonical_sha256(profile)
                    validation_record = _validation_record(
                        validator.validate(
                            candidate_id=str(metadata["candidateId"]),
                            source_profile=profile,
                            expected_raw_source_profile_sha256=source_sha,
                        )
                    )
                entry.update(
                    {
                        "validationStatus": validation_record.get("status"),
                        "validationReportSha256": validation_record.get(
                            "validationReportSha256"
                        ),
                        "validatedProgramSha256": validation_record.get(
                            "programSha256"
                        ),
                        "issueCodes": validation_record.get("issueCodes") or [],
                    }
                )
                if validation_record.get("candidateAcceptable") is not True:
                    entry["disposition"] = "native_validator_rejected"
                else:
                    program_sha = _sha(
                        validation_record.get("programSha256"),
                        name="QD program SHA-256",
                    )
                    if program_sha in seen_programs:
                        entry["disposition"] = "duplicate_program"
                    else:
                        candidate = _accepted_candidate(
                            profile=profile,
                            validation=validation_record,
                            metadata=metadata,
                            generation_index=generation_index,
                            birth_ordinal=len(accepted),
                            proposal_ordinal=ordinal,
                        )
                        entry["candidate"] = candidate
                        entry["disposition"] = "accepted"
                        accepted.append(candidate)
                        accepted_counts[metadata["originKind"]] += 1
                        seen_programs.add(program_sha)
                        seen_identities.add(str(candidate["candidateId"]))
        entry["entrySha256"] = canonical_sha256(entry)
        _write_once(_entry_path(root, ordinal), entry)
        entries.append(entry)
        new_proposals += 1
        accounting = _proposal_accounting(entries)
        checkpoint = {
            "schemaVersion": QD_CHECKPOINT_SCHEMA,
            "configSha256": config["configSha256"],
            "generationIndex": generation_index,
            "nextProposalOrdinal": len(entries),
            "rngCheckpoint": {
                "policy": "per_proposal_content_seed_v1",
                "rootSeed": config_parameters["seed"],
                "nextProposalOrdinal": len(entries),
            },
            "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
            "selectionState": selection_state,
            "acceptedCount": len(accepted),
            "acceptedOriginCounts": dict(sorted(accepted_counts.items())),
            "proposalAccounting": accounting,
            "completed": False,
        }
        checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
        _replace(root / "checkpoint.json", checkpoint)

    if len(accepted) < target:
        if max_new_proposals is not None and new_proposals >= max_new_proposals:
            return {
                "schemaVersion": "temporal_qd_generation_progress_v2",
                "configSha256": config["configSha256"],
                "generationIndex": generation_index,
                "proposalCount": len(entries),
                "acceptedCount": len(accepted),
                "targetUniqueCandidates": target,
                "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
                "completed": False,
            }
        raise TemporalDiscoveryGenerationExhausted(
            f"QD generation accepted {len(accepted)} of {target} unique candidates"
        )

    proposal_order = [
        str(entry["candidate"]["candidateId"])
        for entry in entries
        if entry.get("disposition") == "accepted"
    ]
    accepted.sort(key=lambda item: str(item["candidateId"]))
    accounting = _proposal_accounting(entries)
    population = {
        "schemaVersion": QD_POPULATION_SCHEMA,
        "qdVersion": QD_VERSION,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "targetUniqueCandidates": target,
        "originCounts": accounting["originAcceptedCounts"],
        "proposalOrderCandidateIds": proposal_order,
        "candidateCount": len(accepted),
        "candidates": accepted,
    }
    population["populationSha256"] = canonical_sha256(population)
    journal = {
        "schemaVersion": QD_JOURNAL_SCHEMA,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "proposalCount": len(entries),
        "acceptedCount": len(accepted),
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        **accounting,
        "entrySha256s": [item["entrySha256"] for item in entries],
    }
    journal["journalSha256"] = canonical_sha256(journal)
    _write_once(root / "population.json", population)
    _write_once(root / "generation-journal.json", journal)
    checkpoint = {
        "schemaVersion": QD_CHECKPOINT_SCHEMA,
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "nextProposalOrdinal": len(entries),
        "rngCheckpoint": {
            "policy": "per_proposal_content_seed_v1",
            "rootSeed": config_parameters["seed"],
            "nextProposalOrdinal": len(entries),
        },
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        "selectionState": selection_state,
        "acceptedCount": len(accepted),
        "acceptedOriginCounts": accounting["originAcceptedCounts"],
        "proposalAccounting": accounting,
        "completed": True,
        "populationSha256": population["populationSha256"],
        "journalSha256": journal["journalSha256"],
    }
    checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
    _replace(root / "checkpoint.json", checkpoint)
    manifest = _manifest(root, population_sha=population["populationSha256"])
    return {
        "schemaVersion": "temporal_qd_generation_result_v2",
        "configSha256": config["configSha256"],
        "generationIndex": generation_index,
        "populationSha256": population["populationSha256"],
        "journalSha256": journal["journalSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "proposalCount": len(entries),
        "candidateCount": len(accepted),
        "originProposalCounts": accounting["originProposalCounts"],
        "originAcceptedCounts": accounting["originAcceptedCounts"],
        "nextImmigrantContinuationOrdinal": immigrant_source.next_continuation_ordinal,
        "completed": True,
        "marketEvidenceReadDuringGeneration": False,
        "gatewayContactedDuringGeneration": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    archive = subparsers.add_parser("archive")
    archive.add_argument("--population", type=Path, required=True)
    archive.add_argument("--result-root", type=Path, required=True)
    archive.add_argument("--output", type=Path, required=True)
    archive.add_argument("--generation-index", type=int, required=True)
    archive.add_argument("--previous-archive", type=Path)
    archive.add_argument("--generation-journal", type=Path)
    archive.add_argument("--cell-capacity", type=int, default=8)
    archive.add_argument("--minimum-total-trades", type=int, default=8)
    archive.add_argument("--minimum-trades-per-window", type=int, default=2)

    generate = subparsers.add_parser("generate")
    generate.add_argument("--parent-archive", type=Path, required=True)
    generate.add_argument("--source-preparation", type=Path, required=True)
    generate.add_argument("--base-generator-root", type=Path, required=True)
    generate.add_argument("--confirmed-entry-admission-root", type=Path, required=True)
    generate.add_argument("--immigrant-continuation-start", type=int, default=0)
    generate.add_argument("--validator-command-file", type=Path, required=True)
    generate.add_argument("--output-root", type=Path, required=True)
    generate.add_argument("--generation-index", type=int, required=True)
    generate.add_argument("--parameters", type=Path)
    generate.add_argument("--validator-timeout-seconds", type=float, default=60.0)
    generate.add_argument("--max-new-proposals", type=int)
    args = parser.parse_args()

    if args.command == "archive":
        result = build_qd_archive(
            population_path=args.population,
            result_root=args.result_root,
            output_path=args.output,
            generation_index=args.generation_index,
            previous_archive_path=args.previous_archive,
            generation_journal_path=args.generation_journal,
            cell_capacity=args.cell_capacity,
            minimum_total_trades=args.minimum_total_trades,
            minimum_trades_per_window=args.minimum_trades_per_window,
        )
    else:
        command = json.loads(args.validator_command_file.read_text(encoding="utf-8"))
        if not isinstance(command, list) or not all(
            isinstance(value, str) for value in command
        ):
            raise TemporalDiscoveryContractError(
                "validator command file must contain a string array"
            )
        parameters = (
            _read(args.parameters, name="QD parameter file")
            if args.parameters is not None
            else None
        )
        result = generate_qd_generation(
            parent_archive_path=args.parent_archive,
            source_preparation_path=args.source_preparation,
            base_generator_root=args.base_generator_root,
            confirmed_entry_admission_root=args.confirmed_entry_admission_root,
            immigrant_continuation_start=args.immigrant_continuation_start,
            validator_command=command,
            output_root=args.output_root,
            generation_index=args.generation_index,
            parameters=parameters,
            validator_timeout_seconds=args.validator_timeout_seconds,
            max_new_proposals=args.max_new_proposals,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_QD_PARAMETERS",
    "QD_VERSION",
    "build_qd_archive",
    "generate_qd_generation",
    "qd_behavior_descriptor",
    "select_qd_archive",
]
