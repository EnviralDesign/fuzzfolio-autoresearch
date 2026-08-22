"""Source-grounded V38 follow-up audit: coverage, suboperations, and forensics.

This module never launches a generation, worker, or market evaluation. It only
reads existing V38 artifacts plus the Dashboard catalog and emits versioned
reports. Production scheduling is unchanged.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_indicator_learning_v1 import _period_choices, _range_choices
from .temporal_qd_generation_quality_audit import (
    _support_metrics,
    _window_metrics_from_evaluated,
)
from .temporal_qd_operator_family_matrix import (
    MATRIX_FAMILIES,
    slot_at,
    validate_operator_family_matrix,
    _candidate_metrics,
)

RESOURCE_OPERATOR_ID = "evolvable_resource_v1"
TOPOLOGY_OPERATOR_ID = "evolvable_topology_v1"
INITIAL_PROTECTION_OPERATOR_ID = "evolvable_initial_protection_v1"
HOLD_OPERATOR_ID = "evolvable_hold_policy_v1"
TEMPORAL_OPERATOR_ID = "evolvable_temporal_v1"

RESOURCE_CONSTRUCTION_KINDS = (
    "evidence_group_create",
    "evidence_group_remove",
    "evidence_group_split",
    "evidence_group_merge",
    "evidence_member_insert",
    "evidence_member_remove",
    "evidence_weight_mutate",
    "evidence_threshold_mutate",
    "indicator_instance_insert",
    "indicator_instance_remove",
    "indicator_substitute",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
    "indicator_period_mutate",
    "indicator_range_mutate",
    "directional_event_insert",
    "directional_event_remove",
    "directional_event_substitute",
)
PARAMETER_LEVEL_KINDS = {
    "indicator_period_mutate",
    "indicator_range_mutate",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
}
EVIDENCE_NUMERIC_KINDS = {"evidence_threshold_mutate", "evidence_weight_mutate"}
TOPOLOGY_OPERATIONS = (
    "insert_setup",
    "remove_setup",
    "rewire_entry_branch",
    "insert_entry_branch",
    "remove_entry_branch",
    "insert_confirmation_rejection",
    "insert_timeout_rearm",
    "remove_timeout_rearm",
    "insert_management_region",
    "remove_management_region",
    "rewire_management_region",
    "insert_exit_region",
    "remove_exit_region",
    "rewire_exit_region",
)
ADDITIVE_TOPOLOGY = {
    "insert_setup",
    "insert_entry_branch",
    "insert_confirmation_rejection",
    "insert_timeout_rearm",
    "insert_management_region",
    "insert_exit_region",
}
DESTRUCTIVE_TOPOLOGY = {
    "remove_setup",
    "remove_entry_branch",
    "remove_timeout_rearm",
    "remove_management_region",
    "remove_exit_region",
}
REWIRE_TOPOLOGY = {
    "rewire_entry_branch",
    "rewire_management_region",
    "rewire_exit_region",
}

COVERAGE_SCHEMA = "temporal_qd_indicator_parameter_evolution_coverage_v1"
RESOURCE_HERITABILITY_SCHEMA = "temporal_qd_v38_resource_suboperation_heritability_v1"
TOPOLOGY_AUDIT_SCHEMA = "temporal_qd_v38_topology_operation_audit_v1"
PROTECTION_FORENSIC_SCHEMA = "temporal_qd_v38_initial_protection_tail_forensic_v1"
DEFAULT_CATALOG = Path(r"C:\repos\Trading-Dashboard\shared\constants\indicators.json")
DEFAULT_V38_ROOT = Path(
    r"C:\repos\fuzzfolio-autoresearch\runs\temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38"
)
DEFAULT_OUTPUT = Path("research/temporal-qd/v38-followup")


def numbers_equal(left: Any, right: Any) -> bool:
    """Canonical numeric equality: exact stored float/int identity, not a tolerance."""

    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return left == right


def _mean(values: Sequence[float]) -> float | None:
    return float(sum(values) / len(values)) if values else None


def _median(values: Sequence[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def _quantiles(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p25": None, "median": None, "p75": None, "max": None}
    ordered = sorted(float(item) for item in values)
    return {
        "min": ordered[0],
        "p25": float(statistics.quantiles(ordered, n=4)[0]) if len(ordered) >= 4 else ordered[0],
        "median": float(statistics.median(ordered)),
        "p75": float(statistics.quantiles(ordered, n=4)[2]) if len(ordered) >= 4 else ordered[-1],
        "max": ordered[-1],
    }


def topology_operation_classes(operation: str) -> list[str]:
    labels: list[str] = []
    if operation in ADDITIVE_TOPOLOGY:
        labels.append("additive_complexification")
    if operation in DESTRUCTIVE_TOPOLOGY:
        labels.append("destructive_removal")
    if operation in REWIRE_TOPOLOGY:
        labels.append("rewire")
    if operation in {"insert_setup", "remove_setup", "insert_entry_branch", "remove_entry_branch", "rewire_entry_branch"}:
        labels.append("entry_setup")
    if operation in {"insert_confirmation_rejection", "insert_timeout_rearm", "remove_timeout_rearm"}:
        labels.append("confirmation_rearm")
    if "management_region" in operation:
        labels.append("management_region")
    if "exit_region" in operation:
        labels.append("exit_region")
    return labels


def resource_kind_bucket(kind: str | None) -> str:
    if kind in PARAMETER_LEVEL_KINDS:
        return "parameter_level_indicator"
    if kind in EVIDENCE_NUMERIC_KINDS:
        return "evidence_numeric"
    if kind in {
        "indicator_instance_insert",
        "indicator_instance_remove",
        "indicator_substitute",
    }:
        return "indicator_structure"
    if kind in {
        "evidence_group_create",
        "evidence_group_remove",
        "evidence_group_split",
        "evidence_group_merge",
        "evidence_member_insert",
        "evidence_member_remove",
    }:
        return "evidence_membership"
    if kind in {
        "directional_event_insert",
        "directional_event_remove",
        "directional_event_substitute",
    }:
        return "event_structure"
    return "unrecovered_or_other"


def implied_reward_to_risk(stop: Mapping[str, Any] | None, target: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(stop, Mapping) or not isinstance(target, Mapping):
        return {"defined": False, "reason": "missing_locator"}
    target_kind = target.get("kind")
    stop_kind = stop.get("kind")
    if target_kind == "none":
        return {"defined": False, "reason": "target_none"}
    if target_kind == "reward_multiple" and isinstance(target.get("multiple"), (int, float)):
        return {
            "defined": True,
            "method": "reward_multiple",
            "value": float(target["multiple"]),
            "stopKind": stop_kind,
            "targetKind": target_kind,
        }
    if (
        stop_kind == "fixed_percent"
        and target_kind == "fixed_percent"
        and isinstance(stop.get("percent"), (int, float))
        and isinstance(target.get("percent"), (int, float))
        and float(stop["percent"]) > 0
    ):
        return {
            "defined": True,
            "method": "fixed_percent_ratio",
            "value": float(target["percent"]) / float(stop["percent"]),
            "stopKind": stop_kind,
            "targetKind": target_kind,
        }
    return {
        "defined": False,
        "reason": "non_scalar_or_dynamic_locators",
        "stopKind": stop_kind,
        "targetKind": target_kind,
    }


def _construction_kind(construction: Mapping[str, Any] | None) -> str | None:
    if not isinstance(construction, Mapping):
        return None
    if construction.get("kind") == "hold":
        return "hold"
    if construction.get("kind") == "initial_protection":
        return "initial_protection"
    if construction.get("kind") == "typed_guard_replace":
        return "typed_guard_replace"
    if isinstance(construction.get("operation"), str):
        return str(construction["operation"])
    if isinstance(construction.get("kind"), str):
        return str(construction["kind"])
    return None


def _mutation_trace(delta: Mapping[str, Any]) -> list[dict[str, Any]]:
    application = delta.get("terminalOperatorApplication")
    if not isinstance(application, Mapping):
        return []
    audit = application.get("applicationAudit")
    if not isinstance(audit, Mapping):
        return []
    trace = audit.get("mutationTrace")
    if not isinstance(trace, list):
        return []
    return [item for item in trace if isinstance(item, Mapping)]


def summarize_construction(delta: Mapping[str, Any]) -> dict[str, Any]:
    plan = delta.get("terminalOperatorPlan") if isinstance(delta.get("terminalOperatorPlan"), Mapping) else {}
    construction = plan.get("construction") if isinstance(plan.get("construction"), Mapping) else {}
    kind = _construction_kind(construction)
    trace = _mutation_trace(delta)
    before = after = None
    if trace:
        before = trace[0].get("before")
        after = trace[0].get("after")
    summary = {
        "operatorId": plan.get("operatorId"),
        "choiceKind": plan.get("choiceKind"),
        "planSha256": plan.get("planSha256"),
        "envelopeSha256": canonical_sha256(plan) if plan else None,
        "constructionKind": kind,
        "construction": construction,
        "mutationClass": construction.get("mutationClass"),
        "site": construction.get("site"),
        "before": before,
        "after": after,
        "topologyClasses": topology_operation_classes(kind) if kind in TOPOLOGY_OPERATIONS else [],
        "resourceBucket": resource_kind_bucket(kind) if kind in RESOURCE_CONSTRUCTION_KINDS else None,
    }
    return summary


def v38_generation_root(v38_root: Path) -> Path:
    return v38_root / "run" / "g2-parents-800" / "generations" / "generation-0003"


def load_matrix(v38_root: Path) -> dict[str, Any]:
    path = v38_root / "operator-family-matrix-g2.json"
    return validate_operator_family_matrix(json.loads(path.read_text(encoding="utf-8")))


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise TemporalDiscoveryContractError(f"{path} line {line_number} must be an object")
            yield value


def load_slot_rows(*, v38_root: Path, matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    generation = v38_generation_root(v38_root)
    attempts = {
        int(row["proposalOrdinal"]): row
        for row in iter_jsonl(generation / "proposal" / "proposal-attempts.jsonl")
    }
    population = json.loads((generation / "proposal" / "evaluation-population.json").read_text(encoding="utf-8"))
    accepted_by_ordinal = {
        int(row["proposalOrdinal"]): str(row["candidateId"])
        for row in population.get("candidates") or []
        if isinstance(row, Mapping) and isinstance(row.get("candidateId"), str)
    }
    plans_by_child: dict[str, dict[str, Any]] = {}
    parent_programs: dict[str, dict[str, Any]] = {}
    for row in iter_jsonl(generation / "proposal" / "parent-material.jsonl"):
        candidate_id = str(row.get("candidateId") or "")
        payload = row.get("pairPayload") if isinstance(row.get("pairPayload"), Mapping) else {}
        delta = payload.get("proposalDelta") if isinstance(payload.get("proposalDelta"), Mapping) else {}
        if int(delta.get("generationIndex") or -1) == 3:
            plans_by_child[candidate_id] = summarize_construction(delta)
            plans_by_child[candidate_id]["applicationSha256"] = (
                (delta.get("terminalOperatorApplication") or {}).get("applicationSha256")
                if isinstance(delta.get("terminalOperatorApplication"), Mapping)
                else None
            )
            plans_by_child[candidate_id]["operatorTraceSha256"] = (
                (delta.get("terminalOperatorTrace") or {}).get("operatorTraceSha256")
                if isinstance(delta.get("terminalOperatorTrace"), Mapping)
                else None
            )
            plans_by_child[candidate_id]["lineageRefs"] = {
                "operatorPlanSha256": (delta.get("terminalOperatorTrace") or {}).get("terminalOperatorPlanSha256")
                if isinstance(delta.get("terminalOperatorTrace"), Mapping)
                else None
            }
        if candidate_id in {parent["candidateId"] for parent in matrix["parents"]}:
            parent_programs[candidate_id] = {
                "longProgram": delta.get("longProgram"),
                "shortProgram": delta.get("shortProgram"),
                "longProgramSha256": delta.get("longProgramSha256"),
                "shortProgramSha256": delta.get("shortProgramSha256"),
            }
    slots: list[dict[str, Any]] = []
    for ordinal in range(len(attempts)):
        attempt = attempts[ordinal]
        declared = slot_at(matrix, ordinal)
        if declared is None:
            raise TemporalDiscoveryContractError(f"matrix ordinal {ordinal} is outside the frozen slot grid")
        disposition = str(attempt.get("disposition") or "")
        candidate_id = accepted_by_ordinal.get(ordinal)
        plan = plans_by_child.get(candidate_id or "") if candidate_id else None
        refs = attempt.get("lineageRefs") if isinstance(attempt.get("lineageRefs"), Mapping) else {}
        slots.append(
            {
                **declared,
                "disposition": disposition,
                "reasonCode": attempt.get("reasonCode"),
                "attemptSha256": attempt.get("attemptSha256"),
                "candidateId": candidate_id,
                "parentFromAttempt": (refs.get("parent") or {}).get("candidateId")
                if isinstance(refs.get("parent"), Mapping)
                else None,
                "operatorPlanSha256": refs.get("operatorPlanSha256"),
                "operatorApplicationSha256": refs.get("operatorApplicationSha256"),
                "operatorTraceSha256": refs.get("operatorTraceSha256"),
                "duplicateOrNoop": disposition != "accepted",
                "canonicalCollapse": str(attempt.get("reasonCode") or "") == "duplicate_pair_genome",
                "plan": plan,
                "constructionKind": (plan or {}).get("constructionKind"),
                "recovered": plan is not None,
            }
        )
    for slot in slots:
        if slot["parentFromAttempt"] != slot["parentCandidateId"]:
            raise TemporalDiscoveryContractError("attempt parent drifted from matrix slot parent")
    return slots


def load_evaluated_by_id(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in iter_jsonl(path):
        candidate_id = row.get("candidateId")
        if isinstance(candidate_id, str):
            rows[candidate_id] = row
    return rows


def attach_economics(slots: Sequence[Mapping[str, Any]], evaluated: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for slot in slots:
        row = dict(slot)
        candidate_id = slot.get("candidateId")
        if isinstance(candidate_id, str) and candidate_id in evaluated:
            row["metrics"] = _candidate_metrics(evaluated[candidate_id])
        else:
            row["metrics"] = None
        output.append(row)
    return output


def parent_baselines(
    matrix: Mapping[str, Any],
    evaluated: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    baselines: dict[str, dict[str, Any]] = {}
    for parent in matrix["parents"]:
        candidate_id = parent["candidateId"]
        row = evaluated.get(candidate_id)
        if row is None:
            continue
        baselines[candidate_id] = {"role": parent["role"], **_candidate_metrics(row)}
    return baselines


def relative_to_parent(child: Mapping[str, Any] | None, parent: Mapping[str, Any] | None) -> dict[str, Any]:
    if not child or not parent:
        return {
            "comparable": False,
            "beatParent": False,
            "lostToParent": False,
            "economicTie": False,
            "deltaCumulativeConservativeNetR": None,
            "deltaWorstWindowConservativeNetR": None,
        }
    child_net = child.get("cumulativeConservativeNetR")
    parent_net = parent.get("cumulativeConservativeNetR")
    child_worst = child.get("worstWindowConservativeNetR")
    parent_worst = parent.get("worstWindowConservativeNetR")
    comparable = isinstance(child_net, (int, float)) and isinstance(parent_net, (int, float))
    if not comparable:
        return {
            "comparable": False,
            "beatParent": False,
            "lostToParent": False,
            "economicTie": False,
            "deltaCumulativeConservativeNetR": None,
            "deltaWorstWindowConservativeNetR": None,
        }
    delta = float(child_net) - float(parent_net)
    worst_delta = (
        float(child_worst) - float(parent_worst)
        if isinstance(child_worst, (int, float)) and isinstance(parent_worst, (int, float))
        else None
    )
    return {
        "comparable": True,
        "beatParent": delta > 0,
        "lostToParent": delta < 0,
        "economicTie": numbers_equal(child_net, parent_net),
        "deltaCumulativeConservativeNetR": delta,
        "deltaWorstWindowConservativeNetR": worst_delta,
        "absolutePositive": float(child_net) > 0,
    }


def _yield_row(slots: Sequence[Mapping[str, Any]], *, kind_key: str, kind_value: str | None = None) -> dict[str, Any]:
    attempts = len(slots)
    recovered = [slot for slot in slots if slot.get("recovered")]
    accepted = [slot for slot in slots if slot.get("disposition") == "accepted"]
    rejected = [slot for slot in slots if slot.get("disposition") != "accepted"]
    relatives = [
        slot["relative"]
        for slot in accepted
        if isinstance(slot.get("relative"), Mapping) and slot["relative"].get("comparable")
    ]
    nets = [
        float(slot["metrics"]["cumulativeConservativeNetR"])
        for slot in accepted
        if isinstance(slot.get("metrics"), Mapping)
        and isinstance(slot["metrics"].get("cumulativeConservativeNetR"), (int, float))
    ]
    deltas = [
        float(item["deltaCumulativeConservativeNetR"])
        for item in relatives
        if isinstance(item.get("deltaCumulativeConservativeNetR"), (int, float))
    ]
    worst_deltas = [
        float(item["deltaWorstWindowConservativeNetR"])
        for item in relatives
        if isinstance(item.get("deltaWorstWindowConservativeNetR"), (int, float))
    ]
    return {
        kind_key: kind_value if kind_value is not None else (slots[0].get("constructionKind") if slots else None),
        "attempts": attempts,
        "recoveredPlans": len(recovered),
        "uniqueAcceptedChildren": len(accepted),
        "duplicatesNoopsRejections": len(rejected),
        "duplicatePairGenomeCount": sum(1 for slot in rejected if slot.get("canonicalCollapse")),
        "unrecoveredRejected": sum(1 for slot in rejected if not slot.get("recovered")),
        "economicTies": sum(1 for item in relatives if item.get("economicTie")),
        "parentBeats": sum(1 for item in relatives if item.get("beatParent")),
        "parentLosses": sum(1 for item in relatives if item.get("lostToParent")),
        "absolutePositiveChildren": sum(1 for item in relatives if item.get("absolutePositive")),
        "comparableCount": len(relatives),
        "acceptedNetR": _quantiles(nets),
        "parentRelativeNetR": _quantiles(deltas),
        "parentRelativeWorstWindow": _quantiles(worst_deltas),
        "meanParentRelativeConservativeNetR": _mean(deltas),
        "medianParentRelativeConservativeNetR": _median(deltas),
    }


def bind_relatives(slots: Sequence[dict[str, Any]], baselines: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    bound: list[dict[str, Any]] = []
    for slot in slots:
        parent_id = str(slot.get("parentCandidateId") or "")
        slot["relative"] = relative_to_parent(slot.get("metrics"), baselines.get(parent_id))
        bound.append(slot)
    return bound


def attach_backfill(slots: Sequence[dict[str, Any]], *, v38_root: Path) -> list[dict[str, Any]]:
    generation = v38_generation_root(v38_root)
    panel_paths = {
        "panel-1": generation
        / "campaign"
        / "fast-prefinalizer"
        / "round-0001"
        / "task-0000"
        / "campaign-output"
        / "evaluated-members.jsonl",
        "panel-2": generation
        / "campaign"
        / "fast-prefinalizer"
        / "round-0001"
        / "task-0001"
        / "campaign-output"
        / "evaluated-members.jsonl",
    }
    panels = {name: load_evaluated_by_id(path) for name, path in panel_paths.items() if path.is_file()}
    for slot in slots:
        candidate_id = slot.get("candidateId")
        survival: dict[str, Any] = {}
        for name, rows in panels.items():
            row = rows.get(candidate_id) if isinstance(candidate_id, str) else None
            if row is None:
                survival[name] = None
                continue
            survival[name] = _candidate_metrics(row)
        slot["independentPanels"] = survival
        present = [name for name, value in survival.items() if value is not None]
        slot["independentPanelSurvival"] = {
            "availablePanels": present,
            "positiveOnAvailable": [
                name
                for name in present
                if isinstance((survival[name] or {}).get("cumulativeConservativeNetR"), (int, float))
                and float(survival[name]["cumulativeConservativeNetR"]) > 0
            ],
        }
    return list(slots)


def attach_archive(slots: Sequence[dict[str, Any]], *, v38_root: Path) -> list[dict[str, Any]]:
    archive_path = v38_generation_root(v38_root) / "native-finalization" / "archive.json"
    occupants: set[str] = set()
    if archive_path.is_file():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        for cell in archive.get("cells") or []:
            if not isinstance(cell, Mapping):
                continue
            for member in cell.get("members") or []:
                if isinstance(member, Mapping) and isinstance(member.get("candidateId"), str):
                    occupants.add(member["candidateId"])
    for slot in slots:
        candidate_id = slot.get("candidateId")
        slot["finalArchiveMember"] = isinstance(candidate_id, str) and candidate_id in occupants
    return list(slots)


def _group(slots: Sequence[Mapping[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in slots:
        grouped[str(slot.get(key) or "unrecovered")].append(dict(slot))
    return dict(grouped)


def catalog_indicator_rows(catalog: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = catalog.get("indicators")
    if not isinstance(rows, list):
        raise TemporalDiscoveryContractError("indicator catalog requires indicators[]")
    output: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
        config = item.get("config") if isinstance(item.get("config"), Mapping) else {}
        indicator_id = str(meta.get("id") or "").strip()
        if not indicator_id:
            continue
        talib_meta = meta.get("talibMeta") if isinstance(meta.get("talibMeta"), list) else []
        period_params = []
        other_numeric = []
        enums = []
        for descriptor in talib_meta:
            if not isinstance(descriptor, Mapping):
                continue
            name = str(descriptor.get("name") or "")
            ui_type = descriptor.get("uiType")
            if "period" in name.lower() and ui_type in {"integer_slider", "float_slider"}:
                period_params.append(dict(descriptor))
            elif ui_type in {"integer_slider", "float_slider", "number"}:
                other_numeric.append(dict(descriptor))
            elif ui_type in {"select", "enum", "dropdown"} or descriptor.get("options"):
                enums.append(dict(descriptor))
        period_plans = list(_period_choices(meta, config)) if period_params else []
        range_plans = list(_range_choices(meta, config))
        output.append(
            {
                "indicatorId": indicator_id,
                "name": meta.get("name"),
                "talibFunction": meta.get("talibFunction"),
                "usesRangeConfiguration": meta.get("usesRangeConfiguration") is True,
                "periodParameters": [
                    {
                        "name": row.get("name"),
                        "uiType": row.get("uiType"),
                        "default": row.get("default"),
                        "min": row.get("min"),
                        "max": row.get("max"),
                        "marks": row.get("marks"),
                    }
                    for row in period_params
                ],
                "periodChoicesFromCurrentConfig": period_plans,
                "nonPeriodNumericParameters": [
                    {
                        "name": row.get("name"),
                        "uiType": row.get("uiType"),
                        "default": row.get("default"),
                        "min": row.get("min"),
                        "max": row.get("max"),
                    }
                    for row in other_numeric
                ],
                "enumOrOptionParameters": [
                    {"name": row.get("name"), "uiType": row.get("uiType"), "options": row.get("options")}
                    for row in enums
                ],
                "rangeEvolvable": bool(range_plans),
                "hasPeriodPlan": bool(period_plans) or bool(period_params),
                "hasAnyParameterPlan": bool(period_params) or bool(range_plans),
            }
        )
    return output


def bound_instances_from_parents(parent_material: Path, parent_ids: Sequence[str]) -> list[dict[str, Any]]:
    wanted = set(parent_ids)
    instances: list[dict[str, Any]] = []
    for row in iter_jsonl(parent_material):
        candidate_id = str(row.get("candidateId") or "")
        if candidate_id not in wanted:
            continue
        payload = row.get("pairPayload") if isinstance(row.get("pairPayload"), Mapping) else {}
        delta = payload.get("proposalDelta") if isinstance(payload.get("proposalDelta"), Mapping) else {}
        for side, key in (("long", "longProgram"), ("short", "shortProgram")):
            program = delta.get(key)
            resources = program.get("resources") if isinstance(program, Mapping) else {}
            indicators = resources.get("indicators") if isinstance(resources, Mapping) else []
            if not isinstance(indicators, list):
                continue
            for item in indicators:
                if not isinstance(item, Mapping):
                    continue
                meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
                config = item.get("config") if isinstance(item.get("config"), Mapping) else {}
                instances.append(
                    {
                        "parentCandidateId": candidate_id,
                        "side": side,
                        "indicatorId": meta.get("id") or meta.get("baseIndicatorId"),
                        "instanceId": meta.get("instanceId"),
                        "timeframe": config.get("timeframe"),
                        "lookbackBars": config.get("lookbackBars"),
                        "talibConfig": config.get("talibConfig"),
                        "hasBoundTalibMeta": isinstance(meta.get("talibMeta"), list),
                    }
                )
    return instances


def build_coverage_report(
    *,
    catalog: Mapping[str, Any],
    matrix: Mapping[str, Any],
    parent_material: Path,
) -> dict[str, Any]:
    catalog_rows = catalog_indicator_rows(catalog)
    catalog_count = len(catalog_rows)
    catalog_with_param = sum(1 for row in catalog_rows if row["hasAnyParameterPlan"])
    catalog_with_period = sum(1 for row in catalog_rows if row["hasPeriodPlan"])
    bound = bound_instances_from_parents(
        parent_material, [parent["candidateId"] for parent in matrix["parents"]]
    )
    catalog_by_id = {row["indicatorId"]: row for row in catalog_rows}
    bound_with_plan = []
    for instance in bound:
        catalog_row = catalog_by_id.get(str(instance.get("indicatorId") or ""))
        instance = dict(instance)
        instance["catalogHasPeriodPlan"] = bool(catalog_row and catalog_row["hasPeriodPlan"])
        instance["catalogHasAnyParameterPlan"] = bool(catalog_row and catalog_row["hasAnyParameterPlan"])
        instance["catalogPeriodParameters"] = (catalog_row or {}).get("periodParameters") or []
        bound_with_plan.append(instance)
    missing = []
    for row in catalog_rows:
        for param in row["nonPeriodNumericParameters"]:
            missing.append(
                {
                    "indicatorId": row["indicatorId"],
                    "parameter": param.get("name"),
                    "uiType": param.get("uiType"),
                    "reason": "non_period_numeric_excluded_by_name_contains_period_rule",
                }
            )
        for param in row["enumOrOptionParameters"]:
            missing.append(
                {
                    "indicatorId": row["indicatorId"],
                    "parameter": param.get("name"),
                    "uiType": param.get("uiType"),
                    "reason": "enum_or_option_not_in_current_operator_surface",
                }
            )
    body = {
        "schemaVersion": COVERAGE_SCHEMA,
        "catalogSource": "Trading-Dashboard/shared/constants/indicators.json",
        "resourceOperatorId": RESOURCE_OPERATOR_ID,
        "constructionKinds": list(RESOURCE_CONSTRUCTION_KINDS),
        "periodAdmissionRule": {
            "talibMetaNameContains": "period",
            "uiTypes": ["integer_slider", "float_slider"],
            "choices": ["fast", "nominal", "slow"],
            "fast": "largest mark strictly below default, else min",
            "nominal": "catalog default",
            "slow": "smallest mark strictly above default, else max",
            "orderingConstraint": "fastPeriod < slowPeriod when both fast* and slow* talib keys exist",
        },
        "coverage": {
            "catalogIndicatorCount": catalog_count,
            "catalogIndicatorsWithPeriodSurface": catalog_with_period,
            "catalogIndicatorsWithAnyParameterSurface": catalog_with_param,
            "catalogFractionWithPeriodSurface": catalog_with_period / catalog_count if catalog_count else None,
            "catalogFractionWithAnyParameterSurface": catalog_with_param / catalog_count if catalog_count else None,
            "boundParentInstanceCount": len(bound_with_plan),
            "boundInstancesWithCatalogPeriodSurface": sum(
                1 for row in bound_with_plan if row["catalogHasPeriodPlan"]
            ),
            "boundInstancesWithAnyParameterSurface": sum(
                1 for row in bound_with_plan if row["catalogHasAnyParameterPlan"]
            ),
        },
        "catalogIndicators": catalog_rows,
        "boundParentInstances": bound_with_plan,
        "notCurrentlyEvolvable": missing,
        "proposedMissingParameterSurface": {
            "schemaVersion": "temporal_qd_catalog_bound_parameter_mutation_proposal_v1",
            "doNotAddGenericNumericMutation": True,
            "admit": [
                "named non-period talib sliders with catalog min/max/marks",
                "catalog enum/select options already present on talibMeta",
                "explicit ordering constraints copied from existing fast/slow period rules",
            ],
            "require": [
                "catalog authority hash on every plan",
                "construction identity sha256 over kind+indicatorInstanceId+parameter+before/after",
                "replay-identical apply/audit like current resource operators",
                "targeted tests for admission, exclusion, and identity stability",
            ],
            "exclude": [
                "unbounded floats",
                "parameters without catalog descriptors",
                "silently inventing marks",
            ],
        },
        "limitations": [
            "bound_v38_instances_do_not_embed_talibMeta_join_catalog_by_indicator_id",
            "coverage_is_source_and_parent_material_not_a_new_search",
        ],
    }
    body["reportSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "reportSha256"})
    return body


def build_resource_report(slots: Sequence[Mapping[str, Any]], baselines: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    resource_slots = [slot for slot in slots if slot.get("operatorFamily") == "resource"]
    by_kind = _group(resource_slots, "constructionKind")
    parent_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in resource_slots:
        parent_kind[f"{slot.get('parentCandidateId')}|{slot.get('constructionKind')}"].append(dict(slot))
    kind_rows = [
        _yield_row(group, kind_key="constructionKind", kind_value=kind)
        for kind, group in sorted(by_kind.items())
    ]
    answers = {
        "successfulChildrenMixture": {
            kind: row["uniqueAcceptedChildren"]
            for kind, row in ((item["constructionKind"], item) for item in kind_rows)
            if row["uniqueAcceptedChildren"]
        },
        "parameterLevelRepeatablePositiveTail": any(
            item["absolutePositiveChildren"] > 0 and item["parentBeats"] > 0
            for item in kind_rows
            if item["constructionKind"] in PARAMETER_LEVEL_KINDS
        ),
        "kindsWorkingAcrossAllArchiveParents": [
            kind
            for kind, group in by_kind.items()
            if kind in RESOURCE_CONSTRUCTION_KINDS
            and {
                slot["parentCandidateId"]
                for slot in resource_slots
                if slot.get("parentRole") == "archive"
            }.issubset(
                {
                    slot["parentCandidateId"]
                    for slot in group
                    if slot.get("disposition") == "accepted"
                    and slot.get("relative", {}).get("beatParent")
                    and slot.get("parentRole") == "archive"
                }
            )
        ],
        "negativeTailKinds": [
            item["constructionKind"]
            for item in sorted(kind_rows, key=lambda row: row["acceptedNetR"]["min"] or 0)
            if item["acceptedNetR"]["min"] is not None and item["acceptedNetR"]["min"] < 0
        ],
    }
    body = {
        "schemaVersion": RESOURCE_HERITABILITY_SCHEMA,
        "numericEquality": "exact_stored_float_identity",
        "family": "resource",
        "operatorId": RESOURCE_OPERATOR_ID,
        "slotCount": len(resource_slots),
        "baselinesPresent": sorted(baselines),
        "bySuboperation": kind_rows,
        "byParentAndSuboperation": [
            {
                "parentCandidateId": key.split("|", 1)[0],
                **_yield_row(group, kind_key="constructionKind", kind_value=key.split("|", 1)[1]),
            }
            for key, group in sorted(parent_kind.items())
        ],
        "answers": answers,
        "limitations": [
            "rejected_duplicate_slots_have_plan_hashes_but_no_fast_ephemeral_plan_body",
            "inactive_and_active_negative_parents_lack_panel_3_clone_baselines",
            "independent_panel_survival_only_where_prefinalizer_backfill_evaluated_the_child",
        ],
    }
    body["reportSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "reportSha256"})
    return body


def build_topology_report(slots: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    topology_slots = [slot for slot in slots if slot.get("operatorFamily") == "topology"]
    by_op = _group(topology_slots, "constructionKind")
    class_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in topology_slots:
        classes = (slot.get("plan") or {}).get("topologyClasses") or topology_operation_classes(
            str(slot.get("constructionKind") or "")
        )
        for label in classes or ["unrecovered"]:
            class_groups[label].append(dict(slot))
    kind_rows = [
        _yield_row(group, kind_key="constructionKind", kind_value=kind)
        for kind, group in sorted(by_op.items())
    ]
    additive = _yield_row(
        class_groups.get("additive_complexification") or [],
        kind_key="constructionKind",
        kind_value="additive_complexification",
    )
    destructive = _yield_row(
        class_groups.get("destructive_removal") or [],
        kind_key="constructionKind",
        kind_value="destructive_removal",
    )
    rewire = _yield_row(
        class_groups.get("rewire") or [],
        kind_key="constructionKind",
        kind_value="rewire",
    )
    accepted_kind_damage = sorted(
        (
            item
            for item in kind_rows
            if item["uniqueAcceptedChildren"]
        ),
        key=lambda row: (row["medianParentRelativeConservativeNetR"] is None, row["medianParentRelativeConservativeNetR"] or 0),
    )
    body = {
        "schemaVersion": TOPOLOGY_AUDIT_SCHEMA,
        "family": "topology",
        "operatorId": TOPOLOGY_OPERATOR_ID,
        "operations": list(TOPOLOGY_OPERATIONS),
        "slotCount": len(topology_slots),
        "byOperation": kind_rows,
        "byClass": {
            label: _yield_row(group, kind_key="constructionKind", kind_value=label)
            for label, group in sorted(class_groups.items())
        },
        "answers": {
            "broadlyDestructiveOrFewOperations": {
                "acceptedMedianParentRelativeByWorstOperations": [
                    {
                        "constructionKind": item["constructionKind"],
                        "medianParentRelativeConservativeNetR": item["medianParentRelativeConservativeNetR"],
                        "parentLosses": item["parentLosses"],
                        "uniqueAcceptedChildren": item["uniqueAcceptedChildren"],
                    }
                    for item in accepted_kind_damage[:5]
                ],
                "duplicateCollapseByOperation": [
                    {
                        "constructionKind": item["constructionKind"],
                        "duplicatePairGenomeCount": item["duplicatePairGenomeCount"],
                        "attempts": item["attempts"],
                    }
                    for item in sorted(kind_rows, key=lambda row: -row["duplicatePairGenomeCount"])
                ],
            },
            "additiveLessDestructiveThanRemovalOrRewire": {
                "additiveMedianParentRelative": additive["medianParentRelativeConservativeNetR"],
                "destructiveMedianParentRelative": destructive["medianParentRelativeConservativeNetR"],
                "rewireMedianParentRelative": rewire["medianParentRelativeConservativeNetR"],
                "additiveLessDestructive": (
                    additive["medianParentRelativeConservativeNetR"] is not None
                    and destructive["medianParentRelativeConservativeNetR"] is not None
                    and additive["medianParentRelativeConservativeNetR"]
                    > destructive["medianParentRelativeConservativeNetR"]
                ),
            },
        },
        "limitations": [
            "rejected_topology_slots_unrecovered_without_plan_body",
            "crossover_out_of_scope_for_v38",
        ],
    }
    body["reportSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "reportSha256"})
    return body


def _window_forensic(row: Mapping[str, Any]) -> dict[str, Any]:
    aggregate = row.get("aggregate") if isinstance(row.get("aggregate"), Mapping) else {}
    records = aggregate.get("windowRecords") if isinstance(aggregate.get("windowRecords"), list) else []
    windows: list[dict[str, Any]] = []
    close_reasons: Counter[str] = Counter()
    losing_streaks: list[int] = []
    trades_total = 0
    agg_reasons = aggregate.get("closeReasonDistribution")
    if isinstance(agg_reasons, Mapping):
        for key, value in agg_reasons.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool) and float(value) >= 1:
                close_reasons[str(key)] += int(value)
    for record in records:
        if not isinstance(record, Mapping):
            continue
        trade_nets: list[float] = []
        behavior = record.get("realizedBehavior")
        side_reason_counts: dict[str, int] = {}
        if isinstance(behavior, Mapping):
            sides = behavior.get("sides")
            if isinstance(sides, Mapping):
                for side in sides.values():
                    if not isinstance(side, Mapping):
                        continue
                    counts = side.get("closeReasonCounts")
                    if isinstance(counts, Mapping):
                        for key, value in counts.items():
                            if isinstance(value, (int, float)) and not isinstance(value, bool):
                                close_reasons[str(key)] += int(value)
                                side_reason_counts[str(key)] = side_reason_counts.get(str(key), 0) + int(value)
                    sequence = side.get("tradeSequence")
                    if not isinstance(sequence, list):
                        continue
                    for trade in sequence:
                        if not isinstance(trade, Mapping):
                            continue
                        trades_total += 1
                        net = trade.get("netR", trade.get("grossR"))
                        if isinstance(net, (int, float)) and not isinstance(net, bool):
                            trade_nets.append(float(net))
                        reason = trade.get("closeReason")
                        if isinstance(reason, str) and reason not in side_reason_counts:
                            close_reasons[reason] += 1
        streak = 0
        worst_streak = 0
        for net in trade_nets:
            if net < 0:
                streak += 1
                worst_streak = max(worst_streak, streak)
            else:
                streak = 0
        losing_streaks.append(worst_streak)
        windows.append(
            {
                "windowId": record.get("windowId"),
                "conservativeNetR": record.get("conservativeNetR"),
                "maxDrawdownR": record.get("maxDrawdownR"),
                "trades": record.get("trades"),
                "costDragR": record.get("costDragR"),
                "averageMaeR": record.get("averageMaeR"),
                "averageMfeR": record.get("averageMfeR"),
                "closeReasonDistribution": record.get("closeReasonDistribution") or side_reason_counts,
                "worstLosingStreakInWindow": worst_streak,
            }
        )
    nets = [float(item["conservativeNetR"]) for item in windows if isinstance(item.get("conservativeNetR"), (int, float))]
    cost = aggregate.get("costDragR")
    mechanism = "unknown"
    if windows:
        total_net = sum(nets)
        worst = min(windows, key=lambda item: float(item.get("conservativeNetR") or 0))
        worst_net = float(worst.get("conservativeNetR") or 0)
        if isinstance(cost, (int, float)) and not isinstance(cost, bool) and abs(float(cost)) >= 0.4 * abs(total_net or 1.0):
            mechanism = "cost_drag_and_churn"
        elif abs(worst_net) >= 0.6 * abs(total_net or worst_net) and trades_total < 40:
            mechanism = "concentrated_window_loss"
        elif trades_total >= 20:
            mechanism = "many_small_losses_or_churn"
        else:
            mechanism = "distributed_losses"
    return {
        "windows": windows,
        "closeReasonCounts": dict(close_reasons),
        "closeReasonFractions": agg_reasons if isinstance(agg_reasons, Mapping) else None,
        "tradeCount": trades_total,
        "costDragR": cost,
        "entryFrequencyPerThousand": aggregate.get("entryFrequencyPerThousand"),
        "averageHoldingBars": aggregate.get("averageHoldingBars"),
        "worstLosingStreak": max(losing_streaks) if losing_streaks else 0,
        "lossMechanismHypothesis": mechanism,
        "windowSupport": _support_metrics(_window_metrics_from_evaluated(row), covered_months=max(len(windows), 1)),
    }


def build_protection_report(
    slots: Sequence[Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    protection = [slot for slot in slots if slot.get("operatorFamily") == "initial_protection"]
    accepted = [slot for slot in protection if slot.get("disposition") == "accepted"]
    ranked = sorted(
        accepted,
        key=lambda slot: (
            not isinstance((slot.get("metrics") or {}).get("cumulativeConservativeNetR"), (int, float)),
            (slot.get("metrics") or {}).get("cumulativeConservativeNetR") or 0,
        ),
    )
    outliers = ranked[:8]
    cases = []
    for slot in outliers:
        candidate_id = slot.get("candidateId")
        row = evaluated.get(candidate_id) if isinstance(candidate_id, str) else None
        construction = (slot.get("plan") or {}).get("construction") or {}
        before = (slot.get("plan") or {}).get("before")
        after = (slot.get("plan") or {}).get("after") or construction.get("replacement")
        stop = before if construction.get("site") == "stop" else None
        target = before if construction.get("site") == "target" else None
        after_stop = after if construction.get("site") == "stop" else None
        after_target = after if construction.get("site") == "target" else None
        cases.append(
            {
                "candidateId": candidate_id,
                "parentCandidateId": slot.get("parentCandidateId"),
                "parentRole": slot.get("parentRole"),
                "operatorPlanSha256": slot.get("operatorPlanSha256"),
                "operatorApplicationSha256": slot.get("operatorApplicationSha256"),
                "site": construction.get("site"),
                "mutationClass": construction.get("mutationClass"),
                "beforeLocator": before,
                "afterLocator": after,
                "impliedRewardToRiskBefore": implied_reward_to_risk(stop, target) if construction.get("site") == "target" else implied_reward_to_risk(before if construction.get("site") == "stop" else None, None),
                "impliedRewardToRiskAfter": implied_reward_to_risk(
                    after_stop if construction.get("site") == "stop" else None,
                    after_target if construction.get("site") == "target" else None,
                ),
                "metrics": slot.get("metrics"),
                "relative": slot.get("relative"),
                "forensic": _window_forensic(row) if row else None,
                "finalArchiveMember": slot.get("finalArchiveMember"),
            }
        )
    by_class: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_site: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_before_after: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in protection:
        construction = (slot.get("plan") or {}).get("construction") or {}
        by_class[str(construction.get("mutationClass") or "unrecovered")].append(dict(slot))
        by_site[str(construction.get("site") or "unrecovered")].append(dict(slot))
        key = "|".join(
            [
                str(construction.get("site") or "unrecovered"),
                str(construction.get("mutationClass") or "unrecovered"),
                json.dumps(construction.get("replacement"), sort_keys=True, default=str),
            ]
        )
        by_before_after[key].append(dict(slot))
    worst = cases[0] if cases else None
    body = {
        "schemaVersion": PROTECTION_FORENSIC_SCHEMA,
        "family": "initial_protection",
        "operatorId": INITIAL_PROTECTION_OPERATOR_ID,
        "slotCount": len(protection),
        "catastrophicTail": {
            "approximateMinus61RMatch": worst,
            "note": "The realized extreme is the worst accepted initial-protection child, not a rounded label.",
        },
        "outlierCases": cases,
        "byMutationClass": {
            key: _yield_row(group, kind_key="constructionKind", kind_value=key)
            for key, group in sorted(by_class.items())
        },
        "bySite": {
            key: _yield_row(group, kind_key="constructionKind", kind_value=key)
            for key, group in sorted(by_site.items())
        },
        "byBeforeAfterPlan": [
            {"planKey": key, **_yield_row(group, kind_key="constructionKind", kind_value=key)}
            for key, group in sorted(by_before_after.items())
        ],
        "limitations": [
            "implied_rr_undefined_for_indicator_or_dynamic_locators",
            "compact_trade_sequence_omits_per_trade_mae_mfe_window_averages_retained",
            "rejected_protection_slots_unrecovered_without_plan_body",
        ],
    }
    body["reportSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "reportSha256"})
    return body


def _md_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join("" if item is None else str(item) for item in row) + " |")
    return "\n".join(lines)


def coverage_markdown(report: Mapping[str, Any]) -> str:
    coverage = report["coverage"]
    return "\n".join(
        [
            "# Indicator-parameter evolution coverage v1",
            "",
            "Source-grounded audit of the current `evolvable_resource_v1` parameter surface. This is not a launch.",
            "",
            f"- Catalog indicators: **{coverage['catalogIndicatorCount']}**",
            f"- With period surface: **{coverage['catalogIndicatorsWithPeriodSurface']}** ({coverage['catalogFractionWithPeriodSurface']})",
            f"- With any parameter surface: **{coverage['catalogIndicatorsWithAnyParameterSurface']}** ({coverage['catalogFractionWithAnyParameterSurface']})",
            f"- V38 bound parent instances: **{coverage['boundParentInstanceCount']}**",
            f"- Bound instances whose catalog row has a period surface: **{coverage['boundInstancesWithCatalogPeriodSurface']}**",
            "",
            "Period mutation admits only `talibMeta` parameters whose name contains `period` and whose `uiType` is `integer_slider` or `float_slider`. Choices are exactly fast / nominal / slow.",
            "",
            "Do not add generic numeric mutation. Any missing surface must be catalog-bound, identity-hashed, and tested.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def resource_markdown(report: Mapping[str, Any]) -> str:
    rows = [
        [
            item.get("constructionKind"),
            item["attempts"],
            item["uniqueAcceptedChildren"],
            item["duplicatePairGenomeCount"],
            item["parentBeats"],
            item["parentLosses"],
            item["absolutePositiveChildren"],
            item["medianParentRelativeConservativeNetR"],
        ]
        for item in report["bySuboperation"]
    ]
    return "\n".join(
        [
            "# V38 resource suboperation heritability v1",
            "",
            "Every resource slot is joined to parent, plan SHA, construction kind, and panel-3 economics. Rejected duplicate slots keep their attempt hashes; fast-ephemeral does not persist their plan bodies.",
            "",
            _md_table(
                [
                    "kind",
                    "attempts",
                    "accepted",
                    "dup",
                    "beats",
                    "losses",
                    "abs+",
                    "median Δ net R",
                ],
                rows,
            ),
            "",
            f"Parameter-level repeatable positive tail: **{report['answers']['parameterLevelRepeatablePositiveTail']}**",
            f"Kinds beating all archive parents: {report['answers']['kindsWorkingAcrossAllArchiveParents']}",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def topology_markdown(report: Mapping[str, Any]) -> str:
    rows = [
        [
            item.get("constructionKind"),
            item["attempts"],
            item["uniqueAcceptedChildren"],
            item["duplicatePairGenomeCount"],
            item["parentBeats"],
            item["parentLosses"],
            item["medianParentRelativeConservativeNetR"],
        ]
        for item in report["byOperation"]
    ]
    additive = report["answers"]["additiveLessDestructiveThanRemovalOrRewire"]
    return "\n".join(
        [
            "# V38 topology operation audit v1",
            "",
            "Topology is typed, not arbitrary graph corruption. V38 pooled 14 operations at family level.",
            "",
            _md_table(
                ["operation", "attempts", "accepted", "dup", "beats", "losses", "median Δ net R"],
                rows,
            ),
            "",
            f"Additive median Δ: {additive['additiveMedianParentRelative']}; destructive: {additive['destructiveMedianParentRelative']}; rewire: {additive['rewireMedianParentRelative']}.",
            f"Additive less destructive than removal: **{additive['additiveLessDestructive']}**",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def protection_markdown(report: Mapping[str, Any]) -> str:
    worst = report["catastrophicTail"]["approximateMinus61RMatch"] or {}
    forensic = worst.get("forensic") or {}
    return "\n".join(
        [
            "# V38 initial-protection tail forensic v1",
            "",
            f"Worst accepted child: `{worst.get('candidateId')}` parent `{worst.get('parentCandidateId')}`",
            f"- mutationClass: `{worst.get('mutationClass')}` site `{worst.get('site')}`",
            f"- before: `{worst.get('beforeLocator')}`",
            f"- after: `{worst.get('afterLocator')}`",
            f"- panel-3 cumulative net R: {(worst.get('metrics') or {}).get('cumulativeConservativeNetR')}",
            f"- hypothesized mechanism: `{forensic.get('lossMechanismHypothesis')}`",
            f"- cost drag R: `{forensic.get('costDragR')}`",
            f"- close reason counts: `{forensic.get('closeReasonCounts')}`",
            f"- close reason fractions: `{forensic.get('closeReasonFractions')}`",
            f"- trades: {forensic.get('tradeCount')} worst losing streak {forensic.get('worstLosingStreak')}",
            "",
            "Full before→after decomposition is in the JSON report. Implied R:R is defined only for scalar stop/target locators.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def write_report(path: Path, payload: Mapping[str, Any], markdown: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8", newline="\n")
    path.with_suffix(".md").write_text(markdown, encoding="utf-8", newline="\n")


def run_audit(
    *,
    v38_root: Path,
    catalog_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    matrix = load_matrix(v38_root)
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    generation = v38_generation_root(v38_root)
    slots = load_slot_rows(v38_root=v38_root, matrix=matrix)
    panel3 = load_evaluated_by_id(
        generation / "campaign" / "proposal-current-panel" / "campaign-output" / "evaluated-members.jsonl"
    )
    clones = generation / "campaign" / "fast-prefinalizer" / "round-0000" / "task-0000" / "campaign-output" / "evaluated-members.jsonl"
    if clones.is_file():
        panel3.update(load_evaluated_by_id(clones))
    slots = attach_economics(slots, panel3)
    baselines = parent_baselines(matrix, panel3)
    slots = bind_relatives(slots, baselines)
    slots = attach_backfill(slots, v38_root=v38_root)
    slots = attach_archive(slots, v38_root=v38_root)
    coverage = build_coverage_report(
        catalog=catalog,
        matrix=matrix,
        parent_material=generation / "proposal" / "parent-material.jsonl",
    )
    resource = build_resource_report(slots, baselines)
    topology = build_topology_report(slots)
    protection = build_protection_report(slots, panel3)
    outputs = {
        "indicator-parameter-evolution-coverage-v1.json": (coverage, coverage_markdown(coverage)),
        "v38-resource-suboperation-heritability-v1.json": (resource, resource_markdown(resource)),
        "v38-topology-operation-audit-v1.json": (topology, topology_markdown(topology)),
        "v38-initial-protection-tail-forensic-v1.json": (protection, protection_markdown(protection)),
    }
    written: dict[str, Path] = {}
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        write_report(path, payload, markdown)
        written[name] = path
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v38-root", type=Path, default=DEFAULT_V38_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    written = run_audit(v38_root=args.v38_root, catalog_path=args.catalog, output_dir=args.output_dir)
    for name, path in written.items():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
