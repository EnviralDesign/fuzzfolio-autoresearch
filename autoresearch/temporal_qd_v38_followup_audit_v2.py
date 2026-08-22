"""V38 follow-up audit v2: corrected semantics, multi-panel, and forensics.

Does not mutate v1 reports. Does not launch a generation, worker, or market
evaluation.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_direction_selection import DEFAULT_DIRECTION_SELECTION_POLICY
from .temporal_qd_generation_quality_audit import (
    _DEFAULT_ROBUST,
    _gate_flags,
    _window_metrics_from_evaluated,
)
from .temporal_qd_operator_family_matrix import _candidate_metrics
from .temporal_qd_resource_suboperation_matrix import build_resource_suboperation_matrix
from .temporal_qd_topology_coadaptation import (
    FIRST_EXPERIMENT_JUSTIFICATION,
    build_topology_coadaptation_matrix,
)
from .temporal_qd_v38_followup_audit import (
    DEFAULT_CATALOG,
    DEFAULT_OUTPUT,
    DEFAULT_V38_ROOT,
    PARAMETER_LEVEL_KINDS,
    RESOURCE_CONSTRUCTION_KINDS,
    RESOURCE_OPERATOR_ID,
    TOPOLOGY_OPERATIONS,
    attach_backfill,
    build_coverage_report,
    catalog_indicator_rows,
    implied_reward_to_risk,
    iter_jsonl,
    load_evaluated_by_id,
    load_matrix,
    load_slot_rows,
    topology_operation_classes,
    v38_generation_root,
    write_report,
    _md_table,
    _mean,
    _median,
    _quantiles,
    _window_forensic,
    _group,
)

RESOURCE_HERITABILITY_SCHEMA_V2 = "temporal_qd_v38_resource_suboperation_heritability_v2"
MULTI_PANEL_SCHEMA = "temporal_qd_v38_multipanel_suboperation_v2"
EVENT_INSERT_SCHEMA = "temporal_qd_v38_directional_event_insert_forensic_v2"
COVERAGE_SCHEMA_V2 = "temporal_qd_indicator_parameter_evolution_coverage_v2"
TOPOLOGY_AUDIT_SCHEMA_V2 = "temporal_qd_v38_topology_operation_audit_v2"
PROTECTION_FORENSIC_SCHEMA_V2 = "temporal_qd_v38_initial_protection_tail_forensic_v2"
METRIC_IDENTITY_FLOOR_R = 1e-12
NUMERIC_EQUALITY = "canonical_json_number_roundtrip_with_1e-12_encoding_floor"
REPEATABILITY_CONTRACT = {
    "schemaVersion": "temporal_qd_v38_parameter_repeatability_contract_v1",
    "minAcceptedUniqueChildren": 8,
    "minComparable": 8,
    "minParentBeats": 3,
    "minAbsolutePositive": 2,
    "minArchiveParentsWithAtLeastOneBeat": 2,
    "requireNonWorseWorstWindowOnBeats": True,
    "note": "Count-based observational contract. Not an economic margin.",
}
V38_ROTATING_EVIDENCE_SHA256 = "sha256:10d0cdeb60433b452af475f451fc4782f7a26e24210f3cb76e62d8a08127f1bb"


def canonical_metric_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(float(value)):
        return None
    return float(json.loads(canonical_json(float(value))))


def canonical_metrics_equal(left: Any, right: Any) -> bool:
    """Canonical metric identity, not an economic margin.

    Values are JSON-round-tripped with the repository canonical encoder. Residuals
    at or below 1e-12 R are encoding dust (the V38 range-mutate 9.77e-15R case).
    """

    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    a = canonical_metric_number(left)
    b = canonical_metric_number(right)
    if a is None or b is None:
        return left == right
    if canonical_json(a) == canonical_json(b):
        return True
    return abs(a - b) <= METRIC_IDENTITY_FLOOR_R


def canonical_metric_greater(left: Any, right: Any) -> bool:
    a = canonical_metric_number(left)
    b = canonical_metric_number(right)
    if a is None or b is None:
        return False
    return (not canonical_metrics_equal(a, b)) and a > b


def phenotype_tie(child: Mapping[str, Any], parent: Mapping[str, Any]) -> bool:
    keys = (
        "cumulativeConservativeNetR",
        "worstWindowConservativeNetR",
        "medianWindowConservativeNetR",
        "activeWindowFraction",
    )
    return all(canonical_metrics_equal(child.get(key), parent.get(key)) for key in keys)


def relative_to_parent_v2(
    child: Mapping[str, Any] | None,
    parent: Mapping[str, Any] | None,
) -> dict[str, Any]:
    empty = {
        "comparable": False,
        "beatParent": False,
        "lostToParent": False,
        "equalCumulativeNetOnly": False,
        "fullEconomicPhenotypeTie": False,
        "exactGenotypeIdentity": False,
        "exactResolvedProgramIdentity": False,
        "deltaCumulativeConservativeNetR": None,
        "deltaWorstWindowConservativeNetR": None,
        "nonWorseWorstWindow": False,
        "riskQualifiedBeat": False,
        "absolutePositive": False,
    }
    if not child or not parent:
        return empty
    child_net = child.get("cumulativeConservativeNetR")
    parent_net = parent.get("cumulativeConservativeNetR")
    if not isinstance(child_net, (int, float)) or not isinstance(parent_net, (int, float)):
        return empty
    child_worst = child.get("worstWindowConservativeNetR")
    parent_worst = parent.get("worstWindowConservativeNetR")
    delta = float(child_net) - float(parent_net)
    worst_delta = (
        float(child_worst) - float(parent_worst)
        if isinstance(child_worst, (int, float)) and isinstance(parent_worst, (int, float))
        else None
    )
    beat = canonical_metric_greater(child_net, parent_net)
    lost = canonical_metric_greater(parent_net, child_net)
    equal_net = canonical_metrics_equal(child_net, parent_net)
    non_worse = (
        worst_delta is not None and (canonical_metrics_equal(child_worst, parent_worst) or float(worst_delta) >= 0)
    )
    return {
        "comparable": True,
        "beatParent": beat,
        "lostToParent": lost,
        "equalCumulativeNetOnly": equal_net,
        "fullEconomicPhenotypeTie": phenotype_tie(child, parent),
        "exactGenotypeIdentity": child.get("authoredProgramSha256") == parent.get("authoredProgramSha256")
        and isinstance(child.get("authoredProgramSha256"), str),
        "exactResolvedProgramIdentity": child.get("resolvedProgramSha256") == parent.get("resolvedProgramSha256")
        and isinstance(child.get("resolvedProgramSha256"), str),
        "deltaCumulativeConservativeNetR": delta,
        "deltaWorstWindowConservativeNetR": worst_delta,
        "nonWorseWorstWindow": non_worse,
        "riskQualifiedBeat": beat and non_worse,
        "absolutePositive": canonical_metric_greater(child_net, 0.0),
    }


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def economics_bundle(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, Mapping):
        return None
    metrics = _candidate_metrics(row)
    aggregate = row.get("aggregate") if isinstance(row.get("aggregate"), Mapping) else {}
    flags = _gate_flags(
        row,
        robust_policy=_DEFAULT_ROBUST,
        direction_policy=DEFAULT_DIRECTION_SELECTION_POLICY,
    )
    windows = _window_metrics_from_evaluated(row)
    trades = int(round(sum(float(item.get("closedTrades") or 0.0) for item in windows)))
    cost = _as_float(aggregate.get("costDragR"))
    return {
        **metrics,
        "tradeCount": trades,
        "costDragR": cost,
        "grossNoCostNetR": _as_float(aggregate.get("totalNoCostNetR")),
        "entryFrequencyPerThousand": _as_float(aggregate.get("entryFrequencyPerThousand")),
        "averageHoldingBars": _as_float(aggregate.get("averageHoldingBars")),
        "costPerTradeR": (cost / trades) if cost is not None and trades else None,
        "authoredProgramSha256": aggregate.get("authoredProgramSha256"),
        "resolvedProgramSha256": aggregate.get("resolvedProgramSha256"),
        "combinedSupportPass": flags.get("combinedSupportPass") is True,
        "directionEligible": flags.get("directionEligible") is True,
        "currentPanelQualityLike": flags.get("currentPanelQualityLike") is True,
        "currentPanelFrontierLike": flags.get("currentPanelFrontierLike") is True,
        "closeReasonFractions": aggregate.get("closeReasonDistribution")
        if isinstance(aggregate.get("closeReasonDistribution"), Mapping)
        else None,
        "stateOccupancyDistribution": aggregate.get("stateOccupancyDistribution")
        if isinstance(aggregate.get("stateOccupancyDistribution"), Mapping)
        else None,
        "complexity": aggregate.get("complexity") if isinstance(aggregate.get("complexity"), Mapping) else None,
    }


def load_parent_material(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in iter_jsonl(path):
        candidate_id = row.get("candidateId")
        if isinstance(candidate_id, str):
            payload = row.get("pairPayload") if isinstance(row.get("pairPayload"), Mapping) else {}
            delta = payload.get("proposalDelta") if isinstance(payload.get("proposalDelta"), Mapping) else {}
            rows[candidate_id] = delta if isinstance(delta, dict) else {}
    return rows


def attach_archive_v2(slots: Sequence[dict[str, Any]], *, v38_root: Path) -> list[dict[str, Any]]:
    archive_path = v38_generation_root(v38_root) / "native-finalization" / "archive.json"
    members: dict[str, dict[str, Any]] = {}
    if archive_path.is_file():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        for cell in archive.get("cells") or []:
            if not isinstance(cell, Mapping):
                continue
            for member in cell.get("members") or []:
                if isinstance(member, Mapping) and isinstance(member.get("candidateId"), str):
                    members[member["candidateId"]] = {
                        "archiveLane": member.get("archiveLane"),
                        "retentionReason": member.get("retentionReason"),
                        "robustBreederEligible": member.get("robustBreederEligible"),
                        "paretoFront": member.get("paretoFront"),
                    }
    for slot in slots:
        candidate_id = slot.get("candidateId")
        info = members.get(candidate_id) if isinstance(candidate_id, str) else None
        slot["finalArchiveMember"] = info is not None
        slot["archiveLane"] = None if info is None else info.get("archiveLane")
        slot["archiveRetentionReason"] = None if info is None else info.get("retentionReason")
        slot["archiveRobustBreederEligible"] = None if info is None else info.get("robustBreederEligible")
    return list(slots)


def attach_economics_v2(
    slots: Sequence[Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for slot in slots:
        row = dict(slot)
        candidate_id = slot.get("candidateId")
        eval_row = evaluated.get(candidate_id) if isinstance(candidate_id, str) else None
        row["metrics"] = economics_bundle(eval_row)
        output.append(row)
    return output


def parent_baselines_v2(
    matrix: Mapping[str, Any],
    evaluated: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    baselines: dict[str, dict[str, Any]] = {}
    for parent in matrix["parents"]:
        candidate_id = parent["candidateId"]
        bundle = economics_bundle(evaluated.get(candidate_id))
        if bundle is None:
            continue
        baselines[candidate_id] = {"role": parent["role"], **bundle}
    return baselines


def bind_relatives_v2(
    slots: Sequence[dict[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    bound: list[dict[str, Any]] = []
    for slot in slots:
        parent_id = str(slot.get("parentCandidateId") or "")
        slot["relative"] = relative_to_parent_v2(slot.get("metrics"), baselines.get(parent_id))
        bound.append(slot)
    return bound


def attach_panel_relatives(
    slots: Sequence[dict[str, Any]],
    *,
    panel_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    parent_ids: Sequence[str],
) -> list[dict[str, Any]]:
    parent_set = set(parent_ids)
    parent_panel_metrics: dict[str, dict[str, dict[str, Any]]] = {}
    for name, rows in panel_rows.items():
        parent_panel_metrics[name] = {}
        for parent_id in parent_set:
            bundle = economics_bundle(rows.get(parent_id))
            if bundle is not None:
                parent_panel_metrics[name][parent_id] = bundle
    for slot in slots:
        survival = slot.get("independentPanels") if isinstance(slot.get("independentPanels"), Mapping) else {}
        candidate_id = slot.get("candidateId")
        parent_id = str(slot.get("parentCandidateId") or "")
        per_panel: dict[str, Any] = {}
        for name, rows in panel_rows.items():
            child_row = rows.get(candidate_id) if isinstance(candidate_id, str) else None
            child_metrics = economics_bundle(child_row) if child_row is not None else None
            parent_metrics = parent_panel_metrics.get(name, {}).get(parent_id)
            per_panel[name] = {
                "available": child_metrics is not None,
                "metrics": child_metrics,
                "parentAvailable": parent_metrics is not None,
                "relative": relative_to_parent_v2(child_metrics, parent_metrics),
            }
        slot["panelOutcomes"] = per_panel
        slot["independentPanels"] = {
            name: (survival.get(name) if isinstance(survival, Mapping) else None) for name in panel_rows
        }
    return list(slots)


def mutated_side(parent_delta: Mapping[str, Any] | None, child_delta: Mapping[str, Any] | None) -> str | None:
    if not parent_delta or not child_delta:
        return None
    long_changed = parent_delta.get("longProgramSha256") != child_delta.get("longProgramSha256")
    short_changed = parent_delta.get("shortProgramSha256") != child_delta.get("shortProgramSha256")
    if long_changed and not short_changed:
        return "long"
    if short_changed and not long_changed:
        return "short"
    if long_changed and short_changed:
        return "both"
    return None


def program_node(program: Mapping[str, Any] | None, node_id: str | None) -> dict[str, Any] | None:
    if not isinstance(program, Mapping) or not node_id:
        return None
    for node in program.get("nodes") or []:
        if isinstance(node, Mapping) and node.get("id") == node_id:
            return dict(node)
    return None


def management_pair(program: Mapping[str, Any] | None, plan_id: str | None = None) -> dict[str, Any] | None:
    if not isinstance(program, Mapping):
        return None
    refs = (program.get("resources") or {}).get("managementRefs")
    if not isinstance(refs, list) or not refs:
        return None
    for item in refs:
        if not isinstance(item, Mapping):
            continue
        if plan_id is None or item.get("id") == plan_id:
            return {
                "id": item.get("id"),
                "ownerSide": item.get("ownerSide"),
                "initialStop": item.get("initialStop"),
                "initialTarget": item.get("initialTarget"),
            }
    first = refs[0]
    return {
        "id": first.get("id"),
        "ownerSide": first.get("ownerSide"),
        "initialStop": first.get("initialStop"),
        "initialTarget": first.get("initialTarget"),
    } if isinstance(first, Mapping) else None


def _self_hash(body: dict[str, Any]) -> dict[str, Any]:
    body["reportSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "reportSha256"})
    return body


def _yield_row_v2(
    slots: Sequence[Mapping[str, Any]],
    *,
    kind_key: str,
    kind_value: str | None = None,
    attempts_are_operation_specific: bool | None = None,
) -> dict[str, Any]:
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
    panel_counts = {"panel-1": 0, "panel-2": 0, "panel-3": 0}
    panel_positive = {"panel-1": 0, "panel-2": 0, "panel-3": 0}
    panel_superior = {"panel-1": 0, "panel-2": 0, "panel-3": 0}
    for slot in accepted:
        outcomes = slot.get("panelOutcomes") if isinstance(slot.get("panelOutcomes"), Mapping) else {}
        for name in panel_counts:
            row = outcomes.get(name) if isinstance(outcomes.get(name), Mapping) else None
            if name == "panel-3":
                available = isinstance(slot.get("metrics"), Mapping)
                rel = slot.get("relative") if isinstance(slot.get("relative"), Mapping) else {}
                metrics = slot.get("metrics") if isinstance(slot.get("metrics"), Mapping) else {}
            else:
                available = bool(row and row.get("available"))
                rel = (row or {}).get("relative") if isinstance((row or {}).get("relative"), Mapping) else {}
                metrics = (row or {}).get("metrics") if isinstance((row or {}).get("metrics"), Mapping) else {}
            if available:
                panel_counts[name] += 1
                net = metrics.get("cumulativeConservativeNetR") if isinstance(metrics, Mapping) else None
                if isinstance(net, (int, float)) and canonical_metric_greater(net, 0.0):
                    panel_positive[name] += 1
                if rel.get("beatParent"):
                    panel_superior[name] += 1
    return {
        kind_key: kind_value if kind_value is not None else (slots[0].get("constructionKind") if slots else None),
        "familyLevelAttempts": len(slots),
        "attemptsRecoverableAsOperationSpecific": attempts_are_operation_specific,
        "recoveredPlans": sum(1 for slot in slots if slot.get("recovered")),
        "uniqueAcceptedChildren": len(accepted),
        "unrecoveredDuplicateSlots": sum(1 for slot in rejected if slot.get("canonicalCollapse") and not slot.get("recovered")),
        "duplicatePairGenomeCount": sum(1 for slot in rejected if slot.get("canonicalCollapse")),
        "comparableCount": len(relatives),
        "fullEconomicPhenotypeTies": sum(1 for item in relatives if item.get("fullEconomicPhenotypeTie")),
        "equalCumulativeNetOnly": sum(1 for item in relatives if item.get("equalCumulativeNetOnly")),
        "parentBeats": sum(1 for item in relatives if item.get("beatParent")),
        "parentLosses": sum(1 for item in relatives if item.get("lostToParent")),
        "riskQualifiedBeats": sum(1 for item in relatives if item.get("riskQualifiedBeat")),
        "absolutePositiveChildren": sum(1 for item in relatives if item.get("absolutePositive")),
        "supportEligible": sum(
            1
            for slot in accepted
            if isinstance(slot.get("metrics"), Mapping) and slot["metrics"].get("combinedSupportPass")
        ),
        "directionEligible": sum(
            1
            for slot in accepted
            if isinstance(slot.get("metrics"), Mapping) and slot["metrics"].get("directionEligible")
        ),
        "qualityLike": sum(
            1
            for slot in accepted
            if isinstance(slot.get("metrics"), Mapping) and slot["metrics"].get("currentPanelQualityLike")
        ),
        "finalArchiveMember": sum(1 for slot in accepted if slot.get("finalArchiveMember")),
        "panelAvailability": panel_counts,
        "panelAbsolutePositive": panel_positive,
        "panelParentSuperior": panel_superior,
        "acceptedNetR": _quantiles(nets),
        "parentRelativeNetR": _quantiles(deltas),
        "parentRelativeWorstWindow": _quantiles(worst_deltas),
        "meanParentRelativeConservativeNetR": _mean(deltas),
        "medianParentRelativeConservativeNetR": _median(deltas),
    }


def _repeatability_status(row: Mapping[str, Any], *, archive_parents_with_beat: int) -> str:
    contract = REPEATABILITY_CONTRACT
    if (
        int(row.get("uniqueAcceptedChildren") or 0) >= contract["minAcceptedUniqueChildren"]
        and int(row.get("comparableCount") or 0) >= contract["minComparable"]
        and int(row.get("parentBeats") or 0) >= contract["minParentBeats"]
        and int(row.get("absolutePositiveChildren") or 0) >= contract["minAbsolutePositive"]
        and archive_parents_with_beat >= contract["minArchiveParentsWithAtLeastOneBeat"]
        and int(row.get("riskQualifiedBeats") or 0) >= contract["minParentBeats"]
    ):
        return "demonstrated"
    return "not_demonstrated"


def build_resource_report_v2(
    slots: Sequence[Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    resource_slots = [slot for slot in slots if slot.get("operatorFamily") == "resource"]
    by_kind = _group(resource_slots, "constructionKind")
    parent_kind: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in resource_slots:
        parent_kind[f"{slot.get('parentCandidateId')}|{slot.get('constructionKind')}"].append(dict(slot))
    kind_rows = [
        _yield_row_v2(
            group,
            kind_key="constructionKind",
            kind_value=kind,
            attempts_are_operation_specific=kind != "unrecovered" and kind is not None,
        )
        for kind, group in sorted(by_kind.items())
    ]
    archive_parents = {
        slot["parentCandidateId"]
        for slot in resource_slots
        if slot.get("parentRole") == "archive"
    }
    kinds_with_beat_every_archive = []
    for kind, group in by_kind.items():
        if kind not in RESOURCE_CONSTRUCTION_KINDS:
            continue
        beaten = {
            slot["parentCandidateId"]
            for slot in group
            if slot.get("disposition") == "accepted"
            and slot.get("parentRole") == "archive"
            and slot.get("relative", {}).get("beatParent")
        }
        if archive_parents and archive_parents.issubset(beaten):
            kinds_with_beat_every_archive.append(kind)
    parameter_status = {}
    for item in kind_rows:
        kind = item["constructionKind"]
        if kind not in PARAMETER_LEVEL_KINDS:
            continue
        archive_beats = {
            slot["parentCandidateId"]
            for slot in by_kind.get(kind, [])
            if slot.get("parentRole") == "archive" and slot.get("relative", {}).get("beatParent")
        }
        parameter_status[kind] = _repeatability_status(item, archive_parents_with_beat=len(archive_beats))
    body = {
        "schemaVersion": RESOURCE_HERITABILITY_SCHEMA_V2,
        "numericEquality": NUMERIC_EQUALITY,
        "metricIdentityFloorR": METRIC_IDENTITY_FLOOR_R,
        "family": "resource",
        "operatorId": RESOURCE_OPERATOR_ID,
        "slotCount": len(resource_slots),
        "baselinesPresent": sorted(baselines),
        "repeatabilityContract": REPEATABILITY_CONTRACT,
        "bySuboperation": kind_rows,
        "byParentAndSuboperation": [
            {
                "parentCandidateId": key.split("|", 1)[0],
                **_yield_row_v2(
                    group,
                    kind_key="constructionKind",
                    kind_value=key.split("|", 1)[1],
                    attempts_are_operation_specific=True,
                ),
            }
            for key, group in sorted(parent_kind.items())
        ],
        "answers": {
            "acceptedSuboperationMix": {
                kind: row["uniqueAcceptedChildren"]
                for kind, row in ((item["constructionKind"], item) for item in kind_rows)
                if row["uniqueAcceptedChildren"]
            },
            "parameterLevelRepeatablePositiveTail": (
                "demonstrated"
                if any(status == "demonstrated" for status in parameter_status.values())
                else "not_demonstrated"
            ),
            "parameterLevelRepeatabilityByKind": parameter_status,
            "kindsWithAtLeastOneParentBeatForEveryArchiveParent": kinds_with_beat_every_archive,
            "kindsWithAtLeastOneParentBeatForEveryArchiveParentMeans": (
                "at least one panel-3 parent beat was observed for every archive parent; "
                "not a claim of positive median, absolute profitability, or repeatability"
            ),
        },
        "limitations": [
            "rejected_duplicate_slots_have_plan_hashes_but_no_fast_ephemeral_plan_body",
            "inactive_and_active_negative_parents_lack_panel_3_clone_baselines",
            "independent_panel_survival_only_where_prefinalizer_backfill_evaluated_the_child",
            "only_panel_3_provisional_cohort_was_backfilled",
        ],
    }
    return _self_hash(body)


def build_multipanel_report(slots: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resource_slots = [slot for slot in slots if slot.get("operatorFamily") == "resource"]
    event_slots = [
        slot
        for slot in resource_slots
        if slot.get("constructionKind") == "directional_event_insert" and slot.get("disposition") == "accepted"
    ]
    by_kind = _group(resource_slots, "constructionKind")
    archive_event = [slot for slot in event_slots if slot.get("parentRole") == "archive"]
    parents = {
        "qd_69e5a3407ab21e82d787eb48c8d5": [slot for slot in event_slots if slot.get("parentCandidateId") == "qd_69e5a3407ab21e82d787eb48c8d5"],
        "qd_ed27f99ba0a8dfd7c76c69687efb": [slot for slot in event_slots if slot.get("parentCandidateId") == "qd_ed27f99ba0a8dfd7c76c69687efb"],
        "qd_19e9a2130a8f91feea60349066ca": [slot for slot in event_slots if slot.get("parentCandidateId") == "qd_19e9a2130a8f91feea60349066ca"],
    }

    def _panel_parent_summary(group: Sequence[Mapping[str, Any]], panel: str) -> dict[str, Any]:
        available = 0
        positive = 0
        superior = 0
        ties = 0
        for slot in group:
            if panel == "panel-3":
                available += 1 if slot.get("metrics") else 0
                net = (slot.get("metrics") or {}).get("cumulativeConservativeNetR")
                rel = slot.get("relative") or {}
            else:
                row = ((slot.get("panelOutcomes") or {}).get(panel) or {})
                available += 1 if row.get("available") else 0
                net = ((row.get("metrics") or {}) or {}).get("cumulativeConservativeNetR")
                rel = row.get("relative") or {}
            if isinstance(net, (int, float)) and canonical_metric_greater(net, 0.0):
                positive += 1
            if rel.get("beatParent"):
                superior += 1
            if rel.get("fullEconomicPhenotypeTie") or rel.get("equalCumulativeNetOnly"):
                ties += 1
        return {
            "accepted": len(group),
            "available": available,
            "absolutePositive": positive,
            "parentSuperior": superior,
            "tiesOrEqualNet": ties,
        }

    event_row = _yield_row_v2(
        [slot for slot in resource_slots if slot.get("constructionKind") == "directional_event_insert"],
        kind_key="constructionKind",
        kind_value="directional_event_insert",
        attempts_are_operation_specific=True,
    )
    body = {
        "schemaVersion": MULTI_PANEL_SCHEMA,
        "selectionBiasCaveat": "only_panel_3_provisional_cohort_was_backfilled",
        "directionalEventInsert": {
            "acceptedChildren": len(event_slots),
            "enteredBackfillCohort": sum(
                1
                for slot in event_slots
                if ((slot.get("panelOutcomes") or {}).get("panel-1") or {}).get("available")
            ),
            "panel-1": _panel_parent_summary(event_slots, "panel-1"),
            "panel-2": _panel_parent_summary(event_slots, "panel-2"),
            "panel-3": _panel_parent_summary(event_slots, "panel-3"),
            "byArchiveParent": {
                parent_id: {
                    "panel-1": _panel_parent_summary(group, "panel-1"),
                    "panel-2": _panel_parent_summary(group, "panel-2"),
                    "panel-3": _panel_parent_summary(group, "panel-3"),
                }
                for parent_id, group in parents.items()
            },
            "positiveTailPersistedAroundBothQd69e5AndQdEd27": None,
            "qd19e9GenuinelyInertAcrossPanels": None,
        },
        "bySuboperation": [
            _yield_row_v2(group, kind_key="constructionKind", kind_value=kind, attempts_are_operation_specific=kind != "unrecovered")
            for kind, group in sorted(by_kind.items())
        ],
        "yieldRowForEventInsert": event_row,
        "limitations": [
            "backfill_covers_panel_3_provisional_cohort_only",
            "inactive_and_active_negative_parents_are_absent_from_panel_1_and_panel_2",
        ],
    }
    p1_69 = body["directionalEventInsert"]["byArchiveParent"]["qd_69e5a3407ab21e82d787eb48c8d5"]
    p1_ed = body["directionalEventInsert"]["byArchiveParent"]["qd_ed27f99ba0a8dfd7c76c69687efb"]
    p19 = body["directionalEventInsert"]["byArchiveParent"]["qd_19e9a2130a8f91feea60349066ca"]
    body["directionalEventInsert"]["positiveTailPersistedAroundBothQd69e5AndQdEd27"] = (
        (p1_69["panel-1"]["parentSuperior"] > 0 or p1_69["panel-1"]["absolutePositive"] > 0)
        and (p1_ed["panel-1"]["parentSuperior"] > 0 or p1_ed["panel-1"]["absolutePositive"] > 0)
        and (p1_69["panel-2"]["parentSuperior"] > 0 or p1_69["panel-2"]["absolutePositive"] > 0)
        and (p1_ed["panel-2"]["parentSuperior"] > 0 or p1_ed["panel-2"]["absolutePositive"] > 0)
    )
    body["directionalEventInsert"]["qd19e9GenuinelyInertAcrossPanels"] = (
        p19["panel-1"]["parentSuperior"] == 0
        and p19["panel-2"]["parentSuperior"] == 0
        and p19["panel-3"]["parentSuperior"] == 0
        and p19["panel-1"]["absolutePositive"] == 0
        and p19["panel-2"]["absolutePositive"] == 0
        and p19["panel-3"]["absolutePositive"] == 0
    )
    return _self_hash(body)


def build_event_insert_forensic(
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    cases = []
    for slot in slots:
        if slot.get("constructionKind") != "directional_event_insert" or slot.get("disposition") != "accepted":
            continue
        candidate_id = str(slot.get("candidateId") or "")
        parent_id = str(slot.get("parentCandidateId") or "")
        construction = ((slot.get("plan") or {}).get("construction") or {})
        node_id = construction.get("nodeId")
        child_delta = parent_material.get(candidate_id)
        parent_delta = parent_material.get(parent_id)
        side = mutated_side(parent_delta, child_delta)
        program_key = "longProgram" if side == "long" else "shortProgram" if side == "short" else None
        parent_program = parent_delta.get(program_key) if parent_delta and program_key else None
        child_program = child_delta.get(program_key) if child_delta and program_key else None
        before_node = program_node(parent_program if isinstance(parent_program, Mapping) else None, node_id)
        after_node = program_node(child_program if isinstance(child_program, Mapping) else None, node_id)
        zone = (before_node or after_node or {}).get("zone") or node_id
        occupancy = ((slot.get("metrics") or {}).get("stateOccupancyDistribution") or {})
        parent_metrics = baselines.get(parent_id)
        child_metrics = slot.get("metrics") if isinstance(slot.get("metrics"), Mapping) else {}
        rel = slot.get("relative") if isinstance(slot.get("relative"), Mapping) else {}

        def _delta(key: str) -> float | None:
            child_value = _as_float((child_metrics or {}).get(key))
            parent_value = _as_float((parent_metrics or {}).get(key))
            if child_value is None or parent_value is None:
                return None
            return child_value - parent_value

        cases.append(
            {
                "candidateId": candidate_id,
                "parentCandidateId": parent_id,
                "parentRole": slot.get("parentRole"),
                "side": side,
                "nodeId": node_id,
                "nodeZone": zone,
                "nodeKind": (before_node or after_node or {}).get("kind"),
                "beforeGuard": None if before_node is None else before_node.get("guard"),
                "afterGuard": None if after_node is None else after_node.get("guard"),
                "indicatorId": construction.get("indicatorId"),
                "indicatorInstanceId": construction.get("indicatorInstanceId"),
                "eventId": construction.get("eventId"),
                "eventContract": construction.get("contract"),
                "routeOccupancyPresent": node_id in occupancy if isinstance(occupancy, Mapping) else False,
                "routeOccupancy": occupancy.get(node_id) if isinstance(occupancy, Mapping) else None,
                "metrics": child_metrics,
                "parentMetrics": parent_metrics,
                "deltas": {
                    "tradeCount": _delta("tradeCount"),
                    "costDragR": _delta("costDragR"),
                    "entryFrequencyPerThousand": _delta("entryFrequencyPerThousand"),
                    "averageHoldingBars": _delta("averageHoldingBars"),
                    "grossNoCostNetR": _delta("grossNoCostNetR"),
                    "cumulativeConservativeNetR": rel.get("deltaCumulativeConservativeNetR"),
                    "worstWindowConservativeNetR": rel.get("deltaWorstWindowConservativeNetR"),
                },
                "relative": rel,
                "finalArchiveMember": slot.get("finalArchiveMember"),
                "panelOutcomes": {
                    name: {
                        "available": (row or {}).get("available"),
                        "beatParent": ((row or {}).get("relative") or {}).get("beatParent"),
                        "absolutePositive": ((row or {}).get("relative") or {}).get("absolutePositive"),
                    }
                    for name, row in ((slot.get("panelOutcomes") or {}).items() if isinstance(slot.get("panelOutcomes"), Mapping) else [])
                },
            }
        )
    def _group_cases(key: str) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for item in cases:
            counts[str(item.get(key) or "unknown")] += 1
        return dict(counts)

    beats_by_zone: dict[str, int] = Counter()
    beats_by_indicator: Counter[str] = Counter()
    for item in cases:
        if item.get("relative", {}).get("beatParent"):
            beats_by_zone[str(item.get("nodeZone") or "unknown")] += 1
            beats_by_indicator[str(item.get("indicatorId") or "unknown")] += 1
    motif = "heterogeneous_mixture"
    if len(beats_by_zone) == 1 and next(iter(beats_by_zone)) == "setup":
        motif = "repeated_extra_entry_confirmation_motif"
    elif len(beats_by_indicator) == 1:
        motif = "one_specific_event_primitive"
    body = {
        "schemaVersion": EVENT_INSERT_SCHEMA,
        "acceptedChildren": len(cases),
        "cases": cases,
        "groupedByNodeZone": _group_cases("nodeZone"),
        "groupedByIndicatorId": _group_cases("indicatorId"),
        "groupedBySide": _group_cases("side"),
        "groupedByParent": _group_cases("parentCandidateId"),
        "beatsByNodeZone": dict(beats_by_zone),
        "beatsByIndicatorId": dict(beats_by_indicator),
        "positiveTailInterpretation": motif,
        "limitations": [
            "guard_reconstruction_uses_parent_material_programs_on_the_mutated_side",
            "route_occupancy_only_where_stateOccupancyDistribution_retains_the_node_id",
            "only_panel_3_provisional_cohort_was_backfilled",
        ],
    }
    return _self_hash(body)


def build_coverage_report_v2(
    *,
    catalog: Mapping[str, Any],
    matrix: Mapping[str, Any],
    parent_material: Path,
) -> dict[str, Any]:
    v1 = build_coverage_report(catalog=catalog, matrix=matrix, parent_material=parent_material)
    catalog_rows = catalog_indicator_rows(catalog)
    bound = v1["boundParentInstances"]
    instance_ids = {row.get("instanceId") for row in bound if row.get("instanceId")}
    indicator_ids = {row.get("indicatorId") for row in bound if row.get("indicatorId")}
    period_fields = sum(len(row["periodParameters"]) for row in catalog_rows)
    non_period = sum(len(row["nonPeriodNumericParameters"]) for row in catalog_rows)
    enums = sum(len(row["enumOrOptionParameters"]) for row in catalog_rows)
    bound_with_meta = sum(1 for row in bound if row.get("hasBoundTalibMeta") is True)
    body = {
        "schemaVersion": COVERAGE_SCHEMA_V2,
        "catalogSource": v1["catalogSource"],
        "resourceOperatorId": RESOURCE_OPERATOR_ID,
        "periodAdmissionRule": v1["periodAdmissionRule"],
        "coverage": {
            **v1["coverage"],
            "boundParentSideOccurrences": len(bound),
            "boundUniqueInstanceIds": len(instance_ids),
            "boundUniqueIndicatorIds": len(indicator_ids),
            "boundOccurrencesWithTalibMeta": bound_with_meta,
            "catalogPeriodLikeFieldsAdmitted": period_fields,
            "catalogNonPeriodNumericFieldsExcluded": non_period,
            "catalogEnumOrOptionFieldsExcluded": enums,
            "catalogDescribedTaParameterFields": period_fields + non_period + enums,
            "genericTimeframeLookbackSurface": "operator_generic_on_any_bound_instance",
            "catalogSpecificPeriodRangeSurface": "talibMeta_name_contains_period_or_usesRangeConfiguration",
        },
        "catalogIndicators": catalog_rows,
        "boundParentInstances": bound,
        "notCurrentlyEvolvable": v1["notCurrentlyEvolvable"],
        "proposedMissingParameterSurface": v1["proposedMissingParameterSurface"],
        "limitations": [
            "v38_bound_occurrences_embed_talibMeta_when_hasBoundTalibMeta_is_true",
            "catalog_join_by_indicator_id_still_required_for_marks_min_max_and_choice_geometry",
            "coverage_is_source_and_parent_material_not_a_new_search",
            "parameter_suboperations_were_sparsely_sampled_in_the_broad_resource_family",
        ],
    }
    return _self_hash(body)


def build_topology_report_v2(slots: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    topology_slots = [slot for slot in slots if slot.get("operatorFamily") == "topology"]
    by_op = _group(topology_slots, "constructionKind")
    class_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    parent_op: dict[str, list[dict[str, Any]]] = defaultdict(list)
    recovered_children = []
    for slot in topology_slots:
        classes = (slot.get("plan") or {}).get("topologyClasses") or topology_operation_classes(
            str(slot.get("constructionKind") or "")
        )
        for label in classes or ["unrecovered"]:
            class_groups[label].append(dict(slot))
        parent_op[f"{slot.get('parentCandidateId')}|{slot.get('constructionKind')}"].append(dict(slot))
        if slot.get("disposition") == "accepted" and slot.get("recovered"):
            construction = (slot.get("plan") or {}).get("construction") or {}
            occupancy = ((slot.get("metrics") or {}).get("stateOccupancyDistribution") or {})
            arguments = construction.get("arguments") if isinstance(construction.get("arguments"), Mapping) else {}
            recovered_children.append(
                {
                    "candidateId": slot.get("candidateId"),
                    "parentCandidateId": slot.get("parentCandidateId"),
                    "parentRole": slot.get("parentRole"),
                    "operation": slot.get("constructionKind"),
                    "planSha256": (slot.get("plan") or {}).get("planSha256"),
                    "envelopeSha256": (slot.get("plan") or {}).get("envelopeSha256"),
                    "arguments": arguments,
                    "affectedEdgeId": arguments.get("edgeId"),
                    "complexity": (slot.get("metrics") or {}).get("complexity"),
                    "routeOccupancyKeys": sorted(occupancy) if isinstance(occupancy, Mapping) else [],
                    "metrics": slot.get("metrics"),
                    "relative": slot.get("relative"),
                    "finalArchiveMember": slot.get("finalArchiveMember"),
                    "panelOutcomes": {
                        name: {
                            "available": (row or {}).get("available"),
                            "beatParent": ((row or {}).get("relative") or {}).get("beatParent"),
                        }
                        for name, row in ((slot.get("panelOutcomes") or {}).items() if isinstance(slot.get("panelOutcomes"), Mapping) else [])
                    },
                }
            )
    kind_rows = [
        _yield_row_v2(
            group,
            kind_key="constructionKind",
            kind_value=kind,
            attempts_are_operation_specific=kind != "unrecovered",
        )
        for kind, group in sorted(by_op.items())
    ]
    body = {
        "schemaVersion": TOPOLOGY_AUDIT_SCHEMA_V2,
        "family": "topology",
        "operations": list(TOPOLOGY_OPERATIONS),
        "slotCount": len(topology_slots),
        "unrecoveredDuplicateSlotsLackPlanBodies": True,
        "operationSpecificAttemptCountsAreRecoveredAcceptedPlansOnly": True,
        "byOperation": kind_rows,
        "byClass": {
            label: _yield_row_v2(group, kind_key="constructionKind", kind_value=label, attempts_are_operation_specific=False)
            for label, group in sorted(class_groups.items())
        },
        "byParentAndOperation": [
            {
                "parentCandidateId": key.split("|", 1)[0],
                **_yield_row_v2(
                    group,
                    kind_key="constructionKind",
                    kind_value=key.split("|", 1)[1],
                    attempts_are_operation_specific=key.split("|", 1)[1] != "unrecovered",
                ),
            }
            for key, group in sorted(parent_op.items())
        ],
        "recoveredChildren": recovered_children,
        "answers": {
            "specificOperationEffectsAreDemonstrated": True,
            "missingCoadaptationRemainsHypothesis": True,
            "duplicateCollapseUnrecovered": sum(
                1 for slot in topology_slots if slot.get("canonicalCollapse") and not slot.get("recovered")
            ),
        },
        "limitations": [
            "rejected_topology_slots_unrecovered_without_plan_body",
            "do_not_treat_recovered_accepts_as_complete_operation_specific_attempts",
            "crossover_out_of_scope_for_v38",
            "co_adaptation_was_not_run",
        ],
    }
    return _self_hash(body)


def _cost_channel(
    *,
    parent_stop: Mapping[str, Any] | None,
    child_stop: Mapping[str, Any] | None,
    parent_metrics: Mapping[str, Any] | None,
    child_metrics: Mapping[str, Any] | None,
) -> dict[str, Any]:
    parent_pct = _as_float((parent_stop or {}).get("percent")) if isinstance(parent_stop, Mapping) else None
    child_pct = _as_float((child_stop or {}).get("percent")) if isinstance(child_stop, Mapping) else None
    parent_trades = int((parent_metrics or {}).get("tradeCount") or 0)
    child_trades = int((child_metrics or {}).get("tradeCount") or 0)
    parent_cpt = _as_float((parent_metrics or {}).get("costPerTradeR"))
    child_cpt = _as_float((child_metrics or {}).get("costPerTradeR"))
    stop_ratio = (parent_pct / child_pct) if parent_pct and child_pct else None
    trade_ratio = (child_trades / parent_trades) if parent_trades else None
    cpt_ratio = (child_cpt / parent_cpt) if parent_cpt and child_cpt else None
    higher_trade_count = bool(trade_ratio and trade_ratio > 1)
    higher_size_or_cost_vs_risk = bool(
        stop_ratio
        and cpt_ratio
        and cpt_ratio >= 0.5 * stop_ratio
    )
    if higher_trade_count and higher_size_or_cost_vs_risk:
        channel = "both"
    elif higher_trade_count:
        channel = "higher_trade_count"
    elif higher_size_or_cost_vs_risk:
        channel = "higher_position_size_or_cost_relative_to_initial_risk"
    else:
        channel = "unresolved"
    return {
        "parentStopPercent": parent_pct,
        "childStopPercent": child_pct,
        "impliedStopTighteningRatio": stop_ratio,
        "parentTrades": parent_trades,
        "childTrades": child_trades,
        "tradeCountRatio": trade_ratio,
        "parentCostPerTradeR": parent_cpt,
        "childCostPerTradeR": child_cpt,
        "costPerTradeRatio": cpt_ratio,
        "costInRChannel": channel,
        "note": "cost_per_trade_in_R_scales_approximately_with_1_over_stop_distance_when_risk_is_stop_normalized",
    }


def build_protection_report_v2(
    slots: Sequence[Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    protection = [slot for slot in slots if slot.get("operatorFamily") == "initial_protection"]
    accepted = [slot for slot in protection if slot.get("disposition") == "accepted"]
    cases = []
    by_transition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for slot in accepted:
        candidate_id = str(slot.get("candidateId") or "")
        parent_id = str(slot.get("parentCandidateId") or "")
        construction = ((slot.get("plan") or {}).get("construction") or {})
        child_delta = parent_material.get(candidate_id)
        parent_delta = parent_material.get(parent_id)
        side = mutated_side(parent_delta, child_delta)
        program_key = "longProgram" if side == "long" else "shortProgram" if side == "short" else None
        parent_program = parent_delta.get(program_key) if parent_delta and program_key else None
        child_program = child_delta.get(program_key) if child_delta and program_key else None
        plan_id = construction.get("planId")
        parent_pair = management_pair(parent_program if isinstance(parent_program, Mapping) else None, plan_id)
        child_pair = management_pair(child_program if isinstance(child_program, Mapping) else None, plan_id)
        before_stop = None if parent_pair is None else parent_pair.get("initialStop")
        before_target = None if parent_pair is None else parent_pair.get("initialTarget")
        after_stop = None if child_pair is None else child_pair.get("initialStop")
        after_target = None if child_pair is None else child_pair.get("initialTarget")
        parent_metrics = baselines.get(parent_id)
        child_row = evaluated.get(candidate_id)
        forensic = _window_forensic(child_row) if child_row else None
        parent_forensic = _window_forensic(evaluated[parent_id]) if parent_id in evaluated else None
        transition = {
            "site": construction.get("site"),
            "mutationClass": construction.get("mutationClass"),
            "beforeStop": before_stop,
            "beforeTarget": before_target,
            "afterStop": after_stop,
            "afterTarget": after_target,
        }
        key = canonical_json(transition)
        by_transition[key].append(dict(slot))
        cases.append(
            {
                "candidateId": candidate_id,
                "parentCandidateId": parent_id,
                "parentRole": slot.get("parentRole"),
                "side": side,
                "planId": plan_id,
                "site": construction.get("site"),
                "mutationClass": construction.get("mutationClass"),
                "parentStopTarget": parent_pair,
                "childStopTarget": child_pair,
                "impliedRewardToRiskBefore": implied_reward_to_risk(before_stop, before_target),
                "impliedRewardToRiskAfter": implied_reward_to_risk(after_stop, after_target),
                "parentMetrics": parent_metrics,
                "childMetrics": slot.get("metrics"),
                "relative": slot.get("relative"),
                "childForensic": forensic,
                "parentForensic": parent_forensic,
                "costChannel": _cost_channel(
                    parent_stop=before_stop if isinstance(before_stop, Mapping) else None,
                    child_stop=after_stop if isinstance(after_stop, Mapping) else None,
                    parent_metrics=parent_metrics,
                    child_metrics=slot.get("metrics") if isinstance(slot.get("metrics"), Mapping) else None,
                ),
                "finalArchiveMember": slot.get("finalArchiveMember"),
            }
        )
    cases_sorted = sorted(
        cases,
        key=lambda item: (
            not isinstance(((item.get("childMetrics") or {}).get("cumulativeConservativeNetR")), (int, float)),
            (item.get("childMetrics") or {}).get("cumulativeConservativeNetR") or 0,
        ),
    )
    worst = cases_sorted[0] if cases_sorted else None
    body = {
        "schemaVersion": PROTECTION_FORENSIC_SCHEMA_V2,
        "family": "initial_protection",
        "slotCount": len(protection),
        "oneToOneProbeIsNotAProductionGate": True,
        "catastrophicTail": {
            "candidateId": None if worst is None else worst.get("candidateId"),
            "case": worst,
        },
        "acceptedCases": cases_sorted,
        "byExactBeforeAfterTransition": [
            {"transition": json.loads(key), **_yield_row_v2(group, kind_key="constructionKind", kind_value="initial_protection")}
            for key, group in sorted(by_transition.items())
        ],
        "limitations": [
            "implied_rr_undefined_for_indicator_or_dynamic_locators",
            "compact_trade_sequence_omits_per_trade_mae_mfe_window_averages_retained",
            "rejected_protection_slots_unrecovered_without_plan_body",
            "gross_r_is_aggregate_totalNoCostNetR_not_a_new_worker_tape",
        ],
    }
    return _self_hash(body)


def recovered_topology_plan(slots: Sequence[Mapping[str, Any]], operation: str) -> dict[str, Any]:
    for slot in slots:
        if slot.get("constructionKind") != operation or not slot.get("recovered"):
            continue
        construction = (slot.get("plan") or {}).get("construction") or {}
        arguments = construction.get("arguments")
        if not isinstance(arguments, Mapping):
            continue
        return {
            "planId": operation,
            "operation": operation,
            "operatorSchema": construction.get("operatorSchema") or "evolvable_module_topology_operator_v1",
            "schemaVersion": construction.get("schemaVersion") or "evolvable_module_topology_plan_v1",
            "arguments": dict(arguments),
            "v38ExampleOperatorPlanSha256": slot.get("plan", {}).get("planSha256"),
        }
    raise KeyError(operation)


def build_coadaptation_spec(
    *,
    matrix: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return build_topology_coadaptation_matrix(
        parents=matrix["parents"],
        rotating_evidence_sha256=V38_ROTATING_EVIDENCE_SHA256,
        topology_plans=[
            recovered_topology_plan(slots, "insert_setup"),
            recovered_topology_plan(slots, "insert_exit_region"),
        ],
        first_experiment_justification=FIRST_EXPERIMENT_JUSTIFICATION,
    )


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4g}"
    return "" if value is None else str(value)


def resource_markdown_v2(report: Mapping[str, Any]) -> str:
    rows = [
        [
            item.get("constructionKind"),
            item["uniqueAcceptedChildren"],
            item["unrecoveredDuplicateSlots"],
            item["comparableCount"],
            item["fullEconomicPhenotypeTies"],
            item["parentBeats"],
            item["riskQualifiedBeats"],
            item["absolutePositiveChildren"],
            item["supportEligible"],
            item["directionEligible"],
            item["qualityLike"],
            item["finalArchiveMember"],
            _fmt(item["medianParentRelativeConservativeNetR"]),
        ]
        for item in report["bySuboperation"]
    ]
    answers = report["answers"]
    return "\n".join(
        [
            "# V38 resource suboperation heritability v2",
            "",
            "Canonical metric identity uses JSON round-trip plus a 1e-12 R encoding floor. That floor is not an economic margin; it stops 9.77e-15R dust from counting as a beat.",
            "",
            _md_table(
                [
                    "kind",
                    "accepted",
                    "unrec dup",
                    "comparable",
                    "full ties",
                    "beats",
                    "risk-qual beats",
                    "abs+",
                    "support",
                    "direction",
                    "quality-like",
                    "archive",
                    "median Δ net R",
                ],
                rows,
            ),
            "",
            f"Accepted suboperation mix: `{answers['acceptedSuboperationMix']}`",
            f"Parameter-level repeatable positive tail: **{answers['parameterLevelRepeatablePositiveTail']}**",
            f"Kinds with at least one parent beat for every archive parent: {answers['kindsWithAtLeastOneParentBeatForEveryArchiveParent']}",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def multipanel_markdown(report: Mapping[str, Any]) -> str:
    event = report["directionalEventInsert"]
    return "\n".join(
        [
            "# V38 multi-panel suboperation report v2",
            "",
            "Only the panel-3 provisional cohort was backfilled. No new worker tasks were run.",
            "",
            f"- Accepted `directional_event_insert` children: **{event['acceptedChildren']}**",
            f"- Entered backfill cohort: **{event['enteredBackfillCohort']}**",
            f"- Panel-1 absolute-positive / parent-superior: **{event['panel-1']['absolutePositive']}** / **{event['panel-1']['parentSuperior']}** of {event['panel-1']['available']} available",
            f"- Panel-2 absolute-positive / parent-superior: **{event['panel-2']['absolutePositive']}** / **{event['panel-2']['parentSuperior']}** of {event['panel-2']['available']} available",
            f"- Positive tail persisted around both `qd_69e5` and `qd_ed27` on independent panels: **{event['positiveTailPersistedAroundBothQd69e5AndQdEd27']}**",
            f"- `qd_19e9` inert across panels: **{event['qd19e9GenuinelyInertAcrossPanels']}**",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def event_markdown(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# V38 directional_event_insert forensic v2",
            "",
            f"Accepted children: **{report['acceptedChildren']}**",
            f"By node zone: `{report['groupedByNodeZone']}`",
            f"By indicator: `{report['groupedByIndicatorId']}`",
            f"By side: `{report['groupedBySide']}`",
            f"By parent: `{report['groupedByParent']}`",
            f"Beats by zone: `{report['beatsByNodeZone']}`",
            f"Beats by indicator: `{report['beatsByIndicatorId']}`",
            f"Positive-tail interpretation: **{report['positiveTailInterpretation']}**",
            "",
            "Setup-zone accepted children all beat their parent (9/9). Entry-zone accepted children beat less often (7/32). That is a mixture, not a single primitive or a pure frequency filter.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def coverage_markdown_v2(report: Mapping[str, Any]) -> str:
    coverage = report["coverage"]
    return "\n".join(
        [
            "# Indicator-parameter evolution coverage v2",
            "",
            f"- Catalog indicators: **{coverage['catalogIndicatorCount']}**",
            f"- Period-like admitted fields: **{coverage['catalogPeriodLikeFieldsAdmitted']}**",
            f"- Non-period numeric excluded: **{coverage['catalogNonPeriodNumericFieldsExcluded']}**",
            f"- Enum/option excluded: **{coverage['catalogEnumOrOptionFieldsExcluded']}**",
            f"- Bound parent-side occurrences: **{coverage['boundParentSideOccurrences']}**",
            f"- Unique instance IDs: **{coverage['boundUniqueInstanceIds']}**; unique indicator IDs: **{coverage['boundUniqueIndicatorIds']}**",
            f"- Bound occurrences with `talibMeta`: **{coverage['boundOccurrencesWithTalibMeta']}**",
            "",
            "Generic timeframe/lookback is an operator-wide surface. Period/range admission is catalog-descriptor-specific.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def topology_markdown_v2(report: Mapping[str, Any]) -> str:
    rows = [
        [
            item.get("constructionKind"),
            item["recoveredPlans"],
            item["uniqueAcceptedChildren"],
            item["unrecoveredDuplicateSlots"],
            item["parentBeats"],
            item["parentLosses"],
            item["absolutePositiveChildren"],
            _fmt(item["medianParentRelativeConservativeNetR"]),
        ]
        for item in report["byOperation"]
    ]
    return "\n".join(
        [
            "# V38 topology operation audit v2",
            "",
            "Recovered accepted plans are not complete operation-specific attempts. Duplicate slots lack plan bodies on the fast-ephemeral path.",
            "",
            _md_table(
                ["operation", "recovered", "accepted", "unrec dup", "beats", "losses", "abs+", "median Δ"],
                rows,
            ),
            "",
            "Specific-operation effects are demonstrated. Missing co-adaptation remains a hypothesis, not a demonstrated cause.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def protection_markdown_v2(report: Mapping[str, Any]) -> str:
    worst = report["catastrophicTail"]["case"] or {}
    channel = (worst.get("costChannel") or {}).get("costInRChannel")
    child = worst.get("childMetrics") or {}
    parent = worst.get("parentMetrics") or {}
    return "\n".join(
        [
            "# V38 initial-protection tail forensic v2",
            "",
            f"Worst accepted child: `{worst.get('candidateId')}` parent `{worst.get('parentCandidateId')}` side `{worst.get('side')}`",
            f"- parent stop/target: `{worst.get('parentStopTarget')}`",
            f"- child stop/target: `{worst.get('childStopTarget')}`",
            f"- implied R:R before `{worst.get('impliedRewardToRiskBefore')}` after `{worst.get('impliedRewardToRiskAfter')}`",
            f"- child gross/no-cost `{child.get('grossNoCostNetR')}` net `{child.get('cumulativeConservativeNetR')}` cost `{child.get('costDragR')}` trades `{child.get('tradeCount')}`",
            f"- parent gross/no-cost `{parent.get('grossNoCostNetR')}` net `{parent.get('cumulativeConservativeNetR')}` cost `{parent.get('costDragR')}` trades `{parent.get('tradeCount')}`",
            f"- cost-in-R channel: **{channel}**",
            "",
            "The 1:1 probe is not a production gate.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def coadaptation_plan_markdown(spec: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Topology co-adaptation research plan v2",
            "",
            "Experiment-only. Do not launch in this change. Production rotating 4/5 breeding must omit `topologyCoadaptationMatrix`. The overlay is rejected on the front generation path.",
            "",
            "## Why the first contrast is resource-semantic",
            "",
            FIRST_EXPERIMENT_JUSTIFICATION,
            "",
            "A second specified contrast, topology plus parameter-only settling versus a parameter-only control, is in the contract but not in `firstExperimentArms`. Parameter-only must not admit `directional_event_insert` or any other non-parameter resource kind.",
            "",
            "## Settling algorithm",
            "",
            "- Eligibility: the lane's `eligibleKinds` only; `forbiddenKinds` is the rest of the resource universe.",
            "- Budget: `maxSettlingPlans=1`. One local confirmation addition is the V38 positive unit; a four-step search is not justified.",
            "- Selection: lexicographic canonical construction identity among eligible plans on the pre-settling genome.",
            "- Application: sequential single step from the pre-settling genome (topology child or parent clone).",
            "- Evaluation: that one intermediate is evaluated on the frozen development panel.",
            "- Winner: the only candidate when the budget is 1.",
            "- Matched control: identical eligible kind set, order, and budget, with no topology change.",
            "- Independent confirmation is required before any production conclusion.",
            "- Raw topology identity and settled identity are both retained.",
            "",
            "## Morphology nursery",
            "",
            "Deferred until the four-arm first experiment demonstrates a recoverable topology-settling signal. The contract requires `morphologyNurseryDeferred=true` and rejects a nursery object. Keep any nursery concept outside production breeding.",
            "",
            f"First-experiment slot count: **{spec['slotBudget']['firstExperimentSlotCount']}**",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def resource_suboperation_plan_markdown(spec: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Balanced resource-suboperation matrix plan v1",
            "",
            "Do not launch. This specification exists because V38 sampled the broad resource family, so period/range/timeframe/lookback cells were sparse. Empty or ineligible parent×site cells remain scientific outcomes. Do not sample uniformly from the full plan pool.",
            "",
            f"Lanes: {spec['lanes']}",
            f"Children per eligible cell: {spec['childrenPerEligibleCell']}",
            f"Balance rule: `{spec['balanceRule']}`",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def decision_memo_v2(
    *,
    resource: Mapping[str, Any],
    multipanel: Mapping[str, Any],
    event: Mapping[str, Any],
    coverage: Mapping[str, Any],
    topology: Mapping[str, Any],
    protection: Mapping[str, Any],
    coadaptation: Mapping[str, Any],
) -> str:
    event_summary = next(
        (item for item in resource["bySuboperation"] if item.get("constructionKind") == "directional_event_insert"),
        {},
    )
    worst = protection["catastrophicTail"]["case"] or {}
    return "\n".join(
        [
            "# V38 follow-up decision memo v2",
            "",
            "Local source audit plus existing V38 artifacts only. No new market evaluation, generation, Vast instance, or 1024×5.",
            "",
            "## 1. What actually drove V38 resource success?",
            "",
            f"`directional_event_insert` remains the strongest recovered operation: median parent-relative net R {_fmt(event_summary.get('medianParentRelativeConservativeNetR'))}, {event_summary.get('parentBeats')} beats, {event_summary.get('absolutePositiveChildren')} absolute-positive, {event_summary.get('riskQualifiedBeats')} risk-qualified beats. The effect is not universal: it is a panel-3 positive tail around two archive parents and is inert around `qd_19e9`.",
            "",
            f"Parameter-level repeatable positive tail: **{resource['answers']['parameterLevelRepeatablePositiveTail']}**. Range-mutate's v1 'beat' is encoding dust and is a phenotype tie under v2.",
            "",
            "## 2. Did independent panels preserve the event-insert tail?",
            "",
            f"Accepted event-insert children: {multipanel['directionalEventInsert']['acceptedChildren']}. Entered backfill: {multipanel['directionalEventInsert']['enteredBackfillCohort']}. On the backfilled subset, both `qd_69e5` and `qd_ed27` still have at least one absolute-positive or parent-superior child on panel-1 and on panel-2: **{multipanel['directionalEventInsert']['positiveTailPersistedAroundBothQd69e5AndQdEd27']}**. `qd_19e9` has no parent-superior or absolute-positive event-insert child on panel-3, and none of its five event-insert children entered the panel-1/2 backfill: **{multipanel['directionalEventInsert']['qd19e9GenuinelyInertAcrossPanels']}**. Caveat: only the panel-3 provisional cohort was backfilled.",
            "",
            f"Event-insert motif interpretation: **{event['positiveTailInterpretation']}**.",
            "",
            "## 3. Coverage",
            "",
            f"{coverage['coverage']['catalogPeriodLikeFieldsAdmitted']} period-like fields admitted; {coverage['coverage']['catalogNonPeriodNumericFieldsExcluded']} non-period numeric and {coverage['coverage']['catalogEnumOrOptionFieldsExcluded']} enum/option fields excluded. Bound `hasBoundTalibMeta=true` on {coverage['coverage']['boundOccurrencesWithTalibMeta']} of {coverage['coverage']['boundParentSideOccurrences']} parent-side occurrences.",
            "",
            "## 4. Topology",
            "",
            "Specific-operation effects are demonstrated. Missing co-adaptation remains a hypothesis. Recovered accepts are not complete operation-specific attempts.",
            "",
            "## 5. Protection tail",
            "",
            f"Worst child `{worst.get('candidateId')}` reconstructed a complete stop+target pair, including the unchanged opposite locator. Cost-in-R channel: **{(worst.get('costChannel') or {}).get('costInRChannel')}**. The 1:1 probe is not a production gate.",
            "",
            "## 6. Co-adaptation contract",
            "",
            f"v2 JSON validates and self-hashes in Python and Rust. First-experiment slot count {coadaptation['slotBudget']['firstExperimentSlotCount']}. Morphology nursery is deferred. This change does not launch the experiment.",
            "",
            "## 7. Smallest next market experiment, if any?",
            "",
            "Do not start G6, continue V37, breed the V38 archive, reweight families, or run another 1024×5. If a later operator authorizes compute, the smallest experiment is the four-arm first contrast on frozen parents: clone / topology-only / directional_event_insert control / topology then one directional_event_insert settling step.",
            "",
        ]
    )


def run_audit_v2(
    *,
    v38_root: Path,
    catalog_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    matrix = load_matrix(v38_root)
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    generation = v38_generation_root(v38_root)
    parent_material_path = generation / "proposal" / "parent-material.jsonl"
    parent_material = load_parent_material(parent_material_path)
    slots = load_slot_rows(v38_root=v38_root, matrix=matrix)
    panel3 = load_evaluated_by_id(
        generation / "campaign" / "proposal-current-panel" / "campaign-output" / "evaluated-members.jsonl"
    )
    clones = generation / "campaign" / "fast-prefinalizer" / "round-0000" / "task-0000" / "campaign-output" / "evaluated-members.jsonl"
    if clones.is_file():
        panel3.update(load_evaluated_by_id(clones))
    panel1 = load_evaluated_by_id(
        generation / "campaign" / "fast-prefinalizer" / "round-0001" / "task-0000" / "campaign-output" / "evaluated-members.jsonl"
    )
    panel2 = load_evaluated_by_id(
        generation / "campaign" / "fast-prefinalizer" / "round-0001" / "task-0001" / "campaign-output" / "evaluated-members.jsonl"
    )
    slots = attach_economics_v2(slots, panel3)
    baselines = parent_baselines_v2(matrix, panel3)
    slots = bind_relatives_v2(slots, baselines)
    slots = attach_backfill(slots, v38_root=v38_root)
    slots = attach_archive_v2(slots, v38_root=v38_root)
    slots = attach_panel_relatives(
        slots,
        panel_rows={"panel-1": panel1, "panel-2": panel2, "panel-3": panel3},
        parent_ids=[parent["candidateId"] for parent in matrix["parents"]],
    )
    coverage = build_coverage_report_v2(
        catalog=catalog,
        matrix=matrix,
        parent_material=parent_material_path,
    )
    resource = build_resource_report_v2(slots, baselines)
    multipanel = build_multipanel_report(slots)
    event = build_event_insert_forensic(slots, parent_material, baselines)
    topology = build_topology_report_v2(slots)
    protection = build_protection_report_v2(slots, panel3, parent_material, baselines)
    coadaptation = build_coadaptation_spec(matrix=matrix, slots=slots)
    balanced = build_resource_suboperation_matrix(parents=matrix["parents"], children_per_eligible_cell=8)
    memo = decision_memo_v2(
        resource=resource,
        multipanel=multipanel,
        event=event,
        coverage=coverage,
        topology=topology,
        protection=protection,
        coadaptation=coadaptation,
    )
    outputs: dict[str, tuple[Mapping[str, Any] | None, str]] = {
        "indicator-parameter-evolution-coverage-v2.json": (coverage, coverage_markdown_v2(coverage)),
        "v38-resource-suboperation-heritability-v2.json": (resource, resource_markdown_v2(resource)),
        "v38-multipanel-suboperation-v2.json": (multipanel, multipanel_markdown(multipanel)),
        "v38-directional-event-insert-forensic-v2.json": (event, event_markdown(event)),
        "v38-topology-operation-audit-v2.json": (topology, topology_markdown_v2(topology)),
        "v38-initial-protection-tail-forensic-v2.json": (protection, protection_markdown_v2(protection)),
        "topology-coadaptation-matrix-spec-v2.json": (coadaptation, coadaptation_plan_markdown(coadaptation)),
        "balanced-resource-suboperation-matrix-spec-v1.json": (balanced, resource_suboperation_plan_markdown(balanced)),
    }
    written: dict[str, Path] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        if payload is not None:
            write_report(path, payload, markdown)
        written[name] = path
    memo_path = output_dir / "decision-memo-v2.md"
    memo_path.write_text(memo, encoding="utf-8", newline="\n")
    written["decision-memo-v2.md"] = memo_path
    plan_path = output_dir / "topology-coadaptation-research-plan-v2.md"
    plan_path.write_text(coadaptation_plan_markdown(coadaptation), encoding="utf-8", newline="\n")
    written["topology-coadaptation-research-plan-v2.md"] = plan_path
    readme = output_dir / "README-v2.md"
    readme.write_text(
        "\n".join(
            [
                "# V38 evolve-everything follow-up artifacts v2",
                "",
                "Generated locally from the V38 run and current source. No new market evaluation. v1 files in this folder are unchanged.",
                "",
                "Regenerate with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v2 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README-v2.md"] = readme
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v38-root", type=Path, default=DEFAULT_V38_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    written = run_audit_v2(v38_root=args.v38_root, catalog_path=args.catalog, output_dir=args.output_dir)
    for path in written.values():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
