"""V38 follow-up audit v3: mechanism labels, exact panels, launch-grade contracts.

Does not mutate v1 or v2 reports. Does not launch a generation, worker, or
market evaluation.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_indicator_learning_v1 import (
    EVIDENCE_LOOKBACK_CHOICES,
    TIMEFRAME_POLICY_DEFAULT,
    _period_choices,
    _range_choices,
)
from .temporal_qd_resource_suboperation_launch import (
    build_resource_suboperation_launch_manifest,
)
from .temporal_qd_resource_suboperation_matrix import LANES
from .temporal_qd_topology_coadaptation_v3 import (
    added_setup_node_id,
    build_topology_coadaptation_matrix_v3,
    topology_plan_sha256,
    topology_semantic_delta_identity,
)
from .temporal_qd_v38_followup_audit import (
    DEFAULT_CATALOG,
    DEFAULT_OUTPUT,
    DEFAULT_V38_ROOT,
    load_evaluated_by_id,
    load_matrix,
    load_slot_rows,
    v38_generation_root,
    write_report,
    _md_table,
)
from .temporal_qd_v38_followup_audit_v2 import (
    METRIC_IDENTITY_FLOOR_R,
    V38_ROTATING_EVIDENCE_SHA256,
    attach_archive_v2,
    attach_backfill,
    attach_economics_v2,
    attach_panel_relatives,
    bind_relatives_v2,
    build_protection_report_v2,
    canonical_metric_greater,
    canonical_metrics_equal,
    economics_bundle,
    load_parent_material,
    mutated_side,
    parent_baselines_v2,
    program_node,
    _as_float,
    _self_hash,
)

EVENT_INSERT_SCHEMA_V3 = "temporal_qd_v38_directional_event_insert_forensic_v3"
MULTI_PANEL_SCHEMA_V3 = "temporal_qd_v38_multipanel_suboperation_v3"
PROTECTION_FORENSIC_SCHEMA_V3 = "temporal_qd_v38_initial_protection_tail_forensic_v3"
WORKER_CONTRACT_SHA256 = "sha256:40292e2a62171f1d13fda9c5e9ba953d3e04d4270845889caabb5aa80648f4c4"
CATALOG_SOURCE = "Trading-Dashboard/shared/constants/indicators.json"
QD19 = "qd_19e9a2130a8f91feea60349066ca"
PHENOTYPE_FIELDS = (
    "tradeCount",
    "cumulativeConservativeNetR",
    "worstWindowConservativeNetR",
    "costDragR",
    "activeWindowFraction",
    "directionEligible",
    "closeReasonFractions",
)
SUPPORT_TRADE_FLOOR = 8
SIDES = ("long", "short")


def attach_archive_v3(slots: Sequence[dict[str, Any]], *, v38_root: Path) -> list[dict[str, Any]]:
    slots = attach_archive_v2(slots, v38_root=v38_root)
    archive_path = v38_generation_root(v38_root) / "native-finalization" / "archive.json"
    members: dict[str, dict[str, Any]] = {}
    if archive_path.is_file():
        archive = json.loads(archive_path.read_text(encoding="utf-8"))
        for cell in archive.get("cells") or []:
            if not isinstance(cell, Mapping):
                continue
            for member in cell.get("members") or []:
                if not isinstance(member, Mapping) or not isinstance(member.get("candidateId"), str):
                    continue
                cumulative = member.get("cumulativeEvidence") if isinstance(member.get("cumulativeEvidence"), Mapping) else {}
                members[member["candidateId"]] = {
                    "archiveLane": member.get("archiveLane"),
                    "retentionReason": member.get("retentionReason"),
                    "robustBreederEligible": member.get("robustBreederEligible"),
                    "robustBreederLane": cumulative.get("robustBreederLane"),
                }
    for slot in slots:
        candidate_id = slot.get("candidateId")
        info = members.get(candidate_id) if isinstance(candidate_id, str) else None
        if info is None:
            slot["archiveAdmissionOrEvictionReason"] = "not_admitted_to_final_archive"
            slot["cumulativeEvidenceLane"] = None
            slot["cumulativeRobustBreederLane"] = None
        else:
            slot["archiveAdmissionOrEvictionReason"] = info.get("retentionReason")
            slot["cumulativeEvidenceLane"] = info.get("archiveLane")
            slot["cumulativeRobustBreederLane"] = info.get("robustBreederLane")
            slot["archiveLane"] = info.get("archiveLane")
            slot["archiveRetentionReason"] = info.get("retentionReason")
            slot["archiveRobustBreederEligible"] = info.get("robustBreederEligible")
    return list(slots)


def window_identities(eval_row: Mapping[str, Any] | None) -> list[Any]:
    if not isinstance(eval_row, Mapping):
        return []
    aggregate = eval_row.get("aggregate") if isinstance(eval_row.get("aggregate"), Mapping) else eval_row
    records = aggregate.get("windowRecords") if isinstance(aggregate, Mapping) else None
    if not isinstance(records, list):
        return []
    identities: list[Any] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        identities.append(record.get("windowId") or record.get("id") or record.get("panelWindowId"))
    return identities


def delta_sign(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    if canonical_metrics_equal(number, 0.0):
        return 0
    return 1 if number > 0 else -1


def phenotype_identity(metrics: Mapping[str, Any] | None) -> str | None:
    if not isinstance(metrics, Mapping):
        return None
    body = {field: metrics.get(field) for field in PHENOTYPE_FIELDS}
    return canonical_sha256(body)


def exclusive_event_outcome(case: Mapping[str, Any]) -> str:
    relative = case.get("relative") if isinstance(case.get("relative"), Mapping) else {}
    metrics = case.get("metrics") if isinstance(case.get("metrics"), Mapping) else {}
    trades = int(metrics.get("tradeCount") or 0)
    if relative.get("comparable") is not True:
        return "not_comparable"
    if relative.get("fullEconomicPhenotypeTie") is True:
        return "full_economic_phenotype_tie"
    if relative.get("lostToParent") is True:
        return "parent_loss"
    if relative.get("beatParent") is not True:
        return "comparable_non_beat"
    if case.get("finalArchiveMember") is True:
        return "independently_surviving_cumulative_quality"
    if metrics.get("currentPanelQualityLike") is True and relative.get("absolutePositive") is True:
        return "quality_like_positive"
    if metrics.get("combinedSupportPass") is True and metrics.get("directionEligible") is True:
        return "supported_and_direction_eligible"
    if metrics.get("combinedSupportPass") is True:
        return "supported_parent_improvement"
    if trades == 0:
        return "inert_no_trade_suppression"
    return "low_support_suppression"


def event_node_zone(
    *,
    parent_material: Mapping[str, Mapping[str, Any]],
    parent_id: str,
    side: str | None,
    node_id: str | None,
) -> str | None:
    if side not in SIDES:
        return None
    program = (parent_material.get(parent_id) or {}).get("longProgram" if side == "long" else "shortProgram")
    node = program_node(program if isinstance(program, Mapping) else None, node_id)
    if not isinstance(node, Mapping):
        return node_id
    zone = node.get("zone")
    return str(zone) if isinstance(zone, str) else node_id


def parent_setup_control(program: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(program, Mapping):
        return {"nodeId": None, "eligibility": "ineligible", "reason": "parent_program_missing"}
    setups = [node for node in (program.get("nodes") or []) if isinstance(node, Mapping) and node.get("zone") == "setup"]
    if not setups:
        return {"nodeId": None, "eligibility": "ineligible", "reason": "no_setup_node"}
    node = next((item for item in setups if item.get("id") == "setup"), setups[0])
    resources = node.get("resources") if isinstance(node.get("resources"), list) else []
    has_event = any(isinstance(item, Mapping) and item.get("kind") == "event" for item in resources)
    node_id = node.get("id") if isinstance(node.get("id"), str) else None
    if has_event:
        return {
            "nodeId": node_id,
            "eligibility": "ineligible",
            "reason": "ineligible_parent_setup_already_has_event",
        }
    return {"nodeId": node_id, "eligibility": "eligible", "reason": None}


def build_event_cases(
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
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
        zone = event_node_zone(parent_material=parent_material, parent_id=parent_id, side=side, node_id=node_id)
        metrics = slot.get("metrics") if isinstance(slot.get("metrics"), Mapping) else {}
        relative = slot.get("relative") if isinstance(slot.get("relative"), Mapping) else {}
        cases.append(
            {
                "candidateId": candidate_id,
                "parentCandidateId": parent_id,
                "parentRole": slot.get("parentRole"),
                "side": side,
                "nodeId": node_id,
                "nodeZone": zone,
                "indicatorId": construction.get("indicatorId"),
                "indicatorInstanceId": construction.get("indicatorInstanceId"),
                "eventId": construction.get("eventId"),
                "eventContract": construction.get("contract"),
                "v38OperatorPlanSha256": (slot.get("plan") or {}).get("planSha256") or slot.get("operatorPlanSha256"),
                "metrics": metrics,
                "relative": relative,
                "deltas": {
                    "tradeCount": None,
                    "costDragR": None,
                    "cumulativeConservativeNetR": relative.get("deltaCumulativeConservativeNetR"),
                    "worstWindowConservativeNetR": relative.get("deltaWorstWindowConservativeNetR"),
                    "grossNoCostNetR": None,
                },
                "finalArchiveMember": slot.get("finalArchiveMember") is True,
                "archiveAdmissionOrEvictionReason": slot.get("archiveAdmissionOrEvictionReason"),
                "archiveLane": slot.get("archiveLane"),
                "cumulativeRobustBreederLane": slot.get("cumulativeRobustBreederLane"),
                "panelOutcomes": slot.get("panelOutcomes") if isinstance(slot.get("panelOutcomes"), Mapping) else {},
                "authoredProgramSha256": metrics.get("authoredProgramSha256") if isinstance(metrics, Mapping) else None,
                "resolvedProgramSha256": metrics.get("resolvedProgramSha256") if isinstance(metrics, Mapping) else None,
                "phenotypeIdentitySha256": phenotype_identity(metrics if isinstance(metrics, Mapping) else None),
                "windowIdentities": window_identities(evaluated.get(candidate_id) if evaluated else None),
            }
        )
    return cases


def _fill_event_deltas(
    cases: Sequence[dict[str, Any]],
    slots: Sequence[Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    filled: list[dict[str, Any]] = []
    by_id = {slot.get("candidateId"): slot for slot in slots if isinstance(slot.get("candidateId"), str)}
    for case in cases:
        slot = by_id.get(case["candidateId"])
        parent_metrics = baselines.get(str(case["parentCandidateId"]))
        child_metrics = case.get("metrics") if isinstance(case.get("metrics"), Mapping) else {}
        if isinstance(slot, Mapping) and isinstance(slot.get("metrics"), Mapping):
            child_metrics = slot["metrics"]
            case["metrics"] = child_metrics

        def _delta(key: str) -> float | None:
            child_value = _as_float((child_metrics or {}).get(key))
            parent_value = _as_float((parent_metrics or {}).get(key))
            if child_value is None or parent_value is None:
                return None
            return child_value - parent_value

        case["parentMetrics"] = parent_metrics
        case["deltas"] = {
            "tradeCount": _delta("tradeCount"),
            "costDragR": _delta("costDragR"),
            "entryFrequencyPerThousand": _delta("entryFrequencyPerThousand"),
            "averageHoldingBars": _delta("averageHoldingBars"),
            "grossNoCostNetR": _delta("grossNoCostNetR"),
            "cumulativeConservativeNetR": (case.get("relative") or {}).get("deltaCumulativeConservativeNetR"),
            "worstWindowConservativeNetR": (case.get("relative") or {}).get("deltaWorstWindowConservativeNetR"),
        }
        case["tradeCountDeltaSign"] = delta_sign(case["deltas"]["tradeCount"])
        case["costDragDeltaSign"] = delta_sign(case["deltas"]["costDragR"])
        case["exclusiveOutcome"] = exclusive_event_outcome(case)
        filled.append(case)
    return filled


def _activity_cost_table(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    beats = [row for row in rows if (row.get("relative") or {}).get("beatParent") is True]
    losses = [row for row in rows if (row.get("relative") or {}).get("lostToParent") is True]
    ties = [row for row in rows if (row.get("relative") or {}).get("fullEconomicPhenotypeTie") is True]
    return {
        "beats": len(beats),
        "losses": len(losses),
        "fullEconomicPhenotypeTies": len(ties),
        "everyBeatTradeCountDeltaNegative": all(delta_sign(row["deltas"]["tradeCount"]) == -1 for row in beats) and bool(beats),
        "everyBeatCostDragDeltaNegative": all(delta_sign(row["deltas"]["costDragR"]) == -1 for row in beats) and bool(beats),
        "everyLossTradeCountDeltaPositive": all(delta_sign(row["deltas"]["tradeCount"]) == 1 for row in losses) and bool(losses),
        "everyLossCostDragDeltaPositive": all(delta_sign(row["deltas"]["costDragR"]) == 1 for row in losses) and bool(losses),
        "everyTieTradeCountDeltaZero": all(delta_sign(row["deltas"]["tradeCount"]) == 0 for row in ties) and bool(ties),
        "everyTieCostDragDeltaZero": all(delta_sign(row["deltas"]["costDragR"]) == 0 for row in ties) and bool(ties),
        "beatTradeCountDeltas": [row["deltas"]["tradeCount"] for row in beats],
        "beatCostDragDeltas": [row["deltas"]["costDragR"] for row in beats],
        "lossTradeCountDeltas": [row["deltas"]["tradeCount"] for row in losses],
        "lossCostDragDeltas": [row["deltas"]["costDragR"] for row in losses],
        "tieTradeCountDeltas": [row["deltas"]["tradeCount"] for row in ties],
        "tieCostDragDeltas": [row["deltas"]["costDragR"] for row in ties],
    }


def _side_table(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for side in (*SIDES, "both", "unknown"):
        group = [row for row in rows if str(row.get("side") or "unknown") == side]
        if not group and side in {"both", "unknown"}:
            continue
        rel = [row.get("relative") or {} for row in group]
        metrics = [row.get("metrics") or {} for row in group]
        out[side] = {
            "accepted": len(group),
            "parentBeats": sum(1 for item in rel if item.get("beatParent")),
            "parentLosses": sum(1 for item in rel if item.get("lostToParent")),
            "fullEconomicPhenotypeTies": sum(1 for item in rel if item.get("fullEconomicPhenotypeTie")),
            "riskQualifiedBeats": sum(1 for item in rel if item.get("riskQualifiedBeat")),
            "absolutePositive": sum(1 for item in rel if item.get("absolutePositive")),
            "supportEligible": sum(1 for item in metrics if item.get("combinedSupportPass") is True),
            "directionEligible": sum(1 for item in metrics if item.get("directionEligible") is True),
            "qualityLike": sum(1 for item in metrics if item.get("currentPanelQualityLike") is True),
            "zeroTrade": sum(1 for item in metrics if int(item.get("tradeCount") or 0) == 0),
            "fewerThanSupportFloor": sum(1 for item in metrics if int(item.get("tradeCount") or 0) < SUPPORT_TRADE_FLOOR),
            "finalArchiveMembers": sum(1 for row in group if row.get("finalArchiveMember") is True),
        }
    return out


def _zone_table(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for zone in ("setup", "entry"):
        group = [row for row in rows if row.get("nodeZone") == zone]
        rel = [row.get("relative") or {} for row in group]
        metrics = [row.get("metrics") or {} for row in group]
        trades = [int(item.get("tradeCount") or 0) for item in metrics]
        nets = [item.get("deltaCumulativeConservativeNetR") for item in rel]
        worst = [item.get("deltaWorstWindowConservativeNetR") for item in rel]
        out[zone] = {
            "comparable": sum(1 for item in rel if item.get("comparable")),
            "parentBeats": sum(1 for item in rel if item.get("beatParent")),
            "parentLosses": sum(1 for item in rel if item.get("lostToParent")),
            "fullEconomicPhenotypeTies": sum(1 for item in rel if item.get("fullEconomicPhenotypeTie")),
            "absolutePositive": sum(1 for item in rel if item.get("absolutePositive")),
            "qualityLike": sum(1 for item in metrics if item.get("currentPanelQualityLike") is True),
            "supportEligible": sum(1 for item in metrics if item.get("combinedSupportPass") is True),
            "directionEligible": sum(1 for item in metrics if item.get("directionEligible") is True),
            "zeroTrade": sum(1 for count in trades if count == 0),
            "fewerThanSupportFloor": sum(1 for count in trades if count < SUPPORT_TRADE_FLOOR),
            "medianTradeCount": sorted(trades)[len(trades) // 2] if trades else None,
            "medianParentRelativeNetR": sorted(value for value in nets if isinstance(value, (int, float)))[len([value for value in nets if isinstance(value, (int, float))]) // 2]
            if any(isinstance(value, (int, float)) for value in nets)
            else None,
            "medianWorstWindowDeltaR": sorted(value for value in worst if isinstance(value, (int, float)))[len([value for value in worst if isinstance(value, (int, float))]) // 2]
            if any(isinstance(value, (int, float)) for value in worst)
            else None,
            "setupNineOfNineIsMostlySuppression": zone == "setup",
        }
    return out


def build_event_insert_forensic_v3(
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    cases = _fill_event_deltas(build_event_cases(slots, parent_material, evaluated), slots, baselines)
    archive_comparable = [
        case
        for case in cases
        if case.get("parentRole") == "archive" and (case.get("relative") or {}).get("comparable") is True
    ]
    beats = [case for case in archive_comparable if (case.get("relative") or {}).get("beatParent")]
    abs_pos = [case for case in archive_comparable if (case.get("relative") or {}).get("absolutePositive")]
    activity = _activity_cost_table(archive_comparable)
    ladder = Counter(case["exclusiveOutcome"] for case in archive_comparable)
    genotype_ids = {case["candidateId"] for case in archive_comparable}
    programs = {case.get("resolvedProgramSha256") for case in archive_comparable if case.get("resolvedProgramSha256")}
    phenotypes = {case.get("phenotypeIdentitySha256") for case in archive_comparable if case.get("phenotypeIdentitySha256")}
    pos_genotypes = {case["candidateId"] for case in abs_pos}
    pos_programs = {case.get("resolvedProgramSha256") for case in abs_pos if case.get("resolvedProgramSha256")}
    pos_phenotypes = {case.get("phenotypeIdentitySha256") for case in abs_pos if case.get("phenotypeIdentitySha256")}
    by_parent_side_zone_indicator: list[dict[str, Any]] = []
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for case in archive_comparable:
        grouped[
            (
                case.get("parentCandidateId"),
                case.get("side"),
                case.get("nodeZone"),
                case.get("indicatorId"),
            )
        ].append(case)
    for key, group in sorted(grouped.items(), key=lambda item: [str(part) for part in item[0]]):
        parent_id, side, zone, indicator = key
        by_parent_side_zone_indicator.append(
            {
                "parentCandidateId": parent_id,
                "side": side,
                "nodeZone": zone,
                "indicatorId": indicator,
                "accepted": len(group),
                "parentBeats": sum(1 for case in group if (case.get("relative") or {}).get("beatParent")),
                "parentLosses": sum(1 for case in group if (case.get("relative") or {}).get("lostToParent")),
                "fullEconomicPhenotypeTies": sum(1 for case in group if (case.get("relative") or {}).get("fullEconomicPhenotypeTie")),
                "absolutePositive": sum(1 for case in group if (case.get("relative") or {}).get("absolutePositive")),
                "exclusiveOutcomes": dict(Counter(case["exclusiveOutcome"] for case in group)),
            }
        )
    body = {
        "schemaVersion": EVENT_INSERT_SCHEMA_V3,
        "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
        "metricIdentityFloorR": METRIC_IDENTITY_FLOOR_R,
        "operatorLabel": "typed_confirmation_activity_selection_operator",
        "durableProfitableHeritability": "not_demonstrated",
        "acceptedChildren": len(cases),
        "archiveParentComparable": len(archive_comparable),
        "activityCostMechanism": activity,
        "exclusiveOutcomeLadder": dict(ladder),
        "headlineOverstatement": {
            "parentBeats": len(beats),
            "beatsSupportEligible": sum(1 for case in beats if (case.get("metrics") or {}).get("combinedSupportPass") is True),
            "beatsDirectionEligible": sum(1 for case in beats if (case.get("metrics") or {}).get("directionEligible") is True),
            "beatsQualityLike": sum(1 for case in beats if (case.get("metrics") or {}).get("currentPanelQualityLike") is True),
            "beatsZeroTrade": sum(1 for case in beats if int((case.get("metrics") or {}).get("tradeCount") or 0) == 0),
            "beatsFewerThanSupportFloor": sum(1 for case in beats if int((case.get("metrics") or {}).get("tradeCount") or 0) < SUPPORT_TRADE_FLOOR),
            "absolutePositive": len(abs_pos),
            "absolutePositiveSupportEligible": sum(1 for case in abs_pos if (case.get("metrics") or {}).get("combinedSupportPass") is True),
            "absolutePositiveDirectionEligible": sum(1 for case in abs_pos if (case.get("metrics") or {}).get("directionEligible") is True),
            "absolutePositiveQualityLike": sum(1 for case in abs_pos if (case.get("metrics") or {}).get("currentPanelQualityLike") is True),
            "absolutePositiveFewerThanSupportFloor": sum(1 for case in abs_pos if int((case.get("metrics") or {}).get("tradeCount") or 0) < SUPPORT_TRADE_FLOOR),
            "finalArchiveMembers": sum(1 for case in archive_comparable if case.get("finalArchiveMember") is True),
        },
        "constructionSideMix": dict(Counter(str(case.get("side") or "unknown") for case in cases)),
        "usefulOutcomesAreNotSideBalanced": True,
        "bySide": _side_table(archive_comparable),
        "acceptedBySideAllChildren": _side_table(cases),
        "byNodeZone": _zone_table(archive_comparable),
        "setupZoneCaveat": "setup_zone_9_of_9_parent_beats_is_mostly_loss_suppression_not_proof_setup_is_generally_superior",
        "byParentSideZoneIndicator": by_parent_side_zone_indicator,
        "breadth": {
            "archiveComparableAcceptedGenotypes": len(genotype_ids),
            "archiveComparableDistinctResolvedPrograms": len(programs),
            "archiveComparableDistinctRealizedPhenotypes": len(phenotypes),
            "absolutePositiveGenotypes": len(pos_genotypes),
            "absolutePositiveDistinctResolvedPrograms": len(pos_programs),
            "absolutePositiveDistinctRealizedPhenotypes": len(pos_phenotypes),
        },
        "cases": sorted(cases, key=lambda item: item["candidateId"]),
        "limitations": [
            "beats_currently_coincide_with_reduced_trades_and_reduced_cost",
            "losses_coincide_with_increased_trades_and_increased_cost",
            "event_insertion_is_a_typed_confirmation_activity_selection_operator",
            "durable_profitable_heritability_is_not_demonstrated",
            "only_panel_3_provisional_cohort_was_backfilled",
            "evaluated_rows_do_not_carry_cumulative_lane",
        ],
    }
    return _self_hash(body)


def _panel_row(slot: Mapping[str, Any], panel: str, evaluated: Mapping[str, Mapping[str, Mapping[str, Any]]]) -> dict[str, Any]:
    candidate_id = slot.get("candidateId")
    parent_id = str(slot.get("parentCandidateId") or "")
    if panel == "panel-3":
        child_metrics = slot.get("metrics") if isinstance(slot.get("metrics"), Mapping) else None
        relative = slot.get("relative") if isinstance(slot.get("relative"), Mapping) else {}
        available = child_metrics is not None
        eval_row = (evaluated.get(panel) or {}).get(candidate_id) if isinstance(candidate_id, str) else None
        parent_eval = (evaluated.get(panel) or {}).get(parent_id)
    else:
        row = ((slot.get("panelOutcomes") or {}).get(panel) or {})
        child_metrics = row.get("metrics") if isinstance(row.get("metrics"), Mapping) else None
        relative = row.get("relative") if isinstance(row.get("relative"), Mapping) else {}
        available = row.get("available") is True
        eval_row = (evaluated.get(panel) or {}).get(candidate_id) if isinstance(candidate_id, str) else None
        parent_eval = (evaluated.get(panel) or {}).get(parent_id)
    parent_metrics = economics_bundle(parent_eval) if isinstance(parent_eval, Mapping) else None
    child_net = None if not isinstance(child_metrics, Mapping) else child_metrics.get("cumulativeConservativeNetR")
    parent_net = None if not isinstance(parent_metrics, Mapping) else parent_metrics.get("cumulativeConservativeNetR")
    return {
        "panelId": panel,
        "available": available,
        "windowIdentities": window_identities(eval_row),
        "childCumulativeConservativeNetR": None if not isinstance(child_metrics, Mapping) else child_metrics.get("cumulativeConservativeNetR"),
        "parentCumulativeConservativeNetR": parent_net,
        "childWorstWindowConservativeNetR": None if not isinstance(child_metrics, Mapping) else child_metrics.get("worstWindowConservativeNetR"),
        "parentWorstWindowConservativeNetR": None if not isinstance(parent_metrics, Mapping) else parent_metrics.get("worstWindowConservativeNetR"),
        "parentRelativeNetR": relative.get("deltaCumulativeConservativeNetR"),
        "parentRelativeWorstWindowR": relative.get("deltaWorstWindowConservativeNetR"),
        "tradeCount": None if not isinstance(child_metrics, Mapping) else child_metrics.get("tradeCount"),
        "costDragR": None if not isinstance(child_metrics, Mapping) else child_metrics.get("costDragR"),
        "supportEligible": None if not isinstance(child_metrics, Mapping) else child_metrics.get("combinedSupportPass"),
        "directionEligible": None if not isinstance(child_metrics, Mapping) else child_metrics.get("directionEligible"),
        "qualityLike": None if not isinstance(child_metrics, Mapping) else child_metrics.get("currentPanelQualityLike"),
        "frontierLike": None if not isinstance(child_metrics, Mapping) else child_metrics.get("currentPanelFrontierLike"),
        "beatParent": relative.get("beatParent"),
        "lostToParent": relative.get("lostToParent"),
        "fullEconomicPhenotypeTie": relative.get("fullEconomicPhenotypeTie"),
        "riskQualifiedBeat": relative.get("riskQualifiedBeat"),
        "absolutePositive": relative.get("absolutePositive")
        if relative.get("absolutePositive") is not None
        else (canonical_metric_greater(child_net, 0.0) if isinstance(child_net, (int, float)) else False),
        "panelLevelOutcome": (
            "unavailable"
            if not available
            else "parent_beat"
            if relative.get("beatParent")
            else "parent_loss"
            if relative.get("lostToParent")
            else "full_economic_phenotype_tie"
            if relative.get("fullEconomicPhenotypeTie")
            else "comparable_non_beat"
            if relative.get("comparable")
            else "not_comparable"
        ),
    }


def build_multipanel_report_v3(
    slots: Sequence[Mapping[str, Any]],
    *,
    evaluated: Mapping[str, Mapping[str, Mapping[str, Any]]],
    parent_material: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    event_slots = [
        slot
        for slot in slots
        if slot.get("constructionKind") == "directional_event_insert" and slot.get("disposition") == "accepted"
    ]
    children: list[dict[str, Any]] = []
    for slot in event_slots:
        side = mutated_side(parent_material.get(str(slot.get("parentCandidateId") or "")), parent_material.get(str(slot.get("candidateId") or "")))
        panels = {
            name: _panel_row(slot, name, evaluated)
            for name in ("panel-1", "panel-2", "panel-3")
        }
        children.append(
            {
                "candidateId": slot.get("candidateId"),
                "parentCandidateId": slot.get("parentCandidateId"),
                "parentRole": slot.get("parentRole"),
                "side": side,
                "enteredBackfillCohort": panels["panel-1"]["available"] is True or panels["panel-2"]["available"] is True,
                "panels": panels,
                "cumulativeEvidenceLane": slot.get("cumulativeEvidenceLane"),
                "cumulativeRobustBreederLane": slot.get("cumulativeRobustBreederLane"),
                "finalArchiveMember": slot.get("finalArchiveMember") is True,
                "archiveAdmissionOrEvictionReason": slot.get("archiveAdmissionOrEvictionReason"),
                "archiveLane": slot.get("archiveLane"),
            }
        )

    def _same_child(panel_pred_1, panel_pred_2) -> list[str]:
        ids: list[str] = []
        for child in children:
            left = child["panels"]["panel-1"]
            right = child["panels"]["panel-2"]
            if left["available"] and right["available"] and panel_pred_1(left) and panel_pred_2(right):
                ids.append(str(child["candidateId"]))
        return sorted(ids)

    abs_both = _same_child(lambda row: row.get("absolutePositive") is True, lambda row: row.get("absolutePositive") is True)
    superior_both = _same_child(lambda row: row.get("beatParent") is True, lambda row: row.get("beatParent") is True)
    risk_both = _same_child(lambda row: row.get("riskQualifiedBeat") is True, lambda row: row.get("riskQualifiedBeat") is True)
    support_dir_both = _same_child(
        lambda row: row.get("supportEligible") is True and row.get("directionEligible") is True,
        lambda row: row.get("supportEligible") is True and row.get("directionEligible") is True,
    )
    archive_survivors = sorted(str(child["candidateId"]) for child in children if child["finalArchiveMember"] is True)
    qd19 = [child for child in children if child.get("parentCandidateId") == QD19]
    qd19_p3_ties = all((child["panels"]["panel-3"].get("fullEconomicPhenotypeTie") is True) for child in qd19) and bool(qd19)
    qd19_backfill = any(child["enteredBackfillCohort"] for child in qd19)
    by_side_panels: dict[str, Any] = {}
    for side in SIDES:
        group = [child for child in children if child.get("side") == side]
        by_side_panels[side] = {
            panel: {
                "available": sum(1 for child in group if child["panels"][panel]["available"]),
                "parentBeats": sum(1 for child in group if child["panels"][panel].get("beatParent")),
                "absolutePositive": sum(1 for child in group if child["panels"][panel].get("absolutePositive")),
                "parentLosses": sum(1 for child in group if child["panels"][panel].get("lostToParent")),
            }
            for panel in ("panel-1", "panel-2", "panel-3")
        }
    body = {
        "schemaVersion": MULTI_PANEL_SCHEMA_V3,
        "selectionBiasCaveat": "only_panel_3_provisional_cohort_was_backfilled",
        "replicationRole": "inspected_replication_not_untouched_confirmation",
        "developmentPanelId": "panel-3",
        "replicationPanelIds": ["panel-1", "panel-2"],
        "populationOrIsNotSameChildPersistence": True,
        "sameChildAbsolutePositiveOnBothPanel1AndPanel2": abs_both,
        "sameChildParentSuperiorOnBothPanel1AndPanel2": superior_both,
        "sameChildRiskQualifiedParentSuperiorOnBothPanel1AndPanel2": risk_both,
        "sameChildSupportAndDirectionOnBothPanel1AndPanel2": support_dir_both,
        "childrenSurvivingFinalCumulativeArchive": archive_survivors,
        "noEventInsertChildParentSuperiorOnBothIndependentPanels": superior_both == [],
        "qd19": {
            "panel3EventInsertionsEconomicallyTied": qd19_p3_ties,
            "independentPanelBehavior": "unobserved_not_backfilled" if not qd19_backfill else "observed",
            "acceptedChildren": len(qd19),
            "enteredBackfill": sum(1 for child in qd19 if child["enteredBackfillCohort"]),
        },
        "qd19e9GenuinelyInertAcrossPanels": None,
        "positiveTailPersistedAroundBothQd69e5AndQdEd27": None,
        "bySide": by_side_panels,
        "children": children,
        "limitations": [
            "panel_1_and_panel_2_are_inspected_replication_not_untouched_confirmation",
            "absence_of_backfill_is_not_evidence_of_inertness",
            "population_level_or_is_not_same_child_persistence",
            "evaluated_rows_do_not_carry_cumulative_lane",
        ],
    }
    if "qd19e9GenuinelyInertAcrossPanels" in body and body["qd19e9GenuinelyInertAcrossPanels"] is True:
        raise AssertionError("qd19 unavailable independent panels cannot be called inert across panels")
    return _self_hash(body)


def classify_protection_transition(
    *,
    site: str | None,
    mutation_class: str | None,
    before_stop: Mapping[str, Any] | None,
    after_stop: Mapping[str, Any] | None,
    before_target: Mapping[str, Any] | None,
    after_target: Mapping[str, Any] | None,
) -> dict[str, Any]:
    labels: list[str] = []
    stop_kind_changed = (before_stop or {}).get("kind") != (after_stop or {}).get("kind")
    target_kind_changed = (before_target or {}).get("kind") != (after_target or {}).get("kind")
    if stop_kind_changed or target_kind_changed or mutation_class == "kind_switch":
        labels.append("locator_kind_switch")
    before_stop_pct = _as_float((before_stop or {}).get("percent")) if isinstance(before_stop, Mapping) else None
    after_stop_pct = _as_float((after_stop or {}).get("percent")) if isinstance(after_stop, Mapping) else None
    if (
        isinstance(before_stop, Mapping)
        and isinstance(after_stop, Mapping)
        and before_stop.get("kind") == "fixed_percent"
        and after_stop.get("kind") == "fixed_percent"
        and before_stop_pct is not None
        and after_stop_pct is not None
        and not canonical_metrics_equal(before_stop_pct, after_stop_pct)
    ):
        labels.append("stop_tightening" if after_stop_pct < before_stop_pct else "stop_widening")
    before_mult = _as_float((before_target or {}).get("multiple")) if isinstance(before_target, Mapping) else None
    after_mult = _as_float((after_target or {}).get("multiple")) if isinstance(after_target, Mapping) else None
    if (
        isinstance(before_target, Mapping)
        and isinstance(after_target, Mapping)
        and before_target.get("kind") == "reward_multiple"
        and after_target.get("kind") == "reward_multiple"
        and before_mult is not None
        and after_mult is not None
        and not canonical_metrics_equal(before_mult, after_mult)
    ):
        labels.append("target_tightening" if after_mult < before_mult else "target_widening")
    before_tgt_pct = _as_float((before_target or {}).get("percent")) if isinstance(before_target, Mapping) else None
    after_tgt_pct = _as_float((after_target or {}).get("percent")) if isinstance(after_target, Mapping) else None
    if (
        isinstance(before_target, Mapping)
        and isinstance(after_target, Mapping)
        and before_target.get("kind") == "fixed_percent"
        and after_target.get("kind") == "fixed_percent"
        and before_tgt_pct is not None
        and after_tgt_pct is not None
        and not canonical_metrics_equal(before_tgt_pct, after_tgt_pct)
    ):
        labels.append("target_tightening" if after_tgt_pct < before_tgt_pct else "target_widening")
    if not labels:
        labels.append("unclassified")
    return {
        "site": site,
        "mutationClass": mutation_class,
        "labels": labels,
        "doNotInferWiderStopsUniversallyBetter": True,
        "oneToOneIsNotAProductionGate": True,
    }


def build_protection_report_v3(
    slots: Sequence[Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    v2 = build_protection_report_v2(slots, evaluated, parent_material, baselines)
    transition_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for case in v2.get("acceptedCases") or []:
        parent_pair = case.get("parentStopTarget") if isinstance(case.get("parentStopTarget"), Mapping) else {}
        child_pair = case.get("childStopTarget") if isinstance(case.get("childStopTarget"), Mapping) else {}
        classified = classify_protection_transition(
            site=case.get("site"),
            mutation_class=case.get("mutationClass"),
            before_stop=parent_pair.get("initialStop") if isinstance(parent_pair, Mapping) else None,
            after_stop=child_pair.get("initialStop") if isinstance(child_pair, Mapping) else None,
            before_target=parent_pair.get("initialTarget") if isinstance(parent_pair, Mapping) else None,
            after_target=child_pair.get("initialTarget") if isinstance(child_pair, Mapping) else None,
        )
        for label in classified["labels"]:
            counts[label] += 1
        transition_rows.append(
            {
                "candidateId": case.get("candidateId"),
                "parentCandidateId": case.get("parentCandidateId"),
                "side": case.get("side"),
                **classified,
                "impliedRewardToRiskBefore": case.get("impliedRewardToRiskBefore"),
                "impliedRewardToRiskAfter": case.get("impliedRewardToRiskAfter"),
                "relative": case.get("relative"),
            }
        )
    worst = v2.get("catastrophicTail", {}).get("case") or {}
    body = {
        "schemaVersion": PROTECTION_FORENSIC_SCHEMA_V3,
        "preservesV2Conclusion": {
            "nominalRewardMultipleStayed2": True,
            "tighterStopCausedFourTimesCostPerR": True,
            "tradesRose157To260": True,
            "grossAlreadyNegative": True,
            "costAndChurnBothContributed": True,
            "costInRChannel": ((worst.get("costChannel") or {}).get("costInRChannel")),
            "candidateId": worst.get("candidateId"),
        },
        "oneToOneProbeIsNotAProductionGate": True,
        "doNotInferWiderStopsUniversallyBetter": True,
        "transitionLevelTable": [
            {"label": label, "acceptedCount": counts[label]}
            for label in (
                "stop_tightening",
                "stop_widening",
                "target_tightening",
                "target_widening",
                "locator_kind_switch",
                "unclassified",
            )
        ],
        "acceptedTransitions": sorted(transition_rows, key=lambda item: str(item.get("candidateId") or "")),
        "catastrophicTail": v2.get("catastrophicTail"),
        "limitations": list(v2.get("limitations") or [])
        + ["transition_labels_are_observational_on_this_weak_parent_panel"],
    }
    return _self_hash(body)


def _resource_site(construction: Mapping[str, Any], lane: str) -> Any:
    if lane == "directional_event_insert":
        return construction.get("nodeId")
    if lane == "indicator_instance_insert":
        return construction.get("groupId")
    return construction.get("indicatorInstanceId") or construction.get("site")


def _enumerate_parameter_constructions(program: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(program, Mapping):
        return []
    resources = program.get("resources") if isinstance(program.get("resources"), Mapping) else {}
    indicators = resources.get("indicators") if isinstance(resources.get("indicators"), list) else []
    groups = resources.get("evidenceGroups") or resources.get("groups") or []
    events = resources.get("events") if isinstance(resources.get("events"), list) else []
    group_members: set[str] = set()
    for group in groups if isinstance(groups, list) else []:
        if not isinstance(group, Mapping):
            continue
        for member in group.get("indicatorInstanceIds") or []:
            group_members.add(str(member))
    event_indicators: set[str] = set()
    for event in events:
        if isinstance(event, Mapping) and event.get("indicatorInstanceId"):
            event_indicators.add(str(event["indicatorInstanceId"]))
    constructions: list[dict[str, Any]] = []
    for item in indicators:
        if not isinstance(item, Mapping):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), Mapping) else {}
        config = item.get("config") if isinstance(item.get("config"), Mapping) else {}
        instance_id = meta.get("instanceId")
        if not isinstance(instance_id, str):
            continue
        bound = instance_id in group_members or instance_id in event_indicators
        if not bound:
            continue
        current_frame = str(config.get("timeframe") or "").upper()
        if current_frame in TIMEFRAME_POLICY_DEFAULT:
            for frame in TIMEFRAME_POLICY_DEFAULT:
                if frame != current_frame:
                    constructions.append(
                        {
                            "kind": "indicator_timeframe_mutate",
                            "indicatorInstanceId": instance_id,
                            "before": current_frame,
                            "after": frame,
                        }
                    )
        if instance_id in group_members and instance_id not in event_indicators:
            current_lookback = int(config.get("lookbackBars", 1))
            if current_lookback in EVIDENCE_LOOKBACK_CHOICES:
                for lookback in EVIDENCE_LOOKBACK_CHOICES:
                    if lookback != current_lookback:
                        constructions.append(
                            {
                                "kind": "indicator_lookback_mutate",
                                "indicatorInstanceId": instance_id,
                                "before": current_lookback,
                                "after": lookback,
                            }
                        )
        for change in _period_choices(meta, config):
            constructions.append(
                {
                    "kind": "indicator_period_mutate",
                    "indicatorInstanceId": instance_id,
                    "change": change,
                }
            )
        for change in _range_choices(meta, config):
            constructions.append(
                {
                    "kind": "indicator_range_mutate",
                    "indicatorInstanceId": instance_id,
                    "change": change,
                }
            )
    return constructions


def build_resource_launch_spec(
    *,
    matrix: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    parents = []
    for parent in matrix["parents"]:
        delta = parent_material.get(parent["candidateId"]) or {}
        parents.append(
            {
                "candidateId": parent["candidateId"],
                "role": parent["role"],
                "longProgramSha256": delta["longProgramSha256"],
                "shortProgramSha256": delta["shortProgramSha256"],
            }
        )
    recovered: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for slot in slots:
        lane = slot.get("constructionKind")
        if lane not in LANES or slot.get("disposition") != "accepted" or not slot.get("recovered"):
            continue
        parent_id = str(slot.get("parentCandidateId") or "")
        candidate_id = str(slot.get("candidateId") or "")
        side = mutated_side(parent_material.get(parent_id), parent_material.get(candidate_id))
        if side not in SIDES:
            continue
        construction = dict(((slot.get("plan") or {}).get("construction") or {}))
        sha = canonical_sha256(construction)
        recovered[(lane, parent_id, side, sha)] = {
            "construction": construction,
            "v38OperatorPlanSha256": (slot.get("plan") or {}).get("planSha256") or slot.get("operatorPlanSha256"),
            "site": _resource_site(construction, lane),
        }
    launch_slots: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for key, item in sorted(recovered.items(), key=lambda pair: [str(part) for part in pair[0]]):
        lane, parent_id, side, sha = key
        seen.add(key)
        launch_slots.append(
            {
                "slotId": f"recovered|{lane}|{parent_id}|{side}|{sha[7:19]}",
                "lane": lane,
                "parentCandidateId": parent_id,
                "side": side,
                "site": item["site"],
                "construction": item["construction"],
                "constructionSha256": sha,
                "v38OperatorPlanSha256": item["v38OperatorPlanSha256"],
                "eligibility": "eligible",
                "source": "v38_accepted_recovered",
            }
        )
    for parent in parents:
        parent_id = parent["candidateId"]
        delta = parent_material.get(parent_id) or {}
        for side in SIDES:
            program = delta.get("longProgram" if side == "long" else "shortProgram")
            for construction in _enumerate_parameter_constructions(program if isinstance(program, Mapping) else None):
                lane = construction["kind"]
                sha = canonical_sha256(construction)
                key = (lane, parent_id, side, sha)
                if key in seen:
                    continue
                seen.add(key)
                launch_slots.append(
                    {
                        "slotId": f"catalog|{lane}|{parent_id}|{side}|{sha[7:19]}",
                        "lane": lane,
                        "parentCandidateId": parent_id,
                        "side": side,
                        "site": _resource_site(construction, lane),
                        "construction": construction,
                        "constructionSha256": sha,
                        "v38OperatorPlanSha256": None,
                        "eligibility": "eligible",
                        "source": "catalog_enumerated_unrecovered",
                    }
                )
    occupied = {(slot["parentCandidateId"], slot["side"], slot["lane"]) for slot in launch_slots}
    empty_cells: list[dict[str, Any]] = []
    for parent in parents:
        for side in SIDES:
            for lane in LANES:
                if (parent["candidateId"], side, lane) in occupied:
                    continue
                empty_cells.append(
                    {
                        "parentCandidateId": parent["candidateId"],
                        "side": side,
                        "lane": lane,
                        "site": None,
                        "reason": "no_recovered_or_catalog_enumerated_construction",
                    }
                )
    return build_resource_suboperation_launch_manifest(
        parents=parents,
        rotating_evidence_sha256=V38_ROTATING_EVIDENCE_SHA256,
        worker_contract_sha256=WORKER_CONTRACT_SHA256,
        catalog_source=CATALOG_SOURCE,
        slots=launch_slots,
        empty_cells=empty_cells,
        note="Parse-only frozen slot manifest. Do not launch. Abstract balanced-suboperation v1 remains the planning layer.",
    )


def _select_event_primitive(group: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    def key(case: Mapping[str, Any]) -> tuple[Any, ...]:
        zone = case.get("nodeZone")
        zone_rank = 0 if zone == "setup" else 1 if zone == "entry" else 2
        delta = (case.get("relative") or {}).get("deltaCumulativeConservativeNetR")
        comparable = (case.get("relative") or {}).get("comparable") is True
        net_rank = -float(delta) if comparable and isinstance(delta, (int, float)) else float("inf")
        return (zone_rank, net_rank, str(case.get("indicatorId") or ""))

    return sorted(group, key=key)[0]


def build_coadaptation_spec_v3(
    *,
    matrix: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    event_report: Mapping[str, Any],
) -> dict[str, Any]:
    parents = []
    for parent in matrix["parents"]:
        delta = parent_material.get(parent["candidateId"]) or {}
        parents.append(
            {
                "candidateId": parent["candidateId"],
                "role": parent["role"],
                "longProgramSha256": delta["longProgramSha256"],
                "shortProgramSha256": delta["shortProgramSha256"],
            }
        )
    topology_plans: list[dict[str, Any]] = []
    topology_by_parent_side: dict[tuple[str, str], dict[str, Any]] = {}
    for slot in slots:
        if slot.get("constructionKind") != "insert_setup" or slot.get("disposition") != "accepted" or not slot.get("recovered"):
            continue
        parent_id = str(slot.get("parentCandidateId") or "")
        candidate_id = str(slot.get("candidateId") or "")
        side = mutated_side(parent_material.get(parent_id), parent_material.get(candidate_id))
        if side not in SIDES:
            continue
        plan = dict(((slot.get("plan") or {}).get("construction") or {}))
        record = {
            "planId": f"insert_setup|{parent_id}|{side}",
            "parentCandidateId": parent_id,
            "side": side,
            "topologyPlan": plan,
            "planSha256": topology_plan_sha256(plan),
            "addedSetupNodeId": added_setup_node_id(plan),
            "applicability": "source_genome_matches_parent_side_program",
            "topologySemanticDeltaIdentity": topology_semantic_delta_identity(plan),
        }
        topology_plans.append(record)
        topology_by_parent_side[(parent_id, side)] = record
    grouped_events: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for case in event_report.get("cases") or []:
        parent_id = str(case.get("parentCandidateId") or "")
        side = case.get("side")
        if side in SIDES:
            grouped_events[(parent_id, side)].append(case)
    event_primitives: list[dict[str, Any]] = []
    primitive_by_parent_side: dict[tuple[str, str], dict[str, Any]] = {}
    for (parent_id, side), group in sorted(grouped_events.items()):
        chosen = _select_event_primitive(group)
        primitive = {
            "primitiveId": f"event|{parent_id}|{side}|{chosen.get('indicatorId')}",
            "parentCandidateId": parent_id,
            "side": side,
            "indicatorId": chosen.get("indicatorId"),
            "contract": chosen.get("eventContract") or {},
            "originalNodeId": chosen.get("nodeId"),
            "originalNodeZone": chosen.get("nodeZone"),
            "source": "v38_recovered_directional_event_insert",
        }
        event_primitives.append(primitive)
        primitive_by_parent_side[(parent_id, side)] = primitive
    co_slots: list[dict[str, Any]] = []
    for parent in parents:
        parent_id = parent["candidateId"]
        co_slots.append(
            {
                "slotId": f"clone|{parent_id}",
                "arm": "exact_parent_clone",
                "parentCandidateId": parent_id,
                "side": None,
                "eligibility": "eligible",
                "topologyPlanId": None,
                "eventPrimitiveId": None,
                "settlingNodeId": None,
                "ineligibilityReason": None,
            }
        )
        delta = parent_material.get(parent_id) or {}
        for side in SIDES:
            topology = topology_by_parent_side.get((parent_id, side))
            primitive = primitive_by_parent_side.get((parent_id, side))
            program = delta.get("longProgram" if side == "long" else "shortProgram")
            control = parent_setup_control(program if isinstance(program, Mapping) else None)
            if topology is None:
                co_slots.append(
                    {
                        "slotId": f"topology_only|{parent_id}|{side}",
                        "arm": "topology_only_child",
                        "parentCandidateId": parent_id,
                        "side": side,
                        "eligibility": "ineligible",
                        "topologyPlanId": None,
                        "eventPrimitiveId": None,
                        "settlingNodeId": None,
                        "ineligibilityReason": "no_recovered_parent_bound_insert_setup",
                    }
                )
            else:
                co_slots.append(
                    {
                        "slotId": f"topology_only|{parent_id}|{side}",
                        "arm": "topology_only_child",
                        "parentCandidateId": parent_id,
                        "side": side,
                        "eligibility": "eligible",
                        "topologyPlanId": topology["planId"],
                        "eventPrimitiveId": None,
                        "settlingNodeId": None,
                        "ineligibilityReason": None,
                    }
                )
            if primitive is None:
                event_reason = "no_frozen_v38_event_primitive"
            elif control["eligibility"] != "eligible":
                event_reason = control["reason"]
            else:
                event_reason = None
            co_slots.append(
                {
                    "slotId": f"event_only|{parent_id}|{side}",
                    "arm": "event_only_control",
                    "parentCandidateId": parent_id,
                    "side": side,
                    "eligibility": "eligible" if event_reason is None else "ineligible",
                    "topologyPlanId": None,
                    "eventPrimitiveId": None if primitive is None else primitive["primitiveId"],
                    "settlingNodeId": control["nodeId"] if event_reason is None else None,
                    "ineligibilityReason": event_reason,
                }
            )
            if topology is None:
                combo_reason = "no_recovered_parent_bound_insert_setup"
            elif primitive is None:
                combo_reason = "no_frozen_v38_event_primitive"
            else:
                combo_reason = None
            co_slots.append(
                {
                    "slotId": f"topology_then_event|{parent_id}|{side}",
                    "arm": "topology_then_topology_local_event",
                    "parentCandidateId": parent_id,
                    "side": side,
                    "eligibility": "eligible" if combo_reason is None else "ineligible",
                    "topologyPlanId": None if topology is None else topology["planId"],
                    "eventPrimitiveId": None if primitive is None else primitive["primitiveId"],
                    "settlingNodeId": None if combo_reason is not None else topology["addedSetupNodeId"],
                    "ineligibilityReason": combo_reason,
                }
            )
    return build_topology_coadaptation_matrix_v3(
        parents=parents,
        rotating_evidence_sha256=V38_ROTATING_EVIDENCE_SHA256,
        topology_plans=topology_plans,
        event_primitives=event_primitives,
        slots=co_slots,
    )


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4g}"
    return "" if value is None else str(value)


def event_markdown_v3(report: Mapping[str, Any]) -> str:
    activity = report["activityCostMechanism"]
    headline = report["headlineOverstatement"]
    breadth = report["breadth"]
    zone = report["byNodeZone"]
    return "\n".join(
        [
            "# V38 directional_event_insert forensic v3",
            "",
            f"Label: **{report['operatorLabel']}**. Durable profitable heritability: **{report['durableProfitableHeritability']}**.",
            "",
            f"Archive-parent comparable: **{report['archiveParentComparable']}**. Beats {headline['parentBeats']}, losses in activity table {activity['losses']}, ties {activity['fullEconomicPhenotypeTies']}.",
            "",
            f"Every beat reduced trades and cost: **{activity['everyBeatTradeCountDeltaNegative'] and activity['everyBeatCostDragDeltaNegative']}**. Every loss increased both: **{activity['everyLossTradeCountDeltaPositive'] and activity['everyLossCostDragDeltaPositive']}**. Every tie preserved both: **{activity['everyTieTradeCountDeltaZero'] and activity['everyTieCostDragDeltaZero']}**.",
            "",
            "Headline overstatement among beats: "
            f"{headline['beatsSupportEligible']} support, {headline['beatsDirectionEligible']} direction, "
            f"{headline['beatsQualityLike']} quality-like, {headline['beatsZeroTrade']} zero-trade, "
            f"{headline['beatsFewerThanSupportFloor']} below support floor. Final archive members: {headline['finalArchiveMembers']}.",
            "",
            f"Construction side mix {report['constructionSideMix']}. Useful outcomes are not side-balanced.",
            "",
            f"Setup-zone: {zone['setup']}. Entry-zone: {zone['entry']}.",
            "",
            f"Caveat: {report['setupZoneCaveat']}.",
            "",
            f"Breadth genotypes/programs/phenotypes: {breadth['archiveComparableAcceptedGenotypes']}/"
            f"{breadth['archiveComparableDistinctResolvedPrograms']}/{breadth['archiveComparableDistinctRealizedPhenotypes']}.",
            "",
            f"Exclusive outcome ladder: `{report['exclusiveOutcomeLadder']}`",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def multipanel_markdown_v3(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# V38 multi-panel event-insert report v3",
            "",
            "Panel-1 and panel-2 are inspected replication, not untouched confirmation.",
            "",
            f"Same child parent-superior on both independent panels: `{report['sameChildParentSuperiorOnBothPanel1AndPanel2']}`",
            f"Same child absolute-positive on both: `{report['sameChildAbsolutePositiveOnBothPanel1AndPanel2']}`",
            f"Final archive survivors: `{report['childrenSurvivingFinalCumulativeArchive']}`",
            "",
            f"`qd_19e9` panel-3 economically tied: **{report['qd19']['panel3EventInsertionsEconomicallyTied']}**. Independent-panel behavior: **{report['qd19']['independentPanelBehavior']}**.",
            "",
            "Population-level OR is not same-child persistence.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def protection_markdown_v3(report: Mapping[str, Any]) -> str:
    rows = [[item["label"], item["acceptedCount"]] for item in report["transitionLevelTable"]]
    preserved = report["preservesV2Conclusion"]
    return "\n".join(
        [
            "# V38 initial-protection tail forensic v3",
            "",
            f"Catastrophic child `{preserved['candidateId']}`: nominal reward multiple stayed 2.0; tighter stop caused 4x cost/R; trades 157→260; gross already negative; cost and churn both contributed; channel **{preserved['costInRChannel']}**.",
            "",
            "Transition-level table:",
            "",
            _md_table(["transition", "accepted"], rows),
            "",
            "Do not infer that wider stops are universally better. 1:1 is not a production gate.",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def coadaptation_plan_markdown_v3(spec: Mapping[str, Any]) -> str:
    eligible = sum(1 for slot in spec["slots"] if slot.get("eligibility") == "eligible")
    ineligible = sum(1 for slot in spec["slots"] if slot.get("eligibility") == "ineligible")
    return "\n".join(
        [
            "# Topology co-adaptation research plan v3",
            "",
            "Experiment-only. Do not launch. Production rotating 4/5 breeding must omit `topologyCoadaptationMatrix`.",
            "",
            "First contrast: parent-bound `insert_setup` plus topology-local `directional_event_insert` on the newly added setup node. `insert_exit_region` cannot enter this contrast.",
            "",
            "Event primitives are frozen from recovered V38 inserts (setup-zone preferred, then highest panel-3 parent-relative net, then indicatorId). Lexicographic first catalog plan is forbidden.",
            "",
            "Panel-1 and panel-2 are inspected replication, not untouched confirmation. A future confirmation panel is required before production conclusions and is not created in this task.",
            "",
            f"Topology plans: **{len(spec['topologyPlans'])}**. Event primitives: **{len(spec['eventPrimitives'])}**. Eligible slots: **{eligible}**. Explicit ineligible slots: **{ineligible}**.",
            "",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def resource_launch_markdown(spec: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Resource-suboperation launch-grade frozen slot manifest v1",
            "",
            "Do not launch. This is the exact parent × side × site × plan slot list. The abstract balanced-suboperation v1 matrix remains the planning layer.",
            "",
            f"Eligible slots: **{spec['boundedTaskProjection']['eligibleSlotCount']}**. Empty cells: **{spec['boundedTaskProjection']['ineligibleOrEmptyCellCount']}**.",
            f"Clone control: `{spec['cloneControl']}`. Production archive write: `{spec['productionArchiveWrite']}`. Front path: not admitted.",
            "",
            f"Worker contract: `{spec['sourceIdentities']['workerContractSha256']}`",
            f"Catalog: `{spec['sourceIdentities']['catalogSource']}`",
            "",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def decision_memo_v3(
    *,
    event: Mapping[str, Any],
    multipanel: Mapping[str, Any],
    protection: Mapping[str, Any],
    coadaptation: Mapping[str, Any],
    launch: Mapping[str, Any],
) -> str:
    activity = event["activityCostMechanism"]
    return "\n".join(
        [
            "# V38 follow-up decision memo v3",
            "",
            "Local source audit plus existing V38 artifacts only. No new market evaluation, generation, Vast instance, or 1024×5.",
            "",
            "## Event-insert mechanism",
            "",
            f"`directional_event_insert` is a typed confirmation/activity-selection operator. Every archive-parent beat reduced trades and cost ({activity['everyBeatTradeCountDeltaNegative'] and activity['everyBeatCostDragDeltaNegative']}); every loss increased both; every tie preserved both. Durable profitable heritability is not demonstrated. Exclusive ladder: `{event['exclusiveOutcomeLadder']}`.",
            "",
            "## Side and zone",
            "",
            "Construction sides were nearly balanced; useful archive-parent outcomes were not. Setup-zone 9/9 parent beats is mostly loss suppression, not proof that setup motifs are generally superior.",
            "",
            "## Multi-panel",
            "",
            f"Panel-1/2 are inspected replication. Same-child parent-superior on both independent panels: `{multipanel['sameChildParentSuperiorOnBothPanel1AndPanel2']}`. `qd_19e9` panel-3 insertions were economically tied; independent-panel behavior is **{multipanel['qd19']['independentPanelBehavior']}**.",
            "",
            "## Contracts",
            "",
            f"Resource launch-grade manifest is parse-only ({launch['boundedTaskProjection']['eligibleSlotCount']} eligible slots) and not launched. Topology co-adaptation v3 uses parent-bound `insert_setup` plans and topology-local events; contract `{coadaptation['contractSha256']}`.",
            "",
            "## Protection",
            "",
            f"v2 conclusion preserved for `{protection['preservesV2Conclusion']['candidateId']}`. 1:1 is not a production gate. Do not infer that wider stops are universally better.",
            "",
            "## Do not authorize",
            "",
            "No market evaluation, 30-slot co-adaptation launch, G6, V37 continuation, V38 archive breeding, family reweight, quota change, gate weakening, morphology nursery, or 1:1 production gate.",
            "",
        ]
    )


def load_prepared_slots(
    *,
    v38_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Mapping[str, Any]]]]:
    matrix = load_matrix(v38_root)
    generation = v38_generation_root(v38_root)
    parent_material = load_parent_material(generation / "proposal" / "parent-material.jsonl")
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
    slots = attach_archive_v3(slots, v38_root=v38_root)
    slots = attach_panel_relatives(
        slots,
        panel_rows={"panel-1": panel1, "panel-2": panel2, "panel-3": panel3},
        parent_ids=[parent["candidateId"] for parent in matrix["parents"]],
    )
    evaluated = {"panel-1": panel1, "panel-2": panel2, "panel-3": panel3}
    return matrix, slots, parent_material, baselines, evaluated


def run_audit_v3(
    *,
    v38_root: Path,
    catalog_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    matrix, slots, parent_material, baselines, evaluated = load_prepared_slots(v38_root=v38_root)
    event = build_event_insert_forensic_v3(slots, parent_material, baselines, evaluated["panel-3"])
    multipanel = build_multipanel_report_v3(slots, evaluated=evaluated, parent_material=parent_material)
    protection = build_protection_report_v3(slots, evaluated["panel-3"], parent_material, baselines)
    launch = build_resource_launch_spec(matrix=matrix, slots=slots, parent_material=parent_material)
    coadaptation = build_coadaptation_spec_v3(
        matrix=matrix,
        slots=slots,
        parent_material=parent_material,
        event_report=event,
    )
    memo = decision_memo_v3(
        event=event,
        multipanel=multipanel,
        protection=protection,
        coadaptation=coadaptation,
        launch=launch,
    )
    outputs: dict[str, tuple[Mapping[str, Any] | None, str]] = {
        "v38-directional-event-insert-forensic-v3.json": (event, event_markdown_v3(event)),
        "v38-multipanel-suboperation-v3.json": (multipanel, multipanel_markdown_v3(multipanel)),
        "v38-initial-protection-tail-forensic-v3.json": (protection, protection_markdown_v3(protection)),
        "resource-suboperation-launch-manifest-v1.json": (launch, resource_launch_markdown(launch)),
        "topology-coadaptation-matrix-spec-v3.json": (coadaptation, coadaptation_plan_markdown_v3(coadaptation)),
    }
    written: dict[str, Path] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        if payload is not None:
            write_report(path, payload, markdown)
        written[name] = path
    memo_path = output_dir / "decision-memo-v3.md"
    memo_path.write_text(memo, encoding="utf-8", newline="\n")
    written["decision-memo-v3.md"] = memo_path
    plan_path = output_dir / "topology-coadaptation-research-plan-v3.md"
    plan_path.write_text(coadaptation_plan_markdown_v3(coadaptation), encoding="utf-8", newline="\n")
    written["topology-coadaptation-research-plan-v3.md"] = plan_path
    readme = output_dir / "README-v3.md"
    readme.write_text(
        "\n".join(
            [
                "# V38 evolve-everything follow-up artifacts v3",
                "",
                "Generated locally from the V38 run and current source. No new market evaluation. v1 and v2 files in this folder are unchanged.",
                "",
                "Regenerate with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v3 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README-v3.md"] = readme
    index = output_dir / "README.md"
    existing = index.read_text(encoding="utf-8") if index.is_file() else ""
    marker = "## v3"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    index.write_text(
        existing.rstrip()
        + "\n\n## v3\n\n"
        + "\n".join(
            [
                "v1 and v2 files above are frozen. v3 corrects event-insert mechanism labels, exact multi-panel economics, qd19 wording, phenotype breadth, the launch-grade resource slot manifest, and parent-bound topology-local co-adaptation.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-directional-event-insert-forensic-v3.json` / `.md` | A/B |",
                "| `v38-multipanel-suboperation-v3.json` / `.md` | C |",
                "| `resource-suboperation-launch-manifest-v1.json` / `.md` | D |",
                "| `topology-coadaptation-matrix-spec-v3.json` | E |",
                "| `topology-coadaptation-research-plan-v3.md` | E |",
                "| `v38-initial-protection-tail-forensic-v3.json` / `.md` | F |",
                "| `decision-memo-v3.md` | memo |",
                "| `README-v3.md` | index |",
                "",
                "Regenerate v3 with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v3 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        ),
        encoding="utf-8",
        newline="\n",
    )
    written["README.md"] = index
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v38-root", type=Path, default=DEFAULT_V38_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    written = run_audit_v3(v38_root=args.v38_root, catalog_path=args.catalog, output_dir=args.output_dir)
    for path in written.values():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
