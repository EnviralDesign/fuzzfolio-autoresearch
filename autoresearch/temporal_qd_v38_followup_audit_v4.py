"""V38 follow-up audit v4: 2x2 co-adaptation, archive forensic, honest inventory.

Does not mutate v1/v2/v3 reports. Does not launch a generation, worker, Vast
host, or market evaluation.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evolvable_module_genome import (
    EvolvableModuleCompilerV1,
    decode_program,
    evolvable_resource_fingerprint,
)
from .evolvable_module_resource_operators import GenomeResourceOperatorLayer
from .evolvable_module_topology import apply_plan, make_plan
from .evidence_plan import canonical_sha256
from .temporal_indicator_learning_v1 import IndicatorLearningCatalog
from .temporal_qd_resource_suboperation_inventory import (
    INVENTORY_SCHEMA,
    build_resource_suboperation_balanced_design_proposal,
    build_resource_suboperation_candidate_inventory,
    validate_resource_suboperation_candidate_inventory,
)
from .temporal_qd_resource_suboperation_launch import (
    validate_resource_suboperation_launch_manifest,
)
from .temporal_qd_topology_coadaptation_v3 import (
    ARMS as V3_ARMS,
    validate_topology_coadaptation_matrix_v3,
)
from .temporal_qd_topology_coadaptation_v4 import (
    ARM_E,
    ARM_P,
    ARM_T,
    ARM_TE,
    ARMS,
    BLOCK_CLASS_COMPLETE,
    BLOCK_CLASS_INCOMPLETE,
    COADAPTATION_SCHEMA,
    EVENT_ONLY_SITE_LABEL,
    added_setup_node_id,
    attach_topology_coadaptation_matrix_v4,
    build_topology_coadaptation_matrix_v4,
    coadaptation_interaction,
    promising_coadaptation_observation,
    topology_plan_sha256,
    topology_semantic_delta_sha256,
    validate_topology_coadaptation_matrix_v4,
)
from .temporal_qd_v38_followup_audit import (
    DEFAULT_CATALOG,
    DEFAULT_OUTPUT,
    DEFAULT_V38_ROOT,
    v38_generation_root,
    write_report,
    _md_table,
)
from .temporal_qd_v38_followup_audit_v2 import (
    METRIC_IDENTITY_FLOOR_R,
    V38_ROTATING_EVIDENCE_SHA256,
    canonical_metric_greater,
    canonical_metrics_equal,
    _as_float,
    _self_hash,
)
from .temporal_qd_v38_followup_audit_v3 import (
    CATALOG_SOURCE,
    EVENT_INSERT_SCHEMA_V3,
    MULTI_PANEL_SCHEMA_V3,
    SUPPORT_TRADE_FLOOR,
    WORKER_CONTRACT_SHA256,
    build_coadaptation_spec_v3,
    build_event_insert_forensic_v3,
    build_multipanel_report_v3,
    build_resource_launch_spec,
    load_prepared_slots,
    phenotype_identity,
)

EVENT_INSERT_SCHEMA_V4 = "temporal_qd_v38_directional_event_insert_forensic_v4"
MULTI_PANEL_SCHEMA_V4 = "temporal_qd_v38_multipanel_suboperation_v4"
ARCHIVE_FORENSIC_SCHEMA_V4 = "temporal_qd_v38_cumulative_event_child_archive_forensic_v4"
FOCUS_CHILD_ID = "qd_686f15941b1f07e6273929c8c2a0"
FOCUS_PARENT_ID = "qd_69e5a3407ab21e82d787eb48c8d5"
SIDES = ("long", "short")
PANELS = ("panel-1", "panel-2", "panel-3")
GROSS_COST_CLASSES = (
    "gross_improving_and_net_improving",
    "gross_worsening_net_improving_via_cost",
    "gross_and_net_tie_or_pure_suppression",
    "zero_trade_suppression",
    "other_parent_beat",
)


def classify_gross_versus_cost(case: Mapping[str, Any]) -> str | None:
    relative = case.get("relative") if isinstance(case.get("relative"), Mapping) else {}
    if relative.get("comparable") is not True or relative.get("beatParent") is not True:
        return None
    metrics = case.get("metrics") if isinstance(case.get("metrics"), Mapping) else {}
    parent = case.get("parentMetrics") if isinstance(case.get("parentMetrics"), Mapping) else {}
    trades = int(metrics.get("tradeCount") or 0)
    if trades == 0:
        return "zero_trade_suppression"
    child_gross = _as_float(metrics.get("grossNoCostNetR"))
    parent_gross = _as_float(parent.get("grossNoCostNetR"))
    child_net = _as_float(metrics.get("cumulativeConservativeNetR"))
    parent_net = _as_float(parent.get("cumulativeConservativeNetR"))
    if child_gross is None or parent_gross is None or child_net is None or parent_net is None:
        return "other_parent_beat"
    gross_delta = child_gross - parent_gross
    net_delta = child_net - parent_net
    if canonical_metrics_equal(gross_delta, 0) and (
        canonical_metrics_equal(net_delta, 0) or not canonical_metric_greater(net_delta, 0)
    ):
        return "gross_and_net_tie_or_pure_suppression"
    if canonical_metric_greater(gross_delta, 0) and canonical_metric_greater(net_delta, 0):
        return "gross_improving_and_net_improving"
    if canonical_metric_greater(0, gross_delta) and canonical_metric_greater(net_delta, 0):
        return "gross_worsening_net_improving_via_cost"
    if canonical_metrics_equal(gross_delta, 0) and canonical_metric_greater(net_delta, 0):
        return "gross_and_net_tie_or_pure_suppression"
    return "other_parent_beat"


def _side_panel_table(children: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    table: dict[str, Any] = {}
    for side in SIDES:
        table[side] = {}
        group = [child for child in children if child.get("side") == side]
        for panel in PANELS:
            rows = [child["panels"][panel] for child in group if isinstance(child.get("panels"), Mapping)]
            available = [row for row in rows if row.get("available") is True]
            table[side][panel] = {
                "available": len(available),
                "beats": sum(1 for row in available if row.get("beatParent") is True),
                "losses": sum(1 for row in available if row.get("lostToParent") is True),
                "ties": sum(1 for row in available if row.get("fullEconomicPhenotypeTie") is True),
                "absolutePositive": sum(1 for row in available if row.get("absolutePositive") is True),
                "supportEligible": sum(1 for row in available if row.get("supportEligible") is True),
                "directionEligible": sum(1 for row in available if row.get("directionEligible") is True),
                "qualityLike": sum(1 for row in available if row.get("qualityLike") is True),
            }
    return table


def build_event_insert_forensic_v4(
    slots: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Mapping[str, Any]],
    evaluated: Mapping[str, Mapping[str, Any]],
    multipanel: Mapping[str, Any],
) -> dict[str, Any]:
    v3 = build_event_insert_forensic_v3(slots, parent_material, baselines, evaluated)
    cases = []
    for case in v3.get("cases") or []:
        row = dict(case)
        row["grossVersusCostClass"] = classify_gross_versus_cost(row)
        cases.append(row)
    archive_comparable = [
        case
        for case in cases
        if case.get("parentRole") == "archive" and (case.get("relative") or {}).get("comparable") is True
    ]
    beats = [case for case in archive_comparable if (case.get("relative") or {}).get("beatParent")]
    partition = Counter(case["grossVersusCostClass"] for case in beats)
    children = list(multipanel.get("children") or [])
    body = {
        "schemaVersion": EVENT_INSERT_SCHEMA_V4,
        "preservesV3": {
            "schemaVersion": EVENT_INSERT_SCHEMA_V3,
            "reportSha256": v3.get("reportSha256"),
            "operatorLabel": v3.get("operatorLabel"),
            "durableProfitableHeritability": v3.get("durableProfitableHeritability"),
        },
        "metricEquality": v3.get("metricEquality"),
        "metricIdentityFloorR": METRIC_IDENTITY_FLOOR_R,
        "operatorLabel": "typed_confirmation_activity_selection_operator",
        "durableProfitableHeritability": "not_demonstrated",
        "acceptedChildren": v3.get("acceptedChildren"),
        "archiveParentComparable": v3.get("archiveParentComparable"),
        "activityCostMechanism": v3.get("activityCostMechanism"),
        "grossVersusCostPartitionOfPanel3ParentBeats": {
            "parentBeats": len(beats),
            **{key: int(partition.get(key) or 0) for key in GROSS_COST_CLASSES},
        },
        "sideByPanel": _side_panel_table(children),
        "shortSideReversesAcrossPanels": True,
        "shortSideReversalStatement": (
            "On the V38 discovery panel, selected short-side event gates suppressed harmful activity; "
            "the same short-side mechanism reverses between panel-1, panel-2, and panel-3. "
            "This is a regime-dependent brake, not a universal alpha generator. "
            "Population-level OR across children is not same-child persistence."
        ),
        "populationOrIsNotSameChildPersistence": True,
        "breadth": {
            "acceptedGenotypes": len({case.get("candidateId") for case in archive_comparable}),
            "distinctResolvedPrograms": len(
                {case.get("resolvedProgramSha256") for case in archive_comparable if case.get("resolvedProgramSha256")}
            ),
            "distinctRealizedPhenotypes": len(
                {
                    case.get("phenotypeIdentitySha256") or phenotype_identity(case.get("metrics"))
                    for case in archive_comparable
                    if case.get("phenotypeIdentitySha256") or case.get("metrics")
                }
            ),
        },
        "exclusiveOutcomeLadder": v3.get("exclusiveOutcomeLadder"),
        "bySide": v3.get("bySide"),
        "byNodeZone": v3.get("byNodeZone"),
        "cases": sorted(cases, key=lambda item: str(item.get("candidateId") or "")),
        "limitations": list(v3.get("limitations") or [])
        + [
            "gross_versus_cost_partition_is_descriptive_on_panel_3_archive_parent_beats",
            "short_side_reversal_forbids_pooling_all_short_v38_benefit_with_long_cells",
        ],
    }
    return _self_hash(body)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _archive_members(archive: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    members: dict[str, dict[str, Any]] = {}
    for cell in archive.get("cells") or []:
        if not isinstance(cell, Mapping):
            continue
        cell_members = [item for item in (cell.get("members") or []) if isinstance(item, Mapping)]
        for member in cell_members:
            candidate_id = member.get("candidateId")
            if not isinstance(candidate_id, str):
                continue
            members[candidate_id] = {
                "candidateId": candidate_id,
                "cellId": cell.get("cellId"),
                "descriptor": cell.get("descriptor"),
                "archiveLane": member.get("archiveLane"),
                "retentionReason": member.get("retentionReason"),
                "robustBreederEligible": member.get("robustBreederEligible"),
                "paretoFront": member.get("paretoFront"),
                "crowdingDistance": member.get("crowdingDistance"),
                "objectives": member.get("objectives") or member.get("robustObjectives"),
                "cellCapacity": archive.get("cellCapacity"),
                "cellMemberCount": len(cell_members),
                "competingMemberIds": [
                    item.get("candidateId")
                    for item in cell_members
                    if isinstance(item.get("candidateId"), str) and item.get("candidateId") != candidate_id
                ],
            }
    return members


def _cumulative_members(cumulative: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    members: dict[str, dict[str, Any]] = {}
    quality = {str(item) for item in (cumulative.get("qualityCandidateIds") or []) if isinstance(item, str)}
    frontier = {str(item) for item in (cumulative.get("frontierCandidateIds") or []) if isinstance(item, str)}
    for member in cumulative.get("members") or []:
        if not isinstance(member, Mapping) or not isinstance(member.get("candidateId"), str):
            continue
        candidate_id = member["candidateId"]
        members[candidate_id] = {
            "candidateId": candidate_id,
            "cellId": member.get("cellId"),
            "robustBreederLane": member.get("robustBreederLane"),
            "robustBreederEligible": member.get("robustBreederEligible"),
            "inQualityIds": candidate_id in quality,
            "inFrontierIds": candidate_id in frontier,
            "programSha256": member.get("programSha256") or member.get("resolvedProgramSha256"),
            "windowMetrics": member.get("windowMetrics"),
            "currentPanelRank": member.get("currentPanelRank"),
            "novelty": member.get("novelty"),
        }
    return members


def _disposition(
    *,
    archive_member: Mapping[str, Any] | None,
    cumulative_member: Mapping[str, Any] | None,
    child: Mapping[str, Any] | None,
    same_cell_archive_ids: Sequence[str],
) -> dict[str, Any]:
    if archive_member is not None:
        return {
            "reasonCode": archive_member.get("retentionReason") or "retained_in_final_archive",
            "finalArchiveMember": True,
            "explanation": "Candidate occupies the published final archive.",
            "competingMemberIds": list(archive_member.get("competingMemberIds") or []),
        }
    if cumulative_member is None:
        panels = (child or {}).get("panels") if isinstance(child, Mapping) else {}
        available = {
            name: isinstance(panels.get(name), Mapping) and panels[name].get("available") is True
            for name in PANELS
        }
        if not available.get("panel-1") and not available.get("panel-2"):
            reason = "absent_from_cumulative_archive_and_independent_panels_unobserved"
            explanation = (
                "The child is absent from cumulative-archive.json and was not backfilled onto panel-1/panel-2."
            )
        else:
            reason = "absent_from_cumulative_archive_after_backfill"
            explanation = (
                "The child has inspected replication-panel rows but is absent from cumulative-archive.json."
            )
        return {
            "reasonCode": reason,
            "finalArchiveMember": False,
            "explanation": explanation,
            "competingMemberIds": list(same_cell_archive_ids),
        }
    if cumulative_member.get("inQualityIds") or cumulative_member.get("inFrontierIds"):
        return {
            "reasonCode": "cumulative_admitted_then_cell_capacity_removed",
            "finalArchiveMember": False,
            "explanation": (
                "The child appears in the cumulative quality or frontier ID set but not in archive.json, "
                "so per-cell capacity projection after Pareto admission removed it."
            ),
            "competingMemberIds": list(same_cell_archive_ids),
        }
    lane = cumulative_member.get("robustBreederLane")
    return {
        "reasonCode": "failed_cumulative_support_direction_or_economics",
        "finalArchiveMember": False,
        "explanation": (
            f"Cumulative robust breeder lane is {lane!r} and robustBreederEligible="
            f"{cumulative_member.get('robustBreederEligible')!r}; the child never entered "
            "qualityCandidateIds/frontierCandidateIds, so cell capacity was not the binding exclusion."
        ),
        "competingMemberIds": list(same_cell_archive_ids),
    }


def build_archive_forensic_v4(
    *,
    v38_root: Path,
    event_report: Mapping[str, Any],
    multipanel: Mapping[str, Any],
) -> dict[str, Any]:
    generation = v38_generation_root(v38_root)
    archive = _load_json(generation / "native-finalization" / "archive.json")
    cumulative = _load_json(generation / "native-finalization" / "evidence" / "cumulative-archive.json")
    archive_members = _archive_members(archive)
    cumulative_members = _cumulative_members(cumulative)
    archive_by_cell: dict[str, list[str]] = defaultdict(list)
    for member in archive_members.values():
        cell_id = member.get("cellId")
        if isinstance(cell_id, str):
            archive_by_cell[cell_id].append(member["candidateId"])
    children_by_id = {
        str(child.get("candidateId")): child
        for child in (multipanel.get("children") or [])
        if isinstance(child.get("candidateId"), str)
    }
    event_cases = [case for case in (event_report.get("cases") or []) if case.get("parentRole") == "archive"]
    backfilled = [
        child for child in (multipanel.get("children") or []) if child.get("enteredBackfillCohort") is True
    ]
    records = []
    for case in event_cases:
        candidate_id = str(case.get("candidateId") or "")
        child = children_by_id.get(candidate_id)
        archive_member = archive_members.get(candidate_id)
        cumulative_member = cumulative_members.get(candidate_id)
        cell_id = None if cumulative_member is None else cumulative_member.get("cellId")
        disposition = _disposition(
            archive_member=archive_member,
            cumulative_member=cumulative_member,
            child=child,
            same_cell_archive_ids=archive_by_cell.get(str(cell_id), []),
        )
        records.append(
            {
                "candidateId": candidate_id,
                "parentCandidateId": case.get("parentCandidateId"),
                "side": case.get("side"),
                "indicatorId": case.get("indicatorId"),
                "enteredBackfillCohort": bool(child and child.get("enteredBackfillCohort")),
                "panels": None if child is None else child.get("panels"),
                "panel3Metrics": case.get("metrics"),
                "panel3Relative": case.get("relative"),
                "cumulativeMember": cumulative_member,
                "finalArchiveMember": disposition["finalArchiveMember"],
                "descriptorCell": None if archive_member is None else archive_member.get("descriptor"),
                "cellId": cell_id,
                "reasonCode": disposition["reasonCode"],
                "explanation": disposition["explanation"],
                "competingMemberIds": disposition["competingMemberIds"],
            }
        )
    focus = next((item for item in records if item["candidateId"] == FOCUS_CHILD_ID), None)
    body = {
        "schemaVersion": ARCHIVE_FORENSIC_SCHEMA_V4,
        "notAdmittedToFinalArchiveIsNotATerminalExplanation": True,
        "backfilledEventChildCount": len(backfilled),
        "archiveParentEventChildCount": len(records),
        "finalArchiveEventChildCount": sum(1 for item in records if item["finalArchiveMember"] is True),
        "focusChildId": FOCUS_CHILD_ID,
        "focusParentId": FOCUS_PARENT_ID,
        "focusRecord": focus,
        "records": sorted(records, key=lambda item: str(item.get("candidateId") or "")),
        "limitations": [
            "cumulative_and_final_archive_are_read_only_v38_artifacts",
            "absence_from_quality_and_frontier_sets_is_an_exact_disposition",
        ],
    }
    return _self_hash(body)


def _decode_parent_program(program: Mapping[str, Any] | None) -> Any | None:
    if not isinstance(program, Mapping):
        return None
    try:
        return decode_program(
            program_kind=str(program.get("programKind") or ""),
            codec=str(program.get("codec") or ""),
            payload=program,
        )
    except Exception:
        return None


def _compile_identities(genome: Any, *, candidate_id: str) -> dict[str, Any] | None:
    try:
        compiled = EvolvableModuleCompilerV1().compile(genome, candidate_id=candidate_id)
    except Exception:
        return None
    return {
        "genomeSha256": genome.identity_sha256,
        "programSha256": genome.identity_sha256,
        "profileSha256": compiled.get("profileSha256"),
        "topologySignature": compiled.get("semanticTopologySha256") or genome.semantic_topology_signature(),
        "resourceFingerprint": evolvable_resource_fingerprint(genome),
        "nativeCompileValidationIdentity": compiled.get("compilerPolicySha256"),
        "pairIdentitySha256": canonical_sha256(
            {"candidateId": candidate_id, "genomeSha256": genome.identity_sha256}
        ),
        "genome": genome,
    }


def _catalog_layer(catalog_path: Path) -> GenomeResourceOperatorLayer | None:
    if not catalog_path.is_file():
        return None
    try:
        return GenomeResourceOperatorLayer(IndicatorLearningCatalog(json.loads(catalog_path.read_text(encoding="utf-8"))))
    except Exception:
        return None


def _inventory_from_launch(
    launch: Mapping[str, Any],
    *,
    parent_material: Mapping[str, Mapping[str, Any]],
    catalog_path: Path,
    note: str,
) -> dict[str, Any]:
    layer = _catalog_layer(catalog_path)
    proven: dict[tuple[str, str, str], dict[str, Any]] = {}
    parent_genomes: dict[tuple[str, str], Any] = {}
    if layer is not None:
        for parent in launch.get("parents") or []:
            parent_id = str(parent["candidateId"])
            material = parent_material.get(parent_id) or {}
            for side in SIDES:
                genome = _decode_parent_program(material.get("longProgram" if side == "long" else "shortProgram"))
                if genome is None:
                    continue
                parent_genomes[(parent_id, side)] = genome
                for plan in layer.enumerate_plans(genome):
                    construction = plan.get("construction")
                    if not isinstance(construction, Mapping):
                        continue
                    proven[(parent_id, side, canonical_sha256(dict(construction)))] = {
                        "plan": plan,
                        "genome": genome,
                    }
    slots: list[dict[str, Any]] = []
    for item in launch.get("slots") or []:
        parent_id = str(item.get("parentCandidateId") or "")
        side = str(item.get("side") or "")
        construction = dict(item.get("construction") or {})
        construction_sha = item.get("constructionSha256") or canonical_sha256(construction)
        parent = next((row for row in (launch.get("parents") or []) if row.get("candidateId") == parent_id), None)
        program_sha = None if parent is None else parent["longProgramSha256" if side == "long" else "shortProgramSha256"]
        match = proven.get((parent_id, side, construction_sha))
        identities = None
        application = None
        source = "rejected_not_an_authoritative_applicable_plan"
        eligibility = "ineligible"
        if match is not None and construction.get("kind") == item.get("lane"):
            try:
                child, application = layer.apply(match["genome"], match["plan"])  # type: ignore[union-attr]
                identities = _compile_identities(child, candidate_id=f"{parent_id}-{side}-{item.get('lane')}")
                if identities is not None:
                    eligibility = "eligible"
                    recovered = item.get("v38OperatorPlanSha256")
                    source = (
                        "v38_accepted_recovered_authoritative_plan"
                        if isinstance(recovered, str) and recovered.startswith("sha256:")
                        else "authoritative_enumerate_plans_applicable"
                    )
            except Exception:
                identities = None
        slot = {
            "slotId": item.get("slotId"),
            "lane": item.get("lane"),
            "parentCandidateId": parent_id,
            "side": side,
            "site": item.get("site"),
            "construction": construction,
            "constructionSha256": construction_sha,
            "planSha256": None if identities is None else (match or {}).get("plan", {}).get("planSha256"),
            "operatorSpecSha256": None if identities is None else (match or {}).get("plan", {}).get("operatorSpecSha256"),
            "catalogSha256": None if identities is None else (match or {}).get("plan", {}).get("catalogSha256"),
            "parentGenomeSha256": None if parent is None else program_sha,
            "parentProgramSha256": program_sha,
            "childGenomeSha256": None if identities is None else identities["genomeSha256"],
            "childProgramSha256": None if identities is None else identities["programSha256"],
            "childProfileSha256": None if identities is None else identities["profileSha256"],
            "applicationSha256": None if application is None else application.get("applicationSha256"),
            "v38OperatorPlanSha256": item.get("v38OperatorPlanSha256"),
            "eligibility": eligibility,
            "source": source,
            "cloneControl": False,
        }
        slots.append(slot)
    clone_slots: list[dict[str, Any]] = []
    for parent in launch.get("parents") or []:
        for side in SIDES:
            program = parent["longProgramSha256"] if side == "long" else parent["shortProgramSha256"]
            clone_slots.append(
                {
                    "slotId": f"clone|{parent['candidateId']}|{side}",
                    "parentCandidateId": parent["candidateId"],
                    "side": side,
                    "eligibility": "eligible",
                    "parentGenomeSha256": program,
                    "parentProgramSha256": program,
                    "source": "explicit_parent_clone_control",
                }
            )
    inventory = build_resource_suboperation_candidate_inventory(
        parents=list(launch.get("parents") or []),
        rotating_evidence_sha256=str(
            (launch.get("panelIdentities") or {}).get("rotatingEvidenceSha256") or V38_ROTATING_EVIDENCE_SHA256
        ),
        worker_contract_sha256=str(
            (launch.get("sourceIdentities") or {}).get("workerContractSha256") or WORKER_CONTRACT_SHA256
        ),
        catalog_source=str((launch.get("sourceIdentities") or {}).get("catalogSource") or CATALOG_SOURCE),
        slots=slots,
        empty_cells=list(launch.get("emptyCells") or []),
        clone_slots=clone_slots,
        note=note,
    )
    del parent_genomes
    return validate_resource_suboperation_candidate_inventory(inventory)


def _receipt(
    *,
    block_id: str,
    arm: str,
    parent_id: str,
    side: str,
    eligibility: str,
    identities: Mapping[str, Any] | None,
    delta: Mapping[str, Any] | None,
    audit: Mapping[str, Any] | None,
    attaches: bool | None,
    failure: str | None,
) -> dict[str, Any]:
    identities = identities or {}
    return {
        "receiptId": f"{block_id}|{arm}",
        "blockId": block_id,
        "arm": arm,
        "parentCandidateId": parent_id,
        "side": side,
        "eligibility": eligibility,
        "genomeSha256": identities.get("genomeSha256"),
        "programSha256": identities.get("programSha256"),
        "profileSha256": identities.get("profileSha256"),
        "topologySignature": identities.get("topologySignature"),
        "resourceFingerprint": identities.get("resourceFingerprint"),
        "pairIdentitySha256": identities.get("pairIdentitySha256"),
        "nativeCompileValidationIdentity": identities.get("nativeCompileValidationIdentity"),
        "topologySemanticDelta": delta,
        "operatorApplicationAudit": audit,
        "eventAttachesToAddedSetupNode": attaches,
        "productionArchiveWrite": False,
        "failureReason": failure,
    }


def _event_plan_for_node(layer: GenomeResourceOperatorLayer, genome: Any, *, indicator_id: str, node_id: str) -> dict[str, Any] | None:
    matches = []
    for plan in layer.enumerate_plans(genome):
        construction = plan.get("construction") or {}
        if (
            construction.get("kind") == "directional_event_insert"
            and construction.get("indicatorId") == indicator_id
            and construction.get("nodeId") == node_id
        ):
            matches.append(plan)
    if not matches:
        return None
    return sorted(matches, key=lambda item: str(item.get("planSha256") or ""))[0]


def build_coadaptation_spec_v4(
    *,
    v3_spec: Mapping[str, Any],
    parent_material: Mapping[str, Mapping[str, Any]],
    catalog_path: Path,
) -> dict[str, Any]:
    validate_topology_coadaptation_matrix_v3(v3_spec)
    layer = _catalog_layer(catalog_path)
    parents = [dict(item) for item in (v3_spec.get("parents") or [])]
    parent_by_id = {item["candidateId"]: item for item in parents}
    v3_plans = {item["planId"]: item for item in (v3_spec.get("topologyPlans") or [])}
    v3_events = {item["primitiveId"]: item for item in (v3_spec.get("eventPrimitives") or [])}
    grouped: dict[tuple[str, str], dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for slot in v3_spec.get("slots") or []:
        parent_id = str(slot.get("parentCandidateId") or "")
        side = slot.get("side")
        arm = slot.get("arm")
        if side in SIDES and arm in ARMS:
            grouped[(parent_id, side)][str(arm)] = slot
        elif arm == ARM_P:
            for side_name in SIDES:
                grouped[(parent_id, side_name)][ARM_P] = {
                    **dict(slot),
                    "side": side_name,
                    "slotId": f"clone|{parent_id}|{side_name}",
                }

    event_primitives = []
    for primitive in v3_spec.get("eventPrimitives") or []:
        row = dict(primitive)
        row["selectionProvenance"] = "v38_development_panel_selected_heterogeneous"
        event_primitives.append(row)

    topology_plans: list[dict[str, Any]] = []
    slots: list[dict[str, Any]] = []
    blocks: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    seen_plans: set[str] = set()
    for parent in parents:
        parent_id = parent["candidateId"]
        material = parent_material.get(parent_id) or {}
        for side in SIDES:
            block_id = f"block|{parent_id}|{side}"
            arms = grouped.get((parent_id, side), {})
            t_slot = arms.get(ARM_T)
            e_slot = arms.get(ARM_E)
            te_slot = arms.get(ARM_TE)
            plan_record = v3_plans.get(str((t_slot or te_slot or {}).get("topologyPlanId") or ""))
            primitive = v3_events.get(str((e_slot or te_slot or {}).get("eventPrimitiveId") or ""))
            program = material.get("longProgram" if side == "long" else "shortProgram")
            parent_genome = _decode_parent_program(program if isinstance(program, Mapping) else None)
            parent_identities = None if parent_genome is None else _compile_identities(
                parent_genome, candidate_id=f"{parent_id}-{side}-P"
            )
            actual_delta = None
            topology_identities = None
            event_identities = None
            combined_identities = None
            event_audit = None
            combined_audit = None
            added = None if plan_record is None else plan_record.get("addedSetupNodeId")
            frozen_plan = None
            if plan_record is not None and parent_genome is not None:
                try:
                    arguments = dict((plan_record.get("topologyPlan") or {}).get("arguments") or {})
                    plan = make_plan(parent_genome, operation="insert_setup", **arguments)
                    application = apply_plan(parent_genome, plan)
                    actual_delta = application.delta.canonical()
                    added = added_setup_node_id(plan.canonical())
                    topology_identities = _compile_identities(
                        application.genome, candidate_id=f"{parent_id}-{side}-T"
                    )
                    frozen_plan = {
                        "planId": plan_record["planId"],
                        "parentCandidateId": parent_id,
                        "side": side,
                        "topologyPlan": plan.canonical(),
                        "planSha256": topology_plan_sha256(plan.canonical()),
                        "addedSetupNodeId": added,
                        "applicability": "source_genome_matches_parent_side_program",
                        "topologySemanticDelta": actual_delta,
                        "topologySemanticDeltaSha256": topology_semantic_delta_sha256(actual_delta),
                    }
                    if frozen_plan["planId"] not in seen_plans:
                        topology_plans.append(frozen_plan)
                        seen_plans.add(frozen_plan["planId"])
                    plan_record = frozen_plan
                    if layer is not None and primitive is not None:
                        event_plan = _event_plan_for_node(
                            layer,
                            parent_genome,
                            indicator_id=str(primitive.get("indicatorId") or ""),
                            node_id=str((e_slot or {}).get("settlingNodeId") or primitive.get("originalNodeId") or ""),
                        )
                        if event_plan is not None:
                            event_child, event_audit = layer.apply(parent_genome, event_plan)
                            event_identities = _compile_identities(
                                event_child, candidate_id=f"{parent_id}-{side}-E"
                            )
                        te_plan = _event_plan_for_node(
                            layer,
                            application.genome,
                            indicator_id=str(primitive.get("indicatorId") or ""),
                            node_id=str(added),
                        )
                        if te_plan is not None:
                            te_child, combined_audit = layer.apply(application.genome, te_plan)
                            combined_identities = _compile_identities(
                                te_child, candidate_id=f"{parent_id}-{side}-TE"
                            )
                except Exception:
                    frozen_plan = None
                    actual_delta = None
                    topology_identities = None
            if plan_record is not None and frozen_plan is None and plan_record["planId"] not in seen_plans:
                # Keep the recovered plan visible, but do not claim an application delta.
                pass

            complete = (
                parent_identities is not None
                and topology_identities is not None
                and event_identities is not None
                and combined_identities is not None
                and frozen_plan is not None
                and primitive is not None
                and isinstance(t_slot, Mapping)
                and t_slot.get("eligibility") == "eligible"
                and isinstance(e_slot, Mapping)
                and e_slot.get("eligibility") == "eligible"
                and isinstance(te_slot, Mapping)
                and te_slot.get("eligibility") == "eligible"
            )
            arm_slot_ids = {
                ARM_P: f"clone|{parent_id}|{side}",
                ARM_T: f"topology_only|{parent_id}|{side}",
                ARM_E: f"event_only|{parent_id}|{side}",
                ARM_TE: f"topology_then_event|{parent_id}|{side}",
            }

            def _arm_slot(arm: str, template: Mapping[str, Any] | None, **overrides: Any) -> dict[str, Any]:
                base = dict(template or {})
                base.update(
                    {
                        "slotId": arm_slot_ids[arm],
                        "arm": arm,
                        "parentCandidateId": parent_id,
                        "side": side,
                        "blockId": block_id,
                    }
                )
                base.update(overrides)
                for key in ("eligibility", "topologyPlanId", "eventPrimitiveId", "settlingNodeId", "ineligibilityReason"):
                    base.setdefault(key, None)
                return {
                    "slotId": base.get("slotId"),
                    "arm": arm,
                    "parentCandidateId": parent_id,
                    "side": side,
                    "eligibility": base.get("eligibility"),
                    "blockId": block_id,
                    "topologyPlanId": base.get("topologyPlanId"),
                    "eventPrimitiveId": base.get("eventPrimitiveId"),
                    "settlingNodeId": base.get("settlingNodeId"),
                    "ineligibilityReason": base.get("ineligibilityReason"),
                }

            t_eligible = "eligible" if complete else "ineligible"
            e_eligible = "eligible" if complete else "ineligible"
            te_eligible = "eligible" if complete else "ineligible"
            slots.extend(
                [
                    _arm_slot(ARM_P, arms.get(ARM_P), eligibility="eligible", topologyPlanId=None, eventPrimitiveId=None, settlingNodeId=None, ineligibilityReason=None),
                    _arm_slot(
                        ARM_T,
                        t_slot,
                        eligibility=t_eligible,
                        topologyPlanId=None if frozen_plan is None else frozen_plan["planId"],
                        eventPrimitiveId=None,
                        settlingNodeId=None,
                        ineligibilityReason=None if t_eligible == "eligible" else "offline_topology_application_not_proven",
                    ),
                    _arm_slot(
                        ARM_E,
                        e_slot,
                        eligibility=e_eligible,
                        topologyPlanId=None,
                        eventPrimitiveId=None if primitive is None else primitive["primitiveId"],
                        settlingNodeId=None if e_slot is None else e_slot.get("settlingNodeId"),
                        ineligibilityReason=None if e_eligible == "eligible" else "offline_event_application_not_proven",
                    ),
                    _arm_slot(
                        ARM_TE,
                        te_slot,
                        eligibility=te_eligible,
                        topologyPlanId=None if frozen_plan is None else frozen_plan["planId"],
                        eventPrimitiveId=None if primitive is None else primitive["primitiveId"],
                        settlingNodeId=None if frozen_plan is None else frozen_plan["addedSetupNodeId"],
                        ineligibilityReason=None if te_eligible == "eligible" else "offline_combined_application_not_proven",
                    ),
                ]
            )
            classification = BLOCK_CLASS_COMPLETE if complete else BLOCK_CLASS_INCOMPLETE
            blocks.append(
                {
                    "blockId": block_id,
                    "parentCandidateId": parent_id,
                    "side": side,
                    "parentRole": parent_by_id[parent_id]["role"],
                    "classification": classification,
                    "topologyPlanId": None if frozen_plan is None else frozen_plan["planId"],
                    "eventPrimitiveId": None if primitive is None else primitive["primitiveId"],
                    "armSlotIds": arm_slot_ids,
                    "excludedFromPrimaryCoadaptationCalculation": classification != BLOCK_CLASS_COMPLETE,
                    "incompletenessReason": None
                    if classification == BLOCK_CLASS_COMPLETE
                    else "missing_arm_or_offline_materialization_not_proven",
                }
            )
            if classification == BLOCK_CLASS_COMPLETE:
                for arm, identities, delta, audit, attaches in (
                    (ARM_P, parent_identities, None, {"arm": ARM_P, "replayed": True, "productionArchiveWrite": False}, False),
                    (ARM_T, topology_identities, actual_delta, {"arm": ARM_T, "replayed": True, "productionArchiveWrite": False}, False),
                    (ARM_E, event_identities, None, event_audit or {"arm": ARM_E, "replayed": True, "productionArchiveWrite": False}, False),
                    (ARM_TE, combined_identities, actual_delta, combined_audit or {"arm": ARM_TE, "replayed": True, "productionArchiveWrite": False}, True),
                ):
                    row = dict(identities or {})
                    row.pop("genome", None)
                    receipts.append(
                        _receipt(
                            block_id=block_id,
                            arm=arm,
                            parent_id=parent_id,
                            side=side,
                            eligibility="eligible",
                            identities=row,
                            delta=delta,
                            audit=audit,
                            attaches=attaches,
                            failure=None,
                        )
                    )
            else:
                for arm in ARMS:
                    receipts.append(
                        _receipt(
                            block_id=block_id,
                            arm=arm,
                            parent_id=parent_id,
                            side=side,
                            eligibility="ineligible",
                            identities=None,
                            delta=None,
                            audit=None,
                            attaches=None,
                            failure="exploratory_incomplete_block",
                        )
                    )
    return build_topology_coadaptation_matrix_v4(
        parents=parents,
        rotating_evidence_sha256=str(
            (v3_spec.get("panelIdentities") or {}).get("rotatingEvidenceSha256") or V38_ROTATING_EVIDENCE_SHA256
        ),
        topology_plans=topology_plans,
        event_primitives=event_primitives,
        slots=slots,
        blocks=blocks,
        materialization_receipts=receipts,
    )


def event_markdown_v4(report: Mapping[str, Any]) -> str:
    partition = report["grossVersusCostPartitionOfPanel3ParentBeats"]
    return "\n".join(
        [
            "# V38 directional event-insert forensic v4",
            "",
            "v3 labels are preserved. This revision adds the gross-versus-cost partition and the side x panel reversal table.",
            "",
            f"Operator: **{report['operatorLabel']}**. Durable profitable heritability: **{report['durableProfitableHeritability']}**.",
            "",
            "Panel-3 archive-parent beats partitioned by mechanism:",
            "",
            _md_table(
                ["class", "count"],
                [[key, partition.get(key)] for key in GROSS_COST_CLASSES] + [["parentBeats", partition.get("parentBeats")]],
            ),
            "",
            report["shortSideReversalStatement"],
            "",
            "Population-level OR is not same-child persistence.",
            "",
            f"Breadth: `{report.get('breadth')}`",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def archive_markdown_v4(report: Mapping[str, Any]) -> str:
    focus = report.get("focusRecord") or {}
    return "\n".join(
        [
            "# V38 event-child cumulative archive forensic v4",
            "",
            "`not_admitted_to_final_archive` is not a terminal explanation.",
            "",
            f"Backfilled event children: **{report['backfilledEventChildCount']}**. Final-archive event children: **{report['finalArchiveEventChildCount']}**.",
            "",
            f"Focus child `{report['focusChildId']}` / parent `{report['focusParentId']}`:",
            "",
            f"- reason: `{focus.get('reasonCode')}`",
            f"- explanation: {focus.get('explanation')}",
            f"- competing members: `{focus.get('competingMemberIds')}`",
            f"- backfilled: `{focus.get('enteredBackfillCohort')}`",
            "",
            f"Report sha: `{report['reportSha256']}`",
            "",
        ]
    )


def coadaptation_markdown_v4(spec: Mapping[str, Any]) -> str:
    complete = sum(1 for block in spec["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    incomplete = sum(1 for block in spec["blocks"] if block["classification"] == BLOCK_CLASS_INCOMPLETE)
    return "\n".join(
        [
            "# Topology co-adaptation research plan v4",
            "",
            "Experiment-only. Do not launch. The qualifying unit is a complete P/T/E/TE block.",
            "",
            f"TE must beat both T and E under canonical metric identity. Interaction is `{spec['successCalculation']['interactionIdentity']}` and is descriptive, not a promotion margin.",
            "",
            f"Event-only site label: `{EVENT_ONLY_SITE_LABEL}`.",
            "",
            f"Complete 2x2 blocks: **{complete}**. Explicit incomplete blocks: **{incomplete}**. Incomplete blocks cannot enter the primary co-adaptation calculation.",
            "",
            f"Design scope: `{spec['designScope']}`",
            "",
            "Preferred follow-on (not launched): two predeclared event primitives per complete parent-side topology plan. One event per block cannot support operator-level repeatability.",
            "",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def inventory_markdown(spec: Mapping[str, Any]) -> str:
    budget = spec["boundedTaskProjection"]
    return "\n".join(
        [
            "# Resource-suboperation candidate inventory v1",
            "",
            "This is **not** a balanced launch matrix. Do not launch.",
            "",
            f"Eligible proven slots: **{budget['eligibleSlotCount']}**. Explicit clones: **{budget['explicitCloneCount']}**. Projected worker tasks if launched: **{budget['projectedWorkerTasksIfLaunched']}** (windows/panels included).",
            "",
            f"Contract sha: `{spec['contractSha256']}`",
            "",
        ]
    )


def decision_memo_v4(
    *,
    event: Mapping[str, Any],
    archive: Mapping[str, Any],
    coadaptation: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> str:
    partition = event["grossVersusCostPartitionOfPanel3ParentBeats"]
    complete = sum(1 for block in coadaptation["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    focus = archive.get("focusRecord") or {}
    return "\n".join(
        [
            "# V38 follow-up decision memo v4",
            "",
            "Local source audit plus existing V38 artifacts only. No new market evaluation, generation, Vast instance, or 1024x5.",
            "",
            "## Event-insert mechanism",
            "",
            f"Panel-3 archive-parent beats: {partition['parentBeats']}. Gross-and-net improving: {partition['gross_improving_and_net_improving']}. Gross-worsening net-improving via cost: {partition['gross_worsening_net_improving_via_cost']}. Zero-trade suppression: {partition['zero_trade_suppression']}.",
            "",
            event["shortSideReversalStatement"],
            "",
            "## Archive",
            "",
            f"Focus child `{FOCUS_CHILD_ID}` disposition: `{focus.get('reasonCode')}`. {focus.get('explanation')}",
            "",
            "## Contracts",
            "",
            f"Resource object is a candidate inventory, not a launch matrix (`isBalancedLaunchMatrix={inventory['isBalancedLaunchMatrix']}`). Topology v4 complete blocks: {complete}. Overlay remains off the front generation path.",
            "",
            "## Do not authorize",
            "",
            "No market evaluation, topology co-adaptation launch, resource-suboperation matrix launch, G6, V37/V38 continuation, 1024x5, production family reweighting, gate changes, morphology nursery, 1:1 production gate, or breeding from the V38 archive.",
            "",
        ]
    )


def run_audit_v4(
    *,
    v38_root: Path,
    catalog_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    matrix, slots, parent_material, baselines, evaluated = load_prepared_slots(v38_root=v38_root)
    event_v3 = build_event_insert_forensic_v3(slots, parent_material, baselines, evaluated["panel-3"])
    multipanel = build_multipanel_report_v3(slots, evaluated=evaluated, parent_material=parent_material)
    event = build_event_insert_forensic_v4(slots, parent_material, baselines, evaluated["panel-3"], multipanel)
    archive = build_archive_forensic_v4(v38_root=v38_root, event_report=event, multipanel=multipanel)
    launch = build_resource_launch_spec(matrix=matrix, slots=slots, parent_material=parent_material)
    validate_resource_suboperation_launch_manifest(launch)
    inventory = _inventory_from_launch(
        launch,
        parent_material=parent_material,
        catalog_path=catalog_path,
        note="Candidate inventory. Only authoritative applicable plans are eligible. Do not launch.",
    )
    balanced = build_resource_suboperation_balanced_design_proposal(
        parents=list(inventory["parents"]),
        inventory_slots=list(inventory["slots"]),
        children_per_eligible_cell=1,
    )
    v3_spec = build_coadaptation_spec_v3(
        matrix=matrix,
        slots=slots,
        parent_material=parent_material,
        event_report=event_v3,
    )
    validate_topology_coadaptation_matrix_v3(v3_spec)
    coadaptation = build_coadaptation_spec_v4(
        v3_spec=v3_spec,
        parent_material=parent_material,
        catalog_path=catalog_path,
    )
    validate_topology_coadaptation_matrix_v4(coadaptation)
    attach_topology_coadaptation_matrix_v4(
        {"schemaVersion": "temporal_qd_pair_generation_v2", "configSha256": "placeholder"},
        coadaptation,
    )
    memo = decision_memo_v4(event=event, archive=archive, coadaptation=coadaptation, inventory=inventory)
    outputs: dict[str, tuple[Mapping[str, Any] | None, str]] = {
        "v38-directional-event-insert-forensic-v4.json": (event, event_markdown_v4(event)),
        "v38-multipanel-suboperation-v4.json": (
            {**multipanel, "schemaVersion": MULTI_PANEL_SCHEMA_V4, "sideByPanel": event["sideByPanel"]},
            "# V38 multi-panel event-insert report v4\n\n" + event["shortSideReversalStatement"] + "\n",
        ),
        "v38-cumulative-event-child-archive-forensic-v4.json": (archive, archive_markdown_v4(archive)),
        "resource-suboperation-candidate-inventory-v1.json": (inventory, inventory_markdown(inventory)),
        "resource-suboperation-balanced-design-proposal-v2.json": (
            balanced,
            "# Resource-suboperation balanced design proposal v2\n\nDo not launch.\n",
        ),
        "topology-coadaptation-matrix-spec-v4.json": (coadaptation, coadaptation_markdown_v4(coadaptation)),
    }
    written: dict[str, Path] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        if payload is not None:
            write_report(path, payload, markdown)
        written[name] = path
    memo_path = output_dir / "decision-memo-v4.md"
    memo_path.write_text(memo, encoding="utf-8", newline="\n")
    written["decision-memo-v4.md"] = memo_path
    receipts_path = output_dir / "topology-coadaptation-materialization-receipts-v4.json"
    receipts_path.write_text(
        json.dumps(
            {
                "schemaVersion": "temporal_qd_topology_coadaptation_materialization_receipts_v4",
                "productionArchiveWrite": False,
                "receipts": coadaptation["materializationReceipts"],
                "contractSha256": coadaptation["contractSha256"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["topology-coadaptation-materialization-receipts-v4.json"] = receipts_path
    readme = output_dir / "README-v4.md"
    readme.write_text(
        "\n".join(
            [
                "# V38 evolve-everything follow-up artifacts v4",
                "",
                "Generated locally from the V38 run and current source. No new market evaluation. v1, v2, and v3 files in this folder are unchanged.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-directional-event-insert-forensic-v4.json` / `.md` | A |",
                "| `v38-multipanel-suboperation-v4.json` / `.md` | A |",
                "| `v38-cumulative-event-child-archive-forensic-v4.json` / `.md` | B |",
                "| `resource-suboperation-candidate-inventory-v1.json` / `.md` | C |",
                "| `resource-suboperation-balanced-design-proposal-v2.json` / `.md` | C |",
                "| `topology-coadaptation-matrix-spec-v4.json` / `.md` | D-G |",
                "| `topology-coadaptation-materialization-receipts-v4.json` | F |",
                "| `decision-memo-v4.md` | memo |",
                "| `README-v4.md` | index |",
                "",
                "Regenerate with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v4 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README-v4.md"] = readme
    index = output_dir / "README.md"
    existing = index.read_text(encoding="utf-8") if index.is_file() else ""
    marker = "## v4"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    index.write_text(
        existing.rstrip()
        + "\n\n## v4\n\n"
        + "\n".join(
            [
                "v1, v2, and v3 files above are frozen. v4 adds the gross-versus-cost event partition, exact cumulative archive forensic, honest resource inventory, and complete 2x2 topology co-adaptation contract.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-directional-event-insert-forensic-v4.json` / `.md` | A |",
                "| `v38-multipanel-suboperation-v4.json` / `.md` | A |",
                "| `v38-cumulative-event-child-archive-forensic-v4.json` / `.md` | B |",
                "| `resource-suboperation-candidate-inventory-v1.json` / `.md` | C |",
                "| `resource-suboperation-balanced-design-proposal-v2.json` / `.md` | C |",
                "| `topology-coadaptation-matrix-spec-v4.json` / `.md` | D-G |",
                "| `topology-coadaptation-materialization-receipts-v4.json` | F |",
                "| `decision-memo-v4.md` | memo |",
                "| `README-v4.md` | index |",
                "",
                "Regenerate v4 with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v4 --output-dir research/temporal-qd/v38-followup`",
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
    written = run_audit_v4(v38_root=args.v38_root, catalog_path=args.catalog, output_dir=args.output_dir)
    for path in written.values():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
