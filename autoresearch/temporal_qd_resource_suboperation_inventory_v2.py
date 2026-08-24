"""Resource-suboperation candidate inventory v2.

Repairs v1 clone duplication and the 4x window undercount. Five exact parent
pairs are the clone controls. Worker-task projection uses 4 windows per panel.
This object is still not a launch matrix.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_resource_suboperation_inventory import (
    EMPTY_KEYS,
    INVENTORY_SCHEMA,
    LANES,
    PARENT_KEYS,
    SLOT_KEYS,
    validate_resource_suboperation_candidate_inventory,
)
from .temporal_qd_topology_coadaptation_v5 import reconstructed_pair_program_identity_sha256

INVENTORY_SCHEMA_V2 = "temporal_qd_resource_suboperation_candidate_inventory_v2"
INVENTORY_MODE_V2 = "imbalanced_pair_candidate_inventory_not_a_balanced_launch_matrix_v2"
BALANCED_PROPOSAL_SCHEMA_V3 = "temporal_qd_resource_suboperation_balanced_design_proposal_v3"
BALANCED_PROPOSAL_MODE_V3 = "frozen_plan_ids_case_study_coverage_not_repeatability_v3"
WINDOWS_PER_PANEL = 4
INSPECTED_PANEL_COUNT = 3
FUTURE_CONFIRMATION_PANEL_COUNT = 1
PAIR_CLONE_SOURCE = "explicit_parent_pair_clone_control"
COVERAGE_KIND = "deterministic_case_study_coverage_not_repeatability"
SELECTION_RULE = "lexicographic_minimum_planSha256_among_eligible_cell_plans"
SIDES = ("long", "short")

ROOT_KEYS_V2 = (
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "notAdmittedOnFrontGenerationPath",
    "isBalancedLaunchMatrix",
    "parents",
    "lanes",
    "panelIdentities",
    "sourceIdentities",
    "slots",
    "emptyCells",
    "pairCloneSlots",
    "boundedTaskProjection",
    "contractSha256",
)
PAIR_CLONE_KEYS = (
    "slotId",
    "parentCandidateId",
    "eligibility",
    "parentLongProgramSha256",
    "parentShortProgramSha256",
    "reconstructedPairProgramIdentitySha256",
    "source",
)
BUDGET_KEYS_V2 = (
    "eligibleMutationPairCount",
    "pairCloneCount",
    "pairCandidateCount",
    "windowsPerPanel",
    "panelCount",
    "totalWindowCount",
    "projectedInspectedPanelWorkerTasks",
    "projectedWithFutureConfirmationPanel",
    "doNotLaunch",
    "note",
)
CELL_KEYS_V3 = (
    "parentCandidateId",
    "side",
    "lane",
    "eligiblePlanCount",
    "sampledCount",
    "status",
    "reason",
    "selectedPlanSha256",
    "optionalSecondPlanSha256",
    "coverageKind",
)
PROPOSAL_ROOT_KEYS_V3 = (
    "schemaVersion",
    "mode",
    "doNotLaunch",
    "childrenPerEligibleCell",
    "balanceRule",
    "selectionRule",
    "coverageKind",
    "emptyUnderfilledCellsRemainExplicit",
    "noReplacementFromAnotherLane",
    "sideStratifiedOutcomes",
    "pairCloneCount",
    "windowsPerPanel",
    "panelCount",
    "totalWindowCount",
    "projectedInspectedPanelWorkerTasks",
    "projectedWithFutureConfirmationPanel",
    "parents",
    "lanes",
    "cells",
    "pairCloneSlots",
    "contractSha256",
)


def _unexpected(label: str) -> TemporalDiscoveryContractError:
    return TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _exact_object(value: Any, required: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(required):
        raise _unexpected(label)
    return dict(value)


def _require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    if not all(char in "0123456789abcdef" for char in value[7:]):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return value


def projected_inspected_panel_worker_tasks(*, pair_candidate_count: int) -> int:
    return pair_candidate_count * WINDOWS_PER_PANEL * INSPECTED_PANEL_COUNT


def projected_with_future_confirmation_panel(*, pair_candidate_count: int) -> int:
    return pair_candidate_count * WINDOWS_PER_PANEL * (INSPECTED_PANEL_COUNT + FUTURE_CONFIRMATION_PANEL_COUNT)


def build_pair_clone_slots(parents: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    clones: list[dict[str, Any]] = []
    for parent in parents:
        parent_id = str(parent["candidateId"])
        long_sha = _require_sha(parent["longProgramSha256"], "parent longProgramSha256")
        short_sha = _require_sha(parent["shortProgramSha256"], "parent shortProgramSha256")
        clones.append(
            {
                "slotId": f"pair-clone|{parent_id}",
                "parentCandidateId": parent_id,
                "eligibility": "eligible",
                "parentLongProgramSha256": long_sha,
                "parentShortProgramSha256": short_sha,
                "reconstructedPairProgramIdentitySha256": reconstructed_pair_program_identity_sha256(
                    parent_candidate_id=parent_id,
                    long_program_sha256=long_sha,
                    short_program_sha256=short_sha,
                ),
                "source": PAIR_CLONE_SOURCE,
            }
        )
    return clones


def validate_resource_suboperation_candidate_inventory_v2(contract: Mapping[str, Any]) -> dict[str, Any]:
    if contract.get("schemaVersion") in {
        INVENTORY_SCHEMA,
        "temporal_qd_resource_suboperation_launch_manifest_v1",
        "temporal_qd_resource_suboperation_matrix_v1",
    }:
        raise TemporalDiscoveryContractError("resource inventory v2 cannot masquerade as a launch matrix or v1 inventory")
    row = _exact_object(contract, ROOT_KEYS_V2, "resource suboperation candidate inventory v2")
    if row["schemaVersion"] != INVENTORY_SCHEMA_V2:
        raise TemporalDiscoveryContractError("resource inventory v2 schema drifted")
    if row["mode"] != INVENTORY_MODE_V2:
        raise TemporalDiscoveryContractError("resource inventory v2 mode drifted")
    if row["includeCrossover"] is not False:
        raise TemporalDiscoveryContractError("resource inventory v2 must keep crossover out")
    if row["productionArchiveWrite"] is not False or row["notAdmittedOnFrontGenerationPath"] is not True:
        raise TemporalDiscoveryContractError("resource inventory v2 must stay off the production path")
    if row["isBalancedLaunchMatrix"] is not False:
        raise TemporalDiscoveryContractError("candidate inventory v2 is not a balanced launch matrix")
    if tuple(row["lanes"] or ()) != tuple(LANES):
        raise TemporalDiscoveryContractError("resource inventory v2 lanes drifted")
    parents = []
    seen_parents: set[str] = set()
    for item in row["parents"] or []:
        parent = _exact_object(item, PARENT_KEYS, "resource inventory v2 parent")
        if parent["candidateId"] in seen_parents:
            raise TemporalDiscoveryContractError("resource inventory v2 parent repeated")
        seen_parents.add(str(parent["candidateId"]))
        parents.append(
            {
                "candidateId": str(parent["candidateId"]),
                "role": str(parent["role"]),
                "longProgramSha256": _require_sha(parent["longProgramSha256"], "longProgramSha256"),
                "shortProgramSha256": _require_sha(parent["shortProgramSha256"], "shortProgramSha256"),
            }
        )
    if len(parents) != 5:
        raise TemporalDiscoveryContractError("resource inventory v2 requires the five frozen parents")
    slots = []
    eligible = 0
    for item in row["slots"] or []:
        slot = _exact_object(item, SLOT_KEYS, "resource inventory v2 slot")
        if slot["eligibility"] == "eligible":
            eligible += 1
        slots.append(slot)
    empty_cells = [_exact_object(item, EMPTY_KEYS, "resource inventory v2 empty cell") for item in row["emptyCells"] or []]
    clone_slots = []
    seen_clones: set[str] = set()
    for item in row["pairCloneSlots"] or []:
        if isinstance(item, Mapping) and "side" in item:
            raise TemporalDiscoveryContractError("pair clones cannot be duplicated by side")
        clone = _exact_object(item, PAIR_CLONE_KEYS, "resource inventory v2 pair clone")
        if clone["slotId"] in seen_clones or clone["source"] != PAIR_CLONE_SOURCE:
            raise TemporalDiscoveryContractError("resource inventory v2 pair clone drifted")
        seen_clones.add(str(clone["slotId"]))
        expected = reconstructed_pair_program_identity_sha256(
            parent_candidate_id=str(clone["parentCandidateId"]),
            long_program_sha256=_require_sha(clone["parentLongProgramSha256"], "clone long"),
            short_program_sha256=_require_sha(clone["parentShortProgramSha256"], "clone short"),
        )
        if clone["reconstructedPairProgramIdentitySha256"] != expected:
            raise TemporalDiscoveryContractError("pair clone identity drifted")
        clone_slots.append(clone)
    if len(clone_slots) != 5:
        raise TemporalDiscoveryContractError("resource inventory v2 requires exactly five pair clones")
    budget = _exact_object(row["boundedTaskProjection"], BUDGET_KEYS_V2, "boundedTaskProjection")
    pair_candidates = eligible + len(clone_slots)
    if budget["eligibleMutationPairCount"] != eligible:
        raise TemporalDiscoveryContractError("eligibleMutationPairCount drifted")
    if budget["pairCloneCount"] != 5:
        raise TemporalDiscoveryContractError("pairCloneCount drifted")
    if budget["pairCandidateCount"] != pair_candidates:
        raise TemporalDiscoveryContractError("pairCandidateCount drifted")
    if budget["windowsPerPanel"] != WINDOWS_PER_PANEL:
        raise TemporalDiscoveryContractError("windowsPerPanel must be 4 quarter windows, not panel count")
    if budget["panelCount"] != INSPECTED_PANEL_COUNT:
        raise TemporalDiscoveryContractError("panelCount drifted")
    if budget["totalWindowCount"] != WINDOWS_PER_PANEL * INSPECTED_PANEL_COUNT:
        raise TemporalDiscoveryContractError("totalWindowCount must be windowsPerPanel * panelCount")
    expected_inspected = projected_inspected_panel_worker_tasks(pair_candidate_count=pair_candidates)
    expected_future = projected_with_future_confirmation_panel(pair_candidate_count=pair_candidates)
    if budget["projectedInspectedPanelWorkerTasks"] != expected_inspected:
        raise TemporalDiscoveryContractError("projectedInspectedPanelWorkerTasks drifted")
    if budget["projectedInspectedPanelWorkerTasks"] == pair_candidates * budget["panelCount"]:
        raise TemporalDiscoveryContractError("worker-task projection used panels as windows")
    if budget["projectedWithFutureConfirmationPanel"] != expected_future:
        raise TemporalDiscoveryContractError("projectedWithFutureConfirmationPanel drifted")
    if budget["doNotLaunch"] is not True:
        raise TemporalDiscoveryContractError("resource inventory v2 must not launch")
    body = {
        "schemaVersion": INVENTORY_SCHEMA_V2,
        "mode": INVENTORY_MODE_V2,
        "includeCrossover": False,
        "cloneControl": row["cloneControl"],
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
        "isBalancedLaunchMatrix": False,
        "parents": parents,
        "lanes": list(LANES),
        "panelIdentities": row["panelIdentities"],
        "sourceIdentities": row["sourceIdentities"],
        "slots": slots,
        "emptyCells": empty_cells,
        "pairCloneSlots": clone_slots,
        "boundedTaskProjection": {
            "eligibleMutationPairCount": eligible,
            "pairCloneCount": 5,
            "pairCandidateCount": pair_candidates,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "panelCount": INSPECTED_PANEL_COUNT,
            "totalWindowCount": WINDOWS_PER_PANEL * INSPECTED_PANEL_COUNT,
            "projectedInspectedPanelWorkerTasks": expected_inspected,
            "projectedWithFutureConfirmationPanel": expected_future,
            "doNotLaunch": True,
            "note": budget["note"],
        },
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("resource inventory v2 identity drift")
    body["contractSha256"] = expected
    return body


def build_resource_suboperation_candidate_inventory_v2(v1_inventory: Mapping[str, Any]) -> dict[str, Any]:
    validated = validate_resource_suboperation_candidate_inventory(v1_inventory)
    clones = build_pair_clone_slots(validated["parents"])
    eligible = sum(1 for slot in validated["slots"] if slot.get("eligibility") == "eligible")
    pair_candidates = eligible + len(clones)
    body = {
        "schemaVersion": INVENTORY_SCHEMA_V2,
        "mode": INVENTORY_MODE_V2,
        "includeCrossover": False,
        "cloneControl": validated["cloneControl"],
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
        "isBalancedLaunchMatrix": False,
        "parents": [dict(item) for item in validated["parents"]],
        "lanes": list(validated["lanes"]),
        "panelIdentities": dict(validated["panelIdentities"]),
        "sourceIdentities": dict(validated["sourceIdentities"]),
        "slots": [dict(item) for item in validated["slots"]],
        "emptyCells": [dict(item) for item in validated["emptyCells"]],
        "pairCloneSlots": clones,
        "boundedTaskProjection": {
            "eligibleMutationPairCount": eligible,
            "pairCloneCount": 5,
            "pairCandidateCount": pair_candidates,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "panelCount": INSPECTED_PANEL_COUNT,
            "totalWindowCount": WINDOWS_PER_PANEL * INSPECTED_PANEL_COUNT,
            "projectedInspectedPanelWorkerTasks": projected_inspected_panel_worker_tasks(
                pair_candidate_count=pair_candidates
            ),
            "projectedWithFutureConfirmationPanel": projected_with_future_confirmation_panel(
                pair_candidate_count=pair_candidates
            ),
            "doNotLaunch": True,
            "note": (
                "Pair-candidate inventory. Mutation children keep the opposite side frozen. "
                "Five exact parent-pair clones. Task projection uses 4 windows per inspected panel. Do not launch."
            ),
        },
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_candidate_inventory_v2(body)


def validate_resource_suboperation_balanced_design_proposal_v3(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, PROPOSAL_ROOT_KEYS_V3, "balanced design proposal v3")
    if row["schemaVersion"] != BALANCED_PROPOSAL_SCHEMA_V3:
        raise TemporalDiscoveryContractError("balanced design proposal v3 schema drifted")
    if row["mode"] != BALANCED_PROPOSAL_MODE_V3:
        raise TemporalDiscoveryContractError("balanced design proposal v3 mode drifted")
    if row["doNotLaunch"] is not True:
        raise TemporalDiscoveryContractError("balanced design proposal v3 must not launch")
    if row["childrenPerEligibleCell"] != 1:
        raise TemporalDiscoveryContractError("one-child-per-cell is case-study coverage, not a second invented count")
    if row["selectionRule"] != SELECTION_RULE or row["coverageKind"] != COVERAGE_KIND:
        raise TemporalDiscoveryContractError("balanced design proposal v3 selection/coverage drifted")
    if row["windowsPerPanel"] != WINDOWS_PER_PANEL or row["panelCount"] != INSPECTED_PANEL_COUNT:
        raise TemporalDiscoveryContractError("balanced design proposal v3 window/panel counts drifted")
    if row["pairCloneCount"] != 5:
        raise TemporalDiscoveryContractError("balanced design proposal v3 pair clone count drifted")
    cells = []
    filled = 0
    for item in row["cells"] or []:
        cell = _exact_object(item, CELL_KEYS_V3, "balanced design cell v3")
        if cell["coverageKind"] != COVERAGE_KIND:
            raise TemporalDiscoveryContractError("cell coverageKind drifted")
        if cell["status"] == "filled":
            filled += 1
            _require_sha(cell["selectedPlanSha256"], "selectedPlanSha256")
            if cell["sampledCount"] != 1:
                raise TemporalDiscoveryContractError("filled case-study cell must freeze exactly one plan")
        else:
            if cell["selectedPlanSha256"] is not None or cell["sampledCount"] != 0:
                raise TemporalDiscoveryContractError("empty cells must not borrow a plan from another lane")
        if cell["optionalSecondPlanSha256"] is not None:
            _require_sha(cell["optionalSecondPlanSha256"], "optionalSecondPlanSha256")
            if cell["eligiblePlanCount"] < 2:
                raise TemporalDiscoveryContractError("second plan requires actual cell availability")
        cells.append(cell)
    clones = [_exact_object(item, PAIR_CLONE_KEYS, "proposal pair clone") for item in row["pairCloneSlots"] or []]
    if len(clones) != 5:
        raise TemporalDiscoveryContractError("proposal requires five pair clones")
    pair_candidates = filled + 5
    if row["projectedInspectedPanelWorkerTasks"] != projected_inspected_panel_worker_tasks(
        pair_candidate_count=pair_candidates
    ):
        raise TemporalDiscoveryContractError("proposal inspected-panel task projection drifted")
    if row["projectedWithFutureConfirmationPanel"] != projected_with_future_confirmation_panel(
        pair_candidate_count=pair_candidates
    ):
        raise TemporalDiscoveryContractError("proposal confirmation-panel task projection drifted")
    body = {key: row[key] for key in PROPOSAL_ROOT_KEYS_V3 if key != "contractSha256"}
    body["cells"] = cells
    body["pairCloneSlots"] = clones
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("balanced design proposal v3 identity drift")
    body["contractSha256"] = expected
    return body


def build_resource_suboperation_balanced_design_proposal_v3(
    *,
    inventory_v2: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_resource_suboperation_candidate_inventory_v2(inventory_v2)
    plans_by_cell: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for slot in validated["slots"]:
        if slot.get("eligibility") != "eligible":
            continue
        key = (str(slot["parentCandidateId"]), str(slot["side"]), str(slot["lane"]))
        plan_sha = slot.get("planSha256")
        if isinstance(plan_sha, str):
            plans_by_cell[key].append(plan_sha)
    for key in plans_by_cell:
        plans_by_cell[key] = sorted(set(plans_by_cell[key]))
    cells: list[dict[str, Any]] = []
    filled = 0
    for parent in validated["parents"]:
        for side in SIDES:
            for lane in LANES:
                plans = plans_by_cell.get((str(parent["candidateId"]), side, lane), [])
                if plans:
                    filled += 1
                    cells.append(
                        {
                            "parentCandidateId": parent["candidateId"],
                            "side": side,
                            "lane": lane,
                            "eligiblePlanCount": len(plans),
                            "sampledCount": 1,
                            "status": "filled",
                            "reason": None,
                            "selectedPlanSha256": plans[0],
                            "optionalSecondPlanSha256": plans[1] if len(plans) >= 2 else None,
                            "coverageKind": COVERAGE_KIND,
                        }
                    )
                else:
                    cells.append(
                        {
                            "parentCandidateId": parent["candidateId"],
                            "side": side,
                            "lane": lane,
                            "eligiblePlanCount": 0,
                            "sampledCount": 0,
                            "status": "empty",
                            "reason": "no_eligible_authoritative_plan",
                            "selectedPlanSha256": None,
                            "optionalSecondPlanSha256": None,
                            "coverageKind": COVERAGE_KIND,
                        }
                    )
    pair_candidates = filled + 5
    body = {
        "schemaVersion": BALANCED_PROPOSAL_SCHEMA_V3,
        "mode": BALANCED_PROPOSAL_MODE_V3,
        "doNotLaunch": True,
        "childrenPerEligibleCell": 1,
        "balanceRule": "equal_predeclared_count_per_eligible_parent_side_suboperation_cell",
        "selectionRule": SELECTION_RULE,
        "coverageKind": COVERAGE_KIND,
        "emptyUnderfilledCellsRemainExplicit": True,
        "noReplacementFromAnotherLane": True,
        "sideStratifiedOutcomes": True,
        "pairCloneCount": 5,
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "panelCount": INSPECTED_PANEL_COUNT,
        "totalWindowCount": WINDOWS_PER_PANEL * INSPECTED_PANEL_COUNT,
        "projectedInspectedPanelWorkerTasks": projected_inspected_panel_worker_tasks(
            pair_candidate_count=pair_candidates
        ),
        "projectedWithFutureConfirmationPanel": projected_with_future_confirmation_panel(
            pair_candidate_count=pair_candidates
        ),
        "parents": [dict(item) for item in validated["parents"]],
        "lanes": list(LANES),
        "cells": cells,
        "pairCloneSlots": [dict(item) for item in validated["pairCloneSlots"]],
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_balanced_design_proposal_v3(body)
