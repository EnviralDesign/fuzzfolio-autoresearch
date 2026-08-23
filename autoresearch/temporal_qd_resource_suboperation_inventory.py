"""Resource-suboperation candidate inventory v1.

The v1 265-slot object is an imbalanced candidate inventory, not a launch-grade
balanced matrix. This module names it honestly, strengthens validation, and
keeps a separate do-not-launch balanced-design proposal. It never schedules a
generation or market evaluation.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_resource_suboperation_matrix import LANES, PARENT_ROLES

INVENTORY_SCHEMA = "temporal_qd_resource_suboperation_candidate_inventory_v1"
INVENTORY_MODE = "imbalanced_candidate_inventory_not_a_balanced_launch_matrix_v1"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
BALANCED_PROPOSAL_SCHEMA = "temporal_qd_resource_suboperation_balanced_design_proposal_v2"
BALANCED_PROPOSAL_MODE = "equal_predeclared_count_per_parent_side_suboperation_cell_v2"
ALLOWED_SOURCES = (
    "v38_accepted_recovered_authoritative_plan",
    "authoritative_enumerate_plans_applicable",
    "explicit_parent_clone_control",
    "rejected_not_an_authoritative_applicable_plan",
)
ROOT_KEYS = (
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
    "cloneSlots",
    "boundedTaskProjection",
    "contractSha256",
)
PARENT_KEYS = ("candidateId", "role", "longProgramSha256", "shortProgramSha256")
PANEL_KEYS = (
    "developmentPanelId",
    "replicationPanelIds",
    "replicationRole",
    "rotatingEvidenceSha256",
)
SOURCE_KEYS = ("workerContractSha256", "catalogSource", "resourceOperatorId", "enumerationAuthority")
SLOT_KEYS = (
    "slotId",
    "lane",
    "parentCandidateId",
    "side",
    "site",
    "construction",
    "constructionSha256",
    "planSha256",
    "operatorSpecSha256",
    "catalogSha256",
    "parentGenomeSha256",
    "parentProgramSha256",
    "childGenomeSha256",
    "childProgramSha256",
    "childProfileSha256",
    "applicationSha256",
    "v38OperatorPlanSha256",
    "eligibility",
    "source",
    "cloneControl",
)
CLONE_SLOT_KEYS = (
    "slotId",
    "parentCandidateId",
    "side",
    "eligibility",
    "parentGenomeSha256",
    "parentProgramSha256",
    "source",
)
EMPTY_KEYS = ("parentCandidateId", "side", "lane", "site", "reason")
BUDGET_KEYS = (
    "eligibleSlotCount",
    "ineligibleOrEmptyCellCount",
    "explicitCloneCount",
    "windowCount",
    "panelCount",
    "projectedWorkerTasksIfLaunched",
    "doNotLaunch",
    "note",
)
SIDES = ("long", "short")
PROPOSAL_ROOT_KEYS = (
    "schemaVersion",
    "mode",
    "doNotLaunch",
    "childrenPerEligibleCell",
    "balanceRule",
    "emptyUnderfilledCellsRemainExplicit",
    "noReplacementFromAnotherLane",
    "sideStratifiedOutcomes",
    "parents",
    "lanes",
    "cells",
    "contractSha256",
)
CELL_KEYS = (
    "parentCandidateId",
    "side",
    "lane",
    "eligiblePlanCount",
    "sampledCount",
    "status",
    "reason",
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


def _optional_sha(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _require_sha(value, label)


def validate_resource_suboperation_candidate_inventory(contract: Mapping[str, Any]) -> dict[str, Any]:
    if contract.get("schemaVersion") in {
        "temporal_qd_resource_suboperation_launch_manifest_v1",
        "temporal_qd_resource_suboperation_matrix_v1",
    }:
        raise TemporalDiscoveryContractError("resource inventory cannot masquerade as a balanced launch matrix")
    row = _exact_object(contract, ROOT_KEYS, "resource suboperation candidate inventory")
    if row["schemaVersion"] != INVENTORY_SCHEMA:
        raise TemporalDiscoveryContractError("resource inventory schema is incompatible")
    if row["mode"] != INVENTORY_MODE:
        raise TemporalDiscoveryContractError("resource inventory mode drifted")
    if row["includeCrossover"] is not False:
        raise TemporalDiscoveryContractError("resource inventory must keep crossover out")
    if row["cloneControl"] != CLONE_CONTROL:
        raise TemporalDiscoveryContractError("resource inventory clone control drifted")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("resource inventory must not write the production archive")
    if row["notAdmittedOnFrontGenerationPath"] is not True:
        raise TemporalDiscoveryContractError("resource inventory must stay off the front generation path")
    if row["isBalancedLaunchMatrix"] is not False:
        raise TemporalDiscoveryContractError("candidate inventory is not a balanced launch matrix")
    if tuple(row["lanes"] or ()) != LANES:
        raise TemporalDiscoveryContractError("resource inventory lanes drifted")
    parents_value = row["parents"]
    if not isinstance(parents_value, list) or not parents_value:
        raise TemporalDiscoveryContractError("resource inventory requires frozen parents")
    parents: list[dict[str, str]] = []
    seen_parents: set[str] = set()
    has_archive = False
    for item in parents_value:
        parent = _exact_object(item, PARENT_KEYS, "resource inventory parent")
        if parent["candidateId"] in seen_parents or parent["role"] not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("resource inventory parent drifted")
        seen_parents.add(str(parent["candidateId"]))
        if parent["role"] == "archive":
            has_archive = True
        parents.append(
            {
                "candidateId": str(parent["candidateId"]),
                "role": str(parent["role"]),
                "longProgramSha256": _require_sha(parent["longProgramSha256"], "longProgramSha256"),
                "shortProgramSha256": _require_sha(parent["shortProgramSha256"], "shortProgramSha256"),
            }
        )
    if not has_archive:
        raise TemporalDiscoveryContractError("resource inventory requires at least one archive parent")
    parent_by_id = {item["candidateId"]: item for item in parents}
    panels = _exact_object(row["panelIdentities"], PANEL_KEYS, "resource inventory panelIdentities")
    if panels["developmentPanelId"] != "panel-3" or tuple(panels["replicationPanelIds"] or ()) != ("panel-1", "panel-2"):
        raise TemporalDiscoveryContractError("resource inventory panelIdentities drifted")
    if panels["replicationRole"] != "inspected_replication_not_untouched_confirmation":
        raise TemporalDiscoveryContractError("resource inventory replication role drifted")
    _require_sha(panels["rotatingEvidenceSha256"], "rotatingEvidenceSha256")
    source = _exact_object(row["sourceIdentities"], SOURCE_KEYS, "resource inventory sourceIdentities")
    _require_sha(source["workerContractSha256"], "workerContractSha256")
    if source["resourceOperatorId"] != "evolvable_resource_v1":
        raise TemporalDiscoveryContractError("resource inventory operator identity drifted")
    if source["enumerationAuthority"] != "GenomeResourceOperatorLayer.enumerate_plans":
        raise TemporalDiscoveryContractError("resource inventory enumeration authority drifted")
    if not isinstance(source["catalogSource"], str) or not source["catalogSource"]:
        raise _unexpected("catalogSource")
    slots_value = row["slots"]
    empty_value = row["emptyCells"]
    clone_value = row["cloneSlots"]
    if not isinstance(slots_value, list) or not isinstance(empty_value, list) or not isinstance(clone_value, list):
        raise _unexpected("resource inventory slots")
    slots: list[dict[str, Any]] = []
    seen_slots: set[str] = set()
    eligible = 0
    for item in slots_value:
        slot = _exact_object(item, SLOT_KEYS, "resource inventory slot")
        if slot["slotId"] in seen_slots or slot["lane"] not in LANES or slot["side"] not in SIDES:
            raise TemporalDiscoveryContractError("resource inventory slot drifted")
        seen_slots.add(str(slot["slotId"]))
        parent = parent_by_id.get(str(slot["parentCandidateId"]))
        if parent is None:
            raise TemporalDiscoveryContractError("resource inventory slot parent is not in the frozen parent set")
        expected_program = parent["longProgramSha256"] if slot["side"] == "long" else parent["shortProgramSha256"]
        if slot["parentProgramSha256"] != expected_program:
            raise TemporalDiscoveryContractError("resource inventory slot side program SHA drifted")
        construction = slot["construction"]
        if not isinstance(construction, Mapping):
            raise _unexpected("resource inventory construction")
        kind = construction.get("kind")
        if slot["eligibility"] == "eligible" and kind != slot["lane"] and slot["source"] != "explicit_parent_clone_control":
            raise TemporalDiscoveryContractError("resource inventory construction kind must match the declared lane")
        expected = canonical_sha256(dict(construction))
        if slot["constructionSha256"] != expected:
            raise TemporalDiscoveryContractError("resource inventory construction identity drift")
        if slot["eligibility"] not in {"eligible", "ineligible"}:
            raise TemporalDiscoveryContractError("resource inventory eligibility drifted")
        if slot["source"] not in ALLOWED_SOURCES:
            raise TemporalDiscoveryContractError("resource inventory source/provenance is not allowed")
        if slot["cloneControl"] is not False:
            raise TemporalDiscoveryContractError("mutation slots are not clone controls")
        if slot["eligibility"] == "eligible":
            if slot["source"] == "rejected_not_an_authoritative_applicable_plan":
                raise TemporalDiscoveryContractError("unproven constructions cannot be eligible")
            for key in (
                "planSha256",
                "operatorSpecSha256",
                "catalogSha256",
                "parentGenomeSha256",
                "childGenomeSha256",
                "childProgramSha256",
                "childProfileSha256",
                "applicationSha256",
            ):
                _require_sha(slot[key], key)
            eligible += 1
        else:
            if slot["source"] != "rejected_not_an_authoritative_applicable_plan":
                raise TemporalDiscoveryContractError("ineligible inventory slots must record rejected provenance")
        slots.append({key: slot[key] for key in SLOT_KEYS})
    clone_slots: list[dict[str, Any]] = []
    for item in clone_value:
        clone = _exact_object(item, CLONE_SLOT_KEYS, "resource inventory clone slot")
        if clone["slotId"] in seen_slots or clone["side"] not in SIDES:
            raise TemporalDiscoveryContractError("resource inventory clone slot drifted")
        seen_slots.add(str(clone["slotId"]))
        parent = parent_by_id.get(str(clone["parentCandidateId"]))
        if parent is None:
            raise TemporalDiscoveryContractError("clone slot parent is not in the frozen parent set")
        expected_program = parent["longProgramSha256"] if clone["side"] == "long" else parent["shortProgramSha256"]
        if clone["parentProgramSha256"] != expected_program:
            raise TemporalDiscoveryContractError("clone slot side program SHA drifted")
        if clone["source"] != "explicit_parent_clone_control" or clone["eligibility"] != "eligible":
            raise TemporalDiscoveryContractError("clone slots must be explicit eligible parent controls")
        _require_sha(clone["parentGenomeSha256"], "clone parentGenomeSha256")
        clone_slots.append({key: clone[key] for key in CLONE_SLOT_KEYS})
    empty_cells: list[dict[str, Any]] = []
    for item in empty_value:
        cell = _exact_object(item, EMPTY_KEYS, "resource inventory empty cell")
        if cell["lane"] not in LANES:
            raise TemporalDiscoveryContractError("resource inventory empty cell lane drifted")
        empty_cells.append({key: cell[key] for key in EMPTY_KEYS})
    budget = _exact_object(row["boundedTaskProjection"], BUDGET_KEYS, "boundedTaskProjection")
    if budget["eligibleSlotCount"] != eligible:
        raise TemporalDiscoveryContractError("resource inventory eligibleSlotCount drifted")
    if budget["ineligibleOrEmptyCellCount"] != len(empty_cells):
        raise TemporalDiscoveryContractError("resource inventory empty-cell count drifted")
    if budget["explicitCloneCount"] != len(clone_slots):
        raise TemporalDiscoveryContractError("resource inventory clone count drifted")
    if budget["windowCount"] != 3 or budget["panelCount"] != 3:
        raise TemporalDiscoveryContractError("resource inventory must project windows/panels, not only candidates")
    if budget["projectedWorkerTasksIfLaunched"] != eligible * 3 + len(clone_slots) * 3:
        raise TemporalDiscoveryContractError("resource inventory worker-task projection drifted")
    if budget["doNotLaunch"] is not True:
        raise TemporalDiscoveryContractError("resource inventory must not launch")
    body = {
        "schemaVersion": INVENTORY_SCHEMA,
        "mode": INVENTORY_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
        "isBalancedLaunchMatrix": False,
        "parents": parents,
        "lanes": list(LANES),
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "replicationPanelIds": ["panel-1", "panel-2"],
            "replicationRole": "inspected_replication_not_untouched_confirmation",
            "rotatingEvidenceSha256": panels["rotatingEvidenceSha256"],
        },
        "sourceIdentities": {
            "workerContractSha256": source["workerContractSha256"],
            "catalogSource": source["catalogSource"],
            "resourceOperatorId": "evolvable_resource_v1",
            "enumerationAuthority": "GenomeResourceOperatorLayer.enumerate_plans",
        },
        "slots": slots,
        "emptyCells": empty_cells,
        "cloneSlots": clone_slots,
        "boundedTaskProjection": {
            "eligibleSlotCount": eligible,
            "ineligibleOrEmptyCellCount": len(empty_cells),
            "explicitCloneCount": len(clone_slots),
            "windowCount": 3,
            "panelCount": 3,
            "projectedWorkerTasksIfLaunched": eligible * 3 + len(clone_slots) * 3,
            "doNotLaunch": True,
            "note": budget["note"],
        },
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("resource inventory identity drift")
    body["contractSha256"] = expected
    return body


def build_resource_suboperation_candidate_inventory(
    *,
    parents: Sequence[Mapping[str, Any]],
    rotating_evidence_sha256: str,
    worker_contract_sha256: str,
    catalog_source: str,
    slots: Sequence[Mapping[str, Any]],
    empty_cells: Sequence[Mapping[str, Any]],
    clone_slots: Sequence[Mapping[str, Any]],
    note: str,
) -> dict[str, Any]:
    eligible = sum(1 for slot in slots if slot.get("eligibility") == "eligible")
    body = {
        "schemaVersion": INVENTORY_SCHEMA,
        "mode": INVENTORY_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
        "isBalancedLaunchMatrix": False,
        "parents": [dict(item) for item in parents],
        "lanes": list(LANES),
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "replicationPanelIds": ["panel-1", "panel-2"],
            "replicationRole": "inspected_replication_not_untouched_confirmation",
            "rotatingEvidenceSha256": rotating_evidence_sha256,
        },
        "sourceIdentities": {
            "workerContractSha256": worker_contract_sha256,
            "catalogSource": catalog_source,
            "resourceOperatorId": "evolvable_resource_v1",
            "enumerationAuthority": "GenomeResourceOperatorLayer.enumerate_plans",
        },
        "slots": [dict(item) for item in slots],
        "emptyCells": [dict(item) for item in empty_cells],
        "cloneSlots": [dict(item) for item in clone_slots],
        "boundedTaskProjection": {
            "eligibleSlotCount": eligible,
            "ineligibleOrEmptyCellCount": len(empty_cells),
            "explicitCloneCount": len(clone_slots),
            "windowCount": 3,
            "panelCount": 3,
            "projectedWorkerTasksIfLaunched": eligible * 3 + len(clone_slots) * 3,
            "doNotLaunch": True,
            "note": note,
        },
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_candidate_inventory(body)


def validate_resource_suboperation_balanced_design_proposal(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, PROPOSAL_ROOT_KEYS, "resource balanced design proposal")
    if row["schemaVersion"] != BALANCED_PROPOSAL_SCHEMA:
        raise TemporalDiscoveryContractError("balanced design proposal schema drifted")
    if row["mode"] != BALANCED_PROPOSAL_MODE:
        raise TemporalDiscoveryContractError("balanced design proposal mode drifted")
    if row["doNotLaunch"] is not True:
        raise TemporalDiscoveryContractError("balanced design proposal must not launch")
    if not isinstance(row["childrenPerEligibleCell"], int) or row["childrenPerEligibleCell"] < 1:
        raise TemporalDiscoveryContractError("childrenPerEligibleCell drifted")
    if row["balanceRule"] != "equal_predeclared_count_per_eligible_parent_side_suboperation_cell":
        raise TemporalDiscoveryContractError("balanceRule drifted")
    if row["emptyUnderfilledCellsRemainExplicit"] is not True:
        raise TemporalDiscoveryContractError("underfilled cells must remain explicit")
    if row["noReplacementFromAnotherLane"] is not True:
        raise TemporalDiscoveryContractError("lane replacement is forbidden")
    if row["sideStratifiedOutcomes"] is not True:
        raise TemporalDiscoveryContractError("side-stratified outcomes are required")
    if tuple(row["lanes"] or ()) != LANES:
        raise TemporalDiscoveryContractError("balanced design lanes drifted")
    cells_value = row["cells"]
    if not isinstance(cells_value, list):
        raise _unexpected("balanced design cells")
    cells: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in cells_value:
        cell = _exact_object(item, CELL_KEYS, "balanced design cell")
        key = (str(cell["parentCandidateId"]), str(cell["side"]), str(cell["lane"]))
        if key in seen or cell["side"] not in SIDES or cell["lane"] not in LANES:
            raise TemporalDiscoveryContractError("balanced design cell drifted")
        seen.add(key)
        if cell["status"] not in {"filled", "underfilled", "empty"}:
            raise TemporalDiscoveryContractError("balanced design cell status drifted")
        if cell["status"] == "filled" and int(cell["sampledCount"]) != int(row["childrenPerEligibleCell"]):
            raise TemporalDiscoveryContractError("filled cells must match the predeclared count")
        if cell["status"] != "filled" and cell["sampledCount"] != 0:
            raise TemporalDiscoveryContractError("empty/underfilled cells must not be backfilled from another lane")
        cells.append({key_name: cell[key_name] for key_name in CELL_KEYS})
    body = {
        "schemaVersion": BALANCED_PROPOSAL_SCHEMA,
        "mode": BALANCED_PROPOSAL_MODE,
        "doNotLaunch": True,
        "childrenPerEligibleCell": row["childrenPerEligibleCell"],
        "balanceRule": "equal_predeclared_count_per_eligible_parent_side_suboperation_cell",
        "emptyUnderfilledCellsRemainExplicit": True,
        "noReplacementFromAnotherLane": True,
        "sideStratifiedOutcomes": True,
        "parents": list(row["parents"]),
        "lanes": list(LANES),
        "cells": cells,
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("balanced design proposal identity drift")
    body["contractSha256"] = expected
    return body


def build_resource_suboperation_balanced_design_proposal(
    *,
    parents: Sequence[Mapping[str, Any]],
    inventory_slots: Sequence[Mapping[str, Any]],
    children_per_eligible_cell: int = 1,
) -> dict[str, Any]:
    counts: dict[tuple[str, str, str], int] = Counter()
    for slot in inventory_slots:
        if slot.get("eligibility") != "eligible":
            continue
        counts[(str(slot["parentCandidateId"]), str(slot["side"]), str(slot["lane"]))] += 1
    cells: list[dict[str, Any]] = []
    for parent in parents:
        for side in SIDES:
            for lane in LANES:
                eligible = counts.get((str(parent["candidateId"]), side, lane), 0)
                if eligible >= children_per_eligible_cell:
                    status, sampled, reason = "filled", children_per_eligible_cell, None
                elif eligible:
                    status, sampled, reason = "underfilled", 0, "cell_has_eligible_plans_but_below_predeclared_count"
                else:
                    status, sampled, reason = "empty", 0, "no_eligible_authoritative_plan"
                cells.append(
                    {
                        "parentCandidateId": parent["candidateId"],
                        "side": side,
                        "lane": lane,
                        "eligiblePlanCount": eligible,
                        "sampledCount": sampled,
                        "status": status,
                        "reason": reason,
                    }
                )
    body = {
        "schemaVersion": BALANCED_PROPOSAL_SCHEMA,
        "mode": BALANCED_PROPOSAL_MODE,
        "doNotLaunch": True,
        "childrenPerEligibleCell": children_per_eligible_cell,
        "balanceRule": "equal_predeclared_count_per_eligible_parent_side_suboperation_cell",
        "emptyUnderfilledCellsRemainExplicit": True,
        "noReplacementFromAnotherLane": True,
        "sideStratifiedOutcomes": True,
        "parents": [dict(item) for item in parents],
        "lanes": list(LANES),
        "cells": cells,
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_balanced_design_proposal(body)
