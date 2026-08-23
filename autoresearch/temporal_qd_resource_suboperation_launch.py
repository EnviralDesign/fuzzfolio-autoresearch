"""Launch-grade frozen resource-suboperation slot manifest.

Parse-only. Do not schedule a generation or market evaluation. The abstract
balanced-suboperation contract remains the planning layer.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_resource_suboperation_matrix import LANES, PARENT_ROLES

LAUNCH_SCHEMA = "temporal_qd_resource_suboperation_launch_manifest_v1"
LAUNCH_MODE = "frozen_parent_site_plan_slots_v1"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
ROOT_KEYS = (
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "notAdmittedOnFrontGenerationPath",
    "parents",
    "lanes",
    "panelIdentities",
    "sourceIdentities",
    "slots",
    "emptyCells",
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
SOURCE_KEYS = ("workerContractSha256", "catalogSource", "resourceOperatorId")
SLOT_KEYS = (
    "slotId",
    "lane",
    "parentCandidateId",
    "side",
    "site",
    "construction",
    "constructionSha256",
    "v38OperatorPlanSha256",
    "eligibility",
    "source",
)
EMPTY_KEYS = ("parentCandidateId", "side", "lane", "site", "reason")
BUDGET_KEYS = ("eligibleSlotCount", "ineligibleOrEmptyCellCount", "doNotLaunch", "note")
SIDES = ("long", "short")


def _unexpected(label: str) -> TemporalDiscoveryContractError:
    return TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _exact_object(value: Any, required: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(required):
        raise _unexpected(label)
    return dict(value)


def _require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    if not all(ch in "0123456789abcdef" for ch in value[7:]):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return value


def validate_resource_suboperation_launch_manifest(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, ROOT_KEYS, "resource suboperation launch manifest")
    if row["schemaVersion"] != LAUNCH_SCHEMA:
        raise TemporalDiscoveryContractError("resource launch manifest schema is incompatible")
    if row["mode"] != LAUNCH_MODE:
        raise TemporalDiscoveryContractError("resource launch manifest mode drifted")
    if row["includeCrossover"] is not False:
        raise TemporalDiscoveryContractError("resource launch manifest must keep crossover out")
    if row["cloneControl"] != CLONE_CONTROL:
        raise TemporalDiscoveryContractError("resource launch manifest clone control drifted")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("resource launch manifest must not write the production archive")
    if row["notAdmittedOnFrontGenerationPath"] is not True:
        raise TemporalDiscoveryContractError("resource launch manifest must stay off the front generation path")
    if tuple(row["lanes"] or ()) != LANES:
        raise TemporalDiscoveryContractError("resource launch manifest lanes drifted")
    parents_value = row["parents"]
    if not isinstance(parents_value, list) or not parents_value:
        raise TemporalDiscoveryContractError("resource launch manifest requires frozen parents")
    parents: list[dict[str, str]] = []
    seen: set[str] = set()
    has_archive = False
    for item in parents_value:
        parent = _exact_object(item, PARENT_KEYS, "resource launch parent")
        if parent["candidateId"] in seen or parent["role"] not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("resource launch parent drifted")
        seen.add(str(parent["candidateId"]))
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
        raise TemporalDiscoveryContractError("resource launch manifest requires at least one archive parent")
    panels = _exact_object(row["panelIdentities"], PANEL_KEYS, "resource launch panelIdentities")
    if panels["developmentPanelId"] != "panel-3" or tuple(panels["replicationPanelIds"] or ()) != ("panel-1", "panel-2"):
        raise TemporalDiscoveryContractError("resource launch panelIdentities drifted")
    if panels["replicationRole"] != "inspected_replication_not_untouched_confirmation":
        raise TemporalDiscoveryContractError("resource launch replication role drifted")
    _require_sha(panels["rotatingEvidenceSha256"], "rotatingEvidenceSha256")
    source = _exact_object(row["sourceIdentities"], SOURCE_KEYS, "resource launch sourceIdentities")
    _require_sha(source["workerContractSha256"], "workerContractSha256")
    if source["resourceOperatorId"] != "evolvable_resource_v1":
        raise TemporalDiscoveryContractError("resource launch operator identity drifted")
    if not isinstance(source["catalogSource"], str) or not source["catalogSource"]:
        raise _unexpected("catalogSource")
    slots_value = row["slots"]
    empty_value = row["emptyCells"]
    if not isinstance(slots_value, list) or not isinstance(empty_value, list):
        raise _unexpected("resource launch slots")
    slots: list[dict[str, Any]] = []
    seen_slots: set[str] = set()
    eligible = 0
    for item in slots_value:
        slot = _exact_object(item, SLOT_KEYS, "resource launch slot")
        if slot["slotId"] in seen_slots or slot["lane"] not in LANES or slot["side"] not in SIDES:
            raise TemporalDiscoveryContractError("resource launch slot drifted")
        seen_slots.add(str(slot["slotId"]))
        construction = slot["construction"]
        if not isinstance(construction, Mapping):
            raise _unexpected("resource launch construction")
        expected = canonical_sha256(dict(construction))
        if slot["constructionSha256"] != expected:
            raise TemporalDiscoveryContractError("resource launch construction identity drift")
        if slot["eligibility"] not in {"eligible", "ineligible"}:
            raise TemporalDiscoveryContractError("resource launch eligibility drifted")
        if slot["eligibility"] == "eligible":
            eligible += 1
        if slot["v38OperatorPlanSha256"] is not None:
            _require_sha(slot["v38OperatorPlanSha256"], "v38OperatorPlanSha256")
        slots.append(
            {
                "slotId": slot["slotId"],
                "lane": slot["lane"],
                "parentCandidateId": slot["parentCandidateId"],
                "side": slot["side"],
                "site": slot["site"],
                "construction": dict(construction),
                "constructionSha256": expected,
                "v38OperatorPlanSha256": slot["v38OperatorPlanSha256"],
                "eligibility": slot["eligibility"],
                "source": slot["source"],
            }
        )
    empty_cells: list[dict[str, Any]] = []
    for item in empty_value:
        cell = _exact_object(item, EMPTY_KEYS, "resource launch empty cell")
        if cell["lane"] not in LANES:
            raise TemporalDiscoveryContractError("resource launch empty cell lane drifted")
        empty_cells.append(
            {
                "parentCandidateId": cell["parentCandidateId"],
                "side": cell["side"],
                "lane": cell["lane"],
                "site": cell["site"],
                "reason": cell["reason"],
            }
        )
    budget = _exact_object(row["boundedTaskProjection"], BUDGET_KEYS, "boundedTaskProjection")
    if budget["eligibleSlotCount"] != eligible:
        raise TemporalDiscoveryContractError("resource launch eligibleSlotCount drifted")
    if budget["ineligibleOrEmptyCellCount"] != len(empty_cells):
        raise TemporalDiscoveryContractError("resource launch empty-cell count drifted")
    if budget["doNotLaunch"] is not True:
        raise TemporalDiscoveryContractError("resource launch manifest must not launch")
    body = {
        "schemaVersion": LAUNCH_SCHEMA,
        "mode": LAUNCH_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
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
        },
        "slots": slots,
        "emptyCells": empty_cells,
        "boundedTaskProjection": {
            "eligibleSlotCount": eligible,
            "ineligibleOrEmptyCellCount": len(empty_cells),
            "doNotLaunch": True,
            "note": budget["note"],
        },
    }
    expected_hash = canonical_sha256(body)
    if row["contractSha256"] != expected_hash:
        raise TemporalDiscoveryContractError("resource launch manifest identity drift")
    body["contractSha256"] = expected_hash
    return body


def build_resource_suboperation_launch_manifest(
    *,
    parents: Sequence[Mapping[str, Any]],
    rotating_evidence_sha256: str,
    worker_contract_sha256: str,
    catalog_source: str,
    slots: Sequence[Mapping[str, Any]],
    empty_cells: Sequence[Mapping[str, Any]],
    note: str,
) -> dict[str, Any]:
    eligible = sum(1 for slot in slots if slot.get("eligibility") == "eligible")
    body = {
        "schemaVersion": LAUNCH_SCHEMA,
        "mode": LAUNCH_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "notAdmittedOnFrontGenerationPath": True,
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
        },
        "slots": [dict(item) for item in slots],
        "emptyCells": [dict(item) for item in empty_cells],
        "boundedTaskProjection": {
            "eligibleSlotCount": eligible,
            "ineligibleOrEmptyCellCount": len(empty_cells),
            "doNotLaunch": True,
            "note": note,
        },
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_launch_manifest(body)
