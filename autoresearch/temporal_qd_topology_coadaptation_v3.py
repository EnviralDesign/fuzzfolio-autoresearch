"""Experiment-only topology co-adaptation matrix v3.

v2 parser parity was necessary but not sufficient. This contract freezes
parent-bound insert_setup TopologyPlanV1 records and requires event settling
to target the newly added setup node. insert_exit_region is forbidden in the
first contrast. This overlay never launches a market evaluation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA

COADAPTATION_SCHEMA = "temporal_qd_topology_coadaptation_matrix_v3"
COADAPTATION_MODE = "frozen_parent_insert_setup_then_topology_local_event_v3"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
ARMS = (
    "exact_parent_clone",
    "topology_only_child",
    "event_only_control",
    "topology_then_topology_local_event",
)
FIRST_EXPERIMENT_OPERATION = "insert_setup"
FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS = ("insert_exit_region",)
PARENT_ROLES = ("archive", "inactive_control", "active_negative_control")
TOPOLOGY_PLAN_KEYS = (
    "schemaVersion",
    "operatorSchema",
    "operation",
    "sourceGenomeSha256",
    "arguments",
)
ROOT_KEYS = (
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "mutationDepth",
    "morphologyNurseryDeferred",
    "lexicographicFirstSettlingPlanForbidden",
    "parents",
    "panelIdentities",
    "firstExperimentOperation",
    "forbiddenFirstExperimentOperations",
    "topologyLocalEventRequired",
    "arms",
    "topologyPlans",
    "eventPrimitives",
    "slots",
    "settling",
    "successCalculation",
    "notAdmittedOnFrontGenerationPath",
    "contractSha256",
)
PARENT_KEYS = ("candidateId", "role", "longProgramSha256", "shortProgramSha256")
PANEL_KEYS = (
    "developmentPanelId",
    "developmentRole",
    "replicationPanelIds",
    "replicationRole",
    "futureConfirmationPanel",
    "rotatingEvidenceSha256",
)
FUTURE_PANEL_KEYS = (
    "createdInThisTask",
    "requiredBeforeProductionConclusion",
    "authorityMustBeBoundBeforeLaunch",
    "label",
)
TOPOLOGY_RECORD_KEYS = (
    "planId",
    "parentCandidateId",
    "side",
    "topologyPlan",
    "planSha256",
    "addedSetupNodeId",
    "applicability",
    "topologySemanticDeltaIdentity",
)
EVENT_PRIMITIVE_KEYS = (
    "primitiveId",
    "parentCandidateId",
    "side",
    "indicatorId",
    "contract",
    "originalNodeId",
    "originalNodeZone",
    "source",
)
SLOT_KEYS = (
    "slotId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "topologyPlanId",
    "eventPrimitiveId",
    "settlingNodeId",
    "ineligibilityReason",
)
SETTLING_KEYS = (
    "kind",
    "mustTargetAddedSetupNodeId",
    "selection",
    "matchedControlSite",
    "ineligibleCellsRemainExplicit",
)
SUCCESS_KEYS = (
    "schemaVersion",
    "metricEquality",
    "parentBeat",
    "riskQualifiedBeat",
    "fullEconomicPhenotypeTie",
    "supportDirectionQualityGates",
    "activityCostMechanismRequired",
    "parentBalancingRequired",
    "eventPlanBalancingRequired",
    "requireReplicationPanelSurvivalForPromisingClaim",
    "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    "doNotPromoteOnDevelopmentPanelAlone",
    "noveltyIsNotQuality",
)
SUCCESS_VALUES = {
    "schemaVersion": "temporal_qd_topology_coadaptation_success_v3",
    "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
    "parentBeat": "child_net_strictly_greater_under_canonical_metric_identity",
    "riskQualifiedBeat": "parentBeat_and_non_worse_worst_window",
    "fullEconomicPhenotypeTie": "equal_net_worst_median_active_window_fraction",
    "supportDirectionQualityGates": "unchanged_production_gates",
    "activityCostMechanismRequired": True,
    "parentBalancingRequired": True,
    "eventPlanBalancingRequired": True,
    "requireReplicationPanelSurvivalForPromisingClaim": True,
    "requireUntouchedConfirmationPanelBeforeProductionConclusion": True,
    "doNotPromoteOnDevelopmentPanelAlone": True,
    "noveltyIsNotQuality": True,
}
ELIGIBILITIES = ("eligible", "ineligible")
SIDES = ("long", "short")


def _unexpected(label: str) -> TemporalDiscoveryContractError:
    return TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _exact_object(value: Any, required: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(required):
        raise _unexpected(label)
    return dict(value)


def _require_bool(value: Any, expected: bool, label: str) -> None:
    if value is not expected:
        raise TemporalDiscoveryContractError(f"{label} drifted")


def _require_text(value: Any, expected: str, label: str) -> str:
    if value != expected:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return expected


def _require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    if not all(ch in "0123456789abcdef" for ch in value[7:]):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return value


def canonical_topology_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(plan, Mapping) and "v38ExampleOperatorPlanSha256" in plan:
        raise TemporalDiscoveryContractError("generic example topology plan SHAs cannot satisfy launch-grade slots")
    row = _exact_object(plan, TOPOLOGY_PLAN_KEYS, "topology plan")
    if row["schemaVersion"] != "evolvable_module_topology_plan_v1":
        raise TemporalDiscoveryContractError("topology plan schema drifted")
    if row["operatorSchema"] != "evolvable_module_topology_operator_v1":
        raise TemporalDiscoveryContractError("topology plan operator schema drifted")
    if row["operation"] == "insert_exit_region":
        raise TemporalDiscoveryContractError("insert_exit_region cannot enter the event-settling first contrast")
    if row["operation"] != FIRST_EXPERIMENT_OPERATION:
        raise TemporalDiscoveryContractError("topology plan operation drifted")
    _require_sha(row["sourceGenomeSha256"], "topology plan sourceGenomeSha256")
    if not isinstance(row["arguments"], Mapping):
        raise _unexpected("topology plan arguments")
    return {
        "schemaVersion": "evolvable_module_topology_plan_v1",
        "operatorSchema": "evolvable_module_topology_operator_v1",
        "operation": FIRST_EXPERIMENT_OPERATION,
        "sourceGenomeSha256": row["sourceGenomeSha256"],
        "arguments": dict(row["arguments"]),
    }


def topology_plan_sha256(plan: Mapping[str, Any]) -> str:
    return canonical_sha256(canonical_topology_plan(plan))


def added_setup_node_id(plan: Mapping[str, Any]) -> str:
    digest = topology_plan_sha256(plan).split(":", 1)[1]
    return f"setup_{digest[:16]}"


def topology_semantic_delta_identity(plan: Mapping[str, Any]) -> str:
    canonical = canonical_topology_plan(plan)
    sha = topology_plan_sha256(canonical)
    added = added_setup_node_id(canonical)
    return canonical_sha256(
        {
            "operation": FIRST_EXPERIMENT_OPERATION,
            "planSha256": sha,
            "sourceGenomeSha256": canonical["sourceGenomeSha256"],
            "addedSetupNodeId": added,
        }
    )


def _validate_parents(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation v3 requires frozen parents")
    parents: list[dict[str, str]] = []
    seen: set[str] = set()
    has_archive = False
    for item in value:
        row = _exact_object(item, PARENT_KEYS, "topology coadaptation v3 parent")
        candidate_id = row["candidateId"]
        role = row["role"]
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise _unexpected("topology coadaptation v3 parent")
        if role not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("topology coadaptation v3 parent role is invalid")
        if candidate_id in seen:
            raise TemporalDiscoveryContractError(f"topology coadaptation v3 repeats parent {candidate_id}")
        seen.add(candidate_id)
        if role == "archive":
            has_archive = True
        parents.append(
            {
                "candidateId": candidate_id,
                "role": role,
                "longProgramSha256": _require_sha(row["longProgramSha256"], "parent longProgramSha256"),
                "shortProgramSha256": _require_sha(row["shortProgramSha256"], "parent shortProgramSha256"),
            }
        )
    if not has_archive:
        raise TemporalDiscoveryContractError("topology coadaptation v3 requires at least one archive parent")
    return parents


def _validate_panels(value: Any) -> dict[str, Any]:
    row = _exact_object(value, PANEL_KEYS, "topology coadaptation v3 panelIdentities")
    _require_text(row["developmentPanelId"], "panel-3", "developmentPanelId")
    _require_text(row["developmentRole"], "discovery_and_selection", "developmentRole")
    if tuple(row["replicationPanelIds"] or ()) != ("panel-1", "panel-2"):
        raise TemporalDiscoveryContractError("topology coadaptation v3 replication panels drifted")
    _require_text(
        row["replicationRole"],
        "inspected_replication_not_untouched_confirmation",
        "replicationRole",
    )
    future = _exact_object(row["futureConfirmationPanel"], FUTURE_PANEL_KEYS, "futureConfirmationPanel")
    _require_bool(future["createdInThisTask"], False, "futureConfirmationPanel.createdInThisTask")
    _require_bool(
        future["requiredBeforeProductionConclusion"],
        True,
        "futureConfirmationPanel.requiredBeforeProductionConclusion",
    )
    _require_bool(
        future["authorityMustBeBoundBeforeLaunch"],
        True,
        "futureConfirmationPanel.authorityMustBeBoundBeforeLaunch",
    )
    _require_text(
        future["label"],
        "future_untouched_confirmation_panel",
        "futureConfirmationPanel.label",
    )
    return {
        "developmentPanelId": "panel-3",
        "developmentRole": "discovery_and_selection",
        "replicationPanelIds": ["panel-1", "panel-2"],
        "replicationRole": "inspected_replication_not_untouched_confirmation",
        "futureConfirmationPanel": {
            "createdInThisTask": False,
            "requiredBeforeProductionConclusion": True,
            "authorityMustBeBoundBeforeLaunch": True,
            "label": "future_untouched_confirmation_panel",
        },
        "rotatingEvidenceSha256": _require_sha(row["rotatingEvidenceSha256"], "rotatingEvidenceSha256"),
    }


def validate_topology_coadaptation_matrix_v3(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, ROOT_KEYS, "topology coadaptation v3")
    _require_text(row["schemaVersion"], COADAPTATION_SCHEMA, "schema")
    _require_text(row["mode"], COADAPTATION_MODE, "mode")
    _require_bool(row["includeCrossover"], False, "includeCrossover")
    _require_text(row["cloneControl"], CLONE_CONTROL, "cloneControl")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("topology coadaptation must not write the production archive")
    if row["mutationDepth"] != 1:
        raise TemporalDiscoveryContractError("topology coadaptation v3 mutationDepth drifted")
    _require_bool(row["morphologyNurseryDeferred"], True, "morphologyNurseryDeferred")
    _require_bool(row["lexicographicFirstSettlingPlanForbidden"], True, "lexicographicFirstSettlingPlanForbidden")
    _require_text(row["firstExperimentOperation"], FIRST_EXPERIMENT_OPERATION, "firstExperimentOperation")
    if tuple(row["forbiddenFirstExperimentOperations"] or ()) != FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS:
        raise TemporalDiscoveryContractError("forbiddenFirstExperimentOperations drifted")
    _require_bool(row["topologyLocalEventRequired"], True, "topologyLocalEventRequired")
    if tuple(row["arms"] or ()) != ARMS:
        raise TemporalDiscoveryContractError("topology coadaptation v3 arms drifted")
    _require_bool(row["notAdmittedOnFrontGenerationPath"], True, "notAdmittedOnFrontGenerationPath")
    parents = _validate_parents(row["parents"])
    parent_by_id = {item["candidateId"]: item for item in parents}
    panel_identities = _validate_panels(row["panelIdentities"])
    topology_plans = _validate_topology_records(row["topologyPlans"], parent_by_id)
    event_primitives = _validate_event_primitives(row["eventPrimitives"], parent_by_id)
    slots = _validate_slots(
        row["slots"],
        parent_by_id=parent_by_id,
        topology_plans=topology_plans,
        event_primitives=event_primitives,
    )
    settling = _validate_settling(row["settling"])
    success = _exact_object(row["successCalculation"], SUCCESS_KEYS, "successCalculation")
    if success != SUCCESS_VALUES:
        raise TemporalDiscoveryContractError("topology coadaptation v3 successCalculation drifted")
    body = {
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": COADAPTATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": True,
        "lexicographicFirstSettlingPlanForbidden": True,
        "parents": parents,
        "panelIdentities": panel_identities,
        "firstExperimentOperation": FIRST_EXPERIMENT_OPERATION,
        "forbiddenFirstExperimentOperations": list(FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS),
        "topologyLocalEventRequired": True,
        "arms": list(ARMS),
        "topologyPlans": topology_plans,
        "eventPrimitives": event_primitives,
        "slots": slots,
        "settling": settling,
        "successCalculation": dict(SUCCESS_VALUES),
        "notAdmittedOnFrontGenerationPath": True,
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("topology coadaptation v3 identity drift")
    body["contractSha256"] = expected
    return body


def _validate_topology_records(value: Any, parent_by_id: Mapping[str, Mapping[str, str]]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise _unexpected("topologyPlans")
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_shas: set[str] = set()
    for item in value:
        row = _exact_object(item, TOPOLOGY_RECORD_KEYS, "topology plan record")
        plan_id = row["planId"]
        if not isinstance(plan_id, str) or not plan_id or plan_id in seen_ids:
            raise _unexpected("topology plan record")
        seen_ids.add(plan_id)
        parent_id = row["parentCandidateId"]
        side = row["side"]
        if parent_id not in parent_by_id or side not in SIDES:
            raise TemporalDiscoveryContractError("topology plan record parent/side drifted")
        plan = canonical_topology_plan(row["topologyPlan"])
        expected_sha = topology_plan_sha256(plan)
        if row["planSha256"] != expected_sha:
            raise TemporalDiscoveryContractError("topology plan identity drift")
        if expected_sha in seen_shas:
            raise TemporalDiscoveryContractError("topology plan SHA reused across records")
        seen_shas.add(expected_sha)
        added = added_setup_node_id(plan)
        if row["addedSetupNodeId"] != added:
            raise TemporalDiscoveryContractError("added setup node identity drift")
        expected_source = parent_by_id[parent_id]["longProgramSha256" if side == "long" else "shortProgramSha256"]
        if plan["sourceGenomeSha256"] != expected_source:
            raise TemporalDiscoveryContractError("stale topology plan does not bind this exact parent genome")
        if row["applicability"] != "source_genome_matches_parent_side_program":
            raise TemporalDiscoveryContractError("topology plan applicability drifted")
        delta_identity = canonical_sha256(
            {
                "operation": FIRST_EXPERIMENT_OPERATION,
                "planSha256": expected_sha,
                "sourceGenomeSha256": plan["sourceGenomeSha256"],
                "addedSetupNodeId": added,
            }
        )
        if row["topologySemanticDeltaIdentity"] != delta_identity:
            raise TemporalDiscoveryContractError("topology semantic delta identity drift")
        records.append(
            {
                "planId": plan_id,
                "parentCandidateId": parent_id,
                "side": side,
                "topologyPlan": plan,
                "planSha256": expected_sha,
                "addedSetupNodeId": added,
                "applicability": "source_genome_matches_parent_side_program",
                "topologySemanticDeltaIdentity": delta_identity,
            }
        )
    return records


def _validate_event_primitives(value: Any, parent_by_id: Mapping[str, Mapping[str, str]]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise _unexpected("eventPrimitives")
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        row = _exact_object(item, EVENT_PRIMITIVE_KEYS, "event primitive")
        primitive_id = row["primitiveId"]
        if not isinstance(primitive_id, str) or not primitive_id or primitive_id in seen:
            raise _unexpected("event primitive")
        seen.add(primitive_id)
        if row["parentCandidateId"] not in parent_by_id or row["side"] not in SIDES:
            raise TemporalDiscoveryContractError("event primitive parent/side drifted")
        if not isinstance(row["indicatorId"], str) or not row["indicatorId"]:
            raise _unexpected("event primitive")
        if not isinstance(row["contract"], Mapping):
            raise _unexpected("event primitive contract")
        if not isinstance(row["originalNodeId"], str) or not row["originalNodeId"]:
            raise _unexpected("event primitive")
        if row["originalNodeZone"] not in {"setup", "entry"}:
            raise TemporalDiscoveryContractError("event primitive originalNodeZone drifted")
        if row["source"] != "v38_recovered_directional_event_insert":
            raise TemporalDiscoveryContractError("event primitive source drifted")
        records.append(
            {
                "primitiveId": primitive_id,
                "parentCandidateId": row["parentCandidateId"],
                "side": row["side"],
                "indicatorId": row["indicatorId"],
                "contract": dict(row["contract"]),
                "originalNodeId": row["originalNodeId"],
                "originalNodeZone": row["originalNodeZone"],
                "source": "v38_recovered_directional_event_insert",
            }
        )
    return records


def _validate_slots(
    value: Any,
    *,
    parent_by_id: Mapping[str, Mapping[str, str]],
    topology_plans: Sequence[Mapping[str, Any]],
    event_primitives: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation v3 requires slots")
    topology_by_id = {item["planId"]: item for item in topology_plans}
    event_by_id = {item["primitiveId"]: item for item in event_primitives}
    slots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        row = _exact_object(item, SLOT_KEYS, "topology coadaptation v3 slot")
        slot_id = row["slotId"]
        if not isinstance(slot_id, str) or not slot_id or slot_id in seen:
            raise _unexpected("topology coadaptation v3 slot")
        seen.add(slot_id)
        arm = row["arm"]
        if arm not in ARMS:
            raise TemporalDiscoveryContractError("topology coadaptation v3 slot arm drifted")
        parent_id = row["parentCandidateId"]
        if parent_id not in parent_by_id:
            raise TemporalDiscoveryContractError("topology coadaptation v3 slot parent drifted")
        side = row["side"]
        if side not in (*SIDES, None) and side is not None:
            raise TemporalDiscoveryContractError("topology coadaptation v3 slot side drifted")
        if arm == "exact_parent_clone":
            if row["side"] is not None or row["topologyPlanId"] is not None or row["eventPrimitiveId"] is not None:
                raise TemporalDiscoveryContractError("clone slot must not carry topology or event plans")
            if row["settlingNodeId"] is not None:
                raise TemporalDiscoveryContractError("clone slot must not settle")
        elif arm == "topology_only_child":
            if row["eventPrimitiveId"] is not None or row["settlingNodeId"] is not None:
                raise TemporalDiscoveryContractError("topology-only slot must not include an event")
            if row["eligibility"] == "ineligible" and row["topologyPlanId"] is None:
                pass
            else:
                plan = topology_by_id.get(row["topologyPlanId"])
                if plan is None or plan["parentCandidateId"] != parent_id or plan["side"] != side:
                    raise TemporalDiscoveryContractError("topology-only slot plan parent binding drifted")
        elif arm == "event_only_control":
            if row["topologyPlanId"] is not None:
                raise TemporalDiscoveryContractError("event-only control must not include topology")
            if row["eligibility"] == "ineligible" and row["eventPrimitiveId"] is None:
                pass
            else:
                primitive = event_by_id.get(row["eventPrimitiveId"])
                if primitive is None or primitive["parentCandidateId"] != parent_id or primitive["side"] != side:
                    raise TemporalDiscoveryContractError("event-only control primitive binding drifted")
                if row["eligibility"] == "eligible" and not isinstance(row["settlingNodeId"], str):
                    raise TemporalDiscoveryContractError("event-only control must name the parent setup node")
        elif arm == "topology_then_topology_local_event":
            plan = topology_by_id.get(row["topologyPlanId"])
            primitive = event_by_id.get(row["eventPrimitiveId"])
            if row["eligibility"] == "ineligible" and (plan is None or primitive is None):
                pass
            else:
                if plan is None or primitive is None:
                    raise TemporalDiscoveryContractError("topology+event slot requires both plans")
                if plan["parentCandidateId"] != parent_id or plan["side"] != side:
                    raise TemporalDiscoveryContractError("topology+event topology plan parent drifted")
                if primitive["parentCandidateId"] != parent_id or primitive["side"] != side:
                    raise TemporalDiscoveryContractError("topology+event event primitive parent drifted")
                if row["eligibility"] == "eligible" and row["settlingNodeId"] != plan["addedSetupNodeId"]:
                    raise TemporalDiscoveryContractError("topology+event slot must target the newly added setup node")
        if row["eligibility"] not in ELIGIBILITIES:
            raise TemporalDiscoveryContractError("slot eligibility drifted")
        if row["eligibility"] == "eligible" and row["ineligibilityReason"] is not None:
            raise TemporalDiscoveryContractError("eligible slot cannot carry an ineligibility reason")
        if row["eligibility"] == "ineligible" and not isinstance(row["ineligibilityReason"], str):
            raise TemporalDiscoveryContractError("ineligible slot requires a reason")
        slots.append(
            {
                "slotId": slot_id,
                "arm": arm,
                "parentCandidateId": parent_id,
                "side": side,
                "eligibility": row["eligibility"],
                "topologyPlanId": row["topologyPlanId"],
                "eventPrimitiveId": row["eventPrimitiveId"],
                "settlingNodeId": row["settlingNodeId"],
                "ineligibilityReason": row["ineligibilityReason"],
            }
        )
    return slots


def _validate_settling(value: Any) -> dict[str, Any]:
    row = _exact_object(value, SETTLING_KEYS, "settling")
    _require_text(row["kind"], "directional_event_insert", "settling.kind")
    _require_bool(row["mustTargetAddedSetupNodeId"], True, "mustTargetAddedSetupNodeId")
    _require_text(row["selection"], "frozen_matched_v38_event_primitive_set", "settling.selection")
    _require_text(row["matchedControlSite"], "parent_existing_setup_if_event_free_else_ineligible", "matchedControlSite")
    _require_bool(row["ineligibleCellsRemainExplicit"], True, "ineligibleCellsRemainExplicit")
    return {
        "kind": "directional_event_insert",
        "mustTargetAddedSetupNodeId": True,
        "selection": "frozen_matched_v38_event_primitive_set",
        "matchedControlSite": "parent_existing_setup_if_event_free_else_ineligible",
        "ineligibleCellsRemainExplicit": True,
    }


def topology_coadaptation_v3_from_config(config: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    overlay = config.get("topologyCoadaptationMatrix")
    if overlay is None:
        return None
    return validate_topology_coadaptation_matrix_v3(overlay)


def attach_topology_coadaptation_matrix_v3(
    generation_config: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation overlay requires pair generation v2")
    if "topologyCoadaptationMatrix" in generation_config:
        raise TemporalDiscoveryContractError("topology coadaptation overlay was supplied twice")
    config = {key: value for key, value in generation_config.items() if key != "configSha256"}
    config["topologyCoadaptationMatrix"] = validate_topology_coadaptation_matrix_v3(matrix)
    config["configSha256"] = canonical_sha256(config)
    return config


def build_topology_coadaptation_matrix_v3(
    *,
    parents: Sequence[Mapping[str, Any]],
    rotating_evidence_sha256: str,
    topology_plans: Sequence[Mapping[str, Any]],
    event_primitives: Sequence[Mapping[str, Any]],
    slots: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    body = {
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": COADAPTATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": True,
        "lexicographicFirstSettlingPlanForbidden": True,
        "parents": [dict(item) for item in parents],
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "developmentRole": "discovery_and_selection",
            "replicationPanelIds": ["panel-1", "panel-2"],
            "replicationRole": "inspected_replication_not_untouched_confirmation",
            "futureConfirmationPanel": {
                "createdInThisTask": False,
                "requiredBeforeProductionConclusion": True,
                "authorityMustBeBoundBeforeLaunch": True,
                "label": "future_untouched_confirmation_panel",
            },
            "rotatingEvidenceSha256": rotating_evidence_sha256,
        },
        "firstExperimentOperation": FIRST_EXPERIMENT_OPERATION,
        "forbiddenFirstExperimentOperations": list(FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS),
        "topologyLocalEventRequired": True,
        "arms": list(ARMS),
        "topologyPlans": [dict(item) for item in topology_plans],
        "eventPrimitives": [dict(item) for item in event_primitives],
        "slots": [dict(item) for item in slots],
        "settling": {
            "kind": "directional_event_insert",
            "mustTargetAddedSetupNodeId": True,
            "selection": "frozen_matched_v38_event_primitive_set",
            "matchedControlSite": "parent_existing_setup_if_event_free_else_ineligible",
            "ineligibleCellsRemainExplicit": True,
        },
        "successCalculation": dict(SUCCESS_VALUES),
        "notAdmittedOnFrontGenerationPath": True,
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_topology_coadaptation_matrix_v3(body)
