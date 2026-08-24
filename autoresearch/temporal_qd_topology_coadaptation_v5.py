"""Experiment-only topology co-adaptation matrix v5.

v4 proved complete P/T/E/TE blocks. v5 reconstructs both pair legs, refuses to
label compiler-policy SHA as native validation, chains receipts to slots, and
splits interaction from useful progressive innovation. This overlay never
launches a market evaluation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA

COADAPTATION_SCHEMA = "temporal_qd_topology_coadaptation_matrix_v5"
COADAPTATION_MODE = "frozen_complete_2x2_insert_setup_then_topology_local_event_pair_receipts_v5"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
ARMS = (
    "exact_parent_clone",
    "topology_only_child",
    "event_only_control",
    "topology_then_topology_local_event",
)
ARM_P, ARM_T, ARM_E, ARM_TE = ARMS
FIRST_EXPERIMENT_OPERATION = "insert_setup"
FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS = ("insert_exit_region",)
PARENT_ROLES = ("archive", "inactive_control", "active_negative_control")
BLOCK_CLASS_COMPLETE = "complete_2x2_block"
BLOCK_CLASS_INCOMPLETE = "exploratory_incomplete_block"
EVENT_ONLY_SITE_LABEL = "matched_event_primitive_at_closest_valid_pre_position_setup_site"
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
    "blocks",
    "materializationReceipts",
    "designScope",
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
    "topologySemanticDelta",
    "topologySemanticDeltaSha256",
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
    "selectionProvenance",
)
SLOT_KEYS = (
    "slotId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "blockId",
    "topologyPlanId",
    "eventPrimitiveId",
    "settlingNodeId",
    "ineligibilityReason",
)
BLOCK_KEYS = (
    "blockId",
    "parentCandidateId",
    "side",
    "parentRole",
    "classification",
    "topologyPlanId",
    "eventPrimitiveId",
    "armSlotIds",
    "excludedFromPrimaryCoadaptationCalculation",
    "incompletenessReason",
)
ARM_SLOT_KEYS = ("exact_parent_clone", "topology_only_child", "event_only_control", "topology_then_topology_local_event")
RECEIPT_KEYS = (
    "receiptId",
    "blockId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "changedSideGenomeSha256",
    "changedSideProgramSha256",
    "changedSideProfileSha256",
    "topologySignature",
    "resourceFingerprint",
    "longProgramSha256",
    "shortProgramSha256",
    "unchangedOppositeProgramSha256",
    "unchangedOppositeProgramPreserved",
    "reconstructedPairProgramIdentitySha256",
    "frozenPairIdentitySha256",
    "pairCandidateIdentitySha256",
    "pairProfileSha256",
    "normalizedProfileSnapshotSha256",
    "moduleCompilerPolicySha256",
    "moduleCompileArtifactSha256",
    "canonicalPairCompilerAuthoritySha256",
    "canonicalPairCompileReportSha256",
    "nativeValidationRan",
    "nativeValidationAuthoritySha256",
    "nativeValidationReportSha256",
    "pairCompileStatus",
    "applicationParentGenomeSha256",
    "applicationChildGenomeSha256",
    "topologySemanticDelta",
    "operatorApplicationAudit",
    "eventAttachesToAddedSetupNode",
    "productionArchiveWrite",
    "failureReason",
)
PAIR_COMPILE_STATUS_COMPLETE = "module_sides_reconstructed_pair_compiler_unavailable"
PAIR_COMPILE_STATUS_INCOMPLETE = "incomplete_block_not_materialized"
SETTLING_KEYS = (
    "kind",
    "mustTargetAddedSetupNodeId",
    "selection",
    "matchedControlSite",
    "eventOnlySiteLabel",
    "ineligibleCellsRemainExplicit",
)
SUCCESS_KEYS = (
    "schemaVersion",
    "metricEquality",
    "qualifyingUnit",
    "incompleteBlocksExcludedFromPrimaryCalculation",
    "parentBeat",
    "riskQualifiedBeat",
    "fullEconomicPhenotypeTie",
    "supportDirectionQualityGates",
    "activityCostMechanismRequired",
    "parentBalancingRequired",
    "eventPlanBalancingRequired",
    "sideStratifiedReportingRequired",
    "requireTeNetStrictlyGreaterThanT",
    "requireTeNetStrictlyGreaterThanE",
    "requireTeNetStrictlyGreaterThanPForUsefulInnovation",
    "requireTeWorstWindowNotWorseThanTAndEOrExplicitNonqualifyingTradeoff",
    "requireTeWorstWindowNotWorseThanPForUsefulInnovation",
    "interactionIdentity",
    "interactionObservedIsDescriptiveOnly",
    "usefulProgressiveInnovationRequiresTeGreaterThanP",
    "promisingMeansUsefulProgressiveInnovationNotMereInteraction",
    "noFixedPnlMargin",
    "requireReplicationPanelSurvivalForPromisingClaim",
    "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    "doNotPromoteOnDevelopmentPanelAlone",
    "noveltyIsNotQuality",
    "familyLevelInferenceForbidden",
)
SUCCESS_VALUES = {
    "schemaVersion": "temporal_qd_topology_coadaptation_success_v5",
    "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
    "qualifyingUnit": "complete_2x2_block",
    "incompleteBlocksExcludedFromPrimaryCalculation": True,
    "parentBeat": "child_net_strictly_greater_under_canonical_metric_identity",
    "riskQualifiedBeat": "parentBeat_and_non_worse_worst_window",
    "fullEconomicPhenotypeTie": "equal_net_worst_median_active_window_fraction",
    "supportDirectionQualityGates": "unchanged_production_gates",
    "activityCostMechanismRequired": True,
    "parentBalancingRequired": True,
    "eventPlanBalancingRequired": True,
    "sideStratifiedReportingRequired": True,
    "requireTeNetStrictlyGreaterThanT": True,
    "requireTeNetStrictlyGreaterThanE": True,
    "requireTeNetStrictlyGreaterThanPForUsefulInnovation": True,
    "requireTeWorstWindowNotWorseThanTAndEOrExplicitNonqualifyingTradeoff": True,
    "requireTeWorstWindowNotWorseThanPForUsefulInnovation": True,
    "interactionIdentity": "TE_minus_T_minus_E_plus_P",
    "interactionObservedIsDescriptiveOnly": True,
    "usefulProgressiveInnovationRequiresTeGreaterThanP": True,
    "promisingMeansUsefulProgressiveInnovationNotMereInteraction": True,
    "noFixedPnlMargin": True,
    "requireReplicationPanelSurvivalForPromisingClaim": True,
    "requireUntouchedConfirmationPanelBeforeProductionConclusion": True,
    "doNotPromoteOnDevelopmentPanelAlone": True,
    "noveltyIsNotQuality": True,
    "familyLevelInferenceForbidden": True,
}
DESIGN_SCOPE_KEYS = (
    "unitOfInference",
    "familyLevelInferenceForbidden",
    "oneEventPerBlockCannotSupportOperatorRepeatability",
    "eventSelectionProvenance",
    "preferredFollowOnDesign",
    "preferredFollowOnNotLaunched",
    "computeScientificTradeoff",
    "futureUntouchedConfirmationPanelAuthorityMustBeFrozenBeforeExecution",
    "insertSetupIsTimingMutation",
    "insertSetupIsNotBehaviorPreservingWithoutReplay",
    "evaluationInstrumentation",
    "doNotLaunch",
)
EVALUATION_INSTRUMENTATION_KEYS = (
    "addedSetupNodeOccupancy",
    "barsInAddedSetup",
    "entryTimestampShiftVsP",
    "changedTradeOpportunityCount",
    "eventFreshnessAtE",
    "eventFreshnessAtTE",
    "transitionPathTraceOnAddedNode",
    "computedInThisTask",
)
EVALUATION_INSTRUMENTATION_VALUES = {
    "addedSetupNodeOccupancy": None,
    "barsInAddedSetup": None,
    "entryTimestampShiftVsP": None,
    "changedTradeOpportunityCount": None,
    "eventFreshnessAtE": None,
    "eventFreshnessAtTE": None,
    "transitionPathTraceOnAddedNode": None,
    "computedInThisTask": False,
}
DESIGN_SCOPE_VALUES = {
    "unitOfInference": "deterministic_case_study_complete_2x2_block",
    "familyLevelInferenceForbidden": True,
    "oneEventPerBlockCannotSupportOperatorRepeatability": True,
    "eventSelectionProvenance": "v38_development_panel_selected_heterogeneous",
    "preferredFollowOnDesign": "two_predeclared_event_primitives_per_complete_parent_side_topology_plan",
    "preferredFollowOnNotLaunched": True,
    "computeScientificTradeoff": "one_event_per_block_is_cheaper_and_can_only_support_exact_case_studies; two_events_per_block_doubles_E_and_TE_tasks_and_is_the_minimum_for_within_topology_event_replication",
    "futureUntouchedConfirmationPanelAuthorityMustBeFrozenBeforeExecution": True,
    "insertSetupIsTimingMutation": True,
    "insertSetupIsNotBehaviorPreservingWithoutReplay": True,
    "evaluationInstrumentation": dict(EVALUATION_INSTRUMENTATION_VALUES),
    "doNotLaunch": True,
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
    if not all(char in "0123456789abcdef" for char in value[7:]):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return value


def _optional_sha(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _require_sha(value, label)


def reconstructed_pair_program_identity_sha256(
    *,
    parent_candidate_id: str,
    long_program_sha256: str,
    short_program_sha256: str,
) -> str:
    return canonical_sha256(
        {
            "parentCandidateId": parent_candidate_id,
            "longProgramSha256": long_program_sha256,
            "shortProgramSha256": short_program_sha256,
        }
    )


def pair_candidate_identity_sha256(
    *,
    parent_candidate_id: str,
    arm: str,
    side: str,
    reconstructed_pair_program_identity_sha256: str,
) -> str:
    return canonical_sha256(
        {
            "parentCandidateId": parent_candidate_id,
            "arm": arm,
            "side": side,
            "reconstructedPairProgramIdentitySha256": reconstructed_pair_program_identity_sha256,
        }
    )


def module_compile_artifact_sha256(
    *,
    long_program_sha256: str,
    short_program_sha256: str,
    changed_side_profile_sha256: str | None,
) -> str:
    return canonical_sha256(
        {
            "longProgramSha256": long_program_sha256,
            "shortProgramSha256": short_program_sha256,
            "changedSideProfileSha256": changed_side_profile_sha256,
        }
    )


def _event_node_from_audit(audit: Mapping[str, Any] | None) -> str | None:
    if not isinstance(audit, Mapping):
        return None
    delta = audit.get("semanticDelta")
    if not isinstance(delta, list):
        delta = audit.get("semanticDelta")
    if not isinstance(delta, list) or not delta or not isinstance(delta[0], Mapping):
        return None
    node = delta[0].get("nodeId") or delta[0].get("nodeId")
    return node if isinstance(node, str) and node else None


def _audit_passed(audit: Mapping[str, Any] | None) -> bool:
    if not isinstance(audit, Mapping):
        return False
    write = audit.get("productionArchiveWrite")
    if write is None:
        write = audit.get("productionArchiveWrite")
    report = audit.get("staticInvariantReport")
    if report is None:
        report = audit.get("staticInvariantReport")
    passed = None
    if isinstance(report, Mapping):
        passed = report.get("allChecksPassed")
        if passed is None:
            passed = report.get("allChecksPassed")
    if audit.get("replayed") is True and write is False:
        return passed is not False
    return passed is True


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


def canonical_topology_semantic_delta(delta: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        "schemaVersion",
        "operation",
        "planSha256",
        "beforeGenomeSha256",
        "afterGenomeSha256",
        "beforeTopologySha256",
        "afterTopologySha256",
        "addedNodes",
        "removedNodes",
        "addedEdges",
        "removedEdges",
        "changedEdges",
    )
    row = _exact_object(delta, required, "topology semantic delta")
    if row["schemaVersion"] != "evolvable_module_topology_delta_v1":
        raise TemporalDiscoveryContractError("topology semantic delta schema drifted")
    if row["operation"] != FIRST_EXPERIMENT_OPERATION:
        raise TemporalDiscoveryContractError("topology semantic delta operation drifted")
    for key in ("planSha256", "beforeGenomeSha256", "afterGenomeSha256", "beforeTopologySha256", "afterTopologySha256"):
        _require_sha(row[key], key)
    for key in ("addedNodes", "removedNodes", "addedEdges", "removedEdges", "changedEdges"):
        if not isinstance(row[key], list) or any(not isinstance(item, str) for item in row[key]):
            raise _unexpected(key)
    return {
        "schemaVersion": "evolvable_module_topology_delta_v1",
        "operation": FIRST_EXPERIMENT_OPERATION,
        "planSha256": row["planSha256"],
        "beforeGenomeSha256": row["beforeGenomeSha256"],
        "afterGenomeSha256": row["afterGenomeSha256"],
        "beforeTopologySha256": row["beforeTopologySha256"],
        "afterTopologySha256": row["afterTopologySha256"],
        "addedNodes": list(row["addedNodes"]),
        "removedNodes": list(row["removedNodes"]),
        "addedEdges": list(row["addedEdges"]),
        "removedEdges": list(row["removedEdges"]),
        "changedEdges": list(row["changedEdges"]),
    }


def topology_semantic_delta_sha256(delta: Mapping[str, Any]) -> str:
    return canonical_sha256(canonical_topology_semantic_delta(delta))


def coadaptation_interaction(*, parent: float, topology: float, event: float, combined: float) -> float:
    """Descriptive interaction TE - T - E + P. Not a promotion margin."""

    return combined - topology - event + parent


def promising_coadaptation_observation(
    *,
    parent_net: float,
    topology_net: float,
    event_net: float,
    combined_net: float,
    parent_worst: float,
    topology_worst: float,
    event_worst: float,
    combined_worst: float,
    metric_greater,
    metric_not_worse,
) -> dict[str, Any]:
    te_gt_t = metric_greater(combined_net, topology_net)
    te_gt_e = metric_greater(combined_net, event_net)
    te_gt_p = metric_greater(combined_net, parent_net)
    worst_vs_te = metric_not_worse(combined_worst, topology_worst) and metric_not_worse(combined_worst, event_worst)
    worst_vs_p = metric_not_worse(combined_worst, parent_worst)
    interaction_observed = te_gt_t and te_gt_e
    useful = te_gt_p and worst_vs_p and te_gt_t and te_gt_e
    return {
        "teNetGreaterThanT": te_gt_t,
        "teNetGreaterThanE": te_gt_e,
        "teNetGreaterThanP": te_gt_p,
        "teWorstWindowNotWorseThanTAndE": worst_vs_te,
        "teWorstWindowNotWorseThanP": worst_vs_p,
        "interactionObserved": interaction_observed,
        "usefulProgressiveInnovation": useful,
        "promising": useful,
        "interactionNetR": coadaptation_interaction(
            parent=parent_net, topology=topology_net, event=event_net, combined=combined_net
        ),
        "parentBeatIsNotSufficient": True,
        "interactionIsNotUsefulProgressiveInnovation": True,
    }


def _validate_parents(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation v5 requires frozen parents")
    parents: list[dict[str, str]] = []
    seen: set[str] = set()
    has_archive = False
    for item in value:
        row = _exact_object(item, PARENT_KEYS, "topology coadaptation v5 parent")
        candidate_id = row["candidateId"]
        role = row["role"]
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise _unexpected("topology coadaptation v5 parent")
        if role not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("topology coadaptation v5 parent role is invalid")
        if candidate_id in seen:
            raise TemporalDiscoveryContractError(f"topology coadaptation v5 repeats parent {candidate_id}")
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
        raise TemporalDiscoveryContractError("topology coadaptation v5 requires at least one archive parent")
    return parents


def _validate_panels(value: Any) -> dict[str, Any]:
    row = _exact_object(value, PANEL_KEYS, "topology coadaptation v5 panelIdentities")
    _require_text(row["developmentPanelId"], "panel-3", "developmentPanelId")
    _require_text(row["developmentRole"], "discovery_and_selection", "developmentRole")
    if tuple(row["replicationPanelIds"] or ()) != ("panel-1", "panel-2"):
        raise TemporalDiscoveryContractError("topology coadaptation v5 replication panels drifted")
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
        delta = canonical_topology_semantic_delta(row["topologySemanticDelta"])
        if delta["planSha256"] != expected_sha:
            raise TemporalDiscoveryContractError("topology semantic delta does not bind the frozen plan")
        if added not in delta["addedNodes"]:
            raise TemporalDiscoveryContractError("topology semantic delta must include the added setup node")
        expected_delta_sha = topology_semantic_delta_sha256(delta)
        if row["topologySemanticDeltaSha256"] != expected_delta_sha:
            raise TemporalDiscoveryContractError("projected topology-delta hash cannot replace the actual application delta")
        records.append(
            {
                "planId": plan_id,
                "parentCandidateId": parent_id,
                "side": side,
                "topologyPlan": plan,
                "planSha256": expected_sha,
                "addedSetupNodeId": added,
                "applicability": "source_genome_matches_parent_side_program",
                "topologySemanticDelta": delta,
                "topologySemanticDeltaSha256": expected_delta_sha,
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
        if row["selectionProvenance"] != "v38_development_panel_selected_heterogeneous":
            raise TemporalDiscoveryContractError("event primitive selection provenance drifted")
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
                "selectionProvenance": "v38_development_panel_selected_heterogeneous",
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
        raise TemporalDiscoveryContractError("topology coadaptation v5 requires slots")
    topology_by_id = {item["planId"]: item for item in topology_plans}
    event_by_id = {item["primitiveId"]: item for item in event_primitives}
    slots: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        row = _exact_object(item, SLOT_KEYS, "topology coadaptation v5 slot")
        slot_id = row["slotId"]
        if not isinstance(slot_id, str) or not slot_id or slot_id in seen:
            raise _unexpected("topology coadaptation v5 slot")
        seen.add(slot_id)
        arm = row["arm"]
        if arm not in ARMS:
            raise TemporalDiscoveryContractError("topology coadaptation v5 slot arm drifted")
        parent_id = row["parentCandidateId"]
        if parent_id not in parent_by_id:
            raise TemporalDiscoveryContractError("topology coadaptation v5 slot parent drifted")
        side = row["side"]
        if side not in SIDES:
            raise TemporalDiscoveryContractError("topology coadaptation v5 slot side drifted")
        if not isinstance(row["blockId"], str) or not row["blockId"]:
            raise _unexpected("topology coadaptation v5 blockId")
        if arm == ARM_P:
            if row["topologyPlanId"] is not None or row["eventPrimitiveId"] is not None or row["settlingNodeId"] is not None:
                raise TemporalDiscoveryContractError("clone slot must not carry topology or event plans")
        elif arm == ARM_T:
            if row["eventPrimitiveId"] is not None or row["settlingNodeId"] is not None:
                raise TemporalDiscoveryContractError("topology-only slot must not include an event")
            if row["eligibility"] == "ineligible" and row["topologyPlanId"] is None:
                pass
            else:
                plan = topology_by_id.get(row["topologyPlanId"])
                if plan is None or plan["parentCandidateId"] != parent_id or plan["side"] != side:
                    raise TemporalDiscoveryContractError("topology-only slot plan parent binding drifted")
        elif arm == ARM_E:
            if row["topologyPlanId"] is not None:
                raise TemporalDiscoveryContractError("event-only control must not include topology")
            if row["eligibility"] == "ineligible" and row["eventPrimitiveId"] is None:
                pass
            else:
                primitive = event_by_id.get(row["eventPrimitiveId"])
                if primitive is None or primitive["parentCandidateId"] != parent_id or primitive["side"] != side:
                    raise TemporalDiscoveryContractError("event-only control primitive binding drifted")
                if row["eligibility"] == "eligible" and not isinstance(row["settlingNodeId"], str):
                    raise TemporalDiscoveryContractError("event-only control must name the closest valid pre-position setup site")
        elif arm == ARM_TE:
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
                if row["eligibility"] == "eligible":
                    added = plan["topologySemanticDelta"]["addedNodes"]
                    if row["settlingNodeId"] not in added:
                        raise TemporalDiscoveryContractError("TE event must target the actual added setup node")
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
                "blockId": row["blockId"],
                "topologyPlanId": row["topologyPlanId"],
                "eventPrimitiveId": row["eventPrimitiveId"],
                "settlingNodeId": row["settlingNodeId"],
                "ineligibilityReason": row["ineligibilityReason"],
            }
        )
    return slots


def _validate_blocks(
    value: Any,
    *,
    parent_by_id: Mapping[str, Mapping[str, str]],
    slots: Sequence[Mapping[str, Any]],
    topology_plans: Sequence[Mapping[str, Any]],
    event_primitives: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation v5 requires blocks")
    slots_by_block: dict[str, list[Mapping[str, Any]]] = {}
    for slot in slots:
        slots_by_block.setdefault(str(slot["blockId"]), []).append(slot)
    topology_by_id = {item["planId"]: item for item in topology_plans}
    event_by_id = {item["primitiveId"]: item for item in event_primitives}
    blocks: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in value:
        row = _exact_object(item, BLOCK_KEYS, "topology coadaptation v5 block")
        block_id = row["blockId"]
        if not isinstance(block_id, str) or not block_id or block_id in seen:
            raise _unexpected("topology coadaptation v5 block")
        seen.add(block_id)
        parent_id = row["parentCandidateId"]
        side = row["side"]
        if parent_id not in parent_by_id or side not in SIDES:
            raise TemporalDiscoveryContractError("block parent/side drifted")
        if row["parentRole"] != parent_by_id[parent_id]["role"]:
            raise TemporalDiscoveryContractError("block parent role drifted")
        if row["classification"] not in {BLOCK_CLASS_COMPLETE, BLOCK_CLASS_INCOMPLETE}:
            raise TemporalDiscoveryContractError("block classification drifted")
        arm_slots = _exact_object(row["armSlotIds"], ARM_SLOT_KEYS, "block armSlotIds")
        grouped = slots_by_block.get(block_id, [])
        by_arm = {slot["arm"]: slot for slot in grouped}
        if set(by_arm) != set(ARMS) or len(grouped) != 4:
            raise TemporalDiscoveryContractError("each block requires exactly one P/T/E/TE slot")
        for arm in ARMS:
            slot = by_arm[arm]
            if slot["slotId"] != arm_slots[arm]:
                raise TemporalDiscoveryContractError("block armSlotIds drifted")
            if slot["parentCandidateId"] != parent_id or slot["side"] != side:
                raise TemporalDiscoveryContractError("block slots must share parent and side")
        if row["classification"] == BLOCK_CLASS_COMPLETE:
            if any(by_arm[arm]["eligibility"] != "eligible" for arm in ARMS):
                raise TemporalDiscoveryContractError("complete block requires eligible P/T/E/TE arms")
            if row["excludedFromPrimaryCoadaptationCalculation"] is not False:
                raise TemporalDiscoveryContractError("complete blocks must enter the primary calculation")
            if row["incompletenessReason"] is not None:
                raise TemporalDiscoveryContractError("complete block cannot carry an incompleteness reason")
            t_slot, e_slot, te_slot = by_arm[ARM_T], by_arm[ARM_E], by_arm[ARM_TE]
            if t_slot["topologyPlanId"] != te_slot["topologyPlanId"] or t_slot["topologyPlanId"] != row["topologyPlanId"]:
                raise TemporalDiscoveryContractError("complete block T/TE topology plan drifted")
            if e_slot["eventPrimitiveId"] != te_slot["eventPrimitiveId"] or e_slot["eventPrimitiveId"] != row["eventPrimitiveId"]:
                raise TemporalDiscoveryContractError("E and TE must use the identical event primitive")
            plan = topology_by_id[str(te_slot["topologyPlanId"])]
            primitive = event_by_id[str(te_slot["eventPrimitiveId"])]
            if primitive["parentCandidateId"] != parent_id or primitive["side"] != side:
                raise TemporalDiscoveryContractError("complete block event primitive parent drifted")
            if te_slot["settlingNodeId"] != plan["addedSetupNodeId"]:
                raise TemporalDiscoveryContractError("complete block TE must target the actual added setup node")
        else:
            if row["excludedFromPrimaryCoadaptationCalculation"] is not True:
                raise TemporalDiscoveryContractError("incomplete blocks cannot enter qualification")
            if not isinstance(row["incompletenessReason"], str) or not row["incompletenessReason"]:
                raise TemporalDiscoveryContractError("incomplete block requires an explicit reason")
        blocks.append(
            {
                "blockId": block_id,
                "parentCandidateId": parent_id,
                "side": side,
                "parentRole": row["parentRole"],
                "classification": row["classification"],
                "topologyPlanId": row["topologyPlanId"],
                "eventPrimitiveId": row["eventPrimitiveId"],
                "armSlotIds": {arm: arm_slots[arm] for arm in ARMS},
                "excludedFromPrimaryCoadaptationCalculation": row["excludedFromPrimaryCoadaptationCalculation"],
                "incompletenessReason": row["incompletenessReason"],
            }
        )
    if set(slots_by_block) != seen:
        raise TemporalDiscoveryContractError("every slot must belong to exactly one declared block")
    return blocks


def _validate_receipts(
    value: Any,
    *,
    blocks: Sequence[Mapping[str, Any]],
    slots: Sequence[Mapping[str, Any]],
    parent_by_id: Mapping[str, Mapping[str, str]],
    topology_plans: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise _unexpected("materializationReceipts")
    if len(value) != len(slots):
        raise TemporalDiscoveryContractError("receipt count must equal declared slot count")
    complete_ids = {item["blockId"] for item in blocks if item["classification"] == BLOCK_CLASS_COMPLETE}
    slot_by_arm = {(item["blockId"], item["arm"]): item for item in slots}
    if len(slot_by_arm) != len(slots):
        raise TemporalDiscoveryContractError("slots must be unique per block arm")
    plan_by_id = {item["planId"]: item for item in topology_plans}
    seen: set[str] = set()
    seen_arms: set[tuple[str, str]] = set()
    receipts: list[dict[str, Any]] = []
    complete_arms: dict[str, dict[str, dict[str, Any]]] = {block_id: {} for block_id in complete_ids}
    for item in value:
        if isinstance(item, Mapping) and (
            "pairIdentitySha256" in item or "nativeCompileValidationIdentity" in item
        ):
            raise TemporalDiscoveryContractError("module hashes cannot be labeled as pair or native validation identities")
        row = _exact_object(item, RECEIPT_KEYS, "materialization receipt")
        receipt_id = row["receiptId"]
        if not isinstance(receipt_id, str) or not receipt_id or receipt_id in seen:
            raise _unexpected("materialization receipt")
        seen.add(receipt_id)
        arm = row["arm"]
        side = row["side"]
        block_id = row["blockId"]
        if arm not in ARMS or side not in SIDES:
            raise TemporalDiscoveryContractError("materialization receipt arm/side drifted")
        arm_key = (block_id, arm)
        if arm_key in seen_arms:
            raise TemporalDiscoveryContractError("duplicate receipt for a declared arm")
        seen_arms.add(arm_key)
        slot = slot_by_arm.get(arm_key)
        if slot is None:
            raise TemporalDiscoveryContractError("receipt does not bind to a declared slot")
        if row["parentCandidateId"] != slot["parentCandidateId"] or side != slot["side"]:
            raise TemporalDiscoveryContractError("receipt parent/side does not match its slot")
        _require_bool(row["productionArchiveWrite"], False, "materialization receipt productionArchiveWrite")
        parent = parent_by_id.get(row["parentCandidateId"])
        if parent is None:
            raise TemporalDiscoveryContractError("receipt parent is not in the frozen parent set")
        copied = {key: row[key] for key in RECEIPT_KEYS}
        copied["productionArchiveWrite"] = False
        if block_id in complete_ids:
            if copied["eligibility"] != "eligible":
                raise TemporalDiscoveryContractError("complete-block receipts must materialize")
            for key in (
                "changedSideGenomeSha256",
                "changedSideProgramSha256",
                "changedSideProfileSha256",
                "topologySignature",
                "resourceFingerprint",
                "longProgramSha256",
                "shortProgramSha256",
                "unchangedOppositeProgramSha256",
                "reconstructedPairProgramIdentitySha256",
                "pairCandidateIdentitySha256",
                "moduleCompilerPolicySha256",
                "moduleCompileArtifactSha256",
                "applicationParentGenomeSha256",
                "applicationChildGenomeSha256",
            ):
                _require_sha(copied[key], key)
            if copied["unchangedOppositeProgramPreserved"] is not True:
                raise TemporalDiscoveryContractError("complete-block receipts must preserve the opposite program")
            if copied["failureReason"] is not None:
                raise TemporalDiscoveryContractError("successful receipt cannot carry a failure reason")
            if not isinstance(copied["operatorApplicationAudit"], Mapping):
                raise _unexpected("operatorApplicationAudit")
            if copied["nativeValidationRan"] is not False:
                raise TemporalDiscoveryContractError("native validation must not be claimed unless it ran")
            if copied["frozenPairIdentitySha256"] is not None:
                raise TemporalDiscoveryContractError("frozen pair identity cannot be claimed without FrozenPair.compile")
            if copied["canonicalPairCompilerAuthoritySha256"] is not None or copied["canonicalPairCompileReportSha256"] is not None:
                raise TemporalDiscoveryContractError("canonical pair compiler fields require an actual pair compiler")
            if copied["nativeValidationAuthoritySha256"] is not None or copied["nativeValidationReportSha256"] is not None:
                raise TemporalDiscoveryContractError("native validation receipts require native validation")
            if copied["pairCompileStatus"] != PAIR_COMPILE_STATUS_COMPLETE:
                raise TemporalDiscoveryContractError("complete-block pairCompileStatus drifted")
            expected_long = copied["changedSideProgramSha256"] if side == "long" else parent["longProgramSha256"]
            expected_short = copied["changedSideProgramSha256"] if side == "short" else parent["shortProgramSha256"]
            if copied["longProgramSha256"] != expected_long or copied["shortProgramSha256"] != expected_short:
                raise TemporalDiscoveryContractError("pair program legs drifted")
            opposite = parent["shortProgramSha256"] if side == "long" else parent["longProgramSha256"]
            if copied["unchangedOppositeProgramSha256"] != opposite:
                raise TemporalDiscoveryContractError("opposite program was not preserved")
            expected_pair = reconstructed_pair_program_identity_sha256(
                parent_candidate_id=copied["parentCandidateId"],
                long_program_sha256=copied["longProgramSha256"],
                short_program_sha256=copied["shortProgramSha256"],
            )
            if copied["reconstructedPairProgramIdentitySha256"] != expected_pair:
                raise TemporalDiscoveryContractError("reconstructed pair program identity drifted")
            expected_candidate = pair_candidate_identity_sha256(
                parent_candidate_id=copied["parentCandidateId"],
                arm=arm,
                side=side,
                reconstructed_pair_program_identity_sha256=expected_pair,
            )
            if copied["pairCandidateIdentitySha256"] != expected_candidate:
                raise TemporalDiscoveryContractError("pair candidate identity drifted")
            expected_artifact = module_compile_artifact_sha256(
                long_program_sha256=copied["longProgramSha256"],
                short_program_sha256=copied["shortProgramSha256"],
                changed_side_profile_sha256=copied["changedSideProfileSha256"],
            )
            if copied["moduleCompileArtifactSha256"] != expected_artifact:
                raise TemporalDiscoveryContractError("module compile artifact identity drifted")
            if arm == ARM_TE and copied["eventAttachesToAddedSetupNode"] is not True:
                raise TemporalDiscoveryContractError("TE receipt must prove the event attaches to the added setup node")
            if arm in {ARM_T, ARM_TE}:
                delta = canonical_topology_semantic_delta(copied["topologySemanticDelta"])
                if arm == ARM_T and copied["changedSideGenomeSha256"] != delta["afterGenomeSha256"]:
                    raise TemporalDiscoveryContractError("T receipt genome must match the actual topology application delta")
            if not _audit_passed(copied["operatorApplicationAudit"]):
                raise TemporalDiscoveryContractError("operator application audit must pass")
            complete_arms[block_id][arm] = copied
        else:
            if copied["eligibility"] != "ineligible":
                raise TemporalDiscoveryContractError("incomplete-block receipts remain ineligible")
            if copied["pairCompileStatus"] != PAIR_COMPILE_STATUS_INCOMPLETE:
                raise TemporalDiscoveryContractError("incomplete-block pairCompileStatus drifted")
            if copied["nativeValidationRan"] is not False:
                raise TemporalDiscoveryContractError("native validation must not be claimed unless it ran")
        receipts.append(copied)
    if seen_arms != set(slot_by_arm):
        raise TemporalDiscoveryContractError("receipts must cover every declared slot and no extras")
    for block in blocks:
        if block["classification"] != BLOCK_CLASS_COMPLETE:
            continue
        arms = complete_arms.get(block["blockId"]) or {}
        if set(arms) != set(ARMS):
            raise TemporalDiscoveryContractError("all P/T/E/TE children must materialize, compile, and audit")
        parent_receipt = arms[ARM_P]
        topology_receipt = arms[ARM_T]
        event_receipt = arms[ARM_E]
        combined_receipt = arms[ARM_TE]
        topology_delta = canonical_topology_semantic_delta(topology_receipt["topologySemanticDelta"])
        if topology_delta["beforeGenomeSha256"] != parent_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("P genome must equal topology before genome")
        if topology_delta["afterGenomeSha256"] != topology_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("T genome must equal topology after genome")
        if parent_receipt["applicationParentGenomeSha256"] != parent_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("P application parent must be the parent genome")
        if parent_receipt["applicationChildGenomeSha256"] != parent_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("P application child must be the parent genome")
        if topology_receipt["applicationParentGenomeSha256"] != parent_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("T application parent must be the P genome")
        if topology_receipt["applicationChildGenomeSha256"] != topology_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("T application child must be the T genome")
        if event_receipt["applicationParentGenomeSha256"] != parent_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("E application parent must be the P genome")
        if event_receipt["applicationChildGenomeSha256"] != event_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("E application child must be the E genome")
        if combined_receipt["applicationParentGenomeSha256"] != topology_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("TE application parent must be the T genome")
        if combined_receipt["applicationChildGenomeSha256"] != combined_receipt["changedSideGenomeSha256"]:
            raise TemporalDiscoveryContractError("TE application child must be the TE genome")
        event_audit = event_receipt["operatorApplicationAudit"]
        combined_audit = combined_receipt["operatorApplicationAudit"]
        if event_audit.get("parentGenomeSha256") not in {None, parent_receipt["changedSideGenomeSha256"]}:
            raise TemporalDiscoveryContractError("E audit parent genome drifted")
        if combined_audit.get("parentGenomeSha256") not in {None, topology_receipt["changedSideGenomeSha256"]}:
            raise TemporalDiscoveryContractError("TE audit parent genome drifted")
        plan = plan_by_id.get(block["topologyPlanId"])
        if plan is None:
            raise TemporalDiscoveryContractError("complete block topology plan missing")
        added = plan["addedSetupNodeId"]
        te_node = _event_node_from_audit(combined_audit)
        if te_node not in {None, added}:
            raise TemporalDiscoveryContractError("TE event node must be the added setup node")
        if combined_receipt["eventAttachesToAddedSetupNode"] is not True:
            raise TemporalDiscoveryContractError("TE receipt must attach to the added setup node")
        if event_receipt["eventAttachesToAddedSetupNode"] is True:
            raise TemporalDiscoveryContractError("E receipt must not claim the added setup node")
    return receipts


def _validate_settling(value: Any) -> dict[str, Any]:
    row = _exact_object(value, SETTLING_KEYS, "settling")
    _require_text(row["kind"], "directional_event_insert", "settling.kind")
    _require_bool(row["mustTargetAddedSetupNodeId"], True, "mustTargetAddedSetupNodeId")
    _require_text(row["selection"], "frozen_matched_v38_event_primitive_set", "settling.selection")
    _require_text(
        row["matchedControlSite"],
        "parent_existing_setup_if_event_free_else_ineligible",
        "matchedControlSite",
    )
    _require_text(row["eventOnlySiteLabel"], EVENT_ONLY_SITE_LABEL, "eventOnlySiteLabel")
    _require_bool(row["ineligibleCellsRemainExplicit"], True, "ineligibleCellsRemainExplicit")
    return {
        "kind": "directional_event_insert",
        "mustTargetAddedSetupNodeId": True,
        "selection": "frozen_matched_v38_event_primitive_set",
        "matchedControlSite": "parent_existing_setup_if_event_free_else_ineligible",
        "eventOnlySiteLabel": EVENT_ONLY_SITE_LABEL,
        "ineligibleCellsRemainExplicit": True,
    }


def validate_topology_coadaptation_matrix_v5(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, ROOT_KEYS, "topology coadaptation v5")
    _require_text(row["schemaVersion"], COADAPTATION_SCHEMA, "schema")
    _require_text(row["mode"], COADAPTATION_MODE, "mode")
    _require_bool(row["includeCrossover"], False, "includeCrossover")
    _require_text(row["cloneControl"], CLONE_CONTROL, "cloneControl")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("topology coadaptation must not write the production archive")
    if row["mutationDepth"] != 1:
        raise TemporalDiscoveryContractError("topology coadaptation v5 mutationDepth drifted")
    _require_bool(row["morphologyNurseryDeferred"], True, "morphologyNurseryDeferred")
    _require_bool(row["lexicographicFirstSettlingPlanForbidden"], True, "lexicographicFirstSettlingPlanForbidden")
    _require_text(row["firstExperimentOperation"], FIRST_EXPERIMENT_OPERATION, "firstExperimentOperation")
    if tuple(row["forbiddenFirstExperimentOperations"] or ()) != FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS:
        raise TemporalDiscoveryContractError("forbiddenFirstExperimentOperations drifted")
    _require_bool(row["topologyLocalEventRequired"], True, "topologyLocalEventRequired")
    if tuple(row["arms"] or ()) != ARMS:
        raise TemporalDiscoveryContractError("topology coadaptation v5 arms drifted")
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
    blocks = _validate_blocks(
        row["blocks"],
        parent_by_id=parent_by_id,
        slots=slots,
        topology_plans=topology_plans,
        event_primitives=event_primitives,
    )
    receipts = _validate_receipts(
        row["materializationReceipts"],
        blocks=blocks,
        slots=slots,
        parent_by_id=parent_by_id,
        topology_plans=topology_plans,
    )
    settling = _validate_settling(row["settling"])
    success = _exact_object(row["successCalculation"], SUCCESS_KEYS, "successCalculation")
    if success != SUCCESS_VALUES:
        raise TemporalDiscoveryContractError("topology coadaptation v5 successCalculation drifted")
    design = _exact_object(row["designScope"], DESIGN_SCOPE_KEYS, "designScope")
    if design != DESIGN_SCOPE_VALUES:
        raise TemporalDiscoveryContractError("topology coadaptation v5 designScope drifted")
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
        "blocks": blocks,
        "materializationReceipts": receipts,
        "designScope": dict(DESIGN_SCOPE_VALUES),
        "settling": settling,
        "successCalculation": dict(SUCCESS_VALUES),
        "notAdmittedOnFrontGenerationPath": True,
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("topology coadaptation v5 identity drift")
    body["contractSha256"] = expected
    return body


def topology_coadaptation_v5_from_config(config: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    overlay = config.get("topologyCoadaptationMatrix")
    if overlay is None:
        return None
    return validate_topology_coadaptation_matrix_v5(overlay)


def attach_topology_coadaptation_matrix_v5(
    generation_config: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation overlay requires pair generation v2")
    if "topologyCoadaptationMatrix" in generation_config:
        raise TemporalDiscoveryContractError("topology coadaptation overlay was supplied twice")
    config = {key: value for key, value in generation_config.items() if key != "configSha256"}
    config["topologyCoadaptationMatrix"] = validate_topology_coadaptation_matrix_v5(matrix)
    config["configSha256"] = canonical_sha256(config)
    return config


def build_topology_coadaptation_matrix_v5(
    *,
    parents: Sequence[Mapping[str, Any]],
    rotating_evidence_sha256: str,
    topology_plans: Sequence[Mapping[str, Any]],
    event_primitives: Sequence[Mapping[str, Any]],
    slots: Sequence[Mapping[str, Any]],
    blocks: Sequence[Mapping[str, Any]],
    materialization_receipts: Sequence[Mapping[str, Any]],
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
        "blocks": [dict(item) for item in blocks],
        "materializationReceipts": [dict(item) for item in materialization_receipts],
        "designScope": dict(DESIGN_SCOPE_VALUES),
        "settling": {
            "kind": "directional_event_insert",
            "mustTargetAddedSetupNodeId": True,
            "selection": "frozen_matched_v38_event_primitive_set",
            "matchedControlSite": "parent_existing_setup_if_event_free_else_ineligible",
            "eventOnlySiteLabel": EVENT_ONLY_SITE_LABEL,
            "ineligibleCellsRemainExplicit": True,
        },
        "successCalculation": dict(SUCCESS_VALUES),
        "notAdmittedOnFrontGenerationPath": True,
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_topology_coadaptation_matrix_v5(body)
