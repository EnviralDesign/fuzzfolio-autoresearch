"""v6 no-market authorities: confirmation, resource designs, fail-closed pair compile."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from .temporal_qd_generation_quality_audit import _DEFAULT_ROBUST
from .temporal_direction_selection import DEFAULT_DIRECTION_SELECTION_POLICY
from .evidence_plan import canonical_sha256
from .temporal_qd_resource_suboperation_inventory_v2 import (
    INSPECTED_PANEL_COUNT,
    WINDOWS_PER_PANEL,
    projected_inspected_panel_worker_tasks,
    projected_with_future_confirmation_panel,
)
from .temporal_qd_topology_coadaptation_v6 import BLOCK_CLASS_COMPLETE
from .temporal_qd_v38_followup_audit_v2 import V38_ROTATING_EVIDENCE_SHA256

TOPOLOGY_INSPECTED_AUTHORITY_SCHEMA = "temporal_qd_topology_case_study_inspected_task_authority_v6"
TOPOLOGY_CONFIRMATION_AUTHORITY_SCHEMA = "temporal_qd_topology_future_untouched_confirmation_authority_v6"
RESOURCE_CONFIRMATION_AUTHORITY_SCHEMA = "temporal_qd_resource_future_untouched_confirmation_authority_v6"
RESOURCE_SELECTED_RECEIPTS_SCHEMA = "temporal_qd_resource_selected_pair_receipts_v6"
RESOURCE_ONE_PLAN_SCHEMA = "temporal_qd_resource_one_plan_design_v6"
RESOURCE_NEAR_TWO_PLAN_SCHEMA = "temporal_qd_resource_near_two_plan_design_v6"
PAIR_COMPILE_ATTEMPT_SCHEMA = "temporal_qd_canonical_pair_compile_attempt_v6"


def attempt_canonical_pair_compile_and_native_validation() -> dict[str, Any]:
    body = {
        "schemaVersion": PAIR_COMPILE_ATTEMPT_SCHEMA,
        "pairCompilerAvailable": False,
        "nativeValidatorAvailable": False,
        "frozenPairRunConfigBound": False,
        "frozenPairCompileRan": False,
        "nativeValidationRan": False,
        "dashboardOwnedPairCompilerInvoked": False,
        "unavailableReason": (
            "canonical FrozenPair.compile requires FrozenModule payloads plus a Dashboard-owned "
            "pair compiler and native validator from a frozen pair-run config; no such config is "
            "bound in the v38 follow-up artifact set"
        ),
        "launchAuthorityIdentityKind": "reconstructed_synthetic_not_canonical_frozen_pair",
        "syntheticPairCandidateIdentitiesRemainNonLaunch": True,
        "doNotLaunch": True,
        "marketEvaluationLaunched": False,
    }
    body["attemptSha256"] = canonical_sha256(body)
    return body


def _complete_receipt_ids(topology: Mapping[str, Any]) -> list[str]:
    receipts = {(row["blockId"], row["arm"]): row for row in topology["materializationReceipts"]}
    ids: list[str] = []
    for block in topology["blocks"]:
        if block["classification"] != BLOCK_CLASS_COMPLETE:
            continue
        for arm in topology["arms"]:
            ids.append(str(receipts[(block["blockId"], arm)]["receiptId"]))
    return ids


def build_topology_inspected_task_authority_v6(
    *,
    topology: Mapping[str, Any],
    inventory_v2: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_ids = _complete_receipt_ids(topology)
    n = len(candidate_ids)
    if n != 12:
        raise ValueError("topology case study must bind exactly 12 complete-block pair receipts")
    inspected_tasks = projected_inspected_panel_worker_tasks(pair_candidate_count=n)
    if inspected_tasks != 144:
        raise ValueError("topology case study inspected authority must bind exactly 144 worker tasks")
    source = dict(inventory_v2["sourceIdentities"]) if inventory_v2 is not None else {}
    body = {
        "schemaVersion": TOPOLOGY_INSPECTED_AUTHORITY_SCHEMA,
        "createdInThisTask": True,
        "executedInThisTask": False,
        "doNotLaunch": True,
        "productionArchiveWrite": False,
        "incompleteBlocksExcluded": True,
        "developmentPanelId": "panel-3",
        "inspectedReplicationPanelIds": ["panel-1", "panel-2"],
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "inspectedPanelCount": INSPECTED_PANEL_COUNT,
        "canonicalPairCandidateCount": n,
        "completeBlockCount": 3,
        "armsPerBlock": 4,
        "projectedInspectedPanelWorkerTasks": inspected_tasks,
        "candidateReceiptIds": candidate_ids,
        "rotatingEvidenceSha256": topology["panelIdentities"]["rotatingEvidenceSha256"],
        "sourceIdentities": source,
        "workerContractSha256": source.get("workerContractSha256"),
        "catalogSource": source.get("catalogSource"),
        "pairCompileAttemptSha256": attempt_canonical_pair_compile_and_native_validation()["attemptSha256"],
        "launchBlockedUntilCanonicalFrozenPairCompileAndNativeValidation": True,
        "experimentKind": "topology_case_study_complete_2x2_blocks_only",
    }
    body["authoritySha256"] = canonical_sha256(body)
    return body


def build_topology_confirmation_authority_v6(*, topology: Mapping[str, Any]) -> dict[str, Any]:
    n = len(_complete_receipt_ids(topology))
    if n != 12:
        raise ValueError("topology confirmation must bind the same 12 complete-block pair receipts")
    confirmation_tasks = n * WINDOWS_PER_PANEL
    if confirmation_tasks != 48:
        raise ValueError("topology confirmation authority must bind exactly 48 worker tasks")
    body = {
        "schemaVersion": TOPOLOGY_CONFIRMATION_AUTHORITY_SCHEMA,
        "createdInThisTask": True,
        "executedInThisTask": False,
        "doNotLaunch": True,
        "developmentPanelId": "panel-3",
        "inspectedReplicationPanelIds": ["panel-1", "panel-2"],
        "inspectedReplicationAlreadyInfluencedDesign": True,
        "inspectedReplicationCannotServeAsUntouchedConfirmation": True,
        "canonicalPairCandidateCount": n,
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "projectedConfirmationWorkerTasks": confirmation_tasks,
        "futureConfirmationPanel": {
            "label": "future_untouched_confirmation_panel",
            "createdInThisTask": True,
            "executedInThisTask": False,
            "windowIdentitiesBound": False,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "requiredBeforeProductionConclusion": True,
            "noOverlapWithDevelopmentOrInspectedReplication": True,
        },
        "rotatingEvidenceSha256": topology["panelIdentities"]["rotatingEvidenceSha256"],
        "selectionEvidenceOverlapForbidden": True,
        "authorityMustBeFrozenBeforeAnyLaterExecution": True,
        "experimentKind": "topology_case_study_complete_2x2_blocks_only",
        "mustNotReuseResourceInventoryAsTopologyAuthority": True,
    }
    body["authoritySha256"] = canonical_sha256(body)
    return body


def build_resource_confirmation_authority_v6(*, inventory_v2: Mapping[str, Any]) -> dict[str, Any]:
    projection = inventory_v2["boundedTaskProjection"]
    body = {
        "schemaVersion": RESOURCE_CONFIRMATION_AUTHORITY_SCHEMA,
        "createdInThisTask": True,
        "executedInThisTask": False,
        "doNotLaunch": True,
        "experimentKind": "resource_suboperation_inventory_not_topology_case_study",
        "mustNotBeReusedAsTopologyAuthority": True,
        "pairCandidateCount": projection["pairCandidateCount"],
        "projectedInspectedPanelWorkerTasks": projection["projectedInspectedPanelWorkerTasks"],
        "projectedWithFutureConfirmationPanel": projection["projectedWithFutureConfirmationPanel"],
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "sourceIdentities": inventory_v2["sourceIdentities"],
        "rotatingEvidenceSha256": V38_ROTATING_EVIDENCE_SHA256,
        "inspectedReplicationPanelIds": ["panel-1", "panel-2"],
        "inspectedReplicationAlreadyInfluencedDesign": True,
        "selectionEvidenceOverlapForbidden": True,
        "authorityMustBeFrozenBeforeAnyLaterExecution": True,
        "futureConfirmationPanel": {
            "label": "future_untouched_confirmation_panel",
            "createdInThisTask": True,
            "windowIdentitiesBound": False,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "requiredBeforeProductionConclusion": True,
        },
    }
    body["authoritySha256"] = canonical_sha256(body)
    return body


def _eligible_slots_by_cell(inventory_v2: Mapping[str, Any]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for slot in inventory_v2["slots"]:
        if slot.get("eligibility") != "eligible":
            continue
        key = (str(slot["parentCandidateId"]), str(slot["side"]), str(slot["lane"]))
        grouped[key].append(dict(slot))
    for items in grouped.values():
        items.sort(key=lambda row: str(row["planSha256"]))
    return grouped


def _validate_clones_against_parents(inventory_v2: Mapping[str, Any]) -> list[dict[str, Any]]:
    parents = {row["candidateId"]: row for row in inventory_v2["parents"]}
    clones: list[dict[str, Any]] = []
    for clone in inventory_v2["pairCloneSlots"]:
        parent = parents[clone["parentCandidateId"]]
        if clone["parentLongProgramSha256"] != parent["longProgramSha256"]:
            raise ValueError("pair clone long program SHA drifted from frozen parent")
        if clone["parentShortProgramSha256"] != parent["shortProgramSha256"]:
            raise ValueError("pair clone short program SHA drifted from frozen parent")
        clones.append(
            {
                **dict(clone),
                "matchesFrozenParentLongShortProgramShas": True,
                "canonicalPairCompileRan": False,
                "nativeValidationRan": False,
            }
        )
    if len(clones) != 5:
        raise ValueError("expected five pair clones")
    return clones


def build_resource_selected_pair_receipts_v6(
    *,
    inventory_v2: Mapping[str, Any],
    one_plan_cells: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    attempt = attempt_canonical_pair_compile_and_native_validation()
    clones = _validate_clones_against_parents(inventory_v2)
    selected = [dict(cell) for cell in one_plan_cells if cell.get("status") == "filled"]
    receipts = []
    for cell in selected:
        receipts.append(
            {
                "parentCandidateId": cell["parentCandidateId"],
                "side": cell["side"],
                "lane": cell["lane"],
                "planSha256": cell["selectedPlanSha256"],
                "kind": "selected_one_plan_mutation_pair",
                "canonicalPairCompileRan": False,
                "nativeValidationRan": False,
                "launchAuthorityIdentityKind": attempt["launchAuthorityIdentityKind"],
            }
        )
    body = {
        "schemaVersion": RESOURCE_SELECTED_RECEIPTS_SCHEMA,
        "doNotLaunch": True,
        "selectedOnePlanCellCount": len(selected),
        "pairCloneCount": len(clones),
        "pairCompileAttempt": attempt,
        "selectedMutationReceipts": receipts,
        "pairCloneReceipts": clones,
        "keepImbalancedInventoryUnlaunched": True,
        "inventorySlotCount": len(inventory_v2["slots"]),
    }
    body["receiptSetSha256"] = canonical_sha256(body)
    return body


def build_resource_one_plan_design_v6(*, proposal_v3: Mapping[str, Any]) -> dict[str, Any]:
    filled = [cell for cell in proposal_v3["cells"] if cell.get("status") == "filled"]
    n = len(filled) + int(proposal_v3["pairCloneCount"])
    body = {
        "schemaVersion": RESOURCE_ONE_PLAN_SCHEMA,
        "doNotLaunch": True,
        "coverageKind": "deterministic_case_study_coverage_not_repeatability",
        "filledMutationCellCount": len(filled),
        "pairCloneCount": proposal_v3["pairCloneCount"],
        "pairCandidateCount": n,
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "inspectedPanelCount": INSPECTED_PANEL_COUNT,
        "projectedInspectedPanelWorkerTasks": projected_inspected_panel_worker_tasks(pair_candidate_count=n),
        "projectedWithFutureConfirmationPanel": projected_with_future_confirmation_panel(pair_candidate_count=n),
        "cells": [dict(cell) for cell in proposal_v3["cells"]],
        "pairCloneSlots": [dict(item) for item in proposal_v3["pairCloneSlots"]],
        "notRepeatability": True,
    }
    body["designSha256"] = canonical_sha256(body)
    return body


def build_resource_near_two_plan_design_v6(*, inventory_v2: Mapping[str, Any]) -> dict[str, Any]:
    grouped = _eligible_slots_by_cell(inventory_v2)
    cells: list[dict[str, Any]] = []
    mutation_candidates = 0
    for (parent_id, side, lane), slots in sorted(grouped.items()):
        first = slots[0]["planSha256"]
        second = slots[1]["planSha256"] if len(slots) > 1 else None
        take = 2 if second is not None else 1
        mutation_candidates += take
        cells.append(
            {
                "parentCandidateId": parent_id,
                "side": side,
                "lane": lane,
                "eligiblePlanCount": len(slots),
                "selectedPlanSha256": first,
                "optionalSecondPlanSha256": second,
                "plansConsumed": take,
                "coverageKind": "availability_backed_near_two_plan_not_repeatability",
            }
        )
    clones = _validate_clones_against_parents(inventory_v2)
    n = mutation_candidates + len(clones)
    body = {
        "schemaVersion": RESOURCE_NEAR_TWO_PLAN_SCHEMA,
        "doNotLaunch": True,
        "coverageKind": "availability_backed_near_two_plan_not_repeatability",
        "filledMutationCellCount": len(cells),
        "cellsWithAtLeastTwoPlans": sum(1 for cell in cells if cell["optionalSecondPlanSha256"]),
        "cellsWithExactlyOnePlan": sum(1 for cell in cells if cell["optionalSecondPlanSha256"] is None),
        "mutationPairCandidateCount": mutation_candidates,
        "pairCloneCount": len(clones),
        "pairCandidateCount": n,
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "inspectedPanelCount": INSPECTED_PANEL_COUNT,
        "projectedInspectedPanelWorkerTasks": projected_inspected_panel_worker_tasks(pair_candidate_count=n),
        "projectedWithFutureConfirmationPanel": projected_with_future_confirmation_panel(pair_candidate_count=n),
        "cells": cells,
        "pairCloneSlots": clones,
        "notRepeatability": True,
    }
    body["designSha256"] = canonical_sha256(body)
    return body


def bind_frozen_v38_policies(*, topology: Mapping[str, Any]) -> dict[str, Any]:
    rotating = topology["panelIdentities"]["rotatingEvidenceSha256"]
    if rotating != V38_ROTATING_EVIDENCE_SHA256:
        raise ValueError("rotating evidence SHA drifted from frozen v38 pin")
    live_robust_sha = _DEFAULT_ROBUST.get("policySha256")
    live_direction_sha = DEFAULT_DIRECTION_SELECTION_POLICY.identity_sha256
    body = {
        "archivePolicySha256": None,
        "rotatingEvidenceSha256": rotating,
        "robustBreederPolicySha256": None,
        "directionPolicySha256": None,
        "cumulativeArchiveSha256": None,
        "liveModuleRobustBreederPolicySha256": live_robust_sha,
        "liveModuleDirectionPolicyBindingSha256": live_direction_sha,
        "policyBindingMode": "explicit_v38_pins_not_live_module_defaults",
        "rotatingEvidencePinVerified": True,
        "archiveRobustDirectionCumulativeShasAvailableInFollowupArtifacts": False,
        "missingPolicyShaReportedAsUnavailableNotDefaulted": True,
        "liveModuleDefaultsAreNotSubstitutedAsFrozenV38Pins": True,
    }
    body["policyBindingSha256"] = canonical_sha256(body)
    return body


def build_standalone_receipt_set_v6(*, topology: Mapping[str, Any], pair_attempt: Mapping[str, Any]) -> dict[str, Any]:
    body = {
        "schemaVersion": "temporal_qd_topology_coadaptation_materialization_receipts_v6",
        "productionArchiveWrite": False,
        "nativeValidationRan": False,
        "pairCompilerRan": False,
        "pairCompileAttemptSha256": pair_attempt["attemptSha256"],
        "receipts": list(topology["materializationReceipts"]),
        "contractSha256": topology["contractSha256"],
    }
    body["receiptSetSha256"] = canonical_sha256(body)
    return body
