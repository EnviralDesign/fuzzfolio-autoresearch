"""v7 no-market authorities: confirmation, resource designs, fail-closed pair compile."""

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
from .temporal_qd_topology_coadaptation_v7 import BLOCK_CLASS_COMPLETE
from .temporal_qd_v38_followup_audit_v2 import V38_ROTATING_EVIDENCE_SHA256

TOPOLOGY_INSPECTED_AUTHORITY_SCHEMA = "temporal_qd_topology_case_study_inspected_task_authority_v7"
TOPOLOGY_CONFIRMATION_AUTHORITY_SCHEMA = "temporal_qd_topology_future_untouched_confirmation_authority_v7"
RESOURCE_CONFIRMATION_AUTHORITY_SCHEMA = "temporal_qd_resource_future_untouched_confirmation_authority_v7"
RESOURCE_SELECTED_RECEIPTS_SCHEMA = "temporal_qd_resource_selected_pair_receipts_v7"
RESOURCE_ONE_PLAN_SCHEMA = "temporal_qd_resource_one_plan_design_v7"
RESOURCE_NEAR_TWO_PLAN_SCHEMA = "temporal_qd_resource_near_two_plan_design_v7"
PAIR_COMPILE_ATTEMPT_SCHEMA = "temporal_qd_canonical_pair_compile_attempt_v7"


def attempt_canonical_pair_compile_and_native_validation_v7() -> dict[str, Any]:
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


def build_topology_inspected_task_authority_v7(
    *,
    topology: Mapping[str, Any],
    inventory_v2: Mapping[str, Any] | None = None,
    pair_attempt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_ids = _complete_receipt_ids(topology)
    n = len(candidate_ids)
    complete_blocks = sum(1 for block in topology["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    inspected_tasks = projected_inspected_panel_worker_tasks(pair_candidate_count=n)
    exact = n == 12 and inspected_tasks == 144 and complete_blocks == 3
    source = dict(inventory_v2["sourceIdentities"]) if inventory_v2 is not None else {}
    compiled = pair_attempt.get("frozenPairCompileRan") is True if pair_attempt else False
    native = pair_attempt.get("nativeValidationRan") is True if pair_attempt else False
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
        "completeBlockCount": complete_blocks,
        "armsPerBlock": 4,
        "projectedInspectedPanelWorkerTasks": inspected_tasks,
        "expectedInspectedPanelWorkerTasks": 144,
        "candidateReceiptIds": candidate_ids,
        "rotatingEvidenceSha256": topology["panelIdentities"]["rotatingEvidenceSha256"],
        "sourceIdentities": source,
        "workerContractSha256": source.get("workerContractSha256"),
        "catalogSource": source.get("catalogSource"),
        "pairCompileAttemptSha256": None if pair_attempt is None else pair_attempt.get("attemptSha256"),
        "launchBlockedUntilCanonicalFrozenPairCompileAndNativeValidation": not (exact and compiled and native),
        "readyForTopologyCaseStudyLaunch": exact and compiled and native,
        "experimentKind": "topology_case_study_complete_2x2_blocks_only",
    }
    body["authoritySha256"] = canonical_sha256(body)
    return body


def build_topology_confirmation_authority_v7(
    *,
    topology: Mapping[str, Any],
    rotating_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    n = len(_complete_receipt_ids(topology))
    confirmation_tasks = n * WINDOWS_PER_PANEL
    outer = (rotating_contract or {}).get("outerTail") if isinstance(rotating_contract, Mapping) else None
    panel4 = None
    if isinstance(rotating_contract, Mapping):
        panel4 = next((panel for panel in rotating_contract.get("panels") or [] if panel.get("panelId") == "panel-4"), None)
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
        "expectedConfirmationWorkerTasksIfTwelvePairs": 48,
        "futureConfirmationPanel": {
            "label": "future_untouched_confirmation_panel",
            "createdInThisTask": True,
            "executedInThisTask": False,
            "windowIdentitiesBound": False,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "requiredBeforeProductionConclusion": True,
            "noOverlapWithDevelopmentOrInspectedReplication": True,
            "panel4IsLatinSquareNotOuterTail": True,
            "panel4Selection": None if panel4 is None else panel4.get("selection"),
            "outerTailStart": None if not isinstance(outer, Mapping) else outer.get("analysisWindowStart"),
            "outerTailIsNotThisConfirmationPanel": True,
        },
        "rotatingEvidenceSha256": topology["panelIdentities"]["rotatingEvidenceSha256"],
        "selectionEvidenceOverlapForbidden": True,
        "authorityMustBeFrozenBeforeAnyLaterExecution": True,
        "experimentKind": "topology_case_study_complete_2x2_blocks_only",
        "mustNotReuseResourceInventoryAsTopologyAuthority": True,
    }
    body["authoritySha256"] = canonical_sha256(body)
    return body


def build_resource_confirmation_authority_v7(*, inventory_v2: Mapping[str, Any]) -> dict[str, Any]:
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


def build_resource_selected_pair_receipts_v7(
    *,
    inventory_v2: Mapping[str, Any],
    one_plan_cells: Sequence[Mapping[str, Any]],
    pair_attempt: Mapping[str, Any] | None = None,
    mutation_receipts: Sequence[Mapping[str, Any]] | None = None,
    clone_receipts: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    attempt = pair_attempt if pair_attempt is not None else attempt_canonical_pair_compile_and_native_validation_v7()
    clones = [dict(item) for item in clone_receipts] if clone_receipts is not None else _validate_clones_against_parents(inventory_v2)
    selected = [dict(cell) for cell in one_plan_cells if cell.get("status") == "filled"]
    receipts = [dict(item) for item in mutation_receipts] if mutation_receipts is not None else [
        {
            "parentCandidateId": cell["parentCandidateId"],
            "side": cell["side"],
            "lane": cell["lane"],
            "planSha256": cell["selectedPlanSha256"],
            "kind": "selected_one_plan_mutation_pair",
            "canonicalPairCompileRan": False,
            "nativeValidationRan": False,
            "launchAuthorityIdentityKind": attempt.get("launchAuthorityIdentityKind"),
        }
        for cell in selected
    ]
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
        "inventoryRemainsUnlaunched265SlotManifest": len(inventory_v2["slots"]) == 265,
    }
    body["receiptSetSha256"] = canonical_sha256(body)
    return body


def build_resource_one_plan_design_v7(*, proposal_v3: Mapping[str, Any]) -> dict[str, Any]:
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


def build_resource_near_two_plan_design_v7(*, inventory_v2: Mapping[str, Any]) -> dict[str, Any]:
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


def bind_frozen_v38_policies(
    *,
    topology: Mapping[str, Any],
    v38_archive: Mapping[str, Any] | None = None,
    rotating_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rotating = topology["panelIdentities"]["rotatingEvidenceSha256"]
    if rotating != V38_ROTATING_EVIDENCE_SHA256:
        raise ValueError("rotating evidence SHA drifted from frozen v38 pin")
    live_robust_sha = _DEFAULT_ROBUST.get("policySha256")
    live_direction_sha = DEFAULT_DIRECTION_SELECTION_POLICY.identity_sha256
    archive_sha = None
    robust_sha = None
    direction_sha = None
    cumulative_sha = None
    if isinstance(v38_archive, Mapping):
        archive_sha = v38_archive.get("policySha256")
        direction = (v38_archive.get("frozenPolicy") or {}).get("directionSelection") or {}
        direction_sha = direction.get("selectionPolicySha256")
        rotating_tx = v38_archive.get("rotatingEvidenceTransaction") or {}
        if rotating_tx.get("rotatingEvidenceSha256") not in {None, rotating}:
            raise ValueError("V38 archive rotating evidence SHA drifted from topology pin")
        cumulative_sha = rotating_tx.get("cumulativeArchiveSha256")
    if isinstance(rotating_contract, Mapping):
        if rotating_contract.get("rotatingEvidenceSha256") != rotating:
            raise ValueError("rebuilt rotating evidence SHA drifted from frozen v38 pin")
        robust = ((rotating_contract.get("robustSelection") or {}).get("policy") or {})
        robust_sha = robust.get("policySha256")
    body = {
        "archivePolicySha256": archive_sha,
        "rotatingEvidenceSha256": rotating,
        "robustBreederPolicySha256": robust_sha,
        "directionPolicySha256": direction_sha,
        "cumulativeArchiveSha256": cumulative_sha,
        "liveModuleRobustBreederPolicySha256": live_robust_sha,
        "liveModuleDirectionPolicyBindingSha256": live_direction_sha,
        "policyBindingMode": "explicit_v38_archive_and_rotating_contract_pins_not_live_module_defaults",
        "rotatingEvidencePinVerified": True,
        "archiveRobustDirectionCumulativeShasAvailableInFollowupArtifacts": all(
            isinstance(value, str) and value.startswith("sha256:")
            for value in (archive_sha, robust_sha, direction_sha, cumulative_sha)
        ),
        "missingPolicyShaReportedAsUnavailableNotDefaulted": True,
        "liveModuleDefaultsAreNotSubstitutedAsFrozenV38Pins": True,
        "directionEligibleNotDefaultedWhenEvidenceMissing": True,
    }
    body["policyBindingSha256"] = canonical_sha256(body)
    return body


TASK_MATRIX_SCHEMA = "temporal_qd_topology_case_study_inspected_task_matrix_v7"


def build_topology_inspected_task_matrix_v7(
    *,
    topology: Mapping[str, Any],
    rotating_contract: Mapping[str, Any],
    campaign_worker_contract_sha256: str | None,
    pair_attempt: Mapping[str, Any],
    panel_template_worker_contract_sha256: str | None = None,
) -> dict[str, Any]:
    complete_ids = []
    receipts = {(row["blockId"], row["arm"]): row for row in topology["materializationReceipts"]}
    for block in topology["blocks"]:
        if block["classification"] != BLOCK_CLASS_COMPLETE:
            continue
        for arm in topology["arms"]:
            receipt = receipts[(block["blockId"], arm)]
            if receipt.get("pairCompileStatus") != "canonical_frozen_pair_compiled_and_natively_validated":
                continue
            complete_ids.append(receipt)
    panels = [panel for panel in rotating_contract["panels"] if panel["panelId"] in {"panel-1", "panel-2", "panel-3"}]
    templates = rotating_contract.get("panelTemplates") or {}
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for receipt in complete_ids:
        for panel in panels:
            template = templates.get(panel["panelId"]) or {}
            for window in panel["windows"]:
                key = (receipt["receiptId"], panel["panelId"], window["windowId"])
                if key in seen:
                    raise ValueError("duplicate candidate/window in inspected task matrix")
                seen.add(key)
                rows.append(
                    {
                        "taskId": f"{receipt['receiptId']}|{panel['panelId']}|{window['windowId']}",
                        "receiptId": receipt["receiptId"],
                        "blockId": receipt["blockId"],
                        "arm": receipt["arm"],
                        "parentCandidateId": receipt["parentCandidateId"],
                        "side": receipt["side"],
                        "frozenPairIdentitySha256": receipt["frozenPairIdentitySha256"],
                        "pairCandidateIdentitySha256": receipt["pairCandidateIdentitySha256"],
                        "panelId": panel["panelId"],
                        "windowId": window["windowId"],
                        "analysisWindowStart": window["analysisWindowStart"],
                        "analysisWindowEnd": window["analysisWindowEnd"],
                        "panelTemplatePreparationSha256": template.get("preparationSha256"),
                        "panelTemplateAuthorityId": template.get("authorityId"),
                        "rotatingEvidenceSha256": rotating_contract["rotatingEvidenceSha256"],
                        "workerContractSha256": campaign_worker_contract_sha256,
                        "panelTemplateWorkerContractSha256": panel_template_worker_contract_sha256,
                        "productionArchiveWrite": False,
                        "dispatched": False,
                    }
                )
    rows.sort(key=lambda row: row["taskId"])
    compiled = pair_attempt.get("canonicalPairCount") == 12 and pair_attempt.get("nativeValidCount") == 12
    exact = len(complete_ids) == 12 and len(rows) == 144 and compiled
    body = {
        "schemaVersion": TASK_MATRIX_SCHEMA,
        "createdInThisTask": True,
        "executedInThisTask": False,
        "doNotLaunch": True,
        "dispatched": False,
        "productionArchiveWrite": False,
        "label": "executable_task_matrix" if exact else "task_plan_specification_not_launch_authority",
        "readyForTopologyCaseStudyLaunch": exact,
        "canonicalPairCandidateCount": len(complete_ids),
        "inspectedPanelIds": ["panel-1", "panel-2", "panel-3"],
        "windowsPerPanel": WINDOWS_PER_PANEL,
        "taskCount": len(rows),
        "expectedTaskCount": 144,
        "incompleteBlocksExcluded": True,
        "duplicateCandidateWindowForbidden": True,
        "pairCompileAttemptSha256": pair_attempt["attemptSha256"],
        "rotatingEvidenceSha256": rotating_contract["rotatingEvidenceSha256"],
        "campaignWorkerContractSha256": campaign_worker_contract_sha256,
        "panelTemplateWorkerContractSha256": panel_template_worker_contract_sha256,
        "tasks": rows,
    }
    body["taskMatrixSha256"] = canonical_sha256(body)
    return body


def build_standalone_receipt_set_v7(*, topology: Mapping[str, Any], pair_attempt: Mapping[str, Any]) -> dict[str, Any]:
    body = {
        "schemaVersion": "temporal_qd_topology_coadaptation_materialization_receipts_v7",
        "productionArchiveWrite": False,
        "nativeValidationRan": pair_attempt.get("nativeValidationRan") is True,
        "pairCompilerRan": pair_attempt.get("frozenPairCompileRan") is True,
        "pairCompileAttemptSha256": pair_attempt["attemptSha256"],
        "receipts": list(topology["materializationReceipts"]),
        "contractSha256": topology["contractSha256"],
    }
    body["receiptSetSha256"] = canonical_sha256(body)
    return body
