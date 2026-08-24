"""V38 follow-up audit v5: pair receipts, exact archive gates, honest task math.

Does not mutate v1-v4 report bytes except a README appendix. Does not launch a
generation, worker, Vast host, or market evaluation.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_direction_selection import DEFAULT_DIRECTION_SELECTION_POLICY
from .temporal_qd_generation_quality_audit import _DEFAULT_ROBUST, _gate_flags, _support_metrics
from .temporal_qd_resource_suboperation_inventory_v2 import (
    WINDOWS_PER_PANEL,
    build_resource_suboperation_balanced_design_proposal_v3,
    build_resource_suboperation_candidate_inventory_v2,
    projected_inspected_panel_worker_tasks,
    projected_with_future_confirmation_panel,
    validate_resource_suboperation_candidate_inventory_v2,
)
from .temporal_qd_topology_coadaptation_v4 import (
    ARM_E as V4_ARM_E,
    ARM_P as V4_ARM_P,
    ARM_T as V4_ARM_T,
    ARM_TE as V4_ARM_TE,
    BLOCK_CLASS_COMPLETE as V4_BLOCK_COMPLETE,
    validate_topology_coadaptation_matrix_v4,
)
from .temporal_qd_topology_coadaptation_v5 import (
    ARM_E,
    ARM_P,
    ARM_T,
    ARM_TE,
    ARMS,
    BLOCK_CLASS_COMPLETE,
    PAIR_COMPILE_STATUS_COMPLETE,
    PAIR_COMPILE_STATUS_INCOMPLETE,
    attach_topology_coadaptation_matrix_v5,
    build_topology_coadaptation_matrix_v5,
    module_compile_artifact_sha256,
    pair_candidate_identity_sha256,
    promising_coadaptation_observation,
    reconstructed_pair_program_identity_sha256,
    topology_coadaptation_v5_from_config,
    validate_topology_coadaptation_matrix_v5,
)
from .temporal_qd_v38_followup_audit import (
    DEFAULT_CATALOG,
    DEFAULT_OUTPUT,
    DEFAULT_V38_ROOT,
    write_report,
)
from .temporal_qd_v38_followup_audit_v2 import _self_hash
from .temporal_qd_v38_followup_audit_v4 import (
    ARCHIVE_FORENSIC_SCHEMA_V4,
    EVENT_INSERT_SCHEMA_V4,
    FOCUS_CHILD_ID,
    FOCUS_PARENT_ID,
    MULTI_PANEL_SCHEMA_V4,
)

EVENT_INSERT_SCHEMA_V5 = "temporal_qd_v38_directional_event_insert_forensic_v5"
MULTI_PANEL_SCHEMA_V5 = "temporal_qd_v38_multipanel_suboperation_v5"
ARCHIVE_FORENSIC_SCHEMA_V5 = "temporal_qd_v38_cumulative_event_child_archive_forensic_v5"
CONFIRMATION_SCHEMA_V5 = "temporal_qd_future_untouched_confirmation_panel_authority_v5"
COVERED_MONTHS = 36.0


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _window_rows(metrics: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in metrics:
        rows.append(
            {
                "closedTrades": int(item.get("closedTrades") or item.get("closedTrades") or 0),
                "conservativeNetR": float(item.get("conservativeNetR") or item.get("conservativeNetR") or 0.0),
            }
        )
    return rows


def build_archive_forensic_v5(archive_v4: Mapping[str, Any]) -> dict[str, Any]:
    focus = dict(archive_v4.get("focusRecord") or {})
    member = focus.get("cumulativeMember") if isinstance(focus.get("cumulativeMember"), Mapping) else {}
    windows = _window_rows(member.get("windowMetrics") or [])
    metrics = _support_metrics(windows, covered_months=COVERED_MONTHS)
    flags = _gate_flags(
        {"coveredMonths": COVERED_MONTHS},
        robust_policy=_DEFAULT_ROBUST,
        direction_policy=DEFAULT_DIRECTION_SELECTION_POLICY,
        windows=windows,
        covered_months=COVERED_MONTHS,
    )
    binding = []
    if flags["averageTradesPerMonthPass"] is not True:
        binding.append("average_trades_per_month_below_minimum")
    if flags["medianWindowNetPositive"] is not True:
        binding.append("median_window_conservative_net_not_positive")
    exact_flags = {
        "activeWindowFractionPass": flags["activeWindowFractionPass"],
        "averageTradesPerMonthPass": flags["averageTradesPerMonthPass"],
        "combinedSupportPass": flags["combinedSupportPass"],
        "directionEligible": flags["directionEligible"],
        "cumulativeNetPositive": flags["cumulativeNetPositive"],
        "medianWindowNetPositive": flags["medianWindowNetPositive"],
        "currentPanelQualityLike": flags["currentPanelQualityLike"],
        "currentPanelFrontierLike": flags["currentPanelFrontierLike"],
        "capacityConsidered": False,
        "competingMemberIsNotBindingCause": True,
    }
    focus_v5 = {
        **focus,
        "exactGateFlags": exact_flags,
        "exactSupportMetrics": {
            "activeWindows": sum(1 for row in windows if row["closedTrades"] > 0),
            "windowCount": len(windows),
            "activeWindowFraction": metrics["activeWindowFraction"],
            "closedTrades": sum(row["closedTrades"] for row in windows),
            "coveredMonths": COVERED_MONTHS,
            "averageTradesPerMonth": metrics["averageTradesPerMonth"],
            "minimumActiveWindowFraction": _DEFAULT_ROBUST["minimumActiveWindowFraction"],
            "minimumAverageClosedTradesPerCandidateMonth": _DEFAULT_ROBUST[
                "minimumAverageClosedTradesPerCandidateMonth"
            ],
            "cumulativeConservativeNetR": metrics["cumulativeConservativeNetR"],
            "medianWindowConservativeNetR": metrics["medianWindowConservativeNetR"],
            "worstWindowConservativeNetR": min((row["conservativeNetR"] for row in windows), default=0.0),
        },
        "reasonCode": "failed_trade_density_and_median_window_net",
        "bindingCauses": binding,
        "explanation": (
            "The child is cumulatively profitable and active in 11 of 12 windows, but it fails the "
            "frozen robust-breeder trade-density floor and the positive median-window net requirement. "
            "Quality and frontier admission never occur, so cell capacity and any competing member are "
            "not the binding exclusion."
        ),
    }
    body = {
        "schemaVersion": ARCHIVE_FORENSIC_SCHEMA_V5,
        "notAdmittedToFinalArchiveIsNotATerminalExplanation": True,
        "sourceArchiveForensicSchemaVersion": ARCHIVE_FORENSIC_SCHEMA_V4,
        "backfilledEventChildCount": archive_v4.get("backfilledEventChildCount"),
        "archiveParentEventChildCount": archive_v4.get("archiveParentEventChildCount"),
        "finalArchiveEventChildCount": archive_v4.get("finalArchiveEventChildCount"),
        "focusChildId": FOCUS_CHILD_ID,
        "focusParentId": FOCUS_PARENT_ID,
        "focusRecord": focus_v5,
        "records": list(archive_v4.get("records") or []),
        "limitations": list(archive_v4.get("limitations") or [])
        + ["exact_gate_flags_computed_from_frozen_robust_breeder_policy_without_new_market_eval"],
    }
    return _self_hash(body)


def build_event_and_multipanel_v5(
    *,
    event_v4: Mapping[str, Any],
    multipanel_v4: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    event = {key: value for key, value in event_v4.items() if key != "reportSha256"}
    event["schemaVersion"] = EVENT_INSERT_SCHEMA_V5
    event["sourceEventForensicSchemaVersion"] = EVENT_INSERT_SCHEMA_V4
    event["v5Note"] = "Mechanism partition and side-by-panel reversal are unchanged; this revision reseals identity."
    event = _self_hash(event)
    multipanel = {key: value for key, value in multipanel_v4.items() if key != "reportSha256"}
    multipanel["schemaVersion"] = MULTI_PANEL_SCHEMA_V5
    if "sideByPanel" in event:
        multipanel["sideByPanel"] = event["sideByPanel"]
    multipanel["sourceMultipanelSchemaVersion"] = MULTI_PANEL_SCHEMA_V4
    multipanel["inspectedReplicationPanelsAreNotUntouchedConfirmation"] = True
    return event, _self_hash(multipanel)


def _upgrade_receipt(
    *,
    receipt_v4: Mapping[str, Any],
    parent: Mapping[str, Any],
    arm_genomes: Mapping[str, str | None],
) -> dict[str, Any]:
    arm = str(receipt_v4["arm"])
    side = str(receipt_v4["side"])
    eligibility = str(receipt_v4["eligibility"])
    changed_genome = receipt_v4.get("genomeSha256")
    changed_program = receipt_v4.get("programSha256") or changed_genome
    changed_profile = receipt_v4.get("profileSha256")
    long_sha = changed_program if side == "long" else parent["longProgramSha256"]
    short_sha = changed_program if side == "short" else parent["shortProgramSha256"]
    opposite = parent["shortProgramSha256"] if side == "long" else parent["longProgramSha256"]
    complete = eligibility == "eligible" and isinstance(changed_program, str)
    pair_id = None
    candidate_id = None
    artifact = None
    if complete:
        pair_id = reconstructed_pair_program_identity_sha256(
            parent_candidate_id=str(receipt_v4["parentCandidateId"]),
            long_program_sha256=str(long_sha),
            short_program_sha256=str(short_sha),
        )
        candidate_id = pair_candidate_identity_sha256(
            parent_candidate_id=str(receipt_v4["parentCandidateId"]),
            arm=arm,
            side=side,
            reconstructed_pair_program_identity_sha256=pair_id,
        )
        artifact = module_compile_artifact_sha256(
            long_program_sha256=str(long_sha),
            short_program_sha256=str(short_sha),
            changed_side_profile_sha256=str(changed_profile) if isinstance(changed_profile, str) else None,
        )
    delta = receipt_v4.get("topologySemanticDelta")
    audit = receipt_v4.get("operatorApplicationAudit") if isinstance(receipt_v4.get("operatorApplicationAudit"), Mapping) else {}
    p_genome = arm_genomes.get(ARM_P)
    t_genome = arm_genomes.get(ARM_T)
    if arm == ARM_P:
        app_parent = app_child = changed_genome
    elif arm == ARM_T:
        app_parent = delta.get("beforeGenomeSha256") if isinstance(delta, Mapping) else p_genome
        app_child = changed_genome
    elif arm == ARM_E:
        app_parent = audit.get("parentGenomeSha256") or p_genome
        app_child = audit.get("childGenomeSha256") or changed_genome
    else:
        app_parent = audit.get("parentGenomeSha256") or t_genome
        app_child = audit.get("childGenomeSha256") or changed_genome
    return {
        "receiptId": receipt_v4["receiptId"],
        "blockId": receipt_v4["blockId"],
        "arm": arm,
        "parentCandidateId": receipt_v4["parentCandidateId"],
        "side": side,
        "eligibility": eligibility,
        "changedSideGenomeSha256": changed_genome,
        "changedSideProgramSha256": changed_program,
        "changedSideProfileSha256": changed_profile,
        "topologySignature": receipt_v4.get("topologySignature"),
        "resourceFingerprint": receipt_v4.get("resourceFingerprint"),
        "longProgramSha256": long_sha if complete else None,
        "shortProgramSha256": short_sha if complete else None,
        "unchangedOppositeProgramSha256": opposite if complete else None,
        "unchangedOppositeProgramPreserved": True if complete else None,
        "reconstructedPairProgramIdentitySha256": pair_id,
        "frozenPairIdentitySha256": None,
        "pairCandidateIdentitySha256": candidate_id,
        "pairProfileSha256": None,
        "normalizedProfileSnapshotSha256": None,
        "moduleCompilerPolicySha256": receipt_v4.get("nativeCompileValidationIdentity"),
        "moduleCompileArtifactSha256": artifact,
        "canonicalPairCompilerAuthoritySha256": None,
        "canonicalPairCompileReportSha256": None,
        "nativeValidationRan": False,
        "nativeValidationAuthoritySha256": None,
        "nativeValidationReportSha256": None,
        "pairCompileStatus": PAIR_COMPILE_STATUS_COMPLETE if complete else PAIR_COMPILE_STATUS_INCOMPLETE,
        "applicationParentGenomeSha256": app_parent if complete else None,
        "applicationChildGenomeSha256": app_child if complete else None,
        "topologySemanticDelta": delta,
        "operatorApplicationAudit": receipt_v4.get("operatorApplicationAudit"),
        "eventAttachesToAddedSetupNode": receipt_v4.get("eventAttachesToAddedSetupNode"),
        "productionArchiveWrite": False,
        "failureReason": receipt_v4.get("failureReason"),
    }


def build_topology_spec_v5(spec_v4: Mapping[str, Any]) -> dict[str, Any]:
    validated = validate_topology_coadaptation_matrix_v4(spec_v4)
    parents = {item["candidateId"]: item for item in validated["parents"]}
    receipts_v4 = list(validated["materializationReceipts"])
    by_block: dict[str, dict[str, Mapping[str, Any]]] = {}
    for receipt in receipts_v4:
        by_block.setdefault(str(receipt["blockId"]), {})[str(receipt["arm"])] = receipt
    upgraded: list[dict[str, Any]] = []
    for receipt in receipts_v4:
        block_receipts = by_block[str(receipt["blockId"])]
        arm_genomes = {
            arm: (block_receipts[arm].get("genomeSha256") if arm in block_receipts else None)
            for arm in ARMS
        }
        upgraded.append(
            _upgrade_receipt(
                receipt_v4=receipt,
                parent=parents[str(receipt["parentCandidateId"])],
                arm_genomes=arm_genomes,
            )
        )
    return build_topology_coadaptation_matrix_v5(
        parents=validated["parents"],
        rotating_evidence_sha256=validated["panelIdentities"]["rotatingEvidenceSha256"],
        topology_plans=validated["topologyPlans"],
        event_primitives=validated["eventPrimitives"],
        slots=validated["slots"],
        blocks=validated["blocks"],
        materialization_receipts=upgraded,
    )


def build_confirmation_authority_v5(*, inventory_v2: Mapping[str, Any], topology_v5: Mapping[str, Any]) -> dict[str, Any]:
    projection = inventory_v2["boundedTaskProjection"]
    body = {
        "schemaVersion": CONFIRMATION_SCHEMA_V5,
        "createdInThisTask": False,
        "executedInThisTask": False,
        "doNotLaunch": True,
        "developmentPanelId": "panel-3",
        "inspectedReplicationPanelIds": ["panel-1", "panel-2"],
        "inspectedReplicationAlreadyInfluencedDesign": True,
        "inspectedReplicationCannotServeAsUntouchedConfirmation": True,
        "futureConfirmationPanel": {
            "label": "future_untouched_confirmation_panel",
            "createdInThisTask": False,
            "windowIdentitiesBound": False,
            "windowsPerPanel": WINDOWS_PER_PANEL,
            "requiredBeforeProductionConclusion": True,
        },
        "rotatingEvidenceSha256": topology_v5["panelIdentities"]["rotatingEvidenceSha256"],
        "sourceIdentities": inventory_v2["sourceIdentities"],
        "pairCandidateCount": projection["pairCandidateCount"],
        "projectedInspectedPanelWorkerTasks": projection["projectedInspectedPanelWorkerTasks"],
        "projectedWithFutureConfirmationPanel": projection["projectedWithFutureConfirmationPanel"],
        "selectionEvidenceOverlapForbidden": True,
        "authorityMustBeFrozenBeforeAnyLaterExecution": True,
    }
    body["preparationSha256"] = canonical_sha256(body)
    return body


def decision_memo_v5(
    *,
    archive: Mapping[str, Any],
    inventory: Mapping[str, Any],
    proposal: Mapping[str, Any],
    topology: Mapping[str, Any],
) -> str:
    focus = archive["focusRecord"]
    metrics = focus["exactSupportMetrics"]
    flags = focus["exactGateFlags"]
    budget = inventory["boundedTaskProjection"]
    complete = sum(1 for block in topology["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    return "\n".join(
        [
            "# V38 follow-up decision memo v5",
            "",
            "Local contract repair only. No new market evaluation, generation, Vast host, or 1024x5.",
            "",
            "## Pair receipts",
            "",
            "Complete-block receipts now reconstruct both program legs, preserve the opposite side, and refuse to label compiler-policy SHA as native validation. FrozenPair.compile / native validation did not run; those fields are null.",
            "",
            f"Complete 2x2 blocks remain **{complete}** case studies. `insert_setup` is a timing mutation; occupancy/freshness fields are frozen unevaluated.",
            "",
            "## Archive gates",
            "",
            (
                f"Focus child `{FOCUS_CHILD_ID}`: active {metrics['activeWindows']}/{metrics['windowCount']} "
                f"({metrics['activeWindowFraction']:.4f}, pass={flags['activeWindowFractionPass']}); "
                f"trades {metrics['closedTrades']} / {metrics['coveredMonths']} months "
                f"avg {metrics['averageTradesPerMonth']:.4f} vs {metrics['minimumAverageClosedTradesPerCandidateMonth']}, "
                f"pass={flags['averageTradesPerMonthPass']}; cumulative {metrics['cumulativeConservativeNetR']:.4f}R, "
                f"pass={flags['cumulativeNetPositive']}; median {metrics['medianWindowConservativeNetR']:.4f}R, "
                f"pass={flags['medianWindowNetPositive']}; quality={flags['currentPanelQualityLike']}; "
                f"frontier={flags['currentPanelFrontierLike']}; capacityConsidered={flags['capacityConsidered']}."
            ),
            "",
            f"Binding causes: {', '.join(focus['bindingCauses'])}. Competing members are not the binding exclusion.",
            "",
            "## Task projection",
            "",
            (
                f"Mutation pairs {budget['eligibleMutationPairCount']} + pair clones {budget['pairCloneCount']} = "
                f"{budget['pairCandidateCount']}. Windows per panel {budget['windowsPerPanel']} x panels "
                f"{budget['panelCount']} = {budget['totalWindowCount']} inspected windows. "
                f"Projected inspected tasks **{budget['projectedInspectedPanelWorkerTasks']}**. "
                f"With future confirmation panel **{budget['projectedWithFutureConfirmationPanel']}**."
            ),
            "",
            (
                f"Balanced case-study coverage freezes lexicographic plan IDs. One child per filled cell is "
                f"coverage, not repeatability. Proposal inspected tasks "
                f"**{proposal['projectedInspectedPanelWorkerTasks']}**; with confirmation "
                f"**{proposal['projectedWithFutureConfirmationPanel']}**."
            ),
            "",
            "## Do not authorize",
            "",
            "No market evaluation, topology launch, resource-matrix launch, G6, V37/V38 continuation, 1024x5, family reweighting, gate changes, morphology nursery, or breeding from the V38 archive. Panels 1 and 2 already influenced design and are not untouched confirmation.",
            "",
        ]
    )


def run_audit_v5(*, output_dir: Path, v38_root: Path | None = None, catalog_path: Path | None = None) -> dict[str, Path]:
    del v38_root, catalog_path
    event_v4 = _load_json(output_dir / "v38-directional-event-insert-forensic-v4.json")
    multipanel_v4 = _load_json(output_dir / "v38-multipanel-suboperation-v4.json")
    archive_v4 = _load_json(output_dir / "v38-cumulative-event-child-archive-forensic-v4.json")
    inventory_v1 = _load_json(output_dir / "resource-suboperation-candidate-inventory-v1.json")
    spec_v4 = _load_json(output_dir / "topology-coadaptation-matrix-spec-v4.json")
    event, multipanel = build_event_and_multipanel_v5(event_v4=event_v4, multipanel_v4=multipanel_v4)
    archive = build_archive_forensic_v5(archive_v4)
    inventory = build_resource_suboperation_candidate_inventory_v2(inventory_v1)
    proposal = build_resource_suboperation_balanced_design_proposal_v3(inventory_v2=inventory)
    topology = build_topology_spec_v5(spec_v4)
    validate_topology_coadaptation_matrix_v5(topology)
    validate_resource_suboperation_candidate_inventory_v2(inventory)
    attach_topology_coadaptation_matrix_v5(
        {"schemaVersion": "temporal_qd_pair_generation_v2", "configSha256": "placeholder"},
        topology,
    )
    assert topology_coadaptation_v5_from_config({"schemaVersion": "temporal_qd_pair_generation_v2"}) is None
    confirmation = build_confirmation_authority_v5(inventory_v2=inventory, topology_v5=topology)
    observation = promising_coadaptation_observation(
        parent_net=2.0,
        topology_net=-8.0,
        event_net=-7.0,
        combined_net=-6.0,
        parent_worst=-1.0,
        topology_worst=-4.0,
        event_worst=-3.0,
        combined_worst=-2.5,
        metric_greater=lambda left, right: left > right,
        metric_not_worse=lambda left, right: left >= right,
    )
    assert observation["interactionObserved"] is True
    assert observation["usefulProgressiveInnovation"] is False
    assert observation["promising"] is False
    memo = decision_memo_v5(archive=archive, inventory=inventory, proposal=proposal, topology=topology)
    outputs: dict[str, tuple[Mapping[str, Any] | None, str]] = {
        "v38-directional-event-insert-forensic-v5.json": (
            event,
            "# V38 directional event-insert forensic v5\n\nResealed v4 mechanism partition. No new market evaluation.\n",
        ),
        "v38-multipanel-suboperation-v5.json": (
            multipanel,
            "# V38 multi-panel suboperation v5\n\nSelf-hash resealed after schema/sideByPanel. Panels 1-2 are inspected replication, not untouched confirmation.\n",
        ),
        "v38-cumulative-event-child-archive-forensic-v5.json": (
            archive,
            "# V38 cumulative event-child archive forensic v5\n\nExact robust-breeder gate flags. Competing members are not the binding cause.\n",
        ),
        "resource-suboperation-candidate-inventory-v2.json": (
            inventory,
            "# Resource-suboperation candidate inventory v2\n\nFive pair clones. Task projection uses 4 windows per panel. Do not launch.\n",
        ),
        "resource-suboperation-balanced-design-proposal-v3.json": (
            proposal,
            "# Resource-suboperation balanced design proposal v3\n\nFrozen lexicographic plan IDs. One-per-cell is case-study coverage. Do not launch.\n",
        ),
        "topology-coadaptation-matrix-spec-v5.json": (
            topology,
            "# Topology co-adaptation matrix spec v5\n\nPair-leg receipts, graph chaining, interaction vs useful innovation. Do not launch.\n",
        ),
        "future-untouched-confirmation-panel-authority-v5.json": (
            confirmation,
            "# Future untouched confirmation panel authority v5\n\nPrepared, not executed. Panels 1 and 2 are not untouched confirmation.\n",
        ),
    }
    written: dict[str, Path] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        if payload is not None:
            write_report(path, payload, markdown)
        written[name] = path
    memo_path = output_dir / "decision-memo-v5.md"
    memo_path.write_text(memo, encoding="utf-8", newline="\n")
    written["decision-memo-v5.md"] = memo_path
    receipts_path = output_dir / "topology-coadaptation-materialization-receipts-v5.json"
    receipts_path.write_text(
        canonical_json(
            {
                "schemaVersion": "temporal_qd_topology_coadaptation_materialization_receipts_v5",
                "productionArchiveWrite": False,
                "nativeValidationRan": False,
                "pairCompilerRan": False,
                "receipts": topology["materializationReceipts"],
                "contractSha256": topology["contractSha256"],
            }
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["topology-coadaptation-materialization-receipts-v5.json"] = receipts_path
    readme_v5 = output_dir / "README-v5.md"
    readme_v5.write_text(
        "\n".join(
            [
                "# V38 evolve-everything follow-up artifacts v5",
                "",
                "Generated locally from frozen v4 artifacts and current source. No new market evaluation. v1-v4 files in this folder are unchanged except the shared README appendix.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-directional-event-insert-forensic-v5.json` / `.md` | A |",
                "| `v38-multipanel-suboperation-v5.json` / `.md` | A |",
                "| `v38-cumulative-event-child-archive-forensic-v5.json` / `.md` | A/B |",
                "| `resource-suboperation-candidate-inventory-v2.json` / `.md` | F |",
                "| `resource-suboperation-balanced-design-proposal-v3.json` / `.md` | G |",
                "| `topology-coadaptation-matrix-spec-v5.json` / `.md` | B-E |",
                "| `topology-coadaptation-materialization-receipts-v5.json` | B/C |",
                "| `future-untouched-confirmation-panel-authority-v5.json` / `.md` | H |",
                "| `decision-memo-v5.md` | memo |",
                "| `README-v5.md` | index |",
                "",
                "Regenerate with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v5 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README-v5.md"] = readme_v5
    index = output_dir / "README.md"
    existing = index.read_text(encoding="utf-8") if index.is_file() else ""
    marker = "## v5"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    index.write_text(
        existing.rstrip()
        + "\n\n## v5\n\n"
        + "\n".join(
            [
                "v1-v4 files above are frozen. v5 repairs pair-level receipts, native-validation labeling, receipt chaining, useful-innovation vs interaction, 4-window task math, five pair clones, frozen balanced plan IDs, exact archive gates, and a prepared confirmation-panel authority. No market compute was launched.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-directional-event-insert-forensic-v5.json` / `.md` | A |",
                "| `v38-multipanel-suboperation-v5.json` / `.md` | A |",
                "| `v38-cumulative-event-child-archive-forensic-v5.json` / `.md` | A/B |",
                "| `resource-suboperation-candidate-inventory-v2.json` / `.md` | F |",
                "| `resource-suboperation-balanced-design-proposal-v3.json` / `.md` | G |",
                "| `topology-coadaptation-matrix-spec-v5.json` / `.md` | B-E |",
                "| `topology-coadaptation-materialization-receipts-v5.json` | B/C |",
                "| `future-untouched-confirmation-panel-authority-v5.json` / `.md` | H |",
                "| `decision-memo-v5.md` | memo |",
                "| `README-v5.md` | index |",
                "",
                "Regenerate v5 with:",
                "",
                "`python -m autoresearch.temporal_qd_v38_followup_audit_v5 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        ),
        encoding="utf-8",
        newline="\n",
    )
    written["README.md"] = index
    _ = projected_inspected_panel_worker_tasks, projected_with_future_confirmation_panel, V4_ARM_P, V4_ARM_T, V4_ARM_E, V4_ARM_TE, V4_BLOCK_COMPLETE
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v38-root", type=Path, default=DEFAULT_V38_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    written = run_audit_v5(output_dir=args.output_dir, v38_root=args.v38_root, catalog_path=args.catalog)
    for path in written.values():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
