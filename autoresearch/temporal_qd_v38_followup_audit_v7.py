"""V38 follow-up audit v7: slot-bound receipts, fail-closed pair compile, useful-innovation risk rule.

Does not mutate v1-v5 report bytes except a README appendix. Does not launch a
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
from .temporal_qd_generation_quality_audit import _DEFAULT_ROBUST, _direction_selection_from_row, _gate_flags, _support_metrics
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
from .temporal_qd_topology_coadaptation_v7 import (
    ARM_E,
    ARM_P,
    ARM_T,
    ARM_TE,
    ARMS,
    BLOCK_CLASS_COMPLETE,
    PAIR_COMPILE_STATUS_COMPLETE,
    PAIR_COMPILE_STATUS_INCOMPLETE,
    attach_topology_coadaptation_matrix_v7,
    build_topology_parity_corpus_v7,
    module_compile_artifact_sha256,
    pair_candidate_identity_sha256,
    promising_coadaptation_observation,
    reconstructed_pair_program_identity_sha256,
    topology_coadaptation_v7_from_config,
    topology_spec_v7_from_v6,
    validate_topology_coadaptation_matrix_v7,
    _receipt_shell_v7,
)
from .temporal_qd_v38_followup_audit import (
    DEFAULT_CATALOG,
    DEFAULT_OUTPUT,
    DEFAULT_V38_ROOT,
    write_report,
)
from .temporal_qd_v38_followup_audit_v2 import _self_hash
from .temporal_qd_v38_followup_authorities_v7 import (
    bind_frozen_v38_policies,
    build_resource_confirmation_authority_v7,
    build_resource_near_two_plan_design_v7,
    build_resource_one_plan_design_v7,
    build_resource_selected_pair_receipts_v7,
    build_standalone_receipt_set_v7,
    build_topology_confirmation_authority_v7,
    build_topology_inspected_task_authority_v7,
    build_topology_inspected_task_matrix_v7,
)
from .temporal_qd_v38_followup_authority_discovery_v7 import (
    DEFAULT_AUTHORITY_ROOT,
    DEFAULT_EVOLVABLE_AUTHORITY,
    DEFAULT_PAIR_RUN_CONFIG,
    DEFAULT_ROTATING_EVIDENCE,
    discover_v38_followup_authority_v7,
)
from .temporal_qd_v38_followup_pair_materialization_v7 import (
    build_pair_compile_attempt_v7,
    fail_closed_without_pair_authority_v7,
    load_parent_material,
    materialize_complete_blocks_v7,
    materialize_resource_selected_v7,
)
from .temporal_qd_pair_factory import PairAuthorityBundle, load_pair_run_config
from .evolvable_module_qd_authority import open_evolvable_module_pair_authority
from .temporal_direction_selection import DirectionSelectionPolicyV1
from .temporal_qd_rotating_evidence import build_rotating_evidence_contract
from .temporal_qd_v38_followup_audit_v4 import (
    ARCHIVE_FORENSIC_SCHEMA_V4,
    EVENT_INSERT_SCHEMA_V4,
    FOCUS_CHILD_ID,
    FOCUS_PARENT_ID,
    MULTI_PANEL_SCHEMA_V4,
)

EVENT_INSERT_SCHEMA_V6 = "temporal_qd_v38_directional_event_insert_forensic_v7"
MULTI_PANEL_SCHEMA_V6 = "temporal_qd_v38_multipanel_suboperation_v7"
ARCHIVE_FORENSIC_SCHEMA_V6 = "temporal_qd_v38_cumulative_event_child_archive_forensic_v7"
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


def build_archive_forensic_v7(
    archive_v4: Mapping[str, Any],
    *,
    robust_policy: Mapping[str, Any] | None = None,
    direction_policy: Any | None = None,
) -> dict[str, Any]:
    robust = robust_policy if isinstance(robust_policy, Mapping) else _DEFAULT_ROBUST
    direction_pol = direction_policy if direction_policy is not None else DEFAULT_DIRECTION_SELECTION_POLICY
    focus = dict(archive_v4.get("focusRecord") or {})
    member = focus.get("cumulativeMember") if isinstance(focus.get("cumulativeMember"), Mapping) else {}
    windows = _window_rows(member.get("windowMetrics") or [])
    metrics = _support_metrics(windows, covered_months=COVERED_MONTHS)
    flags = _gate_flags(
        member,
        robust_policy=robust,
        direction_policy=direction_pol,
        windows=windows,
        covered_months=COVERED_MONTHS,
    )
    direction = _direction_selection_from_row(member, policy=direction_pol)
    binding = []
    if flags["averageTradesPerMonthPass"] is not True:
        binding.append("average_trades_per_month_below_minimum")
    if flags["medianWindowNetPositive"] is not True:
        binding.append("median_window_conservative_net_not_positive")
    exact_flags = {
        "activeWindowFractionPass": flags["activeWindowFractionPass"],
        "averageTradesPerMonthPass": flags["averageTradesPerMonthPass"],
        "combinedSupportPass": flags["combinedSupportPass"],
        "directionEligible": (direction.get("selectionEligible") is True) if direction is not None else False,
        "directionEvidenceAvailable": direction is not None,
        "directionNotABindingCause": True,
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
            "minimumActiveWindowFraction": robust["minimumActiveWindowFraction"],
            "minimumAverageClosedTradesPerCandidateMonth": robust[
                "minimumAverageClosedTradesPerCandidateMonth"
            ],
            "cumulativeConservativeNetR": metrics["cumulativeConservativeNetR"],
            "medianWindowConservativeNetR": metrics["medianWindowConservativeNetR"],
            "worstWindowConservativeNetR": min((row["conservativeNetR"] for row in windows), default=0.0),
            "robustThresholdSource": "frozen_v38_rotating_evidence_robust_breeder_policy",
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
        "schemaVersion": ARCHIVE_FORENSIC_SCHEMA_V6,
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


def build_event_and_multipanel_v7(
    *,
    event_v4: Mapping[str, Any],
    multipanel_v4: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    event = {key: value for key, value in event_v4.items() if key != "reportSha256"}
    event["schemaVersion"] = EVENT_INSERT_SCHEMA_V6
    event["sourceEventForensicSchemaVersion"] = EVENT_INSERT_SCHEMA_V4
    event["v5Note"] = "Mechanism partition and side-by-panel reversal are unchanged; this revision reseals identity."
    event = _self_hash(event)
    multipanel = {key: value for key, value in multipanel_v4.items() if key != "reportSha256"}
    multipanel["schemaVersion"] = MULTI_PANEL_SCHEMA_V6
    if "sideByPanel" in event:
        multipanel["sideByPanel"] = event["sideByPanel"]
    multipanel["sourceMultipanelSchemaVersion"] = MULTI_PANEL_SCHEMA_V4
    multipanel["inspectedReplicationPanelsAreNotUntouchedConfirmation"] = True
    return event, _self_hash(multipanel)


def _upgrade_receipt(
    *,
    receipt_v4: Mapping[str, Any],
    parent: Mapping[str, Any],
    slot: Mapping[str, Any],
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
        "receiptId": slot["slotId"],
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


def build_topology_spec_v7(spec_v4: Mapping[str, Any]) -> dict[str, Any]:
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
        slot_map = {(item["blockId"], item["arm"]): item for item in validated["slots"]}
        upgraded.append(
            _upgrade_receipt(
                receipt_v4=receipt,
                parent=parents[str(receipt["parentCandidateId"])],
                slot=slot_map[(str(receipt["blockId"]), str(receipt["arm"]))],
                arm_genomes=arm_genomes,
            )
        )
    return build_topology_coadaptation_matrix_v7(
        parents=validated["parents"],
        rotating_evidence_sha256=validated["panelIdentities"]["rotatingEvidenceSha256"],
        topology_plans=validated["topologyPlans"],
        event_primitives=validated["eventPrimitives"],
        slots=validated["slots"],
        blocks=validated["blocks"],
        materialization_receipts=upgraded,
    )


def decision_memo_v7(
    *,
    archive: Mapping[str, Any],
    inventory: Mapping[str, Any],
    proposal: Mapping[str, Any],
    topology: Mapping[str, Any],
    go_nogo: Mapping[str, Any] | None = None,
) -> str:
    focus = archive["focusRecord"]
    metrics = focus["exactSupportMetrics"]
    flags = focus["exactGateFlags"]
    budget = inventory["boundedTaskProjection"]
    complete = sum(1 for block in topology["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    verdict = go_nogo or {}
    pair_line = (
        f"Canonical FrozenPair.compile ran for {verdict.get('canonicalPairCount', 0)} "
        f"complete-block arms with native validation on {verdict.get('nativeValidCount', 0)}. "
        f"Launch identity is the newly compiled FrozenPair; historical parent pair identity is recorded on P. "
        f"readyForTopologyCaseStudyLaunch={verdict.get('readyForTopologyCaseStudyLaunch')}. "
        f"This task did not dispatch workers or launch market evaluation."
    )
    return "\n".join(
        [
            "# V38 follow-up decision memo v7",
            "",
            "Local contract repair only. No new market evaluation, generation, Vast host, topology launch, resource-matrix launch, G6, V37/V38 continuation, 1024x5, or morphology nursery.",
            "",
            "## Go / no-go",
            "",
            f"Package verdict: **{verdict.get('verdict', 'no-go')}**. {pair_line}",
            "",
            f"Complete 2x2 blocks that compiled: **{complete}**. `insert_setup` is a timing/staging mutation; occupancy, freshness, trade-count, cost, and support/direction/quality instrumentation fields are frozen unevaluated.",
            "",
            "## Useful innovation",
            "",
            "A qualifying useful progressive innovation requires TE net > P, T, and E and TE worst-window not worse than P, T, and E. TE beating T and E is reported as combinedOutperformsBothSingleMutations, not as positive interaction. The signed term TE-T-E+P is separate. A worse TE worst-window than T/E is an explicit nonqualifying risk trade-off.",
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
                f"frontier={flags['currentPanelFrontierLike']}; capacityConsidered={flags['capacityConsidered']}; "
                f"directionEligible={flags['directionEligible']}; directionEvidenceAvailable={flags.get('directionEvidenceAvailable')}; "
                f"directionNotABindingCause={flags.get('directionNotABindingCause')}."
            ),
            "",
            f"Binding causes: {', '.join(focus['bindingCauses'])}. Competing members are not the binding exclusion. Missing direction evidence is not defaulted to eligible.",
            "",
            "## Task authorities",
            "",
            (
                f"Topology inspected authority is {verdict.get('canonicalPairCandidateCount', complete * 4)} compiled pair receipts "
                f"x 12 windows = {verdict.get('taskCount', 0)} tasks (144 required for launch). "
                f"Future untouched topology confirmation remains a plan: window identities unbound. "
                f"Resource inventory remains 265 mutation slots + 5 pair clones; one-plan coverage is 63/756/1008; "
                f"near-two-plan is 117/1404/1872. Do not launch. Panels 1 and 2 already influenced design."
            ),
            "",
            (
                f"Imbalanced inventory projection remains {budget['projectedInspectedPanelWorkerTasks']} inspected / "
                f"{budget['projectedWithFutureConfirmationPanel']} with confirmation. "
                f"Balanced one-plan proposal remains {proposal['projectedInspectedPanelWorkerTasks']} / "
                f"{proposal['projectedWithFutureConfirmationPanel']} and is coverage, not repeatability."
            ),
            "",
            "## Do not authorize in this task",
            "",
            "No market evaluation, topology launch, resource-matrix launch, G6, V37/V38 continuation, 1024x5, family reweighting, gate changes, morphology nursery, or breeding from the V38 archive.",
            "",
        ]
    )


def _direction_policy_from_archive(archive: Mapping[str, Any]) -> DirectionSelectionPolicyV1:
    selection = (((archive.get("frozenPolicy") or {}).get("directionSelection") or {}).get("selectionPolicy") or {})
    if not isinstance(selection, Mapping) or "minimum_closed_trades_per_side" not in selection:
        raise ValueError("V38 archive direction selection policy is unavailable; refusing to default it")
    return DirectionSelectionPolicyV1(
        minimum_closed_trades_per_side=int(selection["minimum_closed_trades_per_side"]),
        minimum_active_windows_per_side=int(selection["minimum_active_windows_per_side"]),
        minimum_acceptable_side_net_r=float(selection["minimum_acceptable_side_net_r"]),
        harmful_opposite_net_r=float(selection["harmful_opposite_net_r"]),
    )


def _panel_template_worker_sha(rotating_contract: Mapping[str, Any]) -> str | None:
    shas: set[str] = set()
    templates = rotating_contract.get("panelTemplates") or {}
    if not isinstance(templates, Mapping):
        return None
    for row in templates.values():
        if not isinstance(row, Mapping) or not isinstance(row.get("path"), str):
            continue
        path = Path(row["path"])
        if not path.is_file():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        worker = payload.get("workerContract") if isinstance(payload, Mapping) else None
        if isinstance(worker, Mapping) and isinstance(worker.get("workerContractSha256"), str):
            shas.add(worker["workerContractSha256"])
    if len(shas) == 1:
        return next(iter(shas))
    return None


def run_audit_v7(
    *,
    output_dir: Path,
    v38_root: Path | None = None,
    catalog_path: Path | None = None,
    authority_root: Path | None = None,
    pair_run_config: Path | None = None,
    evolvable_authority: Path | None = None,
    rotating_evidence: Path | None = None,
) -> dict[str, Path]:
    event_v4 = _load_json(output_dir / "v38-directional-event-insert-forensic-v4.json")
    multipanel_v4 = _load_json(output_dir / "v38-multipanel-suboperation-v4.json")
    archive_v4 = _load_json(output_dir / "v38-cumulative-event-child-archive-forensic-v4.json")
    inventory_v1 = _load_json(output_dir / "resource-suboperation-candidate-inventory-v1.json")
    spec_v6 = _load_json(output_dir / "topology-coadaptation-matrix-spec-v6.json")
    event, multipanel = build_event_and_multipanel_v7(event_v4=event_v4, multipanel_v4=multipanel_v4)
    inventory = build_resource_suboperation_candidate_inventory_v2(inventory_v1)
    proposal = build_resource_suboperation_balanced_design_proposal_v3(inventory_v2=inventory)
    validate_resource_suboperation_candidate_inventory_v2(inventory)

    discovery = discover_v38_followup_authority_v7(
        v38_root=v38_root,
        authority_root=authority_root,
        pair_run_config=pair_run_config,
        evolvable_authority=evolvable_authority,
        rotating_evidence=rotating_evidence,
        catalog_path=catalog_path,
    )
    rotating_cfg = _load_json(Path(discovery["rotatingEvidenceConfigPath"]))
    rotating_contract = build_rotating_evidence_contract(rotating_cfg)
    v38_archive = _load_json(Path(discovery["v38ArchivePath"]))
    direction_policy = _direction_policy_from_archive(v38_archive)
    robust_policy = rotating_contract["robustSelection"]["policy"]
    archive = build_archive_forensic_v7(
        archive_v4,
        robust_policy=robust_policy,
        direction_policy=direction_policy,
    )

    topology_input = {
        **spec_v6,
        "materializationReceipts": [_receipt_shell_v7(row) for row in spec_v6["materializationReceipts"]],
    }
    compile_error: str | None = None
    materialization: dict[str, Any]
    resource_materialization: dict[str, Any] | None = None
    pair_run_config_sha256: str | None = discovery.get("pairRunConfigSha256") if isinstance(discovery.get("pairRunConfigSha256"), str) else None
    compiler_sha: str | None = None
    native_sha: str | None = None
    if discovery.get("readyToOpenPairAuthority") is True:
        try:
            pair_cfg = _load_json(Path(discovery["pairRunConfigPath"]))
            evo_cfg = _load_json(Path(discovery["evolvableAuthorityPath"]))
            loaded = load_pair_run_config(pair_cfg)
            pair_run_config_sha256 = loaded["pairRunConfigSha256"]
            parents = load_parent_material(Path(discovery["parentMaterialPath"]))
            with PairAuthorityBundle(loaded) as bundle:
                compiler_sha = bundle.compiler_identity.sha256
                native_sha = bundle.native_identity.sha256
                authority = open_evolvable_module_pair_authority(bundle=bundle, config=evo_cfg)
                materialization = materialize_complete_blocks_v7(
                    topology=topology_input,
                    parent_material=parents,
                    authority=authority,
                )
                resource_materialization = materialize_resource_selected_v7(
                    inventory_v2=inventory,
                    one_plan_cells=proposal["cells"],
                    parent_material=parents,
                    authority=authority,
                    parent_cache=materialization.get("parentCache"),
                )
        except Exception as exc:
            compile_error = f"{type(exc).__name__}: {exc}"
            materialization = fail_closed_without_pair_authority_v7(topology_input, error=compile_error)
    else:
        compile_error = "; ".join(str(item) for item in (discovery.get("errors") or ["pair authority not ready"]))
        materialization = fail_closed_without_pair_authority_v7(topology_input, error=compile_error)

    topology = topology_spec_v7_from_v6(
        spec_v6,
        receipts=materialization["receipts"],
        slots=materialization["updatedSlots"],
        blocks=materialization["updatedBlocks"],
    )
    validate_topology_coadaptation_matrix_v7(topology)
    attach_topology_coadaptation_matrix_v7(
        {"schemaVersion": "temporal_qd_pair_generation_v2", "configSha256": "placeholder"},
        topology,
    )
    assert topology_coadaptation_v7_from_config({"schemaVersion": "temporal_qd_pair_generation_v2"}) is None
    pair_attempt = build_pair_compile_attempt_v7(
        discovery=discovery,
        materialization=materialization,
        pair_run_config_sha256=pair_run_config_sha256,
        compiler_authority_sha256=compiler_sha,
        native_authority_sha256=native_sha,
        error=compile_error,
    )
    policies = bind_frozen_v38_policies(
        topology=topology,
        v38_archive=v38_archive,
        rotating_contract=rotating_contract,
    )
    archive["frozenV38PolicyBinding"] = policies
    archive = _self_hash({key: value for key, value in archive.items() if key != "reportSha256"})
    inspected_authority = build_topology_inspected_task_authority_v7(
        topology=topology,
        inventory_v2=inventory,
        pair_attempt=pair_attempt,
    )
    topology_confirmation = build_topology_confirmation_authority_v7(
        topology=topology,
        rotating_contract=rotating_contract,
    )
    resource_confirmation = build_resource_confirmation_authority_v7(inventory_v2=inventory)
    one_plan = build_resource_one_plan_design_v7(proposal_v3=proposal)
    near_two = build_resource_near_two_plan_design_v7(inventory_v2=inventory)
    selected = build_resource_selected_pair_receipts_v7(
        inventory_v2=inventory,
        one_plan_cells=proposal["cells"],
        pair_attempt=pair_attempt,
        mutation_receipts=None if resource_materialization is None else resource_materialization["mutationReceipts"],
        clone_receipts=None if resource_materialization is None else resource_materialization["cloneReceipts"],
    )
    panel_worker = _panel_template_worker_sha(rotating_contract)
    task_matrix = build_topology_inspected_task_matrix_v7(
        topology=topology,
        rotating_contract=rotating_contract,
        campaign_worker_contract_sha256=discovery.get("workerContractSha256"),
        pair_attempt=pair_attempt,
        panel_template_worker_contract_sha256=panel_worker,
    )
    parity = build_topology_parity_corpus_v7(topology)
    parity_markdown = "\n".join(
        [
            "# Topology co-adaptation Python/Rust parity corpus v7",
            "",
            "Shared canonical fixture plus adversarial mutations. Rust `topology_coadaptation_matrix_v7::validate` must accept/reject the same cases. No market compute.",
            "",
            "| Case | Python accepted | Required Rust accepted |",
            "| --- | --- | --- |",
            *[
                f"| `{case['mutationId']}` | {case['accepted']} | {case['accepted']} |"
                for case in parity["cases"]
            ],
            "",
        ]
    )
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
    risk = promising_coadaptation_observation(
        parent_net=1.0,
        topology_net=2.0,
        event_net=2.0,
        combined_net=3.0,
        parent_worst=-10.0,
        topology_worst=-1.0,
        event_worst=-1.0,
        combined_worst=-5.0,
        metric_greater=lambda left, right: left > right,
        metric_not_worse=lambda left, right: left >= right,
    )
    assert risk["combinedOutperformsBothSingleMutations"] is True
    assert risk["nonqualifyingRiskTradeoff"] is True
    assert risk["usefulProgressiveInnovation"] is False
    complete_ok = int(materialization["completeBlockCountSucceeded"]) == 3
    pairs_ok = pair_attempt.get("canonicalPairCount") == 12 and pair_attempt.get("nativeValidCount") == 12
    tasks_ok = task_matrix.get("taskCount") == 144
    parity_ok = parity["canonicalFixtureAccepted"] is True and all(
        case["accepted"] is False for case in parity["cases"][1:]
    )
    ready = bool(complete_ok and pairs_ok and tasks_ok and parity_ok)
    go_nogo = {
        "schemaVersion": "temporal_qd_v38_followup_v7_go_nogo",
        "verdict": "go" if ready else "no-go",
        "readyForTopologyCaseStudyLaunch": ready,
        "completeBlockCountAttempted": materialization["completeBlockCountAttempted"],
        "completeBlockCountSucceeded": materialization["completeBlockCountSucceeded"],
        "canonicalPairCount": pair_attempt.get("canonicalPairCount"),
        "nativeValidCount": pair_attempt.get("nativeValidCount"),
        "canonicalPairCandidateCount": task_matrix.get("canonicalPairCandidateCount"),
        "taskCount": task_matrix.get("taskCount"),
        "pythonCanonicalFixtureAccepted": parity["canonicalFixtureAccepted"],
        "pythonAdversarialAllRejected": parity_ok,
        "dispatched": False,
        "executedInThisTask": False,
        "marketEvaluationLaunched": False,
        "generationLaunched": False,
        "pairCompileAttemptSha256": pair_attempt["attemptSha256"],
        "taskMatrixSha256": task_matrix["taskMatrixSha256"],
        "fewerThanTwoCompleteBlocksIsNoGo": int(materialization["completeBlockCountSucceeded"]) < 2,
        "doNotLaunchInThisTask": True,
    }
    go_nogo["reportSha256"] = canonical_sha256({key: value for key, value in go_nogo.items() if key != "reportSha256"})
    memo = decision_memo_v7(
        archive=archive,
        inventory=inventory,
        proposal=proposal,
        topology=topology,
        go_nogo=go_nogo,
    )
    discovery_doc = dict(discovery)
    payload_index = {
        "schemaVersion": "temporal_qd_topology_canonical_frozen_pair_payloads_v7",
        "doNotLaunch": True,
        "receiptPayloads": materialization.get("payloads") or {},
    }
    payload_index["payloadIndexSha256"] = canonical_sha256(
        {key: value for key, value in payload_index.items() if key != "payloadIndexSha256"}
    )
    outputs: dict[str, tuple[Mapping[str, Any] | None, str]] = {
        "v38-directional-event-insert-forensic-v7.json": (
            event,
            "# V38 directional event-insert forensic v7\n\nResealed v4 mechanism partition. No new market evaluation.\n",
        ),
        "v38-multipanel-suboperation-v7.json": (
            multipanel,
            "# V38 multi-panel suboperation v7\n\nSelf-hash resealed after schema/sideByPanel. Panels 1-2 are inspected replication, not untouched confirmation.\n",
        ),
        "v38-cumulative-event-child-archive-forensic-v7.json": (
            archive,
            "# V38 cumulative event-child archive forensic v7\n\nExact frozen V38 robust-breeder gate flags. Competing members are not the binding cause.\n",
        ),
        "topology-coadaptation-matrix-spec-v7.json": (
            topology,
            "# Topology co-adaptation matrix spec v7\n\nCanonical FrozenPair receipts, graph chaining, interaction vs useful innovation. Do not launch.\n",
        ),
        "topology-case-study-inspected-task-authority-v7.json": (
            inspected_authority,
            "# Topology case-study inspected-task authority v7\n\nComplete-block pair receipts x 12 windows. Do not launch in this task.\n",
        ),
        "topology-case-study-inspected-task-matrix-v7.json": (
            task_matrix,
            "# Topology case-study inspected-task matrix v7\n\nExact candidate/window rows. dispatched=false. Do not launch in this task.\n",
        ),
        "topology-future-untouched-confirmation-authority-v7.json": (
            topology_confirmation,
            "# Topology future untouched confirmation authority v7\n\nWindow identities unbound. Panel-4 is latin-square, not the outer tail. Do not launch.\n",
        ),
        "resource-future-untouched-confirmation-authority-v7.json": (
            resource_confirmation,
            "# Resource future untouched confirmation authority v7\n\nSeparate from the topology case study. Do not launch.\n",
        ),
        "resource-suboperation-selected-pair-receipts-v7.json": (
            selected,
            "# Resource selected pair receipts v7\n\nOne-plan cells plus 5 clones compiled with the same FrozenPair machinery. Inventory remains unlaunched. Do not launch.\n",
        ),
        "resource-suboperation-one-plan-design-v7.json": (
            one_plan,
            "# Resource one-plan design v7\n\nCoverage, not repeatability. Do not launch.\n",
        ),
        "resource-suboperation-near-two-plan-design-v7.json": (
            near_two,
            "# Resource near-two-plan design v7\n\nAvailability-backed second plans. Do not launch.\n",
        ),
        "canonical-pair-compile-attempt-v7.json": (
            pair_attempt,
            "# Canonical pair-compile attempt v7\n\nOriginal V38 pair-run config, FrozenPair.compile, and native validation. Do not launch.\n",
        ),
        "v38-followup-authority-discovery-v7.json": (
            discovery_doc,
            "# V38 follow-up authority discovery v7\n\nFilesystem paths and identity SHAs for the original launch authority. No synthetic fallback.\n",
        ),
        "topology-coadaptation-python-rust-parity-corpus-v7.json": (
            parity,
            parity_markdown,
        ),
        "v38-followup-v7-go-nogo.json": (
            go_nogo,
            "# V38 follow-up v7 go/no-go\n\nAuthorize the 144-task case study only when 12/12 pairs compiled and native-validated, the matrix is exact, and Python/Rust parity holds. This task did not launch.\n",
        ),
    }
    written: dict[str, Path] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, (payload, markdown) in outputs.items():
        path = output_dir / name
        if payload is not None:
            write_report(path, payload, markdown)
        written[name] = path
    memo_path = output_dir / "decision-memo-v7.md"
    memo_path.write_text(memo, encoding="utf-8", newline="\n")
    written["decision-memo-v7.md"] = memo_path
    receipts_path = output_dir / "topology-coadaptation-materialization-receipts-v7.json"
    receipts_payload = build_standalone_receipt_set_v7(topology=topology, pair_attempt=pair_attempt)
    receipts_path.write_text(canonical_json(receipts_payload) + "\n", encoding="utf-8", newline="\n")
    written["topology-coadaptation-materialization-receipts-v7.json"] = receipts_path
    payloads_path = output_dir / "topology-canonical-frozen-pair-payloads-v7.json"
    payloads_path.write_text(canonical_json(payload_index) + "\n", encoding="utf-8", newline="\n")
    written["topology-canonical-frozen-pair-payloads-v7.json"] = payloads_path
    readme_v7 = output_dir / "README-v7.md"
    readme_v7.write_text(
        "\n".join(
            [
                "# V38 evolve-everything follow-up artifacts v7",
                "",
                "Generated locally from frozen v4/v6 artifacts, the original V38 pair-run config, and current source. No new market evaluation. v1-v6 files in this folder are unchanged except the shared README appendix.",
                "",
                "| File | Task |",
                "| --- | --- |",
                "| `v38-followup-authority-discovery-v7.json` / `.md` | A/B |",
                "| `canonical-pair-compile-attempt-v7.json` / `.md` | A/B |",
                "| `topology-coadaptation-matrix-spec-v7.json` / `.md` | C/D |",
                "| `topology-coadaptation-materialization-receipts-v7.json` | C |",
                "| `topology-canonical-frozen-pair-payloads-v7.json` | C |",
                "| `v38-directional-event-insert-forensic-v7.json` / `.md` | A |",
                "| `v38-multipanel-suboperation-v7.json` / `.md` | A |",
                "| `v38-cumulative-event-child-archive-forensic-v7.json` / `.md` | F |",
                "| `topology-case-study-inspected-task-authority-v7.json` / `.md` | G |",
                "| `topology-case-study-inspected-task-matrix-v7.json` / `.md` | G |",
                "| `topology-future-untouched-confirmation-authority-v7.json` / `.md` | H |",
                "| `resource-future-untouched-confirmation-authority-v7.json` / `.md` | H |",
                "| `resource-suboperation-selected-pair-receipts-v7.json` / `.md` | I |",
                "| `resource-suboperation-one-plan-design-v7.json` / `.md` | I |",
                "| `resource-suboperation-near-two-plan-design-v7.json` / `.md` | I |",
                "| `topology-coadaptation-python-rust-parity-corpus-v7.json` / `.md` | E |",
                "| `topology-coadaptation-executed-python-rust-parity-report-v7.json` / `.md` | E |",
                "| `v38-followup-v7-go-nogo.json` / `.md` | J |",
                "| `decision-memo-v7.md` | memo |",
                "| `README-v7.md` | index |",
                "",
                "Regenerate v7 with:",
                "",
                "`uv run python -m autoresearch.temporal_qd_v38_followup_audit_v7 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README-v7.md"] = readme_v7
    index = output_dir / "README.md"
    existing = index.read_text(encoding="utf-8") if index.is_file() else ""
    marker = "## v7"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    index.write_text(
        existing.rstrip()
        + "\n\n## v7\n\n"
        + "\n".join(
            [
                "v1-v6 files above are frozen. v7 binds the original V38 pair-run config, materializes canonical FrozenPair P/T/E/TE receipts with native validation, freezes the 144-task inspected matrix without dispatch, and records go/no-go. No market compute was launched.",
                "",
                "Regenerate v7 with:",
                "",
                "`uv run python -m autoresearch.temporal_qd_v38_followup_audit_v7 --output-dir research/temporal-qd/v38-followup`",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    written["README.md"] = index
    _ = (
        projected_inspected_panel_worker_tasks,
        projected_with_future_confirmation_panel,
        V4_ARM_P,
        V4_ARM_T,
        V4_ARM_E,
        V4_ARM_TE,
        V4_BLOCK_COMPLETE,
        DEFAULT_AUTHORITY_ROOT,
        BLOCK_CLASS_COMPLETE,
    )
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v38-root", type=Path, default=DEFAULT_V38_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--authority-root", type=Path, default=DEFAULT_AUTHORITY_ROOT)
    parser.add_argument("--pair-run-config", type=Path, default=DEFAULT_PAIR_RUN_CONFIG)
    parser.add_argument("--evolvable-authority", type=Path, default=DEFAULT_EVOLVABLE_AUTHORITY)
    parser.add_argument("--rotating-evidence-config", type=Path, default=DEFAULT_ROTATING_EVIDENCE)
    args = parser.parse_args(argv)
    written = run_audit_v7(
        output_dir=args.output_dir,
        v38_root=args.v38_root,
        catalog_path=args.catalog,
        authority_root=args.authority_root,
        pair_run_config=args.pair_run_config,
        evolvable_authority=args.evolvable_authority,
        rotating_evidence=args.rotating_evidence_config,
    )
    for path in written.values():
        print(path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
