"""Canonical FrozenPair materialization for V38 follow-up v7.

Uses the original pair-run config, PairAuthorityBundle, evolvable module
authority, FrozenModule native re-admission, and FrozenPair.compile. Does not
dispatch workers or run market evaluation.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evolvable_module_genome import evolvable_resource_fingerprint
from .evolvable_module_qd_authority import open_evolvable_module_pair_authority
from .evolvable_module_topology import apply_plan, make_plan
from .evidence_plan import canonical_json, canonical_sha256
from .temporal_bidirectional_genome import FrozenModule, FrozenPair
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_pair_factory import PairAuthorityBundle, load_pair_run_config
from .temporal_qd_topology_coadaptation_v7 import (
    ARM_E,
    ARM_P,
    ARM_T,
    ARM_TE,
    ARMS,
    BLOCK_CLASS_COMPLETE,
    BLOCK_CLASS_INCOMPLETE,
    PAIR_COMPILE_STATUS_COMPLETE,
    PAIR_COMPILE_STATUS_INCOMPLETE,
    pair_candidate_identity_sha256,
    reconstructed_pair_program_identity_sha256,
    module_compile_artifact_sha256,
)
from .temporal_qd_v38_followup_audit import iter_jsonl
from .temporal_qd_v38_followup_audit_v4 import _event_plan_for_node

MATERIALIZATION_SCHEMA = "temporal_qd_canonical_pair_compile_attempt_v7"
PAYLOAD_INDEX_SCHEMA = "temporal_qd_topology_canonical_frozen_pair_payloads_v7"


def _qd_id(*parts: object) -> str:
    return "qd_" + canonical_sha256({"schemaVersion": "temporal_qd_v38_followup_v7_id", "parts": list(parts)})[7:35]


def _clone(value: Any) -> Any:
    return json.loads(canonical_json(value))


def load_parent_material(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in iter_jsonl(path):
        candidate_id = str(row.get("candidateId") or "")
        if candidate_id and candidate_id not in rows:
            rows[candidate_id] = row
    return rows


def _factory_module_candidate_id(*, seed: str, side: str, genome_sha: str) -> str:
    return "qd_evolvable_module_" + canonical_sha256({"seed": seed, "side": side, "genome": genome_sha})[7:35]


def _factory_pair_candidate_id(*, seed: str) -> str:
    return "qd_evolvable_pair_" + canonical_sha256({"seed": seed})[7:35]


def _freeze_genome(
    *,
    authority: Any,
    genome: Any,
    side: str,
    lineage: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> FrozenModule:
    compiled = authority.compiler.compile(
        genome,
        candidate_id=candidate_id,
        native_validator=authority.bundle.validator,
    )
    report = compiled.get("nativeValidation")
    if not isinstance(report, Mapping):
        raise TemporalDiscoveryContractError("native module validation report is missing")
    return FrozenModule.freeze(
        program=genome.canonical(),
        profile=compiled["profile"],
        grammar_context=authority.grammar_context(side),
        catalog=authority.catalog_identity(side),
        policy=authority.module_policy(side),
        native_authority=authority.bundle.native_identity,
        native_report=report,
        lineage=_clone(list(lineage)),
    )


def _compile_pair(
    *,
    authority: Any,
    long: FrozenModule,
    short: FrozenModule,
    candidate_id: str,
    lineage: Sequence[Mapping[str, Any]],
) -> FrozenPair:
    return FrozenPair.compile(
        long=long,
        short=short,
        pair_compiler_identity=authority.bundle.compiler_identity,
        pair_compiler=authority.bundle.compiler,
        candidate_id=candidate_id,
        side_targeted_lineage=_clone(list(lineage)),
        native_validator=authority.bundle.validator,
    )


def reconstruct_parent_pair(*, authority: Any, parent_row: Mapping[str, Any]) -> dict[str, Any]:
    payload = parent_row["pairPayload"]
    delta = payload["proposalDelta"]
    long_lineage = list(payload["longModuleLineage"])
    short_lineage = list(payload["shortModuleLineage"])
    pair_lineage = list(payload["sideTargetedLineage"])
    seed = str(long_lineage[0]["proposalSeed"])
    long_g = authority.decode_program(delta["longProgram"])
    short_g = authority.decode_program(delta["shortProgram"])
    if long_g.identity_sha256 != delta["longProgramSha256"] or short_g.identity_sha256 != delta["shortProgramSha256"]:
        raise TemporalDiscoveryContractError("parent program SHA drifted during FrozenPair reconstruction")
    long_m = _freeze_genome(
        authority=authority,
        genome=long_g,
        side="long",
        lineage=long_lineage,
        candidate_id=_factory_module_candidate_id(seed=seed, side="long", genome_sha=long_g.identity_sha256),
    )
    short_m = _freeze_genome(
        authority=authority,
        genome=short_g,
        side="short",
        lineage=short_lineage,
        candidate_id=_factory_module_candidate_id(seed=seed, side="short", genome_sha=short_g.identity_sha256),
    )
    pair = _compile_pair(
        authority=authority,
        long=long_m,
        short=short_m,
        candidate_id=_factory_pair_candidate_id(seed=seed),
        lineage=pair_lineage,
    )
    FrozenPair.from_payload(pair.canonical_payload())
    accepted = payload.get("acceptedRecord") if isinstance(payload.get("acceptedRecord"), Mapping) else {}
    compiled = accepted.get("compiled") if isinstance(accepted.get("compiled"), Mapping) else {}
    return {
        "pair": pair,
        "historicalPairIdentitySha256": parent_row.get("pairIdentitySha256"),
        "compiledV3MatchesAcceptedRecord": (
            pair.raw_pair_sha256 == compiled.get("rawPairSha256")
            and pair.profile_sha256 == compiled.get("profileSnapshotSha256")
            and pair.native_program_sha256 == compiled.get("programSha256")
        ),
        "historicalFrozenPairIdentityMatched": pair.identity_sha256 == parent_row.get("pairIdentitySha256"),
    }


def _module_for_side(pair: FrozenPair, side: str) -> FrozenModule:
    return pair.long if side == "long" else pair.short


def _replace_side(pair: FrozenPair, side: str, changed: FrozenModule) -> tuple[FrozenModule, FrozenModule]:
    if side == "long":
        return changed, pair.short
    return pair.long, changed


def _apply_topology(*, authority: Any, module: FrozenModule, plan_record: Mapping[str, Any], candidate_id: str) -> tuple[FrozenModule, dict[str, Any]]:
    genome = authority.decode_module(module)
    raw = plan_record["topologyPlan"]
    topology = make_plan(genome, operation=str(raw["operation"]), **dict(raw["arguments"]))
    if topology.identity_sha256 != plan_record["planSha256"] or topology.canonical() != raw:
        raise TemporalDiscoveryContractError("topology plan identity drifted from the frozen v6/v4 record")
    applied = apply_plan(genome, topology)
    lineage = [*_clone(module.canonical_payload()["lineage"]), {
        "operation": "evolvable_topology",
        "side": module.direction,
        "plan": topology.canonical(),
        "planSha256": topology.identity_sha256,
        "application": applied.delta.canonical(),
    }]
    frozen = _freeze_genome(
        authority=authority,
        genome=applied.genome,
        side=module.direction,
        lineage=lineage,
        candidate_id=candidate_id,
    )
    audit = {
        "schemaVersion": "temporal_qd_evolvable_topology_audit_v1",
        "parentModuleIdentitySha256": module.identity_sha256,
        "childModuleIdentitySha256": frozen.identity_sha256,
        "topologyPlanSha256": topology.identity_sha256,
        "topologyDelta": applied.delta.canonical(),
        "nativeValidationReportSha256": frozen.native_validation_report_sha256,
    }
    audit["auditSha256"] = canonical_sha256(audit)
    return frozen, {"topology": audit, "delta": applied.delta.canonical(), "genome": applied.genome}


def _apply_event(
    *,
    authority: Any,
    module: FrozenModule,
    primitive: Mapping[str, Any],
    node_id: str,
    candidate_id: str,
) -> tuple[FrozenModule, dict[str, Any]]:
    genome = authority.decode_module(module)
    plan = _event_plan_for_node(
        authority.resource_layer,
        genome,
        indicator_id=str(primitive["indicatorId"]),
        node_id=str(node_id),
    )
    if plan is None:
        raise TemporalDiscoveryContractError("event primitive is not applicable at the declared node")
    child, application = authority.resource_layer.apply(genome, plan)
    lineage = [*_clone(module.canonical_payload()["lineage"]), {
        "operation": "evolvable_resource",
        "side": module.direction,
        "plan": _clone(plan),
        "planSha256": plan["planSha256"],
        "application": _clone(application),
    }]
    frozen = _freeze_genome(
        authority=authority,
        genome=child,
        side=module.direction,
        lineage=lineage,
        candidate_id=candidate_id,
    )
    return frozen, application


def _compiled_identities(pair: FrozenPair) -> dict[str, Any]:
    payload = pair.canonical_payload()
    identities = {
        "frozenPairIdentitySha256": pair.identity_sha256,
        "frozenPairPayloadSha256": canonical_sha256(payload),
        "pairProfileSha256": pair.profile_sha256,
        "normalizedProfileSnapshotSha256": pair.profile_sha256,
        "rawPairSha256": pair.raw_pair_sha256,
        "programSha256": pair.native_program_sha256,
        "validationReportSha256": pair.native_validation_report_sha256,
        "pairCompilerAuthoritySha256": pair.pair_compiler.sha256,
        "nativeAuthoritySha256": pair.long.native_authority.sha256,
        "longModuleIdentitySha256": pair.long.identity_sha256,
        "shortModuleIdentitySha256": pair.short.identity_sha256,
        "longNativeValidationReportSha256": pair.long.native_validation_report_sha256,
        "shortNativeValidationReportSha256": pair.short.native_validation_report_sha256,
    }
    if identities["nativeAuthoritySha256"] != pair.short.native_authority.sha256:
        raise TemporalDiscoveryContractError("long/short native authority identity drifted")
    return identities


def _receipt_from_pair(
    *,
    authority: Any,
    base: Mapping[str, Any],
    pair: FrozenPair,
    side: str,
    arm: str,
    parent_candidate_id: str,
    historical_pair_identity: str | None,
    compiled_v3_matches_parent: bool | None,
    topology_delta: Mapping[str, Any] | None,
    event_audit: Mapping[str, Any] | None,
    failure: str | None = None,
) -> dict[str, Any]:
    row = dict(base)
    changed = _module_for_side(pair, side)
    opposite = pair.short if side == "long" else pair.long
    decoded = authority.decode_module(changed)
    identities = _compiled_identities(pair)
    long_sha = pair.long.program_sha256
    short_sha = pair.short.program_sha256
    pair_id = reconstructed_pair_program_identity_sha256(
        parent_candidate_id=parent_candidate_id,
        long_program_sha256=long_sha,
        short_program_sha256=short_sha,
    )
    row.update(
        {
            "changedSideGenomeSha256": decoded.identity_sha256,
            "changedSideProgramSha256": changed.program_sha256,
            "changedSideProfileSha256": changed.profile_sha256,
            "topologySignature": decoded.semantic_topology_signature(),
            "resourceFingerprint": evolvable_resource_fingerprint(decoded),
            "longProgramSha256": long_sha,
            "shortProgramSha256": short_sha,
            "unchangedOppositeProgramSha256": opposite.program_sha256,
            "unchangedOppositeProgramPreserved": True,
            "reconstructedPairProgramIdentitySha256": pair_id,
            "frozenPairIdentitySha256": identities["frozenPairIdentitySha256"],
            "pairCandidateIdentitySha256": pair_candidate_identity_sha256(
                parent_candidate_id=parent_candidate_id,
                arm=arm,
                side=side,
                reconstructed_pair_program_identity_sha256=pair_id,
            ),
            "pairProfileSha256": identities["pairProfileSha256"],
            "normalizedProfileSnapshotSha256": identities["normalizedProfileSnapshotSha256"],
            "moduleCompilerPolicySha256": authority.compiler.policy.sha256,
            "moduleCompileArtifactSha256": module_compile_artifact_sha256(
                long_program_sha256=long_sha,
                short_program_sha256=short_sha,
                changed_side_profile_sha256=changed.profile_sha256,
            ),
            "canonicalPairCompilerAuthoritySha256": identities["pairCompilerAuthoritySha256"],
            "canonicalPairCompileReportSha256": identities["validationReportSha256"],
            "nativeValidationRan": True,
            "nativeValidationAuthoritySha256": identities["nativeAuthoritySha256"],
            "nativeValidationReportSha256": identities["validationReportSha256"],
            "pairCompileStatus": PAIR_COMPILE_STATUS_COMPLETE,
            "canonicalCompiledIdentities": identities,
            "historicalParentPairIdentitySha256": historical_pair_identity if arm == ARM_P else None,
            "compiledV3MatchesHistoricalParent": compiled_v3_matches_parent if arm == ARM_P else None,
            "frozenPairPayloadSha256": identities["frozenPairPayloadSha256"],
            "topologySemanticDelta": _clone(topology_delta) if topology_delta is not None else row.get("topologySemanticDelta"),
            "operatorApplicationAudit": _clone(event_audit) if event_audit is not None else row.get("operatorApplicationAudit"),
            "failureReason": failure,
        }
    )
    return row


def _mark_block_ineligible(
    *,
    block: Mapping[str, Any],
    slots: Mapping[tuple[str, str], Mapping[str, Any]],
    receipts_by_id: dict[str, dict[str, Any]],
    updated_blocks: dict[str, dict[str, Any]],
    updated_slots: dict[str, dict[str, Any]],
    error: BaseException,
) -> None:
    reason = f"canonical FrozenPair.compile/native validation failed: {type(error).__name__}: {error}"
    row = updated_blocks[str(block["blockId"])]
    row["classification"] = BLOCK_CLASS_INCOMPLETE
    row["excludedFromPrimaryCoadaptationCalculation"] = True
    row["incompletenessReason"] = reason
    for arm in ARMS:
        slot = slots[(block["blockId"], arm)]
        slot_row = updated_slots[slot["slotId"]]
        slot_row["eligibility"] = "ineligible"
        slot_row["ineligibilityReason"] = reason
        receipt = receipts_by_id[slot["slotId"]]
        receipt["eligibility"] = "ineligible"
        receipt["pairCompileStatus"] = PAIR_COMPILE_STATUS_INCOMPLETE
        receipt["nativeValidationRan"] = False
        receipt["canonicalCompiledIdentities"] = None
        receipt["frozenPairIdentitySha256"] = None
        receipt["frozenPairPayloadSha256"] = None
        receipt["compiledV3MatchesHistoricalParent"] = None
        receipt["historicalParentPairIdentitySha256"] = None
        receipt["failureReason"] = reason


def materialize_complete_blocks_v7(
    *,
    topology: Mapping[str, Any],
    parent_material: Mapping[str, Mapping[str, Any]],
    authority: Any,
) -> dict[str, Any]:
    receipts_by_id = {row["receiptId"]: dict(row) for row in topology["materializationReceipts"]}
    plans = {row["planId"]: row for row in topology["topologyPlans"]}
    primitives = {row["primitiveId"]: row for row in topology["eventPrimitives"]}
    slots = {(row["blockId"], row["arm"]): row for row in topology["slots"]}
    updated_blocks = {row["blockId"]: dict(row) for row in topology["blocks"]}
    updated_slots = {row["slotId"]: dict(row) for row in topology["slots"]}
    payloads: dict[str, Mapping[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    compiled_count = 0
    native_valid_count = 0
    parent_cache: dict[str, dict[str, Any]] = {}

    complete_blocks = [block for block in topology["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE]
    for block in complete_blocks:
        parent_id = str(block["parentCandidateId"])
        side = str(block["side"])
        try:
            if parent_id not in parent_cache:
                parent_cache[parent_id] = reconstruct_parent_pair(
                    authority=authority,
                    parent_row=parent_material[parent_id],
                )
            reconstructed = parent_cache[parent_id]
            parent_pair: FrozenPair = reconstructed["pair"]
            if reconstructed["compiledV3MatchesAcceptedRecord"] is not True:
                raise TemporalDiscoveryContractError(
                    "P compiled v3 identities do not match the historical parent accepted record"
                )
            plan = plans.get(str(block.get("topologyPlanId") or ""))
            primitive = primitives.get(str(block.get("eventPrimitiveId") or ""))
            if plan is None or primitive is None:
                raise TemporalDiscoveryContractError("complete block is missing its frozen topology plan or event primitive")

            parent_module = _module_for_side(parent_pair, side)
            t_module, t_info = _apply_topology(
                authority=authority,
                module=parent_module,
                plan_record=plan,
                candidate_id=_qd_id("T", parent_id, side),
            )
            e_module, e_audit = _apply_event(
                authority=authority,
                module=parent_module,
                primitive=primitive,
                node_id=str(slots[(block["blockId"], ARM_E)]["settlingNodeId"]),
                candidate_id=_qd_id("E", parent_id, side),
            )
            te_site = str(plan["addedSetupNodeId"])
            te_module, te_audit = _apply_event(
                authority=authority,
                module=t_module,
                primitive=primitive,
                node_id=te_site,
                candidate_id=_qd_id("TE", parent_id, side),
            )
            opposite = parent_pair.short if side == "long" else parent_pair.long
            arms = {
                ARM_P: parent_pair,
                ARM_T: _compile_pair(
                    authority=authority,
                    long=t_module if side == "long" else opposite,
                    short=opposite if side == "long" else t_module,
                    candidate_id=_qd_id("pair", ARM_T, parent_id, side),
                    lineage=[*_clone(parent_pair.canonical_payload()["sideTargetedLineage"]), {"operation": "evolvable_topology", "side": side, "planSha256": plan["planSha256"]}],
                ),
                ARM_E: _compile_pair(
                    authority=authority,
                    long=e_module if side == "long" else opposite,
                    short=opposite if side == "long" else e_module,
                    candidate_id=_qd_id("pair", ARM_E, parent_id, side),
                    lineage=[*_clone(parent_pair.canonical_payload()["sideTargetedLineage"]), {"operation": "evolvable_resource", "side": side, "planSha256": e_audit["planSha256"]}],
                ),
                ARM_TE: _compile_pair(
                    authority=authority,
                    long=te_module if side == "long" else opposite,
                    short=opposite if side == "long" else te_module,
                    candidate_id=_qd_id("pair", ARM_TE, parent_id, side),
                    lineage=[
                        *_clone(parent_pair.canonical_payload()["sideTargetedLineage"]),
                        {"operation": "evolvable_topology", "side": side, "planSha256": plan["planSha256"]},
                        {"operation": "evolvable_resource", "side": side, "planSha256": te_audit["planSha256"]},
                    ],
                ),
            }
            for arm, pair in arms.items():
                FrozenPair.from_payload(pair.canonical_payload())
                slot = slots[(block["blockId"], arm)]
                base = receipts_by_id[slot["slotId"]]
                topology_delta = t_info["delta"] if arm in {ARM_T, ARM_TE} else None
                if arm == ARM_E:
                    event_audit = e_audit
                elif arm == ARM_TE:
                    event_audit = te_audit
                elif arm == ARM_P:
                    event_audit = {
                        "arm": ARM_P,
                        "productionArchiveWrite": False,
                        "replayed": True,
                    }
                else:
                    event_audit = {
                        "arm": ARM_T,
                        "productionArchiveWrite": False,
                        "replayed": True,
                    }
                receipt = _receipt_from_pair(
                    authority=authority,
                    base=base,
                    pair=pair,
                    side=side,
                    arm=arm,
                    parent_candidate_id=parent_id,
                    historical_pair_identity=reconstructed["historicalPairIdentitySha256"],
                    compiled_v3_matches_parent=reconstructed["compiledV3MatchesAcceptedRecord"],
                    topology_delta=topology_delta,
                    event_audit=event_audit,
                )
                if arm == ARM_P:
                    receipt["applicationParentGenomeSha256"] = receipt["changedSideGenomeSha256"]
                    receipt["applicationChildGenomeSha256"] = receipt["changedSideGenomeSha256"]
                elif arm == ARM_T:
                    receipt["applicationParentGenomeSha256"] = t_info["delta"]["beforeGenomeSha256"]
                    receipt["applicationChildGenomeSha256"] = t_info["delta"]["afterGenomeSha256"]
                elif arm == ARM_E:
                    receipt["applicationParentGenomeSha256"] = e_audit["parentGenomeSha256"]
                    receipt["applicationChildGenomeSha256"] = e_audit["childGenomeSha256"]
                else:
                    receipt["applicationParentGenomeSha256"] = te_audit["parentGenomeSha256"]
                    receipt["applicationChildGenomeSha256"] = te_audit["childGenomeSha256"]
                    receipt["eventAttachesToAddedSetupNode"] = (
                        (_event_node_from_audit(te_audit) == te_site)
                    )
                receipts_by_id[slot["slotId"]] = receipt
                payloads[slot["slotId"]] = pair.canonical_payload()
                compiled_count += 1
                native_valid_count += 1
                opposite_pair_module = pair.short if side == "long" else pair.long
                parent_opposite = parent_pair.short if side == "long" else parent_pair.long
                if opposite_pair_module.canonical_payload() != parent_opposite.canonical_payload():
                    raise TemporalDiscoveryContractError("opposite FrozenModule was not preserved byte-for-byte")
        except Exception as exc:
            failures.append(
                {
                    "blockId": block["blockId"],
                    "parentCandidateId": parent_id,
                    "side": side,
                    "errorType": type(exc).__name__,
                    "error": str(exc),
                }
            )
            _mark_block_ineligible(
                block=block,
                slots=slots,
                receipts_by_id=receipts_by_id,
                updated_blocks=updated_blocks,
                updated_slots=updated_slots,
                error=exc,
            )
            for arm in ARMS:
                payloads.pop(slots[(block["blockId"], arm)]["slotId"], None)

    ordered = [receipts_by_id[row["receiptId"]] for row in topology["materializationReceipts"]]
    succeeded = len(complete_blocks) - len({row["blockId"] for row in failures})
    return {
        "receipts": ordered,
        "payloads": payloads,
        "failures": failures,
        "canonicalPairCount": compiled_count,
        "nativeValidCount": native_valid_count,
        "completeBlockCountAttempted": len(complete_blocks),
        "completeBlockCountSucceeded": succeeded,
        "parentCache": parent_cache,
        "updatedBlocks": [updated_blocks[row["blockId"]] for row in topology["blocks"]],
        "updatedSlots": [updated_slots[row["slotId"]] for row in topology["slots"]],
    }


def _event_node_from_audit(audit: Mapping[str, Any] | None) -> str | None:
    if not isinstance(audit, Mapping):
        return None
    delta = audit.get("semanticDelta")
    if not isinstance(delta, list) or not delta or not isinstance(delta[0], Mapping):
        return None
    node_id = delta[0].get("nodeId")
    return str(node_id) if isinstance(node_id, str) and node_id else None


def _apply_resource_plan(
    *,
    authority: Any,
    module: FrozenModule,
    plan_sha256: str,
    candidate_id: str,
) -> tuple[FrozenModule, dict[str, Any]]:
    genome = authority.decode_module(module)
    match = next((plan for plan in authority.resource_layer.enumerate_plans(genome) if plan.get("planSha256") == plan_sha256), None)
    if match is None:
        raise TemporalDiscoveryContractError(f"resource plan {plan_sha256} is not applicable on the reconstructed parent")
    child, application = authority.resource_layer.apply(genome, match)
    lineage = [*_clone(module.canonical_payload()["lineage"]), {
        "operation": "evolvable_resource",
        "side": module.direction,
        "plan": _clone(match),
        "planSha256": match["planSha256"],
        "application": _clone(application),
    }]
    frozen = _freeze_genome(
        authority=authority,
        genome=child,
        side=module.direction,
        lineage=lineage,
        candidate_id=candidate_id,
    )
    return frozen, application


def materialize_resource_selected_v7(
    *,
    inventory_v2: Mapping[str, Any],
    one_plan_cells: Sequence[Mapping[str, Any]],
    parent_material: Mapping[str, Mapping[str, Any]],
    authority: Any,
    parent_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cache = dict(parent_cache or {})
    selected = [dict(cell) for cell in one_plan_cells if cell.get("status") == "filled"]
    mutation_receipts: list[dict[str, Any]] = []
    clone_receipts: list[dict[str, Any]] = []
    payloads: dict[str, Mapping[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    compiled = 0
    native = 0

    def parent_pair_for(parent_id: str) -> FrozenPair:
        if parent_id not in cache:
            cache[parent_id] = reconstruct_parent_pair(authority=authority, parent_row=parent_material[parent_id])
        return cache[parent_id]["pair"]

    for cell in selected:
        parent_id = str(cell["parentCandidateId"])
        side = str(cell["side"])
        plan_sha = str(cell["selectedPlanSha256"])
        try:
            parent_pair = parent_pair_for(parent_id)
            module = _module_for_side(parent_pair, side)
            changed, application = _apply_resource_plan(
                authority=authority,
                module=module,
                plan_sha256=plan_sha,
                candidate_id=_qd_id("resource", parent_id, side, plan_sha),
            )
            opposite = parent_pair.short if side == "long" else parent_pair.long
            pair = _compile_pair(
                authority=authority,
                long=changed if side == "long" else opposite,
                short=opposite if side == "long" else changed,
                candidate_id=_qd_id("resource-pair", parent_id, side, plan_sha),
                lineage=[*_clone(parent_pair.canonical_payload()["sideTargetedLineage"]), {"operation": "evolvable_resource", "side": side, "planSha256": plan_sha}],
            )
            FrozenPair.from_payload(pair.canonical_payload())
            identities = _compiled_identities(pair)
            mutation_receipts.append(
                {
                    "parentCandidateId": parent_id,
                    "side": side,
                    "lane": cell["lane"],
                    "planSha256": plan_sha,
                    "kind": "selected_one_plan_mutation_pair",
                    "canonicalPairCompileRan": True,
                    "nativeValidationRan": True,
                    "frozenPairIdentitySha256": identities["frozenPairIdentitySha256"],
                    "frozenPairPayloadSha256": identities["frozenPairPayloadSha256"],
                    "applicationSha256": application.get("applicationSha256"),
                    "launchAuthorityIdentityKind": "canonical_frozen_pair",
                    "productionArchiveWrite": False,
                    "doNotLaunch": True,
                }
            )
            payloads[f"{parent_id}|{side}|{cell['lane']}|{plan_sha}"] = pair.canonical_payload()
            compiled += 1
            native += 1
        except Exception as exc:
            failures.append(
                {
                    "kind": "selected_one_plan_mutation_pair",
                    "parentCandidateId": parent_id,
                    "side": side,
                    "lane": cell.get("lane"),
                    "planSha256": plan_sha,
                    "errorType": type(exc).__name__,
                    "error": str(exc),
                }
            )
            mutation_receipts.append(
                {
                    "parentCandidateId": parent_id,
                    "side": side,
                    "lane": cell["lane"],
                    "planSha256": plan_sha,
                    "kind": "selected_one_plan_mutation_pair",
                    "canonicalPairCompileRan": False,
                    "nativeValidationRan": False,
                    "frozenPairIdentitySha256": None,
                    "failureReason": f"{type(exc).__name__}: {exc}",
                    "launchAuthorityIdentityKind": "partial_or_unavailable_canonical_frozen_pair",
                    "doNotLaunch": True,
                }
            )

    for clone in inventory_v2["pairCloneSlots"]:
        parent_id = str(clone["parentCandidateId"])
        try:
            reconstructed = cache.get(parent_id) or reconstruct_parent_pair(
                authority=authority,
                parent_row=parent_material[parent_id],
            )
            cache[parent_id] = reconstructed
            pair: FrozenPair = reconstructed["pair"]
            FrozenPair.from_payload(pair.canonical_payload())
            if pair.long.program_sha256 != clone["parentLongProgramSha256"] or pair.short.program_sha256 != clone["parentShortProgramSha256"]:
                raise TemporalDiscoveryContractError("pair clone long/short program SHA drifted from frozen parent")
            identities = _compiled_identities(pair)
            clone_receipts.append(
                {
                    **dict(clone),
                    "matchesFrozenParentLongShortProgramShas": True,
                    "canonicalPairCompileRan": True,
                    "nativeValidationRan": True,
                    "frozenPairIdentitySha256": identities["frozenPairIdentitySha256"],
                    "frozenPairPayloadSha256": identities["frozenPairPayloadSha256"],
                    "compiledV3MatchesHistoricalParent": reconstructed["compiledV3MatchesAcceptedRecord"],
                    "historicalParentPairIdentitySha256": reconstructed["historicalPairIdentitySha256"],
                    "launchAuthorityIdentityKind": "canonical_frozen_pair",
                    "doNotLaunch": True,
                }
            )
            payloads[str(clone["slotId"])] = pair.canonical_payload()
            compiled += 1
            native += 1
        except Exception as exc:
            failures.append(
                {
                    "kind": "pair_clone",
                    "parentCandidateId": parent_id,
                    "slotId": clone.get("slotId"),
                    "errorType": type(exc).__name__,
                    "error": str(exc),
                }
            )
            clone_receipts.append(
                {
                    **dict(clone),
                    "canonicalPairCompileRan": False,
                    "nativeValidationRan": False,
                    "frozenPairIdentitySha256": None,
                    "failureReason": f"{type(exc).__name__}: {exc}",
                    "doNotLaunch": True,
                }
            )

    return {
        "mutationReceipts": mutation_receipts,
        "cloneReceipts": clone_receipts,
        "payloads": payloads,
        "failures": failures,
        "canonicalPairCount": compiled,
        "nativeValidCount": native,
        "parentCache": cache,
    }


def fail_closed_without_pair_authority_v7(topology: Mapping[str, Any], *, error: str) -> dict[str, Any]:
    receipts_by_id = {row["receiptId"]: dict(row) for row in topology["materializationReceipts"]}
    slots = {(row["blockId"], row["arm"]): row for row in topology["slots"]}
    updated_blocks = {row["blockId"]: dict(row) for row in topology["blocks"]}
    updated_slots = {row["slotId"]: dict(row) for row in topology["slots"]}
    failures = []
    for block in topology["blocks"]:
        if block["classification"] != BLOCK_CLASS_COMPLETE:
            continue
        exc = TemporalDiscoveryContractError(error)
        failures.append(
            {
                "blockId": block["blockId"],
                "parentCandidateId": block["parentCandidateId"],
                "side": block["side"],
                "errorType": "TemporalDiscoveryContractError",
                "error": error,
            }
        )
        _mark_block_ineligible(
            block=block,
            slots=slots,
            receipts_by_id=receipts_by_id,
            updated_blocks=updated_blocks,
            updated_slots=updated_slots,
            error=exc,
        )
    return {
        "receipts": [receipts_by_id[row["receiptId"]] for row in topology["materializationReceipts"]],
        "payloads": {},
        "failures": failures,
        "canonicalPairCount": 0,
        "nativeValidCount": 0,
        "completeBlockCountAttempted": len(failures),
        "completeBlockCountSucceeded": 0,
        "parentCache": {},
        "updatedBlocks": [updated_blocks[row["blockId"]] for row in topology["blocks"]],
        "updatedSlots": [updated_slots[row["slotId"]] for row in topology["slots"]],
    }


def open_v38_pair_authority(*, pair_run_config: Mapping[str, Any], evolvable_authority: Mapping[str, Any]) -> tuple[PairAuthorityBundle, Any]:
    loaded = load_pair_run_config(pair_run_config)
    bundle = PairAuthorityBundle(loaded)
    evolvable = open_evolvable_module_pair_authority(bundle=bundle, config=evolvable_authority)
    return bundle, evolvable


def build_pair_compile_attempt_v7(
    *,
    discovery: Mapping[str, Any],
    materialization: Mapping[str, Any] | None,
    pair_run_config_sha256: str | None,
    compiler_authority_sha256: str | None,
    native_authority_sha256: str | None,
    error: str | None = None,
) -> dict[str, Any]:
    compiled = int(materialization["canonicalPairCount"]) if materialization else 0
    native = int(materialization["nativeValidCount"]) if materialization else 0
    body = {
        "schemaVersion": MATERIALIZATION_SCHEMA,
        "pairCompilerAvailable": compiler_authority_sha256 is not None and error is None,
        "nativeValidatorAvailable": native_authority_sha256 is not None and error is None,
        "frozenPairRunConfigBound": pair_run_config_sha256 is not None and discovery.get("pairRunConfigShaMatchesExpected") is True,
        "frozenPairCompileRan": compiled > 0,
        "nativeValidationRan": native > 0,
        "dashboardOwnedPairCompilerInvoked": compiled > 0,
        "pairRunConfigSha256": pair_run_config_sha256,
        "pairCompilerAuthoritySha256": compiler_authority_sha256,
        "nativeAuthoritySha256": native_authority_sha256,
        "canonicalPairCount": compiled,
        "nativeValidCount": native,
        "failures": list((materialization or {}).get("failures") or []),
        "discoverySha256": discovery.get("discoverySha256"),
        "pathsChecked": discovery.get("checkedPaths"),
        "unavailableReason": error,
        "launchAuthorityIdentityKind": (
            "canonical_frozen_pair"
            if compiled == 12 and native == 12
            else "partial_or_unavailable_canonical_frozen_pair"
        ),
        "syntheticPairCandidateIdentitiesRemainNonLaunch": compiled != 12,
        "doNotLaunch": True,
        "marketEvaluationLaunched": False,
        "generationLaunched": False,
    }
    body["attemptSha256"] = canonical_sha256(body)
    return body
