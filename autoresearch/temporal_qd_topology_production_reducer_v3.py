"""V2.5 authenticated reducer using the exact TE-only production policy."""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_panel_usefulness_v2 import (
    evaluate_panel_usefulness_v2,
    evaluate_replication_survival_v3,
    arm_eligibility,
    verify_archive_policy_authority,
)
from .temporal_qd_topology_production_reducer_v2 import (
    ARMS,
    PANELS,
    REPLICATION_RULE_SHA256,
    SCIENTIFIC_SHA256,
    ProductionReducerError,
    _candidate_identity,
    _changed_side,
    _identity_projection,
    _load_object,
    _mechanism as _v2_4_mechanism,
    _unique_by,
    _validate_panel,
    _verify_self_hash,
)

SCHEMA = "temporal_qd_topology_production_analysis_v3"
CONTRACT_SCHEMA = "temporal_qd_topology_production_reducer_contract_v3"
GRAPH_SCHEMA = "temporal_qd_v5_authenticated_campaign_output_graph_v2"
POLICY_SCHEMA = "temporal_qd_topology_panel_usefulness_policy_v2"


def _open_graph(checkpoint: Path, opener: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(opener), "--campaign-output-checkpoint", str(checkpoint.resolve())],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode:
        detail = completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else "unknown opener failure"
        raise ProductionReducerError(f"production graph authentication failed: {detail}")
    value = json.loads(completed.stdout)
    if not isinstance(value, dict) or value.get("schemaVersion") != GRAPH_SCHEMA:
        raise ProductionReducerError("production opener returned an incompatible V2 graph")
    _verify_self_hash(value, "authenticatedGraphSha256", "authenticated graph")
    return value


def _verify_panel_policy(value: Mapping[str, Any]) -> None:
    _verify_self_hash(value, "panelUsefulnessPolicySha256", "panel usefulness policy")
    expected_keys = {
        "schemaVersion",
        "originalScientificContractSha256",
        "originalReplicationRuleSha256",
        "archivePolicyName",
        "archivePolicySha256",
        "directionSelectionPolicySha256",
        "supportThresholds",
        "qualityLane",
        "eligibilityGateScope",
        "controlEligibilityDisposition",
        "panelLocalPredicate",
        "economicComparisons",
        "riskComparisons",
        "crossPanelRule",
        "untouchedConfirmation",
        "dispatchEnabled",
        "panelUsefulnessPolicySha256",
    }
    if set(value) != expected_keys:
        raise ProductionReducerError("panel usefulness policy fields drifted")
    if (
        value.get("schemaVersion") != POLICY_SCHEMA
        or value.get("originalScientificContractSha256") != SCIENTIFIC_SHA256
        or value.get("originalReplicationRuleSha256") != REPLICATION_RULE_SHA256
        or value.get("archivePolicyName") != "stage5e7_v5_direction_aware_breeding_archive"
        or value.get("archivePolicySha256")
        != "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da"
        or value.get("directionSelectionPolicySha256")
        != "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1"
        or value.get("supportThresholds")
        != {"minimumTotalTrades": 8, "minimumTradesPerWindow": 4}
        or value.get("qualityLane") != "finite_support_and_nonnegative_robust_return"
        or value.get("eligibilityGateScope") != "TE_only"
        or value.get("controlEligibilityDisposition") != "diagnostic_nonveto"
        or value.get("panelLocalPredicate") != "U_v2"
        or value.get("crossPanelRule") != "panel-3_AND_panel-1_AND_panel-2"
        or value.get("untouchedConfirmation") != "pending_same_U_v2_exact_block"
        or value.get("dispatchEnabled") is not False
    ):
        raise ProductionReducerError("panel usefulness policy material drifted")


def _mechanism_v2_5(
    *,
    panel_id: str,
    block_id: str,
    side: str,
    arms: Mapping[str, Mapping[str, Any]],
    members: Mapping[str, Mapping[str, Any]],
    bundles: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    mechanism = _v2_4_mechanism(
        panel_id=panel_id,
        block_id=block_id,
        side=side,
        arms=arms,
        members=members,
        bundles=bundles,
    )
    for arm in ARMS:
        row = mechanism[arm]
        row["closedTradeCountChangeVersusP"] = row.pop(
            "changedTradeOpportunityCountVersusP"
        )
        row.pop("tradeCountChangeVersusP", None)
        row["entrySequenceComparison"] = row.pop("entryTimingShiftVersusP")
        row.pop("routeEventActivation", None)
        member = members[arms[arm]["candidateId"]]
        changed_side = member["aggregate"]["realizedBehavior"]["sides"][side]
        transition_distribution = changed_side.get("transitionDistribution")
        if not isinstance(transition_distribution, Mapping):
            raise ProductionReducerError("changed-side transition distribution is unavailable")
        row["changedSideTransitionDistribution"] = {
            "status": "available",
            "values": dict(transition_distribution),
            "bindingSha256": row["bindings"]["bindingSha256"],
        }
        row["eventSpecificActivation"] = {
            "status": "unavailable",
            "reason": "retained evidence does not bind event-guard evaluations or event-specific transition identities",
            "bindingSha256": row["bindings"]["bindingSha256"],
        }
    return mechanism


def reduce_authenticated_graphs_v3(
    *,
    graphs: Iterable[Mapping[str, Any]],
    launch_control: Mapping[str, Any],
    task_mapping: Mapping[str, Any],
    replication_rule: Mapping[str, Any],
    scientific_contract: Mapping[str, Any],
    analyzer_contract: Mapping[str, Any],
    panel_policy: Mapping[str, Any],
) -> dict[str, Any]:
    _verify_panel_policy(panel_policy)
    for value, field, label in (
        (launch_control, "launchControlSha256", "launch control"),
        (task_mapping, "mappingSha256", "task mapping"),
        (replication_rule, "replicationRuleSha256", "replication rule"),
        (scientific_contract, "scientificContractSha256", "scientific contract"),
        (analyzer_contract, "analyzerContractSha256", "analyzer contract"),
    ):
        _verify_self_hash(value, field, label)
    if scientific_contract.get("scientificContractSha256") != SCIENTIFIC_SHA256:
        raise ProductionReducerError("scientific contract is not the frozen V2.3 authority")
    if replication_rule.get("replicationRuleSha256") != REPLICATION_RULE_SHA256:
        raise ProductionReducerError("replication rule is not the frozen V2.3 authority")
    if (
        analyzer_contract.get("scientificContractSha256") != SCIENTIFIC_SHA256
        or analyzer_contract.get("replicationRuleSha256") != REPLICATION_RULE_SHA256
    ):
        raise ProductionReducerError("analyzer contract is not bound to frozen science/rule")
    if launch_control.get("dispatchEnabled") is not False or launch_control.get("totalInspectedTaskCount") != 144:
        raise ProductionReducerError("launch control safety/count binding drifted")

    graph_list = list(graphs)
    for graph in graph_list:
        _verify_self_hash(graph, "authenticatedGraphSha256", "authenticated graph")
        if graph.get("schemaVersion") != GRAPH_SCHEMA:
            raise ProductionReducerError("V2 authenticated campaign-output graph is required")
    graph_by_panel = _unique_by(graph_list, "panelId", "campaign-output graphs")
    if set(graph_by_panel) != set(PANELS):
        raise ProductionReducerError("exact panel-1/panel-2/panel-3 outputs are required")
    policy_authorities = [graph_by_panel[panel].get("campaignPolicyAuthority") for panel in PANELS]
    if any(not isinstance(authority, Mapping) for authority in policy_authorities):
        raise ProductionReducerError("campaign policy authority is missing")
    for authority in policy_authorities:
        _verify_self_hash(authority, "policyAuthoritySha256", "campaign policy authority")
    campaign_policy_authority = dict(policy_authorities[0])
    archive_policy = campaign_policy_authority.get("archivePolicyAuthority")
    if not isinstance(archive_policy, Mapping):
        raise ProductionReducerError("archive policy authority is missing")
    behavior_requirement = campaign_policy_authority.get("behaviorAttributionRequirement")
    if any(
        authority.get("archivePolicyAuthority") != archive_policy
        or authority.get("behaviorAttributionRequirement") != behavior_requirement
        for authority in policy_authorities[1:]
    ):
        raise ProductionReducerError("scientific campaign policy drifted across panels")
    verify_archive_policy_authority(archive_policy)

    control_by_panel = _unique_by(launch_control["panels"], "panelId", "launch control panels")
    task_ids = Counter(row["newTaskId"] for row in task_mapping["mappings"])
    if task_mapping.get("mappedTaskCount") != 144 or len(task_ids) != 144 or any(count != 1 for count in task_ids.values()):
        raise ProductionReducerError("task mapping is not an exact one-to-one 144-task map")
    mapping_by_panel: dict[str, set[tuple[str, str]]] = {}
    for panel_id, graph in graph_by_panel.items():
        graph_task_ids = {row["task_id"] for row in graph["campaignTasks"]}
        mapping_by_panel[panel_id] = {
            (row["candidateId"], row["newTaskId"])
            for row in task_mapping["mappings"]
            if row["newTaskId"] in graph_task_ids
        }
    panel_rows = {
        panel: _validate_panel(graph_by_panel[panel], control_by_panel[panel], mapping_by_panel[panel])
        for panel in PANELS
    }
    baseline_candidates = panel_rows["panel-3"][0]
    for panel_id in ("panel-1", "panel-2"):
        candidates = panel_rows[panel_id][0]
        if set(candidates) != set(baseline_candidates):
            raise ProductionReducerError("candidate set drifted across panels")
        for candidate_id in candidates:
            if _candidate_identity(candidates[candidate_id]) != _candidate_identity(baseline_candidates[candidate_id]):
                raise ProductionReducerError(f"{candidate_id} identity drifted across panels")
            metadata = ("blockId", "arm", "parentCandidateId")
            if tuple(candidates[candidate_id].get(key) for key in metadata) != tuple(
                baseline_candidates[candidate_id].get(key) for key in metadata
            ):
                raise ProductionReducerError(f"{candidate_id} block/arm/parent metadata drifted across panels")

    blocks: dict[str, dict[str, Mapping[str, Any]]] = {}
    for candidate in baseline_candidates.values():
        block_id = str(candidate["blockId"])
        arm = str(candidate["arm"])
        _changed_side(block_id, str(candidate["parentCandidateId"]))
        if arm not in ARMS or arm in blocks.setdefault(block_id, {}):
            raise ProductionReducerError("block has an invalid or duplicate arm")
        blocks[block_id][arm] = candidate
    if len(blocks) != 3 or any(set(arms) != set(ARMS) for arms in blocks.values()):
        raise ProductionReducerError("cohort must contain exactly three complete P/T/E/TE blocks")

    block_reports: dict[str, Any] = {}
    for block_id in sorted(blocks):
        panel_reports: dict[str, Any] = {}
        side = _changed_side(block_id, str(blocks[block_id]["P"]["parentCandidateId"]))
        for panel_id in PANELS:
            candidates, members, bundles = panel_rows[panel_id]
            actual_arms = {arm: candidates[row["candidateId"]] for arm, row in blocks[block_id].items()}
            panel_arms: dict[str, dict[str, Any]] = {}
            for arm, candidate in actual_arms.items():
                member = members[candidate["candidateId"]]
                aggregate = member["aggregate"]
                panel_arms[arm] = {
                    "candidateId": candidate["candidateId"],
                    "conservativeNetR": aggregate["totalConservativeNetR"],
                    "worstWindowConservativeNetR": aggregate["worstWindowConservativeNetR"],
                    "tradeCount": aggregate["totalTrades"],
                    "costDragR": aggregate["costDragR"],
                    **arm_eligibility(member, archive_policy),
                    "identity": _identity_projection(candidate),
                }
            evaluated = evaluate_panel_usefulness_v2(panel_arms)
            evaluated["mechanism"] = _mechanism_v2_5(
                panel_id=panel_id,
                block_id=block_id,
                side=side,
                arms=actual_arms,
                members=members,
                bundles=bundles,
            )
            panel_reports[panel_id] = evaluated
        replication = evaluate_replication_survival_v3(
            {
                panel: panel_reports[panel]["usefulProgressiveInnovationV2"]
                for panel in PANELS
            },
            identities_valid=True,
        )
        block_reports[block_id] = {
            "status": "complete",
            "changedSide": side,
            "arms": {arm: blocks[block_id][arm]["candidateId"] for arm in ARMS},
            "panelReports": panel_reports,
            "replication": replication,
            "productionConfirmed": False,
            "confirmationStatus": "pending",
        }

    result: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "status": "complete",
        "originalScientificContractSha256": SCIENTIFIC_SHA256,
        "originalReplicationRuleSha256": REPLICATION_RULE_SHA256,
        "panelUsefulnessPolicySha256": panel_policy["panelUsefulnessPolicySha256"],
        "campaignPolicyAuthoritySha256ByPanel": {
            panel: graph_by_panel[panel]["campaignPolicyAuthority"]["policyAuthoritySha256"]
            for panel in PANELS
        },
        "archivePolicySha256": archive_policy["policySha256"],
        "directionSelectionPolicySha256": archive_policy["frozenPolicy"]["directionSelection"]["selectionPolicySha256"],
        "launchControlSha256": launch_control["launchControlSha256"],
        "mappingSha256": task_mapping["mappingSha256"],
        "dispatchEnabled": False,
        "untouchedConfirmationStatus": "pending",
        "authenticatedPanels": {
            panel: {
                "evaluatedMembersRawSha256": graph_by_panel[panel]["campaignOutputCheckpoint"]["evaluatedMembers"]["rawSha256"],
                "candidatePanelBundlesRawSha256": graph_by_panel[panel]["campaignOutputCheckpoint"]["candidatePanelBundles"]["rawSha256"],
                "gatewaySemanticReceiptSha256": graph_by_panel[panel]["gatewayExecutionReceipt"]["semanticReceiptSha256"],
                "campaignInputCheckpointSha256": graph_by_panel[panel]["campaignInputCheckpoint"]["checkpointSha256"],
                "taskMatrixSha256": graph_by_panel[panel]["taskMatrixSha256"],
                "campaignPolicyAuthoritySha256": graph_by_panel[panel]["campaignPolicyAuthority"]["policyAuthoritySha256"],
            }
            for panel in PANELS
        },
        "blocks": block_reports,
        "familyLevelInferencePermitted": False,
    }
    result["analysisSha256"] = canonical_sha256(result)
    return result


def reduce_files_v3(
    *,
    checkpoints: Iterable[Path],
    opener: Path,
    launch_control_path: Path,
    task_mapping_path: Path,
    replication_rule_path: Path,
    scientific_contract_path: Path,
    analyzer_contract_path: Path,
    panel_policy_path: Path,
) -> dict[str, Any]:
    try:
        return reduce_authenticated_graphs_v3(
            graphs=[_open_graph(path, opener) for path in checkpoints],
            launch_control=_load_object(launch_control_path),
            task_mapping=_load_object(task_mapping_path),
            replication_rule=_load_object(replication_rule_path),
            scientific_contract=_load_object(scientific_contract_path),
            analyzer_contract=_load_object(analyzer_contract_path),
            panel_policy=_load_object(panel_policy_path),
        )
    except (KeyError, TypeError, json.JSONDecodeError, OSError, subprocess.SubprocessError, ProductionReducerError, ValueError) as exc:
        invalid: dict[str, Any] = {
            "schemaVersion": SCHEMA,
            "status": "incomplete_invalid",
            "reason": str(exc),
            "blocks": {},
            "familyLevelInferencePermitted": False,
            "productionConfirmed": False,
            "confirmationStatus": "pending",
        }
        invalid["analysisSha256"] = canonical_sha256(invalid)
        return invalid


def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-output-checkpoint", type=Path, action="append", required=True)
    parser.add_argument("--production-opener", type=Path, required=True)
    parser.add_argument("--launch-control", type=Path, required=True)
    parser.add_argument("--task-mapping", type=Path, required=True)
    parser.add_argument("--replication-rule", type=Path, required=True)
    parser.add_argument("--scientific-contract", type=Path, required=True)
    parser.add_argument("--analyzer-contract", type=Path, required=True)
    parser.add_argument("--panel-policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = reduce_files_v3(
        checkpoints=args.campaign_output_checkpoint,
        opener=args.production_opener,
        launch_control_path=args.launch_control,
        task_mapping_path=args.task_mapping,
        replication_rule_path=args.replication_rule,
        scientific_contract_path=args.scientific_contract,
        analyzer_contract_path=args.analyzer_contract,
        panel_policy_path=args.panel_policy,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(canonical_json(result) + "\n", encoding="utf-8", newline="\n")
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "CONTRACT_SCHEMA",
    "GRAPH_SCHEMA",
    "POLICY_SCHEMA",
    "SCHEMA",
    "reduce_authenticated_graphs_v3",
    "reduce_files_v3",
]
