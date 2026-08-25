"""Authenticated three-panel production reducer for the topology study.

The reducer accepts paths to production checkpoints, never caller-computed
metrics.  A Rust production opener authenticates each complete output graph;
Python then performs the frozen cohort join and preregistered calculations.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_post_run_analyzer_v1 import evaluate_panel
from .temporal_qd_topology_replication_survival_v2 import evaluate_replication_survival_v2

SCHEMA = "temporal_qd_topology_production_analysis_v2"
CONTRACT_SCHEMA = "temporal_qd_topology_production_reducer_contract_v2"
GRAPH_SCHEMA = "temporal_qd_v5_authenticated_campaign_output_graph_v1"
PANELS = ("panel-3", "panel-1", "panel-2")
ARMS = ("P", "T", "E", "TE")
SCIENTIFIC_SHA256 = "sha256:86a7d46a7ab3c675ef5f409d75c74dc8a8ca7abb3fefe2c7395f8347d80e7cd6"
REPLICATION_RULE_SHA256 = "sha256:c8f878ccd03e7f9fb54228836a165d1f753a712d32bd423dcd48d31262e4db04"


class ProductionReducerError(ValueError):
    """Authenticated input is incomplete or incompatible."""


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ProductionReducerError(f"{path} must contain an object")
    return value


def _verify_self_hash(value: Mapping[str, Any], field: str, label: str) -> None:
    unsigned = dict(value)
    stored = unsigned.pop(field, None)
    if stored != canonical_sha256(unsigned):
        raise ProductionReducerError(f"{label} self-hash mismatch")


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
        raise ProductionReducerError("production opener returned an incompatible graph")
    _verify_self_hash(value, "authenticatedGraphSha256", "authenticated graph")
    return value


def _unique_by(rows: Iterable[Mapping[str, Any]], key: str, label: str) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        identity = row.get(key)
        if not isinstance(identity, str) or identity in result:
            raise ProductionReducerError(f"{label} has missing or duplicate {key}")
        result[identity] = row
    return result


def _finite_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ProductionReducerError(f"{label} must be finite")
    return float(value)


def _candidate_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(
        row.get(key)
        for key in (
            "candidateIdentitySha256",
            "programSha256",
            "resolvedProgramSha256",
            "sourceProfileSha256",
            "profileSnapshotSha256",
            "resolvedProfileSnapshotSha256",
            "candidatePayloadSha256",
            "nativeAuthoritySha256",
        )
    )


def _changed_side(block_id: str, parent_id: str) -> str:
    parts = block_id.split("|")
    if len(parts) != 3 or parts[0] != "block" or parts[1] != parent_id or parts[2] not in {"long", "short"}:
        raise ProductionReducerError("block/parent/changed-side binding is invalid")
    return parts[2]


def _identity_projection(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: candidate[key]
        for key in (
            "candidateId",
            "candidateIdentitySha256",
            "programSha256",
            "resolvedProgramSha256",
            "sourceProfileSha256",
            "profileSnapshotSha256",
            "resolvedProfileSnapshotSha256",
        )
    }


def _unavailable(reason: str, binding_sha256: str) -> dict[str, Any]:
    return {"status": "unavailable", "reason": reason, "bindingSha256": binding_sha256}


def _mechanism(
    *,
    panel_id: str,
    block_id: str,
    side: str,
    arms: Mapping[str, Mapping[str, Any]],
    members: Mapping[str, Mapping[str, Any]],
    bundles: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    parent_profile = arms["P"]["sourceProfile"]
    topology_profile = arms["T"]["sourceProfile"]
    parent_states = {row["id"] for row in parent_profile["graph"]["states"]}
    topology_states = {row["id"] for row in topology_profile["graph"]["states"]}
    added_states = sorted(state for state in topology_states - parent_states if state.startswith(f"{side}_"))
    mechanism: dict[str, Any] = {"addedSetupNodeIds": added_states}
    parent_member = members[arms["P"]["candidateId"]]
    parent_side = parent_member["aggregate"]["realizedBehavior"]["sides"][side]
    for arm in ARMS:
        candidate = arms[arm]
        candidate_id = candidate["candidateId"]
        member = members[candidate_id]
        bundle = bundles[candidate_id]
        aggregate = member["aggregate"]
        arm_side = aggregate["realizedBehavior"]["sides"][side]
        window_bindings = [
            {
                "windowId": row["windowId"],
                "recordSha256": row["recordSha256"],
                "evidenceDigestSha256": row["evidenceDigestSha256"],
                "rawTaskProvenance": row["rawTaskProvenance"],
            }
            for row in bundle["windowEvidence"]
        ]
        bindings = {
            "blockId": block_id,
            "panelId": panel_id,
            "arm": arm,
            "changedSide": side,
            **_identity_projection(candidate),
            "bundleSha256": bundle["bundleSha256"],
            "behaviorIdentitySha256": aggregate["behaviorIdentitySha256"],
            "windows": window_bindings,
        }
        bindings["bindingSha256"] = canonical_sha256(bindings)
        binding_sha256 = bindings["bindingSha256"]
        occupancy = aggregate.get("stateOccupancyDistribution", {})
        transition_distribution = aggregate.get("transitionDistribution", {})
        arm_graph = candidate["sourceProfile"]["graph"]
        arm_states = {row["id"] for row in arm_graph["states"]}
        applicable_nodes = [node for node in added_states if node in arm_states]
        added_occupancy = {node: occupancy[node] for node in applicable_nodes if node in occupancy}
        route_ids = {
            row["id"]
            for row in arm_graph["transitions"]
            if row.get("sourceStateId") in applicable_nodes or row.get("destinationStateId") in applicable_nodes
        }
        route_activation = {
            transition: count
            for transition, count in transition_distribution.items()
            if transition in route_ids
        }
        sequence = arm_side.get("tradeSequence")
        parent_sequence = parent_side.get("tradeSequence")
        entry_shift: dict[str, Any]
        if isinstance(sequence, list) and isinstance(parent_sequence, list) and sequence and parent_sequence:
            entry_shift = {
                "status": "available",
                "candidateEntrySequence": sequence,
                "parentEntrySequence": parent_sequence,
                "bindings": bindings,
            }
        else:
            entry_shift = _unavailable("no comparable attributed entry sequence exists", binding_sha256)
        freshness_needed = arm in {"E", "TE"}
        mechanism[arm] = {
            "bindings": bindings,
            "addedSetupNodeOccupancy": {
                "status": "available" if applicable_nodes else "not_applicable",
                "values": added_occupancy,
            },
            "barsInAddedSetup": _unavailable(
                "retained attribution exposes normalized occupancy but not exact per-node bar counts",
                binding_sha256,
            ),
            "transitionPathThroughAddedSetup": {
                "status": "available" if applicable_nodes else "not_applicable",
                "configuredTransitionIds": sorted(route_ids),
                "transitionDistribution": route_activation,
            },
            "entryTimingShiftVersusP": entry_shift,
            "changedTradeOpportunityCountVersusP": {
                "status": "available",
                "value": int(aggregate["totalTrades"]) - int(parent_member["aggregate"]["totalTrades"]),
            },
            "eventFreshness": (
                _unavailable(
                    "the retained attribution identifies event-bound routes but does not count fresh-event guard evaluations",
                    binding_sha256,
                )
                if freshness_needed
                else {"status": "not_applicable"}
            ),
            "routeEventActivation": {
                "status": "available",
                "changedSideActive": arm_side["active"],
                "transitionDistribution": transition_distribution,
            },
            "tradeCountChangeVersusP": int(aggregate["totalTrades"]) - int(parent_member["aggregate"]["totalTrades"]),
            "costDragRChangeVersusP": _finite_number(aggregate["costDragR"], "cost drag")
            - _finite_number(parent_member["aggregate"]["costDragR"], "parent cost drag"),
        }
    return mechanism


def _validate_panel(
    graph: Mapping[str, Any],
    control_panel: Mapping[str, Any],
    expected_mapping: set[tuple[str, str]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    panel_id = str(graph["panelId"])
    input_checkpoint = graph["campaignInputCheckpoint"]
    if (
        graph["checkpointSha256"] != graph["campaignOutputCheckpoint"]["receiptSha256"]
        or input_checkpoint["checkpointSha256"] != control_panel["checkpointSha256"]
        or graph["taskMatrixSha256"] != control_panel["taskMatrixSha256"]
        or graph["taskCount"] != 48
        or input_checkpoint["candidateCount"] != 12
        or input_checkpoint["windowCount"] != 4
        or input_checkpoint["taskCount"] != 48
        or input_checkpoint["panelId"] != panel_id
    ):
        raise ProductionReducerError(f"{panel_id} launch-control/input binding drifted")
    candidates = _unique_by(graph["cohortPopulation"]["candidates"], "candidateId", f"{panel_id} cohort")
    members = _unique_by(graph["evaluatedMembers"], "candidateId", f"{panel_id} members")
    bundles = _unique_by(graph["candidatePanelBundles"], "candidateId", f"{panel_id} bundles")
    if len(candidates) != 12 or set(candidates) != set(members) or set(candidates) != set(bundles):
        raise ProductionReducerError(f"{panel_id} candidate/member/bundle cardinality drifted")
    task_pairs = {(row["payload"]["candidate_id"], row["task_id"]) for row in graph["campaignTasks"]}
    if len(task_pairs) != 48 or task_pairs != expected_mapping:
        raise ProductionReducerError(f"{panel_id} task mapping drifted")
    for candidate_id, candidate in candidates.items():
        member = members[candidate_id]
        bundle = bundles[candidate_id]
        aggregate = member["aggregate"]
        if (
            member["candidateId"] != candidate_id
            or member["candidate"].get("candidateId") != candidate_id
            or bundle["panelId"] != panel_id
            or bundle["candidateIdentitySha256"] != candidate["candidateIdentitySha256"]
            or bundle["programSha256"] != candidate["resolvedProgramSha256"]
            or bundle["rawSourceProfileSha256"] != candidate["sourceProfileSha256"]
            or bundle["normalizedProfileSnapshotSha256"] != candidate["resolvedProfileSnapshotSha256"]
            or aggregate["authoredProgramSha256"] != candidate["programSha256"]
            or aggregate["resolvedProgramSha256"] != candidate["resolvedProgramSha256"]
            or aggregate["sourceProfileSha256"] != candidate["sourceProfileSha256"]
            or aggregate["resolvedProfileSnapshotSha256"] != candidate["resolvedProfileSnapshotSha256"]
        ):
            raise ProductionReducerError(f"{panel_id}/{candidate_id} identity drifted")
        windows = bundle["windowEvidence"]
        window_ids = [row["windowId"] for row in windows]
        expected_windows = {
            row["payload"]["window_id"]
            for row in graph["campaignTasks"]
            if row["payload"]["candidate_id"] == candidate_id
        }
        if len(windows) != 4 or len(set(window_ids)) != 4 or set(window_ids) != expected_windows:
            raise ProductionReducerError(f"{panel_id}/{candidate_id} four-window evidence drifted")
        aggregate_counts = list(aggregate["tradeCountsByWindow"])
        bundle_counts = [row["metrics"]["closedTrades"] for row in windows]
        if aggregate_counts != bundle_counts or sum(bundle_counts) != aggregate["totalTrades"]:
            raise ProductionReducerError(f"{panel_id}/{candidate_id} trade evidence drifted")
        for metric in ("totalConservativeNetR", "worstWindowConservativeNetR", "costDragR"):
            _finite_number(aggregate[metric], f"{panel_id}/{candidate_id}/{metric}")
        validity = member["finiteDataValidity"]
        if type(validity.get("passesSupportGate")) is not bool or type(validity.get("validForQuality")) is not bool:
            raise ProductionReducerError(f"{panel_id}/{candidate_id} gate evidence is not Boolean")
    return candidates, members, bundles


def reduce_authenticated_graphs(
    *,
    graphs: Iterable[Mapping[str, Any]],
    launch_control: Mapping[str, Any],
    task_mapping: Mapping[str, Any],
    replication_rule: Mapping[str, Any],
    scientific_contract: Mapping[str, Any],
    analyzer_contract: Mapping[str, Any],
) -> dict[str, Any]:
    _verify_self_hash(launch_control, "launchControlSha256", "launch control")
    _verify_self_hash(task_mapping, "mappingSha256", "task mapping")
    _verify_self_hash(replication_rule, "replicationRuleSha256", "replication rule")
    _verify_self_hash(scientific_contract, "scientificContractSha256", "scientific contract")
    _verify_self_hash(analyzer_contract, "analyzerContractSha256", "analyzer contract")
    if scientific_contract["scientificContractSha256"] != SCIENTIFIC_SHA256:
        raise ProductionReducerError("scientific contract is not the frozen V2.3 authority")
    if replication_rule["replicationRuleSha256"] != REPLICATION_RULE_SHA256:
        raise ProductionReducerError("replication rule is not the frozen V2.3 authority")
    if analyzer_contract.get("scientificContractSha256") != SCIENTIFIC_SHA256 or analyzer_contract.get("replicationRuleSha256") != REPLICATION_RULE_SHA256:
        raise ProductionReducerError("analyzer contract is not bound to frozen science/rule")
    if launch_control.get("dispatchEnabled") is not False or launch_control.get("totalInspectedTaskCount") != 144:
        raise ProductionReducerError("launch control safety/count binding drifted")

    graph_list = list(graphs)
    for graph in graph_list:
        _verify_self_hash(graph, "authenticatedGraphSha256", "authenticated graph")
    graph_by_panel = _unique_by(graph_list, "panelId", "campaign-output graphs")
    if set(graph_by_panel) != set(PANELS):
        raise ProductionReducerError("exact panel-1/panel-2/panel-3 outputs are required")
    control_by_panel = _unique_by(launch_control["panels"], "panelId", "launch control panels")
    mapping_by_panel: dict[str, set[tuple[str, str]]] = {}
    task_ids = Counter(row["newTaskId"] for row in task_mapping["mappings"])
    if task_mapping.get("mappedTaskCount") != 144 or len(task_ids) != 144 or any(count != 1 for count in task_ids.values()):
        raise ProductionReducerError("task mapping is not an exact one-to-one 144-task map")
    for panel_id, graph in graph_by_panel.items():
        graph_task_ids = {row["task_id"] for row in graph["campaignTasks"]}
        mapping_by_panel[panel_id] = {
            (row["candidateId"], row["newTaskId"])
            for row in task_mapping["mappings"]
            if row["newTaskId"] in graph_task_ids
        }

    panel_rows: dict[str, tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]] = {}
    for panel_id in PANELS:
        panel_rows[panel_id] = _validate_panel(
            graph_by_panel[panel_id], control_by_panel[panel_id], mapping_by_panel[panel_id]
        )
    baseline_candidates = panel_rows["panel-3"][0]
    for panel_id in ("panel-1", "panel-2"):
        candidates = panel_rows[panel_id][0]
        if set(candidates) != set(baseline_candidates):
            raise ProductionReducerError("candidate set drifted across panels")
        for candidate_id in candidates:
            if _candidate_identity(candidates[candidate_id]) != _candidate_identity(baseline_candidates[candidate_id]):
                raise ProductionReducerError(f"{candidate_id} identity drifted across panels")
            if tuple(candidates[candidate_id].get(key) for key in ("blockId", "arm", "parentCandidateId")) != tuple(
                baseline_candidates[candidate_id].get(key) for key in ("blockId", "arm", "parentCandidateId")
            ):
                raise ProductionReducerError(f"{candidate_id} block/arm/parent metadata drifted across panels")

    blocks: dict[str, dict[str, Mapping[str, Any]]] = {}
    for candidate in baseline_candidates.values():
        block_id = str(candidate["blockId"])
        arm = str(candidate["arm"])
        parent_id = str(candidate["parentCandidateId"])
        _changed_side(block_id, parent_id)
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
            panel_arms: dict[str, dict[str, Any]] = {}
            actual_arms = {arm: candidates[row["candidateId"]] for arm, row in blocks[block_id].items()}
            for arm, candidate in actual_arms.items():
                member = members[candidate["candidateId"]]
                aggregate = member["aggregate"]
                validity = member["finiteDataValidity"]
                direction = aggregate["realizedBehavior"]["sides"][side]["active"]
                if type(direction) is not bool:
                    raise ProductionReducerError("direction evidence is not Boolean")
                panel_arms[arm] = {
                    "candidateId": candidate["candidateId"],
                    "conservativeNetR": aggregate["totalConservativeNetR"],
                    "worstWindowConservativeNetR": aggregate["worstWindowConservativeNetR"],
                    "tradeCount": aggregate["totalTrades"],
                    "tradeCountsByWindow": aggregate["tradeCountsByWindow"],
                    "costDragR": aggregate["costDragR"],
                    "support": validity["passesSupportGate"],
                    "direction": direction,
                    "quality": validity["validForQuality"],
                    "identity": _identity_projection(candidate),
                }
            evaluated = evaluate_panel(panel_arms)
            evaluated["mechanism"] = _mechanism(
                panel_id=panel_id,
                block_id=block_id,
                side=side,
                arms=actual_arms,
                members=members,
                bundles=bundles,
            )
            panel_reports[panel_id] = evaluated
        replication = evaluate_replication_survival_v2(
            {panel: panel_reports[panel]["usefulProgressiveInnovation"] for panel in PANELS},
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
        "scientificContractSha256": SCIENTIFIC_SHA256,
        "replicationRuleSha256": REPLICATION_RULE_SHA256,
        "launchControlSha256": launch_control["launchControlSha256"],
        "mappingSha256": task_mapping["mappingSha256"],
        "dispatchEnabled": False,
        "untouchedConfirmationStatus": "pending",
        "authenticatedPanels": {
            panel: {
                # Root-bound receipt/path identities were authenticated by the
                # Rust opener but are intentionally excluded from the semantic
                # result so identical evidence reduces identically elsewhere.
                "evaluatedMembersRawSha256": graph_by_panel[panel]["campaignOutputCheckpoint"]["evaluatedMembers"]["rawSha256"],
                "candidatePanelBundlesRawSha256": graph_by_panel[panel]["campaignOutputCheckpoint"]["candidatePanelBundles"]["rawSha256"],
                "gatewaySemanticReceiptSha256": graph_by_panel[panel]["gatewayExecutionReceipt"]["semanticReceiptSha256"],
                "campaignInputCheckpointSha256": graph_by_panel[panel]["campaignInputCheckpoint"]["checkpointSha256"],
                "taskMatrixSha256": graph_by_panel[panel]["taskMatrixSha256"],
            }
            for panel in PANELS
        },
        "blocks": block_reports,
        "familyLevelInferencePermitted": False,
    }
    result["analysisSha256"] = canonical_sha256(result)
    return result


def reduce_files(
    *,
    checkpoints: Iterable[Path],
    opener: Path,
    launch_control_path: Path,
    task_mapping_path: Path,
    replication_rule_path: Path,
    scientific_contract_path: Path,
    analyzer_contract_path: Path,
) -> dict[str, Any]:
    try:
        return reduce_authenticated_graphs(
            graphs=[_open_graph(path, opener) for path in checkpoints],
            launch_control=_load_object(launch_control_path),
            task_mapping=_load_object(task_mapping_path),
            replication_rule=_load_object(replication_rule_path),
            scientific_contract=_load_object(scientific_contract_path),
            analyzer_contract=_load_object(analyzer_contract_path),
        )
    except (KeyError, TypeError, json.JSONDecodeError, OSError, subprocess.SubprocessError, ProductionReducerError) as exc:
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
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = reduce_files(
        checkpoints=args.campaign_output_checkpoint,
        opener=args.production_opener,
        launch_control_path=args.launch_control,
        task_mapping_path=args.task_mapping,
        replication_rule_path=args.replication_rule,
        scientific_contract_path=args.scientific_contract,
        analyzer_contract_path=args.analyzer_contract,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(canonical_json(result) + "\n", encoding="utf-8", newline="\n")
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "CONTRACT_SCHEMA",
    "ProductionReducerError",
    "SCHEMA",
    "reduce_authenticated_graphs",
    "reduce_files",
]
