from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_topology_post_run_analyzer_v1 import analyze_block, evaluate_panel
from autoresearch.temporal_qd_topology_production_reducer_v2 import (
    GRAPH_SCHEMA,
    ProductionReducerError,
    reduce_authenticated_graphs,
    reduce_files,
)
from autoresearch.temporal_qd_topology_replication_survival_v2 import (
    evaluate_replication_survival_v2,
)
from autoresearch.temporal_qd_v2_3_launch_gate import build_gate as build_v2_3_gate
from autoresearch.temporal_qd_v2_4_launch_gate import build_gate as build_v2_4_gate

ROOT = Path(__file__).resolve().parents[1]
AUTHORITY = ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-3"


def _signed(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def _arm_metrics(net: float) -> dict:
    return {
        "conservativeNetR": net,
        "worstWindowConservativeNetR": net,
        "tradeCount": 8,
        "costDragR": 0.1,
        "support": True,
        "direction": True,
        "quality": True,
    }


def test_v2_3_analyzer_reproduces_caller_trust_weakness() -> None:
    fabricated = {
        "panel-1": {arm: _arm_metrics(net) for arm, net in zip(("P", "T", "E", "TE"), (0, 1, 1, 3))},
        "panel-2": {arm: _arm_metrics(net) for arm, net in zip(("P", "T", "E", "TE"), (0, 1, 1, 3))},
        "panel-3": {arm: _arm_metrics(net) for arm, net in zip(("P", "T", "E", "TE"), (0, 1, 1, 3))},
    }
    result = analyze_block(block_id="fabricated", panels=fabricated, identities_valid=True)
    assert result["replication"]["inspectedPromising"] is True
    assert result["replication"]["evidenceCompleteAndIdentityValid"] is True


def test_v2_3_gate_reproduces_summary_boolean_trust_weakness(tmp_path: Path) -> None:
    def dump(name: str, value: dict) -> Path:
        path = tmp_path / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    evidence = dump(
        "evidence.json",
        {
            "allCheckpointsOpened": True,
            "crossRootAllEqual": True,
            "allCheckpointIdentitiesMatchExecutedSeamProof": True,
        },
    )
    conformances = [
        dump(f"conformance-{panel}.json", {"validatedCandidateCount": 12, "fullWorkerExecutionFixtureCount": 12, "marketDataRead": False})
        for panel in range(3)
    ]
    full = dump("full.json", {"fullWorkerExecutionFixtureCount": 48, "validatedTaskCount": 48, "marketDataRead": False})
    gateway_log = dump("gateway-log.json", {"loopbackOnly": True, "marketDataRead": False, "ackDurability": [{"journalDurableBeforeAck": True, "resultPackDurableBeforeAck": True}]})
    gateway_receipt = dump("gateway-receipt.json", {"taskCount": 48, "completedTaskCount": 48, "resultCount": 48})
    output = dump(
        "campaign-output.json",
        {
            "freshRestart": False,
            "reopenRestart": True,
            "recoveredRestart": True,
            "tamperRejected": True,
            "taskCount": 48,
            "evaluatedMemberCount": 12,
            "panelBundleCount": 12,
        },
    )
    gate = build_v2_3_gate(
        authority_root=AUTHORITY,
        production_evidence_path=evidence,
        conformance_paths=conformances,
        full_48_conformance_path=full,
        gateway_log_path=gateway_log,
        gateway_receipt_path=gateway_receipt,
        campaign_output_proof_path=output,
    )
    assert gate["readyForAuthorizedTopologyCaseStudyLaunch"] is True


def _candidate(block: int, arm: str) -> dict:
    candidate_id = f"candidate-{block}-{arm}"
    parent = f"parent-{block}"
    side = "long" if block == 0 else "short"
    states = [{"id": "flat_supervisor"}, {"id": f"{side}_base"}]
    transitions = []
    if arm in {"T", "TE"}:
        states.append({"id": f"{side}_added"})
        transitions.append(
            {
                "id": f"{side}_through_added",
                "sourceStateId": "flat_supervisor",
                "destinationStateId": f"{side}_added",
            }
        )
    return {
        "arm": arm,
        "blockId": f"block|{parent}|{side}",
        "candidateId": candidate_id,
        "parentCandidateId": parent,
        "candidateIdentitySha256": f"sha256:{block:02x}{ord(arm[0]):02x}".ljust(71, "a"),
        "candidatePayloadSha256": f"payload-{candidate_id}",
        "nativeAuthoritySha256": "native-authority",
        "programSha256": f"program-{candidate_id}",
        "resolvedProgramSha256": f"program-{candidate_id}",
        "sourceProfileSha256": f"raw-profile-{candidate_id}",
        "profileSnapshotSha256": f"profile-{candidate_id}",
        "resolvedProfileSnapshotSha256": f"profile-{candidate_id}",
        "sourceProfile": {"graph": {"states": states, "transitions": transitions}},
    }


def _graph(panel_number: int) -> dict:
    panel_id = f"panel-{panel_number}"
    candidates = [_candidate(block, arm) for block in range(3) for arm in ("P", "T", "E", "TE")]
    tasks = []
    members = []
    bundles = []
    for candidate in candidates:
        candidate_id = candidate["candidateId"]
        windows = []
        for window in range(4):
            task_id = f"task-{panel_id}-{candidate_id}-{window}"
            tasks.append({"task_id": task_id, "payload": {"candidate_id": candidate_id, "window_id": f"window-{window}"}})
            windows.append(
                {
                    "windowId": f"window-{window}",
                    "recordSha256": f"record-{task_id}",
                    "evidenceDigestSha256": f"evidence-{task_id}",
                    "rawTaskProvenance": {"taskId": task_id, "resultSha256": f"result-{task_id}"},
                    "metrics": {"closedTrades": 2},
                }
            )
        aggregate = {
            "authoredProgramSha256": candidate["programSha256"],
            "resolvedProgramSha256": candidate["resolvedProgramSha256"],
            "sourceProfileSha256": candidate["sourceProfileSha256"],
            "resolvedProfileSnapshotSha256": candidate["resolvedProfileSnapshotSha256"],
            "totalConservativeNetR": {"P": 0.0, "T": 1.0, "E": 1.0, "TE": 3.0}[candidate["arm"]],
            "worstWindowConservativeNetR": 0.0,
            "costDragR": 0.1,
            "totalTrades": 8,
            "tradeCountsByWindow": [2, 2, 2, 2],
            "behaviorIdentitySha256": f"behavior-{panel_id}-{candidate_id}",
            "stateOccupancyDistribution": {},
            "transitionDistribution": {},
            "realizedBehavior": {
                "sides": {
                    "long": {"active": True, "tradeSequence": []},
                    "short": {"active": True, "tradeSequence": []},
                }
            },
        }
        members.append(
            {
                "candidateId": candidate_id,
                "candidate": {"candidateId": candidate_id},
                "aggregate": aggregate,
                "finiteDataValidity": {"passesSupportGate": True, "validForQuality": True},
            }
        )
        bundles.append(
            {
                "candidateId": candidate_id,
                "panelId": panel_id,
                "candidateIdentitySha256": candidate["candidateIdentitySha256"],
                "programSha256": candidate["resolvedProgramSha256"],
                "rawSourceProfileSha256": candidate["sourceProfileSha256"],
                "normalizedProfileSnapshotSha256": candidate["resolvedProfileSnapshotSha256"],
                "bundleSha256": f"bundle-{panel_id}-{candidate_id}",
                "windowEvidence": windows,
            }
        )
    graph = {
        "schemaVersion": GRAPH_SCHEMA,
        "panelId": panel_id,
        "checkpointSha256": f"output-{panel_id}",
        "campaignOutputCheckpoint": {
            "receiptSha256": f"output-{panel_id}",
            "semanticReceiptSha256": f"output-semantic-{panel_id}",
            "evaluatedMembers": {"rawSha256": f"members-{panel_id}"},
            "candidatePanelBundles": {"rawSha256": f"bundles-{panel_id}"},
        },
        "gatewayExecutionReceipt": {"semanticReceiptSha256": f"gateway-semantic-{panel_id}"},
        "campaignInputCheckpoint": {
            "checkpointSha256": f"input-{panel_id}",
            "candidateCount": 12,
            "windowCount": 4,
            "taskCount": 48,
            "panelId": panel_id,
        },
        "taskMatrixSha256": f"matrix-{panel_id}",
        "taskCount": 48,
        "cohortPopulation": {"candidates": candidates},
        "campaignTasks": tasks,
        "evaluatedMembers": members,
        "candidatePanelBundles": bundles,
    }
    return _signed(graph, "authenticatedGraphSha256")


def _authorities(graphs: list[dict]) -> tuple[dict, dict, dict, dict, dict]:
    launch = {
        "dispatchEnabled": False,
        "totalInspectedTaskCount": 144,
        "panels": [
            {
                "panelId": graph["panelId"],
                "checkpointSha256": graph["campaignInputCheckpoint"]["checkpointSha256"],
                "taskMatrixSha256": graph["taskMatrixSha256"],
            }
            for graph in graphs
        ],
    }
    _signed(launch, "launchControlSha256")
    mapping = {
        "mappedTaskCount": 144,
        "mappings": [
            {"candidateId": row["payload"]["candidate_id"], "newTaskId": row["task_id"]}
            for graph in graphs
            for row in graph["campaignTasks"]
        ],
    }
    _signed(mapping, "mappingSha256")
    rule = json.loads((AUTHORITY / "topology-replication-survival-rule-v1.json").read_text())
    scientific = json.loads(
        (ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json").read_text()
    )
    analyzer = json.loads((AUTHORITY / "topology-post-run-analyzer-contract-v1.json").read_text())
    return launch, mapping, rule, scientific, analyzer


def _reduce(graphs: list[dict]) -> dict:
    launch, mapping, rule, scientific, analyzer = _authorities(graphs)
    return reduce_authenticated_graphs(
        graphs=graphs,
        launch_control=launch,
        task_mapping=mapping,
        replication_rule=rule,
        scientific_contract=scientific,
        analyzer_contract=analyzer,
    )


def test_reducer_derives_complete_three_panel_result_without_summary_input() -> None:
    result = _reduce([_graph(1), _graph(2), _graph(3)])
    assert result["status"] == "complete"
    assert len(result["blocks"]) == 3
    assert all(row["replication"]["inspectedPromising"] is True for row in result["blocks"].values())
    assert result["dispatchEnabled"] is False
    assert result["untouchedConfirmationStatus"] == "pending"


@pytest.mark.parametrize("mutation", ["missing_panel", "duplicate_panel", "missing_candidate", "duplicate_arm", "identity_drift", "window_duplicate", "nonfinite"])
def test_reducer_adversarial_structure_and_metric_drift_fails_closed(mutation: str) -> None:
    graphs = [_graph(1), _graph(2), _graph(3)]
    if mutation == "missing_panel":
        graphs.pop()
    elif mutation == "duplicate_panel":
        graphs[2]["panelId"] = "panel-2"
    elif mutation == "missing_candidate":
        graphs[0]["cohortPopulation"]["candidates"].pop()
    elif mutation == "duplicate_arm":
        graphs[0]["cohortPopulation"]["candidates"][1]["arm"] = "P"
    elif mutation == "identity_drift":
        graphs[0]["cohortPopulation"]["candidates"][0]["programSha256"] = "drift"
    elif mutation == "window_duplicate":
        graphs[0]["candidatePanelBundles"][0]["windowEvidence"][1]["windowId"] = "window-0"
    elif mutation == "nonfinite":
        graphs[0]["evaluatedMembers"][0]["aggregate"]["totalConservativeNetR"] = float("inf")
    for graph in graphs:
        graph.pop("authenticatedGraphSha256", None)
        _signed(graph, "authenticatedGraphSha256")
    with pytest.raises(ProductionReducerError):
        _reduce(graphs)


def test_numerical_ties_risk_tradeoff_and_no_fixed_margin() -> None:
    tied = {arm: _arm_metrics(1.0) for arm in ("P", "T", "E", "TE")}
    assert evaluate_panel(tied)["usefulProgressiveInnovation"] is False
    risk = {arm: _arm_metrics(net) for arm, net in zip(("P", "T", "E", "TE"), (1.0, 1.0, 1.0, 1.0))}
    risk["TE"]["conservativeNetR"] = 1.0 + 2e-12
    risk["P"]["worstWindowConservativeNetR"] = 0.0
    risk["T"]["worstWindowConservativeNetR"] = 1.0
    risk["E"]["worstWindowConservativeNetR"] = 1.0
    risk["TE"]["worstWindowConservativeNetR"] = 0.5
    assert evaluate_panel(risk)["nonqualifyingRiskTradeoff"] is True


def test_v2_reporting_projection_reaches_every_declared_category() -> None:
    cases = [
        ({"panel-3": True, "panel-1": True, "panel-2": True}, True),
        ({"panel-3": True, "panel-1": False, "panel-2": False}, True),
        ({"panel-3": False, "panel-1": True, "panel-2": False}, True),
        ({"panel-3": True, "panel-1": True, "panel-2": False}, True),
        ({"panel-3": False, "panel-1": False, "panel-2": False}, True),
        ({"panel-3": True, "panel-1": True}, True),
    ]
    observed = {evaluate_replication_survival_v2(values, identities_valid=valid)["reportingCategory"] for values, valid in cases}
    assert observed == {
        "inspected_promising_pending_untouched_confirmation",
        "development_only_not_replicated",
        "replication_only_discordant_not_promising",
        "mixed_panel_nonqualifying",
        "complete_no_useful_panel",
        "incomplete_invalid",
    }


def test_python_projection_exactly_matches_shared_parity_corpus() -> None:
    corpus = json.loads(
        (ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-4" / "topology-replication-parity-corpus-v2.json").read_text()
    )
    unsigned = dict(corpus)
    assert unsigned.pop("corpusSha256") == canonical_sha256(unsigned)
    for case in corpus["cases"]:
        assert evaluate_replication_survival_v2(
            case["inputs"], identities_valid=case["identitiesValid"]
        ) == case["expected"]


@pytest.mark.skipif(not os.getenv("FUZZFOLIO_V2_4_INTEGRATION_ROOT"), reason="audit-only production proof root not supplied")
def test_exact_three_production_outputs_feed_reducer_without_hand_built_summaries() -> None:
    proof_root = Path(os.environ["FUZZFOLIO_V2_4_INTEGRATION_ROOT"])
    result = reduce_files(
        checkpoints=[proof_root / f"panel-{panel}" / "campaign-output-local" / "campaign-output-checkpoint.json" for panel in (1, 2, 3)],
        opener=ROOT / "rust" / "temporal-qd" / "target" / "debug" / "temporal-qd-campaign-output-graph-json.exe",
        launch_control_path=AUTHORITY / "topology-production-launch-control-v1.json",
        task_mapping_path=AUTHORITY / "topology-production-task-mapping-v1.json",
        replication_rule_path=AUTHORITY / "topology-replication-survival-rule-v1.json",
        scientific_contract_path=ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json",
        analyzer_contract_path=AUTHORITY / "topology-post-run-analyzer-contract-v1.json",
    )
    assert result["status"] == "complete"
    assert len(result["authenticatedPanels"]) == 3
    assert len(result["blocks"]) == 3


@pytest.mark.skipif(not os.getenv("FUZZFOLIO_V2_4_INTEGRATION_ROOT"), reason="audit-only production proof root not supplied")
def test_v2_4_gate_recomputes_complete_production_authority() -> None:
    proof_root = Path(os.environ["FUZZFOLIO_V2_4_INTEGRATION_ROOT"])
    gate = build_v2_4_gate(
        repo_root=ROOT,
        authority_root=AUTHORITY,
        proof_root=proof_root,
        opener=ROOT / "rust" / "temporal-qd" / "target" / "debug" / "temporal-qd-campaign-output-graph-json.exe",
        parity_bin=ROOT / "rust" / "temporal-qd" / "target" / "debug" / "temporal-qd-replication-survival-v2-jsonl.exe",
        cross_root_report_path=proof_root / "topology-v2-4-cross-root-report.json",
    )
    assert gate["readyForAuthorizedTopologyCaseStudyLaunch"] is True
    assert all(gate["gates"].values())
