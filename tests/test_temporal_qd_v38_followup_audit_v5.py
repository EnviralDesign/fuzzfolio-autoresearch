from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_resource_suboperation_inventory_v2 import (
    WINDOWS_PER_PANEL,
    projected_inspected_panel_worker_tasks,
    validate_resource_suboperation_candidate_inventory_v2,
)
from autoresearch.temporal_qd_topology_coadaptation_v5 import (
    ARM_TE,
    BLOCK_CLASS_COMPLETE,
    attach_topology_coadaptation_matrix_v5,
    promising_coadaptation_observation,
    topology_coadaptation_v5_from_config,
    validate_topology_coadaptation_matrix_v5,
)
from autoresearch.temporal_qd_v38_followup_audit_v4 import FOCUS_CHILD_ID
from autoresearch.temporal_qd_v38_followup_audit_v5 import run_audit_v5

V5_SPEC = Path("research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v5.json")
V5_INVENTORY = Path("research/temporal-qd/v38-followup/resource-suboperation-candidate-inventory-v2.json")
V5_ARCHIVE = Path("research/temporal-qd/v38-followup/v38-cumulative-event-child-archive-forensic-v5.json")
V5_MULTI = Path("research/temporal-qd/v38-followup/v38-multipanel-suboperation-v5.json")
V5_EVENT = Path("research/temporal-qd/v38-followup/v38-directional-event-insert-forensic-v5.json")
V5_PROPOSAL = Path("research/temporal-qd/v38-followup/resource-suboperation-balanced-design-proposal-v3.json")
V4_MULTI = Path("research/temporal-qd/v38-followup/v38-multipanel-suboperation-v4.json")
V4_EVENT = Path("research/temporal-qd/v38-followup/v38-directional-event-insert-forensic-v4.json")
V4_ARCHIVE = Path("research/temporal-qd/v38-followup/v38-cumulative-event-child-archive-forensic-v4.json")
V4_INVENTORY = Path("research/temporal-qd/v38-followup/resource-suboperation-candidate-inventory-v1.json")
V4_SPEC = Path("research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v4.json")


def _require_reports() -> None:
    if not V5_SPEC.is_file():
        pytest.skip("v5 reports are not on disk")


def test_interaction_is_not_useful_if_te_is_worse_than_parent() -> None:
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
    assert observation["teNetGreaterThanP"] is False


def test_useful_innovation_requires_te_greater_than_parent() -> None:
    observation = promising_coadaptation_observation(
        parent_net=1.0,
        topology_net=0.0,
        event_net=0.5,
        combined_net=2.0,
        parent_worst=-1.0,
        topology_worst=-2.0,
        event_worst=-1.5,
        combined_worst=-0.5,
        metric_greater=lambda left, right: left > right,
        metric_not_worse=lambda left, right: left >= right,
    )
    assert observation["usefulProgressiveInnovation"] is True
    assert observation["promising"] is True


def test_v4_frozen_bytes_are_unchanged_by_v5_identity_fields() -> None:
    for path in (V4_MULTI, V4_EVENT, V4_ARCHIVE, V4_INVENTORY, V4_SPEC):
        if not path.is_file():
            pytest.skip("v4 reports are not on disk")
    event = json.loads(V4_EVENT.read_text(encoding="utf-8"))
    body = {key: value for key, value in event.items() if key != "reportSha256"}
    assert event["reportSha256"] == canonical_sha256(body)


def test_emitted_v5_self_hashes_and_archive_gates() -> None:
    _require_reports()
    for path in (V5_EVENT, V5_MULTI, V5_ARCHIVE):
        payload = json.loads(path.read_text(encoding="utf-8"))
        body = {key: value for key, value in payload.items() if key != "reportSha256"}
        assert payload["reportSha256"] == canonical_sha256(body)
    archive = json.loads(V5_ARCHIVE.read_text(encoding="utf-8"))
    focus = archive["focusRecord"]
    assert focus["candidateId"] == FOCUS_CHILD_ID
    flags = focus["exactGateFlags"]
    metrics = focus["exactSupportMetrics"]
    assert flags["activeWindowFractionPass"] is True
    assert flags["averageTradesPerMonthPass"] is False
    assert flags["cumulativeNetPositive"] is True
    assert flags["medianWindowNetPositive"] is False
    assert flags["currentPanelQualityLike"] is False
    assert flags["currentPanelFrontierLike"] is False
    assert flags["capacityConsidered"] is False
    assert flags["competingMemberIsNotBindingCause"] is True
    assert metrics["closedTrades"] == 52
    assert metrics["coveredMonths"] == 36
    assert focus["reasonCode"] == "failed_trade_density_and_median_window_net"
    assert "average_trades_per_month_below_minimum" in focus["bindingCauses"]


def test_inventory_v2_uses_windows_not_panels_and_five_pair_clones() -> None:
    _require_reports()
    payload = json.loads(V5_INVENTORY.read_text(encoding="utf-8"))
    validated = validate_resource_suboperation_candidate_inventory_v2(payload)
    budget = validated["boundedTaskProjection"]
    assert budget["windowsPerPanel"] == WINDOWS_PER_PANEL == 4
    assert budget["panelCount"] == 3
    assert budget["totalWindowCount"] == 12
    assert budget["pairCloneCount"] == 5
    assert len(validated["pairCloneSlots"]) == 5
    assert all("side" not in slot for slot in validated["pairCloneSlots"])
    expected = projected_inspected_panel_worker_tasks(pair_candidate_count=budget["pairCandidateCount"])
    assert budget["projectedInspectedPanelWorkerTasks"] == expected
    assert expected == budget["pairCandidateCount"] * 12
    assert expected != budget["pairCandidateCount"] * budget["panelCount"]
    mutated = json.loads(canonical_json(payload))
    mutated["boundedTaskProjection"]["windowsPerPanel"] = mutated["boundedTaskProjection"]["panelCount"]
    mutated["boundedTaskProjection"]["projectedInspectedPanelWorkerTasks"] = (
        mutated["boundedTaskProjection"]["pairCandidateCount"] * mutated["boundedTaskProjection"]["panelCount"]
    )
    with pytest.raises(TemporalDiscoveryContractError, match="windowsPerPanel|panels as windows"):
        validate_resource_suboperation_candidate_inventory_v2(mutated)


def test_topology_v5_receipts_chain_and_reject_mislabeled_native_or_pair_fields() -> None:
    _require_reports()
    payload = json.loads(V5_SPEC.read_text(encoding="utf-8"))
    validated = validate_topology_coadaptation_matrix_v5(payload)
    assert validated["contractSha256"] == payload["contractSha256"]
    complete = [block for block in validated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE]
    assert len(complete) == 3
    receipts = {(item["blockId"], item["arm"]): item for item in validated["materializationReceipts"]}
    for block in complete:
        parent = receipts[(block["blockId"], "exact_parent_clone")]
        topology = receipts[(block["blockId"], "topology_only_child")]
        event = receipts[(block["blockId"], "event_only_control")]
        combined = receipts[(block["blockId"], ARM_TE)]
        delta = topology["topologySemanticDelta"]
        assert delta["beforeGenomeSha256"] == parent["changedSideGenomeSha256"]
        assert delta["afterGenomeSha256"] == topology["changedSideGenomeSha256"]
        assert event["applicationParentGenomeSha256"] == parent["changedSideGenomeSha256"]
        assert combined["applicationParentGenomeSha256"] == topology["changedSideGenomeSha256"]
        assert combined["eventAttachesToAddedSetupNode"] is True
        assert combined["nativeValidationRan"] is False
        assert combined["frozenPairIdentitySha256"] is None
        assert "pairIdentitySha256" not in combined
        assert "nativeCompileValidationIdentity" not in combined
        assert combined["unchangedOppositeProgramPreserved"] is True
    mutated = json.loads(canonical_json(payload))
    te = next(item for item in mutated["materializationReceipts"] if item["arm"] == ARM_TE and item["eligibility"] == "eligible")
    te["applicationParentGenomeSha256"] = te["changedSideGenomeSha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="TE application parent"):
        validate_topology_coadaptation_matrix_v5(mutated)
    extra = json.loads(canonical_json(payload))
    extra["materializationReceipts"][0]["pairIdentitySha256"] = extra["materializationReceipts"][0]["reconstructedPairProgramIdentitySha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="pair or native"):
        validate_topology_coadaptation_matrix_v5(extra)
    swapped = json.loads(canonical_json(payload))
    first = swapped["materializationReceipts"][0]
    second = swapped["materializationReceipts"][1]
    swapped["materializationReceipts"][0] = {**first, "receiptId": second["receiptId"], "arm": second["arm"], "blockId": second["blockId"]}
    swapped["materializationReceipts"][1] = {**second, "receiptId": first["receiptId"], "arm": first["arm"], "blockId": first["blockId"]}
    with pytest.raises(TemporalDiscoveryContractError):
        validate_topology_coadaptation_matrix_v5(swapped)
    assert topology_coadaptation_v5_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    attached = attach_topology_coadaptation_matrix_v5(
        {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"},
        payload,
    )
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    absent = {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "same"}
    assert "topologyCoadaptationMatrix" not in absent


def test_balanced_proposal_freezes_plan_ids_and_keeps_empty_cells() -> None:
    _require_reports()
    payload = json.loads(V5_PROPOSAL.read_text(encoding="utf-8"))
    assert payload["coverageKind"] == "deterministic_case_study_coverage_not_repeatability"
    assert payload["selectionRule"].startswith("lexicographic_minimum_planSha256")
    filled = [cell for cell in payload["cells"] if cell["status"] == "filled"]
    empty = [cell for cell in payload["cells"] if cell["status"] == "empty"]
    assert empty
    assert all(cell["selectedPlanSha256"] for cell in filled)
    assert all(cell["selectedPlanSha256"] is None for cell in empty)
    assert payload["pairCloneCount"] == 5
    assert payload["projectedInspectedPanelWorkerTasks"] == (len(filled) + 5) * 12


def test_v5_emission_is_deterministic_and_omits_runs() -> None:
    _require_reports()
    first = {path: path.read_bytes() for path in (V5_SPEC, V5_INVENTORY, V5_ARCHIVE, V5_MULTI)}
    run_audit_v5(output_dir=Path("research/temporal-qd/v38-followup"))
    for path, original in first.items():
        assert path.read_bytes() == original
    import subprocess

    tracked = subprocess.run(["git", "ls-files"], check=True, capture_output=True, text=True).stdout.splitlines()
    assert not any(item == "runs" or item.startswith("runs/") for item in tracked)
