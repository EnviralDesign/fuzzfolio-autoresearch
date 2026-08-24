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
)
from autoresearch.temporal_qd_topology_coadaptation_v6 import (
    ARM_TE,
    BLOCK_CLASS_COMPLETE,
    PARITY_MUTATION_IDS,
    apply_topology_parity_mutation_v6,
    attach_topology_coadaptation_matrix_v6,
    promising_coadaptation_observation,
    topology_coadaptation_v6_from_config,
    validate_topology_coadaptation_matrix_v6,
)
from autoresearch.temporal_qd_v38_followup_audit_v4 import FOCUS_CHILD_ID
from autoresearch.temporal_qd_v38_followup_audit_v6 import run_audit_v6
from autoresearch.temporal_qd_v38_followup_authorities_v6 import (
    attempt_canonical_pair_compile_and_native_validation,
)

V6_DIR = Path("research/temporal-qd/v38-followup")
V6_SPEC = V6_DIR / "topology-coadaptation-matrix-spec-v6.json"
V6_ARCHIVE = V6_DIR / "v38-cumulative-event-child-archive-forensic-v6.json"
V6_ONE = V6_DIR / "resource-suboperation-one-plan-design-v6.json"
V6_TWO = V6_DIR / "resource-suboperation-near-two-plan-design-v6.json"
V6_INSPECTED = V6_DIR / "topology-case-study-inspected-task-authority-v6.json"
V6_CONFIRM = V6_DIR / "topology-future-untouched-confirmation-authority-v6.json"
V6_PAIR = V6_DIR / "canonical-pair-compile-attempt-v6.json"
V6_CORPUS = V6_DIR / "topology-coadaptation-python-rust-parity-corpus-v6.json"
V6_RECEIPTS = V6_DIR / "topology-coadaptation-materialization-receipts-v6.json"
V5_SPEC = V6_DIR / "topology-coadaptation-matrix-spec-v5.json"
V4_SPEC = V6_DIR / "topology-coadaptation-matrix-spec-v4.json"


def _require_v6() -> None:
    if not V6_SPEC.is_file():
        pytest.skip("v6 reports are not on disk")


def test_useful_innovation_rejects_worse_te_worst_window_than_t_and_e() -> None:
    observation = promising_coadaptation_observation(
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
    assert observation["combinedOutperformsBothSingleMutations"] is True
    assert observation["teNetGreaterThanP"] is True
    assert observation["teWorstWindowNotWorseThanP"] is True
    assert observation["teWorstWindowNotWorseThanTAndE"] is False
    assert observation["nonqualifyingRiskTradeoff"] is True
    assert observation["usefulProgressiveInnovation"] is False
    assert observation["promising"] is False


def test_interaction_observed_is_not_labeled_positive_interaction() -> None:
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
    assert observation["combinedOutperformsBothSingleMutations"] is True
    assert observation["usefulProgressiveInnovation"] is False
    assert observation["interactionNetR"] == (-6.0) - (-8.0) - (-7.0) + 2.0


def test_useful_innovation_requires_te_greater_than_parent_and_components() -> None:
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
    assert observation["nonqualifyingRiskTradeoff"] is False


def test_v5_frozen_bytes_are_unchanged() -> None:
    if not V5_SPEC.is_file():
        pytest.skip("v5 reports are not on disk")
    payload = json.loads(V5_SPEC.read_text(encoding="utf-8"))
    body = {key: value for key, value in payload.items() if key != "contractSha256"}
    assert payload["contractSha256"] == canonical_sha256(body)
    if V4_SPEC.is_file():
        event = json.loads(V4_SPEC.read_text(encoding="utf-8"))
        body = {key: value for key, value in event.items() if key != "contractSha256"}
        assert event["contractSha256"] == canonical_sha256(body)


def test_emitted_v6_receipts_bind_slot_ids_and_reject_fake_native() -> None:
    _require_v6()
    payload = json.loads(V6_SPEC.read_text(encoding="utf-8"))
    validated = validate_topology_coadaptation_matrix_v6(payload)
    assert validated["contractSha256"] == payload["contractSha256"]
    for receipt, slot in zip(validated["materializationReceipts"], validated["slots"], strict=True):
        assert receipt["receiptId"] == slot["slotId"]
    complete = [block for block in validated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE]
    assert len(complete) == 3
    receipts = {(item["blockId"], item["arm"]): item for item in validated["materializationReceipts"]}
    for block in complete:
        combined = receipts[(block["blockId"], ARM_TE)]
        assert combined["nativeValidationRan"] is False
        assert combined["frozenPairIdentitySha256"] is None
        assert "pairIdentitySha256" not in combined
    fake = apply_topology_parity_mutation_v6(payload, "fake_pair_native_report")
    with pytest.raises(TemporalDiscoveryContractError, match="native validation|FrozenPair"):
        validate_topology_coadaptation_matrix_v6(fake)
    drift = apply_topology_parity_mutation_v6(payload, "receipt_id_drift_only")
    with pytest.raises(TemporalDiscoveryContractError, match="receiptId must equal slotId"):
        validate_topology_coadaptation_matrix_v6(drift)


def test_adversarial_parity_mutations_all_reject() -> None:
    _require_v6()
    payload = json.loads(V6_SPEC.read_text(encoding="utf-8"))
    validate_topology_coadaptation_matrix_v6(payload)
    for mutation_id in PARITY_MUTATION_IDS:
        mutated = apply_topology_parity_mutation_v6(payload, mutation_id)
        with pytest.raises(TemporalDiscoveryContractError):
            validate_topology_coadaptation_matrix_v6(mutated)


def test_archive_direction_is_not_defaulted_and_density_median_bind() -> None:
    _require_v6()
    archive = json.loads(V6_ARCHIVE.read_text(encoding="utf-8"))
    focus = archive["focusRecord"]
    assert focus["candidateId"] == FOCUS_CHILD_ID
    flags = focus["exactGateFlags"]
    assert flags["directionEvidenceAvailable"] is False
    assert flags["directionEligible"] is False
    assert flags["directionNotABindingCause"] is True
    assert flags["capacityConsidered"] is False
    assert flags["averageTradesPerMonthPass"] is False
    assert flags["medianWindowNetPositive"] is False
    assert "average_trades_per_month_below_minimum" in focus["bindingCauses"]
    assert archive["frozenV38PolicyBinding"]["rotatingEvidencePinVerified"] is True
    assert archive["frozenV38PolicyBinding"]["missingPolicyShaReportedAsUnavailableNotDefaulted"] is True


def test_topology_and_resource_authorities_are_separate_and_unlaunched() -> None:
    _require_v6()
    inspected = json.loads(V6_INSPECTED.read_text(encoding="utf-8"))
    confirm = json.loads(V6_CONFIRM.read_text(encoding="utf-8"))
    one = json.loads(V6_ONE.read_text(encoding="utf-8"))
    two = json.loads(V6_TWO.read_text(encoding="utf-8"))
    pair = json.loads(V6_PAIR.read_text(encoding="utf-8"))
    assert inspected["canonicalPairCandidateCount"] == 12
    assert inspected["projectedInspectedPanelWorkerTasks"] == 144
    assert inspected["doNotLaunch"] is True
    assert confirm["projectedConfirmationWorkerTasks"] == 48
    assert confirm["futureConfirmationPanel"]["windowIdentitiesBound"] is False
    assert confirm["mustNotReuseResourceInventoryAsTopologyAuthority"] is True
    assert one["pairCandidateCount"] == 63
    assert one["projectedInspectedPanelWorkerTasks"] == 756
    assert one["projectedWithFutureConfirmationPanel"] == 1008
    assert one["coverageKind"] == "deterministic_case_study_coverage_not_repeatability"
    assert two["mutationPairCandidateCount"] == 112
    assert two["pairCandidateCount"] == 117
    assert two["projectedInspectedPanelWorkerTasks"] == 1404
    assert two["projectedWithFutureConfirmationPanel"] == 1872
    assert pair["frozenPairCompileRan"] is False
    assert pair["nativeValidationRan"] is False
    assert attempt_canonical_pair_compile_and_native_validation()["doNotLaunch"] is True
    assert projected_inspected_panel_worker_tasks(pair_candidate_count=12) == 12 * WINDOWS_PER_PANEL * 3


def test_standalone_receipt_set_hashes_and_overlay_stays_inert() -> None:
    _require_v6()
    receipts = json.loads(V6_RECEIPTS.read_text(encoding="utf-8"))
    body = {key: value for key, value in receipts.items() if key != "receiptSetSha256"}
    assert receipts["receiptSetSha256"] == canonical_sha256(body)
    payload = json.loads(V6_SPEC.read_text(encoding="utf-8"))
    assert topology_coadaptation_v6_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    attached = attach_topology_coadaptation_matrix_v6(
        {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"},
        payload,
    )
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    corpus = json.loads(V6_CORPUS.read_text(encoding="utf-8"))
    assert corpus["canonicalFixtureAccepted"] is True
    assert corpus["adversarialCount"] == len(PARITY_MUTATION_IDS)


def test_emit_twice_is_byte_identical(tmp_path: Path) -> None:
    if not V4_SPEC.is_file():
        pytest.skip("frozen v4 reports are not on disk")
    first = tmp_path / "a"
    second = tmp_path / "b"
    for target in (first, second):
        target.mkdir()
        for name in (
            "v38-directional-event-insert-forensic-v4.json",
            "v38-multipanel-suboperation-v4.json",
            "v38-cumulative-event-child-archive-forensic-v4.json",
            "resource-suboperation-candidate-inventory-v1.json",
            "topology-coadaptation-matrix-spec-v4.json",
        ):
            source = V6_DIR / name
            (target / name).write_bytes(source.read_bytes())
    written_a = run_audit_v6(output_dir=first)
    written_b = run_audit_v6(output_dir=second)
    assert set(written_a) == set(written_b)
    for name in written_a:
        if name == "README.md":
            continue
        left = written_a[name].read_bytes()
        right = written_b[name].read_bytes()
        assert left == right, name
    spec_a = json.loads((first / "topology-coadaptation-matrix-spec-v6.json").read_text(encoding="utf-8"))
    spec_b = json.loads((second / "topology-coadaptation-matrix-spec-v6.json").read_text(encoding="utf-8"))
    assert spec_a["contractSha256"] == spec_b["contractSha256"]
    _ = canonical_json
