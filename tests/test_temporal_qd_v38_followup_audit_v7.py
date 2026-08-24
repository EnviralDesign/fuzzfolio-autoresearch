from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_bidirectional_genome import FrozenPair
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA
from autoresearch.temporal_qd_resource_suboperation_inventory_v2 import (
    WINDOWS_PER_PANEL,
    projected_inspected_panel_worker_tasks,
)
from autoresearch.temporal_qd_topology_coadaptation_v7 import (
    ARM_P,
    ARM_TE,
    BLOCK_CLASS_COMPLETE,
    PARITY_MUTATION_IDS,
    apply_topology_parity_mutation_v7,
    attach_topology_coadaptation_matrix_v7,
    promising_coadaptation_observation,
    topology_coadaptation_v7_from_config,
    validate_topology_coadaptation_matrix_v7,
)
from autoresearch.temporal_qd_v38_followup_audit_v4 import FOCUS_CHILD_ID
from autoresearch.temporal_qd_v38_followup_audit_v7 import run_audit_v7

V7_DIR = Path("research/temporal-qd/v38-followup")
V7_SPEC = V7_DIR / "topology-coadaptation-matrix-spec-v7.json"
V7_ARCHIVE = V7_DIR / "v38-cumulative-event-child-archive-forensic-v7.json"
V7_ONE = V7_DIR / "resource-suboperation-one-plan-design-v7.json"
V7_TWO = V7_DIR / "resource-suboperation-near-two-plan-design-v7.json"
V7_INSPECTED = V7_DIR / "topology-case-study-inspected-task-authority-v7.json"
V7_MATRIX = V7_DIR / "topology-case-study-inspected-task-matrix-v7.json"
V7_CONFIRM = V7_DIR / "topology-future-untouched-confirmation-authority-v7.json"
V7_PAIR = V7_DIR / "canonical-pair-compile-attempt-v7.json"
V7_CORPUS = V7_DIR / "topology-coadaptation-python-rust-parity-corpus-v7.json"
V7_RECEIPTS = V7_DIR / "topology-coadaptation-materialization-receipts-v7.json"
V7_PAYLOADS = V7_DIR / "topology-canonical-frozen-pair-payloads-v7.json"
V7_GO = V7_DIR / "v38-followup-v7-go-nogo.json"
V6_SPEC = V7_DIR / "topology-coadaptation-matrix-spec-v6.json"
V5_SPEC = V7_DIR / "topology-coadaptation-matrix-spec-v5.json"
V4_SPEC = V7_DIR / "topology-coadaptation-matrix-spec-v4.json"


def _require_v7() -> None:
    if not V7_SPEC.is_file():
        pytest.skip("v7 reports are not on disk")


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


def test_v5_and_v6_frozen_bytes_are_unchanged() -> None:
    if not V5_SPEC.is_file():
        pytest.skip("v5 reports are not on disk")
    payload = json.loads(V5_SPEC.read_text(encoding="utf-8"))
    body = {key: value for key, value in payload.items() if key != "contractSha256"}
    assert payload["contractSha256"] == canonical_sha256(body)
    if V4_SPEC.is_file():
        event = json.loads(V4_SPEC.read_text(encoding="utf-8"))
        body = {key: value for key, value in event.items() if key != "contractSha256"}
        assert event["contractSha256"] == canonical_sha256(body)
    if V6_SPEC.is_file():
        spec = json.loads(V6_SPEC.read_text(encoding="utf-8"))
        body = {key: value for key, value in spec.items() if key != "contractSha256"}
        assert spec["contractSha256"] == canonical_sha256(body)


def test_emitted_v7_receipts_bind_slot_ids_and_compiled_identities() -> None:
    _require_v7()
    payload = json.loads(V7_SPEC.read_text(encoding="utf-8"))
    validated = validate_topology_coadaptation_matrix_v7(payload)
    assert validated["contractSha256"] == payload["contractSha256"]
    for receipt, slot in zip(validated["materializationReceipts"], validated["slots"], strict=True):
        assert receipt["receiptId"] == slot["slotId"]
    complete = [block for block in validated["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE]
    assert len(complete) >= 2
    receipts = {(item["blockId"], item["arm"]): item for item in validated["materializationReceipts"]}
    for block in complete:
        combined = receipts[(block["blockId"], ARM_TE)]
        assert combined["nativeValidationRan"] is True
        assert combined["frozenPairIdentitySha256"] == combined["canonicalCompiledIdentities"]["frozenPairIdentitySha256"]
        assert "pairIdentitySha256" not in combined
        parent = receipts[(block["blockId"], ARM_P)]
        assert parent["compiledV3MatchesHistoricalParent"] is True
    fake = apply_topology_parity_mutation_v7(payload, "fake_pair_native_report")
    with pytest.raises(TemporalDiscoveryContractError, match="frozenPairIdentitySha256|canonical compiled"):
        validate_topology_coadaptation_matrix_v7(fake)
    drift = apply_topology_parity_mutation_v7(payload, "receipt_id_drift_only")
    with pytest.raises(TemporalDiscoveryContractError, match="receiptId must equal slotId"):
        validate_topology_coadaptation_matrix_v7(drift)


def test_adversarial_parity_mutations_all_reject() -> None:
    _require_v7()
    payload = json.loads(V7_SPEC.read_text(encoding="utf-8"))
    validate_topology_coadaptation_matrix_v7(payload)
    for mutation_id in PARITY_MUTATION_IDS:
        mutated = apply_topology_parity_mutation_v7(payload, mutation_id)
        with pytest.raises(TemporalDiscoveryContractError):
            validate_topology_coadaptation_matrix_v7(mutated)


def test_archive_direction_is_not_defaulted_and_density_median_bind() -> None:
    _require_v7()
    archive = json.loads(V7_ARCHIVE.read_text(encoding="utf-8"))
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
    binding = archive["frozenV38PolicyBinding"]
    assert binding["rotatingEvidencePinVerified"] is True
    assert binding["missingPolicyShaReportedAsUnavailableNotDefaulted"] is True
    assert binding["liveModuleDefaultsAreNotSubstitutedAsFrozenV38Pins"] is True
    assert binding["archivePolicySha256"] == "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da"
    assert binding["rotatingEvidenceSha256"] == "sha256:10d0cdeb60433b452af475f451fc4782f7a26e24210f3cb76e62d8a08127f1bb"
    assert binding["robustBreederPolicySha256"] == "sha256:eb2207df0e0082b9aad33c11ec1ec743d0cf467baf6ffabdaa519081c2399c96"
    assert binding["directionPolicySha256"] == "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1"
    assert binding["cumulativeArchiveSha256"] == "sha256:c95ae5ebcffc731bbe67017fc3bff68ae0979249e5b2dd19c9ca38822853dfd8"


def test_topology_and_resource_authorities_are_separate_and_unlaunched() -> None:
    _require_v7()
    inspected = json.loads(V7_INSPECTED.read_text(encoding="utf-8"))
    confirm = json.loads(V7_CONFIRM.read_text(encoding="utf-8"))
    one = json.loads(V7_ONE.read_text(encoding="utf-8"))
    two = json.loads(V7_TWO.read_text(encoding="utf-8"))
    pair = json.loads(V7_PAIR.read_text(encoding="utf-8"))
    matrix = json.loads(V7_MATRIX.read_text(encoding="utf-8"))
    go = json.loads(V7_GO.read_text(encoding="utf-8"))
    spec = json.loads(V7_SPEC.read_text(encoding="utf-8"))
    complete = sum(1 for block in spec["blocks"] if block["classification"] == BLOCK_CLASS_COMPLETE)
    expected_pairs = complete * 4
    expected_tasks = expected_pairs * 12
    assert complete >= 2
    assert inspected["canonicalPairCandidateCount"] == expected_pairs
    assert inspected["projectedInspectedPanelWorkerTasks"] == expected_tasks
    assert inspected["doNotLaunch"] is True
    assert inspected["executedInThisTask"] is False
    assert confirm["projectedConfirmationWorkerTasks"] == expected_pairs * 4
    assert confirm["futureConfirmationPanel"]["windowIdentitiesBound"] is False
    assert confirm["futureConfirmationPanel"]["panel4IsLatinSquareNotOuterTail"] is True
    assert confirm["mustNotReuseResourceInventoryAsTopologyAuthority"] is True
    assert one["pairCandidateCount"] == 63
    assert one["projectedInspectedPanelWorkerTasks"] == 756
    assert one["projectedWithFutureConfirmationPanel"] == 1008
    assert one["coverageKind"] == "deterministic_case_study_coverage_not_repeatability"
    assert two["mutationPairCandidateCount"] == 112
    assert two["pairCandidateCount"] == 117
    assert two["projectedInspectedPanelWorkerTasks"] == 1404
    assert two["projectedWithFutureConfirmationPanel"] == 1872
    assert pair["frozenPairCompileRan"] is True
    assert pair["nativeValidationRan"] is True
    assert pair["canonicalPairCount"] == expected_pairs
    assert pair["doNotLaunch"] is True
    assert matrix["taskCount"] == expected_tasks
    assert matrix["dispatched"] is False
    assert matrix["executedInThisTask"] is False
    assert go["dispatched"] is False
    assert go["marketEvaluationLaunched"] is False
    assert go["generationLaunched"] is False
    if complete != 3:
        assert go["verdict"] == "no-go"
        assert go["readyForTopologyCaseStudyLaunch"] is False
    assert projected_inspected_panel_worker_tasks(pair_candidate_count=12) == 12 * WINDOWS_PER_PANEL * 3


def test_frozen_pair_payloads_replay() -> None:
    _require_v7()
    if not V7_PAYLOADS.is_file():
        pytest.skip("v7 FrozenPair payloads are not on disk")
    payloads = json.loads(V7_PAYLOADS.read_text(encoding="utf-8"))
    receipt_payloads = payloads["receiptPayloads"]
    spec = json.loads(V7_SPEC.read_text(encoding="utf-8"))
    complete_ids = {
        receipt["receiptId"]
        for receipt, block in (
            (
                item,
                next(b for b in spec["blocks"] if b["blockId"] == item["blockId"]),
            )
            for item in spec["materializationReceipts"]
        )
        if block["classification"] == BLOCK_CLASS_COMPLETE
    }
    assert set(receipt_payloads) == complete_ids
    for receipt_id, payload in receipt_payloads.items():
        replayed = FrozenPair.from_payload(payload)
        assert replayed.identity_sha256 == payload["identities"]["pairIdentitySha256"]
        assert replayed.canonical_payload() == payload


def test_standalone_receipt_set_hashes_and_overlay_stays_inert() -> None:
    _require_v7()
    receipts = json.loads(V7_RECEIPTS.read_text(encoding="utf-8"))
    body = {key: value for key, value in receipts.items() if key != "receiptSetSha256"}
    assert receipts["receiptSetSha256"] == canonical_sha256(body)
    payload = json.loads(V7_SPEC.read_text(encoding="utf-8"))
    assert topology_coadaptation_v7_from_config({"schemaVersion": PAIR_GENERATION_SCHEMA}) is None
    attached = attach_topology_coadaptation_matrix_v7(
        {"schemaVersion": PAIR_GENERATION_SCHEMA, "configSha256": "x"},
        payload,
    )
    unsigned = {key: value for key, value in attached.items() if key != "configSha256"}
    assert attached["configSha256"] == canonical_sha256(unsigned)
    corpus = json.loads(V7_CORPUS.read_text(encoding="utf-8"))
    assert corpus["canonicalFixtureAccepted"] is True
    assert corpus["adversarialCount"] == len(PARITY_MUTATION_IDS)


@pytest.mark.skipif(os.environ.get("FUZZFOLIO_V7_DOUBLE_EMIT") != "1", reason="native FrozenPair compile is slow")
def test_emit_twice_is_byte_identical(tmp_path: Path) -> None:
    if not V6_SPEC.is_file() or not V4_SPEC.is_file():
        pytest.skip("frozen v4/v6 reports are not on disk")
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
            "topology-coadaptation-matrix-spec-v6.json",
        ):
            source = V7_DIR / name
            (target / name).write_bytes(source.read_bytes())
    written_a = run_audit_v7(output_dir=first)
    written_b = run_audit_v7(output_dir=second)
    assert set(written_a) == set(written_b)
    for name in written_a:
        if name == "README.md":
            continue
        left = written_a[name].read_bytes()
        right = written_b[name].read_bytes()
        assert left == right, name
    spec_a = json.loads((first / "topology-coadaptation-matrix-spec-v7.json").read_text(encoding="utf-8"))
    spec_b = json.loads((second / "topology-coadaptation-matrix-spec-v7.json").read_text(encoding="utf-8"))
    assert spec_a["contractSha256"] == spec_b["contractSha256"]
    _ = canonical_json
