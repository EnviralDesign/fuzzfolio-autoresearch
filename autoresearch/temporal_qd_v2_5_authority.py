"""Build the versioned V2.5 panel policy and reducer contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_production_reducer_v2 import (
    REPLICATION_RULE_SHA256,
    SCIENTIFIC_SHA256,
)
from .temporal_qd_topology_production_reducer_v3 import (
    CONTRACT_SCHEMA,
    GRAPH_SCHEMA,
    POLICY_SCHEMA,
    SCHEMA,
)
from .temporal_qd_topology_panel_usefulness_v2 import evaluate_replication_survival_v3


def _raw_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def build_panel_policy() -> dict[str, Any]:
    policy: dict[str, Any] = {
        "schemaVersion": POLICY_SCHEMA,
        "originalScientificContractSha256": SCIENTIFIC_SHA256,
        "originalReplicationRuleSha256": REPLICATION_RULE_SHA256,
        "archivePolicyName": "stage5e7_v5_direction_aware_breeding_archive",
        "archivePolicySha256": "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da",
        "directionSelectionPolicySha256": "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1",
        "supportThresholds": {
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
        },
        "qualityLane": "finite_support_and_nonnegative_robust_return",
        "eligibilityGateScope": "TE_only",
        "controlEligibilityDisposition": "diagnostic_nonveto",
        "panelLocalPredicate": "U_v2",
        "economicComparisons": ["TE_net_gt_P", "TE_net_gt_T", "TE_net_gt_E"],
        "riskComparisons": [
            "TE_worst_not_worse_than_P",
            "TE_worst_not_worse_than_T",
            "TE_worst_not_worse_than_E",
        ],
        "crossPanelRule": "panel-3_AND_panel-1_AND_panel-2",
        "untouchedConfirmation": "pending_same_U_v2_exact_block",
        "dispatchEnabled": False,
    }
    policy["panelUsefulnessPolicySha256"] = canonical_sha256(policy)
    return policy


def build_parity_corpus(repo_root: Path) -> dict[str, Any]:
    truth_cases = []
    for panel_3 in (False, True):
        for panel_1 in (False, True):
            for panel_2 in (False, True):
                inputs = {
                    "panel-3": panel_3,
                    "panel-1": panel_1,
                    "panel-2": panel_2,
                }
                truth_cases.append(
                    {
                        "caseId": f"truth-{int(panel_3)}{int(panel_1)}{int(panel_2)}",
                        "inputs": inputs,
                        "identitiesValid": True,
                        "expected": evaluate_replication_survival_v3(
                            inputs, identities_valid=True
                        ),
                    }
                )
    for case_id, inputs, identities_valid in (
        ("missing-panel", {"panel-3": True, "panel-1": True}, True),
        (
            "identity-drift",
            {"panel-3": True, "panel-1": True, "panel-2": True},
            False,
        ),
    ):
        truth_cases.append(
            {
                "caseId": case_id,
                "inputs": inputs,
                "identitiesValid": identities_valid,
                "expected": evaluate_replication_survival_v3(
                    inputs, identities_valid=identities_valid
                ),
            }
        )
    test_path = repo_root / "tests" / "test_temporal_qd_topology_panel_usefulness_v2.py"
    corpus: dict[str, Any] = {
        "schemaVersion": "temporal_qd_topology_policy_parity_corpus_v2",
        "equality": "complete_canonical_result_object",
        "pythonRustExecutableTest": {
            "path": test_path.relative_to(repo_root).as_posix(),
            "rawSha256": _raw_sha(test_path),
            "expectedCaseCount": 33,
        },
        "armEligibilityCases": [
            "eligible_specialist",
            "changed_side_active_pair_ineligible",
            "changed_side_inactive_opposite_specialist_eligible",
            "validForQuality_true_negative_robust_return",
            "support_failure",
            "materially_harmful_opposite_side",
        ],
        "panelLocalCases": [
            "eligible",
            "ineligible_control_with_eligible_TE",
            "TE_support_failure",
            "TE_quality_failure",
            "TE_direction_failure",
            "exact_tie",
            "numerical_dust_below_1e-12",
            "risk_tradeoff",
        ],
        "structuralFailClosedCases": [
            "missing_panel",
            "duplicate_panel",
            "duplicate_arm",
            "identity_drifted_panel",
            "nonfinite_comparison",
        ],
        "prohibitedCrossPanelOperators": [
            "pooling",
            "averaging",
            "compensation",
            "majority_vote",
            "untouched_confirmation_rescue",
        ],
        "crossPanelTruthAndIncompleteCases": truth_cases,
    }
    corpus["parityCorpusSha256"] = canonical_sha256(corpus)
    return corpus


def build_reducer_contract(
    repo_root: Path, policy: dict[str, Any], parity_corpus: dict[str, Any]
) -> dict[str, Any]:
    paths = (
        repo_root / "autoresearch" / "temporal_qd_topology_production_reducer_v3.py",
        repo_root / "autoresearch" / "temporal_qd_topology_panel_usefulness_v2.py",
        repo_root / "autoresearch" / "temporal_qd_evolution.py",
        repo_root / "autoresearch" / "temporal_direction_selection.py",
        repo_root / "rust" / "temporal-qd" / "crates" / "qd-campaign-freeze" / "src" / "lib.rs",
        repo_root / "rust" / "temporal-qd" / "crates" / "qd-campaign-seal" / "src" / "campaign_output.rs",
        repo_root / "rust" / "temporal-qd" / "crates" / "qd-kernel" / "src" / "topology_panel_usefulness_v2.rs",
    )
    contract: dict[str, Any] = {
        "schemaVersion": CONTRACT_SCHEMA,
        "analysisSchema": SCHEMA,
        "authenticatedGraphSchema": GRAPH_SCHEMA,
        "panelUsefulnessPolicySha256": policy["panelUsefulnessPolicySha256"],
        "parityCorpusSha256": parity_corpus["parityCorpusSha256"],
        "originalScientificContractSha256": SCIENTIFIC_SHA256,
        "originalReplicationRuleSha256": REPLICATION_RULE_SHA256,
        "archivePolicySha256": policy["archivePolicySha256"],
        "directionSelectionPolicySha256": policy["directionSelectionPolicySha256"],
        "supportThresholds": policy["supportThresholds"],
        "panelLocalPredicate": "U_v2",
        "eligibilityGateScope": "TE_only",
        "controlEligibilityDisposition": "diagnostic_nonveto",
        "primaryInputs": "exact_three_authenticated_campaign_output_graph_v2_checkpoints_plus_frozen_v2_3_and_v2_5_authorities",
        "callerComputedMetricDictionariesAccepted": False,
        "callerIdentityValidityBooleanAccepted": False,
        "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
        "fixedPnlMarginPermitted": False,
        "crossPanelPoolingPermitted": False,
        "crossPanelCompensationPermitted": False,
        "typedUnavailableMechanismEvidenceRequired": True,
        "untouchedConfirmationRequired": True,
        "mechanismSchema": {
            "closedTradeCountChangeVersusP": "total_closed_trade_count_difference",
            "entrySequenceComparison": "raw_attributed_sequences_without_shift_claim",
            "changedSideTransitionDistribution": "complete_changed_side_distribution",
            "eventSpecificActivation": "unavailable_without_event_guard_or_transition_identity",
        },
        "sources": [
            {"path": path.relative_to(repo_root).as_posix(), "rawSha256": _raw_sha(path)}
            for path in paths
        ],
    }
    contract["reducerContractSha256"] = canonical_sha256(contract)
    return contract


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--policy-output", type=Path, required=True)
    parser.add_argument("--contract-output", type=Path, required=True)
    parser.add_argument("--parity-output", type=Path, required=True)
    args = parser.parse_args()
    policy = build_panel_policy()
    parity = build_parity_corpus(args.repo_root.resolve())
    contract = build_reducer_contract(args.repo_root.resolve(), policy, parity)
    args.policy_output.write_text(canonical_json(policy) + "\n", encoding="utf-8", newline="\n")
    args.contract_output.write_text(canonical_json(contract) + "\n", encoding="utf-8", newline="\n")
    args.parity_output.write_text(canonical_json(parity) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
