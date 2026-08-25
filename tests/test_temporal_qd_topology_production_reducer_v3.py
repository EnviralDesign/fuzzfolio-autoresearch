from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_topology_production_reducer_v3 import reduce_files_v3
from autoresearch.temporal_qd_v2_5_launch_gate import build_gate

ROOT = Path(__file__).resolve().parents[1]
AUTHORITY = ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-3"
V2_5 = ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-5"
SCIENCE = ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2" / "topology-scientific-contract-v1.json"
OPENER = ROOT / "rust" / "temporal-qd" / "target" / "debug" / "temporal-qd-campaign-output-graph-v2-json.exe"


def _paths(proof: Path) -> list[Path]:
    return [
        proof / f"panel-{panel}" / "campaign-output-local" / "campaign-output-checkpoint.json"
        for panel in (1, 2, 3)
    ]


def _reduce(proof: Path, *, checkpoints: list[Path] | None = None, policy: Path | None = None) -> dict:
    return reduce_files_v3(
        checkpoints=checkpoints or _paths(proof),
        opener=OPENER,
        launch_control_path=AUTHORITY / "topology-production-launch-control-v1.json",
        task_mapping_path=AUTHORITY / "topology-production-task-mapping-v1.json",
        replication_rule_path=AUTHORITY / "topology-replication-survival-rule-v1.json",
        scientific_contract_path=SCIENCE,
        analyzer_contract_path=AUTHORITY / "topology-post-run-analyzer-contract-v1.json",
        panel_policy_path=policy or V2_5 / "topology-panel-usefulness-policy-v2.json",
    )


def _proof_root() -> Path:
    value = os.getenv("FUZZFOLIO_V2_5_INTEGRATION_ROOT")
    if not value:
        pytest.skip("audit-only V2.5 production proof root not supplied")
    return Path(value)


def test_v2_5_authorities_are_self_hashed_and_historical_contracts_unchanged() -> None:
    for path, field in (
        (V2_5 / "topology-panel-usefulness-policy-v2.json", "panelUsefulnessPolicySha256"),
        (V2_5 / "topology-production-reducer-contract-v3.json", "reducerContractSha256"),
        (V2_5 / "topology-policy-parity-corpus-v2.json", "parityCorpusSha256"),
        (V2_5 / "topology-v2-5-cross-root-report.json", "crossRootReportSha256"),
        (V2_5 / "topology-production-launch-gate-v2-5.json", "launchGateSha256"),
    ):
        value = json.loads(path.read_text(encoding="utf-8"))
        stored = value.pop(field)
        assert stored == canonical_sha256(value)
    assert (AUTHORITY / "topology-replication-survival-rule-v1.json").exists()
    assert (ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-4" / "topology-production-launch-gate-v2-4.json").exists()


def test_exact_three_panel_v2_5_reducer_and_mechanism_schema() -> None:
    result = _reduce(_proof_root())
    assert result["status"] == "complete"
    assert result["analysisSha256"] == "sha256:041dd9dc47d09775a21a1c9790032018a49acd7e67652549a20078b57da7553a"
    assert len(result["authenticatedPanels"]) == 3
    assert len(result["blocks"]) == 3
    for block in result["blocks"].values():
        assert block["replication"]["panelLocalPredicate"] == "U_v2"
        for panel in block["panelReports"].values():
            assert panel["comparisonEvidenceComplete"] is True
            for arm in ("P", "T", "E", "TE"):
                row = panel["mechanism"][arm]
                assert "closedTradeCountChangeVersusP" in row
                assert "entrySequenceComparison" in row
                assert "changedSideTransitionDistribution" in row
                assert row["eventSpecificActivation"]["status"] == "unavailable"
                assert "changedTradeOpportunityCountVersusP" not in row
                assert "entryTimingShiftVersusP" not in row
                assert "routeEventActivation" not in row


def test_v2_5_reducer_is_deterministic_and_missing_panel_fails_closed() -> None:
    proof = _proof_root()
    first = _reduce(proof)
    second = _reduce(proof)
    assert first == second
    invalid = _reduce(proof, checkpoints=_paths(proof)[:2])
    assert invalid["status"] == "incomplete_invalid"


def test_v2_5_policy_drift_fails_closed(tmp_path: Path) -> None:
    proof = _proof_root()
    policy = json.loads((V2_5 / "topology-panel-usefulness-policy-v2.json").read_text())
    policy["qualityLane"] = "validForQuality_only"
    policy.pop("panelUsefulnessPolicySha256")
    policy["panelUsefulnessPolicySha256"] = canonical_sha256(policy)
    path = tmp_path / "drifted-policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    assert _reduce(proof, policy=path)["status"] == "incomplete_invalid"


def test_v2_5_cross_root_analysis_is_exact() -> None:
    left = _proof_root()
    right_value = os.getenv("FUZZFOLIO_V2_5_SECOND_ROOT")
    if not right_value:
        pytest.skip("second audit-only V2.5 proof root not supplied")
    assert _reduce(left) == _reduce(Path(right_value))


def test_v2_5_launch_gate_recomputes_without_market_dispatch() -> None:
    proof = _proof_root()
    gate = build_gate(
        repo_root=ROOT,
        authority_root=AUTHORITY,
        v2_5_authority_root=V2_5,
        proof_root=proof,
        opener=OPENER,
        cross_root_report_path=V2_5 / "topology-v2-5-cross-root-report.json",
        prior_v2_4_gate_path=proof / "topology-production-launch-gate-v2-4.json",
    )
    assert gate["readyForAuthorizedTopologyCaseStudyLaunch"] is True
    assert all(gate["gates"].values())
    assert gate["dispatchEnabled"] is False
    assert gate["untouchedConfirmationStatus"] == "pending"
