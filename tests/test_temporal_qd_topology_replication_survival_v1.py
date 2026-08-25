from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_topology_replication_survival_v1 import (
    build_replication_survival_rule,
    evaluate_replication_survival,
    validate_replication_survival_rule,
)

ROOT = Path(__file__).resolve().parents[1]
AUTHORITY = ROOT / "research" / "temporal-qd" / "rust-canonical-authority-v2-3"


def test_exact_three_panel_truth_table_is_strict_all() -> None:
    for development, panel_1, panel_2 in itertools.product((False, True), repeat=3):
        result = evaluate_replication_survival(
            {"panel-3": development, "panel-1": panel_1, "panel-2": panel_2}
        )
        assert result["inspectedPromising"] is (development and panel_1 and panel_2)
        assert result["replicationSurviving"] is (panel_1 and panel_2)


@pytest.mark.parametrize(
    "values,valid",
    [
        ({"panel-3": True, "panel-1": True}, True),
        ({"panel-3": True, "panel-1": True, "panel-2": None}, True),
        ({"panel-3": True, "panel-1": True, "panel-2": True}, False),
    ],
)
def test_missing_invalid_or_identity_drift_is_incomplete_not_observed_failure(
    values: dict[str, bool | None], valid: bool
) -> None:
    result = evaluate_replication_survival(values, identities_valid=valid)
    assert result["reportingCategory"] == "incomplete_invalid"
    assert result["evidenceCompleteAndIdentityValid"] is False
    assert result["inspectedPromising"] is False


def test_rule_is_self_hashed_and_tamper_evident() -> None:
    rule = build_replication_survival_rule(scientific_contract_sha256="sha256:" + "a" * 64)
    validate_replication_survival_rule(rule)
    rule["crossPanelOperator"] = "any"
    with pytest.raises(TemporalDiscoveryContractError, match="self-hash"):
        validate_replication_survival_rule(rule)


def test_committed_rule_and_corpus_are_bound_and_complete() -> None:
    rule = json.loads((AUTHORITY / "topology-replication-survival-rule-v1.json").read_text())
    corpus = json.loads((AUTHORITY / "topology-replication-survival-corpus-v1.json").read_text())
    validate_replication_survival_rule(rule)
    assert corpus["replicationRuleSha256"] == rule["replicationRuleSha256"]
    assert len(corpus["cases"]) == 11
    assert {case.get("caseId") for case in corpus["cases"] if case.get("caseId")} == {
        "missing-panel-2",
        "null-panel-1",
        "identity-drift",
    }


def test_committed_production_launch_control_binds_three_exact_panels() -> None:
    control = json.loads((AUTHORITY / "topology-production-launch-control-v1.json").read_text())
    mapping = json.loads((AUTHORITY / "topology-production-task-mapping-v1.json").read_text())
    outputs = json.loads((AUTHORITY / "topology-production-output-templates-v1.json").read_text())
    unsigned_control = dict(control)
    assert unsigned_control.pop("launchControlSha256") == canonical_sha256(unsigned_control)
    unsigned_mapping = dict(mapping)
    assert unsigned_mapping.pop("mappingSha256") == canonical_sha256(unsigned_mapping)
    assert control["dispatchEnabled"] is False
    assert control["panelTaskCounts"] == [48, 48, 48]
    assert control["totalInspectedTaskCount"] == 144
    assert [row["panelId"] for row in control["panels"]] == ["panel-1", "panel-2", "panel-3"]
    assert mapping["mappedTaskCount"] == len(mapping["mappings"]) == 144
    assert len({row["oldTaskId"] for row in mapping["mappings"]}) == 144
    assert len({row["newTaskId"] for row in mapping["mappings"]}) == 144
    unsigned_outputs = dict(outputs)
    assert unsigned_outputs.pop("templatesSha256") == canonical_sha256(unsigned_outputs)
    assert outputs["dispatchEnabled"] is False
    assert [row["panelId"] for row in outputs["templates"]] == ["panel-1", "panel-2", "panel-3"]
    assert all(row["cohortSource"]["candidateCount"] == 12 for row in outputs["templates"])


def test_committed_v2_3_launch_gate_is_self_hashed_and_complete() -> None:
    gate = json.loads((AUTHORITY / "topology-production-launch-gate-v1.json").read_text())
    unsigned = dict(gate)
    assert unsigned.pop("launchGateSha256") == canonical_sha256(unsigned)
    assert gate["dispatchEnabled"] is False
    assert gate["untouchedConfirmationStatus"] == "pending"
    assert gate["readyForAuthorizedTopologyCaseStudyLaunch"] is True
    assert all(gate["gates"].values())
