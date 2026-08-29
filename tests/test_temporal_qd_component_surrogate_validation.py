from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_component_surrogate_validation import (
    ValidationError,
    run_validation,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _report(payload: dict) -> dict:
    material = dict(payload)
    material["reportSha256"] = canonical_sha256(payload)
    return material


def _candidate(candidate_id: str, *, parent: bool = False) -> dict:
    profile = {
        "graph": {
            "eventBindings": [
                {
                    "id": "short_evt_alpha",
                    "indicatorInstanceId": "short_evtind_alpha",
                    "longOutput": "bullish",
                    "shortOutput": "bearish",
                }
            ],
            "transitions": [
                {"id": "short_setup", "guard": {"eventBindingId": "short_evt_alpha"}}
            ],
        },
        "indicators": [
            {
                "meta": {
                    "id": "TEST_EVENT",
                    "baseIndicatorId": "TEST_BASE",
                    "instanceId": "short_evtind_alpha",
                    "signalPersistence": "event-with-lookback",
                    "signalRole": "trigger",
                },
                "config": {"timeframe": "M5", "lookbackBars": 1, "useFormingBar": False},
            }
        ],
    }
    return {
        "candidateId": candidate_id,
        "aggregate": {
            "behaviorIdentitySha256": f"sha256:{'b' if parent else 'c'}" + "0" * 57,
        },
        "candidate": {
            "candidateId": candidate_id,
            "candidateIdentitySha256": f"sha256:{'a' if parent else 'd'}" + "0" * 57,
            "programSha256": "sha256:program",
            "profileSnapshotSha256": "sha256:profile",
            "sourceProfileSha256": "sha256:source",
            "proposalEntrySha256": "sha256:proposal",
            "sourceProfile": profile,
            "structuralOperatorHistory": [
                {
                    "operation": "directional_event_insert",
                    "terminalOperatorApplicationSha256": "sha256:application",
                    "terminalOperatorPlanSha256": "sha256:plan",
                }
            ],
        },
    }


def _inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    case = {
        "candidateId": "child",
        "parentCandidateId": "parent",
        "eventId": "evt_alpha",
        "indicatorId": "TEST_EVENT",
        "indicatorInstanceId": "evtind_alpha",
        "side": "short",
        "authoredProgramSha256": "sha256:program",
        "resolvedProgramSha256": "sha256:resolved",
        "phenotypeIdentitySha256": "sha256:phenotype",
        "windowIdentities": ["panel-3-window"],
        "metrics": {
            "tradeCount": 2,
            "medianWindowConservativeNetR": 1.0,
            "combinedSupportPass": False,
            "directionEligible": False,
            "currentPanelQualityLike": False,
            "currentPanelFrontierLike": False,
            "economicsBasis": "frozen",
        },
        "parentMetrics": {"medianWindowConservativeNetR": 1.0},
        "deltas": {
            "grossNoCostNetR": 0.0,
            "costDragR": -1.0,
            "cumulativeConservativeNetR": 1.0,
            "tradeCount": -4.0,
            "worstWindowConservativeNetR": 0.5,
        },
        "relative": {
            "comparable": True,
            "beatParent": True,
            "lostToParent": False,
            "fullEconomicPhenotypeTie": False,
        },
    }
    forensic = _report({"acceptedChildren": 1, "archiveParentComparable": 1, "cases": [case]})
    multi = _report(
        {
            "developmentPanelId": "panel-3",
            "selectionBiasCaveat": "selected",
            "replicationRole": "retrospective",
            "replicationPanelIds": ["panel-1", "panel-2"],
            "sameChildAbsolutePositiveOnBothPanel1AndPanel2": 0,
            "sameChildParentSuperiorOnBothPanel1AndPanel2": 0,
            "sameChildRiskQualifiedParentSuperiorOnBothPanel1AndPanel2": 0,
            "sameChildSupportAndDirectionOnBothPanel1AndPanel2": 0,
            "childrenSurvivingFinalCumulativeArchive": 0,
            "children": [{"candidateId": "child", "parentCandidateId": "parent", "panels": {}}],
        }
    )
    forensic_path = tmp_path / "forensic.json"
    multi_path = tmp_path / "multi.json"
    members_path = tmp_path / "members.jsonl"
    _write_json(forensic_path, forensic)
    _write_json(multi_path, multi)
    members_path.write_text(
        "\n".join(json.dumps(row) for row in (_candidate("child"), _candidate("parent", parent=True)))
        + "\n",
        encoding="utf-8",
    )
    return forensic_path, multi_path, members_path


def test_component_validation_is_deterministic_and_declares_missing_score_inputs(tmp_path: Path) -> None:
    forensic, multi, members = _inputs(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    summary = run_validation(
        event_forensic_path=forensic,
        multipanel_path=multi,
        evaluated_members_path=members,
        output_dir=first,
    )
    run_validation(
        event_forensic_path=forensic,
        multipanel_path=multi,
        evaluated_members_path=members,
        output_dir=second,
    )

    assert summary["decision"]["taxonomy"] == "insufficient_retrospective_evidence"
    outcome = json.loads((first / "realized-outcomes.jsonl").read_text(encoding="utf-8"))
    assert outcome["deltas"]["deltaNetR"] == 1.0
    assert outcome["mechanismClassification"] == "cost_suppression_only"
    score = json.loads((first / "frozen-surrogate-scores.jsonl").read_text(encoding="utf-8"))
    assert score["status"] == "unavailable"
    assert (first / "CHECKSUMS.sha256").read_bytes() == (second / "CHECKSUMS.sha256").read_bytes()
    for name in (first / "CHECKSUMS.sha256").read_text(encoding="utf-8").splitlines():
        _, filename = name.split("  ")
        assert (first / filename).read_bytes() == (second / filename).read_bytes()


def test_component_validation_rejects_tampered_retained_report(tmp_path: Path) -> None:
    forensic, multi, members = _inputs(tmp_path)
    payload = json.loads(forensic.read_text(encoding="utf-8"))
    payload["acceptedChildren"] = 2
    _write_json(forensic, payload)

    with pytest.raises(ValidationError, match="report hash mismatch"):
        run_validation(
            event_forensic_path=forensic,
            multipanel_path=multi,
            evaluated_members_path=members,
            output_dir=tmp_path / "out",
        )
