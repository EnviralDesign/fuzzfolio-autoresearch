from __future__ import annotations

import json
from pathlib import Path

from autoresearch.temporal_qd_v37_archive_disposition_ledger import (
    _canonical_sha256,
    build_v37_archive_disposition_ledger,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _candidate(candidate_id: str, *, net: float, supported: bool) -> dict:
    windows = [
        {"windowId": f"w-{index}", "conservativeNetR": net / 4, "noCostNetR": net / 4 + 0.1, "trades": 4}
        for index in range(4)
    ]
    return {
        "candidateId": candidate_id,
        "generationIndex": 1,
        "candidate": {
            "candidateId": candidate_id,
            "candidateIdentitySha256": f"sha256:{candidate_id}",
            "programSha256": f"sha256:program-{candidate_id}",
            "profileSnapshotSha256": f"sha256:profile-{candidate_id}",
            "sourceMode": "qd_random_immigrant_bidirectional_pair",
            "structuralOperatorHistory": [],
        },
        "aggregate": {
            "totalConservativeNetR": net,
            "totalNoCostNetR": net + 0.4,
            "costDragR": 0.4,
            "totalTrades": 16,
            "windowRecords": windows,
        },
        "finiteDataValidity": {"passesSupportGate": supported, "validForQuality": supported},
        "descriptor": {"cellId": "cell-a"},
        "objectives": {"worstWindowConservativeNetR": net / 4},
    }


def _provisional(candidate_id: str) -> dict:
    return {
        "candidateId": candidate_id,
        "candidateIdentitySha256": f"sha256:{candidate_id}",
        "programSha256": f"sha256:program-{candidate_id}",
        "profileSnapshotSha256": f"sha256:profile-{candidate_id}",
        "cellId": "cell-a",
        "currentPanelRank": 1.0,
        "novelty": 1.0,
    }


def _cumulative(candidate_id: str, *, lane: str, eligible: bool) -> dict:
    return {
        "candidateId": candidate_id,
        "robustBreederLane": lane,
        "robustBreederEligible": eligible,
    }


def test_joins_current_candidates_to_native_terminal_outcomes(tmp_path: Path) -> None:
    root = tmp_path / "v37"
    generation = root / "run" / "broad-4000x1024x5" / "generations" / "generation-0001"
    rows = [_candidate("a", net=1.0, supported=True), _candidate("b", net=-1.0, supported=False), _candidate("c", net=1.0, supported=False)]
    member_path = generation / "campaign" / "proposal-current-panel" / "campaign-output" / "evaluated-members.jsonl"
    member_path.parent.mkdir(parents=True, exist_ok=True)
    member_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    bundle_rows = [
        {
            "candidateId": candidate_id,
            "panelId": "panel-1",
            "bundleSha256": f"sha256:bundle-{candidate_id}",
        }
        for candidate_id in ("a", "b", "c")
    ]
    member_path.with_name("candidate-panel-bundles.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in bundle_rows), encoding="utf-8"
    )
    funnel = []
    for candidate_id in ("a", "b", "c"):
        funnel.append({"disposition": "accepted", "originKind": "random_immigrant", "candidate": {"candidateId": candidate_id}, "funnelCandidate": {"staticReachability": {"outcome": "reachable"}, "nativeValidation": {"outcome": "valid"}}})
    _write_json(generation / "proposal" / "evaluation-population.json", {"funnelEntries": funnel})
    provisional = [_provisional("a"), _provisional("c")]
    rich = {"members": [rows[0], rows[2]]}
    _write_json(
        generation / "native-finalization" / "source.json",
        {
            "cohort": {
                "candidates": [{"candidateId": candidate_id} for candidate_id in ("a", "b", "c")],
                "panelId": "panel-1",
                "retainedParentEvaluationCandidateIds": [],
            },
            "provisional": {"candidates": provisional},
            "selectedRichMembers": rich,
            "candidatePanelBundles": [bundle_rows[0], bundle_rows[2]],
            "archivePolicy": {"frozenPolicy": {"directionSelection": {"selectionPolicy": {"minimum_closed_trades_per_side": 1, "minimum_active_windows_per_side": 1, "minimum_acceptable_side_net_r": 0.0, "harmful_opposite_net_r": -0.25}}}},
        },
    )
    _write_json(
        generation / "native-finalization" / "evidence" / "cumulative-archive.json",
        {"members": [_cumulative("a", lane="quality", eligible=True), _cumulative("c", lane="unsupported", eligible=False)], "qualityCandidateIds": ["a"], "frontierCandidateIds": []},
    )
    _write_json(generation / "native-finalization" / "archive.json", {"cells": [{"members": [{"candidateId": "a", "archiveLane": "quality", "retentionReason": "cumulative_robust_quality", "descriptor": {"cellId": "cell-a"}}]}]})

    output = tmp_path / "out"
    summary = build_v37_archive_disposition_ledger(v37_root=root, output_dir=output)

    ledger = [json.loads(line) for line in (output / "candidate-disposition-ledger.jsonl").read_text(encoding="utf-8").splitlines()]
    rollup = [json.loads(line) for line in (output / "candidate-lineage-rollup.jsonl").read_text(encoding="utf-8").splitlines()]
    assert summary["evaluationStateCount"] == 3
    assert summary["proposalCurrentPanelCandidateCount"] == 3
    assert summary["retainedParentReevaluationStateCount"] == 0
    assert len(rollup) == 3
    assert {row["candidateId"]: row["terminalReason"] for row in ledger} == {
        "a": "parent_archive_admitted",
        "b": "prefinalizer_newcomer_cap",
        "c": "cumulative_native_unsupported_reason_unavailable",
    }
    assert all(row["evaluationStateKind"] == "proposal_current_panel" for row in ledger)
    assert all(row["evaluationRecordCanonicalSha256"].startswith("sha256:") for row in ledger)
    assert all(row["coverageCanonicalSha256"].startswith("sha256:") for row in ledger)
    assert all(row["evaluationStateSha256"].startswith("sha256:") for row in ledger)
    assert ledger[0]["firstTerminalStage"] == "parent_archive_admission"
    assert ledger[1]["counterfactualEligibility"] == "exact_retained_required_evidence"


def test_keeps_retained_parent_current_panel_evaluation_as_a_distinct_state(tmp_path: Path) -> None:
    root = tmp_path / "v37"
    generation_1 = root / "run" / "broad-4000x1024x5" / "generations" / "generation-0001"
    generation_2 = root / "run" / "broad-4000x1024x5" / "generations" / "generation-0002"
    parent = _candidate("parent", net=1.0, supported=True)
    newcomer = _candidate("newcomer", net=1.0, supported=True)

    g1_current = generation_1 / "campaign" / "proposal-current-panel" / "campaign-output"
    g1_current.mkdir(parents=True, exist_ok=True)
    (g1_current / "evaluated-members.jsonl").write_text(json.dumps(parent) + "\n", encoding="utf-8")
    (g1_current / "candidate-panel-bundles.jsonl").write_text(
        json.dumps({"candidateId": "parent", "panelId": "panel-1", "bundleSha256": "sha256:parent-p1"}) + "\n",
        encoding="utf-8",
    )
    _write_json(
        generation_1 / "proposal" / "evaluation-population.json",
        {"funnelEntries": [{"disposition": "accepted", "originKind": "random_immigrant", "candidate": {"candidateId": "parent"}, "funnelCandidate": {"staticReachability": {"outcome": "reachable"}, "nativeValidation": {"outcome": "valid"}}}]},
    )
    _write_json(
        generation_1 / "native-finalization" / "source.json",
        {
            "cohort": {"candidates": [{"candidateId": "parent"}], "panelId": "panel-1", "retainedParentEvaluationCandidateIds": []},
            "provisional": {"candidates": [_provisional("parent")]},
            "selectedRichMembers": {"members": [parent]},
            "candidatePanelBundles": [{"candidateId": "parent", "panelId": "panel-1", "bundleSha256": "sha256:parent-p1"}],
        },
    )
    g1_cumulative = {
        "archiveSha256": "sha256:g1-cumulative",
        "members": [_cumulative("parent", lane="quality", eligible=True)],
        "qualityCandidateIds": ["parent"],
        "frontierCandidateIds": [],
        "requiredPanelIds": ["panel-1"],
    }
    _write_json(generation_1 / "native-finalization" / "evidence" / "cumulative-archive.json", g1_cumulative)
    g1_archive = {
        "archiveSha256": "sha256:g1-parent-archive",
        "bidirectionalPairPolicy": {"version": "test"},
        "candidateCountSeen": 1,
        "memberCount": 1,
        "cells": [{"cellId": "cell-a", "members": [{"candidateId": "parent", "archiveLane": "quality", "retentionReason": "cumulative_robust_quality", "descriptor": {"cellId": "cell-a"}}]}],
    }
    _write_json(generation_1 / "native-finalization" / "archive.json", g1_archive)

    g2_current = generation_2 / "campaign" / "proposal-current-panel" / "campaign-output"
    g2_current.mkdir(parents=True, exist_ok=True)
    (g2_current / "evaluated-members.jsonl").write_text(json.dumps(newcomer) + "\n", encoding="utf-8")
    (g2_current / "candidate-panel-bundles.jsonl").write_text(
        json.dumps({"candidateId": "newcomer", "panelId": "panel-2", "bundleSha256": "sha256:newcomer-p2"}) + "\n",
        encoding="utf-8",
    )
    _write_json(
        generation_2 / "proposal" / "evaluation-population.json",
        {"funnelEntries": [{"disposition": "accepted", "originKind": "random_immigrant", "candidate": {"candidateId": "newcomer"}, "funnelCandidate": {"staticReachability": {"outcome": "reachable"}, "nativeValidation": {"outcome": "valid"}}}]},
    )
    retained_output = generation_2 / "campaign" / "fast-prefinalizer" / "round-0000" / "task-0000" / "campaign-output"
    retained_output.mkdir(parents=True, exist_ok=True)
    retained_output.joinpath("evaluated-members.jsonl").write_text(json.dumps(parent) + "\n", encoding="utf-8")
    retained_output.joinpath("candidate-panel-bundles.jsonl").write_text(
        json.dumps({"candidateId": "parent", "panelId": "panel-2", "bundleSha256": "sha256:parent-p2"}) + "\n",
        encoding="utf-8",
    )
    previous_parent_summary = {
        "schemaVersion": "test_parent_summary_v1",
        "archiveSha256": g1_archive["archiveSha256"],
        "bidirectionalPairPolicy": g1_archive["bidirectionalPairPolicy"],
        "candidateCountSeen": g1_archive["candidateCountSeen"],
        "cellIds": ["cell-a"],
        "memberCount": g1_archive["memberCount"],
    }
    previous_parent_summary["summarySha256"] = _canonical_sha256(previous_parent_summary)
    _write_json(
        generation_2 / "native-finalization" / "source.json",
        {
            "cohort": {"candidates": [{"candidateId": "newcomer"}, {"candidateId": "parent"}], "panelId": "panel-2", "retainedParentEvaluationCandidateIds": ["parent"]},
            "previousCumulativeArchive": g1_cumulative,
            "previousParentArchiveSummary": previous_parent_summary,
            "provisional": {"candidates": [_provisional("newcomer"), _provisional("parent")]},
            "selectedRichMembers": {"members": [newcomer, parent]},
            "candidatePanelBundles": [
                {"candidateId": "newcomer", "panelId": "panel-1", "bundleSha256": "sha256:newcomer-p1"},
                {"candidateId": "newcomer", "panelId": "panel-2", "bundleSha256": "sha256:newcomer-p2"},
                {"candidateId": "parent", "panelId": "panel-1", "bundleSha256": "sha256:parent-p1"},
                {"candidateId": "parent", "panelId": "panel-2", "bundleSha256": "sha256:parent-p2"},
            ],
        },
    )
    _write_json(generation_2 / "native-finalization" / "evidence" / "cumulative-archive.json", {"members": [_cumulative("newcomer", lane="quality", eligible=True), _cumulative("parent", lane="unsupported", eligible=False)], "qualityCandidateIds": ["newcomer"], "frontierCandidateIds": []})
    _write_json(generation_2 / "native-finalization" / "archive.json", {"cells": [{"members": [{"candidateId": "newcomer", "archiveLane": "quality", "retentionReason": "cumulative_robust_quality", "descriptor": {"cellId": "cell-a"}}]}]})

    output = tmp_path / "out"
    summary = build_v37_archive_disposition_ledger(v37_root=root, output_dir=output)
    states = [json.loads(line) for line in (output / "candidate-disposition-ledger.jsonl").read_text(encoding="utf-8").splitlines()]
    rollup = {row["candidateId"]: row for row in (json.loads(line) for line in (output / "candidate-lineage-rollup.jsonl").read_text(encoding="utf-8").splitlines())}

    retained_state = next(
        row
        for row in states
        if row["generationIndex"] == 2 and row["candidateId"] == "parent"
    )
    assert summary["evaluationStateCount"] == 3
    assert summary["proposalCurrentPanelCandidateCount"] == 2
    assert summary["retainedParentReevaluationStateCount"] == 1
    assert retained_state["evaluationStateKind"] == "retained_parent_current_panel"
    assert retained_state["funnel"] is None
    assert retained_state["counterfactualEligibility"] == "exact_retained_required_evidence"
    assert retained_state["evidenceCoverage"]["requiredPanelIds"] == ["panel-1", "panel-2"]
    assert rollup["parent"]["evaluationStateCount"] == 2
    continuity = json.loads((output / "cross-generation-control-continuity.json").read_text(encoding="utf-8"))
    assert continuity == [
        {
            "checks": {
                "previousCumulativeArchiveExactCanonicalMatch": True,
                "previousCumulativeArchiveSha256Match": True,
                "previousParentArchiveProjectionExactCanonicalMatch": True,
                "previousParentArchiveSha256Match": True,
                "previousParentArchiveSummarySelfHashMatch": True,
            },
            "generationIndex": 2,
            "historicalCumulativeArchiveSha256": "sha256:g1-cumulative",
            "historicalParentArchiveSha256": "sha256:g1-parent-archive",
            "passed": True,
            "previousGenerationIndex": 1,
            "previousParentArchiveSummarySha256": previous_parent_summary["summarySha256"],
        }
    ]
