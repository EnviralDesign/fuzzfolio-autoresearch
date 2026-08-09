from __future__ import annotations

import copy
import json

import pytest
import autoresearch.temporal_discovery_results as discovery_results

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_qd_rotating_evidence import (
    ROTATING_EVIDENCE_INPUT_SCHEMA,
    build_candidate_panel_bundle,
    build_candidate_window_evidence,
    build_cumulative_breeder_archive,
    build_current_panel_evaluation_cohort,
    build_generation_evidence_checkpoint,
    build_rotating_evidence_contract,
    classify_robust_breeders,
    cumulative_candidate_row,
    panel_for_generation,
    missing_backfill_panel_ids,
    reduce_provisional_diverse_survivors,
    required_panel_ids,
    validate_rotating_evidence_contract,
)
from autoresearch.temporal_qd_evaluation_population import (
    build_rotating_cohort_population,
)


def _contract():
    return build_rotating_evidence_contract({
        "schemaVersion": ROTATING_EVIDENCE_INPUT_SCHEMA,
        "developmentYears": [
            {"analysisWindowStart": f"{year}-01-01T00:00:00Z", "analysisWindowEnd": f"{year + 1}-01-01T00:00:00Z"}
            for year in range(2021, 2025)
        ],
        # These deliberately overlap development: they are research scrutiny,
        # not claimed untouched validation.
        "validationWindow": {"analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"analysisWindowStart": "2021-01-01T00:00:00Z", "analysisWindowEnd": "2024-01-01T00:00:00Z"},
    })


def _candidate(candidate_id: str) -> dict:
    profile = {"version": "v3", "candidate": candidate_id}
    return {
        "candidateId": candidate_id,
        "candidateIdentitySha256": "sha256:" + candidate_id * 64,
        "programSha256": "sha256:" + ("a" if candidate_id == "a" else "b") * 64,
        "sourceProfile": profile,
        "sourceProfileSha256": canonical_sha256(profile),
        "profileSnapshotSha256": "sha256:" + ("1" if candidate_id == "a" else "2") * 64,
    }


def _execution_metrics(candidate: dict, **values) -> dict:
    return {
        **values,
        "sourceProfileSnapshotSha256": candidate["profileSnapshotSha256"],
        "resolvedProfileSnapshotSha256": "sha256:" + "3" * 64,
        "resolvedProgramSha256": candidate["programSha256"],
    }


def _bundle(contract: dict, candidate: dict, panel_id: str) -> dict:
    panel = next(row for row in contract["panels"] if row["panelId"] == panel_id)
    records = []
    for window in panel["windows"]:
        records.append(build_candidate_window_evidence(
            candidate=candidate, panel=panel, window=window, metrics=_execution_metrics(candidate, netR=1.0, costView="research_conservative"),
            evidence_plan_semantic_sha256="sha256:" + "c" * 64,
            provenance={"authorityId": "sha256:" + "d" * 64, "taskMatrixSha256": "sha256:" + "e" * 64, "taskId": f"{candidate['candidateId']}-{window['windowId']}", "resultSha256": "sha256:" + "f" * 64},
        ))
    return build_candidate_panel_bundle(contract=contract, candidate=candidate, panel_id=panel_id, records=records)


def _rehash_record(record: dict) -> None:
    digest_fields = (
        "schemaVersion", "candidateId", "candidateIdentitySha256", "programSha256",
        "rawSourceProfileSha256", "normalizedProfileSnapshotSha256", "panelId", "windowId",
        "analysisWindowStart", "analysisWindowEnd", "evidencePlanSemanticSha256", "metrics",
    )
    record["evidenceDigestSha256"] = canonical_sha256(
        {field: record[field] for field in digest_fields}
    )
    record["recordSha256"] = canonical_sha256(
        {field: value for field, value in record.items() if field != "recordSha256"}
    )


def _rehash_bundle(bundle: dict) -> None:
    records = sorted(bundle["windowEvidence"], key=lambda row: row["windowId"])
    bundle["windowEvidenceDigests"] = [
        {"windowId": row["windowId"], "evidenceDigestSha256": row["evidenceDigestSha256"], "recordSha256": row["recordSha256"]}
        for row in records
    ]
    bundle["rawTaskProvenance"] = [
        {"windowId": row["windowId"], **row["rawTaskProvenance"]} for row in records
    ]
    bundle["bundleSha256"] = canonical_sha256(
        {field: value for field, value in bundle.items() if field != "bundleSha256"}
    )


def test_latin_square_schedule_identity_and_equal_coverage():
    contract = _contract()
    assert len(contract["quarterWindows"]) == 16
    assert [panel["windowIds"] for panel in contract["panels"]] == [
        ["year-1-q1", "year-2-q2", "year-3-q3", "year-4-q4"],
        ["year-1-q2", "year-2-q3", "year-3-q4", "year-4-q1"],
        ["year-1-q3", "year-2-q4", "year-3-q1", "year-4-q2"],
        ["year-1-q4", "year-2-q1", "year-3-q2", "year-4-q3"],
    ]
    assert {window for panel in contract["panels"] for window in panel["windowIds"]} == {row["windowId"] for row in contract["quarterWindows"]}
    assert panel_for_generation(contract, 1)["panelId"] == "panel-1"
    assert panel_for_generation(contract, 5)["panelId"] == "panel-1"
    assert panel_for_generation(contract, 6)["panelId"] == "panel-2"
    assert required_panel_ids(contract, 1) == ["panel-1"]
    assert required_panel_ids(contract, 2) == ["panel-1", "panel-2"]
    assert required_panel_ids(contract, 5) == ["panel-1", "panel-2", "panel-3", "panel-4"]


def test_digest_excludes_authority_but_bundle_retains_authority_provenance():
    contract = _contract(); candidate = _candidate("a"); panel = contract["panels"][0]; window = panel["windows"][0]
    first = build_candidate_window_evidence(candidate=candidate, panel=panel, window=window, metrics=_execution_metrics(candidate, netR=1.0), evidence_plan_semantic_sha256="sha256:" + "c" * 64, provenance={"authorityId": "sha256:" + "d" * 64, "taskMatrixSha256": "sha256:" + "e" * 64, "taskId": "one", "resultSha256": "sha256:" + "f" * 64})
    second = build_candidate_window_evidence(candidate=candidate, panel=panel, window=window, metrics=_execution_metrics(candidate, netR=1.0), evidence_plan_semantic_sha256="sha256:" + "c" * 64, provenance={"authorityId": "sha256:" + "1" * 64, "taskMatrixSha256": "sha256:" + "2" * 64, "taskId": "two", "resultSha256": "sha256:" + "3" * 64})
    assert first["evidenceDigestSha256"] == second["evidenceDigestSha256"]
    assert first["rawTaskProvenance"] != second["rawTaskProvenance"]


def test_provenance_loader_rejects_result_swapped_between_candidates(
    tmp_path, monkeypatch
):
    root = tmp_path / "screening-run"
    result_path = root / "results" / "task.json"
    result_path.parent.mkdir(parents=True)
    raw = {"candidate_id": "candidate-b"}
    result_path.write_text(json.dumps(raw), encoding="utf-8")
    panel = _contract()["panels"][0]
    window = panel["windows"][0]
    candidate = _candidate("a")
    candidate["candidateId"] = "candidate-a"
    task_manifest = {
        "authorityId": "sha256:" + "1" * 64,
        "taskMatrixSha256": "sha256:" + "2" * 64,
        "tasks": [
            {
                "task_id": "task",
                "payload": {
                    "candidate_id": "candidate-a",
                    "analysis_window_start": window["analysisWindowStart"],
                    "analysis_window_end": window["analysisWindowEnd"],
                },
            }
        ],
    }
    checkpoint = {
        "completed": {
            "task": {
                "candidateId": "candidate-a",
                "resultPath": str(result_path),
                "resultSha256": canonical_sha256(raw),
            }
        }
    }
    monkeypatch.setattr(
        discovery_results, "validate_v3_candidate_window_result", lambda *_a, **_k: None
    )
    with pytest.raises(TemporalDiscoveryContractError, match="candidate does not match"):
        discovery_results.load_provenance_bound_window_evidence(
            result_root=root,
            task_manifest=task_manifest,
            checkpoint=checkpoint,
            panel={**panel, "windows": [window]},
            candidates={"candidate-a": candidate},
        )


def test_current_panel_fairness_and_completion_order_invariance():
    rows = [
        {"candidateId": "new", "cellId": "a", "costView": "research_conservative", "currentPanelRank": 3.0},
        {"candidateId": "parent", "cellId": "b", "costView": "research_conservative", "currentPanelRank": 2.0},
        {"candidateId": "other", "cellId": "a", "costView": "research_conservative", "currentPanelRank": 1.0},
    ]
    assert [row["candidateId"] for row in reduce_provisional_diverse_survivors(rows, limit=3)] == ["new", "parent", "other"]
    assert reduce_provisional_diverse_survivors(rows, limit=3) == reduce_provisional_diverse_survivors(reversed(rows), limit=3)
    with pytest.raises(TemporalDiscoveryContractError, match="conservative"):
        reduce_provisional_diverse_survivors([{**rows[0], "costView": "none"}], limit=1)


def test_panel_cycle_dedup_and_replace_archive_rejects_stale_coverage():
    contract = _contract(); candidate = _candidate("a")
    bundles = {"a": [_bundle(contract, candidate, f"panel-{index}") for index in range(1, 5)]}
    provisional = [{**candidate, "cellId": "cell", "currentPanelRank": 1.0}]
    archive = build_cumulative_breeder_archive(contract=contract, generation_index=5, provisional=provisional, bundles=bundles)
    assert archive["mode"] == "replace"
    assert archive["requiredPanelIds"] == ["panel-1", "panel-2", "panel-3", "panel-4"]
    assert archive["staleAggregateCarryPermitted"] is False
    # G1 -> G2 requires a second bundle; a first-panel aggregate may not leak.
    with pytest.raises(TemporalDiscoveryContractError, match="required panel coverage"):
        build_cumulative_breeder_archive(contract=contract, generation_index=2, provisional=provisional, bundles={"a": bundles["a"][:1]}, previous_archive=archive)


def test_contract_rejects_drift_and_bundle_corruption():
    contract = _contract()
    changed = copy.deepcopy(contract); changed["panels"][0]["windowIds"][0] = "forged"
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        validate_rotating_evidence_contract(changed)
    candidate = _candidate("a")
    bundle = _bundle(contract, candidate, "panel-1")
    bundle["rawTaskProvenance"][0]["authorityId"] = "sha256:" + "0" * 64
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        build_cumulative_breeder_archive(contract=contract, generation_index=1, provisional=[{**candidate, "cellId": "c", "currentPanelRank": 1.0}], bundles={"a": [bundle]})


def test_panel_bundle_rebinds_forged_digest_and_exact_half_open_window():
    contract = _contract()
    candidate = _candidate("a")
    candidate["resolvedProgramSha256"] = candidate["programSha256"]
    panel = contract["panels"][0]
    records = [
        build_candidate_window_evidence(
            candidate=candidate, panel=panel, window=window,
            metrics=_execution_metrics(candidate, netR=1.0),
            evidence_plan_semantic_sha256="sha256:" + "c" * 64,
            provenance={"authorityId": "sha256:" + "d" * 64, "taskMatrixSha256": "sha256:" + "e" * 64, "taskId": window["windowId"], "resultSha256": "sha256:" + "f" * 64},
        )
        for window in panel["windows"]
    ]
    forged = copy.deepcopy(records)
    forged[0]["evidenceDigestSha256"] = "sha256:" + "0" * 64
    forged[0]["recordSha256"] = canonical_sha256(
        {key: value for key, value in forged[0].items() if key != "recordSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="evidence digest mismatch"):
        build_candidate_panel_bundle(
            contract=contract, candidate=candidate, panel_id="panel-1", records=forged
        )
    wrong_date = copy.deepcopy(records)
    wrong_date[0]["analysisWindowStart"] = "2021-02-01T00:00:00Z"
    _rehash_record(wrong_date[0])
    with pytest.raises(TemporalDiscoveryContractError, match="canonically bound"):
        build_candidate_panel_bundle(
            contract=contract, candidate=candidate, panel_id="panel-1", records=wrong_date
        )
    wrong_window = copy.deepcopy(records)
    wrong_window[0]["windowId"] = "year-1-q4"
    _rehash_record(wrong_window[0])
    with pytest.raises(TemporalDiscoveryContractError, match="cover each panel window"):
        build_candidate_panel_bundle(
            contract=contract, candidate=candidate, panel_id="panel-1", records=wrong_window
        )


def test_cumulative_boundary_rebinds_even_a_rehashed_bundle():
    contract = _contract()
    candidate = _candidate("a")
    bundle = _bundle(contract, candidate, "panel-1")
    bundle["windowEvidence"][0]["analysisWindowEnd"] = "2021-04-02T00:00:00Z"
    _rehash_record(bundle["windowEvidence"][0])
    _rehash_bundle(bundle)
    with pytest.raises(TemporalDiscoveryContractError, match="canonically bound"):
        cumulative_candidate_row(
            contract=contract, generation_index=1, candidate=candidate,
            bundles=[bundle], cell_id="cell", current_panel_rank=1.0,
        )


def test_split_restart_parity_for_absolute_phase():
    contract = _contract()
    uninterrupted = [panel_for_generation(contract, index)["panelId"] for index in range(1, 9)]
    resumed = [panel_for_generation(contract, index)["panelId"] for index in range(1, 5)] + [panel_for_generation(contract, index)["panelId"] for index in range(5, 9)]
    assert uninterrupted == resumed


def test_parent_is_evaluation_cohort_not_proposal_and_checkpoint_is_restart_exact():
    contract = _contract(); new = _candidate("a"); parent = _candidate("b")
    cohort = build_current_panel_evaluation_cohort(
        new_candidates=[new], retained_parents=[parent], contract=contract, generation_index=2
    )
    assert cohort["newProposalCandidateIds"] == ["a"]
    assert cohort["retainedParentEvaluationCandidateIds"] == ["b"]
    assert cohort["parentReevaluationIsProposal"] is False
    checkpoint = build_generation_evidence_checkpoint(
        contract=contract, generation_index=2, stage="cumulative_backfill", cohort=cohort,
        provisional_candidate_ids=["a", "b"],
    )
    assert checkpoint == build_generation_evidence_checkpoint(
        contract=contract, generation_index=2, stage="cumulative_backfill", cohort=cohort,
        provisional_candidate_ids=["b", "a"],
    )
    assert missing_backfill_panel_ids(contract=contract, generation_index=5, bundles=[_bundle(contract, new, "panel-1")]) == ["panel-2", "panel-3", "panel-4"]


def test_robust_policy_uses_equal_coverage_support_and_soft_worst_window():
    base = {
        "candidateIdentitySha256": "sha256:" + "a" * 64,
        "programSha256": "sha256:" + "b" * 64,
        "cellId": "cell",
        "coveredMonths": 12,
        "novelty": 2.0,
    }
    quality = {
        **base,
        "candidateId": "quality",
        "windowMetrics": [
            {"conservativeNetR": value, "noCostNetR": value + 0.1, "maxDrawdownR": 1.0, "closedTrades": 12}
            for value in (1.0, -0.2, 1.0, 0.2)
        ],
    }
    frontier = {
        **base,
        "candidateId": "frontier",
        "windowMetrics": [
            {"conservativeNetR": -0.1, "noCostNetR": 0.0, "maxDrawdownR": 0.5, "closedTrades": 12}
            for _ in range(4)
        ],
    }
    unsupported = {
        **base,
        "candidateId": "unsupported",
        "windowMetrics": [
            {"conservativeNetR": 10.0, "noCostNetR": 10.0, "maxDrawdownR": 0.0, "closedTrades": 1}
            for _ in range(4)
        ],
    }
    result = classify_robust_breeders(
        candidate_rows=[unsupported, frontier, quality], breeder_width=10
    )
    assert [row["candidateId"] for row in result["quality"]] == ["quality"]
    assert [row["candidateId"] for row in result["frontier"]] == ["frontier"]
    assert result["quality"][0]["robustEconomics"]["worstWindowConservativeNetR"] == -0.2
    assert result["policy"]["worstWindowConservativeNetRIsHardGate"] is False


def test_robust_selection_uses_declared_pareto_objectives_not_return_lexicography():
    base = {
        "candidateIdentitySha256": "sha256:" + "a" * 64,
        "programSha256": "sha256:" + "b" * 64,
        "cellId": "cell",
        "coveredMonths": 2,
    }
    dominated_high_return = {
        **base,
        "candidateId": "high-return-dominated",
        "novelty": 0.0,
        "windowMetrics": [
            {
                "conservativeNetR": value,
                "noCostNetR": value + 2.0,
                "maxDrawdownR": 5.0,
                "closedTrades": 4,
            }
            for value in (20.0, -1.0)
        ],
    }
    objective_winner = {
        **base,
        "candidateId": "objective-winner",
        "novelty": 2.0,
        "windowMetrics": [
            {
                "conservativeNetR": 9.0,
                "noCostNetR": 9.5,
                "maxDrawdownR": 1.0,
                "closedTrades": 4,
            }
            for _ in range(2)
        ],
    }
    result = classify_robust_breeders(
        candidate_rows=[dominated_high_return, objective_winner], breeder_width=1
    )
    assert [row["candidateId"] for row in result["quality"]] == [
        "objective-winner"
    ]


def test_panel_bundle_rejects_wrong_resolved_program_before_cumulative_reduction():
    contract = _contract()
    candidate = _candidate("a")
    candidate["resolvedProgramSha256"] = candidate["programSha256"]
    second_panel = contract["panels"][1]
    with pytest.raises(TemporalDiscoveryContractError, match="resolved program identity drifted"):
        build_candidate_window_evidence(
            candidate=candidate,
            panel=second_panel,
            window=second_panel["windows"][0],
            metrics={
                **_execution_metrics(candidate, netR=1.0),
                "resolvedProgramSha256": "sha256:" + "9" * 64,
            },
            evidence_plan_semantic_sha256="sha256:" + "c" * 64,
            provenance={
                "authorityId": "sha256:" + "d" * 64,
                "taskMatrixSha256": "sha256:" + "e" * 64,
                "taskId": "drift",
                "resultSha256": "sha256:" + "f" * 64,
            },
        )


def test_cumulative_candidate_row_is_completion_order_invariant():
    contract = _contract()
    candidate = _candidate("a")
    bundles = [_bundle(contract, candidate, "panel-1"), _bundle(contract, candidate, "panel-2")]
    first = cumulative_candidate_row(
        contract=contract,
        generation_index=2,
        candidate=candidate,
        bundles=bundles,
        cell_id="cell",
        current_panel_rank=1.0,
    )
    second = cumulative_candidate_row(
        contract=contract,
        generation_index=2,
        candidate=candidate,
        bundles=list(reversed(bundles)),
        cell_id="cell",
        current_panel_rank=1.0,
    )
    assert first == second
    assert first["coveredMonths"] == 24
    assert len(first["windowMetrics"]) == 8


def test_rotating_cohort_population_is_evaluation_only_and_content_bound():
    candidate = {
        **_candidate("a"),
        "sourceProfile": {"version": "v3", "graph": {}},
        "sourceProfileSha256": canonical_sha256({"version": "v3", "graph": {}}),
    }
    first = build_rotating_cohort_population(
        candidates=[candidate],
        generation_index=5,
        panel_id="panel-1",
        cohort_role="prior_panel_backfill",
        rotating_evidence_sha256=_contract()["rotatingEvidenceSha256"],
    )
    assert first["proposalPopulation"] is False
    assert first["generationIndex"] == 5
    assert first["panelId"] == "panel-1"
    assert first["populationSha256"] == canonical_sha256(
        {key: value for key, value in first.items() if key != "populationSha256"}
    )


def test_no_market_g1_g2_g5_backfill_schedule_is_only_missing_distinct_panels():
    contract = _contract()
    candidate = _candidate("a")
    panel_1 = _bundle(contract, candidate, "panel-1")
    panel_2 = _bundle(contract, candidate, "panel-2")
    panel_3 = _bundle(contract, candidate, "panel-3")
    panel_4 = _bundle(contract, candidate, "panel-4")
    assert missing_backfill_panel_ids(
        contract=contract, generation_index=1, bundles=[panel_1]
    ) == []
    # A retained G1 parent is reevaluated on G2/panel-2 and needs no replay of
    # panel-1; a new G2 proposal has only panel-2 and backfills panel-1.
    assert missing_backfill_panel_ids(
        contract=contract, generation_index=2, bundles=[panel_1, panel_2]
    ) == []
    assert missing_backfill_panel_ids(
        contract=contract, generation_index=2, bundles=[panel_2]
    ) == ["panel-1"]
    # G5 repeats panel-1. Existing complete parents do not duplicate it, while
    # new proposals backfill only the other three distinct panels.
    assert missing_backfill_panel_ids(
        contract=contract,
        generation_index=5,
        bundles=[panel_1, panel_2, panel_3, panel_4],
    ) == []
    assert missing_backfill_panel_ids(
        contract=contract, generation_index=5, bundles=[panel_1]
    ) == ["panel-2", "panel-3", "panel-4"]
