from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_stage5e7_v3_validation as validation_harness
import autoresearch.temporal_stage5e7_v3_panel_bridge as panel_bridge
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_qd_evolution import _load_archive
from autoresearch.temporal_search import build_authority, build_task_matrix
from autoresearch.temporal_stage5e7_v3_validation import (
    analyze_operator_panel,
    analyze_policy_ab,
    audit_validation_root,
    build_operator_panel,
    build_policy_ab,
    build_repair_panel,
)


def _dump(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha(value: object) -> str:
    return canonical_sha256(value)


def _candidate(candidate_id: str, index: int) -> dict:
    profile = {
        "version": "v2",
        "instruments": ["EURUSD"],
        "directionMode": "short" if 90 <= index < 105 else "both",
        "graph": {
            "kind": "temporal_graph_v1",
            "states": [{"id": f"s{node}"} for node in range((index % 4) * 10)],
            "transitions": [],
        },
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {"stopLossPercent": 0.5, "takeProfitPercent": 1.0},
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }
    identity_material = {"schemaVersion": "fixture_candidate_identity_v1", "candidateId": candidate_id}
    return {
        "candidateId": candidate_id,
        "candidateIdentityMaterial": identity_material,
        "candidateIdentitySha256": _sha(identity_material),
        "sourceMode": f"origin_{index % 4}",
        "seedId": "fixture",
        "sourceProfile": profile,
        "sourceProfileSha256": _sha(profile),
        "profileSnapshotSha256": _sha(profile),
        "programSha256": _sha({"program": candidate_id}),
        "profile_fingerprint": f"family_{index % 4}",
    }


def _metrics(*, net: float, trades: int, holding: float, observations: int = 1000) -> dict:
    return {
        "observationsProcessed": observations,
        "tradesClosed": trades,
        "wins": max(0, trades // 2),
        "losses": trades - max(0, trades // 2),
        "flatTrades": 0,
        "totalNetR": net,
        "totalGrossR": net + 0.1,
        "maxDrawdownR": max(0.1, -net + 0.2),
        "averageHoldingBars": holding,
        "exposureRatio": 0.1,
        "transitionEntropy": 0.5,
        "winRate": 0.5,
        "profitFactor": 1.2,
        "equityCurveR": [0.0, net],
        "actionCounts": {},
        "closeReasonCounts": {},
        "stateOccupancy": {},
        "transitionCounts": {},
        "trades": [{"holdingBars": int(holding)} for _ in range(trades)],
    }


def _write_results(root: Path, candidates: list[dict], overrides: dict[str, tuple[float, int, float]] | None = None) -> None:
    overrides = overrides or {}
    for index, candidate in enumerate(candidates):
        candidate_id = candidate["candidateId"]
        if candidate_id in overrides:
            net, trades, holding = overrides[candidate_id]
        else:
            source_index = int(candidate_id.rsplit("_", 1)[-1]) if candidate_id.rsplit("_", 1)[-1].isdigit() else index
            if 5 <= source_index < 35:
                net, trades, holding = 1.0, 5, 12.0
            elif 35 <= source_index < 45:
                net, trades, holding = 1.0, 9, 12.0
            elif 45 <= source_index < 60:
                net, trades, holding = -1.0, 5, 12.0
            elif 60 <= source_index < 75:
                net, trades, holding = 0.5, 20, 12.0
            elif 75 <= source_index < 90:
                net, trades, holding = 0.2, 2, 120.0
            else:
                net, trades, holding = 0.0, 3, 12.0
        for window in range(2):
            window_trades = 1 if 35 <= index < 45 and window else trades
            stream = _sha({"sourceProfileSha256": candidate["sourceProfileSha256"], "window": window})
            conservative = _metrics(net=net, trades=window_trades, holding=holding)
            no_cost = dict(conservative)
            no_cost["totalNetR"] = net + 0.1
            _dump(
                root / "results" / f"{index:03d}-{window}.json",
                {
                    "schema_version": "temporal_graph_candidate_window_result_v1",
                    "candidate_id": candidate_id,
                    "analysis_window_start": f"2023-0{window + 1}-01T00:00:00Z",
                    "analysis_window_end": f"2023-0{window + 2}-01T00:00:00Z",
                    "program_sha256": candidate["programSha256"],
                    "observation_stream_sha256": stream,
                    "cost_view_results": {
                        "research_conservative": {"replay_result": {"streamSha256": stream, "metrics": conservative, "trades": conservative["trades"]}},
                        "none": {"replay_result": {"streamSha256": stream, "metrics": no_cost}},
                    },
                },
            )


def _write_v3_results(
    root: Path,
    candidates: list[dict],
    overrides: dict[str, tuple[float, int, float]] | None = None,
    *,
    authority: dict | None = None,
) -> None:
    """Write the compact fixture rows above, then make their economics v3-admissible."""
    planned_tasks = build_task_matrix(authority) if authority is not None else []
    if authority is not None and len(planned_tasks) != len(candidates) * 2:
        raise AssertionError("fixture authority must contain exactly two windows per candidate")
    _write_results(root, candidates, overrides)
    path_sha = _sha({"schema_version": "temporal_graph_cost_view_path_v1", "graph_path": [], "execution_path": [], "trade_path": []})
    result_paths = sorted((root / "results").glob("*.json"), key=lambda path: path.name)
    for ordinal, path in enumerate(result_paths):
        payload = json.loads(path.read_text(encoding="utf-8"))
        task = planned_tasks[ordinal] if authority is not None else None
        if task is not None:
            job = task["payload"]
            payload.update(
                {
                    "task_kind": task["task_kind"],
                    "job_id": job["job_id"],
                    "authority_id": job["authority_id"],
                    "candidate_id": job["candidate_id"],
                    "analysis_window_start": job["analysis_window_start"],
                    "analysis_window_end": job["analysis_window_end"],
                    "evidence_plan_id": job["evidence_plan"]["plan_id"],
                    "lake_window_semantic_sha256": job["lake_window_semantic_sha256"],
                    "shared_observation_stream_id": job["shared_observation_stream_id"],
                }
            )
        start, end = payload["analysis_window_start"], payload["analysis_window_end"]
        stream = payload["observation_stream_sha256"]
        evidence = {
            "schema_version": "temporal_graph_candidate_window_evidence_contract_v1",
            "analysis_window_start": start, "analysis_window_end": end,
            "analysis_window_end_exclusive": True, "requested_bar_limit": 100,
            "effective_bar_limit": 100, "observation_count": 1000,
            "first_admitted_observation_timestamp": start,
            "last_admitted_observation_timestamp": start,
            "warmup_sufficient": True,
            "warmup_sufficiency": {"sufficient": True, "source": "fixture"},
            "excluded_provisional_count": 0, "excluded_outside_analysis_window_count": 0,
        }
        payload.update({
            "task_kind": "temporal_graph_candidate_window", "evidence_contract": evidence,
            "observation_summary": {"observation_count": 1000, "first_bar_start": start, "last_bar_start": start},
            "diagnostics": {"observation_count": 1000, "requested_bar_limit": 100, "effective_bar_limit": 100, "warmup_sufficient": True, "warmup_sufficiency": evidence["warmup_sufficiency"], "first_admitted_observation_timestamp": start, "last_admitted_observation_timestamp": start, "excluded_provisional_count": 0, "excluded_outside_analysis_window_count": 0, "cost_view_decision_path_sha256": path_sha, "cost_view_path_parity": "matched", "cost_view_count": 2, "shared_stream_required": True},
        })
        for cost_view in payload["cost_view_results"].values():
            cost_view.update({"cost_view": "research_conservative" if cost_view is payload["cost_view_results"]["research_conservative"] else "none", "observation_stream_sha256": stream})
            replay = cost_view["replay_result"]
            replay.update({"graphTraces": [], "executionTraces": [], "trades": []})
            metrics = replay["metrics"]
            net = float(metrics["totalNetR"])
            metrics.update({
                "totalExecutionCostPercent": 0.0, "unresolvedPosition": False, "unresolvedPendingEffect": False,
                "terminalValuation": {"schemaVersion": "temporal_terminal_valuation_v1", "policy": "leave_open_mark_to_market_v1", "positionStatus": "no_open_position", "lastCompletedBarId": "fixture-last", "lastCompletedBarStart": start, "lastCompletedBarClose": start, "markPrice": 1.0, "exitCostPercent": 0.0, "pendingEffectStatus": "none", "pendingEffectCancellationTreatment": "not_applicable", "closedTradeCountDelta": 0},
                "terminalAdjustedTotalGrossR": float(metrics["totalGrossR"]), "terminalAdjustedTotalNetR": net,
                "terminalAdjustedTotalExecutionCostPercent": 0.0, "terminalAdjustedEquityCurveR": [0.0, net], "terminalAdjustedMaxDrawdownR": max(0.0, -net),
            })
        _dump(path, payload)


def _bridge_template() -> dict:
    profile = _candidate("template", 0)["sourceProfile"]
    windows = [
        ("development-a", "2023-10-01T00:00:00Z", "2023-11-01T00:00:00Z", "2023-09-01T00:00:00Z"),
        ("development-b", "2021-07-01T00:00:00Z", "2021-08-01T00:00:00Z", "2021-06-01T00:00:00Z"),
    ]
    inputs = []
    for window_id, start, end, data_start in windows:
        plan = {
            "schema_version": "fuzzfolio.replay-evidence-plan.v2",
            "profile_snapshot_sha256": _sha(profile),
            "analysis_window_start": start,
            "analysis_window_end": end,
            "execution_cell_sha256": _sha(profile["executionConfig"]["exitPolicy"]["selectedCell"]),
            "lake_window_binding": {
                "window_semantic_sha256": _sha({"window": window_id}),
                "request": {
                    "data_start": data_start,
                    "data_end": end,
                    "pairs": ["EURUSD"],
                    "timeframes": ["M5"],
                },
            },
        }
        plan["plan_id"] = _sha(plan)
        inputs.append({"windowId": window_id, "evidencePlan": plan})
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "stage5e7-v3-fixture-development",
        "workerContract": {
            "workerContractSha256": "sha256:" + "d" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": "template",
                "sourceProfile": profile,
                "sourceProfileSha256": _sha(profile),
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": inputs,
            }
        ],
        "developmentWindows": [
            {"windowId": window_id, "analysisWindowStart": start, "analysisWindowEnd": end}
            for window_id, start, end, _data_start in windows
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2024-06-29T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "reserved",
            }
        ],
        "bounds": {
            "maxCandidates": 64,
            "maxDevelopmentWindows": 2,
            "maxTasks": 128,
            "maxAttempts": 2,
            "deadlineSeconds": 60,
        },
    }


def _freeze_bridge(population_path: Path, output_root: Path) -> Path:
    template_path = output_root.parent / f"{output_root.name}-template.json"
    _dump(template_path, _bridge_template())
    result = panel_bridge.freeze_finite_panel_campaign(
        population_path=population_path,
        template_preparation_path=template_path,
        worker_contract_sha256="sha256:" + "e" * 64,
        output_root=output_root,
    )
    return Path(result["outputRoot"])


def _write_bound_v3_results(
    root: Path,
    candidates: list[dict],
    bridge_root: Path,
    overrides: dict[str, tuple[float, int, float]] | None = None,
) -> None:
    authority = json.loads((bridge_root / "authority.json").read_text(encoding="utf-8"))
    _dump(root / "authority.json", authority)
    _dump(
        root / "task-manifest.json",
        json.loads((bridge_root / "task-matrix" / "task-manifest.json").read_text(encoding="utf-8")),
    )
    _write_v3_results(root, candidates, overrides, authority=authority)


def _authority_with_label(authority: dict, label: str) -> dict:
    """Build a distinct but internally valid authority for rejection tests."""
    preparation = {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": label,
        "workerContract": authority["workerContract"],
        "candidates": [
            {
                "candidateId": row["candidateId"],
                "sourceProfile": row["sourceProfile"],
                "sourceProfileSha256": row["sourceProfileSha256"],
                "instrument": row["instrument"],
                "timeframe": row["timeframe"],
                "barLimit": row["barLimit"],
                "windowInputs": [
                    {"windowId": item["windowId"], "evidencePlan": item["evidencePlan"]}
                    for item in row["windowInputs"]
                ],
            }
            for row in authority["candidates"]
        ],
        "developmentWindows": authority["developmentWindows"],
        "prohibitedEvidence": authority["prohibitedEvidence"],
        "bounds": authority["bounds"],
    }
    return build_authority(preparation)


def _replace_result_program(root: Path, candidate_id: str, value: object) -> None:
    changed = False
    for path in (root / "results").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("candidate_id") != candidate_id:
            continue
        payload["program_sha256"] = value
        _dump(path, payload)
        changed = True
    assert changed


def _old_inputs(tmp_path: Path, *, source_version: str = "v3") -> tuple[Path, Path, Path, Path]:
    ids = ["qd_390fixture", "qd_9dbfixture", "qd_de455fixture"] + [f"qd_fixture_{index}" for index in range(5, 140)]
    candidates = [_candidate(candidate_id, index) for index, candidate_id in enumerate(ids)]
    archive_controls = [_candidate("qd_538fixture", 140), _candidate("qd_339fixture", 141)]
    population = {"schemaVersion": f"temporal_qd_generation_population_{source_version}", "candidateCount": len(candidates), "candidates": candidates}
    population["populationSha256"] = _sha(population)
    population_path = tmp_path / "old-population.json"
    _dump(population_path, population)
    archive_members = []
    for index, candidate in enumerate(archive_controls):
        aggregate = {
            "candidateId": candidate["candidateId"],
            "totalConservativeNetR": -1.0,
            "worstWindowConservativeNetR": -1.0,
            "totalTrades": 10,
            "tradeCountsByWindow": [5, 5],
            "entryFrequencyPerThousand": 8.0,
            "medianHoldingBars": 48.0,
        }
        archive_members.append(
            {
                "candidateId": candidate["candidateId"],
                "candidate": candidate,
                "aggregate": aggregate,
                "descriptor": {"cellId": f"zz-archive-carryover-{index}"},
                "retentionReason": "fixture_archive_control",
            }
        )
    archive = {
        "schemaVersion": f"temporal_qd_archive_{source_version}",
        "populationSha256": population["populationSha256"],
        "cells": [{"cellId": "fixture-carryovers", "members": archive_members}],
    }
    if source_version == "v2":
        archive["qdVersion"] = "temporal_qd_evolution_v2"
    archive["archiveSha256"] = _sha(archive)
    archive_path = tmp_path / "old-archive.json"
    _dump(archive_path, archive)
    result_root = tmp_path / "old-results"
    _write_results(result_root, candidates)
    dossiers = tmp_path / "candidate_dossiers.csv"
    dossiers.write_text(
        "candidateId,correctedEvidenceResolution\n" + "\n".join(
            f"{candidate['candidateId']},{'resolved' if 5 <= index < 20 else 'unresolved'}"
            for index, candidate in enumerate(candidates)
        ) + "\n",
        encoding="utf-8",
    )
    return archive_path, population_path, result_root, dossiers


def _repair(tmp_path: Path) -> tuple[dict, Path]:
    archive, population, results, dossiers = _old_inputs(tmp_path)
    external = tmp_path / "external-output"
    result = build_repair_panel(
        old_archive_path=archive,
        old_population_path=population,
        old_result_root=results,
        candidate_dossiers_path=dossiers,
        output_root=external,
        version="fixture-v1",
        seed=7,
    )
    return result, external


def test_build_repair_accepts_only_pinned_v2_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    archive_path, population_path, result_root, dossiers = _old_inputs(tmp_path, source_version="v2")
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    population = json.loads(population_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(validation_harness, "FROZEN_STAGE5E7_V2_ARCHIVE_SHA256", archive["archiveSha256"])
    monkeypatch.setattr(validation_harness, "FROZEN_STAGE5E7_V2_POPULATION_SHA256", population["populationSha256"])

    result = build_repair_panel(
        old_archive_path=archive_path,
        old_population_path=population_path,
        old_result_root=result_root,
        candidate_dossiers_path=dossiers,
        output_root=tmp_path / "external-output",
        version="fixture-v2",
        seed=7,
    )

    panel = json.loads((Path(result["outputRoot"]) / "reference-panel.json").read_text(encoding="utf-8"))
    assert panel["source"]["oldArchiveSha256"] == archive["archiveSha256"]
    assert panel["selectionPolicy"]["oldArchiveRankPromotion"] is False
    with pytest.raises(TemporalDiscoveryContractError, match="unknown QD archive schema"):
        _load_archive(archive_path)

    unrelated = dict(archive)
    unrelated["generationIndex"] = 4
    unrelated["archiveSha256"] = _sha({key: value for key, value in unrelated.items() if key != "archiveSha256"})
    unrelated_path = tmp_path / "unrelated-v2-archive.json"
    _dump(unrelated_path, unrelated)
    with pytest.raises(TemporalDiscoveryContractError, match="not a canonical QD archive"):
        validation_harness._archive_context(
            unrelated_path, population["populationSha256"], population["schemaVersion"]
        )


def test_repair_panel_is_deterministic_and_stratified(tmp_path: Path) -> None:
    result, external = _repair(tmp_path)
    panel = json.loads((Path(result["outputRoot"]) / "reference-panel.json").read_text(encoding="utf-8"))
    rows = panel["selectionRows"]
    labels = [tag for row in rows for tag in row["referenceTags"]]

    assert result["candidateCount"] == 64
    assert len(rows) == 64
    for prefix in validation_harness.NAMED_REFERENCES:
        matching_rows = [row for row in rows if str(row["candidateId"]).startswith(prefix)]
        assert matching_rows
        assert any(f"named:{prefix}" in row["referenceTags"] for row in matching_rows)
    assert any(tag.startswith("both_positive_resolved:") for tag in labels)
    assert any(tag.startswith("both_positive_unresolved:") for tag in labels)
    assert all(row["primarySelectionReason"] for row in rows)
    assert all("old_rank" not in row["primarySelectionReason"] for row in rows)
    second = build_repair_panel(
        old_archive_path=panel["source"]["oldArchivePath"],
        old_population_path=panel["source"]["oldPopulationPath"],
        old_result_root=panel["source"]["oldResultRoot"],
        candidate_dossiers_path=panel["source"]["candidateDossiersPath"],
        output_root=tmp_path / "external-output-second",
        version="fixture-v1",
        seed=7,
    )
    assert second["referencePanelSha256"] == result["referencePanelSha256"]
    assert audit_validation_root(output_root=external, version="fixture-v1")["ok"] is True


def test_repair_legacy_aggregate_metrics_are_explicit_proxies_not_terminal_adjusted(tmp_path: Path) -> None:
    result, _external = _repair(tmp_path)
    panel = json.loads((Path(result["outputRoot"]) / "reference-panel.json").read_text(encoding="utf-8"))

    assert panel["selectionPolicy"]["oldArchiveRankPromotion"] is False
    for row in panel["selectionRows"]:
        economics = row["stratificationEconomics"]
        assert economics["basis"] == "legacy_closed_trade_proxy"
        assert economics["v3Admissible"] is False
        assert economics["selectionUse"] == "stratified_coverage_only_no_promotion"
        assert economics["totalNetRSource"].startswith("aggregate.")
        assert economics["worstWindowNetRSource"].startswith("aggregate.")
        assert "terminalAdjustedR" not in row
        assert "worstWindowTerminalAdjustedR" not in row
        assert "legacyClosedTradeProxyR" in row
        assert "worstWindowLegacyClosedTradeProxyR" in row


def test_repair_cohort_includes_archive_only_named_controls_with_provenance(tmp_path: Path) -> None:
    result, _external = _repair(tmp_path)
    panel = json.loads((Path(result["outputRoot"]) / "reference-panel.json").read_text(encoding="utf-8"))
    rows = {row["candidateId"]: row for row in panel["selectionRows"]}

    for candidate_id in ("qd_538fixture", "qd_339fixture"):
        assert rows[candidate_id]["repairCohortSource"] == "archive_carryover"
        assert rows[candidate_id]["repairAggregateSource"] == "archive_embedded_aggregate"
        assert any(tag.startswith("named:") for tag in rows[candidate_id]["referenceTags"])
    cohort = panel["source"]["selectionCohort"]
    assert cohort["archiveOnlyCarryoverCandidateCount"] == 2
    assert cohort["cohortSha256"].startswith("sha256:")


def test_repair_rejects_conflicting_archive_duplicate_identity(tmp_path: Path) -> None:
    archive_path, population_path, result_root, dossiers = _old_inputs(tmp_path)
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    conflicting = json.loads(json.dumps(archive["cells"][0]["members"][0]))
    target = json.loads(population_path.read_text(encoding="utf-8"))["candidates"][0]
    conflicting["candidateId"] = target["candidateId"]
    conflicting["candidate"]["candidateId"] = target["candidateId"]
    conflicting["candidate"]["candidateIdentityMaterial"] = {"schemaVersion": "fixture_conflict_v1"}
    conflicting["candidate"]["candidateIdentitySha256"] = _sha(conflicting["candidate"]["candidateIdentityMaterial"])
    conflicting["aggregate"]["candidateId"] = target["candidateId"]
    archive["cells"][0]["members"].append(conflicting)
    archive["archiveSha256"] = _sha({key: value for key, value in archive.items() if key != "archiveSha256"})
    _dump(archive_path, archive)

    with pytest.raises(TemporalDiscoveryContractError, match="disagree on candidate identity"):
        build_repair_panel(
            old_archive_path=archive_path,
            old_population_path=population_path,
            old_result_root=result_root,
            candidate_dossiers_path=dossiers,
            output_root=tmp_path / "external-output",
            version="fixture-conflict",
            seed=7,
        )


def test_repair_rejects_bad_archive_carryover_identity_hash(tmp_path: Path) -> None:
    archive_path, population_path, result_root, dossiers = _old_inputs(tmp_path)
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    archive["cells"][0]["members"][0]["candidate"]["candidateIdentitySha256"] = "sha256:not-the-material"
    archive["archiveSha256"] = _sha({key: value for key, value in archive.items() if key != "archiveSha256"})
    _dump(archive_path, archive)

    with pytest.raises(TemporalDiscoveryContractError, match="candidate identity mismatch"):
        build_repair_panel(
            old_archive_path=archive_path,
            old_population_path=population_path,
            old_result_root=result_root,
            candidate_dossiers_path=dossiers,
            output_root=tmp_path / "external-output",
            version="fixture-bad-identity",
            seed=7,
        )


def test_candidate_annotations_use_dossier_unresolved_flags_and_reject_conflicts() -> None:
    candidate = _candidate("resolution_fixture", 0)
    aggregate = {
        "candidateId": candidate["candidateId"],
        "totalConservativeNetR": 1.0,
        "worstWindowConservativeNetR": 0.5,
        "totalTrades": 10,
        "tradeCountsByWindow": [5, 5],
        "entryFrequencyPerThousand": 8.0,
        "medianHoldingBars": 48.0,
        "windowRecords": [
            {"conservativeTerminal": {"terminalPositionStatus": "no_open_position"}},
            {"conservativeTerminal": {"terminalPositionStatus": "no_open_position"}},
        ],
    }
    resolved = validation_harness._candidate_annotations(
        candidate,
        aggregate,
        {"cellId": "resolution-fixture"},
        {"any_unresolved_position": "0", "window_unresolved_json": "[0, 0]"},
    )
    assert resolved["resolution"] == "resolved"
    assert "dossier.any_unresolved_position" in resolved["resolutionEvidenceSource"]

    with pytest.raises(TemporalDiscoveryContractError, match="resolution evidence is contradictory"):
        validation_harness._candidate_annotations(
            candidate,
            aggregate,
            {"cellId": "resolution-fixture"},
            {"any_unresolved_position": "1", "window_unresolved_json": "[1, 1]"},
        )


def test_reference_positive_resolution_quota_preserves_the_frozen_capacity() -> None:
    assert validation_harness.REFERENCE_QUOTAS["both_positive_resolved"] == 4
    assert validation_harness.REFERENCE_QUOTAS["both_positive_unresolved"] == 10
    assert len(validation_harness.NAMED_REFERENCES) + sum(validation_harness.REFERENCE_QUOTAS.values()) == 64


def test_repair_origin_slots_cover_the_real_gen4_two_origin_shape() -> None:
    """Gen4 has inline profiles and exactly these two admitted source modes."""
    records = {}
    for index in range(8):
        profile = {
            "version": "v2",
            "directionMode": "both",
            "executionConfig": {"managementLibrary": {"version": "fixture"}},
            "indicators": [
                {
                    "meta": {"id": f"INDICATOR_{index % 4}", "signalRole": "trigger"},
                    "config": {"timeframe": "M5", "isActive": True},
                }
            ],
        }
        candidate = {
            "candidateId": f"real_shape_{index}",
            "sourceMode": "qd_random_immigrant" if index % 2 == 0 else "qd_structural_offspring",
            "sourceProfile": profile,
        }
        aggregate = {
            "totalConservativeNetR": 0.0,
            "worstWindowConservativeNetR": 0.0,
            "totalTrades": 10,
            "tradeCountsByWindow": [5, 5],
            "entryFrequencyPerThousand": 8.0,
            "medianHoldingBars": 48.0,
        }
        records[candidate["candidateId"]] = validation_harness._candidate_annotations(
            candidate,
            aggregate,
            {"cellId": f"descriptor-{index % 4}"},
            {},
        )

    assert {row["structuralFamilySource"] for row in records.values()} == {
        "profile_semantic_shape_with_execution"
    }
    assert len({row["structuralFamilyId"] for row in records.values()}) == 4

    origin_slots = [label for label, _predicate in validation_harness._reference_slots(records) if label.startswith("representative_origin:")]
    assert origin_slots == [
        "representative_origin:qd_random_immigrant:1",
        "representative_origin:qd_random_immigrant:2",
        "representative_origin:qd_structural_offspring:1",
        "representative_origin:qd_structural_offspring:2",
    ]


def test_repair_audit_detects_tampering(tmp_path: Path) -> None:
    result, external = _repair(tmp_path)
    path = Path(result["outputRoot"]) / "reference-panel.json"
    path.write_text(path.read_text(encoding="utf-8").replace("coverage_only", "tampered", 1), encoding="utf-8")

    with pytest.raises(TemporalDiscoveryContractError, match="manifest file mismatch"):
        audit_validation_root(output_root=external, version="fixture-v1")


def test_policy_ab_analysis_reduces_one_shared_v3_population_and_result_set(tmp_path: Path) -> None:
    repair, external = _repair(tmp_path)
    policy = build_policy_ab(reference_root=repair["outputRoot"], output_root=external, version="fixture-v1", seed=7)
    policy_root = Path(policy["outputRoot"])
    population = json.loads((policy_root / "population.json").read_text(encoding="utf-8"))
    bridge_root = _freeze_bridge(
        Path(repair["outputRoot"]) / "reference-population.json",
        tmp_path / "repair-bridge",
    )
    results = tmp_path / "shared-policy-results"
    _write_bound_v3_results(results, population["candidates"], bridge_root)

    analysis = analyze_policy_ab(
        policy_root=policy_root,
        corrected_result_root=results,
        panel_bridge_root=bridge_root,
        output_root=external,
        version="fixture-v1",
    )

    assert (policy_root / "policy-a-v2-like-control" / "config.json").is_file()
    assert (policy_root / "policy-b-v3-robust" / "config.json").is_file()
    assert analysis["analysisSha256"].startswith("sha256:")
    report = json.loads((external / "stage5e7-v3-validation-fixture-v1" / "policy-ab-analysis" / "policy-ab-analysis.json").read_text(encoding="utf-8"))
    assert report["reducedPopulationSha256"] == population["populationSha256"]
    assert report["policyA"]["policyIdentity"]["policyName"] == "stage5e7_v2_like_corrected_archive_reducer"
    assert report["policyB"]["policyIdentity"]["policyName"] == "stage5e7_v3_robust_quality_archive"
    assert report["interpretation"].endswith("no_search_or_breeding")

    _replace_result_program(results, population["candidates"][0]["candidateId"], "sha256:" + "0" * 64)
    with pytest.raises(TemporalDiscoveryContractError, match="program identity does not match"):
        analyze_policy_ab(
            policy_root=policy_root,
            corrected_result_root=results,
            panel_bridge_root=bridge_root,
            output_root=tmp_path / "wrong-program-output",
            version="fixture-v1",
        )
    _replace_result_program(results, population["candidates"][0]["candidateId"], None)
    with pytest.raises(TemporalDiscoveryContractError, match="program identity"):
        analyze_policy_ab(
            policy_root=policy_root,
            corrected_result_root=results,
            panel_bridge_root=bridge_root,
            output_root=tmp_path / "missing-program-output",
            version="fixture-v1",
        )


def test_policy_analysis_rejects_missing_or_wrong_authority_calendar_and_task_coverage(
    tmp_path: Path,
) -> None:
    repair, external = _repair(tmp_path)
    policy = build_policy_ab(reference_root=repair["outputRoot"], output_root=external, version="fixture-v1", seed=7)
    policy_root = Path(policy["outputRoot"])
    population = json.loads((policy_root / "population.json").read_text(encoding="utf-8"))
    bridge_root = _freeze_bridge(
        Path(repair["outputRoot"]) / "reference-population.json",
        tmp_path / "repair-bridge",
    )

    def reject(name: str, expected: str, mutate: object) -> None:
        results = tmp_path / name
        _write_bound_v3_results(results, population["candidates"], bridge_root)
        mutate(results)
        with pytest.raises(TemporalDiscoveryContractError, match=expected):
            analyze_policy_ab(
                policy_root=policy_root,
                corrected_result_root=results,
                panel_bridge_root=bridge_root,
                output_root=tmp_path / f"{name}-output",
                version="fixture-v1",
            )

    def remove_one_window(root: Path) -> None:
        next((root / "results").glob("*.json")).unlink()

    def wrong_calendar(root: Path) -> None:
        path = next((root / "results").glob("*.json"))
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["analysis_window_start"] = "2022-01-01T00:00:00Z"
        _dump(path, payload)

    def add_extra_task(root: Path) -> None:
        path = root / "task-manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        extra = json.loads(json.dumps(manifest["tasks"][0]))
        extra["task_id"] = "temporal-search-extra-task"
        extra["attempt_id"] = extra["task_id"]
        extra["payload"]["job_id"] = extra["task_id"]
        extra["payload"]["attempt_id"] = extra["task_id"]
        manifest["tasks"].append(extra)
        manifest["taskCount"] = len(manifest["tasks"])
        manifest["taskMatrixSha256"] = _sha(manifest["tasks"])
        _dump(path, manifest)

    def wrong_authority(root: Path) -> None:
        authority = json.loads((root / "authority.json").read_text(encoding="utf-8"))
        _dump(root / "authority.json", _authority_with_label(authority, "wrong-panel-authority"))

    reject("missing-window", "do not cover every intended candidate", remove_one_window)
    reject("wrong-calendar", "does not bind to its exact frozen authority task", wrong_calendar)
    reject("extra-task", "exact frozen bridge authority/task matrix", add_extra_task)
    reject("wrong-authority", "exact frozen bridge authority/task matrix", wrong_authority)


def test_operator_builder_caps_and_suppresses_invalid_depth_one_children(tmp_path: Path) -> None:
    repair, external = _repair(tmp_path)

    class StaticValidator:
        def validate(self, **kwargs: object) -> dict:
            source_sha = str(kwargs["expected_raw_source_profile_sha256"])
            return {
                "candidateAcceptable": True,
                "programSha256": _sha({"program": source_sha}),
                "profileSnapshotSha256": source_sha,
                "validationReportSha256": _sha({"report": source_sha}),
            }

    result = build_operator_panel(
        reference_root=repair["outputRoot"],
        output_root=external,
        version="fixture-v1",
        seed=7,
        catalog={"timeframes": {"M5": {}}, "indicators": [{"meta": {"id": "fixture"}}]},
        validator=StaticValidator(),
    )
    panel = json.loads((Path(result["outputRoot"]) / "operator-panel.json").read_text(encoding="utf-8"))

    assert result["parentCount"] == 12
    assert result["candidateCount"] <= 64
    assert any(row.get("suppressionReason") == "static_construction_reachability_failed" for row in panel["applications"])
    assert all(row.get("disposition") != "admitted" or row["childCandidateId"].startswith("stage5e7v3_op_") for row in panel["applications"])


def test_operator_analysis_enforces_noop_equality_and_pairs(tmp_path: Path) -> None:
    external = tmp_path / "external-output"
    operator_root = external / "stage5e7-v3-validation-fixture-v1" / "operator"
    profile = _candidate("profile_fixture", 0)["sourceProfile"]
    parent = {"candidateId": "parent_a", "sourceMode": "fixture", "seedId": "fixture", "sourceProfile": profile, "sourceProfileSha256": _sha(profile), "profileSnapshotSha256": _sha(profile), "programSha256": _sha("parent")}
    control = {**parent, "candidateId": "control_a", "candidateIdentitySha256": _sha("control")}
    child_profile = json.loads(json.dumps(profile))
    child_profile["directionMode"] = "short"
    child = {"candidateId": "child_a", "sourceMode": "fixture_child", "seedId": "fixture", "sourceProfile": child_profile, "sourceProfileSha256": _sha(child_profile), "profileSnapshotSha256": _sha(child_profile), "programSha256": _sha("child"), "candidateIdentitySha256": _sha("child")}
    baselines = {"schemaVersion": "stage5e7_v3_operator_parent_baselines_v1", "candidateCount": 1, "candidates": [parent]}
    baselines["populationSha256"] = _sha(baselines)
    population = {"schemaVersion": "temporal_qd_generation_population_v3", "candidateCount": 2, "candidates": [control, child], "sourceReferencePopulationSha256": _sha("reference")}
    population["populationSha256"] = _sha(population)
    application = {"parentCandidateId": "parent_a", "childCandidateId": "child_a", "operatorId": "fixture_operator", "plannedApplicationSha256": _sha("application"), "disposition": "admitted"}
    panel = {"schemaVersion": "stage5e7_v3_operator_causal_panel_v1", "populationSha256": population["populationSha256"], "parentBaselinePopulationSha256": baselines["populationSha256"], "pairs": [{"parentCandidateId": "parent_a", "controlCandidateId": "control_a"}], "applications": [application]}
    panel["operatorPanelSha256"] = _sha(panel)
    _dump(operator_root / "population.json", population)
    _dump(operator_root / "parent-baselines.json", baselines)
    _dump(operator_root / "operator-panel.json", panel)
    operator_bridge_root = _freeze_bridge(operator_root / "population.json", tmp_path / "operator-bridge")
    parent_bridge_root = _freeze_bridge(operator_root / "parent-baselines.json", tmp_path / "operator-parent-bridge")
    parent_results, paired_results = tmp_path / "parent-results", tmp_path / "paired-results"
    _write_bound_v3_results(parent_results, [parent], parent_bridge_root, {"parent_a": (1.0, 3, 10.0)})
    _write_bound_v3_results(paired_results, [control, child], operator_bridge_root, {"control_a": (1.0, 3, 10.0), "child_a": (2.0, 3, 10.0)})

    analysis = analyze_operator_panel(
        operator_root=operator_root,
        corrected_result_root=paired_results,
        parent_corrected_result_root=parent_results,
        operator_panel_bridge_root=operator_bridge_root,
        parent_panel_bridge_root=parent_bridge_root,
        output_root=external,
        version="fixture-v1",
    )

    assert analysis["analysisSha256"].startswith("sha256:")
    _replace_result_program(parent_results, "parent_a", "sha256:" + "f" * 64)
    with pytest.raises(TemporalDiscoveryContractError, match="program identity does not match"):
        analyze_operator_panel(
            operator_root=operator_root,
            corrected_result_root=paired_results,
            parent_corrected_result_root=parent_results,
            operator_panel_bridge_root=operator_bridge_root,
            parent_panel_bridge_root=parent_bridge_root,
            output_root=tmp_path / "wrong-parent-program-output",
            version="fixture-v1",
        )
    _write_bound_v3_results(parent_results, [parent], parent_bridge_root, {"parent_a": (1.0, 3, 10.0)})
    _replace_result_program(paired_results, "control_a", "sha256:" + "e" * 64)
    with pytest.raises(TemporalDiscoveryContractError, match="program identity does not match"):
        analyze_operator_panel(
            operator_root=operator_root,
            corrected_result_root=paired_results,
            parent_corrected_result_root=parent_results,
            operator_panel_bridge_root=operator_bridge_root,
            parent_panel_bridge_root=parent_bridge_root,
            output_root=tmp_path / "wrong-control-program-output",
            version="fixture-v1",
        )
    _write_bound_v3_results(paired_results, [control, child], operator_bridge_root, {"control_a": (0.5, 3, 10.0), "child_a": (2.0, 3, 10.0)})
    with pytest.raises(TemporalDiscoveryContractError, match="no-op corrected-result equality failed"):
        analyze_operator_panel(
            operator_root=operator_root,
            corrected_result_root=paired_results,
            parent_corrected_result_root=parent_results,
            operator_panel_bridge_root=operator_bridge_root,
            parent_panel_bridge_root=parent_bridge_root,
            output_root=tmp_path / "external-output-2",
            version="fixture-v1",
        )
