from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_prepare_cli import build_pilot_preparation


def _profile(name: str) -> dict:
    return {
        "version": "v2",
        "name": name,
        "description": "pilot seed",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "core_plan",
                "plans": [
                    {
                        "id": "core_plan",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                        "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
                    }
                ],
            }
        },
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": ["data.completed_ohlc"],
            "initialStateId": "flat",
            "states": [{"id": "flat"}],
            "evidenceGroups": [],
            "eventBindings": [],
            "transitions": [],
        },
    }


def _plan(start: str, end: str) -> dict:
    return {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "plan_id": "sha256:" + "1" * 64,
        "profile_snapshot_sha256": "sha256:" + "2" * 64,
        "analysis_window_start": start,
        "analysis_window_end": end,
        "selection_data_end": end,
        "data_availability_cutoff": end,
        "requested_horizon_months": 1,
        "evidence_role": "development",
        "coverage_policy": "require_complete",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "lake_window_binding": {
            "schema_version": "fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "window_semantic_sha256": "sha256:" + "3" * 64,
            "attestation_sha256": "sha256:" + "4" * 64,
            "creation_global_coverage_sha256": "sha256:" + "5" * 64,
            "creation_source_coverage_sha256": "sha256:" + "6" * 64,
            "legacy_selection_manifest_sha256": None,
            "request": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
                "data_start": start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
        },
    }


def _input(tmp_path: Path) -> dict:
    seeds = []
    for index in range(3):
        path = tmp_path / f"seed-{index}.json"
        path.write_text(json.dumps(_profile(f"seed {index}")), encoding="utf-8")
        seeds.append({"seedId": f"seed_{index}", "profilePath": path.name})
    windows = []
    dates = [
        ("a", "2021-01-01T00:00:00Z", "2021-02-01T00:00:00Z", "initial"),
        ("b", "2021-03-01T00:00:00Z", "2021-04-01T00:00:00Z", "confirmation"),
        ("c", "2021-05-01T00:00:00Z", "2021-06-01T00:00:00Z", "initial"),
        ("d", "2021-07-01T00:00:00Z", "2021-08-01T00:00:00Z", "confirmation"),
    ]
    for window_id, start, end, stage in dates:
        path = tmp_path / f"plan-{window_id}.json"
        path.write_text(json.dumps(_plan(start, end)), encoding="utf-8")
        windows.append(
            {
                "windowId": f"window_{window_id}",
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
                "evidencePlanPath": path.name,
                "screeningStage": stage,
            }
        )
    return {
        "schemaVersion": "temporal_graph_discovery_pilot_input_v1",
        "authorityLabel": "pilot",
        "fuzzfolioCommit": "b" * 40,
        "workerContract": {
            "workerContractSha256": "sha256:" + "7" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "instrument": "EURUSD",
        "timeframe": "M5",
        "barLimit": 5000,
        "generatorSeed": 20260731,
        "seeds": seeds,
        "windows": windows,
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2026-01-01T00:00:00Z",
                "analysisWindowEnd": "2026-02-01T00:00:00Z",
                "reason": "reserved",
            }
        ],
    }


def test_builds_exact_256_program_progressive_authority(tmp_path: Path) -> None:
    result = build_pilot_preparation(_input(tmp_path), base_directory=tmp_path)
    assert result["generator"]["targetUniquePrograms"] == 256
    assert result["generator"]["deNovoFraction"] == 0.70
    assert result["screening"]["initialWindowIds"] == ["window_a", "window_c"]
    assert result["screening"]["confirmationWindowIds"] == ["window_b", "window_d"]
    assert result["screening"]["confirmationCandidateCap"] == 96
    assert result["bounds"] == {
        "maxCandidates": 256,
        "maxInitialTasks": 512,
        "maxConfirmationCandidates": 96,
        "maxConfirmationTasks": 192,
        "maxTotalTasks": 704,
        "maxAttempts": 2,
        "deadlineSeconds": 7200.0,
    }


def test_rejects_wrong_window_partition(tmp_path: Path) -> None:
    payload = _input(tmp_path)
    payload["windows"][0]["screeningStage"] = "confirmation"
    with pytest.raises(TemporalDiscoveryContractError, match="two initial"):
        build_pilot_preparation(payload, base_directory=tmp_path)
