from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_prebroad_accepted_pairs import (
    WORKER_CONTRACT_SHA256,
    build_accepted_pairs,
)
from autoresearch.temporal_prebroad_control import WINDOWS, build_prebroad_authority
from autoresearch.temporal_search import TemporalSearchContractError, canonical_sha256


def _profile(index: int) -> dict:
    return {
        "version": "v3",
        "directionMode": "both",
        "instruments": ["EURUSD"],
        "executionConfig": {"managementLibrary": {"version": "temporal_management_v1", "plans": [], "defaultPlanId": "search_plan"}},
        "tag": index,
    }


def _plan(window_id: str, start: str, end: str) -> dict:
    warmup_start = "2023-09-18T00:00:00Z" if start.startswith("2023-") else "2021-06-18T00:00:00Z"
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "analysis_window_start": start,
        "analysis_window_end": end,
        "campaign_plan_id": f"trusted:{window_id}",
        "coverage_policy": "require_complete",
        "data_availability_cutoff": "2026-01-01T00:00:00Z",
        "evidence_role": "development_parity",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "requested_horizon_months": 1,
        "selection_data_end": end,
        "profile_snapshot_sha256": "sha256:" + "a" * 64,
        "lake_window_binding": {
            "schema_version": "fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "request": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["M5", "M15"],
                # This is deliberate warmup, not an analysis-window equality.
                "data_start": warmup_start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "attestation_sha256": "sha256:" + "c" * 64,
            "creation_global_coverage_sha256": "sha256:" + "d" * 64,
            "creation_source_coverage_sha256": "sha256:" + "e" * 64,
            "legacy_selection_manifest_sha256": None,
        },
    }
    identity = dict(plan)
    identity.pop("lake_manifest_sha256")
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _inputs() -> tuple[dict, dict, dict]:
    population = {"candidates": []}
    reports = {}
    for index in range(8):
        candidate_id = f"qd_candidate_{index}"
        profile = _profile(index)
        profile_sha = canonical_sha256(profile)
        report = {
            "candidateId": candidate_id,
            "rawSourceProfileSha256": profile_sha,
            "profileSnapshotSha256": "sha256:" + f"{index:x}" * 64,
            "programSha256": "sha256:" + "1" * 64,
            "validationReportSha256": "sha256:" + "2" * 64,
            "evaluatorId": "bar_bidirectional_single_position_execution_v2",
            "status": "valid_evaluable",
            "candidateAcceptable": True,
        }
        population["candidates"].append({"candidateId": candidate_id, "sourceProfile": profile, "sourceProfileSha256": profile_sha})
        reports[candidate_id] = report
    template_inputs = [{"windowId": window_id, "evidencePlan": _plan(window_id, start, end)} for window_id, start, end in WINDOWS]
    preparation = {
        "workerContract": {"workerContractSha256": WORKER_CONTRACT_SHA256, "workerContractSchema": "replay-worker-contract-v1"},
        "developmentWindows": [{"windowId": window_id, "analysisWindowStart": start, "analysisWindowEnd": end} for window_id, start, end in WINDOWS],
        "candidates": [{"windowInputs": deepcopy(template_inputs)}],
    }
    return population, preparation, reports


def test_adapter_preserves_attested_request_and_uses_raw_source_profile_hash() -> None:
    population, preparation, reports = _inputs()
    accepted = build_accepted_pairs(population, preparation, native_reports=reports)
    assert [item["candidateId"] for item in accepted["pairs"]] == sorted(reports)
    assert all(item["timeframe"] == "M5" and item["barLimit"] == 5000 for item in accepted["pairs"])
    for pair in accepted["pairs"]:
        assert pair["profileSha256"] == canonical_sha256(pair["profile"])
        assert pair["profileSha256"] == pair["validation"]["rawSourceProfileSha256"]
        assert pair["profileSha256"] != pair["validation"]["profileSnapshotSha256"]
        for original, rotated in zip(preparation["candidates"][0]["windowInputs"], pair["windowInputs"], strict=True):
            assert rotated["evidencePlan"]["lake_window_binding"] == original["evidencePlan"]["lake_window_binding"]
            assert rotated["evidencePlan"]["execution_cell_sha256"] is None
            assert rotated["evidencePlan"]["requested_horizon_months"] == 1
    assert build_prebroad_authority(accepted, native_reports=reports)["taskCount"] == 16


def test_adapter_rejects_tampered_attested_binding_and_native_forgery() -> None:
    population, preparation, reports = _inputs()
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]["lake_window_binding"]["request"]["data_start"] = "2023-10-02T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="outside the immutable"):
        build_accepted_pairs(population, preparation, native_reports=reports)

    population, preparation, reports = _inputs()
    reports["qd_candidate_0"]["rawSourceProfileSha256"] = "sha256:" + "f" * 64
    with pytest.raises(TemporalSearchContractError, match="source profile identity drifted"):
        build_accepted_pairs(population, preparation, native_reports=reports)


def test_adapter_rejects_worker_contract_drift_and_protected_window_overlap() -> None:
    population, preparation, reports = _inputs()
    preparation["workerContract"]["workerContractSha256"] = "sha256:" + "f" * 64
    with pytest.raises(TemporalSearchContractError, match="required worker contract"):
        build_accepted_pairs(population, preparation, native_reports=reports)

    population, preparation, reports = _inputs()
    plan = preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["analysis_window_start"] = "2024-06-29T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="fixed month"):
        build_accepted_pairs(population, preparation, native_reports=reports)
