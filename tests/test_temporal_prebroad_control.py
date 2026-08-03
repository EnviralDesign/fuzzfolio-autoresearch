from __future__ import annotations

import pytest

from autoresearch.temporal_prebroad_control import (
    WINDOWS,
    build_prebroad_authority,
    materialize_prebroad_matrix,
    validate_prebroad_authority,
)
from autoresearch.temporal_search import TemporalSearchContractError, canonical_sha256


SHA = "sha256:" + "a" * 64


def _plan(profile_sha: str, window_id: str, start: str, end: str) -> dict:
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": profile_sha,
        "analysis_window_start": start,
        "analysis_window_end": end,
        "campaign_plan_id": f"test:{window_id}",
        "coverage_policy": "require_complete",
        "data_availability_cutoff": "2026-01-01T00:00:00Z",
        "evidence_role": "development_parity",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "requested_horizon_months": 1,
        "selection_data_end": end,
        "lake_window_binding": {
            "schema_version": "fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "request": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
                "data_start": start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
            "window_semantic_sha256": SHA,
            "attestation_sha256": SHA,
            "creation_global_coverage_sha256": SHA,
            "creation_source_coverage_sha256": SHA,
            "legacy_selection_manifest_sha256": None,
        },
    }
    identity = dict(plan)
    identity.pop("lake_manifest_sha256")
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _accepted_pairs() -> dict:
    pairs = []
    for index in range(8):
        candidate = f"candidate_{index}"
        profile = {"version": "v3", "directionMode": "both", "instruments": ["EURUSD"]}
        profile_sha = canonical_sha256(profile)
        pairs.append(
            {
                "candidateId": candidate,
                "profile": profile,
                "profileSha256": profile_sha,
                "validation": {
                    "candidateId": candidate,
                    "candidateAcceptable": True,
                    "status": "valid_evaluable",
                    "programSha256": SHA,
                    "validationReportSha256": SHA,
                    "rawSourceProfileSha256": profile_sha,
                    "profileSnapshotSha256": SHA,
                    "evaluatorId": "bar_bidirectional_single_position_execution_v2",
                },
                "timeframe": "M5",
                "barLimit": 500,
                "windowInputs": [
                    {"windowId": window_id, "evidencePlan": _plan(profile_sha, window_id, start, end)}
                    for window_id, start, end in WINDOWS
                ],
            }
        )
    return {
        "schemaVersion": "temporal_prebroad_accepted_pairs_v1",
        "workerContract": {"workerContractSha256": SHA, "workerContractSchema": "replay-worker-contract-v1"},
        "pairs": pairs,
    }


def _reports(accepted: dict, *, forged: bool = False) -> dict[str, dict]:
    reports = {}
    for pair in accepted["pairs"]:
        report = dict(pair["validation"])
        if forged:
            report["programSha256"] = "sha256:" + "b" * 64
        reports[pair["candidateId"]] = report
    return reports


def test_prebroad_authority_is_exactly_eight_by_two_and_no_dispatch(tmp_path) -> None:
    accepted = _accepted_pairs()
    reports = _reports(accepted)
    authority = build_prebroad_authority(accepted, native_reports=reports)
    assert validate_prebroad_authority(authority, native_reports=reports) == authority
    assert authority["schemaVersion"] == "temporal_prebroad_authority_v1"
    assert authority["candidateCount"] == 8
    assert authority["taskCount"] == 16
    assert authority["costViews"] == ["research_conservative", "none"]
    assert authority["executionPolicy"] == {
        "reservedEvidencePermitted": False,
        "longEconomicSearchPermitted": False,
        "taskDispatchPermitted": False,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    result = materialize_prebroad_matrix(
        authority,
        tmp_path,
        required_authority_id=authority["authorityId"],
        resume=False,
        native_reports=reports,
    )
    assert result["taskCount"] == 16
    assert result["taskDispatchPermitted"] is False


def test_prebroad_authority_fails_closed_without_exactly_eight_pairs() -> None:
    accepted = _accepted_pairs()
    accepted["pairs"] = accepted["pairs"][:-1]
    with pytest.raises(TemporalSearchContractError, match="exactly eight"):
        build_prebroad_authority(accepted, native_reports=_reports(accepted))


def test_prebroad_matrix_rejects_different_required_frozen_hash(tmp_path) -> None:
    accepted = _accepted_pairs()
    reports = _reports(accepted)
    authority = build_prebroad_authority(accepted, native_reports=reports)
    with pytest.raises(TemporalSearchContractError, match="frozen authority hash"):
        materialize_prebroad_matrix(authority, tmp_path, required_authority_id=SHA, resume=True, native_reports=reports)


def test_prebroad_authority_rejects_forged_native_program_identity() -> None:
    accepted = _accepted_pairs()
    with pytest.raises(TemporalSearchContractError, match="forged"):
        build_prebroad_authority(accepted, native_reports=_reports(accepted, forged=True))
