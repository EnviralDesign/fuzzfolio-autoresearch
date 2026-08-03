from __future__ import annotations

import json

import pytest

from autoresearch.temporal_prebroad_canary import _validate_input, audit_canary
from autoresearch.temporal_search import TemporalSearchContractError, canonical_sha256


def test_canary_rejects_artifacts_without_all_required_activation_scenarios() -> None:
    payload = {"schemaVersion": "temporal_prebroad_activation_canary_input_v1", "pairs": []}
    with pytest.raises(TemporalSearchContractError, match="finite non-empty"):
        _validate_input(payload)


def test_canary_input_schema_is_closed() -> None:
    payload = {
        "schemaVersion": "temporal_prebroad_activation_canary_input_v1",
        "pairs": [],
        "gatewayUrl": "http://127.0.0.1:8799",
    }
    with pytest.raises(TemporalSearchContractError, match="unknown or open schema"):
        _validate_input(payload)


def test_canary_audit_rejects_self_consistent_but_open_report(tmp_path) -> None:
    report = {"schemaVersion": "temporal_prebroad_activation_canary_report_v1"}
    report["reportSha256"] = canonical_sha256(report)
    (tmp_path / "activation-canary.json").write_text(
        json.dumps(report), encoding="utf-8"
    )
    with pytest.raises(TemporalSearchContractError, match="open or unsafe"):
        audit_canary(tmp_path)


def _sha(char: str) -> str:
    return "sha256:" + char * 64


def _closed_report(*, scenarios: list[dict], mappings: list[dict], outcomes: list[dict]) -> dict:
    return {
        "schemaVersion": "temporal_prebroad_activation_canary_report_v1",
        "inputSha256": _sha("a"),
        "pairCount": 1,
        "scenarioCount": 4,
        "productionOutcomeCount": len(outcomes),
        "marketEvidenceRead": False,
        "lakeRead": False,
        "gatewayContacted": False,
        "offlineAuditTrustBoundary": {
            "streamProvenance": "not_recomputable_without_the_input_artifact",
            "resultPayloadsEmbedded": False,
            "runTimeBinding": "Dashboard replay verified result stream/profile identities before reporting",
        },
        "dashboardAuthority": {
            "bidirectionalCompiler": "fuzzfolio_core.temporal_graph.bidirectional_compiler.compile_bidirectional_profile",
            "nativeValidator": "fuzzfolio_core.temporal_graph.search_validation.validate_temporal_search_candidate",
            "replayKernel": "fuzzfolio_core.temporal_graph.sequential_replay.run_temporal_replay",
        },
        "compiledPairs": [{
            "candidateId": "candidate_a",
            "compiledProfileSha256": _sha("b"),
            "compiledProgramSha256": _sha("c"),
            "compiledValidationReportSha256": _sha("d"),
            "authoritativeProductionMappings": mappings,
            "scenarios": scenarios,
        }],
        "productionLifecycleOutcomes": outcomes,
    }


def _write_report(tmp_path, report: dict) -> None:
    report["reportSha256"] = canonical_sha256(report)
    (tmp_path / "activation-canary.json").write_text(json.dumps(report), encoding="utf-8")


def test_canary_audit_rejects_rehashed_missing_scenario_outcome(tmp_path) -> None:
    _write_report(tmp_path, _closed_report(scenarios=[], mappings=[], outcomes=[]))
    with pytest.raises(TemporalSearchContractError, match="exactly four scenarios"):
        audit_canary(tmp_path)


def test_canary_audit_rejects_rehashed_subset_production_outcomes(tmp_path) -> None:
    scenarios = [
        {
            "scenarioId": outcome,
            "expectedOutcome": outcome,
            "observedOutcome": outcome,
            "resultSha256": _sha("e"),
            "restartResultSha256": _sha("e"),
            "streamSha256": _sha("f"),
            "profileSnapshotSha256": _sha("b"),
            "transitionIds": [],
            "execution": [],
        }
        for outcome in ("long", "short", "neither", "conflict_abstention")
    ]
    mappings = [{
        "moduleDirection": "long",
        "fragmentIndex": 0,
        "productionId": "arm_level",
        "lifecycles": [{"lifecycle": "armed", "transitionId": "long_f0_arm_level_arm", "actionKind": None}],
    }, {
        "moduleDirection": "short",
        "fragmentIndex": 0,
        "productionId": "arm_level",
        "lifecycles": [{"lifecycle": "armed", "transitionId": "short_f0_arm_level_arm", "actionKind": None}],
    }]
    _write_report(tmp_path, _closed_report(scenarios=scenarios, mappings=mappings, outcomes=[]))
    with pytest.raises(TemporalSearchContractError, match="duplicate production outcomes"):
        audit_canary(tmp_path)


def test_canary_audit_rejects_rehashed_duplicate_scenario_id(tmp_path) -> None:
    scenarios = [
        {"scenarioId": "same", "expectedOutcome": outcome, "observedOutcome": outcome,
         "resultSha256": _sha("e"), "restartResultSha256": _sha("e"), "streamSha256": _sha("f"),
         "profileSnapshotSha256": _sha("b"), "transitionIds": [], "execution": []}
        for outcome in ("long", "short", "neither", "conflict_abstention")
    ]
    _write_report(tmp_path, _closed_report(scenarios=scenarios, mappings=[], outcomes=[]))
    with pytest.raises(TemporalSearchContractError, match="scenario report is invalid"):
        audit_canary(tmp_path)


def test_canary_audit_rejects_rehashed_empty_mapping_side(tmp_path) -> None:
    scenarios = [
        {"scenarioId": outcome, "expectedOutcome": outcome, "observedOutcome": outcome,
         "resultSha256": _sha("e"), "restartResultSha256": _sha("e"), "streamSha256": _sha("f"),
         "profileSnapshotSha256": _sha("b"), "transitionIds": [], "execution": []}
        for outcome in ("long", "short", "neither", "conflict_abstention")
    ]
    mappings = [{"moduleDirection": "long", "fragmentIndex": 0, "productionId": "arm_level", "lifecycles": [{"lifecycle": "armed", "transitionId": "long_f0_arm_level_arm", "actionKind": None}]}]
    _write_report(tmp_path, _closed_report(scenarios=scenarios, mappings=mappings, outcomes=[]))
    with pytest.raises(TemporalSearchContractError, match="fragment mapping is incomplete"):
        audit_canary(tmp_path)


def test_canary_audit_rejects_rehashed_mismatched_scenario_profile(tmp_path) -> None:
    scenarios = [
        {"scenarioId": outcome, "expectedOutcome": outcome, "observedOutcome": outcome,
         "resultSha256": _sha("e"), "restartResultSha256": _sha("e"), "streamSha256": _sha("f"),
         "profileSnapshotSha256": _sha("a"), "transitionIds": [], "execution": []}
        for outcome in ("long", "short", "neither", "conflict_abstention")
    ]
    _write_report(tmp_path, _closed_report(scenarios=scenarios, mappings=[], outcomes=[]))
    with pytest.raises(TemporalSearchContractError, match="scenario report is invalid"):
        audit_canary(tmp_path)
