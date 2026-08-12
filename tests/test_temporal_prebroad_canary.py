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


def _legacy_v1_input_without_transition_aliases() -> dict:
    def module(direction: str, marker: str) -> dict:
        return {
            "context": {},
            "contextSha256": _sha(marker),
            "program": {
                "schemaVersion": "temporal_typed_fragment_grammar_v2",
                "grammarVersion": "3",
                "direction": direction,
                "fragments": [{"productionId": "arm_level"}],
            },
            "programSha256": _sha(marker.upper()),
            "nativeArtifact": {
                "schemaVersion": "temporal_prebroad_frozen_native_module_artifact_v1",
                "profile": {},
                "profileSha256": _sha("c"),
                "validation": {},
                "identities": {},
            },
        }

    scenarios = [
        {
            "scenarioId": outcome,
            "expectedOutcome": outcome,
            "restartAfterObservations": 1,
            "observationStream": {"observations": [{}, {}]},
        }
        for outcome in ("long", "short", "neither", "conflict_abstention")
    ]
    return {
        "schemaVersion": "temporal_prebroad_activation_canary_input_v1",
        "pairs": [
            {
                "candidateId": "legacy_v1_alias_compatibility",
                "longModule": module("long", "a"),
                "shortModule": module("short", "b"),
                "pair": {
                    "profile": {
                        "version": "v3",
                        "directionMode": "both",
                        "instruments": ["EURUSD"],
                    },
                    "validation": {
                        "candidateId": "legacy_v1_alias_compatibility",
                        "candidateAcceptable": True,
                        "status": "valid_evaluable",
                        "programSha256": _sha("d"),
                        "validationReportSha256": _sha("e"),
                    },
                },
                "scenarios": scenarios,
                "productionClaims": [
                    {
                        "moduleDirection": "long",
                        "fragmentIndex": 0,
                        "scenarioId": "long",
                        "lifecycle": "armed",
                        "outcome": "transition_selected",
                    },
                    {
                        "moduleDirection": "short",
                        "fragmentIndex": 0,
                        "scenarioId": "short",
                        "lifecycle": "armed",
                        "outcome": "transition_selected",
                    },
                ],
            }
        ],
    }


def test_canary_accepts_immutable_v1_input_without_transition_aliases() -> None:
    """New aliases are optional for previously written v1 artifacts."""

    payload = _legacy_v1_input_without_transition_aliases()

    validated = _validate_input(payload)
    assert "transitionAliases" not in validated["pairs"][0]["longModule"]
    assert "transitionAliases" not in validated["pairs"][0]["shortModule"]


@pytest.mark.parametrize("invalid_aliases", [None, []])
def test_canary_rejects_present_non_mapping_transition_aliases(invalid_aliases: object) -> None:
    payload = _legacy_v1_input_without_transition_aliases()
    payload["pairs"][0]["longModule"]["transitionAliases"] = invalid_aliases

    with pytest.raises(TemporalSearchContractError, match="incomplete"):
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
