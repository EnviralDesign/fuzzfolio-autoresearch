from __future__ import annotations

import copy
import json
from pathlib import Path
import random

import pytest

from autoresearch.temporal_discovery import (
    TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
    _rotate_evidence_plan,
    audit_discovery,
    finalize_discovery,
    fingerprint_distance,
    generate_discovery,
    pareto_fronts,
    select_confirmation_stage,
)
from autoresearch.temporal_search import canonical_sha256


class FakeValidator:
    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: dict,
        expected_raw_source_profile_sha256: str,
    ) -> dict:
        assert canonical_sha256(source_profile) == expected_raw_source_profile_sha256
        semantic = {
            key: value
            for key, value in source_profile.items()
            if key not in {"name", "description"}
        }
        program = canonical_sha256(
            {
                "schemaVersion": "fake_program_v1",
                "profile": semantic,
            }
        )
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": expected_raw_source_profile_sha256,
            "profileSnapshotSha256": canonical_sha256(
                {"normalized": source_profile}
            ),
            "programSha256": program,
            "evaluatorId": "bar_single_position_execution_v1",
            "status": "valid_evaluable",
            "scoreable": False,
            "candidateAcceptable": True,
            "hasEntryAction": True,
            "hasExplicitManagementLibrary": True,
            "requiredCapabilities": [],
            "missingCapabilities": [],
            "fidelityRequirements": ["data.completed_ohlc"],
            "missingFidelityCapabilities": [],
            "issues": [],
            "validationReportSha256": canonical_sha256(
                {"candidateId": candidate_id, "programSha256": program}
            ),
        }


def _transition(
    transition_id: str,
    source: str,
    destination: str,
    event_class: str,
    guard: dict,
    *,
    actions: list[dict] | None = None,
    priority: int = 10,
) -> dict:
    return {
        "id": transition_id,
        "sourceStateId": source,
        "destinationStateId": destination,
        "eventClass": event_class,
        "priority": priority,
        "guard": guard,
        "actions": actions or [],
        "reasonCode": transition_id,
    }


def _seed_profile(seed_id: str, threshold: float) -> dict:
    return {
        "version": "v2",
        "name": f"Seed {seed_id}",
        "description": "Discovery test seed.",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [
            {
                "indicatorId": "FAKE_SCORE",
                "instanceId": "score_m5",
                "timeframe": "M5",
                "parameters": {"period": 14},
                "catalogSha256": "sha256:" + "1" * 64,
            }
        ],
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "search_plan",
                "plans": [
                    {
                        "id": "search_plan",
                        "initialStop": {
                            "kind": "fixed_percent",
                            "percent": 1.0,
                        },
                        "initialTarget": {
                            "kind": "reward_multiple",
                            "multiple": 2.0,
                        },
                        "trailingStop": {
                            "anchor": {"kind": "bar_close"},
                            "distance": {
                                "kind": "fixed_initial_r",
                                "multiple": 1.0,
                            },
                            "activation": {
                                "kind": "after_unrealized_r",
                                "value": 1.0,
                            },
                            "minimumStepInitialR": 0.25,
                        },
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
            "states": [
                {"id": "flat"},
                {"id": "armed"},
                {"id": "entry_requested"},
                {"id": "open"},
                {"id": "exit_requested"},
                {"id": "done"},
            ],
            "evidenceGroups": [
                {
                    "id": "context",
                    "indicatorInstanceIds": ["score_m5"],
                }
            ],
            "eventBindings": [],
            "transitions": [
                _transition(
                    "flat_to_armed",
                    "flat",
                    "armed",
                    "decision",
                    {
                        "kind": "evidence_at_least",
                        "groupId": "context",
                        "thresholdPercent": threshold,
                    },
                ),
                _transition(
                    "armed_to_entry",
                    "armed",
                    "entry_requested",
                    "decision",
                    {
                        "kind": "all",
                        "guards": [
                            {"kind": "state_age_at_least", "events": 2},
                            {
                                "kind": "evidence_at_least",
                                "groupId": "context",
                                "thresholdPercent": threshold,
                            },
                        ],
                    },
                    actions=[
                        {
                            "kind": "enter_next_open",
                            "managementPlanId": "search_plan",
                        }
                    ],
                ),
                _transition(
                    "entry_filled",
                    "entry_requested",
                    "open",
                    "execution",
                    {"kind": "execution_status_is", "status": "filled"},
                ),
                _transition(
                    "open_to_exit",
                    "open",
                    "exit_requested",
                    "decision",
                    {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {"kind": "position_age_at_least", "events": 8},
                            {
                                "kind": "unrealized_r_at_least",
                                "value": 1.0,
                            },
                        ],
                    },
                    actions=[{"kind": "exit_next_open"}],
                ),
                _transition(
                    "open_closed",
                    "open",
                    "done",
                    "execution",
                    {"kind": "execution_status_is", "status": "closed"},
                ),
                _transition(
                    "exit_closed",
                    "exit_requested",
                    "done",
                    "execution",
                    {"kind": "execution_status_is", "status": "closed"},
                ),
            ],
        },
    }


def _plan(window: dict, profile_sha: str) -> dict:
    request = {
        "schema_version": "fuzzfolio.market-data-window-request.v1",
        "dataset": "bars",
        "pairs": ["EURUSD"],
        "timeframes": ["M5"],
        "data_start": window["analysisWindowStart"],
        "data_end": window["analysisWindowEnd"],
        "coverage_policy": "require_complete",
    }
    binding = {
        "schema_version": "fuzzfolio.market-data-window-binding.v1",
        "semantic_contract_id": "test.semantic.v1",
        "window_semantic_sha256": canonical_sha256(request),
        "request": request,
        "attestation_sha256": "sha256:" + "2" * 64,
        "creation_global_coverage_sha256": "sha256:" + "3" * 64,
        "creation_source_coverage_sha256": "sha256:" + "4" * 64,
        "legacy_selection_manifest_sha256": None,
    }
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": profile_sha,
        "analysis_window_start": window["analysisWindowStart"],
        "analysis_window_end": window["analysisWindowEnd"],
        "requested_horizon_months": 1,
        "selection_data_end": window["analysisWindowEnd"],
        "data_availability_cutoff": window["analysisWindowEnd"],
        "evidence_role": "development",
        "coverage_policy": "require_complete",
        "campaign_plan_id": "test-discovery",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "lake_window_binding": binding,
    }
    identity = dict(plan)
    identity.pop("lake_manifest_sha256")
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _preparation(target: int = 12) -> dict:
    windows = [
        {
            "windowId": "window_a",
            "analysisWindowStart": "2021-01-01T00:00:00Z",
            "analysisWindowEnd": "2021-02-01T00:00:00Z",
        },
        {
            "windowId": "window_b",
            "analysisWindowStart": "2021-03-01T00:00:00Z",
            "analysisWindowEnd": "2021-04-01T00:00:00Z",
        },
        {
            "windowId": "window_c",
            "analysisWindowStart": "2021-05-01T00:00:00Z",
            "analysisWindowEnd": "2021-06-01T00:00:00Z",
        },
        {
            "windowId": "window_d",
            "analysisWindowStart": "2021-07-01T00:00:00Z",
            "analysisWindowEnd": "2021-08-01T00:00:00Z",
        },
    ]
    seed = _seed_profile("one", 55.0)
    seed_sha = canonical_sha256(seed)
    return {
        "schemaVersion": TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
        "authorityLabel": "discovery-test",
        "generator": {
            "seed": 20260731,
            "targetUniquePrograms": target,
            "deNovoFraction": 0.67,
            "maxProposalAttempts": 500,
            "deNovoMutationCount": {"min": 3, "max": 5},
            "seedMutationCount": {"min": 1, "max": 2},
        },
        "validation": {
            "validatorSchema": "temporal_search_candidate_validation_v1",
            "fuzzfolioCommit": "b" * 40,
        },
        "workerContract": {
            "workerContractSha256": "sha256:" + "5" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "instrument": "EURUSD",
        "timeframe": "M5",
        "barLimit": 5000,
        "seeds": [
            {"seedId": "one", "sourceProfile": seed},
            {
                "seedId": "two",
                "sourceProfile": _seed_profile("two", 65.0),
            },
        ],
        "developmentWindows": windows,
        "evidencePlanTemplates": [
            {
                "windowId": window["windowId"],
                "evidencePlan": _plan(window, seed_sha),
            }
            for window in windows
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2026-01-01T00:00:00Z",
                "analysisWindowEnd": "2026-07-01T00:00:00Z",
                "reason": "reserved evidence",
            }
        ],
        "screening": {
            "initialWindowIds": ["window_a", "window_c"],
            "confirmationWindowIds": ["window_b", "window_d"],
            "economicArchiveSize": 5,
            "noveltyArchiveSize": 5,
            "confirmationCandidateCap": 7,
            "minimumTradesPerInitialWindowEconomic": 2,
            "minimumTotalTradesNovelty": 2,
            "finalEconomicArchiveSize": 4,
            "finalNoveltyArchiveSize": 4,
        },
        "bounds": {
            "maxCandidates": target,
            "maxInitialTasks": target * 2,
            "maxConfirmationCandidates": 7,
            "maxConfirmationTasks": 14,
            "maxTotalTasks": target * 2 + 14,
            "maxAttempts": 2,
            "deadlineSeconds": 7200,
        },
    }


def _write_results(
    root: Path,
    *,
    authority: dict,
    population: dict,
    window_ids: list[str],
    candidate_ids: list[str] | None = None,
    reverse: bool = False,
) -> None:
    root.joinpath("results").mkdir(parents=True)
    candidates = {
        item["candidateId"]: item
        for item in population["candidates"]
    }
    selected = candidate_ids or sorted(candidates)
    window_map = {
        item["windowId"]: item
        for item in authority["developmentWindows"]
    }
    rows = []
    for candidate_index, candidate_id in enumerate(selected):
        for window_index, window_id in enumerate(window_ids):
            window = window_map[window_id]
            trades = 3 + ((candidate_index + window_index) % 9)
            base = (candidate_index % 5) - 2 + window_index * 0.25
            conservative = base - trades * 0.1
            no_cost = base
            drawdown = abs(min(conservative, 0.0)) + candidate_index * 0.05
            action = (
                "tighten_stop_next_open"
                if candidate_index % 2
                else "activate_trailing_stop_next_open"
            )
            close = (
                "trailing_stop"
                if candidate_index % 3
                else "discretionary_exit"
            )
            stream = canonical_sha256(
                {
                    "candidateId": candidate_id,
                    "windowId": window_id,
                }
            )
            metrics = {
                "observationsProcessed": 1000,
                "decisionEventCount": 1000,
                "executionEventCount": trades * 3,
                "intentsScheduled": trades * 2,
                "intentsApplied": trades * 2,
                "intentsRejected": 0,
                "intentsCanceled": 0,
                "positionsOpened": trades,
                "tradesClosed": trades,
                "unresolvedPosition": False,
                "unresolvedPendingEffect": False,
                "wins": max(0, trades // 2),
                "losses": trades - trades // 2,
                "flatTrades": 0,
                "totalGrossR": no_cost,
                "totalNetR": conservative,
                "averageNetR": conservative / trades,
                "totalExecutionCostPercent": trades * 0.1,
                "winRate": (trades // 2) / trades,
                "profitFactor": max(0.0, 1.0 + conservative / 10.0),
                "maxDrawdownR": drawdown,
                "averageHoldingBars": 4.0 + candidate_index,
                "maxHoldingBars": 20,
                "averageHoldingHours": 1.0,
                "maxHoldingHours": 2.0,
                "exposureObservations": trades * (4 + candidate_index),
                "exposureRatio": min(
                    1.0,
                    trades * (4 + candidate_index) / 1000.0,
                ),
                "transitionEntropy": 0.5 + candidate_index * 0.1,
                "equityCurveR": [],
                "actionCounts": {action: trades},
                "transitionCounts": {
                    f"route_{candidate_index % 4}": trades
                },
                "stateOccupancy": {
                    f"state_{candidate_index % 3}": trades * 4
                },
                "closeReasonCounts": {close: trades},
            }
            no_cost_metrics = copy.deepcopy(metrics)
            no_cost_metrics["totalNetR"] = no_cost
            no_cost_metrics["averageNetR"] = no_cost / trades
            rows.append(
                {
                    "schema_version": "temporal_graph_candidate_window_result_v1",
                    "candidate_id": candidate_id,
                    "analysis_window_start": window["analysisWindowStart"],
                    "analysis_window_end": window["analysisWindowEnd"],
                    "program_sha256": candidates[candidate_id][
                        "programSha256"
                    ],
                    "observation_stream_sha256": stream,
                    "cost_view_results": {
                        "research_conservative": {
                            "replay_result": {
                                "streamSha256": stream,
                                "metrics": metrics,
                            }
                        },
                        "none": {
                            "replay_result": {
                                "streamSha256": stream,
                                "metrics": no_cost_metrics,
                            }
                        },
                    },
                }
            )
    if reverse:
        rows.reverse()
    for index, row in enumerate(rows):
        root.joinpath("results", f"result-{index:04d}.json").write_text(
            json.dumps(row, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def test_generation_is_deterministic_and_program_deduplicated(
    tmp_path: Path,
) -> None:
    preparation = _preparation()
    left_root = tmp_path / "left"
    right_root = tmp_path / "right"

    left = generate_discovery(
        preparation,
        validator=FakeValidator(),
        output_root=left_root,
    )
    right = generate_discovery(
        preparation,
        validator=FakeValidator(),
        output_root=right_root,
    )

    assert left["authorityId"] == right["authorityId"]
    assert left["populationSha256"] == right["populationSha256"]
    left_population = json.loads(
        (left_root / "population.json").read_text(encoding="utf-8")
    )
    right_population = json.loads(
        (right_root / "population.json").read_text(encoding="utf-8")
    )
    assert left_population == right_population
    assert left_population["candidateCount"] == 12
    assert left_population["deNovoCount"] == round(12 * 0.67)
    assert len(
        {item["programSha256"] for item in left_population["candidates"]}
    ) == 12
    initial_authority = json.loads(
        (left_root / "initial" / "authority.json").read_text(
            encoding="utf-8"
        )
    )
    for candidate in initial_authority["candidates"]:
        for window_input in candidate["windowInputs"]:
            plan = window_input["evidencePlan"]
            assert "execution_cell_sha256" in plan
            assert plan["execution_cell_sha256"] is None
            identity = dict(plan)
            identity.pop("plan_id")
            identity.pop("lake_manifest_sha256", None)
            assert plan["plan_id"] == canonical_sha256(identity)
    assert audit_discovery(left_root)["ok"] is True


def test_evidence_plan_rotation_binds_legacy_execution_cell() -> None:
    profile = _seed_profile("legacy", 55.0)
    selected_cell = {
        "stopLossPercent": 0.75,
        "rewardMultiple": 2.0,
    }
    profile["executionConfig"] = {
        "exitPolicy": {"selectedCell": selected_cell}
    }
    template = _plan(
        {
            "analysisWindowStart": "2021-01-01T00:00:00Z",
            "analysisWindowEnd": "2021-02-01T00:00:00Z",
        },
        canonical_sha256(profile),
    )

    rotated = _rotate_evidence_plan(
        template,
        raw_source_profile_sha256=canonical_sha256(profile),
        source_profile=profile,
    )

    assert rotated["execution_cell_sha256"] == canonical_sha256(selected_cell)
    identity = dict(rotated)
    identity.pop("plan_id")
    identity.pop("lake_manifest_sha256", None)
    assert rotated["plan_id"] == canonical_sha256(identity)


def test_progressive_selection_is_order_independent_and_finalizes(
    tmp_path: Path,
) -> None:
    discovery_root = tmp_path / "discovery"
    generate_discovery(
        _preparation(),
        validator=FakeValidator(),
        output_root=discovery_root,
    )
    population = json.loads(
        (discovery_root / "population.json").read_text(encoding="utf-8")
    )
    initial_authority = json.loads(
        (discovery_root / "initial" / "authority.json").read_text(
            encoding="utf-8"
        )
    )
    initial_root = tmp_path / "initial-results"
    _write_results(
        initial_root,
        authority=initial_authority,
        population=population,
        window_ids=["window_a", "window_c"],
        reverse=True,
    )

    selection = select_confirmation_stage(
        discovery_root,
        initial_result_root=initial_root,
    )
    assert selection["confirmationCandidateCount"] == 7
    selection_payload = json.loads(
        (
            discovery_root
            / "screening"
            / "initial-selection.json"
        ).read_text(encoding="utf-8")
    )
    assert len(selection_payload["economicArchive"]) == 5
    assert len(selection_payload["noveltyArchive"]) == 5
    assert len(selection_payload["confirmationCandidateIds"]) == 7

    confirmation_authority = json.loads(
        (discovery_root / "confirmation" / "authority.json").read_text(
            encoding="utf-8"
        )
    )
    confirmation_root = tmp_path / "confirmation-results"
    _write_results(
        confirmation_root,
        authority=confirmation_authority,
        population=population,
        window_ids=["window_b", "window_d"],
        candidate_ids=selection_payload["confirmationCandidateIds"],
    )
    final = finalize_discovery(
        discovery_root,
        initial_result_root=initial_root,
        confirmation_result_root=confirmation_root,
    )
    assert final["completeCandidateCount"] == 7
    assert final["economicArchiveCount"] == 4
    assert final["noveltyArchiveCount"] == 4
    report = json.loads(
        (discovery_root / "final" / "report.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["funnel"]["initialTaskCount"] == 24
    assert report["funnel"]["confirmationTaskCount"] == 14
    assert audit_discovery(discovery_root)["ok"] is True


def test_pareto_and_novelty_are_distinct_transparent_paths() -> None:
    candidates = []
    for index in range(4):
        candidates.append(
            {
                "candidateId": f"candidate_{index}",
                "totalConservativeNetR": float(index),
                "worstWindowConservativeNetR": float(index - 1),
                "profitableWindowCount": index,
                "maxWindowDrawdownR": float(4 - index),
                "costDragR": float(4 - index),
                "entryFrequencyPerThousand": float(index + 1),
                "averageExposureRatio": index / 4,
                "averageHoldingBars": float(2**index),
                "averageWinRate": index / 4,
                "averageTransitionEntropy": float(index),
                "averageMfeR": float(index) / 3.0,
                "averageMaeR": -float(index + 1) / 4.0,
                "equityShape": [float(index), float(index) / 2.0],
                "entryHourDistribution": {str(index): 1.0},
                "actionDistribution": {f"action_{index}": 1.0},
                "closeReasonDistribution": {f"close_{index}": 1.0},
                "stateOccupancyDistribution": {f"state_{index}": 1.0},
                "complexity": {
                    "stateCount": index + 1,
                    "transitionCount": index + 2,
                    "indicatorCount": 1,
                    "managementPlanCount": 1,
                },
            }
        )
    fronts = pareto_fronts(candidates)
    assert fronts[0][0]["candidateId"] == "candidate_3"
    assert fingerprint_distance(candidates[0], candidates[0]) == 0.0
    assert fingerprint_distance(candidates[0], candidates[3]) > 0.0
