from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_evolution as qd_module
from autoresearch.result_codec import write_gzip_json_once
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_results import (
    _aggregate_candidate,
    _window_record,
    load_stage_results,
    select_economic_archive,
)
from autoresearch.temporal_search import canonical_sha256


_START = "2024-01-01T00:00:00Z"
_END = "2024-02-01T00:00:00Z"
_LAST = "2024-01-31T23:55:00Z"
_POSITION_ID = "sha256:" + "1" * 64
_SOURCE_PROFILE = "sha256:" + "b" * 64
_RESOLVED_PROFILE = "sha256:" + "c" * 64
_RESOLVED_PROGRAM = "sha256:" + "a" * 64
_AUTHORED_PROGRAM = "sha256:" + "d" * 64


def _path_sha() -> str:
    return canonical_sha256(
        {
            "schema_version": "temporal_graph_cost_view_path_v3",
            "graph_path": [],
            "execution_path": [],
            "trade_path": [],
            "final_execution_state": None,
        }
    )


def _metrics(
    *,
    raw_net_r: float,
    raw_gross_r: float,
    raw_cost_percent: float,
    terminal_gross_r: float = 0.0,
    terminal_net_r: float = 0.0,
    terminal_exit_cost_percent: float = 0.0,
    unresolved_position: bool = False,
    unresolved_pending_effect: bool = False,
    trades_closed: int = 3,
) -> dict:
    terminal: dict = {
        "schemaVersion": "temporal_terminal_valuation_v1",
        "policy": "leave_open_mark_to_market_v1",
        "positionStatus": (
            "open_position_marked" if unresolved_position else "no_open_position"
        ),
        "lastCompletedBarId": "EURUSD:M5:2024-01-31T23:55:00Z",
        "lastCompletedBarStart": _LAST,
        "lastCompletedBarClose": _LAST,
        "markPrice": 1.101,
        "exitCostPercent": terminal_exit_cost_percent,
        "pendingEffectStatus": "unresolved" if unresolved_pending_effect else "none",
        "pendingEffectCancellationTreatment": (
            "canceled_for_terminal_valuation_only"
            if unresolved_pending_effect
            else "not_applicable"
        ),
        "closedTradeCountDelta": 0,
    }
    if unresolved_position:
        terminal.update(
            {
                "positionId": _POSITION_ID,
                "direction": "long",
                "grossR": terminal_gross_r,
                "netR": terminal_net_r,
            }
        )
    adjusted_net_r = raw_net_r + terminal_net_r
    adjusted_curve = [adjusted_net_r]
    return {
        "observationsProcessed": 10,
        "tradesClosed": trades_closed,
        "wins": trades_closed,
        "losses": 0,
        "flatTrades": 0,
        "unresolvedPosition": unresolved_position,
        "unresolvedPendingEffect": unresolved_pending_effect,
        "totalGrossR": raw_gross_r,
        "totalNetR": raw_net_r,
        "totalExecutionCostPercent": raw_cost_percent,
        "maxDrawdownR": max(0.0, -raw_net_r),
        "averageHoldingBars": 4.0,
        "exposureRatio": 0.2,
        "transitionEntropy": 0.3,
        "winRate": 1.0 if trades_closed else 0.0,
        "profitFactor": 2.0,
        "equityCurveR": [raw_net_r],
        "actionCounts": {},
        "closeReasonCounts": {},
        "stateOccupancy": {},
        "transitionCounts": {},
        "terminalValuation": terminal,
        "terminalAdjustedTotalGrossR": raw_gross_r + terminal_gross_r,
        "terminalAdjustedTotalNetR": adjusted_net_r,
        "terminalAdjustedTotalExecutionCostPercent": (
            raw_cost_percent + terminal_exit_cost_percent
        ),
        "terminalAdjustedEquityCurveR": adjusted_curve,
        "terminalAdjustedMaxDrawdownR": max(0.0, -adjusted_net_r),
    }


def _v3_result(
    candidate_id: str,
    *,
    conservative_raw_net_r: float,
    no_cost_raw_net_r: float,
    conservative_terminal_net_r: float = 0.0,
    no_cost_terminal_net_r: float = 0.0,
    unresolved_position: bool = False,
    unresolved_pending_effect: bool = False,
    trades_closed: int = 3,
) -> dict:
    stream = canonical_sha256({"candidate": candidate_id, "stream": "shared"})
    conservative_exit_cost = 0.2 if unresolved_position else 0.0
    conservative = _metrics(
        raw_net_r=conservative_raw_net_r,
        raw_gross_r=no_cost_raw_net_r,
        raw_cost_percent=0.1,
        terminal_gross_r=no_cost_terminal_net_r,
        terminal_net_r=conservative_terminal_net_r,
        terminal_exit_cost_percent=conservative_exit_cost,
        unresolved_position=unresolved_position,
        unresolved_pending_effect=unresolved_pending_effect,
        trades_closed=trades_closed,
    )
    no_cost = _metrics(
        raw_net_r=no_cost_raw_net_r,
        raw_gross_r=no_cost_raw_net_r,
        raw_cost_percent=0.0,
        terminal_gross_r=no_cost_terminal_net_r,
        terminal_net_r=no_cost_terminal_net_r,
        unresolved_position=unresolved_position,
        unresolved_pending_effect=unresolved_pending_effect,
        trades_closed=trades_closed,
    )
    evidence = {
        "schema_version": "temporal_graph_candidate_window_evidence_contract_v1",
        "analysis_window_start": _START,
        "analysis_window_end": _END,
        "analysis_window_end_exclusive": True,
        "requested_bar_limit": 100,
        "effective_bar_limit": 120,
        "observation_count": 10,
        "first_admitted_observation_timestamp": _START,
        "last_admitted_observation_timestamp": _LAST,
        "warmup_sufficient": True,
        "warmup_sufficiency": {"sufficient": True, "source": "aligned_scoring"},
        "excluded_provisional_count": 1,
        "excluded_outside_analysis_window_count": 2,
    }
    return {
        "schema_version": "temporal_graph_candidate_window_result_v1",
        "task_kind": "temporal_graph_candidate_window",
        "candidate_id": candidate_id,
        "analysis_window_start": _START,
        "analysis_window_end": _END,
        "source_profile_snapshot_sha256": _SOURCE_PROFILE,
        "resolved_profile_snapshot_sha256": _RESOLVED_PROFILE,
        "program_sha256": _RESOLVED_PROGRAM,
        "observation_stream_sha256": stream,
        "observation_summary": {
            "observation_count": 10,
            "first_bar_start": _START,
            "last_bar_start": _LAST,
        },
        "evidence_contract": evidence,
        "cost_view_results": {
            "research_conservative": {
                "cost_view": "research_conservative",
                "observation_stream_sha256": stream,
                "replay_result": {
                    "streamSha256": stream,
                    "profileSnapshotSha256": _RESOLVED_PROFILE,
                    "programSha256": _RESOLVED_PROGRAM,
                    "graphTraces": [],
                    "executionTraces": [],
                    "trades": [],
                    "metrics": conservative,
                },
            },
            "none": {
                "cost_view": "none",
                "observation_stream_sha256": stream,
                "replay_result": {
                    "streamSha256": stream,
                    "profileSnapshotSha256": _RESOLVED_PROFILE,
                    "programSha256": _RESOLVED_PROGRAM,
                    "graphTraces": [],
                    "executionTraces": [],
                    "trades": [],
                    "metrics": no_cost,
                },
            },
        },
        "diagnostics": {
            "observation_count": 10,
            "requested_bar_limit": 100,
            "effective_bar_limit": 120,
            "warmup_sufficient": True,
            "warmup_sufficiency": evidence["warmup_sufficiency"],
            "first_admitted_observation_timestamp": _START,
            "last_admitted_observation_timestamp": _LAST,
            "excluded_provisional_count": 1,
            "excluded_outside_analysis_window_count": 2,
            "cost_view_decision_path_sha256": _path_sha(),
            "cost_view_path_parity": "matched",
            "cost_view_count": 2,
            "shared_stream_required": True,
        },
    }


def _candidate(candidate_id: str) -> dict:
    return {
        "candidateId": candidate_id,
        "sourceMode": "fixture",
        "seedId": "fixture-seed",
        "sourceProfileSha256": _SOURCE_PROFILE,
        "profileSnapshotSha256": _SOURCE_PROFILE,
        "programSha256": _AUTHORED_PROGRAM,
        "sourceProfile": {"graph": {}, "indicators": [], "executionConfig": {}},
    }


def test_v3_gzip_unresolved_winner_is_terminal_adjusted_before_ranking(
    tmp_path: Path,
) -> None:
    result_root = tmp_path / "results-root"
    results = result_root / "results"
    results.mkdir(parents=True)
    write_gzip_json_once(
        results / "winner.json.gz",
        _v3_result(
            "winner",
            conservative_raw_net_r=4.8,
            no_cost_raw_net_r=5.0,
            conservative_terminal_net_r=-10.2,
            no_cost_terminal_net_r=-10.0,
            unresolved_position=True,
        ),
    )
    write_gzip_json_once(
        results / "stable.json.gz",
        _v3_result(
            "stable",
            conservative_raw_net_r=1.0,
            no_cost_raw_net_r=1.0,
        ),
    )

    loaded = load_stage_results(result_root)
    winner = loaded["winner"][0]
    assert winner["v3Admissible"] is True
    assert winner["rawClosedConservativeNetR"] == 4.8
    assert winner["terminalAdjustedConservativeNetR"] == pytest.approx(-5.4)
    assert winner["conservativeNetR"] == pytest.approx(-5.4)
    assert winner["noCostNetR"] == pytest.approx(-5.0)
    assert winner["terminalAdjustedCostViewDeltaR"] == pytest.approx(0.4)
    assert winner["conservativeTerminal"]["terminalExitCostPercent"] == 0.2
    assert winner["evidenceContractEndpoints"]["effectiveBarLimit"] == 120

    aggregates = [
        _aggregate_candidate(_candidate(candidate_id), rows)
        for candidate_id, rows in sorted(loaded.items())
    ]
    winner_aggregate = next(row for row in aggregates if row["candidateId"] == "winner")
    assert winner_aggregate["worstWindowConservativeNetR"] == pytest.approx(-5.4)
    assert winner_aggregate[
        "worstWindowTerminalAdjustedConservativeNetR"
    ] == pytest.approx(-5.4)
    assert winner_aggregate["worstWindowRawClosedConservativeNetR"] == 4.8
    assert winner_aggregate["terminalEvidence"][0]["positionStatus"] == "open_position_marked"
    assert winner_aggregate["v3Admissible"] is True

    ranked = select_economic_archive(
        aggregates,
        archive_size=2,
        minimum_trades_per_window=1,
    )
    assert ranked[0]["candidateId"] == "stable"


def test_qd_uses_conservative_cost_view_for_quality_while_retaining_no_cost_diagnostics() -> None:
    candidate = _candidate("cost_drag")
    record = _window_record(
        _v3_result(
            "cost_drag",
            conservative_raw_net_r=-0.25,
            no_cost_raw_net_r=1.0,
            trades_closed=8,
        )
    )
    aggregate = _aggregate_candidate(candidate, [record])
    objectives = qd_module._objective_row(candidate, aggregate)
    member = {
        "candidateId": "cost_drag",
        "candidate": candidate,
        "aggregate": aggregate,
        "descriptor": qd_module.qd_behavior_descriptor(candidate, aggregate),
        "objectives": objectives,
        "finiteDataValidity": qd_module._finite_data_validity(
            aggregate,
            minimum_total_trades=8,
            minimum_trades_per_window=4,
        ),
        "cappedTradeSupport": 8.0,
    }

    # The optimistic view remains available to diagnostics, but selection is
    # governed exclusively by terminal-adjusted research-conservative metrics.
    assert record["noCostNetR"] == pytest.approx(1.0)
    assert aggregate["totalNoCostNetR"] == pytest.approx(1.0)
    assert aggregate["windowRecords"][0]["noCostNetR"] == pytest.approx(1.0)
    assert aggregate["worstWindowConservativeNetR"] == pytest.approx(-0.25)
    assert objectives["worstWindowConservativeNetR"] == pytest.approx(-0.25)
    assert member["finiteDataValidity"]["validForQuality"] is True

    cells = qd_module.select_qd_archive([member])
    assert cells[0]["qualityEligibleCountBeforeCapacity"] == 0
    assert cells[0]["members"][0]["archiveLane"] == "negative_novelty"
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="no quality-eligible reproduction members",
    ):
        qd_module._reproduction_cells({"cells": cells})


def test_v3_completed_no_position_uses_explicit_zero_terminal_valuation() -> None:
    record = _window_record(
        _v3_result(
            "flat_terminal",
            conservative_raw_net_r=0.9,
            no_cost_raw_net_r=1.0,
        )
    )

    assert record["conservativeTerminal"]["terminalPositionStatus"] == "no_open_position"
    assert record["conservativeTerminal"]["terminalGrossR"] == 0.0
    assert record["conservativeTerminal"]["terminalNetR"] == 0.0
    assert record["terminalAdjustedConservativeNetR"] == 0.9
    assert record["terminalAdjustedNoCostNetR"] == 1.0
    assert record["noCostNetR"] - record["conservativeNetR"] == pytest.approx(0.1)


def test_v3_aggregate_keeps_authored_and_resolved_identities_distinct() -> None:
    record = _window_record(
        _v3_result("dual_identity", conservative_raw_net_r=0.9, no_cost_raw_net_r=1.0)
    )

    aggregate = _aggregate_candidate(_candidate("dual_identity"), [record])

    assert aggregate["authoredProgramSha256"] == _AUTHORED_PROGRAM
    assert aggregate["sourceProfileSnapshotSha256"] == _SOURCE_PROFILE
    assert aggregate["resolvedProfileSnapshotSha256"] == _RESOLVED_PROFILE
    assert aggregate["resolvedProgramSha256"] == _RESOLVED_PROGRAM
    assert aggregate["programSha256"] == _RESOLVED_PROGRAM
    assert aggregate["authoredProgramSha256"] != aggregate["resolvedProgramSha256"]


def test_v3_aggregate_rejects_source_or_resolved_execution_drift() -> None:
    first = _window_record(
        _v3_result("identity_drift", conservative_raw_net_r=0.0, no_cost_raw_net_r=0.0)
    )
    second = dict(first)
    second["windowId"] = "2024-02-01T00:00:00Z/2024-03-01T00:00:00Z"
    candidate = _candidate("identity_drift")

    source_mismatch = dict(second)
    source_mismatch["sourceProfileSnapshotSha256"] = "sha256:" + "e" * 64
    with pytest.raises(TemporalDiscoveryContractError, match="source profile snapshot identity"):
        _aggregate_candidate(candidate, [first, source_mismatch])

    profile_drift = dict(second)
    profile_drift["resolvedProfileSnapshotSha256"] = "sha256:" + "e" * 64
    with pytest.raises(TemporalDiscoveryContractError, match="resolved profile snapshot identity changed"):
        _aggregate_candidate(candidate, [first, profile_drift])

    program_drift = dict(second)
    program_drift["resolvedProgramSha256"] = "sha256:" + "e" * 64
    program_drift["programSha256"] = program_drift["resolvedProgramSha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="resolved program identity changed"):
        _aggregate_candidate(candidate, [first, program_drift])


def test_v3_validator_requires_source_identity_and_replay_execution_identity() -> None:
    missing_source = _v3_result(
        "missing_source", conservative_raw_net_r=0.0, no_cost_raw_net_r=0.0
    )
    missing_source.pop("source_profile_snapshot_sha256")
    with pytest.raises(TemporalDiscoveryContractError, match="invalid Stage 5E7-v3"):
        _window_record(missing_source)

    replay_drift = _v3_result(
        "replay_drift", conservative_raw_net_r=0.0, no_cost_raw_net_r=0.0
    )
    replay_drift["cost_view_results"]["none"]["replay_result"][
        "programSha256"
    ] = "sha256:" + "e" * 64
    with pytest.raises(TemporalDiscoveryContractError, match="invalid Stage 5E7-v3"):
        _window_record(replay_drift)


def test_v3_noop_authored_equals_resolved_execution_is_valid() -> None:
    result = _v3_result(
        "noop", conservative_raw_net_r=0.0, no_cost_raw_net_r=0.0
    )
    result["resolved_profile_snapshot_sha256"] = _SOURCE_PROFILE
    for cost_view in result["cost_view_results"].values():
        cost_view["replay_result"]["profileSnapshotSha256"] = _SOURCE_PROFILE
    candidate = _candidate("noop")
    candidate["programSha256"] = _RESOLVED_PROGRAM

    aggregate = _aggregate_candidate(candidate, [_window_record(result)])

    assert aggregate["authoredProgramSha256"] == aggregate["resolvedProgramSha256"]
    assert aggregate["sourceProfileSnapshotSha256"] == aggregate["resolvedProfileSnapshotSha256"]


@pytest.mark.parametrize(
    ("curve", "raw_net_r"),
    (
        ([], 0.0),
        ([], 1.0),
        ([0.0], 1.0),
        ("not-an-array", 1.0),
        ([float("nan")], 1.0),
    ),
    ids=(
        "empty-curve-zero-terminal-return",
        "empty-curve-nonzero-terminal-return",
        "final-value-does-not-match-terminal-return",
        "curve-is-not-an-array",
        "curve-has-nonfinite-value",
    ),
)
def test_v3_loader_rejects_unbound_or_malformed_terminal_adjusted_equity_curve(
    curve: object,
    raw_net_r: float,
) -> None:
    payload = _v3_result(
        "invalid_terminal_curve",
        conservative_raw_net_r=raw_net_r,
        no_cost_raw_net_r=raw_net_r,
    )
    for cost_view in payload["cost_view_results"].values():
        cost_view["replay_result"]["metrics"]["terminalAdjustedEquityCurveR"] = curve

    with pytest.raises(TemporalDiscoveryContractError, match="invalid Stage 5E7-v3"):
        _window_record(payload)


@pytest.mark.parametrize("defect", ("warmup", "endpoint"))
def test_v3_loader_rejects_incomplete_warmup_or_half_open_endpoints(
    tmp_path: Path,
    defect: str,
) -> None:
    payload = _v3_result("rejected", conservative_raw_net_r=0.0, no_cost_raw_net_r=0.0)
    if defect == "warmup":
        payload["evidence_contract"]["warmup_sufficient"] = False
    else:
        payload["evidence_contract"]["last_admitted_observation_timestamp"] = _END
    result_dir = tmp_path / defect / "results"
    result_dir.mkdir(parents=True)
    write_gzip_json_once(result_dir / "rejected.json.gz", payload)

    with pytest.raises(TemporalDiscoveryContractError, match="invalid Stage 5E7-v3"):
        load_stage_results(result_dir.parent)


def test_legacy_json_remains_loadable_but_is_not_v3_admissible(tmp_path: Path) -> None:
    legacy = _v3_result("legacy", conservative_raw_net_r=1.0, no_cost_raw_net_r=1.1)
    legacy.pop("evidence_contract")
    for cost_view in legacy["cost_view_results"].values():
        metrics = cost_view["replay_result"]["metrics"]
        for key in list(metrics):
            if key == "terminalValuation" or key.startswith("terminalAdjusted"):
                metrics.pop(key)
    legacy.pop("observation_summary")
    legacy["diagnostics"] = {}
    result_dir = tmp_path / "legacy" / "results"
    result_dir.mkdir(parents=True)
    (result_dir / "legacy.json").write_text(
        json.dumps(legacy, sort_keys=True), encoding="utf-8"
    )

    loaded = load_stage_results(result_dir.parent)
    record = loaded["legacy"][0]
    aggregate = _aggregate_candidate(_candidate("legacy"), [record])
    assert record["v3Admissible"] is False
    assert record["terminalAdjustedConservativeNetR"] is None
    assert aggregate["v3Admissible"] is False
    assert aggregate["economicsBasis"] == "legacy_closed_trade_v1_not_v3_admissible"
