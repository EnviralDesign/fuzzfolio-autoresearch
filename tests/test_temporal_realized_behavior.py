from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_results import _window_record
from autoresearch.temporal_direction_selection import classify_direction_selection
from autoresearch.temporal_realized_behavior import (
    aggregate_realized_behavior,
    behavior_family_clusters,
    build_window_realized_behavior,
)


def _trade(*, direction: str, gross: float, net: float, clock: int) -> dict:
    return {
        "direction": direction,
        "tradeId": f"{direction}-{clock}",
        "positionId": f"position-{direction}-{clock}",
        "entryClockIndex": clock,
        "exitClockIndex": clock + 2,
        "entryTime": f"2024-01-0{clock}T00:00:00Z",
        "exitTime": f"2024-01-0{clock}T00:10:00Z",
        "holdingBars": 2,
        "holdingHours": 1.0,
        "closeReason": "target" if net > 0 else "stop",
        "grossR": gross,
        "netR": net,
    }


def _window(*, window_id: str, trades: list[dict]) -> dict:
    return build_window_realized_behavior(
        window_id=window_id,
        replay={
            "trades": trades,
            "executionTraces": [
                {
                    "tradeId": row["tradeId"],
                    "actionKind": "enter_next_open",
                    "status": "filled",
                }
                for row in trades
            ],
            "graphTraces": [
                {"direction": row["direction"], "transitionId": "entry"}
                for row in trades
            ],
        },
        metrics={
            "tradesClosed": len(trades),
            "observationsProcessed": 10,
            "totalGrossR": sum(row["grossR"] for row in trades),
            "totalNetR": sum(row["netR"] for row in trades),
            "terminalValuation": {"positionStatus": "no_open_position"},
        },
    )


def test_per_side_behavior_keeps_losing_and_inactive_short_visible() -> None:
    behavior = aggregate_realized_behavior(
        [
            {"realizedBehavior": _window(window_id="a", trades=[_trade(direction="long", gross=2.0, net=1.8, clock=1)])},
            {"realizedBehavior": _window(window_id="b", trades=[_trade(direction="short", gross=-1.0, net=-1.2, clock=2)])},
            {"realizedBehavior": _window(window_id="c", trades=[_trade(direction="long", gross=0.5, net=0.4, clock=3)])},
        ]
    )
    assert behavior["sides"]["long"]["netR"] == pytest.approx(2.2)
    assert behavior["sides"]["short"]["netR"] == pytest.approx(-1.2)
    assert behavior["sides"]["short"]["losses"] == 1
    assert behavior["sides"]["short"]["activeWindowCount"] == 1
    assert behavior["sides"]["short"]["activeWindowFraction"] == pytest.approx(1 / 3)
    # The aggregate is the source of direction lanes, not only an internal
    # report.  Preserve the derived per-side active flag that classifier
    # contracts require.
    assert behavior["sides"]["long"]["active"] is True
    assert behavior["sides"]["short"]["active"] is True
    assert classify_direction_selection(behavior)["lane"] == "harmful_opposite_side"


def test_side_swapped_execution_has_a_distinct_behavior_identity() -> None:
    long = aggregate_realized_behavior(
        [{"realizedBehavior": _window(window_id="same", trades=[_trade(direction="long", gross=1.0, net=0.8, clock=1)])}]
    )
    short = aggregate_realized_behavior(
        [{"realizedBehavior": _window(window_id="same", trades=[_trade(direction="short", gross=1.0, net=0.8, clock=1)])}]
    )
    assert long["identitySha256"] != short["identitySha256"]
    assert long["sides"]["long"]["closedTrades"] == 1
    assert short["sides"]["short"]["closedTrades"] == 1


def test_identical_execution_genotypes_form_one_behavior_family_without_deletion() -> None:
    behavior = aggregate_realized_behavior(
        [{"realizedBehavior": _window(window_id="same", trades=[_trade(direction="long", gross=1.0, net=0.8, clock=1)])}]
    )
    candidates = [
        {
            "candidateId": f"genotype_{index}",
            "resolvedProgramSha256": "sha256:" + f"{index:064x}",
            "realizedBehavior": copy.deepcopy(behavior),
        }
        for index in range(8)
    ]
    families = behavior_family_clusters(candidates)
    assert len(families) == 1
    assert families[0]["memberCount"] == 8
    assert len(families[0]["memberCandidateIds"]) == 8
    assert len(families[0]["exactProgramSha256s"]) == 8


def test_cost_totals_reconcile_and_window_record_binds_projection() -> None:
    trades = [
        _trade(direction="long", gross=2.0, net=1.7, clock=1),
        _trade(direction="short", gross=-1.0, net=-1.1, clock=3),
    ]
    window = _window(window_id="a", trades=trades)
    aggregate = aggregate_realized_behavior([{"realizedBehavior": window}])
    assert aggregate["sides"]["long"]["costR"] == pytest.approx(0.3)
    assert aggregate["sides"]["short"]["costR"] == pytest.approx(0.1)
    assert sum(row["grossR"] - row["netR"] for row in aggregate["sides"].values()) == pytest.approx(
        sum(row["costR"] for row in aggregate["sides"].values())
    )

    metrics = {
        "observationsProcessed": 10, "tradesClosed": 2, "wins": 1, "losses": 1,
        "flatTrades": 0, "totalGrossR": 1.0, "totalNetR": 0.6,
        "maxDrawdownR": 1.1, "averageHoldingBars": 2.0, "exposureRatio": 0.4,
        "transitionEntropy": 0.2, "winRate": 0.5, "profitFactor": 1.0,
        "equityCurveR": [0.6], "actionCounts": {}, "closeReasonCounts": {},
        "stateOccupancy": {}, "transitionCounts": {},
    }
    result = {
        "candidate_id": "candidate", "analysis_window_start": "a", "analysis_window_end": "b",
        "observation_stream_sha256": "sha256:" + "a" * 64,
        "program_sha256": "sha256:" + "b" * 64,
        "cost_view_results": {
            "research_conservative": {"replay_result": {"streamSha256": "sha256:" + "a" * 64, "metrics": metrics, "trades": trades, "executionTraces": [], "graphTraces": []}},
            "none": {"replay_result": {"streamSha256": "sha256:" + "a" * 64, "metrics": {**metrics, "totalNetR": 1.0}}},
        },
    }
    assert _window_record(result)["realizedBehavior"]["sides"]["short"]["losses"] == 1


def test_malformed_trade_direction_or_economics_fail_closed() -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="direction"):
        _window(window_id="a", trades=[_trade(direction="sideways", gross=1.0, net=1.0, clock=1)])
    bad = _trade(direction="long", gross=1.0, net=1.0, clock=1)
    bad["netR"] = float("nan")
    with pytest.raises(TemporalDiscoveryContractError, match="netR"):
        _window(window_id="a", trades=[bad])
