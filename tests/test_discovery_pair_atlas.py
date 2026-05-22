from __future__ import annotations

from autoresearch.discovery_pair_atlas import (
    build_discovery_pair_rows,
    score_discovery_pair,
    select_discovery_pair_queue,
)


def _indicator(
    indicator_id: str,
    *,
    signal_role: str,
    strategy_role: str,
    base: str | None = None,
    static_score: float = 70.0,
) -> dict[str, object]:
    return {
        "id": indicator_id,
        "signal_role": signal_role,
        "strategy_role": strategy_role,
        "base_indicator_id": base or indicator_id,
        "generation_eligible": True,
        "static_prior_score": static_score,
    }


def test_score_discovery_pair_marks_exact_known_result() -> None:
    first = _indicator(
        "RSI_MEAN_REVERSION",
        signal_role="context",
        strategy_role="mean-reversion",
        base="RSI",
    )
    second = _indicator(
        "TOBY_CRABEL_NARROW_RANGE",
        signal_role="trigger",
        strategy_role="breakout",
        base="TOBY",
    )

    scored = score_discovery_pair(
        first_row=first,
        second_row=second,
        signal_rollups={
            "TOBY_CRABEL_NARROW_RANGE": {
                "status": "ok",
                "density_bucket": "usable",
                "event_count": 200,
                "balance_bucket_counts": {"balanced": 1},
            }
        },
        forward_priors={
            "TOBY_CRABEL_NARROW_RANGE": {
                "forward_response_prior_score": 72,
                "forward_response_prior_bucket": "context_dependent_forward_response",
                "best_cell_context": "EURUSD M5 long 12",
            }
        },
        slot_priors={},
        known_pairs={
            ("RSI_MEAN_REVERSION", "TOBY_CRABEL_NARROW_RANGE", "M5"): {
                "composite_score": 73.3,
                "probe_id": "l3-known",
            }
        },
        indicator_pair_rollups={},
        probe_timeframe="M5",
        instruments=["EURUSD"],
    )

    assert scored["known_pair_status"] == "exact_known_positive"
    assert scored["discovery_lane"] == "known_result"
    assert scored["known_pair_probe_id"] == "l3-known"


def test_select_discovery_pair_queue_excludes_known_results_by_default() -> None:
    rows = [
        {
            "first_indicator_id": "KNOWN_A",
            "second_indicator_id": "KNOWN_B",
            "probe_timeframe": "M5",
            "discovery_lane": "known_result",
            "local_discovery_score": 99,
        },
        {
            "first_indicator_id": "PROVEN_A",
            "second_indicator_id": "PROVEN_B",
            "probe_timeframe": "M5",
            "discovery_lane": "proven_neighbor",
            "local_discovery_score": 70,
        },
        {
            "first_indicator_id": "PLAUSIBLE_A",
            "second_indicator_id": "PLAUSIBLE_B",
            "probe_timeframe": "M15",
            "discovery_lane": "plausible_novel",
            "local_discovery_score": 68,
        },
        {
            "first_indicator_id": "UNDER_A",
            "second_indicator_id": "UNDER_B",
            "probe_timeframe": "M5",
            "discovery_lane": "under_tested_role_correct",
            "local_discovery_score": 62,
            "first_tested_pair_count": 0,
            "second_tested_pair_count": 0,
        },
        {
            "first_indicator_id": "WILD_A",
            "second_indicator_id": "WILD_B",
            "probe_timeframe": "M15",
            "discovery_lane": "wild_diversity",
            "local_discovery_score": 40,
        },
    ]

    queue = select_discovery_pair_queue(rows, max_pairs=4, random_seed=1)

    assert len(queue) == 4
    assert all(row["discovery_lane"] != "known_result" for row in queue)
    assert [row["queue_rank"] for row in queue] == [1, 2, 3, 4]
    assert all(str(row["probe_id"]).startswith("dp-") for row in queue)


def test_select_discovery_pair_queue_can_include_known_retests() -> None:
    rows = [
        {
            "first_indicator_id": "KNOWN_A",
            "second_indicator_id": "KNOWN_B",
            "probe_timeframe": "M5",
            "discovery_lane": "known_result",
            "local_discovery_score": 99,
        }
    ]

    queue = select_discovery_pair_queue(
        rows,
        max_pairs=1,
        include_known_retests=True,
        random_seed=1,
    )

    assert len(queue) == 1
    assert queue[0]["discovery_lane"] == "known_result"


def test_build_discovery_pair_rows_builds_ordered_pairs_without_self_pairs() -> None:
    rows_by_id = {
        "CONTEXT_A": _indicator("CONTEXT_A", signal_role="context", strategy_role="trend"),
        "TRIGGER_B": _indicator("TRIGGER_B", signal_role="trigger", strategy_role="breakout"),
        "TRIGGER_C": _indicator("TRIGGER_C", signal_role="trigger", strategy_role="confirm"),
    }

    rows = build_discovery_pair_rows(
        rows_by_id=rows_by_id,
        signal_rollups={},
        forward_priors={},
        slot_priors={},
        known_pairs={},
        indicator_pair_rollups={},
        first_ids=None,
        second_ids=None,
        timeframes=["M5", "M15"],
        instruments=["EURUSD"],
    )

    assert len(rows) == 12
    assert all(row["first_indicator_id"] != row["second_indicator_id"] for row in rows)
    assert {row["probe_timeframe"] for row in rows} == {"M5", "M15"}
