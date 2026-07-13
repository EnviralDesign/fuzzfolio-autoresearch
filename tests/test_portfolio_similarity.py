from autoresearch.portfolio_optimizer import (
    OptimizerCandidate,
    analyze_behavioral_similarity,
)


def _candidate(attempt_id: str, daily_r: list[float]) -> OptimizerCandidate:
    return OptimizerCandidate(
        attempt_id=attempt_id,
        row={"attempt_id": attempt_id, "candidate_name": attempt_id},
        instruments=["EURUSD"],
        asset_classes={"fx"},
        primary_asset_class="fx",
        family=f"family:{attempt_id}",
        family_source="unit_test",
        lineage_id=None,
        behavior_fingerprint=None,
        structural_family_signature=None,
        score=70.0,
        created_at=None,
        avg_hold_hours=1.0,
        p90_hold_hours=2.0,
        max_hold_hours=3.0,
        path_quality=1.0,
        stop_loss_percent=1.0,
        trade_count=4,
        trades_per_month=4.0,
        dates=[f"2026-01-0{index}" for index in range(1, len(daily_r) + 1)],
        daily_r=daily_r,
        open_counts=[1] * len(daily_r),
        closed_counts=[1] * len(daily_r),
    )


def test_rust_behavioral_similarity_is_deterministic_and_clusters_substitutes() -> None:
    candidates = [
        _candidate("alpha", [1, -1, -2, 2, -3, 3, -4, 4, -5, 5]),
        _candidate("beta", [2, -2, -4, 4, -6, 6, -8, 8, -10, 10]),
        _candidate("gamma", [-1, 1, 2, -2, 3, -3, 4, -4, 5, -5]),
    ]
    kwargs = {
        "reference_attempt_ids": ["alpha", "beta"],
        "behavioral_weights": {
            "active_overlap": 0.2,
            "return_correlation": 0.2,
            "downside_correlation": 0.3,
            "worst_decile_correlation": 0.3,
        },
        "worst_quantile": 0.4,
        "cluster_threshold": 0.9,
    }

    first = analyze_behavioral_similarity(candidates, **kwargs)
    second = analyze_behavioral_similarity(list(reversed(candidates)), **kwargs)

    assert first == second
    assert first["attempt_ids"] == ["alpha", "beta", "gamma"]
    assert first["return_correlation_matrix"][0][1] == 1.0
    assert first["return_correlation_matrix"][0][2] == -1.0
    assert first["clusters"] == [
        {"id": "behavior:alpha", "members": ["alpha", "beta"]},
        {"id": "behavior:gamma", "members": ["gamma"]},
    ]
