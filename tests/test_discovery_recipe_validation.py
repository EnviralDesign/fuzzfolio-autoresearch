from __future__ import annotations

from autoresearch.discovery_recipe_validation import (
    _retention_bucket,
    build_validation_queue_rows,
)


def _recipe(recipe_id: str, *, confidence: str = "high_candidate") -> dict[str, object]:
    return {
        "recipe_id": recipe_id,
        "confidence": confidence,
        "name": "first cluster + second cluster",
        "compatibility_score": 72.5,
        "best_score": 78.0,
        "positive_pair_count": 12,
        "strong_pair_count": 4,
        "top_timeframes": "M5,M15",
        "slots": {
            "context_or_setup_cluster": {
                "cluster_id": "first_cluster_01",
                "label": "first:test",
                "recommended_indicators": ["ANCHOR_A", "ANCHOR_B"],
            },
            "trigger_or_response_cluster": {
                "cluster_id": "second_cluster_01",
                "label": "second:test",
                "recommended_indicators": ["TRIGGER_X", "TRIGGER_Y"],
            },
        },
        "evidence_examples": [
            {
                "probe_id": "dp-evidence",
                "first_indicator_id": "ANCHOR_A",
                "second_indicator_id": "TRIGGER_X",
                "probe_timeframe": "M5",
                "discovery_lane": "plausible_novel",
                "composite_score": 76.0,
            }
        ],
    }


def test_build_validation_queue_rows_prefers_evidence_and_caps_per_recipe() -> None:
    rows = build_validation_queue_rows(
        [_recipe("discovered_recipe_001")],
        max_recipes=1,
        max_pairs_per_recipe=3,
        instruments=["EURUSD"],
    )

    assert len(rows) == 3
    assert rows[0]["first_indicator_id"] == "ANCHOR_A"
    assert rows[0]["second_indicator_id"] == "TRIGGER_X"
    assert rows[0]["probe_timeframe"] == "M5"
    assert rows[0]["discovery_evidence_probe_id"] == "dp-evidence"
    assert rows[0]["queue_rank"] == 1
    assert str(rows[0]["probe_id"]).startswith("drv-0001-")


def test_build_validation_queue_rows_filters_confidence_buckets() -> None:
    rows = build_validation_queue_rows(
        [
            _recipe("discovered_recipe_001", confidence="high_candidate"),
            _recipe("discovered_recipe_002", confidence="sparse_watch"),
        ],
        included_confidence=["high_candidate"],
        max_recipes=4,
        max_pairs_per_recipe=2,
    )

    assert rows
    assert {row["recipe_id"] for row in rows} == {"discovered_recipe_001"}


def test_retention_bucket_classifies_validation_survival() -> None:
    assert _retention_bucket(0.95, 72) == "retained_strong"
    assert _retention_bucket(0.80, 63) == "retained"
    assert _retention_bucket(0.60, 52) == "partial_retention"
    assert _retention_bucket(0.40, 40) == "failed_retention"
    assert _retention_bucket(None, 0) == "unscored"
    assert _retention_bucket(
        None,
        72,
        has_discovery_evidence=False,
    ) == "new_strong_cluster_expansion"
    assert _retention_bucket(
        None,
        64,
        has_discovery_evidence=False,
    ) == "new_positive_cluster_expansion"
