from __future__ import annotations

from autoresearch.discovery_recipe_validation import (
    _retention_bucket,
    _validation_selection_diagnostics,
    build_retained_scrutiny_queue_rows,
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


def test_build_validation_queue_rows_shrinks_sparse_discovery_evidence() -> None:
    recipe = _recipe("discovered_recipe_001")
    recipe["evidence_examples"] = [
        {
            "probe_id": "dp-sparse-high",
            "first_indicator_id": "ANCHOR_A",
            "second_indicator_id": "TRIGGER_X",
            "probe_timeframe": "M5",
            "composite_score": 90.0,
            "best_trades": 5,
        },
        {
            "probe_id": "dp-durable-good",
            "first_indicator_id": "ANCHOR_A",
            "second_indicator_id": "TRIGGER_X",
            "probe_timeframe": "M15",
            "composite_score": 70.0,
            "best_trades": 120,
        },
    ]

    rows = build_validation_queue_rows(
        [recipe],
        max_recipes=1,
        max_pairs_per_recipe=2,
        first_member_limit=1,
        second_member_limit=1,
        instruments=["EURUSD"],
    )

    assert [row["discovery_evidence_probe_id"] for row in rows] == [
        "dp-durable-good",
        "dp-sparse-high",
    ]
    assert rows[0]["sample_confidence"] == "high"
    assert rows[0]["sample_floor_passed"] is True
    assert rows[1]["sample_confidence"] == "sparse"
    assert rows[1]["sample_floor_passed"] is False
    assert rows[1]["sample_adjusted_discovery_evidence_score"] < rows[1]["discovery_evidence_score"]


def test_build_validation_queue_rows_applies_soft_diversity_pressure() -> None:
    recipe = _recipe("discovered_recipe_001")
    recipe["evidence_examples"] = [
        {
            "probe_id": "dp-duplicate-family",
            "first_indicator_id": "ANCHOR_A",
            "second_indicator_id": "TRIGGER_X",
            "probe_timeframe": "M5",
            "composite_score": 76.0,
            "best_trades": 120,
        },
        {
            "probe_id": "dp-distinct-family",
            "first_indicator_id": "ANCHOR_B",
            "second_indicator_id": "TRIGGER_X",
            "probe_timeframe": "M5",
            "composite_score": 74.0,
            "best_trades": 120,
        },
    ]

    rows = build_validation_queue_rows(
        [recipe],
        max_recipes=1,
        max_pairs_per_recipe=4,
        first_member_limit=2,
        second_member_limit=1,
        instruments=["EURUSD"],
        retained_inventory_rows=[
            {
                "recipe": "discovered_recipe_001",
                "anchor_id": "ANCHOR_A",
                "trigger_id": "TRIGGER_X",
                "probe_timeframe": "M5",
                "pair_sampling_lane": "high_prior",
                "pair_sampling_score": "82",
                "instruments": "EURUSD",
            }
        ],
    )

    assert rows[0]["discovery_evidence_probe_id"] == "dp-distinct-family"
    duplicate = next(row for row in rows if row["discovery_evidence_probe_id"] == "dp-duplicate-family")
    assert duplicate["nearest_retained_similarity"] > 0.5
    assert duplicate["diversity_penalty"] > 0
    assert "canonical_family" in duplicate["diversity_reason"]
    assert duplicate["validation_priority_score"] < duplicate["pre_diversity_priority_score"]


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


def test_validation_selection_diagnostics_explain_recipe_reduction() -> None:
    recipes = [
        _recipe("discovered_recipe_001", confidence="high_candidate"),
        _recipe("discovered_recipe_002", confidence="promising_candidate"),
        _recipe("discovered_recipe_003", confidence="needs_more_evidence"),
    ]
    queue_rows = build_validation_queue_rows(
        recipes,
        included_confidence=["high_candidate", "promising_candidate"],
        max_recipes=1,
        max_pairs_per_recipe=2,
        first_member_limit=2,
        second_member_limit=2,
        instruments=["EURUSD"],
    )

    diagnostics = _validation_selection_diagnostics(
        recipes,
        included_confidence=["high_candidate", "promising_candidate"],
        max_recipes=1,
        max_pairs_per_recipe=2,
        first_member_limit=2,
        second_member_limit=2,
        timeframes=None,
        queue_rows_before_catalog_filter=queue_rows,
        queue_rows_after_catalog_filter=queue_rows[:1],
    )

    assert diagnostics["eligible_recipe_count"] == 2
    assert diagnostics["filtered_by_confidence_count"] == 1
    assert diagnostics["selected_recipe_count"] == 1
    assert diagnostics["recipes_truncated_by_max"] == 1
    assert diagnostics["candidate_pair_rows_before_pair_cap"] == 8
    assert diagnostics["candidate_pair_rows_after_pair_cap"] == 2
    assert diagnostics["queue_rows_before_catalog_filter"] == 2
    assert diagnostics["queue_rows_after_catalog_filter"] == 1
    assert diagnostics["filtered_by_catalog_count"] == 1


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


def test_build_retained_scrutiny_queue_rows_promotes_only_retained_rows() -> None:
    rows = build_retained_scrutiny_queue_rows(
        [
            {
                "status": "ok",
                "probe_id": "drv-kept",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_cluster_id": "cluster_a",
                "first_cluster_label": "Cluster A label",
                "second_cluster_id": "cluster_b",
                "second_cluster_label": "Cluster B label",
                "first_indicator_id": "ANCHOR_A",
                "second_indicator_id": "TRIGGER_X",
                "probe_timeframe": "M5",
                "primary_score": "72",
                "validation_priority_score": "80",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.97",
                "retention_bucket": "retained_strong",
                "best_trades": "25",
            },
            {
                "status": "ok",
                "probe_id": "drv-failed",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_B",
                "second_indicator_id": "TRIGGER_Y",
                "probe_timeframe": "M5",
                "primary_score": "0",
                "retention_bucket": "failed_retention",
            },
        ],
        instruments=["EURUSD"],
    )

    assert len(rows) == 1
    assert rows[0]["source_validation_probe_id"] == "drv-kept"
    assert rows[0]["source_retention_bucket"] == "retained_strong"
    assert rows[0]["first_cluster_label"] == "Cluster A label"
    assert rows[0]["second_cluster_label"] == "Cluster B label"
    assert rows[0]["scrutiny_selection_reason"] == "strict_retention"
    assert rows[0]["sample_confidence"] == "low"
    assert str(rows[0]["probe_id"]).startswith("drs-0001-")
    assert rows[0]["known_pair_status"] == "retained_discovered_recipe_36m_candidate"


def test_build_retained_scrutiny_queue_rows_uses_bounded_high_sample_fallback() -> None:
    rows = build_retained_scrutiny_queue_rows(
        [
            {
                "status": "ok",
                "probe_id": "drv-strict",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_A",
                "second_indicator_id": "TRIGGER_X",
                "probe_timeframe": "M5",
                "primary_score": "60",
                "validation_priority_score": "70",
                "retention_bucket": "retained",
                "best_trades": "30",
            },
            {
                "status": "ok",
                "probe_id": "drv-fallback-good",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_B",
                "second_indicator_id": "TRIGGER_Y",
                "probe_timeframe": "M5",
                "primary_score": "58",
                "validation_priority_score": "90",
                "retention_bucket": "partial_retention",
                "best_trades": "145",
            },
            {
                "status": "ok",
                "probe_id": "drv-fallback-sparse",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_C",
                "second_indicator_id": "TRIGGER_Z",
                "probe_timeframe": "M5",
                "primary_score": "68",
                "validation_priority_score": "99",
                "retention_bucket": "new_positive_cluster_expansion",
                "best_trades": "10",
            },
        ],
        fallback_max_rows=1,
        fallback_min_trades=20,
        instruments=["EURUSD"],
    )

    assert [row["source_validation_probe_id"] for row in rows] == [
        "drv-strict",
        "drv-fallback-good",
    ]
    assert rows[0]["scrutiny_selection_reason"] == "strict_retention"
    assert rows[1]["scrutiny_selection_reason"] == "fallback_partial_retention"
    assert rows[1]["sample_confidence"] == "high"


def test_build_retained_scrutiny_queue_rows_records_sample_shrinkage() -> None:
    rows = build_retained_scrutiny_queue_rows(
        [
            {
                "status": "ok",
                "probe_id": "drv-sparse",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_A",
                "second_indicator_id": "TRIGGER_X",
                "probe_timeframe": "M5",
                "lookback_months": "12",
                "primary_score": "80",
                "validation_priority_score": "95",
                "retention_bucket": "retained_strong",
                "best_trades": "5",
            },
            {
                "status": "ok",
                "probe_id": "drv-durable",
                "recipe_id": "discovered_recipe_001",
                "first_indicator_id": "ANCHOR_B",
                "second_indicator_id": "TRIGGER_Y",
                "probe_timeframe": "M5",
                "lookback_months": "12",
                "primary_score": "68",
                "validation_priority_score": "85",
                "retention_bucket": "retained",
                "best_trades": "150",
                "unique_months": "10",
            },
        ],
        instruments=["EURUSD"],
    )

    assert [row["source_validation_probe_id"] for row in rows] == [
        "drv-durable",
        "drv-sparse",
    ]
    assert rows[0]["sample_confidence"] == "high"
    assert rows[0]["sample_coverage_score"] > 0.8
    assert rows[1]["sample_confidence"] == "sparse"
    assert rows[1]["sample_adjusted_validation_score"] < rows[1]["source_validation_score"]
    assert rows[1]["sample_shrinkage_penalty"] > 0
