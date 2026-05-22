from __future__ import annotations

from autoresearch.recipe_priors import (
    build_recipe_prior_artifacts,
    score_slot_candidate,
    timing_policy_for,
)


def _indicator(
    indicator_id: str,
    *,
    signal_role: str,
    strategy_role: str,
    static_score: float = 70,
) -> dict[str, object]:
    return {
        "id": indicator_id,
        "signal_role": signal_role,
        "strategy_role": strategy_role,
        "namespace": "Test",
        "generation_eligible": "True",
        "static_prior_score": str(static_score),
    }


def test_timing_policy_allows_only_material_nonfragile_variants() -> None:
    policy, adjustment, lookback = timing_policy_for(
        {
            "material_improved_count": 1,
            "lost_positive_count": 0,
            "degraded_count": 0,
            "best_delta": 12.0,
            "best_variant_lookback_bars": "3",
        }
    )

    assert policy == "allow_variant"
    assert adjustment > 0
    assert lookback == "3"

    policy, adjustment, lookback = timing_policy_for(
        {
            "material_improved_count": 1,
            "lost_positive_count": 1,
            "degraded_count": 0,
            "best_delta": 12.0,
            "best_variant_lookback_bars": "3",
        }
    )

    assert policy == "catalog_default_only"
    assert adjustment < 0
    assert lookback is None


def test_score_slot_candidate_blends_static_signal_forward_pair_and_timing() -> None:
    row = _indicator("TRIGGER_A", signal_role="trigger", strategy_role="mean-reversion")

    scored = score_slot_candidate(
        row,
        recipe_name="mean_reversion_reclaim",
        slot_name="trigger",
        static_slot_scores={("mean_reversion_reclaim", "trigger", "TRIGGER_A"): 80},
        signal_rollups={
            "TRIGGER_A": {
                "density_bucket": "usable",
                "balance_bucket_counts": {"balanced": 4},
            }
        },
        forward_priors={
            "TRIGGER_A": {
                "forward_response_prior_score": "75",
                "forward_response_prior_bucket": "context_dependent_forward_response",
            }
        },
        trigger_pair_stats={
            ("mean_reversion_reclaim", "TRIGGER_A"): {
                "count": 2,
                "positive_count": 1,
                "best_score": 68,
                "avg_score": 45,
                "best_probe_id": "l3-test",
                "best_timeframe": "M5",
            }
        },
        anchor_pair_stats={},
        timing_evidence={
            ("mean_reversion_reclaim", "TRIGGER_A"): {
                "material_improved_count": 1,
                "lost_positive_count": 0,
                "degraded_count": 0,
                "best_delta": 10,
                "best_variant_lookback_bars": "3",
            }
        },
    )

    assert scored["recipe_slot_score"] > 70
    assert scored["sampling_weight"] > 0
    assert scored["timing_policy"] == "allow_variant"
    assert scored["recommended_trigger_lookback_bars"] == "3"


def test_build_recipe_prior_artifacts_emits_play_hand_seed_plan() -> None:
    indicators = [
        _indicator("ANCHOR_A", signal_role="setup", strategy_role="mean-reversion"),
        _indicator("TRIGGER_A", signal_role="trigger", strategy_role="mean-reversion"),
        _indicator("FILTER_A", signal_role="filter", strategy_role="filter"),
    ]

    (
        payload,
        slot_rows,
        pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={"TRIGGER_A": {"density_bucket": "usable"}},
        forward_priors={"TRIGGER_A": {"forward_response_prior_score": "70"}},
        pair_results=[
            {
                "anchor_type": "mean_reversion",
                "anchor_id": "ANCHOR_A",
                "trigger_id": "TRIGGER_A",
                "probe_timeframe": "M5",
                "probe_id": "l3-test",
                "pair_prior_score": "80",
                "composite_score": "65",
            }
        ],
        timing_results=[
            {
                "anchor_type": "mean_reversion",
                "trigger_id": "TRIGGER_A",
                "score_delta": "6",
                "timing_bucket": "improved",
                "variant_lookback_bars": "3",
                "timing_probe_id": "l3b-test",
            }
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    trigger_menu = seed_plan["recipes"]["mean_reversion_reclaim"]["slot_menus"]["trigger"]
    assert payload["schema_version"] == "empirical_recipe_priors_v1"
    assert summary["result_counts"]["pair_prior_rows"] == 1
    assert pair_rows[0]["pair_sampling_lane"] == "positive_pair"
    assert any(row["indicator_id"] == "TRIGGER_A" for row in slot_rows)
    assert trigger_menu[0]["indicator_id"] == "TRIGGER_A"
    assert trigger_menu[0]["timing_policy"] == "allow_variant"


def test_build_recipe_prior_artifacts_adds_validated_discovered_recipes() -> None:
    indicators = [
        _indicator("FIRST_A", signal_role="setup", strategy_role="trend"),
        _indicator("SECOND_A", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        slot_rows,
        pair_rows,
        negative_pairs,
        _negative_clusters,
        retention_failures,
        seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        discovery_validation_results=[
            {
                "status": "ok",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "SECOND_A",
                "probe_timeframe": "M5",
                "probe_id": "drv-test",
                "primary_score": "72",
                "composite_score": "72",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.97",
                "retention_bucket": "retained_strong",
            }
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    recipe = seed_plan["recipes"]["discovered_recipe_001"]
    assert recipe["source"] == "discovery_recipe_validation"
    assert recipe["pair_menu"][0]["retention_bucket"] == "retained_strong"
    assert recipe["slot_menus"]["context_or_setup_cluster"][0]["indicator_id"] == "FIRST_A"
    assert recipe["slot_menus"]["trigger_or_response_cluster"][0]["indicator_id"] == "SECOND_A"
    assert any(row["source"] == "discovery_recipe_validation" for row in slot_rows)
    assert any(row["source"] == "discovery_recipe_validation" for row in pair_rows)
    assert negative_pairs == []
    assert retention_failures == []
    assert summary["result_counts"]["discovered_recipe_count"] == 1


def test_build_recipe_prior_artifacts_emits_negative_priors_for_retention_failures() -> None:
    indicators = [
        _indicator("FIRST_A", signal_role="setup", strategy_role="trend"),
        _indicator("SECOND_A", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        _slot_rows,
        _pair_rows,
        negative_pairs,
        negative_clusters,
        retention_failures,
        seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        discovery_validation_results=[
            {
                "status": "ok",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_cluster_id": "cluster_a",
                "second_cluster_id": "cluster_b",
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "SECOND_A",
                "probe_timeframe": "M5",
                "probe_id": "drv-failed",
                "primary_score": "0",
                "composite_score": "0",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0",
                "retention_bucket": "failed_retention",
            }
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    assert negative_pairs[0]["unordered_pair_id"] == "FIRST_A+SECOND_A"
    assert retention_failures[0]["negative_reason"] == "positive_discovery_collapsed"
    assert negative_clusters[0]["failure_rate"] == 1.0
    assert seed_plan["negative_pairs"][0]["probe_id"] == "drv-failed"
    assert summary["result_counts"]["negative_pair_rows"] == 1
