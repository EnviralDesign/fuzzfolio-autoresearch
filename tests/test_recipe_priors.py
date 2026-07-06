from __future__ import annotations

from autoresearch.recipe_priors import (
    build_recipe_prior_artifacts,
    build_timing_evidence,
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


def test_build_timing_evidence_maps_both_side_variants_to_anchor_and_trigger() -> None:
    evidence = build_timing_evidence(
        [
            {
                "anchor_type": "trend",
                "anchor_id": "ANCHOR_A",
                "trigger_id": "TRIGGER_X",
                "variant_side": "both",
                "variant_indicator_id": "ANCHOR_A+TRIGGER_X",
                "variant_lookback_bars": "4",
                "score_delta": "8",
                "timing_bucket": "improved",
                "timing_probe_id": "l3b-both",
            }
        ]
    )

    anchor = evidence[("trend_pullback_continuation", "ANCHOR_A")]
    trigger = evidence[("trend_pullback_continuation", "TRIGGER_X")]
    assert anchor["best_variant_side"] == "both"
    assert trigger["best_variant_side"] == "both"
    assert anchor["variant_side_counts"]["both"] == 1
    assert trigger["best_variant_lookback_bars"] == "4"

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


def test_score_slot_candidate_applies_anchor_side_timing_to_non_trigger_slot() -> None:
    row = _indicator("ANCHOR_A", signal_role="setup", strategy_role="mean-reversion")

    scored = score_slot_candidate(
        row,
        recipe_name="mean_reversion_reclaim",
        slot_name="setup",
        static_slot_scores={("mean_reversion_reclaim", "setup", "ANCHOR_A"): 76},
        signal_rollups={"ANCHOR_A": {"density_bucket": "usable"}},
        forward_priors={},
        trigger_pair_stats={},
        anchor_pair_stats={
            ("mean_reversion_reclaim", "ANCHOR_A"): {
                "count": 2,
                "positive_count": 1,
                "best_score": 62,
            }
        },
        timing_evidence={
            ("mean_reversion_reclaim", "ANCHOR_A"): {
                "material_improved_count": 1,
                "lost_positive_count": 0,
                "degraded_count": 0,
                "best_delta": 8,
                "best_variant_side": "anchor",
                "best_variant_lookback_bars": "12",
            }
        },
    )

    assert scored["timing_policy"] == "allow_variant"
    assert scored["timing_variant_side"] == "anchor"
    assert scored["recommended_trigger_lookback_bars"] == "12"


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


def test_build_recipe_prior_artifacts_reports_pair_prior_cap() -> None:
    indicators = [
        _indicator("ANCHOR_A", signal_role="setup", strategy_role="mean-reversion"),
        _indicator("TRIGGER_A", signal_role="trigger", strategy_role="mean-reversion"),
        _indicator("TRIGGER_B", signal_role="trigger", strategy_role="mean-reversion"),
        _indicator("TRIGGER_C", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        _slot_rows,
        pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        _seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[
            {
                "anchor_type": "mean_reversion",
                "anchor_id": "ANCHOR_A",
                "trigger_id": trigger_id,
                "probe_timeframe": "M5",
                "probe_id": f"l3-{trigger_id.lower()}",
                "pair_prior_score": "80",
                "composite_score": str(score),
            }
            for trigger_id, score in (
                ("TRIGGER_A", 70),
                ("TRIGGER_B", 65),
                ("TRIGGER_C", 60),
            )
        ],
        timing_results=[],
        max_slot_candidates=5,
        max_pair_candidates=2,
    )

    counts = summary["result_counts"]
    assert len(pair_rows) == 2
    assert counts["pair_prior_rows_before_cap"] == 3
    assert counts["pair_prior_rows"] == 2
    assert counts["pair_prior_rows_truncated_by_max"] == 1
    assert counts["pair_prior_source_counts_before_cap"] == {"anchor_pair_atlas": 3}
    assert counts["pair_prior_source_counts"] == {"anchor_pair_atlas": 2}


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


def test_build_recipe_prior_artifacts_injects_playhand_outcome_policy() -> None:
    indicators = [
        _indicator("RSI_CROSSBACK", signal_role="setup", strategy_role="mean-reversion"),
        _indicator("WILLR_MEAN_REVERSION", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        _slot_rows,
        pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
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
                "recipe_id": "discovered_recipe_006",
                "recipe_confidence": "high_candidate",
                "first_indicator_id": "RSI_CROSSBACK",
                "second_indicator_id": "WILLR_MEAN_REVERSION",
                "probe_timeframe": "M5",
                "probe_id": "drs-0002-r006-rsi-crossback-willr-mean-reversi-m5",
                "primary_score": "70",
                "composite_score": "70",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.95",
                "retention_bucket": "retained",
            }
        ],
        playhand_outcome_priors={
            "pair_families": {
                "drs-0002-r006-rsi-crossback-willr-mean-reversi-m5": {
                    "family_policy": "template_locked",
                    "source_batches": "playhand-prior-test-clean-100",
                    "family_cap_share": 0.15,
                    "recommended_max_indicators": 2,
                    "role_balanced_fill_limit": 0,
                    "mutation_pressure": "low",
                    "sampling_weight_multiplier": 1.15,
                    "exact_branch_required": True,
                    "exact_rescue_rate": 0.6667,
                    "mutated_win_rate": 0.1333,
                    "avg_mutation_delta": -44.3961,
                    "promotion_rate": 1.0,
                    "count": 15,
                }
            },
            "recipes": {
                "discovered_recipe_006": {
                    "recipe_policy": "template_locked",
                    "recipe_sampling_weight_multiplier": 1.10,
                    "recipe_cap_share": 0.30,
                    "promotion_rate": 1.0,
                    "count": 27,
                    "source_batches": "playhand-prior-test-clean-100",
                }
            },
        },
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    pair = seed_plan["recipes"]["discovered_recipe_006"]["pair_menu"][0]
    recipe = seed_plan["recipes"]["discovered_recipe_006"]
    assert pair_rows[0]["playhand_family_policy"] == "template_locked"
    assert pair["playhand_recommended_max_indicators"] == 2
    assert pair["playhand_role_balanced_fill_limit"] == 0
    assert pair["playhand_exact_branch_required"] is True
    assert pair["playhand_family_cap_share"] == 0.15
    assert pair["pair_sampling_weight"] == pair["pair_sampling_weight_uncapped"]
    assert recipe["playhand_recipe_policy"] == "template_locked"
    assert recipe["recipe_sampling_weight"] > 0
    assert summary["result_counts"]["outcome_prior_pair_rows_annotated"] == 1


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
            },
            {
                "status": "ok",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_cluster_id": "cluster_a",
                "second_cluster_id": "cluster_b",
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "SECOND_A",
                "probe_timeframe": "M5",
                "probe_id": "drv-retained",
                "primary_score": "70",
                "composite_score": "70",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.95",
                "retention_bucket": "retained_strong",
            }
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    assert negative_pairs[0]["unordered_pair_id"] == "FIRST_A+SECOND_A"
    assert retention_failures[0]["negative_reason"] == "positive_discovery_collapsed"
    assert negative_pairs[0]["negative_scope"] == "hard_unordered"
    assert negative_pairs[0]["negative_scope_type"] == "unordered_pair_timeframe"
    assert negative_pairs[0]["negative_reason_category"] == "positive_discovery_collapsed"
    assert negative_pairs[0]["negative_evidence_strength"] > 0
    assert negative_pairs[0]["source_stage"] == "validation_unknown"
    assert negative_pairs[0]["decay_policy"] == "soft_decay_next_atlas_runs"
    assert negative_pairs[0]["is_hard_block"] is False
    assert negative_clusters[0]["tested_count"] == 2
    assert negative_clusters[0]["retained_count"] == 1
    assert negative_clusters[0]["failure_rate"] == 0.5
    assert negative_clusters[0]["retained_rate"] == 0.5
    assert seed_plan["negative_pairs"][0]["probe_id"] == "drv-failed"
    assert seed_plan["negative_pairs"][0]["features"]["negative_scope_type"] == "unordered_pair_timeframe"
    assert summary["result_counts"]["negative_pair_rows"] == 1
    assert summary["negative_priors"]["negative_reason_category_counts"] == {
        "positive_discovery_collapsed": 1
    }


def test_build_recipe_prior_artifacts_uses_36m_result_over_12m_retention() -> None:
    indicators = [
        _indicator("FIRST_A", signal_role="setup", strategy_role="trend"),
        _indicator("SECOND_A", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        _slot_rows,
        pair_rows,
        negative_pairs,
        _negative_clusters,
        _retention_failures,
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
                "probe_id": "drv-12m",
                "lookback_months": "12",
                "primary_score": "72",
                "composite_score": "72",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.97",
                "retention_bucket": "retained_strong",
            },
            {
                "status": "ok",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "SECOND_A",
                "probe_timeframe": "M5",
                "probe_id": "drs-36m",
                "lookback_months": "36",
                "primary_score": "0",
                "composite_score": "0",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0",
                "retention_bucket": "failed_retention",
            },
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    assert "discovered_recipe_001" not in seed_plan["recipes"]
    assert all(row.get("probe_id") != "drv-12m" for row in pair_rows)
    assert negative_pairs[0]["probe_id"] == "drs-36m"
    assert summary["discovered_recipe_validation"]["latest_pair_rows"] == 1
    assert summary["discovered_recipe_validation"]["multi_horizon_family_rows"] == 1
    assert summary["result_counts"]["discovered_recipe_pair_rows"] == 0


def test_build_recipe_prior_artifacts_preserves_horizon_evidence_for_retained_pairs() -> None:
    indicators = [
        _indicator("FIRST_A", signal_role="setup", strategy_role="trend"),
        _indicator("SECOND_A", signal_role="trigger", strategy_role="mean-reversion"),
    ]

    (
        _payload,
        _slot_rows,
        pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
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
                "probe_id": "drv-12m",
                "lookback_months": "12",
                "primary_score": "60",
                "composite_score": "60",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.82",
                "retention_bucket": "retained",
                "best_trades": "80",
            },
            {
                "status": "ok",
                "recipe_id": "discovered_recipe_001",
                "recipe_confidence": "high_candidate",
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "SECOND_A",
                "probe_timeframe": "M5",
                "probe_id": "drs-36m",
                "lookback_months": "36",
                "primary_score": "63",
                "composite_score": "63",
                "validation_priority_score": "70",
                "discovery_evidence_score": "74",
                "retention_ratio": "0.88",
                "retention_bucket": "retained",
                "best_trades": "160",
            },
        ],
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    pair = pair_rows[0]
    seed_pair = seed_plan["recipes"]["discovered_recipe_001"]["pair_menu"][0]
    seed_slot = seed_plan["recipes"]["discovered_recipe_001"]["slot_menus"][
        "context_or_setup_cluster"
    ][0]
    assert pair["probe_id"] == "drs-36m"
    assert pair["canonical_pair_family_id"] == "discovered_recipe_001|M5|FIRST_A+SECOND_A"
    assert pair["horizon_stability_bucket"] == "retained_36m"
    assert [row["lookback_months"] for row in pair["horizon_evidence"]] == [12, 36]
    assert seed_pair["horizon_evidence"] == pair["horizon_evidence"]
    assert seed_plan["feature_schema_version"] == "atlas_feature_vector_v1"
    assert pair["features"]["horizon_evidence_count"] == 2
    assert seed_slot["features"]["sample_confidence"] == "high"
    assert pair["sample_confidence"] == "high"
    assert summary["discovered_recipe_validation"]["multi_horizon_family_rows"] == 1


def test_build_recipe_prior_artifacts_tiers_sampling_policy_by_distinct_36m_families() -> None:
    indicators = [
        _indicator(f"FIRST_{index}", signal_role="setup", strategy_role="trend")
        for index in range(1, 5)
    ] + [
        _indicator(f"SECOND_{index}", signal_role="trigger", strategy_role="mean-reversion")
        for index in range(1, 5)
    ]

    def retained_row(index: int, *, first: str | None = None, second: str | None = None) -> dict[str, str]:
        return {
            "status": "ok",
            "recipe_id": "discovered_recipe_001",
            "recipe_confidence": "high_candidate",
            "first_indicator_id": first or f"FIRST_{index}",
            "second_indicator_id": second or f"SECOND_{index}",
            "probe_timeframe": "M5",
            "probe_id": f"drs-36m-{index}",
            "lookback_months": "36",
            "primary_score": "70",
            "composite_score": "70",
            "validation_priority_score": "70",
            "discovery_evidence_score": "74",
            "retention_ratio": "0.95",
            "retention_bucket": "retained",
        }

    (
        _payload,
        _slot_rows,
        _pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        pre_36m_seed_plan,
        _summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        discovery_validation_results=[],
        max_slot_candidates=10,
        max_pair_candidates=10,
    )

    assert pre_36m_seed_plan["sampling_policy"]["retained_36m_family_count"] == 0
    assert pre_36m_seed_plan["sampling_policy"]["guided_prior_fraction"] == 0.60
    assert pre_36m_seed_plan["sampling_policy"]["uncertain_prior_fraction"] == 0.25
    assert pre_36m_seed_plan["sampling_policy"]["wild_exploration_fraction"] == 0.15
    assert pre_36m_seed_plan["sampling_policy"]["guided_recipe_source_mix"] == {
        "discovery_recipe_validation": 0.25,
        "curated_recipe_prior": 0.75,
    }
    assert pre_36m_seed_plan["sampling_policy"]["maturity"] == "pre_36m_retention"

    (
        _payload,
        _slot_rows,
        _pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        limited_seed_plan,
        _summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        discovery_validation_results=[
            retained_row(1),
            retained_row(11, first="SECOND_1", second="FIRST_1"),
        ],
        max_slot_candidates=10,
        max_pair_candidates=10,
    )

    assert limited_seed_plan["sampling_policy"]["retained_36m_family_count"] == 1
    assert limited_seed_plan["sampling_policy"]["guided_prior_fraction"] == 0.70
    assert limited_seed_plan["sampling_policy"]["uncertain_prior_fraction"] == 0.20
    assert limited_seed_plan["sampling_policy"]["wild_exploration_fraction"] == 0.10
    assert limited_seed_plan["sampling_policy"]["guided_recipe_source_mix"] == {
        "discovery_recipe_validation": 0.45,
        "curated_recipe_prior": 0.55,
    }
    assert limited_seed_plan["sampling_policy"]["maturity"] == "limited_36m_retention"

    (
        _payload,
        _slot_rows,
        _pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        broad_seed_plan,
        _summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=indicators,
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        discovery_validation_results=[retained_row(index) for index in range(1, 5)],
        max_slot_candidates=10,
        max_pair_candidates=10,
    )

    assert broad_seed_plan["sampling_policy"]["retained_36m_family_count"] == 4
    assert broad_seed_plan["sampling_policy"]["guided_prior_fraction"] == 0.80
    assert broad_seed_plan["sampling_policy"]["uncertain_prior_fraction"] == 0.15
    assert broad_seed_plan["sampling_policy"]["wild_exploration_fraction"] == 0.05
    assert broad_seed_plan["sampling_policy"]["guided_recipe_source_mix"] == {
        "discovery_recipe_validation": 0.60,
        "curated_recipe_prior": 0.40,
    }
    assert broad_seed_plan["sampling_policy"]["maturity"] == "broad_36m_retention"
