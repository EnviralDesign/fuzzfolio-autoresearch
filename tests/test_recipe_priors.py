from __future__ import annotations

import pytest

from autoresearch.recipe_priors import (
    CampaignPolicyValidationError,
    build_campaign_policy_manifest,
    build_recipe_prior_artifacts,
    build_timing_evidence,
    campaign_diversity_cap_count,
    canonical_campaign_candidate_attributes,
    canonical_campaign_candidate_id,
    canonical_campaign_policy_json,
    is_negative_prior_active,
    negative_prior_expiry_status,
    ordered_campaign_policy_conflicts,
    score_slot_candidate,
    timing_policy_for,
    validate_campaign_policy_manifest,
    validate_seed_plan_campaign_policy,
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


def _phase3_policy_manifest(
    *,
    lane_fractions: dict[str, float] | None = None,
    diversity_max_shares: dict[str, float] | None = None,
    lane_eligible_menus: dict[str, object] | None = None,
    source_atlas_generation: str = "atlas-generation-2026-07-21",
    source_atlas_run_sequence: int = 41,
) -> dict[str, object]:
    return build_campaign_policy_manifest(
        lane_fractions=lane_fractions
        or {"guided": 0.60, "uncertain": 0.25, "wild": 0.15},
        lane_eligible_menus=lane_eligible_menus
        or {
            "guided": {
                "recipe_sources": [
                    "discovery_recipe_validation",
                    "curated_recipe_prior",
                ],
                "slot_sampling_lanes": ["medium_prior", "high_prior"],
                "pair_sampling_lanes": ["positive_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "uncertain": {
                "recipe_sources": [
                    "curated_recipe_prior",
                    "discovery_recipe_validation",
                ],
                "slot_sampling_lanes": ["uncertain_prior"],
                "pair_sampling_lanes": ["near_miss_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "wild": {
                "recipe_sources": ["curated_recipe_prior"],
                "slot_sampling_lanes": ["wild_exploration"],
                "pair_sampling_lanes": ["low_pair"],
                "allow_generation_eligible_fallback": True,
            },
        },
        diversity_max_shares=diversity_max_shares
        or {
            "family": 0.05,
            "recipe": 0.30,
            "instrument": 0.10,
            "timeframe": 0.60,
            "indicator": 0.15,
        },
        source_atlas_generation=source_atlas_generation,
        source_atlas_run_sequence=source_atlas_run_sequence,
    )


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


def test_campaign_policy_manifest_has_deterministic_canonical_identity() -> None:
    first = _phase3_policy_manifest()
    second = _phase3_policy_manifest()
    changed_anchor = _phase3_policy_manifest(source_atlas_run_sequence=42)
    tampered = {
        **first,
        "negative_prior_expiry": {
            **first["negative_prior_expiry"],
            "anchor": {
                **first["negative_prior_expiry"]["anchor"],
                "run_sequence": 42,
            },
        },
    }

    assert first == second
    assert canonical_campaign_policy_json(first) == canonical_campaign_policy_json(second)
    assert first["manifest_sha256"] == second["manifest_sha256"]
    assert first["manifest_sha256"] != changed_anchor["manifest_sha256"]
    assert validate_campaign_policy_manifest(first) == first
    with pytest.raises(CampaignPolicyValidationError, match="does not match canonical"):
        validate_campaign_policy_manifest(tampered)


def test_campaign_policy_manifest_normalizes_whitespace_menu_tokens_before_export() -> None:
    baseline = _phase3_policy_manifest()
    whitespace_menus = {
        lane: {
            **baseline["lanes"][lane]["eligible_menus"],
            "recipe_sources": [
                f"  {source}  "
                for source in baseline["lanes"][lane]["eligible_menus"][
                    "recipe_sources"
                ]
            ],
            "slot_sampling_lanes": [
                f"  {sampling_lane}  "
                for sampling_lane in baseline["lanes"][lane]["eligible_menus"][
                    "slot_sampling_lanes"
                ]
            ],
            "pair_sampling_lanes": [
                f"  {sampling_lane}  "
                for sampling_lane in baseline["lanes"][lane]["eligible_menus"][
                    "pair_sampling_lanes"
                ]
            ],
        }
        for lane in ("guided", "uncertain", "wild")
    }

    normalized = _phase3_policy_manifest(lane_eligible_menus=whitespace_menus)

    assert normalized == baseline
    assert all(
        token == token.strip()
        for lane in normalized["lanes"].values()
        for menu_name in ("recipe_sources", "slot_sampling_lanes", "pair_sampling_lanes")
        for token in lane["eligible_menus"][menu_name]
    )


@pytest.mark.parametrize(
    ("lane", "field", "replacement"),
    [
        ("guided", "pair_sampling_lanes", ["near_miss_pair"]),
        ("uncertain", "pair_sampling_lanes", ["low_pair"]),
        ("wild", "recipe_sources", ["discovery_recipe_validation"]),
        ("wild", "slot_sampling_lanes", ["uncertain_prior"]),
    ],
)
def test_campaign_policy_manifest_rejects_cross_lane_menu_sources(
    lane: str,
    field: str,
    replacement: list[str],
) -> None:
    baseline = _phase3_policy_manifest()
    menus = {
        menu_lane: dict(baseline["lanes"][menu_lane]["eligible_menus"])
        for menu_lane in ("guided", "uncertain", "wild")
    }
    menus[lane][field] = replacement

    with pytest.raises(CampaignPolicyValidationError, match="unsupported values"):
        _phase3_policy_manifest(lane_eligible_menus=menus)


def test_campaign_policy_manifest_binds_deterministic_allocation_metadata() -> None:
    policy = _phase3_policy_manifest()
    execution = policy["execution"]
    tampered = {
        **policy,
        "execution": {
            **execution,
            "allocation_algorithm_version": "v2",
        },
    }

    assert execution == {
        "allocation_algorithm": "hamilton_largest_remainder",
        "allocation_algorithm_version": "v1",
        "quota_basis": "campaign_lane_budget_times_lane_fraction",
        "quota_rounding": "floor_then_descending_fractional_remainder",
        "lane_tie_break_order": ["guided", "uncertain", "wild"],
        "candidate_tie_break_order": [
            "recipe_id",
            "canonical_pair_family_id",
            "instrument",
            "timeframe",
            "indicator_ids",
            "candidate_id",
        ],
        "candidate_tie_break_direction": "ascending_lexicographic",
        "candidate_evaluation_order": (
            "lane_order_then_ascending_lexicographic_candidate_tie_break_order"
        ),
        "random_tie_break": "forbidden",
    }
    assert "seed" not in execution
    with pytest.raises(CampaignPolicyValidationError, match="deterministic allocation"):
        validate_campaign_policy_manifest(tampered)


def test_campaign_candidate_id_normalizes_scalars_and_multi_indicators() -> None:
    noisy_candidate = {
        "recipe_id": " discovered_recipe_001 ",
        "canonical_pair_family_id": " discovered_recipe_001|m5|rsi+willr ",
        "instrument": " eurusd ",
        "timeframe": " m5 ",
        "indicator_ids": [" willr ", "RSI", "rsi", "", None, "WILLR"],
    }
    normalized_candidate = {
        "recipe_id": {"kind": "value", "value": "DISCOVERED_RECIPE_001"},
        "canonical_pair_family_id": {
            "kind": "value",
            "value": "DISCOVERED_RECIPE_001|M5|RSI+WILLR",
        },
        "instrument": {"kind": "value", "value": "EURUSD"},
        "timeframe": {"kind": "value", "value": "M5"},
        "indicator_ids": {
            "state": "present",
            "values": [
                {"kind": "absent"},
                {"kind": "blank"},
                {"kind": "value", "value": "RSI"},
                {"kind": "value", "value": "WILLR"},
            ],
        },
    }
    equivalent_candidate = {
        "recipe_id": "DISCOVERED_RECIPE_001",
        "canonical_pair_family_id": "DISCOVERED_RECIPE_001|M5|RSI+WILLR",
        "instrument": "EURUSD",
        "timeframe": "M5",
        "indicator_ids": [None, "", "RSI", "WILLR"],
    }

    assert canonical_campaign_candidate_attributes(noisy_candidate) == normalized_candidate
    assert canonical_campaign_candidate_id(noisy_candidate) == canonical_campaign_candidate_id(
        equivalent_candidate
    )


def test_campaign_candidate_id_normalizes_missing_attributes_explicitly() -> None:
    missing_attributes = canonical_campaign_candidate_attributes({"indicator_ids": []})
    policy = _phase3_policy_manifest()
    tampered = {
        **policy,
        "candidate_identity": {
            **policy["candidate_identity"],
            "scalar_normalization": "in_band_missing_sentinel",
        },
    }

    assert missing_attributes == {
        "recipe_id": {"kind": "absent"},
        "canonical_pair_family_id": {"kind": "absent"},
        "instrument": {"kind": "absent"},
        "timeframe": {"kind": "absent"},
        "indicator_ids": {"state": "present", "values": []},
    }
    assert canonical_campaign_candidate_id({}) == canonical_campaign_candidate_id(
        {"indicator_ids": None}
    )
    assert "missing_value" not in policy["candidate_identity"]
    assert policy["candidate_identity"]["scalar_representation"]["absent"] == {
        "kind": "absent"
    }
    with pytest.raises(CampaignPolicyValidationError, match="canonical candidate_id contract"):
        validate_campaign_policy_manifest(tampered)


def test_campaign_candidate_id_keeps_absent_blank_and_literal_values_distinct() -> None:
    common_candidate = {
        "canonical_pair_family_id": "family",
        "instrument": "EURUSD",
        "timeframe": "M5",
    }
    scalar_variants = [
        common_candidate,
        {**common_candidate, "recipe_id": "  "},
        {**common_candidate, "recipe_id": "<MISSING>"},
        {**common_candidate, "recipe_id": "ordinary"},
    ]
    indicator_variants = [
        {**common_candidate, "recipe_id": "recipe", "indicator_ids": None},
        {**common_candidate, "recipe_id": "recipe", "indicator_ids": [""]},
        {**common_candidate, "recipe_id": "recipe", "indicator_ids": ["<MISSING>"]},
        {**common_candidate, "recipe_id": "recipe", "indicator_ids": ["ordinary"]},
    ]

    assert len({canonical_campaign_candidate_id(candidate) for candidate in scalar_variants}) == 4
    assert len({canonical_campaign_candidate_id(candidate) for candidate in indicator_variants}) == 4
    assert canonical_campaign_candidate_attributes(scalar_variants[0])["recipe_id"] == {
        "kind": "absent"
    }
    assert canonical_campaign_candidate_attributes(scalar_variants[1])["recipe_id"] == {
        "kind": "blank"
    }
    assert canonical_campaign_candidate_attributes(scalar_variants[2])["recipe_id"] == {
        "kind": "value",
        "value": "<MISSING>",
    }
    assert canonical_campaign_candidate_attributes(indicator_variants[0])["indicator_ids"] == {
        "state": "absent",
        "values": [],
    }
    assert canonical_campaign_candidate_attributes(indicator_variants[1])["indicator_ids"] == {
        "state": "present",
        "values": [{"kind": "blank"}],
    }
    assert canonical_campaign_candidate_attributes(indicator_variants[2])["indicator_ids"] == {
        "state": "present",
        "values": [{"kind": "value", "value": "<MISSING>"}],
    }


def test_campaign_policy_manifest_binds_cap_denominator_and_floor_rounding() -> None:
    policy = _phase3_policy_manifest()
    enforcement = policy["diversity_enforcement"]

    assert enforcement["denominator_attribute"] == "target_runs"
    assert enforcement["denominator_definition"] == "total planned campaign deals across all lanes"
    assert enforcement["cap_limit_formula"] == "floor(target_runs * max_share)"
    assert campaign_diversity_cap_count(policy, dimension="family", target_runs=19) == 0
    assert campaign_diversity_cap_count(policy, dimension="family", target_runs=20) == 1
    assert campaign_diversity_cap_count(policy, dimension="indicator", target_runs=13) == 1


def test_campaign_policy_manifest_orders_simultaneous_conflicts_atomically() -> None:
    policy = _phase3_policy_manifest()
    enforcement = policy["diversity_enforcement"]

    assert ordered_campaign_policy_conflicts({"indicator", "family", "timeframe"}) == [
        "family",
        "timeframe",
        "indicator",
    ]
    assert enforcement["candidate_acceptance"] == "all_caps_checked_atomically_before_acceptance"
    assert enforcement["candidate_conflict_result"] == "reject_candidate_and_report_all_conflicts"
    assert enforcement["conflict_report_order"] == [
        "family",
        "recipe",
        "instrument",
        "timeframe",
        "indicator",
    ]


def test_campaign_policy_manifest_binds_lane_exhaustion_without_cross_lane_borrowing() -> None:
    policy = _phase3_policy_manifest()
    enforcement = policy["diversity_enforcement"]
    tampered = {
        **policy,
        "diversity_enforcement": {
            **enforcement,
            "lane_exhaustion_result_type": "lane_exhausted",
        },
    }

    assert enforcement["lane_capacity"] == "fixed_allocated_lane_quota_no_cross_lane_borrowing"
    assert enforcement["lane_exhaustion_result_type"] == "policy_lane_exhausted"
    assert "allocated_lane_quota_unfilled" in enforcement["lane_exhaustion_condition"]
    with pytest.raises(CampaignPolicyValidationError, match="cap enforcement contract"):
        validate_campaign_policy_manifest(tampered)


def test_campaign_policy_manifest_requires_each_diversity_attribute_contract() -> None:
    policy = _phase3_policy_manifest()
    contract = policy["diversity_attribute_contract"]
    tampered = {
        **policy,
        "diversity_attribute_contract": {
            **contract,
            "instrument": {
                **contract["instrument"],
                "source_path": "recipes.<recipe_id>.pair_menu[].instrument",
            },
        },
    }

    assert set(contract) == {"family", "recipe", "instrument", "timeframe", "indicator"}
    assert contract["family"]["candidate_attribute"] == "canonical_pair_family_id"
    assert contract["instrument"]["source_path"] == "runtime_candidate.instrument"
    assert contract["timeframe"]["definition"] == "the resolved timeframe for the selected candidate"
    assert contract["indicator"]["aggregation"] == "one_charge_per_indicator_id_in_selected_deal"
    with pytest.raises(CampaignPolicyValidationError, match="required attribute contract"):
        validate_campaign_policy_manifest(tampered)


@pytest.mark.parametrize(
    ("lane_fractions", "diversity_max_shares", "message"),
    [
        (
            {"guided": 0.55, "uncertain": 0.25, "wild": 0.15},
            None,
            "must sum to exactly 1",
        ),
        (
            None,
            {
                "family": 1.01,
                "recipe": 1.01,
                "instrument": 0.10,
                "timeframe": 0.60,
                "indicator": 0.15,
            },
            "greater than 0 and at most 1",
        ),
    ],
)
def test_campaign_policy_manifest_rejects_invalid_quotas_and_caps(
    lane_fractions: dict[str, float] | None,
    diversity_max_shares: dict[str, float] | None,
    message: str,
) -> None:
    with pytest.raises(CampaignPolicyValidationError, match=message):
        _phase3_policy_manifest(
            lane_fractions=lane_fractions,
            diversity_max_shares=diversity_max_shares,
        )


def test_negative_prior_expiry_is_active_through_the_anchor_ttl_boundary() -> None:
    policy = _phase3_policy_manifest(source_atlas_run_sequence=41)
    negative_prior = {"expires_after_atlas_runs": 3}

    assert (
        negative_prior_expiry_status(
            negative_prior,
            policy,
            current_atlas_generation="atlas-generation-2026-07-21",
            current_atlas_run_sequence=40,
        )
        == "not_yet_active"
    )
    assert is_negative_prior_active(
        negative_prior,
        policy,
        current_atlas_generation="atlas-generation-2026-07-21",
        current_atlas_run_sequence=44,
    )
    assert (
        negative_prior_expiry_status(
            negative_prior,
            policy,
            current_atlas_generation="atlas-generation-2026-07-21",
            current_atlas_run_sequence=45,
        )
        == "expired_run_sequence"
    )
    assert (
        negative_prior_expiry_status(
            negative_prior,
            policy,
            current_atlas_generation="different-generation",
            current_atlas_run_sequence=41,
        )
        == "expired_generation_mismatch"
    )


def test_seed_plan_campaign_policy_preserves_legacy_and_requires_explicit_v2_manifest() -> None:
    legacy_seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {},
    }
    assert validate_seed_plan_campaign_policy(legacy_seed_plan) is None

    policy = _phase3_policy_manifest()
    (
        payload,
        _slot_rows,
        _pair_rows,
        _negative_pairs,
        _negative_clusters,
        _retention_failures,
        seed_plan,
        summary,
    ) = build_recipe_prior_artifacts(
        indicator_rows=[
            _indicator("CONTEXT_A", signal_role="context", strategy_role="trend"),
            _indicator("SETUP_A", signal_role="setup", strategy_role="mean-reversion"),
            _indicator("TRIGGER_A", signal_role="trigger", strategy_role="mean-reversion"),
        ],
        static_slot_scores={},
        signal_rollups={},
        forward_priors={},
        pair_results=[],
        timing_results=[],
        campaign_policy_manifest=policy,
        max_slot_candidates=5,
        max_pair_candidates=5,
    )

    assert seed_plan["schema_version"] == "play_hand_seed_plan_v2"
    assert seed_plan["campaign_policy_sha256"] == policy["manifest_sha256"]
    assert seed_plan["sampling_policy"]["guided_prior_fraction"] == 0.60
    assert validate_seed_plan_campaign_policy(seed_plan) == policy
    with pytest.raises(CampaignPolicyValidationError, match="expected digest"):
        validate_seed_plan_campaign_policy(seed_plan, expected_policy_sha256="")
    assert payload["campaign_policy_sha256"] == policy["manifest_sha256"]
    assert summary["campaign_policy"]["manifest_sha256"] == policy["manifest_sha256"]
