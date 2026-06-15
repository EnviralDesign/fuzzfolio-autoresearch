import json
import random
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import autoresearch.play_hand as play_hand_mod

from autoresearch.play_hand import (
    PlayHandContext,
    _best_sweep_parameters,
    build_early_exit_decision,
    _cleanup_registered_profiles,
    _curve_features,
    _finalize_play_hand_attempt_metadata,
    _evaluate_instrument_scout_records,
    _evaluate_final_scrutiny_branch_candidates,
    _instrument_curve_similarity,
    _instrument_scout_worker_count,
    _json_payload_from_stdout,
    _normalize_sweep_payload,
    _parameter_importance,
    _permutation_count,
    _play_hand_artifact_commands,
    _repair_degenerate_profile_ranges,
    _refine_values,
    _reward_matrix_cli_args,
    _select_instrument_scout_records,
    _sweep_parameter_candidates,
    _sweep_id_from_stderr,
    _sweep_progress_from_cli_stderr,
    _sweep_progress_from_state,
    _top_sweep_score,
    apply_play_hand_profile_defaults,
    apply_seed_pair_template_defaults,
    build_coarse_axes,
    build_focused_axes,
    build_lookback_axes,
    build_required_lookback_axes,
    deal_seed_plan_indicators,
    deal_indicator_count,
    deal_instruments,
    evolutionary_budget_settings,
    fit_axes_to_permutation_budget,
    materialize_profile_variant,
    plan_sweep_axes,
    play_hand_reward_matrix,
    resolve_sweep_budget,
)
from autoresearch.ledger import load_attempts, write_attempts


def _family_policy_seed_plan(policy: str) -> dict[str, object]:
    return {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "discovered_recipe_006": {
                "source": "discovery_recipe_validation",
                "pair_menu": [
                    {
                        "source": "discovery_recipe_validation",
                        "anchor_id": "RSI_CROSSBACK",
                        "trigger_id": "STOCH_CROSSOVER",
                        "probe_id": f"probe-{policy}",
                        "pair_sampling_weight": 100,
                        "pair_sampling_score": 80,
                        "playhand_family_id": f"family-{policy}",
                        "playhand_family_policy": policy,
                        "playhand_exact_branch_required": policy == "template_locked",
                        "playhand_recommended_max_indicators": 2,
                        "playhand_role_balanced_fill_limit": 0
                        if policy == "template_locked"
                        else 1,
                        "playhand_mutation_pressure": "low"
                        if policy == "template_locked"
                        else "guarded",
                        "recommended_profile_template": {
                            "probe_id": f"probe-{policy}",
                            "timeframe": "M5",
                            "indicator_defaults": [
                                {
                                    "indicator_id": "RSI_CROSSBACK",
                                    "timeframe": "M5",
                                    "lookbackBars": 7,
                                },
                                {
                                    "indicator_id": "STOCH_CROSSOVER",
                                    "timeframe": "M5",
                                    "lookbackBars": 5,
                                },
                            ],
                        },
                    }
                ],
                "slot_menus": {},
            }
        },
    }


def test_repair_degenerate_profile_ranges_expands_binary_defaults(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(
        """{
  "profile": {
    "indicators": [
      {"config": {"ranges": {"buy": [1, 1], "sell": [0, 0]}}},
      {"config": {"ranges": {"buy": [2, 1], "sell": [0.2, 0.4]}}}
    ]
  }
}""",
        encoding="utf-8",
    )

    assert _repair_degenerate_profile_ranges(profile_path) is True

    payload = play_hand_mod._load_json(profile_path)
    first_ranges = payload["profile"]["indicators"][0]["config"]["ranges"]
    second_ranges = payload["profile"]["indicators"][1]["config"]["ranges"]
    assert first_ranges["buy"] == [0.5, 1.0]
    assert first_ranges["sell"] == [0.0, 0.5]
    assert second_ranges["buy"] == [1.0, 2.0]
    assert second_ranges["sell"] == [0.2, 0.4]


def test_deal_seed_plan_indicators_uses_validated_discovered_pair() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("FIRST_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("SECOND_A", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("FILL_A", "filter", "state", "higher-context"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "discovered_recipe_001": {
                "source": "discovery_recipe_validation",
                "recipe_confidence": "high_candidate",
                "pair_menu": [
                    {
                        "source": "discovery_recipe_validation",
                        "anchor_id": "FIRST_A",
                        "trigger_id": "SECOND_A",
                        "probe_id": "drv-test",
                        "probe_timeframe": "M5",
                        "pair_sampling_weight": 50,
                        "pair_sampling_score": 75,
                        "retention_bucket": "retained_strong",
                    }
                ],
                "slot_menus": {},
            }
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=3,
        seed_plan=seed_plan,
        rng=random.Random(7),
    )

    assert deal["source"] == "play_hand_seed_plan"
    assert deal["recipe"] == "discovered_recipe_001"
    assert [indicator.id for indicator in deal["indicators"][:2]] == ["FIRST_A", "SECOND_A"]
    assert deal["pair"]["retention_bucket"] == "retained_strong"


def test_deal_seed_plan_indicators_can_use_seed_plan_candidate_pair() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("FILL_A", "filter", "state", "higher-context"),
        play_hand_mod.SeedIndicator("FILL_B", "trigger", "event", "entry"),
    ]
    seed_plan_candidates = [
        play_hand_mod.SeedIndicator("FIRST_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("SECOND_A", "trigger", "event", "entry"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "discovered_recipe_001": {
                "source": "discovery_recipe_validation",
                "pair_menu": [
                    {
                        "source": "discovery_recipe_validation",
                        "anchor_id": "FIRST_A",
                        "trigger_id": "SECOND_A",
                        "pair_sampling_weight": 50,
                        "pair_sampling_score": 75,
                        "retention_bucket": "retained",
                    }
                ],
                "slot_menus": {},
            }
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=2,
        seed_plan=seed_plan,
        rng=random.Random(7),
        seed_plan_candidates=seed_plan_candidates,
    )

    assert [indicator.id for indicator in deal["indicators"]] == ["FIRST_A", "SECOND_A"]
    assert deal["pair"]["retention_bucket"] == "retained"


def test_deal_seed_plan_indicators_keeps_policy_exploration_on_seed_prompt_pool() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("FILL_A", "filter", "state", "higher-context"),
        play_hand_mod.SeedIndicator("FILL_B", "trigger", "event", "entry"),
    ]
    seed_plan_candidates = [
        play_hand_mod.SeedIndicator("FIRST_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("SECOND_A", "trigger", "event", "entry"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 0.0},
        "recipes": {
            "discovered_recipe_001": {
                "source": "discovery_recipe_validation",
                "pair_menu": [
                    {
                        "anchor_id": "FIRST_A",
                        "trigger_id": "SECOND_A",
                        "pair_sampling_weight": 50,
                    }
                ],
                "slot_menus": {},
            }
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=2,
        seed_plan=seed_plan,
        rng=random.Random(7),
        seed_plan_candidates=seed_plan_candidates,
    )

    assert deal["source"] == "role_balanced_policy_exploration"
    assert {indicator.id for indicator in deal["indicators"]} == {"FILL_A", "FILL_B"}


def test_deal_seed_plan_indicators_uses_explicit_recipe_sampling_weight() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("WEAK_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("WEAK_B", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("STRONG_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("STRONG_B", "trigger", "event", "entry"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "huge_pair_menu_but_capped": {
                "recipe_sampling_weight": 1,
                "pair_menu": [
                    {
                        "anchor_id": "WEAK_A",
                        "trigger_id": "WEAK_B",
                        "pair_sampling_weight": 10000,
                    }
                ],
                "slot_menus": {},
            },
            "small_pair_menu_but_boosted": {
                "recipe_sampling_weight": 100,
                "pair_menu": [
                    {
                        "anchor_id": "STRONG_A",
                        "trigger_id": "STRONG_B",
                        "pair_sampling_weight": 1,
                    }
                ],
                "slot_menus": {},
            },
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=2,
        seed_plan=seed_plan,
        rng=random.Random(0),
    )

    assert deal["recipe"] == "small_pair_menu_but_boosted"
    assert [indicator.id for indicator in deal["indicators"]] == ["STRONG_A", "STRONG_B"]


def test_deal_seed_plan_indicators_selects_guided_recipe_source_bucket() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("CURATED_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("CURATED_B", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("DISC_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("DISC_B", "trigger", "event", "entry"),
    ]
    seed_plan = {
        "sampling_policy": {
            "guided_prior_fraction": 1.0,
            "guided_recipe_source_mix": {
                "discovery_recipe_validation": 1.0,
                "curated_recipe_prior": 0.0,
            },
        },
        "recipes": {
            "curated_high_weight": {
                "source": "curated_recipe_prior",
                "recipe_sampling_weight": 10000,
                "pair_menu": [
                    {
                        "anchor_id": "CURATED_A",
                        "trigger_id": "CURATED_B",
                        "pair_sampling_weight": 10000,
                    }
                ],
                "slot_menus": {},
            },
            "discovered_low_weight": {
                "source": "discovery_recipe_validation",
                "recipe_sampling_weight": 1,
                "pair_menu": [
                    {
                        "anchor_id": "DISC_A",
                        "trigger_id": "DISC_B",
                        "pair_sampling_weight": 1,
                    }
                ],
                "slot_menus": {},
            },
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=2,
        seed_plan=seed_plan,
        rng=random.Random(0),
    )

    assert deal["recipe"] == "discovered_low_weight"
    assert deal["recipe_source"] == "discovery_recipe_validation"
    assert deal["guided_recipe_source_bucket"] == "discovery_recipe_validation"
    assert deal["guided_recipe_source_bucket_matched"] is True
    assert deal["guided_recipe_source_bucket_fallback"] is False
    assert [indicator.id for indicator in deal["indicators"]] == ["DISC_A", "DISC_B"]


def test_seed_pair_template_instruments_reads_validated_template_pool() -> None:
    pair = {
        "recommended_profile_template": {
            "instruments": ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]
        }
    }

    assert play_hand_mod._seed_pair_template_instruments(pair) == [
        "EURUSD",
        "GBPUSD",
        "USDJPY",
        "XAUUSD",
    ]


def test_seed_plan_indicator_metadata_reads_config_derived_root(tmp_path: Path) -> None:
    atlas_dir = tmp_path / "custom-derived" / "indicator-atlas"
    atlas_dir.mkdir(parents=True)
    (atlas_dir / "indicator-atlas.json").write_text(
        """{"indicators":[{"id":"RSI_CROSSBACK","signal_role":"trigger"}]}""",
        encoding="utf-8",
    )
    config = SimpleNamespace(
        derived_root=tmp_path / "custom-derived",
        runs_root=tmp_path / "wrong-runs-root",
    )

    metadata = play_hand_mod._seed_plan_indicator_metadata(config)

    assert metadata["RSI_CROSSBACK"]["signal_role"] == "trigger"


def test_seed_template_profile_path_prefers_existing_profile_path(tmp_path: Path) -> None:
    profile_path = tmp_path / "retained-template.json"
    profile_path.write_text('{"profile":{"indicators":[]}}', encoding="utf-8")

    resolved = play_hand_mod._seed_template_profile_path(
        {
            "profile_path": str(profile_path),
            "source_profile_path": str(tmp_path / "fallback.json"),
        }
    )

    assert resolved == profile_path


def test_seed_template_profile_path_returns_none_for_missing_template(tmp_path: Path) -> None:
    resolved = play_hand_mod._seed_template_profile_path(
        {
            "profile_path": str(tmp_path / "missing.json"),
        }
    )

    assert resolved is None


def test_select_final_scrutiny_branch_keeps_exact_template_when_it_passes() -> None:
    selected = play_hand_mod._select_final_scrutiny_branch(
        [
            {"branch": "mutated", "outcome": {"passed": False, "score": 0.0}},
            {"branch": "exact_template", "outcome": {"passed": True, "score": 61.5}},
        ]
    )

    assert selected["branch"] == "exact_template"


def test_select_final_scrutiny_branch_prefers_higher_mutated_score() -> None:
    selected = play_hand_mod._select_final_scrutiny_branch(
        [
            {"branch": "mutated", "outcome": {"passed": True, "score": 70.0}},
            {"branch": "exact_template", "outcome": {"passed": True, "score": 61.5}},
        ]
    )

    assert selected["branch"] == "mutated"


def test_deal_seed_plan_indicators_applies_negative_guard_to_role_balanced_fill() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("FIRST_A", "setup", "event", "mid-setup"),
        play_hand_mod.SeedIndicator("SECOND_A", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("BAD_FILL", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("GOOD_FILL", "trigger", "event", "entry"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "negative_pairs": [
            {
                "first_indicator_id": "FIRST_A",
                "second_indicator_id": "BAD_FILL",
                "negative_reason": "positive_discovery_collapsed",
                "negative_weight": 1.5,
            }
        ],
        "recipes": {
            "discovered_recipe_001": {
                "source": "discovery_recipe_validation",
                "pair_menu": [
                    {
                        "source": "discovery_recipe_validation",
                        "anchor_id": "FIRST_A",
                        "trigger_id": "SECOND_A",
                        "pair_sampling_weight": 50,
                        "pair_sampling_score": 75,
                    }
                ],
                "slot_menus": {},
            }
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=3,
        seed_plan=seed_plan,
        rng=random.Random(7),
    )

    assert [indicator.id for indicator in deal["indicators"]] == [
        "FIRST_A",
        "SECOND_A",
        "GOOD_FILL",
    ]


def test_deal_seed_plan_indicators_template_locked_caps_to_pair_only() -> None:
    indicators = [
        play_hand_mod.SeedIndicator("RSI_CROSSBACK", "setup", "event", "entry"),
        play_hand_mod.SeedIndicator("WILLR_MEAN_REVERSION", "trigger", "event", "entry"),
        play_hand_mod.SeedIndicator("SLOT_ADDON", "filter", "state", "higher-context"),
        play_hand_mod.SeedIndicator("RANDOM_FILL", "filter", "state", "higher-context"),
    ]
    seed_plan = {
        "sampling_policy": {"guided_prior_fraction": 1.0},
        "recipes": {
            "discovered_recipe_006": {
                "source": "discovery_recipe_validation",
                "pair_menu": [
                    {
                        "source": "discovery_recipe_validation",
                        "anchor_id": "RSI_CROSSBACK",
                        "trigger_id": "WILLR_MEAN_REVERSION",
                        "probe_id": "drs-0002-r006-rsi-crossback-willr-mean-reversi-m5",
                        "pair_sampling_weight": 50,
                        "pair_sampling_score": 75,
                        "playhand_family_id": "drs-0002-r006-rsi-crossback-willr-mean-reversi-m5",
                        "playhand_family_policy": "template_locked",
                        "playhand_recommended_max_indicators": 2,
                        "playhand_role_balanced_fill_limit": 0,
                        "playhand_mutation_pressure": "low",
                    }
                ],
                "slot_menus": {
                    "guard": [
                        {
                            "indicator_id": "SLOT_ADDON",
                            "sampling_weight": 100,
                            "source": "slot",
                        }
                    ]
                },
            }
        },
    }

    deal = deal_seed_plan_indicators(
        indicators,
        target_count=4,
        seed_plan=seed_plan,
        rng=random.Random(7),
    )

    assert [indicator.id for indicator in deal["indicators"]] == [
        "RSI_CROSSBACK",
        "WILLR_MEAN_REVERSION",
    ]
    assert deal["policy_target_count"] == 2
    assert deal["pair"]["playhand_family_policy"] == "template_locked"
    assert all(slot["slot"] != "role_balanced_fill" for slot in deal["selected_slots"])
    assert all(slot["indicator_id"] != "BAD_FILL" for slot in deal["selected_slots"])


def test_resolve_playhand_family_policy_normalizes_template_locked() -> None:
    policy = play_hand_mod.resolve_playhand_family_policy(
        {
            "family_policy": {
                "family_id": "family-1",
                "family_policy": "template_locked",
                "exact_branch_required": "true",
                "recommended_max_indicators": 2,
                "role_balanced_fill_limit": 0,
                "mutation_pressure": "low",
            }
        }
    )

    assert policy["family_id"] == "family-1"
    assert policy["family_policy"] == "template_locked"
    assert policy["exact_branch_required"] is True
    assert policy["recommended_max_indicators"] == 2
    assert policy["role_balanced_fill_limit"] == 0
    assert policy["source"] == "indicator_deal.family_policy"


def test_resolve_playhand_family_policy_handles_missing_policy() -> None:
    policy = play_hand_mod.resolve_playhand_family_policy({"pair": None})

    assert policy["family_policy"] == "none"
    assert policy["exact_branch_required"] is False
    assert policy["source"] == "indicator_deal.pair"


def test_apply_seed_pair_template_defaults_preserves_validated_pair_config() -> None:
    profile_payload = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "FIRST_A"},
                    "config": {"timeframe": "M30", "lookbackBars": 3, "weight": 0.5},
                },
                {
                    "meta": {"id": "SECOND_A"},
                    "config": {"timeframe": "H1", "lookbackBars": 2},
                },
            ]
        }
    }
    pair = {
        "recommended_profile_template": {
            "probe_id": "drv-test",
            "timeframe": "M5",
            "indicator_defaults": [
                {
                    "indicator_id": "FIRST_A",
                    "timeframe": "M5",
                    "lookbackBars": 1,
                    "weight": 1.0,
                    "ranges": {"buy": [0, 1], "sell": [0, 1]},
                    "talibConfig": [{"name": "timeperiod", "value": 14}],
                }
            ],
        }
    }

    changes = apply_seed_pair_template_defaults(profile_payload, pair)

    config = profile_payload["profile"]["indicators"][0]["config"]
    assert changes[0]["template_probe_id"] == "drv-test"
    assert config["timeframe"] == "M5"
    assert config["lookbackBars"] == 1
    assert config["ranges"]["buy"] == [0, 1]
    assert profile_payload["profile"]["indicators"][1]["config"]["timeframe"] == "H1"


def test_build_early_exit_decision_reports_lookback_drop() -> None:
    decision = build_early_exit_decision(
        checkpoint="after_lookback_top",
        mode="report",
        evidence={
            "source": {
                "dealt_indicator_source": "role_balanced_policy_exploration",
                "family_policy": None,
            },
            "scores": {
                "baseline_3mo": 63.5626,
                "lookback_top_3mo": 46.5363,
                "coarse_top_3mo": None,
                "focused_top_3mo": None,
            },
            "early_exit_inputs": {
                "safe_for_report_mode": True,
                "guided_or_role_balanced": "role_balanced_policy_exploration"
            },
        },
    )

    assert decision["version"] == play_hand_mod.PLAY_HAND_EARLY_EXIT_VERSION
    assert decision["mode"] == "report"
    assert decision["would_exit"] is True
    assert decision["would_exit_research"] is False
    assert decision["would_exit_compute_expansion"] is True
    assert any(
        reason.startswith("lookback_score_below_baseline_by_")
        for reason in decision["reasons"]
    )
    assert decision["rules_fired"]
    assert decision["deltas"]["lookback_delta_vs_baseline"] is None
    assert decision["saved_if_enforced"]["would_skip_coarse"] is True
    assert decision["saved_if_enforced"]["would_skip_final_36mo"] is False


def test_build_early_exit_decision_degrades_without_health_context() -> None:
    decision = build_early_exit_decision(
        checkpoint="after_baseline",
        mode="report",
        evidence={
            "source": {},
            "scores": {"baseline_3mo": 0.0},
            "early_exit_inputs": {},
        },
    )

    assert decision["would_exit"] is False
    assert decision["would_exit_research"] is False
    assert decision["would_exit_compute_expansion"] is False
    assert decision["rules_fired"] == ["insufficient_health_context"]
    assert "insufficient_health_context" in decision["reasons"]


def test_build_early_exit_decision_enforce_actions() -> None:
    baseline = build_early_exit_decision(
        checkpoint="after_baseline",
        mode="enforce",
        evidence={
            "source": {},
            "scores": {"baseline_3mo": 0.0},
            "early_exit_inputs": {"safe_for_report_mode": True},
        },
    )
    assert baseline["enforced"] is True
    assert baseline["terminal"] is True
    assert baseline["enforce_action"] == "early_exit_tombstone"
    assert "baseline_score_not_positive" in baseline["enforce_reasons"]
    assert "lookback_timing" in baseline["skipped_stages"]
    assert "mutated_final_36mo" in baseline["skipped_stages"]

    lookback = build_early_exit_decision(
        checkpoint="after_lookback_top",
        mode="enforce",
        evidence={
            "source": {},
            "scores": {"baseline_3mo": 50.0, "lookback_top_3mo": 0.0},
            "early_exit_inputs": {"safe_for_report_mode": True},
        },
    )
    assert lookback["enforced"] is True
    assert lookback["terminal"] is True
    assert lookback["enforce_action"] == "early_exit_tombstone"
    assert "lookback_score_not_positive_with_weak_baseline" in lookback["enforce_reasons"]

    scout = build_early_exit_decision(
        checkpoint="before_instrument_scout",
        mode="enforce",
        evidence={
            "source": {},
            "scores": {"baseline_3mo": 50.0, "lookback_top_3mo": 44.0},
            "early_exit_inputs": {"safe_for_report_mode": True},
        },
    )
    assert scout["enforced"] is True
    assert scout["terminal"] is False
    assert scout["skip_instrument_scout"] is True
    assert scout["enforce_action"] == "skip_instrument_scout"
    assert scout["skipped_stages"] == ["instrument_scout"]

    continuation = build_early_exit_decision(
        checkpoint="after_baseline",
        mode="enforce",
        evidence={
            "source": {},
            "scores": {"baseline_3mo": 60.0},
            "early_exit_inputs": {"safe_for_report_mode": True},
        },
    )
    assert continuation["enforced"] is False
    assert continuation["terminal"] is False
    assert continuation["enforce_action"] == "continue"


def _play_hand_cmd_defaults(**overrides):
    params = {
        "instrument": ["EURUSD"],
        "instrument_pool": None,
        "timeframe": "M5",
        "sweep_budget": "high",
        "max_sweep_permutations": None,
        "max_reward_r": None,
        "min_indicators": 2,
        "max_indicators": 2,
        "seed": 1,
        "screen_months": 3,
        "scrutiny_months": 36,
        "coarse_mode": "evolutionary",
        "evolutionary_budget": None,
        "instrument_scout": True,
        "instrument_scout_size": 5,
        "instrument_scout_max_selected": 3,
        "instrument_scout_months": None,
        "final_artifacts": False,
        "final_profile_drop_count": 0,
        "final_profile_drop_workers": 1,
        "job_timeout_seconds": 2400,
        "sweep_timeout_seconds": 7200,
        "dry_run": True,
        "as_json": True,
        "calendar_gate": "off",
        "early_exit_mode": "enforce",
        "coarse_halving_mode": "off",
        "family_policy_mode": "off",
        "coarse_probe_budget": 128,
        "resource_trace": False,
    }
    params.update(overrides)
    return params


def test_resource_trace_records_rollup_and_parallel_group(tmp_path: Path) -> None:
    ctx = PlayHandContext(
        config=None,
        cli=None,
        run_id="run-trace",
        run_dir=tmp_path,
        profiles_dir=tmp_path / "profiles",
        evals_dir=tmp_path / "evals",
        attempts_path=tmp_path / "attempts.jsonl",
        events_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
        resource_trace_enabled=True,
        resource_trace_path=tmp_path / "play-hand-resource-trace.jsonl",
        resource_trace_started_at="2026-06-14T00:00:00+00:00",
        resource_trace_base_perf=time.perf_counter(),
    )
    stage = play_hand_mod.PlayHandStage(1, 2, "trace_test")

    with play_hand_mod._resource_span(
        ctx,
        phase="startup",
        operation="single_step",
        execution_kind="single_sync",
        parallel_capability="none",
        stage=stage,
    ):
        time.sleep(0.001)

    with play_hand_mod._resource_span(
        ctx,
        phase="instrument_scout",
        operation="parallel_group",
        execution_kind="local_parallel",
        parallel_capability="local_threads",
        stage=stage,
        worker_count=2,
    ) as group:
        with play_hand_mod._resource_span(
            ctx,
            phase="instrument_scout",
            operation="parallel_worker_eval",
            execution_kind="remote_worker_blocking",
            parallel_capability="fuzzfolio_worker_pool",
            rollup_role="parallel_worker",
            parent_span_id=group.span_id,
            stage=stage,
        ):
            time.sleep(0.001)

    metadata: dict[str, object] = {}
    summary = play_hand_mod._finalize_resource_trace(ctx, metadata)

    assert summary is not None
    assert metadata["resource_trace"] == summary
    assert summary["enabled"] is True
    assert summary["span_count"] == 3
    assert summary["critical_path_span_count"] == 2
    assert summary["single_sync_seconds"] > 0
    assert summary["local_parallel_seconds"] > 0
    assert summary["parallel_capable_seconds"] >= summary["local_parallel_seconds"]
    assert summary["by_execution_kind_seconds"]["single_sync"] > 0
    assert summary["trace_jsonl"] == str(
        (tmp_path / "play-hand-resource-trace.jsonl").resolve()
    )

    trace_lines = [
        json.loads(line)
        for line in (tmp_path / "play-hand-resource-trace.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len(trace_lines) == 3
    assert any(line["rollup_role"] == "parallel_worker" for line in trace_lines)
    assert summary["parallel_groups"][0]["worker_count"] == 2
    assert summary["parallel_groups"][0]["child_span_count"] == 1
    assert summary["parallel_groups"][0]["estimated_worker_utilization"] is not None


def test_coarse_halving_budget_plan_splits_remaining_budget() -> None:
    plan = play_hand_mod.build_coarse_halving_budget_plan(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
    )

    assert plan["split"] is True
    assert plan["probe_budget"] == 128
    assert plan["expand_budget"] == 896

    unsplit = play_hand_mod.build_coarse_halving_budget_plan(
        mode="enforce",
        total_budget=64,
        probe_budget=128,
    )

    assert unsplit["split"] is False
    assert unsplit["probe_budget"] == 64
    assert unsplit["expand_budget"] == 0


def test_build_coarse_halving_decision_thresholds() -> None:
    missing = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=63.0,
        probe_score=None,
    )
    assert missing["expanded"] is False
    assert missing["decision"] == "skip_expansion"
    assert missing["estimated_saved_evaluations"] == 896

    zero = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=63.0,
        probe_score=0.0,
    )
    assert zero["expanded"] is False
    assert zero["decision"] == "skip_expansion"

    strong_probe = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=42.0,
        probe_score=55.0,
    )
    assert strong_probe["expanded"] is True
    assert "probe_score_met_expand_threshold" in strong_probe["reasons"]

    near_incumbent = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=63.0,
        probe_score=58.0,
    )
    assert near_incumbent["expanded"] is True
    assert "probe_score_met_expand_threshold" in near_incumbent["reasons"]

    rejected_probe_against_strong_incumbent = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=75.0,
        probe_score=56.0,
    )
    assert rejected_probe_against_strong_incumbent["expanded"] is False
    assert rejected_probe_against_strong_incumbent["decision"] == "skip_expansion"
    assert "probe_not_near_strong_incumbent" in rejected_probe_against_strong_incumbent["reasons"]

    weak_near_incumbent = play_hand_mod.build_coarse_halving_decision(
        mode="enforce",
        total_budget=1024,
        probe_budget=128,
        incumbent_score=63.0,
        probe_score=49.0,
    )
    assert weak_near_incumbent["expanded"] is False
    assert "probe_score_below_expand_threshold" in weak_near_incumbent["reasons"]


def test_stage_acceptance_rejects_material_drop_and_accepts_near_or_better() -> None:
    rejected = play_hand_mod.build_stage_acceptance_decision(
        stage="lookback",
        incumbent_score=63.0,
        candidate_score=46.0,
    )
    near = play_hand_mod.build_stage_acceptance_decision(
        stage="lookback",
        incumbent_score=63.0,
        candidate_score=58.0,
    )
    better = play_hand_mod.build_stage_acceptance_decision(
        stage="lookback",
        incumbent_score=63.0,
        candidate_score=64.0,
    )

    assert rejected["accepted"] is False
    assert rejected["reason"] == "candidate_below_incumbent_tolerance"
    assert near["accepted"] is True
    assert near["reason"] == "candidate_within_incumbent_tolerance"
    assert better["accepted"] is True
    assert better["reason"] == "candidate_improved_incumbent"


def test_cmd_play_hand_authenticates_before_seed_prompt(monkeypatch, tmp_path: Path) -> None:
    events: list[str] = []

    class FakeCli:
        def ensure_login(self) -> None:
            events.append("ensure_login")

    def fake_seed_hand(_config, _cli, _run_dir):
        events.append("seed_hand")
        raise RuntimeError("stop after auth")

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            derived_root=tmp_path / "derived",
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_seed_hand", fake_seed_hand)

    with pytest.raises(RuntimeError, match="stop after auth"):
        play_hand_mod.cmd_play_hand(
            instrument=None,
            instrument_pool=None,
            timeframe="M5",
            sweep_budget=None,
            max_sweep_permutations=None,
            max_reward_r=None,
            min_indicators=2,
            max_indicators=4,
            seed=1,
            screen_months=3,
            scrutiny_months=12,
            coarse_mode="grid",
            evolutionary_budget=None,
            instrument_scout=False,
            instrument_scout_size=0,
            instrument_scout_max_selected=0,
            instrument_scout_months=None,
            final_artifacts=False,
            final_profile_drop_count=0,
            final_profile_drop_workers=1,
            job_timeout_seconds=2400,
            sweep_timeout_seconds=7200,
            dry_run=False,
            as_json=True,
        )

    assert events == ["ensure_login", "seed_hand"]


def test_cmd_play_hand_early_exit_enforce_tombstones_after_baseline(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        if phase != "baseline_3mo":
            raise AssertionError(f"{phase} should be skipped by early exit")
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": 0.0,
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(runs_root=tmp_path, fuzzfolio=SimpleNamespace()),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_load_play_hand_seed_plan", lambda _config: (None, None))
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(
        play_hand_mod,
        "build_timing_axes",
        lambda _payload: (_ for _ in ()).throw(AssertionError("lookback should be skipped")),
    )
    monkeypatch.setattr(
        play_hand_mod,
        "_run_sweep",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sweeps should be skipped")),
    )
    monkeypatch.setattr(
        play_hand_mod,
        "_run_instrument_scout",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("scout should be skipped")),
    )

    exit_code = play_hand_mod.cmd_play_hand(**_play_hand_cmd_defaults(final_artifacts=True))

    assert exit_code == 0
    assert eval_calls == ["baseline_3mo"]
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    assert metadata["run_status"] == "tombstoned"
    assert metadata["tombstone_reason"] == play_hand_mod.PLAY_HAND_EARLY_EXIT_TOMBSTONE_REASON
    assert metadata["selected_final_branch"] == "early_exit"
    assert metadata["final_artifacts"]["status"] == "skipped"
    decision = metadata["early_exit_policy"]["decisions"][-1]
    assert decision["checkpoint"] == "after_baseline"
    assert decision["enforced"] is True
    assert decision["terminal"] is True
    assert "baseline_score_not_positive" in decision["enforce_reasons"]
    assert metadata["play_hand_health"]["status"] == "tombstoned"


def test_cmd_play_hand_early_exit_enforce_tombstones_after_weak_lookback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        if phase != "lookback_timing":
            raise AssertionError(f"{phase} should be skipped by early exit")
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": 0.0}
                ],
                "parameter_importance": [],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 50.0,
            "lookback_timing_top_3mo": 0.0,
        }
        if phase not in scores:
            raise AssertionError(f"{phase} should be skipped by early exit")
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(runs_root=tmp_path, fuzzfolio=SimpleNamespace()),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_load_play_hand_seed_plan", lambda _config: (None, None))
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(
        play_hand_mod,
        "build_coarse_axes",
        lambda _payload: (_ for _ in ()).throw(AssertionError("coarse should be skipped")),
    )
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)

    exit_code = play_hand_mod.cmd_play_hand(**_play_hand_cmd_defaults(seed=2))

    assert exit_code == 0
    assert eval_calls == ["baseline_3mo", "lookback_timing_top_3mo"]
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    decision = metadata["early_exit_policy"]["decisions"][-1]
    assert decision["checkpoint"] == "after_lookback_top"
    assert decision["enforced"] is True
    assert decision["terminal"] is True
    assert "lookback_score_not_positive_with_weak_baseline" in decision["enforce_reasons"]
    assert "coarse_probe" in decision["skipped_stages"]
    assert metadata["run_status"] == "tombstoned"


def test_cmd_play_hand_early_exit_enforce_skips_scout_but_keeps_final_scrutiny(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": 44.0}
                ],
                "parameter_importance": [],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 50.0,
            "lookback_timing_top_3mo": 44.0,
            "mutated_final_36mo": 60.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(runs_root=tmp_path, fuzzfolio=SimpleNamespace()),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_load_play_hand_seed_plan", lambda _config: (None, None))
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(play_hand_mod, "build_coarse_axes", lambda _payload: [])
    monkeypatch.setattr(play_hand_mod, "build_focused_axes", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(
        play_hand_mod,
        "_run_instrument_scout",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("scout should be skipped")),
    )

    exit_code = play_hand_mod.cmd_play_hand(**_play_hand_cmd_defaults(seed=3))

    assert exit_code == 0
    assert "mutated_final_36mo" in eval_calls
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    decision = [
        item
        for item in metadata["early_exit_policy"]["decisions"]
        if item["checkpoint"] == "before_instrument_scout"
    ][0]
    assert decision["enforced"] is True
    assert decision["skip_instrument_scout"] is True
    assert decision["terminal"] is False
    assert metadata["instrument_scout"]["status"] == "skipped"
    assert metadata["instrument_scout"]["reason"] == "early_exit_policy_skip_instrument_scout"
    assert metadata["run_status"] == "promoted"
    assert metadata["final_scrutiny_score"] == 60.0


def test_cmd_play_hand_coarse_halving_no_expand_skips_expensive_stages(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sweep_calls: list[dict[str, object]] = []
    eval_calls: list[dict[str, object]] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        if phase in {"coarse_expand", "focused"}:
            raise AssertionError(f"{phase} should be skipped")
        sweep_calls.append(
            {
                "phase": phase,
                "mode": mode,
                "sweep_budget": sweep_budget,
                "max_permutations": max_permutations,
            }
        )
        top_score = 46.0 if phase == "coarse_probe" else 50.0
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": top_score}
                ],
                "parameter_importance": [
                    {"axis": "indicator[0].talib.timeperiod", "importance": 1.0}
                ],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(
            {
                "phase": phase,
                "profile_ref": profile_ref,
                "lookback_months": lookback_months,
            }
        )
        scores = {
            "baseline_3mo": 63.5626,
            "lookback_timing_top_3mo": 46.5363,
            "coarse_probe_top_3mo": 46.2,
            "mutated_final_36mo": 12.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    def fake_instrument_scout(*_args, **_kwargs):
        raise AssertionError("instrument scout should be skipped")

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_load_play_hand_seed_plan", lambda _config: (None, None))
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(
        play_hand_mod,
        "build_coarse_axes",
        lambda _payload: ["indicator[0].talib.timeperiod=5,14"],
    )
    monkeypatch.setattr(
        play_hand_mod,
        "build_focused_axes",
        lambda *_args, **_kwargs: ["focused=1,2"],
    )
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(play_hand_mod, "_run_instrument_scout", fake_instrument_scout)

    exit_code = play_hand_mod.cmd_play_hand(
        instrument=["EURUSD"],
        instrument_pool=None,
        timeframe="M5",
        sweep_budget="high",
        max_sweep_permutations=None,
        max_reward_r=None,
        min_indicators=2,
        max_indicators=2,
        seed=1,
        screen_months=3,
        scrutiny_months=36,
        coarse_mode="evolutionary",
        evolutionary_budget=None,
        instrument_scout=True,
        instrument_scout_size=5,
        instrument_scout_max_selected=3,
        instrument_scout_months=None,
        final_artifacts=False,
        final_profile_drop_count=0,
        final_profile_drop_workers=1,
        job_timeout_seconds=2400,
        sweep_timeout_seconds=7200,
        dry_run=True,
        as_json=True,
        calendar_gate="off",
        early_exit_mode="report",
        coarse_halving_mode="enforce",
        coarse_probe_budget=128,
    )

    assert exit_code == 0
    assert [call["phase"] for call in sweep_calls] == ["lookback_timing", "coarse_probe"]
    coarse_probe_call = sweep_calls[1]
    assert coarse_probe_call["sweep_budget"] == "128"
    assert coarse_probe_call["max_permutations"] == 128
    assert [call["phase"] for call in eval_calls] == [
        "baseline_3mo",
        "lookback_timing_top_3mo",
        "coarse_probe_top_3mo",
        "mutated_final_36mo",
    ]
    assert eval_calls[-1]["profile_ref"] == "dry-hand_base"

    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    halving = metadata["coarse_halving"]
    assert halving["version"] == play_hand_mod.PLAY_HAND_COARSE_HALVING_VERSION
    assert halving["mode"] == "enforce"
    assert halving["decision"] == "skip_expansion"
    assert halving["expanded"] is False
    assert halving["estimated_saved_evaluations"] == 896
    assert halving["skipped_stages"] == ["coarse_expand", "focused", "instrument_scout"]
    assert metadata["instrument_scout"]["status"] == "skipped"
    assert metadata["instrument_scout"]["reason"] == "coarse_halving_skip_expansion"
    assert metadata["play_hand_phase_scores"]["coarse_probe_top_3mo"] == 46.2
    assert metadata["play_hand_phase_scores"]["coarse_top_3mo"] == 63.5626
    assert metadata["stage_incumbent"]["profile_ref"] == "dry-hand_base"
    assert [item["accepted"] for item in metadata["stage_acceptance_decisions"]] == [
        False,
        False,
    ]
    assert "play_hand_health" in metadata


def test_cmd_play_hand_coarse_halving_expand_runs_remaining_work(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sweep_calls: list[dict[str, object]] = []
    eval_calls: list[dict[str, object]] = []
    scout_calls: list[dict[str, object]] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        sweep_calls.append(
            {
                "phase": phase,
                "mode": mode,
                "sweep_budget": sweep_budget,
                "max_permutations": max_permutations,
            }
        )
        top_scores = {
            "lookback_timing": 60.5,
            "coarse_probe": 56.0,
            "coarse_expand": 66.0,
            "focused": 67.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": top_scores[phase]}
                ],
                "parameter_importance": [
                    {"axis": "indicator[0].talib.timeperiod", "importance": 1.0}
                ],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append({"phase": phase, "profile_ref": profile_ref})
        scores = {
            "baseline_3mo": 60.0,
            "lookback_timing_top_3mo": 60.5,
            "coarse_probe_top_3mo": 56.0,
            "coarse_expand_top_3mo": 66.0,
            "focused_top_3mo": 67.0,
            "mutated_final_36mo": 15.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    def fake_instrument_scout(
        ctx,
        *,
        stage,
        profile_ref,
        profile_path,
        instrument_deal,
        instruments,
        timeframe,
        lookback_months,
        rng,
        enabled,
        scout_size,
        max_selected,
        reward_matrix=None,
        as_of_date=None,
    ):
        scout_calls.append({"profile_ref": profile_ref, "enabled": enabled})
        return {
            "version": "instrument_scout_v1",
            "status": "completed",
            "selected_instruments": list(instruments),
            "accepted": [],
            "rejected": [],
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(play_hand_mod, "_load_play_hand_seed_plan", lambda _config: (None, None))
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(
        play_hand_mod,
        "build_coarse_axes",
        lambda _payload: ["indicator[0].talib.timeperiod=5,14"],
    )
    monkeypatch.setattr(
        play_hand_mod,
        "build_focused_axes",
        lambda *_args, **_kwargs: ["indicator[0].talib.timeperiod=8,14"],
    )
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(play_hand_mod, "_run_instrument_scout", fake_instrument_scout)

    exit_code = play_hand_mod.cmd_play_hand(
        instrument=["EURUSD"],
        instrument_pool=None,
        timeframe="M5",
        sweep_budget="high",
        max_sweep_permutations=None,
        max_reward_r=None,
        min_indicators=2,
        max_indicators=2,
        seed=2,
        screen_months=3,
        scrutiny_months=36,
        coarse_mode="evolutionary",
        evolutionary_budget=None,
        instrument_scout=True,
        instrument_scout_size=5,
        instrument_scout_max_selected=3,
        instrument_scout_months=None,
        final_artifacts=False,
        final_profile_drop_count=0,
        final_profile_drop_workers=1,
        job_timeout_seconds=2400,
        sweep_timeout_seconds=7200,
        dry_run=True,
        as_json=True,
        calendar_gate="off",
        early_exit_mode="report",
        coarse_halving_mode="enforce",
        coarse_probe_budget=128,
    )

    assert exit_code == 0
    assert [call["phase"] for call in sweep_calls] == [
        "lookback_timing",
        "coarse_probe",
        "coarse_expand",
        "focused",
    ]
    assert sweep_calls[1]["sweep_budget"] == "128"
    assert sweep_calls[1]["max_permutations"] == 128
    assert sweep_calls[2]["sweep_budget"] == "896"
    assert sweep_calls[2]["max_permutations"] == 896
    assert [call["phase"] for call in eval_calls] == [
        "baseline_3mo",
        "lookback_timing_top_3mo",
        "coarse_probe_top_3mo",
        "coarse_expand_top_3mo",
        "focused_top_3mo",
        "mutated_final_36mo",
    ]
    assert eval_calls[-1]["profile_ref"] == "dry-focused_top"
    assert scout_calls == [{"profile_ref": "dry-focused_top", "enabled": True}]

    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    halving = metadata["coarse_halving"]
    assert halving["decision"] == "expand"
    assert halving["expanded"] is True
    assert halving["expand_budget"] == 896
    assert halving["estimated_saved_evaluations"] == 0
    assert metadata["play_hand_phase_scores"]["coarse_probe_top_3mo"] == 56.0
    assert metadata["play_hand_phase_scores"]["coarse_expand_top_3mo"] == 66.0
    assert metadata["play_hand_phase_scores"]["focused_top_3mo"] == 67.0
    assert metadata["stage_incumbent"]["profile_ref"] == "dry-focused_top"
    assert all(item["accepted"] for item in metadata["stage_acceptance_decisions"])


def test_cmd_play_hand_family_policy_template_locked_enforce_skips_mutation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 48.0,
            "exact_template_screen_3mo": 62.0,
            "exact_template_36mo": 64.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    def fail_sweep(*_args, **_kwargs):
        raise AssertionError("mutation sweeps should be skipped")

    def fail_scout(*_args, **_kwargs):
        raise AssertionError("instrument scout should be skipped")

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            derived_root=tmp_path / "derived",
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(
        play_hand_mod,
        "_load_play_hand_seed_plan",
        lambda _config: (_family_policy_seed_plan("template_locked"), tmp_path / "seed.json"),
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fail_sweep)
    monkeypatch.setattr(play_hand_mod, "_run_instrument_scout", fail_scout)

    exit_code = play_hand_mod.cmd_play_hand(
        instrument=["EURUSD"],
        instrument_pool=None,
        timeframe="M5",
        sweep_budget="high",
        max_sweep_permutations=None,
        max_reward_r=None,
        min_indicators=2,
        max_indicators=2,
        seed=3,
        screen_months=3,
        scrutiny_months=36,
        coarse_mode="evolutionary",
        evolutionary_budget=None,
        instrument_scout=True,
        instrument_scout_size=5,
        instrument_scout_max_selected=3,
        instrument_scout_months=None,
        final_artifacts=False,
        final_profile_drop_count=0,
        final_profile_drop_workers=1,
        job_timeout_seconds=2400,
        sweep_timeout_seconds=7200,
        dry_run=True,
        as_json=True,
        calendar_gate="off",
        family_policy_mode="enforce",
    )

    assert exit_code == 0
    assert eval_calls == [
        "baseline_3mo",
        "exact_template_screen_3mo",
        "exact_template_36mo",
    ]
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    policy = metadata["family_policy_execution"]
    assert policy["version"] == play_hand_mod.PLAY_HAND_FAMILY_POLICY_EXECUTION_VERSION
    assert policy["mode"] == "enforce"
    assert policy["family_policy"] == "template_locked"
    assert policy["decision"] == "template_locked_exact_only"
    assert policy["mutation_allowed"] is False
    assert policy["exact_template_used_as_incumbent"] is True
    assert policy["skipped_stages"] == [
        "lookback_timing",
        "coarse_probe",
        "coarse_expand",
        "focused",
        "instrument_scout",
        "mutated_final_36mo",
    ]
    assert metadata["instrument_scout"]["reason"] == "family_policy_template_locked_exact_only"
    assert metadata["mutated_attempt_id"] is None
    assert metadata["selected_final_branch"] == "exact_template"
    assert metadata["canonical_selection_reason"] == "template_locked_exact_only"
    assert metadata["exact_template_score"] == 64.0


def test_cmd_play_hand_family_policy_template_guarded_enforce_benchmarks_exact(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sweep_calls: list[str] = []
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        sweep_calls.append(phase)
        score = 56.0 if phase == "coarse_probe" else 66.0
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": score}
                ],
                "parameter_importance": [
                    {"axis": "indicator[0].talib.timeperiod", "importance": 1.0}
                ],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 58.0,
            "exact_template_screen_3mo": 62.0,
            "lookback_timing_top_3mo": 62.5,
            "coarse_probe_top_3mo": 64.0,
            "coarse_expand_top_3mo": 66.0,
            "focused_top_3mo": 67.0,
            "mutated_final_36mo": 70.0,
            "exact_template_36mo": 62.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    def fake_instrument_scout(*_args, **_kwargs):
        return {
            "version": "instrument_scout_v1",
            "status": "completed",
            "selected_instruments": ["EURUSD"],
            "accepted": [],
            "rejected": [],
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            derived_root=tmp_path / "derived",
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(
        play_hand_mod,
        "_load_play_hand_seed_plan",
        lambda _config: (_family_policy_seed_plan("template_guarded"), tmp_path / "seed.json"),
    )
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(
        play_hand_mod,
        "build_coarse_axes",
        lambda _payload: ["indicator[0].talib.timeperiod=5,14"],
    )
    monkeypatch.setattr(
        play_hand_mod,
        "build_focused_axes",
        lambda *_args, **_kwargs: ["indicator[0].talib.timeperiod=8,14"],
    )
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(play_hand_mod, "_run_instrument_scout", fake_instrument_scout)

    exit_code = play_hand_mod.cmd_play_hand(
        instrument=["EURUSD"],
        instrument_pool=None,
        timeframe="M5",
        sweep_budget="high",
        max_sweep_permutations=None,
        max_reward_r=None,
        min_indicators=2,
        max_indicators=2,
        seed=4,
        screen_months=3,
        scrutiny_months=36,
        coarse_mode="evolutionary",
        evolutionary_budget=None,
        instrument_scout=True,
        instrument_scout_size=5,
        instrument_scout_max_selected=3,
        instrument_scout_months=None,
        final_artifacts=False,
        final_profile_drop_count=0,
        final_profile_drop_workers=1,
        job_timeout_seconds=2400,
        sweep_timeout_seconds=7200,
        dry_run=True,
        as_json=True,
        calendar_gate="off",
        coarse_halving_mode="enforce",
        family_policy_mode="enforce",
    )

    assert exit_code == 0
    assert eval_calls == [
        "baseline_3mo",
        "exact_template_screen_3mo",
        "lookback_timing_top_3mo",
        "coarse_probe_top_3mo",
        "coarse_expand_top_3mo",
        "focused_top_3mo",
        "mutated_final_36mo",
        "exact_template_36mo",
    ]
    assert sweep_calls == [
        "lookback_timing",
        "coarse_probe",
        "coarse_expand",
        "focused",
    ]
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    policy = metadata["family_policy_execution"]
    assert policy["decision"] == "template_guarded_exact_benchmark_mutation_allowed"
    assert policy["mutation_allowed"] is True
    assert policy["exact_template_used_as_incumbent"] is True
    assert metadata["stage_acceptance_decisions"][0]["stage"] == "exact_template_screen"
    assert metadata["stage_acceptance_decisions"][0]["accepted"] is True
    assert metadata["selected_final_branch"] == "mutated"
    assert {row["branch"] for row in metadata["final_branch_scores"]} == {
        "mutated",
        "exact_template",
    }


def test_cmd_play_hand_early_exit_enforce_suppresses_terminal_when_exact_template_available(
    monkeypatch,
    tmp_path: Path,
) -> None:
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        if phase != "lookback_timing":
            raise AssertionError(f"{phase} should be the only mutation sweep")
        assert all("indicator[2]" not in str(axis) for axis in axes)
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": 0.0}
                ],
                "parameter_importance": [],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 0.0,
            "exact_template_screen_3mo": 65.0,
            "lookback_timing_top_3mo": 0.0,
            "mutated_final_36mo": 20.0,
            "exact_template_36mo": 66.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            derived_root=tmp_path / "derived",
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(
        play_hand_mod,
        "_load_play_hand_seed_plan",
        lambda _config: (_family_policy_seed_plan("template_guarded"), tmp_path / "seed.json"),
    )
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(play_hand_mod, "build_coarse_axes", lambda _payload: [])
    monkeypatch.setattr(play_hand_mod, "build_focused_axes", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(
        play_hand_mod,
        "_run_instrument_scout",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("scout should be skipped")),
    )

    exit_code = play_hand_mod.cmd_play_hand(
        **_play_hand_cmd_defaults(
            seed=5,
            min_indicators=3,
            max_indicators=3,
            early_exit_mode="enforce",
            family_policy_mode="enforce",
            instrument_scout=True,
        )
    )

    assert exit_code == 0
    assert "exact_template_36mo" in eval_calls
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    assert len(run_dirs) == 1
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    decisions = {
        item["checkpoint"]: item
        for item in metadata["early_exit_policy"]["decisions"]
    }
    assert decisions["after_baseline"]["enforcement_suppressed"] is True
    assert decisions["after_baseline"]["enforced"] is False
    assert decisions["after_baseline"]["terminal"] is False
    assert decisions["after_lookback_top"]["enforcement_suppressed"] is True
    assert decisions["after_lookback_top"]["enforced"] is False
    assert decisions["after_lookback_top"]["terminal"] is False
    assert metadata["run_status"] == "promoted"
    assert metadata["selected_final_branch"] == "exact_template"
    assert metadata["exact_template_score"] == 66.0


def test_cmd_play_hand_family_policy_report_keeps_mutation_flow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sweep_calls: list[str] = []
    eval_calls: list[str] = []

    class FakeCli:
        pass

    def fake_run_sweep(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_payload,
        instruments,
        axes,
        mode,
        sweep_budget,
        max_permutations,
        reward_matrix=None,
        as_of_date=None,
    ):
        sweep_calls.append(phase)
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "axes": list(axes),
            "result": {
                "ranked_permutations": [
                    {"parameters": {"fake": phase}, "fitness_value": 66.0}
                ],
                "parameter_importance": [],
            },
        }

    def fake_materialize(ctx, *, stage, source_profile_path, sweep_payload, phase):
        output_path = ctx.profiles_dir / f"{phase}_top.json"
        output_path.write_text(
            source_profile_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return output_path, f"dry-{phase}_top", {"fake": phase}

    def fake_evaluate_profile(
        ctx,
        *,
        stage,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
    ):
        eval_calls.append(phase)
        scores = {
            "baseline_3mo": 50.0,
            "exact_template_screen_3mo": 62.0,
            "lookback_timing_top_3mo": 55.0,
            "coarse_top_3mo": 58.0,
            "mutated_final_36mo": 60.0,
            "exact_template_36mo": 62.0,
        }
        return {
            "artifact_dir": str(ctx.evals_dir / phase),
            "attempt_id": f"attempt-{phase}",
            "score": scores[phase],
            "profile_ref": profile_ref,
            "profile_path": str(profile_path),
        }

    monkeypatch.setattr(
        play_hand_mod,
        "load_config",
        lambda: SimpleNamespace(
            runs_root=tmp_path,
            derived_root=tmp_path / "derived",
            fuzzfolio=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(play_hand_mod, "FuzzfolioCli", lambda _config: FakeCli())
    monkeypatch.setattr(
        play_hand_mod,
        "_load_play_hand_seed_plan",
        lambda _config: (_family_policy_seed_plan("template_locked"), tmp_path / "seed.json"),
    )
    monkeypatch.setattr(play_hand_mod, "build_timing_axes", lambda _payload: ["timing=1,2"])
    monkeypatch.setattr(
        play_hand_mod,
        "build_coarse_axes",
        lambda _payload: ["indicator[0].talib.timeperiod=5,14"],
    )
    monkeypatch.setattr(play_hand_mod, "build_focused_axes", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(play_hand_mod, "_run_sweep", fake_run_sweep)
    monkeypatch.setattr(
        play_hand_mod,
        "_materialize_and_register_best_sweep_candidate",
        fake_materialize,
    )
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(
        play_hand_mod,
        "_run_instrument_scout",
        lambda *_args, **_kwargs: {
            "version": "instrument_scout_v1",
            "status": "disabled",
            "selected_instruments": ["EURUSD"],
            "accepted": [],
            "rejected": [],
        },
    )

    exit_code = play_hand_mod.cmd_play_hand(
        instrument=["EURUSD"],
        instrument_pool=None,
        timeframe="M5",
        sweep_budget="low",
        max_sweep_permutations=None,
        max_reward_r=None,
        min_indicators=2,
        max_indicators=2,
        seed=5,
        screen_months=3,
        scrutiny_months=36,
        coarse_mode="grid",
        evolutionary_budget=None,
        instrument_scout=False,
        instrument_scout_size=0,
        instrument_scout_max_selected=0,
        instrument_scout_months=None,
        final_artifacts=False,
        final_profile_drop_count=0,
        final_profile_drop_workers=1,
        job_timeout_seconds=2400,
        sweep_timeout_seconds=7200,
        dry_run=True,
        as_json=True,
        calendar_gate="off",
        family_policy_mode="report",
    )

    assert exit_code == 0
    assert "exact_template_screen_3mo" in eval_calls
    assert "mutated_final_36mo" in eval_calls
    assert sweep_calls == ["lookback_timing", "coarse"]
    run_dirs = list(tmp_path.glob("*-playhand-v1"))
    metadata = json.loads((run_dirs[0] / "run-metadata.json").read_text(encoding="utf-8"))
    policy = metadata["family_policy_execution"]
    assert policy["mode"] == "report"
    assert policy["decision"] == "would_template_locked_exact_only"
    assert policy["mutation_allowed"] is True
    assert policy["would_skip_stages"] == [
        "lookback_timing",
        "coarse_probe",
        "coarse_expand",
        "focused",
        "instrument_scout",
        "mutated_final_36mo",
    ]
    assert metadata["mutated_attempt_id"] == "attempt-mutated_final_36mo"


def test_cleanup_registered_profiles_deletes_unique_cloud_refs(tmp_path: Path) -> None:
    deleted: list[str] = []

    class FakeCli:
        def run(self, args, *, check=True):
            assert args[:3] == ["profiles", "delete", "--profile-ref"]
            deleted.append(args[3])
            return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    ctx = SimpleNamespace(
        cli=FakeCli(),
        registered_profile_refs=["prof-a", "prof-b", "prof-a"],
        run_id="run-1",
        events_path=tmp_path / "events.jsonl",
        io_lock=threading.RLock(),
    )

    summary = _cleanup_registered_profiles(
        ctx,
        keep_cloud_profiles=False,
        reason="completed",
    )

    assert deleted == ["prof-a", "prof-b"]
    assert summary["status"] == "completed"
    assert summary["attempted_count"] == 2
    assert summary["deleted_count"] == 2
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert events[-1]["phase"] == "cloud_profile_cleanup"
    assert events[-1]["deleted_count"] == 2


def test_play_hand_artifact_commands_heal_full_backtests_and_top_drop() -> None:
    commands = _play_hand_artifact_commands(
        run_id="run-123",
        profile_drop_count=1,
        profile_drop_workers=2,
    )

    assert len(commands) == 1
    drop_command = commands[0]
    assert drop_command[0] == sys.executable
    assert drop_command[1] == "-c"
    assert "main(sys.argv[1:])" in drop_command[2]
    for expected in (
        "finalize-corpus",
        "--run-id",
        "run-123",
        "--scope",
        "dashboard",
        "--lookback-months",
        "36",
        "--profile-drop-workers",
        "2",
        "--json",
    ):
        assert expected in drop_command


def test_play_hand_artifact_commands_can_skip_profile_drops() -> None:
    commands = _play_hand_artifact_commands(
        run_id="run-123",
        profile_drop_count=0,
        profile_drop_workers=1,
    )

    assert commands == []


def test_play_hand_artifact_commands_target_final_attempt_for_profile_drop() -> None:
    commands = _play_hand_artifact_commands(
        run_id="run-123",
        profile_drop_count=1,
        profile_drop_workers=1,
        final_attempt_id="run-123-attempt-00011",
    )

    drop_command = commands[0]
    assert "--attempt-id" in drop_command
    assert "run-123-attempt-00011" in drop_command
    assert "--scope" not in drop_command


def test_finalize_play_hand_attempt_metadata_marks_canonical_and_scout_decisions(tmp_path: Path) -> None:
    attempts_path = tmp_path / "attempts.jsonl"
    write_attempts(
        attempts_path,
        [
            {
                "attempt_id": "run-1-attempt-00001",
                "run_id": "run-1",
                "candidate_name": "baseline_3mo",
            },
            {
                "attempt_id": "run-1-attempt-00002",
                "run_id": "run-1",
                "candidate_name": "instrument_scout_USDJPY_3mo",
            },
            {
                "attempt_id": "run-1-attempt-00003",
                "run_id": "run-1",
                "candidate_name": "instrument_scout_USDCHF_3mo",
            },
            {
                "attempt_id": "run-1-attempt-00004",
                "run_id": "run-1",
                "candidate_name": "final_36mo",
            },
        ],
    )
    ctx = PlayHandContext(
        config=None,
        cli=None,
        run_id="run-1",
        run_dir=tmp_path,
        profiles_dir=tmp_path / "profiles",
        evals_dir=tmp_path / "evals",
        attempts_path=attempts_path,
        events_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
    )

    reward_matrix = play_hand_reward_matrix(4)
    summary = _finalize_play_hand_attempt_metadata(
        ctx,
        final_attempt_id="run-1-attempt-00004",
        scout_result={
            "primary": {
                "attempt_id": "run-1-attempt-00002",
                "instrument": "USDJPY",
            },
            "accepted": [],
            "rejected": [
                {
                    "attempt_id": "run-1-attempt-00003",
                    "instrument": "USDCHF",
                    "decision_reasons": ["too_similar_to_selected"],
                }
            ],
        },
        selected_instruments=["XAUUSD", "USDJPY"],
        reward_matrix=reward_matrix,
    )

    attempts = {row["attempt_id"]: row for row in load_attempts(attempts_path)}
    assert summary["updated_count"] == 4
    assert attempts["run-1-attempt-00001"]["attempt_decision"] == "intermediate"
    assert attempts["run-1-attempt-00002"]["attempt_decision"] == "accepted"
    assert attempts["run-1-attempt-00003"]["attempt_decision"] == "rejected"
    assert attempts["run-1-attempt-00003"]["attempt_decision_reasons"] == [
        "too_similar_to_selected"
    ]
    final_attempt = attempts["run-1-attempt-00004"]
    assert final_attempt["is_canonical_playhand_attempt"] is True
    assert final_attempt["attempt_role"] == "final"
    assert final_attempt["attempt_decision"] == "canonical"
    assert final_attempt["canonical_attempt_id"] == "run-1-attempt-00004"
    assert final_attempt["play_hand_selected_instruments"] == ["XAUUSD", "USDJPY"]
    assert final_attempt["reward_step_r"] == 0.5
    assert final_attempt["reward_columns"] == 8
    assert final_attempt["effective_max_reward_r"] == 4.0


def test_finalize_play_hand_attempt_metadata_tombstones_failed_final_scrutiny(
    tmp_path: Path,
) -> None:
    attempts_path = tmp_path / "attempts.jsonl"
    write_attempts(
        attempts_path,
        [
            {
                "attempt_id": "run-1-attempt-00001",
                "run_id": "run-1",
                "candidate_name": "focused_top_3mo",
            },
            {
                "attempt_id": "run-1-attempt-00002",
                "run_id": "run-1",
                "candidate_name": "final_36mo",
            },
        ],
    )
    ctx = PlayHandContext(
        config=None,
        cli=None,
        run_id="run-1",
        run_dir=tmp_path,
        profiles_dir=tmp_path / "profiles",
        evals_dir=tmp_path / "evals",
        attempts_path=attempts_path,
        events_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
    )

    summary = _finalize_play_hand_attempt_metadata(
        ctx,
        final_attempt_id="run-1-attempt-00002",
        scout_result=None,
        selected_instruments=["XAUUSD"],
        final_scrutiny_passed=False,
        final_scrutiny_score=0.0,
        tombstone_reason="final_36mo_score_not_positive",
        tombstone_reasons=[
            "final_36mo_scrutiny_failed",
            "final_36mo_score_not_positive",
        ],
    )

    attempts = {row["attempt_id"]: row for row in load_attempts(attempts_path)}
    assert summary["run_tombstoned"] is True
    assert summary["canonical_attempt_id"] is None
    assert attempts["run-1-attempt-00001"]["run_tombstoned"] is True
    final_attempt = attempts["run-1-attempt-00002"]
    assert final_attempt["attempt_decision"] == "tombstoned"
    assert final_attempt["attempt_tombstoned"] is True
    assert final_attempt["is_canonical_attempt"] is False
    assert final_attempt["canonical_attempt_id"] is None
    assert final_attempt["attempt_decision_reasons"] == [
        "final_36mo_scrutiny_failed",
        "final_36mo_score_not_positive",
    ]


def test_play_hand_reward_matrix_caps_default_reward_grid() -> None:
    matrix = play_hand_reward_matrix(4.25)

    assert matrix is not None
    assert matrix["requested_max_reward_r"] == 4.25
    assert matrix["reward_step_r"] == 0.5
    assert matrix["reward_columns"] == 8
    assert matrix["effective_max_reward_r"] == 4.0
    assert matrix["is_active_cap"] is True
    assert _reward_matrix_cli_args(matrix) == [
        "--reward-step-r",
        "0.5",
        "--reward-columns",
        "8",
    ]


def test_play_hand_reward_matrix_defaults_to_four_r() -> None:
    matrix = play_hand_reward_matrix(None)

    assert matrix is not None
    assert matrix["requested_max_reward_r"] == 4.0
    assert matrix["reward_columns"] == 8
    assert matrix["effective_max_reward_r"] == 4.0
    assert matrix["default_max_reward_r"] == 4.0
    assert matrix["hard_max_reward_r"] == 12.5
    assert matrix["is_default_cap"] is True
    assert matrix["is_active_cap"] is True


def test_play_hand_reward_matrix_keeps_hard_ceiling_for_large_caps() -> None:
    matrix = play_hand_reward_matrix(20)

    assert matrix is not None
    assert matrix["reward_columns"] == 25
    assert matrix["effective_max_reward_r"] == 12.5
    assert matrix["default_max_reward_r"] == 4.0
    assert matrix["hard_max_reward_r"] == 12.5
    assert matrix["is_default_cap"] is False
    assert matrix["is_active_cap"] is False


def test_play_hand_reward_matrix_rejects_unrepresentable_caps() -> None:
    with pytest.raises(ValueError, match="at least 0.5"):
        play_hand_reward_matrix(0.25)


def test_instrument_scout_worker_count_defaults_and_env(monkeypatch) -> None:
    monkeypatch.delenv("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS", raising=False)
    assert _instrument_scout_worker_count(0) == 0
    assert _instrument_scout_worker_count(1) == 1
    assert _instrument_scout_worker_count(5) == 4

    monkeypatch.setenv("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS", "2")
    assert _instrument_scout_worker_count(5) == 2

    monkeypatch.setenv("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS", "200")
    assert _instrument_scout_worker_count(12) == 8

    monkeypatch.setenv("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS", "nope")
    assert _instrument_scout_worker_count(5) == 4


def test_evaluate_instrument_scout_records_parallelizes_evaluations(
    monkeypatch,
    tmp_path: Path,
) -> None:
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_evaluate_profile(*_args, instruments, **_kwargs):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            instrument = instruments[0]
            return {
                "artifact_dir": str(tmp_path / instrument),
                "attempt_id": f"attempt-{instrument}",
                "score": float(len(instrument)),
            }
        finally:
            with lock:
                active -= 1

    def fake_scout_record(instrument, evaluation):
        return {
            "instrument": instrument,
            "attempt_id": evaluation["attempt_id"],
            "score": evaluation["score"],
        }

    monkeypatch.setenv("AUTORESEARCH_PLAY_HAND_INSTRUMENT_SCOUT_WORKERS", "3")
    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)
    monkeypatch.setattr(play_hand_mod, "_instrument_scout_record", fake_scout_record)

    ctx = PlayHandContext(
        config=None,
        cli=None,
        run_id="run-1",
        run_dir=tmp_path,
        profiles_dir=tmp_path / "profiles",
        evals_dir=tmp_path / "evals",
        attempts_path=tmp_path / "attempts.jsonl",
        events_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
    )

    primary, candidates, worker_count = _evaluate_instrument_scout_records(
        ctx,
        stage=play_hand_mod.PlayHandStage(7, 9, "instrument_scout"),
        profile_ref="profile-ref",
        profile_path=tmp_path / "profile.json",
        primary="XAUUSD",
        candidates=["EURUSD", "USDJPY", "AUDUSD"],
        timeframe="M5",
        lookback_months=3,
        reward_matrix=None,
    )

    assert worker_count == 3
    assert max_active > 1
    assert primary["instrument"] == "XAUUSD"
    assert [record["instrument"] for record in candidates] == [
        "EURUSD",
        "USDJPY",
        "AUDUSD",
    ]


def test_evaluate_final_scrutiny_branch_candidates_parallelizes_and_preserves_order(
    monkeypatch,
    tmp_path: Path,
) -> None:
    active = 0
    max_active = 0
    trace_roles: list[tuple[str, str, int | None]] = []
    lock = threading.Lock()

    def fake_evaluate_profile(
        *_args,
        phase,
        profile_ref,
        profile_path,
        instruments,
        timeframe,
        lookback_months,
        reward_matrix=None,
        as_of_date=None,
        resource_trace_role="critical_path",
        resource_trace_parent_span_id=None,
        **_kwargs,
    ):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            trace_roles.append((phase, resource_trace_role, resource_trace_parent_span_id))
            return {
                "artifact_dir": str(tmp_path / phase),
                "attempt_id": f"attempt-{phase}",
                "score": 60.0 if phase == "mutated_final_36mo" else 66.0,
                "profile_ref": profile_ref,
                "profile_path": str(profile_path),
                "instruments": list(instruments),
                "timeframe": timeframe,
                "lookback_months": lookback_months,
                "reward_matrix": reward_matrix,
                "as_of_date": as_of_date,
            }
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(play_hand_mod, "_evaluate_profile", fake_evaluate_profile)

    ctx = PlayHandContext(
        config=None,
        cli=None,
        run_id="run-1",
        run_dir=tmp_path,
        profiles_dir=tmp_path / "profiles",
        evals_dir=tmp_path / "evals",
        attempts_path=tmp_path / "attempts.jsonl",
        events_path=tmp_path / "events.jsonl",
        summary_path=tmp_path / "summary.json",
        resource_trace_enabled=True,
        resource_trace_path=tmp_path / "trace.jsonl",
        resource_trace_started_at="2026-06-15T00:00:00+00:00",
        resource_trace_base_perf=time.perf_counter(),
    )

    candidates = _evaluate_final_scrutiny_branch_candidates(
        ctx,
        stage=play_hand_mod.PlayHandStage(8, 9, "Final scrutiny"),
        branch_specs=[
            {
                "branch": "mutated",
                "phase": "mutated_final_36mo",
                "profile_ref": "mutated-ref",
                "profile_path": tmp_path / "mutated.json",
                "instruments": ["EURUSD"],
                "timeframe": "M15",
            },
            {
                "branch": "exact_template",
                "phase": "exact_template_36mo",
                "profile_ref": "exact-ref",
                "profile_path": tmp_path / "exact.json",
                "instruments": ["EURUSD", "GBPUSD"],
                "timeframe": "M15",
            },
        ],
        scrutiny_months=36,
        reward_matrix=None,
    )

    assert max_active > 1
    assert [candidate["branch"] for candidate in candidates] == [
        "mutated",
        "exact_template",
    ]
    assert [candidate["outcome"]["score"] for candidate in candidates] == [60.0, 66.0]
    assert {phase for phase, role, _parent in trace_roles} == {
        "mutated_final_36mo",
        "exact_template_36mo",
    }
    assert {role for _phase, role, _parent in trace_roles} == {"parallel_worker"}
    assert all(parent is not None for _phase, _role, parent in trace_roles)


def test_json_payload_from_stdout_accepts_trailing_json_payload() -> None:
    payload = _json_payload_from_stdout('noise\\n{"calculated": 2, "failed": 0}\\n')

    assert payload == {"calculated": 2, "failed": 0}


def test_sweep_progress_from_state_formats_deterministic_percent() -> None:
    progress = _sweep_progress_from_state(
        {
            "mode": "deterministic",
            "progress": {"completed": 240, "total": 960, "failed": 1},
        },
        fallback_mode="deterministic",
    )

    assert progress is not None
    assert progress["percent"] == 25.0
    assert progress["display"] == "25.0% (240/960 perms, 1 failed)"


def test_sweep_progress_from_state_formats_evolutionary_percent() -> None:
    progress = _sweep_progress_from_state(
        {
            "mode": "evolutionary",
            "progress": {
                "completed": 384,
                "total": 1024,
                "failed": 0,
                "generation": 6,
                "max_generations": 16,
                "best_fitness": 59.12346,
            },
        },
        fallback_mode="evolutionary",
    )

    assert progress is not None
    assert progress["percent"] == 37.5
    assert progress["display"] == "37.5% (384/1024 evals, gen 6/16, best=59.1235)"


def test_sweep_progress_from_cli_stderr_fallback_parses_latest_line() -> None:
    stderr = "\n".join(
        [
            "[sweep] Submitted sweep sweep-abc123",
            "[sweep] 120/960 permutations complete (0 failed)",
            "[sweep] 240/960 permutations complete (1 failed)",
        ]
    )

    assert _sweep_id_from_stderr(stderr) == "sweep-abc123"
    progress = _sweep_progress_from_cli_stderr(stderr, fallback_mode="deterministic")
    assert progress is not None
    assert progress["percent"] == 25.0
    assert progress["display"] == "25.0% (240/960 perms, 1 failed)"


def _profile_payload() -> dict:
    return {
        "format": "fuzzfolio.scoring-profile",
        "profile": {
            "name": "Example",
            "indicators": [
                {
                    "meta": {"id": "RSI_CROSSBACK"},
                    "config": {
                        "timeframe": "M5",
                        "lookbackBars": 1,
                        "talibConfig": [{"name": "timeperiod", "value": 14}],
                    },
                },
                {
                    "meta": {"id": "ADX"},
                    "config": {
                        "timeframe": "H1",
                        "lookbackBars": 1,
                        "talibConfig": [{"name": "timeperiod", "value": 20}],
                    },
                },
            ],
        },
    }


def test_lookback_axes_target_every_active_indicator_with_timeframe_ranges() -> None:
    assert build_lookback_axes(_profile_payload()) == [
        "indicator[0].config.lookbackBars=1,2,3,4,5",
        "indicator[1].config.lookbackBars=1,2,3",
    ]


def test_required_lookback_axes_aliases_universal_timing_axes() -> None:
    assert build_required_lookback_axes(_profile_payload()) == [
        "indicator[0].config.lookbackBars=1,2,3,4,5",
        "indicator[1].config.lookbackBars=1,2,3",
    ]


def test_deal_instruments_honors_pinned_instrument() -> None:
    dealt = deal_instruments(
        instrument=["XAUUSD"],
        instrument_pool=["EURUSD", "GBPUSD"],
        rng=random.Random(123),
    )

    assert dealt["source"] == "pinned"
    assert dealt["primary_instrument"] == "XAUUSD"
    assert dealt["instruments"] == ["XAUUSD"]


def test_deal_instruments_shuffles_from_pool_when_unpinned() -> None:
    dealt = deal_instruments(
        instrument=None,
        instrument_pool=["EURUSD", "GBPUSD", "XAUUSD"],
        rng=random.Random(123),
    )

    assert dealt["source"] == "dealt"
    assert dealt["primary_instrument"] in {"EURUSD", "GBPUSD", "XAUUSD"}
    assert dealt["instruments"] == [dealt["primary_instrument"]]
    assert dealt["instrument_pool"] == ["EURUSD", "GBPUSD", "XAUUSD"]


def _scout_record(instrument: str, score: float, equity_values: list[float]) -> dict:
    peak = max(equity_values) if equity_values else 0.0
    points = [
        {
            "date": f"2026-01-{index + 1:02d}",
            "equity_r": value,
            "realized_r": value,
            "drawdown_r": max(0.0, peak - value),
            "closed_trade_count": index,
        }
        for index, value in enumerate(equity_values)
    ]
    return {
        "instrument": instrument,
        "score": score,
        "resolved_trades": 12,
        "expectancy_r": 0.2,
        "_curve_features": _curve_features(points),
    }


def test_instrument_curve_similarity_uses_strategy_output_changes() -> None:
    primary = _scout_record("EURUSD", 80.0, [0, 1, 2, 3, 4, 5])
    clone = _scout_record("GBPUSD", 78.0, [0, 2, 4, 6, 8, 10])

    similarity = _instrument_curve_similarity(clone, primary)

    assert similarity["positive_correlation"] == 1.0
    assert similarity["similarity_score"] > 0.72


def test_instrument_scout_selects_viable_different_instrument() -> None:
    primary = _scout_record("EURUSD", 82.0, [0, 1, 2, 3, 4, 5])
    clone = _scout_record("GBPUSD", 80.0, [0, 2, 4, 6, 8, 10])
    different = _scout_record("AUDUSD", 76.0, [0, 1, 0, 1, 0, 1])

    result = _select_instrument_scout_records(
        primary,
        [clone, different],
        max_selected=3,
    )

    assert result["selected_instruments"] == ["EURUSD", "AUDUSD"]
    assert result["accepted"][0]["instrument"] == "AUDUSD"
    assert result["rejected"][0]["instrument"] == "GBPUSD"
    assert "too_similar_to_selected" in result["rejected"][0]["decision_reasons"]


def test_instrument_scout_rejects_large_score_drop_even_when_different() -> None:
    primary = _scout_record("EURUSD", 82.0, [0, 1, 2, 3, 4, 5])
    weak = _scout_record("AUDUSD", 55.0, [0, 1, 0, 1, 0, 1])

    result = _select_instrument_scout_records(primary, [weak], max_selected=3)

    assert result["selected_instruments"] == ["EURUSD"]
    assert result["rejected"][0]["instrument"] == "AUDUSD"
    assert "score_below_floor" in result["rejected"][0]["decision_reasons"]


def test_deal_indicator_count_varies_inside_min_max_range() -> None:
    counts = {
        deal_indicator_count(
            available_count=8,
            min_indicators=1,
            max_indicators=4,
            rng=random.Random(seed),
        )
        for seed in range(20)
    }

    assert counts <= {1, 2, 3, 4}
    assert len(counts) >= 2


def test_deal_indicator_count_supports_exact_hand_size() -> None:
    assert (
        deal_indicator_count(
            available_count=8,
            min_indicators=4,
            max_indicators=4,
            rng=random.Random(1),
        )
        == 4
    )


def test_deal_indicator_count_clamps_to_available_hand() -> None:
    assert (
        deal_indicator_count(
            available_count=3,
            min_indicators=2,
            max_indicators=8,
            rng=random.Random(1),
        )
        <= 3
    )


def test_coarse_axes_uses_numeric_talib_parameters() -> None:
    axes = build_coarse_axes(_profile_payload())

    assert axes[0].startswith("indicator[0].talib.timeperiod=")
    assert axes[1].startswith("indicator[1].talib.timeperiod=")


def test_play_hand_profile_defaults_deals_candlestick_pattern_bundle() -> None:
    payload = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "CANDLESTICK_PATTERNS"},
                    "config": {
                        "talibConfig": [
                            {"name": "patterns", "value": []},
                            {"name": "aggregation", "value": "any"},
                        ]
                    },
                }
            ]
        }
    }

    changes = apply_play_hand_profile_defaults(payload, rng=random.Random(1))
    talib = payload["profile"]["indicators"][0]["config"]["talibConfig"]
    patterns = next(item["value"] for item in talib if item["name"] == "patterns")

    assert changes
    assert changes[0]["bundle"]
    assert isinstance(patterns, list)
    assert patterns
    assert all(str(pattern).startswith("CDL") for pattern in patterns)


def test_play_hand_profile_defaults_keeps_existing_candlestick_patterns() -> None:
    payload = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "CANDLESTICK_PATTERNS"},
                    "config": {
                        "talibConfig": [
                            {"name": "patterns", "value": ["CDLENGULFING"]},
                        ]
                    },
                }
            ]
        }
    }

    assert apply_play_hand_profile_defaults(payload, rng=random.Random(1)) == []
    talib = payload["profile"]["indicators"][0]["config"]["talibConfig"]
    assert next(item["value"] for item in talib if item["name"] == "patterns") == ["CDLENGULFING"]


def test_sweep_planner_reduces_value_counts_before_constraining_axes() -> None:
    axes = [
        "indicator[0].talib.fastperiod=2,3,4,5,6",
        "indicator[0].talib.slowperiod=6,8,10,12,16",
        "indicator[1].talib.timeperiod=8,11,14,18,22",
        "indicator[2].talib.multiplier=0.8,1.0,1.5,2.0,2.5",
        "indicator[3].talib.acceleration=0.01,0.015,0.02,0.025,0.03",
        "indicator[3].talib.maximum=0.1,0.15,0.2,0.25,0.3",
    ]
    plan = plan_sweep_axes(axes, max_permutations=625, phase="coarse")

    assert plan.original_permutations == 15625
    assert plan.selected_permutations <= 625
    assert len(plan.axes) == len(axes)
    assert plan.anchored_axes == []
    assert any("=2,4,6" in axis or "=2,5,6" in axis for axis in plan.axes)
    assert any(
        item["selected_value_count"] < item["original_value_count"]
        for item in plan.axis_plans
    )


def test_fit_axes_to_permutation_budget_uses_procedural_planner() -> None:
    selected, dropped, original_count = fit_axes_to_permutation_budget(
        [
            "indicator[1].talib.fastperiod=2,3,4,5,6",
            "indicator[1].talib.slowperiod=6,8,10,12,16",
            "indicator[2].talib.timeperiod=8,11,14,18,22",
            "indicator[3].talib.acceleration=0.01,0.015,0.02,0.025,0.03",
            "indicator[3].talib.maximum=0.1,0.15,0.2,0.25,0.3",
        ],
        max_permutations=256,
    )

    assert original_count == 3125
    assert _permutation_count(selected) <= 256
    assert len(selected) == 5
    assert dropped == []


def test_evolutionary_budget_presets_scale_evenly_to_high_budget() -> None:
    assert evolutionary_budget_settings("low") == {
        "population_size": 32,
        "max_generations": 8,
        "evaluation_budget": 256,
    }
    assert evolutionary_budget_settings("medium") == {
        "population_size": 40,
        "max_generations": 16,
        "evaluation_budget": 640,
    }
    assert evolutionary_budget_settings("high") == {
        "population_size": 64,
        "max_generations": 16,
        "evaluation_budget": 1024,
    }


def test_sweep_budget_resolution_defaults_to_high_and_keeps_legacy_aliases() -> None:
    assert resolve_sweep_budget() == {
        "label": "high",
        "tier": "high",
        "value": 1024,
        "source": "default",
    }
    assert resolve_sweep_budget(sweep_budget="medium") == {
        "label": "medium",
        "tier": "medium",
        "value": 640,
        "source": "sweep_budget",
    }
    assert resolve_sweep_budget(evolutionary_budget="low") == {
        "label": "low",
        "tier": "low",
        "value": 256,
        "source": "evolutionary_budget",
    }
    assert resolve_sweep_budget(max_sweep_permutations=333) == {
        "label": "custom:333",
        "tier": None,
        "value": 333,
        "source": "max_sweep_permutations",
    }


def test_custom_evolutionary_budget_derives_metered_population_shape() -> None:
    assert evolutionary_budget_settings("custom:500", evaluation_budget=500) == {
        "population_size": 50,
        "max_generations": 10,
        "evaluation_budget": 500,
    }


def test_evolutionary_sweep_planner_keeps_broad_search_space() -> None:
    axes = [
        f"indicator[{index}].talib.timeperiod=5,8,13,21,34"
        for index in range(10)
    ]

    plan = plan_sweep_axes(
        axes,
        max_permutations=1024,
        phase="coarse",
        search_mode="evolutionary",
    )

    assert plan.search_mode == "evolutionary"
    assert plan.original_permutations == 9765625
    assert plan.selected_permutations == 3486784401
    assert len(plan.axes) == len(axes)
    assert plan.anchored_axes == []
    assert plan.dropped_axes == []
    assert all(item["selected_value_count"] == 9 for item in plan.axis_plans)


def test_sweep_planner_anchors_low_priority_axes_by_priority_not_order() -> None:
    axes = [
        "indicator[0].talib.signalperiod=3,5,7,9,11",
        "indicator[1].talib.smoothing=1,2,3,4,5",
        "indicator[2].talib.sma1=5,8,10,12,15",
        "indicator[3].talib.fastperiod=4,6,8,10,12",
        "indicator[3].talib.slowperiod=12,16,20,24,32",
        "indicator[4].talib.timeperiod=8,11,14,18,22",
        "indicator[5].talib.multiplier=0.8,1.0,1.5,2.0,2.5",
        "indicator[6].talib.threshold=10,20,30,40,50",
        "indicator[7].talib.window=8,13,21,34,55",
        "indicator[8].talib.length=5,10,15,20,25",
        "indicator[9].talib.period=3,6,9,12,15",
    ]
    plan = plan_sweep_axes(axes, max_permutations=625, phase="coarse")

    selected_keys = {item["key"] for item in plan.axis_plans if item["status"] == "selected"}
    anchored_keys = {item["key"] for item in plan.axis_plans if item["status"] == "anchored"}

    assert plan.selected_permutations <= 625
    assert "indicator[10].talib.period" not in selected_keys
    assert "indicator[0].talib.signalperiod" in anchored_keys
    assert "indicator[3].talib.fastperiod" in selected_keys
    assert "indicator[9].talib.period" in selected_keys


def test_sweep_planner_preserves_one_axis_per_indicator_when_possible() -> None:
    profile = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "CMO_TREND", "signalRole": "setup"},
                    "config": {
                        "talibConfig": [{"name": "timeperiod", "value": 14}],
                    },
                },
                {
                    "meta": {"id": "KST_CROSSOVER", "signalRole": "trigger"},
                    "config": {
                        "talibConfig": [
                            {"name": "roc1", "value": 10},
                            {"name": "roc2", "value": 15},
                            {"name": "roc3", "value": 20},
                            {"name": "roc4", "value": 30},
                            {"name": "sma1", "value": 10},
                            {"name": "sma2", "value": 10},
                            {"name": "sma3", "value": 10},
                            {"name": "sma4", "value": 15},
                            {"name": "signalperiod", "value": 9},
                        ],
                    },
                },
            ]
        }
    }
    plan = plan_sweep_axes(
        build_coarse_axes(profile),
        profile_payload=profile,
        phase="coarse",
        max_permutations=625,
    )

    selected_keys = {item["key"] for item in plan.axis_plans if item["status"] == "selected"}
    anchored_keys = {item["key"] for item in plan.axis_plans if item["status"] == "anchored"}

    assert "indicator[0].talib.timeperiod" in selected_keys
    assert "indicator[1].talib.signalperiod" in anchored_keys
    assert plan.selected_permutations <= 625


def test_sweep_planner_preserves_current_values_when_sampling() -> None:
    profile = _profile_payload()
    plan = plan_sweep_axes(
        [
            "indicator[0].talib.timeperiod=8,11,14,18,22",
            "indicator[1].talib.timeperiod=10,14,20,25,32",
            "indicator[0].config.lookbackBars=1,2,3,4,5",
            "indicator[1].config.lookbackBars=1,2,3",
        ],
        profile_payload=profile,
        phase="lookback_timing",
        max_permutations=24,
    )

    by_key = {item["key"]: item for item in plan.axis_plans}
    assert 14 in by_key["indicator[0].talib.timeperiod"]["selected_values"]
    assert 20 in by_key["indicator[1].talib.timeperiod"]["selected_values"]
    assert 1 in by_key["indicator[0].config.lookbackBars"]["selected_values"]


def test_build_coarse_axes_collects_late_numeric_parameters() -> None:
    profile = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "KST_CROSSOVER"},
                    "config": {
                        "timeframe": "M5",
                        "talibConfig": [
                            {"name": "roc1", "value": 10},
                            {"name": "roc2", "value": 15},
                            {"name": "roc3", "value": 20},
                            {"name": "roc4", "value": 30},
                            {"name": "sma1", "value": 10},
                            {"name": "sma2", "value": 10},
                            {"name": "sma3", "value": 10},
                            {"name": "sma4", "value": 15},
                            {"name": "signalperiod", "value": 9},
                        ],
                    },
                }
            ]
        }
    }

    axes = build_coarse_axes(profile)

    assert len(axes) == 9
    assert axes[-1].startswith("indicator[0].talib.signalperiod=")


def test_normalize_sweep_payload_maps_backend_axes_to_requested_axes(tmp_path: Path) -> None:
    definition_path = tmp_path / "sweep-definition.json"
    definition_path.write_text(
        __import__("json").dumps(
            {
                "axes": [
                    {"indicator_instance_id": "abc", "param_key": "timeperiod"},
                    {"indicator_instance_id": "def", "param_key": "lookbackBars"},
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = {
        "data": {
            "fitness_metric": "score_lab",
            "parameter_importance": [
                {"axis": "abc.timeperiod", "best_value": 18, "importance_pct": 55.0}
            ],
            "ranked_permutations": [
                {
                    "fitness_value": 42.0,
                    "parameters": {"abc.timeperiod": 18, "def.lookbackBars": 3},
                }
            ],
        }
    }

    _normalize_sweep_payload(
        payload,
        requested_axes=[
            "indicator[0].talib.timeperiod=8,14,18",
            "indicator[1].config.lookbackBars=1,2,3",
        ],
        definition_path=definition_path,
    )

    assert _top_sweep_score(payload) == 42.0
    assert _best_sweep_parameters(payload) == {
        "indicator[0].talib.timeperiod": 18,
        "indicator[1].config.lookbackBars": 3,
    }
    assert _parameter_importance(payload)[0]["axis"] == "indicator[0].talib.timeperiod"
    assert _parameter_importance(payload)[0]["backend_axis"] == "abc.timeperiod"


def test_focused_axes_refines_high_importance_numeric_winners() -> None:
    focused = build_focused_axes(
        [
            {
                "axis": "indicator[0].talib.timeperiod",
                "importance_pct": 42.0,
                "best_value": 14,
            },
            {
                "axis": "indicator[1].talib.timeperiod",
                "importance_pct": 2.0,
                "best_value": 20,
            },
        ],
        [
            "indicator[0].talib.timeperiod=8,11,14,18,22",
            "indicator[1].talib.timeperiod=10,14,20,25,32",
        ],
    )

    assert focused == ["indicator[0].talib.timeperiod=10,12,14,16,18"]


def test_refine_values_stays_inside_prior_numeric_range_at_boundaries() -> None:
    assert _refine_values([12, 20, 32], 12) == [12, 16, 20, 24, 28]
    assert _refine_values([12, 20, 32], 32) == [16, 20, 24, 28, 32]


def test_focused_axes_do_not_extrapolate_below_prior_axis_floor() -> None:
    focused = build_focused_axes(
        [
            {
                "axis": "indicator[2].talib.fastperiod",
                "importance_pct": 55.0,
                "best_value": 12,
            },
        ],
        ["indicator[2].talib.fastperiod=12,20,32"],
    )

    assert focused == ["indicator[2].talib.fastperiod=12,16,20,24,28"]


def test_sweep_parameter_candidates_preserve_ranked_fallbacks() -> None:
    candidates = _sweep_parameter_candidates(
        {
            "data": {
                "ranked_permutations": [
                    {"parameters": {"indicator[0].talib.fastperiod": 4}},
                    {"parameters": {"indicator[0].talib.fastperiod": 12}},
                    {"parameters": {"indicator[0].talib.fastperiod": 12}},
                ],
                "best": {"parameters": {"indicator[0].talib.fastperiod": 16}},
            }
        }
    )

    assert candidates == [
        {"indicator[0].talib.fastperiod": 4},
        {"indicator[0].talib.fastperiod": 12},
        {"indicator[0].talib.fastperiod": 16},
    ]


def test_materialize_profile_variant_applies_config_and_talib_params(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    output = tmp_path / "output.json"
    source.write_text(
        __import__("json").dumps(_profile_payload()),
        encoding="utf-8",
    )

    materialize_profile_variant(
        source,
        output,
        {
            "indicator[0].config.lookbackBars": 3,
            "indicator[1].talib.timeperiod": 28,
        },
        name_suffix="[top]",
    )

    payload = __import__("json").loads(output.read_text(encoding="utf-8"))
    indicators = payload["profile"]["indicators"]
    assert indicators[0]["config"]["lookbackBars"] == 3
    assert indicators[1]["config"]["talibConfig"][0]["value"] == 28
    assert payload["profile"]["name"].endswith("[top]")
