from __future__ import annotations

from pathlib import Path

from autoresearch.controller import (
    ResearchController,
    _candidate_summary_from_profile_payload,
    _extract_timeframe_mismatch_from_output,
    _normalized_profile_material_changes,
    _summarize_sweep_results_payload,
)


def test_candidate_summary_from_profile_payload() -> None:
    payload = {
        "format": "fuzzfolio.scoring-profile",
        "profile": {
            "name": "Scaffold STOCH_CROSSOVER + MFI_TREND",
            "indicators": [
                {
                    "config": {"timeframe": "M5"},
                    "meta": {
                        "id": "STOCH_CROSSOVER",
                        "instanceId": "scaffold-stoch-crossover-1",
                    },
                },
                {
                    "config": {"timeframe": "M5"},
                    "meta": {
                        "id": "MFI_TREND",
                        "instanceId": "scaffold-mfi-trend-2",
                    },
                },
            ],
            "instruments": ["BTCUSD", "AUDJPY"],
        },
    }
    summary = _candidate_summary_from_profile_payload(
        payload,
        draft_name="sharp-growth-momentum",
    )
    assert summary is not None
    assert summary["candidate_name"] == "sharp-growth-momentum"
    assert summary["draft_name"] == "sharp-growth-momentum"
    assert summary["family_id"] == "scaffold-mfi-trend-2|scaffold-stoch-crossover-1"
    assert summary["indicator_ids"] == ["STOCH_CROSSOVER", "MFI_TREND"]
    assert summary["instruments"] == ["BTCUSD", "AUDJPY"]
    assert summary["timeframe_summary"] == "M5 + M5"
    assert summary["instrument_summary"] == "BTCUSD, AUDJPY"
    assert summary["instrument_count"] == 2


def test_normalized_profile_material_changes_detected() -> None:
    source = {
        "profile": {
            "name": "x",
            "indicators": [
                {
                    "config": {"timeframe": "M5", "weight": 1.0},
                    "meta": {"id": "A", "instanceId": "a-1"},
                }
            ],
            "instruments": ["EURUSD"],
        }
    }
    normalized = {
        "name": "x",
        "indicators": [
            {
                "config": {"timeframe": "H1", "weight": 1.0},
                "meta": {"id": "A", "instanceId": "a-1"},
            }
        ],
        "instruments": ["EURUSD"],
    }
    assert _normalized_profile_material_changes(source, normalized) is True
    assert _normalized_profile_material_changes(source, source["profile"]) is False


def test_summarize_sweep_results_payload_plateau() -> None:
    payload = {
        "data": {
            "elapsed_seconds": 10.2,
            "fitness_metric": "quality_score",
            "mode": "deterministic",
            "parameter_importance": [
                {
                    "axis": "a.ranges.buy",
                    "importance_pct": 0.0,
                    "raw_spread": 0.0,
                },
                {
                    "axis": "b.ranges.buy",
                    "importance_pct": 0.0,
                    "raw_spread": 0.0,
                },
            ],
            "ranked_permutations": [
                {
                    "rank": 1,
                    "fitness_value": 57.2705,
                    "parameters": {
                        "a.ranges.buy": "[55",
                        "b.ranges.buy": "[60",
                    },
                    "fitness": {
                        "quality_score": 57.2705,
                        "resolved_trade_count_max": 14,
                        "quality_score_payload": {
                            "inputs": {"effective_window_months": 11.56}
                        },
                    },
                }
            ],
        }
    }
    summary = _summarize_sweep_results_payload(
        payload,
        artifact_dir=Path(r"C:\tmp\sweep"),
    )
    assert summary is not None
    assert summary["mode"] == "deterministic"
    assert summary["fitness_metric"] == "quality_score"
    assert summary["top_score"] == 57.2705
    assert summary["top_parameters"] == {
        "a.ranges.buy": "[55",
        "b.ranges.buy": "[60",
    }
    assert summary["top_effective_window_months"] == 11.56
    assert summary["parameter_importance_flat"] is True
    assert "plateau" in summary["recommended_interpretation"].lower()


def test_extract_timeframe_mismatch_from_output_handles_quoted_values() -> None:
    entry = _extract_timeframe_mismatch_from_output(
        "Auto-adjusted timeframe from 'H1' to 'M5' because active indicators require lower data."
    )
    assert entry is not None
    assert entry["requested"] == "H1"
    assert entry["effective"] == "M5"
    assert entry["mismatch"] is True
    assert entry["source"] == "cli_output"


def test_resolve_timeframe_mismatch_prefers_structured_fields() -> None:
    controller = object.__new__(ResearchController)
    controller._timeframe_mismatches = []
    cli_result = {
        "result": {
            "parsed_json": {
                "requested_timeframe": "D1",
                "effective_timeframe": "M5",
            },
            "stdout": "",
            "stderr": "",
        }
    }
    entry = controller._resolve_timeframe_mismatch(cli_result, auto_log=None)
    assert entry is not None
    assert entry["requested"] == "D1"
    assert entry["effective"] == "M5"
    assert entry["source"] == "parsed_json"
    assert controller._timeframe_mismatches == [entry]


def test_summarize_candidate_handle_prefers_concrete_refs() -> None:
    summary = ResearchController._summarize_candidate_handle(
        {
            "candidate_summary": {
                "candidate_name": "cand",
                "draft_name": "cand",
                "family_id": "fam-a|fam-b",
            },
            "created_profile_ref": "abc123",
            "next_recommended_action": "evaluate_candidate",
        }
    )
    assert summary == r"candidate_name=cand, profile_ref=abc123, next=evaluate_candidate"


def test_summarize_sweep_handle_omits_artifact_path() -> None:
    summary = ResearchController._summarize_sweep_handle(
        {
            "inspect_ref": "sweep_alpha_20260401",
            "artifact_dir": r"C:\tmp\sweep_alpha_20260401",
            "mode": "deterministic",
            "quality_score_preset": "profile-drop",
            "next_recommended_action": "inspect_artifact",
        }
    )
    assert summary == (
        r"inspect_ref=sweep_alpha_20260401, mode=deterministic, "
        r"score_preset=profile-drop, next=inspect_artifact"
    )


def test_history_result_summary_compacts_validate_success_for_prompt_visibility() -> None:
    controller = object.__new__(ResearchController)

    summarized = controller._history_result_summary(
        {
            "tool": "validate_profile",
            "ok": True,
            "candidate_name": "initial_seed_scaffold",
            "candidate_summary": {
                "candidate_name": "initial_seed_scaffold",
                "draft_name": "initial_seed_scaffold",
                "profile_name": "Scaffold ADX + SAR_TREND",
                "candidate_fingerprint": "abc1234567890",
                "family_id": "scaffold-adx-1|scaffold-sar-trend-2",
                "indicator_ids": ["ADX", "SAR_TREND"],
                "indicator_instance_ids": ["scaffold-adx-1", "scaffold-sar-trend-2"],
                "instruments": ["BTCUSD", "EURUSD"],
                "timeframe_summary": "M5 + M5",
            },
            "controller_hint": "Validation passed. Register next.",
            "ready_for_registration": True,
            "material_changes": False,
            "result": {
                "argv": ["profiles", "validate", "--file", "cand.json", "--pretty"],
                "returncode": 0,
                "parsed_json": {"status": "ok", "data": {"normalized_profile": {}}},
            },
        }
    )

    assert summarized == {
        "tool": "validate_profile",
        "ok": True,
        "candidate_name": "initial_seed_scaffold",
        "controller_hint": "Validation passed. Register next.",
        "indicator_ids": ["ADX", "SAR_TREND"],
        "instruments": ["BTCUSD", "EURUSD"],
        "timeframe_summary": "M5 + M5",
        "ready_for_registration": True,
        "material_changes": False,
    }


def test_history_result_summary_compacts_compare_artifacts_for_prompt_visibility() -> None:
    controller = object.__new__(ResearchController)

    summarized = controller._history_result_summary(
        {
            "tool": "compare_artifacts",
            "ok": True,
            "ranked_comparison": [
                {
                    "label": "att-1",
                    "artifact_dir": r"C:\runs\att-1",
                    "quality_score": 58.2,
                    "best": {
                        "quality_score": 58.2,
                        "signal_count": 44,
                        "timeframe": "M5",
                        "best_cell": {"resolved_trades": 19},
                        "market_data_window": {"effective_window_months": 12.4},
                    },
                },
                {
                    "label": "att-2",
                    "artifact_dir": r"C:\runs\att-2",
                    "quality_score": 41.1,
                },
            ],
            "dominant_deltas": ["score_delta=17.1000", "other=1", "third=2", "ignore=3"],
            "suggested_next_move": "evaluate_candidate on leader unless retention already satisfied",
        }
    )

    assert summarized == {
        "tool": "compare_artifacts",
        "ok": True,
        "ranked_preview": [
            {
                "label": "att-1",
                "quality_score": 58.2,
                "signal_count": 44,
                "timeframe": "M5",
                "resolved_trades": 19,
                "effective_window_months": 12.4,
            },
            {
                "label": "att-2",
                "quality_score": 41.1,
            },
        ],
        "dominant_deltas": ["score_delta=17.1000", "other=1", "third=2"],
        "suggested_next_move": "evaluate_candidate on leader unless retention already satisfied",
    }


def test_history_result_summary_keeps_sweep_followup_scaffold_preview() -> None:
    controller = object.__new__(ResearchController)

    summarized = controller._history_result_summary(
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_kind": "parameter_sweep",
            "sweep_summary": {
                "fitness_metric": "quality_score",
                "top_score": 57.2705,
                "top_effective_window_months": 11.56,
                "top_parameters": {
                    "indicator[0].config.timeframe": "H1",
                },
            },
            "recommended_destination_candidate_name": "cand_sweep_top",
            "recommended_followup_actions": [
                {
                    "tool": "prepare_profile",
                    "mode": "clone_local",
                    "source_profile_ref": "ref-a",
                    "destination_candidate_name": "cand_sweep_top",
                },
                {
                    "tool": "mutate_profile",
                    "candidate_name": "cand_sweep_top",
                    "mutations": [
                        {
                            "path": "indicator[0].config.timeframe",
                            "value": "H1",
                        }
                    ],
                },
                {
                    "tool": "validate_profile",
                    "candidate_name": "cand_sweep_top",
                },
            ],
            "controller_hint": "Clone-first sweep follow-up is ready.",
        }
    )

    assert summarized == {
        "tool": "inspect_artifact",
        "ok": True,
        "artifact_kind": "parameter_sweep",
        "sweep_summary": {
            "fitness_metric": "quality_score",
            "top_score": 57.2705,
            "top_effective_window_months": 11.56,
            "top_parameters": {
                "indicator[0].config.timeframe": "H1",
            },
        },
        "recommended_destination_candidate_name": "cand_sweep_top",
        "recommended_followup_actions": [
            {
                "tool": "prepare_profile",
                "mode": "clone_local",
                "source_profile_ref": "ref-a",
                "destination_candidate_name": "cand_sweep_top",
            },
            {
                "tool": "mutate_profile",
                "candidate_name": "cand_sweep_top",
                "mutations": [
                    {
                        "path": "indicator[0].config.timeframe",
                        "value": "H1",
                    }
                ],
            },
            {
                "tool": "validate_profile",
                "candidate_name": "cand_sweep_top",
            },
        ],
        "controller_hint": "Clone-first sweep follow-up is ready.",
    }


def test_history_result_summary_omits_artifact_paths_for_eval_and_inspect() -> None:
    controller = object.__new__(ResearchController)

    eval_summary = controller._history_result_summary(
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a",
            "attempt_id": "attempt-1",
            "effective_window_months": 11.2,
            "artifact_dir": r"C:\runs\evals\artifact-a",
        }
    )
    inspect_summary = controller._history_result_summary(
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_dir": r"C:\runs\evals\artifact-a",
            "artifact_kind": "sensitivity_eval",
        }
    )

    assert "artifact_dir" not in eval_summary
    assert "artifact_dir" not in inspect_summary
