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
    assert summary["draft_name"] == "sharp-growth-momentum"
    assert summary["family_id"] == "scaffold-mfi-trend-2|scaffold-stoch-crossover-1"
    assert summary["indicator_ids"] == ["STOCH_CROSSOVER", "MFI_TREND"]
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
                "family_id": "fam-a|fam-b",
                "profile_path": r"C:\tmp\cand.json",
            },
            "created_profile_ref": "abc123",
            "next_recommended_action": "evaluate_candidate",
        }
    )
    assert summary == (
        r"family=fam-a|fam-b, profile_ref=abc123, "
        r"profile_path=C:\tmp\cand.json, next=evaluate_candidate"
    )


def test_summarize_sweep_handle_includes_inspect_ref() -> None:
    summary = ResearchController._summarize_sweep_handle(
        {
            "inspect_ref": "sweep_alpha_20260401",
            "artifact_dir": r"C:\tmp\sweep_alpha_20260401",
            "quality_score_preset": "profile-drop",
            "next_recommended_action": "inspect_artifact",
        }
    )
    assert summary == (
        r"inspect_ref=sweep_alpha_20260401, "
        r"artifact_dir=C:\tmp\sweep_alpha_20260401, "
        r"score_preset=profile-drop, next=inspect_artifact"
    )
