import json
import struct
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import __main__ as ar_main
from autoresearch import portfolio as pf


def _write_minimal_png(path: Path) -> None:
    def chunk(chunk_type: bytes, payload: bytes) -> bytes:
        crc = zlib.crc32(chunk_type + payload) & 0xFFFFFFFF
        return (
            struct.pack(">I", len(payload))
            + chunk_type
            + payload
            + struct.pack(">I", crc)
        )

    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"\x00\x00\x00\x00\x00"))
        + chunk(b"IEND", b"")
    )


def test_load_portfolio_spec_defaults_when_file_missing(tmp_path: Path) -> None:
    spec, defaulted = pf.load_portfolio_spec(tmp_path / "missing.json")

    assert defaulted is True
    assert spec["portfolio_name"] == "default-portfolio"
    assert spec["full_backtest_job_timeout_seconds"] == 2400
    assert [sleeve["name"] for sleeve in spec["sleeves"]] == [
        "quality",
        "cadence",
        "breadth",
    ]


def test_load_portfolio_build_specs_supports_variants_and_account_presets(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "portfolio-variants.json"
    config_path.write_text(
        json.dumps(
            {
                "version": 2,
                "catch_up_full_backtests": False,
                "generate_profile_drops": False,
                "export_bundle": False,
                "sleeves": [{"name": "shared-quality"}],
                "account_presets": {
                    "tiny-live": {
                        "name": "Tiny Live",
                        "account_size_usd": 124,
                        "leverage": 500,
                        "risk_per_trade_pct": 0.75,
                        "allowed_asset_classes": ["forex"],
                    }
                },
                "portfolio_variants": [
                    {
                        "portfolio_name": "micro-live",
                        "account_preset": "tiny-live",
                    },
                    {
                        "portfolio_name": "darwinex-zero",
                        "account": {
                            "name": "Darwinex Zero",
                            "account_size_usd": 100000,
                            "leverage": 200,
                            "blocked_asset_classes": ["crypto"],
                        },
                        "sleeves": [{"name": "prop-growth"}],
                    },
                ],
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    specs, defaulted = pf.load_portfolio_build_specs(config_path)

    assert defaulted is False
    assert [item["portfolio_name"] for item in specs] == ["micro-live", "darwinex-zero"]
    assert specs[0]["account"]["account_preset_name"] == "tiny-live"
    assert specs[0]["account"]["allowed_asset_classes"] == ["fx"]
    assert specs[0]["account"]["leverage"] == 500.0
    assert [sleeve["name"] for sleeve in specs[0]["sleeves"]] == ["shared-quality"]
    assert specs[1]["account"]["blocked_asset_classes"] == ["crypto"]
    assert [sleeve["name"] for sleeve in specs[1]["sleeves"]] == ["prop-growth"]


def test_default_portfolio_config_path_prefers_account_presets(tmp_path: Path) -> None:
    modern_path = tmp_path / "portfolio.account-presets.json"
    legacy_path = tmp_path / "portfolio.config.json"
    modern_path.write_text("{}", encoding="utf-8")
    legacy_path.write_text("{}", encoding="utf-8")

    config = SimpleNamespace(repo_root=tmp_path)

    assert ar_main._default_portfolio_config_path(config) == modern_path


def test_default_portfolio_config_path_falls_back_to_legacy(tmp_path: Path) -> None:
    legacy_path = tmp_path / "portfolio.config.json"
    legacy_path.write_text("{}", encoding="utf-8")

    config = SimpleNamespace(repo_root=tmp_path)

    assert ar_main._default_portfolio_config_path(config) == legacy_path


def test_build_sleeve_selection_annotates_selected_rows() -> None:
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 70.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    sleeve = pf.build_sleeve_selection(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "quality",
            "shortlist_size": 1,
        },
    )

    assert sleeve["name"] == "quality"
    assert len(sleeve["selected_rows"]) == 1
    assert sleeve["selected_rows"][0]["sleeve_name"] == "quality"
    assert sleeve["selected_rows"][0]["sleeve_selection_rank"] == 1


def test_build_selection_basket_curve_recomputes_drawdown_from_basket_equity(
    tmp_path: Path,
) -> None:
    curve_a = tmp_path / "curve-a.json"
    curve_b = tmp_path / "curve-b.json"
    curve_a.write_text(
        json.dumps(
            {
                "curve": {
                    "points": [
                        {
                            "time": 1704067200,
                            "date": "2024-01-01",
                            "equity_r": 10.0,
                            "drawdown_r": 0.0,
                            "realized_r": 10.0,
                            "closed_trade_count": 1,
                        },
                        {
                            "time": 1704153600,
                            "date": "2024-01-02",
                            "equity_r": 7.0,
                            "drawdown_r": 3.0,
                            "realized_r": 7.0,
                            "closed_trade_count": 2,
                        },
                    ]
                }
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    curve_b.write_text(
        json.dumps(
            {
                "curve": {
                    "points": [
                        {
                            "time": 1704067200,
                            "date": "2024-01-01",
                            "equity_r": 10.0,
                            "drawdown_r": 0.0,
                            "realized_r": 10.0,
                            "closed_trade_count": 1,
                        },
                        {
                            "time": 1704153600,
                            "date": "2024-01-02",
                            "equity_r": 20.0,
                            "drawdown_r": 0.0,
                            "realized_r": 20.0,
                            "closed_trade_count": 2,
                        },
                    ]
                }
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    basket = ar_main._build_selection_basket_curve(
        [
            {"full_backtest_curve_path_36m": str(curve_a)},
            {"full_backtest_curve_path_36m": str(curve_b)},
        ]
    )

    assert basket["point_count"] == 2
    assert basket["points"][0]["equity_r"] == 20.0
    assert basket["points"][0]["drawdown_r"] == 0.0
    assert basket["points"][1]["equity_r"] == 27.0
    assert basket["points"][1]["drawdown_r"] == 0.0
    assert basket["max_drawdown_r"] == 0.0
    assert basket["final_drawdown_r"] == 0.0


def test_resolve_prefilter_limit_uses_legacy_candidate_limit_when_present() -> None:
    assert (
        pf.resolve_prefilter_limit(
            {
                **pf.DEFAULT_SLEEVE_SPEC,
                "prefilter_limit": -1,
                "candidate_limit": 77,
            }
        )
        == 77
    )


def test_build_sleeve_prefilter_uses_sleeve_local_scalar_utility() -> None:
    rows = [
        {
            "attempt_id": "slow-high-score",
            "run_id": "run-a",
            "score_36m": 80.0,
            "trades_per_month_36m": 0.2,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "fast-lower-score",
            "run_id": "run-b",
            "score_36m": 78.0,
            "trades_per_month_36m": 6.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    result = pf.build_sleeve_prefilter(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "cadence",
            "prefilter_limit": 1,
            "trade_rate_bonus_weight": 3.0,
            "trade_rate_bonus_target": 6.0,
        },
    )

    assert result["qualified_rows"][0]["attempt_id"] == "slow-high-score"
    assert [row["attempt_id"] for row in result["candidate_rows"]] == ["fast-lower-score"]
    assert result["prefilter_limit"] == 1
    assert result["prefilter_excluded_count"] == 1


def test_build_sleeve_prefilter_can_reward_breadth_metrics() -> None:
    rows = [
        {
            "attempt_id": "narrow-higher-score",
            "run_id": "run-a",
            "score_36m": 80.0,
            "durability_score_36m": 0.1,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "broad-lower-score",
            "run_id": "run-b",
            "score_36m": 76.0,
            "durability_score_36m": 1.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    result = pf.build_sleeve_prefilter(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "breadth",
            "prefilter_limit": 1,
            "scalar_metric_terms": [
                {
                    "name": "breadth_score",
                    "field_candidates": ["breadth_score_36m", "durability_score_36m"],
                    "direction": "higher",
                    "target": 1.0,
                    "weight": 6.0,
                }
            ],
        },
    )

    assert [row["attempt_id"] for row in result["candidate_rows"]] == ["broad-lower-score"]
    assert float(result["candidate_rows"][0]["prefilter_scalar_metric_bonus_component"]) == 6.0
    assert result["candidate_rows"][0]["prefilter_scalar_metric_bonus_terms"][0]["field"] == (
        "durability_score_36m"
    )


def test_build_sleeve_prefilter_can_apply_field_filters() -> None:
    rows = [
        {
            "attempt_id": "too-heavy",
            "run_id": "run-a",
            "score_36m": 82.0,
            "account_estimated_avg_margin_load_pct_36m": 28.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "fits-budget",
            "run_id": "run-b",
            "score_36m": 81.0,
            "account_estimated_avg_margin_load_pct_36m": 11.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    result = pf.build_sleeve_prefilter(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "margin-aware",
            "field_filters": [
                {
                    "name": "avg_margin_load",
                    "field": "account_estimated_avg_margin_load_pct_36m",
                    "direction": "lower",
                    "target": 20.0,
                }
            ],
        },
    )

    assert [row["attempt_id"] for row in result["qualified_rows"]] == ["fits-budget"]
    assert result["filter_rejections"]["field_filter_failed_avg_margin_load"] == 1


def test_filter_play_hand_candidate_scope_keeps_canonical_and_non_playhand_rows() -> None:
    rows = [
        {
            "attempt_id": "run-a-attempt-00001",
            "run_id": "run-a-playhand-v1",
            "runner": "play_hand_v1",
            "score_36m": 90.0,
        },
        {
            "attempt_id": "run-a-attempt-00002",
            "run_id": "run-a-playhand-v1",
            "runner": "play_hand_v1",
            "score_36m": 80.0,
            "canonical_attempt_id": "run-a-attempt-00002",
        },
        {
            "attempt_id": "manual-attempt-00001",
            "run_id": "manual-run",
            "score_36m": 70.0,
        },
    ]

    filtered, scope = pf.filter_play_hand_candidate_scope(rows, "promoted")

    assert [row["attempt_id"] for row in filtered] == [
        "run-a-attempt-00002",
        "manual-attempt-00001",
    ]
    assert scope["dropped_count"] == 1
    assert scope["playhand_runs_with_canonical"] == 1


def test_select_dashboard_preferred_attempt_rows_matches_reader_logic() -> None:
    rows = [
        {
            "run_id": "playhand-run",
            "attempt_id": "playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "score_36m": 95.0,
            "composite_score": 95.0,
            "canonical_attempt_id": "playhand-run-attempt-00002",
        },
        {
            "run_id": "playhand-run",
            "attempt_id": "playhand-run-attempt-00002",
            "runner": "play_hand_v1",
            "score_36m": 80.0,
            "composite_score": 80.0,
            "canonical_attempt_id": "playhand-run-attempt-00002",
            "is_canonical_playhand_attempt": True,
        },
        {
            "run_id": "explorer-run",
            "attempt_id": "explorer-run-attempt-00001",
            "score_36m": 65.0,
            "composite_score": 70.0,
        },
        {
            "run_id": "explorer-run",
            "attempt_id": "explorer-run-attempt-00002",
            "score_36m": 75.0,
            "composite_score": 72.0,
        },
        {
            "run_id": "explorer-run",
            "attempt_id": "explorer-run-attempt-00003",
            "score_36m": None,
            "composite_score": 99.0,
        },
    ]

    selected, info = pf.select_dashboard_preferred_attempt_rows(rows)

    assert [row["attempt_id"] for row in selected] == [
        "playhand-run-attempt-00002",
        "explorer-run-attempt-00002",
    ]
    assert info["canonical_run_count"] == 1
    assert info["score_selected_run_count"] == 1


def test_select_dashboard_preferred_attempt_rows_skips_tombstoned_playhand_run() -> None:
    rows = [
        {
            "run_id": "dead-playhand-run",
            "attempt_id": "dead-playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "attempt_role": "instrument_scout",
            "score_36m": 88.0,
            "composite_score": 88.0,
        },
        {
            "run_id": "dead-playhand-run",
            "attempt_id": "dead-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 0.0,
            "composite_score": 0.0,
            "attempt_decision": "tombstoned",
            "attempt_decision_reasons": ["final_36mo_scrutiny_failed"],
        },
        {
            "run_id": "live-playhand-run",
            "attempt_id": "live-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 61.0,
            "composite_score": 61.0,
            "is_canonical_playhand_attempt": True,
        },
    ]

    selected, info = pf.select_dashboard_preferred_attempt_rows(rows)

    assert [row["attempt_id"] for row in selected] == [
        "live-playhand-run-attempt-00011"
    ]
    assert info["tombstoned_run_count"] == 1
    assert info["tombstoned_dropped_count"] == 2


def test_select_dashboard_preferred_attempt_rows_skips_incomplete_playhand_run() -> None:
    rows = [
        {
            "run_id": "incomplete-playhand-run",
            "attempt_id": "incomplete-playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "attempt_role": "baseline",
            "score_36m": 93.0,
            "composite_score": 93.0,
        },
        {
            "run_id": "live-playhand-run",
            "attempt_id": "live-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 61.0,
            "composite_score": 61.0,
            "is_canonical_playhand_attempt": True,
        },
    ]

    selected, info = pf.select_dashboard_preferred_attempt_rows(rows)

    assert [row["attempt_id"] for row in selected] == [
        "live-playhand-run-attempt-00011"
    ]
    assert info["incomplete_playhand_run_count"] == 1
    assert info["incomplete_playhand_dropped_count"] == 1


def test_filter_play_hand_candidate_scope_all_skips_incomplete_playhand_run() -> None:
    rows = [
        {
            "run_id": "incomplete-playhand-run",
            "attempt_id": "incomplete-playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "attempt_role": "baseline",
            "score_36m": 93.0,
            "composite_score": 93.0,
        },
        {
            "run_id": "completed-playhand-run",
            "attempt_id": "completed-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 61.0,
            "composite_score": 61.0,
            "is_canonical_playhand_attempt": True,
        },
    ]

    selected, info = pf.filter_play_hand_candidate_scope(rows, "all")

    assert [row["attempt_id"] for row in selected] == [
        "completed-playhand-run-attempt-00011"
    ]
    assert info["incomplete_playhand_run_count"] == 1
    assert info["incomplete_playhand_dropped_count"] == 1


def test_enrich_rows_for_account_computes_account_metrics(tmp_path: Path) -> None:
    result_path = tmp_path / "full-backtest-result.json"
    result_path.write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "behavior_summary": {
                            "signal_coverage_ratio": 0.12,
                            "signal_density": 0.12,
                            "bars_per_signal": 18.0,
                            "signal_selectivity": "selective",
                        },
                        "best_cell": {
                            "profit_factor": 1.4,
                            "avg_net_r_per_closed_trade": 0.22,
                            "stop_loss_percent": 0.5,
                        },
                        "best_cell_path_metrics": {
                            "avg_holding_hours": 36.0,
                            "p90_holding_hours": 72.0,
                            "max_holding_hours": 120.0,
                            "time_under_water_ratio": 0.18,
                            "final_equity_r": 240.0,
                        },
                        "matrix_summary": {
                            "positive_cell_ratio": 0.75,
                            "robust_cell": {
                                "avg_net_r_per_closed_trade": 0.19,
                                "stop_loss_percent": 0.6,
                            },
                        },
                        "quality_score": {
                            "inputs": {"edge_rate_r_per_month": 9.0}
                        },
                    }
                }
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 82.0,
            "trades_per_month_36m": 24.0,
            "instruments_36m": ["EURUSD"],
            "full_backtest_result_path_36m": str(result_path),
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        }
    ]

    enriched = pf.enrich_rows_for_account(
        rows,
        {
            "name": "Tiny Live",
            "account_size_usd": 124,
            "leverage": 500,
            "risk_per_trade_pct": 0.75,
            "allowed_asset_classes": ["fx"],
        },
    )

    row = enriched[0]
    assert row["asset_classes_36m"] == ["fx"]
    assert row["primary_asset_class_36m"] == "fx"
    assert row["estimated_avg_open_positions_36m"] is not None
    assert row["account_asset_class_allowed_flag_36m"] == 1.0
    assert row["account_estimated_avg_margin_load_pct_36m"] is not None
    assert row["account_estimated_peak_margin_load_pct_36m"] >= row["account_estimated_avg_margin_load_pct_36m"]


def test_build_sleeve_selection_reports_prefilter_counts() -> None:
    rows = [
        {
            "attempt_id": "A",
            "run_id": "run-a",
            "score_36m": 80.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "B",
            "run_id": "run-b",
            "score_36m": 79.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "C",
            "run_id": "run-c",
            "score_36m": 78.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    sleeve = pf.build_sleeve_selection(
        rows,
        {
            **pf.DEFAULT_SLEEVE_SPEC,
            "name": "quality",
            "prefilter_limit": 2,
            "shortlist_size": 1,
        },
    )

    assert len(sleeve["qualified_rows"]) == 3
    assert len(sleeve["candidate_rows"]) == 2
    assert sleeve["prefilter_excluded_count"] == 1
    assert sleeve["prefilter_limit"] == 2


def test_materialized_corpus_index_requires_fresh_manifest(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "run-a"
    run_dir.mkdir(parents=True, exist_ok=True)
    attempts_path = run_dir / "attempts.jsonl"
    attempts_path.write_text("{}", encoding="utf-8")
    (run_dir / "run-metadata.json").write_text("{}", encoding="utf-8")

    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    rows = [
        {"attempt_id": "B", "composite_score": 1.0},
        {"attempt_id": "A", "composite_score": 2.0},
    ]
    (derived_root / "attempt-catalog.json").write_text(
        json.dumps(rows, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (derived_root / "attempt-catalog-manifest.json").write_text(
        json.dumps(
            ar_main._attempt_catalog_manifest_payload([run_dir], rows),
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    config = SimpleNamespace(
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_manifest_path=derived_root / "attempt-catalog-manifest.json",
    )

    loaded_rows = ar_main._load_materialized_corpus_index_rows(config, run_dirs=[run_dir])
    assert [row["attempt_id"] for row in loaded_rows] == ["A", "B"]

    attempts_path.write_text("{}\n{}", encoding="utf-8")
    assert ar_main._load_materialized_corpus_index_rows(config, run_dirs=[run_dir]) is None


def test_merge_portfolio_sleeves_unions_and_labels_overlap() -> None:
    merged = pf.merge_portfolio_sleeves(
        [
            {
                "name": "quality",
                "selected_rows": [
                    {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                ],
                "candidate_rows": [
                    {"attempt_id": "A", "run_id": "run-a", "score_36m": 80.0},
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                ],
            },
            {
                "name": "cadence",
                "selected_rows": [
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                    {"attempt_id": "C", "run_id": "run-c", "score_36m": 69.0},
                ],
                "candidate_rows": [
                    {"attempt_id": "B", "run_id": "run-b", "score_36m": 70.0},
                    {"attempt_id": "C", "run_id": "run-c", "score_36m": 69.0},
                ],
            },
        ]
    )

    selected_rows = merged["selected_rows"]
    assert [row["attempt_id"] for row in selected_rows] == ["B", "A", "C"]
    assert selected_rows[0]["selected_by_sleeves"] == ["quality", "cadence"]
    assert selected_rows[0]["selected_by_sleeves_count"] == 2
    assert merged["selected_union_count"] == 3
    assert merged["selected_overlap_count"] == 1


def test_build_selection_basket_summary_uses_curve_terminal_values(tmp_path: Path) -> None:
    curve_path = tmp_path / "curve.json"
    curve_path.write_text(
        """
        {
          "curve": {
            "points": [
              {"date": "2026-01-01", "realized_r": 4.0},
              {"date": "2026-02-01", "realized_r": 9.0}
            ]
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    summary = ar_main._build_selection_basket_summary(
        [
            {
                "trades_per_month_36m": 2.5,
                "score_36m": 80.0,
                "max_drawdown_r_36m": 6.0,
                "effective_window_months_36m": 3.0,
                "full_backtest_curve_path_36m": str(curve_path),
            }
        ]
    )

    assert summary["strategy_count"] == 1
    assert summary["trades_per_month"]["sum"] == 2.5
    assert summary["realized_r_total_36m"]["sum"] == 9.0
    assert summary["realized_r_per_month_36m"]["mean"] == 3.0
    assert summary["max_drawdown_r_per_month_36m"]["mean"] == 2.0


def test_profile_drop_attempt_token_prefers_row_identity() -> None:
    token = ar_main._profile_drop_attempt_token(
        {
            "selection_rank": 1,
            "attempt_id": "RUN-attempt-00051",
            "candidate_name": "winner",
        },
        {
            "attempt_id": "RUN-attempt-00046",
            "candidate_name": "stale",
        },
    )

    assert token == "1-run-attempt-00051"


def test_nuke_deep_cache_artifacts_removes_rebuildable_outputs_only(
    tmp_path: Path,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    artifact_dir = runs_root / "run-a" / "evals" / "candidate-a"
    scrutiny_dir = artifact_dir / "scrutiny-cache" / "36mo"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    scrutiny_dir.mkdir(parents=True, exist_ok=True)
    derived_root.mkdir(parents=True, exist_ok=True)

    full_curve = artifact_dir / "full-backtest-36mo-curve.json"
    full_result = artifact_dir / "full-backtest-36mo-result.json"
    profile_drop_png = runs_root / "run-a" / "profile-drop-36mo.png"
    profile_drop_manifest = runs_root / "run-a" / "profile-drop-36mo.manifest.json"
    source_snapshot = artifact_dir / "sensitivity-response.json"
    derived_payload = derived_root / "portfolio-report.json"

    full_curve.write_text("{}", encoding="utf-8")
    full_result.write_text("{}", encoding="utf-8")
    (scrutiny_dir / "manifest.json").write_text("{}", encoding="utf-8")
    profile_drop_png.write_text("png", encoding="utf-8")
    profile_drop_manifest.write_text("{}", encoding="utf-8")
    source_snapshot.write_text('{"keep": true}', encoding="utf-8")
    derived_payload.write_text("{}", encoding="utf-8")

    summary = ar_main._nuke_deep_cache_artifacts(
        runs_root=runs_root,
        derived_root=derived_root,
        summary_timestamp="20260406T170000",
    )

    assert summary["deleted_full_backtest_curve_files"] == 1
    assert summary["deleted_full_backtest_result_files"] == 1
    assert summary["deleted_scrutiny_cache_dirs"] == 1
    assert summary["deleted_run_profile_drop_pngs"] == 1
    assert summary["deleted_run_profile_drop_manifests"] == 1
    assert summary["derived_entries_before_reset"] == 1
    assert full_curve.exists() is False
    assert full_result.exists() is False
    assert scrutiny_dir.parent.exists() is False
    assert profile_drop_png.exists() is False
    assert profile_drop_manifest.exists() is False
    assert source_snapshot.exists() is True
    assert derived_root.exists() is True
    assert (derived_root / "portfolio-report.json").exists() is False
    assert (derived_root / "deep-cache-reset-20260406T170000.json").exists() is True


def test_export_portfolio_bundle_uses_human_strategy_folders(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    report_root = derived_root / "portfolio-report" / "default-portfolio"
    report_root.mkdir(parents=True, exist_ok=True)

    profile_path = runs_root / "run-a" / "profiles" / "alpha.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_document = {
        "format": "legacy",
        "formatVersion": 99,
        "profile": {
            "name": "Alpha Prime",
            "version": "v7",
            "directionMode": "both",
            "indicators": [
                {
                    "meta": {
                        "id": "RSI",
                        "instanceId": "rsi-1",
                        "signalRole": "trigger",
                        "strategyRole": "mean-reversion",
                    },
                    "config": {"isActive": True},
                }
            ],
        },
    }
    profile_path.write_text(json.dumps(profile_document), encoding="utf-8")

    source_drop_root = report_root / "profile-drops" / "1-attempt-a-alpha"
    source_drop_root.mkdir(parents=True, exist_ok=True)
    source_drop_png = source_drop_root / "profile-drop-36mo.png"
    source_drop_manifest = source_drop_root / "profile-drop-36mo.manifest.json"
    _write_minimal_png(source_drop_png)
    assert ar_main._embed_profile_document_metadata_in_png(source_drop_png, profile_document) is True
    source_drop_manifest.write_text(
        '{"display_name":"Alpha Prime","tagline":"Fast FX cadence"}',
        encoding="utf-8",
    )

    report_path = report_root / "portfolio-report.json"
    report_path.write_text("{}", encoding="utf-8")

    payload = {
        "portfolio_name": "default-portfolio",
        "account": {
            "name": "Test Account",
            "account_size_usd": 1000.0,
            "leverage": 100.0,
            "risk_per_trade_pct": 1.0,
            "allowed_asset_classes": ["fx"],
        },
        "selected_basket_summary": {
            "trades_per_month": {"sum": 10.0},
        },
        "selected_deployment_summary": {
            "asset_class_counts": {"fx": 1},
            "account_estimated_avg_margin_load_pct_36m": {"sum": 4.0},
            "account_estimated_peak_margin_load_pct_36m": {"sum": 8.0},
            "avg_holding_hours_36m": {"mean": 12.0},
        },
        "selected_basket_curve_36m": {
            "final_equity_r": 5.0,
            "max_drawdown_r": 1.5,
            "final_drawdown_r": 0.0,
            "points": [
                {"date": "2024-01-01", "equity_r": 1.0, "drawdown_r": 0.0},
                {"date": "2024-01-02", "equity_r": 2.0, "drawdown_r": 0.5},
            ],
        },
        "selected": [
            {
                "portfolio_rank": 1,
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "profile_ref": "abc123",
                "primary_asset_class_36m": "fx",
                "score_36m": 90.0,
                "trades_per_month_36m": 10.0,
                "max_drawdown_r_36m": 1.5,
                "final_equity_r_36m": 5.0,
                "account_estimated_avg_margin_load_pct_36m": 4.0,
                "account_estimated_peak_margin_load_pct_36m": 8.0,
            }
        ],
        "profile_drops": [
            {
                "attempt_id": "attempt-a",
                "profile_ref": "abc123",
                "png_path": str(source_drop_png),
                "manifest_path": str(source_drop_manifest),
                "display_name": "Alpha Prime",
                "tagline": "Fast FX cadence",
                "short_description": "Short copy",
                "long_description": "Long copy",
            }
        ],
    }

    config = SimpleNamespace(derived_root=derived_root, runs_root=runs_root)
    summary = ar_main._export_portfolio_bundle(
        config=config,
        payload=payload,
        report_root=report_root,
        report_path=report_path,
    )

    bundle_root = Path(summary["bundle_root"])
    item_root = bundle_root / "Alpha-Prime"

    assert item_root.exists() is True
    assert (item_root / "Alpha-Prime.json").exists() is True
    assert (item_root / "Alpha-Prime.png").exists() is True
    exported_profile = ar_main.load_json_if_exists(item_root / "Alpha-Prime.json")
    assert exported_profile["format"] == "fuzzfolio.scoring-profile"
    assert exported_profile["formatVersion"] == 1
    assert exported_profile["profile"]["version"] == "v1"
    assert exported_profile["profile"]["indicators"][0]["meta"] == {
        "id": "RSI",
        "instanceId": "rsi-1",
    }
    embedded_exported_profile = ar_main._load_png_profile_document(item_root / "Alpha-Prime.png")
    assert embedded_exported_profile["profile"]["indicators"][0]["meta"] == {
        "id": "RSI",
        "instanceId": "rsi-1",
    }
    assert (item_root / "profile-drop-36mo.manifest.json").exists() is True
    assert (report_root / "portfolio-report.md").exists() is True
    assert (bundle_root / "portfolio-report.md").exists() is True
    assert (bundle_root / "portfolio-report.json").exists() is False
    assert (bundle_root / "portfolio-report.csv").exists() is False
    assert (bundle_root / "selected-attempts.csv").exists() is False
    assert (bundle_root / "bundle-manifest.json").exists() is False
    assert summary["exported_profiles"] == 1
    assert summary["exported_drop_pngs"] == 1
    assert summary["exported_drop_manifests"] == 1
    assert summary["bundle_markdown_path"] == str(bundle_root / "portfolio-report.md")
    assert summary["selected_rows"][0]["display_name"] == "Alpha Prime"
    assert summary["selected_rows"][0]["drop_manifest_export_path"] == str(
        item_root / "profile-drop-36mo.manifest.json"
    )
    markdown_text = (bundle_root / "portfolio-report.md").read_text(encoding="utf-8")
    assert "## Risk Scenarios" in markdown_text
    assert "## Growth Tracks" in markdown_text
    assert "Alpha Prime" in markdown_text


def test_portfolio_risk_scenarios_scale_growth_and_margin_load() -> None:
    payload = {
        "account": {
            "account_size_usd": 1000.0,
            "risk_per_trade_pct": 0.5,
        },
        "selected_deployment_summary": {
            "account_estimated_avg_margin_load_pct_36m": {"sum": 10.0},
            "account_estimated_peak_margin_load_pct_36m": {"sum": 20.0},
        },
        "selected_basket_curve_36m": {
            "points": [
                {"date": "2024-01-01", "equity_r": 1.0, "drawdown_r": 0.0},
                {"date": "2024-01-02", "equity_r": 2.0, "drawdown_r": 0.25},
            ]
        },
    }

    scenarios = ar_main._portfolio_risk_scenarios(payload)
    one_pct = next(item for item in scenarios if abs(item["risk_pct"] - 1.0) < 1e-9)

    assert abs(one_pct["avg_margin_load_pct"] - 20.0) < 1e-9
    assert abs(one_pct["peak_margin_load_pct"] - 40.0) < 1e-9
    assert abs(one_pct["final_balance"] - 1010.0) < 1e-6
    assert abs(one_pct["geometric_daily_return_pct"] - 0.498756211208895) < 1e-9
    assert one_pct["checkpoints"][30] == one_pct["final_balance"]


def test_portfolio_risk_scenarios_preserve_zero_balance_outcomes() -> None:
    payload = {
        "account": {
            "account_size_usd": 1000.0,
            "risk_per_trade_pct": 1.0,
        },
        "selected_basket_curve_36m": {
            "points": [
                {"date": "2024-01-01", "equity_r": 0.0, "drawdown_r": 0.0},
                {"date": "2024-01-02", "equity_r": -100.0, "drawdown_r": 100.0},
            ]
        },
    }

    scenarios = ar_main._portfolio_risk_scenarios(payload)
    one_pct = next(item for item in scenarios if abs(item["risk_pct"] - 1.0) < 1e-9)

    assert one_pct["final_balance"] == 0.0
    assert one_pct["final_return_pct"] == -100.0
    assert one_pct["checkpoints"][30] == 0.0


def test_render_portfolio_markdown_report_includes_growth_tracks() -> None:
    payload = {
        "generated_at": "2026-04-22T00:00:00-05:00",
        "portfolio_name": "tiny-live-fx",
        "account": {
            "name": "Tiny Live FX",
            "account_size_usd": 124.0,
            "leverage": 500.0,
            "risk_per_trade_pct": 0.75,
            "allowed_asset_classes": ["fx"],
            "blocked_asset_classes": [],
        },
        "selected_basket_summary": {
            "trades_per_month": {"sum": 12.5},
        },
        "selected_deployment_summary": {
            "asset_class_counts": {"fx": 2},
            "avg_holding_hours_36m": {"mean": 8.5},
            "account_estimated_avg_margin_load_pct_36m": {"sum": 0.5},
            "account_estimated_peak_margin_load_pct_36m": {"sum": 1.2},
        },
        "selected_basket_curve_36m": {
            "final_equity_r": 25.0,
            "max_drawdown_r": 3.0,
            "final_drawdown_r": 0.0,
            "points": [
                {"date": "2024-01-01", "equity_r": 1.0, "drawdown_r": 0.0},
                {"date": "2024-01-02", "equity_r": 1.5, "drawdown_r": 0.25},
                {"date": "2024-01-03", "equity_r": 3.0, "drawdown_r": 0.0},
            ],
        },
        "selected": [
            {
                "portfolio_rank": 1,
                "attempt_id": "attempt-a",
                "candidate_name": "alpha",
                "primary_asset_class_36m": "fx",
                "score_36m": 88.0,
                "trades_per_month_36m": 6.0,
                "max_drawdown_r_36m": 1.2,
                "final_equity_r_36m": 10.0,
                "account_estimated_avg_margin_load_pct_36m": 0.2,
                "account_estimated_peak_margin_load_pct_36m": 0.5,
            }
        ],
        "profile_drops": [
            {
                "attempt_id": "attempt-a",
                "display_name": "Alpha Prime",
            }
        ],
    }

    markdown = ar_main._render_portfolio_markdown_report(
        payload,
        report_path=Path("C:/tmp/portfolio-report.json"),
    )

    assert "# tiny-live-fx Portfolio Report" in markdown
    assert "## Portfolio Snapshot" in markdown
    assert "## Risk Scenarios" in markdown
    assert "## Growth Tracks" in markdown
    assert "## Selected Profiles" in markdown
    assert "Scenario horizon" in markdown
    assert "Geom day" in markdown
    assert "Arith mean day" in markdown
    assert "0.50%" in markdown
    assert "30d" in markdown
    assert "Alpha Prime" in markdown


def test_render_portfolio_profile_drops_updates_existing_report(
    tmp_path: Path, monkeypatch
) -> None:
    derived_root = tmp_path / "runs" / "derived"
    report_root = derived_root / "portfolio-report" / "default-portfolio"
    report_root.mkdir(parents=True, exist_ok=True)
    report_path = report_root / "portfolio-report.json"
    payload = {
        "portfolio_name": "default-portfolio",
        "portfolio_spec": {
            "generate_profile_drops": False,
            "profile_drop_lookback_months": 36,
            "profile_drop_timeout_seconds": 1800,
            "profile_drop_workers": 3,
        },
        "profile_drop_phase": "skipped",
        "profile_drops": [],
        "selected": [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
            }
        ],
    }
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    config = SimpleNamespace(
        derived_root=derived_root,
        repo_root=tmp_path,
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    seen: dict[str, object] = {}

    def fake_render_profile_drop_rows(**kwargs):
        seen.update(kwargs)
        return [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "rendered",
                "png_path": str(report_root / "profile-drops" / "alpha" / "profile-drop-36mo.png"),
                "manifest_path": str(
                    report_root / "profile-drops" / "alpha" / "profile-drop-36mo.manifest.json"
                ),
            }
        ]

    monkeypatch.setattr(ar_main, "_render_profile_drop_rows", fake_render_profile_drop_rows)

    exit_code = ar_main.cmd_render_portfolio_profile_drops(
        portfolio_report=str(report_path),
        profile_drop_workers=None,
        force_rebuild=False,
        as_json=True,
    )

    assert exit_code == 0
    assert seen["rows"] == payload["selected"]
    assert seen["output_root"] == report_root / "profile-drops"
    assert seen["lookback_months"] == 36
    assert seen["timeout_seconds"] == 1800
    assert seen["profile_drop_workers"] == 3
    assert seen["force_rebuild"] is False
    assert seen["progress_label"] == "portfolio profile drops"

    updated_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert updated_payload["profile_drop_phase"] == "complete"
    assert len(updated_payload["profile_drops"]) == 1
    assert updated_payload["profile_drops"][0]["status"] == "rendered"
    assert "generated_at" in updated_payload


def test_cmd_build_portfolio_continues_when_catch_up_reports_failures(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    derived_root = tmp_path / "runs" / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    attempt_catalog_summary_path = derived_root / "attempt-catalog-summary.json"
    full_backtest_failures_json_path = derived_root / "full-backtest-failures.json"
    attempt_catalog_summary_path.write_text(
        json.dumps({"attempt_count": 1}, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    full_backtest_failures_json_path.write_text(
        json.dumps({"failed_count": 7}, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    config = SimpleNamespace(
        repo_root=tmp_path,
        derived_root=derived_root,
        attempt_catalog_summary_path=attempt_catalog_summary_path,
        full_backtest_failures_json_path=full_backtest_failures_json_path,
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    portfolio_spec = {
        "portfolio_name": "default-portfolio",
        "catch_up_full_backtests": True,
        "catch_up_force_rebuild": False,
        "catch_up_require_scrutiny_36": False,
        "generate_profile_drops": False,
        "export_bundle": False,
        "sleeves": [{"name": "quality"}],
    }
    load_spec_calls: list[Path] = []

    def fake_load_portfolio_build_specs(path: Path):
        load_spec_calls.append(path)
        return [portfolio_spec], False

    monkeypatch.setattr(
        ar_main,
        "load_portfolio_build_specs",
        fake_load_portfolio_build_specs,
    )
    monkeypatch.setattr(
        ar_main,
        "cmd_calculate_full_backtests",
        lambda **_kwargs: 1,
    )

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "profile_ref": "profile-a",
            "score_36m": 88.0,
            "composite_score": 88.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        }
    ]
    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized", "row_count": 1}),
    )

    monkeypatch.setattr(
        ar_main,
        "build_sleeve_prefilter",
        lambda _rows, sleeve_spec: {
            "name": sleeve_spec.get("name"),
            "spec": sleeve_spec,
            "qualified_rows": list(rows),
            "candidate_rows": list(rows),
            "prefilter_limit": 1,
            "prefilter_excluded_count": 0,
            "filter_rejections": {},
        },
    )
    monkeypatch.setattr(ar_main, "build_candidate_similarity_payload", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        ar_main,
        "finalize_sleeve_selection",
        lambda sleeve, similarity_payload=None: {
            **sleeve,
            "selected_rows": list(rows),
            "board": {"alternates": []},
        },
    )

    def fake_merge_portfolio_sleeves(items):
        candidate_rows: list[dict[str, object]] = []
        selected_rows: list[dict[str, object]] = []
        for item in items:
            candidate_rows.extend(list(item.get("candidate_rows") or []))
            selected_rows.extend(list(item.get("selected_rows") or []))
        return {
            "candidate_rows": candidate_rows,
            "selected_rows": selected_rows,
            "selected_overlap_count": 0,
        }

    monkeypatch.setattr(ar_main, "merge_portfolio_sleeves", fake_merge_portfolio_sleeves)
    monkeypatch.setattr(ar_main, "subset_similarity_payload", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(ar_main, "render_attempt_tradeoff_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_attempt_tradeoff_overlay_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_attempt_drawdown_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_similarity_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_similarity_heatmap_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "_build_selection_basket_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(ar_main, "_build_selection_basket_curve", lambda *_args, **_kwargs: {})

    exit_code = ar_main.cmd_build_portfolio(
        run_ids=None,
        attempt_ids=None,
        portfolio_config=None,
        catch_up_full_backtests=None,
        catch_up_force_rebuild=None,
        catch_up_require_scrutiny_36=None,
        generate_profile_drops=None,
        export_bundle=None,
        profile_drop_workers=None,
        profile_drop_exit_policy_cell=None,
        as_json=True,
    )

    assert exit_code == 0
    assert load_spec_calls == [tmp_path / "portfolio.account-presets.json"]

    report_path = derived_root / "portfolio-report" / "default-portfolio" / "portfolio-report.json"
    assert report_path.exists() is True
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_payload["catch_up_summary"]["exit_code"] == 1
    assert report_payload["catch_up_summary"]["status"] == "partial_failure"
    assert report_payload["selected_union_count"] == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["catch_up_exit_code"] == 1
    assert payload["catch_up_status"] == "partial_failure"
    assert payload["selected_union_count"] == 1


def test_cmd_build_portfolio_honors_config_selected_attempt_ids(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    derived_root = tmp_path / "runs" / "derived"
    derived_root.mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(
        repo_root=tmp_path,
        derived_root=derived_root,
        attempt_catalog_summary_path=derived_root / "attempt-catalog-summary.json",
        full_backtest_failures_json_path=derived_root / "full-backtest-failures.json",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    portfolio_spec = {
        "portfolio_name": "optimizer-config",
        "catch_up_full_backtests": False,
        "generate_profile_drops": False,
        "export_bundle": False,
        "candidate_scope": "all",
        "selected_attempt_ids": ["attempt-b"],
        "sleeves": [{"name": "optimizer", "shortlist_size": 10, "candidate_limit": -1}],
    }
    monkeypatch.setattr(
        ar_main,
        "load_portfolio_build_specs",
        lambda _path: ([portfolio_spec], False),
    )
    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "profile_ref": "profile-a",
            "score_36m": 91.0,
            "composite_score": 91.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "attempt-b",
            "run_id": "run-b",
            "candidate_name": "bravo",
            "profile_ref": "profile-b",
            "score_36m": 72.0,
            "composite_score": 72.0,
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]
    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized", "row_count": 2}),
    )

    def fake_build_sleeve_prefilter(filtered_rows, sleeve_spec):
        assert [row["attempt_id"] for row in filtered_rows] == ["attempt-b"]
        return {
            "name": sleeve_spec.get("name"),
            "spec": sleeve_spec,
            "qualified_rows": list(filtered_rows),
            "candidate_rows": list(filtered_rows),
            "prefilter_limit": 1,
            "prefilter_excluded_count": 0,
            "filter_rejections": {},
        }

    monkeypatch.setattr(ar_main, "build_sleeve_prefilter", fake_build_sleeve_prefilter)
    monkeypatch.setattr(ar_main, "build_candidate_similarity_payload", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        ar_main,
        "finalize_sleeve_selection",
        lambda sleeve, similarity_payload=None: {
            **sleeve,
            "selected_rows": list(sleeve.get("candidate_rows") or []),
            "board": {"alternates": []},
        },
    )
    monkeypatch.setattr(
        ar_main,
        "merge_portfolio_sleeves",
        lambda items: {
            "candidate_rows": list(items[0].get("candidate_rows") or []),
            "selected_rows": list(items[0].get("selected_rows") or []),
            "selected_overlap_count": 0,
        },
    )
    monkeypatch.setattr(ar_main, "subset_similarity_payload", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(ar_main, "render_attempt_tradeoff_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_attempt_tradeoff_overlay_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_attempt_drawdown_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_similarity_scatter_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "render_similarity_heatmap_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(ar_main, "_build_selection_basket_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(ar_main, "_build_selection_basket_curve", lambda *_args, **_kwargs: {})

    exit_code = ar_main.cmd_build_portfolio(
        run_ids=None,
        attempt_ids=None,
        portfolio_config=None,
        catch_up_full_backtests=None,
        catch_up_force_rebuild=None,
        catch_up_require_scrutiny_36=None,
        generate_profile_drops=None,
        export_bundle=None,
        profile_drop_workers=None,
        profile_drop_exit_policy_cell=None,
        as_json=True,
    )

    assert exit_code == 0
    report_path = derived_root / "portfolio-report" / "optimizer-config" / "portfolio-report.json"
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_payload["candidate_scope"]["candidate_scope"] == "config_selected_attempts"
    assert report_payload["candidate_scope"]["matched_attempt_count"] == 1
    assert [row["attempt_id"] for row in report_payload["selected"]] == ["attempt-b"]
    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_union_count"] == 1


def test_build_parser_rejects_removed_leaf_commands() -> None:
    parser = ar_main.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["render-corpus-profile-drops"])


def test_build_parser_includes_finalize_corpus_defaults() -> None:
    parser = ar_main.build_parser()

    args = parser.parse_args(["finalize-corpus"])

    assert args.command == "finalize-corpus"
    assert args.scope == "dashboard"
    assert args.lookback_months == 36
    assert args.profile_drop_workers == 4
    assert args.profile_drop_timeout_seconds == 1800
    assert args.force_rebuild is False
    assert args.allow_presentation_fallback is False
    assert args.dry_run is False
    assert args.run_id is None
    assert args.attempt_id is None


def test_cmd_finalize_corpus_uses_dashboard_visible_attempts(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    rows = [
        {
            "run_id": "playhand-run",
            "attempt_id": "playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "score_36m": 95.0,
            "composite_score": 95.0,
            "canonical_attempt_id": "playhand-run-attempt-00002",
        },
        {
            "run_id": "playhand-run",
            "attempt_id": "playhand-run-attempt-00002",
            "runner": "play_hand_v1",
            "score_36m": 80.0,
            "composite_score": 80.0,
            "canonical_attempt_id": "playhand-run-attempt-00002",
            "is_canonical_playhand_attempt": True,
        },
        {
            "run_id": "explorer-run",
            "attempt_id": "explorer-run-attempt-00001",
            "score_36m": 65.0,
            "composite_score": 70.0,
        },
        {
            "run_id": "explorer-run",
            "attempt_id": "explorer-run-attempt-00002",
            "score_36m": 75.0,
            "composite_score": 72.0,
        },
    ]
    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized"}),
    )
    render_calls: list[dict[str, object]] = []

    def fake_render_corpus_profile_drops(**kwargs):
        render_calls.append(kwargs)
        print(
            json.dumps(
                {
                    "selected_count": len(kwargs["attempt_ids"]),
                    "profile_drop_rendered": len(kwargs["attempt_ids"]),
                    "profile_drop_cached": 0,
                    "profile_drop_failed": 0,
                }
            )
        )
        return 0

    monkeypatch.setattr(
        ar_main, "cmd_render_corpus_profile_drops", fake_render_corpus_profile_drops
    )
    monkeypatch.setattr(
        ar_main,
        "_refresh_global_derived_corpus_state",
        lambda _config: {"attempt_count": len(rows)},
    )

    exit_code = ar_main.cmd_finalize_corpus(
        run_ids=None,
        attempt_ids=None,
        scope="dashboard",
        lookback_months=36,
        profile_drop_workers=2,
        profile_drop_timeout_seconds=90,
        force_rebuild=False,
        require_presentation_metadata=True,
        dry_run=False,
        as_json=True,
        full_backtest_workers=12,
    )

    assert exit_code == 0
    assert render_calls[0]["attempt_ids"] == [
        "playhand-run-attempt-00002",
        "explorer-run-attempt-00002",
    ]
    assert render_calls[0]["profile_drop_workers"] == 2
    assert render_calls[0]["full_backtest_workers"] == 12
    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_count"] == 2
    assert payload["selection"]["selection_scope"] == "dashboard"
    assert payload["selection"]["canonical_run_count"] == 1
    assert payload["selection"]["score_selected_run_count"] == 1
    assert payload["refresh_summary"]["attempt_count"] == 4


def test_cmd_finalize_corpus_skips_tombstoned_runs(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    rows = [
        {
            "run_id": "dead-playhand-run",
            "attempt_id": "dead-playhand-run-attempt-00001",
            "runner": "play_hand_v1",
            "attempt_role": "instrument_scout",
            "score_36m": 88.0,
            "composite_score": 88.0,
        },
        {
            "run_id": "dead-playhand-run",
            "attempt_id": "dead-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 0.0,
            "composite_score": 0.0,
            "attempt_decision": "tombstoned",
            "attempt_decision_reasons": ["final_36mo_scrutiny_failed"],
        },
        {
            "run_id": "live-playhand-run",
            "attempt_id": "live-playhand-run-attempt-00011",
            "runner": "play_hand_v1",
            "attempt_role": "final",
            "score_36m": 62.0,
            "composite_score": 62.0,
            "is_canonical_playhand_attempt": True,
        },
    ]
    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized"}),
    )
    render_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        ar_main,
        "cmd_render_corpus_profile_drops",
        lambda **kwargs: render_calls.append(kwargs) or print("{}") or 0,
    )
    monkeypatch.setattr(
        ar_main,
        "_refresh_global_derived_corpus_state",
        lambda _config: {"attempt_count": len(rows)},
    )

    exit_code = ar_main.cmd_finalize_corpus(
        run_ids=None,
        attempt_ids=None,
        scope="dashboard",
        lookback_months=36,
        profile_drop_workers=2,
        profile_drop_timeout_seconds=90,
        force_rebuild=False,
        require_presentation_metadata=True,
        dry_run=False,
        as_json=True,
        full_backtest_workers=None,
    )

    assert exit_code == 0
    assert render_calls[0]["attempt_ids"] == ["live-playhand-run-attempt-00011"]
    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_count"] == 1
    assert payload["selection"]["tombstoned_run_count"] == 1
    assert payload["selection"]["tombstoned_dropped_count"] == 2


def test_cmd_render_corpus_profile_drops_heals_selected_attempts_before_render(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    first_rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 90.0,
            "composite_score": 80.0,
            "artifact_dir": str(tmp_path / "runs" / "run-a" / "evals" / "alpha"),
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
        },
        {
            "attempt_id": "attempt-b",
            "run_id": "run-b",
            "candidate_name": "beta",
            "score_36m": 70.0,
            "composite_score": 65.0,
            "artifact_dir": str(tmp_path / "runs" / "run-b" / "evals" / "beta"),
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]
    refreshed_rows = [
        {
            **first_rows[0],
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        first_rows[1],
    ]
    selection_calls = {"count": 0}

    def fake_selection_corpus_rows(*_args, **_kwargs):
        selection_calls["count"] += 1
        rows = first_rows if selection_calls["count"] == 1 else refreshed_rows
        return rows, {"source": "materialized"}

    monkeypatch.setattr(ar_main, "_selection_corpus_rows", fake_selection_corpus_rows)

    catchup_calls: list[dict[str, object]] = []

    def fake_calculate_full_backtests(**kwargs):
        catchup_calls.append(kwargs)
        return 0

    monkeypatch.setattr(ar_main, "cmd_calculate_full_backtests", fake_calculate_full_backtests)

    render_calls: list[dict[str, object]] = []

    def fake_render_profile_drop_rows(**kwargs):
        render_calls.append(kwargs)
        return [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "rendered",
                "png_path": str(tmp_path / "runs" / "run-a" / "evals" / "alpha" / "profile-drop-36mo.png"),
                "manifest_path": str(
                    tmp_path / "runs" / "run-a" / "evals" / "alpha" / "profile-drop-36mo.manifest.json"
                ),
            }
        ]

    monkeypatch.setattr(ar_main, "_render_profile_drop_rows", fake_render_profile_drop_rows)

    exit_code = ar_main.cmd_render_corpus_profile_drops(
        run_ids=None,
        attempt_ids=None,
        top_results=1,
        rank_start=0,
        lookback_months=36,
        profile_drop_workers=3,
        profile_drop_timeout_seconds=90,
        force_rebuild=False,
        full_backtest_workers=17,
        as_json=True,
    )

    assert exit_code == 0
    assert len(catchup_calls) == 1
    assert catchup_calls[0]["attempt_ids"] == ["attempt-a"]
    assert catchup_calls[0]["max_workers"] == 17
    assert catchup_calls[0]["force_rebuild"] is False
    assert catchup_calls[0]["require_scrutiny_36"] is False
    assert selection_calls["count"] == 2
    assert len(render_calls) == 1
    assert render_calls[0]["layout_mode"] == "attempt_local"
    assert render_calls[0]["output_root"] is None
    assert [row["attempt_id"] for row in render_calls[0]["rows"]] == ["attempt-a"]
    assert render_calls[0]["rows"][0]["full_backtest_validation_status_36m"] == "valid"

    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_count"] == 1
    assert payload["rank_start"] == 0
    assert payload["healed_full_backtests"] == 1
    assert payload["profile_drop_rendered"] == 1
    assert payload["profile_drop_cached"] == 0
    assert payload["profile_drop_failed"] == 0


def test_cmd_render_corpus_profile_drops_streams_catch_up_progress_in_plain_mode(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 90.0,
            "composite_score": 80.0,
            "artifact_dir": str(tmp_path / "runs" / "run-a" / "evals" / "alpha"),
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
        }
    ]
    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized"}),
    )

    catchup_calls: list[dict[str, object]] = []

    def fake_calculate_full_backtests(**kwargs):
        catchup_calls.append(kwargs)
        print("queue 1/1 streamed-catchup")
        print("  done: run-a attempt-a")
        return 0

    monkeypatch.setattr(ar_main, "cmd_calculate_full_backtests", fake_calculate_full_backtests)
    monkeypatch.setattr(
        ar_main,
        "_render_profile_drop_rows",
        lambda **_kwargs: [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "skipped",
                "reason": "test",
            }
        ],
    )

    exit_code = ar_main.cmd_render_corpus_profile_drops(
        run_ids=None,
        attempt_ids=["attempt-a"],
        top_results=None,
        rank_start=0,
        lookback_months=36,
        profile_drop_workers=3,
        profile_drop_timeout_seconds=90,
        force_rebuild=False,
        as_json=False,
    )

    assert exit_code == 0
    assert catchup_calls[0]["as_json"] is False
    assert catchup_calls[0]["emit_summary"] is False
    captured = capsys.readouterr()
    assert "queue 1/1 streamed-catchup" in captured.out
    assert "done: run-a attempt-a" in captured.out
    assert "full-backtest catch-up complete" in captured.out


def test_cmd_render_corpus_profile_drops_continues_when_catch_up_returns_nonzero(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 90.0,
            "composite_score": 80.0,
            "artifact_dir": str(tmp_path / "runs" / "run-a" / "evals" / "alpha"),
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
        }
    ]

    selection_calls = {"count": 0}

    def fake_selection_corpus_rows(*_args, **_kwargs):
        selection_calls["count"] += 1
        return rows, {"source": "materialized"}

    monkeypatch.setattr(ar_main, "_selection_corpus_rows", fake_selection_corpus_rows)
    monkeypatch.setattr(
        ar_main,
        "cmd_calculate_full_backtests",
        lambda **_kwargs: 1,
    )

    render_calls: list[dict[str, object]] = []

    def fake_render_profile_drop_rows(**kwargs):
        render_calls.append(kwargs)
        return [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "failed",
                "error": "missing full-backtest artifacts",
            }
        ]

    monkeypatch.setattr(ar_main, "_render_profile_drop_rows", fake_render_profile_drop_rows)

    exit_code = ar_main.cmd_render_corpus_profile_drops(
        run_ids=None,
        attempt_ids=None,
        top_results=1,
        rank_start=0,
        lookback_months=36,
        profile_drop_workers=2,
        profile_drop_timeout_seconds=60,
        force_rebuild=False,
        as_json=True,
    )

    assert exit_code == 1
    assert selection_calls["count"] == 2
    assert len(render_calls) == 1
    assert render_calls[0]["layout_mode"] == "attempt_local"

    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_count"] == 1
    assert payload["rank_start"] == 0
    assert payload["full_backtest_catch_up_exit_code"] == 1
    assert payload["status"] == "partial_failure"
    assert payload["profile_drop_rendered"] == 0
    assert payload["profile_drop_cached"] == 0
    assert payload["profile_drop_failed"] == 1


def test_cmd_render_corpus_profile_drops_fails_when_catch_up_fails_even_if_drop_renders(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 90.0,
            "composite_score": 80.0,
            "artifact_dir": str(tmp_path / "runs" / "run-a" / "evals" / "alpha"),
            "has_full_backtest_36m": False,
            "full_backtest_validation_status_36m": "missing",
        }
    ]

    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized"}),
    )
    monkeypatch.setattr(
        ar_main,
        "cmd_calculate_full_backtests",
        lambda **_kwargs: 1,
    )
    monkeypatch.setattr(
        ar_main,
        "_render_profile_drop_rows",
        lambda **_kwargs: [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "status": "rendered",
                "png_path": str(
                    tmp_path / "runs" / "run-a" / "evals" / "alpha" / "profile-drop-36mo.png"
                ),
                "manifest_path": str(
                    tmp_path
                    / "runs"
                    / "run-a"
                    / "evals"
                    / "alpha"
                    / "profile-drop-36mo.manifest.json"
                ),
            }
        ],
    )

    exit_code = ar_main.cmd_render_corpus_profile_drops(
        run_ids=None,
        attempt_ids=None,
        top_results=1,
        rank_start=0,
        lookback_months=36,
        profile_drop_workers=2,
        profile_drop_timeout_seconds=60,
        force_rebuild=False,
        as_json=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["status"] == "partial_failure"
    assert payload["full_backtest_catch_up_exit_code"] == 1
    assert payload["profile_drop_rendered"] == 1
    assert payload["profile_drop_failed"] == 0


def test_cmd_render_corpus_profile_drops_respects_rank_start(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        fuzzfolio=SimpleNamespace(),
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    rows = [
        {
            "attempt_id": "attempt-a",
            "run_id": "run-a",
            "candidate_name": "alpha",
            "score_36m": 95.0,
            "composite_score": 95.0,
            "artifact_dir": str(tmp_path / "runs" / "run-a" / "evals" / "alpha"),
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "attempt-b",
            "run_id": "run-b",
            "candidate_name": "beta",
            "score_36m": 85.0,
            "composite_score": 85.0,
            "artifact_dir": str(tmp_path / "runs" / "run-b" / "evals" / "beta"),
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
        {
            "attempt_id": "attempt-c",
            "run_id": "run-c",
            "candidate_name": "gamma",
            "score_36m": 75.0,
            "composite_score": 75.0,
            "artifact_dir": str(tmp_path / "runs" / "run-c" / "evals" / "gamma"),
            "has_full_backtest_36m": True,
            "full_backtest_validation_status_36m": "valid",
        },
    ]

    monkeypatch.setattr(
        ar_main,
        "_selection_corpus_rows",
        lambda *_args, **_kwargs: (rows, {"source": "materialized"}),
    )
    monkeypatch.setattr(
        ar_main,
        "cmd_calculate_full_backtests",
        lambda **_kwargs: 0,
    )

    render_calls: list[dict[str, object]] = []

    def fake_render_profile_drop_rows(**kwargs):
        render_calls.append(kwargs)
        return []

    monkeypatch.setattr(ar_main, "_render_profile_drop_rows", fake_render_profile_drop_rows)

    exit_code = ar_main.cmd_render_corpus_profile_drops(
        run_ids=None,
        attempt_ids=None,
        top_results=1,
        rank_start=1,
        lookback_months=36,
        profile_drop_workers=2,
        profile_drop_timeout_seconds=60,
        force_rebuild=False,
        as_json=True,
    )

    assert exit_code == 0
    assert len(render_calls) == 1
    assert [row["attempt_id"] for row in render_calls[0]["rows"]] == ["attempt-b"]

    payload = json.loads(capsys.readouterr().out)
    assert payload["selected_count"] == 1
    assert payload["rank_start"] == 1


def test_render_profile_drop_rows_emits_plain_progress(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        fuzzfolio=SimpleNamespace(),
    )
    run_dir = tmp_path / "runs" / "run-a"
    run_dir.mkdir(parents=True)
    png_path = tmp_path / "profile-drop-36mo.png"
    manifest_path = tmp_path / "profile-drop-36mo.manifest.json"
    png_path.write_bytes(b"png")
    manifest_path.write_text("{}", encoding="utf-8")
    row = {
        "attempt_id": "attempt-a",
        "run_id": "run-a",
        "candidate_name": "alpha",
    }
    attempt = {
        "attempt_id": "attempt-a",
        "candidate_name": "alpha",
        "best_summary": {
            "best_cell": {"stop_loss_percent": 0.1, "reward_multiple": 2.0},
            "matrix_summary": {},
        },
    }

    class FakeCli:
        def __init__(self, *_args, **_kwargs):
            pass

        def ensure_login(self) -> None:
            return None

    monkeypatch.setattr(ar_main, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(
        ar_main,
        "_resolve_drop_renderer_executable",
        lambda _config: (tmp_path / "renderer.exe", tmp_path),
    )
    monkeypatch.setattr(
        ar_main,
        "_matched_attempt_items",
        lambda *_args, **_kwargs: [(run_dir, [attempt], attempt)],
    )
    monkeypatch.setattr(
        ar_main,
        "_render_profile_drop_for_attempt",
        lambda **_kwargs: {
            "status": "cached",
            "png_path": str(png_path),
            "manifest_path": str(manifest_path),
            "profile_ref": "profile-ref",
        },
    )

    results = ar_main._render_profile_drop_rows(
        config=config,
        rows=[row],
        output_root=None,
        lookback_months=36,
        timeout_seconds=60,
        force_rebuild=False,
        profile_drop_workers=1,
        as_json=False,
        progress_label="corpus profile drops",
        layout_mode="attempt_local",
    )

    assert results[0]["status"] == "cached"
    captured = capsys.readouterr()
    assert "[corpus profile drops] selected=1 queued=1" in captured.out
    assert "[corpus profile drops] 1/1 complete (100.0%)" in captured.out
    assert "cached=1" in captured.out
    assert captured.err == ""


def test_render_profile_drop_rows_skips_attempt_without_renderable_exit_cell(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        fuzzfolio=SimpleNamespace(),
    )
    run_dir = tmp_path / "runs" / "run-empty"
    artifact_dir = run_dir / "evals" / "final"
    artifact_dir.mkdir(parents=True)
    row = {
        "attempt_id": "attempt-empty",
        "run_id": "run-empty",
        "candidate_name": "empty",
        "full_backtest_validation_status_36m": "missing",
    }
    attempt = {
        "attempt_id": "attempt-empty",
        "candidate_name": "empty",
        "artifact_dir": str(artifact_dir),
        "best_summary": {
            "best_cell": None,
            "matrix_summary": {"robust_cell": None},
            "signal_count": 0,
        },
    }

    class FakeCli:
        def __init__(self, *_args, **_kwargs):
            pass

        def ensure_login(self) -> None:
            return None

    monkeypatch.setattr(ar_main, "FuzzfolioCli", FakeCli)
    monkeypatch.setattr(
        ar_main,
        "_resolve_drop_renderer_executable",
        lambda _config: (tmp_path / "renderer.exe", tmp_path),
    )
    monkeypatch.setattr(
        ar_main,
        "_matched_attempt_items",
        lambda *_args, **_kwargs: [(run_dir, [attempt], attempt)],
    )
    monkeypatch.setattr(
        ar_main,
        "_render_profile_drop_for_attempt",
        lambda **_kwargs: pytest.fail("unrenderable attempts should be skipped before rendering"),
    )

    results = ar_main._render_profile_drop_rows(
        config=config,
        rows=[row],
        output_root=None,
        lookback_months=36,
        timeout_seconds=60,
        force_rebuild=False,
        profile_drop_workers=1,
        as_json=False,
        progress_label="corpus profile drops",
        layout_mode="attempt_local",
    )

    assert results == [
        {
            "attempt_id": "attempt-empty",
            "run_id": "run-empty",
            "candidate_name": "empty",
            "status": "skipped",
            "reason": "no_backtestable_exit_cell",
        }
    ]
    captured = capsys.readouterr()
    assert "[corpus profile drops] selected=1 queued=0" in captured.out
    assert "skipped=1" in captured.out
    assert "failed=0" in captured.out


def test_render_profile_drop_rows_preflights_required_metadata_provider(
    tmp_path: Path, monkeypatch
) -> None:
    config = SimpleNamespace(
        repo_root=tmp_path,
        fuzzfolio=SimpleNamespace(),
        research=SimpleNamespace(presentation_metadata_provider_profile="writer"),
        providers={"writer": SimpleNamespace(type="codex")},
    )

    class FakeProvider:
        def complete_json(self, _messages):
            raise ar_main.ProviderError("writer offline")

    class UnexpectedCli:
        def __init__(self, *_args, **_kwargs):
            pytest.fail("Fuzzfolio CLI should not initialize when metadata preflight fails")

    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: FakeProvider())
    monkeypatch.setattr(ar_main, "FuzzfolioCli", UnexpectedCli)

    with pytest.raises(RuntimeError, match="Presentation metadata provider preflight failed"):
        ar_main._render_profile_drop_rows(
            config=config,
            rows=[
                {
                    "attempt_id": "attempt-a",
                    "run_id": "run-a",
                    "candidate_name": "alpha",
                }
            ],
            output_root=None,
            lookback_months=36,
            timeout_seconds=60,
            force_rebuild=False,
            profile_drop_workers=1,
            as_json=False,
            progress_label="corpus profile drops",
            layout_mode="attempt_local",
            require_presentation_metadata=True,
        )


def test_generate_presentation_metadata_repairs_invalid_writer_copy(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[list[ar_main.ChatMessage]] = []

    class RepairingProvider:
        def complete_json(self, messages):
            calls.append(messages)
            if len(calls) == 1:
                return {
                    "display_name": "Gold Pullback",
                    "tagline": "Trend context with pullback entries.",
                    "short_description": "Trades XAUUSD pullbacks when trend and trigger agree.",
                    "long_description": "Too short.",
                }
            return {
                "display_name": "Gold Pullback",
                "tagline": "Trend context with pullback entries.",
                "short_description": "Trades XAUUSD pullbacks when trend and trigger agree.",
                "long_description": (
                    "Reads the broader gold trend first, then waits for a brief pullback and a clear "
                    "M15 trigger. It trades only when context and timing agree."
                ),
            }

        def close(self):
            return None

    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: RepairingProvider())
    config = SimpleNamespace(
        providers={"writer": SimpleNamespace(type="codex")},
        research=SimpleNamespace(presentation_metadata_provider_profile="writer"),
    )
    metadata_path = tmp_path / "presentation.json"

    result = ar_main._generate_presentation_metadata(
        config=config,
        run_dir=tmp_path / "run-a",
        row={"attempt_id": "attempt-a", "candidate_name": "candidate"},
        attempt={"attempt_id": "attempt-a", "candidate_name": "candidate"},
        package_inputs={"timeframe": "M15", "instruments": ["XAUUSD"]},
        lookback_months=36,
        profile_ref="profile-a",
        profile_document_payload={
            "profile": {
                "name": "candidate",
                "description": "Portable scoring profile scaffolded from live indicator templates.",
                "directionMode": "both",
                "indicators": [],
            }
        },
        presentation_signature="sig-a",
        metadata_artifact_path=metadata_path,
        emit=None,
    )

    assert result is not None
    assert len(calls) == 2
    assert "Validation failures" in calls[1][-1].content
    assert result["display_name"] == "Gold Pullback"
    assert metadata_path.exists() is True


def test_public_portfolio_row_strips_path_fields() -> None:
    public = ar_main._public_portfolio_row(
        {
            "attempt_id": "attempt-a",
            "candidate_name": "alpha",
            "profile_ref": "ref-a",
            "profile_path": r"C:\runs\run-a\profiles\alpha.json",
            "destination_path": r"C:\runs\run-a\profiles\beta.json",
            "source_profile_path": r"C:\runs\run-a\profiles\alpha.json",
        }
    )

    assert public == {
        "attempt_id": "attempt-a",
        "candidate_name": "alpha",
        "profile_ref": "ref-a",
    }


def test_human_bundle_item_token_avoids_collisions() -> None:
    used: set[str] = set()

    first = ar_main._human_bundle_item_token("alpha", "attempt-a", used)
    second = ar_main._human_bundle_item_token("alpha", "attempt-b", used)

    assert first == "alpha"
    assert second == "alpha-2"


def test_portable_profile_document_for_import_strips_rich_indicator_meta() -> None:
    payload = {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {
            "name": "Scaffold " + ("x" * 160),
            "version": "v9",
            "indicators": [
                {
                    "meta": {
                        "id": "RSI",
                        "instanceId": "rsi-1",
                        "signalRole": "trigger",
                        "strategyRole": "mean-reversion",
                    },
                    "config": {},
                }
            ],
        },
    }

    portable = ar_main._portable_profile_document_for_import(payload)

    assert portable["format"] == "fuzzfolio.scoring-profile"
    assert portable["formatVersion"] == 1
    assert portable["profile"]["version"] == "v1"
    assert portable["profile"]["name"] == "x" * 120
    assert portable["profile"]["indicators"][0]["meta"] == {
        "id": "RSI",
        "instanceId": "rsi-1",
    }


def test_embed_profile_document_metadata_in_png_rewrites_import_payload(tmp_path: Path) -> None:
    png_path = tmp_path / "profile-drop-36mo.png"
    _write_minimal_png(png_path)
    payload = {
        "profile": {
            "name": "Momentum Pullback",
            "version": "v5",
            "indicators": [
                {
                    "meta": {
                        "id": "TRENDFLEX",
                        "instanceId": "trendflex-1",
                        "preferredTimeframeRole": "context",
                        "signalPersistence": "sparse",
                    },
                    "config": {"timeframe": "H1"},
                }
            ],
        },
    }

    assert ar_main._embed_profile_document_metadata_in_png(png_path, payload) is True

    embedded = ar_main._load_png_profile_document(png_path)
    assert embedded["format"] == "fuzzfolio.scoring-profile"
    assert embedded["formatVersion"] == 1
    assert embedded["profile"]["version"] == "v1"
    assert embedded["profile"]["indicators"][0]["meta"] == {
        "id": "TRENDFLEX",
        "instanceId": "trendflex-1",
    }


def test_profile_drop_description_fallback_detects_generic_text() -> None:
    assert (
        ar_main._is_generic_profile_description(
            "Portable scoring profile scaffolded from live indicator templates."
        )
        is True
    )
    assert ar_main._is_generic_profile_description("") is True
    assert ar_main._is_generic_profile_description("Custom mean reversion profile.") is False


def test_build_profile_drop_description_uses_profile_structure() -> None:
    payload = {
        "profile": {
            "description": "Portable scoring profile scaffolded from live indicator templates.",
            "directionMode": "both",
            "indicators": [
                {
                    "config": {
                        "isActive": True,
                        "isTrendFollowing": True,
                        "label": "RSI",
                        "timeframe": "M5",
                    },
                    "meta": {"id": "RSI"},
                },
                {
                    "config": {
                        "isActive": True,
                        "isTrendFollowing": False,
                        "label": "WILLR",
                        "timeframe": "H1",
                    },
                    "meta": {"id": "WILLR"},
                },
            ],
        }
    }

    description = ar_main._build_profile_drop_description(
        payload,
        package_inputs={"instruments": ["EURUSD", "GBPUSD"], "timeframe": "M5"},
        row={},
        attempt={},
    )

    assert "Both-direction basket profile across EURUSD and GBPUSD on M5" in description
    assert "RSI and WILLR" in description
    assert "M5 and H1 timeframes" in description
    assert "1 trend-following and 1 mean-reversion" in description


def test_apply_profile_drop_description_fallback_rewrites_bundle_profile(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    profile_document_path = bundle_dir / "profile-document.json"
    profile_document_path.write_text(
        """
        {
          "format": "fuzzfolio.scoring-profile",
          "formatVersion": 1,
          "profile": {
            "description": "Portable scoring profile scaffolded from live indicator templates.",
            "directionMode": "both",
            "indicators": [
              {
                "config": {
                  "isActive": true,
                  "isTrendFollowing": true,
                  "label": "ADX",
                  "timeframe": "M15"
                },
                "meta": {"id": "ADX"}
              }
            ]
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    applied = ar_main._apply_profile_drop_description_fallback(
        bundle_dir,
        package_inputs={"instruments": ["EURUSD"], "timeframe": "M15"},
        row={},
        attempt={},
    )
    updated = ar_main.load_json_if_exists(profile_document_path)

    assert applied is not None
    assert "EURUSD profile on M15 using ADX" in applied
    assert updated["profile"]["description"] == applied


def _write_canonical_profile_drop_response(bundle_dir: Path) -> None:
    (bundle_dir / "sensitivity-response.json").write_text(
        json.dumps(
            {
                "data": {
                    "aggregate": {
                        "score_lab": {
                            "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                            "score": 42.0,
                        }
                    }
                }
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_render_profile_drop_for_attempt_generates_reuses_and_regenerates_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-a"
    run_dir.mkdir(parents=True, exist_ok=True)
    output_root = tmp_path / "profile-drops"
    working_dir = tmp_path
    writer_calls = {"count": 0}
    renderer_calls: list[list[str]] = []
    workspace_root = tmp_path / "Trading-Dashboard"
    renderer_source_root = workspace_root / "scripts" / "profile-drop-renderer"
    renderer_source_root.mkdir(parents=True, exist_ok=True)
    (renderer_source_root / "index.html").write_text("<html></html>", encoding="utf-8")

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            _write_canonical_profile_drop_response(bundle_dir)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand7",
                            "description": "Portable scoring profile scaffolded from live indicator templates.",
                            "directionMode": "both",
                            "indicators": [
                                {
                                    "meta": {"id": "BBANDS"},
                                    "config": {
                                        "label": "Bollinger Bands",
                                        "isActive": True,
                                        "isTrendFollowing": False,
                                        "timeframe": "M15",
                                    },
                                },
                                {
                                    "meta": {"id": "ATR"},
                                    "config": {
                                        "label": "ATR",
                                        "isActive": True,
                                        "isTrendFollowing": False,
                                        "timeframe": "M15",
                                    },
                                },
                            ],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    class FakeProvider:
        def complete_json(self, messages):
            writer_calls["count"] += 1
            return {
                "display_name": "Band Snapback Filter",
                "tagline": "Fade stretched band breaks after ATR expansion.",
                "short_description": "Targets Bollinger overshoots only when ATR says the move was forceful enough to mean revert.",
                "long_description": (
                    "This profile fades Bollinger overshoots after ATR expands, so it targets forceful "
                    "pushes that look exhausted and avoids quiet drift that lacks enough pressure to snap back."
                ),
            }

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        renderer_calls.append(list(argv))
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="writer",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=workspace_root),
        providers={"writer": SimpleNamespace(type="codex")},
    )
    row = {
        "attempt_id": "attempt-0001",
        "run_id": "run-a",
        "candidate_name": "cand7",
        "profile_ref": "prof-7",
        "composite_score": 67.4,
        "effective_window_months": 24.0,
    }
    attempt = {
        "attempt_id": "attempt-0001",
        "candidate_name": "cand7",
        "profile_ref": "prof-7",
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "profile_ref": "prof-7",
            "profile_path": tmp_path / "profiles" / "cand7.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: FakeProvider())
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result_first = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    manifest_path = Path(result_first["manifest_path"])
    metadata_path = Path(result_first["presentation_metadata_path"])
    manifest_payload = ar_main.load_json_if_exists(manifest_path)
    bundle_dir = output_root / ar_main._profile_drop_attempt_token(row, attempt) / "bundle" / "bundle-0001"
    bundle_payload = ar_main.load_json_if_exists(bundle_dir / "profile-document.json")

    assert result_first["status"] == "rendered"
    assert writer_calls["count"] == 1
    assert "--renderer-root" in renderer_calls[0]
    assert renderer_calls[0][renderer_calls[0].index("--renderer-root") + 1] == str(
        renderer_source_root
    )
    assert metadata_path.exists() is True
    assert manifest_payload["display_name"] == "Band Snapback Filter"
    assert manifest_payload["tagline"] == "Fade stretched band breaks after ATR expansion."
    assert manifest_payload["presentation_signature"]
    assert bundle_payload["profile"]["name"] == "Band Snapback Filter"
    assert bundle_payload["profile"]["description"] == manifest_payload["long_description"]

    result_cached = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    assert result_cached["status"] == "cached"
    assert writer_calls["count"] == 1

    stale_metadata = ar_main.load_json_if_exists(metadata_path)
    stale_metadata["presentation_signature"] = "stale-signature"
    stale_metadata["writer_profile"] = "stale-writer"
    metadata_path.write_text(json.dumps(stale_metadata, ensure_ascii=True, indent=2), encoding="utf-8")

    result_regenerated = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    assert result_regenerated["status"] == "rendered"
    assert writer_calls["count"] == 2
    assert ar_main.load_json_if_exists(metadata_path)["presentation_signature"] != "stale-signature"


def test_render_profile_drop_for_attempt_falls_back_when_metadata_writer_fails(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-b"
    run_dir.mkdir(parents=True, exist_ok=True)
    output_root = tmp_path / "profile-drops"
    working_dir = tmp_path

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            _write_canonical_profile_drop_response(bundle_dir)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand9",
                            "description": "Portable scoring profile scaffolded from live indicator templates.",
                            "directionMode": "both",
                            "indicators": [
                                {
                                    "meta": {"id": "ADX"},
                                    "config": {
                                        "label": "ADX",
                                        "isActive": True,
                                        "isTrendFollowing": True,
                                        "timeframe": "M15",
                                    },
                                }
                            ],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    class FailingProvider:
        def complete_json(self, messages):
            raise ar_main.ProviderError("metadata writer offline")

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="writer",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=None),
        providers={"writer": SimpleNamespace(type="codex")},
    )
    row = {
        "attempt_id": "attempt-0002",
        "run_id": "run-b",
        "candidate_name": "cand9",
        "profile_ref": "prof-9",
    }
    attempt = {
        "attempt_id": "attempt-0002",
        "candidate_name": "cand9",
        "profile_ref": "prof-9",
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "profile_ref": "prof-9",
            "profile_path": tmp_path / "profiles" / "cand9.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "create_provider", lambda _profile: FailingProvider())
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=output_root,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
    )

    manifest_payload = ar_main.load_json_if_exists(Path(result["manifest_path"]))
    bundle_dir = output_root / ar_main._profile_drop_attempt_token(row, attempt) / "bundle" / "bundle-0001"
    bundle_payload = ar_main.load_json_if_exists(bundle_dir / "profile-document.json")

    assert result["status"] == "rendered"
    assert manifest_payload["tagline"] is None
    assert manifest_payload["short_description"] is None
    assert "EURUSD profile on M15 using ADX" in manifest_payload["long_description"]
    assert bundle_payload["profile"]["description"] == manifest_payload["long_description"]
    assert Path(result["presentation_metadata_path"]).exists() is False


def test_render_profile_drop_for_attempt_attempt_local_layout_caches_in_hidden_folder(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-c"
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir = run_dir / "evals" / "cand-local"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "notes").mkdir(parents=True, exist_ok=True)
    working_dir = tmp_path

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            _write_canonical_profile_drop_response(bundle_dir)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand-local",
                            "description": "Local profile description.",
                            "directionMode": "both",
                            "indicators": [],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=None),
        providers={},
    )
    row = {
        "attempt_id": "attempt-local-1",
        "run_id": "run-c",
        "candidate_name": "cand-local",
        "profile_ref": "prof-local",
    }
    attempt = {
        "attempt_id": "attempt-local-1",
        "candidate_name": "cand-local",
        "profile_ref": "prof-local",
        "artifact_dir": str(artifact_dir),
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "artifact_dir": artifact_dir,
            "profile_ref": "prof-local",
            "profile_path": tmp_path / "profiles" / "cand-local.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result_first = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=None,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
        layout_mode="attempt_local",
    )

    hidden_root = artifact_dir / ".profile-drop-36mo"
    bundle_dir = hidden_root / "bundle" / "bundle-0001"
    assert result_first["status"] == "rendered"
    assert Path(result_first["png_path"]) == artifact_dir / "profile-drop-36mo.png"
    assert Path(result_first["manifest_path"]) == artifact_dir / "profile-drop-36mo.manifest.json"
    assert hidden_root.exists() is True
    assert bundle_dir.exists() is True
    assert (artifact_dir / "notes").exists() is True

    result_cached = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=None,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
        layout_mode="attempt_local",
    )

    assert result_cached["status"] == "cached"
    assert (artifact_dir / "notes").exists() is True


def test_render_profile_drop_for_attempt_uses_cache_before_cloud_profile_repair(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-d"
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir = run_dir / "evals" / "cand-cache"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    working_dir = tmp_path
    renderer_calls: list[list[str]] = []

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            output_root_arg = Path(args[args.index("--output-root") + 1])
            bundle_dir = output_root_arg / "bundle-0001"
            bundle_dir.mkdir(parents=True, exist_ok=True)
            _write_canonical_profile_drop_response(bundle_dir)
            (bundle_dir / "profile-document.json").write_text(
                json.dumps(
                    {
                        "profile": {
                            "name": "cand-cache",
                            "description": "Local profile description.",
                            "directionMode": "both",
                            "indicators": [],
                        }
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        renderer_calls.append(list(argv))
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=None),
        providers={},
    )
    row = {
        "attempt_id": "attempt-cache-1",
        "run_id": "run-d",
        "candidate_name": "cand-cache",
        "profile_ref": "prof-stale",
    }
    attempt = {
        "attempt_id": "attempt-cache-1",
        "candidate_name": "cand-cache",
        "profile_ref": "prof-stale",
        "artifact_dir": str(artifact_dir),
    }

    monkeypatch.setattr(
        ar_main,
        "_build_package_inputs",
        lambda *_args, **_kwargs: {
            "artifact_dir": artifact_dir,
            "profile_ref": "prof-stale",
            "profile_path": tmp_path / "profiles" / "cand-cache.json",
            "timeframe": "M15",
            "instruments": ["EURUSD"],
            "lookback_months": 36,
        },
    )
    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result_first = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=None,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
        layout_mode="attempt_local",
    )

    assert result_first["status"] == "rendered"
    assert len(renderer_calls) == 1

    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        ar_main,
        "_create_cloud_profile",
        lambda *_args, **_kwargs: pytest.fail("cached render should not recreate cloud profile"),
    )

    result_second = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=working_dir,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=None,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
        layout_mode="attempt_local",
    )

    assert result_second["status"] == "cached"
    assert len(renderer_calls) == 1


def _write_existing_full_backtest_profile_drop_inputs(tmp_path: Path) -> tuple[Path, Path]:
    artifact_dir = tmp_path / "runs" / "run-fast" / "evals" / "candidate"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    profile_path = tmp_path / "profiles" / "candidate.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        json.dumps(
            {
                "format": "fuzzfolio.scoring-profile",
                "formatVersion": 1,
                "profile": {
                    "name": "Scaffold Candidate " + ("X" * 150),
                    "version": "v9",
                    "description": "Portable scoring profile scaffolded from live indicator templates.",
                    "directionMode": "both",
                    "instruments": ["EURUSD"],
                    "indicators": [
                        {
                            "meta": {"id": "RSI"},
                            "config": {
                                "label": "RSI",
                                "isActive": True,
                                "isTrendFollowing": False,
                                "timeframe": "M15",
                            },
                        }
                    ],
                },
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    response = {
        "data": {
            "aggregate": {
                "axes": {
                    "stop_loss_percent": [0.05, 0.1],
                    "reward_multiple": [2.0, 3.0],
                },
                "bar_limit": 1000,
                "best_cell": {
                    "stop_loss_percent": 0.1,
                    "reward_multiple": 3.0,
                    "take_profit_percent": 0.3,
                    "avg_net_r_per_closed_trade": 0.4,
                    "profit_factor": 2.5,
                    "resolved_trades": 27,
                },
                "market_data_window": {
                    "effective_bar_limit": 2000,
                    "requested_window_start": "2023-01-01T00:00:00+00:00",
                    "requested_window_end": "2026-01-01T00:00:00+00:00",
                    "source": "lake_bars",
                },
                "matrix": {"avg_net_r_per_closed_trade": [[0.1, 0.2], [0.3, 0.4]]},
                "matrix_summary": {},
                "mode": "basket",
                "recommended_cell": {
                    "stop_loss_percent": 0.05,
                    "reward_multiple": 2.0,
                    "take_profit_percent": 0.1,
                },
                "recommended_cell_basis": "robust_cell",
                "score_lab": {
                    "version": ar_main.CANONICAL_SCORE_LAB_VERSION,
                    "score": 73.5,
                },
            }
        }
    }
    detail = {
        "artifact_mode": "profile_drop_cell_detail_compact_v1",
        "job_id": "deepreplay-fast-1",
        "cell": {
            "stop_loss_percent": 0.1,
            "reward_multiple": 3.0,
            "take_profit_percent": 0.3,
        },
        "path_metrics": {"final_equity_r": 12.3, "max_drawdown_r": 1.2},
        "curve": {
            "points": [
                {
                    "time": 1704067200,
                    "date": "2024-01-01",
                    "equity_r": 0.0,
                    "drawdown_r": 0.0,
                    "realized_r": 0.0,
                    "closed_trade_count": 0,
                },
                {
                    "time": 1704153600,
                    "date": "2024-01-02",
                    "equity_r": 12.3,
                    "drawdown_r": 0.0,
                    "realized_r": 12.3,
                    "closed_trade_count": 4,
                },
            ]
        },
    }
    job = {
        "job_id": "deepreplay-fast-1",
        "request": {
            "alert_threshold": 80.0,
            "analysis_window_start": "2023-01-01T00:00:00Z",
            "analysis_window_end": "2026-01-01T00:00:00Z",
            "bar_limit": 1000,
            "direction_mode": "both",
            "instruments": ["EURUSD"],
            "market_data_source": "lake_bars",
            "matrix": {
                "sl_step_percent": 0.05,
                "sl_rows": 2,
                "reward_step_r": 1.0,
                "reward_columns": 2,
            },
            "options": {
                "path_metrics_mode": "highlighted",
                "quality_score_preset": "profile_drop",
            },
            "timeframe": "M15",
            "view_mode": "overview",
        },
    }
    (artifact_dir / "full-backtest-36mo-result.json").write_text(
        json.dumps(response, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (artifact_dir / "full-backtest-36mo-curve.json").write_text(
        json.dumps(detail, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    (artifact_dir / "deep-replay-job.json").write_text(
        json.dumps(job, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return artifact_dir, profile_path


def test_materialize_profile_drop_bundle_reuses_existing_full_backtest_artifacts(
    tmp_path: Path,
) -> None:
    artifact_dir, profile_path = _write_existing_full_backtest_profile_drop_inputs(tmp_path)
    cell_detail_calls: list[list[str]] = []

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            cell_detail_calls.append(list(args))
            out_path = Path(args[args.index("--out") + 1])
            payload = {
                "artifact_mode": "profile_drop_cell_detail_v1",
                "job_id": "deepreplay-fast-1",
                "scope": "instrument",
                "instrument": "EURUSD",
                "cell": {
                    "stop_loss_percent": 0.05,
                    "reward_multiple": 2.0,
                    "take_profit_percent": 0.1,
                },
                "path_metrics": {"final_equity_r": 8.2, "max_drawdown_r": 0.9},
                "curve": {"points": []},
            }
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload), encoding="utf-8")
            return SimpleNamespace(returncode=0, stdout="", stderr="", parsed_json=payload)

    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile-drop"),
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )
    package_output_root = tmp_path / "bundle-root"

    bundle_dir = ar_main._materialize_profile_drop_bundle_from_existing_artifacts(
        config=config,
        cli=FakeCli(),
        working_dir=tmp_path,
        attempt={"attempt_id": "attempt-fast", "artifact_dir": str(artifact_dir)},
        package_inputs={
            "artifact_dir": artifact_dir,
            "profile_path": profile_path,
            "timeframe": "M15",
            "instruments": ["EURUSD"],
        },
        package_output_root=package_output_root,
        lookback_months=36,
        profile_ref="profile-fast",
        attempt_token="attempt-fast",
        layout_mode="attempt_local",
    )

    assert bundle_dir is not None
    profile_document = ar_main.load_json_if_exists(bundle_dir / "profile-document.json")
    bundle_response = ar_main.load_json_if_exists(bundle_dir / "sensitivity-response.json")
    run_metadata = ar_main.load_json_if_exists(bundle_dir / "run-metadata.json")
    recommended_detail = ar_main.load_json_if_exists(
        bundle_dir / "recommended-cell-path-detail.json"
    )
    best_detail = ar_main.load_json_if_exists(bundle_dir / "best-cell-path-detail.json")

    assert cell_detail_calls
    assert cell_detail_calls[0][:2] == ["deep-replay", "cell-detail"]
    assert cell_detail_calls[0][cell_detail_calls[0].index("--stop-loss-percent") + 1] == "0.05"
    assert cell_detail_calls[0][cell_detail_calls[0].index("--reward-multiple") + 1] == "2"
    assert (bundle_dir / "recommended-cell-path-detail.json").exists() is True
    assert (bundle_dir / "best-cell-path-detail.json").exists() is True
    assert recommended_detail["cell"]["stop_loss_percent"] == 0.05
    assert recommended_detail["cell"]["reward_multiple"] == 2.0
    assert best_detail["cell"]["stop_loss_percent"] == 0.1
    assert best_detail["cell"]["reward_multiple"] == 3.0
    assert bundle_response["data"]["mode"] == "basket"
    assert bundle_response["data"]["aggregate"]["recommended_cell_basis"] == "robust_cell"
    assert bundle_response["data"]["aggregate"]["recommended_cell"]["reward_multiple"] == 2.0
    assert bundle_response["data"]["aggregate"]["recommended_cell"]["stop_loss_percent"] == 0.05
    assert (
        bundle_response["data"]["aggregate"]["recommended_cell_path_metrics"][
            "final_equity_r"
        ]
        == 8.2
    )
    assert profile_document["profile"]["version"] == "v1"
    assert len(profile_document["profile"]["name"]) <= 120
    exit_policy = profile_document["profile"]["executionConfig"]["exitPolicy"]
    assert exit_policy["selectedCell"]["stopLossPercent"] == 0.05
    assert exit_policy["selectedCell"]["rewardMultiple"] == 2.0
    assert exit_policy["recommendation"]["basis"] == "robust_cell"
    assert run_metadata["analysis"]["analysis_backend"] == "deep_replay_existing_artifact"
    assert run_metadata["analysis"]["source_artifact_kind"] == "full_backtest_36mo"
    assert run_metadata["analysis"]["deep_replay_job_id"] == "deepreplay-fast-1"


def test_materialize_profile_drop_bundle_reuses_existing_recommended_full_backtest_detail(
    tmp_path: Path,
) -> None:
    artifact_dir, profile_path = _write_existing_full_backtest_profile_drop_inputs(tmp_path)
    recommended_detail = {
        "artifact_mode": "profile_drop_cell_detail_compact_v1",
        "job_id": "deepreplay-fast-1",
        "scope": "instrument",
        "instrument": "EURUSD",
        "cell": {
            "stop_loss_percent": 0.05,
            "reward_multiple": 2.0,
            "take_profit_percent": 0.1,
        },
        "path_metrics": {"final_equity_r": 8.2, "max_drawdown_r": 0.9},
        "curve": {"points": []},
    }
    (
        artifact_dir / ar_main.FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME
    ).write_text(
        json.dumps(recommended_detail, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            pytest.fail("existing recommended full-backtest detail should be reused")

    config = SimpleNamespace(
        research=SimpleNamespace(quality_score_preset="profile-drop"),
        fuzzfolio=SimpleNamespace(base_url="http://localhost:7946/api/dev"),
    )

    bundle_dir = ar_main._materialize_profile_drop_bundle_from_existing_artifacts(
        config=config,
        cli=FakeCli(),
        working_dir=tmp_path,
        attempt={"attempt_id": "attempt-fast", "artifact_dir": str(artifact_dir)},
        package_inputs={
            "artifact_dir": artifact_dir,
            "profile_path": profile_path,
            "timeframe": "M15",
            "instruments": ["EURUSD"],
        },
        package_output_root=tmp_path / "bundle-root",
        lookback_months=36,
        profile_ref="profile-fast",
        attempt_token="attempt-fast",
        layout_mode="attempt_local",
    )

    assert bundle_dir is not None
    bundle_response = ar_main.load_json_if_exists(bundle_dir / "sensitivity-response.json")
    copied_recommended = ar_main.load_json_if_exists(
        bundle_dir / "recommended-cell-path-detail.json"
    )
    sensitivity_request = ar_main.load_json_if_exists(
        bundle_dir / "sensitivity-request.json"
    )

    assert copied_recommended["cell"]["stop_loss_percent"] == 0.05
    assert copied_recommended["cell"]["reward_multiple"] == 2.0
    assert (
        bundle_response["data"]["aggregate"]["recommended_cell_path_metrics"][
            "final_equity_r"
        ]
        == 8.2
    )
    assert (
        sensitivity_request["source"]["recommended_cell_detail_source"]
        == "existing_recommended_cell_detail"
    )


def test_align_profile_drop_response_to_detail_preserves_matching_recommended_metrics() -> None:
    response = {
        "data": {
            "aggregate": {
                "recommended_cell": {
                    "stop_loss_percent": 0.08,
                    "reward_multiple": 4.0,
                    "take_profit_percent": 0.32,
                    "avg_net_r_per_closed_trade": 1.25,
                    "profit_factor": 3.4,
                    "resolved_trades": 52,
                },
                "recommended_cell_basis": "robust_cell",
                "best_cell": {
                    "stop_loss_percent": 0.1,
                    "reward_multiple": 3.0,
                    "avg_net_r_per_closed_trade": 0.9,
                },
            }
        }
    }
    detail = {
        "cell": {
            "stop_loss_percent": 0.08,
            "reward_multiple": 4.0,
            "take_profit_percent": 0.32,
        },
        "path_metrics": {"equity_curve_r_squared": 0.98},
    }

    bundle_response, recommended_cell, basis, source = (
        ar_main._align_profile_drop_response_to_detail(response, detail)
    )

    aggregate = bundle_response["data"]["aggregate"]
    assert source == "detail_cell"
    assert basis == "robust_cell"
    assert recommended_cell["avg_net_r_per_closed_trade"] == 1.25
    assert recommended_cell["profit_factor"] == 3.4
    assert recommended_cell["stop_loss_percent"] == 0.08
    assert aggregate["recommended_cell"]["resolved_trades"] == 52
    assert aggregate["recommended_cell_path_metrics"]["equity_curve_r_squared"] == 0.98


def test_align_profile_drop_response_to_detail_preserves_mismatched_recommended_cell() -> None:
    response = {
        "data": {
            "aggregate": {
                "recommended_cell": {
                    "stop_loss_percent": 0.08,
                    "reward_multiple": 2.5,
                    "avg_net_r_per_closed_trade": 0.42,
                },
                "recommended_cell_basis": "robust_cell",
                "best_cell": {
                    "stop_loss_percent": 0.04,
                    "reward_multiple": 4.0,
                    "avg_net_r_per_closed_trade": 0.9,
                },
            }
        }
    }
    detail = {
        "cell": {
            "stop_loss_percent": 0.04,
            "reward_multiple": 4.0,
            "take_profit_percent": 0.16,
        },
        "path_metrics": {"equity_curve_r_squared": 0.99},
    }

    bundle_response, recommended_cell, basis, source = (
        ar_main._align_profile_drop_response_to_detail(response, detail)
    )

    aggregate = bundle_response["data"]["aggregate"]
    assert source == "response_recommended_cell_detail_mismatch"
    assert basis == "robust_cell"
    assert recommended_cell["stop_loss_percent"] == 0.08
    assert recommended_cell["reward_multiple"] == 2.5
    assert aggregate["recommended_cell"]["stop_loss_percent"] == 0.08
    assert aggregate["recommended_cell"]["reward_multiple"] == 2.5
    assert "recommended_cell_path_metrics" not in aggregate
    assert aggregate["_autoresearch_profile_drop_bundle"]["cell_detail_role"] == "best_cell"
    assert (
        aggregate["_autoresearch_profile_drop_bundle"][
            "recommended_cell_preserved"
        ]
        is True
    )


def test_render_profile_drop_for_attempt_skips_cli_package_when_full_artifacts_exist(
    tmp_path: Path, monkeypatch
) -> None:
    run_dir = tmp_path / "runs" / "run-fast"
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir, profile_path = _write_existing_full_backtest_profile_drop_inputs(tmp_path)
    renderer_calls: list[list[str]] = []

    class FakeCli:
        def run(self, args, cwd=None, timeout_seconds=None, check=True):
            if args and args[0] == "package":
                pytest.fail("profile-drop rendering should reuse existing full-backtest artifacts")
            if args[:2] == ["deep-replay", "cell-detail"]:
                out_path = Path(args[args.index("--out") + 1])
                payload = {
                    "artifact_mode": "profile_drop_cell_detail_v1",
                    "job_id": "deepreplay-fast-1",
                    "scope": "instrument",
                    "instrument": "EURUSD",
                    "cell": {
                        "stop_loss_percent": 0.05,
                        "reward_multiple": 2.0,
                        "take_profit_percent": 0.1,
                    },
                    "path_metrics": {"final_equity_r": 8.2, "max_drawdown_r": 0.9},
                    "curve": {"points": []},
                }
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json.dumps(payload), encoding="utf-8")
                return SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(payload),
                    stderr="",
                    parsed_json=payload,
                )
            return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_run_external(argv, cwd=None, timeout_seconds=None):
        renderer_calls.append(list(argv))
        png_path = Path(argv[argv.index("--out") + 1])
        png_path.parent.mkdir(parents=True, exist_ok=True)
        png_path.write_text("png", encoding="utf-8")

    config = SimpleNamespace(
        research=SimpleNamespace(
            quality_score_preset="profile-drop",
            presentation_metadata_provider_profile="",
        ),
        fuzzfolio=SimpleNamespace(workspace_root=None, base_url="http://localhost:7946/api/dev"),
        providers={},
    )
    row = {
        "attempt_id": "attempt-fast",
        "run_id": "run-fast",
        "candidate_name": "candidate",
        "profile_ref": "profile-fast",
    }
    attempt = {
        "attempt_id": "attempt-fast",
        "candidate_name": "candidate",
        "profile_ref": "profile-fast",
        "artifact_dir": str(artifact_dir),
        "profile_path": str(profile_path),
    }

    monkeypatch.setattr(ar_main, "_cloud_profile_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(ar_main, "_run_external", fake_run_external)

    result = ar_main._render_profile_drop_for_attempt(
        config=config,
        cli=FakeCli(),
        renderer_executable=tmp_path / "renderer",
        working_dir=tmp_path,
        run_dir=run_dir,
        attempts=[attempt],
        row=row,
        attempt=attempt,
        output_root=None,
        lookback_months=36,
        force_rebuild=False,
        timeout_seconds=30,
        emit=None,
        layout_mode="attempt_local",
    )

    hidden_root = artifact_dir / ".profile-drop-36mo"
    bundle_dirs = list((hidden_root / "bundle").iterdir())
    assert result["status"] == "rendered"
    assert len(renderer_calls) == 1
    assert Path(result["png_path"]) == artifact_dir / "profile-drop-36mo.png"
    assert len(bundle_dirs) == 1
    assert ar_main.load_json_if_exists(bundle_dirs[0] / "run-metadata.json")["analysis"][
        "analysis_backend"
    ] == "deep_replay_existing_artifact"
