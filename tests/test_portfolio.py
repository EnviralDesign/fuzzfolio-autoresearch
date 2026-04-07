from pathlib import Path
from types import SimpleNamespace

from autoresearch import __main__ as ar_main
from autoresearch import portfolio as pf


def test_load_portfolio_spec_defaults_when_file_missing(tmp_path: Path) -> None:
    spec, defaulted = pf.load_portfolio_spec(tmp_path / "missing.json")

    assert defaulted is True
    assert spec["portfolio_name"] == "default-portfolio"
    assert spec["full_backtest_job_timeout_seconds"] == 2400
    assert [sleeve["name"] for sleeve in spec["sleeves"]] == ["quality", "cadence"]


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

    profile_path = runs_root / "run-a" / "evals" / "candidate-a" / "profile.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text('{"name":"alpha"}', encoding="utf-8")

    source_drop_root = report_root / "profile-drops" / "1-attempt-a-alpha"
    source_drop_root.mkdir(parents=True, exist_ok=True)
    source_drop_png = source_drop_root / "profile-drop-36mo.png"
    source_drop_manifest = source_drop_root / "profile-drop-36mo.manifest.json"
    source_drop_png.write_text("png", encoding="utf-8")
    source_drop_manifest.write_text("{}", encoding="utf-8")

    report_path = report_root / "portfolio-report.json"
    report_path.write_text("{}", encoding="utf-8")

    payload = {
        "portfolio_name": "default-portfolio",
        "selected": [
            {
                "attempt_id": "attempt-a",
                "run_id": "run-a",
                "candidate_name": "alpha",
                "profile_path": str(profile_path),
                "profile_ref": "abc123",
            }
        ],
        "profile_drops": [
            {
                "attempt_id": "attempt-a",
                "profile_ref": "abc123",
                "png_path": str(source_drop_png),
                "manifest_path": str(source_drop_manifest),
            }
        ],
    }

    config = SimpleNamespace(derived_root=derived_root)
    summary = ar_main._export_portfolio_bundle(
        config=config,
        payload=payload,
        report_root=report_root,
        report_path=report_path,
    )

    bundle_root = Path(summary["bundle_root"])
    item_root = bundle_root / "alpha"

    assert item_root.exists() is True
    assert (item_root / "alpha.json").exists() is True
    assert (item_root / "alpha.png").exists() is True
    assert (item_root / "profile-drop-36mo.manifest.json").exists() is False
    assert (bundle_root / "portfolio-report.json").exists() is False
    assert (bundle_root / "portfolio-report.csv").exists() is False
    assert (bundle_root / "selected-attempts.csv").exists() is False
    assert (bundle_root / "bundle-manifest.json").exists() is False
    assert summary["exported_profiles"] == 1
    assert summary["exported_drop_pngs"] == 1


def test_human_bundle_item_token_avoids_collisions() -> None:
    used: set[str] = set()

    first = ar_main._human_bundle_item_token("alpha", "attempt-a", used)
    second = ar_main._human_bundle_item_token("alpha", "attempt-b", used)

    assert first == "alpha"
    assert second == "alpha-2"


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
