from pathlib import Path

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
