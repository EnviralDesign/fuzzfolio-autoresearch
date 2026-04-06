from pathlib import Path

from autoresearch import __main__ as ar_main
from autoresearch import portfolio as pf


def test_load_portfolio_spec_defaults_when_file_missing(tmp_path: Path) -> None:
    spec, defaulted = pf.load_portfolio_spec(tmp_path / "missing.json")

    assert defaulted is True
    assert spec["portfolio_name"] == "default-portfolio"
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
