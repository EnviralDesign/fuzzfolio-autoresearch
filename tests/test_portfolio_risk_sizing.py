import json
from pathlib import Path

from autoresearch.portfolio_risk_sizing import (
    RiskSizingSpec,
    build_risk_sizing_schedule,
    write_risk_sizing_report,
)


def _write_profile_export(root: Path, *, folder: str, attempt_id: str, name: str) -> None:
    export_dir = root / folder
    export_dir.mkdir(parents=True)
    (export_dir / "profile-drop-36mo.manifest.json").write_text(
        json.dumps({"attempt_id": attempt_id}),
        encoding="utf-8",
    )
    (export_dir / f"{folder}.json").write_text(
        json.dumps({"profile": {"name": name}}),
        encoding="utf-8",
    )


def test_risk_sizing_prefers_short_low_drawdown_export_name(tmp_path: Path) -> None:
    curve_path = tmp_path / "curve.json"
    curve_path.write_text(
        json.dumps(
            {
                "path_metrics": {
                    "final_equity_r": 80,
                    "max_drawdown_r": 2,
                    "avg_holding_hours": 5,
                    "p90_holding_hours": 12,
                    "max_holding_hours": 24,
                    "max_consecutive_losses": 4,
                    "avg_loss_streak": 2,
                    "temporal_breadth": {
                        "top_10_day_share": 0.25,
                        "positive_week_ratio": 0.35,
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "portfolio-report.json"
    report_path.write_text(
        json.dumps(
            {
                "selected": [
                    {
                        "attempt_id": "attempt-1",
                        "candidate_name": "raw-candidate-name",
                        "score_36m": 82,
                        "instruments_36m": ["XAGUSD"],
                        "full_backtest_calendar_curve_path_36m": str(curve_path),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    export_root = tmp_path / "export"
    _write_profile_export(
        export_root,
        folder="Pretty-Strategy",
        attempt_id="attempt-1",
        name="Pretty Strategy",
    )

    schedule = build_risk_sizing_schedule(
        portfolio_report_path=report_path,
        export_bundle_path=export_root,
        spec=RiskSizingSpec(),
    )

    assert schedule[0]["name"] == "Pretty Strategy"
    assert schedule[0]["risk_percent"] > 2.0
    assert schedule[0]["tier"] in {"medium_high", "high"}
    assert "short-hold" in schedule[0]["rationale"]


def test_risk_sizing_caps_long_hold_lossy_path(tmp_path: Path) -> None:
    curve_path = tmp_path / "curve.json"
    curve_path.write_text(
        json.dumps(
            {
                "path_metrics": {
                    "final_equity_r": 120,
                    "max_drawdown_r": 4,
                    "avg_holding_hours": 55,
                    "p90_holding_hours": 150,
                    "max_holding_hours": 260,
                    "max_consecutive_losses": 16,
                    "avg_loss_streak": 5,
                    "temporal_breadth": {
                        "top_10_day_share": 0.4,
                        "positive_week_ratio": 0.4,
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "portfolio-report.json"
    report_path.write_text(
        json.dumps(
            {
                "selected": [
                    {
                        "attempt_id": "attempt-2",
                        "candidate_name": "long-hold",
                        "score_36m": 85,
                        "instruments_36m": ["EURUSD"],
                        "full_backtest_calendar_curve_path_36m": str(curve_path),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    schedule = build_risk_sizing_schedule(portfolio_report_path=report_path)
    summary = write_risk_sizing_report(
        schedule=schedule,
        output_dir=tmp_path / "out",
        portfolio_report_path=report_path,
        export_bundle_path=None,
    )

    assert schedule[0]["risk_percent"] <= 1.0
    assert schedule[0]["tier"] == "capped"
    assert "hold-time cap" in schedule[0]["rationale"]
    assert Path(summary["json_path"]).exists()
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["markdown_path"]).exists()
