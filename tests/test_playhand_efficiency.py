from __future__ import annotations

import json
from pathlib import Path

from autoresearch.__main__ import build_parser
from autoresearch.playhand_efficiency import (
    SCHEMA_VERSION,
    build_playhand_efficiency_report,
    write_playhand_efficiency_report,
)


def _write_run(
    runs_root: Path,
    run_id: str,
    metadata: dict[str, object],
    attempts: list[dict[str, object]] | None = None,
) -> Path:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "runner": "play_hand_v1",
        "created_at": "2026-01-01T00:00:00+00:00",
        **metadata,
    }
    (run_dir / "run-metadata.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    with (run_dir / "attempts.jsonl").open("w", encoding="utf-8") as handle:
        for attempt in attempts or []:
            handle.write(json.dumps(attempt, ensure_ascii=True) + "\n")
    return run_dir


def test_build_playhand_efficiency_report_counts_saving_mechanisms(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_run(
        runs_root,
        "20260101T000000000000Z-playhand-v1",
        {
            "run_status": "tombstoned",
            "selected_final_branch": "early_exit",
            "dealt_indicator_source": "play_hand_seed_plan",
            "dealt_recipe": "mean_reversion_reclaim",
            "sweep_budget_value": 1024,
            "coarse_probe_budget": 128,
            "instrument_scout_size": 5,
            "early_exit_policy": {
                "mode": "enforce",
                "decisions": [
                    {
                        "checkpoint": "after_baseline",
                        "would_exit": True,
                        "enforced": True,
                        "terminal": True,
                        "enforce_action": "early_exit_tombstone",
                        "rules_fired": ["baseline_score_not_positive"],
                        "skipped_stages": [
                            "lookback_timing",
                            "coarse_probe",
                            "coarse_expand",
                            "focused",
                            "instrument_scout",
                            "mutated_final_36mo",
                        ],
                    }
                ],
            },
            "coarse_halving": {"mode": "enforce", "decisions": []},
            "family_policy_execution": {
                "mode": "enforce",
                "family_policy": "none",
                "decision": "not_applicable",
                "mutation_allowed": True,
            },
            "play_hand_health": {
                "calendar": {"status": "unknown", "passed": None, "reasons": []}
            },
        },
        attempts=[
            {
                "attempt_id": "early-attempt-1",
                "created_at": "2026-01-01T00:02:00+00:00",
            }
        ],
    )
    _write_run(
        runs_root,
        "20260101T010000000000Z-playhand-v1",
        {
            "run_status": "promoted",
            "selected_final_branch": "exact_template",
            "final_scrutiny_score": 72.5,
            "dealt_indicator_source": "play_hand_seed_plan",
            "dealt_recipe": "discovered_recipe_006",
            "early_exit_policy": {
                "mode": "enforce",
                "decisions": [
                    {
                        "checkpoint": "before_final_scrutiny",
                        "would_exit": False,
                        "enforced": False,
                        "terminal": False,
                        "enforce_action": "continue",
                        "rules_fired": [],
                        "skipped_stages": [],
                    }
                ],
            },
            "coarse_halving": {
                "mode": "enforce",
                "decisions": [
                    {
                        "decision": "skip_expand",
                        "expanded": False,
                        "estimated_saved_evaluations": 896,
                    }
                ],
            },
            "family_policy_execution": {
                "mode": "enforce",
                "family_policy": "template_locked",
                "decision": "template_locked_exact_only",
                "exact_template_available": True,
                "exact_template_used_as_incumbent": True,
                "mutation_allowed": False,
                "skipped_stages": [
                    "lookback_timing",
                    "coarse_probe",
                    "coarse_expand",
                    "focused",
                    "instrument_scout",
                ],
            },
            "play_hand_health": {
                "calendar": {"status": "passed", "passed": True, "reasons": []}
            },
        },
        attempts=[
            {
                "attempt_id": "exact-attempt-1",
                "created_at": "2026-01-01T01:04:00+00:00",
            }
        ],
    )

    report = build_playhand_efficiency_report(runs_root, limit=None)
    summary = report["summary"]

    assert report["schema_version"] == SCHEMA_VERSION
    assert summary["run_count"] == 2
    assert summary["completed_count"] == 2
    assert summary["coarse_halving"]["skipped_expansion_runs"] == 1
    assert summary["coarse_halving"]["total_estimated_saved_evaluations"] == 896
    assert summary["early_exit"]["enforced_terminal_tombstones"] == 1
    assert summary["early_exit"]["estimated_saved"] == {
        "coarse_permutations_avoided": 1024,
        "instrument_scout_evals_avoided": 5,
        "deep_replay_jobs_avoided": 1,
    }
    assert summary["family_policy"]["exact_only_skip_runs"] == 1
    assert summary["family_policy"]["exact_template_used_as_incumbent_runs"] == 1
    assert summary["selected_final_branch_counts"] == {
        "early_exit": 1,
        "exact_template": 1,
    }
    assert summary["calendar"]["status_counts"] == {"passed": 1, "unknown": 1}
    assert summary["top_savings_runs"][0]["run_id"] == "20260101T000000000000Z-playhand-v1"


def test_write_playhand_efficiency_report_writes_artifacts(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_run(
        runs_root,
        "20260101T000000000000Z-playhand-v1",
        {
            "run_status": "promoted",
            "selected_final_branch": "mutated",
            "final_scrutiny_score": 61.0,
            "early_exit_policy": {"mode": "off", "decisions": []},
            "coarse_halving": {"mode": "off", "decisions": []},
            "family_policy_execution": {"mode": "off", "family_policy": "none"},
        },
    )
    report = build_playhand_efficiency_report(runs_root, limit=10)

    paths = write_playhand_efficiency_report(report, tmp_path / "report")

    json_path = Path(paths["playhand_efficiency_report_json"])
    markdown_path = Path(paths["playhand_efficiency_report_markdown"])
    csv_path = Path(paths["playhand_efficiency_runs_csv"])
    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_version"] == SCHEMA_VERSION
    assert "PlayHand Efficiency Report" in markdown_path.read_text(encoding="utf-8")
    assert "run_id,created_at,run_status" in csv_path.read_text(encoding="utf-8")


def test_build_playhand_efficiency_report_respects_zero_limit(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_run(
        runs_root,
        "20260101T000000000000Z-playhand-v1",
        {"run_status": "promoted"},
    )

    report = build_playhand_efficiency_report(runs_root, limit=0)

    assert report["summary"]["run_count"] == 0
    assert report["rows"] == []


def test_build_parser_accepts_playhand_efficiency_report_defaults() -> None:
    parser = build_parser()
    args = parser.parse_args(["playhand-efficiency-report"])
    assert args.command == "playhand-efficiency-report"
    assert args.limit == 200
    assert args.all_runs is False

    args = parser.parse_args(
        [
            "playhand-efficiency-report",
            "--run-id",
            "run-a",
            "--all-runs",
            "--json",
        ]
    )
    assert args.run_id == ["run-a"]
    assert args.all_runs is True
    assert args.json is True


def test_build_playhand_efficiency_report_uses_catalog_run_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    selected_id = "20260101T000000000000Z-playhand-v1"
    ignored_id = "20260101T010000000000Z-playhand-v1"
    _write_run(
        runs_root,
        selected_id,
        {"run_status": "promoted", "selected_final_branch": "mutated"},
    )
    _write_run(
        runs_root,
        ignored_id,
        {"run_status": "tombstoned", "selected_final_branch": "early_exit"},
    )

    fake_config = type(
        "FakeConfig",
        (),
        {
            "runs_root": runs_root,
            "derived_root": tmp_path / "derived",
            "attempt_catalog_sqlite_path": tmp_path / "derived" / "attempt-catalog.sqlite",
        },
    )()

    monkeypatch.setattr(
        "autoresearch.playhand_efficiency.iter_playhand_run_ids",
        lambda _config: iter([selected_id]),
    )

    report = build_playhand_efficiency_report(
        runs_root,
        limit=None,
        config=fake_config,  # type: ignore[arg-type]
    )

    assert report["summary"]["run_count"] == 1
    assert report["rows"][0]["run_id"] == selected_id
