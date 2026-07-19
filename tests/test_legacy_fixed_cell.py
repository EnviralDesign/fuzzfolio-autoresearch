from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import __main__ as ar_main
from autoresearch import legacy_fixed_cell as legacy


CELL = {"stop_loss_percent": 0.5, "reward_multiple": 2.0}


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _archive_files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _sha256(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _profile(attempt_id: str) -> dict[str, object]:
    return {
        "profile": {
            "name": attempt_id,
            "instruments": ["EURUSD"],
            "directionMode": "both",
            "notificationThreshold": 80,
            "indicators": [],
        }
    }


def _result() -> dict[str, object]:
    return {"data": {"aggregate": {"matrix_summary": {"robust_cell": CELL}}}}


def _authority(tag: str = "current") -> dict[str, object]:
    return {
        "execution_plan_sha256": f"sha256:{tag:0<64}"[:71],
        "worker_image": legacy.REQUIRED_WORKER_IMAGE,
        "worker_contract_sha256": legacy.REQUIRED_WORKER_CONTRACT_SHA256,
        "lake_semantic_sha256": legacy.REQUIRED_LAKE_SEMANTIC_SHA256,
        "runtime_policy_lock": {"policy_lock_sha256": f"sha256:{tag:0<64}"[:71]},
        "profile_model_source_lock": {
            "source_lock_sha256": f"sha256:{tag:0<64}"[:71]
        },
        "engine_id": "engine",
        "engine_sha256": "sha256:" + "1" * 64,
        "scoring_policy_id": "scoring",
        "scoring_policy_sha256": "sha256:" + "2" * 64,
        "cost_policy_id": "cost",
        "cost_policy_sha256": "sha256:" + "3" * 64,
    }


@pytest.fixture
def comparison_environment(tmp_path: Path) -> dict[str, object]:
    legacy_root = tmp_path / "legacy-runs"
    archive_root = tmp_path / "archive-runs"
    derived_root = tmp_path / "active-runs" / "derived"
    dashboard_root = tmp_path / "trading-dashboard"
    dashboard_root.mkdir(parents=True)
    archive_root.mkdir()

    cohort_ids = [f"cohort-{index:03d}" for index in range(443)]
    external_ids = [f"external-{index:02d}" for index in range(9)]
    missing_ids = [f"missing-{index:02d}" for index in range(3)]
    run_ids = {attempt_id: f"run-{attempt_id}" for attempt_id in cohort_ids + external_ids}
    candidates: list[dict[str, object]] = []
    catalog_rows: list[tuple[str, str]] = []

    for attempt_id in cohort_ids:
        run_id = run_ids[attempt_id]
        profile_old = legacy_root / run_id / "profiles" / "profile.json"
        result_old = legacy_root / run_id / "evals" / "final" / "full-backtest-36mo-result.json"
        curve_old = legacy_root / run_id / "evals" / "final" / "full-backtest-36mo-curve.json"
        profile = archive_root / profile_old.relative_to(legacy_root)
        result = archive_root / result_old.relative_to(legacy_root)
        curve = archive_root / curve_old.relative_to(legacy_root)
        detail = curve.with_name("full-backtest-36mo-recommended-cell-path-detail.json")
        _write_json(profile, _profile(attempt_id))
        _write_json(result, _result())
        _write_json(curve, {"curve": []})
        _write_json(detail, {"cell": CELL})
        _write_json(archive_root / run_id / "run-metadata.json", {})
        candidates.append(
            {
                "attempt_id": attempt_id,
                "source": {
                    "paths": {
                        "profile_path": str(profile_old),
                        "full_backtest_result_path_36m": str(result_old),
                        "full_backtest_curve_path_36m": str(curve_old),
                    },
                    "sha256": {
                        "profile_path": _sha256(profile),
                        "full_backtest_result_path_36m": _sha256(result),
                        "full_backtest_curve_path_36m": _sha256(curve),
                    },
                },
            }
        )
        catalog_rows.append(
            (
                attempt_id,
                json.dumps(
                    {"attempt_id": attempt_id, "run_id": run_id, "candidate_name": attempt_id}
                ),
            )
        )

    for attempt_id in external_ids:
        run_id = run_ids[attempt_id]
        profile_old = legacy_root / run_id / "profiles" / "profile.json"
        profile = archive_root / profile_old.relative_to(legacy_root)
        evidence = archive_root / run_id / "evals" / "final"
        _write_json(profile, _profile(attempt_id))
        _write_json(evidence / "full-backtest-36mo-result.json", _result())
        _write_json(
            evidence / "full-backtest-36mo-recommended-cell-path-detail.json",
            {"cell": CELL},
        )
        _write_json(
            evidence / "full-backtest-36mo-manifest.json",
            {
                "schema": "autoresearch-full-backtest-provenance-v1",
                "attempt_id": attempt_id,
                "source_profile_path": str(profile_old),
            },
        )
        _write_json(archive_root / run_id / "run-metadata.json", {})
        catalog_rows.append(
            (
                attempt_id,
                json.dumps(
                    {
                        "attempt_id": attempt_id,
                        "run_id": run_id,
                        "candidate_name": attempt_id,
                        "profile_path": str(profile_old),
                        "full_backtest_result_path_36m": str(
                            legacy_root
                            / (evidence / "full-backtest-36mo-result.json").relative_to(
                                archive_root
                            )
                        ),
                        "full_backtest_recommended_curve_path_36m": str(
                            legacy_root
                            / (
                                evidence
                                / "full-backtest-36mo-recommended-cell-path-detail.json"
                            ).relative_to(archive_root)
                        ),
                    }
                ),
            )
        )

    catalog = archive_root / "derived" / "attempt-catalog.sqlite"
    catalog.parent.mkdir(parents=True)
    with sqlite3.connect(catalog) as connection:
        connection.execute(
            "CREATE TABLE attempt_rows (attempt_id TEXT PRIMARY KEY, row_json TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO attempt_rows (attempt_id, row_json) VALUES (?, ?)", catalog_rows
        )

    snapshot_old = legacy_root / "derived" / "cohort-candidates.json"
    snapshot = archive_root / snapshot_old.relative_to(legacy_root)
    _write_json(snapshot, {"candidates": candidates})
    cohort_manifest_id = "sha256:" + "a" * 64
    cohort = {
        "manifest_id": cohort_manifest_id,
        "attempt_ids": cohort_ids,
        "source": {
            "candidate_snapshot_path": str(snapshot_old),
            "candidate_snapshot_sha256": _sha256(snapshot),
        },
    }
    cohort_path = archive_root / "derived" / "fixed-cohort.json"
    _write_json(cohort_path, cohort)
    nested_report = {
        "status": "complete",
        "attempt_cohort_manifest_id": cohort_manifest_id,
    }
    nested_report_path = archive_root / "derived" / "nested-report.json"
    _write_json(nested_report_path, nested_report)

    june_ids = cohort_ids[:25] + external_ids[:2] + missing_ids
    july_ids = cohort_ids[:6] + cohort_ids[25:42] + external_ids[2:]
    assert len(june_ids) == len(july_ids) == 30
    assert len(set(june_ids) | set(july_ids)) == 54

    source_rows: dict[str, dict[str, str]] = {}
    for attempt_id in cohort_ids + external_ids:
        source_rows[attempt_id] = {
            "attempt_id": attempt_id,
            "run_id": run_ids[attempt_id],
            "profile_path": str(legacy_root / run_ids[attempt_id] / "profiles" / "profile.json"),
            "candidate_name": attempt_id,
        }
    for attempt_id in missing_ids:
        source_rows[attempt_id] = {
            "attempt_id": attempt_id,
            "run_id": f"run-{attempt_id}",
            "profile_path": str(legacy_root / f"run-{attempt_id}" / "profiles" / "profile.json"),
            "candidate_name": attempt_id,
        }

    membership_paths: dict[str, Path] = {}
    for name, attempt_ids in (("june", june_ids), ("july", july_ids)):
        path = archive_root / "derived" / f"{name}-selected.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["attempt_id", "run_id", "profile_path", "candidate_name"],
            )
            writer.writeheader()
            writer.writerows(source_rows[attempt_id] for attempt_id in attempt_ids)
        membership_paths[name] = path

    sources = {
        "exact_catalog_database": {
            "archive_relative_path": catalog.relative_to(archive_root).as_posix(),
            "projected_archived_path": str(catalog),
            "sha256": _sha256(catalog),
        },
        "fixed_cohort_manifest": {
            "archive_relative_path": cohort_path.relative_to(archive_root).as_posix(),
            "projected_archived_path": str(cohort_path),
            "sha256": _sha256(cohort_path),
        },
        "nested_evidence_report": {
            "archive_relative_path": nested_report_path.relative_to(archive_root).as_posix(),
            "projected_archived_path": str(nested_report_path),
            "sha256": _sha256(nested_report_path),
        },
    }
    for name, key in (("june", "june_selected_membership"), ("july", "july_selected_membership")):
        path = membership_paths[name]
        sources[key] = {
            "archive_relative_path": path.relative_to(archive_root).as_posix(),
            "projected_archived_path": str(path),
            "sha256": _sha256(path),
        }
    identity = {
        "schema": "legacy-controls-manifest-v1",
        "archive_context": {
            "pre_cutover_runs_root": str(legacy_root),
            "projected_archived_runs_root": str(archive_root),
            "reference_only": True,
        },
        "exclusion_contract": {
            "active_seeding": "excluded",
            "active_candidate_scans": "excluded",
            "active_selection": "excluded",
            "active_optimizer_candidates": "excluded",
            "empirical_priors": "excluded",
            "permitted_use": "post_campaign_comparison_only",
            "copy_profiles": False,
            "edit_active_runs": False,
        },
        "categories": {
            "june": {"attempt_ids": june_ids},
            "july": {"attempt_ids": july_ids},
            "overlap": {"attempt_ids": cohort_ids[:6]},
            "campaign_controls": {
                "fixed_cohort": {
                    "manifest_id": cohort_manifest_id,
                    "attempt_count": 443,
                }
            },
        },
        "counts": {
            "june_membership": 30,
            "july_membership": 30,
            "unique_strategy_attempt_ids_across_june_july": 54,
        },
        "source_artifacts": sources,
    }
    identity_hash = "sha256:" + hashlib.sha256(
        json.dumps(
            identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    ).hexdigest()
    controls_path = tmp_path / "legacy-controls.json"
    _write_json(
        controls_path,
        {
            "schema": "legacy-controls-manifest-v1",
            "identity": identity,
            "integrity": {
                "identity_hash_algorithm": "sha256",
                "self_hash": identity_hash,
                "manifest_id": identity_hash,
            },
        },
    )
    authority_plan = tmp_path / "authority-plan.json"
    _write_json(authority_plan, {"plan": "synthetic"})
    return {
        "archive_root": archive_root,
        "controls_path": controls_path,
        "authority_plan": authority_plan,
        "dashboard_root": dashboard_root,
        "config": SimpleNamespace(derived_root=derived_root),
        "first_profile": archive_root / run_ids[cohort_ids[0]] / "profiles" / "profile.json",
        "june_membership": membership_paths["june"],
    }


def _prepare(environment: dict[str, object], monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(legacy, "_load_authority", lambda **_kwargs: _authority())
    return legacy.prepare_legacy_fixed_comparison(
        config=environment["config"],
        legacy_controls=environment["controls_path"],
        archive_runs_root=environment["archive_root"],
        authority_execution_plan=environment["authority_plan"],
        trading_dashboard_root=environment["dashboard_root"],
        comparison_id="legacy-pre-tail-36m-v1",
    )


def test_prepares_exact_fixed_cell_comparison_without_archive_writes(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = comparison_environment["archive_root"]
    before = _archive_files(archive_root)
    prepared = _prepare(comparison_environment, monkeypatch)
    repeated = _prepare(comparison_environment, monkeypatch)

    assert prepared.plan["plan_id"] == repeated.plan["plan_id"]
    assert prepared.plan["preflight"] == {
        "schema": legacy.LEGACY_FIXED_COMPARISON_PREFLIGHT_SCHEMA,
        "plan_id": prepared.plan["plan_id"],
        "task_count": 452,
        "cohort_task_count": 443,
        "out_of_cohort_task_count": 9,
        "unresolved_source_count": 3,
        "june_resolved_count": 27,
        "july_resolved_count": 30,
        "overlap_resolved_count": 6,
        "archive_read_only": True,
        "enqueued": False,
    }
    assert len(prepared.items) == len(prepared.cell_receipts_by_attempt_id) == 452
    assert all(
        task["evidence_plan"]["analysis_window_start"] == legacy.WINDOW_START
        and task["evidence_plan"]["analysis_window_end"] == legacy.WINDOW_END
        and task["evidence_plan"]["requested_horizon_months"] == 36
        and task["evidence_plan"]["execution_cell_sha256"]
        == task["frozen_execution_cell"]["execution_cell_sha256"]
        for task in prepared.plan["tasks"]
    )
    assert [row["status"] for row in prepared.plan["terminal_outcomes"]] == [
        "unresolved_source",
        "unresolved_source",
        "unresolved_source",
    ]
    assert all(
        str(attempt["artifact_dir"]).startswith(str(prepared.output_root))
        for _run_dir, attempt, _row, _metadata in prepared.items
    )
    assert _archive_files(archive_root) == before

    written = legacy.write_legacy_fixed_comparison_plan(prepared)
    assert Path(written["plan_path"]).is_file()
    assert _archive_files(archive_root) == before


def test_rejects_hash_drift_in_frozen_sources(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    comparison_environment["first_profile"].write_text("{}", encoding="utf-8")
    with pytest.raises(legacy.LegacyFixedComparisonError, match="hash differs"):
        _prepare(comparison_environment, monkeypatch)


def test_rejects_membership_hash_drift(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    comparison_environment["june_membership"].write_text("attempt_id\n", encoding="utf-8")
    with pytest.raises(legacy.LegacyFixedComparisonError, match="hash differs"):
        _prepare(comparison_environment, monkeypatch)


def test_catalog_reads_are_immutable_and_ignore_unbound_sqlite_sidecars(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = comparison_environment["archive_root"] / "derived" / "attempt-catalog.sqlite"
    catalog.with_name(f"{catalog.name}-wal").write_bytes(b"unbound WAL")
    prepared = _prepare(comparison_environment, monkeypatch)
    assert len(prepared.items) == 452


def test_execute_revalidates_current_authority_before_creating_outputs(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepare(comparison_environment, monkeypatch)
    monkeypatch.setattr(legacy, "_load_authority", lambda **_kwargs: _authority("drift"))
    called = False

    def unexpected_run(**_kwargs: object) -> tuple[list[dict[str, object]], int, int]:
        nonlocal called
        called = True
        return [], 0, 0

    monkeypatch.setattr(legacy, "run_lab_full_backtests", unexpected_run)
    with pytest.raises(legacy.LegacyFixedComparisonError, match="source, authority, or plan"):
        legacy.execute_legacy_fixed_comparison(
            prepared=prepared,
            config=comparison_environment["config"],
            trading_dashboard_root=comparison_environment["dashboard_root"],
        )
    assert not called
    assert not prepared.output_root.exists()


def test_execute_submits_the_exact_planned_evidence_payloads(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepare(comparison_environment, monkeypatch)
    captured: dict[str, object] = {}
    monkeypatch.setattr(legacy, "_verify_live_lake_identity", lambda **_kwargs: legacy.REQUIRED_LAKE_SEMANTIC_SHA256)

    def capture_run(**kwargs: object) -> tuple[list[dict[str, object]], int, int]:
        captured.update(kwargs)
        return [], 452, 0

    monkeypatch.setattr(legacy, "run_lab_full_backtests", capture_run)
    monkeypatch.setattr(
        legacy,
        "resolve_lab_backtest_config",
        lambda **_kwargs: legacy.LabBacktestConfig(
            gateway_url="https://gateway.invalid",
            worker_contract_hash=legacy.REQUIRED_WORKER_CONTRACT_SHA256,
        ),
    )
    result = legacy.execute_legacy_fixed_comparison(
        prepared=prepared,
        config=comparison_environment["config"],
        trading_dashboard_root=comparison_environment["dashboard_root"],
    )

    assert result["calculated"] == 452
    assert captured["campaign_plan_id"] == prepared.plan["execution_plan_id"]
    assert captured["evidence_plans_by_attempt_id"] == prepared.evidence_plans_by_attempt_id
    assert captured["task_ids_by_attempt_id"] == prepared.task_ids_by_attempt_id


def test_execute_rejects_nested_output_symlink_before_materialization(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = _prepare(comparison_environment, monkeypatch)
    target = Path(prepared.items[0][1]["artifact_dir"])
    target.parent.mkdir(parents=True)
    try:
        target.symlink_to(comparison_environment["archive_root"], target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable on this Windows host: {exc}")
    monkeypatch.setattr(
        legacy,
        "_verify_live_lake_identity",
        lambda **_kwargs: legacy.REQUIRED_LAKE_SEMANTIC_SHA256,
    )
    monkeypatch.setattr(
        legacy,
        "resolve_lab_backtest_config",
        lambda **_kwargs: legacy.LabBacktestConfig(
            gateway_url="https://gateway.invalid",
            worker_contract_hash=legacy.REQUIRED_WORKER_CONTRACT_SHA256,
        ),
    )
    with pytest.raises(legacy.LegacyFixedComparisonError, match="symlink or junction"):
        legacy.execute_legacy_fixed_comparison(
            prepared=prepared,
            config=comparison_environment["config"],
            trading_dashboard_root=comparison_environment["dashboard_root"],
        )


def test_rejects_out_of_cohort_cell_source_hash_drift(
    comparison_environment: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = comparison_environment["archive_root"]
    result_path = (
        archive_root
        / "run-external-00"
        / "evals"
        / "final"
        / "full-backtest-36mo-result.json"
    )
    result_path.write_text("{}", encoding="utf-8")
    with pytest.raises(legacy.LegacyFixedComparisonError, match="source cell"):
        _prepare(comparison_environment, monkeypatch)


def test_authority_requires_exact_lake_identity_in_both_runtime_argument_sets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dashboard_root = tmp_path / "dashboard"
    dashboard_root.mkdir()
    config = SimpleNamespace()
    plan = {
        "bound_contract": {
            "worker_image": legacy.REQUIRED_WORKER_IMAGE,
            "worker_contract_sha256": legacy.REQUIRED_WORKER_CONTRACT_SHA256,
            # A correct value here is intentionally insufficient: it is not the
            # Level C executor's authoritative lake surface.
            "lake_semantic_sha256": legacy.REQUIRED_LAKE_SEMANTIC_SHA256,
        },
        "atlas_arguments": {},
        "playhand_arguments": {},
    }
    monkeypatch.setattr(
        legacy, "load_authoritative_level_c_execution_plan", lambda *_args, **_kwargs: plan
    )
    monkeypatch.setattr(
        legacy,
        "build_runtime_policy_lock",
        lambda *_args, **_kwargs: {
            "schema_version": "test",
            "policy_lock_sha256": "sha256:" + "1" * 64,
        },
    )
    monkeypatch.setattr(
        legacy,
        "build_profile_model_source_lock",
        lambda *_args, **_kwargs: {"source_lock_sha256": "sha256:" + "2" * 64},
    )
    monkeypatch.setattr(
        legacy,
        "policy_lock_provenance",
        lambda _lock: {
            "engine_id": "engine",
            "engine_sha256": "sha256:" + "3" * 64,
            "scoring_policy_id": "scoring",
            "scoring_policy_sha256": "sha256:" + "4" * 64,
            "cost_policy_id": "cost",
            "cost_policy_sha256": "sha256:" + "5" * 64,
        },
    )
    authority_path = tmp_path / "authority.json"
    _write_json(authority_path, {})

    with pytest.raises(legacy.LegacyFixedComparisonError, match="lake identity differs"):
        legacy._load_authority(
            config=config,
            authority_execution_plan=authority_path,
            trading_dashboard_root=dashboard_root,
        )

    plan["atlas_arguments"]["lake_manifest_sha256"] = legacy.REQUIRED_LAKE_SEMANTIC_SHA256
    plan["playhand_arguments"]["lake_manifest_sha256"] = legacy.REQUIRED_LAKE_SEMANTIC_SHA256
    authority = legacy._load_authority(
        config=config,
        authority_execution_plan=authority_path,
        trading_dashboard_root=dashboard_root,
    )
    assert authority["lake_semantic_sha256"] == legacy.REQUIRED_LAKE_SEMANTIC_SHA256

    plan["playhand_arguments"]["lake_manifest_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(legacy.LegacyFixedComparisonError, match="lake identity differs"):
        legacy._load_authority(
            config=config,
            authority_execution_plan=authority_path,
            trading_dashboard_root=dashboard_root,
        )


def test_live_lake_identity_must_match_before_enqueue(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    import requests

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"coverage_sha256": "sha256:" + "f" * 64}

    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: Response())
    with pytest.raises(legacy.LegacyFixedComparisonError, match="live lake semantic identity differs"):
        legacy._verify_live_lake_identity(lake_url="https://lake.invalid", lake_token=None)

def test_cli_registers_comparison_only_command() -> None:
    parser = ar_main.build_parser()
    args = parser.parse_args(
        [
            "plan-legacy-fixed-cell-comparison",
            "--legacy-controls",
            "legacy.json",
            "--archive-runs-root",
            "archive",
            "--authority-execution-plan",
            "authority.json",
            "--trading-dashboard-root",
            "dashboard",
            "--comparison-id",
            "legacy-pre-tail-36m-v1",
            "--dry-run",
        ]
    )
    assert args.command == "plan-legacy-fixed-cell-comparison"
    assert args.dry_run is True
    assert args.execute is False
