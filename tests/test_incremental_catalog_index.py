from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import autoresearch.catalog_index as catalog_index
import autoresearch.__main__ as ar_main
from autoresearch.catalog_index import (
    catalog_summary_from_sqlite,
    iter_catalog_rows,
    refresh_incremental_attempt_catalog,
    stream_catalog_csv_from_sqlite,
    stream_catalog_json_from_sqlite,
)
from autoresearch.corpus_tools import (
    FULL_BACKTEST_CURVE_FILENAME,
    FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
    FULL_BACKTEST_RESULT_FILENAME,
    build_full_backtest_audit,
    catalog_summary,
)
from autoresearch.ledger import ATTEMPTS_FILE_NAME, RUN_METADATA_FILE_NAME
from autoresearch.scoring import CANONICAL_SCORE_LAB_VERSION


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_attempt(run_dir, *, attempt_id: str, score: float = 10.0) -> dict:
    artifact_dir = run_dir / "evals" / attempt_id
    profile_path = artifact_dir / "profile.json"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_json(profile_path, {"profile": {"instruments": ["EURUSD"]}})
    attempt = {
        "run_id": run_dir.name,
        "attempt_id": attempt_id,
        "sequence": 1,
        "created_at": "2026-01-01T00:00:00Z",
        "candidate_name": attempt_id,
        "artifact_dir": str(artifact_dir),
        "profile_ref": f"profile:{attempt_id}",
        "profile_path": str(profile_path),
        "composite_score": score,
        "score_basis": "test",
        "metrics": {},
        "best_summary": {
            "best_cell": {
                "trade_count": 4,
                "resolved_trades": 4,
            },
            "quality_score_payload": {
                "inputs": {
                    "effective_window_months": 3,
                    "trades_per_month": 1.3,
                }
            },
        },
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / ATTEMPTS_FILE_NAME).write_text(
        json.dumps(attempt, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _write_json(run_dir / RUN_METADATA_FILE_NAME, {"run_status": "complete"})
    return attempt


def _write_full_backtest(artifact_dir) -> None:
    artifact_dir = Path(artifact_dir)
    cell = {
        "reward_multiple": 2,
        "stop_loss_percent": 0.01,
        "take_profit_percent": 0.02,
    }
    _write_json(
        artifact_dir / FULL_BACKTEST_RESULT_FILENAME,
        {
            "data": {
                "aggregate": {
                    "analysis_status": "success",
                    "best_cell": cell,
                    "score_lab": {
                        "version": CANONICAL_SCORE_LAB_VERSION,
                        "score": 72.0,
                    },
                }
            }
        },
    )
    _write_json(
        artifact_dir / FULL_BACKTEST_CURVE_FILENAME,
        {
            "curve": {
                "cell": cell,
                "points": [{"x": 0, "y": 0}, {"x": 1, "y": 1}],
            }
        },
    )
    _write_json(
        artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
        {
            "cell": cell,
            "curve": {
                "points": [{"x": 0, "y": 0}, {"x": 1, "y": 1}],
            },
        },
    )


def test_incremental_catalog_reuses_unchanged_runs_and_rebuilds_changed_artifacts(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    attempt_1 = _write_attempt(run_1, attempt_id="attempt-1", score=20.0)
    _write_attempt(run_2, attempt_id="attempt-2", score=15.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2],
    )
    assert [row["attempt_id"] for row in rows] == ["attempt-1", "attempt-2"]
    assert info["rebuilt_run_count"] == 2
    assert info["reused_run_count"] == 0

    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2],
    )
    assert [row["attempt_id"] for row in rows] == ["attempt-1", "attempt-2"]
    assert info["rebuilt_run_count"] == 0
    assert info["reused_run_count"] == 2

    _write_full_backtest(attempt_1["artifact_dir"])
    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2],
    )
    assert info["rebuilt_run_count"] == 1
    assert info["reused_run_count"] == 1
    row_by_attempt = {row["attempt_id"]: row for row in rows}
    assert row_by_attempt["attempt-1"]["has_full_backtest_36m"] is True
    assert row_by_attempt["attempt-2"]["has_full_backtest_36m"] is False

    shutil.rmtree(run_2)
    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1],
    )
    assert [row["attempt_id"] for row in rows] == ["attempt-1"]
    assert info["deleted_run_count"] == 1


def test_incremental_catalog_preserves_duplicate_attempt_ids_within_run(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-duplicates"
    first = _write_attempt(run_dir, attempt_id="same-attempt", score=20.0)
    second = dict(first)
    second["sequence"] = 2
    second["candidate_name"] = "same-attempt-second"
    second_artifact_dir = run_dir / "evals" / "same-attempt-second"
    second_artifact_dir.mkdir(parents=True, exist_ok=True)
    second_profile_path = second_artifact_dir / "profile.json"
    _write_json(second_profile_path, {"profile": {"instruments": ["GBPUSD"]}})
    second["artifact_dir"] = str(second_artifact_dir)
    second["profile_path"] = str(second_profile_path)
    (run_dir / ATTEMPTS_FILE_NAME).write_text(
        "\n".join(
            [
                json.dumps(first, ensure_ascii=True),
                json.dumps(second, ensure_ascii=True),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    rows, info = refresh_incremental_attempt_catalog(config, run_dirs=[run_dir])

    assert info["rebuilt_run_count"] == 1
    assert [row["attempt_id"] for row in rows] == ["same-attempt", "same-attempt"]
    assert {row["candidate_name"] for row in rows} == {
        "same-attempt",
        "same-attempt-second",
    }


def test_incremental_catalog_can_refresh_without_loading_all_rows(
    tmp_path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    _write_attempt(run_1, attempt_id="attempt-1", score=20.0)
    _write_attempt(run_2, attempt_id="attempt-2", score=15.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    def fail_load_rows(*_args, **_kwargs):
        raise AssertionError("load_rows=False should not decode every catalog row")

    monkeypatch.setattr(catalog_index, "_load_rows", fail_load_rows)

    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2],
        load_rows=False,
    )
    streamed_rows = list(iter_catalog_rows(config))

    assert rows == []
    assert info["rows_loaded"] is False
    assert info["row_count"] == 2
    assert info["summary"]["summary_mode"] == "sqlite_counts"
    assert info["summary"]["attempt_count"] == 2
    assert [row["attempt_id"] for row in streamed_rows] == ["attempt-1", "attempt-2"]


def test_incremental_catalog_uses_rust_reuse_scan_to_skip_unchanged_runs(
    tmp_path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    _write_attempt(run_1, attempt_id="attempt-1", score=20.0)
    _write_attempt(run_2, attempt_id="attempt-2", score=15.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )
    refresh_incremental_attempt_catalog(config, run_dirs=[run_1, run_2], load_rows=False)

    def fake_rust_scan(*, db_path, run_dirs):
        return (
            {"run-1": 1},
            {},
            {
                "backend": "rust",
                "available": True,
                "reusable_run_count": 1,
                "migration_candidate_count": 0,
                "scanned_run_count": len(run_dirs),
                "existing_signature_count": 2,
                "invalid_run_count": 1,
                "missing_signature_count": 0,
                "stale_signature_count": 0,
            },
        )

    original_load_run_attempts = catalog_index.load_run_attempts

    def guarded_load_run_attempts(run_dir):
        if Path(run_dir).name == "run-1":
            raise AssertionError("Rust-reused run should not load attempts.jsonl")
        return original_load_run_attempts(run_dir)

    monkeypatch.setattr(catalog_index, "_rust_catalog_scan", fake_rust_scan)
    monkeypatch.setattr(catalog_index, "load_run_attempts", guarded_load_run_attempts)

    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2],
        load_rows=True,
    )

    assert info["reuse_scan"]["backend"] == "rust"
    assert info["reused_run_count"] == 1
    assert info["rebuilt_run_count"] == 1
    assert [row["attempt_id"] for row in rows] == ["attempt-1", "attempt-2"]


def test_incremental_catalog_migrates_known_old_extraction_rows_without_rebuild(
    tmp_path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-1"
    attempt = _write_attempt(run_dir, attempt_id="attempt-1", score=20.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )
    refresh_incremental_attempt_catalog(config, run_dirs=[run_dir], load_rows=False)

    old_version = f"2026-07-08.2:{CANONICAL_SCORE_LAB_VERSION}"
    conn = sqlite3.connect(config.attempt_catalog_sqlite_path)
    try:
        row_json = conn.execute(
            "SELECT row_json FROM attempt_rows WHERE run_id = ?",
            (run_dir.name,),
        ).fetchone()[0]
        payload = json.loads(row_json)
        payload.pop("full_backtest_recommended_curve_path_36m", None)
        payload.pop("has_full_backtest_recommended_curve_36m", None)
        payload["profile_ref"] = "lab-inline:legacy"
        conn.execute(
            "UPDATE attempt_rows SET row_json = ? WHERE run_id = ?",
            (
                json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                run_dir.name,
            ),
        )
        signature_json = conn.execute(
            "SELECT signature_json FROM run_signatures WHERE run_id = ?",
            (run_dir.name,),
        ).fetchone()[0]
        signature = json.loads(signature_json)
        signature["extraction_version"] = old_version
        conn.execute(
            "UPDATE run_signatures SET signature_json = ? WHERE run_id = ?",
            (
                json.dumps(signature, ensure_ascii=True, separators=(",", ":")),
                run_dir.name,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    def fake_rust_scan(*, db_path, run_dirs):
        return (
            {},
            {
                "run-1": {
                    "row_count": 1,
                    "from_extraction_version": old_version,
                }
            },
            {
                "backend": "rust",
                "available": True,
                "reusable_run_count": 0,
                "migration_candidate_count": 1,
                "scanned_run_count": len(run_dirs),
                "existing_signature_count": 1,
                "invalid_run_count": 0,
                "missing_signature_count": 0,
                "stale_signature_count": 0,
            },
        )

    def fail_extract(*_args, **_kwargs):
        raise AssertionError("known old catalog rows should migrate without rebuild")

    monkeypatch.setattr(catalog_index, "_rust_catalog_scan", fake_rust_scan)
    monkeypatch.setattr(catalog_index, "extract_attempt_catalog_row", fail_extract)

    rows, info = refresh_incremental_attempt_catalog(config, run_dirs=[run_dir])

    assert info["migrated_run_count"] == 1
    assert info["rebuilt_run_count"] == 0
    assert rows[0]["profile_ref"] == attempt["profile_ref"]
    assert rows[0]["full_backtest_recommended_curve_path_36m"].endswith(
        "full-backtest-36mo-recommended-cell-path-detail.json"
    )


def test_incremental_catalog_streams_materialized_outputs_from_sqlite(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    _write_attempt(run_1, attempt_id="attempt-low", score=10.0)
    _write_attempt(run_2, attempt_id="attempt-high", score=30.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    refresh_incremental_attempt_catalog(config, run_dirs=[run_1, run_2], load_rows=False)
    json_count = stream_catalog_json_from_sqlite(
        config, derived_root / "attempt-catalog.json"
    )
    csv_count = stream_catalog_csv_from_sqlite(
        config, derived_root / "attempt-catalog.csv"
    )

    rows = json.loads((derived_root / "attempt-catalog.json").read_text(encoding="utf-8"))
    csv_text = (derived_root / "attempt-catalog.csv").read_text(encoding="utf-8")
    summary = catalog_summary_from_sqlite(config)

    assert json_count == 2
    assert csv_count == 2
    assert [row["attempt_id"] for row in rows] == ["attempt-high", "attempt-low"]
    assert csv_text.splitlines()[0].startswith("run_id,attempt_id,")
    assert summary["attempt_count"] == 2
    assert summary["scored_attempt_count"] == 2


def test_sqlite_summary_and_audit_match_streaming_semantics(tmp_path) -> None:
    derived_root = tmp_path / "runs" / "derived"
    config = SimpleNamespace(
        runs_root=tmp_path / "runs",
        derived_root=derived_root,
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )
    rows = [
        {
            "run_id": "run-a",
            "attempt_id": "attempt-a",
            "composite_score": 45.0,
            "score_36m": 50.0,
            "runner": "play_hand_v1",
            "is_canonical_attempt": True,
            "is_canonical_playhand_attempt": True,
            "base_strategy_key": "M5|EURUSD",
            "has_scrutiny_12m": True,
            "has_scrutiny_36m": True,
            "strategy_key_36m": "M5|EURUSD",
            "has_full_backtest_36m": True,
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": True,
            "full_backtest_validation_status_36m": "valid",
            "has_sensitivity_response": True,
        },
        {
            "run_id": "run-a",
            "attempt_id": "attempt-a2",
            "composite_score": 10.0,
            "score_36m": None,
            "runner": "play_hand_v1",
            "is_canonical_attempt": False,
            "is_canonical_playhand_attempt": False,
            "base_strategy_key": "M5|EURUSD",
            "has_scrutiny_12m": False,
            "has_scrutiny_36m": False,
            "strategy_key_36m": None,
            "has_full_backtest_36m": False,
            "has_full_backtest_result_36m": False,
            "has_full_backtest_curve_36m": False,
            "full_backtest_validation_status_36m": "missing",
            "has_sensitivity_response": True,
        },
        {
            "run_id": "run-b",
            "attempt_id": "attempt-b",
            "composite_score": 65.0,
            "score_36m": 70.0,
            "runner": "manual",
            "is_canonical_attempt": True,
            "is_canonical_playhand_attempt": False,
            "base_strategy_key": "H1|GBPUSD",
            "has_scrutiny_12m": True,
            "has_scrutiny_36m": True,
            "strategy_key_36m": "H1|GBPUSD",
            "has_full_backtest_36m": False,
            "has_full_backtest_result_36m": False,
            "has_full_backtest_curve_36m": False,
            "full_backtest_validation_status_36m": "missing",
            "has_sensitivity_response": True,
        },
        {
            "run_id": "run-c",
            "attempt_id": "attempt-c",
            "composite_score": 25.0,
            "score_36m": 20.0,
            "runner": "manual",
            "is_canonical_attempt": True,
            "is_canonical_playhand_attempt": False,
            "base_strategy_key": "M15|XAUUSD",
            "has_scrutiny_12m": False,
            "has_scrutiny_36m": True,
            "strategy_key_36m": "M15|XAUUSD",
            "has_full_backtest_36m": True,
            "has_full_backtest_result_36m": True,
            "has_full_backtest_curve_36m": False,
            "full_backtest_validation_status_36m": "invalid",
            "has_sensitivity_response": False,
        },
    ]
    with catalog_index._connect_catalog_db(config.attempt_catalog_sqlite_path) as conn:
        for index, row in enumerate(rows):
            conn.execute(
                """
                INSERT INTO attempt_rows(
                    run_id, row_key, row_index, attempt_id, composite_score,
                    score_36m, is_tombstoned, has_full_backtest_36m,
                    full_backtest_validation_status_36m, runner,
                    base_strategy_key, strategy_key_36m,
                    is_canonical_attempt, is_canonical_playhand_attempt,
                    has_scrutiny_12m, has_scrutiny_36m,
                    has_full_backtest_result_36m,
                    has_full_backtest_curve_36m, has_sensitivity_response,
                    row_json, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    row["run_id"],
                    f"{index:08d}:{row['attempt_id']}",
                    index,
                    row["attempt_id"],
                    row["composite_score"],
                    row["score_36m"],
                    0,
                    int(bool(row["has_full_backtest_36m"])),
                    row["full_backtest_validation_status_36m"],
                    *catalog_index._audit_column_values(row),
                    json.dumps(row),
                    "now",
                ),
            )
        conn.commit()

    expected_summary = catalog_summary(rows)
    actual_summary = catalog_summary_from_sqlite(config)
    expected_audit = build_full_backtest_audit(rows)
    actual_audit = catalog_index.build_full_backtest_audit_from_sqlite(config)

    assert actual_summary == expected_summary
    assert actual_audit == expected_audit


def test_metadata_only_signature_ack_avoids_catalog_rebuild(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-a"
    _write_attempt(run_dir, attempt_id="attempt-a", score=10.0)
    config = SimpleNamespace(
        runs_root=runs_root,
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )
    refresh_incremental_attempt_catalog(config, run_dirs=[run_dir], load_rows=False)
    metadata_path = run_dir / RUN_METADATA_FILE_NAME
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["play_hand_health"] = {"version": "test"}
    _write_json(metadata_path, metadata)

    acknowledged = catalog_index.acknowledge_run_metadata_only_updates(
        config,
        run_ids=[run_dir.name],
    )
    _rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_dir],
        load_rows=False,
        prune_missing=False,
    )

    assert acknowledged == 1
    assert info["reused_run_count"] == 1
    assert info["rebuilt_run_count"] == 0


def test_build_attempt_catalog_debug_exports_are_explicit(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-1"
    _write_attempt(run_dir, attempt_id="attempt-a", score=10.0)
    config = SimpleNamespace(
        runs_root=runs_root,
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
        attempt_catalog_json_path=derived_root / "attempt-catalog.json",
        attempt_catalog_csv_path=derived_root / "attempt-catalog.csv",
        attempt_catalog_summary_path=derived_root / "attempt-catalog-summary.json",
        attempt_catalog_manifest_path=derived_root / "attempt-catalog-manifest.json",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)

    exit_code = ar_main.cmd_build_attempt_catalog(
        run_ids=None,
        debug_export_json=False,
        debug_export_csv=False,
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert config.attempt_catalog_sqlite_path.exists()
    assert config.attempt_catalog_summary_path.exists()
    assert config.attempt_catalog_manifest_path.exists()
    assert not config.attempt_catalog_json_path.exists()
    assert not config.attempt_catalog_csv_path.exists()
    assert payload["attempt_catalog_json"] is None
    assert payload["attempt_catalog_csv"] is None
    assert payload["debug_exports"]["json_exported"] is False
    assert payload["debug_exports"]["csv_exported"] is False

    exit_code = ar_main.cmd_build_attempt_catalog(
        run_ids=None,
        debug_export_json=True,
        debug_export_csv=True,
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert config.attempt_catalog_json_path.exists()
    assert config.attempt_catalog_csv_path.exists()
    assert payload["debug_exports"]["json_exported"] is True
    assert payload["debug_exports"]["csv_exported"] is True


def test_incremental_catalog_iterates_selected_attempt_ids(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    _write_attempt(run_1, attempt_id="attempt-a", score=10.0)
    _write_attempt(run_2, attempt_id="attempt-b", score=30.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    refresh_incremental_attempt_catalog(config, run_dirs=[run_1, run_2], load_rows=False)
    rows = list(iter_catalog_rows(config, attempt_ids=["attempt-a"]))

    assert [row["attempt_id"] for row in rows] == ["attempt-a"]
    assert catalog_summary_from_sqlite(config, attempt_ids=["attempt-a"])[
        "attempt_count"
    ] == 1


def test_incremental_catalog_subset_refresh_does_not_prune_other_runs(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    _write_attempt(run_1, attempt_id="attempt-a", score=10.0)
    _write_attempt(run_2, attempt_id="attempt-b", score=30.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    refresh_incremental_attempt_catalog(config, run_dirs=[run_1, run_2], load_rows=False)
    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1],
        load_rows=True,
        prune_missing=False,
    )

    assert info["deleted_run_count"] == 0
    assert info["prune_missing"] is False
    assert [row["attempt_id"] for row in rows] == ["attempt-a"]
    assert [row["attempt_id"] for row in iter_catalog_rows(config)] == [
        "attempt-b",
        "attempt-a",
    ]


def test_incremental_catalog_sqlite_order_matches_catalog_priority(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    run_3 = runs_root / "run-3"
    _write_attempt(run_1, attempt_id="composite-high", score=90.0)
    _write_attempt(run_2, attempt_id="scrutiny-low-composite", score=10.0)
    _write_attempt(run_3, attempt_id="scrutiny-high", score=20.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )

    refresh_incremental_attempt_catalog(config, run_dirs=[run_1, run_2, run_3])
    conn = sqlite3.connect(config.attempt_catalog_sqlite_path)
    try:
        for attempt_id, score_36 in (
            ("scrutiny-low-composite", 95.0),
            ("scrutiny-high", 96.0),
        ):
            row_json = conn.execute(
                "SELECT row_json FROM attempt_rows WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()[0]
            payload = json.loads(row_json)
            payload["has_scrutiny_36m"] = True
            payload["score_36m"] = score_36
            conn.execute(
                """
                UPDATE attempt_rows
                SET score_36m = ?, row_json = ?
                WHERE attempt_id = ?
                """,
                (
                    score_36,
                    json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                    attempt_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    streamed_rows = list(iter_catalog_rows(config))
    unordered_rows = list(iter_catalog_rows(config, order_by_priority=False))

    assert [row["attempt_id"] for row in streamed_rows] == [
        "scrutiny-high",
        "scrutiny-low-composite",
        "composite-high",
    ]
    assert [row["attempt_id"] for row in unordered_rows] == [
        "composite-high",
        "scrutiny-low-composite",
        "scrutiny-high",
    ]


def test_incremental_catalog_migrates_old_unique_attempt_schema(tmp_path) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_dir = runs_root / "run-1"
    _write_attempt(run_dir, attempt_id="attempt-1", score=20.0)
    db_path = derived_root / "attempt-catalog.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE run_signatures (
                run_id TEXT PRIMARY KEY,
                signature_json TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE attempt_rows (
                run_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                composite_score REAL,
                score_36m REAL,
                is_tombstoned INTEGER NOT NULL,
                has_full_backtest_36m INTEGER NOT NULL,
                full_backtest_validation_status_36m TEXT,
                row_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, attempt_id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=db_path,
    )

    rows, info = refresh_incremental_attempt_catalog(config, run_dirs=[run_dir])

    assert [row["attempt_id"] for row in rows] == ["attempt-1"]
    assert info["rebuilt_run_count"] == 1
    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(attempt_rows)").fetchall()
        }
    finally:
        conn.close()
    assert "row_key" in columns


def test_catalog_backfills_typed_audit_columns_without_rebuilding_runs(tmp_path) -> None:
    db_path = tmp_path / "derived" / "attempt-catalog.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": "run-a",
        "attempt_id": "attempt-a",
        "runner": "play_hand_v1",
        "base_strategy_key": "M5|EURUSD",
        "strategy_key_36m": "M5|EURUSD",
        "is_canonical_attempt": True,
        "is_canonical_playhand_attempt": True,
        "has_scrutiny_12m": True,
        "has_scrutiny_36m": True,
        "has_full_backtest_36m": True,
        "has_full_backtest_result_36m": True,
        "has_full_backtest_curve_36m": False,
        "has_sensitivity_response": True,
        "composite_score": 60.0,
        "score_36m": 72.0,
        "full_backtest_validation_status_36m": "invalid",
    }
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE attempt_rows (
                run_id TEXT NOT NULL,
                row_key TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                composite_score REAL,
                score_36m REAL,
                is_tombstoned INTEGER NOT NULL,
                has_full_backtest_36m INTEGER NOT NULL,
                full_backtest_validation_status_36m TEXT,
                row_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, row_key)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO attempt_rows VALUES (
                'run-a', '00000000:attempt-a', 0, 'attempt-a', 60.0, 72.0,
                0, 1, 'invalid', ?, 'now'
            )
            """,
            (json.dumps(payload),),
        )
        conn.commit()
    finally:
        conn.close()

    with catalog_index._connect_catalog_db(db_path) as conn:
        typed = conn.execute(
            """
            SELECT runner, base_strategy_key, strategy_key_36m,
                   is_canonical_attempt, has_scrutiny_12m, has_scrutiny_36m,
                   has_full_backtest_result_36m, has_full_backtest_curve_36m,
                   has_sensitivity_response
            FROM attempt_rows
            """
        ).fetchone()
        version = conn.execute(
            "SELECT value FROM metadata WHERE key = 'audit_columns_version'"
        ).fetchone()[0]

    assert typed == (
        "play_hand_v1",
        "M5|EURUSD",
        "M5|EURUSD",
        1,
        1,
        1,
        1,
        0,
        1,
    )
    assert version == str(catalog_index.CATALOG_AUDIT_COLUMNS_VERSION)


def test_incremental_catalog_commits_batches_before_later_failure(
    tmp_path,
    monkeypatch,
) -> None:
    runs_root = tmp_path / "runs"
    derived_root = runs_root / "derived"
    run_1 = runs_root / "run-1"
    run_2 = runs_root / "run-2"
    run_3 = runs_root / "run-3"
    _write_attempt(run_1, attempt_id="attempt-1", score=20.0)
    _write_attempt(run_2, attempt_id="attempt-2", score=19.0)
    _write_attempt(run_3, attempt_id="attempt-3", score=18.0)
    config = SimpleNamespace(
        derived_root=derived_root,
        validation_cache_root=derived_root / "validation-cache",
        attempt_catalog_sqlite_path=derived_root / "attempt-catalog.sqlite",
    )
    original_extract = catalog_index.extract_attempt_catalog_row

    def flaky_extract(attempt, *args, **kwargs):
        if attempt["run_id"] == "run-3":
            raise RuntimeError("boom")
        return original_extract(attempt, *args, **kwargs)

    monkeypatch.setattr(catalog_index, "extract_attempt_catalog_row", flaky_extract)
    try:
        refresh_incremental_attempt_catalog(
            config,
            run_dirs=[run_1, run_2, run_3],
            commit_interval_runs=1,
        )
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("expected injected failure")

    monkeypatch.setattr(
        catalog_index,
        "extract_attempt_catalog_row",
        original_extract,
    )
    rows, info = refresh_incremental_attempt_catalog(
        config,
        run_dirs=[run_1, run_2, run_3],
        commit_interval_runs=1,
    )

    assert [row["attempt_id"] for row in rows] == [
        "attempt-1",
        "attempt-2",
        "attempt-3",
    ]
    assert info["reused_run_count"] == 2
    assert info["rebuilt_run_count"] == 1
