from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import autoresearch.catalog_index as catalog_index
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

    assert [row["attempt_id"] for row in streamed_rows] == [
        "scrutiny-high",
        "scrutiny-low-composite",
        "composite-high",
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
