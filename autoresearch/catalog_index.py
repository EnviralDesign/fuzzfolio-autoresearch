from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .corpus_tools import (
    FULL_BACKTEST_CALENDAR_CURVE_FILENAME,
    FULL_BACKTEST_CURVE_FILENAME,
    FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
    FULL_BACKTEST_RESULT_FILENAME,
    catalog_summary,
    extract_attempt_catalog_row,
    legacy_validation_cache_dir,
    scrutiny_cache_dir_for_artifact_dir,
)
from .ledger import (
    attempts_path_for_run_dir,
    load_run_attempts,
    load_run_metadata,
    run_metadata_path_for_run_dir,
)
from .scoring import CANONICAL_SCORE_LAB_VERSION


CATALOG_INDEX_SCHEMA_VERSION = 2
CATALOG_EXTRACTION_VERSION = f"2026-07-09.1:{CANONICAL_SCORE_LAB_VERSION}"
CATALOG_COMMIT_INTERVAL_RUNS = 250


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _json_loads(payload: str | None) -> Any:
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _catalog_db_path(config: Any) -> Path:
    path = getattr(config, "attempt_catalog_sqlite_path", None)
    if path is not None:
        return Path(path)
    return Path(config.derived_root) / "attempt-catalog.sqlite"


def _file_signature(path: Path) -> dict[str, Any]:
    path = Path(path)
    try:
        stat = path.stat()
    except OSError:
        return {
            "path": str(path),
            "exists": False,
            "size": 0,
            "mtime_ns": None,
        }
    return {
        "path": str(path),
        "exists": True,
        "size": int(stat.st_size),
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
    }


def _source_signature(run_dir: Path) -> dict[str, Any]:
    return {
        "attempts": _file_signature(attempts_path_for_run_dir(run_dir)),
        "run_metadata": _file_signature(run_metadata_path_for_run_dir(run_dir)),
    }


def _attempt_artifact_paths(
    attempt: dict[str, Any],
    *,
    validation_cache_root: Path | None,
) -> list[Path]:
    paths: list[Path] = []
    artifact_dir_raw = str(attempt.get("artifact_dir") or "").strip()
    if artifact_dir_raw:
        artifact_dir = Path(artifact_dir_raw).resolve()
        paths.extend(
            [
                artifact_dir / "sensitivity-response.json",
                artifact_dir / "best-cell-path-detail.json",
                artifact_dir / "deep-replay-job.json",
                artifact_dir / FULL_BACKTEST_RESULT_FILENAME,
                artifact_dir / FULL_BACKTEST_CURVE_FILENAME,
                artifact_dir / FULL_BACKTEST_CALENDAR_CURVE_FILENAME,
                artifact_dir / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
            ]
        )
        for lookback_months in (12, 36):
            scrutiny_dir = scrutiny_cache_dir_for_artifact_dir(
                artifact_dir, lookback_months
            )
            paths.extend(
                [
                    scrutiny_dir / "manifest.json",
                    scrutiny_dir / "sensitivity-response.json",
                    scrutiny_dir / "best-cell-path-detail.json",
                    scrutiny_dir / "deep-replay-job.json",
                ]
            )

    profile_path_raw = str(attempt.get("profile_path") or "").strip()
    if profile_path_raw:
        paths.append(Path(profile_path_raw).resolve())

    if validation_cache_root is not None:
        run_id = str(attempt.get("run_id") or "").strip()
        if run_id:
            for lookback_months in (12, 36):
                legacy_dir = legacy_validation_cache_dir(
                    validation_cache_root, run_id, lookback_months
                )
                paths.extend(
                    [
                        legacy_dir / "manifest.json",
                        legacy_dir / "sensitivity-response.json",
                        legacy_dir / "best-cell-path-detail.json",
                        legacy_dir / "deep-replay-job.json",
                    ]
                )

    return sorted({path for path in paths}, key=lambda path: str(path))


def _signature_from_paths(
    *,
    run_dir: Path,
    source_signature: dict[str, Any],
    artifact_paths: list[Path],
) -> dict[str, Any]:
    return {
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "extraction_version": CATALOG_EXTRACTION_VERSION,
        "run_id": run_dir.name,
        "sources": source_signature,
        "artifacts": [_file_signature(path) for path in artifact_paths],
    }


def _signature_from_attempts(
    *,
    run_dir: Path,
    attempts: list[dict[str, Any]],
    source_signature: dict[str, Any],
    validation_cache_root: Path | None,
) -> dict[str, Any]:
    artifact_paths: list[Path] = []
    for attempt in attempts:
        artifact_paths.extend(
            _attempt_artifact_paths(
                attempt,
                validation_cache_root=validation_cache_root,
            )
        )
    return _signature_from_paths(
        run_dir=run_dir,
        source_signature=source_signature,
        artifact_paths=sorted({path for path in artifact_paths}, key=lambda path: str(path)),
    )


def _signature_from_existing(
    *,
    run_dir: Path,
    source_signature: dict[str, Any],
    existing_signature: dict[str, Any],
) -> dict[str, Any] | None:
    if existing_signature.get("schema_version") != CATALOG_INDEX_SCHEMA_VERSION:
        return None
    if existing_signature.get("extraction_version") != CATALOG_EXTRACTION_VERSION:
        return None
    if existing_signature.get("run_id") != run_dir.name:
        return None
    existing_sources = existing_signature.get("sources")
    if existing_sources != source_signature:
        return None
    raw_artifacts = existing_signature.get("artifacts")
    if not isinstance(raw_artifacts, list):
        return None
    artifact_paths: list[Path] = []
    for item in raw_artifacts:
        if not isinstance(item, dict):
            return None
        path_raw = str(item.get("path") or "").strip()
        if not path_raw:
            return None
        artifact_paths.append(Path(path_raw))
    return _signature_from_paths(
        run_dir=run_dir,
        source_signature=source_signature,
        artifact_paths=artifact_paths,
    )


def _connect_catalog_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_signatures (
            run_id TEXT PRIMARY KEY,
            signature_json TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    existing_attempt_rows = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'attempt_rows'"
    ).fetchone()
    if existing_attempt_rows is not None:
        columns = {
            str(row[1]): row
            for row in conn.execute("PRAGMA table_info(attempt_rows)").fetchall()
        }
        row_key_info = columns.get("row_key")
        if row_key_info is None or int(row_key_info[5]) == 0:
            conn.execute("DROP TABLE IF EXISTS attempt_rows")
            conn.execute("DROP TABLE IF EXISTS run_signatures")
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
        CREATE TABLE IF NOT EXISTS attempt_rows (
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
        "CREATE INDEX IF NOT EXISTS idx_attempt_rows_attempt_id ON attempt_rows(attempt_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempt_rows_run_id ON attempt_rows(run_id)"
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_attempt_rows_priority
        ON attempt_rows(score_36m DESC, composite_score DESC, attempt_id)
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
        ("schema_version", str(CATALOG_INDEX_SCHEMA_VERSION)),
    )
    conn.commit()
    return conn


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_existing_signatures(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT run_id, signature_json FROM run_signatures"
    ).fetchall()
    signatures: dict[str, dict[str, Any]] = {}
    for run_id, signature_json in rows:
        payload = _json_loads(signature_json)
        if isinstance(payload, dict):
            signatures[str(run_id)] = payload
    return signatures


def _delete_run(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("DELETE FROM attempt_rows WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM run_signatures WHERE run_id = ?", (run_id,))


def _replace_run_rows(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    signature: dict[str, Any],
    rows: list[dict[str, Any]],
) -> None:
    now = datetime.now().astimezone().isoformat()
    _delete_run(conn, run_id)
    conn.executemany(
        """
        INSERT INTO attempt_rows(
            run_id,
            row_key,
            row_index,
            attempt_id,
            composite_score,
            score_36m,
            is_tombstoned,
            has_full_backtest_36m,
            full_backtest_validation_status_36m,
            row_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(row.get("run_id") or run_id),
                f"{row_index:08d}:{str(row.get('attempt_id') or '').strip()}",
                row_index,
                str(row.get("attempt_id") or "").strip(),
                _safe_float(row.get("composite_score")),
                _safe_float(row.get("score_36m")),
                1 if bool(row.get("is_tombstoned")) else 0,
                1 if bool(row.get("has_full_backtest_36m")) else 0,
                str(row.get("full_backtest_validation_status_36m") or "") or None,
                _json_dumps(row),
                now,
            )
            for row_index, row in enumerate(rows)
        ],
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO run_signatures(
            run_id, signature_json, row_count, updated_at
        )
        VALUES (?, ?, ?, ?)
        """,
        (run_id, _json_dumps(signature), len(rows), now),
    )


def _row_select_sql(run_ids: list[str] | None = None) -> tuple[str, list[Any]]:
    params: list[Any] = []
    where = ""
    if run_ids:
        placeholders = ",".join("?" for _ in run_ids)
        where = f"WHERE run_id IN ({placeholders})"
        params.extend(run_ids)
    sql = f"""
        SELECT row_json
        FROM attempt_rows
        {where}
        ORDER BY
            CASE
                WHEN score_36m IS NULL AND composite_score IS NULL THEN 1
                ELSE 0
            END ASC,
            COALESCE(score_36m, composite_score, -1.0e308) DESC,
            COALESCE(composite_score, -1.0e308) DESC,
            attempt_id ASC,
            row_key ASC
    """
    return sql, params


def iter_catalog_rows(
    config: Any,
    *,
    run_ids: list[str] | None = None,
) -> Any:
    db_path = _catalog_db_path(config)
    with _connect_catalog_db(db_path) as conn:
        sql, params = _row_select_sql(run_ids)
        for (row_json,) in conn.execute(sql, params):
            payload = _json_loads(row_json)
            if isinstance(payload, dict):
                yield payload


def _load_rows(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sql, params = _row_select_sql(run_ids)
    for (row_json,) in conn.execute(sql, params):
        payload = _json_loads(row_json)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _catalog_summary_from_db(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
) -> dict[str, Any]:
    where = ""
    params: list[Any] = []
    if run_ids:
        where = f" WHERE run_id IN ({','.join('?' for _ in run_ids)})"
        params.extend(run_ids)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT run_id),
            SUM(CASE WHEN composite_score IS NOT NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN has_full_backtest_36m != 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN full_backtest_validation_status_36m = 'valid' THEN 1 ELSE 0 END),
            SUM(CASE WHEN full_backtest_validation_status_36m = 'invalid' THEN 1 ELSE 0 END)
        FROM attempt_rows
        {where}
        """,
        params,
    ).fetchone()
    if not row:
        return catalog_summary([])
    return {
        "summary_mode": "sqlite_counts",
        "attempt_count": int(row[0] or 0),
        "run_count": int(row[1] or 0),
        "scored_attempt_count": int(row[2] or 0),
        "attempts_with_full_backtest_36m": int(row[3] or 0),
        "attempts_with_valid_full_backtest_36m": int(row[4] or 0),
        "attempts_with_invalid_full_backtest_36m": int(row[5] or 0),
    }


def refresh_incremental_attempt_catalog(
    config: Any,
    *,
    run_dirs: list[Path],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    commit_interval_runs: int = CATALOG_COMMIT_INTERVAL_RUNS,
    load_rows: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    db_path = _catalog_db_path(config)
    validation_cache_root = getattr(config, "validation_cache_root", None)
    validation_cache_root = Path(validation_cache_root) if validation_cache_root else None
    total_runs = len(run_dirs)
    if progress_callback is not None:
        progress_callback({"stage": "start", "total_runs": total_runs})

    reused_runs = 0
    rebuilt_runs = 0
    deleted_runs = 0
    row_count = 0
    pending_commit_runs = 0

    with _connect_catalog_db(db_path) as conn:
        existing_signatures = _load_existing_signatures(conn)
        current_run_ids = {run_dir.name for run_dir in run_dirs}
        for stale_run_id in sorted(set(existing_signatures) - current_run_ids):
            _delete_run(conn, stale_run_id)
            deleted_runs += 1
            pending_commit_runs += 1
        if pending_commit_runs:
            conn.commit()
            pending_commit_runs = 0

        for index, run_dir in enumerate(run_dirs, start=1):
            run_id = run_dir.name
            source_signature = _source_signature(run_dir)
            existing_signature = existing_signatures.get(run_id)
            current_signature = (
                _signature_from_existing(
                    run_dir=run_dir,
                    source_signature=source_signature,
                    existing_signature=existing_signature,
                )
                if existing_signature is not None
                else None
            )
            if current_signature is not None and current_signature == existing_signature:
                reused_runs += 1
                old_count = conn.execute(
                    "SELECT row_count FROM run_signatures WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                row_count += int(old_count[0]) if old_count else 0
                attempt_count = int(old_count[0]) if old_count else 0
            else:
                run_metadata = load_run_metadata(run_dir)
                attempts = load_run_attempts(run_dir)
                rows = [
                    extract_attempt_catalog_row(
                        attempt,
                        run_metadata,
                        validation_cache_root=validation_cache_root,
                    )
                    for attempt in attempts
                ]
                rebuilt_signature = _signature_from_attempts(
                    run_dir=run_dir,
                    attempts=attempts,
                    source_signature=source_signature,
                    validation_cache_root=validation_cache_root,
                )
                _replace_run_rows(
                    conn,
                    run_id=run_id,
                    signature=rebuilt_signature,
                    rows=rows,
                )
                rebuilt_runs += 1
                pending_commit_runs += 1
                attempt_count = len(rows)
                row_count += len(rows)
                if pending_commit_runs >= max(1, int(commit_interval_runs)):
                    conn.commit()
                    pending_commit_runs = 0

            if progress_callback is not None:
                progress_callback(
                    {
                        "stage": "progress",
                        "completed_runs": index,
                        "total_runs": total_runs,
                        "run_id": run_id,
                        "attempt_count": attempt_count,
                        "row_count": row_count,
                        "reused_runs": reused_runs,
                        "rebuilt_runs": rebuilt_runs,
                        "deleted_runs": deleted_runs,
                        "committed": pending_commit_runs == 0,
                    }
                )

        if pending_commit_runs:
            conn.commit()
            pending_commit_runs = 0
        rows = _load_rows(conn) if load_rows else []
        summary = catalog_summary(rows) if load_rows else _catalog_summary_from_db(conn)

    return rows, {
        "source": "sqlite_incremental",
        "path": str(db_path),
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "extraction_version": CATALOG_EXTRACTION_VERSION,
        "refreshed": rebuilt_runs > 0 or deleted_runs > 0,
        "run_count": len(run_dirs),
        "row_count": len(rows) if load_rows else row_count,
        "reused_run_count": reused_runs,
        "rebuilt_run_count": rebuilt_runs,
        "deleted_run_count": deleted_runs,
        "summary": summary,
        "rows_loaded": bool(load_rows),
    }
