from __future__ import annotations

import csv
import json
import os
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from .corpus_tools import (
    FULL_BACKTEST_CALENDAR_CURVE_FILENAME,
    FULL_BACKTEST_CURVE_FILENAME,
    FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME,
    FULL_BACKTEST_RESULT_FILENAME,
    catalog_json_cache,
    catalog_priority_key,
    catalog_summary,
    build_full_backtest_audit,
    extract_attempt_catalog_row,
    legacy_validation_cache_dir,
    load_market_data_coverage,
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
CATALOG_AUDIT_COLUMNS_VERSION = 1
RUST_CATALOG_SCAN_ENV = "AUTORESEARCH_RUST_CATALOG_SCAN"
CATALOG_LEDGER_MIGRATION_SOURCE_VERSIONS = {
    f"2026-07-08.2:{CANONICAL_SCORE_LAB_VERSION}",
}
CATALOG_AUDIT_COLUMN_DEFINITIONS = (
    ("runner", "TEXT"),
    ("base_strategy_key", "TEXT"),
    ("strategy_key_36m", "TEXT"),
    ("is_canonical_attempt", "INTEGER"),
    ("is_canonical_playhand_attempt", "INTEGER"),
    ("has_scrutiny_12m", "INTEGER"),
    ("has_scrutiny_36m", "INTEGER"),
    ("has_full_backtest_result_36m", "INTEGER"),
    ("has_full_backtest_curve_36m", "INTEGER"),
    ("has_sensitivity_response", "INTEGER"),
)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _json_loads(payload: str | None) -> Any:
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _atomic_text_path(path: Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )


def _replace_atomic(tmp_path: Path, final_path: Path) -> None:
    Path(tmp_path).replace(final_path)


def _catalog_db_path(config: Any) -> Path:
    path = getattr(config, "attempt_catalog_sqlite_path", None)
    if path is not None:
        return Path(path)
    derived_root = getattr(config, "derived_root", None)
    if derived_root is not None:
        return Path(derived_root) / "attempt-catalog.sqlite"
    return Path(config.runs_root) / "derived" / "attempt-catalog.sqlite"


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
            runner TEXT,
            base_strategy_key TEXT,
            strategy_key_36m TEXT,
            is_canonical_attempt INTEGER,
            is_canonical_playhand_attempt INTEGER,
            has_scrutiny_12m INTEGER,
            has_scrutiny_36m INTEGER,
            has_full_backtest_result_36m INTEGER,
            has_full_backtest_curve_36m INTEGER,
            has_sensitivity_response INTEGER,
            row_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (run_id, row_key)
        )
        """
    )
    columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(attempt_rows)").fetchall()
    }
    for column_name, column_type in CATALOG_AUDIT_COLUMN_DEFINITIONS:
        if column_name not in columns:
            conn.execute(
                f"ALTER TABLE attempt_rows ADD COLUMN {column_name} {column_type}"
            )
    audit_columns_version_row = conn.execute(
        "SELECT value FROM metadata WHERE key = 'audit_columns_version'"
    ).fetchone()
    try:
        audit_columns_version = (
            int(audit_columns_version_row[0]) if audit_columns_version_row else 0
        )
    except (TypeError, ValueError):
        audit_columns_version = 0
    if audit_columns_version != CATALOG_AUDIT_COLUMNS_VERSION:
        conn.execute(
            """
            UPDATE attempt_rows
            SET
                runner = NULLIF(json_extract(row_json, '$.runner'), ''),
                base_strategy_key = NULLIF(
                    json_extract(row_json, '$.base_strategy_key'), ''
                ),
                strategy_key_36m = NULLIF(
                    json_extract(row_json, '$.strategy_key_36m'), ''
                ),
                is_canonical_attempt = COALESCE(
                    json_extract(row_json, '$.is_canonical_attempt'), 0
                ),
                is_canonical_playhand_attempt = COALESCE(
                    json_extract(row_json, '$.is_canonical_playhand_attempt'), 0
                ),
                has_scrutiny_12m = COALESCE(
                    json_extract(row_json, '$.has_scrutiny_12m'), 0
                ),
                has_scrutiny_36m = COALESCE(
                    json_extract(row_json, '$.has_scrutiny_36m'), 0
                ),
                has_full_backtest_result_36m = COALESCE(
                    json_extract(row_json, '$.has_full_backtest_result_36m'), 0
                ),
                has_full_backtest_curve_36m = COALESCE(
                    json_extract(row_json, '$.has_full_backtest_curve_36m'), 0
                ),
                has_sensitivity_response = COALESCE(
                    json_extract(row_json, '$.has_sensitivity_response'), 0
                )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            ("audit_columns_version", str(CATALOG_AUDIT_COLUMNS_VERSION)),
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
        """
        CREATE INDEX IF NOT EXISTS idx_attempt_rows_scrutiny_score
        ON attempt_rows(has_scrutiny_36m, score_36m)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_attempt_rows_audit_cover
        ON attempt_rows(
            run_id,
            attempt_id,
            composite_score,
            score_36m,
            runner,
            is_canonical_attempt,
            is_canonical_playhand_attempt,
            base_strategy_key,
            has_scrutiny_12m,
            has_scrutiny_36m,
            strategy_key_36m,
            has_full_backtest_36m,
            full_backtest_validation_status_36m,
            has_full_backtest_result_36m,
            has_full_backtest_curve_36m,
            has_sensitivity_response
        )
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


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _audit_column_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _optional_text(row.get("runner")),
        _optional_text(row.get("base_strategy_key")),
        _optional_text(row.get("strategy_key_36m")),
        1 if bool(row.get("is_canonical_attempt")) else 0,
        1 if bool(row.get("is_canonical_playhand_attempt")) else 0,
        1 if bool(row.get("has_scrutiny_12m")) else 0,
        1 if bool(row.get("has_scrutiny_36m")) else 0,
        1 if bool(row.get("has_full_backtest_result_36m")) else 0,
        1 if bool(row.get("has_full_backtest_curve_36m")) else 0,
        1 if bool(row.get("has_sensitivity_response")) else 0,
    )


def _query_signature_rows(
    conn: sqlite3.Connection,
    *,
    columns: str,
    run_ids: list[str] | None,
) -> list[tuple[Any, ...]]:
    if run_ids is None:
        return conn.execute(f"SELECT {columns} FROM run_signatures").fetchall()
    rows: list[tuple[Any, ...]] = []
    for offset in range(0, len(run_ids), 900):
        chunk = run_ids[offset : offset + 900]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"SELECT {columns} FROM run_signatures WHERE run_id IN ({placeholders})",
                chunk,
            ).fetchall()
        )
    return rows


def _load_existing_signatures(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    rows = _query_signature_rows(
        conn,
        columns="run_id, signature_json",
        run_ids=run_ids,
    )
    signatures: dict[str, dict[str, Any]] = {}
    for run_id, signature_json in rows:
        payload = _json_loads(signature_json)
        if isinstance(payload, dict):
            signatures[str(run_id)] = payload
    return signatures


def _load_existing_signature(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT signature_json FROM run_signatures WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    payload = _json_loads(row[0])
    return payload if isinstance(payload, dict) else None


def _load_existing_signature_counts(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
) -> dict[str, int]:
    try:
        rows = _query_signature_rows(
            conn,
            columns="run_id, row_count",
            run_ids=run_ids,
        )
    except sqlite3.OperationalError:
        return {}
    return {str(run_id): int(row_count or 0) for run_id, row_count in rows}


def acknowledge_run_metadata_only_updates(
    config: Any,
    *,
    run_ids: list[str],
) -> int:
    """Advance cached metadata signatures after a non-indexed metadata-only edit."""
    wanted = sorted({str(run_id).strip() for run_id in run_ids if str(run_id).strip()})
    if not wanted:
        return 0
    updated = 0
    now = datetime.now().astimezone().isoformat()
    with _connect_catalog_db(_catalog_db_path(config)) as conn:
        signatures = _load_existing_signatures(conn, run_ids=wanted)
        for run_id in wanted:
            signature = signatures.get(run_id)
            if not isinstance(signature, dict):
                continue
            if signature.get("schema_version") != CATALOG_INDEX_SCHEMA_VERSION:
                continue
            if signature.get("extraction_version") != CATALOG_EXTRACTION_VERSION:
                continue
            sources = signature.get("sources")
            if not isinstance(sources, dict):
                continue
            signature = dict(signature)
            signature["sources"] = {
                **sources,
                "run_metadata": _file_signature(
                    run_metadata_path_for_run_dir(Path(config.runs_root) / run_id)
                ),
            }
            conn.execute(
                "UPDATE run_signatures SET signature_json = ?, updated_at = ? WHERE run_id = ?",
                (_json_dumps(signature), now, run_id),
            )
            updated += 1
        conn.commit()
    return updated


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _rust_catalog_scanner_manifest() -> Path:
    return _repo_root() / "rust" / "catalog-indexer" / "Cargo.toml"


def _rust_catalog_scanner_binary() -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return (
        _repo_root()
        / "rust"
        / "catalog-indexer"
        / "target"
        / "release"
        / f"catalog-indexer-rs{suffix}"
    )


def _rust_catalog_scanner_sources() -> list[Path]:
    root = _repo_root() / "rust" / "catalog-indexer"
    return [
        root / "Cargo.toml",
        root / "src" / "main.rs",
    ]


def _needs_rebuild(binary_path: Path, source_paths: list[Path]) -> bool:
    if not binary_path.exists():
        return True
    try:
        binary_mtime = binary_path.stat().st_mtime
    except OSError:
        return True
    for source_path in source_paths:
        try:
            if source_path.stat().st_mtime > binary_mtime:
                return True
        except OSError:
            return True
    return False


def _ensure_rust_catalog_scanner_binary() -> Path | None:
    setting = str(os.environ.get(RUST_CATALOG_SCAN_ENV) or "auto").strip().lower()
    if setting in {"0", "false", "off", "disabled", "python"}:
        return None
    manifest = _rust_catalog_scanner_manifest()
    if not manifest.exists():
        if setting in {"1", "true", "on", "required", "rust"}:
            raise RuntimeError(f"Rust catalog scanner manifest not found: {manifest}")
        return None
    binary_path = _rust_catalog_scanner_binary()
    source_paths = _rust_catalog_scanner_sources()
    if not _needs_rebuild(binary_path, source_paths):
        return binary_path
    command = [
        "cargo",
        "build",
        "--quiet",
        "--release",
        "--manifest-path",
        str(manifest),
    ]
    try:
        proc = subprocess.run(
            command,
            cwd=str(_repo_root()),
            text=True,
            encoding="utf-8",
            capture_output=True,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired):
        if setting in {"1", "true", "on", "required", "rust"}:
            raise
        return None
    if proc.returncode != 0:
        if setting in {"1", "true", "on", "required", "rust"}:
            raise RuntimeError(
                "failed to build Rust catalog scanner: "
                f"stdout={proc.stdout[-1600:]} stderr={proc.stderr[-1600:]}"
            )
        return None
    return binary_path if binary_path.exists() else None


def _rust_catalog_scan(
    *,
    db_path: Path,
    run_dirs: list[Path],
) -> tuple[dict[str, int], dict[str, dict[str, Any]], dict[str, Any]]:
    scanner = _ensure_rust_catalog_scanner_binary()
    if scanner is None:
        return {}, {}, {
            "backend": "python",
            "available": False,
            "reusable_run_count": 0,
            "migration_candidate_count": 0,
        }
    payload = {
        "db_path": str(db_path),
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "extraction_version": CATALOG_EXTRACTION_VERSION,
        "run_dirs": [
            {
                "run_id": run_dir.name,
                "path": str(run_dir),
            }
            for run_dir in run_dirs
        ],
    }
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        dir=db_path.parent,
        prefix=".catalog-rust-scan.",
        suffix=".json",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=True, separators=(",", ":"))
        input_path = Path(handle.name)
    try:
        proc = subprocess.run(
            [str(scanner), str(input_path)],
            cwd=str(_repo_root()),
            text=True,
            encoding="utf-8",
            capture_output=True,
            timeout=300,
        )
    finally:
        try:
            input_path.unlink(missing_ok=True)
        except OSError:
            pass
    if proc.returncode != 0:
        raise RuntimeError(
            "Rust catalog scanner failed: "
            f"stdout={proc.stdout[-1600:]} stderr={proc.stderr[-1600:]}"
        )
    try:
        output = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Rust catalog scanner returned invalid JSON: {proc.stdout[-1600:]}"
        ) from exc
    reusable_runs = output.get("reusable_runs")
    if not isinstance(reusable_runs, list):
        raise RuntimeError("Rust catalog scanner output missing reusable_runs list")
    reusable: dict[str, int] = {}
    for item in reusable_runs:
        if not isinstance(item, dict):
            continue
        run_id = str(item.get("run_id") or "").strip()
        if not run_id:
            continue
        reusable[run_id] = int(item.get("row_count") or 0)
    migration_runs = output.get("migration_runs")
    if not isinstance(migration_runs, list):
        raise RuntimeError("Rust catalog scanner output missing migration_runs list")
    migrations: dict[str, dict[str, Any]] = {}
    for item in migration_runs:
        if not isinstance(item, dict):
            continue
        run_id = str(item.get("run_id") or "").strip()
        if not run_id:
            continue
        migrations[run_id] = {
            "row_count": int(item.get("row_count") or 0),
            "from_extraction_version": str(item.get("from_extraction_version") or ""),
        }
    info = {
        "backend": "rust",
        "available": True,
        "reusable_run_count": len(reusable),
        "migration_candidate_count": len(migrations),
        "scanned_run_count": int(output.get("scanned_run_count") or 0),
        "existing_signature_count": int(output.get("existing_signature_count") or 0),
        "invalid_run_count": int(output.get("invalid_run_count") or 0),
        "missing_signature_count": int(output.get("missing_signature_count") or 0),
        "stale_signature_count": int(output.get("stale_signature_count") or 0),
    }
    return reusable, migrations, info


def _delete_run(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("DELETE FROM attempt_rows WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM run_signatures WHERE run_id = ?", (run_id,))


def _recommended_curve_path_for_row(row: dict[str, Any]) -> str | None:
    artifact_dir_raw = str(row.get("artifact_dir") or "").strip()
    if not artifact_dir_raw:
        return None
    return str(Path(artifact_dir_raw) / FULL_BACKTEST_RECOMMENDED_CURVE_FILENAME)


def _migrate_run_rows_from_ledger(
    conn: sqlite3.Connection,
    *,
    run_dir: Path,
    existing_signature: dict[str, Any],
    from_extraction_version: str,
) -> int | None:
    if from_extraction_version not in CATALOG_LEDGER_MIGRATION_SOURCE_VERSIONS:
        return None
    run_id = run_dir.name
    rows = conn.execute(
        """
        SELECT row_key, row_index, attempt_id, row_json
        FROM attempt_rows
        WHERE run_id = ?
        ORDER BY row_key
        """,
        (run_id,),
    ).fetchall()
    attempts = load_run_attempts(run_dir)
    if len(rows) != len(attempts):
        return None
    migrated: list[tuple[str, str]] = []
    for position, (row_key, row_index, attempt_id, row_json) in enumerate(rows):
        payload = _json_loads(row_json)
        if not isinstance(payload, dict):
            return None
        try:
            row_index_int = int(row_index)
        except (TypeError, ValueError):
            return None
        if row_index_int != position:
            return None
        attempt = attempts[position]
        if str(attempt.get("attempt_id") or "") != str(attempt_id or ""):
            return None
        if str(payload.get("attempt_id") or "") != str(attempt_id or ""):
            return None
        recommended_curve_path = _recommended_curve_path_for_row(payload)
        payload["full_backtest_recommended_curve_path_36m"] = recommended_curve_path
        payload["has_full_backtest_recommended_curve_36m"] = bool(
            recommended_curve_path and Path(recommended_curve_path).exists()
        )
        payload["profile_ref"] = attempt.get("profile_ref")
        migrated.append((str(row_key), _json_dumps(payload)))

    now = datetime.now().astimezone().isoformat()
    conn.executemany(
        """
        UPDATE attempt_rows
        SET row_json = ?, updated_at = ?
        WHERE run_id = ? AND row_key = ?
        """,
        [
            (
                row_json,
                now,
                run_id,
                row_key,
            )
            for row_key, row_json in migrated
        ],
    )
    signature = dict(existing_signature)
    signature["extraction_version"] = CATALOG_EXTRACTION_VERSION
    conn.execute(
        """
        UPDATE run_signatures
        SET signature_json = ?, updated_at = ?
        WHERE run_id = ?
        """,
        (_json_dumps(signature), now, run_id),
    )
    return len(migrated)


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
            runner,
            base_strategy_key,
            strategy_key_36m,
            is_canonical_attempt,
            is_canonical_playhand_attempt,
            has_scrutiny_12m,
            has_scrutiny_36m,
            has_full_backtest_result_36m,
            has_full_backtest_curve_36m,
            has_sensitivity_response,
            row_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                *_audit_column_values(row),
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


def _row_select_sql(
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
    *,
    order_by_priority: bool = True,
) -> tuple[str, list[Any]]:
    params: list[Any] = []
    clauses: list[str] = []
    if run_ids:
        placeholders = ",".join("?" for _ in run_ids)
        clauses.append(f"run_id IN ({placeholders})")
        params.extend(run_ids)
    if attempt_ids:
        placeholders = ",".join("?" for _ in attempt_ids)
        clauses.append(f"attempt_id IN ({placeholders})")
        params.extend(attempt_ids)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_sql = """
        ORDER BY
            CASE
                WHEN score_36m IS NULL AND composite_score IS NULL THEN 1
                ELSE 0
            END ASC,
            COALESCE(score_36m, composite_score, -1.0e308) DESC,
            COALESCE(composite_score, -1.0e308) DESC,
            attempt_id ASC,
            row_key ASC
    """ if order_by_priority else "ORDER BY run_id ASC, row_key ASC"
    sql = f"""
        SELECT row_json
        FROM attempt_rows
        {where}
        {order_sql}
    """
    return sql, params


def iter_catalog_rows(
    config: Any,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
    order_by_priority: bool = True,
) -> Any:
    db_path = _catalog_db_path(config)
    with _connect_catalog_db(db_path) as conn:
        sql, params = _row_select_sql(
            run_ids,
            attempt_ids,
            order_by_priority=order_by_priority,
        )
        for (row_json,) in conn.execute(sql, params):
            payload = _json_loads(row_json)
            if isinstance(payload, dict):
                yield payload


def iter_full_backtest_rows(
    config: Any,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    clauses = ["has_full_backtest_36m != 0"]
    params: list[Any] = []
    if run_ids:
        clauses.append(f"run_id IN ({','.join('?' for _ in run_ids)})")
        params.extend(run_ids)
    if attempt_ids:
        clauses.append(f"attempt_id IN ({','.join('?' for _ in attempt_ids)})")
        params.extend(attempt_ids)
    with _connect_catalog_db(_catalog_db_path(config)) as conn:
        cursor = conn.execute(
            f"""
            SELECT row_json
            FROM attempt_rows
            WHERE {' AND '.join(clauses)}
            ORDER BY run_id ASC, row_key ASC
            """,
            params,
        )
        for (row_json,) in cursor:
            payload = _json_loads(row_json)
            if isinstance(payload, dict):
                yield payload


def catalog_summary_from_sqlite(
    config: Any,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> dict[str, Any]:
    with _connect_catalog_db(_catalog_db_path(config)) as conn:
        return _complete_catalog_summary_from_db(
            conn,
            run_ids=run_ids,
            attempt_ids=attempt_ids,
        )


def _catalog_filter_sql(
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if run_ids:
        clauses.append(f"run_id IN ({','.join('?' for _ in run_ids)})")
        params.extend(run_ids)
    if attempt_ids:
        clauses.append(f"attempt_id IN ({','.join('?' for _ in attempt_ids)})")
        params.extend(attempt_ids)
    return (f"WHERE {' AND '.join(clauses)}" if clauses else ""), params


def _complete_catalog_summary_from_db(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> dict[str, Any]:
    where, params = _catalog_filter_sql(run_ids=run_ids, attempt_ids=attempt_ids)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT run_id),
            SUM(composite_score IS NOT NULL),
            SUM(COALESCE(runner = 'play_hand_v1', 0)),
            SUM(COALESCE(is_canonical_attempt, 0)),
            SUM(COALESCE(is_canonical_playhand_attempt, 0)),
            COUNT(DISTINCT base_strategy_key),
            SUM(COALESCE(has_scrutiny_12m, 0)),
            SUM(COALESCE(has_scrutiny_36m, 0)),
            COUNT(DISTINCT CASE
                WHEN COALESCE(has_scrutiny_36m, 0)
                THEN strategy_key_36m
            END),
            COUNT(DISTINCT CASE
                WHEN has_full_backtest_36m != 0
                THEN strategy_key_36m
            END),
            SUM(has_full_backtest_36m != 0),
            SUM(full_backtest_validation_status_36m = 'valid'),
            SUM(full_backtest_validation_status_36m = 'invalid'),
            SUM(
                COALESCE(has_full_backtest_result_36m, 0)
                != COALESCE(has_full_backtest_curve_36m, 0)
            ),
            SUM(COALESCE(has_sensitivity_response, 0)),
            SUM(
                COALESCE(has_scrutiny_36m, 0)
                AND score_36m IS NOT NULL
            ),
            SUM(
                COALESCE(has_scrutiny_36m, 0)
                AND score_36m >= 40.0
            ),
            SUM(
                COALESCE(has_scrutiny_36m, 0)
                AND score_36m >= 60.0
            ),
            SUM(
                COALESCE(has_scrutiny_36m, 0)
                AND score_36m >= 70.0
            ),
            SUM(has_full_backtest_36m != 0 AND score_36m >= 40.0),
            SUM(has_full_backtest_36m != 0 AND score_36m >= 60.0),
            SUM(has_full_backtest_36m != 0 AND score_36m >= 70.0)
        FROM attempt_rows INDEXED BY idx_attempt_rows_audit_cover
        {where}
        """,
        params,
    ).fetchone()
    if not row or int(row[0] or 0) == 0:
        return catalog_summary([])

    score_count = int(row[16] or 0)
    median_score_36: float | None = None
    if score_count:
        median_where = (
            f"{where} AND " if where else "WHERE "
        ) + "COALESCE(has_scrutiny_36m, 0) != 0 AND score_36m IS NOT NULL"
        median_row = conn.execute(
            f"""
            SELECT score_36m
            FROM attempt_rows
            {median_where}
            ORDER BY score_36m ASC
            LIMIT 1 OFFSET ?
            """,
            [*params, score_count // 2],
        ).fetchone()
        if median_row is not None and median_row[0] is not None:
            median_score_36 = float(median_row[0])

    attempt_count = int(row[0] or 0)
    scrutiny_36_count = int(row[8] or 0)
    full_backtest_36_count = int(row[11] or 0)
    valid_full_backtest_36_count = int(row[12] or 0)
    return {
        "run_count": int(row[1] or 0),
        "attempt_count": attempt_count,
        "scored_attempt_count": int(row[2] or 0),
        "playhand_attempt_count": int(row[3] or 0),
        "canonical_attempt_count": int(row[4] or 0),
        "canonical_playhand_attempt_count": int(row[5] or 0),
        "unique_base_strategy_count": int(row[6] or 0),
        "unique_strategy_count_36m": int(row[9] or 0),
        "unique_full_backtest_strategy_count_36m": int(row[10] or 0),
        "attempts_with_scrutiny_12m": int(row[7] or 0),
        "attempts_with_scrutiny_36m": scrutiny_36_count,
        "attempts_with_full_backtest_36m": full_backtest_36_count,
        "attempts_with_valid_full_backtest_36m": valid_full_backtest_36_count,
        "attempts_with_invalid_full_backtest_36m": int(row[13] or 0),
        "attempts_with_partial_full_backtest_36m": int(row[14] or 0),
        "attempts_with_base_sensitivity": int(row[15] or 0),
        "scrutiny_36m_coverage_ratio": (
            float(scrutiny_36_count) / float(attempt_count)
        ),
        "full_backtest_36m_coverage_ratio": (
            float(full_backtest_36_count) / float(attempt_count)
        ),
        "valid_full_backtest_36m_coverage_ratio": (
            float(valid_full_backtest_36_count) / float(attempt_count)
        ),
        "full_backtest_36m_vs_scrutiny_coverage_ratio": (
            float(full_backtest_36_count) / float(scrutiny_36_count)
            if scrutiny_36_count > 0
            else None
        ),
        "valid_full_backtest_36m_vs_scrutiny_coverage_ratio": (
            float(valid_full_backtest_36_count) / float(scrutiny_36_count)
            if scrutiny_36_count > 0
            else None
        ),
        "median_score_36m": median_score_36,
        "score_36m_ge_40": int(row[17] or 0),
        "score_36m_ge_60": int(row[18] or 0),
        "score_36m_ge_70": int(row[19] or 0),
        "full_backtest_36m_ge_40": int(row[20] or 0),
        "full_backtest_36m_ge_60": int(row[21] or 0),
        "full_backtest_36m_ge_70": int(row[22] or 0),
    }


def build_full_backtest_audit_from_sqlite(
    config: Any,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
    invalid_example_limit: int = 25,
    pending_example_limit: int = 25,
) -> dict[str, Any]:
    with _connect_catalog_db(_catalog_db_path(config)) as conn:
        summary = _complete_catalog_summary_from_db(
            conn,
            run_ids=run_ids,
            attempt_ids=attempt_ids,
        )
        where, params = _catalog_filter_sql(run_ids=run_ids, attempt_ids=attempt_ids)

        def example_rows(condition: str, limit: int) -> list[dict[str, Any]]:
            if limit <= 0:
                return []
            filtered_where = f"{where} AND {condition}" if where else f"WHERE {condition}"
            rows: list[dict[str, Any]] = []
            for (row_json,) in conn.execute(
                f"""
                SELECT row_json
                FROM attempt_rows
                {filtered_where}
                ORDER BY
                    COALESCE(score_36m, -1.0e308) DESC,
                    COALESCE(composite_score, -1.0e308) DESC,
                    run_id ASC,
                    row_key ASC
                LIMIT ?
                """,
                [*params, int(limit)],
            ):
                payload = _json_loads(row_json)
                if isinstance(payload, dict):
                    rows.append(payload)
            return rows

        invalid_rows = example_rows(
            "full_backtest_validation_status_36m = 'invalid'",
            invalid_example_limit,
        )
        pending_rows = example_rows(
            "COALESCE(has_scrutiny_36m, 0) != 0 "
            "AND has_full_backtest_36m = 0",
            pending_example_limit,
        )
    return build_full_backtest_audit(
        [*invalid_rows, *pending_rows],
        invalid_example_limit=invalid_example_limit,
        pending_example_limit=pending_example_limit,
        summary_override=summary,
    )


def stream_catalog_json_from_sqlite(
    config: Any,
    output_path: Path,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> int:
    output_path = Path(output_path)
    handle = _atomic_text_path(output_path)
    tmp_path = Path(handle.name)
    count = 0
    try:
        with handle:
            handle.write("[")
            db_path = _catalog_db_path(config)
            with _connect_catalog_db(db_path) as conn:
                sql, params = _row_select_sql(run_ids, attempt_ids)
                for (row_json,) in conn.execute(sql, params):
                    if count:
                        handle.write(",")
                    handle.write(str(row_json))
                    count += 1
            handle.write("]")
        _replace_atomic(tmp_path, output_path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return count


def stream_catalog_csv_from_sqlite(
    config: Any,
    output_path: Path,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> int:
    output_path = Path(output_path)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in iter_catalog_rows(config, run_ids=run_ids, attempt_ids=attempt_ids):
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)

    handle = _atomic_text_path(output_path)
    tmp_path = Path(handle.name)
    count = 0
    try:
        with handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in iter_catalog_rows(
                config, run_ids=run_ids, attempt_ids=attempt_ids
            ):
                serialized: dict[str, Any] = {}
                for key in fieldnames:
                    value = row.get(key)
                    if isinstance(value, (list, dict)):
                        serialized[key] = json.dumps(value, ensure_ascii=True)
                    else:
                        serialized[key] = value
                writer.writerow(serialized)
                count += 1
        _replace_atomic(tmp_path, output_path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return count


def _load_rows(
    conn: sqlite3.Connection,
    *,
    run_ids: list[str] | None = None,
    attempt_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sql, params = _row_select_sql(run_ids, attempt_ids)
    for (row_json,) in conn.execute(sql, params):
        payload = _json_loads(row_json)
        if isinstance(payload, dict):
            rows.append(payload)
    rows.sort(key=catalog_priority_key)
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
    prune_missing: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    db_path = _catalog_db_path(config)
    validation_cache_root = getattr(config, "validation_cache_root", None)
    validation_cache_root = Path(validation_cache_root) if validation_cache_root else None
    repo_root = getattr(config, "repo_root", None)
    trading_dashboard_root = (
        Path(repo_root).parent / "Trading-Dashboard" if repo_root is not None else None
    )
    market_data_coverage = load_market_data_coverage(
        trading_dashboard_root
        if trading_dashboard_root is not None and trading_dashboard_root.exists()
        else None
    )
    total_runs = len(run_dirs)
    if progress_callback is not None:
        progress_callback({"stage": "start", "total_runs": total_runs})

    reused_runs = 0
    migrated_runs = 0
    rebuilt_runs = 0
    deleted_runs = 0
    row_count = 0
    pending_commit_runs = 0
    reuse_scan_info: dict[str, Any] = {
        "backend": "python",
        "available": False,
        "reusable_run_count": 0,
        "migration_candidate_count": 0,
    }

    with _connect_catalog_db(db_path) as conn:
        current_run_ids = {run_dir.name for run_dir in run_dirs}
        scoped_run_ids = None if prune_missing else sorted(current_run_ids)
        existing_signature_counts = _load_existing_signature_counts(
            conn,
            run_ids=scoped_run_ids,
        )
        existing_signatures: dict[str, dict[str, Any]] = {}
        rust_reusable_counts: dict[str, int] = {}
        rust_migration_info: dict[str, dict[str, Any]] = {}
        if existing_signature_counts and prune_missing:
            try:
                if progress_callback is not None:
                    progress_callback(
                        {
                            "stage": "reuse_scan_start",
                            "backend": "rust",
                            "total_runs": total_runs,
                            "existing_signature_count": len(existing_signature_counts),
                        }
                    )
                (
                    rust_reusable_counts,
                    rust_migration_info,
                    reuse_scan_info,
                ) = _rust_catalog_scan(
                    db_path=db_path,
                    run_dirs=run_dirs,
                )
                if progress_callback is not None:
                    progress_callback(
                        {
                            "stage": "reuse_scan_done",
                            **reuse_scan_info,
                        }
                    )
            except Exception as exc:
                required = str(os.environ.get(RUST_CATALOG_SCAN_ENV) or "").strip().lower() in {
                    "1",
                    "true",
                    "on",
                    "required",
                    "rust",
                }
                if required:
                    raise
                reuse_scan_info = {
                    "backend": "python",
                    "available": False,
                    "reusable_run_count": 0,
                    "migration_candidate_count": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                if progress_callback is not None:
                    progress_callback(
                        {
                            "stage": "reuse_scan_unavailable",
                            **reuse_scan_info,
                        }
                    )
                rust_reusable_counts = {}
                rust_migration_info = {}
        use_rust_reuse_scan = bool(reuse_scan_info.get("available")) and (
            reuse_scan_info.get("backend") == "rust"
        )
        if not use_rust_reuse_scan:
            existing_signatures = _load_existing_signatures(
                conn,
                run_ids=scoped_run_ids,
            )
        if prune_missing:
            existing_run_ids = (
                set(existing_signature_counts)
                if use_rust_reuse_scan
                else set(existing_signatures)
            )
            for stale_run_id in sorted(existing_run_ids - current_run_ids):
                _delete_run(conn, stale_run_id)
                deleted_runs += 1
                pending_commit_runs += 1
            if pending_commit_runs:
                conn.commit()
                pending_commit_runs = 0

        for index, run_dir in enumerate(run_dirs, start=1):
            run_id = run_dir.name
            rust_reused_count = rust_reusable_counts.get(run_id)
            if rust_reused_count is not None:
                reused_runs += 1
                row_count += int(rust_reused_count)
                attempt_count = int(rust_reused_count)
            else:
                source_signature = _source_signature(run_dir)
                migration_info = rust_migration_info.get(run_id)
                migrated_count: int | None = None
                if migration_info is not None:
                    migration_signature = _load_existing_signature(conn, run_id)
                    if migration_signature is not None:
                        migrated_count = _migrate_run_rows_from_ledger(
                            conn,
                            run_dir=run_dir,
                            existing_signature=migration_signature,
                            from_extraction_version=str(
                                migration_info.get("from_extraction_version") or ""
                            ),
                        )
                if migrated_count is not None:
                    migrated_runs += 1
                    pending_commit_runs += 1
                    row_count += int(migrated_count)
                    attempt_count = int(migrated_count)
                    if pending_commit_runs >= max(1, int(commit_interval_runs)):
                        conn.commit()
                        pending_commit_runs = 0
                else:
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
                        old_count = existing_signature_counts.get(run_id)
                        if old_count is None:
                            fetched_count = conn.execute(
                                "SELECT row_count FROM run_signatures WHERE run_id = ?",
                                (run_id,),
                            ).fetchone()
                            old_count = int(fetched_count[0]) if fetched_count else 0
                        row_count += int(old_count)
                        attempt_count = int(old_count)
                    else:
                        with catalog_json_cache():
                            run_metadata = load_run_metadata(run_dir)
                            attempts = load_run_attempts(run_dir)
                            rows = [
                                extract_attempt_catalog_row(
                                    attempt,
                                    run_metadata,
                                    validation_cache_root=validation_cache_root,
                                    config=config,
                                    full_backtest_max_age_days=7.0,
                                    market_session_tolerance_days=5,
                                    market_data_coverage=market_data_coverage,
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
                        "migrated_runs": migrated_runs,
                        "rebuilt_runs": rebuilt_runs,
                        "deleted_runs": deleted_runs,
                        "committed": pending_commit_runs == 0,
                    }
                )

        if pending_commit_runs:
            conn.commit()
            pending_commit_runs = 0
        selected_run_ids = sorted(current_run_ids)
        if selected_run_ids:
            rows = _load_rows(conn, run_ids=selected_run_ids) if load_rows else []
            summary = (
                catalog_summary(rows)
                if load_rows
                else _catalog_summary_from_db(conn, run_ids=selected_run_ids)
            )
        else:
            rows = []
            summary = catalog_summary([])

    return rows, {
        "source": "sqlite_incremental",
        "path": str(db_path),
        "schema_version": CATALOG_INDEX_SCHEMA_VERSION,
        "extraction_version": CATALOG_EXTRACTION_VERSION,
        "refreshed": migrated_runs > 0 or rebuilt_runs > 0 or deleted_runs > 0,
        "prune_missing": bool(prune_missing),
        "run_count": len(run_dirs),
        "row_count": len(rows) if load_rows else row_count,
        "reused_run_count": reused_runs,
        "migrated_run_count": migrated_runs,
        "rebuilt_run_count": rebuilt_runs,
        "deleted_run_count": deleted_runs,
        "reuse_scan": reuse_scan_info,
        "summary": summary,
        "rows_loaded": bool(load_rows),
    }
