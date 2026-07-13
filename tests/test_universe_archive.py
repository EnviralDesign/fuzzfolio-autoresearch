from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import autoresearch.corpus_archive as corpus_archive
from autoresearch.catalog_index import catalog_row_is_active
from autoresearch.corpus_archive import (
    archive_retired_universe_runs,
    archive_plan_path,
    build_archive_plan,
    compact_archive_metadata,
    exclusion_index_path,
    is_excluded,
)
from autoresearch.instrument_universe import require_research_eligible, universe_provenance
from autoresearch.ledger import list_run_dirs, write_run_metadata


def _write_run(runs_root: Path, run_id: str, instruments: list[str]) -> Path:
    run_dir = runs_root / run_id
    artifact_dir = run_dir / "artifacts" / "attempt"
    artifact_dir.mkdir(parents=True)
    attempt = {
        "run_id": run_id,
        "attempt_id": f"{run_id}-attempt-00001",
        "artifact_dir": str(artifact_dir),
        "request_payload": {"instruments": instruments},
    }
    (run_dir / "attempts.jsonl").write_text(json.dumps(attempt) + "\n", encoding="utf-8")
    return run_dir


def _append_attempt(run_dir: Path, attempt_id: str, instruments: list[str]) -> None:
    artifact_dir = run_dir / "artifacts" / attempt_id
    artifact_dir.mkdir(parents=True)
    attempt = {
        "run_id": run_dir.name,
        "attempt_id": attempt_id,
        "artifact_dir": str(artifact_dir),
        "request_payload": {"instruments": instruments},
    }
    with (run_dir / "attempts.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(attempt) + "\n")


def _catalog_source_entry(path: Path) -> dict[str, object]:
    try:
        stat = path.stat()
    except OSError:
        return {"path": str(path), "exists": False, "size": 0, "mtime_ns": None}
    return {
        "path": str(path),
        "exists": True,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _write_catalog(
    runs_root: Path,
    rows: list[tuple[Path, list[str] | list[list[str]], dict[str, object] | None]],
) -> None:
    path = runs_root / "derived" / "attempt-catalog.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE run_signatures ("
            "run_id TEXT PRIMARY KEY, signature_json TEXT NOT NULL, "
            "row_count INTEGER NOT NULL, updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE attempt_rows ("
            "run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
            "base_strategy_key TEXT, row_json TEXT NOT NULL)"
        )
        for run_dir, instruments, signature_override in rows:
            attempt_instruments = (
                instruments
                if instruments and isinstance(instruments[0], list)
                else [instruments]
            )
            signature = signature_override or {
                "sources": {
                    "attempts": _catalog_source_entry(run_dir / "attempts.jsonl"),
                    "run_metadata": _catalog_source_entry(run_dir / "run-metadata.json"),
                }
            }
            conn.execute(
                "INSERT INTO run_signatures(run_id, signature_json, row_count, updated_at) "
                "VALUES (?, ?, ?, 'now')",
                (run_dir.name, json.dumps(signature), len(attempt_instruments)),
            )
            for position, attempt_symbols in enumerate(attempt_instruments, start=1):
                conn.execute(
                    "INSERT INTO attempt_rows(run_id, attempt_id, base_strategy_key, row_json) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        run_dir.name,
                        f"{run_dir.name}-attempt-{position:05d}",
                        f"M5|{','.join(attempt_symbols)}",
                        json.dumps({"base_instruments": attempt_symbols}),
                    ),
                )


def test_universe_policy_rejects_mixed_active_input() -> None:
    with pytest.raises(ValueError, match="ineligible=US500"):
        require_research_eligible(["EURUSD", "US500"], context="test")


def test_run_metadata_records_authoritative_universe_contract(tmp_path: Path) -> None:
    write_run_metadata(tmp_path / "run", {"run_id": "run"})
    payload = json.loads((tmp_path / "run" / "run-metadata.json").read_text(encoding="utf-8"))
    assert payload["universe_contract"] == universe_provenance()


def test_archive_is_dry_run_first_then_moves_once_and_excludes_discovery(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    retired = _write_run(runs_root, "retired-run", ["EURUSD", "BTCUSD"])
    active = _write_run(runs_root, "active-run", ["EURUSD"])

    preview = archive_retired_universe_runs(runs_root, cohort="unit-cohort")
    assert preview["dry_run"] is True
    assert [entry["run_id"] for entry in preview["entries"]] == ["retired-run"]
    assert retired.exists()
    assert not exclusion_index_path(runs_root).exists()

    receipt = archive_retired_universe_runs(runs_root, cohort="unit-cohort", apply=True)
    archived = tmp_path / "runs_archive" / "unit-cohort" / "retired-run"
    assert receipt["moved_run_count"] == 1
    assert archived.exists() and not retired.exists()
    assert [path.name for path in list_run_dirs(runs_root)] == ["active-run"]
    index = json.loads(exclusion_index_path(runs_root).read_text(encoding="utf-8"))
    entry = index["entries"][0]
    assert entry["source_run_dir"] == str(retired.resolve())
    assert entry["archive_run_dir"] == str(archived.resolve())
    assert "attempts" not in entry
    assert "artifact_path_mappings" not in entry
    receipt_payload = json.loads(
        (runs_root / "derived" / "archive-receipts" / "unit-cohort.json").read_text(
            encoding="utf-8"
        )
    )
    assert "entries" not in receipt_payload
    assert "completed_entries" not in receipt_payload
    config = type("Config", (), {"runs_root": runs_root})()
    assert not catalog_row_is_active(
        config,
        {
            "run_id": "retired-run",
            "attempt_id": "retired-run-attempt-00001",
            "instruments_36m": ["EURUSD"],
        },
    )

    resumed = archive_retired_universe_runs(runs_root, cohort="unit-cohort", apply=True)
    assert resumed["moved_run_count"] == 0
    assert len(json.loads(exclusion_index_path(runs_root).read_text(encoding="utf-8"))["entries"]) == 1


def test_archive_retains_mixed_run_and_excludes_only_ineligible_attempts(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    mixed = _write_run(runs_root, "mixed-run", ["EURUSD"])
    _append_attempt(mixed, "mixed-run-attempt-00002", ["EURUSD", "BTCUSD"])

    preview = archive_retired_universe_runs(runs_root, cohort="mixed-cohort")
    entry = preview["entries"][0]
    assert entry["archive_scope"] == "attempts"
    assert entry["attempt_ids"] == ["mixed-run-attempt-00002"]
    assert entry["eligible_attempt_count"] == 1
    assert entry["ineligible_attempt_count"] == 1
    assert "attempts" not in entry

    receipt = archive_retired_universe_runs(runs_root, cohort="mixed-cohort", apply=True)
    assert receipt["moved_run_count"] == 0
    assert mixed.exists()
    assert [path.name for path in list_run_dirs(runs_root)] == ["mixed-run"]
    config = type("Config", (), {"runs_root": runs_root})()
    assert catalog_row_is_active(
        config,
        {
            "run_id": "mixed-run",
            "attempt_id": "mixed-run-attempt-00001",
            "instruments_36m": ["EURUSD"],
        },
    )
    assert not catalog_row_is_active(
        config,
        {
            "run_id": "mixed-run",
            "attempt_id": "mixed-run-attempt-00002",
            "instruments_36m": ["EURUSD", "BTCUSD"],
        },
    )


def test_archive_resumes_after_a_move_before_index_persistence(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    source = _write_run(runs_root, "deferred-run", ["EURUSD", "US500"])
    plan = build_archive_plan(runs_root, cohort="resume-cohort")
    plan_path = archive_plan_path(runs_root, "resume-cohort")
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    destination = tmp_path / "runs_archive" / "resume-cohort" / "deferred-run"
    destination.parent.mkdir(parents=True)
    source.replace(destination)

    receipt = archive_retired_universe_runs(runs_root, cohort="resume-cohort", apply=True)

    assert receipt["resumed_run_count"] == 1
    assert exclusion_index_path(runs_root).exists()


def test_archive_catalog_prefilter_verifies_only_candidate_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs_root = tmp_path / "runs"
    eligible = _write_run(runs_root, "eligible-run", ["EURUSD"])
    retired = _write_run(runs_root, "retired-run", ["EURUSD", "BTCUSD"])
    _write_catalog(
        runs_root,
        [(eligible, ["EURUSD"], None), (retired, ["EURUSD", "BTCUSD"], None)],
    )
    loaded_runs: list[str] = []
    original_load_attempts = corpus_archive._load_run_attempts

    def capture_loaded_runs(run_dir: Path) -> list[dict[str, object]]:
        loaded_runs.append(run_dir.name)
        return original_load_attempts(run_dir)

    monkeypatch.setattr(corpus_archive, "_load_run_attempts", capture_loaded_runs)
    plan = build_archive_plan(runs_root, cohort="catalog-prefilter")

    assert loaded_runs == ["retired-run"]
    assert [entry["run_id"] for entry in plan["entries"]] == ["retired-run"]
    discovery = plan["candidate_discovery"]
    assert discovery["source"] == "attempt-catalog.sqlite"
    assert discovery["catalog_path"] == str(
        (runs_root / "derived" / "attempt-catalog.sqlite").resolve()
    )
    assert discovery["catalog_rows_scanned"] == 2
    assert discovery["catalog_evidence_run_count"] == 1
    assert discovery["candidate_run_count"] == 1
    assert discovery["full_raw_verification_run_count"] == 0
    assert discovery["catalog_assisted_run_count"] == 1
    assert discovery["raw_verified_attempt_count"] == 1


def test_archive_catalog_prefilter_reopens_changed_sources_and_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog_root = tmp_path / "catalog-runs"
    changed = _write_run(catalog_root, "changed-run", ["EURUSD", "BTCUSD"])
    stale_signature = {
        "sources": {
            "attempts": {"exists": True, "size": 1, "mtime_ns": 1},
            "run_metadata": _catalog_source_entry(changed / "run-metadata.json"),
        }
    }
    _write_catalog(catalog_root, [(changed, ["EURUSD"], stale_signature)])
    catalog_plan = build_archive_plan(catalog_root, cohort="catalog-stale")
    assert [entry["run_id"] for entry in catalog_plan["entries"]] == ["changed-run"]
    assert catalog_plan["candidate_discovery"]["stale_source_run_count"] == 1

    fallback_root = tmp_path / "fallback-runs"
    _write_run(fallback_root, "eligible-run", ["EURUSD"])
    _write_run(fallback_root, "retired-run", ["EURUSD", "BTCUSD"])
    loaded_runs: list[str] = []
    original_load_attempts = corpus_archive._load_run_attempts

    def capture_loaded_runs(run_dir: Path) -> list[dict[str, object]]:
        loaded_runs.append(run_dir.name)
        return original_load_attempts(run_dir)

    monkeypatch.setattr(corpus_archive, "_load_run_attempts", capture_loaded_runs)
    fallback_plan = build_archive_plan(fallback_root, cohort="catalog-fallback")

    assert loaded_runs == ["eligible-run", "retired-run"]
    assert fallback_plan["candidate_discovery"]["source"] == "full-ledger-fallback"
    assert [entry["run_id"] for entry in fallback_plan["entries"]] == ["retired-run"]


def test_archive_catalog_prefilter_resolves_only_flagged_mixed_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs_root = tmp_path / "runs"
    mixed = _write_run(runs_root, "mixed-run", ["EURUSD"])
    _append_attempt(mixed, "mixed-run-attempt-00002", ["EURUSD", "BTCUSD"])
    _write_catalog(runs_root, [(mixed, [["EURUSD"], ["EURUSD", "BTCUSD"]], None)])
    resolved_attempt_ids: list[str] = []
    original_resolve = corpus_archive._resolved_attempt_instruments

    def capture_resolved_attempts(attempt: dict[str, object]) -> list[str]:
        resolved_attempt_ids.append(str(attempt["attempt_id"]))
        return original_resolve(attempt)

    monkeypatch.setattr(corpus_archive, "_resolved_attempt_instruments", capture_resolved_attempts)
    plan = build_archive_plan(runs_root, cohort="catalog-mixed")

    assert resolved_attempt_ids == ["mixed-run-attempt-00002"]
    assert plan["entries"][0]["archive_scope"] == "attempts"
    assert plan["entries"][0]["attempt_ids"] == ["mixed-run-attempt-00002"]
    assert plan["candidate_discovery"]["catalog_verified_attempt_count"] == 1
    assert plan["candidate_discovery"]["raw_verified_attempt_count"] == 1


def test_exclusion_lookup_cache_reuses_index_and_invalidates_after_archive_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "retired-run", ["EURUSD", "BTCUSD"])
    archive_retired_universe_runs(runs_root, cohort="cache-cohort", apply=True)

    index_path = exclusion_index_path(runs_root).resolve()
    corpus_archive.invalidate_exclusion_lookup(runs_root)
    read_count = 0
    original_read_text = Path.read_text

    def count_index_reads(path: Path, *args: object, **kwargs: object) -> str:
        nonlocal read_count
        if path.resolve() == index_path:
            read_count += 1
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", count_index_reads)
    config = type("Config", (), {"runs_root": runs_root})()
    for _ in range(20):
        assert is_excluded(
            runs_root,
            run_id="retired-run",
            attempt_id="retired-run-attempt-00001",
        )
        assert not catalog_row_is_active(
            config,
            {
                "run_id": "retired-run",
                "attempt_id": "retired-run-attempt-00001",
                "instruments_36m": ["EURUSD"],
            },
        )
    assert read_count == 1

    archive_retired_universe_runs(runs_root, cohort="cache-cohort", apply=True)
    assert is_excluded(
        runs_root,
        run_id="retired-run",
        attempt_id="retired-run-attempt-00001",
    )
    assert read_count == 2


def test_compact_archive_metadata_migrates_legacy_payloads_without_touching_runs(
    tmp_path: Path,
) -> None:
    runs_root = tmp_path / "runs"
    retained_run = _write_run(runs_root, "mixed-run", ["EURUSD"])
    cohort = "legacy-cohort"
    legacy_entry = {
        "run_id": "mixed-run",
        "archive_scope": "attempts",
        "source_run_dir": str(retained_run.resolve()),
        "attempt_ids": ["mixed-run-attempt-00002"],
        "eligible_attempt_ids": ["mixed-run-attempt-00001"],
        "artifact_dirs": [str((retained_run / "artifacts").resolve())],
        "attempts": [
            {
                "attempt_id": "mixed-run-attempt-00001",
                "research_eligible": True,
                "instruments": ["EURUSD"],
            },
            {
                "attempt_id": "mixed-run-attempt-00002",
                "research_eligible": False,
                "ineligible_instruments": ["BTCUSD"],
                "unknown_instruments": [],
            },
        ],
    }
    plan_payload = {
        "schema_version": 1,
        "cohort": cohort,
        "planned_at": "2026-07-13T00:00:00+00:00",
        "universe_contract": universe_provenance(),
        "entries": [legacy_entry],
    }
    index_payload = {
        "schema_version": 1,
        "updated_at": "2026-07-13T00:00:00+00:00",
        "universe_contract": universe_provenance(),
        "entries": [legacy_entry],
    }
    receipt_payload = {
        **plan_payload,
        "completed_at": "2026-07-13T00:01:00+00:00",
        "moved_run_count": 0,
        "resumed_run_count": 0,
        "completed_entries": [legacy_entry],
    }
    plan_path = archive_plan_path(runs_root, cohort)
    receipt_path = runs_root / "derived" / "archive-receipts" / f"{cohort}.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text(json.dumps(plan_payload), encoding="utf-8")
    receipt_path.write_text(json.dumps(receipt_payload), encoding="utf-8")
    exclusion_index_path(runs_root).write_text(json.dumps(index_payload), encoding="utf-8")

    preview = compact_archive_metadata(runs_root, cohort=cohort)
    assert preview["dry_run"] is True
    assert preview["files"]["index"]["after_bytes"] < preview["files"]["index"]["before_bytes"]
    assert "attempts" in json.loads(plan_path.read_text(encoding="utf-8"))["entries"][0]
    assert retained_run.exists()

    result = compact_archive_metadata(runs_root, cohort=cohort, apply=True)
    compact_index = json.loads(exclusion_index_path(runs_root).read_text(encoding="utf-8"))
    compact_plan = json.loads(plan_path.read_text(encoding="utf-8"))
    compact_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert result["dry_run"] is False
    assert compact_index["entries"][0]["attempt_ids"] == ["mixed-run-attempt-00002"]
    assert compact_index["entries"][0]["evidence"]["ineligible_instruments"] == ["BTCUSD"]
    assert "attempts" not in compact_index["entries"][0]
    assert "attempts" not in compact_plan["entries"][0]
    assert "entries" not in compact_receipt
    assert "completed_entries" not in compact_receipt
    assert retained_run.exists()
    assert is_excluded(runs_root, attempt_id="mixed-run-attempt-00002")
