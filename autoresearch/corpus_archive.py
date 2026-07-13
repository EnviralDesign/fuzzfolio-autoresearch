from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .corpus_tools import attempt_instruments
from .instrument_universe import research_eligibility_report, universe_provenance


ARCHIVE_INDEX_SCHEMA_VERSION = 2
ARCHIVE_CATALOG_SCAN_PROGRESS_INTERVAL = 50_000


@dataclass(frozen=True)
class ExclusionLookup:
    signature: tuple[bool, int | None, int | None]
    payload: dict[str, Any]
    archived_run_ids: frozenset[str]
    excluded_attempt_ids: frozenset[str]


@dataclass(frozen=True)
class ArchiveCandidateSelection:
    candidate_run_ids: frozenset[str]
    full_verification_run_ids: frozenset[str]
    catalog_attempt_reports: dict[str, dict[str, dict[str, Any]]]
    report: dict[str, Any]


_EXCLUSION_LOOKUP_CACHE: dict[Path, ExclusionLookup] = {}


def exclusion_index_path(runs_root: Path) -> Path:
    return Path(runs_root) / "derived" / "universe-exclusions.json"


def archive_receipt_path(runs_root: Path, cohort: str) -> Path:
    return Path(runs_root) / "derived" / "archive-receipts" / f"{cohort}.json"


def archive_plan_path(runs_root: Path, cohort: str) -> Path:
    return Path(runs_root) / "derived" / "archive-receipts" / f"{cohort}.plan.json"


def _index_signature(path: Path) -> tuple[bool, int | None, int | None]:
    try:
        stat = path.stat()
    except OSError:
        return (False, None, None)
    return (True, int(stat.st_size), int(stat.st_mtime_ns))


def invalidate_exclusion_lookup(runs_root: Path) -> None:
    _EXCLUSION_LOOKUP_CACHE.pop(exclusion_index_path(runs_root).resolve(), None)


def exclusion_lookup(runs_root: Path) -> ExclusionLookup:
    path = exclusion_index_path(runs_root).resolve()
    signature = _index_signature(path)
    cached = _EXCLUSION_LOOKUP_CACHE.get(path)
    if cached is not None and cached.signature == signature:
        return cached
    payload: dict[str, Any] = {"schema_version": ARCHIVE_INDEX_SCHEMA_VERSION, "entries": []}
    if signature[0]:
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            loaded = payload
        if isinstance(loaded, dict):
            payload = loaded
    if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
        raise ValueError(f"Invalid universe exclusion index: {path}")
    entries = [entry for entry in payload["entries"] if isinstance(entry, dict)]
    lookup = ExclusionLookup(
        signature=signature,
        payload=payload,
        archived_run_ids=frozenset(
            str(entry.get("run_id") or "").strip()
            for entry in entries
            if str(entry.get("archive_scope") or "run") == "run"
            and str(entry.get("run_id") or "").strip()
        ),
        excluded_attempt_ids=frozenset(
            str(attempt_id).strip()
            for entry in entries
            for attempt_id in entry.get("attempt_ids") or []
            if str(attempt_id).strip()
        ),
    )
    _EXCLUSION_LOOKUP_CACHE[path] = lookup
    return lookup


def load_exclusion_index(runs_root: Path) -> dict[str, Any]:
    return exclusion_lookup(runs_root).payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(payload, handle, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _compact_archive_entry(entry: dict[str, Any]) -> dict[str, Any]:
    scope = str(entry.get("archive_scope") or "run")
    attempts = entry.get("attempts") if isinstance(entry.get("attempts"), list) else []
    attempt_ids = sorted(
        {
            str(value).strip()
            for value in entry.get("attempt_ids") or []
            if str(value).strip()
        }
    )
    eligible_count = entry.get("eligible_attempt_count")
    if not isinstance(eligible_count, int):
        eligible_count = len(entry.get("eligible_attempt_ids") or [])
    if attempts:
        eligible_count = sum(
            1 for attempt in attempts if isinstance(attempt, dict) and attempt.get("research_eligible")
        )
    ineligible_count = entry.get("ineligible_attempt_count")
    if not isinstance(ineligible_count, int):
        ineligible_count = len(attempt_ids) if scope != "run" else 0
    if attempts:
        ineligible_count = sum(
            1 for attempt in attempts if isinstance(attempt, dict) and not attempt.get("research_eligible")
        )
    ineligible_instruments: set[str] = set()
    unknown_instruments: set[str] = set()
    for attempt in attempts:
        if not isinstance(attempt, dict) or attempt.get("research_eligible"):
            continue
        ineligible_instruments.update(
            str(value).strip() for value in attempt.get("ineligible_instruments") or [] if str(value).strip()
        )
        unknown_instruments.update(
            str(value).strip() for value in attempt.get("unknown_instruments") or [] if str(value).strip()
        )
    evidence = entry.get("evidence") if isinstance(entry.get("evidence"), dict) else {}
    compact_evidence = {
        "ineligible_instruments": sorted(ineligible_instruments)
        or sorted(str(value) for value in evidence.get("ineligible_instruments") or []),
        "unknown_instruments": sorted(unknown_instruments)
        or sorted(str(value) for value in evidence.get("unknown_instruments") or []),
    }
    compact = {
        "run_id": str(entry.get("run_id") or ""),
        "archive_scope": scope,
        "eligible_attempt_count": int(eligible_count),
        "ineligible_attempt_count": int(ineligible_count),
        "evidence": compact_evidence,
    }
    if scope != "run":
        compact["attempt_ids"] = attempt_ids
    for field in ("source_run_dir", "archive_run_dir", "archived_at"):
        value = entry.get(field)
        if value:
            compact[field] = str(value)
    return compact


def _compact_archive_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    return {
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "cohort": str(payload.get("cohort") or ""),
        "planned_at": payload.get("planned_at"),
        "universe_contract": payload.get("universe_contract") or universe_provenance(),
        "candidate_discovery": payload.get("candidate_discovery") or {},
        "entries": [
            _compact_archive_entry(entry) for entry in entries if isinstance(entry, dict)
        ],
    }


def _compact_exclusion_index_payload(payload: dict[str, Any]) -> dict[str, Any]:
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    return {
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "updated_at": payload.get("updated_at") or datetime.now(timezone.utc).isoformat(),
        "universe_contract": payload.get("universe_contract") or universe_provenance(),
        "entries": [
            _compact_archive_entry(entry) for entry in entries if isinstance(entry, dict)
        ],
    }


def _compact_receipt_payload(
    payload: dict[str, Any], *, plan_path: Path, exclusion_path: Path
) -> dict[str, Any]:
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    completed = payload.get("completed_entries")
    completed_count = len(completed) if isinstance(completed, list) else len(entries)
    return {
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "cohort": str(payload.get("cohort") or ""),
        "dry_run": False,
        "completed_at": payload.get("completed_at") or datetime.now(timezone.utc).isoformat(),
        "universe_contract": payload.get("universe_contract") or universe_provenance(),
        "plan_path": str(plan_path),
        "exclusion_index": str(exclusion_path),
        "planned_entry_count": len(entries),
        "completed_entry_count": completed_count,
        "moved_run_count": int(payload.get("moved_run_count") or 0),
        "resumed_run_count": int(payload.get("resumed_run_count") or 0),
    }


def archived_run_ids(runs_root: Path) -> set[str]:
    return set(exclusion_lookup(runs_root).archived_run_ids)


def is_excluded_from_lookup(
    lookup: ExclusionLookup, *, run_id: str | None = None, attempt_id: str | None = None
) -> bool:
    return bool(
        (run_id and str(run_id) in lookup.archived_run_ids)
        or (attempt_id and str(attempt_id) in lookup.excluded_attempt_ids)
    )


def is_excluded(
    runs_root: Path, *, run_id: str | None = None, attempt_id: str | None = None
) -> bool:
    return is_excluded_from_lookup(
        exclusion_lookup(runs_root), run_id=run_id, attempt_id=attempt_id
    )


def default_archive_cohort() -> str:
    provenance = universe_provenance()
    return (
        f"{datetime.now(timezone.utc).date().isoformat()}-"
        f"{provenance['universe_id']}-v{provenance['universe_version']}"
    )


def _load_run_attempts(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "attempts.jsonl"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _raw_run_dirs(runs_root: Path, run_ids: Iterable[str] | None = None) -> list[Path]:
    wanted = {str(value).strip() for value in (run_ids or []) if str(value).strip()}
    if not Path(runs_root).exists():
        return []
    return sorted(
        path
        for path in Path(runs_root).iterdir()
        if path.is_dir()
        and path.name != "derived"
        and (not wanted or path.name in wanted)
    )


def _source_signature(run_dir: Path) -> dict[str, tuple[bool, int, int | None]]:
    def file_state(path: Path) -> tuple[bool, int, int | None]:
        try:
            stat = path.stat()
        except OSError:
            return (False, 0, None)
        return (True, int(stat.st_size), int(stat.st_mtime_ns))

    return {
        "attempts": file_state(run_dir / "attempts.jsonl"),
        "run_metadata": file_state(run_dir / "run-metadata.json"),
    }


def _catalog_source_signature(signature: Any) -> dict[str, tuple[bool, int, int | None]] | None:
    if not isinstance(signature, dict):
        return None
    sources = signature.get("sources")
    if not isinstance(sources, dict):
        return None
    normalized: dict[str, tuple[bool, int, int | None]] = {}
    for name in ("attempts", "run_metadata"):
        item = sources.get(name)
        if not isinstance(item, dict):
            return None
        exists = bool(item.get("exists"))
        try:
            size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            return None
        raw_mtime = item.get("mtime_ns")
        try:
            mtime_ns = int(raw_mtime) if raw_mtime is not None else None
        except (TypeError, ValueError):
            return None
        normalized[name] = (exists, size, mtime_ns)
    return normalized


def _catalog_connection(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro&immutable=1", uri=True)


def _strategy_key_instruments(strategy_key: Any) -> list[str] | None:
    value = str(strategy_key or "").strip()
    if not value:
        return None
    instrument_part = value.split("|", 1)[-1]
    instruments = [token.strip() for token in instrument_part.split(",") if token.strip()]
    return instruments or None


def _catalog_candidate_run_ids(
    runs_root: Path,
    run_dirs: list[Path],
    *,
    progress_callback: Callable[[str], None] | None = None,
) -> ArchiveCandidateSelection | None:
    catalog_path = Path(runs_root) / "derived" / "attempt-catalog.sqlite"
    if not catalog_path.is_file():
        return None
    try:
        with _catalog_connection(catalog_path) as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('attempt_rows', 'run_signatures')"
                )
            }
            if tables != {"attempt_rows", "run_signatures"}:
                return None
            signatures: dict[str, tuple[dict[str, tuple[bool, int, int | None]] | None, int]] = {}
            for run_id, signature_json, row_count in conn.execute(
                "SELECT run_id, signature_json, row_count FROM run_signatures"
            ):
                try:
                    signature = json.loads(signature_json)
                except (TypeError, json.JSONDecodeError):
                    signature = None
                try:
                    count = int(row_count)
                except (TypeError, ValueError):
                    count = 0
                signatures[str(run_id)] = (_catalog_source_signature(signature), count)

            candidate_ids: set[str] = set()
            catalog_rows_scanned = 0
            evidence_run_ids: set[str] = set()
            # base_strategy_key is a materialized representation of the resolved
            # base_instruments emitted by extract_attempt_catalog_row. There are
            # only thousands of distinct keys, so this avoids a full row_json scan.
            strategy_keys = [
                row[0]
                for row in conn.execute("SELECT DISTINCT base_strategy_key FROM attempt_rows")
            ]
            ineligible_strategy_keys = [
                key
                for key in strategy_keys
                if not research_eligibility_report(_strategy_key_instruments(key))["is_eligible"]
            ]
            for offset in range(0, len(ineligible_strategy_keys), 900):
                chunk = ineligible_strategy_keys[offset : offset + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                for run_id, attempt_id in conn.execute(
                    "SELECT run_id, attempt_id FROM attempt_rows "
                    f"WHERE base_strategy_key IN ({placeholders})",
                    chunk,
                ):
                    normalized_run_id = str(run_id).strip()
                    if normalized_run_id:
                        candidate_ids.add(normalized_run_id)
                        evidence_run_ids.add(normalized_run_id)
            for run_id, attempt_id in conn.execute(
                "SELECT run_id, attempt_id FROM attempt_rows "
                "WHERE base_strategy_key IS NULL OR base_strategy_key = ''"
            ):
                normalized_run_id = str(run_id).strip()
                if normalized_run_id:
                    candidate_ids.add(normalized_run_id)
                    evidence_run_ids.add(normalized_run_id)
            catalog_rows_scanned = sum(count for _, count in signatures.values())
            if progress_callback is not None:
                progress_callback(
                    "archive catalog key scan: "
                    f"rows={catalog_rows_scanned} keys={len(strategy_keys)} "
                    f"candidate_runs={len(candidate_ids)}"
                )

            missing_signature_count = 0
            stale_source_count = 0
            empty_catalog_run_count = 0
            full_verification_run_ids: set[str] = set()
            for run_dir in run_dirs:
                stored = signatures.get(run_dir.name)
                if stored is None:
                    candidate_ids.add(run_dir.name)
                    full_verification_run_ids.add(run_dir.name)
                    missing_signature_count += 1
                    continue
                stored_sources, row_count = stored
                if stored_sources is None or stored_sources != _source_signature(run_dir):
                    candidate_ids.add(run_dir.name)
                    full_verification_run_ids.add(run_dir.name)
                    stale_source_count += 1
                if row_count <= 0 and (run_dir / "attempts.jsonl").is_file():
                    candidate_ids.add(run_dir.name)
                    full_verification_run_ids.add(run_dir.name)
                    empty_catalog_run_count += 1
            catalog_attempt_reports: dict[str, dict[str, dict[str, Any]]] = {}
            fresh_candidate_ids = sorted(candidate_ids - full_verification_run_ids)
            for offset in range(0, len(fresh_candidate_ids), 900):
                chunk = fresh_candidate_ids[offset : offset + 900]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                for run_id, attempt_id, strategy_key in conn.execute(
                    "SELECT run_id, attempt_id, base_strategy_key FROM attempt_rows "
                    f"WHERE run_id IN ({placeholders})",
                    chunk,
                ):
                    normalized_run_id = str(run_id).strip()
                    normalized_attempt_id = str(attempt_id).strip()
                    instruments = _strategy_key_instruments(strategy_key)
                    if not normalized_run_id or not normalized_attempt_id or instruments is None:
                        full_verification_run_ids.add(normalized_run_id)
                        continue
                    reports = catalog_attempt_reports.setdefault(normalized_run_id, {})
                    if normalized_attempt_id in reports:
                        full_verification_run_ids.add(normalized_run_id)
                        continue
                    reports[normalized_attempt_id] = research_eligibility_report(instruments)
            for run_id in full_verification_run_ids:
                catalog_attempt_reports.pop(run_id, None)
            if progress_callback is not None:
                progress_callback(
                    "archive catalog scan complete: "
                    f"rows={catalog_rows_scanned} candidate_runs={len(candidate_ids)}"
                )
            report = {
                "source": "attempt-catalog.sqlite",
                "catalog_path": str(catalog_path.resolve()),
                "catalog_rows_scanned": catalog_rows_scanned,
                "catalog_evidence_run_count": len(evidence_run_ids),
                "candidate_run_count": len(candidate_ids),
                "missing_signature_run_count": missing_signature_count,
                "stale_source_run_count": stale_source_count,
                "empty_catalog_run_count": empty_catalog_run_count,
                "full_raw_verification_run_count": len(full_verification_run_ids),
                "raw_verification_authoritative": True,
            }
            return ArchiveCandidateSelection(
                candidate_run_ids=frozenset(candidate_ids),
                full_verification_run_ids=frozenset(full_verification_run_ids),
                catalog_attempt_reports=catalog_attempt_reports,
                report=report,
            )
    except (OSError, sqlite3.DatabaseError):
        return None


def _resolved_attempt_instruments(attempt: dict[str, Any]) -> list[str]:
    resolved = attempt_instruments(attempt)
    if resolved:
        return resolved
    request_payload = attempt.get("request_payload")
    if isinstance(request_payload, dict):
        raw_instruments = request_payload.get("instruments")
        if isinstance(raw_instruments, list):
            return [str(value) for value in raw_instruments]
    return []


def _artifact_path_mapping(
    artifact_dir: str,
    *,
    source_run_dir: Path,
    archive_run_dir: Path,
) -> dict[str, str]:
    source = Path(artifact_dir).resolve()
    mapping = {"source": str(source)}
    try:
        relative = source.relative_to(source_run_dir)
    except ValueError:
        return mapping
    mapping["archived"] = str((archive_run_dir / relative).resolve())
    return mapping


def build_archive_plan(
    runs_root: Path,
    *,
    run_ids: Iterable[str] | None = None,
    cohort: str | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    selected_cohort = str(cohort or default_archive_cohort()).strip()
    if not selected_cohort:
        raise ValueError("Archive cohort must not be empty")
    existing = archived_run_ids(runs_root)
    raw_run_dirs = [
        run_dir for run_dir in _raw_run_dirs(runs_root, run_ids) if run_dir.name not in existing
    ]
    catalog_candidates = _catalog_candidate_run_ids(
        runs_root, raw_run_dirs, progress_callback=progress_callback
    )
    if catalog_candidates is None:
        candidate_run_dirs = raw_run_dirs
        candidate_selection = ArchiveCandidateSelection(
            candidate_run_ids=frozenset(run_dir.name for run_dir in candidate_run_dirs),
            full_verification_run_ids=frozenset(run_dir.name for run_dir in candidate_run_dirs),
            catalog_attempt_reports={},
            report={
            "source": "full-ledger-fallback",
            "catalog_path": str((Path(runs_root) / "derived" / "attempt-catalog.sqlite").resolve()),
            "catalog_rows_scanned": 0,
            "catalog_evidence_run_count": 0,
            "candidate_run_count": len(candidate_run_dirs),
            "missing_signature_run_count": 0,
            "stale_source_run_count": 0,
            "empty_catalog_run_count": 0,
            "full_raw_verification_run_count": len(candidate_run_dirs),
            "raw_verification_authoritative": True,
            },
        )
        candidate_discovery = candidate_selection.report
        if progress_callback is not None:
            progress_callback(
                "archive catalog unavailable; using full ledger verification: "
                f"candidate_runs={len(candidate_run_dirs)}"
            )
    else:
        candidate_selection = catalog_candidates
        candidate_run_dirs = [
            run_dir
            for run_dir in raw_run_dirs
            if run_dir.name in candidate_selection.candidate_run_ids
        ]
        candidate_discovery = {
            **candidate_selection.report,
            "candidate_run_count": len(candidate_run_dirs),
        }
    if progress_callback is not None:
        progress_callback(
            "archive ledger verification: "
            f"candidate_runs={len(candidate_run_dirs)} raw_runs={len(raw_run_dirs)}"
        )
    entries: list[dict[str, Any]] = []
    catalog_assisted_runs = 0
    catalog_verified_attempts = 0
    raw_verified_attempts = 0
    dynamic_full_verification_runs = 0

    def report_verification_progress(run_position: int) -> None:
        if progress_callback is not None and (
            run_position == len(candidate_run_dirs) or run_position % 100 == 0
        ):
            progress_callback(
                "archive ledger verification progress: "
                f"runs={run_position}/{len(candidate_run_dirs)} archive_entries={len(entries)}"
            )

    for run_position, run_dir in enumerate(candidate_run_dirs, start=1):
        attempts = _load_run_attempts(run_dir)
        catalog_reports = candidate_selection.catalog_attempt_reports.get(run_dir.name)
        attempt_ids = [str(attempt.get("attempt_id") or "").strip() for attempt in attempts]
        use_catalog_reports = bool(catalog_reports) and (
            run_dir.name not in candidate_selection.full_verification_run_ids
            and len(attempt_ids) == len(catalog_reports)
            and len(set(attempt_ids)) == len(attempt_ids)
            and set(attempt_ids) == set(catalog_reports)
        )
        if not use_catalog_reports and (
            run_dir.name not in candidate_selection.full_verification_run_ids
        ):
            dynamic_full_verification_runs += 1
        if use_catalog_reports:
            catalog_assisted_runs += 1
        archive_run_dir = (
            Path(runs_root).parent / "runs_archive" / selected_cohort / run_dir.name
        ).resolve()
        attempt_entries: list[dict[str, Any]] = []
        eligible_attempt_ids: list[str] = []
        ineligible_attempt_ids: list[str] = []
        eligible_attempt_count = 0
        ineligible_attempt_count = 0
        for attempt_position, attempt in enumerate(attempts, start=1):
            attempt_id = str(attempt.get("attempt_id") or "").strip()
            catalog_report = catalog_reports.get(attempt_id) if use_catalog_reports else None
            if catalog_report is not None and catalog_report["is_eligible"]:
                report = catalog_report
                catalog_verified_attempts += 1
            else:
                report = research_eligibility_report(_resolved_attempt_instruments(attempt))
                raw_verified_attempts += 1
            artifact_dir = str(attempt.get("artifact_dir") or "").strip() or None
            attempt_entry = {
                "attempt_id": attempt_id or None,
                "attempt_position": attempt_position,
                "artifact_dir": artifact_dir,
                "instruments": report["instruments"],
                "ineligible_instruments": report["ineligible"],
                "unknown_instruments": report["unknown"],
                "lifecycle": report["lifecycle"],
                "research_eligible": bool(report["is_eligible"]),
            }
            attempt_entries.append(attempt_entry)
            if report["is_eligible"]:
                eligible_attempt_count += 1
                if attempt_id:
                    eligible_attempt_ids.append(attempt_id)
            else:
                ineligible_attempt_count += 1
                if attempt_id:
                    ineligible_attempt_ids.append(attempt_id)
        if not ineligible_attempt_count:
            report_verification_progress(run_position)
            continue
        if eligible_attempt_count and len(ineligible_attempt_ids) != ineligible_attempt_count:
            raise ValueError(
                f"Cannot attempt-exclude run {run_dir.name}: an ineligible attempt is missing attempt_id"
            )
        archive_scope = "run" if not eligible_attempt_count else "attempts"
        scoped_attempt_ids = (
            [entry["attempt_id"] for entry in attempt_entries if entry["attempt_id"]]
            if archive_scope == "run"
            else ineligible_attempt_ids
        )
        scoped_artifact_dirs = [
            str(entry["artifact_dir"])
            for entry in attempt_entries
            if entry["artifact_dir"]
            and (archive_scope == "run" or not entry["research_eligible"])
        ]
        artifact_path_mappings = (
            [
                _artifact_path_mapping(
                    artifact_dir,
                    source_run_dir=run_dir.resolve(),
                    archive_run_dir=archive_run_dir,
                )
                for artifact_dir in scoped_artifact_dirs
            ]
            if archive_scope == "run"
            else []
        )
        entries.append(
            {
                "run_id": run_dir.name,
                "archive_scope": archive_scope,
                "source_run_dir": str(run_dir.resolve()),
                "archive_run_dir": str(archive_run_dir) if archive_scope == "run" else None,
                "attempt_ids": sorted(set(scoped_attempt_ids)),
                "eligible_attempt_ids": sorted(set(eligible_attempt_ids)),
                "artifact_dirs": sorted(set(scoped_artifact_dirs)),
                "artifact_path_mappings": artifact_path_mappings,
                "attempts": attempt_entries,
            }
        )
        report_verification_progress(run_position)
    candidate_discovery = {
        **candidate_discovery,
        "catalog_assisted_run_count": catalog_assisted_runs,
        "catalog_verified_attempt_count": catalog_verified_attempts,
        "raw_verified_attempt_count": raw_verified_attempts,
        "dynamic_full_verification_run_count": dynamic_full_verification_runs,
    }
    return {
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "cohort": selected_cohort,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "universe_contract": universe_provenance(),
        "candidate_discovery": candidate_discovery,
        "entries": [_compact_archive_entry(entry) for entry in entries],
    }


def compact_archive_metadata(
    runs_root: Path, *, cohort: str, apply: bool = False
) -> dict[str, Any]:
    selected_cohort = str(cohort).strip()
    if not selected_cohort:
        raise ValueError("Archive cohort must not be empty")
    paths = {
        "index": exclusion_index_path(runs_root),
        "plan": archive_plan_path(runs_root, selected_cohort),
        "receipt": archive_receipt_path(runs_root, selected_cohort),
    }
    compacted: dict[str, tuple[Path, dict[str, Any], int]] = {}
    for name, path in paths.items():
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot compact invalid archive metadata: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Cannot compact invalid archive metadata: {path}")
        if name == "index":
            compact = _compact_exclusion_index_payload(payload)
        elif name == "plan":
            compact = _compact_archive_plan_payload(payload)
        else:
            compact = _compact_receipt_payload(
                payload, plan_path=paths["plan"], exclusion_path=paths["index"]
            )
        compacted[name] = (path, compact, path.stat().st_size)
    if not compacted:
        raise FileNotFoundError(f"No archive metadata found for cohort: {selected_cohort}")
    files = {
        name: {
            "path": str(path),
            "before_bytes": before_bytes,
            "after_bytes": len(
                json.dumps(compact, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ),
        }
        for name, (path, compact, before_bytes) in compacted.items()
    }
    result = {
        "cohort": selected_cohort,
        "dry_run": not apply,
        "metadata_only": True,
        "files": files,
    }
    if not apply:
        return result
    # This routine never reads, moves, or rewrites run directories.
    for name in ("plan", "index", "receipt"):
        item = compacted.get(name)
        if item is not None:
            _write_json_atomic(item[0], item[1])
    if "index" in compacted:
        invalidate_exclusion_lookup(runs_root)
    return {**result, "dry_run": False}


def archive_retired_universe_runs(
    runs_root: Path,
    *,
    run_ids: Iterable[str] | None = None,
    cohort: str | None = None,
    apply: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    plan = build_archive_plan(
        runs_root,
        run_ids=run_ids,
        cohort=cohort,
        progress_callback=progress_callback,
    )
    if not apply:
        return {**plan, "dry_run": True, "moved_run_count": 0}
    index = load_exclusion_index(runs_root)
    existing_by_run = {
        str(entry.get("run_id")): entry
        for entry in index.get("entries") or []
        if isinstance(entry, dict) and str(entry.get("run_id") or "")
    }
    manifest_path = archive_plan_path(runs_root, str(plan["cohort"]))
    if manifest_path.exists():
        # Read the persisted move plan directly so a move interrupted before index
        # persistence can resume.
        try:
            prior_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prior_payload = {}
        prior_entries = prior_payload.get("entries") if isinstance(prior_payload, dict) else []
        if isinstance(prior_entries, list):
            planned_by_run = {
                str(entry.get("run_id")): entry
                for entry in plan["entries"]
                if isinstance(entry, dict) and str(entry.get("run_id") or "")
            }
            for entry in prior_entries:
                run_id = str(entry.get("run_id") or "") if isinstance(entry, dict) else ""
                if run_id and run_id not in existing_by_run:
                    planned_by_run.setdefault(run_id, entry)
            plan["entries"] = [planned_by_run[key] for key in sorted(planned_by_run)]
    # Persist the immutable move plan before changing any directory. This is the
    # recovery record when an interruption lands between move and index writes.
    _write_json_atomic(manifest_path, plan)
    moved = 0
    resumed = 0
    completed_entries: list[dict[str, Any]] = []
    for entry in plan["entries"]:
        if str(entry.get("archive_scope") or "run") != "run":
            entry = {**entry, "archived_at": datetime.now(timezone.utc).isoformat()}
            existing_by_run[str(entry["run_id"])] = entry
            completed_entries.append(entry)
            continue
        source = Path(str(entry["source_run_dir"]))
        destination = Path(str(entry["archive_run_dir"]))
        if destination.exists() and not source.exists():
            resumed += 1
        elif source.exists() and not destination.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.replace(destination)
            moved += 1
        elif source.exists() and destination.exists():
            raise RuntimeError(f"Archive destination already exists: {destination}")
        entry = {**entry, "archived_at": datetime.now(timezone.utc).isoformat()}
        existing_by_run[str(entry["run_id"])] = entry
        completed_entries.append(entry)
    index = _compact_exclusion_index_payload({
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "universe_contract": universe_provenance(),
        "entries": [existing_by_run[key] for key in sorted(existing_by_run)],
    })
    _write_json_atomic(exclusion_index_path(runs_root), index)
    invalidate_exclusion_lookup(runs_root)
    receipt = {
        "schema_version": ARCHIVE_INDEX_SCHEMA_VERSION,
        "cohort": plan["cohort"],
        "dry_run": False,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "universe_contract": plan["universe_contract"],
        "plan_path": str(manifest_path),
        "planned_entry_count": len(plan["entries"]),
        "completed_entry_count": len(completed_entries),
        "moved_run_count": moved,
        "resumed_run_count": resumed,
        "exclusion_index": str(exclusion_index_path(runs_root)),
    }
    _write_json_atomic(archive_receipt_path(runs_root, str(plan["cohort"])), receipt)
    return receipt
