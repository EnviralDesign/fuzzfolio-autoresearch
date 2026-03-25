from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .scoring import AttemptScore


@dataclass
class AttemptRecord:
    attempt_id: str
    sequence: int
    created_at: str
    run_id: str
    candidate_name: str
    artifact_dir: str
    profile_ref: str | None
    profile_path: str | None
    primary_score: float | None
    composite_score: float | None
    score_basis: str
    metrics: dict[str, float | None]
    best_summary: dict[str, Any]
    sensitivity_snapshot_path: str | None
    note: str | None = None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_attempts(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl(path)


def append_attempt(path: Path, record: AttemptRecord) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(record), ensure_ascii=True) + "\n")


def write_attempts(path: Path, attempts: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for attempt in attempts:
            handle.write(json.dumps(attempt, ensure_ascii=True) + "\n")


def attempt_exists(path: Path, artifact_dir: Path) -> bool:
    target = str(artifact_dir.resolve()).lower()
    for row in load_attempts(path):
        if str(row.get("artifact_dir", "")).lower() == target:
            return True
    return False


def make_attempt_record(
    app_config: AppConfig,
    run_id: str,
    artifact_dir: Path,
    score: AttemptScore,
    *,
    candidate_name: str | None = None,
    profile_ref: str | None = None,
    profile_path: Path | None = None,
    sensitivity_snapshot_path: Path | None = None,
    note: str | None = None,
) -> AttemptRecord:
    existing = load_attempts(app_config.attempts_path)
    sequence = len(existing) + 1
    attempt_id = f"attempt-{sequence:05d}"
    return AttemptRecord(
        attempt_id=attempt_id,
        sequence=sequence,
        created_at=datetime.now(timezone.utc).isoformat(),
        run_id=run_id,
        candidate_name=candidate_name or artifact_dir.name,
        artifact_dir=str(artifact_dir.resolve()),
        profile_ref=profile_ref,
        profile_path=str(profile_path.resolve()) if profile_path else None,
        primary_score=score.primary_score,
        composite_score=score.composite_score,
        score_basis=score.score_basis,
        metrics=score.metrics,
        best_summary=score.best_summary,
        sensitivity_snapshot_path=str(sensitivity_snapshot_path.resolve()) if sensitivity_snapshot_path else None,
        note=note,
    )
