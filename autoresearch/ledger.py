from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .instrument_universe import universe_provenance
from .scoring import AttemptScore

ATTEMPTS_FILE_NAME = "attempts.jsonl"
RUN_METADATA_FILE_NAME = "run-metadata.json"


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
    requested_horizon_months: int | None = None
    effective_window_months: float | None = None
    requested_timeframe: str | None = None
    effective_timeframe: str | None = None
    max_reward_r: float | None = None
    reward_matrix: dict[str, Any] | None = None
    reward_step_r: float | None = None
    reward_columns: int | None = None
    effective_max_reward_r: float | None = None
    validation_outcome: str | None = None
    coverage_status: str | None = None
    job_status: str | None = None
    resolved_trades: int | None = None
    trades_per_month: float | None = None
    positive_cell_ratio: float | None = None
    effective_window_source: str | None = None
    signal_coverage_ratio: float | None = None
    bars_per_signal: float | None = None
    max_consecutive_signal_run: float | None = None
    trigger_indicator_count: float | None = None
    runner: str | None = None
    attempt_role: str | None = None
    attempt_decision: str | None = None
    attempt_decision_reasons: list[str] | None = None
    strategy_family_id: str | None = None
    canonical_attempt_id: str | None = None
    is_canonical_attempt: bool = False
    is_canonical_playhand_attempt: bool = False
    play_hand_role: str | None = None
    play_hand_stage: str | None = None
    play_hand_instrument: str | None = None
    play_hand_selected_instruments: list[str] | None = None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or not line.strip("\x00").strip():
                continue
            rows.append(json.loads(line))
    return rows


def load_attempts(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl(path)


def attempts_path_for_run_dir(run_dir: Path) -> Path:
    return run_dir / ATTEMPTS_FILE_NAME


def run_metadata_path_for_run_dir(run_dir: Path) -> Path:
    return run_dir / RUN_METADATA_FILE_NAME


def load_run_metadata(run_dir: Path) -> dict[str, Any]:
    path = run_metadata_path_for_run_dir(run_dir)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def write_run_metadata(run_dir: Path, metadata: dict[str, Any]) -> Path:
    metadata = dict(metadata)
    metadata["universe_contract"] = universe_provenance()
    path = run_metadata_path_for_run_dir(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(metadata, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return path


def load_run_attempts(run_dir: Path) -> list[dict[str, Any]]:
    return load_attempts(attempts_path_for_run_dir(run_dir))


def list_run_dirs(runs_root: Path) -> list[Path]:
    if not runs_root.exists():
        return []
    from .corpus_archive import archived_run_ids

    archived = archived_run_ids(runs_root)
    run_dirs = [
        path
        for path in runs_root.iterdir()
        if path.is_dir() and path.name != "derived" and path.name not in archived
    ]
    return sorted(run_dirs)


def latest_run_dir(runs_root: Path) -> Path | None:
    run_dirs = list_run_dirs(runs_root)
    if not run_dirs:
        return None
    return run_dirs[-1]


def load_all_run_attempts(runs_root: Path) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for run_dir in list_run_dirs(runs_root):
        attempts.extend(load_run_attempts(run_dir))
    return attempts


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
    attempts_path: Path,
    run_id: str,
    artifact_dir: Path,
    score: AttemptScore,
    *,
    candidate_name: str | None = None,
    profile_ref: str | None = None,
    profile_path: Path | None = None,
    sensitivity_snapshot_path: Path | None = None,
    note: str | None = None,
    requested_horizon_months: int | None = None,
    effective_window_months: float | None = None,
    requested_timeframe: str | None = None,
    effective_timeframe: str | None = None,
    max_reward_r: float | None = None,
    reward_matrix: dict[str, Any] | None = None,
    reward_step_r: float | None = None,
    reward_columns: int | None = None,
    effective_max_reward_r: float | None = None,
    validation_outcome: str | None = None,
    coverage_status: str | None = None,
    job_status: str | None = None,
    resolved_trades: int | None = None,
    trades_per_month: float | None = None,
    positive_cell_ratio: float | None = None,
    effective_window_source: str | None = None,
    signal_coverage_ratio: float | None = None,
    bars_per_signal: float | None = None,
    max_consecutive_signal_run: float | None = None,
    trigger_indicator_count: float | None = None,
    runner: str | None = None,
    attempt_role: str | None = None,
    attempt_decision: str | None = None,
    attempt_decision_reasons: list[str] | None = None,
    strategy_family_id: str | None = None,
    canonical_attempt_id: str | None = None,
    is_canonical_attempt: bool = False,
    is_canonical_playhand_attempt: bool = False,
    play_hand_role: str | None = None,
    play_hand_stage: str | None = None,
    play_hand_instrument: str | None = None,
    play_hand_selected_instruments: list[str] | None = None,
) -> AttemptRecord:
    existing = load_attempts(attempts_path)
    sequence = len(existing) + 1
    attempt_id = f"{run_id}-attempt-{sequence:05d}"
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
        requested_horizon_months=requested_horizon_months,
        effective_window_months=effective_window_months,
        requested_timeframe=requested_timeframe,
        effective_timeframe=effective_timeframe,
        max_reward_r=max_reward_r,
        reward_matrix=reward_matrix,
        reward_step_r=reward_step_r,
        reward_columns=reward_columns,
        effective_max_reward_r=effective_max_reward_r,
        validation_outcome=validation_outcome,
        coverage_status=coverage_status,
        job_status=job_status,
        resolved_trades=resolved_trades,
        trades_per_month=trades_per_month,
        positive_cell_ratio=positive_cell_ratio,
        effective_window_source=effective_window_source,
        signal_coverage_ratio=signal_coverage_ratio,
        bars_per_signal=bars_per_signal,
        max_consecutive_signal_run=max_consecutive_signal_run,
        trigger_indicator_count=trigger_indicator_count,
        runner=runner,
        attempt_role=attempt_role,
        attempt_decision=attempt_decision,
        attempt_decision_reasons=attempt_decision_reasons,
        strategy_family_id=strategy_family_id,
        canonical_attempt_id=canonical_attempt_id,
        is_canonical_attempt=is_canonical_attempt,
        is_canonical_playhand_attempt=is_canonical_playhand_attempt,
        play_hand_role=play_hand_role,
        play_hand_stage=play_hand_stage,
        play_hand_instrument=play_hand_instrument,
        play_hand_selected_instruments=play_hand_selected_instruments,
    )
