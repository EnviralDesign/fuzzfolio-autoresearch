"""Normalized artifact / job resolution status for inspect_artifact and tooling."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

# Resolution labels returned to the model / runtime
RESOLUTION_FULLY_SCORED = "fully_scored"
RESOLUTION_READY_TO_SCORE = "ready_to_score"
RESOLUTION_JOB_PENDING = "job_pending"
RESOLUTION_JOB_TIMED_OUT_ARTIFACTS_PRESENT = "job_timed_out_but_artifacts_present"
RESOLUTION_ARTIFACTS_INCOMPLETE = "artifacts_incomplete"
RESOLUTION_ATTEMPT_MISMATCH = "attempt_resolution_mismatch"
RESOLUTION_UNKNOWN = "unknown"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _job_suggests_pending_or_stuck(job: dict[str, Any]) -> tuple[bool, bool]:
    """Return (looks_pending, looks_failed_or_timeout)."""
    pending = False
    bad = False
    status = str(job.get("status") or job.get("state") or "").lower()
    phase = str(job.get("phase") or "").lower()
    err = job.get("error") or job.get("last_error")
    if status in {"pending", "queued", "running", "submitted", "in_progress"}:
        pending = True
    if phase in {"pending", "running", "submitted"}:
        pending = True
    if status in {"failed", "error", "timeout", "timed_out", "cancelled"}:
        bad = True
    if isinstance(err, str) and err.strip():
        low = err.lower()
        if "timeout" in low or "timed out" in low:
            bad = True
    return pending, bad


def artifact_resolution_status(
    artifact_dir: Path,
    *,
    expected_attempt_id: str | None = None,
    ledger_artifact_dir: str | None = None,
    score_artifact: Callable[[Path], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Inspect directory state without requiring the CLI when score_artifact is omitted.

    When score_artifact is provided, may classify fully_scored vs ready_to_score.
    """
    path = artifact_dir.resolve()
    out: dict[str, Any] = {
        "artifact_dir": str(path),
        "resolution": RESOLUTION_UNKNOWN,
        "reason": None,
        "deep_replay_job_id": None,
        "deep_replay_job": None,
        "has_sensitivity_response": False,
        "attempt_mismatch": False,
    }

    if ledger_artifact_dir and expected_attempt_id:
        try:
            left = str(path.resolve()).lower()
            right = str(Path(str(ledger_artifact_dir)).resolve()).lower()
            if left != right:
                out["attempt_mismatch"] = True
                out["resolution"] = RESOLUTION_ATTEMPT_MISMATCH
                out["reason"] = "attempt_id points at a different artifact_dir than provided"
                return out
        except OSError:
            out["attempt_mismatch"] = True
            out["resolution"] = RESOLUTION_ATTEMPT_MISMATCH
            out["reason"] = "could not compare artifact paths"
            return out

    if not path.is_dir():
        out["resolution"] = RESOLUTION_ARTIFACTS_INCOMPLETE
        out["reason"] = "artifact_dir is not a directory"
        return out

    sens = path / "sensitivity-response.json"
    out["has_sensitivity_response"] = sens.exists()
    job_path = path / "deep-replay-job.json"
    job = _read_json(job_path)
    if job is not None:
        out["deep_replay_job"] = job
        out["deep_replay_job_id"] = job.get("id") or job.get("job_id") or job.get("jobId")
        pending, bad = _job_suggests_pending_or_stuck(job)

        if not sens.exists():
            if pending and not bad:
                out["resolution"] = RESOLUTION_JOB_PENDING
                out["reason"] = "deep-replay job present without sensitivity-response yet"
            elif bad:
                out["resolution"] = RESOLUTION_JOB_PENDING
                out["reason"] = "deep-replay job in failed/timeout state without sensitivity-response"
            else:
                out["resolution"] = RESOLUTION_ARTIFACTS_INCOMPLETE
                out["reason"] = "missing sensitivity-response.json"
            return out

        if bad and sens.exists():
            out["resolution"] = RESOLUTION_JOB_TIMED_OUT_ARTIFACTS_PRESENT
            out["reason"] = (
                "job metadata suggests failure/timeout but sensitivity-response exists"
            )
            # Continue to scoring hint below

    if not sens.exists():
        out["resolution"] = RESOLUTION_ARTIFACTS_INCOMPLETE
        out["reason"] = "missing sensitivity-response.json"
        return out

    if score_artifact is None:
        out["resolution"] = RESOLUTION_READY_TO_SCORE
        out["reason"] = "sensitivity-response present (scoring not run here)"
        return out

    try:
        compare = score_artifact(path)
    except Exception as exc:  # noqa: BLE001
        out["resolution"] = RESOLUTION_READY_TO_SCORE
        out["reason"] = f"score_artifact failed: {exc!s}"[:500]
        return out

    best = compare.get("best")
    score_val = None
    if isinstance(best, dict):
        score_val = best.get("quality_score")
    if score_val is not None:
        out["resolution"] = RESOLUTION_FULLY_SCORED
        out["reason"] = "compare payload includes quality_score"
        out["quality_score"] = score_val
    else:
        out["resolution"] = RESOLUTION_READY_TO_SCORE
        out["reason"] = "compare ran but no quality_score on best cell"
    out["compare_keys"] = (
        list(compare.keys())[:20] if isinstance(compare, dict) else None
    )
    return out
