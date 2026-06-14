from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ledger import load_attempts, load_run_metadata, write_run_metadata


PLAY_HAND_HEALTH_VERSION = "play_hand_health_v1"
PLAY_HAND_RUNNER = "play_hand_v1"
PLAY_HAND_HEALTH_STATUSES = {
    "in_progress",
    "tombstoned",
    "canonical_retained",
    "research_retained",
    "missing_artifacts",
    "legacy_unclassified",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric else None


def _as_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _clean_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _unique_sorted(values: list[str]) -> list[str]:
    return sorted({value for value in values if value})


def _is_playhand(run_metadata: dict[str, Any], attempts: list[dict[str, Any]]) -> bool:
    if str(run_metadata.get("runner") or "").strip() == PLAY_HAND_RUNNER:
        return True
    return any(
        str(attempt.get("runner") or "").strip() == PLAY_HAND_RUNNER
        for attempt in attempts
    )


def _phase_score(run_metadata: dict[str, Any], *phase_names: str) -> float | None:
    phase_scores = run_metadata.get("play_hand_phase_scores")
    if isinstance(phase_scores, dict):
        for phase_name in phase_names:
            value = _as_float(phase_scores.get(phase_name))
            if value is not None:
                return value
    for row in list(run_metadata.get("phase_rows") or []):
        if not isinstance(row, dict):
            continue
        phase = str(row.get("phase") or "").strip().lower().replace(" ", "_")
        if phase in phase_names:
            value = _as_float(row.get("score"))
            if value is not None:
                return value
    return None


def select_play_hand_health_attempt(
    run_metadata: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    canonical_id = str(run_metadata.get("canonical_attempt_id") or "").strip()
    final_id = str(run_metadata.get("final_attempt_id") or "").strip()
    for wanted_id in (canonical_id, final_id):
        if not wanted_id:
            continue
        for attempt in attempts:
            if str(attempt.get("attempt_id") or "").strip() == wanted_id:
                return attempt
    for attempt in attempts:
        if bool(attempt.get("is_canonical_playhand_attempt")):
            return attempt
    for attempt in attempts:
        role = str(
            attempt.get("attempt_role") or attempt.get("play_hand_role") or ""
        ).strip()
        if role == "final":
            return attempt
    return attempts[-1] if attempts else None


def _final_attempt_id(
    run_metadata: dict[str, Any],
    selected_attempt: dict[str, Any],
) -> str | None:
    explicit_final_id = str(run_metadata.get("final_attempt_id") or "").strip()
    if explicit_final_id:
        return explicit_final_id
    attempt_id = str(selected_attempt.get("attempt_id") or "").strip()
    if not attempt_id:
        return None
    role = str(
        selected_attempt.get("attempt_role") or selected_attempt.get("play_hand_role") or ""
    ).strip()
    is_finalish = bool(
        selected_attempt.get("is_canonical_playhand_attempt")
        or role == "final"
        or selected_attempt.get("final_scrutiny_score") is not None
        or selected_attempt.get("final_scrutiny_passed") is not None
    )
    return attempt_id if is_finalish else None


def _selected_catalog_row(catalog_row: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(catalog_row, dict) and catalog_row:
        return catalog_row
    return {}


def _selected_catalog_row_for_healing(
    run_metadata: dict[str, Any],
    attempts: list[dict[str, Any]],
    catalog_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if isinstance(catalog_row, dict) and catalog_row:
        return catalog_row
    selected_attempt = select_play_hand_health_attempt(run_metadata, attempts)
    if not isinstance(selected_attempt, dict) or not selected_attempt:
        return None
    from .corpus_tools import extract_attempt_catalog_row

    return extract_attempt_catalog_row(selected_attempt, run_metadata)


def _calendar_evidence(run_metadata: dict[str, Any], catalog_row: dict[str, Any]) -> dict[str, Any]:
    gate = run_metadata.get("calendar_gate")
    gate = gate if isinstance(gate, dict) else {}
    metrics = gate.get("metrics") if isinstance(gate.get("metrics"), dict) else {}
    available = bool(gate) or bool(catalog_row.get("has_full_backtest_calendar_curve_36m"))
    passed = _as_bool(gate.get("passed"))
    segments_positive = (
        metrics.get("segments_positive")
        if metrics.get("segments_positive") is not None
        else metrics.get("positive_segment_count")
    )
    min_segments_positive = (
        metrics.get("min_segments_positive")
        if metrics.get("min_segments_positive") is not None
        else metrics.get("min_positive_segments")
    )
    return {
        "available": available,
        "mode": run_metadata.get("calendar_gate_mode"),
        "passed": passed,
        "reasons": _clean_strings(gate.get("reasons")),
        "segments_positive": segments_positive,
        "min_segments_positive": min_segments_positive,
        "status": (
            "passed"
            if passed is True
            else "failed"
            if passed is False
            else "curve_available"
            if available
            else "unknown"
        ),
    }


def build_play_hand_evidence(
    *,
    run_metadata: dict[str, Any],
    attempts: list[dict[str, Any]] | None = None,
    catalog_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_metadata = run_metadata if isinstance(run_metadata, dict) else {}
    attempts = list(attempts or [])
    catalog_row = _selected_catalog_row(catalog_row)
    selected_attempt = select_play_hand_health_attempt(run_metadata, attempts) or {}

    final_score = _as_float(
        run_metadata.get("final_scrutiny_score")
        if run_metadata.get("final_scrutiny_score") is not None
        else run_metadata.get("final_score")
    )
    if final_score is None:
        final_score = _as_float(selected_attempt.get("final_scrutiny_score"))
    if final_score is None:
        final_score = _as_float(catalog_row.get("score_36m"))
    final_passed = _as_bool(run_metadata.get("final_scrutiny_passed"))
    if final_passed is None:
        final_passed = _as_bool(selected_attempt.get("final_scrutiny_passed"))
    if final_passed is None and final_score is not None:
        final_passed = final_score > 0.0

    baseline = _phase_score(run_metadata, "baseline")
    lookback = _phase_score(
        run_metadata,
        "lookback",
        "lookback_top_3mo",
        "lookback_timing_top_3mo",
    )
    coarse = _phase_score(run_metadata, "coarse", "coarse_top_3mo")
    focused = _phase_score(run_metadata, "focused", "focused_top_3mo")
    mutated = _as_float(run_metadata.get("mutated_score"))
    exact = _as_float(run_metadata.get("exact_template_score"))
    scores = {
        "baseline_3mo": baseline,
        "lookback_top_3mo": lookback,
        "coarse_top_3mo": coarse,
        "focused_top_3mo": focused,
        "mutated_36mo": mutated,
        "exact_template_36mo": exact,
        "selected_final_score": final_score,
    }

    run_tombstoned = bool(
        run_metadata.get("run_tombstoned")
        or run_metadata.get("tombstoned")
        or run_metadata.get("is_tombstoned_run")
        or selected_attempt.get("run_tombstoned")
        or selected_attempt.get("is_tombstoned")
        or selected_attempt.get("attempt_tombstoned")
        or selected_attempt.get("is_tombstoned_attempt")
    )
    tombstone_reason = str(
        run_metadata.get("tombstone_reason")
        or run_metadata.get("run_tombstone_reason")
        or selected_attempt.get("tombstone_reason")
        or selected_attempt.get("run_tombstone_reason")
        or ""
    ).strip() or None
    final = {
        "final_attempt_id": _final_attempt_id(run_metadata, selected_attempt),
        "canonical_attempt_id": run_metadata.get("canonical_attempt_id") or None,
        "selected_final_branch": run_metadata.get("selected_final_branch"),
        "final_scrutiny_passed": final_passed,
        "run_tombstoned": run_tombstoned,
        "tombstone_reason": tombstone_reason,
    }
    calendar = _calendar_evidence(run_metadata, catalog_row)
    missing: list[str] = []
    if not attempts:
        missing.append("attempts")
    full_status = str(
        catalog_row.get("full_backtest_validation_status_36m") or "unknown"
    )
    if full_status in {"missing", "unknown"}:
        missing.append("full_backtest_36m")
    elif full_status == "invalid":
        missing.append("valid_full_backtest_36m")

    source = {
        "runner": run_metadata.get("runner"),
        "dealt_indicator_source": run_metadata.get("dealt_indicator_source"),
        "dealt_recipe": run_metadata.get("dealt_recipe"),
        "dealt_recipe_source": run_metadata.get("dealt_recipe_source"),
        "template_branch_source_probe_id": run_metadata.get(
            "template_branch_source_probe_id"
        ),
        "family_policy": run_metadata.get("dealt_pair_family_policy"),
        "recipe_policy": run_metadata.get("dealt_recipe_confidence"),
    }
    return {
        "version": PLAY_HAND_HEALTH_VERSION,
        "is_play_hand": _is_playhand(run_metadata, attempts),
        "source": source,
        "scores": scores,
        "final": final,
        "calendar": calendar,
        "artifacts": {
            "attempts_available": bool(attempts),
            "full_backtest_36m_status": full_status,
            "missing": _unique_sorted(missing),
        },
        "early_exit_inputs": {
            "safe_for_report_mode": bool(_is_playhand(run_metadata, attempts)),
            "family_policy": source["family_policy"],
            "guided_or_role_balanced": source["dealt_indicator_source"],
            "lookback_delta_vs_baseline": (
                lookback - baseline if lookback is not None and baseline is not None else None
            ),
            "coarse_delta_vs_baseline": (
                coarse - baseline if coarse is not None and baseline is not None else None
            ),
            "focused_delta_vs_baseline": (
                focused - baseline if focused is not None and baseline is not None else None
            ),
            "final_delta_vs_focused": (
                final_score - focused
                if final_score is not None and focused is not None
                else None
            ),
        },
    }


def build_play_hand_health(
    *,
    run_metadata: dict[str, Any],
    attempts: list[dict[str, Any]] | None = None,
    catalog_row: dict[str, Any] | None = None,
    computed_at: str | None = None,
) -> dict[str, Any]:
    evidence = build_play_hand_evidence(
        run_metadata=run_metadata,
        attempts=attempts,
        catalog_row=catalog_row,
    )
    final = evidence["final"]
    artifacts = evidence["artifacts"]
    calendar = evidence["calendar"]
    scores = evidence["scores"]
    reasons = _clean_strings(run_metadata.get("tombstone_reasons"))
    tombstone_reason = final.get("tombstone_reason")
    if tombstone_reason:
        reasons.append(str(tombstone_reason))

    if not evidence["is_play_hand"]:
        status = "legacy_unclassified"
        reasons.append("not_play_hand_run")
    elif final.get("run_tombstoned"):
        status = "tombstoned"
    elif final.get("final_attempt_id") is None and scores.get("selected_final_score") is None:
        status = "in_progress"
    elif scores.get("selected_final_score") is None:
        status = "missing_artifacts"
        reasons.append("missing_final_36mo_score")
    elif final.get("final_scrutiny_passed") is False or float(scores["selected_final_score"]) <= 0.0:
        status = "tombstoned"
        reasons.append("final_36mo_scrutiny_failed")
    elif final.get("canonical_attempt_id"):
        status = "canonical_retained"
    elif artifacts.get("missing"):
        status = "missing_artifacts"
    else:
        status = "research_retained"

    if calendar.get("passed") is False:
        reason = (
            "calendar_gate_failed_report_only"
            if calendar.get("mode") == "report"
            else "calendar_gate_failed"
        )
        reasons.append(reason)
    for reason in _clean_strings(calendar.get("reasons")):
        reasons.append(f"calendar:{reason}")

    if status not in PLAY_HAND_HEALTH_STATUSES:
        status = "legacy_unclassified"
        reasons.append("invalid_health_status")

    return {
        "version": PLAY_HAND_HEALTH_VERSION,
        "status": status,
        "reasons": _unique_sorted(reasons),
        "computed_at": computed_at or _utc_now_iso(),
        **{
            key: evidence[key]
            for key in ("source", "scores", "final", "calendar", "artifacts", "early_exit_inputs")
        },
    }


def heal_play_hand_run_metadata(
    run_dir: Path,
    *,
    catalog_row: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    run_metadata = load_run_metadata(run_dir)
    attempts_path = run_dir / "attempts.jsonl"
    attempts = load_attempts(attempts_path)
    existing = run_metadata.get("play_hand_health")
    existing = existing if isinstance(existing, dict) else {}
    resolved_catalog_row = _selected_catalog_row_for_healing(
        run_metadata,
        attempts,
        catalog_row,
    )
    prior_computed_at = (
        str(existing.get("computed_at") or "").strip()
        if not force and existing.get("version") == PLAY_HAND_HEALTH_VERSION
        else ""
    )
    health = build_play_hand_health(
        run_metadata=run_metadata,
        attempts=attempts,
        catalog_row=resolved_catalog_row,
        computed_at=prior_computed_at or None,
    )
    if health.get("status") == "legacy_unclassified":
        return {
            "run_id": run_metadata.get("run_id") or run_dir.name,
            "updated": False,
            "play_hand_health": health,
        }
    updated = False
    if run_metadata.get("play_hand_health") != health:
        run_metadata["play_hand_health"] = health
        write_run_metadata(run_dir, run_metadata)
        updated = True
    return {
        "run_id": run_metadata.get("run_id") or run_dir.name,
        "updated": updated,
        "play_hand_health": health,
    }
