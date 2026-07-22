from __future__ import annotations

import csv
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .catalog_index import iter_playhand_run_ids
from .config import AppConfig
from .ledger import (
    attempts_path_for_run_dir,
    list_run_dirs,
    load_attempts,
    load_run_metadata,
)


SCHEMA_VERSION = "playhand_efficiency_report_v1"
PLAY_HAND_RUNNER = "play_hand_v1"
DEFAULT_PLAYHAND_EFFICIENCY_DIRNAME = "playhand-efficiency-report"

ROW_FIELDNAMES = [
    "run_id",
    "created_at",
    "run_status",
    "selected_final_branch",
    "final_scrutiny_score",
    "dealt_indicator_source",
    "dealt_recipe",
    "family_policy",
    "family_policy_decision",
    "family_exact_template_available",
    "family_exact_template_used_as_incumbent",
    "coarse_halving_mode",
    "coarse_halving_decision",
    "coarse_halving_expanded",
    "coarse_estimated_saved_evaluations",
    "early_exit_mode",
    "early_exit_last_checkpoint",
    "early_exit_last_action",
    "early_exit_terminal_enforced",
    "early_exit_skip_instrument_scout",
    "early_exit_enforcement_suppressed",
    "early_exit_skipped_stage_count",
    "early_exit_estimated_coarse_permutations_avoided",
    "early_exit_estimated_instrument_scout_evals_avoided",
    "early_exit_estimated_deep_replay_jobs_avoided",
    "attempt_count",
    "observed_elapsed_seconds",
    "calendar_status",
    "calendar_passed",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _as_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_int(value: Any, default: int = 0) -> int:
    number = _safe_float(value)
    if number is None:
        return default
    return int(number)


def _round(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(value, digits)


def _rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def _counter_payload(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _list_counter_payload(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"name": name, "count": count}
        for name, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _observed_elapsed_seconds(
    metadata: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> float | None:
    timestamps = [
        parsed
        for parsed in [_parse_datetime(metadata.get("created_at"))]
        if parsed is not None
    ]
    for attempt in attempts:
        parsed = _parse_datetime(attempt.get("created_at"))
        if parsed is not None:
            timestamps.append(parsed)
    if len(timestamps) < 2:
        return None
    elapsed = (max(timestamps) - min(timestamps)).total_seconds()
    return round(elapsed, 3) if elapsed >= 0 else None


def _median(values: list[float]) -> float | None:
    values = sorted(value for value in values if value == value)
    if not values:
        return None
    midpoint = len(values) // 2
    if len(values) % 2:
        return round(values[midpoint], 3)
    return round((values[midpoint - 1] + values[midpoint]) / 2.0, 3)


def _is_playhand_run(run_dir: Path, metadata: dict[str, Any]) -> bool:
    runner = str(metadata.get("runner") or "").strip()
    return runner == PLAY_HAND_RUNNER or run_dir.name.endswith("-playhand-v1")


def _family_policy_name(metadata: dict[str, Any]) -> str:
    execution = _as_dict(metadata.get("family_policy_execution"))
    direct = _clean(execution.get("family_policy"))
    if direct:
        return direct
    policy = metadata.get("dealt_pair_family_policy")
    if isinstance(policy, dict):
        direct = _clean(policy.get("family_policy") or policy.get("playhand_family_policy"))
        if direct:
            return direct
    return "none"


def _score(metadata: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _safe_float(metadata.get(key))
        if value is not None:
            return value
    health_scores = _as_dict(_as_dict(metadata.get("play_hand_health")).get("scores"))
    for key in keys:
        value = _safe_float(health_scores.get(key))
        if value is not None:
            return value
    return None


def _calendar_status(metadata: dict[str, Any]) -> tuple[str, bool | None, list[str]]:
    health_calendar = _as_dict(_as_dict(metadata.get("play_hand_health")).get("calendar"))
    status = _clean(health_calendar.get("status"))
    passed = _as_bool(health_calendar.get("passed"))
    reasons = [str(item) for item in _as_list(health_calendar.get("reasons")) if str(item)]
    if status:
        return status, passed, reasons
    gate = _as_dict(metadata.get("calendar_gate"))
    passed = _as_bool(gate.get("passed"))
    reasons = [str(item) for item in _as_list(gate.get("reasons")) if str(item)]
    if passed is True:
        return "passed", True, reasons
    if passed is False:
        return "failed", False, reasons
    return "unknown", None, reasons


def _estimate_early_exit_savings(
    decision: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, int]:
    skipped_stages = {str(stage) for stage in _as_list(decision.get("skipped_stages"))}
    coarse_budget = _safe_int(
        metadata.get("sweep_budget_value") or metadata.get("max_sweep_permutations")
    )
    probe_budget = _safe_int(metadata.get("coarse_probe_budget"))
    if "coarse_probe" in skipped_stages or "coarse_expand" in skipped_stages:
        coarse_permutations = coarse_budget
    elif "coarse_expand" in skipped_stages and coarse_budget > probe_budget:
        coarse_permutations = coarse_budget - probe_budget
    else:
        coarse_permutations = 0
    scout_evals = (
        _safe_int(metadata.get("instrument_scout_size"), default=1)
        if "instrument_scout" in skipped_stages
        else 0
    )
    deep_replay_jobs = 0
    if "mutated_final_36mo" in skipped_stages or "final_36mo" in skipped_stages:
        deep_replay_jobs += 1
    if "exact_template_36mo" in skipped_stages and bool(
        _as_dict(metadata.get("family_policy_execution")).get("exact_template_available")
    ):
        deep_replay_jobs += 1
    return {
        "coarse_permutations_avoided": max(0, coarse_permutations),
        "instrument_scout_evals_avoided": max(0, scout_evals),
        "deep_replay_jobs_avoided": max(0, deep_replay_jobs),
    }


def _run_row(
    run_dir: Path,
    metadata: dict[str, Any],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    coarse = _as_dict(metadata.get("coarse_halving"))
    coarse_decisions = [
        decision
        for decision in _as_list(coarse.get("decisions"))
        if isinstance(decision, dict)
    ]
    coarse_last = coarse_decisions[-1] if coarse_decisions else coarse
    coarse_saved = sum(
        _safe_int(decision.get("estimated_saved_evaluations"))
        for decision in coarse_decisions
    )

    early_exit = _as_dict(metadata.get("early_exit_policy"))
    early_decisions = [
        decision
        for decision in _as_list(early_exit.get("decisions"))
        if isinstance(decision, dict)
    ]
    early_last = early_decisions[-1] if early_decisions else {}
    enforced_decisions = [
        decision for decision in early_decisions if bool(decision.get("enforced"))
    ]
    terminal_enforced = any(bool(decision.get("terminal")) for decision in enforced_decisions)
    scout_skip = any(bool(decision.get("skip_instrument_scout")) for decision in enforced_decisions)
    suppressed = any(bool(decision.get("enforcement_suppressed")) for decision in early_decisions)

    early_savings = {
        "coarse_permutations_avoided": 0,
        "instrument_scout_evals_avoided": 0,
        "deep_replay_jobs_avoided": 0,
    }
    skipped_stage_count = 0
    for decision in enforced_decisions:
        skipped_stage_count += len(_as_list(decision.get("skipped_stages")))
        savings = _estimate_early_exit_savings(decision, metadata)
        for key, value in savings.items():
            early_savings[key] += value

    family = _as_dict(metadata.get("family_policy_execution"))
    calendar_status, calendar_passed, _calendar_reasons = _calendar_status(metadata)
    return {
        "run_id": metadata.get("run_id") or run_dir.name,
        "created_at": metadata.get("created_at"),
        "run_status": metadata.get("run_status") or "unknown",
        "selected_final_branch": metadata.get("selected_final_branch") or None,
        "final_scrutiny_score": _round(_score(metadata, "final_scrutiny_score", "selected_final_score")),
        "dealt_indicator_source": metadata.get("dealt_indicator_source") or None,
        "dealt_recipe": metadata.get("dealt_recipe") or None,
        "family_policy": _family_policy_name(metadata),
        "family_policy_decision": family.get("decision") or None,
        "family_exact_template_available": bool(family.get("exact_template_available")),
        "family_exact_template_used_as_incumbent": bool(
            family.get("exact_template_used_as_incumbent")
        ),
        "coarse_halving_mode": coarse.get("mode") or metadata.get("coarse_halving_mode") or "off",
        "coarse_halving_decision": coarse_last.get("decision") or None,
        "coarse_halving_expanded": _as_bool(coarse_last.get("expanded")),
        "coarse_estimated_saved_evaluations": coarse_saved,
        "early_exit_mode": early_exit.get("mode") or metadata.get("early_exit_mode") or "off",
        "early_exit_last_checkpoint": early_last.get("checkpoint") or None,
        "early_exit_last_action": early_last.get("enforce_action") or None,
        "early_exit_terminal_enforced": terminal_enforced,
        "early_exit_skip_instrument_scout": scout_skip,
        "early_exit_enforcement_suppressed": suppressed,
        "early_exit_skipped_stage_count": skipped_stage_count,
        "early_exit_estimated_coarse_permutations_avoided": early_savings[
            "coarse_permutations_avoided"
        ],
        "early_exit_estimated_instrument_scout_evals_avoided": early_savings[
            "instrument_scout_evals_avoided"
        ],
        "early_exit_estimated_deep_replay_jobs_avoided": early_savings[
            "deep_replay_jobs_avoided"
        ],
        "attempt_count": len(attempts),
        "observed_elapsed_seconds": _observed_elapsed_seconds(metadata, attempts),
        "calendar_status": calendar_status,
        "calendar_passed": calendar_passed,
    }


def _select_run_dirs(
    runs_root: Path,
    *,
    limit: int | None,
    run_ids: list[str] | None,
    config: AppConfig | None = None,
) -> list[tuple[Path, dict[str, Any]]]:
    wanted_ids = {str(run_id).strip() for run_id in (run_ids or []) if str(run_id).strip()}
    matched: list[tuple[Path, dict[str, Any]]] = []

    catalog_run_ids: list[str] | None = None
    if config is not None:
        catalog_run_ids = list(iter_playhand_run_ids(config))
        if not catalog_run_ids:
            catalog_run_ids = None

    if catalog_run_ids is not None:
        candidate_ids = [
            run_id
            for run_id in catalog_run_ids
            if not wanted_ids or run_id in wanted_ids
        ]
        if limit is not None and limit <= 0:
            return []
        if limit is not None and limit > 0 and len(candidate_ids) > limit:
            candidate_ids = candidate_ids[-limit:]
        for run_id in candidate_ids:
            run_dir = runs_root / run_id
            if not run_dir.is_dir():
                continue
            metadata = load_run_metadata(run_dir)
            if not _is_playhand_run(run_dir, metadata):
                continue
            matched.append((run_dir, metadata))
        return matched

    for run_dir in list_run_dirs(runs_root):
        if wanted_ids and run_dir.name not in wanted_ids:
            continue
        metadata = load_run_metadata(run_dir)
        if not _is_playhand_run(run_dir, metadata):
            continue
        matched.append((run_dir, metadata))
    if limit is not None and limit <= 0:
        return []
    if limit is not None and limit > 0 and len(matched) > limit:
        matched = matched[-limit:]
    return matched


def build_playhand_efficiency_report(
    runs_root: Path,
    *,
    limit: int | None = 200,
    run_ids: list[str] | None = None,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    matched = _select_run_dirs(
        runs_root,
        limit=limit,
        run_ids=run_ids,
        config=config,
    )
    rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    selected_branch_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    calendar_counts: Counter[str] = Counter()
    calendar_failure_reasons: Counter[str] = Counter()
    final_scores: list[float] = []
    elapsed_seconds: list[float] = []

    coarse_mode_counts: Counter[str] = Counter()
    coarse_decision_counts: Counter[str] = Counter()
    early_mode_counts: Counter[str] = Counter()
    early_checkpoint_counts: Counter[str] = Counter()
    early_action_counts: Counter[str] = Counter()
    early_rule_counts: Counter[str] = Counter()
    early_skipped_stage_counts: Counter[str] = Counter()
    family_mode_counts: Counter[str] = Counter()
    family_policy_counts: Counter[str] = Counter()
    family_decision_counts: Counter[str] = Counter()
    family_skipped_stage_counts: Counter[str] = Counter()

    coarse_runs_with_decisions = 0
    coarse_expanded_runs = 0
    coarse_skipped_expansion_runs = 0
    coarse_saved_evaluations = 0
    early_would_exit_decisions = 0
    early_enforced_terminal_tombstones = 0
    early_enforced_scout_skips = 0
    early_suppressed_decisions = 0
    early_runs_with_enforcement = 0
    early_saved = {
        "coarse_permutations_avoided": 0,
        "instrument_scout_evals_avoided": 0,
        "deep_replay_jobs_avoided": 0,
    }
    family_exact_available_runs = 0
    family_exact_incumbent_runs = 0
    family_mutation_disallowed_runs = 0
    family_exact_only_skip_runs = 0

    for run_dir, metadata in matched:
        attempts = load_attempts(attempts_path_for_run_dir(run_dir))
        row = _run_row(run_dir, metadata, attempts)
        rows.append(row)

        status_counts[str(row["run_status"] or "unknown")] += 1
        selected_branch_counts[str(row["selected_final_branch"] or "none")] += 1
        source_counts[str(row["dealt_indicator_source"] or "unknown")] += 1
        calendar_status, _calendar_passed, calendar_reasons = _calendar_status(metadata)
        calendar_counts[calendar_status] += 1
        if calendar_status == "failed":
            calendar_failure_reasons.update(calendar_reasons or ["failed"])
        final_score = _safe_float(row.get("final_scrutiny_score"))
        if final_score is not None:
            final_scores.append(final_score)
        elapsed = _safe_float(row.get("observed_elapsed_seconds"))
        if elapsed is not None:
            elapsed_seconds.append(elapsed)

        coarse = _as_dict(metadata.get("coarse_halving"))
        coarse_mode_counts[str(coarse.get("mode") or metadata.get("coarse_halving_mode") or "off")] += 1
        coarse_decisions = [
            decision
            for decision in _as_list(coarse.get("decisions"))
            if isinstance(decision, dict)
        ]
        if coarse_decisions:
            coarse_runs_with_decisions += 1
        for decision in coarse_decisions:
            name = str(decision.get("decision") or "unknown")
            coarse_decision_counts[name] += 1
            saved = _safe_int(decision.get("estimated_saved_evaluations"))
            coarse_saved_evaluations += saved
            if bool(decision.get("expanded")):
                coarse_expanded_runs += 1
            else:
                coarse_skipped_expansion_runs += 1

        early_exit = _as_dict(metadata.get("early_exit_policy"))
        early_mode_counts[str(early_exit.get("mode") or metadata.get("early_exit_mode") or "off")] += 1
        early_decisions = [
            decision
            for decision in _as_list(early_exit.get("decisions"))
            if isinstance(decision, dict)
        ]
        run_had_enforcement = False
        for decision in early_decisions:
            early_checkpoint_counts[str(decision.get("checkpoint") or "unknown")] += 1
            early_action_counts[str(decision.get("enforce_action") or "none")] += 1
            early_rule_counts.update(
                str(rule) for rule in _as_list(decision.get("rules_fired")) if str(rule)
            )
            if bool(decision.get("would_exit")):
                early_would_exit_decisions += 1
            if bool(decision.get("enforcement_suppressed")):
                early_suppressed_decisions += 1
            if bool(decision.get("enforced")):
                run_had_enforcement = True
                early_skipped_stage_counts.update(
                    str(stage)
                    for stage in _as_list(decision.get("skipped_stages"))
                    if str(stage)
                )
                savings = _estimate_early_exit_savings(decision, metadata)
                for key, value in savings.items():
                    early_saved[key] += value
                if bool(decision.get("terminal")):
                    early_enforced_terminal_tombstones += 1
                if bool(decision.get("skip_instrument_scout")):
                    early_enforced_scout_skips += 1
        if run_had_enforcement:
            early_runs_with_enforcement += 1

        family = _as_dict(metadata.get("family_policy_execution"))
        family_mode_counts[str(family.get("mode") or metadata.get("family_policy_mode") or "off")] += 1
        family_policy_counts[_family_policy_name(metadata)] += 1
        family_decision = str(family.get("decision") or "unknown")
        family_decision_counts[family_decision] += 1
        skipped_stages = [
            str(stage) for stage in _as_list(family.get("skipped_stages")) if str(stage)
        ]
        family_skipped_stage_counts.update(skipped_stages)
        if bool(family.get("exact_template_available")):
            family_exact_available_runs += 1
        if bool(family.get("exact_template_used_as_incumbent")):
            family_exact_incumbent_runs += 1
        if family.get("mutation_allowed") is False:
            family_mutation_disallowed_runs += 1
        if (
            "template_locked_exact_only" in family_decision
            or family.get("mutation_allowed") is False
            or bool(set(skipped_stages) & {"lookback_timing", "coarse_probe", "coarse_expand", "focused"})
        ):
            family_exact_only_skip_runs += 1

    row_savings = sorted(
        rows,
        key=lambda row: (
            _safe_int(row.get("coarse_estimated_saved_evaluations"))
            + _safe_int(row.get("early_exit_estimated_coarse_permutations_avoided"))
            + _safe_int(row.get("early_exit_estimated_instrument_scout_evals_avoided"))
            + _safe_int(row.get("early_exit_estimated_deep_replay_jobs_avoided")),
            str(row.get("run_id") or ""),
        ),
        reverse=True,
    )[:10]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "filters": {
            "limit": limit,
            "run_ids": run_ids or None,
        },
        "window": {
            "first_run_id": rows[0]["run_id"] if rows else None,
            "last_run_id": rows[-1]["run_id"] if rows else None,
        },
        "run_count": len(rows),
        "completed_count": sum(
            1 for row in rows if str(row.get("run_status") or "") in {"promoted", "tombstoned"}
        ),
        "status_counts": _counter_payload(status_counts),
        "selected_final_branch_counts": _counter_payload(selected_branch_counts),
        "dealt_indicator_source_counts": _counter_payload(source_counts),
        "final_36m": {
            "scored_runs": len(final_scores),
            "positive_final_scores": sum(1 for value in final_scores if value > 0),
            "average_final_score": _round(
                sum(final_scores) / len(final_scores) if final_scores else None
            ),
            "best_final_score": _round(max(final_scores) if final_scores else None),
        },
        "elapsed": {
            "observed_runs": len(elapsed_seconds),
            "total_observed_seconds": _round(sum(elapsed_seconds), 3),
            "average_observed_seconds": _round(
                sum(elapsed_seconds) / len(elapsed_seconds) if elapsed_seconds else None,
                3,
            ),
            "median_observed_seconds": _median(elapsed_seconds),
        },
        "coarse_halving": {
            "mode_counts": _counter_payload(coarse_mode_counts),
            "runs_with_decisions": coarse_runs_with_decisions,
            "decision_counts": _counter_payload(coarse_decision_counts),
            "expanded_runs": coarse_expanded_runs,
            "skipped_expansion_runs": coarse_skipped_expansion_runs,
            "total_estimated_saved_evaluations": coarse_saved_evaluations,
            "average_saved_evaluations_per_run": _round(
                coarse_saved_evaluations / len(rows) if rows else None
            ),
        },
        "early_exit": {
            "mode_counts": _counter_payload(early_mode_counts),
            "runs_with_enforcement": early_runs_with_enforcement,
            "checkpoint_counts": _counter_payload(early_checkpoint_counts),
            "action_counts": _counter_payload(early_action_counts),
            "rule_counts": _counter_payload(early_rule_counts),
            "would_exit_decisions": early_would_exit_decisions,
            "enforced_terminal_tombstones": early_enforced_terminal_tombstones,
            "enforced_scout_skips": early_enforced_scout_skips,
            "enforcement_suppressed_decisions": early_suppressed_decisions,
            "skipped_stage_counts": _counter_payload(early_skipped_stage_counts),
            "estimated_saved": early_saved,
        },
        "family_policy": {
            "mode_counts": _counter_payload(family_mode_counts),
            "policy_counts": _counter_payload(family_policy_counts),
            "decision_counts": _counter_payload(family_decision_counts),
            "exact_template_available_runs": family_exact_available_runs,
            "exact_template_used_as_incumbent_runs": family_exact_incumbent_runs,
            "mutation_disallowed_runs": family_mutation_disallowed_runs,
            "exact_only_skip_runs": family_exact_only_skip_runs,
            "skipped_stage_counts": _counter_payload(family_skipped_stage_counts),
        },
        "calendar": {
            "status_counts": _counter_payload(calendar_counts),
            "failure_reason_counts": _counter_payload(calendar_failure_reasons),
        },
        "top_savings_runs": row_savings,
    }
    summary["mechanism_rates"] = {
        "coarse_skip_rate": _rate(coarse_skipped_expansion_runs, coarse_runs_with_decisions),
        "early_exit_enforcement_rate": _rate(early_runs_with_enforcement, len(rows)),
        "family_exact_available_rate": _rate(family_exact_available_runs, len(rows)),
        "final_positive_rate": _rate(
            int(summary["final_36m"]["positive_final_scores"]),
            int(summary["final_36m"]["scored_runs"]),
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "summary": summary,
        "rows": rows,
    }


def _markdown_table(rows: list[list[Any]]) -> str:
    if not rows:
        return ""
    widths = [0 for _ in rows[0]]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(str(value)))
    rendered = []
    for row_index, row in enumerate(rows):
        rendered.append(
            "| " + " | ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)) + " |"
        )
        if row_index == 0:
            rendered.append(
                "| " + " | ".join("-" * widths[index] for index, _ in enumerate(row)) + " |"
            )
    return "\n".join(rendered)


def render_playhand_efficiency_markdown(report: dict[str, Any]) -> str:
    summary = _as_dict(report.get("summary"))
    coarse = _as_dict(summary.get("coarse_halving"))
    early = _as_dict(summary.get("early_exit"))
    family = _as_dict(summary.get("family_policy"))
    final = _as_dict(summary.get("final_36m"))
    elapsed = _as_dict(summary.get("elapsed"))
    saved = _as_dict(early.get("estimated_saved"))
    lines = [
        "# PlayHand Efficiency Report",
        "",
        f"Generated: {summary.get('generated_at')}",
        f"Runs: {summary.get('run_count')} ({summary.get('window', {}).get('first_run_id')} to {summary.get('window', {}).get('last_run_id')})",
        "",
        "## Savings Signals",
        "",
        _markdown_table(
            [
                ["Mechanism", "Value"],
                ["Coarse saved evaluations", coarse.get("total_estimated_saved_evaluations", 0)],
                ["Coarse skipped expansions", coarse.get("skipped_expansion_runs", 0)],
                ["Early terminal tombstones", early.get("enforced_terminal_tombstones", 0)],
                ["Early scout skips", early.get("enforced_scout_skips", 0)],
                ["Early coarse permutations avoided", saved.get("coarse_permutations_avoided", 0)],
                ["Instrument scout evals avoided", saved.get("instrument_scout_evals_avoided", 0)],
                ["Deep replay jobs avoided", saved.get("deep_replay_jobs_avoided", 0)],
                ["Family exact-only skip runs", family.get("exact_only_skip_runs", 0)],
            ]
        ),
        "",
        "## Outcome Snapshot",
        "",
        _markdown_table(
            [
                ["Metric", "Value"],
                ["Completed runs", summary.get("completed_count", 0)],
                ["Scored 36m runs", final.get("scored_runs", 0)],
                ["Positive final scores", final.get("positive_final_scores", 0)],
                ["Average final score", final.get("average_final_score")],
                ["Median observed seconds", elapsed.get("median_observed_seconds")],
            ]
        ),
        "",
        "## Mechanism Counts",
        "",
        "### Coarse Halving",
        "",
        _markdown_table([["Decision", "Count"], *_counter_rows(coarse.get("decision_counts"))]),
        "",
        "### Early Exit Actions",
        "",
        _markdown_table([["Action", "Count"], *_counter_rows(early.get("action_counts"))]),
        "",
        "### Family Policy",
        "",
        _markdown_table([["Policy", "Count"], *_counter_rows(family.get("policy_counts"))]),
        "",
        "## Top Savings Runs",
        "",
        _markdown_table(
            [
                [
                    "Run",
                    "Status",
                    "Branch",
                    "Coarse saved",
                    "Early coarse avoided",
                    "Scout avoided",
                    "Deep jobs avoided",
                ],
                *[
                    [
                        row.get("run_id"),
                        row.get("run_status"),
                        row.get("selected_final_branch"),
                        row.get("coarse_estimated_saved_evaluations"),
                        row.get("early_exit_estimated_coarse_permutations_avoided"),
                        row.get("early_exit_estimated_instrument_scout_evals_avoided"),
                        row.get("early_exit_estimated_deep_replay_jobs_avoided"),
                    ]
                    for row in _as_list(summary.get("top_savings_runs"))[:10]
                ],
            ]
        ),
        "",
    ]
    return "\n".join(lines)


def _counter_rows(value: Any) -> list[list[Any]]:
    if not isinstance(value, dict):
        return []
    return [[key, count] for key, count in value.items()]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROW_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in ROW_FIELDNAMES})


def write_playhand_efficiency_report(
    report: dict[str, Any],
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "playhand-efficiency-report.json"
    markdown_path = output_dir / "playhand-efficiency-report.md"
    csv_path = output_dir / "playhand-efficiency-runs.csv"
    _write_json(json_path, report)
    markdown_path.write_text(
        render_playhand_efficiency_markdown(report),
        encoding="utf-8",
    )
    _write_csv(csv_path, list(report.get("rows") or []))
    return {
        "playhand_efficiency_report_json": str(json_path),
        "playhand_efficiency_report_markdown": str(markdown_path),
        "playhand_efficiency_runs_csv": str(csv_path),
    }
