from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import csv
import math
import shutil
from pathlib import Path
from statistics import median
from typing import Any

import matplotlib.pyplot as plt
import json

from .ledger import load_run_metadata


def compute_frontier(
    attempts: list[dict[str, Any]],
    *,
    lower_is_better: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    frontier: list[dict[str, Any]] = []
    non_frontier: list[dict[str, Any]] = []
    best_score: float | None = None
    for attempt in attempts:
        score = attempt.get("composite_score")
        if score is None:
            non_frontier.append(attempt)
            continue
        score = float(score)
        improved = best_score is None or (
            score < best_score if lower_is_better else score > best_score
        )
        if improved:
            frontier.append(attempt)
            best_score = score
        else:
            non_frontier.append(attempt)
    return frontier, non_frontier


def _metadata_model_summary(run_metadata: dict[str, Any] | None) -> str | None:
    if not isinstance(run_metadata, dict) or not run_metadata:
        return None
    explorer_profile = str(run_metadata.get("explorer_profile") or "").strip()
    explorer_model = str(run_metadata.get("explorer_model") or "").strip()
    supervisor_profile = str(run_metadata.get("supervisor_profile") or "").strip()
    supervisor_model = str(run_metadata.get("supervisor_model") or "").strip()
    quality_score_preset = str(run_metadata.get("quality_score_preset") or "").strip()
    parts: list[str] = []
    if explorer_model or explorer_profile:
        explorer = explorer_model or explorer_profile
        if explorer_profile and explorer_model and explorer_profile != explorer_model:
            explorer = f"{explorer_profile} / {explorer_model}"
        parts.append(f"Explorer: {explorer}")
    if supervisor_model or supervisor_profile:
        supervisor = supervisor_model or supervisor_profile
        if (
            supervisor_profile
            and supervisor_model
            and supervisor_profile != supervisor_model
        ):
            supervisor = f"{supervisor_profile} / {supervisor_model}"
        parts.append(f"Supervisor: {supervisor}")
    if quality_score_preset:
        parts.append(f"Preset: {quality_score_preset}")
    return " | ".join(parts) if parts else None


def render_progress_plot(
    attempts: list[dict[str, Any]],
    output_path: Path,
    *,
    run_metadata: dict[str, Any] | None = None,
    lower_is_better: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid = [
        attempt for attempt in attempts if attempt.get("composite_score") is not None
    ]
    total_logged = len(attempts)
    model_summary = _metadata_model_summary(run_metadata)

    plt.figure(figsize=(16, 8))
    if not valid:
        title = "Autoresearch Progress: No Attempts Yet"
        if model_summary:
            title = f"{title}\n{model_summary}"
        plt.title(title)
        plt.xlabel("Attempt #")
        plt.ylabel("Quality Score")
        plt.grid(True, alpha=0.25)
        plt.tight_layout()
        plt.savefig(output_path, dpi=160)
        plt.close()
        return

    frontier, discarded = compute_frontier(valid, lower_is_better=lower_is_better)

    x_all = [attempt["sequence"] for attempt in valid]
    y_all = [float(attempt["composite_score"]) for attempt in valid]
    x_disc = [attempt["sequence"] for attempt in discarded]
    y_disc = [float(attempt["composite_score"]) for attempt in discarded]
    x_front = [attempt["sequence"] for attempt in frontier]
    y_front = [float(attempt["composite_score"]) for attempt in frontier]

    plt.scatter(x_all, y_all, c="#d6d6d6", s=14, alpha=0.35, label="Attempt")
    if discarded:
        plt.scatter(x_disc, y_disc, c="#c7c7c7", s=14, alpha=0.5, label="Non-frontier")
    plt.plot(x_front, y_front, color="#57c785", linewidth=2.0, label="Running best")
    plt.scatter(
        x_front,
        y_front,
        c="#2ecc71",
        edgecolors="#2d6a4f",
        s=46,
        zorder=3,
        label="Frontier",
    )

    for attempt in frontier:
        label = _attempt_plot_label(attempt)
        if len(label) > 40:
            label = label[:37] + "..."
        plt.annotate(
            label,
            (attempt["sequence"], float(attempt["composite_score"])),
            textcoords="offset points",
            xytext=(8, 8),
            fontsize=8,
            color="#40916c",
            rotation=28,
        )

    direction = "lower is better" if lower_is_better else "higher is better"
    title = f"Autoresearch Progress: {len(valid)} Scored / {total_logged} Logged, {len(frontier)} Frontier Points"
    if model_summary:
        title = f"{title}\n{model_summary}"
    plt.title(title)
    plt.xlabel("Attempt #")
    plt.ylabel(f"Quality Score ({direction})")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _profile_file_label(attempt: dict[str, Any]) -> str | None:
    raw = attempt.get("profile_path")
    if not isinstance(raw, str) or not raw.strip():
        return None
    return Path(raw).stem


def _attempt_plot_label(attempt: dict[str, Any]) -> str:
    sequence = attempt.get("sequence")
    prefix = f"#{sequence} " if sequence is not None else ""
    profile_label = _profile_file_label(attempt)
    if profile_label:
        return prefix + profile_label
    candidate_name = (
        str(attempt.get("candidate_name", "candidate")).strip() or "candidate"
    )
    return prefix + candidate_name


def _progress_index_rows(
    attempts: list[dict[str, Any]], run_metadata: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for attempt in attempts:
        rows.append(
            {
                "sequence": attempt.get("sequence"),
                "attempt_id": attempt.get("attempt_id"),
                "plot_label": _attempt_plot_label(attempt),
                "candidate_name": attempt.get("candidate_name"),
                "profile_file": Path(str(attempt["profile_path"])).name
                if attempt.get("profile_path")
                else None,
                "profile_path": attempt.get("profile_path"),
                "profile_ref": attempt.get("profile_ref"),
                "composite_score": attempt.get("composite_score"),
                "score_basis": attempt.get("score_basis"),
                "artifact_dir": attempt.get("artifact_dir"),
                "explorer_profile": run_metadata.get("explorer_profile")
                if isinstance(run_metadata, dict)
                else None,
                "explorer_model": run_metadata.get("explorer_model")
                if isinstance(run_metadata, dict)
                else None,
                "supervisor_profile": run_metadata.get("supervisor_profile")
                if isinstance(run_metadata, dict)
                else None,
                "supervisor_model": run_metadata.get("supervisor_model")
                if isinstance(run_metadata, dict)
                else None,
                "quality_score_preset": run_metadata.get("quality_score_preset")
                if isinstance(run_metadata, dict)
                else None,
            }
        )
    return rows


def write_progress_index(
    attempts: list[dict[str, Any]],
    output_path: Path,
    *,
    run_metadata: dict[str, Any] | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = _progress_index_rows(attempts, run_metadata)
    output_path.write_text(
        json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    csv_path = output_path.with_suffix(".csv")
    fieldnames = [
        "sequence",
        "attempt_id",
        "plot_label",
        "candidate_name",
        "profile_file",
        "profile_path",
        "profile_ref",
        "composite_score",
        "score_basis",
        "artifact_dir",
        "explorer_profile",
        "explorer_model",
        "supervisor_profile",
        "supervisor_model",
        "quality_score_preset",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_progress_artifacts(
    attempts: list[dict[str, Any]],
    primary_output_path: Path,
    *,
    run_metadata_path: Path | None = None,
    lower_is_better: bool = False,
    mirror_output_path: Path | None = None,
    mirror_attempts: list[dict[str, Any]] | None = None,
    mirror_run_metadata_path: Path | None = None,
) -> None:
    run_metadata = (
        load_run_metadata(run_metadata_path.parent)
        if run_metadata_path and run_metadata_path.exists()
        else None
    )
    render_progress_plot(
        attempts,
        primary_output_path,
        run_metadata=run_metadata,
        lower_is_better=lower_is_better,
    )
    write_progress_index(
        attempts,
        primary_output_path.with_name(f"{primary_output_path.stem}-index.json"),
        run_metadata=run_metadata,
    )
    if mirror_output_path is None:
        return
    mirror_output_path.parent.mkdir(parents=True, exist_ok=True)
    if mirror_attempts is None:
        shutil.copy2(primary_output_path, mirror_output_path)
        source_index = primary_output_path.with_name(
            f"{primary_output_path.stem}-index.json"
        )
        target_index = mirror_output_path.with_name(
            f"{mirror_output_path.stem}-index.json"
        )
        if source_index.exists():
            shutil.copy2(source_index, target_index)
            source_csv = source_index.with_suffix(".csv")
            target_csv = target_index.with_suffix(".csv")
            if source_csv.exists():
                shutil.copy2(source_csv, target_csv)
        return
    render_progress_plot(
        mirror_attempts,
        mirror_output_path,
        run_metadata=(
            load_run_metadata(mirror_run_metadata_path.parent)
            if mirror_run_metadata_path and mirror_run_metadata_path.exists()
            else None
        ),
        lower_is_better=lower_is_better,
    )
    write_progress_index(
        mirror_attempts,
        mirror_output_path.with_name(f"{mirror_output_path.stem}-index.json"),
        run_metadata=(
            load_run_metadata(mirror_run_metadata_path.parent)
            if mirror_run_metadata_path and mirror_run_metadata_path.exists()
            else None
        ),
    )


def _leaderboard_label(
    attempt: dict[str, Any], run_metadata: dict[str, Any] | None
) -> str:
    run_id = str(attempt.get("run_id", "run")).strip() or "run"
    candidate_name = (
        str(attempt.get("candidate_name", "candidate")).strip() or "candidate"
    )
    explorer_model = str((run_metadata or {}).get("explorer_model") or "").strip()
    explorer_profile = str((run_metadata or {}).get("explorer_profile") or "").strip()
    model_label = explorer_model or explorer_profile
    label = f"{run_id} | {candidate_name}"
    if model_label:
        label = f"{run_id} | {model_label} | {candidate_name}"
    if len(label) > 72:
        label = label[:69] + "..."
    return label


def _model_group_label(run_metadata: dict[str, Any] | None) -> str | None:
    payload = run_metadata or {}
    explorer_model = str(payload.get("explorer_model") or "").strip()
    explorer_profile = str(payload.get("explorer_profile") or "").strip()
    if explorer_model:
        return explorer_model
    if explorer_profile:
        return explorer_profile
    return None


def _attempt_trade_count(attempt: dict[str, Any]) -> int | None:
    best_summary = attempt.get("best_summary")
    if not isinstance(best_summary, dict):
        return None
    candidates = [
        best_summary.get("best_cell_path_metrics"),
        best_summary.get("best_cell"),
    ]
    for payload in candidates:
        if not isinstance(payload, dict):
            continue
        raw = payload.get("trade_count")
        if raw is None:
            raw = payload.get("resolved_trades")
        try:
            trade_count = int(raw)
        except (TypeError, ValueError):
            continue
        if trade_count >= 0:
            return trade_count
    return None


def _attempt_effective_window_months(attempt: dict[str, Any]) -> float | None:
    best_summary = attempt.get("best_summary")
    if not isinstance(best_summary, dict):
        return None
    current: Any = best_summary.get("quality_score_payload")
    if isinstance(current, dict):
        inputs = current.get("inputs")
        if isinstance(inputs, dict):
            try:
                value = float(inputs.get("effective_window_months"))
            except (TypeError, ValueError):
                value = None
            if value is not None and value > 0:
                return value
    market_window = best_summary.get("market_data_window")
    if isinstance(market_window, dict):
        try:
            value = float(market_window.get("effective_window_months"))
        except (TypeError, ValueError):
            value = None
        if value is not None and value > 0:
            return value
    return None


def _attempt_trades_per_month(attempt: dict[str, Any]) -> float | None:
    best_summary = attempt.get("best_summary")
    if isinstance(best_summary, dict):
        quality_score_payload = best_summary.get("quality_score_payload")
        if isinstance(quality_score_payload, dict):
            inputs = quality_score_payload.get("inputs")
            if isinstance(inputs, dict):
                try:
                    value = float(inputs.get("trades_per_month"))
                except (TypeError, ValueError):
                    value = None
                if value is not None and value >= 0:
                    return value
    trade_count = _attempt_trade_count(attempt)
    effective_window_months = _attempt_effective_window_months(attempt)
    if (
        trade_count is None
        or effective_window_months is None
        or effective_window_months <= 0
    ):
        return None
    return float(trade_count) / float(effective_window_months)


def _best_scored_attempts_by_run(
    attempts: list[dict[str, Any]],
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    scored = [
        attempt for attempt in attempts if attempt.get("composite_score") is not None
    ]
    best_by_run: dict[str, dict[str, Any]] = {}

    for attempt in scored:
        run_id = str(attempt.get("run_id", "")).strip()
        if not run_id:
            continue
        existing = best_by_run.get(run_id)
        if existing is None:
            best_by_run[run_id] = attempt
            continue
        left = float(attempt.get("composite_score"))
        right = float(existing.get("composite_score"))
        improved = left < right if lower_is_better else left > right
        if improved:
            best_by_run[run_id] = attempt

    return list(best_by_run.values())


def _compute_tradeoff_frontier(
    attempts: list[dict[str, Any]],
    *,
    lower_is_better: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted(
        attempts,
        key=lambda attempt: (
            float(attempt.get("trades_per_month", 0.0)),
            float(attempt.get("composite_score")),
        ),
        reverse=False,
    )
    frontier: list[dict[str, Any]] = []
    non_frontier: list[dict[str, Any]] = []
    best_seen_score: float | None = None
    for attempt in ordered:
        score = float(attempt.get("composite_score"))
        if best_seen_score is None:
            frontier.append(attempt)
            best_seen_score = score
            continue
        improved = (
            score < best_seen_score if lower_is_better else score > best_seen_score
        )
        if improved:
            frontier.append(attempt)
            best_seen_score = score
        else:
            non_frontier.append(attempt)
    return frontier, non_frontier


def _compute_tradeoff_envelope(
    attempts: list[dict[str, Any]],
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    if not attempts:
        return []

    distinct_trade_rates = sorted(
        {
            float(attempt.get("trades_per_month", 0.0))
            for attempt in attempts
            if float(attempt.get("trades_per_month", 0.0)) > 0
        }
    )
    if not distinct_trade_rates:
        return []

    def pick_best(candidates: list[dict[str, Any]]) -> dict[str, Any]:
        return sorted(
            candidates,
            key=lambda attempt: float(attempt.get("composite_score")),
            reverse=not lower_is_better,
        )[0]

    selected: list[dict[str, Any]] = []
    if len(distinct_trade_rates) <= 12:
        for trade_rate in distinct_trade_rates:
            bucket = [
                attempt
                for attempt in attempts
                if float(attempt.get("trades_per_month", 0.0)) == trade_rate
            ]
            if bucket:
                selected.append(pick_best(bucket))
    else:
        min_trade = min(distinct_trade_rates)
        max_trade = max(distinct_trade_rates)
        use_log_bins = max_trade >= max(8.0, min_trade * 6.0)
        bucket_count = min(12, max(6, int(len(attempts) ** 0.5) + 2))
        edges: list[float] = []
        if use_log_bins:
            log_min = float(math.log10(min_trade))
            log_max = float(math.log10(max_trade))
            step = (log_max - log_min) / bucket_count if bucket_count > 0 else 1.0
            edges = [
                10 ** (log_min + step * index) for index in range(bucket_count + 1)
            ]
        else:
            span = max_trade - min_trade
            step = span / bucket_count if bucket_count > 0 else 1.0
            edges = [min_trade + step * index for index in range(bucket_count + 1)]

        for index in range(len(edges) - 1):
            left = edges[index]
            right = edges[index + 1]
            if index == len(edges) - 2:
                bucket = [
                    attempt
                    for attempt in attempts
                    if left <= float(attempt.get("trades_per_month", 0.0)) <= right
                ]
            else:
                bucket = [
                    attempt
                    for attempt in attempts
                    if left <= float(attempt.get("trades_per_month", 0.0)) < right
                ]
            if bucket:
                selected.append(pick_best(bucket))

    envelope: list[dict[str, Any]] = []
    seen_attempt_ids: set[str] = set()
    for attempt in sorted(
        selected, key=lambda row: float(row.get("trades_per_month", 0.0))
    ):
        attempt_id = str(attempt.get("attempt_id", "")).strip()
        if attempt_id and attempt_id in seen_attempt_ids:
            continue
        if attempt_id:
            seen_attempt_ids.add(attempt_id)
        envelope.append(attempt)
    return envelope


def render_leaderboard_artifacts(
    attempts: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    run_metadata_by_run_id: dict[str, dict[str, Any]] | None = None,
    lower_is_better: bool = False,
    limit: int = 15,
) -> list[dict[str, Any]]:
    best_by_run = _best_scored_attempts_by_run(
        attempts, lower_is_better=lower_is_better
    )
    ranked = sorted(
        best_by_run,
        key=lambda attempt: float(attempt.get("composite_score")),
        reverse=not lower_is_better,
    )[:limit]

    enriched_ranked: list[dict[str, Any]] = []
    for attempt in ranked:
        run_id = str(attempt.get("run_id", "")).strip()
        run_metadata = (run_metadata_by_run_id or {}).get(run_id, {})
        enriched = dict(attempt)
        enriched["run_metadata"] = run_metadata
        enriched["leaderboard_label"] = _leaderboard_label(attempt, run_metadata)
        enriched_ranked.append(enriched)

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(enriched_ranked, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(16, max(6, min(14, len(enriched_ranked) * 0.65 + 2))))
    if not enriched_ranked:
        plt.title("Autoresearch Leaderboard: No Scored Runs Yet")
        plt.xlabel("Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return enriched_ranked

    labels = []
    scores = []
    for attempt in enriched_ranked:
        labels.append(str(attempt.get("leaderboard_label") or "run"))
        scores.append(float(attempt.get("composite_score")))

    positions = list(range(len(enriched_ranked)))
    colors = ["#2ecc71"] + ["#8ecae6"] * max(0, len(enriched_ranked) - 1)
    plt.barh(positions, scores, color=colors)
    plt.yticks(positions, labels, fontsize=8)
    plt.gca().invert_yaxis()
    plt.xlabel("Quality Score")
    plt.title(
        f"Autoresearch Leaderboard: Best Candidate Per Run ({len(enriched_ranked)} runs)"
    )

    for index, score in enumerate(scores):
        plt.text(score, index, f" {score:.3f}", va="center", fontsize=8)

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return enriched_ranked


def render_model_leaderboard_artifacts(
    attempts: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    run_metadata_by_run_id: dict[str, dict[str, Any]] | None = None,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for attempt in _best_scored_attempts_by_run(
        attempts, lower_is_better=lower_is_better
    ):
        run_id = str(attempt.get("run_id", "")).strip()
        label = _model_group_label((run_metadata_by_run_id or {}).get(run_id))
        if not label:
            label = "unknown"
        grouped.setdefault(label, []).append(attempt)

    summary_rows: list[dict[str, Any]] = []
    for label, model_attempts in grouped.items():
        scores = [float(attempt.get("composite_score")) for attempt in model_attempts]
        run_ids = sorted(
            {
                str(attempt.get("run_id", "")).strip()
                for attempt in model_attempts
                if str(attempt.get("run_id", "")).strip()
            }
        )
        summary_rows.append(
            {
                "model_label": label,
                "run_count": len(run_ids),
                "average_score": sum(scores) / len(scores),
                "median_score": median(scores),
                "best_score": min(scores) if lower_is_better else max(scores),
                "worst_score": max(scores) if lower_is_better else min(scores),
                "run_ids": run_ids,
            }
        )

    summary_rows.sort(
        key=lambda row: float(row["average_score"]),
        reverse=not lower_is_better,
    )

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(summary_rows, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(14, max(5, min(12, len(summary_rows) * 0.65 + 2))))
    if not summary_rows:
        plt.title("Autoresearch Model Averages: No Scored Runs Yet")
        plt.xlabel("Average Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return summary_rows

    positions = list(range(len(summary_rows)))
    scores = [float(row["average_score"]) for row in summary_rows]
    labels = [f"{row['model_label']} (n={row['run_count']})" for row in summary_rows]
    colors = ["#ffb703"] + ["#90e0ef"] * max(0, len(summary_rows) - 1)

    plt.barh(positions, scores, color=colors)
    plt.yticks(positions, labels, fontsize=9)
    plt.gca().invert_yaxis()
    plt.xlabel("Average Best-Per-Run Quality Score")
    plt.title("Autoresearch Model Averages: Mean Best Candidate Per Run")

    for index, row in enumerate(summary_rows):
        plt.text(
            float(row["average_score"]),
            index,
            f"  {float(row['average_score']):.3f} avg | {float(row['median_score']):.3f} med",
            va="center",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return summary_rows


def render_tradeoff_leaderboard_artifacts(
    attempts: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    run_metadata_by_run_id: dict[str, dict[str, Any]] | None = None,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    enriched_rows: list[dict[str, Any]] = []
    min_display_score = None if lower_is_better else 15.0
    for attempt in _best_scored_attempts_by_run(
        attempts, lower_is_better=lower_is_better
    ):
        trade_count = _attempt_trade_count(attempt)
        trades_per_month = _attempt_trades_per_month(attempt)
        effective_window_months = _attempt_effective_window_months(attempt)
        score = attempt.get("composite_score")
        if trade_count is None or trades_per_month is None or score is None:
            continue
        score_value = float(score)
        if min_display_score is not None and score_value < min_display_score:
            continue
        run_id = str(attempt.get("run_id", "")).strip()
        run_metadata = (run_metadata_by_run_id or {}).get(run_id, {})
        enriched = dict(attempt)
        enriched["run_metadata"] = run_metadata
        enriched["leaderboard_label"] = _leaderboard_label(attempt, run_metadata)
        enriched["trade_count"] = trade_count
        enriched["trades_per_month"] = trades_per_month
        enriched["effective_window_months"] = effective_window_months
        enriched_rows.append(enriched)

    frontier, non_frontier = _compute_tradeoff_frontier(
        enriched_rows,
        lower_is_better=lower_is_better,
    )
    envelope = _compute_tradeoff_envelope(
        enriched_rows,
        lower_is_better=lower_is_better,
    )
    frontier_run_ids = {
        str(attempt.get("run_id", "")).strip()
        for attempt in frontier
        if str(attempt.get("run_id", "")).strip()
    }
    envelope_attempt_ids = {
        str(attempt.get("attempt_id", "")).strip()
        for attempt in envelope
        if str(attempt.get("attempt_id", "")).strip()
    }

    serializable_rows: list[dict[str, Any]] = []
    for attempt in sorted(
        enriched_rows,
        key=lambda row: (
            float(row.get("trades_per_month", 0.0)),
            float(row.get("composite_score")),
        ),
    ):
        serialized = dict(attempt)
        serialized["is_frontier"] = (
            str(attempt.get("run_id", "")).strip() in frontier_run_ids
        )
        serialized["is_trade_envelope"] = (
            str(attempt.get("attempt_id", "")).strip() in envelope_attempt_ids
        )
        serializable_rows.append(serialized)

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(serializable_rows, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(14, 9))
    if not serializable_rows:
        plt.title(
            "Autoresearch Score vs Trade Rate: No Scored Runs With Trade Counts Yet"
        )
        plt.xlabel("Average Resolved Trades / Month")
        plt.ylabel("Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return serializable_rows

    x_all = [float(attempt["trades_per_month"]) for attempt in serializable_rows]
    y_all = [float(attempt["composite_score"]) for attempt in serializable_rows]
    plt.scatter(
        x_all,
        y_all,
        c="#8ecae6",
        edgecolors="#4d88a8",
        s=54,
        alpha=0.75,
        label="Best run candidate",
    )

    if envelope:
        envelope_sorted = sorted(
            envelope, key=lambda attempt: float(attempt.get("trades_per_month", 0.0))
        )
        x_front = [float(attempt["trades_per_month"]) for attempt in envelope_sorted]
        y_front = [float(attempt["composite_score"]) for attempt in envelope_sorted]
        plt.plot(
            x_front,
            y_front,
            color="#2a9d8f",
            linewidth=2.1,
            alpha=0.9,
            label="Upper envelope",
        )
        plt.scatter(
            x_front,
            y_front,
            c="#2ecc71",
            edgecolors="#1f6f54",
            s=86,
            zorder=3,
            label="Envelope point",
        )
        for attempt in envelope_sorted:
            label = str(attempt.get("leaderboard_label") or "run")
            if len(label) > 40:
                label = label[:37] + "..."
            plt.annotate(
                label,
                (float(attempt["trades_per_month"]), float(attempt["composite_score"])),
                textcoords="offset points",
                xytext=(8, 6),
                fontsize=8,
                color="#1f6f54",
            )

    if frontier:
        frontier_sorted = sorted(
            frontier, key=lambda attempt: float(attempt.get("trades_per_month", 0.0))
        )
        plt.scatter(
            [float(attempt["trades_per_month"]) for attempt in frontier_sorted],
            [float(attempt["composite_score"]) for attempt in frontier_sorted],
            facecolors="none",
            edgecolors="#146356",
            s=106,
            linewidths=1.1,
            zorder=4,
            label="Pareto point",
        )

    x_min = min(x_all)
    x_max = min(200.0, max(x_all))
    y_min = min(y_all)
    y_max = max(y_all)
    x_padding = max(0.15, (x_max - x_min) * 0.08) if x_max != x_min else 0.15
    plt.xlim(max(0.0, x_min - x_padding), x_max + x_padding)
    y_padding = max(0.5, (y_max - y_min) * 0.08) if y_max != y_min else 0.5
    if lower_is_better:
        plt.ylim(y_max + y_padding, y_min - y_padding)
    else:
        plt.ylim(y_min - y_padding, y_max + y_padding)

    direction = "lower is better" if lower_is_better else "higher is better"
    plt.xlabel("Average Resolved Trades / Month")
    plt.ylabel(f"Quality Score ({direction})")
    plt.title("Autoresearch Tradeoff Map: Best Score vs Trade Rate")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return serializable_rows


def render_validation_scatter_artifacts(
    rows: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    serializable_rows = [
        dict(row)
        for row in rows
        if row.get("score_12m") is not None and row.get("score_36m") is not None
    ]
    serializable_rows.sort(
        key=lambda row: float(row.get("score_36m", float("-inf"))),
        reverse=not lower_is_better,
    )

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(serializable_rows, ensure_ascii=True, indent=2), encoding="utf-8"
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(13, 9))
    if not serializable_rows:
        plt.title("Autoresearch Validation Map: No Validation Rows Yet")
        plt.xlabel("36m Quality Score")
        plt.ylabel("12m Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return serializable_rows

    x_all = [float(row["score_36m"]) for row in serializable_rows]
    y_all = [float(row["score_12m"]) for row in serializable_rows]
    sizes = []
    for row in serializable_rows:
        trade_rate = row.get("trades_per_month_36m")
        try:
            numeric = float(trade_rate)
        except (TypeError, ValueError):
            numeric = 0.0
        sizes.append(max(44.0, min(130.0, 42.0 + numeric * 1.2)))

    plt.scatter(
        x_all,
        y_all,
        s=sizes,
        c="#8ecae6",
        edgecolors="#376481",
        alpha=0.78,
    )

    diagonal_min = min(min(x_all), min(y_all))
    diagonal_max = max(max(x_all), max(y_all))
    plt.plot(
        [diagonal_min, diagonal_max],
        [diagonal_min, diagonal_max],
        color="#60d6c3",
        linewidth=1.8,
        linestyle="--",
        alpha=0.9,
        label="12m = 36m",
    )

    annotation_rows = (
        serializable_rows if len(serializable_rows) <= 30 else serializable_rows[:20]
    )
    for row in annotation_rows:
        label = str(row.get("leaderboard_label") or row.get("run_id") or "run")
        if len(label) > 34:
            label = label[:31] + "..."
        plt.annotate(
            label,
            (float(row["score_36m"]), float(row["score_12m"])),
            textcoords="offset points",
            xytext=(8, 6),
            fontsize=8,
            color="#cfe7ff",
        )

    x_min = min(x_all)
    x_max = max(x_all)
    y_min = min(y_all)
    y_max = max(y_all)
    x_padding = max(0.5, (x_max - x_min) * 0.08) if x_max != x_min else 0.5
    y_padding = max(0.5, (y_max - y_min) * 0.08) if y_max != y_min else 0.5
    if lower_is_better:
        plt.xlim(x_max + x_padding, x_min - x_padding)
        plt.ylim(y_max + y_padding, y_min - y_padding)
    else:
        plt.xlim(x_min - x_padding, x_max + x_padding)
        plt.ylim(y_min - y_padding, y_max + y_padding)

    plt.xlabel("36m Quality Score")
    plt.ylabel("12m Quality Score")
    plt.title("Autoresearch Validation Map: Short-Horizon Winner vs 3-Year Scrutiny")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return serializable_rows


def render_validation_delta_artifacts(
    rows: list[dict[str, Any]],
    png_output_path: Path,
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    serializable_rows = [
        dict(row)
        for row in rows
        if row.get("score_12m") is not None and row.get("score_36m") is not None
    ]
    for row in serializable_rows:
        score_12 = float(row["score_12m"])
        score_36 = float(row["score_36m"])
        row["score_delta"] = score_36 - score_12
        row["score_retention_ratio"] = (
            (score_36 / score_12) if score_12 not in {0.0, -0.0} else None
        )

    serializable_rows.sort(
        key=lambda row: float(row.get("score_delta", float("-inf"))),
        reverse=not lower_is_better,
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(15, max(6, min(14, len(serializable_rows) * 0.65 + 2))))
    if not serializable_rows:
        plt.title("Autoresearch Validation Delta: No Validation Rows Yet")
        plt.xlabel("36m - 12m Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return serializable_rows

    labels = []
    deltas = []
    colors = []
    for row in serializable_rows:
        label = str(row.get("leaderboard_label") or row.get("run_id") or "run")
        if len(label) > 42:
            label = label[:39] + "..."
        labels.append(label)
        delta = float(row["score_delta"])
        deltas.append(delta)
        colors.append("#60d6c3" if delta >= 0 else "#ff9a76")

    positions = list(range(len(serializable_rows)))
    plt.barh(positions, deltas, color=colors, alpha=0.9)
    plt.yticks(positions, labels, fontsize=8)
    plt.gca().invert_yaxis()
    plt.axvline(0.0, color="#d8e4ff", linewidth=1.0, alpha=0.7)
    plt.xlabel("36m - 12m Quality Score")
    plt.title(
        "Autoresearch Validation Delta: How Much the Leaders Survive 3-Year Scrutiny"
    )

    for index, row in enumerate(serializable_rows):
        delta = float(row["score_delta"])
        score_12 = float(row["score_12m"])
        score_36 = float(row["score_36m"])
        plt.text(
            delta,
            index,
            f"  {delta:+.2f} | 12m {score_12:.2f} -> 36m {score_36:.2f}",
            va="center",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return serializable_rows


def render_similarity_heatmap_artifacts(
    payload: dict[str, Any],
    png_output_path: Path,
    json_output_path: Path,
) -> dict[str, Any]:
    serializable_payload = {
        "leaders": list(payload.get("leaders") or []),
        "pairs": list(payload.get("pairs") or []),
        "matrix_labels": list(payload.get("matrix_labels") or []),
        "matrix_values": list(payload.get("matrix_values") or []),
    }
    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(
        json.dumps(serializable_payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    matrix_values = serializable_payload["matrix_values"]
    matrix_labels = serializable_payload["matrix_labels"]
    axis_size = min(30.0, max(7.0, len(matrix_labels) * 0.45))
    plt.figure(figsize=(axis_size, axis_size))
    if not matrix_values or not matrix_labels:
        plt.title("Autoresearch Similarity Heatmap: No Validated Curves Yet")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return serializable_payload

    image = plt.imshow(matrix_values, cmap="viridis", vmin=0.0, vmax=1.0)
    plt.colorbar(image, fraction=0.046, pad=0.04, label="Sameness score")
    ticks = list(range(len(matrix_labels)))
    font_size = max(4, min(8, int(round(10 - (len(matrix_labels) / 18)))))
    truncated = [
        label if len(label) <= 22 else label[:19] + "..." for label in matrix_labels
    ]
    plt.xticks(ticks, truncated, rotation=55, ha="right", fontsize=font_size)
    plt.yticks(ticks, truncated, fontsize=font_size)
    plt.title("Autoresearch Similarity Heatmap: 36m Realized-Return Sameness")
    plt.xlabel("Validated run leader")
    plt.ylabel("Validated run leader")

    if len(matrix_labels) <= 32:
        for row_index, row in enumerate(matrix_values):
            for col_index, value in enumerate(row):
                text_color = "#f5fbff" if float(value) < 0.62 else "#08111d"
                plt.text(
                    col_index,
                    row_index,
                    f"{float(value):.2f}",
                    ha="center",
                    va="center",
                    fontsize=max(5, font_size - 1),
                    color=text_color,
                )

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return serializable_payload


def render_similarity_scatter_artifacts(
    payload: dict[str, Any],
    png_output_path: Path,
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    leaders = [dict(row) for row in (payload.get("leaders") or [])]
    leaders = [row for row in leaders if row.get("score_36m") is not None]
    leaders.sort(
        key=lambda row: float(row.get("score_36m", float("-inf"))),
        reverse=not lower_is_better,
    )

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(13, 9))
    if not leaders:
        plt.title("Autoresearch Diversity Map: No Similarity Rows Yet")
        plt.xlabel("Closest-match sameness")
        plt.ylabel("36m Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return leaders

    x_all = [float(row.get("max_sameness", 0.0) or 0.0) for row in leaders]
    y_all = [float(row["score_36m"]) for row in leaders]
    sizes = []
    for row in leaders:
        trade_rate = row.get("trades_per_month_36m")
        try:
            numeric = float(trade_rate)
        except (TypeError, ValueError):
            numeric = 0.0
        sizes.append(max(48.0, min(136.0, 44.0 + numeric * 1.5)))

    plt.scatter(
        x_all,
        y_all,
        s=sizes,
        c="#ffba6d",
        edgecolors="#85501f",
        alpha=0.82,
    )

    median_sameness = median(x_all) if x_all else 0.0
    plt.axvline(
        median_sameness,
        color="#60d6c3",
        linewidth=1.6,
        linestyle="--",
        alpha=0.8,
        label=f"Median sameness {median_sameness:.2f}",
    )

    annotation_rows = leaders if len(leaders) <= 30 else leaders[:20]
    for row in annotation_rows:
        label = str(row.get("leaderboard_label") or row.get("run_id") or "run")
        if len(label) > 34:
            label = label[:31] + "..."
        plt.annotate(
            label,
            (float(row.get("max_sameness", 0.0) or 0.0), float(row["score_36m"])),
            textcoords="offset points",
            xytext=(8, 6),
            fontsize=8,
            color="#f5d9b7",
        )

    x_min = min(x_all)
    x_max = max(x_all)
    y_min = min(y_all)
    y_max = max(y_all)
    x_padding = max(0.03, (x_max - x_min) * 0.1) if x_max != x_min else 0.03
    y_padding = max(0.5, (y_max - y_min) * 0.08) if y_max != y_min else 0.5
    plt.xlim(max(0.0, x_min - x_padding), min(1.0, x_max + x_padding))
    if lower_is_better:
        plt.ylim(y_max + y_padding, y_min - y_padding)
    else:
        plt.ylim(y_min - y_padding, y_max + y_padding)

    plt.xlabel("Closest-match sameness (0 = distinct, 1 = highly similar)")
    plt.ylabel("36m Quality Score")
    plt.title(
        "Autoresearch Diversity Map: Long-Horizon Score vs Closest-Match Sameness"
    )
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return leaders
