from __future__ import annotations

import csv
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
        improved = best_score is None or (score < best_score if lower_is_better else score > best_score)
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
        if supervisor_profile and supervisor_model and supervisor_profile != supervisor_model:
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
    valid = [attempt for attempt in attempts if attempt.get("composite_score") is not None]
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
    plt.scatter(x_front, y_front, c="#2ecc71", edgecolors="#2d6a4f", s=46, zorder=3, label="Frontier")

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
    candidate_name = str(attempt.get("candidate_name", "candidate")).strip() or "candidate"
    return prefix + candidate_name


def _progress_index_rows(attempts: list[dict[str, Any]], run_metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for attempt in attempts:
        rows.append(
            {
                "sequence": attempt.get("sequence"),
                "attempt_id": attempt.get("attempt_id"),
                "plot_label": _attempt_plot_label(attempt),
                "candidate_name": attempt.get("candidate_name"),
                "profile_file": Path(str(attempt["profile_path"])).name if attempt.get("profile_path") else None,
                "profile_path": attempt.get("profile_path"),
                "profile_ref": attempt.get("profile_ref"),
                "composite_score": attempt.get("composite_score"),
                "score_basis": attempt.get("score_basis"),
                "artifact_dir": attempt.get("artifact_dir"),
                "explorer_profile": run_metadata.get("explorer_profile") if isinstance(run_metadata, dict) else None,
                "explorer_model": run_metadata.get("explorer_model") if isinstance(run_metadata, dict) else None,
                "supervisor_profile": run_metadata.get("supervisor_profile") if isinstance(run_metadata, dict) else None,
                "supervisor_model": run_metadata.get("supervisor_model") if isinstance(run_metadata, dict) else None,
                "quality_score_preset": run_metadata.get("quality_score_preset") if isinstance(run_metadata, dict) else None,
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
    output_path.write_text(json.dumps(rows, ensure_ascii=True, indent=2), encoding="utf-8")

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
    run_metadata = load_run_metadata(run_metadata_path.parent) if run_metadata_path and run_metadata_path.exists() else None
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
        source_index = primary_output_path.with_name(f"{primary_output_path.stem}-index.json")
        target_index = mirror_output_path.with_name(f"{mirror_output_path.stem}-index.json")
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


def _leaderboard_label(attempt: dict[str, Any], run_metadata: dict[str, Any] | None) -> str:
    run_id = str(attempt.get("run_id", "run")).strip() or "run"
    candidate_name = str(attempt.get("candidate_name", "candidate")).strip() or "candidate"
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


def _best_scored_attempts_by_run(
    attempts: list[dict[str, Any]],
    *,
    lower_is_better: bool = False,
) -> list[dict[str, Any]]:
    scored = [attempt for attempt in attempts if attempt.get("composite_score") is not None]
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


def render_leaderboard_artifacts(
    attempts: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    run_metadata_by_run_id: dict[str, dict[str, Any]] | None = None,
    lower_is_better: bool = False,
    limit: int = 15,
) -> list[dict[str, Any]]:
    best_by_run = _best_scored_attempts_by_run(attempts, lower_is_better=lower_is_better)
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
    json_output_path.write_text(json.dumps(enriched_ranked, ensure_ascii=True, indent=2), encoding="utf-8")

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
    plt.title(f"Autoresearch Leaderboard: Best Candidate Per Run ({len(enriched_ranked)} runs)")

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
    for attempt in _best_scored_attempts_by_run(attempts, lower_is_better=lower_is_better):
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
    json_output_path.write_text(json.dumps(summary_rows, ensure_ascii=True, indent=2), encoding="utf-8")

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
    labels = [f'{row["model_label"]} (n={row["run_count"]})' for row in summary_rows]
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
            f'  {float(row["average_score"]):.3f} avg | {float(row["median_score"]):.3f} med',
            va="center",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return summary_rows
