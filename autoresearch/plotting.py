from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import json


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


def render_progress_plot(
    attempts: list[dict[str, Any]],
    output_path: Path,
    *,
    lower_is_better: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid = [attempt for attempt in attempts if attempt.get("composite_score") is not None]

    plt.figure(figsize=(16, 8))
    if not valid:
        plt.title("Autoresearch Progress: No Attempts Yet")
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
        label = str(attempt.get("candidate_name", "candidate"))
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
    plt.title(f"Autoresearch Progress: {len(valid)} Attempts, {len(frontier)} Frontier Points")
    plt.xlabel("Attempt #")
    plt.ylabel(f"Quality Score ({direction})")
    plt.grid(True, alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def render_progress_artifacts(
    attempts: list[dict[str, Any]],
    primary_output_path: Path,
    *,
    lower_is_better: bool = False,
    mirror_output_path: Path | None = None,
    mirror_attempts: list[dict[str, Any]] | None = None,
) -> None:
    render_progress_plot(
        attempts,
        primary_output_path,
        lower_is_better=lower_is_better,
    )
    if mirror_output_path is None:
        return
    mirror_output_path.parent.mkdir(parents=True, exist_ok=True)
    if mirror_attempts is None:
        shutil.copy2(primary_output_path, mirror_output_path)
        return
    render_progress_plot(
        mirror_attempts,
        mirror_output_path,
        lower_is_better=lower_is_better,
    )


def render_leaderboard_artifacts(
    attempts: list[dict[str, Any]],
    png_output_path: Path,
    json_output_path: Path,
    *,
    lower_is_better: bool = False,
    limit: int = 15,
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

    ranked = sorted(
        best_by_run.values(),
        key=lambda attempt: float(attempt.get("composite_score")),
        reverse=not lower_is_better,
    )[:limit]

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(json.dumps(ranked, ensure_ascii=True, indent=2), encoding="utf-8")

    png_output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(16, max(6, min(14, len(ranked) * 0.65 + 2))))
    if not ranked:
        plt.title("Autoresearch Leaderboard: No Scored Runs Yet")
        plt.xlabel("Quality Score")
        plt.tight_layout()
        plt.savefig(png_output_path, dpi=160)
        plt.close()
        return ranked

    labels = []
    scores = []
    for attempt in ranked:
        run_id = str(attempt.get("run_id", "run"))
        label = f"{run_id} | {attempt.get('candidate_name', 'candidate')}"
        if len(label) > 72:
            label = label[:69] + "..."
        labels.append(label)
        scores.append(float(attempt.get("composite_score")))

    positions = list(range(len(ranked)))
    colors = ["#2ecc71"] + ["#8ecae6"] * max(0, len(ranked) - 1)
    plt.barh(positions, scores, color=colors)
    plt.yticks(positions, labels, fontsize=8)
    plt.gca().invert_yaxis()
    plt.xlabel("Quality Score")
    plt.title(f"Autoresearch Leaderboard: Best Candidate Per Run ({len(ranked)} runs)")

    for index, score in enumerate(scores):
        plt.text(score, index, f" {score:.3f}", va="center", fontsize=8)

    plt.tight_layout()
    plt.savefig(png_output_path, dpi=160)
    plt.close()
    return ranked
