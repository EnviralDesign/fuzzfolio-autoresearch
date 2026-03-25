from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


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
