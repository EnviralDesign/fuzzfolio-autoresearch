from __future__ import annotations

import csv

from autoresearch.discovery_cluster_atlas import (
    build_discovery_cluster_atlas,
    build_cluster_pair_rows,
    build_discovered_recipes,
    build_indicator_success_signatures,
    cluster_indicator_signatures,
    signature_similarity,
)


def _row(
    first: str,
    second: str,
    score: float,
    *,
    first_strategy: str = "mean-reversion",
    second_strategy: str = "breakout",
    timeframe: str = "M5",
    lane: str = "plausible_novel",
) -> dict[str, object]:
    return {
        "probe_id": f"dp-test-{first.lower()}-{second.lower()}-{timeframe.lower()}",
        "first_indicator_id": first,
        "second_indicator_id": second,
        "first_signal_role": "setup",
        "second_signal_role": "trigger",
        "first_strategy_role": first_strategy,
        "second_strategy_role": second_strategy,
        "first_namespace": "Test",
        "second_namespace": "Test",
        "probe_timeframe": timeframe,
        "discovery_lane": lane,
        "composite_score": str(score),
        "composite_score_number": score,
        "best_trades": "42",
        "best_expectancy_r": "0.1",
        "best_profit_factor": "1.2",
    }


def test_success_signatures_ignore_negative_partner_edges() -> None:
    rows = [
        _row("ANCHOR_A", "TRIGGER_X", 49.9),
        _row("ANCHOR_A", "TRIGGER_Y", 50.0),
    ]

    signatures = build_indicator_success_signatures(rows, side="first")

    assert signatures["ANCHOR_A"]["tested_count"] == 2
    assert signatures["ANCHOR_A"]["positive_count"] == 1
    assert "TRIGGER_X" not in signatures["ANCHOR_A"]["partner_vector"]
    assert "TRIGGER_Y" in signatures["ANCHOR_A"]["partner_vector"]


def test_cluster_indicator_signatures_group_shared_success_shapes() -> None:
    rows = [
        _row("ANCHOR_A", "TRIGGER_X", 72),
        _row("ANCHOR_A", "TRIGGER_Y", 60),
        _row("ANCHOR_B", "TRIGGER_X", 71),
        _row("ANCHOR_B", "TRIGGER_Y", 58),
        _row("ANCHOR_C", "TRIGGER_Z", 68, first_strategy="trend"),
    ]

    signatures = build_indicator_success_signatures(rows, side="first")
    similarity, shared = signature_similarity(signatures["ANCHOR_A"], signatures["ANCHOR_B"])
    clusters = cluster_indicator_signatures(
        signatures,
        side="first",
        min_similarity=0.25,
        min_shared_partners=1,
    )

    member_sets = [set(cluster["members"]) for cluster in clusters]
    assert shared == 2
    assert similarity > 0.5
    assert any({"ANCHOR_A", "ANCHOR_B"} <= members for members in member_sets)
    assert not any({"ANCHOR_A", "ANCHOR_C"} <= members for members in member_sets)


def test_cluster_pair_rows_emit_discovered_recipe_templates() -> None:
    rows = [
        _row("ANCHOR_A", "TRIGGER_X", 72),
        _row("ANCHOR_A", "TRIGGER_Y", 61),
        _row("ANCHOR_B", "TRIGGER_X", 73),
        _row("ANCHOR_B", "TRIGGER_Y", 62),
        _row("ANCHOR_C", "TRIGGER_Z", 48, first_strategy="trend"),
    ]
    first_signatures = build_indicator_success_signatures(rows, side="first")
    second_signatures = build_indicator_success_signatures(rows, side="second")
    first_clusters = cluster_indicator_signatures(
        first_signatures,
        side="first",
        min_similarity=0.25,
        min_shared_partners=1,
    )
    second_clusters = cluster_indicator_signatures(
        second_signatures,
        side="second",
        min_similarity=0.25,
        min_shared_partners=1,
    )

    cluster_pairs = build_cluster_pair_rows(
        rows,
        first_clusters=first_clusters,
        second_clusters=second_clusters,
    )
    recipes = build_discovered_recipes(
        cluster_pairs,
        first_clusters=first_clusters,
        second_clusters=second_clusters,
        max_recipes=4,
    )

    assert cluster_pairs[0]["positive_pair_count"] == 4
    assert cluster_pairs[0]["strong_pair_count"] == 2
    assert recipes
    assert recipes[0]["source"] == "discovery_pair_cluster_atlas"
    assert "ANCHOR_A" in recipes[0]["slots"]["context_or_setup_cluster"]["recommended_indicators"]
    assert "TRIGGER_X" in recipes[0]["slots"]["trigger_or_response_cluster"]["recommended_indicators"]


def test_build_discovery_cluster_atlas_reports_recipe_truncation(tmp_path) -> None:
    source_dir = tmp_path / "discovery-pair-atlas"
    source_dir.mkdir()
    queue_path = source_dir / "discovery-pair-queue.csv"
    results_path = source_dir / "discovery-pair-probe-results.csv"
    rows = [
        _row("ANCHOR_A", "TRIGGER_X", 72, first_strategy="mean-reversion", second_strategy="breakout"),
        _row("ANCHOR_B", "TRIGGER_Y", 73, first_strategy="trend", second_strategy="momentum"),
    ]
    with queue_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["probe_id"])
        writer.writeheader()
        writer.writerows({"probe_id": row["probe_id"]} for row in rows)
    with results_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    result = build_discovery_cluster_atlas(
        object(),
        discovery_pair_dir=source_dir,
        out_dir=tmp_path / "cluster-atlas",
        min_similarity=0.9,
        max_recipes=1,
    )

    counts = result.summary["result_counts"]
    assert counts["recipe_candidates_before_max"] == 2
    assert counts["discovered_recipes"] == 1
    assert counts["recipes_truncated_by_max"] == 1
    assert result.summary["cluster_shape"]["first"]["cluster_count"] == 2
    assert result.summary["cluster_shape"]["second"]["cluster_count"] == 2
