from __future__ import annotations

from pathlib import Path

from autoresearch.playhand_outcome_priors import build_playhand_outcome_prior_artifacts


def _write_csv(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def test_build_playhand_outcome_priors_classifies_template_locked_family(tmp_path: Path) -> None:
    report_dir = tmp_path / "playhand-prior-test-clean-100"
    _write_csv(
        report_dir / "recipe-performance-pairs.csv",
        """
template_branch_source_probe_id,dealt_pair_probe_id,dealt_recipe,dealt_pair_source,count,promoted,tombstoned,promotion_rate,exact_selected,mutated_selected,exact_rescues,exact_outscored_mutated,comparable_template_runs,mutated_wins_over_exact,exact_rescue_rate,exact_selected_rate,mutated_win_rate,avg_mutation_delta,median_mutation_delta,family_classification,avg_score,avg_positive_score,best_score,best_seed
drs-0002-r006-rsi-crossback-willr-mean-reversi-m5,drs-0002-r006-rsi-crossback-willr-mean-reversi-m5,discovered_recipe_006,discovery_recipe_validation,15,15,0,1.0,13,2,10,3,15,2,0.6667,0.8667,0.1333,-44.3961,-64.9935,template_locked,66.2509,66.2509,73.2977,114
""",
    )
    _write_csv(
        report_dir / "recipe-performance-recipes.csv",
        """
dealt_recipe,dealt_recipe_source,count,promoted,tombstoned,promotion_rate,exact_selected,mutated_selected,exact_rescues,exact_outscored_mutated,comparable_template_runs,mutated_wins_over_exact,exact_rescue_rate,exact_selected_rate,mutated_win_rate,avg_mutation_delta,median_mutation_delta,family_classification,avg_score,avg_positive_score,best_score,best_seed
discovered_recipe_006,discovery_recipe_validation,27,27,0,1.0,20,7,14,6,27,7,0.5185,0.7407,0.2593,-34.5689,-62.3366,template_locked,66.5509,66.5509,75.3234,76
unknown,unknown,28,15,13,0.5357,0,28,0,0,0,0,0.0,0.0,,,,unstable,31.9939,59.722,75.8862,97
""",
    )

    payload, pair_rows, recipe_rows, summary = build_playhand_outcome_prior_artifacts(
        report_dirs=[report_dir]
    )

    row = payload["pair_families"]["drs-0002-r006-rsi-crossback-willr-mean-reversi-m5"]
    assert row["family_policy"] == "template_locked"
    assert row["recommended_max_indicators"] == 2
    assert row["role_balanced_fill_limit"] == 0
    assert row["sampling_weight_multiplier"] == 1.15
    assert summary["result_counts"]["template_locked_pair_families"] == 1
    assert pair_rows[0]["family_id"] == "drs-0002-r006-rsi-crossback-willr-mean-reversi-m5"
    assert len(recipe_rows) == 1
    assert recipe_rows[0]["recipe"] == "discovered_recipe_006"
