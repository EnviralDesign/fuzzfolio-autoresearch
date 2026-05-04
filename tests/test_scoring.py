from autoresearch.scoring import build_attempt_score


def test_compact_sensitivity_score_lab_is_canonical_score() -> None:
    payload = {
        "best": {
            "quality_score": {"score": 88.0, "version": "v1", "belief_basis": "psr"},
            "score_lab": {
                "version": "score_lab_v2_5_2",
                "score": 64.5,
                "combiner": "geometric_mean",
            },
            "best_cell_path_metrics": {"psr": 0.91},
        }
    }

    score = build_attempt_score(payload, {"data": {"aggregate": payload["best"]}})

    assert score.primary_score == 64.5
    assert score.composite_score == 64.5
    assert score.score_basis == "score_lab_v2_5_2:geometric_mean"
    assert score.metrics["score_lab"] == 64.5
    assert score.metrics["legacy_quality_score"] == 88.0


def test_legacy_quality_score_without_score_lab_is_diagnostic_only() -> None:
    payload = {
        "best": {
            "quality_score": {"score": 88.0, "version": "v1", "belief_basis": "psr"},
            "best_cell_path_metrics": {"psr": 0.91},
        }
    }

    score = build_attempt_score(payload, {"data": {"aggregate": payload["best"]}})

    assert score.primary_score is None
    assert score.composite_score is None
    assert score.score_basis == "missing_score_lab_v2_5_2:v1:psr"
    assert score.metrics["score_lab"] is None
    assert score.metrics["legacy_quality_score"] == 88.0
