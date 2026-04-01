"""Tests for artifact directory resolution status."""

from __future__ import annotations

import json
from pathlib import Path

from autoresearch.artifact_resolution import (
    RESOLUTION_ARTIFACTS_INCOMPLETE,
    RESOLUTION_ATTEMPT_MISMATCH,
    RESOLUTION_FULLY_SCORED,
    RESOLUTION_READY_TO_SCORE,
    artifact_resolution_status,
)


def test_incomplete_missing_response(tmp_path: Path) -> None:
    d = tmp_path / "eval1"
    d.mkdir()
    st = artifact_resolution_status(d)
    assert st["resolution"] == RESOLUTION_ARTIFACTS_INCOMPLETE


def test_ready_to_score_no_cli(tmp_path: Path) -> None:
    d = tmp_path / "eval2"
    d.mkdir()
    (d / "sensitivity-response.json").write_text(
        json.dumps({"ok": True, "data": {}}), encoding="utf-8"
    )
    st = artifact_resolution_status(d, score_artifact=None)
    assert st["resolution"] == RESOLUTION_READY_TO_SCORE


def test_fully_scored_with_mock_scorer(tmp_path: Path) -> None:
    d = tmp_path / "eval3"
    d.mkdir()
    (d / "sensitivity-response.json").write_text("{}", encoding="utf-8")

    def score_artifact(_p: Path) -> dict:
        return {"best": {"quality_score": 55.5, "market_data_window": {}}}

    st = artifact_resolution_status(d, score_artifact=score_artifact)
    assert st["resolution"] == RESOLUTION_FULLY_SCORED
    assert st.get("quality_score") == 55.5


def test_attempt_mismatch(tmp_path: Path) -> None:
    d = tmp_path / "eval4"
    d.mkdir()
    (d / "sensitivity-response.json").write_text("{}", encoding="utf-8")
    other = tmp_path / "other"
    other.mkdir()
    st = artifact_resolution_status(
        d,
        expected_attempt_id="a1",
        ledger_artifact_dir=str(other),
    )
    assert st["resolution"] == RESOLUTION_ATTEMPT_MISMATCH
    assert st["attempt_mismatch"] is True
