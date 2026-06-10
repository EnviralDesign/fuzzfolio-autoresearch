import json
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import autoresearch.play_hand as play_hand_mod
from autoresearch.calendar_robustness import (
    GATE_REASON_CURVE_INSUFFICIENT,
    GATE_REASON_RECENCY,
    GATE_REASON_SEGMENTS,
    compute_calendar_robustness,
    evaluate_calendar_gate,
)


def _curve(
    start: date,
    days: int,
    value_for_day: Callable[[int, int], float],
) -> list[dict]:
    return [
        {
            "date": (start + timedelta(days=index)).isoformat(),
            "realized_r": float(value_for_day(index, days)),
        }
        for index in range(days)
    ]


def test_profit_concentrated_in_last_third_fails_gate() -> None:
    points = _curve(
        date(2023, 1, 1),
        360,
        lambda index, days: 0.2 if index >= (2 * days) // 3 else 0.0,
    )

    robustness = compute_calendar_robustness(points)

    assert robustness.sufficient is True
    assert robustness.recent_third_share is not None
    assert robustness.recent_third_share > 0.9

    decision = evaluate_calendar_gate(robustness)

    assert decision.passed is False
    assert GATE_REASON_RECENCY in decision.reasons


def test_evenly_distributed_profit_passes_gate() -> None:
    points = _curve(date(2023, 1, 1), 360, lambda _index, _days: 0.05)

    robustness = compute_calendar_robustness(points)

    assert robustness.sufficient is True
    assert len(robustness.segment_r) == 4
    assert robustness.segments_positive == 4

    decision = evaluate_calendar_gate(robustness)

    assert decision.passed is True
    assert decision.reasons == []
    assert decision.metrics["segments_positive"] == 4


def test_negative_total_r_skips_recency_but_fails_on_segments() -> None:
    points = _curve(
        date(2023, 1, 1),
        360,
        lambda index, days: 0.01 if index >= (3 * days) // 4 else -0.05,
    )

    robustness = compute_calendar_robustness(points)

    assert robustness.total_r < 0
    assert robustness.recent_third_share is None
    assert len(robustness.segment_r) == 4
    assert robustness.segments_positive == 1

    decision = evaluate_calendar_gate(robustness)

    assert decision.passed is False
    assert GATE_REASON_SEGMENTS in decision.reasons
    assert GATE_REASON_RECENCY not in decision.reasons


def test_short_curve_is_insufficient_and_passes_with_warning() -> None:
    points = _curve(date(2026, 1, 1), 30, lambda _index, _days: 0.1)

    robustness = compute_calendar_robustness(points)

    assert robustness.sufficient is False
    assert robustness.point_count == 30

    decision = evaluate_calendar_gate(robustness)

    assert decision.passed is True
    assert decision.reasons == [GATE_REASON_CURVE_INSUFFICIENT]


def test_empty_curve_is_insufficient() -> None:
    robustness = compute_calendar_robustness([])

    assert robustness.sufficient is False
    assert robustness.point_count == 0
    assert robustness.segment_r == []
    assert robustness.recent_third_share is None

    decision = evaluate_calendar_gate(robustness)

    assert decision.passed is True
    assert decision.reasons == [GATE_REASON_CURVE_INSUFFICIENT]


def _write_best_cell_detail(artifact_dir: Path, points: list[dict]) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "best-cell-path-detail.json").write_text(
        json.dumps({"curve": {"points": points}}, ensure_ascii=True),
        encoding="utf-8",
    )


def test_enforce_mode_selects_alternate_branch_when_selected_fails_gate(
    tmp_path: Path,
) -> None:
    start = date(2023, 1, 1)
    concentrated = _curve(
        start, 360, lambda index, days: 0.2 if index >= (2 * days) // 3 else 0.0
    )
    even = _curve(start, 360, lambda _index, _days: 0.05)
    mutated_dir = tmp_path / "eval_mutated_final_36mo"
    template_dir = tmp_path / "eval_exact_template_36mo"
    _write_best_cell_detail(mutated_dir, concentrated)
    _write_best_cell_detail(template_dir, even)

    branches = [
        {
            "branch": "mutated",
            "outcome": {"passed": True, "score": 70.0},
            "scrutiny": {"artifact_dir": str(mutated_dir)},
        },
        {
            "branch": "exact_template",
            "outcome": {"passed": True, "score": 61.5},
            "scrutiny": {"artifact_dir": str(template_dir)},
        },
    ]
    for branch in branches:
        branch["calendar_gate"] = play_hand_mod._branch_calendar_gate(
            branch, mode="enforce"
        )

    assert branches[0]["calendar_gate"]["passed"] is False
    assert branches[1]["calendar_gate"]["passed"] is True

    relaxed = play_hand_mod._select_final_scrutiny_branch(branches)
    assert relaxed["branch"] == "mutated"

    enforced = play_hand_mod._select_final_scrutiny_branch(
        branches, enforce_calendar_gate=True
    )
    assert enforced["branch"] == "exact_template"


def test_resolve_calendar_gate_mode_env_overrides_cli(monkeypatch) -> None:
    monkeypatch.delenv(play_hand_mod.PLAY_HAND_CALENDAR_GATE_ENV, raising=False)
    assert play_hand_mod._resolve_calendar_gate_mode(None) == "report"
    assert play_hand_mod._resolve_calendar_gate_mode("enforce") == "enforce"
    assert play_hand_mod._resolve_calendar_gate_mode("bogus") == "report"

    monkeypatch.setenv(play_hand_mod.PLAY_HAND_CALENDAR_GATE_ENV, "off")
    assert play_hand_mod._resolve_calendar_gate_mode("enforce") == "off"

    monkeypatch.setenv(play_hand_mod.PLAY_HAND_CALENDAR_GATE_ENV, "nonsense")
    assert play_hand_mod._resolve_calendar_gate_mode("enforce") == "enforce"
