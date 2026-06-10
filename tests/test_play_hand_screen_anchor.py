"""Tests for the randomized screen anchor used by play-hand discovery phases."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from autoresearch.play_hand import (
    AVERAGE_DAYS_PER_MONTH,
    PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MODE,
    PLAY_HAND_SCREEN_ANCHOR_ENV,
    PLAY_HAND_SCREEN_ANCHOR_OFFSET_BUDGET_MONTHS,
    _resolve_screen_anchor_mode,
    sample_screen_anchor,
)

NOW = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)


def test_now_mode_never_anchors():
    anchor = sample_screen_anchor(mode="now", screen_months=3, seed=42, now=NOW)
    assert anchor["mode"] == "now"
    assert anchor["as_of_date"] is None
    assert anchor["offset_days"] == 0


def test_random_mode_is_reproducible_for_seed():
    first = sample_screen_anchor(mode="random", screen_months=3, seed=42, now=NOW)
    second = sample_screen_anchor(mode="random", screen_months=3, seed=42, now=NOW)
    assert first == second


def test_random_mode_varies_across_seeds():
    anchors = {
        sample_screen_anchor(mode="random", screen_months=3, seed=seed, now=NOW)["offset_days"]
        for seed in range(20)
    }
    assert len(anchors) > 1


def test_random_offset_stays_within_effective_budget():
    for seed in range(50):
        anchor = sample_screen_anchor(
            mode="random", screen_months=3, max_offset_months=24, seed=seed, now=NOW
        )
        assert anchor["effective_max_offset_months"] == 24
        assert 0 <= anchor["offset_days"] <= 24 * AVERAGE_DAYS_PER_MONTH
        if anchor["offset_days"] > 0:
            as_of = datetime.fromisoformat(anchor["as_of_date"]).replace(tzinfo=timezone.utc)
            assert as_of < NOW
            assert NOW - as_of <= timedelta(days=24 * AVERAGE_DAYS_PER_MONTH + 1)


def test_effective_max_clamped_by_lake_budget():
    anchor = sample_screen_anchor(
        mode="random", screen_months=12, max_offset_months=24, seed=1, now=NOW
    )
    expected = PLAY_HAND_SCREEN_ANCHOR_OFFSET_BUDGET_MONTHS - 12
    assert anchor["requested_max_offset_months"] == 24
    assert anchor["effective_max_offset_months"] == expected
    assert anchor["offset_days"] <= expected * AVERAGE_DAYS_PER_MONTH


def test_zero_budget_falls_back_to_now_anchor():
    anchor = sample_screen_anchor(
        mode="random",
        screen_months=PLAY_HAND_SCREEN_ANCHOR_OFFSET_BUDGET_MONTHS,
        max_offset_months=24,
        seed=7,
        now=NOW,
    )
    assert anchor["effective_max_offset_months"] == 0
    assert anchor["as_of_date"] is None
    assert anchor["offset_days"] == 0


def test_as_of_date_is_bare_iso_date():
    anchor = sample_screen_anchor(mode="random", screen_months=3, seed=3, now=NOW)
    if anchor["as_of_date"] is not None:
        assert len(anchor["as_of_date"]) == 10
        datetime.strptime(anchor["as_of_date"], "%Y-%m-%d")


def test_resolve_mode_defaults_and_cli(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(PLAY_HAND_SCREEN_ANCHOR_ENV, raising=False)
    assert _resolve_screen_anchor_mode(None) == PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MODE
    assert _resolve_screen_anchor_mode("random") == "random"
    assert _resolve_screen_anchor_mode("bogus") == PLAY_HAND_SCREEN_ANCHOR_DEFAULT_MODE


def test_resolve_mode_env_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(PLAY_HAND_SCREEN_ANCHOR_ENV, "random")
    assert _resolve_screen_anchor_mode("now") == "random"
    monkeypatch.setenv(PLAY_HAND_SCREEN_ANCHOR_ENV, "not-a-mode")
    assert _resolve_screen_anchor_mode("random") == "random"
