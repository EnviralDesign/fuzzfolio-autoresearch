from __future__ import annotations

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_evidence_ladder import (
    build_evidence_ladder,
    validate_template_discovery_windows,
)


def _input() -> dict:
    return {
        "schemaVersion": "temporal_qd_evidence_ladder_input_v1",
        "frozenSeed": "ladder-fixture-seed",
        "historicalMonthStarts": [
            "2020-01-01T00:00:00Z",
            "2020-03-01T00:00:00Z",
            "2020-06-01T00:00:00Z",
            "2020-09-01T00:00:00Z",
        ],
        "validationWindow": {
            "analysisWindowStart": "2021-01-01T00:00:00Z",
            "analysisWindowEnd": "2022-01-01T00:00:00Z",
        },
        "scrutinyWindow": {
            "analysisWindowStart": "2016-01-01T00:00:00Z",
            "analysisWindowEnd": "2019-01-01T00:00:00Z",
        },
    }


def test_evidence_ladder_is_seed_deterministic_disjoint_and_outer_tail_free() -> None:
    first = build_evidence_ladder(_input())
    reversed_input = _input()
    reversed_input["historicalMonthStarts"].reverse()
    second = build_evidence_ladder(reversed_input)
    assert first == second
    assert first["discovery"]["windowCount"] == 3
    assert first["discovery"]["totalMonths"] == 3
    assert first["validation"]["maxDiverseSurvivorCount"] == 128
    assert first["scrutiny"]["maxFinalistCount"] == 32
    assert first["outerTail"] == {
        "analysisWindowStart": "2025-08-01T00:00:00Z",
        "selectionInput": False,
        "touched": False,
    }
    template = {"developmentWindows": [{"windowId": f"w{index}", **window} for index, window in enumerate(first["discovery"]["windows"])]}
    validate_template_discovery_windows(template, first)


def test_evidence_ladder_fails_closed_when_template_or_outer_tail_would_leak() -> None:
    ladder = build_evidence_ladder(_input())
    wrong = {"developmentWindows": []}
    with pytest.raises(TemporalDiscoveryContractError, match="exactly bind"):
        validate_template_discovery_windows(wrong, ladder)
    leaked = _input()
    leaked["validationWindow"] = {
        "analysisWindowStart": "2025-08-01T00:00:00Z",
        "analysisWindowEnd": "2026-08-01T00:00:00Z",
    }
    with pytest.raises(TemporalDiscoveryContractError, match="outer tail"):
        build_evidence_ladder(leaked)
