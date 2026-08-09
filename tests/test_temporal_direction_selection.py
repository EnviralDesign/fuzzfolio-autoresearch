from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_direction_selection import (
    DIRECTION_SELECTION_SCHEMA,
    DirectionSelectionPolicyV1,
    LANE_BALANCED_BIDIRECTIONAL,
    LANE_HARMFUL_OPPOSITE_SIDE,
    LANE_INACTIVE_OR_UNSUPPORTED,
    LANE_LONG_SPECIALIST,
    LANE_SHORT_SPECIALIST,
    classify_direction_selection,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_realized_behavior import REALIZED_BEHAVIOR_SCHEMA


def _behavior(
    *,
    long_net: float = 0.0,
    short_net: float = 0.0,
    long_trades: int = 0,
    short_trades: int = 0,
    long_windows: int = 0,
    short_windows: int = 0,
    windows: int = 4,
) -> dict:
    def side(net: float, trades: int, active_windows: int) -> dict:
        gross = net + (0.2 if trades else 0.0)
        return {
            "closedTrades": trades,
            "activeWindowCount": active_windows,
            "activeWindowFraction": active_windows / windows,
            "grossR": gross,
            "netR": net,
            "costR": gross - net,
            "active": bool(trades),
            "terminalDirectionCount": 0,
        }

    material = {
        "schemaVersion": REALIZED_BEHAVIOR_SCHEMA,
        "windowCount": windows,
        "sides": {
            "long": side(long_net, long_trades, long_windows),
            "short": side(short_net, short_trades, short_windows),
        },
    }
    material["identitySha256"] = canonical_sha256(material)
    return material


def _swapped(behavior: dict) -> dict:
    result = copy.deepcopy(behavior)
    result["sides"]["long"], result["sides"]["short"] = (
        result["sides"]["short"], result["sides"]["long"],
    )
    material = {key: value for key, value in result.items() if key != "identitySha256"}
    result["identitySha256"] = canonical_sha256(material)
    return result


def test_balanced_and_specialist_lanes_are_explicit_and_cost_inclusive() -> None:
    balanced = classify_direction_selection(
        _behavior(long_net=2.0, short_net=1.5, long_trades=3, short_trades=2, long_windows=2, short_windows=2)
    )
    assert balanced["schemaVersion"] == DIRECTION_SELECTION_SCHEMA
    assert balanced["lane"] == LANE_BALANCED_BIDIRECTIONAL
    assert balanced["selectionEligible"] is True
    assert balanced["rankingKey"]["combinedNetR"] == pytest.approx(3.5)
    assert balanced["rankingKey"]["combinedCostR"] == pytest.approx(0.4)

    long = classify_direction_selection(
        _behavior(long_net=2.0, long_trades=3, long_windows=2)
    )
    assert long["lane"] == LANE_LONG_SPECIALIST
    assert long["selectionEligible"] is True
    assert long["specialistSide"] == "long"


def test_profitable_long_cannot_hide_a_materially_harmful_short() -> None:
    result = classify_direction_selection(
        _behavior(long_net=4.0, short_net=-1.5, long_trades=4, short_trades=3, long_windows=3, short_windows=2)
    )
    assert result["lane"] == LANE_HARMFUL_OPPOSITE_SIDE
    assert result["selectionEligible"] is False
    assert result["sides"]["long"]["acceptable"] is True
    assert result["sides"]["short"]["materiallyHarmful"] is True


def test_inactive_and_unsupported_sides_are_not_silently_balanced() -> None:
    inactive = classify_direction_selection(_behavior())
    assert inactive["lane"] == LANE_INACTIVE_OR_UNSUPPORTED
    assert inactive["selectionEligible"] is False

    weak = classify_direction_selection(
        _behavior(long_net=2.0, short_net=-0.1, long_trades=2, short_trades=2, long_windows=2, short_windows=2)
    )
    assert weak["lane"] == LANE_INACTIVE_OR_UNSUPPORTED
    assert weak["selectionEligible"] is False


def test_mirrored_sides_preserve_mirror_identity_and_rank_but_name_the_specialist() -> None:
    long = classify_direction_selection(_behavior(long_net=2.0, long_trades=3, long_windows=2))
    short = classify_direction_selection(_swapped(_behavior(long_net=2.0, long_trades=3, long_windows=2)))
    assert long["lane"] == LANE_LONG_SPECIALIST
    assert short["lane"] == LANE_SHORT_SPECIALIST
    assert long["mirrorIdentitySha256"] == short["mirrorIdentitySha256"]
    assert long["rankingKey"] == short["rankingKey"]
    assert long["identitySha256"] != short["identitySha256"]


def test_policy_support_contract_is_immutable_and_deterministic() -> None:
    policy = DirectionSelectionPolicyV1(
        minimum_closed_trades_per_side=2,
        minimum_active_windows_per_side=2,
    )
    behavior = _behavior(long_net=4.0, long_trades=1, long_windows=1)
    first = classify_direction_selection(behavior, policy=policy)
    second = classify_direction_selection(copy.deepcopy(behavior), policy=policy)
    assert first["lane"] == LANE_INACTIVE_OR_UNSUPPORTED
    assert first["identitySha256"] == second["identitySha256"]
    assert first["policyIdentitySha256"] == policy.identity_sha256
    with pytest.raises(TemporalDiscoveryContractError, match="at least one"):
        DirectionSelectionPolicyV1(minimum_closed_trades_per_side=0)


def test_malformed_or_non_cost_inclusive_behavior_fails_closed() -> None:
    bad_schema = _behavior(long_net=1.0, long_trades=1, long_windows=1)
    bad_schema["schemaVersion"] = "wrong"
    with pytest.raises(TemporalDiscoveryContractError, match="schema"):
        classify_direction_selection(bad_schema)

    bad_cost = _behavior(long_net=1.0, long_trades=1, long_windows=1)
    bad_cost["sides"]["long"]["costR"] = 99.0
    with pytest.raises(TemporalDiscoveryContractError, match="does not reconcile"):
        classify_direction_selection(bad_cost)

    missing_identity = _behavior(long_net=1.0, long_trades=1, long_windows=1)
    missing_identity.pop("identitySha256")
    with pytest.raises(TemporalDiscoveryContractError, match="exact sha256"):
        classify_direction_selection(missing_identity)
