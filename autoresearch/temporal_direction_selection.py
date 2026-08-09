"""Read-only, direction-aware admission lanes for temporal research.

This is deliberately a policy *projection*, not selection code.  The existing
quality archive remains authoritative until a future campaign explicitly binds
this versioned policy.  Keeping the projection separate lets callers see when
aggregate profit comes from a usable bidirectional profile, a consciously
specialist profile, or one side hiding a harmful/opportunistic opposite side.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import math
from typing import Any, Final

from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_realized_behavior import REALIZED_BEHAVIOR_SCHEMA


DIRECTION_SELECTION_POLICY_SCHEMA: Final = "temporal_direction_selection_policy_v1"
DIRECTION_SELECTION_SCHEMA: Final = "temporal_direction_selection_v1"
DIRECTION_SELECTION_IDENTITY_SCHEMA: Final = "temporal_direction_selection_identity_v1"
DIRECTION_SELECTION_MIRROR_IDENTITY_SCHEMA: Final = (
    "temporal_direction_selection_mirror_identity_v1"
)

LANE_BALANCED_BIDIRECTIONAL: Final = "balanced_bidirectional"
LANE_LONG_SPECIALIST: Final = "long_specialist"
LANE_SHORT_SPECIALIST: Final = "short_specialist"
LANE_INACTIVE_OR_UNSUPPORTED: Final = "inactive_or_unsupported"
LANE_HARMFUL_OPPOSITE_SIDE: Final = "harmful_opposite_side"
DIRECTION_SELECTION_LANES: Final = (
    LANE_BALANCED_BIDIRECTIONAL,
    LANE_LONG_SPECIALIST,
    LANE_SHORT_SPECIALIST,
    LANE_INACTIVE_OR_UNSUPPORTED,
    LANE_HARMFUL_OPPOSITE_SIDE,
)
_SIDES: Final = ("long", "short")


def _finite(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise TemporalDiscoveryContractError(f"{name} must be finite")
    return result


def _nonnegative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(f"{name} must be an integer") from exc
    if result < 0:
        raise TemporalDiscoveryContractError(f"{name} must be nonnegative")
    return result


def _sha256_identity(value: Any, *, name: str) -> str:
    token = str(value or "")
    if not (
        token.startswith("sha256:")
        and len(token) == 71
        and all(character in "0123456789abcdef" for character in token[7:])
    ):
        raise TemporalDiscoveryContractError(f"{name} must be an exact sha256 identity")
    return token


@dataclass(frozen=True)
class DirectionSelectionPolicyV1:
    """Explicit support/economic thresholds for a new campaign only.

    ``harmful_opposite_net_r`` is an aggregate, cost-inclusive R floor.  It is
    intentionally separate from a neutral side: a merely weak side does not
    become a balanced admission, while a materially losing, supported side is
    named as harmful rather than subsidized by the profitable side.
    """

    minimum_closed_trades_per_side: int = 1
    minimum_active_windows_per_side: int = 1
    minimum_acceptable_side_net_r: float = 0.0
    harmful_opposite_net_r: float = -0.25

    def __post_init__(self) -> None:
        if self.minimum_closed_trades_per_side < 1:
            raise TemporalDiscoveryContractError(
                "minimum_closed_trades_per_side must be at least one"
            )
        if self.minimum_active_windows_per_side < 1:
            raise TemporalDiscoveryContractError(
                "minimum_active_windows_per_side must be at least one"
            )
        acceptable = _finite(
            self.minimum_acceptable_side_net_r,
            name="minimum_acceptable_side_net_r",
        )
        harmful = _finite(
            self.harmful_opposite_net_r,
            name="harmful_opposite_net_r",
        )
        if harmful >= acceptable:
            raise TemporalDiscoveryContractError(
                "harmful_opposite_net_r must be below the acceptable side floor"
            )

    def material(self) -> dict[str, Any]:
        return {
            "schemaVersion": DIRECTION_SELECTION_POLICY_SCHEMA,
            **asdict(self),
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.material())


DEFAULT_DIRECTION_SELECTION_POLICY = DirectionSelectionPolicyV1()


def _side_summary(
    behavior: Mapping[str, Any], side: str, policy: DirectionSelectionPolicyV1, window_count: int
) -> dict[str, Any]:
    sides = behavior.get("sides")
    if not isinstance(sides, Mapping):
        raise TemporalDiscoveryContractError("realized behavior sides must be an object")
    row = sides.get(side)
    if not isinstance(row, Mapping):
        raise TemporalDiscoveryContractError(f"realized behavior {side} side is invalid")
    closed = _nonnegative_int(row.get("closedTrades"), name=f"{side} closedTrades")
    active_windows = _nonnegative_int(
        row.get("activeWindowCount"), name=f"{side} activeWindowCount"
    )
    gross = _finite(row.get("grossR"), name=f"{side} grossR")
    net = _finite(row.get("netR"), name=f"{side} netR")
    cost = _finite(row.get("costR"), name=f"{side} costR")
    if not math.isclose(gross - net, cost, abs_tol=1e-9):
        raise TemporalDiscoveryContractError(
            f"realized behavior {side} gross/net/cost R does not reconcile"
        )
    fraction = _finite(
        row.get("activeWindowFraction"), name=f"{side} activeWindowFraction"
    )
    if not 0.0 <= fraction <= 1.0:
        raise TemporalDiscoveryContractError(
            f"{side} activeWindowFraction must be between zero and one"
        )
    if active_windows > window_count or not math.isclose(
        fraction, active_windows / window_count, abs_tol=1e-12
    ):
        raise TemporalDiscoveryContractError(
            f"{side} active-window evidence is inconsistent"
        )
    terminal_direction_count = _nonnegative_int(
        row.get("terminalDirectionCount", 0), name=f"{side} terminalDirectionCount"
    )
    if bool(row.get("active")) != bool(closed or terminal_direction_count):
        raise TemporalDiscoveryContractError(f"{side} active flag is inconsistent")
    supported = (
        closed >= policy.minimum_closed_trades_per_side
        and active_windows >= policy.minimum_active_windows_per_side
    )
    acceptable = supported and net >= policy.minimum_acceptable_side_net_r
    materially_harmful = supported and net <= policy.harmful_opposite_net_r
    return {
        "side": side,
        "closedTrades": closed,
        "activeWindowCount": active_windows,
        "activeWindowFraction": fraction,
        "grossR": gross,
        "netR": net,
        "costR": cost,
        "supported": supported,
        "acceptable": acceptable,
        "materiallyHarmful": materially_harmful,
    }


def _classify(
    long: Mapping[str, Any], short: Mapping[str, Any]
) -> tuple[str, bool, str | None]:
    # A supported materially harmful side always wins over the aggregate: a
    # good long cannot hide a genuinely bad short (and vice versa).
    if (long["acceptable"] and short["materiallyHarmful"]) or (
        short["acceptable"] and long["materiallyHarmful"]
    ):
        return LANE_HARMFUL_OPPOSITE_SIDE, False, None
    if long["acceptable"] and short["acceptable"]:
        return LANE_BALANCED_BIDIRECTIONAL, True, None
    if long["acceptable"] and not short["supported"]:
        return LANE_LONG_SPECIALIST, True, "long"
    if short["acceptable"] and not long["supported"]:
        return LANE_SHORT_SPECIALIST, True, "short"
    # This includes both inactive candidates and evidence where a side has
    # activity but cannot meet the declared support or net economics contract.
    return LANE_INACTIVE_OR_UNSUPPORTED, False, None


def _rank_key(
    *, lane: str, long: Mapping[str, Any], short: Mapping[str, Any]
) -> dict[str, Any]:
    # Lower lane tier is preferred only *within an explicitly lane-aware
    # allocator*.  Consumers must not collapse this into one scalar selection
    # score, or strong specialists would be accidentally banned.
    tier = {
        LANE_BALANCED_BIDIRECTIONAL: 0,
        LANE_LONG_SPECIALIST: 1,
        LANE_SHORT_SPECIALIST: 1,
        LANE_INACTIVE_OR_UNSUPPORTED: 2,
        LANE_HARMFUL_OPPOSITE_SIDE: 3,
    }[lane]
    eligible_nets = sorted(
        (row["netR"] for row in (long, short) if row["acceptable"]), reverse=True
    )
    return {
        "laneTier": tier,
        "eligibleSideCount": len(eligible_nets),
        "eligibleNetRDescending": eligible_nets,
        "combinedNetR": long["netR"] + short["netR"],
        "combinedCostR": long["costR"] + short["costR"],
    }


def classify_direction_selection(
    realized_behavior: Mapping[str, Any],
    *,
    policy: DirectionSelectionPolicyV1 = DEFAULT_DIRECTION_SELECTION_POLICY,
) -> dict[str, Any]:
    """Classify aggregate realized behavior without changing selection.

    The result is canonical JSON material and contains both direction-labelled
    and mirror-normalized identities.  Mirroring long/short changes the named
    specialist lane but preserves the latter identity and ranking economics.
    """
    if not isinstance(realized_behavior, Mapping):
        raise TemporalDiscoveryContractError("realized behavior must be an object")
    if realized_behavior.get("schemaVersion") != REALIZED_BEHAVIOR_SCHEMA:
        raise TemporalDiscoveryContractError("realized behavior schema is unsupported")
    window_count = _nonnegative_int(
        realized_behavior.get("windowCount"), name="realized behavior windowCount"
    )
    if window_count < policy.minimum_active_windows_per_side:
        raise TemporalDiscoveryContractError(
            "realized behavior windowCount cannot meet the active-window contract"
        )
    realized_behavior_identity = _sha256_identity(
        realized_behavior.get("identitySha256"), name="realized behavior identity"
    )
    long = _side_summary(realized_behavior, "long", policy, window_count)
    short = _side_summary(realized_behavior, "short", policy, window_count)
    lane, eligible, specialist_side = _classify(long, short)
    rank = _rank_key(lane=lane, long=long, short=short)
    policy_material = policy.material()
    identity_material = {
        "schemaVersion": DIRECTION_SELECTION_IDENTITY_SCHEMA,
        "policyIdentitySha256": policy.identity_sha256,
        "realizedBehaviorIdentitySha256": realized_behavior_identity,
        "lane": lane,
        "specialistSide": specialist_side,
        "selectionEligible": eligible,
        "sides": {"long": long, "short": short},
        "rankingKey": rank,
    }
    # Side-neutral ordering gives a stable way to identify equivalent
    # long/short mirrors without erasing the direction-labelled identity.
    mirror_sides = sorted(
        (
            {
                key: row[key]
                for key in (
                    "closedTrades", "activeWindowCount", "activeWindowFraction",
                    "grossR", "netR", "costR", "supported", "acceptable",
                    "materiallyHarmful",
                )
            }
            for row in (long, short)
        ),
        key=lambda value: canonical_sha256(value),
    )
    mirror_lane = (
        "specialist" if lane in {LANE_LONG_SPECIALIST, LANE_SHORT_SPECIALIST} else lane
    )
    mirror_material = {
        "schemaVersion": DIRECTION_SELECTION_MIRROR_IDENTITY_SCHEMA,
        "policyIdentitySha256": policy.identity_sha256,
        "lane": mirror_lane,
        "selectionEligible": eligible,
        "sides": mirror_sides,
        "rankingKey": rank,
    }
    return {
        "schemaVersion": DIRECTION_SELECTION_SCHEMA,
        "policy": policy_material,
        "policyIdentitySha256": policy.identity_sha256,
        "realizedBehaviorIdentitySha256": realized_behavior_identity,
        "lane": lane,
        "laneId": f"{DIRECTION_SELECTION_SCHEMA}:{lane}",
        "selectionEligible": eligible,
        "specialistSide": specialist_side,
        "sides": {"long": long, "short": short},
        "rankingKey": rank,
        "identityMaterial": identity_material,
        "identitySha256": canonical_sha256(identity_material),
        "mirrorIdentityMaterial": mirror_material,
        "mirrorIdentitySha256": canonical_sha256(mirror_material),
    }


__all__ = [
    "DEFAULT_DIRECTION_SELECTION_POLICY",
    "DIRECTION_SELECTION_IDENTITY_SCHEMA",
    "DIRECTION_SELECTION_LANES",
    "DIRECTION_SELECTION_MIRROR_IDENTITY_SCHEMA",
    "DIRECTION_SELECTION_POLICY_SCHEMA",
    "DIRECTION_SELECTION_SCHEMA",
    "DirectionSelectionPolicyV1",
    "LANE_BALANCED_BIDIRECTIONAL",
    "LANE_HARMFUL_OPPOSITE_SIDE",
    "LANE_INACTIVE_OR_UNSUPPORTED",
    "LANE_LONG_SPECIALIST",
    "LANE_SHORT_SPECIALIST",
    "classify_direction_selection",
]
