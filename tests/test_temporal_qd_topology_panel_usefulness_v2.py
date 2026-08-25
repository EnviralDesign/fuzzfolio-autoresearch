from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import subprocess

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_evolution import DIRECTIONAL_QD_POLICY_AUTHORITY
from autoresearch.temporal_qd_topology_panel_usefulness_v2 import (
    arm_eligibility,
    arm_eligibility_parity_projection,
    evaluate_panel_usefulness_v2,
    evaluate_replication_survival_v3,
)
from autoresearch.temporal_qd_topology_post_run_analyzer_v1 import evaluate_panel
from autoresearch.temporal_realized_behavior import REALIZED_BEHAVIOR_SCHEMA


def _side(*, net: float, trades: int, windows: int, terminal: int = 0) -> dict:
    gross = net + (0.2 if trades or terminal else 0.0)
    return {
        "closedTrades": trades,
        "wins": max(trades, 0),
        "losses": 0,
        "flatTrades": 0,
        "grossR": gross,
        "netR": net,
        "costR": gross - net,
        "holdingBars": trades,
        "holdingHours": float(trades),
        "active": bool(trades or terminal),
        "activeWindowCount": windows,
        "activeWindowFraction": windows / 4,
        "exposureProxy": 0.0,
        "terminalDirectionCount": terminal,
        "conflictAbstentions": 0,
        "closeReasonDistribution": {},
        "actionDistribution": {},
        "transitionDistribution": {},
        "terminalStatusCounts": {},
    }


def _behavior(*, long: dict | None = None, short: dict | None = None) -> dict:
    sides = {
        "long": long or _side(net=0.0, trades=0, windows=0),
        "short": short or _side(net=0.0, trades=0, windows=0),
    }
    identity = {"sides": deepcopy(sides)}
    return {
        "schemaVersion": REALIZED_BEHAVIOR_SCHEMA,
        "windowCount": 4,
        "sides": sides,
        "identityMaterial": identity,
        "identitySha256": canonical_sha256(identity),
    }


def _member(
    *,
    net: float,
    worst: float,
    counts: list[int],
    behavior: dict,
) -> dict:
    total = sum(counts)
    finite = True
    total_check = total >= 8
    window_check = all(value >= 4 for value in counts)
    positive = True
    support = finite and total_check and window_check and positive
    return {
        "aggregate": {
            "totalConservativeNetR": net,
            "worstWindowConservativeNetR": worst,
            "totalTrades": total,
            "tradeCountsByWindow": counts,
            "costDragR": 0.1,
            "maxWindowDrawdownR": 0.1,
            "totalObservations": 120,
            "realizedBehavior": behavior,
        },
        "objectives": {"worstWindowConservativeNetR": worst},
        "finiteDataValidity": {
            "isFiniteData": finite,
            "passesSupportGate": support,
            "validForQuality": finite and support,
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "totalTrades": total,
            "tradeCountsByWindow": counts,
            "checks": {
                "finiteEconomicMetrics": finite,
                "minimumTotalTrades": total_check,
                "minimumTradesEveryWindow": window_check,
                "positiveObservationSupport": positive,
            },
        },
    }


def _row(arm: str, member: dict) -> dict:
    aggregate = member["aggregate"]
    return {
        "candidateId": f"candidate-{arm}",
        "conservativeNetR": aggregate["totalConservativeNetR"],
        "worstWindowConservativeNetR": aggregate["worstWindowConservativeNetR"],
        "tradeCount": aggregate["totalTrades"],
        "costDragR": aggregate["costDragR"],
        **arm_eligibility(member, deepcopy(DIRECTIONAL_QD_POLICY_AUTHORITY)),
        "identity": {"candidateId": f"candidate-{arm}"},
    }


def _old_row(member: dict, *, changed_side: str = "long") -> dict:
    aggregate = member["aggregate"]
    validity = member["finiteDataValidity"]
    return {
        "conservativeNetR": aggregate["totalConservativeNetR"],
        "worstWindowConservativeNetR": aggregate["worstWindowConservativeNetR"],
        "tradeCount": aggregate["totalTrades"],
        "costDragR": aggregate["costDragR"],
        "support": validity["passesSupportGate"],
        "quality": validity["validForQuality"],
        "direction": aggregate["realizedBehavior"]["sides"][changed_side]["active"],
    }


def _eligible_behavior() -> dict:
    return _behavior(long=_side(net=1.0, trades=8, windows=4))


def test_v2_4_changed_side_active_proxy_has_direction_false_positive() -> None:
    member = _member(
        net=1.0,
        worst=0.0,
        counts=[4, 4, 4, 4],
        behavior=_behavior(long=_side(net=-0.2, trades=0, windows=4, terminal=4)),
    )
    assert _old_row(member)["direction"] is True
    projection = arm_eligibility(member, deepcopy(DIRECTIONAL_QD_POLICY_AUTHORITY))
    assert projection["directionSelection"]["eligible"] is False
    assert projection["directionSelection"]["reasonCode"] == "inactive_or_unsupported"


def test_v2_4_changed_side_active_proxy_has_specialist_false_negative() -> None:
    member = _member(
        net=1.0,
        worst=0.0,
        counts=[4, 4, 4, 4],
        behavior=_behavior(short=_side(net=1.0, trades=8, windows=4)),
    )
    assert _old_row(member)["direction"] is False
    projection = arm_eligibility(member, deepcopy(DIRECTIONAL_QD_POLICY_AUTHORITY))
    assert projection["directionSelection"]["eligible"] is True
    assert projection["directionSelection"]["selection"]["lane"] == "short_specialist"


def test_v2_4_valid_for_quality_proxy_has_negative_robust_return_false_positive() -> None:
    member = _member(
        net=1.0,
        worst=-0.1,
        counts=[4, 4, 4, 4],
        behavior=_eligible_behavior(),
    )
    assert _old_row(member)["quality"] is True
    projection = arm_eligibility(member, deepcopy(DIRECTIONAL_QD_POLICY_AUTHORITY))
    assert projection["qualityLaneEligibility"]["eligible"] is False
    assert projection["qualityLaneEligibility"]["reasonCode"] == "negative_worst_window_robust_return"


def test_ineligible_controls_are_diagnostic_not_a_veto() -> None:
    controls = {
        "P": _member(net=0.0, worst=-0.2, counts=[0, 0, 0, 0], behavior=_behavior()),
        "T": _member(net=0.5, worst=-0.1, counts=[0, 0, 0, 0], behavior=_behavior()),
        "E": _member(net=0.75, worst=0.0, counts=[0, 0, 0, 0], behavior=_behavior()),
    }
    te = _member(net=2.0, worst=0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior())
    old = evaluate_panel({arm: _old_row(member) for arm, member in {**controls, "TE": te}.items()})
    assert old["usefulProgressiveInnovation"] is False
    corrected = evaluate_panel_usefulness_v2(
        {arm: _row(arm, member) for arm, member in {**controls, "TE": te}.items()}
    )
    assert corrected["usefulProgressiveInnovationV2"] is True
    assert corrected["teSupportEligible"] is True
    assert corrected["allArmEligibilityDiagnostic"]["P"] == {
        "support": False,
        "quality": False,
        "direction": False,
    }


@pytest.mark.parametrize("failure", ["support", "quality", "direction", "net", "risk"])
def test_te_only_gate_failures_reject_u_v2(failure: str) -> None:
    members = {
        "P": _member(net=0.0, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "T": _member(net=0.5, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "E": _member(net=0.75, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "TE": _member(net=2.0, worst=0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
    }
    if failure == "support":
        members["TE"] = _member(net=2.0, worst=0.1, counts=[1, 1, 1, 1], behavior=_eligible_behavior())
    elif failure == "quality":
        members["TE"] = _member(net=2.0, worst=-0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior())
    elif failure == "direction":
        members["TE"] = _member(net=2.0, worst=0.1, counts=[4, 4, 4, 4], behavior=_behavior())
    elif failure == "net":
        members["TE"]["aggregate"]["totalConservativeNetR"] = 0.5
    elif failure == "risk":
        members["TE"]["aggregate"]["worstWindowConservativeNetR"] = -0.1
        members["TE"]["objectives"]["worstWindowConservativeNetR"] = -0.1
    result = evaluate_panel_usefulness_v2({arm: _row(arm, member) for arm, member in members.items()})
    assert result["usefulProgressiveInnovationV2"] is False


@pytest.fixture(scope="session")
def rust_policy_binary() -> Path:
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            "cargo",
            "build",
            "-p",
            "temporal-qd-kernel",
            "--bin",
            "temporal-qd-topology-panel-usefulness-v2-jsonl",
        ],
        cwd=root / "rust" / "temporal-qd",
        check=True,
    )
    suffix = ".exe" if __import__("os").name == "nt" else ""
    return (
        root
        / "rust"
        / "temporal-qd"
        / "target"
        / "debug"
        / f"temporal-qd-topology-panel-usefulness-v2-jsonl{suffix}"
    )


def _rust_projection(binary: Path, request: dict) -> dict:
    completed = subprocess.run(
        [str(binary)],
        input=json.dumps(request, separators=(",", ":")) + "\n",
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


@pytest.mark.parametrize(
    "member",
    [
        _member(net=1.0, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        _member(
            net=1.0,
            worst=0.0,
            counts=[4, 4, 4, 4],
            behavior=_behavior(long=_side(net=-0.2, trades=0, windows=4, terminal=4)),
        ),
        _member(
            net=1.0,
            worst=0.0,
            counts=[4, 4, 4, 4],
            behavior=_behavior(short=_side(net=1.0, trades=8, windows=4)),
        ),
        _member(net=1.0, worst=-0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        _member(net=1.0, worst=0.0, counts=[1, 1, 1, 1], behavior=_eligible_behavior()),
        _member(
            net=1.0,
            worst=0.0,
            counts=[4, 4, 4, 4],
            behavior=_behavior(
                long=_side(net=1.0, trades=8, windows=4),
                short=_side(net=-0.3, trades=8, windows=4),
            ),
        ),
    ],
    ids=[
        "eligible-specialist",
        "active-unsupported",
        "inactive-changed-side-opposite-specialist",
        "negative-robust-return",
        "support-failure",
        "harmful-opposite-side",
    ],
)
def test_python_rust_exact_arm_projection_parity(rust_policy_binary: Path, member: dict) -> None:
    authority = deepcopy(DIRECTIONAL_QD_POLICY_AUTHORITY)
    expected = arm_eligibility_parity_projection(member, authority)
    actual = _rust_projection(
        rust_policy_binary,
        {
            "schemaVersion": "temporal_qd_topology_arm_eligibility_request_v2",
            "archivePolicyAuthority": authority,
            "member": member,
        },
    )
    assert actual == expected


def _panel_members() -> dict[str, dict]:
    return {
        "P": _member(net=0.0, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "T": _member(net=0.5, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "E": _member(net=0.75, worst=0.0, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
        "TE": _member(net=2.0, worst=0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior()),
    }


@pytest.mark.parametrize(
    "mutation",
    ["eligible", "control-ineligible", "support", "quality", "direction", "tie", "dust", "risk"],
)
def test_python_rust_exact_panel_projection_parity(
    rust_policy_binary: Path, mutation: str
) -> None:
    members = _panel_members()
    if mutation == "control-ineligible":
        members["P"] = _member(net=-1.0, worst=-1.0, counts=[0, 0, 0, 0], behavior=_behavior())
    elif mutation == "support":
        members["TE"] = _member(net=2.0, worst=0.1, counts=[1, 1, 1, 1], behavior=_eligible_behavior())
    elif mutation == "quality":
        members["TE"] = _member(net=2.0, worst=-0.1, counts=[4, 4, 4, 4], behavior=_eligible_behavior())
    elif mutation == "direction":
        members["TE"] = _member(net=2.0, worst=0.1, counts=[4, 4, 4, 4], behavior=_behavior())
    elif mutation == "tie":
        members["TE"]["aggregate"]["totalConservativeNetR"] = 0.75
    elif mutation == "dust":
        members["TE"]["aggregate"]["totalConservativeNetR"] = 0.75 + 5e-13
    elif mutation == "risk":
        members["T"] = _member(net=0.5, worst=0.2, counts=[4, 4, 4, 4], behavior=_eligible_behavior())
    arms = {arm: _row(arm, member) for arm, member in members.items()}
    expected = evaluate_panel_usefulness_v2(arms)
    actual = _rust_projection(
        rust_policy_binary,
        {
            "schemaVersion": "temporal_qd_topology_panel_usefulness_request_v2",
            "arms": arms,
        },
    )
    assert actual == expected


@pytest.mark.parametrize(
    "panel_3,panel_1,panel_2,identities_valid",
    [
        *((p3, p1, p2, True) for p3 in (False, True) for p1 in (False, True) for p2 in (False, True)),
        (True, True, None, True),
        (True, True, True, False),
    ],
)
def test_python_rust_exact_cross_panel_projection_parity(
    rust_policy_binary: Path,
    panel_3: bool | None,
    panel_1: bool | None,
    panel_2: bool | None,
    identities_valid: bool,
) -> None:
    panels = {"panel-3": panel_3, "panel-1": panel_1, "panel-2": panel_2}
    expected = evaluate_replication_survival_v3(panels, identities_valid=identities_valid)
    actual = _rust_projection(
        rust_policy_binary,
        {
            "schemaVersion": "temporal_qd_topology_replication_survival_request_v3",
            "panelUsefulProgressiveInnovationV2": panels,
            "identitiesValid": identities_valid,
        },
    )
    assert actual == expected
