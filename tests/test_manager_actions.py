"""Tests for manager decision parsing and deterministic application."""

from __future__ import annotations

from autoresearch import branch_lifecycle as bl
from autoresearch import manager_actions as ma
from autoresearch import manager_hooks as mh
from autoresearch import validation_outcome as vo
from autoresearch.manager_models import (
    ManagerAction,
    ManagerActionKind,
    ManagerDecision,
)


class DummyPolicy:
    mode_name = "run"
    allow_finish = True
    window_start = None
    window_end = None
    timezone_name = "UTC"
    stop_mode = "after_step"
    soft_wrap_minutes = 0


class _Cfg:
    research = type(
        "R",
        (),
        {
            "reseed_min_remaining_steps": 1,
            "collapse_recovery_max_steps": 3,
            "plot_lower_is_better": False,
            "bankruptcy_cooldown_steps": 1,
            "reseed_max_recent_failures_window": 2,
        },
    )()


class DummyCtrl:
    def __init__(self) -> None:
        self.config = _Cfg()
        self._family_branches: dict[str, bl.FamilyBranchState] = {
            "fam-a": bl.FamilyBranchState(family_id="fam-a"),
        }
        self._branch_overlay = bl.BranchRunOverlay()
        self._manager_runtime = type(
            "M",
            (),
            {"manager_notes": []},
        )()

    def _trace_runtime(self, *_args: object, **_kwargs: object) -> None:
        return


def test_parse_manager_decision_minimal() -> None:
    d = ma.parse_manager_decision(
        {"rationale": "test", "confidence": "high", "actions": []}
    )
    assert d is not None
    assert d.actions == []


def test_parse_manager_decision_unknown_kind_dropped() -> None:
    d = ma.parse_manager_decision(
        {
            "rationale": "x",
            "actions": [{"kind": "set_budget_mode", "budget_mode": "scouting"}],
        }
    )
    assert d is not None
    assert len(d.actions) == 1
    assert d.actions[0].kind == ManagerActionKind.set_budget_mode


def test_apply_set_budget_mode() -> None:
    ctrl = DummyCtrl()
    decision = ManagerDecision(
        rationale="q",
        actions=[
            ManagerAction(
                kind=ManagerActionKind.set_budget_mode,
                payload={"budget_mode": "validation"},
            )
        ],
    )
    applied = ma.apply_manager_decision(
        ctrl, None, decision, step=1, step_limit=10, policy=DummyPolicy()
    )
    assert applied[0].get("ok") is True
    assert ctrl._branch_overlay.budget_mode == bl.BUDGET_VALIDATION


def test_apply_mark_unresolved() -> None:
    ctrl = DummyCtrl()
    decision = ManagerDecision(
        rationale="q",
        actions=[
            ManagerAction(
                kind=ManagerActionKind.mark_unresolved,
                payload={"family_id": "fam-a"},
            )
        ],
    )
    ma.apply_manager_decision(
        ctrl, None, decision, step=1, step_limit=10, policy=DummyPolicy()
    )
    assert ctrl._family_branches["fam-a"].last_validation_outcome == vo.VALIDATION_UNRESOLVED


def test_apply_validated_leader_syncs_family_state() -> None:
    ctrl = DummyCtrl()
    decision = ManagerDecision(
        rationale="q",
        actions=[
            ManagerAction(
                kind=ManagerActionKind.set_validated_leader,
                payload={"family_id": "fam-a"},
            )
        ],
    )
    ma.apply_manager_decision(
        ctrl, None, decision, step=1, step_limit=10, policy=DummyPolicy()
    )
    branch = ctrl._family_branches["fam-a"]
    assert ctrl._branch_overlay.validated_leader_family_id == "fam-a"
    assert branch.lifecycle_state == bl.LIFECYCLE_VALIDATED_LEADER
    assert branch.promotion_level == bl.PROMOTION_VALIDATED


def test_suppressing_leader_clears_overlay_refs() -> None:
    ctrl = DummyCtrl()
    ctrl._branch_overlay.provisional_leader_family_id = "fam-a"
    decision = ManagerDecision(
        rationale="q",
        actions=[
            ManagerAction(
                kind=ManagerActionKind.suppress_family,
                payload={"family_id": "fam-a", "reason": "manager_suppress"},
            )
        ],
    )
    ma.apply_manager_decision(
        ctrl, None, decision, step=1, step_limit=10, policy=DummyPolicy()
    )
    assert ctrl._branch_overlay.provisional_leader_family_id is None
    assert ctrl._family_branches["fam-a"].lifecycle_state == bl.LIFECYCLE_COLLAPSED


def test_select_post_eval_hook_prefers_specific_events() -> None:
    assert (
        mh.select_post_eval_hook(
            had_timeframe_mismatch=False,
            explicit_retention_fail=True,
            unresolved_validation=True,
            frontier_improved=True,
        )
        == mh.ManagerHookEvent.on_explicit_retention_fail
    )
    assert (
        mh.select_post_eval_hook(
            had_timeframe_mismatch=True,
            explicit_retention_fail=False,
            unresolved_validation=True,
            frontier_improved=True,
        )
        == mh.ManagerHookEvent.on_timeframe_mismatch
    )
