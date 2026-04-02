from __future__ import annotations

import json
from types import SimpleNamespace

from autoresearch import branch_lifecycle as bl
from autoresearch import manager_packet as mp
from autoresearch import validation_outcome as vo
from autoresearch.controller import ResearchController
from autoresearch.manager_models import ManagerHookEvent
from autoresearch.scoring import AttemptScore


def _make_controller(
    family_map: dict[str, str] | None = None,
) -> ResearchController:
    controller = object.__new__(ResearchController)
    controller.config = SimpleNamespace(
        research=SimpleNamespace(
            plot_lower_is_better=False,
            validated_leader_min_horizon_months=12,
            horizon_late_months=24,
            horizon_wrap_up_months=36,
            retention_strong_candidate_threshold=55.0,
            validated_leader_min_score=45.0,
            reseed_after_stale_validation_steps=10,
            phase_early_ratio=0.35,
            phase_late_ratio=0.75,
            run_wrap_up_steps=3,
        ),
        manager=SimpleNamespace(max_candidate_families_in_packet=8),
    )
    controller._family_branches = {}
    controller._branch_overlay = bl.BranchRunOverlay()
    controller._validation_stale_without_validated = 0
    controller._frontier_prior_best = None
    controller.profile_sources = {}
    mapping = family_map or {}
    controller._family_id_for_profile_ref = lambda ref: mapping.get(ref)
    return controller


def _make_attempt(
    *,
    sequence: int,
    name: str,
    profile_ref: str,
    score: float,
    horizon_months: int,
    effective_window_months: float,
    trades_per_month: float,
    resolved_trades: int,
    validation_outcome: str = vo.VALIDATION_UNRESOLVED,
    requested_timeframe: str = "M15",
    effective_timeframe: str = "M15",
    positive_cell_ratio: float = 0.62,
) -> dict[str, object]:
    return {
        "sequence": sequence,
        "candidate_name": name,
        "profile_ref": profile_ref,
        "composite_score": score,
        "requested_horizon_months": horizon_months,
        "requested_timeframe": requested_timeframe,
        "effective_timeframe": effective_timeframe,
        "validation_outcome": validation_outcome,
        "best_summary": {
            "best_cell": {"resolved_trades": resolved_trades},
            "market_data_window": {
                "effective_window_months": effective_window_months
            },
            "matrix_summary": {"positive_cell_ratio": positive_cell_ratio},
            "quality_score_payload": {
                "inputs": {
                    "trades_per_month": trades_per_month,
                    "resolved_trades": resolved_trades,
                    "effective_window_months": effective_window_months,
                }
            },
        },
    }


def test_support_quality_uses_cadence_not_absolute_trade_count() -> None:
    controller = _make_controller()
    short_hot_run = _make_attempt(
        sequence=1,
        name="spike",
        profile_ref="ref-short",
        score=83.5486,
        horizon_months=3,
        effective_window_months=2.53,
        trades_per_month=11.07,
        resolved_trades=28,
    )

    assert controller._derive_support_quality(short_hot_run) == "broad"
    digest = controller._generate_behavior_digest(short_hot_run)
    assert digest["failure_mode_hint"] == "short_window_spike"
    assert digest["next_move_hint"] == "pressure_test_horizon"


def test_admissible_frontier_prefers_durable_candidate_over_short_spike() -> None:
    controller = _make_controller(
        {"ref-short": "fam-short", "ref-long": "fam-long"}
    )
    controller._family_branches = {
        "fam-short": bl.FamilyBranchState(
            family_id="fam-short",
            promotability_status=vo.PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE,
        ),
        "fam-long": bl.FamilyBranchState(
            family_id="fam-long",
            promotability_status=vo.PROMOTABILITY_VALIDATED_READY,
        ),
    }
    attempts = [
        _make_attempt(
            sequence=1,
            name="spike",
            profile_ref="ref-short",
            score=83.5486,
            horizon_months=3,
            effective_window_months=2.53,
            trades_per_month=11.07,
            resolved_trades=28,
        ),
        _make_attempt(
            sequence=2,
            name="durable",
            profile_ref="ref-long",
            score=57.311,
            horizon_months=24,
            effective_window_months=22.83,
            trades_per_month=4.1,
            resolved_trades=61,
            validation_outcome=vo.VALIDATION_PASSED,
            positive_cell_ratio=0.92,
        ),
    ]

    frontier = controller._admissible_frontier_snapshot(attempts)
    assert frontier["best"]["family_id"] == "fam-long"
    assert frontier["best"]["candidate_name"] == "durable"

    controller._run_attempts = lambda _run_id: attempts
    score_target = controller._score_target_snapshot(SimpleNamespace(run_id="run-a"))
    assert score_target["current_run_best_candidate"] == "durable"
    assert score_target["raw_run_best_candidate"] == "spike"
    assert "Raw run best=83.549 remains informational only." in score_target["summary"]


def test_gut_check_state_escalates_12_then_24_then_36() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._family_branches = {"fam-a": bl.FamilyBranchState(family_id="fam-a")}
    attempts = [
        _make_attempt(
            sequence=1,
            name="spike",
            profile_ref="ref-a",
            score=83.5486,
            horizon_months=3,
            effective_window_months=2.53,
            trades_per_month=11.07,
            resolved_trades=28,
        )
    ]

    state = controller._current_gut_check_state(attempts, phase_name="early")
    assert state is not None
    assert state["target_horizon_months"] == 12

    attempts.append(
        _make_attempt(
            sequence=2,
            name="spike-12m",
            profile_ref="ref-a",
            score=71.2,
            horizon_months=12,
            effective_window_months=12.1,
            trades_per_month=4.6,
            resolved_trades=55,
            validation_outcome=vo.VALIDATION_PASSED,
        )
    )
    state = controller._current_gut_check_state(attempts, phase_name="late")
    assert state is not None
    assert state["target_horizon_months"] == 24

    attempts.append(
        _make_attempt(
            sequence=3,
            name="spike-24m",
            profile_ref="ref-a",
            score=64.8,
            horizon_months=24,
            effective_window_months=24.7,
            trades_per_month=3.9,
            resolved_trades=97,
            validation_outcome=vo.VALIDATION_PASSED,
        )
    )
    state = controller._current_gut_check_state(attempts, phase_name="wrap_up")
    assert state is not None
    assert state["target_horizon_months"] == 36


def test_manager_packet_includes_admissibility_and_gut_check_state() -> None:
    controller = _make_controller({"ref-a": "fam-a", "ref-b": "fam-b"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            best_score=83.5486,
            promotability_status=vo.PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE,
        ),
        "fam-b": bl.FamilyBranchState(
            family_id="fam-b",
            best_score=57.311,
            promotability_status=vo.PROMOTABILITY_VALIDATED_READY,
        ),
    }
    controller._branch_overlay = bl.BranchRunOverlay(provisional_leader_family_id="fam-a")
    attempts = [
        _make_attempt(
            sequence=1,
            name="spike",
            profile_ref="ref-a",
            score=83.5486,
            horizon_months=3,
            effective_window_months=2.53,
            trades_per_month=11.07,
            resolved_trades=28,
        ),
        _make_attempt(
            sequence=2,
            name="durable",
            profile_ref="ref-b",
            score=57.311,
            horizon_months=24,
            effective_window_months=22.83,
            trades_per_month=4.1,
            resolved_trades=61,
            validation_outcome=vo.VALIDATION_PASSED,
            positive_cell_ratio=0.92,
        ),
    ]
    controller._run_attempts = lambda _run_id: attempts

    packet = mp.build_manager_packet(
        controller,
        SimpleNamespace(run_id="run-a"),
        ManagerHookEvent.on_candidate_frontier_change,
        step=4,
        step_limit=20,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert packet.extra["admissible_frontier_best"]["family_id"] == "fam-b"
    assert packet.extra["gut_check_pending"]["target_horizon_months"] == 12
    assert packet.extra["wrap_up_focus"]["family_id"] == "fam-a"


def test_resolve_support_metrics_uses_nested_attempt_payload_when_roots_missing() -> None:
    controller = _make_controller()
    attempt = _make_attempt(
        sequence=1,
        name="durable",
        profile_ref="ref-a",
        score=57.311,
        horizon_months=24,
        effective_window_months=22.83,
        trades_per_month=4.1,
        resolved_trades=61,
        validation_outcome=vo.VALIDATION_PASSED,
        positive_cell_ratio=0.92,
    )

    resolved_trades, trades_per_month, positive_ratio = controller._resolve_support_metrics(
        attempt,
        resolved_trades=None,
        trades_per_month=None,
        positive_ratio=None,
    )

    assert resolved_trades == 61
    assert trades_per_month == 4.1
    assert positive_ratio == 0.92


def test_normalized_attempt_record_evidence_backfills_nested_metrics(tmp_path) -> None:
    controller = _make_controller()
    controller.config.research.effective_coverage_min_ratio = 0.8
    controller._requested_horizon_from_artifact_dir = lambda _artifact_dir: 12
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    tmp_attempt = {
        "best_cell": {"resolved_trades": 1841},
        "market_data_window": {"effective_window_months": 11.27},
        "matrix_summary": {"positive_cell_ratio": 0.625},
        "quality_score_payload": {
            "inputs": {
                "trades_per_month": 163.354037,
                "resolved_trades": 1841,
                "effective_window_months": 11.27,
            }
        },
    }
    score = AttemptScore(
        primary_score=24.2299,
        composite_score=24.2299,
        score_basis="v1:psr",
        metrics={
            "quality_score": 24.2299,
            "dsr": None,
            "psr": 0.9999,
            "k_ratio": 72.1232,
            "sharpe_r": 0.0806,
        },
        best_summary=tmp_attempt,
    )

    evidence = controller._normalized_attempt_record_evidence(
        artifact_dir=artifact_dir,
        sensitivity_snapshot={
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
        },
        score=score,
        compare_payload={"best": tmp_attempt},
    )

    assert evidence["resolved_trades"] == 1841
    assert evidence["trades_per_month"] == 163.354037
    assert evidence["positive_cell_ratio"] == 0.625


def test_current_wrap_up_focus_prefers_retryable_provisional_line() -> None:
    controller = _make_controller({"ref-a": "fam-a", "ref-b": "fam-b"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
            latest_attempt_id="attempt-a",
            last_profile_ref="ref-a",
        ),
        "fam-b": bl.FamilyBranchState(
            family_id="fam-b",
            promotability_status=vo.PROMOTABILITY_BLOCKED,
            retention_status=bl.RETENTION_PENDING,
        ),
    }
    controller._branch_overlay = bl.BranchRunOverlay(
        provisional_leader_family_id="fam-a",
        budget_mode=bl.BUDGET_WRAP_UP,
    )
    attempts = [
        _make_attempt(
            sequence=1,
            name="focus",
            profile_ref="ref-a",
            score=31.5779,
            horizon_months=24,
            effective_window_months=23.54,
            trades_per_month=3.7,
            resolved_trades=87,
        ),
        _make_attempt(
            sequence=2,
            name="blocked",
            profile_ref="ref-b",
            score=40.0,
            horizon_months=24,
            effective_window_months=23.54,
            trades_per_month=3.1,
            resolved_trades=73,
            validation_outcome=vo.VALIDATION_FAILED,
        ),
    ]

    focus = controller._current_wrap_up_focus_state(attempts)

    assert focus is not None
    assert focus["family_id"] == "fam-a"
    assert focus["reason"] == "provisional_leader"


def test_wrap_up_validation_blocks_unrelated_family_tuning() -> None:
    controller = _make_controller({"ref-a": "fam-a", "ref-b": "fam-b"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
        ),
        "fam-b": bl.FamilyBranchState(
            family_id="fam-b",
            promotability_status=vo.PROMOTABILITY_PROVISIONAL_BEST_AVAILABLE,
            retention_status=bl.RETENTION_PENDING,
        ),
    }
    controller._branch_overlay = bl.BranchRunOverlay(
        provisional_leader_family_id="fam-a",
        budget_mode=bl.BUDGET_WRAP_UP,
    )
    attempts = [
        _make_attempt(
            sequence=1,
            name="focus",
            profile_ref="ref-a",
            score=31.5779,
            horizon_months=24,
            effective_window_months=23.54,
            trades_per_month=3.7,
            resolved_trades=87,
        ),
        _make_attempt(
            sequence=2,
            name="other",
            profile_ref="ref-b",
            score=29.0,
            horizon_months=24,
            effective_window_months=23.54,
            trades_per_month=3.4,
            resolved_trades=80,
        ),
    ]
    controller._run_attempts = lambda _run_id: attempts

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a"),
        actions=[
            {
                "tool": "evaluate_candidate",
                "profile_ref": "ref-b",
                "requested_horizon_months": 36,
            }
        ],
        step=198,
        step_limit=200,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert len(errors) == 1
    assert "wrap_up focus is fam-a" in errors[0]


def test_wrap_up_validation_blocks_generic_run_cli_replay_on_focus_family() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
        )
    }
    controller._branch_overlay = bl.BranchRunOverlay(
        provisional_leader_family_id="fam-a",
        budget_mode=bl.BUDGET_WRAP_UP,
    )
    attempts = [
        _make_attempt(
            sequence=1,
            name="focus",
            profile_ref="ref-a",
            score=24.2299,
            horizon_months=12,
            effective_window_months=11.27,
            trades_per_month=163.35,
            resolved_trades=1841,
        )
    ]
    controller._run_attempts = lambda _run_id: attempts

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a", run_dir=None),
        actions=[
            {
                "tool": "run_cli",
                "args": [
                    "deep-replay",
                    "submit",
                    "--profile-ref",
                    "ref-a",
                    "--instrument",
                    "NVDA",
                    "--timeframe",
                    "M5",
                ],
            }
        ],
        step=199,
        step_limit=200,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert len(errors) == 1
    assert "only decisive focus-path actions are allowed" in errors[0]


def test_wrap_up_validation_blocks_terminal_compare_on_focus_family() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
        )
    }
    controller._branch_overlay = bl.BranchRunOverlay(
        provisional_leader_family_id="fam-a",
        budget_mode=bl.BUDGET_WRAP_UP,
    )
    attempts = [
        {
            **_make_attempt(
                sequence=1,
                name="focus-a",
                profile_ref="ref-a",
                score=24.2299,
                horizon_months=12,
                effective_window_months=11.27,
                trades_per_month=163.35,
                resolved_trades=1841,
            ),
            "attempt_id": "attempt-1",
            "artifact_dir": "C:\\runs\\focus-a",
        },
        {
            **_make_attempt(
                sequence=2,
                name="focus-b",
                profile_ref="ref-a",
                score=11.055,
                horizon_months=24,
                effective_window_months=23.54,
                trades_per_month=566.99,
                resolved_trades=13347,
            ),
            "attempt_id": "attempt-2",
            "artifact_dir": "C:\\runs\\focus-b",
        },
    ]
    controller._run_attempts = lambda _run_id: attempts
    controller._attempt_row_for_id = lambda _tool_context, attempt_id: next(
        (att for att in attempts if att.get("attempt_id") == attempt_id),
        None,
    )
    controller._attempt_row_for_artifact_dir = lambda _tool_context, artifact_dir: next(
        (att for att in attempts if att.get("artifact_dir") == artifact_dir),
        None,
    )

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a", run_dir=None),
        actions=[
            {
                "tool": "compare_artifacts",
                "attempt_ids": ["attempt-1", "attempt-2"],
            }
        ],
        step=200,
        step_limit=200,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert len(errors) == 1
    assert "only decisive focus-path actions are allowed" in errors[0]


def test_branch_validation_handles_non_cli_action_on_exploit_dead_family() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_COLLAPSED,
            exploit_dead=True,
        )
    }
    controller._branch_overlay = bl.BranchRunOverlay(budget_mode=bl.BUDGET_SCOUTING)
    attempts = [
        {
            **_make_attempt(
                sequence=1,
                name="focus-a",
                profile_ref="ref-a",
                score=0.0013,
                horizon_months=3,
                effective_window_months=2.51,
                trades_per_month=4849.0,
                resolved_trades=12171,
                positive_cell_ratio=0.0,
            ),
            "attempt_id": "attempt-1",
            "artifact_dir": "C:\\runs\\focus-a",
        }
    ]
    controller._run_attempts = lambda _run_id: attempts
    controller._attempt_row_for_id = lambda _tool_context, attempt_id: next(
        (att for att in attempts if att.get("attempt_id") == attempt_id),
        None,
    )

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a", run_dir=None),
        actions=[
            {
                "tool": "inspect_artifact",
                "attempt_id": "attempt-1",
                "view": "summary",
            }
        ],
        step=10,
        step_limit=200,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert errors == []


def test_trace_runtime_preserves_manager_snapshot(tmp_path) -> None:
    controller = object.__new__(ResearchController)
    tool_context = SimpleNamespace(run_dir=tmp_path, run_id="run-a")
    state_path = tmp_path / "runtime-state.json"
    state_path.write_text(
        json.dumps(
            {
                "controller": {"step": 12},
                "controller_updated_at": "2026-04-02T00:00:00+00:00",
                "manager": {"last_hook": "on_unresolved_validation"},
            }
        ),
        encoding="utf-8",
    )

    controller._trace_runtime(
        tool_context,
        step=13,
        phase="step",
        status="start",
        message="hello",
    )

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["manager"]["last_hook"] == "on_unresolved_validation"
