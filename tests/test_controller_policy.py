from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

from autoresearch import branch_lifecycle as bl
from autoresearch import manager_packet as mp
from autoresearch import validation_outcome as vo
import autoresearch.controller as ctrlmod
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
            quality_score_preset="profile-drop",
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
        llm=SimpleNamespace(explorer_profile="test-profile"),
        compact_trigger_tokens_for=lambda *_args, **_kwargs: 12000,
        compact_target_tokens_for=lambda *_args, **_kwargs: 9000,
    )
    controller._family_branches = {}
    controller._branch_overlay = bl.BranchRunOverlay()
    controller._validation_stale_without_validated = 0
    controller._frontier_prior_best = None
    controller.profile_sources = {}
    controller._manager_runtime = ctrlmod.ManagerRuntimeState()
    controller._pending_manager_events = []
    controller._delta_chunk_fingerprints = {}
    controller._delta_chunk_relevance = {}
    controller._last_checkpoint_event_state = None
    controller._checkpoint_required_next_step = False
    controller.last_created_profile_ref = None
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


def test_canonicalize_local_opening_step_inserts_mode_and_keeps_single_prepare() -> None:
    payload = {
        "reasoning": "Start from seed.",
        "actions": [
            {
                "tool": "prepare_profile",
                "indicator_ids": ["A", "B"],
                "instruments": ["EURUSD"],
                "candidate_name": "cand-a",
                "destination_path": r"C:\runs\cand-a.json",
            },
            {"tool": "validate_profile", "profile_path": r"C:\runs\cand-a.json"},
        ],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(payload)

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "scaffold_from_seed",
            "indicator_ids": ["A", "B"],
            "instruments": ["EURUSD"],
            "candidate_name": "cand-a",
        }
    ]


def test_canonicalize_local_opening_step_maps_legacy_fields() -> None:
    payload = {
        "reasoning": "Open with scaffold.",
        "actions": [
            {
                "tool": "prepare_profile",
                "seed_indicators": ["ID_X"],
                "profile_name": "legacy-cand",
                "destination_path": r"C:\runs\legacy-cand.json",
            }
        ],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(payload)

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "scaffold_from_seed",
            "indicator_ids": ["ID_X"],
            "candidate_name": "legacy-cand",
        }
    ]


def test_canonicalize_local_opening_step_leaves_non_prepare_response_unchanged() -> None:
    payload = {
        "reasoning": "Inspect last run.",
        "actions": [{"tool": "inspect_artifact", "attempt_id": "att-1", "view": "summary"}],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(payload)

    assert normalized == payload


def test_canonicalize_local_opening_step_rewrites_bad_grounding_into_run_profiles() -> None:
    payload = {
        "reasoning": "Start from seed.",
        "actions": [
            {
                "tool": "prepare_profile",
                "indicator_ids": ["A", "B"],
                "profile_name": "cand-a",
                "instruments": ["ALL"],
                "destination_path": r"C:\profiles\cand-a.json",
            }
        ],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(
        payload,
        starter_instruments=["EURUSD"],
        candidate_name_hint="cand-a",
    )

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "scaffold_from_seed",
            "indicator_ids": ["A", "B"],
            "instruments": ["EURUSD"],
            "candidate_name": "cand-a",
        }
    ]


def test_canonicalize_local_opening_step_fills_missing_destination_when_safe() -> None:
    payload = {
        "reasoning": "Start from seed.",
        "actions": [
            {
                "tool": "prepare_profile",
                "indicator_ids": ["A"],
                "candidate_name": "cand-b",
            }
        ],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(
        payload,
        starter_instruments=["EURUSD", "GBPUSD"],
    )

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "scaffold_from_seed",
            "indicator_ids": ["A"],
            "instruments": ["EURUSD", "GBPUSD"],
            "candidate_name": "cand-b",
        }
    ]


def test_canonicalize_local_opening_step_leaves_ambiguous_instruments_unchanged_without_starter_list() -> None:
    payload = {
        "reasoning": "Start from seed.",
        "actions": [
            {
                "tool": "prepare_profile",
                "indicator_ids": ["A"],
                "destination_path": r"C:\runs\example\profiles\cand-c.json",
            }
        ],
    }

    normalized = ctrlmod.canonicalize_local_opening_step_response(payload)

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "scaffold_from_seed",
            "indicator_ids": ["A"],
            "candidate_name": "cand-c",
        }
    ]


def test_canonicalize_followup_step_response_fills_handle_and_instruments_from_template() -> None:
    payload = {
        "reasoning": "Evaluate the registered profile next.",
        "actions": [
            {
                "tool": "evaluate_candidate",
                "profile_path": r"C:\runs\example\profiles\cand-a.json",
            },
            {"tool": "inspect_artifact", "attempt_id": "att-extra", "view": "summary"},
        ],
    }

    normalized = ctrlmod.canonicalize_followup_step_response(
        payload,
        next_action_template={
            "tool": "evaluate_candidate",
            "profile_ref": "ref-a",
            "instruments": ["EURUSD"],
            "evaluation_mode": "screen",
            "timeframe_policy": "profile_default",
        },
    )

    assert normalized["actions"] == [
        {
            "tool": "evaluate_candidate",
            "candidate_name": "cand-a",
            "profile_ref": "ref-a",
            "instruments": ["EURUSD"],
            "evaluation_mode": "screen",
            "timeframe_policy": "profile_default",
        }
    ]


def test_canonicalize_followup_step_response_ignores_wrong_tool() -> None:
    payload = {
        "reasoning": "Inspect first.",
        "actions": [{"tool": "inspect_artifact", "attempt_id": "att-1", "view": "summary"}],
    }

    normalized = ctrlmod.canonicalize_followup_step_response(
        payload,
        next_action_template={
            "tool": "evaluate_candidate",
            "profile_ref": "ref-a",
            "instruments": ["EURUSD"],
        },
    )

    assert normalized == payload


def test_apply_runtime_interventions_passes_step_limit_and_policy_to_followup_template() -> None:
    controller = _make_controller()
    controller._trace_runtime = lambda *_args, **_kwargs: None
    controller._append_raw_explorer_payload = lambda *_args, **_kwargs: None
    controller._is_true_opening_step = lambda *_args, **_kwargs: False
    seen: dict[str, object] = {}

    def followup_template(_tool_context, **kwargs):
        seen.update(kwargs)
        return {"tool": "validate_profile", "candidate_name": "cand-a"}

    controller._followup_next_action_template_prompt_state = followup_template
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))
    policy = ctrlmod.RunPolicy(mode_name="run")
    response = {
        "reasoning": "Validate the candidate next.",
        "actions": [{"tool": "validate_profile", "candidate_name": "cand-a"}],
    }

    normalized = controller._apply_runtime_interventions(
        tool_context,
        2,
        response,
        phase="explorer_normalize",
        step_limit=50,
        policy=policy,
    )

    assert normalized == response
    assert seen["step"] == 2
    assert seen["step_limit"] == 50
    assert seen["policy"] is policy


def test_repair_invalid_response_passes_step_limit_and_policy_to_followup_template() -> None:
    controller = _make_controller()
    controller._trace_runtime = lambda *_args, **_kwargs: None
    controller._append_raw_explorer_payload = lambda *_args, **_kwargs: None
    controller._uses_local_transformers_provider = lambda: True
    controller._compact_repair_messages = lambda payload, **kwargs: payload
    controller._provider_scope = lambda **_kwargs: nullcontext()
    controller.provider = SimpleNamespace(complete_json=lambda messages: messages)
    controller._normalize_model_response = lambda repaired: repaired
    controller._is_true_opening_step = lambda *_args, **_kwargs: False
    controller._validate_actions = lambda _actions: []
    controller._validate_finish_timing = lambda *_args, **_kwargs: []
    controller._validate_repeated_actions = lambda *_args, **_kwargs: []
    controller._validate_timeframe_mismatch_block = lambda *_args, **_kwargs: []
    controller._validate_branch_lifecycle_actions = lambda *_args, **_kwargs: []
    controller._current_run_policy = None
    controller._current_step_limit = 50
    seen: dict[str, object] = {}

    def followup_template(_tool_context, **kwargs):
        seen.update(kwargs)
        return {"tool": "validate_profile", "candidate_name": "cand-a"}

    controller._followup_next_action_template_prompt_state = followup_template
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))
    policy = ctrlmod.RunPolicy(mode_name="run")

    repaired = controller._repair_invalid_response(
        tool_context,
        3,
        [],
        "Need repair",
        [{"tool": "validate_profile", "candidate_name": "cand-a"}],
        ["bad action"],
        step_limit=50,
        policy=policy,
    )

    assert repaired["actions"] == [{"tool": "validate_profile", "candidate_name": "cand-a"}]
    assert seen["step"] == 3
    assert seen["step_limit"] == 50
    assert seen["policy"] is policy


def test_system_protocol_stays_stable_and_opening_overlay_moves_to_step_update() -> None:
    controller = _make_controller()
    controller.config.provider = SimpleNamespace(provider_type="openai")
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: []
    controller._durable_system_appendix_text = lambda: "Program:\npolicy"
    controller._local_opening_grounding_prompt_state = lambda *_args, **_kwargs: {
        "preferred_initial_instruments": ["EURUSD"],
        "candidate_name_hint": "cand1",
    }
    controller._early_seed_goal_text = lambda *_args, **_kwargs: ""
    controller._followup_next_action_template_prompt_state = lambda *_args, **_kwargs: None
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "early",
        "summary": "Phase summary",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "12 months",
        "guidance": "Use 12 months",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find a scorer",
    }
    controller._run_attempts = lambda _run_id: []
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- No live handles are pinned yet."
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: No evaluated attempts yet."
    )
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        seed_prompt_path=Path("C:/runs/example/seed-prompt.json"),
    )

    protocol_step_1 = controller._system_protocol_text(
        ctrlmod.RunPolicy(),
        tool_context=tool_context,
        step=1,
    )
    protocol_step_2 = controller._system_protocol_text(
        ctrlmod.RunPolicy(),
        tool_context=tool_context,
        step=2,
    )
    step_prompt = controller._step_update_prompt(
        tool_context,
        ctrlmod.RunPolicy(),
        step=1,
        step_limit=10,
    )

    assert protocol_step_1 == protocol_step_2
    assert "fresh-run opening step" not in protocol_step_1.lower()
    assert "Program:\npolicy" in protocol_step_1
    assert "===== OPENING OVERLAY =====" in step_prompt
    assert "Opening-step overlay:" in step_prompt
    assert '"tool": "prepare_profile"' in step_prompt


def test_run_state_prompt_uses_step_update_sections_and_keeps_durable_doctrine_out() -> None:
    controller = _make_controller()
    controller.config.provider = SimpleNamespace(provider_type="openai")
    controller._uses_local_transformers_provider = lambda: False
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: []
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "early",
        "summary": "Phase summary",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "12 months",
        "guidance": "Use 12 months",
        "rationale": "durability",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find a scorer",
        "rationale": "need evidence",
    }
    controller._followup_next_action_template_prompt_state = lambda *_args, **_kwargs: None
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand1"
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    controller._local_opening_grounding_prompt_state = lambda *_args, **_kwargs: {
        "preferred_initial_instruments": ["EURUSD"],
        "candidate_name_hint": "cand1",
    }
    controller._early_seed_goal_text = lambda *_args, **_kwargs: ""
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: No evaluated attempts yet."
    )

    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        seed_prompt_path=Path("C:/runs/example/seed-prompt.json"),
        indicator_catalog_summary="Indicator facts",
        seed_indicator_parameter_hints="Hint block",
        instrument_catalog_summary="Instrument facts",
    )

    prompt = controller._run_state_prompt(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=1,
        step_limit=10,
    )

    assert "===== STEP FRAME =====" in prompt
    assert "===== GOAL STATE =====" in prompt
    assert "===== BRANCH AUTHORITY =====" in prompt
    assert "===== ACTIVE HANDLES =====" in prompt
    assert "===== OPENING OVERLAY =====" in prompt
    assert "===== CURRENT CONTROLLER UPDATE =====" not in prompt
    assert "===== CONTEXTUAL INJECTIONS =====" not in prompt
    assert "Current seed hand:" not in prompt
    assert "Sticky indicator context:" not in prompt
    assert "Program:" not in prompt
    assert "Portable profile template note:" not in prompt
    assert "Tool reference:" not in prompt
    assert "Sensitivity artifact layout (on disk after evaluations):" not in prompt


def test_pinned_run_reference_keeps_seed_schema_but_omits_dynamic_run_state() -> None:
    controller = _make_controller()
    controller._seed_indicator_ids = lambda *_args, **_kwargs: ["ADX", "RSI_MEAN_REVERSION"]
    controller._compact_seed_parameter_schema_text = (
        lambda *_args, **_kwargs: "- ADX: tf_default=M5 | params=timeperiod\n- RSI_MEAN_REVERSION: tf_default=M5 | params=timeperiod"
    )
    controller._compact_instrument_reference_text = (
        lambda *_args, **_kwargs: "- Use exact symbols from the catalog.\n- Prefer coverage-qualified symbols first."
    )
    tool_context = SimpleNamespace(
        seed_prompt_path=Path("C:/runs/example/seed-prompt.json"),
        indicator_catalog_summary="Supported timeframes: M1, M5, H1",
        seed_indicator_parameter_hints="unused",
        instrument_catalog_summary="Prefer coverage-qualified symbols first",
    )

    prompt = controller._pinned_run_reference_prompt(tool_context)

    assert "Run reference (stable for this run):" in prompt
    assert "Exact seeded indicator ids for this run: ADX, RSI_MEAN_REVERSION" in prompt
    assert "Seeded indicator mutation schema:" in prompt
    assert "Run-owned profiles so far:" not in prompt
    assert "Checkpoint summary:" not in prompt
    assert "Recent attempts:" not in prompt


def test_chunked_history_trim_keeps_prefix_and_recent_step_chunks_only(tmp_path: Path) -> None:
    controller = _make_controller()
    controller._trace_runtime = lambda *_args, **_kwargs: None
    controller._approx_message_tokens = lambda messages: len(messages) * 10
    tool_context = SimpleNamespace(
        run_dir=tmp_path,
        run_id="run-a",
    )
    prefix = [
        ctrlmod.ChatMessage(role="system", content="system"),
        ctrlmod.ChatMessage(role="user", content="run-ref"),
    ]
    history = [
        ctrlmod.ChatMessage(role="user", content="step-1"),
        ctrlmod.ChatMessage(role="assistant", content="reply-1"),
        ctrlmod.ChatMessage(role="user", content="step-2"),
        ctrlmod.ChatMessage(role="assistant", content="reply-2"),
        ctrlmod.ChatMessage(role="user", content="step-3"),
        ctrlmod.ChatMessage(role="assistant", content="reply-3"),
        ctrlmod.ChatMessage(role="user", content="step-4"),
        ctrlmod.ChatMessage(role="assistant", content="reply-4"),
    ]
    messages = prefix + history
    controller.config.compact_target_tokens_for = lambda *_args, **_kwargs: 75

    trimmed, did_trim = controller._trim_message_history(
        messages,
        tool_context,
        current_user_message=ctrlmod.ChatMessage(role="user", content="current-step"),
        step=8,
        compact_trigger_tokens=100,
    )

    assert did_trim is True
    assert trimmed[:2] == prefix
    assert trimmed[2:] == history[-4:]
    assert not (tmp_path / "checkpoint-summary.txt").exists()
    assert controller._checkpoint_required_next_step is True


def test_chunked_history_trim_can_shrink_below_preferred_tail_when_required(
    tmp_path: Path,
) -> None:
    controller = _make_controller()
    controller._trace_runtime = lambda *_args, **_kwargs: None
    controller._approx_message_tokens = lambda messages: sum(
        len(str(message.content)) for message in messages
    )
    tool_context = SimpleNamespace(
        run_dir=tmp_path,
        run_id="run-a",
    )
    prefix = [
        ctrlmod.ChatMessage(role="system", content="system"),
        ctrlmod.ChatMessage(role="user", content="run-ref"),
    ]
    history: list[ctrlmod.ChatMessage] = []
    for idx in range(1, 7):
        history.append(ctrlmod.ChatMessage(role="user", content=f"step-{idx}-" + ("u" * 80)))
        history.append(
            ctrlmod.ChatMessage(role="assistant", content=f"reply-{idx}-" + ("a" * 80))
        )
    messages = prefix + history
    controller.config.compact_target_tokens_for = lambda *_args, **_kwargs: 200

    trimmed, did_trim = controller._trim_message_history(
        messages,
        tool_context,
        current_user_message=ctrlmod.ChatMessage(role="user", content="current-step"),
        step=12,
        compact_trigger_tokens=400,
    )

    assert did_trim is True
    assert trimmed[:2] == prefix
    assert len(trimmed[2:]) < len(history[-8:])
    assert controller._checkpoint_required_next_step is True


def test_delta_packet_omits_unchanged_chunks_after_checkpoint() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "mid",
        "summary": "Stay focused.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "24 months",
        "guidance": "Durability check",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find durable scorers",
    }
    controller._followup_next_action_template_prompt_state = lambda *_args, **_kwargs: None
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand-a"
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: No evaluated attempts yet."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))

    first_prompt, first_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=2,
        step_limit=10,
        prior_results=None,
        checkpoint_required=True,
    )
    controller._commit_sent_step_packet_state(first_state)
    second_prompt, _second_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=3,
        step_limit=10,
        prior_results=None,
    )

    assert "===== GOAL STATE =====" in first_prompt
    assert "===== BRANCH AUTHORITY =====" in first_prompt
    assert "===== ACTIVE HANDLES =====" in first_prompt
    assert "===== STEP FRAME =====" in second_prompt
    assert "===== GOAL STATE =====" not in second_prompt
    assert "===== BRANCH AUTHORITY =====" not in second_prompt
    assert "===== ACTIVE HANDLES =====" not in second_prompt


def test_delta_packet_emits_changed_active_handles_once() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "mid",
        "summary": "Stay focused.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "24 months",
        "guidance": "Durability check",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find durable scorers",
    }
    controller._followup_next_action_template_prompt_state = lambda *_args, **_kwargs: None
    working_memory = {"text": "Pinned working memory:\n- current_candidate: candidate_name=cand-a"}
    controller._working_memory_text = lambda *_args, **_kwargs: working_memory["text"]
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: No evaluated attempts yet."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))

    first_prompt, first_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=2,
        step_limit=10,
        checkpoint_required=True,
    )
    controller._commit_sent_step_packet_state(first_state)
    working_memory["text"] = "Pinned working memory:\n- current_candidate: candidate_name=cand-b"
    second_prompt, _second_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=3,
        step_limit=10,
    )

    assert "candidate_name=cand-a" in first_prompt
    assert "===== ACTIVE HANDLES =====" in second_prompt
    assert "candidate_name=cand-b" in second_prompt


def test_delta_packet_emits_sweep_priority_and_context_after_eval_plateau() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "late",
        "summary": "Pressure-test survivors.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "24 months",
        "guidance": "Durability check",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Pressure-test the current leader",
    }
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand-a, profile_ref=ref-a"
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: No evaluated attempts yet."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    controller._recent_known_instruments_for_handle = (
        lambda *_args, **_kwargs: ["EURUSD"]
    )
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: [
        {
            "step": 10,
            "results": [
                {
                    "tool": "evaluate_candidate",
                    "ok": True,
                    "profile_ref": "ref-a",
                    "requested_horizon_months": 24,
                    "requested_timeframe": "M5",
                    "effective_timeframe": "M5",
                    "validation_outcome": vo.VALIDATION_UNRESOLVED,
                    "coverage_status": "qualified",
                    "score": 54.0,
                }
            ],
        },
        {
            "step": 11,
            "results": [
                {
                    "tool": "evaluate_candidate",
                    "ok": True,
                    "profile_ref": "ref-a",
                    "requested_horizon_months": 24,
                    "requested_timeframe": "M5",
                    "effective_timeframe": "M5",
                    "validation_outcome": vo.VALIDATION_UNRESOLVED,
                    "coverage_status": "qualified",
                    "score": 55.0,
                }
            ],
        },
    ]

    def latest_result(
        _tool_context,
        *,
        tool_names: set[str],
        limit: int = 12,
    ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
        if tool_names == {
            "prepare_profile",
            "mutate_profile",
            "validate_profile",
            "register_profile",
        }:
            return (
                {
                    "tool": "register_profile",
                    "candidate_name": "cand-a",
                    "profile_ref": "ref-a",
                    "candidate_summary": {
                        "candidate_name": "cand-a",
                        "profile_ref": "ref-a",
                        "indicator_ids": [
                            "MACD_CROSSOVER",
                            "BBANDS_POSITION_TREND",
                        ],
                    },
                },
                {},
            )
        if tool_names == {"evaluate_candidate"}:
            return (
                {
                    "tool": "evaluate_candidate",
                    "profile_ref": "ref-a",
                    "score": 55.0,
                },
                {},
            )
        return None, None

    controller._latest_successful_step_result = latest_result
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        evals_dir=Path("C:/runs/example/evals"),
        profiles_dir=Path("C:/runs/example/profiles"),
        seed_prompt_path=None,
        seed_indicator_parameter_hints="",
        instrument_catalog_summary="Use exact catalog symbols only.",
    )

    first_prompt, first_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=12,
        step_limit=50,
        checkpoint_required=True,
    )
    controller._commit_sent_step_packet_state(first_state)
    second_prompt, _second_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=13,
        step_limit=50,
    )

    assert "===== PREPARE OR MUTATE CONTEXT =====" in first_prompt
    assert "mutate_profile is for field-level edits" in first_prompt
    assert "===== SWEEP PRIORITY =====" in first_prompt
    assert "preferred_next_tool: run_parameter_sweep" in first_prompt
    assert "===== SWEEP CONTEXT =====" in first_prompt
    assert "evolutionary for broader discovery across a wider axis set" in first_prompt
    assert "low=100 evals" in first_prompt
    assert "1-2 axes with 5-6 values each" in first_prompt
    assert "indicator[N].config.<field>" in first_prompt
    assert "===== EVALUATE CONTEXT =====" not in first_prompt
    assert "===== PREPARE OR MUTATE CONTEXT =====" not in second_prompt
    assert "===== SWEEP PRIORITY =====" not in second_prompt
    assert "===== SWEEP CONTEXT =====" not in second_prompt


def test_delta_packet_emits_sweep_priority_after_single_credible_mid_phase_eval() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "mid",
        "summary": "Deepen evidence on the strongest families.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "12 months",
        "guidance": "Screen quickly, then refine locally.",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find the best pocket around the current branch",
    }
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand-a, profile_ref=ref-a"
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: One credible branch is active."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    controller._recent_known_instruments_for_handle = (
        lambda *_args, **_kwargs: ["EURUSD"]
    )
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: [
        {
            "step": 10,
            "results": [
                {
                    "tool": "evaluate_candidate",
                    "ok": True,
                    "profile_ref": "ref-a",
                    "requested_horizon_months": 12,
                    "requested_timeframe": "M5",
                    "effective_timeframe": "M5",
                    "validation_outcome": vo.VALIDATION_UNRESOLVED,
                    "coverage_status": "ok",
                    "score": 58.0,
                    "retention_relevant_flags": {
                        "evaluation_mode": "screen",
                    },
                }
            ],
        }
    ]

    def latest_result(
        _tool_context,
        *,
        tool_names: set[str],
        limit: int = 12,
    ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
        if tool_names == {
            "prepare_profile",
            "mutate_profile",
            "validate_profile",
            "register_profile",
        }:
            return (
                {
                    "tool": "register_profile",
                    "candidate_name": "cand-a",
                    "profile_ref": "ref-a",
                    "ready_to_evaluate": True,
                    "candidate_summary": {
                        "candidate_name": "cand-a",
                        "profile_ref": "ref-a",
                        "indicator_ids": ["MACD_CROSSOVER"],
                    },
                },
                {},
            )
        return (None, None)

    controller._latest_successful_step_result = latest_result
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        evals_dir=Path("C:/runs/example/evals"),
        profiles_dir=Path("C:/runs/example/profiles"),
        seed_prompt_path=None,
        seed_indicator_parameter_hints="",
        instrument_catalog_summary="Use exact catalog symbols only.",
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "evaluate_candidate", "profile_ref": "ref-a"},
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a",
            "candidate_name": "cand-a",
            "requested_horizon_months": 12,
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": vo.VALIDATION_UNRESOLVED,
            "coverage_status": "ok",
            "score": 58.0,
            "retention_relevant_flags": {
                "evaluation_mode": "screen",
            },
        },
        step=11,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    first_prompt, _first_state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=12,
        step_limit=50,
        prior_results=[
            {
                "tool": "evaluate_candidate",
                "ok": True,
                "profile_ref": "ref-a",
                "candidate_name": "cand-a",
                "requested_horizon_months": 12,
                "requested_timeframe": "M5",
                "effective_timeframe": "M5",
                "validation_outcome": vo.VALIDATION_UNRESOLVED,
                "coverage_status": "ok",
                "score": 58.0,
                "retention_relevant_flags": {"evaluation_mode": "screen"},
            }
        ],
        checkpoint_required=True,
    )

    assert "===== LOCAL POCKET CADENCE =====" in first_prompt
    assert "stage: probe_local_pocket" in first_prompt
    assert "preferred_move_family: run_parameter_sweep" in first_prompt
    assert "===== SWEEP PRIORITY =====" in first_prompt
    assert "preferred_sweep_mode: deterministic" in first_prompt
    assert "slightly broader neighboring sweep" in first_prompt
    assert "===== SWEEP CONTEXT =====" in first_prompt
    assert "===== EVALUATE CONTEXT =====" not in first_prompt
    assert "- next_action_template:" not in first_prompt
    assert "===== REFLECTION PROMPTS =====" in first_prompt


def test_eval_template_remains_active_when_eval_evidence_changes() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._recent_known_instruments_for_handle = (
        lambda *_args, **_kwargs: ["EURUSD"]
    )
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: [
        {
            "step": 10,
            "results": [
                {
                    "tool": "evaluate_candidate",
                    "ok": True,
                    "profile_ref": "ref-a",
                    "requested_horizon_months": 12,
                    "requested_timeframe": "M5",
                    "effective_timeframe": "M5",
                    "validation_outcome": vo.VALIDATION_UNRESOLVED,
                    "coverage_status": "qualified",
                }
            ],
        },
        {
            "step": 11,
            "results": [
                {
                    "tool": "evaluate_candidate",
                    "ok": True,
                    "profile_ref": "ref-a",
                    "requested_horizon_months": 24,
                    "requested_timeframe": "M5",
                    "effective_timeframe": "M5",
                    "validation_outcome": vo.VALIDATION_UNRESOLVED,
                    "coverage_status": "qualified",
                }
            ],
        },
    ]
    controller._latest_successful_step_result = lambda *_args, **kwargs: (
        {
            "tool": "register_profile",
            "candidate_name": "cand-a",
            "profile_ref": "ref-a",
            "ready_to_evaluate": True,
            "candidate_summary": {
                "candidate_name": "cand-a",
                "profile_ref": "ref-a",
                "indicator_ids": ["MACD_CROSSOVER"],
            },
        },
        {},
    ) if kwargs.get("tool_names") == {
        "inspect_artifact",
        "prepare_profile",
        "mutate_profile",
        "validate_profile",
        "register_profile",
    } else (None, None)
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))

    template = controller._followup_next_action_template_prompt_state(
        tool_context,
        step=40,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    assert template == {
        "tool": "evaluate_candidate",
        "profile_ref": "ref-a",
        "instruments": ["EURUSD"],
        "timeframe_policy": "profile_default",
        "evaluation_mode": "screen",
    }


def test_inspect_artifact_sweep_summary_returns_clone_first_followup(tmp_path: Path) -> None:
    controller = _make_controller()
    controller.cli = SimpleNamespace(score_artifact=lambda *_args, **_kwargs: {})
    tool_context = SimpleNamespace(run_id="run-a", run_dir=tmp_path, evals_dir=tmp_path / "evals")
    artifact_dir = tool_context.evals_dir / "sweep_alpha_20260401"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "sweep-results.json").write_text(
        json.dumps(
            {
                "data": {
                    "fitness_metric": "quality_score",
                    "ranked_permutations": [
                        {
                            "rank": 1,
                            "fitness_value": 57.2705,
                            "parameters": {
                                "indicator[0].config.timeframe": "H1",
                                "indicator[1].talib.timeperiod": 14,
                            },
                            "fitness": {
                                "quality_score_payload": {
                                    "inputs": {"effective_window_months": 11.56}
                                }
                            },
                        }
                    ],
                }
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    controller._load_recent_step_payloads = lambda *_args, **_kwargs: [
        {
            "step": 9,
            "results": [
                {
                    "tool": "run_parameter_sweep",
                    "ok": True,
                    "inspect_ref": artifact_dir.name,
                    "artifact_dir": str(artifact_dir.resolve()),
                    "source_profile_ref": "ref-a",
                    "source_candidate_name": "cand-a",
                    "candidate_name_prefix": "cand_a_sweep",
                }
            ],
        }
    ]

    result = controller._typed_inspect_artifact(
        tool_context,
        {"tool": "inspect_artifact", "inspect_ref": artifact_dir.name, "view": "summary"},
    )

    assert result["next_recommended_action"] == "prepare_profile"
    assert result["recommended_destination_candidate_name"] == "cand_a_sweep_top"
    assert result["recommended_mutations"] == [
        {"path": "indicator[0].config.timeframe", "value": "H1"},
        {"path": "indicator[1].talib.timeperiod", "value": 14},
    ]
    assert result["recommended_followup_actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "clone_local",
            "source_profile_ref": "ref-a",
            "destination_candidate_name": "cand_a_sweep_top",
        },
        {
            "tool": "mutate_profile",
            "candidate_name": "cand_a_sweep_top",
            "mutations": [
                {"path": "indicator[0].config.timeframe", "value": "H1"},
                {"path": "indicator[1].talib.timeperiod", "value": 14},
            ],
        },
        {
            "tool": "validate_profile",
            "candidate_name": "cand_a_sweep_top",
        },
    ]


def test_typed_run_parameter_sweep_forwards_profile_drop_quality_score_preset(tmp_path: Path) -> None:
    controller = _make_controller()
    captured: dict[str, object] = {}

    def fake_execute(
        _tool_context,
        *,
        args,
        cwd,
        step,
        step_limit,
        policy,
        source_action,
        result_tool="run_cli",
    ):
        captured["args"] = list(args)
        return {
            "tool": result_tool,
            "ok": True,
            "result": {
                "parsed_json": {
                    "sweep_id": "sw-1",
                    "ranked": [],
                }
            },
            "artifacts": {},
        }

    controller._execute_cli_invocation = fake_execute
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=tmp_path,
        evals_dir=tmp_path / "evals",
    )
    tool_context.evals_dir.mkdir(parents=True, exist_ok=True)

    result = controller._typed_run_parameter_sweep(
        tool_context,
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": ["indicator[0].talib.timeperiod=10,14,20,26"],
        },
        step=5,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    args = captured["args"]
    assert "--quality-score-preset" in args
    flag_index = args.index("--quality-score-preset") + 1
    assert args[flag_index] == "profile-drop"
    assert result["quality_score_preset"] == "profile-drop"
    assert "--mode" in args
    mode_index = args.index("--mode") + 1
    assert args[mode_index] == "deterministic"
    assert result["mode"] == "deterministic"


def test_typed_run_parameter_sweep_uses_medium_evolutionary_budget_by_default(
    tmp_path: Path,
) -> None:
    controller = _make_controller()
    captured: dict[str, object] = {}

    def fake_execute(
        _tool_context,
        *,
        args,
        cwd,
        step,
        step_limit,
        policy,
        source_action,
        result_tool="run_cli",
    ):
        captured["args"] = list(args)
        return {
            "tool": result_tool,
            "ok": True,
            "result": {
                "parsed_json": {
                    "sweep_id": "sw-evo-default",
                    "ranked": [],
                }
            },
            "artifacts": {},
        }

    controller._execute_cli_invocation = fake_execute
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=tmp_path,
        evals_dir=tmp_path / "evals",
    )
    tool_context.evals_dir.mkdir(parents=True, exist_ok=True)

    result = controller._typed_run_parameter_sweep(
        tool_context,
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": [
                "indicator[0].config.timeframe=M5,M15,M30,H1,H4",
                "indicator[1].talib.timeperiod=8,10,14,20,26",
            ],
            "mode": "evolutionary",
        },
        step=5,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    args = captured["args"]
    assert "--mode" in args
    assert args[args.index("--mode") + 1] == "evolutionary"
    assert "--population-size" in args
    assert args[args.index("--population-size") + 1] == "30"
    assert "--max-generations" in args
    assert args[args.index("--max-generations") + 1] == "10"
    assert result["mode"] == "evolutionary"
    assert result["evolutionary_budget"] == "medium"
    assert result["population_size"] == 30
    assert result["max_generations"] == 10
    assert result["planned_evaluations"] == 300


def test_typed_run_parameter_sweep_maps_evolutionary_budget_preset(tmp_path: Path) -> None:
    controller = _make_controller()
    captured: dict[str, object] = {}

    def fake_execute(
        _tool_context,
        *,
        args,
        cwd,
        step,
        step_limit,
        policy,
        source_action,
        result_tool="run_cli",
    ):
        captured["args"] = list(args)
        return {
            "tool": result_tool,
            "ok": True,
            "result": {
                "parsed_json": {
                    "sweep_id": "sw-evo-1",
                    "ranked": [],
                }
            },
            "artifacts": {},
        }

    controller._execute_cli_invocation = fake_execute
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=tmp_path,
        evals_dir=tmp_path / "evals",
    )
    tool_context.evals_dir.mkdir(parents=True, exist_ok=True)

    result = controller._typed_run_parameter_sweep(
        tool_context,
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": [
                "indicator[0].config.timeframe=M5,M15,M30,H1,H4",
                "indicator[1].talib.timeperiod=8,10,14,20,26",
            ],
            "mode": "evolutionary",
            "evolutionary_budget": "high",
        },
        step=5,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    args = captured["args"]
    assert "--mode" in args
    assert args[args.index("--mode") + 1] == "evolutionary"
    assert "--population-size" in args
    assert args[args.index("--population-size") + 1] == "40"
    assert "--max-generations" in args
    assert args[args.index("--max-generations") + 1] == "12"
    assert result["mode"] == "evolutionary"
    assert result["evolutionary_budget"] == "high"
    assert result["population_size"] == 40
    assert result["max_generations"] == 12
    assert result["planned_evaluations"] == 480


def test_typed_run_parameter_sweep_forwards_custom_evolutionary_controls(
    tmp_path: Path,
) -> None:
    controller = _make_controller()
    captured: dict[str, object] = {}

    def fake_execute(
        _tool_context,
        *,
        args,
        cwd,
        step,
        step_limit,
        policy,
        source_action,
        result_tool="run_cli",
    ):
        captured["args"] = list(args)
        return {
            "tool": result_tool,
            "ok": True,
            "result": {
                "parsed_json": {
                    "sweep_id": "sw-evo-1",
                    "ranked": [],
                }
            },
            "artifacts": {},
        }

    controller._execute_cli_invocation = fake_execute
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=tmp_path,
        evals_dir=tmp_path / "evals",
    )
    tool_context.evals_dir.mkdir(parents=True, exist_ok=True)

    result = controller._typed_run_parameter_sweep(
        tool_context,
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": [
                "indicator[0].config.timeframe=M5,M15,M30,H1,H4",
                "indicator[1].talib.timeperiod=8,10,14,20,26",
            ],
            "mode": "evolutionary",
            "population_size": 25,
            "max_generations": 8,
        },
        step=5,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    args = captured["args"]
    assert "--population-size" in args
    assert args[args.index("--population-size") + 1] == "25"
    assert "--max-generations" in args
    assert args[args.index("--max-generations") + 1] == "8"
    assert "evolutionary_budget" not in result
    assert result["planned_evaluations"] == 200


def test_late_phase_does_not_autostart_local_pocket_cycle() -> None:
    controller = _make_controller()
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))

    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "evaluate_candidate", "profile_ref": "ref-a"},
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a",
            "candidate_name": "cand-a",
            "requested_horizon_months": 12,
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": vo.VALIDATION_UNRESOLVED,
            "coverage_status": "qualified",
            "score": 58.0,
            "retention_relevant_flags": {"evaluation_mode": "screen"},
        },
        step=40,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    assert controller._branch_overlay.local_pocket.stage == bl.LOCAL_POCKET_STAGE_IDLE


def test_first_fertile_mid_phase_sweep_stays_in_probe_and_biases_one_more_neighbor() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        profiles_dir=Path("C:/runs/example/profiles"),
        evals_dir=Path("C:/runs/example/evals"),
    )

    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "evaluate_candidate", "profile_ref": "ref-a"},
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a",
            "candidate_name": "cand-a",
            "requested_horizon_months": 3,
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": vo.VALIDATION_UNRESOLVED,
            "coverage_status": "qualified",
            "score": 58.0,
            "effective_window_months": 2.9,
            "retention_relevant_flags": {"evaluation_mode": "screen"},
        },
        step=10,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "run_parameter_sweep", "profile_ref": "ref-a"},
        {
            "tool": "run_parameter_sweep",
            "ok": True,
            "profile_ref": "ref-a",
            "source_profile_ref": "ref-a",
            "inspect_ref": "sweep-a",
            "axes": ["indicator[0].config.timeframe=M5,H1"],
        },
        step=11,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "inspect_artifact", "inspect_ref": "sweep-a"},
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_kind": "parameter_sweep",
            "artifact_dir": "C:/runs/example/evals/sweep-a",
            "sweep_summary": {
                "top_score": 60.0,
                "top_effective_window_months": 3.1,
                "parameter_importance_flat": False,
            },
            "recommended_destination_candidate_name": "cand_a_top",
            "recommended_mutations": [
                {"path": "indicator[0].config.timeframe", "value": "H1"}
            ],
        },
        step=12,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    pocket = controller._branch_overlay.local_pocket
    cadence = controller._local_pocket_cadence_prompt_state(
        tool_context,
        step=13,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    assert pocket.stage == bl.LOCAL_POCKET_STAGE_PROBE
    assert pocket.last_sweep_fertile is True
    assert pocket.expected_materialized_candidate_name == "cand_a_top"
    assert cadence is not None
    assert cadence["preferred_move_family"] == "run_parameter_sweep"
    assert cadence["destination_candidate_name"] == "cand_a_top"


def test_second_fertile_mid_phase_sweep_transitions_to_materialize() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_PROBE,
        anchor_family_id="fam-a",
        anchor_profile_ref="ref-a",
        anchor_candidate_name="cand-a",
        generation=1,
        sweep_count_used=1,
        sweep_cap=3,
        used_axes=["indicator[0].config.timeframe=M5,H1"],
        last_sweep_inspect_ref="sweep-a",
        last_sweep_fertile=True,
        expected_materialized_candidate_name="cand_a_top",
        expected_materialized_mutations=[
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    )

    controller._mark_local_pocket_sweep_started(
        {
            "tool": "run_parameter_sweep",
            "ok": True,
            "profile_ref": "ref-a",
            "source_profile_ref": "ref-a",
            "inspect_ref": "sweep-b",
            "axes": ["indicator[0].talib.fastperiod=8,12,16"],
        }
    )
    controller._mark_local_pocket_sweep_inspected(
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_kind": "parameter_sweep",
            "artifact_dir": "C:/runs/example/evals/sweep-b",
            "sweep_summary": {
                "top_score": 61.0,
                "top_effective_window_months": 3.2,
                "parameter_importance_flat": False,
            },
            "recommended_destination_candidate_name": "cand_a_top",
            "recommended_mutations": [
                {"path": "indicator[0].talib.fastperiod", "value": 12}
            ],
        },
        phase_name="mid",
    )

    pocket = controller._branch_overlay.local_pocket
    assert pocket.stage == bl.LOCAL_POCKET_STAGE_MATERIALIZE
    assert pocket.expected_materialized_candidate_name == "cand_a_top"
    assert pocket.expected_materialized_mutations == [
        {"path": "indicator[0].talib.fastperiod", "value": 12}
    ]


def test_local_pocket_register_sets_gut_check_due_and_packet_emits_context() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "mid",
        "summary": "Deepen evidence on the strongest families.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "12 months",
        "guidance": "Screen quickly, then refine locally.",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find the best pocket around the current branch",
    }
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand-a, profile_ref=ref-a"
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: One credible branch is active."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    controller._recent_known_instruments_for_handle = (
        lambda *_args, **_kwargs: ["EURUSD"]
    )
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        evals_dir=Path("C:/runs/example/evals"),
        profiles_dir=Path("C:/runs/example/profiles"),
        seed_prompt_path=None,
        seed_indicator_parameter_hints="",
        instrument_catalog_summary="Use exact catalog symbols only.",
    )

    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "evaluate_candidate", "profile_ref": "ref-a"},
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a",
            "candidate_name": "cand-a",
            "requested_horizon_months": 3,
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": vo.VALIDATION_UNRESOLVED,
            "coverage_status": "qualified",
            "score": 58.0,
            "effective_window_months": 2.9,
            "retention_relevant_flags": {"evaluation_mode": "screen"},
        },
        step=10,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "run_parameter_sweep", "profile_ref": "ref-a"},
        {
            "tool": "run_parameter_sweep",
            "ok": True,
            "profile_ref": "ref-a",
            "source_profile_ref": "ref-a",
            "inspect_ref": "sweep-a",
            "axes": ["indicator[0].config.timeframe=M5,H1"],
        },
        step=11,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "inspect_artifact", "inspect_ref": "sweep-a"},
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_kind": "parameter_sweep",
            "sweep_summary": {
                "top_score": 60.0,
                "top_effective_window_months": 3.1,
                "parameter_importance_flat": False,
            },
            "recommended_destination_candidate_name": "cand_a_top",
        },
        step=12,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "run_parameter_sweep", "profile_ref": "ref-a"},
        {
            "tool": "run_parameter_sweep",
            "ok": True,
            "profile_ref": "ref-a",
            "source_profile_ref": "ref-a",
            "inspect_ref": "sweep-b",
            "axes": ["indicator[0].talib.fastperiod=8,12,16"],
        },
        step=13,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )
    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "inspect_artifact", "inspect_ref": "sweep-b"},
        {
            "tool": "inspect_artifact",
            "ok": True,
            "artifact_kind": "parameter_sweep",
            "sweep_summary": {
                "top_score": 61.0,
                "top_effective_window_months": 3.2,
                "parameter_importance_flat": False,
            },
            "recommended_destination_candidate_name": "cand_a_top",
        },
        step=14,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    assert controller._branch_overlay.local_pocket.stage == bl.LOCAL_POCKET_STAGE_MATERIALIZE

    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "register_profile", "candidate_name": "cand_a_top"},
        {
            "tool": "register_profile",
            "ok": True,
            "candidate_name": "cand_a_top",
            "profile_ref": "ref-a-top",
            "ready_to_evaluate": True,
        },
        step=15,
        step_limit=50,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    prompt, _state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=16,
        step_limit=50,
        checkpoint_required=True,
    )

    assert controller._branch_overlay.local_pocket.gut_check_due is True
    assert controller._branch_overlay.local_pocket.stage == bl.LOCAL_POCKET_STAGE_DURABILITY
    assert "===== GUT CHECK CONTEXT =====" in prompt
    assert "target_horizon_months: 12" in prompt
    assert "Durability evidence now takes priority" in prompt
    assert "===== SWEEP PRIORITY =====" not in prompt
    assert "===== SWEEP CONTEXT =====" not in prompt
    assert '"tool": "evaluate_candidate"' not in prompt


def test_local_pocket_validation_blocks_fourth_sweep_and_repeated_axes() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_PROBE,
        anchor_family_id="fam-a",
        anchor_profile_ref="ref-a",
        anchor_candidate_name="cand-a",
        generation=1,
        sweep_count_used=3,
        sweep_cap=3,
        used_axes=["indicator[0].config.timeframe=M5,H1"],
        last_sweep_fertile=True,
    )

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a", run_dir=None),
        actions=[
            {
                "tool": "run_parameter_sweep",
                "profile_ref": "ref-a",
                "axes": ["indicator[0].config.timeframe=M5,H1"],
            }
        ],
        step=20,
        step_limit=80,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert any("same-pocket sweep budget exhausted" in error for error in errors)
    assert any("repeated same-pocket sweep axes are blocked" in error for error in errors)


def test_local_pocket_validation_blocks_additional_same_pocket_sweep_after_flat_result() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_PROBE,
        anchor_family_id="fam-a",
        anchor_profile_ref="ref-a",
        anchor_candidate_name="cand-a",
        generation=1,
        sweep_count_used=1,
        sweep_cap=3,
        used_axes=["indicator[0].config.timeframe=M5,H1"],
        last_sweep_fertile=False,
    )

    errors = controller._validate_branch_lifecycle_actions(
        SimpleNamespace(run_id="run-a", run_dir=None),
        actions=[
            {
                "tool": "run_parameter_sweep",
                "profile_ref": "ref-a",
                "axes": ["indicator[0].talib.fastperiod=8,12,16"],
            }
        ],
        step=20,
        step_limit=80,
        policy=SimpleNamespace(allow_finish=True),
    )

    assert any("did not look fertile" in error for error in errors)


def test_local_pocket_gut_check_success_reopens_probe_cycle_in_mid_phase() -> None:
    controller = _make_controller({"ref-a-top": "fam-a"})
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_DURABILITY,
        anchor_family_id="fam-a",
        anchor_profile_ref="ref-a",
        anchor_candidate_name="cand-a",
        generation=1,
        sweep_count_used=1,
        sweep_cap=3,
        last_materialized_profile_ref="ref-a-top",
        last_materialized_candidate_name="cand_a_top",
        gut_check_due=True,
        gut_check_target_horizon_months=12,
    )
    tool_context = SimpleNamespace(run_id="run-a", run_dir=Path("C:/runs/example"))

    controller._update_local_pocket_after_result(
        tool_context,
        {"tool": "evaluate_candidate", "profile_ref": "ref-a-top"},
        {
            "tool": "evaluate_candidate",
            "ok": True,
            "profile_ref": "ref-a-top",
            "candidate_name": "cand_a_top",
            "requested_horizon_months": 12,
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": vo.VALIDATION_PASSED,
            "coverage_status": "qualified",
            "score": 63.0,
            "effective_window_months": 12.4,
            "retention_relevant_flags": {"evaluation_mode": "validate"},
        },
        step=20,
        step_limit=60,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    pocket = controller._branch_overlay.local_pocket
    assert pocket.stage == bl.LOCAL_POCKET_STAGE_PROBE
    assert pocket.generation == 2
    assert pocket.anchor_profile_ref == "ref-a-top"
    assert pocket.gut_check_due is False


def test_reflection_prompts_do_not_appear_on_ordinary_mechanical_followup() -> None:
    controller = _make_controller()
    controller._run_attempts = lambda _run_id: []
    controller._run_phase_info = lambda *_args, **_kwargs: {
        "name": "mid",
        "summary": "Deepen evidence on the strongest families.",
    }
    controller._horizon_policy_snapshot = lambda *_args, **_kwargs: {
        "summary": "12 months",
        "guidance": "Screen quickly, then refine locally.",
    }
    controller._score_target_snapshot = lambda *_args, **_kwargs: {
        "summary": "Find the best pocket around the current branch",
    }
    controller._working_memory_text = lambda *_args, **_kwargs: (
        "Pinned working memory:\n- current_candidate: candidate_name=cand-a, profile_ref=ref-a"
    )
    controller._run_outcome_text = lambda *_args, **_kwargs: (
        "Run outcome state:\n- official_winner: none yet"
    )
    controller._recent_behavior_digest_text = lambda *_args, **_kwargs: (
        "Behavior digest: One credible branch is active."
    )
    controller._timeframe_mismatch_status_text = (
        lambda *_args, **_kwargs: "Timeframe intent status: No auto-adjustments detected."
    )
    tool_context = SimpleNamespace(
        run_id="run-a",
        run_dir=Path("C:/runs/example"),
        evals_dir=Path("C:/runs/example/evals"),
        profiles_dir=Path("C:/runs/example/profiles"),
        seed_prompt_path=None,
        seed_indicator_parameter_hints="",
        instrument_catalog_summary="Use exact catalog symbols only.",
    )

    prompt, _state = controller._build_step_update_packet(
        tool_context,
        ctrlmod.RunPolicy(mode_name="run"),
        step=8,
        step_limit=50,
        prior_results=[
            {
                "tool": "validate_profile",
                "ok": True,
                "candidate_name": "cand-a",
                "ready_for_registration": True,
            }
        ],
        checkpoint_required=True,
    )

    assert "===== REFLECTION PROMPTS =====" not in prompt


def test_history_append_keeps_completed_turn_only() -> None:
    controller = _make_controller()
    messages: list[ctrlmod.ChatMessage] = []

    controller._append_step_history_messages(
        messages,
        user_packet_content="===== STEP FRAME =====\n- step: 1/5",
        reasoning="Do the next thing.",
        actions=[{"tool": "validate_profile", "candidate_name": "cand-a"}],
    )

    assert [message.role for message in messages] == ["user", "assistant"]


def test_compact_recent_attempts_prompt_uses_existing_trade_count_helper(tmp_path: Path) -> None:
    controller = _make_controller()
    tool_context = SimpleNamespace(run_dir=tmp_path)
    attempts_path = tmp_path / "attempts.jsonl"
    attempts_path.write_text(
        json.dumps(
            {
                "sequence": 1,
                "candidate_name": "cand-a",
                "composite_score": 12.5,
                "best_summary": {
                    "best_cell": {"resolved_trades": 17},
                    "market_data_window": {"effective_window_months": 11.9},
                },
            },
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )

    prompt = controller._compact_recent_attempts_prompt_text(tool_context, limit=2)

    assert "candidate=cand-a" in prompt
    assert "trades=17" in prompt


def test_eval_handle_summary_omits_artifact_path_but_keeps_window() -> None:
    summary = ctrlmod.ResearchController._summarize_eval_handle(
        {
            "attempt_id": "att-1",
            "profile_ref": "ref-1",
            "score": 42.1,
            "effective_window_months": 23.82,
            "artifact_dir": r"C:\runs\example\evals\artifact",
            "next_recommended_action": "inspect_artifact",
        }
    )

    assert summary is not None
    assert "artifact_dir=" not in summary
    assert "window=23.82" in summary


def test_followup_next_action_template_suggests_validate_after_prepare() -> None:
    controller = _make_controller()
    controller._latest_successful_step_result = lambda *_args, **_kwargs: (
        {"tool": "prepare_profile", "candidate_name": "cand-a"},
        {},
    )
    tool_context = SimpleNamespace(run_dir=Path("C:/runs/example"), run_id="run-a")

    template = controller._followup_next_action_template_prompt_state(tool_context)

    assert template == {"tool": "validate_profile", "candidate_name": "cand-a"}


def test_followup_next_action_template_prefers_mutate_after_clone_local_sweep_prepare() -> None:
    controller = _make_controller()
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_MATERIALIZE,
        expected_materialized_candidate_name="cand-a",
        expected_materialized_mutations=[
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    )
    controller._latest_successful_step_result = lambda *_args, **_kwargs: (
        {"tool": "prepare_profile", "candidate_name": "cand-a"},
        {},
    )
    tool_context = SimpleNamespace(run_dir=Path("C:/runs/example"), run_id="run-a")

    template = controller._followup_next_action_template_prompt_state(tool_context)

    assert template == {
        "tool": "mutate_profile",
        "candidate_name": "cand-a",
        "mutations": [
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    }


def test_followup_next_action_template_prefers_mutate_when_probe_stage_keeps_expected_winner() -> None:
    controller = _make_controller()
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_PROBE,
        expected_materialized_candidate_name="cand-a",
        expected_materialized_mutations=[
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    )
    controller._latest_successful_step_result = lambda *_args, **_kwargs: (
        {"tool": "prepare_profile", "candidate_name": "cand-a"},
        {},
    )
    tool_context = SimpleNamespace(run_dir=Path("C:/runs/example"), run_id="run-a")

    template = controller._followup_next_action_template_prompt_state(tool_context)

    assert template == {
        "tool": "mutate_profile",
        "candidate_name": "cand-a",
        "mutations": [
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    }


def test_followup_next_action_template_prefers_prepare_after_sweep_inspect() -> None:
    controller = _make_controller()
    controller._branch_overlay.local_pocket = bl.LocalPocketState(
        stage=bl.LOCAL_POCKET_STAGE_MATERIALIZE,
        anchor_profile_ref="ref-anchor",
        expected_materialized_candidate_name="cand-a",
        expected_materialized_mutations=[
            {"path": "indicator[0].config.timeframe", "value": "H1"}
        ],
    )
    controller._latest_successful_step_result = lambda *_args, **_kwargs: (
        {
            "tool": "inspect_artifact",
            "artifact_kind": "parameter_sweep",
            "recommended_followup_actions": [
                {
                    "tool": "prepare_profile",
                    "mode": "clone_local",
                    "source_profile_ref": "ref-anchor",
                    "destination_candidate_name": "cand-a",
                },
                {
                    "tool": "mutate_profile",
                    "candidate_name": "cand-a",
                    "mutations": [
                        {"path": "indicator[0].config.timeframe", "value": "H1"}
                    ],
                },
            ],
        },
        {},
    )
    tool_context = SimpleNamespace(run_dir=Path("C:/runs/example"), run_id="run-a")

    template = controller._followup_next_action_template_prompt_state(tool_context)

    assert template == {
        "tool": "prepare_profile",
        "mode": "clone_local",
        "source_profile_ref": "ref-anchor",
        "destination_candidate_name": "cand-a",
    }


def test_followup_next_action_template_requires_deterministic_instruments_for_evaluate() -> None:
    controller = _make_controller()
    controller._latest_successful_step_result = lambda *_args, **_kwargs: (
        {
            "tool": "register_profile",
            "candidate_name": "cand-a",
            "profile_ref": "ref-a",
            "ready_to_evaluate": True,
        },
        {},
    )
    tool_context = SimpleNamespace(run_dir=Path("C:/runs/example"), run_id="run-a")

    controller._recent_known_instruments_for_handle = lambda *_args, **_kwargs: None
    assert controller._followup_next_action_template_prompt_state(tool_context) is None

    controller._recent_known_instruments_for_handle = (
        lambda *_args, **_kwargs: ["EURUSD"]
    )
    assert controller._followup_next_action_template_prompt_state(tool_context) == {
        "tool": "evaluate_candidate",
        "profile_ref": "ref-a",
        "instruments": ["EURUSD"],
        "timeframe_policy": "profile_default",
        "evaluation_mode": "screen",
    }


def test_pathless_response_compatibility_rewrites_legacy_profile_fields() -> None:
    controller = _make_controller()

    normalized = controller._pathless_response_compatibility(
        {
            "reasoning": "Use the draft.",
            "actions": [
                {
                    "tool": "validate_profile",
                    "profile_path": r"C:\runs\example\profiles\cand-a.json",
                },
                {
                    "tool": "prepare_profile",
                    "mode": "clone_local",
                    "source_profile_path": r"C:\runs\example\profiles\cand-a.json",
                    "destination_path": r"C:\runs\example\profiles\cand-b.json",
                },
            ],
        }
    )

    assert normalized["actions"] == [
        {
            "tool": "validate_profile",
            "candidate_name": "cand-a",
        },
        {
            "tool": "prepare_profile",
            "mode": "clone_local",
            "source_candidate_name": "cand-a",
            "destination_candidate_name": "cand-b",
        },
    ]


def test_followup_response_canonicalization_overrides_mutate_profile_with_template_mutations() -> None:
    normalized = ctrlmod.canonicalize_followup_step_response(
        {
            "reasoning": "Patch the sweep winner.",
            "actions": [
                {
                    "tool": "validate_profile",
                    "candidate_name": "sw1_top",
                }
            ],
        },
        next_action_template={
            "tool": "mutate_profile",
            "candidate_name": "sw1_top",
            "mutations": [
                {
                    "path": "cc231442-54d5-554c-88a5-395fe850eff5.timeperiod",
                    "value": 30,
                }
            ],
        },
    )

    assert normalized["actions"] == [
        {
            "tool": "mutate_profile",
            "candidate_name": "sw1_top",
            "mutations": [
                {
                    "path": "cc231442-54d5-554c-88a5-395fe850eff5.timeperiod",
                    "value": 30,
                }
            ],
        }
    ]


def test_followup_response_canonicalization_overrides_prepare_profile_with_template_clone() -> None:
    normalized = ctrlmod.canonicalize_followup_step_response(
        {
            "reasoning": "Clone the sweep winner.",
            "actions": [
                {
                    "tool": "prepare_profile",
                    "mode": "clone_local",
                    "source_profile_ref": "ref-anchor",
                    "candidate_name": "candidate",
                    "indicator_ids": ["wrong-extra"],
                }
            ],
        },
        next_action_template={
            "tool": "prepare_profile",
            "mode": "clone_local",
            "source_profile_ref": "ref-anchor",
            "destination_candidate_name": "sw1_top",
        },
    )

    assert normalized["actions"] == [
        {
            "tool": "prepare_profile",
            "mode": "clone_local",
            "source_profile_ref": "ref-anchor",
            "destination_candidate_name": "sw1_top",
        }
    ]


def test_validate_action_accepts_candidate_name_profile_handles() -> None:
    controller = _make_controller()

    assert controller._validate_action(
        {"tool": "validate_profile", "candidate_name": "cand-a"}
    ) is None
    assert controller._validate_action(
        {"tool": "register_profile", "candidate_name": "cand-a", "operation": "create"}
    ) is None
    assert controller._validate_action(
        {
            "tool": "mutate_profile",
            "candidate_name": "cand-a",
            "destination_candidate_name": "cand-b",
            "mutations": [{"path": "profile.name", "value": "cand-b"}],
        }
    ) is None
    assert controller._validate_action(
        {
            "tool": "evaluate_candidate",
            "candidate_name": "cand-a",
            "instruments": ["EURUSD"],
        }
    ) is None
    assert controller._validate_action(
        {"tool": "inspect_artifact", "inspect_ref": "sweep_alpha_20260401"}
    ) is None


def test_validate_action_accepts_evolutionary_budget_preset() -> None:
    controller = _make_controller()

    assert controller._validate_action(
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": ["indicator[0].config.timeframe=M5,M15,M30,H1,H4"],
            "mode": "evolutionary",
            "evolutionary_budget": "med",
        }
    ) is None


def test_validate_action_rejects_mixed_evolutionary_budget_controls() -> None:
    controller = _make_controller()

    assert controller._validate_action(
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": ["indicator[0].config.timeframe=M5,M15,M30,H1,H4"],
            "mode": "evolutionary",
            "evolutionary_budget": "high",
            "population_size": 40,
            "max_generations": 12,
        }
    ) == (
        "run_parameter_sweep use either evolutionary_budget or "
        "population_size/max_generations, not both."
    )


def test_validate_action_rejects_oversized_custom_evolutionary_budget() -> None:
    controller = _make_controller()

    assert controller._validate_action(
        {
            "tool": "run_parameter_sweep",
            "profile_ref": "ref-a",
            "axes": ["indicator[0].config.timeframe=M5,M15,M30,H1,H4"],
            "mode": "evolutionary",
            "population_size": 50,
            "max_generations": 11,
        }
    ) == (
        "run_parameter_sweep evolutionary budget exceeds autoresearch cap "
        "of 500 evaluations."
    )


def test_resolve_artifact_path_accepts_inspect_ref(tmp_path: Path) -> None:
    controller = _make_controller()
    tool_context = SimpleNamespace(run_dir=tmp_path, evals_dir=tmp_path / "evals")
    sweep_dir = tool_context.evals_dir / "sweep_alpha_20260401"
    sweep_dir.mkdir(parents=True)

    resolved = controller._resolve_artifact_path(
        tool_context,
        {"inspect_ref": "sweep_alpha_20260401"},
    )

    assert resolved == sweep_dir.resolve()


def test_clone_local_prepare_profile_preserves_destination_candidate_name(
    tmp_path: Path,
) -> None:
    controller = _make_controller()
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    source = profiles_dir / "source-a.json"
    source.write_text(
        json.dumps(
            {
                "profile": {
                    "name": "source-a",
                    "indicators": [
                        {"meta": {"id": "MACD_CROSSOVER", "instanceId": "macd-1"}}
                    ],
                    "instruments": ["EURUSD"],
                }
            },
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_execute_cli_invocation(
        _tool_context,
        *,
        args,
        cwd,
        step,
        step_limit,
        policy,
        source_action,
        result_tool,
    ):
        out_path = Path(args[args.index("--out") + 1])
        out_path.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        return {
            "tool": result_tool,
            "ok": True,
            "result": {"argv": args, "returncode": 0, "parsed_json": {}},
            "warnings": [],
            "errors": [],
            "artifacts": {},
            "state_updates": {},
            "status": "ok",
        }

    controller._execute_cli_invocation = fake_execute_cli_invocation
    tool_context = SimpleNamespace(
        run_dir=tmp_path,
        profiles_dir=profiles_dir,
        evals_dir=tmp_path / "evals",
    )

    result = controller._typed_prepare_profile(
        tool_context,
        {
            "tool": "prepare_profile",
            "mode": "clone_local",
            "source_candidate_name": "source-a",
            "destination_candidate_name": "cand-b",
        },
        step=1,
        step_limit=10,
        policy=ctrlmod.RunPolicy(mode_name="run"),
    )

    assert result["candidate_name"] == "cand-b"
    assert result["profile_name"] == "cand-b"
    assert result["candidate_summary"]["candidate_name"] == "cand-b"


def test_prompt_visible_action_signature_strips_profile_paths() -> None:
    controller = _make_controller()

    normalized = controller._prompt_visible_action_signature(
        {
            "tool": "mutate_profile",
            "profile_path": r"C:\runs\example\profiles\cand-a.json",
            "destination_path": r"C:\runs\example\profiles\cand-b.json",
            "mutations": [{"path": "profile.name", "value": "cand-b"}],
        }
    )

    assert normalized == {
        "tool": "mutate_profile",
        "candidate_name": "cand-a",
        "destination_candidate_name": "cand-b",
        "mutations": [{"path": "profile.name", "value": "cand-b"}],
    }


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


def test_current_wrap_up_focus_prefers_best_steering_sibling_not_raw_spike() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
            best_attempt_id="attempt-best",
            latest_attempt_id="attempt-latest",
            last_profile_ref="ref-a",
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
                name="best-focus",
                profile_ref="ref-a",
                score=50.274,
                horizon_months=24,
                effective_window_months=22.67,
                trades_per_month=1.191001,
                resolved_trades=27,
                positive_cell_ratio=0.7344,
            ),
            "attempt_id": "attempt-best",
        },
        {
            **_make_attempt(
                sequence=2,
                name="latest-weaker",
                profile_ref="ref-a",
                score=41.5354,
                horizon_months=24,
                effective_window_months=23.53,
                trades_per_month=6.544836,
                resolved_trades=154,
                positive_cell_ratio=0.375,
            ),
            "attempt_id": "attempt-latest",
        },
    ]

    focus = controller._current_wrap_up_focus_state(attempts)

    assert focus is not None
    assert focus["family_id"] == "fam-a"
    assert focus["candidate_name"] == "latest-weaker"
    assert focus["selected_attempt_id"] == "attempt-latest"
    assert focus["latest_attempt_id"] == "attempt-latest"
    assert focus["requested_horizon_months"] == 24
    assert focus["requested_timeframe"] == "M15"
    assert focus["effective_timeframe"] == "M15"


def test_select_best_attempt_snapshot_prefers_steering_score_within_retryable_set() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    snapshots = [
        controller._attempt_admissibility_snapshot(
            {
                **_make_attempt(
                    sequence=1,
                    name="raw-spike",
                    profile_ref="ref-a",
                    score=50.274,
                    horizon_months=24,
                    effective_window_months=22.67,
                    trades_per_month=1.191001,
                    resolved_trades=27,
                    positive_cell_ratio=0.7344,
                ),
                "attempt_id": "attempt-best",
            }
        ),
        controller._attempt_admissibility_snapshot(
            {
                **_make_attempt(
                    sequence=2,
                    name="durable-sibling",
                    profile_ref="ref-a",
                    score=41.5354,
                    horizon_months=24,
                    effective_window_months=23.53,
                    trades_per_month=6.544836,
                    resolved_trades=154,
                    positive_cell_ratio=0.375,
                ),
                "attempt_id": "attempt-latest",
            }
        ),
    ]

    best = controller._select_best_attempt_snapshot(
        [snap for snap in snapshots if isinstance(snap, dict)],
        prefer_highest_horizon=True,
    )

    assert best is not None
    assert best["candidate_name"] == "durable-sibling"


def test_run_outcome_snapshot_separates_live_focus_from_historical_validated() -> None:
    controller = _make_controller({"ref-a": "fam-a", "ref-b": "fam-b"})
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_COLLAPSED,
            promotion_level=bl.PROMOTION_VALIDATED,
            promotability_status=vo.PROMOTABILITY_BLOCKED,
            retention_status=bl.RETENTION_FAILED,
            exploit_dead=True,
            best_attempt_id="attempt-validated",
            latest_attempt_id="attempt-validated",
        ),
        "fam-b": bl.FamilyBranchState(
            family_id="fam-b",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotion_level=bl.PROMOTION_PROVISIONAL,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
            best_attempt_id="attempt-live",
            latest_attempt_id="attempt-live",
            last_profile_ref="ref-b",
        ),
    }
    controller._branch_overlay.provisional_leader_family_id = "fam-b"
    attempts = [
        {
            **_make_attempt(
                sequence=1,
                name="historic-validated",
                profile_ref="ref-a",
                score=61.0,
                horizon_months=24,
                effective_window_months=23.0,
                trades_per_month=4.0,
                resolved_trades=92,
                validation_outcome=vo.VALIDATION_PASSED,
            ),
            "attempt_id": "attempt-validated",
        },
        {
            **_make_attempt(
                sequence=2,
                name="live-focus",
                profile_ref="ref-b",
                score=14.0,
                horizon_months=36,
                effective_window_months=35.0,
                trades_per_month=7.0,
                resolved_trades=240,
            ),
            "attempt_id": "attempt-live",
        },
    ]

    outcome = controller._run_outcome_snapshot(attempts)

    assert outcome["official_winner_type"] == "none"
    assert outcome["official_winner"] is None
    assert outcome["best_live_focus"]["attempt_id"] == "attempt-live"
    assert outcome["best_historical_validated"]["attempt_id"] == "attempt-validated"
    assert outcome["best_historical_validated"]["currently_live"] is False
    assert "No official winner" in outcome["rationale"]


def test_build_branch_runtime_snapshot_includes_run_outcome() -> None:
    controller = _make_controller({"ref-a": "fam-a"})
    controller._tool_usage_counts = {}
    controller._run_attempts = lambda _run_id: [
        {
            **_make_attempt(
                sequence=1,
                name="live-focus",
                profile_ref="ref-a",
                score=18.0,
                horizon_months=24,
                effective_window_months=23.0,
                trades_per_month=5.0,
                resolved_trades=120,
            ),
            "attempt_id": "attempt-live",
        }
    ]
    controller._family_branches = {
        "fam-a": bl.FamilyBranchState(
            family_id="fam-a",
            lifecycle_state=bl.LIFECYCLE_PROVISIONAL_LEADER,
            promotion_level=bl.PROMOTION_PROVISIONAL,
            promotability_status=vo.PROMOTABILITY_RETRY_RECOMMENDED,
            retention_status=bl.RETENTION_PASSED,
            best_attempt_id="attempt-live",
            latest_attempt_id="attempt-live",
            last_profile_ref="ref-a",
        )
    }
    controller._branch_overlay.provisional_leader_family_id = "fam-a"
    tool_context = SimpleNamespace(run_id="run-a")

    snapshot = controller._build_branch_runtime_snapshot(tool_context, step=42)

    assert snapshot["run_outcome"]["official_winner_type"] == "none"
    assert snapshot["run_outcome"]["best_live_focus"]["attempt_id"] == "attempt-live"
    assert snapshot["wrap_up_focus"]["selected_attempt_id"] == "attempt-live"


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


def test_trace_runtime_serializes_exception_fields(tmp_path) -> None:
    controller = object.__new__(ResearchController)
    tool_context = SimpleNamespace(run_dir=tmp_path, run_id="run-a")

    controller._trace_runtime(
        tool_context,
        step=79,
        phase="response_repair",
        status="failed",
        message="Response repair failed.",
        error=RuntimeError("Model returned invalid actions payload"),
        details={"nested": ValueError("bad field")},
    )

    trace_path = tmp_path / "runtime-trace.jsonl"
    row = json.loads(trace_path.read_text(encoding="utf-8").strip())
    assert row["error"] == "RuntimeError: Model returned invalid actions payload"
    assert row["details"]["nested"] == "ValueError: bad field"


def test_maybe_auto_log_attempt_forwards_requested_horizon() -> None:
    controller = _make_controller()
    captured: dict[str, object] = {}

    def fake_record_attempt(tool_context, artifact_dir, **kwargs):
        captured["artifact_dir"] = artifact_dir
        captured.update(kwargs)
        return {"status": "logged"}

    controller._record_attempt_from_artifact = fake_record_attempt

    result = controller._maybe_auto_log_attempt(
        SimpleNamespace(attempts_path=Path("attempts.jsonl"), run_id="run-a"),
        [
            "sensitivity-basket",
            "--profile-ref",
            "ref-a",
            "--output-dir",
            "C:\\runs\\eval-a",
            "--lookback-months",
            "24",
        ],
    )

    assert result == {"status": "logged"}
    assert captured["profile_ref"] == "ref-a"
    assert captured["requested_horizon_months"] == 24


def test_record_attempt_from_artifact_persists_explicit_requested_horizon(
    tmp_path, monkeypatch
) -> None:
    controller = _make_controller()
    controller._render_run_progress = lambda _tool_context: None
    controller.profile_sources = {}
    controller.cli = SimpleNamespace(score_artifact=lambda _artifact_dir: {"best": {}})

    run_dir = tmp_path / "run"
    artifact_dir = run_dir / "evals" / "eval_case"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "sensitivity-response.json").write_text("{}", encoding="utf-8")
    tool_context = SimpleNamespace(
        attempts_path=run_dir / "attempts.jsonl",
        run_id="run-a",
        progress_plot_path=run_dir / "progress.png",
    )

    monkeypatch.setattr(ctrlmod, "attempt_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(ctrlmod, "load_sensitivity_snapshot", lambda _artifact_dir: {})
    monkeypatch.setattr(
        ctrlmod,
        "build_attempt_score",
        lambda *_args, **_kwargs: AttemptScore(
            primary_score=12.3,
            composite_score=12.3,
            score_basis="v1:psr",
            metrics={"quality_score": 12.3},
            best_summary={},
        ),
    )
    monkeypatch.setattr(
        controller,
        "_normalized_attempt_record_evidence",
        lambda *_args, **_kwargs: {
            "requested_horizon_months": None,
            "effective_window_months": 11.5,
            "effective_window_source": "test",
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": None,
            "coverage_status": "ok",
            "job_status": None,
            "resolved_trades": 50,
            "trades_per_month": 4.0,
            "positive_cell_ratio": 0.5,
        },
    )

    captured: dict[str, object] = {}

    def fake_make_attempt_record(*args, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            attempt_id="run-a-attempt-00001",
            sequence=1,
            created_at="2026-04-02T00:00:00+00:00",
            run_id="run-a",
            candidate_name="eval_case",
            artifact_dir=str(artifact_dir),
            profile_ref="ref-a",
            profile_path=None,
            primary_score=12.3,
            composite_score=12.3,
            score_basis="v1:psr",
            metrics={"quality_score": 12.3},
            best_summary={},
            sensitivity_snapshot_path=None,
            requested_horizon_months=kwargs.get("requested_horizon_months"),
            effective_window_months=11.5,
            requested_timeframe="M5",
            effective_timeframe="M5",
            validation_outcome=None,
            coverage_status="ok",
            job_status=None,
            resolved_trades=50,
            trades_per_month=4.0,
            positive_cell_ratio=0.5,
            effective_window_source="test",
        )

    monkeypatch.setattr(ctrlmod, "make_attempt_record", fake_make_attempt_record)
    monkeypatch.setattr(ctrlmod, "append_attempt", lambda *_args, **_kwargs: None)

    controller._record_attempt_from_artifact(
        tool_context,
        artifact_dir,
        profile_ref="ref-a",
        requested_horizon_months=24,
    )

    assert captured["requested_horizon_months"] == 24


def test_record_attempt_from_artifact_resolves_relative_sweep_dir_against_evals_dir(
    tmp_path, monkeypatch
) -> None:
    controller = _make_controller()
    controller._render_run_progress = lambda _tool_context: None
    controller.profile_sources = {}

    run_dir = tmp_path / "run"
    evals_dir = run_dir / "evals"
    artifact_dir = evals_dir / "sweep_sw_final_20260413T182240158728Z"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "sensitivity-response.json").write_text("{}", encoding="utf-8")

    score_calls: list[Path] = []

    def fake_score_artifact(path: Path) -> dict[str, object]:
        score_calls.append(path)
        return {"best": {}}

    controller.cli = SimpleNamespace(score_artifact=fake_score_artifact)
    tool_context = SimpleNamespace(
        attempts_path=run_dir / "attempts.jsonl",
        run_id="run-a",
        progress_plot_path=run_dir / "progress.png",
        evals_dir=evals_dir,
        run_dir=run_dir,
    )

    monkeypatch.setattr(ctrlmod, "attempt_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(ctrlmod, "load_sensitivity_snapshot", lambda _artifact_dir: {})
    monkeypatch.setattr(
        ctrlmod,
        "build_attempt_score",
        lambda *_args, **_kwargs: AttemptScore(
            primary_score=12.3,
            composite_score=12.3,
            score_basis="v1:psr",
            metrics={"quality_score": 12.3},
            best_summary={},
        ),
    )
    monkeypatch.setattr(
        controller,
        "_normalized_attempt_record_evidence",
        lambda *_args, **_kwargs: {
            "requested_horizon_months": None,
            "effective_window_months": 11.5,
            "effective_window_source": "test",
            "requested_timeframe": "M5",
            "effective_timeframe": "M5",
            "validation_outcome": None,
            "coverage_status": "ok",
            "job_status": None,
            "resolved_trades": 50,
            "trades_per_month": 4.0,
            "positive_cell_ratio": 0.5,
        },
    )

    monkeypatch.setattr(
        ctrlmod,
        "make_attempt_record",
        lambda *args, **kwargs: SimpleNamespace(
            attempt_id="run-a-attempt-00001",
            sequence=1,
            created_at="2026-04-02T00:00:00+00:00",
            run_id="run-a",
            candidate_name="eval_case",
            artifact_dir=str(artifact_dir),
            profile_ref="ref-a",
            profile_path=None,
            primary_score=12.3,
            composite_score=12.3,
            score_basis="v1:psr",
            metrics={"quality_score": 12.3},
            best_summary={},
            sensitivity_snapshot_path=None,
            requested_horizon_months=kwargs.get("requested_horizon_months"),
            effective_window_months=11.5,
            requested_timeframe="M5",
            effective_timeframe="M5",
            validation_outcome=None,
            coverage_status="ok",
            job_status=None,
            resolved_trades=50,
            trades_per_month=4.0,
            positive_cell_ratio=0.5,
            effective_window_source="test",
        ),
    )
    monkeypatch.setattr(ctrlmod, "append_attempt", lambda *_args, **_kwargs: None)

    controller._record_attempt_from_artifact(
        tool_context,
        Path(artifact_dir.name),
        profile_ref="ref-a",
        requested_horizon_months=24,
    )

    assert score_calls == [artifact_dir]
