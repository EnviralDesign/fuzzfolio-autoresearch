"""Apply deterministic labels, recovery classes, and keep/discard heuristics."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION
from .rules import (
    NON_TYPED_TOOLS,
    RECOVERY_FAILURE_CLASSES,
    TYPED_TOOLS,
    action_tools,
    current_first_tool,
    current_result_facts,
    current_result_tools,
    deterministic_followup_target,
    has_marker,
    is_read_or_list_only,
    is_run_cli_only,
    is_typed_tool_only,
    latest_prior_had_error,
    latest_prior_timeframe_mismatch,
    prompt_timeframe_repeat_blocked,
    repeated_browse_loop,
)
from .validators import has_non_empty_reasoning, has_valid_target_response, known_action_tools

ALL_KNOWN_TOOLS = TYPED_TOOLS | NON_TYPED_TOOLS


def _tool_has_required_fields(tool: str, action: dict[str, Any]) -> bool:
    if tool == "prepare_profile":
        mode = str(action.get("mode") or "").strip()
        if not mode:
            return False
        if mode == "clone_local":
            return bool(action.get("source_candidate_name") or action.get("source_profile_ref"))
        return bool(action.get("candidate_name"))
    if tool == "mutate_profile":
        return bool(action.get("candidate_name") or action.get("profile_ref")) and bool(
            action.get("mutations")
        )
    if tool == "validate_profile":
        return bool(action.get("candidate_name") or action.get("profile_ref"))
    if tool == "register_profile":
        return bool(action.get("candidate_name") or action.get("profile_ref"))
    if tool == "evaluate_candidate":
        return bool(action.get("profile_ref") or action.get("candidate_name"))
    if tool == "run_parameter_sweep":
        axes = action.get("axes")
        return bool(action.get("profile_ref")) and isinstance(axes, list) and bool(axes)
    if tool == "inspect_artifact":
        return bool(action.get("artifact_dir") or action.get("attempt_id"))
    if tool == "compare_artifacts":
        return bool(action.get("left_artifact_dir") or action.get("artifact_dir") or action.get("attempt_id"))
    if tool == "finish":
        return bool(str(action.get("summary") or "").strip())
    if tool == "read_file":
        return bool(action.get("path"))
    if tool == "list_dir":
        return bool(action.get("path"))
    if tool == "write_file":
        return bool(action.get("path"))
    if tool == "run_cli":
        args = action.get("args")
        return isinstance(args, list) and bool(args)
    return False


def _required_fields_present(record: dict[str, Any]) -> bool:
    actions = record.get("target_actions")
    if not isinstance(actions, list) or not actions:
        return False
    for action in actions:
        if not isinstance(action, dict):
            return False
        tool = str(action.get("tool") or "").strip()
        if not tool or not _tool_has_required_fields(tool, action):
            return False
    return True


def _missing_required_field(record: dict[str, Any]) -> bool:
    result_facts = current_result_facts(record)
    for fact in result_facts:
        errors = fact.get("errors")
        if isinstance(errors, list) and any("requires" in str(item).lower() for item in errors):
            return True
        error = str(fact.get("error") or "").lower()
        if "requires" in error:
            return True
    return False


def _invalid_cli_family(record: dict[str, Any]) -> bool:
    if not is_run_cli_only(record):
        return False
    for fact in current_result_facts(record):
        error = str(fact.get("error") or "").lower()
        if "unknown" in error or "unsupported" in error or "invalid" in error:
            return True
    return False


def _profile_handle_resolution_error(record: dict[str, Any]) -> bool:
    for fact in current_result_facts(record):
        resolution = fact.get("artifact_resolution")
        if isinstance(resolution, dict):
            if "mismatch" in str(resolution.get("resolution") or "").lower():
                return True
        error = str(fact.get("error") or "").lower()
        if "profile_ref" in error or "candidate_name" in error or "artifact_dir" in error:
            return True
    return False


def _finish_denied(record: dict[str, Any]) -> bool:
    return "yield_guard" in current_result_tools(record) or any(
        fact.get("phase") == "finish" and str(fact.get("status") or "") == "denied"
        for fact in (record.get("trace_event_facts") or [])
        if isinstance(fact, dict)
    )


def _timeframe_repeat_block(record: dict[str, Any]) -> bool:
    if not prompt_timeframe_repeat_blocked(record):
        return False
    mismatch = latest_prior_timeframe_mismatch(record)
    if not isinstance(mismatch, dict):
        return False
    requested = str(mismatch.get("requested") or "").strip().lower()
    if not requested:
        return False
    for action in record.get("target_actions") or []:
        if not isinstance(action, dict):
            continue
        tool = str(action.get("tool") or "").strip()
        if tool == "evaluate_candidate":
            timeframe = str(action.get("timeframe") or "").strip().lower()
            timeframe_policy = str(action.get("timeframe_policy") or "").strip().lower()
            if timeframe_policy == "explicit" and timeframe == requested:
                return True
        if tool == "run_cli":
            args = " ".join(str(item).lower() for item in action.get("args") or [])
            if requested in args:
                return True
    return False


def _wrong_tool_for_state(record: dict[str, Any]) -> bool:
    target = deterministic_followup_target(record)
    current = current_first_tool(record)
    return bool(target and current and target != current)


def _overuse_of_read_file(record: dict[str, Any]) -> bool:
    if not is_read_or_list_only(record):
        return False
    if repeated_browse_loop(record):
        return True
    prior_target = deterministic_followup_target(record)
    return prior_target in {"inspect_artifact", "compare_artifacts"}


def _repeated_stall(record: dict[str, Any]) -> bool:
    if repeated_browse_loop(record):
        return True
    return has_marker(record, "response_guard_blocked") or has_marker(record, "step_guard_triggered")


def _mechanical_score(record: dict[str, Any]) -> int:
    score = 0
    if has_valid_target_response(record):
        score += 1
    if known_action_tools(record, ALL_KNOWN_TOOLS):
        score += 1
    if _required_fields_present(record):
        score += 1
    if not has_marker(record, "response_repair_triggered") and not has_marker(
        record, "payload_shape_repair_triggered"
    ):
        score += 1
    if not is_run_cli_only(record):
        score += 1
    return score


def _policy_score(record: dict[str, Any]) -> int:
    score = 0
    if is_typed_tool_only(record):
        score += 2
    if deterministic_followup_target(record) == current_first_tool(record):
        score += 2
    if not _timeframe_repeat_block(record) and not _overuse_of_read_file(record):
        score += 1
    return min(score, 5)


def _recovery_score(record: dict[str, Any]) -> int:
    score = 0
    if latest_prior_had_error(record):
        score += 2
    if not _wrong_tool_for_state(record):
        score += 1
    if not _timeframe_repeat_block(record):
        score += 1
    if not repeated_browse_loop(record):
        score += 1
    return min(score, 5)


def _outcome_score(record: dict[str, Any]) -> int:
    score = 0
    for fact in current_result_facts(record):
        if fact.get("ready_for_registration"):
            score += 2
        if fact.get("ready_to_evaluate"):
            score += 2
        if fact.get("score") is not None or fact.get("attempt_id"):
            score += 1
        if fact.get("artifact_kind") == "parameter_sweep":
            score += 1
        if fact.get("suggested_next_move"):
            score += 1
    return min(score, 5)


def _grade(mechanical: int, policy: int, recovery: int, outcome: int, keep_base_sft: bool) -> str:
    total = mechanical + policy + recovery + outcome
    if keep_base_sft and total >= 13:
        return "A"
    if keep_base_sft and total >= 10:
        return "B"
    if total >= 7:
        return "C"
    if total >= 4:
        return "D"
    return "F"


def label_record(record: dict[str, Any]) -> dict[str, Any]:
    labeled = dict(record)
    actions = action_tools(record)
    deterministic_target = deterministic_followup_target(record)
    has_reasoning = has_non_empty_reasoning(record)
    recovery_failure_classes: list[str] = []
    if has_marker(record, "response_repair_triggered") or has_marker(
        record, "payload_shape_repair_triggered"
    ):
        recovery_failure_classes.append("invalidjsonshape")
    if _missing_required_field(record):
        recovery_failure_classes.append("missingrequiredfield")
    if _wrong_tool_for_state(record):
        recovery_failure_classes.append("wrongtoolfor_state")
    if _invalid_cli_family(record):
        recovery_failure_classes.append("invalidclifamilyorsubcommand")
    if _profile_handle_resolution_error(record):
        recovery_failure_classes.append("profilehandleresolution_error")
    if _timeframe_repeat_block(record):
        recovery_failure_classes.append("timeframerepeatblock")
    if _finish_denied(record):
        recovery_failure_classes.append("finish_denied")
    if _repeated_stall(record):
        recovery_failure_classes.append("repeated_stall")
    if _overuse_of_read_file(record):
        recovery_failure_classes.append("overuseofread_file")
    recovery_failure_classes = [
        item for item in RECOVERY_FAILURE_CLASSES if item in set(recovery_failure_classes)
    ]

    mechanical_labels = {
        "valid_json_shape": has_valid_target_response(record),
        "known_tool_names": known_action_tools(record, ALL_KNOWN_TOOLS),
        "required_fields_present": _required_fields_present(record),
        "typed_tool_only": is_typed_tool_only(record),
        "run_cli_used": "run_cli" in actions,
        "repair_needed": has_marker(record, "response_repair_triggered")
        or has_marker(record, "payload_shape_repair_triggered"),
        "arg_format_ok": True,
    }
    policy_labels = {
        "controller_admissible": not has_marker(record, "response_guard_blocked")
        and not has_marker(record, "step_guard_triggered"),
        "deterministic_followup_target": deterministic_target,
        "deterministic_followup_matched": bool(
            deterministic_target and deterministic_target == current_first_tool(record)
        ),
        "phase_appropriate": current_first_tool(record) != "finish" or not _finish_denied(record),
        "inspect_compare_preferred": not _overuse_of_read_file(record),
        "timeframe_repeat_blocked": _timeframe_repeat_block(record),
        "fallback_run_cli_justified": bool("run_cli" not in actions or latest_prior_had_error(record)),
    }
    recovery_labels = {
        "is_recovery_example": bool(recovery_failure_classes) or latest_prior_had_error(record),
        "failure_classes": recovery_failure_classes,
        "prior_error_visible": latest_prior_had_error(record),
        "one_step_recovery_candidate": bool(
            latest_prior_had_error(record)
            and deterministic_target
            and deterministic_target == current_first_tool(record)
        ),
    }
    outcome_labels = {
        "advances_state": any(
            fact.get("ready_for_registration")
            or fact.get("ready_to_evaluate")
            or fact.get("attempt_id")
            or fact.get("score") is not None
            for fact in current_result_facts(record)
        ),
        "registered_candidate": any(
            str(fact.get("tool") or "") == "register_profile" and bool(fact.get("ok"))
            for fact in current_result_facts(record)
        ),
        "validated_candidate": any(
            str(fact.get("tool") or "") == "validate_profile" and bool(fact.get("ok"))
            for fact in current_result_facts(record)
        ),
        "scored_candidate": any(
            str(fact.get("tool") or "") == "evaluate_candidate"
            and (fact.get("score") is not None or fact.get("attempt_id"))
            for fact in current_result_facts(record)
        ),
        "stall": _repeated_stall(record),
        "dead_end": bool(
            has_marker(record, "hard_action_failure") and not latest_prior_had_error(record)
        ),
    }

    keep_base_sft = (
        mechanical_labels["valid_json_shape"]
        and mechanical_labels["known_tool_names"]
        and mechanical_labels["required_fields_present"]
        and policy_labels["controller_admissible"]
        and is_typed_tool_only(record)
        and not recovery_labels["is_recovery_example"]
        and not is_run_cli_only(record)
        and not is_read_or_list_only(record)
        and (
            has_reasoning
            or policy_labels["deterministic_followup_matched"]
        )
    )
    keep_recovery = bool(recovery_failure_classes) or recovery_labels["one_step_recovery_candidate"]
    rejection_reasons: list[str] = []
    if not keep_base_sft:
        if is_run_cli_only(record):
            rejection_reasons.append("run_cli_policy_step")
        if is_read_or_list_only(record):
            rejection_reasons.append("browse_only_step")
        if not has_reasoning and not policy_labels["deterministic_followup_matched"]:
            rejection_reasons.append("empty_reasoning")
        if recovery_labels["is_recovery_example"]:
            rejection_reasons.append("recovery_or_failure_state")
        if not policy_labels["controller_admissible"]:
            rejection_reasons.append("controller_blocked")
        if has_marker(record, "hard_action_failure"):
            rejection_reasons.append("hard_action_failure")
    if keep_recovery and "recovery_or_failure_state" not in rejection_reasons:
        rejection_reasons.append("recovery_or_failure_state")

    mechanical_score = _mechanical_score(record)
    policy_score = _policy_score(record)
    recovery_score = _recovery_score(record)
    outcome_score = _outcome_score(record)
    grade = _grade(mechanical_score, policy_score, recovery_score, outcome_score, keep_base_sft)

    labeled["sourcetype"] = record.get("sourcetype") or record.get("source_type") or "realrun"
    labeled["mechanical_labels"] = mechanical_labels
    labeled["policy_labels"] = policy_labels
    labeled["recovery_labels"] = recovery_labels
    labeled["outcome_labels"] = outcome_labels
    labeled["quality_labels"] = {
        "mechanical_score": mechanical_score,
        "policy_score": policy_score,
        "recovery_score": recovery_score,
        "outcome_contribution_score": outcome_score,
        "grade": grade,
        "keep_for_base_sft": keep_base_sft,
        "keep_for_recovery": keep_recovery,
    }
    labeled["label_sources"] = ["controller-log", "runtime-trace", "deterministic-rules-v0"]
    labeled["rejection_reasons"] = sorted(set(rejection_reasons))
    return labeled


def label_file(input_path: Path, output_path: Path, summary_path: Path | None = None) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    recovery_counts = Counter()
    with input_path.open("r", encoding="utf-8") as source, output_path.open(
        "w", encoding="utf-8"
    ) as sink:
        for line in source:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                continue
            labeled = label_record(record)
            quality = labeled.get("quality_labels") if isinstance(labeled.get("quality_labels"), dict) else {}
            counts["records"] += 1
            counts[f"grade_{quality.get('grade', 'unknown')}"] += 1
            if quality.get("keep_for_base_sft"):
                counts["keep_for_base_sft"] += 1
            if quality.get("keep_for_recovery"):
                counts["keep_for_recovery"] += 1
            recovery = labeled.get("recovery_labels")
            if isinstance(recovery, dict):
                for item in recovery.get("failure_classes") or []:
                    recovery_counts[str(item)] += 1
            sink.write(json.dumps(labeled, ensure_ascii=True) + "\n")
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "output_path": str(output_path.resolve()),
        "counts": dict(counts),
        "recovery_failure_class_counts": dict(recovery_counts),
    }
    if summary_path is not None:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply deterministic labels to replayed training examples."
    )
    parser.add_argument("--input", type=Path, required=True, help="Input JSONL path.")
    parser.add_argument("--out", type=Path, required=True, help="Output JSONL path.")
    parser.add_argument(
        "--summary-out",
        type=Path,
        help="Optional JSON summary report path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    label_file(args.input, args.out, args.summary_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
